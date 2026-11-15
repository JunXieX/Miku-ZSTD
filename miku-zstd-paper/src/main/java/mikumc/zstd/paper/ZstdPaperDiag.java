package mikumc.zstd.paper;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelId;
import io.papermc.paper.threadedregions.scheduler.ScheduledTask;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Logger;
import mikumc.zstd.protocol.ZstdDebugLog;
import mikumc.zstd.protocol.ZstdDiagFrames;
import mikumc.zstd.protocol.ZstdDiagStore;
import mikumc.zstd.protocol.ZstdVarInts;
import org.bukkit.plugin.java.JavaPlugin;

/**
 * 卡住会话自动取证（Paper 端）。
 *
 * <h2>它解决什么</h2>
 * <p>"名字进来了、人没进来"这种幽灵连接，服务端能看到的只是"协商查询发出去了、应答没回来"。
 * 真正的原因（客户端收没收到、装没装解码器、卡在哪个锚点）只在客户端那一侧，而玩家既不知道
 * 文件在哪，一旦放弃重连现场也就没了。所以这里在<b>判定卡住的当下</b>主动做两件事：</p>
 * <ol>
 *   <li>把本端调试文件尾部 + 连接事实写成一组现场（{@code logs/stall/<玩家>-<时间>/}）；</li>
 *   <li>向该连接发一条 {@code zstd:diag} 登录查询，把客户端调试日志的尾部取回来。</li>
 * </ol>
 *
 * <h2>为什么用登录查询而不是别的通道</h2>
 * <p>登录阶段客户端不能主动发消息，只能应答服务端查询；{@code zstd:negotiate} 走的就是这条路，
 * 这里与它同构。卡住恰好发生在这个阶段，所以只有它可用。</p>
 *
 * <h2>为什么预取</h2>
 * <p>如果客户端已经卡在"该解 zstd 却解不开"里，它连入站包都读不了，这次查询必然拿不到东西。
 * 但客户端日志是跨连接追加写的：下一次登录时先取一次"上一轮的尾巴"，就能把那次卡死带上来。
 * 预取只在协商成功（客户端有模组）后发一次，且不与卡住探针抢同一个 txId。</p>
 *
 * <p>全部方法都不向外抛异常：取证是附加能力，坏掉不能连累登录。</p>
 */
public final class ZstdPaperDiag {

    /** 每条连接的状态。 */
    private static final class Session {
        final Channel channel;
        /** 对端地址（主机部分），用于和"这个玩家已经进服了"对上号。 */
        final String host;
        final long openedAt = System.currentTimeMillis();
        /** 协商查询真正发出的时刻（tick 里从管理器观察到）；0 = 还没发。 */
        volatile long sentAt;
        volatile long negotiatedAt;
        volatile boolean stallTriggered;
        volatile boolean preflightTriggered;
        volatile Path stallBundle;
        volatile Path preflightBundle;

        Session(Channel channel) {
            this.channel = channel;
            this.host = hostOf(channel.remoteAddress());
        }
    }

    /** 预取等这么久再发：登录尖峰时别和真正的协商抢时序。 */
    private static final long PREFLIGHT_DELAY_MS = 3000L;

    /** 名字hint的存活时间：同名玩家连续重试时还能用上。 */
    private static final long NAME_TTL_MS = 120_000L;

    private static final Map<ChannelId, Session> SESSIONS = new ConcurrentHashMap<>();
    private static final Map<String, String> NAMES = new ConcurrentHashMap<>();
    private static final Map<String, Long> NAME_TIMES = new ConcurrentHashMap<>();
    /** 已进服的地址：进服即证明这条连接没卡住，立刻退出判定（否则每个正常玩家都会被误判）。 */
    private static final Map<String, Long> JOINED = new ConcurrentHashMap<>();

    private static volatile ScheduledTask task;
    private static volatile Logger logger;
    private static volatile Path stallRoot;
    private static volatile boolean enabled;
    private static volatile boolean pullClientLog;
    private static volatile boolean preflight;
    private static volatile int stallDetectMs = 15000;
    private static volatile int maxKb = 256;
    private static volatile int keepBundles = 20;

    private ZstdPaperDiag() {
    }

    /** 由插件在 {@code onEnable} 末尾启动（配置就绪之后）。 */
    public static void start(JavaPlugin owner, Path dataDir) {
        stop();
        ZstdPaperConfig cfg = ZstdPaperConfig.INSTANCE;
        logger = owner.getLogger();
        enabled = cfg.diagnosticsEnabled;
        pullClientLog = cfg.pullClientLog;
        preflight = cfg.preflight;
        stallDetectMs = cfg.stallDetectMs;
        maxKb = cfg.diagMaxKb;
        keepBundles = cfg.keepBundles;
        stallRoot = dataDir.resolve("logs").resolve("stall");
        if (!enabled) {
            return;
        }
        try {
            task = owner.getServer().getGlobalRegionScheduler()
                    .runAtFixedRate(owner, scheduled -> tick(), 20L, 20L);
        } catch (Throwable t) {
            logger.warning("[Zstd] 卡住会话取证未能启动（不影响连接）: " + t);
        }
    }

    /** 由插件在 {@code onDisable} 里调用。 */
    public static void stop() {
        ScheduledTask running = task;
        task = null;
        if (running != null) {
            try {
                running.cancel();
            } catch (Throwable ignored) {
                // 关服阶段的取消失败没有补救手段
            }
        }
        SESSIONS.clear();
        NAMES.clear();
        NAME_TIMES.clear();
        JOINED.clear();
        enabled = false;
    }

    /**
     * 由 {@code PlayerJoinEvent} 记录"这个地址已经进服"。
     *
     * <p>⚠️ 这条登记不是可选项：协商器在定案后还要在管线里留
     * {@code late_answer_window_ms}（默认 120s）吞迟到应答，如果不拿"进服"当退出信号，
     * 每个正常玩家都会在 15 秒后被判成卡住，现场目录与诊断查询全被正常连接淹没。</p>
     */
    public static void onJoin(InetSocketAddress address) {
        if (!enabled || address == null) {
            return;
        }
        String host = hostOf(address);
        if (host != null) {
            JOINED.put(host, System.currentTimeMillis());
        }
    }

    /**
     * 由 {@code AsyncPlayerPreLoginEvent} 记录名字与地址的对应关系（卡住时才知道是谁）。
     *
     * <p>⚠️ 这里收 {@link InetAddress} 而不是 {@link InetSocketAddress}：该事件给的是
     * 纯地址（没有端口），而 {@code PlayerJoinEvent} 与连接层拿到的是带端口的 socket 地址。
     * 两者归一化到"主机地址字符串"才能对上号——端口在登录链路上每连接都不同，不能参与匹配。</p>
     */
    public static void onPreLogin(String name, InetAddress address) {
        if (!enabled || name == null || address == null) {
            return;
        }
        String host = hostOf(address);
        if (host == null) {
            return;
        }
        NAMES.put(host, name);
        NAME_TIMES.put(host, System.currentTimeMillis());
    }

    /** 协商器装上时登记；只有登记过的连接才会被判定。 */
    static void open(Channel channel) {
        if (!enabled || channel == null) {
            return;
        }
        SESSIONS.putIfAbsent(channel.id(), new Session(channel));
    }

    /** 协商器摘除或连接关闭时注销。 */
    static void close(Channel channel) {
        if (channel == null) {
            return;
        }
        SESSIONS.remove(channel.id());
    }

    /** 协商成功（客户端认得 {@code zstd:negotiate}）——安排一次预取。 */
    static void onNegotiated(Channel channel) {
        Session session = channel == null ? null : SESSIONS.get(channel.id());
        if (session == null || !enabled || !preflight) {
            return;
        }
        session.negotiatedAt = System.currentTimeMillis();
    }

    /**
     * 消费一条诊断应答。
     *
     * @return {@code true} 表示这条应答属于本模块，调用方必须吞掉（不能漏给原版登录监听器）
     */
    static boolean onAnswer(Channel channel, int txId, byte[] payload) {
        Session session = channel == null ? null : SESSIONS.get(channel.id());
        if (session == null) {
            return false;
        }
        ZstdPaperChannelManager mgr = channel.attr(ZstdPaperChannelManager.KEY).get();
        if (mgr == null || !mgr.isDiagTxId(txId)) {
            return false;
        }
        boolean stall = txId == mgr.getStallTxId();
        Path dir = stall ? session.stallBundle : session.preflightBundle;
        ZstdDiagFrames.Answer answer = ZstdDiagFrames.readAnswer(payload);
        String mode = stall ? "卡住探针" : "预取";
        if (answer == null) {
            // success=false（客户端不认识该通道）与"认识但载荷畸形"都会落到这里
            note(dir, mode + ": 应答无法解析（客户端可能是旧版模组）");
            info(mode + "：客户端未回传可用内容（应答无法解析）");
            return true;
        }
        if (!answer.hasPayload()) {
            note(dir, mode + ": 状态=" + statusName(answer.status()) + "，无载荷");
            info(mode + "：客户端未回传（" + statusName(answer.status()) + "）");
            return true;
        }
        int inflated = -1;
        byte[] check = ZstdDiagFrames.gunzipBounded(answer.payload(), (maxKb << 10) + 65536);
        if (check != null) {
            inflated = check.length;
        }
        if (dir != null) {
            try {
                Files.write(dir.resolve("client.log.gz"), answer.payload());
            } catch (IOException e) {
                info(mode + "：客户端日志写入失败 " + e);
            }
            note(dir, mode + ": 状态=" + statusName(answer.status())
                    + " 回传=" + answer.payload().length + "B 解压后="
                    + (inflated < 0 ? "拒绝（超上限或非 gzip）" : inflated + "B"));
            info(mode + "：已捕获客户端日志 " + dir + "（" + answer.payload().length + "B"
                    + (inflated < 0 ? "" : " → " + inflated + "B") + "）");
        } else {
            info(mode + "：收到客户端日志但本次没有现场目录（" + answer.payload().length + "B）");
        }
        return true;
    }

    /** 每秒扫一次：该预取的预取，该判定的判定。 */
    private static void tick() {
        if (!enabled) {
            return;
        }
        long now = System.currentTimeMillis();
        pruneJoined(now);
        for (Session session : SESSIONS.values()) {
            Channel channel = session.channel;
            if (channel == null || !channel.isActive()) {
                if (channel != null) {
                    SESSIONS.remove(channel.id(), session);
                }
                continue;
            }
            try {
                ZstdPaperChannelManager mgr = channel.attr(ZstdPaperChannelManager.KEY).get();
                // 管理器缺失 / 已激活 / 这个地址已经进服 → 都不算卡住
                if (mgr == null || mgr.isReplaced() || joined(session, now)) {
                    SESSIONS.remove(channel.id(), session);
                    continue;
                }
                // 只有真正发出过协商查询的连接才可能"卡在协商"：状态 ping 之类的连接
                // 永远走不到这一步，不能把它们算进来。
                if (mgr.getNegotiateTxId() == -1) {
                    continue;
                }
                if (session.sentAt == 0L) {
                    session.sentAt = now;
                }
                maybePreflight(session, now);
                maybeStall(session, now);
            } catch (Throwable t) {
                // 单条连接的问题不能拖垮整轮扫描
                ZstdDebugLog.d("[Zstd] 卡住判定异常（忽略）: {}", t.toString());
            }
        }
    }

    /** 地址是否在这次判定之前就已经进服（进服是"没卡住"的确凿证据）。 */
    private static boolean joined(Session session, long now) {
        String host = session.host;
        if (host == null) {
            return false;
        }
        Long at = JOINED.get(host);
        return at != null && now - at <= NAME_TTL_MS;
    }

    private static void pruneJoined(long now) {
        JOINED.entrySet().removeIf(entry -> now - entry.getValue() > NAME_TTL_MS);
    }

    private static void maybePreflight(Session session, long now) {
        if (!preflight || session.preflightTriggered || session.negotiatedAt <= 0L) {
            return;
        }
        if (now - session.negotiatedAt < PREFLIGHT_DELAY_MS) {
            return;
        }
        session.preflightTriggered = true;
        session.preflightBundle = writeServerSide(session, "预取（上一轮尾巴）");
        if (!pullClientLog) {
            return;
        }
        sendQuery(session.channel, ZstdDiagFrames.MODE_PREFLIGHT);
    }

    private static void maybeStall(Session session, long now) {
        if (session.stallTriggered) {
            return;
        }
        // 计时基准是"协商查询发出的时刻"，不是"连接建立"：握手、请求 SetCompression
        // 这些前置步骤的耗时不是我们要抓的东西，把它们算进去只会误伤慢客户端。
        long since = session.sentAt == 0L ? session.openedAt : session.sentAt;
        long age = now - since;
        if (age < stallDetectMs) {
            return;
        }
        session.stallTriggered = true;
        session.stallBundle = writeServerSide(session, "卡住探针（协商查询发出后 " + age + "ms 无进展）");
        info("判定卡住：" + resolveName(session.channel) + " 已 " + age + "ms 未完成登录协商，现场="
                + session.stallBundle);
        if (ZstdDebugLog.file() == null) {
            // 关掉 debug 是服主的权利，但他得知道这次现场里少了什么，
            // 否则会以为"日志本来就该是空的"。
            info("调试日志已关闭（logging.debug=false），本次现场只有 session.txt 的连接事实；"
                    + "要拿到逐包时间线请把 logging.debug 打开");
        }
        if (pullClientLog) {
            sendQuery(session.channel, ZstdDiagFrames.MODE_STALL);
        }
    }

    private static Path writeServerSide(Session session, String modeText) {
        try {
            Map<String, String> facts = new LinkedHashMap<>();
            facts.put("模式", modeText);
            facts.put("拓扑", "Paper（若经代理，远端即代理地址）");
            facts.put("远端", String.valueOf(session.channel.remoteAddress()));
            facts.put("连接存活ms", String.valueOf(System.currentTimeMillis() - session.openedAt));
            ZstdPaperChannelManager mgr = session.channel.attr(ZstdPaperChannelManager.KEY).get();
            if (mgr != null) {
                facts.put("协商txId", String.valueOf(mgr.getNegotiateTxId()));
                facts.put("协商应答", mgr.isResponseReceived() ? "已收到" : "未收到");
                facts.put("已激活zstd", mgr.isReplaced() ? "是" : "否");
            }
            // 关闭时 file() 是 null：直说"关掉了"，别让人以为是读失败。
            facts.put("本端调试日志", ZstdDebugLog.file() == null
                    ? "关闭（logging.debug=false）" : String.valueOf(ZstdDebugLog.file()));
            byte[] tail = ZstdDiagFrames.readTail(ZstdDebugLog.file(), maxKb << 10).data();
            return ZstdDiagStore.writeBundle(stallRoot, resolveName(session.channel),
                    System.currentTimeMillis(), tail, null, facts, keepBundles);
        } catch (Throwable t) {
            ZstdDebugLog.d("[Zstd] 写现场失败（忽略）: {}", t.toString());
            return null;
        }
    }

    /**
     * 发一条诊断查询；txId 先登记再写，避免应答先到。
     *
     * <p>⚠️ 从<b>协商器自己的 context</b> 写出，而不是 {@code channel.writeAndFlush}：
     * 后者会经过协商器覆写的 {@code write()}，在激活窗口内被扣进
     * {@code heldPackets} 等人放行——而卡住恰恰就卡在"等不到放行"这一点上，
     * 那条查询会陪着一起卡住。绕过自己只跳过扣包，帧编码与压缩照旧。</p>
     *
     * <p>协商器已摘除（登录早结束）时直接不发：那时写登录查询本身就是非法包。</p>
     */
    private static void sendQuery(Channel channel, byte mode) {
        ZstdPaperChannelManager mgr = channel.attr(ZstdPaperChannelManager.KEY).get();
        if (mgr == null) {
            return;
        }
        io.netty.channel.ChannelHandlerContext writeCtx =
                channel.pipeline().context(ZstdPaperNegotiator.class);
        if (writeCtx == null) {
            return;
        }
        int txId = ThreadLocalRandom.current().nextInt(1, Integer.MAX_VALUE);
        boolean stall = mode == ZstdDiagFrames.MODE_STALL;
        boolean claimed = stall ? mgr.claimStallTxId(txId) : mgr.claimPreflightTxId(txId);
        if (!claimed) {
            return;
        }
        ByteBuf buf = writeCtx.alloc().buffer();
        boolean written = false;
        try {
            ZstdVarInts.write(buf, 0x04);                                  // LoginPluginRequest
            ZstdVarInts.write(buf, txId);
            ZstdVarInts.writeString(buf, ZstdDiagFrames.CHANNEL);
            ZstdDiagFrames.writeRequest(buf, ZstdDiagFrames.PROTOCOL_VERSION, mode);
            writeCtx.writeAndFlush(buf);
            written = true;
            ZstdDebugLog.d("[Zstd] Sent zstd:diag mode={} txId={} remote={}", mode, txId,
                    channel.remoteAddress());
        } catch (Throwable t) {
            ZstdDebugLog.d("[Zstd] 诊断查询发送失败（忽略）: {}", t.toString());
        } finally {
            if (!written && buf.refCnt() > 0) {
                buf.release();
            }
        }
    }

    private static String resolveName(Channel channel) {
        String host = hostOf(channel == null ? null : channel.remoteAddress());
        if (host != null) {
            Long at = NAME_TIMES.get(host);
            if (at != null && System.currentTimeMillis() - at <= NAME_TTL_MS) {
                String name = NAMES.get(host);
                if (name != null && !name.isBlank()) {
                    return name;
                }
            }
        }
        return "unknown";
    }

    /** 归一化到主机地址：登录事件给 {@link InetAddress}，连接/进服给 {@link InetSocketAddress}。 */
    private static String hostOf(Object address) {
        if (address instanceof InetSocketAddress socket) {
            InetAddress resolved = socket.getAddress();
            return resolved == null ? socket.getHostString() : resolved.getHostAddress();
        }
        if (address instanceof InetAddress ip) {
            return ip.getHostAddress();
        }
        return null;
    }

    private static void note(Path dir, String text) {
        if (dir == null) {
            return;
        }
        try {
            Files.writeString(dir.resolve("session.txt"), text + System.lineSeparator(),
                    StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (Throwable ignored) {
            // 备注写不进去不影响主文件
        }
    }

    private static String statusName(int status) {
        return switch (status) {
            case ZstdDiagFrames.STATUS_OK -> "OK";
            case ZstdDiagFrames.STATUS_NOT_MODDED -> "NOT_MODDED（客户端无该通道）";
            case ZstdDiagFrames.STATUS_NO_LOG -> "NO_LOG（客户端未提供）";
            case ZstdDiagFrames.STATUS_TRUNCATED -> "TRUNCATED（已截断）";
            case ZstdDiagFrames.STATUS_NOT_CLIENT -> "NOT_CLIENT（这条链路对面是代理）";
            default -> "ERROR(" + status + ")";
        };
    }

    private static void info(String message) {
        Logger out = logger;
        if (out != null) {
            out.info("[Zstd] " + message);
        }
    }

    /** 仅供状态命令展示。 */
    public static String statusLine() {
        return "卡住取证：" + (enabled ? "开启（阈值 " + stallDetectMs + "ms，现场 " + keepBundles + " 组）" : "关闭");
    }
}
