package mikumc.zstd;

import com.velocitypowered.api.proxy.ProxyServer;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelId;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import mikumc.zstd.protocol.ZstdDebugLog;
import mikumc.zstd.protocol.ZstdDiagFrames;
import mikumc.zstd.protocol.ZstdDiagStore;

/**
 * 卡住会话自动取证（Velocity 端）。
 *
 * <h2>它解决什么</h2>
 * <p>代理端是唯一知道"这个玩家叫什么"的一侧：后端只能看到代理的地址，玩家名要等登录包过完。
 * 也正因为如此，卡在代理与客户端之间的连接，只有这里能一边判定"是谁卡住了"，
 * 一边向那个客户端索取它自己的日志。判定口径：{@code PostLoginEvent} 已经发过
 * （名字确凿），但这条连接始终没走到"接上后端"——协商器自移除之前都留在登记表里。</p>
 *
 * <h2>为什么发送走 {@link ZstdHijacker#sendDiagQuery}</h2>
 * <p>登录查询必须从 {@code minecraft-encoder} 的 context 写出，帧长度才会被补上；
 * 走 {@code channel.writeAndFlush} 会把裸包丢给帧层，对端读到的是坏帧。协商查询踩过这个坑，
 * 这里直接复用同一条发送路径。</p>
 */
public final class ZstdVelocityDiag {

    /** 每条连接的状态；连接正常走完登录时随协商器一起被摘除。 */
    private static final class Session {
        final Channel channel;
        /** 对端地址（主机部分），用于和"这个玩家已经接上后端"对上号。 */
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

    /** 预取延迟：登录尖峰时别和真正的协商抢时序。 */
    private static final long PREFLIGHT_DELAY_MS = 3000L;

    /** 名字 hint 的存活时间。 */
    private static final long NAME_TTL_MS = 120_000L;

    private static final Map<ChannelId, Session> SESSIONS = new ConcurrentHashMap<>();
    private static final Map<String, String> NAMES = new ConcurrentHashMap<>();
    private static final Map<String, Long> NAME_TIMES = new ConcurrentHashMap<>();

    private static volatile Object task;
    private static volatile Object schedulerPlugin;
    private static volatile ProxyServer proxy;
    private static volatile Logger logger;
    private static volatile Path stallRoot;
    private static volatile boolean enabled;
    private static volatile boolean pullClientLog;
    private static volatile boolean preflight;
    private static volatile int stallDetectMs = 15000;
    private static volatile int maxKb = 256;
    private static volatile int keepBundles = 20;

    private ZstdVelocityDiag() {
    }

    /** 由插件在 {@code ProxyInitializeEvent} 末尾启动。 */
    public static void start(Object plugin, ProxyServer server, Logger log, Path dataDir) {
        stop();
        ZstdVelocityConfig cfg = ZstdVelocityConfig.INSTANCE;
        proxy = server;
        logger = log;
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
        schedulerPlugin = plugin;
        scheduleTask();
    }

    /** 挂上每秒一次扫描（幂等：已有任务就返回）。 */
    private static void scheduleTask() {
        if (task != null) {
            return;
        }
        Object owner = schedulerPlugin;
        ProxyServer server = proxy;
        if (owner == null || server == null) {
            return;
        }
        try {
            task = server.getScheduler()
                    .buildTask(owner, ZstdVelocityDiag::tick)
                    .repeat(Duration.ofSeconds(1))
                    .delay(Duration.ofSeconds(1))
                    .schedule();
        } catch (Throwable t) {
            Logger out = logger;
            if (out != null) {
                out.warn("[Zstd] 卡住会话取证未能启动（不影响连接）: {}", t.toString());
            }
        }
    }

    /** 收掉扫描任务（幂等）。 */
    private static void cancelTask() {
        Object running = task;
        task = null;
        if (running instanceof com.velocitypowered.api.scheduler.ScheduledTask scheduled) {
            try {
                scheduled.cancel();
            } catch (Throwable ignored) {
                // 关服阶段的取消失败没有补救手段
            }
        }
    }

    /**
     * 只重读可即时生效的标量（{@code /mikuzstd reload} 用），不清空已登记的连接。
     *
     * <p>开关从关到开要顺手把任务挂上，从开到关要连任务一起收掉——否则 reload 后
     * {@code /mikuzstd status} 显示"开启"，实际没有任何扫描在跑。</p>
     */
    public static void applyConfig() {
        ZstdVelocityConfig cfg = ZstdVelocityConfig.INSTANCE;
        pullClientLog = cfg.pullClientLog;
        preflight = cfg.preflight;
        stallDetectMs = cfg.stallDetectMs;
        maxKb = cfg.diagMaxKb;
        keepBundles = cfg.keepBundles;
        boolean want = cfg.diagnosticsEnabled;
        if (!want) {
            cancelTask();
            SESSIONS.clear();
            enabled = false;
            return;
        }
        enabled = true;
        if (task == null && proxy != null) {
            scheduleTask();
        }
    }

    /** 由插件在 {@code ProxyShutdownEvent} 里调用。 */
    public static void stop() {
        cancelTask();
        schedulerPlugin = null;
        SESSIONS.clear();
        NAMES.clear();
        NAME_TIMES.clear();
        enabled = false;
    }

    /** 由 {@code PostLoginEvent} 登记名字：卡住时才知道是谁。 */
    public static void onPostLogin(InetSocketAddress address, String username) {
        if (!enabled || username == null || address == null) {
            return;
        }
        String host = hostOf(address);
        if (host == null) {
            return;
        }
        NAMES.put(host, username);
        NAME_TIMES.put(host, System.currentTimeMillis());
    }

    static void open(Channel channel) {
        if (!enabled || channel == null) {
            return;
        }
        SESSIONS.putIfAbsent(channel.id(), new Session(channel));
    }

    static void close(Channel channel) {
        if (channel == null) {
            return;
        }
        SESSIONS.remove(channel.id());
    }

    /** 协商应答到达（客户端认得模组）——安排一次预取。 */
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
     * @return {@code true} 表示这条应答属于本模块，调用方必须吞掉（不能下传）
     */
    static boolean onAnswer(Channel channel, int txId, byte[] payload) {
        Session session = channel == null ? null : SESSIONS.get(channel.id());
        if (session == null) {
            return false;
        }
        ZstdChannelManager mgr = channel.attr(ZstdChannelManager.KEY).get();
        if (mgr == null || !mgr.isDiagTxId(txId)) {
            return false;
        }
        boolean stall = txId == mgr.getStallTxId();
        Path dir = stall ? session.stallBundle : session.preflightBundle;
        String mode = stall ? "卡住探针" : "预取";
        ZstdDiagFrames.Answer answer = ZstdDiagFrames.readAnswer(payload);
        if (answer == null) {
            note(dir, mode + ": 应答无法解析（客户端可能是旧版模组）");
            info(mode + "：客户端未回传可用内容（应答无法解析）");
            return true;
        }
        if (!answer.hasPayload()) {
            note(dir, mode + ": 状态=" + statusName(answer.status()) + "，无载荷");
            info(mode + "：客户端未回传（" + statusName(answer.status()) + "）");
            return true;
        }
        byte[] check = ZstdDiagFrames.gunzipBounded(answer.payload(), (maxKb << 10) + 65536);
        int inflated = check == null ? -1 : check.length;
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
        for (Session session : SESSIONS.values()) {
            Channel channel = session.channel;
            if (channel == null || !channel.isActive()) {
                if (channel != null) {
                    SESSIONS.remove(channel.id(), session);
                }
                continue;
            }
            try {
                ZstdChannelManager mgr = channel.attr(ZstdChannelManager.KEY).get();
                // 管理器缺失 / 已激活 / 已经接上后端 → 都不算卡住
                if (mgr == null || mgr.isReplaced() || completedLogin(session)) {
                    SESSIONS.remove(channel.id(), session);
                    continue;
                }
                // 只有真正发出过协商查询的连接才可能"卡在协商"：握手嗅探器装上去之后
                // 状态 ping 之类的连接永远走不到这一步，不能把它们算进来。
                if (!mgr.isNegotiationSent()) {
                    continue;
                }
                if (session.sentAt == 0L) {
                    session.sentAt = now;
                }
                maybePreflight(session, now);
                maybeStall(session, now);
            } catch (Throwable t) {
                ZstdDebugLog.d("[Zstd] 卡住判定异常（忽略）: {}", t.toString());
            }
        }
    }

    /** 已经接上后端的连接不再算卡住（协商器通常已自移除，这里再收一道）。 */
    private static boolean completedLogin(Session session) {
        ProxyServer server = proxy;
        if (server == null) {
            return false;
        }
        String host = session.host;
        if (host == null) {
            return false;
        }
        Long at = NAME_TIMES.get(host);
        if (at == null || System.currentTimeMillis() - at > NAME_TTL_MS) {
            return false;
        }
        String name = NAMES.get(host);
        if (name == null) {
            return false;
        }
        return server.getPlayer(name).map(player -> player.getCurrentServer().isPresent()).orElse(false);
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
        if (pullClientLog) {
            ZstdHijacker.sendDiagQuery(session.channel, ZstdDiagFrames.MODE_PREFLIGHT);
        }
    }

    private static void maybeStall(Session session, long now) {
        if (session.stallTriggered) {
            return;
        }
        // 计时基准是"协商查询发出的时刻"：握手与虚拟主机的处理耗时不是要抓的东西。
        long since = session.sentAt == 0L ? session.openedAt : session.sentAt;
        long age = now - since;
        if (age < stallDetectMs) {
            return;
        }
        session.stallTriggered = true;
        session.stallBundle = writeServerSide(session, "卡住探针（协商查询发出后 " + age + "ms 无进展）");
        info("判定卡住：" + resolveName(session.channel) + " 已 " + age + "ms 未接上后端，现场="
                + session.stallBundle);
        if (pullClientLog) {
            ZstdHijacker.sendDiagQuery(session.channel, ZstdDiagFrames.MODE_STALL);
        }
    }

    private static Path writeServerSide(Session session, String modeText) {
        try {
            Map<String, String> facts = new LinkedHashMap<>();
            facts.put("模式", modeText);
            facts.put("拓扑", "Velocity（代理端）");
            facts.put("远端", String.valueOf(session.channel.remoteAddress()));
            facts.put("连接存活ms", String.valueOf(System.currentTimeMillis() - session.openedAt));
            ZstdChannelManager mgr = session.channel.attr(ZstdChannelManager.KEY).get();
            if (mgr != null) {
                facts.put("协商txId", String.valueOf(mgr.getNegotiateTxId()));
                facts.put("协商已发出", mgr.isNegotiationSent() ? "是" : "否");
                facts.put("协商应答", mgr.isAnswerReceived() ? "已收到" : "未收到");
                facts.put("客户端就绪", mgr.isClientReady() ? "是" : "否");
                facts.put("已激活zstd", mgr.isReplaced() ? "是" : "否");
            }
            facts.put("本端调试日志", String.valueOf(ZstdDebugLog.file()));
            byte[] tail = ZstdDiagFrames.readTail(ZstdDebugLog.file(), maxKb << 10).data();
            return ZstdDiagStore.writeBundle(stallRoot, resolveName(session.channel),
                    System.currentTimeMillis(), tail, null, facts, keepBundles);
        } catch (Throwable t) {
            ZstdDebugLog.d("[Zstd] 写现场失败（忽略）: {}", t.toString());
            return null;
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

    private static String hostOf(Object address) {
        if (address instanceof InetSocketAddress socket && socket.getAddress() != null) {
            return socket.getAddress().getHostAddress();
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
        return "卡住取证：" + (enabled
                ? "开启（阈值 " + stallDetectMs + "ms，现场 " + keepBundles + " 组）"
                : "关闭");
    }
}
