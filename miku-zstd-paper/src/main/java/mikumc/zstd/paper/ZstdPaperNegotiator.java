package mikumc.zstd.paper;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import mikumc.zstd.protocol.ZstdDictId;
import mikumc.zstd.protocol.ZstdNegotiateStatus;
import mikumc.zstd.protocol.ZstdVarInts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * 协商与激活（Paper 端核心调度）。
 *
 * <h2>时序（实测确定）</h2>
 * <ol>
 *   <li>注入发生在<b>连接建立时</b>（见 MikuZstdPaper 的 childHandler 包装）；</li>
 *   <li>协商查询在<b>首个入站包之后</b>发出——太早客户端还没挂好登录查询接收器；</li>
 *   <li>检测到 SetCompression 写出时进入 hold，结论确定后再按定型管道补发。</li>
 * </ol>
 * <p>实测顺序：negotiate → （客户端应答）→ SetCompression ✓</p>
 */
public class ZstdPaperNegotiator extends ChannelDuplexHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger("miku-zstd");

    /** NMS 包接口（MC 26.x 未混淆）；用于区分"MC 包"与 netty 控制消息 */
    private static final Class<?> NMS_PACKET = tryLoad("net.minecraft.network.protocol.Packet");

    private static final long RETRY_MS = 100;
    private static final int MAX_RETRIES = 20;

    private boolean holding;
    private final List<Object> heldPackets = new ArrayList<>();
    private final List<ChannelPromise> heldPromises = new ArrayList<>();

    /**
     * 连接去向：{@code TRUE}=登录连接、{@code FALSE}=服务器列表 ping、{@code null}=尚未确定。
     *
     * <p>⚠️ 不能只用「是否见过握手包」判断——{@code ClientIntentionPacket} 在
     * <b>ping 与登录两种连接上都会发</b>，区别只在握手里的 nextState（1=STATUS、2=LOGIN）。
     * 曾因门控写成「见过握手包就发」，给 ping 连接也发了 LoginPluginRequest，
     * 客户端解析错位（{@code Received unknown packet id 4}）、服务器列表直接显示连不上。</p>
     */
    private Boolean loginConnection;

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ZstdPaperChannelManager mgr = ctx.channel().attr(ZstdPaperChannelManager.KEY).get();

        // 1) 握手包：必须区分「服务器列表 ping」与「登录」。
        //    ⚠️ 两者都会发 ClientIntentionPacket，只看"有没有握手包"等于没有门控——
        //    给 ping 连接发 LoginPluginRequest 会让客户端把它当成 status 阶段的包解析，
        //    结果是 ping 失败（服务器列表显示连不上）+ 客户端刷 DecoderException。
        //    真正的判据是握手包的 nextState：1=STATUS(列表 ping)、2=LOGIN、3=TRANSFER。
        if (msg != null && msg.getClass().getSimpleName().contains("ClientIntention")) {
            int nextState = readNextState(msg);
            if (nextState == 1) {
                loginConnection = Boolean.FALSE; // 服务器列表 ping：永不发协商
            } else if (nextState >= 2) {
                loginConnection = Boolean.TRUE;
            } else {
                loginConnection = null; // 读不出 → 交给下面 login start 兜底判定
            }
        }

        // 2) 发协商：客户端此刻已挂好登录查询接收器，是最早能发的时机。
        //    · 已确认是登录连接 → 立即发；
        //    · 握手包读不出 nextState → 必须等到真正的登录首包（login start）才发。
        //      绝不允许"读不出就发"：那等于把 ping 一起打了，代价比"zstd 没生效"大得多。
        if (mgr != null && mgr.getNegotiateTxId() == -1
                && (Boolean.TRUE.equals(loginConnection)
                    || (loginConnection == null && isLoginStartPacket(msg)))) {
            sendNegotiate(ctx);
        }

        // 2) 包层拦截协商应答（需在 packet_handler 之前消费，否则原版会当成"意外应答"断连）
        if (mgr != null && msg != null && !mgr.isResponseReceived()) {
            String cn = msg.getClass().getSimpleName();
            if (cn.contains("Query") || cn.contains("Plugin")) {
                int tx = readInt(msg, "transactionId", "id", "transactionID");
                boolean isNegotiate = tx != 0 && tx == mgr.getNegotiateTxId();
                boolean isDict = tx != 0 && tx == mgr.getDictTxId();
                if (isNegotiate || isDict) {
                    byte[] payload = readBytes(msg, "data", "payload", "contents");
                    // 取不到字节 = 客户端没理解这次查询（success=false），与"理解了但拒绝"必须分开表述
                    boolean success = payload != null;
                    int enc = ZstdNegotiateStatus.VANILLA;
                    int dec = ZstdNegotiateStatus.VANILLA;
                    if (payload != null && payload.length >= 2) {
                        int[] cursor = {0};
                        enc = ZstdNegotiateStatus.sanitize(
                                ZstdVarInts.readOr(payload, cursor, ZstdNegotiateStatus.FALLBACK));
                        dec = ZstdNegotiateStatus.sanitize(
                                ZstdVarInts.readOr(payload, cursor, ZstdNegotiateStatus.FALLBACK));
                    }
                    LOGGER.debug("[Zstd] 捕获{}应答: {} tx={} enc={} dec={} success={}",
                            isDict ? "字典" : "协商", cn, tx, enc, dec, success);
                    io.netty.util.ReferenceCountUtil.release(msg);
                    if (isNegotiate && ZstdPaperNegotiator.needsDictPush(enc, dec)) {
                        // 协议 v4：客户端缺字典 → 单独推送 zstd:dict，门控结论等其应答
                        mgr.markDictRequested();
                        sendDictQuery(ctx, ZstdNegotiateStatus.needsDict(enc),
                                ZstdNegotiateStatus.needsDict(dec));
                    } else {
                        mgr.markResponse(enc, dec, success);
                        ctx.channel().eventLoop().execute(() -> tryActivate(ctx, 0));
                    }
                    return;
                }
            }
        }

        super.channelRead(ctx, msg);
    }



    /** 发轻量 negotiate：只带协议版本与 dictId（本端无字典训练，恒为 0）。 */
    private void sendNegotiate(ChannelHandlerContext ctx) {
        ZstdPaperChannelManager mgr = ctx.channel().attr(ZstdPaperChannelManager.KEY).get();
        if (mgr == null || mgr.getNegotiateTxId() != -1) return;

        int txId = ThreadLocalRandom.current().nextInt(1, Integer.MAX_VALUE);
        mgr.setNegotiateTxId(txId);

        ByteBuf buf = ctx.alloc().buffer();
        try {
            ZstdVarInts.write(buf, 0x04);
            ZstdVarInts.write(buf, txId);
            ZstdVarInts.writeString(buf, "zstd:negotiate");
            buf.writeInt(ZstdPaperChannelManager.PROTOCOL_VERSION);
            long encId = ZstdPaperDictRegistry.encoderDictId();
            long decId = ZstdPaperDictRegistry.decoderDictId();
            buf.writeLong(encId);
            buf.writeLong(decId);
            byte flags = 0;
            if (encId != 0) flags |= 1;
            if (decId != 0) flags |= 2;
            buf.writeByte(flags); // v4 仅声明"有字典"，字节按需另发
            ctx.writeAndFlush(buf);
            LOGGER.debug("[Zstd] Sent zstd:negotiate txId={}", txId);
        } catch (Throwable t) {
            if (buf.refCnt() > 0) buf.release();
            throw t;
        }
    }

    /** 协议 v4：按需推送字典（仅当客户端报告某个方向缺字典时）。 */
    private void sendDictQuery(ChannelHandlerContext ctx, boolean needEncoder, boolean needDecoder) {
        ZstdPaperChannelManager mgr = ctx.channel().attr(ZstdPaperChannelManager.KEY).get();
        if (mgr == null) return;
        byte[] enc = needEncoder ? ZstdPaperDictRegistry.encoderDictBytes() : null;
        byte[] dec = needDecoder ? ZstdPaperDictRegistry.decoderDictBytes() : null;
        byte flags = 0;
        if (enc != null && enc.length > 0) flags |= 1;
        if (dec != null && dec.length > 0) flags |= 2;

        int txId = ThreadLocalRandom.current().nextInt(1, Integer.MAX_VALUE);
        mgr.setDictTxId(txId);

        ByteBuf buf = ctx.alloc().buffer();
        try {
            ZstdVarInts.write(buf, 0x04);
            ZstdVarInts.write(buf, txId);
            ZstdVarInts.writeString(buf, "zstd:dict");
            buf.writeByte(flags);
            if (enc != null && enc.length > 0) writeDictBytes(buf, enc);
            if (dec != null && dec.length > 0) writeDictBytes(buf, dec);
            ctx.writeAndFlush(buf);
            LOGGER.debug("[Zstd] 已推送 zstd:dict txId={} flags={} (enc={}B dec={}B)", txId, flags,
                    enc == null ? 0 : enc.length, dec == null ? 0 : dec.length);
        } catch (Throwable t) {
            if (buf.refCnt() > 0) buf.release();
            throw t;
        }
    }

    /** 线上字典帧：{@code [int crc32][int len][bytes]}；crc 必须与字典 id 同源。 */
    private static void writeDictBytes(ByteBuf buf, byte[] dict) {
        buf.writeInt(ZstdDictId.wireChecksum(dict));
        buf.writeInt(dict.length);
        buf.writeBytes(dict);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        ZstdPaperChannelManager mgr = ctx.channel().attr(ZstdPaperChannelManager.KEY).get();
        if (mgr == null || mgr.isReplaced()) {
            super.write(ctx, msg, promise);
            return;
        }

        if (!holding && isPacket(msg)
                && msg.getClass().getSimpleName().contains("ClientboundLoginCompression")) {
            sendNegotiate(ctx); // 兜底
            holding = true;
            super.write(ctx, msg, promise);
            ctx.channel().eventLoop().execute(() -> tryActivate(ctx, 0));
            return;
        }

        if (holding) {
            // ⚠️ 只扣 MC 包：管道里还会流经 netty 自身的控制消息（协议阶段切换用的 Runnable），
            // 扣住它会阻塞 LOGIN→CONFIGURATION 切换且日志无异常。
            if (isPacket(msg)) {
                heldPackets.add(msg);
                heldPromises.add(promise);
                return;
            }
            super.write(ctx, msg, promise);
            return;
        }

        super.write(ctx, msg, promise);
    }

    /** 激活流程入口：包一层异常可见性——原实现里 scheduled task 抛异常会被静默吞掉。 */
    private void tryActivate(ChannelHandlerContext ctx, int attempt) {
        try {
            tryActivate0(ctx, attempt);
        } catch (Throwable t) {
            LOGGER.error("[Zstd] 激活流程异常（attempt={}）", attempt, t);
            releaseHeld(ctx);
        }
    }

    private void tryActivate0(ChannelHandlerContext ctx, int attempt) {
        Channel channel = ctx.channel();
        if (!channel.isActive()) return;
        ZstdPaperChannelManager mgr = channel.attr(ZstdPaperChannelManager.KEY).get();
        if (mgr == null || mgr.isReplaced()) return;

        if (!mgr.isResponseReceived()) {
            if (attempt >= MAX_RETRIES) {
                LOGGER.warn("[Zstd] no negotiate response within {}ms — staying vanilla",
                        RETRY_MS * MAX_RETRIES);
                releaseHeld(ctx);
                return;
            }
            channel.eventLoop().schedule(() -> tryActivate(ctx, attempt + 1), RETRY_MS, TimeUnit.MILLISECONDS);
            return;
        }

        if (mgr.isRefused()) {
            LOGGER.warn("[Zstd] 客户端保持原版 zlib，本端不激活");
            releaseHeld(ctx);
            return;
        }

        // ⚠️ 协议 v4 硬门控：客户端报告过"缺字典"，但最终没确认字典就绪
        //（推送丢失 / 客户端加载失败 / 应答没被捕获到）。此时绝不能激活——
        // 客户端会因为等字典停在 PLAIN 状态永不激活，单侧 zstd 必断连。
        if (mgr.isDictRequested() && !mgr.isConfirmed()) {
            LOGGER.warn("[Zstd] 客户端请求了字典但未确认就绪（zstd:dict 应答缺失）"
                    + "— 放弃 zstd 并回落原版，避免单侧 zstd 断连");
            releaseHeld(ctx);
            return;
        }
        // 客户端确认就绪 → 装载共享字典（仅此时装载，避免与客户端不一致）
        if (mgr.isConfirmed()) {
            mgr.loadTrainerDicts();
        }


        LOGGER.debug("[Zstd] 正在安装 zstd 编解码器 (attempt={}, pipeline={})",
                attempt, channel.pipeline().names());
        if (!mgr.installEncoder(channel) || !mgr.installDecoder(channel)) {
            // ⚠️ 原版的 compress/decompress 是在 SetCompression 写出**之后**才安装的
            // （MC 先发包再 setupCompression），首次尝试时它们往往还不存在。
            // 这里必须继续重试而不是放弃——实战（Leaf 26.2）就因此永久回落到 zlib。
            // 重试期间 holding 保持为 true，待发数据仍被扣住，不会发出与原版不一致的帧。
            if (attempt < MAX_RETRIES) {
                channel.eventLoop().schedule(() -> tryActivate(ctx, attempt + 1),
                        RETRY_MS, TimeUnit.MILLISECONDS);
                return;
            }
            LOGGER.warn("[Zstd] anchors 始终未出现（compress/decompress 缺失）— 回落原版; pipeline={}",
                    channel.pipeline().names());
            releaseHeld(ctx);
            return;
        }
        mgr.setReplaced(true); // 计数已内聚在 setReplaced 内（幂等，避免重复 +1）
        LOGGER.info("[Zstd] zstd transport activated on Paper");
        releaseHeld(ctx);
    }

    private void releaseHeld(ChannelHandlerContext ctx) {
        holding = false;
        if (heldPackets.isEmpty()) return;
        List<Object> packets = new ArrayList<>(heldPackets);
        List<ChannelPromise> promises = new ArrayList<>(heldPromises);
        heldPackets.clear();
        heldPromises.clear();
        LOGGER.debug("[Zstd] releasing {} packet(s) held during activation window", packets.size());
        for (int i = 0; i < packets.size(); i++) {
            ctx.write(packets.get(i), promises.get(i));
        }
        ctx.flush();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        holding = false;
        // ⚠️ 先把包本身 release 掉：它们是 ReferenceCounted 的（内部持有 ByteBuf），
        // 扣下时所有权已转到本处理器；既然永远不会被写出，就没有别人替我们释放。
        // 少了这一步，每次"激活窗口内断线"都会泄漏一批（Netty leak detector 会报 LEAK）。
        for (Object p : heldPackets) {
            io.netty.util.ReferenceCountUtil.release(p);
        }
        heldPackets.clear();
        for (ChannelPromise p : heldPromises) {
            p.tryFailure(new java.nio.channels.ClosedChannelException());
        }
        heldPromises.clear();
        super.channelInactive(ctx);
    }

    /**
     * 客户端是否在 negotiate 应答里报告了"缺字典"（协议 v4 需要先推一次 {@code zstd:dict}）。
     * 与 Velocity 端保持同一份判断——两端漏分支的表现都是"单侧 zstd → 断连"。
     */
    private static boolean needsDictPush(int encStatus, int decStatus) {
        return ZstdNegotiateStatus.needsDict(encStatus) || ZstdNegotiateStatus.needsDict(decStatus);
    }

    private static boolean isPacket(Object msg) {
        return NMS_PACKET != null && NMS_PACKET.isInstance(msg);
    }

    /**
     * 读握手包的去向：{@code 1}=STATUS(服务器列表 ping)、{@code 2}=LOGIN、{@code 3}=TRANSFER。
     *
     * <p>读不出返回 {@code -1}——调用方<b>必须保守处理</b>，不可当作登录（否则会给 ping 发协商）。
     * MC 里该字段是 {@code ClientIntent} 枚举，也可能被映射成整数，两种都支持。</p>
     */
    private static int readNextState(Object intention) {
        Object v = readAny(intention, "nextState", "intention", "desiredState", "state");
        if (v instanceof Number n) {
            int i = n.intValue();
            return (i >= 1 && i <= 3) ? i : -1;
        }
        if (v instanceof Enum<?> e) {
            String n = e.name();
            if (n.contains("STATUS")) return 1;
            if (n.contains("LOGIN")) return 2;
            if (n.contains("TRANSFER")) return 3;
            return -1;
        }
        return -1;
    }

    /**
     * 是否是登录阶段的首包（login start）。
     *
     * <p>只在「握手包读不出 nextState」时作为兜底判据使用。刻意不匹配
     * {@code ClientIntention}，避免把 status ping 误判成登录。</p>
     */
    private static boolean isLoginStartPacket(Object msg) {
        if (msg == null) {
            return false;
        }
        String cn = msg.getClass().getSimpleName();
        return cn.contains("HelloPacket") || cn.contains("LoginStart") || cn.contains("ServerboundKey");
    }

    private static int readInt(Object target, String... names) {
        Object v = readAny(target, names);
        return v instanceof Number n ? n.intValue() : 0;
    }

    /**
     * 读取字节负载，兼容包装类型。
     *
     * <p>⚠️ 实测（Paper 26.2）：应答包的字段是
     * {@code payload: CustomQueryAnswerPayload}（包装对象），**不是 byte[]**。
     * 早先按 byte[] 直读失败后走了默认值 2，导致被误判成"客户端保持原版"。
     * 这里多下探一层，从包装对象里取出真正的字节。</p>
     */
    private static byte[] readBytes(Object target, String... names) {
        Object v = readAny(target, names);
        if (v instanceof java.util.Optional<?> opt) v = opt.orElse(null);
        if (v instanceof byte[] b) return b;
        if (v == null) return null;
        for (String n : new String[]{"buffer", "data", "bytes", "payload", "value", "contents", "buf"}) {
            Object inner = readAny(v, n);
            if (inner instanceof java.util.Optional<?> o2) inner = o2.orElse(null);
            if (inner instanceof byte[] b2) return b2;
            // 实测（Paper 26.2）：负载放在 FriendlyByteBuf 里
            if (inner instanceof ByteBuf bb) {
                byte[] out = new byte[bb.readableBytes()];
                bb.getBytes(bb.readerIndex(), out);
                return out;
            }
        }
        LOGGER.debug("[Zstd] 未能从 {} 取出应答字节（该包的负载结构可能又变了）", v.getClass().getName());
        return null;
    }

    private static Object readAny(Object target, String... names) {
        Class<?> c = target.getClass();
        while (c != null && c != Object.class) {
            for (String n : names) {
                try {
                    Field f = c.getDeclaredField(n);
                    f.setAccessible(true);
                    return f.get(target);
                } catch (Throwable ignored) {
                    // 试下一个候选名
                }
            }
            c = c.getSuperclass();
        }
        return null;
    }

    private static Class<?> tryLoad(String name) {
        try {
            return Class.forName(name);
        } catch (Throwable t) {
            return null;
        }
    }
}
