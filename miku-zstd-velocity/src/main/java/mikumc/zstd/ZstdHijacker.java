package mikumc.zstd;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.util.ReferenceCountUtil;
import mikumc.zstd.protocol.ZstdNegotiateStatus;
import mikumc.zstd.protocol.ZstdVarInts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * 出站嗅探/协商处理器（Velocity 端核心调度）。
 *
 * <p>职责（按生命周期）：</p>
 * <ol>
 *   <li><b>协商</b>：首个出站包写出前发送 {@code zstd:negotiate} 登录查询
 *       （该时刻客户端登录监听器必然就绪）；</li>
 *   <li><b>应答</b>：拦截客户端回包 LoginPluginResponsePacket（txId 匹配；Velocity 内部类型，
 *       反射访问，见下方字段注释），解析后再回填
 *       {@link ZstdChannelManager#markNegotiateResponse}——该包的 txId
 *       不属于 Velocity 内核 inflight 列表，在此消费最干净；</li>
 *   <li><b>激活</b>：检测到 SetCompression 写出时启动管线接管（插入而非替换）。激活前按门控
 *       等待协商应答（100ms × 20 重试，超时/版本不匹配则放弃 zstd 回落原版）；
 *       激活完成后<b>自移除</b>，热路径不再有任何开销。</li>
 * </ol>
 *
 * <h2>3.1.0 变更</h2>
 * <ul>
 *   <li>SetCompression 判定由 {@code getSimpleName().equals(...)} 改为类型判断（原为
 *       {@code instanceof}，现为 {@code Class#isInstance}——内部类型改为反射访问后
 *       拿不到编译期常量，但二者同为 JVM 内建、开销同量级）
 *       ——该判断在激活前会对每个出站包执行，字符串比较（含 getSimpleName 的反射开销）
 *       是不必要的热路径成本；</li>
 *   <li>激活前<b>先校验两端锚点齐备</b>再动手安装，避免"只装上编码器"这类
 *       单侧 zstd 的必断连组合；</li>
 *   <li>VarInt 读取统一走 {@link ZstdVarInts}。</li>
 * </ul>
 */
public class ZstdHijacker extends ChannelDuplexHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger("miku-zstd");

    // ── Velocity 内部类型的反射访问 ─────────────────────────────────────────
    //
    // 这三个类位于 velocity-proxy（内部实现），不在公开的 velocity-api 中，
    // 而 velocity-proxy.jar 也不在任何公开 Maven 仓库里。以前直接 import 它们，
    // 结果任何拿不到该 jar 的环境（包括 CI、以及刚 clone 本仓库的人）都
    // **根本无法编译这个模块**。改为反射后：编译期只依赖 velocity-api，
    // 运行期由 Velocity 自己提供这些类，只要类存在，行为与 instanceof 完全一致。
    //
    // 性能：两处判断用 Class#isInstance（JVM 内建，与 instanceof 同量级）；
    // 反射读字段只在登录阶段的少数几个包上发生，不在热路径上。
    // 类不存在时（极端的 Velocity 分支/改名）全部判为"不匹配"，安全降级为原版。
    private static final Class<?> C_SET_COMPRESSION =
            tryLoad("com.velocitypowered.proxy.protocol.packet.SetCompressionPacket");
    private static final Class<?> C_LOGIN_RESPONSE =
            tryLoad("com.velocitypowered.proxy.protocol.packet.LoginPluginResponsePacket");
    private static final Class<?> C_MINECRAFT_PACKET =
            tryLoad("com.velocitypowered.proxy.protocol.MinecraftPacket");

    private static final Method M_RESP_ID = methodOrNull(C_LOGIN_RESPONSE, "getId");
    private static final Method M_RESP_SUCCESS = methodOrNull(C_LOGIN_RESPONSE, "isSuccess");
    private static final Method M_RESP_CONTENT = methodOrNull(C_LOGIN_RESPONSE, "content");

    private static Class<?> tryLoad(String name) {
        for (ClassLoader cl : new ClassLoader[]{
                ZstdHijacker.class.getClassLoader(),
                Thread.currentThread().getContextClassLoader(),
                ClassLoader.getSystemClassLoader()}) {
            if (cl == null) {
                continue;
            }
            try {
                return Class.forName(name, false, cl);
            } catch (Throwable ignored) {
                // 换下一个类加载器
            }
        }
        return null;
    }

    private static Method methodOrNull(Class<?> owner, String name) {
        if (owner == null) {
            return null;
        }
        try {
            return owner.getMethod(name);
        } catch (Throwable ignored) {
            return null;
        }
    }

    /** 反射取值失败时返回 null —— 调用方按"不是我们要的包"处理，保持原语义。 */
    private static Object invokeOrNull(Method m, Object target) {
        if (m == null) {
            return null;
        }
        try {
            return m.invoke(target);
        } catch (Throwable t) {
            LOGGER.debug("[Zstd] reflective read {} failed: {}", m.getName(), t);
            return null;
        }
    }

    /** 锚点缺失告警的"只报一次"标志（该情形属配置问题，重复告警无价值） */
    private static final java.util.concurrent.atomic.AtomicBoolean ANCHOR_WARNED =
            new java.util.concurrent.atomic.AtomicBoolean();

    private static final String MINECRAFT_DECODER = "minecraft-decoder";
    private static final String MINECRAFT_ENCODER = "minecraft-encoder";
    private static final String COMPRESSION_DECODER = "compression-decoder";
    private static final String COMPRESSION_ENCODER = "compression-encoder";

    /** 等待协商应答的重试间隔与次数（合计约 2s） */
    private static final long RESPONSE_RETRY_MS = 100;
    private static final int RESPONSE_MAX_RETRIES = 20;

    /**
     * 取"发送登录期查询包"的写入位置：<b>只能用 {@code minecraft-encoder} 的 context</b>。
     *
     * <p>⚠️ 这里曾经优先用 {@code compression-encoder} 的 context（还在其上手工拼 MC 压缩帧）。
     * 那条路是错的：管线里补<b>外层帧长度</b>的处理器加入得更早（在 compression-encoder 之前），
     * 写在 compression-encoder 的 context 上会绕过它 —— 实测客户端直接抛
     * {@code CorruptedFrameException: Frame length cannot be zero}（它把我们的首字节当成了帧长度）。</p>
     *
     * <p>写在 minecraft-encoder 的 context 上则同时满足两件事：
     * ①外层长度由更早加入的处理器补齐；②压缩一旦启用，原版 compression-encoder 会<b>自己按阈值</b>
     * 处理（小包直存 / 大包 deflate）—— 所以我们既不需要、也不应该手工拼 MC 的压缩帧。</p>
     *
     * <p>⚠️ 协议 v7 把编码器改成"插在原版压缩处理器旁边"之后，这里（negotiate）
     * <b>仍然不能</b>挪到 {@code compression-encoder} 的 context：它写的是<b>裸 MC 包</b>，
     * 长度前缀与压缩都得靠管线补齐；从压缩处理器之后写出等于把这一层补全一起跳过，
     * 又回到上面那个"帧长为 0"的崩法。真正的"写帧"位置是另一个方法
     *（见 {@link #compressionWriteCtx}）—— 两者的区别是"写包"还是"写帧"。</p>
     *
     * @return 写入位置；{@code null} 表示锚点不存在（极端的管线差异），调用方按"发不出去"处理
     */
    private static ChannelHandlerContext loginQueryWriteCtx(ChannelPipeline pipeline) {
        return pipeline.context(MINECRAFT_ENCODER);
    }

    private boolean pipelineActivated;

    /** 本处理器在管道中的上下文（handlerAdded 时记录），用于被动触发激活 */
    private ChannelHandlerContext selfCtx;

    /** 激活结论未定期间扣下的出站包（见 write() 中的说明） */
    private final List<Object> heldPackets = new ArrayList<>();
    private final List<ChannelPromise> heldPromises = new ArrayList<>();
    private boolean holding;

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        this.selfCtx = ctx;
        // 记录类来源：plugins/ 下若存在同 id 的多个 jar，Velocity 只会加载其一，
        // 此时"改了代码却没生效"极难察觉——这行日志可以直接确认加载的是哪个 jar
        LOGGER.debug("[Zstd] ZstdHijacker loaded from {}",
                ZstdHijacker.class.getProtectionDomain().getCodeSource());
    }

    /**
     * 由 {@link ZstdNegotiateAnswerSniffer} 在捕获到协商应答后调用，
     * 立即触发激活——省掉最长 100ms 的轮询等待。
     *
     * <p>只有当 SetCompression 已经写出（{@code pipelineActivated}）时才动手：
     * 否则我们会先于 Velocity 的 setupCompression 改动管线，随后被原版压缩处理器覆盖。</p>
     */
    public void onAnswerReceived() {
        ChannelHandlerContext c = this.selfCtx;
        if (c == null || !pipelineActivated) return;
        c.channel().eventLoop().execute(() -> tryActivate(c, 0));
    }

    /** 供嗅探器按通道调用（无处理器时静默忽略）。 */
    public static void notifyAnswerReceived(io.netty.channel.Channel channel) {
        ZstdHijacker h = channel.pipeline().get(ZstdHijacker.class);
        if (h != null) h.onAnswerReceived();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ZstdChannelManager mgr = ctx.channel().attr(ZstdChannelManager.KEY).get();
        if (LOGGER.isDebugEnabled()) {
            // 登录阶段的入站包诊断：定位"应答没被拦到"这类问题时非常有用。
            // 激活后本处理器会自移除，因此这里的量级只限于登录阶段的少量包。
            LOGGER.debug("[Zstd] spy inbound: {} (mgr={}, txId={})",
                    msg.getClass().getName(), mgr != null, mgr == null ? -1 : mgr.getNegotiateTxId());
        }
        if (mgr != null && C_LOGIN_RESPONSE != null && C_LOGIN_RESPONSE.isInstance(msg)) {
            // txId 必须非 0：未发起协商时字段默认为 0，若不加判断，
            // 其它插件发起的、txId 恰为 0 的登录查询应答会被我们误吞
            Object idValue = invokeOrNull(M_RESP_ID, msg);
            int txId = idValue instanceof Number n ? n.intValue() : 0;
            if (mgr.getNegotiateTxId() != 0 && txId == mgr.getNegotiateTxId()) {
                try {
                    Object contentValue = invokeOrNull(M_RESP_CONTENT, msg);
                    ByteBuf data = contentValue instanceof ByteBuf bb ? bb : null;
                    boolean success = Boolean.TRUE.equals(invokeOrNull(M_RESP_SUCCESS, msg));
                    int status = ZstdNegotiateStatus.FALLBACK;
                    if (success && data != null && data.readableBytes() >= 1) {
                        status = readStatus(data);
                    }
                    mgr.markNegotiateResponse(status, success);
                    // 立即触发激活（若 SetCompression 已写出），省掉最长 100ms 的轮询等待
                    ZstdHijacker.notifyAnswerReceived(ctx.channel());
                } finally {
                    ReferenceCountUtil.release(msg);
                }
                return; // 消费该包，不下传（txId 不属于内核 inflight，下传只会产生噪音）
            }
        }
        super.channelRead(ctx, msg);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        ZstdChannelManager mgr = ctx.channel().attr(ZstdChannelManager.KEY).get();
        if (LOGGER.isDebugEnabled() && mgr != null && !mgr.isReplaced()) {
            // 出站诊断：定位"激活窗口内的包走了哪条编码路径"
            LOGGER.debug("[Zstd] spy outbound: {} (holding={}, activated={})",
                    msg.getClass().getSimpleName(), holding, pipelineActivated);
        }

        // ⚠️ 服务器列表 ping 绝不能走协商：STATUS 协议里没有 LoginPluginRequest(0x04)，
        // 客户端会把它当成未知包解析失败，服务器在列表里直接显示为离线。
        // 事件兜底路径拿不到 nextState，这里用"首个出站包是不是 StatusResponse"来判 ——
        // ping 连接的首包必然是它，而登录连接的首包是 EncryptionRequest / SetCompression。
        if (mgr != null && !mgr.isNegotiationSent() && isStatusResponse(msg)) {
            LOGGER.debug("[Zstd] first outbound is a status response — not a login connection, skipping zstd");
            if (ctx.pipeline().get(ZstdHijacker.class) != null) {
                ctx.pipeline().remove(this);
            }
            super.write(ctx, msg, promise);
            return;
        }

        // ⚠️ 只在 SetCompression（且阈值 >= 0）时发 negotiate，不能对"任意首个出站包"发。
        // 后者会把 LoginPluginRequest 发给不支持登录插件通道的极简服务端（limbo 登录服就是
        // 这样：它只实现 handshake + login start，收到 0x04 直接断连），表现为玩家进 limbo
        // 两秒后被踢、而两端日志里都没有异常。真实服务端在登录阶段必发 SetCompression，
        // 阈值 < 0 表示它已禁用压缩，也就不需要 zstd。
        if (C_SET_COMPRESSION != null && C_SET_COMPRESSION.isInstance(msg)) {
            int threshold = readThresholdValue(msg);
            if (threshold != UNKNOWN_THRESHOLD && threshold < 0) {
                LOGGER.debug("[Zstd] peer disabled compression (threshold<0) — not negotiating");
                if (ctx.pipeline().get(ZstdHijacker.class) != null) {
                    ctx.pipeline().remove(this);
                }
                super.write(ctx, msg, promise);
                return;
            }
            if (mgr != null && !mgr.isNegotiationSent()) {
                sendNegotiateNow(ctx.channel());
                mgr = ctx.channel().attr(ZstdChannelManager.KEY).get();
            }
        }

        if (mgr == null || mgr.isReplaced()) {
            super.write(ctx, msg, promise);
            return;
        }

        // ⚠️ 第二次及以后的 SetCompression 会被这里放过（pipelineActivated 已置位）。
        // 它来自"同一条连接重新走登录"的场景：离线玩家经 limbo 登录服验证后转后端、
        // 或代理重设压缩阈值。此刻客户端会重装原版编解码器，必须让日志能看出发生过这件事，
        // 否则现场只有一句 lost connection，无从判断。
        if (pipelineActivated && C_SET_COMPRESSION != null && C_SET_COMPRESSION.isInstance(msg)) {
            int threshold = readThresholdValue(msg);
            LOGGER.info("[Zstd] 同一条连接收到第二次 SetCompression（threshold={}）；pipeline={}",
                    threshold, ctx.channel().pipeline().names());
            if (threshold != UNKNOWN_THRESHOLD && threshold < 0) {
                // 对端要"关掉压缩"（limbo 登录服就是这样把连接降回明文的）。
                // 本端若继续用 zstd 发帧，对端会按明文读 —— 第一帧就错乱。
                LOGGER.info("[Zstd] 压缩被禁用（threshold<0）——本端退回明文");
                super.write(ctx, msg, promise); // 先把这条包原样送出，再让位
                giveUp(ctx);
                return;
            }
        }

        if (!pipelineActivated && C_SET_COMPRESSION != null && C_SET_COMPRESSION.isInstance(msg)) {
            pipelineActivated = true;
            LOGGER.debug("[Zstd] SetCompression detected — deferring zstd activation until negotiate response; pipeline={}",
                    ctx.channel().pipeline().names());
            // ⚠️ 必须同步进入 hold：Velocity 在写完 SetCompression 后会立刻写 LoginSuccess，
            // 而激活要等协商应答（一个 RTT）。若不拦下这段窗口内的包，它们会走原版压缩编码器
            // 发出去，而客户端收到 SetCompression 后已经把解码器换成了 zstd —— 原版压缩帧
            // （[dataLength>0][deflate]）会被 zstd 解码器按 zstd 帧解析，必然错乱/断连。
            //（未压缩帧 [0][packet] 与 zstd 直存帧逐字节同形、不在其中；但 LoginSuccess 这类
            // 数据量较大的包会被原版压缩器真正压缩，所以整段窗口都必须扣住。）
            holding = true;
            super.write(ctx, msg, promise); // SetCompression 本身原样透传（此刻尚未压缩）
            ctx.channel().eventLoop().execute(() -> tryActivate(ctx, 0));
            return;
        }

        if (holding) {
            // ⚠️ 只扣 MC 包。管道里还会流经 netty 自身的控制消息（例如协议阶段切换用的
            // UnconfiguredPipelineHandler$OutboundConfigurationTask，一个 Runnable）——
            // 一旦把它们扣住，服务端的协议阶段切换会被阻塞、连接随即断开，
            // 而且日志里没有任何异常，极难定位（在 Paper 端移植时实测踩到过）。
            if (C_MINECRAFT_PACKET != null && C_MINECRAFT_PACKET.isInstance(msg)) {
                // 激活结论未定：先扣住，等 activateZstd / 回落原版后再按正确的编码路径补发
                heldPackets.add(msg);
                heldPromises.add(promise);
                return;
            }
            super.write(ctx, msg, promise);
            return;
        }

        // 兜底：回落原版之后（本处理器已自移除）若 SetCompression 才被写出，
        // 客户端已放弃 zstd、继续用原版帧 —— 而本端不会再有任何人切回 zstd。
        if (C_SET_COMPRESSION != null && C_SET_COMPRESSION.isInstance(msg)) {
            LOGGER.warn("[Zstd] SetCompression written after zstd was skipped — "
                    + "dropping negotiation so both sides stay vanilla");
            ZstdChannelManager m = ctx.channel().attr(ZstdChannelManager.KEY).get();
            if (m != null) {
                m.setProtocolMismatch();
            }
        }

        super.write(ctx, msg, promise);
    }

    /**
     * 通道关闭时把扣住的包与 promise 一起收尾。
     *
     * <p>⚠️ <b>两件事都必须做，缺一件各有一个隐蔽后果：</b></p>
     * <ul>
     *   <li><b>不补 promise</b>：扣包时把调用方给的 promise 存进了 {@link #heldPromises}，
     *       只有 {@code releaseHeld} 才会让它们被 netty 完成。若连接在激活窗口内断开
     *       （客户端掉线、超时），{@code tryActivate} 会因为 {@code !channel.isActive()}
     *       提前返回，那些 promise 就再也没人完成——等待它们的一方会永久挂住。
     *       这类问题不报错、不泄漏堆，只表现为"偶发卡死"；</li>
     *   <li><b>不 release 包</b>：这些包是 ReferenceCounted 的（内部持有 ByteBuf），
     *       扣下时所有权已转到本处理器。它们永远不会被写出，因此不会有人替我们释放——
     *       每次"激活窗口内断线"都会泄漏一批（Netty leak detector 会报 LEAK）。</li>
     * </ul>
     */
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        failHeldPromises(new ClosedChannelException());
        super.channelInactive(ctx);
    }

    /**
     * 本处理器被<b>外部</b>从管线摘除时的收尾（插件卸载 / 管线重建）。
     *
     * <p>⚠️ 这是 {@code channelInactive} 之外<b>另一条</b>独立的销毁路径：通道仍然活着，
     * 但处理器已经不在管线里。此时 {@code channelInactive} 永远不会触发，而激活流程也
     * 因为 {@link #pipelineRemoved(Channel)} 直接 return —— 扣住的包与 promise 无人收尾：
     * 包是 ReferenceCounted 的（不释放就是泄漏，Netty leak detector 会报 LEAK），
     * promise 不完成则上游 future 永久挂起。</p>
     *
     * <p>Paper 端从一开始就有这段收尾（见 {@code ZstdPaperNegotiator#handlerRemoved}），
     * Velocity 端此前缺失，属同类销毁路径只覆盖了一半。</p>
     *
     * <p>正常激活路径不受影响：那时调用方已经 {@code releaseHeld}，列表为空，本方法幂等。</p>
     */
    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        failHeldPromises(new ClosedChannelException());
        super.handlerRemoved(ctx);
    }

    /** 用给定异常补完所有扣住的 promise（幂等：列表清空后重复调用无副作用）。 */
    private void failHeldPromises(Throwable cause) {
        holding = false;
        // ⚠️ 顺序不能反：先把包本身释放掉，再补 promise。
        // 这些包是 ReferenceCounted 的（内部持有 ByteBuf），扣下时所有权就转到我们手上；
        // 既然永远不会被写出，就必须由我们 release，否则每次"激活窗口内断线"都泄漏一批。
        releaseHeldPackets();
        if (heldPromises.isEmpty()) {
            return;
        }
        LOGGER.debug("[Zstd] channel closed with {} held packet(s) — failing their promises",
                heldPromises.size());
        for (ChannelPromise p : heldPromises) {
            if (p != null) {
                p.tryFailure(cause);
            }
        }
        heldPromises.clear();
    }

    /**
     * 释放扣住的包本身（{@link #heldPackets}），不碰 promise。
     *
     * <p>只用于"这些包永远不会被写出"的路径（连接关闭）。正常补发走 {@link #releaseHeld}，
     * 那条路上包会被交给管线，由管线负责释放——两处不要混淆，否则会双重释放。</p>
     */
    private void releaseHeldPackets() {
        if (heldPackets.isEmpty()) {
            return;
        }
        for (Object p : heldPackets) {
            ReferenceCountUtil.release(p);
        }
        heldPackets.clear();
    }

    /**
     * 补发 hold 期间扣下的包。
     *
     * <p>调用时机必须是"管道已定型"之后：激活成功时 zstd 编解码器已就位，
     * 回落原版时原版压缩编码器仍在位——两种情形下都只需按原路径写出即可。</p>
     */
    /**
     * 回落原版 + 摘掉自身处理器。
     *
     * <p>⚠️ 回落分支<b>必须</b>走这里：只 releaseHeld 会让这个 duplex handler 永久留在
     * 每个出站包的热路径上（attr 查询 + SetCompression 判定 + debug 日志），
     * 并持续持有本插件的类加载器。类注释承诺的"激活后自移除"在回落路径上同样成立。</p>
     */
    private void giveUp(ChannelHandlerContext ctx) {
        releaseHeld(ctx);
        if (ctx.pipeline().get(ZstdHijacker.class) != null) {
            ctx.pipeline().remove(this);
        }
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

    /**
     * 门控激活入口：包一层异常兜底后再进 {@link #tryActivate0}。
     *
     * <p>⚠️ 这层兜底不能省。本方法跑在 {@code eventLoop().execute/schedule} 里，
     * 抛出的异常会被 event loop 吞掉，于是 {@link #releaseHeld} <b>永不执行</b>——
     * 激活窗口内扣住的包与 promise 会永久挂起，客户端表现为<b>连接卡死</b>（而不是断连），
     * 日志里只有一行堆栈甚至什么都没有。Paper 端一直有这层保护，本端曾有缺口。</p>
     */
    private void tryActivate(ChannelHandlerContext ctx, int attempt) {
        try {
            tryActivate0(ctx, attempt);
        } catch (Throwable t) {
            LOGGER.error("[Zstd] activation flow failed (attempt={}) — releasing held packets, staying vanilla",
                    attempt, t);
            // ⚠️ 安装只做到一半就抛异常时，管线里可能躺着 zstd_encoder/zstd_decoder：
            // 它们会继续按 zstd 编解码，而客户端还没收到 zstd:switch，仍然按原版解释入站帧。
            // 这里必须先把它们摘干净，再补发扣住的包（补发走的才是完整原版路径）。
            // ⚠️ 判据是 isReplaced()：一旦已激活成功（切换通知也已发出），对端就是 zstd 了，
            // 此时拆掉我们的处理器才是真正的单侧 zstd —— 那种情况只能保持现状。
            ZstdChannelManager m = ctx.channel().attr(ZstdChannelManager.KEY).get();
            if (m != null && !m.isReplaced()) {
                m.uninstallZstd(ctx.channel());
            }
            releaseHeld(ctx);
            // ⚠️ 同时摘掉自身：本端与 Paper 不同，回落分支若不显式移除，处理器就会
            // 永久留在每个出站包的热路径上（write 里每次都做 attr 查询与 SetCompression 判定），
            // 并持续持有本插件的类加载器。
            if (ctx.pipeline().get(ZstdHijacker.class) != null) {
                ctx.pipeline().remove(this);
            }
        }
    }

    /** 门控激活：等待协商应答；客户端保持原版/超时则放弃（原版 zlib 全程接管）。 */
    private void tryActivate0(ChannelHandlerContext ctx, int attempt) {
        Channel channel = ctx.channel();
        if (!channel.isActive() || pipelineRemoved(channel)) return;

        ZstdChannelManager mgr = channel.attr(ZstdChannelManager.KEY).get();
        if (mgr == null || mgr.isReplaced()) return;

        if (!mgr.isAnswerReceived()) {
            // 协议 v8 只剩一段等待：negotiate 的应答（一个 RTT）。原来"字典推送 → 等字典应答"
            // 那一段 10s 预算随训练字典一起删掉了，所以这里只需一个 2s 上限。
            if (attempt >= RESPONSE_MAX_RETRIES) {
                LOGGER.warn("[Zstd] Negotiate response not received after {}ms — closing connection"
                                + "（客户端没有回 packet id 0x02 的 login plugin 应答）",
                        RESPONSE_RETRY_MS * RESPONSE_MAX_RETRIES);
                // ⚠️ 完全没应答只可能是异常（原版客户端也会回 success=false 走 isProtocolMismatch）。
                // 按原版补发扣包时，对端若其实是模组客户端（应答丢了），它已切 zstd → 必断连。
                // 明确关连接，让客户端重连一次，故障也更可见。
                releaseHeld(ctx);
                ctx.channel().close();
                if (ctx.pipeline().get(ZstdHijacker.class) != null) {
                    ctx.pipeline().remove(this);
                }
                return;
            }
            channel.eventLoop().schedule(() -> tryActivate(ctx, attempt + 1),
                    RESPONSE_RETRY_MS, TimeUnit.MILLISECONDS);
            return;
        }

        if (mgr.isProtocolMismatch()) {
            LOGGER.warn("[Zstd] Protocol mismatch reported by client — skipping zstd (vanilla fallback)");
            giveUp(ctx); // 回落原版：补发扣下的包 + 摘掉自身
            return;
        }

        // ⚠️ 门控不能只看"应答已到"：客户端答复"保持原版"时同样已经应答，
        // 但此时激活就是单侧 zstd（服务端 zstd / 客户端 vanilla → 必断连）。
        // 唯一正面判据是 status == 0（isClientReady）。
        if (!mgr.isClientReady()) {
            LOGGER.warn("[Zstd] 客户端未答复就绪（status!=0）— 放弃 zstd 并回落原版，避免单侧 zstd 断连");
            giveUp(ctx); // 回落原版：补发扣下的包 + 摘掉自身
            return;
        }

        // 只有"确实激活成功"才补发；重试 / 回落原版这两条路径已由 activateZstd 自行收尾
        //（见该方法的返回值约定）。
        if (activateZstd(ctx, mgr)) {
            releaseHeld(ctx); // 激活路径内已补发；此处为空操作，保留以防调用顺序变动
        }
    }

    private boolean pipelineRemoved(Channel channel) {
        return channel.pipeline().get(ZstdHijacker.class) == null;
    }

    /**
     * 安装 zstd 编解码器并置位激活状态。
     *
     * <p>⚠️ <b>返回值语义必须严格遵守</b>，它是"扣住的包该不该现在补发"的唯一判据：</p>
     * <ul>
     *   <li>{@code true}：激活成功，管道已定型为 zstd —— 调用方应 {@code releaseHeld}
     *       按 zstd 路径补发；</li>
     *   <li>{@code false}：本方法<b>已自行收尾</b>（排定了重试，或已补发并回落原版），
     *       调用方<b>不得</b>再动扣住的包。</li>
     * </ul>
     * <p>曾经调用方是无条件 {@code releaseHeld}：重试路径上（锚点尚未就绪，这在原版
     * SetCompression 之后是常态）会把扣住的包<b>提前</b>用尚未定型、甚至仍是原版压缩的
     * 路径发出去，同时把 {@code holding} 置假——激活窗口的门控就此失效，正是 v4 批量帧
     * 与原版帧混发导致客户端解析错乱的成因。</p>
     */
    private boolean activateZstd(ChannelHandlerContext ctx, ZstdChannelManager mgr) {
        return installHandlers(ctx, mgr, 0);
    }

    /**
     * 安装阶段的独立重试循环（预算 {@link #RESPONSE_MAX_RETRIES}，与应答等待阶段互不影响）。
     *
     * <p>⚠️ 必须与 {@code tryActivate0} 的 attempt 分开计数：那个 attempt 指的是
     * "等协商/字典应答等了多少轮"，字典阶段可等 100 轮。若共用，慢链路上进入安装阶段时
     * attempt 已 > 20，安装重试会被立刻判为耗尽 → 明明锚点马上就好，却直接回落原版。</p>
     *
     * @param installAttempt 安装阶段自己的重试序号（首轮 0）
     * @return 语义同 {@link #activateZstd}：true = 已激活，调用方应补发扣包
     */
    private boolean installHandlers(ChannelHandlerContext ctx, ZstdChannelManager mgr, int installAttempt) {
        Channel channel = ctx.channel();
        ChannelPipeline p = channel.pipeline();

        if (p.get(MINECRAFT_DECODER) == null || p.get(MINECRAFT_ENCODER) == null) {
            LOGGER.warn("[Zstd] Cannot activate zstd — not a frontend Minecraft channel");
            giveUp(ctx); // 不可激活：补发扣住的包 + 摘掉自身，否则连接卡死
            return false;
        }
        // 先确认两端锚点都齐备，再动手——避免只装上一侧造成单侧 zstd 的必断连组合
        if (!hasEncoderAnchor(p) || !hasDecoderAnchor(p)) {
            LOGGER.error("[Zstd] Cannot activate zstd — pipeline anchors missing (encoder={}, decoder={}); "
                    + "pipeline={}", hasEncoderAnchor(p), hasDecoderAnchor(p), p.names());
            giveUp(ctx); // 同上：不能把包一直扣着
            return false;
        }

        // 这里原先要"按门控装载训练字典"，5.0.42 起训练字典整体删除：跨帧重复由协议基类的
        // 滚动历史（每方向 64KB 滑窗）在帧边界自动维护，本端不需要任何准备动作。

        // ⚠️ 先在**代码路径上**把两个处理器都装上，成功了才置 replaced。
        //
        // 曾经这里先 setReplaced(true) 再安装：安装失败走重试时，{@code tryActivate0}
        // 开头的 {@code mgr.isReplaced()} 早退会把重试**整个吃掉**（replaced 已为 true
        // → 直接 return），于是 ①重试形同虚设；②那条 return 不会 releaseHeld，
        // 激活窗口内扣住的包与 promise 永挂 —— 客户端表现为**连接卡死**（不是断连），
        // 日志里只有一行 ERROR；③监控里的"使用人数"还被错误 +1 且永不回落。
        // Paper 端一直是"装完再置位"（见 ZstdPaperNegotiator），此处与它对齐。
        if (!mgr.installHandlers(channel)) {
            // ⚠️ 两件事都必须做：
            // 1) 重试——原版压缩处理器是 SetCompression 之后才装的，可能尚未就绪；
            // 2) 重试用尽后必须 releaseHeld——否则 hold 期间扣下的包永不补发，
            //    客户端表现为**连接卡死**（而不是断连），且日志里只有一行 ERROR，极难排查。
            if (installAttempt < RESPONSE_MAX_RETRIES) {
                channel.eventLoop().schedule(() -> installHandlers(ctx, mgr, installAttempt + 1),
                        RESPONSE_RETRY_MS, TimeUnit.MILLISECONDS);
                return false; // 重试期间保持 holding：包必须留到管道定型后再发
            }
            LOGGER.error("[Zstd] zstd handler installation failed after {} attempts; pipeline={}",
                    installAttempt, p.names());
            // ⚠️ 不再有"半装死局"：v7 起原版压缩处理器从来没被摘掉（只插入、不替换），
            // 所以"只装上编码器、解码器没装上"这种中间态是可以撤销的 —— 把我们的四个
            // handler 全摘掉，管线就回到完整原版形态。
            // 这一步必须做：残留的 zstd_encoder 会继续把出站包编成 zstd 帧，
            // 而客户端此刻还没收到 zstd:switch（那条通知在安装成功之后才发），
            // 它按原版解释入站帧 → 第一帧就错乱。
            mgr.uninstallZstd(channel);
            giveUp(ctx); // 回落原版：补发扣住的包 + 摘掉自身（此时管线已是完整原版）
            return false;
        }

        mgr.setReplaced(true); // 计数已内聚在 setReplaced 内（幂等）

        // ⚠️ 字段名必须如实：这里打印的是"客户端答了就绪"（status==0），不是"装了字典"。
        // 训练字典已删除，任何形如 dict=true 的标签都只会把排查方向带偏。
        LOGGER.info("[Zstd] Zstd transport activated (client-ready={})", mgr.isClientReady());

        // ⚠️ 切换闸门此刻正扣着登录期的大帧（例如 LoginSuccess 压缩帧），
        // 客户端在收到它之前不可能完成登录，也就没有任何东西会触发它的切换。
        // 因此必须主动补一条"服务端已就绪、请切换"的通知 —— 客户端据此切换并发确认。
        sendSwitchRequest(channel);

        // ⚠️ 顺序必须【先补发、后自移除】：handlerRemoved 会把"仍被扣住的包"当作销毁
        // 泄漏收尾（释放 + 失败化 promise）。若先移除，这些包就永远发不出去——登录阶段的
        // LoginSuccess 等会直接被丢弃，玩家表现为卡在登录。Paper 端一直就是这个顺序。
        // （releaseHeld 会清空列表，调用方再调一次是空操作。）
        releaseHeld(ctx);

        // 使命完成，自移除——热路径（每个出站包）不再经过本处理器
        if (p.get(ZstdHijacker.class) != null) {
            p.remove(this);
        }
        // 管道已定型为 zstd：调用方据此补发扣住的包（此处已补发，为空操作）
        return true;
    }

    /**
     * 出站方向是否有**可插入**的锚点。
     *
     * <p>⚠️ 必须与 {@link ZstdChannelManager#installEncoder} 支持的锚点集合完全一致。</p>
     *
     * <p>只认原版<b>压缩</b>编码器：本端的插法是"加在它 tail 侧、再从它的 context 往下写"，
     * 它的存在就是这套写法的前提。{@code cipher-encoder} 在它 head 侧（更早被加密，
     * 那我们就得自己处理加密层），{@code minecraft-encoder} 拿到的还是 MC 包对象而不是帧。
     * 若把这两种也算成锚点，门控会认为"可以装"、实际必然失败，白白重试 20 次才回落。</p>
     */
    private static boolean hasEncoderAnchor(ChannelPipeline p) {
        return p.get("zstd_encoder") != null || p.get(COMPRESSION_ENCODER) != null
                || p.get("compress") != null;
    }

    /**
     * 入站方向是否有**可插入**的锚点。
     *
     * <p>⚠️ 同上：绝不能把 {@code minecraft-decoder} 算作锚点。在它之前插入一层会让
     * 本解码器输出的包被它再当成外层帧解析一次，症状是 Velocity 打出
     * "A packet frame decoder failed" 并断连。</p>
     */
    private static boolean hasDecoderAnchor(ChannelPipeline p) {
        return p.get("zstd_decoder") != null || p.get(COMPRESSION_DECODER) != null
                || p.get("decompress") != null;
    }

    /**
     * "已经成好形的帧"该从哪个 context 写出：原版压缩处理器的 context。
     *
     * <p>⚠️ 这一步不能省：从 {@code zstd_encoder} 的 ctx 往下写会先撞上原版压缩处理器，
     * 我们的帧被 zlib 再压一遍，对端把 zlib 头当成 dataLength 读 → 第一帧就解析失败。
     * 写完的帧已经是最终形态（自带 bodyLen），只该继续往下游走（加密 / socket）。</p>
     *
     * <p>两种叫法都要认（{@code compression-encoder} / {@code compress}），且必须与
     * {@code ZstdBatchEncoder#downstream} 用同一套锚点：两处一旦不一致，编码器发出的帧
     * 和这里发出的握手帧就会落在管线不同位置，其中一边必然被原版再处理一次（错层）。</p>
     *
     * <p>兜底退回 {@code zstd_encoder} 的 context：走到那说明原版压缩处理器已被第三方插件
     * 摘走，管线里不再有能把这一帧再压一遍的处理器，从我们自己的 ctx 往下写仍是最终形态。</p>
     */
    private static ChannelHandlerContext compressionWriteCtx(ChannelPipeline pipeline) {
        ChannelHandlerContext ctx = pipeline.context(COMPRESSION_ENCODER);
        if (ctx == null) ctx = pipeline.context("compress");
        return ctx != null ? ctx : pipeline.context("zstd_encoder");
    }

    /**
     * 立即发送 zstd:negotiate（login 阶段 custom query，裸 ByteBuf 手工编码）。
     * 写入位置见 {@link #loginQueryWriteCtx}：只有 {@code minecraft-encoder} 的 context 会被补上
     * 外层帧长度；压缩未启用时它就是裸帧，压缩启用后由原版 compress 处理器按阈值处理。
     */
    public static void sendNegotiateNow(Channel channel) {
        try {
            ChannelHandlerContext writeCtx = loginQueryWriteCtx(channel.pipeline());
            if (writeCtx == null) {
                // 只报一次：本方法在协商成功前会被每个出站包调用，无条件告警会刷屏
                if (ANCHOR_WARNED.compareAndSet(false, true)) {
                    LOGGER.warn("[Zstd] minecraft-encoder not found, cannot send negotiate (logged once)");
                }
                return;
            }

            ZstdChannelManager mgr = channel.attr(ZstdChannelManager.KEY).get();
            if (mgr != null && mgr.isNegotiationSent()) {
                return;
            }

            int txId = ThreadLocalRandom.current().nextInt(1, Integer.MAX_VALUE);
            if (mgr != null) {
                mgr.setNegotiationSent(true);
                mgr.setNegotiateTxId(txId);
            }

            ByteBuf buf = writeCtx.alloc().buffer();
            try {
                ZstdVarInts.write(buf, 0x04);
                ZstdVarInts.write(buf, txId);
                ZstdVarInts.writeString(buf, "zstd:negotiate");
                // 协议 v8：载荷只有协议版本（4 字节大端）。字典 id / flags 随训练字典一起删掉了，
                // 客户端只需回一个状态字节，告诉本端"就绪"还是"保持原版"。
                buf.writeInt(ZstdChannelManager.PROTOCOL_VERSION);

                writeCtx.writeAndFlush(buf);
                LOGGER.debug("[Zstd] Sent zstd:negotiate txId={} version={}",
                        txId, ZstdChannelManager.PROTOCOL_VERSION);
            } catch (Throwable t) {
                if (buf.refCnt() > 0) buf.release();
                throw t;
            }
        } catch (Exception e) {
            LOGGER.error("[Zstd] Failed to send negotiate", e);
        }
    }

    /** 反射读不到阈值时的哨兵：与"对端真的传了 -1"区分开，后者才是禁用压缩。 */
    private static final int UNKNOWN_THRESHOLD = Integer.MIN_VALUE;

    /**
     * 读 SetCompressionPacket 的阈值。
     *
     * <p>⚠️ 读不到时必须返回 {@link #UNKNOWN_THRESHOLD} 而不是负数：SetCompression 出现
     * 本身就说明对端要开压缩，若把"反射失败"当成"阈值 -1（禁用）"，一次字段改名就会让
     * 全体模组客户端的 zstd 静默失效，而日志里只有一行 debug。</p>
     */
    private static int readThresholdValue(Object packet) {
        try {
            for (java.lang.reflect.Method m : packet.getClass().getMethods()) {
                if (m.getParameterCount() == 0
                        && (m.getName().equals("getThreshold") || m.getName().equals("threshold"))) {
                    return ((Number) m.invoke(packet)).intValue();
                }
            }
        } catch (Throwable ignored) {
            // 反射不可用：按"未知"处理，继续协商
        }
        return UNKNOWN_THRESHOLD;
    }

    /**
     * 通知客户端"服务端已切换到 zstd，请切换并发确认"（{@code zstd:switch}，载荷 4 字节魔数）。
     *
     * <p>⚠️ <b>从原版压缩处理器的 context 写出</b>（与 {@code ZstdBatchEncoder#downstream}
     * 同一个位置）：本帧是<b>已经成好形</b>的 zstd 直存帧，不能再让任何编码器处理它；
     * 对端（已切换到 zstd 的解码器）按直存帧读出裸包。</p>
     *
     * <p>⚠️ 接收方要看<b>包 id 之后</b>的魔数，而不是通道名：本端写的是完整登录查询包
     * {@code [0x04][txId][channelName][magic]}，但客户端一侧的"裸包"只包含
     * {@code [txId][channelName][magic]}（包 id 已被帧层吃掉）。</p>
     *
     * @return 是否确实发出
     */
    static boolean sendSwitchRequest(Channel channel) {
        // ⚠️ 写出位置是<b>原版压缩处理器的 context</b>，理由逐条：
        //   · 本帧已是最终形态的 zstd 直存帧，不能再被任何编码器处理一次；
        //   · 从 zstd_encoder 的 ctx 写会落到原版压缩处理器上——它会把我们的帧再包一层
        //     [dataLength>0][deflate]，而客户端此刻已经切到 zstd，解出来还是 zstd 帧，
        //     解析必然错乱（2.0.0 的断连回归就是这个形态）；
        //   · 从原版压缩处理器之后写，闸门（挂在 minecraft-encoder 的 tail 侧）也不会扣住它；
        //   · Velocity 侧写外层长度的就是这个「长度 + 压缩」合一的处理器，它被跳过后没人
        //     替我们补长度，所以下面的帧必须自带 bodyLen —— 若照旧注释去"补 frame-encoder"，
        //     会写出双层长度前缀，同样回到 2.0.0 的回归形态。
        // 几十字节远低于压缩阈值，直存即可。
        ChannelHandlerContext writeCtx = compressionWriteCtx(channel.pipeline());
        if (writeCtx == null) {
            LOGGER.debug("[Zstd] cannot send switch request — compression anchor missing");
            return false;
        }
        try {
            ByteBuf packet = writeCtx.alloc().buffer();
            ByteBuf frame = writeCtx.alloc().buffer();
            try {
                int txId = ThreadLocalRandom.current().nextInt(1, Integer.MAX_VALUE);
                ZstdChannelManager mgr = channel.attr(ZstdChannelManager.KEY).get();
                if (mgr != null) mgr.expectSwitchTxId(txId);
                mikumc.zstd.protocol.ZstdStepFrames.writeQuery(packet, txId,
                        mikumc.zstd.protocol.ZstdSwitchGate.CHANNEL);

                // Velocity 端没有独立补外层长度的处理器在我们之后：外层 bodyLen 只能由
                // "写帧的一方"自带（本方法、以及 ZstdBatchEncoder 里的 writeBodyLen）。
                // 既然我们从原版压缩处理器之后写，就必须自己把 bodyLen 补上，
                // 否则客户端 frame-decoder 会把首字节 0x00 当成"帧长为 0"直接抛异常。
                // 帧格式（v6）：[bodyLen][rawSize=0][packet] —— 直存帧一帧一包、不带内层长度前缀，
                // 与原版未压缩帧逐字节同形（见 ZstdBatchEncoderBase 的帧格式说明）。
                int bodyLen = 1 /*rawSize=0*/ + packet.readableBytes();
                mikumc.zstd.protocol.ZstdVarInts.write(frame, bodyLen);
                mikumc.zstd.protocol.ZstdVarInts.write(frame, 0);
                frame.writeBytes(packet);

                writeCtx.writeAndFlush(frame);
                LOGGER.debug("[Zstd] switch request sent");
                return true;
            } catch (Throwable t) {
                if (packet.refCnt() > 0) packet.release();
                if (frame.refCnt() > 0) frame.release();
                throw t;
            } finally {
                if (packet.refCnt() > 0) packet.release();
            }
        } catch (Exception e) {
            LOGGER.debug("[Zstd] failed to send switch request: {}", e.toString());
            return false;
        }
    }

    /** 是否是服务器列表 ping 的响应（STATUS 阶段唯一的出站包）。 */
    private static boolean isStatusResponse(Object msg) {
        return msg != null && msg.getClass().getName().endsWith("StatusResponse");
    }

    /**
     * 解析应答状态；解析失败或越界一律按"不可用"({@link ZstdNegotiateStatus#VANILLA}) 处理。
     *
     * <p>必须走白名单归一化：畸形载荷里的越界值（如 7）如果被原样放行，
     * 会变成两端理解不一致的"第三种状态"——见 {@link ZstdNegotiateStatus}。</p>
     */
    private static int readStatus(ByteBuf buf) {
        int v = ZstdVarInts.tryRead(buf, Integer.MAX_VALUE);
        if (v == ZstdVarInts.NEED_MORE || v == ZstdVarInts.INVALID) {
            return ZstdNegotiateStatus.VANILLA;
        }
        return ZstdNegotiateStatus.sanitize(v);
    }
}
