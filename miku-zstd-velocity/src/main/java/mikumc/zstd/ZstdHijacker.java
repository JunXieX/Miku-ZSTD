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
 *       反射访问，见下方字段注释），解析字典状态回填
 *       {@link ZstdChannelManager#markDictResponse}——该包的 txId
 *       不属于 Velocity 内核 inflight 列表，在此消费最干净；</li>
 *   <li><b>激活</b>：检测到 SetCompression 写出时启动管线替换。激活前按门控
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
 *   <li>激活前<b>先校验两端锚点齐备</b>再动手替换，避免"只换上编码器"这类
 *       单侧 zstd 的必断连组合；</li>
 *   <li>VarInt 与字典读取统一走 {@link ZstdVarInts} / {@link ZstdDictRegistry}。</li>
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
     * 「已推送 {@code zstd:dict}、正在等其应答」阶段的重试次数（合计约 10s）。
     *
     * <p>⚠️ 这一阶段<b>不能</b>沿用上面那个 2s 预算，否则会产出一个<b>必断连</b>组合，
     * 而不是安全回落：</p>
     * <ol>
     *   <li>两个阶段共用同一个 {@code attempt} 计数器（从 SetCompression 写出时开始累加），
     *       协商应答本身就要吃掉一个 RTT；</li>
     *   <li>字典阶段要传输的是<b>字典字节</b>——两个方向各最多 {@code dict_max_bytes}
     *       （默认 128KB、配置上限 1MB，即最多 2MB），客户端还要做 CRC32 校验并构建
     *       {@code ZstdDictCompress/Decompress}（实测约 3.2MB 的原生预计算）；</li>
     *   <li>慢链路（移动网络 / 跨境 / 拥塞）下 256KB 就可能超过剩余的 1~2s。</li>
     * </ol>
     * <p>一旦超时，本端按"回落原版"补发扣住的包并保持 vanilla zlib；而客户端那边
     * <b>字典已经装载成功</b>，它会在应答写出后照常切到 zstd —— 于是形成
     * 「服务端 zlib / 客户端 zstd」，双向都解析错乱、必然断连。
     * 这与本项目到处防范的那类缺陷完全同型，只是触发条件从"逻辑漏判"变成了"预算不够"。</p>
     *
     * <p>取 10s 的取舍：足以让 2MB 字典在约 200KB/s 的慢链路上跑完；同时它有界，
     * 真正死掉的连接最多让登录卡 10s 后回落原版，不会无限期挂住。</p>
     */
    private static final int DICT_RESPONSE_MAX_RETRIES = 100;

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
     * 否则我们会先于 Velocity 的 setupCompression 替换管道，随后被原版压缩处理器覆盖。</p>
     */
    public void onDictResponseReceived() {
        ChannelHandlerContext c = this.selfCtx;
        if (c == null || !pipelineActivated) return;
        c.channel().eventLoop().execute(() -> tryActivate(c, 0));
    }

    /** 供嗅探器按通道调用（无处理器时静默忽略）。 */
    public static void notifyDictResponse(io.netty.channel.Channel channel) {
        ZstdHijacker h = channel.pipeline().get(ZstdHijacker.class);
        if (h != null) h.onDictResponseReceived();
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
            boolean isNegotiate = mgr.getNegotiateTxId() != 0 && txId == mgr.getNegotiateTxId();
            boolean isDict = mgr.getDictTxId() != 0 && txId == mgr.getDictTxId();
            if (isNegotiate || isDict) {
                try {
                    Object contentValue = invokeOrNull(M_RESP_CONTENT, msg);
                    ByteBuf data = contentValue instanceof ByteBuf bb ? bb : null;
                    boolean success = Boolean.TRUE.equals(invokeOrNull(M_RESP_SUCCESS, msg));
                    int encStatus = ZstdNegotiateStatus.VANILLA;
                    int decStatus = ZstdNegotiateStatus.VANILLA;
                    if (success && data != null && data.readableBytes() >= 2) {
                        encStatus = readStatus(data);
                        decStatus = readStatus(data);
                    }
                    if (isNegotiate && needsDictPush(encStatus, decStatus)) {
                        // 协议 v4：客户端报告缺字典 —— 单独推送一次 zstd:dict，
                        // 门控结论等这次推送的应答到达后再定（见 markDictResponse 调用点）
                        mgr.markDictRequested();
                        if (!sendDictQuery(ctx.channel(), ZstdNegotiateStatus.needsDict(encStatus),
                                ZstdNegotiateStatus.needsDict(decStatus))) {
                            // 推送没发出去（字典过大 / 锚点缺失）：立即回落原版，
                            // 不要白等满 10 秒的字典阶段预算。
                            LOGGER.warn("[Zstd] zstd:dict 未能发出 — 立即回落原版");
                            releaseHeld(ctx);
                            return;
                        }
                    } else {
                        mgr.markDictResponse(encStatus, decStatus, success);
                        if (isDict) {
                            ZstdHijacker.notifyDictResponse(ctx.channel());
                        }
                    }
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

        // negotiate 兜底触发（主路径在握手注入时已发出，防重）
        if (mgr != null && !mgr.isNegotiationSent()) {
            sendNegotiateNow(ctx.channel());
            mgr = ctx.channel().attr(ZstdChannelManager.KEY).get();
        }

        if (mgr == null || mgr.isReplaced()) {
            super.write(ctx, msg, promise);
            return;
        }

        if (!pipelineActivated && C_SET_COMPRESSION != null && C_SET_COMPRESSION.isInstance(msg)) {
            pipelineActivated = true;
            LOGGER.debug("[Zstd] SetCompression detected — deferring zstd activation until negotiate response; pipeline={}",
                    ctx.channel().pipeline().names());
            // ⚠️ 必须同步进入 hold：Velocity 在写完 SetCompression 后会立刻写 LoginSuccess，
            // 而激活要等协商应答（一个 RTT）。若不拦下这段窗口内的包，它们会走原版压缩编码器
            // 发出去，而客户端收到 SetCompression 后已经切到 zstd —— v3 的批量帧格式
            // （内层带 pktLen）与原版帧 `[0][packet]` 不再同形，客户端必然解析错乱。
            // v2 之所以侥幸没事，是因为"一帧一包"下两种格式恰好同形。
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
            releaseHeld(ctx);
        }
    }

    /** 门控激活：等待协商应答；版本不匹配/字典未确认/超时则放弃（原版 zlib 全程接管）。 */
    private void tryActivate0(ChannelHandlerContext ctx, int attempt) {
        Channel channel = ctx.channel();
        if (!channel.isActive() || pipelineRemoved(channel)) return;

        ZstdChannelManager mgr = channel.attr(ZstdChannelManager.KEY).get();
        if (mgr == null || mgr.isReplaced()) return;

        if (!mgr.isDictResponseReceived()) {
            // ⚠️ 预算必须分阶段：等"协商应答"只要一个 RTT（2s 足够），而等"字典应答"
            // 要传完字典字节 + 客户端建好原生字典（最多 2MB，慢链路上远超 2s）。
            // 用同一个 2s 预算会在慢链路上超时回落，而客户端那边字典已到、照常切 zstd
            // → 单侧 zstd → 断连。详见 DICT_RESPONSE_MAX_RETRIES 的注释。
            int maxRetries = mgr.isDictRequested() ? DICT_RESPONSE_MAX_RETRIES : RESPONSE_MAX_RETRIES;
            if (attempt >= maxRetries) {
                // ⚠️ 这句日志必须能自证"卡在哪一步"：只写"超时"无法区分
                //「协商应答就没收到」与「协商应答收到了、字典也推了，但字典应答没回来」，
                // 而两者的排查方向完全相反（前者看客户端有没有收到查询，后者看推送有没有被接受）。
                LOGGER.warn("[Zstd] Negotiate response not received after {}ms — skipping zstd (vanilla fallback)"
                                + "（字典已请求={} 字典已确认={}；字典已请求=false 表示连协商应答都没收到）",
                        RESPONSE_RETRY_MS * maxRetries,
                        mgr.isDictRequested(), mgr.isDictConfirmed());
                releaseHeld(ctx); // 回落原版：补发扣下的包（原版压缩编码器仍在位）
                return;
            }
            channel.eventLoop().schedule(() -> tryActivate(ctx, attempt + 1),
                    RESPONSE_RETRY_MS, TimeUnit.MILLISECONDS);
            return;
        }

        if (mgr.isProtocolMismatch()) {
            LOGGER.warn("[Zstd] Protocol mismatch reported by client — skipping zstd (vanilla fallback)");
            releaseHeld(ctx); // 回落原版：补发扣下的包
            return;
        }

        // ⚠️ 协议 v4 硬门控（必需，不能只依赖"应答已到"）：
        // 客户端报告过"缺字典"，但最终没有确认字典就绪 —— 推送丢失、客户端加载失败、
        // 或该应答在帧层被别的捕获点消费掉了。此时**绝不能激活**：客户端会因为等字典
        // 停在 PLAIN 状态永不激活，于是服务端 zstd / 客户端 vanilla → 必断连。
        // 宁可回落原版（少一点压缩率），也不能让玩家进不去。
        if (mgr.isDictRequested() && !mgr.isDictConfirmed()) {
            LOGGER.warn("[Zstd] 客户端请求了字典但未确认就绪（zstd:dict 应答缺失）"
                    + "— 放弃 zstd 并回落原版，避免单侧 zstd 断连");
            releaseHeld(ctx);
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
            releaseHeld(ctx); // 不可激活：必须补发扣住的包，否则连接卡死
            return false;
        }
        // 先确认两端都能替换，再动手——避免只装上一侧造成单侧 zstd 的必断连组合
        if (!hasEncoderAnchor(p) || !hasDecoderAnchor(p)) {
            LOGGER.error("[Zstd] Cannot activate zstd — pipeline anchors missing (encoder={}, decoder={}); "
                    + "pipeline={}", hasEncoderAnchor(p), hasDecoderAnchor(p), p.names());
            releaseHeld(ctx); // 同上：不能把包一直扣着
            return false;
        }

        // 字典门控：仅客户端确认就绪时加载字典
        if (mgr.isDictConfirmed()) {
            mgr.loadTrainerDicts();
        } else {
            LOGGER.info("[Zstd] Activating without trained dicts (client not ready) — compression still active");
        }

        // ⚠️ 先在**代码路径上**把两个处理器都装上，成功了才置 replaced。
        //
        // 曾经这里先 setReplaced(true) 再安装：安装失败走重试时，{@code tryActivate0}
        // 开头的 {@code mgr.isReplaced()} 早退会把重试**整个吃掉**（replaced 已为 true
        // → 直接 return），于是 ①重试形同虚设；②那条 return 不会 releaseHeld，
        // 激活窗口内扣住的包与 promise 永挂 —— 客户端表现为**连接卡死**（不是断连），
        // 日志里只有一行 ERROR；③监控里的"使用人数"还被错误 +1 且永不回落。
        // Paper 端一直是"装完再置位"（见 ZstdPaperNegotiator），此处与它对齐。
        if (!mgr.installEncoder(channel) || !mgr.installDecoder(channel)) {
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
            releaseHeld(ctx); // 回落原版：把扣住的包按原路径补发
            return false;
        }

        mgr.setReplaced(true); // 计数已内聚在 setReplaced 内（幂等）

        LOGGER.info("[Zstd] Zstd transport activated (dict={})", mgr.isDictConfirmed());

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

    private static boolean hasEncoderAnchor(ChannelPipeline p) {
        return p.get("zstd_encoder") != null || p.get(COMPRESSION_ENCODER) != null
                || p.get("cipher-encoder") != null || p.get(MINECRAFT_ENCODER) != null;
    }

    private static boolean hasDecoderAnchor(ChannelPipeline p) {
        return p.get("zstd_decoder") != null || p.get(COMPRESSION_DECODER) != null
                || p.get("decompress") != null || p.get(MINECRAFT_DECODER) != null;
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

            long encId = ZstdDictRegistry.encoderDictId();
            long decId = ZstdDictRegistry.decoderDictId();

            ByteBuf buf = writeCtx.alloc().buffer();
            try {
                ZstdVarInts.write(buf, 0x04);
                ZstdVarInts.write(buf, txId);
                ZstdVarInts.writeString(buf, "zstd:negotiate");
                buf.writeInt(ZstdChannelManager.PROTOCOL_VERSION);
                buf.writeLong(encId);
                buf.writeLong(decId);
                byte flags = 0;
                if (encId != 0) flags |= 1;
                if (decId != 0) flags |= 2;
                buf.writeByte(flags);

                // 协议 v4：字典不再内联——只声明 id，客户端缺哪个再单独推 zstd:dict

                writeCtx.writeAndFlush(buf);
                LOGGER.debug("[Zstd] Sent zstd:negotiate txId={} encId={} decId={}", txId, encId, decId);
            } catch (Throwable t) {
                if (buf.refCnt() > 0) buf.release();
                throw t;
            }
        } catch (Exception e) {
            LOGGER.error("[Zstd] Failed to send negotiate", e);
        }
    }

    /**
     * 协议 v4：按需推送字典（仅当客户端报告某个方向缺字典时）。
     *
     * <p>与 negotiate 分开的原因：直连场景（Paper 端）无法预筛客户端，
     * 若把几百 KB 的字典内联进 negotiate，不支持 zstd 的客户端也要白吃这份流量。</p>
     *
     * <p>包可见：帧层嗅探器（{@link ZstdNegotiateAnswerSniffer}）捕获到 negotiate 应答后
     * 也要走这条路——它是主路径，且应答在帧层就被消费掉了，Hijacker 的包层分支看不到。</p>
     *
     * @return 是否确实发出；{@code false} 表示字典过大或写锚点缺失，
     *         调用方应立即触发回落（否则只能白等满一个字典阶段的应答预算）
     */
    static boolean sendDictQuery(Channel channel, boolean needEncoder, boolean needDecoder) {
        try {
            ZstdChannelManager mgr = channel.attr(ZstdChannelManager.KEY).get();
            if (mgr == null) return false;
            ChannelHandlerContext writeCtx = loginQueryWriteCtx(channel.pipeline());
            if (writeCtx == null) {
                LOGGER.warn("[Zstd] cannot send zstd:dict — minecraft-encoder anchor missing");
                return false;
            }

            int txId = ThreadLocalRandom.current().nextInt(1, Integer.MAX_VALUE);
            mgr.setDictTxId(txId);

            byte[] enc = needEncoder ? ZstdDictRegistry.encoderDictBytes() : null;
            byte[] dec = needDecoder ? ZstdDictRegistry.decoderDictBytes() : null;
            // ⚠️ 兜底：本包把两个方向的字典放在一起，而 MC 单帧上限是 2^21-1。
            // 配置那边已按 MAX_DICT_BYTES_PER_DIRECTION 夹取，这里再挡一次——
            // 因为字典文件是磁盘上的产物（可能是旧版本/手改过的），不能假设它守规矩。
            // 超限时放弃推送（客户端会停在"等字典"→ 服务端随后按硬门控安全回落原版），
            // 总好过发出一个客户端必然拒绝的超长帧（那是"进不去"，比"没压缩"严重得多）。
            long payload = (long) (enc == null ? 0 : enc.length + 8)
                    + (long) (dec == null ? 0 : dec.length + 8);
            if (payload + 64 > ZstdVarInts.MC_MAX_FRAME_BYTES) {
                LOGGER.warn("[Zstd] 字典过大，拒绝推送（enc={}B dec={}B，合帧上限 {}B）"
                                + "— 本连接将回落原版；请调小 trainer.dict_max_bytes",
                        enc == null ? 0 : enc.length, dec == null ? 0 : dec.length,
                        ZstdVarInts.MC_MAX_FRAME_BYTES);
                return false;
            }
            byte flags = 0;
            if (enc != null && enc.length > 0) flags |= 1;
            if (dec != null && dec.length > 0) flags |= 2;

            ByteBuf buf = writeCtx.alloc().buffer();
            try {
                ZstdVarInts.write(buf, 0x04);
                ZstdVarInts.write(buf, txId);
                ZstdVarInts.writeString(buf, "zstd:dict");
                buf.writeByte(flags);
                if (enc != null && enc.length > 0) writeDictBytes(buf, enc);
                if (dec != null && dec.length > 0) writeDictBytes(buf, dec);
                writeCtx.writeAndFlush(buf);
                LOGGER.info("[Zstd] 已推送 zstd:dict txId={} flags={} (enc={}B dec={}B)，等待其应答",
                        txId, flags, enc == null ? 0 : enc.length, dec == null ? 0 : dec.length);
                return true;
            } catch (Throwable t) {
                if (buf.refCnt() > 0) buf.release();
                throw t;
            }
        } catch (Exception e) {
            LOGGER.error("[Zstd] Failed to send dict query", e);
            return false;
        }
    }

    /** 线上字典帧：{@code [int crc32][int len][bytes]}；crc 必须与字典 id 同源。 */
    private static void writeDictBytes(ByteBuf buf, byte[] dict) {
        buf.writeInt(mikumc.zstd.protocol.ZstdDictId.wireChecksum(dict));
        buf.writeInt(dict.length);
        buf.writeBytes(dict);
    }

    /**
     * 客户端是否在 negotiate 应答里报告了"缺字典"（协议 v4 需要先推一次 {@code zstd:dict}）。
     *
     * <p><b>两条捕获点（包层 Hijacker / 帧层嗅探器）必须用同一份判断</b>——
     * 以前帧层嗅探器少了这个分支，导致服务端跳过字典推送就直接激活，
     * 而客户端因为"等字典"停在 PLAIN 状态永不激活 → 单侧 zstd → 断连。</p>
     */
    static boolean needsDictPush(int encStatus, int decStatus) {
        return ZstdNegotiateStatus.needsDict(encStatus) || ZstdNegotiateStatus.needsDict(decStatus);
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
