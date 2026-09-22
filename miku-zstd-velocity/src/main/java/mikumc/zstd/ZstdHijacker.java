package mikumc.zstd;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.util.ReferenceCountUtil;
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
                    int encStatus = 2;
                    int decStatus = 2;
                    Object contentValue = invokeOrNull(M_RESP_CONTENT, msg);
                    ByteBuf data = contentValue instanceof ByteBuf bb ? bb : null;
                    boolean success = Boolean.TRUE.equals(invokeOrNull(M_RESP_SUCCESS, msg));
                    if (success && data != null && data.readableBytes() >= 2) {
                        encStatus = readStatus(data);
                        decStatus = readStatus(data);
                    }
                    if (isNegotiate && (encStatus == 1 || decStatus == 1)) {
                        // 协议 v4：客户端报告缺字典 —— 单独推送一次 zstd:dict，
                        // 门控结论等这次推送的应答到达后再定（见 markDictResponse 调用点）
                        sendDictQuery(ctx.channel(), encStatus == 1, decStatus == 1);
                    } else {
                        mgr.markDictResponse(encStatus, decStatus);
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
     * 通道关闭时把扣住的 promise 补完。
     *
     * <p>⚠️ 缺少这一步会造成 <b>promise 永不完成</b>：扣包时我们把调用方给的 promise
     * 存进了 {@link #heldPromises}，只有 {@code releaseHeld} 才会让它们被 netty 完成。
     * 若连接在激活窗口内断开（客户端掉线、超时），{@code tryActivate} 会因为
     * {@code !channel.isActive()} 提前返回，那些 promise 就再也没人完成——
     * 等待它们的一方会永久挂住。这类问题不会报错、不会泄漏堆，只会表现为"偶发卡死"。</p>
     */
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        failHeldPromises(new ClosedChannelException());
        super.channelInactive(ctx);
    }

    /** 用给定异常补完所有扣住的 promise（幂等：列表清空后重复调用无副作用）。 */
    private void failHeldPromises(Throwable cause) {
        holding = false;
        if (heldPromises.isEmpty()) {
            heldPackets.clear();
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

    /** 门控激活：等待协商应答；版本不匹配/超时则放弃（原版 zlib 全程接管）。 */
    private void tryActivate(ChannelHandlerContext ctx, int attempt) {
        Channel channel = ctx.channel();
        if (!channel.isActive() || pipelineRemoved(channel)) return;

        ZstdChannelManager mgr = channel.attr(ZstdChannelManager.KEY).get();
        if (mgr == null || mgr.isReplaced()) return;

        if (!mgr.isDictResponseReceived()) {
            if (attempt >= RESPONSE_MAX_RETRIES) {
                LOGGER.warn("[Zstd] Negotiate response not received after {}ms — skipping zstd (vanilla fallback)",
                        RESPONSE_RETRY_MS * RESPONSE_MAX_RETRIES);
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

        activateZstd(ctx, mgr, attempt);
        releaseHeld(ctx); // zstd 编解码器已就位：补发扣下的包（走 zstd 路径）
    }

    private boolean pipelineRemoved(Channel channel) {
        return channel.pipeline().get(ZstdHijacker.class) == null;
    }

    private void activateZstd(ChannelHandlerContext ctx, ZstdChannelManager mgr, int attempt) {
        Channel channel = ctx.channel();
        ChannelPipeline p = channel.pipeline();

        if (p.get(MINECRAFT_DECODER) == null || p.get(MINECRAFT_ENCODER) == null) {
            LOGGER.warn("[Zstd] Cannot activate zstd — not a frontend Minecraft channel");
            return;
        }
        // 先确认两端都能替换，再动手——避免只装上一侧造成单侧 zstd 的必断连组合
        if (!hasEncoderAnchor(p) || !hasDecoderAnchor(p)) {
            LOGGER.error("[Zstd] Cannot activate zstd — pipeline anchors missing (encoder={}, decoder={}); "
                    + "pipeline={}", hasEncoderAnchor(p), hasDecoderAnchor(p), p.names());
            return;
        }

        // 字典门控：仅客户端确认就绪时加载字典
        if (mgr.isDictConfirmed()) {
            mgr.loadTrainerDicts();
        } else {
            LOGGER.info("[Zstd] Activating without trained dicts (client not ready) — compression still active");
        }

        mgr.setReplaced(true);

        if (!mgr.installEncoder(channel) || !mgr.installDecoder(channel)) {
            // ⚠️ 两件事都必须做：
            // 1) 重试——原版压缩处理器是 SetCompression 之后才装的，可能尚未就绪；
            // 2) 重试用尽后必须 releaseHeld——否则 hold 期间扣下的包永不补发，
            //    客户端表现为**连接卡死**（而不是断连），且日志里只有一行 ERROR，极难排查。
            if (attempt < RESPONSE_MAX_RETRIES) {
                channel.eventLoop().schedule(() -> tryActivate(ctx, attempt + 1),
                        RESPONSE_RETRY_MS, TimeUnit.MILLISECONDS);
                return;
            }
            LOGGER.error("[Zstd] zstd handler installation failed after {} attempts; pipeline={}",
                    attempt, p.names());
            mgr.setReplaced(false);
            releaseHeld(ctx); // 回落原版：把扣住的包按原路径补发
            return;
        }

        LOGGER.info("[Zstd] Zstd transport activated (dict={})", mgr.isDictConfirmed());

        // 使命完成，自移除——热路径（每个出站包）不再经过本处理器
        if (p.get(ZstdHijacker.class) != null) {
            p.remove(this);
        }
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
     * 写入 compression-encoder（位于 pe-encoder-packetevents 等第三方编码器下游），
     * 由其补上外层长度前缀，经 cipher-encoder 直达网络。
     */
    public static void sendNegotiateNow(Channel channel) {
        try {
            ChannelHandlerContext writeCtx = channel.pipeline().context(COMPRESSION_ENCODER);
            if (writeCtx == null) {
                writeCtx = channel.pipeline().context(MINECRAFT_ENCODER);
            }
            if (writeCtx == null) {
                // 只报一次：本方法在协商成功前会被每个出站包调用，无条件告警会刷屏
                if (ANCHOR_WARNED.compareAndSet(false, true)) {
                    LOGGER.warn("[Zstd] neither compression-encoder nor minecraft-encoder found, "
                            + "cannot send negotiate (this message is logged once)");
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
                LOGGER.info("[Zstd] Sent zstd:negotiate txId={} encId={} decId={}", txId, encId, decId);
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
     */
    private static void sendDictQuery(Channel channel, boolean needEncoder, boolean needDecoder) {
        try {
            ZstdChannelManager mgr = channel.attr(ZstdChannelManager.KEY).get();
            if (mgr == null) return;
            ChannelHandlerContext writeCtx = channel.pipeline().context(COMPRESSION_ENCODER);
            if (writeCtx == null) {
                writeCtx = channel.pipeline().context(MINECRAFT_ENCODER);
            }
            if (writeCtx == null) {
                LOGGER.warn("[Zstd] cannot send zstd:dict — no encoder anchor");
                return;
            }

            int txId = ThreadLocalRandom.current().nextInt(1, Integer.MAX_VALUE);
            mgr.setDictTxId(txId);

            byte[] enc = needEncoder ? ZstdDictRegistry.encoderDictBytes() : null;
            byte[] dec = needDecoder ? ZstdDictRegistry.decoderDictBytes() : null;
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
                LOGGER.info("[Zstd] Sent zstd:dict txId={} flags={}", txId, flags);
            } catch (Throwable t) {
                if (buf.refCnt() > 0) buf.release();
                throw t;
            }
        } catch (Exception e) {
            LOGGER.error("[Zstd] Failed to send dict query", e);
        }
    }

    private static void writeDictBytes(ByteBuf buf, byte[] dict) {
        java.util.zip.CRC32 crc = new java.util.zip.CRC32();
        crc.update(dict);
        buf.writeInt((int) crc.getValue());
        buf.writeInt(dict.length);
        buf.writeBytes(dict);
    }

    /** 解析应答状态；解析失败按"不可用"(2) 处理。 */
    private static int readStatus(ByteBuf buf) {
        int v = ZstdVarInts.tryRead(buf, Integer.MAX_VALUE);
        return (v == ZstdVarInts.NEED_MORE || v == ZstdVarInts.INVALID) ? 2 : v;
    }
}
