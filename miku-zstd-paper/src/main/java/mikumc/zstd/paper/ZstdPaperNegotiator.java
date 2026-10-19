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

    /**
     * 「已推送 {@code zstd:dict}、正在等其应答」阶段的重试次数（合计约 10s）。
     *
     * <p>⚠️ 不能沿用上面的 2s：字典阶段要传输的是字典字节（两个方向各最多
     * {@code dict_max_bytes}，默认 128KB / 配置上限 1MB），客户端还要做 CRC32 校验并构建
     * 原生字典对象。慢链路下这会超过 2s，于是本端超时回落原版、而客户端字典已装载
     * 并照常切 zstd —— 形成「服务端 zlib / 客户端 zstd」的必断连组合。
     * 详见 Velocity 端同名常量的完整推理（两端必须一致）。</p>
     */
    private static final int DICT_MAX_RETRIES = 100;

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
        // ⚠️ 这里<b>不能</b>用 `!mgr.isResponseReceived()` 做前置门控：
        //    该标志在收到 negotiate 应答时就已置 true，而协议 v4 的 zstd:dict 应答
        //    是<b>之后</b>才到的第二条应答。加上这个门控的后果是——字典应答既不被消费、
        //    也不解析，`confirmed` 永远为 false，激活前的硬门控判定"客户端请求了字典但未确认"
        //    并回落原版：<b>凡是需要字典的客户端在 Paper 端永远用不上 zstd</b>。
        //    真正的去重判据是下面的 txId 匹配（dictTxId 未设置时为 -1，不会误命中）；
        //    激活成功后协商器会自移除，也不会留下长期开销。
        // ⚠️ 但必须保留「尚未发起协商就整段跳过」：否则回落原版的连接上，
        //    本处理器会对**每一个**入站包做 getSimpleName + 字段反射读取，
        //    是每条原版连接都要永久支付的税（negotiateTxId 为 -1 时不可能命中任何应答）。
        if (mgr != null && msg != null && mgr.getNegotiateTxId() != -1) {
            // 0) 切换确认（客户端发来的 zstd:switch）：必须在通用应答解析之前拦下
            //    帧层嗅探器是主路径；这里只是包层兜底（嗅探器装不上时）。
            int switchTx = readSwitchTxId(msg);
            if (switchTx > 0) {
                io.netty.util.ReferenceCountUtil.release(msg);
                if (mgr.isReplaced()) {
                    mgr.markSwitchConfirmed(switchTx);
                }
                return;
            }
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
                        if (!sendDictQuery(ctx, ZstdNegotiateStatus.needsDict(enc),
                                ZstdNegotiateStatus.needsDict(dec))) {
                            // 推送没发出去（字典过大 / 锚点缺失）：立即回落原版，
                            // 不要白等满 10 秒的字典应答预算。
                            finishVanilla(ctx);
                        }
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



    /** 发轻量 negotiate：只带协议版本与两个 dictId（字典字节按需另发，见 {@link #sendDictQuery}）。 */
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

    /**
     * 协议 v4：按需推送字典（仅当客户端报告某个方向缺字典时）。
     *
     * @return 是否确实发出；{@code false} 表示字典过大或写锚点缺失，调用方应立即回落原版
     *         （否则会白等满一个字典阶段的应答预算）
     */
    private boolean sendDictQuery(ChannelHandlerContext ctx, boolean needEncoder, boolean needDecoder) {
        ZstdPaperChannelManager mgr = ctx.channel().attr(ZstdPaperChannelManager.KEY).get();
        if (mgr == null) return false;
        byte[] enc = needEncoder ? ZstdPaperDictRegistry.encoderDictBytes() : null;
        byte[] dec = needDecoder ? ZstdPaperDictRegistry.decoderDictBytes() : null;
        // ⚠️ 兜底：本包把两个方向的字典放在一起，而 MC 单帧上限是 2^21-1。
        // 本端这条包还是在原版压缩启用**之前**发出的裸帧，不会被压缩缩小，因此更危险。
        // 超限就放弃推送 → 客户端停在"等字典" → 硬门控让本端安全回落原版
        //（好过发出一个客户端必然拒绝的超长帧：那是"进不去"，比"没压缩"严重得多）。
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
            return true;
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
            // ⚠️ 必须走 finishVanilla（补发 + 自移除）：只 releaseHeld 会让协商器
            // 永久留在入站/出站热路径上，对每个包继续做字符串判定并持有旧类加载器。
            finishVanilla(ctx);
        }
    }

    private void tryActivate0(ChannelHandlerContext ctx, int attempt) {
        Channel channel = ctx.channel();
        if (!channel.isActive()) return;
        ZstdPaperChannelManager mgr = channel.attr(ZstdPaperChannelManager.KEY).get();
        if (mgr == null || mgr.isReplaced()) return;

        if (!mgr.isResponseReceived()) {
            // ⚠️ 预算必须分阶段：等"协商应答"只要一个 RTT（2s 足够），而等"字典应答"要传完
            // 字典字节 + 客户端建好原生字典对象，慢链路上远超 2s。用同一个预算会在慢链路
            // 上超时回落，而客户端那边字典已到、照常切 zstd → 单侧 zstd → 断连。
            // 详见 DICT_MAX_RETRIES 的注释。
            int maxRetries = mgr.isDictRequested() ? DICT_MAX_RETRIES : MAX_RETRIES;
            if (attempt >= maxRetries) {
                LOGGER.warn("[Zstd] no negotiate response within {}ms — staying vanilla",
                        RETRY_MS * maxRetries);
                finishVanilla(ctx); // 明确回落：补发扣包并自移除
                return;
            }
            channel.eventLoop().schedule(() -> tryActivate(ctx, attempt + 1), RETRY_MS, TimeUnit.MILLISECONDS);
            return;
        }

        if (mgr.isRefused()) {
            LOGGER.warn("[Zstd] 客户端保持原版 zlib，本端不激活");
            finishVanilla(ctx); // 明确回落：补发扣包并自移除
            return;
        }

        // ⚠️ 协议 v4 硬门控：客户端报告过"缺字典"，但最终没确认字典就绪
        //（推送丢失 / 客户端加载失败 / 应答没被捕获到）。此时绝不能激活——
        // 客户端会因为等字典停在 PLAIN 状态永不激活，单侧 zstd 必断连。
        if (mgr.isDictRequested() && !mgr.isConfirmed()) {
            LOGGER.warn("[Zstd] 客户端请求了字典但未确认就绪（zstd:dict 应答缺失）"
                    + "— 放弃 zstd 并回落原版，避免单侧 zstd 断连");
            finishVanilla(ctx);
            return;
        }
        // 客户端确认就绪 → 装载共享字典（仅此时装载，避免与客户端不一致）
        if (mgr.isConfirmed()) {
            mgr.loadTrainerDicts();
        }


        LOGGER.debug("[Zstd] 正在安装 zstd 编解码器 (pipeline={})", channel.pipeline().names());
        // 安装阶段走自己的重试循环：与"等协商/字典应答"的 attempt 完全分开计数。
        // 若共用，慢链路上（字典阶段可等 100 轮）进入安装阶段时 attempt 已 > 20，
        // 安装重试会被立即判为耗尽 → 锚点明明马上就好，却直接回落原版。
        installHandlers(ctx, mgr, 0);
    }

    /**
     * 安装 zstd 编解码器（独立重试循环，预算 {@link #MAX_RETRIES}）。
     *
     * <p>先确认<b>两端都能替换</b>再动手：若先装编码器、再发现解码器锚点缺失就返回，
     * 管道会停在"zstd 出站 / 原版入站"的半装状态——客户端以 zstd 发帧、却按原版解帧，
     * 是必然的乱码断连，且两侧日志都显示"激活过"，极难定位。</p>
     *
     * <p>原版 compress/decompress 是在 SetCompression 写出<b>之后</b>才安装的，
     * 所以首轮必然找不到锚点——必须重试，但重试期间要保持 holding，
     * 否则扣住的包会用尚未定型的路径提前发出去。</p>
     *
     * @param installAttempt 安装阶段自己的重试序号（首轮 0）
     */
    private void installHandlers(ChannelHandlerContext ctx, ZstdPaperChannelManager mgr, int installAttempt) {
        Channel channel = ctx.channel();
        if (!channel.isActive()) return;

        if (!mgr.hasEncoderAnchor(channel) || !mgr.hasDecoderAnchor(channel)) {
            // 原版 compress/decompress 是在 SetCompression 写出之后才装的，首轮必然缺失。
            if (installAttempt < MAX_RETRIES) {
                channel.eventLoop().schedule(() -> installHandlers(ctx, mgr, installAttempt + 1),
                        RETRY_MS, TimeUnit.MILLISECONDS);
                return;
            }
            LOGGER.warn("[Zstd] anchors 始终未出现（compress/decompress 缺失）— 回落原版; pipeline={}",
                    channel.pipeline().names());
            finishVanilla(ctx);
            return;
        }

        // ⚠️ 闸门必须先于编码器就位（包层），且必须先于解码器——解码器靠它的状态判断
        // "当前入站是否还可能是原版帧"。
        mgr.installSwitchGate(channel);

        if (!mgr.installEncoder(channel)) {
            if (installAttempt < MAX_RETRIES) {
                channel.eventLoop().schedule(() -> installHandlers(ctx, mgr, installAttempt + 1),
                        RETRY_MS, TimeUnit.MILLISECONDS);
                return;
            }
            LOGGER.warn("[Zstd] zstd 编码器安装失败（锚点在位却替换失败）— 回落原版; pipeline={}",
                    channel.pipeline().names());
            finishVanilla(ctx);
            return;
        }
        if (!mgr.installDecoder(channel)) {
            // 解码器安装失败：立即回滚已装的编码器，避免半装管道。
            if (installAttempt < MAX_RETRIES) {
                channel.eventLoop().schedule(() -> installHandlers(ctx, mgr, installAttempt + 1),
                        RETRY_MS, TimeUnit.MILLISECONDS);
                return;
            }
            LOGGER.warn("[Zstd] zstd 解码器安装失败（锚点在位却替换失败）— 回落原版; pipeline={}",
                    channel.pipeline().names());
            finishVanilla(ctx);
            return;
        }
        mgr.setReplaced(true); // 计数已内聚在 setReplaced 内（幂等，避免重复 +1）
        LOGGER.info("[Zstd] zstd transport activated on Paper");

        // ⚠️ 闸门此刻正扣着登录期的大帧（例如 LoginSuccess 压缩帧）。客户端在收到它之前
        // 不可能完成登录，也就没有任何东西会触发它的切换 —— 必须主动补一条"服务端已就绪"。
        sendSwitchRequest(channel);

        releaseHeld(ctx);
        // 激活后本处理器已无用：留在管线里会让**每个**出站包多一次 attr 查询、
        // 每个入站包多一次 getSimpleName().contains(...) 判定，是永久税。
        // 对齐 Velocity 端（激活即自移除）。顺序上必须先 releaseHeld 再移除 ——
        // 移除后经该 ctx 写出的语义不再有保证，扣住的包就会发不出去。
        removeSelf(ctx);
    }

    /**
     * 明确回落原版：补发扣住的包，并<b>自移除</b>协商器。
     *
     * <p>⚠️ 自移除这一步以前只在"激活成功"分支里有，所有回落分支（超时、客户端拒绝、
     * 字典未确认、锚点始终缺失）都只 releaseHeld 就返回——协商器于是常驻这条连接的
     * 入站/出站热路径，对<b>每一个</b>包继续做 {@code getSimpleName().contains(...)}
     * 字符串判定，并且一直持有本插件的类加载器。这些回落路径并不罕见（原版客户端、
     * 版本不匹配的客户端都会命中），属"每条非 zstd 连接都白付一份永久税"。</p>
     */
    private void finishVanilla(ChannelHandlerContext ctx) {
        releaseHeld(ctx);
        removeSelf(ctx);
    }

    /** 从管线摘掉自身（幂等；已移除或通道已关时静默）。 */
    private void removeSelf(ChannelHandlerContext ctx) {
        try {
            ctx.pipeline().remove(this);
        } catch (Throwable t) {
            // 幂等：可能已移除、或通道正在关闭 → 无需处理
            LOGGER.debug("[Zstd] negotiator self-removal skipped: {}", t.toString());
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

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        releaseHeldOnTeardown();
        super.channelInactive(ctx);
    }

    /**
     * 本处理器被<b>外部</b>从管线摘除时的收尾（插件停用 / 重载会走这条路）。
     *
     * <p>⚠️ 这是 {@code channelInactive} 之外<b>另一条</b>独立的销毁路径：
     * {@code MikuZstdPaper#uninstallHook} 会遍历在线连接、直接
     * {@code pipeline.remove("miku-zstd-negotiator")}。若这条连接此刻正处在激活窗口内
     * （{@code holding == true}，扣着若干包与 promise），而通道<b>仍然活着</b>，
     * 那么 {@code channelInactive} 永远不会触发 —— 扣住的包没人 release（引用计数泄漏，
     * Netty leak detector 会报 LEAK），promise 也没人完成（上游 future 永久挂起）。</p>
     *
     * <p>所以两条销毁路径必须共用同一份收尾逻辑，且都要幂等。</p>
     */
    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        releaseHeldOnTeardown();
        super.handlerRemoved(ctx);
    }

    /** 销毁收尾（幂等）：释放扣住的包并失败化其 promise。列表清空后重复调用无副作用。 */
    private void releaseHeldOnTeardown() {
        holding = false;
        // ⚠️ 先把包本身 release 掉：它们是 ReferenceCounted 的（内部持有 ByteBuf），
        // 扣下时所有权已转到本处理器；既然永远不会被写出，就没有别人替我们释放。
        for (Object p : heldPackets) {
            io.netty.util.ReferenceCountUtil.release(p);
        }
        heldPackets.clear();
        if (!heldPromises.isEmpty()) {
            LOGGER.debug("[Zstd] negotiator torn down with {} held packet(s) — failing their promises",
                    heldPromises.size());
            for (ChannelPromise p : heldPromises) {
                p.tryFailure(new java.nio.channels.ClosedChannelException());
            }
            heldPromises.clear();
        }
    }

    /**
     * 包层兜底：从查询/应答包里读出切换确认的 txId。
     *
     * <p>⚠️ 与帧层嗅探器同一判据（末尾 4 字节魔数），但这里拿不到帧的外层形态，
     * 只用于"嗅探器没装上的极端情况"。</p>
     *
     * @return txId；不是切换确认返回 {@code -1}
     */
    private static int readSwitchTxId(Object msg) {
        if (msg == null) return -1;
        String cn = msg.getClass().getSimpleName();
        if (!cn.contains("Query") && !cn.contains("Plugin")) return -1;
        byte[] payload = readBytes(msg, "data", "payload", "contents");
        if (payload == null || payload.length < 5) return -1;
        byte[] magic = new byte[4];
        System.arraycopy(payload, payload.length - 4, magic, 0, 4);
        if (!mikumc.zstd.protocol.ZstdStepFrames.matchesMagic(magic)) return -1;
        int[] cursor = {0};
        int txId = ZstdVarInts.readOr(payload, cursor, -1);
        return txId > 0 ? txId : -1;
    }

    /**
     * 发送"服务端已切换，请切换并发确认"（{@code zstd:switch}，4 字节魔数）。
     *
     * <p>⚠️ 经 {@code zstd_encoder} 的 context 写出：此刻原版压缩编码器已被替换，
     * 外层长度由 prepender 负责，对端（已切换的解码器）按 zstd 直存帧读出裸包。</p>
     */
    private static void sendSwitchRequest(Channel channel) {
        // ⚠️ 从 zstd_encoder 的 ctx 写出，并自己编码成 zstd 直存帧：
        //   · ctx.write 会跳过 zstd_encoder 本身，所以不能再让它编码一次；
        //   · 闸门位于压缩编码器之前，从它之后写出不会被自己扣住；
        //   · 外层长度由 prepender 负责（Paper 有独立 prepender）。
        io.netty.channel.ChannelHandlerContext writeCtx = channel.pipeline().context("zstd_encoder");
        if (writeCtx == null) {
            LOGGER.debug("[Zstd] cannot send switch request — zstd_encoder anchor missing");
            return;
        }
        ByteBuf packet = writeCtx.alloc().buffer();
        ByteBuf frame = writeCtx.alloc().buffer();
        try {
            int txId = ThreadLocalRandom.current().nextInt(1, Integer.MAX_VALUE);
            ZstdPaperChannelManager mgr = channel.attr(ZstdPaperChannelManager.KEY).get();
            if (mgr != null) mgr.expectSwitchTxId(txId);
            mikumc.zstd.protocol.ZstdStepFrames.writeQuery(packet, txId,
                    mikumc.zstd.protocol.ZstdSwitchGate.CHANNEL);
            int pktLen = packet.readableBytes();
            ZstdVarInts.write(frame, 0);
            ZstdVarInts.write(frame, pktLen);
            frame.writeBytes(packet);
            writeCtx.writeAndFlush(frame);
            LOGGER.debug("[Zstd] switch request sent");
        } catch (Throwable t) {
            if (packet.refCnt() > 0) packet.release();
            if (frame.refCnt() > 0) frame.release();
            LOGGER.debug("[Zstd] failed to send switch request: {}", t.toString());
        } finally {
            if (packet.refCnt() > 0) packet.release();
        }
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
