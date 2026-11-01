package mikumc.zstd.protocol;

import com.github.luben.zstd.ZstdDecompressCtx;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Miku-ZSTD 下行解码器基类（帧格式 <b>v7</b>，协商版本 v11）——客户端使用。
 *
 * <h2>下行帧格式（v7）</h2>
 * <pre>
 *   [varint rawSize][varint dictGen][varint blockCount] + block...     // rawSize > 0（压缩帧）
 *   [0][packet]                                                        // rawSize == 0（直存帧）
 *   block    = [varint blockRawLen][varint chunkLen][chunk]
 *   payload  = [varint pktLen][pkt] [varint pktLen][pkt] ...           // 解压后
 * </pre>
 *
 * <h2>本类在管线里的位置（协议 v7 起）</h2>
 * <p>本处理器<b>不替换</b>原版 {@code decompress}，而是插在它<b>上游</b>（入站先经过本类），
 * 解出的包<b>从原版解压处理器的 context 往下 fire</b>——即跳过它，直接进包解码器。
 * 它继续留在管线里，只是不再参与这条连接的数据流：</p>
 * <ul>
 *   <li>ViaVersion / PacketEvents / Grim 这类<b>按名字查找</b> {@code decompress} 的插件照常工作；</li>
 *   <li>回落原版只需把自己摘掉，不存在"原版处理器已被移除、Netty 非 {@code @Sharable}
 *       实例无法重新插入"的死局（旧实现因此只能关连接）。</li>
 * </ul>
 *
 * <h2>为什么不能再套一层 {@code [0][packet]}</h2>
 * <p>v7 的第一版实现是"把每个包补一个 {@code dataLength = 0} 前缀，交给原版解压器透传"，
 * 看起来更省事（原版对 0 是原样放过）。它在 vanilla 上确实能跑，但<b>在装了 Krypton 的客户端上必炸</b>
 * ——Krypton 的 {@code MinecraftCompressDecoder} 会把这条约定当作硬校验：</p>
 * <pre>
 * if (claimedUncompressedSize == 0) {
 *     checkState(actualUncompressedSize &lt; threshold, "Actual uncompressed size %s is greater than threshold %s");
 * }
 * </pre>
 * <p>也就是说"未压缩帧必须比阈值短"。我们解出来的包动辄上千字节，一帧就撞上
 * {@code IllegalStateException} → 客户端直接断连（实测 2026-11-01，Krypton + 5.0.36）。
 * 所以现在改为<b>跳过</b>原版解压器，既满足"不拆原版处理器"的初衷，也不再依赖任何
 * "透传"约定。</p>
 * <p>{@code rawSize == 0} 表示未压缩直存：帧体<b>恰好是一个包</b>（{@code [0][packet]}），
 * 与原版未压缩帧<b>逐字节同形</b>。本类只接收已协商的下行帧，不解析上行或猜测原版压缩帧。</p>
 * <p>压缩帧（{@code rawSize > 0}）可含<b>多个</b>包，按内层 {@code [varint pktLen]} 逐个切出
 * 并下传。（v2 是一帧一包、内层没有长度前缀；v4 起直存帧同样是一帧一包——原因见
 * {@code ZstdBatchEncoderBase} 的"帧格式"一节。）</p>
 * <p>传入的 {@code in} 已是完整帧负载：两端管线的前置 frame-decoder / prepender
 * 已负责外层长度（服务端侧由编码器自带 bodyLen、客户端侧由 prepender 负责）。</p>
 *
 * <h2>两道防护</h2>
 * <ul>
 *   <li><b>压缩比上限</b>（{@link #MAX_EXPANSION_RATIO} + {@link #MIN_EXPANSION_ALLOWANCE}）：
 *       {@code rawSize} 由对端声明，不设上限就变成"1 字节输入换 32MB 分配"的放大攻击；</li>
 *   <li><b>scratch 上限</b>（{@link #SCRATCH_KEEP_LIMIT}）：否则"连接见过的最大包"会永久占住缓冲
 *       ——configuration 阶段出现一次 32MB 的 Registry 帧，这条连接整个生命周期就常驻 32MB。</li>
 * </ul>
 *
 * <h2>协议错乱一律 fail-fast</h2>
 * <p>旧实现在非法 varint 时直接 return 且不消费字节，连接会静默僵死（无异常、无断连、
 * 无日志）。现在所有非法帧都记 WARN 并关闭连接。</p>
 */
public abstract class ZstdBatchDecoderBase extends ByteToMessageDecoder {

    protected static final Logger LOGGER = LoggerFactory.getLogger("miku-zstd");

    /**
     * 声明解压尺寸相对压缩尺寸的<b>最大倍数</b>。
     *
     * <p>防的是这个放大攻击：{@code rawSize} 由对端声明，攻击者只要发
     * {@code [varint 32MB][1 字节垃圾]}，我们就会先按 32MB 分配数组再去解压
     * ——<b>1 字节输入换 32MB 分配</b>，放大三千多万倍。</p>
     *
     * <p>⚠️ 实测的"整体压缩比 8~20:1"只是**全流量平均**，<b>单帧</b>可以极端得多
     * （见 {@link #MIN_EXPANSION_ALLOWANCE}），所以倍数只用来约束大帧。</p>
     */
    private static final int MAX_EXPANSION_RATIO = 256;

    /**
     * 倍数检查的保底额度：小帧允许解压到这个大小，避免误伤"极小帧高压缩比"的合法情况。
     *
     * <p>⚠️ 取值必须不低于 MC 自身解码器的硬上限（8MB），否则会<b>误杀合法帧</b>：
     * 实测（2026-09-27 生产事故）limbo 空区块数据把 445524 字节的批次压成 471 字节
     * （约 945:1），而旧的 256KB 保底算出的上限只有 382720 → 被判成放大攻击 → 直接关连接，
     * 玩家秒退（客户端日志：{@code declared rawSize 445524 exceeds expansion limit 382720}）。
     * 按 MC 的规则，只要声明长度不超过 8MB 就是合法的。</p>
     *
     * <p>最终判据是 {@code min(32MB, max(8MB, 压缩字节 × 256))}：小帧给足 8MB（与 MC 一致，
     * 杜绝误杀），大帧仍由倍数约束 —— "1 字节换 32MB"这类攻击照样被拦下。</p>
     */
    private static final int MIN_EXPANSION_ALLOWANCE = 8 * 1024 * 1024;

    /**
     * 常驻 scratch 的上限。超过此尺寸的帧改用临时数组处理。
     *
     * <p>大帧只在进服阶段出现，用临时数组的分配开销可以忽略；而常驻会让
     * 200 人在线时仅此一项就吃掉数 GB。</p>
     */
    private static final int SCRATCH_KEEP_LIMIT = 512 * 1024;

    /** 代数入口接受完整非负int范围；一致性由后续相等校验保证。 */
    private static final int MAX_DICT_GEN = Integer.MAX_VALUE;

    private ZstdDecompressCtx decompressCtx;

    /**
     * 本方向的滚动历史（协议 v7）。
     *
     * <p>⚠️ 必须与<b>对端编码器</b>逐字节对称地喂入：它在成帧后喂"这一帧的原始字节"，
     * 本类在解码出同一帧后喂同一个 {@code byte[]}。两端由此算出相同的字典与代数，
     * 见 {@link ZstdRollingDict} 的类注释。</p>
     */
    private final ZstdRollingDict rolling = new ZstdRollingDict();

    /** 已装载进解压上下文的那份历史字典。 */
    private byte[] appliedRollingDict;

    /** 压缩输入 scratch（方法内即用即弃，可安全复用） */
    private byte[] scratch = new byte[0];
    /** {@link #readVarIntAt} 的结束位置出参（避免热路径上每包一次 int[] 分配） */
    private int varintEnd;
    /** 传给 {@link ZstdVarInts#readAt} 的出参容器（复用，零分配） */
    private final int[] varintEndCursor = new int[1];

    // ────────────────────────────────────────────────────────────────
    // 子类钩子：两端唯一的差异点
    // ────────────────────────────────────────────────────────────────

    /**
     * 解析本连接的解压上下文（{@code handlerAdded} 时调用一次）。
     *
     * @return 解压上下文；{@code null} 表示该连接不可用（收到帧时会 fail-fast）
     */
    /** 单帧最多允许多少块（并行压缩上限的兜底，防止对端声明一个巨大的块数）。 */
    protected static final int MAX_BLOCK_COUNT = 64;

    /** 块头的最大开销：{@code blockCount} 加每块两个 varint（各 5 字节）。 */
    protected static final int MAX_BLOCK_HEADER_BYTES = 5 + MAX_BLOCK_COUNT * 10;

    protected abstract ZstdDecompressCtx resolveDecompressContext(ChannelHandlerContext ctx);

    /** 本方向的滚动历史（只读用途：诊断命令里读代数）。 */
    protected final ZstdRollingDict rollingDict() {
        return rolling;
    }

    /**
     * 本帧实际要使用的解压上下文（每帧调用）。
     *
     * <p>默认返回 {@code handlerAdded} 时解析出的实例——服务端两端的上下文生命周期与
     * 连接一致，够用。<b>客户端必须覆写</b>：它在重协商到"无字典"的服务器时会换掉
     * 上下文来清空旧字典，旧实例随即被 close。</p>
     */
    protected ZstdDecompressCtx decompressContextForFrame() {
        return decompressCtx;
    }

    /**
     * 直存帧的统计回调（{@code frameBodyBytes} = 单包字节数，不含rawSize=0标记）。
     * 服务端无统计，空实现即可。
     */
    protected abstract void onStoredFrame(int frameBodyBytes);

    /**
     * 压缩帧的统计回调。
     *
     * @param inBytes  完整帧体字节数，含rawSize实际头跨度，不含MC外层长度
     * @param outBytes 解压后的字节数
     */
    protected abstract void onCompressedFrame(int inBytes, int outBytes);

    // ────────────────────────────────────────────────────────────────
    // 共享实现
    // ────────────────────────────────────────────────────────────────

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        decompressCtx = resolveDecompressContext(ctx);
        if (decompressCtx == null) {
            LOGGER.warn("[Zstd] decoder installed without a decompress context on this channel");
        }
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        if (!in.isReadable()) return;
        if (!ctx.channel().isActive()) {
            // 通道已关闭：丢弃残留数据，避免对已释放的解压上下文反复报错刷屏
            in.skipBytes(in.readableBytes());
            return;
        }

        // 帧起点：任何"半帧"返回都必须把读指针退回到这里。
        // ⚠️ 只对第一个 varint 退是不够的：后面的 dictGen / blockCount 同样可能读到半截，
        // 而那时 rawSize 已经被消费掉 —— 直接 return 会让下一次 decode 把帧体首字节当成新帧的
        // rawSize，整条连接从此错位（表现为成片 "illegal frame header"）。
        // 正常管线里 splitter 只交完整帧，这条路径理论上走不到，但它是"读一半就返回"的唯一正确写法。
        int frameStart = in.readerIndex();

        int rawSize = ZstdVarInts.tryRead(in, ZstdVarInts.DEFAULT_MAX_VALUE);
        if (rawSize == ZstdVarInts.NEED_MORE) {
            in.readerIndex(frameStart);
            return; // 半帧：等待更多数据
        }
        if (rawSize == ZstdVarInts.INVALID) {
            protocolError(ctx, "illegal frame header (bad varint or rawSize > "
                    + ZstdVarInts.DEFAULT_MAX_VALUE);
            return;
        }

        int remaining = in.readableBytes();
        int frameBodyBytes = remaining + in.readerIndex() - frameStart;
        if (remaining <= 0) {
            protocolError(ctx, "empty frame body (rawSize=" + rawSize + ")");
            return;
        }

        if (rawSize == 0) {
            // 未压缩帧：帧体<b>恰好是一个包</b>。这里之所以（也<b>必须</b>）完全不区分
            // "原版未压缩帧"与"zstd 直存帧"，是因为编码侧把直存帧做成了与原版帧逐字节同形
            //（{@code [0][packet]}，见 ZstdBatchEncoderBase 的帧格式一节）：两种解释结果相同，
            // 因此不存在任何"看起来像不像 zstd 帧"的启发式判断（那正是残留风险的来源）。
            onStoredFrame(remaining);
            // 历史记账的形态是 {@code [varint pktLen][pkt]}，与编码器喂进去的完全一致；
            byte[] batch = singlePacketBatch(in, remaining);
            feedHistory(batch);
            emit(ctx, in.readRetainedSlice(remaining));
            return;
        }

        // 压缩帧现在含"块数 + 每块的 (rawLen, chunkLen)"头，所以允许比 rawSize 略大：
        // 不可压缩数据压出来几乎等于原文，块头会把总量顶过 rawSize。
        if (remaining > rawSize + MAX_BLOCK_HEADER_BYTES) {
            protocolError(ctx, "compressed size " + remaining + " >> declared rawSize " + rawSize);
            return;
        }

        // 压缩比上限：rawSize 是对端声明的，不设上限就会变成"1 字节换 32MB"的放大攻击
        long allowed = Math.min((long) ZstdVarInts.DEFAULT_MAX_VALUE,
                (long) remaining * MAX_EXPANSION_RATIO + MIN_EXPANSION_ALLOWANCE);
        if (rawSize > allowed) {
            protocolError(ctx, "declared rawSize " + rawSize + " exceeds expansion limit " + allowed
                    + " for " + remaining + " compressed byte(s)");
            return;
        }

        // 协议 v7：rawSize 之后是 dictGen（滚动历史代数）。
        // ⚠️ 这不是"可选的优化字段"，而是<b>一致性校验</b>：两端的滑窗必须逐字节相同，
        // 一旦代数对不上就说明历史已经分叉（版本不一致 / 某一端的喂入点被改坏），
        // 此时解压出来的是垃圾。宁可在这一帧上按协议错乱关连接并留一条可归因的日志，
        // 也不能让脏数据流进包解码器 —— 后者表现为"莫名断连 + 客户端协议错误"，几乎无法定位。
        int dictGen = ZstdVarInts.tryRead(in, MAX_DICT_GEN);
        if (dictGen == ZstdVarInts.NEED_MORE) {
            in.readerIndex(frameStart); // 半帧：整帧退回去等更多字节
            return;
        }
        if (dictGen == ZstdVarInts.INVALID) {
            protocolError(ctx, "illegal dictGen in frame header (rawSize=" + rawSize + ")");
            return;
        }
        if (dictGen != rolling.generation()) {
            protocolError(ctx, "dict generation mismatch: frame=" + dictGen
                    + ", local=" + rolling.generation()
                    + " (两端历史已分叉，多半是一端版本不一致)");
            return;
        }

        // 其后是 blockCount，再逐块 [blockRawLen][chunkLen][chunk]。
        int blockCount = ZstdVarInts.tryRead(in, MAX_BLOCK_COUNT);
        if (blockCount == ZstdVarInts.NEED_MORE) {
            in.readerIndex(frameStart); // 同上：半帧整帧退回
            return;
        }
        if (blockCount <= 0) {
            protocolError(ctx, "illegal blockCount in frame header (rawSize=" + rawSize + ")");
            return;
        }

        ZstdDecompressCtx dctx = decompressContextForFrame();
        if (dctx == null) {
            protocolError(ctx, "decompress context unavailable");
            return;
        }

        byte[] data = new byte[rawSize];
        int off = 0;
        try {
            for (int i = 0; i < blockCount; i++) {
                int blockRaw = ZstdVarInts.tryRead(in, rawSize);
                int chunkLen = ZstdVarInts.tryRead(in, in.readableBytes());
                // 与前三处帧头读取保持同一语义：半帧就把整帧退回去等更多字节，不能当非法帧关连接。
                // 正常管线里 splitter 只交完整帧，这一支走不到——但它存在的意义是"语义一致"，
                // 而不是靠"理论上不会发生"来保证正确性。
                if (blockRaw == ZstdVarInts.NEED_MORE || chunkLen == ZstdVarInts.NEED_MORE) {
                    in.readerIndex(frameStart);
                    return;
                }
                if (blockRaw <= 0 || chunkLen <= 0
                        || blockRaw > rawSize - off || chunkLen > in.readableBytes()) {
                    protocolError(ctx, "illegal block " + i + " (blockRaw=" + blockRaw
                            + ", chunk=" + chunkLen + ", left=" + (rawSize - off) + ")");
                    return;
                }
                byte[] chunk = acquireScratch(chunkLen);
                in.readBytes(chunk, 0, chunkLen);
                // 直接解进 data 的偏移处：zstd-jni 的 decompress(src,..,originalSize) 内部
                // 是 new byte[originalSize] 再整块拷贝，分块帧因此会多分配一份 rawSize
                // 级别的数组并多一次全量 memcpy（进服 Registry 那种 4 块帧最吃亏）。
                long n = dctx.decompressByteArray(data, off, rawSize - off, chunk, 0, chunkLen);
                if (n != blockRaw) {
                    protocolError(ctx, "block " + i + " decompressed to " + n
                            + " but declared " + blockRaw);
                    return;
                }
                off += blockRaw;
            }
            onCompressedFrame(frameBodyBytes, data.length);
            // 历史记账：与编码器同一帧、同一份字节、同样一次喂入
            feedHistory(data);
            splitAndEmit(ctx, data, out);
        } catch (Exception e) {
            protocolError(ctx, "decompress failed (rawSize=" + rawSize + ", compressed=" + remaining
                    + "): " + e);
        }
    }

    /** 直存单包补成[pktLen][packet]，与编码侧滚动历史逐字节对称。 */
    private static byte[] singlePacketBatch(ByteBuf in, int len) {
        byte[] out = new byte[ZstdVarInts.length(len) + len];
        int off = ZstdVarInts.writeTo(out, 0, len);
        in.getBytes(in.readerIndex(), out, off, len);
        return out;
    }

    /**
     * 解出来的包交给谁：从哪个 handler 的 context 往下 fire。
     *
     * <p>默认 {@code null} = 从本处理器自己的 ctx 往下（= 仍会经过下游的原版解压处理器）。</p>
     *
     * <p>⚠️ <b>客户端必须覆写为"原版解压处理器"本身</b>（{@code decompress} /
     * {@code compression-decoder}），理由见类注释的"为什么不能再套一层 [0]"。</p>
     */
    protected ChannelHandlerContext decodeForwardTarget(ChannelHandlerContext ctx) {
        return null;
    }

    /** 交出一个<b>已解好</b>的包：跳过原版解压处理器，直接进包解码器。 */
    private void emit(ChannelHandlerContext ctx, ByteBuf msg) {
        ChannelHandlerContext target = decodeForwardTarget(ctx);
        if (target == null) {
            target = ctx;
        }
        target.fireChannelRead(msg);
    }

    /**
     * 喂入本帧的原始字节，并（必要时）把新算出的历史字典装进解压上下文。
     *
     * <p>⚠️ 粒度为"<b>一帧一次、整帧喂入</b>"，必须与对端编码器严格相同
     * （压缩帧喂整批原始负载；直存帧喂 {@code [varint pktLen][pkt]}）。粒度一旦不同，
     * 两端历史就会分叉 —— 表现为 {@code dictGen} 校验失败（这是我们刻意做出来的
     * fail-fast，而不是解压出垃圾）。</p>
     */
    private void feedHistory(byte[] raw) {
        rolling.feed(raw, 0, raw.length);
        applyRollingDict();
    }

    /** 把滚动历史算出的字典装载进解压上下文（同一份只装一次）。 */
    private void applyRollingDict() {
        byte[] dict = rolling.activeDict();
        if (dict == null || dict == appliedRollingDict) {
            return;
        }
        ZstdDecompressCtx c = decompressContextForFrame();
        if (c == null) {
            return;
        }
        c.loadDict(dict);
        c.setMagicless(true);
        appliedRollingDict = dict;
        LOGGER.debug("[Zstd] 解压侧历史字典已更新 gen={} ({}B)", rolling.generation(), dict.length);
    }

    /** 压缩帧：把解压出来的 byte[] 按内层长度前缀切成多个包。 */
    private void splitAndEmit(ChannelHandlerContext ctx, byte[] data, List<Object> out) {
        int off = 0;
        int packets = 0;
        while (off < data.length) {
            int pktLen = readVarIntAt(data, off);
            if (pktLen <= 0) {
                protocolError(ctx, "illegal inner packet length in compressed frame at offset " + off);
                return;
            }
            off = varintEnd;
            // ⚠️ pktLen 来自对端（最大 2^31-1），必须用 long 相加：int 溢出会让越界判定
            // 失效而放行 2GB 级别的内层长度（回归用例见 FrameLayoutTest）。
            if (off + (long) pktLen > data.length) {
                protocolError(ctx, "inner packet length " + pktLen + " overruns payload ("
                        + off + "+" + pktLen + " > " + data.length + ")");
                return;
            }
            // data 是本帧独占的新数组，wrappedBuffer 共享它是安全的（零拷贝）；
            // 从原版 decompress 的 context 直接下传，避免再次解压或校验直存阈值。
            emit(ctx, Unpooled.wrappedBuffer(data, off, pktLen));
            off += pktLen;
            packets++;
        }
        if (packets == 0) {
            protocolError(ctx, "compressed frame contained no packet");
        }
    }

    /**
     * 从 {@code data[pos]} 起读一个 varint，结束位置写回 {@link #varintEnd}。
     *
     * <p>委托给共享的 {@link ZstdVarInts#readAt}；这里的 {@code int[]} 只作为出参容器
     * 复用（每连接一个，热路径上无分配）。</p>
     */
    private int readVarIntAt(byte[] data, int pos) {
        int value = ZstdVarInts.readAt(data, pos, varintEndCursor);
        varintEnd = varintEndCursor[0];
        return value;
    }

    /**
     * 协议错乱：记录并关闭连接。旧实现把这类错误静默吞掉，导致"连接活着但永远
     * 收不到包"的僵死现象，极难排查。
     */
    private void protocolError(ChannelHandlerContext ctx, String reason) {
        if (!ctx.channel().isActive()) return; // 已关闭：只报一次首因，不刷级联告警
        LOGGER.warn("[Zstd] frame rejected — closing channel: {} (remote={})",
                reason, ctx.channel().remoteAddress());
        ctx.close();
    }

    /**
     * 取一个至少有 {@code size} 字节的输入缓冲。
     *
     * <p>小帧复用常驻 scratch（零分配）；大帧（超过 {@link #SCRATCH_KEEP_LIMIT}）
     * 返回临时数组，用完即弃。</p>
     */
    private byte[] acquireScratch(int size) {
        if (size > SCRATCH_KEEP_LIMIT) {
            // 超大帧：临时分配，用完即弃（不常驻——否则"连过的最大包"会永久占住缓冲）
            return new byte[size];
        }
        if (scratch.length < size) {
            scratch = new byte[Math.max(size, 8 * 1024)];
        }
        return scratch;
    }

    /**
     * ⚠️ 必须记录异常后再关闭。解码器处理的是<b>不可信输入</b>，静默关闭会把
     * "连接莫名断开"变成无日志事件——本类其它路径都遵守 fail-fast + 单条 WARN 的约定，
     * 唯独这里曾经例外（编码器一侧一直是有日志的，两侧行为不一致）。
     *
     * <p><b>唯一的例外是「通道已经关闭」</b>：此时异常必然是收尾噪音——本插件在断线时会把
     * 挂起的写 promise 失败化，那些 promise 上的 {@code FIRE_EXCEPTION_ON_FAILURE} 监听器
     * 会把异常从 pipeline 头部广播回来，正好撞在本解码器上。这类噪音降为 debug，
     * 否则每次关服/退服都会打一条吓人的 "decoder exception" + 一长串堆栈。
     * 通道仍活跃时保持 WARN：真异常必须留痕。</p>
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // ⚠️ 别的插件用"抛异常"来取消某一条出站包（CraftEngine 的 CancelPacketException
        // 就是这样：它在 encoder 里 throw，靠异常短路 pipeline）。这不是连接错误，
        // 更不该关连接 —— 实测玩家执行 /spawn 触发取消包时被我们按"解码异常"踢下线。
        if (isPacketCancellation(cause)) {
            LOGGER.debug("[Zstd] packet cancelled by another plugin (remote={}): {}",
                    ctx.channel().remoteAddress(), cause.toString());
            return;
        }
        // ⚠️ 连接层 I/O 异常（对端 RST、网络中断）不是解码问题：栈来自 NioSocketChannel.doReadBytes，
        // 与我们的帧格式无关。此时 isActive() 往往还是 true（异常先于 inactive 事件送达），
        // 不单独识别的话会打一条 WARN + 完整堆栈，看上去像插件把玩家踢了。
        if (isConnectionIoError(cause)) {
            LOGGER.debug("[Zstd] connection I/O error (remote={}): {}",
                    ctx.channel().remoteAddress(), cause.toString());
        } else if (!ctx.channel().isActive()) {
            LOGGER.debug("[Zstd] decoder exception after channel closed (remote={}): {}",
                    ctx.channel().remoteAddress(), cause.toString());
        } else {
            LOGGER.warn("[Zstd] decoder exception — closing channel (remote={})",
                    ctx.channel().remoteAddress(), cause);
        }
        if (ctx.channel().isOpen()) {
            ctx.close();
        }
    }

    /**
     * 处理器被移除（连接关闭 / 回落原版）时释放滑窗。
     *
     * <p>滑窗 + 当前字典共 128KB，挂在连接上会一直留到 GC；挂机玩家的连接可能挂几个小时，
     * 200 人在线就是几十 MB 的纯浪费。必须走 {@code super}：{@code ByteToMessageDecoder}
     * 在那里释放累积缓冲（{@code ByteToMessageDecoder#handlerRemoved} 是 final，
     * 子类的扩展点是 {@code handlerRemoved0}）。</p>
     */
    @Override
    protected void handlerRemoved0(ChannelHandlerContext ctx) throws Exception {
        rolling.reset();
        appliedRollingDict = null;
    }

    /** 是否为连接层 I/O 异常（对端重置/中断），而不是协议或解压错误。 */
    private static boolean isConnectionIoError(Throwable cause) {
        return cause instanceof java.io.IOException;
    }

    /** 是否为"发包被其它插件取消"的控制流异常（不是连接错误）。 */
    private static boolean isPacketCancellation(Throwable cause) {
        for (Throwable t = cause; t != null; t = t.getCause()) {
            if (t.getClass().getSimpleName().contains("CancelPacket")) {
                return true;
            }
        }
        return false;
    }
}
