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
 * Miku-ZSTD 入站解码器基类（帧格式 <b>v3</b>，协商版本 v4）——三端共享实现。
 *
 * <h2>帧格式（v3）</h2>
 * <pre>
 *   [varint rawSize][zstd(payload) | payload]
 *   payload = [varint pktLen][pkt] [varint pktLen][pkt] ...
 * </pre>
 * <p>{@code rawSize == 0} 表示未压缩直存。一帧可含<b>多个</b>包，按内层
 * {@code [varint pktLen]} 逐个切出并下传（v2 是一帧一包，内层没有长度前缀）。</p>
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
     * ——<b>1 字节输入换 32MB 分配</b>，放大三千多万倍。压一个倍数上限即可根治。</p>
     *
     * <p>取值足够宽松（实测真实流量的整体压缩比约 8~20:1），不会误伤合法的
     * 高压缩比数据；真正的巨量分配只可能来自恶意声明。</p>
     */
    private static final int MAX_EXPANSION_RATIO = 256;

    /** 倍数检查的保底额度：小帧允许解压到这个大小，避免误伤"极小帧高压缩比"的合法情况 */
    private static final int MIN_EXPANSION_ALLOWANCE = 256 * 1024;

    /**
     * 常驻 scratch 的上限。超过此尺寸的帧改用临时数组处理。
     *
     * <p>大帧只在进服阶段出现，用临时数组的分配开销可以忽略；而常驻会让
     * 200 人在线时仅此一项就吃掉数 GB。</p>
     */
    private static final int SCRATCH_KEEP_LIMIT = 512 * 1024;

    /**
     * 大帧（超过 {@link #SCRATCH_KEEP_LIMIT}）的临时分配统计。
     *
     * <p>刻意<b>不做池化</b>：这类帧只在进服阶段出现（Registry 等一次性大数据），
     * 频率极低，而池化需要一个跨连接的共享缓冲——那就要加锁，反而把 event loop
     * 拖进争用。所以这里只把「发生了多少次、共多少字节」记下来，
     * 让"要不要调大常驻上限"有据可依，而不是凭感觉改。</p>
     */
    private static final java.util.concurrent.atomic.LongAdder TRANSIENT_ALLOCS =
            new java.util.concurrent.atomic.LongAdder();
    private static final java.util.concurrent.atomic.LongAdder TRANSIENT_BYTES =
            new java.util.concurrent.atomic.LongAdder();

    /** 大帧临时分配统计（命令层用）。 */
    public static String scratchStats() {
        long n = TRANSIENT_ALLOCS.sum();
        long bytes = TRANSIENT_BYTES.sum();
        return String.format("常驻上限 %dKB | 大帧临时分配 %,d 次 / 共 %.1fMB",
                SCRATCH_KEEP_LIMIT / 1024, n, bytes / 1048576.0);
    }

    private ZstdDecompressCtx decompressCtx;
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
    protected abstract ZstdDecompressCtx resolveDecompressContext(ChannelHandlerContext ctx);

    /**
     * 直存帧的统计回调（{@code frameBodyBytes} = 帧体字节数）。
     * 服务端无统计，空实现即可。
     */
    protected abstract void onStoredFrame(int frameBodyBytes);

    /**
     * 解压帧的数据采样回调（供字典训练使用）。
     *
     * <p>默认空实现。传入的 {@code data} 是本帧独占的新数组，采样方可直接持有引用。</p>
     *
     * <p><b>实现建议</b>：整帧一次性交给采样方（格式与直存帧相同，都是
     * {@code [varint pktLen][pkt]...}），由采样方在锁外解析、只为真正入库的样本拷贝。
     * 早期实现在这里逐包 {@code new byte[]} + {@code arraycopy}，等于给每个入站包
     * 都加了一次堆分配——而编码侧早已改成整批提交。</p>
     */
    protected void onDecodedPayload(byte[] data) {
    }

    /**
     * 直存帧的负载采样回调（{@code data} 的格式与解压帧相同：{@code [varint pktLen][pkt]...}）。
     *
     * <p>默认空实现。<b>直存帧恰恰是字典收益的主战场</b>——按定义它们都是"小到不值得压缩"
     * 的包。早期实现只对解压帧采样，于是入站训练集里只有大包，字典在最该发力的地方
     * 反而没有样本。只对服务端两侧有意义（客户端不训练字典）。</p>
     *
     * <p>需要采样的子类必须同时覆写 {@link #wantsStoredPayload()} 返回 {@code true}，
     * 否则本方法不会被调用——这样不需要采样的一端不必为此付出一次拷贝。</p>
     */
    protected void onStoredPayload(byte[] data) {
    }

    /**
     * 是否需要直存帧的负载采样。
     *
     * <p>默认 {@code false}：不需要采样的一端（Fabric 客户端）连拷贝都不会发生。</p>
     */
    protected boolean wantsStoredPayload() {
        return false;
    }

    /**
     * 压缩帧的统计回调。
     *
     * @param inBytes  收到的压缩字节数
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

        int rawSize = ZstdVarInts.tryRead(in, ZstdVarInts.DEFAULT_MAX_VALUE);
        if (rawSize == ZstdVarInts.NEED_MORE) {
            return; // 半帧：等待更多数据
        }
        if (rawSize == ZstdVarInts.INVALID) {
            protocolError(ctx, "illegal frame header (bad varint or rawSize > "
                    + ZstdVarInts.DEFAULT_MAX_VALUE + ")");
            return;
        }

        int remaining = in.readableBytes();
        if (remaining <= 0) {
            protocolError(ctx, "empty frame body (rawSize=" + rawSize + ")");
            return;
        }

        if (rawSize == 0) {
            // 直存帧：payload 就在输入缓冲里，用 retained slice 零拷贝切分
            onStoredFrame(remaining);
            if (wantsStoredPayload()) {
                // 直存帧按定义一定不大（超过阈值就会被压缩），整帧拷一份的代价可忽略；
                // 但采样方要求"独占数组"（帧缓冲会被复用），所以这里必须拷。
                byte[] stored = new byte[remaining];
                in.getBytes(in.readerIndex(), stored, 0, remaining);
                onStoredPayload(stored);
            }
            splitAndEmit(ctx, in, remaining, out);
            return;
        }

        if (remaining > rawSize) {
            protocolError(ctx, "compressed size " + remaining + " > declared rawSize " + rawSize);
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

        byte[] src = acquireScratch(remaining);
        in.getBytes(in.readerIndex(), src, 0, remaining);

        ZstdDecompressCtx dctx = decompressCtx;
        if (dctx == null) {
            protocolError(ctx, "decompress context unavailable");
            return;
        }
        try {
            byte[] data = dctx.decompress(src, 0, remaining, rawSize);
            in.skipBytes(remaining);
            onCompressedFrame(remaining, data.length);
            onDecodedPayload(data); // 采样（data 为本帧独占数组，可安全持有）
            splitAndEmit(ctx, data, out);
        } catch (Exception e) {
            protocolError(ctx, "decompress failed (rawSize=" + rawSize + ", compressed=" + remaining
                    + "): " + e);
        }
    }

    /** 直存帧：在输入缓冲上按内层长度前缀切出各包（零拷贝 retained slice）。 */
    private void splitAndEmit(ChannelHandlerContext ctx, ByteBuf in, int total, List<Object> out) {
        int consumed = 0;
        int packets = 0;
        while (consumed < total) {
            int before = in.readerIndex();
            int pktLen = ZstdVarInts.tryRead(in, ZstdVarInts.DEFAULT_MAX_VALUE);
            if (pktLen <= 0) {
                protocolError(ctx, "illegal inner packet length in stored frame at offset " + consumed);
                return;
            }
            consumed += in.readerIndex() - before;
            if (consumed + pktLen > total) {
                protocolError(ctx, "inner packet length " + pktLen + " overruns frame ("
                        + consumed + "+" + pktLen + " > " + total + ")");
                return;
            }
            out.add(in.readRetainedSlice(pktLen));
            consumed += pktLen;
            packets++;
        }
        if (packets == 0) {
            protocolError(ctx, "stored frame contained no packet");
        }
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
            if (off + pktLen > data.length) {
                protocolError(ctx, "inner packet length " + pktLen + " overruns payload ("
                        + off + "+" + pktLen + " > " + data.length + ")");
                return;
            }
            // data 是本帧独占的新数组，wrappedBuffer 共享它是安全的（零拷贝）
            out.add(Unpooled.wrappedBuffer(data, off, pktLen));
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
            TRANSIENT_ALLOCS.increment();
            TRANSIENT_BYTES.add(size);
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
        if (!ctx.channel().isActive()) {
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
}
