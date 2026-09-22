package mikumc.zstd.protocol;

import com.github.luben.zstd.ZstdCompressCtx;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.nio.channels.ClosedChannelException;

/**
 * Miku-ZSTD 出站编码器基类（帧格式 <b>v3</b>，协商版本 v4）——三端共享实现。
 *
 * <h2>帧格式（v3）</h2>
 * <pre>
 *   [varint bodyLen?][varint rawSize][zstd(payload) | payload]
 *   payload = [varint pktLen][pkt] [varint pktLen][pkt] ...
 * </pre>
 * <p>{@code rawSize == 0} 表示未压缩直存。{@code bodyLen} 只由 <b>服务端</b>写
 * （Velocity 管线无独立 frame-encoder）；客户端侧由 prepender 负责外层长度，
 * 子类通过 {@link #writeBodyLen()} 声明。</p>
 *
 * <h2>批处理</h2>
 * <p>把时间窗（默认 3ms）内发出的多个包合成一帧。实测每包单独成帧时，帧头与
 * 熵表重复发送几乎吃掉小包的全部收益（小包甚至膨胀到 105%，只能直存）。</p>
 * <p>为不破坏批处理，窗口等待期间的 {@code flush()} 被<b>吞掉</b>（数据由窗口
 * 到期后的统一 flush 送出）；无待发包时 flush 照常透传。</p>
 *
 * <h2>为什么放在共享模块</h2>
 * <p>本类与 {@link ZstdBatchDecoderBase} 是两端唯一需要"逐字节对称"的代码：
 * 帧格式一旦漂移，症状是"握手能成、之后静默错乱"，极难定位（2.0.0 曾因客户端
 * 多写一层长度前缀造成过断连回归）。共享单一实现后，这类漂移在结构上不可能发生；
 * 剩余的跨端差异（外层长度、统计口径、配置来源、字典状态）全部收敛为下面这些钩子。</p>
 */
public abstract class ZstdBatchEncoderBase extends ChannelDuplexHandler {

    protected static final Logger LOGGER = LoggerFactory.getLogger("miku-zstd");

    /**
     * 复用缓冲的初始容量。
     *
     * <p>取 8KB 而非 64KB：典型批次只有 2~8KB，而这两个缓冲是<b>每连接</b>各一份——
     * 64KB 初值意味着每连接白占 128KB 堆。扩容均摊 O(1)，不值得为省几次 arraycopy 常驻大数组。</p>
     */
    private static final int INITIAL_BUFFER = 8 * 1024;

    private ZstdCompressCtx compressCtx;
    /** 压缩目标缓冲（渐进扩容复用） */
    private byte[] dstArray;
    /** 原始负载拼接缓冲（复用） */
    private byte[] rawArray;

    private final List<ByteBuf> pending = new ArrayList<>();
    private final List<ChannelPromise> pendingPromises = new ArrayList<>();
    private ScheduledFuture<?> windowTask;

    /** 是否有压缩任务在途（仅 event loop 线程读写） */
    private boolean compressing;
    /** 在途压缩任务持有的 promise（仅 event loop 线程读写）；通道关闭时需一并失败化 */
    private List<ChannelPromise> inflightPromises;

    // ── 压缩线程池 ───────────────────────────────────────────────────────────
    // 共享实现在 ZstdCompressPool，本类只调用它的静态入口。
    //
    // 为什么压缩必须离开 event loop：在 event loop 上同步压缩时，一个玩家的大批次
    //（几毫秒）会阻塞同一 event loop 上所有其他玩家的包处理——表现为"延迟随在线人数
    // 放大"的抖动，而且 ping 上看不出来（它是突发性的，不是固定开销）。
    //
    // 池的规模、活跃/排队数、单次耗时都在 ZstdCompressPool 里统一统计，
    // 命令层可直接取摘要——"要不要降 level / 加线程"只能靠这些数字判断。
    //（这里原本挂了一段 javadoc，但其后紧跟的是别的字段，等于一段没有归属的注释。）

    private int batchWindowMs = 3;
    private int batchMaxPackets = 64;

    // ────────────────────────────────────────────────────────────────
    // 子类钩子：两端唯一的差异点
    // ────────────────────────────────────────────────────────────────

    /**
     * 解析本连接的压缩上下文（{@code handlerAdded} 时调用一次）。
     *
     * @return 压缩上下文；{@code null} 表示该连接不做 zstd，走透传
     */
    protected abstract ZstdCompressCtx resolveCompressContext(ChannelHandlerContext ctx);

    /** 本连接是否已装载压缩字典（影响小包跳过压缩的阈值，每帧查询一次） */
    protected abstract boolean hasCompressDict();

    /** 小包跳过压缩的阈值：低于该值直接直存，不调用 zstd */
    protected abstract int skipCompressThreshold(boolean hasCompressDict);

    /** 帧头是否写外层 {@code bodyLen}（服务端 true；客户端 false——prepender 负责） */
    protected abstract boolean writeBodyLen();

    /**
     * 一帧产出后的统计回调。
     *
     * @param rawBytes   原始字节口径（{@code 1 + 批内原始总量}）
     * @param wireBytes  实际写出的帧字节数
     * @param compressed 该帧是否走了压缩路径
     */
    protected abstract void onFrame(int rawBytes, int wireBytes, boolean compressed);

    /**
     * 批内原始数据采样回调（供字典训练使用）。
     *
     * <p>默认空实现：不需要字典的一端无需关心。实现在此接收的是<b>未经压缩的批次字节</b>
     * （{@code [varint pktLen][pkt]...}），采样方必须自行复制——该缓冲会被复用。</p>
     */
    protected void onRawBatch(byte[] raw, int length) {
    }

    /** 批处理窗口（毫秒），0 = 立刻成帧 */
    protected abstract int configBatchWindowMs();

    /** 单帧最多合并多少个包 */
    protected abstract int configBatchMaxPackets();

    /**
     * 压缩上下文的互斥锁。
     *
     * <p>压缩已卸载到线程池，因此<b>必须与任何改写 {@code compressCtx} 的操作互斥</b>
     * （典型场景：字典训练完成后 {@code loadDict()} 换字典）。默认返回本 handler 实例，
     * 但字典通常由 ChannelManager 持有并改写——那种情况下子类必须覆写为
     * <b>与 ChannelManager 相同的那把锁</b>，否则会与池线程并发使用同一个 ctx。</p>
     */
    protected Object compressLock() {
        return this;
    }

    // ────────────────────────────────────────────────────────────────
    // 共享实现
    // ────────────────────────────────────────────────────────────────

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        compressCtx = resolveCompressContext(ctx);
        if (compressCtx == null) {
            LOGGER.warn("[Zstd] encoder installed without a compress context — passthrough mode");
        }
        batchWindowMs = Math.max(0, configBatchWindowMs());
        batchMaxPackets = Math.max(1, configBatchMaxPackets());
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
        if (!(msg instanceof ByteBuf packet)) {
            ctx.write(msg, promise);
            return;
        }
        if (compressCtx == null) {
            ctx.write(packet, promise); // 透传：不缓冲，保持原语义
            return;
        }
        pending.add(packet);
        pendingPromises.add(promise);

        if (pending.size() >= batchMaxPackets) {
            emitBatch(ctx);
            return;
        }
        scheduleWindow(ctx);
    }

    /**
     * 窗口等待期间吞掉 flush（由窗口到期统一 flush），否则 writeAndFlush 会让每批只剩 1 个包。
     * 无待发包时保持原语义透传。
     */
    @Override
    public void flush(ChannelHandlerContext ctx) {
        if (windowTask == null) {
            ctx.flush();
        }
    }

    private void scheduleWindow(ChannelHandlerContext ctx) {
        if (windowTask != null) return;
        if (batchWindowMs <= 0) {
            emitBatch(ctx); // 窗口为 0：退化为"立刻成帧"
            return;
        }
        windowTask = ctx.executor().schedule(() -> {
            windowTask = null;
            emitBatch(ctx);
        }, batchWindowMs, TimeUnit.MILLISECONDS);
    }

    /**
     * 成帧并写出。压缩路径会<b>卸载到 {@link ZstdCompressPool}</b>，
     * 直存路径（小包）仍在 event loop 上完成——它本来就不调用 zstd，多绕一次线程池不划算。
     */
    private void emitBatch(ChannelHandlerContext ctx) {
        if (windowTask != null) {
            windowTask.cancel(false);
            windowTask = null;
        }
        if (pending.isEmpty()) {
            return;
        }
        // ⚠️ 有压缩任务在途时绝不开始新一批：compressCtx / rawArray / dstArray 都是
        // 「每连接独占」的资源，被两个线程同时用会直接损坏数据；同时这也是保持写出
        // 顺序的关键。积压的数据不会滞留——在途任务完成后的回调会重新检查 pending。
        if (compressing) {
            return;
        }

        List<ChannelPromise> promises = new ArrayList<>(pendingPromises);
        pendingPromises.clear();

        final int rawTotal;
        try {
            rawTotal = concatPending();
        } catch (Throwable t) {
            failAll(promises, t);
            LOGGER.error("[Zstd] encoder batch concat failed", t);
            return;
        }

        final boolean withBodyLen = writeBodyLen();

        // ── 小包直存 ──
        // 实测：无字典时 ≤32B 压缩后膨胀到 102~113%，48B 起才有收益；
        // 有字典时 24B 起才有收益。低于阈值直接直存，省一次 zstd 调用且线路字节更少。
        if (rawTotal < skipCompressThreshold(hasCompressDict())) {
            try {
                writeFrame(ctx, buildStoredFrame(ctx, rawTotal, withBodyLen), promises);
            } catch (Throwable t) {
                failAll(promises, t);
                LOGGER.error("[Zstd] encoder stored-frame build failed", t);
            }
            return;
        }

        // ── 压缩路径：交给线程池 ──
        compressing = true;
        inflightPromises = promises;
        final Object lock = compressLock();

        ZstdCompressPool.execute(() -> {
            final int produced;
            try {
                // 与「换字典」互斥：ctx 不是线程安全的
                synchronized (lock) {
                    ensureCompressDstCapacity(rawTotal);
                    // 第 3 个参数是"目标缓冲区可用空间"，必须传数组实际长度
                    // （传 srcSize 会报 Destination buffer is too small）
                    produced = compressCtx.compressByteArray(
                            dstArray, 0, dstArray.length, rawArray, 0, rawTotal);
                }
            } catch (Throwable t) {
                backToEventLoop(ctx, promises, t, 0, rawTotal, withBodyLen);
                return;
            }
            backToEventLoop(ctx, promises, null, produced, rawTotal, withBodyLen);
        });
    }

    /** 压缩完成后回到 event loop：组帧、写出、并立刻接手积压的下一批。 */
    private void backToEventLoop(ChannelHandlerContext ctx, List<ChannelPromise> promises,
                                 Throwable failure, int produced, int rawTotal, boolean withBodyLen) {
        ctx.executor().execute(() -> {
            compressing = false;
            inflightPromises = null;

            if (failure != null) {
                failAll(promises, failure);
            } else if (!ctx.channel().isActive()) {
                // 通道已关：结果无处可去，promise 也已由 channelInactive 失败化（tryFailure 幂等）
                failAll(promises, new ClosedChannelException());
                return;
            } else {
                try {
                    boolean compressed = produced > 0 && produced < rawTotal;
                    writeFrame(ctx, buildCompressedFrame(ctx, rawTotal, produced, compressed, withBodyLen), promises);
                } catch (Throwable t) {
                    failAll(promises, t);
                    LOGGER.error("[Zstd] encoder compressed-frame build failed", t);
                }
            }

            // 压缩期间攒下的包：立刻接着发（不设 compressing 了，可以再走一轮）
            if (!pending.isEmpty()) {
                emitBatch(ctx);
            }
        });
    }

    /** 把 pending 拼成 {@code [varint pktLen][pkt]...}，返回原始总长；随后释放并清空 pending。 */
    private int concatPending() {
        int rawTotal = 0;
        for (ByteBuf b : pending) {
            rawTotal += ZstdVarInts.length(b.readableBytes()) + b.readableBytes();
        }
        ensureRawCapacity(rawTotal);
        int off = 0;
        for (ByteBuf b : pending) {
            int n = b.readableBytes();
            off = ZstdVarInts.writeTo(rawArray, off, n);
            b.getBytes(b.readerIndex(), rawArray, off, n);
            off += n;
        }
        onRawBatch(rawArray, rawTotal); // 采样（必须在缓冲复用前）
        releasePending();
        return rawTotal;
    }

    /** 写出帧并完成这一批的 promise。 */
    private void writeFrame(ChannelHandlerContext ctx, ByteBuf frame, List<ChannelPromise> promises) {
        ctx.write(frame).addListener(f -> {
            for (ChannelPromise p : promises) {
                if (f.isSuccess()) {
                    p.trySuccess();
                } else {
                    p.tryFailure(f.cause());
                }
            }
        });
        ctx.flush();
    }

    private static void failAll(List<ChannelPromise> promises, Throwable cause) {
        for (ChannelPromise p : promises) {
            p.tryFailure(cause);
        }
    }

    /** 直存帧：{@code [bodyLen?][0][payload]}。 */
    private ByteBuf buildStoredFrame(ChannelHandlerContext ctx, int rawTotal, boolean withBodyLen) {
        ByteBuf frame;
        if (withBodyLen) {
            int bodyLen = 1 + rawTotal; // rawSize=0（1 字节 varint）+ payload
            frame = ctx.alloc().directBuffer(ZstdVarInts.length(bodyLen) + bodyLen);
            ZstdVarInts.write(frame, bodyLen);
        } else {
            frame = ctx.alloc().directBuffer(ZstdVarInts.length(0) + rawTotal);
        }
        ZstdVarInts.write(frame, 0);
        frame.writeBytes(rawArray, 0, rawTotal);
        onFrame(1 + rawTotal, frame.readableBytes(), false);
        return frame;
    }

    /** 压缩帧（或压缩无收益时的直存）：{@code [bodyLen?][rawSize][payload]}。 */
    private ByteBuf buildCompressedFrame(ChannelHandlerContext ctx, int rawTotal, int outLen,
                                         boolean compressed, boolean withBodyLen) {
        int rawSizeField = compressed ? rawTotal : 0;
        int payloadLen = compressed ? outLen : rawTotal;

        ByteBuf frame;
        if (withBodyLen) {
            int bodyLen = ZstdVarInts.length(rawSizeField) + payloadLen;
            frame = ctx.alloc().directBuffer(ZstdVarInts.length(bodyLen) + bodyLen);
            ZstdVarInts.write(frame, bodyLen);
        } else {
            frame = ctx.alloc().directBuffer(ZstdVarInts.length(rawSizeField) + payloadLen);
        }
        ZstdVarInts.write(frame, rawSizeField);
        if (compressed) {
            frame.writeBytes(dstArray, 0, outLen);
        } else {
            frame.writeBytes(rawArray, 0, rawTotal);
        }
        onFrame(1 + rawTotal, frame.readableBytes(), compressed);
        return frame;
    }

    private void releasePending() {
        for (ByteBuf b : pending) {
            ReferenceCountUtil.release(b);
        }
        pending.clear();
    }

    /**
     * 压缩目标缓冲（按需增长复用）。
     *
     * <p>初值刻意取得小（8KB）而不是 64KB：实际批次通常只有 2~8KB，而 64KB 初值是
     * <b>每连接</b>两份（本缓冲 + rawArray），200 连接光初值就白占 25MB。
     * 扩容是均摊 O(1)，大流量下多几次 arraycopy 远比常驻大数组划算。</p>
     */
    private void ensureCompressDstCapacity(int srcSize) {
        int needed = srcSize + (srcSize >>> 8) + 64;
        if (dstArray == null || dstArray.length < needed) {
            dstArray = new byte[Math.max(needed, INITIAL_BUFFER)];
        }
    }

    private void ensureRawCapacity(int needed) {
        if (rawArray == null || rawArray.length < needed) {
            rawArray = new byte[Math.max(needed, INITIAL_BUFFER)];
        }
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        if (windowTask != null) {
            windowTask.cancel(false);
            windowTask = null;
        }
        if (!pending.isEmpty() && ctx.channel().isActive()) {
            emitBatch(ctx); // 处理器被移除前把残留数据送出去，避免丢包
        } else {
            releasePending();
            failPending(new ClosedChannelException());
            pendingPromises.clear();
        }
        failInflight(new ClosedChannelException());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        if (windowTask != null) {
            windowTask.cancel(false);
            windowTask = null;
        }
        releasePending();
        failPending(new ClosedChannelException());
        pendingPromises.clear();
        failInflight(new ClosedChannelException());
        super.channelInactive(ctx);
    }

    /**
     * 通道关闭 / handler 被移除时，把「尚未成帧」的写 promise 统一失败化。
     *
     * <p>promise 必须被完成（否则上游 future 永久挂起），但 <b>cause 必须用 Netty 标准的
     * {@link ClosedChannelException}</b>：这些 promise 上挂着
     * {@code ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE}（Minecraft 发包时挂的），
     * 一旦失败它就会 {@code pipeline().fireExceptionCaught(cause)} 从 pipeline 头部广播出去。
     * 曾经这里 new 的是 {@code IllegalStateException("channel closed")}，于是关服/退服时
     * 必然打出一条 "decoder exception" + 20 行堆栈的假报错。</p>
     */
    private void failPending(ClosedChannelException cause) {
        for (ChannelPromise p : pendingPromises) {
            p.tryFailure(cause);
        }
    }

    /**
     * 把「在途压缩任务」持有的 promise 也失败化。
     *
     * <p>这批 promise 已经被移出 {@code pendingPromises}，通道关闭时若不处理，
     * 上游会一直等一个永远不会完成的 future。任务回调里还会再失败一次，
     * 但 {@code tryFailure} 幂等，重复无害。</p>
     */
    private void failInflight(ClosedChannelException cause) {
        List<ChannelPromise> inflight = inflightPromises;
        if (inflight != null) {
            for (ChannelPromise p : inflight) {
                p.tryFailure(cause);
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // 通道已关闭时的异常一律是收尾噪音（本插件在断线时会失败化挂起的写 promise，
        // 那些 promise 的监听器会把异常广播回 pipeline），降为 debug，
        // 避免关服/退服时刷 ERROR + 堆栈。
        if (!ctx.channel().isActive()) {
            LOGGER.debug("[Zstd] encoder exception after channel closed: {}", cause.toString());
        } else {
            LOGGER.error("[Zstd] encoder exception, closing channel", cause);
        }
        ctx.close();
    }
}
