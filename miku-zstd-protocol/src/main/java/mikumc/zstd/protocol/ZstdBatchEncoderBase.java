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
 * <p>把时间窗（各端配置项决定，默认 1ms）内发出的多个包合成一帧。实测每包单独成帧时，帧头与
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

    /**
     * 常驻缓冲上限。超过此尺寸的批次改用临时数组（用完即弃），对齐解码侧
     * {@link ZstdBatchDecoderBase} 的同名策略。
     *
     * <p>进服阶段的区块/注册表大帧会把这两块缓冲顶到几十 MB；若常驻，200 人在线
     * 仅此一项就是 GB 级。大帧只在进服时出现，临时分配的代价可忽略。</p>
     */
    private static final int SCRATCH_KEEP_LIMIT = 512 * 1024;

    private ZstdCompressCtx compressCtx;
    /** 压缩目标缓冲（渐进扩容复用；只增到 {@link #SCRATCH_KEEP_LIMIT}，超限走临时数组） */
    private byte[] dstArray;
    /** 原始负载拼接缓冲（复用；只增到 {@link #SCRATCH_KEEP_LIMIT}，超限走临时数组） */
    private byte[] rawArray;
    /**
     * 当前批次<b>实际</b>使用的原始缓冲：小批次是常驻的 {@link #rawArray}，大帧是临时数组。
     * 压缩在池线程执行，必须把实例捕获进 lambda，不能在池线程里重新读字段
     *（那时字段可能已被下一次成帧换掉）。
     */
    private byte[] batchRaw;

    private final List<ByteBuf> pending = new ArrayList<>();
    private final List<ChannelPromise> pendingPromises = new ArrayList<>();
    private ScheduledFuture<?> windowTask;

    /** 是否有压缩任务在途（仅 event loop 线程读写） */
    private boolean compressing;
    /** 在途压缩任务持有的 promise（仅 event loop 线程读写）；通道关闭时需一并失败化 */
    private List<ChannelPromise> inflightPromises;
    /** handler 被移除后，异步压缩回调不得再向当前 pipeline 写帧。 */
    private boolean removed;

    /**
     * 独享锁（子类钩子为默认值 {@link #compressLock()} 时使用）。
     *
     * <p>真正需要在池线程与 {@code close()} 之间做互斥的端，必须让
     * {@link #compressLock()} 返回<b>与 ChannelManager 同一个</b> {@link ZstdCompressLock}
     * —— 那里带着"上下文已关闭"标志（见该类的类注释，它防的是一次会让进程 SIGSEGV 的
     * use-after-free）。</p>
     */
    private final ZstdCompressLock ownLock = new ZstdCompressLock();

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

    /**
     * 批处理窗口与单帧包数上限。这里的初值只是"{@code handlerAdded} 之前万一被读到"的兜底
     * ——真正生效的值在任何写操作都可能发生之前就已由 {@code handlerAdded} 从各端配置写入
     *（见该处的 {@code Math.max(...)}）。因此不要把配置默认值写在这里：两处一旦不一致，
     * 读代码的人无法判断哪个才是生效值。
     */
    private int batchWindowMs = 1;
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
     * （典型场景：字典训练完成后 {@code loadDict()} 换字典）。默认返回本 handler 独占的
     * {@link ZstdCompressLock}，但字典通常由 ChannelManager 持有并改写——那种情况下子类
     * <b>必须</b>覆写为与 ChannelManager 相同的那把锁，否则会与池线程并发使用同一个 ctx。</p>
     *
     * <p>⚠️ 返回类型刻意是 {@link ZstdCompressLock} 而非 {@code Object}：池线程需要它来
     * 判断"上下文是否已被 close"（见该类注释——少这一判就是一次 use-after-free 崩溃）。</p>
     */
    protected ZstdCompressLock compressLock() {
        return ownLock;
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
        if (removed) {
            ReferenceCountUtil.release(msg);
            promise.tryFailure(new ClosedChannelException());
            return;
        }
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
        if (removed) {
            releasePending();
            failPending(new ClosedChannelException());
            pendingPromises.clear();
            return;
        }
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

        int batchCount = takeFrameBatch();
        if (batchCount == 0) {
            if (!removed && !pending.isEmpty()) {
                schedulePendingBatch(ctx);
            }
            return;
        }
        List<ByteBuf> batchPackets = new ArrayList<>(pending.subList(0, batchCount));
        List<ChannelPromise> promises = new ArrayList<>(pendingPromises.subList(0, batchCount));

        final int rawTotal;
        final byte[] raw;
        try {
            rawTotal = concatPending(batchPackets);
            raw = batchRaw; // 本批实际使用的缓冲：大帧是临时数组，必须捕获实例传给池线程
            // 立刻摘掉字段引用：此后只有上面这个局部变量持有它。否则大帧（如进服阶段的
            // 注册表帧，几十 MB）会被字段一直留到下一批 —— 刚进服就挂机的连接会白占一份。
            batchRaw = null;
        } catch (Throwable t) {
            // ⚠️ concatPending 可能在**释放 pending 之前**就抛出来（采样回调抛异常、超大批次
            // 分配失败等）。此时 pending 里仍压着已 retain 的 ByteBuf：不在这里释放并清空，
            // 它们永远没人释放（引用计数泄漏），而且下一批会把这批残缺数据一并发出去，
            // 同时让 pendingPromises 与 pending 彻底失去对应关系。
            batchRaw = null; // 失败批次的缓冲也不再被本 handler 引用（大帧可立即回收）
            pending.subList(0, batchCount).clear();
            pendingPromises.subList(0, batchCount).clear();
            releasePackets(batchPackets);
            failAll(promises, t);
            LOGGER.error("[Zstd] encoder batch concat failed", t);
            if (!removed && !pending.isEmpty()) {
                schedulePendingBatch(ctx);
            }
            return;
        }
        pending.subList(0, batchCount).clear();
        pendingPromises.subList(0, batchCount).clear();

        final boolean withBodyLen = writeBodyLen();

        // ── 小包直存 ──
        // 实测：无字典时 ≤32B 压缩后膨胀到 102~113%，48B 起才有收益；
        // 有字典时 24B 起才有收益。低于阈值直接直存，省一次 zstd 调用且线路字节更少。
        if (rawTotal < skipCompressThreshold(hasCompressDict())) {
            try {
                writeFrame(ctx, buildStoredFrame(ctx, raw, rawTotal, withBodyLen), promises);
            } catch (Throwable t) {
                failAll(promises, t);
                LOGGER.error("[Zstd] encoder stored-frame build failed", t);
            }
            if (!pending.isEmpty()) {
                schedulePendingBatch(ctx);
            }
            return;
        }

        // ── 压缩路径：交给线程池 ──
        compressing = true;
        inflightPromises = promises;
        final ZstdCompressLock lock = compressLock();

        try {
            ZstdCompressPool.execute(() -> {
                final int produced;
                final byte[] dst;
                try {
                    // 与「换字典 / close」互斥：ctx 不是线程安全的，且可能已被释放
                    synchronized (lock) {
                        // ⚠️ 拿到锁之后再判一次：本任务可能在连接断开**之前**提交、
                        // 在 close() 释放 native 上下文**之后**才轮到执行。此时必须直接放弃，
                        // 绝不能碰 compressCtx（那是已释放的内存 → SIGSEGV，整进程崩）。
                        // 判据必须在锁内读——见 ZstdCompressLock 的类注释。
                        if (lock.isClosed()) {
                            backToEventLoop(ctx, promises, new ClosedChannelException(),
                                    0, null, raw, rawTotal, withBodyLen);
                            return;
                        }
                        // raw 是本批捕获的实例（大帧是临时数组）；dst 也按需临时分配
                        dst = ensureCompressDstCapacity(rawTotal);
                        // 第 3 个参数是"目标缓冲区可用空间"，必须传数组实际长度
                        // （传 srcSize 会报 Destination buffer is too small）
                        produced = compressCtx.compressByteArray(
                                dst, 0, dst.length, raw, 0, rawTotal);
                    }
                } catch (Throwable t) {
                    backToEventLoop(ctx, promises, t, 0, null, raw, rawTotal, withBodyLen);
                    return;
                }
                backToEventLoop(ctx, promises, null, produced, dst, raw, rawTotal, withBodyLen);
            });
        } catch (Throwable t) {
            // 池被关闭 / 线程创建失败：这批已经移出 pending 且 promises 已进 inflight，
            // 不在这里收尾就是"promise 永挂 + compressing 永久为真"（该连接之后再也发不出帧）。
            compressing = false;
            inflightPromises = null;
            failAll(promises, t);
            LOGGER.error("[Zstd] encoder compression task rejected", t);
        }
    }

    /** 压缩完成后回到 event loop：组帧、写出、并立刻接手积压的下一批。 */
    private void backToEventLoop(ChannelHandlerContext ctx, List<ChannelPromise> promises,
                                 Throwable failure, int produced, byte[] dst, byte[] raw,
                                 int rawTotal, boolean withBodyLen) {
        ctx.executor().execute(() -> {
            compressing = false;
            inflightPromises = null;

            if (removed) {
                failAll(promises, new ClosedChannelException());
                return;
            } else if (failure != null) {
                failAll(promises, failure);
            } else if (!ctx.channel().isActive()) {
                // 通道已关：结果无处可去，promise 也已由 channelInactive 失败化（tryFailure 幂等）
                failAll(promises, new ClosedChannelException());
                return;
            } else {
                try {
                    boolean compressed = produced > 0 && produced < rawTotal;
                    writeFrame(ctx, buildCompressedFrame(ctx, dst, raw, rawTotal, produced, compressed, withBodyLen), promises);
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

    /**
     * 选出本帧要装的包（前缀），返回包数。
     *
     * <p>只有一种包会被拒绝：单包自身超过对端解码器的 rawSize 上限
     * （{@link ZstdVarInts#DEFAULT_MAX_VALUE}，32MB）。那种包无论怎么压缩都送不出去
     * （解码侧 {@code tryRead} 会直接判非法），留着只会让后续批次永远卡在队首。</p>
     *
     * <p>⚠️ 超过 {@link #maxRawBatchBytes()} 的包<b>不拒绝</b>，只是让它独占一帧：
     * 那只是合批的软目标。按原始大小拒绝会把配置阶段的大 Registry 包（原始数 MB、
     * 压缩后几百 KB）直接掐死，玩家进不了服。</p>
     */
    private int takeFrameBatch() {
        int softMax = maxRawBatchBytes();
        long batchBytes = 0;
        int count = 0;
        while (count < pending.size()) {
            ByteBuf packet = pending.get(count);
            int readable = packet.readableBytes();
            long packetBytes = (long) ZstdVarInts.length(readable) + readable;
            if (packetBytes > ZstdVarInts.DEFAULT_MAX_VALUE) {
                if (count == 0) {
                    IllegalArgumentException failure = new IllegalArgumentException(
                            "packet size " + packetBytes + " exceeds decoder rawSize limit "
                                    + ZstdVarInts.DEFAULT_MAX_VALUE);
                    ByteBuf rejected = pending.remove(0);
                    ChannelPromise promise = pendingPromises.remove(0);
                    ReferenceCountUtil.release(rejected);
                    promise.tryFailure(failure);
                    LOGGER.warn("[Zstd] rejected outbound packet: {}B exceeds decoder rawSize limit {}B",
                            packetBytes, ZstdVarInts.DEFAULT_MAX_VALUE);
                    return 0;
                }
                break;
            }
            if (count > 0 && batchBytes + packetBytes > softMax) {
                break; // 软预算：装不下就起新帧（第一个包总是能进）
            }
            batchBytes += packetBytes;
            count++;
            if (count >= batchMaxPackets) {
                break;
            }
        }
        return count;
    }

    private int concatPending(List<ByteBuf> packets) {
        long rawTotalLong = 0;
        for (ByteBuf b : packets) {
            rawTotalLong += (long) ZstdVarInts.length(b.readableBytes()) + b.readableBytes();
        }
        if (rawTotalLong > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("batch raw size exceeds integer range: " + rawTotalLong);
        }
        int rawTotal = (int) rawTotalLong;
        final byte[] raw = ensureRawCapacity(rawTotal);
        batchRaw = raw;
        int off = 0;
        for (ByteBuf b : packets) {
            int n = b.readableBytes();
            off = ZstdVarInts.writeTo(raw, off, n);
            b.getBytes(b.readerIndex(), raw, off, n);
            off += n;
        }
        onRawBatch(raw, rawTotal); // 采样（必须在缓冲复用前）
        releasePackets(packets);
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
    private ByteBuf buildStoredFrame(ChannelHandlerContext ctx, byte[] raw, int rawTotal, boolean withBodyLen) {
        ByteBuf frame;
        if (withBodyLen) {
            int bodyLen = 1 + rawTotal; // rawSize=0（1 字节 varint）+ payload
            frame = ctx.alloc().directBuffer(ZstdVarInts.length(bodyLen) + bodyLen);
            ZstdVarInts.write(frame, bodyLen);
        } else {
            frame = ctx.alloc().directBuffer(ZstdVarInts.length(0) + rawTotal);
        }
        ZstdVarInts.write(frame, 0);
        frame.writeBytes(raw, 0, rawTotal);
        ensureFrameFitsMcLimit(frame, rawTotal);
        onFrame(1 + rawTotal, frame.readableBytes(), false);
        return frame;
    }

    /** 压缩帧（或压缩无收益时的直存）：{@code [bodyLen?][rawSize][payload]}。 */
    private ByteBuf buildCompressedFrame(ChannelHandlerContext ctx, byte[] dst, byte[] raw,
                                         int rawTotal, int outLen, boolean compressed, boolean withBodyLen) {
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
            frame.writeBytes(dst, 0, outLen);
        } else {
            frame.writeBytes(raw, 0, rawTotal);
        }
        ensureFrameFitsMcLimit(frame, rawTotal);
        onFrame(1 + rawTotal, frame.readableBytes(), compressed);
        return frame;
    }

    /**
     * 线路帧的<b>硬</b>上限：MC 的外层长度前缀只有 21 bit，超了客户端必断连。
     *
     * <p>能走到这里且仍然超限的只有一种情况：单个包原始就接近/超过帧上限、
     * 且 zstd 压不动（已压缩过的数据）。原版 zlib 在这种输入上同样发不出去，
     * 所以失败化 promise（而不是发出一个对端必然拒绝的帧）是唯一正确的处理。</p>
     *
     * <p>注意判的是<b>最终帧</b>而不是原始字节：几 MB 的 Registry 包压缩后只有几百 KB，
     * 属于完全合法的常态流量，绝不能按原始大小拒绝（见 {@link #maxRawBatchBytes()}）。</p>
     */
    private static void ensureFrameFitsMcLimit(ByteBuf frame, int rawTotal) {
        if (frame.readableBytes() <= ZstdVarInts.MC_MAX_FRAME_BYTES) {
            return;
        }
        int actual = frame.readableBytes();
        frame.release();
        throw new IllegalArgumentException("frame " + actual + "B exceeds MC 21-bit limit "
                + ZstdVarInts.MC_MAX_FRAME_BYTES + "B (raw=" + rawTotal + "B)");
    }

    /**
     * 合批的<b>软</b>目标：原始负载达到这个量就起新帧，<b>但绝不因此拒绝单个包</b>。
     *
     * <p>⚠️ 这里曾经直接把 MC 的 21-bit 帧上限套在<b>原始</b>字节上，并据此拒绝"超限"的包。
     * 那是个会让玩家进不了服的错误：21-bit 约束的是<b>压缩后的外层帧</b>，而配置阶段的
     * Registry 包原始可达数 MB、zstd 压缩后只有几百 KB（解码器本身也允许 rawSize 到 32MB）。
     * 按原始大小拒绝，等于把最需要压缩的大包直接掐死。</p>
     *
     * <p>真正的硬上限只有两条，分别在 {@link #takeFrameBatch()}（对端解码器的 32MB rawSize
     * 上限）与 {@link #buildCompressedFrame}（最终线路帧的 21-bit 上限）里判。</p>
     */
    private int maxRawBatchBytes() {
        int outer = writeBodyLen() ? 5 : 0;
        int maxFrame = ZstdVarInts.MC_MAX_FRAME_BYTES - outer - 5;
        // 再扣掉 zstd 的最坏膨胀量（compressBound = n + n/256 + 64）。
        // 否则"压不动"的满批会产出一个刚好越过 21-bit 的帧，只能整批失败丢包；
        // 少装几个包换"永远发得出去"，对不可压缩流量才是正确取舍。
        // 直存路径（rawTotal < skip 阈值）不受影响：它没有膨胀，且阈值远小于此。
        return maxFrame - (maxFrame >>> 8) - 64;
    }

    private void schedulePendingBatch(ChannelHandlerContext ctx) {
        ctx.executor().execute(() -> emitBatch(ctx));
    }

    private void releasePending() {
        releasePackets(pending);
        pending.clear();
    }

    private static void releasePackets(List<ByteBuf> packets) {
        for (ByteBuf b : packets) {
            ReferenceCountUtil.release(b);
        }
    }

    /**
     * 压缩目标缓冲（按需增长复用）。
     *
     * <p>初值刻意取得小（8KB）而不是 64KB：实际批次通常只有 2~8KB，而 64KB 初值是
     * <b>每连接</b>两份（本缓冲 + rawArray），200 连接光初值就白占 25MB。
     * 扩容是均摊 O(1)，大流量下多几次 arraycopy 远比常驻大数组划算。</p>
     */
    private byte[] ensureCompressDstCapacity(int srcSize) {
        int needed = srcSize + (srcSize >>> 8) + 64;
        if (needed > SCRATCH_KEEP_LIMIT) {
            // 大帧临时分配，用完即弃：常驻会让"连过的最大批"永久占住缓冲
            return new byte[needed];
        }
        if (dstArray == null || dstArray.length < needed) {
            dstArray = new byte[Math.max(needed, INITIAL_BUFFER)];
        }
        return dstArray;
    }

    private byte[] ensureRawCapacity(int needed) {
        if (needed > SCRATCH_KEEP_LIMIT) {
            // 同上：大帧临时分配，不写入常驻字段
            return new byte[needed];
        }
        if (rawArray == null || rawArray.length < needed) {
            rawArray = new byte[Math.max(needed, INITIAL_BUFFER)];
        }
        return rawArray;
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        if (windowTask != null) {
            windowTask.cancel(false);
            windowTask = null;
        }
        // 只有「通道还在、且没有在途压缩任务占用每连接的缓冲」时才有可能补发；
        // 有在途任务时 emitBatch 会直接返回（见那里的说明），于是下面必须自己收尾。
        // ⚠️ 补发必须在置 removed 之前：emitBatch 见到 removed 会直接丢弃残留，
        // 若先置位，这段补发就成了永远不会生效的死代码。
        if (!pending.isEmpty() && !compressing && ctx.channel().isActive()) {
            emitBatch(ctx); // 处理器被移除前把残留数据送出去，避免丢包
        }
        removed = true;
        // ⚠️ 没能补发的残留必须在这里处理干净：本处理器已被移除，emitBatch 的
        //「等压缩回来再发」那条路径不会再走回来，留下的是永不完成的 promise 与永不释放的缓冲
        //（后者是 ReferenceCounted，扣在 pending 里没人替我们 release）。
        if (!pending.isEmpty()) {
            releasePending();
            failPending(new ClosedChannelException());
        }
        pendingPromises.clear();
        failInflight(new ClosedChannelException());
        inflightPromises = null;
        compressing = false;
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
