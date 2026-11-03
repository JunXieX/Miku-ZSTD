package mikumc.zstd.protocol;

import com.github.luben.zstd.EndDirective;
import com.github.luben.zstd.ZstdCompressCtx;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.PromiseCombiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * 连接级连续 Zstd 下行编码器，帧格式见 {@link ZstdProtocolVersion#FRAME}。
 * 原包直接交给工作池，不拼接原文；输出直接写入最终运输帧并回填外层长度。
 * 每连接仅一个在途批次，原生上下文由 manager 持有，关闭与压缩共用同一把锁。
 * flush 立即启动处理；计时器只兜底 write 未 flush，不为正常 flush 添加等待。
 */
public abstract class ZstdBatchEncoderBase extends ChannelDuplexHandler {
    protected static final Logger LOGGER = LoggerFactory.getLogger("miku-zstd");
    private static final long MAX_PENDING_BYTES = 128L * 1024 * 1024;
    private static final int INITIAL_OUTPUT = 8 * 1024;
    private static final int OUTPUT_GROWTH = 8 * 1024;
    private static final int HEAP_SCRATCH = 64 * 1024;
    private static final int LENGTH_RESERVE = 5;

    // 通常只有 event loop 访问；仅回调提交被拒时允许 worker 在此锁内清理队列。
    private final Object stateLock = new Object();
    private final ArrayDeque<PendingWrite> pending = new ArrayDeque<>();
    private final ZstdCompressLock ownLock = new ZstdCompressLock();
    private ZstdCompressCtx compressCtx;
    private long pendingBytes;
    private int pendingPackets;
    private ScheduledFuture<?> windowTask;
    private int batchWindowMs = 50;
    private int batchMaxPackets = 64;
    private boolean compressing;
    private boolean draining;
    private Batch inflight;
    private volatile boolean removed;
    private volatile boolean failed;
    private volatile boolean streamStarted;

    protected abstract ZstdCompressCtx resolveCompressContext(ChannelHandlerContext ctx);
    protected abstract boolean writeBodyLen();
    protected abstract void onFrame(int rawBytes, int wireBytes, boolean compressed);
    protected abstract int configBatchWindowMs();
    protected abstract int configBatchMaxPackets();

    protected ChannelHandlerContext downstream(ChannelHandlerContext ctx) {
        return ctx;
    }

    protected ZstdCompressLock compressLock() {
        return ownLock;
    }

    protected ZstdCompressCtx compressContextForBatch() {
        return compressCtx;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        compressCtx = resolveCompressContext(ctx);
        if (compressCtx == null) {
            throw new IllegalStateException("continuous encoder requires a compression context");
        }
        batchWindowMs = Math.max(0, configBatchWindowMs());
        batchMaxPackets = Math.max(1, configBatchMaxPackets());
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
        synchronized (stateLock) {
            if (removed || failed || !ctx.channel().isActive()) {
                ReferenceCountUtil.release(msg);
                promise.tryFailure(new ClosedChannelException());
                return;
            }
            if (!(msg instanceof ByteBuf packet)) {
                // 同一有序队列保存控制消息，避免多个屏障与包交错时控制消息超车。
                if (!compressing && pending.isEmpty()) {
                    ctx.write(msg, promise);
                } else {
                    pending.addLast(new PendingWrite(msg, promise));
                    emitBatch(ctx);
                }
                return;
            }
            int length = packet.readableBytes();
            if (length <= 0 || length > ZstdVarInts.DEFAULT_MAX_VALUE) {
                packet.release();
                IllegalArgumentException cause = new IllegalArgumentException("illegal outbound packet length " + length);
                promise.tryFailure(cause);
                abort(ctx, cause);
                return;
            }
            pending.addLast(new PendingWrite(packet, promise));
            pendingBytes += length;
            pendingPackets++;
            if (pendingBytes > MAX_PENDING_BYTES) {
                abort(ctx, new IllegalStateException("outbound backlog exceeds " + MAX_PENDING_BYTES));
                return;
            }
            if (pendingPackets >= batchMaxPackets || batchWindowMs == 0) {
                emitBatch(ctx);
            } else {
                scheduleFallback(ctx);
            }
        }
    }

    @Override
    public void flush(ChannelHandlerContext ctx) {
        synchronized (stateLock) {
            if (removed || failed) return;
            if (pending.isEmpty()) {
                ctx.flush();
            } else {
                emitBatch(ctx);
            }
        }
    }

    private void scheduleFallback(ChannelHandlerContext ctx) {
        if (windowTask != null || compressing || removed || failed) return;
        try {
            windowTask = ctx.executor().schedule(() -> {
                synchronized (stateLock) {
                    windowTask = null;
                    emitBatch(ctx);
                }
            }, batchWindowMs, TimeUnit.MILLISECONDS);
        } catch (Throwable cause) {
            abort(ctx, cause);
        }
    }

    private void cancelWindow() {
        if (windowTask != null) {
            windowTask.cancel(false);
            windowTask = null;
        }
    }

    /** 调用方持有 stateLock；原包所有权只在成功提交任务后转给 worker。 */
    private void emitBatch(ChannelHandlerContext ctx) {
        cancelWindow();
        if (removed || failed || compressing || draining) return;
        draining = true;
        try {
            drainBatch(ctx);
        } finally {
            draining = false;
        }
    }

    // 控制消息的同步 promise/flush 回调可以重入 write/flush；只让外层推进一次。
    private void drainBatch(ChannelHandlerContext ctx) {
        boolean wroteControl = false;
        while (!pending.isEmpty() && !(pending.peekFirst().message() instanceof ByteBuf)) {
            PendingWrite entry = pending.removeFirst();
            try {
                ctx.write(entry.message(), entry.promise());
                wroteControl = true;
            } catch (Throwable cause) {
                ReferenceCountUtil.safeRelease(entry.message());
                if (!entry.promise().isVoid()) entry.promise().tryFailure(cause);
                abort(ctx, cause);
                return;
            }
            if (removed || failed || !ctx.channel().isActive()) return;
        }
        if (wroteControl) ctx.flush();
        if (pending.isEmpty() || removed || failed) return;

        List<ByteBuf> packets = new ArrayList<>();
        List<ChannelPromise> promises = new ArrayList<>();
        int rawBytes = 0;
        while (!pending.isEmpty() && packets.size() < batchMaxPackets) {
            PendingWrite entry = pending.peekFirst();
            if (!(entry.message() instanceof ByteBuf packet)) break;
            int bytes = packet.readableBytes();
            int encoded = ZstdVarInts.length(bytes) + bytes;
            // 单个合法 32MiB 包独占批，允许其长度前缀再多占最多5B。
            if (!packets.isEmpty() && (long) rawBytes + encoded > ZstdVarInts.DEFAULT_MAX_VALUE) break;
            pending.removeFirst();
            pendingBytes -= bytes;
            pendingPackets--;
            packets.add(packet);
            if (!entry.promise().isVoid()) promises.add(entry.promise());
            rawBytes += encoded;
        }
        Batch batch = new Batch(packets, promises, rawBytes, writeBodyLen());
        compressing = true;
        inflight = batch;
        try {
            ZstdCompressPool.execute(() -> compressBatch(ctx, batch));
        } catch (Throwable cause) {
            releasePackets(batch.packets());
            failPromises(batch.promises(), cause);
            inflight = null;
            compressing = false;
            abort(ctx, cause);
        }
    }

    private void compressBatch(ChannelHandlerContext ctx, Batch batch) {
        StreamOutput output = new StreamOutput(ctx, batch.bodyLength());
        Result result = null;
        Throwable failure = null;
        ByteBuf prefix = null;
        ZstdCompressLock lease = null;
        boolean acquired = false;
        try {
            lease = compressLock();
            acquired = lease.tryAcquire();
            if (!acquired || removed || failed) throw new ClosedChannelException();
            ZstdCompressCtx compressor = compressContextForBatch();
            if (compressor == null) throw new ClosedChannelException();
            prefix = ctx.alloc().directBuffer(LENGTH_RESERVE, LENGTH_RESERVE);
            streamStarted = true;
            for (ByteBuf packet : batch.packets()) {
                if (removed || failed) throw new ClosedChannelException();
                prefix.clear();
                ZstdVarInts.write(prefix, packet.readableBytes());
                pump(compressor, prefix.nioBuffer(), EndDirective.CONTINUE, output);
                for (ByteBuffer input : packet.nioBuffers(packet.readerIndex(), packet.readableBytes())) {
                    feedInput(ctx, compressor, input, output);
                }
            }
            pump(compressor, prefix.nioBuffer(0, 0), EndDirective.FLUSH, output);
            result = output.finish();
        } catch (Throwable cause) {
            failure = cause;
        } finally {
            // 归还回调可能抛错，仍须释放所有输入/输出并提交统一完成通知。
            try {
                if (acquired) lease.release();
            } catch (Throwable cleanup) {
                if (failure == null) failure = cleanup;
                else if (failure != cleanup) failure.addSuppressed(cleanup);
            }
            ReferenceCountUtil.safeRelease(prefix);
            releasePackets(batch.packets());
            output.close();
        }
        final Result ready = result;
        final Throwable cause = failure;
        // worker 完成时刻与回调真正开始执行时刻之间，可能还排着别的 event loop 任务；
        // 这段等待属于"回调延迟"，既不是压缩耗时，也不是网络延迟。
        final long finishedNanos = System.nanoTime();
        try {
            ctx.executor().execute(() -> finishBatch(ctx, batch, ready, cause, finishedNanos));
        } catch (Throwable rejected) {
            releaseResult(ready);
            synchronized (stateLock) {
                failPromises(batch.promises(), rejected);
                if (inflight == batch) {
                    inflight = null;
                    compressing = false;
                }
                abort(ctx, rejected);
            }
        }
    }

    /** direct/NIO 分段直接读取；只有 heap 输入才用有界 direct scratch。 */
    private static void feedInput(ChannelHandlerContext ctx, ZstdCompressCtx compressor,
                                  ByteBuffer input, StreamOutput output) throws ClosedChannelException {
        if (input.isDirect()) {
            pump(compressor, input, EndDirective.CONTINUE, output);
            return;
        }
        ByteBuf scratch = ctx.alloc().directBuffer(Math.min(HEAP_SCRATCH, input.remaining()), HEAP_SCRATCH);
        try {
            while (input.hasRemaining()) {
                int bytes = Math.min(HEAP_SCRATCH, input.remaining());
                ByteBuffer view = input.slice();
                view.limit(bytes);
                scratch.clear();
                scratch.writeBytes(view);
                input.position(input.position() + bytes);
                pump(compressor, scratch.nioBuffer(), EndDirective.CONTINUE, output);
            }
        } finally {
            scratch.release();
        }
    }

    private static void pump(ZstdCompressCtx compressor, ByteBuffer input, EndDirective directive,
                             StreamOutput output) throws ClosedChannelException {
        do {
            ByteBuf target = output.writable();
            ByteBuffer destination = target.internalNioBuffer(target.writerIndex(), target.writableBytes());
            int beforeInput = input.position();
            int beforeOutput = destination.position();
            boolean flushed = compressor.compressDirectByteBufferStream(destination, input, directive);
            int written = destination.position() - beforeOutput;
            target.writerIndex(target.writerIndex() + written);
            if (directive == EndDirective.CONTINUE && !input.hasRemaining()) return;
            if (directive == EndDirective.FLUSH && flushed) return;
            if (input.position() == beforeInput && written == 0 && destination.hasRemaining()) {
                throw new IllegalStateException("Zstd compression made no progress");
            }
        } while (true);
    }

    private void finishBatch(ChannelHandlerContext ctx, Batch batch, Result result, Throwable failure,
                             long finishedNanos) {
        ZstdLatencyStats.record(ZstdLatencyStats.Stage.EVENT_LOOP, System.nanoTime() - finishedNanos);
        synchronized (stateLock) {
            try {
                if (removed || failed || !ctx.channel().isActive()) {
                    ClosedChannelException cause = new ClosedChannelException();
                    failPromises(batch.promises(), cause);
                    abort(ctx, cause);
                    return;
                }
                if (failure != null || result == null) {
                    abort(ctx, failure == null ? new IllegalStateException("missing stream output") : failure);
                    return;
                }
                writeFrames(ctx, batch, result);
            } catch (Throwable cause) {
                abort(ctx, cause);
            } finally {
                releaseResult(result);
                if (inflight == batch) {
                    inflight = null;
                    compressing = false;
                }
            }
            if (!removed && !failed && ctx.channel().isActive()) emitBatch(ctx);
        }
    }

    private void writeFrames(ChannelHandlerContext ctx, Batch batch, Result result) {
        ChannelHandlerContext target = downstream(ctx);
        if (target == null) throw new IllegalStateException("original compression context disappeared");
        long writeStartedNanos = System.nanoTime();
        ChannelPromise completed = ctx.newPromise();
        completed.addListener(future -> {
            // 这一时刻只说明 Netty 的写 promise 完成，不证明对端已经收到或确认。
            ZstdLatencyStats.record(ZstdLatencyStats.Stage.WRITE, System.nanoTime() - writeStartedNanos);
            if (future.isSuccess()) {
                for (ChannelPromise promise : batch.promises()) promise.trySuccess();
            } else {
                failPromises(batch.promises(), future.cause());
            }
        });
        PromiseCombiner combiner = new PromiseCombiner(ctx.executor());
        try {
            for (int i = 0; i < result.frames().size(); i++) {
                if (removed || failed || !ctx.channel().isActive()) throw new IllegalStateException("channel closed during stream write");
                ByteBuf frame = result.frames().set(i, null);
                ChannelPromise written = ctx.newPromise();
                written.addListener(future -> {
                    if (!future.isSuccess()) {
                        synchronized (stateLock) {
                            abort(ctx, future.cause());
                        }
                    }
                });
                combiner.add((io.netty.util.concurrent.Future<?>) written);
                try {
                    target.write(frame, written);
                } catch (Throwable cause) {
                    if (frame.refCnt() > 0) frame.release();
                    written.tryFailure(cause);
                    throw cause;
                }
            }
            combiner.finish(completed);
            onFrame(batch.rawBytes(), result.wireBytes(), true);
            target.flush();
        } catch (Throwable cause) {
            completed.tryFailure(cause);
            abort(ctx, cause);
        }
    }

    /** 失败后流状态不可恢复，失败化队列与在途 promise，不发原版回退或跳包续流。 */
    private void abort(ChannelHandlerContext ctx, Throwable cause) {
        if (failed) return;
        failed = true;
        cancelWindow();
        Batch active = inflight;
        inflight = null;
        compressing = false;
        if (active != null) failPromises(active.promises(), cause);
        PendingWrite entry;
        while ((entry = pending.pollFirst()) != null) {
            ReferenceCountUtil.safeRelease(entry.message());
            if (!entry.promise().isVoid()) entry.promise().tryFailure(cause);
        }
        pendingBytes = 0;
        pendingPackets = 0;
        if (!(cause instanceof java.io.IOException) && ctx.channel().isActive()) {
            LOGGER.warn("[Zstd] continuous stream failed, closing channel {}", ctx.channel().remoteAddress(), cause);
        }
        if (ctx.channel().isOpen()) ctx.close();
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        synchronized (stateLock) {
            removed = true;
            // 已安装到启用中的下行管线，移除后不再存在合法的继续发送路径。
            boolean mustClose = streamStarted || compressing || !pending.isEmpty();
            cancelWindow();
            ClosedChannelException cause = new ClosedChannelException();
            if (inflight != null) failPromises(inflight.promises(), cause);
            PendingWrite entry;
            while ((entry = pending.pollFirst()) != null) {
                ReferenceCountUtil.safeRelease(entry.message());
                if (!entry.promise().isVoid()) entry.promise().tryFailure(cause);
            }
            pendingBytes = 0;
            pendingPackets = 0;
            if (mustClose && ctx.channel().isOpen()) ctx.close();
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        synchronized (stateLock) {
            abort(ctx, new ClosedChannelException());
        }
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        if (isPacketCancellation(cause)) {
            LOGGER.debug("[Zstd] packet cancelled before stream encoding: {}", cause.toString());
            return;
        }
        synchronized (stateLock) {
            abort(ctx, cause);
        }
    }

    private static boolean isPacketCancellation(Throwable cause) {
        for (Throwable current = cause; current != null; current = current.getCause()) {
            if (current.getClass().getSimpleName().contains("CancelPacket")) return true;
        }
        return false;
    }

    private static void releasePackets(List<ByteBuf> packets) {
        for (ByteBuf packet : packets) ReferenceCountUtil.safeRelease(packet);
    }

    private static void failPromises(List<ChannelPromise> promises, Throwable cause) {
        for (ChannelPromise promise : promises) {
            if (!promise.isVoid()) promise.tryFailure(cause);
        }
    }

    private static void releaseResult(Result result) {
        if (result == null) return;
        for (ByteBuf frame : result.frames()) ReferenceCountUtil.safeRelease(frame);
        result.frames().clear();
    }

    private record PendingWrite(Object message, ChannelPromise promise) { }
    private record Batch(List<ByteBuf> packets, List<ChannelPromise> promises, int rawBytes, boolean bodyLength) { }
    private record Result(List<ByteBuf> frames, int wireBytes) { }

    /** 输出缓冲就是最终运输帧；达到体长上限才分段，分段可处于压缩块或内层包中间。 */
    private static final class StreamOutput implements AutoCloseable {
        private final ChannelHandlerContext ctx;
        private final boolean bodyLength;
        private final int reserve;
        private final List<ByteBuf> frames = new ArrayList<>();
        private ByteBuf current;
        private int wireBytes;

        StreamOutput(ChannelHandlerContext ctx, boolean bodyLength) {
            this.ctx = ctx;
            this.bodyLength = bodyLength;
            reserve = bodyLength ? LENGTH_RESERVE : 0;
        }

        ByteBuf writable() {
            if (current != null && current.writerIndex() == current.maxCapacity()) seal();
            if (current == null) {
                current = ctx.alloc().directBuffer(INITIAL_OUTPUT + reserve,
                        ZstdVarInts.MC_MAX_FRAME_BYTES + reserve);
                current.writerIndex(reserve);
            } else if (!current.isWritable()) {
                current.ensureWritable(Math.min(OUTPUT_GROWTH, current.maxCapacity() - current.writerIndex()));
            }
            return current;
        }

        private void seal() {
            ByteBuf frame = current;
            if (frame == null) return;
            int end = frame.writerIndex();
            int body = end - reserve;
            if (body == 0) {
                current = null;
                frame.release();
                return;
            }
            if (body > ZstdVarInts.MC_MAX_FRAME_BYTES) throw new IllegalStateException("transport frame exceeds MC limit");
            if (bodyLength) {
                int start = reserve - ZstdVarInts.length(body);
                frame.writerIndex(start);
                ZstdVarInts.write(frame, body);
                frame.writerIndex(end);
                frame.readerIndex(start);
            }
            frames.add(frame);
            current = null;
            wireBytes += ZstdVarInts.length(body) + body;
        }

        Result finish() {
            seal();
            if (frames.isEmpty()) throw new IllegalStateException("flush produced no stream output");
            List<ByteBuf> completed = new ArrayList<>(frames);
            frames.clear();
            return new Result(completed, wireBytes);
        }

        @Override
        public void close() {
            ReferenceCountUtil.safeRelease(current);
            current = null;
            for (ByteBuf frame : frames) ReferenceCountUtil.safeRelease(frame);
            frames.clear();
        }
    }
}
