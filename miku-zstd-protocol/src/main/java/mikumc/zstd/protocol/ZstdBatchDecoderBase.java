package mikumc.zstd.protocol;

import com.github.luben.zstd.ZstdDecompressCtx;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * FRAME8 下行连续流解码器。每次输入都是 splitter 已剥掉 MC 外层长度的完整运输帧，
 * 帧体只有同一条 magicless Zstd 流的后续字节，不带原长或直存标记。
 *
 * <p>解压得到连续的 {@code [VarInt packetLength][packet]}。长度和包体均可跨运输帧；
 * 完整包直接保留输出块的切片，只有跨输出块的包体需要组装。客户端通过
 * {@link #decodeForwardTarget} 从原版解压处理器之后交包，保留原处理器但跳过重复解压。</p>
 *
 * <p>解压上下文归连接管理器所有，本类不归还、重置或装载字典。流只允许 FLUSH，
 * native 报告流结束、超限或异常时终止连接。</p>
 */
public abstract class ZstdBatchDecoderBase extends MessageToMessageDecoder<ByteBuf> {

    protected static final Logger LOGGER = LoggerFactory.getLogger("miku-zstd");

    private static final int OUTPUT_CHUNK_BYTES = 128 * 1024;
    private static final int MAX_FRAME_OUTPUT_BYTES = ZstdVarInts.DEFAULT_MAX_VALUE + 5;
    private static final int MAX_WINDOW_LOG = 27;

    private ZstdDecompressCtx decompressCtx;
    private ZstdDecompressCtx streamCtx;
    private ByteBuf partial;
    private int packetLength;
    private int lengthValue;
    private int lengthBytes;
    private final byte[] streamHeader = new byte[2];
    private int streamHeaderBytes;
    private boolean windowChecked;
    private boolean headerFed;
    private boolean messageDelivered;
    private boolean decoding;
    private boolean removed;
    private boolean closed;

    /** 在安装时解析连接上下文；生命周期与归还仍由连接管理器负责。 */
    protected abstract ZstdDecompressCtx resolveDecompressContext(ChannelHandlerContext ctx);

    /** 每运输帧重新取得当前上下文；同一已开始的流不允许中途更换实例。 */
    protected ZstdDecompressCtx decompressContextForFrame() {
        return decompressCtx;
    }

    /**
     * 每个成功消费的运输帧调用一次，包括只有半包、半个长度或零输出的运输帧。
     *
     * @param wireBytes 压缩帧体及其 MC 外层 VarInt 的总字节数
     * @param rawBytes 本帧实际解出的内层流字节数，包含包长度前缀
     */
    protected abstract void onCompressedFrame(int wireBytes, int rawBytes);

    /** 返回交包起点；默认从本处理器之后交付。客户端应返回原版解压处理器的 context。 */
    protected ChannelHandlerContext decodeForwardTarget(ChannelHandlerContext ctx) {
        return null;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        decompressCtx = resolveDecompressContext(ctx);
        if (decompressCtx == null) {
            LOGGER.warn("[Zstd] decoder installed without a decompress context on this channel");
        }
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        if (stopped(ctx)) return;
        if (decoding) {
            protocolError(ctx, "reentrant compressed frame");
            return;
        }
        int frameBodyBytes = in.readableBytes();
        if (frameBodyBytes < 1 || frameBodyBytes > ZstdVarInts.MC_MAX_FRAME_BYTES) {
            protocolError(ctx, "illegal compressed frame size " + frameBodyBytes);
            return;
        }
        ByteBuf temporary = null;
        ByteBuf initialHeader = null;
        decoding = true;
        try {
            ZstdDecompressCtx dctx = decompressContextForFrame();
            if (stopped(ctx)) return;
            if (dctx == null) {
                protocolError(ctx, "decompress context unavailable");
                return;
            }
            if (streamCtx == null) {
                dctx.setMagicless(true);
                streamCtx = dctx;
            } else if (streamCtx != dctx) {
                protocolError(ctx, "decompress context changed during a continuous stream");
                return;
            }
            int rawBytes = 0;
            if (!windowChecked && !readStreamHeader(ctx, in)) {
                if (!stopped(ctx)) onCompressedFrame(frameBodyBytes + ZstdVarInts.length(frameBodyBytes), 0);
                return;
            }
            if (!headerFed) {
                // 首两字节先在Java验证，避免native依据未验证窗口分配；不复制后续帧体。
                initialHeader = ctx.alloc().directBuffer(2, 2);
                initialHeader.writeBytes(streamHeader);
                headerFed = true;
                rawBytes = inflate(ctx, dctx, initialHeader.nioBuffer(), rawBytes);
                if (rawBytes < 0) return;
            }
            int bytes = in.readableBytes();
            if (bytes > 0) {
                ByteBuf compressed = in;
                if (!in.isDirect() || in.nioBufferCount() != 1) {
                    temporary = ctx.alloc().directBuffer(bytes, bytes);
                    temporary.writeBytes(in, in.readerIndex(), bytes);
                    compressed = temporary;
                }
                ByteBuffer src = compressed.nioBuffer(compressed.readerIndex(), bytes);
                rawBytes = inflate(ctx, dctx, src, rawBytes);
                if (rawBytes < 0) return;
                in.skipBytes(bytes);
            }
            if (!sameContext(ctx, dctx)) return;
            onCompressedFrame(frameBodyBytes + ZstdVarInts.length(frameBodyBytes), rawBytes);
        } catch (Exception e) {
            if (!stopped(ctx)) protocolError(ctx, "decompress failed: " + e);
        } finally {
            if (initialHeader != null) initialHeader.release();
            if (temporary != null) temporary.release();
            decoding = false;
        }
    }

    /** FRAME8只允许无magic/contentSize/dictID/checksum的开放流，首头固定descriptor+window。 */
    private boolean readStreamHeader(ChannelHandlerContext ctx, ByteBuf in) {
        while (streamHeaderBytes < streamHeader.length && in.isReadable()) {
            streamHeader[streamHeaderBytes++] = in.readByte();
            if (streamHeaderBytes == 1 && streamHeader[0] != 0) {
                protocolError(ctx, "unexpected continuous Zstd frame parameters");
                return false;
            }
        }
        if (streamHeaderBytes < streamHeader.length) return false;
        int descriptor = streamHeader[1] & 0xFF;
        int windowLog = 10 + (descriptor >>> 3);
        if (windowLog > MAX_WINDOW_LOG || (windowLog == MAX_WINDOW_LOG && (descriptor & 7) != 0)) {
            protocolError(ctx, "Zstd window exceeds windowLog " + MAX_WINDOW_LOG);
            return false;
        }
        windowChecked = true;
        return !stopped(ctx);
    }

    /** 返回本运输帧累计原长；-1表示下游重入或错误已终止当前解码。 */
    private int inflate(ChannelHandlerContext ctx, ZstdDecompressCtx dctx, ByteBuffer src, int rawBytes) {
        boolean outputFull;
        do {
            if (stopped(ctx)) return -1;
            // 一字节哨兵检测超限；恰好达上限后仍可消费无输出的流控制字节。
            int capacity = Math.min(OUTPUT_CHUNK_BYTES, MAX_FRAME_OUTPUT_BYTES - rawBytes + 1);
            ByteBuf decoded = ctx.alloc().directBuffer(capacity, capacity);
            try {
                ByteBuffer dst = decoded.nioBuffer(0, capacity);
                int beforeInput = src.position();
                int beforeOutput = dst.position();
                boolean ended = dctx.decompressDirectByteBufferStream(dst, src);
                int consumed = src.position() - beforeInput;
                int produced = dst.position() - beforeOutput;
                decoded.writerIndex(produced);
                if (ended) {
                    protocolError(ctx, "unexpected end of continuous Zstd stream");
                    return -1;
                }
                if (produced > MAX_FRAME_OUTPUT_BYTES - rawBytes) {
                    protocolError(ctx, "decompressed frame exceeds " + MAX_FRAME_OUTPUT_BYTES + " bytes");
                    return -1;
                }
                outputFull = !dst.hasRemaining();
                if (consumed == 0 && produced == 0) {
                    if (!src.hasRemaining()) break; // 空输入drain等待后续帧，不自旋。
                    protocolError(ctx, "decompress made no progress");
                    return -1;
                }
                rawBytes += produced;
                if (!splitAndEmit(ctx, decoded) || !sameContext(ctx, dctx)) return -1;
            } finally {
                decoded.release();
            }
        } while (src.hasRemaining() || outputFull);
        return rawBytes;
    }

    /** 持久化半个 VarInt 与半包，不把运输帧边界当作包边界。 */
    private boolean splitAndEmit(ChannelHandlerContext ctx, ByteBuf decoded) {
        while (decoded.isReadable()) {
            if (stopped(ctx)) return false;
            if (packetLength == 0) {
                while (decoded.isReadable()) {
                    int b = decoded.readUnsignedByte();
                    // 第五字节只允许最低四位，防止移位溢出绕过包长上限。
                    if (lengthBytes == 4 && (b & 0xF0) != 0) {
                        protocolError(ctx, "illegal inner packet length VarInt");
                        return false;
                    }
                    lengthValue |= (b & 0x7F) << (lengthBytes * 7);
                    lengthBytes++;
                    if (lengthValue > ZstdVarInts.DEFAULT_MAX_VALUE) {
                        protocolError(ctx, "inner packet length exceeds " + ZstdVarInts.DEFAULT_MAX_VALUE);
                        return false;
                    }
                    if ((b & 0x80) == 0) {
                        packetLength = lengthValue;
                        lengthValue = 0;
                        lengthBytes = 0;
                        if (packetLength < 1) {
                            protocolError(ctx, "empty inner packet");
                            return false;
                        }
                        break;
                    }
                }
                if (packetLength == 0) return true;
            }

            if (partial == null && decoded.readableBytes() >= packetLength) {
                ByteBuf packet = decoded.readRetainedSlice(packetLength);
                packetLength = 0;
                if (!emit(ctx, packet)) return false;
            } else if (decoded.isReadable()) {
                if (partial == null) {
                    // 按实际到达的片段增长，不按对端声明的最大包长预分配。
                    partial = ctx.alloc().directBuffer(
                            Math.min(OUTPUT_CHUNK_BYTES, packetLength), packetLength);
                }
                int copied = Math.min(decoded.readableBytes(), packetLength - partial.readableBytes());
                partial.writeBytes(decoded, copied);
                if (partial.readableBytes() == packetLength) {
                    ByteBuf packet = partial;
                    partial = null;
                    packetLength = 0;
                    if (!emit(ctx, packet)) return false;
                }
            }
        }
        return !stopped(ctx);
    }

    /** 调用下游前先交出所有权，移除回调只会释放尚未交出的 partial。 */
    private boolean emit(ChannelHandlerContext ctx, ByteBuf packet) {
        boolean transferred = false;
        try {
            if (stopped(ctx)) return false;
            ChannelHandlerContext target = decodeForwardTarget(ctx);
            if (stopped(ctx)) return false;
            if (target == null) target = ctx;
            transferred = true;
            messageDelivered = true;
            target.fireChannelRead(packet);
            return sameContext(ctx, streamCtx);
        } finally {
            if (!transferred) packet.release();
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object message) throws Exception {
        if (!(message instanceof ByteBuf)) messageDelivered = true;
        super.channelRead(ctx, message);
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        // 包从decompress context直接交付，父类看不到out；不能因此穿过下游AUTO_READ暂停。
        boolean delivered = messageDelivered;
        messageDelivered = false;
        if (!delivered && !stopped(ctx) && !ctx.channel().config().isAutoRead()) ctx.read();
        ctx.fireChannelReadComplete();
    }

    private boolean stopped(ChannelHandlerContext ctx) {
        return removed || closed || ctx.isRemoved() || !ctx.channel().isActive();
    }

    private boolean sameContext(ChannelHandlerContext ctx, ZstdDecompressCtx expected) {
        if (stopped(ctx)) return false;
        ZstdDecompressCtx current = decompressContextForFrame();
        if (stopped(ctx)) return false;
        if (current != expected) {
            protocolError(ctx, "decompress context changed during frame delivery");
            return false;
        }
        return true;
    }

    private void releasePartial() {
        ByteBuf pending = partial;
        partial = null;
        packetLength = 0;
        lengthValue = 0;
        lengthBytes = 0;
        if (pending != null) pending.release();
    }

    private void protocolError(ChannelHandlerContext ctx, String reason) {
        if (removed || closed || ctx.isRemoved()) return;
        closed = true;
        releasePartial();
        if (ctx.channel().isActive()) {
            LOGGER.warn("[Zstd] frame rejected — closing channel: {} (remote={})",
                    reason, ctx.channel().remoteAddress());
        }
        if (ctx.channel().isOpen()) ctx.close();
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        removed = true;
        releasePartial();
        decompressCtx = null;
        streamCtx = null;
        super.handlerRemoved(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        closed = true;
        releasePartial();
        decompressCtx = null;
        streamCtx = null;
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        if (isPacketCancellation(cause)) {
            LOGGER.debug("[Zstd] packet cancelled by another plugin (remote={}): {}",
                    ctx.channel().remoteAddress(), cause.toString());
            return;
        }
        if (isConnectionIoError(cause)) {
            LOGGER.debug("[Zstd] connection I/O error (remote={}): {}",
                    ctx.channel().remoteAddress(), cause.toString());
        } else if (stopped(ctx)) {
            LOGGER.debug("[Zstd] decoder exception after channel closed (remote={}): {}",
                    ctx.channel().remoteAddress(), cause.toString());
        } else {
            LOGGER.warn("[Zstd] decoder exception — closing channel (remote={})",
                    ctx.channel().remoteAddress(), cause);
        }
        closed = true;
        releasePartial();
        if (ctx.channel().isOpen()) ctx.close();
    }

    private static boolean isConnectionIoError(Throwable cause) {
        return cause instanceof java.io.IOException;
    }

    private static boolean isPacketCancellation(Throwable cause) {
        for (Throwable t = cause; t != null; t = t.getCause()) {
            if (t.getClass().getSimpleName().contains("CancelPacket")) return true;
        }
        return false;
    }
}
