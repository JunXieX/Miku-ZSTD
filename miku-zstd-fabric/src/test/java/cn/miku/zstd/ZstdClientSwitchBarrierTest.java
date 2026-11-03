package cn.miku.zstd;

import com.github.luben.zstd.ZstdCompressCtx;
import com.github.luben.zstd.EndDirective;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.util.ReferenceCountUtil;
import mikumc.zstd.protocol.ZstdStepFrames;
import mikumc.zstd.protocol.ZstdSwitchGate;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import static org.junit.jupiter.api.Assertions.*;

class ZstdClientSwitchBarrierTest {
    private EmbeddedChannel channel;
    private ZstdChannelManager manager;

    @AfterEach
    void close() {
        if (channel == null) return;
        try {
            Object message;
            while ((message = channel.readInbound()) != null) ReferenceCountUtil.release(message);
            while ((message = channel.readOutbound()) != null) ReferenceCountUtil.release(message);
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void negotiateBeforeCompressionAnchorArmsAtFrameDecoderAndMovesBeforeCompressionWhenInstalled() {
        channel = new EmbeddedChannel(new MinecraftFrameSplitter());
        channel.pipeline().addLast("decoder", new ChannelInboundHandlerAdapter());
        channel.pipeline().addLast("encoder", new ChannelOutboundHandlerAdapter());
        manager = new ZstdChannelManager();
        channel.attr(ZstdChannelManager.KEY).set(manager);
        channel.closeFuture().addListener(future -> manager.close());
        channel.attr(ZstdChannelManager.ZSTD_STATE).set(ZstdChannelManager.TransportState.NEGOTIATING);

        ZstdClientActivation.prepareForSwitch(channel);
        assertTrue(channel.pipeline().names().indexOf("zstd_switch_sniffer")
                < channel.pipeline().names().indexOf("decoder"));
        assertNull(channel.pipeline().get("zstd_decoder"));

        channel.pipeline().addBefore("decoder", "decompress", new VanillaCompressionDecoder());
        ZstdClientActivation.prepareForSwitch(channel);

        assertTrue(channel.pipeline().names().indexOf("zstd_switch_sniffer")
                < channel.pipeline().names().indexOf("decompress"));
        assertNull(channel.pipeline().get("zstd_decoder"), "压缩锚点到达时仍不能提前开Zstd解码");
    }

    @Test
    void prematureSwitchBeforeCompressionAnchorClosesWithoutSendingFalseFallback() {
        channel = new EmbeddedChannel(new MinecraftFrameSplitter());
        channel.pipeline().addLast("decoder", new ChannelInboundHandlerAdapter());
        channel.pipeline().addLast("encoder", new ChannelOutboundHandlerAdapter());
        manager = new ZstdChannelManager();
        channel.attr(ZstdChannelManager.KEY).set(manager);
        channel.closeFuture().addListener(future -> manager.close());
        channel.attr(ZstdChannelManager.ZSTD_STATE).set(ZstdChannelManager.TransportState.NEGOTIATING);
        ZstdClientActivation.prepareForSwitch(channel);
        assertNull(channel.pipeline().get("decompress"));

        channel.writeInbound(Unpooled.wrappedBuffer(vanillaStoredFrame(switchPacket(74))));

        assertFalse(channel.isActive(), "尚无原版压缩上下文时不能承诺接受Zstd切换");
        assertNull(channel.readOutbound(), "空的false确认不是原版回落，不能发送" );
    }

    @Test
    void sameTcpReadPassesVanillaFrameThenSwitchThenContinuousZstdFrame() {
        openNegotiatingChannel();
        assertNotNull(channel.pipeline().get("zstd_switch_sniffer"));
        assertNull(channel.pipeline().get("zstd_decoder"), "READY 和 SetCompression 期间仍使用原版解压");

        byte[] vanillaPacket = {0x03, 0x31, 0x32, 0x33};
        byte[] switchPacket = switchPacket(37);
        byte[] nextZstdPacket = {0x01, 0x55, 0x66, 0x77};
        byte[] rawStream = packetWithLength(nextZstdPacket);

        byte[] sameTcpRead = concatenate(
                vanillaCompressedFrame(vanillaPacket),
                vanillaCompressedFrame(switchPacket),
                zstdFrame(rawStream));
        channel.writeInbound(Unpooled.wrappedBuffer(sameTcpRead));

        assertEquals(ZstdChannelManager.TransportState.ZSTD_ACTIVE,
                channel.attr(ZstdChannelManager.ZSTD_STATE).get());
        assertTrue(Boolean.TRUE.equals(channel.attr(ZstdClientActivation.ACTIVATED).get()));
        assertNull(channel.pipeline().get("zstd_switch_sniffer"));
        assertNotNull(channel.pipeline().get("zstd_decoder"));

        ByteBuf decodedVanilla = channel.readInbound();
        ByteBuf decodedZstd = channel.readInbound();
        try {
            assertNotNull(decodedVanilla, "切换前的zlib帧必须走原版解压器");
            assertArrayEquals(vanillaPacket, bytes(decodedVanilla));
            assertNotNull(decodedZstd, "同一TCP读取中switch后的下一帧必须立即走Zstd解码器");
            assertArrayEquals(nextZstdPacket, bytes(decodedZstd));
            assertNull(channel.readInbound(), "switch控制帧本身不能交给Minecraft包解码器");
        } finally {
            ReferenceCountUtil.release(decodedVanilla);
            ReferenceCountUtil.release(decodedZstd);
        }

        ByteBuf confirmation = channel.readOutbound();
        try {
            assertNotNull(confirmation, "decoder安装成功后应发送switch确认");
            assertEquals(0x02, confirmation.readUnsignedByte(), "确认包ID");
            assertEquals(37, readVarInt(confirmation), "确认必须回显switch transaction ID");
            assertTrue(confirmation.readBoolean());
            assertEquals(ZstdSwitchGate.MAGIC, confirmation.readInt());
            assertFalse(confirmation.isReadable());
        } finally {
            ReferenceCountUtil.release(confirmation);
        }
    }

    @Test
    void sameTcpReadPassesStoredVanillaPacketsThenSwitchThenContinuousZstdFrame() {
        openNegotiatingChannel();
        byte[] vanillaPacket = {0x03, 0x21, 0x22};
        byte[] nextZstdPacket = {0x01, 0x41, 0x42, 0x43};
        byte[] sameTcpRead = concatenate(
                vanillaStoredFrame(vanillaPacket),
                vanillaStoredFrame(switchPacket(53)),
                zstdFrame(packetWithLength(nextZstdPacket)));

        channel.writeInbound(Unpooled.wrappedBuffer(sameTcpRead));

        ByteBuf decodedVanilla = channel.readInbound();
        ByteBuf decodedZstd = channel.readInbound();
        try {
            assertNotNull(decodedVanilla);
            assertArrayEquals(vanillaPacket, bytes(decodedVanilla));
            assertNotNull(decodedZstd);
            assertArrayEquals(nextZstdPacket, bytes(decodedZstd));
            assertEquals(ZstdChannelManager.TransportState.ZSTD_ACTIVE,
                    channel.attr(ZstdChannelManager.ZSTD_STATE).get());
        } finally {
            ReferenceCountUtil.release(decodedVanilla);
            ReferenceCountUtil.release(decodedZstd);
        }
        ByteBuf confirmation = channel.readOutbound();
        try {
            assertNotNull(confirmation);
            assertEquals(0x02, confirmation.readUnsignedByte());
            assertEquals(53, readVarInt(confirmation));
            assertTrue(confirmation.readBoolean());
            assertEquals(ZstdSwitchGate.MAGIC, confirmation.readInt());
        } finally {
            ReferenceCountUtil.release(confirmation);
        }
    }

    @Test
    void fragmentedMultiByteOuterLengthAndSwitchBodyDoNotActivateEarly() {
        openNegotiatingChannel();

        byte[] largePacket = new byte[512];
        new Random(83).nextBytes(largePacket);
        largePacket[0] = 0x03;
        byte[] largeVanillaFrame = vanillaCompressedFrame(largePacket);
        assertTrue((largeVanillaFrame[0] & 0x80) != 0, "MC outer frame length must use multiple VarInt bytes");
        channel.writeInbound(Unpooled.wrappedBuffer(largeVanillaFrame, 0, 1));
        assertEquals(ZstdChannelManager.TransportState.NEGOTIATING,
                channel.attr(ZstdChannelManager.ZSTD_STATE).get());
        assertNull(channel.pipeline().get("zstd_decoder"));
        assertNull(channel.readInbound(), "partial outer VarInt must stay buffered");

        channel.writeInbound(Unpooled.wrappedBuffer(largeVanillaFrame, 1, largeVanillaFrame.length - 1));
        ByteBuf decodedLarge = channel.readInbound();
        try {
            assertNotNull(decodedLarge);
            assertArrayEquals(largePacket, bytes(decodedLarge));
            assertEquals(ZstdChannelManager.TransportState.NEGOTIATING,
                    channel.attr(ZstdChannelManager.ZSTD_STATE).get());
            assertNull(channel.pipeline().get("zstd_decoder"));
        } finally {
            ReferenceCountUtil.release(decodedLarge);
        }

        byte[] switchFrame = vanillaCompressedFrame(switchPacket(83));
        channel.writeInbound(Unpooled.wrappedBuffer(switchFrame, 0, switchFrame.length - 1));
        assertEquals(ZstdChannelManager.TransportState.NEGOTIATING,
                channel.attr(ZstdChannelManager.ZSTD_STATE).get(), "incomplete switch body cannot advance state");
        assertNull(channel.pipeline().get("zstd_decoder"));
        assertNull(channel.readInbound());
        assertNull(channel.readOutbound());

        channel.writeInbound(Unpooled.wrappedBuffer(switchFrame, switchFrame.length - 1, 1));
        assertEquals(ZstdChannelManager.TransportState.ZSTD_ACTIVE,
                channel.attr(ZstdChannelManager.ZSTD_STATE).get());
        assertNotNull(channel.pipeline().get("zstd_decoder"));
        assertNull(channel.pipeline().get("zstd_switch_sniffer"));
        ByteBuf confirmation = channel.readOutbound();
        try {
            assertNotNull(confirmation);
            assertEquals(0x02, confirmation.readUnsignedByte());
            assertEquals(83, readVarInt(confirmation));
            assertTrue(confirmation.readBoolean());
            assertEquals(ZstdSwitchGate.MAGIC, confirmation.readInt());
        } finally {
            ReferenceCountUtil.release(confirmation);
        }
    }

    @Test
    void duplicateCompressionPreparationAfterSwitchKeepsDecoderAndDoesNotRearmSniffer() {
        openNegotiatingChannel();
        channel.writeInbound(Unpooled.wrappedBuffer(vanillaCompressedFrame(switchPacket(91))));
        ZstdBatchDecoder decoder = (ZstdBatchDecoder) channel.pipeline().get("zstd_decoder");
        assertNotNull(decoder);
        assertNull(channel.pipeline().get("zstd_switch_sniffer"));

        ZstdClientActivation.prepareForSwitch(channel);
        ZstdClientActivation.prepareForSwitch(channel);

        assertSame(decoder, channel.pipeline().get("zstd_decoder"));
        assertNull(channel.pipeline().get("zstd_switch_sniffer"),
                "重复SetCompression不得在连续Zstd流中重新安装魔数嗅探器");
        assertEquals(ZstdChannelManager.TransportState.ZSTD_ACTIVE,
                channel.attr(ZstdChannelManager.ZSTD_STATE).get());
    }

    @Test
    void oversizedPreSwitchVanillaFramePassesSnifferFastPath() {
        openNegotiatingChannel();
        byte[] packet = new byte[512];
        new Random(0x5A535743L).nextBytes(packet);
        packet[0] = 0x03; // 不是LoginPluginRequest/QueryAnswer，不能命中switch嗅探。
        byte[] frame = vanillaCompressedFrame(packet);
        assertTrue(frameLength(frame) > 64, "用例须实际覆盖嗅探器的大帧快速透传分支");

        for (int i = 0; i < frame.length - 1; i++) {
            channel.writeInbound(Unpooled.wrappedBuffer(new byte[]{frame[i]}));
            assertEquals(ZstdChannelManager.TransportState.NEGOTIATING,
                    channel.attr(ZstdChannelManager.ZSTD_STATE).get());
            assertNull(channel.pipeline().get("zstd_decoder"));
            assertNull(channel.readInbound(), "split outer length/body must remain buffered by Minecraft splitter");
        }
        channel.writeInbound(Unpooled.wrappedBuffer(new byte[]{frame[frame.length - 1]}));
        ByteBuf decoded = channel.readInbound();
        try {
            assertNotNull(decoded);
            assertArrayEquals(packet, bytes(decoded));
            assertEquals(ZstdChannelManager.TransportState.NEGOTIATING,
                    channel.attr(ZstdChannelManager.ZSTD_STATE).get());
            assertNull(channel.pipeline().get("zstd_decoder"));
        } finally {
            ReferenceCountUtil.release(decoded);
        }
    }

    private void openNegotiatingChannel() {
        channel = new EmbeddedChannel(new MinecraftFrameSplitter());
        channel.pipeline().addLast("decoder", new ChannelInboundHandlerAdapter());
        channel.pipeline().addLast("encoder", new ChannelOutboundHandlerAdapter());
        manager = new ZstdChannelManager();
        channel.attr(ZstdChannelManager.KEY).set(manager);
        channel.closeFuture().addListener(future -> manager.close());
        channel.attr(ZstdChannelManager.ZSTD_STATE).set(ZstdChannelManager.TransportState.NEGOTIATING);

        // READY 在 compression anchor 出现前先挂接到 packet decoder 上游。
        ZstdClientActivation.prepareForSwitch(channel);
        channel.runPendingTasks();
        assertNotNull(channel.pipeline().get("zstd_switch_sniffer"));
        assertNull(channel.pipeline().get("zstd_decoder"));

        // 模拟后续 SetCompression 在同一 packet decoder 前安装原版解压器。
        channel.pipeline().addBefore("decoder", "decompress", new VanillaCompressionDecoder());
        ZstdClientActivation.prepareForSwitch(channel);
        channel.runPendingTasks();
        assertTrue(channel.pipeline().names().indexOf("zstd_switch_sniffer")
                < channel.pipeline().names().indexOf("decompress"));
    }

    private static byte[] switchPacket(int txId) {
        ByteBuf packet = ZstdStepFrames.writeQuery(Unpooled.buffer(), txId, "zstd:switch");
        try {
            byte[] bytes = bytes(packet);
            return bytes;
        } finally {
            packet.release();
        }
    }

    /** FRAME8的连接级流中仅包含连续magicless Zstd字节，不添加旧版帧元数据。 */
    private static byte[] zstdFrame(byte[] raw) {
        try (ZstdCompressCtx compressor = new ZstdCompressCtx()) {
            compressor.setLevel(3).setWindowLog(20).setMagicless(true)
                    .setContentSize(false).setDictID(false).setChecksum(false);
            ByteBuffer source = ByteBuffer.allocateDirect(raw.length);
            source.put(raw).flip();
            ByteBuffer destination = ByteBuffer.allocateDirect(4096);
            boolean flushed = false;
            while (!flushed) {
                flushed = compressor.compressDirectByteBufferStream(destination, source, EndDirective.FLUSH);
                if (!flushed && !destination.hasRemaining()) fail("test Zstd output buffer exhausted");
            }
            byte[] stream = new byte[destination.position()];
            destination.flip();
            destination.get(stream);
            return lengthFrame(stream);
        }
    }

    private static byte[] packetWithLength(byte[] packet) {
        ByteBuf raw = Unpooled.buffer();
        try {
            writeVarInt(raw, packet.length);
            raw.writeBytes(packet);
            return bytes(raw);
        } finally {
            raw.release();
        }
    }

    /** 模拟FRP隧道另一端恢复后的原版MC zlib frame。 */
    private static byte[] vanillaStoredFrame(byte[] packet) {
        ByteBuf body = Unpooled.buffer();
        try {
            body.writeByte(0); // MC compression dataLength=0: transport仍走原版stored packet路径。
            body.writeBytes(packet);
            return lengthFrame(bytes(body));
        } finally {
            body.release();
        }
    }

    private static byte[] vanillaCompressedFrame(byte[] packet) {
        Deflater deflater = new Deflater(6);
        byte[] encoded = new byte[packet.length + 128];
        deflater.setInput(packet);
        deflater.finish();
        int count = deflater.deflate(encoded);
        assertTrue(deflater.finished());
        deflater.end();
        ByteBuf body = Unpooled.buffer();
        try {
            writeVarInt(body, packet.length);
            body.writeBytes(encoded, 0, count);
            return lengthFrame(bytes(body));
        } finally {
            body.release();
        }
    }

    private static byte[] lengthFrame(byte[] body) {
        ByteBuf frame = Unpooled.buffer();
        try {
            writeVarInt(frame, body.length);
            frame.writeBytes(body);
            return bytes(frame);
        } finally {
            frame.release();
        }
    }

    private static byte[] concatenate(byte[]... arrays) {
        int length = Arrays.stream(arrays).mapToInt(value -> value.length).sum();
        byte[] combined = new byte[length];
        int offset = 0;
        for (byte[] array : arrays) {
            System.arraycopy(array, 0, combined, offset, array.length);
            offset += array.length;
        }
        return combined;
    }

    private static int frameLength(byte[] framed) {
        int[] cursor = {0};
        return readVarInt(framed, cursor);
    }

    private static int readVarInt(ByteBuf buffer) {
        int value = 0;
        int shift = 0;
        while (true) {
            int next = buffer.readUnsignedByte();
            value |= (next & 0x7F) << shift;
            if ((next & 0x80) == 0) return value;
            shift += 7;
            if (shift > 28) throw new IllegalArgumentException("VarInt too long");
        }
    }

    private static int readVarInt(byte[] bytes, int[] cursor) {
        int value = 0;
        int shift = 0;
        while (true) {
            int next = bytes[cursor[0]++] & 0xFF;
            value |= (next & 0x7F) << shift;
            if ((next & 0x80) == 0) return value;
            shift += 7;
            if (shift > 28) throw new IllegalArgumentException("VarInt too long");
        }
    }

    private static void writeVarInt(ByteBuf buffer, int value) {
        while ((value & -128) != 0) {
            buffer.writeByte(value & 0x7F | 0x80);
            value >>>= 7;
        }
        buffer.writeByte(value);
    }

    private static byte[] bytes(ByteBuf buffer) {
        byte[] result = new byte[buffer.readableBytes()];
        buffer.getBytes(buffer.readerIndex(), result);
        return result;
    }

    private static final class MinecraftFrameSplitter extends ByteToMessageDecoder {
        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
            int start = in.readerIndex();
            int length = 0;
            int prefixBytes = 0;
            int shift = 0;
            while (prefixBytes < 3) {
                if (start + prefixBytes >= in.writerIndex()) return;
                int next = in.getUnsignedByte(start + prefixBytes++);
                length |= (next & 0x7F) << shift;
                if ((next & 0x80) == 0) break;
                shift += 7;
            }
            if (prefixBytes == 3 && (in.getUnsignedByte(start + prefixBytes - 1) & 0x80) != 0) {
                throw new IllegalArgumentException("Minecraft frame VarInt too long");
            }
            if (length < 0 || length > (1 << 21) - 1) {
                throw new IllegalArgumentException("Minecraft frame length out of range");
            }
            if (in.readableBytes() < prefixBytes + length) return;
            in.skipBytes(prefixBytes);
            out.add(in.readRetainedSlice(length));
        }
    }

    private static final class VanillaCompressionDecoder extends MessageToMessageDecoder<ByteBuf> {
        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
            int declared = readVarInt(in);
            if (declared == 0) {
                out.add(in.readRetainedSlice(in.readableBytes()));
                return;
            }
            byte[] source = bytes(in);
            byte[] decoded = new byte[declared];
            Inflater inflater = new Inflater();
            try {
                inflater.setInput(source);
                int count = inflater.inflate(decoded);
                if (count != declared || !inflater.finished() || inflater.getRemaining() != 0) {
                    throw new IllegalArgumentException("invalid vanilla compressed test frame");
                }
                out.add(Unpooled.wrappedBuffer(decoded));
            } catch (java.util.zip.DataFormatException error) {
                throw new IllegalArgumentException(error);
            } finally {
                inflater.end();
            }
        }
    }
}
