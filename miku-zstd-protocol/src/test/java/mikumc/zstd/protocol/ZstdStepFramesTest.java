package mikumc.zstd.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.util.zip.Deflater;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class ZstdStepFramesTest {

    @Test
    @DisplayName("下行切换确认：原版裸帧、阈值下直存与达到阈值的 zlib 帧均能识别")
    void recognizesConfirmAcrossVanillaCompressionThresholds() {
        for (int txId : new int[]{1, 127, 128, 16_384, Integer.MAX_VALUE}) {
            ByteBuf packet = ZstdStepFrames.writeConfirm(Unpooled.buffer(), txId, true);
            try {
                assertRecognizedAcrossThresholds(packet, txId);
            } finally {
                packet.release();
            }
        }
    }

    @Test
    @DisplayName("下行切换询问：原版三种外层形态均保留通道、魔数与 txId")
    void recognizesQueryAcrossVanillaCompressionThresholds() {
        for (int txId : new int[]{1, 128, 16_384, Integer.MAX_VALUE}) {
            ByteBuf packet = ZstdStepFrames.writeQuery(
                    Unpooled.buffer(), txId, ZstdSwitchGate.CHANNEL);
            try {
                assertRecognizedAcrossThresholds(packet, txId);
            } finally {
                packet.release();
            }
        }
    }

    @Test
    @DisplayName("原版 zlib 空应答：dataLength=4 与询问包 id 相同时仍按压缩帧识别")
    void recognizesCompressedUnconfirmedAnswerWithFourByteRawLength() {
        ByteBuf packet = ZstdStepFrames.writeConfirm(Unpooled.buffer(), 128, false);
        ByteBuf frame = vanillaFrame(packet, 0);
        try {
            assertEquals(4, packet.readableBytes(), "回归前提：压缩头首字节恰好是询问包 id");
            byte[] bare = ZstdStepFrames.asBarePacket(frame);
            assertNotNull(bare, "原版 zlib 空应答仍应可识别");
            assertEquals(128, ZstdStepFrames.readSwitchRejectTxId(bare));
            assertEquals(-1, ZstdStepFrames.readSwitchTxId(bare), "空应答不得被当成魔数确认");
        } finally {
            frame.release();
            packet.release();
        }
    }

    @Test
    @DisplayName("原版 zlib 确认：截断校验尾与附加尾随字节都不得成为有效确认")
    void rejectsTruncatedAndTrailingZlibConfirmFrames() {
        ByteBuf packet = ZstdStepFrames.writeConfirm(Unpooled.buffer(), 1, true);
        ByteBuf valid = vanillaFrame(packet, 0);
        ByteBuf truncated = valid.copy(valid.readerIndex(), valid.readableBytes() - 1);
        ByteBuf trailing = Unpooled.buffer()
                .writeBytes(valid, valid.readerIndex(), valid.readableBytes()).writeByte(0);
        try {
            assertNull(ZstdStepFrames.asBarePacket(truncated), "缺少完整 zlib 校验尾不得确认");
            assertNull(ZstdStepFrames.asBarePacket(trailing), "完整流后多余的字节不得确认");
        } finally {
            truncated.release();
            trailing.release();
            valid.release();
            packet.release();
        }
    }

    @Test
    @DisplayName("原版 zlib 确认：声明原长必须精确，解压出的包 id 必须是询问或应答")
    void rejectsWrongRawLengthAndNonHandshakePacketId() {
        ByteBuf packet = ZstdStepFrames.writeConfirm(Unpooled.buffer(), 1, true);
        byte[] raw = new byte[packet.readableBytes()];
        packet.getBytes(packet.readerIndex(), raw);
        try {
            byte[] compressed = zlib(raw);
            for (int declared : new int[]{raw.length - 1, raw.length + 1}) {
                ByteBuf frame = Unpooled.buffer();
                ZstdVarInts.write(frame, declared);
                frame.writeBytes(compressed);
                try {
                    assertNull(ZstdStepFrames.asBarePacket(frame), "声明原长不符不得确认");
                } finally {
                    frame.release();
                }
            }
            packet.setByte(packet.readerIndex(), 0x2A);
            ByteBuf wrongId = vanillaFrame(packet, 0);
            try {
                assertNull(ZstdStepFrames.asBarePacket(wrongId), "其它包 id 即使带魔数也不得确认");
            } finally {
                wrongId.release();
            }
        } finally {
            packet.release();
        }
    }

    private static void assertRecognizedAcrossThresholds(ByteBuf packet, int txId) {
        byte[] expected = new byte[packet.readableBytes() - 1];
        packet.getBytes(packet.readerIndex() + 1, expected);
        int rawLength = packet.readableBytes();
        for (int threshold : new int[]{-1, 0, rawLength - 1, rawLength, rawLength + 1, 256}) {
            ByteBuf frame = vanillaFrame(packet, threshold);
            try {
                int reader = frame.readerIndex();
                int writer = frame.writerIndex();
                byte[] bare = ZstdStepFrames.asBarePacket(frame);
                assertArrayEquals(expected, bare, "阈值=" + threshold + " 时必须还原完整包体");
                assertEquals(txId, ZstdStepFrames.readSwitchTxId(bare));
                assertEquals(reader, frame.readerIndex(), "嗅探不得消费原版入站帧");
                assertEquals(writer, frame.writerIndex(), "嗅探不得改写帧边界");
            } finally {
                frame.release();
            }
        }
    }

    /** 模拟原版压缩编码器；负阈值表示尚未启用 SetCompression。 */
    private static ByteBuf vanillaFrame(ByteBuf packet, int threshold) {
        ByteBuf frame = Unpooled.buffer();
        // 非零 readerIndex 也必须正确识别，避免把缓冲区起点误当作帧起点。
        frame.writeByte(0x7F);
        if (threshold < 0) {
            frame.writeBytes(packet, packet.readerIndex(), packet.readableBytes());
        } else if (packet.readableBytes() < threshold) {
            ZstdVarInts.write(frame, 0);
            frame.writeBytes(packet, packet.readerIndex(), packet.readableBytes());
        } else {
            byte[] raw = new byte[packet.readableBytes()];
            packet.getBytes(packet.readerIndex(), raw);
            ZstdVarInts.write(frame, raw.length);
            frame.writeBytes(zlib(raw));
        }
        frame.readerIndex(1);
        return frame;
    }

    private static byte[] zlib(byte[] raw) {
        Deflater deflater = new Deflater();
        try {
            deflater.setInput(raw);
            deflater.finish();
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            byte[] buffer = new byte[128];
            while (!deflater.finished()) {
                int count = deflater.deflate(buffer);
                bytes.write(buffer, 0, count);
            }
            return bytes.toByteArray();
        } finally {
            deflater.end();
        }
    }
}
