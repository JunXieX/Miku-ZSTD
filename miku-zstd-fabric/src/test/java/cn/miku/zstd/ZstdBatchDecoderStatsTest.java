package cn.miku.zstd;

import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.LongAdder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

class ZstdBatchDecoderStatsTest {

    @Test
    @DisplayName("下行统计适配器：线路含 MC 外长，原始流字节不补标记")
    void compressedFrameKeepsExactWireCount() throws ReflectiveOperationException {
        Field inField = ZstdStatsData.class.getDeclaredField("ACCUM_RX_IN");
        Field outField = ZstdStatsData.class.getDeclaredField("ACCUM_RX_OUT");
        inField.setAccessible(true);
        outField.setAccessible(true);
        LongAdder incoming = (LongAdder) inField.get(null);
        LongAdder original = (LongAdder) outField.get(null);
        long beforeIn = incoming.sum();
        long beforeOut = original.sum();

        ZstdBatchDecoder decoder = new ZstdBatchDecoder();
        assertFalse(decoder.hasStartedStream());
        decoder.onCompressedFrame(130, 5000);
        assertTrue(decoder.hasStartedStream());
        assertEquals(beforeIn + 130, incoming.sum(), "适配器须原样计入基类给出的完整线路字节");
        assertEquals(beforeOut + 5000, original.sum(), "原始侧仅计实际解出的内层流字节");

        decoder.onCompressedFrame(18, 0);
        assertEquals(beforeIn + 148, incoming.sum(), "只推进流状态的帧也须计入线路字节");
        assertEquals(beforeOut + 5000, original.sum(), "零输出帧不得虚增原始字节");
    }

    @Test
    @DisplayName("同步交包已推进流状态：关闭压缩时断开，不能丢半包后重建")
    void thresholdDisableClosesAfterPacketForwarding() {
        ZstdBatchDecoder decoder = new ZstdBatchDecoder();
        EmbeddedChannel channel = installedChannel(decoder, false);
        try {
            assertSame(channel.pipeline().context("decompress"),
                    decoder.decodeForwardTarget(channel.pipeline().context("zstd_decoder")));
            assertTrue(decoder.hasStartedStream());
            ZstdClientActivation.onCompressionThreshold(channel, -1);
            channel.runPendingTasks();
            assertFalse(channel.isActive());
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    @DisplayName("尚未收到流字节：初始负阈值保留原版连接")
    void thresholdDisableBeforeStreamKeepsVanillaConnection() {
        ZstdBatchDecoder decoder = new ZstdBatchDecoder();
        EmbeddedChannel channel = installedChannel(decoder, false);
        try {
            assertFalse(decoder.hasStartedStream());
            ZstdClientActivation.onCompressionThreshold(channel, -1);
            channel.runPendingTasks();
            assertTrue(channel.isActive());
            assertNull(channel.pipeline().get("zstd_decoder"));
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    @DisplayName("已安装解码器发生错位：关闭连接，不移除后新建丢失半包")
    void readyDecoderMisplacementClosesInsteadOfRebuilding() {
        ZstdBatchDecoder decoder = new ZstdBatchDecoder();
        EmbeddedChannel channel = installedChannel(decoder, true);
        try {
            ZstdClientActivation.prepareForSwitch(channel);
            channel.runPendingTasks();
            assertFalse(channel.isActive());
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    private static EmbeddedChannel installedChannel(ZstdBatchDecoder decoder, boolean misplaced) {
        EmbeddedChannel channel = new EmbeddedChannel();
        channel.pipeline().addLast("decompress", new ChannelInboundHandlerAdapter());
        if (misplaced) channel.pipeline().addAfter("decompress", "zstd_decoder", decoder);
        else channel.pipeline().addBefore("decompress", "zstd_decoder", decoder);
        channel.attr(ZstdChannelManager.ZSTD_STATE).set(ZstdChannelManager.TransportState.ZSTD_ACTIVE);
        channel.attr(ZstdClientActivation.ACTIVATED).set(Boolean.TRUE);
        return channel;
    }
}
