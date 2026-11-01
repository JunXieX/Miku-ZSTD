package cn.miku.zstd;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.LongAdder;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ZstdBatchDecoderStatsTest {

    @Test
    @DisplayName("下行统计适配器：完整压缩帧体不重复补头，原始侧保留直存基线")
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
        decoder.onCompressedFrame(130, 5000);
        assertEquals(beforeIn + 130, incoming.sum(), "适配器须原样计入基类给出的完整帧体");
        assertEquals(beforeOut + 5001, original.sum(), "原始侧保持原有的一字节直存比较基线");

        decoder.onStoredFrame(17);
        assertEquals(beforeIn + 148, incoming.sum(), "直存仍包含一字节标记");
        assertEquals(beforeOut + 5019, original.sum());
    }
}
