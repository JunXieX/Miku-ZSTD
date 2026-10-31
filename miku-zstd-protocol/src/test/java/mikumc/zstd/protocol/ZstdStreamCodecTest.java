package mikumc.zstd.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * {@link ZstdStreamCodec}：跨批共享历史的流式编解码对称性。
 *
 * <p>这组用例是接入协议 v6 的前置条件：编码端与解码端必须对"同一段流"给出完全一致的
 * 字节，否则症状是"能连上、几秒后断"（用户对这类问题的容忍度为零）。</p>
 */
class ZstdStreamCodecTest {

    @Test
    @DisplayName("流式编解码：多批连续数据必须逐字节还原")
    void roundTripAcrossBatches() throws Exception {
        ZstdStreamCodec.Encoder enc = new ZstdStreamCodec.Encoder(3, 0);
        ZstdStreamCodec.Decoder dec = new ZstdStreamCodec.Decoder();

        Random random = new Random(42);
        for (int round = 0; round < 8; round++) {
            // 每批内容相似度很高——正是流式该吃到收益的场景
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 400; i++) {
                sb.append("{\"id\":\"").append(random.nextInt(64)).append("\",\"v\":").append(round).append('}');
            }
            byte[] src = sb.toString().getBytes(StandardCharsets.UTF_8);

            ByteBuf raw = Unpooled.wrappedBuffer(src);
            ByteBuf chunk = enc.encode(raw, src.length);
            assertNotNull(chunk, "第 " + round + " 批必须有输出");
            byte[] out = dec.decode(chunk, src.length);
            assertArrayEquals(src, out, "第 " + round + " 批必须逐字节还原");
            chunk.release();
            raw.release();
        }
        enc.close();
        dec.close();
    }

    @Test
    @DisplayName("流式编解码：顺序喂帧时不得依赖批大小整除关系")
    void roundTripWithVaryingSizes() throws Exception {
        ZstdStreamCodec.Encoder enc = new ZstdStreamCodec.Encoder(3, 0);
        ZstdStreamCodec.Decoder dec = new ZstdStreamCodec.Decoder();

        int[] sizes = {1, 7, 64, 1024, 33, 4096, 2};
        for (int size : sizes) {
            byte[] src = new byte[size];
            new Random(size).nextBytes(src);
            ByteBuf raw = Unpooled.wrappedBuffer(src);
            ByteBuf chunk = enc.encode(raw, size);
            assertNotNull(chunk);
            assertArrayEquals(src, dec.decode(chunk, size));
            chunk.release();
            raw.release();
        }
        enc.close();
        dec.close();
    }
}
