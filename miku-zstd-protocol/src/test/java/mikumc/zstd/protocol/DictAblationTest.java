package mikumc.zstd.protocol;

import com.github.luben.zstd.EndDirective;
import com.github.luben.zstd.ZstdCompressCtx;
import com.github.luben.zstd.ZstdDecompressCtx;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/**
 * one-shot 与同一ctx多次CONTINUE/FLUSH的合成消融，仅记录压缩体，不含运输帧头。
 * 不依赖已退出的滚动字典，不把探针比率作为生产收益或通过门槛。
 */
class DictAblationTest {
    private static final int LEVEL = 3;
    private static final int[] BAND_LIMIT = {128, 4096};
    private static final long CORPUS_BYTES = 12L * 1024 * 1024;

    private static final class Result {
        final long[] band = new long[3];
        long total;
        int count;
    }

    @Test
    void ablation() throws IOException {
        Corpus corpus = build();
        Result oneShot = run(corpus.packets, false);
        Result continuous = run(corpus.packets, true);
        StringBuilder report = new StringBuilder();
        report.append("==== synthetic one-shot vs continuous probe ====\n");
        report.append("Probe only: not a production bandwidth, latency or CPU claim.\n");
        report.append("Body bytes only. continuous: one FLUSH/packet, no END/reset; one-shot: independent complete frames.\n");
        report.append(String.format(Locale.ROOT, "raw=%dB samples=%d chunks=%d level=%d%n",
                corpus.rawTotal, corpus.packets.size(), corpus.chunks, LEVEL));
        report.append(String.format(Locale.ROOT, "%-12s %12s %12s %12s%n",
                "band", "one-shot", "continuous", "body delta"));
        String[] bands = {"<128B", "128B-4KB", ">=4KB"};
        for (int i = 0; i < bands.length; i++) {
            report.append(String.format(Locale.ROOT, "%-12s %12d %12d %11.2f%%%n",
                    bands[i], oneShot.band[i], continuous.band[i], pct(oneShot.band[i], continuous.band[i])));
        }
        report.append(String.format(Locale.ROOT, "%-12s %12d %12d %11.2f%%%n",
                "TOTAL", oneShot.total, continuous.total, pct(oneShot.total, continuous.total)));
        System.out.print(report);
        // 保持现有CI报告产物路径；类名仅为兼容现有test过滤器。
        Path out = Path.of("build", "ablation-report.txt");
        Files.createDirectories(out.getParent());
        Files.writeString(out, report.toString(), StandardCharsets.UTF_8);
        assertEquals(corpus.packets.size(), oneShot.count);
        assertEquals(oneShot.count, continuous.count);
        assertTrue(oneShot.total > 0 && continuous.total > 0);
        // 不断言哪种更小：语料、flush频率和参数都可能影响消融结果。
    }

    private static double pct(long base, long value) {
        return base == 0 ? 0 : (1.0 - (double) value / base) * 100;
    }

    private static Result run(List<byte[]> corpus, boolean continuous) {
        Result result = new Result();
        try (ZstdCompressCtx compress = new ZstdCompressCtx();
             ZstdDecompressCtx decompress = new ZstdDecompressCtx()) {
            compress.setLevel(LEVEL).setMagicless(true).setContentSize(false).setDictID(false);
            decompress.setMagicless(true);
            ByteBuffer source = ByteBuffer.allocateDirect(64 * 1024);
            ByteBuffer destination = ByteBuffer.allocateDirect(1021);
            ByteBuffer decodeDestination = ByteBuffer.allocateDirect(4093);
            for (byte[] packet : corpus) {
                byte[] raw = inner(packet);
                byte[] compressed;
                if (continuous) {
                    source.clear(); source.put(raw).flip();
                    ByteArrayOutputStream output = new ByteArrayOutputStream();
                    while (source.hasRemaining()) {
                        int before = source.position(); destination.clear();
                        compress.compressDirectByteBufferStream(destination, source, EndDirective.CONTINUE);
                        int produced = append(output, destination);
                        assertTrue(source.position() != before || produced > 0, "CONTINUE须有位置进展");
                    }
                    boolean flushed;
                    do {
                        destination.clear();
                        flushed = compress.compressDirectByteBufferStream(destination, source, EndDirective.FLUSH);
                        int produced = append(output, destination);
                        assertTrue(flushed || produced > 0, "FLUSH未完成时须输出字节");
                    } while (!flushed);
                    compressed = output.toByteArray();
                    ByteBuffer encoded = ByteBuffer.allocateDirect(compressed.length);
                    encoded.put(compressed).flip();
                    ByteArrayOutputStream decoded = new ByteArrayOutputStream();
                    boolean full;
                    do {
                        int before = encoded.position(); decodeDestination.clear();
                        boolean ended = decompress.decompressDirectByteBufferStream(decodeDestination, encoded);
                        full = !decodeDestination.hasRemaining();
                        int produced = append(decoded, decodeDestination);
                        assertFalse(ended, "连续探针只FLUSH，不应出现native END");
                        if (encoded.hasRemaining()) assertTrue(encoded.position() != before || produced > 0);
                    } while (encoded.hasRemaining() || full);
                    assertArrayEquals(raw, decoded.toByteArray(), "连续ctx逐样本真实往返");
                } else {
                    compressed = compress.compress(raw);
                    assertArrayEquals(raw, decompress.decompress(compressed, raw.length), "one-shot真实往返");
                }
                assertTrue(compressed.length > 0);
                result.count++;
                result.band[bandOf(packet.length)] += compressed.length;
                result.total += compressed.length;
            }
        }
        return result;
    }

    private static int append(ByteArrayOutputStream output, ByteBuffer buffer) {
        int produced = buffer.position(); buffer.flip();
        byte[] bytes = new byte[produced]; buffer.get(bytes); output.writeBytes(bytes); return produced;
    }

    private static byte[] inner(byte[] packet) {
        ByteBuf buf = Unpooled.buffer(packet.length + 5);
        try {
            ZstdVarInts.write(buf, packet.length); buf.writeBytes(packet);
            byte[] raw = new byte[buf.readableBytes()]; buf.readBytes(raw); return raw;
        } finally { buf.release(); }
    }

    private static int bandOf(int len) {
        if (len < BAND_LIMIT[0]) return 0;
        return len < BAND_LIMIT[1] ? 1 : 2;
    }

    /** 语料 + 它有多少个区块，仅用于报告。 */
    private static final class Corpus {
        final List<byte[]> packets = new ArrayList<>();
        int chunks;
        long rawTotal;
    }

    /**
     * 合成语料：区块形态 + 小包 + 引用当前片段的中包，仅作为可复现探针。
     *
     * <p>每个合成区块的调色板不同，包含人为选择的短期重复。探针不代表真实MC抓包，
     * 不据此推断生产带宽、CPU、延迟或玩家规模收益。</p>
     */
    private static Corpus build() {
        Random rnd = new Random(20261101L);
        Corpus c = new Corpus();
        long bytes = 0;
        long chunkIndex = 0;
        // 合成共享词表；不代表生产频次。
        byte[][] words = {
                "minecraft:overworld".getBytes(StandardCharsets.US_ASCII),
                "minecraft:the_nether".getBytes(StandardCharsets.US_ASCII),
                "minecraft:the_end".getBytes(StandardCharsets.US_ASCII),
                "minecraft:stone".getBytes(StandardCharsets.US_ASCII),
                "textures/block/stone.png".getBytes(StandardCharsets.US_ASCII),
                "mob_effect".getBytes(StandardCharsets.US_ASCII),
        };
        byte[] palette = null;
        while (bytes < CORPUS_BYTES) {
            int roll = rnd.nextInt(100);
            if (roll < 8) {
                // 区块包：独立调色板 + 由下标构成的大段数据
                palette = new byte[64];
                new Random(900000 + chunkIndex).nextBytes(palette);
                byte[] chunk = new byte[8192 + rnd.nextInt(16384)];
                chunk[0] = 0x25;
                chunk[1] = (byte) (chunkIndex & 0xFF);
                System.arraycopy(palette, 0, chunk, 2, 64);
                int pos = 66;
                int idx = rnd.nextInt(16);
                while (pos < chunk.length) {
                    int run = 1 + rnd.nextInt(24);
                    byte v = (byte) (idx & 0x0F);
                    for (int i = 0; i < run && pos < chunk.length; i++) chunk[pos++] = v;
                    idx = (idx + (rnd.nextBoolean() ? 1 : 15)) & 0x0F;
                }
                c.packets.add(chunk);
                bytes += chunk.length;
                c.chunks++;
                chunkIndex++;
            } else if (roll < 85) {
                // 小包：词表 + 变化的数值（实体/聊天/计分板形态）
                int len = 40 + rnd.nextInt(80);
                byte[] b = new byte[len];
                b[0] = (byte) (0x20 + rnd.nextInt(8));
                byte[] w = words[rnd.nextInt(words.length)];
                int pos = 1;
                System.arraycopy(w, 0, b, pos, Math.min(w.length, len - pos));
                pos += Math.min(w.length, len - pos);
                while (pos < len) {
                    b[pos++] = (byte) rnd.nextInt(256);
                }
                c.packets.add(b);
                bytes += b.length;
            } else {
                // 中包：人为植入当前片段的短期重复，供连续流历史探针观察。
                int len = 200 + rnd.nextInt(2800);
                byte[] b = new byte[len];
                b[0] = 0x31;
                int pos = 1;
                if (palette != null) {
                    System.arraycopy(palette, 0, b, pos, Math.min(64, len - pos));
                    pos += Math.min(64, len - pos);
                }
                while (pos < len) {
                    b[pos++] = palette == null ? 0 : palette[rnd.nextInt(16) * 4];
                }
                c.packets.add(b);
                bytes += b.length;
            }
        }
        for (byte[] pk : c.packets) {
            c.rawTotal += ZstdVarInts.length(pk.length) + pk.length;
        }
        return c;
    }
}
