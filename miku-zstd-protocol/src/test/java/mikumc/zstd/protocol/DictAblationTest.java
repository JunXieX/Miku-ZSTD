package mikumc.zstd.protocol;

import com.github.luben.zstd.ZstdCompressCtx;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 同一份合成语料对照无字典与生产滚动历史，只统计压缩体，不含线路帧头。
 * 每个样本按一次 flush 的单包批次补长度前缀；历史喂入与轮换直接使用生产实现。
 */
class DictAblationTest {

    private static final int LEVEL = 3;
    private static final int[] BAND_LIMIT = {128, 4096};
    private static final long CORPUS_BYTES = 12L * 1024 * 1024;

    private static final class Result {
        final long[] band = new long[3];
        long total;
        int count;
        int rotations;
    }

    @Test
    void ablation() throws IOException {
        Corpus corpus = build();
        Result none = run(corpus.packets, false);
        Result rolling = run(corpus.packets, true);

        StringBuilder sb = new StringBuilder();
        sb.append(String.format("==== dict ablation (synthetic, production rolling history) ====%n"));
        sb.append(String.format("raw=%dB, samples=%d, chunks=%d, level=%d, dict=%dB, rotate=%dB%n",
                corpus.rawTotal, none.count, corpus.chunks, LEVEL,
                ZstdRollingDict.DICT_BYTES, ZstdRollingDict.ROTATE_AFTER_BYTES));
        sb.append(String.format("%-12s %11s %11s %8s%n",
                "band", "none", "rolling", "vs none"));
        String[] names = {"<128B", "128B-4KB", ">=4KB"};
        for (int i = 0; i < names.length; i++) {
            sb.append(String.format("%-12s %11d %11d %7.1f%%%n",
                    names[i], none.band[i], rolling.band[i], pct(none.band[i], rolling.band[i])));
        }
        sb.append(String.format("%-12s %11d %11d %7.1f%%%n",
                "TOTAL", none.total, rolling.total, pct(none.total, rolling.total)));
        sb.append(String.format("生产滚动次数：%d%n", rolling.rotations));
        System.out.print(sb);
        Path out = Path.of("build", "ablation-report.txt");
        Files.createDirectories(out.getParent());
        Files.writeString(out, sb.toString(), StandardCharsets.UTF_8);

        assertTrue(rolling.rotations > 0, "语料必须触发真实滚动历史轮换，避免对照空转");
        assertTrue(rolling.total <= none.total * 1.01,
                "滚动历史不得让压缩体变差，实际 " + pct(none.total, rolling.total) + "%");
    }

    private static double pct(long base, long value) {
        return base == 0 ? 0 : (1.0 - (double) value / base) * 100;
    }

    private Result run(List<byte[]> corpus, boolean useRolling) {
        Result r = new Result();
        r.count = corpus.size();
        ZstdRollingDict history = useRolling ? new ZstdRollingDict() : null;
        byte[] appliedDict = null;
        ZstdCompressCtx ctx = new ZstdCompressCtx();
        try {
            ctx.setLevel(LEVEL);
            ctx.setMagicless(true);
            ctx.setContentSize(false);
            ctx.setDictID(false);
            for (byte[] packet : corpus) {
                byte[] raw = new byte[ZstdVarInts.length(packet.length) + packet.length];
                ByteBuf batch = Unpooled.wrappedBuffer(raw);
                try {
                    batch.clear();
                    ZstdVarInts.write(batch, packet.length);
                    batch.writeBytes(packet);
                } finally {
                    batch.release();
                }
                if (history != null && history.activeDict() != appliedDict) {
                    appliedDict = history.activeDict();
                    ctx.loadDict(appliedDict);
                }
                int n = ctx.compress(raw).length;
                r.band[bandOf(packet.length)] += n;
                r.total += n;
                if (history != null) {
                    history.feed(raw, 0, raw.length);
                }
            }
        } finally {
            ctx.close();
        }
        r.rotations = history == null ? 0 : history.generation();
        return r;
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
     * 造语料：区块流 + 小包 + 引用当前区块的中包。
     *
     * <p>关键点：<b>每个区块的调色板都不一样</b>，所以跨区块的字节重复很低——静态字典学不到
     * 未来区块的调色板，而滚动历史也只能吃到"刚刚发过的那片"。这才是 MC 的真实形态。</p>
     */
    private static Corpus build() {
        Random rnd = new Random(20261101L);
        Corpus c = new Corpus();
        long bytes = 0;
        long chunkIndex = 0;
        // 共享小词表：维度名、常见字符串、字段名（真实世界里这些确实跨会话重复）
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
                // 中包：引用"当前区块调色板"的方块更新 —— 只有滚动历史吃得到
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
