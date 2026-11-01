package mikumc.zstd.protocol;

import com.github.luben.zstd.ZstdCompressCtx;
import com.github.luben.zstd.ZstdDictCompress;
import com.github.luben.zstd.ZstdDictTrainer;
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
 * 字典消融对照：<b>只跑数据，不做门禁</b>（唯一断言是"滚动历史必须有效"，防用例空转）。
 *
 * <h2>它回答什么问题</h2>
 * <p>5.0.37 起"静态训练字典"（gen 0）与"滚动历史"（gen≥1）先后作用于同一条连接。
 * 线上日志只能给出"新候选打不过现有字典"，回答不了"滚动历史在场时，静态字典还剩多少价值"。
 * 本对照把四种组合放在<b>同一份语料</b>上跑，给出每档字节差。</p>
 *
 * <h2>语料为什么要"每块独立调色板"</h2>
 * <p>第一版语料把整个流都从一块 32KB 模板池里拼出来，于是 64KB 的字典能把"世界"整个背下来，
 * 静态字典拿到 95.8% 的假成绩——那是在测"字典能不能记住一个小词表"，不是测 MC 流量。
 * 现在改成真实结构：</p>
 * <ul>
 *   <li><b>区块包</b>：每块自带<b>独立调色板</b>（16×4B，块块不同）+ 由调色板下标构成的大段字节。
 *       块内重复极高、<b>块间几乎不重复</b> —— 这正是静态字典无能为力、而"刚刚发过的同一片区块"
 *       才有用的形态；</li>
 *   <li><b>小包</b>：共享一份真实世界存在的小词表（维度名、常见字符串、字段名）+ 变化的数值；</li>
 *   <li><b>中包</b>：引用<b>当前区块</b>调色板的方块更新 —— 只有滚动历史能吃到这份局部相关性。</li>
 * </ul>
 *
 * <h2>四个模式</h2>
 * <ul>
 *   <li>{@code none}：不装字典；</li>
 *   <li>{@code trained}：只装静态训练字典（64KB，与线上 dict_max_bytes 同量级）；</li>
 *   <li>{@code roll-256K}：滚动历史，每 256KB 轮换（当前线上值）；</li>
 *   <li>{@code roll-1M}：滚动历史，每 1MB 轮换（5.0.38 试过又回退的那个值）。</li>
 * </ul>
 */
class DictAblationTest {

    private static final int LEVEL = 3;
    private static final int DICT_BYTES = ZstdRollingDict.DICT_BYTES;
    private static final int[] BAND_LIMIT = {128, 4096};
    private static final long CORPUS_BYTES = 12L * 1024 * 1024;

    private enum Mode { NONE, TRAINED, ROLLING }

    private static final class Result {
        final long[] band = new long[3];
        long total;
        int count;
        int rotations;
    }

    @Test
    void ablation() throws IOException {
        Corpus corpus = build();
        byte[] trained = trainDict(corpus.packets);

        Result none = run(corpus.packets, Mode.NONE, 0, null);
        Result trainedR = run(corpus.packets, Mode.TRAINED, 0, trained);
        Result roll256 = run(corpus.packets, Mode.ROLLING, 256 * 1024, null);
        Result roll1m = run(corpus.packets, Mode.ROLLING, 1024 * 1024, null);

        StringBuilder sb = new StringBuilder();
        sb.append("==== dict ablation v2 (synthetic, per-chunk palettes) ====\n");
        sb.append("raw=").append(none.total).append("B, samples=").append(none.count)
                .append(", chunks=").append(corpus.chunks)
                .append(", level=").append(LEVEL).append(", dict=").append(DICT_BYTES).append("B\n\n");
        sb.append(String.format("%-11s %11s %11s %8s %11s %8s %11s %8s%n",
                "band", "none", "trained", "vs none", "roll256K", "vs none", "roll1M", "vs none"));
        String[] names = {"<128B", "128B-4KB", ">=4KB"};
        for (int i = 0; i < 3; i++) {
            sb.append(String.format("%-11s %11d %11d %7.1f%% %11d %7.1f%% %11d %7.1f%%%n",
                    names[i], none.band[i], trainedR.band[i], pct(none.band[i], trainedR.band[i]),
                    roll256.band[i], pct(none.band[i], roll256.band[i]),
                    roll1m.band[i], pct(none.band[i], roll1m.band[i])));
        }
        sb.append(String.format("%-11s %11d %11d %7.1f%% %11d %7.1f%% %11d %7.1f%%%n",
                "TOTAL", none.total, trainedR.total, pct(none.total, trainedR.total),
                roll256.total, pct(none.total, roll256.total),
                roll1m.total, pct(none.total, roll1m.total)));
        sb.append(String.format("%n滚动次数：256K=%d，1M=%d%n", roll256.rotations, roll1m.rotations));
        sb.append(String.format("trained 相对 roll256K 的字节比：%.1f%%（>0 = 静态字典更小）%n",
                (1.0 - (double) trainedR.total / roll256.total) * 100));
        sb.append(String.format("roll1M 相对 roll256K 的字节比：%.1f%%（>0 = 1MB 更小，<0 = 放宽阈值有代价）%n",
                (1.0 - (double) roll1m.total / roll256.total) * 100));

        System.out.print(sb);
        Path out = Path.of("build", "ablation-report.txt");
        Files.createDirectories(out.getParent());
        Files.writeString(out, sb.toString(), StandardCharsets.UTF_8);

        assertTrue(roll256.total < none.total * 0.95,
                "滚动历史必须带来明显收益，实际 " + pct(none.total, roll256.total) + "%");
    }

    private static double pct(long base, long value) {
        return base == 0 ? 0 : (1.0 - (double) value / base) * 100;
    }

    private Result run(List<byte[]> corpus, Mode mode, int rotateBytes, byte[] trainedDict) {
        Result r = new Result();
        r.count = corpus.size();
        ZstdCompressCtx ctx = new ZstdCompressCtx();
        ZstdDictCompress dictObj = null;
        try {
            ctx.setLevel(LEVEL);
            ctx.setMagicless(true);
            ctx.setContentSize(false);
            ctx.setDictID(false);
            if (mode == Mode.TRAINED) {
                dictObj = new ZstdDictCompress(trainedDict, LEVEL);
                ctx.loadDict(dictObj);
            }

            byte[] ring = new byte[DICT_BYTES];
            int fill = 0;
            int sinceRotate = 0;
            for (byte[] s : corpus) {
                if (mode == Mode.ROLLING && fill >= DICT_BYTES && sinceRotate >= rotateBytes) {
                    byte[] snapshot = new byte[DICT_BYTES];
                    System.arraycopy(ring, 0, snapshot, 0, DICT_BYTES);
                    ctx.loadDict(snapshot);
                    sinceRotate = 0;
                    r.rotations++;
                }
                int n = ctx.compress(s).length;
                r.band[bandOf(s.length)] += n;
                r.total += n;
                if (mode == Mode.ROLLING) {
                    feedRing(ring, fill, s);
                    fill = Math.min(DICT_BYTES, fill + s.length);
                    sinceRotate += s.length;
                }
            }
        } finally {
            ctx.close();
            if (dictObj != null) dictObj.close();
        }
        return r;
    }

    private static void feedRing(byte[] ring, int fill, byte[] s) {
        if (s.length >= DICT_BYTES) {
            System.arraycopy(s, s.length - DICT_BYTES, ring, 0, DICT_BYTES);
            return;
        }
        int overflow = fill + s.length - DICT_BYTES;
        if (overflow > 0) {
            System.arraycopy(ring, overflow, ring, 0, fill - overflow);
            System.arraycopy(s, 0, ring, fill - overflow, s.length);
        } else {
            System.arraycopy(s, 0, ring, fill, s.length);
        }
    }

    private static int bandOf(int len) {
        if (len < BAND_LIMIT[0]) return 0;
        return len < BAND_LIMIT[1] ? 1 : 2;
    }

    private static byte[] trainDict(List<byte[]> corpus) {
        long total = 0;
        for (byte[] s : corpus) total += s.length;
        ZstdDictTrainer tr = new ZstdDictTrainer((int) Math.min(total, 64L * 1024 * 1024), DICT_BYTES);
        for (byte[] s : corpus) {
            if (!tr.addSample(s)) break;
        }
        return tr.trainSamples();
    }

    /** 语料 + 它有多少个区块，仅用于报告。 */
    private static final class Corpus {
        final List<byte[]> packets = new ArrayList<>();
        int chunks;
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
        return c;
    }
}
