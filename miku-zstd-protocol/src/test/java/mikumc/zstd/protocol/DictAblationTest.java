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
 * 字典消融对照：<b>只跑数据，不做门禁</b>（唯一的断言是"滚动历史必须有效"，防用例空转）。
 *
 * <h2>它回答什么问题</h2>
 * <p>5.0.37 起"静态训练字典"与"滚动历史"会先后作用于同一条连接（前者 gen 0，后者 gen≥1）。
 * 线上日志只能告诉你"新候选打不过现有字典"（{@code insufficient improvement: -1.3%}），
 * 回答不了"在滚动历史已经在场的情况下，静态字典还剩多少价值"。本对照把四种组合放在
 * <b>同一份语料</b>上跑，直接给出每档字节数的差。</p>
 *
 * <h2>为什么用合成语料</h2>
 * <p>仓库里不能放真实玩家流量，而字典收益高度依赖"重复结构"。所以这里按 MC 的形态人工造：
 * 小包（聊天/计分板/实体移动，40~120B）、中包（方块更新/物品，200~3KB）、
 * 大包（区块数据，8~24KB，由重复块拼成）。<b>结论只能用于相对比较</b>，
 * 绝对值不要当线上指标引用。</p>
 *
 * <h2>四个模式</h2>
 * <ul>
 *   <li>{@code none}：不装字典（5.0.35 之前的形态）；</li>
 *   <li>{@code trained}：只装静态训练字典（64KB，与线上 dict_max_bytes 同量级）；</li>
 *   <li>{@code roll-1M}：滚动历史，每 1MB 轮换（5.0.38 起）；</li>
 *   <li>{@code roll-256K}：滚动历史，每 256KB 轮换（5.0.37）—— 用来量化轮换阈值放宽的代价。</li>
 * </ul>
 */
class DictAblationTest {

    private static final int LEVEL = 3;
    private static final int DICT_BYTES = ZstdRollingDict.DICT_BYTES;
    private static final int[] BAND_LIMIT = {128, 4096}; // <128B / 128B~4KB / >=4KB

    private enum Mode { NONE, TRAINED, ROLLING }

    /** 每个模式：3 档字节数 + 总字节数 + 样本数。 */
    private static final class Result {
        final long[] band = new long[3];
        long total;
        int count;
    }

    @Test
    void ablation() throws IOException {
        List<byte[]> corpus = corpus();
        byte[] trained = trainDict(corpus);

        Result none = run(corpus, Mode.NONE, 0, null);
        Result trainedR = run(corpus, Mode.TRAINED, 0, trained);
        Result roll1m = run(corpus, Mode.ROLLING, 1024 * 1024, null);
        Result roll256k = run(corpus, Mode.ROLLING, 256 * 1024, null);

        StringBuilder sb = new StringBuilder();
        sb.append("==== dict ablation (synthetic corpus, level=").append(LEVEL)
                .append(", samples=").append(none.count)
                .append(", raw=").append(none.total).append("B) ====\n");
        sb.append(String.format("%-10s %12s %12s %10s %12s %10s %12s %10s%n",
                "band", "none", "trained", "vs none", "roll-1M", "vs none", "roll-256K", "vs none"));
        String[] names = {"<128B", "128B-4KB", ">=4KB"};
        for (int i = 0; i < 3; i++) {
            sb.append(String.format("%-10s %12d %12d %9.1f%% %12d %9.1f%% %12d %9.1f%%%n",
                    names[i], none.band[i], trainedR.band[i], pct(none.band[i], trainedR.band[i]),
                    roll1m.band[i], pct(none.band[i], roll1m.band[i]),
                    roll256k.band[i], pct(none.band[i], roll256k.band[i])));
        }
        sb.append(String.format("%-10s %12d %12d %9.1f%% %12d %9.1f%% %12d %9.1f%%%n",
                "TOTAL", none.total, trainedR.total, pct(none.total, trainedR.total),
                roll1m.total, pct(none.total, roll1m.total),
                roll256k.total, pct(none.total, roll256k.total)));

        // 关键差值：滚动历史在场时，静态字典还剩多少
        sb.append(String.format("%n静态字典在滚动历史之上的增量（负=更差）：%.2f%%%n",
                (1.0 - (double) roll256k.total / trainedR.total) * 100));
        sb.append(String.format("轮换阈值 256KB→1MB 的代价：%.2f%%%n",
                (1.0 - (double) roll1m.total / roll256k.total) * 100));

        System.out.print(sb);
        Path out = Path.of("build", "ablation-report.txt");
        Files.createDirectories(out.getParent());
        Files.writeString(out, sb.toString(), StandardCharsets.UTF_8);

        // 唯一的门禁：滚动历史必须真的省字节（否则说明用例/实现有问题，而不是"字典没用"）
        assertTrue(roll1m.total < none.total * 0.95,
                "滚动历史必须带来明显收益，实际 " + pct(none.total, roll1m.total) + "%");
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
                if (mode == Mode.ROLLING) {
                    // 先按"上一帧之后的历史"压，再把这帧喂进滑窗（与线上顺序一致：
                    // 帧用旧历史压完才喂入自己）
                    if (fill >= DICT_BYTES && sinceRotate >= rotateBytes) {
                        byte[] snapshot = new byte[DICT_BYTES];
                        System.arraycopy(ring, 0, snapshot, 0, DICT_BYTES);
                        ctx.loadDict(snapshot);
                        sinceRotate = 0;
                    }
                    int n = ctx.compress(s).length;
                    r.band[bandOf(s.length)] += n;
                    r.total += n;
                    feedRing(ring, fill, s);
                    fill = Math.min(DICT_BYTES, fill + s.length);
                    sinceRotate += s.length;
                } else {
                    int n = ctx.compress(s).length;
                    r.band[bandOf(s.length)] += n;
                    r.total += n;
                }
            }
        } finally {
            if (dictObj != null) dictObj.close();
            ctx.close();
        }
        return r;
    }

    /** 滑窗按"最近 DICT_BYTES 字节"滚动（与 ZstdRollingDict 同语义）。 */
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

    /**
     * 造语料：一块"世界"模板 + 三类包。总量约 4MB，足够让 1MB 轮换发生 3~4 次。
     */
    private static List<byte[]> corpus() {
        Random rnd = new Random(20261101L);
        // 模板池：模拟重复度高的内容（区块/计分板/物品表）
        byte[][] blocks = new byte[64][];
        for (int i = 0; i < blocks.length; i++) {
            blocks[i] = new byte[512];
            new Random(7000 + i).nextBytes(blocks[i]);
        }
        List<byte[]> out = new ArrayList<>();
        long bytes = 0;
        while (bytes < 4L * 1024 * 1024) {
            int roll = rnd.nextInt(100);
            byte[] pkt;
            if (roll < 72) {
                pkt = small(rnd, blocks);           // 小包：数量多
            } else if (roll < 96) {
                pkt = medium(rnd, blocks);          // 中包
            } else {
                pkt = large(rnd, blocks);           // 大包：字节多
            }
            out.add(pkt);
            bytes += pkt.length;
        }
        return out;
    }

    private static byte[] small(Random rnd, byte[][] blocks) {
        int len = 40 + rnd.nextInt(80);
        byte[] b = new byte[len];
        b[0] = (byte) (0x20 + rnd.nextInt(8));
        byte[] src = blocks[rnd.nextInt(blocks.length)];
        int off = rnd.nextInt(src.length - len + 1);
        System.arraycopy(src, off, b, 1, len - 1);
        return b;
    }

    private static byte[] medium(Random rnd, byte[][] blocks) {
        int len = 200 + rnd.nextInt(2800);
        byte[] b = new byte[len];
        b[0] = (byte) (0x30 + rnd.nextInt(8));
        int pos = 1;
        while (pos < len) {
            byte[] src = blocks[rnd.nextInt(blocks.length)];
            int n = Math.min(src.length, len - pos);
            System.arraycopy(src, 0, b, pos, n);
            pos += n;
        }
        // 少量扰动，模拟"同一区域但内容有变化"
        for (int i = 0; i < len / 64; i++) b[1 + rnd.nextInt(len - 1)] ^= 0x5A;
        return b;
    }

    private static byte[] large(Random rnd, byte[][] blocks) {
        int len = 8192 + rnd.nextInt(16384);
        byte[] b = new byte[len];
        b[0] = (byte) 0x25;
        int pos = 1;
        while (pos < len) {
            byte[] src = blocks[rnd.nextInt(blocks.length)];
            int n = Math.min(src.length, len - pos);
            System.arraycopy(src, 0, b, pos, n);
            pos += n;
        }
        return b;
    }
}
