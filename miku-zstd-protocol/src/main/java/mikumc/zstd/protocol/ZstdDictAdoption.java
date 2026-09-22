package mikumc.zstd.protocol;

/**
 * 字典评估与采纳判定（<b>两端共用</b>）。
 *
 * <h2>为什么必须共用</h2>
 * {@link #shouldAdoptDict} 是"字典到底会不会被启用"的<b>唯一开关</b>。改错一个比较符号，
 * 要么字典永远无法启用（整条训练机制变成死代码），要么频繁换字典导致连接侧反复重建；
 * 而这类错误<b>不会报错</b>，只会让压缩率悄悄变差。这段逻辑原来在 Velocity 与 Paper
 * 各有一份——当前判定一致，但任何一端被单独修改，就会造成"两端采纳行为不同"，
 * 属于最难发现的一类漂移（本项目已因"一端漏抄一行"踩过两次坑）。
 *
 * <h2>分档的意义</h2>
 * 字典收益高度集中在<b>小包</b>（无字典时小包压缩后反而膨胀，字典是让小包可压缩的前提），
 * 但小包只占总字节的百分之几 —— 所以"按总字节加权"的 overall 改进往往只有 1~3%，
 * 单看这个数会低估字典价值。因此评估同时给出三档改进率，供日志诊断。
 */
public final class ZstdDictAdoption {

    /** 小包边界（字节） */
    public static final int SMALL_BELOW = 128;
    /** 中包边界（字节） */
    public static final int MID_BELOW = 4096;
    /** 分档数量：小 / 中 / 大 */
    public static final int BANDS = 3;

    private ZstdDictAdoption() {
    }

    /** 包长 → 分档下标：0=小包(<128B)、1=中包(128B~4KB)、2=大包(≥4KB)。 */
    public static int bandOf(int packetLength) {
        return packetLength < SMALL_BELOW ? 0 : (packetLength < MID_BELOW ? 1 : 2);
    }

    /** 某一档的改进率：{@code 1 - new/old}（旧字节为 0 时返回 0）。 */
    public static double bandRatio(long oldBytes, long newBytes) {
        if (oldBytes == 0) {
            return 0;
        }
        return 1.0 - (double) newBytes / oldBytes;
    }

    /** 百分比格式化（输入是比例，如 0.176 → "17.6%"）。 */
    public static String pct(double value) {
        return String.format("%.1f%%", value * 100);
    }

    /**
     * 字典采纳判定。
     *
     * <h2>为什么首部字典不能套用替换阈值</h2>
     * <p>实测（{@code 开发期探针 MemProbe9/10}）：</p>
     * <ul>
     *   <li>无字典时小包压缩后<b>膨胀</b>（&lt;48B 为 102~113%），只能全部直存 ——
     *       字典是让小包压缩变得可行的<b>前提</b>，不是锦上添花；</li>
     *   <li>但字典收益集中在小包，而小包只占总字节的百分之几，
     *       <b>按总字节加权后整体改进往往只有 1~3%</b>。</li>
     * </ul>
     * <p>若首部字典也要求达到替换阈值（旧默认 3%），在多数真实服务器上
     * <b>字典永远不会被采纳，整条字典机制等于死代码</b>——这正是 3.1.7 修掉的问题。</p>
     *
     * <p>替换字典仍按阈值把关：避免频繁换字典导致连接侧反复重建字典对象。</p>
     *
     * @param firstDict   当前还没有字典
     * @param improvement 整体改进率（首部字典时即"相对无字典"）
     * @param threshold   替换字典所需的改进率（配置项）
     */
    public static boolean shouldAdoptDict(boolean firstDict, double improvement, double threshold) {
        return firstDict ? improvement > 0 : improvement >= threshold;
    }

    /** 评估结果：整体改进率 + 三个包长档的改进率。 */
    public static final class EvalResult {
        public final double overall;
        /** &lt;128B */
        public final double small;
        /** 128B ~ 4KB */
        public final double mid;
        /** ≥4KB */
        public final double large;

        public EvalResult(double overall, double small, double mid, double large) {
            this.overall = overall;
            this.small = small;
            this.mid = mid;
            this.large = large;
        }

        /** 无有效样本 / 评估失败时的占位值。 */
        public static EvalResult none() {
            return new EvalResult(0, 0, 0, 0);
        }

        /** 一行分档摘要，用于日志。 */
        public String bandSummary() {
            return String.format("小包<%dB %s / 中包%dB-4KB %s / 大包≥4KB %s",
                    SMALL_BELOW, pct(small), SMALL_BELOW, pct(mid), pct(large));
        }
    }
}
