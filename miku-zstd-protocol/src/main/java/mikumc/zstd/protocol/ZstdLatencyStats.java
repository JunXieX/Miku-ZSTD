package mikumc.zstd.protocol;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * 压缩链路分段耗时的有界直方图（三端共用，只统计，不参与任何决策）。
 *
 * <h2>解决什么问题</h2>
 * 之前池里只有"worker 开始执行到结束"的平均值与最大值。可是玩家感觉到的卡顿可能来自
 * 完全不同的三处：任务在队列里等线程、worker 真的在压缩、或者压缩完了却迟迟排不上
 * event loop 回调。只有一个平均值时无法区分它们，调配置就只能猜。
 *
 * <h2>为什么用直方图而不是累加平均</h2>
 * 尾延迟是分位数问题，不是均值问题：一次 200 毫秒的尖峰会被 999 次 0.2 毫秒抹平，
 * 但玩家只记得那次尖峰。这里按 {@link Stage} 各保留一份固定 512 桶的对数直方图，
 * 每个桶宽度约为下界的 1/8，因此分位点估计的相对误差不超过 12.5%。
 *
 * <h2>开销口径</h2>
 * 每次记录 = 一次 {@link AtomicLongArray#incrementAndGet(int)} + 一次原子最大值比较，
 * 都在批（batch）边界上采样，不是逐包。批本身已经包含数十个包与一次原生压缩调用，
 * 因此这点开销可以被忽略；记录本身也从不分配对象。
 */
public final class ZstdLatencyStats {

    /** 分段计时点；顺序即报告里的输出顺序。 */
    public enum Stage {
        /** 提交压缩任务 → worker 真正开始执行（排队等待）。 */
        QUEUE,
        /** worker 执行压缩本身。 */
        SERVICE,
        /** worker 结束 → event loop 回调真正开始执行。 */
        EVENT_LOOP,
        /** 开始交付运输帧 → 全部真实 write promise 完成。 */
        WRITE
    }

    /** 每个 2 倍区间再切 8 份，桶宽约为下界的 12.5%。 */
    private static final int SUB_BUCKETS = 8;
    private static final int BUCKETS = 64 * SUB_BUCKETS;
    private static final Stage[] STAGES = Stage.values();

    private static final AtomicLongArray[] HISTOGRAMS = new AtomicLongArray[STAGES.length];
    private static final AtomicLong[] MAX_NANOS = new AtomicLong[STAGES.length];

    static {
        for (int stage = 0; stage < STAGES.length; stage++) {
            HISTOGRAMS[stage] = new AtomicLongArray(BUCKETS);
            MAX_NANOS[stage] = new AtomicLong();
        }
    }

    private ZstdLatencyStats() {
    }

    /** 记录一次分段耗时；负数视为无效样本直接忽略。 */
    public static void record(Stage stage, long nanos) {
        if (stage == null || nanos < 0) {
            return;
        }
        HISTOGRAMS[stage.ordinal()].incrementAndGet(bucket(nanos));
        MAX_NANOS[stage.ordinal()].accumulateAndGet(nanos, Math::max);
    }

    /** 该阶段的样本数（0 表示还没有任何一次批处理走完这个阶段）。 */
    public static long sampleCount(Stage stage) {
        AtomicLongArray histogram = HISTOGRAMS[stage.ordinal()];
        long total = 0;
        for (int index = 0; index < BUCKETS; index++) {
            total += histogram.get(index);
        }
        return total;
    }

    /**
     * 分位点估计（返回毫秒）；未记录的阶段返回 0。
     *
     * <p>返回值取命中的那个桶的<b>上界</b>，因此估计略偏保守：真实值落在该桶内就不会被低估。</p>
     */
    public static double percentileMillis(Stage stage, double quantile) {
        if (quantile <= 0 || quantile > 1) {
            throw new IllegalArgumentException("quantile must be in (0,1]: " + quantile);
        }
        AtomicLongArray histogram = HISTOGRAMS[stage.ordinal()];
        long total = 0;
        for (int index = 0; index < BUCKETS; index++) {
            total += histogram.get(index);
        }
        if (total == 0) {
            return 0.0;
        }
        long target = Math.max(1L, (long) Math.ceil(quantile * total));
        long seen = 0;
        for (int index = 0; index < BUCKETS; index++) {
            seen += histogram.get(index);
            if (seen >= target) {
                return bucketUpperNanos(index) / 1_000_000.0;
            }
        }
        return bucketUpperNanos(BUCKETS - 1) / 1_000_000.0;
    }

    /** 该阶段观测到的最大值（毫秒）；未记录时为 0。 */
    public static double maxMillis(Stage stage) {
        return MAX_NANOS[stage.ordinal()].get() / 1_000_000.0;
    }

    /** 清空全部样本（重载/复测时使用）。 */
    public static void reset() {
        for (int stage = 0; stage < STAGES.length; stage++) {
            AtomicLongArray histogram = HISTOGRAMS[stage];
            for (int index = 0; index < BUCKETS; index++) {
                histogram.set(index, 0L);
            }
            MAX_NANOS[stage].set(0L);
        }
    }

    /**
     * 一行机器可读摘要：{@code lat: queue_p50=.. queue_p99=.. ... samples=..}。
     *
     * <p>字段名与顺序固定，供外部采样脚本解析；数值单位统一为毫秒，保留三位小数。
     * 各阶段的分位点<b>不能</b>互相推导，也不等同于端到端网络延迟。</p>
     */
    public static String machineLine() {
        return String.format(java.util.Locale.ROOT,
                "lat: queue_p50=%.3f queue_p99=%.3f service_p50=%.3f service_p99=%.3f"
                        + " eventloop_p50=%.3f eventloop_p99=%.3f write_p50=%.3f write_p99=%.3f"
                        + " samples=%d",
                percentileMillis(Stage.QUEUE, 0.50), percentileMillis(Stage.QUEUE, 0.99),
                percentileMillis(Stage.SERVICE, 0.50), percentileMillis(Stage.SERVICE, 0.99),
                percentileMillis(Stage.EVENT_LOOP, 0.50), percentileMillis(Stage.EVENT_LOOP, 0.99),
                percentileMillis(Stage.WRITE, 0.50), percentileMillis(Stage.WRITE, 0.99),
                sampleCount(Stage.SERVICE));
    }

    /** 纳秒值映射到桶下标：先取 floor(log2)，再按区间内位置切 8 份。 */
    private static int bucket(long nanos) {
        if (nanos <= 1) {
            return 0;
        }
        int shift = 63 - Long.numberOfLeadingZeros(nanos);
        long base = 1L << shift;
        int sub = (int) ((nanos - base) * SUB_BUCKETS / base);
        if (sub >= SUB_BUCKETS) {
            sub = SUB_BUCKETS - 1;
        }
        return Math.min(shift * SUB_BUCKETS + sub, BUCKETS - 1);
    }

    /** 桶的上界（纳秒）。 */
    private static double bucketUpperNanos(int index) {
        int shift = index / SUB_BUCKETS;
        int sub = index % SUB_BUCKETS;
        double base = Math.scalb(1.0, shift);
        return base * (1.0 + (sub + 1) / (double) SUB_BUCKETS);
    }
}
