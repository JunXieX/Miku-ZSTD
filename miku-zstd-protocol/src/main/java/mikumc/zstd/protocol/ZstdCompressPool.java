package mikumc.zstd.protocol;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * 压缩线程池与运行时统计（三端共用）。
 *
 * <h2>为什么池和统计放在一起</h2>
 * 池的规模、饱和程度、单次压缩耗时，这三者是同一件事的三个面——
 * 而且"该不该降压缩等级 / 该不该加核"这个决策，只能靠它们来回答。
 * 以前这些数字全在系统里，但看不到，只能凭感觉调配置。
 *
 * <h2>统计口径</h2>
 * <ul>
 *   <li><b>活跃</b>：正在执行的任务数（≥2 说明池确实在并行工作）；</li>
 *   <li><b>队列</b>：等待执行的任务数。<b>持续 >0 是 CPU 不足的直接信号</b>——
 *       意味着压缩已经跟不上流量，此时该降 level 或加核，而不是继续调窗口大小；</li>
 *   <li><b>平均 / 最大耗时</b>：最大耗时才是卡顿的来源（平均值会把它抹平）；</li>
 *   <li><b>慢任务数</b>：超过 {@link #slowThresholdMs} 的次数。</li>
 * </ul>
 */
public final class ZstdCompressPool {

    /** 默认保留一个核给服务端主逻辑（tick / 区块 / 实体） */
    private static final int RESERVED_CORES = 1;

    private static volatile ExecutorService pool;
    private static volatile int threads;
    private static volatile long slowThresholdMs = 5;

    private static final AtomicInteger ACTIVE = new AtomicInteger();
    private static final LongAdder TOTAL_TASKS = new LongAdder();
    private static final LongAdder TOTAL_NANOS = new LongAdder();
    private static final AtomicLong SLOW_TASKS = new AtomicLong();
    private static final AtomicLong MAX_NANOS = new AtomicLong();
    private static final AtomicLong REJECTED = new AtomicLong();

    private ZstdCompressPool() {
    }

    /**
     * 初始化池（幂等，只在首次调用时生效）。
     *
     * @param configuredThreads 配置的线程数；{@code <= 0} 表示自动
     *                          （CPU 核心数 − {@value #RESERVED_CORES}，至少 1）
     * @param slowMs            慢任务阈值（毫秒）
     */
    public static synchronized void init(int configuredThreads, long slowMs) {
        if (pool != null) {
            return;
        }
        int cores = Math.max(1, Runtime.getRuntime().availableProcessors());
        int n = configuredThreads > 0
                ? configuredThreads
                : Math.max(1, cores - RESERVED_CORES);
        ZstdCompressPool.threads = n;
        if (slowMs > 0) {
            ZstdCompressPool.slowThresholdMs = slowMs;
        }
        AtomicInteger seq = new AtomicInteger();
        ZstdCompressPool.pool = Executors.newFixedThreadPool(n, r -> {
            Thread t = new Thread(r, "miku-zstd-compress-" + seq.incrementAndGet());
            t.setDaemon(true);
            return t;
        });
    }

    /** 提交一个压缩任务（未初始化时按默认值自动初始化）。 */
    public static void execute(Runnable task) {
        if (pool == null) {
            init(0, 0);
        }
        ACTIVE.incrementAndGet();
        TOTAL_TASKS.increment();
        try {
            pool.execute(() -> {
                long t0 = System.nanoTime();
                try {
                    task.run();
                } finally {
                    long dt = System.nanoTime() - t0;
                    TOTAL_NANOS.add(dt);
                    MAX_NANOS.accumulateAndGet(dt, Math::max);
                    if (dt >= slowThresholdMs * 1_000_000L) {
                        SLOW_TASKS.incrementAndGet();
                    }
                    ACTIVE.decrementAndGet();
                }
            });
        } catch (Throwable t) {
            // 池被关闭等极端情况：把计数还原，避免 ACTIVE 永久偏高
            ACTIVE.decrementAndGet();
            REJECTED.incrementAndGet();
            throw t;
        }
    }

    public static int threads() {
        return threads;
    }

    public static int active() {
        return ACTIVE.get();
    }

    /** 等待执行的任务数（持续 >0 说明压缩跟不上流量）。 */
    public static int queueDepth() {
        ExecutorService p = pool;
        if (p instanceof ThreadPoolExecutor tpe) {
            return tpe.getQueue().size();
        }
        return 0;
    }

    public static long totalTasks() {
        return TOTAL_TASKS.sum();
    }

    public static long slowTasks() {
        return SLOW_TASKS.get();
    }

    public static long maxNanos() {
        return MAX_NANOS.get();
    }

    /** 一行摘要，供命令层直接输出。 */
    public static String summary() {
        long total = TOTAL_TASKS.sum();
        long avgNs = total == 0 ? 0 : TOTAL_NANOS.sum() / total;
        return String.format(
                "线程 %d | 活跃 %d | 队列 %d | 累计 %,d 次 | 平均 %.2fms | 最大 %.2fms | 慢(>%dms) %d 次",
                threads, ACTIVE.get(), queueDepth(), total,
                avgNs / 1_000_000.0, MAX_NANOS.get() / 1_000_000.0,
                slowThresholdMs, SLOW_TASKS.get());
    }

    /** 队列积压是否值得提醒（命令层可据此给出调整建议）。 */
    public static boolean backlogged() {
        return queueDepth() > threads;
    }
}
