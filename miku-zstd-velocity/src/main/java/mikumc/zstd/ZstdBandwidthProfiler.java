package mikumc.zstd;

import mikumc.zstd.protocol.ZstdBossBarFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/**
 * 出站带宽剖析（全服聚合，默认 60 秒一行）。
 *
 * <h2>为什么需要它</h2>
 * <p>"怎么继续降低带宽"这个问题<b>必须用真实流量分布来回答</b>，因为不同包长区间的
 * 优化手段完全不同。实测（合成流量，见 <code>tools/memprobe/MemProbe7~9</code>）：</p>
 * <ul>
 *   <li>小包（&lt;48B）：无字典时压缩后<b>膨胀到 105%</b>（全部直存、纯浪费 CPU），
 *       有字典后降到 80%——<b>字典的价值全在这里</b>；</li>
 *   <li>大包（&gt;1KB）：字典几乎无用（0.8%），且压缩等级 9/15/19 结果几乎相同；</li>
 *   <li>因此"下一步该优化什么"取决于你自己的字节分布。</li>
 * </ul>
 *
 * <p>本剖析按包长分档统计<b>原始字节 / 线路字节 / 直存帧数</b>，直接告诉运维：
 * 字节都花在哪个区间、该区间压缩率如何、值不值得继续投入。</p>
 *
 * <p>代价：每包一次数组下标 + {@link LongAdder#add}，可忽略。</p>
 */
public final class ZstdBandwidthProfiler {

    private static final Logger LOGGER = LoggerFactory.getLogger("miku-zstd");

    /** 分档上界（字节）：&lt;128B、&lt;512B、&lt;4KB、≥4KB */
    private static final int[] BAND_LIMIT = {128, 512, 4096, Integer.MAX_VALUE};
    private static final String[] BAND_NAME = {"小包 <128B", "中包 128B-512B", "大包 512B-4KB", "巨包 ≥4KB"};

    private static final LongAdder[] RAW = new LongAdder[BAND_LIMIT.length];
    private static final LongAdder[] WIRE = new LongAdder[BAND_LIMIT.length];
    private static final LongAdder[] FRAMES = new LongAdder[BAND_LIMIT.length];
    private static final LongAdder[] STORED = new LongAdder[BAND_LIMIT.length];
    private static final LongAdder TOTAL_RAW = new LongAdder();
    private static final LongAdder TOTAL_WIRE = new LongAdder();

    private static volatile boolean enabled;
    private static ScheduledExecutorService scheduler;

    static {
        for (int i = 0; i < BAND_LIMIT.length; i++) {
            RAW[i] = new LongAdder();
            WIRE[i] = new LongAdder();
            FRAMES[i] = new LongAdder();
            STORED[i] = new LongAdder();
        }
    }

    private ZstdBandwidthProfiler() {
    }

    /**
     * 启动周期上报（由插件初始化时调用；debug 关闭时不启动，零开销）。
     *
     * <p><b>幂等</b>：{@code /mikuzstd reload} 会再调一次——若不判重，每 reload 一次就会
     * 多出一个上报线程，带宽剖析越看越花。</p>
     */
    public static synchronized void start(boolean debugEnabled, int intervalSeconds) {
        enabled = debugEnabled;
        if (!debugEnabled) return;
        if (scheduler != null) return; // 已在运行
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "miku-zstd-bwprof");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleWithFixedDelay(ZstdBandwidthProfiler::report,
                intervalSeconds, intervalSeconds, TimeUnit.SECONDS);
        LOGGER.info("[Zstd] 带宽剖析已启用，每 {}s 输出一次（关闭 logging.debug 可停用）", intervalSeconds);
    }

    public static synchronized void shutdown() {
        if (scheduler != null) {
            scheduler.shutdown();
            scheduler = null;
        }
    }

    /** 编码器热路径调用：记录一个出站包。 */
    public static void record(int rawSize, int wireSize, boolean compressed) {
        if (!enabled) return;
        int band = bandOf(rawSize);
        RAW[band].add(rawSize);
        WIRE[band].add(wireSize);
        FRAMES[band].increment();
        if (!compressed) STORED[band].increment();
        TOTAL_RAW.add(rawSize);
        TOTAL_WIRE.add(wireSize);
    }

    private static int bandOf(int size) {
        for (int i = 0; i < BAND_LIMIT.length; i++) {
            if (size < BAND_LIMIT[i]) return i;
        }
        return BAND_LIMIT.length - 1;
    }

    private static void report() {
        try {
            long totalRaw = TOTAL_RAW.sumThenReset();
            long totalWire = TOTAL_WIRE.sumThenReset();
            if (totalRaw == 0) return;

            StringBuilder sb = new StringBuilder();
            sb.append(String.format("出站 %s → %s（压缩率 %.1f%%，省 %.1f%%）",
                    fmt(totalRaw), fmt(totalWire), 100.0 * totalWire / totalRaw,
                    100.0 - 100.0 * totalWire / totalRaw));
            for (int i = 0; i < BAND_LIMIT.length; i++) {
                long raw = RAW[i].sumThenReset();
                long wire = WIRE[i].sumThenReset();
                long frames = FRAMES[i].sumThenReset();
                long stored = STORED[i].sumThenReset();
                if (raw == 0) continue;
                sb.append(String.format("%n    %-14s 原始 %8s (%4.1f%%)  线路 %8s  压缩率 %5.1f%%  帧 %6d  直存 %6d",
                        BAND_NAME[i], fmt(raw), 100.0 * raw / totalRaw, fmt(wire),
                        100.0 * wire / raw, frames, stored));
            }
            LOGGER.info("[Zstd] 带宽剖析（本周期）：{}", sb);
        } catch (Throwable t) {
            LOGGER.warn("[Zstd] 带宽剖析输出失败", t);
        }
    }

    /** 字节格式化：统一走 {@link ZstdBossBarFormat#fmtBytes}，避免项目里两套 KB/MB 口径。 */
    private static String fmt(long bytes) {
        return ZstdBossBarFormat.fmtBytes(bytes);
    }
}
