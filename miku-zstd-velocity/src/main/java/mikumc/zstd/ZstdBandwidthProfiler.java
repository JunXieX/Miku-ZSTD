package mikumc.zstd;

import mikumc.zstd.protocol.ZstdBossBarFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/**
 * 下行压缩批次带宽剖析（全服聚合，默认 60 秒一行）。
 *
 * <p>按完成批次的原始流字节大小分档，记录原始字节与包含 Minecraft 外层长度的线路字节。
 * 一个批次可输出多个传输帧，因此分档与计数不代表单个游戏包或传输帧的大小分布。</p>
 *
 * <p>压缩效果以当前实例的实际流量为准；同一连接的批次共享连续流历史。</p>
 */
public final class ZstdBandwidthProfiler {

    private static final Logger LOGGER = LoggerFactory.getLogger("miku-zstd");

    /** 原始批次大小分档上界（字节）：&lt;128B、&lt;512B、&lt;4KB、≥4KB */
    private static final int[] BAND_LIMIT = {128, 512, 4096, Integer.MAX_VALUE};
    private static final String[] BAND_NAME = {"小批 <128B", "中批 128B-512B", "大批 512B-4KB", "巨批 ≥4KB"};

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
     *
     * <p>⚠️ 反向也要处理：{@code reload} 把 {@code logging.debug} 从 true 改成 false 时，
     * 必须<b>停掉</b>已在运行的上报线程。旧实现只把 {@code enabled} 置假就返回
     * （{@code if (!debugEnabled) return;}），线程仍按周期空转——统计虽不再累加
     *（{@link #record} 会早退），但"关了开关线程还在"本身就是一个泄漏点。</p>
     */
    public static synchronized void start(boolean debugEnabled, int intervalSeconds) {
        enabled = debugEnabled;
        if (!debugEnabled) {
            shutdown(); // reload 关闭 debug：连同线程一起收掉
            return;
        }
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
        // ⚠️ 一并关掉热路径开关：shutdown 之后 {@link #record} 仍会被编码器调用，
        // 不置假就等于"停了线程却还在每个包上累加"。
        enabled = false;
        if (scheduler != null) {
            scheduler.shutdown();
            scheduler = null;
        }
    }

    /** 编码器在一个下行批次完成时聚合调用。 */
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
                sb.append(String.format("%n    %-14s 原始 %8s (%4.1f%%)  线路 %8s  压缩率 %5.1f%%  批 %6d  直存 %6d",
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
