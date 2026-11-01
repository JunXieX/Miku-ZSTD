package cn.miku.zstd;

import java.util.concurrent.atomic.LongAdder;

/** 下行 zstd 统计（HUD 数据源）；上行完全由原版处理，不计入 zstd 统计。 */
public final class ZstdStatsData {

    private static final int RING = 10;
    private static final LongAdder ACCUM_RX_IN = new LongAdder();
    private static final LongAdder ACCUM_RX_OUT = new LongAdder();
    private static final long[] RING_RX_IN = new long[RING];
    private static final long[] RING_RX_OUT = new long[RING];

    private static long lastCaptureTime;
    private static long lastRxIn;
    private static long lastRxOut;
    private static int samples;
    private static int ringIdx;
    private static int ringCount;

    private ZstdStatsData() {
    }

    /** 入站：in = 收到的帧负载字节数，out = 解压后字节数。 */
    public static void addRxBatch(int inBytes, int outBytes) {
        ACCUM_RX_IN.add(inBytes);
        ACCUM_RX_OUT.add(outBytes);
    }

    /** 数据不足或无下行流量时返回 null；上行只显示原版标记。 */
    public static synchronized String captureTabStats(int intervalSec) {
        long now = System.currentTimeMillis();
        boolean first = lastCaptureTime == 0;
        long elapsed = first ? 0 : now - lastCaptureTime;
        long curRxIn = ACCUM_RX_IN.sum();
        long curRxOut = ACCUM_RX_OUT.sum();
        long dRxIn = curRxIn - lastRxIn;
        long dRxOut = curRxOut - lastRxOut;

        samples++;
        lastCaptureTime = now;
        lastRxIn = curRxIn;
        lastRxOut = curRxOut;

        // 首次采样或 HUD 长时间关闭后只重建基线，避免旧流量形成速率尖峰。
        if (first || elapsed > intervalSec * 3000L) {
            java.util.Arrays.fill(RING_RX_IN, 0);
            java.util.Arrays.fill(RING_RX_OUT, 0);
            ringIdx = 0;
            ringCount = 0;
            return null;
        }

        double sec = elapsed / 1000.0;
        if (sec <= 0) return null;
        RING_RX_IN[ringIdx] = (long) (dRxIn / sec);
        RING_RX_OUT[ringIdx] = (long) (dRxOut / sec);
        ringIdx = (ringIdx + 1) % RING;
        if (ringCount < RING) ringCount++;
        if (samples < 3) return null;

        long avgRxIn = avg(RING_RX_IN);
        long avgRxOut = avg(RING_RX_OUT);
        if (avgRxIn == 0) return null;
        double rxRatio = avgRxOut > 0 ? 100.0 * avgRxIn / avgRxOut : 100.0;
        return String.format("\u00a7lTX:\u00a7r Vanilla  |  \u00a7lRX:\u00a7r %s \u2192 %s %s",
                fmtBytes(avgRxOut), fmtBytes(avgRxIn), colorRatio(rxRatio));
    }

    private static String fmtBytes(long bps) {
        return String.format("%7s", mikumc.zstd.protocol.ZstdBossBarFormat.rate(bps));
    }

    public static synchronized String debugSummary() {
        return String.format("samples=%d upstream=vanilla rxIn=%d rxOut=%d",
                samples, ACCUM_RX_IN.sum(), ACCUM_RX_OUT.sum());
    }

    private static String colorRatio(double ratio) {
        String value = String.format("%5.1f%%", ratio);
        if (ratio < 30.0) return "\u00a7a" + value;
        if (ratio < 80.0) return "\u00a7e" + value;
        return "\u00a7c" + value;
    }

    private static long avg(long[] ring) {
        long sum = 0;
        for (int i = 0; i < ringCount; i++) sum += ring[i];
        return ringCount > 0 ? sum / ringCount : 0;
    }
}
