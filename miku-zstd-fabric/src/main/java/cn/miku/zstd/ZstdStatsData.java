package cn.miku.zstd;

import java.util.concurrent.atomic.LongAdder;

/**
 * TX/RX 压缩统计（HUD 数据源）。
 *
 * <h2>3.1.0 变更</h2>
 * <ul>
 *   <li>累计量改 {@link LongAdder}：旧实现每个包都进 {@code synchronized (ZstdStatsData.class)}，
 *       而写入发生在 netty 的 I/O 线程上，锁竞争会直接落到数据路径上；</li>
 *   <li>删除从未被调用、也从未被读取的 {@code updateTx/updateRx} 与
 *       {@code *PerSec} 字段（HUD 只用 {@link #captureTabStats}）；</li>
 *   <li>写入方（编码器/解码器）在 3.0.0 重构中被整体替换掉，累计量恒为 0 →
 *       HUD 永远空白；现已接回。</li>
 * </ul>
 */
public final class ZstdStatsData {

    /** 速率平滑窗口（采样次数） */
    private static final int RING = 10;

    private static final LongAdder ACCUM_TX_RAW = new LongAdder();
    private static final LongAdder ACCUM_TX_WIRE = new LongAdder();
    private static final LongAdder ACCUM_RX_IN = new LongAdder();
    private static final LongAdder ACCUM_RX_OUT = new LongAdder();

    private static final long[] RING_TX_RAW = new long[RING];
    private static final long[] RING_TX_WIRE = new long[RING];
    private static final long[] RING_RX_IN = new long[RING];
    private static final long[] RING_RX_OUT = new long[RING];

    private static long lastCaptureTime;
    private static long lastTxRaw;
    private static long lastTxWire;
    private static long lastRxIn;
    private static long lastRxOut;
    private static int samples;
    private static int ringIdx;
    private static int ringCount;

    private ZstdStatsData() {
    }

    /** 出站：raw = 原始包字节数，wire = 实际写出的帧负载字节数。 */
    public static void addTxBatch(int rawBytes, int wireBytes) {
        ACCUM_TX_RAW.add(rawBytes);
        ACCUM_TX_WIRE.add(wireBytes);
    }

    /** 入站：in = 收到的帧负载字节数，out = 解压后字节数。 */
    public static void addRxBatch(int inBytes, int outBytes) {
        ACCUM_RX_IN.add(inBytes);
        ACCUM_RX_OUT.add(outBytes);
    }

    /**
     * 计算采样窗口内的平均速率，返回 HUD 单行文本（含 § 颜色码）。
     * 数据不足（前 3 次采样）或无流量时返回 null。
     */
    public static synchronized String captureTabStats(int intervalSec) {
        long now = System.currentTimeMillis();
        long elapsed = lastCaptureTime > 0 ? now - lastCaptureTime : intervalSec * 1000L;
        long curTxRaw = ACCUM_TX_RAW.sum();
        long curTxWire = ACCUM_TX_WIRE.sum();
        long curRxIn = ACCUM_RX_IN.sum();
        long curRxOut = ACCUM_RX_OUT.sum();

        long dTxRaw = curTxRaw - lastTxRaw;
        long dTxWire = curTxWire - lastTxWire;
        long dRxIn = curRxIn - lastRxIn;
        long dRxOut = curRxOut - lastRxOut;

        samples++;
        lastCaptureTime = now;
        lastTxRaw = curTxRaw;
        lastTxWire = curTxWire;
        lastRxIn = curRxIn;
        lastRxOut = curRxOut;

        double sec = elapsed / 1000.0;
        if (sec <= 0) return null;

        RING_TX_RAW[ringIdx] = (long) (dTxRaw / sec);
        RING_TX_WIRE[ringIdx] = (long) (dTxWire / sec);
        RING_RX_IN[ringIdx] = (long) (dRxIn / sec);
        RING_RX_OUT[ringIdx] = (long) (dRxOut / sec);
        ringIdx = (ringIdx + 1) % RING;
        if (ringCount < RING) ringCount++;
        if (samples < 3) return null;

        long avgTxRaw = avg(RING_TX_RAW);
        long avgTxWire = avg(RING_TX_WIRE);
        long avgRxIn = avg(RING_RX_IN);
        long avgRxOut = avg(RING_RX_OUT);
        if (avgTxRaw == 0 && avgRxIn == 0) return null;

        double txRatio = avgTxRaw > 0 ? 100.0 * avgTxWire / avgTxRaw : 100.0;
        double rxRatio = avgRxOut > 0 ? 100.0 * avgRxIn / avgRxOut : 100.0;
        return String.format("\u00a7lTX:\u00a7r %s \u2192 %s %s  |  \u00a7lRX:\u00a7r %s \u2192 %s %s",
                fmtBytes(avgTxRaw), fmtBytes(avgTxWire), colorRatio(txRatio),
                fmtBytes(avgRxOut), fmtBytes(avgRxIn), colorRatio(rxRatio));
    }

    /** 带宽格式化：统一走共享实现，避免项目里出现第二套 KB/MB 口径。 */
    private static String fmtBytes(long bps) {
        return String.format("%7s", mikumc.zstd.protocol.ZstdBossBarFormat.rate(bps));
    }

    /**
     * 诊断用摘要：用于回答"HUD 为什么没内容"——是埋点没被调用（计数为 0），
     * 还是采样次数不够，还是渲染路径没跑到。
     */
    public static synchronized String debugSummary() {
        return String.format("samples=%d txRaw=%d txWire=%d rxIn=%d rxOut=%d",
                samples, ACCUM_TX_RAW.sum(), ACCUM_TX_WIRE.sum(),
                ACCUM_RX_IN.sum(), ACCUM_RX_OUT.sum());
    }

    private static String colorRatio(double ratio) {
        String s = String.format("%5.1f%%", ratio);
        if (ratio < 30.0) return "\u00a7a" + s;
        if (ratio < 80.0) return "\u00a7e" + s;
        return "\u00a7c" + s;
    }

    private static long avg(long[] ring) {
        long sum = 0;
        for (int i = 0; i < ringCount; i++) sum += ring[i];
        return ringCount > 0 ? sum / ringCount : 0;
    }
}
