package mikumc.zstd;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

/**
 * 运行时流量统计。编码器热路径直接调用本类。
 *
 * <p>⚠️ <b>本类绝不能引用任何平台类型</b>（Adventure / Velocity / Bukkit）。
 * 协议回归测试会直接实例化编码器，而它的 classpath 里只有 netty 与 zstd。
 * 曾经把统计与 BossBar 显示写在同一个类里，结果 {@code onFrame} 一被调用就
 * {@code NoClassDefFoundError: net/kyori/adventure/text/format/TextColor}，
 * 整个帧布局测试组全红。显示相关的代码一律放 {@link ZstdBossBarMonitor}。</p>
 */
public final class ZstdTrafficStats {

    private static final AtomicInteger ACTIVE_ZSTD = new AtomicInteger();
    private static final LongAdder TOTAL_RAW = new LongAdder();
    private static final LongAdder TOTAL_WIRE = new LongAdder();

    private static volatile boolean enabled;
    private static volatile long lastSampleMs;
    private static volatile long lastRaw;
    private static volatile long lastWire;

    private ZstdTrafficStats() {
    }

    /** 开关采集：由 BossBar 监控控制。关闭时 {@link #record} 只剩一次 volatile 读。 */
    public static void setEnabled(boolean value) {
        enabled = value;
    }

    public static boolean isEnabled() {
        return enabled;
    }

    /** 编码器每帧回调（{@code raw} 未压缩字节、{@code wire} 压缩后实际写出的字节）。 */
    public static void record(int rawBytes, int wireBytes) {
        if (!enabled) {
            return;
        }
        TOTAL_RAW.add(rawBytes);
        TOTAL_WIRE.add(wireBytes);
    }

    public static void playerActivated() {
        ACTIVE_ZSTD.incrementAndGet();
    }

    public static void playerDeactivated() {
        ACTIVE_ZSTD.updateAndGet(v -> v > 0 ? v - 1 : 0);
    }

    /** 重置采样基线：开启监控时先调一次，避免把历史累计当成 1 秒的量。 */
    public static synchronized void resetBaseline() {
        lastSampleMs = System.currentTimeMillis();
        lastRaw = TOTAL_RAW.sum();
        lastWire = TOTAL_WIRE.sum();
    }

    /** 采样快照：本周期速率与压缩率。 */
    public static final class Snapshot {
        public final int players;
        public final long rawPerSec;
        public final long wirePerSec;
        /** 压缩后 / 原始，如 0.18 表示压到 18% */
        public final double ratio;

        Snapshot(int players, long rawPerSec, long wirePerSec, double ratio) {
            this.players = players;
            this.rawPerSec = rawPerSec;
            this.wirePerSec = wirePerSec;
            this.ratio = ratio;
        }
    }

    public static synchronized Snapshot sample() {
        long now = System.currentTimeMillis();
        long curRaw = TOTAL_RAW.sum();
        long curWire = TOTAL_WIRE.sum();
        double sec = lastSampleMs == 0 ? 1.0 : Math.max((now - lastSampleMs) / 1000.0, 0.001);

        long dRaw = curRaw - lastRaw;
        long dWire = curWire - lastWire;
        lastSampleMs = now;
        lastRaw = curRaw;
        lastWire = curWire;

        long rawPerSec = (long) (dRaw / sec);
        long wirePerSec = (long) (dWire / sec);
        double ratio = dRaw > 0 ? (double) dWire / dRaw : 1.0;
        return new Snapshot(ACTIVE_ZSTD.get(), rawPerSec, wirePerSec, ratio);
    }
}
