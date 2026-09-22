package mikumc.zstd.protocol;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

/**
 * 运行时流量统计的<b>共享实现</b>（Velocity 与 Paper 两端共用）。
 *
 * <h2>为什么必须共享</h2>
 * <p>这段"有没有人看 / 采一次速率 / 活跃连接数"的逻辑原本在两端各写了一份
 * （Velocity 的 {@code ZstdTrafficStats} 与 Paper 的 {@code ZstdPaperMonitor}），
 * 逐行几乎相同。这与本项目其它地方刻意避免的漂移模式完全一致——
 * 结果显示逻辑（{@link ZstdBossBarFormat}）被抽出来共享了，统计口径却留了两份，
 * 于是"两端同一个模板显示不同数字"这类问题只会在生产上被发现。</p>
 *
 * <p>两端各自的 <i>展示</i> 层（Velocity 用 Adventure、Paper 用 Bukkit）仍留在各自模块里，
 * 本类只负责计数与采样。</p>
 *
 * <h2>为什么允许静态（而非实例）</h2>
 * <p>一个进程只有一个服务端，因此<b>一个进程只需要一组计数器</b>。三端各自独立部署，
 * 静态状态不会跨端串味。</p>
 *
 * <h2>⚠️ 本类绝不能引用任何平台类型</h2>
 * <p>（Adventure / Velocity / Bukkit 都不行。）编码器热路径会直接调用 {@link #record}，
 * 而协议回归测试的 classpath 里只有 netty 与 zstd——曾把统计与 BossBar 显示写在同一个类里，
 * 结果 {@code onFrame} 一被调用就 {@code NoClassDefFoundError}，整个帧布局测试组全红。</p>
 */
public final class ZstdTrafficCounter {

    private static final AtomicInteger ACTIVE_ZSTD = new AtomicInteger();
    private static final LongAdder TOTAL_RAW = new LongAdder();
    private static final LongAdder TOTAL_WIRE = new LongAdder();

    /** 采集开关：由展示层（BossBar 监控）控制。关闭时 {@link #record} 只剩一次 volatile 读。 */
    private static volatile boolean enabled;
    private static volatile long lastSampleMs;
    private static volatile long lastRaw;
    private static volatile long lastWire;

    private ZstdTrafficCounter() {
    }

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

    /**
     * 重置采样基线：开启监控时先调一次，避免把历史累计当成 1 秒的量。
     *
     * <p>只在持有展示层的锁时调用（两端都在各自的 toggle 里同步调用）。</p>
     */
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

    /** 取一次快照并推进基线（两端都只在展示层线程上调用）。 */
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
