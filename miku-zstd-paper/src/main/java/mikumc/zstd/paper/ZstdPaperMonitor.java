package mikumc.zstd.paper;

import org.bukkit.Bukkit;
import org.bukkit.boss.BarColor;
import org.bukkit.boss.BarStyle;
import org.bukkit.boss.BossBar;
import org.bukkit.entity.Player;
import org.bukkit.command.CommandSender;
import org.bukkit.scheduler.BukkitTask;

import mikumc.zstd.protocol.ZstdBossBarFormat;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

/**
 * 运行时监控：ZSTD 通道玩家数 + 带宽与压缩率统计（BossBar 实时显示）。
 *
 * <h2>数据来源</h2>
 * <ul>
 *   <li><b>ZSTD 通道玩家数</b>：连接激活时 +1、断开时 -1（见 ZstdPaperNegotiator / ChannelManager）；</li>
 *   <li><b>带宽与压缩率</b>：编码器每帧回调 {@link #record(int, int)}
 *       —— {@code raw} 是未经压缩的原始字节（"不压缩时本要发送的量"），
 *       {@code wire} 是 zstd 压缩后实际写出的字节。</li>
 * </ul>
 * <p>注意：统计口径只覆盖<b>走 zstd 的连接</b>。未装模组的玩家走原版 zlib，不在本统计内。</p>
 */
public final class ZstdPaperMonitor {

    /** 走 zstd 通道的活跃连接数 */
    private static final AtomicInteger ACTIVE_ZSTD = new AtomicInteger();
    /** 累计原始字节（未压缩口径） */
    private static final LongAdder TOTAL_RAW = new LongAdder();
    /** 累计 zstd 压缩后字节 */
    private static final LongAdder TOTAL_WIRE = new LongAdder();

    private static volatile long lastSampleMs;
    private static volatile long lastRaw;
    private static volatile long lastWire;

    // ── BossBar ──
    private static BossBar bossBar;
    private static BukkitTask refreshTask;
    /** BossBar 的持有人（谁执行了命令） */
    private static Player owner;

    private ZstdPaperMonitor() {
    }

    // ── 数据采集 ──

    /**
     * 采集开关（由 BossBar 监控开关控制）。
     *
     * <p>与 Velocity 端的 {@code ZstdTrafficStats} 保持一致：没人看的时候不必累计。
     * 关闭时 {@link #record} 只剩一次 volatile 读，编码器热路径仍然极便宜。</p>
     */
    private static volatile boolean enabled;

    public static void setEnabled(boolean value) {
        enabled = value;
    }

    public static boolean isEnabled() {
        return enabled;
    }

    /** 编码器每次成帧后调用（热路径，仅两次加法）。 */
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

    // ─────────────── BossBar ───────────────

    /** 开/关 BossBar，返回给调用者的提示文本。 */
    public static synchronized String toggle(CommandSender sender) {
        if (bossBar != null) {
            stop();
            return "§7BossBar 监控已关闭。";
        }
        if (!(sender instanceof Player player)) {
            return "§cBossBar 需要由玩家执行（控制台没有屏幕）。可用 /mikuzstd 查看文本状态。";
        }
        owner = player;
        bossBar = Bukkit.createBossBar("§bMiku-ZSTD", BarColor.GREEN, BarStyle.SEGMENTED_20);
        bossBar.addPlayer(player);
        setEnabled(true);
        // 先取一次基线，避免第一次算速率时把历史累计当成 1 秒的量
        sample();
        MikuZstdPaper plugin = org.bukkit.plugin.java.JavaPlugin.getPlugin(MikuZstdPaper.class);
        refreshTask = Bukkit.getScheduler().runTaskTimer(plugin, ZstdPaperMonitor::refresh, 20L, 20L);
        return "§aBossBar 监控已开启（每秒刷新）。再执行 /mikuzstd bar 可关闭。";
    }

    public static synchronized void stop() {
        if (refreshTask != null) {
            refreshTask.cancel();
            refreshTask = null;
        }
        if (bossBar != null) {
            bossBar.removeAll();
            bossBar = null;
        }
        owner = null;
        setEnabled(false);
    }

    private static void refresh() {
        BossBar bar = bossBar;
        Player viewer = owner;
        if (bar == null) return;
        // 玩家离线后自动收尾
        if (viewer == null || !viewer.isOnline()) {
            stop();
            return;
        }

        Snapshot s = sample();
        bar.setTitle(applyFormat(ZstdPaperConfig.INSTANCE.bossbarFormat, s));
        // 进度条表示"省下的比例"：压缩越好条越长
        bar.setProgress(Math.max(0.0, Math.min(1.0, 1.0 - s.ratio)));
    }

    /**
     * 按配置模板渲染文本。
     *
     * <p>占位符：{@code %players%} 使用人数、{@code %raw%} 原始带宽、
     * {@code %wire%} zstd 后带宽、{@code %ratio%} 全服平均压缩率。
     * 带宽占位符<b>自带单位</b>，模板里不用再补 "/s"；空闲无流量时
     * {@code %ratio%} 渲染为 {@code --}。</p>
     */
    static String applyFormat(String format, Snapshot s) {
        // 格式化逻辑在 ZstdBossBarFormat 里，与 Velocity 端共用同一份——两端表现必须一致。
        return ZstdBossBarFormat.colorize(ZstdBossBarFormat.substitute(
                format, s.players, s.rawPerSec, s.wirePerSec, s.ratio));
    }
}
