package mikumc.zstd.paper;

import org.bukkit.Bukkit;
import org.bukkit.boss.BarColor;
import org.bukkit.boss.BarStyle;
import org.bukkit.boss.BossBar;
import org.bukkit.entity.Player;
import org.bukkit.command.CommandSender;
import org.bukkit.scheduler.BukkitTask;

import mikumc.zstd.protocol.ZstdBossBarFormat;
import mikumc.zstd.protocol.ZstdTrafficCounter;

/**
 * 运行时监控（Paper 端）：ZSTD 通道玩家数 + 带宽与压缩率统计（BossBar 实时显示）。
 *
 * <h2>数据来源</h2>
 * <ul>
 *   <li><b>ZSTD 通道玩家数</b>：连接激活时 +1、断开时 -1（见 ZstdPaperNegotiator / ChannelManager）；</li>
 *   <li><b>带宽与压缩率</b>：编码器每帧回调 {@link #record(int, int)}
 *       —— {@code raw} 是未经压缩的原始字节（"不压缩时本要发送的量"），
 *       {@code wire} 是 zstd 压缩后实际写出的字节。</li>
 * </ul>
 * <p>注意：统计口径只覆盖<b>走 zstd 的连接</b>。未装模组的玩家走原版 zlib，不在本统计内。</p>
 *
 * <h2>为什么计数与采样不在本类里</h2>
 * <p>那部分逻辑与 Velocity 端逐行相同（活跃连接数、累计字节、速率采样、采集开关），
 * 已收敛到共享的 {@link ZstdTrafficCounter}——本类只保留 <b>Bukkit 相关的展示</b>
 * （BossBar 创建、任务调度、§ 颜色码）。这是"同一个模板在两端显示不同数字"的唯一根治办法：
 * 口径只有一份。</p>
 */
public final class ZstdPaperMonitor {

    // ── BossBar（本类独有的展示层） ──
    private static BossBar bossBar;
    private static BukkitTask refreshTask;
    /** BossBar 的持有人（谁执行了命令） */
    private static Player owner;

    private ZstdPaperMonitor() {
    }

    // ── 数据采集：全部转发到共享实现 ──

    /** 采集开关（由 BossBar 监控开关控制）。关闭时只剩一次 volatile 读。 */
    public static void setEnabled(boolean value) {
        ZstdTrafficCounter.setEnabled(value);
    }

    public static boolean isEnabled() {
        return ZstdTrafficCounter.isEnabled();
    }

    /** 编码器每次成帧后调用（热路径，仅两次加法）。 */
    public static void record(int rawBytes, int wireBytes) {
        ZstdTrafficCounter.record(rawBytes, wireBytes);
    }

    public static void playerActivated() {
        ZstdTrafficCounter.playerActivated();
    }

    public static void playerDeactivated() {
        ZstdTrafficCounter.playerDeactivated();
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
        ZstdTrafficCounter.resetBaseline();
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

        ZstdTrafficCounter.Snapshot s = ZstdTrafficCounter.sample();
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
    static String applyFormat(String format, ZstdTrafficCounter.Snapshot s) {
        // 格式化逻辑在 ZstdBossBarFormat 里，与 Velocity 端共用同一份——两端表现必须一致。
        return ZstdBossBarFormat.colorize(ZstdBossBarFormat.substitute(
                format, s.players, s.rawPerSec, s.wirePerSec, s.ratio));
    }
}
