package mikumc.zstd.paper;

import io.papermc.paper.threadedregions.scheduler.ScheduledTask;
import org.bukkit.Bukkit;
import org.bukkit.boss.BarColor;
import org.bukkit.boss.BarStyle;
import org.bukkit.boss.BossBar;
import org.bukkit.entity.Player;
import org.bukkit.command.CommandSender;
import org.bukkit.plugin.java.JavaPlugin;

import mikumc.zstd.protocol.ZstdBossBarFormat;
import mikumc.zstd.protocol.ZstdTrafficCounter;

/**
 * 运行时监控（Paper / Folia 端）：ZSTD 通道玩家数 + 下行带宽与压缩率统计（BossBar 实时显示）。
 *
 * <h2>数据来源</h2>
 * <ul>
 *   <li><b>ZSTD 通道玩家数</b>：连接激活时 +1、断开时 -1（见 ZstdPaperNegotiator / ChannelManager）；</li>
 *   <li><b>带宽与压缩率</b>：编码器每帧回调 {@link #record(int, int)}
 *       —— {@code raw} 是未经压缩的原始字节（"不压缩时本要发送的量"），
 *       {@code wire} 是 zstd 压缩后实际写出的字节。</li>
 * </ul>
 * <p>注意：统计口径只覆盖<b>走 zstd 的连接的下行</b>，不统计原版上行。未装模组的玩家走原版 zlib，不在本统计内。</p>
 *
 * <h2>为什么计数与采样不在本类里</h2>
 * <p>那部分逻辑与 Velocity 端逐行相同（活跃连接数、累计字节、速率采样、采集开关），
 * 已收敛到共享的 {@link ZstdTrafficCounter}——本类只保留 <b>Bukkit 相关的展示</b>
 * （BossBar 创建、任务调度、§ 颜色码）。这是"同一个模板在两端显示不同数字"的唯一根治办法：
 * 口径只有一份。</p>
 *
 * <h2>为什么刷新任务绑在玩家实体上（Folia）</h2>
 * <p>BossBar 的每个挂载玩家都是<b>区域化的实体</b>：{@code addPlayer} / {@code removeAll} /
 * {@code setTitle} 最终都会向该玩家的连接发包。Folia 里没有任何"主线程"可以承载这种操作，
 * 而 {@code RegionScheduler} 绑定的是<b>固定坐标</b>——玩家一传送就失效。
 * 因此这里把刷新任务交给 {@code player.getScheduler()}：实体移动、换世界、跨区域传送后
 * 任务仍落在拥有该玩家的区域线程上；实体退役时触发 {@code retired} 回调，不会留下悬挂任务。</p>
 */
public final class ZstdPaperMonitor {

    // ── BossBar（本类独有的展示层） ──
    private static BossBar bossBar;
    /** 刷新任务。Folia 上来自 {@code EntityScheduler}，Paper 上由区域调度器的兼容实现提供。 */
    private static ScheduledTask refreshTask;
    /** BossBar 的持有人（谁执行了命令） */
    private static Player owner;

    private ZstdPaperMonitor() {
    }

    // ── 数据采集：全部转发到共享实现 ──

    /** 采集开关（由 BossBar 监控开关控制）。关闭时只剩一次 volatile 读。 */
    public static void setEnabled(boolean value) {
        ZstdTrafficCounter.setEnabled(value);
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
        // BossBar 的宿主是玩家实体：创建、挂载、刷新、摘除都要落在拥有该玩家的区域线程。
        // Paper 上 isOwnedByCurrentRegion 恒为 true（命令本就在主线程执行），所以这一支在 Paper 上
        // 仍然是同步完成的，行为与改造前一致；只有 Folia 会真正走投递分支。
        if (Bukkit.isOwnedByCurrentRegion(player)) {
            startFor(player);
        } else if (!player.getScheduler().execute(plugin(), () -> startFor(player), null, 0L)) {
            return "§c无法开启：该玩家当前不在任何已加载区域（刚下线或正在切换世界），请稍后再试。";
        }
        return "§aBossBar 监控已开启（每秒刷新）。再执行 /mikuzstd bar 可关闭。";
    }

    /**
     * 在玩家所属区域线程上创建并挂载 BossBar，并把刷新任务绑到该玩家的实体调度器上。
     *
     * <p>方法本身是 {@code synchronized}：{@link #toggle} 与投递进来的任务可能来自不同线程，
     * 两边都要能安全地"检查一次、只建一个"。</p>
     */
    private static synchronized void startFor(Player player) {
        if (bossBar != null || !player.isOnline()) {
            return;
        }
        BossBar bar = Bukkit.createBossBar("§bMiku-ZSTD", BarColor.GREEN, BarStyle.SEGMENTED_20);
        bar.addPlayer(player);
        owner = player;
        bossBar = bar;
        setEnabled(true);
        // 先取一次基线，避免第一次算速率时把历史累计当成 1 秒的量
        ZstdTrafficCounter.resetBaseline();
        // ⚠️ 返回 null 表示实体已退役（任务没排上），此时不能留着半截状态。
        ScheduledTask task = player.getScheduler().runAtFixedRate(
                plugin(), scheduled -> refresh(), ZstdPaperMonitor::onOwnerRetired, 20L, 20L);
        if (task == null) {
            clearState(true);
            return;
        }
        refreshTask = task;
    }

    /** 主动关闭（命令 / 插件停用）：取消任务并把 BossBar 从玩家屏幕上摘掉。 */
    public static void stop() {
        clearState(true);
    }

    /** 每秒刷新一次；运行在 owner 所属的区域线程上。 */
    private static void refresh() {
        synchronized (ZstdPaperMonitor.class) {
            BossBar bar = bossBar;
            Player viewer = owner;
            if (bar == null) {
                return;
            }
            // 玩家离线后自动收尾（Folia 上实体退役另有 retired 回调，这里只作兜底）
            if (viewer == null || !viewer.isOnline()) {
                clearState(false);
                return;
            }

            ZstdTrafficCounter.Snapshot s = ZstdTrafficCounter.sample();
            bar.setTitle(applyFormat(ZstdPaperConfig.INSTANCE.bossbarFormat, s));
            // 进度条表示"省下的比例"：压缩越好条越长
            bar.setProgress(Math.max(0.0, Math.min(1.0, 1.0 - s.ratio)));
        }
    }

    /**
     * 实体退役回调（Folia 语义：玩家被移除 / 离开世界 / 尚未重新加入）。
     *
     * <p>只释放内存状态：该回调运行在关键路径上，不能在这里做发包、加载区块之类的操作，
     * 也不该再去 cancel 一个已经随实体一起退役的任务。</p>
     */
    private static void onOwnerRetired() {
        clearState(false);
    }

    /**
     * 清理状态。
     *
     * @param detachBar 是否把 BossBar 从玩家屏幕上摘掉。只有"主动关闭"才需要；
     *                  实体退役时玩家已经不在，屏幕上本就没有这条 BossBar。
     */
    private static synchronized void clearState(boolean detachBar) {
        BossBar bar = bossBar;
        Player viewer = owner;
        ScheduledTask task = refreshTask;
        bossBar = null;
        owner = null;
        refreshTask = null;
        setEnabled(false);

        if (task != null) {
            try {
                task.cancel();
            } catch (Throwable ignored) {
                // 任务已随实体退役被取消：无需处理
            }
        }
        if (!detachBar || bar == null || viewer == null || !viewer.isOnline()) {
            return;
        }
        // removeAll 会向挂载玩家发包，必须在玩家自己的区域线程上执行。
        if (Bukkit.isOwnedByCurrentRegion(viewer)) {
            bar.removeAll();
            return;
        }
        MikuZstdPaper plugin = pluginOrNull();
        if (plugin == null || !viewer.getScheduler().execute(plugin, bar::removeAll, null, 0L)) {
            // 玩家刚退役 / 插件已在停用流程中：屏幕上不会再有这条 BossBar，直接丢弃。
            bar.removeAll();
        }
    }

    private static MikuZstdPaper plugin() {
        return JavaPlugin.getPlugin(MikuZstdPaper.class);
    }

    /** 停用流程中拿插件实例；拿不到返回 null（此时不再依赖调度器，直接同步清理）。 */
    private static MikuZstdPaper pluginOrNull() {
        try {
            return plugin();
        } catch (Throwable t) {
            return null;
        }
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
