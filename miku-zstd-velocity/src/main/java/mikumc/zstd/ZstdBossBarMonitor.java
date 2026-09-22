package mikumc.zstd;

import com.velocitypowered.api.command.CommandSource;
import com.velocitypowered.api.proxy.Player;
import com.velocitypowered.api.proxy.ProxyServer;
import com.velocitypowered.api.scheduler.ScheduledTask;
import mikumc.zstd.protocol.ZstdBossBarFormat;
import net.kyori.adventure.bossbar.BossBar;
import net.kyori.adventure.text.Component;
import net.kyori.adventure.text.format.NamedTextColor;
import net.kyori.adventure.text.serializer.legacy.LegacyComponentSerializer;

import java.time.Duration;

/**
 * 代理端运行时监控：zstd 通道玩家数 + 带宽与压缩率统计（BossBar 实时显示）。
 *
 * <p><b>为什么统计放在 Velocity 端</b>：在"代理 + 后端子服"的架构里，zstd 是在
 * <b>代理 ↔ 客户端</b>这一段生效的，所以只有代理端看到的字节数才等于玩家真实带宽。
 * 子服侧若有插件，统计的是"子服 ↔ 代理"那一段，口径不同。</p>
 *
 * <p>数据来自 {@link ZstdTrafficStats}（统计与显示刻意分开：那个类零平台依赖，
 * 因为编码器热路径会调用它，而协议回归测试的 classpath 里没有 Adventure）。
 * 统计口径只覆盖<b>走 zstd 的连接</b>。</p>
 */
public final class ZstdBossBarMonitor {

    private static final LegacyComponentSerializer LEGACY = LegacyComponentSerializer.legacyAmpersand();

    private static volatile ProxyServer proxy;
    private static volatile Object plugin;

    private static volatile BossBar bossBar;
    private static volatile ScheduledTask refreshTask;
    private static volatile Player owner;

    private ZstdBossBarMonitor() {
    }

    /** 由插件启用时调用一次，保存显示 BossBar 所需的上下文。 */
    public static void init(ProxyServer proxyServer, Object pluginInstance) {
        proxy = proxyServer;
        plugin = pluginInstance;
    }

    // ─────────────── BossBar ───────────────

    /** 开/关 BossBar，返回给调用者的提示（已带颜色）。 */
    public static synchronized Component toggle(CommandSource source) {
        if (bossBar != null) {
            stop();
            return Component.text("BossBar 监控已关闭。", NamedTextColor.GRAY);
        }
        if (!(source instanceof Player player)) {
            return Component.text("BossBar 需要由玩家执行（控制台没有屏幕）。可用 /mikuzstd status 查看文本状态。",
                    NamedTextColor.RED);
        }
        if (proxy == null || plugin == null) {
            return Component.text("BossBar 监控不可用：插件尚未完成初始化。", NamedTextColor.RED);
        }

        owner = player;
        BossBar bar = BossBar.bossBar(Component.empty(), 1.0f, BossBar.Color.GREEN, BossBar.Overlay.NOTCHED_20);
        bossBar = bar;
        ZstdTrafficStats.setEnabled(true);
        player.showBossBar(bar);
        ZstdTrafficStats.resetBaseline(); // 先取基线，避免把历史累计当成 1 秒的量

        refreshTask = proxy.getScheduler()
                .buildTask(plugin, ZstdBossBarMonitor::refresh)
                .delay(Duration.ofMillis(1000))
                .repeat(Duration.ofMillis(1000))
                .schedule();
        return Component.text("BossBar 监控已开启（每秒刷新）。再执行 /mikuzstd bar 可关闭。", NamedTextColor.GREEN);
    }

    public static synchronized void stop() {
        ScheduledTask task = refreshTask;
        refreshTask = null;
        if (task != null) {
            task.cancel();
        }
        BossBar bar = bossBar;
        bossBar = null;
        ZstdTrafficStats.setEnabled(false);
        Player viewer = owner;
        owner = null;
        if (bar != null && viewer != null && viewer.isActive()) {
            viewer.hideBossBar(bar);
        }
    }

    public static boolean isRunning() {
        return bossBar != null;
    }

    private static void refresh() {
        BossBar bar = bossBar;
        Player viewer = owner;
        if (bar == null) {
            return;
        }
        // 玩家离线后自动收尾
        if (viewer == null || !viewer.isActive()) {
            stop();
            return;
        }

        ZstdTrafficStats.Snapshot s = ZstdTrafficStats.sample();
        String text = ZstdBossBarFormat.substitute(
                ZstdVelocityConfig.INSTANCE.bossbarFormat, s.players, s.rawPerSec, s.wirePerSec, s.ratio);
        // ⚠️ Adventure 5.x 把 BossBar#title 改名成了 #name（4.x 才叫 title）
        bar.name(LEGACY.deserialize(text));
        // 进度条表示"省下的比例"：压缩越好条越长
        bar.progress(Math.max(0.0f, Math.min(1.0f, (float) (1.0 - s.ratio))));
    }
}
