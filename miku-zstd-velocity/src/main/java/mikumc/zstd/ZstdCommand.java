package mikumc.zstd;

import com.mojang.brigadier.tree.LiteralCommandNode;
import com.velocitypowered.api.command.BrigadierCommand;
import com.velocitypowered.api.command.CommandSource;
import com.velocitypowered.api.proxy.ProxyServer;
import net.kyori.adventure.text.Component;
import net.kyori.adventure.text.format.NamedTextColor;
import mikumc.zstd.protocol.ZstdCompressPool;

/**
 * {@code /mikuzstd} 命令树（Velocity 端）。
 *
 * <p>⚠️ <b>必须用 Brigadier 显式建树</b>，不能只给 {@code SimpleCommand} + {@code metaBuilder}：
 * 那种组合注册出来的是<b>一个不接受任何参数的光杆 literal 节点</b>——客户端命令树里没有子节点，
 * 于是 {@code /mikuzstd bar} 会报
 * {@code Incorrect argument for command at position 9: mikuzstd}，TAB 也一点提示都没有。</p>
 *
 * <p>更糟的是 Velocity 注入的命令树<b>会盖住后端子服的同名命令</b>（即使子服的注册是对的），
 * 所以这个缺陷会直接表现为"在后端子服务器里用不了"。</p>
 *
 * <p>用 literal 子节点的额外好处：TAB 补全由 Brigadier 自动完成，不必手写 suggest。</p>
 */
public final class ZstdCommand {

    private final ProxyServer proxy;
    private final MikuZstdVelocity plugin;

    public ZstdCommand(ProxyServer proxy, MikuZstdVelocity plugin) {
        this.proxy = proxy;
        this.plugin = plugin;
    }

    /** 构建命令树：{@code /mikuzstd [status|top|bar|reload]}。 */
    public BrigadierCommand build() {
        LiteralCommandNode<CommandSource> node = BrigadierCommand.literalArgumentBuilder("mikuzstd")
                // ⚠️ 主节点刻意【不设 requires】。
                //
                // Velocity 自身没有内置权限系统：代理上要是没装权限插件（LuckPerms 之类），
                // 玩家的 hasPermission 恒为 false。而 Brigadier 对 requires=false 的节点是
                // 【把它连同子树整个从命令树里剔除】——不只是禁用。客户端拿到的命令树里
                // 根本没有 mikuzstd，于是表现为"命令不存在 + TAB 毫无提示"，连在后端子服里
                // 敲也没用（命令树是代理注入的，与子服无关）。
                // 所以这里只读/无副作用的子命令对所有人开放。
                .executes(ctx -> {
                    showStatus(ctx.getSource());
                    return 1;
                })
                .then(BrigadierCommand.literalArgumentBuilder("status")
                        .executes(ctx -> {
                            showStatus(ctx.getSource());
                            return 1;
                        }))
                // top 只读，对所有人开放（和 status 同级）
                .then(BrigadierCommand.literalArgumentBuilder("top")
                        .executes(ctx -> {
                            showTop(ctx.getSource());
                            return 1;
                        }))
                // ⚠️ bar 有权限门槛：它看起来只是"开个显示"，实际是<b>全局</b>状态
                //（单人持有、且开关时会连带启停 ZstdTrafficCounter 采集）。
                // 以前它和只读子命令一样对所有人开放，于是任何玩家都能把管理员
                // 正在看的监控关掉，并顺带让统计归零——这是状态变更，不是一个视图开关。
                .then(BrigadierCommand.literalArgumentBuilder("bar")
                        .requires(ZstdCommand::hasPermission)
                        .executes(ctx -> {
                            ctx.getSource().sendMessage(ZstdBossBarMonitor.toggle(ctx.getSource()));
                            return 1;
                        }))
                // 重载配置会改动服务端状态，保留权限门槛：
                // 装了权限插件后授予 mikuzstd.command（或 zstd.command）即可。
                .then(BrigadierCommand.literalArgumentBuilder("reload")
                        .requires(ZstdCommand::hasPermission)
                        .executes(ctx -> {
                            reload(ctx.getSource());
                            return 1;
                        }))
                .build();
        return new BrigadierCommand(node);
    }

    /** 兼容两端的权限名：Velocity 沿用 zstd.command，同时接受 Paper 端的 mikuzstd.command。 */
    private static boolean hasPermission(CommandSource source) {
        return source.hasPermission("mikuzstd.command") || source.hasPermission("zstd.command");
    }

    private void showStatus(CommandSource source) {
        ZstdVelocityConfig cfg = ZstdVelocityConfig.INSTANCE;

        source.sendMessage(Component.text("═══ Miku-ZSTD 状态 ═══", NamedTextColor.GOLD));
        source.sendMessage(Component.text("版本：" + BuildConstants.VERSION, NamedTextColor.GRAY));
        source.sendMessage(Component.text("── 运行概况 ──", NamedTextColor.YELLOW));
        source.sendMessage(Component.text("  压缩等级：" + cfg.level + "  窗口指数：" + cfg.windowLog
                + "  在线玩家：" + proxy.getPlayerCount(), NamedTextColor.GRAY));
        source.sendMessage(Component.text("  线程池：" + ZstdCompressPool.summary(), NamedTextColor.GRAY));
        if (ZstdCompressPool.backlogged()) {
            source.sendMessage(Component.text(
                    "  队列积压：压缩跟不上流量，请优先降低压缩等级，其次增加线程数",
                    NamedTextColor.RED));
        }

        source.sendMessage(Component.text("  调试日志：" + (cfg.debug ? "开启" : "关闭"), NamedTextColor.GRAY));
    }

    /** 各连接的压缩统计排行 —— 定位"是谁在拖后腿"。 */
    private void showTop(CommandSource source) {
        source.sendMessage(Component.text("═══ 各连接下行压缩统计 ═══", NamedTextColor.GOLD));
        source.sendMessage(Component.text("  " + mikumc.zstd.protocol.ZstdConnStats.totalSummary(),
                NamedTextColor.GRAY));
        java.util.List<String> rows = mikumc.zstd.protocol.ZstdConnStats.top(10);
        if (rows.isEmpty()) {
            source.sendMessage(Component.text("  （当前没有走 zstd 的连接）", NamedTextColor.GRAY));
            return;
        }
        for (String row : rows) {
            source.sendMessage(Component.text("  " + row, NamedTextColor.GRAY));
        }
    }

    private void reload(CommandSource source) {
        ZstdVelocityConfig.reload();
        // 让 logging.debug 的改动也生效（启动时只应用过一次）：
        // 否则改完配置 reload 会显示 "debug: true"，日志却一条都不多——比不写这个开关更糟。
        if (plugin != null) {
            plugin.applyDebugFromConfig();
        }
        ZstdBandwidthProfiler.start(ZstdVelocityConfig.INSTANCE.debug, 60);
        source.sendMessage(Component.text("配置已重载。", NamedTextColor.GREEN));
        // ⚠️ 必须说清楚作用范围，否则管理员会以为 reload 没生效。三类参数都不会作用于已有连接：
        //   · level / window_log：连接建立时应用到该连接的 zstd 上下文（见 ZstdChannelManager 构造器）；
        //   · batch_window_ms / batch_max_packets：编码器 handlerAdded 时快照；
        //   · threads：线程池只在首次 init 时固定。
        source.sendMessage(Component.text(
                "即时生效：logging.debug、bossbar.format、skip_compress_below_bytes；"
                        + "需新连接：level、window_log、batch_window_ms、batch_max_packets；"
                        + "需重启代理：threads。已有连接仍在用旧值。",
                NamedTextColor.YELLOW));
    }

}
