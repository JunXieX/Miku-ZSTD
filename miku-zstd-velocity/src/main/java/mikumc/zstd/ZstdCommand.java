package mikumc.zstd;

import com.mojang.brigadier.tree.LiteralCommandNode;
import com.velocitypowered.api.command.BrigadierCommand;
import com.velocitypowered.api.command.CommandSource;
import com.velocitypowered.api.proxy.ProxyServer;
import net.kyori.adventure.audience.Audience;
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

    /** 构建命令树：{@code /mikuzstd [status|bar|reload|train [force]]}。 */
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
                // 下面两个会真的改动服务端状态（重载配置 / 触发训练），保留权限门槛：
                // 装了权限插件后授予 mikuzstd.command（或 zstd.command）即可。
                // 注意 requires 沿链累积，force 会自动继承 train 的限制。
                .then(BrigadierCommand.literalArgumentBuilder("reload")
                        .requires(ZstdCommand::hasPermission)
                        .executes(ctx -> {
                            reload(ctx.getSource());
                            return 1;
                        }))
                .then(BrigadierCommand.literalArgumentBuilder("train")
                        .requires(ZstdCommand::hasPermission)
                        .executes(ctx -> {
                            train(ctx.getSource(), false);
                            return 1;
                        })
                        .then(BrigadierCommand.literalArgumentBuilder("force")
                                .executes(ctx -> {
                                    train(ctx.getSource(), true);
                                    return 1;
                                })))
                .build();
        return new BrigadierCommand(node);
    }

    /** 兼容两端的权限名：Velocity 沿用 zstd.command，同时接受 Paper 端的 mikuzstd.command。 */
    private static boolean hasPermission(CommandSource source) {
        return source.hasPermission("mikuzstd.command") || source.hasPermission("zstd.command");
    }

    private void showStatus(CommandSource source) {
        ZstdVelocityConfig cfg = ZstdVelocityConfig.INSTANCE;

        source.sendMessage(Component.text("═══ Miku-ZSTD Status ═══", NamedTextColor.GOLD));
        source.sendMessage(Component.text("Version: " + BuildConstants.VERSION, NamedTextColor.GRAY));

        source.sendMessage(Component.text("── Compression ──", NamedTextColor.YELLOW));
        source.sendMessage(Component.text("  Level: " + cfg.level + "  WindowLog: " + cfg.windowLog,
                NamedTextColor.GRAY));

        source.sendMessage(Component.text("── Connections ──", NamedTextColor.YELLOW));
        source.sendMessage(Component.text("  Online players: " + proxy.getPlayerCount(), NamedTextColor.GRAY));

        source.sendMessage(Component.text("── Trainer ──", NamedTextColor.YELLOW));
        showTrainerStatus(source, "Encoder", ZstdSampleTrainer.getEncoder());
        showTrainerStatus(source, "Decoder", ZstdSampleTrainer.getDecoder());

        source.sendMessage(Component.text("── 压缩线程池 ──", NamedTextColor.YELLOW));
        source.sendMessage(Component.text("  " + ZstdCompressPool.summary(), NamedTextColor.GRAY));

        source.sendMessage(Component.text("── 解压缓冲 ──", NamedTextColor.YELLOW));
        source.sendMessage(Component.text(
                "  " + mikumc.zstd.protocol.ZstdBatchDecoderBase.scratchStats(), NamedTextColor.GRAY));
        if (ZstdCompressPool.backlogged()) {
            source.sendMessage(Component.text(
                    "  队列积压：压缩跟不上流量 → 优先降低 level，其次调大 threads",
                    NamedTextColor.RED));
        }

        source.sendMessage(Component.text("── Config ──", NamedTextColor.YELLOW));
        source.sendMessage(Component.text("  debug: " + cfg.debug, NamedTextColor.GRAY));
    }

    private void showTrainerStatus(Audience source, String label, ZstdSampleTrainer trainer) {
        if (trainer == null) {
            source.sendMessage(Component.text("  " + label + ": not initialized", NamedTextColor.RED));
            return;
        }
        long dictId = trainer.getCurrentDictId();
        byte[] dict = trainer.getCurrentDict();
        source.sendMessage(Component.text("  " + label + ": samples=" + trainer.getSampleCount()
                + " bytes=" + trainer.getSampleBytes()
                + " dictId=" + dictId
                + " dictSize=" + (dict != null ? dict.length : 0) + "B",
                NamedTextColor.GRAY));
    }

    /** 各连接的压缩统计排行 —— 定位"是谁在拖后腿"。 */
    private void showTop(CommandSource source) {
        source.sendMessage(Component.text("═══ 各连接压缩统计 ═══", NamedTextColor.GOLD));
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
        source.sendMessage(Component.text("Config reloaded.", NamedTextColor.GREEN));
        // ⚠️ 必须说清楚作用范围，否则管理员会以为 reload 没生效。三类参数都不会作用于已有连接：
        //   · level / window_log：连接建立时应用到该连接的 zstd 上下文（见 ZstdChannelManager 构造器）；
        //   · threads：线程池是首次 init 时就固定下来的（ZstdCompressPool.init 幂等）；
        source.sendMessage(Component.text(
                "注意：level / window_log / threads 只对【之后新建立的连接】（线程池则需重启代理）生效；"
                        + "已有连接仍在用旧值。logging.debug 与 bossbar.format 已即时生效。",
                NamedTextColor.YELLOW));
    }

    private void train(CommandSource source, boolean forced) {
        ZstdSampleTrainer enc = ZstdSampleTrainer.getEncoder();
        ZstdSampleTrainer dec = ZstdSampleTrainer.getDecoder();

        // train       ：样本达标才训练（绕过冷却与"环满 / 兜底超时"门槛）
        // train force ：连样本门槛一起绕过（小服 / 测试环境用）
        boolean encStarted = triggerTrain(enc, forced);
        boolean decStarted = triggerTrain(dec, forced);

        Component msg;
        if (!forced && !encStarted && !decStarted) {
            msg = Component.text("两端样本都不足，本次未触发训练。"
                    + "可用 /mikuzstd train force 绕过样本门槛。", NamedTextColor.YELLOW);
        } else {
            msg = Component.text(forced ? "Force training triggered. " : "Training triggered. ",
                    NamedTextColor.GREEN);
        }
        if (enc != null) {
            msg = msg.append(Component.text("Encoder: " + enc.getSampleCount() + " samples, dict="
                    + enc.getCurrentDictId() + "  ", NamedTextColor.GRAY));
        }
        if (dec != null) {
            msg = msg.append(Component.text("Decoder: " + dec.getSampleCount() + " samples, dict="
                    + dec.getCurrentDictId(), NamedTextColor.GRAY));
        }
        source.sendMessage(msg);
    }

    /** 按 {@code forced} 选择"绕过一切"还是"仍要求样本达标"。 */
    private static boolean triggerTrain(ZstdSampleTrainer trainer, boolean forced) {
        if (trainer == null) {
            return false;
        }
        if (forced) {
            trainer.forceTrain();
            return true;
        }
        return trainer.requestTrain();
    }
}
