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

    public ZstdCommand(ProxyServer proxy) {
        this.proxy = proxy;
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
                .then(BrigadierCommand.literalArgumentBuilder("bar")
                        .executes(ctx -> {
                            ctx.getSource().sendMessage(ZstdBossBarMonitor.toggle(ctx.getSource()));
                            return 1;
                        }))
                // 下面两个会真的改动服务端状态（重载配置 / 强制训练），保留权限门槛：
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
        source.sendMessage(Component.text("Config reloaded.", NamedTextColor.GREEN));
        // ⚠️ 必须说清楚作用范围：compressCtx 的 level / window_log 只在【连接建立时】应用到
        // 该连接的上下文上（见 ZstdChannelManager 构造器）。reload 只替换配置单例，
        // 已经建立的连接仍拿着旧参数——不提示的话，管理员会以为 reload 没生效。
        source.sendMessage(Component.text(
                "注意：level / window_log 等参数只对【之后新建立的连接】生效；"
                        + "已有连接仍在用旧值，需要【重启代理】才会全部更新。",
                NamedTextColor.YELLOW));
    }

    private void train(CommandSource source, boolean forced) {
        ZstdSampleTrainer enc = ZstdSampleTrainer.getEncoder();
        ZstdSampleTrainer dec = ZstdSampleTrainer.getDecoder();

        // `/mikuzstd train` 与 `/mikuzstd train force` 语义一致：都是立即触发一轮训练
        // （forceTrain 本就绕过采样门槛与冷却）。
        if (enc != null) {
            enc.forceTrain();
        }
        if (dec != null) {
            dec.forceTrain();
        }

        Component msg = Component.text(forced ? "Force training triggered. " : "Training triggered. ",
                NamedTextColor.GREEN);
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
}
