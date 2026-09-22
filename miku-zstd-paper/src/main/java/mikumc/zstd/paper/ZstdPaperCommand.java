package mikumc.zstd.paper;

import io.papermc.paper.command.brigadier.BasicCommand;
import io.papermc.paper.command.brigadier.CommandSourceStack;
import org.bukkit.command.CommandSender;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import mikumc.zstd.protocol.ZstdCompressPool;

/**
 * {@code /mikuzstd}：状态与字典检测，以及 BossBar 实时监控开关。
 *
 * <p>⚠️ Paper 插件<b>不支持</b>在 {@code paper-plugin.yml} 里声明命令（调用
 * {@code JavaPlugin#getCommand} 会直接抛 {@code UnsupportedOperationException}），
 * 必须实现 {@link BasicCommand} 并经 {@code LifecycleEvents.COMMANDS} 注册。</p>
 *
 * <p>⚠️ TAB 补全必须显式实现 {@link #suggest}：{@link BasicCommand} 的默认实现返回空集合，
 * 不写就<b>一点补全提示都没有</b>（实测踩过）。</p>
 */
public final class ZstdPaperCommand implements BasicCommand {

    /** 子命令表：执行分发与 TAB 补全<b>共用</b>同一份，保证两者永不脱节。 */
    private static final List<String> SUBCOMMANDS = List.of("bar", "status", "top");

    @Override
    public void execute(CommandSourceStack source, String[] args) {
        CommandSender sender = source.getSender();

        if (args.length > 0 && "top".equalsIgnoreCase(args[0])) {
            sender.sendMessage("§6═══ 各连接压缩统计 ═══");
            sender.sendMessage("§7  " + mikumc.zstd.protocol.ZstdConnStats.totalSummary());
            java.util.List<String> rows = mikumc.zstd.protocol.ZstdConnStats.top(10);
            if (rows.isEmpty()) {
                sender.sendMessage("§7  （当前没有走 zstd 的连接）");
            } else {
                for (String row : rows) {
                    sender.sendMessage("§7  " + row);
                }
            }
            return;
        }
        if (args.length > 0 && "bar".equalsIgnoreCase(args[0])) {
            // ⚠️ bar 要权限：它看起来只是"开个显示"，实际是全局状态
            //（单人持有，且开关时连带启停 ZstdPaperMonitor 的采集）。
            // 只读的 status / top 不设门槛，与 Velocity 端策略保持一致。
            if (!sender.hasPermission("mikuzstd.command")) {
                sender.sendMessage("§c你没有权限执行这个子命令（需要 mikuzstd.command）。");
                return;
            }
            sender.sendMessage("§b[Miku-ZSTD]§r " + ZstdPaperMonitor.toggle(sender));
            return;
        }
        ZstdPaperConfig cfg = ZstdPaperConfig.INSTANCE;

        sender.sendMessage("§6═══ Miku-ZSTD (Paper) 状态 ═══");
        sender.sendMessage("§7  用法: /mikuzstd [status|top|bar]");

        int threshold = ZstdPaperCommandSupport.readCompressionThreshold();
        sender.sendMessage("§e── 环境 ──");
        sender.sendMessage(threshold < 0
                ? "§c  原版压缩阈值 = " + threshold + "（已关闭）→ 本插件会自动停用"
                : "§a  原版压缩阈值 = " + threshold + " → zstd 可接管");
        sender.sendMessage("§7  压缩等级=" + cfg.level + " 窗口=" + cfg.windowLog
                + " 批处理=" + cfg.batchWindowMs + "ms/" + cfg.batchMaxPackets + "包");

        sender.sendMessage("§e── 压缩线程池 ──");
        sender.sendMessage("§7  " + ZstdCompressPool.summary());
        if (ZstdCompressPool.backlogged()) {
            sender.sendMessage("§c  队列积压：压缩跟不上流量 → 优先降低 level，其次调大 threads");
        }

        sender.sendMessage("§e── 解压缓冲 ──");
        sender.sendMessage("§7  " + mikumc.zstd.protocol.ZstdBatchDecoderBase.scratchStats());

        sender.sendMessage("§e── 字典训练 ──");
        showTrainer(sender, "压缩(encoder)", ZstdPaperTrainer.getEncoder(), cfg.trainerMinSamples);
        showTrainer(sender, "解压(decoder)", ZstdPaperTrainer.getDecoder(), cfg.trainerMinSamples);

        sender.sendMessage("§e── 共享注册表 ──");
        long encId = ZstdPaperDictRegistry.encoderDictId();
        long decId = ZstdPaperDictRegistry.decoderDictId();
        sender.sendMessage(encId == 0
                ? "§7  压缩字典: §c未装载（dictId=0，正在用无字典压缩）"
                : "§7  压缩字典: §aid=" + encId);
        sender.sendMessage(decId == 0
                ? "§7  解压字典: §c未装载（dictId=0）"
                : "§7  解压字典: §aid=" + decId);

        if (encId == 0) {
            sender.sendMessage("§8  提示：字典需累积 " + cfg.trainerMinSamples
                    + " 个样本或等 " + (cfg.trainerFallbackTimeoutMs / 60000) + " 分钟兜底触发首轮训练；"
                    + "训练成功后日志会出现「采纳新字典」");
        }
    }

    /**
     * TAB 补全：只补第一个参数（子命令）。
     *
     * <p>{@link BasicCommand} 的 {@code suggest} 默认返回空集合 —— 不覆写就没有任何补全。</p>
     */
    @Override
    public Collection<String> suggest(CommandSourceStack source, String[] args) {
        if (args.length > 1) {
            return List.of(); // 本命令没有第二级参数
        }
        String prefix = args.length == 0 ? "" : args[0].toLowerCase(Locale.ROOT);
        List<String> out = new ArrayList<>(SUBCOMMANDS.size());
        for (String sub : SUBCOMMANDS) {
            if (sub.startsWith(prefix)) {
                out.add(sub);
            }
        }
        return out;
    }

    /**
     * 本命令<b>不设整树权限门槛</b>：只读子命令（status / top）对所有人开放，
     * 与 Velocity 端策略一致（那一端的注释解释了为什么整树门控会让命令彻底消失）。
     * 有副作用的 {@code bar} 在 {@link #execute} 里单独校验 {@code mikuzstd.command}。
     *
     * <p>返回 {@code null} 表示"没有额外权限要求"。以前这里返回
     * {@code "mikuzstd.command"}，而 {@code paper-plugin.yml} 又没声明该权限、
     * 非 OP 默认不持有——于是普通玩家连只读的 status 都用不了。</p>
     */
    @Override
    public String permission() {
        return null;
    }

    private static void showTrainer(CommandSender sender, String label, ZstdPaperTrainer t, int minSamples) {
        if (t == null) {
            sender.sendMessage("§7  " + label + ": §c未初始化");
            return;
        }
        byte[] dict = t.getCurrentDict();
        sender.sendMessage("§7  " + label + ": samples=" + t.getSampleCount() + "/" + minSamples
                + " bytes=" + t.getSampleBytes()
                + " dictId=" + t.getCurrentDictId()
                + " dictSize=" + (dict == null ? 0 : dict.length) + "B");
    }
}
