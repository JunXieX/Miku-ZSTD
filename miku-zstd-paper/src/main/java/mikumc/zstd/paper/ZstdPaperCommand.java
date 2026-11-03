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
 * {@code /mikuzstd}：运行状态与 BossBar 实时监控开关。
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
    private static final List<String> SUBCOMMANDS = List.of("bar", "status", "top", "ping", "latency");

    /** ping 回包里的令牌上限与允许字符（只用于标识一次往返，不做任何解释）。 */
    private static final int PING_TOKEN_LIMIT = 32;

    @Override
    public void execute(CommandSourceStack source, String[] args) {
        CommandSender sender = source.getSender();

        if (args.length > 0 && "latency".equalsIgnoreCase(args[0])) {
            // 分位点是累计直方图算出来的：要看"某个测量窗口内"的分布，先 reset 再采样。
            if (args.length > 1 && "reset".equalsIgnoreCase(args[1])) {
                mikumc.zstd.protocol.ZstdLatencyStats.reset();
                sender.sendMessage("§b[Miku-ZSTD]§r 分段耗时样本已清零");
            }
            sender.sendMessage("§7" + ZstdCompressPool.latencyLine());
            return;
        }
        if (args.length > 0 && "ping".equalsIgnoreCase(args[0])) {
            // 只回一行"pong <令牌>"：让调用方用客户端自己的单调时钟量出真实往返，
            // 而不是拿服务端处理耗时或玩家列表里的 ping 冒充。
            String token = args.length > 1 ? sanitizeToken(args[1]) : "-";
            sender.sendMessage("§b[Miku-ZSTD]§r pong " + token);
            return;
        }
        if (args.length > 0 && "top".equalsIgnoreCase(args[0])) {
            sender.sendMessage("§6═══ 各连接下行压缩统计 ═══");
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
        // 平台一行是排查"任务为什么没跑"的第一判据：Paper 上所有调度都在主线程，
        // Folia 上则按区域/实体分派，同一个秒级任务在两边的触发时机完全不同。
        sender.sendMessage("§7  服务端: §b" + MikuZstdPaper.platformName()
                + (MikuZstdPaper.isFolia()
                        ? "§7（区域化多线程：监控任务跟随玩家实体）"
                        : "§7（单主线程）"));
        sender.sendMessage(threshold < 0
                ? "§e  原版压缩阈值 = " + threshold + "（已关闭）→ 仅提供命令与监控，不接管压缩"
                : "§a  原版压缩阈值 = " + threshold + " → zstd 可接管");
        sender.sendMessage("§e── 运行概况 ──");
        sender.sendMessage("§7  压缩等级=" + cfg.level + " 窗口指数=" + cfg.windowLog
                + " 下行批处理=" + cfg.batchWindowMs + "ms/" + cfg.batchMaxPackets + "包");

        sender.sendMessage("§7  在线玩家：" + org.bukkit.Bukkit.getOnlinePlayers().size());
        sender.sendMessage("§7  线程池：" + ZstdCompressPool.summary());
        sender.sendMessage("§7  分段耗时：" + ZstdCompressPool.latencyLine());
        if (ZstdCompressPool.backlogged()) {
            sender.sendMessage("§c  队列积压：压缩跟不上流量，请优先降低压缩等级，其次增加线程数");
        }

        sender.sendMessage("§e── 跨帧历史 ──");
        sender.sendMessage("§7  连续流历史：§a自动§7（同一连接持续压缩，批次随 flush 输出）"
                + " §8无需字典配置或下发");
    }

    /** 把令牌限制成短小的标识符：它只用于匹配一次往返，不需要也没有理由接受任意文本。 */
    private static String sanitizeToken(String raw) {
        StringBuilder out = new StringBuilder(Math.min(raw.length(), PING_TOKEN_LIMIT));
        for (int index = 0; index < raw.length() && out.length() < PING_TOKEN_LIMIT; index++) {
            char c = raw.charAt(index);
            if ((c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
                    || c == '_' || c == '-') {
                out.append(c);
            }
        }
        return out.length() == 0 ? "-" : out.toString();
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
}
