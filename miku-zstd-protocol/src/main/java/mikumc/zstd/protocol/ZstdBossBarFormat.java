package mikumc.zstd.protocol;

/**
 * BossBar 文本模板的<b>共享</b>实现（Paper 端与 Velocity 端共用同一份）。
 *
 * <p>两端的文本必须完全一致，否则同一个模板在两端会长得不一样——所以占位符替换
 * 与带宽格式化只在这里实现一次。颜色码由各端自行处理：Paper 走传统的 {@code §}
 * （见 {@link #colorize}），Velocity 交给 Adventure 的 {@code legacyAmpersand()} 反序列化。</p>
 */
public final class ZstdBossBarFormat {

    /** 默认模板：只用四项 —— 使用人数 / 原始带宽 / zstd 压缩后带宽 / 全服平均压缩率。 */
    public static final String DEFAULT_FORMAT =
            "&a%players% &7人在用 &8| &f%raw% &7\u2192 &a%wire% &8| 压缩率 &e%ratio%";

    private ZstdBossBarFormat() {
    }

    /**
     * 占位符替换（颜色码原样保留，由各端自行处理）。
     *
     * <p>占位符：{@code %players%} 使用人数、{@code %raw%} 原始带宽、
     * {@code %wire%} zstd 后带宽、{@code %ratio%} 全服平均压缩率。
     * 带宽占位符<b>自带单位</b>（如 {@code 1.4MB/s}），模板里不用再补 "/s"；
     * 空闲无流量时 {@code %ratio%} 渲染为 {@code --}。</p>
     */
    public static String substitute(String format, int players, long rawPerSec, long wirePerSec, double ratio) {
        // 无流量时 ratio 的兜底值是 1.0，直接渲染会显示 "100.0%"，容易被误读成"压缩很差"
        boolean idle = rawPerSec == 0 && wirePerSec == 0;
        return format
                .replace("%players%", String.valueOf(players))
                .replace("%raw%", rate(rawPerSec))
                .replace("%wire%", rate(wirePerSec))
                .replace("%ratio%", idle ? "--" : String.format("%.1f%%", ratio * 100.0));
    }

    /**
     * 字节量格式化（<b>十进制</b>：1000B = 1KB）。
     *
     * <p>项目里面向用户显示的 KB / MB / GB 统一走这里。为什么统一用十进制而不是 2 的幂：
     * 这些数字描述的都是<b>网络流量</b>，而带宽按十进制是行业惯例；更要紧的是混用会让
     * 同一个流量在 BossBar 与 {@code /mikuzstd top} 里显示成两个不同的数——
     * 管理员会以为统计出错。历史上这里确实是两套口径（BossBar 用 1000、
     * 连接统计用 1048576），现已收敛。</p>
     */
    public static String fmtBytes(long bytes) {
        if (bytes < 1000) {
            return bytes + "B";
        }
        if (bytes < 1_000_000) {
            return String.format("%.1fKB", bytes / 1000.0);
        }
        if (bytes < 1_000_000_000L) {
            return String.format("%.1fMB", bytes / 1_000_000.0);
        }
        return String.format("%.2fGB", bytes / 1_000_000_000.0);
    }

    /** 带宽格式化（带单位与 /s）。 */
    public static String rate(long bps) {
        return fmtBytes(bps) + "/s";
    }

    /**
     * 把 {@code &} 颜色代码转成 {@code §}（Paper 端用）。
     *
     * <p>自己实现而不用 {@code ChatColor#translateAlternateColorCodes}——后者已废弃，
     * 会引入编译告警；这里只需支持 16 色与常见格式码。</p>
     */
    public static String colorize(String s) {
        final String codes = "0123456789abcdefklmnorABCDEFKLMNOR";
        char[] c = s.toCharArray();
        for (int i = 0; i < c.length - 1; i++) {
            if (c[i] == '&' && codes.indexOf(c[i + 1]) >= 0) {
                c[i] = '\u00a7';
            }
        }
        return new String(c);
    }
}
