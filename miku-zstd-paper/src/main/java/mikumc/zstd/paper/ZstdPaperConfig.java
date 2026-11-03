package mikumc.zstd.paper;

import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

/**
 * Miku-ZSTD Paper 端配置。
 *
 * <p>与 Velocity 端保持同一套默认值（等级 3 / 窗口 2^20）——两端各自与同一个客户端协商，
 * 行为必须一致。等级与窗口只在新连接初始化时应用，流存续期间不重设。</p>
 *
 * <p>配置只在文件不存在时生成模板——绝不覆盖用户手改的内容。</p>
 */
public class ZstdPaperConfig {

    public static volatile ZstdPaperConfig INSTANCE = new ZstdPaperConfig(Map.of());

    /** BossBar 默认文本模板（与 Velocity 端共用 {@code ZstdBossBarFormat} 里的同一份）。 */
    public static final String DEFAULT_BOSSBAR_FORMAT = mikumc.zstd.protocol.ZstdBossBarFormat.DEFAULT_FORMAT;

    public final int level;
    public final int windowLog;

    /** 压缩线程池大小；<=0 表示自动（CPU 核心数 − 1，给服务端主逻辑留一个核） */
    public final int compressThreads;
    public final int batchWindowMs;
    public final int batchMaxPackets;
    public final boolean debug;

    /** BossBar 文本模板（支持 & 颜色代码与 %players%/%raw%/%wire%/%ratio% 占位符）。 */
    public final String bossbarFormat;

    /**
     * ⚠️ 与 Velocity 端同规则：<b>所有数值都夹取</b>，不接受越界值。
     *
     * <p>越界的 {@code window_log} 会在构造 {@code ZstdPaperChannelManager} 时让
     * zstd-jni 抛异常，而那个构造发生在<b>通道初始化路径</b>上——
     * 一个配置打错就会让每条新连接都失败。</p>
     */
    private ZstdPaperConfig(Map<String, Object> map) {
        Map<String, Object> c = section(map, "compression");
        this.level = clamp(getInt(c, "level", 3), 1, 22);
        this.windowLog = clamp(getInt(c, "window_log", 20), 10, 27);
        this.compressThreads = clamp(getInt(c, "threads", 0), 0, 256);
        this.batchWindowMs = clamp(getInt(c, "batch_window_ms", 50), 0, 1000);
        this.batchMaxPackets = clamp(getInt(c, "batch_max_packets", 256), 1, 4096);

        Map<String, Object> l = section(map, "logging");
        this.debug = getBool(l, "debug", false);

        Map<String, Object> bb = section(map, "bossbar");
        this.bossbarFormat = getString(bb, "format", DEFAULT_BOSSBAR_FORMAT);
    }

    private static int clamp(int value, int min, int max) {
        if (value < min) return min;
        return Math.min(value, max);
    }

    public static void load(Path configPath) {
        try {
            boolean exists = Files.exists(configPath);
            Map<String, Object> existing = Map.of();
            if (exists) {
                try (Reader r = Files.newBufferedReader(configPath, StandardCharsets.UTF_8)) {
                    Map<String, Object> raw = new Yaml().load(r);
                    if (raw != null) existing = raw;
                }
            }
            ZstdPaperConfig cfg = new ZstdPaperConfig(existing);
            if (!exists) writeConfig(configPath);
            INSTANCE = cfg;
        } catch (Exception e) {
            System.err.println("[Miku-ZSTD] 配置加载失败: " + e);
            INSTANCE = new ZstdPaperConfig(Map.of());
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> section(Map<String, Object> parent, String key) {
        Object o = parent.get(key);
        return o instanceof Map ? (Map<String, Object>) o : Map.of();
    }

    private static int getInt(Map<String, Object> map, String key, int def) {
        Object o = map.get(key);
        return o instanceof Number ? ((Number) o).intValue() : def;
    }

    private static String getString(Map<String, Object> map, String key, String def) {
        Object o = map.get(key);
        return o instanceof String s && !s.isBlank() ? s : def;
    }

    private static boolean getBool(Map<String, Object> map, String key, boolean def) {
        Object o = map.get(key);
        return o instanceof Boolean ? (Boolean) o : def;
    }

    private static void writeConfig(Path configPath) {
        try {
            Files.createDirectories(configPath.getParent());
            String yaml = """
                    # ──────────────────────────────────────────────────────────────
                    #  Miku-ZSTD — Paper 端配置文件
                    #  用 zstd 替代原版 zlib 压缩玩家连接的网络流量（需客户端安装 Miku-ZSTD 模组）
                    #  本文件只在首次启动时生成，之后不会被插件覆盖；删除它可重新生成默认值，
                    #  已有文件里缺失的键会用代码内默认值兜底。
                    # ──────────────────────────────────────────────────────────────

                    compression:
                      # 压缩等级 1-22；默认 3。等级越高，压缩端计算量通常越大。
                      # 参数仅在新连接的压缩上下文初始化时应用。
                      level: 3

                      # 连续流的历史窗口指数，范围 10-27（20 = 1MiB）。
                      # 原生上下文持续保留历史；调大窗口会增加每连接内存开销。
                      window_log: 20

                      # 跨连接共享压缩线程池，同一连接只推进一个批次。
                      # 0 = 自动：可用 CPU 核心数减 1，至少 1 个线程。
                      threads: 0

                      # 上游 flush 时将待发包送入同一条连续 Zstd 流并 FLUSH 输出。
                      # 批次不是独立 Zstd 帧，大输出可拆成多个 Minecraft 传输帧。
                      # 此项只用于上游只 write 不 flush 的兜底，范围 0-1000 毫秒。
                      # 0 = 写入后即启动批次；不在批次之间重置上下文。
                      batch_window_ms: 50

                      # 每批最多合并的包数，范围 1-4096；不是传输帧内的包数上限。
                      batch_max_packets: 256

                    bossbar:
                      # ── BossBar 实时监控文本（/mikuzstd bar 开关）──
                      # 支持 & 颜色代码，以及以下占位符：
                      #   %players%  使用 zstd 通道的玩家数
                      #   %raw%      原始带宽（未压缩口径，自带单位，如 1.4MB/s）
                      #   %wire%     zstd 压缩后的带宽（自带单位）
                      #   %ratio%    全服平均 zstd 压缩率（如 17.6%；空闲无流量时显示 --）
                      # 注意：整行必须用引号包住，否则开头的 & 会被 YAML 当成锚点。
                      format: "&a%players% &7人在用 &8| &f%raw% &7\u2192 &a%wire% &8| 压缩率 &e%ratio%"

                    logging:
                      # 调试日志开关：输出逐包诊断信息（协商、切换确认、扣包/放行、安装锚点）
                      # 默认关闭；排查问题时再开启，已有配置不会被自动覆盖。
                      debug: false
                    """;
            Files.writeString(configPath, yaml, StandardCharsets.UTF_8);
        } catch (IOException e) {
            System.err.println("[Miku-ZSTD] 配置写入失败: " + e);
        }
    }
}
