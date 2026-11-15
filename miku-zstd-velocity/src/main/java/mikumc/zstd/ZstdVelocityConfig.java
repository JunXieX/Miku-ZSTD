package mikumc.zstd;

import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

/**
 * Miku-ZSTD Velocity 配置。
 *
 * <p>默认压缩等级为 3、连续流窗口为 2^20（1MiB）。等级与窗口在新连接初始化时应用；
 * 原生上下文在同一条连接上持续保留历史，不在批次之间装载字典或重置。</p>
 *
 * <h2>3.1.0 修复</h2>
 * <p>旧实现每次 {@code load()} 都无条件调用 {@code writeConfig()}，把配置文件
 * 重置为内置模板——用户手改的等级、窗口与注释每次启动都会丢失
 * （{@code /mikuzstd reload} 同样如此）。现在只在<b>文件不存在</b>时生成模板；
 * 已有文件缺失的键由代码内默认值兜底。</p>
 */
public class ZstdVelocityConfig {

    public static volatile ZstdVelocityConfig INSTANCE = new ZstdVelocityConfig(Map.of());
    private static Path lastConfigPath;

    public final int level;
    public final int windowLog;

    /** 压缩线程池大小；<=0 表示自动（CPU 核心数 − 1，给服务端主逻辑留一个核） */
    public final int compressThreads;

    /**
     * 是否把逐连接诊断写进数据目录下的调试文件（默认<b>开启</b>）。
     *
     * <p>⚠️ 6.1.1 起语义改变：旧实现用反射把 {@code miku-zstd} 调到 DEBUG，即"诊断只进控制台"。
     * 控制台是看告警的地方，每连接几十行会把真问题埋掉；现在诊断一律进文件，
     * 想同时看控制台请开 {@link #consoleDebug}。</p>
     */
    public final boolean debug;

    /** 是否把调试行同时镜像到控制台（默认关闭）。 */
    public final boolean consoleDebug;

    /** 调试文件路径（相对数据目录）。 */
    public final String debugFile;

    /** 单个调试文件的字节上限（MiB）。 */
    public final int debugMaxFileMb;

    /** 调试文件保留的历史份数。 */
    public final int debugKeepFiles;

    /**
     * 是否启用带宽剖析（每 60 秒一段，写进调试文件）。
     *
     * <p>⚠️ 它与 {@link #debug} 分开：剖析是"按帧长区间看字节花在哪"的分析工具，
     * 不是故障诊断；跟着调试开关默认打开会让每个代理都不停地算分档统计。</p>
     */
    public final boolean profiler;

    /**
     * 兜底成帧窗口（毫秒）。
     *
     * <p>⚠️ 自协议 v7 起<b>不再是"攒批窗口"</b>：攒批边界就是上游的 {@code flush()}，
     * 一次 flush 推进一个批次（服务端在 tick 末尾、区块发送完成、登录期写包时都会 flush）。
     * 这个值只在"上游只 {@code write()}、始终不 flush"时才会真正触发——到点强制成帧，
     * 免得包无限期滞留；正常流量下 flush 一定先到，定时器随即被取消。
     * 所以它不再等价于"每包固定延迟"，调大它也不会攒到更多包。</p>
     */
    public final int batchWindowMs;
    /** 单批最多合并多少个包（防止延迟与内存失控） */
    public final int batchMaxPackets;

    /** 每批原始字节预算；0 = 只按包数上限收批（保持旧行为）。 */
    public final int batchMaxBytes;

    /** BossBar 文本模板（支持 & 颜色代码与 %players%/%raw%/%wire%/%ratio% 占位符）。 */
    public final String bossbarFormat;

    /**
     * 等客户端协商应答的预算（毫秒）。
     *
     * <p>⚠️ 旧实现写死 2 秒，且超时直接断开连接。客户端在进服瞬间忙两秒（加载区块、
     * 初次解析整合包资源、一次 GC）就会 miss，代价是整条连接被踢——玩家看到的是
     * "进不去"，两端日志只有一行 Connecting to。放宽到 10 秒后同一次延迟只退回原版。</p>
     */
    public final int answerTimeoutMs;

    /**
     * ⚠️ <b>所有数值都经 {@link #clamp(int, int, int)} 夹取</b>，不接受越界值。
     *
     * <p>以前配置原样透传：{@code window_log: 60} 这类越界值会在构造
     * {@code ZstdChannelManager} 时让 zstd-jni 抛异常，而那个构造发生在
     * <b>建连路径</b>上（握手嗅探器 / Paper 的通道初始化）——于是"一个配置打错"
     * 的后果从"zstd 不生效"恶化成"玩家连不上"。夹取把它变回无害。</p>
     */
    private ZstdVelocityConfig(Map<String, Object> map) {
        Map<String, Object> c = getMap(map, "compression");
        this.level = clamp(getInt(c, "level", 3), 1, 22);
        this.windowLog = clamp(getInt(c, "window_log", 20), 10, 27);
        this.compressThreads = clamp(getInt(c, "threads", 0), 0, 256);
        this.batchWindowMs = clamp(getInt(c, "batch_window_ms", 50), 0, 1000);
        this.batchMaxPackets = clamp(getInt(c, "batch_max_packets", 256), 1, 4096);
        this.batchMaxBytes = clamp(getInt(c, "batch_max_bytes", 0), 0, 16 * 1024 * 1024);

        Map<String, Object> lg = getMap(map, "login");
        this.answerTimeoutMs = clamp(getInt(lg, "answer_timeout_ms", 10000), 1000, 120000);

        Map<String, Object> l = getMap(map, "logging");
        this.debug = getBool(l, "debug", true);
        this.consoleDebug = getBool(l, "console_debug", false);
        this.debugFile = getString(l, "file", "logs/zstd-debug.log");
        this.debugMaxFileMb = clamp(getInt(l, "max_file_mb", 8), 1, 128);
        this.debugKeepFiles = clamp(getInt(l, "keep_files", 2), 0, 10);
        this.profiler = getBool(l, "profiler", false);

        Map<String, Object> bb = getMap(map, "bossbar");
        this.bossbarFormat = getString(bb, "format", mikumc.zstd.protocol.ZstdBossBarFormat.DEFAULT_FORMAT);
    }

    private static int clamp(int value, int min, int max) {
        if (value < min) return min;
        return Math.min(value, max);
    }

    public static void load(Path configPath) {
        lastConfigPath = configPath;
        ZstdVelocityConfig cfg;
        try {
            boolean exists = Files.exists(configPath);
            Map<String, Object> existing = Map.of();
            if (exists) {
                try (Reader r = Files.newBufferedReader(configPath, StandardCharsets.UTF_8)) {
                    Yaml yaml = new Yaml();
                    Map<String, Object> raw = yaml.load(r);
                    if (raw != null) existing = raw;
                }
            }
            cfg = new ZstdVelocityConfig(existing);
            if (!exists) {
                writeConfig(configPath);
            }
        } catch (Exception e) {
            System.err.println("[Miku-ZSTD] 配置加载失败: " + e);
            cfg = new ZstdVelocityConfig(Map.of());
        }
        INSTANCE = cfg;
    }

    public static void reload() {
        if (lastConfigPath != null) load(lastConfigPath);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> getMap(Map<String, Object> parent, String key) {
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

    /** 带默认值的布尔读取（缺失键时用它，而不是硬编码 false）。 */
    private static boolean getBool(Map<String, Object> map, String key, boolean def) {
        Object o = map.get(key);
        return o instanceof Boolean ? (Boolean) o : def;
    }

    // ------------------------------------------------------------------
    // 默认配置生成（全中文注释）
    // 注意：本模板仅在配置文件不存在时写入一次，之后用户的修改不会被覆盖。
    // 模板数值必须与上面各 getInt 的默认值保持一致。
    // ------------------------------------------------------------------

    private static void writeConfig(Path configPath) {
        try {
            Files.createDirectories(configPath.getParent());
            String yaml = """
                    # ──────────────────────────────────────────────────────────────
                    #  Miku-ZSTD — Velocity 端配置文件
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

                      # 每批的原始字节预算，范围 0-16777216；0 = 不启用（只按包数收批）。
                      batch_max_bytes: 0

                    login:
                      # 等客户端协商应答的预算（毫秒，1000-120000，默认 10000）。
                      # 超时后本端退回原版（照常进服，只是这一轮不走 zstd），不再直接断开连接。
                      # 必须显著大于客户端 ZstdLoginNetworking 的看门狗（固定 2000ms）。
                      answer_timeout_ms: 10000

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
                      # ── 调试日志（6.1.1 起默认开启，写文件、不刷控制台）──
                      # 协商、切换确认、扣包/放行、安装锚点这些逐连接诊断会写进下面这个文件。
                      # 出问题时把该文件直接发出来即可，不必再改代理的日志框架。
                      debug: true

                      # 调试文件路径（相对 plugins/Miku-ZSTD/）。
                      file: "logs/zstd-debug.log"

                      # 单个文件上限（MiB，1-128）与保留的历史份数（0-10）。
                      # 超限即滚动：zstd-debug.log -> .1 -> .2 ...，总量有硬上界。
                      max_file_mb: 8
                      keep_files: 2

                      # 是否同时把调试行打进代理控制台。默认关闭：
                      # 控制台留给真正的告警，几十条每连接的行会把它们埋掉。
                      console_debug: false

                      # 带宽剖析（每 60 秒一段，按帧长区间统计字节去向）写进调试文件。
                      # 默认关闭：它是分析工具，不是故障诊断，常开会白白消耗 CPU。
                      profiler: false
                    """;
            Files.writeString(configPath, yaml, StandardCharsets.UTF_8);
        } catch (IOException e) {
            System.err.println("[Miku-ZSTD] 配置写入失败: " + e);
        }
    }
}
