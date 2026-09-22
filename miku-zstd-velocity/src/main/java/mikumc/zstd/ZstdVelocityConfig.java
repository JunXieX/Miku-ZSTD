package mikumc.zstd;

import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Miku-ZSTD Velocity 配置。
 *
 * <p>默认值面向<b>低带宽占用</b>：zstd 压缩等级 15（显著高于旧默认 9），
 * 配合 8MB 滑动窗口与 LDM 长距离匹配，最大限度降低网络流量。</p>
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
    public final boolean debug;

    /** 协议 v3 批处理窗口（毫秒）：窗口内发往同一玩家的多个包合成一帧 */
    public final int batchWindowMs;
    /** 单帧最多合并多少个包（防止延迟与内存失控） */
    public final int batchMaxPackets;

    /**
     * 无字典时：小于此字节数的批次直接直存，不调用 zstd。
     * 实测（探针 10）无字典时 ≤32B 压缩后膨胀到 102~113%，跳过可同时省 CPU 与线路字节；
     * 48B 起压缩才开始有收益，故默认取 48。
     */
    public final int skipCompressBelowBytes;
    /**
     * 有字典时：阈值更低。实测有字典时 24B 起才有收益（16B 仍膨胀到 103.5%），故默认取 24。
     * 注意不能与无字典共用同一个值——有字典时 48B 能压到 84.8%，跳过会白白丢掉这部分收益。
     */
    public final int skipCompressBelowBytesWithDict;

    public final int trainerMaxSamples;
    public final int trainerMinSamples;
    public final long trainerCooldownMs;
    public final long trainerFallbackTimeoutMs;
    public final int trainerDictMaxBytes;
    public final int trainerSampleTargetBytes;
    public final int trainerMaxHistorySamples;
    public final double trainerAdoptionThreshold;
    public final int trainerPruneMinPayload;

    /** BossBar 文本模板（支持 & 颜色代码与 %players%/%raw%/%wire%/%ratio% 占位符）。 */
    public final String bossbarFormat;

    /**
     * ⚠️ <b>所有数值都经 {@link #clamp(int, int, int)} 夹取</b>，不接受越界值。
     *
     * <p>以前配置原样透传：{@code window_log: 60} 这类越界值会在构造
     * {@code ZstdChannelManager} 时让 zstd-jni 抛异常，而那个构造发生在
     * <b>建连路径</b>上（握手嗅探器 / Paper 的通道初始化）——于是"一个配置打错"
     * 的后果从"zstd 不生效"恶化成"玩家连不上"。夹取把它变回无害。</p>
     *
     * <p>{@code dict_max_bytes} 的上限刻意取 1MB：客户端 {@code loadOne} 会拒绝
     * 超过 0x100000 的字典并回报告"不可用"，那会被当成协议不匹配而整条回落。
     * 夹在这里，越界配置就不会造成"字典白训练"。</p>
     */
    private ZstdVelocityConfig(Map<String, Object> map) {
        Map<String, Object> c = getMap(map, "compression");
        this.level = clamp(getInt(c, "level", 3), 1, 22);
        this.windowLog = clamp(getInt(c, "window_log", 20), 10, 27);
        this.compressThreads = clamp(getInt(c, "threads", 0), 0, 256);
        this.batchWindowMs = clamp(getInt(c, "batch_window_ms", 1), 0, 1000);
        this.batchMaxPackets = clamp(getInt(c, "batch_max_packets", 64), 1, 4096);
        this.skipCompressBelowBytes = clamp(getInt(c, "skip_compress_below_bytes", 48), 0, 65536);
        this.skipCompressBelowBytesWithDict =
                clamp(getInt(c, "skip_compress_below_bytes_with_dict", 24), 0, 65536);

        Map<String, Object> t = getMap(map, "trainer");
        this.trainerMaxSamples = clamp(getInt(t, "max_samples", 10000), 16, 10_000_000);
        // min_samples 不能超过环容量，否则训练永远不会触发（环已满也到不了门槛）
        this.trainerMinSamples = clamp(getInt(t, "min_samples", 2000), 1, this.trainerMaxSamples);
        this.trainerCooldownMs = Math.max(0L, getLong(t, "cooldown_ms", 300000));
        this.trainerFallbackTimeoutMs = Math.max(1000L, getLong(t, "fallback_timeout_ms", 600000));
        this.trainerDictMaxBytes = clamp(getInt(t, "dict_max_bytes", 131072), 1024, 0x100000);
        this.trainerSampleTargetBytes = clamp(getInt(t, "sample_target_bytes", 1048576), 1024, 1 << 30);
        this.trainerMaxHistorySamples = clamp(getInt(t, "max_history_samples", 20000), 0, 10_000_000);
        this.trainerAdoptionThreshold = clampD(getDouble(t, "adoption_threshold", 0.01), 0.0, 1.0);
        this.trainerPruneMinPayload = clamp(getInt(t, "prune_min_payload", 16), 0, 4096);

        Map<String, Object> l = getMap(map, "logging");
        this.debug = getBool(l, "debug");

        Map<String, Object> bb = getMap(map, "bossbar");
        this.bossbarFormat = getString(bb, "format", mikumc.zstd.protocol.ZstdBossBarFormat.DEFAULT_FORMAT);
    }

    private static int clamp(int value, int min, int max) {
        if (value < min) return min;
        return Math.min(value, max);
    }

    private static double clampD(double value, double min, double max) {
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

    private static long getLong(Map<String, Object> map, String key, long def) {
        Object o = map.get(key);
        return o instanceof Number ? ((Number) o).longValue() : def;
    }

    private static double getDouble(Map<String, Object> map, String key, double def) {
        Object o = map.get(key);
        return o instanceof Number ? ((Number) o).doubleValue() : def;
    }

    private static String getString(Map<String, Object> map, String key, String def) {
        Object o = map.get(key);
        return o instanceof String s && !s.isBlank() ? s : def;
    }

    private static boolean getBool(Map<String, Object> map, String key) {
        Object o = map.get(key);
        return o instanceof Boolean ? (Boolean) o : false;
    }

    // ------------------------------------------------------------------
    // 默认配置生成（全中文注释）
    // 注意：本模板仅在配置文件不存在时写入一次，之后用户的修改不会被覆盖。
    // 模板数值必须与上面各 getInt/getLong 的默认值保持一致。
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
                      # ── 压缩等级 (1-22) ──
                      # 默认 3。依据是 tools/levelprobe 的实测（带 128KB 字典、按批处理形态）：
                      #   · 小包是字典收益的主战场，而高等级在这里【反而更差】——
                      #     单包 64B：L3 压到 45.3%，L9 只能压到 57.8%，L3 好 12.5 个百分点；
                      #   · 数据越长高等级才越有优势：8KB 批次上 L9 好约 4 个百分点；
                      #   · 但代价悬殊：L9 的压缩耗时是 L3 的 8~12 倍（8KB 批次 173μs vs 14.7μs）；
                      #     而【解压耗时与等级无关】—— 等级只让压缩端变慢；
                      #   · 对 ping 的影响 < 0.1ms，可以忽略。真正的代价是 CPU：
                      #     CPU 饱和会让压缩线程池排队，那才是延迟抖动的来源。
                      # 结论：默认 3。若大包（区块数据）占比高、CPU 也富余，可调到 6 捡回几个百分点。
                      level: 3

                      # 滑动窗口大小，单位为 2 的幂（20 = 1MB）。
                      # 窗口决定"能记住多少历史数据用于跨包匹配"，听起来对大包有帮助，但实测
                      # （tools/levelprobe/WindowProbe，逐值独立 JVM 测量）恰恰相反：
                      #   window_log 17 / 20 / 23 的压缩率【完全相同】——批次 29.5%、大包重复 12.5%。
                      # 原因：小包的重复内容已被【字典】吃掉；大包（区块）多为新内容，本就匹配不到历史。
                      # 而 zstd 上下文的哈希表规模是 2^(windowLog-1) × 4B ——【指数增长】：
                      #   23 ≈ 16MB      20 ≈ 2MB      18 ≈ 0.5MB
                      # 且这是【每连接】一份（200 人在线就是 GB 级差距）。
                      # 所以默认取 20：内存省一个数量级，压缩率实测无损。
                      # ⚠️ 不要盲目调大——每高一级，每连接的 native 内存翻倍。
                      window_log: 20

                      # ── 压缩线程池 ──
                      # 压缩已从 Netty event loop 卸载到线程池，因此【跨连接是全核并行】的
                      #（单个批次仍是单线程——zstd 的 nbWorkers 面向大输入，几 KB 的批次
                      #  用多线程只会被调度开销吃掉）。
                      # 0 = 自动：CPU 核心数 − 1，刻意留一个核给服务端主逻辑（tick/区块/实体）。
                      # 如果你观察到 /mikuzstd status 里"队列"长期大于 0（压缩跟不上流量），
                      # 才需要考虑加线程或降低 level；盲目调到核心数会让服务端掉 TPS。
                      threads: 0

                      # ── 批处理（协议 v4）──
                      # 把时间窗内发往同一玩家的多个包合成一帧再压缩。
                      # 实测收益很大：每包单独成帧时，帧头与熵表的重复发送几乎吃掉小包的全部收益
                      #（小包甚至会膨胀到 105%，只能直存）；合批后可再省约 34% 线路字节。
                      #
                      # batch_window_ms 是"攒包"的时长，代价是等量的延迟。
                      # ⚠️ 这是【每个包都要等】的固定开销，而 ping 往返会翻倍计：
                      #     3ms → 往返 +6ms；1ms → 往返 +2ms。
                      # 同一 tick 内写的包间隔通常只有几十微秒，1ms 足够把它们合并，
                      # 所以取 1 几乎不损失带宽收益。
                      # ⚠️ 不要设 0：那等于关闭批处理（每包一帧），带宽收益会消失。
                      batch_window_ms: 1

                      # 单帧最多合并多少个包（防止极端流量下内存与延迟失控）。
                      # 批次越大单次压缩越久：实测 32×256B 的批次约 15μs（level 3）。
                      batch_max_packets: 64

                      # ── 小包跳过压缩（实测阈值，别凭感觉改）──
                      # 无字典时：≤32B 的批次压缩后【膨胀到 102~113%】，48B 起压缩才有收益。
                      skip_compress_below_bytes: 48

                      # 有字典时阈值更低：实测 24B 起就有收益（16B 仍膨胀到 103.5%）。
                      # ⚠️ 不要与上面共用一个值 —— 有字典时 48B 能压到 84.8%，用 48 会白丢这块收益。
                      skip_compress_below_bytes_with_dict: 24

                    trainer:
                      # ── 字典自动训练 ──
                      # 字典是【小包收益的主要来源】：实测单包 64B 在无字典时压缩后膨胀到 107.8%，
                      # 带上字典后能压到 45.3%。插件会持续采样真实流量并自动训练字典，
                      # 训练完成后推送给客户端。
                      # 注意口径：字典收益集中在"小包"（数量上占绝大多数），但小包只占总字节
                      # 的一小部分 —— 所以按【总字节】加权后整体收益通常只有 1~3%。

                      # 采样环容量：攒够此数量的样本后触发一次训练
                      max_samples: 10000

                      # 触发训练所需的最少样本数
                      min_samples: 2000

                      # 两次训练之间的最小间隔（毫秒），默认 5 分钟
                      cooldown_ms: 300000

                      # 兜底超时（毫秒）：达到 min_samples 后等待这么久即强制训练
                      fallback_timeout_ms: 600000

                      # 训练出的字典大小上限（字节），默认 128KB
                      dict_max_bytes: 131072

                      # 单轮训练的目标样本总量（字节）
                      sample_target_bytes: 1048576

                      # 跨重启保留的历史样本上限
                      max_history_samples: 20000

                      # 新字典需比旧字典至少提升此比例才会被替换。
                      # ⚠️ 设成 0.03 会让字典在多数服务器上【永远无法被采纳】，整个训练机制等于失效
                      #（字典收益按总字节加权往往只有 1~3%）。首部字典不受此限：只要不劣化就采纳。
                      adoption_threshold: 0.01

                      # 采样时忽略载荷小于此值（字节）的包
                      prune_min_payload: 16

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
                      # 调试日志开关：输出逐包诊断信息（仅在排查问题时开启）
                      debug: false
                    """;
            Files.writeString(configPath, yaml, StandardCharsets.UTF_8);
        } catch (IOException e) {
            System.err.println("[Miku-ZSTD] 配置写入失败: " + e);
        }
    }
}
