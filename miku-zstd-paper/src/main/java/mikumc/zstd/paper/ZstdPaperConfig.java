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
 * 行为必须一致。改动默认值前请先读生成的配置模板里的实测依据。</p>
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
    public final int skipCompressBelowBytes;
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
        this.batchMaxPackets = clamp(getInt(c, "batch_max_packets", 64), 1, 4096);
        this.skipCompressBelowBytes = clamp(getInt(c, "skip_compress_below_bytes", 48), 0, 65536);

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
                    #  5.0.42 删除了训练字典，trainer.* 与 skip_compress_below_bytes_with_dict
                    #  已不再读取：旧文件里留着这些键不会报错，只是被忽略。
                    # ──────────────────────────────────────────────────────────────

                    compression:
                      # ── 压缩等级 (1-22) ──
                      # 默认 3。开发期探针（按批处理形态）的实测结论：
                      #   · 数据越长，高等级才越有优势：8KB 批次上 L9 只比 L3 好约 4 个百分点；
                      #   · 代价悬殊：L9 的压缩耗时是 L3 的 8~12 倍（8KB 批次 173μs vs 14.7μs），
                      #     而【解压耗时与等级无关】—— 等级只让压缩端变慢；
                      #   · 对 ping 的影响 < 0.1ms，可以忽略。真正的代价是 CPU：
                      #     CPU 饱和会让压缩线程池排队，那才是延迟抖动的来源。
                      # 5.0.42 删掉训练字典后，几十字节的小包已统一走直存
                      #（见 skip_compress_below_bytes），高等级能发挥的只剩大帧，更不值得拿 CPU 去换。
                      # 结论：默认 3；若大包（区块数据）占比高、CPU 也富余，可调到 6 捡回几个百分点。
                      level: 3

                      # 滑动窗口大小，单位为 2 的幂（20 = 1MB）。
                      # 窗口决定"一帧之内能记住多少历史数据用于匹配"，听起来对大帧有帮助，但实测
                      # （开发期探针 WindowProbe，逐值独立 JVM 测量）恰恰相反：
                      #   window_log 17 / 20 / 23 的压缩率【完全相同】——批次 29.5%、大包重复 12.5%。
                      # 原因：大帧（区块）多为新内容，本就匹配不到上下文里的历史；而跨帧的重复
                      # 由滚动历史那份 64KB 兜住，与这个窗口无关。
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

                      # ── 批处理（协议 v7 起：攒批边界 = flush）──
                      # 一次 flush 之前攒下的包合成一帧再压缩。实测收益很大：每包单独成帧时，
                      # 帧头与熵表的重复发送几乎吃掉小包的全部收益（小包甚至会膨胀到 105%，
                      # 只能直存）；帧边界对齐 flush 之后，跨帧的滚动历史字典再把这一帧接回
                      # 上一帧的匹配（协议内自动维护，这里没有对应配置项）。
                      #
                      # 攒批边界用的是上游本来就在发生的 flush（tick 末尾、区块发送完成、
                      # 登录期写包时服务端都会 flush），所以合批本身【不额外增加延迟】。
                      #
                      # batch_window_ms 因此只是【兜底】：只有"上游只 write、始终不 flush"的
                      # 调用方才轮得到它——包不能无限期滞留，到点强制成帧。正常流量下 flush
                      # 一定比它先到，定时器随即被取消，一次也不会触发。
                      # ⚠️ 不要为了"多攒一点"把它调大：那只会把兜底变成常态延迟，而 flush
                      # 路径根本读不到这个值。
                      # 0 = 写即成帧（连兜底都不要），仅在确认上游都会 flush 时才可设。
                      batch_window_ms: 50

                      # 单帧最多合并多少个包（防止极端流量下内存与延迟失控）。
                      # 批次越大单次压缩越久：实测 32×256B 的批次约 15μs（level 3）。
                      batch_max_packets: 64

                      # ── 小包跳过压缩（实测阈值，别凭感觉改）──
                      # ≤32B 的批次压缩后【膨胀到 102~113%】，48B 起压缩才有收益。
                      # 5.0.42 起只有这一个阈值：滚动历史只提供内容匹配、不提供熵表，
                      # 几十字节的包照样压不动，原来"有字典 24B"那一档随训练字典一起删除了。
                      # 低于 16B 的硬下限由共享模块兜住（那种包连 zstd 都不会调）。
                      skip_compress_below_bytes: 48

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
