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
 * <p>默认值以<b>实测结论</b>为准，而不是"看起来更狠"：压缩等级 <b>3</b>、窗口 <b>2^20（1MB）</b>。
 * 实测高等级在小包上反而更差（单包 64B：L3 压到 45.3%，L9 只有 57.8%），而耗时是 8~12 倍；
 * 窗口 17/20/23 的压缩率完全相同，但哈希表按 {@code 2^(windowLog-1) × 4B} 指数增长、
 * 且每连接一份。完整依据见下方生成的配置模板注释。</p>
 *
 * <h2>5.0.42：训练字典已整体删除</h2>
 * <p>实机对照显示装上 128KB 训练字典后同一批样本反而多 3.7%，而它唯一可能帮到的小包
 * （&lt;128B）只占总字节 0.1% 且仍然膨胀。跨帧重复改由<b>滚动历史</b>承担
 * （{@link mikumc.zstd.protocol.ZstdRollingDict}，协议内自动维护，没有对应配置项）。
 * 因此 {@code trainer.*} 与 {@code skip_compress_below_bytes_with_dict} 两个配置段落均已删除，
 * 旧配置文件里留着它们也不会报错（未知键被忽略）。</p>
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

    /**
     * 兜底成帧窗口（毫秒）。
     *
     * <p>⚠️ 自协议 v7 起<b>不再是"攒批窗口"</b>：攒批边界就是上游的 {@code flush()}，
     * 一次 flush 成一帧（服务端在 tick 末尾、区块发送完成、登录期写包时都会 flush）。
     * 这个值只在"上游只 {@code write()}、始终不 flush"时才会真正触发——到点强制成帧，
     * 免得包无限期滞留；正常流量下 flush 一定先到，定时器随即被取消。
     * 所以它不再等价于"每包固定延迟"，调大它也不会攒到更多包。</p>
     */
    public final int batchWindowMs;
    /** 单帧最多合并多少个包（防止延迟与内存失控） */
    public final int batchMaxPackets;

    /**
     * 小于此字节数的批次直接直存，不调用 zstd。
     * 实测 ≤32B 压缩后膨胀到 102~113%，48B 起压缩才开始有收益，故默认取 48。
     *
     * <p>⚠️ 5.0.42 删掉训练字典后只剩这一个阈值（原来的
     * {@code skip_compress_below_bytes_with_dict} 已移除）：滚动历史生效后
     * （gen ≥ 1）小包同样能压出收益，但判断口径仍是"这一批有多大"，
     * 不再区分"有没有训练字典"。</p>
     */
    public final int skipCompressBelowBytes;

    /** BossBar 文本模板（支持 & 颜色代码与 %players%/%raw%/%wire%/%ratio% 占位符）。 */
    public final String bossbarFormat;

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
        this.batchMaxPackets = clamp(getInt(c, "batch_max_packets", 64), 1, 4096);
        this.skipCompressBelowBytes = clamp(getInt(c, "skip_compress_below_bytes", 48), 0, 65536);

        Map<String, Object> l = getMap(map, "logging");
        // ⚠️ 5.0.36 起默认开启（与 Paper 端一致）：登录期问题只有 debug 日志能定位。
        this.debug = getBool(l, "debug", true);

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
                      # ── 压缩等级 (1-22) ──
                      # 默认 3。依据是开发期探针的实测（按批处理形态）：
                      #   · 小包是收益的主战场，而高等级在这里【反而更差】——
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
                      # （开发期探针 WindowProbe，逐值独立 JVM 测量）恰恰相反：
                      #   window_log 17 / 20 / 23 的压缩率【完全相同】——批次 29.5%、大包重复 12.5%。
                      # 原因：小包的重复内容已被【滚动历史】吃掉（每方向 64KB 滑窗，协议内自动维护）；
                      # 大包（区块）多为新内容，本就匹配不到历史。
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
                      # 另外协议层还有一道 16B 的硬地板：切换握手包必须保持直存，
                      # 否则帧层嗅探器认不出它（配置调到 16 以下也不会低于这个地板）。
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
                      # 5.0.36 起默认 true —— 登录期的问题只有这些日志能定位；排查完可以改回 false。
                      debug: true
                    """;
            Files.writeString(configPath, yaml, StandardCharsets.UTF_8);
        } catch (IOException e) {
            System.err.println("[Miku-ZSTD] 配置写入失败: " + e);
        }
    }
}
