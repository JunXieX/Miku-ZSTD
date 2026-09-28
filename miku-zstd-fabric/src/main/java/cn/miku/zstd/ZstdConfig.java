package cn.miku.zstd;

import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

/**
 * Miku-ZSTD 客户端配置。
 *
 * <p>默认压缩等级 <b>3</b>、窗口 <b>2^20</b>（与两端服务端一致，改动前先读模板里的实测依据），
 * 配置文件带中文注释。</p>
 *
 * <h2>3.1.0 修复</h2>
 * <ul>
 *   <li>旧实现每次 {@code load()} 都无条件 {@code writeConfig()}，把配置文件重置为
 *       内置模板——用户手改的等级/窗口与注释每次启动都会丢失。现在只在文件
 *       <b>不存在</b>时生成模板；已有文件缺失的键由代码内默认值兜底。</li>
 *   <li>删除从未被调用的 {@code defaults()}（真实默认值来自各 {@code getInt(..., def)}，
 *       两者并存只会互相漂移）。</li>
 *   <li>{@code hudEnabledRuntime} 不再由实例构造器赋值——改为在 {@link #load} 中
 *       统一初始化，语义更直白。</li>
 * </ul>
 */
public class ZstdConfig {

    public static volatile ZstdConfig INSTANCE = new ZstdConfig(Map.of());

    /** HUD 开关的运行时状态（F8 键位可切换，不落盘） */
    public static volatile boolean hudEnabledRuntime;

    public final int level;
    public final int windowLog;

    /** 压缩线程池大小；<=0 表示自动（CPU 核心数 − 1，给服务端主逻辑留一个核） */
    public final int compressThreads;
    public final boolean debug;
    public final boolean hudEnabled;

    /** 帧格式 v3 的批处理窗口（毫秒）：窗口内发出的多个包合成一帧 */
    public final int batchWindowMs;
    /** 单帧最多合并多少个包 */
    public final int batchMaxPackets;

    /** 无字典时：小于此字节数的批次直接直存（实测 ≤32B 压缩后膨胀，48B 起才有收益） */
    public final int skipCompressBelowBytes;
    /** 有字典时：阈值更低（实测 24B 起才有收益） */
    public final int skipCompressBelowBytesWithDict;

    /**
     * ⚠️ 与服务端同规则：<b>所有数值都夹取</b>。
     *
     * <p>越界的 {@code window_log} 会在构造 {@code ZstdChannelManager} 时让 zstd-jni 抛异常，
     * 而那个构造发生在<b>登录协商的回调里</b>——一个配置打错就会让每次连接都失败。</p>
     */
    private ZstdConfig(Map<String, Object> map) {
        Map<String, Object> c = section(map, "compression");
        this.level = clamp(getInt(c, "level", 3), 1, 22);
        this.windowLog = clamp(getInt(c, "window_log", 20), 10, 27);
        this.compressThreads = clamp(getInt(c, "threads", 0), 0, 256);
        this.batchWindowMs = clamp(getInt(c, "batch_window_ms", 1), 0, 1000);
        this.batchMaxPackets = clamp(getInt(c, "batch_max_packets", 64), 1, 4096);
        this.skipCompressBelowBytes = clamp(getInt(c, "skip_compress_below_bytes", 48), 0, 65536);
        this.skipCompressBelowBytesWithDict =
                clamp(getInt(c, "skip_compress_below_bytes_with_dict", 24), 0, 65536);

        Map<String, Object> l = section(map, "logging");
        this.debug = getBool(l, "debug", false);

        Map<String, Object> d = section(map, "display");
        this.hudEnabled = getBool(d, "hud_enabled", false);
    }

    private static int clamp(int value, int min, int max) {
        if (value < min) return min;
        return Math.min(value, max);
    }

    private static boolean getBool(Map<String, Object> map, String key, boolean def) {
        Object o = map.get(key);
        return o instanceof Boolean ? (Boolean) o : def;
    }

    private static int getInt(Map<String, Object> map, String key, int def) {
        Object o = map.get(key);
        return o instanceof Number ? ((Number) o).intValue() : def;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> section(Map<String, Object> parent, String key) {
        Object o = parent.get(key);
        return o instanceof Map ? (Map<String, Object>) o : Map.of();
    }

    public static void load(Path configPath) {
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
            ZstdConfig cfg = new ZstdConfig(existing);
            hudEnabledRuntime = cfg.hudEnabled;
            if (!exists) {
                writeConfig(configPath);
            }
            INSTANCE = cfg;
        } catch (Exception e) {
            System.err.println("[Miku-ZSTD] 配置加载失败: " + e);
            INSTANCE = new ZstdConfig(Map.of());
            hudEnabledRuntime = INSTANCE.hudEnabled;
        }
    }

    // ── 配置文件生成（全中文注释；只在文件不存在时写入一次）──

    private static void writeConfig(Path configPath) {
        try {
            Files.createDirectories(configPath.getParent());
            String yaml = """
                    # ──────────────────────────────────────────────────────────────
                    #  Miku-ZSTD — 客户端配置
                    #  与服务端的 Miku-ZSTD 插件配合，用 zstd 压缩 Minecraft 网络流量。
                    #
                    #  配置文件位置：<游戏目录>/Miku-ZSTD/config.yml
                    #  （旧版曾在 config/miku_zstd.yml 与 miku-zstd/config.yml，
                    #    首次启动若只找到旧文件会自动搬过来。）
                    #
                    #  本文件只在首次启动时生成，之后不会被模组覆盖；删除它可重新生成默认值，
                    #  已有文件里缺失的键会用代码内默认值兜底。
                    # ──────────────────────────────────────────────────────────────

                    compression:
                      # ── 压缩等级 (1-22) ──
                      # 默认 3。依据是开发期探针的实测（带 128KB 字典、按批处理形态）：
                      #   · 小包是字典收益的主战场，而高等级在这里【反而更差】——
                      #     单包 64B：L3 压到 45.3%，L9 只能压到 57.8%，L3 好 12.5 个百分点；
                      #   · 数据越长高等级才越有优势：8KB 批次上 L9 好约 4 个百分点；
                      #   · 但代价悬殊：L9 的压缩耗时是 L3 的 8~12 倍（8KB 批次 173μs vs 14.7μs）；
                      #   · 解压耗时与等级无关，但客户端【也要压上行流量】，所以等级同样影响上传侧。
                      # 结论：默认 3。两端等级可以不同，但帧格式与字典必须一致（由模组自动协商）。
                      level: 3

                      # 滑动窗口大小，单位为 2 的幂（20 = 1MB）。
                      # 窗口决定"能记住多少历史数据用于跨包匹配"，听起来对大包有帮助，但实测
                      # （开发期探针 WindowProbe，逐值独立 JVM 测量）恰恰相反：
                      #   window_log 17 / 20 / 23 的压缩率【完全相同】——批次 29.5%、大包重复 12.5%。
                      # 原因：小包的重复内容已被【字典】吃掉；大包（区块）多为新内容，本就匹配不到历史。
                      # 而 zstd 上下文的哈希表规模是 2^(windowLog-1) × 4B ——【指数增长】：
                      #   23 ≈ 16MB      20 ≈ 2MB      18 ≈ 0.5MB
                      # 所以默认取 20：内存省一个数量级，压缩率实测无损。
                      # ⚠️ 不要盲目调大——每高一级，本机 native 内存翻倍。
                      window_log: 20

                      # ── 压缩线程池 ──
                      # 压缩已从 Netty event loop 卸载到线程池，因此【跨连接是全核并行】的
                      #（单个批次仍是单线程——zstd 的 nbWorkers 面向大输入，几 KB 的批次
                      #  用多线程只会被调度开销吃掉）。
                      # 0 = 自动：CPU 核心数 − 1，刻意留一个核给游戏主线程（渲染与逻辑）。
                      # 客户端上压缩线程过多会与渲染抢 CPU，收益还不如让它降级；
                      # 一般保持 0（自动）即可，只有在日志里看到压缩明显排队时才考虑调整。
                      threads: 0

                      # ── 批处理（帧格式 v3 引入）──
                      # 把时间窗内发出的多个包合成一帧再压缩。
                      # 实测收益很大：每包单独成帧时，帧头与熵表的重复发送几乎吃掉小包的全部收益
                      #（小包甚至会膨胀到 105%，只能直存）；合批后可再省约 34% 线路字节。
                      #
                      # batch_window_ms 是"攒包"的时长，代价是等量的延迟。
                      # ⚠️ 这是【每个包都要等】的固定开销，ping 往返会翻倍计：
                      #     3ms → 往返 +6ms；1ms → 往返 +2ms。取 1 几乎不损失带宽收益。
                      # ⚠️ 不要设 0：那等于关闭批处理（每包一帧），带宽收益会消失。
                      batch_window_ms: 1

                      # 单帧最多合并多少个包（防止极端流量下内存与延迟失控）。
                      batch_max_packets: 64

                      # ── 小包跳过压缩（实测阈值，别凭感觉改）──
                      # 无字典时：≤32B 的批次压缩后【膨胀到 102~113%】，48B 起压缩才有收益。
                      skip_compress_below_bytes: 48

                      # 有字典时阈值更低：实测 24B 起就有收益（16B 仍膨胀到 103.5%）。
                      # ⚠️ 不要与上面共用一个值，否则会白丢这块收益。
                      skip_compress_below_bytes_with_dict: 24

                    display:
                      # 客户端 HUD：在屏幕上显示实时压缩统计。
                      # 游戏内按 F8 也可临时开关（不落盘，重启后回到这里的值）。
                      hud_enabled: false

                    logging:
                      # 调试日志开关：输出逐包收发诊断（仅在排查问题时开启，会刷很多行）
                      debug: false
                    """;
            Files.writeString(configPath, yaml, StandardCharsets.UTF_8);
        } catch (IOException e) {
            System.err.println("[Miku-ZSTD] 配置写入失败: " + e);
        }
    }
}
