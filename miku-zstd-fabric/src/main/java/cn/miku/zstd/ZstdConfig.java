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
 * <p>默认压缩等级 <b>3</b>、窗口 <b>2^20</b>（与两端服务端保持一致，改动前先读模板里的实测依据），
 * 配置文件带中文注释。</p>
 *
 * <p>⚠️ 5.0.43 起客户端<b>上行不压缩</b>（只批处理，实测压了反而膨胀 13%），因此
 * {@code level} / {@code window_log} / {@code threads} 三项在本端<b>已不产生任何效果</b>：
 * 它们仍然保留并被读取，只是客户端再没有"压缩这一步"。删不删等下次清配置时统一决定。</p>
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

    /** 压缩等级。5.0.43 起客户端上行不压缩，本项不再影响任何字节（保留以与服务端模板对齐）。 */
    public final int level;
    /** 压缩窗口。同上，客户端当前无压缩步骤，本项仅有构造上下文时的形式意义。 */
    public final int windowLog;

    /** 压缩线程池大小；<=0 表示自动（CPU 核心数 − 1）。客户端当前不再压缩，池实际闲置。 */
    public final int compressThreads;
    public final boolean hudEnabled;

    /**
     * 兜底窗口（毫秒）：仅在"上游只 {@code write} 不 flush"时才轮得到。
     *
     * <p>协议 v7 起攒批边界是上游自己的 {@code flush}（一次 flush 成一帧），
     * 本值不再是"攒包时长"——正常流量下 flush 总是先到，定时器会被取消。详见
     * {@code ZstdBatchEncoderBase} 的类注释。</p>
     */
    public final int batchWindowMs;
    /** 单帧最多合并多少个包 */
    public final int batchMaxPackets;

    /**
     * 小包跳过压缩的阈值：小于此字节数的批次直接直存，不调用 zstd。
     *
     * <p>实测 ≤32B 的批次压缩后膨胀到 102~113%，48B 起压缩才有收益。这个口径是按
     * "上下文里还没有历史字典"测出来的——滚动历史要等本方向攒够 320KB（64KB 滑窗填满
     * + 256KB 轮换间隔）才第一次生效，而客户端上行以几十字节的移动包为主，基本走不到，
     * 所以不再按"有无字典"分两档。</p>
     */
    public final int skipCompressBelowBytes;

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
        this.batchWindowMs = clamp(getInt(c, "batch_window_ms", 50), 0, 1000);
        this.batchMaxPackets = clamp(getInt(c, "batch_max_packets", 64), 1, 4096);
        this.skipCompressBelowBytes = clamp(getInt(c, "skip_compress_below_bytes", 48), 0, 65536);

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
                      # 默认 3。依据是开发期探针的实测（当时代码还带着训练字典，这里只取等级之间的
                      # 相对关系；训练字典已于 5.0.42 删除，滚动历史沿用同一套帧格式）：
                      #   · 小包是滚动历史收益的主战场，而高等级在这里【反而更差】——
                      #     单包 64B：L3 压到 45.3%，L9 只能压到 57.8%，L3 好 12.5 个百分点；
                      #   · 数据越长高等级才越有优势：8KB 批次上 L9 好约 4 个百分点；
                      #   · 但代价悬殊：L9 的压缩耗时是 L3 的 8~12 倍（8KB 批次 173μs vs 14.7μs）；
                      #   · 解压耗时与等级无关。
                      # 结论：默认 3。两端等级可以不同，但帧格式必须一致（由模组自动协商）；
                      # 滚动历史是两端各自按同一批字节算出来的，不需要也不应该协商。
                      # ⚠️ 5.0.43 起客户端上行不压缩（只批处理），【本项对客户端已不生效】：
                      #    上面这些数据讲的是"要压缩时怎么挑等级"，服务端仍然适用；
                      #    客户端保留这个键只是为了与服务端模板保持一致，改它不会改变任何字节。
                      level: 3

                      # 滑动窗口大小，单位为 2 的幂（20 = 1MB）。
                      # 窗口决定"能记住多少历史数据用于跨包匹配"，听起来对大包有帮助，但实测
                      # （开发期探针 WindowProbe，逐值独立 JVM 测量）恰恰相反：
                      #   window_log 17 / 20 / 23 的压缩率【完全相同】——批次 29.5%、大包重复 12.5%。
                      # 原因：小包的重复内容已被【滚动历史】吃掉；大包（区块）多为新内容，本就匹配不到历史。
                      # 而 zstd 上下文的哈希表规模是 2^(windowLog-1) × 4B ——【指数增长】：
                      #   23 ≈ 16MB      20 ≈ 2MB      18 ≈ 0.5MB
                      # 所以默认取 20：内存省一个数量级，压缩率实测无损。
                      # ⚠️ 不要盲目调大——每高一级，本机 native 内存翻倍。
                      # ⚠️ 与 level 一样，5.0.43 起客户端不压缩，本项对客户端不再产生效果。
                      window_log: 20

                      # ── 压缩线程池 ──
                      # 压缩已从 Netty event loop 卸载到线程池，因此【跨连接是全核并行】的
                      #（单个批次仍是单线程——zstd 的 nbWorkers 面向大输入，几 KB 的批次
                      #  用多线程只会被调度开销吃掉）。
                      # 0 = 自动：CPU 核心数 − 1，刻意留一个核给游戏主线程（渲染与逻辑）。
                      # 客户端上压缩线程过多会与渲染抢 CPU，收益还不如让它降级；
                      # 一般保持 0（自动）即可，只有在日志里看到压缩明显排队时才考虑调整。
                      # ⚠️ 5.0.43 起客户端不压缩，这个池实际闲置（线程按需创建，不再是热路径）；
                      #    保留配置项是为了与服务端对齐，也留给"哪天恢复上行压缩"。
                      threads: 0

                      # ── 批处理（协议 v7 起以 flush 为界）──
                      # 一次 flush 成一帧：上游（tick 末尾、区块发送完成、登录期写包）本来就在
                      # 这些时机 flush，帧边界与它们天然对齐，不额外增加任何延迟。
                      # 实测收益很大：每包单独成帧时，帧头与熵表的重复发送几乎吃掉小包的全部收益
                      #（小包甚至会膨胀到 105%，只能直存）；跨帧重复则由滚动历史字典补回来。
                      #
                      # batch_window_ms 只是【兜底】：上游若只 write 不 flush（少数插件会这么干），
                      # 包不能无限期滞留在管线里，到点强制成帧。
                      # ⚠️ 正常流量下这个定时器【永远轮不到】——flush 会先到并把它取消，
                      #    所以它不产生任何额外延迟；调大它也不会"攒更多包"（攒批边界是 flush）。
                      # ⚠️ 取 0 表示"只写即出帧"：这时每个包单独成帧，等于放弃跨包合批，
                      #    只在上游确实从不 flush、又要求最低延迟时才这么设。
                      batch_window_ms: 50

                      # 单帧最多合并多少个包（防止极端流量下内存与延迟失控）。
                      batch_max_packets: 64

                      # ── 小包跳过压缩（实测阈值，别凭感觉改）──
                      # ≤32B 的批次压缩后【膨胀到 102~113%】，48B 起压缩才有收益。
                      # 训练字典删除后只保留这一档：实测"有字典时 24B 起才有收益"那组数据是
                      # 按 128KB 训练字典测的，而滚动历史要等本方向攒够 320KB 才第一次生效，
                      # 客户端的上行流量基本走不到，按 24B 分档等于白留一个永远不命中的开关。
                      skip_compress_below_bytes: 48

                    display:
                      # 客户端 HUD：在屏幕上显示实时压缩统计。
                      # 游戏内按 F8 也可临时开关（不落盘，重启后回到这里的值）。
                      hud_enabled: false

                    """;
            Files.writeString(configPath, yaml, StandardCharsets.UTF_8);
        } catch (IOException e) {
            System.err.println("[Miku-ZSTD] 配置写入失败: " + e);
        }
    }
}
