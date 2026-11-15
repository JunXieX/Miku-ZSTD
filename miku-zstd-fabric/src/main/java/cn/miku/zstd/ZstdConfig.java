package cn.miku.zstd;

import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

/**
 * 客户端显示配置；上行沿用原版，不再配置 zstd 压缩或批处理。
 * 旧配置中的 compression 段会被忽略，已有文件保持原样。
 */
public class ZstdConfig {

    public static volatile ZstdConfig INSTANCE = new ZstdConfig(Map.of());

    /** HUD 开关的运行时状态（F8 键位可切换，不落盘）。 */
    public static volatile boolean hudEnabledRuntime;

    public final boolean hudEnabled;

    /**
     * 旧配置里是否显式写着 {@code logging.debug=false}。
     *
     * <p>⚠️ 6.1.1 起客户端调试日志也<b>强制开启</b>：客户端没有"服务端控制台"，
     * 而 {@code latest.log} 同样不是给玩家看的东西——诊断一律进独立的小文件，
     * 出问题时玩家直接把这个文件发出来即可。这个字段不参与行为，只用于启动时一行提示。</p>
     */
    public final boolean legacyDebugIgnored;

    /** 调试文件路径（相对游戏目录）。 */
    public final String debugFile;

    /** 单个调试文件的字节上限（MiB）。 */
    public final int debugMaxFileMb;

    /** 调试文件保留的历史份数。 */
    public final int debugKeepFiles;

    /**
     * 服务端判定"卡住"并索取日志时，是否把本机调试日志尾部回传。
     *
     * <p>默认开：这条通道只在玩家已经进不去服的时候才会被用到，回传的是本机调试文件尾部
     * （不含聊天、不含账号信息），正是唯一能定位问题的那份东西。</p>
     */
    public final boolean sendClientLog;

    /** 单次回传的尾部上限（KiB）。服务端的 {@code diagnostics.max_kb} 不应小于它。 */
    public final int diagMaxKb;

    private ZstdConfig(Map<String, Object> map) {
        Map<String, Object> display = section(map, "display");
        this.hudEnabled = getBool(display, "hud_enabled", false);

        Map<String, Object> logging = section(map, "logging");
        this.legacyDebugIgnored = Boolean.FALSE.equals(logging.get("debug"));
        this.debugFile = getString(logging, "file", "Miku-ZSTD/logs/zstd-debug.log");
        this.debugMaxFileMb = clamp(getInt(logging, "max_file_mb", 8), 1, 128);
        this.debugKeepFiles = clamp(getInt(logging, "keep_files", 2), 0, 10);

        Map<String, Object> diagnostics = section(map, "diagnostics");
        this.sendClientLog = getBool(diagnostics, "send_client_log", true);
        this.diagMaxKb = clamp(getInt(diagnostics, "max_kb", 256), 16, 512);
    }

    private static int clamp(int value, int min, int max) {
        if (value < min) return min;
        return Math.min(value, max);
    }

    private static int getInt(Map<String, Object> map, String key, int def) {
        Object value = map.get(key);
        return value instanceof Number ? ((Number) value).intValue() : def;
    }

    private static String getString(Map<String, Object> map, String key, String def) {
        Object value = map.get(key);
        return value instanceof String s && !s.isBlank() ? s : def;
    }

    private static boolean getBool(Map<String, Object> map, String key, boolean def) {
        Object value = map.get(key);
        return value instanceof Boolean ? (Boolean) value : def;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> section(Map<String, Object> parent, String key) {
        Object value = parent.get(key);
        return value instanceof Map ? (Map<String, Object>) value : Map.of();
    }

    public static void load(Path configPath) {
        try {
            boolean exists = Files.exists(configPath);
            Map<String, Object> existing = Map.of();
            if (exists) {
                try (Reader reader = Files.newBufferedReader(configPath, StandardCharsets.UTF_8)) {
                    Map<String, Object> raw = new Yaml().load(reader);
                    if (raw != null) existing = raw;
                }
            }
            ZstdConfig cfg = new ZstdConfig(existing);
            hudEnabledRuntime = cfg.hudEnabled;
            if (!exists) writeConfig(configPath);
            INSTANCE = cfg;
        } catch (Exception e) {
            System.err.println("[Miku-ZSTD] 配置加载失败: " + e);
            INSTANCE = new ZstdConfig(Map.of());
            hudEnabledRuntime = INSTANCE.hudEnabled;
        }
    }

    /** 仅在配置不存在时生成；旧文件中的显示设置与注释保持原样。 */
    private static void writeConfig(Path configPath) {
        try {
            Files.createDirectories(configPath.getParent());
            String yaml = """
                    # Miku-ZSTD — 客户端配置
                    # 与服务端插件配合解压 zstd 下行；上行完全沿用原版压缩、长度前缀与加密。
                    # 配置位置：<游戏目录>/Miku-ZSTD/config.yml
                    # 本文件只在首次启动时生成，已有文件不会被覆盖。
                    # 旧配置中的 compression 段已停用，保留该段也不会影响连接。

                    display:
                      # HUD 显示下行速率与压缩率。
                      # 游戏内按 F8 可临时开关（不落盘，重启后恢复此设置）。
                      hud_enabled: false

                    logging:
                      # ── 调试日志（强制开启，写文件、不进 latest.log）──
                      # 记协商、看门狗、切换确认、管线安装这些逐连接诊断。
                      # 连不上服/卡在连接协商时，把下面这个文件发出来即可。
                      # ⚠️ 没有开关：旧版这里的 debug 键已移除，写了不生效。

                      # 文件路径（相对游戏目录）。默认 <游戏目录>/Miku-ZSTD/logs/zstd-debug.log
                      file: "Miku-ZSTD/logs/zstd-debug.log"

                      # 单个文件上限（MiB，1-128）与保留的历史份数（0-10）。
                      max_file_mb: 8
                      keep_files: 2

                    diagnostics:
                      # ── 卡住取证（6.1.1）──
                      # 服务端发现你"名字进去了、人没进来"时，会向本机索取调试日志尾部。
                      # 一条连接最多被问两次（登录后预取一次 + 判定卡住时一次），
                      # 只发上面那个调试文件的尾部，不含聊天与账号信息。
                      send_client_log: true

                      # 单次回传的尾部上限（KiB，16-512）。
                      max_kb: 256
                    """;
            Files.writeString(configPath, yaml, StandardCharsets.UTF_8);
        } catch (IOException e) {
            System.err.println("[Miku-ZSTD] 配置写入失败: " + e);
        }
    }
}
