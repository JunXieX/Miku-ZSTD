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

    private ZstdConfig(Map<String, Object> map) {
        Map<String, Object> display = section(map, "display");
        this.hudEnabled = getBool(display, "hud_enabled", false);
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
                    """;
            Files.writeString(configPath, yaml, StandardCharsets.UTF_8);
        } catch (IOException e) {
            System.err.println("[Miku-ZSTD] 配置写入失败: " + e);
        }
    }
}
