package mikumc.zstd;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.Reader;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ZstdVelocityConfigLoadTest {

    @TempDir
    Path directory;

    private ZstdVelocityConfig previousConfig;
    private Field configPathField;
    private Path previousPath;

    @BeforeEach
    void rememberConfig() throws ReflectiveOperationException {
        previousConfig = ZstdVelocityConfig.INSTANCE;
        configPathField = ZstdVelocityConfig.class.getDeclaredField("lastConfigPath");
        configPathField.setAccessible(true);
        previousPath = (Path) configPathField.get(null);
    }

    @AfterEach
    void restoreConfig() throws ReflectiveOperationException {
        ZstdVelocityConfig.INSTANCE = previousConfig;
        configPathField.set(null, previousPath);
    }

    @Test
    @DisplayName("首次真实load：调试日志默认开启，模板带 debug 开关")
    void newTemplateHasDebugSwitch() throws IOException {
        Path file = directory.resolve("nested/config.yml");
        assertFalse(Files.exists(file));
        ZstdVelocityConfig.load(file);
        // 默认开启（不写也给日志），关掉要显式写 false；控制台与剖析各有独立开关。
        assertTrue(ZstdVelocityConfig.INSTANCE.debug);
        assertFalse(ZstdVelocityConfig.INSTANCE.consoleDebug);
        assertFalse(ZstdVelocityConfig.INSTANCE.profiler);
        assertEquals("logs/zstd-debug.log", ZstdVelocityConfig.INSTANCE.debugFile);
        assertEquals(8, ZstdVelocityConfig.INSTANCE.debugMaxFileMb);
        assertEquals(2, ZstdVelocityConfig.INSTANCE.debugKeepFiles);
        assertTrue(Files.isRegularFile(file));
        try (Reader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            Map<?, ?> generated = new Yaml().load(reader);
            Map<?, ?> logging = (Map<?, ?>) generated.get("logging");
            assertEquals(Boolean.TRUE, logging.get("debug"), "模板必须带开关且默认开启");
            assertEquals(Boolean.FALSE, logging.get("console_debug"));
            assertEquals(Boolean.FALSE, logging.get("profiler"));
            assertEquals("logs/zstd-debug.log", logging.get("file"));
            assertEquals(8, ((Number) logging.get("max_file_mb")).intValue());
            assertEquals(2, ((Number) logging.get("keep_files")).intValue());
            Map<?, ?> diagnostics = (Map<?, ?>) generated.get("diagnostics");
            assertNotNull(diagnostics, "模板应生成 diagnostics 段");
            assertEquals(Boolean.TRUE, diagnostics.get("enabled"));
            assertEquals(15000, ((Number) diagnostics.get("stall_detect_ms")).intValue());
            assertEquals(Boolean.TRUE, diagnostics.get("pull_client_log"));
            assertEquals(Boolean.TRUE, diagnostics.get("preflight"));
            assertEquals(256, ((Number) diagnostics.get("max_kb")).intValue());
            assertEquals(20, ((Number) diagnostics.get("keep_bundles")).intValue());
        }
        String generated = Files.readString(file, StandardCharsets.UTF_8);
        assertFalse(generated.contains("skip_compress_below_bytes"), "新模板不再生成旧直存阈值");
        ZstdVelocityConfig.load(file);
        assertEquals(generated, Files.readString(file, StandardCharsets.UTF_8));
    }

    @ParameterizedTest
    @ValueSource(strings = {"logging: {max_file_mb: 0, keep_files: -5}",
            "logging: {max_file_mb: 999, keep_files: 99}"})
    @DisplayName("logging 段越界值被夹取到合法区间")
    void loggingSectionIsClamped(String contents) throws IOException {
        Path file = directory.resolve("config.yml");
        Files.writeString(file, contents, StandardCharsets.UTF_8);
        ZstdVelocityConfig.load(file);
        int megabytes = ZstdVelocityConfig.INSTANCE.debugMaxFileMb;
        int keep = ZstdVelocityConfig.INSTANCE.debugKeepFiles;
        assertTrue(megabytes >= 1 && megabytes <= 128, "max_file_mb 越界: " + megabytes);
        assertTrue(keep >= 0 && keep <= 10, "keep_files 越界: " + keep);
    }

    @Test
    @DisplayName("profiler 开关与 console_debug 各自独立生效")
    void profilerAndConsoleDebugAreIndependent() throws IOException {
        Path file = directory.resolve("config.yml");
        Files.writeString(file,
                "logging: {console_debug: true, profiler: true}\n",
                StandardCharsets.UTF_8);
        ZstdVelocityConfig.load(file);
        assertTrue(ZstdVelocityConfig.INSTANCE.consoleDebug);
        assertTrue(ZstdVelocityConfig.INSTANCE.profiler);
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "{}", "compression: {level: 6}", "logging: {}", "logging:"})
    @DisplayName("空文件或缺省 logging 段：不覆写已有文件，也不误判成旧键")
    void missingLoggingSectionDoesNotRewrite(String contents) throws IOException {
        Path file = directory.resolve("config.yml");
        Files.writeString(file, contents, StandardCharsets.UTF_8);
        for (int i = 0; i < 2; i++) {
            ZstdVelocityConfig.load(file);
            assertTrue(ZstdVelocityConfig.INSTANCE.debug, "缺 logging 段时按默认开启");
            assertEquals(contents, Files.readString(file, StandardCharsets.UTF_8));
        }
    }

    @Test
    @DisplayName("debug: false 真的关掉日志，原文不被重写")
    void debugFalseIsHonouredNotRewritten() throws IOException {
        Path file = directory.resolve("config.yml");
        String contents = "logging: {debug: false, keep_files: 5}\n";
        Files.writeString(file, contents, StandardCharsets.UTF_8);
        for (int i = 0; i < 2; i++) {
            ZstdVelocityConfig.load(file);
            assertFalse(ZstdVelocityConfig.INSTANCE.debug, "关闭必须生效，不能只是提示一句");
            assertEquals(5, ZstdVelocityConfig.INSTANCE.debugKeepFiles, "其它键照常生效");
            assertEquals(contents, Files.readString(file, StandardCharsets.UTF_8), "不得重写用户原文");
        }
    }

@Test
    @DisplayName("diagnostics 段缺省：开启取证、15s 判定、预取开启、256KiB 尾、保留 20 组")
    void diagnosticsSectionDefaults() throws IOException {
        Path file = directory.resolve("config.yml");
        Files.writeString(file, "logging: {}\n", StandardCharsets.UTF_8);
        ZstdVelocityConfig.load(file);
        assertTrue(ZstdVelocityConfig.INSTANCE.diagnosticsEnabled);
        assertTrue(ZstdVelocityConfig.INSTANCE.pullClientLog);
        assertTrue(ZstdVelocityConfig.INSTANCE.preflight);
        assertEquals(15000, ZstdVelocityConfig.INSTANCE.stallDetectMs);
        assertEquals(256, ZstdVelocityConfig.INSTANCE.diagMaxKb);
        assertEquals(20, ZstdVelocityConfig.INSTANCE.keepBundles);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "diagnostics: {stall_detect_ms: 1, max_kb: 1, keep_bundles: 0}",
            "diagnostics: {stall_detect_ms: 999999, max_kb: 99999, keep_bundles: 9999}"})
    @DisplayName("diagnostics 段越界值被夹取到合法区间")
    void diagnosticsSectionIsClamped(String contents) throws IOException {
        Path file = directory.resolve("config.yml");
        Files.writeString(file, contents, StandardCharsets.UTF_8);
        ZstdVelocityConfig.load(file);
        int stall = ZstdVelocityConfig.INSTANCE.stallDetectMs;
        int kb = ZstdVelocityConfig.INSTANCE.diagMaxKb;
        int keep = ZstdVelocityConfig.INSTANCE.keepBundles;
        assertTrue(stall >= 5000 && stall <= 120000, "stall_detect_ms 越界: " + stall);
        assertTrue(kb >= 16 && kb <= 1024, "max_kb 越界: " + kb);
        assertTrue(keep >= 1 && keep <= 100, "keep_bundles 越界: " + keep);
    }

    @Test
    @DisplayName("旧阈值与未知键在重复真实load后原文保留")
    void userContentsSurviveRepeatedLoad() throws IOException {
        Path file = directory.resolve("config.yml");
        String contents = """
                # 保留用户注释
                compression: {level: 6, skip_compress_below_bytes: 65536}
                logging: {debug: false}
                custom-key: keep
                """;
        Files.writeString(file, contents, StandardCharsets.UTF_8);
        for (int i = 0; i < 2; i++) {
            ZstdVelocityConfig.load(file);
            assertEquals(6, ZstdVelocityConfig.INSTANCE.level);
            assertFalse(ZstdVelocityConfig.INSTANCE.debug, "logging.debug: false 要真的关掉");
            assertEquals(contents, Files.readString(file, StandardCharsets.UTF_8));
        }
    }
}
