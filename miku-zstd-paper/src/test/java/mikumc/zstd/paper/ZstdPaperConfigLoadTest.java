package mikumc.zstd.paper;

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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ZstdPaperConfigLoadTest {

    @TempDir
    Path directory;

    private ZstdPaperConfig previousConfig;

    @BeforeEach
    void rememberConfig() {
        previousConfig = ZstdPaperConfig.INSTANCE;
    }

    @AfterEach
    void restoreConfig() {
        ZstdPaperConfig.INSTANCE = previousConfig;
    }

    @Test
    @DisplayName("首次真实load：调试日志强制开启，模板里不再生成 debug 键")
    void newTemplateHasNoDebugSwitch() throws IOException {
        Path file = directory.resolve("nested/config.yml");
        assertFalse(Files.exists(file));
        ZstdPaperConfig.load(file);
        // 6.1.1 起调试日志强制开启：没有 debug 开关，console_debug 仍是独立且默认关闭。
        assertFalse(ZstdPaperConfig.INSTANCE.legacyDebugIgnored);
        assertFalse(ZstdPaperConfig.INSTANCE.consoleDebug);
        assertEquals("logs/zstd-debug.log", ZstdPaperConfig.INSTANCE.debugFile);
        assertEquals(8, ZstdPaperConfig.INSTANCE.debugMaxFileMb);
        assertEquals(2, ZstdPaperConfig.INSTANCE.debugKeepFiles);
        assertTrue(Files.isRegularFile(file));
        try (Reader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            Map<?, ?> generated = new Yaml().load(reader);
            Map<?, ?> logging = (Map<?, ?>) generated.get("logging");
            assertFalse(logging.containsKey("debug"), "模板不应再生成 debug 键（它已经不是开关）");
            assertEquals(Boolean.FALSE, logging.get("console_debug"));
            assertEquals("logs/zstd-debug.log", logging.get("file"));
            assertEquals(8, ((Number) logging.get("max_file_mb")).intValue());
            assertEquals(2, ((Number) logging.get("keep_files")).intValue());
        }
        String generated = Files.readString(file, StandardCharsets.UTF_8);
        assertFalse(generated.contains("skip_compress_below_bytes"), "新模板不再生成旧直存阈值");
        ZstdPaperConfig.load(file);
        assertEquals(generated, Files.readString(file, StandardCharsets.UTF_8));
    }

    @ParameterizedTest
    @ValueSource(strings = {"logging: {max_file_mb: 0, keep_files: -5}",
            "logging: {max_file_mb: 999, keep_files: 99}"})
    @DisplayName("logging 段越界值被夹取到合法区间")
    void loggingSectionIsClamped(String contents) throws IOException {
        Path file = directory.resolve("config.yml");
        Files.writeString(file, contents, StandardCharsets.UTF_8);
        ZstdPaperConfig.load(file);
        int megabytes = ZstdPaperConfig.INSTANCE.debugMaxFileMb;
        int keep = ZstdPaperConfig.INSTANCE.debugKeepFiles;
        assertTrue(megabytes >= 1 && megabytes <= 128, "max_file_mb 越界: " + megabytes);
        assertTrue(keep >= 0 && keep <= 10, "keep_files 越界: " + keep);
    }

    @Test
    @DisplayName("显式 console_debug 与自定义路径按原样生效")
    void explicitConsoleDebugAndPathAreHonoured() throws IOException {
        Path file = directory.resolve("config.yml");
        Files.writeString(file,
                "logging: {console_debug: true, file: \"custom/dbg.log\"}\n",
                StandardCharsets.UTF_8);
        ZstdPaperConfig.load(file);
        assertTrue(ZstdPaperConfig.INSTANCE.consoleDebug);
        assertEquals("custom/dbg.log", ZstdPaperConfig.INSTANCE.debugFile);
    }

    @Test
    @DisplayName("旧配置里的 debug: false 只留下提示标记，原文不被重写")
    void legacyDebugFalseIsIgnoredNotRewritten() throws IOException {
        Path file = directory.resolve("config.yml");
        String contents = "logging: {debug: false, keep_files: 5}\n";
        Files.writeString(file, contents, StandardCharsets.UTF_8);
        for (int i = 0; i < 2; i++) {
            ZstdPaperConfig.load(file);
            assertTrue(ZstdPaperConfig.INSTANCE.legacyDebugIgnored, "应识别出旧键并提示一次");
            assertEquals(5, ZstdPaperConfig.INSTANCE.debugKeepFiles, "其它键照常生效");
            assertEquals(contents, Files.readString(file, StandardCharsets.UTF_8), "不得重写用户原文");
        }
    }

    @Test
    @DisplayName("login 段缺省：应答预算 10s、迟到应答窗口 120s")
    void loginSectionDefaults() throws IOException {
        Path file = directory.resolve("config.yml");
        Files.writeString(file, "logging: {console_debug: false}\n", StandardCharsets.UTF_8);
        ZstdPaperConfig.load(file);
        assertEquals(10000, ZstdPaperConfig.INSTANCE.answerTimeoutMs);
        assertEquals(120000, ZstdPaperConfig.INSTANCE.lateAnswerWindowMs);
    }

    @ParameterizedTest
    @ValueSource(strings = {"login: {answer_timeout_ms: 1, late_answer_window_ms: 1}",
            "login: {answer_timeout_ms: 999999, late_answer_window_ms: 9999999}"})
    @DisplayName("login 段越界值被夹取到合法区间")
    void loginSectionIsClamped(String contents) throws IOException {
        Path file = directory.resolve("config.yml");
        Files.writeString(file, contents, StandardCharsets.UTF_8);
        ZstdPaperConfig.load(file);
        int answer = ZstdPaperConfig.INSTANCE.answerTimeoutMs;
        int late = ZstdPaperConfig.INSTANCE.lateAnswerWindowMs;
        assertTrue(answer >= 1000 && answer <= 120000, "answer_timeout_ms 越界: " + answer);
        assertTrue(late >= 5000 && late <= 600000, "late_answer_window_ms 越界: " + late);
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "{}", "compression: {level: 6}", "logging: {}", "logging:"})
    @DisplayName("空文件或缺省 logging 段：不覆写已有文件，也不误判成旧键")
    void missingLoggingSectionDoesNotRewrite(String contents) throws IOException {
        Path file = directory.resolve("config.yml");
        Files.writeString(file, contents, StandardCharsets.UTF_8);
        for (int i = 0; i < 2; i++) {
            ZstdPaperConfig.load(file);
            assertFalse(ZstdPaperConfig.INSTANCE.legacyDebugIgnored);
            assertEquals(contents, Files.readString(file, StandardCharsets.UTF_8));
        }
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
            ZstdPaperConfig.load(file);
            assertEquals(6, ZstdPaperConfig.INSTANCE.level);
            assertTrue(ZstdPaperConfig.INSTANCE.legacyDebugIgnored);
            assertEquals(contents, Files.readString(file, StandardCharsets.UTF_8));
        }
    }
}
