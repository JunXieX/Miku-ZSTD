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
    @DisplayName("首次真实load：默认关闭debug且生成的YAML也是false")
    void newFileDefaultsToDebugOff() throws IOException {
        Path file = directory.resolve("nested/config.yml");
        assertFalse(Files.exists(file));
        ZstdPaperConfig.load(file);
        assertFalse(ZstdPaperConfig.INSTANCE.debug);
        assertTrue(Files.isRegularFile(file));
        try (Reader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            Map<?, ?> generated = new Yaml().load(reader);
            Map<?, ?> logging = (Map<?, ?>) generated.get("logging");
            assertEquals(Boolean.FALSE, logging.get("debug"));
        }
        String generated = Files.readString(file, StandardCharsets.UTF_8);
        assertFalse(generated.contains("skip_compress_below_bytes"), "新模板不再生成旧直存阈值");
        ZstdPaperConfig.load(file);
        assertFalse(ZstdPaperConfig.INSTANCE.debug);
        assertEquals(generated, Files.readString(file, StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("login 段缺省：应答预算 10s、迟到应答窗口 120s")
    void loginSectionDefaults() throws IOException {
        Path file = directory.resolve("config.yml");
        Files.writeString(file, "logging: {debug: false}\n", StandardCharsets.UTF_8);
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
    @DisplayName("空文件或缺省logging/debug：真实load为false且不覆写已有文件")
    void missingDebugDefaultsOffWithoutRewriting(String contents) throws IOException {
        Path file = directory.resolve("config.yml");
        Files.writeString(file, contents, StandardCharsets.UTF_8);
        for (int i = 0; i < 2; i++) {
            ZstdPaperConfig.load(file);
            assertFalse(ZstdPaperConfig.INSTANCE.debug);
            assertEquals(contents, Files.readString(file, StandardCharsets.UTF_8));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    @DisplayName("显式debug值、旧阈值与未知键在重复真实load后原文保留")
    void explicitDebugAndUserContentsSurviveRepeatedLoad(boolean debug) throws IOException {
        Path file = directory.resolve("config.yml");
        String contents = """
                # 保留用户注释
                compression: {level: 6, skip_compress_below_bytes: 65536}
                logging: {debug: %s}
                custom-key: keep
                """.formatted(debug);
        Files.writeString(file, contents, StandardCharsets.UTF_8);
        for (int i = 0; i < 2; i++) {
            ZstdPaperConfig.load(file);
            assertEquals(debug, ZstdPaperConfig.INSTANCE.debug);
            assertEquals(6, ZstdPaperConfig.INSTANCE.level);
            assertEquals(contents, Files.readString(file, StandardCharsets.UTF_8));
        }
    }
}
