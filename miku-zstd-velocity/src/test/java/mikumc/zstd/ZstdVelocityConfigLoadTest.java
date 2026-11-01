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
    @DisplayName("首次真实load：默认关闭debug且生成的YAML也是false")
    void newFileDefaultsToDebugOff() throws IOException {
        Path file = directory.resolve("nested/config.yml");
        assertFalse(Files.exists(file));
        ZstdVelocityConfig.load(file);
        assertFalse(ZstdVelocityConfig.INSTANCE.debug);
        assertTrue(Files.isRegularFile(file));
        try (Reader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            Map<?, ?> generated = new Yaml().load(reader);
            Map<?, ?> logging = (Map<?, ?>) generated.get("logging");
            assertEquals(Boolean.FALSE, logging.get("debug"));
        }
        String generated = Files.readString(file, StandardCharsets.UTF_8);
        ZstdVelocityConfig.load(file);
        assertFalse(ZstdVelocityConfig.INSTANCE.debug);
        assertEquals(generated, Files.readString(file, StandardCharsets.UTF_8));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "{}", "compression: {level: 6}", "logging: {}", "logging:"})
    @DisplayName("空文件或缺省logging/debug：真实load为false且不覆写已有文件")
    void missingDebugDefaultsOffWithoutRewriting(String contents) throws IOException {
        Path file = directory.resolve("config.yml");
        Files.writeString(file, contents, StandardCharsets.UTF_8);
        for (int i = 0; i < 2; i++) {
            ZstdVelocityConfig.load(file);
            assertFalse(ZstdVelocityConfig.INSTANCE.debug);
            assertEquals(contents, Files.readString(file, StandardCharsets.UTF_8));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    @DisplayName("显式debug值与用户注释配置在重复真实load后保持不变")
    void explicitDebugAndUserContentsSurviveRepeatedLoad(boolean debug) throws IOException {
        Path file = directory.resolve("config.yml");
        String contents = """
                # 保留用户注释
                compression: {level: 6}
                logging: {debug: %s}
                custom-key: keep
                """.formatted(debug);
        Files.writeString(file, contents, StandardCharsets.UTF_8);
        for (int i = 0; i < 2; i++) {
            ZstdVelocityConfig.load(file);
            assertEquals(debug, ZstdVelocityConfig.INSTANCE.debug);
            assertEquals(6, ZstdVelocityConfig.INSTANCE.level);
            assertEquals(contents, Files.readString(file, StandardCharsets.UTF_8));
        }
    }
}
