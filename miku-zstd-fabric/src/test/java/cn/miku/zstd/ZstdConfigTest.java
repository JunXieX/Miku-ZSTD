package cn.miku.zstd;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 客户端配置的读写口径。
 *
 * <p>重点只有一件事：{@code logging.debug} 是<b>真开关</b>——不写默认关闭，写 true 才写文件，
 * 而且不得改写玩家手改过的原文（客户端配置是玩家自己维护的，重写会吞掉他的注释）。</p>
 */
class ZstdConfigTest {

    @TempDir
    Path directory;

    private static final ZstdConfig ORIGINAL = ZstdConfig.INSTANCE;

    @AfterEach
    void restoreConfig() {
        ZstdConfig.INSTANCE = ORIGINAL;
    }

    @Test
    void firstLoadWritesTemplateWithDebugSwitchOff() throws IOException {
        Path file = directory.resolve("nested/config.yml");
        assertFalse(Files.exists(file));

        ZstdConfig.load(file);

        assertFalse(ZstdConfig.INSTANCE.debug, "缺省关闭：日志是排查工具，不是常驻开销");
        assertEquals("Miku-ZSTD/logs/zstd-debug.log", ZstdConfig.INSTANCE.debugFile);
        assertTrue(ZstdConfig.INSTANCE.sendClientLog, "取证回传默认开启");
        assertTrue(Files.isRegularFile(file), "首次加载要写出模板");

        String text = Files.readString(file, StandardCharsets.UTF_8);
        assertTrue(text.contains("debug: false"), "模板必须带开关且默认关闭：" + text);
        assertTrue(text.contains("send_client_log: true"), text);
    }

    @Test
    void debugTrueIsHonouredAndFileKeptByteIdentical() throws IOException {
        Path file = directory.resolve("config.yml");
        String contents = "logging: {debug: true, keep_files: 5}\ndisplay: {hud_enabled: true}\n";
        Files.writeString(file, contents, StandardCharsets.UTF_8);

        for (int i = 0; i < 2; i++) {
            ZstdConfig.load(file);
            assertTrue(ZstdConfig.INSTANCE.debug, "默认关闭，写 true 必须真的打开");
            assertEquals(5, ZstdConfig.INSTANCE.debugKeepFiles, "其它键照常读取");
            assertTrue(ZstdConfig.INSTANCE.hudEnabled, "同段之外的键不受影响");
            assertEquals(contents, Files.readString(file, StandardCharsets.UTF_8), "不得重写玩家原文");
        }
    }

    @Test
    void missingLoggingSectionKeepsSwitchOff() throws IOException {
        Path file = directory.resolve("config.yml");
        String contents = "display: {hud_enabled: false}\n";
        Files.writeString(file, contents, StandardCharsets.UTF_8);

        ZstdConfig.load(file);

        assertFalse(ZstdConfig.INSTANCE.debug, "缺 logging 段时按默认关闭");
        assertEquals(contents, Files.readString(file, StandardCharsets.UTF_8), "已有文件不该被重写");
    }
}
