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
 * <p>重点只有一件事：{@code logging.debug} 是<b>真开关</b>——不写默认开启，写了 false 必须生效，
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
    void firstLoadWritesTemplateWithDebugSwitchOn() throws IOException {
        Path file = directory.resolve("nested/config.yml");
        assertFalse(Files.exists(file));

        ZstdConfig.load(file);

        assertTrue(ZstdConfig.INSTANCE.debug, "缺省必须开启：没有日志时玩家没法自证问题");
        assertEquals("Miku-ZSTD/logs/zstd-debug.log", ZstdConfig.INSTANCE.debugFile);
        assertTrue(ZstdConfig.INSTANCE.sendClientLog, "取证回传默认开启");
        assertTrue(Files.isRegularFile(file), "首次加载要写出模板");

        String text = Files.readString(file, StandardCharsets.UTF_8);
        assertTrue(text.contains("debug: true"), "模板必须带开关且默认开启：" + text);
        assertTrue(text.contains("send_client_log: true"), text);
    }

    @Test
    void debugFalseIsHonouredAndFileKeptByteIdentical() throws IOException {
        Path file = directory.resolve("config.yml");
        String contents = "logging: {debug: false, keep_files: 5}\ndisplay: {hud_enabled: true}\n";
        Files.writeString(file, contents, StandardCharsets.UTF_8);

        for (int i = 0; i < 2; i++) {
            ZstdConfig.load(file);
            assertFalse(ZstdConfig.INSTANCE.debug, "关闭必须真的生效");
            assertEquals(5, ZstdConfig.INSTANCE.debugKeepFiles, "其它键照常读取");
            assertTrue(ZstdConfig.INSTANCE.hudEnabled, "同段之外的键不受影响");
            assertEquals(contents, Files.readString(file, StandardCharsets.UTF_8), "不得重写玩家原文");
        }
    }

    @Test
    void missingLoggingSectionKeepsSwitchOn() throws IOException {
        Path file = directory.resolve("config.yml");
        String contents = "display: {hud_enabled: false}\n";
        Files.writeString(file, contents, StandardCharsets.UTF_8);

        ZstdConfig.load(file);

        assertTrue(ZstdConfig.INSTANCE.debug, "缺 logging 段时按默认开启");
        assertEquals(contents, Files.readString(file, StandardCharsets.UTF_8), "已有文件不该被重写");
    }
}
