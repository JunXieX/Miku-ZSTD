package mikumc.zstd.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 诊断通道（{@code zstd:diag}）的线格式与现场落地。
 *
 * <p>这些用例守的都是"客户端给的东西不可信"这一类性质：压缩炸弹、超长文件名、
 * 半行截断、目录里堆满现场。跑在 Velocity 模块的 test 任务里（协议测试统一挂那里）。</p>
 */
class ZstdDiagTest {

    @TempDir
    Path directory;

    @AfterEach
    void cleanup() {
        ZstdDebugLog.close();
    }

    @Test
    @DisplayName("请求载荷往返：版本 + 模式")
    void requestRoundTrip() {
        ByteBuf buf = Unpooled.buffer();
        try {
            ZstdDiagFrames.writeRequest(buf, ZstdDiagFrames.PROTOCOL_VERSION, ZstdDiagFrames.MODE_STALL);
            byte[] payload = new byte[buf.readableBytes()];
            buf.readBytes(payload);
            ZstdDiagFrames.Request req = ZstdDiagFrames.readRequest(payload);
            assertNotNull(req);
            assertEquals(ZstdDiagFrames.PROTOCOL_VERSION, req.version());
            assertEquals(ZstdDiagFrames.MODE_STALL, req.mode());
            assertTrue(req.isSupported());
        } finally {
            buf.release();
        }
        assertFalse(ZstdDiagFrames.readRequest(new byte[0]) != null);
        assertFalse(ZstdDiagFrames.readRequest(new byte[] {0, 0, 0, 1}) != null, "长度不足必须判无效");
    }

    @Test
    @DisplayName("应答载荷往返：状态、版本、原始长度与 gzip 体")
    void answerRoundTrip() {
        byte[] raw = "hello diag".getBytes(StandardCharsets.UTF_8);
        byte[] gz = ZstdDiagFrames.gzip(raw);
        assertNotNull(gz);
        byte[] payload = ZstdDiagFrames.encodeAnswer(ZstdDiagFrames.STATUS_OK,
                ZstdDiagFrames.PROTOCOL_VERSION, raw.length, gz);
        ZstdDiagFrames.Answer answer = ZstdDiagFrames.readAnswer(payload);
        assertNotNull(answer);
        assertEquals(ZstdDiagFrames.STATUS_OK, answer.status());
        assertEquals(raw.length, answer.rawLength());
        assertTrue(answer.hasPayload());
        assertArrayEqualsRaw(raw, ZstdDiagFrames.gunzipBounded(answer.payload(), 1024));

        ZstdDiagFrames.Answer noPayload = ZstdDiagFrames.readAnswer(
                ZstdDiagFrames.encodeAnswer(ZstdDiagFrames.STATUS_NO_LOG,
                        ZstdDiagFrames.PROTOCOL_VERSION, 0, null));
        assertNotNull(noPayload);
        assertFalse(noPayload.hasPayload());
        assertNull(ZstdDiagFrames.readAnswer(new byte[3]), "长度不足必须判无效");
    }

    @Test
    @DisplayName("取尾部：只留最近一段，并丢掉被切断的半行")
    void readTailDropsPartialFirstLine() throws Exception {
        Path file = directory.resolve("zstd-debug.log");
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 500; i++) {
            sb.append("line-").append(i).append(" padding-padding-padding\n");
        }
        Files.writeString(file, sb.toString(), StandardCharsets.UTF_8);

        ZstdDiagFrames.Tail tail = ZstdDiagFrames.readTail(file, 400);
        assertFalse(tail.isEmpty());
        assertTrue(tail.truncated(), "文件大于上限时必须标记截断");
        String text = new String(tail.data(), StandardCharsets.UTF_8);
        assertTrue(text.startsWith("line-"), "开头不能是半行：" + text.substring(0, Math.min(20, text.length())));
        assertTrue(text.contains("line-499 "), "必须包含最后一行");

        ZstdDiagFrames.Tail whole = ZstdDiagFrames.readTail(file, 1 << 20);
        assertFalse(whole.truncated(), "上限大于文件时不算截断");
        assertEquals(Files.size(file), whole.fileBytes());

        assertTrue(ZstdDiagFrames.readTail(directory.resolve("nope.log"), 400).isEmpty());
        assertTrue(ZstdDiagFrames.readTail(file, 0).isEmpty());
    }

    @Test
    @DisplayName("解压有上界：压缩炸弹被拒，正常内容照常解出")
    void gunzipIsBounded() {
        byte[] big = new byte[2 * 1024 * 1024];
        byte[] gz = ZstdDiagFrames.gzip(big);
        assertNotNull(gz);
        assertTrue(gz.length < 64 * 1024, "全零数据应压得很小，实际 " + gz.length);
        assertNull(ZstdDiagFrames.gunzipBounded(gz, 64 * 1024), "超过上限必须整体放弃");
        assertNotNull(ZstdDiagFrames.gunzipBounded(gz, 4 * 1024 * 1024), "上限足够时应解出");
        assertNull(ZstdDiagFrames.gunzipBounded(new byte[] {1, 2, 3}, 1024), "非 gzip 输入返回 null");
    }

    @Test
    @DisplayName("文件名净化：路径分隔符与非常规字符一律替换，长度有上限")
    void sanitizeStripsDangerousCharacters() {
        assertEquals("Steve", ZstdDiagFrames.sanitize("Steve", 48));
        assertEquals(".._.._etc_passwd", ZstdDiagFrames.sanitize("../../etc/passwd", 48));
        assertEquals("a_b", ZstdDiagFrames.sanitize("a/b", 48));
        assertEquals("unknown", ZstdDiagFrames.sanitize("   ", 48));
        assertEquals("unknown", ZstdDiagFrames.sanitize(null, 48));
        assertEquals(8, ZstdDiagFrames.sanitize("abcdefghijklmn", 8).length());
        assertEquals("____", ZstdDiagFrames.sanitize("玩家名字", 48), "非 ASCII 逐字符替换，不留空");
    }

    @Test
    @DisplayName("现场落地：三件套写齐，并按 keep 数清理最旧")
    void writeBundleAndPrune() throws Exception {
        Path root = directory.resolve("stall");
        Map<String, String> session = new LinkedHashMap<>();
        session.put("拓扑", "直连");
        session.put("服务端版本", "1.0.0");

        Path first = ZstdDiagStore.writeBundle(root, "Steve", 1_700_000_000_000L,
                "server-tail".getBytes(StandardCharsets.UTF_8),
                ZstdDiagFrames.gzip("client-tail".getBytes(StandardCharsets.UTF_8)),
                session, 2);
        assertNotNull(first);
        assertTrue(Files.isRegularFile(first.resolve("server.log")));
        assertTrue(Files.isRegularFile(first.resolve("client.log.gz")));
        String text = Files.readString(first.resolve("session.txt"), StandardCharsets.UTF_8);
        assertTrue(text.contains("玩家: Steve"), text);
        assertTrue(text.contains("拓扑: 直连"), text);

        Path second = ZstdDiagStore.writeBundle(root, "Steve", 1_700_000_001_000L, new byte[0], null, session, 2);
        Path third = ZstdDiagStore.writeBundle(root, "Steve", 1_700_000_002_000L, new byte[0], null, session, 2);
        assertNotNull(second);
        assertNotNull(third);
        try (Stream<Path> stream = Files.list(root)) {
            List<Path> dirs = stream.filter(Files::isDirectory).toList();
            assertEquals(2, dirs.size(), "keep=2 应只留最近两组：" + dirs);
            assertFalse(dirs.contains(first), "最旧的一组应被清掉");
        }

        Path noClient = ZstdDiagStore.writeBundle(root, "../../etc", 1_700_000_003_000L,
                new byte[0], null, session, 2);
        assertNotNull(noClient);
        assertTrue(noClient.getFileName().toString().startsWith(".._.._etc-"),
                "目录名必须净化：" + noClient.getFileName());
        assertFalse(Files.exists(noClient.resolve("client.log.gz")), "没有回传时不应留空文件");
    }

    private static void assertArrayEqualsRaw(byte[] expected, byte[] actual) {
        assertNotNull(actual, "解压结果不应为 null");
        assertEquals(expected.length, actual.length);
        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], actual[i], "字节 " + i + " 不一致");
        }
    }
}
