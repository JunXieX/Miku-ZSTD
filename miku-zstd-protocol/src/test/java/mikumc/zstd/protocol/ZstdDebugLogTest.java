package mikumc.zstd.protocol;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 调试文件通道的行为约束。
 *
 * <p>这些用例保护的都是"日志本身不能成为故障源"这一类性质：不阻塞调用方、不抛异常、
 * 有硬上界、关得掉。它们跑在 Velocity 模块的 test 任务里（协议测试统一挂在那里）。</p>
 */
class ZstdDebugLogTest {

    /** configure 的字节下限：小于它按它处理，所以这里直接按它来测滚动。 */
    private static final int MIN_LIMIT = 64 * 1024;

    @TempDir
    Path directory;

    @AfterEach
    void closeSink() {
        ZstdDebugLog.close();
    }

    @Test
    @DisplayName("启用后按行写入：带时间戳与线程名，占位符与异常堆栈都成型")
    void enabledWritesLinesInOrder() throws Exception {
        Path file = directory.resolve("logs/zstd-debug.log");
        ZstdDebugLog.configure(file, true, MIN_LIMIT, 2, null);
        assertTrue(ZstdDebugLog.enabled());

        ZstdDebugLog.d("hello {}", "world");
        ZstdDebugLog.d("plain line");
        ZstdDebugLog.d("boom", new IllegalStateException("bad"));
        ZstdDebugLog.close();

        List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
        // 异常条目会按 slf4j 的约定在行尾追加堆栈帧（帧数随 JDK 版本变化），所以这里数"条目"
        // 而不是物理行：三条记录的第一行必定带时间戳前缀。
        assertTrue(lines.size() >= 3, "三条记录至少三行：" + lines);
        assertEquals(3, ZstdDebugLog.writtenLines(), "每次调用记一条：" + ZstdDebugLog.writtenLines());
        assertTrue(lines.get(0).matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{3} \\[.+\\] hello world"),
                "行首必须是 时间戳 [线程] 内容：" + lines.get(0));
        assertEquals(1, lines.stream().filter(line -> line.endsWith("] plain line")).count());
        String boom = lines.stream()
                .filter(line -> line.contains("boom java.lang.IllegalStateException: bad"))
                .findFirst()
                .orElse("");
        assertFalse(boom.isEmpty(), "未配对占位符的末位异常按堆栈追加：" + lines);
        assertTrue(lines.stream().anyMatch(line -> line.trim().startsWith("at ")), "异常条目应带堆栈帧：" + lines);
    }

    @Test
    @DisplayName("关闭时不建文件、不起线程，调用直接返回")
    void disabledCreatesNoFile() {
        Path file = directory.resolve("logs/zstd-debug.log");
        ZstdDebugLog.configure(file, false, MIN_LIMIT, 2, null);
        assertFalse(ZstdDebugLog.enabled());
        assertNull(ZstdDebugLog.file());
        for (int i = 0; i < 10; i++) {
            ZstdDebugLog.d("line {}", i);
        }
        ZstdDebugLog.close();
        assertFalse(Files.exists(file), "关闭状态不应留下文件");
        assertTrue(ZstdDebugLog.statsLine().contains("关闭"));
    }

    @Test
    @DisplayName("超过上限即滚动，历史份数受 keep_files 限制")
    void rotationKeepsBoundedFiles() throws Exception {
        Path file = directory.resolve("zstd-debug.log");
        ZstdDebugLog.configure(file, true, MIN_LIMIT, 2, null);

        String padding = "x".repeat(60);
        for (int i = 0; i < 3000; i++) {
            ZstdDebugLog.d("line-{} {}", i, padding);
        }
        ZstdDebugLog.close();

        assertTrue(Files.exists(file), "当前文件必须存在");
        assertTrue(Files.exists(file.resolveSibling("zstd-debug.log.1")), "滚动后的上一份必须存在");
        assertFalse(Files.exists(file.resolveSibling("zstd-debug.log.3")), "keep_files=2 不应留下第 3 份历史");
        for (Path candidate : existing(directory, "zstd-debug.log")) {
            long size = Files.size(candidate);
            assertTrue(size <= MIN_LIMIT + 4096L, candidate.getFileName() + " 超过上限: " + size);
        }
    }

    @Test
    @DisplayName("keep_files=0 时改用截断，不留历史份数")
    void keepFilesZeroTruncatesInsteadOfBackingUp() throws Exception {
        Path file = directory.resolve("zstd-debug.log");
        ZstdDebugLog.configure(file, true, MIN_LIMIT, 0, null);

        String padding = "y".repeat(60);
        for (int i = 0; i < 3000; i++) {
            ZstdDebugLog.d("line-{} {}", i, padding);
        }
        ZstdDebugLog.close();

        assertFalse(Files.exists(file.resolveSibling("zstd-debug.log.1")), "keep_files=0 不应有历史文件");
        String content = Files.readString(file, StandardCharsets.UTF_8);
        assertTrue(content.contains("line-2999 "), "截断后仍应保留最后写入的内容");
    }

    @Test
    @DisplayName("目标不可写时降级为不记录，且调用方永不收到异常")
    void neverThrowsOnUnwritablePath() throws Exception {
        Path blocker = directory.resolve("blocker");
        Files.writeString(blocker, "not a directory", StandardCharsets.UTF_8);
        Path target = blocker.resolve("logs").resolve("zstd-debug.log");

        assertDoesNotThrow(() -> ZstdDebugLog.configure(target, true, MIN_LIMIT, 2, null));
        assertFalse(ZstdDebugLog.enabled(), "打不开文件时必须关闭通道");
        assertNull(ZstdDebugLog.file());
        assertDoesNotThrow(() -> {
            for (int i = 0; i < 100; i++) {
                ZstdDebugLog.d("line {}", i);
            }
            ZstdDebugLog.close();
        });
    }

    @Test
    @DisplayName("close 可重复调用，之后仍可重新配置到新文件（reload 路径）")
    void closeIsIdempotentAndReconfigurable() throws Exception {
        Path first = directory.resolve("first.log");
        ZstdDebugLog.configure(first, true, MIN_LIMIT, 1, null);
        ZstdDebugLog.d("first-line");
        ZstdDebugLog.close();
        assertDoesNotThrow(ZstdDebugLog::close);
        assertFalse(ZstdDebugLog.enabled());

        Path second = directory.resolve("nested/second.log");
        ZstdDebugLog.configure(second, true, MIN_LIMIT, 1, null);
        ZstdDebugLog.d("second-line");
        ZstdDebugLog.close();

        assertTrue(Files.readString(first, StandardCharsets.UTF_8).contains("first-line"));
        assertTrue(Files.readString(second, StandardCharsets.UTF_8).contains("second-line"));
    }

    @Test
    @DisplayName("并发灌入不阻塞、不死锁：写入方耗时有上界")
    void queueSaturationDoesNotBlock() throws Exception {
        Path file = directory.resolve("burst.log");
        ZstdDebugLog.configure(file, true, 64 * MIN_LIMIT, 1, null);

        int threads = 4;
        int perThread = 5000;
        CountDownLatch start = new CountDownLatch(1);
        List<Thread> workers = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            Thread worker = new Thread(() -> {
                try {
                    start.await();
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
                for (int i = 0; i < perThread; i++) {
                    ZstdDebugLog.d("burst-{}", i);
                }
            }, "writer-" + t);
            worker.start();
            workers.add(worker);
        }

        long begin = System.nanoTime();
        start.countDown();
        for (Thread worker : workers) {
            worker.join(TimeUnit.SECONDS.toMillis(10));
        }
        long elapsedMs = (System.nanoTime() - begin) / 1_000_000L;
        ZstdDebugLog.close();

        assertTrue(elapsedMs < 5000, "写入方耗时应远小于 5 秒，实际 " + elapsedMs + "ms");
        assertTrue(Files.exists(file), "启用后文件必须存在");
        assertFalse(Files.readAllLines(file, StandardCharsets.UTF_8).isEmpty(), "文件里至少要有一行");
    }

    @Test
    @DisplayName("会话横幅与状态行给出可粘贴的现场信息")
    void headerAndStatsLine() throws Exception {
        Path file = directory.resolve("logs/zstd-debug.log");
        ZstdDebugLog.configure(file, true, MIN_LIMIT, 2, null);
        ZstdDebugLog.sessionHeader("9.9.9", "unit-test", Map.of("key", "value"));
        String stats = ZstdDebugLog.statsLine();
        ZstdDebugLog.close();

        String content = Files.readString(file, StandardCharsets.UTF_8);
        assertTrue(content.contains("版本=9.9.9 平台=unit-test"), "横幅应含版本与平台：" + content);
        assertTrue(content.contains("key=value"), "横幅应含附加键值");
        assertTrue(stats.contains("zstd-debug.log"), "状态行应给出文件路径：" + stats);
        assertTrue(stats.contains("已写"), "状态行应给出计数：" + stats);
    }

    private static List<Path> existing(Path parent, String baseName) throws Exception {
        try (var stream = Files.list(parent)) {
            return stream.filter(p -> p.getFileName().toString().startsWith(baseName)).toList();
        }
    }
}
