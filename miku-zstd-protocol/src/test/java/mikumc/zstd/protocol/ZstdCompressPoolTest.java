package mikumc.zstd.protocol;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ZstdCompressPoolTest {

    @Test
    @DisplayName("完成计数：运行中与排队任务不得进入平均耗时分母")
    void completedCountExcludesRunningAndQueuedTasks() throws Exception {
        // 仅取得真实执行器等待wrapper退出，不改计数、不新增生产测试钩子。
        Field poolField = ZstdCompressPool.class.getDeclaredField("pool");
        poolField.setAccessible(true);
        ExecutorService executor = (ExecutorService) poolField.get(null);
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try {
            ZstdCompressPool.shutdown();
            if (executor != null) {
                assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS), "前序压缩任务应已退出");
            }
            ZstdCompressPool.init(1, 5);
            executor = (ExecutorService) poolField.get(null);
            assertNotNull(executor, "必须使用生产线程池");
            long before = ZstdCompressPool.totalTasks();

            ZstdCompressPool.execute(() -> {
                started.countDown();
                try {
                    if (!release.await(10, TimeUnit.SECONDS)) {
                        throw new AssertionError("等待测试放行超时");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError("阻塞任务被中断", e);
                }
            });
            assertTrue(started.await(10, TimeUnit.SECONDS), "首个任务必须已经开始");
            for (int i = 0; i < 8; i++) {
                ZstdCompressPool.execute(() -> { });
            }

            assertEquals(1, ZstdCompressPool.active(), "首个任务仍在运行");
            assertEquals(8, ZstdCompressPool.queueDepth(), "单线程池应积压八个任务");
            assertEquals(before, ZstdCompressPool.totalTasks(), "未完成任务不得污染完成计数");

            executor.shutdown();
            release.countDown();
            // task body的闩锁不代表统计finally已执行；执行器终止才覆盖全部wrapper收尾。
            assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS), "九个任务必须全部完成收尾");
            assertEquals(before + 9, ZstdCompressPool.totalTasks(), "每个完成任务恰好计数一次");
            assertEquals(0, ZstdCompressPool.active());
            assertEquals(0, ZstdCompressPool.queueDepth());
            ZstdCompressPool.shutdown();
            assertThrows(RejectedExecutionException.class, () -> ZstdCompressPool.execute(() -> { }));
            assertNull(poolField.get(null), "提交被拒后不得自动重建池");
            assertEquals(before + 9, ZstdCompressPool.totalTasks(), "拒绝任务不得计入完成数");
        } finally {
            release.countDown();
            ZstdCompressPool.shutdown();
            if (executor != null) {
                assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS), "测试线程必须退出");
            }
        }
    }
}
