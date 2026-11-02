package mikumc.zstd.protocol;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ZstdCompressLockTest {

    @Test
    @DisplayName("在途租约不阻塞关闭，工作线程退出后恰好归还一次")
    void closeReturnsWhileWorkerStillHoldsLease() throws Exception {
        ZstdCompressLock lock = new ZstdCompressLock();
        AtomicInteger returns = new AtomicInteger();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        CountDownLatch acquired = new CountDownLatch(1);
        CountDownLatch allowRelease = new CountDownLatch(1);
        CountDownLatch closeReturned = new CountDownLatch(1);
        Thread worker = startThread("lease-worker", failure, () -> {
            assertTrue(lock.tryAcquire());
            try {
                acquired.countDown();
                await(allowRelease);
            } finally {
                lock.release();
            }
        });
        Thread closer = null;
        try {
            assertTrue(acquired.await(5, TimeUnit.SECONDS), "工作线程应先取得租约");
            closer = startThread("lease-close", failure, () -> {
                lock.closeWhenIdle(() -> {
                    assertFalse(Thread.holdsLock(lock), "延迟归还必须在锁外执行");
                    returns.incrementAndGet();
                });
                closeReturned.countDown();
            });
            assertTrue(closeReturned.await(5, TimeUnit.SECONDS), "关闭应在工作线程释放前返回");
            assertTrue(lock.isClosed());
            assertFalse(lock.tryAcquire(), "关闭后禁止新租约");
            assertEquals(0, returns.get(), "在途上下文不得提前归还");
        } finally {
            allowRelease.countDown();
            join(worker);
            if (closer != null) join(closer);
        }
        assertNull(failure.get(), "测试线程不得有异常");
        assertEquals(1, returns.get());
        lock.closeWhenIdle(returns::incrementAndGet);
        assertEquals(1, returns.get(), "释放后的重复关闭不得重复归还");
    }

    @Test
    @DisplayName("多个租约只由最后释放者执行首次登记的回调")
    void lastReleaseRunsOnlyOriginalCloseAction() {
        ZstdCompressLock lock = new ZstdCompressLock();
        AtomicInteger returns = new AtomicInteger();
        AtomicInteger replacement = new AtomicInteger();
        assertTrue(lock.tryAcquire());
        assertTrue(lock.tryAcquire());
        lock.closeWhenIdle(returns::incrementAndGet);
        lock.closeWhenIdle(replacement::incrementAndGet);
        assertFalse(lock.tryAcquire());
        lock.release();
        assertEquals(0, returns.get());
        lock.release();
        assertEquals(1, returns.get());
        assertEquals(0, replacement.get(), "重复关闭不得替换原回调");
        lock.closeWhenIdle(replacement::incrementAndGet);
        assertEquals(0, replacement.get());
        assertThrows(IllegalStateException.class, lock::release);
    }

    @Test
    @DisplayName("无租约关闭立即在锁外归还，重复关闭保持幂等")
    void idleCloseRunsOutsideMonitorExactlyOnce() {
        ZstdCompressLock lock = new ZstdCompressLock();
        AtomicInteger returns = new AtomicInteger();
        lock.closeWhenIdle(() -> {
            assertFalse(Thread.holdsLock(lock), "立即归还也必须在锁外执行");
            assertTrue(lock.isClosed());
            assertFalse(lock.tryAcquire());
            returns.incrementAndGet();
            lock.closeWhenIdle(returns::incrementAndGet);
        });
        lock.closeWhenIdle(returns::incrementAndGet);
        assertEquals(1, returns.get());
        assertFalse(lock.tryAcquire());
        assertThrows(IllegalStateException.class, lock::release);
    }

    @Test
    @DisplayName("markClosed仅停止新租约，随后空闲关闭仍登记归还")
    void markClosedStillAllowsIdleCloseRegistration() {
        ZstdCompressLock lock = new ZstdCompressLock();
        AtomicInteger returns = new AtomicInteger();
        assertFalse(lock.isClosed());
        lock.markClosed();
        lock.markClosed();
        assertTrue(lock.isClosed());
        assertFalse(lock.tryAcquire());
        assertEquals(0, returns.get());
        lock.closeWhenIdle(returns::incrementAndGet);
        lock.closeWhenIdle(returns::incrementAndGet);
        assertEquals(1, returns.get());
    }

    @Test
    @DisplayName("markClosed后仍能为现存租约登记延迟归还")
    void markClosedStillAllowsDeferredCloseRegistration() {
        ZstdCompressLock lock = new ZstdCompressLock();
        AtomicInteger returns = new AtomicInteger();
        assertTrue(lock.tryAcquire());
        lock.markClosed();
        assertFalse(lock.tryAcquire());
        lock.closeWhenIdle(returns::incrementAndGet);
        assertEquals(0, returns.get());
        lock.release();
        assertEquals(1, returns.get());
        lock.closeWhenIdle(returns::incrementAndGet);
        assertEquals(1, returns.get());
    }

    @Test
    @DisplayName("先释放后关闭在关闭调用内归还，租约可在未关闭时重新取得")
    void releaseBeforeCloseReturnsImmediately() {
        ZstdCompressLock lock = new ZstdCompressLock();
        AtomicInteger returns = new AtomicInteger();
        assertTrue(lock.tryAcquire());
        lock.release();
        assertTrue(lock.tryAcquire());
        lock.release();
        assertFalse(lock.isClosed());
        lock.closeWhenIdle(returns::incrementAndGet);
        assertEquals(1, returns.get());
        assertFalse(lock.tryAcquire());
    }

    @Test
    @DisplayName("无对应acquire的release抛错，且不破坏后续租约计数")
    void unmatchedReleaseThrowsWithoutChangingState() {
        ZstdCompressLock lock = new ZstdCompressLock();
        assertThrows(IllegalStateException.class, lock::release);
        assertTrue(lock.tryAcquire());
        lock.release();
        assertThrows(IllegalStateException.class, lock::release);
        assertTrue(lock.tryAcquire());
        AtomicInteger returns = new AtomicInteger();
        lock.closeWhenIdle(returns::incrementAndGet);
        lock.release();
        assertEquals(1, returns.get());
        assertThrows(IllegalStateException.class, lock::release);
        assertEquals(1, returns.get());
    }

    @Test
    @DisplayName("空归还动作先校验，不关闭租约入口也不吞掉后续有效登记")
    void nullCloseActionIsRejectedWithoutChangingState() {
        ZstdCompressLock lock = new ZstdCompressLock();
        AtomicInteger returns = new AtomicInteger();
        assertThrows(NullPointerException.class, () -> lock.closeWhenIdle(null));
        assertFalse(lock.isClosed());
        assertTrue(lock.tryAcquire());
        assertThrows(NullPointerException.class, () -> lock.closeWhenIdle(null));
        assertFalse(lock.isClosed());
        lock.closeWhenIdle(returns::incrementAndGet);
        assertEquals(0, returns.get());
        assertThrows(NullPointerException.class, () -> lock.closeWhenIdle(null));
        lock.release();
        assertEquals(1, returns.get());
        lock.closeWhenIdle(returns::incrementAndGet);
        assertEquals(1, returns.get());
    }

    @Test
    @DisplayName("立即或延迟归还动作抛错后均不重试，也不保留已释放租约")
    void throwingActionIsNotRetried() {
        for (boolean deferred : new boolean[]{false, true}) {
            ZstdCompressLock lock = new ZstdCompressLock();
            AtomicInteger returns = new AtomicInteger();
            IllegalStateException expected = new IllegalStateException("return failed");
            Runnable action = () -> {
                assertFalse(Thread.holdsLock(lock));
                returns.incrementAndGet();
                throw expected;
            };
            if (deferred) {
                assertTrue(lock.tryAcquire());
                lock.closeWhenIdle(action);
                assertSame(expected, assertThrows(IllegalStateException.class, lock::release));
            } else {
                assertSame(expected, assertThrows(IllegalStateException.class,
                        () -> lock.closeWhenIdle(action)));
            }
            assertEquals(1, returns.get());
            lock.closeWhenIdle(returns::incrementAndGet);
            assertEquals(1, returns.get());
            assertFalse(lock.tryAcquire());
            assertThrows(IllegalStateException.class, lock::release);
        }
    }

    @Test
    @DisplayName("最后释放与关闭并发交错仍恰好归还一次")
    void concurrentLastReleaseAndCloseReturnExactlyOnce() throws Exception {
        for (int attempt = 0; attempt < 64; attempt++) {
            ZstdCompressLock lock = new ZstdCompressLock();
            AtomicInteger returns = new AtomicInteger();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            CountDownLatch start = new CountDownLatch(1);
            assertTrue(lock.tryAcquire());
            Thread releaser = startThread("lease-release-race", failure, () -> {
                await(start);
                lock.release();
            });
            Thread closer = startThread("lease-close-race", failure, () -> {
                await(start);
                lock.closeWhenIdle(() -> {
                    assertFalse(Thread.holdsLock(lock));
                    returns.incrementAndGet();
                });
            });
            start.countDown();
            join(releaser);
            join(closer);
            assertNull(failure.get(), "并发交错不得抛错");
            assertEquals(1, returns.get());
            assertFalse(lock.tryAcquire());
        }
    }

    private static Thread startThread(String name, AtomicReference<Throwable> failure,
                                      Runnable action) {
        Thread thread = new Thread(() -> {
            try {
                action.run();
            } catch (Throwable t) {
                failure.compareAndSet(null, t);
            }
        }, name);
        thread.setDaemon(true);
        thread.start();
        return thread;
    }

    private static void await(CountDownLatch latch) {
        try {
            assertTrue(latch.await(5, TimeUnit.SECONDS), "等待测试闩锁超时");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("等待测试闩锁被中断", e);
        }
    }

    private static void join(Thread thread) throws InterruptedException {
        thread.join(TimeUnit.SECONDS.toMillis(5));
        assertFalse(thread.isAlive(), "测试线程应已退出：" + thread.getName());
    }
}
