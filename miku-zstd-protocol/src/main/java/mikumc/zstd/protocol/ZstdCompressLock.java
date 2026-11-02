package mikumc.zstd.protocol;

import java.util.Objects;

/**
 * 每连接的压缩上下文租约：关闭立即停止新租借，最后一个使用者负责归还上下文。
 *
 * <p>监视器只保护租约计数与关闭状态，不覆盖原生压缩或归还回调，因而 event loop
 * 不会为了关闭连接等待整批压缩。租约仅保证上下文存活；原生调用的串行性仍由编码器
 * 的单在途批次约束维持。</p>
 *
 * <pre>
 * if (compressLock.tryAcquire()) {
 *     try {
 *         // 在这里取得并使用压缩上下文，不持有 compressLock 监视器。
 *     } finally {
 *         compressLock.release();
 *     }
 * }
 * // 关闭连接时登记唯一归还动作；有在途租约时立即返回。
 * compressLock.closeWhenIdle(() -> ZstdCtxPool.returnCompress(compressCtx));
 * </pre>
 */
public final class ZstdCompressLock {

    private boolean closed;
    private boolean closeRequested;
    private int users;
    private Runnable closeAction;

    /** 原子地检查关闭状态并取得租约；成功后必须恰好调用一次 {@link #release()}。 */
    public synchronized boolean tryAcquire() {
        if (closed) return false;
        users++;
        return true;
    }

    /** 释放一个租约；最后一个使用者在锁外执行已登记的归还动作。 */
    public void release() {
        Runnable action = null;
        synchronized (this) {
            if (users == 0) {
                throw new IllegalStateException("No compression lease to release");
            }
            users--;
            if (users == 0) {
                action = closeAction;
                closeAction = null;
            }
        }
        if (action != null) action.run();
    }

    /**
     * 停止新租借并登记唯一归还动作，不等待仍在使用的上下文。
     * 无租约时立即在锁外执行；重复关闭保留首次登记的动作，即使该动作已经执行或抛错。
     */
    public void closeWhenIdle(Runnable action) {
        Objects.requireNonNull(action, "action");
        Runnable onIdle = null;
        synchronized (this) {
            if (closeRequested) return;
            closed = true;
            closeRequested = true;
            if (users == 0) {
                onIdle = action;
            } else {
                closeAction = action;
            }
        }
        if (onIdle != null) onIdle.run();
    }

    /** 是否已停止新租借；现存租约此时仍可能持有上下文。 */
    public synchronized boolean isClosed() {
        return closed;
    }

    /** 仅停止新租借；不登记归还动作，也不妨碍随后 {@link #closeWhenIdle(Runnable)}。 */
    public synchronized void markClosed() {
        closed = true;
    }
}
