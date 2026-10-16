package mikumc.zstd.protocol;

/**
 * 每连接独占的<b>压缩互斥锁 + 原生上下文关闭标志</b>（三端共用的唯一实现）。
 *
 * <h2>它解决的是一个会让整个服务端崩溃的竞态</h2>
 * <p>压缩已卸载到 {@link ZstdCompressPool}，而 {@code ZstdCompressCtx} 是<b>原生内存</b>。
 * 关连接时 ChannelManager 会 close 掉这个上下文。仅用"双方都 synchronized 同一把锁"
 * <b>不足以</b>避免 use-after-free：</p>
 *
 * <pre>
 *   t0  池线程读到"未关闭"（此时通道仍活跃）
 *   t1  event loop：channelInactive → ChannelManager#close()
 *         └─ synchronized(lock) { compressCtx.close(); }   // 无人持锁，直接关掉
 *   t2  池线程才进入 synchronized(lock) 并调用 compressCtx.compressByteArray(...)
 *         └─ 那是一块已释放的原生内存 → SIGSEGV（整个进程崩，不止这条连接）
 * </pre>
 *
 * <p>问题在于"读标志"与"拿锁"是两步，中间可以让出：volatile 读本身不提供跨线程的
 * "随后加锁"顺序保证。可行的判据只有一个——<b>标志必须在锁内写、也必须在锁内读</b>：
 * 双方都持同一把锁时，先进入者要么完成压缩（此后 close 才关上下文），要么先关上下文
 *（此后压缩任务必然读到标志并放弃）。两种顺序都安全。</p>
 *
 * <h2>为什么把标志做在锁对象上，而不是各自加一个 volatile 字段</h2>
 * <p>标志与它要保护的锁必须成对出现、同生共死，否则很容易在某个端漏加一次、或写成
 * "锁外读标志"。把两者做成同一个对象，就使"持锁读标志"成为唯一自然的用法，
 * 三端也不需要各自复制一遍这段并发推理。</p>
 *
 * <h2>用法</h2>
 * <pre>
 *   // ChannelManager（写入侧）：
 *   synchronized (compressLock) {
 *       compressLock.markClosed();      // 先立标志，再关上下文
 *       compressCtx.close();
 *   }
 *
 *   // 编码器（读取侧，跑在池线程上）：
 *   synchronized (compressLock) {
 *       if (compressLock.isClosed()) { 放弃，绝不碰 compressCtx }
 *       compressCtx.compressByteArray(...);
 *   }
 * </pre>
 */
public final class ZstdCompressLock {

    /** 原生上下文是否已关闭。只在持锁时读写（见类注释）。*/
    private boolean closed;

    /** 持锁时读取：该连接的压缩上下文是否已不可用。 */
    public boolean isClosed() {
        return closed;
    }

    /**
     * 标记上下文已关闭。<b>必须与 {@code compressCtx.close()} 在同一段
     * {@code synchronized (this)} 内、且先调用</b>，否则标志与真实状态会不一致。
     */
    public void markClosed() {
        closed = true;
    }
}
