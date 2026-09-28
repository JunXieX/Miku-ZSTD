package cn.miku.zstd;

import com.github.luben.zstd.ZstdDictCompress;
import com.github.luben.zstd.ZstdDictDecompress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 训练字典的<b>进程级共享</b>注册表（客户端）。
 *
 * <h2>为什么需要它</h2>
 * <p>旧实现每次收到 negotiate 里的字典都执行 {@code new ZstdDictCompress(...)} /
 * {@code new ZstdDictDecompress(...)} 并直接交给上下文，既不复用也不释放。实测
 * （128KB 字典 / level 15）每次重建的代价：</p>
 * <pre>
 *   ZstdDictCompress   : 约 3227 KB（共享后约 5 KB）
 *   ZstdDictDecompress : 约  141 KB（共享后基本为零）
 * </pre>
 * <p>客户端反复重连同一服务器时，每次都重建这批预计算表是纯粹的浪费。</p>
 *
 * <h2>顺带消除的隐患</h2>
 * <p>旧实现里那个临时字典对象在 {@code loadDict} 之后立即不可达，而 zstd-jni 的
 * {@code AutoCloseBase} 会在 GC 时经 Cleaner 释放底层 CDict/DDict——上下文若仍持有
 * 其指针，就是一个悬垂引用（use-after-free）。现在字典对象有强引用且带引用计数，
 * 只在确实无人使用时才释放。</p>
 *
 * <h2>方向语义（与 Velocity 端相反，勿混淆）</h2>
 * <ul>
 *   <li>服务端下发的 "encoder" 字典 → 客户端<b>解压</b>用（{@link ZstdDictDecompress}）；</li>
 *   <li>服务端下发的 "decoder" 字典 → 客户端<b>压缩</b>用（{@link ZstdDictCompress}）。</li>
 * </ul>
 */
public final class ZstdDictRegistry {

    private static final Logger LOGGER = LoggerFactory.getLogger(MikuZstd.LOGGER_NAME);

    /** 每个方向最多保留几代字典（客户端可能连过多个服务器） */
    private static final int MAX_ENTRIES = 4;

    private static final Object LOCK = new Object();
    /** key = dictId + "@" + level，保证等级变化时重建 */
    private static final Map<String, Entry> COMPRESS = new LinkedHashMap<>();
    private static final Map<String, Entry> DECOMPRESS = new LinkedHashMap<>();

    private ZstdDictRegistry() {
    }

    /** 一个不可变的字典条目，带引用计数。 */
    public static final class Entry {
        private final long id;
        private final int level;
        private final byte[] bytes;
        private final ZstdDictCompress compressDict;
        private final ZstdDictDecompress decompressDict;

        private final AtomicInteger refs = new AtomicInteger(1);
        private boolean retired;
        private boolean closed;

        private Entry(long id, int level, byte[] bytes,
                      ZstdDictCompress compressDict, ZstdDictDecompress decompressDict) {
            this.id = id;
            this.level = level;
            this.bytes = bytes;
            this.compressDict = compressDict;
            this.decompressDict = decompressDict;
        }

        public long id() {
            return id;
        }

        public ZstdDictCompress compressDict() {
            return compressDict;
        }

        public ZstdDictDecompress decompressDict() {
            return decompressDict;
        }

        /** 取用一个引用；false 表示已被淘汰，调用方应重新获取。 */
        public boolean acquire() {
            synchronized (this) {
                if (retired || closed) return false;
                refs.incrementAndGet();
                return true;
            }
        }

        /** 释放一个引用；已淘汰且引用归零时真正释放原生资源。 */
        public void release() {
            synchronized (this) {
                if (refs.decrementAndGet() == 0 && retired) {
                    closeInternal();
                }
            }
        }

        private void retire() {
            synchronized (this) {
                retired = true;
                if (refs.decrementAndGet() == 0) {
                    closeInternal();
                }
            }
        }

        private void closeInternal() {
            if (closed) return;
            closed = true;
            if (compressDict != null) compressDict.close();
            if (decompressDict != null) decompressDict.close();
        }
    }

    /**
     * 取用（必要时创建）"服务端 → 客户端"解压字典条目。
     *
     * @return 条目；参数非法时返回 null
     */
    public static Entry acquireDecompressDict(byte[] bytes, long id) {
        if (bytes == null || bytes.length == 0 || id == 0L) return null;
        return acquire(DECOMPRESS, id + "@0", () -> new Entry(id, 0, bytes, null, new ZstdDictDecompress(bytes)), id);
    }

    /**
     * 取用（必要时创建）"客户端 → 服务端"压缩字典条目。
     */
    public static Entry acquireCompressDict(byte[] bytes, long id, int level) {
        if (bytes == null || bytes.length == 0 || id == 0L) return null;
        return acquire(COMPRESS, id + "@" + level,
                () -> new Entry(id, level, bytes, new ZstdDictCompress(bytes, level), null), id);
    }

    private static Entry acquire(Map<String, Entry> cache, String key, java.util.function.Supplier<Entry> factory,
                                 long id) {
        for (int attempt = 0; attempt < 8; attempt++) {
            Entry existing;
            Entry evicted = null;
            synchronized (LOCK) {
                existing = cache.get(key);
                if (existing == null) {
                    Entry fresh = factory.get();
                    cache.put(key, fresh);
                    // 超出容量时淘汰最旧的一代（在用的条目靠引用计数延迟释放）
                    while (cache.size() > MAX_ENTRIES) {
                        String oldest = cache.keySet().iterator().next();
                        if (oldest.equals(key)) break;
                        evicted = cache.remove(oldest);
                        if (evicted != null) evicted.retire();
                    }
                    LOGGER.info("[Zstd] Dict entry created: id={} cached={}", id, cache.size());
                    return fresh;
                }
            }
            if (existing.acquire()) return existing;
        }
        return null;
    }

}
