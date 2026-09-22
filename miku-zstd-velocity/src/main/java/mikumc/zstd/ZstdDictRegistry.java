package mikumc.zstd;

import com.github.luben.zstd.ZstdDictCompress;
import com.github.luben.zstd.ZstdDictDecompress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 训练字典的<b>进程级共享</b>注册表（Velocity 端）。
 *
 * <h2>为什么需要它</h2>
 * <p>旧实现（≤3.0.0）在每条连接激活时执行
 * {@code new ZstdDictCompress(dict, 9)} 并交给该连接的 {@code compressCtx}。
 * 实测（128KB 字典 / level 15 / 500 连接）每连接的字典对象开销：</p>
 * <pre>
 *   每连接各自 new ZstdDictCompress :  3227.5 KB/conn
 *   全进程共享同一个 ZstdDictCompress:     5.3 KB/conn
 *   每连接各自 new ZstdDictDecompress:   245.7 KB/conn
 *   全进程共享同一个 ZstdDictDecompress:  105.2 KB/conn
 * </pre>
 * <p>即 200 人在线时纯字典对象就要多占约 <b>670MB</b>。字典内容对所有连接
 * 完全相同（是全局训练产物），共享是安全且必然的。</p>
 *
 * <h2>顺带消除的隐患</h2>
 * <p>旧实现里那个临时 {@code ZstdDictCompress} 在 {@code loadDict} 之后立即
 * 不可达，而 zstd-jni 的 {@code AutoCloseBase} 会在 GC 时经 Cleaner 释放底层
 * CDict——压缩上下文若仍持有其指针，就是一个悬垂引用（use-after-free）。
 * 现在字典对象有强引用且带引用计数，只在<b>确实无人使用</b>时才释放。</p>
 *
 * <h2>生命周期</h2>
 * <p>字典换代（训练器采纳新字典）时旧条目进入 retired 状态，仍在使用它的连接
 * 释放引用后才会真正 close，避免打断进行中的连接。</p>
 */
public final class ZstdDictRegistry {

    private static final Logger LOGGER = LoggerFactory.getLogger("miku-zstd");

    private static final Object LOCK = new Object();

    private static volatile Entry encoderEntry;
    private static volatile Entry decoderEntry;

    private ZstdDictRegistry() {
    }

    /**
     * 一个不可变的字典条目。{@code id + level} 是它的身份：两者任一变化都需要重建
     * （{@code ZstdDictCompress} 的预计算表与压缩等级绑定）。
     */
    public static final class Entry {
        private final long id;
        private final int level;
        private final byte[] bytes;
        private final ZstdDictCompress compressDict;
        private final ZstdDictDecompress decompressDict;

        /** 注册表自身持有 1 个引用，每条连接激活时再 acquire 1 个。 */
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

        public int level() {
            return level;
        }

        public byte[] bytes() {
            return bytes;
        }

        /** 供 {@code compressCtx.loadDict(...)} 使用；解压方向的条目返回 null。 */
        public ZstdDictCompress compressDict() {
            return compressDict;
        }

        /** 供 {@code decompressCtx.loadDict(...)} 使用；压缩方向的条目返回 null。 */
        public ZstdDictDecompress decompressDict() {
            return decompressDict;
        }

        /**
         * 取用一个引用。返回 false 表示该条目已被换代淘汰，调用方应重新获取。
         */
        public boolean acquire() {
            synchronized (this) {
                if (retired || closed) return false;
                refs.incrementAndGet();
                return true;
            }
        }

        /** 释放一个引用；当条目已淘汰且引用归零时真正释放原生资源。 */
        public void release() {
            synchronized (this) {
                if (refs.decrementAndGet() == 0 && retired) {
                    closeInternal();
                }
            }
        }

        /** 注册表放弃该条目（换代时调用）。 */
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
            LOGGER.debug("[Zstd] Retired dict entry released: id={} level={}", id, level);
        }
    }

    /**
     * 更新"服务端 → 客户端"方向的压缩字典（装入 compressCtx）。
     *
     * @return 新条目；{@code dictBytes} 为空时返回 null 并清空当前条目
     */
    public static Entry updateEncoder(long id, byte[] dictBytes, int level) {
        if (id == 0 || dictBytes == null || dictBytes.length == 0) {
            swap(true, null);
            return null;
        }
        Entry current = encoderEntry;
        if (current != null && current.id == id && current.level == level) {
            return current; // 同一代字典，无需重建 3MB 的 CDict
        }
        Entry fresh = new Entry(id, level, dictBytes, new ZstdDictCompress(dictBytes, level), null);
        swap(true, fresh);
        LOGGER.info("[Zstd] Encoder dict entry rebuilt: id={} level={} size={}B",
                id, level, dictBytes.length);
        return fresh;
    }

    /**
     * 更新"客户端 → 服务端"方向的解压字典（装入 decompressCtx）。
     */
    public static Entry updateDecoder(long id, byte[] dictBytes) {
        if (id == 0 || dictBytes == null || dictBytes.length == 0) {
            swap(false, null);
            return null;
        }
        Entry current = decoderEntry;
        if (current != null && current.id == id) {
            return current;
        }
        Entry fresh = new Entry(id, 0, dictBytes, null, new ZstdDictDecompress(dictBytes));
        swap(false, fresh);
        LOGGER.info("[Zstd] Decoder dict entry rebuilt: id={} size={}B", id, dictBytes.length);
        return fresh;
    }

    private static void swap(boolean encoder, Entry fresh) {
        Entry old;
        synchronized (LOCK) {
            if (encoder) {
                old = encoderEntry;
                encoderEntry = fresh;
            } else {
                old = decoderEntry;
                decoderEntry = fresh;
            }
        }
        if (old != null) old.retire();
    }

    /** 取用当前压缩字典条目（含引用计数），无字典时返回 null。 */
    public static Entry acquireEncoder() {
        for (int i = 0; i < 8; i++) {
            Entry e = encoderEntry;
            if (e == null) return null;
            if (e.acquire()) return e;
        }
        return null;
    }

    /** 取用当前解压字典条目（含引用计数），无字典时返回 null。 */
    public static Entry acquireDecoder() {
        for (int i = 0; i < 8; i++) {
            Entry e = decoderEntry;
            if (e == null) return null;
            if (e.acquire()) return e;
        }
        return null;
    }

    public static long encoderDictId() {
        Entry e = encoderEntry;
        return e == null ? 0L : e.id;
    }

    public static long decoderDictId() {
        Entry e = decoderEntry;
        return e == null ? 0L : e.id;
    }

    /** 当前压缩方向字典字节（negotiate 内联推送用），无字典时返回 null。 */
    public static byte[] encoderDictBytes() {
        Entry e = encoderEntry;
        return e == null ? null : e.bytes;
    }

    /** 当前解压方向字典字节，无字典时返回 null。 */
    public static byte[] decoderDictBytes() {
        Entry e = decoderEntry;
        return e == null ? null : e.bytes;
    }

    /** 插件卸载时释放两个条目（此时已无活跃连接引用）。 */
    public static void shutdown() {
        swap(true, null);
        swap(false, null);
    }
}
