package mikumc.zstd.paper;

import com.github.luben.zstd.ZstdDictCompress;
import com.github.luben.zstd.ZstdDictDecompress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 字典的进程级共享注册表（Paper 端，参照 Velocity 端移植）。
 *
 * <p>为什么必须共享：每连接各建一份 {@code ZstdDictCompress} 实测约 3.2MB（200 人 ≈ 670MB），
 * 且旧写法里临时字典对象会被 Cleaner 回收，而压缩上下文仍持有其底层 CDict 指针——
 * 那是悬垂引用（use-after-free）。这里改为强引用 + 引用计数，换代时延迟释放。</p>
 */
public final class ZstdPaperDictRegistry {

    private static final Logger LOGGER = LoggerFactory.getLogger("miku-zstd");
    private static final Object LOCK = new Object();

    private static volatile Entry encoderEntry;
    private static volatile Entry decoderEntry;

    private ZstdPaperDictRegistry() {
    }

    /** 不可变字典条目：{@code id + level} 是身份，任一变化都要重建预计算表。 */
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

        public byte[] bytes() {
            return bytes;
        }

        public ZstdDictCompress compressDict() {
            return compressDict;
        }

        public ZstdDictDecompress decompressDict() {
            return decompressDict;
        }

        public boolean acquire() {
            synchronized (this) {
                if (retired || closed) return false;
                refs.incrementAndGet();
                return true;
            }
        }

        public void release() {
            synchronized (this) {
                if (refs.decrementAndGet() == 0 && retired) closeInternal();
            }
        }

        private void retire() {
            synchronized (this) {
                retired = true;
                if (refs.decrementAndGet() == 0) closeInternal();
            }
        }

        private void closeInternal() {
            if (closed) return;
            closed = true;
            if (compressDict != null) compressDict.close();
            if (decompressDict != null) decompressDict.close();
            LOGGER.debug("[Zstd] 退役字典已释放: id={}", id);
        }
    }

    public static Entry updateEncoder(long id, byte[] dictBytes, int level) {
        if (id == 0 || dictBytes == null || dictBytes.length == 0) {
            swap(true, null);
            return null;
        }
        Entry current = encoderEntry;
        if (current != null && current.id == id && current.level == level) return current;
        Entry fresh = new Entry(id, level, dictBytes, new ZstdDictCompress(dictBytes, level), null);
        swap(true, fresh);
        LOGGER.info("[Zstd] 压缩字典条目重建: id={} level={} size={}B", id, level, dictBytes.length);
        return fresh;
    }

    public static Entry updateDecoder(long id, byte[] dictBytes) {
        if (id == 0 || dictBytes == null || dictBytes.length == 0) {
            swap(false, null);
            return null;
        }
        Entry current = decoderEntry;
        if (current != null && current.id == id) return current;
        Entry fresh = new Entry(id, 0, dictBytes, null, new ZstdDictDecompress(dictBytes));
        swap(false, fresh);
        LOGGER.info("[Zstd] 解压字典条目重建: id={} size={}B", id, dictBytes.length);
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

    public static Entry acquireEncoder() {
        for (int i = 0; i < 8; i++) {
            Entry e = encoderEntry;
            if (e == null) return null;
            if (e.acquire()) return e;
        }
        return null;
    }

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

    public static byte[] encoderDictBytes() {
        Entry e = encoderEntry;
        return e == null ? null : e.bytes;
    }

    public static byte[] decoderDictBytes() {
        Entry e = decoderEntry;
        return e == null ? null : e.bytes;
    }

    public static void shutdown() {
        swap(true, null);
        swap(false, null);
    }
}
