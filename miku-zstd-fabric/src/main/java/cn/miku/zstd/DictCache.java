package cn.miku.zstd;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** 训练字典缓存：避免同一字典在重连时重复传输。 */
public class DictCache {
    private static final Map<Long, byte[]> cache = new ConcurrentHashMap<>();

    public static byte[] get(long dictId) {
        return cache.get(dictId);
    }

    public static void put(long dictId, byte[] data) {
        cache.put(dictId, data);
    }

    public static boolean contains(long dictId) {
        return cache.containsKey(dictId);
    }
}
