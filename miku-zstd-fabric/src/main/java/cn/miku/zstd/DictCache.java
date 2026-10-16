package cn.miku.zstd;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 训练字典缓存：避免同一字典在重连时重复传输。
 *
 * <h2>为什么必须限容</h2>
 * <p>缓存键是字典 id（内容寻址的 CRC32），所以<b>只要内容变一点点就是一个新键</b>。
 * 而服务端会持续从真实流量重训字典——每采纳一代新字典，所有客户端下次登录都会经历
 * "拉取新字典 → 按新 id 入库"。于是这个 Map 在长生命周期客户端上是<b>只增不减</b>的：
 * 连着玩几小时、或连过多个服务器，就会积累几代 × 每代最大 1MB 的字节数组。</p>
 *
 * <p>限容在这里是<b>安全</b>的：缓存只用于回答"这次登录还需不需要下载字典"。
 * 淘汰某个 id 顶多让下次登录重新下载一次那几个字节，不会影响正确性
 *（当前正在使用的字典由 {@link ZstdDictRegistry} 以强引用 + 引用计数持有，
 * 与本缓存的淘汰互不相干）。</p>
 *
 * <h2>为什么用 LRU 而不是 FIFO</h2>
 * <p>客户端可能反复连同一台服务器（该服务器的当前字典就是"最近用过"的那一个），
 * 也可能只连过一次的旧服务器字典永远不再需要。按访问顺序淘汰，正好先扔掉后者。</p>
 */
public final class DictCache {

    /**
     * 最多保留几代字典。
     *
     * <p>取 6：足以覆盖"最近连过几台服务器 + 当前服务器换了几代字典"的正常情况，
     * 又能在极端情况下把内存钉在 6MB 以内（每代字典上限 1MB）。</p>
     */
    private static final int MAX_ENTRIES = 6;

    /** accessOrder=true → 迭代顺序即"最久未用在前"，淘汰时直接取第一个。 */
    private static final Map<Long, byte[]> CACHE = new LinkedHashMap<>(16, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<Long, byte[]> eldest) {
            return size() > MAX_ENTRIES;
        }
    };

    private DictCache() {
    }

    public static synchronized byte[] get(long dictId) {
        return CACHE.get(dictId);
    }

    public static synchronized void put(long dictId, byte[] data) {
        if (data == null || data.length == 0) {
            return;
        }
        CACHE.put(dictId, data);
        // removeEldestEntry 会在插入后自动淘汰，无需在此手工处理
    }

    public static synchronized boolean contains(long dictId) {
        return CACHE.containsKey(dictId);
    }
}
