package mikumc.zstd.protocol;

import java.util.zip.CRC32;

/**
 * 字典 id 的<b>唯一定义</b>（三端共用）。
 *
 * <h2>这是一个跨端契约，不是一个工具函数</h2>
 * <p>服务端用这个 id 在 negotiate 里声明"我有哪些字典"，
 * 客户端用<b>同一算法</b>把收到的字典字节换算成缓存键（{@code DictCache}）。
 * 两端算法一旦不一致，会产生一类极隐蔽的故障：客户端查缓存时"以为没有"
 * （于是每次都请求重推，白费流量），或者更糟——"以为有"却拿到了别的字节，
 * 于是服务端带字典压缩、客户端用错误的字典解压→断连。</p>
 *
 * <p>所以算法只在这里实现一次，三端各自调用（原来是服务端训练器一处 CRC32、
 * Paper 训练器一处、客户端 {@code crcOf} 两处，共四份独立实现）。</p>
 *
 * <h2>为什么是 CRC32</h2>
 * <p>字典是训练产物、内容寻址最自然：内容变则 id 变，无需额外版本号管理。
 * CRC32 的碰撞概率对一个"同内容才复用"的场景足够低，且计算是零成本的。</p>
 */
public final class ZstdDictId {

    /** 未装载字典 / 无字典的哨兵值。 */
    public static final long NONE = 0L;

    private ZstdDictId() {
    }

    /**
     * 字典字节 → 字典 id（0 ~ 2^32-1）。
     *
     * @return 字典 id；{@code null} 或空数组返回 {@link #NONE}
     */
    public static long of(byte[] dictBytes) {
        if (dictBytes == null || dictBytes.length == 0) {
            return NONE;
        }
        CRC32 crc = new CRC32();
        crc.update(dictBytes, 0, dictBytes.length);
        return crc.getValue();
    }

    /**
     * 字典字节 → 线上校验字段（32 位，与 {@link #of} 同一数值）。
     *
     * <p>协议里 {@code zstd:dict} 的每个方向用 {@code [int crc][int len][bytes]}
     * 承载字典，这个 int 必须是 {@link #of} 的低 32 位——客户端读回后再用
     * {@link #of} 算出的值校验，两处必须同源。</p>
     */
    public static int wireChecksum(byte[] dictBytes) {
        return (int) of(dictBytes);
    }
}
