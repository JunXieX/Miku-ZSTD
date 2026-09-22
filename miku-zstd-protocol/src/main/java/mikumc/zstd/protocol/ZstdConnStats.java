package mikumc.zstd.protocol;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

/**
 * 单连接的压缩统计（供 {@code /mikuzstd top} 定位异常连接）。
 *
 * <h2>解决什么问题</h2>
 * 之前只有全局统计：知道"整体压缩率 20%"，但不知道**是哪个玩家在拖后腿**。
 * 常见场景——某个玩家的连接压缩率异常高（比如一直在传大区块），
 * 全局数字被它拉偏，却看不出是谁。这里给每条连接一份独立累计，命令层按流量排序。
 *
 * <p>生命周期与连接一致：{@code ZstdChannelManager} 构造时登记、{@code close()} 时注销。
 * 计数用 {@link LongAdder}（网络线程并发写，且是热路径）。</p>
 */
public final class ZstdConnStats {

    private static final Set<ZstdConnStats> ACTIVE = ConcurrentHashMap.newKeySet();
    private static final AtomicInteger SEQ = new AtomicInteger();

    private final String id;
    private final long startedAt = System.currentTimeMillis();
    private final LongAdder rawBytes = new LongAdder();
    private final LongAdder wireBytes = new LongAdder();
    private final LongAdder frames = new LongAdder();

    public ZstdConnStats() {
        this.id = "#" + SEQ.incrementAndGet();
        ACTIVE.add(this);
    }

    /** 编码器每帧回调（热路径，只做三次加法）。 */
    public void record(int raw, int wire) {
        rawBytes.add(raw);
        wireBytes.add(wire);
        frames.increment();
    }

    /** 连接关闭时注销（由 ChannelManager#close 调用）。 */
    public void remove() {
        ACTIVE.remove(this);
    }

    /** 按「原始字节」降序取前 n 条摘要，供命令输出。 */
    public static List<String> top(int n) {
        List<ZstdConnStats> list = new ArrayList<>(ACTIVE);
        list.sort((a, b) -> Long.compare(b.rawBytes.sum(), a.rawBytes.sum()));
        List<String> out = new ArrayList<>();
        for (int i = 0; i < Math.min(n, list.size()); i++) {
            out.add(list.get(i).describe());
        }
        return out;
    }

    /** 全部连接的合计压缩率（用于和全局统计对账）。 */
    public static String totalSummary() {
        long raw = 0;
        long wire = 0;
        for (ZstdConnStats s : ACTIVE) {
            raw += s.rawBytes.sum();
            wire += s.wireBytes.sum();
        }
        double ratio = raw > 0 ? 100.0 * wire / raw : 100.0;
        return String.format("活跃 %d 条 | 合计原始 %.1fMB → %.1fMB (%.1f%%)",
                ACTIVE.size(), raw / 1048576.0, wire / 1048576.0, ratio);
    }

    private String describe() {
        long raw = rawBytes.sum();
        long wire = wireBytes.sum();
        double ratio = raw > 0 ? 100.0 * wire / raw : 100.0;
        return String.format("%-6s 原始 %7.1fMB → %7.1fMB (%5.1f%%)  帧 %6d  存活 %ds",
                id, raw / 1048576.0, wire / 1048576.0, ratio, frames.sum(),
                (System.currentTimeMillis() - startedAt) / 1000);
    }
}
