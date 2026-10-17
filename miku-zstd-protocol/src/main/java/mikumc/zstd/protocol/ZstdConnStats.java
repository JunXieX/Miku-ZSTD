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

    /**
     * 本连接是否<b>真的走了 zstd 流量</b>（至少成过一帧）。
     *
     * <p>⚠️ 必须按这个口径筛选，而不是"在 ACTIVE 集合里就算"：两端登记时机的语义不同——
     * Velocity 只在识别到 zstd 客户端时才创建 manager（集合里天然全是 zstd 连接），
     * 而 Paper 的 manager 是<b>每条连接</b>都建的（它要做协商，原版客户端也要发过协商查询）。
     * 若不过滤，Paper 端的 {@code /mikuzstd top} 会把所有原版玩家都列进来（零字节、零帧），
     * "活跃 N 条"被在线人数灌水，且"当前没有走 zstd 的连接"这句话永远不会出现。</p>
     */
    private boolean carriesZstd() {
        return frames.sum() > 0;
    }

    /** 按「原始字节」降序取前 n 条摘要，供命令输出。 */
    public static List<String> top(int n) {
        List<ZstdConnStats> list = new ArrayList<>(ACTIVE);
        // 只保留真正产生过 zstd 帧的连接（见 carriesZstd 的说明）
        list.removeIf(s -> !s.carriesZstd());
        list.sort((a, b) -> Long.compare(b.rawBytes.sum(), a.rawBytes.sum()));
        List<String> out = new ArrayList<>();
        for (int i = 0; i < Math.min(n, list.size()); i++) {
            out.add(list.get(i).describe());
        }
        return out;
    }

    /** 全部连接（仅计真正走过 zstd 的）的合计压缩率（用于和全局统计对账）。 */
    public static String totalSummary() {
        long raw = 0;
        long wire = 0;
        int count = 0;
        for (ZstdConnStats s : ACTIVE) {
            if (!s.carriesZstd()) continue; // 同 top：排除只协商过、从未走过 zstd 的连接
            raw += s.rawBytes.sum();
            wire += s.wireBytes.sum();
            count++;
        }
        double ratio = raw > 0 ? 100.0 * wire / raw : 100.0;
        return String.format("活跃 %d 条 | 合计原始 %s → %s (%.1f%%)",
                count, ZstdBossBarFormat.fmtBytes(raw), ZstdBossBarFormat.fmtBytes(wire), ratio);
    }

    private String describe() {
        long raw = rawBytes.sum();
        long wire = wireBytes.sum();
        double ratio = raw > 0 ? 100.0 * wire / raw : 100.0;
        return String.format("%-6s 原始 %9s → %9s (%5.1f%%)  帧 %6d  存活 %ds",
                id, ZstdBossBarFormat.fmtBytes(raw), ZstdBossBarFormat.fmtBytes(wire), ratio, frames.sum(),
                (System.currentTimeMillis() - startedAt) / 1000);
    }
}
