package mikumc.zstd.protocol;

/**
 * 三端共享的协议版本常量（单一来源）。
 *
 * <p>⚠️ 以前 {@code PROTOCOL_VERSION} 在 Velocity / Paper / Fabric 各存一份，改版本时漏改一处，
 * 结果是「模组装了但 zstd 从不生效」——客户端版本不一致时是<b>静默回落</b>，
 * 日志里只有一行 debug，几乎不可能在现网发现。现在三端都引用这里。</p>
 */
public final class ZstdProtocolVersion {

    /**
     * 协商协议版本（登录期在 negotiate 载荷里交换，三端必须一致）。
     *
     * <p>9：上行（客户端→服务端）<b>彻底去掉 zstd</b>，只保留批处理：该方向的帧固定为
     * 存储批帧 {@code [0][count][pktLen][pkt]...}，不压缩、无 {@code dictGen}、不建压缩上下文。
     * 依据：上行单包均值 28.8B，压缩后反而膨胀 13%，字典收益 0.0%（2026-11-01 实测，
     * 跑图/飞行/PVP 场景）；唯一有效的是批处理省掉的每包前缀。</p>
     *
     * <p>8：删除训练字典（trainer / 字典下发 / negotiate 里的 dictId 与 flags）。
     * 协商载荷简化为 {@code [int 版本]}，应答简化为 {@code [varint 状态]}。
     * 两端版本不一致时客户端干净回落原版。</p>
     *
     * <p>7：帧头新增 {@code dictGen}（滚动历史字典代数），两端据此<b>校验历史是否同源</b>；
     * 同时编码器改为"不替换原版压缩处理器、从它的 context 往下写"，解码器改为
     * "挂在原版 decompress 上游、发出 {@code [0][packet]}"。线格式因此不与 v6 兼容
     * ——旧客户端会在协商阶段看到版本不符并<b>干净回落原版</b>（能进服，只是不压缩）。</p>
     *
     * <p>6：帧内改成 [rawSize][blockCount] + 每块 [blockRawLen][chunkLen][chunk]（大帧分块并行）。</p>
     */
    public static final int NEGOTIATE = 9;

    /** 帧格式版本（写进文档；线格式变更时与 {@link #NEGOTIATE} 一起升）。 */
    public static final int FRAME = 7;

    private ZstdProtocolVersion() {
    }
}
