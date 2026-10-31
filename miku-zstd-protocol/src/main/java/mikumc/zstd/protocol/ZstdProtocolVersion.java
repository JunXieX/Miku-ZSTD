package mikumc.zstd.protocol;

/**
 * 三端共享的协议版本常量（单一来源）。
 *
 * <p>⚠️ 以前 {@code PROTOCOL_VERSION} 在 Velocity / Paper / Fabric 各存一份，改版本时漏改一处，
 * 结果是「模组装了但 zstd 从不生效」——客户端版本不一致时是<b>静默回落</b>，
 * 日志里只有一行 debug，几乎不可能在现网发现。现在三端都引用这里。</p>
 */
public final class ZstdProtocolVersion {

    /** 协商协议版本（登录期在 negotiate 载荷里交换，三端必须一致）。 */
    /**
     * 6：帧内改成 [rawSize][blockCount] + 每块 [blockRawLen][chunkLen][chunk]（大帧分块并行）。
     * 两端必须同版本。
     */
    public static final int NEGOTIATE = 6;

    /** 帧格式版本（写进文档；线格式变更时与 {@link #NEGOTIATE} 一起升）。 */
    public static final int FRAME = 4;

    private ZstdProtocolVersion() {
    }
}
