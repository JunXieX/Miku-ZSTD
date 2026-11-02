package mikumc.zstd.protocol;

/** 三端共享的协商与下行传输版本，版本不匹配时保留原版连接。 */
public final class ZstdProtocolVersion {

    /**
     * 12：下行改为连接级连续 Zstd 流；上行、能力应答与 switch 确认仍走原版。
     * 与旧批帧格式不兼容，三端必须同时升级；旧客户端通过协商回落原版。
     */
    public static final int NEGOTIATE = 12;

    /**
     * 8：MC 外层长度之后仅为连续 magicless Zstd 流的一个非空片段。
     * 解压字节流为重复的 [VarInt packetLength][packet]，包可跨传输帧。
     * 每次真实 flush、批大小上限或兜底窗口以 Zstd FLUSH 输出全部已提交输入，
     * 不 END、不重置、不直存回退；传输帧按 MC 21-bit 上限分段，内层包跨帧时由接收端重组。
     */
    public static final int FRAME = 8;

    private ZstdProtocolVersion() {
    }
}
