package mikumc.zstd;

import com.github.luben.zstd.ZstdDecompressCtx;
import io.netty.channel.ChannelHandlerContext;
import mikumc.zstd.protocol.ZstdBatchDecoderBase;

/**
 * Miku-ZSTD 解码器（客户端 → 服务端）帧格式 <b>v3</b>（协商版本 v4）。
 *
 * <p>帧解析、内层切包、两道防护（压缩比上限 / scratch 上限）、fail-fast 协议
 * 全部在共享基类 {@link ZstdBatchDecoderBase} 中（两端同一份源码）；
 * 本类只声明解压上下文的来源。</p>
 *
 * <p>服务端不做入站统计——带宽剖析（{@code ZstdBandwidthProfiler}）只统计出站，
 * 因为"降带宽"关心的就是服务端发给玩家的字节量。</p>
 */
public class ZstdBatchDecoder extends ZstdBatchDecoderBase {

    /** handlerAdded 时记录，供 {@link #switchConfirmed()} 读取闸门状态（volatile：跨线程可见）。 */
    private volatile ZstdChannelManager currentManager;

    @Override
    protected ZstdDecompressCtx resolveDecompressContext(ChannelHandlerContext ctx) {
        ZstdChannelManager mgr = ctx.channel().attr(ZstdChannelManager.KEY).get();
        currentManager = mgr;
        if (mgr == null) {
            LOGGER.warn("[Zstd] decoder: ZstdChannelManager not found on channel");
            return null;
        }
        return mgr.getDecompressCtx();
    }

    @Override
    protected void onStoredFrame(int frameBodyBytes) {
        // 服务端无入站统计（有意）
    }

    @Override
    protected void onCompressedFrame(int inBytes, int outBytes) {
        // 服务端无入站统计（有意）
    }

    /** 直存帧也是字典收益的主战场（它们都是"小到不值得压缩"的包），必须采样。 */
    @Override
    protected boolean wantsStoredPayload() {
        return true;
    }

    /**
     * 服务端声明：切换确认之前，入站只可能是原版帧（由 {@link ZstdSwitchGate} 保证）。
     * 确认之后（或超时兜底之后）立即转为严格 zstd 模式。
     */
    @Override
    protected boolean acceptsLegacyFrames() {
        return true;
    }

    /**
     * 读闸门的确认状态；闸门<b>尚未安装</b>时按"不可用"处理。
     *
     * <p>⚠️ 不能默认 {@code true}：本解码器的 handlerAdded 与闸门安装在同一段代码里，
     * 一旦安装顺序被后续改动打乱，默认 true 会让窗口内直接走严格模式 ——
     * 服务端立刻开始把原版帧当 zstd 帧解析，正是要消除的那种断连。默认 false 则最坏
     * 只是多按原版解释几帧（客户端此时确实还在用原版帧）。</p>
     */
    @Override
    protected boolean switchConfirmed() {
        ZstdChannelManager mgr = currentManager;
        return mgr != null && mgr.isSwitchConfirmed();
    }

    /** 直存帧采样：格式与解压帧相同，直接整帧批量提交。 */
    @Override
    protected void onStoredPayload(byte[] data) {
        ZstdSampleTrainer.submitDecoderBatch(data, data.length);
    }

    /**
     * 解压帧采样：整帧批量提交给训练器。
     *
     * <p>以前这里逐包 {@code new byte[]} + {@code arraycopy}，等于给<b>每一个入站包</b>
     * 都在 event loop 上加一次堆分配，而且还要再手写一遍 varint 解析。
     * 现在把整帧交给 {@code addBatch}：解析与过滤都在锁外完成，且只在样本真正入库时
     * 才拷贝——编码侧早就是这个做法，解码侧补齐。</p>
     */
    @Override
    protected void onDecodedPayload(byte[] data) {
        ZstdSampleTrainer.submitDecoderBatch(data, data.length);
    }
}
