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

    @Override
    protected ZstdDecompressCtx resolveDecompressContext(ChannelHandlerContext ctx) {
        ZstdChannelManager mgr = ctx.channel().attr(ZstdChannelManager.KEY).get();
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
