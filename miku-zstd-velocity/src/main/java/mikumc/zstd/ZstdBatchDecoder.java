package mikumc.zstd;

import com.github.luben.zstd.ZstdDecompressCtx;
import io.netty.channel.ChannelHandlerContext;
import mikumc.zstd.protocol.ZstdBatchDecoderBase;

/**
 * Miku-ZSTD 解码器（客户端 → 服务端）协议 <b>v3</b>。
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

    /** 采样：解压帧按包拆开逐个提交。 */
    @Override
    protected void onDecodedPayload(byte[] data) {
        if (data == null || data.length == 0) return;
        int pos = 0;
        while (pos < data.length) {
            int pktLen = 0;
            int shift = 0;
            while (pos < data.length && shift <= 28) {
                byte b = data[pos++];
                pktLen |= (b & 0x7F) << shift;
                if ((b & 0x80) == 0) break;
                shift += 7;
            }
            if (pktLen <= 0 || pos + pktLen > data.length) break;
            byte[] sample = new byte[pktLen];
            System.arraycopy(data, pos, sample, 0, pktLen);
            ZstdSampleTrainer.submitDecoderSample(sample);
            pos += pktLen;
        }
    }
}
