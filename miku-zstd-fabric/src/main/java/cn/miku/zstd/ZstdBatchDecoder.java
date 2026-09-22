package cn.miku.zstd;

import com.github.luben.zstd.ZstdDecompressCtx;
import io.netty.channel.ChannelHandlerContext;
import mikumc.zstd.protocol.ZstdBatchDecoderBase;

/**
 * Miku-ZSTD 解码器（Velocity → 客户端）帧格式 <b>v3</b>（协商版本 v4）。
 *
 * <p>帧解析、内层切包、两道防护（压缩比上限 / scratch 上限）、fail-fast 协议
 * 全部在共享基类 {@link ZstdBatchDecoderBase} 中（两端同一份源码）；
 * 本类只声明解压上下文来源与 HUD 统计口径。</p>
 *
 * <h2>统计口径</h2>
 * <p>两个方向都按"帧体线下字节"与"解压后字节"记账，其中解压后口径额外
 * {@code +1} 以含入 rawSize 标记——这样小包直存时压缩率恰好显示 100%，
 * 不会把协议自身的帧头算成"膨胀"。</p>
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
        ZstdStatsData.addRxBatch(frameBodyBytes, 1 + frameBodyBytes);
    }

    @Override
    protected void onCompressedFrame(int inBytes, int outBytes) {
        ZstdStatsData.addRxBatch(inBytes, 1 + outBytes);
    }
}
