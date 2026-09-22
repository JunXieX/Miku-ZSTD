package mikumc.zstd.paper;

import com.github.luben.zstd.ZstdDecompressCtx;
import io.netty.channel.ChannelHandlerContext;
import mikumc.zstd.protocol.ZstdBatchDecoderBase;

/**
 * Miku-ZSTD 解码器（客户端 → Paper）协议 v4。
 *
 * <p>帧解析、切包、压缩比上限、fail-fast 全部由共享基类提供，本类只声明解压上下文来源。</p>
 */
public class ZstdPaperDecoder extends ZstdBatchDecoderBase {

    @Override
    protected ZstdDecompressCtx resolveDecompressContext(ChannelHandlerContext ctx) {
        ZstdPaperChannelManager mgr = ctx.channel().attr(ZstdPaperChannelManager.KEY).get();
        if (mgr == null) {
            LOGGER.warn("[Zstd] decoder: channel manager not found");
            return null;
        }
        return mgr.getDecompressCtx();
    }

    @Override
    protected void onStoredFrame(int frameBodyBytes) {
        // 预留：入站统计出口
    }

    @Override
    protected void onCompressedFrame(int inBytes, int outBytes) {
        // 预留：入站统计出口
    }

    /** 采样：解压帧按包拆开逐个提交（与编码器同理：整帧样本又大又少，达不到门槛）。 */
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
            ZstdPaperTrainer.submitDecoderSample(sample);
            pos += pktLen;
        }
    }
}
