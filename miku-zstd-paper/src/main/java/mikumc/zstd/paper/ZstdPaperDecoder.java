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

    /** 直存帧也是字典收益的主战场（它们都是"小到不值得压缩"的包），必须采样。 */
    @Override
    protected boolean wantsStoredPayload() {
        return true;
    }

    /** 直存帧采样：格式与解压帧相同，直接整帧批量提交。 */
    @Override
    protected void onStoredPayload(byte[] data) {
        ZstdPaperTrainer.submitDecoderBatch(data, data.length);
    }

    /**
     * 解压帧采样：整帧批量提交给训练器（与编码侧同一个做法）。
     *
     * <p>以前逐包 {@code new byte[]} + {@code arraycopy}，给每个入站包都在 event loop
     * 上加一次堆分配，还顺带手写了一遍 varint 解析。</p>
     */
    @Override
    protected void onDecodedPayload(byte[] data) {
        ZstdPaperTrainer.submitDecoderBatch(data, data.length);
    }
}
