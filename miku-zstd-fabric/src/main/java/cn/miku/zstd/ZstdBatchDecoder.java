package cn.miku.zstd;

import com.github.luben.zstd.ZstdDecompressCtx;
import io.netty.channel.ChannelHandlerContext;
import mikumc.zstd.protocol.ZstdBatchDecoderBase;

/**
 * Miku-ZSTD 解码器（Velocity → 客户端）帧格式 <b>v4</b>（协商版本 v5）。
 *
 * <p>帧解析、内层切包、两道防护（压缩比上限 / scratch 上限）、fail-fast 协议
 * 全部在共享基类 {@link ZstdBatchDecoderBase} 中（两端同一份源码）；
 * 本类只声明解压上下文来源与 HUD 统计口径。</p>
 *
 * <h2>统计口径</h2>
 * <p>两个方向都按"帧体线下字节"与"解压后字节"记账，且<b>两侧都计入 rawSize 标记</b>
 * ——这样小包直存时压缩率恰好显示 100%，不会把协议自身的帧头算成"膨胀"。</p>
 */
public class ZstdBatchDecoder extends ZstdBatchDecoderBase {

    private ZstdChannelManager manager;

    @Override
    protected ZstdDecompressCtx resolveDecompressContext(ChannelHandlerContext ctx) {
        manager = ctx.channel().attr(ZstdChannelManager.KEY).get();
        if (manager == null) {
            LOGGER.warn("[Zstd] decoder: ZstdChannelManager not found on channel");
            return null;
        }
        return manager.getDecompressCtx();
    }

    /**
     * 每帧重新取解压上下文。
     *
     * <p>⚠️ 与编码器同理：客户端重协商到"无字典"的服务器时，
     * {@code ZstdChannelManager#resetDicts()} 会 close 旧上下文并换一个新的。
     * 解压走的 {@code decoderDictEntry} 不是 null 的旧实例 —— 继续用会读已释放的
     * 原生内存。读取发生在 decode（event loop）上，替换也只在 event loop 上发生
     * （协商回调），因此不存在并发。</p>
     */
    @Override
    protected ZstdDecompressCtx decompressContextForFrame() {
        ZstdChannelManager m = manager;
        return m == null ? null : m.getDecompressCtx();
    }

    @Override
    protected void onStoredFrame(int frameBodyBytes) {
        // ⚠️ 两侧口径必须一致（都含 rawSize 标记），否则直存帧会显示成 99.9% 而不是 100%
        // —— 直存帧本来就没有解压步骤，把协议头算成"膨胀"是纯误导。
        ZstdStatsData.addRxBatch(1 + frameBodyBytes, 1 + frameBodyBytes);
    }

    @Override
    protected void onCompressedFrame(int inBytes, int outBytes) {
        // 同上：线路侧补上 rawSize 标记（它同样在线上），解压侧也含入该标记。
        ZstdStatsData.addRxBatch(1 + inBytes, 1 + outBytes);
    }
}
