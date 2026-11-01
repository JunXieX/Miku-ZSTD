package cn.miku.zstd;

import com.github.luben.zstd.ZstdDecompressCtx;
import io.netty.channel.ChannelHandlerContext;
import mikumc.zstd.protocol.ZstdBatchDecoderBase;

/**
 * 服务端 → 客户端的 zstd 下行解码器，帧格式保持 v7，协商版本取共享常量。
 *
 * <p>共享基类负责帧解析、分块解压、内层切包与边界检查。本类只提供下行上下文、
 * 已解包的转发位置与 HUD 下行统计，不实现任何上行或原版压缩帧兼容分支。</p>
 * <pre>
 *   socket → decrypt → splitter → zstd_switch_sniffer → zstd_decoder → decompress → decoder
 * </pre>
 * <p>收到的是已解密且剥掉外层长度的帧体；解出的普通包从 decompress 的 context 向后交付，
 * 跳过原版/Krypton 重复解压。直存帧仍为 {@code [0][packet]}，压缩帧格式保持不变。</p>
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
     * <p>重放登录（重新协商）时 {@code ZstdChannelManager#resetContexts()}
     * 会把旧上下文归还池、换一个新的。旧实例里绑着上一轮滚动历史算出的字典，继续用就会按
     * 错代的字典解压 —— 帧头 {@code dictGen} 对不上，或者更糟：解出一堆看着像包的东西。
     * 读取发生在 decode（event loop）上，替换也只在 event loop 上发生（协商回调），
     * 因此不存在并发。</p>
     */
    @Override
    protected ZstdDecompressCtx decompressContextForFrame() {
        ZstdChannelManager m = manager;
        return m == null ? null : m.getDecompressCtx();
    }

    /**
     * 解出的包要跳过原版 {@code decompress}（本类在它上游）。
     *
     * <p>⚠️ 必须是原版解压处理器本身：{@code fireChannelRead} 从它的 context 出发 = 跳过它、
     * 直接进包解码器。v7 第一版是"补 {@code [0]} 前缀让原版透传"，在装了 <b>Krypton</b> 的客户端上
     * 必炸——Krypton 的 {@code MinecraftCompressDecoder} 会硬校验"未压缩帧必须短于阈值"
     * （{@code checkState(actualUncompressedSize < threshold)}），而解出来的包动辄上千字节
     * （实测 2026-11-01：1036 > 256 → IllegalStateException → 客户端直接断连）。</p>
     *
     * <p>Krypton 会替换同名 handler 的实现，但<b>名字仍是 {@code decompress}</b>（见其实测管线），
     * 所以按名字取 context 对原版与 Krypton 都成立。</p>
     */
    @Override
    protected ChannelHandlerContext decodeForwardTarget(ChannelHandlerContext ctx) {
        io.netty.channel.ChannelHandlerContext c = ctx.pipeline().context("decompress");
        if (c != null) return c;
        return ctx.pipeline().context("compression-decoder");
    }

    @Override
    protected void onStoredFrame(int frameBodyBytes) {
        // ⚠️ 两侧口径必须一致（都含 rawSize 标记），否则直存帧会显示成 99.9% 而不是 100%
        // —— 直存帧本来就没有解压步骤，把协议头算成"膨胀"是纯误导。
        ZstdStatsData.addRxBatch(1 + frameBodyBytes, 1 + frameBodyBytes);
    }

    @Override
    protected void onCompressedFrame(int inBytes, int outBytes) {
        // 线路侧已含完整帧头；原始侧保持一字节直存标记的既有比较基线。
        ZstdStatsData.addRxBatch(inBytes, 1 + outBytes);
    }
}
