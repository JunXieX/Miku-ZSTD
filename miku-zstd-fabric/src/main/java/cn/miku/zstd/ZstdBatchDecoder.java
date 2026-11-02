package cn.miku.zstd;

import com.github.luben.zstd.ZstdDecompressCtx;
import io.netty.channel.ChannelHandlerContext;
import mikumc.zstd.protocol.ZstdBatchDecoderBase;

/**
 * 服务端 → 客户端的 zstd 下行解码器，连续流帧格式 v8，协商版本取共享常量。
 *
 * <p>共享基类负责持续解压、跨帧内层切包与边界检查。本类只提供下行上下文、
 * 已解包的转发位置与 HUD 下行统计，不实现任何上行或原版压缩帧兼容分支。</p>
 * <pre>
 *   socket → decrypt → splitter → zstd_switch_sniffer → zstd_decoder → decompress → decoder
 * </pre>
 * <p>收到的是已解密且剥掉外层长度的帧体；解出的普通包从 decompress 的 context 向后交付，
 * 跳过原版/Krypton 重复解压。每个帧体仅含同一条 magicless Zstd 流的后续字节。</p>
 */
public class ZstdBatchDecoder extends ZstdBatchDecoderBase {

    private ZstdChannelManager manager;
    private boolean streamStarted;

    boolean hasStartedStream() {
        return streamStarted;
    }

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
     * 会把旧上下文归还池、换一个新的；旧实例的流状态属于上一轮协商，不能继续复用。
     * 同一轮连接期间始终使用同一个上下文，不在传输帧之间重置或装载字典。
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
        // 交包可能同步触发压缩设置；必须先标记，不能等整帧统计回调。
        streamStarted = true;
        io.netty.channel.ChannelHandlerContext c = ctx.pipeline().context("decompress");
        if (c != null) return c;
        return ctx.pipeline().context("compression-decoder");
    }

    @Override
    protected void onCompressedFrame(int wireBytes, int rawBytes) {
        streamStarted = true;
        // 基类已计入 MC 外层长度；原始侧是实际解出的内层流字节，不另补标记。
        ZstdStatsData.addRxBatch(wireBytes, rawBytes);
    }
}
