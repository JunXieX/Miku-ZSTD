package mikumc.zstd.paper;

import com.github.luben.zstd.ZstdDecompressCtx;
import io.netty.channel.ChannelHandlerContext;
import mikumc.zstd.protocol.ZstdBatchDecoderBase;

/**
 * Miku-ZSTD 解码器（客户端 → Paper）帧格式 v6（协商版本 v7）。
 *
 * <p>帧解析、切包、压缩比上限、fail-fast 全部由共享基类提供，本类只声明解压上下文来源。</p>
 *
 * <p>安装位置：原版 {@code decompress} 的<b>上游</b>。解出的每个包都以原版未压缩帧
 * {@code [0][packet]} 交出，由原版 {@code decompress} 原样透传下去——它一直留在管线里，
 * 我们不拆它（见 {@link ZstdPaperChannelManager#installDecoder(io.netty.channel.Channel)}）。</p>
 */
public class ZstdPaperDecoder extends ZstdBatchDecoderBase {

    /** handlerAdded 时记录，供 {@link #switchConfirmed()} 读取闸门状态（volatile：跨线程可见）。 */
    private volatile ZstdPaperChannelManager currentManager;

    @Override
    protected ZstdDecompressCtx resolveDecompressContext(ChannelHandlerContext ctx) {
        ZstdPaperChannelManager mgr = ctx.channel().attr(ZstdPaperChannelManager.KEY).get();
        currentManager = mgr;
        if (mgr == null) {
            LOGGER.warn("[Zstd] decoder: channel manager not found");
            return null;
        }
        return mgr.getDecompressCtx();
    }

    /**
     * 解出的包要跳过原版 {@code decompress}（本类在它上游）。
     *
     * <p>⚠️ 必须是<b>原版解压处理器</b>本身：{@code fireChannelRead} 从它的 context 出发，
     * 等于跳过它、直接进包解码器。v7 第一版是"补 {@code [0]} 前缀让原版透传"，在装了
     * <b>Krypton</b> 的客户端上会触发它那条"未压缩帧必须短于阈值"的硬校验（实测直接断连）。
     * 找不到（原版压缩未启用 / 已被第三方拆掉）就返回 {@code null}，基类退回自己的 ctx。</p>
     */
    @Override
    protected ChannelHandlerContext decodeForwardTarget(ChannelHandlerContext ctx) {
        io.netty.channel.ChannelHandlerContext c = ctx.pipeline().context("decompress");
        if (c != null) return c;
        io.netty.channel.ChannelHandlerContext c = ctx.pipeline().context("compression-decoder");
        if (c != null) return c;
        return null;
    }

    @Override
    protected void onStoredFrame(int frameBodyBytes) {
        // 服务端无入站统计（有意）——与 Velocity 端一致：带宽口径只统计出站
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
     * 服务端声明：切换确认之前，入站只可能是原版帧（由 {@link mikumc.zstd.protocol.ZstdSwitchGate}
     * 保证）。确认之后立即转为严格 zstd 模式。
     */
    @Override
    protected boolean acceptsLegacyFrames() {
        return true;
    }

    /**
     * 读闸门的确认状态；闸门<b>尚未安装</b>时按"未确认"处理（宁可多按原版解释几帧，
     * 也不能在切换窗口里把原版帧当 zstd 帧解析——那正是断连的成因）。
     */
    @Override
    protected boolean switchConfirmed() {
        ZstdPaperChannelManager mgr = currentManager;
        return mgr != null && mgr.isSwitchConfirmed();
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
