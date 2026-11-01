package mikumc.zstd;

import com.github.luben.zstd.ZstdDecompressCtx;
import io.netty.channel.ChannelHandlerContext;
import mikumc.zstd.protocol.ZstdBatchDecoderBase;

/**
 * Miku-ZSTD 解码器（客户端 → 服务端）帧格式 <b>v7</b>（协商版本 v9）。
 *
 * <p>帧解析、内层切包、两道防护（压缩比上限 / scratch 上限）、fail-fast 协议
 * 全部在共享基类 {@link ZstdBatchDecoderBase} 中（两端同一份源码）；
 * 本类只声明解压上下文的来源，以及<b>本方向的帧形态</b>（见 {@link #storedBatchFrames}）。</p>
 *
 * <p>⚠️ <b>5.0.43 起上行不再压缩</b>：实测上行单包均值只有 28.8B，压缩后反而膨胀 13%，
 * 而"压缩"这一步一个字节都没省下来（全靠直存兜底）。所以这个方向只保留批处理——
 * 客户端把一次 flush 内的多个包写成<b>一个</b>存储批帧 {@code [0][count][pktLen][pkt]...}，
 * 本解码器把它拆回逐包下传。zstd 只留在下行（区块/注册表/TAB 那些真能压的数据）。</p>
 *
 * <p>⚠️ <b>安装位置</b>：由 {@code ZstdChannelManager#installDecoder} 以 {@code addBefore}
 * 插在原版压缩解码器的<b>上游</b>，解出的每个包以原版未压缩帧 {@code [0][packet]} 交出，
 * 由原版 {@code decompress} 原样透传 —— 原版处理器留在管线里，我们不替换它。</p>
 *
 * <p>服务端不做入站统计——带宽剖析（{@code ZstdBandwidthProfiler}）只统计出站，
 * 因为"降带宽"关心的就是服务端发给玩家的字节量。</p>
 *
 * <p>⚠️ 5.0.42 起<b>不再覆写采样钩子</b>（{@code onDecodedPayload} / {@code onStoredPayload}
 * / {@code wantsStoredPayload}）：训练字典已整体删除，覆写它们只会白拷一份入站字节。
 * 协议基类的空实现即是最终形态。</p>
 */
public class ZstdBatchDecoder extends ZstdBatchDecoderBase {

    /** handlerAdded 时记录，供 {@link #switchConfirmed()} 读取闸门状态（volatile：跨线程可见）。 */
    private volatile ZstdChannelManager currentManager;

    /**
     * 解压上下文的来源。
     *
     * <p>⚠️ 5.0.43 起<b>上行已不压缩</b>，这个 ctx 实际上不会再有压缩帧来用；基类仍会在
     * {@code handlerAdded} 里调用本方法，所以先保留（编译绿了再单独清理，避免一次改动
     * 同时动"方向语义"和"资源生命周期"两件事）。</p>
     *
     * <p>这里顺带把 manager 记下来：{@link #switchConfirmed()} 靠它读闸门状态，
     * 与解压上下文是否被使用无关，不能因为"上行不压缩"就把这行一起删掉。</p>
     */
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

    /**
     * 解出的包要跳过原版压缩解码器（本类在它上游）。
     *
     * <p>⚠️ 必须是<b>原版解压处理器</b>本身：{@code fireChannelRead} 从它的 context 出发，
     * 等于跳过它、直接进包解码器。v7 第一版是"补 {@code [0]} 前缀让原版透传"，在装了
     * <b>Krypton</b> 的客户端上会触发它那条"未压缩帧必须短于阈值"的硬校验（实测直接断连）。
     * 找不到（原版压缩未启用 / 已被第三方拆掉）就返回 {@code null}，基类退回自己的 ctx。</p>
     */
    @Override
    protected ChannelHandlerContext decodeForwardTarget(ChannelHandlerContext ctx) {
        for (String name : new String[]{"compression-decoder", "decompress"}) {
            io.netty.channel.ChannelHandlerContext c = ctx.pipeline().context(name);
            if (c != null) return c;
        }
        return null;
    }

    @Override
    protected void onStoredFrame(int frameBodyBytes) {
        // 服务端无入站统计（有意）
    }

    @Override
    protected void onCompressedFrame(int inBytes, int outBytes) {
        // 服务端无入站统计（有意）
    }

    /**
     * 本方向的帧是"存储批帧"：{@code [0][varint count] + count × [varint pktLen][packet]}。
     *
     * <p>返回 {@code true} 的理由只有一个：<b>这个方向不再压缩</b>（上行单包 28.8B，
     * 压完反而膨胀 13%，字典收益 0.0%），客户端只做批处理。于是 {@code rawSize == 0}
     * 的帧不再"恰好一个包"，而是"<b>一帧多包</b>"，必须由基类按 {@code count} 逐包拆开下传；
     * 若按旧语义整帧当一个包交出去，包解码器会把这个帧当成一个超长的畸形包 → 直接断连。</p>
     *
     * <p>⚠️ 与切换窗口不冲突：确认之前的入站帧仍走基类的 {@code legacyFallbackAllowed()}
     * 分支原样交给原版解压器（客户端那时还没切），只有确认之后才按存储批解析。</p>
     *
     * <p>⚠️ 长度不自洽（count/pktLen 越界）由基类按协议错误关连接，不做"尽力而为"的猜测——
     * 猜错包边界比断连更难查。</p>
     */
    @Override
    protected boolean storedBatchFrames() {
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

}
