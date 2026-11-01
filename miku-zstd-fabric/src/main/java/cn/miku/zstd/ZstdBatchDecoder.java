package cn.miku.zstd;

import com.github.luben.zstd.ZstdDecompressCtx;
import io.netty.channel.ChannelHandlerContext;
import mikumc.zstd.protocol.ZstdBatchDecoderBase;

/**
 * Miku-ZSTD 解码器（服务端 → 客户端）帧格式 <b>v6</b>（协商版本 v7）。
 *
 * <p>帧解析、内层切包、两道防护（压缩比上限 / scratch 上限）、fail-fast 协议
 * 全部在共享基类 {@link ZstdBatchDecoderBase} 中（三端同一份源码）；
 * 本类只声明解压上下文来源与 HUD 统计口径。</p>
 *
 * <h2>安装位置与顺序前提</h2>
 * <p>本类挂在原版 {@code decompress} 的<b>上游</b>（{@code addBefore}），不替换它：解出的每个包
 * 都以原版未压缩帧 {@code [0][packet]} 交出，由原版 {@code decompress} 原样透传下去。完整的
 * 入站顺序是：</p>
 * <pre>
 *   socket → decrypt → splitter → zstd_switch_sniffer → zstd_decoder（本类）→ decompress → decoder
 * </pre>
 * <p>⚠️ 这个顺序是本类的<b>隐含前提</b>：<b>正版加密</b>时 {@code decrypt} 插在 {@code splitter}
 * 之前，所以本类拿到的必然是"已解密、且外层长度已被 {@code splitter} 剥掉"的完整帧——它才能
 * 把首字节当 {@code rawSize} 读。若别的模组把解密挪到 {@code splitter} 之后（或干脆挪到本类
 * 下游），进到这里的会是密文，帧头必然是非法 varint，表现为"一连服务器就断"。</p>
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
        // 同上：线路侧补上 rawSize 标记（它同样在线上），解压侧也含入该标记。
        ZstdStatsData.addRxBatch(1 + inBytes, 1 + outBytes);
    }
}
