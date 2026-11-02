package mikumc.zstd.paper;

import com.github.luben.zstd.ZstdCompressCtx;
import io.netty.channel.ChannelHandlerContext;
import mikumc.zstd.protocol.ZstdBatchEncoderBase;
import mikumc.zstd.protocol.ZstdCompressLock;

/**
 * Miku-ZSTD 编码器（Paper → 客户端）连续流帧格式 v8（协商版本见共享协议常量）。
 *
 * <p>⚠️ 与 Velocity 端唯一的协议差异：服务端管线<b>有 prepender</b>，外层长度由它负责，
 * 因此本编码器<b>不写</b> bodyLen（与 Fabric 客户端一致；Velocity 端才需要写）。
 * 这条差异没有因为"改成插入"而改变：我们只是绕开了原版 {@code compress}，
 * {@code prepender} 仍在写帧路径的下游。</p>
 *
 * <p>安装位置：原版 {@code compress} 之后，成帧结果从<b>它的 context</b> 往下写
 *（见 {@link #downstream(ChannelHandlerContext)}）。</p>
 */
public class ZstdPaperEncoder extends ZstdBatchEncoderBase {

    private ZstdPaperChannelManager manager;

    @Override
    protected ZstdCompressCtx resolveCompressContext(ChannelHandlerContext ctx) {
        manager = ctx.channel().attr(ZstdPaperChannelManager.KEY).get();
        return manager == null ? null : manager.getCompressCtx();
    }

    /**
     * 压缩已卸载到线程池，而 {@code compressCtx} 由 ChannelManager 持有、并在关连接时归还
     * （池内部会 reset）。因此这里必须返回<b>与它相同的那把锁</b>——否则池线程压缩
     * 会与「归还上下文」并发，后者是 use-after-free（SIGSEGV）。
     */
    @Override
    protected ZstdCompressLock compressLock() {
        ZstdPaperChannelManager m = manager;
        return m == null ? super.compressLock() : m.compressLock();
    }

    @Override
    protected boolean writeBodyLen() {
        return false;
    }

    /**
     * 成帧结果从<b>原版压缩处理器的 context</b> 继续往下写：跳过它，但不拆它。
     *
     * <p>原版 {@code compress} 一直留在管线里（见
     * {@link ZstdPaperChannelManager#installEncoder(io.netty.channel.Channel)}），
     * 我们从它的 context 写出，只绕开它自己，后面的 {@code prepender} / 加密 / socket
     * 照常经过——外层长度因此仍由 prepender 补，{@link #writeBodyLen()} 保持 false。</p>
     *
     * <p>⚠️ 候选名要与安装时的锚点一致：常规是 {@code compress}，装了别的压缩实现时是
     * {@code compression-encoder}。两个都查不到时返回 {@code null}，由基类关闭连接；
     * 已进入的连续流不能退回原版压缩或跳过失败批次。</p>
     */
    @Override
    protected ChannelHandlerContext downstream(ChannelHandlerContext ctx) {
        var p = ctx.pipeline();
        ChannelHandlerContext vanilla = p.context("compress");
        return vanilla != null ? vanilla : p.context("compression-encoder");
    }

    @Override
    protected void onFrame(int rawBytes, int wireBytes, boolean compressed) {
        ZstdPaperMonitor.record(rawBytes, wireBytes);
        ZstdPaperChannelManager m = manager;
        if (m != null) {
            m.stats.record(rawBytes, wireBytes);
        }
    }

    @Override
    protected int configBatchWindowMs() {
        return ZstdPaperConfig.INSTANCE.batchWindowMs;
    }

    @Override
    protected int configBatchMaxPackets() {
        return ZstdPaperConfig.INSTANCE.batchMaxPackets;
    }
}
