package mikumc.zstd.paper;

import com.github.luben.zstd.ZstdCompressCtx;
import io.netty.channel.ChannelHandlerContext;
import mikumc.zstd.protocol.ZstdBatchEncoderBase;
import mikumc.zstd.protocol.ZstdCompressLock;

/**
 * Miku-ZSTD 编码器（Paper → 客户端）帧格式 v6（协商版本 v7）。
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
     * 压缩已卸载到线程池；字典由 ChannelManager 改写 {@code compressCtx}、且它负责在
     * 关连接时 close 该上下文，所以这里必须返回<b>与它相同的那把锁</b>——否则池线程压缩
     * 会与「换字典 / close」并发，后者是 use-after-free（SIGSEGV）。
     */
    @Override
    protected ZstdCompressLock compressLock() {
        ZstdPaperChannelManager m = manager;
        return m == null ? super.compressLock() : m.compressLock();
    }

    @Override
    protected boolean hasCompressDict() {
        ZstdPaperChannelManager m = manager;
        return m != null && m.hasCompressDict();
    }

    @Override
    protected int skipCompressThreshold(boolean hasCompressDict) {
        ZstdPaperConfig cfg = ZstdPaperConfig.INSTANCE;
        return hasCompressDict ? cfg.skipCompressBelowBytesWithDict : cfg.skipCompressBelowBytes;
    }

    @Override
    protected int configLevel() {
        return ZstdPaperConfig.INSTANCE.level;
    }

    @Override
    protected int configWindowLog() {
        return ZstdPaperConfig.INSTANCE.windowLog;
    }

    @Override
    protected com.github.luben.zstd.ZstdDictCompress compressDict() {
        ZstdPaperChannelManager m = manager;
        return m == null ? null : m.compressDict();
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
     * {@code compression-encoder}。两个都查不到（管线被第三方拆改过）就返回 {@code null}，
     * 交给基类退回自己的 ctx：那一帧会多经一次原版压缩、对端多半解不开，但<b>不会丢包</b>
     * ——丢包会让包解码器错位，比丢一帧难查得多。</p>
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

    /**
     * 采样：把整批原始字节交给训练器，由它在锁外按包拆开、过滤、只为入库样本拷贝
     * （见 {@code ZstdPaperTrainer#addBatch}）。
     */
    @Override
    protected void onRawBatch(io.netty.buffer.ByteBuf raw, int length) {
        // ⚠️ 先问训练器要不要，再决定拷不拷：这份拷贝跑在 event loop 上，而攒批以 flush
        // 为界（tick 末尾、区块发送完成都会 flush），高流量时每条连接每秒成百批，
        // 训练环满了之后这些拷贝就是全额白做。
        if (!ZstdPaperTrainer.wantsEncoderSample()) {
            return;
        }
        byte[] copy = new byte[length];
        raw.getBytes(raw.readerIndex(), copy, 0, length);
        ZstdPaperTrainer.submitEncoderBatch(copy, length);
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
