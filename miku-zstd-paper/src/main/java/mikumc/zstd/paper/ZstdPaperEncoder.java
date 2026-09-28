package mikumc.zstd.paper;

import com.github.luben.zstd.ZstdCompressCtx;
import io.netty.channel.ChannelHandlerContext;
import mikumc.zstd.protocol.ZstdBatchEncoderBase;

/**
 * Miku-ZSTD 编码器（Paper → 客户端）帧格式 v3（协商版本 v4）。
 *
 * <p>⚠️ 与 Velocity 端唯一的协议差异：服务端管线<b>有 prepender</b>，外层长度由它负责，
 * 因此本编码器<b>不写</b> bodyLen（与 Fabric 客户端一致；Velocity 端才需要写）。</p>
 */
public class ZstdPaperEncoder extends ZstdBatchEncoderBase {

    private ZstdPaperChannelManager manager;

    @Override
    protected ZstdCompressCtx resolveCompressContext(ChannelHandlerContext ctx) {
        manager = ctx.channel().attr(ZstdPaperChannelManager.KEY).get();
        return manager == null ? null : manager.getCompressCtx();
    }

    /**
     * 压缩已卸载到线程池；字典由 ChannelManager 改写 {@code compressCtx}，
     * 所以这里必须返回<b>与它相同的那把锁</b>，否则池线程压缩与换字典会并发写同一个 ctx。
     */
    @Override
    protected Object compressLock() {
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
    protected boolean writeBodyLen() {
        return false;
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
    protected void onRawBatch(byte[] raw, int length) {
        ZstdPaperTrainer.submitEncoderBatch(raw, length);
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
