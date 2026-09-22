package mikumc.zstd;

import com.github.luben.zstd.ZstdCompressCtx;
import io.netty.channel.ChannelHandlerContext;
import mikumc.zstd.protocol.ZstdBatchEncoderBase;
import mikumc.zstd.protocol.ZstdTrafficCounter;

/**
 * Miku-ZSTD 编码器（服务端 → 客户端）帧格式 <b>v3</b>（协商版本 v4）。
 *
 * <p>批处理、建帧、promise 编排等全部逻辑在共享基类
 * {@link ZstdBatchEncoderBase} 中（两端同一份源码）；本类只声明服务端的
 * 五处差异：外层长度归属、统计出口、配置来源、字典状态查询、批处理参数。</p>
 *
 * <p>⚠️ <b>外层长度</b>：Velocity 4.x 管线没有独立 frame-encoder，因此本编码器
 * <b>必须自带</b> bodyLen（与客户端相反——客户端侧由 prepender 负责）。
 * 缺层或双层都会导致对端解码失败（2.0.0 曾因此断连回归）。</p>
 */
public class ZstdBatchEncoder extends ZstdBatchEncoderBase {

    private ZstdChannelManager manager;

    @Override
    protected ZstdCompressCtx resolveCompressContext(ChannelHandlerContext ctx) {
        manager = ctx.channel().attr(ZstdChannelManager.KEY).get();
        return manager == null ? null : manager.getCompressCtx();
    }

    /**
     * 压缩已卸载到线程池；字典由 ChannelManager 改写 {@code compressCtx}，
     * 所以这里必须返回<b>与它相同的那把锁</b>，否则池线程压缩与换字典会并发写同一个 ctx。
     */
    @Override
    protected Object compressLock() {
        ZstdChannelManager m = manager;
        return m == null ? super.compressLock() : m.compressLock();
    }

    @Override
    protected boolean hasCompressDict() {
        ZstdChannelManager m = manager;
        return m != null && m.hasCompressDict();
    }

    @Override
    protected int skipCompressThreshold(boolean hasCompressDict) {
        ZstdVelocityConfig cfg = ZstdVelocityConfig.INSTANCE;
        return hasCompressDict
                ? cfg.skipCompressBelowBytesWithDict
                : cfg.skipCompressBelowBytes;
    }

    /** Velocity 无 frame-encoder：外层 bodyLen 由本编码器写。 */
    @Override
    protected boolean writeBodyLen() {
        return true;
    }

    @Override
    protected void onFrame(int rawBytes, int wireBytes, boolean compressed) {
        ZstdBandwidthProfiler.record(rawBytes, wireBytes, compressed);
        ZstdTrafficCounter.record(rawBytes, wireBytes);
        ZstdChannelManager m = manager;
        if (m != null) {
            m.stats.record(rawBytes, wireBytes);
        }
    }

    /**
     * 采样：把整批原始字节交给训练器，由它在锁外按包拆开、过滤、只为入库样本拷贝
     * （见 {@code ZstdSampleTrainer#addBatch}）。
     *
     * <p>不能把整批当成"一个样本"：那样 1MB 数据只有 26 个样本，永远够不到 min_samples；
     * 也不该在这里逐包拆分——那会把解析与过滤搬到 event loop 上。</p>
     */
    @Override
    protected void onRawBatch(byte[] raw, int length) {
        ZstdSampleTrainer.submitEncoderBatch(raw, length);
    }

    @Override
    protected int configBatchWindowMs() {
        return ZstdVelocityConfig.INSTANCE.batchWindowMs;
    }

    @Override
    protected int configBatchMaxPackets() {
        return ZstdVelocityConfig.INSTANCE.batchMaxPackets;
    }
}
