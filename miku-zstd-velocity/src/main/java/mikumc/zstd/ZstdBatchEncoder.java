package mikumc.zstd;

import com.github.luben.zstd.ZstdCompressCtx;
import io.netty.channel.ChannelHandlerContext;
import mikumc.zstd.protocol.ZstdBatchEncoderBase;

/**
 * Miku-ZSTD 编码器（服务端 → 客户端）协议 <b>v3</b>。
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
        ZstdTrafficStats.record(rawBytes, wireBytes);
        ZstdChannelManager m = manager;
        if (m != null) {
            m.stats.record(rawBytes, wireBytes);
        }
    }

    /** 采样：按包拆开逐个提交——整批提交会让样本数永远达不到 min_samples 门槛。 */
    @Override
    protected void onRawBatch(byte[] raw, int length) {
        // 采样提交已批量化：一次加锁入环，不再逐包提交（见 ZstdSampleTrainer#addBatch）
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
