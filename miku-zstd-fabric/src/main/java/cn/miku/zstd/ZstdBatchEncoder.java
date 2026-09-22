package cn.miku.zstd;

import com.github.luben.zstd.ZstdCompressCtx;
import io.netty.channel.ChannelHandlerContext;
import mikumc.zstd.protocol.ZstdBatchEncoderBase;

/**
 * Miku-ZSTD 编码器（客户端 → Velocity）协议 <b>v3</b>。
 *
 * <p>批处理、建帧、promise 编排等全部逻辑在共享基类
 * {@link ZstdBatchEncoderBase} 中（两端同一份源码）；本类只声明客户端的
 * 五处差异：外层长度归属、统计出口、配置来源、字典状态查询、批处理参数。</p>
 *
 * <p>⚠️ <b>外层长度</b>：客户端管线（26.x）有独立 prepender 负责外层帧长度，
 * 因此本编码器<b>只输出</b> {@code [rawSize][payload]}。若写成双层长度，服务端
 * 拆掉 prepender 层后会把 bodyLen 误当作 zstd rawSize，导致解压失败与断连
 * （2.0.0 的回归缺陷，勿再犯）。</p>
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
        ZstdConfig cfg = ZstdConfig.INSTANCE;
        return hasCompressDict
                ? cfg.skipCompressBelowBytesWithDict
                : cfg.skipCompressBelowBytes;
    }

    /** 客户端管线有 prepender：本编码器不写外层长度。 */
    @Override
    protected boolean writeBodyLen() {
        return false;
    }

    @Override
    protected void onFrame(int rawBytes, int wireBytes, boolean compressed) {
        ZstdStatsData.addTxBatch(rawBytes, wireBytes);
    }

    @Override
    protected int configBatchWindowMs() {
        return ZstdConfig.INSTANCE.batchWindowMs;
    }

    @Override
    protected int configBatchMaxPackets() {
        return ZstdConfig.INSTANCE.batchMaxPackets;
    }
}
