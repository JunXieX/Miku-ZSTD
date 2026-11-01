package mikumc.zstd;

import com.github.luben.zstd.ZstdCompressCtx;
import io.netty.channel.ChannelHandlerContext;
import mikumc.zstd.protocol.ZstdBatchEncoderBase;
import mikumc.zstd.protocol.ZstdCompressLock;
import mikumc.zstd.protocol.ZstdTrafficCounter;

/**
 * Miku-ZSTD 编码器（服务端 → 客户端）帧格式 <b>v6</b>（协商版本 v7）。
 *
 * <p>批处理、建帧、promise 编排等全部逻辑在共享基类
 * {@link ZstdBatchEncoderBase} 中（两端同一份源码）；本类只声明服务端的
 * 差异点：外层长度归属、统计出口、配置来源、字典状态查询、锁，
 * 外加一处<b>位置</b>差异——成帧结果写到原版压缩处理器的 context（见 {@link #downstream}）。</p>
 *
 * <p>⚠️ <b>安装位置</b>：本类由 {@code ZstdChannelManager#installEncoder} 以
 * {@code addAfter} 插在原版压缩处理器的 tail 侧，<b>不替换</b>它。原版处理器一直留在
 * 管线里，我们只是把成帧结果从它<b>之后</b>写出——绕过它，但不拆它。</p>
 *
 * <p>⚠️ <b>外层长度</b>：Velocity 把「写外层帧长度」与「压缩」放在<b>同一个</b>处理器里
 *（{@code compression-encoder}），我们从它的 context 往下写会把这层长度一起跳过，
 * 所以 bodyLen <b>必须</b>由本编码器自带（与客户端相反——客户端侧由 prepender 负责）。
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
     * 压缩已卸载到线程池；字典由 ChannelManager 改写 {@code compressCtx}、且它负责在
     * 关连接时 close 该上下文，所以这里必须返回<b>与它相同的那把锁</b>——否则池线程压缩
     * 会与「换字典 / close」并发，后者是 use-after-free（SIGSEGV）。
     */
    @Override
    protected ZstdCompressLock compressLock() {
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

    @Override
    protected int configLevel() {
        return ZstdVelocityConfig.INSTANCE.level;
    }

    @Override
    protected int configWindowLog() {
        return ZstdVelocityConfig.INSTANCE.windowLog;
    }

    @Override
    protected com.github.luben.zstd.ZstdDictCompress compressDict() {
        ZstdChannelManager m = manager;
        return m == null ? null : m.compressDict();
    }

    /**
     * 外层 {@code bodyLen} 由本编码器写。
     *
     * <p>⚠️ 不能指望下游替我们补：Velocity 侧写外层长度的就是那个「长度 + 压缩」合一的
     * 处理器，我们从它的 context 往下写已经把它整个跳过了，再往 head 只剩加密与 socket。</p>
     */
    @Override
    protected boolean writeBodyLen() {
        return true;
    }

    /**
     * 成帧结果写到<b>原版压缩处理器的 context</b>。
     *
     * <p>从那个 context 出发正好跳过它自己（我们的帧已是最终形态，不能再被它压一遍），
     * 继续走本该走的下游（加密 / socket）。这样原版处理器<b>留在管线里</b>，
     * ViaVersion / PacketEvents / Grim 这类按名字找它的插件照常工作，
     * 回落原版也退化成"把自己摘掉"（见 {@code ZstdChannelManager#uninstallZstd}）。</p>
     *
     * <p>两种叫法都要认：不同分支/移植版把同一个处理器注册成
     * {@code compression-encoder} 或 {@code compress}。</p>
     *
     * <p>⚠️ 都找不到时返回 {@code null}（基类退回本处理器自己的 ctx）：那一帧多半会被
     * 原版压缩再包一层、对端解不开，但至少不丢包——真实场景里只有第三方插件在激活后
     * 拆改管线才会走到这里。</p>
     */
    @Override
    protected ChannelHandlerContext downstream(ChannelHandlerContext ctx) {
        var p = ctx.pipeline();
        ChannelHandlerContext target = p.context("compression-encoder");
        return target != null ? target : p.context("compress");
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
    protected void onRawBatch(io.netty.buffer.ByteBuf raw, int length) {
        // ⚠️ 先问训练器要不要：这份拷贝跑在 event loop 上，环满后（到下一轮训练之间
        // 是分钟级）每批都会白拷一份。代理是所有玩家流量的汇聚点，量级比单实例大得多。
        if (!ZstdSampleTrainer.wantsEncoderSample()) {
            return;
        }
        byte[] copy = new byte[length];
        raw.getBytes(raw.readerIndex(), copy, 0, length);
        ZstdSampleTrainer.submitEncoderBatch(copy, length);
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
