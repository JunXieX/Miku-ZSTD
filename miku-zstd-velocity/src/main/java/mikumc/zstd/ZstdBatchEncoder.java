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
 * 差异点：外层长度归属、统计出口、配置来源、锁，
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

    /**
     * 小包跳过压缩的阈值。
     *
     * <p>5.0.42 删掉训练字典后只剩这一个值（原来按"有没有训练字典"二选一）。
     * 滚动历史生效后小包同样能压出收益，但判据仍是"这一批有多大"；
     * 协议层另有 16B 硬地板，保证切换握手包永远直存。</p>
     */
    @Override
    protected int skipCompressThreshold() {
        return ZstdVelocityConfig.INSTANCE.skipCompressBelowBytes;
    }

    @Override
    protected int configLevel() {
        return ZstdVelocityConfig.INSTANCE.level;
    }

    @Override
    protected int configWindowLog() {
        return ZstdVelocityConfig.INSTANCE.windowLog;
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

    @Override
    protected int configBatchWindowMs() {
        return ZstdVelocityConfig.INSTANCE.batchWindowMs;
    }

    @Override
    protected int configBatchMaxPackets() {
        return ZstdVelocityConfig.INSTANCE.batchMaxPackets;
    }
}
