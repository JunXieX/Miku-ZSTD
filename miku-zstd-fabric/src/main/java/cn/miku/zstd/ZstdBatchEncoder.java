package cn.miku.zstd;

import com.github.luben.zstd.ZstdCompressCtx;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import mikumc.zstd.protocol.ZstdBatchEncoderBase;
import mikumc.zstd.protocol.ZstdCompressLock;

/**
 * Miku-ZSTD 编码器（客户端 → 服务端）帧格式 <b>v6</b>（协商版本 v7）。
 *
 * <p>批处理、建帧、promise 编排等全部逻辑在共享基类
 * {@link ZstdBatchEncoderBase} 中（三端同一份源码）；本类只声明客户端的
 * 六处差异：写出位置、外层长度归属、统计出口、配置来源、字典状态查询、批处理参数。</p>
 *
 * <p><b>只管出站包 → 帧</b>：本类挂在原版 {@code compress} 的 <b>tail 侧</b>（出站先经过它），
 * 收到的是包编码器产出的 ByteBuf。成帧结果<b>不</b>经过原版 {@code compress} ——
 * {@link #downstream} 把写出位置指到它的 context，于是那一层被<b>跳过</b>而不是被拆掉。</p>
 *
 * <p>⚠️ <b>外层长度</b>：客户端管线（26.x）有独立 prepender 负责外层帧长度，
 * 因此本编码器<b>只输出</b> {@code [rawSize][payload]}（{@link #writeBodyLen()} 恒为 false）。
 * 若写成双层长度，服务端拆掉 prepender 层后会把 bodyLen 误当作 zstd rawSize，
 * 导致解压失败与断连（2.0.0 的回归缺陷，勿再犯）。</p>
 *
 * <h2>出站顺序（为什么必须写在原版 context 上）</h2>
 * <pre>
 *   encoder（Packet → ByteBuf）→ zstd_encoder（本类）→ [跳过 compress] → prepender → encrypt → socket
 * </pre>
 * <p>{@code prepender} 仍在我们下游，外层长度照旧由它补；{@code encrypt} 更靠下游，加密不受影响。
 * 方括号里的 {@code compress} 一直留在管线里（其它模组按名字找得到它），只是我们再没让它
 * 处理过任何一帧。</p>
 */
public class ZstdBatchEncoder extends ZstdBatchEncoderBase {

    private ZstdChannelManager manager;

    @Override
    protected ZstdCompressCtx resolveCompressContext(ChannelHandlerContext ctx) {
        manager = ctx.channel().attr(ZstdChannelManager.KEY).get();
        return manager == null ? null : manager.getCompressCtx();
    }

    /**
     * 每批压缩前重新取一次上下文。
     *
     * <p>⚠️ 必须动态取，不能只用 {@link #resolveCompressContext} 缓存的实例：
     * 客户端在"重新协商到无字典的服务器"时，{@code ZstdChannelManager#resetDicts()}
     * 会 close 旧上下文并换一个新的来清空字典（zstd 没有卸载接口）。若这里仍拿着
     * 旧实例，压缩就会写已释放的原生内存 → SIGSEGV。读取在池线程、调用方已在
     * {@code compressLock} 内，因此不会与替换并发。</p>
     */
    @Override
    protected ZstdCompressCtx compressContextForBatch() {
        ZstdChannelManager m = manager;
        return m == null ? null : m.getCompressCtx();
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
        ZstdConfig cfg = ZstdConfig.INSTANCE;
        return hasCompressDict
                ? cfg.skipCompressBelowBytesWithDict
                : cfg.skipCompressBelowBytes;
    }

    @Override
    protected int configLevel() {
        return ZstdConfig.INSTANCE.level;
    }

    @Override
    protected int configWindowLog() {
        return ZstdConfig.INSTANCE.windowLog;
    }

    @Override
    protected com.github.luben.zstd.ZstdDictCompress compressDict() {
        ZstdChannelManager m = manager;
        return m == null ? null : m.compressDict();
    }

    /**
     * 成帧结果写到原版压缩处理器的 context 上：从那个 context 发出的出站事件会<b>跳过它自己</b>，
     * 直接落到 prepender / encrypt，于是它既留在管线里、又没参与压缩。
     *
     * <p>⚠️ 按名字<b>每帧</b>解析，不缓存实例：Krypton 这类模组会在 {@code setupCompression} 的
     * HEAD 用同名 handler 换掉原版压缩器，重放登录时原版自己也会重建一次。名字永远指向"当前
     * 生效的那一个"，缓存实例则会指向已经被移除的旧对象（那样帧会绕过多余的一层，或者压根
     * 找不到位置）。</p>
     *
     * <p>两个名字都不在管线里时返回 {@code null}：基类会退回自己的 ctx —— 宁可多经一层，
     * 也不静默丢包。这条路径只在第三方模组当场拆改管线时才可能走到。</p>
     */
    @Override
    protected ChannelHandlerContext downstream(ChannelHandlerContext ctx) {
        ChannelPipeline p = ctx.pipeline();
        ChannelHandlerContext anchor = p.context("compress");
        return anchor != null ? anchor : p.context("compression-encoder");
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
