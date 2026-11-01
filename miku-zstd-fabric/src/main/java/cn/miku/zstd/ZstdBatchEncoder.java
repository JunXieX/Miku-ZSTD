package cn.miku.zstd;

import com.github.luben.zstd.ZstdCompressCtx;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import mikumc.zstd.protocol.ZstdBatchEncoderBase;
import mikumc.zstd.protocol.ZstdCompressLock;

/**
 * Miku-ZSTD 编码器（客户端 → 服务端）帧格式 <b>v7</b>（协商版本 v9）—— <b>只批处理，不压缩</b>。
 *
 * <p>批处理、建帧、promise 编排等全部逻辑在共享基类 {@link ZstdBatchEncoderBase} 中
 * （三端同一份源码）；本类只声明客户端的几处差异：写出位置、外层长度归属、统计出口、
 * 配置来源、批处理参数，以及"这个方向不压"。</p>
 *
 * <h2>为什么上行不压缩（5.0.43）</h2>
 * <p>实测（2026-11-01，跑图/飞行/PVP 模拟）：上行单包均值 28.8B，压完<b>反而多 13%</b>，
 * 滚动历史在这个尺寸上收益 0.0%——这一层压缩一个字节都没省，全靠"压不动就直存"兜底，
 * 代价是每个包一次 zstd 调用。上行真正有效的只有<b>批处理</b>：把一次 flush 内的包合成一帧，
 * 省掉每包 1 字节的 {@code [0]} 前缀与逐包的帧头。所以 {@link #compressUpstream()} 恒返回
 * {@code false}，帧形态固定为 {@code [0][count][pktLen][pkt]...}（见基类的
 * {@code writeStoredBatch}）。</p>
 *
 * <p>因此 {@link #skipCompressThreshold()}、{@link #configLevel()}、{@link #configWindowLog()}、
 * {@link #compressContextForBatch()} 在本端<b>都不会被调用</b>（基类只在压缩路径上读它们）；
 * 保留实现只为满足基类接口，并让"哪天恢复上行压缩"只需要改一个返回值。</p>
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
 * <p>虽然上行不再压缩，{@code prepender} 仍在我们下游（外层长度照旧由它补），{@code encrypt}
 * 更靠下游，加密不受影响。方括号里的 {@code compress} 一直留在管线里（其它模组按名字找得到它），
 * 只是我们再没让它处理过任何一帧——现在这一步更不能错：它会给已经成帧的存储批再套一层 zlib，
 * 对端把 zlib 头当成 {@code count} 或包长读，必然解析失败。</p>
 */
public class ZstdBatchEncoder extends ZstdBatchEncoderBase {

    private ZstdChannelManager manager;

    /**
     * 取本连接的压缩上下文——上行不再压缩后，它<b>唯一的作用是让基类进入攒批路径</b>。
     *
     * <p>⚠️ 这里仍然<b>不能返回 {@code null}</b>：基类的 {@code write()} 用
     * {@code compressCtx == null} 判"整条通道完全透传（不攒批）"，返回 null 会把批处理一起关掉
     * ——而上行现在<b>只剩</b>批处理。压缩本身不会再读它（见 {@link #compressUpstream()}）。</p>
     */
    @Override
    protected ZstdCompressCtx resolveCompressContext(ChannelHandlerContext ctx) {
        manager = ctx.channel().attr(ZstdChannelManager.KEY).get();
        return manager == null ? null : manager.getCompressCtx();
    }

    /**
     * 上行（客户端 → 服务端）只批处理、不压缩：恒返回 {@code false}。
     *
     * <p>实测（2026-11-01，跑图/飞行/PVP 模拟）：上行单包均值 28.8B，压完<b>反而多 13%</b>，
     * 滚动历史在这个尺寸上收益 0.0%——一个字节都没省，全靠"压不动就直存"兜底，代价是每包
     * 一次 zstd 调用。上行真正有效的只有批处理：把一次 flush 内的包合成一帧，省掉每包 1 字节的
     * {@code [0]} 前缀与逐包帧头。</p>
     *
     * <p>返回 {@code false} 后基类完全不碰 zstd：不建压缩上下文、不喂滚动历史、不写
     * {@code dictGen}，帧形态固定为 {@code [0][count][pktLen][pkt]...}。对端必须按"存储批帧"
     * 解析，两端靠协商版本 v9 对齐——版本不一致时客户端会干净回落原版，不会出现半解析。</p>
     */
    @Override
    protected boolean compressUpstream() {
        return false;
    }

    /**
     * 每批压缩前重新取一次上下文。
     *
     * <p>⚠️ 上行不压缩后本方法<b>不会被调用</b>（基类只在压缩路径上调它）。留着是因为它守着一个
     * 会让进程崩掉的细节：重放登录（重新协商）时 {@code ZstdChannelManager#resetContexts()} 会把
     * 旧上下文归还池、换一个新的，而缓存下来的实例会指向别人已经借走的内存。哪天把
     * {@link #compressUpstream()} 改回 {@code true}，这段判断必须原样还在。</p>
     */
    @Override
    protected ZstdCompressCtx compressContextForBatch() {
        ZstdChannelManager m = manager;
        return m == null ? null : m.getCompressCtx();
    }

    /**
     * 与 ChannelManager 共用同一把锁。
     *
     * <p>上行不压缩后池线程不再参与压缩，但这把锁仍被基类的 {@code handlerRemoved} 用到
     * （归还常驻缓冲前要等可能存在的池线程退出）。它还带着"上下文已关闭"标志——若哪天恢复上行
     * 压缩，这里必须是同一把锁，否则就是最难查的那类 use-after-free。</p>
     */
    @Override
    protected ZstdCompressLock compressLock() {
        ZstdChannelManager m = manager;
        return m == null ? super.compressLock() : m.compressLock();
    }

    /**
     * 小包跳过压缩的阈值。
     *
     * <p>上行不压缩后本方法<b>不会被调用</b>（它只在压缩路径的分支判断里），保留只为满足基类
     * 接口。值仍取配置里的 {@code skip_compress_below_bytes}：哪天恢复上行压缩，默认口径不用
     * 重新推一遍。</p>
     */
    @Override
    protected int skipCompressThreshold() {
        return ZstdConfig.INSTANCE.skipCompressBelowBytes;
    }

    /** 压缩等级：上行不压缩后不会被调用，保留只为满足基类接口（分块并行只在压缩路径读）。 */
    @Override
    protected int configLevel() {
        return ZstdConfig.INSTANCE.level;
    }

    /** 压缩窗口：同上，保留只为满足基类接口。 */
    @Override
    protected int configWindowLog() {
        return ZstdConfig.INSTANCE.windowLog;
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

    /**
     * 每帧的统计出口。
     *
     * <p>上行只剩批处理，所以这里收到的 {@code compressed} 恒为 {@code false}：原始口径是
     * "1 字节 {@code [0]} + 整批负载"，线路口径是成帧后的实际字节数。HUD 上的上行压缩率因此
     * 反映的是<b>批处理摊薄</b>，不是 zstd。</p>
     */
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
