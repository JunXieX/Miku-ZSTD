package mikumc.zstd;

import com.github.luben.zstd.ZstdCompressCtx;
import com.github.luben.zstd.ZstdDecompressCtx;
import io.netty.channel.Channel;
import io.netty.util.AttributeKey;
import mikumc.zstd.protocol.ZstdVarInts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Miku-ZSTD 每连接上下文（Velocity 端）。
 *
 * <p>持有 zstd 压缩/解压上下文与协商状态机。生命周期：嗅探器检测到
 * {@code \0ZSTD\0} 握手标记时创建，连接关闭时释放本地资源。</p>
 *
 * <h2>协议 v2</h2>
 * <ul>
 *   <li>服务端 → 客户端帧：{@code [varint bodyLen][varint rawSize][zstd(packet) | packet]}
 *       —— Velocity 4.x 管线无独立 frame-encoder，编码器<b>自带</b> bodyLen 外层长度。</li>
 *   <li>客户端 → 服务端帧：{@code [varint rawSize][zstd(packet) | packet]}
 *       —— 外层长度由客户端管线的 prepender 负责，服务端 frame-decoder 剥离后
 *       本解码器收到的即完整帧负载。</li>
 *   <li>rawSize==0 表示未压缩直存，此时整个数据就是单个 Minecraft 包。</li>
 *   <li>v1 的内层 {@code [pktSize]} 前缀已移除：单包模式下包长可从数据长度推出。</li>
 * </ul>
 *
 * <h2>字典门控</h2>
 * <p>negotiate 应答由 Hijacker 拦截（{@link #markDictResponse}）。仅当客户端确认
 * 双向字典就绪（status==0）时才把字典加载进压缩/解压上下文；否则以无字典模式
 * 激活（压缩仍有效，仅失去字典增益）。协议版本不匹配时完全跳过 zstd，回落原版 zlib。</p>
 *
 * <h2>3.1.0 变更</h2>
 * <ul>
 *   <li>字典改为从 {@link ZstdDictRegistry} 共享获取（每连接省约 3.35MB，见该类实测数据），
 *       并带引用计数，连接关闭时归还；</li>
 *   <li>VarInt 收敛到 {@link ZstdVarInts}；</li>
 *   <li>删除只写不读的 {@code ZSTD_ENABLED} / {@code ZSTD_STATE} 属性（服务端无读取方）。</li>
 * </ul>
 */
public class ZstdChannelManager {

    private static final Logger LOGGER = LoggerFactory.getLogger("miku-zstd");

    public static final AttributeKey<ZstdChannelManager> KEY =
            AttributeKey.valueOf("zstd:manager");

    /** 协商协议版本（三端必须一致）。帧长度上限见 {@link ZstdVarInts#DEFAULT_MAX_VALUE}。 */
    public static final int PROTOCOL_VERSION = 4;

    /** 本连接的压缩统计（/mikuzstd top 用），close 时注销 */
    public final mikumc.zstd.protocol.ZstdConnStats stats = new mikumc.zstd.protocol.ZstdConnStats();

    private final ZstdCompressCtx compressCtx;
    private final ZstdDecompressCtx decompressCtx;

    // ── 协商状态 ──
    /** negotiate 的 txId，用于在 Hijacker 中匹配应答 */
    private int negotiateTxId = -1;
    /** zstd:dict 查询的 txId（协议 v4 按需推送字典时使用） */
    private int dictTxId = -1;
    /** 已收到 negotiate 应答 */
    private volatile boolean dictResponseReceived;
    /** 客户端确认双向字典就绪（encStatus==0 && decStatus==0） */
    private volatile boolean dictConfirmed;
    /**
     * 客户端曾报告"缺字典"（encStatus==1 / decStatus==1），即协议 v4 需要推一次
     * {@code zstd:dict} 才能激活。激活前的硬门控要看它：见
     * {@link #isDictConfirmed()} 与 ZstdHijacker 的 activateZstd。
     */
    private volatile boolean dictRequested;
    /** 客户端协议版本不匹配（status==2），须完全跳过 zstd */
    private volatile boolean protocolMismatch;

    /** 「协议不匹配」告警的只报一次标志（该情形通常影响所有客户端，逐连接重复无价值） */
    private static final java.util.concurrent.atomic.AtomicBoolean MISMATCH_WARNED =
            new java.util.concurrent.atomic.AtomicBoolean();

    private volatile boolean replaced;
    private volatile boolean negotiationSent;

    /** 已从注册表取用的字典条目，连接关闭时归还引用 */
    private ZstdDictRegistry.Entry encoderDictEntry;
    private ZstdDictRegistry.Entry decoderDictEntry;

    public ZstdChannelManager() {
        ZstdVelocityConfig cfg = ZstdVelocityConfig.INSTANCE;
        this.compressCtx = new ZstdCompressCtx();
        this.compressCtx.setLevel(cfg.level);
        this.compressCtx.setWindowLog(cfg.windowLog);
        this.compressCtx.setLong(1); // LDM：提升区块级长距离重复的压缩率
        this.decompressCtx = new ZstdDecompressCtx();
        applySlimFrameFormat();
        // 注意：字典不在这里加载——必须等 negotiate 应答确认客户端字典就绪后
        // 由 activateZstd 按门控结果显式调用 loadTrainerDicts()。
    }

    /**
     * 协议 v3 帧头精简：去掉双方都已知道的冗余字段。
     *
     * <ul>
     *   <li>{@code magicless}：省 4 字节 magic（对几十字节的小包占比可观）；</li>
     *   <li>{@code contentSize=false}：省 1~4 字节——解压尺寸由协议里的 rawSize 提供，帧内不必再写；</li>
     *   <li>{@code dictID=false}：省 1~4 字节——字典由双方显式加载，帧内不必再写。</li>
     * </ul>
     *
     * <p>三者都只是"不再重复写入双方已知的信息"，语义不变。实测（每包一帧）可再省约 10.7%。
     * 解压端必须设置相同的 magicless 开关。</p>
     *
     * <p>loadDict 之后会再调用一次——zstd 的部分参数在装载字典时可能被重置，
     * 这里不依赖其内部行为，直接幂等地重新施加。</p>
     */
    private void applySlimFrameFormat() {
        compressCtx.setMagicless(true);
        compressCtx.setContentSize(false);
        compressCtx.setDictID(false);
        decompressCtx.setMagicless(true);
    }

    /**
     * 把当前字典装进两端上下文（仅在 dictConfirmed 时调用）。
     *
     * <p>字典对象由 {@link ZstdDictRegistry} 全进程共享，此处只取引用，
     * 不做 {@code new ZstdDictCompress(...)}（后者每连接约 3.2MB）。</p>
     */
    public void loadTrainerDicts() {
        synchronized (compressLock) {
        ZstdDictRegistry.Entry enc = ZstdDictRegistry.acquireEncoder();
        if (enc != null && enc.compressDict() != null) {
            compressCtx.loadDict(enc.compressDict());
            encoderDictEntry = enc;
            LOGGER.debug("[Zstd] Loaded encoder dict {} ({}B) -> compressCtx",
                    enc.id(), enc.bytes().length);
        }
        ZstdDictRegistry.Entry dec = ZstdDictRegistry.acquireDecoder();
        if (dec != null && dec.decompressDict() != null) {
            decompressCtx.loadDict(dec.decompressDict());
            decoderDictEntry = dec;
            LOGGER.debug("[Zstd] Loaded decoder dict {} ({}B) -> decompressCtx",
                    dec.id(), dec.bytes().length);
        }
        applySlimFrameFormat(); // loadDict 后幂等重申，避免参数被重置
        }
    }

    /**
     * 压缩上下文的互斥锁。
     *
     * <p>压缩已卸载到线程池执行，因此任何改写 {@link #compressCtx} 的操作
     * （换字典、重申帧头参数）都必须与池线程互斥——{@code ZstdCompressCtx} 不是线程安全的。
     * 编码器通过 {@code ZstdBatchEncoderBase#compressLock()} 取得同一把锁。</p>
     */
    private final Object compressLock = new Object();

    /** 供编码器使用：所有对 compressCtx 的访问都收敛到这一把锁上。 */
    public Object compressLock() {
        return compressLock;
    }

    public ZstdCompressCtx getCompressCtx() {
        return compressCtx;
    }

    /**
     * 压缩方向是否已装载字典。编码器据此选择"小包跳过压缩"的阈值：
     * 实测无字典时 48B 以下压缩反而膨胀，有字典时 24B 起才有收益（见 ZstdVelocityConfig）。
     */
    public boolean hasCompressDict() {
        return encoderDictEntry != null;
    }

    public ZstdDecompressCtx getDecompressCtx() {
        return decompressCtx;
    }

    // ── 协商门控 ──

    public int getNegotiateTxId() {
        return negotiateTxId;
    }

    public void setNegotiateTxId(int txId) {
        this.negotiateTxId = txId;
    }

    public int getDictTxId() {
        return dictTxId;
    }

    public void setDictTxId(int txId) {
        this.dictTxId = txId;
    }

    /**
     * Hijacker/嗅探器拦截到 negotiate 应答时回填结果。
     *
     * <p>⚠️ <b>状态语义（双端必须完全一致）</b>：{@code 0 = 客户端会启用 zstd}，
     * {@code 2 = 客户端保持原版}。服务端严格按 2 回落原版 zlib，因此客户端侧
     * 任何返回 2 的分支都必须同时放弃激活 zstd——否则会出现"服务端 zlib /
     * 客户端 zstd"的必断连组合（3.1.0 实测"无法进入服务器"的根因即客户端在
     * 服务端尚无字典时返回 2 却仍激活了 zstd）。</p>
     *
     * <p>状态码经 {@link mikumc.zstd.protocol.ZstdNegotiateStatus#sanitize} 归一化：
     * 畸形载荷里的越界值不能被当成"第三种状态"放行。</p>
     *
     * @param encStatus      客户端对 encoder 字典的应答（0=可用，1=需要字典，2=不可用）
     * @param decStatus      客户端对 decoder 字典的应答
     * @param clientAnswered 客户端是否<b>理解</b>了这次查询（应答的 success 位）。
     *                       {@code false} 表示它根本不认识 {@code zstd:negotiate}
     *                       （多半是没装模组），与"装了模组但版本不匹配"要分开表述，
     *                       否则公开服上每个原版玩家登录都会刷一条误导性的 WARN。
     */
    public void markDictResponse(int encStatus, int decStatus, boolean clientAnswered) {
        this.dictResponseReceived = true;
        int enc = mikumc.zstd.protocol.ZstdNegotiateStatus.sanitize(encStatus);
        int dec = mikumc.zstd.protocol.ZstdNegotiateStatus.sanitize(decStatus);
        if (enc == mikumc.zstd.protocol.ZstdNegotiateStatus.VANILLA
                || dec == mikumc.zstd.protocol.ZstdNegotiateStatus.VANILLA) {
            this.protocolMismatch = true;
            if (!clientAnswered) {
                // 原版客户端 / 未安装模组：这是完全正常的预期行为，不是问题
                LOGGER.debug("[Zstd] 客户端未识别 zstd:negotiate（未安装 Miku-ZSTD 模组），保持原版 zlib");
            } else if (MISMATCH_WARNED.compareAndSet(false, true)) {
                // 真正值得上报的信号：客户端认识这个查询但拒绝了（多为版本不匹配）。
                // 只报一次——若真是版本不匹配，它会命中每一条连接，逐条告警只会淹没日志。
                LOGGER.warn("[Zstd] 客户端明确拒绝 zstd（enc={}, dec={}）。若所有客户端都如此，"
                        + "通常是两端版本不匹配；本端保持原版 zlib，不影响进服。"
                        + "（本条只提示一次）", enc, dec);
            }
        } else {
            this.dictConfirmed = (enc == mikumc.zstd.protocol.ZstdNegotiateStatus.READY
                    && dec == mikumc.zstd.protocol.ZstdNegotiateStatus.READY);
            LOGGER.debug("[Zstd] Negotiate 应答：enc={} dec={} dictConfirmed={}", enc, dec, dictConfirmed);
        }
    }

    /**
     * 记下"客户端请求了字典"。协议 v4 在推完 {@code zstd:dict} 并拿到它的应答之前，
     * 都不能激活 zstd——见 ZstdHijacker 激活前的硬门控。
     */
    public void markDictRequested() {
        this.dictRequested = true;
    }

    /** 客户端是否报告过缺字典（决定激活前是否必须等到字典确认）。 */
    public boolean isDictRequested() {
        return dictRequested;
    }

    public boolean isDictResponseReceived() {
        return dictResponseReceived;
    }

    public boolean isDictConfirmed() {
        return dictConfirmed;
    }

    public boolean isProtocolMismatch() {
        return protocolMismatch;
    }

    public boolean isReplaced() {
        return replaced;
    }

    /**
     * 唯一的「激活状态」迁移入口，同时维护监控计数。
     *
     * <p>⚠️ 必须幂等：{@code ZstdHijacker} 在激活失败回落时会再传一次 {@code false}，
     * 若直接加减计数会算错。把「激活 +1 / 关闭 -1」内聚到状态迁移里，保证严格成对。</p>
     */
    public synchronized void setReplaced(boolean v) {
        if (this.replaced == v) {
            return; // 状态未变 → 不重复计数
        }
        this.replaced = v;
        if (v) {
            ZstdTrafficStats.playerActivated();
        } else {
            ZstdTrafficStats.playerDeactivated();
        }
    }

    public boolean isNegotiationSent() {
        return negotiationSent;
    }

    public void setNegotiationSent(boolean v) {
        this.negotiationSent = v;
    }

    // ── 管线安装 ──

    /**
     * 用 zstd 编码器替换原版压缩编码器。
     *
     * @return 是否确实安装成功（false 表示管线中找不到任何可用锚点）
     */
    public boolean installEncoder(Channel channel) {
        if (channel == null) return false;
        var p = channel.pipeline();
        if (p.get("zstd_encoder") != null) return true;
        if (p.get("compression-encoder") != null) {
            p.replace("compression-encoder", "zstd_encoder", new ZstdBatchEncoder());
            return true;
        }
        if (p.get("cipher-encoder") != null) {
            p.addAfter("cipher-encoder", "zstd_encoder", new ZstdBatchEncoder());
            return true;
        }
        if (p.get("minecraft-encoder") != null) {
            p.addBefore("minecraft-encoder", "zstd_encoder", new ZstdBatchEncoder());
            return true;
        }
        LOGGER.warn("[Zstd] encoder not installed — no anchor found in pipeline {}", p.names());
        return false;
    }

    /**
     * 用 zstd 解码器替换原版压缩解码器。
     *
     * @return 是否确实安装成功
     */
    public boolean installDecoder(Channel channel) {
        if (channel == null) return false;
        var p = channel.pipeline();
        if (p.get("zstd_decoder") != null) return true;
        if (p.get("compression-decoder") != null) {
            p.replace("compression-decoder", "zstd_decoder", new ZstdBatchDecoder());
            return true;
        }
        if (p.get("decompress") != null) {
            p.replace("decompress", "zstd_decoder", new ZstdBatchDecoder());
            return true;
        }
        if (p.get("minecraft-decoder") != null) {
            p.addBefore("minecraft-decoder", "zstd_decoder", new ZstdBatchDecoder());
            return true;
        }
        LOGGER.warn("[Zstd] decoder not installed — no anchor found in pipeline {}", p.names());
        return false;
    }

    /** 释放 zstd 上下文并归还共享字典引用。 */
    public void close() {
        stats.remove();
        // 走 setReplaced 归还计数（幂等），而不是直接判断 replaced —— 重复 close 不会多减
        setReplaced(false);
        compressCtx.close();
        decompressCtx.close();
        ZstdDictRegistry.Entry enc = encoderDictEntry;
        if (enc != null) {
            encoderDictEntry = null;
            enc.release();
        }
        ZstdDictRegistry.Entry dec = decoderDictEntry;
        if (dec != null) {
            decoderDictEntry = null;
            dec.release();
        }
    }
}
