package mikumc.zstd.paper;

import com.github.luben.zstd.ZstdCompressCtx;
import com.github.luben.zstd.ZstdDecompressCtx;
import io.netty.channel.Channel;
import io.netty.util.AttributeKey;
import mikumc.zstd.protocol.ZstdVarInts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Miku-ZSTD 每连接上下文（Paper 端）。
 *
 * <p>Paper/vanilla 服务端与客户端共用同一个 {@code net.minecraft.network.Connection} 类，
 * 因此管线结构与 Fabric 客户端<b>对称</b>：</p>
 * <ul>
 *   <li>出站：{@code encoder} → {@code prepender}（服务端有 prepender，故本端编码器
 *       <b>不写</b>外层 bodyLen，与客户端一致；Velocity 端因为无 prepender 才必须自带）；</li>
 *   <li>入站：{@code splitter} → {@code decompress} → {@code decoder}。</li>
 * </ul>
 *
 * <p>本版本不做字典训练，故 {@code dictId} 恒为 0——客户端照常参与 zstd（无字典压缩仍有效）。</p>
 */
public class ZstdPaperChannelManager {

    private static final Logger LOGGER = LoggerFactory.getLogger("miku-zstd");

    public static final AttributeKey<ZstdPaperChannelManager> KEY =
            AttributeKey.valueOf("miku-zstd:manager");
    /** 协商协议版本（三端必须一致）。帧长度上限见 {@link ZstdVarInts#DEFAULT_MAX_VALUE}。 */
    public static final int PROTOCOL_VERSION = 4;

    /** 本连接的压缩统计（/mikuzstd top 用），close 时注销 */
    public final mikumc.zstd.protocol.ZstdConnStats stats = new mikumc.zstd.protocol.ZstdConnStats();

    private final ZstdCompressCtx compressCtx;
    private final ZstdDecompressCtx decompressCtx;

    private int negotiateTxId = -1;
    /** zstd:dict 查询的 txId（协议 v4 按需推送字典） */
    private int dictTxId = -1;
    /** 已取用的共享字典条目，连接关闭时归还引用 */
    private ZstdPaperDictRegistry.Entry encoderDictEntry;
    private ZstdPaperDictRegistry.Entry decoderDictEntry;
    private volatile boolean responseReceived;
    private volatile boolean refused;
    private volatile boolean confirmed;
    private volatile boolean dictRequested;
    private volatile boolean replaced;

    public ZstdPaperChannelManager() {
        ZstdPaperConfig cfg = ZstdPaperConfig.INSTANCE;
        this.compressCtx = new ZstdCompressCtx();
        this.compressCtx.setLevel(cfg.level);
        this.compressCtx.setWindowLog(cfg.windowLog);
        this.compressCtx.setLong(1); // LDM：提升区块级长距离重复的压缩率
        this.decompressCtx = new ZstdDecompressCtx();
        // 帧头精简必须与客户端一致（magicless / 无 contentSize / 无 dictID）
        this.compressCtx.setMagicless(true);
        this.compressCtx.setContentSize(false);
        this.compressCtx.setDictID(false);
        this.decompressCtx.setMagicless(true);
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
     * 把共享字典装进两端上下文（仅在客户端确认就绪后调用）。
     * 字典对象由 {@link ZstdPaperDictRegistry} 进程级共享，此处只取引用。
     */
    public void loadTrainerDicts() {
        synchronized (compressLock) {
        ZstdPaperDictRegistry.Entry enc = ZstdPaperDictRegistry.acquireEncoder();
        if (enc != null && enc.compressDict() != null) {
            compressCtx.loadDict(enc.compressDict());
            encoderDictEntry = enc;
            compressCtx.setMagicless(true);
            compressCtx.setContentSize(false);
            compressCtx.setDictID(false);
            LOGGER.debug("[Zstd] 已装载压缩字典 id={} ({}B)", enc.id(), enc.bytes().length);
        }
        ZstdPaperDictRegistry.Entry dec = ZstdPaperDictRegistry.acquireDecoder();
        if (dec != null && dec.decompressDict() != null) {
            decompressCtx.loadDict(dec.decompressDict());
            decoderDictEntry = dec;
            decompressCtx.setMagicless(true);
            LOGGER.debug("[Zstd] 已装载解压字典 id={} ({}B)", dec.id(), dec.bytes().length);
        }
        }
    }

    /** 压缩方向是否已装载字典（编码器据此选小包跳过阈值）。 */
    public boolean hasCompressDict() {
        return encoderDictEntry != null;
    }

    public int getDictTxId() {
        return dictTxId;
    }

    public void setDictTxId(int txId) {
        this.dictTxId = txId;
    }

    public ZstdDecompressCtx getDecompressCtx() {
        return decompressCtx;
    }

    public int getNegotiateTxId() {
        return negotiateTxId;
    }

    public void setNegotiateTxId(int txId) {
        this.negotiateTxId = txId;
    }

    /**
     * 回填协商结论。语义与另两端完全一致：
     * {@code 0 = 客户端启用 zstd}、{@code 1 = 需要字典}、{@code 2 = 客户端保持原版}。
     *
     * <p>两点收紧（与 Velocity 端对齐）：</p>
     * <ul>
     *   <li>状态码经 {@link ZstdNegotiateStatus#sanitize} 归一化，越界值不再被当成
     *       第三种状态放行；</li>
     *   <li>{@code confirmed} 的含义<b>必须</b>是"两个方向都是 0（就绪）"。旧实现写成
     *       {@code confirmed = !refused}，于是 {@code (1,1)}（客户端还在等字典）也会被
     *       判为"已确认"，本端就会在客户端尚未激活时先行激活 → 单侧 zstd → 断连。</li>
     * </ul>
     *
     * @param clientAnswered 客户端是否理解了这次查询（应答的 success 位）；
     *                       false 表示它不认识该通道（多半没装模组），与"装了但版本不匹配"要分开看
     */
    public void markResponse(int encStatus, int decStatus, boolean clientAnswered) {
        this.responseReceived = true;
        int enc = mikumc.zstd.protocol.ZstdNegotiateStatus.sanitize(encStatus);
        int dec = mikumc.zstd.protocol.ZstdNegotiateStatus.sanitize(decStatus);
        this.refused = enc == mikumc.zstd.protocol.ZstdNegotiateStatus.VANILLA
                || dec == mikumc.zstd.protocol.ZstdNegotiateStatus.VANILLA;
        this.confirmed = !this.refused
                && enc == mikumc.zstd.protocol.ZstdNegotiateStatus.READY
                && dec == mikumc.zstd.protocol.ZstdNegotiateStatus.READY;
        if (!clientAnswered) {
            LOGGER.debug("[Zstd] 客户端未识别 zstd:negotiate（未安装 Miku-ZSTD 模组），保持原版 zlib");
        } else if (this.refused) {
            LOGGER.debug("[Zstd] 客户端拒绝 zstd（enc={} dec={}）——多为两端版本不匹配，保持原版 zlib", enc, dec);
        } else {
            LOGGER.debug("[Zstd] 协商应答：enc={} dec={} confirmed={}", enc, dec, this.confirmed);
        }
    }

    /** 客户端报告过"缺字典"（协议 v4 需先推 {@code zstd:dict}）。 */
    public void markDictRequested() {
        this.dictRequested = true;
    }

    /** 客户端是否报告过缺字典——激活前必须等字典确认，见 {@code ZstdPaperNegotiator} 的硬门控。 */
    public boolean isDictRequested() {
        return dictRequested;
    }

    public boolean isResponseReceived() {
        return responseReceived;
    }

    /** 客户端明确拒绝（协议不匹配等）——必须回落原版 zlib。 */
    public boolean isRefused() {
        return refused;
    }

    public boolean isConfirmed() {
        return confirmed;
    }

    public boolean isReplaced() {
        return replaced;
    }

    /**
     * 唯一的「激活状态」迁移入口，同时维护监控计数。
     *
     * <p>⚠️ 必须幂等：{@code tryActivate} 有<b>两个</b>触发点（检测到 SetCompression 写出、
     * 以及收到客户端应答），两者都可能在同一条连接上跑完整流程；重试路径也可能再次走到这里。
     * 因此把「激活 +1 / 关闭 -1」内聚进状态迁移，保证严格成对、绝不重复计数。</p>
     */
    public synchronized void setReplaced(boolean v) {
        if (this.replaced == v) {
            return; // 状态未变 → 不重复计数
        }
        this.replaced = v;
        if (v) {
            ZstdPaperMonitor.playerActivated();
        } else {
            ZstdPaperMonitor.playerDeactivated();
        }
    }

    public boolean installEncoder(Channel channel) {
        var p = channel.pipeline();
        if (p.get("zstd_encoder") != null) return true;
        for (String anchor : new String[]{"compress", "compression-encoder"}) {
            if (p.get(anchor) != null) {
                p.replace(anchor, "zstd_encoder", new ZstdPaperEncoder());
                return true;
            }
        }
        // ⚠️ 绝不"提前插入"。曾经为了让 threshold=-1 也能工作而在此处 addBefore("encoder")，
        // 结果：应答一到就激活（早于原版 SetCompression），于是原版的 SetCompression 包被
        // zstd 压缩发出，而客户端还没切换 → 解析失败 → 无法进入服务器。
        // 正确做法是**只替换**原版装好的 compress，即等原版流程走到那一步。
        return false;
    }

    public boolean installDecoder(Channel channel) {
        var p = channel.pipeline();
        if (p.get("zstd_decoder") != null) return true;
        for (String anchor : new String[]{"decompress", "compression-decoder"}) {
            if (p.get(anchor) != null) {
                p.replace(anchor, "zstd_decoder", new ZstdPaperDecoder());
                return true;
            }
        }
        // 同上：绝不提前插入，只替换原版装好的 decompress
        return false;
    }

    public void close() {
        stats.remove();
        // 走 setReplaced 归还计数（幂等），而不是直接判断 replaced —— 重复 close 不会多减
        setReplaced(false);
        compressCtx.close();
        decompressCtx.close();
        ZstdPaperDictRegistry.Entry enc = encoderDictEntry;
        if (enc != null) {
            encoderDictEntry = null;
            enc.release();
        }
        ZstdPaperDictRegistry.Entry dec = decoderDictEntry;
        if (dec != null) {
            decoderDictEntry = null;
            dec.release();
        }
    }
}
