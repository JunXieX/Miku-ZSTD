package mikumc.zstd.paper;

import com.github.luben.zstd.ZstdCompressCtx;
import com.github.luben.zstd.ZstdDecompressCtx;
import io.netty.channel.Channel;
import io.netty.util.AttributeKey;
import mikumc.zstd.protocol.ZstdCompressLock;
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
 * <p>字典链路与 Velocity 端同构：{@link ZstdPaperTrainer} 从真实流量采样训练，
 * {@link ZstdPaperDictRegistry} 进程级共享并按引用计数换代释放；协商时按客户端所缺方向
 * 通过 {@code zstd:dict} 按需下发。</p>
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
     * （换字典、重申帧头参数、close）都必须与池线程互斥——{@code ZstdCompressCtx} 不是线程安全的。
     * 编码器通过 {@code ZstdBatchEncoderBase#compressLock()} 取得同一把锁。</p>
     *
     * <p>⚠️ 类型是 {@link ZstdCompressLock} 而不是 {@code Object}：它同时承载"上下文已关闭"
     * 标志，池线程在锁内读它来避免 use-after-free（详见该类注释）。</p>
     */
    private final ZstdCompressLock compressLock = new ZstdCompressLock();

    /** 供编码器使用：所有对 compressCtx 的访问都收敛到这一把锁上。 */
    public ZstdCompressLock compressLock() {
        return compressLock;
    }

    public ZstdCompressCtx getCompressCtx() {
        return compressCtx;
    }

    /**
     * 把共享字典装进两端上下文（仅在客户端确认就绪后调用）。
     * 字典对象由 {@link ZstdPaperDictRegistry} 进程级共享，此处只取引用。
     *
     * <p>⚠️ <b>必须幂等</b>：本方法在激活路径上，而激活失败会重试
     * （{@code ZstdPaperNegotiator.tryActivate0} 的锚点重试是<b>常态</b>——原版
     * compress/decompress 是在 SetCompression 写出之后才安装的，首次尝试必然找不到锚点）。
     * 每次重试都重新 {@code acquireEncoder()} 会<b>丢掉上一次引用的归还</b>：注册表引用计数
     * 只增不减，换字典时条目永远等不到归零，那几 MB 原生字典内存就此泄漏。
     * 现在改为"已持有则不再取"，顺带省掉重复的 {@code loadDict}。</p>
     */
    public void loadTrainerDicts() {
        synchronized (compressLock) {
        // 通道已关闭（上下文已释放）：装载字典等于写已释放的原生内存，直接放弃
        //（正常时序下不会走到这里，此判仅作为兜底，见 ZstdCompressLock 的类注释）。
        if (compressLock.isClosed()) return;
        if (encoderDictEntry == null) {
            ZstdPaperDictRegistry.Entry enc = ZstdPaperDictRegistry.acquireEncoder();
            if (enc != null && enc.compressDict() != null) {
                compressCtx.loadDict(enc.compressDict());
                encoderDictEntry = enc;
                compressCtx.setMagicless(true);
                compressCtx.setContentSize(false);
                compressCtx.setDictID(false);
                LOGGER.debug("[Zstd] 已装载压缩字典 id={} ({}B)", enc.id(), enc.bytes().length);
            }
        }
        if (decoderDictEntry == null) {
            ZstdPaperDictRegistry.Entry dec = ZstdPaperDictRegistry.acquireDecoder();
            if (dec != null && dec.decompressDict() != null) {
                decompressCtx.loadDict(dec.decompressDict());
                decoderDictEntry = dec;
                decompressCtx.setMagicless(true);
                LOGGER.debug("[Zstd] 已装载解压字典 id={} ({}B)", dec.id(), dec.bytes().length);
            }
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

    /** 本连接的切换闸门（未安装时为 null，此时按"未确认"处理）。 */
    private volatile mikumc.zstd.protocol.ZstdSwitchGate switchGate;

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

    /**
     * 出站方向是否已有可替换的锚点（原版 {@code compress} 或其别名）。
     *
     * <p>用途：激活前先确认<b>两个方向都可替换</b>再动手，避免"只装上编码器"造成
     * 单侧 zstd 的必断连组合（见 {@code ZstdPaperNegotiator.tryActivate0}）。</p>
     */
    public boolean hasEncoderAnchor(Channel channel) {
        var p = channel.pipeline();
        return p.get("zstd_encoder") != null
                || p.get("compress") != null
                || p.get("compression-encoder") != null;
    }

    /**
     * 入站方向是否已有可替换的锚点（原版 {@code decompress} 或其别名）。
     *
     * <p>同上：必须在任何替换动作之前与出站方向一起校验。</p>
     */
    public boolean hasDecoderAnchor(Channel channel) {
        var p = channel.pipeline();
        return p.get("zstd_decoder") != null
                || p.get("decompress") != null
                || p.get("compression-decoder") != null;
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

    /**
     * 安装切换闸门（幂等）：挂在 {@code compress} 之前，即 <b>MC 包层</b>。
     *
     * <p>⚠️ 必须先在包层扣住，激活后才能改用 zstd 编码器；挂在字节层则放行时用的仍是
     * 扣下那一刻的编码形态。详见共享层 {@code ZstdSwitchGate} 的类注释。</p>
     */
    public void installSwitchGate(Channel channel) {
        if (switchGate != null) return;
        var p = channel.pipeline();
        if (p.get("zstd_switch_gate") != null) return;
        for (String anchor : new String[]{"compress", "compression-encoder"}) {
            if (p.get(anchor) != null) {
                switchGate = mikumc.zstd.protocol.ZstdSwitchGate.create();
                // ⚠️ 必须是 addAfter：Netty 出站事件从 tail 走向 head（索引递减），
                // addBefore 会把闸门放到编码器<b>之后</b>，那时包已是 ByteBuf，无法按包类型判断。
                // addAfter 让出站先经过闸门（拿到 Packet），放行时才交给编码器。
                p.addAfter(anchor, "zstd_switch_gate", switchGate);
                installConfirmSniffer(p);
                return;
            }
        }
        LOGGER.debug("[Zstd] 切换闸门未能安装（compress 锚点缺失）; pipeline={}", p.names());
    }

    /**
     * 在压缩解码器之前安装确认嗅探器（帧层）。
     *
     * <p>⚠️ 必须在帧层：包层看到它时已经被原版解码器按"未知包 id"处理掉了。</p>
     */
    private void installConfirmSniffer(io.netty.channel.ChannelPipeline p) {
        if (p.get(ZstdPaperSwitchConfirmSniffer.NAME) != null) return;
        // ⚠️ 必须同时认 zstd_decoder：如果解码器已经被替换，原来的 decompress 名字就不存在了。
        for (String anchor : new String[]{"decompress", "compression-decoder", "zstd_decoder"}) {
            if (p.get(anchor) != null) {
                p.addBefore(anchor, ZstdPaperSwitchConfirmSniffer.NAME,
                        new ZstdPaperSwitchConfirmSniffer());
                return;
            }
        }
        LOGGER.debug("[Zstd] 确认嗅探器未能安装（压缩解码器锚点缺失）; pipeline={}", p.names());
    }

    /** 本连接是否已由客户端确认切换（供解码器决定是否启用握手窗口模式）。 */
    public boolean isSwitchConfirmed() {
        var gate = switchGate;
        return gate != null && gate.isConfirmed();
    }

    /**
     * 收到客户端的切换确认：校验 txId 后放行扣住的包。
     *
     * <p>⚠️ 必须校验 txId：任何尾部恰好是魔数的帧都不该让我们提前放行。</p>
     *
     * @return 是否接受
     */
    public boolean markSwitchConfirmed(int txId) {
        var gate = switchGate;
        if (gate == null) {
            LOGGER.debug("[Zstd] switch confirmation received after the gate was already released");
            return false;
        }
        return gate.confirm(txId);
    }

    /** 记录本次切换询问的 txId（发送请求时调用）。 */
    public void expectSwitchTxId(int txId) {
        var gate = switchGate;
        if (gate != null) gate.expectTxId(txId);
    }

    /**
     * 客户端回了一条<b>不带魔数</b>的应答（没能 / 不会确认切换）：放行扣住的包。
     *
     * <p>⚠️ 这条应答无法区分"装了模组但没切换"与"旧版客户端按原版规则回的空应答"，
     * 而后者其实是 zstd —— 当成拒绝去关连接会把正常玩家挡在门外。见
     * {@link mikumc.zstd.protocol.ZstdSwitchGate} 的类注释。</p>
     *
     * @return 是否接受
     */
    public boolean releaseUnconfirmed(int txId) {
        var gate = switchGate;
        if (gate == null) return false;
        return gate.releaseUnconfirmed(txId);
    }

    public void close() {
        stats.remove();
        // 走 setReplaced 归还计数（幂等），而不是直接判断 replaced —— 重复 close 不会多减
        setReplaced(false);
        // ⚠️ 必须与池线程拿的是**同一把锁**：压缩在 ZstdCompressPool 的线程上执行，
        // 若在途压缩还没回来就 close 掉 native 上下文，池线程会继续写已释放的内存 → SIGSEGV。
        // ⚠️ 且必须**先立标志再关上下文**：标志与 close 在同一段持锁区内，池线程在锁内读到
        // 标志即可安全放弃；顺序反了（先 close 后立标志）等于没防（详见 ZstdCompressLock 注释）。
        synchronized (compressLock) {
            compressLock.markClosed();
            compressCtx.close();
            decompressCtx.close();
        }
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
