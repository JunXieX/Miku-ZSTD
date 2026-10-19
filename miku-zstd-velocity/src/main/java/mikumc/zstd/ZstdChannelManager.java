package mikumc.zstd;

import com.github.luben.zstd.ZstdCompressCtx;
import com.github.luben.zstd.ZstdDecompressCtx;
import io.netty.channel.Channel;
import io.netty.util.AttributeKey;
import mikumc.zstd.protocol.ZstdCompressLock;
import mikumc.zstd.protocol.ZstdTrafficCounter;
import mikumc.zstd.protocol.ZstdVarInts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Miku-ZSTD 每连接上下文（Velocity 端）。
 *
 * <p>持有 zstd 压缩/解压上下文与协商状态机。生命周期：嗅探器检测到
 * {@code \0ZSTD\0} 握手标记时创建，连接关闭时释放本地资源。</p>
 *
 * <h2>帧格式（v4；协商版本见 {@link #PROTOCOL_VERSION}）</h2>
 * <ul>
 *   <li>服务端 → 客户端帧：{@code [varint bodyLen][varint rawSize][zstd(payload) | packet]}
 *       —— Velocity 4.x 管线无独立 frame-encoder，编码器<b>自带</b> bodyLen 外层长度。</li>
 *   <li>客户端 → 服务端帧：{@code [varint rawSize][zstd(payload) | packet]}
 *       —— 外层长度由客户端管线的 prepender 负责，服务端 frame-decoder 剥离后
 *       本解码器收到的即完整帧负载。</li>
 *   <li>{@code rawSize == 0} 表示未压缩直存：帧体<b>恰好是一个包</b>（{@code [0][packet]}），
 *       与原版未压缩帧逐字节同形；{@code rawSize > 0} 才是压缩帧，其
 *       {@code payload = [varint pktLen][pkt]...} 可含多个包（批处理只在这里）。</li>
 *   <li>「v几」有三种含义（协商版本 / 帧格式 / 历史版本），见
 *       {@link ZstdVarInts} 类注释里的术语表——不要混用。</li>
 * </ul>
 *
 * <h2>字典门控</h2>
 * <p>negotiate 应答由 {@code ZstdNegotiateAnswerSniffer}（帧层）与 Hijacker（包层，兜底）
 * 捕获并回填 {@link #markDictResponse}。仅当客户端确认双向字典就绪（status==0）时才把字典
 * 加载进压缩/解压上下文。</p>
 *
 * <p>⚠️ 客户端报告过"缺字典"（status==1）时，本端<b>必须先推 {@code zstd:dict} 并等到
 * 它的应答</b>才能激活——否则客户端会停在"等字典"状态永不激活，而本端已开始用 zstd 发帧，
 * 形成单侧 zstd 的必断连组合。激活路径上有一道硬门控兜底（见 ZstdHijacker 的 activateZstd）。</p>
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

    /** 协商协议版本（三端必须一致）。版本不一致时客户端会干净回落原版，不会出现半 zstd。 */
    public static final int PROTOCOL_VERSION = 5;

    /** 本连接的压缩统计（/mikuzstd top 用），close 时注销 */
    public final mikumc.zstd.protocol.ZstdConnStats stats = new mikumc.zstd.protocol.ZstdConnStats();

    private final ZstdCompressCtx compressCtx;
    private final ZstdDecompressCtx decompressCtx;

    // ── 协商状态 ──
    /**
     * negotiate 的 txId，用于在 Hijacker 中匹配应答。
     *
     * <p>volatile：发送侧（{@code sendNegotiateNow} / {@code sendDictQuery}）可能跑在出站写路径上，
     * 而读取侧是帧层嗅探器（event loop）——写入若不可见，嗅探器就匹配不上应答，
     * 表现为"协商静默超时、回落原版"。</p>
     */
    private volatile int negotiateTxId = -1;
    /** zstd:dict 查询的 txId（协议 v4 按需推送字典时使用）；volatile 理由同 {@link #negotiateTxId}。 */
    private volatile int dictTxId = -1;
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
    /** volatile：写入侧在出站路径上（可能非 event loop），读取侧在出站路径上做"只发一次"判定。 */
    private volatile boolean negotiationSent;

    /** 本连接的切换闸门（未安装时为 null，此时按"未确认"处理）。 */
    private volatile mikumc.zstd.protocol.ZstdSwitchGate switchGate;

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
     * 帧头精简（自帧格式 v3 起）：去掉双方都已知道的冗余字段。
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
     *
     * <p>⚠️ <b>必须幂等</b>：本方法位于激活路径上，而激活在锚点未就绪时会重试
     * （{@code ZstdHijacker.activateZstd} 的锚点重试）。每次重入都重新
     * {@code acquireEncoder()} 会<b>丢掉上一次引用的归还</b>：注册表引用计数只增不减，
     * 换字典时条目永远等不到归零，那几 MB 原生字典内存就此泄漏。
     * 现在改为"已持有则不再取"，顺带省掉重复的 {@code loadDict}。</p>
     */
    public void loadTrainerDicts() {
        synchronized (compressLock) {
        // 通道已关闭（上下文已释放）：装载字典等于写已释放的原生内存，直接放弃
        //（正常时序下不会走到这里，此判仅作为兜底，见 ZstdCompressLock 的类注释）。
        if (compressLock.isClosed()) return;
        if (encoderDictEntry == null) {
            ZstdDictRegistry.Entry enc = ZstdDictRegistry.acquireEncoder();
            if (enc != null && enc.compressDict() != null) {
                compressCtx.loadDict(enc.compressDict());
                encoderDictEntry = enc;
                LOGGER.debug("[Zstd] Loaded encoder dict {} ({}B) -> compressCtx",
                        enc.id(), enc.bytes().length);
            }
        }
        if (decoderDictEntry == null) {
            ZstdDictRegistry.Entry dec = ZstdDictRegistry.acquireDecoder();
            if (dec != null && dec.decompressDict() != null) {
                decompressCtx.loadDict(dec.decompressDict());
                decoderDictEntry = dec;
                LOGGER.debug("[Zstd] Loaded decoder dict {} ({}B) -> decompressCtx",
                        dec.id(), dec.bytes().length);
            }
        }
        applySlimFrameFormat(); // loadDict 后幂等重申，避免参数被重置
        }
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

    /**
     * 标记"本连接不参与 zstd"（客户端据此回落原版）。
     *
     * <p>用途：本端在 SetCompression 写出之前就已放弃 zstd（协商超时 / 锚点缺失），
     * 而后到的那条 SetCompression 会让客户端照常切到 zstd。必须把它判成协议不匹配，
     * 否则形成"服务端原版 / 客户端 zstd"。幂等。</p>
     */
    public void setProtocolMismatch() {
        this.protocolMismatch = true;
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
            ZstdTrafficCounter.playerActivated();
        } else {
            ZstdTrafficCounter.playerDeactivated();
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
        // Velocity 4.x 的原版压缩编码器可能叫 compression-encoder 或 compress
        for (String anchor : new String[]{"compression-encoder", "compress"}) {
            if (p.get(anchor) != null) {
                p.replace(anchor, "zstd_encoder", new ZstdBatchEncoder());
                return true;
            }
        }
        // ⚠️ 同理，绝不能 addAfter("cipher-encoder") / addBefore("minecraft-encoder")：
        // 那会**保留**原版压缩编码器，本编码器产出的 zstd 帧会被它再压缩一遍（双层压缩），
        // 对端解一层后得到的仍是 zstd 帧，表现为解析错乱/断连。
        // "接管压缩"的唯一正确形态是**替换**原版压缩处理器。
        LOGGER.warn("[Zstd] encoder not installed — no replaceable anchor found in pipeline {}", p.names());
        return false;
    }

    /**
     * 安装切换闸门（幂等）：挂在<b>包层编码器</b>（{@code minecraft-encoder}，MC Packet → ByteBuf）
     * <b>之后</b>，也就是出站方向上比它更靠 tail 的一侧。
     *
     * <p>为什么是这个位置：</p>
     * <ul>
     *   <li>扣住的是 <b>MC 包对象</b>，包类型可直接辨认（不必去翻最终帧的字节）；
     *       Netty 出站事件从 tail 走向 head，所以闸门必须位于 {@code minecraft-encoder}
     *       的 tail 侧，出站才会"先经过闸门（拿到 Packet）、再到编码器"；</li>
     *   <li>被扣的包在放行时走的是"当时的"编码器 —— 激活后就是 zstd 编码器，
     *       因此客户端收到的一定是 zstd 帧（而确认之前客户端收到的都是原版帧）；</li>
     *   <li>切换请求由服务端经 {@code zstd_encoder} 的 ctx 写出，那是本闸门<b>更靠 head</b>
     *       的一侧，不会回头经过它，握手不会自扣留。</li>
     * </ul>
     *
     * <p>⚠️ 锚点<b>不能</b>是压缩编码器（{@code compression-encoder}）：它消费的是 ByteBuf，
     * 且在管线里位于 {@code minecraft-encoder} 的 head 侧，闸门挂在它旁边仍会落在
     * 包层编码器之后 —— 那时消息已经是 ByteBuf，闸门一个包都扣不住（详见方法内注释）。</p>
     *
     * @return 闸门是否已就位（{@code false} = 锚点缺失，调用方应中止激活）
     */
    private boolean installSwitchGate(Channel channel) {
        io.netty.channel.ChannelPipeline p = channel.pipeline();
        if (switchGate != null) return true;
        if (p.get("zstd_switch_gate") != null) return true;
        // ⚠️ 锚点必须是<b>包层编码器</b>（minecraft-encoder：MC Packet → ByteBuf），
        // <b>不能</b>是压缩编码器（compression-encoder：它消费 ByteBuf）。
        //
        // 闸门要按"包对象类型"判定该不该扣，所以它只能是出站路径上**先于**
        // minecraft-encoder 的那一个 —— Netty 出站事件从 tail 走向 head（索引递减），
        // 因此闸门在管线里的位置必须比 minecraft-encoder 更靠 tail。
        //
        // Velocity 是用 addBefore("minecraft-encoder", "compression-encoder") 插入压缩编码器的
        // （已用 javap 核对 velocity-proxy 的 MinecraftConnection.setupOutboundProtocol），
        // 所以 compression-encoder 在管线里位于 minecraft-encoder 的 head 侧：
        // 旧实现 addAfter(compression-encoder) 把闸门夹进了两者之间，出站先经过
        // minecraft-encoder（Packet 已变 ByteBuf）再到闸门 —— 而
        // {@link mikumc.zstd.protocol.ZstdSwitchGate#mustHold} 对 ByteBuf 返回 false，
        // 于是闸门<b>一个包都扣不住</b>，"确认之前不发出登录期大包"这条协议保证实际失效
        //（表现为"切换确认没到的那一瞬间，压缩帧抢先发出 → 客户端还没切换就收到 zstd 帧 → 断连"）。
        if (p.get("minecraft-encoder") == null) {
            LOGGER.warn("[Zstd] 切换闸门无法安装（minecraft-encoder 锚点缺失）— 放弃激活; pipeline={}", p.names());
            return false;
        }
        switchGate = mikumc.zstd.protocol.ZstdSwitchGate.create();
        p.addAfter("minecraft-encoder", "zstd_switch_gate", switchGate);

        // 确认嗅探器挂在压缩解码器之前（帧层）：确认包可能被原版压缩或直存
        if (p.get("zstd_switch_confirm") == null) {
            // ⚠️ 也要认 zstd_decoder：解码器可能已经被替换，原名字已不存在。
            for (String anchor : new String[]{"compression-decoder", "decompress", "zstd_decoder"}) {
                if (p.get(anchor) != null) {
                    p.addBefore(anchor, "zstd_switch_confirm", new ZstdSwitchConfirmSniffer());
                    break;
                }
            }
        }
        return true;
    }

    /**
     * 收到客户端的切换确认：放行扣住的包。
     *
     * <p>⚠️ 必须校验 txId：任何尾部恰好是魔数的帧都不该让我们提前放行 ——
     * 提前放行意味着服务端用 zstd 发帧，而对方可能还没切换。</p>
     *
     * @param txId 客户端回发的 txId
     * @return 是否接受
     */
    public boolean confirmSwitch(int txId) {
        var gate = switchGate;
        if (gate == null) {
            LOGGER.debug("[Zstd] switch confirmation received after the gate was already released");
            return false;
        }
        return gate.confirm(txId);
    }

    /** 记录本次切换询问的 txId（服务端发送请求时调用）。 */
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

    /**
     * 本连接是否已由客户端确认切换（供解码器决定是否启用握手窗口模式）。
     *
     * <p>闸门尚未安装（zstd 还未激活）时按"未确认"返回：此时解码器本身也不在管线里，
     * 语义上就是"切换尚未发生"。</p>
     */
    public boolean isSwitchConfirmed() {
        var gate = switchGate;
        return gate != null && gate.isConfirmed();
    }

    /**
     * 安装编解码器（先装闸门，再装编码器，最后装解码器）。
     *
     * <p>⚠️ 顺序不可调换：</p>
     * <ol>
     *   <li>闸门必须先于 {@code zstd_encoder} 插入——否则激活之后到确认之前，
     *       出站帧会以 zstd 形态直接发给还没切换的客户端；而它同时必须先于
     *       {@code zstd_decoder} 就位：解码器在确认之前只按原版规则解释；</li>
     *   <li>{@code zstd_encoder} 的 {@code handlerAdded} 会立即解析压缩上下文，
     *       因此必须在字典装载之后插入。</li>
     * </ol>
     *
     * <p>⚠️ <b>闸门装不上就必须整体放弃激活</b>：闸门的锚点集合与编码器完全相同
     * （{@code compression-encoder} / {@code compress}），所以正常不该出现"闸门失败、
     * 编码器成功"。真出现时后果极严重——没有闸门就没有"确认"这个信号，
     * {@code isSwitchConfirmed()} 恒为 false，解码器会<b>永久停在"原版兼容窗口"</b>：
     * 客户端切换后发来的 zstd 压缩帧会被当作原版 deflate 帧解压 → 失败 → 断连。
     * 宁可回落原版。</p>
     *
     * @return 是否确实安装成功
     */
    public boolean installHandlers(Channel channel) {
        if (!installSwitchGate(channel)) {
            LOGGER.warn("[Zstd] 切换闸门安装失败（锚点缺失）— 放弃本次激活，回落原版 zlib");
            return false;
        }
        if (!installEncoder(channel)) return false;
        return installDecoder(channel);
    }

    /**
     * 用 zstd 解码器替换原版压缩解码器。
     *
     * <p>⚠️ 调用前 {@link #installSwitchGate} 必须已执行（见 {@link #installHandlers}）。</p>
     *
     * @return 是否确实安装成功
     */
    private boolean installDecoder(Channel channel) {
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
        // ⚠️ 绝不能 addBefore("minecraft-decoder")：那是在**保留** Minecraft 帧解码器的
        // 前提下再加一层，本解码器输出的裸包会被 minecraft-decoder 当成外层帧**再解析一次**，
        // 必然失败 —— 症状是 Velocity 打出 "A packet frame decoder failed"
        // （QuietRuntimeException）并断连，而我们自己的解码器不会报任何 "frame rejected"，
        // 极难归因。正确做法与上面两条一致：**替换**，让"拆外层帧 + 解压"只由一层负责。
        LOGGER.warn("[Zstd] decoder not installed — no replaceable anchor found in pipeline {}", p.names());
        return false;
    }

    /** 释放 zstd 上下文并归还共享字典引用。 */
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
