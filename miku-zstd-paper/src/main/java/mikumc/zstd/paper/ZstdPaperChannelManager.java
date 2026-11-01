package mikumc.zstd.paper;

import com.github.luben.zstd.ZstdCompressCtx;
import com.github.luben.zstd.ZstdDecompressCtx;
import mikumc.zstd.protocol.ZstdCtxPool;
import io.netty.channel.Channel;
import io.netty.util.AttributeKey;
import mikumc.zstd.protocol.ZstdCompressLock;
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
 * <p>5.0.42 起没有"训练字典"这一层了：跨帧匹配全部由
 * {@link mikumc.zstd.protocol.ZstdRollingDict} 提供（每帧把最近 64KB 原始字节当字典，
 * 两端对称维护），本类只管上下文与互斥锁，不再持有任何字典对象——省掉下发通道，
 * 也省掉一处"两端字典不一致就断连"的故障源。</p>
 */
public class ZstdPaperChannelManager {

    private static final Logger LOGGER = LoggerFactory.getLogger("miku-zstd");

    public static final AttributeKey<ZstdPaperChannelManager> KEY =
            AttributeKey.valueOf("miku-zstd:manager");
    /** 协商协议版本（三端必须一致）。版本不一致时客户端会干净回落原版，不会出现半 zstd。 */
    public static final int PROTOCOL_VERSION = mikumc.zstd.protocol.ZstdProtocolVersion.NEGOTIATE;

    /** 本连接的压缩统计（/mikuzstd top 用），close 时注销 */
    public final mikumc.zstd.protocol.ZstdConnStats stats = new mikumc.zstd.protocol.ZstdConnStats();

    private final ZstdCompressCtx compressCtx;

    /**
     * 解压上下文。
     *
     * <p>⚠️ 5.0.43 起上行（客户端 → 服务端）不再压缩，这个 ctx 已经<b>没有实际用途</b>
     * ——入站只剩"握手窗口里的原版压缩帧"，那条路径用的是 JDK 的 {@code Inflater}。
     * 暂时保留：基类仍会向解码器索要它，租借/归还也挂在同一条生命周期上。
     * 等这版跑稳再单独清理（连带 {@link ZstdCtxPool#borrowDecompress()} 那一档）。</p>
     */
    private final ZstdDecompressCtx decompressCtx;

    private volatile int negotiateTxId = -1;
    private volatile boolean responseReceived;
    private volatile boolean refused;
    private volatile boolean replaced;

    public ZstdPaperChannelManager() {
        ZstdPaperConfig cfg = ZstdPaperConfig.INSTANCE;
        // ⚠️ 从租借池取，而不是每连接 new：level 3 下每个 ctx 约 650KiB 原生内存，
        // 登录风暴时集中 malloc/free 会造成 RSS 尖峰与分配抖动（GC 管不到原生内存）。
        // 池里归还的实例已被 reset 成"刚创建"的状态，因此下面必须重新设全部参数。
        this.compressCtx = ZstdCtxPool.borrowCompress(cfg.level);
        this.compressCtx.setWindowLog(cfg.windowLog);
        this.compressCtx.setLong(1); // LDM：提升区块级长距离重复的压缩率
        // 上行已不压缩，这份 ctx 仅剩占位（见字段注释）；归还路径与压缩 ctx 共用同一把锁
        this.decompressCtx = ZstdCtxPool.borrowDecompress();
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
     * （装载滚动历史、重申帧头参数、close）都必须与池线程互斥——{@code ZstdCompressCtx} 不是线程安全的。
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

    public ZstdDecompressCtx getDecompressCtx() {
        return decompressCtx;
    }

    public int getNegotiateTxId() {
        return negotiateTxId;
    }

    /**
     * 抢占协商查询的 txId（每条连接只允许发一次）。
     *
     * <p>⚠️ 必须是原子的 check-and-set：发查询有<b>两个</b>触发点——入站线程看到握手包、
     * 写包线程看到 {@code SetCompression} 时的兜底。Folia 上这两个线程天然分离，
     * 没有抢占时可能各发一条 {@code LoginPluginRequest}；客户端回两个应答，
     * 而协商器只认最后登记的那个 txId，剩下那个会漏给
     * {@code ServerLoginPacketListenerImpl} —— 它不认识我们自造的 txId，
     * 直接以 "unexpected query response" 断开连接（实测：Folia 上玩家因此进不了服）。</p>
     *
     * @return 是否抢到（{@code false} 表示已经发过，调用方应直接返回）
     */
    public synchronized boolean claimNegotiateTxId(int txId) {
        if (this.negotiateTxId != -1) return false;
        this.negotiateTxId = txId;
        return true;
    }



    /**
     * 回填协商结论。语义与另两端完全一致：
     * {@code 0 = 客户端启用 zstd}、{@code 2 = 客户端保持原版}
     *（5.0.42 删掉了原来的 {@code 1 = 需要字典}——滚动历史不需要任何下发）。
     *
     * <p>状态码先经 {@link ZstdNegotiateStatus#sanitize} 归一化：旧客户端的 1、以及畸形载荷里的
     * 任何其它值都按 2 处理。"不动"永远比"单侧动"安全——服务端一旦激活、客户端没激活，
     * 就是必断连的单侧 zstd。</p>
     *
     * @param clientAnswered 客户端是否理解了这次查询（应答的 success 位）；
     *                       false 表示它不认识该通道（多半没装模组），与"装了但版本不匹配"要分开看
     */
    public void markResponse(int status, boolean clientAnswered) {
        this.responseReceived = true;
        int s = mikumc.zstd.protocol.ZstdNegotiateStatus.sanitize(status);
        this.refused = s == mikumc.zstd.protocol.ZstdNegotiateStatus.VANILLA;
        if (!clientAnswered) {
            LOGGER.debug("[Zstd] 客户端未识别 zstd:negotiate（未安装 Miku-ZSTD 模组），保持原版 zlib");
        } else if (this.refused) {
            LOGGER.debug("[Zstd] 客户端保持原版（status={}）——多为两端版本不匹配", s);
        } else {
            LOGGER.debug("[Zstd] 协商应答：status={} → 可激活", s);
        }
    }

    public boolean isResponseReceived() {
        return responseReceived;
    }

    /** 客户端明确拒绝（协议不匹配等）——必须回落原版 zlib。 */
    public boolean isRefused() {
        return refused;
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

    /**
     * 安装出站编码器：插在原版 {@code compress} <b>之后</b>（出站方向上先经过我们）。
     *
     * <p>⚠️ 这里<b>不换掉</b>原版处理器。协议 v7 起编码器只负责"把攒下的包合成一帧"，
     * 成帧结果从 {@code ZstdBatchEncoderBase#downstream(ChannelHandlerContext)} 指的位置
     * 写出——也就是原版 {@code compress} 的 context：绕过它，但不拆它。留下它换回三件事：
     * 按名字查找 {@code compress}/{@code prepender} 的插件（ViaVersion / PacketEvents /
     * Grim 这类）照常工作；回落原版只是"把自己摘掉"（见 {@link #uninstallZstd(Channel)}），
     * 不存在"非 {@code @Sharable} 的实例被移除后再也放不回去"的死局；也不会再出现
     * "只装上一半"的必断连中间态。</p>
     *
     * <p>⚠️ 时机同样不能提前：曾经为了让 {@code threshold=-1} 也能工作而在这里
     * {@code addBefore("encoder")}，结果应答一到就激活（早于原版 SetCompression），
     * 于是 SetCompression 包被 zstd 压缩发出、客户端却还没切换 → 解析失败 → 进不了服。
     * 本方法只负责"原版装好之后挂到它旁边"，不创造时机。</p>
     *
     * @return {@code false} 表示锚点还没出现，调用方应按 {@code RETRY_MS} 重试
     */
    public boolean installEncoder(Channel channel) {
        var p = channel.pipeline();
        if (p.get("zstd_encoder") != null) return true;
        for (String anchor : new String[]{"compress", "compression-encoder"}) {
            if (p.get(anchor) != null) {
                p.addAfter(anchor, "zstd_encoder", new ZstdPaperEncoder());
                return true;
            }
        }
        return false;
    }

    /**
     * 出站方向是否已有可插入的锚点（原版 {@code compress} 或其别名）。
     *
     * <p>用途：激活前先确认<b>两个方向都插得进去</b>再动手。只装上一侧会构成单侧 zstd，
     * 而单侧 zstd 必定断连（见 {@code ZstdPaperNegotiator.tryActivate0}）。</p>
     */
    public boolean hasEncoderAnchor(Channel channel) {
        var p = channel.pipeline();
        return p.get("zstd_encoder") != null
                || p.get("compress") != null
                || p.get("compression-encoder") != null;
    }

    /**
     * 入站方向是否已有可插入的锚点（原版 {@code decompress} 或其别名）。
     *
     * <p>同上：必须在任何插入动作之前与出站方向一起校验。</p>
     */
    public boolean hasDecoderAnchor(Channel channel) {
        var p = channel.pipeline();
        return p.get("zstd_decoder") != null
                || p.get("decompress") != null
                || p.get("compression-decoder") != null;
    }

    /**
     * 安装入站解码器：插在原版 {@code decompress} 的<b>上游</b>。
     *
     * <p>入站是从 head 往 tail 走的，{@code addBefore} 让我们的解码器先拿到帧；解出来的
     * 每个包都包成原版未压缩帧 {@code [0][packet]} 交给紧跟其后的原版 {@code decompress}
     * ——它对 {@code dataLength == 0} 是<b>原样透传</b>，等于借它把包送到包解码器，
     * 而它自己什么都不做。原版处理器因此可以一直留在管线里。</p>
     *
     * <p>⚠️ 位置一旦换到它下游就会乱码：原版 {@code decompress} 会把我们的输出当成
     * {@code [dataLength][deflate]} 再解一次，帧头里的 {@code rawSize} 被当成长度读。</p>
     */
    public boolean installDecoder(Channel channel) {
        var p = channel.pipeline();
        if (p.get("zstd_decoder") != null) return true;
        for (String anchor : new String[]{"decompress", "compression-decoder"}) {
            if (p.get(anchor) != null) {
                p.addBefore(anchor, "zstd_decoder", new ZstdPaperDecoder());
                return true;
            }
        }
        return false;
    }

    /**
     * 摘掉本插件装在管线上的<b>全部</b>处理器（幂等）：编码器、解码器、确认嗅探器、切换闸门。
     *
     * <p>⚠️ 只摘自己的名字，原版 {@code compress} / {@code decompress} / {@code prepender}
     * 一律不动。协议 v7 起我们本来就是"插进去"的，所以摘干净之后管线就是一条标准的原版
     * 连接——这正是回落路径第一次可以真正回滚、而不必关连接的原因（旧实现用
     * {@code pipeline.replace} 把原版处理器拿走过，非 {@code @Sharable} 的实例再也放不回去，
     * 只能拉倒重连）。</p>
     *
     * <p>⚠️ 必须在 event loop 上调用：摘处理器会与在途的成帧/写帧抢同一条线程，
     * 而编码器的 {@code downstream()} 在每一帧上按名字查原版锚点。</p>
     *
     * @return 是否真的摘掉了至少一个处理器（{@code false} = 本来就没装，调用方可直接跳过）
     */
    public boolean uninstallZstd(Channel channel) {
        if (channel == null) return false;
        var p = channel.pipeline();
        boolean changed = removeIfPresent(p, "zstd_encoder");
        changed |= removeIfPresent(p, "zstd_decoder");
        changed |= removeIfPresent(p, ZstdPaperSwitchConfirmSniffer.NAME);
        changed |= removeIfPresent(p, "zstd_switch_gate");
        if (changed) {
            switchGate = null;
            // 归还"激活"计数（内部幂等）：否则监控面板会把已经回落的连接一直算作 zstd 通道。
            setReplaced(false);
        }
        return changed;
    }

    /** 幂等摘除：名字不存在时静默（{@code remove} 对不存在的名字会抛异常）。 */
    private static boolean removeIfPresent(io.netty.channel.ChannelPipeline p, String name) {
        if (p.get(name) == null) return false;
        try {
            p.remove(name);
            return true;
        } catch (Throwable t) {
            LOGGER.debug("[Zstd] 摘除管线处理器 {} 失败（忽略）: {}", name, t.toString());
            return false;
        }
    }

    /**
     * 安装切换闸门（幂等）：挂在<b>包层编码器</b>（{@code encoder}，MC Packet → ByteBuf）
     * <b>之后</b>，即出站方向上比它更靠 tail 的一侧。
     *
     * <p>⚠️ 锚点<b>不能</b>是 {@code compress}：原版是用 {@code addBefore("encoder", "compress")}
     * 插入压缩编码器的，它在管线里位于 {@code encoder} 的 head 侧；把闸门 {@code addAfter(compress)}
     * 等于插在两者之间 —— 出站先经过 {@code encoder}（Packet 已变成 ByteBuf）再到闸门，而
     * {@code ZstdSwitchGate.mustHold} 对 ByteBuf 返回 false，闸门于是<b>一个包都扣不住</b>，
     * "确认之前不发出登录期大包"这条协议保证实际失效。挂在 {@code encoder} 之后，出站才会
     * 先经过闸门（拿到 Packet）、放行时才交给（此刻已是 zstd 的）编码器。</p>
     *
     * @return 闸门是否已就位（{@code false} = 锚点缺失，调用方应中止激活）
     */
    public boolean installSwitchGate(Channel channel) {
        if (switchGate != null) return true;
        var p = channel.pipeline();
        if (p.get("zstd_switch_gate") != null) return true;
        if (p.get("encoder") == null) {
            LOGGER.debug("[Zstd] 切换闸门无法安装（encoder 锚点缺失）; pipeline={}", p.names());
            return false;
        }
        switchGate = mikumc.zstd.protocol.ZstdSwitchGate.create();
        p.addAfter("encoder", "zstd_switch_gate", switchGate);
        installConfirmSniffer(p);
        return true;
    }

    /**
     * 在压缩解码器之前安装确认嗅探器（帧层）。
     *
     * <p>⚠️ 必须在帧层：包层看到它时已经被原版解码器按"未知包 id"处理掉了。</p>
     *
     * <p>⚠️ 锚点顺序同样是协议要求：{@code zstd_decoder} 优先。嗅探器必须排在<b>我们的
     * 解码器上游</b>，否则它看到的是我们解出来的 {@code [0][packet]}，而不是客户端真正
     * 发出的那一帧——被原版压缩过的确认（阈值很小时会走压缩帧）就此认不出来，闸门只能
     * 白等满超时才放行。平时它先装在 {@code decompress} 之前，之后解码器再插到
     * {@code decompress} 前面，两条路径的最终顺序相同：嗅探器 → 我们的解码器 → 原版。</p>
     */
    private void installConfirmSniffer(io.netty.channel.ChannelPipeline p) {
        if (p.get(ZstdPaperSwitchConfirmSniffer.NAME) != null) return;
        for (String anchor : new String[]{"zstd_decoder", "decompress", "compression-decoder"}) {
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
            // 归还而非关闭：池内部会 reset（清字典与参数），池满或已停用时才真正 close
            ZstdCtxPool.returnCompress(compressCtx);
            ZstdCtxPool.returnDecompress(decompressCtx);
        }
    }
}
