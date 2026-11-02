package mikumc.zstd;

import com.github.luben.zstd.ZstdCompressCtx;
import mikumc.zstd.protocol.ZstdCtxPool;
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
 * <p>持有下行 zstd 压缩上下文与协商状态机。生命周期：嗅探器检测到
 * {@code \0ZSTD\0} 握手标记时创建，连接关闭时释放本地资源。</p>
 *
 * <h2>帧格式（v8；协商版本见 {@link #PROTOCOL_VERSION}）</h2>
 * <ul>
 *   <li>下行：{@code [varint bodyLen][magicless Zstd 连续字节]}，内层为连续的
 *       {@code [varint packetLen][packet]}，一个包允许跨传输帧。Velocity 把外层长度与压缩
 *       放在同一个原版处理器里，绕过它后由本编码器写 bodyLen。</li>
 *   <li>同一连接只用一个压缩上下文；每批末尾 FLUSH，不 END，不装字典、不 reset。</li>
 *   <li>上行始终沿用原版压缩协议：{@code [0][packet]} 或
 *       {@code [dataLength][zlib(packet)]}，交给原版解码器。</li>
 * </ul>
 *
 * <h2>协商应答门控</h2>
 * <p>协商载荷只有 {@code [int 协议版本]}，应答只有 {@code [varint 状态]}
 *（{@code 0 = 就绪}、{@code 2 = 保持原版}）。应答由 {@code ZstdNegotiateAnswerSniffer}
 *（帧层，主路径）与 Hijacker（包层，兜底）捕获并回填 {@link #markNegotiateResponse}，
 * 激活前必须看到"已应答且状态为就绪"，否则一律回落原版。</p>
 *
 * <p>跨批次匹配由持续的原生流状态维护，本类不持有或下发字典对象。</p>
 *
 * <h2>3.1.0 变更</h2>
 * <ul>
 *   <li>VarInt 收敛到 {@link ZstdVarInts}；</li>
 *   <li>删除只写不读的 {@code ZSTD_ENABLED} / {@code ZSTD_STATE} 属性（服务端无读取方）。</li>
 * </ul>
 */
public class ZstdChannelManager {

    private static final Logger LOGGER = LoggerFactory.getLogger("miku-zstd");

    public static final AttributeKey<ZstdChannelManager> KEY =
            AttributeKey.valueOf("zstd:manager");

    /** 协商协议版本（三端必须一致）。版本不一致时客户端会干净回落原版，不会出现半 zstd。 */
    public static final int PROTOCOL_VERSION = mikumc.zstd.protocol.ZstdProtocolVersion.NEGOTIATE;

    /** 本连接的压缩统计（/mikuzstd top 用），close 时注销 */
    public final mikumc.zstd.protocol.ZstdConnStats stats = new mikumc.zstd.protocol.ZstdConnStats();

    private final ZstdCompressCtx compressCtx;

    // ── 协商状态 ──
    /**
     * negotiate 的 txId，用于在 Hijacker 中匹配应答。
     *
     * <p>volatile：发送侧（{@code sendNegotiateNow}）跑在出站写路径上，
     * 而读取侧是帧层嗅探器（event loop）——写入若不可见，嗅探器就匹配不上应答，
     * 表现为"协商静默超时、回落原版"。</p>
     */
    private volatile int negotiateTxId = -1;
    /** 已收到 negotiate 应答（无论结论是就绪还是保持原版） */
    private volatile boolean answerReceived;
    /** 客户端答复"就绪"（status==0）：本连接可以切 zstd */
    private volatile boolean clientReady;
    /** 客户端保持原版（status==2），须完全跳过 zstd */
    private volatile boolean protocolMismatch;

    /** 「协议不匹配」告警的只报一次标志（该情形通常影响所有客户端，逐连接重复无价值） */
    private static final java.util.concurrent.atomic.AtomicBoolean MISMATCH_WARNED =
            new java.util.concurrent.atomic.AtomicBoolean();

    private volatile boolean replaced;
    /** volatile：写入侧在出站路径上（可能非 event loop），读取侧在出站路径上做"只发一次"判定。 */
    private volatile boolean negotiationSent;

    /** 本连接的切换闸门（未安装时为 null，此时按"未确认"处理）。 */
    private volatile mikumc.zstd.protocol.ZstdSwitchGate switchGate;

    public ZstdChannelManager() {
        ZstdVelocityConfig cfg = ZstdVelocityConfig.INSTANCE;
        // 上下文从租借池取（见 ZstdCtxPool 的说明）：池内实例已被 reset，参数需重设。
        this.compressCtx = ZstdCtxPool.borrowCompress(cfg.level);
        this.compressCtx.setWindowLog(cfg.windowLog);
        // 保留显式windowLog；setLong的参数是窗口位数，不是启用布尔值。
        applySlimFrameFormat();
        // 仅在新借到的上下文上初始化；连接存续期间保留流状态，不装载字典或 reset。
    }

    /** 新上下文的流格式：省略 magic、总原长和字典 ID；只在构造时调用一次。 */
    private void applySlimFrameFormat() {
        compressCtx.setMagicless(true);
        compressCtx.setContentSize(false);
        compressCtx.setChecksum(false);
        compressCtx.setDictID(false);
    }

    /**
     * 压缩上下文的生命周期租约，与编码器共用。
     *
     * <p>编码器取得租约后才读取上下文，并在使用结束后释放；关闭只禁止新租借并登记归还，
     * 最后一个使用者退出后再归还池。监视器不包围原生压缩，event loop 不等待在途批次。</p>
     *
     * <p>租约只保护上下文的存活期；原生压缩调用仍由编码器的单在途批次串行化。</p>
     */
    private final ZstdCompressLock compressLock = new ZstdCompressLock();

    /** 供编码器使用：取得租约后才可访问 compressCtx，使用结束必须释放。 */
    public ZstdCompressLock compressLock() {
        return compressLock;
    }

    public ZstdCompressCtx getCompressCtx() {
        return compressCtx;
    }

    // ── 协商门控 ──

    public int getNegotiateTxId() {
        return negotiateTxId;
    }

    public void setNegotiateTxId(int txId) {
        this.negotiateTxId = txId;
    }

    /**
     * Hijacker/嗅探器拦截到 negotiate 应答时回填结果。
     *
     * <p>⚠️ <b>状态语义（三端必须完全一致）</b>：{@code 0 = 客户端会启用 zstd}，
     * {@code 2 = 客户端保持原版}。服务端严格按 2 回落原版 zlib，因此客户端侧
     * 任何返回 2 的分支都必须同时放弃激活 zstd——否则会出现"服务端 zlib /
     * 客户端 zstd"的必断连组合。</p>
     *
     * <p>状态码经 {@link mikumc.zstd.protocol.ZstdNegotiateStatus#sanitize} 归一化：
     * 畸形载荷里的越界值——以及旧客户端可能仍回的 {@code 1 = 需要字典}——一律按 2 处理，
     * 不能被当成"第三种状态"放行。</p>
     *
     * @param status         应答里的状态码（0=就绪，其余一律按保持原版）
     * @param clientAnswered 客户端是否<b>理解</b>了这次查询（应答的 success 位）。
     *                       {@code false} 表示它根本不认识 {@code zstd:negotiate}
     *                       （多半是没装模组），与"装了模组但版本不匹配"要分开表述，
     *                       否则公开服上每个原版玩家登录都会刷一条误导性的 WARN。
     */
    public void markNegotiateResponse(int status, boolean clientAnswered) {
        this.answerReceived = true;
        int s = mikumc.zstd.protocol.ZstdNegotiateStatus.sanitize(status);
        if (s != mikumc.zstd.protocol.ZstdNegotiateStatus.READY) {
            this.clientReady = false;
            this.protocolMismatch = true;
            if (!clientAnswered) {
                // 原版客户端 / 未安装模组：这是完全正常的预期行为，不是问题
                LOGGER.debug("[Zstd] 客户端未识别 zstd:negotiate（未安装 Miku-ZSTD 模组），保持原版 zlib");
            } else if (MISMATCH_WARNED.compareAndSet(false, true)) {
                // 真正值得上报的信号：客户端认识这个查询但拒绝了（多为版本不匹配）。
                // 只报一次——若真是版本不匹配，它会命中每一条连接，逐条告警只会淹没日志。
                LOGGER.warn("[Zstd] 客户端明确拒绝 zstd（status={}）。若所有客户端都如此，"
                        + "通常是两端版本不匹配；本端保持原版 zlib，不影响进服。"
                        + "（本条只提示一次）", s);
            }
        } else {
            this.clientReady = true;
            LOGGER.debug("[Zstd] Negotiate 应答：status={} → 客户端就绪", s);
        }
    }

    /** 是否已收到 negotiate 应答（结论未定也算"已应答"，但还要看 {@link #isClientReady()}）。 */
    public boolean isAnswerReceived() {
        return answerReceived;
    }

    /** 客户端是否答复"就绪"（status==0）。激活的唯一正面判据。 */
    public boolean isClientReady() {
        return clientReady;
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
     * 把 zstd 编码器插到原版压缩编码器的 <b>tail 侧</b>（出站先经过我们）。
     *
     * <p>⚠️ 这里是<b>插入</b>，不是"替换"。出站事件从 tail 走向 head，{@code addAfter}
     * 让本处理器排在原版压缩处理器<b>前面</b>：包先被我们收进批次，成帧后从
     * <b>原版处理器的 context</b> 往下写（见 {@code ZstdBatchEncoder#downstream}），
     * 等于绕过它、但不拆它。</p>
     *
     * <p>为什么不再替换：原版处理器一旦被摘掉就<b>装不回去</b>（Netty 对非
     * {@code @Sharable} 实例的 added 标志永不重置），于是任何一次"只装上一半"都只能
     * 以关连接收场；而且按名字查它的一批插件会直接落空。插入形态下回落只是"把自己摘掉"，
     * 原版那两个处理器从头到尾都在原位（见 {@link #uninstallZstd}）。</p>
     *
     * <p>⚠️ 也不能插到 {@code minecraft-encoder} 旁边：那里消息还是 MC 包对象，不是帧；
     * 更不能用 {@code addBefore} 插到原版压缩处理器的 head 侧——那样出站会<b>先被原版
     * 压一遍</b>再交给我们，对端解一层后仍是 zstd 帧（双层压缩，解析错乱）。</p>
     *
     * @return 是否确实安装成功（false 表示管线中找不到任何可用锚点）
     */
    public boolean installEncoder(Channel channel) {
        if (channel == null) return false;
        var p = channel.pipeline();
        // ⚠️ 幂等：激活重试会反复调用，已经装过就直接成功（不能再插一份同名处理器）
        if (p.get("zstd_encoder") != null) return true;
        // Velocity 4.x 的原版压缩编码器可能叫 compression-encoder 或 compress
        for (String anchor : new String[]{"compression-encoder", "compress"}) {
            if (p.get(anchor) != null) {
                p.addAfter(anchor, "zstd_encoder", new ZstdBatchEncoder());
                return true;
            }
        }
        LOGGER.warn("[Zstd] encoder not installed — compression anchor missing in pipeline {}", p.names());
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
     *   <li>切换请求由服务端经<b>原版压缩处理器的 context</b> 写出（见
     *       {@code ZstdHijacker#sendSwitchRequest}）—— 那是本闸门<b>更靠 head</b> 的一侧，
     *       不会回头经过它，握手不会自扣留。</li>
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

        // 确认嗅探器只读取原版未压缩或 zlib 帧；正常入站继续走原版解压与包解码。
        if (p.get("zstd_switch_confirm") == null) {
            boolean installed = false;
            for (String anchor : new String[]{"compression-decoder", "decompress"}) {
                if (p.get(anchor) != null) {
                    p.addBefore(anchor, "zstd_switch_confirm", new ZstdSwitchConfirmSniffer());
                    installed = true;
                    break;
                }
            }
            // ⚠️ 装不上就不能继续：没有任何处理器能走到 {@code confirm()}，闸门会把扣住的
            // 登录包一直扣到 5 秒超时才放行（每次登录固定卡 5 秒），而不是给出一条明确的
            // 放弃日志。这里与上面 minecraft-encoder 缺失的分支对称处理。
            if (!installed) {
                LOGGER.warn("[Zstd] 确认嗅探器无法安装（压缩解码器锚点缺失）— 放弃激活; pipeline={}",
                        p.names());
                if (p.get("zstd_switch_gate") != null) {
                    p.remove("zstd_switch_gate");
                }
                switchGate = null;
                return false;
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
     * 安装下行切换处理器：先安装扣包闸门和原版帧确认嗅探器，再安装下行编码器。
     * 任一步失败都整体回滚；正常上行一直走原版解压器与包解码器。
     *
     * <p>闸门必须先于下行编码器就位，确保客户端确认下行解码就绪之前，
     * 登录期数据仍被扣住。此时尚未发送 {@code zstd:switch}，安装失败可摘掉自身处理器回落原版。</p>
     *
     * @return 是否确实安装成功
     */
    public boolean installHandlers(Channel channel) {
        if (!installSwitchGate(channel)) {
            LOGGER.warn("[Zstd] 切换闸门安装失败（锚点缺失）— 放弃本次激活，回落原版 zlib");
            uninstallZstd(channel);
            return false;
        }
        if (!installEncoder(channel)) {
            uninstallZstd(channel);
            return false;
        }
        return true;
    }

    /**
     * 摘掉本插件装上的所有 handler（幂等），管线回到完整原版形态。
     *
     * <p>协议 v7 起原版压缩处理器<b>从来没有被移除过</b>（只插入、不替换），所以"回落原版"
     * 就等于"把自己摘干净"：{@code compression-encoder} / {@code compression-decoder}
     * 一直在原位，摘掉之后入站出站都回到原版 zlib 路径。旧实现替换掉了它们，回落时
     * 无处理器可装回（Netty 对非 {@code @Sharable} 实例的 added 标志永不重置），
     * 只能关连接——那正是这次改掉的东西。</p>
     *
     * <p>⚠️ 必须先于 {@code giveUp} 的补发：补发的包要按"当时的"管线编码，
     * 而摘掉之后管线里剩下的就是原版压缩处理器。</p>
     *
     * <p>⚠️ 只按名字摘<b>自己的</b>东西：绝不能顺手碰 {@code compression-encoder} /
     * {@code compression-decoder} / {@code frame-decoder}，那些都是原版的。</p>
     *
     * <p>⚠️ 只能在 event loop 上调用（与安装同一条路径）：移除 ByteToMessageDecoder
     * 会触发它的 {@code handlerRemoved}，那里面要做释放，跨线程摘会被 Netty 拒绝。</p>
     */
    public void uninstallZstd(Channel channel) {
        if (channel == null) return;
        var p = channel.pipeline();
        for (String name : new String[]{"zstd_encoder",
                "zstd_switch_gate", "zstd_switch_confirm"}) {
            // remove 对不存在的名字会抛 NoSuchElementException，先判存在
            if (p.get(name) != null) {
                p.remove(name);
            }
        }
        // 闸门实例已离开管线，清空引用供后续安装重新创建。
        switchGate = null;
        // 本连接不再走 zstd：计数与状态一起回退（setReplaced 幂等，未激活时是空操作）
        setReplaced(false);
    }

    /** 释放 zstd 上下文（归还租借池）。 */
    public void close() {
        stats.remove();
        // 走 setReplaced 归还计数（幂等），而不是直接判断 replaced —— 重复 close 不会多减
        setReplaced(false);
        // 只登记归还，不等待工作池的在途压缩；最后一个租约退出后由释放者归还。
        // 归还动作在租约锁外执行，重复 close 也只归还一次。
        compressLock.closeWhenIdle(() -> ZstdCtxPool.returnCompress(compressCtx));
    }
}
