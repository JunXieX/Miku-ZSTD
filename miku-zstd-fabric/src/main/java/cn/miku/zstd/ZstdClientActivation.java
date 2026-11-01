package cn.miku.zstd;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelPipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 客户端 zstd 管道激活（幂等）。
 *
 * <p>触发点有三个，互为保险：</p>
 * <ol>
 *   <li>{@code Connection.setupCompression} 的 TAIL（原始时机）</li>
 *   <li>{@code ClientHandshakePacketListenerImpl.handleCompression} 的 TAIL</li>
 *   <li>{@link #activateAfterQueryResponseSent}：{@code zstd:negotiate} 应答写出之后的兜底。
 *       协商正常走在 SetCompression 之前，所以它多数时候只会记一句 debug；留着它是为了防
 *       "登录顺序被代理/模组调换"那种情况（详见该方法的注释）</li>
 * </ol>
 *
 * <h2>管线位置（协议 v7 起）</h2>
 * <pre>
 *   出站：encoder → zstd_encoder → [压缩被跳过] → prepender → encrypt → socket
 *   入站：socket → decrypt → splitter → zstd_switch_sniffer → zstd_decoder → decompress → decoder
 * </pre>
 * <p>本模组只<b>插入</b>，不替换原版 {@code compress}/{@code decompress}：出站把成帧结果从原版
 * 那个 context 往下写（跳过它），入站把每个包包成 {@code [0][packet]} 交给它透传。原版处理器
 * 因此一直留在管线里，按名字找它的模组照常工作，回落原版也只剩"把自己摘掉"这一个动作。</p>
 *
 * <p>路径 2 的必要性随之变了：现在锚点按名字<b>逐帧</b>解析，装早装晚都不会指向错的处理器。
 * 它还留在那里的理由有两个：一是重放登录（ViaFabricPlus 协议切换、同一条连接重新协商）后
 * 原版会拆掉 {@code compress}/{@code decompress} 再按名字重装，新实例可能落到我们<b>前面</b>，
 * 需要有人把位置掰回来（见 {@link #installEncoder} / {@link #installDecoder}）；二是协商应答
 * 之后仍需要一个兜底时机——顺序被调换时，它是唯一能补上激活的地方。</p>
 *
 * <p><b>与 Krypton 共存</b>：Krypton（客户端侧压缩优化模组）在 {@code setupCompression} 的
 * HEAD 就把 {@code compress}/{@code decompress} 换成基于 libdeflate 的同名 handler。
 * 名字不变，而我们是插在名字<b>旁边</b>、并且每帧按名字取锚点，所以它换多少次都不影响我们：
 * 出站跳过的是"当时占着这个名字的那一个"，入站仍由我们的解码器先解。激活成功后由 zstd
 * 接管压缩，Krypton 的加密、长度前缀（prepender）、flush 合并继续生效；若本轮没有协商成功，
 * Krypton 会继续用 libdeflate 压缩（原版 zlib 帧），不影响进服。</p>
 *
 * <p>⚠️ 不能把"本模组与 Krypton 都改压缩管线"理解成冲突：真正的冲突是<b>两侧被不同模组
 * 各换一半</b>（出站 zstd、入站 libdeflate）。因此 {@code activateInEventLoop} 在安装后
 * 会再校验一次 {@code zstd_encoder} 与 {@code zstd_decoder} 是否同时在位，缺一侧就放弃
 * 本轮激活，让下一次触发补齐，绝不置成"已激活"。</p>
 *
 * <h2>3.1.0 修复</h2>
 * <ul>
 *   <li><b>跳过时不再上锁</b>：旧实现在 {@code state != NEGOTIATING} 时也会把
 *       {@code ACTIVATED} 置真，一旦第一条触发路径早于 negotiate 处理完成，
 *       第二条路径就再也不可能激活 zstd（且日志上看不出异常）。</li>
 *   <li><b>安装后校验</b>：旧实现在管道锚点全不匹配时会静默什么都不做，却仍把状态
 *       置为 {@code ZSTD_ACTIVE}，造成"客户端单侧 zstd、服务端 vanilla"的必断连组合。
 *       现在锚点缺失直接报错返回、不置状态。</li>
 *   <li>删除 {@code addLast} 兜底：把 handler 挂到管道末端时它收到的是 {@code Packet}
 *       对象而非 {@code ByteBuf}，会走透传分支——zstd 实际未生效但状态却显示已激活。</li>
 * </ul>
 */
public final class ZstdClientActivation {

    private static final Logger LOGGER = LoggerFactory.getLogger(MikuZstd.LOGGER_NAME);

    /** 每连接防重标志（AttributeKey 值） */
    public static final io.netty.util.AttributeKey<Boolean> ACTIVATED =
            io.netty.util.AttributeKey.valueOf("zstd:client-activated");

    private ZstdClientActivation() {
    }

    /**
     * 把 zstd 编解码器插到原版压缩处理器两侧。幂等，可从任意线程调用
     * （内部会切到 eventLoop 执行）。
     */
    public static void activate(Channel channel) {
        if (channel == null) return;
        channel.eventLoop().execute(() -> activateInEventLoop(channel, false));
    }

    /**
     * 压缩阈值变化回调（{@code setupCompression} 每次都走这里，含同一条连接的第二次）。
     *
     * <p>阈值 &lt; 0 表示对端要关掉压缩（limbo 登录服把连接降回明文就是这样做的）。
     * 此时必须把 zstd 编解码器摘掉：原版 {@code setupCompression(-1)} 只移除
     * {@code compress}/{@code decompress} 这两个名字，而我们的处理器叫
     * {@code zstd_encoder}/{@code zstd_decoder}——不摘的话客户端继续用 zstd 发帧，
     * 对端却按明文读，第一帧就错乱。</p>
     *
     * <p>摘掉之后不需要任何补救：原版那两个处理器一条都没被我们摘过，连接自动回到
     * 原版（阈值 &gt; 0 时是 zlib、&lt; 0 时是明文）。协议 v7 之前正好相反——原版被替换掉了，
     * 这个时刻只能关连接。</p>
     */
    public static void onCompressionThreshold(Channel channel, int threshold) {
        if (channel == null) return;
        if (threshold < 0) {
            channel.eventLoop().execute(() -> deactivateQuietly(channel));
            return;
        }
        prepareForSwitch(channel);
    }

    /** 摘掉 zstd 编解码器，回到原版（阈值 &lt; 0 时对端已经切成明文）。 */
    private static void deactivateQuietly(Channel channel) {
        if (!channel.isActive()) return;
        // ⚠️ 直接摘，不走 deactivateIfStale 的状态闸门：那个闸门判的是"本轮登录不参与 zstd"，
        // 而这里是对端当场关掉压缩——状态可能仍是 ZSTD_ACTIVE，照闸门走就一个处理器都不摘，
        // 客户端继续按 zstd 发帧、对端已按明文读，首帧即错乱。
        // 状态本身刻意不改：同一条连接之后若重新下发 SetCompression（limbo 转后端），
        // ZSTD_ACTIVE 仍能放行重装（见 activateInEventLoop 里对它的说明）。
        boolean changed = removeOwnHandlers(channel.pipeline());
        channel.attr(ACTIVATED).set(Boolean.FALSE);
        if (changed) {
            LOGGER.info("[Zstd] 压缩被对端禁用（threshold<0）——客户端退回原版; pipeline={}",
                    channel.pipeline().names());
        }
    }

    public static void prepareForSwitch(Channel channel) {
        if (channel == null) return;
        // ⚠️ 必须在<b>当前线程</b>就切换：本方法由 {@code setupCompression} 的 TAIL 调用，
        // 那一刻本来就在 event loop 上。旧实现无条件 {@code eventLoop().execute(...)}，
        // 等于把切换排到下一轮事件循环 —— 而服务端的 zstd:switch 帧（以及 MC 对未注册
        // channel 自动回的 success=false 空应答）可能先一步到达：
        //   服务端收到 success=false → 立即放行扣住的登录包（zstd 编码）
        //   而本端切换任务还没跑 → 仍按原版解 → 首帧即失败断连。
        // 这就是 5.0.6 实机掉线的时序（与客户端版本新旧无关）。
        if (channel.eventLoop().inEventLoop()) {
            prepareNow(channel);
            return;
        }
        channel.eventLoop().execute(() -> prepareNow(channel));
    }

    private static void prepareNow(Channel channel) {
        if (!channel.isActive()) return;
        {
            // ⚠️ 必须<b>立即切换</b>，不能等服务端的 zstd:switch：
            // 服务端在收到协商应答后就激活并按 zstd 发帧；若本端等到 switch 才切，
            // 只要那条 switch 帧没被识别（嗅探器锚点缺失、第三方压缩层、旧版服务端
            // 不发 switch），本端就会回 success=false 并停在原版 ——
            // 服务端此时已提交到 zstd，只能"放行扣包"，两边帧格式对不上即断连。
            // 实测（Velocity + 5.0.6 客户端）就是这个路径。
            // 陈旧处理（去激活、位置复核）由 activateInEventLoop 自己完成，
            // 这里重复一遍只会让两处逻辑必须永久手工对齐。
            ChannelPipeline p = channel.pipeline();
            activateInEventLoop(channel, false);
            // 切换嗅探器仍要装：它负责把"已切换"的确认（带魔数）回给服务端的闸门，
            // 让服务端立刻放行扣住的登录包，而不是干等 5 秒超时。
            installSwitchSniffer(p);
            LOGGER.debug("[Zstd] client activated + armed for switch confirm; pipeline={}", p.names());
        }
    }

    /**
     * {@code zstd:negotiate} 应答<b>写出之后</b>调用：补一次激活尝试（quiet 版）。
     *
     * <h2>它现在是兜底，不是主路径</h2>
     * <p>正常的登录顺序是「协商 → SetCompression → 激活」：协商结束时 {@link ZstdChannelManager}
     * 的状态刚变成 NEGOTIATING，但压缩处理器还没装上，锚点一个都不存在，所以本方法多数时候
     * 只会记一句 debug 就返回。真正决定成败的仍是那两个 TAIL 触发点。</p>
     *
     * <p>保留它的理由是顺序不可靠：代理重发、模组改管线、重放登录都可能让 SetCompression
     * <b>早于</b>协商完成——那种情况下两个 TAIL 触发点当时都因为 {@code state != NEGOTIATING}
     * 跳过了，之后再也没有人激活。这一条就是那时候唯一还能补上的时机。</p>
     *
     * <h2>⚠️ 为什么必须"应答写出之后"才能调</h2>
     * <p>本方法会真的切换出站管道。若在应答写出<b>之前</b>切换，这条应答就会以 zstd 帧
     * （{@code [rawSize][packet]}）发出，而服务端此刻既没有 zstd 解码器，
     * 帧层应答嗅探器也只认裸帧 {@code [0x02][txId]...}——应答会被丢弃或按未知包 id 解析，
     * 协商直接失败。调用点因此挂在"应答包写出"的回调上（Fabric 的 {@code callbacksConsumer}）。</p>
     *
     * <h2>与 {@link #activate} 的唯一区别</h2>
     * <p>锚点缺失时只记 debug、不报 ERROR —— 那是本方法的正常时序；真正需要报警的尝试由上面
     * 那两个 TAIL 触发点负责（它们必然在 SetCompression 处理期间执行）。</p>
     */
    public static void activateAfterQueryResponseSent(Channel channel) {
        if (channel == null) return;
        channel.eventLoop().execute(() -> activateInEventLoop(channel, true));
    }

    /**
     * 服务端发来 {@code zstd:switch} 后调用：<b>先</b>把自己的解码器挂进入站管线，<b>再</b>回发确认。
     *
     * <h2>为什么必须先换再确认</h2>
     * <p>服务端在收到确认之前一个登录期大包都不会发（{@code ZstdSwitchGate} 扣着）。确认发出后
     * 它会立刻按 zstd 编码放行 —— 若本端此刻还没换解码器，第一个收到的 zstd 帧就会被原版
     * 解码器当成 {@code dataLength} 解析错乱，表现为<b>无法进入服务器</b>。这两件事的顺序
     * 是协议保证的一部分，不能调换。</p>
     *
     * @param txId 服务端询问带来的 txId（原样回发，服务端据此配对）
     */
    public static void activateAndConfirm(Channel channel, int txId) {
        if (channel == null) return;
        channel.eventLoop().execute(() -> {
            activateInEventLoop(channel, false);
            boolean onZstd = Boolean.TRUE.equals(channel.attr(ACTIVATED).get())
                    && channel.pipeline().get("zstd_decoder") != null;
            // ⚠️ 无论能否切换，都必须回一条应答：
            //   · 切成了 → success=true（服务端立即放行扣住的包并转严格 zstd）；
            //   · 没切成就 → success=false（服务端<b>不会</b>退回原版 —— 那做不到，但会据此
            //     立即放行扣住的包；本端届时仍是原版，解不开那些帧）。
            // 若不回，服务端只能等满 5 秒超时才放行 —— 每次登录都白白多等 5 秒。
            sendSwitchConfirm(channel, txId, onZstd);
        });
    }

    /**
     * 本轮登录明确判定"不参与 zstd"（版本不匹配 / 载荷异常）：请激活层
     * 拆掉上一轮可能残留的 zstd 处理器。
     *
     * <p>⚠️ 同一个 {@code Channel} 可能被<b>重放登录</b>（ViaFabricPlus 协议切换、或同一条连接
     * 重新协商）。上一轮若已激活 zstd，本端会一直以 zstd 发帧、并按 zstd 解帧；而这一轮对端
     * 是原版（或旧版插件）—— 形成"单侧 zstd"，对端发来的原版压缩帧会被本端 zstd 解码器拒绝，
     * 玩家<b>进不去服务器</b>。见 {@code ZstdLoginNetworking#markDeclined} 的调用点。</p>
     *
     * <p>从任意线程调用都安全（内部切到 eventLoop）。</p>
     */
    public static void onRoundDeclined(Channel channel) {
        if (channel == null) return;
        channel.eventLoop().execute(() -> deactivateIfStale(channel));
    }

    /**
     * 若状态已被判定为 {@code PLAIN}，则把上一轮残留的 zstd 处理器拆掉、清除激活标志。
     *
     * <p>⚠️ <b>只拆自己的三个名字</b>，原版 {@code compress}/{@code decompress} 一个都不碰。
     * 这不是"顺手保守"，而是回滚能不能成立的全部：协议 v7 起原版处理器从未被摘过，
     * 所以把它们留在原处，连接拆完就<b>立刻</b>回到原版 zlib（阈值 &lt; 0 时是明文）。
     * 旧实现反过来——原版在激活时被 {@code replace} 掉了，这里再把残留下来的同名 handler
     * 一并删掉，这条连接就再也回不到原版了（只能关连接重来）。</p>
     *
     * @return 是否确实处于"本轮不参与 zstd"状态（调用方据此提前返回）
     */
    private static boolean deactivateIfStale(Channel channel) {
        if (channel.attr(ZstdChannelManager.ZSTD_STATE).get()
                != ZstdChannelManager.TransportState.PLAIN) {
            return false;
        }
        boolean changed = removeOwnHandlers(channel.pipeline());
        if (changed) {
            channel.attr(ACTIVATED).set(Boolean.FALSE);
            LOGGER.info("[Zstd] 本轮登录判定为不参与 zstd —— 已拆除上一轮残留的 zstd 处理器，保持原版");
        }
        return true;
    }

    /**
     * 摘掉本模组自己的三个 handler（幂等），返回是否真的摘到了东西。
     *
     * <p>⚠️ 名单是<b>封闭</b>的：原版 {@code compress}/{@code decompress}（以及任何同名变体）
     * 永远不在这里。它们是我们"插入式接管"得以成立的前提，也是回滚时那条现成的退路。</p>
     */
    private static boolean removeOwnHandlers(ChannelPipeline p) {
        boolean changed = false;
        for (String name : new String[]{"zstd_encoder", "zstd_decoder", "zstd_switch_sniffer"}) {
            if (p.get(name) != null) {
                p.remove(name);
                changed = true;
            }
        }
        return changed;
    }

    /**
     * 发送切换应答。
     *
     * <p>⚠️ 必须写出<b>手工拼好的完整帧</b>，不能让编码器处理：编码器以 {@code flush} 为界成帧，
     * 确认包会被与同一次 flush 里的其它包合并成多包帧；而服务端帧层嗅探器只认"单包直存帧"，
     * 合帧后识别失败 → 确认丢失 → 服务端等满超时 → 双方各猜一半 → 进不去服务器。</p>
     *
     * <p>⚠️ 已切到 zstd 时，写出的位置必须是<b>原版压缩处理器的 context</b>，不能是本编码器的：
     * 出站事件从"该 context 的上一个处理器"继续，所以写在 {@code compress} 的 context 上等于
     * 跳过它、直接落到 prepender；写在自己的 {@code zstd_encoder} 上则会先经过 {@code compress}，
     * 它把这条手工帧当普通包再压一遍，服务端收到的是 {@code [dataLength][deflate(...)]}，
     * 帧层根本认不出裸帧。协议 v7 之前 {@code zstd_encoder} 占着 {@code compress} 的位置，
     * 所以旧代码写在自己身上恰好也是"跳过压缩"——别照抄那个写法。</p>
     *
     * <p>客户端管线有独立 prepender，所以手工帧只需 {@code [rawSize=0][packet]}，
     * 外层长度由它补齐。<b>不能</b>在 0 之后多写一层内层长度前缀：直存帧一帧一包、与原版
     * 未压缩帧逐字节同形，服务端才能正确解释（见 {@code ZstdBatchEncoderBase} 的帧格式说明）。</p>
     */
    private static void sendSwitchConfirm(Channel channel, int txId, boolean accepted) {
        ChannelPipeline p = channel.pipeline();
        boolean zstdActive = p.get("zstd_encoder") != null;
        io.netty.buffer.ByteBuf packet = channel.alloc().buffer();
        try {
            // ⚠️ 必须是 serverbound 的查询应答（0x02），而不是 clientbound 的询问（0x04）：
            // 服务端会把 0x04 解析成别的包并直接断连。
            mikumc.zstd.protocol.ZstdStepFrames.writeConfirm(packet, txId, accepted);

            if (zstdActive) {
                // 已切到 zstd：手工直存帧从原版压缩处理器之后写出——既绕开编码器的成帧逻辑
                // （确认必须单独成帧），也跳过原版那层压缩（见方法注释的第二段）。
                io.netty.channel.ChannelHandlerContext target = p.context("compress");
                if (target == null) target = p.context("compression-encoder");
                if (target == null) target = p.context("zstd_encoder");
                io.netty.buffer.ByteBuf frame = channel.alloc().buffer();
                try {
                    mikumc.zstd.protocol.ZstdVarInts.write(frame, 0);
                    frame.writeBytes(packet);
                    target.writeAndFlush(frame);
                    // ⚠️ 武装点必须精确落在这里：确认帧已经交给管线（排在它后面的字节之后才会
                    // 上线），从这一刻起本端产出的帧才是"服务端会离开兼容窗口去解的 zstd 帧"。
                    // 早一步 ⇒ 窗口内的帧只有本端记账；晚一步 ⇒ 本端漏记服务端已记的帧。
                    ChannelHandler encoder = p.get("zstd_encoder");
                    if (encoder instanceof ZstdBatchEncoder batch) {
                        batch.arm();
                    }
                } catch (Throwable t) {
                    if (frame.refCnt() > 0) frame.release();
                    throw t;
                }
            } else {
                // 未切到 zstd：交给原版压缩编码器（它自己处理阈值与压缩）。
                // 写在 "encoder" 的 context 上 = 跳过包编码器、正好落在 compress 上；
                // 退到 "compress" 那档时压缩会被跳过，只在管线被第三方拆改到连包编码器
                // 都没有时才会发生（那时怎么写都已经不可用，此处只保证不抛异常）。
                io.netty.channel.ChannelHandlerContext plain = p.context("encoder");
                if (plain == null) plain = p.context("compress");
                if (plain == null) {
                    LOGGER.debug("[Zstd] cannot send switch response — no write anchor");
                    return;
                }
                plain.writeAndFlush(packet.retain());
            }
            LOGGER.debug("[Zstd] switch {} sent (txId={})", accepted ? "confirmation" : "rejection", txId);
        } catch (Throwable t) {
            LOGGER.debug("[Zstd] failed to send switch response: {}", t.toString());
        } finally {
            if (packet.refCnt() > 0) packet.release();
        }
    }

    private static void activateInEventLoop(Channel channel, boolean quiet) {
        if (!channel.isActive()) return;
        ChannelPipeline p = channel.pipeline();

        // ⚠️ 顺序：必须先判"本轮明确不参与 zstd"（状态被置为 PLAIN）再往下走。
        // 这一步会摘掉上一轮残留的 zstd 处理器；放到安装流程之后，同一个 eventLoop 任务里
        // 刚装好的处理器就会立刻被自己拆掉（详见 deactivateIfStale 的说明）。
        if (deactivateIfStale(channel)) {
            return;
        }

        // ⚠️ 这里<b>不</b>清理"并存的原版处理器"：协议 v7 起它们从来就没被摘过，并存本身就是
        // 设计——出站跳过原版 compress（成帧结果从它的 context 往下写），入站交给原版
        // decompress 透传，因此不存在双重压缩。旧实现要清它们，是因为它把原版 replace 掉了，
        // 重放登录时原版又装回来一份新的，那才真的会压两遍（见 installEncoder 的位置复核）。

        // ⚠️ 激活标志只在"zstd 处理器仍在位、且位置正确"时才算数。
        // 曾直接把 ACTIVATED 当作终态早退：一旦这条 Channel 上重放登录流程
        // （ViaFabricPlus 协议切换、或同一条连接重新协商），原版会把 compress/decompress
        // 拆掉重装，新实例可能落到我们前面（出站就先被 deflate 压一遍），而 ACTIVATED
        // 仍为 true → 后续所有激活入口都提前返回，位置再也修不回来（日志上只有一条 debug）。
        if (Boolean.TRUE.equals(channel.attr(ACTIVATED).get())) {
            if (p.get("zstd_encoder") != null && p.get("zstd_decoder") != null
                    && handlersPositionedCorrectly(p)) {
                return; // 已激活、处理器在位且位置正确：无需重复安装
            }
            // 处理器缺失或位置被原版重建打乱：清掉标志，让下面的安装流程重新接管
            channel.attr(ACTIVATED).set(Boolean.FALSE);
            LOGGER.debug("[Zstd] reactivating — zstd handlers missing or misplaced; pipeline={}",
                    p.names());
        }

        ZstdChannelManager.TransportState state = channel.attr(ZstdChannelManager.ZSTD_STATE).get();
        // ⚠️ ZSTD_ACTIVE 也必须放行：走到这里说明上一段的"在位 + 位置"检查没通过，
        // 也就是处理器被摘掉了、或位置被原版重建打乱。真实触发场景是<b>同一条连接的第二次
        // SetCompression</b>——离线玩家经 limbo 登录服验证后转后端、或代理重发压缩阈值时，
        // 客户端会再次跑 setupCompression 并把 compress/decompress 拆掉重装。
        // 旧实现只认 NEGOTIATING，于是这次重装被 (state != NEGOTIATING) 挡掉，
        // 客户端停在原版 zlib 而服务端仍是 zstd —— 单侧 zstd，玩家被踢。
        if (state != ZstdChannelManager.TransportState.NEGOTIATING
                && state != ZstdChannelManager.TransportState.ZSTD_ACTIVE) {
            // 注意：此处不置 ACTIVATED —— 另一条触发路径（handleCompression TAIL）
            // 仍可能在 negotiate 到达后完成激活。
            LOGGER.debug("[Zstd] client activation deferred: state={} (negotiate not received yet)",
                    state == null ? "NONE" : state);
            return;
        }

        ZstdChannelManager existing = channel.attr(ZstdChannelManager.KEY).get();
        if (existing == null) {
            existing = new ZstdChannelManager();
            channel.attr(ZstdChannelManager.KEY).set(existing);
            final ZstdChannelManager toClose = existing;
            channel.closeFuture().addListener(f -> toClose.close());
        }
        final ZstdChannelManager mgr = existing;

        // ⚠️ 先查两端锚点是否都在，再动手插入（对齐服务端两侧）：
        // 否则可能出现"编码器插好了、解码器还没装"——本端以 zstd 发帧、却仍按原版
        // 解帧，与服务端形成必断连组合，且两侧日志都显示"activated"，极难定位。
        // 锚点缺失时<b>不</b>退回任何兜底位置：插在错的锚点上比不插更糟（见 installEncoder）。
        boolean encReady = p.get("zstd_encoder") != null || p.get("compress") != null
                || p.get("compression-encoder") != null;
        boolean decReady = p.get("zstd_decoder") != null || p.get("decompress") != null
                || p.get("compression-decoder") != null;
        if (!encReady || !decReady) {
            if (quiet) {
                // 协商比 SetCompression 先到：压缩处理器还没装上，属正常时序。
                // 真正决定成败的那次尝试在 setupCompression / handleCompression 的 TAIL 上，那里会报错。
                LOGGER.debug("[Zstd] activation deferred — compression handlers not installed yet "
                        + "(encoder={}, decoder={})", encReady, decReady);
            } else {
                LOGGER.error("[Zstd] activation aborted — pipeline anchors missing "
                        + "(encoder={}, decoder={}); pipeline={}", encReady, decReady, p.names());
            }
            return;
        }

        boolean encOk = installEncoder(p);
        boolean decOk = installDecoder(p);
        if (!encOk || !decOk) {
            // 锚点已确认齐备却仍插入失败：属异常（例如处理器被别的模组动态移除）。
            // 绝不置 ACTIVATED —— 让另一条触发路径仍有补救机会。
            LOGGER.error("[Zstd] activation aborted — handler insert failed despite anchors "
                    + "(encoder={}, decoder={}); pipeline={}", encOk, decOk, p.names());
            return;
        }

        // 已切到 zstd：此时管线里 {@code zstd_decoder} 与 {@code decompress} 并存。
        // 服务端的切换询问（zstd:switch）以裸帧到达，必须<b>在 zstd_decoder 上游</b>截获：
        // 让它先过我们的话，它会被当成"直存帧"喂进滚动历史（服务端那条询问是绕过编码器
        // 写的、没有喂历史 → 两端历史当场分叉 → 下一帧 dictGen 校验失败关连接），
        // 还会被原版 decompress 透传到包解码器，客户端按未注册通道回一条 success=false。
        installSwitchSniffer(p);

        // ⚠️ 安装后必须再校验一次"两侧确实都在位"。
        // 协议 v7 起我们不占用 compress/decompress 这两个名字，所以"被同名 handler 换掉"
        // 这条风险消失了；但反过来，别的模组若把我们的处理器当成"压缩处理器"摘掉、
        // 或整段重建管线，仍会留下单侧状态：出站 zstd / 入站原版（或反向），
        // 两边日志都显示已激活，首个往返即断连。
        // 缺任何一侧都不置 ACTIVATED，让下一次触发（handleCompression 的 TAIL）补齐；
        // 补齐后仍不一致则本端保持原版，服务端会按"未确认/无应答"处理，不会形成半 zstd。
        if (p.get("zstd_encoder") == null || p.get("zstd_decoder") == null) {
            LOGGER.warn("[Zstd] 激活后校验失败（encoder={}, decoder={}）——管线被其它模组覆盖，本轮保持原版",
                    p.get("zstd_encoder") != null, p.get("zstd_decoder") != null);
            return;
        }

        channel.attr(ZstdChannelManager.ZSTD_STATE).set(ZstdChannelManager.TransportState.ZSTD_ACTIVE);
        channel.attr(ACTIVATED).set(Boolean.TRUE);

        LOGGER.info("[Zstd] Client zstd transport activated; pipeline={}", p.names());
    }

    /**
     * 把切换询问嗅探器装到<b>我们解码器的上游</b>（幂等）。
     *
     * <p>锚点顺序固定为 {@code zstd_decoder} → {@code decompress} → {@code compression-decoder}：
     * 协议 v7 起 {@code zstd_decoder} 与 {@code decompress} 是<b>并存</b>的（我们不再替换它），
     * 只认 {@code decompress} 会把嗅探器插到我们自己<b>下游</b>，那时的帧已经变成
     * {@code [0][packet]}，理由见 {@link #activateInEventLoop} 里调用处的注释。</p>
     */
    private static void installSwitchSniffer(ChannelPipeline p) {
        String anchor = firstPresent(p, SNIFFER_ANCHORS);
        if (anchor == null) {
            LOGGER.debug("[Zstd] switch sniffer not installed (no anchor); pipeline={}", p.names());
            return;
        }
        if (p.get("zstd_switch_sniffer") != null) {
            if (isBeforeInPipeline(p, "zstd_switch_sniffer", anchor)) {
                return; // 已在正确一侧
            }
            // 位置错（例如我们的解码器被重插过、嗅探器留在下游）：摘掉重装，否则它看到的是
            // 我们改写过的 [0][packet]，那条询问会被当成普通包透传到包解码器
            p.remove("zstd_switch_sniffer");
            LOGGER.debug("[Zstd] switch sniffer was on the wrong side of {} — reinserting", anchor);
        }
        p.addBefore(anchor, "zstd_switch_sniffer", (ChannelHandler) new ZstdSwitchSniffer());
    }

    /** 原版压缩编码器可能的两个名字（顺序即优先级，与其它两端一致）。 */
    private static final String[] ENCODER_ANCHORS = {"compress", "compression-encoder"};

    /** 原版压缩解码器可能的两个名字（顺序即优先级）。 */
    private static final String[] DECODER_ANCHORS = {"decompress", "compression-decoder"};

    /**
     * 切换嗅探器的锚点（顺序即优先级）。
     *
     * <p>{@code zstd_decoder} 排在第一位：协议 v7 起它与 {@code decompress} 并存，而嗅探器
     * 必须落在<b>我们解码器的上游</b>。</p>
     */
    private static final String[] SNIFFER_ANCHORS = {"zstd_decoder", "decompress", "compression-decoder"};

    /**
     * 把编码器插到原版压缩处理器的 <b>tail 侧</b>（出站先经过我们），幂等。
     *
     * <p><b>为什么是插入而不是替换</b>：原版 {@code compress} 留在管线里，按名字查找它的
     * 模组（Krypton / ViaVersion / PacketEvents）照常工作；回落原版也只剩"把我们摘掉"这一个
     * 动作。出站事件按管线尾 → 头流动，所以"tail 侧"= 排在它<b>之后</b>（更靠尾）；
     * 成帧结果再由 {@code ZstdBatchEncoder#downstream} 从原版那个 context 往下写，
     * 于是那一层被<b>跳过</b>而不是被拆掉。</p>
     *
     * <p>⚠️ <b>位置必须复核</b>：重放登录（ViaFabricPlus 协议切换 / 同一条连接重新协商）时
     * 原版会拆掉 {@code compress} 再按名字重装，新实例会落在我们<b>前面</b>——那样出站先被
     * deflate 压一遍，我们再拿这堆压缩字节当"包"成帧，对端必然解不开。位置错了就重插一次，
     * 位置对了原样保留（重插要付代价，见下）。</p>
     *
     * <p>重插的代价：{@code handlerRemoved} 会清空本方向的滚动历史（攒批中的包也会被失败化），
     * 于是本端代数归零而对端仍在原代数，下一帧就会按 {@code dictGen} 校验失败关连接。
     * 之所以仍然选择重插：走到这里说明位置<b>已经</b>是错的（每一帧都发不出去），而真实触发
     * 时机是重放登录刚开始——那时上行还没发过几个字节，两端代数都是 0，代价并不发生。</p>
     */
    private static boolean installEncoder(ChannelPipeline p) {
        String anchor = firstPresent(p, ENCODER_ANCHORS);
        if (anchor == null) {
            // 没有原版锚点就没法确定"跳过谁"：已装也按失败上报，绝不插到别处去
            return p.get("zstd_encoder") != null;
        }
        if (p.get("zstd_encoder") != null) {
            if (isAfterInPipeline(p, "zstd_encoder", anchor)) {
                return true; // 已在 tail 侧：原样保留（重插会丢状态）
            }
            p.remove("zstd_encoder");
            LOGGER.debug("[Zstd] zstd_encoder was on the wrong side of {} — reinserting", anchor);
        }
        // ⚠️ 新装的编码器必须是"未武装"状态：我们在 SetCompression 时就把它挂进管线，但真正
        // 允许产 zstd 帧要等到回出切换确认那一刻（见 sendSwitchConfirm 里的 arm()）。
        // 中间这段时间对端还处在"按原版解释"的窗口里、只放行不记账；若本端这时产帧，
        // 就会出现"一端记了、一端没记"，滚动历史当场分叉（之后 dictGen 必炸）。
        ZstdBatchEncoder encoder = new ZstdBatchEncoder();
        encoder.disarm();
        p.addAfter(anchor, "zstd_encoder", (ChannelHandler) encoder);
        return true;
    }

    /**
     * 把解码器插到原版压缩处理器的 <b>上游</b>（入站先经过我们），幂等。
     *
     * <p>我们解出的每个包都以 {@code [0][packet]} 交给原版 {@code decompress} 透传，所以它必须
     * 留在我们<b>下游</b>。入站事件按管线头 → 尾流动，"上游"= 排在它<b>之前</b>（更靠头），
     * 对应 {@code addBefore}。</p>
     *
     * <p>⚠️ 位置同样要复核：原版重建 {@code decompress} 时新实例可能插到我们前面，
     * 那样它会先按原版规则读我们的 zstd 帧（把 {@code rawSize} 当 {@code dataLength}）→ 首帧即断。
     * 重插时同样会清空本方向的历史，所以只在确实错位时才动——错位本身已经是"每帧都解不开"，
     * 而正常重建（原版按 {@code addBefore("decoder")} 重装）不会改变相对顺序。</p>
     */
    private static boolean installDecoder(ChannelPipeline p) {
        String anchor = firstPresent(p, DECODER_ANCHORS);
        if (anchor == null) {
            return p.get("zstd_decoder") != null;
        }
        if (p.get("zstd_decoder") != null) {
            if (isBeforeInPipeline(p, "zstd_decoder", anchor)) {
                return true; // 已在上游侧：原样保留
            }
            p.remove("zstd_decoder");
            LOGGER.debug("[Zstd] zstd_decoder was on the wrong side of {} — reinserting", anchor);
        }
        p.addBefore(anchor, "zstd_decoder", (ChannelHandler) new ZstdBatchDecoder());
        return true;
    }

    /** 两个 zstd 处理器是否都在锚点的正确一侧（{@link #activateInEventLoop} 的早退判据）。 */
    private static boolean handlersPositionedCorrectly(ChannelPipeline p) {
        String encAnchor = firstPresent(p, ENCODER_ANCHORS);
        String decAnchor = firstPresent(p, DECODER_ANCHORS);
        return (encAnchor == null || isAfterInPipeline(p, "zstd_encoder", encAnchor))
                && (decAnchor == null || isBeforeInPipeline(p, "zstd_decoder", decAnchor));
    }

    /**
     * 出站顺序检查：{@code name} 是否排在 {@code anchor} 的<b>尾侧</b>（出站先经过它）。
     *
     * <p>出站事件按管线尾 → 头流动，下标更大 = 更靠尾 = 更早收到写事件。</p>
     */
    private static boolean isAfterInPipeline(ChannelPipeline p, String name, String anchor) {
        java.util.List<String> names = p.names();
        int anchorIndex = names.indexOf(anchor);
        int nameIndex = names.indexOf(name);
        return anchorIndex >= 0 && nameIndex >= 0 && nameIndex > anchorIndex;
    }

    /** 入站顺序检查：{@code name} 是否排在 {@code anchor} 的<b>头侧</b>（入站先经过它）。 */
    private static boolean isBeforeInPipeline(ChannelPipeline p, String name, String anchor) {
        java.util.List<String> names = p.names();
        int anchorIndex = names.indexOf(anchor);
        int nameIndex = names.indexOf(name);
        return anchorIndex >= 0 && nameIndex >= 0 && nameIndex < anchorIndex;
    }

    /** 按优先级取第一个存在于管线里的名字；都不在返回 {@code null}。 */
    private static String firstPresent(ChannelPipeline p, String[] candidates) {
        for (String name : candidates) {
            if (p.get(name) != null) {
                return name;
            }
        }
        return null;
    }
}
