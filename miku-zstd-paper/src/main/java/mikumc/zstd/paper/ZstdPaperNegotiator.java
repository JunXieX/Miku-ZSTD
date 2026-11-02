package mikumc.zstd.paper;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import mikumc.zstd.protocol.ZstdNegotiateStatus;
import mikumc.zstd.protocol.ZstdVarInts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * 协商与激活（Paper 端核心调度）。
 *
 * <h2>时序（实测确定）</h2>
 * <ol>
 *   <li>注入发生在<b>连接建立时</b>（见 MikuZstdPaper 的 childHandler 包装）；</li>
 *   <li>协商查询在<b>首个入站包之后</b>发出——太早客户端还没挂好登录查询接收器；</li>
 *   <li>检测到 SetCompression 写出时进入 hold，结论确定后再按定型管道补发。</li>
 * </ol>
 * <p>实测顺序：negotiate → （客户端应答）→ SetCompression ✓</p>
 */
public class ZstdPaperNegotiator extends ChannelDuplexHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger("miku-zstd");

    /** NMS 包接口（MC 26.x 未混淆）；用于区分"MC 包"与 netty 控制消息 */
    private static final Class<?> NMS_PACKET = loadNmsPacket();

    /**
     * 加载 NMS 包接口；失败时必须留下一条 WARN。
     *
     * <p>⚠️ 旧实现直接 {@code tryLoad(...)} 返回 null：{@link #isPacket} 从此恒为 false，
     * 协商器<b>一个包都不扣</b>——zstd 仍然会激活，但登录期的包会以原版压缩发出，
     * 客户端已切 zstd 就解不开，表现成"偶发进不去"且日志里什么都没有。</p>
     */
    private static Class<?> loadNmsPacket() {
        Class<?> c = tryLoad("net.minecraft.network.protocol.Packet");
        if (c == null) {
            LOGGER.warn("[Zstd] 未找到 net.minecraft.network.protocol.Packet —— 协商器不会扣包，"
                    + "登录期包将以原版压缩直发；请确认服务端版本与本插件匹配");
        }
        return c;
    }

    private static final long RETRY_MS = 100;
    /**
     * 协商与安装共用的重试预算：{@code 100ms × 15 = 1.5s}。
     *
     * <p>只等一个 RTT 的应答。不能等太久：一旦 {@code SetCompression} 已写出，本处理器正
     * <b>扣着后续所有出站包</b>（holding），玩家看到的就是"卡在连接中"。超时后也不做
     * "按原版补发"，而是关连接让客户端重连（理由见 {@code tryActivate0} 的超时分支）。</p>
     *
     * <p>安装阶段同样用这个预算：锚点（原版 compress/decompress）是 {@code setupCompression}
     * 在同一次调用里紧跟着装上的，正常情况下几微秒就位；等不到就是这条连接压根没有压缩层。</p>
     */
    private static final int MAX_RETRIES = 15;

    /**
     * 等客户端<b>协商应答</b>的预算（30 × 100ms = 3s），比锚点安装宽一倍。
     *
     * <p>两者不能共用一个数：应答要跨客户端主线程（过登录监听器、可能正在加载区块 / GC），
     * 1.5s 在低配客户端上会误判成"客户端不回应"，而误判的代价是白回落一次；
     * 锚点安装是本端管线操作，几微秒就位，1.5s 已经非常宽松。</p>
     */
    private static final int ANSWER_RETRIES = 30;

    /**
     * 是否处于「已看到 SetCompression、尚未完成激活」的扣包窗口。
     *
     * <p>⚠️ {@code volatile} + {@link #holdLock}：写包线程与激活线程不是同一个。
     * Paper 上写包基本集中在主线程，Folia 上则分布在 global region 与各区域线程，
     * 而 {@code tryActivate} / {@code releaseHeld} 跑在 netty eventLoop。
     * 非 volatile 时 eventLoop 可能读不到写线程刚置位的值 —— 于是 SetCompression 之后
     * 的包会以<b>原版 zlib</b> 发出；而客户端在收到 SetCompression 时就已切到 zstd，
     * 这一帧必然解码失败、连接立即断开（表现为 activated 日志后 1 秒内 Disconnected）。</p>
     */
    private volatile boolean holding;

    /**
     * 协商是否已经定案（激活成功或明确回落）。
     *
     * <p>⚠️ 定案后<b>不能</b>立刻把协商器摘掉：客户端对查询的应答可能在服务端已经
     * 切进 CONFIGURATION 之后才到（Folia 上尤其明显）。这些迟到的应答若漏给
     * {@code ServerLoginPacketListenerImpl}，它不认识我们自造的 txId，会直接以
     * "unexpected query response" 断开 —— 本标志让协商器在定案后继续吞掉这类应答。</p>
     */
    private volatile boolean settled;

    /** 保护 {@link #holding} 的迁移与两个扣包列表：写线程与 eventLoop 线程都会碰。 */
    private final Object holdLock = new Object();
    private final List<Object> heldPackets = new ArrayList<>();
    private final List<ChannelPromise> heldPromises = new ArrayList<>();

    /**
     * 连接去向：{@code TRUE}=登录连接、{@code FALSE}=服务器列表 ping、{@code null}=尚未确定。
     *
     * <p>⚠️ 不能只用「是否见过握手包」判断——{@code ClientIntentionPacket} 在
     * <b>ping 与登录两种连接上都会发</b>，区别只在握手里的 nextState（1=STATUS、2=LOGIN）。
     * 曾因门控写成「见过握手包就发」，给 ping 连接也发了 LoginPluginRequest，
     * 客户端解析错位（{@code Received unknown packet id 4}）、服务器列表直接显示连不上。</p>
     */
    private Boolean loginConnection;

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (settled) {
            // 协商已定案，但协商器还要留一小段时间：客户端对查询的应答可能晚于服务端的
            // 协议切换到达。只吞本插件 txId 的应答，其余包原样下传。
            // ⚠️ 但"切换确认"要先把 txId 回填给闸门再吞 —— 帧层嗅探器是主路径，这里是它
            // 缺位时的包层兜底；若和普通应答一起吞掉，闸门只能白等 5 秒超时才放行扣包。
            if (isQueryAnswer(msg)) {
                int switchTx = readSwitchTxId(msg);
                ZstdPaperChannelManager m = ctx.channel().attr(ZstdPaperChannelManager.KEY).get();
                if (m != null && (readInt(msg, "transactionId", "id", "transactionID") == m.getNegotiateTxId()
                        || (switchTx > 0 && m.isReplaced() && m.markSwitchConfirmed(switchTx)))) {
                    io.netty.util.ReferenceCountUtil.release(msg);
                    return;
                }
            }
            super.channelRead(ctx, msg);
            return;
        }

        ZstdPaperChannelManager mgr = ctx.channel().attr(ZstdPaperChannelManager.KEY).get();

        // 1) 握手包：必须区分「服务器列表 ping」与「登录」。
        //    ⚠️ 两者都会发 ClientIntentionPacket，只看"有没有握手包"等于没有门控——
        //    给 ping 连接发 LoginPluginRequest 会让客户端把它当成 status 阶段的包解析，
        //    结果是 ping 失败（服务器列表显示连不上）+ 客户端刷 DecoderException。
        //    真正的判据是握手包的 nextState：1=STATUS(列表 ping)、2=LOGIN、3=TRANSFER。
        if (msg != null && msg.getClass().getSimpleName().contains("ClientIntention")) {
            int nextState = readNextState(msg);
            if (nextState == 1) {
                loginConnection = Boolean.FALSE; // 服务器列表 ping：永不发协商
            } else if (nextState >= 2) {
                loginConnection = Boolean.TRUE;
            } else {
                loginConnection = null; // 读不出 → 交给下面 login start 兜底判定
            }
        }

        // 2) 发协商：客户端此刻已挂好登录查询接收器，是最早能发的时机。
        //    · 已确认是登录连接 → 立即发；
        //    · 握手包读不出 nextState → 必须等到真正的登录首包（login start）才发。
        //      绝不允许"读不出就发"：那等于把 ping 一起打了，代价比"zstd 没生效"大得多。
        if (mgr != null && mgr.getNegotiateTxId() == -1
                && (Boolean.TRUE.equals(loginConnection)
                    || (loginConnection == null && isLoginStartPacket(msg)))) {
            sendNegotiate(ctx);
        }

        // 2) 包层拦截协商应答（需在 packet_handler 之前消费，否则原版会当成"意外应答"断连）
        // ⚠️ 这里<b>不能</b>用 `!mgr.isResponseReceived()` 做前置门控：应答到达的时机与
        //    激活触发点并不一一对应（SetCompression 那一侧也会起一条激活链），门控掉之后
        //    应答既不被消费也不被解析，落到登录监听器就是 "unexpected query response" 断连。
        //    真正的去重判据是下面的 txId 匹配；激活成功后协商器会自移除，不会留下长期开销。
        // ⚠️ 但必须保留「尚未发起协商就整段跳过」：否则回落原版的连接上，
        //    本处理器会对**每一个**入站包做 getSimpleName + 字段反射读取，
        //    是每条原版连接都要永久支付的税（negotiateTxId 为 -1 时不可能命中任何应答）。
        if (mgr != null && msg != null && mgr.getNegotiateTxId() != -1) {
            // 0) 切换确认（客户端发来的 zstd:switch）：必须在通用应答解析之前拦下
            //    帧层嗅探器是主路径；这里只是包层兜底（嗅探器装不上时）。
            int switchTx = readSwitchTxId(msg);
            if (switchTx > 0 && mgr.isReplaced() && mgr.markSwitchConfirmed(switchTx)) {
                io.netty.util.ReferenceCountUtil.release(msg);
                return;
            }
            String cn = msg.getClass().getSimpleName();
            if (cn.contains("Query") || cn.contains("Plugin")) {
                int tx = readInt(msg, "transactionId", "id", "transactionID");
                if (tx != 0 && tx == mgr.getNegotiateTxId()) {
                    byte[] payload = readBytes(msg, "data", "payload", "contents");
                    // 取不到字节 = 客户端没理解这次查询（success=false），与"理解了但拒绝"必须分开表述
                    boolean success = payload != null;
                    // 5.0.42 起应答载荷只有一个 varint 状态（0=就绪 / 2=保持原版）。
                    // 旧客户端的载荷仍是两个状态字节，但它会先因版本不符（现在是 8）回 2，
                    // 所以只读第一个 varint 就够；多出来的字节在本通道里被丢弃、不会外泄。
                    int status = ZstdNegotiateStatus.FALLBACK;
                    if (payload != null && payload.length >= 1) {
                        int[] cursor = {0};
                        status = ZstdNegotiateStatus.sanitize(
                                ZstdVarInts.readOr(payload, cursor, ZstdNegotiateStatus.FALLBACK));
                    }
                    LOGGER.debug("[Zstd] 捕获协商应答: {} tx={} status={} success={}",
                            cn, tx, status, success);
                    io.netty.util.ReferenceCountUtil.release(msg);
                    mgr.markResponse(status, success);
                    ctx.channel().eventLoop().execute(() -> tryActivate(ctx, 0));
                    return;
                }
            }
        }

        super.channelRead(ctx, msg);
    }



    /**
     * 发协商查询：载荷只有 {@code [int 协议版本]}。
     *
     * <p>不携带字典或压缩流状态；版本匹配且切换确认完成后，双方开始一条新连续流。</p>
     */
    private void sendNegotiate(ChannelHandlerContext ctx) {
        ZstdPaperChannelManager mgr = ctx.channel().attr(ZstdPaperChannelManager.KEY).get();
        if (mgr == null) return;

        int txId = ThreadLocalRandom.current().nextInt(1, Integer.MAX_VALUE);
        // ⚠️ 必须用原子抢占，不能"先读后写"：本方法有两条调用路径（入站看到握手包、
        // 写包看到 SetCompression 的兜底），Folia 上这两条路径的线程天然分离。
        // 重复发出会让客户端多回一个应答，而多出来的应答没人认领 ——
        // 落到登录监听器就是 "unexpected query response" 断连。
        if (!mgr.claimNegotiateTxId(txId)) return;

        ByteBuf buf = ctx.alloc().buffer();
        try {
            ZstdVarInts.write(buf, 0x04);              // 包 id：LoginPluginRequest
            ZstdVarInts.write(buf, txId);              // 消息 id，客户端原样带回
            ZstdVarInts.writeString(buf, "zstd:negotiate");
            buf.writeInt(ZstdPaperChannelManager.PROTOCOL_VERSION); // 载荷：只有版本
            ctx.writeAndFlush(buf);
            LOGGER.debug("[Zstd] Sent zstd:negotiate txId={}", txId);
        } catch (Throwable t) {
            if (buf.refCnt() > 0) buf.release();
            throw t;
        }
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        // ⚠️ 进入扣包窗口后，判定/扣包/放行必须与 releaseHeld 串行 —— 后者在 event loop
        // 上把扣住的包逐条写出。若写包线程（Folia 上是区域线程）就地放行，新包会插到
        // 那批旧包之前，登录期包顺序被打乱。
        // 只在"窗口内"或"本包是 SetCompression"时才投递，正常出站路径没有额外开销。
        // ⚠️ 一旦进入扣包窗口，本 handler 的所有出站写都必须排到 event loop，
        // 保证与 releaseHeld 串行、登录期包序不被打乱。
        //（不能只在"当前是 SetCompression"时排队：窗口开启后同线程紧随的 LoginSuccess
        // 会就地写出，排到仍在排队的 SetCompression 前面 —— 客户端先收 LoginSuccess
        // 再收 0x03 就是未知包。）
        if (!ctx.executor().inEventLoop()) {
            synchronized (holdLock) {
                if (!holding && isCompressionPacket(msg)) {
                    holding = true;
                }
            }
            ctx.executor().execute(() -> {
                try {
                    writeInEventLoop(ctx, msg, promise);
                } catch (Throwable t) {
                    io.netty.util.ReferenceCountUtil.release(msg);
                    promise.tryFailure(t);
                }
            });
            return;
        }
        writeInEventLoop(ctx, msg, promise);
    }

    private void writeInEventLoop(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
            throws Exception {
        if (settled) {
            // 已定案：不再参与协商，也不扣包（激活后的编码由 zstd_encoder 负责）
            super.write(ctx, msg, promise);
            return;
        }
        ZstdPaperChannelManager mgr = ctx.channel().attr(ZstdPaperChannelManager.KEY).get();
        if (mgr == null || mgr.isReplaced()) {
            super.write(ctx, msg, promise);
            return;
        }

        // ⚠️ 判定与扣包必须在同一把锁内完成，且要先置 holding 再写 SetCompression：
        // 否则并发的写线程可能在「SetCompression 已写出、holding 只对本题线程可见」的缝里
        // 把后续包按原版 zlib 发出去 —— 客户端此时已切 zstd，收到即解码失败断开。
        boolean startHold = false;
        boolean held = false;
        synchronized (holdLock) {
            if (!holding && isCompressionPacket(msg)) {
                holding = true;
                startHold = true;
            } else if (holding && isPacket(msg)) {
                // ⚠️ 只扣 MC 包：管道里还会流经 netty 自身的控制消息（协议阶段切换用的 Runnable），
                // 扣住它会阻塞 LOGIN→CONFIGURATION 切换且日志无异常。
                heldPackets.add(msg);
                heldPromises.add(promise);
                held = true;
            }
        }

        if (startHold) {
            // 这条日志是"该连接到底有没有压缩层"的唯一一手证据：
            // 有它 = setupCompression 已执行（客户端即将切压缩）；没有它却回落原版，
            // 说明这条连接的 SetCompression 压根没发出（阈值 <0 或 isMemoryConnection）。
            LOGGER.info("[Zstd] SetCompression 写出（remote={}, pipeline={}）",
                    ctx.channel().remoteAddress(), ctx.channel().pipeline().names());
            sendNegotiate(ctx); // 兜底（写操作放在锁外，避免持锁进 netty）
            super.write(ctx, msg, promise);
            ctx.channel().eventLoop().execute(() -> tryActivate(ctx, 0));
            return;
        }
        if (held) {
            return;
        }

        super.write(ctx, msg, promise);
    }

    /** 激活流程入口：包一层异常可见性——原实现里 scheduled task 抛异常会被静默吞掉。 */
    private void tryActivate(ChannelHandlerContext ctx, int attempt) {
        try {
            tryActivate0(ctx, attempt);
        } catch (Throwable t) {
            LOGGER.error("[Zstd] 激活流程异常（attempt={}）", attempt, t);
            // ⚠️ 必须走 finishVanilla（补发 + 自移除）：只 releaseHeld 会让协商器
            // 永久留在入站/出站热路径上，对每个包继续做字符串判定并持有旧类加载器。
            finishVanilla(ctx);
        }
    }

    private void tryActivate0(ChannelHandlerContext ctx, int attempt) {
        Channel channel = ctx.channel();
        if (!channel.isActive()) return;
        ZstdPaperChannelManager mgr = channel.attr(ZstdPaperChannelManager.KEY).get();
        if (mgr == null || mgr.isReplaced()) return;

        if (!mgr.isResponseReceived()) {
            // ⚠️ 预算只有一段：5.0.42 删掉字典下发后，应答里不再需要传字典字节，
            // 剩下的只是"客户端回一个 varint 状态"这一个 RTT —— 没有第二个阶段要等。
            if (attempt >= ANSWER_RETRIES) {
                // ⚠️ 完全没应答只可能是异常：原版客户端也会回一条 success=false 的应答
                //（走 isRefused 分支立即回落，不会走到这里）。所以这里不能"按原版补发扣包" ——
                // 对端若其实是模组客户端（只是应答丢了），它已经切到 zstd，补发即断连；
                // 明确关连接让客户端重连一次，故障可见且不会出现"单侧 zstd"的诡异状态。
                LOGGER.warn("[Zstd] no negotiate response within {}ms — closing connection so the client can retry",
                        RETRY_MS * ANSWER_RETRIES);
                releaseHeld(ctx);
                ctx.channel().close();
                retire(ctx);
                return;
            }
            channel.eventLoop().schedule(() -> tryActivate(ctx, attempt + 1), RETRY_MS, TimeUnit.MILLISECONDS);
            return;
        }

        if (mgr.isRefused()) {
            LOGGER.warn("[Zstd] 客户端保持原版 zlib，本端不激活");
            finishVanilla(ctx); // 明确回落：补发扣包并自移除
            return;
        }

        // 走到这里说明应答是 0（就绪）：isRefused 那条分支已经处理了 2（保持原版）。
        // 没有字典下发或补偿阶段；安装就绪后等待原版 switch 确认。
        LOGGER.debug("[Zstd] 正在安装下行 zstd 编码器 (pipeline={})", channel.pipeline().names());
        // 安装阶段仍然走自己的重试循环（计数从 0 起）：它等的是原版 handler 出现，
        // 与"等协商应答"是两条独立的时序，共用一个 attempt 会让晚进入安装阶段的一方
        // 一上来就被判预算耗尽。
        installHandlers(ctx, mgr, 0);
    }

    /**
     * 安装下行 zstd 编码器（独立重试循环，预算 {@link #MAX_RETRIES}）。
     *
     * <p>先确认下行压缩锚点与确认嗅探器所需的原版解压锚点，再安装闸门与下行编码器。
     * 正常上行始终经过原版 decompress 与包解码器，确认只同步客户端下行解码就绪。</p>
     *
     * <p>原版 compress/decompress 是在 SetCompression 写出<b>之后</b>才安装的，
     * 所以首轮必然找不到锚点——必须重试，但重试期间要保持 holding，
     * 否则扣住的包会用尚未定型的路径提前发出去。</p>
     *
     * @param installAttempt 安装阶段自己的重试序号（首轮 0）
     */
    private void installHandlers(ChannelHandlerContext ctx, ZstdPaperChannelManager mgr, int installAttempt) {
        Channel channel = ctx.channel();
        if (!channel.isActive()) return;
        // ⚠️ 重试任务走的是本方法（不是 tryActivate0），所以这里必须自己判一次：
        // 两条触发路径（SetCompression 写出 / 协商应答到达）都会起一条安装链，
        // 先成功的那条已经 setReplaced 并发出 switch，晚到的重试若不加这道判断，
        // 会再装一遍（各步都幂等）并<b>再发一次 switch 请求</b>——客户端收到两个
        // 不同 txId 的切换请求，第二次确认无人认领。实测（模拟客户端）可稳定复现。
        if (mgr.isReplaced()) return;

        if (!mgr.hasEncoderAnchor(channel) || !mgr.hasConfirmAnchor(channel)) {
            // 原版 compress/decompress 是在 SetCompression 写出之后才装的，首轮必然缺失。
            if (installAttempt < MAX_RETRIES) {
                channel.eventLoop().schedule(() -> installHandlers(ctx, mgr, installAttempt + 1),
                        RETRY_MS, TimeUnit.MILLISECONDS);
                return;
            }
            // ⚠️ 这不是错误：说明这条连接上原版压缩层没出现（多数是登录流程被第三方插件
            // 拉长，setupCompression 还没跑到），本端回落原版、玩家照常进服，只是拿不到 zstd。
            // 带 remote 是为了能把这条记录和具体玩家对上（同一个服里 6 次激活 / 11 次回落时，
            // 没有地址就分不清是哪条连接）。
            // ⚠️ 字段名必须如实：这里打印的是 holding（"扣包窗口是否开启"），不是"是否见过
            // SetCompression"。旧标签写成 setCompressionSeen，运维会据此误判成"服务器没发压缩包"。
            // 级别也必须是 debug：24 人在线的服上，每次回落都打一整行 pipeline 会淹没真问题；
            // 需要它的时候把 logging.debug 打开即可（5.0.36 起默认就是开的）。
            LOGGER.debug("[Zstd] anchors 未出现（compress/decompress 缺失）— 该连接回落原版; "
                            + "remote={}, holding={}, pipeline={}",
                    channel.remoteAddress(), holding, channel.pipeline().names());
            finishVanilla(ctx);
            return;
        }

        // 闸门及原版确认帧嗅探器先就位，确认客户端下行解码就绪后再放行扣包。
        if (!mgr.installSwitchGate(channel)) {
            if (installAttempt < MAX_RETRIES) {
                channel.eventLoop().schedule(() -> installHandlers(ctx, mgr, installAttempt + 1),
                        RETRY_MS, TimeUnit.MILLISECONDS);
                return;
            }
            LOGGER.warn("[Zstd] 切换闸门安装失败（encoder 或原版 decompress 锚点缺失）— 回落原版; pipeline={}",
                    channel.pipeline().names());
            finishVanilla(ctx);
            return;
        }

        if (!mgr.installEncoder(channel)) {
            if (installAttempt < MAX_RETRIES) {
                channel.eventLoop().schedule(() -> installHandlers(ctx, mgr, installAttempt + 1),
                        RETRY_MS, TimeUnit.MILLISECONDS);
                return;
            }
            LOGGER.warn("[Zstd] zstd 编码器安装失败（锚点在位却插入失败）— 回落原版; pipeline={}",
                    channel.pipeline().names());
            finishVanilla(ctx);
            return;
        }
        mgr.setReplaced(true); // 计数已内聚在 setReplaced 内（幂等，避免重复 +1）
        LOGGER.info("[Zstd] zstd transport activated on {}", MikuZstdPaper.platformName());
        LOGGER.debug("[Zstd] activation pipeline: {}", channel.pipeline().names());

        // ⚠️ 闸门此刻正扣着登录期的大帧（例如 LoginSuccess 压缩帧）。客户端在收到它之前
        // 不可能完成登录，也就没有任何东西会触发它的切换 —— 必须主动补一条"服务端已就绪"。
        sendSwitchRequest(channel);

        releaseHeld(ctx);
        // 激活后协商器不再参与协商，但仍需短暂留驻：迟到的查询应答必须被吞掉
        // （见 settled 注释）。顺序上必须先 releaseHeld 再退役 ——
        // 退役后经该 ctx 写出的语义不再有保证，扣住的包就会发不出去。
        retire(ctx);
    }

    /**
     * 明确回落原版：补发扣住的包，并<b>自移除</b>协商器。
     *
     * <p>⚠️ 自移除这一步以前只在"激活成功"分支里有，所有回落分支（超时、客户端拒绝、
     * 锚点始终缺失、安装失败）都只 releaseHeld 就返回——协商器于是常驻这条连接的
     * 入站/出站热路径，对<b>每一个</b>包继续做 {@code getSimpleName().contains(...)}
     * 字符串判定，并且一直持有本插件的类加载器。这些回落路径并不罕见（原版客户端、
     * 版本不匹配的客户端都会命中），属"每条非 zstd 连接都白付一份永久税"。</p>
     */
    private void finishVanilla(ChannelHandlerContext ctx) {
        ZstdPaperChannelManager mgr = ctx.channel().attr(ZstdPaperChannelManager.KEY).get();
        boolean vanillaConfirmed = mgr != null && mgr.isResponseReceived() && mgr.isRefused();
        if (vanillaConfirmed) {
            // 原版答复可能晚于 SetCompression：先摘除自身下行处理器，再按原版路径补发扣包。
            mgr.uninstallZstd(ctx.channel());
        }
        // 尚未明确收到原版答复时，客户端可能已经切换下行 zstd；保留既有 fail-fast 收尾。
        if (holding && !vanillaConfirmed) {
            releaseHeld(ctx);
            ctx.channel().close();
            retire(ctx);
            return;
        }
        releaseHeld(ctx);
        retire(ctx);
    }

    /**
     * 协商定案后的退役：先置位 {@link #settled} 吞掉迟到应答，再延迟自移除。
     *
     * <p>延迟而不是立刻移除，是因为客户端对 {@code zstd:negotiate} 的应答可能晚于服务端
     * 切进 CONFIGURATION 才到；这类应答漏给
     * {@code ServerLoginPacketListenerImpl} 会被判为 "unexpected query response" 并断开。
     * 10 秒足以覆盖登录与配置阶段的全部时序，之后自移除，避免热路径永久多一次判断。</p>
     */
    private void retire(ChannelHandlerContext ctx) {
        settled = true;
        try {
            ctx.channel().eventLoop().schedule(() -> removeSelf(ctx), 10, TimeUnit.SECONDS);
        } catch (Throwable t) {
            removeSelf(ctx);
        }
    }

    /** 是否是我们的查询应答（登录查询通道的应答；见 {@link #readSwitchTxId} 的同类判定）。 */
    private static boolean isQueryAnswer(Object msg) {
        if (msg == null) return false;
        String cn = msg.getClass().getSimpleName();
        return cn.contains("Query") || cn.contains("Plugin");
    }

    /** 从管线摘掉自身（幂等；已移除或通道已关时静默）。 */
    private void removeSelf(ChannelHandlerContext ctx) {
        try {
            ctx.pipeline().remove(this);
        } catch (Throwable t) {
            // 幂等：可能已移除、或通道正在关闭 → 无需处理
            LOGGER.debug("[Zstd] negotiator self-removal skipped: {}", t.toString());
        }
    }

    private void releaseHeld(ChannelHandlerContext ctx) {
        List<Object> packets;
        List<ChannelPromise> promises;
        // 取快照与清空在同一把锁内，写操作放到锁外：持锁进 netty 会与写线程互相等。
        synchronized (holdLock) {
            holding = false;
            if (heldPackets.isEmpty()) return;
            packets = new ArrayList<>(heldPackets);
            promises = new ArrayList<>(heldPromises);
            heldPackets.clear();
            heldPromises.clear();
        }
        LOGGER.debug("[Zstd] releasing {} packet(s) held during activation window", packets.size());
        for (int i = 0; i < packets.size(); i++) {
            ctx.write(packets.get(i), promises.get(i));
        }
        ctx.flush();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        releaseHeldOnTeardown();
        super.channelInactive(ctx);
    }

    /**
     * 本处理器被<b>外部</b>从管线摘除时的收尾（插件停用 / 重载会走这条路）。
     *
     * <p>⚠️ 这是 {@code channelInactive} 之外<b>另一条</b>独立的销毁路径：
     * {@code MikuZstdPaper#uninstallHook} 会遍历在线连接、直接
     * {@code pipeline.remove("miku-zstd-negotiator")}。若这条连接此刻正处在激活窗口内
     * （{@code holding == true}，扣着若干包与 promise），而通道<b>仍然活着</b>，
     * 那么 {@code channelInactive} 永远不会触发 —— 扣住的包没人 release（引用计数泄漏，
     * Netty leak detector 会报 LEAK），promise 也没人完成（上游 future 永久挂起）。</p>
     *
     * <p>所以两条销毁路径必须共用同一份收尾逻辑，且都要幂等。</p>
     */
    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        releaseHeldOnTeardown();
        super.handlerRemoved(ctx);
    }

    /** 销毁收尾（幂等）：释放扣住的包并失败化其 promise。列表清空后重复调用无副作用。 */
    private void releaseHeldOnTeardown() {
        List<Object> packets;
        List<ChannelPromise> promises;
        synchronized (holdLock) {
            holding = false;
            packets = new ArrayList<>(heldPackets);
            promises = new ArrayList<>(heldPromises);
            heldPackets.clear();
            heldPromises.clear();
        }
        // ⚠️ 先把包本身 release 掉：它们是 ReferenceCounted 的（内部持有 ByteBuf），
        // 扣下时所有权已转到本处理器；既然永远不会被写出，就没有别人替我们释放。
        for (Object p : packets) {
            io.netty.util.ReferenceCountUtil.release(p);
        }
        if (!promises.isEmpty()) {
            LOGGER.debug("[Zstd] negotiator torn down with {} held packet(s) — failing their promises",
                    promises.size());
            for (ChannelPromise p : promises) {
                p.tryFailure(new java.nio.channels.ClosedChannelException());
            }
        }
    }

    /**
     * 包层兜底：从查询/应答包里读出切换确认的 txId。
     *
     * <p>⚠️ 与帧层嗅探器同一判据（末尾 4 字节魔数），但这里拿不到帧的外层形态，
     * 只用于"嗅探器没装上的极端情况"。</p>
     *
     * <p>⚠️ txId 必须取<b>包的字段</b>（{@code transactionId}），不能从载荷首字节读：
     * 载荷的第一字节是 {@code success} 位，按 varint 读出来恒为 1（见
     * {@code ZstdStepFrames.writeConfirm} 的线格式），于是 txId 校验必然不通过、
     * 闸门永远等满超时才放行。</p>
     *
     * @return txId；不是切换确认返回 {@code -1}
     */
    private static int readSwitchTxId(Object msg) {
        if (msg == null) return -1;
        String cn = msg.getClass().getSimpleName();
        if (!cn.contains("Query") && !cn.contains("Plugin")) return -1;
        byte[] payload = readBytes(msg, "data", "payload", "contents");
        if (payload == null || payload.length < 5) return -1;
        byte[] magic = new byte[4];
        System.arraycopy(payload, payload.length - 4, magic, 0, 4);
        if (!mikumc.zstd.protocol.ZstdStepFrames.matchesMagic(magic)) return -1;
        int txId = readInt(msg, "transactionId", "id", "transactionID");
        return txId > 0 ? txId : -1;
    }

    /**
     * 发送"服务端已切换，请切换并发确认"（{@code zstd:switch}，4 字节魔数）。
     *
     * <p>⚠️ 必须经<b>原版压缩处理器</b>的 context 写出。协议 v7 起它还在管线里（我们只是
     * 插在它出站方向的 tail 侧），从它的 context 往下写等于跳过它自己、继续走 prepender。
     * 若改从 {@code zstd_encoder} 的 context 写，这一帧会被原版 {@code compress} 再压一遍
     * ——对端读到的 {@code dataLength} 就成了 zlib 流的头几个字节，必然解析失败。</p>
     *
     * <p>帧本身仍是直存形态 {@code [0][packet]}，外层长度由 prepender 补。对端在
     * <b>原版压缩</b>状态下也能读它（与原版未压缩帧逐字节同形）——"先切换再确认"的时序
     * 就建立在这条同形性上。</p>
     */
    private static void sendSwitchRequest(Channel channel) {
        // ⚠️ 写出位置：原版压缩处理器的 context（跳过它，保留 prepender）。
        //   · 闸门在更靠 tail 的一侧（出站先经过它），从这里写出不会被自己扣住；
        //   · 帧是手工编好的，绝不能再见一次压缩器（理由见方法注释）；
        //   · 找不到原版处理器时退回 zstd_encoder 的 ctx —— 那一格之后再无压缩器，
        //     帧不会被压第二遍（这种管线本来也是第三方改过的）。
        var pipeline = channel.pipeline();
        io.netty.channel.ChannelHandlerContext writeCtx = pipeline.context("compress");
        if (writeCtx == null) writeCtx = pipeline.context("compression-encoder");
        if (writeCtx == null) writeCtx = pipeline.context("zstd_encoder");
        if (writeCtx == null) {
            LOGGER.debug("[Zstd] cannot send switch request — no compression anchor in pipeline");
            return;
        }
        ByteBuf packet = writeCtx.alloc().buffer();
        ByteBuf frame = writeCtx.alloc().buffer();
        try {
            int txId = ThreadLocalRandom.current().nextInt(1, Integer.MAX_VALUE);
            ZstdPaperChannelManager mgr = channel.attr(ZstdPaperChannelManager.KEY).get();
            if (mgr != null) mgr.expectSwitchTxId(txId);
            mikumc.zstd.protocol.ZstdStepFrames.writeQuery(packet, txId,
                    mikumc.zstd.protocol.ZstdSwitchGate.CHANNEL);
            // 原版 switch 控制帧：[dataLength=0][packet]，不进入连续 Zstd 流。
            // 外层 Minecraft 长度仍由原版 prepender 添加。
            ZstdVarInts.write(frame, 0);
            frame.writeBytes(packet);
            writeCtx.writeAndFlush(frame);
            LOGGER.debug("[Zstd] switch request sent");
        } catch (Throwable t) {
            if (packet.refCnt() > 0) packet.release();
            if (frame.refCnt() > 0) frame.release();
            LOGGER.debug("[Zstd] failed to send switch request: {}", t.toString());
        } finally {
            if (packet.refCnt() > 0) packet.release();
        }
    }

    private static boolean isPacket(Object msg) {
        return NMS_PACKET != null && NMS_PACKET.isInstance(msg);
    }

    /** 是否是原版下发的 {@code SetCompression}（进入扣包窗口的唯一触发点）。 */
    private static boolean isCompressionPacket(Object msg) {
        return isPacket(msg)
                && msg.getClass().getSimpleName().contains("ClientboundLoginCompression");
    }

    /**
     * 读握手包的去向：{@code 1}=STATUS(服务器列表 ping)、{@code 2}=LOGIN、{@code 3}=TRANSFER。
     *
     * <p>读不出返回 {@code -1}——调用方<b>必须保守处理</b>，不可当作登录（否则会给 ping 发协商）。
     * MC 里该字段是 {@code ClientIntent} 枚举，也可能被映射成整数，两种都支持。</p>
     */
    private static int readNextState(Object intention) {
        Object v = readAny(intention, "nextState", "intention", "desiredState", "state");
        if (v instanceof Number n) {
            int i = n.intValue();
            return (i >= 1 && i <= 3) ? i : -1;
        }
        if (v instanceof Enum<?> e) {
            String n = e.name();
            if (n.contains("STATUS")) return 1;
            if (n.contains("LOGIN")) return 2;
            if (n.contains("TRANSFER")) return 3;
            return -1;
        }
        return -1;
    }

    /**
     * 是否是登录阶段的首包（login start）。
     *
     * <p>只在「握手包读不出 nextState」时作为兜底判据使用。刻意不匹配
     * {@code ClientIntention}，避免把 status ping 误判成登录。</p>
     */
    private static boolean isLoginStartPacket(Object msg) {
        String cn = msg.getClass().getSimpleName();
        return cn.contains("HelloPacket") || cn.contains("LoginStart") || cn.contains("ServerboundKey");
    }

    private static int readInt(Object target, String... names) {
        Object v = readAny(target, names);
        return v instanceof Number n ? n.intValue() : 0;
    }

    /**
     * 读取字节负载，兼容包装类型。
     *
     * <p>⚠️ 实测（Paper 26.2）：应答包的字段是
     * {@code payload: CustomQueryAnswerPayload}（包装对象），**不是 byte[]**。
     * 早先按 byte[] 直读失败后走了默认值 2，导致被误判成"客户端保持原版"。
     * 这里多下探一层，从包装对象里取出真正的字节。</p>
     */
    private static byte[] readBytes(Object target, String... names) {
        Object v = readAny(target, names);
        if (v instanceof java.util.Optional<?> opt) v = opt.orElse(null);
        if (v instanceof byte[] b) return b;
        if (v == null) return null;
        for (String n : new String[]{"buffer", "data", "bytes", "payload", "value", "contents", "buf"}) {
            Object inner = readAny(v, n);
            if (inner instanceof java.util.Optional<?> o2) inner = o2.orElse(null);
            if (inner instanceof byte[] b2) return b2;
            // 实测（Paper 26.2）：负载放在 FriendlyByteBuf 里
            if (inner instanceof ByteBuf bb) {
                byte[] out = new byte[bb.readableBytes()];
                bb.getBytes(bb.readerIndex(), out);
                return out;
            }
        }
        LOGGER.debug("[Zstd] 未能从 {} 取出应答字节（该包的负载结构可能又变了）", v.getClass().getName());
        return null;
    }

    private static Object readAny(Object target, String... names) {
        Class<?> c = target.getClass();
        while (c != null && c != Object.class) {
            for (String n : names) {
                try {
                    Field f = c.getDeclaredField(n);
                    f.setAccessible(true);
                    return f.get(target);
                } catch (Throwable ignored) {
                    // 试下一个候选名
                }
            }
            c = c.getSuperclass();
        }
        return null;
    }

    private static Class<?> tryLoad(String name) {
        try {
            return Class.forName(name);
        } catch (Throwable t) {
            return null;
        }
    }
}
