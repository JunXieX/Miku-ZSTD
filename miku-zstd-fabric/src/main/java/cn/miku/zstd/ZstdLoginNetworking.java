package cn.miku.zstd;

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import net.fabricmc.fabric.api.client.networking.v1.ClientLoginNetworking;
import net.minecraft.client.Minecraft;
import net.minecraft.client.multiplayer.ClientHandshakePacketListenerImpl;
import net.minecraft.network.Connection;
import net.minecraft.network.FriendlyByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import cn.miku.zstd.mixin.ClientHandshakePacketListenerAccessor;
import cn.miku.zstd.mixin.ConnectionAccessor;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import mikumc.zstd.protocol.ZstdNegotiateStatus;

/**
 * 登录协商：zstd:negotiate 往返保留下行能力确认，上行始终沿用原版。
 * <pre>
 *   服务端 → 客户端：[int 协商版本]（4 字节大端）
 *   客户端 → 服务端：[varint 状态]（READY = 准备启用下行，VANILLA = 保持原版）
 * </pre>
 * <p>READY 应答不代替安装屏障：客户端仍须安装下行解码器，并在 zstd:switch 到达后回发
 * confirm，服务端才放行扣住的下行登录包。版本或载荷异常时，VANILLA 应答与本地回落同步执行。</p>
 */
public final class ZstdLoginNetworking {
    private static final Logger LOGGER = LoggerFactory.getLogger(MikuZstd.LOGGER_NAME);
    private static final io.netty.util.AttributeKey<CompressionResponseBarrier> RESPONSE_BARRIER =
            io.netty.util.AttributeKey.valueOf("zstd:compression-response-barrier");
    private static final io.netty.util.AttributeKey<Boolean> RESPONSE_CLOSE_HOOK =
            io.netty.util.AttributeKey.valueOf("zstd:compression-response-close-hook");

    /**
     * 观察压缩管线的最长等待（毫秒）。
     *
     * <p>⚠️ 旧实现<b>没有上限</b>：服务端发出 {@code zstd:negotiate} 却由于其它协议路径
     * 始终不发 SetCompression 时，本端会把应答无限期挂住；而服务端那边的等待预算只有
     * 2~3 秒，到点直接断开连接。现场表现为"卡在连接中进不去，两端日志都只有一行
     * Connecting to"，没有任何异常可查。</p>
     *
     * <p>到期后走 {@link #onCompressionWatchdog}：仍未装上原版压缩处理器时按"本轮不参与
     * zstd"应答 VANILLA（与 {@code markDeclined} 同步执行），并把这条路径记成一条 WARN。
     * 必须显著小于服务端的 {@code login.answer_timeout_ms}（默认 10s）。</p>
     */
    private static volatile long compressionWatchdogMs = 2000L;

    /** 原版压缩处理器的候选名——与 {@link ZstdClientActivation} 的锚点候选保持一致。 */
    private static final String[] COMPRESSION_DECODER_HANDLERS = {"decompress", "compression-decoder"};
    private static final String[] COMPRESSION_ENCODER_HANDLERS = {"compress", "compression-encoder"};

    private ZstdLoginNetworking() {
    }

    public static void register() {
        ClientLoginNetworking.registerGlobalReceiver(ZstdNegotiatePayload.ID, ZstdLoginNetworking::handleNegotiate);
    }

    private static CompletableFuture<FriendlyByteBuf> handleNegotiate(
            Minecraft client, ClientHandshakePacketListenerImpl handler,
            FriendlyByteBuf data, Consumer<io.netty.channel.ChannelFutureListener> listenerConsumer) {
        Channel channel = null;
        try {
            Connection connection = ((ClientHandshakePacketListenerAccessor) handler).getConnection();
            channel = ((ConnectionAccessor) connection).getChannel();
            beginNegotiation(channel, handler);

            // Fabric LoginQueryRequestHandler.receive 的线程契约是 Netty event loop。
            // 新 negotiate 先同步摘除旧下行 handler、清 ACTIVATED，再更换 native 上下文；
            // 旧 decoder 的 handlerRemoved 负责释放未完成包与暂存输出。
            // 仅此入口开启新流，重复 SetCompression 仍保留当前 decoder 和流状态。
            channel.attr(ZstdChannelManager.ZSTD_STATE).set(ZstdChannelManager.TransportState.PLAIN);
            ZstdClientActivation.onRoundDeclined(channel);

            // ⚠️ 本轮判定之前就要把上一轮的上下文换掉：这条连接可能被重放登录流程
            //（ViaFabricPlus 协议切换 / 重连到另一台服务器）。上一轮的解压流状态不属于
            // 新一轮连接；先撤旧 decoder 再换新 ctx，避免旧半包或历史进入新流。
            // 版本不匹配时也照做：这一轮不参与，但上下文不能把脏状态留给下一轮。
            ZstdChannelManager existing = channel.attr(ZstdChannelManager.KEY).get();
            if (existing != null) {
                existing.resetContexts();
            }

            int protocolVersion = data.readInt();
            LOGGER.info("[Zstd] negotiate rx version={} (client={}, match={}, remote={})",
                    protocolVersion, ZstdChannelManager.PROTOCOL_VERSION,
                    protocolVersion == ZstdChannelManager.PROTOCOL_VERSION, channel.remoteAddress());
            if (protocolVersion != ZstdChannelManager.PROTOCOL_VERSION) {
                LOGGER.error("[Zstd] Protocol version mismatch: server={}, client={} \u2014 staying vanilla",
                        protocolVersion, ZstdChannelManager.PROTOCOL_VERSION);
                markDeclined(handler);
                return answerAfterCompression(channel, ZstdNegotiateStatus.VANILLA);
            }

            ZstdChannelManager mgr = existing;
            if (mgr == null) {
                mgr = new ZstdChannelManager();
                channel.attr(ZstdChannelManager.KEY).set(mgr);
                final ZstdChannelManager toClose = mgr;
                channel.closeFuture().addListener(f -> toClose.close());
            }

            // 此刻原版压缩处理器可能尚未安装；切换嗅探器允许暂时挂在packet decoder前，
            // 等SetCompression插入decompress后仍位于它上游。连续解码器必须等实际switch帧。
            channel.attr(ZstdChannelManager.ZSTD_STATE)
                    .set(ZstdChannelManager.TransportState.NEGOTIATING);
            // 在 READY 答复交给网络前尝试挂接轻量切换嗅探器，避免响应监听器迟于switch帧。
            ZstdClientActivation.prepareForSwitch(channel);
            LOGGER.debug("[Zstd] negotiate ok (version={}) \u2014 waiting for switch before activating zstd", protocolVersion);

            // 应答写出后再补一次 switch 嗅探器安装，覆盖首次查询时 packet decoder 锚点尚未挂好的管线。
            // 此回调只准备原版阶段的嗅探器，不能提前安装连续流解码器。
            final Channel responseChannel = channel;
            listenerConsumer.accept(operation ->
                    ZstdClientActivation.activateAfterQueryResponseSent(responseChannel));

            // Fabric sends the completed Future immediately. Wait for handleCompression's TAIL so
            // the vanilla/Krypton encoder adds the data-length prefix to this small response.
            return answerAfterCompression(channel, ZstdNegotiateStatus.READY);
        } catch (Exception e) {
            LOGGER.error("[Zstd] Failed to process negotiate \u2014 staying vanilla", e);
            markDeclined(handler);
            return channel == null
                    ? CompletableFuture.completedFuture(unavailable())
                    : answerAfterCompression(channel, ZstdNegotiateStatus.VANILLA);
        }
    }

    /** Begin a new query round and retain a future until compression setup is observable. */
    private static final io.netty.util.AttributeKey<Object> COMPRESSION_READY_HANDLER =
            io.netty.util.AttributeKey.valueOf("zstd:compression-ready-handler");
    private static final io.netty.util.AttributeKey<Object> COMPRESSION_SETUP_HANDLER =
            io.netty.util.AttributeKey.valueOf("zstd:compression-setup-handler");
    private static final io.netty.util.AttributeKey<Object> COMPRESSION_SETUP_PACKET =
            io.netty.util.AttributeKey.valueOf("zstd:compression-setup-packet");

    /**
     * 原版压缩是否已经就位。
     *
     * <p>⚠️ 这里<b>不能</b>只认 {@code compress}/{@code decompress} 两个写死的名字：
     * {@link ZstdClientActivation} 的锚点候选里还有新版可能采用的
     * {@code compression-encoder}/{@code compression-decoder}。两处判定若不一致，
     * 一旦 26.x 换了名字，屏障就永远完不成——客户端不回包，服务端到点踢人。</p>
     */
    private static boolean compressionHandlersReady(Channel channel) {
        return firstPresent(channel, COMPRESSION_ENCODER_HANDLERS) != null
                && firstPresent(channel, COMPRESSION_DECODER_HANDLERS) != null;
    }

    private static String firstPresent(Channel channel, String[] candidates) {
        for (String name : candidates) {
            if (channel.pipeline().get(name) != null) return name;
        }
        return null;
    }

    /** 仅测试用：调整观察压缩管线的等待上限（0 = 立即到期）。 */
    static void setCompressionWatchdogMillisForTests(long millis) {
        compressionWatchdogMs = Math.max(0L, millis);
    }

    static void beginNegotiation(Channel channel, Object loginHandler) {
        CompressionResponseBarrier previous = channel.attr(RESPONSE_BARRIER).get();
        if (previous != null) previous.cancel();
        CompressionResponseBarrier barrier = new CompressionResponseBarrier(loginHandler, channel);
        channel.attr(RESPONSE_BARRIER).set(barrier);
        channel.attr(COMPRESSION_SETUP_HANDLER).set(loginHandler);
        if (channel.attr(RESPONSE_CLOSE_HOOK).setIfAbsent(Boolean.TRUE) == null) {
            channel.closeFuture().addListener(ignored -> {
                CompressionResponseBarrier pending = channel.attr(RESPONSE_BARRIER).getAndSet(null);
                if (pending != null) pending.cancel();
            });
        }
        if (loginHandler != null && channel.attr(COMPRESSION_READY_HANDLER).get() == loginHandler
                && compressionHandlersReady(channel)) {
            barrier.complete("compression-already-ready");
            return;
        }
        // 这一轮的压缩管线还看不见：挂一个有界看门狗，避免"永不回包"。
        barrier.armWatchdog();
        LOGGER.info("[Zstd] negotiate rx — waiting up to {}ms for compression setup (remote={})",
                compressionWatchdogMs, channel.remoteAddress());
    }

    /**
     * 看门狗到期：本轮观察不到压缩管线。
     *
     * <p>按"原版压缩处理器是否已经就位"分流：</p>
     * <ul>
     *   <li>已就位而屏障仍未完成（管线补齐晚于回调的极端时序）→ 补一次 {@code complete}，
     *       按本轮原本的结论应答。此时对端已经把压缩装好，回 VANILLA 反而造成单侧 zstd。</li>
     *   <li>尚未就位 → 本轮判定"不参与 zstd"：状态置 PLAIN、拆掉可能残留的下行处理器，
     *       应答 VANILLA。服务端据此按原版 zlib 结束，玩家照常进服而不是被超时踢掉。</li>
     * </ul>
     */
    private static void onCompressionWatchdog(CompressionResponseBarrier barrier) {
        if (barrier.isFinished()) return;
        Channel channel = barrier.channel;
        if (channel == null) {
            barrier.complete("watchdog-no-channel");
            return;
        }
        if (compressionHandlersReady(channel)) {
            LOGGER.info("[Zstd] compression handlers appeared after the watchdog — completing this round (remote={})",
                    channel.remoteAddress());
            barrier.complete("compression-ready-late");
            return;
        }
        LOGGER.warn("[Zstd] compression setup not observed in {}ms — answering VANILLA "
                        + "(this connection will not use zstd; remote={}, pipeline={})",
                compressionWatchdogMs, channel.remoteAddress(), channel.pipeline().names());
        try {
            channel.attr(ZstdChannelManager.ZSTD_STATE).set(ZstdChannelManager.TransportState.PLAIN);
            ZstdClientActivation.onRoundDeclined(channel);
        } catch (Throwable t) {
            LOGGER.debug("[Zstd] watchdog cleanup skipped: {}", t.toString());
        }
        barrier.overrideToVanilla();
        barrier.complete("watchdog-vanilla");
    }

    /** Called at handleCompression HEAD, before vanilla or Krypton rebuilds the compression handlers. */
    public static void onCompressionSetupStarted(Channel channel, Object loginHandler, Object packet) {
        if (channel == null) return;
        channel.attr(COMPRESSION_SETUP_HANDLER).set(loginHandler);
        channel.attr(COMPRESSION_SETUP_PACKET).set(packet);
        channel.attr(COMPRESSION_READY_HANDLER).set(null);
        CompressionResponseBarrier barrier = channel.attr(RESPONSE_BARRIER).get();
        if (barrier != null && barrier.loginHandler == loginHandler) barrier.beginSetup();
    }

    /** Called only after handleCompression/Connection.setupCompression has completed. */
    public static void markCompressionReady(Channel channel, Object loginHandler, Object packet) {
        if (channel == null) return;
        Runnable mark = () -> {
            if (channel.attr(COMPRESSION_SETUP_HANDLER).get() != loginHandler
                    || channel.attr(COMPRESSION_SETUP_PACKET).get() != packet) return;
            if (!compressionHandlersReady(channel)) return;
            channel.attr(COMPRESSION_READY_HANDLER).set(loginHandler);
            ZstdClientActivation.prepareForSwitch(channel);
            CompressionResponseBarrier barrier = channel.attr(RESPONSE_BARRIER).get();
            if (barrier != null && barrier.loginHandler == loginHandler) barrier.complete("compression-setup");
        };
        if (channel.eventLoop().inEventLoop()) mark.run();
        else channel.eventLoop().execute(mark);
    }

    /**
     * 本轮应答：先等压缩管线，再按<b>最终结论</b>写出状态码。
     *
     * <p>⚠️ 状态码必须在完成时从屏障里取，不能在调用时捕获：看门狗到期会把本轮结论从
     * READY 改成 VANILLA，若闭包里写死了 READY，就会出现"本端已按 VANILLA 收尾、却把
     * READY 发出去"的单侧 zstd——正是必须避免的组合。</p>
     */
    static CompletableFuture<FriendlyByteBuf> answerAfterCompression(Channel channel, int status) {
        CompressionResponseBarrier barrier = channel.attr(RESPONSE_BARRIER).get();
        if (barrier == null) return CompletableFuture.completedFuture(answer(status));
        barrier.requestStatus(status);
        return barrier.readyFuture().thenApply(ignored -> {
            int effective = barrier.effectiveStatus();
            LOGGER.info("[Zstd] negotiate answer={} ({}ms, outcome={}, remote={})",
                    effective == ZstdNegotiateStatus.READY ? "READY" : "VANILLA",
                    barrier.elapsedMillis(), barrier.outcome(), channel.remoteAddress());
            return answer(effective);
        });
    }

    static final class CompressionResponseBarrier {
        private final Object loginHandler;
        private final Channel channel;
        private final long startedNanos = System.nanoTime();
        private CompletableFuture<Void> ready = new CompletableFuture<>();
        private java.util.concurrent.Future<?> watchdog;
        private int effectiveStatus = ZstdNegotiateStatus.FALLBACK;
        private String outcome = "pending";

        private CompressionResponseBarrier(Object loginHandler, Channel channel) {
            this.loginHandler = loginHandler;
            this.channel = channel;
        }

        private synchronized void requestStatus(int status) { this.effectiveStatus = status; }

        private synchronized int effectiveStatus() { return effectiveStatus; }

        private synchronized String outcome() { return outcome; }

        private long elapsedMillis() { return (System.nanoTime() - startedNanos) / 1_000_000L; }

        private synchronized void beginSetup() {
            if (ready.isDone()) {
                ready = new CompletableFuture<>();
                outcome = "pending";
                armWatchdogLocked();
            }
        }

        private void armWatchdog() {
            synchronized (this) { armWatchdogLocked(); }
        }

        /** 已持有 this 的调用点用它，避免重复进锁。 */
        private void armWatchdogLocked() {
            cancelWatchdog();
            long millis = compressionWatchdogMs;
            if (millis <= 0L) {
                // 0 = 立即到期：交给 event loop 下一轮执行，保持"异步完成"的语义。
                if (channel != null) {
                    try {
                        watchdog = channel.eventLoop().schedule(
                                () -> onCompressionWatchdog(this), 0L, java.util.concurrent.TimeUnit.MILLISECONDS);
                        return;
                    } catch (Throwable ignored) {
                        // 落到下面的"不挂"分支。
                    }
                }
                return;
            }
            if (channel == null) return;
            try {
                watchdog = channel.eventLoop().schedule(
                        () -> onCompressionWatchdog(this), millis, java.util.concurrent.TimeUnit.MILLISECONDS);
            } catch (Throwable t) {
                LOGGER.debug("[Zstd] watchdog arm skipped: {}", t.toString());
            }
        }

        private void cancelWatchdog() {
            if (watchdog != null) {
                watchdog.cancel(false);
                watchdog = null;
            }
        }

        /** 正常完成：按 {@link #effectiveStatus()} 应答。 */
        private synchronized void complete(String reason) {
            cancelWatchdog();
            outcome = reason;
            if (!ready.isDone()) ready.complete(null);
        }

        private synchronized void cancel() {
            cancelWatchdog();
            outcome = "cancelled";
            ready.cancel(false);
        }

        private synchronized boolean isFinished() { return ready.isDone(); }

        private synchronized CompletableFuture<Void> readyFuture() { return ready; }

        /** 看门狗到期时把本轮结论改成 VANILLA（只在"压缩管线确实没来"的分支调用）。 */
        private synchronized void overrideToVanilla() {
            effectiveStatus = ZstdNegotiateStatus.VANILLA;
        }
    }

    /**
     * 本轮登录明确判定"不参与 zstd"（版本不匹配 / 载荷异常）：把状态置为
     * {@link ZstdChannelManager.TransportState#PLAIN}，并请激活层拆掉可能残留的 zstd 处理器。
     *
     * <p>⚠️ <b>为什么必须显式做这件事</b>：同一个 {@code Channel} 可能被<b>重放登录</b>
     * （ViaFabricPlus 协议切换、或同一条连接重新协商）。上一轮若已激活下行 zstd，本端就会
     * 继续按 zstd 解帧；而这一轮的对端是原版（或旧版插件）——形成"单侧 zstd"，
     * 对端发来的原版压缩帧会被本端 zstd 解码器拒绝，玩家<b>进不去服务器</b>。
     * 只有状态被置为 PLAIN，激活层才知道该把上一轮的 zstd 处理器拆掉（见
     * {@code ZstdClientActivation#onRoundDeclined}）。</p>
     *
     * <p>取不到 Channel 时静默跳过：这条路径只是"多一层保险"，失败不该影响登录。</p>
     */
    private static void markDeclined(ClientHandshakePacketListenerImpl handler) {
        try {
            Connection connection = ((ClientHandshakePacketListenerAccessor) handler).getConnection();
            Channel channel = ((ConnectionAccessor) connection).getChannel();
            channel.attr(ZstdChannelManager.ZSTD_STATE).set(ZstdChannelManager.TransportState.PLAIN);
            ZstdClientActivation.onRoundDeclined(channel);
        } catch (Throwable t) {
            LOGGER.debug("[Zstd] markDeclined skipped: {}", t.toString());
        }
    }

    /** 应答载荷只有一个 varint 下行能力状态；上行没有 zstd 协商状态。 */
    private static FriendlyByteBuf answer(int status) {
        FriendlyByteBuf buf = new FriendlyByteBuf(Unpooled.buffer());
        buf.writeVarInt(status);
        return buf;
    }

    /** 无法参与 zstd：按"保持原版"上报，服务端会据此回落原版 zlib。 */
    private static FriendlyByteBuf unavailable() {
        return answer(ZstdNegotiateStatus.VANILLA);
    }
}
