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

    private static boolean compressionHandlersReady(Channel channel) {
        return channel.pipeline().get("compress") != null
                && channel.pipeline().get("decompress") != null;
    }

    static void beginNegotiation(Channel channel, Object loginHandler) {
        CompressionResponseBarrier previous = channel.attr(RESPONSE_BARRIER).get();
        if (previous != null) previous.cancel();
        CompressionResponseBarrier barrier = new CompressionResponseBarrier(loginHandler);
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
            barrier.complete();
        }
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
            if (channel.pipeline().get("compress") == null
                    || channel.pipeline().get("decompress") == null) return;
            channel.attr(COMPRESSION_READY_HANDLER).set(loginHandler);
            ZstdClientActivation.prepareForSwitch(channel);
            CompressionResponseBarrier barrier = channel.attr(RESPONSE_BARRIER).get();
            if (barrier != null && barrier.loginHandler == loginHandler) barrier.complete();
        };
        if (channel.eventLoop().inEventLoop()) mark.run();
        else channel.eventLoop().execute(mark);
    }

    static CompletableFuture<FriendlyByteBuf> answerAfterCompression(Channel channel, int status) {
        CompressionResponseBarrier barrier = channel.attr(RESPONSE_BARRIER).get();
        if (barrier == null) return CompletableFuture.completedFuture(answer(status));
        return barrier.readyFuture().thenApply(ignored -> answer(status));
    }

    static final class CompressionResponseBarrier {
        private final Object loginHandler;
        private CompletableFuture<Void> ready = new CompletableFuture<>();
        private CompressionResponseBarrier(Object loginHandler) { this.loginHandler = loginHandler; }
        private synchronized void beginSetup() {
            if (ready.isDone()) ready = new CompletableFuture<>();
        }
        private synchronized void complete() { ready.complete(null); }
        private synchronized void cancel() { ready.cancel(false); }
        private synchronized CompletableFuture<Void> readyFuture() { return ready; }
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
