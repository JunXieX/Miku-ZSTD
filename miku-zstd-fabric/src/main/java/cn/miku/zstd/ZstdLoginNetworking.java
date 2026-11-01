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

    private ZstdLoginNetworking() {
    }

    public static void register() {
        ClientLoginNetworking.registerGlobalReceiver(ZstdNegotiatePayload.ID, ZstdLoginNetworking::handleNegotiate);
    }

    private static CompletableFuture<FriendlyByteBuf> handleNegotiate(
            Minecraft client, ClientHandshakePacketListenerImpl handler,
            FriendlyByteBuf data, Consumer<io.netty.channel.ChannelFutureListener> listenerConsumer) {
        try {
            Connection connection = ((ClientHandshakePacketListenerAccessor) handler).getConnection();
            Channel channel = ((ConnectionAccessor) connection).getChannel();

            // Fabric LoginQueryRequestHandler.receive 的线程契约是 Netty event loop。
            // 新 negotiate 先同步摘除旧下行 handler、清 ACTIVATED，再更换 native 上下文；
            // 旧 decoder 的 handlerRemoved0 负责释放滚动历史与字典引用。
            // 仅此入口清理，重复 SetCompression 仍保留当前 decoder 和历史。
            channel.attr(ZstdChannelManager.ZSTD_STATE).set(ZstdChannelManager.TransportState.PLAIN);
            ZstdClientActivation.onRoundDeclined(channel);

            // ⚠️ 本轮判定之前就要把上一轮的上下文换掉：这条连接可能被重放登录流程
            //（ViaFabricPlus 协议切换 / 重连到另一台服务器）。滚动历史算出的字典是
            //「装载进上下文」的，而 zstd 没有卸载接口——上一轮那份历史会留在上下文里，
            // 新一轮的滑窗却从零开始，两边的 dictGen 与字典内容立刻错位，帧被静默解成垃圾。
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
                return CompletableFuture.completedFuture(unavailable());
            }

            ZstdChannelManager mgr = existing;
            if (mgr == null) {
                mgr = new ZstdChannelManager();
                channel.attr(ZstdChannelManager.KEY).set(mgr);
                final ZstdChannelManager toClose = mgr;
                channel.closeFuture().addListener(f -> toClose.close());
            }

            // ⚠️ 这里<b>不</b>安装任何 handler：此刻压缩处理器还没装，唯一的锚点会是
            // packet_handler —— 那里收到的是已解码的 Packet 对象，帧层处理根本不会执行。
            // 正确时机在 ZstdClientActivation.activateInEventLoop（锚点齐备之后）。
            channel.attr(ZstdChannelManager.ZSTD_STATE)
                    .set(ZstdChannelManager.TransportState.NEGOTIATING);
            LOGGER.debug("[Zstd] negotiate ok (version={}) \u2014 will activate zstd", protocolVersion);

            // 应答写出之后再补一次激活尝试（quiet 版）：正常情况下 SetCompression 还没到、
            // 锚点不存在，这里只会记 debug；但代理或其它模组把登录顺序调换时，它是唯一
            // 能补上的时机。这里只准备下行；switch/confirm 仍承担安装就绪屏障。
            listenerConsumer.accept(operation ->
                    ZstdClientActivation.activateAfterQueryResponseSent(channel));

            return CompletableFuture.completedFuture(answer(ZstdNegotiateStatus.READY));
        } catch (Exception e) {
            LOGGER.error("[Zstd] Failed to process negotiate \u2014 staying vanilla", e);
            markDeclined(handler);
            return CompletableFuture.completedFuture(unavailable());
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
