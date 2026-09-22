package cn.miku.zstd;

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import net.fabricmc.fabric.api.client.networking.v1.ClientLoginNetworking;
import net.minecraft.client.Minecraft;
import net.minecraft.client.multiplayer.ClientHandshakePacketListenerImpl;
import net.minecraft.network.Connection;
import net.minecraft.network.FriendlyByteBuf;
import net.minecraft.resources.Identifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import cn.miku.zstd.mixin.ClientHandshakePacketListenerAccessor;
import cn.miku.zstd.mixin.ConnectionAccessor;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import mikumc.zstd.protocol.ZstdDictId;
import mikumc.zstd.protocol.ZstdNegotiateStatus;

/**
 * 登录协商：{@code zstd:negotiate}（轻量握手）+ {@code zstd:dict}（按需推送字典）。
 *
 * <h2>协议 v4：两段式（相对 v3 的变化）</h2>
 * <p>v3 把字典字节直接内联进 negotiate——所有连接都要吃这几百 KB，无论客户端支不支持
 * zstd、也不管本地是否已缓存。v4 拆成两段：</p>
 * <ol>
 *   <li><b>negotiate</b>：只带协议版本与两个 dictId（几十字节）。客户端按本地缓存情况
 *       回 {@code 0 = 就绪} / {@code 1 = 需要字典}；</li>
 *   <li><b>zstd:dict</b>：仅当客户端回 1 时才由服务端推送字典字节，客户端加载后回
 *       {@code 0} / {@code 2}。</li>
 * </ol>
 * <p>于是重连（缓存命中）与非 zstd 客户端都是零字典流量——这对<b>Paper 端</b>尤其重要：
 * 直连场景下无法用主机名标记预筛客户端，只能向每条连接发 negotiate。</p>
 *
 * <h2>状态语义（双端必须完全一致）</h2>
 * <p>{@code 0 = 客户端会启用 zstd}、{@code 1 = 需要字典（仍打算启用）}、
 * {@code 2 = 客户端保持原版}。服务端严格按 2 回落原版 zlib，因此客户端任何返回 2
 * 的分支都必须同时放弃激活，否则会出现"服务端 zlib / 客户端 zstd"的必断连组合。</p>
 */
public final class ZstdLoginNetworking {
    private static final Logger LOGGER = LoggerFactory.getLogger(MikuZstd.LOGGER_NAME);

    /** 字典按需推送的查询标识 */
    static final Identifier DICT_ID = Identifier.fromNamespaceAndPath("zstd", "dict");

    private ZstdLoginNetworking() {
    }

    public static void register() {
        ClientLoginNetworking.registerGlobalReceiver(ZstdNegotiatePayload.ID, ZstdLoginNetworking::handleNegotiate);
        ClientLoginNetworking.registerGlobalReceiver(DICT_ID, ZstdLoginNetworking::handleDict);
    }

    private static CompletableFuture<FriendlyByteBuf> handleNegotiate(
            Minecraft client, ClientHandshakePacketListenerImpl handler,
            FriendlyByteBuf data, Consumer<io.netty.channel.ChannelFutureListener> listenerConsumer) {
        try {
            Connection connection = ((ClientHandshakePacketListenerAccessor) handler).getConnection();
            Channel channel = ((ConnectionAccessor) connection).getChannel();

            int protocolVersion = data.readInt();
            if (protocolVersion != ZstdChannelManager.PROTOCOL_VERSION) {
                LOGGER.error("[Zstd] Protocol version mismatch: server={}, client={} \u2014 staying vanilla",
                        protocolVersion, ZstdChannelManager.PROTOCOL_VERSION);
                return CompletableFuture.completedFuture(unavailable());
            }
            long encoderDictId = data.readLong();
            long decoderDictId = data.readLong();
            data.readByte(); // flags：v4 不再据此判断是否内联字节，仅保留字段以兼容读取

            ZstdChannelManager existing = channel.attr(ZstdChannelManager.KEY).get();
            if (existing == null) {
                existing = new ZstdChannelManager();
                channel.attr(ZstdChannelManager.KEY).set(existing);
                final ZstdChannelManager toClose = existing;
                channel.closeFuture().addListener(f -> toClose.close());
            }
            final ZstdChannelManager mgr = existing;

            // 每个方向：READY = 已就绪（无需字典或本地已缓存），NEED_DICT = 需要服务端推送
            byte encoderStatus = resolve(mgr, encoderDictId, true);
            byte decoderStatus = resolve(mgr, decoderDictId, false);

            boolean refuse = encoderStatus == ZstdNegotiateStatus.VANILLA
                    || decoderStatus == ZstdNegotiateStatus.VANILLA;
            boolean needDict = encoderStatus == ZstdNegotiateStatus.NEED_DICT
                    || decoderStatus == ZstdNegotiateStatus.NEED_DICT;
            // 需要字典时先保持 PLAIN：等 zstd:dict 到达并加载成功后才进入 NEGOTIATING。
            // 这样即便服务端最终没推字典，客户端也只是保持原版，不会形成单侧 zstd。
            channel.attr(ZstdChannelManager.ZSTD_STATE).set(refuse || needDict
                    ? ZstdChannelManager.TransportState.PLAIN
                    : ZstdChannelManager.TransportState.NEGOTIATING);

            LOGGER.debug("[Zstd] LoginPlugin negotiated: encId={} encStatus={} decId={} decStatus={} -> {}",
                    encoderDictId, encoderStatus, decoderDictId, decoderStatus,
                    refuse ? "staying vanilla" : (needDict ? "awaiting dict" : "will activate zstd"));

            return CompletableFuture.completedFuture(answer(encoderStatus, decoderStatus));
        } catch (Exception e) {
            LOGGER.error("[Zstd] Failed to process negotiate \u2014 staying vanilla", e);
            return CompletableFuture.completedFuture(unavailable());
        }
    }

    /** zstd:dict：服务端按需推送字典字节，客户端加载后回最终状态。 */
    private static CompletableFuture<FriendlyByteBuf> handleDict(
            Minecraft client, ClientHandshakePacketListenerImpl handler,
            FriendlyByteBuf data, Consumer<io.netty.channel.ChannelFutureListener> listenerConsumer) {
        try {
            Connection connection = ((ClientHandshakePacketListenerAccessor) handler).getConnection();
            Channel channel = ((ConnectionAccessor) connection).getChannel();
            ZstdChannelManager mgr = channel.attr(ZstdChannelManager.KEY).get();
            if (mgr == null) {
                LOGGER.warn("[Zstd] dict query without channel manager \u2014 staying vanilla");
                return CompletableFuture.completedFuture(unavailable());
            }

            byte flags = data.readByte();
            // 未被 flags 点名的方向按"不可用"处理：服务端会据此回落，而不是单侧激活
            byte encoderStatus = ZstdNegotiateStatus.VANILLA;
            byte decoderStatus = ZstdNegotiateStatus.VANILLA;
            if ((flags & 1) != 0) {
                encoderStatus = loadOne(mgr, data, true);
            }
            if ((flags & 2) != 0) {
                decoderStatus = loadOne(mgr, data, false);
            }

            boolean ok = encoderStatus == ZstdNegotiateStatus.READY
                    && decoderStatus == ZstdNegotiateStatus.READY;
            if (ok) {
                channel.attr(ZstdChannelManager.ZSTD_STATE)
                        .set(ZstdChannelManager.TransportState.NEGOTIATING);
            }
            LOGGER.debug("[Zstd] dict delivered: encStatus={} decStatus={} -> {}",
                    encoderStatus, decoderStatus, ok ? "will activate zstd" : "staying vanilla");
            return CompletableFuture.completedFuture(answer(encoderStatus, decoderStatus));
        } catch (Exception e) {
            LOGGER.error("[Zstd] Failed to process dict \u2014 staying vanilla", e);
            return CompletableFuture.completedFuture(unavailable());
        }
    }

    /**
     * 判断一个方向是否需要字典。
     *
     * <p>⚠️ 装载失败一律改报 {@link ZstdNegotiateStatus#NEED_DICT}（而不是 VANILLA）：
     * 让服务端推一份过来就能继续用 zstd，比直接放弃整条链路好。关键是<b>绝不能谎报就绪</b>——
     * 那会造成服务端带字典压缩、本端无字典解压的必断连组合。</p>
     *
     * @return {@link ZstdNegotiateStatus#READY} 或 {@link ZstdNegotiateStatus#NEED_DICT}
     */
    private static byte resolve(ZstdChannelManager mgr, long dictId, boolean isEncoder) {
        if (dictId == ZstdDictId.NONE) {
            return ZstdNegotiateStatus.READY; // 服务端尚无字典：协议没问题，照常参与 zstd
        }
        if (!DictCache.contains(dictId)) {
            return ZstdNegotiateStatus.NEED_DICT; // 本地无缓存：请求服务端推送
        }
        return loadCached(mgr, dictId, isEncoder)
                ? (byte) ZstdNegotiateStatus.READY
                : (byte) ZstdNegotiateStatus.NEED_DICT;
    }

    private static boolean loadCached(ZstdChannelManager mgr, long dictId, boolean isEncoder) {
        try {
            byte[] cached = DictCache.get(dictId);
            if (cached == null) {
                // 缓存查到了 id 却取不到字节 = 状态不一致。绝不能"假装成功"。
                LOGGER.warn("[Zstd] 缓存中取不到字典 id={} \u2014 改报需要服务端推送", dictId);
                return false;
            }
            boolean ok = isEncoder
                    ? mgr.loadEncoderDict(cached, dictId)
                    : mgr.loadDecoderDict(cached, dictId);
            if (!ok) {
                LOGGER.warn("[Zstd] 装载缓存字典失败 id={} \u2014 改报需要服务端推送", dictId);
            }
            return ok;
        } catch (Exception e) {
            LOGGER.warn("[Zstd] Failed to load cached dict id={} \u2014 reporting unavailable", dictId, e);
            return false;
        }
    }

    /**
     * 按 {@code [int crc][int len][bytes]} 读取一个方向的字典并装载。
     *
     * <p>这里的 crc 就是<b>字典 id 本身</b>（见 {@link ZstdDictId}）——服务端用它声明、
     * 客户端用它做缓存键，两端必须是同一个算法。校验通过后按同一数值入库，
     * 这样重连时 {@code DictCache.contains(id)} 才命中，否则每次登录都要重下几百 KB。</p>
     */
    private static byte loadOne(ZstdChannelManager mgr, FriendlyByteBuf in, boolean isEncoder) {
        try {
            int crc = in.readInt();
            int len = in.readInt();
            if (len <= 0 || len > 0x100000) {
                LOGGER.warn("[Zstd] dict size out of range: {} \u2014 reporting unavailable", len);
                return (byte) ZstdNegotiateStatus.VANILLA;
            }
            byte[] bytes = new byte[len];
            in.readBytes(bytes);
            long dictId = ZstdDictId.of(bytes);
            if ((int) dictId != crc) {
                LOGGER.warn("[Zstd] dict CRC mismatch \u2014 reporting unavailable");
                return (byte) ZstdNegotiateStatus.VANILLA;
            }
            DictCache.put(dictId, bytes);
            boolean loaded = isEncoder
                    ? mgr.loadEncoderDict(bytes, dictId)
                    : mgr.loadDecoderDict(bytes, dictId);
            if (!loaded) {
                // 不能谎报就绪：服务端会据此带字典压缩，而本端实际没有该字典 → 断连
                LOGGER.warn("[Zstd] 装载推送的字典失败 \u2014 reporting unavailable");
                return (byte) ZstdNegotiateStatus.VANILLA;
            }
            return (byte) ZstdNegotiateStatus.READY;
        } catch (Exception e) {
            LOGGER.warn("[Zstd] Failed to read dict \u2014 reporting unavailable", e);
            return (byte) ZstdNegotiateStatus.VANILLA;
        }
    }

    private static FriendlyByteBuf answer(int encStatus, int decStatus) {
        FriendlyByteBuf buf = new FriendlyByteBuf(Unpooled.buffer());
        buf.writeVarInt(encStatus);
        buf.writeVarInt(decStatus);
        return buf;
    }

    /** 无法参与 zstd：两个方向都按"保持原版"上报，服务端会据此回落。 */
    private static FriendlyByteBuf unavailable() {
        return answer(ZstdNegotiateStatus.VANILLA, ZstdNegotiateStatus.VANILLA);
    }
}
