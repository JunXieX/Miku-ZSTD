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
 *   <li>{@link #activateAfterQueryResponseSent}：{@code zstd:dict} 应答写出之后。
 *       前两条都跑在 SetCompression 处理期间，而"等字典"的客户端那一刻还停在 PLAIN 被跳过，
 *       没有这一条就再也没人激活（详见该方法的注释）</li>
 * </ol>
 *
 * <p>路径 2 存在的意义是<b>接管必须晚于所有压缩处理器的安装</b>：服务端下发
 * SetCompression 后，压缩处理器可能在 setupCompression 内部或之后才被装上，
 * 从而覆盖路径 1 的结果。路径 2 在 handleCompression 整体返回后（本轮所有注入
 * 都已完成）再通过 eventLoop 排队执行，因此总是最后生效。</p>
 *
 * <p>⚠️ <b>不兼容 Krypton</b>（客户端侧压缩优化模组）：本模组已内置客户端压缩优化，
 * 两者会争抢同一条压缩管线，同时安装时压缩行为会互相覆盖。<br>
 * 注意上面「晚于后置安装」这条保证<b>不是</b>为了与 Krypton 共存——它只是确保 zstd
 * 相对于本轮登录压缩流程中安装的处理器最终生效。</p>
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
     * 用 zstd 编解码器替换管线中的压缩处理器。幂等，可从任意线程调用
     * （内部会切到 eventLoop 执行）。
     */
    public static void activate(Channel channel) {
        if (channel == null) return;
        channel.eventLoop().execute(() -> activateInEventLoop(channel, false));
    }

    /**
     * 登录查询应答<b>写出之后</b>调用（当前唯一调用点是 {@code zstd:dict}）：
     * 字典装载成功、状态刚变为 {@link ZstdChannelManager.TransportState#NEGOTIATING} 时补一次激活尝试。
     *
     * <h2>为什么必须有这个入口</h2>
     * <p>原有的两个触发点（{@code setupCompression} 与 {@code handleCompression} 的 TAIL）都在
     * <b>SetCompression 处理期间</b>执行。而"需要字典"的客户端在那一时刻还停在 PLAIN
     * （见 {@code ZstdLoginNetworking.handleNegotiate}），于是两条路径都只记一句 debug 就跳过；
     * 等 {@code zstd:dict} 到达、状态改成 NEGOTIATING 时，已经没有任何触发点了——
     * 客户端停在 NEGOTIATING 永不替换管线，而服务端收到本端应答后已切换到 zstd 发帧，
     * 形成"服务端 zstd / 客户端 vanilla"的必断连组合。</p>
     *
     * <h2>⚠️ 为什么必须"应答写出之后"才能调</h2>
     * <p>本方法会真的替换出站管道。若在应答写出<b>之前</b>替换，这条应答就会以 zstd 帧
     * （{@code [rawSize][varint pktLen][pkt]}）发出，而服务端此刻既没有 zstd 解码器，
     * 帧层应答嗅探器也只认裸帧 {@code [0x02][txId]...}——应答会被丢弃或按未知包 id 解析，
     * 协商直接失败。调用点因此挂在"应答包写出"的回调上（Fabric 的 {@code callbacksConsumer}）。</p>
     *
     * <h2>与 {@link #activate} 的唯一区别</h2>
     * <p>管道里还没有压缩处理器时（字典比 SetCompression 先到）只记 debug、不报 ERROR：
     * 那是正常时序，真正需要报警的尝试由上面那两个 TAIL 触发点负责（它们必然在 SetCompression 时执行）。</p>
     */
    public static void activateAfterQueryResponseSent(Channel channel) {
        if (channel == null) return;
        channel.eventLoop().execute(() -> activateInEventLoop(channel, true));
    }

    private static void activateInEventLoop(Channel channel, boolean quiet) {
        if (!channel.isActive()) return;
        if (Boolean.TRUE.equals(channel.attr(ACTIVATED).get())) return;

        ZstdChannelManager.TransportState state = channel.attr(ZstdChannelManager.ZSTD_STATE).get();
        if (state != ZstdChannelManager.TransportState.NEGOTIATING) {
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
        ChannelPipeline p = channel.pipeline();

        // 清理后装上的压缩处理器：zstd_encoder 已在位时，移除它装回来的 compress
        if (p.get("zstd_encoder") != null && p.get("compress") != null) {
            p.remove("compress");
            LOGGER.debug("[Zstd] Removed stale compress handler installed after zstd_encoder");
        }

        boolean encOk = installEncoder(p);
        boolean decOk = installDecoder(p);
        if (!encOk || !decOk) {
            if (quiet) {
                // 字典比 SetCompression 先到：压缩处理器还没装上，属正常时序。
                // 真正决定成败的那次尝试在 setupCompression / handleCompression 的 TAIL 上，那里会报错。
                LOGGER.debug("[Zstd] activation deferred — compression handlers not installed yet "
                        + "(encoder={}, decoder={})", encOk, decOk);
            } else {
                LOGGER.error("[Zstd] activation aborted — pipeline anchors missing "
                        + "(encoder={}, decoder={}); pipeline={}", encOk, decOk, p.names());
            }
            return;
        }

        channel.attr(ZstdChannelManager.ZSTD_STATE).set(ZstdChannelManager.TransportState.ZSTD_ACTIVE);
        channel.attr(ACTIVATED).set(Boolean.TRUE);

        LOGGER.info("[Zstd] Client zstd transport activated; pipeline={}", p.names());
    }

    private static boolean installEncoder(ChannelPipeline p) {
        if (p.get("zstd_encoder") != null) return true;
        if (p.get("compress") != null) {
            p.replace("compress", "zstd_encoder", (ChannelHandler) new ZstdBatchEncoder());
            return true;
        }
        if (p.get("compression-encoder") != null) {
            p.replace("compression-encoder", "zstd_encoder", (ChannelHandler) new ZstdBatchEncoder());
            return true;
        }
        return false;
    }

    private static boolean installDecoder(ChannelPipeline p) {
        if (p.get("zstd_decoder") != null) return true;
        if (p.get("decompress") != null) {
            p.replace("decompress", "zstd_decoder", (ChannelHandler) new ZstdBatchDecoder());
            return true;
        }
        if (p.get("compression-decoder") != null) {
            p.replace("compression-decoder", "zstd_decoder", (ChannelHandler) new ZstdBatchDecoder());
            return true;
        }
        return false;
    }
}
