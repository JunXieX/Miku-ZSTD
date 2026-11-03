package cn.miku.zstd;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import mikumc.zstd.protocol.ZstdStepFrames;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 客户端下行 zstd 管道激活（幂等）。
 *
 * <p>上行始终沿用原版管线：</p>
 * <pre>
 *   encoder → compress → prepender → encrypt → socket
 * </pre>
 * <p>切换前只监听原版帧；切换确认时的管线变更同步发生在 event loop：</p>
 * <pre>
 *   switch 前：socket → decrypt → splitter → zstd_switch_sniffer → decompress → decoder
 *   switch 后：socket → decrypt → splitter → zstd_decoder → decompress → decoder
 * </pre>
 * <p>zstd_decoder 从原版 decompress 的 context 向后交付已解出的包，跳过重复解压。
 * 原版及 Krypton 的上行压缩、长度前缀、加密与 flush 处理保持原样。</p>
 *
 * <p>SetCompression 的两个 TAIL 触发点只准备原版阶段的 switch 嗅探器；negotiate 应答写出后再补一次，
 * 覆盖压缩设置早于协商到达的情况。收到 zstd:switch 后在同一 event loop 内安装连续解码器，
 * 再通过原版上行管线发送确认，服务端据此放行下行登录包。</p>
 */
public final class ZstdClientActivation {

    private static final Logger LOGGER = LoggerFactory.getLogger(MikuZstd.LOGGER_NAME);

    /** 每连接下行安装就绪标志。 */
    public static final io.netty.util.AttributeKey<Boolean> ACTIVATED =
            io.netty.util.AttributeKey.valueOf("zstd:client-activated");

    private static final String[] DECODER_ANCHORS = {"decompress", "compression-decoder"};
    private static final String[] SNIFFER_ANCHORS = {
            "zstd_decoder", "decompress", "compression-decoder", "decoder"
    };

    private ZstdClientActivation() {
    }

    /** 从任意线程准备切换监听；Zstd 解码只在收到 switch 后启用。 */
    public static void activate(Channel channel) {
        prepareForSwitch(channel);
    }

    /** 初始阈值关闭仍用原版；已推进的连续流关闭连接，不丢半包后另装解码器。 */
    public static void onCompressionThreshold(Channel channel, int threshold) {
        if (channel == null) return;
        if (threshold < 0) {
            if (channel.eventLoop().inEventLoop()) {
                deactivateQuietly(channel);
            } else {
                channel.eventLoop().execute(() -> deactivateQuietly(channel));
            }
            return;
        }
        prepareForSwitch(channel);
    }

    private static void deactivateQuietly(Channel channel) {
        if (!channel.isActive()) return;
        var handler = channel.pipeline().get("zstd_decoder");
        boolean started = handler instanceof ZstdBatchDecoder decoder
                ? decoder.hasStartedStream() : Boolean.TRUE.equals(channel.attr(ACTIVATED).get());
        boolean zstdCommitted = channel.attr(ZstdChannelManager.ZSTD_STATE).get()
                == ZstdChannelManager.TransportState.ZSTD_ACTIVE;
        if (started || zstdCommitted) {
            channel.attr(ACTIVATED).set(Boolean.FALSE);
            LOGGER.warn("[Zstd] closing connection — compression disabled after Zstd switch");
            channel.close();
            return;
        }
        boolean changed = removeOwnHandlers(channel.pipeline());
        channel.attr(ACTIVATED).set(Boolean.FALSE);
        // 尚未消费任何流字节时才允许保留协商状态，等待后续 SetCompression。
        if (changed) {
            LOGGER.info("[Zstd] 压缩被对端禁用（threshold<0）——客户端下行退回原版; pipeline={}",
                    channel.pipeline().names());
        }
    }

    /** 在 SetCompression 回调内立即准备切换监听，不提前启用连续流解码。 */
    public static void prepareForSwitch(Channel channel) {
        if (channel == null) return;
        if (channel.eventLoop().inEventLoop()) {
            prepareNow(channel);
        } else {
            channel.eventLoop().execute(() -> prepareNow(channel));
        }
    }

    private static void prepareNow(Channel channel) {
        if (!channel.isActive()) return;
        if (deactivateIfStale(channel)) return;
        ZstdChannelManager.TransportState state = channel.attr(ZstdChannelManager.ZSTD_STATE).get();
        if (state == ZstdChannelManager.TransportState.ZSTD_ACTIVE
                && Boolean.TRUE.equals(channel.attr(ACTIVATED).get())) {
            // switch 已完成后，重复 SetCompression 只能复核现有管线，不能重插嗅探器。
            if (!handlersPositionedCorrectly(channel.pipeline())) {
                channel.attr(ACTIVATED).set(Boolean.FALSE);
                LOGGER.warn("[Zstd] closing continuous stream — downstream handlers missing or misplaced; pipeline={}",
                        channel.pipeline().names());
                channel.close();
            }
            return;
        }
        if (state != ZstdChannelManager.TransportState.NEGOTIATING) return;
        // SetCompression 只建立原版解压锚点；在 switch 到达前不能安装 zstd_decoder，
        // 否则中继的缓冲/分段可能让仍属原版阶段的帧先进入连续流解码器。
        installSwitchSniffer(channel.pipeline());
    }

    /** negotiate 应答写出后的兜底：只准备 switch 嗅探器，不提前切换下行解码。 */
    public static void activateAfterQueryResponseSent(Channel channel) {
        if (channel == null) return;
        if (channel.eventLoop().inEventLoop()) {
            prepareNow(channel);
        } else {
            channel.eventLoop().execute(() -> prepareNow(channel));
        }
    }

    /** 在 switch 帧被消费的同一 event loop 内安装下行解码器，再发送确认。 */
    public static void activateAndConfirm(Channel channel, int txId) {
        if (channel == null) return;
        if (channel.eventLoop().inEventLoop()) {
            activateAndConfirmInEventLoop(channel, txId);
        } else {
            channel.eventLoop().execute(() -> activateAndConfirmInEventLoop(channel, txId));
        }
    }

    private static void activateAndConfirmInEventLoop(Channel channel, int txId) {
        // switch 嗅探器尚在当前decode回调内；先同步插入decoder并提交状态，再移除嗅探器、发确认。
        boolean ready = activateInEventLoop(channel);
        if (!ready) {
            // 服务端收到 false 仍会按“已提交 zstd”放行，不能在本端未就绪时发送假回落。
            channel.close();
            return;
        }
        if (channel.pipeline().get("zstd_switch_sniffer") != null) {
            channel.pipeline().remove("zstd_switch_sniffer");
        }
        sendSwitchConfirm(channel, txId, true);
    }

    /** 本轮协商回落原版时，清理上一轮可能残留的下行处理器。 */
    public static void onRoundDeclined(Channel channel) {
        if (channel == null) return;
        if (channel.eventLoop().inEventLoop()) {
            deactivateIfStale(channel);
        } else {
            channel.eventLoop().execute(() -> deactivateIfStale(channel));
        }
    }

    private static boolean deactivateIfStale(Channel channel) {
        if (channel.attr(ZstdChannelManager.ZSTD_STATE).get()
                != ZstdChannelManager.TransportState.PLAIN) {
            return false;
        }
        boolean changed = removeOwnHandlers(channel.pipeline());
        channel.attr(ACTIVATED).set(Boolean.FALSE);
        if (changed) {
            LOGGER.info("[Zstd] 已拆除上一轮下行 zstd 处理器，当前管线恢复原版");
        }
        return true;
    }

    /** 只管理本模组下行处理器，原版压缩、长度前缀与加密处理器保持原样。 */
    private static boolean removeOwnHandlers(ChannelPipeline p) {
        boolean changed = false;
        for (String name : new String[]{"zstd_decoder", "zstd_switch_sniffer"}) {
            if (p.get(name) != null) {
                p.remove(name);
                changed = true;
            }
        }
        return changed;
    }

    /**
     * 写普通 serverbound 查询应答，不手工添加压缩标记或长度前缀。
     * 从包层 encoder 的 context 写出跳过 Packet → ByteBuf 编码，后续原版 compress
     * 自行处理阈值，再由 prepender 与 encrypt 正常处理；没有绕过压缩层的兜底位置。
     */
    private static void sendSwitchConfirm(Channel channel, int txId, boolean accepted) {
        ChannelHandlerContext encoder = channel.pipeline().context("encoder");
        if (encoder == null) {
            LOGGER.error("[Zstd] switch response not sent — packet encoder anchor missing; pipeline={}",
                    channel.pipeline().names());
            return;
        }
        ByteBuf packet = channel.alloc().buffer();
        try {
            ZstdStepFrames.writeConfirm(packet, txId, accepted);
            encoder.writeAndFlush(packet);
            packet = null; // 所有权已交给原版上行管线。
            LOGGER.debug("[Zstd] switch {} sent through vanilla upstream (txId={})",
                    accepted ? "confirmation" : "rejection", txId);
        } catch (Throwable t) {
            LOGGER.debug("[Zstd] failed to send switch response: {}", t.toString());
        } finally {
            if (packet != null && packet.refCnt() > 0) packet.release();
        }
    }

    private static boolean activateInEventLoop(Channel channel) {
        if (!channel.isActive() || deactivateIfStale(channel)) return false;
        ChannelPipeline p = channel.pipeline();
        if (Boolean.TRUE.equals(channel.attr(ACTIVATED).get())) {
            if (handlersPositionedCorrectly(p)) return true;
            // 旧解码器可能持有半包；重建会丢失边界，持续流也没有可重新同步的帧头。
            channel.attr(ACTIVATED).set(Boolean.FALSE);
            LOGGER.warn("[Zstd] closing continuous stream — downstream handlers missing or misplaced; pipeline={}",
                    p.names());
            channel.close();
            return false;
        }

        ZstdChannelManager.TransportState state = channel.attr(ZstdChannelManager.ZSTD_STATE).get();
        if (state != ZstdChannelManager.TransportState.NEGOTIATING) {
            LOGGER.warn("[Zstd] switch received outside a negotiated state: {}", state);
            return false;
        }
        if (firstPresent(p, DECODER_ANCHORS) == null) {
            LOGGER.error("[Zstd] switch received before downstream compression handler was installed; pipeline={}",
                    p.names());
            return false;
        }

        ZstdChannelManager manager = channel.attr(ZstdChannelManager.KEY).get();
        if (manager == null) {
            manager = new ZstdChannelManager();
            channel.attr(ZstdChannelManager.KEY).set(manager);
            final ZstdChannelManager toClose = manager;
            channel.closeFuture().addListener(f -> toClose.close());
        }
        if (!installDecoder(p)) {
            LOGGER.error("[Zstd] activation aborted — downstream decoder insert failed; pipeline={}", p.names());
            return false;
        }
        if (!handlersPositionedCorrectly(p)) {
            LOGGER.warn("[Zstd] 下行激活校验失败——解码器缺失或错位; pipeline={}", p.names());
            return false;
        }

        channel.attr(ZstdChannelManager.ZSTD_STATE).set(ZstdChannelManager.TransportState.ZSTD_ACTIVE);
        channel.attr(ACTIVATED).set(Boolean.TRUE);
        LOGGER.info("[Zstd] Client downstream zstd activated; upstream remains vanilla; pipeline={}", p.names());
        return true;
    }

    /** switch 必须先于 zstd_decoder 截获，否则原版控制帧会被误作 Zstd 流。 */
    private static void installSwitchSniffer(ChannelPipeline p) {
        String anchor = firstPresent(p, SNIFFER_ANCHORS);
        if (anchor == null) return;
        if (p.get("zstd_switch_sniffer") != null) {
            if (isBeforeInPipeline(p, "zstd_switch_sniffer", anchor)) return;
            p.remove("zstd_switch_sniffer");
            LOGGER.debug("[Zstd] switch sniffer was on the wrong side of {} — reinserting", anchor);
        }
        p.addBefore(anchor, "zstd_switch_sniffer", new ZstdSwitchSniffer());
    }

    /** 仅安装就绪前允许调整位置；重复激活保留既有解码器与半包状态。 */
    private static boolean installDecoder(ChannelPipeline p) {
        String anchor = firstPresent(p, DECODER_ANCHORS);
        if (anchor == null) return false;
        if (p.get("zstd_decoder") != null) {
            if (isBeforeInPipeline(p, "zstd_decoder", anchor)) return true;
            p.remove("zstd_decoder");
            LOGGER.debug("[Zstd] zstd_decoder was on the wrong side of {} — reinserting", anchor);
        }
        p.addBefore(anchor, "zstd_decoder", new ZstdBatchDecoder());
        return true;
    }

    /** 解码器须在原版 decompress 上游；嗅探器收到 switch 后会自行移除。 */
    private static boolean handlersPositionedCorrectly(ChannelPipeline p) {
        String anchor = firstPresent(p, DECODER_ANCHORS);
        return anchor != null
                && isBeforeInPipeline(p, "zstd_decoder", anchor)
                && (p.get("zstd_switch_sniffer") == null
                    || isBeforeInPipeline(p, "zstd_switch_sniffer", "zstd_decoder"));
    }

    private static boolean isBeforeInPipeline(ChannelPipeline p, String name, String anchor) {
        java.util.List<String> names = p.names();
        int anchorIndex = names.indexOf(anchor);
        int nameIndex = names.indexOf(name);
        return anchorIndex >= 0 && nameIndex >= 0 && nameIndex < anchorIndex;
    }

    private static String firstPresent(ChannelPipeline p, String[] candidates) {
        for (String name : candidates) {
            if (p.get(name) != null) return name;
        }
        return null;
    }
}
