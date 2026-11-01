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
 * <p>本模组只安装下行解码器与切换嗅探器，不替换原版压缩处理器：</p>
 * <pre>
 *   socket → decrypt → splitter → zstd_switch_sniffer → zstd_decoder → decompress → decoder
 * </pre>
 * <p>zstd_decoder 从原版 decompress 的 context 向后交付已解出的包，跳过重复解压。
 * 原版及 Krypton 的上行压缩、长度前缀、加密与 flush 处理保持原样。</p>
 *
 * <p>SetCompression 的两个 TAIL 触发点负责安装与位置复核；negotiate 应答写出后再补一次尝试，
 * 覆盖压缩设置早于协商到达的情况。收到 zstd:switch 后必须先确认下行处理器已安装，
 * 再通过原版上行管线发送确认，服务端据此放行下行登录包。</p>
 */
public final class ZstdClientActivation {

    private static final Logger LOGGER = LoggerFactory.getLogger(MikuZstd.LOGGER_NAME);

    /** 每连接下行安装就绪标志。 */
    public static final io.netty.util.AttributeKey<Boolean> ACTIVATED =
            io.netty.util.AttributeKey.valueOf("zstd:client-activated");

    private static final String[] DECODER_ANCHORS = {"decompress", "compression-decoder"};
    private static final String[] SNIFFER_ANCHORS = {"zstd_decoder", "decompress", "compression-decoder"};

    private ZstdClientActivation() {
    }

    /** 从任意线程请求激活；管线修改在 event loop 执行。 */
    public static void activate(Channel channel) {
        if (channel == null) return;
        channel.eventLoop().execute(() -> activateInEventLoop(channel, false));
    }

    /** 阈值关闭时仅摘掉下行处理器，原版上下行处理器由 Connection 自行管理。 */
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
        boolean changed = removeOwnHandlers(channel.pipeline());
        channel.attr(ACTIVATED).set(Boolean.FALSE);
        // 保留协商状态，允许同一连接再次收到 SetCompression 时重装下行处理器。
        if (changed) {
            LOGGER.info("[Zstd] 压缩被对端禁用（threshold<0）——客户端下行退回原版; pipeline={}",
                    channel.pipeline().names());
        }
    }

    /** 在 SetCompression 回调内立即安装，避免后续 switch 帧先于排队任务到达。 */
    public static void prepareForSwitch(Channel channel) {
        if (channel == null) return;
        if (channel.eventLoop().inEventLoop()) {
            prepareNow(channel);
        } else {
            channel.eventLoop().execute(() -> prepareNow(channel));
        }
    }

    private static void prepareNow(Channel channel) {
        activateInEventLoop(channel, false);
        if (Boolean.TRUE.equals(channel.attr(ACTIVATED).get())) {
            installSwitchSniffer(channel.pipeline());
        }
    }

    /** negotiate 应答写出后的兜底；此时尚无压缩锚点属于正常登录时序。 */
    public static void activateAfterQueryResponseSent(Channel channel) {
        if (channel == null) return;
        channel.eventLoop().execute(() -> activateInEventLoop(channel, true));
    }

    /** 先安装并复核下行解码器，再确认；服务端收到确认后才放行 zstd 登录包。 */
    public static void activateAndConfirm(Channel channel, int txId) {
        if (channel == null) return;
        channel.eventLoop().execute(() -> {
            activateInEventLoop(channel, false);
            boolean ready = Boolean.TRUE.equals(channel.attr(ACTIVATED).get())
                    && handlersPositionedCorrectly(channel.pipeline());
            sendSwitchConfirm(channel, txId, ready);
        });
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

    private static void activateInEventLoop(Channel channel, boolean quiet) {
        if (!channel.isActive()) return;
        ChannelPipeline p = channel.pipeline();
        if (deactivateIfStale(channel)) return;

        if (Boolean.TRUE.equals(channel.attr(ACTIVATED).get())) {
            if (handlersPositionedCorrectly(p)) return;
            channel.attr(ACTIVATED).set(Boolean.FALSE);
            LOGGER.debug("[Zstd] reactivating — downstream handlers missing or misplaced; pipeline={}",
                    p.names());
        }

        ZstdChannelManager.TransportState state = channel.attr(ZstdChannelManager.ZSTD_STATE).get();
        // 原版重建 decompress 后，已协商连接也必须允许位置复核与重装。
        if (state != ZstdChannelManager.TransportState.NEGOTIATING
                && state != ZstdChannelManager.TransportState.ZSTD_ACTIVE) {
            LOGGER.debug("[Zstd] client activation deferred: state={} (negotiate not received yet)",
                    state == null ? "NONE" : state);
            return;
        }

        if (firstPresent(p, DECODER_ANCHORS) == null) {
            if (quiet) {
                LOGGER.debug("[Zstd] activation deferred — downstream compression handler not installed yet");
            } else {
                LOGGER.error("[Zstd] activation aborted — downstream anchor missing; pipeline={}", p.names());
            }
            return;
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
            return;
        }
        // switch 询问绕过服务端 zstd 编码器，不得进入本端滚动历史。
        installSwitchSniffer(p);
        if (p.get("zstd_switch_sniffer") == null || !handlersPositionedCorrectly(p)) {
            LOGGER.warn("[Zstd] 下行激活校验失败——解码器或切换嗅探器缺失/错位; pipeline={}", p.names());
            return;
        }

        channel.attr(ZstdChannelManager.ZSTD_STATE).set(ZstdChannelManager.TransportState.ZSTD_ACTIVE);
        channel.attr(ACTIVATED).set(Boolean.TRUE);
        LOGGER.info("[Zstd] Client downstream zstd activated; upstream remains vanilla; pipeline={}", p.names());
    }

    /** switch 必须先于 zstd_decoder 截获，否则会污染下行滚动历史。 */
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

    /** 只在确实错位时重插；正常激活保留解码器及其滚动历史。 */
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
