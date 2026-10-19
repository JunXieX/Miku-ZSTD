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
        channel.eventLoop().execute(() -> activateInEventLoop(channel, false, 0));
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
        channel.eventLoop().execute(() -> activateInEventLoop(channel, true, 0));
    }

    /**
     * 服务端发来 {@code zstd:switch} 后调用：<b>先</b>把入站解码器换成 zstd，<b>再</b>回发确认。
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
            activateInEventLoop(channel, false, txId);
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
     * 发送切换应答。
     *
     * <p>⚠️ 必须从 {@code zstd_encoder} 的 ctx 写出<b>手工拼好的完整帧</b>，不能走
     * {@code encoder} 再让编码器处理：编码器有 1ms 批处理窗口，确认包会被与后续任意包
     * 合并成多包帧；而服务端帧层嗅探器只认"单包直存帧"，合帧后识别失败 →
     * 确认丢失 → 服务端等满超时 → 双方各猜一半 → 进不去服务器。</p>
     *
     * <p>客户端管线有独立 prepender，所以手工帧只需 {@code [rawSize=0][pktLen][packet]}，
     * 外层长度由它补齐。</p>
     */
    private static void sendSwitchConfirm(Channel channel, int txId, boolean accepted) {
        io.netty.channel.ChannelHandlerContext zstdCtx = channel.pipeline().context("zstd_encoder");
        io.netty.buffer.ByteBuf packet = channel.alloc().buffer();
        try {
            // ⚠️ 必须是 serverbound 的查询应答（0x02），而不是 clientbound 的询问（0x04）：
            // 服务端会把 0x04 解析成别的包并直接断连。
            mikumc.zstd.protocol.ZstdStepFrames.writeConfirm(packet, txId, accepted);

            if (zstdCtx != null) {
                // 已切到 zstd：从编码器之后写出手工直存帧（避开 1ms 批处理窗口，
                // 否则确认可能与其它包合帧，服务端帧层嗅探器只认单包帧）。
                io.netty.buffer.ByteBuf frame = channel.alloc().buffer();
                try {
                    int pktLen = packet.readableBytes();
                    mikumc.zstd.protocol.ZstdVarInts.write(frame, 0);
                    mikumc.zstd.protocol.ZstdVarInts.write(frame, pktLen);
                    frame.writeBytes(packet);
                    zstdCtx.writeAndFlush(frame);
                } catch (Throwable t) {
                    if (frame.refCnt() > 0) frame.release();
                    throw t;
                }
            } else {
                // 未切到 zstd：仍由原版压缩编码器负责编码（它自己处理阈值与压缩）。
                io.netty.channel.ChannelHandlerContext plain =
                        channel.pipeline().context("compress");
                if (plain == null) plain = channel.pipeline().context("encoder");
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

    private static void activateInEventLoop(Channel channel, boolean quiet, int confirmTxId) {
        if (!channel.isActive()) return;
        ChannelPipeline p = channel.pipeline();

        // 幂等清理：zstd 已在位时，原版 SetCompression 可能再次被处理而装回
        // compress/decompress（ViaFabricPlus 协议切换会重放登录流程），与 zstd 处理器
        // 并存会导致双重压缩、断连。必须放在 ACTIVATED 早退之前——否则"已激活"这条
        // 路径根本不会执行清理。
        removeStaleVanillaHandlers(p);

        // ⚠️ 激活标志只在"zstd 处理器仍在位"时才算数。
        // 曾直接把 ACTIVATED 当作终态早退：一旦这条 Channel 上重放登录流程
        // （ViaFabricPlus 协议切换、或同一条连接重新协商），原版 compress/decompress
        // 会被重新装回并替换掉 zstd 处理器，而 ACTIVATED 仍为 true →
        // 后续所有激活入口都提前返回，zstd 再也不生效（且日志上只有一条 debug）。
        if (Boolean.TRUE.equals(channel.attr(ACTIVATED).get())) {
            if (p.get("zstd_encoder") != null && p.get("zstd_decoder") != null) {
                return; // 确实已激活且处理器在位：无需重复安装
            }
            // 处理器已被原版重新覆盖：清掉标志，让下面的安装流程重新接管
            channel.attr(ACTIVATED).set(Boolean.FALSE);
            LOGGER.debug("[Zstd] reactivating — zstd handlers were replaced by vanilla again; pipeline={}",
                    p.names());
        }

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

        // ⚠️ 先查两端锚点是否都在，再动手替换（对齐服务端两侧）：
        // 否则可能出现"编码器替换成功、解码器还没装好"——本端以 zstd 发帧、却仍按原版
        // 解帧，与服务端形成必断连组合，且两侧日志都显示"activated"，极难定位。
        boolean encReady = p.get("zstd_encoder") != null || p.get("compress") != null
                || p.get("compression-encoder") != null;
        boolean decReady = p.get("zstd_decoder") != null || p.get("decompress") != null
                || p.get("compression-decoder") != null;
        if (!encReady || !decReady) {
            if (quiet) {
                // 字典比 SetCompression 先到：压缩处理器还没装上，属正常时序。
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
            // 锚点已确认齐备却仍替换失败：属异常（例如处理器被别的模组动态移除）。
            // 绝不置 ACTIVATED —— 让另一条触发路径仍有补救机会。
            LOGGER.error("[Zstd] activation aborted — handler replacement failed despite anchors "
                    + "(encoder={}, decoder={}); pipeline={}", encOk, decOk, p.names());
            return;
        }

        // 已切到 zstd：此时 decompress 已被替换成 zstd_decoder，锚点只剩它。
        // 服务端的切换询问（zstd:switch）以裸帧到达，必须在 zstd_decoder 之前截获，
        // 否则首个 varint（0x04）会被当成 rawSize 解析 → 直接断连。
        installSwitchSniffer(p);

        channel.attr(ZstdChannelManager.ZSTD_STATE).set(ZstdChannelManager.TransportState.ZSTD_ACTIVE);
        channel.attr(ACTIVATED).set(Boolean.TRUE);

        LOGGER.info("[Zstd] Client zstd transport activated; pipeline={}", p.names());
    }

    /** 把切换询问嗅探器装到 {@code zstd_decoder} 之前（幂等）。 */
    private static void installSwitchSniffer(ChannelPipeline p) {
        if (p.get("zstd_switch_sniffer") != null) return;
        if (p.get("zstd_decoder") != null) {
            p.addBefore("zstd_decoder", "zstd_switch_sniffer",
                    (ChannelHandler) new ZstdSwitchSniffer());
            return;
        }
        if (p.get("decompress") != null) {
            p.addBefore("decompress", "zstd_switch_sniffer",
                    (ChannelHandler) new ZstdSwitchSniffer());
            return;
        }
        LOGGER.debug("[Zstd] switch sniffer not installed (no anchor); pipeline={}", p.names());
    }

    /**
     * 移除与已装 zstd 处理器并存的原版压缩处理器（幂等）。
     *
     * <p>只在对应方向的 zstd 处理器已在位时才动手：这样第一次安装期间不会误删
     * 待替换的原版 handler。二次进入（重放 SetCompression）时 zstd 已在位，
     * 原版 handler 便是残留，必须清掉。</p>
     */
    private static void removeStaleVanillaHandlers(ChannelPipeline p) {
        if (p.get("zstd_encoder") != null) {
            removeIfPresent(p, "compress", "zstd_encoder");
            removeIfPresent(p, "compression-encoder", "zstd_encoder");
        }
        if (p.get("zstd_decoder") != null) {
            removeIfPresent(p, "decompress", "zstd_decoder");
            removeIfPresent(p, "compression-decoder", "zstd_decoder");
        }
    }

    private static void removeIfPresent(ChannelPipeline p, String name, String zstdName) {
        if (p.get(name) != null) {
            p.remove(name);
            LOGGER.debug("[Zstd] Removed stale {} handler installed after {}", name, zstdName);
        }
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
