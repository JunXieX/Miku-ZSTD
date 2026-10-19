package mikumc.zstd.paper;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import mikumc.zstd.protocol.ZstdStepFrames;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 切换确认嗅探器（Paper 服务端，帧层）：识别客户端回发的 {@code zstd:switch} 确认。
 *
 * <p>⚠️ 为什么不能像"协商应答"那样在包层识别：</p>
 * <ul>
 *   <li>包层在 {@code decompress} 之后，那时确认包已经被原版解码器按"未知包 id"
 *       处理掉（甚至直接断连）；</li>
 *   <li>确认包可能被原版压缩（阈值很小时）或直存，只有帧层能同时兼容三种外层形态。</li>
 * </ul>
 * <p>因此在压缩解码器之前识别、消费并放行闸门。这与 Velocity 端是同一套语义，
 * 只是两端管线不同（Velocity 用 {@code ZstdSwitchConfirmSniffer}）。</p>
 *
 * <p>继承 {@code MessageToMessageDecoder} 而不是 {@code ByteToMessageDecoder}：后者要求
 * "往 out 里放消息就必须消费字节"，否则会反复抛 {@code DecoderException} 让入站流卡死。</p>
 */
public final class ZstdPaperSwitchConfirmSniffer extends MessageToMessageDecoder<ByteBuf> {

    private static final Logger LOGGER = LoggerFactory.getLogger("miku-zstd");

    /**
     * 单帧上限：确认包是 {@code [0][0x02][varint txId][bool][magic]} ≈ 12B（被原版压缩过也只有二三十字节），
     * 超过这个尺寸的帧一律不可能是它。
     *
     * <p>⚠️ 本嗅探器是<b>常驻</b>的（确认到达后仍要留在管线里等极晚到达的重传），所以这个上限
     * 同时是热路径开关：调大意味着更多入站帧要走 {@link ZstdStepFrames#asBarePacket} 的解析。</p>
     */
    private static final int MAX_FRAME_BYTES = 64;

    /** 管线名（幂等安装用）。 */
    public static final String NAME = "miku-zstd-switch-confirm";

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        if (in.readableBytes() > MAX_FRAME_BYTES) {
            out.add(in.retain());
            return;
        }
        byte[] bare = ZstdStepFrames.asBarePacket(in);
        int txId = ZstdStepFrames.readSwitchTxId(bare);
        boolean accepted = true;
        if (txId <= 0) {
            // 不带魔数的应答：客户端没能（或不会）确认切换。**不能**当拒绝处理 ——
            // 旧版客户端在收到询问后会按原版规则回一个同样字节的空应答，而它其实是 zstd。
            // 服务端已提交到 zstd，所以只按"未确认"放行扣住的包（见 ZstdSwitchGate 类注释）。
            txId = ZstdStepFrames.readSwitchRejectTxId(bare);
            accepted = false;
        }
        if (txId <= 0) {
            out.add(in.retain());
            return;
        }

        ZstdPaperChannelManager mgr = ctx.channel().attr(ZstdPaperChannelManager.KEY).get();
        boolean handled = mgr != null
                && (accepted ? mgr.markSwitchConfirmed(txId) : mgr.releaseUnconfirmed(txId));
        if (!handled) {
            // ⚠️ txId 与本端询问不符：这不是本次询问的应答，多半只是"恰好长得像"的普通小包。
            // 此时**绝不能摘掉自己** —— 摘掉之后真确认到达时就没人认了，服务端只能等满
            // 5 秒超时再放行；而且原帧必须原样下传（它是正常协议流量，吞掉即协议错乱）。
            out.add(in.retain());
            return;
        }
        ctx.pipeline().remove(this);
        if (accepted) {
            LOGGER.debug("[Zstd] switch confirmation received (txId={}) — releasing gate", txId);
        } else {
            LOGGER.warn("[Zstd] switch NOT confirmed by client (txId={}) — releasing held packets anyway",
                    txId);
        }
    }
}
