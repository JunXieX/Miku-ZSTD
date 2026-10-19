package mikumc.zstd;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import mikumc.zstd.protocol.ZstdStepFrames;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 切换询问嗅探器（Velocity 服务端，帧层）：识别客户端回发的 {@code zstd:switch} 确认。
 *
 * <p>客户端在把入站解码器换成 zstd 之后、且<b>只</b>在那之后才回发它；服务端收到 =
 * "客户端已切换完毕，可以放行扣住的包了"。</p>
 *
 * <h2>为什么必须在帧层（压缩解码器之前）</h2>
 * <p>确认包可能被原版压缩（阈值很小时）或直存，放在帧层可以直接按三种外层形态识别，
 * 不必依赖原版解码器的行为。</p>
 *
 * <h2>为什么继承 {@code MessageToMessageDecoder}</h2>
 * <p>⚠️ 不能用 {@code ByteToMessageDecoder} + {@code out.add(in.retain())} 透传：那个基类
 * 要求"往 out 里放消息就必须消费 cumulation 的字节"，否则会抛
 * {@code DecoderException("... did not read anything but decoded a message")}，
 * 而且字节没被消费 → 下一轮继续报同一个异常，<b>入站流直接卡死</b>（表现为登录卡住）。
 * 本类只做"识别 + 放行/丢弃"，正是 {@code MessageToMessageDecoder} 的语义。</p>
 */
public final class ZstdSwitchConfirmSniffer extends MessageToMessageDecoder<ByteBuf> {

    private static final Logger LOGGER = LoggerFactory.getLogger("miku-zstd");

    /** 单帧上限：确认包极小，超过这个尺寸的帧一律不可能是它。 */
    private static final int MAX_FRAME_BYTES = 512;

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
            // 拒绝包（success=false，无魔数）：客户端无法切换，服务端必须回滚
            txId = ZstdStepFrames.readSwitchRejectTxId(bare);
            accepted = false;
        }
        if (txId <= 0) {
            out.add(in.retain());
            return;
        }

        ctx.pipeline().remove(this);
        ZstdChannelManager mgr = ctx.channel().attr(ZstdChannelManager.KEY).get();
        if (mgr != null) {
            if (accepted) {
                LOGGER.debug("[Zstd] switch confirmation received (txId={}) — releasing gate", txId);
                mgr.confirmSwitch(txId);
            } else {
                LOGGER.debug("[Zstd] switch REJECTED by client (txId={}) — rolling back", txId);
                mgr.rejectSwitch(txId);
            }
        }
    }
}
