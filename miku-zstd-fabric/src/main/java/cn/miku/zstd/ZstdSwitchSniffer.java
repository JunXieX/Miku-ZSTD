package cn.miku.zstd;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import mikumc.zstd.protocol.ZstdStepFrames;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 切换询问嗅探器（Fabric 客户端，帧层）：识别服务端发来的 {@code zstd:switch} 询问，
 * 切换本端管道后回发确认。
 *
 * <h2>为什么客户端需要它</h2>
 * <p>服务端在激活 zstd 后会扣住登录期的大帧（例如 LoginSuccess 的压缩帧），等本端确认
 * 之后才放行。没有它，本端只能靠"收到 SetCompression"这一本地信号切换 —— 而服务端
 * 此刻已经换成 zstd 口径，两边对切换时刻的理解不一致，就会出现原版帧与 zstd 帧交错、
 * 玩家<b>无法进入服务器</b>。</p>
 *
 * <h2>⚠️ 安装位置只能是压缩解码器之前</h2>
 * <p>本类处理的是 {@code ByteBuf}（帧）。挂到 {@code packet_handler} 之前时，它收到的
 * 是已经解码的 {@code Packet} 对象，{@code decode} 根本不会被调用；更糟的是那个位置
 * 已经过 {@code zstd_decoder}，裸的询问包会被当成 zstd 帧解析错乱。因此只在
 * {@code decompress} / {@code compression-decoder} 之前安装，找不到锚点就返回 false，
 * 由调用方稍后再试。</p>
 *
 * <h2>为什么继承 {@code MessageToMessageDecoder}</h2>
 * <p>⚠️ 不能用 {@code ByteToMessageDecoder} + {@code out.add(in.retain())}：那个基类要求
 * "往 out 里放消息就必须消费字节"，否则抛 {@code DecoderException} 且字节未消费 →
 * 下一轮重复报错，<b>入站流卡死</b>（登录界面停住）。</p>
 */
public final class ZstdSwitchSniffer extends MessageToMessageDecoder<ByteBuf> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MikuZstd.LOGGER_NAME);

    /**
     * 单帧上限：询问包是 {@code [0][0x04][varint txId][len]["zstd:switch"][magic]} ≈ 23B，
     * 超过这个尺寸的帧一律不可能是它。
     *
     * <p>⚠️ 本嗅探器是<b>常驻</b>的（服务端的询问只发一次，但确认之前它必须一直在位），
     * 所以这个上限同时是热路径开关：调大意味着更多入站帧要走
     * {@link ZstdStepFrames#asBarePacket} 的解析（其中"被压缩过的帧"还要尝试一次 zlib 解压）。</p>
     */
    private static final int MAX_FRAME_BYTES = 64;

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        if (in.readableBytes() > MAX_FRAME_BYTES) {
            out.add(in.retain());
            return;
        }
        int txId = ZstdStepFrames.readSwitchTxId(ZstdStepFrames.asBarePacket(in));
        if (txId <= 0) {
            out.add(in.retain());
            return;
        }

        // 命中：消费该帧（这不是原版协议里的包），先切换、后回确认
        ctx.pipeline().remove(this);
        LOGGER.debug("[Zstd] switch request received (txId={}) — activating and confirming", txId);
        ZstdClientActivation.activateAndConfirm(ctx.channel(), txId);
    }
}
