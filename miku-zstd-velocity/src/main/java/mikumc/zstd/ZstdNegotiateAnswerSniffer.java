package mikumc.zstd;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * negotiate 应答嗅探器（帧层，位于 frame-decoder 之后、压缩解码器之前）。
 *
 * <h2>为什么必须在这一层捕获</h2>
 * <p>Velocity 处理 login start 后会<b>立即</b>启用压缩（安装 compression-decoder 并写出
 * SetCompressionPacket），而我们的 negotiate 查询只能在第一个出站包时发出——离线模式
 * 下这个"第一个出站包"恰恰就是 SetCompression 本身。客户端收到查询后马上回包（裸帧，
 * 因为它此时还没收到 SetCompression），但这个应答要经过一个 RTT 才到达服务端，
 * <b>那时服务端的原版压缩解码器已经装好</b>，会把裸帧应答当作压缩帧解析并丢弃。</p>
 *
 * <p>实测（本地 Velocity 4.2 + 协议 v2 模拟客户端）：</p>
 * <pre>
 *   应答按裸帧发送   -> 服务端 spy 完全看不到该包，markDictResponse 永不触发，zstd 静默不激活
 *   应答按压缩帧发送 -> "Negotiate response received: enc=0 dec=0 dictConfirmed=true"
 * </pre>
 *
 * <p>在线模式（用户生产环境）之所以正常，是因为 EncryptionRequest 带来一个额外 RTT，
 * 应答恰好赶在 SetCompression 之前到达；<b>离线模式代理则完全失效</b>——这是本次
 * 端到端测试才暴露出来的缺陷。</p>
 *
 * <h2>方案</h2>
 * <p>把嗅探点放到 <b>frame-decoder 之后、压缩解码器之前</b>：这里拿到的是解密、拆帧后的
 * 原始负载，与压缩状态无关。以 negotiate 的随机 txId 作为唯一标识匹配，
 * 命中即消费该帧（不下传），避免 Velocity 产生"未知 login plugin 应答"噪音。</p>
 *
 * <p>原 {@link ZstdHijacker#channelRead} 的拦截保留为兜底路径（例如应答恰好走正常通道到达）。</p>
 */
public class ZstdNegotiateAnswerSniffer extends MessageToMessageDecoder<ByteBuf> {

    private static final Logger LOGGER = LoggerFactory.getLogger("miku-zstd");

    /** 只在前若干帧内检测，避免长期驻留（应答通常出现在第 3 帧） */
    private static final int MAX_FRAMES = 16;

    /** 应答包 id：login 阶段 C→S 的 custom query answer */
    private static final int PACKET_ID_ANSWER = 0x02;

    private int framesChecked;

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        if (framesChecked++ >= MAX_FRAMES) {
            ctx.pipeline().remove(this);
            out.add(in.retain());
            return;
        }

        ZstdChannelManager mgr = ctx.channel().attr(ZstdChannelManager.KEY).get();
        if (mgr == null || mgr.isReplaced() || mgr.isDictResponseReceived()) {
            ctx.pipeline().remove(this);
            out.add(in.retain());
            return;
        }

        if (tryHandleAnswer(ctx, in, mgr)) {
            ctx.pipeline().remove(this);
            return; // 已消费，不下传
        }
        out.add(in.retain());
    }

    /**
     * 匹配 {@code [id=0x02][varint txId][bool success][varint encStatus][varint decStatus]}。
     * 命中则回填协商结果并返回 true。
     */
    private boolean tryHandleAnswer(ChannelHandlerContext ctx, ByteBuf in, ZstdChannelManager mgr) {
        int txId = mgr.getNegotiateTxId();
        if (txId <= 0) return false;

        int start = in.readerIndex();
        int end = in.writerIndex();
        if (end - start < 3) return false;
        if ((in.getByte(start) & 0xFF) != PACKET_ID_ANSWER) return false;

        // 解析 txId（无副作用读取）
        long value = 0;
        int shift = 0;
        int pos = start + 1;
        while (true) {
            if (pos >= end || shift > 35) return false;
            byte b = in.getByte(pos++);
            value |= (long) (b & 0x7F) << shift;
            if ((b & 0x80) == 0) break;
            shift += 7;
        }
        if ((int) value != txId) return false;

        int encStatus = 2;
        int decStatus = 2;
        if (pos < end && in.getByte(pos) != 0) {
            // success=true：剩余字节是两个 varint 状态
            byte[] tail = new byte[end - pos - 1];
            if (tail.length > 0) {
                in.getBytes(pos + 1, tail);
                int[] cursor = {0};
                encStatus = readVarInt(tail, cursor, 2);
                decStatus = readVarInt(tail, cursor, 2);
            }
        }

        LOGGER.debug("[Zstd] negotiate answer captured at frame level (txId={}, enc={}, dec={})",
                txId, encStatus, decStatus);
        mgr.markDictResponse(encStatus, decStatus);
        // 立即触发激活（若 SetCompression 已写出），省掉最长 100ms 的轮询等待
        ZstdHijacker.notifyDictResponse(ctx.channel());
        return true;
    }

    private static int readVarInt(byte[] data, int[] cursor, int fallback) {
        int result = 0;
        int shift = 0;
        while (cursor[0] < data.length && shift <= 28) {
            byte b = data[cursor[0]++];
            result |= (b & 0x7F) << shift;
            if ((b & 0x80) == 0) return result;
            shift += 7;
        }
        return fallback;
    }
}
