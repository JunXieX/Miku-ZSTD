package mikumc.zstd;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.MessageToMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 握手嗅探器：挂在 frame-decoder 之后，检查 handshake 包主机名是否携带 "\0ZSTD\0" 标记。
 *
 * <p>Velocity 4.x 的 ConnectionHandshakeEvent 为异步派发，监听器注入的管道处理器
 * 总是晚于同批读取的 login start 处理，导致 negotiate 无法在 login 阶段早期
 * 被客户端（已挂载登录监听器后）正确接收。
 * 本嗅探器在解码链上游同步检测标记，并在 login start 被处理<b>之前</b>完成管道注入；
 * negotiate 仍由 spy.write 首次触发（velocity 发出 encryption request /
 * SetCompression 之时先于该包写出），该时刻客户端登录监听器必然已就绪。</p>
 *
 * <p>仅对登录连接（nextState==2）生效；检测完成后自移除。</p>
 *
 * <h2>3.1.0 变更</h2>
 * <p>本嗅探器会挂到<b>每一条</b>新连接上（绝大多数是非 zstd 客户端），因此
 * 快速失败路径必须是零分配的：先用 {@link ByteBuf#indexOf} 在缓冲内直接找
 * NUL 字节（标记的第一个字节），找不到就直接判定"非 zstd 客户端"，不再像旧实现
 * 那样无条件把整帧拷进 {@code byte[]} 再做朴素匹配。</p>
 */
public class ZstdHandshakeSniffer extends MessageToMessageDecoder<ByteBuf> {

    private static final Logger LOGGER = LoggerFactory.getLogger("miku-zstd");

    private static final byte[] MARKER = "\0ZSTD\0".getBytes(java.nio.charset.StandardCharsets.ISO_8859_1);

    private int framesChecked;

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        // 透传给下游（compression-decoder / minecraft-decoder）
        out.add(in.retain());

        if (framesChecked++ > 2) {
            // handshake + login start 都未命中，放弃检测（非 zstd 客户端）
            ctx.pipeline().remove(this);
            return;
        }

        if (!mayContainMarker(in)) {
            return;
        }

        byte[] data = new byte[in.readableBytes()];
        in.getBytes(in.readerIndex(), data);
        if (!contains(data, MARKER)) {
            return;
        }

        // 仅对登录连接（nextState==2）注入；服务器列表 ping（nextState==1）不做任何处理。
        Integer nextState = parseHandshakeNextState(data);
        if (nextState == null || nextState != 2) {
            ctx.pipeline().remove(this);
            return;
        }

        Channel channel = ctx.channel();
        LOGGER.info("[Zstd] Sniffer detected Zstd client on {}", channel.remoteAddress());

        ZstdChannelManager mgr = new ZstdChannelManager();
        channel.attr(ZstdChannelManager.KEY).set(mgr);
        channel.closeFuture().addListener(f -> mgr.close());

        // 当前处于 eventLoop 线程且早于 login start 处理，同步注入安全。
        // 注意：此处不发送 negotiate——真实客户端在发出 login start 之前尚未挂载
        // 登录监听器，过早发送会被丢弃。negotiate 由 spy.write 首次触发
        // （velocity 发出 encryption request / SetCompression 之时，先于该包），
        // 该时刻客户端监听器必然就绪，与 Velocity 3.x 的原始时序一致。
        ChannelPipeline p = channel.pipeline();
        try {
            if (p.get("zstd_outbound_spy") == null && p.get("handler") != null) {
                p.addBefore("handler", "zstd_outbound_spy", new ZstdHijacker());
                LOGGER.debug("[Zstd] Sniffer injected spy before login processing; pipeline={}", p.names());
            }
            // 帧层应答嗅探：必须位于压缩解码器之前，否则离线模式下应答会被原版压缩
            // 解码器当作压缩帧丢弃（详见 ZstdNegotiateAnswerSniffer 的类注释）
            if (p.get("zstd-negotiate-answer") == null) {
                p.addAfter(ctx.name(), "zstd-negotiate-answer", new ZstdNegotiateAnswerSniffer());
                LOGGER.debug("[Zstd] Negotiate-answer sniffer installed at frame level");
            }
        } catch (Exception e) {
            LOGGER.error("[Zstd] Sniffer failed to inject spy, falling back to vanilla", e);
        }
        p.remove(this);
    }

    /**
     * 零分配快速筛查：标记以 NUL 开头，若整帧不含 NUL 则必然不是 zstd 客户端。
     * 命中候选位置后再逐字节比对，避免为绝大多数普通连接做整帧拷贝。
     */
    private static boolean mayContainMarker(ByteBuf in) {
        int from = in.readerIndex();
        int to = in.writerIndex();
        for (int i = in.indexOf(from, to, (byte) 0); i >= 0; i = in.indexOf(i + 1, to, (byte) 0)) {
            if (i + MARKER.length > to) return false;
            boolean hit = true;
            for (int j = 0; j < MARKER.length; j++) {
                if (in.getByte(i + j) != MARKER[j]) {
                    hit = false;
                    break;
                }
            }
            if (hit) return true;
        }
        return false;
    }

    /**
     * 解析 handshake 包：id(varint=0) + protocol(varint) + host(string) + port(ushort) + nextState(varint)。
     * 解析失败返回 null。
     */
    private static Integer parseHandshakeNextState(byte[] data) {
        try {
            java.nio.ByteBuffer buf = java.nio.ByteBuffer.wrap(data);
            int id = readVarint(buf);
            if (id != 0) return null;
            readVarint(buf); // protocolVersion
            int hostLen = readVarint(buf);
            buf.position(buf.position() + hostLen); // host
            buf.getShort(); // port
            return readVarint(buf); // nextState
        } catch (Exception e) {
            return null;
        }
    }

    private static int readVarint(java.nio.ByteBuffer buf) {
        int result = 0;
        int shift = 0;
        while (true) {
            byte b = buf.get();
            result |= (b & 0x7F) << shift;
            if ((b & 0x80) == 0) return result;
            shift += 7;
            if (shift > 35) throw new IllegalArgumentException("varint too big");
        }
    }

    private static boolean contains(byte[] haystack, byte[] needle) {
        if (haystack.length < needle.length) return false;
        outer:
        for (int i = 0; i <= haystack.length - needle.length; i++) {
            for (int j = 0; j < needle.length; j++) {
                if (haystack[i + j] != needle[j]) continue outer;
            }
            return true;
        }
        return false;
    }
}
