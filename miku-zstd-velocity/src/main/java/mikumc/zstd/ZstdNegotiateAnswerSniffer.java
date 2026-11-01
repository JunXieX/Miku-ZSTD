package mikumc.zstd;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import mikumc.zstd.protocol.ZstdNegotiateStatus;
import mikumc.zstd.protocol.ZstdVarInts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 协商应答嗅探器（帧层，位于 frame-decoder 之后、压缩解码器之前）。
 *
 * <h2>为什么必须在这一层捕获</h2>
 * <p>Velocity 处理 login start 后会<b>立即</b>启用压缩（安装 compression-decoder 并写出
 * SetCompressionPacket），而我们的 negotiate 查询只能在第一个出站包时发出——离线模式
 * 下这个"第一个出站包"恰恰就是 SetCompression 本身。客户端收到查询后马上回包（裸帧，
 * 因为它此时还没收到 SetCompression），但这个应答要经过一个 RTT 才到达服务端，
 * <b>那时服务端的原版压缩解码器已经装好</b>，会把裸帧应答当作压缩帧解析并丢弃。</p>
 *
 * <p>实测（本地 Velocity 4.2 + 当时的 v2 模拟客户端）：</p>
 * <pre>
 *   应答按裸帧发送   -> 服务端 spy 完全看不到该包，结论永不回填，zstd 静默不激活
 *   应答按压缩帧发送 -> "Negotiate 应答：status=0 → 客户端就绪"
 * </pre>
 *
 * <p>在线模式（用户生产环境）之所以正常，是因为 EncryptionRequest 带来一个额外 RTT，
 * 应答恰好赶在 SetCompression 之前到达；<b>离线模式代理则完全失效</b>。</p>
 *
 * <h2>方案</h2>
 * <p>把嗅探点放到 <b>frame-decoder 之后、压缩解码器之前</b>：这里拿到的是解密、拆帧后的
 * 原始负载，与压缩状态无关。以 negotiate 的随机 txId 作为唯一标识匹配，
 * 命中即消费该帧（不下传），避免 Velocity 产生"未知 login plugin 应答"噪音。</p>
 *
 * <h2>协议 v8 的应答格式</h2>
 * <p>{@code [0x02][varint txId][bool success][varint status]}。5.0.42 删掉训练字典后
 * <b>只剩一个</b>状态字段：{@code 0 = 就绪}、{@code 2 = 保持原版}（旧客户端可能仍回
 * {@code 1 = 需要字典}，{@link ZstdNegotiateStatus#sanitize} 会把它按 2 处理）。
 * {@code zstd:dict} 通道整体消失，因此本类只认这一种应答，命中即回填结论并自移除。</p>
 */
public class ZstdNegotiateAnswerSniffer extends MessageToMessageDecoder<ByteBuf> {

    private static final Logger LOGGER = LoggerFactory.getLogger("miku-zstd");

    /** 只在前若干帧内检测，避免长期驻留（应答通常出现在第 3 帧） */
    private static final int MAX_FRAMES = 64;

    /** 应答包 id：login 阶段 C→S 的 custom query answer */
    private static final int PACKET_ID_ANSWER = 0x02;

    /**
     * 可能被当成"应答帧"来解析的<b>最大帧长</b>。超过此长度的帧一律不做解析。
     *
     * <p>⚠️ 这不是为了省一次判断，而是为了堵住一条<b>无上限的热路径开销</b>：
     * {@link #asRawPacket} 在"裸帧 / 直存帧"分支里会 {@code new byte[len]} 把<b>整帧</b>拷出来。
     * 登录查询应答只有几字节（{@code [0x02][varint txId][bool][varint]}），即使被原版压缩帧
     * 或 zstd 帧包一层也只有十几字节；而 play 阶段的区块/地图数据帧动辄几百 KB，
     * 会被这条判断直接挡在 {@code asRawPacket} 之外。</p>
     */
    private static final int MAX_ANSWER_FRAME_BYTES = 4096;

    private int framesChecked;

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        ZstdChannelManager mgr = ctx.channel().attr(ZstdChannelManager.KEY).get();
        if (mgr == null || mgr.isReplaced() || mgr.isAnswerReceived()) {
            // 已经激活 / 已定结论：使命结束，热路径上不再有任何解析
            ctx.pipeline().remove(this);
            out.add(in.retain());
            return;
        }

        if (++framesChecked > MAX_FRAMES) {
            // 前 64 帧都没等到应答：这条连接多半压根没装模组，收工
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
     * 匹配登录查询应答并回填结论。
     *
     * <p>⚠️ 帧的<b>外层形态有三种</b>（见 {@link #asRawPacket}），全部都要认得：漏掉任何一种，
     * 应答就永远回填不上 —— 服务端 2 秒后回落原版，而客户端那边已经切到 zstd，
     * 于是形成单侧 zstd → 必断连。</p>
     *
     * @return 是否命中并已消费该帧
     */
    private boolean tryHandleAnswer(ChannelHandlerContext ctx, ByteBuf in, ZstdChannelManager mgr) {
        int negotiateTxId = mgr.getNegotiateTxId();
        if (negotiateTxId <= 0) return false;

        // ⚠️ 必须在 asRawPacket 之前挡住大帧：那个方法对"裸帧 / 直存帧"分支会整帧拷贝。
        int readable = in.writerIndex() - in.readerIndex();
        if (readable > MAX_ANSWER_FRAME_BYTES) return false;

        byte[] packet = asRawPacket(in);
        if (packet == null || packet.length < 3) return false;
        if ((packet[0] & 0xFF) != PACKET_ID_ANSWER) return false;

        // 包结构： [packetId 0x02][varint txId][bool success][varint status]
        int[] cursor = {1};
        int txId = ZstdVarInts.readOr(packet, cursor, ZstdVarInts.INVALID);
        if (txId != negotiateTxId) return false;

        boolean success = cursor[0] < packet.length && packet[cursor[0]] != 0;
        cursor[0]++;
        int status = ZstdNegotiateStatus.FALLBACK;
        if (success && cursor[0] < packet.length) {
            status = ZstdNegotiateStatus.sanitize(
                    ZstdVarInts.readOr(packet, cursor, ZstdNegotiateStatus.FALLBACK));
        }

        LOGGER.debug("[Zstd] negotiate 应答已在帧层捕获（txId={}, status={}, success={}）",
                txId, status, success);
        mgr.markNegotiateResponse(status, success);
        // 立即触发激活（若 SetCompression 已写出），省掉最长 100ms 的轮询等待
        ZstdHijacker.notifyAnswerReceived(ctx.channel());
        return true;
    }

    /**
     * 把一帧还原成"裸包字节"（以包 id 开头），兼容客户端在三种状态下发出的应答。
     *
     * <ul>
     *   <li><b>裸帧</b> {@code [0x02][txId]...} —— 客户端还没装 zlib 编码器（SetCompression 之前，
     *       离线模式下 negotiate 的应答就是这种）；</li>
     *   <li><b>直存帧</b> {@code [0x00][0x02][txId]...} —— 客户端已装 zlib 编码器、且应答小于阈值。
     *       登录查询应答只有几字节，所以这是<b>SetCompression 之后的常态</b>；</li>
     *   <li><b>压缩帧</b> {@code [varint 原始长度>0][deflate]} —— 客户端已装 zlib 编码器、但阈值很小
     *       （例如 {@code network-compression-threshold=1}），此时应答会被真正压缩。</li>
     * </ul>
     *
     * <p>为什么要三种都认：代理这一侧的压缩解码器会把这些帧当作 zlib 帧处理（直存帧恰好被
     * "长度 0 = 未压缩"规则放行，压缩帧被正常解开），所以<b>帧层是唯一能拦到应答的地方</b>；
     * 少认一种，应答就丢了（详见 {@link #tryHandleAnswer} 的警告）。</p>
     *
     * <p>本方法<b>不改变</b>传入缓冲的读位置：识别失败时原帧必须原样交给下游。</p>
     *
     * @return 裸包字节；无法识别返回 {@code null}
     */
    private static byte[] asRawPacket(ByteBuf in) {
        int start = in.readerIndex();
        int len = in.writerIndex() - start;
        if (len < 3) return null;

        int first = in.getByte(start) & 0xFF;
        if (first == PACKET_ID_ANSWER) { // 裸帧
            byte[] out = new byte[len];
            in.getBytes(start, out, 0, len);
            return out;
        }
        if (first == 0x00) { // 直存帧：跳过"未压缩"标记（下一字节必须是包 id 0x02）
            if (len >= 4 && (in.getByte(start + 1) & 0xFF) == PACKET_ID_ANSWER) {
                byte[] out = new byte[len - 1];
                in.getBytes(start + 1, out, 0, out.length);
                return out;
            }
            return null;
        }

        // 其余情况按压缩帧处理：先读声明长度，再解压。
        // ⚠️ 声明长度上限必须收紧到 64：这里声明的就是"解压后的包长"，而我们的应答只有
        // 几字节。放宽会让每一个常规压缩入站帧都走一次 new Inflater() + 解压尝试 —— 那是原生分配。
        try {
            int declared = -1;
            int idx = start;
            int value = 0;
            int shift = 0;
            while (idx < in.writerIndex() && shift <= 28) {
                byte b = in.getByte(idx++);
                value |= (b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    declared = value;
                    break;
                }
                shift += 7;
            }
            if (declared <= 0 || declared > 64 || idx >= in.writerIndex()) return null;

            byte[] src = new byte[in.writerIndex() - idx];
            in.getBytes(idx, src, 0, src.length);
            java.util.zip.Inflater inflater = new java.util.zip.Inflater();
            try {
                inflater.setInput(src);
                byte[] out = new byte[declared];
                // ⚠️ zlib 在 flush 边界可能一次产不完：必须循环 inflate 到 finished()，
                // 否则"单次返回长度不足"会被当成解压失败，把好端端的应答丢掉 ——
                // 表现为偶发"zstd 没生效"（回落原版，不断连），极难复现。
                int total = 0;
                while (total < declared && !inflater.finished()) {
                    int n = inflater.inflate(out, total, declared - total);
                    if (n == 0) {
                        // 没有进展：要么输入不够，要么需要预设字典，都无法继续
                        if (inflater.needsInput() || inflater.needsDictionary()) break;
                    }
                    total += n;
                }
                if (total != declared) return null;
                return out;
            } finally {
                inflater.end();
            }
        } catch (Throwable t) {
            LOGGER.debug("[Zstd] 应答帧解压失败（忽略该帧）: {}", t.toString());
            return null;
        }
    }
}
