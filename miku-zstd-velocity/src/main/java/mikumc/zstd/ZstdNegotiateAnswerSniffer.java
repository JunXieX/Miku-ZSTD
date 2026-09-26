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
 * 原始负载，与压缩状态无关。以协商/字典查询的随机 txId 作为唯一标识匹配，
 * 命中即消费该帧（不下传），避免 Velocity 产生"未知 login plugin 应答"噪音。</p>
 *
 * <h2>⚠️ 两种应答都必须在这里捕获（曾经只捕了一种，造成必断连）</h2>
 * <p>协议 v4 的流程是<b>两段式</b>：negotiate → 客户端回"缺字典" → 服务端推
 * {@code zstd:dict} → 客户端回"就绪"。第二段的应答与第一段走完全相同的通道，
 * 因此同样会撞上"此时压缩解码器已就绪"的问题，<b>也必须在帧层捕获</b>。</p>
 *
 * <p>旧实现只匹配 {@code negotiateTxId}，且在命中协商应答后无条件
 * {@code markDictResponse} 并自移除，于是：</p>
 * <ol>
 *   <li>客户端答"缺字典"(status=1) 被当成"最终结论"，服务端<b>从不推送字典</b>；</li>
 *   <li>客户端在等字典，状态停在 PLAIN，<b>永不激活</b>；</li>
 *   <li>服务端却按"应答已到、无协议不匹配"照常激活 zstd → 单侧 zstd → <b>断连</b>。</li>
 * </ol>
 * <p>现在：命中协商应答且需要字典时，走 {@link ZstdHijacker#sendDictQuery} 推字典，
 * <b>不自移除、也不下结论</b>，继续等字典应答；结论只由最后那一次应答给出。
 * 另外在 ZstdHijacker 的激活路径上还有一道硬门控兜底（见 activateZstd），
 * 即使这里的应答真的丢了，也只会回落原版而不会断连。</p>
 */
public class ZstdNegotiateAnswerSniffer extends MessageToMessageDecoder<ByteBuf> {

    private static final Logger LOGGER = LoggerFactory.getLogger("miku-zstd");

    /** 只在前若干帧内检测，避免长期驻留（应答通常出现在第 3 帧） */
    private static final int MAX_FRAMES = 64;

    /** 应答包 id：login 阶段 C→S 的 custom query answer */
    private static final int PACKET_ID_ANSWER = 0x02;

    private int framesChecked;

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        ZstdChannelManager mgr = ctx.channel().attr(ZstdChannelManager.KEY).get();
        if (mgr == null || mgr.isReplaced() || mgr.isDictResponseReceived()) {
            ctx.pipeline().remove(this);
            out.add(in.retain());
            return;
        }

        framesChecked++;
        // 帧上限只是"这件事彻底没戏了"的安全阀。**等待字典推送应答期间不启用**：
        // 那一段要等客户端收完几十~几百 KB 并算完 CRC 才回包，很容易越过原本的 16 帧。
        // 真正的收尾由"结论已回填"或通道关闭负责。
        if (!awaitingDict(mgr) && framesChecked >= MAX_FRAMES) {
            ctx.pipeline().remove(this);
            out.add(in.retain());
            return;
        }

        if (tryHandleAnswer(ctx, in, mgr)) {
            // 结论已定才自移除；若刚发完字典推送，则继续留在帧层等它的应答
            if (mgr.isDictResponseReceived()) {
                ctx.pipeline().remove(this);
            }
            return; // 已消费，不下传
        }
        out.add(in.retain());
    }

    /** 是否正在等待 {@code zstd:dict} 推送的应答。 */
    private static boolean awaitingDict(ZstdChannelManager mgr) {
        return mgr.isDictRequested() && !mgr.isDictResponseReceived();
    }

    /**
     * 匹配登录查询应答：{@code [0x02][varint txId][bool success][varint encStatus][varint decStatus]}，
     * txId 与 negotiate 或 dict 查询任一相符即命中。
     *
     * <p>⚠️ 帧的<b>外层形态有三种</b>（见 {@link #asRawPacket}），全部都要认得：漏掉任何一种，
     * 应答就永远回填不上 —— 服务端 2 秒后回落原版，而客户端此刻已经装载字典并切到 zstd，
     * 于是形成单侧 zstd → 必断连。</p>
     *
     * @return 是否命中并已消费该帧
     */
    private boolean tryHandleAnswer(ChannelHandlerContext ctx, ByteBuf in, ZstdChannelManager mgr) {
        int negotiateTxId = mgr.getNegotiateTxId();
        int dictTxId = mgr.getDictTxId();
        if (negotiateTxId <= 0 && dictTxId <= 0) return false;

        byte[] packet = asRawPacket(in);
        if (packet == null || packet.length < 3) return false;
        if ((packet[0] & 0xFF) != PACKET_ID_ANSWER) return false;

        // 包结构： [packetId 0x02][varint txId][bool success][varint encStatus][varint decStatus]
        int[] cursor = {1};
        int txId = ZstdVarInts.readOr(packet, cursor, ZstdVarInts.INVALID);
        if (txId < 0) return false;

        boolean isNegotiate = negotiateTxId > 0 && txId == negotiateTxId;
        boolean isDict = dictTxId > 0 && txId == dictTxId;
        if (!isNegotiate && !isDict) return false;

        boolean success = cursor[0] < packet.length && packet[cursor[0]] != 0;
        cursor[0]++;
        int encStatus = ZstdNegotiateStatus.VANILLA;
        int decStatus = ZstdNegotiateStatus.VANILLA;
        if (success && cursor[0] < packet.length) {
            encStatus = ZstdNegotiateStatus.sanitize(
                    ZstdVarInts.readOr(packet, cursor, ZstdNegotiateStatus.FALLBACK));
            decStatus = ZstdNegotiateStatus.sanitize(
                    ZstdVarInts.readOr(packet, cursor, ZstdNegotiateStatus.FALLBACK));
        }

        if (isNegotiate && ZstdHijacker.needsDictPush(encStatus, decStatus)) {
            // 协议 v4：客户端缺字典。推一次 zstd:dict，**先不下结论**——
            // 门控必须等这次推送的应答，否则服务端会先于客户端激活（单侧 zstd）。
            mgr.markDictRequested();
            ZstdHijacker.sendDictQuery(ctx.channel(),
                    ZstdNegotiateStatus.needsDict(encStatus), ZstdNegotiateStatus.needsDict(decStatus));
            LOGGER.debug("[Zstd] negotiate 应答要求字典（enc={} dec={}），已推送 zstd:dict，继续等待其应答",
                    encStatus, decStatus);
            return true;
        }

        LOGGER.debug("[Zstd] {} 应答已在帧层捕获（txId={}, enc={}, dec={}, success={}）",
                isDict ? "zstd:dict" : "negotiate", txId, encStatus, decStatus, success);
        mgr.markDictResponse(encStatus, decStatus, success);
        // 立即触发激活（若 SetCompression 已写出），省掉最长 100ms 的轮询等待
        ZstdHijacker.notifyDictResponse(ctx.channel());
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
        if (first == 0x00) { // 直存帧：跳过"未压缩"标记
            if (len < 4 || (in.getByte(start + 1) & 0xFF) != PACKET_ID_ANSWER) return null;
            byte[] out = new byte[len - 1];
            in.getBytes(start + 1, out, 0, out.length);
            return out;
        }

        // 其余情况按压缩帧处理：先读声明长度，再解压。
        // 长度上限刻意收紧到 64KB —— 登录查询应答不可能这么大，避免畸形帧逼出大分配。
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
            if (declared <= 0 || declared > 65536 || idx >= in.writerIndex()) return null;

            byte[] src = new byte[in.writerIndex() - idx];
            in.getBytes(idx, src, 0, src.length);
            java.util.zip.Inflater inflater = new java.util.zip.Inflater();
            try {
                inflater.setInput(src);
                byte[] out = new byte[declared];
                if (inflater.inflate(out) != declared) return null;
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
