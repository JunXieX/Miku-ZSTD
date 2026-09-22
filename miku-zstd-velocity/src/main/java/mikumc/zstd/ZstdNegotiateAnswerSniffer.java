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
     * 匹配 {@code [id=0x02][varint txId][bool success][varint encStatus][varint decStatus]}，
     * txId 与 negotiate 或 dict 查询任一相符即命中。
     *
     * @return 是否命中并已消费该帧
     */
    private boolean tryHandleAnswer(ChannelHandlerContext ctx, ByteBuf in, ZstdChannelManager mgr) {
        int negotiateTxId = mgr.getNegotiateTxId();
        int dictTxId = mgr.getDictTxId();
        if (negotiateTxId <= 0 && dictTxId <= 0) return false;

        int start = in.readerIndex();
        int end = in.writerIndex();
        if (end - start < 3) return false;
        if ((in.getByte(start) & 0xFF) != PACKET_ID_ANSWER) return false;

        // 无副作用读取 txId：借共享的 ByteBuf 版读取器，读完把读位置还原。
        // （不能依赖 tryRead 内部的 mark/reset——它会把标记覆盖成 varint 的起点。）
        // 成功后 tryRead 会把读位置推到 varint 之后，正好就是 success 位的下标，
        // 这比用 length(txId) 重算更可靠（对非最小编码也成立）。
        in.readerIndex(start + 1);
        int txId = ZstdVarInts.tryRead(in, Integer.MAX_VALUE);
        int successPos = in.readerIndex();
        in.readerIndex(start);
        if (txId < 0) return false;

        boolean isNegotiate = negotiateTxId > 0 && txId == negotiateTxId;
        boolean isDict = dictTxId > 0 && txId == dictTxId;
        if (!isNegotiate && !isDict) return false;

        // 包结构： [varint txId][bool success][varint encStatus][varint decStatus]
        boolean success = successPos < end && in.getByte(successPos) != 0;
        int encStatus = ZstdNegotiateStatus.VANILLA;
        int decStatus = ZstdNegotiateStatus.VANILLA;
        if (success && successPos + 1 < end) {
            byte[] tail = new byte[end - successPos - 1];
            in.getBytes(successPos + 1, tail);
            int[] cursor = {0};
            encStatus = ZstdNegotiateStatus.sanitize(
                    ZstdVarInts.readOr(tail, cursor, ZstdNegotiateStatus.FALLBACK));
            decStatus = ZstdNegotiateStatus.sanitize(
                    ZstdVarInts.readOr(tail, cursor, ZstdNegotiateStatus.FALLBACK));
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
}
