package mikumc.zstd.protocol;

import io.netty.buffer.ByteBuf;

/**
 * 切换握手（{@code zstd:switch}）的线格式工具：三端共用同一份编码 / 识别规则。
 *
 * <h2>询问（服务端 → 客户端）</h2>
 * <pre>
 *   完整包：[0x04][varint txId][varint len "zstd:switch"][int MAGIC]   // LoginPluginRequest
 * </pre>
 *
 * <h2>确认（客户端 → 服务端）</h2>
 * <pre>
 *   完整包：[0x02][varint txId][bool success=1][magic(4B)]            // CustomQueryAnswer
 * </pre>
 * <p>⚠️ 客户端必须用 {@code 0x02}（serverbound 的查询应答），而不是 {@code 0x04}
 * （clientbound 的询问）：服务端在包层会把 {@code 0x04} 解析成别的包直接断连。</p>
 *
 * <h2>识别</h2>
 * <p>服务端在帧层识别客户端确认，客户端在帧层识别服务端询问，两侧都会先把帧还原成
 * "裸包体"（<b>不含包 id</b>），再校验<b>末尾 4 字节魔数</b>并读出 txId。</p>
 *
 * <p>不校验通道名是有意的：通道名是本项目自定义字符串，两端版本不同时长度都可能变；
 * 4 字节魔数（{@link ZstdSwitchGate#MAGIC}）已足够唯一。</p>
 */
public final class ZstdStepFrames {

    /** clientbound LoginPluginRequest（服务端 → 客户端询问）。 */
    public static final int PACKET_ID_LOGIN_PLUGIN = 0x04;

    /** serverbound CustomQueryAnswer（客户端 → 服务端确认）。 */
    public static final int PACKET_ID_QUERY_ANSWER = 0x02;

    /** 解压上限：询问 / 确认都是几十字节，超过这个尺寸的一定不是它。 */
    private static final int MAX_BARE_BYTES = 512;

    private ZstdStepFrames() {
    }

    /** 写服务端询问包（含包 id）。 */
    public static ByteBuf writeQuery(ByteBuf buf, int txId, String channel) {
        ZstdVarInts.write(buf, PACKET_ID_LOGIN_PLUGIN);
        ZstdVarInts.write(buf, txId);
        ZstdVarInts.writeString(buf, channel);
        buf.writeInt(ZstdSwitchGate.MAGIC);
        return buf;
    }

    /**
     * 写客户端确认包（含包 id）：{@code [0x02][txId][success][payload]}。
     *
     * @param accepted true = 已切换到 zstd；false = 客户端无法切换，请服务端保持原版
     */
    public static ByteBuf writeConfirm(ByteBuf buf, int txId, boolean accepted) {
        ZstdVarInts.write(buf, PACKET_ID_QUERY_ANSWER);
        ZstdVarInts.write(buf, txId);
        buf.writeBoolean(accepted);
        if (accepted) {
            buf.writeInt(ZstdSwitchGate.MAGIC);
        }
        return buf;
    }

    /**
     * 把一帧还原成"裸包体字节"（<b>不含包 id</b>），兼容三种外层形态：
     * <ul>
     *   <li>直存 {@code [0][varint pktLen][packet]}（原版压缩启用后的未压缩帧 / zstd 直存帧）；</li>
     *   <li>裸帧 {@code [packet]}（外层长度已被 frame-decoder 剥掉）；</li>
     *   <li>压缩帧 {@code [varint dataLength][deflate(packet)]}（原版压缩帧）。</li>
     * </ul>
     *
     * @return 裸包体；无法识别返回 {@code null}
     */
    public static byte[] asBarePacket(ByteBuf in) {
        int start = in.readerIndex();
        int end = in.writerIndex();
        int len = end - start;
        if (len < 2) return null;
        int first = in.getByte(start) & 0xFF;

        // ① 直存：[0][varint pktLen][packet]
        if (first == 0x00) {
            int idx = start + 1;
            int pktLen = readVarInt(in, idx);
            if (pktLen > 0) {
                idx += varIntLen(in, idx);
                // 必须<b>恰好</b>走完：原版未压缩帧同样是 [0][...]，但那段字节是包体而非长度。
                if ((long) idx + pktLen == end) {
                    byte[] out = new byte[Math.max(0, pktLen - 1)]; // 去掉包 id
                    in.getBytes(idx + 1, out, 0, out.length);
                    return out;
                }
            }
            return null;
        }

        // ② 裸帧：帧层已剥掉外层长度，首字节是包 id
        if (first == PACKET_ID_LOGIN_PLUGIN || first == PACKET_ID_QUERY_ANSWER) {
            byte[] out = new byte[len - 1];
            in.getBytes(start + 1, out, 0, out.length);
            return out;
        }

        // ③ 压缩帧：[varint dataLength][deflate(packet)]
        try {
            int idx = start;
            int declared = readVarInt(in, idx);
            if (declared <= 0 || declared > MAX_BARE_BYTES) return null;
            idx += varIntLen(in, idx);
            if (idx >= end) return null;
            byte[] data = inflate(in, idx, end, declared);
            if (data == null) return null;
            byte[] out = new byte[Math.max(0, data.length - 1)];
            System.arraycopy(data, 1, out, 0, out.length);
            return out;
        } catch (Throwable t) {
            return null;
        }
    }

    /**
     * 按原版规则解压（zlib 包装，与 Minecraft 的 {@code CompressionDecoder} 一致），
     * 循环直到解满 {@code declared} 或解压器结束。
     *
     * @return 恰好 {@code declared} 字节时返回；否则 {@code null}
     */
    private static byte[] inflate(ByteBuf in, int from, int to, int declared) {
        byte[] src = new byte[to - from];
        in.getBytes(from, src, 0, src.length);
        java.util.zip.Inflater inflater = new java.util.zip.Inflater();
        try {
            inflater.setInput(src);
            byte[] out = new byte[declared];
            int total = 0;
            while (total < declared && !inflater.finished()) {
                int n = inflater.inflate(out, total, declared - total);
                if (n == 0 && (inflater.needsInput() || inflater.needsDictionary())) break;
                total += n;
            }
            // 必须完整解满：半截数据不能拿去匹配魔数
            return total == declared ? out : null;
        } catch (Throwable t) {
            return null;
        } finally {
            inflater.end();
        }
    }

    /**
     * 从裸包体里读出切换握手的 txId。
     *
     * @return txId；不是切换握手返回 {@code -1}
     */
    public static int readSwitchTxId(byte[] bare) {
        if (bare == null || bare.length < 4) return -1;
        byte[] magic = new byte[4];
        System.arraycopy(bare, bare.length - 4, magic, 0, 4);
        if (!matchesMagic(magic)) return -1;
        int[] cursor = {0};
        int txId = ZstdVarInts.readOr(bare, cursor, -1);
        return txId > 0 ? txId : -1;
    }

    /**
     * 读出"客户端答了、但没能确认切换"的 txId：{@code [varint txId][0]}（裸包体，无魔数）。
     *
     * <p>必须单独识别：这条应答里没有魔数，但它能告诉服务端"不必再等" ——
     * 收不到它，服务端只能等满 {@link ZstdSwitchGate#RELEASE_TIMEOUT_MS} 才放行扣住的包，
     * 每次登录都白白多等 5 秒。</p>
     *
     * <p>⚠️ <b>它不能被当成"客户端拒绝 zstd"</b>：旧版客户端（没有切换嗅探器）在收到询问后
     * 会按原版登录规则回一个<b>同样字节</b>的空应答，而它其实是 zstd（同样在 SetCompression
     * 时切换了）。把它当拒绝去关连接，等于把正常玩家挡在门外。服务端因此只按
     * "未确认但已提交"处理（见 {@link ZstdSwitchGate} 的类注释）。</p>
     *
     * @return txId；不是这种应答返回 {@code -1}
     */
    public static int readSwitchRejectTxId(byte[] bare) {
        if (bare == null || bare.length < 2) return -1;
        int[] cursor = {0};
        int txId = ZstdVarInts.readOr(bare, cursor, -1);
        if (txId <= 0) return -1;
        // 后面必须恰好只剩一个 success=false 字节
        if (bare.length != cursor[0] + 1) return -1;
        return bare[cursor[0]] == 0 ? txId : -1;
    }

    /** 是否为一段完整的魔数：恰好 4 字节且等于 {@link ZstdSwitchGate#MAGIC}。 */
    public static boolean matchesMagic(byte[] data) {
        if (data == null || data.length != 4) return false;
        int v = ((data[0] & 0xFF) << 24) | ((data[1] & 0xFF) << 16)
                | ((data[2] & 0xFF) << 8) | (data[3] & 0xFF);
        return v == ZstdSwitchGate.MAGIC;
    }

    private static int readVarInt(ByteBuf in, int idx) {
        int value = 0;
        int shift = 0;
        while (idx < in.writerIndex() && shift <= 28) {
            byte b = in.getByte(idx++);
            value |= (b & 0x7F) << shift;
            if ((b & 0x80) == 0) return value;
            shift += 7;
        }
        return -1;
    }

    private static int varIntLen(ByteBuf in, int idx) {
        int n = 0;
        while (idx < in.writerIndex() && n < 5) {
            byte b = in.getByte(idx++);
            n++;
            if ((b & 0x80) == 0) break;
        }
        return n;
    }
}
