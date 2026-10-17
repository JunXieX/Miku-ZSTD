package mikumc.zstd.protocol;

import io.netty.buffer.ByteBuf;

/**
 * Minecraft VarInt 编解码，以及 varint 相关字节级读取（<b>三端共用的唯一实现</b>）。
 *
 * <p>本类位于共享协议模块 {@code miku-zstd-protocol}，由三端各自通过 {@code sourceSets}
 * 引用同一份源码编译（见模块 README）。
 * 3.0.0 之前项目里存在四份独立实现（两端的管理器、Hijacker 的通用循环版、
 * 嗅探器的 ByteBuffer 版），行为与性能均不一致；后来又有 6 处新写的循环。
 * 现在 byte[] / ByteBuf 两种输入都有唯一的入口：{@link #tryRead}、{@link #readAt}、{@link #readOr}。</p>
 *
 * <p>写路径带 1~3 字节快路径（覆盖 99% 的 MC 包长），读路径支持"数据不足时
 * 不消费缓冲"的尽力语义。</p>
 *
 * <h2>术语：本项目里的 "v几" 有三个不同含义，不要混用</h2>
 * <ul>
 *   <li><b>协商版本</b>：三端 {@code PROTOCOL_VERSION = 4}，在登录期 negotiate 载荷里交换。
 *       三端必须一致，不一致即回落原版 zlib。v4 的改动是"字典改为按需推送"
 *      （v3 把字典内联在 negotiate 里，所有连接都要白吃这几百 KB）。</li>
 *   <li><b>帧格式 v3</b>：{@code [bodyLen?][rawSize][zstd payload | payload]}，
 *       {@code payload = [varint pktLen][pkt]...}。v3 做了帧头精简（magicless /
 *       无 contentSize / 无 dictID）与"一帧多包"批处理。<b>v4 没有再改帧格式。</b></li>
 *   <li><b>v1 / v2</b>：历史版本，只应出现在"我们为什么这么改"的说明里。</li>
 * </ul>
 * <p>所以"帧格式 v3"与"协商版本 v4"是两个维度的编号，同一份代码同时属于这两者。</p>
 */
public final class ZstdVarInts {

    /** 单帧长度上限的默认值（32MB，26.x configuration 阶段存在超大 Registry 包） */
    public static final int DEFAULT_MAX_VALUE = 32 * 1024 * 1024;

    /**
     * Minecraft 单个网络帧的<b>硬上限</b>：{@code 2^21 - 1 = 2097151} 字节。
     *
     * <p>依据：MC 的外层帧长前缀是 3 字节 VarInt（{@code Varint21FrameDecoder} /
     * {@code Varint21LengthFieldPrepender}），能表示的最大值就是 2^21-1；超过它客户端会
     * 直接抛 {@code CorruptedFrameException: length wider than 21-bit}，服务端侧则是
     * prepender 抛 "unable to fit ... into 3 bytes" 后断连。</p>
     *
     * <p>⚠️ 这是<b>帧</b>的上限，不是包的上限：一个包超过它就无法上线，无论内容多合法。
     * 本项目里唯一可能触碰它的是 {@code zstd:dict} —— 它把<b>两个方向</b>的字典放进
     * <b>同一个包</b>，且该包在 Paper 端是在原版压缩启用<b>之前</b>发出的（裸帧，
     * 不会被压缩缩小）。所以 {@code dict_max_bytes} 必须按"两倍 + 头开销"来卡上限，
     * 见 {@link #MAX_DICT_BYTES_PER_DIRECTION}。</p>
     */
    public static final int MC_MAX_FRAME_BYTES = (1 << 21) - 1;

    /**
     * 单方向字典字节数的安全上限：保证"两个方向同帧发送"时不会撑破
     * {@link #MC_MAX_FRAME_BYTES}。
     *
     * <p>一个 {@code zstd:dict} 包的布局：
     * {@code [varint pktId][varint txId][varint len + "zstd:dict"][byte flags]}
     * + 每方向 {@code [int crc][int len][bytes]}。固定开销为
     * 包 id 1 + txId ≤5 + 字符串 10 + flags 1 + 两组的 crc/len 16 ≈ 33 字节，
     * 保守留 64 字节余量：{@code 2 × D + 64 ≤ 2097151} → {@code D ≤ 1048543}。
     * 取 {@code 1000 × 1024 = 1024000}（约 1000KB）：既留足余量，也是个好记的整数。</p>
     *
     * <p>它比"客户端 {@code loadOne} 能接受的 1MB"略小，两者取小是刻意的：客户端那条限制
     * 防的是畸形载荷，而这里的限制防的是<b>协议帧上限</b>——后者更硬，一旦触发就是
     * "连不上"，而不是"字典不生效"。</p>
     */
    public static final int MAX_DICT_BYTES_PER_DIRECTION = 1000 * 1024;

    /** 数据不足，需要更多字节 */
    public static final int NEED_MORE = -1;
    /** 非法编码或超出上限 */
    public static final int INVALID = -2;

    private ZstdVarInts() {
    }

    /**
     * 写 VarInt。对 Minecraft 包最常见的 1~3 字节值做特化，其余走通用循环。
     */
    public static void write(ByteBuf buf, int value) {
        if ((value & ~0x7F) == 0) {
            buf.writeByte(value);
            return;
        }
        if ((value & ~0x3FFF) == 0) {
            buf.writeByte((value & 0x7F) | 0x80);
            buf.writeByte(value >>> 7);
            return;
        }
        if ((value & ~0x1FFFFF) == 0) {
            buf.writeByte((value & 0x7F) | 0x80);
            buf.writeByte(((value >>> 7) & 0x7F) | 0x80);
            buf.writeByte(value >>> 14);
            return;
        }
        while ((value & ~0x7F) != 0) {
            buf.writeByte((value & 0x7F) | 0x80);
            value >>>= 7;
        }
        buf.writeByte(value & 0x7F);
    }

    /**
     * 尽力读取 VarInt，数据不足时不消耗缓冲。
     *
     * @param maxValue 允许的最大值（超出视为非法）
     * @return 值；{@link #NEED_MORE} = 数据不足等待更多字节；{@link #INVALID} = 非法值
     */
    public static int tryRead(ByteBuf buf, int maxValue) {
        buf.markReaderIndex();
        int result = 0;
        int shift = 0;
        int read = 0;
        while (read < 5) {
            if (!buf.isReadable()) {
                buf.resetReaderIndex();
                return NEED_MORE;
            }
            byte b = buf.readByte();
            read++;
            result |= (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                if (result < 0 || result > maxValue) {
                    buf.resetReaderIndex();
                    return INVALID;
                }
                return result;
            }
            shift += 7;
            // 注意：这里不需要额外的 shift 上限检查——while (read < 5) 已保证 shift <= 28
        }
        buf.resetReaderIndex();
        return INVALID;
    }

    /** VarInt 编码后的字节数。 */
    public static int length(int value) {
        for (int i = 1; i < 5; i++) {
            if ((value & (0xFFFFFFFF << (7 * i))) == 0) {
                return i;
            }
        }
        return 5;
    }

    /**
     * 从 {@code data[pos]} 起读一个 varint，结束位置写回 {@code endOut[0]}。
     *
     * <p>非法（超过 5 字节仍未结束）或结果为负时返回 {@link #INVALID}。</p>
     *
     * <p>存在的意义：帧格式解析（按内层 {@code pktLen} 切包）需要在 <b>byte[]</b> 上读 varint，
     * 而 {@link #tryRead} 面向 ByteBuf。以前每个需要它的类都自己写了一遍循环
     *（解码器基类、握手嗅探器、应答嗅探器、Paper 协商器、两个训练器……），
     * 这类"看着照抄不会错"的代码恰恰是最容易悄悄漂移的。</p>
     */
    public static int readAt(byte[] data, int pos, int[] endOut) {
        int result = 0;
        int shift = 0;
        int cursor = pos;
        while (cursor < data.length && shift <= 28) {
            byte b = data[cursor++];
            result |= (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                endOut[0] = cursor;
                return result < 0 ? INVALID : result;
            }
            shift += 7;
        }
        endOut[0] = cursor;
        return INVALID;
    }

    /**
     * 游标版读取：{@code cursor[0]} 既是入参起点、也是出参终点；失败返回 {@code fallback}。
     *
     * <p>与 {@link #readAt} 的区别：这里<b>不</b>把负数判成非法——调用方通常紧接着用
     * {@code <= 0} 自行判断（例如"包长必须为正"）。需要严格语义时用 {@link #readAt}。</p>
     */
    public static int readOr(byte[] data, int[] cursor, int fallback) {
        int pos = cursor[0];
        int result = 0;
        int shift = 0;
        while (pos < data.length && shift <= 28) {
            byte b = data[pos++];
            result |= (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                cursor[0] = pos;
                return result;
            }
            shift += 7;
        }
        cursor[0] = pos;
        return fallback;
    }

    /** 写 UTF-8 字符串（VarInt 长度前缀 + 字节）。 */
    public static void writeString(ByteBuf buf, String s) {
        byte[] bytes = s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        write(buf, bytes.length);
        buf.writeBytes(bytes);
    }

    /**
     * 写入 byte[] 并返回新的偏移。用于批处理时把多个包拼进同一个连续缓冲区
     * （zstd-jni 只接受 byte[]/ByteBuffer，不能直接喂 ByteBuf）。
     */
    public static int writeTo(byte[] dst, int offset, int value) {
        int v = value;
        while ((v & ~0x7F) != 0) {
            dst[offset++] = (byte) ((v & 0x7F) | 0x80);
            v >>>= 7;
        }
        dst[offset++] = (byte) (v & 0x7F);
        return offset;
    }
}
