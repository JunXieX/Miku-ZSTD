package mikumc.zstd.protocol;

import io.netty.buffer.ByteBuf;

/**
 * Minecraft VarInt 编解码（<b>两端共享的唯一实现</b>）。
 *
 * <p>本类位于共享协议模块 {@code miku-zstd-protocol}，由 Velocity 端与 Fabric 端
 * 各自通过 {@code sourceSets} 引用同一份源码编译（见模块 README）。
 * 3.0.0 之前项目里存在四份独立实现（两端的管理器、Hijacker 的通用循环版、
 * 嗅探器的 ByteBuffer 版），行为与性能均不一致；现已全部收敛到这里。</p>
 *
 * <p>写路径带 1~3 字节快路径（覆盖 99% 的 MC 包长），读路径支持"数据不足时
 * 不消费缓冲"的尽力语义。</p>
 */
public final class ZstdVarInts {

    /** 单帧长度上限的默认值（32MB，26.x configuration 阶段存在超大 Registry 包） */
    public static final int DEFAULT_MAX_VALUE = 32 * 1024 * 1024;

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
