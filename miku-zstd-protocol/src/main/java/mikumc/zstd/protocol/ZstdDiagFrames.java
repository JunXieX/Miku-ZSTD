package mikumc.zstd.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * 客户端诊断日志通道（{@code zstd:diag}）的线格式与取尾部工具。
 *
 * <h2>为什么需要它</h2>
 * <p>"卡在连接协商中"这类故障，服务端日志只能看到"查询发出去了、应答没回来"，
 * 而真正的原因（客户端有没有收到、装没装解码器、卡在哪个锚点）只在客户端那一侧。
 * 让玩家手动发文件在两件事上会失败：他不知道文件在哪；他放弃之后连接已断，什么也取不到。
 * 所以由服务端在判定卡住时主动问一次，客户端把<b>自己调试文件的尾部</b>压上来。</p>
 *
 * <h2>为什么只能"服务端问、客户端答"</h2>
 * <p>登录阶段客户端不能主动发消息（协议上只有 LoginStart / 加密应答 / 查询应答），
 * 所以形态必然是登录查询。请求与应答都走 {@code LoginPluginRequest} /
 * {@code LoginPluginResponse}，与 {@code zstd:negotiate} 同构。</p>
 *
 * <h2>边界</h2>
 * <ul>
 *   <li>只取尾部：卡住时最近的几百行才有信息量，整份文件既传不动也没必要。</li>
 *   <li>服务端对回传做<b>定量解压</b>：客户端是不可信输入，压缩炸弹不能把服务端打爆。</li>
 *   <li>任何时候都不抛异常给调用方：诊断通道自身坏掉，不能连累登录。</li>
 * </ul>
 */
public final class ZstdDiagFrames {

    /** 通道标识（服务端手工拼 0x04 时用的字符串）。 */
    public static final String CHANNEL = "zstd:diag";

    /** 载荷版本；服务端不匹配时只记一行，不重试。 */
    public static final int PROTOCOL_VERSION = 1;

    /** 卡住探针：服务端判定连接卡住后发起。 */
    public static final byte MODE_STALL = 0;

    /** 预取：登录成功后立刻取一次上一轮的尾部（覆盖"这次已经卡死、问不到"的情况）。 */
    public static final byte MODE_PREFLIGHT = 1;

    public static final int STATUS_OK = 0;
    /** 客户端不认识这个通道（旧版模组 / 原版客户端）。 */
    public static final int STATUS_NOT_MODDED = 1;
    /** 认这个通道，但本次不提供（客户端关闭了回传，或读不到自己的文件）。 */
    public static final int STATUS_NO_LOG = 2;
    /** 已按上限截断。 */
    public static final int STATUS_TRUNCATED = 3;
    public static final int STATUS_ERROR = 4;
    /** 代理代答：这条登录链路的"客户端"其实是代理，真正的玩家日志在代理那一侧。 */
    public static final int STATUS_NOT_CLIENT = 5;

    /** 单次回传的原始字节上限（解压后）——防御压缩炸弹。 */
    public static final int MAX_INFLATED_BYTES = 4 * 1024 * 1024;

    private ZstdDiagFrames() {
    }

    /** 请求载荷：{@code [int version][byte mode]}。 */
    public static void writeRequest(ByteBuf buf, int version, byte mode) {
        buf.writeInt(version);
        buf.writeByte(mode);
    }

    /** 请求载荷解码；长度不足或版本不认识时返回 {@code null}。 */
    public static Request readRequest(byte[] payload) {
        if (payload == null || payload.length < 5) {
            return null;
        }
        ByteBuffer bb = ByteBuffer.wrap(payload);
        int version = bb.getInt();
        byte mode = bb.get();
        return new Request(version, mode);
    }

    /**
     * 应答载荷：{@code [byte status][int version][int rawLen][gzip...]}。
     *
     * <p>{@code rawLen} 是压缩前的字节数，用于服务端在解压时先做一次上界预检。</p>
     */
    public static void writeAnswer(ByteBuf buf, int status, int version, int rawLength, byte[] gzip) {
        buf.writeByte(status);
        buf.writeInt(version);
        buf.writeInt(rawLength);
        if (gzip != null && gzip.length > 0) {
            buf.writeBytes(gzip);
        }
    }

    /** 应答载荷编码（给不方便直接用 ByteBuf 的一侧）。 */
    public static byte[] encodeAnswer(int status, int version, int rawLength, byte[] gzip) {
        ByteBuf buf = Unpooled.buffer(9 + (gzip == null ? 0 : gzip.length));
        try {
            writeAnswer(buf, status, version, rawLength, gzip);
            byte[] out = new byte[buf.readableBytes()];
            buf.readBytes(out);
            return out;
        } finally {
            buf.release();
        }
    }

    /** 应答载荷解码；长度不足时返回 {@code null}。 */
    public static Answer readAnswer(byte[] payload) {
        if (payload == null || payload.length < 9) {
            return null;
        }
        ByteBuffer bb = ByteBuffer.wrap(payload);
        int status = bb.get() & 0xFF;
        int version = bb.getInt();
        int rawLength = bb.getInt();
        byte[] rest = new byte[bb.remaining()];
        bb.get(rest);
        return new Answer(status, version, rawLength, rest);
    }

    /**
     * 读文件尾部：最多 {@code maxBytes} 字节，并且丢掉被切断的第一行。
     *
     * <p>丢掉半行不是洁癖：日志要照着时间线读，开头半行会让"最后一条"看起来像是被谁截断的。</p>
     */
    public static Tail readTail(Path file, int maxBytes) {
        if (file == null || maxBytes <= 0) {
            return Tail.empty();
        }
        try {
            if (!Files.isRegularFile(file)) {
                return Tail.empty();
            }
            long size = Files.size(file);
            long from = Math.max(0L, size - maxBytes);
            boolean truncated = from > 0L;
            try (FileChannel ch = FileChannel.open(file, StandardOpenOption.READ)) {
                ch.position(from);
                ByteBuffer bb = ByteBuffer.allocate((int) (size - from));
                while (bb.hasRemaining() && ch.read(bb) > 0) {
                    // 读到文件末尾
                }
                byte[] raw = new byte[bb.position()];
                bb.flip();
                bb.get(raw);
                if (truncated) {
                    raw = dropPartialFirstLine(raw);
                }
                return new Tail(raw, truncated, size);
            }
        } catch (Throwable t) {
            return Tail.empty();
        }
    }

    private static byte[] dropPartialFirstLine(byte[] raw) {
        for (int i = 0; i < raw.length; i++) {
            if (raw[i] == '\n') {
                byte[] out = new byte[raw.length - i - 1];
                System.arraycopy(raw, i + 1, out, 0, out.length);
                return out;
            }
        }
        return raw;
    }

    /** 压缩；失败返回 {@code null}（调用方按 NO_LOG 处理）。 */
    public static byte[] gzip(byte[] raw) {
        if (raw == null || raw.length == 0) {
            return null;
        }
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream(Math.max(64, raw.length / 4));
            try (GZIPOutputStream gz = new GZIPOutputStream(out)) {
                gz.write(raw);
            }
            return out.toByteArray();
        } catch (Throwable t) {
            return null;
        }
    }

    /**
     * 定量解压：解压结果超过 {@code maxBytes} 立即放弃，返回 {@code null}。
     *
     * <p>服务端拿到的字节是客户端给的，不能直接 {@code readAllBytes} ——
     * 一个几十 KB 的 gzip 可以解出几 GB。</p>
     */
    public static byte[] gunzipBounded(byte[] gz, int maxBytes) {
        if (gz == null || gz.length == 0) {
            return null;
        }
        int limit = Math.max(1, Math.min(maxBytes, MAX_INFLATED_BYTES));
        try (GZIPInputStream in = new GZIPInputStream(new ByteArrayInputStream(gz))) {
            ByteArrayOutputStream out = new ByteArrayOutputStream(Math.min(limit, 64 * 1024));
            byte[] chunk = new byte[8192];
            int total = 0;
            int n;
            while ((n = in.read(chunk)) > 0) {
                total += n;
                if (total > limit) {
                    return null;
                }
                out.write(chunk, 0, n);
            }
            return out.toByteArray();
        } catch (IOException t) {
            return null;
        }
    }

    /**
     * 文件名净化：只留字母、数字、下划线、点、连字符，长度上限 48。
     *
     * <p>玩家名在部分服务端可以带非 ASCII 甚至路径分隔符，落盘前必须清洗。</p>
     */
    public static String sanitize(String raw, int maxLength) {
        if (raw == null || raw.isBlank()) {
            return "unknown";
        }
        StringBuilder sb = new StringBuilder(Math.min(raw.length(), maxLength));
        for (int i = 0; i < raw.length() && sb.length() < maxLength; i++) {
            char c = raw.charAt(i);
            boolean keep = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
                    || c == '_' || c == '-' || c == '.';
            sb.append(keep ? c : '_');
        }
        return sb.isEmpty() ? "unknown" : sb.toString();
    }

    /** 请求参数。 */
    public record Request(int version, byte mode) {
        public boolean isSupported() {
            return version == PROTOCOL_VERSION && (mode == MODE_STALL || mode == MODE_PREFLIGHT);
        }
    }

    /** 应答参数；{@code payload} 是 gzip 原文（可能为空）。 */
    public record Answer(int status, int version, int rawLength, byte[] payload) {
        public boolean hasPayload() {
            return payload != null && payload.length > 0;
        }
    }

    /** 读尾部的结果。 */
    public record Tail(byte[] data, boolean truncated, long fileBytes) {
        public static Tail empty() {
            return new Tail(new byte[0], false, 0L);
        }

        public boolean isEmpty() {
            return data == null || data.length == 0;
        }
    }
}
