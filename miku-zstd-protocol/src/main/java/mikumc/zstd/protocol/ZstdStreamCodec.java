package mikumc.zstd.protocol;

import com.github.luben.zstd.ZstdDictCompress;
import com.github.luben.zstd.ZstdDictDecompress;
import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayDeque;

/**
 * 跨批共享压缩历史的 zstd 流式编解码（协议 v6）。
 *
 * <h2>为什么要流式</h2>
 * <p>旧实现（v5 及以前）每批独立成帧：{@code [rawSize][zstd(payload)]}。每一帧都是一个
 * <b>独立的 zstd 帧</b>，批次之间不共享历史窗口——两批之间高度重复的内容（区块包、
 * 实体元数据、物品 NBT）被反复重新编码。把 {@code ZstdOutputStream} 的
 * {@code setCloseFrameOnFlush(false)} 打开后，flush 只把当前可输出的数据吐出来而<b>不结束
 * 帧</b>，于是后续批次接着同一段历史继续压，压缩率随之上升。</p>
 *
 * <h2>帧格式不变</h2>
 * <p>仍为 {@code [rawSize][chunk]}，直存帧仍是 {@code [0][packet]}（与原版逐字节同形）。
 * 差别只在 {@code chunk} 的语义：它现在是"连续流的一段"，而不是一个完整帧。解码端把
 * 各段依次喂给 {@link ZstdInputStream}，按 {@code rawSize} 取回原文。</p>
 *
 * <h2>为什么不阻塞</h2>
 * <p>Netty 的 decoder 跑在 event loop 上，绝不能阻塞。{@link ZstdInputStream#read} 在底层
 * 返回 EOF 时会抛 {@code Unexpected end of stream} 而不是挂住；因此 {@link FrameSource}
 * 在队列为空时返回 {@code -1}。正常路径下一帧的数据足以解出 {@code rawSize} 字节
 *（编码端 flush 保证），只有协议错乱才会读到 EOF——那种情况本就该断连。</p>
 *
 * <h2>字典</h2>
 * <p>两个方向都支持：{@link ZstdOutputStream#setDict} / {@link ZstdInputStream#setDict}。
 * 换字典必须重建流对象（zstd 没有"卸载历史"的接口，且新旧字典的历史不能混用）。</p>
 */
public final class ZstdStreamCodec {

    private ZstdStreamCodec() {
    }

    /** 编码端：把每批原始字节写进同一条流，flush 后取出本批产生的 chunk。 */
    public static final class Encoder {

        private final Collector collector = new Collector();
        private ZstdOutputStream out;
        private byte[] pending;

        public Encoder(int level, int windowLog) throws IOException {
            this.out = new ZstdOutputStream(collector, level)
                    // ⚠️ 关键：flush 不结束帧，跨批共享历史窗口
                    .setCloseFrameOnFlush(false)
                    .setWindowLog(windowLog)
                    .setLong(1)
                    .setChecksum(false);
        }

        /** 绑定压缩字典（传 null 表示清空）。绑定后历史重新开始。 */
        public void setDict(ZstdDictCompress dict) throws IOException {
            out.setDict(dict);
        }

        /**
         * 写入一批原始字节并 flush。
         *
         * @return 本批产生的 chunk（调用方负责 release）；返回 {@code null} 表示本批没有输出
         *        （只可能发生在输入极小、zstd 还在攒块时——调用方应按直存处理这一批）
         */
        public ByteBuf encode(ByteBuf raw, int length) throws IOException {
            if (pending == null || pending.length < length) {
                pending = new byte[length];
            }
            raw.getBytes(raw.readerIndex(), pending, 0, length);
            collector.reset();
            out.write(pending, 0, length);
            out.flush();
            return collector.take();
        }

        public void close() {
            try {
                out.close();
            } catch (Throwable ignored) {
                // 连接关闭路径：忽略
            }
            collector.reset();
        }
    }

    /** 解码端：把每帧 chunk 依次喂给同一条流，按 rawSize 取回原文。 */
    public static final class Decoder {

        private final FrameSource source = new FrameSource();
        private final ZstdInputStream in;

        public Decoder() throws IOException {
            this.in = new ZstdInputStream(source);
            // ⚠️ 连续模式：单段结束时不要终止流
            this.in.setContinuous(true);
        }

        /** 绑定解压字典（传 null 表示清空）。 */
        public void setDict(ZstdDictDecompress dict) throws IOException {
            in.setDict(dict);
        }

        /**
         * 喂入一帧 chunk 并解出 {@code expected} 字节。
         *
         * @throws IOException 流提前结束（协议错乱 / 丢帧）：调用方应关闭连接
         */
        public byte[] decode(ByteBuf chunk, int expected) throws IOException {
            source.push(chunk.retain());
            byte[] out = new byte[expected];
            int off = 0;
            while (off < expected) {
                int n = in.read(out, off, expected - off);
                if (n <= 0) {
                    throw new IOException("zstd stream ended before " + expected + "B (got " + off + "B)");
                }
                off += n;
            }
            return out;
        }

        public void close() {
            try {
                in.close();
            } catch (Throwable ignored) {
                // 连接关闭路径：忽略
            }
            source.clear();
        }
    }

    /** 收集 {@link ZstdOutputStream} 的输出到 ByteBuf。 */
    private static final class Collector extends OutputStream {

        private ByteBuf buf = Unpooled.buffer(8 * 1024);

        void reset() {
            if (buf != null) {
                buf.clear();
            }
        }

        @Override
        public void write(int b) {
            buf.writeByte(b);
        }

        @Override
        public void write(byte[] b, int off, int len) {
            buf.writeBytes(b, off, len);
        }

        ByteBuf take() {
            if (buf == null || !buf.isReadable()) {
                return null;
            }
            ByteBuf out = buf;
            buf = Unpooled.buffer(Math.max(8 * 1024, out.readableBytes()));
            return out;
        }
    }

    /**
     * 从"帧队列"读的 {@link InputStream}。
     *
     * <p>⚠️ 队列为空时返回 {@code -1}（EOF）而<b>不是</b>阻塞：event loop 上阻塞等于整个
     * 连接组卡死。EOF 会让 zstd 抛异常，随后由解码器关闭连接——故障可见且不外溢。</p>
     */
    private static final class FrameSource extends InputStream {

        private final ArrayDeque<ByteBuf> queue = new ArrayDeque<>();
        private ByteBuf current;

        void push(ByteBuf b) {
            queue.add(b);
        }

        @Override
        public int read() {
            byte[] one = new byte[1];
            int n = read(one, 0, 1);
            return n <= 0 ? -1 : (one[0] & 0xFF);
        }

        @Override
        public int read(byte[] b, int off, int len) {
            while (current == null || !current.isReadable()) {
                if (current != null) {
                    current.release();
                    current = null;
                }
                current = queue.poll();
                if (current == null) {
                    return -1;
                }
            }
            int n = Math.min(len, current.readableBytes());
            current.readBytes(b, off, n);
            return n;
        }

        void clear() {
            if (current != null) {
                current.release();
                current = null;
            }
            ByteBuf b;
            while ((b = queue.poll()) != null) {
                b.release();
            }
        }
    }
}
