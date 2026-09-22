package mikumc.zstd.protocol;

import com.github.luben.zstd.ZstdCompressCtx;
import com.github.luben.zstd.ZstdDecompressCtx;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 帧格式的<b>跨端黄金不变量</b>测试。
 *
 * <h2>它保护什么</h2>
 * <p>编码器与解码器共享 {@link ZstdBatchEncoderBase} / {@link ZstdBatchDecoderBase}，
 * 而帧格式是"逐字节对称"的：一旦漂移，症状是<b>握手能成、之后静默错乱</b>
 *（2.0.0 就因客户端多写一层长度前缀造成过断连回归）。本测试把帧布局逐字段固定下来，
 * 任何一侧改动导致布局变化都会在这里失败。</p>
 *
 * <h2>为什么用测试自带的编解码器，而不是三个模块里的实现</h2>
 * <p>三端的差异只有"外层长度归属"这一个开关（{@code writeBodyLen()}），
 * 帧体布局完全在共享基类里。所以用两个只差这个开关的子类，就能同时覆盖
 * Velocity 形态（自带 bodyLen）与 Paper/Fabric 形态（由 prepender 补）。
 * 本测试因此<b>不依赖</b> Velocity / Paper / Minecraft 的任何类，纯 netty + zstd 即可运行。</p>
 *
 * <h2>为什么放在 miku-zstd-protocol 目录下</h2>
 * <p>被测代码是共享层，测试理应挨着它。该目录不是独立 Gradle 工程（见模块 README），
 * 所以由 Velocity 模块把它加进 {@code sourceSets.test} 来执行——三个模块里只有它是
 * 纯 {@code java} 插件，代价最小。</p>
 *
 * <h2>时序确定性</h2>
 * <p>批处理窗口设成 60s（测试期间不会到期），靠"攒够 batch_max_packets 立即成帧"来
 * 精确控制帧边界：<b>写 N 个包 → 恰好一帧含 N 个包</b>。这样既不依赖虚拟时钟推进，
 * 也不会有随机时序抖动。压缩路径本身是异步的（提交到压缩线程池），
 * 那条用例才需要等待。</p>
 */
class FrameLayoutTest {

    /** 批处理窗口：远大于测试耗时，等价于"只在达到 max_packets 时成帧"。 */
    private static final int NEVER_DUE_MS = 60_000;

    private ZstdCompressCtx compressCtx;
    private ZstdDecompressCtx decompressCtx;

    @BeforeEach
    void setUp() {
        compressCtx = new ZstdCompressCtx();
        compressCtx.setLevel(3);
        compressCtx.setMagicless(true);
        compressCtx.setContentSize(false);
        compressCtx.setDictID(false);
        decompressCtx = new ZstdDecompressCtx();
        decompressCtx.setMagicless(true);
    }

    @AfterEach
    void tearDown() {
        compressCtx.close();
        decompressCtx.close();
    }

    // ────────────────────────────────────────────────────────────────
    // 直存帧：布局
    // ────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("直存帧（Velocity 形态）：[bodyLen][0][payload]，bodyLen 等于其后字节数")
    void storedFrameLayoutWithBodyLen() {
        TestEncoder encoder = new TestEncoder(true, Integer.MAX_VALUE, 1);
        EmbeddedChannel ch = new EmbeddedChannel(encoder);

        ch.writeOutbound(packet(ascii("hello")));
        ByteBuf frame = readFrame(ch);
        assertNotNull(frame, "编码器应当产出一帧");

        int bodyLen = ZstdVarInts.tryRead(frame, Integer.MAX_VALUE);
        // bodyLen 的含义是"其后剩余字节数"，必须在读掉 rawSize 之前取这个值
        int afterBodyLen = frame.readableBytes();
        int rawSize = ZstdVarInts.tryRead(frame, Integer.MAX_VALUE);
        assertEquals(0, rawSize, "直存帧的 rawSize 必须是 0");
        assertEquals(afterBodyLen, bodyLen, "bodyLen 必须等于其后剩余字节数");
        assertFalse(encoder.lastCompressed, "低于阈值不应走压缩");
    }

    @Test
    @DisplayName("直存帧（Paper/Fabric 形态）：帧头就是 [0][payload]，不写 bodyLen")
    void storedFrameLayoutWithoutBodyLen() {
        TestEncoder encoder = new TestEncoder(false, Integer.MAX_VALUE, 1);
        EmbeddedChannel ch = new EmbeddedChannel(encoder);

        ch.writeOutbound(packet(ascii("aa")));
        ByteBuf frame = readFrame(ch);
        assertNotNull(frame);

        int total = frame.readableBytes();
        int rawSize = ZstdVarInts.tryRead(frame, Integer.MAX_VALUE);
        assertEquals(0, rawSize);
        // 帧 = [varint 0][varint pktLen][pkt]，其中 pkt = 1 字节包 id + 2 字节负载
        assertEquals(1 + ZstdVarInts.length(3) + 3, total,
                "帧应只含 [0][varint pktLen][pkt]，不含任何外层长度");
    }

    // ────────────────────────────────────────────────────────────────
    // 批处理与往返
    // ────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("批处理：攒够 batch_max_packets 合成一帧，负载为 [varint pktLen][pkt] 顺序拼接")
    void batchIsCombinedIntoSingleFrame() {
        TestEncoder encoder = new TestEncoder(true, Integer.MAX_VALUE, 3);
        EmbeddedChannel ch = new EmbeddedChannel(encoder);

        byte[] p1 = ascii("alpha");
        byte[] p2 = ascii("beta");
        byte[] p3 = ascii("gamma");
        ch.writeOutbound(packet(p1));
        ch.writeOutbound(packet(p2));
        ch.writeOutbound(packet(p3));

        ByteBuf frame = readFrame(ch);
        assertNotNull(frame);
        int bodyLen = ZstdVarInts.tryRead(frame, Integer.MAX_VALUE);
        int afterBodyLen = frame.readableBytes();
        int rawSize = ZstdVarInts.tryRead(frame, Integer.MAX_VALUE);
        assertEquals(0, rawSize);
        assertEquals(afterBodyLen, bodyLen);

        assertArrayEquals(new byte[][]{fullPacket(p1), fullPacket(p2), fullPacket(p3)},
                drainPackets(frame), "三个包应按顺序出现在同一帧里");
        assertEquals(1, encoder.frames.size(), "三包应只产生一帧");
        assertNull(readFrame(ch), "不应有第二帧");
    }

    @Test
    @DisplayName("往返：一帧多包 → 解码按序还原（两种外层长度形态各测一遍）")
    void roundTrip() {
        for (boolean withBodyLen : new boolean[]{true, false}) {
            assertRoundTrip(withBodyLen, 3, 3);
            assertRoundTrip(withBodyLen, 8, 32);
        }
    }

    // ────────────────────────────────────────────────────────────────
    // 压缩帧
    // ────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("压缩帧：[bodyLen][rawSize>0][zstd]，且重复数据确实变小")
    void compressedFrameLayoutAndRoundTrip() {
        TestEncoder encoder = new TestEncoder(true, 0 /* 一律压缩 */, 1);
        EmbeddedChannel ch = new EmbeddedChannel(encoder);
        EmbeddedChannel in = new EmbeddedChannel(new TestDecoder(decompressCtx));

        byte[] payload = repetitive(4096);
        ch.writeOutbound(packet(payload));
        ByteBuf frame = readFrame(ch, 5000);
        assertNotNull(frame, "压缩走线程池，需要等待回到 event loop");

        // ⚠️ 解析必须用 duplicate()：读断言会推进 readerIndex，而这一帧下面还要原样喂给解码器。
        // （曾经直接用 frame 解析，于是喂过去的是"已被消费掉帧头"的残帧，解码必然失败。）
        ByteBuf probe = frame.duplicate();
        int bodyLen = ZstdVarInts.tryRead(probe, Integer.MAX_VALUE);
        int afterBodyLen = probe.readableBytes();
        int rawSize = ZstdVarInts.tryRead(probe, Integer.MAX_VALUE);
        assertTrue(rawSize > 0, "压缩帧的 rawSize 应为原始长度（>0）");
        assertEquals(1 + ZstdVarInts.length(payload.length) + payload.length, rawSize,
                "rawSize 应等于 [varint pktLen][pkt] 的总长");
        assertEquals(afterBodyLen, bodyLen, "bodyLen 必须等于其后剩余字节数");
        assertTrue(encoder.lastCompressed, "应当走压缩路径");
        assertTrue(encoder.lastWire < encoder.lastRaw, "重复数据的压缩帧应当更小");

        feed(in, frame);
        List<byte[]> got = drainInbound(in);
        if (got.isEmpty()) {
            // 解码器拒绝帧时会自行 LOGGER.warn 出原因（测试用 slf4j-simple 打到 stderr），
            // 这里补一句上下文，免得只看到"没产出包"而无从下手
            fail("解压帧未被还原：channelOpen=" + in.isOpen() + "，帧体=" + frame.readableBytes() + "B");
        }
        assertArrayEquals(fullPacket(payload), got.get(0), "解压后应与原始包一致（含包 id）");
    }

    // ────────────────────────────────────────────────────────────────
    // 畸形输入：一律 fail-fast（关连接），绝不放行
    // ────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("拒绝：非法帧头（varint 超过 5 字节仍未结束）")
    void rejectMalformedVarintHeader() {
        ByteBuf buf = Unpooled.buffer();
        // 0x80 = 仅续位（continue bit）：合法写法里超过 5 字节即非法。
        // 注意 128 << 28 会溢出成 0，所以必须连续续位才能让解析循环走到上限。
        buf.writeByte(0x80);
        buf.writeByte(0x80);
        buf.writeByte(0x80);
        buf.writeByte(0x80);
        buf.writeByte(0x80);
        buf.writeByte(0x01);
        assertRejected(buf);
    }

    @Test
    @DisplayName("拒绝：rawSize 超上限（32MB）")
    void rejectRawSizeOverLimit() {
        ByteBuf buf = Unpooled.buffer();
        ZstdVarInts.write(buf, 32 * 1024 * 1024 + 1);
        buf.writeByte(0x00);
        assertRejected(buf);
    }

    @Test
    @DisplayName("拒绝：空帧体（只有帧头没有负载）")
    void rejectEmptyFrameBody() {
        ByteBuf buf = Unpooled.buffer();
        ZstdVarInts.write(buf, 0);
        assertRejected(buf);
    }

    @Test
    @DisplayName("拒绝：放大攻击（声明 1MB 却只给 1 字节）")
    void rejectAmplificationAttack() {
        ByteBuf buf = Unpooled.buffer();
        ZstdVarInts.write(buf, 1024 * 1024);
        buf.writeByte(0x00);
        assertRejected(buf);
    }

    @Test
    @DisplayName("拒绝：内层包长越界（声明长度超出帧体）")
    void rejectInnerLengthOverrun() {
        ByteBuf buf = Unpooled.buffer();
        ZstdVarInts.write(buf, 0);   // 直存
        ZstdVarInts.write(buf, 999); // 内层声明 999 字节
        buf.writeBytes(ascii("short"));
        assertRejected(buf);
    }

    @Test
    @DisplayName("拒绝：压缩体比声明的 rawSize 还大")
    void rejectCompressedLargerThanDeclared() {
        ByteBuf buf = Unpooled.buffer();
        ZstdVarInts.write(buf, 4);     // 声明原文 4 字节
        buf.writeBytes(new byte[16]);  // 却给了 16 字节压缩体
        assertRejected(buf);
    }

    @Test
    @DisplayName("拒绝：直存帧里内层包长为 0")
    void rejectZeroInnerLength() {
        ByteBuf buf = Unpooled.buffer();
        ZstdVarInts.write(buf, 0);
        ZstdVarInts.write(buf, 0);
        buf.writeByte(0x01);
        assertRejected(buf);
    }

    // ────────────────────────────────────────────────────────────────
    // 辅助
    // ────────────────────────────────────────────────────────────────

    private void assertRoundTrip(boolean withBodyLen, int perFrame, int total) {
        TestEncoder encoder = new TestEncoder(withBodyLen, Integer.MAX_VALUE, perFrame);
        EmbeddedChannel out = new EmbeddedChannel(encoder);
        EmbeddedChannel in = new EmbeddedChannel(new TestDecoder(decompressCtx));

        List<byte[]> sent = new ArrayList<>();
        for (int i = 0; i < total; i++) {
            byte[] payload = repetitive(20 + i);
            sent.add(fullPacket(payload)); // 编码器看到的是含包 id 的完整包
            out.writeOutbound(packet(payload));
        }

        List<byte[]> got = new ArrayList<>();
        ByteBuf f;
        while ((f = readFrame(out)) != null) {
            feed(in, f);
            got.addAll(drainInbound(in));
        }

        assertEquals(sent.size(), got.size(),
                "包数应当一致（withBodyLen=" + withBodyLen + ", perFrame=" + perFrame + "）");
        for (int i = 0; i < sent.size(); i++) {
            assertArrayEquals(sent.get(i), got.get(i), "第 " + i + " 个包内容应一致");
        }
    }

    /**
     * 把一帧喂给解码器。
     *
     * <p>若该帧带外层 {@code bodyLen}，先按真实管线的行为把它剥掉（那是 frame-decoder 的职责），
     * 只把负载交给解码器——这正是三端"外层长度归属不同"的差异所在，测试必须如实模拟。</p>
     */
    private static void feed(EmbeddedChannel in, ByteBuf frame) {
        // 防呆：被断言读过的帧不该再喂给解码器（否则喂进去的是残帧，
        // 症状是"解压不出任何包"，而根因在调用方——所以在这里直接报出来）。
        if (frame.readerIndex() != 0) {
            fail("传入的帧已被消费过（readerIndex=" + frame.readerIndex()
                    + "）：解析断言请改用 frame.duplicate()");
        }
        int start = frame.readerIndex();
        int first = ZstdVarInts.tryRead(frame, Integer.MAX_VALUE);
        if (first == frame.readableBytes()) {
            // [bodyLen][...]：bodyLen 恰好等于其后剩余字节数 → 剥掉它
            in.writeInbound(frame.readRetainedSlice(first));
        } else {
            frame.readerIndex(start);
            in.writeInbound(frame.retain());
        }
    }

    private static List<byte[]> drainInbound(EmbeddedChannel in) {
        List<byte[]> out = new ArrayList<>();
        Object o;
        while ((o = in.readInbound()) != null) {
            ByteBuf b = (ByteBuf) o;
            try {
                byte[] data = new byte[b.readableBytes()];
                b.getBytes(b.readerIndex(), data);
                out.add(data);
            } finally {
                b.release();
            }
        }
        return out;
    }

    /** 逐字段走一遍帧体，按 {@code [varint pktLen][pkt]...} 切出各包。 */
    private static byte[][] drainPackets(ByteBuf frame) {
        byte[] data = new byte[frame.readableBytes()];
        frame.getBytes(frame.readerIndex(), data);
        List<byte[]> out = new ArrayList<>();
        int[] cursor = {0};
        while (cursor[0] < data.length) {
            int len = ZstdVarInts.readAt(data, cursor[0], cursor);
            assertTrue(len > 0, "内层包长必须为正");
            assertTrue(cursor[0] + len <= data.length, "内层包长不得越界");
            byte[] p = new byte[len];
            System.arraycopy(data, cursor[0], p, 0, len);
            cursor[0] += len;
            out.add(p);
        }
        return out.toArray(new byte[0][]);
    }

    private static void assertRejected(ByteBuf frame) {
        EmbeddedChannel in = new EmbeddedChannel(new TestDecoder(null));
        in.writeInbound(frame);
        in.runPendingTasks();
        assertNull(in.readInbound(), "畸形帧不得产出任何包");
        assertFalse(in.isOpen(), "畸形帧必须 fail-fast 关闭连接（旧实现会静默僵死）");
        in.finishAndReleaseAll();
    }

    /** 读一帧出站数据（直存路径同步可得）。 */
    private static ByteBuf readFrame(EmbeddedChannel ch) {
        Object o = ch.readOutbound();
        return o == null ? null : (ByteBuf) o;
    }

    /** 读一帧出站数据，必要时等待压缩线程池 + event loop（压缩路径是异步的）。 */
    private static ByteBuf readFrame(EmbeddedChannel ch, long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            ch.runPendingTasks();
            Object o = ch.readOutbound();
            if (o != null) {
                return (ByteBuf) o;
            }
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        return null;
    }

    /** 造一个"编码器看到的完整包"字节数组（1 字节包 id + 负载）。 */
    private static byte[] fullPacket(byte[] payload) {
        byte[] out = new byte[payload.length + 1];
        out[0] = 0x2A;
        System.arraycopy(payload, 0, out, 1, payload.length);
        return out;
    }

    /** 编码器收到的就是"已含包 id 的裸包"，这里补一个单字节包 id。 */
    private static ByteBuf packet(byte[] payload) {
        ByteBuf buf = Unpooled.buffer();
        buf.writeByte(0x2A);
        buf.writeBytes(payload);
        return buf;
    }

    private static byte[] ascii(String s) {
        return s.getBytes(StandardCharsets.US_ASCII);
    }

    /** 高重复度数据：保证压缩确实变小。 */
    private static byte[] repetitive(int size) {
        byte[] b = new byte[size];
        for (int i = 0; i < size; i++) {
            b[i] = (byte) (i % 7);
        }
        return b;
    }

    // ────────────────────────────────────────────────────────────────
    // 测试专用子类（只差 writeBodyLen 这一个开关）
    // ────────────────────────────────────────────────────────────────

    private final class TestEncoder extends ZstdBatchEncoderBase {

        private final boolean bodyLen;
        private final int skip;
        private final int maxPackets;

        final List<int[]> frames = new ArrayList<>();
        boolean lastCompressed;
        int lastRaw;
        int lastWire;

        TestEncoder(boolean bodyLen, int skip, int maxPackets) {
            this.bodyLen = bodyLen;
            this.skip = skip;
            this.maxPackets = maxPackets;
        }

        @Override
        protected ZstdCompressCtx resolveCompressContext(ChannelHandlerContext ctx) {
            return compressCtx;
        }

        @Override
        protected boolean hasCompressDict() {
            return false;
        }

        @Override
        protected int skipCompressThreshold(boolean hasDict) {
            return skip;
        }

        @Override
        protected boolean writeBodyLen() {
            return bodyLen;
        }

        @Override
        protected void onFrame(int rawBytes, int wireBytes, boolean compressed) {
            frames.add(new int[]{rawBytes, wireBytes, compressed ? 1 : 0});
            lastCompressed = compressed;
            lastRaw = rawBytes;
            lastWire = wireBytes;
        }

        /** 窗口大到测试期间不会到期：成帧只发生在"攒够 max_packets"这一刻。 */
        @Override
        protected int configBatchWindowMs() {
            return NEVER_DUE_MS;
        }

        @Override
        protected int configBatchMaxPackets() {
            return maxPackets;
        }
    }

    private static final class TestDecoder extends ZstdBatchDecoderBase {

        private final ZstdDecompressCtx ctx;

        TestDecoder(ZstdDecompressCtx ctx) {
            this.ctx = ctx;
        }

        @Override
        protected ZstdDecompressCtx resolveDecompressContext(ChannelHandlerContext c) {
            return ctx;
        }

        @Override
        protected void onStoredFrame(int frameBodyBytes) {
        }

        @Override
        protected void onCompressedFrame(int inBytes, int outBytes) {
        }
    }
}
