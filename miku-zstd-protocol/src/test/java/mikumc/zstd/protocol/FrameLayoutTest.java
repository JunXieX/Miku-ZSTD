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

    @Test
    @DisplayName("压缩帧多包批：3 个包合成一帧压缩，解码按序还原")
    void compressedFrameMultiPacketRoundTrip() {
        // 现有一帧多包用例只覆盖直存路径；压缩路径的内层切包（splitAndEmit(byte[])）
        // 与直存路径是两份不同实现，必须单独验证。
        TestEncoder encoder = new TestEncoder(true, 0 /* 一律压缩 */, 3);
        EmbeddedChannel ch = new EmbeddedChannel(encoder);
        EmbeddedChannel in = new EmbeddedChannel(new TestDecoder(decompressCtx));

        byte[] p1 = repetitive(300);
        byte[] p2 = repetitive(500);
        byte[] p3 = repetitive(700);
        ch.writeOutbound(packet(p1));
        ch.writeOutbound(packet(p2));
        ch.writeOutbound(packet(p3));

        ByteBuf frame = readFrame(ch, 5000);
        assertNotNull(frame, "压缩走线程池，需要等待回到 event loop");
        assertTrue(encoder.lastCompressed, "应当走压缩路径");
        assertEquals(1, encoder.frames.size(), "三包应只产生一帧");

        ByteBuf probe = frame.duplicate();
        ZstdVarInts.tryRead(probe, Integer.MAX_VALUE); // bodyLen
        int rawSize = ZstdVarInts.tryRead(probe, Integer.MAX_VALUE);
        assertTrue(rawSize > 0, "压缩帧的 rawSize 应为原始长度（>0）");

        feed(in, frame);
        List<byte[]> got = drainInbound(in);
        assertEquals(3, got.size(), "解压后应还原成 3 个包（压缩帧多包批路径）");
        assertArrayEquals(fullPacket(p1), got.get(0), "第 1 个包");
        assertArrayEquals(fullPacket(p2), got.get(1), "第 2 个包");
        assertArrayEquals(fullPacket(p3), got.get(2), "第 3 个包");
    }

    // ────────────────────────────────────────────────────────────────
    // 原生上下文生命周期：关闭后池线程绝不能再碰 compressCtx
    // ────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("压缩上下文已关闭时：在途压缩任务必须干净放弃（不得触碰 native ctx，也不得丢 promise）")
    void compressionAbortsSafelyWhenContextClosed() throws InterruptedException {
        // 回归用例：压缩任务可能"在连接断开前提交、在 ChannelManager#close() 释放 native
        // 上下文之后才轮到执行"。若那时仍调用 compressCtx，就是 use-after-free → SIGSEGV
        // （整进程崩，不止这条连接），而在纯 JVM 测试里它会表现为 JVM 崩溃或 native 报错。
        TestEncoder encoder = new TestEncoder(true, 0 /* 一律压缩 */, 1);
        EmbeddedChannel ch = new EmbeddedChannel(encoder);
        // 模拟"连接已断、上下文已关"：必须在写入之前置位，让池线程拿到锁后立刻放弃。
        encoder.lock.markClosed();

        // ⚠️ 用 ch.write(...) 而不是 ch.writeOutbound(...)：后者的返回值是 boolean，
        // 拿不到本次写的 promise —— 而本用例要断言的正是"promise 必须被完成"。
        io.netty.channel.ChannelFuture f = ch.write(packet(repetitive(4096)));
        ch.flush();
        // 放弃结论由池线程经 eventLoop 回投，嵌入式 channel 需要手动推进任务队列。
        long deadline = System.currentTimeMillis() + 3000;
        while (!f.isDone() && System.currentTimeMillis() < deadline) {
            ch.runPendingTasks();
            Thread.sleep(2);
        }

        assertTrue(f.isDone(), "写 promise 必须被完成（否则上游永久挂起）");
        assertFalse(f.isSuccess(), "上下文已关闭时该批必须以 ClosedChannelException 失败化");
        assertTrue(encoder.frames.isEmpty(), "放弃的批次不得产出任何帧");
        assertNull(readFrame(ch), "不得有帧写出");
        ch.finishAndReleaseAll();
    }

    @Test
    @DisplayName("编码器移除后：在途压缩回调不得向仍活跃的 pipeline 写出帧")
    void removedEncoderDoesNotWriteInflightFrame() throws InterruptedException {
        TestEncoder encoder = new TestEncoder(true, 0 /* 一律压缩 */, 1);
        EmbeddedChannel ch = new EmbeddedChannel(encoder);

        io.netty.channel.ChannelFuture writeFuture;
        synchronized (encoder.lock) {
            writeFuture = ch.write(packet(repetitive(4096)));
            ch.flush();
            // 持锁期间提交压缩任务：池线程必然阻塞在 lock 上，此时移除 handler
            // 才确定覆盖「压缩已在途、回调尚未回到 event loop」这一竞态。
            ch.pipeline().remove(encoder);
        }

        long deadline = System.currentTimeMillis() + 3000;
        while (!writeFuture.isDone() && System.currentTimeMillis() < deadline) {
            ch.runPendingTasks();
            Thread.sleep(2);
        }
        // handlerRemoved 已失败化 promise，但池线程此刻可能仍在压缩（或正阻塞在这把锁上）。
        // 拿一次锁并 markClosed：若任务尚未进入压缩，它会读到标志直接放弃；若正在压缩，
        // 这里会等它结束。两种顺序下 @AfterEach 关闭 native 上下文都不会踩到 use-after-free。
        synchronized (encoder.lock) {
            encoder.lock.markClosed();
        }
        ch.runPendingTasks();

        assertTrue(writeFuture.isDone(), "handlerRemoved 必须完成在途 promise");
        assertFalse(writeFuture.isSuccess(), "被移除 handler 持有的写必须失败");
        assertTrue(encoder.frames.isEmpty(), "handler 移除后不允许迟到的压缩帧写入 pipeline");
        assertNull(readFrame(ch), "不应有迟到的出站帧");
        ch.finishAndReleaseAll();
    }

    @Test
    @DisplayName("编码器按包边界拆分批次，避免超过 MC 21-bit 外层帧上限")
    void splitsBatchesAtFrameBudget() throws InterruptedException {
        // 用真实生产预算（无需测试钩子）：单个包 ~1.1MB 合法，两个合起来
        // （2,200,008B）必然突破 2,097,141B 的 raw 预算，因此必须拆成两帧。
        // 若拆分逻辑失效，它们会被塞进同一帧 —— 帧数 1，且外层长度超出 21-bit 上限。
        final int payload = 1_100_000;
        // 两个包就攒满 max_packets → 立即成帧，正好在成帧点验证预算拆分
        TestEncoder encoder = new TestEncoder(true, Integer.MAX_VALUE, 2);
        EmbeddedChannel ch = new EmbeddedChannel(encoder);
        ch.write(packet(new byte[payload]));
        ch.write(packet(new byte[payload]));
        long deadline = System.currentTimeMillis() + 3000;
        while (encoder.frames.size() < 2 && System.currentTimeMillis() < deadline) {
            ch.runPendingTasks();
            Thread.sleep(1);
        }

        int frames = 0;
        ByteBuf frame;
        while ((frame = readFrame(ch)) != null) {
            frames++;
            assertTrue(frame.readableBytes() <= ZstdVarInts.MC_MAX_FRAME_BYTES,
                    "每帧都必须处于 MC 21-bit 外层长度上限内");
            frame.release();
        }
        assertEquals(2, frames, "超出字节预算的包必须另起一帧，而不是合并");
        int perPacket = payload + 1 + ZstdVarInts.length(payload + 1);
        assertEquals(2 * (1 + perPacket), encoder.frames.stream().mapToInt(v -> v[0]).sum(),
                "两帧各自的 rawSize 字段都要计入");
        ch.finishAndReleaseAll();
    }

    @Test
    @DisplayName("单个大包（原始超 21-bit、压缩后很小）必须正常发出，不得按原始大小拒绝")
    void largeCompressiblePacketIsNotRejected() throws InterruptedException {
        // 回归用例：曾把 MC 的 21-bit 帧上限直接套在**原始**字节上，于是配置阶段
        // 数 MB 的 Registry 包会在编码器里被丢弃、promise 失败 → 玩家进不了服。
        // 实际上限约束的是压缩后的线路帧，这类包压缩后只有几百 KB。
        final int payload = 4 * 1024 * 1024;
        TestEncoder encoder = new TestEncoder(true, 0 /* 一律压缩 */, 1);
        EmbeddedChannel ch = new EmbeddedChannel(encoder);
        io.netty.channel.ChannelFuture f = ch.write(packet(repetitive(payload)));
        ch.flush();

        long deadline = System.currentTimeMillis() + 5000;
        while (!f.isDone() && System.currentTimeMillis() < deadline) {
            ch.runPendingTasks();
            Thread.sleep(2);
        }
        assertTrue(f.isDone(), "写 promise 必须被完成");
        assertTrue(f.isSuccess(), "大包压缩后远小于帧上限，必须成功发出而不是被拒绝");

        ByteBuf frame = readFrame(ch, 5000);
        assertNotNull(frame, "应当产出一帧");
        assertTrue(frame.readableBytes() <= ZstdVarInts.MC_MAX_FRAME_BYTES,
                "线路帧必须处于 21-bit 上限内");
        assertTrue(encoder.lastCompressed, "应当走压缩路径");

        EmbeddedChannel in = new EmbeddedChannel(new TestDecoder(decompressCtx));
        feed(in, frame);
        List<byte[]> got = drainInbound(in);
        assertEquals(1, got.size(), "应还原成 1 个包");
        assertEquals(payload + 1, got.get(0).length, "包内容长度应与原始一致");
        in.finishAndReleaseAll();
        ch.finishAndReleaseAll();
    }

    @Test
    @DisplayName("批次拆分后仍能正常工作：拆分出的多帧顺序与内容都不丢")
    void splitBatchesPreserveOrderAndContent() throws InterruptedException {
        // 与 splitsBatchesAtFrameBudget 互补：那条验证"必须拆开"，这条验证
        // "拆开之后内容与顺序仍然正确"（拆分逻辑直接操作 pending 下标，最容易错位）。
        // 用两个 1.1MB 包：合计超软预算，所以第二个包必然被推到下一帧。
        // maxPackets=2 才会在第二个包写入时立即触发成帧（测试窗口是 60s，永不到期）。
        final int payload = 1_100_000;
        byte[] big1 = repetitive(payload);
        byte[] big2 = repetitive(payload);
        big2[0] = 0x55; // 让两包内容不同，便于验证顺序
        TestEncoder encoder = new TestEncoder(true, Integer.MAX_VALUE, 2);
        EmbeddedChannel ch = new EmbeddedChannel(encoder);
        EmbeddedChannel in = new EmbeddedChannel(new TestDecoder(decompressCtx));
        ch.write(packet(big1));
        ch.write(packet(big2));
        ch.flush();

        // EmbeddedChannel 不会自动推进定时任务：必须循环 runPendingTasks 让后续批次跑完。
        long deadline = System.currentTimeMillis() + 3000;
        while (encoder.frames.size() < 2 && System.currentTimeMillis() < deadline) {
            ch.runPendingTasks();
            Thread.sleep(2);
        }

        int frames = 0;
        List<byte[]> got = new ArrayList<>();
        ByteBuf frame;
        while ((frame = readFrame(ch)) != null) {
            frames++;
            feed(in, frame);
            got.addAll(drainInbound(in));
        }
        assertTrue(frames >= 2, "两个大包应至少拆成两帧，实际 " + frames);
        assertEquals(2, got.size(), "拆批不得丢包或重复：" + got.size());
        assertEquals(payload + 1, got.get(0).length, "第 1 个包应是第一个大包");
        assertEquals(payload + 1, got.get(1).length, "第 2 个包应是第二个大包");
        assertEquals((byte) 0x55, got.get(1)[1], "顺序必须保持：第二个包的负载首字节应来自 big2");
        in.finishAndReleaseAll();
        ch.finishAndReleaseAll();
    }

    /**
     * 切换窗口内的原版帧兼容（服务端专用）。
     *
     * <p>背景：没有字典时，服务端收到 negotiate 应答就会激活，而客户端此时可能还没处理到
     * SetCompression，仍在发原版帧。原版未压缩帧 {@code [0][裸包]} 与 zstd 直存帧同形，
     * 不加区分会切出垃圾、让 Minecraft 解码器抛 {@code A packet frame decoder failed}。</p>
     */
    @Test
    @DisplayName("切换窗口：原版未压缩帧（[0][裸包]）必须按原版解释，而不是当成 zstd 直存帧切包")
    void legacyUncompressedFrameAcceptedDuringSwitchWindow() {
        TestDecoder decoder = new TestDecoder(decompressCtx, true);
        EmbeddedChannel in = new EmbeddedChannel(decoder);

        byte[] packet = fullPacket(ascii("legacy-login-ack"));
        ByteBuf frame = Unpooled.buffer();
        ZstdVarInts.write(frame, 0);        // dataLength = 0 → 未压缩
        frame.writeBytes(packet);
        in.writeInbound(frame);

        List<byte[]> got = drainInbound(in);
        assertEquals(1, got.size(), "必须还原成 1 个包");
        assertArrayEquals(packet, got.get(0), "内容必须与原版包完全一致");
        assertTrue(in.isOpen(), "切换窗口内的原版帧不得关闭连接");
        in.finishAndReleaseAll();
    }

    @Test
    @DisplayName("切换窗口：原版 zlib 压缩帧（[rawLen][deflate]）必须能用 zlib 解开")
    void legacyZlibFrameAcceptedDuringSwitchWindow() {
        TestDecoder decoder = new TestDecoder(decompressCtx, true);
        EmbeddedChannel in = new EmbeddedChannel(decoder);

        // 帧体 = [rawLen][raw deflate(payload)] 之后再紧跟一个原版未压缩帧的 [0]。
        // 这正是真实管线里的"混批"形态：前置 frame-decoder 只按外层长度切帧，
        // 下一帧的裸包会被一起带进本帧的帧体。
        byte[] packet = fullPacket(repetitive(300));
        byte[] deflated = rawDeflate(packet);

        ByteBuf frame = Unpooled.buffer();
        ZstdVarInts.write(frame, packet.length);
        frame.writeBytes(deflated);
        ZstdVarInts.write(frame, 0);
        in.writeInbound(frame);

        List<byte[]> got = drainInbound(in);
        assertEquals(1, got.size(), "混批帧只能还原出压缩的那 1 个包");
        assertArrayEquals(packet, got.get(0), "压缩帧内容必须与原版包完全一致");
        assertTrue(in.isOpen(), "合法原版帧不得关闭连接");
        in.finishAndReleaseAll();
    }

    @Test
    @DisplayName("切换窗口：真·原版压缩帧（rawLen 即解压长度）必须整包还原，不得切包")
    void legacyFullZlibFrameAcceptedDuringSwitchWindow() {
        TestDecoder decoder = new TestDecoder(decompressCtx, true);
        EmbeddedChannel in = new EmbeddedChannel(decoder);

        byte[] packet = fullPacket(repetitive(600));
        byte[] deflated = rawDeflate(packet);

        ByteBuf frame = Unpooled.buffer();
        ZstdVarInts.write(frame, packet.length); // 原版：rawLen 是解压后长度
        frame.writeBytes(deflated);
        in.writeInbound(frame);

        List<byte[]> got = drainInbound(in);
        assertEquals(1, got.size(), "必须还原成 1 个包（不得按 zstd 切包）");
        assertArrayEquals(packet, got.get(0), "内容必须与原版包完全一致");
        assertTrue(in.isOpen(), "合法原版帧不得关闭连接");
        in.finishAndReleaseAll();
    }

    @Test
    @DisplayName("切换窗口：一旦见过合法 zstd 帧，畸形帧仍必须被严格拒绝")
    void legacyFallbackStopsAfterFirstZstdFrame() {
        TestDecoder decoder = new TestDecoder(decompressCtx, true);
        EmbeddedChannel in = new EmbeddedChannel(decoder);

        // 先来一个合法 zstd 直存帧
        byte[] packet = fullPacket(ascii("zstd-packet"));
        ByteBuf ok = Unpooled.buffer();
        ZstdVarInts.write(ok, 0);
        ZstdVarInts.write(ok, packet.length);
        ok.writeBytes(packet);
        in.writeInbound(ok);
        assertEquals(1, drainInbound(in).size(), "首个合法 zstd 帧应正常还原");

        // 再来一个内层长度越界的畸形帧：不得被当成"原版帧"放行
        ByteBuf bad = Unpooled.buffer();
        ZstdVarInts.write(bad, 0);
        ZstdVarInts.write(bad, 999);
        bad.writeBytes(ascii("short"));
        in.writeInbound(bad);
        in.runPendingTasks();
        assertNull(in.readInbound(), "见过 zstd 帧后不得再回退");
        assertFalse(in.isOpen(), "畸形帧必须 fail-fast 关闭连接");
        in.finishAndReleaseAll();
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
    @DisplayName("边界：帧头 rawSize 上界为 32MB（含）——32MB-1 与 32MB 合法，32MB+1 非法")
    void rawSizeUpperBoundIsInclusive() {
        // 显式钉死边界语义：tryRead 的判定是 result > maxValue，故上界值本身合法（含）。
        // 不写死它，极易在别处按"32MB-1 才是上界"写出 off-by-one。
        ByteBuf below = Unpooled.buffer();
        ZstdVarInts.write(below, ZstdVarInts.DEFAULT_MAX_VALUE - 1);
        assertEquals(ZstdVarInts.DEFAULT_MAX_VALUE - 1,
                ZstdVarInts.tryRead(below, ZstdVarInts.DEFAULT_MAX_VALUE), "32MB-1 合法");
        below.release();

        ByteBuf atLimit = Unpooled.buffer();
        ZstdVarInts.write(atLimit, ZstdVarInts.DEFAULT_MAX_VALUE);
        assertEquals(ZstdVarInts.DEFAULT_MAX_VALUE,
                ZstdVarInts.tryRead(atLimit, ZstdVarInts.DEFAULT_MAX_VALUE), "32MB 本身合法（含上界）");
        atLimit.release();

        ByteBuf over = Unpooled.buffer();
        ZstdVarInts.write(over, ZstdVarInts.DEFAULT_MAX_VALUE + 1);
        assertEquals(ZstdVarInts.INVALID,
                ZstdVarInts.tryRead(over, ZstdVarInts.DEFAULT_MAX_VALUE), "32MB+1 非法");
        over.release();
    }

    @Test
    @DisplayName("拒绝：空帧体（只有帧头没有负载）")
    void rejectEmptyFrameBody() {
        ByteBuf buf = Unpooled.buffer();
        ZstdVarInts.write(buf, 0);
        assertRejected(buf);
    }

    @Test
    @DisplayName("拒绝：放大攻击（声明 32MB 却只给 1 字节）")
    void rejectAmplificationAttack() {
        ByteBuf buf = Unpooled.buffer();
        // 声明 32MB（帧长度上限）而压缩体只有 1 字节：远超"保底额度 8MB"，必须拦下。
        // 注意不能用 1MB —— 8MB 以内的声明是合法范围（见下面的接受用例）。
        ZstdVarInts.write(buf, ZstdVarInts.DEFAULT_MAX_VALUE);
        buf.writeByte(0x00);
        assertRejected(buf);
    }

    @Test
    @DisplayName("接受：合法的极高压缩比帧（445KB → 数百字节）不得被判成放大攻击")
    void acceptsLegitimatelyHighCompressionRatio() {
        // 回归用例：2026-09-27 生产事故 —— limbo 空区块数据把 445524 字节压成 471 字节（约 945:1），
        // 被 256KB 的保底额度误判为放大攻击而关闭连接，玩家秒退。
        byte[] payload = new byte[445_524];
        ByteBuf raw = Unpooled.buffer();
        ZstdVarInts.write(raw, payload.length);
        raw.writeBytes(payload); // 全 0：等价于虚空世界的空区块，压缩比可达 ~1000:1
        byte[] body = new byte[raw.readableBytes()];
        raw.getBytes(raw.readerIndex(), body);
        raw.release();

        byte[] compressed = compressCtx.compress(body);
        assertTrue(compressed.length < 4096,
                "用例前提是高压缩比，实际压缩后 " + compressed.length + "B");

        ByteBuf frame = Unpooled.buffer();
        ZstdVarInts.write(frame, body.length); // 声明解压后长度
        frame.writeBytes(compressed);

        EmbeddedChannel in = new EmbeddedChannel(new TestDecoder(decompressCtx));
        in.writeInbound(frame);
        List<byte[]> got = drainInbound(in);
        assertEquals(1, got.size(), "合法的高压缩比帧必须被还原成 1 个包");
        // ⚠️ 比的是"包"的长度（帧体内层长度前缀之后的内容），不是帧体长度——后者还多一个 pktLen 前缀
        assertEquals(payload.length, got.get(0).length);
        assertTrue(in.isOpen(), "不得因为高压缩比而关闭连接");
        in.finishAndReleaseAll();
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
    @DisplayName("拒绝：直存帧内层 pktLen 超过 32MB 上限（走 INVALID 而非整数溢出）")
    void rejectStoredInnerLengthOverMax() {
        // 直存帧的内层 pktLen 由 tryRead 以 DEFAULT_MAX_VALUE（32MB）为上限，
        // 因此 0x7FFFFFFF 会被直接判成 INVALID 并干净拒绝 —— 这条路径**不存在** int 溢出
        //（压缩帧那一侧用的是不设上限的 readAt，才需要 long 相加，见下一个用例）。
        // 本用例钉死这一差异，避免日后有人"顺手"把两侧改成同一套判据而引入回归。
        ByteBuf frame = Unpooled.buffer();
        ZstdVarInts.write(frame, 0);                  // rawSize = 0 → 直存帧
        ZstdVarInts.write(frame, Integer.MAX_VALUE);  // 内层 pktLen = 0x7FFFFFFF > 32MB
        frame.writeBytes(ascii("short"));
        assertRejected(frame);
    }

    @Test
    @DisplayName("拒绝：压缩帧解压后内层 pktLen 巨大（0x7FFFFFFF，整数溢出回归用例）")
    void rejectCompressedInnerLengthOverflow() {
        // 回归用例（S1 同型缺陷）：解压后的负载内层声明约 2GB。若越界判定用 int 相加，
        // off + pktLen 会溢出成负数而被放行，进而按 2GB 去切片。
        byte[] payload = new byte[1029];
        payload[0] = (byte) 0xFF;
        payload[1] = (byte) 0xFF;
        payload[2] = (byte) 0xFF;
        payload[3] = (byte) 0xFF;
        payload[4] = 0x07; // varint(0x7FFFFFFF)：内层 pktLen
        byte[] compressed = compressCtx.compress(payload);
        assertTrue(compressed.length < payload.length,
                "用例前提：压缩后更小，才不会被「压缩体大于 rawSize」的前置检查拦下");

        ByteBuf frame = Unpooled.buffer();
        ZstdVarInts.write(frame, payload.length); // 声明解压后长度
        frame.writeBytes(compressed);

        TestDecoder decoder = new TestDecoder(decompressCtx);
        EmbeddedChannel in = new EmbeddedChannel(decoder);
        in.writeInbound(frame);
        in.runPendingTasks();

        assertNull(in.readInbound(), "内层 pktLen 越界的压缩帧不得产出任何包");
        assertFalse(in.isOpen(), "必须 fail-fast 关闭连接");
        assertNull(decoder.lastError,
                "越界必须走 protocolError 干净拒绝；非 null 说明 int 溢出让判定失效、"
                        + "改由 IndexOutOfBoundsException 兜住");
        in.finishAndReleaseAll();
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

    /**
     * 用 <b>raw deflate</b>（{@code nowrap = true}）压缩：与 Minecraft 原版
     * {@code new Deflater(level, true)} / {@code new Inflater(true)} 一致。
     */
    private static byte[] rawDeflate(byte[] data) {
        java.io.ByteArrayOutputStream bos = new java.io.ByteArrayOutputStream();
        java.util.zip.Deflater deflater = new java.util.zip.Deflater(
                java.util.zip.Deflater.DEFAULT_COMPRESSION, true);
        try {
            deflater.setInput(data);
            deflater.finish();
            byte[] chunk = new byte[512];
            while (!deflater.finished()) {
                int n = deflater.deflate(chunk);
                bos.write(chunk, 0, n);
            }
        } finally {
            deflater.end();
        }
        return bos.toByteArray();
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
        final ZstdCompressLock lock = new ZstdCompressLock();

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
        protected ZstdCompressLock compressLock() {
            return lock;
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

        /**
         * 记录「经由异常路径关闭」的原因。正常拒绝走 {@code protocolError}（WARN + close），
         * 不会置位；只有越界判定被绕过、抛出 IndexOutOfBoundsException 之类时才非 null。
         */
        Throwable lastError;

        private final boolean legacy;

        TestDecoder(ZstdDecompressCtx ctx) {
            this(ctx, false);
        }

        TestDecoder(ZstdDecompressCtx ctx, boolean legacy) {
            this.ctx = ctx;
            this.legacy = legacy;
        }

        @Override
        protected boolean acceptsLegacyFrames() {
            return legacy;
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext c, Throwable cause) {
            lastError = cause;
            super.exceptionCaught(c, cause);
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
