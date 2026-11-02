package mikumc.zstd.protocol;

import com.github.luben.zstd.EndDirective;
import com.github.luben.zstd.ZstdCompressCtx;
import com.github.luben.zstd.ZstdDecompressCtx;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.util.ReferenceCountUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * FRAME 8 的真实共享编解码器契约。子类只接入连接上下文、统计与外层长度归属。
 * TCP 分片只交给独立的 MC splitter；被测 decoder 始终收到一个完整运输帧体。
 * 独立 native 夹具只构造跨运输帧/畸形内层输入，不复刻生产的成批或切包实现。
 */
class FrameLayoutTest {
    private static final int NEVER_DUE_MS = 60_000;
    private static final long TIMEOUT_MS = 10_000;
    private final List<EmbeddedChannel> channels = new ArrayList<>();
    private final List<TestEncoder> encoders = new ArrayList<>();
    private final List<ZstdDecompressCtx> decoders = new ArrayList<>();

    @BeforeEach
    void setUp() throws Exception {
        stopPool();
        ZstdCompressPool.init(2, 5);
    }

    @AfterEach
    void tearDown() throws Exception {
        // 先停接收、等所有已接收 worker 退出，再释放 native ctx；不拿 event loop 等锁。
        stopPool();
        for (EmbeddedChannel channel : channels) pump(channel);
        for (EmbeddedChannel channel : channels) channel.finishAndReleaseAll();
        for (TestEncoder encoder : encoders) {
            synchronized (encoder.lock) {
                encoder.lock.markClosed();
                encoder.nativeCtx.close();
            }
        }
        for (ZstdDecompressCtx ctx : decoders) ctx.close();
    }

    @Test
    @DisplayName("decoder 的输入类型固定为完整运输帧，不再自行累积 TCP")
    void decoderRetainsTransportFrameBoundaries() {
        assertTrue(MessageToMessageDecoder.class.isAssignableFrom(ZstdBatchDecoderBase.class));
        assertFalse(ByteToMessageDecoder.class.isAssignableFrom(ZstdBatchDecoderBase.class));
    }

    @Test
    @DisplayName("多次 flush 共享同一 native 流；所有小包也压缩，第二批确实引用历史")
    void repeatedFlushesKeepHistoryAndAllPacketsEnterStream() {
        TestEncoder encoder = encoder(true, 64, NEVER_DUE_MS);
        EmbeddedChannel out = channel(encoder);
        TestDecoder decoder = decoder();
        EmbeddedChannel in = channel(new OuterSplitter(), decoder);
        byte[] history = randomBytes(48 * 1024, 20261102L);
        int firstBody = 0;
        for (int i = 0; i < 16; i++) {
            byte[] packet = i == 0 || i == 1 ? history : randomBytes(1 + i, i);
            ChannelFuture sent = out.write(input(packet, i % 3));
            out.flush();
            await(out, sent::isDone);
            assertTrue(sent.isSuccess(), String.valueOf(sent.cause()));
            List<byte[]> bodies = takeBodies(out, true);
            assertFalse(bodies.isEmpty());
            int bodyBytes = bodies.stream().mapToInt(b -> b.length).sum();
            if (i == 0) firstBody = bodyBytes;
            if (i == 1) assertTrue(bodyBytes < firstBody / 2, "第二次相同随机包须真实使用前批历史");
            for (byte[] body : bodies) feedTcp(in, body, 17L + i);
            assertPackets(in, List.of(packet));
            assertTrue(in.isOpen());
        }
        assertEquals(16, encoder.frames.size(), "统计按完成批而非运输帧回调");
        assertTrue(encoder.frames.stream().allMatch(f -> f[2] == 1), "没有直存或压后回退路径");
        assertTrue(encoder.workerThreads.stream().allMatch(n -> n.startsWith("miku-zstd-compress-")));
        assertTrue(encoder.workerThreads.stream().noneMatch(n -> n.equals(Thread.currentThread().getName())));
    }

    @Test
    @DisplayName("heap/direct/复合输入均从原 readerIndex 往返，且由worker持有原包")
    void inputKindsAndNonZeroReaderIndices() {
        for (int kind = 0; kind < 3; kind++) {
            TestEncoder encoder = encoder(true, 64, NEVER_DUE_MS);
            EmbeddedChannel out = channel(encoder);
            EmbeddedChannel in = channel(new OuterSplitter(), decoder());
            byte[] expected = randomBytes(80_003, kind + 7);
            ByteBuf original = input(expected, kind);
            ChannelFuture sent;
            synchronized (encoder.lock) {
                sent = out.write(original);
                out.flush();
                assertTrue(original.refCnt() > 0, "排队worker仍须拥有原包，不得先拼接并释放输入");
                assertFalse(sent.isDone());
            }
            await(out, sent::isDone);
            assertTrue(sent.isSuccess(), String.valueOf(sent.cause()));
            await(out, () -> original.refCnt() == 0);
            for (byte[] body : takeBodies(out, true)) feedTcp(in, body, 901 + kind);
            assertPackets(in, List.of(expected));
        }
    }

    @Test
    @DisplayName("writeBodyLen 只改变 MC 外长；body 是逐字节相同的纯 magicless 连续流")
    void bodyLengthOwnershipDoesNotChangeCompressedBytesOrStats() {
        TestEncoder with = encoder(true, 64, NEVER_DUE_MS);
        TestEncoder without = encoder(false, 64, NEVER_DUE_MS);
        EmbeddedChannel a = channel(with);
        EmbeddedChannel b = channel(without);
        TestDecoder da = decoder();
        TestDecoder db = decoder();
        EmbeddedChannel ia = channel(new OuterSplitter(), da);
        EmbeddedChannel ib = channel(new OuterSplitter(), db);
        for (int i = 0; i < 3; i++) {
            byte[] packet = randomBytes(4_000 + i, 22 + i);
            ChannelFuture fa = a.write(input(packet, 0));
            ChannelFuture fb = b.write(input(packet, 1));
            a.flush(); b.flush();
            await(a, fa::isDone); await(b, fb::isDone);
            assertTrue(fa.isSuccess()); assertTrue(fb.isSuccess());
            List<byte[]> ba = takeBodies(a, true);
            List<byte[]> bb = takeBodies(b, false);
            assertEquals(ba.size(), bb.size());
            long wire = 0;
            for (int j = 0; j < ba.size(); j++) {
                assertArrayEquals(ba.get(j), bb.get(j));
                wire += ZstdVarInts.length(ba.get(j).length) + ba.get(j).length;
                feedTcp(ia, ba.get(j), 30 + j);
                feedTcp(ib, bb.get(j), 90 + j);
            }
            assertPackets(ia, List.of(packet)); assertPackets(ib, List.of(packet));
            assertArrayEquals(with.frames.get(i), without.frames.get(i));
            assertEquals(packet.length + ZstdVarInts.length(packet.length), with.frames.get(i)[0]);
            assertEquals(wire, with.frames.get(i)[1], "Paper 形态也计入由prepender写的MC外长");
        }
        assertEquals(with.frames.stream().mapToInt(f -> f[1]).sum(), da.wire);
        assertEquals(da.wire, db.wire);
        assertEquals(with.frames.stream().mapToInt(f -> f[0]).sum(), da.raw);
    }

    @Test
    @DisplayName("独立splitter承接随机TCP切片，内层半VarInt和半包跨完整运输帧保留")
    void partialInnerVarIntAndPacketSurviveAcrossTransportFrames() {
        byte[] packet = randomBytes(32_789, 77);
        byte[] inner = inner(packet);
        try (NativeStream stream = new NativeStream()) {
            TestDecoder decoder = decoder();
            EmbeddedChannel in = channel(new OuterSplitter(), decoder);
            byte[] a = stream.flush(Arrays.copyOfRange(inner, 0, 1));
            feedTcp(in, a, 1);
            assertNull(in.readInbound(), "第一帧仅有packetLen首字节");
            byte[] b = stream.flush(Arrays.copyOfRange(inner, 1, 113));
            feedTcp(in, b, 2);
            assertNull(in.readInbound(), "第二帧已有包长但包体不完整");
            byte[] c = stream.flush(Arrays.copyOfRange(inner, 113, inner.length));
            feedTcp(in, c, 3);
            assertPackets(in, List.of(packet));
            assertTrue(in.isOpen());
            assertEquals(3, decoder.frames);
            assertEquals(inner.length, decoder.raw);
        }
    }

    @Test
    @DisplayName("decoder完整压缩体接受heap/direct/复合缓冲，不改变运输边界")
    void decoderInputKinds() {
        for (int kind = 0; kind < 3; kind++) {
            try (NativeStream stream = new NativeStream()) {
                byte[] expected = randomBytes(17_019, 100 + kind);
                byte[] compressed = stream.flush(inner(expected));
                EmbeddedChannel in = channel(decoder());
                ByteBuf body = input(compressed, kind);
                in.writeInbound(body);
                assertPackets(in, List.of(expected));
                assertEquals(0, body.refCnt());
            }
        }
    }

    @Test
    @DisplayName("不可压缩大包按MC体上限分运输帧，单包仍完整且不回退原文")
    void largeIncompressiblePacketSpansBoundedTransportFrames() {
        TestEncoder encoder = encoder(true, 64, NEVER_DUE_MS);
        EmbeddedChannel out = channel(encoder);
        TestDecoder decoder = decoder();
        EmbeddedChannel in = channel(new OuterSplitter(), decoder);
        byte[] expected = randomBytes(3 * 1024 * 1024 + 13, 104);
        ChannelFuture sent = out.write(input(expected, 2));
        out.flush(); await(out, sent::isDone);
        assertTrue(sent.isSuccess(), String.valueOf(sent.cause()));
        List<byte[]> bodies = takeBodies(out, true);
        assertTrue(bodies.size() >= 2, "超2MiB不可压缩包必须分运输帧，不能拒包或直存");
        for (byte[] body : bodies) {
            assertTrue(body.length > 0 && body.length <= ZstdVarInts.MC_MAX_FRAME_BYTES);
            feedTcp(in, body, body.length);
        }
        assertPackets(in, List.of(expected));
        assertEquals(bodies.size(), decoder.frames);
        assertEquals(1, encoder.frames.size());
        assertEquals(1, encoder.frames.getFirst()[2]);
    }

    @Test
    @DisplayName("32MiB单包上限包含端点；长度前缀额外开销允许独占批")
    void maximumPacketAndBatchPrefixAllowance() {
        byte[] expected = new byte[ZstdVarInts.DEFAULT_MAX_VALUE];
        for (int i = 0; i < expected.length; i++) expected[i] = (byte) (i % 251);
        TestEncoder encoder = encoder(true, 64, NEVER_DUE_MS);
        EmbeddedChannel out = channel(encoder);
        EmbeddedChannel in = channel(new OuterSplitter(), decoder());
        ChannelFuture sent = out.write(input(expected, 1));
        out.flush(); await(out, sent::isDone);
        assertTrue(sent.isSuccess(), String.valueOf(sent.cause()));
        for (byte[] body : takeBodies(out, true)) feedTcp(in, body, 80);
        assertPackets(in, List.of(expected));
        assertEquals(expected.length + ZstdVarInts.length(expected.length), encoder.frames.getFirst()[0]);
    }

    @Test
    @DisplayName("空flush不向流写空运输体，也不增加统计")
    void emptyFlushIsHarmlessBeforeBetweenAndAfterPackets() {
        TestEncoder encoder = encoder(true, 64, NEVER_DUE_MS);
        EmbeddedChannel out = channel(encoder);
        out.flush(); out.flush(); pump(out);
        assertNull(out.readOutbound()); assertTrue(encoder.frames.isEmpty());
        EmbeddedChannel in = channel(new OuterSplitter(), decoder());
        for (int i = 0; i < 2; i++) {
            byte[] expected = {(byte) (i + 1)};
            ChannelFuture sent = out.write(input(expected, 0));
            out.flush(); await(out, sent::isDone);
            for (byte[] body : takeBodies(out, true)) feedTcp(in, body, 55);
            assertPackets(in, List.of(expected));
            out.flush(); out.flush(); pump(out);
            assertNull(out.readOutbound());
        }
        assertEquals(2, encoder.frames.size());
    }

    @Test
    @DisplayName("仅write的兜底定时器完成真实流压缩，不需上游flush")
    void writeOnlyFallbackFlushes() {
        TestEncoder encoder = encoder(true, 64, 1);
        EmbeddedChannel out = channel(encoder);
        EmbeddedChannel in = channel(new OuterSplitter(), decoder());
        byte[] expected = randomBytes(401, 1);
        ChannelFuture sent = out.write(input(expected, 0));
        out.advanceTimeBy(2, TimeUnit.MILLISECONDS);
        out.runScheduledPendingTasks();
        await(out, sent::isDone);
        assertTrue(sent.isSuccess());
        for (byte[] body : takeBodies(out, true)) feedTcp(in, body, 10);
        assertPackets(in, List.of(expected));
    }

    @Test
    @DisplayName("真实控制消息屏障等待多批pending，afterBarrier仍严格按原序")
    void barrierWaitsForAllPendingBatchesAndAfterBarrier() {
        TestEncoder encoder = encoder(true, 2, NEVER_DUE_MS);
        EmbeddedChannel out = channel(encoder);
        EmbeddedChannel in = channel(new OuterSplitter(), decoder());
        Object barrier = new Object();
        List<ChannelFuture> futures = new ArrayList<>();
        List<byte[]> before = new ArrayList<>();
        List<byte[]> after = new ArrayList<>();
        synchronized (encoder.lock) {
            for (int i = 0; i < 7; i++) {
                byte[] packet = randomBytes(1300 + i, i);
                before.add(packet); futures.add(out.write(input(packet, i % 3)));
            }
            futures.add(out.write(barrier));
            for (int i = 0; i < 5; i++) {
                byte[] packet = randomBytes(1400 + i, 50 + i);
                after.add(packet); futures.add(out.write(input(packet, i % 3)));
            }
            out.flush(); assertNull(out.readOutbound(), "锁住在途批时屏障也须排队");
        }
        await(out, () -> futures.stream().allMatch(ChannelFuture::isDone));
        assertTrue(futures.stream().allMatch(ChannelFuture::isSuccess));
        boolean crossed = false;
        Object message;
        List<byte[]> gotBefore = new ArrayList<>();
        List<byte[]> gotAfter = new ArrayList<>();
        while ((message = out.readOutbound()) != null) {
            if (message == barrier) {
                assertFalse(crossed); crossed = true;
                gotBefore.addAll(drain(in));
                assertPacketLists(before, gotBefore);
            } else {
                ByteBuf frame = (ByteBuf) message;
                try { feedTcp(in, bodyBytes(frame, true), 19); }
                finally { frame.release(); }
                if (crossed) gotAfter.addAll(drain(in));
            }
        }
        assertTrue(crossed, "真实非ByteBuf消息必须恰好经过一次");
        assertPacketLists(after, gotAfter);
    }

    @Test
    @DisplayName("C1 promise同步flush/write重入时，后续P1/C2仍保序且每批仅提交一次")
    void controlPromiseReentryPreservesPacketsAndMultipleBarriers() throws Exception {
        TestEncoder encoder = encoder(true, 1, NEVER_DUE_MS);
        IoProbe io = new IoProbe();
        EmbeddedChannel out = channel(io, encoder);
        EmbeddedChannel in = channel(new OuterSplitter(), decoder());
        Object c1 = new Object(); Object c2 = new Object();
        byte[] p0 = randomBytes(401, 301); byte[] p1 = randomBytes(503, 302);
        byte[] p2 = randomBytes(607, 303);
        ByteBuf first = input(p0, 1); ByteBuf queued = input(p1, 0); ByteBuf reentrant = input(p2, 2);
        ChannelPromise control = out.newPromise();
        List<ChannelFuture> injected = new ArrayList<>();
        List<Thread> listenerThreads = new ArrayList<>();
        List<Boolean> listenerInsideFlush = new ArrayList<>();
        control.addListener(future -> {
            listenerThreads.add(Thread.currentThread());
            listenerInsideFlush.add(io.flushDepth > 0);
            out.flush();
            injected.add(out.write(reentrant));
            out.flush();
        });
        List<ChannelFuture> futures = new ArrayList<>();
        try (PoolGate ignored = blockedPool()) {
            futures.add(out.write(first));
            futures.add(out.write(c1, control));
            futures.add(out.write(queued));
            futures.add(out.write(c2));
            out.flush();
            assertFalse(control.isDone()); assertNull(out.readOutbound());
        }
        await(out, () -> futures.stream().allMatch(ChannelFuture::isDone)
                && injected.size() == 1 && injected.getFirst().isDone());
        assertTrue(futures.stream().allMatch(ChannelFuture::isSuccess));
        assertTrue(injected.getFirst().isSuccess(), String.valueOf(injected.getFirst().cause()));
        assertEquals(List.of(Thread.currentThread()), listenerThreads, "控制promise监听必须真实同步重入主线程");
        assertEquals(List.of(true), listenerInsideFlush, "监听必须发生在真实运输flush尚未返回的调用栈内");
        List<Object> observed = new ArrayList<>();
        Object message;
        while ((message = out.readOutbound()) != null) {
            if (message == c1 || message == c2) {
                observed.add(message);
            } else {
                ByteBuf frame = (ByteBuf) message;
                try { feedTcp(in, bodyBytes(frame, true), 301); }
                finally { frame.release(); }
                observed.addAll(drain(in));
            }
        }
        assertEquals(5, observed.size(), "重复提交、漏包或重复控制消息都会改变真实解码序列");
        assertArrayEquals(p0, (byte[]) observed.get(0)); assertSame(c1, observed.get(1));
        assertArrayEquals(p1, (byte[]) observed.get(2)); assertSame(c2, observed.get(3));
        assertArrayEquals(p2, (byte[]) observed.get(4));
        assertEquals(3, encoder.frames.size()); assertEquals(3, encoder.workerThreads.size());
        assertEquals(3, encoder.finishedCallbacks, "成功promise之外还须等全部finishBatch回调退出");
        assertEquals(0, first.refCnt()); assertEquals(0, queued.refCnt()); assertEquals(0, reentrant.refCnt());
        assertTrue(out.isOpen()); assertNull(out.readOutbound());
    }

    @Test
    @DisplayName("原包使用真实voidPromise时，实际运输write失败仅关闭一次且归还全部ByteBuf")
    void voidPacketPromiseTransportFailureClosesOnceWithoutRecursion() {
        IoProbe io = new IoProbe(); HoldingWrites sink = new HoldingWrites();
        TestEncoder encoder = encoder(true, 1, NEVER_DUE_MS);
        List<Throwable> exceptions = new ArrayList<>();
        EmbeddedChannel out = channel(io, sink, new ChannelInboundHandlerAdapter() {
            @Override public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                // 放在encoder前观察真实voidPromise广播，再继续交给真实基类处理。
                exceptions.add(cause); ctx.fireExceptionCaught(cause);
            }
        }, encoder);
        WorkerReadInput original = new WorkerReadInput(input(randomBytes(80_013, 304), 1));
        ChannelPromise originalPromise = out.voidPromise();
        assertTrue(originalPromise.isVoid());
        out.write(original, originalPromise); out.flush();
        await(out, () -> !sink.promises.isEmpty());
        assertTrue(original.reads.get() > 0, "真实worker必须读取原包而非eventloop副本");
        assertTrue(sink.promises.stream().noneMatch(ChannelPromise::isVoid), "运输write使用真实非void promise");
        IllegalStateException failure = new IllegalStateException("actual void-packet transport write failure");
        assertDoesNotThrow(() -> sink.failRest(failure), "写失败不应StackOverflow或触发同步递归风暴");
        await(out, () -> !out.isOpen() && original.refCnt() == 0
                && sink.messages.stream().allMatch(buf -> buf.refCnt() == 0));
        pump(out);
        assertEquals(1, io.closes, "abort与channelInactive重入仅产生一次真实close");
        assertTrue(exceptions.isEmpty(), "voidPromise不应把失败递归广播为exceptionCaught: " + exceptions);
        assertTrue(sink.promises.stream().allMatch(ChannelPromise::isDone));
        assertTrue(sink.promises.stream().noneMatch(ChannelPromise::isSuccess));
        assertSame(failure, sink.promises.getFirst().cause());
        assertEquals(1, encoder.workerThreads.size()); assertEquals(1, encoder.finishedCallbacks);
        assertNull(out.readOutbound());
    }

    @Test
    @DisplayName("下游收到完整包同步关闭AUTO_READ后，decoder readComplete尊重背压不额外read")
    void deliveredPacketRespectsDownstreamAutoReadPause() {
        IoProbe readProbe = new IoProbe();
        EmbeddedChannel in = channel(readProbe, decoder(), new ChannelInboundHandlerAdapter() {
            @Override public void channelRead(ChannelHandlerContext ctx, Object message) {
                ctx.channel().config().setAutoRead(false);
                ctx.fireChannelRead(message);
            }
        });
        assertTrue(in.config().isAutoRead(), "必须由下游交包回调同步切换到手动读取");
        int readsBefore = readProbe.reads;
        byte[] packet = randomBytes(411, 305);
        try (NativeStream stream = new NativeStream()) {
            ByteBuf transport = Unpooled.wrappedBuffer(stream.flush(inner(packet)));
            in.writeInbound(transport); pump(in);
            assertPackets(in, List.of(packet));
            assertFalse(in.config().isAutoRead());
            assertEquals(readsBefore, readProbe.reads, "已真实交包，父类空out不能越过下游暂停额外read");
            assertEquals(0, transport.refCnt()); assertTrue(in.isOpen());
        }
    }

    @Test
    @DisplayName("AUTO_READ=false且只解出半VarInt或半包时，decoder readComplete主动续读一次")
    void partialVarIntAndPacketRequestManualReadUntilDelivery() {
        byte[] packet = randomBytes(32_789, 306); byte[] raw = inner(packet);
        for (int split : new int[]{1, ZstdVarInts.length(packet.length) + 37}) {
            IoProbe readProbe = new IoProbe(); TestDecoder decoder = decoder();
            EmbeddedChannel in = channel(readProbe, decoder);
            in.config().setAutoRead(false);
            int readsBefore = readProbe.reads;
            try (NativeStream stream = new NativeStream()) {
                ByteBuf incomplete = Unpooled.wrappedBuffer(stream.flush(Arrays.copyOfRange(raw, 0, split)));
                in.writeInbound(incomplete); pump(in);
                assertNull(in.readInbound(), "本运输帧只有半VarInt或半包，没有真实交包");
                assertEquals(split, decoder.raw, "native确实已解出所需半包字节");
                assertEquals(readsBefore + 1, readProbe.reads, "手动读取模式的半包必须主动补读");
                assertEquals(0, incomplete.refCnt());
                ByteBuf remainder = Unpooled.wrappedBuffer(stream.flush(Arrays.copyOfRange(raw, split, raw.length)));
                in.writeInbound(remainder); pump(in);
                assertPackets(in, List.of(packet));
                assertEquals(readsBefore + 1, readProbe.reads, "后续完整交包的readComplete不再补读");
                assertFalse(in.config().isAutoRead()); assertEquals(0, remainder.refCnt()); assertTrue(in.isOpen());
            }
        }
    }

    @Test
    @DisplayName("promise须绑定实际write，多运输帧最后一写失败传播全部所属包")
    void promisesWaitForEveryActualWriteAndPropagateFailure() {
        HoldingWrites sink = new HoldingWrites();
        TestEncoder encoder = encoder(true, 64, NEVER_DUE_MS);
        EmbeddedChannel out = channel(sink, encoder);
        ByteBuf first = input(randomBytes(1_500_000, 100), 1);
        ByteBuf second = input(randomBytes(1_500_000, 101), 0);
        ChannelFuture a = out.write(first);
        ChannelFuture b = out.write(second);
        out.flush();
        await(out, () -> sink.promises.size() >= 2);
        assertFalse(a.isDone()); assertFalse(b.isDone());
        sink.succeed(0); pump(out);
        assertFalse(a.isDone()); assertFalse(b.isDone());
        IllegalStateException failure = new IllegalStateException("actual transport write failed");
        sink.failRest(failure); pump(out);
        await(out, () -> a.isDone() && b.isDone());
        assertFalse(a.isSuccess()); assertFalse(b.isSuccess());
        assertSame(failure, a.cause()); assertSame(failure, b.cause());
        assertFalse(out.isOpen(), "流已前进但写失败后必须关闭连接");
        assertEquals(0, first.refCnt()); assertEquals(0, second.refCnt());
        assertTrue(sink.messages.stream().allMatch(buf -> buf.refCnt() == 0));
    }

    @Test
    @DisplayName("取消fixture异常不关闭连接，普通异常仍关闭")
    void packetCancellationMustNotCloseChannel() {
        for (boolean outbound : new boolean[]{false, true}) {
            EmbeddedChannel ch = outbound ? channel(encoder(true, 64, NEVER_DUE_MS)) : channel(decoder());
            ch.pipeline().fireExceptionCaught(new io.netty.handler.codec.EncoderException(
                    new net.momirealms.craftengine.bukkit.plugin.network.CancelPacketException()));
            pump(ch); assertTrue(ch.isOpen());
            ch.pipeline().fireExceptionCaught(new IllegalStateException("boom"));
            pump(ch); assertFalse(ch.isOpen());
        }
    }

    @Test
    @DisplayName("新连接使用全新ctx，与旧连接历史或partial彻底隔离")
    void newConnectionStartsAnIndependentStream() {
        TestEncoder old = encoder(true, 64, NEVER_DUE_MS);
        EmbeddedChannel previous = channel(old);
        byte[] packet = randomBytes(32_000, 202);
        ChannelFuture f = previous.write(input(packet, 1));
        previous.flush(); await(previous, f::isDone);
        takeBodies(previous, true);
        previous.close(); pump(previous);
        TestEncoder fresh = encoder(true, 64, NEVER_DUE_MS);
        EmbeddedChannel out = channel(fresh);
        EmbeddedChannel in = channel(new OuterSplitter(), decoder());
        assertNotSame(old.nativeCtx, fresh.nativeCtx);
        ChannelFuture sent = out.write(input(packet, 0));
        out.flush(); await(out, sent::isDone);
        assertTrue(sent.isSuccess());
        for (byte[] body : takeBodies(out, true)) feedTcp(in, body, 201);
        assertPackets(in, List.of(packet)); assertTrue(in.isOpen());
    }

    @Test
    @DisplayName("真实压缩的零/超限/溢出内层长度全部关闭连接")
    void malformedInnerLengthsCloseConnection() {
        for (byte[] bad : List.of(new byte[]{0}, new byte[]{(byte) 0x80, (byte) 0x80,
                (byte) 0x80, (byte) 0x80, (byte) 0x80, 0},
                varInt(ZstdVarInts.DEFAULT_MAX_VALUE + 1),
                new byte[]{(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, 0x0f})) {
            try (NativeStream stream = new NativeStream()) { assertRejected(stream.flush(bad)); }
        }
    }

    @Test
    @DisplayName("空体/超MC上限/畸形压缩数据关闭，未回退原版")
    void malformedAndOversizedCompressedBodiesCloseConnection() {
        assertRejected(new byte[0]);
        assertRejected(new byte[]{(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff});
        assertRejected(new byte[ZstdVarInts.MC_MAX_FRAME_BYTES + 1]);
    }

    @Test
    @DisplayName("native END属于协议故障，不当成flush或允许下一条流")
    void nativeEndIsRejected() {
        try (NativeStream stream = new NativeStream()) {
            assertRejected(stream.end(new byte[]{(byte) 0x80}));
        }
        try (NativeStream stream = new NativeStream()) {
            assertRejected(stream.end(inner(new byte[]{1, 2, 3})));
        }
    }

    @Test
    @DisplayName("高压缩比合法包通过；一个运输帧解压超过32MiB+5则关闭")
    void decompressedTransportLimitHasNoRatioShortcut() {
        try (NativeStream stream = new NativeStream()) {
            byte[] packet = new byte[8 * 1024 * 1024];
            byte[] body = stream.flush(inner(packet));
            EmbeddedChannel in = channel(decoder());
            in.writeInbound(Unpooled.wrappedBuffer(body));
            assertPackets(in, List.of(packet)); assertTrue(in.isOpen());
        }
        try (NativeStream stream = new NativeStream()) {
            byte[] half = inner(new byte[16 * 1024 * 1024]);
            byte[] raw = new byte[half.length * 2];
            System.arraycopy(half, 0, raw, 0, half.length);
            System.arraycopy(half, 0, raw, half.length, half.length);
            byte[] body = stream.flush(raw);
            EmbeddedChannel in = channel(decoder());
            in.writeInbound(Unpooled.wrappedBuffer(body)); pump(in);
            assertFalse(in.isOpen(), "同一运输帧原文总量须独立受限");
        }
    }

    @Test
    @DisplayName("close/handlerRemoved期间原包交worker持有，pending与屏障promise均终结")
    void closeAndRemovalReleaseInputsWithoutRacingTheWorker() throws Exception {
        for (boolean remove : new boolean[]{false, true}) {
            TestEncoder encoder = encoder(true, 1, NEVER_DUE_MS);
            EmbeddedChannel out = channel(encoder);
            ByteBuf inflight = input(randomBytes(9001, 8), 1);
            ByteBuf pending = input(randomBytes(1201, 9), 0);
            ByteBuf after = input(randomBytes(1301, 10), 2);
            List<ChannelFuture> futures = new ArrayList<>();
            try (PoolGate ignored = blockedPool()) {
                futures.add(out.write(inflight));
                futures.add(out.write(pending));
                futures.add(out.write(new Object()));
                futures.add(out.write(after));
                out.flush();
                assertEquals(1, inflight.refCnt());
                if (remove) out.pipeline().remove(encoder); else out.close();
                pump(out);
                assertTrue(futures.stream().allMatch(ChannelFuture::isDone));
                assertTrue(futures.stream().noneMatch(ChannelFuture::isSuccess));
                assertEquals(1, inflight.refCnt(), "排队worker尚未接管前也不能抢先释放原包");
                assertEquals(0, pending.refCnt()); assertEquals(0, after.refCnt());
                assertFalse(out.isOpen());
            }
            stopPool(); pump(out);
            assertEquals(0, inflight.refCnt());
            assertNull(out.readOutbound()); assertTrue(encoder.frames.isEmpty());
            ZstdCompressPool.init(2, 5);
        }
    }

    @Test
    @DisplayName("只有pending、尚无worker时关闭和移除也释放所有权并失败promise")
    void closeAndRemoveBeforeAnyFlushReleasePending() {
        for (boolean remove : new boolean[]{false, true}) {
            TestEncoder encoder = encoder(true, 64, NEVER_DUE_MS);
            EmbeddedChannel out = channel(encoder);
            ByteBuf packet = input(randomBytes(27, 1), 2);
            ChannelFuture sent = out.write(packet);
            assertFalse(sent.isDone());
            if (remove) out.pipeline().remove(encoder); else out.close();
            pump(out);
            assertTrue(sent.isDone()); assertFalse(sent.isSuccess());
            assertEquals(0, packet.refCnt()); assertNull(out.readOutbound());
        }
    }

    @Test
    @DisplayName("ctx已关闭标志使worker避开native，失败promise且不产帧")
    void closedContextAbortsBeforeNativeUse() {
        TestEncoder encoder = encoder(true, 1, NEVER_DUE_MS);
        encoder.lock.markClosed();
        EmbeddedChannel out = channel(encoder);
        ByteBuf packet = input(randomBytes(4096, 14), 1);
        ChannelFuture sent = out.write(packet); out.flush();
        await(out, sent::isDone);
        assertFalse(sent.isSuccess()); assertFalse(out.isOpen());
        await(out, () -> packet.refCnt() == 0);
        assertTrue(encoder.workerThreads.isEmpty(), "markClosed后不得解析或触碰原生ctx");
        assertNull(out.readOutbound());
    }

    @Test
    @DisplayName("停用池拒新任务，释放输入并失败promise，绝不在eventloop同步压缩")
    void shutdownPoolRejectsNewCodecTasks() throws Exception {
        TestEncoder encoder = encoder(true, 64, NEVER_DUE_MS);
        EmbeddedChannel out = channel(encoder);
        stopPool();
        ByteBuf packet = input(randomBytes(411, 4), 1);
        ChannelFuture sent = out.write(packet); out.flush(); pump(out);
        assertTrue(sent.isDone()); assertFalse(sent.isSuccess());
        assertInstanceOf(RejectedExecutionException.class, sent.cause());
        assertEquals(0, packet.refCnt()); assertFalse(out.isOpen());
        assertTrue(encoder.workerThreads.isEmpty()); assertNull(out.readOutbound());
        assertThrows(RejectedExecutionException.class, () -> ZstdCompressPool.execute(() -> { }));
    }

    @Test
    @DisplayName("池shutdown仍跑完已接收真实codec；移除与正常完成两分支都不丢所有权")
    void shutdownDrainsAcceptedCodecTasks() throws Exception {
        for (boolean remove : new boolean[]{false, true}) {
            TestEncoder encoder = encoder(true, 1, NEVER_DUE_MS);
            EmbeddedChannel out = channel(encoder);
            ByteBuf packet = input(randomBytes(600 * 1024, 25), 2);
            ChannelFuture sent;
            ExecutorService accepted;
            try (PoolGate ignored = blockedPool()) {
                accepted = pool();
                sent = out.write(packet); out.flush();
                assertTrue(ZstdCompressPool.queueDepth() > 0);
                ZstdCompressPool.shutdown();
                assertNull(pool());
                if (remove) out.pipeline().remove(encoder);
                assertTrue(packet.refCnt() > 0);
            }
            assertTrue(accepted.awaitTermination(TIMEOUT_MS, TimeUnit.MILLISECONDS));
            pump(out);
            assertEquals(0, packet.refCnt()); assertTrue(sent.isDone());
            if (remove) {
                assertFalse(sent.isSuccess()); assertNull(out.readOutbound()); assertFalse(out.isOpen());
            } else {
                assertTrue(sent.isSuccess(), String.valueOf(sent.cause()));
                EmbeddedChannel in = channel(new OuterSplitter(), decoder());
                for (byte[] body : takeBodies(out, true)) feedTcp(in, body, 5);
                assertPackets(in, List.of(randomBytes(600 * 1024, 25)));
            }
            ZstdCompressPool.init(2, 5);
        }
    }

    @Test
    @DisplayName("worker回eventloop拒绝须归还已生成帧及原包，失败全部promise并关闭")
    void rejectedEventLoopReturnReleasesInputsOutputsAndPromises() throws Exception {
        TestEncoder encoder = encoder(true, 1, NEVER_DUE_MS);
        IoProbe io = new IoProbe();
        EmbeddedChannel out = channel(io, encoder);
        TrackingAllocator allocations = new TrackingAllocator();
        out.config().setAllocator(allocations);
        ByteBuf first = input(randomBytes(80_019, 51), 1);
        ByteBuf pending = input(randomBytes(33, 52), 0);
        ChannelFuture a;
        ChannelFuture b;
        ExecutorService accepted;
        try (PoolGate ignored = blockedPool()) {
            accepted = pool(); a = out.write(first); b = out.write(pending); out.flush();
            encoder.rejectReturns = true;
            ZstdCompressPool.shutdown();
        }
        assertTrue(accepted.awaitTermination(TIMEOUT_MS, TimeUnit.MILLISECONDS));
        pump(out);
        await(out, () -> a.isDone() && b.isDone());
        assertFalse(a.isSuccess()); assertFalse(b.isSuccess());
        assertInstanceOf(RejectedExecutionException.class, a.cause());
        assertEquals(0, encoder.finishedCallbacks, "真实回投拒绝不得偷跑finishBatch");
        assertEquals(1, encoder.closedCallbacks, "拒绝路径关闭须经过主线程桥");
        assertEquals(1, io.closes);
        assertEquals(0, first.refCnt()); assertEquals(0, pending.refCnt());
        assertFalse(out.isOpen()); assertNull(out.readOutbound());
        assertFalse(allocations.allocated.isEmpty(), "真实worker须已分配prefix/scratch/最终输出");
        assertTrue(allocations.allocated.stream().allMatch(buf -> buf.refCnt() == 0),
                "回投拒绝不得泄漏已生成输出或临时direct缓冲");
        assertEquals(1, encoder.workerThreads.size(), "只压一个在途批，pending不得续流");
    }

    @Test
    @DisplayName("流已推进后下一批故障必须关闭，不跳过、不原版回退、不继续写")
    void failureAfterAnEarlierFlushIsTerminal() {
        TestEncoder encoder = encoder(true, 64, NEVER_DUE_MS);
        EmbeddedChannel out = channel(encoder);
        ChannelFuture good = out.write(input(new byte[]{4, 5, 6}, 0));
        out.flush(); await(out, good::isDone); assertTrue(good.isSuccess());
        takeBodies(out, true);
        IllegalStateException failure = new IllegalStateException("stream worker fixture failure");
        encoder.batchFailure = failure;
        ByteBuf failed = input(randomBytes(901, 42), 1);
        ChannelFuture sent = out.write(failed); out.flush(); await(out, sent::isDone);
        assertFalse(sent.isSuccess()); assertSame(failure, sent.cause());
        assertFalse(out.isOpen()); assertEquals(0, failed.refCnt());
        ByteBuf later = input(new byte[]{7}, 0);
        ChannelFuture late = out.write(later); out.flush(); pump(out);
        assertTrue(late.isDone()); assertFalse(late.isSuccess()); assertEquals(0, later.refCnt());
        assertNull(out.readOutbound()); assertEquals(1, encoder.frames.size());
    }

    @Test
    @DisplayName("半包按已到字节有界分配；close/handlerRemoved释放partial，新连接无残留")
    void decoderPartialIsBoundedAndReleasedOnCloseOrRemoval() throws Exception {
        for (boolean remove : new boolean[]{false, true}) {
            TestDecoder decoder = decoder(); EmbeddedChannel in = channel(decoder);
            ByteBuf partial;
            try (NativeStream stream = new NativeStream()) {
                byte[] prefix = varInt(ZstdVarInts.DEFAULT_MAX_VALUE);
                byte[] raw = Arrays.copyOf(prefix, prefix.length + 101);
                in.writeInbound(Unpooled.wrappedBuffer(stream.flush(raw)));
                assertTrue(in.isOpen()); assertNull(in.readInbound());
                partial = partial(decoder);
                assertNotNull(partial); assertTrue(partial.refCnt() > 0);
                assertTrue(partial.capacity() < ZstdVarInts.DEFAULT_MAX_VALUE,
                        "不得仅因声明32MiB就预分配32MiB");
                if (remove) in.pipeline().remove(decoder); else in.close();
                pump(in); assertEquals(0, partial.refCnt());
            }
            try (NativeStream fresh = new NativeStream()) {
                EmbeddedChannel next = channel(decoder());
                byte[] packet = {8, 9, 10};
                next.writeInbound(Unpooled.wrappedBuffer(fresh.flush(inner(packet))));
                assertPackets(next, List.of(packet)); assertTrue(next.isOpen());
            }
        }
    }

    @Test
    @DisplayName("同帧首包下传时移除decoder后停止native循环，保留已交付切片所有权")
    void decoderRemovalDuringDeliveryDoesNotEmitLaterPackets() {
        TestDecoder decoder = decoder();
        List<ByteBuf> delivered = new ArrayList<>();
        EmbeddedChannel in = channel(decoder, new ChannelInboundHandlerAdapter() {
            @Override public void channelRead(ChannelHandlerContext ctx, Object message) {
                delivered.add((ByteBuf) message); ctx.pipeline().remove(decoder);
            }
        });
        byte[] first = inner(new byte[]{1, 2, 3});
        byte[] second = inner(new byte[]{4, 5, 6});
        byte[] raw = Arrays.copyOf(first, first.length + second.length);
        System.arraycopy(second, 0, raw, first.length, second.length);
        try (NativeStream stream = new NativeStream()) {
            in.writeInbound(Unpooled.wrappedBuffer(stream.flush(raw)));
            assertEquals(1, delivered.size());
            assertArrayEquals(new byte[]{1, 2, 3}, bytes(delivered.getFirst()));
            assertTrue(delivered.getFirst().refCnt() > 0);
        } finally {
            for (ByteBuf packet : delivered) ReferenceCountUtil.safeRelease(packet);
        }
        assertTrue(delivered.stream().allMatch(packet -> packet.refCnt() == 0));
    }

    @Test
    @DisplayName("原版压缩与解压handler保留但由context跳过，裸包不再套零长度标记")
    void originalHandlersRemainInstalledButAreBypassed() {
        String original = "original-compression";
        TestEncoder encoder = encoder(true, 64, NEVER_DUE_MS);
        encoder.forwardHandler = original;
        EmbeddedChannel out = channel();
        out.pipeline().addLast(original, new ChannelOutboundHandlerAdapter() {
            @Override public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
                ReferenceCountUtil.release(msg); promise.setFailure(new AssertionError("原版compress不得收到流帧"));
            }
        });
        out.pipeline().addLast(encoder);
        TestDecoder decoder = decoder(); decoder.forwardHandler = original;
        EmbeddedChannel in = channel(new OuterSplitter(), decoder);
        in.pipeline().addLast(original, new ChannelInboundHandlerAdapter() {
            @Override public void channelRead(ChannelHandlerContext ctx, Object msg) {
                ReferenceCountUtil.release(msg); fail("原版decompress不得收到裸包");
            }
        });
        byte[] expected = randomBytes(4099, 80);
        ChannelFuture sent = out.write(input(expected, 1)); out.flush(); await(out, sent::isDone);
        assertTrue(sent.isSuccess(), String.valueOf(sent.cause()));
        for (byte[] body : takeBodies(out, true)) feedTcp(in, body, 1);
        assertPackets(in, List.of(expected));
        assertNotNull(out.pipeline().get(original)); assertNotNull(in.pipeline().get(original));
    }

    @Test
    @DisplayName("空出站包与超过32MiB单包都是流故障，输入归还且promise失败")
    void invalidOutboundPacketLengthsFailWithoutFallback() {
        for (int size : new int[]{0, ZstdVarInts.DEFAULT_MAX_VALUE + 1}) {
            TestEncoder encoder = encoder(true, 64, NEVER_DUE_MS);
            EmbeddedChannel out = channel(encoder);
            ByteBuf packet = Unpooled.buffer(Math.max(1, size)).writerIndex(size);
            ChannelFuture sent = out.write(packet); out.flush(); pump(out);
            assertTrue(sent.isDone()); assertFalse(sent.isSuccess());
            assertEquals(0, packet.refCnt()); assertFalse(out.isOpen()); assertNull(out.readOutbound());
        }
    }

    @Test
    @DisplayName("完整MC上限运输帧可承载连续流中段，其余压缩数据在下一运输帧延续")
    void exactTransportBodyLimitIsInclusive() {
        byte[] packet = randomBytes(ZstdVarInts.MC_MAX_FRAME_BYTES + 90_013, 990);
        try (NativeStream stream = new NativeStream()) {
            byte[] compressed = stream.flush(inner(packet));
            assertTrue(compressed.length > ZstdVarInts.MC_MAX_FRAME_BYTES);
            TestDecoder decoder = decoder();
            EmbeddedChannel in = channel(new OuterSplitter(), decoder);
            feedTcp(in, Arrays.copyOfRange(compressed, 0, ZstdVarInts.MC_MAX_FRAME_BYTES), 80);
            assertTrue(in.isOpen()); assertNull(in.readInbound(), "包尚跨下一运输帧");
            feedTcp(in, Arrays.copyOfRange(compressed, ZstdVarInts.MC_MAX_FRAME_BYTES, compressed.length), 81);
            assertPackets(in, List.of(packet)); assertTrue(in.isOpen());
            assertEquals(2, decoder.frames);
        }
    }

    @Test
    @DisplayName("原始批含内长前缀最多32MiB；连续大包按批预算拆分而不改包顺序")
    void rawBatchBudgetSplitsAtPacketBoundaries() {
        byte[] packet = new byte[17 * 1024 * 1024];
        for (int i = 0; i < packet.length; i++) packet[i] = (byte) (i % 251);
        TestEncoder encoder = encoder(true, 64, NEVER_DUE_MS);
        EmbeddedChannel out = channel(encoder);
        EmbeddedChannel in = channel(new OuterSplitter(), decoder());
        ChannelFuture first = out.write(input(packet, 1));
        ChannelFuture second = out.write(input(packet, 0));
        out.flush(); await(out, () -> first.isDone() && second.isDone());
        assertTrue(first.isSuccess()); assertTrue(second.isSuccess());
        assertEquals(2, encoder.frames.size());
        assertTrue(encoder.frames.stream().allMatch(frame -> frame[0] <= ZstdVarInts.DEFAULT_MAX_VALUE));
        for (byte[] body : takeBodies(out, true)) feedTcp(in, body, 123);
        assertPackets(in, List.of(packet, packet));
    }

    @Test
    @DisplayName("多运输帧所有实际write成功后才成功原包promise")
    void allActualWritesMustSucceedBeforePacketPromise() {
        HoldingWrites sink = new HoldingWrites();
        TestEncoder encoder = encoder(true, 64, NEVER_DUE_MS);
        EmbeddedChannel out = channel(sink, encoder);
        ChannelFuture sent = out.write(input(randomBytes(3 * 1024 * 1024, 42), 1));
        out.flush(); await(out, () -> sink.promises.size() >= 2);
        assertFalse(sent.isDone());
        for (int i = 0; i < sink.promises.size() - 1; i++) {
            sink.succeed(i); pump(out); assertFalse(sent.isDone());
        }
        sink.succeed(sink.promises.size() - 1); pump(out);
        assertTrue(sent.isSuccess(), String.valueOf(sent.cause())); assertTrue(out.isOpen());
        assertTrue(sink.messages.stream().allMatch(buf -> buf.refCnt() == 0));
    }

    @Test
    @DisplayName("原包真实读取发生在共享worker，不以eventloop拼接副本替代")
    void originalPacketIsReadByTheWorker() {
        TestEncoder encoder = encoder(true, 1, NEVER_DUE_MS);
        EmbeddedChannel out = channel(encoder);
        EmbeddedChannel in = channel(new OuterSplitter(), decoder());
        byte[] expected = randomBytes(90_019, 115);
        WorkerReadInput original = new WorkerReadInput(input(expected, 1));
        ChannelFuture sent;
        synchronized (encoder.lock) {
            sent = out.write(original); out.flush();
            assertEquals(0, original.reads.get(), "等待worker期间eventloop不得复制原包内容");
            assertTrue(original.refCnt() > 0);
        }
        await(out, sent::isDone);
        assertTrue(sent.isSuccess(), String.valueOf(sent.cause()));
        assertTrue(original.reads.get() > 0, "真实压缩必须读取转交的原包");
        assertEquals(0, original.refCnt());
        for (byte[] body : takeBodies(out, true)) feedTcp(in, body, 115);
        assertPackets(in, List.of(expected));
    }

    @Test
    @DisplayName("真实ctx池归还reset后复租；两方向各与独立全新对端往返以防对称泄漏自证")
    void pooledContextsResetHistoryBeforeNewConnections() {
        ZstdCompressCtx borrowedCompress = ZstdCtxPool.borrowCompress(3);
        ZstdDecompressCtx borrowedDecompress = ZstdCtxPool.borrowDecompress();
        borrowedDecompress.setMagicless(true);
        TestEncoder oldEncoder = new TestEncoder(borrowedCompress, true, 64, NEVER_DUE_MS);
        encoders.add(oldEncoder); decoders.add(borrowedDecompress);
        EmbeddedChannel oldOut = channel(oldEncoder);
        EmbeddedChannel oldIn = channel(new OuterSplitter(), new TestDecoder(borrowedDecompress));
        byte[] history = randomBytes(48 * 1024, 44);
        ChannelFuture oldSent = oldOut.write(input(history, 1));
        oldOut.flush(); await(oldOut, oldSent::isDone); assertTrue(oldSent.isSuccess());
        for (byte[] body : takeBodies(oldOut, true)) feedTcp(oldIn, body, 44);
        assertPackets(oldIn, List.of(history));
        oldOut.close(); oldIn.close(); pump(oldOut); pump(oldIn);
        synchronized (oldEncoder.lock) {
            oldEncoder.lock.markClosed();
            ZstdCtxPool.returnCompress(borrowedCompress);
        }
        ZstdCtxPool.returnDecompress(borrowedDecompress);
        encoders.remove(oldEncoder); decoders.remove(borrowedDecompress);

        ZstdCompressCtx reusedCompress = ZstdCtxPool.borrowCompress(3);
        ZstdDecompressCtx reusedDecompress = ZstdCtxPool.borrowDecompress();
        TestEncoder nextEncoder = new TestEncoder(reusedCompress, true, 64, NEVER_DUE_MS);
        encoders.add(nextEncoder); decoders.add(reusedDecompress);
        assertSame(borrowedCompress, reusedCompress, "必须真的复租相同ctx，不能退化成new测试");
        assertSame(borrowedDecompress, reusedDecompress);
        EmbeddedChannel nextOut = channel(nextEncoder);
        EmbeddedChannel freshPeer = channel(new OuterSplitter(), decoder());
        ChannelFuture nextSent = nextOut.write(input(history, 0));
        nextOut.flush(); await(nextOut, nextSent::isDone); assertTrue(nextSent.isSuccess());
        for (byte[] body : takeBodies(nextOut, true)) feedTcp(freshPeer, body, 45);
        assertPackets(freshPeer, List.of(history)); assertTrue(freshPeer.isOpen());
        // 复租decoder单独接新compressor，避免两方向都忘reset时仍对称通过。
        EmbeddedChannel reusedIn = channel(new TestDecoder(reusedDecompress));
        try (NativeStream freshStream = new NativeStream()) {
            byte[] packet = randomBytes(1501, 46);
            reusedIn.writeInbound(Unpooled.wrappedBuffer(freshStream.flush(inner(packet))));
            assertPackets(reusedIn, List.of(packet)); assertTrue(reusedIn.isOpen());
        }
    }

    private static final class WorkerReadInput extends CompositeByteBuf {
        final java.util.concurrent.atomic.AtomicInteger reads = new java.util.concurrent.atomic.AtomicInteger();
        WorkerReadInput(ByteBuf original) {
            super(io.netty.buffer.UnpooledByteBufAllocator.DEFAULT, false, 16);
            addComponent(true, original);
        }
        private void readOnWorker() {
            assertTrue(Thread.currentThread().getName().startsWith("miku-zstd-compress-"),
                    "原包数据只能在共享压缩worker读取");
            reads.incrementAndGet();
        }
        @Override public ByteBuffer[] nioBuffers(int index, int length) {
            readOnWorker(); return super.nioBuffers(index, length);
        }
        @Override public ByteBuffer nioBuffer(int index, int length) {
            readOnWorker(); return super.nioBuffer(index, length);
        }
        @Override public CompositeByteBuf getBytes(int index, byte[] destination, int offset, int length) {
            readOnWorker(); return super.getBytes(index, destination, offset, length);
        }
        @Override public CompositeByteBuf getBytes(int index, ByteBuf destination, int offset, int length) {
            readOnWorker(); return super.getBytes(index, destination, offset, length);
        }
        @Override public CompositeByteBuf getBytes(int index, ByteBuffer destination) {
            readOnWorker(); return super.getBytes(index, destination);
        }
    }

    @Test
    @DisplayName("真实worker已生成输出但回调未跑时close/移除，迟到结果全部归还而不写出")
    void generatedOutputIsReleasedAfterCloseOrRemovalBeforeCallback() throws Exception {
        for (boolean remove : new boolean[]{false, true}) {
            TestEncoder encoder = encoder(true, 1, NEVER_DUE_MS);
            EmbeddedChannel out = channel(encoder);
            TrackingAllocator allocator = new TrackingAllocator(); out.config().setAllocator(allocator);
            ByteBuf original = input(randomBytes(101_003, 29), 1);
            ChannelFuture sent;
            CountDownLatch workerFinished = new CountDownLatch(1);
            try (PoolGate ignored = blockedPool()) {
                sent = out.write(original); out.flush();
                ZstdCompressPool.execute(workerFinished::countDown);
            }
            assertTrue(workerFinished.await(TIMEOUT_MS, TimeUnit.MILLISECONDS));
            assertEquals(0, original.refCnt());
            assertFalse(sent.isDone(), "尚未推进eventloop，原promise不能提前成功");
            assertTrue(allocator.allocated.stream().anyMatch(buf -> buf.refCnt() > 0),
                    "必须真实持有尚待回投的压缩结果，避免空转通过");
            // 用pipeline/context关闭，避免EmbeddedChannel.close先自动推进pending callback。
            if (remove) out.pipeline().remove(encoder); else out.pipeline().close();
            pump(out);
            assertTrue(sent.isDone()); assertFalse(sent.isSuccess()); assertFalse(out.isOpen());
            assertEquals(1, encoder.finishedCallbacks, "原promise虽被close提前失败，仍须跑完迟到finish清理");
            assertNull(out.readOutbound());
            assertTrue(allocator.allocated.stream().allMatch(buf -> buf.refCnt() == 0));
            assertTrue(encoder.frames.isEmpty(), "迟到结果不能被计作已下发批次");
        }
    }

    @Test
    @DisplayName("合法半包后畸形压缩续片关闭并释放partial，不把失败字节混入下一包")
    void malformedContinuationReleasesPartial() throws Exception {
        TestDecoder decoder = decoder(); EmbeddedChannel in = channel(decoder);
        try (NativeStream stream = new NativeStream()) {
            byte[] prefix = varInt(4096);
            byte[] first = Arrays.copyOf(prefix, prefix.length + 37);
            in.writeInbound(Unpooled.wrappedBuffer(stream.flush(first)));
            ByteBuf partial = partial(decoder);
            assertNotNull(partial); assertTrue(partial.refCnt() > 0); assertNull(in.readInbound());
            ByteBuf malformed = Unpooled.buffer(3).writeMedium(0xFFFFFF);
            in.writeInbound(malformed); pump(in);
            assertFalse(in.isOpen()); assertEquals(0, malformed.refCnt()); assertEquals(0, partial.refCnt());
            assertNull(in.readInbound());
        }
    }

    @Test
    @DisplayName("magicless首2字节可跨运输帧；校验完整前不触碰native且每帧仍计数")
    void streamHeaderIsValidatedBeforeNativeAcrossTransportFrames() {
        NativeReadProbe ctx = new NativeReadProbe(); decoders.add(ctx);
        TestDecoder decoder = new TestDecoder(ctx);
        EmbeddedChannel in = channel(new OuterSplitter(), decoder);
        byte[] packet = randomBytes(301, 21);
        try (NativeStream stream = new NativeStream()) {
            byte[] body = stream.flush(inner(packet));
            assertEquals(0, body[0], "FRAME8首头固定无contentSize/dictID/checksum");
            feedTcp(in, Arrays.copyOfRange(body, 0, 1), 1);
            assertTrue(in.isOpen()); assertNull(in.readInbound());
            assertEquals(0, ctx.calls, "窗口尚未知时不得先把第一字节交native");
            assertEquals(1, decoder.frames, "只有首头字节的运输帧也须计入wire");
            assertEquals(0, decoder.raw);
            feedTcp(in, Arrays.copyOfRange(body, 1, body.length), 2);
            assertPackets(in, List.of(packet)); assertTrue(in.isOpen());
            assertTrue(ctx.calls > 0); assertEquals(2, decoder.frames);
            assertEquals(inner(packet).length, decoder.raw);
        }
    }

    @Test
    @DisplayName("固定magicless首头拒绝额外参数以及超过2^27的整档/分数窗口，拒绝发生在native前")
    void headerParametersAndOversizedWindowsAreRejectedBeforeNative() {
        // 官方Zstd格式：FHD=0；WD 0x90为2^28，0x89为2^27加1/8，而非只看log位。
        List<byte[]> headers = new ArrayList<>();
        headers.add(new byte[]{0, (byte) 0x90});
        headers.add(new byte[]{0, (byte) 0x89});
        for (int descriptor : new int[]{0x01, 0x04, 0x08, 0x10, 0x20, 0x40}) {
            headers.add(new byte[]{(byte) descriptor, 0x60, 0, 0, 0});
        }
        for (byte[] header : headers) {
            NativeReadProbe ctx = new NativeReadProbe(); decoders.add(ctx);
            EmbeddedChannel in = channel(new TestDecoder(ctx));
            ByteBuf body = Unpooled.buffer(header.length).writeBytes(header);
            in.writeInbound(body); pump(in);
            assertFalse(in.isOpen()); assertEquals(0, body.refCnt()); assertNull(in.readInbound());
            assertEquals(0, ctx.calls, "非法窗口/descriptor必须在JNI入口前拒绝");
        }
    }

    @Test
    @DisplayName("首头第二字节分片后才显露超限窗口，也须在native入口前关闭")
    void splitOversizedWindowHeaderNeverReachesNative() {
        NativeReadProbe ctx = new NativeReadProbe(); decoders.add(ctx);
        EmbeddedChannel in = channel(new OuterSplitter(), new TestDecoder(ctx));
        feedTcp(in, new byte[]{0}, 1);
        assertTrue(in.isOpen()); assertEquals(0, ctx.calls);
        feedTcp(in, new byte[]{(byte) 0x90}, 2);
        assertFalse(in.isOpen()); assertNull(in.readInbound()); assertEquals(0, ctx.calls);
    }

    @Test
    @DisplayName("native带输入零进展是终结故障，不能静默等待下一运输帧")
    void nativeStallWithInputClosesAndReleasesFrame() {
        NativeReadProbe ctx = new NativeReadProbe(); ctx.stall = true; decoders.add(ctx);
        EmbeddedChannel in = channel(new TestDecoder(ctx));
        ByteBuf body = Unpooled.buffer(5).writeByte(0).writeByte(0x60).writeMedium(0);
        in.writeInbound(body); pump(in);
        assertEquals(1, ctx.calls); assertFalse(in.isOpen());
        assertEquals(0, body.refCnt()); assertNull(in.readInbound());
    }

    private static final class NativeReadProbe extends ZstdDecompressCtx {
        int calls;
        boolean stall;
        @Override public boolean decompressDirectByteBufferStream(ByteBuffer destination, ByteBuffer source) {
            calls++;
            return !stall && super.decompressDirectByteBufferStream(destination, source);
        }
    }

    private static ByteBuf partial(TestDecoder decoder) throws Exception {
        Field field = ZstdBatchDecoderBase.class.getDeclaredField("partial");
        field.setAccessible(true); return (ByteBuf) field.get(decoder);
    }

    private static PoolGate blockedPool() throws Exception {
        stopPool(); ZstdCompressPool.init(1, 5); return new PoolGate();
    }

    private static final class PoolGate implements AutoCloseable {
        final CountDownLatch release = new CountDownLatch(1);
        PoolGate() throws InterruptedException {
            CountDownLatch started = new CountDownLatch(1);
            ZstdCompressPool.execute(() -> {
                started.countDown();
                try { assertTrue(release.await(TIMEOUT_MS, TimeUnit.MILLISECONDS), "pool gate超时"); }
                catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new AssertionError(e); }
            });
            assertTrue(started.await(TIMEOUT_MS, TimeUnit.MILLISECONDS));
        }
        @Override public void close() { release.countDown(); }
    }

    private static final class TrackingAllocator extends io.netty.buffer.AbstractByteBufAllocator {
        final List<ByteBuf> allocated = new java.util.concurrent.CopyOnWriteArrayList<>();
        TrackingAllocator() { super(true); }
        @Override protected ByteBuf newHeapBuffer(int initial, int max) {
            ByteBuf buffer = Unpooled.buffer(initial, max); allocated.add(buffer); return buffer;
        }
        @Override protected ByteBuf newDirectBuffer(int initial, int max) {
            ByteBuf buffer = Unpooled.directBuffer(initial, max); allocated.add(buffer); return buffer;
        }
        @Override public boolean isDirectBufferPooled() { return false; }
    }

    private TestEncoder encoder(boolean bodyLen, int maxPackets, int windowMs) {
        TestEncoder encoder = new TestEncoder(bodyLen, maxPackets, windowMs);
        encoders.add(encoder); return encoder;
    }

    private TestDecoder decoder() {
        ZstdDecompressCtx ctx = new ZstdDecompressCtx(); ctx.setMagicless(true);
        decoders.add(ctx); return new TestDecoder(ctx);
    }

    private EmbeddedChannel channel(io.netty.channel.ChannelHandler... handlers) {
        EmbeddedChannel channel = new EmbeddedChannel(handlers);
        channels.add(channel); return channel;
    }

    private void pump(EmbeddedChannel channel) {
        // EmbeddedEventLoop的任务队列不是并发队列；仅测试主线程把worker回投转入真实eventloop。
        for (TestEncoder encoder : encoders) encoder.pumpCallbacks(channel);
        channel.runPendingTasks();
    }

    private void await(EmbeddedChannel channel, BooleanSupplier done) {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(TIMEOUT_MS);
        do {
            // promise已done仍先泵已回投的finishBatch；迟到worker另由相应测试等完成计数。
            pump(channel); channel.runScheduledPendingTasks();
            if (done.getAsBoolean()) return;
            try { Thread.sleep(1); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new AssertionError(e); }
        } while (System.nanoTime() < deadline);
        pump(channel); assertTrue(done.getAsBoolean(), "异步真实codec未完成");
    }

    private static ExecutorService pool() throws Exception {
        Field field = ZstdCompressPool.class.getDeclaredField("pool");
        field.setAccessible(true); return (ExecutorService) field.get(null);
    }

    private static void stopPool() throws Exception {
        ExecutorService executor = pool(); ZstdCompressPool.shutdown();
        if (executor != null) assertTrue(executor.awaitTermination(TIMEOUT_MS, TimeUnit.MILLISECONDS));
    }

    private static byte[] randomBytes(int count, long seed) {
        byte[] data = new byte[count]; new Random(seed).nextBytes(data); return data;
    }

    private static ByteBuf input(byte[] packet, int kind) {
        ByteBuf buf;
        if (kind == 2) {
            int mid = packet.length / 2;
            CompositeByteBuf composite = Unpooled.compositeBuffer();
            composite.addComponents(true, Unpooled.wrappedBuffer(new byte[]{99, 98}),
                    Unpooled.wrappedBuffer(Arrays.copyOfRange(packet, 0, mid)),
                    Unpooled.wrappedBuffer(Arrays.copyOfRange(packet, mid, packet.length)));
            buf = composite;
        } else {
            buf = kind == 1 ? Unpooled.directBuffer(packet.length + 2) : Unpooled.buffer(packet.length + 2);
            buf.writeShort(0x6362).writeBytes(packet);
        }
        buf.skipBytes(2); return buf;
    }

    private static byte[] varInt(int value) {
        ByteBuf buf = Unpooled.buffer(5);
        try { ZstdVarInts.write(buf, value); return bytes(buf); }
        finally { buf.release(); }
    }

    private static byte[] inner(byte[] packet) {
        byte[] prefix = varInt(packet.length);
        byte[] raw = Arrays.copyOf(prefix, prefix.length + packet.length);
        System.arraycopy(packet, 0, raw, prefix.length, packet.length); return raw;
    }

    private static byte[] bytes(ByteBuf buf) {
        byte[] bytes = new byte[buf.readableBytes()]; buf.getBytes(buf.readerIndex(), bytes); return bytes;
    }

    private static byte[] bodyBytes(ByteBuf frame, boolean bodyLen) {
        ByteBuf view = frame.duplicate();
        if (bodyLen) {
            int n = ZstdVarInts.tryRead(view, ZstdVarInts.MC_MAX_FRAME_BYTES);
            assertTrue(n > 0); assertEquals(n, view.readableBytes());
        }
        assertTrue(view.isReadable()); assertTrue(view.readableBytes() <= ZstdVarInts.MC_MAX_FRAME_BYTES);
        return bytes(view);
    }

    private static List<byte[]> takeBodies(EmbeddedChannel out, boolean bodyLen) {
        List<byte[]> bodies = new ArrayList<>(); ByteBuf frame;
        while ((frame = out.readOutbound()) != null) {
            try { bodies.add(bodyBytes(frame, bodyLen)); }
            finally { frame.release(); }
        }
        return bodies;
    }

    private static void feedTcp(EmbeddedChannel in, byte[] body, long seed) {
        ByteBuf wire = Unpooled.buffer(body.length + 5);
        ZstdVarInts.write(wire, body.length); wire.writeBytes(body);
        Random random = new Random(seed);
        try {
            // 强制把多字节MC长度头拆开；后面随机块覆盖边界合并与拆分。
            if (wire.isReadable()) in.writeInbound(wire.readRetainedSlice(1));
            while (wire.isReadable()) {
                int n = Math.min(wire.readableBytes(), 1 + random.nextInt(8192));
                in.writeInbound(wire.readRetainedSlice(n));
            }
        } finally { wire.release(); }
    }

    private static List<byte[]> drain(EmbeddedChannel in) {
        List<byte[]> packets = new ArrayList<>(); ByteBuf packet;
        while ((packet = in.readInbound()) != null) {
            try { assertTrue(packet.isDirect(), "解出的完整包保留池化direct切片"); packets.add(bytes(packet)); }
            finally { packet.release(); }
        }
        return packets;
    }

    private static void assertPacketLists(List<byte[]> expected, List<byte[]> actual) {
        assertEquals(expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) assertArrayEquals(expected.get(i), actual.get(i), "packet " + i);
    }

    private static void assertPackets(EmbeddedChannel in, List<byte[]> expected) {
        assertPacketLists(expected, drain(in));
    }

    private void assertRejected(byte[] body) {
        EmbeddedChannel in = channel(decoder()); ByteBuf input = Unpooled.buffer(Math.max(1, body.length)).writeBytes(body);
        in.writeInbound(input); pump(in);
        assertFalse(in.isOpen(), "畸形连续流必须关闭连接");
        assertNull(in.readInbound(), "该畸形fixture尚无完整合法包");
        assertEquals(0, input.refCnt());
    }

    /** 只负责 MC 运输边界；不解析zstd或内层包。 */
    private static final class OuterSplitter extends ByteToMessageDecoder {
        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
            int start = in.readerIndex();
            int length = ZstdVarInts.tryRead(in, ZstdVarInts.MC_MAX_FRAME_BYTES);
            if (length == ZstdVarInts.NEED_MORE) return;
            if (length <= 0) { ctx.close(); return; }
            if (in.readableBytes() < length) { in.readerIndex(start); return; }
            out.add(in.readRetainedSlice(length));
        }
    }

    private static final class TestEncoder extends ZstdBatchEncoderBase {
        final ZstdCompressCtx nativeCtx;
        final ZstdCompressLock lock = new ZstdCompressLock();
        final List<int[]> frames = new ArrayList<>();
        final List<String> workerThreads = new ArrayList<>();
        final boolean bodyLen;
        final int maxPackets;
        final int windowMs;
        String forwardHandler;
        volatile RuntimeException batchFailure;
        final Thread testThread = Thread.currentThread();
        final ConcurrentLinkedQueue<Runnable> callbacks = new ConcurrentLinkedQueue<>();
        ChannelHandlerContext realContext;
        ChannelHandlerContext bridgedContext;
        volatile boolean rejectReturns;
        int finishedCallbacks;
        int closedCallbacks;
        TestEncoder(boolean bodyLen, int maxPackets, int windowMs) {
            this(new ZstdCompressCtx(), bodyLen, maxPackets, windowMs);
        }
        TestEncoder(ZstdCompressCtx nativeCtx, boolean bodyLen, int maxPackets, int windowMs) {
            this.nativeCtx = nativeCtx;
            this.bodyLen = bodyLen; this.maxPackets = maxPackets; this.windowMs = windowMs;
            nativeCtx.setLevel(3).setWindowLog(20).setMagicless(true)
                    .setContentSize(false).setDictID(false).setChecksum(false);
        }
        @Override protected ZstdCompressCtx resolveCompressContext(ChannelHandlerContext ctx) { return nativeCtx; }
        @Override protected ZstdCompressCtx compressContextForBatch() {
            workerThreads.add(Thread.currentThread().getName());
            if (batchFailure != null) throw batchFailure;
            return nativeCtx;
        }
        @Override protected ZstdCompressLock compressLock() { return lock; }
        @Override protected ChannelHandlerContext downstream(ChannelHandlerContext ctx) {
            return forwardHandler == null ? ctx : ctx.pipeline().context(forwardHandler);
        }
        @Override public void write(ChannelHandlerContext ctx, Object message, ChannelPromise promise) {
            super.write(callbackContext(ctx), message, promise);
        }
        @Override public void flush(ChannelHandlerContext ctx) { super.flush(callbackContext(ctx)); }
        private ChannelHandlerContext callbackContext(ChannelHandlerContext ctx) {
            assertSame(testThread, Thread.currentThread(), "codec入口只能由测试主线程驱动");
            if (bridgedContext != null) return bridgedContext;
            realContext = ctx;
            io.netty.util.concurrent.EventExecutor executor = ctx.executor();
            Object bridge = java.lang.reflect.Proxy.newProxyInstance(
                    io.netty.util.concurrent.EventExecutor.class.getClassLoader(),
                    new Class<?>[]{io.netty.util.concurrent.EventExecutor.class}, (proxy, method, arguments) -> {
                        if (method.getName().equals("execute") && Thread.currentThread() != testThread) {
                            if (rejectReturns) throw new RejectedExecutionException("event loop return fixture");
                            Runnable callback = (Runnable) arguments[0];
                            callbacks.add(() -> {
                                try { callback.run(); }
                                finally { finishedCallbacks++; }
                            });
                            return null;
                        }
                        try { return method.invoke(executor, arguments); }
                        catch (java.lang.reflect.InvocationTargetException e) { throw e.getCause(); }
                    });
            bridgedContext = (ChannelHandlerContext) java.lang.reflect.Proxy.newProxyInstance(
                    ChannelHandlerContext.class.getClassLoader(), new Class<?>[]{ChannelHandlerContext.class},
                    (proxy, method, arguments) -> {
                        if (method.getName().equals("executor")) return bridge;
                        if (method.getName().equals("close") && Thread.currentThread() != testThread) {
                            ChannelPromise closed = arguments == null || arguments.length == 0
                                    ? ctx.newPromise() : (ChannelPromise) arguments[0];
                            callbacks.add(() -> {
                                try { ctx.close(closed); }
                                finally { closedCallbacks++; }
                            });
                            return closed;
                        }
                        try { return method.invoke(ctx, arguments); }
                        catch (java.lang.reflect.InvocationTargetException e) { throw e.getCause(); }
                    });
            return bridgedContext;
        }
        void pumpCallbacks(EmbeddedChannel channel) {
            assertSame(testThread, Thread.currentThread(), "回投桥仅由测试主线程排空");
            if (realContext == null || realContext.channel() != channel) return;
            Runnable callback;
            while ((callback = callbacks.poll()) != null) realContext.executor().execute(callback);
        }
        @Override protected boolean writeBodyLen() { return bodyLen; }
        @Override protected void onFrame(int rawBytes, int wireBytes, boolean compressed) {
            frames.add(new int[]{rawBytes, wireBytes, compressed ? 1 : 0});
        }
        @Override protected int configBatchWindowMs() { return windowMs; }
        @Override protected int configBatchMaxPackets() { return maxPackets; }
    }

    private static final class TestDecoder extends ZstdBatchDecoderBase {
        final ZstdDecompressCtx nativeCtx;
        int frames;
        int wire;
        int raw;
        String forwardHandler;
        TestDecoder(ZstdDecompressCtx nativeCtx) { this.nativeCtx = nativeCtx; }
        @Override protected ZstdDecompressCtx resolveDecompressContext(ChannelHandlerContext ctx) { return nativeCtx; }
        @Override protected ChannelHandlerContext decodeForwardTarget(ChannelHandlerContext ctx) {
            return forwardHandler == null ? ctx : ctx.pipeline().context(forwardHandler);
        }
        @Override protected void onCompressedFrame(int wireBytes, int rawBytes) {
            frames++; wire += wireBytes; raw += rawBytes;
        }
    }

    private static final class IoProbe extends ChannelOutboundHandlerAdapter {
        final Thread testThread = Thread.currentThread();
        int reads;
        int closes;
        int flushDepth;
        @Override public void flush(ChannelHandlerContext ctx) {
            assertSame(testThread, Thread.currentThread(), "真实flush只能由测试主线程驱动");
            flushDepth++;
            try { ctx.flush(); }
            finally { flushDepth--; }
        }
        @Override public void read(ChannelHandlerContext ctx) {
            assertSame(testThread, Thread.currentThread(), "真实read只能由测试主线程驱动");
            reads++; ctx.read();
        }
        @Override public void close(ChannelHandlerContext ctx, ChannelPromise promise) {
            assertSame(testThread, Thread.currentThread(), "真实close只能由测试主线程驱动");
            closes++; ctx.close(promise);
        }
    }

    private static final class HoldingWrites extends ChannelOutboundHandlerAdapter {
        final List<ByteBuf> messages = new ArrayList<>();
        final List<ChannelPromise> promises = new ArrayList<>();
        @Override public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
            messages.add((ByteBuf) msg); promises.add(promise);
        }
        void succeed(int index) { messages.get(index).release(); promises.get(index).setSuccess(); }
        void failRest(Throwable cause) {
            for (int i = 0; i < promises.size(); i++) {
                if (!promises.get(i).isDone()) { messages.get(i).release(); promises.get(i).setFailure(cause); }
            }
        }
        @Override public void handlerRemoved(ChannelHandlerContext ctx) {
            failRest(new java.nio.channels.ClosedChannelException());
        }
    }

    /** 精确API：1.5.7-17的dst在前，布尔值仅表示本次FLUSH/END完成；字节数取position差。 */
    private static final class NativeStream implements AutoCloseable {
        final ZstdCompressCtx ctx = new ZstdCompressCtx();
        NativeStream() {
            ctx.setLevel(3).setWindowLog(20).setMagicless(true)
                    .setContentSize(false).setDictID(false).setChecksum(false);
        }
        byte[] flush(byte[] raw) { return write(raw, EndDirective.FLUSH); }
        byte[] end(byte[] raw) { return write(raw, EndDirective.END); }
        private byte[] write(byte[] raw, EndDirective end) {
            ByteBuffer src = ByteBuffer.allocateDirect(raw.length); src.put(raw).flip();
            ByteBuffer dst = ByteBuffer.allocateDirect(257);
            ByteArrayOutputStream result = new ByteArrayOutputStream();
            while (src.hasRemaining()) {
                int before = src.position(); dst.clear();
                ctx.compressDirectByteBufferStream(dst, src, EndDirective.CONTINUE);
                int produced = dst.position(); dst.flip();
                byte[] chunk = new byte[produced]; dst.get(chunk); result.writeBytes(chunk);
                assertTrue(src.position() != before || produced > 0, "native CONTINUE无进展");
            }
            boolean done;
            do {
                int before = src.position(); dst.clear();
                done = ctx.compressDirectByteBufferStream(dst, src, end);
                int produced = dst.position(); dst.flip();
                byte[] chunk = new byte[produced]; dst.get(chunk); result.writeBytes(chunk);
                assertTrue(done || src.position() != before || produced > 0, "native FLUSH/END无进展");
            } while (!done);
            return result.toByteArray();
        }
        @Override public void close() { ctx.close(); }
    }
}
