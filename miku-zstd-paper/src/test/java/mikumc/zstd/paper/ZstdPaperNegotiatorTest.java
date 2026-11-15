package mikumc.zstd.paper;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.AbstractReferenceCounted;
import mikumc.zstd.protocol.ZstdCtxPool;
import mikumc.zstd.protocol.ZstdSwitchGate;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.io.TempDir;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class ZstdPaperNegotiatorTest {
    private final List<QueryAnswer> queries = new ArrayList<>();
    private ZstdPaperChannelManager manager;
    private ZstdPaperNegotiator negotiator;
    private EmbeddedChannel channel;
    private ZstdPaperConfig previousConfig;

    @TempDir
    Path directory;

    @BeforeEach
    void open() {
        previousConfig = ZstdPaperConfig.INSTANCE;
        manager = new ZstdPaperChannelManager();
        assertTrue(manager.claimNegotiateTxId(17));
        negotiator = new ZstdPaperNegotiator();
        channel = new EmbeddedChannel(negotiator);
        channel.attr(ZstdPaperChannelManager.KEY).set(manager);
    }

    @AfterEach
    void close() {
        ZstdPaperConfig.INSTANCE = previousConfig;
        try { channel.finishAndReleaseAll(); }
        finally {
            for (QueryAnswer q : queries) if (q.refCnt() > 0) q.release();
            try { manager.close(); } finally { ZstdCtxPool.shutdown(); }
        }
    }

    /**
     * 应答预算用尽时必须"回落原版并保留连接"。
     *
     * <p>旧实现在这条路径上直接 {@code close()}：客户端只是慢了一拍（加载区块 / GC /
     * 初次解析整合包资源），整条登录就被踢掉，而两端日志里只有一行 Connecting to。
     * 现在只有"对端已切 zstd"（编码器装好）才允许断连。</p>
     */
    @Test
    void missingAnswerFallsBackToVanillaAndKeepsTheConnection() throws Exception {
        Path file = directory.resolve("config.yml");
        Files.writeString(file, "login: {answer_timeout_ms: 1000}\n", StandardCharsets.UTF_8);
        ZstdPaperConfig.load(file);
        assertEquals(1000, ZstdPaperConfig.INSTANCE.answerTimeoutMs);

        var activate = ZstdPaperNegotiator.class.getDeclaredMethod("tryActivate", ChannelHandlerContext.class, int.class);
        activate.setAccessible(true);
        // answer_timeout_ms=1000ms → 预算 10 轮，第 10 轮即判定用尽。
        activate.invoke(negotiator, channel.pipeline().context(negotiator), 10);

        assertTrue(channel.isOpen(), "缺少应答时不得断开连接，玩家应当照常以原版进服");
        assertFalse(manager.isReplaced(), "未收到 READY 时不得把这条连接标记成 zstd");
    }

    private void retire(boolean settled) throws Exception {
        if (!settled) return;
        var method = ZstdPaperNegotiator.class.getDeclaredMethod("retire", ChannelHandlerContext.class);
        method.setAccessible(true);
        method.invoke(negotiator, channel.pipeline().context(negotiator));
        assertSame(negotiator, channel.pipeline().get(ZstdPaperNegotiator.class));
    }

    private QueryAnswer query(int tx, byte[] data) {
        QueryAnswer q = new QueryAnswer(tx, data);
        queries.add(q);
        return q;
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void onlyOwnQueryIsConsumed(boolean settled) throws Exception {
        retire(settled);
        for (byte[] data : new byte[][]{null, new byte[]{2}}) {
            QueryAnswer foreign = query(73, data);
            assertTrue(channel.writeInbound(foreign));
            assertSame(foreign, channel.readInbound());
            assertEquals(1, foreign.refCnt());
        }
        QueryAnswer own = query(17, new byte[]{2});
        assertFalse(channel.writeInbound(own));
        assertEquals(0, own.refCnt());
        assertEquals(1, own.releases);
        ByteBuf packet = Unpooled.buffer(1).writeByte(7);
        channel.writeInbound(packet);
        ByteBuf forwarded = channel.readInbound();
        try { assertSame(packet, forwarded); }
        finally { if (forwarded != null) forwarded.release(); }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void onlyAcceptedSwitchIsConsumed(boolean settled) throws Exception {
        channel.pipeline().addFirst("encoder", new ChannelOutboundHandlerAdapter());
        channel.pipeline().addFirst("decompress", new ChannelInboundHandlerAdapter());
        assertTrue(manager.installSwitchGate(channel));
        manager.expectSwitchTxId(23);
        manager.setReplaced(true);
        ZstdSwitchGate gate = channel.pipeline().get(ZstdSwitchGate.class);
        retire(settled);
        // 包层兜底当前读取 success + magic；实际帧层确认由登录 E2E 覆盖。
        byte[] payload = {1, 0x5A, 0x53, 0x57, 0x43};
        QueryAnswer foreign = query(24, payload);
        assertTrue(channel.writeInbound(foreign));
        assertSame(foreign, channel.readInbound());
        assertEquals(1, foreign.refCnt());
        assertFalse(gate.isConfirmed());
        QueryAnswer own = query(23, payload);
        assertFalse(channel.writeInbound(own));
        assertEquals(0, own.refCnt());
        assertEquals(1, own.releases);
        assertTrue(gate.isConfirmed());
    }

    private static final class QueryAnswer extends AbstractReferenceCounted {
        private final int transactionId;
        private final byte[] data;
        private int releases;
        private QueryAnswer(int tx, byte[] data) { this.transactionId = tx; this.data = data; }
        @Override protected void deallocate() { releases++; }
        @Override public QueryAnswer touch(Object hint) { return this; }
    }
}
