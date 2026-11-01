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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import java.util.ArrayList;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class ZstdPaperNegotiatorTest {
    private final List<QueryAnswer> queries = new ArrayList<>();
    private ZstdPaperChannelManager manager;
    private ZstdPaperNegotiator negotiator;
    private EmbeddedChannel channel;

    @BeforeEach
    void open() {
        manager = new ZstdPaperChannelManager();
        assertTrue(manager.claimNegotiateTxId(17));
        negotiator = new ZstdPaperNegotiator();
        channel = new EmbeddedChannel(negotiator);
        channel.attr(ZstdPaperChannelManager.KEY).set(manager);
    }

    @AfterEach
    void close() {
        try { channel.finishAndReleaseAll(); }
        finally {
            for (QueryAnswer q : queries) if (q.refCnt() > 0) q.release();
            try { manager.close(); } finally { ZstdCtxPool.shutdown(); }
        }
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
