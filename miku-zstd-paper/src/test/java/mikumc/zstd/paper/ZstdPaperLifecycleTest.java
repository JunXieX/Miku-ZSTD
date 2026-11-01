package mikumc.zstd.paper;

import com.github.luben.zstd.ZstdCompressCtx;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import mikumc.zstd.protocol.ZstdCtxPool;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.junit.jupiter.api.Assertions.*;

class ZstdPaperLifecycleTest {
    private final EmbeddedChannel channel = new EmbeddedChannel();
    private final ZstdPaperChannelManager manager = new ZstdPaperChannelManager();
    private final ChannelOutboundHandlerAdapter vanilla = new ChannelOutboundHandlerAdapter();

    @BeforeEach
    void bind() throws Exception {
        var bind = MikuZstdPaper.class.getDeclaredMethod("bindManager", Channel.class, ZstdPaperChannelManager.class);
        bind.setAccessible(true);
        bind.invoke(null, channel, manager);
        channel.pipeline().addLast("compress", vanilla);
        channel.pipeline().addLast("decompress", new ChannelInboundHandlerAdapter());
        channel.pipeline().addLast("encoder", new ChannelOutboundHandlerAdapter());
        channel.pipeline().addLast("miku-zstd-negotiator", new ZstdPaperNegotiator());
    }

    @AfterEach
    void close() {
        try { channel.finishAndReleaseAll(); }
        finally {
            try { manager.close(); } finally { ZstdCtxPool.shutdown(); }
        }
    }

    private void revert() throws Exception {
        var revert = MikuZstdPaper.class.getDeclaredMethod("revertConnection", Channel.class);
        revert.setAccessible(true);
        revert.invoke(null, channel);
        channel.runPendingTasks();
    }

    private static boolean closed(ZstdPaperChannelManager manager) {
        synchronized (manager.compressLock()) { return manager.compressLock().isClosed(); }
    }

    @Test
    void normalCloseDetachesAndReleasesManager() {
        assertSame(manager, channel.attr(ZstdPaperChannelManager.KEY).get());
        channel.close();
        channel.runPendingTasks();
        assertNull(channel.attr(ZstdPaperChannelManager.KEY).get());
        assertTrue(closed(manager));
        assertFalse(manager.isReplaced());
    }

    @Test
    void repeatedCloseDoesNotReturnOneContextToTwoBorrowers() {
        manager.close();
        manager.close();
        assertTrue(closed(manager));
        ZstdCompressCtx first = ZstdCtxPool.borrowCompress(3);
        ZstdCompressCtx second = ZstdCtxPool.borrowCompress(3);
        try { assertNotSame(first, second); }
        finally {
            first.close();
            if (second != first) second.close();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void revertAlwaysReleasesButOnlyDisconnectsActivated(boolean activated) throws Exception {
        if (activated) {
            assertTrue(manager.installSwitchGate(channel));
            assertTrue(manager.installEncoder(channel));
            manager.setReplaced(true);
        }
        revert();
        assertNull(channel.attr(ZstdPaperChannelManager.KEY).get());
        assertTrue(closed(manager));
        assertFalse(manager.isReplaced());
        assertEquals(!activated, channel.isActive());
        assertNull(channel.pipeline().get("miku-zstd-negotiator"));
        assertNull(channel.pipeline().get("zstd_encoder"));
        if (!activated) {
            assertSame(vanilla, channel.pipeline().get("compress"));
            ByteBuf packet = Unpooled.buffer(1).writeByte(7);
            assertTrue(channel.writeOutbound(packet));
            ByteBuf forwarded = channel.readOutbound();
            try { assertSame(packet, forwarded); }
            finally { if (forwarded != null) forwarded.release(); }
            revert();
            assertTrue(channel.isActive());
        }
    }

    @Test
    void revertRemovesCloseListenerFromSurvivingVanillaChannel() throws Exception {
        revert();
        assertTrue(channel.isActive());
        assertTrue(closed(manager));
        ZstdPaperChannelManager unbound = new ZstdPaperChannelManager();
        try {
            // 未绑定新close监听器；遗留旧监听器会在close时误关这个真实manager。
            channel.attr(ZstdPaperChannelManager.KEY).set(unbound);
            channel.close();
            channel.runPendingTasks();
            assertSame(unbound, channel.attr(ZstdPaperChannelManager.KEY).get());
            assertFalse(closed(unbound));
        } finally {
            channel.attr(ZstdPaperChannelManager.KEY).getAndSet(null);
            unbound.close();
        }
    }
}
