package cn.miku.zstd;

import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import net.minecraft.network.FriendlyByteBuf;
import mikumc.zstd.protocol.ZstdNegotiateStatus;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;

class ZstdLoginResponseBarrierTest {
    @Test
    void queryAnswerWaitsUntilCompressionSetupCompletes() {
        EmbeddedChannel channel = new EmbeddedChannel();
        try {
            Object loginHandler = new Object();
            Object compressionPacket = new Object();
            ZstdLoginNetworking.beginNegotiation(channel, loginHandler);
            CompletableFuture<FriendlyByteBuf> response = ZstdLoginNetworking.answerAfterCompression(
                    channel, ZstdNegotiateStatus.READY);
            assertFalse(response.isDone(), "raw READY response must not leave before SetCompression");

            ZstdLoginNetworking.onCompressionSetupStarted(channel, loginHandler, compressionPacket);
            assertFalse(response.isDone(), "HEAD of handleCompression only starts setup");

            // Mirrors Krypton/vanilla installing both handlers before handleCompression TAIL.
            channel.pipeline().addLast("decompress", new ChannelInboundHandlerAdapter());
            channel.pipeline().addLast("compress", new ChannelOutboundHandlerAdapter());
            ZstdLoginNetworking.markCompressionReady(channel, loginHandler, compressionPacket);
            channel.runPendingTasks();

            assertTrue(response.isDone());
            FriendlyByteBuf payload = response.join();
            try {
                assertEquals(ZstdNegotiateStatus.READY, payload.readVarInt());
            } finally {
                payload.release();
            }
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void compressionSetupStartedAfterAnEarlierReadyRoundRearmsTheBarrier() {
        EmbeddedChannel channel = new EmbeddedChannel();
        try {
            Object loginHandler = new Object();
            Object firstCompressionPacket = new Object();
            Object secondCompressionPacket = new Object();
            channel.pipeline().addLast("decompress", new ChannelInboundHandlerAdapter());
            channel.pipeline().addLast("compress", new ChannelOutboundHandlerAdapter());
            ZstdLoginNetworking.onCompressionSetupStarted(channel, loginHandler, firstCompressionPacket);
            ZstdLoginNetworking.markCompressionReady(channel, loginHandler, firstCompressionPacket);
            ZstdLoginNetworking.beginNegotiation(channel, loginHandler);
            CompletableFuture<FriendlyByteBuf> first = ZstdLoginNetworking.answerAfterCompression(
                    channel, ZstdNegotiateStatus.READY);
            assertTrue(first.isDone());
            first.join().release();

            ZstdLoginNetworking.onCompressionSetupStarted(channel, loginHandler, secondCompressionPacket);
            CompletableFuture<FriendlyByteBuf> second = ZstdLoginNetworking.answerAfterCompression(
                    channel, ZstdNegotiateStatus.VANILLA);
            assertFalse(second.isDone(), "reconfiguration must close the old response window");

            ZstdLoginNetworking.markCompressionReady(channel, loginHandler, firstCompressionPacket);
            channel.runPendingTasks();
            assertFalse(second.isDone(), "a stale TAIL from an earlier setup on the same listener must be ignored");
            ZstdLoginNetworking.markCompressionReady(channel, loginHandler, secondCompressionPacket);
            channel.runPendingTasks();
            FriendlyByteBuf payload = second.join();
            try {
                assertEquals(ZstdNegotiateStatus.VANILLA, payload.readVarInt());
            } finally {
                payload.release();
            }
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void priorLoginCompressionCallbackCannotReleaseCurrentRound() {
        EmbeddedChannel channel = new EmbeddedChannel();
        try {
            Object previousHandler = new Object();
            Object currentHandler = new Object();
            Object previousCompressionPacket = new Object();
            Object currentCompressionPacket = new Object();
            channel.pipeline().addLast("decompress", new ChannelInboundHandlerAdapter());
            channel.pipeline().addLast("compress", new ChannelOutboundHandlerAdapter());

            ZstdLoginNetworking.onCompressionSetupStarted(channel, previousHandler, previousCompressionPacket);
            ZstdLoginNetworking.markCompressionReady(channel, previousHandler, previousCompressionPacket);
            channel.runPendingTasks();

            ZstdLoginNetworking.beginNegotiation(channel, currentHandler);
            CompletableFuture<FriendlyByteBuf> response = ZstdLoginNetworking.answerAfterCompression(
                    channel, ZstdNegotiateStatus.READY);
            assertFalse(response.isDone(), "the previous listener's ready state is not valid for this login");

            ZstdLoginNetworking.markCompressionReady(channel, previousHandler, previousCompressionPacket);
            channel.runPendingTasks();
            assertFalse(response.isDone(), "a delayed TAIL from the prior round must not release the new answer");

            ZstdLoginNetworking.onCompressionSetupStarted(channel, currentHandler, currentCompressionPacket);
            ZstdLoginNetworking.markCompressionReady(channel, currentHandler, currentCompressionPacket);
            channel.runPendingTasks();
            FriendlyByteBuf payload = response.join();
            try {
                assertEquals(ZstdNegotiateStatus.READY, payload.readVarInt());
            } finally {
                payload.release();
            }
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void closingConnectionCancelsAnUnansweredQueryRound() {
        EmbeddedChannel channel = new EmbeddedChannel();
        ZstdLoginNetworking.beginNegotiation(channel, null);
        CompletableFuture<FriendlyByteBuf> response = ZstdLoginNetworking.answerAfterCompression(
                channel, ZstdNegotiateStatus.READY);
        assertFalse(response.isDone());

        channel.close();
        channel.runPendingTasks();
        assertTrue(response.isCompletedExceptionally());
        channel.finishAndReleaseAll();
    }
}
