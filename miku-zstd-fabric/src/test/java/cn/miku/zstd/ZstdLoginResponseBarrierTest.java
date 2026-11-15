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

    /**
     * 压缩管线一直不出现时，看门狗必须按 VANILLA 收尾。
     *
     * <p>旧实现在这条路径上无限期等待：服务端发了查询却因为别的协议路径永不发
     * SetCompression 时，本端不回包，服务端到点把连接踢掉——现场只剩一行 Connecting to。
     * 现在 ≤ 看门狗时长内必定给出结论，并按"本轮不参与 zstd"把本地状态清回 PLAIN。</p>
     */
    @Test
    void watchdogAnswersVanillaWhenCompressionNeverArrives() {
        ZstdLoginNetworking.setCompressionWatchdogMillisForTests(0L);
        EmbeddedChannel channel = new EmbeddedChannel();
        try {
            Object loginHandler = new Object();
            channel.attr(ZstdChannelManager.ZSTD_STATE).set(ZstdChannelManager.TransportState.NEGOTIATING);
            ZstdLoginNetworking.beginNegotiation(channel, loginHandler);
            CompletableFuture<FriendlyByteBuf> response = ZstdLoginNetworking.answerAfterCompression(
                    channel, ZstdNegotiateStatus.READY);
            assertFalse(response.isDone(), "看门狗到期前不应提前回包");

            // 0ms 看门狗在同一轮事件循环到期：runPendingTasks 处理即时任务，
            // runScheduledPendingTasks 处理已到期的定时任务（两者分开调用，跨 Netty 版本稳定）。
            channel.runPendingTasks();
            channel.runScheduledPendingTasks();

            assertTrue(response.isDone(), "看门狗到期后必须给出结论");
            FriendlyByteBuf payload = response.join();
            try {
                assertEquals(ZstdNegotiateStatus.VANILLA, payload.readVarInt(),
                        "没有压缩管线时只能回 VANILLA，否则会造成单侧 zstd");
            } finally {
                payload.release();
            }
            assertEquals(ZstdChannelManager.TransportState.PLAIN,
                    channel.attr(ZstdChannelManager.ZSTD_STATE).get());
        } finally {
            ZstdLoginNetworking.setCompressionWatchdogMillisForTests(2000L);
            channel.finishAndReleaseAll();
        }
    }

    /** 新版管线名（compression-encoder/compression-decoder）同样要能让屏障完成。 */
    @Test
    void alternativeCompressionHandlerNamesCompleteTheBarrier() {
        EmbeddedChannel channel = new EmbeddedChannel();
        try {
            Object loginHandler = new Object();
            Object compressionPacket = new Object();
            ZstdLoginNetworking.beginNegotiation(channel, loginHandler);
            CompletableFuture<FriendlyByteBuf> response = ZstdLoginNetworking.answerAfterCompression(
                    channel, ZstdNegotiateStatus.READY);
            assertFalse(response.isDone());

            channel.pipeline().addLast("compression-decoder", new ChannelInboundHandlerAdapter());
            channel.pipeline().addLast("compression-encoder", new ChannelOutboundHandlerAdapter());
            ZstdLoginNetworking.onCompressionSetupStarted(channel, loginHandler, compressionPacket);
            ZstdLoginNetworking.markCompressionReady(channel, loginHandler, compressionPacket);
            channel.runPendingTasks();

            assertTrue(response.isDone(), "候选 handler 名必须与锚点候选一致，否则屏障永不完成");
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
