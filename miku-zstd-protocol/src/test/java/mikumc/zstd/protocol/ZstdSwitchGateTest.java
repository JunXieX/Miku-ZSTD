package mikumc.zstd.protocol;

import com.velocitypowered.proxy.protocol.FakeMinecraftPacket;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link ZstdSwitchGate}：切换闸门的行为契约。
 *
 * <p>它承担"正常路径无歧义"的全部保证：客户端确认之前服务端不出站登录期大包、
 * 确认之后立即按原顺序放行。这里逐条验证——扣包、放行、超时、销毁收尾、魔数匹配。</p>
 */
class ZstdSwitchGateTest {

    @Test
    @DisplayName("闸门：确认之前扣住登录期大包，确认之后按原顺序放行并自移除")
    void holdsUntilConfirmed() {
        ZstdSwitchGate gate = ZstdSwitchGate.create();
        EmbeddedChannel ch = new EmbeddedChannel(gate);

        // ⚠️ 不能用 writeOutbound 的返回值判断是否被扣住：EmbeddedChannel 对"未 flush 的写入"
        // 一律返回 false。真正可靠的判据是 readOutbound() 是否拿得到数据。
        ch.writeOutbound(new FakeMinecraftPacket(1));
        ch.writeOutbound(new FakeMinecraftPacket(2));
        assertNull(ch.readOutbound(), "闸门放行前不得有任何出站数据");
        assertEquals(2, gate.heldCount(), "两个包都必须被扣住");

        assertFalse(gate.isConfirmed(), "尚未确认");
        gate.confirm();

        Object a = ch.readOutbound();
        Object b = ch.readOutbound();
        assertEquals(1, ((FakeMinecraftPacket) a).id, "必须按原顺序放行（第 1 帧）");
        assertEquals(2, ((FakeMinecraftPacket) b).id, "必须按原顺序放行（第 2 帧）");
        assertTrue(gate.isConfirmed(), "确认后应标记为已确认");
        assertNull(ch.pipeline().get(ZstdSwitchGate.class), "放行后必须自移除（热路径不再经过它）");

        // 放行之后的数据直接透传
        ch.writeOutbound(new FakeMinecraftPacket(3));
        assertEquals(3, ((FakeMinecraftPacket) ch.readOutbound()).id);
        ch.finishAndReleaseAll();
    }

    @Test
    @DisplayName("闸门：连接已关闭时，销毁收尾绝不得改动管线（否则会抛 ChannelPipelineException）")
    void noRollbackOnChannelClosed() {
        ZstdSwitchGate gate = ZstdSwitchGate.create();
        java.util.concurrent.atomic.AtomicBoolean rolledBack =
                new java.util.concurrent.atomic.AtomicBoolean();
        gate.setRollback(() -> rolledBack.set(true));
        EmbeddedChannel ch = new EmbeddedChannel(gate);

        // ⚠️ 用 voidPromise：EmbeddedChannel 会对"普通 promise 失败"记录异常并在后续调用时
        // 重放出来（那是测试框架的行为，不是缺陷）。voidPromise 不参与记录，
        // 因此这里能干净地验证"销毁时不得回滚"。
        ch.writeOneOutbound(new FakeMinecraftPacket(11), ch.voidPromise());
        assertEquals(1, gate.heldCount(), "确认前必须扣住");

        // 关闭 = 连接不可用。EmbeddedChannel 在 close 时会走 pipeline destroy 路径，
        // 与线上"玩家断线→channelUnregistered→destroy"完全同型。
        // 此时绝不得触发回滚：已注销的 pipeline 不接受任何 add/replace/remove，
        // 原版压缩处理器又是非 @Sharable，重复插入必然抛 ChannelPipelineException。
        ch.close();

        assertNull(ch.pipeline().get(ZstdSwitchGate.class), "关闭后闸门应已从管线摘除");
        assertFalse(rolledBack.get(), "连接已关闭：不得回滚（回滚只会抛 ChannelPipelineException）");
        assertFalse(gate.isConfirmed(), "未确认");
        ch.finishAndReleaseAll();
    }

    @Test
    @DisplayName("闸门：回滚动作自身抛异常时不得冒泡（只记 debug）")
    void rollbackFailureIsSwallowed() {
        ZstdSwitchGate gate = ZstdSwitchGate.create();
        gate.setRollback(() -> {
            throw new IllegalStateException("boom");
        });
        EmbeddedChannel ch = new EmbeddedChannel(gate);

        ch.writeOutbound(new FakeMinecraftPacket(12));
        // 主动 abort（等价于客户端拒绝 / 安装失败）：不得让回滚异常逃逸
        gate.abort();

        assertNull(ch.pipeline().get(ZstdSwitchGate.class), "abort 后必须自移除");
        assertEquals(12, ((FakeMinecraftPacket) ch.readOutbound()).id, "扣住的包必须补发");
        ch.finishAndReleaseAll();
    }

    @Test
    @DisplayName("闸门：Netty 控制消息必须透传（扣住会卡死连接建立且无日志）")
    void doesNotHoldControlMessages() {
        ZstdSwitchGate gate = ZstdSwitchGate.create();
        EmbeddedChannel ch = new EmbeddedChannel(gate);

        Runnable task = () -> { };
        ch.writeOutbound(task);
        assertSame(task, ch.readOutbound(), "控制消息必须原样透传");

        assertEquals(0, gate.heldCount(), "控制消息不得被扣住");
        ch.finishAndReleaseAll();
    }

    @Test
    @DisplayName("闸门：处理器被摘除且连接仍活跃时，扣住的包必须补发（不得丢失）")
    void releasesOnRemovalWhileActive() {
        ZstdSwitchGate gate = ZstdSwitchGate.create();
        EmbeddedChannel ch = new EmbeddedChannel(gate);

        ByteBuf payload = Unpooled.buffer().writeByte(0x01);
        FakeMinecraftPacket packet = new FakeMinecraftPacket(9, payload);
        // ⚠️ 必须用 writeOutbound：EmbeddedChannel 只把经它写入的消息记到 outbound 队列，
        // pipeline.write / ctx.write 对"是否被扣住"的观察都不可靠。
        ch.writeOutbound(packet);
        assertEquals(1, gate.heldCount(), "确认前必须扣住");

        // 摘除处理器 = 与 channelInactive 同类的销毁收尾。
        ch.pipeline().remove(gate);

        assertEquals(9, ((FakeMinecraftPacket) ch.readOutbound()).id, "扣住的包必须补发，不得丢失");
        assertFalse(gate.isConfirmed(), "未收到确认就销毁：不得标记为已确认");
        ch.finishAndReleaseAll();
    }

    @Test
    @DisplayName("闸门：通道已关闭时扣住的包必须被释放（不泄漏），promise 以失败收尾")
    void releasesOnChannelClosed() {
        ZstdSwitchGate gate = ZstdSwitchGate.create();
        EmbeddedChannel ch = new EmbeddedChannel(gate);

        ByteBuf payload = Unpooled.buffer().writeByte(0x01);
        FakeMinecraftPacket packet = new FakeMinecraftPacket(9, payload);
        io.netty.channel.ChannelPromise promise = ch.newPromise();
        // 直接走管道（不经 EmbeddedChannel.writeAndFlush 的 PromiseNotification 包装），
        // 这样才能在闸门扣包后精确断言调用方拿到的是失败。
        ch.pipeline().writeAndFlush(packet, promise);
        assertEquals(1, gate.heldCount(), "确认前必须扣住");

        ch.close().awaitUninterruptibly();

        assertFalse(gate.isConfirmed(), "未收到确认就关闭：不得标记为已确认");
        assertEquals(0, payload.refCnt(), "包不会被写出：必须由闸门释放（不泄漏）");
        assertTrue(promise.isDone(), "挂起的 promise 必须被收尾（不能永久挂住调用方）");
        assertFalse(promise.isSuccess(), "通道已关闭：promise 必须以失败收尾");
        ch.finishAndReleaseAll();
    }

    @Test
    @DisplayName("闸门：超时后无条件放行并判定未确认（服务端据此双向回滚到原版）")
    void releasesOnTimeout() throws InterruptedException {
        ZstdSwitchGate gate = ZstdSwitchGate.create();
        EmbeddedChannel ch = new EmbeddedChannel(gate);

        ch.writeOutbound(new FakeMinecraftPacket(7));
        assertNull(ch.readOutbound());

        // EmbeddedChannel 的调度任务只有 runPendingTasks() 才会执行
        // （它没有真实事件循环），因此这里轮询推进，而不是 sleep 等真实时间。
        long deadline = System.currentTimeMillis() + ZstdSwitchGate.RELEASE_TIMEOUT_MS + 3000;
        while (ch.pipeline().get(ZstdSwitchGate.class) != null
                && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
            ch.runPendingTasks();
        }

        assertNull(ch.pipeline().get(ZstdSwitchGate.class), "超时后必须自移除");
        assertFalse(gate.isConfirmed(), "超时 = 未确认：服务端必须双向回滚，绝不能留下一半 zstd 一半原版");
        assertEquals(7, ((FakeMinecraftPacket) ch.readOutbound()).id, "超时后扣住的包仍必须补发");
        ch.finishAndReleaseAll();
    }

    @Test
    @DisplayName("魔数：只认 4 字节魔数，长度或内容不符一律拒绝")
    void confirmPayloadMatching() {
        ByteBuf buf = Unpooled.buffer(4).writeInt(ZstdSwitchGate.MAGIC);
        byte[] ok = new byte[4];
        buf.readBytes(ok);
        buf.release();

        assertTrue(ZstdStepFrames.matchesMagic(ok), "4 字节魔数必须命中");
        assertFalse(ZstdStepFrames.matchesMagic(new byte[]{0, 0, 0, 0}), "内容不符必须拒绝");
        assertFalse(ZstdStepFrames.matchesMagic(new byte[]{ok[0], ok[1], ok[2]}), "长度不符必须拒绝");
        assertFalse(ZstdStepFrames.matchesMagic(new byte[]{ok[0], ok[1], ok[2], ok[3], 0}),
                "多余字节必须拒绝");
        assertFalse(ZstdStepFrames.matchesMagic(null), "null 必须拒绝");
    }
}
