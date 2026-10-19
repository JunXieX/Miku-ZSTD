package com.velocitypowered.proxy.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCounted;

/**
 * 测试用假包：类名落在 {@code ZstdSwitchGate.isMinecraftPacket} 识别的 Velocity 包前缀内，
 * 因此与线上真实 MC 包走同一条"是否扣住"的判定分支。
 */
public final class FakeMinecraftPacket implements ReferenceCounted {

    public final int id;
    private final ByteBuf payload;

    public FakeMinecraftPacket(int id) {
        this(id, Unpooled.buffer().writeByte(id));
    }

    public FakeMinecraftPacket(int id, ByteBuf payload) {
        this.id = id;
        this.payload = payload;
    }

    @Override
    public int refCnt() {
        return payload.refCnt();
    }

    @Override
    public ReferenceCounted retain() {
        payload.retain();
        return this;
    }

    @Override
    public ReferenceCounted retain(int increment) {
        payload.retain(increment);
        return this;
    }

    @Override
    public ReferenceCounted touch() {
        return this;
    }

    @Override
    public ReferenceCounted touch(Object hint) {
        return this;
    }

    @Override
    public boolean release() {
        return payload.release();
    }

    @Override
    public boolean release(int decrement) {
        return payload.release(decrement);
    }
}
