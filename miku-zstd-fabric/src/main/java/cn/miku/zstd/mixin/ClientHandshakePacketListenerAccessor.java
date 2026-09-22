package cn.miku.zstd.mixin;

import io.netty.channel.Channel;
import net.minecraft.client.multiplayer.ClientHandshakePacketListenerImpl;
import net.minecraft.network.Connection;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.gen.Accessor;

/**
 * 26.x 的 ClientHandshakePacketListenerImpl 不再继承 ClientCommonPacketListenerImpl，
 * 需要单独的 accessor 获取其 connection 字段。
 */
@Mixin(ClientHandshakePacketListenerImpl.class)
public interface ClientHandshakePacketListenerAccessor {
    @Accessor("connection")
    Connection getConnection();
}
