package cn.miku.zstd.mixin;

import net.minecraft.client.multiplayer.ClientHandshakePacketListenerImpl;
import net.minecraft.network.protocol.login.ClientboundLoginCompressionPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.Shadow;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;

/**
 * 登录压缩设置监听：TAIL 时触发 zstd 接管（Krypton 共存主路径）。
 */
@Mixin(ClientHandshakePacketListenerImpl.class)
public class MixinHandshakeListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(cn.miku.zstd.MikuZstd.LOGGER_NAME);

    @Shadow
    private net.minecraft.network.Connection connection;

    @Inject(method = "handleCompression", at = @At("HEAD"))
    private void zstd$debugHandleCompression(ClientboundLoginCompressionPacket packet, CallbackInfo ci) {
        LOGGER.debug("[Zstd] handleCompression: threshold={}", packet.getCompressionThreshold());
    }

    @Inject(method = "handleCompression", at = @At("TAIL"))
    private void zstd$activateAfterCompression(ClientboundLoginCompressionPacket packet, CallbackInfo ci) {
        // 原版/Krypton 的压缩 handler 已在本方法内安装完毕；
        // 通过 eventLoop 排队激活，保证晚于 Krypton 等后置注入的覆盖动作。
        if (this.connection != null) {
            cn.miku.zstd.ZstdClientActivation.activate(
                    ((cn.miku.zstd.mixin.ConnectionAccessor) this.connection).getChannel());
        }
    }
}
