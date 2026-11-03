package cn.miku.zstd.mixin;

import net.minecraft.client.multiplayer.ClientHandshakePacketListenerImpl;
import net.minecraft.network.protocol.login.ClientboundLoginCompressionPacket;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.Shadow;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;

/**
 * 登录压缩回调 TAIL 的 switch 嗅探器安装兜底。
 * 原版或 Krypton 重建 decompress 后复核嗅探器相对位置；decoder 等待switch后同步安装。
 */
@Mixin(ClientHandshakePacketListenerImpl.class)
public class MixinHandshakeListener {

    @Shadow
    private net.minecraft.network.Connection connection;

    @Inject(method = "handleCompression", at = @At("HEAD"))
    private void zstd$compressionSetupStarted(ClientboundLoginCompressionPacket packet, CallbackInfo ci) {
        if (this.connection != null) {
            cn.miku.zstd.ZstdLoginNetworking.onCompressionSetupStarted(
                    ((cn.miku.zstd.mixin.ConnectionAccessor) this.connection).getChannel(), this, packet);
        }
    }

    @Inject(method = "handleCompression", at = @At("TAIL"))
    private void zstd$activateAfterCompression(ClientboundLoginCompressionPacket packet, CallbackInfo ci) {
        // 原版/Krypton均在这里完成压缩处理器安装；随后才能发送挂起的LoginQuery答复。
        if (this.connection != null) {
            cn.miku.zstd.ZstdLoginNetworking.markCompressionReady(
                    ((cn.miku.zstd.mixin.ConnectionAccessor) this.connection).getChannel(), this, packet);
        }
    }
}
