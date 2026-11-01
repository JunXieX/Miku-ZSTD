package cn.miku.zstd.mixin;

import net.minecraft.client.multiplayer.ClientHandshakePacketListenerImpl;
import net.minecraft.network.protocol.login.ClientboundLoginCompressionPacket;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.Shadow;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;

/**
 * 登录压缩回调 TAIL 的下行安装兜底。
 * 原版或 Krypton 重建 decompress 后再次复核相对位置，不改动任何上行处理器。
 */
@Mixin(ClientHandshakePacketListenerImpl.class)
public class MixinHandshakeListener {

    @Shadow
    private net.minecraft.network.Connection connection;

    @Inject(method = "handleCompression", at = @At("TAIL"))
    private void zstd$activateAfterCompression(ClientboundLoginCompressionPacket packet, CallbackInfo ci) {
        // 完整压缩设置回调末尾再次复核下行处理器的位置。
        if (this.connection != null) {
            cn.miku.zstd.ZstdClientActivation.prepareForSwitch(
                    ((cn.miku.zstd.mixin.ConnectionAccessor) this.connection).getChannel());
        }
    }
}
