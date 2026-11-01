package cn.miku.zstd.mixin;

import io.netty.channel.Channel;
import net.minecraft.network.Connection;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.Shadow;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;

/**
 * 原版 setupCompression 的 TAIL：压缩处理器安装后，准备下行 zstd 解码器与 switch 嗅探器。
 * 上行的 compress、prepender 与加密处理全部沿用原版。
 */
@Mixin(Connection.class)
public class MixinConnectionSetup {

    @Shadow
    private Channel channel;

    @Inject(method = "setupCompression", at = @At("TAIL"))
    private void zstd$onSetupCompressionTail(int threshold, boolean validateDecompression, CallbackInfo ci) {
        if (this.channel != null) {
            cn.miku.zstd.ZstdClientActivation.onCompressionThreshold(this.channel, threshold);
        }
    }
}
