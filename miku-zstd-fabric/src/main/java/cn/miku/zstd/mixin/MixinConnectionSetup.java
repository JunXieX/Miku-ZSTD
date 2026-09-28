package cn.miku.zstd.mixin;

import io.netty.channel.Channel;
import net.minecraft.network.Connection;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.Shadow;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;

/**
 * 服务端下发 Set Compression（客户端 setupCompression 被调用）后，
 * 用 zstd 编解码器替换原版 zlib 的 compress/decompress 处理器。
 */
@Mixin(Connection.class)
public class MixinConnectionSetup {

    @Shadow
    private Channel channel;

    @Inject(method = "setupCompression", at = @At("TAIL"))
    private void zstd$onSetupCompressionTail(int threshold, boolean validateDecompression, CallbackInfo ci) {
        if (this.channel != null) {
            cn.miku.zstd.ZstdClientActivation.activate(this.channel);
        }
    }
}
