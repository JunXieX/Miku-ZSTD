package cn.miku.zstd.mixin;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelPipeline;
import net.minecraft.network.Connection;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.Shadow;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;
import cn.miku.zstd.ZstdBatchDecoder;
import cn.miku.zstd.ZstdBatchEncoder;
import cn.miku.zstd.ZstdChannelManager;
import cn.miku.zstd.MikuZstd;

/**
 * 服务端下发 Set Compression（客户端 setupCompression 被调用）后，
 * 用 zstd 编解码器替换原版 zlib 的 compress/decompress 处理器。
 */
@Mixin(Connection.class)
public class MixinConnectionSetup {

    @Shadow
    private Channel channel;

    @Inject(method = "setupCompression", at = @At("HEAD"))
    private void zstd$onSetupCompressionHead(int threshold, boolean validateDecompression, CallbackInfo ci) {
        MikuZstd.LOGGER.debug("[Zstd] setupCompression entered: threshold={} state={}", threshold,
                this.channel == null ? "channel-null" : this.channel.attr(ZstdChannelManager.ZSTD_STATE).get());
    }

    @Inject(method = "setupCompression", at = @At("TAIL"))
    private void zstd$onSetupCompressionTail(int threshold, boolean validateDecompression, CallbackInfo ci) {
        if (this.channel != null) {
            cn.miku.zstd.ZstdClientActivation.activate(this.channel);
        }
    }
}
