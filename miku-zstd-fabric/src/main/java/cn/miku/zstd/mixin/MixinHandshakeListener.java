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
 * 登录压缩设置监听：TAIL 时触发 zstd 接管。
 *
 * <p>这是两条激活触发点中较晚的一条（另一条见 {@code MixinConnectionSetup}）：
 * 它在本方法整体返回后（本轮所有注入都已完成）才通过 eventLoop 排队执行，
 * 因此晚于任何在登录压缩流程中后置安装的压缩处理器，保证 zstd 最终生效。</p>
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
        // 压缩 handler 已在本方法内安装完毕；通过 eventLoop 排队激活，
        // 保证晚于任何在本轮登录压缩流程中后置安装的处理器。
        if (this.connection != null) {
            cn.miku.zstd.ZstdClientActivation.activate(
                    ((cn.miku.zstd.mixin.ConnectionAccessor) this.connection).getChannel());
        }
    }
}
