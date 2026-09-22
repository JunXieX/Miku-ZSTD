package cn.miku.zstd.mixin;

import net.minecraft.client.multiplayer.ClientHandshakePacketListenerImpl;
import net.minecraft.network.protocol.login.ClientboundLoginCompressionPacket;
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
 *
 * <p>⚠️ 这条「晚于后置安装」的保证<b>不是</b>为了与 Krypton 共存（本模组<b>不兼容
 * Krypton</b>，客户端压缩优化已内置，两者会争抢同一条压缩管线）。</p>
 */
@Mixin(ClientHandshakePacketListenerImpl.class)
public class MixinHandshakeListener {

    @Shadow
    private net.minecraft.network.Connection connection;

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
