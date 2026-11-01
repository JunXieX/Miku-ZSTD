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
 * 因此晚于任何在登录压缩流程中后置安装的压缩处理器。</p>
 *
 * <p>晚到还有一个现在的用途：重放登录（ViaFabricPlus 协议切换、同一条连接重新协商）时
 * 原版会把 {@code compress}/{@code decompress} 拆掉重装，新实例可能落到我们前面；
 * 这一次补装会把位置复核一遍（见 {@code ZstdClientActivation#installEncoder}）。
 * 我们不替换原版处理器，所以与 Krypton（在 {@code setupCompression} 的 HEAD 用同名
 * handler 装上 libdeflate 版压缩器）天然共存：出站跳过当前占着那个名字的一层，
 * 入站仍由我们的解码器先解，Krypton 其余优化保留。</p>
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
            cn.miku.zstd.ZstdClientActivation.prepareForSwitch(
                    ((cn.miku.zstd.mixin.ConnectionAccessor) this.connection).getChannel());
        }
    }
}
