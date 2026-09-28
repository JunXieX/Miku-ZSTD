package cn.miku.zstd.mixin;

import net.minecraft.network.protocol.handshake.ClientIntentionPacket;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.ModifyVariable;

/**
 * 在握手包构造时向主机名末尾追加 "\0ZSTD\0" 标记，
 * Velocity 端通过 ConnectionHandshakeEvent 检测该标记以启用 zstd 传输。
 */
@Mixin(ClientIntentionPacket.class)
public class MixinClientIntentionPacket {

    @ModifyVariable(method = "<init>(ILjava/lang/String;ILnet/minecraft/network/protocol/handshake/ClientIntent;)V",
            at = @At("HEAD"), argsOnly = true, ordinal = 0)
    private static String zstd$appendMarker(String hostName) {
        return hostName + "\u0000ZSTD\u0000";
    }
}
