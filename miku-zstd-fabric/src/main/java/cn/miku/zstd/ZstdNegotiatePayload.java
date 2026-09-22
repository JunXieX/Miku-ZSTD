package cn.miku.zstd;

import net.minecraft.network.FriendlyByteBuf;
import net.minecraft.network.protocol.login.custom.CustomQueryPayload;
import net.minecraft.resources.Identifier;

/**
 * {@code zstd:negotiate} 登录查询的载荷标识。
 *
 * <p>这里只需要 {@link #ID}——Fabric API 的 {@code ClientLoginNetworking}
 * 接收器按该标识注册，真正的解析在 {@link ZstdLoginNetworking} 里直接读
 * {@code FriendlyByteBuf}，不需要本类承载数据。</p>
 *
 * <h2>3.1.0 清理</h2>
 * <p>旧 javadoc 描述的是已被弃用的实现路线（{@code MixinCustomQueryCapture} +
 * {@code MixinConnectionLogin}，在 0.3.4 起改用 Fabric API 官方登录查询接收器时
 * 就已删除），属于过期文档，容易误导后续维护。</p>
 */
public record ZstdNegotiatePayload(byte[] data) implements CustomQueryPayload {

    public static final Identifier ID = Identifier.fromNamespaceAndPath("zstd", "negotiate");

    @Override
    public Identifier id() {
        return ID;
    }

    @Override
    public void write(FriendlyByteBuf buf) {
        buf.writeBytes(data);
    }
}
