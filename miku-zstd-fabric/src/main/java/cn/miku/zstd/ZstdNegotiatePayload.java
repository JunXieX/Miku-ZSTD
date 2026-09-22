package cn.miku.zstd;

import net.minecraft.resources.Identifier;

/**
 * {@code zstd:negotiate} 登录查询的<b>通道标识</b>。
 *
 * <h2>为什么只留一个常量</h2>
 * <p>旧实现是一个 {@code record ZstdNegotiatePayload implements CustomQueryPayload}
 * 并带 {@code write()}——但那条路线早已废弃：</p>
 * <ul>
 *   <li>客户端注册接收器只需要 {@link #ID}（Fabric API 的
 *       {@code ClientLoginNetworking.registerGlobalReceiver}），载荷由
 *       {@link ZstdLoginNetworking} 直接从 {@code FriendlyByteBuf} 读出；</li>
 *   <li>服务端（Velocity / Paper）自己手工拼裸 ByteBuf 写出去，不经过任何 MC 载荷类型。</li>
 * </ul>
 * <p>于是那个 record 从来没被实例化过，它的 {@code data} 组件与 {@code write()}
 * 都是死代码——只会在读代码时让人以为"还有一条 payload 路线"。</p>
 */
public final class ZstdNegotiatePayload {

    /** 协商查询的通道标识（服务端侧对应的字符串是 {@code "zstd:negotiate"}）。 */
    public static final Identifier ID = Identifier.fromNamespaceAndPath("zstd", "negotiate");

    private ZstdNegotiatePayload() {
    }
}
