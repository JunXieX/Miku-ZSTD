package cn.miku.zstd;

import net.minecraft.resources.Identifier;

/**
 * {@code zstd:diag} 登录查询的<b>通道标识</b>。
 *
 * <p>与 {@link ZstdNegotiatePayload} 同构：客户端注册接收器只需要 {@link #ID}，
 * 载荷由 {@link ZstdLoginNetworking} 直接从 {@code FriendlyByteBuf} 读；服务端自己拼裸包写出。</p>
 *
 * <p>这条通道只在一种情况下出现：服务端判定某条连接"卡住"（名字进来了、人没进来），
 * 于是向该连接索取本机调试日志的尾部。也就是说，收到它时玩家大概率正卡在进服界面——
 * 这正是它存在的理由：那一侧的日志只有这里能带出来。</p>
 */
public final class ZstdDiagPayload {

    /** 卡住取证查询的通道标识（协议侧对应字符串 {@code "zstd:diag"}）。 */
    public static final Identifier ID = Identifier.fromNamespaceAndPath("zstd", "diag");

    private ZstdDiagPayload() {
    }
}
