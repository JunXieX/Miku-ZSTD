package cn.miku.zstd.mixin;

import cn.miku.zstd.MikuZstd;
import cn.miku.zstd.ZstdConfig;
import io.netty.channel.ChannelHandlerContext;
import net.minecraft.network.Connection;
import net.minecraft.network.protocol.Packet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.Shadow;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;

/**
 * 连接诊断（默认关闭，由 {@code logging.debug: true} 开启）。
 *
 * <h2>3.1.0 变更</h2>
 * <p>本类原本是排查阶段加上的诊断代码，却随正式版一起发布，造成两个问题：</p>
 * <ul>
 *   <li>把 MC 有意降级为 debug 的连接异常、以及每一次正常断连都提升为 ERROR，
 *       日志被无意义堆栈淹没；</li>
 *   <li>{@code channelRead0} 的逐包记录在进入 play 阶段后，会对<b>每一个</b>
 *       入站包做 {@code getSimpleName()} 加 4 次 {@code String.contains()} 的
 *       字符串匹配且永不停止——netty 线程上的长期逐包开销。</li>
 * </ul>
 * <p>现在：逐包记录与断连详情只在 debug 开启时输出；{@code exceptionCaught} 在<b>通道仍活跃</b>时
 * 保持 ERROR（真正的异常必须留痕），通道已关闭时的收尾噪音降为 debug——与服务端
 * {@code ZstdBatchDecoderBase#exceptionCaught} 的约定一致，否则每次退出服务器都会刷一条 ERROR + 堆栈。</p>
 */
@Mixin(Connection.class)
public class MixinConnectionDebug {

    private static final Logger LOGGER = LoggerFactory.getLogger(MikuZstd.LOGGER_NAME);

    @Inject(method = "exceptionCaught(Lio/netty/channel/ChannelHandlerContext;Ljava/lang/Throwable;)V",
            at = @At("HEAD"))
    private void zstd$logException(ChannelHandlerContext ctx, Throwable t, CallbackInfo ci) {
        // ⚠️ 与服务端同一约定：通道已关闭时的异常是收尾噪音 —— 本模组在断线时会失败化挂起的写
        // promise，它们挂着的 FIRE_EXCEPTION_ON_FAILURE 会把异常从 pipeline 头部广播回来，
        // 正好撞在这里。那种情况降为 debug，否则每次退出服务器都要刷一条 ERROR + 堆栈。
        // 通道仍活跃时保持 ERROR：真异常必须留痕。
        if (!ctx.channel().isActive()) {
            LOGGER.debug("[Zstd] connection exception after channel closed: {}", t.toString());
        } else {
            LOGGER.error("[Zstd] connection exception (normally swallowed at debug level):", t);
        }
    }

    @Inject(method = "channelInactive", at = @At("HEAD"))
    private void zstd$debugInactive(ChannelHandlerContext ctx, CallbackInfo ci) {
        if (ZstdConfig.INSTANCE.debug) {
            LOGGER.info("[Zstd] client channel closed (remote={})", ctx.channel().remoteAddress());
        }
    }

    private int playPacketsLogged;

    @Inject(method = "channelRead0(Lio/netty/channel/ChannelHandlerContext;Lnet/minecraft/network/protocol/Packet;)V",
            at = @At("HEAD"))
    private void zstd$debugPacket(ChannelHandlerContext ctx, Packet<?> packet, CallbackInfo ci) {
        if (!ZstdConfig.INSTANCE.debug) {
            return; // 关键：非 debug 时零开销，不做任何字符串匹配
        }
        String name = packet.getClass().getSimpleName();
        // 收到 FinishConfiguration 后进入 play 记录模式：前 15 个 play 包全部记录
        if (name.contains("Finish")) {
            playPacketsLogged = 15;
        }
        if (playPacketsLogged > 0) {
            playPacketsLogged--;
            LOGGER.info("[Zstd] client recv(play): {}", name);
        } else {
            LOGGER.info("[Zstd] client recv: {}", name);
        }
    }
}
