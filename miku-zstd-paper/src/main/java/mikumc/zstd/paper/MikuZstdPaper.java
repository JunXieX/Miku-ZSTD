package mikumc.zstd.paper;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import org.bukkit.plugin.java.JavaPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.List;
import mikumc.zstd.protocol.ZstdCompressPool;

/**
 * Miku-ZSTD Paper 端插件入口。
 *
 * <h2>为什么要碰 NMS</h2>
 * <p>Paper API 不暴露 netty 管线（{@code io.papermc.paper.connection.PlayerLoginConnection}
 * 只有 profile 方法，拿不到 Channel），而接管压缩必须替换 {@code compress}/{@code decompress}。
 * MC 26.x 未混淆，反射访问 {@code Connection.channel} 与管线锚点是稳定可行的。</p>
 *
 * <h2>注入时机：连接建立时（不是登录事件）</h2>
 * <p>两个已被实测否定的方案：</p>
 * <ul>
 *   <li>{@code PlayerLoginEvent}：Paper 检测到有插件监听它就会走
 *       HorriblePlayerLoginEventHack 改变登录时序，negotiate 必然晚于 SetCompression
 *       （且该事件已被标记 deprecated）。异步调度与同步等待都试过，均无效。</li>
 *   <li>在 {@code handlerAdded} 里发 negotiate：太早——客户端在发出 login start 之前
 *       还没挂载登录查询接收器，查询会被丢弃。</li>
 * </ul>
 * <p>因此这里改为<b>包装服务端监听通道的 childHandler</b>：每条新连接建立后立即注入协商器，
 * 而协商查询由协商器在<b>收到第一个入站包之后</b>发出（见 {@link ZstdPaperNegotiator}）。
 * 这样既早于 SetCompression，又晚于客户端挂载接收器。</p>
 */
public class MikuZstdPaper extends JavaPlugin {

    private static final Logger LOGGER = LoggerFactory.getLogger("miku-zstd");

    @Override
    public void onEnable() {
        ZstdPaperConfig.load(Path.of(getDataFolder().getPath(), "config.yml"));

        // ⚠️ 服务端关闭原版压缩（network-compression-threshold=-1）时本插件无法工作：
        // zstd 需要借原版的 SetCompression 做双端同步信号、借它装好的 compress/decompress
        // 做替换锚点；该配置下这二者都不存在（伪造切换信号会导致协议切换期异常、无法进服）。
        // 与其静默失效，不如明确停用并说明原因。
        int compressionThreshold = ZstdPaperCommandSupport.readCompressionThreshold();
        if (compressionThreshold < 0) {
            LOGGER.error("[Zstd] 检测到 network-compression-threshold={}（服务端已关闭原版压缩）。", compressionThreshold);
            LOGGER.error("[Zstd] 本插件依赖原版压缩流程作为切换信号与替换锚点，该配置下无法工作，");
            LOGGER.error("[Zstd] 因此自动停用（不影响正常进服）。如需 zstd，请把该值设为 >= 0，推荐 256。");
            getServer().getPluginManager().disablePlugin(this);
            return;
        }

        ZstdPaperTrainer.init(getDataFolder().toPath());
        ZstdCompressPool.init(ZstdPaperConfig.INSTANCE.compressThreads, 5);
        // onEnable 阶段 MC 的监听通道通常还没建立，等服务器就绪后再挂钩
        getServer().getScheduler().runTask(this, this::installHook);
        // ⚠️ Paper 插件不支持在 paper-plugin.yml 里声明命令，getCommand() 会抛
        // UnsupportedOperationException（曾因此导致插件启用失败、自动停用）。
        // 必须走生命周期事件注册。
        getLifecycleManager().registerEventHandler(
                io.papermc.paper.plugin.lifecycle.event.types.LifecycleEvents.COMMANDS,
                commands -> commands.registrar().register(
                        "mikuzstd", "查看 Miku-ZSTD 状态与字典训练情况", new ZstdPaperCommand()));
        LOGGER.info("[Zstd] Miku-ZSTD Paper 已启用（目标 Paper 26.2+，纯 Paper，无 Bukkit 兼容层）");
    }

    @Override
    public void onDisable() {
        ZstdPaperMonitor.stop();
        ZstdPaperTrainer.shutdown();
        LOGGER.info("[Zstd] Miku-ZSTD Paper 已停用");
    }

    /** 挂钩监听通道；首次未成功时再延迟重试一次。 */
    private void installHook() {
        if (tryInstallHook()) return;
        getServer().getScheduler().runTaskLater(this, () -> {
            if (!tryInstallHook()) {
                LOGGER.warn("[Zstd] 未能挂钩监听通道——本端 zstd 不可用（连接仍走原版 zlib，不影响正常游玩）");
            }
        }, 40L);
    }

    private boolean tryInstallHook() {
        try {
            Object craftServer = getServer();
            Object mcServer = craftServer.getClass().getMethod("getServer").invoke(craftServer);
            Object serverConnection = mcServer.getClass().getMethod("getConnection").invoke(mcServer);

            int hooked = 0;
            for (Field f : serverConnection.getClass().getDeclaredFields()) {
                Object v = readQuietly(f, serverConnection);
                if (!(v instanceof List<?> list)) continue;
                for (Object item : list) {
                    Channel ch = channelOf(item);
                    if (ch != null && hookServerChannel(ch)) hooked++;
                }
            }
            if (hooked > 0) {
                LOGGER.info("[Zstd] 已挂钩 {} 个监听通道：新连接建立时立即注入协商器", hooked);
                return true;
            }
            return false;
        } catch (Throwable t) {
            LOGGER.warn("[Zstd] 挂钩失败", t);
            return false;
        }
    }

    /** 替换 netty ServerBootstrapAcceptor 持有的 childHandler（包装原初始化器）。 */
    private static boolean hookServerChannel(Channel serverChannel) {
        for (String name : serverChannel.pipeline().names()) {
            Object handler = serverChannel.pipeline().get(name);
            if (handler == null) continue;
            if (!handler.getClass().getName().contains("ServerBootstrapAcceptor")) continue;
            for (Field f : handler.getClass().getDeclaredFields()) {
                Object v = readQuietly(f, handler);
                if (!(v instanceof ChannelInitializer<?> original)) continue;
                try {
                    f.setAccessible(true);
                    f.set(handler, new WrappedInitializer(original));
                    return true;
                } catch (Throwable t) {
                    LOGGER.warn("[Zstd] 替换 childHandler 失败", t);
                }
            }
        }
        return false;
    }

    /** 先执行原初始化逻辑（建立 splitter/encoder 等锚点），再注入本插件的处理器。 */
    private static final class WrappedInitializer extends ChannelInitializer<Channel> {

        private final ChannelInitializer<?> original;

        WrappedInitializer(ChannelInitializer<?> original) {
            this.original = original;
        }

        @Override
        protected void initChannel(Channel ch) throws Exception {
            // ChannelInitializer.initChannel 是 protected：用反射调用原实现
            Method m = ChannelInitializer.class.getDeclaredMethod("initChannel", Channel.class);
            m.setAccessible(true);
            m.invoke(original, ch);
            injectHandlers(ch);
        }
    }

    /** 注入应答嗅探器与协商器（eventLoop 线程；幂等）。 */
    private static void injectHandlers(Channel channel) {
        if (!channel.isActive()) return;
        if (channel.attr(ZstdPaperChannelManager.KEY).get() != null) return;

        var p = channel.pipeline();
        ZstdPaperChannelManager mgr = new ZstdPaperChannelManager();
        channel.attr(ZstdPaperChannelManager.KEY).set(mgr);
        channel.closeFuture().addListener(f -> mgr.close());

        // 注：不再安装帧层应答嗅探器 —— 应答改由协商器在包层拦截（见 ZstdPaperNegotiator.channelRead），
        // 这样不依赖 "splitter" 等 handler 名在注入时刻是否已存在。
        // 协商器：必须放在 packet_handler **之前**。
        // ⚠️ 实测教训：放在末尾（packet_handler 之后）时，出站事件仍能收到（negotiate 靠兜底发出去了），
        // 但**入站包会被 packet_handler 先消费掉**，协商器永远收不到应答——表现为
        // "negotiate 发出、客户端也回了，服务端却一直等不到应答"。
        // 放在 packet_handler 之前即可同时满足：入站能看到 login start 与应答，出站在最前面看到 SetCompression。
        if (p.get("miku-zstd-negotiator") == null) {
            if (p.get("packet_handler") != null) {
                p.addBefore("packet_handler", "miku-zstd-negotiator", new ZstdPaperNegotiator());
            } else if (p.get("encoder") != null) {
                p.addAfter("encoder", "miku-zstd-negotiator", new ZstdPaperNegotiator());
            } else {
                p.addLast("miku-zstd-negotiator", new ZstdPaperNegotiator());
            }
        }
    }

    private static Object readQuietly(Field f, Object target) {
        try {
            f.setAccessible(true);
            return f.get(target);
        } catch (Throwable t) {
            return null;
        }
    }

    private static Channel channelOf(Object futureLike) {
        try {
            if (futureLike instanceof ChannelFuture cf) return cf.channel();
            Method m = futureLike.getClass().getMethod("channel");
            Object c = m.invoke(futureLike);
            return c instanceof Channel ch ? ch : null;
        } catch (Throwable t) {
            return null;
        }
    }
}
