package mikumc.zstd.paper;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.papermc.paper.ServerBuildInfo;
import net.kyori.adventure.key.Key;
import org.bukkit.plugin.java.JavaPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
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

    /**
     * 挂钩现场，供 {@link #onDisable()} 还原。
     *
     * <p>不还原的后果在<b>插件重载</b>时才会显现：被替换掉的 {@code childHandler} 一直
     * 持有本插件的类加载器（{@code WrappedInitializer} 是本插件的内部类），
     * 老类加载器永远无法回收，而新加载的实例又会再包一层。</p>
     */
    private final List<HookedField> hookedFields = new ArrayList<>();

    @Override
    public void onEnable() {
        ZstdPaperConfig.load(Path.of(getDataFolder().getPath(), "config.yml"));
        if (ZstdPaperConfig.INSTANCE.debug) {
            applyDebugLogLevel();
        }

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
        // onEnable 阶段 MC 的监听通道通常还没建立，等服务器就绪后再挂钩。
        // ⚠️ 这里不能用 BukkitScheduler：Folia 上 Bukkit.getScheduler() 直接抛
        // UnsupportedOperationException（BukkitScheduler 建立在"唯一主线程"这个前提上，Folia 没有该前提）。
        // GlobalRegionScheduler 在 Paper 上等价于主线程、在 Folia 上是真正的全局区域线程，
        // 而挂钩动作本身不属于任何区域（只走反射替换 Netty 的 childHandler），因此两端共用同一份代码。
        getServer().getGlobalRegionScheduler().run(this, task -> installHook());
        // ⚠️ Paper 插件不支持在 paper-plugin.yml 里声明命令，getCommand() 会抛
        // UnsupportedOperationException（曾因此导致插件启用失败、自动停用）。
        // 必须走生命周期事件注册。
        getLifecycleManager().registerEventHandler(
                io.papermc.paper.plugin.lifecycle.event.types.LifecycleEvents.COMMANDS,
                commands -> commands.registrar().register(
                        "mikuzstd", "查看 Miku-ZSTD 状态与字典训练情况", new ZstdPaperCommand()));
        LOGGER.info("[Zstd] Miku-ZSTD Paper 已启用（目标 Paper/Folia 26.2+，平台={}，无 Bukkit 兼容层）",
                platformName());
    }

    @Override
    public void onDisable() {
        ZstdPaperMonitor.stop();
        ZstdPaperTrainer.shutdown(); // 内部会调 ZstdPaperDictRegistry.shutdown()，释放字典原生内存
        uninstallHook();
        LOGGER.info("[Zstd] Miku-ZSTD Paper 已停用");
    }

    /**
     * 还原挂钩：把原始 {@code childHandler} 放回去，并摘掉仍在线连接上的协商器。
     *
     * <p>这样插件被重载时不会残留：已建立的连接立即回到原版路径（不再协商），
     * 新连接也不会再被注入，被替换的字段不再持有本插件的类加载器。</p>
     *
     * <p>⚠️ <b>已经激活 zstd 的连接不能"拆回原版"</b>：激活时原版 {@code compress}/{@code decompress}
     * 实例是被 {@code pipeline.replace} <b>移除</b>过的，而 Netty 对非 {@code @Sharable} handler 的
     * {@code added} 标志永不复位——同一个实例再也放不回管线。所以这里只摘协商器，
     * 让那条连接继续用"它自己那套"zstd 处理器跑完生命周期（持有旧类加载器的时间以该连接关闭为界）。
     * 强行移除 zstd 处理器只会得到"管线里既没有原版压缩、也没有 zstd"的必断连状态。</p>
     */
    private void uninstallHook() {
        for (HookedField hook : hookedFields) {
            try {
                hook.field().setAccessible(true);
                hook.field().set(hook.target(), hook.original());
            } catch (Throwable t) {
                LOGGER.debug("[Zstd] 还原 childHandler 失败（忽略）", t);
            }
        }
        hookedFields.clear();
        // ⚠️ 必须遍历**已建立的玩家连接**，而不是监听通道：协商器是注入到每条客户端连接的
        // 子通道上的（见 injectHandlers），监听通道的管线里从来没有它。旧实现只遍历监听通道，
        // 于是这段"摘掉在线连接的协商器"的承诺根本没兑现——插件重载后，老协商器仍挂在
        // 每一条在线连接的出/入站热路径上，对每个包跑字符串判定，并继续持有旧类加载器。
        try {
            forEachLiveConnectionChannel(channel -> {
                if (channel.pipeline().get("miku-zstd-negotiator") != null) {
                    channel.pipeline().remove("miku-zstd-negotiator");
                }
            });
        } catch (Throwable t) {
            LOGGER.debug("[Zstd] 摘除在线连接的协商器失败（忽略）", t);
        }
    }

    /**
     * 按配置开启 debug 日志（Level 由 log4j2 反射设置）。
     *
     * <p>Paper 用 log4j2，与 Velocity 端同一套路。缺这一步时 {@code logging.debug}
     * 是个"写了也没用"的配置——模板却承诺它会输出逐包诊断。</p>
     */
    private void applyDebugLogLevel() {
        try {
            Class<?> configuratorClass = Class.forName("org.apache.logging.log4j.core.config.Configurator");
            Class<?> levelClass = Class.forName("org.apache.logging.log4j.Level");
            Object debugLevel = levelClass.getField("DEBUG").get(null);
            configuratorClass.getMethod("setLevel", String.class, levelClass)
                    .invoke(null, "miku-zstd", debugLevel);
            LOGGER.info("[Zstd] Debug logging enabled");
        } catch (Throwable t) {
            LOGGER.warn("[Zstd] 无法设置 debug 日志等级，请自行配置日志框架（设置未生效）: {}", t.toString());
        }
    }

    /** 挂钩监听通道；首次未成功时再延迟重试一次。 */
    private void installHook() {
        if (tryInstallHook()) return;
        // 延迟重试同样走全局区域调度器（Folia 上没有可用的 BukkitScheduler）。
        getServer().getGlobalRegionScheduler().runDelayed(this, task -> {
            if (!tryInstallHook()) {
                LOGGER.warn("[Zstd] 未能挂钩监听通道——本端 zstd 不可用（连接仍走原版 zlib，不影响正常游玩）");
            }
        }, 40L);
    }

    /**
     * 当前是否运行在 Folia 上。
     *
     * <p>判定走 {@link ServerBuildInfo#isBrandCompatible(Key)}，而不是查类是否存在：
     * Folia 与 Paper 共用 paper-api，区域调度器类在两端<b>都存在</b>，
     * 用类路径判断必然误判成 Paper。品牌标识才是权威信号。</p>
     *
     * <p>包了 try/catch：该方法在拿不到 build info 的实现上会抛异常，
     * 而当前它只影响状态展示与日志文案，不值得让启用流程失败。</p>
     */
    public static boolean isFolia() {
        try {
            return ServerBuildInfo.buildInfo().isBrandCompatible(Key.key("papermc", "folia"));
        } catch (Throwable t) {
            return false;
        }
    }

    /** 平台名（日志与 /mikuzstd 状态共用）。 */
    public static String platformName() {
        return isFolia() ? "Folia" : "Paper";
    }

    private boolean tryInstallHook() {
        try {
            int[] hooked = {0};
            forEachServerChannel(ch -> {
                if (hookServerChannel(ch)) hooked[0]++;
            });
            if (hooked[0] > 0) {
                LOGGER.info("[Zstd] 已挂钩 {} 个监听通道：新连接建立时立即注入协商器", hooked[0]);
                return true;
            }
            return false;
        } catch (Throwable t) {
            LOGGER.warn("[Zstd] 挂钩失败", t);
            return false;
        }
    }

    /**
     * 遍历原版监听通道（{@code ServerConnectionListener} 里那些 {@code List<ChannelFuture>} 字段）。
     *
     * <p>挂钩与还原共用这一份遍历，避免两处各写一遍反射、日后只改一边。</p>
     */
    private void forEachServerChannel(java.util.function.Consumer<Channel> action) throws Exception {
        Object craftServer = getServer();
        Object mcServer = craftServer.getClass().getMethod("getServer").invoke(craftServer);
        Object serverConnection = mcServer.getClass().getMethod("getConnection").invoke(mcServer);
        for (Field f : serverConnection.getClass().getDeclaredFields()) {
            Object v = readQuietly(f, serverConnection);
            if (!(v instanceof List<?> list)) continue;
            for (Object item : list) {
                Channel ch = channelOf(item);
                if (ch != null) {
                    action.accept(ch);
                }
            }
        }
    }

    /**
     * 遍历<b>已建立的玩家连接</b>的 netty 通道（{@code ServerConnectionListener} 里
     * 那个装 {@code Connection} 的集合字段，每个 {@code Connection} 内含 {@code channel}）。
     *
     * <p>与 {@link #forEachServerChannel} 的区别：后者遍历的是<b>监听</b>通道（accept
     * 用的 server channel），本条遍历的是每个玩家各自那条子通道。协商器只存在于后者。</p>
     *
     * <p>字段名在不同版本可能变（{@code connections} / {@code connectionList}），因此按
     * "值是集合、元素能取出 channel"来识别，而不是写死名字；取不到就安静跳过——
     * 这段只服务于插件重载时的清理，失败不应影响停用流程。</p>
     */
    private void forEachLiveConnectionChannel(java.util.function.Consumer<Channel> action) throws Exception {
        Object craftServer = getServer();
        Object mcServer = craftServer.getClass().getMethod("getServer").invoke(craftServer);
        Object serverConnection = mcServer.getClass().getMethod("getConnection").invoke(mcServer);
        for (Field f : serverConnection.getClass().getDeclaredFields()) {
            Object v = readQuietly(f, serverConnection);
            if (!(v instanceof Iterable<?> connections)) continue;
            for (Object conn : connections) {
                // 跳过监听通道那一组（元素本身就是 ChannelFuture 的情形由 channelOf 统一处理，
                // 但真正的玩家连接元素是 Connection，需从它的 channel 字段取）
                Channel ch = channelOf(conn);
                if (ch == null) {
                    ch = readChannelField(conn);
                }
                if (ch != null) {
                    action.accept(ch);
                }
            }
        }
    }

    /** 从任意对象（典型是 MC 的 {@code Connection}）的 {@code channel} 字段取 netty 通道。 */
    private static Channel readChannelField(Object target) {
        if (target == null || target instanceof Channel) {
            return target instanceof Channel c ? c : null;
        }
        for (Class<?> c = target.getClass(); c != null && c != Object.class; c = c.getSuperclass()) {
            try {
                Field f = c.getDeclaredField("channel");
                Object v = readQuietly(f, target);
                if (v instanceof Channel ch) {
                    return ch;
                }
            } catch (Throwable ignored) {
                // 该层没有该字段：继续往父类找
            }
        }
        return null;
    }

    /**
     * 替换 netty ServerBootstrapAcceptor 持有的 childHandler（包装原初始化器），
     * 并记录现场供停用时还原。
     */
    private boolean hookServerChannel(Channel serverChannel) {
        for (String name : serverChannel.pipeline().names()) {
            Object handler = serverChannel.pipeline().get(name);
            if (handler == null) continue;
            if (!handler.getClass().getName().contains("ServerBootstrapAcceptor")) continue;
            for (Field f : handler.getClass().getDeclaredFields()) {
                Object v = readQuietly(f, handler);
                if (!(v instanceof ChannelInitializer<?> original)) continue;
                if (original instanceof WrappedInitializer) continue; // 已挂钩过，避免重复包装
                try {
                    f.setAccessible(true);
                    f.set(handler, new WrappedInitializer(original));
                    hookedFields.add(new HookedField(handler, f, original));
                    return true;
                } catch (Throwable t) {
                    LOGGER.warn("[Zstd] 替换 childHandler 失败", t);
                }
            }
        }
        return false;
    }

    /** 挂钩现场：某个 acceptor 上被替换掉的字段及其原值。 */
    private record HookedField(Object target, Field field, Object original) {
    }

    /** 先执行原初始化逻辑（建立 splitter/encoder 等锚点），再注入本插件的处理器。 */
    private static final class WrappedInitializer extends ChannelInitializer<Channel> {

        /**
         * 反射取到的 {@code ChannelInitializer#initChannel}（protected）。
         *
         * <p>缓存成静态常量：它是每<b>一条新连接</b>都要用的热路径，
         * 而 {@code getDeclaredMethod} + {@code setAccessible} 每次都要走一遍
         * 权限检查与查找——没必要在连接路径上重复做。</p>
         */
        private static final Method INIT_CHANNEL = lookupInitChannel();

        private final ChannelInitializer<?> original;

        WrappedInitializer(ChannelInitializer<?> original) {
            this.original = original;
        }

        private static Method lookupInitChannel() {
            try {
                Method m = ChannelInitializer.class.getDeclaredMethod("initChannel", Channel.class);
                m.setAccessible(true);
                return m;
            } catch (Throwable t) {
                throw new ExceptionInInitializerError(t);
            }
        }

        @Override
        protected void initChannel(Channel ch) throws Exception {
            INIT_CHANNEL.invoke(original, ch);
            injectHandlers(ch);
        }
    }

    /** 注入协商器（eventLoop 线程；幂等）。 */
    private static void injectHandlers(Channel channel) {
        injectHandlers(channel, 0);
    }

    /**
     * 注入实现：<b>必须等 {@code packet_handler} 就位</b>再插入协商器。
     *
     * <p>协商器的存在意义就是"抢在 {@code packet_handler} 之前消费查询应答"。
     * 若协议处理器尚未出现就插进去（Folia 与部分协议库的初始化顺序并不保证
     * childHandler 调用期间已完成协议配置），协商器会落在链尾；等 MC 用 {@code addLast}
     * 把 splitter/decoder/prepender/encoder/packet_handler 追加上来后，它实际位于
     * packet_handler <b>之后</b> —— 入站应答被 {@code ServerLoginPacketListenerImpl} 先消费，
     * 而它不认识我们自造的 txId，直接判定 "unexpected query response" 并断开。
     * 实测日志：{@code EncoderException: Pipeline has no outbound protocol configured ...
     * ClientboundLoginDisconnectPacket[reason=multiplayer.disconnect.unexpected_query_response]}。</p>
     */
    private static void injectHandlers(Channel channel, int attempt) {
        if (!channel.isActive()) return;
        if (channel.attr(ZstdPaperChannelManager.KEY).get() != null) return;

        var p = channel.pipeline();
        if (p.get("packet_handler") == null) {
            if (attempt < 200) {
                // 每 10ms 重试一次，上限约 2s：正常情况下协议处理器在通道激活时就已经在了，
                // 这里只是兜住初始化顺序差异，不会真的等满。
                channel.eventLoop().schedule(() -> injectHandlers(channel, attempt + 1),
                        10, TimeUnit.MILLISECONDS);
            } else {
                LOGGER.debug("[Zstd] packet_handler 始终未出现，放弃为本次连接注入协商器; pipeline={}",
                        p.names());
            }
            return;
        }

        // ⚠️ 构造放在 try 内：ZstdPaperChannelManager 的构造器会立刻把配置里的
        // level / window_log 施加到 zstd 上下文上，越界配置会让 zstd-jni 抛异常。
        // 这段跑在通道初始化路径上，异常外泄会让"配置写错"变成"连不上"。
        ZstdPaperChannelManager mgr;
        try {
            mgr = new ZstdPaperChannelManager();
        } catch (Throwable t) {
            LOGGER.error("[Zstd] 无法为本连接创建压缩上下文（检查 level / window_log 配置），"
                    + "该连接保持原版 zlib", t);
            return;
        }
        channel.attr(ZstdPaperChannelManager.KEY).set(mgr);
        channel.closeFuture().addListener(f -> mgr.close());

        // 注：不再安装帧层应答嗅探器 —— 应答改由协商器在包层拦截（见 ZstdPaperNegotiator.channelRead），
        // 这样不依赖 "splitter" 等 handler 名在注入时刻是否已存在。
        // 协商器：必须放在 packet_handler **之前**，且只此一个位置，不再提供 addLast 兜底。
        if (p.get("miku-zstd-negotiator") == null) {
            p.addBefore("packet_handler", "miku-zstd-negotiator", new ZstdPaperNegotiator());
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
