package mikumc.zstd;

import com.google.inject.Inject;
import com.velocitypowered.api.event.Subscribe;
import com.velocitypowered.api.event.connection.ConnectionHandshakeEvent;
import com.velocitypowered.api.event.proxy.ProxyInitializeEvent;
import com.velocitypowered.api.event.proxy.ProxyShutdownEvent;
import com.velocitypowered.api.plugin.Plugin;
import com.velocitypowered.api.plugin.annotation.DataDirectory;
import com.velocitypowered.api.proxy.InboundConnection;
import com.velocitypowered.api.proxy.ProxyServer;
import io.netty.channel.Channel;
import org.slf4j.Logger;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Path;
import mikumc.zstd.protocol.ZstdCompressPool;
import java.io.IOException;
import java.nio.file.Files;

@Plugin(id = "miku-zstd", name = "Miku-ZSTD", version = BuildConstants.VERSION, authors = {"JunXieX"})
public class MikuZstdVelocity {

    private static final String[] CHANNEL_FIELD_NAMES = {"channel"};
    private static final String[] CONNECTION_FIELD_NAMES = {"connection", "delegate"};

    private final Logger logger;
    private final ProxyServer proxy;
    private final Path dataDirectory;

    @Inject
    public MikuZstdVelocity(Logger logger, ProxyServer proxy, @DataDirectory Path dataDirectory) {
        this.logger = logger;
        this.proxy = proxy;
        this.dataDirectory = dataDirectory;
    }

    @Subscribe
    public void onProxyInitialization(ProxyInitializeEvent event) {
        Path dataDir = resolveDataDir(dataDirectory);
        ZstdVelocityConfig.load(dataDir.resolve("config.yml"));
        ZstdSampleTrainer.init(dataDir);
        ZstdCompressPool.init(ZstdVelocityConfig.INSTANCE.compressThreads, 5);
        if (ZstdVelocityConfig.INSTANCE.debug) {
            applyDebugLogLevel();
        }
        // 带宽剖析：用真实流量回答"字节花在哪个包长区间"，debug 关闭时零开销
        ZstdBandwidthProfiler.start(ZstdVelocityConfig.INSTANCE.debug, 60);
        logger.info("Miku-ZSTD Velocity plugin initialized");
        ZstdBossBarMonitor.init(proxy, this);
        // ⚠️ 必须注册 BrigadierCommand（显式建树）。用 metaBuilder + SimpleCommand 会生成
        // "不接受参数的光杆 literal"，导致 /mikuzstd bar 报 Incorrect argument 且 TAB 无提示。
        com.velocitypowered.api.command.BrigadierCommand mikuCommand = new ZstdCommand(proxy, this).build();
        proxy.getCommandManager().register(
                proxy.getCommandManager().metaBuilder(mikuCommand)
                        .plugin(this)
                        .build(),
                mikuCommand);
        installHandshakeSniffer();
    }

    /**
     * Velocity 4.x 的 ConnectionHandshakeEvent 为异步派发，监听器注入的处理器
     * 总是晚于同批读取的 login start 处理，negotiate 因此无法先于 SetCompression 到达客户端，
     * 客户端与服务端的压缩切换时序错开会导致对端 zlib 解压失败。
     * 通过替换 ServerChannelInitializer，在每条新连接的解码链中挂入握手嗅探器，
     * 于 login 处理之前的同步点完成检测与注入。
     */
    @SuppressWarnings("unchecked") // 反射取回的对象只能强转成带泛型的类型，无法静态校验；
                                   // 后面会通过 setMethod 的签名把它写回，类型由 Velocity 保证
    private void installHandshakeSniffer() {
        try {
            // 全反射：ServerChannelInitializerHolder 位于 velocity-proxy 内部实现，不在 api 包中
            java.lang.reflect.Field cmField = proxy.getClass().getDeclaredField("cm");
            cmField.setAccessible(true);
            Object cm = cmField.get(proxy);
            java.lang.reflect.Field holderField = cm.getClass().getField("serverChannelInitializer");
            Object holder = holderField.get(cm);

            java.lang.reflect.Method getMethod = holder.getClass().getMethod("get");
            io.netty.channel.ChannelInitializer<io.netty.channel.Channel> original =
                    (io.netty.channel.ChannelInitializer<io.netty.channel.Channel>) getMethod.invoke(holder);
            java.lang.reflect.Method setMethod = holder.getClass().getMethod("set", io.netty.channel.ChannelInitializer.class);
            java.lang.reflect.Method initMethod = io.netty.channel.ChannelInitializer.class
                    .getDeclaredMethod("initChannel", io.netty.channel.Channel.class);
            initMethod.setAccessible(true);

            setMethod.invoke(holder, new io.netty.channel.ChannelInitializer<io.netty.channel.Channel>() {
                @Override
                protected void initChannel(io.netty.channel.Channel ch) throws Exception {
                    initMethod.invoke(original, ch);
                    if (ch.pipeline().get("frame-decoder") != null
                            && ch.pipeline().get("zstd-handshake-sniff") == null) {
                        ch.pipeline().addAfter("frame-decoder", "zstd-handshake-sniff", new ZstdHandshakeSniffer());
                    }
                }
            });
            logger.info("[Zstd] Handshake sniffer installed into server channel initializer");
        } catch (Throwable t) {
            logger.warn("[Zstd] Could not install handshake sniffer — falling back to handshake-event mode", t);
        }
    }

    /**
     * 解析本插件的数据目录：{@code plugins/Miku-ZSTD}。
     *
     * <h2>为什么不直接改 plugin id</h2>
     * Velocity 的数据目录名<b>强绑定 plugin id</b>，而 id 的校验规则是
     * {@code [a-z][a-z0-9-_]{0,63}} —— 注解处理器会直接报
     * {@code Invalid ID for plugin ... IDs must start alphabetically, have lowercase
     * alphanumeric characters}。所以 {@code id = "Miku-ZSTD"} 连编译都过不去。
     *
     * <p>这里改用注入路径的<b>父目录</b>（即 {@code plugins/}）去拼 {@code Miku-ZSTD}，
     * 既保住合法的 id，又让目录名与插件显示名一致。</p>
     *
     * <p>首次切换时把旧目录（{@code plugins/miku-zstd}，含 {@code config.yml} 与
     * {@code zstd_dicts/}）整体搬过来，避免丢配置与已训练的字典。</p>
     */
    private Path resolveDataDir(Path injected) {
        Path parent = injected.getParent();
        if (parent == null) {
            return injected; // 兜底：拿不到 plugins/ 就沿用原路径
        }
        Path target = parent.resolve("Miku-ZSTD");
        if (Files.exists(target)) {
            return target;
        }
        if (Files.exists(injected)) {
            try {
                Files.move(injected, target);
                logger.info("[Zstd] 数据目录已迁移: {} -> {}", injected.getFileName(), target.getFileName());
            } catch (IOException e) {
                logger.warn("[Zstd] 数据目录迁移失败，继续使用旧目录: {}", e.toString());
                return injected;
            }
        }
        return target;
    }

    @Subscribe
    public void onProxyShutdown(ProxyShutdownEvent event) {
        ZstdBossBarMonitor.stop();
        ZstdBandwidthProfiler.shutdown();
        ZstdSampleTrainer.shutdown();
        logger.info("[Zstd] Samples saved on shutdown");
    }

    @Subscribe
    public void onConnectionHandshake(ConnectionHandshakeEvent event) {
        InboundConnection inbound = event.getConnection();

        // 嗅探器（若已安装）会在 login 处理前同步完成注入；此处仅作兜底与日志。
        Channel channel = extractChannel(inbound);
        if (channel != null && channel.attr(ZstdChannelManager.KEY).get() != null) {
            logger.info("[Zstd] Connection already negotiated by sniffer: {}",
                    inbound.getRawVirtualHost().orElse(""));
            return;
        }

        String rawHost = inbound.getRawVirtualHost().orElse("");
        if (!rawHost.contains("\0ZSTD\0")) {
            return;
        }

        logger.info("[Zstd] Detected Zstd client via handshake event (fallback): {}", rawHost);

        if (channel == null) {
            logger.warn("[Zstd] Failed to extract Netty Channel, falling back to vanilla");
            return;
        }

        ZstdChannelManager mgr = new ZstdChannelManager();
        channel.attr(ZstdChannelManager.KEY).set(mgr);
        channel.closeFuture().addListener(f -> mgr.close());

        // 兜底路径：事件可能在 login 处理之后触发，统一投递到 eventLoop。
        channel.eventLoop().execute(() -> {
            if (!channel.isActive()) return;
            try {
                if (channel.pipeline().get("zstd_outbound_spy") == null
                        && channel.pipeline().get("handler") != null) {
                    channel.pipeline().addBefore("handler", "zstd_outbound_spy", new ZstdHijacker());
                }
                ZstdHijacker.sendNegotiateNow(channel);
            } catch (Exception e) {
                logger.error("[Zstd] Failed to activate zstd negotiation, falling back to vanilla", e);
            }
        });
    }

    /**
     * 按当前配置应用 debug 日志等级。
     *
     * <p>启动时调一次，{@code /mikuzstd reload} 后再调一次——否则改完配置 reload 会显示
     * {@code debug: true} 而日志一条都不多，比不提供这个开关更糟。</p>
     *
     * <p>只处理"开启"：关闭需要人工改日志框架，插件不去重置等级（会误伤其它插件）。</p>
     */
    void applyDebugFromConfig() {
        if (ZstdVelocityConfig.INSTANCE.debug) {
            applyDebugLogLevel();
        }
    }

    private void applyDebugLogLevel() {
        boolean ok = tryLog4j2() || tryLogback();
        if (ok) {
            logger.info("[Zstd] Debug logging enabled");
        } else {
            logger.warn("[Zstd] Could not set debug log level — configure the logging framework manually");
        }
    }

    private boolean tryLog4j2() {
        try {
            Class<?> configuratorClass = Class.forName("org.apache.logging.log4j.core.config.Configurator");
            Class<?> levelClass = Class.forName("org.apache.logging.log4j.Level");
            Object debugLevel = levelClass.getField("DEBUG").get(null);
            configuratorClass.getMethod("setLevel", String.class, levelClass)
                    .invoke(null, "miku-zstd", debugLevel);
            return true;
        } catch (Exception ignored) {
            return false;
        }
    }

    private boolean tryLogback() {
        try {
            Class<?> levelClass = Class.forName("ch.qos.logback.classic.Level");
            Object debugLevel = levelClass.getField("DEBUG").get(null);
            Class<?> loggerClass = Class.forName("ch.qos.logback.classic.Logger");
            Object logbackLogger = org.slf4j.LoggerFactory.getLogger("miku-zstd");
            loggerClass.getMethod("setLevel", levelClass).invoke(logbackLogger, debugLevel);
            return true;
        } catch (Exception ignored) {
            return false;
        }
    }

    private Channel extractChannel(InboundConnection inbound) {
        Channel channel = tryReflectGetChannel(inbound);
        if (channel != null) return channel;

        for (String connField : CONNECTION_FIELD_NAMES) {
            Object conn = reflectField(inbound, connField);
            if (conn == null) continue;

            channel = tryReflectGetChannel(conn);
            if (channel != null) return channel;

            for (String nestedField : CONNECTION_FIELD_NAMES) {
                Object nested = reflectField(conn, nestedField);
                if (nested == null) continue;
                channel = tryReflectGetChannel(nested);
                if (channel != null) return channel;
            }
        }

        logger.warn("[Zstd] Channel not found, inboundClass={}", inbound.getClass().getName());
        return null;
    }

    private Channel tryReflectGetChannel(Object obj) {
        if (obj instanceof Channel) return (Channel) obj;

        Channel ch = reflectFieldTyped(obj, Channel.class, CHANNEL_FIELD_NAMES);
        if (ch != null) return ch;

        try {
            Method m = obj.getClass().getMethod("getChannel");
            Object result = m.invoke(obj);
            if (result instanceof Channel) return (Channel) result;
        } catch (Exception ignored) {
        }

        return null;
    }

    private static Channel reflectFieldTyped(Object obj, Class<Channel> type, String[] names) {
        for (String name : names) {
            Object val = reflectField(obj, name);
            if (type.isInstance(val)) return (Channel) val;
        }
        return null;
    }

    private static Object reflectField(Object obj, String fieldName) {
        Class<?> clazz = obj.getClass();
        while (clazz != null && clazz != Object.class) {
            try {
                Field field = clazz.getDeclaredField(fieldName);
                field.setAccessible(true);
                return field.get(obj);
            } catch (NoSuchFieldException e) {
                clazz = clazz.getSuperclass();
            } catch (Exception e) {
                return null;
            }
        }
        return null;
    }
}
