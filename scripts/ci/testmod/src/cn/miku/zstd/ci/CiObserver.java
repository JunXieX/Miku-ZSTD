package cn.miku.zstd.ci;

import java.io.BufferedWriter;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import net.fabricmc.api.ClientModInitializer;
import net.fabricmc.loader.api.FabricLoader;
import net.fabricmc.loader.api.ModContainer;
import net.fabricmc.loader.api.metadata.ModOrigin;
import net.minecraft.client.Minecraft;
import net.minecraft.client.multiplayer.ClientPacketListener;
import net.minecraft.network.Connection;

/**
 * CI-only observation mod: it reports what the running client actually did.
 *
 * <p>这是测试组件，永远不进正式发布物。它不实现任何游戏功能，只做三件事：</p>
 * <ol>
 *   <li>启动时记录真实加载的模组身份与来源文件路径（用于证明跑的是指定成品 JAR）；</li>
 *   <li>用独立守护线程按固定间隔采样客户端状态（是否进入世界、玩家实体 tick 数、坐标）；</li>
 *   <li>在连接建立后往 Netty 入站管线最前端挂一个只读计数器，累计<b>真实</b>入站字节数。</li>
 * </ol>
 *
 * <p>为什么要单独挂计数器：驱动需要在"同一测量区间"内拿到真实下行字节增量，
 * 而不是解析 HUD 或命令输出里的格式化字符串。计数器只读 {@code readableBytes()}，
 * 不 retain、不释放、不改写任何消息，然后原样向后传递。</p>
 *
 * <p>所有记录写入 {@code -Dmiku.zstd.ci.evidence=<path>} 指定的 JSONL 文件；每行一个对象，
 * 逐行 flush，因此进程被 SIGTERM/SIGKILL 时已写出的样本仍然可读。</p>
 */
public final class CiObserver implements ClientModInitializer {

    /** 证据文件路径（驱动通过 JVM 参数注入）。 */
    private static final String EVIDENCE_PROPERTY = "miku.zstd.ci.evidence";
    /** 采样间隔毫秒。 */
    private static final String INTERVAL_PROPERTY = "miku.zstd.ci.interval-ms";
    /** 管线内计数器名。 */
    private static final String COUNTER_NAME = "miku_zstd_ci_inbound";
    /** 需要记录来源路径的重点模组（其余模组只记 id/版本）。 */
    private static final List<String> ORIGIN_MODS = List.of("miku_zstd", "krypton", "fabric-api");

    private final LongAdder inboundBytes = new LongAdder();
    private final LongAdder inboundMessages = new LongAdder();
    private final long startedNanos = System.nanoTime();
    private volatile BufferedWriter writer;
    private volatile Channel countedChannel;
    private volatile Field channelField;
    private long playStartedNanos = -1L;
    private long lastPlayingNanos = -1L;
    private long playNanos = 0L;

    @Override
    public void onInitializeClient() {
        try {
            Path evidence = resolveEvidencePath();
            Files.createDirectories(evidence.getParent());
            writer = Files.newBufferedWriter(evidence, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (Throwable failure) {
            System.err.println("[miku-zstd-ci] cannot open evidence file: " + failure);
            return;
        }
        write(describeEnvironment());
        Runtime.getRuntime().addShutdownHook(new Thread(this::writeFinal, "miku-zstd-ci-final"));
        Thread sampler = new Thread(this::sampleLoop, "miku-zstd-ci-sampler");
        sampler.setDaemon(true);
        sampler.setPriority(Thread.MIN_PRIORITY);
        sampler.start();
    }

    private Path resolveEvidencePath() {
        String configured = System.getProperty(EVIDENCE_PROPERTY);
        if (configured != null && !configured.isBlank()) {
            return Paths.get(configured).toAbsolutePath();
        }
        Path gameDirectory;
        try {
            gameDirectory = Minecraft.getInstance().gameDirectory.toPath();
        } catch (Throwable ignored) {
            gameDirectory = Paths.get(".").toAbsolutePath();
        }
        return gameDirectory.resolve("miku-zstd-ci").resolve("evidence.jsonl");
    }

    private String describeEnvironment() {
        StringBuilder json = new StringBuilder();
        json.append("{\"type\":\"boot\"");
        json.append(",\"mono_ms\":").append(elapsedMillis());
        json.append(",\"wall_ms\":").append(System.currentTimeMillis());
        json.append(",\"java_version\":\"").append(System.getProperty("java.version", "")).append('"');
        json.append(",\"vm_name\":\"").append(System.getProperty("java.vm.name", "")).append('"');
        json.append(",\"os_name\":\"").append(System.getProperty("os.name", "")).append('"');
        json.append(",\"os_arch\":\"").append(System.getProperty("os.arch", "")).append('"');
        List<ModContainer> containers = new ArrayList<>(FabricLoader.getInstance().getAllMods());
        containers.sort((left, right) -> left.getMetadata().getId()
                .compareTo(right.getMetadata().getId()));
        json.append(",\"mod_count\":").append(containers.size());
        json.append(",\"mods\":[");
        boolean first = true;
        for (ModContainer container : containers) {
            if (!first) {
                json.append(',');
            }
            first = false;
            json.append('{');
            json.append("\"id\":\"").append(container.getMetadata().getId()).append('"');
            json.append(",\"version\":\"").append(container.getMetadata().getVersion().getFriendlyString()).append('"');
            ModOrigin origin = container.getOrigin();
            json.append(",\"origin_kind\":\"").append(origin.getKind().name()).append('"');
            if (ORIGIN_MODS.contains(container.getMetadata().getId())) {
                json.append(",\"origin_paths\":[");
                List<Path> paths = origin.getPaths();
                for (int index = 0; index < paths.size(); index++) {
                    if (index > 0) {
                        json.append(',');
                    }
                    json.append('"').append(escape(paths.get(index).toAbsolutePath().toString())).append('"');
                }
                json.append(']');
            }
            json.append('}');
        }
        json.append("]}");
        return json.toString();
    }

    private void sampleLoop() {
        long interval = Long.getLong(INTERVAL_PROPERTY, 500L);
        while (!Thread.currentThread().isInterrupted()) {
            try {
                sample();
                Thread.sleep(Math.max(50L, interval));
            } catch (InterruptedException interrupted) {
                return;
            } catch (Throwable ignored) {
                // 采样失败绝不能影响游戏本身。
            }
        }
    }

    private void sample() {
        Minecraft client = Minecraft.getInstance();
        if (client == null) {
            return;
        }
        installCounter(client);
        ClientPacketListener listener = client.getConnection();
        boolean playing = listener != null && client.level != null && client.player != null;
        long now = System.nanoTime();
        if (playing) {
            if (playStartedNanos < 0) {
                playStartedNanos = now;
            }
            if (lastPlayingNanos >= 0) {
                playNanos += Math.max(0L, now - lastPlayingNanos);
            }
            lastPlayingNanos = now;
        } else if (lastPlayingNanos >= 0) {
            playNanos += Math.max(0L, now - lastPlayingNanos);
            lastPlayingNanos = -1L;
        }
        int ticks = playing ? client.player.tickCount : -1;
        double x = playing ? client.player.getX() : Double.NaN;
        double y = playing ? client.player.getY() : Double.NaN;
        double z = playing ? client.player.getZ() : Double.NaN;
        write(sampleRecord(listener != null, playing, ticks, x, y, z));
    }

    private String sampleRecord(boolean connected, boolean playing, int ticks,
                                double x, double y, double z) {
        StringBuilder json = new StringBuilder();
        json.append("{\"type\":\"sample\"");
        json.append(",\"mono_ms\":").append(elapsedMillis());
        json.append(",\"wall_ms\":").append(System.currentTimeMillis());
        json.append(",\"connected\":").append(connected);
        json.append(",\"playing\":").append(playing);
        json.append(",\"inbound_bytes\":").append(inboundBytes.sum());
        json.append(",\"inbound_messages\":").append(inboundMessages.sum());
        json.append(",\"play_ms\":").append(playNanos / 1_000_000L);
        if (playing) {
            json.append(",\"ticks\":").append(ticks);
            json.append(",\"x\":").append(format(x));
            json.append(",\"y\":").append(format(y));
            json.append(",\"z\":").append(format(z));
        }
        json.append('}');
        return json.toString();
    }

    private void writeFinal() {
        StringBuilder json = new StringBuilder();
        json.append("{\"type\":\"final\"");
        json.append(",\"mono_ms\":").append(elapsedMillis());
        json.append(",\"inbound_bytes\":").append(inboundBytes.sum());
        json.append(",\"inbound_messages\":").append(inboundMessages.sum());
        json.append(",\"play_ms\":").append(playNanos / 1_000_000L);
        json.append('}');
        write(json.toString());
        BufferedWriter stream = writer;
        if (stream != null) {
            try {
                stream.close();
            } catch (IOException ignored) {
                // 关闭失败不影响退出。
            }
        }
    }

    private void installCounter(Minecraft client) {
        ClientPacketListener listener = client.getConnection();
        if (listener == null) {
            return;
        }
        Connection connection = listener.getConnection();
        Channel channel = channelOf(connection);
        if (channel == null || channel == countedChannel) {
            return;
        }
        synchronized (this) {
            if (channel == countedChannel) {
                return;
            }
            try {
                if (channel.pipeline().get(COUNTER_NAME) == null) {
                    channel.pipeline().addFirst(COUNTER_NAME, new Counter());
                }
                countedChannel = channel;
            } catch (Throwable ignored) {
                // 管线不可写（已关闭）时忽略，下次采样再试。
            }
        }
    }

    private Channel channelOf(Connection connection) {
        try {
            Field field = channelField;
            if (field == null) {
                field = Connection.class.getDeclaredField("channel");
                field.setAccessible(true);
                channelField = field;
            }
            Object value = field.get(connection);
            return value instanceof Channel channel ? channel : null;
        } catch (Throwable unavailable) {
            return null;
        }
    }

    private void write(String line) {
        BufferedWriter stream = writer;
        if (stream == null) {
            return;
        }
        synchronized (this) {
            try {
                stream.write(line);
                stream.newLine();
                stream.flush();
            } catch (IOException failed) {
                writer = null;
            }
        }
    }

    private long elapsedMillis() {
        return (System.nanoTime() - startedNanos) / 1_000_000L;
    }

    private static String format(double value) {
        return Double.isFinite(value) ? String.format(java.util.Locale.ROOT, "%.3f", value) : "null";
    }

    private static String escape(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    /** 只读入站计数器：不做 retain/release，只统计并原样向后传递。 */
    private final class Counter extends ChannelInboundHandlerAdapter {
        @Override
        public void channelRead(ChannelHandlerContext context, Object message) throws Exception {
            if (message instanceof ByteBuf buffer) {
                inboundBytes.add(buffer.readableBytes());
                inboundMessages.increment();
            }
            super.channelRead(context, message);
        }
    }
}
