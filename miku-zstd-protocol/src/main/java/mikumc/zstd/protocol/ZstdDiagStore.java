package mikumc.zstd.protocol;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Stream;

/**
 * 卡住会话的现场落地。
 *
 * <p>一次卡住会被写成一个目录，三件套放在一起——分开存的话，事后要靠时间戳猜哪两个文件
 * 是同一次事件，而这个仓库已经因为"两边日志对不上时刻"吃过一次教训。</p>
 *
 * <pre>
 * plugins/Miku-ZSTD/logs/stall/&lt;player&gt;-&lt;yyyyMMdd-HHmmss&gt;/
 *   server.log     本端调试文件尾部（同一时刻的上下文）
 *   client.log.gz  客户端回传原文（已是 gzip，不再解压落地）
 *   session.txt    两端版本、拓扑、远端地址、触发时刻、各阶段耗时
 * </pre>
 *
 * <p>所有方法都不抛异常：诊断失败不能影响登录，调用方只关心"有没有拿到路径"。</p>
 */
public final class ZstdDiagStore {

    private static final DateTimeFormatter STAMP =
            DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss", Locale.ROOT);

    private ZstdDiagStore() {
    }

    /**
     * 写一组现场。
     *
     * @param stallRoot   现场根目录（{@code <dataDir>/logs/stall}）
     * @param player      玩家名（会被净化，作为目录名前缀）
     * @param epochMillis 触发时刻
     * @param serverTail  本端调试文件尾部（可为空）
     * @param clientGzip  客户端回传的 gzip 原文（没有就传 {@code null}）
     * @param session     会话事实（顺序即写入顺序，可为空）
     * @param keepBundles 保留组数，&lt;=0 表示不清理
     * @return 现场目录；失败返回 {@code null}
     */
    public static Path writeBundle(Path stallRoot, String player, long epochMillis,
                                   byte[] serverTail, byte[] clientGzip,
                                   Map<String, String> session, int keepBundles) {
        if (stallRoot == null) {
            return null;
        }
        try {
            Files.createDirectories(stallRoot);
            String stamp = STAMP.format(Instant.ofEpochMilli(epochMillis).atZone(ZoneId.systemDefault()));
            String base = ZstdDiagFrames.sanitize(player, 32) + "-" + stamp;
            Path dir = stallRoot.resolve(base);
            for (int i = 2; Files.exists(dir) && i < 100; i++) {
                dir = stallRoot.resolve(base + "-" + i);
            }
            Files.createDirectories(dir);
            if (serverTail != null && serverTail.length > 0) {
                Files.write(dir.resolve("server.log"), serverTail);
            }
            if (clientGzip != null && clientGzip.length > 0) {
                Files.write(dir.resolve("client.log.gz"), clientGzip);
            }
            Files.writeString(dir.resolve("session.txt"), sessionText(player, epochMillis, session),
                    StandardCharsets.UTF_8);
            prune(stallRoot, keepBundles);
            return dir;
        } catch (Throwable t) {
            return null;
        }
    }

    /** 只保留最近 {@code keepBundles} 组（按目录名里的时间戳排序，与创建时间无关）。 */
    public static void prune(Path stallRoot, int keepBundles) {
        if (stallRoot == null || keepBundles <= 0) {
            return;
        }
        try {
            List<Path> dirs = new ArrayList<>();
            try (Stream<Path> stream = Files.list(stallRoot)) {
                stream.filter(Files::isDirectory).forEach(dirs::add);
            }
            if (dirs.size() <= keepBundles) {
                return;
            }
            dirs.sort((a, b) -> a.getFileName().toString().compareTo(b.getFileName().toString()));
            for (int i = 0; i < dirs.size() - keepBundles; i++) {
                deleteTree(dirs.get(i));
            }
        } catch (Throwable ignored) {
            // 清理失败不是错误：下次再清
        }
    }

    private static void deleteTree(Path root) {
        try (Stream<Path> stream = Files.walk(root)) {
            stream.sorted((a, b) -> b.getNameCount() - a.getNameCount()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (Throwable ignored) {
                    // 单个文件删不掉不影响其它
                }
            });
        } catch (Throwable ignored) {
            // 同上
        }
    }

    private static String sessionText(String player, long epochMillis, Map<String, String> session) {
        StringBuilder sb = new StringBuilder(256);
        sb.append("玩家: ").append(player == null ? "?" : player).append('\n');
        sb.append("触发: ").append(Instant.ofEpochMilli(epochMillis)).append('\n');
        if (session != null) {
            for (Map.Entry<String, String> e : session.entrySet()) {
                sb.append(e.getKey()).append(": ").append(e.getValue()).append('\n');
            }
        }
        return sb.toString();
    }
}
