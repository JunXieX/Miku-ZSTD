package mikumc.zstd.paper;

/**
 * 反射读取服务端配置的小工具（主类与命令共用）。
 *
 * <p>⚠️ 用的是 {@code MinecraftServer.getProperties().networkCompressionThreshold}，
 * 而不是"pipeline 里有没有 compress"——后者无法区分"尚未安装"与"永远不会安装"。</p>
 */
final class ZstdPaperCommandSupport {

    private ZstdPaperCommandSupport() {
    }

    /** @return 原版压缩阈值；读取失败返回 0（按"可用"处理，保证不影响进服） */
    static int readCompressionThreshold() {
        try {
            Object bukkit = org.bukkit.Bukkit.getServer();
            Object mcServer = bukkit.getClass().getMethod("getServer").invoke(bukkit);
            Object props = mcServer.getClass().getMethod("getProperties").invoke(mcServer);
            java.lang.reflect.Field f = props.getClass().getDeclaredField("networkCompressionThreshold");
            f.setAccessible(true);
            return f.getInt(props);
        } catch (Throwable t) {
            return 0;
        }
    }
}
