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
            // ⚠️ 必须沿父类链查找：这个字段声明在 DedicatedServerProperties 还是其父类
            // ServerProperties，随版本变动；只查 getDeclaredField（不含父类）时一旦声明在父类上
            // 就会抛 NoSuchField 并被下面的 catch 吞成 0 —— 于是 network-compression-threshold=-1
            // （压缩关闭）时插件不会按文档停用 zstd，每条登录都要白等约 2 秒才回落。
            for (Class<?> c = props.getClass(); c != null && c != Object.class; c = c.getSuperclass()) {
                try {
                    java.lang.reflect.Field f = c.getDeclaredField("networkCompressionThreshold");
                    f.setAccessible(true);
                    return f.getInt(props);
                } catch (NoSuchFieldException ignored) {
                    // 继续往父类找
                }
            }
            return 0;
        } catch (Throwable t) {
            return 0;
        }
    }
}
