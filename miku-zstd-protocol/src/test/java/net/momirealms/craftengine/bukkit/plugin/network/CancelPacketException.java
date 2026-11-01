package net.momirealms.craftengine.bukkit.plugin.network;

/**
 * 测试夹具：与 CraftEngine 同名同包的异常。
 *
 * <p>生产代码按 {@code getSimpleName()} 识别"取消发包"的控制流异常，因此这里只需
 * 类名一致即可复现真实行为，不必引入那个插件做编译期依赖。</p>
 */
public class CancelPacketException extends RuntimeException {
    public CancelPacketException() {
        super("packet cancelled");
    }
}
