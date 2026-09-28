package cn.miku.zstd.fabric.client;

import cn.miku.zstd.MikuZstd;
import cn.miku.zstd.ZstdConfig;
import cn.miku.zstd.ZstdLoginNetworking;
import net.fabricmc.api.ClientModInitializer;
import net.fabricmc.fabric.api.client.event.lifecycle.v1.ClientTickEvents;
import net.fabricmc.fabric.api.client.keymapping.v1.KeyMappingHelper;
import net.minecraft.client.KeyMapping;
import net.minecraft.client.Minecraft;
import net.minecraft.network.chat.Component;
import net.minecraft.resources.Identifier;

/**
 * 客户端入口：加载配置、注册 F8 键位（切换 HUD）。
 * HUD 绘制通过 MixinHud 注入 26.x 的 Hud.extractRenderState。
 *
 * <h2>3.1.1 变更</h2>
 * <p>切换 HUD 时给出<b>可见反馈</b>（聊天栏一行 + INFO 日志）。
 * 之前按下 F8 完全没有反馈，用户无法判断"按键没生效"还是"HUD 画不出来"，
 * 只能靠猜——这类静默开关本身就是可观测性缺陷。</p>
 */
public final class ZstdFabricClient implements ClientModInitializer {

    /** GLFW_KEY_F8 */
    private static final int KEY_F8 = 297;

    @Override
    public void onInitializeClient() {
        MikuZstd.init();

        ZstdLoginNetworking.register();

        // 使用三参构造（name, key, category）：内部默认键盘类型，
        // 避免引用 InputConstants$Type 枚举——该枚举常量在 26.2/26.3 间存在命名差异
        KeyMapping toggleHud = KeyMappingHelper.registerKeyMapping(new KeyMapping(
                "key.miku_zstd.toggle_hud",
                KEY_F8,
                KeyMapping.Category.register(Identifier.fromNamespaceAndPath("miku_zstd", "main"))));

        ClientTickEvents.END_CLIENT_TICK.register(client -> {
            while (toggleHud.consumeClick()) {
                ZstdConfig.hudEnabledRuntime = !ZstdConfig.hudEnabledRuntime;
                boolean on = ZstdConfig.hudEnabledRuntime;
                MikuZstd.LOGGER.info("[Zstd] HUD toggled by key: {}", on ? "ON" : "OFF");
                var player = Minecraft.getInstance().player;
                if (player != null) {
                    player.sendSystemMessage(Component.literal(
                            "\u00a7b[Miku-ZSTD]\u00a7r 统计 HUD: " + (on ? "\u00a7a已开启" : "\u00a77已关闭")));
                }
            }
        });

        MikuZstd.LOGGER.info("[Zstd] Miku-ZSTD client initialized (F8 toggles stats HUD, currently {})",
                ZstdConfig.hudEnabledRuntime ? "ON" : "OFF");
    }
}
