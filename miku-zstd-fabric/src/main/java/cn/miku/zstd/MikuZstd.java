package cn.miku.zstd;

import mikumc.zstd.protocol.ZstdDebugLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 模组入口常量与初始化。
 *
 * <h2>配置文件位置</h2>
 * <p>当前为 {@code <游戏目录>/Miku-ZSTD/config.yml}（与服务端插件的目录命名一致）。
 * 历史上有过两代位置，首次启动时若<b>只</b>找到旧文件会自动<b>搬</b>过来，不会丢设置：</p>
 * <ol>
 *   <li>{@code miku-zstd/config.yml}（4.1.6 起的自建目录）</li>
 *   <li>{@code config/miku_zstd.yml}（最早的位置，埋在 config/ 里不容易找到）</li>
 * </ol>
 *
 * <h2>3.1.0 变更</h2>
 * <ul>
 *   <li>日志器名统一为 {@link #LOGGER_NAME}（旧实现存在多个名字，无法按 logger 统一开关）。</li>
 * </ul>
 */
public final class MikuZstd {

    public static final String MOD_ID = "miku_zstd";
    /** 全模组统一的日志器名（配置里可用它单独调等级） */
    public static final String LOGGER_NAME = "miku-zstd";
    public static final Logger LOGGER = LoggerFactory.getLogger(LOGGER_NAME);

    private MikuZstd() {
    }

    public static void init() {
        Path configPath = Paths.get("Miku-ZSTD", "config.yml");
        migrateLegacyConfig(configPath);
        ZstdConfig.load(configPath);
        initDebugLog();
        detectKrypton();
    }

    /**
     * 打开调试文件通道并写会话横幅。
     *
     * <p>放在配置读完之后、任何网络逻辑之前：协商、看门狗、切换确认的诊断才有地方落。
     * 打开失败（目录只读、磁盘满）只降级为"不写文件"，游戏照常进出服。</p>
     *
     * <p>由 {@code logging.debug} 决定（默认开启）：关掉后不建文件，服务端的取证查询
     * 只会收到"没有日志"；改完需要重启游戏生效。</p>
     */
    private static void initDebugLog() {
        ZstdConfig cfg = ZstdConfig.INSTANCE;
        ZstdDebugLog.configure(
                Paths.get(cfg.debugFile),
                cfg.debug,
                cfg.debugMaxFileMb << 20,
                cfg.debugKeepFiles,
                null);
        if (!cfg.debug) {
            LOGGER.info("[Zstd] 调试日志已关闭（logging.debug=false）：连不上服时将没有文件可发，"
                    + "服务端也拿不到你的日志");
        }
        if (ZstdDebugLog.file() != null) {
            Map<String, String> facts = new LinkedHashMap<>();
            facts.put("hud_enabled", String.valueOf(cfg.hudEnabled));
            facts.put("debug", String.valueOf(cfg.debug));
            facts.put("krypton", kryptonState());
            ZstdDebugLog.sessionHeader(modVersion(), "Fabric client", facts);
            // 控制台只留这一行：玩家据此就能找到要发的文件。
            LOGGER.info("[Zstd] 调试日志: {}（连不上服时把这个文件发出来）", ZstdDebugLog.file());
        }
    }

    /** 模组版本；取不到时退回 "?"，不让日志初始化因为元数据问题失败。 */
    private static String modVersion() {
        try {
            return net.fabricmc.loader.api.FabricLoader.getInstance()
                    .getModContainer(MOD_ID)
                    .map(container -> container.getMetadata().getVersion().getFriendlyString())
                    .orElse("?");
        } catch (Throwable t) {
            return "?";
        }
    }

    /** 是否装了 Krypton（只影响诊断口径：上行由谁处理）。 */
    private static String kryptonState() {
        try {
            Class.forName("me.steinborn.krypton.mod.shared.network.compression.MinecraftCompressEncoder");
            return "installed";
        } catch (Throwable t) {
            return "absent";
        }
    }

    /**
     * 一次性说明 Krypton 共存口径：上行继续使用其原版协议压缩、加密、长度前缀与 flush 优化；
     * 已协商的 zstd 下行由本模组先解包，再从 decompress 的 context 向后交付。
     */
    private static void detectKrypton() {
        try {
            Class.forName("me.steinborn.krypton.mod.shared.network.compression.MinecraftCompressEncoder");
            LOGGER.info("[Zstd] 检测到 Krypton：zstd 仅接管已协商的下行解压，"
                    + "上行压缩、加密、长度前缀与 flush 合并继续由原版/Krypton 处理");
        } catch (ClassNotFoundException notInstalled) {
            // 未安装 Krypton：无需提示
        } catch (Throwable t) {
            ZstdDebugLog.d("[Zstd] Krypton 检测跳过: {}", t.toString());
        }
    }

    /**
     * 搬迁旧版本的配置文件。
     *
     * <p>4.1.6 之前配置放在 {@code config/miku_zstd.yml}。新位置还没有文件、而旧文件存在时，
     * 直接<b>搬</b>过来——用户已有的显示设置与注释不该因为换了路径就丢掉，
     * 同时也不留旧文件，避免以后分不清哪个才是生效的。</p>
     */
    private static void migrateLegacyConfig(Path configPath) {
        if (Files.exists(configPath)) {
            return;
        }
        // 依次找更早的两代路径：4.1.6 的 miku-zstd/config.yml、以及最早的 config/miku_zstd.yml
        Path legacy = Paths.get("miku-zstd", "config.yml");
        if (!Files.exists(legacy)) {
            legacy = Paths.get("config", MOD_ID + ".yml");
        }
        if (!Files.exists(legacy)) {
            return;
        }
        try {
            Files.createDirectories(configPath.getParent());
            // 用 move 而不是 copy：搬走后旧位置不再残留，避免用户分不清哪个在生效
            Files.move(legacy, configPath);
            LOGGER.info("[Zstd] 已把旧配置 {} 迁移到 {}", legacy, configPath);
        } catch (IOException e) {
            LOGGER.warn("[Zstd] 旧配置迁移失败，将使用默认值：{}", e.toString());
        }
    }
}
