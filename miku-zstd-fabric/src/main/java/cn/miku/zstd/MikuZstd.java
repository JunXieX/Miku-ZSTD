package cn.miku.zstd;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import mikumc.zstd.protocol.ZstdCompressPool;

import java.nio.file.Path;
import java.nio.file.Paths;

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
        ZstdCompressPool.init(ZstdConfig.INSTANCE.compressThreads, 5);
        detectKrypton();
    }

    /**
     * 检测 Krypton 是否在场，并说明两者如何共存。
     *
     * <p>Krypton 在 {@code setupCompression} 的 HEAD 用<b>同名</b> handler
     * （{@code compress}/{@code decompress}）装上 libdeflate 版压缩器；本模组在 TAIL 把
     * zstd 编解码器插到这些名字<b>旁边</b>，出站跳过它、入站抢在它前面解。所以两者可以共存：
     * zstd 负责压缩，Krypton 的加密、长度前缀与 flush 合并继续生效；本轮未协商成功时
     * Krypton 继续用 libdeflate。</p>
     *
     * <p>只做一次性日志，不据此改变行为——真正的冲突由 {@code activateInEventLoop}
     * 的"双侧校验"兜住（缺一侧就放弃激活）。</p>
     */
    private static void detectKrypton() {
        try {
            Class.forName("me.steinborn.krypton.mod.shared.network.compression.MinecraftCompressEncoder");
            LOGGER.info("[Zstd] 检测到 Krypton：zstd 激活后将接管压缩管线，"
                    + "Krypton 的其余优化（加密 / 长度前缀 / flush 合并）继续生效");
        } catch (ClassNotFoundException notInstalled) {
            // 未安装 Krypton：无需提示
        } catch (Throwable t) {
            LOGGER.debug("[Zstd] Krypton 检测跳过: {}", t.toString());
        }
    }

    /**
     * 搬迁旧版本的配置文件。
     *
     * <p>4.1.6 之前配置放在 {@code config/miku_zstd.yml}。新位置还没有文件、而旧文件存在时，
     * 直接<b>搬</b>过来——用户手改过的等级/阈值不该因为换了路径就丢掉，
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
