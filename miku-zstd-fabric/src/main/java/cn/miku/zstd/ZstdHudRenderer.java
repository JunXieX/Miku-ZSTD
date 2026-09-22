package cn.miku.zstd;

import net.minecraft.client.Minecraft;
import net.minecraft.client.gui.Font;
import net.minecraft.client.gui.GuiGraphicsExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Pattern;

/**
 * 左上角统计 HUD：TX/RX 速率与压缩率。
 * 由 MixinHud 在 {@code Hud.extractRenderState} 尾部调用。
 *
 * <h2>3.1.1 变更</h2>
 * <p>加入可判定的诊断输出（限频，仅在 HUD 开启时）：HUD 打不开时，
 * 需要一眼区分三种原因——①按键没生效（状态仍为 OFF）、②渲染注入没跑到、
 * ③渲染跑了但统计为空。原来这三种情况在日志里都是"什么都没有"，无法定位。</p>
 */
public final class ZstdHudRenderer {

    private static final Logger LOGGER = LoggerFactory.getLogger(MikuZstd.LOGGER_NAME);

    private static final int REFRESH_MS = 100;
    private static final int LINE_HEIGHT = 10;
    /**
     * 文字颜色：必须是 <b>ARGB</b>（含 alpha）。
     *
     * <p>⚠️ 这里曾经写成 {@code 0xFFFFFF}——alpha=00 表示完全透明，于是背景框（fill 的
     * 0x80000000 alpha 明确给了 0x80）能画出来、文字却一个字都看不见。
     * vanilla {@code Hud} 传的是 {@code iconst_m1} = 0xFFFFFFFF，与之一致即可。</p>
     */
    private static final int TEXT_COLOR = 0xFFFFFFFF;
    /** 诊断输出限频（毫秒） */
    private static final long DIAG_INTERVAL_MS = 30000;
    private static final Pattern FORMATTING = Pattern.compile("\u00a7[0-9a-fk-orl]");

    private static long lastUpdateMs;
    private static long lastDiagMs;
    /** 上一次诊断的"类别"，类别变化时立即输出（避免周期性刷屏） */
    private static String lastDiagKey;
    private static String lastTxLine;
    private static String lastRxLine;

    private ZstdHudRenderer() {
    }

    public static void render(GuiGraphicsExtractor g, net.minecraft.client.DeltaTracker deltaTracker) {
        if (!ZstdConfig.hudEnabledRuntime) {
            return;
        }
        Minecraft mc = Minecraft.getInstance();
        long now = System.currentTimeMillis();

        if (mc.player == null) {
            // 标题界面/未进入世界：Hud.extractRenderState 仍会被调用，但没有玩家就不画
            diag("no-player", "HUD on but no player in world (title screen?) — nothing drawn");
            return;
        }

        if (now - lastUpdateMs >= REFRESH_MS) {
            lastUpdateMs = now;
            String stats = ZstdStatsData.captureTabStats(1);
            if (stats != null) {
                int sep = stats.indexOf('|');
                if (sep < 0) {
                    lastTxLine = stats.trim();
                } else {
                    lastTxLine = stats.substring(0, sep).trim();
                    lastRxLine = stats.substring(sep + 1).trim();
                }
                diag("drawing", "HUD drawing: " + lastTxLine + "  ||  " + lastRxLine);
            } else {
                diag("waiting", "HUD waiting for stats — " + ZstdStatsData.debugSummary());
            }
        }
        drawHud(g, mc.font, lastTxLine, lastRxLine);
    }

    /**
     * 诊断输出：帮助判定"HUD 为什么没内容"。类别变化时立即输出，否则按
     * {@link #DIAG_INTERVAL_MS} 限频——正常游玩时不会刷屏。
     */
    private static void diag(String key, String message) {
        long now = System.currentTimeMillis();
        boolean changed = !key.equals(lastDiagKey);
        if (!changed && now - lastDiagMs < DIAG_INTERVAL_MS) return;
        lastDiagKey = key;
        lastDiagMs = now;
        LOGGER.info("[Zstd] {}", message);
    }

    private static void drawHud(GuiGraphicsExtractor g, Font font, String tx, String rx) {
        if (tx == null && rx == null) {
            return;
        }
        int x = 4;
        int y = 4;
        int totalLines = (tx != null ? 1 : 0) + (rx != null ? 1 : 0);
        int maxW = Math.max(tx != null ? font.width(stripFormatting(tx)) : 0,
                rx != null ? font.width(stripFormatting(rx)) : 0) + 4;
        g.fill(x - 2, y - 2, x + maxW, y + LINE_HEIGHT * totalLines, 0x80000000);
        if (tx != null) {
            g.text(font, tx, x, y, TEXT_COLOR);
            y += LINE_HEIGHT;
        }
        if (rx != null) {
            g.text(font, rx, x, y, TEXT_COLOR);
        }
    }

    private static String stripFormatting(String s) {
        return FORMATTING.matcher(s).replaceAll("");
    }
}
