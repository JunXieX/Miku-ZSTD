package cn.miku.zstd;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ZstdStatsDataTest {

    // 统计状态属于整个客户端，首次采样到恢复采样必须在同一条流程中验证。
    @Test
    void capturesTrafficIdleAndResumeInOneSession() throws InterruptedException {
        assertNull(ZstdStatsData.captureTabStats(1), "首次采样只建立基线");

        Thread.sleep(110);
        ZstdStatsData.addRxBatch(4_096, 16_384);
        assertNull(ZstdStatsData.captureTabStats(1), "第二次采样仍未就绪");

        Thread.sleep(110);
        ZstdStatsData.addRxBatch(4_096, 16_384);
        String active = ZstdStatsData.captureTabStats(1);
        assertNotNull(active, "有效下行流量应产生统计");
        assertTrue(active.contains("下行："), active);
        assertTrue(active.contains("→"), active);
        assertTrue(active.contains("/s"), active);
        assertFalse(active.contains("TX"), active);
        assertFalse(active.contains("RX"), active);
        assertFalse(active.contains("Vanilla"), active);
        assertFalse(active.contains("\n"), "HUD 统计应为单行");
        assertFalse(active.matches(".*\\s0B/s\\s*→\\s*0B/s\\s.*"), "有流量时不应输出双零速率");

        String idle = null;
        for (int i = 0; i < 10; i++) {
            Thread.sleep(110);
            idle = ZstdStatsData.captureTabStats(1);
        }
        assertNotNull(idle, "就绪后的空闲采样应输出零速率，而非保留旧统计");
        assertTrue(idle.matches(".*\\s0B/s\\s*→\\s*0B/s\\s.*"), idle);

        ZstdStatsData.addRxBatch(4_000_000, 16_000_000);
        Thread.sleep(3_100);
        assertNull(ZstdStatsData.captureTabStats(1), "停采超过三秒后应重新建立基线");

        Thread.sleep(110);
        String resumedIdle = ZstdStatsData.captureTabStats(1);
        assertNotNull(resumedIdle, "重建基线后的采样应恢复输出");
        assertTrue(resumedIdle.matches(".*\\s0B/s\\s*→\\s*0B/s\\s.*"),
                "停采期间的旧流量不应进入恢复后的速率：" + resumedIdle);
    }
}
