package mikumc.zstd.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** {@link ZstdLatencyStats} 的桶映射、分位点与重置语义。 */
class ZstdLatencyStatsTest {

    @BeforeEach
    void resetStats() {
        ZstdLatencyStats.reset();
    }

    @Test
    void emptyStageReportsZero() {
        assertEquals(0L, ZstdLatencyStats.sampleCount(ZstdLatencyStats.Stage.QUEUE));
        assertEquals(0.0, ZstdLatencyStats.percentileMillis(ZstdLatencyStats.Stage.QUEUE, 0.99));
        assertEquals(0.0, ZstdLatencyStats.maxMillis(ZstdLatencyStats.Stage.QUEUE));
    }

    @Test
    void quantileEstimateIsNotBelowTheRecordedValue() {
        // 桶上界估计只允许偏保守：真实值落在桶内时不得被低估。
        long sample = 1_234_567L; // 1.234567 毫秒
        for (int i = 0; i < 100; i++) {
            ZstdLatencyStats.record(ZstdLatencyStats.Stage.SERVICE, sample);
        }
        double p50 = ZstdLatencyStats.percentileMillis(ZstdLatencyStats.Stage.SERVICE, 0.50);
        double p99 = ZstdLatencyStats.percentileMillis(ZstdLatencyStats.Stage.SERVICE, 0.99);
        double recorded = sample / 1_000_000.0;
        assertTrue(p50 >= recorded, "p50 " + p50 + " below recorded " + recorded);
        assertTrue(p99 >= p50, "quantiles must be monotonic");
        assertTrue(p99 <= recorded * 1.25, "bucket error must stay within one bucket");
        assertEquals(100L, ZstdLatencyStats.sampleCount(ZstdLatencyStats.Stage.SERVICE));
    }

    @Test
    void percentilesAreMonotonicAcrossStages() {
        for (int i = 1; i <= 1000; i++) {
            ZstdLatencyStats.record(ZstdLatencyStats.Stage.EVENT_LOOP, i * 1_000L);
        }
        double p50 = ZstdLatencyStats.percentileMillis(ZstdLatencyStats.Stage.EVENT_LOOP, 0.50);
        double p95 = ZstdLatencyStats.percentileMillis(ZstdLatencyStats.Stage.EVENT_LOOP, 0.95);
        double p99 = ZstdLatencyStats.percentileMillis(ZstdLatencyStats.Stage.EVENT_LOOP, 0.99);
        assertTrue(p50 <= p95 && p95 <= p99, "p50=" + p50 + " p95=" + p95 + " p99=" + p99);
        // 1000 个样本全部 ≤1 毫秒时，p99 也必须落在 1 毫秒附近而不是更小。
        assertTrue(p99 >= 0.99, "p99 " + p99);
    }

    @Test
    void stagesAreIndependent() {
        ZstdLatencyStats.record(ZstdLatencyStats.Stage.QUEUE, 5_000_000L);
        assertEquals(0L, ZstdLatencyStats.sampleCount(ZstdLatencyStats.Stage.SERVICE));
        assertEquals(1L, ZstdLatencyStats.sampleCount(ZstdLatencyStats.Stage.QUEUE));
        assertTrue(ZstdLatencyStats.maxMillis(ZstdLatencyStats.Stage.QUEUE) >= 5.0);
    }

    @Test
    void negativeSamplesAreIgnored() {
        ZstdLatencyStats.record(ZstdLatencyStats.Stage.WRITE, -1L);
        assertEquals(0L, ZstdLatencyStats.sampleCount(ZstdLatencyStats.Stage.WRITE));
    }

    @Test
    void resetClearsEverything() {
        ZstdLatencyStats.record(ZstdLatencyStats.Stage.QUEUE, 1_000_000L);
        ZstdLatencyStats.reset();
        assertEquals(0L, ZstdLatencyStats.sampleCount(ZstdLatencyStats.Stage.QUEUE));
        assertEquals(0.0, ZstdLatencyStats.maxMillis(ZstdLatencyStats.Stage.QUEUE));
    }

    @Test
    void machineLineHasStableFields() {
        ZstdLatencyStats.record(ZstdLatencyStats.Stage.SERVICE, 2_000_000L);
        String line = ZstdLatencyStats.machineLine();
        assertTrue(line.startsWith("lat: "), line);
        for (String field : new String[]{"queue_p50=", "queue_p99=", "service_p50=", "service_p99=",
                "eventloop_p50=", "eventloop_p99=", "write_p50=", "write_p99=", "samples="}) {
            assertTrue(line.contains(field), "missing " + field + " in " + line);
        }
    }
}
