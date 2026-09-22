package mikumc.zstd.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 共享协议层里那些"短小但对齐要求很高"的组件的单元测试。
 *
 * <p>这些类都是三端（或两端）共用的契约点：VarInt 编码、字典 id、协商状态码、
 * 采样过滤、BossBar 模板、流量统计。它们的共同风险是"改错了不报错，只悄悄改变行为"，
 * 因此每一个都要有可执行的边界样例。</p>
 */
class ProtocolUnitsTest {

    // ────────────────────────────────────────────────────────────────
    // VarInt
    // ────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("VarInt：length 与 write 必须一致，且覆盖 1~5 字节各档边界")
    void varIntLengthMatchesWrite() {
        int[] values = {0, 1, 127, 128, 16383, 16384, 2097151, 2097152,
                Integer.MAX_VALUE, Integer.MIN_VALUE, -1};
        for (int v : values) {
            ByteBuf buf = Unpooled.buffer();
            ZstdVarInts.write(buf, v);
            assertEquals(ZstdVarInts.length(v), buf.readableBytes(),
                    "length(" + v + ") 与实际写出的字节数不符");
            assertEquals(v, ZstdVarInts.tryRead(buf, Integer.MAX_VALUE),
                    "写完再读必须得到原值：" + v);
            buf.release();
        }
    }

    @Test
    @DisplayName("VarInt：tryRead 数据不足时不消费内部（NEED_MORE）")
    void varIntNeedMoreDoesNotConsume() {
        ByteBuf buf = Unpooled.buffer().writeByte(0x80); // 只有续位，缺后续字节
        int before = buf.readerIndex();
        assertEquals(ZstdVarInts.NEED_MORE, ZstdVarInts.tryRead(buf, Integer.MAX_VALUE));
        assertEquals(before, buf.readerIndex(), "数据不足时必须复位读位置，否则半帧会丢字节");
        buf.release();
    }

    @Test
    @DisplayName("VarInt：超过 maxValue 判为 INVALID")
    void varIntRejectsOverMax() {
        ByteBuf buf = Unpooled.buffer();
        ZstdVarInts.write(buf, 1000);
        assertEquals(ZstdVarInts.INVALID, ZstdVarInts.tryRead(buf, 100));
        buf.release();
    }

    @Test
    @DisplayName("VarInt：readAt 返回结束位置，且拒绝 5 字节续位")
    void varIntReadAtAndInvalid() {
        byte[] data = {0x7F}; // 127
        int[] end = {0};
        assertEquals(127, ZstdVarInts.readAt(data, 0, end));
        assertEquals(1, end[0]);

        byte[] bad = {(byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, 1};
        assertEquals(ZstdVarInts.INVALID, ZstdVarInts.readAt(bad, 0, end));
    }

    @Test
    @DisplayName("VarInt：readOr 在成功时推进游标、失败时返回 fallback")
    void varIntReadOr() {
        byte[] data = {0x05, 0x7F};
        int[] cursor = {0};
        assertEquals(5, ZstdVarInts.readOr(data, cursor, -1));
        assertEquals(1, cursor[0], "成功时必须把游标推到 varint 之后");
        assertEquals(127, ZstdVarInts.readOr(data, cursor, -1));

        int[] empty = {0};
        assertEquals(-9, ZstdVarInts.readOr(new byte[0], empty, -9));
    }

    // ────────────────────────────────────────────────────────────────
    // 字典 id（跨端契约）
    // ────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("字典 id：同一内容恒等、不同内容不同、空值返回 NONE")
    void dictIdIsContentAddressed() {
        byte[] a = "dictionary-A".getBytes(StandardCharsets.US_ASCII);
        byte[] aCopy = Arrays.copyOf(a, a.length);
        byte[] b = "dictionary-B".getBytes(StandardCharsets.US_ASCII);

        assertEquals(ZstdDictId.of(a), ZstdDictId.of(aCopy), "同样内容必须得到同样的 id");
        assertNotEquals(ZstdDictId.of(a), ZstdDictId.of(b), "不同内容不应碰撞（同长度不同字节）");
        assertEquals(ZstdDictId.NONE, ZstdDictId.of(null));
        assertEquals(ZstdDictId.NONE, ZstdDictId.of(new byte[0]));
    }

    @Test
    @DisplayName("字典 id：wireChecksum 必须是 of 的低 32 位（线上帧与缓存键同源）")
    void dictIdWireChecksumMatchesId() {
        byte[] d = "some-dict-bytes".getBytes(StandardCharsets.US_ASCII);
        assertEquals((int) ZstdDictId.of(d), ZstdDictId.wireChecksum(d),
                "线上 int 校验字段必须与 id 同源，否则客户端缓存键会对不上");
    }

    // ────────────────────────────────────────────────────────────────
    // 协商状态码（白名单）
    // ────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("状态码：0/1/2 原样保留，其它一律归一为 2（保持原版）")
    void negotiateStatusWhitelist() {
        assertEquals(0, ZstdNegotiateStatus.sanitize(0));
        assertEquals(1, ZstdNegotiateStatus.sanitize(1));
        assertEquals(2, ZstdNegotiateStatus.sanitize(2));

        // 畸形载荷里的越界值绝不能被当成"第三种状态"放行
        for (int bad : new int[]{-1, 3, 7, 255, Integer.MAX_VALUE, Integer.MIN_VALUE}) {
            assertEquals(ZstdNegotiateStatus.VANILLA, ZstdNegotiateStatus.sanitize(bad),
                    "越界值 " + bad + " 必须归一为 VANILLA");
        }
        assertTrue(ZstdNegotiateStatus.needsDict(1));
        assertFalse(ZstdNegotiateStatus.needsDict(0));
        assertFalse(ZstdNegotiateStatus.needsDict(2));
    }

    // ────────────────────────────────────────────────────────────────
    // 采样过滤
    // ────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("采样过滤：过短、恰好 8 字节（多为 KeepAlive）与低于门槛的包都被排除")
    void sampleFilterRules() {
        int threshold = 16;
        assertFalse(ZstdSampleFilter.shouldKeep(null, threshold));
        assertFalse(ZstdSampleFilter.shouldKeep(new byte[0], threshold));

        // 包 = [varint id][payload]，id 占 1 字节
        assertFalse(ZstdSampleFilter.shouldKeep(pkt(1, 2), threshold), "载荷 2 字节太短");
        assertFalse(ZstdSampleFilter.shouldKeep(pkt(1, 8), threshold), "8 字节载荷按启发式排除");
        assertFalse(ZstdSampleFilter.shouldKeep(pkt(1, 15), threshold), "低于门槛");
        assertTrue(ZstdSampleFilter.shouldKeep(pkt(1, 16), threshold), "达到门槛应保留");
        assertTrue(ZstdSampleFilter.shouldKeep(pkt(1, 64), threshold));
    }

    @Test
    @DisplayName("采样过滤：门槛调低到 1 时，短载荷规则仍会拦下 ≤2 与 8 字节")
    void sampleFilterLowThresholdStillGuards() {
        assertFalse(ZstdSampleFilter.shouldKeep(pkt(1, 1), 1));
        assertFalse(ZstdSampleFilter.shouldKeep(pkt(1, 2), 1));
        assertFalse(ZstdSampleFilter.shouldKeep(pkt(1, 8), 1));
        assertTrue(ZstdSampleFilter.shouldKeep(pkt(1, 3), 1));
        assertTrue(ZstdSampleFilter.shouldKeep(pkt(1, 4), 1));
    }

    // ────────────────────────────────────────────────────────────────
    // BossBar 模板与单位
    // ────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("BossBar：占位符替换、空闲时 ratio 显示为 --")
    void bossBarSubstitute() {
        String fmt = ZstdBossBarFormat.DEFAULT_FORMAT;
        String out = ZstdBossBarFormat.substitute(fmt, 12, 1000, 176, 0.176);
        assertTrue(out.contains("12"), out);
        assertTrue(out.contains("1.0KB/s"), out);
        assertTrue(out.contains("176B/s"), out);
        assertTrue(out.contains("17.6%"), out);

        String idle = ZstdBossBarFormat.substitute(fmt, 0, 0, 0, 1.0);
        assertTrue(idle.contains("--"), "空闲时不应显示 100.0%（会被误读成压缩很差）: " + idle);
    }

    @Test
    @DisplayName("BossBar：fmtBytes 用十进制单位（项目内统一口径）")
    void bossBarByteUnits() {
        assertEquals("0B", ZstdBossBarFormat.fmtBytes(0));
        assertEquals("999B", ZstdBossBarFormat.fmtBytes(999));
        assertEquals("1.0KB", ZstdBossBarFormat.fmtBytes(1000));
        assertEquals("1.5KB", ZstdBossBarFormat.fmtBytes(1500));
        assertEquals("1.0MB", ZstdBossBarFormat.fmtBytes(1_000_000));
        assertEquals("1.00GB", ZstdBossBarFormat.fmtBytes(1_000_000_000L));
        assertEquals("1.0KB/s", ZstdBossBarFormat.rate(1000));
    }

    @Test
    @DisplayName("BossBar：colorize 只转换合法颜色码，且不动其它 &")
    void bossBarColorize() {
        assertEquals("\u00a7aOK", ZstdBossBarFormat.colorize("&aOK"));
        assertEquals("\u00a7lB\u00a7r", ZstdBossBarFormat.colorize("&lB&r"));
        // '&' 后不是颜色码时保持原样（例如 URL 查询串里的 &）
        assertEquals("a&b", ZstdBossBarFormat.colorize("a&b"));
        assertEquals("&", ZstdBossBarFormat.colorize("&"), "结尾单 & 不应越界");
    }

    // ────────────────────────────────────────────────────────────────
    // 流量统计（两端共享）
    // ────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("流量统计：关闭时不累计，开启后按差额算速率，活跃计数不为负")
    void trafficCounterSampling() {
        ZstdTrafficCounter.setEnabled(false);
        ZstdTrafficCounter.record(1000, 200);
        ZstdTrafficCounter.resetBaseline();
        ZstdTrafficCounter.Snapshot off = ZstdTrafficCounter.sample();
        assertEquals(0, off.rawPerSec, "关闭采集时不应累计");

        ZstdTrafficCounter.setEnabled(true);
        ZstdTrafficCounter.resetBaseline();
        ZstdTrafficCounter.record(10_000, 2_000);
        ZstdTrafficCounter.Snapshot on = ZstdTrafficCounter.sample();
        assertTrue(on.rawPerSec > 0, "开启后应当有速率");
        assertEquals(0.2, on.ratio, 0.05, "ratio = wire / raw");

        // 活跃计数：多减几次也不能变成负数（连接关闭路径可能重复调用）
        int before = ZstdTrafficCounter.sample().players;
        ZstdTrafficCounter.playerActivated();
        ZstdTrafficCounter.playerDeactivated();
        ZstdTrafficCounter.playerDeactivated();
        ZstdTrafficCounter.playerDeactivated();
        assertEquals(before, ZstdTrafficCounter.sample().players, "计数不得为负");

        ZstdTrafficCounter.setEnabled(false);
    }

    // ────────────────────────────────────────────────────────────────
    // 字典采纳判定（"字典会不会被启用"的唯一开关）
    // ────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("字典采纳：首部字典只要不劣化就采纳；替换字典才看阈值")
    void dictAdoption() {
        // 首部字典：改进 0.1% 也应采纳（否则小包永远压不动，整条机制成死代码）
        assertTrue(ZstdDictAdoption.shouldAdoptDict(true, 0.001, 0.01));
        assertFalse(ZstdDictAdoption.shouldAdoptDict(true, 0.0, 0.01), "不劣化=严格大于 0");
        assertFalse(ZstdDictAdoption.shouldAdoptDict(true, -0.2, 0.01));

        // 替换字典：必须达到阈值
        assertFalse(ZstdDictAdoption.shouldAdoptDict(false, 0.009, 0.01));
        assertTrue(ZstdDictAdoption.shouldAdoptDict(false, 0.01, 0.01));
    }

    @Test
    @DisplayName("字典采纳：分档边界与改进率计算")
    void dictBandMath() {
        assertEquals(0, ZstdDictAdoption.bandOf(ZstdDictAdoption.SMALL_BELOW - 1));
        assertEquals(1, ZstdDictAdoption.bandOf(ZstdDictAdoption.SMALL_BELOW));
        assertEquals(1, ZstdDictAdoption.bandOf(ZstdDictAdoption.MID_BELOW - 1));
        assertEquals(2, ZstdDictAdoption.bandOf(ZstdDictAdoption.MID_BELOW));

        assertEquals(0.0, ZstdDictAdoption.bandRatio(0, 100), 1e-9, "旧字节为 0 时不得除零");
        assertEquals(0.2, ZstdDictAdoption.bandRatio(1000, 800), 1e-9);
        assertEquals(3, ZstdDictAdoption.BANDS, "分档数量与 bandOf 的返回值域必须一致");
    }

    // ────────────────────────────────────────────────────────────────

    /** 造一个包：[varint id][payload of given size]。 */
    private static byte[] pkt(int id, int payloadSize) {
        ByteBuf buf = Unpooled.buffer();
        ZstdVarInts.write(buf, id);
        buf.writeBytes(new byte[payloadSize]);
        byte[] out = new byte[buf.readableBytes()];
        buf.getBytes(buf.readerIndex(), out);
        buf.release();
        return out;
    }
}
