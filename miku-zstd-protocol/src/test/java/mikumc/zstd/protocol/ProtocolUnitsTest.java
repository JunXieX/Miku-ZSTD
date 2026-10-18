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
        // 只测非负值：协议里所有 varint 都是长度/状态码，而 tryRead 会主动拒绝负结果
        //（负结果只可能来自畸形输入，见下面的 varIntRejectsNegative）
        int[] values = {0, 1, 127, 128, 16383, 16384, 2097151, 2097152, Integer.MAX_VALUE};
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
    @DisplayName("VarInt：负数一律读作 INVALID（协议只承载非负长度与状态码）")
    void varIntRejectsNegative() {
        for (int v : new int[]{-1, -127, Integer.MIN_VALUE}) {
            ByteBuf buf = Unpooled.buffer();
            ZstdVarInts.write(buf, v);
            assertEquals(ZstdVarInts.INVALID, ZstdVarInts.tryRead(buf, Integer.MAX_VALUE),
                    "负数 " + v + " 必须被拒绝（它是畸形输入的信号，不是合法值）");
            buf.release();
        }
        // length 仍应给出真实的编码长度（写路径用它分配缓冲）
        assertEquals(5, ZstdVarInts.length(-1));
        assertEquals(5, ZstdVarInts.length(Integer.MIN_VALUE));
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
    // 协议帧上限（跨端硬约束）
    // ────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("帧上限：MC 外层帧是 3 字节 VarInt，上限 2^21-1")
    void mcFrameLimitIs21Bit() {
        // 这不是"我们选的"，而是 MC 的 Varint21FrameDecoder 决定的：
        // 超过它客户端抛 "length wider than 21-bit"，服务端 prepender 也装不下。
        assertEquals(2_097_151, ZstdVarInts.MC_MAX_FRAME_BYTES);
        // 用一个 3 字节 varint 刚好能写出这个值、但写不出 +1 —— 交叉验证常量本身
        assertEquals(3, ZstdVarInts.length(ZstdVarInts.MC_MAX_FRAME_BYTES));
        assertEquals(4, ZstdVarInts.length(ZstdVarInts.MC_MAX_FRAME_BYTES + 1));
    }

    @Test
    @DisplayName("帧上限：两个方向的字典同帧发送也不得撑破帧上限（含包头开销）")
    void bothDictionariesFitInOneFrame() {
        // zstd:dict 把两个方向的字典放进同一个包：
        //   [pktId][txId][len+"zstd:dict"][flags] + 2 × ([int crc][int len][bytes])
        // 包头固定开销按 64 字节保守估算（本题的判据与服务端 sendDictQuery 的兜底一致）。
        int perDir = ZstdVarInts.MAX_DICT_BYTES_PER_DIRECTION;
        long frameBytes = 2L * (perDir + 8) + 64;
        assertTrue(frameBytes <= ZstdVarInts.MC_MAX_FRAME_BYTES,
                "两方向各 " + perDir + "B 时合帧需要 " + frameBytes
                        + "B，已超过 MC 帧上限 " + ZstdVarInts.MC_MAX_FRAME_BYTES);

        // 反面：如果仍按 1MB 单方向来夹，两方向就会超限 —— 这正是本常量存在的理由
        long oneMegEach = 2L * (0x100000 + 8) + 64;
        assertTrue(oneMegEach > ZstdVarInts.MC_MAX_FRAME_BYTES,
                "前提校验：两方向各 1MB 应当超限，否则本上限就没有必要");
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

    @Test
    @DisplayName("采样过滤：按偏移判定（免复制路径）必须与整包判定逐例一致")
    void sampleFilterOffsetOverloadMatchesByteArray() {
        // 训练器热路径现在直接在整帧缓冲上按 (offset,length) 判定，只有通过才复制。
        // 两条路径的结论必须完全相同——否则"过滤掉哪些包"会随调用方式漂移。
        int[] payloads = {0, 1, 2, 3, 8, 9, 15, 16, 64};
        int[] thresholds = {0, 1, 16};
        for (int payload : payloads) {
            byte[] packet = pkt(1, payload);
            // 前面塞一段干扰字节，确保实现真的用了 offset 而不是从 0 开始
            byte[] frame = new byte[packet.length + 3];
            Arrays.fill(frame, 0, 3, (byte) 0x7F);
            System.arraycopy(packet, 0, frame, 3, packet.length);
            for (int th : thresholds) {
                assertEquals(ZstdSampleFilter.shouldKeep(packet, th),
                        ZstdSampleFilter.shouldKeep(frame, 3, packet.length, th),
                        "payload=" + payload + " threshold=" + th + " 两种判定的结论必须一致");
            }
        }
        // 越界一律拒绝（绝不因算错长度而读到帧外）
        assertFalse(ZstdSampleFilter.shouldKeep(new byte[4], 3, 5, 0));
        assertFalse(ZstdSampleFilter.shouldKeep(new byte[4], -1, 2, 0));
        assertFalse(ZstdSampleFilter.shouldKeep(new byte[4], 0, 0, 0));
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
        assertEquals("\u00a7b", ZstdBossBarFormat.colorize("&b"), "&b 是合法颜色码");
        // '&' 后不是颜色码时保持原样（例如 URL 查询串、或 &z 这类占位）
        assertEquals("x&zy", ZstdBossBarFormat.colorize("x&zy"));
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

    @Test
    @DisplayName("连接统计：只统计真正走过 zstd 的连接（Paper 端每条连接都会登记）")
    void connStatsExcludesNeverUsedConnections() {
        // Paper 端为**每条连接**都建 manager（含原版客户端，它也要走协商），
        // 因此 ACTIVE 集合里必然混着"只协商过、一帧都没发"的连接。
        // 若 top/totalSummary 不过滤，命令输出会被在线人数灌水。
        ZstdConnStats idle = new ZstdConnStats();   // 只登记、从未成帧（模拟原版客户端）
        ZstdConnStats active = new ZstdConnStats();
        active.record(1000, 200);
        try {
            String summary = ZstdConnStats.totalSummary();
            assertTrue(summary.contains("活跃 1 条"), "只应统计成过帧的连接：" + summary);

            java.util.List<String> rows = ZstdConnStats.top(10);
            assertEquals(1, rows.size(), "零帧连接不得出现在 top 列表中");
        } finally {
            idle.remove();
            active.remove();
        }
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
