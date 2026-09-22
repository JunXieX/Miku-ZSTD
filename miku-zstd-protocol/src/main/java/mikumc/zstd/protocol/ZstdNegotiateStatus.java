package mikumc.zstd.protocol;

/**
 * 协商应答状态码的<b>白名单</b>（三端共用）。
 *
 * <h2>为什么必须共用且必须白名单</h2>
 * <p>{@code 0 = 客户端会启用 zstd}、{@code 1 = 需要字典（仍打算启用）}、
 * {@code 2 = 客户端保持原版}。服务端严格按 2 回落原版 zlib，因此客户端任何
 * 返回 2 的分支都必须同时放弃激活——否则会出现"服务端 zlib / 客户端 zstd"
 * 的必断连组合。</p>
 *
 * <p>三端各自解析状态码（Velocity 有两处捕获点、Paper 一处、客户端写入侧一处），
 * 以前每处都自己写 {@code (v == NEED_MORE || v == INVALID) ? 2 : v}——
 * 越界值会被<b>原样放行</b>：一个畸形载荷里的 {@code 7} 会被当成"既不是 0 也不是
 * 2"的第三种状态，在两端产生不同解释。这里统一归一化：<b>任何非 0/1/2 的值一律视为
 * 2（保持原版）</b>——这是唯一安全的默认值，"不动"永远比"单侧动"安全。</p>
 */
public final class ZstdNegotiateStatus {

    /** 客户端会启用 zstd（字典已就绪或不需要字典）。 */
    public static final int READY = 0;
    /** 客户端需要服务端推送字典，推送成功后仍会启用 zstd。 */
    public static final int NEED_DICT = 1;
    /** 客户端保持原版 zlib（协议不匹配 / 未安装模组 / 无法加载字典）。 */
    public static final int VANILLA = 2;

    /** 解析失败时的兜底值：按"保持原版"处理。 */
    public static final int FALLBACK = VANILLA;

    private ZstdNegotiateStatus() {
    }

    /** 归一化：非 0/1/2 一律返回 {@link #VANILLA}。 */
    public static int sanitize(int raw) {
        return raw == READY || raw == NEED_DICT || raw == VANILLA ? raw : VANILLA;
    }

    /** 该状态是否要求服务端推送字典。 */
    public static boolean needsDict(int raw) {
        return raw == NEED_DICT;
    }
}
