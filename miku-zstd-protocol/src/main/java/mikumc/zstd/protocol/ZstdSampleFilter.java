package mikumc.zstd.protocol;

/**
 * 字典训练的采样过滤规则（<b>两端共用</b>）。
 *
 * <h2>为什么要抽出来</h2>
 * 这段"这个包值不值得当字典样本"的判定，原来在 Velocity 与 Paper 各有一份逐行相同的实现。
 * 它恰好是<b>最容易悄悄漂移</b>的那类代码：规则本身很短、看着"照抄不会错"，
 * 可一旦一端改了阈值或加了排除条件，另一端不会报错、只会让字典质量悄悄变差——
 * 这正是本项目已经踩过的坑（协议门控少抄一行 nextState 判断，表现为"命令不存在"）。
 *
 * <p>判定是纯函数（只依赖 {@code pruneMinPayload}），放在共享模块没有副作用，
 * 两端只保留一层转发。</p>
 */
public final class ZstdSampleFilter {

    private ZstdSampleFilter() {
    }

    /**
     * 该包是否值得作为字典训练样本。
     *
     * <p>规则（均为实测结论）：</p>
     * <ul>
     *   <li>先跳过 varint 编码的包 id（最多 5 字节），剩下的才是载荷；</li>
     *   <li>载荷 ≤2 字节：太短，进字典只会占位；</li>
     *   <li>载荷恰好 8 字节：<b>绝大多数是 KeepAlive</b>——内容是一个随机 long，
     *       对字典只有污染没有贡献（这条是踩过坑补上的）。
     *       ⚠️ 但要清楚它是<b>按长度做的启发式</b>：其它恰好 8 字节载荷的包也会被一并排除，
     *       这是刻意的取舍（宁可少收几个样本，也不让随机数进字典）；</li>
     *   <li>最后要求达到 {@code pruneMinPayload} 门槛（默认 16）。</li>
     * </ul>
     *
     * <p>上面两条短载荷规则在默认门槛（16）下会被最后一条完全覆盖，属于冗余但无害；
     * 保留它们是因为用户可以把 {@code prune_min_payload} 调到很小，
     * 那时这两条就是唯一的拦截。</p>
     *
     * @param packetBytes    单个包（含包 id 前缀）
     * @param pruneMinPayload 采样时忽略载荷小于此值的包
     * @return 是否保留为样本
     */
    public static boolean shouldKeep(byte[] packetBytes, int pruneMinPayload) {
        return packetBytes != null && shouldKeep(packetBytes, 0, packetBytes.length, pruneMinPayload);
    }

    /**
     * 同一套判定，但直接在<b>整帧缓冲区</b>上按偏移检查，<b>不复制</b>。
     *
     * <p>⚠️ 存在的理由是可测量的开销：采样入口原先对<b>每一个</b>包都
     * {@code new byte[pktLen]} + {@code arraycopy}，然后才判断"要不要留"。
     * 而绝大多数包（KeepAlive、动作确认、短状态包）都会在这一步被丢掉——
     * 也就是说，网络线程上白白分配并复制了它们。判定只依赖长度与包 id 首字节，
     * 完全没必要先复制；只有真正入库的样本才需要独占数组。</p>
     *
     * @param data            整帧缓冲区
     * @param offset          该包在缓冲区中的起点
     * @param length          该包长度
     * @param pruneMinPayload 采样时忽略载荷小于此值的包
     */
    public static boolean shouldKeep(byte[] data, int offset, int length, int pruneMinPayload) {
        if (data == null || length <= 0 || offset < 0 || (long) offset + length > data.length) {
            return false;
        }
        int end = offset + length;
        int idLen = 0;
        int i = offset;
        while (i < end && idLen < 5) {
            byte b = data[i++];
            idLen++;
            if ((b & 0x80) == 0) {
                break;
            }
        }
        int payloadLen = length - idLen;
        if (payloadLen <= 2) {
            return false;
        }
        if (payloadLen == 8) {
            return false;
        }
        return payloadLen >= pruneMinPayload;
    }
}
