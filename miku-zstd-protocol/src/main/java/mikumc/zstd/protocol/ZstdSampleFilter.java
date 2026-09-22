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
     *   <li>载荷恰好 8 字节：<b>排除 KeepAlive</b>——它的内容是完全随机的 id，
     *       对字典只有污染没有贡献（这条是踩过坑补上的）；</li>
     *   <li>最后要求达到 {@code pruneMinPayload} 门槛（默认 16）。</li>
     * </ul>
     *
     * @param packetBytes    单个包（含包 id 前缀）
     * @param pruneMinPayload 采样时忽略载荷小于此值的包
     * @return 是否保留为样本
     */
    public static boolean shouldKeep(byte[] packetBytes, int pruneMinPayload) {
        if (packetBytes == null || packetBytes.length == 0) {
            return false;
        }
        int idLen = 0;
        while (idLen < packetBytes.length && idLen < 5) {
            byte b = packetBytes[idLen];
            idLen++;
            if ((b & 0x80) == 0) {
                break;
            }
        }
        int payloadLen = packetBytes.length - idLen;
        if (payloadLen <= 2) {
            return false;
        }
        if (payloadLen == 8) {
            return false;
        }
        return payloadLen >= pruneMinPayload;
    }
}
