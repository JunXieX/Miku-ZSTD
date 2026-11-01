package mikumc.zstd.protocol;

import io.netty.buffer.ByteBuf;

/**
 * 每连接的"最近历史"滑窗，用来现场构造 zstd 字典（协议 v7 / 帧格式 v6 引入）。
 *
 * <h2>为什么需要</h2>
 * <p>帧是<b>独立压缩</b>的（每个批次一个 zstd 帧、帧内不写 contentSize / dictID），
 * 这意味着<b>帧与帧之间零共享</b>：每帧都要重建一次熵表，上一帧发过的内容下一帧完全匹配不到。
 * 而 MC 的下行流量里恰好有大量<b>跨帧重复</b>——TAB 列表、计分板、实体位置模式、
 * 反复下发的同一片区块数据、Boss 条文字……旧实现靠"训练字典"近似这段历史，
 * 但训练字典是<b>静态快照</b>且只覆盖小包（实测按总字节加权只有 1~3%），
 * 跨大帧的重复一点也吃不到。</p>
 *
 * <p>本类把"最近 N KB 的原始字节"当作 raw-content 字典喂给 zstd（{@code loadDict(byte[])}），
 * 于是每一帧都能向前引用最多 {@link #DICT_BYTES} 字节的历史——等价于给独立帧装了一段
 * 滑动窗口。这是对"deflate 长连接单一流"那种做法的最小代价替代：仍然<b>帧独立、可校验、
 * 出错只废一帧</b>，不像流式压缩那样一错就整条连接报废。</p>
 *
 * <h2>两端为什么能各自算出同一份字典（关键约束）</h2>
 * <p>字典<b>不通过网络传输</b>：两端各自把"这条连接上该方向的原始字节流"喂进本类，
 * 得到的是<b>逐字节相同</b>的历史与代数。前提是三个"喂入点"必须严格对称：</p>
 * <ul>
 *   <li>压缩帧：发送端喂<b>整批原始负载</b>（{@code [varint pktLen][pkt]...}），
 *       接收端喂<b>解压出来的同一个 byte[]</b>；</li>
 *   <li>直存帧：发送端喂"该包连同它的 varint 长度前缀"，接收端用同样的构造补上前缀再喂
 *       （故 {@code [0][packet]} 与 {@code [varint len][packet]} 在这条链路上等价）；</li>
 *   <li>喂入与轮换都发生在<b>帧边界</b>，且一帧一次——所以"轮换点落在哪一帧之后"
 *       在两端必然一致。</li>
 * </ul>
 * <p>一旦这条对称被破坏（版本不一致、某一端没喂、某帧被跳过），后果不是"压缩率变差"而是
 * <b>解压出垃圾</b>。因此帧头里带 {@code dictGen}（见 {@link ZstdBatchEncoderBase} 的帧格式），
 * 接收端只要发现代数对不上就立刻按协议错乱关连接——把"静默错乱"变成一条可归因的日志。</p>
 *
 * <h2>为什么阈值是常量而不是配置项</h2>
 * <p>两端必须用<b>同一组</b>参数才能算出同一份字典。配置项在客户端与服务端之间没有同步机制，
 * 一旦运维只改了一边，结果就是上面那种"解压出垃圾"。所以这两个值是<b>线格式语义</b>，
 * 固定在本类里；将来要调只能随协议版本一起升。</p>
 *
 * <h2>内存与开销</h2>
 * <p>每方向常驻 {@link #DICT_BYTES}（滑窗）+ 同样大小的一份"当前生效字典"，即每方向 128KB、
 * 每连接双向 256KB。喂入是纯 memcpy；轮换每 {@link #ROTATE_AFTER_BYTES} 字节发生一次，
 * 摊到每字节约 {@code DICT_BYTES / ROTATE_AFTER_BYTES} 的额外拷贝与一次
 * {@code loadDict}（raw-content 只登记指针/拷贝，不做 CDict 预计算），实测可忽略。</p>
 *
 * <p>⚠️ <b>非线程安全</b>：发送端只在 event loop 上喂（成帧时）与读代数，
 * 接收端只在解码线程上喂与读，两者都是"本连接的那一条线程"。</p>
 */
public final class ZstdRollingDict {

    /**
     * 滑窗（= 字典）大小。
     *
     * <p>取 64KB：覆盖得住 MC 里典型的跨帧重复（TAB/计分板几 KB、实体批次几 KB、
     * 相邻区块的重复片段），同时每方向的常驻代价只有 128KB——比 {@code window_log: 20}
     * 那份 2MB 哈希表小一个数量级，也比"整条连接一个 zstd 流"便宜得多。</p>
     */
    public static final int DICT_BYTES = 64 * 1024;

    /**
     * 多少字节喂入后重建一次字典。
     *
     * <p>取 256KB：滑窗被填满后，每 256KB 才换一次字典 —— 换得太勤，{@code loadDict} 与
     * 字典生效初期的匹配率损失会吃掉收益；换得太懒，滑窗里早就全是"很久以前"的数据。
     * 256KB ≈ 200~2000 帧，对每条连接都是秒级，足够跟上内容变化。</p>
     */
    public static final int ROTATE_AFTER_BYTES = 256 * 1024;

    /** 滑窗本体，按时间顺序排列（{@code [0, fill)} 有效，不回绕）。 */
    private final byte[] ring = new byte[DICT_BYTES];

    /** 滑窗内有效字节数（0..{@link #DICT_BYTES}）。 */
    private int fill;

    /** 距上次重建已喂入多少字节。 */
    private int sinceRotate;

    /** 当前代数：0 = 还没轮换过（用协商阶段下发的静态字典），≥1 = 用本类算出的历史字典。 */
    private int generation;

    /**
     * 当前生效的历史字典（{@link #generation} ≥ 1 时非 null）。
     *
     * <p>与 {@link #ring} 分开保存：{@code ring} 会随新数据滑动，而已经交给 zstd 上下文的那份
     * 必须保持稳定，否则"正在用于压缩的字典"会在对方还在解压旧帧时被改写。</p>
     */
    private byte[] active;

    /** 当前代数（写进帧头的 {@code dictGen}）。 */
    public int generation() {
        return generation;
    }

    /** 当前生效的历史字典；{@code null} 表示还该用 gen 0 的静态字典（或完全没有字典）。 */
    public byte[] activeDict() {
        return active;
    }

    /**
     * 喂入一段原始字节（{@code ByteBuf} 视图，不改动其 readerIndex）。
     *
     * @param src   源缓冲
     * @param index 起始绝对下标
     * @param len   长度
     */
    public void feed(ByteBuf src, int index, int len) {
        if (len <= 0) return;
        if (len >= DICT_BYTES) {
            // 单次喂入就超过整个滑窗：只保留末尾 DICT_BYTES，省掉中间的搬移
            src.getBytes(index + len - DICT_BYTES, ring, 0, DICT_BYTES);
            fill = DICT_BYTES;
        } else {
            int overflow = fill + len - DICT_BYTES;
            if (overflow > 0) {
                System.arraycopy(ring, overflow, ring, 0, fill - overflow);
                fill -= overflow;
            }
            src.getBytes(index, ring, fill, len);
            fill += len;
        }
        advance(len);
    }

    /** 喂入一段原始字节（{@code byte[]} 视图）。 */
    public void feed(byte[] src, int off, int len) {
        if (len <= 0) return;
        if (len >= DICT_BYTES) {
            System.arraycopy(src, off + len - DICT_BYTES, ring, 0, DICT_BYTES);
            fill = DICT_BYTES;
        } else {
            int overflow = fill + len - DICT_BYTES;
            if (overflow > 0) {
                System.arraycopy(ring, overflow, ring, 0, fill - overflow);
                fill -= overflow;
            }
            System.arraycopy(src, off, ring, fill, len);
            fill += len;
        }
        advance(len);
    }

    private void advance(int len) {
        sinceRotate += len;
        // ⚠️ 两个条件都与"字节流"一一对应，才能真正保证两端同时轮换：
        //     · 滑窗必须已经填满（否则字典内容还取决于 fill，而不是完整的一段历史）；
        //     · 距上次轮换已满 ROTATE_AFTER_BYTES。
        // 只用 sinceRotate 判定是不行的：刚开始时滑窗还没满，两端也可能因为首批长度不同
        // （例如一端把整批一次性喂进来、另一端逐帧喂）而在"填充度"上产生分歧。
        if (fill < DICT_BYTES || sinceRotate < ROTATE_AFTER_BYTES) return;
        generation++;
        sinceRotate = 0;
        byte[] snapshot = new byte[DICT_BYTES];
        System.arraycopy(ring, 0, snapshot, 0, DICT_BYTES);
        active = snapshot;
    }

    /** 连接关闭 / 重新协商时清空（释放 64KB+64KB，避免挂机的旧连接白占内存）。 */
    public void reset() {
        fill = 0;
        sinceRotate = 0;
        generation = 0;
        active = null;
    }
}
