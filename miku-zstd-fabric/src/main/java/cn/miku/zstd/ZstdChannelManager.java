package cn.miku.zstd;

import com.github.luben.zstd.ZstdCompressCtx;
import com.github.luben.zstd.ZstdDecompressCtx;
import mikumc.zstd.protocol.ZstdCtxPool;
import io.netty.util.AttributeKey;
import mikumc.zstd.protocol.ZstdCompressLock;

/**
 * 每连接上下文：zstd 压缩/解压上下文与传输状态。
 *
 * <p><b>协商版本 v9</b>（{@link #PROTOCOL_VERSION}），<b>帧格式 v7</b>：
 * 客户端→服务端 {@code [0][count][pktLen][packet]...}（存储批帧，<b>不压缩</b>，外层长度由
 * prepender 补）；服务端→客户端 {@code [bodyLen][rawSize][zstd(payload)|packet]}
 * （bodyLen 由服务端编码器自带，已被 frame-decoder 剥离）。{@code rawSize == 0} 表示直存：
 * 帧体恰好是一个包（{@code [0][packet]}），与原版未压缩帧逐字节同形。</p>
 *
 * <p>5.0.42 起训练字典整套删除：不再有协商下发、本地缓存、上下文装载。下行的"字典"只剩
 * 滚动历史——两端各自从这条连接的原始字节流现算，每帧向前引用最近 64KB，<b>不经过网络</b>
 * （见 {@code mikumc.zstd.protocol.ZstdRollingDict}）。</p>
 *
 * <p>5.0.43 起<b>上行彻底不压缩</b>（实测压了反而膨胀 13%），只批处理，因此本类的
 * {@code compressCtx} 对客户端已经没有任何压缩职责，只作为"编码器愿意攒批"的占位存在
 * （见字段注释）。</p>
 */
public class ZstdChannelManager {

    public static final AttributeKey<TransportState> ZSTD_STATE = AttributeKey.valueOf("zstd:state");
    public static final AttributeKey<ZstdChannelManager> KEY = AttributeKey.valueOf("zstd:manager");

    /** 协商协议版本（三端必须一致）。版本不一致时本端会干净回落原版，不会出现半 zstd。 */
    public static final int PROTOCOL_VERSION = mikumc.zstd.protocol.ZstdProtocolVersion.NEGOTIATE;

    /**
     * 压缩上下文。<b>上行已不压缩（5.0.43），此 ctx 仅剩占位</b>。
     *
     * <p>⚠️ 为什么还不删：编码器的 {@code write()} 用"有没有 ctx"来决定"要不要攒批"
     * （{@code compressCtx == null} 意味着整条通道透传），删掉它会把上行唯一保留的优化
     * ——批处理——一起关掉。它的 {@code level} / {@code window_log} 参数因此也不再影响任何
     * 字节，只是构造成本。等基类把"是否攒批"与"是否压缩"彻底拆成两个判定，这里就可以删。</p>
     *
     * <p>⚠️ 非 final：{@link #resetContexts()} 用"换一个新实例"来丢掉旧上下文里绑定的
     * 滚动历史字典（zstd 没有卸载接口）。所有读写都必须在 {@code compressLock} 内完成 ——
     * 池线程通过该锁取用，因此替换上下文与压缩执行互斥，不会拿到已被归还池的实例。</p>
     */
    private ZstdCompressCtx compressCtx;
    private ZstdDecompressCtx decompressCtx;

    public ZstdChannelManager() {
        ZstdConfig cfg = ZstdConfig.INSTANCE;
        // 上下文从租借池取（池内实例已 reset，参数需重设）
        // ⚠️ 上行不压缩后下面三行只是"借一份占位"：参数设置没有实际效果，但仍是编码器进入
        // 攒批路径的前提（见 compressCtx 字段注释）。保留是为了让恢复上行压缩只改一个返回值。
        this.compressCtx = ZstdCtxPool.borrowCompress(cfg.level);
        this.compressCtx.setWindowLog(cfg.windowLog);
        this.compressCtx.setLong(1); // LDM：提升区块级长距离重复的压缩率
        this.decompressCtx = ZstdCtxPool.borrowDecompress();
        applySlimFrameFormat();
    }

    /**
     * 帧格式 v3 的帧头精简：去掉双方都已知道的冗余字段。
     *
     * <ul>
     *   <li>{@code magicless}：省 4 字节 magic（对几十字节的小包占比可观）；</li>
     *   <li>{@code contentSize=false}：省 1~4 字节——解压尺寸由协议里的 rawSize 提供；</li>
     *   <li>{@code dictID=false}：省 1~4 字节——字典是两端各自算出的滚动历史，帧里不用带 id。</li>
     * </ul>
     *
     * <p>三者都只是"不再重复写入双方已知的信息"，语义不变。实测（每包一帧）可再省约 10.7%。
     * 解压端必须设置相同的 magicless 开关。</p>
     */
    private void applySlimFrameFormat() {
        compressCtx.setMagicless(true);
        compressCtx.setContentSize(false);
        compressCtx.setDictID(false);
        decompressCtx.setMagicless(true);
    }

    /**
     * 压缩上下文的互斥锁。
     *
     * <p>压缩已卸载到线程池执行，因此任何改写 {@link #compressCtx} 的操作
     * （重新协商换上下文、重申帧头参数、归还池）都必须与池线程互斥——
     * {@code ZstdCompressCtx} 不是线程安全的。编码器通过
     * {@code ZstdBatchEncoderBase#compressLock()} 取得同一把锁。</p>
     *
     * <p>⚠️ 类型是 {@link ZstdCompressLock} 而不是 {@code Object}：它同时承载"上下文已关闭"
     * 标志，池线程在锁内读它来避免 use-after-free（详见该类注释）。</p>
     */
    private final ZstdCompressLock compressLock = new ZstdCompressLock();

    /** 供编码器使用：所有对 compressCtx 的访问都收敛到这一把锁上。 */
    public ZstdCompressLock compressLock() {
        return compressLock;
    }

    /**
     * 当前压缩上下文。<b>调用方必须已持有 {@link #compressLock()}</b>：
     * {@link #resetContexts()} 会在锁内替换实例，锁外读取可能拿到已被归还池的旧实例。
     */
    public ZstdCompressCtx getCompressCtx() {
        return compressCtx;
    }

    /**
     * 当前解压上下文。解压只在 event loop 上发生，而 {@link #resetContexts()} 也只在
     * event loop（协商回调）上调用，两者天然串行。
     */
    public ZstdDecompressCtx getDecompressCtx() {
        return decompressCtx;
    }

    /**
     * 换一份干净的压缩/解压上下文（旧的归还池，池内 reset 会解绑字典）。
     *
     * <p>⚠️ 为什么每一轮协商都要换：{@code ZstdChannelManager} 的生命周期挂在 <b>Channel</b> 上，
     * 而同一条 Channel 可能被重放登录流程（ViaFabricPlus 协议切换、或重连到另一台服务器）。
     * 滚动历史算出的字典是<b>装载进上下文</b>的，而 zstd 没有卸载接口——上一轮那份历史会一直
     * 留在上下文里，新一轮的滑窗却从零开始。两边的 {@code dictGen} 与字典内容于是当场错位，
     * 表现不是"压缩率变差"而是<b>解压出垃圾</b>。</p>
     *
     * <p>⚠️ 与压缩任务共用同一把锁：压缩跑在池线程上，若这里不持锁就把上下文还回池，
     * 在途压缩会继续写同一份原生内存（那份内存随即被别的连接借走）——数据被写坏，
     * 极端情况下是 SIGSEGV。解压侧只在 event loop 上跑，天然与本方法串行。</p>
     */
    public void resetContexts() {
        synchronized (compressLock) {
            if (compressLock.isClosed()) return;
            // 换一份干净上下文（旧的归还池，池内 reset 会清掉字典绑定）
            ZstdCtxPool.returnCompress(compressCtx);
            ZstdCtxPool.returnDecompress(decompressCtx);
            ZstdConfig cfg = ZstdConfig.INSTANCE;
            compressCtx = ZstdCtxPool.borrowCompress(cfg.level);
            compressCtx.setWindowLog(cfg.windowLog);
            compressCtx.setLong(1);
            decompressCtx = ZstdCtxPool.borrowDecompress();
            applySlimFrameFormat();
        }
    }

    public void close() {
        // ⚠️ 必须与池线程拿的是**同一把锁**：压缩在 ZstdCompressPool 的线程上执行，
        // 若在途压缩还没回来就把上下文还回池，池线程会继续写同一份内存（那份随即被别人借走）
        // → 数据错乱，极端情况下 SIGSEGV。
        // ⚠️ 且必须**先立标志再还上下文**：标志与归还在同一段持锁区内，池线程在锁内读到
        // 标志即可安全放弃；顺序反了（先还后立标志）等于没防（详见 ZstdCompressLock 注释）。
        synchronized (compressLock) {
            compressLock.markClosed();
            // 归还池（内部 reset 清字典）；池满/已停用时才真正 close
            ZstdCtxPool.returnCompress(this.compressCtx);
            ZstdCtxPool.returnDecompress(this.decompressCtx);
        }
    }

    public enum TransportState {
        PLAIN,
        NEGOTIATING,
        ZSTD_ACTIVE
    }
}
