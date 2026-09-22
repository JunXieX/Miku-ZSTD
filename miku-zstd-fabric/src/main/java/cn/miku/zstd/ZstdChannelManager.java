package cn.miku.zstd;

import com.github.luben.zstd.ZstdCompressCtx;
import com.github.luben.zstd.ZstdDecompressCtx;
import io.netty.util.AttributeKey;
import mikumc.zstd.protocol.ZstdVarInts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 每连接上下文：zstd 压缩/解压上下文、字典加载、传输状态（协议 v2）。
 *
 * <p>帧格式 v2：客户端→服务端 {@code [rawSize][zstd(packet)|packet]}（prepender
 * 负责外层长度）；服务端→客户端 {@code [bodyLen][rawSize][zstd(packet)|packet]}
 * （bodyLen 由服务端编码器自带，已被 frame-decoder 剥离）。</p>
 *
 * <p>⚠️ 注意与 Velocity 端的语义差异——客户端的 "encoder" 字典用于<b>解压</b>
 * 服务端发来的数据，"decoder" 字典用于<b>压缩</b>发往服务端的数据。</p>
 *
 * <h2>3.1.0 变更</h2>
 * <ul>
 *   <li>字典对象改由 {@link ZstdDictRegistry} 全进程共享并按引用计数释放
 *       （旧实现每次登录都重建 3.2MB 的 {@code ZstdDictCompress} 且从不释放）；</li>
 *   <li>VarInt 收敛到 {@link ZstdVarInts}；</li>
 *   <li>删除从未被赋值的 {@code expectedDictId} 与 {@code finishConfigPending}
 *       ——它们支撑的 zstd:dict 推送链路服务端从未实现（整条链路已移除）。</li>
 * </ul>
 */
public class ZstdChannelManager {

    public static final AttributeKey<TransportState> ZSTD_STATE = AttributeKey.valueOf("zstd:state");
    public static final AttributeKey<ZstdChannelManager> KEY = AttributeKey.valueOf("zstd:manager");

    /** 协商协议版本（三端必须一致）。帧长度上限见 {@link ZstdVarInts#DEFAULT_MAX_VALUE}。 */
    public static final int PROTOCOL_VERSION = 4;

    private final ZstdCompressCtx compressCtx;
    private final ZstdDecompressCtx decompressCtx;

    /** 已取用的共享字典条目，连接关闭时归还引用 */
    private ZstdDictRegistry.Entry encoderDictEntry;
    private ZstdDictRegistry.Entry decoderDictEntry;

    public ZstdChannelManager() {
        ZstdConfig cfg = ZstdConfig.INSTANCE;
        this.compressCtx = new ZstdCompressCtx();
        this.compressCtx.setLevel(cfg.level);
        this.compressCtx.setWindowLog(cfg.windowLog);
        this.compressCtx.setLong(1); // LDM：提升区块级长距离重复的压缩率
        this.decompressCtx = new ZstdDecompressCtx();
        applySlimFrameFormat();
    }

    /**
     * 协议 v3 帧头精简：去掉双方都已知道的冗余字段。
     *
     * <ul>
     *   <li>{@code magicless}：省 4 字节 magic（对几十字节的小包占比可观）；</li>
     *   <li>{@code contentSize=false}：省 1~4 字节——解压尺寸由协议里的 rawSize 提供；</li>
     *   <li>{@code dictID=false}：省 1~4 字节——字典由双方显式加载。</li>
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
     * （换字典、重申帧头参数）都必须与池线程互斥——{@code ZstdCompressCtx} 不是线程安全的。
     * 编码器通过 {@code ZstdBatchEncoderBase#compressLock()} 取得同一把锁。</p>
     */
    private final Object compressLock = new Object();

    /** 供编码器使用：所有对 compressCtx 的访问都收敛到这一把锁上。 */
    public Object compressLock() {
        return compressLock;
    }

    public ZstdCompressCtx getCompressCtx() {
        return compressCtx;
    }

    /**
     * 压缩方向是否已装载字典。编码器据此选择"小包跳过压缩"的阈值：
     * 实测无字典时 48B 以下压缩反而膨胀，有字典时 24B 起才有收益（见 ZstdConfig）。
     */
    public boolean hasCompressDict() {
        return decoderDictEntry != null;
    }

    public ZstdDecompressCtx getDecompressCtx() {
        return decompressCtx;
    }

    /** 加载"服务端→客户端"方向的解压字典（客户端解压用）。 */
    public void loadEncoderDict(byte[] dictBytes, long dictId) {
        synchronized (compressLock) {
        ZstdDictRegistry.Entry e = ZstdDictRegistry.acquireDecompressDict(dictBytes, dictId);
        if (e == null) return;
        releaseQuietly(encoderDictEntry);
        encoderDictEntry = e;
        decompressCtx.loadDict(e.decompressDict());
        applySlimFrameFormat(); // loadDict 后幂等重申，避免参数被重置
        }
    }

    /** 加载"客户端→服务端"方向的压缩字典（客户端压缩用）。 */
    public void loadDecoderDict(byte[] dictBytes, long dictId) {
        synchronized (compressLock) {
        ZstdDictRegistry.Entry e = ZstdDictRegistry.acquireCompressDict(
                dictBytes, dictId, ZstdConfig.INSTANCE.level);
        if (e == null) return;
        releaseQuietly(decoderDictEntry);
        decoderDictEntry = e;
        compressCtx.loadDict(e.compressDict());
        applySlimFrameFormat(); // loadDict 后幂等重申，避免参数被重置
        }
    }

    private static void releaseQuietly(ZstdDictRegistry.Entry e) {
        if (e != null) e.release();
    }

    public void close() {
        this.compressCtx.close();
        this.decompressCtx.close();
        ZstdDictRegistry.Entry enc = encoderDictEntry;
        if (enc != null) {
            encoderDictEntry = null;
            enc.release();
        }
        ZstdDictRegistry.Entry dec = decoderDictEntry;
        if (dec != null) {
            decoderDictEntry = null;
            dec.release();
        }
    }

    public enum TransportState {
        PLAIN,
        NEGOTIATING,
        ZSTD_ACTIVE
    }
}
