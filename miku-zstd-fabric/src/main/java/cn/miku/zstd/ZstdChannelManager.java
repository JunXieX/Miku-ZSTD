package cn.miku.zstd;

import com.github.luben.zstd.ZstdDecompressCtx;
import io.netty.util.AttributeKey;
import mikumc.zstd.protocol.ZstdCtxPool;

/**
 * 每连接的下行解压上下文与协商状态。
 *
 * <p>上行完全沿用原版包编码、压缩阈值、长度前缀与加密处理，不创建 zstd 压缩上下文。
 * 下行帧格式为 {@code [bodyLen][magicless Zstd 连续字节]}；外层长度由 splitter 剥离，
 * 内层为连续的 {@code [packetLen][packet]}，包与长度前缀均可跨传输帧。</p>
 *
 * <p>上下文的读取、协商重置与关闭均在该连接的 event loop 上串行执行。</p>
 */
public final class ZstdChannelManager {

    public static final AttributeKey<TransportState> ZSTD_STATE = AttributeKey.valueOf("zstd:state");
    public static final AttributeKey<ZstdChannelManager> KEY = AttributeKey.valueOf("zstd:manager");

    /** 协商协议版本（三端必须一致）；版本不匹配时回落原版。 */
    public static final int PROTOCOL_VERSION = mikumc.zstd.protocol.ZstdProtocolVersion.NEGOTIATE;

    private ZstdDecompressCtx decompressCtx;

    public ZstdChannelManager() {
        this.decompressCtx = borrowDecompressContext();
    }

    private static ZstdDecompressCtx borrowDecompressContext() {
        ZstdDecompressCtx context = ZstdCtxPool.borrowDecompress();
        try {
            context.setMagicless(true);
            return context;
        } catch (RuntimeException | Error failure) {
            ZstdCtxPool.returnDecompress(context);
            throw failure;
        }
    }

    /** 每帧重新读取，避免重放登录时继续使用已归还池的旧上下文。 */
    public ZstdDecompressCtx getDecompressCtx() {
        return decompressCtx;
    }

    /** 仅在重新协商时换一份干净的下行上下文，清除上一轮连续流状态。 */
    public void resetContexts() {
        if (decompressCtx == null) return;
        ZstdDecompressCtx replacement = borrowDecompressContext();
        ZstdDecompressCtx previous = decompressCtx;
        decompressCtx = replacement;
        ZstdCtxPool.returnDecompress(previous);
    }

    /** 幂等归还下行上下文；已关闭的管理器不再重新借用资源。 */
    public void close() {
        ZstdDecompressCtx previous = decompressCtx;
        decompressCtx = null;
        if (previous != null) ZstdCtxPool.returnDecompress(previous);
    }

    public enum TransportState {
        PLAIN,
        NEGOTIATING,
        ZSTD_ACTIVE
    }
}
