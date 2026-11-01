package mikumc.zstd.protocol;

import com.github.luben.zstd.ZstdCompressCtx;
import com.github.luben.zstd.ZstdDecompressCtx;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * 原生 zstd 上下文的租借池（三端共享）。
 *
 * <h2>为什么需要</h2>
 * <p>每个 {@link ZstdCompressCtx} 在 level 3 下约占 650KiB 原生内存，level 越高越大；
 * 旧实现是「每连接两份」（压缩 + 解压），连接建立时 {@code new}、关闭时 {@code close}。
 * 开服 / 重启的登录风暴会在几秒内集中 malloc/free 几十到上百份原生上下文，
 * 表现为 RSS 尖峰与分配抖动（GC 管不到原生内存，只能靠 here 复用）。</p>
 *
 * <h2>归还为什么必须 reset</h2>
 * <p>zstd-jni 的 {@code reset()} 语义是「清空全部状态与参数，等同于刚创建的上下文」，
 * 并且会释放已绑定的字典引用（见其源码：{@code reset0} 成功后把 {@code compression_dict}
 * 置 null 并解锁）。这条保证是池化安全的前提——否则上一条连接的字典会被带到下一条，
 * 而字典 id 与内容对不上时对端会直接解压失败。</p>
 *
 * <h2>上限</h2>
 * <p>缓存 {@code max(4, 2×核数)} 份。超过上限的归还直接 close：池的目的是摊平
 * 「短时间大量连接」的分配抖动，不是无限缓存。</p>
 */
public final class ZstdCtxPool {

    /** 每个方向最多缓存多少份（对齐成熟实现的 2×核数口径）。 */
    private static final int LIMIT = Math.max(4, Runtime.getRuntime().availableProcessors() * 2);

    private static final Deque<ZstdCompressCtx> COMPRESS = new ArrayDeque<>();
    private static final Deque<ZstdDecompressCtx> DECOMPRESS = new ArrayDeque<>();

    private static volatile boolean closed;

    private ZstdCtxPool() {
    }

    /**
     * 借一个压缩上下文。
     *
     * <p>⚠️ 等级必须<b>每次</b>设置：{@link #returnCompress} 里的 {@code reset()} 会把
     * 参数一起清回默认值（见类注释），池命中时若不重设，配置里的 level 就只在"池恰好为空"
     * 的头几条连接上生效——之后全部按 zstd 默认的 3 跑，改大改小都看不出来。</p>
     */
    public static ZstdCompressCtx borrowCompress(int level) {
        ZstdCompressCtx c;
        synchronized (COMPRESS) {
            c = COMPRESS.pollFirst();
        }
        if (c == null) {
            c = new ZstdCompressCtx();
        }
        return c.setLevel(level);
    }

    /** 归还压缩上下文：先 reset，再入池；池满或已停用则 close。 */
    public static void returnCompress(ZstdCompressCtx ctx) {
        if (ctx == null) {
            return;
        }
        if (closed) {
            ctx.close();
            return;
        }
        try {
            ctx.reset();
        } catch (Throwable t) {
            // reset 失败说明上下文已不可用（原生状态异常）：只能丢弃
            ctx.close();
            return;
        }
        synchronized (COMPRESS) {
            if (COMPRESS.size() < LIMIT) {
                COMPRESS.addFirst(ctx);
                return;
            }
        }
        ctx.close();
    }

    /** 借一个解压上下文（池空则新建）。 */
    public static ZstdDecompressCtx borrowDecompress() {
        synchronized (DECOMPRESS) {
            ZstdDecompressCtx pooled = DECOMPRESS.pollFirst();
            if (pooled != null) {
                return pooled;
            }
        }
        return new ZstdDecompressCtx();
    }

    /** 归还解压上下文（同构：先 reset 再入池）。 */
    public static void returnDecompress(ZstdDecompressCtx ctx) {
        if (ctx == null) {
            return;
        }
        if (closed) {
            ctx.close();
            return;
        }
        try {
            ctx.reset();
        } catch (Throwable t) {
            ctx.close();
            return;
        }
        synchronized (DECOMPRESS) {
            if (DECOMPRESS.size() < LIMIT) {
                DECOMPRESS.addFirst(ctx);
                return;
            }
        }
        ctx.close();
    }

    /** 停服 / 关代理：清空池并关闭其中的上下文。 */
    public static void shutdown() {
        closed = true;
        synchronized (COMPRESS) {
            for (ZstdCompressCtx ctx : COMPRESS) {
                try {
                    ctx.close();
                } catch (Throwable ignored) {
                    // 关闭失败无需上报：进程退出路径
                }
            }
            COMPRESS.clear();
        }
        synchronized (DECOMPRESS) {
            for (ZstdDecompressCtx ctx : DECOMPRESS) {
                try {
                    ctx.close();
                } catch (Throwable ignored) {
                    // 同上
                }
            }
            DECOMPRESS.clear();
        }
    }
}
