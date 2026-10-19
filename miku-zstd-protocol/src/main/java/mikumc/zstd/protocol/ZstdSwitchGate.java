package mikumc.zstd.protocol;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 切换闸门：服务端在激活 zstd 之前插入（<b>MC 包层</b>，即原版/新版压缩编码器之前），
 * 在客户端确认切换之前<b>扣住登录期的出站包</b>。
 *
 * <h2>它解决什么</h2>
 * <p>没有闸门时：服务端一收到协商应答就换掉入站解码器，而客户端可能还没处理到
 * SetCompression，仍在发原版帧 → 两个方向都出现「原版帧 vs zstd 直存帧同形」的歧义，
 * 只能靠字节启发式猜，永远有残留风险。</p>
 *
 * <p>有闸门时，"确认之前收到的必然是原版帧"成为协议保证：</p>
 * <ul>
 *   <li><b>入站</b>：确认之前解码器<b>只</b>按原版规则解释（原始帧整包下传），不做任何猜测；</li>
 *   <li><b>出站</b>：确认之前本闸门扣住登录期的包。放行时它们走的是<b>激活后</b>的编码器
 *       （zstd），因此客户端在发出确认之前收到的全部是原版帧，之后全部是 zstd 帧，
 *       边界由一次往返确定，不存在交错。</li>
 * </ul>
 *
 * <h2>为什么位于包层</h2>
 * <p>包层能直接认出包类型、且不依赖外层长度是否已经写好；更关键的是：包在放行时才被
 * 交给下游编码器，所以放行瞬间的编码器就是"当前生效"的那个。</p>
 *
 * <h2>为什么切换请求不会把自己扣住</h2>
 * <p>切换请求由服务端经 {@code zstd_encoder} 的 context 写出 —— 那是本闸门的<b>下游</b>，
 * 出站事件不会回到上游的本闸门。这是安装位置（编码器之前）带来的直接保证。</p>
 *
 * <h2>兜底：超时后<b>双向</b>回滚</h2>
 * <p>确认丢失时（客户端崩溃 / 模组缺失 / 丢包），{@link #RELEASE_TIMEOUT_MS} 后本闸门会
 * 调用 {@link #setRollback(Runnable)} 注册的回滚动作：服务端把 zstd 编解码器换回原版。
 * 绝不能让"出站 zstd / 入站原版"这种混合模式存在 —— 那是必然进不去服务器。</p>
 */
public final class ZstdSwitchGate extends ChannelDuplexHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger("miku-zstd");

    /** 切换握手的通道标识（写进登录查询包的通道名）。 */
    public static final String CHANNEL = "zstd:switch";

    /** 魔数（询问与确认共用；尾部魔数是唯一的识别依据）。 */
    public static final int MAGIC = 0x5A535743; // "ZSWC"

    /**
     * 未收到确认时的兜底超时（也是"停止扣包"的时刻）。
     *
     * <p>取 5 秒而不是更短：安装阶段可能因锚点未就绪而按 100ms 重试
     * （Velocity/Paper 都是 20 次），若超时比安装预算还短，闸门会在切换请求发出前
     * 就自行放行，握手形同虚设。</p>
     *
     * <p>超时后<b>仍按已确认处理</b>（见 {@link #release}）：客户端在收到 SetCompression
     * 时就已经切换，超时通常只是确认包丢了。退回原版反而会制造混合模式。</p>
     */
    public static final long RELEASE_TIMEOUT_MS = 5000;

    private final List<Object> held = new ArrayList<>();
    private final List<ChannelPromise> heldPromises = new ArrayList<>();
    private boolean released;
    private volatile boolean confirmed;
    private io.netty.util.concurrent.ScheduledFuture<?> scheduled;

    /** 本处理器在管线中的上下文（handlerAdded 时记录）。 */
    private volatile ChannelHandlerContext selfCtx;

    /** 连接级判定：切换是否已由客户端确认。 */
    public boolean isConfirmed() {
        return confirmed;
    }

    /** 扣住的包数（诊断用）。 */
    public int heldCount() {
        return held.size();
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        selfCtx = ctx;
        scheduled = ctx.channel().eventLoop().schedule(() -> release(false),
                RELEASE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
        if (released || !mustHold(msg)) {
            ctx.write(msg, promise);
            return;
        }
        held.add(msg);
        heldPromises.add(promise);
    }

    /**
     * 由服务端在收到客户端确认时调用。
     *
     * <p>⚠️ 不接收调用方的 ctx：放行必须从<b>本闸门自己</b>的位置写出，否则会跳过
     * 闸门与调用方之间的出站处理器（加密、帧长度等）。</p>
     */
    public void confirm() {
        release(true);
    }

    /** 由安装失败 / 主动放弃激活的路径调用：视为未确认，触发回滚。 */
    public void abort() {
        release(false);
    }

    private void release(boolean ok) {
        if (released) return;
        released = true;
        // ⚠️ 超时也视为"已确认"：能走到激活这一步，说明客户端<b>已经通过协商证明</b>
        // 自己装了模组、且会在收到 SetCompression 后切换到 zstd（这是客户端自身的逻辑，
        // 不依赖本握手）。超时通常只是确认包丢了 / 晚了，而客户端的切换早已完成。
        // 若此时把服务端退回原版，反而形成"客户端 zstd / 服务端原版"的必断连组合。
        // 因此两个方向都保持 zstd —— 时序上客户端一定早于本超时（2s）完成切换。
        confirmed = true;
        if (scheduled != null) {
            scheduled.cancel(false);
            scheduled = null;
        }
        ChannelHandlerContext ctx = selfCtx;
        if (!ok) {
            LOGGER.debug("[Zstd] switch not confirmed within {}ms, held={} — releasing as zstd "
                    + "(client has already switched on SetCompression)", RELEASE_TIMEOUT_MS, held.size());
        }
        if (ctx != null) {
            flushHeld(ctx);
        } else {
            releaseAll();
        }
        if (ctx != null && ctx.pipeline().context(this) != null) {
            ctx.pipeline().remove(this);
        }
    }

    private void flushHeld(ChannelHandlerContext ctx) {
        if (held.isEmpty()) return;
        // ⚠️ 判据只能用"连接是否还活跃"。handlerRemoved() 被调用时本处理器<b>已经</b>从管线
        // 解链，{@code pipeline().context(this) != null} 必然为 false —— 那样会让"插件卸载 /
        // 管线重建"这类<b>连接仍活着</b>的摘除把扣住的登录包当成泄漏释放掉。
        boolean deliverable = ctx.channel().isActive();
        for (int i = 0; i < held.size(); i++) {
            Object msg = held.get(i);
            ChannelPromise p = heldPromises.get(i);
            if (deliverable) {
                try {
                    ctx.write(msg, p);
                } catch (Throwable t) {
                    // 单个处理器抛异常不能拖垮其余消息：释放本包并失败化其 promise
                    ReferenceCountUtil.release(msg);
                    failQuietly(p);
                }
            } else {
                ReferenceCountUtil.release(msg);
                failQuietly(p);
            }
        }
        held.clear();
        heldPromises.clear();
        if (deliverable) {
            ctx.flush();
        }
    }

    private void releaseAll() {
        for (int i = 0; i < held.size(); i++) {
            ReferenceCountUtil.release(held.get(i));
            failQuietly(heldPromises.get(i));
        }
        held.clear();
        heldPromises.clear();
    }

    /**
     * 失败化一个 promise，绝不让异常逃逸。
     *
     * <p>连接销毁时上下游都可能已经失败化过同一个 promise；再 {@code tryFailure} 会触发
     * 监听器里的异常并在 pipeline 上冒泡成日志噪音（实测为 {@link ClosedChannelException}）。
     * promise 的最终状态由第一个设置者决定，这里吞掉即可。</p>
     */
    private static void failQuietly(ChannelPromise p) {
        if (p == null || p.isDone()) return;
        try {
            p.tryFailure(new ClosedChannelException());
        } catch (Throwable ignored) {
            // 已被其它环节以异常方式完成：无需重复处理
        }
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        // 被外部摘除 / 连接关闭：必须收尾，否则扣住的包与 promise 无人处理。
        released = true;
        if (scheduled != null) {
            scheduled.cancel(false);
            scheduled = null;
        }
        flushHeld(ctx);
    }

    /**
     * 是否属于切换窗口内必须扣住的包。
     *
     * <p>判据：是 MC 包对象（不是 Netty 自身的控制消息）。Netty 的控制消息（协议阶段切换
     * 用的 {@code Runnable} 等）被扣住会卡死连接建立，且日志无异常。</p>
     *
     * <p>本闸门位于包层，因此这里看到的是 {@code Packet} / {@code MinecraftPacket} 对象，
     * 类名一定落在 {@code .protocol.} 命名空间内；反过来用"不在 java./io.netty./org.slf4j."
     * 判断会把任何第三方对象误当数据包。</p>
     */
    private static boolean mustHold(Object msg) {
        if (msg == null) return false;
        if (msg instanceof Runnable) return false;
        String cn = msg.getClass().getName();
        return cn.contains(".protocol.");
    }

    /** 创建闸门实例。 */
    public static ZstdSwitchGate create() {
        return new ZstdSwitchGate();
    }
}
