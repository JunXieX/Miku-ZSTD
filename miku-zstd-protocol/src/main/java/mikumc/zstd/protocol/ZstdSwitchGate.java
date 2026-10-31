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
 * 切换闸门：服务端在激活 zstd 之前插入（<b>MC 包层</b>，在客户端确认切换之前
 * <b>扣住登录期的出站包</b>）。
 *
 * <h2>⚠️ 安装位置必须落在"包层编码器之后、压缩编码器之前"</h2>
 * <p>Netty 出站事件<b>从 tail 走向 head</b>，所以"闸门能看见 MC 包对象"的充要条件是：
 * 闸门在管线里的位置比<b>把 Packet 变成 ByteBuf 的那个处理器</b>（Velocity 的
 * {@code minecraft-encoder} / Paper 的 {@code encoder}）更靠 tail。</p>
 *
 * <p>⚠️ 不能拿<b>压缩编码器</b>当锚点：它消费的是 ByteBuf，而且在管线里位于包层编码器的
 * <b>head 侧</b>（例如 Paper/原版用 {@code addBefore("encoder", "compress")} 插入它）。
 * 把闸门插在压缩编码器旁边，出站仍会先经过包层编码器 —— 消息已是 ByteBuf，
 * {@link #mustHold} 对其返回 {@code false}，闸门将<b>一个包都扣不住</b>，
 * 本类承诺的协议保证会静默失效（客户端还没切换就收到 zstd 压缩帧 → 断连）。</p>
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
 * <h2>兜底：任何"停止扣包"的理由都按<b>已提交</b>处理</h2>
 * <p>确认到达、超时（{@link #RELEASE_TIMEOUT_MS}）、客户端回了一条<b>不带魔数</b>的应答、
 * 或本处理器被外部摘除 —— 这四种情形本闸门都只是"把扣住的包按当前管线放行"，并置位
 * {@link #isConfirmed()}。理由：</p>
 * <ul>
 *   <li>服务端在<b>激活那一刻</b>就已经把编解码器换成了 zstd。客户端则在收到
 *       SetCompression 时也切换了（客户端与 SetCompression 的处理顺序决定），
 *       因此"退回原版"既<b>做不到</b>，也会制造"出站 zstd / 入站原版"的必断连组合；</li>
 *   <li><b>"退回原版"做不到</b>：原版压缩处理器实例在激活时已被 {@code pipeline.replace}
 *       移除过一次，而 Netty 对非 {@code @Sharable} handler 的 {@code added} 标志
 *       <b>永不重置</b>（{@code DefaultChannelPipeline#checkMultiplicity} 只置真、从不清零），
 *       把同一个实例重新放回管线必然抛 {@code ChannelPipelineException}。所以本闸门
 *       不提供任何"回滚"动作；</li>
 *   <li>客户端回的那条"不带魔数"的应答（{@code [0x02][varint txId][0]}）<b>无法区分</b>
 *       "装了模组但没切换"与"旧版客户端（没有切换嗅探器，收到询问后按原版规则回了一个
 *       空应答）"。而旧版客户端<b>其实是 zstd</b>（它同样在 SetCompression 时切换了），
 *       把它当成拒绝去关连接等于把正常玩家挡在门外。所以这条应答只作"未确认"记录，
 *       动作与超时一致：放行并提交。</li>
 * </ul>
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
     * 时就已经切换，超时通常只是确认包丢了。退回原版反而会制造混合模式，而且原版处理器
     * 实例已无法重新插入管线（见类注释）。</p>
     */
    public static final long RELEASE_TIMEOUT_MS = 5000;

    /** "客户端未确认切换"告警的只报一次标志（多发于旧版模组客户端，逐连接重复无价值）。 */
    private static final java.util.concurrent.atomic.AtomicBoolean UNCONFIRMED_WARNED =
            new java.util.concurrent.atomic.AtomicBoolean();

    private final List<Object> held = new ArrayList<>();
    private final List<ChannelPromise> heldPromises = new ArrayList<>();

    /**
     * 保护 {@link #held} / {@link #heldPromises} 与 {@link #released}。
     *
     * <p>⚠️ 扣包发生在<b>写包线程</b>（Paper 主线程；Folia 上是 global region 或区域线程），
     * 而释放发生在 netty eventLoop（确认嗅探器、超时任务）或管线重建线程。
     * 没有这把锁时，释放瞬间仍在 {@code add} 的包会掉出快照 —— 既没被发出、也没被释放，
     * 客户端因此缺包；缺的是登录期关键包时，表现就是「激活日志之后连接直接断开」。</p>
     */
    private final Object heldLock = new Object();
    private volatile boolean released;
    private volatile boolean confirmed;
    private io.netty.util.concurrent.ScheduledFuture<?> scheduled;

    /**
     * 收到"未确认"应答后的延迟放行任务。
     *
     * <p>⚠️ 不能收到就放行：MC 客户端对<b>未注册 channel</b> 的 {@code LoginPluginRequest}
     * 会<b>自动</b>回一条 {@code success=false} 的空应答，而我们的帧层嗅探器随后还会回一条
     * 带魔数的真确认（切换在客户端 event loop 上完成）。若拿到前一条就放行，客户端可能
     * 还没切完，首个 zstd 帧就解不开 —— 这是 5.0.6 实机掉线的直接成因。</p>
     */
    private io.netty.util.concurrent.ScheduledFuture<?> delayedRelease;

    /** 本处理器在管线中的上下文（handlerAdded 时记录）。 */
    private volatile ChannelHandlerContext selfCtx;

    /** 本次询问的 txId（0 表示不校验）。 */
    private volatile int expectedTxId;

    /** 连接级判定：切换窗口是否已结束（结束即视为"服务端已提交到 zstd"）。 */
    public boolean isConfirmed() {
        return confirmed;
    }

    /** 记录本次询问的 txId（服务端在发送请求时调用）。 */
    public void expectTxId(int txId) {
        this.expectedTxId = txId;
    }

    /**
     * 校验并接受客户端确认。
     *
     * @return 是否接受（txId 不符时拒绝，避免任何"尾部恰好是魔数"的帧误放行）
     */
    public boolean confirm(int txId) {
        int expected = expectedTxId;
        if (expected != 0 && txId != expected) {
            LOGGER.debug("[Zstd] switch confirmation txId mismatch: got={}, expected={}", txId, expected);
            return false;
        }
        release();
        return true;
    }

    /**
     * 客户端回了一条<b>不带魔数</b>的应答：它没能（或不会）确认切换。
     *
     * <p>⚠️ 这条应答<b>不能</b>当"客户端拒绝 zstd"来处理 —— 旧版客户端在收到切换询问后
     * 会按原版规则回一个空应答，而它其实是 zstd（同样在 SetCompression 时切换了）。
     * 若据此关闭连接，正常玩家就会被挡在门外。见类注释。</p>
     *
     * @return 是否接受（txId 不符时忽略，避免误放行）
     */
    public boolean releaseUnconfirmed(int txId) {
        int expected = expectedTxId;
        if (expected != 0 && txId != expected) {
            LOGGER.debug("[Zstd] switch answer txId mismatch: got={}, expected={}", txId, expected);
            return false;
        }
        LOGGER.debug("[Zstd] switch not confirmed by client (txId={}) — releasing held packets", txId);
        // ⚠️ 这是<b>正常兼容路径</b>而非异常：旧版客户端没有切换嗅探器，收到询问后会按原版
        // 规则回一个空应答，而它同样在 SetCompression 时切到了 zstd —— 服务端照常放行即可。
        // 以前这条打 WARN，运维看到会以为出了问题（实测日志里就是这么被误读的），现降为 INFO。
        if (UNCONFIRMED_WARNED.compareAndSet(false, true)) {
            LOGGER.info("[Zstd] 客户端未回切换确认（不带魔数的应答）——旧版模组的正常行为，"
                    + "服务端已放行扣住的包；本条只提示一次");
        }
        // ⚠️ 延迟放行，不等这个空应答立刻放开：新客户端（延迟切换 / 或嗅探器还没跑完）
        // 会在随后回一条带魔数的真确认；200ms 足够它到达，而旧客户端只多等 200ms。
        // 真确认到达时 release() 会取消这个延迟任务并立即放行。
        ChannelHandlerContext gateCtx = selfCtx;
        if (gateCtx != null) {
            if (delayedRelease == null) {
                delayedRelease = gateCtx.executor().schedule(this::release, 200,
                        TimeUnit.MILLISECONDS);
            }
        } else {
            release();
        }
        return true;
    }

    /** 扣住的包数（诊断用）。 */
    public int heldCount() {
        synchronized (heldLock) {
            return held.size();
        }
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        selfCtx = ctx;
        // ⚠️ 超时按"已确认"处理：服务端在激活时就已经换成了 zstd 编码器，而客户端收到
        // SetCompression 时必然也切换了（TCP 顺序），此时退回原版既做不到，也会制造
        //"出站 zstd / 入站原版"的必断连组合。见 release / 类注释。
        scheduled = ctx.channel().eventLoop().schedule(this::release,
                RELEASE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
        // ⚠️ 扣包与放行必须全部串行到 event loop：release() 在 event loop 上把扣住的包
        // 逐条写出，而写包线程（Folia 上是区域线程）若在 released 置位后就地放行，
        // 新包可能插到那批旧包之前 —— 登录期包顺序被打乱，客户端包解码器直接错位。
        // 投递一次 event loop 任务换来的是"扣包序列与放行序列天然有序"。
        if (!ctx.executor().inEventLoop()) {
            ctx.executor().execute(() -> enqueueOrPass(ctx, msg, promise));
            return;
        }
        enqueueOrPass(ctx, msg, promise);
    }

    private void enqueueOrPass(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
        if (!ctx.channel().isActive()) {
            ReferenceCountUtil.release(msg);
            promise.tryFailure(new ClosedChannelException());
            return;
        }
        boolean pass;
        synchronized (heldLock) {
            pass = released || !mustHold(msg);
            if (!pass) {
                held.add(msg);
                heldPromises.add(promise);
            }
        }
        if (pass) {
            ctx.write(msg, promise);
        }
    }

    /**
     * 由服务端在收到客户端确认时调用（不校验 txId，仅供内部/测试使用）。
     *
     * <p>⚠️ 不接收调用方的 ctx：放行必须从<b>本闸门自己</b>的位置写出，否则会跳过
     * 闸门与调用方之间的出站处理器（加密、帧长度等）。</p>
     */
    public void confirm() {
        release();
    }

    /**
     * 停止扣包：把扣住的包从<b>本闸门自己的位置</b>按当前（已经是 zstd 的）管道放行，
     * 置位 {@link #isConfirmed()}，并自移除。
     *
     * <p>本方法是<b>唯一</b>的释放路径 —— 确认、超时、未确认应答、外部摘除都走它，
     * 因为"服务端已提交到 zstd"是这四种情形的共同结论（详见类注释）。</p>
     */
    private void release() {
        List<Object> heldSnapshot;
        List<ChannelPromise> promiseSnapshot;
        ChannelHandlerContext ctx = selfCtx;
        synchronized (heldLock) {
            if (released) return;
            released = true;
            confirmed = true;
            heldSnapshot = new ArrayList<>(held);
            promiseSnapshot = new ArrayList<>(heldPromises);
            held.clear();
            heldPromises.clear();
        }
        if (scheduled != null) {
            scheduled.cancel(false);
            scheduled = null;
        }
        if (delayedRelease != null) {
            delayedRelease.cancel(false);
            delayedRelease = null;
        }
        if (ctx != null) {
            flushHeld(ctx, heldSnapshot, promiseSnapshot);
        } else {
            releaseAll(heldSnapshot, promiseSnapshot);
        }
        if (ctx != null) {
            try {
                if (ctx.pipeline().context(this) != null) {
                    ctx.pipeline().remove(this);
                }
            } catch (Throwable t) {
                // 管线正在被拆除时移除会失败：忽略即可（本处理器终将被 netty 自己清掉）
                LOGGER.debug("[Zstd] gate self-removal skipped: {}", t.toString());
            }
        }
    }

    private void flushHeld(ChannelHandlerContext ctx, List<Object> held, List<ChannelPromise> heldPromises) {
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
        if (deliverable) {
            ctx.flush();
        }
    }

    private void releaseAll(List<Object> held, List<ChannelPromise> heldPromises) {
        for (int i = 0; i < held.size(); i++) {
            ReferenceCountUtil.release(held.get(i));
            failQuietly(heldPromises.get(i));
        }
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
        // 被外部摘除（插件卸载 / 管线重建）：与超时同语义 —— 服务端在这一刻已经是 zstd，
        // 扣住的包按当前管道补发，并标记"已提交"。这里<b>绝不</b>改动管线（原版处理器
        // 实例已无法重新插入，见类注释），否则只会抛一条吓人的 ChannelPipelineException。
        //
        // ⚠️ 取快照与清空必须在一把锁内完成：写包线程可能正在 add，漏掉的包既发不出去也
        // 不会被释放（引用计数泄漏 + 客户端缺包）。
        List<Object> heldSnapshot;
        List<ChannelPromise> promiseSnapshot;
        synchronized (heldLock) {
            heldSnapshot = new ArrayList<>(held);
            promiseSnapshot = new ArrayList<>(heldPromises);
            held.clear();
            heldPromises.clear();
            released = true;
            confirmed = true;
        }
        if (scheduled != null) {
            scheduled.cancel(false);
            scheduled = null;
        }
        flushHeld(selfCtx != null ? selfCtx : ctx, heldSnapshot, promiseSnapshot);
    }

    /**
     * 是否属于切换窗口内必须扣住的包。
     *
     * <p>判据：是 MC 包对象（类名落在 {@code .protocol.} 命名空间），而不是已编码的
     * {@code ByteBuf}、也不是 Netty 的控制消息（协议阶段切换用的 {@code Runnable} 等）。</p>
     *
     * <p>⚠️ 放行 {@code ByteBuf} 是必需的：服务端自己的 {@code zstd:switch} 请求
     * 是"已编码好的帧"，从编码器的 ctx 写出时会经过本闸门；若把 ByteBuf 也扣住，
     * 请求永远发不出去，握手必然超时。</p>
     */
    private static boolean mustHold(Object msg) {
        if (msg == null) return false;
        if (msg instanceof Runnable) return false;
        if (msg instanceof io.netty.buffer.ByteBuf) return false;
        String cn = msg.getClass().getName();
        return cn.contains(".protocol.");
    }

    /** 创建闸门实例。 */
    public static ZstdSwitchGate create() {
        return new ZstdSwitchGate();
    }
}
