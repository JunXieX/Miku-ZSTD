package mikumc.zstd.protocol;

import com.github.luben.zstd.ZstdCompressCtx;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.nio.channels.ClosedChannelException;

/**
 * Miku-ZSTD 下行编码器基类（帧格式 <b>v7</b>，协商版本 v11）——服务端共享实现。
 *
 * <h2>下行帧格式（v7）</h2>
 * <pre>
 *   [varint bodyLen?][varint rawSize][varint dictGen][varint blockCount] + block...
 *   block = [varint blockRawLen][varint chunkLen][chunk]
 *   payload（解压后）= [varint pktLen][pkt] [varint pktLen][pkt] ...
 * </pre>
 * <p>{@code rawSize == 0} 表示未压缩直存：此时帧体<b>恰好是一个包</b>（{@code [0][packet]}），
 * 与原版未压缩帧<b>逐字节同形</b>。</p>
 *
 * <p>⚠️ {@code dictGen} <b>只出现在压缩帧里</b>（{@code rawSize > 0} 之后），直存帧一个字节都不多。
 * 这不是随手定的顺序：直存帧"与原版未压缩帧逐字节同形"是<b>整个切换窗口的安全前提</b>
 * （见下文的协议保证），一旦给它加字段，这条同形性就没了，窗口内"按原版解释"的兼容路径
 * 立刻失效。直存帧本来也不使用字典（低于跳过阈值），不需要代数。</p>
 *
 * <p>⚠️ <b>直存帧必须一帧一包，且不能带任何内层长度前缀</b>。原版 {@code CompressionDecoder}
 * 及其后的包解码器会把 dataLength 之后的<b>全部</b>字节当成<b>一个</b>包；在 0 之后多写一层
 * {@code [varint pktLen]}，包解码器读到的"包 id"就是那个长度值 —— 直接 {@code Unknown packet}
 * 断连（2026-10-19 生产事故：玩家完全进不去服务器）。Velocity-CTD 的 frame-decoder 还会在
 * LOGIN 阶段校验 serverbound 包 id（compressionEnabled 且 dataLength==0 时把下一个 varint
 * 当包 id 查注册表），判据与上者相同。逐字节同形后，任何一层（本插件解码器 / 原版解码器 /
 * 包解码器 / Velocity 的校验器）都能正确解释，<b>不存在任何字节级启发式</b>。</p>
 *
 * <p>{@code bodyLen} <b>只由 Velocity 写</b>：它 4.x 的压缩编码器把"外层长度 + 压缩"合在同一个
 * 处理器里，跳过它就必须自带长度。Paper 的 {@code prepender} 仍在写帧路径下游，
 * 由它负责外层长度，子类通过 {@link #writeBodyLen()} 声明。</p>
 *
 * <h2>批处理：以 flush 为界</h2>
 * <p><b>一个 flush 成出一帧</b>：{@code flush()} 到达时把攒下的包合成一帧、写出并 flush。
 * 这样攒批边界就是<b>上游本来就在发生的 flush</b>——服务端在 tick 末尾、区块发送完成、
 * 登录期写包时都会 flush，帧边界天然与它们对齐，<b>不额外增加任何延迟</b>。</p>
 *
 * <p>历史沿革（为什么不是定时器）：旧实现是"吞掉 {@code flush()}，再用一个 2ms 定时器成帧"。
 * 那是纯粹的固定开销：每个包都要等满窗口（往返 +4ms），却<b>并不对齐</b> tick ——
 * 一 tick 内多次 flush 照样被切成多帧；而窗口到期的那一帧还会与 tick 末尾的 flush 撞车。
 * 更要命的是它在"上游只 write 不 flush"和"上游 writeAndFlush 极频繁"两种极端下都表现最差。</p>
 *
 * <p>保留一个<b>兜底窗口</b>（{@code configBatchWindowMs()}，默认 50ms）：上游若只 {@code write()}
 * 不 flush（少数插件会这么干），包不能无限期滞留，到点强制成帧。正常流量下这个定时器
 * <b>永远轮不到</b>——flush 会比它先到，届时定时器被取消。</p>
 *
 * <p>为什么"每 flush 一帧"不会把压缩率做烂：帧是独立压缩的，单看一帧，帧头与熵表确实要重复
 * 发送（实测每包单独成帧时小包甚至会膨胀到 105%，只能直存）。但这个损失已被
 * {@link ZstdRollingDict} 的<b>跨帧历史</b>补回来——每一帧都能向前引用最多 64KB 的原始历史，
 * 上一帧发过的内容这一帧直接匹配，不再依赖"同一帧内合批"来摊薄熵表。两条改动必须
 * <b>一起</b>上：只改成 flush 成帧而不引入历史字典，带宽会退回到"每包一帧"的水平。</p>
 *
 * <h2>安装位置（协议 v7 起）</h2>
 * <p>本处理器<b>不替换</b>原版压缩处理器，而是插在它<b>tail 侧</b>（出站先经过本类），
 * 然后把成帧结果从<b>原版处理器的 context</b> 往下写 —— 绕开它，但不拆它。
 * 这样做换来三件事：</p>
 * <ul>
 *   <li>原版 {@code compress} / {@code prepender} 一直留在管线里，ViaVersion / PacketEvents /
 *       Grim 这类<b>按名字查找</b>它们的插件照常工作；</li>
 *   <li>回落原版只是"把自己摘掉"，不存在"原版处理器已被移除、Netty 非 {@code @Sharable}
 *       实例无法重新插入"的死局（旧实现因此只能关连接）；</li>
 *   <li>不再有"只装上一半"的中间态——最坏情况是这一帧没走 zstd，而不是单侧 zstd 必断连。</li>
 * </ul>
 * <p>具体写到哪个 context 由子类的 {@link #downstream(ChannelHandlerContext)} 决定。</p>
 *
 * <h2>为什么放在共享模块</h2>
 * <p>本类与 {@link ZstdBatchDecoderBase} 是两端唯一需要"逐字节对称"的代码：
 * 帧格式一旦漂移，症状是"握手能成、之后静默错乱"，极难定位（2.0.0 曾因客户端
 * 多写一层长度前缀造成过断连回归）。共享单一实现后，这类漂移在结构上不可能发生；
 * 剩余的跨端差异（外层长度、统计口径、配置来源、字典状态）全部收敛为下面这些钩子。</p>
 */
public abstract class ZstdBatchEncoderBase extends ChannelDuplexHandler {

    protected static final Logger LOGGER = LoggerFactory.getLogger("miku-zstd");

    /**
     * 复用缓冲的初始容量。
     *
     * <p>典型批次只有2~8KB，每连接的原文缓冲从小容量增长，大帧另用临时缓冲。</p>
     */
    private static final int INITIAL_BUFFER = 8 * 1024;

    /**
     * 常驻缓冲上限。超过此尺寸的批次改用临时数组（用完即弃），对齐解码侧
     * {@link ZstdBatchDecoderBase} 的同名策略。
     *
     * <p>进服阶段的区块/注册表大帧会把原文缓冲顶到几十 MB；若常驻，200 人在线
     * 仅此一项就是 GB 级。大帧只在进服时出现，临时分配的代价可忽略。</p>
     */
    private static final int SCRATCH_KEEP_LIMIT = 512 * 1024;

    private ZstdCompressCtx compressCtx;

    /** 原始负载拼接缓冲：池化直存，按需增长复用。 */
    private ByteBuf rawBuf;

    /**
     * 当前批次<b>实际</b>使用的原始缓冲：小批次是常驻的 {@link #rawBuf}，大帧是临时缓冲。
     * 压缩在池线程执行，必须把实例捕获进 lambda，不能在池线程里重新读字段
     *（那时字段可能已被下一次成帧换掉）。
     */
    private ByteBuf batchRaw;

    /**
     * 单连接待发送数据的硬上限（字节）。
     *
     * <p>⚠️ 这是防「一条连接拖垮整个服务端」的最后一道闸：压缩任务在途时
     * （{@code compressing=true}）{@code emitBatch} 会直接返回，而上游仍可继续写包，
     * {@code pending} 因此<b>没有天然上限</b>。正常流量下"一个 flush 一批" + 几毫秒压缩
     * 只会积压几十个包；但若某个大包压缩异常慢（或线程池被其它连接占满），
     * 积压会无界增长——而这是<b>堆内存</b>，最终 OOM 的是整个服务端进程，
     * 不是这一条连接。</p>
     *
     * <p>取 128MB：远高于正常进服峰值（几 MB 的 Registry 包 + 数十个包合批），
     * 只有异常情况才可能触发。触发时选择<b>关闭这一条连接</b>并明确记日志——
     * 与"整个服务器 OOM"相比，这是唯一合理的取舍（包不能丢，丢了协议就错乱）。</p>
     */
    private static final long MAX_PENDING_BYTES = 128L * 1024 * 1024;

    /**
     * 直存阈值的地板。
     *
     * <p>为什么不能允许配置把它压到 0：切换握手（{@code zstd:switch} 询问 / 确认）是
     * <b>十几个字节</b>的小包，而且是<b>帧层嗅探器</b>按"直存帧 {@code [0][packet]} /
     * 裸帧 / 原版 zlib 帧"三种形态识别的（见 {@code ZstdStepFrames.asBarePacket}）。
     * 一旦这类小包被压成 zstd 帧，嗅探器认不出来，闸门就只能等满超时才放行——
     * 每次登录白等 5 秒（不致命，但会让人误判成"卡登录"）。</p>
     *
     * <p>16 字节是个保守的地板：低于它的批次压缩后几乎必然膨胀（实测 16B 带字典仍膨胀到
     * 103.5%），本来就该直存；钉在这里而不是靠各端配置记得调，是为了让"协议能不能被识别"
     * 不依赖运维配置。</p>
     */
    private static final int MIN_STORED_THRESHOLD = 16;

    /** 低于这个原始大小不切块：切了只会多摊块头，并行收益抵不过块头的带宽成本。 */
    private static final int PARALLEL_MIN_BYTES = 256 * 1024;

    /** 单帧最多切几块。 */
    private static final int MAX_PARALLEL_BLOCKS = 4;

    private final List<ByteBuf> pending = new ArrayList<>();
    private final List<ChannelPromise> pendingPromises = new ArrayList<>();

    /**
     * "非 ByteBuf 出站消息"的屏障队列（顺序保证，见 {@link #write}）。
     *
     * <p>典型成员是原版切协议时从<b>出站方向</b>写下来的任务对象
     * （{@code UnconfiguredPipelineHandler} 的 lambda，要一路走到头侧 decoder 位置的
     * {@code inbound_config} 才被吃掉）。它们不是包字节，不能塞进 {@link #pending}
     * （那是压缩对象），但又<b>必须排在"已经攒下的包、以及它们的压缩结果"之后</b>——
     * 压缩是异步的，如果这时直接 {@code ctx.write}，任务对象就会插到还没写出的帧前面，
     * 等于"协议先切、帧后到"，对端按新协议解旧帧 → 直接断连。</p>
     *
     * <p>数量极少（每条连接只切两次协议），用线性表足够。</p>
     */
    private final List<Object> barriers = new ArrayList<>();
    private final List<ChannelPromise> barrierPromises = new ArrayList<>();

    /**
     * "屏障<b>之后</b>"写入的包。
     *
     * <p>⚠️ 光把任务塞进 {@link #barriers} 还不够：任务进队之后到它真正写出之间，上游可能
     * 继续写包。那些包<b>语义上排在任务之后</b>（包层 encoder 在本处理器上游，它们的序列化
     * 用的是切换前还是切换后的协议取决于任务何时执行），因此必须先攒在这里，等
     * {@link #drainBarriers} 把任务写出去、再按原顺序交回 {@link #pending} 成帧。</p>
     *
     * <p>数量同样极少（只覆盖"协议切换任务落地 + 紧随其后的若干个包"这一瞬）。</p>
     */
    private final List<ByteBuf> afterBarrier = new ArrayList<>();
    private final List<ChannelPromise> afterBarrierPromises = new ArrayList<>();
    private long afterBarrierBytes;
    /** {@link #pending} 中 ByteBuf 的总可读字节数（仅 event loop 线程读写） */
    private long pendingBytes;
    private ScheduledFuture<?> windowTask;

    /** 是否有批次尚未完成压缩、历史提交或资源收尾（仅 event loop 线程读写） */
    private boolean compressing;
    /** 在途压缩任务持有的 promise（仅 event loop 线程读写）；通道关闭时需一并失败化 */
    private List<ChannelPromise> inflightPromises;
    /** handler 被移除后，异步压缩回调不得再向当前 pipeline 写帧。 */
    private boolean removed;

    /**
     * handlerRemoved 时若仍有在途压缩，把 scratch 释放推迟到 {@code backToEventLoop}。
     *
     * <p>⚠️ 否则会踩到 use-after-free：{@code handlerRemoved} 里可能刚通过
     * {@code emitBatch} 起了一个压缩任务，池线程此时正要读 {@code rawBuf}；而
     * {@code releaseScratch()} 会把它还给池化分配器，同一块内存立刻可能被别的连接取走改写。
     * 加 {@code compressLock} 并不够——池线程可能还没进临界区，会在我们释放<b>之后</b>才进去
     * （锁只保护"正在压缩"的那一段，不保护"即将开始"）。</p>
     */
    private boolean deferredScratchRelease;

    /**
     * 独享锁（子类钩子为默认值 {@link #compressLock()} 时使用）。
     *
     * <p>真正需要在池线程与 {@code close()} 之间做互斥的端，必须让
     * {@link #compressLock()} 返回<b>与 ChannelManager 同一个</b> {@link ZstdCompressLock}
     * —— 那里带着"上下文已关闭"标志（见该类的类注释，它防的是一次会让进程 SIGSEGV 的
     * use-after-free）。</p>
     */
    private final ZstdCompressLock ownLock = new ZstdCompressLock();

    // ── 压缩线程池 ───────────────────────────────────────────────────────────
    // 共享实现在 ZstdCompressPool，本类只调用它的静态入口。
    //
    // 为什么压缩必须离开 event loop：在 event loop 上同步压缩时，一个玩家的大批次
    //（几毫秒）会阻塞同一 event loop 上所有其他玩家的包处理——表现为"延迟随在线人数
    // 放大"的抖动，而且 ping 上看不出来（它是突发性的，不是固定开销）。
    //
    // 池的规模、活跃/排队数、单次耗时都在 ZstdCompressPool 里统一统计，
    // 命令层可直接取摘要——"要不要降 level / 加线程"只能靠这些数字判断。


    /**
     * 兜底窗口与单帧包数上限。这里的初值只是"{@code handlerAdded} 之前万一被读到"的兜底
     * ——真正生效的值在任何写操作都可能发生之前就已由 {@code handlerAdded} 从各端配置写入
     *（见该处的 {@code Math.max(...)}）。因此不要把配置默认值写在这里：两处一旦不一致，
     * 读代码的人无法判断哪个才是生效值。
     *
     * <p>⚠️ {@code batchWindowMs} 自协议 v7 起<b>只</b>是"上游不发 flush 时的兜底"，
     * 不再是攒批窗口——正常的攒批边界是 {@code flush()} 本身（见类注释）。</p>
     */
    private int batchWindowMs = 50;
    private int batchMaxPackets = 64;

    /**
     * 本方向的滚动历史字典（协议 v7）。
     *
     * <p>⚠️ 喂入点、喂入内容、轮换时机必须与<b>对端解码器</b>严格对称——两边算出的字典
     * 与代数必须逐字节相同，否则解压出的是垃圾。README / 帧格式里的约定见
     * {@link ZstdRollingDict} 的类注释。</p>
     */
    private final ZstdRollingDict rolling = new ZstdRollingDict();

    /** 已经 {@code loadDict} 进 zstd 上下文的那份历史字典（同一份不重复装载）。 */
    private byte[] appliedRollingDict;

    /**
     * 在途批次使用的字典代数，{@code buildCompressedFrame} 写进帧头。
     *
     * <p>用字段而不是参数传递：同一时刻只允许一个在途批次（{@code compressing} 门），
     * 而写帧发生在该批次回到 event loop 的回调里——语义上仍属于"这一批"。</p>
     */
    private int inflightDictGen;

    // ────────────────────────────────────────────────────────────────
    // 子类钩子：两端唯一的差异点
    // ────────────────────────────────────────────────────────────────

    /**
     * 解析本连接的压缩上下文（{@code handlerAdded} 时调用一次）。
     *
     * @return 压缩上下文；{@code null} 表示该连接不做 zstd，走透传
     */
    protected abstract ZstdCompressCtx resolveCompressContext(ChannelHandlerContext ctx);

    /**
     * 小包跳过压缩的阈值：低于该值直接直存，不调用 zstd。
     *
     * <p>5.0.42 起不再区分"有没有静态字典"：训练字典整套已删除，滚动历史不提供熵表，
     * 所以这里只有一个阈值（各端配置项 {@code skip_compress_below_bytes}）。</p>
     */
    protected abstract int skipCompressThreshold();

    /** 压缩等级（分块并行时给每块配临时 ctx 用）。 */
    protected abstract int configLevel();

    /** 窗口日志（0 = 交给 zstd 按等级自动选）。 */
    protected abstract int configWindowLog();

    /** 帧头是否写外层 {@code bodyLen}（Velocity true；Paper 由 prepender 负责） */
    protected abstract boolean writeBodyLen();

    /**
     * 成帧结果要写到哪个 context 的<b>下游</b>。
     *
     * <p>默认就是本处理器的 ctx（"替换原版压缩处理器"的老形态）。协议 v7 起三端都应覆写为
     * <b>被绕过的原版压缩处理器的 context</b>：{@code ctx.write()} 从那个 context 出发，
     * 恰好跳过它自己，继续走本该走的下游（prepender / encrypt / socket）。
     * 这样原版处理器<b>留在管线里不被拆掉</b>，详见类注释的"安装位置"。</p>
     *
     * <p>⚠️ 覆写方返回 {@code null}（锚点此刻不在管线里）时，本类退回 {@code ctx}——
     * 宁可多经一层（原版压缩会按阈值处理我们的帧，结果多半是对端解不开），也不要静默丢包；
     * 真实场景里这一步只在管线被第三方插件拆改的瞬间可能发生。</p>
     */
    protected ChannelHandlerContext downstream(ChannelHandlerContext ctx) {
        return ctx;
    }

    /** 本方向的滚动历史（只读用途：子类可在诊断命令里读代数/命中情况）。 */
    protected final ZstdRollingDict rollingDict() {
        return rolling;
    }

    /**
     * 把"当前生效的历史"装进给定压缩上下文。
     *
     * <p>5.0.42 起只有一种字典来源：滚动历史（{@link ZstdRollingDict}）。训练字典整套已删除
     * ——实机对照显示它比不装还差 3.7%，而滚动历史才是那 85% 节省的来源。</p>
     */
    private void loadActiveDict(ZstdCompressCtx c) {
        byte[] bytes = rolling.activeDict();
        if (bytes != null) {
            c.loadDict(bytes);
        }
    }

    /**
     * 一帧产出后的统计回调。
     *
     * @param rawBytes   原始字节口径（{@code 1 + 本帧原始总量}；直存帧一帧一包，故按包计）
     * @param wireBytes  实际写出的帧字节数
     * @param compressed 该帧是否走了压缩路径
     */
    protected abstract void onFrame(int rawBytes, int wireBytes, boolean compressed);

    /**
     * 批内原始数据采样回调（供字典训练使用）。
     *
     * <p>默认空实现：不需要字典的一端无需关心。实现在此接收的是<b>未经压缩的批次字节</b>
     * （{@code [varint pktLen][pkt]...}），采样方必须自行复制——该缓冲会被复用。</p>
     *
     * <p>参数是池化直存 {@link ByteBuf}（{@code readerIndex()} 在起点、可读长度为 length），
     * 采样方用 {@code getBytes} 复制即可，不要改动其 readerIndex。</p>
     */
    protected void onRawBatch(ByteBuf raw, int length) {
    }

    /** 批处理窗口（毫秒），0 = 立刻成帧 */
    protected abstract int configBatchWindowMs();

    /** 单帧最多合并多少个包 */
    protected abstract int configBatchMaxPackets();

    /**
     * 压缩上下文的互斥锁。
     *
     * <p>压缩已卸载到线程池，因此<b>必须与任何改写 {@code compressCtx} 的操作互斥</b>
     * （典型场景：字典训练完成后 {@code loadDict()} 换字典）。默认返回本 handler 独占的
     * {@link ZstdCompressLock}，但字典通常由 ChannelManager 持有并改写——那种情况下子类
     * <b>必须</b>覆写为与 ChannelManager 相同的那把锁，否则会与池线程并发使用同一个 ctx。</p>
     *
     * <p>⚠️ 返回类型刻意是 {@link ZstdCompressLock} 而非 {@code Object}：池线程需要它来
     * 判断"上下文是否已被 close"（见该类注释——少这一判就是一次 use-after-free 崩溃）。</p>
     */
    protected ZstdCompressLock compressLock() {
        return ownLock;
    }

    /**
     * 本批实际要使用的压缩上下文（在池线程、已持有 {@link #compressLock()} 时调用）。
     *
     * <p>默认返回 {@code handlerAdded} 时解析出的下行上下文；生命周期由服务端
     * ChannelManager 与连接共同管理，释放时必须持有同一把压缩锁。</p>
     *
     * @return 压缩上下文；{@code null} 表示当前不可用（本批按失败处理）
     */
    protected ZstdCompressCtx compressContextForBatch() {
        return compressCtx;
    }

    // ────────────────────────────────────────────────────────────────
    // 共享实现
    // ────────────────────────────────────────────────────────────────

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        compressCtx = resolveCompressContext(ctx);
        if (compressCtx == null) {
            LOGGER.warn("[Zstd] encoder installed without a compress context — passthrough mode");
        }
        batchWindowMs = Math.max(0, configBatchWindowMs());
        batchMaxPackets = Math.max(1, configBatchMaxPackets());
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
        if (removed) {
            ReferenceCountUtil.release(msg);
            promise.tryFailure(new ClosedChannelException());
            return;
        }
        if (!(msg instanceof ByteBuf packet)) {
            // 非包字节（协议切换任务等）：没有积压时直接透传，零成本；
            // 有积压（或压缩在途）时作为屏障排队，等帧写出后再按序放行。
            if (pending.isEmpty() && !compressing) {
                ctx.write(msg, promise);
                return;
            }
            barriers.add(msg);
            barrierPromises.add(promise);
            emitBatch(ctx);
            return;
        }
        if (compressCtx == null) {
            // 没有下行压缩上下文时交回原版处理器，不缓冲、不产帧、不记账。
            ctx.write(packet, promise);
            return;
        }
        if (!barriers.isEmpty()) {
            // 屏障（协议切换任务）还没落地：本包必须排在它后面，不能进 pending 抢跑。
            afterBarrier.add(packet);
            afterBarrierPromises.add(promise);
            afterBarrierBytes += packet.readableBytes();
            if (afterBarrierBytes + pendingBytes > MAX_PENDING_BYTES) {
                // 与 pending 同一个硬上限：长期不落地同样会把堆拖爆
                LOGGER.error("[Zstd] outbound backlog (post-barrier) exceeds {}B — closing connection {}",
                        MAX_PENDING_BYTES, ctx.channel().remoteAddress());
                releaseAfterBarrier();
                failAll(afterBarrierPromises, new ClosedChannelException());
                afterBarrierPromises.clear();
                ctx.close();
                return;
            }
            scheduleFallback(ctx);
            return;
        }
        pending.add(packet);
        pendingPromises.add(promise);
        pendingBytes += packet.readableBytes();
        // 积压超硬上限：关闭该连接，而不是让堆无界增长（见 MAX_PENDING_BYTES）。
        // 走到这里说明压缩已经长时间跟不上这一条连接的上游产出，继续累积只会
        // 把整个进程拖进 OOM；而丢包会造成协议错乱，所以只能断开重连。
        if (pendingBytes > MAX_PENDING_BYTES) {
            LOGGER.error("[Zstd] outbound backlog {}B exceeds {}B — closing connection {}",
                    pendingBytes, MAX_PENDING_BYTES, ctx.channel().remoteAddress());
            // 先按"通道关闭"语义收尾：释放 pending、失败化 promise（否则上游 future 永挂）
            if (windowTask != null) {
                windowTask.cancel(false);
                windowTask = null;
            }
            releasePending();
            failPending(new ClosedChannelException());
            pendingPromises.clear();
            ctx.close();
            return;
        }

        if (pending.size() >= batchMaxPackets) {
            emitBatch(ctx);
            return;
        }
        scheduleFallback(ctx);
    }

    /**
     * <b>攒批边界就是这里</b>：一次 flush 成出一帧。
     *
     * <p>上游的 {@code writeAndFlush} / tick 末尾的批量 flush / 登录期写包时的 flush，
     * 都在这个时机把"这段时间攒下的包"变成<b>一帧</b>——帧边界与上游的 flush 语义对齐，
     * 本类不额外引入任何等待（见类注释的"批处理：以 flush 为界"）。</p>
     *
     * <p>⚠️ 成帧后<b>不再</b>在这里补 {@code ctx.flush()}：</p>
     * <ul>
     *   <li>直存路径（{@link #writeStoredFrames}）在 event loop 上同步写完并自己 flush；</li>
     *   <li>压缩路径（{@link #emitBatch} 交给池线程）在批次回到 event loop 时才写帧，
     *       那里同样自带 flush；</li>
     *   <li>若此刻正好有在途批次（{@code compressing} 门），{@link #emitBatch} 会直接返回，
     *       积压会在在途批次收尾后立刻被接手并 flush —— 此时补 flush 也没有数据可送。</li>
     * </ul>
     * <p>无待发包时保持原语义透传，否则下游（encrypt / socket）永远等不到 flush，
     * 表现为"发了但对方收不到"。</p>
     */
    @Override
    public void flush(ChannelHandlerContext ctx) {
        if (!removed && !pending.isEmpty()) {
            emitBatch(ctx);
            return;
        }
        ctx.flush();
    }

    /**
     * 兜底定时器：<b>只在"上游只 {@code write()} 而不 flush"时</b>才会真正触发。
     *
     * <p>正常流量下每次 {@code writeAndFlush} 都会先走 {@link #flush(ChannelHandlerContext)}
     * 把包成帧并取消本定时器，所以它不会带来任何额外延迟；而一旦少了它，不发 flush 的调用方
     * 会让包无限期滞留在这里（对端表现为"卡住不动"，比掉线更难排查）。</p>
     */
    private void scheduleFallback(ChannelHandlerContext ctx) {
        if (windowTask != null) return;
        if (batchWindowMs <= 0) {
            emitBatch(ctx); // 兜底窗口为 0：退化为"写即成帧"
            return;
        }
        windowTask = ctx.executor().schedule(() -> {
            windowTask = null;
            emitBatch(ctx);
        }, batchWindowMs, TimeUnit.MILLISECONDS);
    }

    /**
     * 成帧并写出。压缩路径会<b>卸载到 {@link ZstdCompressPool}</b>，
     * 直存路径（小包）仍在 event loop 上完成——它本来就不调用 zstd，多绕一次线程池不划算。
     */
    private void emitBatch(ChannelHandlerContext ctx) {
        if (removed) {
            releasePending();
            failPending(new ClosedChannelException());
            pendingPromises.clear();
            return;
        }
        if (windowTask != null) {
            windowTask.cancel(false);
            windowTask = null;
        }
        if (pending.isEmpty()) {
            return;
        }
        // ⚠️ 有压缩任务在途时绝不开始新一批：compressCtx / rawBuf 都是
        // 「每连接独占」的资源，被两个线程同时用会直接损坏数据；同时这也是保持写出
        // 顺序的关键。积压的数据不会滞留——在途任务完成后的回调会重新检查 pending。
        if (compressing) {
            return;
        }

        int batchCount = takeFrameBatch();
        if (batchCount == 0) {
            if (!removed && !pending.isEmpty()) {
                schedulePendingBatch(ctx);
            }
            return;
        }
        List<ByteBuf> batchPackets = new ArrayList<>(pending.subList(0, batchCount));
        List<ChannelPromise> promises = new ArrayList<>(pendingPromises.subList(0, batchCount));

        final int rawTotal;
        final ByteBuf raw;
        final boolean rawTemporary;
        try {
            rawTotal = concatPending(batchPackets, ctx);
            raw = batchRaw; // 本批实际使用的缓冲：大帧是临时缓冲，必须捕获实例传给池线程
            // ⚠️ 必须在这里就记下"是不是临时缓冲"：回调执行时 handlerRemoved 可能已经把
            // rawBuf 释放并置 null，那时再比较 raw != rawBuf 会成立，导致对已被释放的
            // 常驻缓冲二次 release（实测 IllegalReferenceCountException: refCnt 0）。
            rawTemporary = raw != rawBuf;
            // 立刻摘掉字段引用：此后只有上面这个局部变量持有它。否则大帧（如进服阶段的
            // 注册表帧，几十 MB）会被字段一直留到下一批 —— 刚进服就挂机的连接会白占一份。
            batchRaw = null;
        } catch (Throwable t) {
            // ⚠️ concatPending 可能在**释放 pending 之前**就抛出来（采样回调抛异常、超大批次
            // 分配失败等）。此时 pending 里仍压着已 retain 的 ByteBuf：不在这里释放并清空，
            // 它们永远没人释放（引用计数泄漏），而且下一批会把这批残缺数据一并发出去，
            // 同时让 pendingPromises 与 pending 彻底失去对应关系。
            // 失败批次的缓冲也不再被本 handler 引用；大帧那份是临时分配，
            // 不显式 release 就直接泄漏（池化直存缓冲不会被 GC 回收）。
            if (batchRaw != null && batchRaw != rawBuf) {
                batchRaw.release();
            }
            batchRaw = null;
            // removePendingPrefix 现在自带 release，不能再额外 releasePackets（会双重释放）
            removePendingPrefix(batchCount);
            failAll(promises, t);
            LOGGER.error("[Zstd] encoder batch concat failed", t);
            if (!removed && !pending.isEmpty()) {
                schedulePendingBatch(ctx);
            }
            return;
        }
        removePendingPrefix(batchCount);

        final boolean withBodyLen = writeBodyLen();
        compressing = true;
        inflightPromises = promises;

        // ── 小包直存 ──
        // 实测：无字典时 ≤32B 压缩后膨胀到 102~113%，48B 起才有收益；
        // 有字典时 24B 起才有收益。低于阈值直接直存，省一次 zstd 调用且线路字节更少。
        // ⚠️ 滚动历史生效时也算"有字典"：gen ≥ 1 之后两端用的就是那份 64KB 历史，
        // 此时 24B 起压缩确实有收益——按无字典的 48B 判会白丢这一段（见配置里的实测值）。
        // ⚠️ 直存帧一帧一包、不带内层长度前缀（见类注释的"帧格式"一节），所以这里
        // 不能把上面那份带长度前缀的拼接缓冲直接当一个帧发出去。
        // ⚠️ 阈值只认【带熵表的静态字典】，滚动历史不算：那个 24B 是拿训练字典实测出来的
        //（16B 仍膨胀到 103.5%，靠字典预置的 Huffman/FSE 表）；滚动历史是 raw content，
        // 只有内容匹配、没有熵表，24~47B 的包压出来照样膨胀 —— 把它算作"有字典"等于每帧
        // 白跑一次 zstd（字节不会变差，压不动会退回直存，但 CPU 是白花的）。
        if (rawTotal < Math.max(MIN_STORED_THRESHOLD, skipCompressThreshold())) {
            try {
                writeStoredFrames(ctx, raw, rawTotal, withBodyLen, promises);
            } catch (Throwable t) {
                failAll(promises, t);
                failBarriers(t);
                LOGGER.error("[Zstd] encoder stored-frame build failed", t);
            } finally {
                completeBatch(ctx, raw, rawTemporary);
            }
            return;
        }

        // ── 压缩路径：交给线程池 ──
        // 定格本批代数；历史与资源收尾完成前，不允许下一批复用原文和上下文。
        inflightDictGen = rolling.generation();
        try {
            applyRollingDict();
            final ZstdCompressLock lock = compressLock();
            int blocks = parallelBlocks(rawTotal);
            if (blocks <= 1) {
                ZstdCompressPool.execute(() ->
                        compressOneBlock(ctx, promises, raw, rawTotal, withBodyLen, lock, rawTemporary));
            } else {
                compressBlocksInParallel(ctx, promises, raw, rawTotal, withBodyLen, lock, blocks, rawTemporary);
            }
        } catch (Throwable t) {
            // 池被关闭 / 线程创建失败：这批已经移出 pending 且 promises 已进 inflight，
            // 不在这里收尾就是"promise 永挂 + compressing 永久为真"（该连接之后再也发不出帧）。
            failAll(promises, t);
            failBarriers(t);
            LOGGER.error("[Zstd] encoder compression task rejected", t);
            completeBatch(ctx, raw, rawTemporary);
        }
    }

    /**
     * 分块数：小批次不切（切了反而多摊块头），只有大帧才值得并行。
     *
     * <p>上限取"池线程数"——块任务本身就跑在池里，超过线程数只会排队，
     * 而每个排队任务都会白借一份原生 ctx。</p>
     */
    private int parallelBlocks(int rawTotal) {
        if (rawTotal < PARALLEL_MIN_BYTES) {
            return 1;
        }
        int bySize = rawTotal / PARALLEL_MIN_BYTES;
        return Math.max(1, Math.min(Math.min(MAX_PARALLEL_BLOCKS, bySize), ZstdCompressPool.threads()));
    }

    /** 单块：小批次走这条（与分块前的行为逐字节等价，只是多了一个 blockCount=1）。 */
    private void compressOneBlock(ChannelHandlerContext ctx, List<ChannelPromise> promises,
                                  ByteBuf raw, int rawTotal, boolean withBodyLen,
                                  ZstdCompressLock lock, boolean rawTemporary) {
        Block block;
        try {
            synchronized (lock) {
                if (lock.isClosed()) {
                    backToEventLoop(ctx, promises, new ClosedChannelException(), null, raw, rawTotal, withBodyLen, rawTemporary);
                    return;
                }
                ZstdCompressCtx useCtx = compressContextForBatch();
                if (useCtx == null) {
                    backToEventLoop(ctx, promises, new ClosedChannelException(), null, raw, rawTotal, withBodyLen, rawTemporary);
                    return;
                }
                block = compressBlock(ctx, raw, 0, rawTotal, useCtx);
            }
        } catch (Throwable t) {
            backToEventLoop(ctx, promises, t, null, raw, rawTotal, withBodyLen, rawTemporary);
            return;
        }
        backToEventLoop(ctx, promises, null, new Block[]{block}, raw, rawTotal, withBodyLen, rawTemporary);
    }

    /**
     * 大帧分块并行压缩。
     *
     * <p>每块独占一份原生 ctx（块之间不能共享，zstd ctx 非线程安全），按字节均分——
     * 原始负载是 {@code [pktLen][pkt]...} 的连续字节，按任意字节切分，解压后顺序拼回即可。</p>
     *
     * <p>汇聚点放在"最后一个完成的块"上：主线程不再等待，避免池线程互等造成排队。</p>
     */
    private void compressBlocksInParallel(ChannelHandlerContext ctx, List<ChannelPromise> promises,
                                          ByteBuf raw, int rawTotal, boolean withBodyLen,
                                          ZstdCompressLock lock, int blocks, boolean rawTemporary) {
        int per = (rawTotal + blocks - 1) / blocks;
        Block[] results = new Block[blocks];
        java.util.concurrent.atomic.AtomicInteger remaining =
                new java.util.concurrent.atomic.AtomicInteger(blocks);
        java.util.concurrent.atomic.AtomicReference<Throwable> failure =
                new java.util.concurrent.atomic.AtomicReference<>();

        for (int i = 0; i < blocks; i++) {
            final int index = i;
            final int offset = i * per;
            final int length = Math.min(per, rawTotal - offset);
            try {
                ZstdCompressPool.execute(() -> {
                    try {
                        // ⚠️ 锁只用来"借 ctx 并绑字典"与判 closed，压缩本身必须在锁外：
                        // 块并行用的是各块独立的临时 ctx，把它们整段放进连接的锁里
                        // 会让所有块重新串行化，块并行就白做了。
                        ZstdCompressCtx blockCtx;
                        synchronized (lock) {
                            if (lock.isClosed()) {
                                throw new ClosedChannelException();
                            }
                            blockCtx = borrowBlockCtx();
                        }
                        try {
                            results[index] = compressBlock(ctx, raw, offset, length, blockCtx);
                        } finally {
                            ZstdCtxPool.returnCompress(blockCtx);
                        }
                    } catch (Throwable t) {
                        failure.compareAndSet(null, t);
                    } finally {
                        if (remaining.decrementAndGet() == 0) {
                            backToEventLoop(ctx, promises, failure.get(), results, raw, rawTotal, withBodyLen, rawTemporary);
                        }
                    }
                });
            } catch (Throwable t) {
                failure.compareAndSet(null, t);
                // 提交被拒也可能是最后一个：不在这里兜底，promise 与 compressing 就永远悬着。
                if (remaining.decrementAndGet() == 0) {
                    backToEventLoop(ctx, promises, failure.get(), results, raw, rawTotal, withBodyLen, rawTemporary);
                }
            }
        }
    }

    /** 借一份已按本连接参数配好的临时 ctx（并行块专用，用完必须归还）。 */
    private ZstdCompressCtx borrowBlockCtx() {
        ZstdCompressCtx c = ZstdCtxPool.borrowCompress(configLevel());
        c.setWindowLog(configWindowLog());
        c.setLong(1);
        c.setMagicless(true);
        c.setContentSize(false);
        c.setDictID(false);
        // 字典必须与本帧头里的 dictGen 一致：滚动历史优先，否则用静态字典。
        // 每块一份独立的 ctx，所以这里必须逐块装载（不能指望主 ctx 上已装好）。
        loadActiveDict(c);
        return c;
    }

    /** 压一块。返回的 chunk 是池化缓冲，由调用方（或帧构建）释放。 */
    private Block compressBlock(ChannelHandlerContext ctx, ByteBuf raw, int offset, int length,
                               ZstdCompressCtx useCtx) {
        ByteBuf dst = ctx.alloc().directBuffer(length + (length >>> 8) + 64);
        try {
            // ⚠️ srcOffset 必须是<b>缓冲内绝对偏移</b>（readerIndex + 块起点），不能传 0。
            // zstd-jni 的 native 实现是 GetDirectBufferAddress(src) + srcOffset：
            // GetDirectBufferAddress 返回的是整块内存的基址，**它不看 ByteBuffer 的 position**，
            // 所以 internalNioBuffer(index, len) 这个"视图"只服务于 Java 侧的 limit 校验。
            // 传 0 的后果是每块都压"缓冲开头那一段"，解压后拼起来等于第一块重复 N 次 ——
            // 表现为真实大帧（>256KB，例如 limbo/进服的 Registry）在块边界附近报
            // "illegal inner packet length"（实测 offset 恰好落在 512KB 附近）。
            int produced = useCtx.compressDirectByteBuffer(
                    dst.internalNioBuffer(0, dst.capacity()), 0, dst.capacity(),
                    raw.internalNioBuffer(0, raw.capacity()),
                    raw.readerIndex() + offset, length);
            dst.writerIndex(produced);
            return new Block(length, dst);
        } catch (Throwable t) {
            dst.release();
            throw t;
        }
    }

    /** 一个块的压缩结果：原文长度 + 压缩后的 chunk。 */
    private static final class Block {
        final int rawLen;
        final ByteBuf chunk;

        Block(int rawLen, ByteBuf chunk) {
            this.rawLen = rawLen;
            this.chunk = chunk;
        }
    }

    /** 压缩完成后回到 event loop：组帧、写出、并立刻接手积压的下一批。 */
    private void backToEventLoop(ChannelHandlerContext ctx, List<ChannelPromise> promises,
                                 Throwable failure, Block[] blocks, ByteBuf raw,
                                 int rawTotal, boolean withBodyLen, boolean rawTemporary) {
        ctx.executor().execute(() -> {
            try {
                if (removed || failure != null || !ctx.channel().isActive()) {
                    // 三种收尾共用一个出口：处理器已移除 / 本批压缩失败 / 通道已关。
                    // 屏障队列也必须一起收尾，否则排队中的协议切换任务会永久悬着。
                    Throwable cause = failure != null ? failure : new ClosedChannelException();
                    failAll(promises, cause);
                    failBarriers(cause);
                } else {
                    int wireTotal = 0;
                    for (Block b : blocks) {
                        wireTotal += b.chunk.readableBytes();
                    }
                    // 压缩无收益（压不动、或压缩后反而更大）时退回直存：直存帧与压缩帧
                    // 逐字节不同形，对端靠 rawSize==0 分支识别，所以这条回退是协议内的。
                    if (wireTotal > 0 && wireTotal < rawTotal) {
                        writeFrame(ctx, buildCompressedFrame(ctx, blocks, rawTotal, withBodyLen), promises);
                        // 压缩帧在历史里按"整批原始负载"记账：一次性喂入，与对端解码后
                        // 拿到的同一个 byte[] 完全一致。
                        feedHistory(raw, raw.readerIndex(), rawTotal);
                    } else {
                        // 直存路径在每写出一帧后自己喂（一帧一包，两端粒度相同），
                        // 所以这里<b>不能</b>再喂一次，否则两端的记账点会分叉。
                        writeStoredFrames(ctx, raw, rawTotal, withBodyLen, promises);
                    }
                }
            } catch (Throwable t) {
                failAll(promises, t);
                // 屏障队列也必须一起收尾：否则协议切换任务的 promise 永久悬着
                failBarriers(t);
                LOGGER.error("[Zstd] encoder frame build failed", t);
            } finally {
                releaseBlocks(blocks);
                completeBatch(ctx, raw, rawTemporary);
            }
        });
    }

    /** 历史与原文的所有权交还后，才允许下一批或协议屏障推进。 */
    private void completeBatch(ChannelHandlerContext ctx, ByteBuf raw, boolean rawTemporary) {
        if (rawTemporary) {
            raw.release();
        }
        if (deferredScratchRelease) {
            deferredScratchRelease = false;
            synchronized (compressLock()) {
                releaseScratch();
            }
        }
        compressing = false;
        inflightPromises = null;
        if (!removed) {
            if (!pending.isEmpty()) {
                schedulePendingBatch(ctx);
            } else if (!barriers.isEmpty()) {
                drainBarriers(ctx, out(ctx));
            }
        }
    }

    /**
     * 选出本帧要装的包（前缀），返回包数。
     *
     * <p>只有一种包会被拒绝：单包自身超过对端解码器的 rawSize 上限
     * （{@link ZstdVarInts#DEFAULT_MAX_VALUE}，32MB）。那种包无论怎么压缩都送不出去
     * （解码侧 {@code tryRead} 会直接判非法），留着只会让后续批次永远卡在队首。</p>
     *
     * <p>⚠️ 超过 {@link #maxRawBatchBytes()} 的包<b>不拒绝</b>，只是让它独占一帧：
     * 那只是合批的软目标。按原始大小拒绝会把配置阶段的大 Registry 包（原始数 MB、
     * 压缩后几百 KB）直接掐死，玩家进不了服。</p>
     */
    private int takeFrameBatch() {
        int softMax = maxRawBatchBytes();
        long batchBytes = 0;
        int count = 0;
        while (count < pending.size()) {
            ByteBuf packet = pending.get(count);
            int readable = packet.readableBytes();
            long packetBytes = (long) ZstdVarInts.length(readable) + readable;
            if (packetBytes > ZstdVarInts.DEFAULT_MAX_VALUE) {
                if (count == 0) {
                    IllegalArgumentException failure = new IllegalArgumentException(
                            "packet size " + packetBytes + " exceeds decoder rawSize limit "
                                    + ZstdVarInts.DEFAULT_MAX_VALUE);
                    ByteBuf rejected = pending.remove(0);
                    ChannelPromise promise = pendingPromises.remove(0);
                    pendingBytes -= rejected.readableBytes();
                    ReferenceCountUtil.release(rejected);
                    promise.tryFailure(failure);
                    LOGGER.warn("[Zstd] rejected outbound packet: {}B exceeds decoder rawSize limit {}B",
                            packetBytes, ZstdVarInts.DEFAULT_MAX_VALUE);
                    return 0;
                }
                break;
            }
            if (count > 0 && batchBytes + packetBytes > softMax) {
                break; // 软预算：装不下就起新帧（第一个包总是能进）
            }
            batchBytes += packetBytes;
            count++;
            if (count >= batchMaxPackets) {
                break;
            }
        }
        return count;
    }

    private int concatPending(List<ByteBuf> packets, ChannelHandlerContext ctx) {
        long rawTotalLong = 0;
        for (ByteBuf b : packets) {
            rawTotalLong += (long) ZstdVarInts.length(b.readableBytes()) + b.readableBytes();
        }
        if (rawTotalLong > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("batch raw size exceeds integer range: " + rawTotalLong);
        }
        int rawTotal = (int) rawTotalLong;
        final ByteBuf raw = ensureRawCapacity(ctx, rawTotal);
        batchRaw = raw;
        raw.clear();
        for (ByteBuf b : packets) {
            int n = b.readableBytes();
            ZstdVarInts.write(raw, n);
            raw.writeBytes(b, b.readerIndex(), n);
        }
        onRawBatch(raw, rawTotal); // 采样（必须在缓冲复用前）
        // ⚠️ 这里<b>不</b>释放 packets：release 必须发生在 removePendingPrefix 读完长度之后。
        // 旧顺序（先 release 再由 removePendingPrefix 读 readableBytes）读的是已释放的 buffer，
        // pendingBytes 记账会漂移 —— 而 MAX_PENDING_BYTES 这道防 OOM 闸门唯一的输入就是它。
        return rawTotal;
    }

    /**
     * 成帧结果的实际写出位置：跳过被绕开的原版压缩处理器（见 {@link #downstream}）。
     *
     * <p>⚠️ 写与 flush 都必须用<b>同一个</b> target：从本处理器 ctx 发 flush 虽然也能一路走到
     * socket，但它会先经过我们已经绕开的那个处理器，把"它自己的"待发数据一起刷出去
     * —— 在那条路径上我们没有向它写过任何东西，语义上纯属多余；统一用下游 ctx 更干净。</p>
     */
    private ChannelHandlerContext out(ChannelHandlerContext ctx) {
        ChannelHandlerContext d = downstream(ctx);
        return d == null ? ctx : d;
    }

    /**
     * 喂入本帧的原始字节，并（必要时）把新算出的历史字典装进压缩上下文。
     *
     * <p>⚠️ <b>必须在帧边界、以与对端完全相同的粒度调用</b>：对端解码器在同一帧处理完后
     * 喂同样的字节。粒度一旦不同（例如这里按整批喂、对端按包喂），两端的历史与代数就会分叉，
     * 后果是解压出垃圾而不是压缩率下降。详见 {@link ZstdRollingDict} 的类注释。</p>
     */
    private void feedHistory(ByteBuf raw, int index, int len) {
        rolling.feed(raw, index, len);
        applyRollingDict();
    }

    /** 把滚动历史算出的字典装载进压缩上下文（同一份只装一次）。 */
    private void applyRollingDict() {
        byte[] dict = rolling.activeDict();
        if (dict == null || dict == appliedRollingDict) {
            return;
        }
        ZstdCompressLock lock = compressLock();
        synchronized (lock) {
            // 上下文已释放（连接已关）：写已释放的原生内存是 SIGSEGV，这里必须放弃
            if (lock.isClosed()) return;
            ZstdCompressCtx c = compressContextForBatch();
            if (c == null) return;
            c.loadDict(dict);
            // 与 ChannelManager 装载静态字典后一致：帧头精简参数必须重设一遍
            c.setMagicless(true);
            c.setContentSize(false);
            c.setDictID(false);
        }
        appliedRollingDict = dict;
        // ⚠️ 必须抽样打印：满速连接每 MB 一次轮换、约 2~3 次/秒，逐次打会把 debug 日志刷满
        //（实测 40 秒 99 行，别人什么都看不见）。首代必打（确认识别到滚动历史已生效），
        // 之后每 64 代一行。
        int gen = rolling.generation();
        if (gen == 1 || gen % 64 == 0) {
            LOGGER.debug("[Zstd] 滚动历史字典已更新 gen={} ({}B)", gen, dict.length);
        }
    }

    /** 写出帧并完成这一批的 promise。 */
    private void writeFrame(ChannelHandlerContext ctx, ByteBuf frame, List<ChannelPromise> promises) {
        ChannelHandlerContext target = out(ctx);
        target.write(frame).addListener(f -> {
            for (ChannelPromise p : promises) {
                if (f.isSuccess()) {
                    p.trySuccess();
                } else {
                    p.tryFailure(f.cause());
                }
            }
        });
        target.flush();
    }

    private static void releaseBlocks(Block[] blocks) {
        if (blocks == null) {
            return;
        }
        for (Block b : blocks) {
            if (b != null) {
                b.chunk.release();
            }
        }
    }

    private static void failAll(List<ChannelPromise> promises, Throwable cause) {
        for (ChannelPromise p : promises) {
            p.tryFailure(cause);
        }
    }

    /**
     * 把排队的"非包消息"按序交给下游——<b>只能在帧写出之后调用</b>。
     *
     * <p>{@code target.write()} 是入队而非落盘，所以"先写帧、再写屏障"在同一个 event loop
     * 任务里就是确定的先后关系，不需要等 write future 完成。</p>
     */
    private void drainBarriers(ChannelHandlerContext ctx, ChannelHandlerContext target) {
        if (barriers.isEmpty()) {
            return;
        }
        for (int i = 0; i < barriers.size(); i++) {
            target.write(barriers.get(i), barrierPromises.get(i));
        }
        barriers.clear();
        barrierPromises.clear();
        target.flush();
        // 屏障之后的包现在才轮得到：按原顺序交回待成帧队列，并立刻成帧。
        // 顺序保证来自"屏障已经 write 出去、且本处理器 ctx 上的写同样从 tail 方向排队"。
        if (!afterBarrier.isEmpty()) {
            pending.addAll(afterBarrier);
            pendingPromises.addAll(afterBarrierPromises);
            pendingBytes += afterBarrierBytes;
            afterBarrier.clear();
            afterBarrierPromises.clear();
            afterBarrierBytes = 0;
            emitBatch(ctx);
        }
    }

    /** 丢弃屏障队列：释放消息、失败化 promise（通道关闭 / 批次失败时使用）。 */
    private void failBarriers(Throwable cause) {
        for (Object o : barriers) {
            ReferenceCountUtil.release(o);
        }
        barriers.clear();
        for (ChannelPromise p : barrierPromises) {
            p.tryFailure(cause);
        }
        barrierPromises.clear();
        if (!afterBarrier.isEmpty()) {
            releaseAfterBarrier();
            for (ChannelPromise p : afterBarrierPromises) {
                p.tryFailure(cause);
            }
            afterBarrierPromises.clear();
        }
    }

    /** 释放"屏障之后"攒下的包（通道关闭 / 溢出）。 */
    private void releaseAfterBarrier() {
        for (ByteBuf b : afterBarrier) {
            ReferenceCountUtil.release(b);
        }
        afterBarrier.clear();
        afterBarrierBytes = 0;
    }

    /**
     * 直存帧写出：<b>一帧一包</b>，每帧帧体为 {@code [0][packet]}。
     *
     * <p>输入的 {@code raw} 是 {@link #concatPending} 拼好的批缓冲，格式为
     * {@code [varint pktLen][pkt]...}（该格式只用于<b>采样</b>与压缩路径；压缩帧的负载
     * 保持它不变）。这里把它按内层长度前缀重新切开，<b>丢掉</b>前缀、逐个成帧。</p>
     *
     * <p>⚠️ 先把整批校验完再动手写：写了一半才失败，会让"已经写出去的帧"与"被失败化的
     * promise"对不上——上游以为没发出去，其实数据已经在路上。</p>
     */
    private void writeStoredFrames(ChannelHandlerContext ctx, ByteBuf raw, int rawTotal,
                                   boolean withBodyLen, List<ChannelPromise> promises) {
        final int bodyLimit = ZstdVarInts.MC_MAX_FRAME_BYTES - (withBodyLen ? 5 : 0);
        final int base = raw.readerIndex();
        final int end = base + rawTotal;

        int[] cursor = {base};
        while (cursor[0] < end) {
            int start = cursor[0];
            int pktLen = ZstdVarInts.readOr(raw, cursor, ZstdVarInts.INVALID, end);
            if (pktLen <= 0 || cursor[0] + (long) pktLen > end) {
                throw new IllegalStateException("corrupt batch layout at offset " + start
                        + " (pktLen=" + pktLen + ", total=" + rawTotal + ")");
            }
            if (1 + pktLen > bodyLimit) {
                throw new IllegalArgumentException("stored frame " + (1 + pktLen)
                        + "B exceeds MC 21-bit frame limit " + ZstdVarInts.MC_MAX_FRAME_BYTES + "B");
            }
            cursor[0] += pktLen;
        }

        cursor[0] = base;
        int promiseIndex = 0;
        final ChannelHandlerContext target = out(ctx);
        while (cursor[0] < end) {
            int pktStart = cursor[0];
            int pktLen = ZstdVarInts.readOr(raw, cursor, ZstdVarInts.INVALID, end);
            int bodyOff = cursor[0];
            int bodyLen = 1 /* rawSize=0 */ + pktLen;

            ByteBuf frame;
            if (withBodyLen) {
                frame = ctx.alloc().directBuffer(ZstdVarInts.length(bodyLen) + bodyLen);
                ZstdVarInts.write(frame, bodyLen);
            } else {
                frame = ctx.alloc().directBuffer(bodyLen);
            }
            ZstdVarInts.write(frame, 0);
            frame.writeBytes(raw, bodyOff, pktLen);
            onFrame(1 + pktLen, frame.readableBytes(), false);

            // ⚠️ 帧与 promise 是 1:1 的（直存帧一帧一包，promises 与该批包数一一对应），
            // 所以每个 promise 挂到<b>自己那一帧</b>的 write future 上——不能统一挂到
            // "最后一次 flush"，那样单帧失败会被同批其它帧的成功掩盖。
            final ChannelPromise promise = promises.get(promiseIndex++);
            try {
                target.write(frame).addListener(f -> {
                    if (f.isSuccess()) {
                        promise.trySuccess();
                    } else {
                        promise.tryFailure(f.cause());
                    }
                });
            } catch (Throwable t) {
                // 下游 handler 抛异常（例如别的插件用 CancelPacketException 取消出站包）时，
                // frame 还没被任何人持有，必须由这里释放，否则每被取消一帧泄漏一个 direct 缓冲。
                frame.release();
                throw t;
            }
            cursor[0] = bodyOff + pktLen;
            // 直存帧在历史里按 {@code [varint pktLen][pkt]} 记账 —— 与对端解码器补上前缀后的
            // 形态一致（两端的滑窗必须逐字节相同，见 ZstdRollingDict 类注释）。
            feedHistory(raw, pktStart, (bodyOff + pktLen) - pktStart);
        }
        // 一批对应多个帧：全部写完后只 flush 一次（不会因此多出 TCP 段）。
        target.flush();
    }

    /**
     * 压缩帧：{@code [bodyLen?][rawSize][dictGen][blockCount]} + 每块
     * {@code [blockRawLen][chunkLen][chunk]}。
     *
     * <p>单块（blockCount=1）与分块并行共用同一格式，解码端无需区分。每块自带原文长度，
     * 是因为 zstd 帧头里的 contentSize 被关掉了（省字节），没有它就不知道该解出多少。</p>
     */
    private ByteBuf buildCompressedFrame(ChannelHandlerContext ctx, Block[] blocks,
                                         int rawTotal, boolean withBodyLen) {
        int payloadLen = 0;
        for (Block b : blocks) {
            payloadLen += ZstdVarInts.length(b.rawLen) + ZstdVarInts.length(b.chunk.readableBytes())
                    + b.chunk.readableBytes();
        }
        int innerLen = ZstdVarInts.length(rawTotal) + ZstdVarInts.length(inflightDictGen)
                + ZstdVarInts.length(blocks.length) + payloadLen;

        ByteBuf frame;
        if (withBodyLen) {
            frame = ctx.alloc().directBuffer(ZstdVarInts.length(innerLen) + innerLen);
            ZstdVarInts.write(frame, innerLen);
        } else {
            frame = ctx.alloc().directBuffer(innerLen);
        }
        ZstdVarInts.write(frame, rawTotal);
        ZstdVarInts.write(frame, inflightDictGen);
        ZstdVarInts.write(frame, blocks.length);
        for (Block b : blocks) {
            ZstdVarInts.write(frame, b.rawLen);
            ZstdVarInts.write(frame, b.chunk.readableBytes());
            frame.writeBytes(b.chunk, b.chunk.readerIndex(), b.chunk.readableBytes());
        }
        ensureFrameFitsMcLimit(frame, rawTotal);
        onFrame(1 + rawTotal, frame.readableBytes(), true);
        return frame;
    }

    /**
     * 线路帧的<b>硬</b>上限：MC 的外层长度前缀只有 21 bit，超了客户端必断连。
     *
     * <p>能走到这里且仍然超限的只有一种情况：单个包原始就接近/超过帧上限、
     * 且 zstd 压不动（已压缩过的数据）。原版 zlib 在这种输入上同样发不出去，
     * 所以失败化 promise（而不是发出一个对端必然拒绝的帧）是唯一正确的处理。</p>
     *
     * <p>注意判的是<b>最终帧</b>而不是原始字节：几 MB 的 Registry 包压缩后只有几百 KB，
     * 属于完全合法的常态流量，绝不能按原始大小拒绝（见 {@link #maxRawBatchBytes()}）。</p>
     */
    private static void ensureFrameFitsMcLimit(ByteBuf frame, int rawTotal) {
        if (frame.readableBytes() <= ZstdVarInts.MC_MAX_FRAME_BYTES) {
            return;
        }
        int actual = frame.readableBytes();
        frame.release();
        throw new IllegalArgumentException("frame " + actual + "B exceeds MC 21-bit limit "
                + ZstdVarInts.MC_MAX_FRAME_BYTES + "B (raw=" + rawTotal + "B)");
    }

    /**
     * 合批的<b>软</b>目标：原始负载达到这个量就起新帧，<b>但绝不因此拒绝单个包</b>。
     *
     * <p>⚠️ 这里曾经直接把 MC 的 21-bit 帧上限套在<b>原始</b>字节上，并据此拒绝"超限"的包。
     * 那是个会让玩家进不了服的错误：21-bit 约束的是<b>压缩后的外层帧</b>，而配置阶段的
     * Registry 包原始可达数 MB、zstd 压缩后只有几百 KB（解码器本身也允许 rawSize 到 32MB）。
     * 按原始大小拒绝，等于把最需要压缩的大包直接掐死。</p>
     *
     * <p>真正的硬上限只有两条，分别在 {@link #takeFrameBatch()}（对端解码器的 32MB rawSize
     * 上限）与 {@link #buildCompressedFrame}（最终线路帧的 21-bit 上限）里判。</p>
     */
    private int maxRawBatchBytes() {
        int outer = writeBodyLen() ? 5 : 0;
        int maxFrame = ZstdVarInts.MC_MAX_FRAME_BYTES - outer - 5;
        // 再扣掉 zstd 的最坏膨胀量（compressBound = n + n/256 + 64）。
        // 否则"压不动"的满批会产出一个刚好越过 21-bit 的帧，只能整批失败丢包；
        // 少装几个包换"永远发得出去"，对不可压缩流量才是正确取舍。
        // 直存路径（rawTotal < skip 阈值）不受影响：它没有膨胀，且阈值远小于此。
        return maxFrame - (maxFrame >>> 8) - 64;
    }

    private void schedulePendingBatch(ChannelHandlerContext ctx) {
        ctx.executor().execute(() -> emitBatch(ctx));
    }

    private void releasePending() {
        releasePackets(pending);
        pending.clear();
        pendingBytes = 0;
    }

    /**
     * 从队首移除 {@code count} 个包并同步字节计数。
     *
     * <p>⚠️ 字节计数必须与实际出队严格一致：它只用于 {@link #MAX_PENDING_BYTES} 的
     * 防 OOM 判定，一旦漂移，要么误关正常连接，要么让上限形同虚设。因此所有
     * 「从 pending 出队」的地方都必须走这里（或等价的显式扣减），不要再直接
     * {@code pending.subList(...).clear()}。</p>
     */
    private void removePendingPrefix(int count) {
        for (int i = 0; i < count; i++) {
            ByteBuf b = pending.get(i);
            // 读长度必须早于 release：release 之后 buffer 的索引不再可信
            pendingBytes -= b.readableBytes();
            ReferenceCountUtil.release(b);
        }
        pending.subList(0, count).clear();
        pendingPromises.subList(0, count).clear();
    }

    private static void releasePackets(List<ByteBuf> packets) {
        for (ByteBuf b : packets) {
            ReferenceCountUtil.release(b);
        }
    }

    /**
     * 取原始负载缓冲：小批次复用常驻的池化直存缓冲，大帧临时分配（用完即弃）。
     *
     * <p>大帧只在进服时出现（区块 / 注册表），常驻会让"连过的最大批"永久占住内存。</p>
     */
    private ByteBuf ensureRawCapacity(ChannelHandlerContext ctx, int needed) {
        if (needed > SCRATCH_KEEP_LIMIT) {
            return ctx.alloc().directBuffer(needed);
        }
        if (rawBuf == null || rawBuf.capacity() < needed) {
            if (rawBuf != null) {
                rawBuf.release();
            }
            rawBuf = ctx.alloc().directBuffer(Math.max(needed, INITIAL_BUFFER));
        }
        return rawBuf;
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        if (windowTask != null) {
            windowTask.cancel(false);
            windowTask = null;
        }
        // 只有「通道还在、且没有在途压缩任务占用每连接的缓冲」时才有可能补发；
        // 有在途任务时 emitBatch 会直接返回（见那里的说明），于是下面必须自己收尾。
        // ⚠️ 补发必须在置 removed 之前：emitBatch 见到 removed 会直接丢弃残留，
        // 若先置位，这段补发就成了永远不会生效的死代码。
        if (!pending.isEmpty() && !compressing && ctx.channel().isActive()) {
            emitBatch(ctx); // 处理器被移除前把残留数据送出去，避免丢包
        }
        removed = true;
        // ⚠️ 没能补发的残留必须在这里处理干净：本处理器已被移除，emitBatch 的
        //「等压缩回来再发」那条路径不会再走回来，留下的是永不完成的 promise 与永不释放的缓冲
        //（后者是 ReferenceCounted，扣在 pending 里没人替我们 release）。
        if (!pending.isEmpty()) {
            releasePending();
            failPending(new ClosedChannelException());
        }
        pendingPromises.clear();
        failBarriers(new ClosedChannelException());
        failInflight(new ClosedChannelException());
        inflightPromises = null;
        // 上面那次 emitBatch 可能真的起了一个压缩任务：那就不能在这里释放 scratch
        //（池线程可能还没进临界区，会在我们释放之后才读到 rawBuf）。交给回调收尾。
        boolean inflight = compressing;
        compressing = false;
        // ⚠️ 必须在压缩锁内释放：置 compressing=false 只是"不再开新批"，池线程可能仍读着
        // rawBuf 在压缩。此时把它还给 PooledByteBufAllocator，同一块内存会立刻被别的连接
        // 取走改写 —— 池线程读到的是别人的数据（产物随后被丢弃，不会污染对端，但这是一次
        // 跨线程的未定义读写）。
        if (inflight) {
            deferredScratchRelease = true;
            // 滑窗可以先放掉：它只被本类的记账路径读，而 removed 已经置位，不会再有新帧
            rolling.reset();
            appliedRollingDict = null;
            return;
        }
        synchronized (compressLock()) {
            releaseScratch();
        }
        // 滑窗 + 当前字典共 128KB，这条连接（或这个方向）不会再用了，立刻释放
        rolling.reset();
        appliedRollingDict = null;
    }

    /**
     * 仅释放本handler常驻的原文缓冲；临时原文由{@link #completeBatch}单独归还。
     */
    private void releaseScratch() {
        if (rawBuf != null) {
            rawBuf.release();
            rawBuf = null;
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        if (windowTask != null) {
            windowTask.cancel(false);
            windowTask = null;
        }
        releasePending();
        failPending(new ClosedChannelException());
        pendingPromises.clear();
        failBarriers(new ClosedChannelException());
        failInflight(new ClosedChannelException());
        // 注意不在这里 releaseScratch()：channelInactive 之后 handlerRemoved 仍会执行，
        // 放在两处会 double-release（池化缓冲释放两次会让分配器状态错乱）。
        super.channelInactive(ctx);
    }

    /**
     * 通道关闭 / handler 被移除时，把「尚未成帧」的写 promise 统一失败化。
     *
     * <p>promise 必须被完成（否则上游 future 永久挂起），但 <b>cause 必须用 Netty 标准的
     * {@link ClosedChannelException}</b>：这些 promise 上挂着
     * {@code ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE}（Minecraft 发包时挂的），
     * 一旦失败它就会 {@code pipeline().fireExceptionCaught(cause)} 从 pipeline 头部广播出去。
     * 曾经这里 new 的是 {@code IllegalStateException("channel closed")}，于是关服/退服时
     * 必然打出一条 "decoder exception" + 20 行堆栈的假报错。</p>
     */
    private void failPending(ClosedChannelException cause) {
        for (ChannelPromise p : pendingPromises) {
            p.tryFailure(cause);
        }
    }

    /**
     * 把「在途压缩任务」持有的 promise 也失败化。
     *
     * <p>这批 promise 已经被移出 {@code pendingPromises}，通道关闭时若不处理，
     * 上游会一直等一个永远不会完成的 future。任务回调里还会再失败一次，
     * 但 {@code tryFailure} 幂等，重复无害。</p>
     */
    private void failInflight(ClosedChannelException cause) {
        List<ChannelPromise> inflight = inflightPromises;
        if (inflight != null) {
            for (ChannelPromise p : inflight) {
                p.tryFailure(cause);
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // ⚠️ 别的插件用"抛异常"取消某一条出站包（CraftEngine 的 CancelPacketException
        // 就是这种用法：在 encoder 里 throw 以短路 pipeline）。这是控制流，不是连接错误。
        if (isPacketCancellation(cause)) {
            LOGGER.debug("[Zstd] packet cancelled by another plugin: {}", cause.toString());
            return;
        }
        // 连接层 I/O 异常（对端 RST、网络中断）不是编码问题，不打 ERROR 堆栈。
        if (cause instanceof java.io.IOException) {
            LOGGER.debug("[Zstd] connection I/O error on write: {}", cause.toString());
            ctx.close();
            return;
        }
        // 通道已关闭时的异常一律是收尾噪音（本插件在断线时会失败化挂起的写 promise，
        // 那些 promise 的监听器会把异常广播回 pipeline），降为 debug，
        // 避免关服/退服时刷 ERROR + 堆栈。
        if (!ctx.channel().isActive()) {
            LOGGER.debug("[Zstd] encoder exception after channel closed: {}", cause.toString());
        } else {
            LOGGER.error("[Zstd] encoder exception, closing channel", cause);
        }
        ctx.close();
    }

    /** 是否为"发包被其它插件取消"的控制流异常（不是连接错误）。 */
    private static boolean isPacketCancellation(Throwable cause) {
        for (Throwable t = cause; t != null; t = t.getCause()) {
            if (t.getClass().getSimpleName().contains("CancelPacket")) {
                return true;
            }
        }
        return false;
    }
}
