package mikumc.zstd;

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdCompressCtx;
import com.github.luben.zstd.ZstdDictCompress;
import com.github.luben.zstd.ZstdDictTrainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import mikumc.zstd.protocol.ZstdDictId;
import mikumc.zstd.protocol.ZstdVarInts;

import mikumc.zstd.protocol.ZstdDictAdoption.EvalResult;

import static mikumc.zstd.protocol.ZstdDictAdoption.bandOf;
import static mikumc.zstd.protocol.ZstdDictAdoption.bandRatio;
import static mikumc.zstd.protocol.ZstdDictAdoption.pct;
import static mikumc.zstd.protocol.ZstdDictAdoption.shouldAdoptDict;

/**
 * zstd 字典自动训练器（Velocity 端）。
 *
 * <p>样本来源：encoder/decoder 热路径提交。采样在 event loop 线程仅做入环
 * （轻量同步段），训练/评估/持久化全部在<b>同一个单线程执行器</b>上串行完成，
 * 绝不阻塞网络线程，也不存在两个线程同时写同一个文件的竞态。</p>
 *
 * <h2>3.1.0 修复</h2>
 * <ol>
 *   <li><b>样本环不再永久冻结</b>：3.0.0 只在"采纳新字典"分支清空样本环，
 *       而未达改进阈值（默认 3%，真实流量下是常态）时直接 return——此时环已满
 *       （{@code sampleBytes >= sampleSize}），入环会永远早退，
 *       字典从此只会在同一批陈旧样本上重训。现在无论是否采纳，每轮训练结束后
 *       都会归档并清空样本环。</li>
 *   <li><b>历史样本不再重复累积</b>：旧实现每次保存都"读回历史 + 追加整个环 + 写回"，
 *       而环在非采纳路径不清空，导致同一批样本被反复入库（文件线性膨胀、训练集偏斜）。
 *       现在用 {@code savedCount} 游标记录"环中前多少个样本已入库"，只追加增量。</li>
 *   <li><b>持久化串行化</b>：autosave 不再在自己的线程上直接写文件，而是投递到
 *       训练执行器，与 persist() 天然互斥。</li>
 *   <li><b>字典对象共享</b>：采纳/载入后通知 {@link ZstdDictRegistry} 重建进程级
 *       共享条目，连接侧只取引用（每连接省约 3.35MB）。</li>
 *   <li>{@code currentDict} / {@code currentDictId} 改 volatile（训练线程写、
 *       event loop 读）；{@code shutdown()} 等待在途任务结束；评估与字典准备
 *       统一使用运行时压缩等级。</li>
 * </ol>
 */
public class ZstdSampleTrainer {

    private static final Logger LOGGER = LoggerFactory.getLogger("miku-zstd");

    /** 字典方向：OUTBOUND = 服务端压缩用（装入 compressCtx），INBOUND = 解压用 */
    public enum Direction {
        OUTBOUND,
        INBOUND
    }

    private final String name;
    private final Direction direction;
    private final Path dictDir;

    private final List<byte[]> sampleRing = new ArrayList<>(1024);
    /** 环中前 savedCount 个样本已写入历史文件（去重游标） */
    private int savedCount;
    private int sampleBytes;
    /** 环已满且样本数已达 minSamples：让 addBatch 跳过"复制 + 入环"（训练清空环时复位） */
    private volatile boolean ringFull;
    /**
     * 满环早退路径的判定节流时间点。
     *
     * <p>封环后每个批次都会走到 {@link #maybeTrain()}，而它要加锁 + 读时钟；
     * 满环期间这是<b>每批一次</b>的纯浪费（真实流量下每秒数百次）。训练门槛
     * （冷却 5 分钟 / 兜底 10 分钟）远大于 1 秒，节流不会推迟任何实际训练。</p>
     */
    private volatile long nextTrainCheckMs;
    private long lastTrainTime;
    private volatile byte[] currentDict;
    private volatile long currentDictId;
    private final AtomicBoolean training = new AtomicBoolean();

    /**
     * 训练样本缓冲的硬上限（字节）。
     *
     * <p>{@code ZstdDictTrainer} 会在构造时 {@code allocateDirect(capacity)}，
     * 该容量必须覆盖整个样本集，否则 {@code addSample} 返回 false、样本被静默丢弃。
     * 上限只是防"配置异常导致一次分配几个 GB"的兜底：正常情况下样本环由
     * {@code max_samples} 与字节预算双重约束，远小于此值。</p>
     */
    private static final int MAX_TRAIN_SAMPLE_BYTES = 256 * 1024 * 1024;

    private final int maxSamples;
    private final int minSamples;
    /**
     * 样本环的<b>硬</b>字节上限。
     *
     * <p>⚠️ {@code sample_target_bytes} 只在样本数已达 {@code min_samples} 后才是封环条件
     * （见 {@link #addBatch}），所以样本数不足时环会继续长。若没有这条硬上限，
     * "几个 KB~几 MB 的大包 + min_samples=2000"会让环累积到 GB 级，训练时还要
     * {@code allocateDirect} 同等容量 —— 直接把服务端内存打爆。
     * 取 {@code max(sample_target_bytes, 64MB)}：正常配置下永不触发，纯属安全阀。</p>
     */
    private final long hardSampleBytes;
    private final long trainCooldownMs;
    private final long fallbackTimeoutMs;
    private final int dictSize;
    private final int sampleSize;
    private final int maxHistorySamples;
    private final double adoptRatioImprovement;
    private final int pruneMinPayload;
    private final int runtimeLevel;

    private static volatile ZstdSampleTrainer encoderInstance;
    private static volatile ZstdSampleTrainer decoderInstance;
    private static ScheduledExecutorService autoSaveExecutor;
    /** 训练 + 持久化专用单线程执行器：全部磁盘 IO 都在这条线上串行 */
    private static volatile ExecutorService trainExecutor;

    public static void init(Path dataDir) {
        Path dir = dataDir.resolve("zstd_dicts");
        try {
            Files.createDirectories(dir);
        } catch (IOException ignored) {
        }
        ZstdVelocityConfig cfg = ZstdVelocityConfig.INSTANCE;
        trainExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "miku-zstd-trainer");
            t.setDaemon(true);
            return t;
        });
        encoderInstance = new ZstdSampleTrainer("encoder", Direction.OUTBOUND, dir, cfg);
        decoderInstance = new ZstdSampleTrainer("decoder", Direction.INBOUND, dir, cfg);
        encoderInstance.loadFromDisk();
        decoderInstance.loadFromDisk();
        LOGGER.info("[Zstd] Trainers initialized. encoderDictId={} decoderDictId={}",
                encoderInstance.currentDictId, decoderInstance.currentDictId);

        autoSaveExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "miku-zstd-autosave");
            t.setDaemon(true);
            return t;
        });
        // autosave 只负责"到点投递"，真正的写盘在 trainExecutor 上与训练串行
        autoSaveExecutor.scheduleWithFixedDelay(() -> {
            ZstdSampleTrainer enc = encoderInstance;
            ZstdSampleTrainer dec = decoderInstance;
            ExecutorService te = trainExecutor;
            // ⚠️ 必须判 isShutdown：scheduleWithFixedDelay 的任务一旦抛出异常，
            // 后续所有周期执行都会被静默取消。关服时 autoSaveExecutor 与 trainExecutor
            // 的关闭顺序无法保证，若在此刻向已关闭的 trainExecutor 投递任务，
            // RejectedExecutionException 会让这个定时任务永久停摆（虽然此时已在关服，
            // 但同一进程内若只是插件重载，autosave 就真的不再工作了）。
            if (te == null || te.isShutdown()) return;
            try {
                te.execute(() -> {
                    if (enc != null) enc.appendUnsavedSamplesToHistory();
                    if (dec != null) dec.appendUnsavedSamplesToHistory();
                });
            } catch (RejectedExecutionException ignored) {
                // 执行器刚好在此刻关闭：本次归档放弃，shutdown() 里还有一次最终落盘
            }
        }, 5, 5, TimeUnit.MINUTES);
    }

    public static void shutdown() {
        if (autoSaveExecutor != null) {
            autoSaveExecutor.shutdown();
        }
        ExecutorService te = trainExecutor;
        if (te != null) {
            ZstdSampleTrainer enc = encoderInstance;
            ZstdSampleTrainer dec = decoderInstance;
            try {
                // 最后一次落盘也走训练线程，避免与在途训练任务竞态
                te.execute(() -> {
                    if (enc != null) enc.appendUnsavedSamplesToHistory();
                    if (dec != null) dec.appendUnsavedSamplesToHistory();
                });
            } catch (Exception ignored) {
                // 执行器已关闭时忽略
            }
            te.shutdown();
            try {
                if (!te.awaitTermination(5, TimeUnit.SECONDS)) {
                    LOGGER.warn("[Zstd] trainer executor did not terminate in time");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        ZstdDictRegistry.shutdown();
    }

    /**
     * 批量提交一帧（{@code [varint pktLen][pkt]...}）；见 {@link #addBatch}。
     *
     * <p>编码方向与解码方向都走这里：两端各只有这一个入口。早期的"逐包提交"
     * 入口（{@code submitEncoderSample} / {@code submitDecoderSample}）连同它们
     * 依赖的 {@code addSample} 已删除——编码侧改成批量化后就没人调用了，
     * 解码侧也在 3.1.0 补齐成批量提交。</p>
     */
    public static void submitEncoderBatch(byte[] raw, int length) {
        ZstdSampleTrainer t = encoderInstance;
        if (t != null) t.addBatch(raw, length);
    }

    /** 批量提交一帧（解码方向）。 */
    public static void submitDecoderBatch(byte[] raw, int length) {
        ZstdSampleTrainer t = decoderInstance;
        if (t != null) t.addBatch(raw, length);
    }

    public static ZstdSampleTrainer getEncoder() {
        return encoderInstance;
    }

    public static ZstdSampleTrainer getDecoder() {
        return decoderInstance;
    }

    private ZstdSampleTrainer(String name, Direction direction, Path dictDir, ZstdVelocityConfig cfg) {
        this.name = name;
        this.direction = direction;
        this.dictDir = dictDir;
        this.maxSamples = cfg.trainerMaxSamples;
        this.minSamples = cfg.trainerMinSamples;
        this.trainCooldownMs = cfg.trainerCooldownMs;
        this.fallbackTimeoutMs = cfg.trainerFallbackTimeoutMs;
        this.dictSize = cfg.trainerDictMaxBytes;
        this.sampleSize = cfg.trainerSampleTargetBytes;
        this.hardSampleBytes = Math.max((long) sampleSize, 64L * 1024 * 1024);
        this.maxHistorySamples = cfg.trainerMaxHistorySamples;
        this.adoptRatioImprovement = cfg.trainerAdoptionThreshold;
        this.pruneMinPayload = cfg.trainerPruneMinPayload;
        this.runtimeLevel = cfg.level;
        LOGGER.info("[Zstd] {} 字典采纳策略：首部字典改进 > 0 即采纳（否则小包永远压不动）；"
                        + "替换字典需 >= {}（trainer.adoption_threshold）",
                name, pct(this.adoptRatioImprovement));
    }

    public byte[] getCurrentDict() {
        return currentDict;
    }

    public long getCurrentDictId() {
        return currentDictId;
    }

    public synchronized int getSampleCount() {
        return sampleRing.size();
    }

    public synchronized int getSampleBytes() {
        return sampleBytes;
    }

    /**
     * 手动请求一次训练（{@code /mikuzstd train} 入口）。
     *
     * <p>与 {@link #forceTrain()} 的区别：本方法<b>仍要求样本达到 minSamples</b>，
     * 只是绕过冷却与"环满 / 兜底超时"两个门槛。<br>
     * 以前两个子命令都直接调 {@code forceTrain()}，{@code force} 只改了提示文案——
     * 等于一个没有任何作用的子命令。</p>
     *
     * @return 是否真的启动了训练
     */
    public boolean requestTrain() {
        int samples;
        synchronized (this) {
            samples = sampleRing.size();
        }
        if (samples < minSamples) {
            LOGGER.info("[Zstd] {} 样本不足，未训练（{} < {}）；如需忽略门槛请用 train force",
                    name, samples, minSamples);
            return false;
        }
        return forceTrain();
    }

    /**
     * 强制触发训练（{@code /mikuzstd train force}），绕过采样门槛与冷却。
     *
     * @return 是否<b>确实</b>提交了训练任务：执行器已关闭（关服 / 插件停用）或已有训练在跑时
     *         返回 {@code false} —— 调用方据此给出诚实的提示，而不是一律回"已触发"。
     */
    public boolean forceTrain() {
        ExecutorService te = trainExecutor;
        if (te == null) return false;
        if (!training.compareAndSet(false, true)) {
            LOGGER.info("[Zstd] {} training already in progress", name);
            return false;
        }
        int count;
        synchronized (this) {
            lastTrainTime = System.currentTimeMillis();
            count = sampleRing.size();
        }
        // ⚠️ 必须把"低于门槛"的结论带进训练线程：force 的语义就是绕过样本门槛，
        // 而 trainAndAdopt 在样本不足时默认会直接 return。曾经这里固定传 false，
        // 于是 /mikuzstd train force 在样本不足时只会打一条 "Training triggered"，
        // 实际什么也没做 —— 命令与文档承诺的行为不一致。
        final boolean belowMin = count < minSamples;
        // ⚠️ 提交失败必须把 training 复位：执行器已 shutdown（关服 / 插件停用）时
        // execute 会抛 RejectedExecutionException，而 trainAndAdopt 的 finally 永不执行 ——
        // training 就此卡在 true，此后**所有**训练请求（含自动触发）都被"already in progress"挡掉，
        // 字典机制静默失效。这类"标志位只在成功路径复位"的缺陷不会报错，只会让功能悄悄死掉。
        try {
            te.execute(() -> trainAndAdopt(belowMin));
            return true;
        } catch (RejectedExecutionException e) {
            training.set(false);
            LOGGER.debug("[Zstd] {} training not submitted (executor shut down)", name);
            return false;
        }
    }

    /**
     * 拆分整帧并批量入环：帧格式为 {@code [varint pktLen][pkt]...}。
     *
     * <p>必须按包拆分——整批当一个样本的话，样本"个数"增长极慢（实测 1MB 数据只有 26 个），
     * 永远够不到 min_samples，字典训练形同虚设。</p>
     *
     * <p>要点：把「解析 + 过滤 + 复制」放在<b>锁外</b>完成，
     * 只在入环那一刻加一次锁。原来一帧几十个包就要几十次加锁 + 几十次训练判定，
     * 网络线程上这是白白的争用。（解析不放进锁内，是为了不让它拖长持锁时间。）</p>
     */
    private void addBatch(byte[] raw, int length) {
        if (raw == null || length <= 0) {
            return;
        }
        if (ringFull) {
            // 环满且样本数已够：跳过下面的"复制 + 入环"。下轮训练最长要等 5~10 分钟，
            // 这期间每个包都白拷一次、而且全在网络线程上。
            // ⚠️ 训练判定**不能省**：否则满环后再也走不到 maybeTrain，环永远清不掉
            // —— 3.1.0 修过同类的"样本环永久冻结"，这里不能倒退回去。
            // 但也不需要每批都判：训练门槛以分钟计，1 秒节流不会漏掉任何一次该训的时机。
            long now = System.currentTimeMillis();
            if (now >= nextTrainCheckMs) {
                nextTrainCheckMs = now + 1000;
                maybeTrain();
            }
            return;
        }
        List<byte[]> batch = null;
        int[] cursor = {0};
        while (cursor[0] < length) {
            // 共享的 varint 读取器：以前这里手写了一遍，与其它 5 处各自实现等价但可能漂移
            int pktLen = ZstdVarInts.readOr(raw, cursor, ZstdVarInts.INVALID);
            // ⚠️ pktLen 来自对端（最大 2^31-1），必须用 long 相加：int 溢出会让越界判断失效，
            // 紧接着的 new byte[pktLen] 就是一次 2GB 分配（远程 OOM），在 event loop 上直接打崩。
            if (pktLen <= 0 || cursor[0] + (long) pktLen > length) {
                break;
            }
            int start = cursor[0];
            cursor[0] += pktLen;
            // ⚠️ 先判定、后复制：绝大多数包（KeepAlive、短状态包）会被过滤掉，
            // 先 copy 再判定等于在网络线程上为它们白分配一次数组。
            if (!mikumc.zstd.protocol.ZstdSampleFilter.shouldKeep(raw, start, pktLen, pruneMinPayload)) {
                continue;
            }
            byte[] sample = new byte[pktLen];
            System.arraycopy(raw, start, sample, 0, pktLen);
            if (batch == null) {
                batch = new ArrayList<>(16);
            }
            batch.add(sample);
        }
        if (batch == null) {
            return;
        }

        int size;
        boolean budgetExhausted = false;
        synchronized (this) {
            for (byte[] s : batch) {
                // ⚠️ 字节预算只有在**样本数已达门槛**后才生效。
                // 否则两者会互相矛盾：默认 sample_target_bytes=1MB、min_samples=2000，
                // 而真实流量平均包长有几 KB —— 1MB 只装得下 200~300 个样本，
                // 于是每轮都在"样本数远低于门槛"的状态下训练，字典质量长期偏低，
                // 且每次只训几百个样本就重来一遍（CPU 与日志都被白白消耗）。
                // 样本数不足时让环继续长，由 max_samples 与 hardSampleBytes 兜住内存上限。
                if ((sampleBytes >= sampleSize && sampleRing.size() >= minSamples)
                        || sampleBytes >= hardSampleBytes) {
                    budgetExhausted = true;
                    ringFull = true; // 封环：此后每批走早退路径，不再白拷贝
                    break;
                }
                sampleRing.add(s);
                sampleBytes += s.length;
                if (sampleRing.size() >= maxSamples) {
                    ringFull = true; // 环容量已满，等训练清空
                    break;
                }
            }
            size = sampleRing.size();
        }
        if (size >= minSamples || budgetExhausted) {
            maybeTrain();
        }
    }

    /**
     * 判断是否满足训练条件（满足则以 training 原子标志占位，异步执行）。
     */
    private void maybeTrain() {
        final boolean belowMin;
        synchronized (this) {
            int count = sampleRing.size();
            if (count == 0) return;
            long now = System.currentTimeMillis();
            // 门槛口径必须与 addBatch 的封环条件一致：字节预算只在对样本数够时才算"满"。
            boolean full = (sampleBytes >= sampleSize && count >= minSamples)
                    || count >= maxSamples || sampleBytes >= hardSampleBytes;
            if (count < minSamples && !full) return;
            // ⚠️ 冷却是**所有**路径共用的闸门。曾经只在"定时/满环"路径上判，跳过它会让
            // 清空样本环后新样本很快又攒满 → 再训练 → 无限循环，CPU 全耗在训练与评估上。
            if (now - lastTrainTime < trainCooldownMs) return;
            boolean timedOut = (now - lastTrainTime) >= fallbackTimeoutMs;
            // 容量已满但样本数不足门槛（仅当 min_samples > max_samples 之类配置异常）时，
            // 也必须放行，否则环永远清不掉 —— 见 addBatch 的注释。
            if (count >= minSamples && !full && !timedOut) return;
            if (!training.compareAndSet(false, true)) return;
            lastTrainTime = now;
            belowMin = count < minSamples;
        }
        ExecutorService te = trainExecutor;
        if (te == null) {
            training.set(false);
            return;
        }
        // 同 forceTrain：提交被拒（执行器已 shutdown）时必须复位标志，否则训练永久卡死
        try {
            te.execute(() -> trainAndAdopt(belowMin));
        } catch (RejectedExecutionException e) {
            training.set(false);
        }
    }

    /**
     * 训练 + 评估 + 采纳 + 归档 + 清空样本环（训练线程调用）。
     *
     * <p>关键：<b>无论是否采纳新字典，样本环都会归档并清空</b>。否则环一旦攒满，
     * 入环时的容量早退会让它永久拒绝新样本，字典再无进化可能。</p>
     *
     * @param allowBelowMin {@code true} 表示"字节预算/环容量已满，样本数不足也先训一轮"。
     *                      这条路径<b>必须</b>同样走完归档+清空——否则环仍然是满的，
     *                      下一批又会触发，形成"反复空转 + 刷日志"。
     */
    private void trainAndAdopt(boolean allowBelowMin) {
        try {
            List<byte[]> snapshot;
            synchronized (this) {
                snapshot = new ArrayList<>(sampleRing);
            }
            if (snapshot.isEmpty()) {
                return;
            }
            if (snapshot.size() < minSamples) {
                if (!allowBelowMin) {
                    // DEBUG：周期任务每轮都会走到这里，INFO 会把控制台刷满
                    LOGGER.debug("[Zstd] {} not enough samples to train ({} < {})",
                            name, snapshot.size(), minSamples);
                    return;
                }
                // 低于门槛但预算已满：**照常训练**，而不是把样本丢掉。
                // 训练器本身对样本数没有硬要求（少则训出的字典质量差、甚至训不出，
                // 那会走下面的"训练结果为空"分支并照常清空环）；而采纳环节还有
                // shouldAdoptDict 把关，质量不够的字典不会替换现有字典。
                // 关键是**必须走完归档 + 清空环**，否则环一直是满的，下一批又会触发，
                // 形成"反复空转 + 刷日志"，且新样本永远进不来。
                LOGGER.debug("[Zstd] {} 样本数低于门槛（{} < {}），但预算已满：仍训练一轮并清空样本环",
                        name, snapshot.size(), minSamples);
            }

            // ⚠️ 周期训练每 5 分钟一轮、两个方向各一轮：只有"字典被采纳"这一条留 INFO
            // （它意味着线路上的字典真的换了），其余步骤一律 DEBUG。
            // 曾经的 4 行/轮（开始 / 重建 / 采纳 / 结束）会把控制台刷满。
            int totalBytes = snapshot.stream().mapToInt(s -> s.length).sum();
            LOGGER.debug("[Zstd] {} training with {} samples ({} bytes)", name, snapshot.size(), totalBytes);

            byte[] newDict = train(snapshot);
            boolean adopted = false;

            if (newDict == null || newDict.length == 0) {
                LOGGER.warn("[Zstd] {} training produced empty dict", name);
            } else {
                long dictId = ZstdDictId.of(newDict);

                if (currentDict != null && currentDictId == dictId) {
                    LOGGER.debug("[Zstd] {} dict unchanged, skipping", name);
                } else {
                    EvalResult ev = evaluate(snapshot, newDict);
                    // ⚠️ 首部字典与替换字典的判定标准必须不同：
                    //   · 首部字典（currentDict == null）：只要不劣化就采纳。因为无字典时
                    //     小包压缩后反而膨胀（实测 ≤32B 为 102~113%），字典是让小包压缩
                    //     变得可行的前提；而"按总字节加权"的改进往往只有 1~3%，
                    //     用替换字典的阈值去卡它 → 字典永远无法启用，整条字典机制变成死代码。
                    //   · 替换字典：仍按配置阈值，避免频繁换字典带来的抖动与连接侧重建。
                    boolean firstDict = (currentDict == null);
                    boolean accept = shouldAdoptDict(firstDict, ev.overall, adoptRatioImprovement);
                    if (!accept) {
                        LOGGER.debug("[Zstd] {} dict insufficient improvement: {} (阈值 {}，分档：{})",
                                name, pct(ev.overall), firstDict ? "首部字典需>0" : pct(adoptRatioImprovement),
                                ev.bandSummary());
                    } else {
                        synchronized (this) {
                            currentDict = newDict;
                            currentDictId = dictId;
                        }
                        publishToRegistry();
                        adopted = true;
                        LOGGER.info("[Zstd] {} dict adopted: id={} size={}B improvement={}{} samples={}（{}B）分档：{}",
                                name, dictId, newDict.length, pct(ev.overall),
                                firstDict ? "（首部字典，只要不劣化即采纳）" : "",
                                snapshot.size(), totalBytes, ev.bandSummary());
                    }
                }
            }

            // 先把未入库样本归档（无论采纳与否），再清空样本环
            persist(adopted);
            synchronized (this) {
                sampleRing.clear();
                sampleBytes = 0;
                ringFull = false;
                nextTrainCheckMs = 0;
                savedCount = 0;
            }
        } catch (Exception e) {
            LOGGER.error("[Zstd] {} train task failed", name, e);
        } finally {
            training.set(false);
        }
    }

    /** 把当前字典发布到进程级共享注册表（连接侧只取引用，不重建字典对象）。 */
    private void publishToRegistry() {
        byte[] dict = currentDict;
        if (dict == null || dict.length == 0) return;
        if (direction == Direction.OUTBOUND) {
            ZstdDictRegistry.updateEncoder(currentDictId, dict, runtimeLevel);
        } else {
            ZstdDictRegistry.updateDecoder(currentDictId, dict);
        }
    }

    private byte[] train(List<byte[]> samples) {
        try {
            // ⚠️ ZstdDictTrainer 的第一个参数是<b>训练样本缓冲的容量</b>，不是"目标样本量"。
            // 曾经这里传的是 sample_target_bytes（1MB）：一旦样本环超过 1MB（现在会为了
            // 满足 min_samples 而继续增长），addSample 就开始返回 false、下面的 break
            // 会把<b>剩余样本全部静默丢掉</b>——字典始终只由最前面那 1MB 训练。
            // 这里按实际样本总字节分配（并设上限防止极端配置把训练线程的内存打爆）。
            long total = 0;
            for (byte[] s : samples) {
                total += s.length;
            }
            int capacity = (int) Math.min(Math.max(total, 1), MAX_TRAIN_SAMPLE_BYTES);
            ZstdDictTrainer trainer = new ZstdDictTrainer(capacity, dictSize);
            for (byte[] s : samples) {
                if (!trainer.addSample(s)) {
                    break; // 达到 capacity 上限（仅可能发生在样本总量超过上限时）
                }
            }
            return trainer.trainSamples();
        } catch (Exception e) {
            LOGGER.error("[Zstd] {} training failed", name, e);
            return null;
        }
    }


    /**
     * 用运行时压缩等级评估新旧字典；字典对象按运行时等级准备并及时释放。
     *
     * <p>评估上下文与真实编码器保持<b>同样的帧头精简设置</b>（magicless / 无 contentSize /
     * 无 dictID），否则测出来的是带 magic 的完整帧开销，与实际线路字节不符。</p>
     */
    private EvalResult evaluate(List<byte[]> samples, byte[] newDict) {
        ZstdCompressCtx oldCtx = null;
        ZstdCompressCtx newCtx = null;
        ZstdDictCompress oldDictObj = null;
        ZstdDictCompress newDictObj = null;
        try {
            byte[] oldDict = currentDict;
            oldCtx = new ZstdCompressCtx();
            newCtx = new ZstdCompressCtx();
            for (ZstdCompressCtx c : new ZstdCompressCtx[]{oldCtx, newCtx}) {
                c.setLevel(runtimeLevel);
                c.setMagicless(true);
                c.setContentSize(false);
                c.setDictID(false);
            }
            if (oldDict != null && oldDict.length > 0) {
                oldDictObj = new ZstdDictCompress(oldDict, runtimeLevel);
                oldCtx.loadDict(oldDictObj);
            }
            if (newDict.length > 0) {
                newDictObj = new ZstdDictCompress(newDict, runtimeLevel);
                newCtx.loadDict(newDictObj);
            }

            long oldTotal = 0;
            long newTotal = 0;
            long[] oldBand = new long[3];
            long[] newBand = new long[3];
            int testCount = Math.min(samples.size(), 500);
            for (int i = 0; i < testCount; i++) {
                byte[] s = samples.get(i);
                int band = bandOf(s.length);
                int o = oldCtx.compress(s).length;
                int n = newCtx.compress(s).length;
                oldTotal += o;
                newTotal += n;
                oldBand[band] += o;
                newBand[band] += n;
            }

            if (oldTotal == 0) return EvalResult.none();
            return new EvalResult(
                    1.0 - (double) newTotal / oldTotal,
                    bandRatio(oldBand[0], newBand[0]),
                    bandRatio(oldBand[1], newBand[1]),
                    bandRatio(oldBand[2], newBand[2]));
        } catch (Exception e) {
            LOGGER.warn("[Zstd] {} evaluation failed", name, e);
            return EvalResult.none();
        } finally {
            if (oldCtx != null) oldCtx.close();
            if (newCtx != null) newCtx.close();
            if (oldDictObj != null) oldDictObj.close();
            if (newDictObj != null) newDictObj.close();
        }
    }

    /** 归档字典（可选）与未入库样本（训练线程调用，与训练串行）。 */
    private void persist(boolean writeDict) {
        try {
            if (writeDict) {
                writeDictFile();
            }
            appendUnsavedSamplesToHistory();
        } catch (IOException e) {
            LOGGER.error("[Zstd] {} persist failed", name, e);
        }
    }

    private void writeDictFile() throws IOException {
        byte[] dict = currentDict;
        if (dict == null || dict.length == 0) return;

        Path target = dictDir.resolve(name + "_dict.bin");
        Path tmp = dictDir.resolve(name + "_dict.bin.tmp");
        Files.write(tmp, dict);
        Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);

        // 清理旧格式/历史字典文件，避免目录积累与误加载
        try (var stream = Files.list(dictDir)) {
            for (Path p : (Iterable<Path>) stream.filter(p -> {
                String fn = p.getFileName().toString();
                return fn.startsWith(name + "_dict_") || fn.equals(name + "_dict.bin.tmp");
            })::iterator) {
                Files.deleteIfExists(p);
            }
        }
    }

    /**
     * 只把环中"尚未入库"的增量样本追加到历史文件。
     *
     * <p>旧实现每次都把整个环重新追加到读回的历史之后，而环在非采纳路径不清空，
     * 于是同一批样本被反复写入（文件线性膨胀、训练集严重偏斜）。</p>
     */
    private void appendUnsavedSamplesToHistory() {
        List<byte[]> unsaved;
        int upto;
        synchronized (this) {
            upto = sampleRing.size();
            if (savedCount >= upto) return;
            unsaved = new ArrayList<>(sampleRing.subList(savedCount, upto));
        }
        try {
            List<byte[]> history = loadHistory();
            history.addAll(unsaved);
            // 裁剪到上限。⚠️ 必须用 subList().clear() 而不是循环 remove(0)：
            // 后者每次都把整个列表前移一位，history 上限 20000、单轮新增样本可达数千，
            // 于是这一步在最坏情况下是 O(n²)（上亿次元素搬移）。它跑在训练线程上、
            // 会拖长归档耗时，也让"训练期间磁盘/CPU 占用"看起来莫名偏高。
            if (history.size() > maxHistorySamples) {
                history.subList(0, history.size() - maxHistorySamples).clear();
            }
            writeSamples(history);
            synchronized (this) {
                savedCount = Math.max(savedCount, upto);
            }
            LOGGER.debug("[Zstd] {} archived {} new samples (history={})", name, unsaved.size(), history.size());
        } catch (IOException e) {
            LOGGER.warn("[Zstd] {} sample archive failed", name, e);
        }
    }

    private void loadFromDisk() {
        try {
            Path dictFile = dictDir.resolve(name + "_dict.bin");
            if (Files.exists(dictFile)) {
                currentDict = Files.readAllBytes(dictFile);
                currentDictId = ZstdDictId.of(currentDict);
                LOGGER.info("[Zstd] {} loaded dict from disk: id={} size={}", name, currentDictId, currentDict.length);
            } else {
                // 迁移：旧版 *_dict_<crc>.bin 文件按修改时间取最新
                try (var stream = Files.list(dictDir)) {
                    List<Path> legacy = stream
                            .filter(p -> p.getFileName().toString().startsWith(name + "_dict_"))
                            .sorted(Comparator.comparing(this::lastModified))
                            .toList();
                    if (!legacy.isEmpty()) {
                        Path latest = legacy.get(legacy.size() - 1);
                        currentDict = Files.readAllBytes(latest);
                        currentDictId = ZstdDictId.of(currentDict);
                        LOGGER.info("[Zstd] {} loaded legacy dict {} (by mtime): id={} size={}",
                                name, latest.getFileName(), currentDictId, currentDict.length);
                    }
                }
            }

            List<byte[]> history = loadHistory();
            if (!history.isEmpty()) {
                synchronized (this) {
                    sampleRing.addAll(history);
                    for (byte[] s : history) sampleBytes += s.length;
                    savedCount = sampleRing.size(); // 历史已在盘上，无需重复归档
                }
                LOGGER.info("[Zstd] {} loaded {} history samples from disk", name, history.size());
            }

            // 启动即把磁盘字典发布到共享注册表，连接激活时可直接取用
            publishToRegistry();
        } catch (IOException e) {
            LOGGER.warn("[Zstd] {} failed to load from disk", name, e);
        }
    }

    private long lastModified(Path p) {
        try {
            return Files.getLastModifiedTime(p).toMillis();
        } catch (IOException e) {
            return 0L;
        }
    }

    /** 历史样本存储；3.0.0 起 zstd 压缩（*_samples.zst），旧 *.dat 忽略。 */
    private List<byte[]> loadHistory() throws IOException {
        Path samplesFile = dictDir.resolve(name + "_samples.zst");
        if (!Files.exists(samplesFile)) return new ArrayList<>();
        byte[] compressed = Files.readAllBytes(samplesFile);
        // ⚠️ 这里必须连 RuntimeException 一起兜住：文件被截断/手改/磁盘损坏时，
        // Zstd.getFrameContentSize / decompress 抛的是 ZstdException（RuntimeException），
        // 而调用方（init → loadFromDisk）只声明了 IOException —— 一个坏文件会直接
        // 让插件初始化失败，表现为「装了这个插件服务器起不来」。
        // 归档只是优化手段，读不出来就当没有历史，绝不该阻断启动。
        try {
            Long sizeHint = Zstd.getFrameContentSize(compressed);
            if (sizeHint == null || sizeHint <= 0 || sizeHint > 256L * 1024 * 1024) {
                return new ArrayList<>();
            }
            byte[] data = Zstd.decompress(compressed, sizeHint.intValue());

            List<byte[]> result = new ArrayList<>();
            ByteBuffer buf = ByteBuffer.wrap(data);
            while (buf.remaining() >= 4) {
                int len = buf.getInt();
                if (len <= 0 || len > buf.remaining()) break;
                byte[] sample = new byte[len];
                buf.get(sample);
                result.add(sample);
            }
            return result;
        } catch (RuntimeException e) {
            LOGGER.warn("[Zstd] {} 历史样本文件损坏，已忽略（{}）", name, e.toString());
            return new ArrayList<>();
        }
    }

    private void writeSamples(List<byte[]> samples) throws IOException {
        // 用 long 累加后做上限保护：样本总量理论上可能超过 int 范围
        long total = 0;
        for (byte[] s : samples) {
            total += s.length + 4L;
        }
        if (total <= 0 || total > Integer.MAX_VALUE - 8) {
            throw new IOException("sample history too large: " + total + " bytes");
        }
        ByteBuffer buf = ByteBuffer.allocate((int) total);
        for (byte[] s : samples) {
            buf.putInt(s.length);
            buf.put(s);
        }
        byte[] compressed = Zstd.compress(buf.array(), 3); // 存储用低等级：速度优先
        Path target = dictDir.resolve(name + "_samples.zst");
        Path tmp = dictDir.resolve(name + "_samples.zst.tmp");
        Files.write(tmp, compressed);
        Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }
}
