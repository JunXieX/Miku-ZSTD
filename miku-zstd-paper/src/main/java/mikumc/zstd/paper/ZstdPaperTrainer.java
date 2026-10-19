package mikumc.zstd.paper;

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
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import mikumc.zstd.protocol.ZstdDictAdoption.EvalResult;

import static mikumc.zstd.protocol.ZstdDictAdoption.bandOf;
import static mikumc.zstd.protocol.ZstdDictAdoption.bandRatio;
import static mikumc.zstd.protocol.ZstdDictAdoption.pct;
import static mikumc.zstd.protocol.ZstdDictAdoption.shouldAdoptDict;
import mikumc.zstd.protocol.ZstdDictAdoption;

/**
 * zstd 字典自动训练器（Paper 端，参照 Velocity 端 {@code ZstdSampleTrainer} 移植）。
 *
 * <p>与 Velocity 端的差异：Paper 端以<b>单个进程</b>跑一台服务器，采样来自本机全部连接，
 * 因此不需要跨端的字典共享策略差异；持久化与采纳规则保持一致：</p>
 * <ul>
 *   <li>首部字典"只要不劣化就采纳"（否则按总字节加权只有 1~3% 的改进会让字典永远出不来）；</li>
 *   <li>替换字典按阈值把关；</li>
 *   <li>每轮训练结束都把样本环归档并清空，避免环冻结导致字典不再进化；</li>
 *   <li>训练/评估/落盘全部在单独线程，不阻塞网络线程。</li>
 * </ul>
 */
public final class ZstdPaperTrainer {

    private static final Logger LOGGER = LoggerFactory.getLogger("miku-zstd");

    public enum Direction {
        /** 服务端压缩方向（装入 compressCtx），推给客户端解压 */
        OUTBOUND,
        /** 服务端解压方向（装入 decompressCtx），解客户端压缩的数据 */
        INBOUND
    }

    private static volatile ZstdPaperTrainer encoderInstance;
    private static volatile ZstdPaperTrainer decoderInstance;
    private static volatile ExecutorService trainExecutor;
    private static volatile ScheduledExecutorService archiveExecutor;

    private final String name;
    private final Direction direction;
    private final Path dictDir;
    private final List<byte[]> ring = new ArrayList<>(1024);
    private int savedCount;
    private int sampleBytes;
    /** 环已满且样本数已达 minSamples：让 addBatch 跳过"复制 + 入环"（训练清空环时复位） */
    private volatile boolean ringFull;
    /** 满环早退路径的判定节流点：训练门槛以分钟计，1 秒节流不会漏掉任何该训的时机 */
    private volatile long nextTrainCheckMs;
    private long lastTrainTime;
    private volatile byte[] currentDict;
    private volatile long currentDictId;
    private final AtomicBoolean training = new AtomicBoolean();

    /** 训练样本缓冲硬上限（防止配置异常时一次 allocateDirect 几个 GB）；见 {@link #train}。 */
    private static final int MAX_TRAIN_SAMPLE_BYTES = 256 * 1024 * 1024;

    private final int maxSamples;
    private final int minSamples;
    /**
     * 样本环的硬字节上限：{@code sample_target_bytes} 只在样本数达标后才封环，
     * 样本数不足时环会继续长——没有这条安全阀，大包 + 高 min_samples 会累积到 GB 级。
     */
    private final long hardSampleBytes;
    private final int dictSize;
    private final int sampleSize;
    private final int maxHistorySamples;
    private final int pruneMinPayload;
    private final int runtimeLevel;
    private final long cooldownMs;
    private final long fallbackTimeoutMs;
    private final double adoptThreshold;

    public static void init(Path dataDir) {
        Path dir = dataDir.resolve("zstd_dicts");
        try {
            Files.createDirectories(dir);
        } catch (IOException ignored) {
            // 目录已存在
        }
        ZstdPaperConfig cfg = ZstdPaperConfig.INSTANCE;
        trainExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "miku-zstd-paper-trainer");
            t.setDaemon(true);
            return t;
        });
        archiveExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "miku-zstd-paper-archive");
            t.setDaemon(true);
            return t;
        });
        encoderInstance = new ZstdPaperTrainer("encoder", Direction.OUTBOUND, dir, cfg);
        decoderInstance = new ZstdPaperTrainer("decoder", Direction.INBOUND, dir, cfg);
        encoderInstance.loadFromDisk();
        decoderInstance.loadFromDisk();
        LOGGER.info("[Zstd] Paper 字典训练器就绪: encoderDictId={} decoderDictId={}",
                encoderInstance.currentDictId, decoderInstance.currentDictId);
        // 周期归档样本（投递到训练线程，与训练/落盘天然串行）
        archiveExecutor.scheduleWithFixedDelay(() -> {
            ExecutorService te = trainExecutor;
            if (te == null || te.isShutdown()) return;
            // ⚠️ 必须捕获 RejectedExecutionException：scheduleWithFixedDelay 的任务一旦抛出异常，
            // 后续所有周期执行都会被静默取消。关服 / 插件停用时执行器的关闭顺序无法保证，
            // 若此刻投递就会让归档任务永久停摆（与 Velocity 端同一处修正）。
            try {
                te.execute(ZstdPaperTrainer::saveAll);
            } catch (RejectedExecutionException ignored) {
                // 执行器刚好在此刻关闭：本次归档放弃，shutdown() 里还有一次最终落盘
            }
        }, 5, 5, TimeUnit.MINUTES);
    }

    public static void shutdown() {
        ScheduledExecutorService ae = archiveExecutor;
        if (ae != null) {
            ae.shutdown();
        }
        ExecutorService te = trainExecutor;
        if (te != null) {
            try {
                te.execute(ZstdPaperTrainer::saveAll);
            } catch (Exception ignored) {
                // 执行器已关闭
            }
            te.shutdown();
            try {
                te.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        ZstdPaperDictRegistry.shutdown();
    }

    private static void saveAll() {
        ZstdPaperTrainer e = encoderInstance;
        ZstdPaperTrainer d = decoderInstance;
        if (e != null) e.archiveUnsaved();
        if (d != null) d.archiveUnsaved();
    }

    /**
     * 批量提交一帧（{@code [varint pktLen][pkt]...}）；见 {@link #addBatch}。
     *
     * <p>编码与解码两个方向都走这里。早期的"逐包提交"入口
     *（{@code submitEncoderSample} / {@code submitDecoderSample}）及其依赖的
     * {@code addSample} 已删除——编码侧批量化后就没人调用，解码侧也已改为批量提交。</p>
     */
    public static void submitEncoderBatch(byte[] raw, int length) {
        ZstdPaperTrainer t = encoderInstance;
        if (t != null) t.addBatch(raw, length);
    }

    /** 批量提交一帧（解码方向）。 */
    public static void submitDecoderBatch(byte[] raw, int length) {
        ZstdPaperTrainer t = decoderInstance;
        if (t != null) t.addBatch(raw, length);
    }

    public static ZstdPaperTrainer getEncoder() {
        return encoderInstance;
    }

    public static ZstdPaperTrainer getDecoder() {
        return decoderInstance;
    }

    private ZstdPaperTrainer(String name, Direction direction, Path dictDir, ZstdPaperConfig cfg) {
        this.name = name;
        this.direction = direction;
        this.dictDir = dictDir;
        this.maxSamples = cfg.trainerMaxSamples;
        this.minSamples = cfg.trainerMinSamples;
        this.cooldownMs = cfg.trainerCooldownMs;
        this.fallbackTimeoutMs = cfg.trainerFallbackTimeoutMs;
        this.dictSize = cfg.trainerDictMaxBytes;
        this.sampleSize = cfg.trainerSampleTargetBytes;
        this.hardSampleBytes = Math.max((long) sampleSize, 64L * 1024 * 1024);
        this.maxHistorySamples = cfg.trainerMaxHistorySamples;
        this.adoptThreshold = cfg.trainerAdoptionThreshold;
        this.pruneMinPayload = cfg.trainerPruneMinPayload;
        this.runtimeLevel = cfg.level;
    }

    public byte[] getCurrentDict() {
        return currentDict;
    }

    public long getCurrentDictId() {
        return currentDictId;
    }

    public synchronized int getSampleCount() {
        return ring.size();
    }

    public synchronized int getSampleBytes() {
        return sampleBytes;
    }

    /**
     * 拆分整帧并批量入环：帧格式为 {@code [varint pktLen][pkt]...}。
     *
     * <p>必须按包拆分——整批当一个样本的话，样本"个数"增长极慢，永远够不到 min_samples，
     * 字典训练形同虚设。</p>
     *
     * <p>要点：「解析 + 过滤 + 复制」都在<b>锁外</b>完成，
     * 只在入环那一刻加一次锁。原来一帧几十个包就要几十次加锁与训练判定，
     * 在 Netty 网络线程上是白白的争用。</p>
     */
    private void addBatch(byte[] raw, int length) {
        if (raw == null || length <= 0) {
            return;
        }
        if (ringFull) {
            // 环满且样本数已够：跳过下面的"复制 + 入环"，这期间的每包拷贝都是白费，
            // 而且全在 Netty 网络线程上。
            // ⚠️ 训练判定**不能省**：否则满环后再也走不到 maybeTrain，环永远清不掉。
            // 但也不需要每批都判：训练门槛以分钟计，1 秒节流不会漏掉任何该训的时机。
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
            // 共享的 varint 读取器（与 Velocity 端、解码器基类同一份实现）
            int pktLen = mikumc.zstd.protocol.ZstdVarInts.readOr(
                    raw, cursor, mikumc.zstd.protocol.ZstdVarInts.INVALID);
            // ⚠️ pktLen 来自对端（最大 2^31-1），必须用 long 相加：int 溢出会让越界判断失效，
            // 紧接着的 new byte[pktLen] 就是一次 2GB 分配（远程 OOM），在 Netty 线程上直接打崩。
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
                // ⚠️ 字节预算只有在**样本数已达门槛**后才生效。否则两者互相矛盾：
                // 默认 sample_target_bytes=1MB、min_samples=2000，而真实流量平均包长有几 KB
                // —— 1MB 只装得下 200~300 个样本，于是每轮都在样本数远低于门槛时训练，
                // 字典质量长期偏低，且每次都重来一遍（CPU 与日志都被白白消耗）。
                // 样本数不足时让环继续长，由 max_samples 兜住内存上限。
                if ((sampleBytes >= sampleSize && ring.size() >= minSamples)
                        || sampleBytes >= hardSampleBytes) {
                    budgetExhausted = true;
                    ringFull = true; // 封环：此后每批走早退路径，不再白拷贝
                    break;
                }
                ring.add(s);
                sampleBytes += s.length;
                if (ring.size() >= maxSamples) {
                    ringFull = true; // 环容量已满，等训练清空
                    break;
                }
            }
            size = ring.size();
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
            int count = ring.size();
            if (count == 0) return;
            long now = System.currentTimeMillis();
            // 门槛口径必须与 addBatch 的封环条件一致：字节预算只在对样本数够时才算"满"。
            boolean full = (sampleBytes >= sampleSize && count >= minSamples)
                    || count >= maxSamples || sampleBytes >= hardSampleBytes;
            if (count < minSamples && !full) return;
            // ⚠️ 冷却是**所有**路径共用的闸门。曾经只在"定时/满环"路径上判，跳过它会让
            // 清空样本环后新样本很快又攒满 → 再训练 → 无限循环，CPU 全耗在训练与评估上。
            if (now - lastTrainTime < cooldownMs) return;
            boolean timedOut = (now - lastTrainTime) >= fallbackTimeoutMs;
            // 容量已满但样本数不足门槛（仅当配置异常）时也必须放行，否则环永远清不掉。
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
        // ⚠️ 提交被拒（执行器已 shutdown，关服 / 插件停用时）必须复位 training：
        // 否则 trainAndAdopt 的 finally 永不执行，标志卡在 true，此后所有训练请求
        // 都被"already in progress"挡掉 —— 字典机制静默失效（与 Velocity 端同一处修正）。
        try {
            te.execute(() -> trainAndAdopt(belowMin));
        } catch (RejectedExecutionException e) {
            training.set(false);
        }
    }

    /**
     * @param allowBelowMin {@code true} 表示"字节预算/环容量已满，样本数不足也先清空环"
     *                      （见 {@link #addBatch}）。这条路径必须同样归档+清空，
     *                      否则环仍满、下一批又触发，形成反复空转。
     */
    private void trainAndAdopt(boolean allowBelowMin) {
        try {
            List<byte[]> snapshot;
            synchronized (this) {
                snapshot = new ArrayList<>(ring);
            }
            if (snapshot.isEmpty()) return;
            if (snapshot.size() < minSamples) {
                if (!allowBelowMin) return;
                // 低于门槛但预算已满：照常训练（质量不够的字典会被 shouldAdoptDict 挡下），
                // 关键是必须走完归档+清空，否则环一直满、下一批又触发，形成空转。
                LOGGER.debug("[Zstd] {} 样本数低于门槛（{} < {}），但预算已满：仍训练一轮并清空样本环",
                        name, snapshot.size(), minSamples);
            }

            // ⚠️ 周期训练每 5 分钟一轮、两个方向各一轮：只有"字典被采纳"这一条留 INFO，
            // 其余步骤一律 DEBUG —— 否则每轮 4 行 INFO 会把控制台刷满。
            int totalBytes = snapshot.stream().mapToInt(s -> s.length).sum();
            LOGGER.debug("[Zstd] {} 开始训练: {} 个样本 ({} 字节)", name, snapshot.size(), totalBytes);
            byte[] newDict = train(snapshot);
            boolean adopted = false;

            if (newDict == null || newDict.length == 0) {
                LOGGER.warn("[Zstd] {} 训练结果为空", name);
            } else {
                long dictId = mikumc.zstd.protocol.ZstdDictId.of(newDict);

                if (currentDict != null && currentDictId == dictId) {
                    LOGGER.debug("[Zstd] {} 字典未变化，跳过", name);
                } else {
                    EvalResult ev = evaluate(snapshot, newDict);
                    // 判定抽到共享模块（ZstdDictAdoption）——它是"字典会不会被启用"的唯一开关，
                    // 两端必须完全一致；首部字典与替换字典的标准刻意不同，见那里的注释。
                    boolean firstDict = currentDict == null;
                    boolean accept = shouldAdoptDict(firstDict, ev.overall, adoptThreshold);
                    if (!accept) {
                        LOGGER.debug("[Zstd] {} 改进不足: {}（阈值 {}，分档：{}）", name, pct(ev.overall),
                                firstDict ? "首部字典需>0" : pct(adoptThreshold), ev.bandSummary());
                    } else {
                        synchronized (this) {
                            currentDict = newDict;
                            currentDictId = dictId;
                        }
                        publishToRegistry();
                        adopted = true;
                        LOGGER.info("[Zstd] {} 采纳新字典 id={} size={}B 改进={}{} 样本={}（{}B）分档：{}",
                                name, dictId, newDict.length, pct(ev.overall),
                                firstDict ? "（首部字典）" : "",
                                snapshot.size(), totalBytes, ev.bandSummary());
                    }
                }
            }

            persist(adopted);
            synchronized (this) {
                ring.clear();
                sampleBytes = 0;
                ringFull = false;
                nextTrainCheckMs = 0;
                savedCount = 0;
            }
        } catch (Exception e) {
            LOGGER.error("[Zstd] {} 训练任务失败", name, e);
        } finally {
            training.set(false);
        }
    }

    private void publishToRegistry() {
        byte[] dict = currentDict;
        if (dict == null || dict.length == 0) return;
        if (direction == Direction.OUTBOUND) {
            ZstdPaperDictRegistry.updateEncoder(currentDictId, dict, runtimeLevel);
        } else {
            ZstdPaperDictRegistry.updateDecoder(currentDictId, dict);
        }
    }

    private byte[] train(List<byte[]> samples) {
        try {
            // ⚠️ 第一个参数是<b>训练样本缓冲容量</b>，不是"目标样本量"。
            // 曾传 sample_target_bytes（1MB）：样本环为满足 min_samples 而超过 1MB 后，
            // addSample 就开始返回 false，下面的 break 会把剩余样本静默丢弃。
            long total = 0;
            for (byte[] s : samples) {
                total += s.length;
            }
            int capacity = (int) Math.min(Math.max(total, 1), MAX_TRAIN_SAMPLE_BYTES);
            ZstdDictTrainer trainer = new ZstdDictTrainer(capacity, dictSize);
            for (byte[] s : samples) {
                if (!trainer.addSample(s)) {
                    break; // 达到 capacity 上限（仅可能在样本总量超过上限时发生）
                }
            }
            return trainer.trainSamples();
        } catch (Exception e) {
            LOGGER.error("[Zstd] {} 训练失败", name, e);
            return null;
        }
    }

    /**
     * 用运行时压缩等级评估新旧字典；字典对象按运行时等级准备并及时释放。
     *
     * <p>评估上下文与真实编码器保持<b>同样的帧头精简设置</b>（magicless / 无 contentSize /
     * 无 dictID），否则测出来的是带 magic 的完整帧开销，与实际线路字节不符。</p>
     *
     * <p>除整体改进率外还给出<b>分档改进率</b>（小/中/大包）——字典收益集中在小包，
     * 只看整体加权值（往往 1~3%）会低估它。</p>
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
            long[] oldBand = new long[ZstdDictAdoption.BANDS];
            long[] newBand = new long[ZstdDictAdoption.BANDS];
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
            LOGGER.warn("[Zstd] {} 评估失败", name, e);
            return EvalResult.none();
        } finally {
            if (oldCtx != null) oldCtx.close();
            if (newCtx != null) newCtx.close();
            if (oldDictObj != null) oldDictObj.close();
            if (newDictObj != null) newDictObj.close();
        }
    }

    private void persist(boolean writeDict) {
        try {
            if (writeDict) writeDictFile();
            archiveUnsaved();
        } catch (IOException e) {
            LOGGER.error("[Zstd] {} 持久化失败", name, e);
        }
    }

    private void writeDictFile() throws IOException {
        byte[] dict = currentDict;
        if (dict == null || dict.length == 0) return;
        Path target = dictDir.resolve(name + "_dict.bin");
        Path tmp = dictDir.resolve(name + "_dict.bin.tmp");
        Files.write(tmp, dict);
        Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }

    private void archiveUnsaved() {
        List<byte[]> unsaved;
        int upto;
        synchronized (this) {
            upto = ring.size();
            if (savedCount >= upto) return;
            unsaved = new ArrayList<>(ring.subList(savedCount, upto));
        }
        try {
            List<byte[]> history = loadHistory();
            history.addAll(unsaved);
            // ⚠️ 必须用 subList().clear() 而不是循环 remove(0)：后者每次把整个列表前移一位，
            // 上限 20000 时最坏情况是 O(n²)。与 Velocity 端同一处修正。
            if (history.size() > maxHistorySamples) {
                history.subList(0, history.size() - maxHistorySamples).clear();
            }
            writeSamples(history);
            synchronized (this) {
                savedCount = Math.max(savedCount, upto);
            }
        } catch (IOException e) {
            LOGGER.warn("[Zstd] {} 样本归档失败", name, e);
        }
    }

    private void loadFromDisk() {
        try {
            Path dictFile = dictDir.resolve(name + "_dict.bin");
            if (Files.exists(dictFile)) {
                currentDict = Files.readAllBytes(dictFile);
                currentDictId = mikumc.zstd.protocol.ZstdDictId.of(currentDict);
                LOGGER.info("[Zstd] {} 已从磁盘加载字典 id={} size={}B", name, currentDictId, currentDict.length);
            }
            List<byte[]> history = loadHistory();
            if (!history.isEmpty()) {
                synchronized (this) {
                    ring.addAll(history);
                    for (byte[] s : history) sampleBytes += s.length;
                    savedCount = ring.size();
                }
                LOGGER.info("[Zstd] {} 已加载 {} 个历史样本", name, history.size());
            }
            publishToRegistry();
        } catch (IOException e) {
            LOGGER.warn("[Zstd] {} 从磁盘加载失败", name, e);
        }
    }

    private List<byte[]> loadHistory() throws IOException {
        Path samplesFile = dictDir.resolve(name + "_samples.zst");
        if (!Files.exists(samplesFile)) return new ArrayList<>();
        byte[] compressed = Files.readAllBytes(samplesFile);
        // ⚠️ 连 RuntimeException 一起兜住：损坏/截断的文件会让 zstd-jni 抛 ZstdException，
        // 而调用方只声明 IOException —— 一个坏文件就会让插件初始化失败（服务器起不来）。
        try {
            Long sizeHint = Zstd.getFrameContentSize(compressed);
            if (sizeHint == null || sizeHint <= 0 || sizeHint > 256L * 1024 * 1024) return new ArrayList<>();
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
        long total = 0;
        for (byte[] s : samples) {
            total += s.length + 4L;
        }
        if (total <= 0 || total > Integer.MAX_VALUE - 8) {
            throw new IOException("样本历史过大: " + total);
        }
        ByteBuffer buf = ByteBuffer.allocate((int) total);
        for (byte[] s : samples) {
            buf.putInt(s.length);
            buf.put(s);
        }
        byte[] compressed = Zstd.compress(buf.array(), 3);
        Path target = dictDir.resolve(name + "_samples.zst");
        Path tmp = dictDir.resolve(name + "_samples.zst.tmp");
        Files.write(tmp, compressed);
        Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }
}
