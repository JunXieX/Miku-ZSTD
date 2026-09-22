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
    private long lastTrainTime;
    private volatile byte[] currentDict;
    private volatile long currentDictId;
    private final AtomicBoolean training = new AtomicBoolean();

    private final int maxSamples;
    private final int minSamples;
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
            if (te != null && !te.isShutdown()) {
                te.execute(ZstdPaperTrainer::saveAll);
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

    /** 手动触发训练（命令入口），绕过门槛与冷却。 */
    public void forceTrain() {
        if (!training.compareAndSet(false, true)) return;
        synchronized (this) {
            lastTrainTime = System.currentTimeMillis();
        }
        ExecutorService te = trainExecutor;
        if (te == null) {
            training.set(false);
            return;
        }
        te.execute(this::trainAndAdopt);
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
        List<byte[]> batch = null;
        int[] cursor = {0};
        while (cursor[0] < length) {
            // 共享的 varint 读取器（与 Velocity 端、解码器基类同一份实现）
            int pktLen = mikumc.zstd.protocol.ZstdVarInts.readOr(
                    raw, cursor, mikumc.zstd.protocol.ZstdVarInts.INVALID);
            if (pktLen <= 0 || cursor[0] + pktLen > length) {
                break;
            }
            byte[] sample = new byte[pktLen];
            System.arraycopy(raw, cursor[0], sample, 0, pktLen);
            cursor[0] += pktLen;
            if (!shouldKeep(sample)) {
                continue;
            }
            if (batch == null) {
                batch = new ArrayList<>(16);
            }
            batch.add(sample);
        }
        if (batch == null) {
            return;
        }

        int size;
        synchronized (this) {
            for (byte[] s : batch) {
                if (sampleBytes >= sampleSize) {
                    break;
                }
                ring.add(s);
                sampleBytes += s.length;
            }
            size = ring.size();
        }
        if (size >= minSamples) {
            maybeTrain();
        }
    }

    private void maybeTrain() {
        synchronized (this) {
            if (ring.size() < minSamples) return;
            long now = System.currentTimeMillis();
            if (now - lastTrainTime < cooldownMs) return;
            boolean full = ring.size() >= maxSamples;
            boolean timedOut = (now - lastTrainTime) >= fallbackTimeoutMs;
            if (!full && !timedOut) return;
            if (!training.compareAndSet(false, true)) return;
            lastTrainTime = now;
        }
        ExecutorService te = trainExecutor;
        if (te == null) {
            training.set(false);
            return;
        }
        te.execute(this::trainAndAdopt);
    }

    private void trainAndAdopt() {
        try {
            List<byte[]> snapshot;
            synchronized (this) {
                snapshot = new ArrayList<>(ring);
            }
            if (snapshot.size() < minSamples) return;

            LOGGER.info("[Zstd] {} 开始训练: {} 个样本", name, snapshot.size());
            byte[] newDict = train(snapshot);
            boolean adopted = false;

            if (newDict == null || newDict.length == 0) {
                LOGGER.warn("[Zstd] {} 训练结果为空", name);
            } else {
                long dictId = mikumc.zstd.protocol.ZstdDictId.of(newDict);

                if (currentDict != null && currentDictId == dictId) {
                    LOGGER.info("[Zstd] {} 字典未变化，跳过", name);
                } else {
                    EvalResult ev = evaluate(snapshot, newDict);
                    // 判定抽到共享模块（ZstdDictAdoption）——它是"字典会不会被启用"的唯一开关，
                    // 两端必须完全一致；首部字典与替换字典的标准刻意不同，见那里的注释。
                    boolean firstDict = currentDict == null;
                    boolean accept = shouldAdoptDict(firstDict, ev.overall, adoptThreshold);
                    if (!accept) {
                        LOGGER.info("[Zstd] {} 改进不足: {}（阈值 {}，分档：{}）", name, pct(ev.overall),
                                firstDict ? "首部字典需>0" : pct(adoptThreshold), ev.bandSummary());
                    } else {
                        synchronized (this) {
                            currentDict = newDict;
                            currentDictId = dictId;
                        }
                        publishToRegistry();
                        adopted = true;
                        LOGGER.info("[Zstd] {} 采纳新字典 id={} size={}B 改进={}{} 分档：{}",
                                name, dictId, newDict.length, pct(ev.overall),
                                firstDict ? "（首部字典）" : "", ev.bandSummary());
                    }
                }
            }

            persist(adopted);
            synchronized (this) {
                ring.clear();
                sampleBytes = 0;
                savedCount = 0;
            }
            LOGGER.info("[Zstd] {} 本轮结束(adopted={})，样本环已刷新", name, adopted);
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
            ZstdDictTrainer trainer = new ZstdDictTrainer(sampleSize, dictSize);
            for (byte[] s : samples) {
                if (!trainer.addSample(s)) break;
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

    /** 采样过滤：剥离 packetId varint；payload==8 大概率是 KeepAlive（内容随机、无价值）。 */
    /**
     * 该包是否值得作为字典样本——规则已抽到共享模块，两端共用同一份实现。
     *
     * <p>（原来两端各抄一份逐行相同的判定，属于最容易悄悄漂移的代码：
     * 改了一端不会报错，只会让字典质量悄悄变差。）</p>
     */
    private boolean shouldKeep(byte[] packetBytes) {
        return mikumc.zstd.protocol.ZstdSampleFilter.shouldKeep(packetBytes, pruneMinPayload);
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
            while (history.size() > maxHistorySamples) history.remove(0);
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
