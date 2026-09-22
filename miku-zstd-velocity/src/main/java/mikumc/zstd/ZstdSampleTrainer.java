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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

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
 *       （{@code sampleBytes >= sampleSize}），{@code addSample} 会永远早退，
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
    private long lastTrainTime;
    private volatile byte[] currentDict;
    private volatile long currentDictId;
    private final AtomicBoolean training = new AtomicBoolean();

    private final int maxSamples;
    private final int minSamples;
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
            if (te == null) return;
            te.execute(() -> {
                if (enc != null) enc.appendUnsavedSamplesToHistory();
                if (dec != null) dec.appendUnsavedSamplesToHistory();
            });
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

    public static void submitEncoderSample(byte[] packetBytes) {
        ZstdSampleTrainer t = encoderInstance;
        if (t != null) t.addSample(packetBytes);
    }

    public static void submitDecoderSample(byte[] packetBytes) {
        ZstdSampleTrainer t = decoderInstance;
        if (t != null) t.addSample(packetBytes);
    }

    /** 批量提交一帧（{@code [varint pktLen][pkt]...}）；见 {@link #addBatch}。 */
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

    public long getLastTrainTime() {
        return lastTrainTime;
    }

    /** 手动触发训练（/mikuzstd train 命令入口），绕过采样门槛与冷却。 */
    public void forceTrain() {
        ExecutorService te = trainExecutor;
        if (te == null) return;
        if (!training.compareAndSet(false, true)) {
            LOGGER.info("[Zstd] {} training already in progress", name);
            return;
        }
        synchronized (this) {
            lastTrainTime = System.currentTimeMillis();
        }
        te.execute(this::trainAndAdopt);
    }

    /**
     * 拆分整帧并批量入环：帧格式为 {@code [varint pktLen][pkt]...}。
     *
     * <p>必须按包拆分——整批当一个样本的话，样本"个数"增长极慢（实测 1MB 数据只有 26 个），
     * 永远够不到 min_samples，字典训练形同虚设。</p>
     *
     * <p>与逐包 {@link #addSample} 的区别：把「解析 + 过滤 + 复制」放在<b>锁外</b>完成，
     * 只在入环那一刻加一次锁。原来一帧几十个包就要几十次加锁 + 几十次训练判定，
     * 网络线程上这是白白的争用。（解析不放进锁内，是为了不让它拖长持锁时间。）</p>
     */
    private void addBatch(byte[] raw, int length) {
        if (raw == null || length <= 0) {
            return;
        }
        List<byte[]> batch = null;
        int pos = 0;
        while (pos < length) {
            int pktLen = 0;
            int shift = 0;
            while (pos < length && shift <= 28) {
                byte b = raw[pos++];
                pktLen |= (b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    break;
                }
                shift += 7;
            }
            if (pktLen <= 0 || pos + pktLen > length) {
                break;
            }
            byte[] sample = new byte[pktLen];
            System.arraycopy(raw, pos, sample, 0, pktLen);
            pos += pktLen;
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
                sampleRing.add(s);
                sampleBytes += s.length;
            }
            size = sampleRing.size();
        }
        if (size >= minSamples) {
            maybeTrain();
        }
    }

    private void addSample(byte[] packetBytes) {
        if (packetBytes == null || packetBytes.length == 0) return;
        if (!shouldKeep(packetBytes)) return;

        int size;
        synchronized (this) {
            if (sampleBytes >= sampleSize) return;
            sampleRing.add(packetBytes);
            sampleBytes += packetBytes.length;
            size = sampleRing.size();
        }
        if (size >= minSamples) {
            maybeTrain();
        }
    }

    /** 判断是否满足训练条件（满足则以 training 原子标志占位，异步执行）。 */
    private void maybeTrain() {
        synchronized (this) {
            if (sampleRing.size() < minSamples) return;
            long now = System.currentTimeMillis();
            if (now - lastTrainTime < trainCooldownMs) return;
            boolean full = sampleRing.size() >= maxSamples;
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

    /**
     * 训练 + 评估 + 采纳 + 归档 + 清空样本环（训练线程调用）。
     *
     * <p>关键：<b>无论是否采纳新字典，样本环都会归档并清空</b>。否则环一旦攒满，
     * {@code addSample} 的容量早退会让它永久拒绝新样本，字典再无进化可能。</p>
     */
    private void trainAndAdopt() {
        try {
            List<byte[]> snapshot;
            synchronized (this) {
                snapshot = new ArrayList<>(sampleRing);
            }
            if (snapshot.size() < minSamples) {
                LOGGER.info("[Zstd] {} not enough samples to train ({} < {})", name, snapshot.size(), minSamples);
                return;
            }

            LOGGER.info("[Zstd] {} starting training with {} samples ({} bytes)",
                    name, snapshot.size(), snapshot.stream().mapToInt(s -> s.length).sum());

            byte[] newDict = train(snapshot);
            boolean adopted = false;

            if (newDict == null || newDict.length == 0) {
                LOGGER.warn("[Zstd] {} training produced empty dict", name);
            } else {
                Checksum crc = new CRC32();
                crc.update(newDict, 0, newDict.length);
                long dictId = crc.getValue();

                if (currentDict != null && currentDictId == dictId) {
                    LOGGER.info("[Zstd] {} dict unchanged, skipping", name);
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
                        LOGGER.info("[Zstd] {} dict insufficient improvement: {}% (阈值 {}%，分档：{})",
                                name, pct(ev.overall), firstDict ? "首部字典需>0" : pct(adoptRatioImprovement),
                                ev.bandSummary());
                    } else {
                        synchronized (this) {
                            currentDict = newDict;
                            currentDictId = dictId;
                        }
                        publishToRegistry();
                        adopted = true;
                        LOGGER.info("[Zstd] {} new dict adopted: id={} size={} improvement={}%{} 分档：{}",
                                name, dictId, newDict.length, pct(ev.overall),
                                firstDict ? "（首部字典，只要不劣化即采纳）" : "", ev.bandSummary());
                    }
                }
            }

            // 先把未入库样本归档（无论采纳与否），再清空样本环
            persist(adopted);
            synchronized (this) {
                sampleRing.clear();
                sampleBytes = 0;
                savedCount = 0;
            }
            LOGGER.info("[Zstd] {} training round finished (adopted={}), sample ring refreshed", name, adopted);
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
            ZstdDictTrainer trainer = new ZstdDictTrainer(sampleSize, dictSize);
            for (byte[] s : samples) {
                if (!trainer.addSample(s)) break;
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


    /**
     * 该包是否值得作为字典样本——规则已抽到共享模块，两端共用同一份实现。
     *
     * <p>（原来两端各抄一份逐行相同的判定，属于最容易悄悄漂移的代码：
     * 改了一端不会报错，只会让字典质量悄悄变差。）</p>
     */
    private boolean shouldKeep(byte[] packetBytes) {
        return mikumc.zstd.protocol.ZstdSampleFilter.shouldKeep(packetBytes, pruneMinPayload);
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
            while (history.size() > maxHistorySamples) history.remove(0);
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
                currentDictId = crc32(currentDict);
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
                        currentDictId = crc32(currentDict);
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

    private static long crc32(byte[] data) {
        Checksum crc = new CRC32();
        crc.update(data, 0, data.length);
        return crc.getValue();
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
