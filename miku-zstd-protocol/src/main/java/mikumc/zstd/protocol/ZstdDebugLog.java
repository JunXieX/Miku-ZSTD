package mikumc.zstd.protocol;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * 调试日志的文件落盘端（三端共用）。
 *
 * <h2>为什么不用日志框架的文件 appender</h2>
 * <p>Paper / Velocity / 客户端各自跑在不同的日志实现上（log4j2、logback、被游戏接管的
 * log4j2），要在运行期反射挂一个 appender 再把它从根 logger 上摘掉，既依赖各实现的内部结构，
 * 又会把这一层的 INFO/WARN 一起挡在控制台外——排查时最不该丢的就是真正的告警。
 * 这里改为<b>独立的一条文件通道</b>：控制台的口径一行不变，只是把逐包诊断另存一份。</p>
 *
 * <h2>写入约束</h2>
 * <ul>
 *   <li><b>不阻塞调用方</b>：{@code d(...)} 只做格式化与入队，落盘在单条守护线程上；
 *       队列满则丢弃并计数，绝不 {@code put}——它会被 netty event loop 与客户端渲染线程调到。</li>
 *   <li><b>不抛异常</b>：格式化失败、磁盘只读、目录不存在，全部就地降级。</li>
 *   <li><b>有硬上界</b>：单文件超过 {@code maxFileBytes} 即滚动，最多保留 {@code keepFiles} 份历史；
 *       另有 5000 行/秒的令牌桶，防止某个异常循环在几秒内写满磁盘。</li>
 *   <li><b>当场可见</b>：每行都 flush。本日志的主要用途恰恰是"卡住/断连"这类现场取证，
 *       进程被强杀时缓冲区里的内容必须已经落盘。</li>
 * </ul>
 *
 * <p>未调用 {@link #configure} 时所有方法都是无害空操作，因此协议层单测不产生任何文件。</p>
 */
public final class ZstdDebugLog {

    /** 行首时间戳；与终端里 {@code date} 的口径一致，便于和系统日志对齐。 */
    private static final DateTimeFormatter STAMP = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    /** 队列容量：够 100 条连接同时进服的突发，又不至于让掉队的内容占住内存。 */
    private static final int QUEUE_CAPACITY = 8192;

    /** 每秒最多接受的行数。超过即丢弃并计数——正常负载下永远碰不到。 */
    private static final int MAX_LINES_PER_SECOND = 5000;

    /** 每丢弃这么多行，补写一条汇总，避免"丢了但看不出来"。 */
    private static final long DROP_REPORT_EVERY = 2000L;

    /** 队列里的关机哨兵，用引用比较，杜绝与真实内容混淆。 */
    private static final String POISON = new String("<miku-zstd-debug-poison>");

    /** 文件流与滚动相关的唯一互斥量：写线程每行取一次，重配与关停也取它。 */
    private static final Object FILE_LOCK = new Object();

    /** 限速窗口自身的小锁，与文件锁分开，避免写盘时拖住调用方。 */
    private static final Object RATE_LOCK = new Object();

    private static final AtomicLong DROPPED = new AtomicLong();
    private static final AtomicLong WRITTEN = new AtomicLong();

    private static volatile boolean enabled;
    private static volatile Path file;
    private static volatile Consumer<String> consoleMirror;
    private static volatile BlockingQueue<String> queue;
    private static volatile Thread writerThread;
    private static volatile BufferedWriter out;
    private static volatile long maxFileBytes = 8L << 20;
    private static volatile int keepFiles = 2;

    /** 受 {@link #FILE_LOCK} 保护：当前文件的累计字节数（含重开时已有的内容）。 */
    private static long fileBytes;
    private static volatile boolean openFailed;

    private static long rateWindowStartMs;
    private static int rateWindowCount;

    private ZstdDebugLog() {
    }

    /**
     * 配置并打开文件通道。
     *
     * @param targetFile   目标文件（相对路径由调用方先按各自数据目录解析成绝对路径）
     * @param enable       false 表示本次不记录任何调试信息（不建文件、不起线程）
     * @param maxBytes     单文件字节上限，&lt; 64 KiB 时按 64 KiB 处理
     * @param keepBackups  保留的历史文件个数，夹取到 0-10
     * @param mirror       非 null 时，每行同时交给它（用于 {@code console_debug}）
     */
    public static synchronized void configure(Path targetFile, boolean enable, int maxBytes,
                                              int keepBackups, Consumer<String> mirror) {
        closeInternal();
        consoleMirror = mirror;
        file = null;
        if (!enable || targetFile == null) {
            return;
        }
        maxFileBytes = Math.max(64L * 1024L, maxBytes);
        keepFiles = Math.max(0, Math.min(10, keepBackups));
        file = targetFile.toAbsolutePath();
        synchronized (FILE_LOCK) {
            openFailed = false;
            openLocked();
        }
        if (out == null) {
            reportOpenFailure();
            file = null;
            return;
        }
        DROPPED.set(0);
        WRITTEN.set(0);
        // 限速窗口属于"本次会话"：换了文件就重新计数，否则上一轮（reload 前/上一个进程）
        // 用掉的额度会把新会话开头的几百行直接吞掉——症状是文件建出来了却是空的。
        synchronized (RATE_LOCK) {
            rateWindowStartMs = 0L;
            rateWindowCount = 0;
        }
        queue = new ArrayBlockingQueue<>(QUEUE_CAPACITY);
        enabled = true;
        startWriter(queue);
    }

    /**
     * 写一段会话横幅：多进程/多次启动的内容写在同一个文件里时，靠它区分现场。
     *
     * @param version  本端版本号
     * @param platform 平台描述（Paper / Velocity / Fabric 客户端 + 具体版本）
     * @param facts    追加的键值（配置快照、环境信息等），可为 null
     */
    public static void sessionHeader(String version, String platform, Map<String, String> facts) {
        if (!enabled) return;
        d("================ Miku-ZSTD 调试日志开始 ================");
        d("版本={} 平台={}", version, platform);
        d("Java={} OS={}",
                System.getProperty("java.version", "?"),
                System.getProperty("os.name", "?") + "/" + System.getProperty("os.arch", "?"));
        if (file != null) {
            d("文件={}", file);
        }
        if (facts != null) {
            for (Map.Entry<String, String> e : facts.entrySet()) {
                d("{}={}", e.getKey(), e.getValue());
            }
        }
        d("========================================================");
    }

    /** 记录一行纯文本。 */
    public static void d(String msg) {
        if (!enabled) return;
        emit(msg == null ? "null" : msg);
    }

    /**
     * 记录一行，占位符沿用 slf4j 的 {@code {}} 写法——迁移时调用点只需要换方法名。
     *
     * <p>末位参数若是异常且有富余占位符，按 slf4j 的约定把堆栈附在行尾（最多 8 帧）。</p>
     */
    public static void d(String fmt, Object... args) {
        if (!enabled) return;
        emit(substitute(fmt, args));
    }

    /** 关闭文件通道；可重复调用。 */
    public static synchronized void close() {
        closeInternal();
    }

    public static boolean enabled() {
        return enabled;
    }

    /** 当前文件绝对路径；未启用时为 {@code null}。 */
    public static Path file() {
        return file;
    }

    public static long writtenLines() {
        return WRITTEN.get();
    }

    public static long droppedLines() {
        return DROPPED.get();
    }

    /** 供 {@code /mikuzstd status} 等命令直接展示的一行摘要。 */
    public static String statsLine() {
        Path target = file;
        if (target == null) {
            // 6.1.1 起没有关闭开关，这里只剩"本端打开失败"这一种情况。
            return "调试日志：关闭（未启用或写入失败）";
        }
        long bytes;
        synchronized (FILE_LOCK) {
            bytes = fileBytes;
        }
        return "调试日志：" + (enabled ? "开启" : "关闭") + " → " + target
                + "（" + humanBytes(bytes) + "，已写 " + WRITTEN.get() + " 行，丢弃 " + DROPPED.get() + " 行）";
    }

    // ── 内部实现 ────────────────────────────────────────────────────────────

    private static void closeInternal() {
        enabled = false;
        BlockingQueue<String> q = queue;
        Thread t = writerThread;
        queue = null;
        writerThread = null;
        if (q != null && t != null && t.isAlive()) {
            if (!q.offer(POISON)) {
                // 队列满：丢掉排队内容，保证哨兵能进得去，否则 join 只能等超时
                q.clear();
                q.offer(POISON);
            }
            try {
                t.join(2000L);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
            if (t.isAlive()) {
                warnConsole("[Miku-ZSTD] 调试日志线程未在 2 秒内退出，放弃等待");
            }
        }
        synchronized (FILE_LOCK) {
            closeStreamLocked();
        }
    }

    /**
     * 启动写线程，并把队列<b>按值</b>交给它。
     *
     * <p>不能让写线程去读静态字段 {@code queue}：{@link #closeInternal()} 会先把该字段置空，
     * 线程再读到 null 就会空转（既看不到哨兵，也永远不退出）。每次重配换新队列、换新线程，
     * 一条线程只服务一个队列。</p>
     */
    private static void startWriter(BlockingQueue<String> ownedQueue) {
        Thread t = new Thread(() -> writerLoop(ownedQueue), "miku-zstd-debug-log");
        t.setDaemon(true);
        writerThread = t;
        t.start();
    }

    private static void writerLoop(BlockingQueue<String> ownedQueue) {
        while (true) {
            BlockingQueue<String> current = queue;
            if (current != null && current != ownedQueue) {
                // 已被新会话接管（configure 换了文件）：立刻退出，绝不能拿旧队列的行去写新文件。
                // close 时 queue 为 null，此时仍需把剩余行写完，所以只在"换成另一个队列"时退出。
                return;
            }
            String line;
            try {
                line = ownedQueue.poll(500L, TimeUnit.MILLISECONDS);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return;
            }
            if (line == null) {
                continue;
            }
            if (line == POISON) {
                return;
            }
            appendToFile(line);
            reportDropsIfNeeded();
        }
    }

    private static void emit(String body) {
        Consumer<String> m = consoleMirror;
        if (m != null) {
            try {
                m.accept(body);
            } catch (Throwable ignored) {
                // 控制台镜像失败绝不能影响文件通道
            }
        }
        if (!allowLine()) {
            DROPPED.incrementAndGet();
            return;
        }
        BlockingQueue<String> q = queue;
        if (q == null) {
            DROPPED.incrementAndGet();
            return;
        }
        String line = STAMP.format(LocalDateTime.now())
                + " [" + Thread.currentThread().getName() + "] " + body
                + System.lineSeparator();
        if (!q.offer(line)) {
            DROPPED.incrementAndGet();
        }
    }

    private static boolean allowLine() {
        long now = System.currentTimeMillis();
        synchronized (RATE_LOCK) {
            if (now - rateWindowStartMs >= 1000L) {
                rateWindowStartMs = now;
                rateWindowCount = 0;
            }
            if (rateWindowCount >= MAX_LINES_PER_SECOND) {
                return false;
            }
            rateWindowCount++;
            return true;
        }
    }

    private static void appendToFile(String line) {
        byte[] bytes = line.getBytes(StandardCharsets.UTF_8);
        synchronized (FILE_LOCK) {
            if (out == null) {
                return;
            }
            try {
                out.write(line);
                out.flush();
                fileBytes += bytes.length;
                WRITTEN.incrementAndGet();
                if (fileBytes > maxFileBytes) {
                    rotateLocked();
                }
            } catch (Throwable t) {
                // 磁盘满/被删/权限变化：关掉本通道，避免每行都失败一次
                closeStreamLocked();
                reportOpenFailure();
            }
        }
    }

    private static void reportDropsIfNeeded() {
        long dropped = DROPPED.get();
        if (dropped > 0 && dropped % DROP_REPORT_EVERY == 0) {
            appendToFile(STAMP.format(LocalDateTime.now())
                    + " [miku-zstd-debug-log] [debug] 已丢弃 " + dropped + " 行（队列满或超过限速）"
                    + System.lineSeparator());
        }
    }

    private static void rotateLocked() {
        closeStreamLocked();
        try {
            Path base = file;
            if (base == null) {
                return;
            }
            if (keepFiles <= 0) {
                Files.deleteIfExists(base);
            } else {
                Files.deleteIfExists(backupPath(base, keepFiles));
                for (int i = keepFiles - 1; i >= 1; i--) {
                    Path from = backupPath(base, i);
                    if (Files.exists(from)) {
                        Files.move(from, backupPath(base, i + 1), StandardCopyOption.REPLACE_EXISTING);
                    }
                }
                if (Files.exists(base)) {
                    Files.move(base, backupPath(base, 1), StandardCopyOption.REPLACE_EXISTING);
                }
            }
            openLocked();
        } catch (Throwable t) {
            out = null;
            fileBytes = 0L;
            reportOpenFailure();
        }
    }

    private static Path backupPath(Path base, int index) {
        return base.resolveSibling(base.getFileName().toString() + "." + index);
    }

    private static void openLocked() {
        out = null;
        fileBytes = 0L;
        Path target = file;
        if (target == null) {
            return;
        }
        try {
            Path parent = target.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            out = Files.newBufferedWriter(target, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
            fileBytes = Files.size(target);
        } catch (Throwable t) {
            out = null;
            fileBytes = 0L;
        }
    }

    private static void closeStreamLocked() {
        BufferedWriter w = out;
        out = null;
        if (w == null) {
            return;
        }
        try {
            w.flush();
        } catch (IOException ignored) {
            // 关流阶段的 flush 失败没有补救手段，忽略
        }
        try {
            w.close();
        } catch (IOException ignored) {
            // 同上
        }
    }

    /** 只在"从没有报过"与"刚失败"时各打一次，避免每行都刷。 */
    private static void reportOpenFailure() {
        if (openFailed) {
            return;
        }
        openFailed = true;
        warnConsole("[Miku-ZSTD] 调试日志无法写入 " + file + "，本次不再记录（功能不受影响）");
    }

    private static void warnConsole(String message) {
        Consumer<String> m = consoleMirror;
        if (m != null) {
            try {
                m.accept(message);
                return;
            } catch (Throwable ignored) {
                // 落到 System.err
            }
        }
        System.err.println(message);
    }

    /** slf4j 风格的 {@code {}} 替换；任何异常都退回原文，绝不让日志本身抛错。 */
    private static String substitute(String fmt, Object[] args) {
        if (fmt == null) {
            return "null";
        }
        if (args == null || args.length == 0) {
            return fmt;
        }
        try {
            int argCount = args.length;
            Throwable throwable = null;
            if (args[argCount - 1] instanceof Throwable t && countPlaceholders(fmt) < argCount) {
                throwable = t;
                argCount--;
            }
            StringBuilder sb = new StringBuilder(fmt.length() + 32);
            int argIndex = 0;
            for (int i = 0; i < fmt.length(); i++) {
                char c = fmt.charAt(i);
                if (c == '\\' && i + 1 < fmt.length() && fmt.charAt(i + 1) == '{') {
                    sb.append('{');
                    i++;
                } else if (c == '{' && i + 1 < fmt.length() && fmt.charAt(i + 1) == '}'
                        && argIndex < argCount) {
                    sb.append(safeToString(args[argIndex++]));
                    i++;
                } else {
                    sb.append(c);
                }
            }
            if (throwable != null) {
                sb.append(' ').append(stackOf(throwable));
            }
            return sb.toString();
        } catch (Throwable t) {
            return fmt;
        }
    }

    private static int countPlaceholders(String fmt) {
        int count = 0;
        for (int i = 0; i + 1 < fmt.length(); i++) {
            if (fmt.charAt(i) == '\\' && fmt.charAt(i + 1) == '{') {
                i++;
            } else if (fmt.charAt(i) == '{' && fmt.charAt(i + 1) == '}') {
                count++;
                i++;
            }
        }
        return count;
    }

    private static String safeToString(Object value) {
        if (value == null) {
            return "null";
        }
        try {
            return String.valueOf(value);
        } catch (Throwable t) {
            return value.getClass().getName() + "(toString failed: " + t + ")";
        }
    }

    /** 异常只留类名、消息与 8 帧——文件是给人读的，不是 dump 容器。 */
    private static String stackOf(Throwable t) {
        StringBuilder sb = new StringBuilder();
        sb.append(t);
        StackTraceElement[] frames = t.getStackTrace();
        int limit = Math.min(frames.length, 8);
        for (int i = 0; i < limit; i++) {
            sb.append("\n    at ").append(frames[i]);
        }
        if (frames.length > limit) {
            sb.append("\n    ... ").append(frames.length - limit).append(" more");
        }
        return sb.toString();
    }

    private static String humanBytes(long bytes) {
        if (bytes < 1024L) {
            return bytes + " B";
        }
        if (bytes < 1024L * 1024L) {
            return String.format(java.util.Locale.ROOT, "%.1f KB", bytes / 1024.0);
        }
        return String.format(java.util.Locale.ROOT, "%.1f MB", bytes / (1024.0 * 1024.0));
    }
}
