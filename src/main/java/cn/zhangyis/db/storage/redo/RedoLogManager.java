package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;

import java.util.ArrayList;
import java.util.List;
import java.time.Duration;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Redo 日志管理器：同步分配 LSN 区间并把记录追加到内存 buffer；R1 增加可选的同步 writer/flusher 和 durability wait。
 *
 * <p>默认构造器保持 D3 内存语义（不配置 redo 文件），供 MTR/FSP 现有测试继续使用。通过 {@link #durable} 创建的实例会把
 * append 批次保存在 pending 队列，{@link #flush()} 同步写 redo 文件并 fsync，成功后推进 flushedToDiskLsn。
 *
 * <p><b>并发边界</b>：{@code lock} 只保护 LSN 分配、pending 队列、recent closed 和 durable 边界；
 * {@code ioLock} 串行 redo 文件 write/fsync。flush 执行阻塞 IO 时不持有状态锁，因此前台 append 可继续预留
 * 新 LSN 并进入下一轮 pending，但同一时间仍只有一个 flush owner 写 redo 文件。
 */
public final class RedoLogManager {

    private static final long NANOS_PER_SECOND = 1_000_000_000L;

    /** 保护 nextLsn 与 buffer 的互斥锁。 */
    private final ReentrantLock lock = new ReentrantLock();
    /** 串行 redo 文件 write/fsync；不得在持有状态锁时等待该锁，避免 append 被长 fsync 间接阻塞。 */
    private final ReentrantLock ioLock = new ReentrantLock();
    /** durability 等待条件；flush 推进 flushedToDiskLsn 后唤醒。 */
    private final Condition flushedAdvanced = lock.newCondition();
    /** write 等待条件；write/flush 推进 writtenToDiskLsn 后唤醒。 */
    private final Condition writtenAdvanced = lock.newCondition();
    /** 下一个空闲 LSN（long 计数，append 时推进）。 */
    private long nextLsn = 0;
    /**
     * 内存 redo buffer（append 顺序）。内存模式下累积全部 append 记录，供 MTR/FSP/B+Tree 测试断言与手动 replay；
     * durable 模式下仅保留「尚未 flush」窗口——{@link #flush()} 落盘后清空，避免长运行内存无界增长。
     * 它不是恢复依据：durable 模式恢复只读 redo 文件，本 buffer 与恢复正确性无关。
     */
    private final List<RedoRecord> buffer = new ArrayList<>();
    /**
     * 已分配 LSN 的批次诊断快照。语义与 {@link #buffer} 一致：内存模式累积全量、durable 模式 flush 后清空。
     * 与 {@link #pendingBatches} 区别：pendingBatches 是落盘待办（权威），本列表仅供测试/诊断观察。
     */
    private final List<RedoLogBatch> batches = new ArrayList<>();
    /** 尚未写入 redo 文件的批次。 */
    private final List<RedoLogBatch> pendingBatches = new ArrayList<>();
    /**
     * 已写入 OS/page cache、但尚未完成 fsync 的批次。force 失败后保留在这里，下一次 flush 成功后再清理诊断快照。
     */
    private final List<RedoLogBatch> writtenNotFlushedBatches = new ArrayList<>();
    /** 可选 redo IO 端口；为空表示 D3 内存模式。 */
    private final RedoLogIo io;
    /** 已 fsync 的最高 LSN；D3 内存模式恒为 0。 */
    private Lsn flushedToDiskLsn = Lsn.of(0);
    /**
     * 已写入 OS page cache、但未必 fsync 的最高 LSN（三阶段中的 write 边界）。WRITE_ON_COMMIT 等待该值。
     * 不变量：{@code flushedToDiskLsn <= writtenToDiskLsn}（fsync 不会领先于 write）。D3 内存模式恒为 0。
     */
    private Lsn writtenToDiskLsn = Lsn.of(0);
    /**
     * writer 可读取的 recent written 连续边界。当前 append 同步完成 buffer 写入，因此批次创建后立即推进；
     * 后续若拆成真正并发 log buffer segment，可继续复用该 tracker 合并乱序写入完成事件。
     */
    private final ContiguousLsnTracker recentWritten = new ContiguousLsnTracker(Lsn.of(0));
    /**
     * checkpoint 可见的 recent closed 连续边界。只有 MTR 已把相关 dirty page 发布到 Buffer Pool 后才允许推进，
     * 否则 fuzzy checkpoint 可能越过尚未进入 flush list 的 redo 区间，导致崩溃恢复缺少必要日志。
     */
    private final ContiguousLsnTracker recentClosed = new ContiguousLsnTracker(Lsn.of(0));
    /** 启动恢复边界是否已显式安装；只允许在首个新 append 前执行一次（同值调用幂等）。 */
    private boolean recoveredBoundaryInstalled;

    public RedoLogManager() {
        this(null);
    }

    RedoLogManager(RedoLogIo io) {
        this.io = io;
    }

    /**
     * 创建带 redo 文件持久化能力的管理器。writer/flusher 仍是同步调用，后台线程、capacity/checkpoint 留后续批次。
     *
     * @param repository redo 文件仓储。
     * @return durable redo manager。
     */
    public static RedoLogManager durable(RedoLogFileRepository repository) {
        if (repository == null) {
            throw new DatabaseValidationException("redo log repository must not be null");
        }
        return new RedoLogManager(new RepositoryRedoLogIo(repository));
    }

    /**
     * 追加一批 redo 记录，分配并返回其 LSN 区间 {@code [start, end)}（end = start + Σ byteLength）。空批返回退化区间 [cur,cur)。
     */
    public LogRange append(List<RedoRecord> records) {
        if (records == null) {
            throw new DatabaseValidationException("redo records must not be null");
        }
        lock.lock();
        try {
            long start = nextLsn;
            long end = start;
            for (RedoRecord r : records) {
                if (r == null) {
                    throw new DatabaseValidationException("redo record must not be null");
                }
                end += r.byteLength();
                buffer.add(r);
            }
            nextLsn = end;
            if (!records.isEmpty()) {
                RedoLogBatch batch = new RedoLogBatch(new LogRange(Lsn.of(start), Lsn.of(end)), records);
                batches.add(batch);
                if (io != null) {
                    pendingBatches.add(batch);
                }
                recentWritten.mark(batch.range());
            }
            return new LogRange(Lsn.of(start), Lsn.of(end));
        } finally {
            lock.unlock();
        }
    }

    /** 当前下一个空闲 LSN。 */
    public Lsn currentLsn() {
        lock.lock();
        try {
            return Lsn.of(nextLsn);
        } finally {
            lock.unlock();
        }
    }

    /** 已连续写入 redo buffer、可供 writer 顺序写出的 LSN 边界。 */
    public Lsn readyForWriteLsn() {
        lock.lock();
        try {
            return recentWritten.boundary();
        } finally {
            lock.unlock();
        }
    }

    /** 所有已发布 dirty page 的连续 redo 边界，checkpoint 不能越过该值。 */
    public Lsn closedLsn() {
        lock.lock();
        try {
            return recentClosed.boundary();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 标记一个 redo 区间已经完成 dirty page 发布，可以纳入 checkpoint 边界。
     *
     * <p>调用方必须保证该方法发生在 pageLSN 盖戳与 dirty 发布之后；若提前关闭，checkpoint 可能持久化到
     * 一个无法覆盖 Buffer Pool dirty view 的 LSN，崩溃恢复会从过新的位置开始扫描。
     *
     * @param range 已发布完成的 redo 区间；空区间不改变边界。
     */
    public void markClosed(LogRange range) {
        if (range == null) {
            throw new DatabaseValidationException("closed redo range must not be null");
        }
        lock.lock();
        try {
            recentClosed.mark(range);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 三阶段中的「write」：把 pending redo 写到 OS page cache（不 fsync），推进 {@link #writtenToDiskLsn}。
     * 供 WRITE_ON_COMMIT 使用——崩溃进程不丢、但宕机/断电可能丢。D3 内存模式无 IO，返回当前 writtenToDiskLsn（0）。
     *
     * @return write 后已写入 OS cache 的最高 LSN。
     */
    public Lsn write() {
        if (io == null) {
            lock.lock();
            try {
                return writtenToDiskLsn;
            } finally {
                lock.unlock();
            }
        }
        ioLock.lock();
        try {
            drainPendingToWritten();
            lock.lock();
            try {
                return writtenToDiskLsn;
            } finally {
                lock.unlock();
            }
        } finally {
            ioLock.unlock();
        }
    }

    /** 已写入 OS page cache、但未必 fsync 的最高 LSN（write 边界）。 */
    public Lsn writtenToDiskLsn() {
        lock.lock();
        try {
            return writtenToDiskLsn;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 等待 {@link #writtenToDiskLsn()} 覆盖目标 LSN（OS-cache 写入边界）。带明确 timeout，不无界等待。
     *
     * @param target  目标 written LSN。
     * @param timeout 最大等待时间。
     * @return true 表示目标已写到 OS cache；false 表示 timeout 或线程被中断。
     */
    public boolean waitWritten(Lsn target, Duration timeout) {
        if (target == null || timeout == null) {
            throw new DatabaseValidationException("redo wait target/timeout must not be null");
        }
        if (timeout.isNegative()) {
            throw new DatabaseValidationException("redo wait timeout must not be negative: " + timeout);
        }
        lock.lock();
        try {
            if (writtenToDiskLsn.value() >= target.value()) {
                return true;
            }
            long nanos = timeoutNanos(timeout);
            while (writtenToDiskLsn.value() < target.value()) {
                if (nanos <= 0) {
                    return false;
                }
                try {
                    nanos = writtenAdvanced.awaitNanos(nanos);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
            return true;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 已 fsync 到 redo 文件的最高 LSN。Flush 模块未来的 WAL gate 使用该值判断 {@code pageLSN <= flushedToDiskLsn}。
     */
    public Lsn flushedToDiskLsn() {
        lock.lock();
        try {
            return flushedToDiskLsn;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 同步写出并 fsync 所有 pending redo 批次，推进 durable LSN。
     *
     * <p>D3 内存模式没有 writer/flusher，调用该方法只返回当前 flushedToDiskLsn（0），不伪造 durable。
     *
     * @return flush 后的 durable LSN。
     */
    public Lsn flush() {
        if (io == null) {
            lock.lock();
            try {
                return flushedToDiskLsn;
            } finally {
                lock.unlock();
            }
        }
        ioLock.lock();
        try {
            drainPendingToWritten();
            Lsn physicalFlushed = io.flushTo(io.writtenToDiskLsn());
            lock.lock();
            try {
                // 新进程 writer/flusher 从 0 构造，但 restoreRecoveredBoundary 已安装历史 durable 边界；
                // 空 pending flush 绝不能把该边界降回 0。只有本进程新写出的更高 LSN 才推进。
                if (physicalFlushed.value() > flushedToDiskLsn.value()) {
                    flushedToDiskLsn = physicalFlushed;
                }
                // durable 模式下 buffer/batches 仅作诊断快照；已成功 fsync 的批次对恢复无意义，落盘后释放。
                clearFlushedDiagnostics();
                flushedAdvanced.signalAll();
                return flushedToDiskLsn;
            } finally {
                lock.unlock();
            }
        } finally {
            ioLock.unlock();
        }
    }

    /**
     * redo replay 完成后安装恢复边界，使新 MTR 从 {@code recoveredToLsn} 继续分配，并把历史日志视为已 durable。
     * 只能在本 manager 尚无新 append/pending batch 时调用；否则重设 nextLsn 会制造重叠区间。
     *
     * @param recoveredToLsn recovery reader 验证过的最后完整 LSN。
     */
    public void restoreRecoveredBoundary(Lsn recoveredToLsn) {
        if (recoveredToLsn == null) {
            throw new DatabaseValidationException("recovered redo boundary must not be null");
        }
        lock.lock();
        try {
            if (recoveredBoundaryInstalled) {
                if (nextLsn == recoveredToLsn.value()) {
                    return;
                }
                throw new DatabaseValidationException("redo recovery boundary already installed at " + nextLsn);
            }
            if (nextLsn != 0 || !buffer.isEmpty() || !batches.isEmpty()
                    || !pendingBatches.isEmpty() || !writtenNotFlushedBatches.isEmpty()) {
                throw new DatabaseValidationException(
                        "redo recovery boundary must be installed before any new append");
            }
            nextLsn = recoveredToLsn.value();
            // 恢复到的 redo 既已 durable，write 与 flush 边界都置为 recoveredToLsn（保持 flushed <= written 不变量）。
            flushedToDiskLsn = recoveredToLsn;
            writtenToDiskLsn = recoveredToLsn;
            recentWritten.reset(recoveredToLsn);
            recentClosed.reset(recoveredToLsn);
            recoveredBoundaryInstalled = true;
            flushedAdvanced.signalAll();
            writtenAdvanced.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 等待 {@link #flushedToDiskLsn()} 覆盖目标 LSN。等待有明确 timeout，避免事务/flush 路径无界阻塞。
     *
     * @param target 目标 durable LSN。
     * @param timeout 最大等待时间。
     * @return true 表示目标已 durable；false 表示 timeout 或线程被中断。
     */
    public boolean waitFlushed(Lsn target, Duration timeout) {
        if (target == null || timeout == null) {
            throw new DatabaseValidationException("redo wait target/timeout must not be null");
        }
        if (timeout.isNegative()) {
            throw new DatabaseValidationException("redo wait timeout must not be negative: " + timeout);
        }
        lock.lock();
        try {
            if (flushedToDiskLsn.value() >= target.value()) {
                return true;
            }
            long nanos = timeoutNanos(timeout);
            while (flushedToDiskLsn.value() < target.value()) {
                if (nanos <= 0) {
                    return false;
                }
                try {
                    nanos = flushedAdvanced.awaitNanos(nanos);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
            return true;
        } finally {
            lock.unlock();
        }
    }

    private static long timeoutNanos(Duration timeout) {
        long seconds = timeout.getSeconds();
        int nanos = timeout.getNano();
        if (seconds > Long.MAX_VALUE / NANOS_PER_SECOND) {
            return Long.MAX_VALUE;
        }
        long base = seconds * NANOS_PER_SECOND;
        if (Long.MAX_VALUE - base < nanos) {
            return Long.MAX_VALUE;
        }
        return base + nanos;
    }

    /** 内存 buffer 的不可变快照（测试/诊断用）。内存模式为累积全量；durable 模式仅含尚未 flush 的记录。 */
    public List<RedoRecord> bufferedRecords() {
        lock.lock();
        try {
            return List.copyOf(buffer);
        } finally {
            lock.unlock();
        }
    }

    /** 已分配 LSN 的批次快照（测试/诊断用）。内存模式为累积全量；durable 模式仅含尚未 flush 的批次。 */
    public List<RedoLogBatch> bufferedBatches() {
        lock.lock();
        try {
            return List.copyOf(batches);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 把 pending 批次顺序写到 OS page cache（不 fsync），并推进 {@link #writtenToDiskLsn}（单调）。
     *
     * <p>必须在持有 {@code ioLock} 时调用：实际文件 write 期间不持状态锁，故前台 append 可继续预留 LSN/入 pending。
     * {@link #write()} 与 {@link #flush()} 共用本方法，保证两条路径推进 write 边界的逻辑一致；推进后唤醒
     * {@link #writtenAdvanced} 上的 {@code waitWritten} 等待者。
     */
    private void drainPendingToWritten() {
        while (true) {
            RedoLogBatch batch;
            lock.lock();
            try {
                if (pendingBatches.isEmpty()) {
                    break;
                }
                batch = pendingBatches.get(0);
            } finally {
                lock.unlock();
            }
            io.write(batch);
            lock.lock();
            try {
                removeWrittenPendingBatch(batch);
                writtenNotFlushedBatches.add(batch);
            } finally {
                lock.unlock();
            }
        }
        Lsn written = io.writtenToDiskLsn();
        lock.lock();
        try {
            if (written.value() > writtenToDiskLsn.value()) {
                writtenToDiskLsn = written;
                writtenAdvanced.signalAll();
            }
        } finally {
            lock.unlock();
        }
    }

    private void removeWrittenPendingBatch(RedoLogBatch batch) {
        if (!pendingBatches.isEmpty() && pendingBatches.get(0).equals(batch)) {
            pendingBatches.remove(0);
            return;
        }
        if (!pendingBatches.remove(batch)) {
            throw new DatabaseValidationException("written redo batch no longer pending: " + batch.range());
        }
    }

    private void clearFlushedDiagnostics() {
        for (RedoLogBatch batch : writtenNotFlushedBatches) {
            batches.remove(batch);
            for (int i = 0; i < batch.records().size() && !buffer.isEmpty(); i++) {
                buffer.remove(0);
            }
        }
        writtenNotFlushedBatches.clear();
    }

    /** 生产 redo IO 适配器：复用现有 writer/flusher，但由 manager 外层 ioLock 串行调用。 */
    private static final class RepositoryRedoLogIo implements RedoLogIo {
        private final RedoLogWriter writer;
        private final RedoLogFlusher flusher;

        private RepositoryRedoLogIo(RedoLogFileRepository repository) {
            this.writer = new RedoLogWriter(repository);
            this.flusher = new RedoLogFlusher(repository);
        }

        @Override
        public Lsn write(RedoLogBatch batch) {
            return writer.write(batch);
        }

        @Override
        public Lsn writtenToDiskLsn() {
            return writer.writtenToDiskLsn();
        }

        @Override
        public Lsn flushTo(Lsn target) {
            return flusher.flushTo(target);
        }
    }
}
