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
 * <p><b>并发简化（已知瓶颈）</b>：{@link #flush()} 在持有 {@code lock} 期间执行 writer 写文件与 flusher fsync，
 * 而 {@link #append} 也抢同一把锁——即所有 append 会被一次 fsync 完全串行化阻塞。R1/R2 接受该简化（同步驱动、
 * 无后台线程）；待后台 writer/flusher 落地时应拆分「LSN 分配锁」与「write/flush 锁」，使 append 不被 fsync 阻塞。
 */
public final class RedoLogManager {

    private static final long NANOS_PER_SECOND = 1_000_000_000L;

    /** 保护 nextLsn 与 buffer 的互斥锁。 */
    private final ReentrantLock lock = new ReentrantLock();
    /** durability 等待条件；flush 推进 flushedToDiskLsn 后唤醒。 */
    private final Condition flushedAdvanced = lock.newCondition();
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
    /** 可选 writer；为空表示 D3 内存模式。 */
    private final RedoLogWriter writer;
    /** 可选 flusher；为空表示 D3 内存模式。 */
    private final RedoLogFlusher flusher;
    /** 已 fsync 的最高 LSN；D3 内存模式恒为 0。 */
    private Lsn flushedToDiskLsn = Lsn.of(0);
    /** 启动恢复边界是否已显式安装；只允许在首个新 append 前执行一次（同值调用幂等）。 */
    private boolean recoveredBoundaryInstalled;

    public RedoLogManager() {
        this(null, null);
    }

    private RedoLogManager(RedoLogWriter writer, RedoLogFlusher flusher) {
        this.writer = writer;
        this.flusher = flusher;
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
        return new RedoLogManager(new RedoLogWriter(repository), new RedoLogFlusher(repository));
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
                if (writer != null) {
                    pendingBatches.add(batch);
                }
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
        lock.lock();
        try {
            if (writer == null || flusher == null) {
                return flushedToDiskLsn;
            }
            while (!pendingBatches.isEmpty()) {
                RedoLogBatch batch = pendingBatches.get(0);
                writer.write(batch);
                pendingBatches.remove(0);
            }
            Lsn physicalFlushed = flusher.flushTo(writer.writtenToDiskLsn());
            // 新进程 writer/flusher 从 0 构造，但 restoreRecoveredBoundary 已安装历史 durable 边界；
            // 空 pending flush 绝不能把该边界降回 0。只有本进程新写出的更高 LSN 才推进。
            if (physicalFlushed.value() > flushedToDiskLsn.value()) {
                flushedToDiskLsn = physicalFlushed;
            }
            // durable 模式下 buffer/batches 仅作诊断快照；本轮 pending 已全部写盘并 fsync，
            // 这两个列表对恢复无意义，落盘后立即释放，防止长运行 append 累积导致内存无界增长。
            buffer.clear();
            batches.clear();
            flushedAdvanced.signalAll();
            return flushedToDiskLsn;
        } finally {
            lock.unlock();
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
            if (nextLsn != 0 || !buffer.isEmpty() || !batches.isEmpty() || !pendingBatches.isEmpty()) {
                throw new DatabaseValidationException(
                        "redo recovery boundary must be installed before any new append");
            }
            nextLsn = recoveredToLsn.value();
            flushedToDiskLsn = recoveredToLsn;
            recoveredBoundaryInstalled = true;
            flushedAdvanced.signalAll();
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
}
