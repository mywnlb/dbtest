package cn.zhangyis.db.storage.flush.checkpoint;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.redo.RedoCheckpointLabel;
import cn.zhangyis.db.storage.redo.RedoCheckpointStore;
import cn.zhangyis.db.storage.redo.RedoLogManager;

import java.util.concurrent.locks.ReentrantLock;

/**
 * fuzzy checkpoint 协调器。F1 默认构造器只计算和单调发布内存安全 checkpoint LSN；R2 可注入
 * {@link RedoCheckpointStore} 持久化 redo control label。它仍不触发后台 page cleaner，也不回收 redo 文件。
 *
 * <p>checkpoint 必须读取 {@link RedoLogManager#closedLsn()}，不能用 current LSN 代替。current 只说明 redo
 * 已分配到哪里，closed 才说明相关 dirty page 已发布到 Buffer Pool 的 flush 视图。
 */
public final class CheckpointCoordinator {

    private final BufferPool bufferPool;
    private final RedoLogManager redo;
    /** 可选 redo control store；为空时保持 F1 内存 checkpoint 语义。 */
    private final RedoCheckpointStore checkpointStore;
    private final ReentrantLock lock = new ReentrantLock();
    private Lsn lastCheckpointLsn = Lsn.of(0);

    public CheckpointCoordinator(BufferPool bufferPool, RedoLogManager redo) {
        this(bufferPool, redo, null);
    }

    /**
     * 创建可持久化 checkpoint 的协调器。传入 checkpointStore 时，安全 LSN 单调前进会先写 redo control label，
     * 写入成功后才发布内存 lastCheckpointLsn；control 写失败时保持旧 checkpoint，避免恢复起点虚高。
     */
    public CheckpointCoordinator(BufferPool bufferPool, RedoLogManager redo, RedoCheckpointStore checkpointStore) {
        if (bufferPool == null || redo == null) {
            throw new DatabaseValidationException("checkpoint buffer pool/redo must not be null");
        }
        this.bufferPool = bufferPool;
        this.redo = redo;
        this.checkpointStore = checkpointStore;
    }

    /**
     * 计算安全 checkpoint LSN：不能越过 Buffer Pool oldest dirty、redo closed/current、redo flushed 任一边界。
     *
     * @return 当前安全 checkpoint LSN。
     */
    public Lsn computeSafeCheckpointLsn() {
        Lsn flushed = redo.flushedToDiskLsn();
        Lsn closed = redo.closedLsn();
        if (!bufferPool.hasDirtyPages()) {
            return min(closed, flushed);
        }
        Lsn oldestDirty = bufferPool.oldestDirtyLsnOr(flushed);
        return min(oldestDirty, closed, flushed);
    }

    /**
     * 单调推进内存 checkpoint。若当前安全边界没有超过 lastCheckpointLsn，则保持旧值。
     *
     * @return 推进后的 lastCheckpointLsn。
     */
    public Lsn advanceCheckpoint() {
        Lsn safe = computeSafeCheckpointLsn();
        lock.lock();
        try {
            if (safe.value() > lastCheckpointLsn.value()) {
                if (checkpointStore != null) {
                    checkpointStore.write(RedoCheckpointLabel.of(safe, redo.currentLsn(), System.currentTimeMillis()));
                }
                lastCheckpointLsn = safe;
            }
            return lastCheckpointLsn;
        } finally {
            lock.unlock();
        }
    }

    /** 最近一次发布的内存 checkpoint LSN。 */
    public Lsn lastCheckpointLsn() {
        lock.lock();
        try {
            return lastCheckpointLsn;
        } finally {
            lock.unlock();
        }
    }

    private static Lsn min(Lsn a, Lsn b, Lsn c) {
        long value = Math.min(a.value(), Math.min(b.value(), c.value()));
        return Lsn.of(value);
    }

    private static Lsn min(Lsn a, Lsn b) {
        return Lsn.of(Math.min(a.value(), b.value()));
    }
}
