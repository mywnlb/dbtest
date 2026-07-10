package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.ArrayDeque;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 已提交 undo log 的 history list（设计 §5.6/§7.4，purge 内存版）。两条独立队列：
 * <ul>
 *   <li><b>committed</b>：含 update/delete undo 的已提交事务，按提交序 FIFO 入队（提交序 = TransactionNo 单调序）。
 *       purge 从队首按 TransactionNo boundary 处理，遇首个未达 boundary 即停。</li>
 *   <li><b>insertReclaim</b>：纯 insert undo 段（提交即可回收、无 boundary），purge 直接排空 dropUndoSegment。</li>
 * </ul>
 *
 * <p><b>并发</b>：生产者是 commit（{@code UndoLogManager.onCommit}），消费者是 {@code PurgeCoordinator}；用一把
 * {@link ReentrantLock} 短锁保护两队列，锁内只增删队列、不做 IO、不等待。禁止 {@code synchronized}（项目约束）。
 * peek/poll 分离，使 purge 能"先 peek 处理、成功后才 poll"实现 per-entry 原子（失败保留队首、停批）。
 *
 * <p><b>本片简化</b>：内存版，无持久化 / 无 crash recovery 重建 / 单 rollback segment 单链（多 rseg、持久 rseg
 * header、恢复期 history 重建留后续片）。
 */
public final class HistoryList {

    /** 保护两队列的短锁。 */
    private final ReentrantLock lock = new ReentrantLock();
    /** 含 update/delete undo 的已提交 undo log，按提交序 FIFO。 */
    private final ArrayDeque<HistoryEntry> committed = new ArrayDeque<>();
    /** 纯 insert undo 段回收队列（无序，提交即可回收）。 */
    private final ArrayDeque<InsertReclaimEntry> insertReclaim = new ArrayDeque<>();

    /** 提交时把含 update/delete undo 的事务挂入 history 队尾（提交序）。 */
    public void submitCommitted(HistoryEntry entry) {
        if (entry == null) {
            throw new DatabaseValidationException("history entry must not be null");
        }
        lock.lock();
        try {
            committed.addLast(entry);
        } finally {
            lock.unlock();
        }
    }

    /** 提交时把纯 insert undo 段挂入回收队列。 */
    public void submitInsertReclaim(InsertReclaimEntry entry) {
        if (entry == null) {
            throw new DatabaseValidationException("insert reclaim entry must not be null");
        }
        lock.lock();
        try {
            insertReclaim.addLast(entry);
        } finally {
            lock.unlock();
        }
    }

    /** 查看 committed 队首（最老提交 = purge 下一候选），不移除。purge 先 peek+boundary 判定再决定是否处理。 */
    public Optional<HistoryEntry> peekCommitted() {
        lock.lock();
        try {
            return Optional.ofNullable(committed.peekFirst());
        } finally {
            lock.unlock();
        }
    }

    /** 移除并返回 committed 队首（purge 成功处理完一条后调用，实现 per-entry 原子）。 */
    public Optional<HistoryEntry> pollCommitted() {
        lock.lock();
        try {
            return Optional.ofNullable(committed.pollFirst());
        } finally {
            lock.unlock();
        }
    }

    /** 移除并返回一条 insert-reclaim 条目（无序）；空则 empty，供 purge 排空。 */
    public Optional<InsertReclaimEntry> pollInsertReclaim() {
        lock.lock();
        try {
            return Optional.ofNullable(insertReclaim.pollFirst());
        } finally {
            lock.unlock();
        }
    }

    /**
     * 查看 insert-reclaim 队首但不移除。purge 必须先成功 drop segment 再 poll；若先 poll 后 IO 失败，
     * 纯 insert undo 段会永久丢失回收任务。
     */
    public Optional<InsertReclaimEntry> peekInsertReclaim() {
        lock.lock();
        try {
            return Optional.ofNullable(insertReclaim.peekFirst());
        } finally {
            lock.unlock();
        }
    }

    /** 当前 committed（update/delete）队列长度。 */
    public int committedSize() {
        lock.lock();
        try {
            return committed.size();
        } finally {
            lock.unlock();
        }
    }

    /** 当前 insert-reclaim 队列长度。 */
    public int insertReclaimSize() {
        lock.lock();
        try {
            return insertReclaim.size();
        } finally {
            lock.unlock();
        }
    }
}
