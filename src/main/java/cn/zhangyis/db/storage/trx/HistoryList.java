package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.ArrayDeque;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 已提交 update/delete undo log 的 history list（设计 §5.6/§7.4，purge 内存投影）。事务按提交序 FIFO 入队，
 * purge 从队首按 TransactionNo boundary 处理；纯 insert undo 在 commit finalization 中直接 drop，不进入 history。
 *
 * <p><b>并发</b>：生产者是 commit（{@code UndoLogManager.onCommit}），消费者是 {@code PurgeCoordinator}；用一把
 * {@link ReentrantLock} 短锁保护队列，锁内只增删、不做 IO、不等待。禁止 {@code synchronized}。peek/complete
 * 分离，使 purge 先处理物理任务，finalization 成功后再按 expected identity 精确摘队首。
 *
 * <p><b>恢复</b>：队列自身不持久化；page3 中仍占用且 first-page state=COMMITTED 的 slot 是恢复重建权威。
 */
public final class HistoryList {

    /** 保护 committed FIFO 的短锁。 */
    private final ReentrantLock lock = new ReentrantLock();
    /** 含 update/delete undo 的已提交 undo log，按提交序 FIFO。 */
    private final ArrayDeque<HistoryEntry> committed = new ArrayDeque<>();

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

    /** 查看 committed 队首（最老提交 = purge 下一候选），不移除。purge 先 peek+boundary 判定再决定是否处理。 */
    public Optional<HistoryEntry> peekCommitted() {
        lock.lock();
        try {
            return Optional.ofNullable(committed.peekFirst());
        } finally {
            lock.unlock();
        }
    }

    /**
     * finalization commit 后精确移除 expected committed 队首。错序或重复完成代表内存投影与 page3 生命周期漂移，
     * 必须 fail-closed，不能静默摘除其它事务 history。
     */
    public void completeCommitted(HistoryEntry expected) {
        if (expected == null) {
            throw new DatabaseValidationException("completed history entry must not be null");
        }
        lock.lock();
        try {
            HistoryEntry current = committed.peekFirst();
            if (!expected.equals(current)) {
                throw new DatabaseValidationException(
                        "committed history head mismatch: expected=" + expected + ", current=" + current);
            }
            committed.removeFirst();
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

}
