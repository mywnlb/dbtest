package cn.zhangyis.db.storage.trx.lock;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.TransactionId;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 已授予事务锁的释放句柄。句柄采用 RAII 风格，调用 {@link #close()} 会释放这一把锁；
 * 事务结束时也可以使用 {@link LockManager#releaseAll(TransactionId)} 批量释放，批量释放后旧句柄再次 close 为 no-op。
 */
public final class LockHandle implements AutoCloseable {

    /** 创建该句柄的 LockManager，负责实际修改锁表。 */
    private final LockManager manager;

    /** 句柄对应的内部请求 id；仅用于在分片锁表中精确删除一把锁。 */
    private final long requestId;

    /** 持锁事务。 */
    private final TransactionId owner;

    /** 已授予锁资源。 */
    private final TransactionLockKey key;

    /** 已授予锁模式。 */
    private final TransactionLockMode mode;

    /** 防止 close 与 releaseAll 重复释放同一请求。 */
    private final AtomicBoolean closed = new AtomicBoolean(false);

    LockHandle(LockManager manager, long requestId, TransactionId owner, TransactionLockKey key, TransactionLockMode mode) {
        if (manager == null) {
            throw new DatabaseValidationException("lock handle manager must not be null");
        }
        if (owner == null || owner.isNone()) {
            throw new DatabaseValidationException("lock handle owner must be a real transaction id");
        }
        if (key == null) {
            throw new DatabaseValidationException("lock handle key must not be null");
        }
        if (mode == null) {
            throw new DatabaseValidationException("lock handle mode must not be null");
        }
        this.manager = manager;
        this.requestId = requestId;
        this.owner = owner;
        this.key = key;
        this.mode = mode;
    }

    /** 返回持锁事务 id。 */
    public TransactionId owner() {
        return owner;
    }

    /** 返回锁资源 key。 */
    public TransactionLockKey key() {
        return key;
    }

    /** 返回锁模式。 */
    public TransactionLockMode mode() {
        return mode;
    }

    long requestId() {
        return requestId;
    }

    boolean markClosedByCaller() {
        return closed.compareAndSet(false, true);
    }

    void markReleasedByManager() {
        closed.set(true);
    }

    /**
     * 释放这一把已授予事务锁。释放操作幂等；若事务级 releaseAll 已经清理该锁，本方法不会再次修改锁表。
     */
    @Override
    public void close() {
        manager.release(this);
    }
}
