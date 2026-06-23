package cn.zhangyis.db.storage.fil;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.SpaceId;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 每表空间 operation-level 准入控制器。
 *
 * <p>普通 page/FSP/flush 路径获取共享 lease；truncate/discard 获取独占 lease。公平读写锁避免持续普通 IO
 * 饿死维护操作。锁只按 {@link SpaceId} 分片，不使用全局大锁；等待有统一超时且支持线程中断。
 * 独占 owner 可在同线程内重入共享锁，供截断服务调用维护 MTR/flush，但不得把 lease 跨线程转交。
 */
public final class TablespaceAccessController {

    /** 生产默认等待上界；调用方超时后应回滚/重试，不能无界卡住关闭或恢复。 */
    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30);

    /** 每个 SpaceId 独立公平锁；映射只增长到实例打开过的空间数。 */
    private final ConcurrentMap<SpaceId, ReentrantReadWriteLock> locks = new ConcurrentHashMap<>();

    /** 单次 lease 获取等待上界。 */
    private final long timeoutNanos;

    public TablespaceAccessController() {
        this(DEFAULT_TIMEOUT);
    }

    public TablespaceAccessController(Duration timeout) {
        if (timeout == null || timeout.isZero() || timeout.isNegative()) {
            throw new DatabaseValidationException("tablespace access timeout must be positive");
        }
        try {
            this.timeoutNanos = timeout.toNanos();
        } catch (ArithmeticException overflow) {
            throw new DatabaseValidationException("tablespace access timeout is too large", overflow);
        }
    }

    /**
     * 获取普通访问共享 lease。MTR 应在第一次 fix 该空间页时获取，并晚于所有 page guard 释放。
     *
     * @param spaceId 目标表空间。
     * @return 同线程关闭的共享 lease。
     */
    public TablespaceAccessLease acquireShared(SpaceId spaceId) {
        return acquire(spaceId, false);
    }

    /**
     * 获取生命周期维护独占 lease。返回前已 drain 所有其它线程共享/独占 owner。
     *
     * @param spaceId 目标表空间。
     * @return 同线程关闭的独占 lease。
     */
    public TablespaceAccessLease acquireExclusive(SpaceId spaceId) {
        return acquire(spaceId, true);
    }

    private TablespaceAccessLease acquire(SpaceId spaceId, boolean exclusive) {
        if (spaceId == null) {
            throw new DatabaseValidationException("tablespace access space id must not be null");
        }
        ReentrantReadWriteLock rw = locks.computeIfAbsent(spaceId, ignored -> new ReentrantReadWriteLock(true));
        Lock lock = exclusive ? rw.writeLock() : rw.readLock();
        try {
            if (!lock.tryLock(timeoutNanos, TimeUnit.NANOSECONDS)) {
                throw new TablespaceAccessTimeoutException("timed out acquiring "
                        + (exclusive ? "exclusive" : "shared") + " tablespace lease: " + spaceId.value());
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new TablespaceAccessTimeoutException("interrupted acquiring tablespace lease: "
                    + spaceId.value(), interrupted);
        }
        Thread owner = Thread.currentThread();
        AtomicBoolean closed = new AtomicBoolean();
        return () -> {
            if (Thread.currentThread() != owner) {
                throw new DatabaseValidationException("tablespace lease must be closed by owner thread");
            }
            if (closed.compareAndSet(false, true)) {
                lock.unlock();
            }
        };
    }
}
