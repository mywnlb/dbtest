package cn.zhangyis.db.storage.fil.lock;


import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * #1 表空间生命周期闩（S/X）。保护 open/close/discard/drop/truncate 与普通 page IO 之间的生命周期关系：
 * 普通 read/write/flush/recovery 持 S（共享、可并发）；drop/truncate/discard/close 持 X（排他，获取即 drain 掉所有 S 持有者）。
 *
 * <p>它是物理文件锁，不进入数据库事务锁系统，也不进死锁检测；等待只靠 timeout/IO error/drain。
 * 基于 {@link ReentrantReadWriteLock}（禁用 synchronized，AGENTS.md）。
 *
 * <p>加锁顺序（设计 §8.1/§18）：Lifecycle → DataFileHandle(#2,预留) → FileSize → PageIoRange(#4,预留) → Fsync(#5,预留)。
 */
public final class TablespaceLifecycleLatch {

    /**
     * 共享锁：普通 IO 持有，允许多读并发。
     */
    private final ReentrantReadWriteLock.ReadLock sharedLock;

    /**
     * 排他锁：生命周期变更持有，排他于一切 IO。
     */
    private final ReentrantReadWriteLock.WriteLock exclusiveLock;

    public TablespaceLifecycleLatch() {
        // 非公平模式：X 等待时新 S 请求可能插队，换取 S 路径更低延迟与吞吐；如需对 drop/truncate 提供更强的
        // drain 前进保证（不被新读插队），可改为 new ReentrantReadWriteLock(true)。当前优先普通读路径，故选非公平。
        ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        this.sharedLock = lock.readLock();
        this.exclusiveLock = lock.writeLock();
    }

    /**
     * 获取共享 S 闩（阻塞），用于普通 page read/write/autoextend。
     *
     * <p>这是无限期阻塞获取，但符合 AGENTS.md “明确的唤醒条件”：S 仅被 drop/truncate/discard/close 这类
     * 短促且确定会 drain 释放的 X 操作排除，X 完成即唤醒。需要为可能异常卡住的 X 设置超时上界时，改用
     * {@link #tryAcquireShared(long, java.util.concurrent.TimeUnit)}。
     *
     * @return 释放守卫，close 时解 S。
     */
    public ResourceGuard acquireShared() {
        sharedLock.lock();
        return sharedLock::unlock;
    }

    /**
     * 限时尝试获取共享 S 闩。给普通 IO 一个超时逃生口：当 X 持有者异常卡住时，调用方可在超时后上报 IO 错误
     * 而非无限挂起（物理锁不进 Wait-For Graph，设计 §8.1）。
     *
     * @param timeout 超时时长。
     * @param unit 时间单位。
     * @return 成功返回释放守卫，超时返回 null。
     */
    public ResourceGuard tryAcquireShared(long timeout, TimeUnit unit) {
        try {
            if (sharedLock.tryLock(timeout, unit)) {
                return sharedLock::unlock;
            }
            return null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    /**
     * 获取排他 X 闩，用于 drop/truncate/discard/close。写锁获取即等待所有 S 持有者离开（drain）。
     *
     * @return 释放守卫，close 时解 X。
     */
    public ResourceGuard acquireExclusive() {
        exclusiveLock.lock();
        return exclusiveLock::unlock;
    }

    /**
     * 限时尝试获取排他 X 闩。物理锁不进 Wait-For Graph，等待必须有超时上界（设计 §8.1）。
     *
     * @param timeout 超时时长。
     * @param unit 时间单位。
     * @return 成功返回释放守卫，超时返回 null。
     */
    public ResourceGuard tryAcquireExclusive(long timeout, TimeUnit unit) {
        try {
            if (exclusiveLock.tryLock(timeout, unit)) {
                return exclusiveLock::unlock;
            }
            return null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }
}
