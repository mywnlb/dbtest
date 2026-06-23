package cn.zhangyis.db.storage.fil.lock;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * #3 文件大小锁（X-only）。保护 currentSizeInPages、freeLimit 与文件尾零填充：autoextend 在持有 Lifecycle(S)
 * 的同时排他持有它，串行化文件增长。持有它时不得等待任何 page latch（设计 §8.1），避免 autoextend 与 flush 互锁。
 *
 * <p>物理文件锁，不进死锁检测；基于 {@link ReentrantLock}。
 */
public final class FileSizeLock {

    /**
     * 文件大小排他锁。owner 为正在修改文件尾的线程（当前是 autoextend 增长，未来可包含 truncate 收缩）。
     */
    private final ReentrantLock lock = new ReentrantLock();

    /**
     * 获取文件大小锁（阻塞）。仅应在持有者（autoextend）确定会短促完成的前提下使用；需要超时上界时改用
     * {@link #tryAcquire(long, java.util.concurrent.TimeUnit)}，避免被异常卡住的扩展无限阻塞。
     *
     * @return 释放守卫，close 时解锁。
     */
    public ResourceGuard acquire() {
        lock.lock();
        return lock::unlock;
    }

    /**
     * 限时尝试获取文件大小锁。
     *
     * @param timeout 超时时长。
     * @param unit 时间单位。
     * @return 成功返回释放守卫，超时返回 null。
     */
    public ResourceGuard tryAcquire(long timeout, TimeUnit unit) {
        try {
            if (lock.tryLock(timeout, unit)) {
                return lock::unlock;
            }
            return null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }
}
