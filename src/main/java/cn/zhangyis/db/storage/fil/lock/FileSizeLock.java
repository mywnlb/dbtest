package cn.zhangyis.db.storage.fil.lock;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 单个数据文件的大小变更排他锁。
 *
 * <p>{@code DataFileHandle} 在 extend、ensure-capacity 和 truncate 路径中先持有 lifecycle
 * 共享或独占锁，再持有本锁。它串行化文件尾范围分配/零填充、物理 truncate 和
 * {@code currentSizeInPages} 发布，防止两个大小变更基于同一旧边界重复操作。本锁不保护 FSP
 * 的 {@code freeLimit}，也不负责页内容并发。</p>
 *
 * <p>这是基于 {@link ReentrantLock} 的进程内物理锁，不进入事务死锁检测。持有期间只允许执行
 * 当前文件的短尺寸计算和 IO，不得等待 page latch 或事务锁；guard 必须由获取锁的线程关闭。</p>
 */
public final class FileSizeLock {

    /**
     * 文件大小变更的独占 ownership；owner 是正在扩展、确保容量或截断该文件的线程。
     * {@link ReentrantLock} 允许同线程重入，但每次成功获取都必须恰好关闭对应 guard。
     */
    private final ReentrantLock lock = new ReentrantLock();

    /**
     * 不可中断地获取文件大小排他锁。
     *
     * <p>当前数据文件内部的 extend/ensure-capacity/truncate 路径使用该入口，并通过外层
     * try-with-resources 保证 IO 异常时解锁。调用方必须先取得对应 lifecycle 锁，且不得在持有
     * 本锁时等待 page latch 或事务锁。</p>
     *
     * @return 绑定当前 owner 线程的一次性释放守卫；不会返回 {@code null}
     */
    public ResourceGuard acquire() {
        lock.lock();
        return lock::unlock;
    }

    /**
     * 在指定上界内可中断地尝试获取文件大小排他锁。
     *
     * <p>超时和中断都以 {@code null} 表示；中断路径会恢复当前线程中断标记。返回 guard 后，
     * 必须由当前获取线程关闭且只能关闭一次。</p>
     *
     * @param timeout 最大等待时长；非正值表示只尝试立即获取
     * @param unit timeout 的非空时间单位
     * @return 成功时返回绑定当前线程的释放守卫；等待到期或中断时返回 {@code null}
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
