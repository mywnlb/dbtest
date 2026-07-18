package cn.zhangyis.db.storage.fil.lock;


import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 单个已打开数据文件的生命周期读写闩。
 *
 * <p>{@code DataFileHandle} 的 read/write/extend/ensure-capacity/force 路径持共享闩，使普通页 IO、
 * 文件增长和 fsync 可以在句柄仍然打开时执行；truncate 与 close 持排他闩，必须等已有共享 owner
 * 全部退出，并阻止新的共享 owner 进入临界区。create/open 发生在 handle 发布之前，不由本闩保护；
 * drop/discard 的上层状态机最终通过 close 使用本闩排空物理 IO。</p>
 *
 * <p>它基于同一把非公平 {@link ReentrantReadWriteLock} 的 read/write view，不进入数据库事务锁系统
 * 或 Wait-For Graph。当前物理获取顺序是 lifecycle 在先，随后按路径获取 file-size 和/或 fsync；
 * 持有物理闩时不得等待事务锁。guard 绑定获取线程，必须按作用域恰好释放一次。</p>
 */
public final class TablespaceLifecycleLatch {

    /**
     * 底层非公平读写锁的共享 view。它保护“channel 尚未被 truncate/close 排他改变”的操作生命周期，
     * 不保护页 body；多个普通 IO owner 可同时持有。
     */
    private final ReentrantReadWriteLock.ReadLock sharedLock;

    /**
     * 与 {@link #sharedLock} 同源的排他 view。truncate/close 获得它时，旧共享 owner 已全部退出，
     * 当前线程成为唯一生命周期 owner。
     */
    private final ReentrantReadWriteLock.WriteLock exclusiveLock;

    /**
     * 创建一个非公平的生命周期闩。
     *
     * <p>非公平模式允许读路径获得较低调度开销，但不承诺已等待的排他 owner 严格先于后来读者；
     * 本类的限时入口供需要有界等待的调用方使用。</p>
     */
    public TablespaceLifecycleLatch() {
        // shared/exclusive 必须来自同一底层锁，才能让排他获取真实 drain 全部普通 IO owner。
        ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        this.sharedLock = lock.readLock();
        this.exclusiveLock = lock.writeLock();
    }

    /**
     * 不可中断地获取共享生命周期闩。
     *
     * <p>用于 page read/write、文件增长和 force。成功后排他 truncate/close 无法进入，直到 guard
     * 关闭；该入口本身没有等待上界，需要有界失败语义的调用方应使用
     * {@link #tryAcquireShared(long, TimeUnit)}。</p>
     *
     * @return 绑定当前线程的一次性共享 guard；不会返回 {@code null}
     */
    public ResourceGuard acquireShared() {
        sharedLock.lock();
        return sharedLock::unlock;
    }

    /**
     * 在指定上界内可中断地尝试获取共享生命周期闩。
     *
     * <p>超时与中断都返回 {@code null}，且都没有取得共享 ownership；中断路径会恢复线程中断标记。
     * 成功返回的 guard 必须由获取线程恰好关闭一次。</p>
     *
     * @param timeout 最大等待时长；非正值表示只尝试立即获取
     * @param unit timeout 的非空时间单位
     * @return 成功时返回共享 guard；超时或中断时返回 {@code null}
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
     * 不可中断地获取排他生命周期闩。
     *
     * <p>用于物理 truncate 和 close；成功返回时，同一 handle 的其它共享/排他 owner 已全部退出，
     * 新普通 IO 会等待到 guard 关闭。该入口本身没有等待上界。</p>
     *
     * @return 绑定当前线程的一次性排他 guard；不会返回 {@code null}
     */
    public ResourceGuard acquireExclusive() {
        exclusiveLock.lock();
        return exclusiveLock::unlock;
    }

    /**
     * 在指定上界内可中断地尝试 drain 普通 IO 并取得排他生命周期闩。
     *
     * <p>超时或中断时返回 {@code null}，调用方没有获得执行 truncate/close 的权利；中断路径会恢复
     * 线程中断标记。成功 guard 必须由获取线程恰好关闭一次。</p>
     *
     * @param timeout 最大 drain 等待时长；非正值表示只尝试立即获取
     * @param unit timeout 的非空时间单位
     * @return 成功时返回排他 guard；超时或中断时返回 {@code null}
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
