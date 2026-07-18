package cn.zhangyis.db.storage.fil.lock;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Semaphore;

/**
 * 单个数据文件的 fsync 串行化锁。
 *
 * <p>每个 {@code DataFileHandle} 独立持有一个实例，把同一文件上的并发
 * {@code FileChannel.force(true)} 限制为一次，避免 page cleaner、显式 flush、truncate 或 shutdown
 * 同时推动相同 channel 的持久化。不同数据文件仍可并发 force。</p>
 *
 * <p>该锁只保护物理 force 调用，不证明 page LSN 已通过 WAL gate，也不推进 checkpoint。它不进入
 * 事务 Wait-For Graph；调用方先取得 lifecycle 锁，truncate 路径还会先取得 file-size 锁，然后才能
 * 获取本 permit，且持有期间不得等待 page latch 或事务锁。</p>
 */
public final class FsyncLock {

    /**
     * 单文件 fsync 的唯一 permit，是本锁的权威占用状态。公平信号量使已经排队的后台/前台 force
     * 按等待顺序竞争；它不可重入，同线程嵌套获取也必须等待前一个 permit 释放。
     *
     * <p>Semaphore 不强制 owner 线程释放，因此正确性依赖 guard 的词法作用域：每个成功获取返回的
     * guard 必须且只能关闭一次，否则重复 release 会破坏“并发数为 1”的不变量。</p>
     */
    private final Semaphore permit = new Semaphore(1, true);

    /**
     * 不可中断地等待并取得 fsync permit。
     *
     * <p>{@code DataFileHandle.force/truncateTo} 在已持有相应前置物理锁后使用该入口，并把返回值放入
     * try-with-resources。等待期间收到中断不会提前退出；成功返回时线程的中断状态仍由并发工具保留。</p>
     *
     * @return 关闭时释放唯一 permit 的一次性 guard；不会返回 {@code null}
     */
    public ResourceGuard acquire() {
        permit.acquireUninterruptibly();
        return permit::release;
    }

    /**
     * 在指定上界内可中断地尝试取得 fsync permit。
     *
     * <p>超时与中断都返回 {@code null}，中断路径同时恢复线程中断标记；两种失败均没有取得 permit。
     * 返回 guard 后必须恰好关闭一次。</p>
     *
     * @param timeout 等待上限；不能为空、不能为负数，并且必须可换算为 {@code long} 纳秒；
     *                零表示只尝试立即获取
     * @return 成功返回释放 permit 的 guard；超时或中断返回 {@code null}
     * @throws DatabaseValidationException timeout 为空或为负数时抛出；permit 状态不变
     */
    public ResourceGuard tryAcquire(Duration timeout) {
        if (timeout == null || timeout.isNegative()) {
            throw new DatabaseValidationException("fsync lock timeout must not be null/negative");
        }
        try {
            if (permit.tryAcquire(timeout.toNanos(), TimeUnit.NANOSECONDS)) {
                return permit::release;
            }
            return null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }
}
