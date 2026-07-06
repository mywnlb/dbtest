package cn.zhangyis.db.storage.fil.lock;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Semaphore;

/**
 * #5 data-file fsync 限流锁。它是每个 {@code DataFileHandle} 内部的物理文件锁，用来限制同一数据文件上并发
 * {@code FileChannel.force(true)} 的线程数为 1，避免 page cleaner、前台 flush 或 shutdown flush 重复推动同一文件
 * 的 fsync。该锁不进入事务 Wait-For Graph，只保护物理 IO；调用方必须按 Lifecycle/FileSize 之后的顺序获取。
 */
public final class FsyncLock {

    /**
     * 单文件 fsync permit。公平信号量让后台/前台 force 请求按等待顺序推进；选择 Semaphore 而非 ReentrantLock，
     * 是为了表达“一个 data file 同时只有一个 fsync permit”，即使同一线程误入嵌套 force 也不能绕过限流。
     */
    private final Semaphore permit = new Semaphore(1, true);

    /**
     * 获取 fsync permit。用于确定会短促完成的 {@code force(true)} 路径；如需给外部调用方提供明确超时，
     * 可使用 {@link #tryAcquire(Duration)}。
     *
     * @return 关闭时释放 permit 的 RAII guard。
     */
    public ResourceGuard acquire() {
        permit.acquireUninterruptibly();
        return permit::release;
    }

    /**
     * 限时获取 fsync permit。物理文件锁不参与死锁检测，因此测试、诊断或未来可取消 IO 路径必须能选择超时失败。
     *
     * @param timeout 等待上限，不能为 null 或负数。
     * @return 成功返回释放 guard；超时或中断返回 null，中断会恢复线程中断标志。
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
