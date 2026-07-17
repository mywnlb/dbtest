package cn.zhangyis.db.storage.trx;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 同一聚簇主键的 DML/purge 短物理协调租约。租约不代表事务 record lock，不可跨线程转移，也不得跨事务终态持有；
 * 调用方必须用 try-with-resources 包围“读取/更新多棵索引的若干短 MTR”，进入可能阻塞的事务锁等待前先释放。
 */
public final class PurgeDmlRowGuard implements AutoCloseable {

    /** 实际持有的公平 stripe lock；只允许创建 guard 的线程解锁。 */
    private final ReentrantLock lock;

    /** 防止异常清理与正常路径重复 close 导致二次 unlock；该标志不表达数据库权威状态。 */
    private final AtomicBoolean closed = new AtomicBoolean();

    /**
     * 在 manager 已成功取得 stripe lock 后创建租约。
     *
     * @param lock 当前线程已经持有的公平 stripe lock；guard 接管其唯一释放责任。
     */
    PurgeDmlRowGuard(ReentrantLock lock) {
        this.lock = lock;
    }

    /**
     * 释放 stripe lock。重复 close 是幂等 no-op；首次 close 必须发生在 owner 线程，违反时由显式锁抛错，
     * 不能静默掩盖跨线程资源生命周期错误。
     *
     * @throws IllegalMonitorStateException 首次关闭发生在非锁 owner 线程时由 {@link ReentrantLock} 抛出；
     *                                      该异常表示编程错误，不能转换成“释放成功”。
     */
    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            lock.unlock();
        }
    }
}
