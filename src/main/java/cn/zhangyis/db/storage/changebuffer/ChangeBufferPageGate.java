package cn.zhangyis.db.storage.changebuffer;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 固定条带的 per-target buffer/merge/drain gate。相同 PageId 恒落同一显式 {@link ReentrantLock}；hash 碰撞只降低并行度，
 * 不影响正确性。所有获取均使用配置 timeout，lease 通过 try-with-resources 释放，不使用隐式 monitor。
 */
public final class ChangeBufferPageGate {

    /** 固定条带锁；构造后不扩容，避免 PageId→lock 映射在运行期漂移。 */
    private final ReentrantLock[] stripes;

    /**
     * @param stripeCount 正条带数；建议二次幂，但算法不依赖
     */
    public ChangeBufferPageGate(int stripeCount) {
        if (stripeCount <= 0) {
            throw new DatabaseValidationException("change buffer gate stripe count must be positive");
        }
        this.stripes = new ReentrantLock[stripeCount];
        for (int i = 0; i < stripeCount; i++) {
            stripes[i] = new ReentrantLock();
        }
    }

    /**
     * 在有界时间内取得目标页条带锁。等待者不得持事务行锁以外的资源；发布前 loader 若仍持 parent S latch，
     * buffer 路径只能以 parent S 定位并在提交后释放 gate，不能形成 gate→parent X 的反向等待。
     *
     * @param pageId 待串行化目标页；不得为 {@code null}
     * @param timeout 正等待上限；不得为 {@code null}
     * @return 必须关闭一次的可重入 lease
     * @throws ChangeBufferPageGateTimeoutException 超时或中断时抛出，未取得的锁无需释放
     */
    public Lease acquire(PageId pageId, Duration timeout) {
        if (pageId == null || timeout == null || timeout.isZero() || timeout.isNegative()) {
            throw new DatabaseValidationException("change buffer gate page/timeout is invalid");
        }
        long nanos;
        try {
            nanos = timeout.toNanos();
        } catch (ArithmeticException overflow) {
            nanos = Long.MAX_VALUE;
        }
        ReentrantLock lock = stripes[stripe(pageId)];
        boolean acquired;
        try {
            acquired = lock.tryLock(nanos, TimeUnit.NANOSECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new ChangeBufferPageGateTimeoutException(
                    "interrupted waiting for change buffer page gate " + pageId, interrupted);
        }
        if (!acquired) {
            throw new ChangeBufferPageGateTimeoutException(
                    "timed out waiting for change buffer page gate " + pageId + " after " + timeout);
        }
        return new Lease(pageId, lock, Thread.currentThread());
    }

    private int stripe(PageId pageId) {
        int hash = 31 * Integer.hashCode(pageId.spaceId().value()) + Long.hashCode(pageId.pageNo().value());
        return Math.floorMod(hash, stripes.length);
    }

    /** 一次条带锁所有权；owner 线程关闭且重复关闭被拒绝。 */
    public static final class Lease implements AutoCloseable {
        /** 诊断与重复关闭异常所需的稳定目标页 identity。 */
        private final PageId pageId;
        /** 本 lease 对应的一层可重入条带锁 hold；仅创建线程可以释放。 */
        private final ReentrantLock lock;
        /** 取得本层 hold 的唯一线程；用于在 ReentrantLock 抛裸 monitor ownership 异常前给出领域错误。 */
        private final Thread owner;
        /** 防止同一 lease 重复减少 hold count；只由 owner 线程访问。 */
        private boolean closed;

        private Lease(PageId pageId, ReentrantLock lock, Thread owner) {
            this.pageId = pageId;
            this.lock = lock;
            this.owner = owner;
        }

        /**
         * 释放本次可重入获取对应的一层 hold count。
         *
         * @throws DatabaseValidationException 非 owner 线程关闭或同一 lease 重复关闭时抛出，原 hold 保持不变
         */
        @Override
        public void close() {
            if (Thread.currentThread() != owner) {
                throw new DatabaseValidationException(
                        "change buffer page gate lease must be closed by its owner: " + pageId);
            }
            if (closed) {
                throw new DatabaseValidationException("change buffer page gate lease already closed: " + pageId);
            }
            closed = true;
            lock.unlock();
        }
    }
}
