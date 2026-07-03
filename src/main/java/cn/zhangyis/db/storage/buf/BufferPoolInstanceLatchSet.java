package cn.zhangyis.db.storage.buf;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 单个 {@link BufferPoolInstance} 的内部锁集合。
 *
 * <p>13.1c 已拆出第一组真实子锁：{@code pageHashLock} 只保护 page hash 映射，{@link BufferFrame#frameMutex}
 * 保护单帧元数据。旧 {@code metadataLock} 暂时降级为 list/meta compatibility lock，用于 free list、LRU 和跨帧扫描，
 * 后续 13.1d 再拆成 {@code freeListLock/lruListLock/flushListLock}。
 *
 * <p><b>锁顺序</b>：{@code pageHashLock -> frameMutex -> metadataLock(compat) -> pageLatch}。进入 PageStore 物理
 * IO、DirtyVictimFlusher、PageLoadFuture 等可能阻塞路径前必须释放上述所有 Buffer Pool 内部锁。
 */
final class BufferPoolInstanceLatchSet {

    /** 保护本分片 PageId→frame/loading 映射的短锁；不得跨物理 IO 或 future wait 持有。 */
    private final ReentrantLock pageHashLock = new ReentrantLock();

    /** 兼容锁：13.1c 暂时保护 free list、LRU replacement policy 和跨 frame 扫描。13.1d 会继续拆分。 */
    private final ReentrantLock metadataLock = new ReentrantLock();

    /** drain 等待专用锁；不保护 frame 元数据，只承载 Condition，避免复用 metadata 大锁。 */
    private final ReentrantLock drainWaitLock = new ReentrantLock();

    /** fixCount 下降时通知 truncate/drop invalidation drain 继续检查；谓词由调用方重新扫描 frameMutex 下的 frame 元数据。 */
    private final Condition frameReleased = drainWaitLock.newCondition();

    /** 当前线程持有的 Buffer Pool 内部锁计数，供断言和测试观察，不参与并发控制决策。 */
    private static final ThreadLocal<HoldCounts> HOLD_COUNTS = ThreadLocal.withInitial(HoldCounts::new);

    /** 进入 page hash 临界区。调用方必须用 try/finally 调用 {@link #unlockPageHash()}。 */
    void lockPageHash() {
        pageHashLock.lock();
        HOLD_COUNTS.get().pageHashDepth++;
    }

    /** 退出 page hash 临界区。 */
    void unlockPageHash() {
        HoldCounts counts = HOLD_COUNTS.get();
        counts.pageHashDepth--;
        pageHashLock.unlock();
    }

    /** 进入单 frame 元数据临界区。调用方必须用 try/finally 调用 {@link #unlockFrame(BufferFrame)}。 */
    void lockFrame(BufferFrame frame) {
        if (frame == null) {
            throw new DatabaseValidationException("buffer frame must not be null");
        }
        frame.frameMutex.lock();
        HOLD_COUNTS.get().frameDepth++;
    }

    /** 退出单 frame 元数据临界区。 */
    void unlockFrame(BufferFrame frame) {
        if (frame == null) {
            throw new DatabaseValidationException("buffer frame must not be null");
        }
        HoldCounts counts = HOLD_COUNTS.get();
        counts.frameDepth--;
        frame.frameMutex.unlock();
    }

    /** 进入 list/meta 兼容临界区。调用方必须用 try/finally 调用 {@link #unlockMetadata()}。 */
    void lockMetadata() {
        metadataLock.lock();
        HOLD_COUNTS.get().metadataDepth++;
    }

    /** 退出 list/meta 兼容临界区。 */
    void unlockMetadata() {
        HoldCounts counts = HOLD_COUNTS.get();
        counts.metadataDepth--;
        metadataLock.unlock();
    }

    /**
     * 等待 frame release 信号。该 condition 不保护 fixCount 谓词；调用方每次返回后必须重新扫描目标 frame。
     * 为避免 release 信号在扫描和等待之间丢失导致长时间空等，单次等待最多 10ms，然后主动重查。
     *
     * @param nanosTimeout 剩余等待纳秒数。
     * @return 剩余纳秒数估计值。
     * @throws InterruptedException 等待被中断时抛出，由调用方恢复中断位并转领域异常。
     */
    long awaitFrameReleased(long nanosTimeout) throws InterruptedException {
        long waitNanos = Math.min(nanosTimeout, 10_000_000L);
        drainWaitLock.lock();
        try {
            frameReleased.awaitNanos(waitNanos);
        } finally {
            drainWaitLock.unlock();
        }
        return nanosTimeout - waitNanos;
    }

    /** 唤醒等待 frame fixCount 下降的维护线程。调用方不需要持其它 Buffer Pool 内部锁。 */
    void signalFrameReleased() {
        drainWaitLock.lock();
        try {
            frameReleased.signalAll();
        } finally {
            drainWaitLock.unlock();
        }
    }

    /**
     * 确认当前线程没有持有 Buffer Pool 内部锁。用于进入物理 IO、dirty victim flush、PageLoadFuture wait 前的守卫。
     *
     * @param operation 即将执行的阻塞操作名，用于诊断消息。
     */
    void assertMetadataUnlocked(String operation) {
        HoldCounts counts = HOLD_COUNTS.get();
        if (counts.pageHashDepth > 0 || counts.frameDepth > 0 || counts.metadataDepth > 0) {
            throw new BufferPoolLatchViolationException(
                    "buffer pool internal lock held while entering " + operation);
        }
    }

    /** 测试与 IO 边界诊断用：当前线程是否仍持有 pageHashLock 或任一 frameMutex。 */
    static boolean currentThreadHoldsPageHashOrFrameLock() {
        HoldCounts counts = HOLD_COUNTS.get();
        return counts.pageHashDepth > 0 || counts.frameDepth > 0;
    }

    /** ThreadLocal 内的持锁深度计数，支持同线程可重入锁重入。 */
    private static final class HoldCounts {
        private int pageHashDepth;
        private int frameDepth;
        private int metadataDepth;
    }
}
