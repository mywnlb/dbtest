package cn.zhangyis.db.storage.buf;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 单个 {@link BufferPoolInstance} 的内部锁集合。
 *
 * <p>13.1d 已拆出 free/LRU/flush 三条 list 锁：{@code pageHashLock} 只保护 page hash 映射，
 * {@link BufferFrame#frameMutex} 保护单帧元数据，list 锁只保护各自链表结构。旧 {@code lockMetadata} 方法保留为
 * 包内测试兼容入口，生产路径应使用更具体的 list 锁方法。
 *
 * <p><b>锁顺序</b>：{@code pageHashLock -> frameMutex -> freeListLock/lruListLock/flushListLock -> pageLatch}。
 * 进入 PageStore 物理 IO、DirtyVictimFlusher、PageLoadFuture 等可能阻塞路径前必须释放上述所有 Buffer Pool 内部锁。
 */
final class BufferPoolInstanceLatchSet {

    /** 保护本分片 PageId→frame/loading 映射的短锁；不得跨物理 IO 或 future wait 持有。 */
    private final ReentrantLock pageHashLock = new ReentrantLock();

    /** 保护 free frame 队列的短锁；不得跨 frame 等待、PageStore IO 或 victim flush 持有。 */
    private final ReentrantLock freeListLock = new ReentrantLock();

    /** 保护 midpoint LRU old/new 子链的短锁；调用 ReplacementPolicy 必须持有它。 */
    private final ReentrantLock lruListLock = new ReentrantLock();

    /** 保护真实 flush list 的短锁；只维护 dirty 页定位与 LSN 边界，不保护页体。 */
    private final ReentrantLock flushListLock = new ReentrantLock();

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

    /** 进入单 frame 元数据临界区。调用方必须用 try/finally 调用 {@link #unlockFrame(BufferFrame)}。
     * @param frame 已固定的页面、frame 或页头视图；不得为 {@code null}，必须指向目标 PageId，并在访问期间持有契约要求的 fix/latch
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    void lockFrame(BufferFrame frame) {
        if (frame == null) {
            throw new DatabaseValidationException("buffer frame must not be null");
        }
        frame.frameMutex.lock();
        HOLD_COUNTS.get().frameDepth++;
    }

    /** 退出单 frame 元数据临界区。
     *
     * @param frame 已固定的页面、frame 或页头视图；不得为 {@code null}，必须指向目标 PageId，并在访问期间持有契约要求的 fix/latch
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    void unlockFrame(BufferFrame frame) {
        if (frame == null) {
            throw new DatabaseValidationException("buffer frame must not be null");
        }
        HoldCounts counts = HOLD_COUNTS.get();
        counts.frameDepth--;
        frame.frameMutex.unlock();
    }

    /** 进入 free list 临界区。调用方必须用 try/finally 调用 {@link #unlockFreeList()}。 */
    void lockFreeList() {
        freeListLock.lock();
        HOLD_COUNTS.get().freeListDepth++;
    }

    /** 退出 free list 临界区。 */
    void unlockFreeList() {
        HoldCounts counts = HOLD_COUNTS.get();
        counts.freeListDepth--;
        freeListLock.unlock();
    }

    /** 进入 LRU list 临界区。调用方必须用 try/finally 调用 {@link #unlockLruList()}。 */
    void lockLruList() {
        lruListLock.lock();
        HOLD_COUNTS.get().lruListDepth++;
    }

    /** 退出 LRU list 临界区。 */
    void unlockLruList() {
        HoldCounts counts = HOLD_COUNTS.get();
        counts.lruListDepth--;
        lruListLock.unlock();
    }

    /** 进入 flush list 临界区。调用方必须用 try/finally 调用 {@link #unlockFlushList()}。 */
    void lockFlushList() {
        flushListLock.lock();
        HOLD_COUNTS.get().flushListDepth++;
    }

    /** 退出 flush list 临界区。 */
    void unlockFlushList() {
        HoldCounts counts = HOLD_COUNTS.get();
        counts.flushListDepth--;
        flushListLock.unlock();
    }

    /** 兼容入口：一次性持有三条 list 锁，仅供旧测试验证“list 锁未释放不能进 IO”的总守卫。 */
    void lockMetadata() {
        lockFreeList();
        lockLruList();
        lockFlushList();
        HOLD_COUNTS.get().metadataDepth++;
    }

    /** 退出兼容 list/meta 临界区。 */
    void unlockMetadata() {
        HoldCounts counts = HOLD_COUNTS.get();
        counts.metadataDepth--;
        unlockFlushList();
        unlockLruList();
        unlockFreeList();
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
     * @throws BufferPoolLatchViolationException 页固定、闩锁、淘汰或 frame 代际校验失败时抛出；调用方应释放已持 Guard 后重试或终止操作
     */
    void assertMetadataUnlocked(String operation) {
        HoldCounts counts = HOLD_COUNTS.get();
        if (counts.pageHashDepth > 0 || counts.frameDepth > 0 || counts.metadataDepth > 0
                || counts.freeListDepth > 0 || counts.lruListDepth > 0 || counts.flushListDepth > 0) {
            throw new BufferPoolLatchViolationException(
                    "buffer pool internal lock held while entering " + operation);
        }
    }

    /** 测试与 IO 边界诊断用：当前线程是否仍持有 pageHashLock 或任一 frameMutex。
     *
     * @return {@code currentThreadHoldsPageHashOrFrameLock} 成功完成其命名的受控动作并发布结果时为 {@code true}；未命中、未执行或状态竞争失败时为 {@code false}
     */
    static boolean currentThreadHoldsPageHashOrFrameLock() {
        HoldCounts counts = HOLD_COUNTS.get();
        return counts.pageHashDepth > 0 || counts.frameDepth > 0;
    }

    /** ThreadLocal 内的持锁深度计数，支持同线程可重入锁重入。 */
    private static final class HoldCounts {
        /**
         * 记录 {@code pageHashDepth} 的权威数值状态；仅由本类受控路径更新，取值范围和特殊值遵循所属格式或状态机，溢出必须拒绝。
         */
        private int pageHashDepth;
        /**
         * 记录 {@code frameDepth} 的权威数值状态；仅由本类受控路径更新，取值范围和特殊值遵循所属格式或状态机，溢出必须拒绝。
         */
        private int frameDepth;
        /**
         * 记录 {@code metadataDepth} 的权威数值状态；仅由本类受控路径更新，取值范围和特殊值遵循所属格式或状态机，溢出必须拒绝。
         */
        private int metadataDepth;
        /**
         * 记录 {@code freeListDepth} 的权威数值状态；仅由本类受控路径更新，取值范围和特殊值遵循所属格式或状态机，溢出必须拒绝。
         */
        private int freeListDepth;
        /**
         * 记录 {@code lruListDepth} 的权威数值状态；仅由本类受控路径更新，取值范围和特殊值遵循所属格式或状态机，溢出必须拒绝。
         */
        private int lruListDepth;
        /**
         * 记录 {@code flushListDepth} 的权威数值状态；仅由本类受控路径更新，取值范围和特殊值遵循所属格式或状态机，溢出必须拒绝。
         */
        private int flushListDepth;
    }
}
