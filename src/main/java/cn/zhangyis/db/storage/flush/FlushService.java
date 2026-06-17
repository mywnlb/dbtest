package cn.zhangyis.db.storage.flush;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.DirtyPageCandidate;
import cn.zhangyis.db.storage.redo.RedoCapacityDecision;
import cn.zhangyis.db.storage.redo.RedoCapacityPolicy;
import cn.zhangyis.db.storage.redo.RedoLogManager;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.LockSupport;

/**
 * Flush 模块门面。它把 R2 redo capacity pressure、F1 FlushCoordinator 和 CheckpointCoordinator 串起来，
 * 但不直接写 page image，也不读取 BufferPool 内部链表。
 */
public final class FlushService {

    /** dirty view 来源；F2 drain 会按 space 过滤候选。 */
    private final BufferPool bufferPool;
    /** 唯一执行 WAL gate、doublewrite 和 data file 写盘的组件。 */
    private final FlushCoordinator flushCoordinator;
    /** fuzzy checkpoint 边界计算和可选持久化 label。 */
    private final CheckpointCoordinator checkpointCoordinator;
    /** redo LSN 和 durable 边界来源。 */
    private final RedoLogManager redo;
    /** checkpoint age 到 pressure 的策略。 */
    private final RedoCapacityPolicy capacityPolicy;
    /** pressure 到本轮刷页数的策略。 */
    private final AdaptiveFlushPolicy adaptiveFlushPolicy;

    public FlushService(BufferPool bufferPool,
                        FlushCoordinator flushCoordinator,
                        CheckpointCoordinator checkpointCoordinator,
                        RedoLogManager redo,
                        RedoCapacityPolicy capacityPolicy,
                        AdaptiveFlushPolicy adaptiveFlushPolicy) {
        if (bufferPool == null || flushCoordinator == null || checkpointCoordinator == null
                || redo == null || capacityPolicy == null || adaptiveFlushPolicy == null) {
            throw new DatabaseValidationException("flush service dependencies must not be null");
        }
        this.bufferPool = bufferPool;
        this.flushCoordinator = flushCoordinator;
        this.checkpointCoordinator = checkpointCoordinator;
        this.redo = redo;
        this.capacityPolicy = capacityPolicy;
        this.adaptiveFlushPolicy = adaptiveFlushPolicy;
    }

    /**
     * 根据当前 redo capacity pressure 执行一轮 flush list 刷脏。没有压力时不刷页，也不推进 checkpoint；
     * 有压力时由策略决定 target LSN 和页数，刷完后尝试推进 checkpoint。
     *
     * @param maxPages 调用方允许本轮最多刷出的页数。
     * @return 本轮调度结果。
     */
    public FlushCycleResult flushForCapacity(int maxPages) {
        if (maxPages < 0) {
            throw new DatabaseValidationException("flush max pages must not be negative: " + maxPages);
        }
        Lsn before = checkpointCoordinator.lastCheckpointLsn();
        RedoCapacityDecision decision = capacityPolicy.evaluate(redo.currentLsn(), before);
        FlushAdvice advice = adaptiveFlushPolicy.plan(decision, maxPages);
        List<FlushResult> results = advice.shouldFlush()
                ? flushCoordinator.flushList(advice.targetLsn(), advice.maxPages())
                : List.of();
        Lsn after = advice.shouldFlush() ? checkpointCoordinator.advanceCheckpoint() : before;
        return new FlushCycleResult(decision, advice, results, before, after);
    }

    /**
     * Drain 指定 tablespace 的 dirty page。调用方应在进入本方法前阻止该 space 新写入；本方法只负责刷出现存 dirty，
     * 不获取 tablespace lifecycle X latch，也不参与 DDL 状态机。
     *
     * @param spaceId 目标 tablespace。
     * @param timeout 最大 drain 时间，0 表示只做一次超时检查并返回。
     * @return drain 结果。
     */
    public TablespaceDrainResult drainTablespace(SpaceId spaceId, Duration timeout) {
        if (spaceId == null || timeout == null) {
            throw new DatabaseValidationException("drain space/timeout must not be null");
        }
        if (timeout.isNegative()) {
            throw new DatabaseValidationException("drain timeout must not be negative: " + timeout);
        }
        long deadline = deadlineFromNow(timeout);
        List<FlushResult> results = new ArrayList<>();
        while (true) {
            List<PageId> targets = dirtyPagesInSpace(spaceId);
            if (targets.isEmpty()) {
                return new TablespaceDrainResult(spaceId, results, false,
                        checkpointCoordinator.advanceCheckpoint());
            }
            if (deadlineReached(deadline)) {
                return new TablespaceDrainResult(spaceId, results, true,
                        checkpointCoordinator.lastCheckpointLsn());
            }
            int cleanCountBefore = cleanCount(results);
            for (PageId pageId : targets) {
                if (deadlineReached(deadline)) {
                    return new TablespaceDrainResult(spaceId, results, true,
                            checkpointCoordinator.lastCheckpointLsn());
                }
                results.add(flushCoordinator.singlePageFlush(pageId));
            }
            checkpointCoordinator.advanceCheckpoint();
            if (cleanCount(results) == cleanCountBefore && !dirtyPagesInSpace(spaceId).isEmpty()) {
                parkBriefly(deadline);
            }
        }
    }

    private List<PageId> dirtyPagesInSpace(SpaceId spaceId) {
        return bufferPool.dirtyPageCandidates(Lsn.of(Long.MAX_VALUE), bufferPool.capacity())
                .stream()
                .map(DirtyPageCandidate::pageId)
                .filter(pageId -> pageId.spaceId().equals(spaceId))
                .toList();
    }

    private static int cleanCount(List<FlushResult> results) {
        int count = 0;
        for (FlushResult result : results) {
            if (result.status() == FlushResultStatus.CLEAN) {
                count++;
            }
        }
        return count;
    }

    private static void parkBriefly(long deadline) {
        if (deadline == Long.MAX_VALUE) {
            LockSupport.parkNanos(1_000_000L);
            return;
        }
        long remaining = deadline - System.nanoTime();
        if (remaining > 0) {
            LockSupport.parkNanos(Math.min(remaining, 1_000_000L));
        }
    }

    private static boolean deadlineReached(long deadline) {
        return deadline != Long.MAX_VALUE && System.nanoTime() - deadline >= 0;
    }

    private static long deadlineFromNow(Duration timeout) {
        long nanos = timeoutNanos(timeout);
        if (nanos == Long.MAX_VALUE) {
            return Long.MAX_VALUE;
        }
        long now = System.nanoTime();
        if (Long.MAX_VALUE - now < nanos) {
            return Long.MAX_VALUE;
        }
        return now + nanos;
    }

    private static long timeoutNanos(Duration timeout) {
        final long nanosPerSecond = 1_000_000_000L;
        long seconds = timeout.getSeconds();
        int nanos = timeout.getNano();
        if (seconds > Long.MAX_VALUE / nanosPerSecond) {
            return Long.MAX_VALUE;
        }
        long base = seconds * nanosPerSecond;
        if (Long.MAX_VALUE - base < nanos) {
            return Long.MAX_VALUE;
        }
        return base + nanos;
    }

}
