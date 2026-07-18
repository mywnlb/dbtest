package cn.zhangyis.db.storage.flush;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.DirtyPageCandidate;
import cn.zhangyis.db.storage.flush.checkpoint.CheckpointCoordinator;
import cn.zhangyis.db.storage.flush.policy.AdaptiveFlushPolicy;
import cn.zhangyis.db.storage.flush.policy.FlushAdvice;
import cn.zhangyis.db.storage.flush.policy.FlushBatchPlan;
import cn.zhangyis.db.storage.flush.policy.FlushRateMeter;
import cn.zhangyis.db.storage.flush.policy.FlushRuntimeSnapshot;
import cn.zhangyis.db.storage.flush.policy.FlushTuning;
import cn.zhangyis.db.storage.redo.RedoCapacityDecision;
import cn.zhangyis.db.storage.redo.RedoCapacityPolicy;
import cn.zhangyis.db.storage.redo.RedoCapacityPressure;
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
    /**
     * 构造时冻结的 {@code tuning} 配置快照；已完成范围和组合校验，运行期策略读取它但不得就地修改。
     */
    private final FlushTuning tuning;
    /**
     * 本对象持有的 {@code rateMeter} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final FlushRateMeter rateMeter = new FlushRateMeter();
    /**
     * 记录 {@code successfulFlushedPages} 的权威数值状态；仅由本类受控路径更新，取值范围和特殊值遵循所属格式或状态机，溢出必须拒绝。
     */
    private int successfulFlushedPages;

    /**
     * 创建 {@code FlushService}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param bufferPool 由组合根提供的 {@code BufferPool} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param flushCoordinator 由组合根提供的 {@code FlushCoordinator} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param checkpointCoordinator 由组合根提供的 {@code CheckpointCoordinator} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param redo 由组合根提供的 {@code RedoLogManager} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param capacityPolicy 调用方提供的长度或容量值对象；不得为 {@code null}，且必须已通过其构造范围校验
     * @param adaptiveFlushPolicy 由组合根注入的下游协作者；不得为 {@code null}，生命周期至少覆盖本对象
     */
    public FlushService(BufferPool bufferPool,
                        FlushCoordinator flushCoordinator,
                        CheckpointCoordinator checkpointCoordinator,
                        RedoLogManager redo,
                        RedoCapacityPolicy capacityPolicy,
                        AdaptiveFlushPolicy adaptiveFlushPolicy) {
        this(bufferPool, flushCoordinator, checkpointCoordinator, redo, capacityPolicy, adaptiveFlushPolicy,
                FlushTuning.defaults(16 * 1024, Math.max(1, bufferPool.capacity())));
    }

    /**
     * 创建 {@code FlushService}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取必需协作者、身份与配置边界，在字段赋值或资源打开前拒绝 null、越界和相互矛盾的组合。</li>
     *     <li>完成跨参数校验并推导不可变配置；若构造过程创建自有资源，后续失败必须在异常路径关闭。</li>
     *     <li>把已校验协作者与配置绑定到字段，并初始化本对象拥有的状态、显式锁、队列或缓存，不允许 this 提前逃逸。</li>
     *     <li>构造完成后对象处于类契约声明的初始状态；任一步失败都抛出领域异常且不发布半初始化实例。</li>
     * </ol>
     *
     * @param bufferPool 由组合根提供的 {@code BufferPool} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param flushCoordinator 由组合根提供的 {@code FlushCoordinator} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param checkpointCoordinator 由组合根提供的 {@code CheckpointCoordinator} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param redo 由组合根提供的 {@code RedoLogManager} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param capacityPolicy 调用方提供的长度或容量值对象；不得为 {@code null}，且必须已通过其构造范围校验
     * @param adaptiveFlushPolicy 由组合根注入的下游协作者；不得为 {@code null}，生命周期至少覆盖本对象
     * @param tuning 恢复、checkpoint、doublewrite 或刷脏阶段的协作状态；不得为 {@code null}，阶段和持久化边界必须与当前实例的恢复状态机一致
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public FlushService(BufferPool bufferPool,
                        FlushCoordinator flushCoordinator,
                        CheckpointCoordinator checkpointCoordinator,
                        RedoLogManager redo,
                        RedoCapacityPolicy capacityPolicy,
                        AdaptiveFlushPolicy adaptiveFlushPolicy,
                        FlushTuning tuning) {
        // 1、校验必需协作者、身份与配置边界，在字段赋值或资源打开前拒绝非法组合。
        if (bufferPool == null || flushCoordinator == null || checkpointCoordinator == null
                || redo == null || capacityPolicy == null || adaptiveFlushPolicy == null || tuning == null) {
            throw new DatabaseValidationException("flush service dependencies must not be null");
        }
        this.bufferPool = bufferPool;
        // 2、完成跨参数校验并推导不可变配置；后续失败仍由当前构造路径收口已创建资源。
        this.flushCoordinator = flushCoordinator;
        this.checkpointCoordinator = checkpointCoordinator;
        // 3、绑定已校验协作者并初始化本对象拥有的状态、显式锁、队列或缓存，不允许半初始化实例逃逸。
        this.redo = redo;
        this.capacityPolicy = capacityPolicy;
        this.adaptiveFlushPolicy = adaptiveFlushPolicy;
        // 4、完成初始状态发布；失败以领域异常终止构造，成功对象满足类级生命周期不变量。
        this.tuning = tuning;
    }

    /**
     * 根据当前 redo capacity pressure 执行一轮 flush list 刷脏。没有压力时不刷页；只有 dirty view 为空时才把
     * “无 dirty 且 redo 已 durable”的安全边界写入 redo control，避免后台 tick 把仍未落盘的 dirty 页边界提前发布。
     * 有压力时由策略决定 target LSN 和页数，刷完后再尝试推进 checkpoint。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取脏页、page LSN、代际与 checkpoint 压力快照，先排除未固定或已失效的候选。</li>
     *     <li>在不持页闩执行慢等待的前提下推进 redo durable 边界，确保数据页写盘前满足 WAL。</li>
     *     <li>按既定 doublewrite、表空间写入和 force 顺序持久化快照，部分失败只确认实际成功的页面。</li>
     *     <li>重新校验代际与 dirty version 后发布完成状态；并发再修改页继续保持 dirty，异常不推进不安全边界。</li>
     * </ol>
     *
     * @param maxPages 调用方允许本轮最多刷出的页数。
     * @return 本轮调度结果。
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public FlushCycleResult flushForCapacity(int maxPages) {
        // 1、读取脏页、page LSN、代际与 checkpoint 压力快照，在共享或持久副作用前拒绝非法状态。
        if (maxPages < 0) {
            throw new DatabaseValidationException("flush max pages must not be negative: " + maxPages);
        }
        Lsn before = checkpointCoordinator.lastCheckpointLsn();
        RedoCapacityDecision decision = capacityPolicy.evaluate(redo.currentLsn(), before);
        // 比例策略按「需刷出以推进 checkpoint 到 target 的脏页数」自适应批量；NONE 不刷，省去计数。
        int dirtyPagesBeforeTarget = decision.pressure() == RedoCapacityPressure.NONE
                ? 0
                : bufferPool.dirtyPageCandidates(decision.targetCheckpointLsn(), bufferPool.capacity()).size();
        // 2、继续完成范围、身份与候选校验；通过后，在不持页闩执行慢等待的前提下推进 redo durable 边界，保持处理顺序与资源边界。
        FlushRuntimeSnapshot runtime = rateMeter.sample(redo.currentLsn(), successfulFlushedPages,
                dirtyPagesBeforeTarget, bufferPool.capacity(), bufferPool.freeFrameCount());
        FlushBatchPlan batchPlan = adaptiveFlushPolicy.planBatches(decision, dirtyPagesBeforeTarget,
                maxPages, runtime, tuning);
        FlushAdvice advice = new FlushAdvice(batchPlan.targetLsn(), batchPlan.totalPages(),
                batchPlan.synchronousPressure());
        List<FlushResult> results = new ArrayList<>();
        // 3、在中间分支复核阶段性结果；满足条件后，按既定 doublewrite、表空间写入和 force 顺序持久化快照，并维持领域不变量。
        if (batchPlan.lruPages() > 0) {
            results.addAll(flushCoordinator.flushBatch(new FlushBatchRequest(
                    FlushBatchSource.LRU, batchPlan.targetLsn(), batchPlan.lruPages())));
        }
        if (batchPlan.flushListPages() > 0) {
            results.addAll(flushCoordinator.flushBatch(new FlushBatchRequest(
                    FlushBatchSource.FLUSH_LIST, batchPlan.targetLsn(), batchPlan.flushListPages())));
        }
        successfulFlushedPages += (int) results.stream().filter(result ->
                result.status() == FlushResultStatus.CLEAN || result.status() == FlushResultStatus.KEPT_DIRTY).count();
        Lsn after = advice.shouldFlush() || hasNoDirtyPages()
                ? checkpointCoordinator.advanceCheckpoint()
                : before;
        // 4、重新校验代际与 dirty version 后发布完成状态，以稳定返回或领域异常完成收口。
        return new FlushCycleResult(decision, advice, results, before, after);
    }

    /** 无压力后台 tick 的 checkpoint 保护：只有 dirty view 为空时才允许空刷推进恢复起点。 */
    private boolean hasNoDirtyPages() {
        return bufferPool.dirtyPageCandidates(redo.currentLsn(), 1).isEmpty();
    }

    /**
     * Drain 指定 tablespace 的 dirty page。调用方应在进入本方法前阻止该 space 新写入；本方法只负责刷出现存 dirty，
     * 不获取 tablespace lifecycle X latch，也不参与 DDL 状态机。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取脏页、page LSN、代际与 checkpoint 压力快照，先排除未固定或已失效的候选。</li>
     *     <li>在不持页闩执行慢等待的前提下推进 redo durable 边界，确保数据页写盘前满足 WAL。</li>
     *     <li>按既定 doublewrite、表空间写入和 force 顺序持久化快照，部分失败只确认实际成功的页面。</li>
     *     <li>重新校验代际与 dirty version 后发布完成状态；并发再修改页继续保持 dirty，异常不推进不安全边界。</li>
     * </ol>
     *
     * @param spaceId 目标 tablespace。
     * @param timeout 最大 drain 时间，0 表示只做一次超时检查并返回。
     * @return drain 结果。
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public TablespaceDrainResult drainTablespace(SpaceId spaceId, Duration timeout) {
        // 1、读取脏页、page LSN、代际与 checkpoint 压力快照，在共享或持久副作用前拒绝非法状态。
        if (spaceId == null || timeout == null) {
            throw new DatabaseValidationException("drain space/timeout must not be null");
        }
        // 2、继续完成范围、身份与候选校验；通过后，在不持页闩执行慢等待的前提下推进 redo durable 边界，保持处理顺序与资源边界。
        if (timeout.isNegative()) {
            throw new DatabaseValidationException("drain timeout must not be negative: " + timeout);
        }
        // 3、在中间分支复核阶段性结果；满足条件后，按既定 doublewrite、表空间写入和 force 顺序持久化快照，并维持领域不变量。
        long deadline = deadlineFromNow(timeout);
        List<FlushResult> results = new ArrayList<>();
        // 4、重新校验代际与 dirty version 后发布完成状态，以稳定返回或领域异常完成收口。
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
                if (!awaitDirtyStateChange(deadline)) {
                    if (!dirtyPagesInSpace(spaceId).isEmpty()) {
                        return new TablespaceDrainResult(spaceId, results, true,
                                checkpointCoordinator.lastCheckpointLsn());
                    }
                }
            }
        }
    }

    /**
     * 建立 lifecycle marker 的全局持久化屏障：同步 fsync redo，反复刷出所有 oldest LSN 不大于 target 的脏页，
     * 并推进 checkpoint，直到 checkpoint 覆盖 target。不能只 drain 目标表空间，因为任一其它空间的更老脏页都会
     * 限制 fuzzy checkpoint；超时则保留上层 TRUNCATING marker，禁止继续物理缩短。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取脏页、page LSN、代际与 checkpoint 压力快照，先排除未固定或已失效的候选。</li>
     *     <li>在不持页闩执行慢等待的前提下推进 redo durable 边界，确保数据页写盘前满足 WAL。</li>
     *     <li>按既定 doublewrite、表空间写入和 force 顺序持久化快照，部分失败只确认实际成功的页面。</li>
     *     <li>重新校验代际与 dirty version 后发布完成状态；并发再修改页继续保持 dirty，异常不推进不安全边界。</li>
     * </ol>
     *
     * @param targetLsn 必须已由 marker MTR 分配的目标 LSN。
     * @param timeout 最大等待时间。
     * @return 已覆盖 target 的 checkpoint LSN。
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws FlushBarrierTimeoutException 操作在约定时限内无法完成时抛出；调用方可回滚或稍后重试
     */
    public Lsn flushThrough(Lsn targetLsn, Duration timeout) {
        // 1、读取脏页、page LSN、代际与 checkpoint 压力快照，在共享或持久副作用前拒绝非法状态。
        if (targetLsn == null || timeout == null) {
            throw new DatabaseValidationException("flush-through target/timeout must not be null");
        }
        // 2、继续完成范围、身份与候选校验；通过后，在不持页闩执行慢等待的前提下推进 redo durable 边界，保持处理顺序与资源边界。
        if (timeout.isNegative()) {
            throw new DatabaseValidationException("flush-through timeout must not be negative: " + timeout);
        }
        // 3、在中间分支复核阶段性结果；满足条件后，按既定 doublewrite、表空间写入和 force 顺序持久化快照，并维持领域不变量。
        long deadline = deadlineFromNow(timeout);
        redo.flush();
        // 4、重新校验代际与 dirty version 后发布完成状态，以稳定返回或领域异常完成收口。
        while (true) {
            List<DirtyPageCandidate> candidates = bufferPool.dirtyPageCandidates(targetLsn, bufferPool.capacity());
            for (DirtyPageCandidate candidate : candidates) {
                flushCoordinator.singlePageFlush(candidate.pageId());
            }
            Lsn checkpoint = checkpointCoordinator.advanceCheckpoint();
            if (checkpoint.value() >= targetLsn.value()) {
                return checkpoint;
            }
            if (deadlineReached(deadline)) {
                throw new FlushBarrierTimeoutException("timed out flushing/checkpointing through LSN "
                        + targetLsn.value() + "; durable=" + redo.flushedToDiskLsn().value()
                        + ", checkpoint=" + checkpoint.value());
            }
            parkBriefly(deadline);
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

    private boolean awaitDirtyStateChange(long deadline) {
        long remaining = remainingNanos(deadline);
        if (remaining <= 0) {
            return false;
        }
        return bufferPool.awaitDirtyStateChange(Duration.ofNanos(remaining));
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

    private static long remainingNanos(long deadline) {
        if (deadline == Long.MAX_VALUE) {
            return Long.MAX_VALUE;
        }
        return deadline - System.nanoTime();
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
