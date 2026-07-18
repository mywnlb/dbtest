package cn.zhangyis.db.storage.flush.policy;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.redo.RedoCapacityDecision;
import cn.zhangyis.db.storage.redo.RedoCapacityPressure;

/**
 * Adaptive flush 策略。把 redo capacity pressure（来自 checkpoint age）映射为本轮 flush list 刷页数与 target LSN。
 *
 * <p>两种模式：
 * <ul>
 *   <li>{@link #fixed}（离散）：pressure → 固定档位（ASYNC=min、SYNC=(min+max)/2、HARD=max）。确定性，供定向测试/故障隔离。</li>
 *   <li>{@link #adaptive}（§7.4 比例版，production 默认）：按实际脏页 backlog 比例刷
 *       {@code targetPages = clamp(basePages + pressureFactor·dirtyPagesBeforeTarget, min, max)}，
 *       backlog 多时刷得多、少时刷得少。</li>
 * </ul>
 *
 * <p>简化点（§7.4「第一阶段」）：proportional 只用 {@code dirtyPagesBeforeTarget} + per-pressure factor；尚未引入
 * redo 生成率、实际 flush 率、IO capacity/idle percent、neighbor flush 等输入。{@code NONE} 一律不刷（容量驱动下无压力）。
 */
public final class AdaptiveFlushPolicy {

    /** 刷页数计算模式。 */
    private enum Mode {
        /** 离散档位（与 backlog 无关）。 */
        DISCRETE,
        /** 按 backlog 比例（§7.4）。 */
        PROPORTIONAL
    }

    /** ASYNC 压力下刷出 backlog 的比例。 */
    private static final double ASYNC_FACTOR = 0.25;
    /** SYNC 压力下刷出 backlog 的比例。 */
    private static final double SYNC_FACTOR = 0.5;
    /** HARD 压力下刷出 backlog 的比例（整批 backlog，上限 maxBatch）。 */
    private static final double HARD_FACTOR = 1.0;

    /**
     * 本对象的权威状态机字段 {@code mode}；只有合法转换方法可以更新，更新受显式锁、原子发布或单一 owner 线程保护，下游据此决定可执行阶段。
     */
    private final Mode mode;
    /** proportional 的基础刷页数（与 backlog 无关的最小推进量）；discrete 模式忽略。 */
    private final int basePages;
    /** 有压力时单轮最小刷页数（下限）。 */
    private final int minBatchPages;
    /** 单轮最大刷页数（上限）。 */
    private final int maxBatchPages;

    /**
     * 创建 {@code AdaptiveFlushPolicy}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param mode 调用方请求的目标状态、阶段或模式；不得为 {@code null}，且必须是当前状态机允许的后继值
     * @param basePages 参与 {@code 构造} 的上界或规格值 {@code basePages}；必须非负且不能使容量、页数或编码长度计算溢出
     * @param minBatchPages 参与 {@code 构造} 的上界或规格值 {@code minBatchPages}；必须非负且不能使容量、页数或编码长度计算溢出
     * @param maxBatchPages 参与 {@code 构造} 的上界或规格值 {@code maxBatchPages}；必须非负且不能使容量、页数或编码长度计算溢出
     */
    private AdaptiveFlushPolicy(Mode mode, int basePages, int minBatchPages, int maxBatchPages) {
        this.mode = mode;
        this.basePages = basePages;
        this.minBatchPages = minBatchPages;
        this.maxBatchPages = maxBatchPages;
    }

    /**
     * 创建离散档位策略：ASYNC→min、SYNC→(min+max)/2、HARD→max，与脏页 backlog 无关。
     *
     * @param minBatchPages ASYNC 档与下限。
     * @param maxBatchPages HARD 档与上限。
     * @return 离散策略。
     */
    public static AdaptiveFlushPolicy fixed(int minBatchPages, int maxBatchPages) {
        validateBatch(minBatchPages, maxBatchPages);
        return new AdaptiveFlushPolicy(Mode.DISCRETE, minBatchPages, minBatchPages, maxBatchPages);
    }

    /**
     * 创建 §7.4 比例策略（production 默认）：{@code clamp(basePages + factor·dirtyPagesBeforeTarget, min, max)}。
     *
     * @param basePages     与 backlog 无关的基础刷页数（≥0）。
     * @param minBatchPages 有压力时单轮下限（≥1）。
     * @param maxBatchPages 单轮上限（≥min）。
     * @return 比例策略。
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public static AdaptiveFlushPolicy adaptive(int basePages, int minBatchPages, int maxBatchPages) {
        if (basePages < 0) {
            throw new DatabaseValidationException("adaptive flush base pages must be >= 0: " + basePages);
        }
        validateBatch(minBatchPages, maxBatchPages);
        return new AdaptiveFlushPolicy(Mode.PROPORTIONAL, basePages, minBatchPages, maxBatchPages);
    }

    /**
     * 根据 redo capacity decision 与当前脏页 backlog 生成本轮刷脏建议。
     *
     * @param decision               redo capacity pressure 判断。
     * @param dirtyPagesBeforeTarget  oldestModificationLsn ≤ target 的脏页数（需要刷出以推进 checkpoint 到 target）；
     *                                discrete 模式忽略该值。
     * @param requestMaxPages         调用方允许本轮最多刷出的页数（外层上限）。
     * @return flush advice。
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public FlushAdvice plan(RedoCapacityDecision decision, int dirtyPagesBeforeTarget, int requestMaxPages) {
        if (decision == null) {
            throw new DatabaseValidationException("redo capacity decision must not be null");
        }
        if (dirtyPagesBeforeTarget < 0) {
            throw new DatabaseValidationException("dirty pages before target must not be negative: "
                    + dirtyPagesBeforeTarget);
        }
        if (requestMaxPages < 0) {
            throw new DatabaseValidationException("requested max pages must not be negative: " + requestMaxPages);
        }
        RedoCapacityPressure pressure = decision.pressure();
        int planned = pressure == RedoCapacityPressure.NONE
                ? 0
                : (mode == Mode.DISCRETE ? discrete(pressure) : proportional(pressure, dirtyPagesBeforeTarget));
        int pages = Math.min(planned, requestMaxPages);
        boolean sync = pressure == RedoCapacityPressure.SYNC_FLUSH || pressure == RedoCapacityPressure.HARD_LIMIT;
        return new FlushAdvice(decision.targetCheckpointLsn(), pages, sync);
    }

    /** 根据运行时 redo/flush 速率生成 FlushList/LRU 批量分配。
     *
     * @param decision redo 收集、定位或重放所需的日志对象；不得为 {@code null}，其 LSN 范围和记录格式必须连续且属于当前恢复或 MTR 上下文
     * @param dirtyPagesBeforeTarget 参与 {@code planBatches} 的上界或规格值 {@code dirtyPagesBeforeTarget}；必须非负且不能使容量、页数或编码长度计算溢出
     * @param requestMaxPages 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     * @param runtime 恢复、checkpoint、doublewrite 或刷脏阶段的协作状态；不得为 {@code null}，阶段和持久化边界必须与当前实例的恢复状态机一致
     * @param tuning 恢复、checkpoint、doublewrite 或刷脏阶段的协作状态；不得为 {@code null}，阶段和持久化边界必须与当前实例的恢复状态机一致
     * @return {@code planBatches} 产生的恢复或持久化阶段对象；成功时不为 {@code null}，其中的 durable 边界不超过已安全完成的工作
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public FlushBatchPlan planBatches(RedoCapacityDecision decision, int dirtyPagesBeforeTarget,
                                      int requestMaxPages, FlushRuntimeSnapshot runtime,
                                      FlushTuning tuning) {
        if (runtime == null || tuning == null) {
            throw new DatabaseValidationException("flush runtime/tuning must not be null");
        }
        FlushAdvice base = plan(decision, dirtyPagesBeforeTarget, requestMaxPages);
        if (!base.shouldFlush()) {
            return new FlushBatchPlan(base.targetLsn(), 0, 0, base.synchronousPressure());
        }
        long redoPages = tuning.pageSizeBytes() == 0 ? 0
                : (runtime.redoBytesGenerated() + tuning.pageSizeBytes() - 1L) / tuning.pageSizeBytes();
        long deficit = Math.max(0L, redoPages - runtime.flushedPages());
        long raw = (long) base.maxPages() + deficit;
        long ioBudget = runtime.sampleSeconds() <= 0
                ? tuning.maxBatchPages()
                : (long) Math.ceil(tuning.ioCapacityPagesPerSecond() * runtime.sampleSeconds()
                        * (decision.pressure() == RedoCapacityPressure.ASYNC_FLUSH
                        ? tuning.idleFlushPercent() / 100.0 : 1.0));
        int total = (int) Math.min(requestMaxPages, Math.min(tuning.maxBatchPages(),
                Math.max(tuning.minBatchPages(), Math.max(1L, Math.min(raw, Math.max(1L, ioBudget))))));
        int lru = 0;
        if (runtime.capacityFrames() > 0
                && runtime.freeFrames() * 100L <= runtime.capacityFrames() * tuning.lruFreeFrameLowWatermarkPercent()) {
            lru = Math.min(total, runtime.dirtyPages());
        }
        return new FlushBatchPlan(base.targetLsn(), total - lru, lru, base.synchronousPressure());
    }

    private int discrete(RedoCapacityPressure pressure) {
        return switch (pressure) {
            case NONE -> 0;
            case ASYNC_FLUSH -> minBatchPages;
            case SYNC_FLUSH -> (minBatchPages + maxBatchPages) / 2;
            case HARD_LIMIT -> maxBatchPages;
        };
    }

    private int proportional(RedoCapacityPressure pressure, int dirtyPagesBeforeTarget) {
        // §7.4：targetPages = clamp(basePages + factor·dirtyPagesBeforeTarget, min, max)。factor 随压力升高，
        // 使 backlog 越大、压力越高时本轮刷得越多；用 long 中间值避免 base+backlog 溢出 int。
        double factor = switch (pressure) {
            case NONE -> 0.0;
            case ASYNC_FLUSH -> ASYNC_FACTOR;
            case SYNC_FLUSH -> SYNC_FACTOR;
            case HARD_LIMIT -> HARD_FACTOR;
        };
        long raw = basePages + Math.round(factor * dirtyPagesBeforeTarget);
        return (int) Math.max(minBatchPages, Math.min(maxBatchPages, raw));
    }

    private static void validateBatch(int minBatchPages, int maxBatchPages) {
        if (minBatchPages < 1) {
            throw new DatabaseValidationException("adaptive flush min batch must be >= 1: " + minBatchPages);
        }
        if (maxBatchPages < minBatchPages) {
            throw new DatabaseValidationException("adaptive flush max batch must be >= min batch: "
                    + maxBatchPages + " < " + minBatchPages);
        }
    }
}
