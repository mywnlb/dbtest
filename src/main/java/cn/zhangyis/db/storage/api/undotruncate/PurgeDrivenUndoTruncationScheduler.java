package cn.zhangyis.db.storage.api.undotruncate;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.trx.PurgeCycleMaintenance;
import cn.zhangyis.db.storage.trx.PurgeSummary;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

/**
 * 在成功 purge cycle 后驱动当前单 undo space 的自动截断策略。
 *
 * <p>本类不创建线程；生产调用始终来自单 {@code PurgeDriverWorker} owner。原子时间门和不可变 metrics 快照仍使
 * 诊断读取及误用的并发调用保持数据竞争安全。cooldown 只限制候选检查，不改变 page3/marker 持久真相。</p>
 */
@Slf4j
public final class PurgeDrivenUndoTruncationScheduler implements PurgeCycleMaintenance {

    /** 自动调度的不可变启用、extent 门槛与冷却配置。 */
    private final UndoTruncationConfig config;
    /** 当前实现唯一系统 undo space；多空间选择不在本切片猜测。 */
    private final SpaceId spaceId;
    /** 生产指向共享 truncate service，测试可替换为无 IO fake。 */
    private final UndoTruncationAttemptTarget target;
    /** 单调时钟；不得使用墙钟决定冷却。 */
    private final LongSupplier nanoTime;
    /** 饱和换算后的冷却纳秒，避免极大 Duration 溢出。 */
    private final long checkIntervalNanos;
    /** 最近一次赢得检查权的单调时刻；null 明确表示从未检查，避免占用任一合法 nanoTime 值作哨兵。 */
    private final AtomicReference<Long> lastCheckNanos = new AtomicReference<>();
    /** 诊断读者一次取得自洽不可变快照，不读取散落计数器。 */
    private final AtomicReference<UndoTruncationMetricsSnapshot> metrics;

    /**
     * 创建生产 scheduler，attempt target 固定为组合根共享的 crash-safe truncate service。
     *
     * @param config 自动调度完整策略；不得为 {@code null}
     * @param spaceId 当前系统 undo space；不得为 {@code null}
     * @param service recovery/live 共用的 truncate service；不得为 {@code null}
     * @throws DatabaseValidationException 任一依赖为空时抛出，不发布半初始化 scheduler
     */
    public PurgeDrivenUndoTruncationScheduler(UndoTruncationConfig config, SpaceId spaceId,
                                               UndoTablespaceTruncationService service) {
        this(config, spaceId, requireService(service), System::nanoTime);
    }

    /** 包内确定性构造器：测试注入 fake target 与单调时钟，不引入 sleep 竞争。 */
    PurgeDrivenUndoTruncationScheduler(UndoTruncationConfig config, SpaceId spaceId,
                                       UndoTruncationAttemptTarget target, LongSupplier nanoTime) {
        if (config == null || spaceId == null || target == null || nanoTime == null) {
            throw new DatabaseValidationException("undo truncation scheduler dependencies must not be null");
        }
        this.config = config;
        this.spaceId = spaceId;
        this.target = target;
        this.nanoTime = nanoTime;
        this.checkIntervalNanos = saturatedNanos(config.checkInterval());
        this.metrics = new AtomicReference<>(UndoTruncationMetricsSnapshot.initial(config.enabled()));
    }

    /**
     * 成功 purge batch 后按 cooldown 尝试维护；summary 可以是零进展，使已空闲空间最终仍能被回收。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>验证 purge summary 并读取启用状态；禁用时不触碰时钟、lease 或物理服务。</li>
     *     <li>使用单调时钟 CAS 取得本轮检查权；冷却未到或另一调用已取得时直接返回。</li>
     *     <li>调用零等待 attempt target，把阈值跳过、四类 deferred 或完成映射到原子 metrics 快照。</li>
     *     <li>真实存储异常先记录 FAILED 和诊断，再原样传播给 driver；未知运行时异常包装并保留 cause。</li>
     * </ol>
     *
     * @param summary 已成功完成并释放 purge 资源的批次统计；不得为 {@code null}
     * @throws DatabaseValidationException summary 为空时抛出
     * @throws DatabaseRuntimeException truncate 格式、owner、WAL、flush 或 IO 无法安全继续时抛出
     */
    @Override
    public void afterSuccessfulBatch(PurgeSummary summary) {
        // 1、禁用是稳定配置结果；不把它计作一次检查或跳过。
        if (summary == null) {
            throw new DatabaseValidationException("purge maintenance summary must not be null");
        }
        if (!config.enabled()) {
            return;
        }
        // 2、CAS 同时承担冷却和误并发去重；时钟回退时允许新检查，避免永久卡死。
        long now = nanoTime.getAsLong();
        if (!claimCheck(now)) {
            return;
        }
        try {
            // 3、attempt 自己在 X lease 下读取持久 initial/page3；scheduler 不复制候选真相。
            UndoTruncationAttemptResult result = target.tryTruncate(spaceId, config.minReclaimableExtents());
            publishResult(result);
            if (result.status() == UndoTruncationAttemptStatus.COMPLETED) {
                var completion = result.completion().orElseThrow();
                log.info("automatic undo truncation completed: space={}, epoch={}, reclaimedPages={}",
                        spaceId.value(), completion.truncateEpoch(), result.observedReclaimablePages());
            }
        } catch (DatabaseRuntimeException failure) {
            // 4、先发布可观察失败再传播；driver 会进入 FAILED，marker 后状态留给 recovery。
            publishFailure(failure);
            log.error("automatic undo truncation failed: space={}", spaceId.value(), failure);
            throw failure;
        } catch (RuntimeException failure) {
            DatabaseRuntimeException wrapped = new DatabaseRuntimeException(
                    "unexpected automatic undo truncation failure: space=" + spaceId.value(), failure);
            publishFailure(wrapped);
            log.error("automatic undo truncation failed unexpectedly: space={}", spaceId.value(), failure);
            throw wrapped;
        }
    }

    /**
     * 返回当前原子 metrics 快照；查询不要求 engine OPEN，也不会清除最后失败。
     *
     * @return 自洽的累计计数、最近状态与 durable epoch
     */
    public UndoTruncationMetricsSnapshot metricsSnapshot() {
        return metrics.get();
    }

    /**
     * 在创建方法引用前执行领域校验，避免空 service 泄漏裸 {@link NullPointerException}。
     *
     * @param service recovery/live 共用的 truncate service；不得为 {@code null}
     * @return 绑定该 service 的包内 attempt 端口
     * @throws DatabaseValidationException service 为空时抛出
     */
    private static UndoTruncationAttemptTarget requireService(UndoTablespaceTruncationService service) {
        if (service == null) {
            throw new DatabaseValidationException("undo truncation service must not be null");
        }
        return service::tryTruncate;
    }

    /**
     * 用 elapsed 语义处理 nanoTime 回绕；成功 CAS 是本轮检查的线性化点。
     *
     * @param now 当前单调时钟读数；允许跨 {@code long} 符号位
     * @return 当前调用取得检查 ownership 时为 {@code true}，仍在 cooldown 或 CAS 竞争失败后发现未到期时为 false
     */
    private boolean claimCheck(long now) {
        while (true) {
            Long previous = lastCheckNanos.get();
            if (previous != null) {
                long elapsed = now - previous;
                if (elapsed >= 0 && elapsed < checkIntervalNanos) {
                    return false;
                }
            }
            if (lastCheckNanos.compareAndSet(previous, now)) {
                return true;
            }
        }
    }

    /**
     * 单次原子发布成功/跳过/deferred 统计，避免读者看到拆分字段的中间组合。
     *
     * @param result attempt target 返回的完整分类与可选持久完成证据；不得为 {@code null}
     * @throws DatabaseValidationException result 为空或其完成证据不满足状态契约时抛出
     */
    private void publishResult(UndoTruncationAttemptResult result) {
        if (result == null) {
            throw new DatabaseValidationException("undo truncation attempt result must not be null");
        }
        metrics.updateAndGet(previous -> {
            boolean skipped = result.status() == UndoTruncationAttemptStatus.BELOW_THRESHOLD;
            boolean deferred = result.status().deferred();
            boolean completed = result.status() == UndoTruncationAttemptStatus.COMPLETED;
            long epoch = completed ? result.completion().orElseThrow().truncateEpoch()
                    : previous.lastCompletedEpoch();
            long reclaimed = completed
                    ? saturatedAdd(previous.reclaimedPages(), result.observedReclaimablePages())
                    : previous.reclaimedPages();
            return new UndoTruncationMetricsSnapshot(true, saturatedAdd(previous.checks(), 1),
                    saturatedAdd(previous.skipped(), skipped ? 1 : 0),
                    saturatedAdd(previous.deferred(), deferred ? 1 : 0),
                    saturatedAdd(previous.completed(), completed ? 1 : 0), previous.failures(), reclaimed,
                    cycleStatus(result.status()), epoch, previous.lastFailure());
        });
    }

    /**
     * 失败也计入一次已到期检查；最后失败保留类型和消息供引擎关闭后诊断。
     *
     * @param failure 将继续传播给 purge driver 的真实存储错误；不得为 {@code null}
     */
    private void publishFailure(DatabaseRuntimeException failure) {
        String diagnostic = failure.getClass().getSimpleName() + ": " + String.valueOf(failure.getMessage());
        metrics.updateAndGet(previous -> new UndoTruncationMetricsSnapshot(true,
                saturatedAdd(previous.checks(), 1), previous.skipped(), previous.deferred(),
                previous.completed(), saturatedAdd(previous.failures(), 1), previous.reclaimedPages(),
                UndoTruncationCycleStatus.FAILED, previous.lastCompletedEpoch(), diagnostic));
    }

    /**
     * 把 attempt 领域状态映射为 scheduler 观测状态。
     *
     * @param status target 返回的非空状态
     * @return 与 status 一一对应的 metrics 最近状态
     */
    private static UndoTruncationCycleStatus cycleStatus(UndoTruncationAttemptStatus status) {
        return switch (status) {
            case BELOW_THRESHOLD -> UndoTruncationCycleStatus.BELOW_THRESHOLD;
            case DEFERRED_ACCESS_BUSY -> UndoTruncationCycleStatus.DEFERRED_ACCESS_BUSY;
            case DEFERRED_HISTORY -> UndoTruncationCycleStatus.DEFERRED_HISTORY;
            case DEFERRED_ACTIVE_SLOTS -> UndoTruncationCycleStatus.DEFERRED_ACTIVE_SLOTS;
            case DEFERRED_REUSE_BUSY -> UndoTruncationCycleStatus.DEFERRED_REUSE_BUSY;
            case COMPLETED -> UndoTruncationCycleStatus.COMPLETED;
        };
    }

    /**
     * 把正 Duration 转为饱和纳秒，防止单位换算溢出把冷却变成频繁执行。
     *
     * @param duration 已由配置校验为正的冷却时间
     * @return 可用于单调差值比较的正纳秒数，溢出时为 {@link Long#MAX_VALUE}
     */
    private static long saturatedNanos(Duration duration) {
        try {
            return duration.toNanos();
        } catch (ArithmeticException overflow) {
            return Long.MAX_VALUE;
        }
    }

    /**
     * 对 metrics 非负计数执行饱和加法，达到 long 上限后不回绕。
     *
     * @param left 当前非负累计值
     * @param right 本轮非负增量
     * @return 两者之和，溢出时为 {@link Long#MAX_VALUE}
     */
    private static long saturatedAdd(long left, long right) {
        if (right > 0 && left > Long.MAX_VALUE - right) {
            return Long.MAX_VALUE;
        }
        return left + right;
    }
}
