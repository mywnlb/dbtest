package cn.zhangyis.db.storage.api.undotruncate;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.fil.state.TablespaceState;
import cn.zhangyis.db.storage.trx.PurgeSummary;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** purge cycle 后台 truncate 调度测试：单调冷却、结果计数与失败传播均不依赖真实 IO。 */
class PurgeDrivenUndoTruncationSchedulerTest {

    private static final SpaceId SPACE = SpaceId.of(5);
    private static final PurgeSummary EMPTY_PURGE = new PurgeSummary(0, 0, 0, 0, 0);

    /** 首个成功 purge cycle 即可检查；冷却内即使 purge 无进展也不重复竞争 lifecycle lease。 */
    @Test
    void runsFirstCheckAndThenHonorsMonotonicCooldown() {
        AtomicLong clock = new AtomicLong(100L);
        AtomicInteger calls = new AtomicInteger();
        UndoTruncationAttemptTarget target = (spaceId, minExtents) -> {
            calls.incrementAndGet();
            return UndoTruncationAttemptResult.incomplete(
                    UndoTruncationAttemptStatus.BELOW_THRESHOLD, 0L);
        };
        PurgeDrivenUndoTruncationScheduler scheduler = new PurgeDrivenUndoTruncationScheduler(
                new UndoTruncationConfig(true, 1, Duration.ofSeconds(30)), SPACE, target, clock::get);

        scheduler.afterSuccessfulBatch(EMPTY_PURGE);
        scheduler.afterSuccessfulBatch(EMPTY_PURGE);
        clock.addAndGet(Duration.ofSeconds(29).toNanos());
        scheduler.afterSuccessfulBatch(EMPTY_PURGE);
        assertEquals(1, calls.get());

        clock.addAndGet(Duration.ofSeconds(1).toNanos());
        scheduler.afterSuccessfulBatch(EMPTY_PURGE);
        assertEquals(2, calls.get());
        assertEquals(2L, scheduler.metricsSnapshot().checks());
        assertEquals(2L, scheduler.metricsSnapshot().skipped());
    }

    /** {@link System#nanoTime()} 的任意 long 值都合法；最小值不能与“从未检查”哨兵混淆而绕过 cooldown。 */
    @Test
    void longMinNanoTimeStillEstablishesCooldown() {
        AtomicLong clock = new AtomicLong(Long.MIN_VALUE);
        AtomicInteger calls = new AtomicInteger();
        PurgeDrivenUndoTruncationScheduler scheduler = new PurgeDrivenUndoTruncationScheduler(
                new UndoTruncationConfig(true, 1, Duration.ofSeconds(1)), SPACE,
                (spaceId, minExtents) -> {
                    calls.incrementAndGet();
                    return UndoTruncationAttemptResult.incomplete(
                            UndoTruncationAttemptStatus.BELOW_THRESHOLD, 0L);
                }, clock::get);

        scheduler.afterSuccessfulBatch(EMPTY_PURGE);
        clock.incrementAndGet();
        scheduler.afterSuccessfulBatch(EMPTY_PURGE);

        assertEquals(1, calls.get());
    }

    /** 禁用策略不能调用物理服务，metrics 必须明确显示 DISABLED 而不是假装从未配置。 */
    @Test
    void disabledSchedulerNeverCallsTarget() {
        AtomicInteger calls = new AtomicInteger();
        PurgeDrivenUndoTruncationScheduler scheduler = new PurgeDrivenUndoTruncationScheduler(
                new UndoTruncationConfig(false, 1, Duration.ofSeconds(30)), SPACE,
                (spaceId, minExtents) -> {
                    calls.incrementAndGet();
                    return UndoTruncationAttemptResult.incomplete(
                            UndoTruncationAttemptStatus.BELOW_THRESHOLD, 0L);
                }, () -> 0L);

        scheduler.afterSuccessfulBatch(EMPTY_PURGE);

        assertEquals(0, calls.get());
        assertEquals(UndoTruncationCycleStatus.DISABLED, scheduler.metricsSnapshot().lastStatus());
    }

    /** deferred 与完成结果分别累计，完成发布回收页数和 durable epoch。 */
    @Test
    void recordsDeferredAndCompletedAttempts() {
        AtomicLong clock = new AtomicLong();
        AtomicInteger call = new AtomicInteger();
        UndoTruncationAttemptTarget target = (spaceId, minExtents) -> switch (call.getAndIncrement()) {
            case 0 -> UndoTruncationAttemptResult.incomplete(
                    UndoTruncationAttemptStatus.DEFERRED_HISTORY, 64L);
            default -> UndoTruncationAttemptResult.completed(128L,
                    new UndoTablespaceTruncationResult(SPACE, 3L, PageNo.of(64),
                            Lsn.of(900L), TablespaceState.ACTIVE));
        };
        PurgeDrivenUndoTruncationScheduler scheduler = new PurgeDrivenUndoTruncationScheduler(
                new UndoTruncationConfig(true, 1, Duration.ofSeconds(1)), SPACE, target, clock::get);

        scheduler.afterSuccessfulBatch(EMPTY_PURGE);
        clock.addAndGet(Duration.ofSeconds(1).toNanos());
        scheduler.afterSuccessfulBatch(EMPTY_PURGE);

        UndoTruncationMetricsSnapshot metrics = scheduler.metricsSnapshot();
        assertEquals(1L, metrics.deferred());
        assertEquals(1L, metrics.completed());
        assertEquals(128L, metrics.reclaimedPages());
        assertEquals(3L, metrics.lastCompletedEpoch());
        assertEquals(UndoTruncationCycleStatus.COMPLETED, metrics.lastStatus());
    }

    /** 存储异常必须先进入可观测 FAILED 快照再原样传播，不能被降级为 deferred。 */
    @Test
    void recordsFailureBeforeRethrowingToDriver() {
        DatabaseRuntimeException failure = new DatabaseRuntimeException("induced truncate failure");
        PurgeDrivenUndoTruncationScheduler scheduler = new PurgeDrivenUndoTruncationScheduler(
                UndoTruncationConfig.defaults(), SPACE,
                (spaceId, minExtents) -> {
                    throw failure;
                }, () -> 0L);

        assertEquals(failure, assertThrows(DatabaseRuntimeException.class,
                () -> scheduler.afterSuccessfulBatch(EMPTY_PURGE)));
        UndoTruncationMetricsSnapshot metrics = scheduler.metricsSnapshot();
        assertEquals(1L, metrics.failures());
        assertEquals(UndoTruncationCycleStatus.FAILED, metrics.lastStatus());
        assertTrue(metrics.lastFailure().contains("induced truncate failure"));
    }

    /** 生产构造器必须在创建 method reference 前把空 service 转为项目领域异常。 */
    @Test
    void rejectsNullProductionServiceWithDomainException() {
        assertThrows(DatabaseValidationException.class,
                () -> new PurgeDrivenUndoTruncationScheduler(
                        UndoTruncationConfig.defaults(), SPACE, null));
    }
}
