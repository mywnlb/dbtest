package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.TransactionNo;
import cn.zhangyis.db.domain.UndoSlotId;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** table token 调度器：异表并发、同表 FIFO，依赖等待不能占用 purge worker。 */
class PurgeWorkerPoolTest {

    /** 两个不相交表必须同时进入 worker，而不是退化成 dispatcher 串行。 */
    @Test
    void unrelatedTablesExecuteConcurrently() throws Exception {
        PurgeWorkerPool pool = PurgeWorkerPool.parallel(new PurgeConfig(2, 4, Duration.ofSeconds(2)));
        CountDownLatch entered = new CountDownLatch(2);
        CountDownLatch release = new CountDownLatch(1);
        HistoryEntry first = entry(1, 11);
        HistoryEntry second = entry(2, 22);
        try (var caller = Executors.newSingleThreadExecutor()) {
            var execution = caller.submit(() -> pool.execute(List.of(
                    new PurgeLogWork(first, () -> awaitAndReady(first, entered, release)),
                    new PurgeLogWork(second, () -> awaitAndReady(second, entered, release)))));

            assertTrue(entered.await(1, TimeUnit.SECONDS), "independent table tasks should occupy both workers");
            release.countDown();
            assertEquals(2, execution.get(2, TimeUnit.SECONDS).size());
        } finally {
            pool.requestStop();
            assertTrue(pool.awaitStopped(Duration.ofSeconds(2)));
        }
    }

    /** 同表第二条日志只有在第一条 READY 后才可提交，依赖本身不得占住第二个 worker。 */
    @Test
    void sameTablePreservesFifoWithoutBlockingUnrelatedLane() throws Exception {
        PurgeWorkerPool pool = PurgeWorkerPool.parallel(new PurgeConfig(2, 4, Duration.ofSeconds(2)));
        CountDownLatch firstEntered = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        CountDownLatch unrelatedEntered = new CountDownLatch(1);
        AtomicBoolean secondEntered = new AtomicBoolean();
        HistoryEntry first = entry(1, 11);
        HistoryEntry second = entry(2, 11);
        HistoryEntry unrelated = entry(3, 22);
        try (var caller = Executors.newSingleThreadExecutor()) {
            var execution = caller.submit(() -> pool.execute(List.of(
                    new PurgeLogWork(first, () -> awaitAndReady(first, firstEntered, releaseFirst)),
                    new PurgeLogWork(second, () -> {
                        secondEntered.set(true);
                        return PurgeLogTaskResult.ready(second, 0, 0, 0);
                    }),
                    new PurgeLogWork(unrelated, () -> {
                        unrelatedEntered.countDown();
                        return PurgeLogTaskResult.ready(unrelated, 0, 0, 0);
                    }))));

            assertTrue(firstEntered.await(1, TimeUnit.SECONDS));
            assertTrue(unrelatedEntered.await(1, TimeUnit.SECONDS),
                    "an unrelated lane must not wait behind the first table");
            assertFalse(secondEntered.get(), "same-table successor must wait for READY predecessor");
            releaseFirst.countDown();
            assertEquals(3, execution.get(2, TimeUnit.SECONDS).size());
            assertTrue(secondEntered.get());
        } finally {
            pool.requestStop();
            assertTrue(pool.awaitStopped(Duration.ofSeconds(2)));
        }
    }

    /** 一条跨表日志必须同时占有所有 table lane，任一关联表后继都不能越过它。 */
    @Test
    void multiTableLogFencesEveryAffectedTableLane() throws Exception {
        PurgeWorkerPool pool = PurgeWorkerPool.parallel(new PurgeConfig(2, 4, Duration.ofSeconds(2)));
        CountDownLatch predecessorEntered = new CountDownLatch(1);
        CountDownLatch releasePredecessor = new CountDownLatch(1);
        CountDownLatch unrelatedEntered = new CountDownLatch(1);
        AtomicBoolean tableTwentyTwoSuccessorRan = new AtomicBoolean();
        HistoryEntry predecessor = entry(1, 11, 22);
        HistoryEntry tableTwentyTwoSuccessor = entry(2, 22);
        HistoryEntry unrelated = entry(3, 33);
        try (var caller = Executors.newSingleThreadExecutor()) {
            var execution = caller.submit(() -> pool.execute(List.of(
                    new PurgeLogWork(predecessor,
                            () -> awaitAndReady(predecessor, predecessorEntered, releasePredecessor)),
                    new PurgeLogWork(tableTwentyTwoSuccessor, () -> {
                        tableTwentyTwoSuccessorRan.set(true);
                        return PurgeLogTaskResult.ready(tableTwentyTwoSuccessor, 0, 0, 0);
                    }),
                    new PurgeLogWork(unrelated, () -> {
                        unrelatedEntered.countDown();
                        return PurgeLogTaskResult.ready(unrelated, 0, 0, 0);
                    }))));

            assertTrue(predecessorEntered.await(1, TimeUnit.SECONDS));
            assertTrue(unrelatedEntered.await(1, TimeUnit.SECONDS));
            assertFalse(tableTwentyTwoSuccessorRan.get());
            releasePredecessor.countDown();
            assertEquals(3, execution.get(2, TimeUnit.SECONDS).size());
            assertTrue(tableTwentyTwoSuccessorRan.get());
        } finally {
            releasePredecessor.countDown();
            pool.requestStop();
            assertTrue(pool.awaitStopped(Duration.ofSeconds(2)));
        }
    }

    /** DEFERRED 前驱阻止同表后继，但不会阻止已经接纳的其它表。 */
    @Test
    void deferredPredecessorBlocksOnlyItsTableLane() {
        PurgeWorkerPool pool = PurgeWorkerPool.parallel(new PurgeConfig(2, 4, Duration.ofSeconds(2)));
        AtomicBoolean sameTableRan = new AtomicBoolean();
        AtomicBoolean unrelatedRan = new AtomicBoolean();
        HistoryEntry deferred = entry(1, 11);
        HistoryEntry sameTable = entry(2, 11);
        HistoryEntry unrelated = entry(3, 22);
        try {
            List<PurgeLogTaskResult> results = pool.execute(List.of(
                    new PurgeLogWork(deferred, () -> PurgeLogTaskResult.deferred(deferred, 0, 0, 0)),
                    new PurgeLogWork(sameTable, () -> {
                        sameTableRan.set(true);
                        return PurgeLogTaskResult.ready(sameTable, 0, 0, 0);
                    }),
                    new PurgeLogWork(unrelated, () -> {
                        unrelatedRan.set(true);
                        return PurgeLogTaskResult.ready(unrelated, 0, 0, 0);
                    })));

            assertEquals(PurgeLogTaskStatus.DEFERRED, results.get(0).status());
            assertEquals(PurgeLogTaskStatus.BLOCKED, results.get(1).status());
            assertEquals(PurgeLogTaskStatus.READY, results.get(2).status());
            assertFalse(sameTableRan.get());
            assertTrue(unrelatedRan.get());
        } finally {
            pool.requestStop();
            assertTrue(pool.awaitStopped(Duration.ofSeconds(2)));
        }
    }

    /** 批次超时必须 fail-stop，但不得在线程位于记录内 MTR/B+Tree 修改时强制中断。 */
    @Test
    void timeoutFailStopsPoolWithoutInterruptingInRecordWork() throws Exception {
        PurgeWorkerPool pool = PurgeWorkerPool.parallel(new PurgeConfig(1, 2, Duration.ofMillis(50)));
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch releaseRecord = new CountDownLatch(1);
        AtomicBoolean interrupted = new AtomicBoolean();
        HistoryEntry blocked = entry(1, 11);
        try (var caller = Executors.newSingleThreadExecutor()) {
            var execution = caller.submit(() -> pool.execute(List.of(new PurgeLogWork(blocked, () -> {
                entered.countDown();
                try {
                    releaseRecord.await();
                } catch (InterruptedException unsafeCancellation) {
                    interrupted.set(true);
                    Thread.currentThread().interrupt();
                }
                return PurgeLogTaskResult.ready(blocked, 0, 0, 0);
            }))));

            assertTrue(entered.await(1, TimeUnit.SECONDS));
            ExecutionException failure = assertThrows(ExecutionException.class,
                    () -> execution.get(2, TimeUnit.SECONDS));
            assertInstanceOf(PurgeBatchTimeoutException.class, failure.getCause());
            assertEquals(PurgeWorkerPoolState.FAILED, pool.state());
            assertFalse(pool.awaitStopped(Duration.ofMillis(20)),
                    "record work remains owner until it reaches its own stable boundary");
            assertFalse(interrupted.get(), "timeout must not interrupt an in-record physical mutation");
            releaseRecord.countDown();
            assertTrue(pool.awaitStopped(Duration.ofSeconds(1)));
            assertThrows(PurgeWorkerStoppedException.class,
                    () -> pool.execute(List.of(new PurgeLogWork(blocked,
                            () -> PurgeLogTaskResult.ready(blocked, 0, 0, 0)))));
        } finally {
            releaseRecord.countDown();
            pool.requestStop();
            assertTrue(pool.awaitStopped(Duration.ofSeconds(1)));
        }
    }

    /** close 发布取消后 dispatcher 立即醒来，但 worker 只在当前记录稳定结束后退出。 */
    @Test
    void requestStopCancelsDispatcherWithoutInterruptingInRecordWork() throws Exception {
        PurgeWorkerPool pool = PurgeWorkerPool.parallel(new PurgeConfig(1, 2, Duration.ofSeconds(5)));
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch releaseRecord = new CountDownLatch(1);
        AtomicBoolean interrupted = new AtomicBoolean();
        HistoryEntry active = entry(1, 11);
        try (var caller = Executors.newSingleThreadExecutor()) {
            var execution = caller.submit(() -> pool.execute(List.of(new PurgeLogWork(active, () -> {
                entered.countDown();
                try {
                    releaseRecord.await();
                } catch (InterruptedException unsafeCancellation) {
                    interrupted.set(true);
                    Thread.currentThread().interrupt();
                }
                return PurgeLogTaskResult.ready(active, 0, 0, 0);
            }))));

            assertTrue(entered.await(1, TimeUnit.SECONDS));
            pool.requestStop();

            ExecutionException failure = assertThrows(ExecutionException.class,
                    () -> execution.get(1, TimeUnit.SECONDS));
            assertInstanceOf(PurgeWorkerStoppedException.class, failure.getCause());
            assertFalse(pool.awaitStopped(Duration.ofMillis(20)));
            assertFalse(interrupted.get());
            releaseRecord.countDown();
            assertTrue(pool.awaitStopped(Duration.ofSeconds(1)));
            assertEquals(PurgeWorkerPoolState.STOPPED, pool.state());
        } finally {
            releaseRecord.countDown();
            pool.requestStop();
            assertTrue(pool.awaitStopped(Duration.ofSeconds(1)));
        }
    }

    /** 极大正 timeout 作为“近似无限”预算使用，纳秒换算不得溢出为裸 ArithmeticException。 */
    @Test
    void saturatedBatchTimeoutStillExecutesNormally() {
        PurgeWorkerPool pool = PurgeWorkerPool.parallel(
                new PurgeConfig(1, 1, Duration.ofSeconds(Long.MAX_VALUE)));
        HistoryEntry entry = entry(1, 11);
        try {
            List<PurgeLogTaskResult> results = pool.execute(List.of(new PurgeLogWork(entry,
                    () -> PurgeLogTaskResult.ready(entry, 0, 0, 0))));

            assertEquals(PurgeLogTaskStatus.READY, results.getFirst().status());
        } finally {
            pool.requestStop();
            assertTrue(pool.awaitStopped(Duration.ofSeconds(1)));
        }
    }

    private static PurgeLogTaskResult awaitAndReady(HistoryEntry entry, CountDownLatch entered,
                                                     CountDownLatch release) {
        entered.countDown();
        try {
            if (!release.await(1, TimeUnit.SECONDS)) {
                throw new AssertionError("test task release timed out");
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError("test task interrupted", interrupted);
        }
        return PurgeLogTaskResult.ready(entry, 0, 0, 0);
    }

    private static HistoryEntry entry(long sequence, long... tableIds) {
        SpaceId spaceId = SpaceId.of(7);
        return new HistoryEntry(TransactionNo.of(sequence), TransactionId.of(100 + sequence), spaceId,
                PageId.of(spaceId, PageNo.of(10 + sequence)), UndoSlotId.of((int) sequence),
                Arrays.stream(tableIds).boxed().collect(java.util.stream.Collectors.toUnmodifiableSet()));
    }
}
