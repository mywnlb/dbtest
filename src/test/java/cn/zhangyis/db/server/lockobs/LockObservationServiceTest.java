package cn.zhangyis.db.server.lockobs;

import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.server.lockobs.api.DefaultLockObservationService;
import cn.zhangyis.db.server.lockobs.api.SnapshotRequest;
import cn.zhangyis.db.server.lockobs.report.DeadlockReport;
import cn.zhangyis.db.server.lockobs.snapshot.DataLockRow;
import cn.zhangyis.db.server.lockobs.snapshot.LockDiagnosticSnapshot;
import cn.zhangyis.db.storage.record.page.SearchKey;
import cn.zhangyis.db.storage.record.type.ColumnValue;
import cn.zhangyis.db.storage.trx.lock.DeadlockDetectedException;
import cn.zhangyis.db.storage.trx.lock.LockHandle;
import cn.zhangyis.db.storage.trx.lock.LockManager;
import cn.zhangyis.db.storage.trx.lock.LockWaitTimeoutException;
import cn.zhangyis.db.storage.trx.lock.RecordLockKey;
import cn.zhangyis.db.storage.trx.lock.TransactionLockMode;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * server.lockobs 第一片验收：只通过公开观测 API 消费 LockManager 当前事实，验证 data_locks、
 * data_lock_waits、wait slot 和最近 deadlock report。测试不查询 LockManager 内部队列，确保观测层只是只读适配。
 */
class LockObservationServiceTest {

    private static final Duration TEST_TIMEOUT = Duration.ofMillis(800);

    @Test
    void dataLocksAndWaitsExposeGrantedAndWaitingRecordRowsWithThreadEvents() {
        DefaultLockObservationService observation = new DefaultLockObservationService();
        LockManager manager = new LockManager(4, 16, observation);
        RecordLockKey record = recordKey(1);
        TransactionId blocker = TransactionId.of(1001);
        TransactionId waiter = TransactionId.of(1002);

        manager.acquire(blocker, record, TransactionLockMode.REC_X, TEST_TIMEOUT);
        CompletableFuture<LockHandle> waitingAcquire = CompletableFuture.supplyAsync(
                () -> manager.acquire(waiter, record, TransactionLockMode.REC_X, TEST_TIMEOUT));
        awaitUntil(() -> hasWaitingRow(observation, manager, waiter));

        LockDiagnosticSnapshot snapshot = observation.captureSnapshot(manager.snapshot(), SnapshotRequest.defaults());
        DataLockRow blockerRow = rowFor(snapshot, blocker);
        DataLockRow waiterRow = rowFor(snapshot, waiter);
        assertEquals("INNODB", blockerRow.engine());
        assertEquals("RECORD", blockerRow.lockType());
        assertEquals("X,REC_NOT_GAP", blockerRow.lockMode());
        assertEquals("GRANTED", blockerRow.lockStatus());
        assertEquals("WAITING", waiterRow.lockStatus());
        assertTrue(waiterRow.threadId() > 0, "waiting row must expose the waiting Java thread id");
        assertTrue(waiterRow.eventId() > 0, "waiting row must expose a stable wait event id");
        assertTrue(waiterRow.lockData().contains("heap=1"));
        assertEquals(1, snapshot.waitSlots().size(), "current wait slot should be visible while the thread waits");
        assertEquals(1, snapshot.dataLockWaits().size());
        assertEquals(waiterRow.engineLockId(), snapshot.dataLockWaits().getFirst().requestingEngineLockId());
        assertEquals(blockerRow.engineLockId(), snapshot.dataLockWaits().getFirst().blockingEngineLockId());

        assertEquals(1, manager.releaseAll(blocker));
        LockHandle grantedToWaiter = join(waitingAcquire);
        assertEquals(waiter, grantedToWaiter.owner());
        assertTrue(observation.captureSnapshot(manager.snapshot(), SnapshotRequest.defaults()).waitSlots().isEmpty(),
                "grant after wait must complete and clear the current wait slot");
        assertEquals(1, manager.releaseAll(waiter));
    }

    @Test
    void dataLockWaitsExposeAllBlockingTransactionsForOneWaitingRequest() {
        DefaultLockObservationService observation = new DefaultLockObservationService();
        LockManager manager = new LockManager(4, 16, observation);
        RecordLockKey record = recordKey(2);
        TransactionId firstBlocker = TransactionId.of(2001);
        TransactionId secondBlocker = TransactionId.of(2002);
        TransactionId waiter = TransactionId.of(2003);

        manager.acquire(firstBlocker, record, TransactionLockMode.REC_S, TEST_TIMEOUT);
        manager.acquire(secondBlocker, record, TransactionLockMode.REC_S, TEST_TIMEOUT);
        CompletableFuture<LockHandle> waitingAcquire = CompletableFuture.supplyAsync(
                () -> manager.acquire(waiter, record, TransactionLockMode.REC_X, TEST_TIMEOUT));
        awaitUntil(() -> observation.captureSnapshot(manager.snapshot(), SnapshotRequest.defaults())
                .dataLockWaits().size() == 2);

        LockDiagnosticSnapshot snapshot = observation.captureSnapshot(manager.snapshot(), SnapshotRequest.defaults());
        Set<TransactionId> blockers = snapshot.dataLockWaits().stream()
                .map(row -> row.blockingEngineTransactionId())
                .collect(Collectors.toSet());
        assertEquals(Set.of(firstBlocker, secondBlocker), blockers);

        assertEquals(1, manager.releaseAll(firstBlocker));
        assertFalse(waitingAcquire.isDone(), "one blocker still holds S, so waiter must remain waiting");
        assertEquals(1, manager.releaseAll(secondBlocker));
        join(waitingAcquire);
        assertEquals(1, manager.releaseAll(waiter));
    }

    @Test
    void deadlockVictimIsRecordedAsLatestDeadlockReport() {
        DefaultLockObservationService observation = new DefaultLockObservationService();
        LockManager manager = new LockManager(4, 16, observation);
        RecordLockKey left = recordKey(3);
        RecordLockKey right = recordKey(4);
        TransactionId first = TransactionId.of(3001);
        TransactionId second = TransactionId.of(3002);

        manager.acquire(first, left, TransactionLockMode.REC_X, TEST_TIMEOUT);
        manager.acquire(second, right, TransactionLockMode.REC_X, TEST_TIMEOUT);
        CompletableFuture<LockHandle> firstWaits = CompletableFuture.supplyAsync(
                () -> manager.acquire(first, right, TransactionLockMode.REC_X, TEST_TIMEOUT));
        awaitUntil(() -> hasWaitingRow(observation, manager, first));

        assertThrows(DeadlockDetectedException.class,
                () -> manager.acquire(second, left, TransactionLockMode.REC_X, TEST_TIMEOUT));

        List<DeadlockReport> reports = observation.latestDeadlocks();
        assertEquals(1, reports.size());
        DeadlockReport report = reports.getFirst();
        assertEquals(second, report.victimTransactionId());
        assertTrue(report.edges().stream().anyMatch(edge ->
                edge.waitingTransactionId().equals(first) && edge.blockingTransactionId().equals(second)));
        assertTrue(report.edges().stream().anyMatch(edge ->
                edge.waitingTransactionId().equals(second) && edge.blockingTransactionId().equals(first)));

        assertEquals(1, manager.releaseAll(second));
        join(firstWaits);
        assertEquals(2, manager.releaseAll(first));
    }

    @Test
    void timeoutCleansWaitSlotAndWaitingRows() {
        DefaultLockObservationService observation = new DefaultLockObservationService();
        LockManager manager = new LockManager(4, 16, observation);
        RecordLockKey record = recordKey(5);
        TransactionId blocker = TransactionId.of(4001);
        TransactionId waiter = TransactionId.of(4002);

        manager.acquire(blocker, record, TransactionLockMode.REC_X, TEST_TIMEOUT);

        assertThrows(LockWaitTimeoutException.class,
                () -> manager.acquire(waiter, record, TransactionLockMode.REC_X, Duration.ofMillis(20)));

        LockDiagnosticSnapshot snapshot = observation.captureSnapshot(manager.snapshot(), SnapshotRequest.defaults());
        assertTrue(snapshot.waitSlots().isEmpty(), "timeout must complete the wait slot");
        assertTrue(snapshot.dataLocks().stream().noneMatch(row -> row.engineTransactionId().equals(waiter)));
        assertTrue(snapshot.dataLockWaits().isEmpty());
        assertEquals(1, manager.releaseAll(blocker));
    }

    private static boolean hasWaitingRow(DefaultLockObservationService observation,
                                         LockManager manager,
                                         TransactionId waiter) {
        return observation.captureSnapshot(manager.snapshot(), SnapshotRequest.defaults())
                .dataLocks().stream()
                .anyMatch(row -> row.engineTransactionId().equals(waiter)
                        && row.lockStatus().equals("WAITING"));
    }

    private static DataLockRow rowFor(LockDiagnosticSnapshot snapshot, TransactionId owner) {
        return snapshot.dataLocks().stream()
                .filter(row -> row.engineTransactionId().equals(owner))
                .findFirst()
                .orElseThrow();
    }

    private static void awaitUntil(BooleanSupplier condition) {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(700);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                TimeUnit.MILLISECONDS.sleep(5);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("interrupted while waiting for lock observation condition", e);
            }
        }
        throw new AssertionError("lock observation condition was not satisfied before timeout");
    }

    private static LockHandle join(CompletableFuture<LockHandle> future) {
        try {
            LockHandle handle = future.get(2, TimeUnit.SECONDS);
            assertNotNull(handle);
            return handle;
        } catch (Exception e) {
            throw new AssertionError("lock acquire future did not complete", e);
        }
    }

    private static RecordLockKey recordKey(long heapNo) {
        return new RecordLockKey(1L, PageId.of(SpaceId.of(1), PageNo.of(3)), Math.toIntExact(heapNo));
    }

    @SuppressWarnings("unused")
    private static SearchKey intKey(long value) {
        return new SearchKey(List.of(new ColumnValue.IntValue(value)));
    }
}
