package cn.zhangyis.db.storage.trx.lock;

import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.storage.record.page.SearchKey;
import cn.zhangyis.db.storage.record.type.ColumnValue;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 最小 LockManager 内核验收：只通过事务锁公开 API 验证 record/gap/next-key/insert intention 的
 * 授予、等待、超时清理、死锁检测和快照语义。测试不接 B+Tree，后续 current-read 片只负责构造这些锁 key。
 */
class LockManagerCoreTest {

    private static final Duration TEST_TIMEOUT = Duration.ofMillis(800);

    @Test
    void recordSharedLocksCoexistButExclusiveWaitsUntilConflictingOwnerReleases() {
        LockManager manager = new LockManager(4, 16);
        RecordLockKey record = recordKey(10);
        TransactionId t1 = TransactionId.of(1);
        TransactionId t2 = TransactionId.of(2);
        TransactionId t3 = TransactionId.of(3);

        manager.acquire(t1, record, TransactionLockMode.REC_S, TEST_TIMEOUT);
        manager.acquire(t2, record, TransactionLockMode.REC_S, TEST_TIMEOUT);

        CompletableFuture<LockHandle> waiter = CompletableFuture.supplyAsync(
                () -> manager.acquire(t3, record, TransactionLockMode.REC_X, TEST_TIMEOUT));
        awaitUntil(() -> hasWaitEdge(manager, t3, t1) && hasWaitEdge(manager, t3, t2));

        assertEquals(1, manager.releaseAll(t1), "releaseAll 只释放 T1 已授予的 record S 锁");
        assertFalse(waiter.isDone(), "T2 仍持有共享锁时，T3 的 X 锁不能提前授予");

        assertEquals(1, manager.releaseAll(t2), "释放最后一个冲突者后应唤醒队首等待者");
        LockHandle xHandle = join(waiter);
        assertEquals(t3, xHandle.owner());
        assertEquals(TransactionLockMode.REC_X, xHandle.mode());
        assertTrue(manager.snapshot().grantedLocks().stream()
                .anyMatch(lock -> lock.owner().equals(t3) && lock.mode() == TransactionLockMode.REC_X));
        assertEquals(1, manager.releaseAll(t3));
    }

    @Test
    void gapLocksAllowRecordReadButBlockInsertIntentionUntilReleased() {
        assertGapLockAllowsRecordReadButBlocksInsert(TransactionLockMode.GAP_S, 110);
        assertGapLockAllowsRecordReadButBlocksInsert(TransactionLockMode.GAP_X, 120);
    }

    private static void assertGapLockAllowsRecordReadButBlocksInsert(TransactionLockMode gapMode, long baseTxnId) {
        LockManager manager = new LockManager(4, 16);
        GapLockKey gap = gapKey(1, 10, 20);
        RecordLockKey recordInsideGap = recordKey(15);
        TransactionId t1 = TransactionId.of(baseTxnId + 1);
        TransactionId t2 = TransactionId.of(baseTxnId + 2);
        TransactionId t3 = TransactionId.of(baseTxnId + 3);

        manager.acquire(t1, gap, gapMode, TEST_TIMEOUT);
        manager.acquire(t2, recordInsideGap, TransactionLockMode.REC_S, TEST_TIMEOUT);

        CompletableFuture<LockHandle> insert = CompletableFuture.supplyAsync(
                () -> manager.acquire(t3, new InsertIntentionLockKey(gap),
                        TransactionLockMode.INSERT_INTENTION, TEST_TIMEOUT));
        awaitUntil(() -> hasWaitEdge(manager, t3, t1));

        assertEquals(1, manager.releaseAll(t1));
        assertEquals(TransactionLockMode.INSERT_INTENTION, join(insert).mode());
        assertEquals(1, manager.releaseAll(t2));
        assertEquals(1, manager.releaseAll(t3));
    }

    @Test
    void insertIntentionsOnSameGapAreCompatibleWithEachOther() {
        LockManager manager = new LockManager(4, 16);
        GapLockKey gap = gapKey(1, 20, 30);
        TransactionId t1 = TransactionId.of(21);
        TransactionId t2 = TransactionId.of(22);

        manager.acquire(t1, new InsertIntentionLockKey(gap), TransactionLockMode.INSERT_INTENTION, TEST_TIMEOUT);
        manager.acquire(t2, new InsertIntentionLockKey(gap), TransactionLockMode.INSERT_INTENTION, TEST_TIMEOUT);

        assertEquals(2, manager.snapshot().grantedLocks().stream()
                .filter(lock -> lock.mode() == TransactionLockMode.INSERT_INTENTION)
                .count());
        assertEquals(1, manager.releaseAll(t1));
        assertEquals(1, manager.releaseAll(t2));
    }

    @Test
    void nextKeyExclusiveBlocksBothCoveredRecordAndPrecedingGapInsert() {
        LockManager manager = new LockManager(4, 16);
        RecordLockKey record = recordKey(40);
        GapLockKey gap = gapKey(1, 30, 40);
        NextKeyLockKey nextKey = new NextKeyLockKey(record, gap);
        TransactionId t1 = TransactionId.of(31);
        TransactionId t2 = TransactionId.of(32);
        TransactionId t3 = TransactionId.of(33);

        manager.acquire(t1, nextKey, TransactionLockMode.NEXT_KEY_X, TEST_TIMEOUT);
        CompletableFuture<LockHandle> recordWaiter = CompletableFuture.supplyAsync(
                () -> manager.acquire(t2, record, TransactionLockMode.REC_S, TEST_TIMEOUT));
        CompletableFuture<LockHandle> insertWaiter = CompletableFuture.supplyAsync(
                () -> manager.acquire(t3, new InsertIntentionLockKey(gap),
                        TransactionLockMode.INSERT_INTENTION, TEST_TIMEOUT));
        awaitUntil(() -> hasWaitEdge(manager, t2, t1) && hasWaitEdge(manager, t3, t1));

        assertEquals(1, manager.releaseAll(t1));
        assertEquals(TransactionLockMode.REC_S, join(recordWaiter).mode());
        assertEquals(TransactionLockMode.INSERT_INTENTION, join(insertWaiter).mode());
        assertEquals(1, manager.releaseAll(t2));
        assertEquals(1, manager.releaseAll(t3));
    }

    /** logical secondary prefix 共享读可共存；DML/UPDATE 的 X 必须等待所有共享 reader 终态释放。 */
    @Test
    void secondaryLogicalPrefixSupportsSharedAndExclusiveCompatibility() {
        LockManager manager = new LockManager(4, 16);
        SecondaryLogicalKeyLockKey prefix = new SecondaryLogicalKeyLockKey(9, "normalized-team");
        TransactionId firstReader = TransactionId.of(34);
        TransactionId secondReader = TransactionId.of(35);
        TransactionId writer = TransactionId.of(36);

        manager.acquire(firstReader, prefix, TransactionLockMode.REC_S, TEST_TIMEOUT);
        manager.acquire(secondReader, prefix, TransactionLockMode.REC_S, TEST_TIMEOUT);
        CompletableFuture<LockHandle> waitingWriter = CompletableFuture.supplyAsync(
                () -> manager.acquire(writer, prefix, TransactionLockMode.REC_X, TEST_TIMEOUT));
        awaitUntil(() -> hasWaitEdge(manager, writer, firstReader)
                && hasWaitEdge(manager, writer, secondReader));

        assertEquals(1, manager.releaseAll(firstReader));
        assertFalse(waitingWriter.isDone());
        assertEquals(1, manager.releaseAll(secondReader));
        assertEquals(TransactionLockMode.REC_X, join(waitingWriter).mode());
        assertEquals(1, manager.releaseAll(writer));
    }

    @Test
    void lockWaitTimeoutRemovesWaitingRequestAndWaitForEdge() {
        LockManager manager = new LockManager(4, 16);
        RecordLockKey record = recordKey(50);
        TransactionId t1 = TransactionId.of(41);
        TransactionId t2 = TransactionId.of(42);

        manager.acquire(t1, record, TransactionLockMode.REC_X, TEST_TIMEOUT);

        assertThrows(LockWaitTimeoutException.class,
                () -> manager.acquire(t2, record, TransactionLockMode.REC_X, Duration.ofMillis(30)));
        LockSnapshot snapshot = manager.snapshot();
        assertTrue(snapshot.waitingLocks().isEmpty(), "timeout 后 wait queue 不能残留请求");
        assertTrue(snapshot.waitEdges().isEmpty(), "timeout 后 wait-for graph 不能残留边");
        assertEquals(1, snapshot.grantedLocks().size(), "holder 的已授予锁不受等待超时影响");
        assertEquals(1, manager.releaseAll(t1));
    }

    @Test
    void twoTransactionDeadlockThrowsForCurrentWaiterAndKeepsOtherWaiterObservable() {
        LockManager manager = new LockManager(4, 16);
        RecordLockKey a = recordKey(60);
        RecordLockKey b = recordKey(61);
        TransactionId t1 = TransactionId.of(51);
        TransactionId t2 = TransactionId.of(52);

        manager.acquire(t1, a, TransactionLockMode.REC_X, TEST_TIMEOUT);
        manager.acquire(t2, b, TransactionLockMode.REC_X, TEST_TIMEOUT);
        CompletableFuture<LockHandle> t1WaitsB = CompletableFuture.supplyAsync(
                () -> manager.acquire(t1, b, TransactionLockMode.REC_X, TEST_TIMEOUT));
        awaitUntil(() -> hasWaitEdge(manager, t1, t2));

        assertThrows(DeadlockDetectedException.class,
                () -> manager.acquire(t2, a, TransactionLockMode.REC_X, TEST_TIMEOUT));
        assertFalse(hasWaitEdge(manager, t2, t1), "victim 请求应从 wait-for graph 中移除");

        assertEquals(1, manager.releaseAll(t2));
        assertEquals(TransactionLockMode.REC_X, join(t1WaitsB).mode());
        assertEquals(2, manager.releaseAll(t1));
    }

    @Test
    void threeTransactionDeadlockIsDetectedWithinBoundedSearch() {
        LockManager manager = new LockManager(4, 16);
        RecordLockKey a = recordKey(70);
        RecordLockKey b = recordKey(71);
        RecordLockKey c = recordKey(72);
        TransactionId t1 = TransactionId.of(61);
        TransactionId t2 = TransactionId.of(62);
        TransactionId t3 = TransactionId.of(63);

        manager.acquire(t1, a, TransactionLockMode.REC_X, TEST_TIMEOUT);
        LockHandle t2HoldsB = manager.acquire(t2, b, TransactionLockMode.REC_X, TEST_TIMEOUT);
        LockHandle t3HoldsC = manager.acquire(t3, c, TransactionLockMode.REC_X, TEST_TIMEOUT);

        CompletableFuture<LockHandle> t1WaitsB = CompletableFuture.supplyAsync(
                () -> manager.acquire(t1, b, TransactionLockMode.REC_X, TEST_TIMEOUT));
        CompletableFuture<LockHandle> t2WaitsC = CompletableFuture.supplyAsync(
                () -> manager.acquire(t2, c, TransactionLockMode.REC_X, TEST_TIMEOUT));
        awaitUntil(() -> hasWaitEdge(manager, t1, t2) && hasWaitEdge(manager, t2, t3));

        assertThrows(DeadlockDetectedException.class,
                () -> manager.acquire(t3, a, TransactionLockMode.REC_X, TEST_TIMEOUT));

        t2HoldsB.close();
        assertEquals(TransactionLockMode.REC_X, join(t1WaitsB).mode());
        t3HoldsC.close();
        assertEquals(TransactionLockMode.REC_X, join(t2WaitsC).mode());
        assertEquals(2, manager.releaseAll(t1));
        assertEquals(1, manager.releaseAll(t2));
    }

    private static boolean hasWaitEdge(LockManager manager, TransactionId waiter, TransactionId blocker) {
        return manager.snapshot().waitEdges().stream()
                .anyMatch(edge -> edge.waitingTransactionId().equals(waiter)
                        && edge.blockingTransactionId().equals(blocker));
    }

    private static void awaitUntil(BooleanSupplier condition) {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(500);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                TimeUnit.MILLISECONDS.sleep(5);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("interrupted while waiting for lock-manager condition", e);
            }
        }
        throw new AssertionError("lock-manager condition was not satisfied before timeout");
    }

    private static LockHandle join(CompletableFuture<LockHandle> future) {
        try {
            return future.get(1, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new AssertionError("lock request did not complete", e);
        }
    }

    private static RecordLockKey recordKey(long heapNo) {
        return new RecordLockKey(1L, PageId.of(SpaceId.of(1), PageNo.of(3)), Math.toIntExact(heapNo));
    }

    private static GapLockKey gapKey(long indexId, long left, long right) {
        return new GapLockKey(indexId, intKey(left), intKey(right));
    }

    private static SearchKey intKey(long value) {
        return new SearchKey(List.of(new ColumnValue.IntValue(value)));
    }
}
