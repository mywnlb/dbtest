package cn.zhangyis.db.storage.btree;

import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.storage.api.DiskSpaceManager;
import cn.zhangyis.db.storage.api.SegmentRef;
import cn.zhangyis.db.storage.api.index.IndexPageAccess;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.fil.access.TablespaceAccessController;
import cn.zhangyis.db.storage.fil.exception.TablespaceNotOpenException;
import cn.zhangyis.db.storage.fil.io.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.flush.FlushCoordinator;
import cn.zhangyis.db.storage.flush.doublewrite.NoDoublewriteStrategy;
import cn.zhangyis.db.storage.fsp.segment.SegmentPurpose;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import cn.zhangyis.db.storage.record.format.LogicalRecord;
import cn.zhangyis.db.storage.record.format.RecordType;
import cn.zhangyis.db.storage.record.page.SearchKey;
import cn.zhangyis.db.storage.record.schema.ColumnDef;
import cn.zhangyis.db.storage.record.schema.ColumnId;
import cn.zhangyis.db.storage.record.schema.ColumnType;
import cn.zhangyis.db.storage.record.schema.IndexKeyDef;
import cn.zhangyis.db.storage.record.schema.KeyOrder;
import cn.zhangyis.db.storage.record.schema.KeyPartDef;
import cn.zhangyis.db.storage.record.schema.TableSchema;
import cn.zhangyis.db.storage.record.type.ColumnValue;
import cn.zhangyis.db.storage.record.type.TypeCodecRegistry;
import cn.zhangyis.db.storage.trx.IsolationLevel;
import cn.zhangyis.db.storage.trx.lock.DeadlockDetectedException;
import cn.zhangyis.db.storage.trx.lock.GapLockKey;
import cn.zhangyis.db.storage.trx.lock.InsertIntentionLockKey;
import cn.zhangyis.db.storage.trx.lock.LockHandle;
import cn.zhangyis.db.storage.trx.lock.LockManager;
import cn.zhangyis.db.storage.trx.lock.LockWaitTimeoutException;
import cn.zhangyis.db.storage.trx.lock.NextKeyLockKey;
import cn.zhangyis.db.storage.trx.lock.RecordLockKey;
import cn.zhangyis.db.storage.trx.lock.TransactionLockMode;
import cn.zhangyis.db.storage.redo.RedoLogFileRepository;
import cn.zhangyis.db.storage.redo.RedoLogManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * B+Tree current-read 第一片：验证 B+Tree 在短 MTR 内定位 record/gap，释放 page latch/fix 后等待事务锁，
 * 授锁后重新定位并校验，且 unique insert check 使用 record/gap/insert-intention 锁。
 */
class BTreeCurrentReadServiceTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(51);
    private static final long INDEX_ID = 19L;
    private static final Duration WAIT_TIMEOUT = Duration.ofMillis(900);

    @TempDir
    Path dir;

    private final TypeCodecRegistry registry = new TypeCodecRegistry();

    /**
     * 验证 {@code pointForUpdateWaitsWithoutHoldingPageLatchOrBufferFix} 所描述的并发场景，并断言等待、唤醒、超时与资源释放顺序。
     */
    @Test
    void pointForUpdateWaitsWithoutHoldingPageLatchOrBufferFix() {
        onPool(ctx -> {
            ctx.createTablespaceAndRoot();
            SplitCapableBTreeIndexService btree = ctx.service();
            BTreeCurrentReadService currentRead = ctx.currentReadService(btree);
            BTreeIndex index = ctx.clusteredIndex();
            BTreeInsertResult inserted = insertCommitted(ctx, btree, index, smallRow(1));
            TransactionId blocker = TransactionId.of(1001);
            TransactionId waiter = TransactionId.of(1002);
            ctx.lockManager.acquire(blocker, RecordLockKey.from(inserted.recordRef()),
                    TransactionLockMode.REC_X, WAIT_TIMEOUT);

            CompletableFuture<Optional<BTreeLookupResult>> waitingRead = CompletableFuture.supplyAsync(
                    () -> currentRead.lockPoint(index, kId(1), request(waiter, IsolationLevel.READ_COMMITTED),
                            BTreeCurrentReadMode.FOR_UPDATE));
            awaitUntil(() -> hasWaitEdge(ctx.lockManager, waiter, blocker));

            assertTrue(canTakeRootExclusiveLatch(ctx, index),
                    "current-read must release page latch and buffer fix before waiting on LockManager");

            assertEquals(1, ctx.lockManager.releaseAll(blocker));
            Optional<BTreeLookupResult> result = join(waitingRead);
            assertTrue(result.isPresent());
            assertEquals(1L, idOf(result.orElseThrow()));
            assertEquals(1, ctx.lockManager.releaseAll(waiter));
        });
    }

    /**
     * 验证 {@code pointReadRelocatesAfterRecordRefChangesDuringWait} 所描述的并发场景，并断言等待、唤醒、超时与资源释放顺序。
     */
    @Test
    void pointReadRelocatesAfterRecordRefChangesDuringWait() {
        onPool(ctx -> {
            ctx.createTablespaceAndRoot();
            SplitCapableBTreeIndexService btree = ctx.service();
            BTreeCurrentReadService currentRead = ctx.currentReadService(btree);
            BTreeIndex index = ctx.clusteredIndex();
            BTreeInsertResult first = insertCommitted(ctx, btree, index, wideRow(1));
            RecordLockKey staleKey = RecordLockKey.from(first.recordRef());
            TransactionId blocker = TransactionId.of(1101);
            TransactionId waiter = TransactionId.of(1102);
            ctx.lockManager.acquire(blocker, staleKey, TransactionLockMode.REC_X, WAIT_TIMEOUT);

            CompletableFuture<Optional<BTreeLookupResult>> waitingRead = CompletableFuture.supplyAsync(
                    () -> currentRead.lockPoint(index, kId(1), request(waiter, IsolationLevel.READ_COMMITTED),
                            BTreeCurrentReadMode.FOR_UPDATE));
            awaitUntil(() -> hasWaitEdge(ctx.lockManager, waiter, blocker));

            BTreeIndex after = index;
            for (long id = 2; id <= 4; id++) {
                BTreeInsertResult result = insertCommitted(ctx, btree, after, wideRow(id));
                after = result.indexAfterInsert();
            }
            assertEquals(1, after.rootLevel(), "wide rows should force root split and move row 1 to a new leaf");

            assertEquals(1, ctx.lockManager.releaseAll(blocker));
            BTreeLookupResult relocated = join(waitingRead).orElseThrow();
            RecordLockKey relocatedKey = RecordLockKey.from(relocated.recordRef());
            assertNotEquals(staleKey, relocatedKey, "current-read must drop the stale record lock and re-lock relocated row");
            assertTrue(ctx.lockManager.snapshot().grantedLocks().stream()
                    .anyMatch(lock -> lock.owner().equals(waiter) && lock.key().equals(relocatedKey)
                            && lock.mode() == TransactionLockMode.REC_X));
            assertFalse(ctx.lockManager.snapshot().grantedLocks().stream()
                    .anyMatch(lock -> lock.owner().equals(waiter) && lock.key().equals(staleKey)));
            assertEquals(1, ctx.lockManager.releaseAll(waiter));
        });
    }

    /**
     * 验证 {@code readCommittedPointMissDoesNotTakeGapLock} 所描述的并发场景，并断言等待、唤醒、超时与资源释放顺序。
     */
    @Test
    void readCommittedPointMissDoesNotTakeGapLock() {
        onPool(ctx -> {
            ctx.createTablespaceAndRoot();
            SplitCapableBTreeIndexService btree = ctx.service();
            BTreeCurrentReadService currentRead = ctx.currentReadService(btree);
            BTreeIndex index = ctx.clusteredIndex();
            insertCommitted(ctx, btree, index, smallRow(10));
            TransactionId owner = TransactionId.of(1201);

            Optional<BTreeLookupResult> result = currentRead.lockPoint(index, kId(5),
                    request(owner, IsolationLevel.READ_COMMITTED), BTreeCurrentReadMode.FOR_UPDATE);

            assertTrue(result.isEmpty());
            assertTrue(ctx.lockManager.snapshot().grantedLocks().stream()
                    .noneMatch(lock -> lock.owner().equals(owner)), "RC point miss should not take a gap lock");
        });
    }

    /**
     * 验证 {@code repeatableReadPointMissTakesGapLock} 所描述的并发场景，并断言等待、唤醒、超时与资源释放顺序。
     */
    @Test
    void repeatableReadPointMissTakesGapLock() {
        onPool(ctx -> {
            ctx.createTablespaceAndRoot();
            SplitCapableBTreeIndexService btree = ctx.service();
            BTreeCurrentReadService currentRead = ctx.currentReadService(btree);
            BTreeIndex index = ctx.clusteredIndex();
            insertCommitted(ctx, btree, index, smallRow(10));
            TransactionId owner = TransactionId.of(1301);

            Optional<BTreeLookupResult> result = currentRead.lockPoint(index, kId(5),
                    request(owner, IsolationLevel.REPEATABLE_READ), BTreeCurrentReadMode.FOR_UPDATE);

            assertTrue(result.isEmpty());
            assertTrue(ctx.lockManager.snapshot().grantedLocks().stream()
                    .anyMatch(lock -> lock.owner().equals(owner)
                            && lock.key() instanceof GapLockKey
                            && lock.mode() == TransactionLockMode.GAP_X));
            assertEquals(1, ctx.lockManager.releaseAll(owner));
        });
    }

    /**
     * 验证 {@code uniqueInsertCheckWaitsWithInsertIntentionOnTargetGap} 所描述的并发场景，并断言等待、唤醒、超时与资源释放顺序。
     */
    @Test
    void uniqueInsertCheckWaitsWithInsertIntentionOnTargetGap() {
        onPool(ctx -> {
            ctx.createTablespaceAndRoot();
            SplitCapableBTreeIndexService btree = ctx.service();
            BTreeCurrentReadService currentRead = ctx.currentReadService(btree);
            BTreeIndex index = ctx.clusteredIndex();
            insertCommitted(ctx, btree, index, smallRow(10));
            TransactionId blocker = TransactionId.of(1401);
            TransactionId waiter = TransactionId.of(1402);
            ctx.lockManager.acquire(blocker, new GapLockKey(INDEX_ID, null, kId(10)),
                    TransactionLockMode.GAP_X, WAIT_TIMEOUT);

            CompletableFuture<BTreeUniqueCheckResult> waitingCheck = CompletableFuture.supplyAsync(
                    () -> currentRead.checkUniqueForInsert(index, kId(5),
                            request(waiter, IsolationLevel.REPEATABLE_READ)));
            awaitUntil(() -> hasWaitEdge(ctx.lockManager, waiter, blocker));

            assertEquals(1, ctx.lockManager.releaseAll(blocker));
            BTreeUniqueCheckResult result = joinUnique(waitingCheck);
            assertFalse(result.duplicate());
            assertTrue(ctx.lockManager.snapshot().grantedLocks().stream()
                    .anyMatch(lock -> lock.owner().equals(waiter)
                            && lock.mode() == TransactionLockMode.INSERT_INTENTION));
            assertEquals(1, ctx.lockManager.releaseAll(waiter));
        });
    }

    /**
     * 验证 {@code uniqueInsertCheckWaitsOnDuplicateRecordAndReportsDuplicate} 所描述的并发场景，并断言等待、唤醒、超时与资源释放顺序。
     */
    @Test
    void uniqueInsertCheckWaitsOnDuplicateRecordAndReportsDuplicate() {
        onPool(ctx -> {
            ctx.createTablespaceAndRoot();
            SplitCapableBTreeIndexService btree = ctx.service();
            BTreeCurrentReadService currentRead = ctx.currentReadService(btree);
            BTreeIndex index = ctx.clusteredIndex();
            BTreeInsertResult inserted = insertCommitted(ctx, btree, index, smallRow(1));
            TransactionId blocker = TransactionId.of(1501);
            TransactionId waiter = TransactionId.of(1502);
            ctx.lockManager.acquire(blocker, RecordLockKey.from(inserted.recordRef()),
                    TransactionLockMode.REC_X, WAIT_TIMEOUT);

            CompletableFuture<BTreeUniqueCheckResult> waitingCheck = CompletableFuture.supplyAsync(
                    () -> currentRead.checkUniqueForInsert(index, kId(1),
                            request(waiter, IsolationLevel.READ_COMMITTED)));
            awaitUntil(() -> hasWaitEdge(ctx.lockManager, waiter, blocker));

            assertEquals(1, ctx.lockManager.releaseAll(blocker));
            BTreeUniqueCheckResult result = joinUnique(waitingCheck);
            assertTrue(result.duplicate());
            assertEquals(1L, idOf(result.duplicateRecord().orElseThrow()));
            assertTrue(ctx.lockManager.snapshot().grantedLocks().stream()
                    .anyMatch(lock -> lock.owner().equals(waiter)
                            && lock.mode() == TransactionLockMode.REC_S));
            assertEquals(1, ctx.lockManager.releaseAll(waiter));
        });
    }

    /**
     * 验证 {@code currentReadPropagatesLockWaitTimeoutAndCleansWaitingState} 所描述的并发场景，并断言等待、唤醒、超时与资源释放顺序。
     */
    @Test
    void currentReadPropagatesLockWaitTimeoutAndCleansWaitingState() {
        onPool(ctx -> {
            ctx.createTablespaceAndRoot();
            SplitCapableBTreeIndexService btree = ctx.service();
            BTreeCurrentReadService currentRead = ctx.currentReadService(btree);
            BTreeIndex index = ctx.clusteredIndex();
            BTreeInsertResult inserted = insertCommitted(ctx, btree, index, smallRow(1));
            TransactionId blocker = TransactionId.of(1601);
            TransactionId waiter = TransactionId.of(1602);
            ctx.lockManager.acquire(blocker, RecordLockKey.from(inserted.recordRef()),
                    TransactionLockMode.REC_X, WAIT_TIMEOUT);

            assertThrows(LockWaitTimeoutException.class,
                    () -> currentRead.lockPoint(index, kId(1),
                            new BTreeCurrentReadRequest(waiter, IsolationLevel.READ_COMMITTED,
                                    Duration.ofMillis(20), 2),
                            BTreeCurrentReadMode.FOR_UPDATE));

            assertTrue(ctx.lockManager.snapshot().waitingLocks().stream()
                    .noneMatch(lock -> lock.owner().equals(waiter)));
            assertTrue(ctx.lockManager.snapshot().waitEdges().stream()
                    .noneMatch(edge -> edge.waitingTransactionId().equals(waiter)));
            assertEquals(1, ctx.lockManager.releaseAll(blocker));
        });
    }

    /**
     * 验证 {@code currentReadPropagatesDeadlockDetectedExceptionAndCleansVictimState} 所描述的并发场景，并断言等待、唤醒、超时与资源释放顺序。
     */
    @Test
    void currentReadPropagatesDeadlockDetectedExceptionAndCleansVictimState() {
        onPool(ctx -> {
            ctx.createTablespaceAndRoot();
            SplitCapableBTreeIndexService btree = ctx.service();
            BTreeCurrentReadService currentRead = ctx.currentReadService(btree);
            BTreeIndex index = ctx.clusteredIndex();
            BTreeInsertResult first = insertCommitted(ctx, btree, index, smallRow(1));
            BTreeInsertResult second = insertCommitted(ctx, btree, index, smallRow(2));
            TransactionId firstOwner = TransactionId.of(1701);
            TransactionId secondOwner = TransactionId.of(1702);
            ctx.lockManager.acquire(firstOwner, RecordLockKey.from(first.recordRef()),
                    TransactionLockMode.REC_X, WAIT_TIMEOUT);
            ctx.lockManager.acquire(secondOwner, RecordLockKey.from(second.recordRef()),
                    TransactionLockMode.REC_X, WAIT_TIMEOUT);

            CompletableFuture<Optional<BTreeLookupResult>> firstWaitsForSecond = CompletableFuture.supplyAsync(
                    () -> currentRead.lockPoint(index, kId(2),
                            new BTreeCurrentReadRequest(firstOwner, IsolationLevel.READ_COMMITTED,
                                    Duration.ofSeconds(2), 2),
                            BTreeCurrentReadMode.FOR_UPDATE));
            awaitUntil(() -> hasWaitEdge(ctx.lockManager, firstOwner, secondOwner));

            assertThrows(DeadlockDetectedException.class,
                    () -> currentRead.lockPoint(index, kId(1),
                            request(secondOwner, IsolationLevel.READ_COMMITTED),
                            BTreeCurrentReadMode.FOR_UPDATE));

            assertTrue(ctx.lockManager.snapshot().waitingLocks().stream()
                    .noneMatch(lock -> lock.owner().equals(secondOwner)));
            assertTrue(ctx.lockManager.snapshot().waitEdges().stream()
                    .noneMatch(edge -> edge.waitingTransactionId().equals(secondOwner)));
            assertEquals(1, ctx.lockManager.releaseAll(secondOwner));
            BTreeLookupResult result = join(firstWaitsForSecond).orElseThrow();
            assertEquals(2L, idOf(result));
            assertEquals(2, ctx.lockManager.releaseAll(firstOwner));
        });
    }

    /**
     * 验证 {@code currentReadReleasesGrantedLockWhenRelocationFails} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void currentReadReleasesGrantedLockWhenRelocationFails() {
        onPool(ctx -> {
            ctx.createTablespaceAndRoot();
            SplitCapableBTreeIndexService btree = ctx.service();
            BTreeCurrentReadService currentRead = ctx.currentReadService(btree);
            BTreeIndex index = ctx.clusteredIndex();
            BTreeInsertResult inserted = insertCommitted(ctx, btree, index, smallRow(1));
            TransactionId blocker = TransactionId.of(1801);
            TransactionId waiter = TransactionId.of(1802);
            ctx.lockManager.acquire(blocker, RecordLockKey.from(inserted.recordRef()),
                    TransactionLockMode.REC_X, WAIT_TIMEOUT);

            CompletableFuture<Optional<BTreeLookupResult>> waitingRead = CompletableFuture.supplyAsync(
                    () -> currentRead.lockPoint(index, kId(1), request(waiter, IsolationLevel.READ_COMMITTED),
                            BTreeCurrentReadMode.FOR_UPDATE));
            awaitUntil(() -> hasWaitEdge(ctx.lockManager, waiter, blocker));

            ctx.flushAllDirty();
            ctx.pool.invalidateTablespace(SPACE, Duration.ofMillis(500));
            ctx.store.close(SPACE);
            assertEquals(1, ctx.lockManager.releaseAll(blocker));
            ExecutionException failure = joinFailure(waitingRead);

            assertTrue(failure.getCause() instanceof TablespaceNotOpenException);
            assertTrue(ctx.lockManager.snapshot().grantedLocks().stream()
                    .noneMatch(lock -> lock.owner().equals(waiter)),
                    "relocation failure must not leak the transaction lock granted to current-read");
        });
    }

    /**
     * 验证 {@code repeatableReadRangeTakesNextKeyAndTerminalGapLocks} 所描述的并发场景，并断言等待、唤醒、超时与资源释放顺序。
     */
    @Test
    void repeatableReadRangeTakesNextKeyAndTerminalGapLocks() {
        onPool(ctx -> {
            ctx.createTablespaceAndRoot();
            SplitCapableBTreeIndexService btree = ctx.service();
            BTreeCurrentReadService currentRead = ctx.currentReadService(btree);
            BTreeIndex index = ctx.clusteredIndex();
            insertCommitted(ctx, btree, index, smallRow(1));
            BTreeInsertResult three = insertCommitted(ctx, btree, index, smallRow(3));
            BTreeInsertResult five = insertCommitted(ctx, btree, index, smallRow(5));
            insertCommitted(ctx, btree, index, smallRow(10));
            TransactionId owner = TransactionId.of(1901);

            List<BTreeLookupResult> rows = currentRead.lockRange(index,
                    new BTreeScanRange(kId(2), true, kId(6), true, 20),
                    request(owner, IsolationLevel.REPEATABLE_READ), BTreeCurrentReadMode.FOR_UPDATE);

            assertEquals(List.of(3L, 5L), idsOf(rows));
            assertTrue(ctx.lockManager.snapshot().grantedLocks().stream()
                    .anyMatch(lock -> lock.owner().equals(owner)
                            && lock.key() instanceof NextKeyLockKey nextKey
                            && nextKey.recordKey().equals(RecordLockKey.from(three.recordRef()))
                            && lock.mode() == TransactionLockMode.NEXT_KEY_X));
            assertTrue(ctx.lockManager.snapshot().grantedLocks().stream()
                    .anyMatch(lock -> lock.owner().equals(owner)
                            && lock.key() instanceof NextKeyLockKey nextKey
                            && nextKey.recordKey().equals(RecordLockKey.from(five.recordRef()))
                            && lock.mode() == TransactionLockMode.NEXT_KEY_X));
            assertTrue(ctx.lockManager.snapshot().grantedLocks().stream()
                    .anyMatch(lock -> lock.owner().equals(owner)
                            && lock.key().equals(new GapLockKey(INDEX_ID, kId(5), kId(10)))
                            && lock.mode() == TransactionLockMode.GAP_X));
            assertEquals(3, ctx.lockManager.releaseAll(owner));
        });
    }

    /**
     * 验证 {@code repeatableReadEmptyRangeTakesGapLockAndBlocksInsertIntention} 所描述的并发场景，并断言等待、唤醒、超时与资源释放顺序。
     */
    @Test
    void repeatableReadEmptyRangeTakesGapLockAndBlocksInsertIntention() {
        onPool(ctx -> {
            ctx.createTablespaceAndRoot();
            SplitCapableBTreeIndexService btree = ctx.service();
            BTreeCurrentReadService currentRead = ctx.currentReadService(btree);
            BTreeIndex index = ctx.clusteredIndex();
            insertCommitted(ctx, btree, index, smallRow(1));
            insertCommitted(ctx, btree, index, smallRow(10));
            TransactionId reader = TransactionId.of(2001);
            TransactionId inserter = TransactionId.of(2002);
            GapLockKey rangeGap = new GapLockKey(INDEX_ID, kId(1), kId(10));

            List<BTreeLookupResult> rows = currentRead.lockRange(index,
                    new BTreeScanRange(kId(2), true, kId(5), true, 20),
                    request(reader, IsolationLevel.REPEATABLE_READ), BTreeCurrentReadMode.FOR_UPDATE);
            CompletableFuture<LockHandle> waitingInsert = CompletableFuture.supplyAsync(
                    () -> ctx.lockManager.acquire(inserter, new InsertIntentionLockKey(rangeGap),
                            TransactionLockMode.INSERT_INTENTION, WAIT_TIMEOUT));
            awaitUntil(() -> hasWaitEdge(ctx.lockManager, inserter, reader));

            assertTrue(rows.isEmpty());
            assertEquals(1, ctx.lockManager.releaseAll(reader));
            joinHandle(waitingInsert).close();
        });
    }

    /**
     * 验证 {@code readCommittedRangeTakesRecordLocksOnly} 所描述的并发场景，并断言等待、唤醒、超时与资源释放顺序。
     */
    @Test
    void readCommittedRangeTakesRecordLocksOnly() {
        onPool(ctx -> {
            ctx.createTablespaceAndRoot();
            SplitCapableBTreeIndexService btree = ctx.service();
            BTreeCurrentReadService currentRead = ctx.currentReadService(btree);
            BTreeIndex index = ctx.clusteredIndex();
            insertCommitted(ctx, btree, index, smallRow(1));
            insertCommitted(ctx, btree, index, smallRow(3));
            insertCommitted(ctx, btree, index, smallRow(5));
            insertCommitted(ctx, btree, index, smallRow(10));
            TransactionId owner = TransactionId.of(2101);

            List<BTreeLookupResult> rows = currentRead.lockRange(index,
                    new BTreeScanRange(kId(2), true, kId(6), true, 20),
                    request(owner, IsolationLevel.READ_COMMITTED), BTreeCurrentReadMode.FOR_UPDATE);

            assertEquals(List.of(3L, 5L), idsOf(rows));
            assertEquals(2, ctx.lockManager.snapshot().grantedLocks().stream()
                    .filter(lock -> lock.owner().equals(owner) && lock.mode() == TransactionLockMode.REC_X)
                    .count());
            assertTrue(ctx.lockManager.snapshot().grantedLocks().stream()
                    .noneMatch(lock -> lock.owner().equals(owner)
                            && (lock.key() instanceof GapLockKey || lock.key() instanceof NextKeyLockKey)));
            LockHandle insertIntention = ctx.lockManager.acquire(TransactionId.of(2102),
                    new InsertIntentionLockKey(new GapLockKey(INDEX_ID, kId(5), kId(10))),
                    TransactionLockMode.INSERT_INTENTION, WAIT_TIMEOUT);
            insertIntention.close();
            assertEquals(2, ctx.lockManager.releaseAll(owner));
        });
    }

    /**
     * 验证 {@code rangeWaitsWithoutHoldingPageLatchOrBufferFix} 所描述的并发场景，并断言等待、唤醒、超时与资源释放顺序。
     */
    @Test
    void rangeWaitsWithoutHoldingPageLatchOrBufferFix() {
        onPool(ctx -> {
            ctx.createTablespaceAndRoot();
            SplitCapableBTreeIndexService btree = ctx.service();
            BTreeCurrentReadService currentRead = ctx.currentReadService(btree);
            BTreeIndex index = ctx.clusteredIndex();
            insertCommitted(ctx, btree, index, smallRow(1));
            insertCommitted(ctx, btree, index, smallRow(3));
            insertCommitted(ctx, btree, index, smallRow(5));
            insertCommitted(ctx, btree, index, smallRow(10));
            TransactionId blocker = TransactionId.of(2201);
            TransactionId waiter = TransactionId.of(2202);
            GapLockKey beforeFive = new GapLockKey(INDEX_ID, kId(3), kId(5));
            ctx.lockManager.acquire(blocker, new InsertIntentionLockKey(beforeFive),
                    TransactionLockMode.INSERT_INTENTION, WAIT_TIMEOUT);

            CompletableFuture<List<BTreeLookupResult>> waitingRange = CompletableFuture.supplyAsync(
                    () -> currentRead.lockRange(index, new BTreeScanRange(kId(2), true, kId(6), true, 20),
                            request(waiter, IsolationLevel.REPEATABLE_READ), BTreeCurrentReadMode.FOR_UPDATE));
            awaitUntil(() -> hasWaitEdge(ctx.lockManager, waiter, blocker));

            assertTrue(canTakeRootExclusiveLatch(ctx, index),
                    "range current-read must release page latch and buffer fix before waiting on LockManager");
            assertEquals(1, ctx.lockManager.releaseAll(blocker));
            assertEquals(List.of(3L, 5L), idsOf(joinRange(waitingRange)));
            assertEquals(3, ctx.lockManager.releaseAll(waiter));
        });
    }

    /**
     * SERIALIZABLE range locking read 必须与 RR 一样取得每行 next-key 和 terminal gap，
     * 不能退化成 RC 的纯 record 锁。
     */
    @Test
    void serializableRangeUsesNextKeyAndTerminalGapLocks() {
        onPool(ctx -> {
            ctx.createTablespaceAndRoot();
            SplitCapableBTreeIndexService btree = ctx.service();
            BTreeCurrentReadService currentRead = ctx.currentReadService(btree);
            BTreeIndex index = ctx.clusteredIndex();
            insertCommitted(ctx, btree, index, smallRow(3));
            insertCommitted(ctx, btree, index, smallRow(5));
            TransactionId owner = TransactionId.of(2250);

            assertEquals(List.of(3L, 5L), idsOf(currentRead.lockRange(
                    index, new BTreeScanRange(kId(2), true, kId(6), true, 20),
                    request(owner, IsolationLevel.SERIALIZABLE),
                    BTreeCurrentReadMode.FOR_SHARE)));
            var granted = ctx.lockManager.snapshot().grantedLocks().stream()
                    .filter(lock -> lock.owner().equals(owner)).toList();
            assertEquals(3, granted.size());
            assertEquals(2, granted.stream()
                    .filter(lock -> lock.mode() == TransactionLockMode.NEXT_KEY_S).count());
            assertEquals(1, granted.stream()
                    .filter(lock -> lock.mode() == TransactionLockMode.GAP_S).count());
            assertEquals(3, ctx.lockManager.releaseAll(owner));
        });
    }

    /**
     * 验证 {@code rangeRelocatesAfterRowsChangeDuringWait} 所描述的并发场景，并断言等待、唤醒、超时与资源释放顺序。
     */
    @Test
    void rangeRelocatesAfterRowsChangeDuringWait() {
        onPool(ctx -> {
            ctx.createTablespaceAndRoot();
            SplitCapableBTreeIndexService btree = ctx.service();
            BTreeCurrentReadService currentRead = ctx.currentReadService(btree);
            BTreeIndex index = ctx.clusteredIndex();
            insertCommitted(ctx, btree, index, smallRow(1));
            insertCommitted(ctx, btree, index, smallRow(3));
            BTreeInsertResult five = insertCommitted(ctx, btree, index, smallRow(5));
            insertCommitted(ctx, btree, index, smallRow(7));
            insertCommitted(ctx, btree, index, smallRow(10));
            TransactionId blocker = TransactionId.of(2301);
            TransactionId waiter = TransactionId.of(2302);
            GapLockKey staleBeforeFive = new GapLockKey(INDEX_ID, kId(3), kId(5));
            ctx.lockManager.acquire(blocker, new InsertIntentionLockKey(staleBeforeFive),
                    TransactionLockMode.INSERT_INTENTION, WAIT_TIMEOUT);

            CompletableFuture<List<BTreeLookupResult>> waitingRange = CompletableFuture.supplyAsync(
                    () -> currentRead.lockRange(index, new BTreeScanRange(kId(2), true, kId(8), true, 20),
                            request(waiter, IsolationLevel.REPEATABLE_READ), BTreeCurrentReadMode.FOR_UPDATE));
            awaitUntil(() -> hasWaitEdge(ctx.lockManager, waiter, blocker));

            insertCommitted(ctx, btree, index, smallRow(4));
            assertEquals(1, ctx.lockManager.releaseAll(blocker));
            assertEquals(List.of(3L, 4L, 5L, 7L), idsOf(joinRange(waitingRange)));
            assertTrue(ctx.lockManager.snapshot().grantedLocks().stream()
                    .noneMatch(lock -> lock.owner().equals(waiter)
                            && lock.key() instanceof NextKeyLockKey nextKey
                            && nextKey.recordKey().equals(RecordLockKey.from(five.recordRef()))
                            && nextKey.gapKey().equals(staleBeforeFive)));
            assertEquals(5, ctx.lockManager.releaseAll(waiter));
        });
    }

    /**
     * 验证 {@code rangeTimeoutReleasesPreviouslyGrantedLocks} 所描述的并发场景，并断言等待、唤醒、超时与资源释放顺序。
     */
    @Test
    void rangeTimeoutReleasesPreviouslyGrantedLocks() {
        onPool(ctx -> {
            ctx.createTablespaceAndRoot();
            SplitCapableBTreeIndexService btree = ctx.service();
            BTreeCurrentReadService currentRead = ctx.currentReadService(btree);
            BTreeIndex index = ctx.clusteredIndex();
            insertCommitted(ctx, btree, index, smallRow(1));
            insertCommitted(ctx, btree, index, smallRow(3));
            insertCommitted(ctx, btree, index, smallRow(5));
            TransactionId blocker = TransactionId.of(2401);
            TransactionId waiter = TransactionId.of(2402);
            ctx.lockManager.acquire(blocker, new InsertIntentionLockKey(new GapLockKey(INDEX_ID, kId(3), kId(5))),
                    TransactionLockMode.INSERT_INTENTION, WAIT_TIMEOUT);

            assertThrows(LockWaitTimeoutException.class,
                    () -> currentRead.lockRange(index, new BTreeScanRange(kId(2), true, kId(6), true, 20),
                            new BTreeCurrentReadRequest(waiter, IsolationLevel.REPEATABLE_READ,
                                    Duration.ofMillis(20), 2),
                            BTreeCurrentReadMode.FOR_UPDATE));

            assertTrue(ctx.lockManager.snapshot().grantedLocks().stream()
                    .noneMatch(lock -> lock.owner().equals(waiter)),
                    "range timeout must release locks granted earlier in the same attempt");
            assertEquals(1, ctx.lockManager.releaseAll(blocker));
        });
    }

    /**
     * 验证同一 range current-read 的多个锁等待共享单一绝对预算：第一行消耗的等待时间必须从第二行扣除，
     * 第二个冲突锁不得重新获得完整 timeout；失败后本轮已授予的第一行锁也必须被清理。
     */
    @Test
    void rangeLocksShareOneAbsoluteWaitBudget() {
        onPool(ctx -> {
            ctx.createTablespaceAndRoot();
            SplitCapableBTreeIndexService btree = ctx.service();
            BTreeCurrentReadService currentRead = ctx.currentReadService(btree);
            BTreeIndex index = ctx.clusteredIndex();
            BTreeInsertResult three = insertCommitted(ctx, btree, index, smallRow(3));
            BTreeInsertResult five = insertCommitted(ctx, btree, index, smallRow(5));
            TransactionId firstBlocker = TransactionId.of(2451);
            TransactionId secondBlocker = TransactionId.of(2452);
            TransactionId waiter = TransactionId.of(2453);
            ctx.lockManager.acquire(firstBlocker, RecordLockKey.from(three.recordRef()),
                    TransactionLockMode.REC_X, WAIT_TIMEOUT);
            ctx.lockManager.acquire(secondBlocker, RecordLockKey.from(five.recordRef()),
                    TransactionLockMode.REC_X, WAIT_TIMEOUT);
            Duration totalBudget = Duration.ofMillis(600);

            // 1、waiter 先被第一行阻塞，明确消费超过一半的总预算。
            CompletableFuture<List<BTreeLookupResult>> waitingRange = CompletableFuture.supplyAsync(
                    () -> currentRead.lockRange(index,
                            new BTreeScanRange(kId(2), true, kId(6), true, 20),
                            new BTreeCurrentReadRequest(waiter, IsolationLevel.READ_COMMITTED,
                                    totalBudget, 2),
                            BTreeCurrentReadMode.FOR_UPDATE));
            awaitUntil(() -> hasWaitEdge(ctx.lockManager, waiter, firstBlocker));
            try {
                TimeUnit.MILLISECONDS.sleep(350);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new AssertionError("interrupted while consuming first range-lock budget", interrupted);
            }

            // 2、释放第一行后 waiter 会取得它并转等第二行，但第二行只能使用不足 250ms 的剩余预算。
            assertEquals(1, ctx.lockManager.releaseAll(firstBlocker));
            awaitUntil(() -> hasWaitEdge(ctx.lockManager, waiter, secondBlocker));
            ExecutionException timeout = assertThrows(ExecutionException.class,
                    () -> waitingRange.get(400, TimeUnit.MILLISECONDS),
                    "second row must not receive a fresh 600ms timeout");

            // 3、总预算超时沿用领域异常，并逆序释放本次 range 已取得的第一行锁和 wait queue。
            assertTrue(timeout.getCause() instanceof LockWaitTimeoutException);
            assertTrue(ctx.lockManager.snapshot().grantedLocks().stream()
                    .noneMatch(lock -> lock.owner().equals(waiter)));
            assertTrue(ctx.lockManager.snapshot().waitEdges().stream()
                    .noneMatch(edge -> edge.waitingTransactionId().equals(waiter)));
            assertEquals(1, ctx.lockManager.releaseAll(secondBlocker));
        });
    }

    /**
     * 验证 {@code rangeDeadlockReleasesAttemptLocksButKeepsPreExistingOwnerLocks} 所描述的并发场景，并断言等待、唤醒、超时与资源释放顺序。
     */
    @Test
    void rangeDeadlockReleasesAttemptLocksButKeepsPreExistingOwnerLocks() {
        onPool(ctx -> {
            ctx.createTablespaceAndRoot();
            SplitCapableBTreeIndexService btree = ctx.service();
            BTreeCurrentReadService currentRead = ctx.currentReadService(btree);
            BTreeIndex index = ctx.clusteredIndex();
            insertCommitted(ctx, btree, index, smallRow(1));
            BTreeInsertResult three = insertCommitted(ctx, btree, index, smallRow(3));
            insertCommitted(ctx, btree, index, smallRow(5));
            TransactionId first = TransactionId.of(2501);
            TransactionId second = TransactionId.of(2502);
            RecordLockKey rowThree = RecordLockKey.from(three.recordRef());
            ctx.lockManager.acquire(first, rowThree, TransactionLockMode.REC_X, WAIT_TIMEOUT);
            ctx.lockManager.acquire(second, new InsertIntentionLockKey(new GapLockKey(INDEX_ID, kId(3), kId(5))),
                    TransactionLockMode.INSERT_INTENTION, WAIT_TIMEOUT);

            CompletableFuture<Optional<BTreeLookupResult>> secondWaitsForFirst = CompletableFuture.supplyAsync(
                    () -> currentRead.lockPoint(index, kId(3),
                            new BTreeCurrentReadRequest(second, IsolationLevel.READ_COMMITTED,
                                    Duration.ofSeconds(2), 2),
                            BTreeCurrentReadMode.FOR_UPDATE));
            awaitUntil(() -> hasWaitEdge(ctx.lockManager, second, first));

            assertThrows(DeadlockDetectedException.class,
                    () -> currentRead.lockRange(index, new BTreeScanRange(kId(2), true, kId(6), true, 20),
                            request(first, IsolationLevel.REPEATABLE_READ), BTreeCurrentReadMode.FOR_UPDATE));

            assertTrue(ctx.lockManager.snapshot().grantedLocks().stream()
                    .noneMatch(lock -> lock.owner().equals(first) && lock.key() instanceof NextKeyLockKey),
                    "range deadlock must release next-key locks granted in the failed attempt");
            assertTrue(ctx.lockManager.snapshot().grantedLocks().stream()
                    .anyMatch(lock -> lock.owner().equals(first)
                            && lock.key().equals(rowThree)
                            && lock.mode() == TransactionLockMode.REC_X),
                    "cleanup must not release locks the transaction held before lockRange");
            ctx.lockManager.releaseAll(second);
            joinFailure(secondWaitsForFirst);
            assertEquals(1, ctx.lockManager.releaseAll(first));
        });
    }

    private BTreeInsertResult insertCommitted(Ctx ctx, SplitCapableBTreeIndexService btree,
                                              BTreeIndex index, LogicalRecord row) {
        MiniTransaction m = ctx.mgr.begin();
        try {
            BTreeInsertResult result = btree.insertClustered(m, index, row, TransactionId.of(77), RollPointer.NULL);
            ctx.mgr.commit(m);
            return result;
        } catch (RuntimeException e) {
            ctx.mgr.rollbackUncommitted(m);
            throw e;
        }
    }

    private boolean canTakeRootExclusiveLatch(Ctx ctx, BTreeIndex index) {
        CompletableFuture<Boolean> probe = CompletableFuture.supplyAsync(() -> {
            MiniTransaction m = ctx.mgr.begin();
            try {
                ctx.access.openIndexPage(m, index.rootPageId(), PageLatchMode.EXCLUSIVE);
                ctx.mgr.commit(m);
                return true;
            } catch (RuntimeException e) {
                ctx.mgr.rollbackUncommitted(m);
                throw e;
            }
        });
        try {
            return probe.get(1, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new AssertionError("could not take root X latch while current-read waits", e);
        }
    }

    private static boolean hasWaitEdge(LockManager manager, TransactionId waiter, TransactionId blocker) {
        return manager.snapshot().waitEdges().stream()
                .anyMatch(edge -> edge.waitingTransactionId().equals(waiter)
                        && edge.blockingTransactionId().equals(blocker));
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
                throw new AssertionError("interrupted while waiting for current-read condition", e);
            }
        }
        throw new AssertionError("current-read condition was not satisfied before timeout");
    }

    private static Optional<BTreeLookupResult> join(CompletableFuture<Optional<BTreeLookupResult>> future) {
        try {
            return future.get(2, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new AssertionError("current-read future did not complete", e);
        }
    }

    private static List<BTreeLookupResult> joinRange(CompletableFuture<List<BTreeLookupResult>> future) {
        try {
            return future.get(2, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new AssertionError("range current-read future did not complete", e);
        }
    }

    private static BTreeUniqueCheckResult joinUnique(CompletableFuture<BTreeUniqueCheckResult> future) {
        try {
            return future.get(2, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new AssertionError("unique-check future did not complete", e);
        }
    }

    private static LockHandle joinHandle(CompletableFuture<LockHandle> future) {
        try {
            return future.get(2, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new AssertionError("lock future did not complete", e);
        }
    }

    private static ExecutionException joinFailure(CompletableFuture<?> future) {
        try {
            future.get(2, TimeUnit.SECONDS);
            throw new AssertionError("current-read future completed successfully");
        } catch (ExecutionException e) {
            return e;
        } catch (Exception e) {
            throw new AssertionError("current-read future did not fail as expected", e);
        }
    }

    private static BTreeCurrentReadRequest request(TransactionId owner, IsolationLevel isolationLevel) {
        return new BTreeCurrentReadRequest(owner, isolationLevel, WAIT_TIMEOUT, 3);
    }

    private static long idOf(BTreeLookupResult result) {
        return ((ColumnValue.IntValue) result.record().columnValues().get(0)).value();
    }

    private static List<Long> idsOf(List<BTreeLookupResult> rows) {
        return rows.stream().map(BTreeCurrentReadServiceTest::idOf).toList();
    }

    private static TableSchema clusteredSchema() {
        return new TableSchema(1, List.of(
                new ColumnDef(new ColumnId(0), "id", ColumnType.intType(false, false), 0),
                new ColumnDef(new ColumnId(1), "payload", ColumnType.varchar(5000, true), 1)), true);
    }

    private static IndexKeyDef idKey() {
        return new IndexKeyDef(INDEX_ID, List.of(new KeyPartDef(new ColumnId(0), KeyOrder.ASC, 0)));
    }

    private static SearchKey kId(long id) {
        return new SearchKey(List.of(new ColumnValue.IntValue(id)));
    }

    private static LogicalRecord smallRow(long id) {
        return new LogicalRecord(1, List.of(new ColumnValue.IntValue(id),
                new ColumnValue.StringValue("v")), false, RecordType.CONVENTIONAL);
    }

    private static LogicalRecord wideRow(long id) {
        return new LogicalRecord(1, List.of(new ColumnValue.IntValue(id),
                new ColumnValue.StringValue("x".repeat(5000))), false, RecordType.CONVENTIONAL);
    }

    private void onPool(Body body) {
        PageStore store = new FileChannelPageStore();
        try (PageStore ignored = store;
             BufferPool pool = new LruBufferPool(store, PS, 128);
             RedoLogFileRepository redoRepo = RedoLogFileRepository.open(dir.resolve("current-read-redo.log"))) {
            RedoLogManager redo = RedoLogManager.durable(redoRepo);
            body.run(new Ctx(store, pool, redo));
        }
    }

    private interface Body {
        void run(Ctx ctx);
    }

    private final class Ctx {
        private final MiniTransactionManager mgr;
        private final PageStore store;
        private final BufferPool pool;
        private final RedoLogManager redo;
        private final DiskSpaceManager disk;
        private final IndexPageAccess access;
        private final LockManager lockManager = new LockManager(4, 32);
        private SegmentRef leafSegment;
        private SegmentRef nonLeafSegment;
        private PageId rootPageId;

        private Ctx(PageStore store, BufferPool pool, RedoLogManager redo) {
            this.store = store;
            this.pool = pool;
            this.redo = redo;
            this.mgr = new MiniTransactionManager(new TablespaceAccessController(), redo);
            this.disk = new DiskSpaceManager(pool, store, PS);
            this.access = new IndexPageAccess(pool, PS);
        }

        private SplitCapableBTreeIndexService service() {
            return new SplitCapableBTreeIndexService(access, disk, registry);
        }

        private BTreeCurrentReadService currentReadService(SplitCapableBTreeIndexService btree) {
            return new BTreeCurrentReadService(mgr, btree, lockManager);
        }

        private void createTablespaceAndRoot() {
            MiniTransaction m = mgr.begin();
            disk.createTablespace(m, SPACE, dir.resolve("current-read.ibd"), PageNo.of(64));
            leafSegment = disk.createSegment(m, SPACE, SegmentPurpose.INDEX_LEAF);
            nonLeafSegment = disk.createSegment(m, SPACE, SegmentPurpose.INDEX_NON_LEAF);
            rootPageId = disk.allocatePage(m, leafSegment);
            access.createIndexPage(m, rootPageId, INDEX_ID, 0);
            mgr.commit(m);
        }

        private void flushAllDirty() {
            redo.flush();
            FlushCoordinator coordinator = new FlushCoordinator(pool, store, redo, PS,
                    new NoDoublewriteStrategy(), Duration.ofMillis(50));
            coordinator.flushList(Lsn.of(Long.MAX_VALUE), pool.capacity());
        }

        private BTreeIndex clusteredIndex() {
            return new BTreeIndex(INDEX_ID, rootPageId, 0, idKey(), clusteredSchema(), true,
                    leafSegment, nonLeafSegment);
        }
    }
}
