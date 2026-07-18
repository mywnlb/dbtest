package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.domain.RollbackSegmentId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.api.DiskSpaceManager;
import cn.zhangyis.db.storage.api.DiskSpaceUndoAllocator;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.fil.io.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import cn.zhangyis.db.storage.record.format.HiddenColumns;
import cn.zhangyis.db.storage.record.schema.ColumnDef;
import cn.zhangyis.db.storage.record.schema.ColumnId;
import cn.zhangyis.db.storage.record.schema.ColumnType;
import cn.zhangyis.db.storage.record.schema.IndexKeyDef;
import cn.zhangyis.db.storage.record.schema.KeyOrder;
import cn.zhangyis.db.storage.record.schema.KeyPartDef;
import cn.zhangyis.db.storage.record.schema.TableSchema;
import cn.zhangyis.db.storage.record.type.ColumnValue;
import cn.zhangyis.db.storage.record.type.TypeCodecRegistry;
import cn.zhangyis.db.storage.undo.RollbackSegmentHeaderSnapshot;
import cn.zhangyis.db.storage.undo.UndoLogKind;
import cn.zhangyis.db.storage.undo.UndoHistoryNodeSnapshot;
import cn.zhangyis.db.storage.undo.UndoLogSegment;
import cn.zhangyis.db.storage.undo.UndoLogSegmentAccess;
import cn.zhangyis.db.storage.undo.UndoLogicalHead;
import cn.zhangyis.db.storage.undo.UndoSegmentHandle;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * undo segment reuse 生产链协作测试：终结时 active→cache/free、cache LIFO 与 free FIFO 复用，以及多页段拒绝复用。
 */
class UndoSegmentCacheIntegrationTest {

    private static final PageSize PAGE_SIZE = PageSize.ofBytes(16 * 1024);
    private static final SpaceId UNDO_SPACE = SpaceId.of(81);
    private static final long TABLE_ID = 1L;
    private static final long INDEX_ID = 9L;

    @TempDir
    Path dir;

    @Test
    void insertCommitCachesInLifoOrderAndNextFirstWriteReusesTopOwner() {
        withHarness(2, h -> {
            Transaction first = h.newWriter();
            Transaction second = h.newWriter();
            PageId firstPage = h.appendInsert(first, 1);
            PageId secondPage = h.appendInsert(second, 2);
            assertNotEquals(firstPage, secondPage, "两个并发 active log 必须先各自分配物理段");

            h.commit(first);
            h.commit(second);

            RollbackSegmentHeaderSnapshot cached = h.snapshot();
            assertEquals(List.of(firstPage, secondPage), cached.cachedInsertSegments(),
                    "终结顺序形成栈底到栈顶的持久 LIFO 目录");
            assertEquals(2, h.finalization.cache().cachedCount(UndoLogKind.INSERT));
            assertTrue(cached.occupiedSlots().isEmpty());

            Transaction reused = h.newWriter();
            UndoWritePlan plan = h.undoManager.planInsert(
                    reused, TABLE_ID, INDEX_ID, key(3), keyDef(), schema());
            assertEquals(UndoSegmentAcquisition.REUSE_CACHED, plan.acquisition());
            assertEquals(secondPage, plan.expectedFirstPageId(), "首写必须弹出同 kind 栈顶");
            MiniTransaction append = h.mtrManager.begin();
            h.undoManager.appendPlanned(reused, append, plan);
            h.mtrManager.commit(append);

            assertEquals(secondPage, reused.undoContext().binding(UndoLogKind.INSERT).firstPageId());
            RollbackSegmentHeaderSnapshot active = h.snapshot();
            assertEquals(List.of(firstPage), active.cachedInsertSegments());
            assertTrue(active.occupiedSlots().containsValue(secondPage),
                    "page3 owner 在同一 append MTR 中由 cache 原子转入 active slot");
            MiniTransaction read = h.mtrManager.beginReadOnly();
            UndoLogSegment segment = h.undoAccess.open(read, secondPage, PageLatchMode.SHARED);
            assertTrue(segment.isActive());
            assertEquals(reused.transactionId(), segment.creatorTransactionId());
            h.mtrManager.commit(read);

            h.commit(reused);
            assertEquals(List.of(firstPage, secondPage), h.snapshot().cachedInsertSegments());
        });
    }

    /** 两次 UPDATE commit 形成持久双向链；逐头 purge 原子推进 base/newHead.prev，提交号高水位不回退。 */
    @Test
    void updateCommitAppendsPersistentDoublyLinkedHistoryAndPurgeUnlinksHead() {
        withHarness(0, h -> {
            Transaction first = h.newWriter();
            MiniTransaction firstWrite = h.mtrManager.begin();
            UndoTestWrites.update(h.undoManager, first, firstWrite, TABLE_ID, INDEX_ID, key(1), key(1),
                    new HiddenColumns(first.transactionId(), RollPointer.NULL), keyDef(), schema());
            h.mtrManager.commit(firstWrite);
            PageId firstPage = first.undoContext().binding(UndoLogKind.UPDATE).firstPageId();
            h.commit(first);

            Transaction second = h.newWriter();
            MiniTransaction secondWrite = h.mtrManager.begin();
            UndoTestWrites.update(h.undoManager, second, secondWrite, TABLE_ID, INDEX_ID, key(2), key(2),
                    new HiddenColumns(second.transactionId(), RollPointer.NULL), keyDef(), schema());
            h.mtrManager.commit(secondWrite);
            PageId secondPage = second.undoContext().binding(UndoLogKind.UPDATE).firstPageId();
            h.commit(second);

            RollbackSegmentHeaderSnapshot two = h.snapshot();
            assertEquals(java.util.Optional.of(firstPage), two.historyBase().headPageId());
            assertEquals(java.util.Optional.of(secondPage), two.historyBase().tailPageId());
            assertEquals(2L, two.historyBase().length());
            assertEquals(second.transactionNo(), two.historyBase().lastTransactionNo());
            UndoHistoryNodeSnapshot firstNode = h.historyNode(firstPage);
            UndoHistoryNodeSnapshot secondNode = h.historyNode(secondPage);
            assertTrue(firstNode.previousHistoryPageId().isEmpty());
            assertEquals(java.util.Optional.of(secondPage), firstNode.nextHistoryPageId());
            assertEquals(java.util.Optional.of(firstPage), secondNode.previousHistoryPageId());
            assertTrue(secondNode.nextHistoryPageId().isEmpty());

            HistoryEntry firstEntry = h.history.peekCommitted().orElseThrow();
            h.consumeLogicalHead(firstEntry);
            try (HistoryList.HeadRemovalLease lease = h.history.beginHeadRemoval(firstEntry)) {
                h.finalization.finalizer().finalizePurgedHistory(firstEntry, lease);
            }
            RollbackSegmentHeaderSnapshot one = h.snapshot();
            assertEquals(java.util.Optional.of(secondPage), one.historyBase().headPageId());
            assertEquals(java.util.Optional.of(secondPage), one.historyBase().tailPageId());
            assertEquals(1L, one.historyBase().length());
            assertEquals(second.transactionNo(), one.historyBase().lastTransactionNo());
            assertTrue(h.historyNode(secondPage).previousHistoryPageId().isEmpty());

            HistoryEntry secondEntry = h.history.peekCommitted().orElseThrow();
            h.consumeLogicalHead(secondEntry);
            try (HistoryList.HeadRemovalLease lease = h.history.beginHeadRemoval(secondEntry)) {
                h.finalization.finalizer().finalizePurgedHistory(secondEntry, lease);
            }
            RollbackSegmentHeaderSnapshot empty = h.snapshot();
            assertEquals(0L, empty.historyBase().length());
            assertTrue(empty.historyBase().headPageId().isEmpty());
            assertEquals(second.transactionNo(), empty.historyBase().lastTransactionNo());
            assertFalse(h.finalization.slots().isOccupied(secondEntry.slotId()));
        });
    }

    @Test
    void concurrentUpdateCommitsSerializePersistentAppendWithoutLostTail() throws Exception {
        withHarness(0, h -> {
            Transaction left = h.newWriter();
            Transaction right = h.newWriter();
            MiniTransaction leftWrite = h.mtrManager.begin();
            UndoTestWrites.update(h.undoManager, left, leftWrite, TABLE_ID, INDEX_ID, key(11), key(11),
                    new HiddenColumns(left.transactionId(), RollPointer.NULL), keyDef(), schema());
            h.mtrManager.commit(leftWrite);
            MiniTransaction rightWrite = h.mtrManager.begin();
            UndoTestWrites.update(h.undoManager, right, rightWrite, TABLE_ID, INDEX_ID, key(12), key(12),
                    new HiddenColumns(right.transactionId(), RollPointer.NULL), keyDef(), schema());
            h.mtrManager.commit(rightWrite);

            CountDownLatch start = new CountDownLatch(1);
            try (var executor = Executors.newFixedThreadPool(2)) {
                var leftCommit = executor.submit(() -> {
                    start.await();
                    h.commit(left);
                    return null;
                });
                var rightCommit = executor.submit(() -> {
                    start.await();
                    h.commit(right);
                    return null;
                });
                start.countDown();
                assertDoesNotThrow(() -> leftCommit.get(5, TimeUnit.SECONDS));
                assertDoesNotThrow(() -> rightCommit.get(5, TimeUnit.SECONDS));
            }

            RollbackSegmentHeaderSnapshot page3 = h.snapshot();
            assertEquals(2L, page3.historyBase().length());
            assertEquals(2, h.history.committedSize());
            List<PageId> runtimeOrder = h.history.snapshot().stream()
                    .map(HistoryEntry::undoFirstPageId).toList();
            List<PageId> diskOrder = new ArrayList<>();
            java.util.Optional<PageId> current = page3.historyBase().headPageId();
            while (current.isPresent()) {
                PageId page = current.orElseThrow();
                diskOrder.add(page);
                current = h.historyNode(page).nextHistoryPageId();
            }
            assertEquals(runtimeOrder, diskOrder);
            assertEquals(page3.historyBase().tailPageId(),
                    java.util.Optional.of(diskOrder.getLast()));
        });
    }

    @Test
    void fullCacheKeepsExistingTopAndMovesNewlyFinalizedSegmentToFreeFifo() {
        withHarness(1, h -> {
            Transaction first = h.newWriter();
            Transaction second = h.newWriter();
            PageId firstPage = h.appendInsert(first, 10);
            h.appendInsert(second, 11);
            UndoSegmentHandle secondHandle = h.handle(second, UndoLogKind.INSERT);

            h.commit(first);
            h.commit(second);

            assertEquals(List.of(firstPage), h.snapshot().cachedInsertSegments(),
                    "容量满时保留已有 top，新终结段进入跨 kind free FIFO");
            assertEquals(java.util.Optional.of(secondHandle.firstPageId()),
                    h.snapshot().freeListBase().headPageId());
            MiniTransaction inspect = h.mtrManager.beginReadOnly();
            assertEquals(1L, h.allocator.inspectDropPlan(inspect, secondHandle).usedPageCount(),
                    "free owner 必须继续保留可复用的 FSP inode");
            h.mtrManager.commit(inspect);
        });
    }

    @Test
    void zeroCacheCapacityStillRetainsEligibleSegmentInFreeFifo() {
        withHarness(0, h -> {
            Transaction transaction = h.newWriter();
            h.appendInsert(transaction, 20);
            UndoSegmentHandle handle = h.handle(transaction, UndoLogKind.INSERT);

            h.commit(transaction);

            assertTrue(h.snapshot().cachedInsertSegments().isEmpty());
            assertEquals(java.util.Optional.of(handle.firstPageId()), h.snapshot().freeListBase().headPageId());
            MiniTransaction inspect = h.mtrManager.beginReadOnly();
            assertEquals(1L, h.allocator.inspectDropPlan(inspect, handle).usedPageCount());
            h.mtrManager.commit(inspect);
        });
    }

    /** 同 kind cache 优先保留；目标 kind 无 cache 时，free head 可跨 INSERT→UPDATE 重新分类并保持 FIFO。 */
    @Test
    void freeHeadIsReusedAcrossKindsAfterSameKindCacheMiss() {
        withHarness(1, h -> {
            Transaction cachedOwner = h.newWriter();
            Transaction freeOwner = h.newWriter();
            PageId cachedPage = h.appendInsert(cachedOwner, 21);
            PageId freePage = h.appendInsert(freeOwner, 22);
            h.commit(cachedOwner);
            h.commit(freeOwner);

            assertEquals(List.of(cachedPage), h.snapshot().cachedInsertSegments());
            assertEquals(java.util.Optional.of(freePage), h.snapshot().freeListBase().headPageId());

            Transaction update = h.newWriter();
            UndoWritePlan plan = h.undoManager.planUpdate(update, TABLE_ID, INDEX_ID, key(22), key(22),
                    new HiddenColumns(update.transactionId(), RollPointer.NULL), keyDef(), schema());
            assertEquals(UndoSegmentAcquisition.REUSE_FREE, plan.acquisition());
            assertEquals(freePage, plan.expectedFirstPageId());
            MiniTransaction append = h.mtrManager.begin();
            h.undoManager.appendPlanned(update, append, plan);
            h.mtrManager.commit(append);

            assertEquals(freePage, update.undoContext().binding(UndoLogKind.UPDATE).firstPageId());
            assertEquals(List.of(cachedPage), h.snapshot().cachedInsertSegments(),
                    "跨 kind free reuse 不得消费 INSERT cache top");
            assertEquals(0L, h.snapshot().freeListBase().length());
        });
    }

    @Test
    void multiPageInsertSegmentIsDroppedInsteadOfCached() {
        withHarness(2, h -> {
            Transaction transaction = h.newWriter();
            for (int i = 0; i < 500; i++) {
                h.appendInsertRecord(transaction, 1_000 + i);
            }
            UndoSegmentHandle handle = h.handle(transaction, UndoLogKind.INSERT);
            assertNotEquals(handle.firstPageId(), handle.lastPageId(),
                    "测试前置条件必须真实跨页，不能把单页路径误当成拒绝缓存");

            h.commit(transaction);

            assertTrue(h.snapshot().cachedInsertSegments().isEmpty());
            MiniTransaction inspect = h.mtrManager.beginReadOnly();
            assertThrows(DatabaseRuntimeException.class,
                    () -> h.allocator.inspectDropPlan(inspect, handle));
            h.mtrManager.rollbackUncommitted(inspect);
        });
    }

    @Test
    void purgedSinglePageUpdateUsesIndependentUpdateCache() {
        withHarness(2, h -> {
            Transaction transaction = h.newWriter();
            MiniTransaction write = h.mtrManager.begin();
            UndoTestWrites.update(h.undoManager, transaction, write, TABLE_ID, INDEX_ID, key(30), key(30),
                    new HiddenColumns(transaction.transactionId(), RollPointer.NULL), keyDef(), schema());
            h.mtrManager.commit(write);
            PageId updatePage = transaction.undoContext().binding(UndoLogKind.UPDATE).firstPageId();

            h.commit(transaction);
            HistoryEntry entry = h.history.peekCommitted().orElseThrow();
            h.consumeLogicalHead(entry);
            try (HistoryList.HeadRemovalLease lease = h.history.beginHeadRemoval(entry)) {
                h.finalization.finalizer().finalizePurgedHistory(entry, lease);
            }

            RollbackSegmentHeaderSnapshot snapshot = h.snapshot();
            assertTrue(snapshot.cachedInsertSegments().isEmpty());
            assertEquals(List.of(updatePage), snapshot.cachedUpdateSegments());
            UndoHistoryNodeSnapshot cachedNode = h.historyNode(updatePage);
            assertTrue(cachedNode.isCached());
            assertTrue(cachedNode.previousHistoryPageId().isEmpty());
            assertTrue(cachedNode.nextHistoryPageId().isEmpty());

            Transaction reused = h.newWriter();
            TableSchema wideSchema = new TableSchema(1, List.of(
                    new ColumnDef(new ColumnId(0), "id", ColumnType.intType(false, false), 0),
                    new ColumnDef(new ColumnId(1), "payload", ColumnType.varchar(20_000, true), 1)), true);
            List<ColumnValue> wideOldRow = List.of(new ColumnValue.IntValue(31),
                    new ColumnValue.StringValue("cache-grow".repeat(1_800)));
            UndoWritePlan plan = h.undoManager.planUpdate(reused, TABLE_ID, INDEX_ID, key(31), wideOldRow,
                    new HiddenColumns(reused.transactionId(), RollPointer.NULL), keyDef(), wideSchema);
            assertEquals(UndoSegmentAcquisition.REUSE_CACHED, plan.acquisition());
            assertEquals(updatePage, plan.expectedFirstPageId());
            assertTrue(plan.external(), "cached reuse 仍必须按计划预留 external payload 页");
            MiniTransaction append = h.mtrManager.begin();
            h.undoManager.appendPlanned(reused, append, plan);
            h.mtrManager.commit(append);
            assertEquals(updatePage, reused.undoContext().binding(UndoLogKind.UPDATE).firstPageId());

            h.commit(reused);
            HistoryEntry grown = h.history.peekCommitted().orElseThrow();
            h.consumeLogicalHead(grown);
            try (HistoryList.HeadRemovalLease lease = h.history.beginHeadRemoval(grown)) {
                h.finalization.finalizer().finalizePurgedHistory(grown, lease);
            }
            assertTrue(h.snapshot().cachedUpdateSegments().isEmpty(),
                    "复用后 grow/external 的多页段在下一次终结必须 drop，不能缩回 cache");
        });
    }

    private void withHarness(int cacheCapacity, TestBody body) {
        PageStore store = new FileChannelPageStore();
        try (PageStore ignored = store; BufferPool pool = new LruBufferPool(store, PAGE_SIZE, 128)) {
            MiniTransactionManager manager = new MiniTransactionManager();
            DiskSpaceManager disk = new DiskSpaceManager(pool, store, PAGE_SIZE);
            DiskSpaceUndoAllocator allocator = new DiskSpaceUndoAllocator(disk);
            UndoLogSegmentAccess access = new UndoLogSegmentAccess(
                    pool, PAGE_SIZE, allocator, new TypeCodecRegistry());
            RollbackSegmentSlotManager slots = new RollbackSegmentSlotManager(RollbackSegmentId.of(0), 16);
            UndoFinalizationTestSupport.Components finalization = UndoFinalizationTestSupport.create(
                    manager, pool, PAGE_SIZE, access, allocator, slots, cacheCapacity);
            HistoryList history = new HistoryList();
            UndoLogManager undoManager = finalization.manager(access, UNDO_SPACE, history, manager);
            TransactionManager transactions = new TransactionManager(new TransactionSystem());

            MiniTransaction boot = manager.begin();
            disk.createTablespace(boot, UNDO_SPACE, dir.resolve("undo-cache-" + cacheCapacity + ".ibu"),
                    PageNo.of(64));
            finalization.format(boot, UNDO_SPACE);
            manager.commit(boot);

            body.run(new Harness(manager, allocator, access, finalization, history, undoManager, transactions));
        }
    }

    private static TableSchema schema() {
        return new TableSchema(1, List.of(
                new ColumnDef(new ColumnId(0), "id", ColumnType.intType(false, false), 0)), true);
    }

    private static IndexKeyDef keyDef() {
        return new IndexKeyDef(INDEX_ID,
                List.of(new KeyPartDef(new ColumnId(0), KeyOrder.ASC, 0)));
    }

    private static List<ColumnValue> key(long id) {
        return List.of(new ColumnValue.IntValue(id));
    }

    @FunctionalInterface
    private interface TestBody {
        void run(Harness harness);
    }

    private record Harness(MiniTransactionManager mtrManager,
                           DiskSpaceUndoAllocator allocator,
                           UndoLogSegmentAccess undoAccess,
                           UndoFinalizationTestSupport.Components finalization,
                           HistoryList history,
                           UndoLogManager undoManager,
                           TransactionManager transactionManager) {

        Transaction newWriter() {
            Transaction transaction = transactionManager.begin(TransactionOptions.defaults());
            transactionManager.assignWriteId(transaction);
            return transaction;
        }

        PageId appendInsert(Transaction transaction, long key) {
            appendInsertRecord(transaction, key);
            return transaction.undoContext().binding(UndoLogKind.INSERT).firstPageId();
        }

        void appendInsertRecord(Transaction transaction, long key) {
            MiniTransaction mtr = mtrManager.begin();
            UndoTestWrites.insert(undoManager, transaction, mtr, TABLE_ID, INDEX_ID,
                    UndoSegmentCacheIntegrationTest.key(key), keyDef(), schema());
            mtrManager.commit(mtr);
        }

        UndoSegmentHandle handle(Transaction transaction, UndoLogKind kind) {
            MiniTransaction read = mtrManager.beginReadOnly();
            UndoSegmentHandle handle = undoAccess.open(
                    read, transaction.undoContext().binding(kind).firstPageId(), PageLatchMode.SHARED).handle();
            mtrManager.commit(read);
            return handle;
        }

        void commit(Transaction transaction) {
            transactionManager.prepareCommit(transaction);
            undoManager.onCommit(transaction);
            transactionManager.commit(transaction);
        }

        RollbackSegmentHeaderSnapshot snapshot() {
            MiniTransaction read = mtrManager.beginReadOnly();
            RollbackSegmentHeaderSnapshot snapshot = finalization.header().read(
                    read, UNDO_SPACE, finalization.slots().rollbackSegmentId(),
                    finalization.slots().slotCapacity(), finalization.cache().capacityPerKind());
            mtrManager.commit(read);
            return snapshot;
        }

        UndoHistoryNodeSnapshot historyNode(PageId firstPageId) {
            MiniTransaction read = mtrManager.beginReadOnly();
            UndoHistoryNodeSnapshot snapshot = undoAccess.inspectHistoryNode(read, firstPageId);
            mtrManager.commit(read);
            return snapshot;
        }

        /** 本类只测 finalization/cache；模拟记录级 purge 已把持久 logical head 消费为空。 */
        void consumeLogicalHead(HistoryEntry entry) {
            MiniTransaction progress = mtrManager.begin();
            UndoLogSegment segment = undoAccess.open(
                    progress, entry.undoFirstPageId(), PageLatchMode.EXCLUSIVE);
            segment.updateLogicalHead(segment.logicalHead(), UndoLogicalHead.EMPTY, keyDef(), schema());
            mtrManager.commit(progress);
        }
    }
}
