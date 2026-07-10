package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.domain.RollbackSegmentId;
import cn.zhangyis.db.domain.SegmentId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.TransactionNo;
import cn.zhangyis.db.domain.UndoNo;
import cn.zhangyis.db.domain.UndoSlotId;
import cn.zhangyis.db.storage.api.DiskSpaceManager;
import cn.zhangyis.db.storage.api.DiskSpaceUndoAllocator;
import cn.zhangyis.db.storage.api.index.IndexPageAccess;
import cn.zhangyis.db.storage.api.SegmentRef;
import cn.zhangyis.db.storage.btree.BTreeIndex;
import cn.zhangyis.db.storage.btree.BTreeLookupResult;
import cn.zhangyis.db.storage.btree.SplitCapableBTreeIndexService;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.fil.io.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.fil.access.TablespaceAccessController;
import cn.zhangyis.db.storage.flush.FlushCoordinator;
import cn.zhangyis.db.storage.flush.doublewrite.NoDoublewriteStrategy;
import cn.zhangyis.db.storage.fsp.segment.SegmentPurpose;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import cn.zhangyis.db.storage.record.format.HiddenColumns;
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
import cn.zhangyis.db.storage.redo.RedoLogFileRepository;
import cn.zhangyis.db.storage.redo.RedoLogManager;
import cn.zhangyis.db.storage.redo.FspMetadataDeltaKind;
import cn.zhangyis.db.storage.redo.FspMetadataDeltaRecord;
import cn.zhangyis.db.storage.undo.UndoLogSegment;
import cn.zhangyis.db.storage.undo.UndoLogSegmentAccess;
import cn.zhangyis.db.storage.undo.UndoRecord;
import cn.zhangyis.db.storage.undo.UndoSegmentHandle;
import cn.zhangyis.db.storage.undo.UndoSpaceAllocator;
import cn.zhangyis.db.storage.undo.UndoSpaceReservation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * P6 PurgeCoordinator 端到端：单线程聚簇 purge + undo 段回收。整栈 test-wired
 * （assignWriteId → beforeX → Xclustered → commit+onCommit → runBatch）。覆盖：无 live ReadView 物理移除 delete-marked +
 * 回收；live ReadView 挡住、release 后放行；UPDATE-only 只回收段不碰记录（#9）；insert-only 在 commit
 * 原子终结；per-entry 原子（drop 失败保留 history head，#8）。
 */
class PurgeCoordinatorTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId DATA_SPACE = SpaceId.of(41);
    private static final SpaceId UNDO_SPACE = SpaceId.of(77);
    private static final long INDEX_ID = 9L;
    private static final long TABLE_ID = 1L;

    @TempDir
    Path dir;

    private final TypeCodecRegistry registry = new TypeCodecRegistry();

    @Test
    void purgesCommittedDeleteWhenNoLiveReadView() {
        onPool(false, ctx -> {
            ctx.boot();
            BTreeIndex index = ctx.clusteredIndex();
            Transaction t1 = ctx.beginRw();
            ctx.insert(index, t1, 1);
            ctx.commit(t1);
            Transaction t2 = ctx.beginRw();
            ctx.deleteMark(index, t2, 1);
            ctx.commit(t2);
            assertEquals(1, ctx.history.committedSize(), "delete txn enqueued to history");

            PurgeSummary summary = ctx.purge.runBatch(10);

            assertEquals(1, summary.purgedLogs());
            assertEquals(1, summary.removedRecords(), "delete-marked row physically removed");
            assertEquals(0, ctx.history.committedSize(), "history drained");
            MiniTransaction r = ctx.mgr.begin();
            assertTrue(ctx.svc.lookupIncludingDeleted(r, index, search(1)).isEmpty(), "row gone after purge");
            ctx.mgr.commit(r);
        });
    }

    @Test
    void liveReadViewBlocksPurgeThenReleaseAllows() {
        onPool(false, ctx -> {
            ctx.boot();
            BTreeIndex index = ctx.clusteredIndex();
            Transaction t1 = ctx.beginRw();
            ctx.insert(index, t1, 1);
            ctx.commit(t1);

            // 在 delete 提交前打开 RR 快照：该快照看不到后续 delete，purge 必须保留 delete undo
            Transaction reader = ctx.txnMgr.begin(new TransactionOptions(IsolationLevel.REPEATABLE_READ, true, true));
            ctx.txnMgr.readViewManager().openReadView(reader);

            Transaction t2 = ctx.beginRw();
            ctx.deleteMark(index, t2, 1);
            ctx.commit(t2);

            PurgeSummary blocked = ctx.purge.runBatch(10);
            assertEquals(0, blocked.purgedLogs(), "live older ReadView blocks delete purge");
            assertEquals(1, ctx.history.committedSize(), "delete log retained while reader live");
            MiniTransaction chk = ctx.mgr.begin();
            assertTrue(ctx.svc.lookupIncludingDeleted(chk, index, search(1)).isPresent(), "row still present (marked)");
            ctx.mgr.commit(chk);

            ctx.txnMgr.readViewManager().release(reader); // 注销快照 → 边界推进

            PurgeSummary after = ctx.purge.runBatch(10);
            assertEquals(1, after.purgedLogs());
            assertEquals(1, after.removedRecords());
            MiniTransaction r = ctx.mgr.begin();
            assertTrue(ctx.svc.lookupIncludingDeleted(r, index, search(1)).isEmpty(), "purged after reader released");
            ctx.mgr.commit(r);
        });
    }

    @Test
    void updateOnlyReclaimsSegmentWithoutTouchingRecord() {
        onPool(false, ctx -> {
            ctx.boot();
            BTreeIndex index = ctx.clusteredIndex();
            Transaction t1 = ctx.beginRw();
            ctx.insert(index, t1, 1);
            ctx.commit(t1);
            Transaction t2 = ctx.beginRw();
            ctx.update(index, t2, 1, "v2");
            ctx.commit(t2);

            PurgeSummary summary = ctx.purge.runBatch(10);

            assertEquals(1, summary.purgedLogs(), "update undo log reclaimed");
            assertEquals(0, summary.removedRecords(), "UPDATE-only purge does not remove any clustered record");
            assertEquals(0, ctx.history.committedSize());
            MiniTransaction r = ctx.mgr.begin();
            BTreeLookupResult found = ctx.svc.lookup(r, index, search(1)).orElseThrow();
            ctx.mgr.commit(r);
            assertEquals("v2", payloadOf(found), "current version untouched by purge");
        });
    }

    /**
     * 部分回滚后的物理分支不再属于 committed logical chain。即使该废弃槽随后损坏，purge 也应只沿持久头处理
     * 当前链并成功回收 segment；按物理槽扫描会在解码废弃 DELETE_MARK 时错误失败。
     */
    @Test
    void purgeSkipsCorruptPhysicalBranchDetachedByPartialRollback() {
        onPool(false, ctx -> {
            ctx.boot();
            BTreeIndex index = ctx.clusteredIndex();
            Transaction creator = ctx.beginRw();
            ctx.insert(index, creator, 1);
            ctx.commit(creator);

            Transaction txn = ctx.beginRw();
            ctx.update(index, txn, 1, "v2");
            TransactionSavepoint savepoint = ctx.rollbackService.createSavepoint(txn);
            RollPointer detached = ctx.deleteMark(index, txn, 1);
            ctx.rollbackService.rollbackToSavepoint(txn, index, savepoint);
            ctx.corruptDetachedUndoType(detached);
            ctx.commit(txn);

            PurgeSummary summary = ctx.purge.runBatch(10);

            assertEquals(1, summary.purgedLogs());
            assertEquals(0, summary.removedRecords(),
                    "rolled-back DELETE_MARK branch must not become a purge task");
            MiniTransaction read = ctx.mgr.begin();
            BTreeLookupResult row = ctx.svc.lookup(read, index, search(1)).orElseThrow();
            ctx.mgr.commit(read);
            assertEquals("v2", payloadOf(row));
        });
    }

    /** 物理链页数超过小池容量时，purge 仍应逐 pointer 短读；旧单 MTR FIL 扫描会累计 fix 后耗尽池。 */
    @Test
    void purgeLogicalTraversalSurvivesSmallBufferPoolReopen() {
        Path dataPath = dir.resolve("purge-small-pool-data.ibd");
        Path undoPath = dir.resolve("purge-small-pool-undo.ibu");
        MultiPagePurgeFixture fixture = buildMultiPagePurgeFixture(dataPath, undoPath);

        try (PageStore store = new FileChannelPageStore();
             BufferPool pool = new LruBufferPool(store, PS, 4)) {
            MiniTransactionManager mgr = new MiniTransactionManager();
            DiskSpaceManager disk = new DiskSpaceManager(pool, store, PS);
            disk.openTablespace(DATA_SPACE, dataPath);
            disk.openTablespace(UNDO_SPACE, undoPath);
            UndoSpaceAllocator allocator = new DiskSpaceUndoAllocator(disk);
            UndoLogSegmentAccess undoAccess = new UndoLogSegmentAccess(pool, PS, allocator, registry);
            RollbackSegmentSlotManager slots = new RollbackSegmentSlotManager(RollbackSegmentId.of(0), 64);
            slots.restore(fixture.slotId(), fixture.firstPageId());
            UndoFinalizationTestSupport.Components finalization = UndoFinalizationTestSupport.create(
                    mgr, pool, PS, undoAccess, allocator, slots);
            HistoryList history = new HistoryList();
            history.submitCommitted(new HistoryEntry(TransactionNo.of(1), TransactionId.of(7),
                    UNDO_SPACE, fixture.firstPageId(), fixture.slotId()));
            TransactionSystem system = new TransactionSystem();
            system.restoreCounters(8, 2);
            PurgeCoordinator purge = new PurgeCoordinator(mgr, system, history, undoAccess,
                    finalization.finalizer(), new SplitCapableBTreeIndexService(
                            new IndexPageAccess(pool, PS), disk, registry), fixture.index());

            PurgeSummary summary = purge.runBatch(1);

            assertEquals(1, summary.purgedLogs());
            assertEquals(0, summary.removedRecords());
            assertEquals(0, history.committedSize());
            assertTrue(mgr.redoLogManager().bufferedBatches().getLast().records().stream()
                            .anyMatch(record -> record instanceof FspMetadataDeltaRecord delta
                                    && delta.kind() == FspMetadataDeltaKind.XDES_FIELD),
                    "finalization batch must release the segment-owned extent, not only fragment pages");
        }
    }

    @Test
    void insertOnlyUndoReclaimedImmediately() {
        onPool(false, ctx -> {
            ctx.boot();
            BTreeIndex index = ctx.clusteredIndex();
            Transaction t1 = ctx.beginRw();
            ctx.insert(index, t1, 1);
            ctx.commit(t1);
            assertEquals(0, ctx.history.committedSize());
            assertEquals(0, ctx.slots.activeSlotCount(), "insert-only commit atomically releases its undo slot");

            PurgeSummary summary = ctx.purge.runBatch(10);

            assertEquals(0, summary.purgedLogs(), "insert-only undo never enters purge history");
            assertEquals(0, summary.removedRecords());
            MiniTransaction r = ctx.mgr.begin();
            assertTrue(ctx.svc.lookup(r, index, search(1)).isPresent(), "committed inserted row stays (only undo reclaimed)");
            ctx.mgr.commit(r);
        });
    }

    /** insert-only commit 的 drop 失败发生在提交发布前；事务与 slot 保持 ACTIVE/占用，不能伪装提交成功。 */
    @Test
    void insertCommitFailsClosedWhenAtomicFinalizationDropFails() {
        onPool(true, ctx -> {
            ctx.boot();
            BTreeIndex index = ctx.clusteredIndex();
            Transaction txn = ctx.beginRw();
            ctx.insert(index, txn, 1);

            assertThrows(UndoFinalizationException.class, () -> ctx.commit(txn));

            assertEquals(TransactionState.ACTIVE, txn.state(), "commit must not publish after finalization failure");
            assertEquals(1, ctx.slots.activeSlotCount(), "owner remains available for crash recovery");
            assertEquals(0, ctx.history.committedSize(), "insert-only undo has no deferred reclaim queue");
        });
    }

    @Test
    void perEntryAtomicityKeepsHistoryHeadOnDropFailure() {
        onPool(true, ctx -> { // failing dropUndoSegment
            ctx.boot();
            BTreeIndex index = ctx.clusteredIndex();
            // 单事务 insert+delete-mark → 提交为含 delete undo 的 committed log（无独立 insert-reclaim）
            Transaction t1 = ctx.beginRw();
            ctx.insert(index, t1, 1);
            ctx.deleteMark(index, t1, 1);
            ctx.commit(t1);
            assertEquals(1, ctx.history.committedSize());

            // drop 抛错：异常传播、history head 不被 poll
            assertThrows(DatabaseRuntimeException.class, () -> ctx.purge.runBatch(10));
            assertEquals(1, ctx.history.committedSize(), "history head retained on hard failure (per-entry atomic)");
            assertEquals(1, ((FailingDropAllocator) ctx.undoAllocator).dropAttempts());

            assertThrows(DatabaseRuntimeException.class, () -> ctx.purge.runBatch(10));
            assertEquals(1, ((FailingDropAllocator) ctx.undoAllocator).dropAttempts(),
                    "physical failure keeps FINALIZING, so retry cannot invoke allocator again");
        });
    }

    /** finalization commit 后、history 内存摘除前 crash：page3 已清，重启不会从残留 Java 队列重建该条目。 */
    @Test
    void purgeCrashAfterFinalizationCommitLeavesOnlyStaleMemoryProjection() {
        AtomicBoolean injected = new AtomicBoolean();
        onPool(false, (kind, slotId, firstPageId) -> {
            if (kind == UndoFinalizationKind.PURGE && injected.compareAndSet(false, true)) {
                throw new SimulatedPurgeCrashException("crash after purge finalization commit");
            }
        }, ctx -> {
            ctx.boot();
            BTreeIndex index = ctx.clusteredIndex();
            Transaction creator = ctx.beginRw();
            ctx.insert(index, creator, 1);
            ctx.commit(creator);
            Transaction updater = ctx.beginRw();
            ctx.update(index, updater, 1, "v2");
            ctx.commit(updater);
            HistoryEntry head = ctx.history.peekCommitted().orElseThrow();

            assertThrows(SimulatedPurgeCrashException.class, () -> ctx.purge.runBatch(1));

            assertEquals(1, ctx.history.committedSize(),
                    "old process has not published history completion at the crash point");
            assertTrue(ctx.slots.isOccupied(head.slotId()),
                    "old process also retains its stale memory slot projection");
            MiniTransaction read = ctx.mgr.begin();
            var snapshot = ctx.finalization.header().read(read, UNDO_SPACE,
                    ctx.slots.rollbackSegmentId(), ctx.slots.slotCapacity());
            ctx.mgr.commit(read);
            assertFalse(snapshot.occupiedSlots().containsKey(head.slotId()),
                    "page3 recovery authority was cleared by the committed finalization batch");
        });
    }

    // ---- helpers ----

    private static String payloadOf(BTreeLookupResult r) {
        return ((ColumnValue.StringValue) r.record().columnValues().get(1)).value();
    }

    private static TableSchema clusteredSchema() {
        return new TableSchema(1, List.of(
                new ColumnDef(new ColumnId(0), "id", ColumnType.intType(false, false), 0),
                new ColumnDef(new ColumnId(1), "payload", ColumnType.varchar(200, true), 1)), true);
    }

    private static IndexKeyDef idKey() {
        return new IndexKeyDef(INDEX_ID, List.of(new KeyPartDef(new ColumnId(0), KeyOrder.ASC, 0)));
    }

    private static List<ColumnValue> key(long id) {
        return List.of(new ColumnValue.IntValue(id));
    }

    private static SearchKey search(long id) {
        return new SearchKey(List.of(new ColumnValue.IntValue(id)));
    }

    private static LogicalRecord row(long id) {
        return new LogicalRecord(1, List.of(new ColumnValue.IntValue(id),
                new ColumnValue.StringValue("payload-" + id)), false, RecordType.CONVENTIONAL);
    }

    /** 构建 70 条约 7KiB UPDATE undo（超过 32 个 fragment 页并进入 extent）供 4-frame reopen 验证终结资源边界。 */
    private MultiPagePurgeFixture buildMultiPagePurgeFixture(Path dataPath, Path undoPath) {
        Path redoPath = dir.resolve("purge-small-pool-redo.log");
        try (PageStore store = new FileChannelPageStore();
             BufferPool pool = new LruBufferPool(store, PS, 64);
             RedoLogFileRepository repo = RedoLogFileRepository.open(redoPath)) {
            RedoLogManager redo = RedoLogManager.durable(repo);
            MiniTransactionManager mgr = new MiniTransactionManager(new TablespaceAccessController(), redo);
            DiskSpaceManager disk = new DiskSpaceManager(pool, store, PS);
            IndexPageAccess indexAccess = new IndexPageAccess(pool, PS);
            UndoSpaceAllocator allocator = new DiskSpaceUndoAllocator(disk);
            UndoLogSegmentAccess undoAccess = new UndoLogSegmentAccess(pool, PS, allocator, registry);
            RollbackSegmentSlotManager slots = new RollbackSegmentSlotManager(RollbackSegmentId.of(0), 64);
            UndoFinalizationTestSupport.Components finalization = UndoFinalizationTestSupport.create(
                    mgr, pool, PS, undoAccess, allocator, slots);

            MiniTransaction boot = mgr.begin();
            disk.createTablespace(boot, DATA_SPACE, dataPath, PageNo.of(64));
            SegmentRef leaf = disk.createSegment(boot, DATA_SPACE, SegmentPurpose.INDEX_LEAF);
            SegmentRef nonLeaf = disk.createSegment(boot, DATA_SPACE, SegmentPurpose.INDEX_NON_LEAF);
            PageId root = disk.allocatePage(boot, leaf);
            indexAccess.createIndexPage(boot, root, INDEX_ID, 0);
            disk.createTablespace(boot, UNDO_SPACE, undoPath, PageNo.of(64));
            finalization.format(boot, UNDO_SPACE);
            mgr.commit(boot);

            TableSchema schema = largePurgeSchema();
            BTreeIndex index = new BTreeIndex(INDEX_ID, root, 0, idKey(), schema, true, leaf, nonLeaf);
            MiniTransaction create = mgr.begin();
            UndoLogSegment segment = undoAccess.create(create, UNDO_SPACE, TransactionId.of(7));
            PageId firstPageId = segment.firstPageId();
            UndoSlotId slotId = slots.claim(firstPageId);
            try (var ignored = create.allowOutOfOrderPageLatch(
                    "test fixture persists rseg slot after creating undo first page")) {
                finalization.header().claimSlot(create, UNDO_SPACE, slotId, firstPageId);
            }
            RollPointer previous = RollPointer.NULL;
            String largeValue = "x".repeat(7_000);
            for (int i = 1; i <= 70; i++) {
                if (i > 1) {
                    mgr.commit(create);
                    create = mgr.begin();
                    segment = undoAccess.open(create, firstPageId, PageLatchMode.EXCLUSIVE);
                }
                UndoRecord record = UndoRecord.update(UndoNo.of(i), TransactionId.of(7), TABLE_ID, INDEX_ID,
                        key(i), List.of(new ColumnValue.IntValue(i), new ColumnValue.StringValue(largeValue)),
                        new HiddenColumns(TransactionId.of(3), RollPointer.NULL), previous);
                previous = segment.append(record, index.keyDef(), index.schema());
            }
            mgr.commit(create);
            MiniTransaction committed = mgr.begin();
            undoAccess.open(committed, firstPageId, PageLatchMode.EXCLUSIVE)
                    .markCommitted(TransactionNo.of(1));
            mgr.commit(committed);

            redo.flush();
            new FlushCoordinator(pool, store, redo, PS, new NoDoublewriteStrategy(), Duration.ofMillis(100))
                    .flushList(Lsn.of(Long.MAX_VALUE), pool.capacity());
            return new MultiPagePurgeFixture(index, firstPageId, slotId);
        }
    }

    private static TableSchema largePurgeSchema() {
        return new TableSchema(1, List.of(
                new ColumnDef(new ColumnId(0), "id", ColumnType.intType(false, false), 0),
                new ColumnDef(new ColumnId(1), "payload", ColumnType.varchar(8_000, true), 1)), true);
    }

    private record MultiPagePurgeFixture(BTreeIndex index, PageId firstPageId, UndoSlotId slotId) {
    }

    private void onPool(boolean failDrop, Body body) {
        onPool(failDrop, UndoFinalizationFaultInjector.none(), body);
    }

    private void onPool(boolean failDrop, UndoFinalizationFaultInjector faultInjector, Body body) {
        PageStore store = new FileChannelPageStore();
        try (PageStore ignored = store; BufferPool pool = new LruBufferPool(store, PS, 128)) {
            body.run(new Ctx(store, pool, failDrop, faultInjector));
        }
    }

    private interface Body {
        void run(Ctx ctx);
    }

    /** 仅在 dropUndoSegment 抛错的分配器包装，用于 per-entry 原子测试；create/allocate 透传真实分配器。 */
    private static final class FailingDropAllocator implements UndoSpaceAllocator {
        private final UndoSpaceAllocator delegate;
        /** 记录是否有重复终结越过运行期 lease 到达物理 allocator。 */
        private final AtomicInteger dropAttempts = new AtomicInteger();

        private FailingDropAllocator(UndoSpaceAllocator delegate) {
            this.delegate = delegate;
        }

        @Override
        public UndoSegmentHandle createUndoSegment(MiniTransaction mtr, SpaceId undoSpace) {
            return delegate.createUndoSegment(mtr, undoSpace);
        }

        @Override
        public UndoSpaceReservation reserveGrowPages(MiniTransaction mtr, SpaceId undoSpace, long pages) {
            return delegate.reserveGrowPages(mtr, undoSpace, pages);
        }

        @Override
        public PageId allocatePage(MiniTransaction mtr, SpaceId undoSpace, int inodeSlot, SegmentId segmentId) {
            return delegate.allocatePage(mtr, undoSpace, inodeSlot, segmentId);
        }

        @Override
        public void dropUndoSegment(MiniTransaction mtr, UndoSegmentHandle handle) {
            dropAttempts.incrementAndGet();
            throw new DatabaseRuntimeException("injected dropUndoSegment failure");
        }

        private int dropAttempts() {
            return dropAttempts.get();
        }
    }

    private final class Ctx {
        final MiniTransactionManager mgr = new MiniTransactionManager();
        final DiskSpaceManager disk;
        final IndexPageAccess access;
        final UndoSpaceAllocator undoAllocator;
        final UndoLogSegmentAccess undoAccess;
        final RollbackSegmentSlotManager slots;
        final UndoFinalizationTestSupport.Components finalization;
        final HistoryList history = new HistoryList();
        final TransactionManager txnMgr = new TransactionManager(new TransactionSystem());
        final UndoLogManager undoMgr;
        final SplitCapableBTreeIndexService svc;
        final RollbackService rollbackService;
        final BufferPool pool;
        PurgeCoordinator purge;
        private SegmentRef leafSegment;
        private SegmentRef nonLeafSegment;
        private PageId rootPageId;

        private Ctx(PageStore store, BufferPool pool, boolean failDrop,
                    UndoFinalizationFaultInjector faultInjector) {
            this.pool = pool;
            this.disk = new DiskSpaceManager(pool, store, PS);
            this.access = new IndexPageAccess(pool, PS);
            UndoSpaceAllocator real = new DiskSpaceUndoAllocator(disk);
            this.undoAllocator = failDrop ? new FailingDropAllocator(real) : real;
            this.undoAccess = new UndoLogSegmentAccess(pool, PS, undoAllocator, registry);
            this.slots = new RollbackSegmentSlotManager(RollbackSegmentId.of(0), 64);
            this.finalization = UndoFinalizationTestSupport.create(
                    mgr, pool, PS, undoAccess, undoAllocator, slots, faultInjector);
            this.undoMgr = finalization.manager(undoAccess, UNDO_SPACE, history, mgr);
            this.svc = new SplitCapableBTreeIndexService(access, disk, registry);
            this.rollbackService = new RollbackService(
                    svc, undoAccess, txnMgr, mgr, finalization.finalizer());
        }

        private void boot() {
            MiniTransaction b = mgr.begin();
            disk.createTablespace(b, DATA_SPACE, dir.resolve("data.ibd"), PageNo.of(64));
            leafSegment = disk.createSegment(b, DATA_SPACE, SegmentPurpose.INDEX_LEAF);
            nonLeafSegment = disk.createSegment(b, DATA_SPACE, SegmentPurpose.INDEX_NON_LEAF);
            rootPageId = disk.allocatePage(b, leafSegment);
            access.createIndexPage(b, rootPageId, INDEX_ID, 0);
            disk.createTablespace(b, UNDO_SPACE, dir.resolve("undo.ibu"), PageNo.of(64));
            finalization.format(b, UNDO_SPACE);
            mgr.commit(b);
            this.purge = new PurgeCoordinator(mgr, txnMgr.system(), history, undoAccess,
                    finalization.finalizer(), svc, clusteredIndex());
        }

        private BTreeIndex clusteredIndex() {
            return new BTreeIndex(INDEX_ID, rootPageId, 0, idKey(), clusteredSchema(), true,
                    leafSegment, nonLeafSegment);
        }

        private Transaction beginRw() {
            Transaction txn = txnMgr.begin(TransactionOptions.defaults());
            txnMgr.assignWriteId(txn);
            return txn;
        }

        private void insert(BTreeIndex index, Transaction txn, long id) {
            MiniTransaction m = mgr.begin();
            RollPointer rp = undoMgr.beforeInsert(txn, m, TABLE_ID, INDEX_ID, key(id), index.keyDef(), index.schema());
            svc.insertClustered(m, index, row(id), txn.transactionId(), rp);
            mgr.commit(m);
        }

        private RollPointer deleteMark(BTreeIndex index, Transaction txn, long id) {
            MiniTransaction read = mgr.begin();
            BTreeLookupResult old = svc.lookup(read, index, search(id)).orElseThrow();
            mgr.commit(read);
            HiddenColumns oldHidden = old.record().hiddenColumns();
            MiniTransaction m = mgr.begin();
            RollPointer delRp = undoMgr.beforeDelete(txn, m, TABLE_ID, INDEX_ID, key(id),
                    old.record().columnValues(), oldHidden, index.keyDef(), index.schema());
            svc.setClusteredDeleteMark(m, index, search(id), true,
                    new HiddenColumns(txn.transactionId(), delRp), oldHidden.dbTrxId(), oldHidden.dbRollPtr());
            mgr.commit(m);
            return delRp;
        }

        private RollPointer update(BTreeIndex index, Transaction txn, long id, String payload) {
            MiniTransaction read = mgr.begin();
            BTreeLookupResult old = svc.lookup(read, index, search(id)).orElseThrow();
            mgr.commit(read);
            HiddenColumns oldHidden = old.record().hiddenColumns();
            MiniTransaction m = mgr.begin();
            RollPointer newRp = undoMgr.beforeUpdate(txn, m, TABLE_ID, INDEX_ID, key(id),
                    old.record().columnValues(), oldHidden, index.keyDef(), index.schema());
            svc.replaceClustered(m, index, search(id),
                    new LogicalRecord(1, List.of(new ColumnValue.IntValue(id), new ColumnValue.StringValue(payload)),
                            false, RecordType.CONVENTIONAL, new HiddenColumns(txn.transactionId(), newRp)),
                    oldHidden.dbTrxId(), oldHidden.dbRollPtr());
            mgr.commit(m);
            return newRp;
        }

        /** 只破坏已被持久 logical head 摘除的槽，验证 purge 不再读取无效物理分支。 */
        private void corruptDetachedUndoType(RollPointer detached) {
            MiniTransaction corrupt = mgr.begin();
            corrupt.getPage(pool, PageId.of(UNDO_SPACE, detached.pageNo()), PageLatchMode.EXCLUSIVE)
                    .writeBytes(detached.offset() + Short.BYTES, new byte[]{(byte) 0x7F});
            mgr.commit(corrupt);
        }

        /** commit 编排：先预留提交号，再完成 undo 终态，最后才向事务系统发布 COMMITTED。 */
        private void commit(Transaction txn) {
            txnMgr.prepareCommit(txn);
            undoMgr.onCommit(txn);
            txnMgr.commit(txn);
        }
    }

    /** 只模拟 finalization 已提交后进程消失；生产 purge 不安装该 hook。 */
    private static final class SimulatedPurgeCrashException extends DatabaseRuntimeException {
        private SimulatedPurgeCrashException(String message) {
            super(message);
        }
    }
}
