package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.domain.RollbackSegmentId;
import cn.zhangyis.db.domain.SegmentId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.UndoNo;
import cn.zhangyis.db.domain.UndoSlotId;
import cn.zhangyis.db.storage.api.DiskSpaceManager;
import cn.zhangyis.db.storage.api.DiskSpaceUndoAllocator;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.fil.access.TablespaceAccessController;
import cn.zhangyis.db.storage.fil.io.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.flush.FlushCoordinator;
import cn.zhangyis.db.storage.flush.doublewrite.NoDoublewriteStrategy;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import cn.zhangyis.db.storage.record.schema.ColumnDef;
import cn.zhangyis.db.storage.record.schema.ColumnId;
import cn.zhangyis.db.storage.record.schema.ColumnType;
import cn.zhangyis.db.storage.record.schema.IndexKeyDef;
import cn.zhangyis.db.storage.record.schema.KeyOrder;
import cn.zhangyis.db.storage.record.schema.KeyPartDef;
import cn.zhangyis.db.storage.record.schema.TableSchema;
import cn.zhangyis.db.storage.record.format.HiddenColumns;
import cn.zhangyis.db.storage.record.type.ColumnValue;
import cn.zhangyis.db.storage.record.type.TypeCodecRegistry;
import cn.zhangyis.db.storage.undo.UndoLogSegment;
import cn.zhangyis.db.storage.undo.UndoLogSegmentAccess;
import cn.zhangyis.db.storage.undo.UndoLogFormatException;
import cn.zhangyis.db.storage.undo.UndoPageOverflowException;
import cn.zhangyis.db.storage.undo.UndoRecord;
import cn.zhangyis.db.storage.undo.UndoRecordType;
import cn.zhangyis.db.storage.undo.UndoSlotOwnershipConflictException;
import cn.zhangyis.db.storage.undo.UndoSegmentHandle;
import cn.zhangyis.db.storage.undo.UndoSpaceAllocator;
import cn.zhangyis.db.storage.undo.UndoSpaceReservation;
import cn.zhangyis.db.storage.redo.RedoRecord;
import cn.zhangyis.db.storage.redo.RedoLogFileRepository;
import cn.zhangyis.db.storage.redo.RedoLogManager;
import cn.zhangyis.db.storage.redo.FspMetadataDeltaRecord;
import cn.zhangyis.db.storage.redo.FspPageFreeRecord;
import cn.zhangyis.db.storage.redo.RedoLogBatch;
import cn.zhangyis.db.storage.redo.TransactionStateDeltaReason;
import cn.zhangyis.db.storage.redo.TransactionStateDeltaRecord;
import cn.zhangyis.db.storage.redo.TransactionStateDeltaState;
import cn.zhangyis.db.storage.redo.UndoMetadataDeltaKind;
import cn.zhangyis.db.storage.redo.UndoMetadataDeltaRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * T1.3c UndoLogManager 事务 undo 写路径接线。onPool harness 复用 T1.3b 注入（FileChannelPageStore +
 * LruBufferPool + DiskSpaceManager + DiskSpaceUndoAllocator + UndoLogSegmentAccess）。覆盖：首写建段 + 占内存
 * slot + 返回非 NULL 的 insert DB_ROLL_PTR；同 txn 多 insert undoNo 递增 + prevRollPointer 串链 + readRecord
 * 回值；内存 rseg slot 落该段首页；commit + 新 PageStore/BufferPool reload 后按 DB_ROLL_PTR 读回 undo record
 * 等值原 clusterKey（读回依赖 roll pointer + undo first page，不依赖持久 rseg header）。
 *
 * <p><b>非目标</b>（留 T1.3d+，在 current map 记为缺口）：真 rollback 反向走链、slot 回收、失败插入原子清理。
 * 本片只覆盖成功插入路径——MTR rollback 不做 content undo，失败插入会留下 orphan undo，风险记录在
 * {@code UndoWritePathWiringTest} 名称与 current map。
 */
class UndoLogManagerTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId UNDO_SPACE = SpaceId.of(77);
    private static final long TABLE_ID = 1L;
    private static final long INDEX_ID = 9L;
    private static final int SLOT_CAPACITY = 64;

    @TempDir
    Path dir;

    private final TypeCodecRegistry registry = new TypeCodecRegistry();

    private static TableSchema schema() {
        return new TableSchema(1, List.of(
                new ColumnDef(new ColumnId(0), "id", ColumnType.intType(false, false), 0)), true);
    }

    private static IndexKeyDef keyDef() {
        return new IndexKeyDef(INDEX_ID, List.of(new KeyPartDef(new ColumnId(0), KeyOrder.ASC, 0)));
    }

    private static List<ColumnValue> keyOf(long id) {
        return List.of(new ColumnValue.IntValue(id));
    }

    @Test
    void firstWriteBuildsSegmentClaimsSlotReturnsInsertRollPointer() {
        onPool(h -> {
            Transaction txn = h.txnMgr.begin(TransactionOptions.defaults());
            h.txnMgr.assignWriteId(txn);

            MiniTransaction m = h.mgr.begin();
            RollPointer rp = h.undoMgr.beforeInsert(txn, m, TABLE_ID, INDEX_ID, keyOf(100), keyDef(), schema());
            h.mgr.commit(m);

            assertFalse(rp.isNull(), "first write must return a non-NULL insert roll pointer");
            assertTrue(rp.insert(), "insert undo roll pointer must have insert flag set");

            UndoContext ctx = txn.undoContext();
            assertNotNull(ctx, "ensureUndoContext must bind an UndoContext on first write");
            assertEquals(UndoNo.of(1), ctx.lastUndoNo(), "first append advances lastUndoNo to 1");
            assertEquals(rp, ctx.lastRollPointer(), "ctx.lastRollPointer is the just-returned roll pointer");
            // roll pointer 指向 undo segment 首页（单条记录不跨页）
            assertEquals(ctx.undoFirstPageId().pageNo(), rp.pageNo());
            // 内存 rseg slot 落该段首页
            assertEquals(ctx.undoFirstPageId(),
                    h.slots.insertUndoFirstPageId(ctx.slotId()),
                    "in-memory rseg slot must point to the insert undo segment first page");
            assertEquals(RollbackSegmentId.of(0), ctx.rollbackSegmentId(),
                    "ctx rseg id comes from the slot manager's fixed default rseg");
            h.txnMgr.commit(txn);
        });
    }

    /**
     * page3 与空内存目录冲突时必须在 undo segment 创建前失败：取消 RESERVED，不绑定事务，也不推进 FSP segment id。
     */
    @Test
    void persistentSlotConflictCancelsReservationBeforeSegmentCreation() {
        onPool(h -> {
            PageId persistentOwner = PageId.of(UNDO_SPACE, PageNo.of(63));
            MiniTransaction occupy = h.mgr.begin();
            h.finalization.header().claimSlot(occupy, UNDO_SPACE, UndoSlotId.of(0), persistentOwner);
            h.mgr.commit(occupy);

            MiniTransaction usageBeforeMtr = h.mgr.begin();
            long nextSegmentIdBefore = h.disk.usage(usageBeforeMtr, UNDO_SPACE).nextSegmentId();
            h.mgr.commit(usageBeforeMtr);

            Transaction txn = h.txnMgr.begin(TransactionOptions.defaults());
            h.txnMgr.assignWriteId(txn);
            MiniTransaction write = h.mgr.begin();
            assertThrows(UndoSlotOwnershipConflictException.class,
                    () -> h.undoMgr.beforeInsert(txn, write, TABLE_ID, INDEX_ID,
                            keyOf(100), keyDef(), schema()));
            h.mgr.rollbackUncommitted(write);

            assertEquals(0, h.slots.activeSlotCount(), "failed preflight cancels the in-memory reservation");
            assertNull(txn.undoContext(), "failed preflight never binds an undo context");

            MiniTransaction usageAfterMtr = h.mgr.begin();
            long nextSegmentIdAfter = h.disk.usage(usageAfterMtr, UNDO_SPACE).nextSegmentId();
            h.mgr.commit(usageAfterMtr);
            assertEquals(nextSegmentIdBefore, nextSegmentIdAfter,
                    "page3 conflict must be discovered before any physical segment allocation");
        });
    }

    /**
     * 预检通过后若另一个 owner 抢先持久化同槽，已经创建并 bind 的 segment 不能补偿释放；必须升级为 fatal。
     */
    @Test
    void postBindClaimConflictFailsStopWithFatalPublicationError() {
        BlockingCreateAllocator[] blocking = new BlockingCreateAllocator[1];
        onPool(UndoFinalizationFaultInjector.none(), delegate -> {
            blocking[0] = new BlockingCreateAllocator(delegate);
            return blocking[0];
        }, h -> {
            Transaction txn = h.txnMgr.begin(TransactionOptions.defaults());
            h.txnMgr.assignWriteId(txn);
            ExecutorService executor = Executors.newSingleThreadExecutor();
            try {
                Future<?> writer = executor.submit(() -> {
                    MiniTransaction write = h.mgr.begin();
                    try {
                        h.undoMgr.beforeInsert(txn, write, TABLE_ID, INDEX_ID,
                                keyOf(100), keyDef(), schema());
                        h.mgr.commit(write);
                    } catch (RuntimeException error) {
                        h.mgr.rollbackUncommitted(write);
                        throw error;
                    }
                });
                assertTrue(blocking[0].awaitCreate(), "writer passed page3 preflight and reached segment create");

                PageId competingOwner = PageId.of(UNDO_SPACE, PageNo.of(63));
                MiniTransaction drift = h.mgr.begin();
                h.finalization.header().claimSlot(drift, UNDO_SPACE, UndoSlotId.of(0), competingOwner);
                h.mgr.commit(drift);
                blocking[0].releaseCreate();

                ExecutionException failure = assertThrows(ExecutionException.class,
                        () -> writer.get(5, TimeUnit.SECONDS));
                assertTrue(failure.getCause() instanceof UndoClaimPublicationException,
                        "post-bind publication conflict is fatal, not a retryable format error");
                assertEquals(1, h.slots.activeSlotCount(), "bound ACTIVE slot remains fenced");
                assertNull(txn.undoContext(), "context is not published after persistent claim failure");
                assertEquals(1, blocking[0].createAttempts(), "same-process recovery does not recreate the segment");
            } finally {
                blocking[0].releaseCreate();
                executor.shutdownNow();
                assertDoesNotThrow(() -> executor.awaitTermination(5, TimeUnit.SECONDS));
            }
        });
    }

    /**
     * 两个终态命令命中同一 owner 时，只有第一个能越过物理 drop 边界；第二个必须在 page/FSP 访问前失败。
     */
    @Test
    void concurrentFinalizationAllowsOnlyOnePhysicalDropAttempt() {
        BlockingDropAllocator[] blocking = new BlockingDropAllocator[1];
        onPool(UndoFinalizationFaultInjector.none(), delegate -> {
            blocking[0] = new BlockingDropAllocator(delegate);
            return blocking[0];
        }, h -> {
            Transaction txn = h.txnMgr.begin(TransactionOptions.defaults());
            h.txnMgr.assignWriteId(txn);
            MiniTransaction write = h.mgr.begin();
            h.undoMgr.beforeInsert(txn, write, TABLE_ID, INDEX_ID, keyOf(101), keyDef(), schema());
            h.mgr.commit(write);
            h.txnMgr.prepareCommit(txn);

            UndoContext context = txn.undoContext();
            ExecutorService executor = Executors.newFixedThreadPool(2);
            try {
                Future<?> first = executor.submit(
                        () -> h.finalization.finalizer().finalizeInsertCommit(txn, context));
                assertTrue(blocking[0].awaitFirstDrop(), "first terminal command reached the drop boundary");

                Future<?> duplicate = executor.submit(
                        () -> h.finalization.finalizer().finalizeInsertCommit(txn, context));
                ExecutionException duplicateFailure = assertThrows(ExecutionException.class,
                        () -> duplicate.get(5, TimeUnit.SECONDS));
                assertTrue(duplicateFailure.getCause() instanceof DatabaseRuntimeException,
                        "duplicate terminal command returns a domain failure");
                assertEquals(1, blocking[0].dropAttempts(),
                        "duplicate finalization must fail before invoking the physical allocator");

                blocking[0].releaseFirstDrop();
                assertDoesNotThrow(() -> first.get(5, TimeUnit.SECONDS));
                assertEquals(0, h.slots.activeSlotCount(), "the winning finalization publishes FREE after commit");
            } finally {
                blocking[0].releaseFirstDrop();
                executor.shutdownNow();
                assertDoesNotThrow(() -> executor.awaitTermination(5, TimeUnit.SECONDS));
            }
        });
    }

    /** undo-header identity 预检失败尚未触碰 FSP，lease 必须恢复 ACTIVE，让正确终态命令仍可完成。 */
    @Test
    void finalizationPreflightFailureRestoresActiveOwnerForCorrectRetry() {
        onPool(h -> {
            Transaction txn = h.txnMgr.begin(TransactionOptions.defaults());
            TransactionId creator = h.txnMgr.assignWriteId(txn);
            MiniTransaction write = h.mgr.begin();
            h.undoMgr.beforeInsert(txn, write, TABLE_ID, INDEX_ID, keyOf(102), keyDef(), schema());
            h.mgr.commit(write);
            UndoContext context = txn.undoContext();

            assertThrows(UndoLogFormatException.class,
                    () -> h.finalization.finalizer().finalizeRecoveredRollback(
                            context.slotId(), context.undoFirstPageId(), TransactionId.of(creator.value() + 1)));
            assertEquals(context.undoFirstPageId(), h.slots.insertUndoFirstPageId(context.slotId()),
                    "pre-physical identity failure restores the original ACTIVE owner");

            h.txnMgr.prepareCommit(txn);
            h.undoMgr.onCommit(txn);
            assertEquals(0, h.slots.activeSlotCount(), "correct terminal command succeeds after preflight failure");
        });
    }

    /** 旧 identity 穿过真实 finalizer 命中新复用槽时，creator/owner 校验必须在 allocator drop 前拒绝。 */
    @Test
    void staleFinalizerCannotDropReusedPhysicalSegment() {
        CountingDropAllocator[] counting = new CountingDropAllocator[1];
        onPool(UndoFinalizationFaultInjector.none(), delegate -> {
            counting[0] = new CountingDropAllocator(delegate);
            return counting[0];
        }, h -> {
            Transaction oldTxn = h.txnMgr.begin(TransactionOptions.defaults());
            TransactionId oldCreator = h.txnMgr.assignWriteId(oldTxn);
            MiniTransaction oldWrite = h.mgr.begin();
            h.undoMgr.beforeInsert(oldTxn, oldWrite, TABLE_ID, INDEX_ID, keyOf(103), keyDef(), schema());
            h.mgr.commit(oldWrite);
            UndoContext oldContext = oldTxn.undoContext();
            h.txnMgr.prepareCommit(oldTxn);
            h.undoMgr.onCommit(oldTxn);
            h.txnMgr.commit(oldTxn);
            assertEquals(1, counting[0].dropAttempts());

            Transaction newTxn = h.txnMgr.begin(TransactionOptions.defaults());
            h.txnMgr.assignWriteId(newTxn);
            MiniTransaction newWrite = h.mgr.begin();
            h.undoMgr.beforeInsert(newTxn, newWrite, TABLE_ID, INDEX_ID, keyOf(104), keyDef(), schema());
            h.mgr.commit(newWrite);
            UndoContext newContext = newTxn.undoContext();
            assertEquals(oldContext.slotId(), newContext.slotId(), "completed slot is reused by first-fit");

            assertThrows(DatabaseRuntimeException.class,
                    () -> h.finalization.finalizer().finalizeRecoveredRollback(
                            oldContext.slotId(), oldContext.undoFirstPageId(), oldCreator));
            assertEquals(1, counting[0].dropAttempts(),
                    "stale finalizer is rejected before touching the new segment inode/pages");
            assertEquals(newContext.undoFirstPageId(), h.slots.insertUndoFirstPageId(newContext.slotId()));

            h.txnMgr.prepareCommit(newTxn);
            h.undoMgr.onCommit(newTxn);
            h.txnMgr.commit(newTxn);
            assertEquals(2, counting[0].dropAttempts(), "the new owner can still finalize normally");
        });
    }

    @Test
    void multipleInsertsIncrementUndoNoAndChainPrevRollPointer() {
        onPool(h -> {
            Transaction txn = h.txnMgr.begin(TransactionOptions.defaults());
            h.txnMgr.assignWriteId(txn);

            RollPointer[] rps = new RollPointer[3];
            for (int i = 0; i < 3; i++) {
                MiniTransaction m = h.mgr.begin();
                rps[i] = h.undoMgr.beforeInsert(txn, m, TABLE_ID, INDEX_ID, keyOf(100 + i), keyDef(), schema());
                h.mgr.commit(m);
            }

            UndoContext ctx = txn.undoContext();
            assertEquals(UndoNo.of(3), ctx.lastUndoNo(), "lastUndoNo increments across inserts");
            assertEquals(rps[2], ctx.lastRollPointer());

            // 读回验证 undoNo 递增 + prevRollPointer 串链
            MiniTransaction read = h.mgr.begin();
            UndoLogSegment seg = h.access.open(read, ctx.undoFirstPageId(), PageLatchMode.SHARED);
            UndoRecord r1 = seg.readRecord(rps[0], keyDef(), schema());
            UndoRecord r2 = seg.readRecord(rps[1], keyDef(), schema());
            UndoRecord r3 = seg.readRecord(rps[2], keyDef(), schema());
            h.mgr.rollbackUncommitted(read);

            assertEquals(UndoNo.of(1), r1.undoNo());
            assertTrue(r1.prevRollPointer().isNull(), "first undo record prev is NULL");
            assertEquals(UndoNo.of(2), r2.undoNo());
            assertEquals(rps[0], r2.prevRollPointer(), "2nd undo record chains back to 1st roll pointer");
            assertEquals(UndoNo.of(3), r3.undoNo());
            assertEquals(rps[1], r3.prevRollPointer(), "3rd undo record chains back to 2nd roll pointer");
            // undo record 落本事务 id 与表/索引定位
            assertEquals(txn.transactionId(), r1.transactionId());
            assertEquals(TABLE_ID, r1.tableId());
            assertEquals(INDEX_ID, r1.indexId());
            assertEquals(keyOf(102), r3.clusterKey(), "undo record stores original cluster key");
            h.txnMgr.commit(txn);
        });
    }

    @Test
    void reloadReadsUndoRecordByRollPointerEqualToOriginalClusterKey() {
        Path path = dir.resolve("undo.ibu");
        RollPointer[] holder = new RollPointer[1];
        PageId[] firstPageHolder = new PageId[1];
        TransactionId[] widHolder = new TransactionId[1];

        // build session：建 undo 表空间 + 一个事务一次 insert + commit，然后显式 flush 脏页供新 store 重开。
        try (PageStore store = new FileChannelPageStore();
             BufferPool pool = new LruBufferPool(store, PS, 128);
             RedoLogFileRepository repo = RedoLogFileRepository.open(dir.resolve("undo-manager-reload-redo.log"))) {
            RedoLogManager redo = RedoLogManager.durable(repo);
            MiniTransactionManager mgr = new MiniTransactionManager(new TablespaceAccessController(), redo);
            DiskSpaceManager disk = new DiskSpaceManager(pool, store, PS);
            DiskSpaceUndoAllocator allocator = new DiskSpaceUndoAllocator(disk);
            UndoLogSegmentAccess access = new UndoLogSegmentAccess(pool, PS, allocator, registry);
            RollbackSegmentSlotManager slots = new RollbackSegmentSlotManager(RollbackSegmentId.of(0), SLOT_CAPACITY);
            UndoFinalizationTestSupport.Components finalization = UndoFinalizationTestSupport.create(
                    mgr, pool, PS, access, allocator, slots);
            UndoLogManager undoMgr = finalization.manager(access, UNDO_SPACE, new HistoryList(), mgr);
            TransactionManager txnMgr = new TransactionManager(new TransactionSystem());

            MiniTransaction boot = mgr.begin();
            disk.createTablespace(boot, UNDO_SPACE, path, PageNo.of(64));
            finalization.format(boot, UNDO_SPACE);
            mgr.commit(boot);

            Transaction txn = txnMgr.begin(TransactionOptions.defaults());
            widHolder[0] = txnMgr.assignWriteId(txn);
            MiniTransaction m = mgr.begin();
            holder[0] = undoMgr.beforeInsert(txn, m, TABLE_ID, INDEX_ID, keyOf(700), keyDef(), schema());
            firstPageHolder[0] = txn.undoContext().undoFirstPageId();
            mgr.commit(m);
            txnMgr.commit(txn);
            flushAllDirty(pool, store, redo);
        }

        // reload session：全新 PageStore/BufferPool，仅靠 roll pointer + undo first page 读回（不依赖持久 rseg header）
        try (PageStore store = new FileChannelPageStore();
             BufferPool pool = new LruBufferPool(store, PS, 128)) {
            store.open(UNDO_SPACE, path, PS);
            MiniTransactionManager mgr = new MiniTransactionManager();
            DiskSpaceManager disk = new DiskSpaceManager(pool, store, PS);
            UndoLogSegmentAccess access = new UndoLogSegmentAccess(pool, PS, new DiskSpaceUndoAllocator(disk), registry);

            RollPointer rp = holder[0];
            assertFalse(rp.isNull());
            MiniTransaction read = mgr.begin();
            UndoLogSegment seg = access.open(read, firstPageHolder[0], PageLatchMode.SHARED);
            UndoRecord rec = seg.readRecord(rp, keyDef(), schema());
            mgr.rollbackUncommitted(read);

            assertEquals(keyOf(700), rec.clusterKey(), "reload reads back original cluster key by roll pointer");
            assertEquals(UndoNo.of(1), rec.undoNo());
            assertTrue(rec.prevRollPointer().isNull());
            assertEquals(widHolder[0], rec.transactionId());
            assertEquals(TABLE_ID, rec.tableId());
            assertEquals(INDEX_ID, rec.indexId());
        }
    }

    @Test
    void rejectsNoneTransactionId() {
        onPool(h -> {
            Transaction txn = h.txnMgr.begin(TransactionOptions.defaults());
            // 未 assignWriteId → transactionId 为 NONE
            MiniTransaction m = h.mgr.begin();
            assertThrows(TransactionStateException.class,
                    () -> h.undoMgr.beforeInsert(txn, m, TABLE_ID, INDEX_ID, keyOf(1), keyDef(), schema()));
            h.mgr.rollbackUncommitted(m);
        });
    }

    @Test
    void rejectsNonActiveTransaction() {
        onPool(h -> {
            Transaction txn = h.txnMgr.begin(TransactionOptions.defaults());
            h.txnMgr.assignWriteId(txn);
            h.txnMgr.commit(txn); // state -> COMMITTED

            MiniTransaction m = h.mgr.begin();
            assertThrows(TransactionStateException.class,
                    () -> h.undoMgr.beforeInsert(txn, m, TABLE_ID, INDEX_ID, keyOf(1), keyDef(), schema()));
            h.mgr.rollbackUncommitted(m);
        });
    }

    @Test
    void rejectsNullArgs() {
        onPool(h -> {
            Transaction txn = h.txnMgr.begin(TransactionOptions.defaults());
            h.txnMgr.assignWriteId(txn);
            MiniTransaction m = h.mgr.begin();
            assertThrows(TransactionStateException.class,
                    () -> h.undoMgr.beforeInsert(null, m, TABLE_ID, INDEX_ID, keyOf(1), keyDef(), schema()));
            assertThrows(TransactionStateException.class,
                    () -> h.undoMgr.beforeInsert(txn, null, TABLE_ID, INDEX_ID, keyOf(1), keyDef(), schema()));
            assertThrows(TransactionStateException.class,
                    () -> h.undoMgr.beforeInsert(txn, m, TABLE_ID, INDEX_ID, null, keyDef(), schema()));
            h.mgr.rollbackUncommitted(m);
        });
    }

    // ---- T1.3d：commit 回收 insert undo slot（对齐 trx_undo_insert_cleanup） ----

    @Test
    void onCommitReleasesSlotForReclaim() {
        onPool(h -> {
            Transaction txn = h.txnMgr.begin(TransactionOptions.defaults());
            h.txnMgr.assignWriteId(txn);
            MiniTransaction m = h.mgr.begin();
            h.undoMgr.beforeInsert(txn, m, TABLE_ID, INDEX_ID, keyOf(100), keyDef(), schema());
            h.mgr.commit(m);
            assertEquals(1, h.slots.activeSlotCount(), "slot claimed on first write");

            // commit 编排：先预留提交号，onCommit 原子回收 insert undo，最后发布事务 COMMITTED。
            h.txnMgr.prepareCommit(txn);
            h.undoMgr.onCommit(txn);
            h.txnMgr.commit(txn);

            assertEquals(0, h.slots.activeSlotCount(), "onCommit releases the insert undo slot");

            // 释放后的 slot 可被后续事务重认领
            Transaction txn2 = h.txnMgr.begin(TransactionOptions.defaults());
            h.txnMgr.assignWriteId(txn2);
            MiniTransaction m2 = h.mgr.begin();
            h.undoMgr.beforeInsert(txn2, m2, TABLE_ID, INDEX_ID, keyOf(200), keyDef(), schema());
            h.mgr.commit(m2);
            assertEquals(1, h.slots.activeSlotCount(), "released slot reusable by next txn");
            h.txnMgr.prepareCommit(txn2);
            h.undoMgr.onCommit(txn2);
            h.txnMgr.commit(txn2);
        });
    }

    /**
     * 模拟 finalization MTR 已提交、内存 slot 尚未发布即进程崩溃：page3 与 FSP drop 必须已经处于同一 redo batch，
     * 新进程只依据 page3 不会复活该段；旧进程残留的内存映射只是不可继续运行的瞬时投影。
     */
    @Test
    void crashAfterAtomicInsertFinalizationCommitDoesNotLeavePersistentOwner() {
        AtomicBoolean injected = new AtomicBoolean();
        onPool((kind, slotId, firstPageId) -> {
            if (kind == UndoFinalizationKind.INSERT_COMMIT && injected.compareAndSet(false, true)) {
                throw new SimulatedFinalizationCrashException("crash after finalization MTR commit");
            }
        }, h -> {
            Transaction txn = h.txnMgr.begin(TransactionOptions.defaults());
            h.txnMgr.assignWriteId(txn);
            MiniTransaction write = h.mgr.begin();
            h.undoMgr.beforeInsert(txn, write, TABLE_ID, INDEX_ID, keyOf(100), keyDef(), schema());
            h.mgr.commit(write);
            UndoContext context = txn.undoContext();
            int batchesBeforeFinalization = h.mgr.redoLogManager().bufferedBatches().size();

            h.txnMgr.prepareCommit(txn);
            assertThrows(SimulatedFinalizationCrashException.class, () -> h.undoMgr.onCommit(txn));

            assertEquals(TransactionState.ACTIVE, txn.state(), "transaction terminal state was not published");
            assertTrue(h.slots.isOccupied(context.slotId()),
                    "simulated old process still has the pre-crash memory projection");
            assertThrows(DatabaseValidationException.class, () -> h.undoMgr.onCommit(txn),
                    "post-commit crash leaves FINALIZING and rejects same-process retry before page access");
            MiniTransaction read = h.mgr.begin();
            var snapshot = h.finalization.header().read(read, UNDO_SPACE,
                    h.slots.rollbackSegmentId(), h.slots.slotCapacity());
            h.mgr.commit(read);
            assertFalse(snapshot.occupiedSlots().containsKey(context.slotId()),
                    "page3 is recovery authority and was cleared before the simulated crash");

            List<RedoLogBatch> batches = h.mgr.redoLogManager().bufferedBatches();
            assertEquals(batchesBeforeFinalization + 1, batches.size(),
                    "drop, slot clear and commit diagnostic share one finalization MTR");
            List<RedoRecord> records = batches.getLast().records();
            assertTrue(records.stream().anyMatch(FspMetadataDeltaRecord.class::isInstance),
                    "finalization batch contains the physical segment-release ledger changes");
            assertTrue(records.stream().anyMatch(FspPageFreeRecord.class::isInstance),
                    "the same batch contains the undo page free intent");
            assertTrue(records.stream().anyMatch(record -> record instanceof UndoMetadataDeltaRecord delta
                            && delta.kind() == UndoMetadataDeltaKind.RSEG_SLOT
                            && delta.subIndex() == context.slotId().value()),
                    "the same batch clears the exact page3 owner slot");
            assertTrue(records.stream().anyMatch(record -> record instanceof TransactionStateDeltaRecord delta
                            && delta.reason() == TransactionStateDeltaReason.COMMIT
                            && delta.toState() == TransactionStateDeltaState.COMMITTED),
                    "the same batch records the insert commit diagnostic boundary");
        });
    }

    @Test
    void onCommitWithoutWriteIsNoOp() {
        onPool(h -> {
            Transaction txn = h.txnMgr.begin(TransactionOptions.defaults());
            // 未写入：undoContext 为 null，onCommit 不应抛、不动 slot
            h.undoMgr.onCommit(txn);
            assertEquals(0, h.slots.activeSlotCount());
            h.txnMgr.commit(txn);
        });
    }

    // ---- T1.3e：beforeUpdate（UPDATE undo 写）+ onCommit 含 update 不回收 slot ----

    @Test
    void beforeUpdateWritesUpdateUndoChainsAndCarriesOldImage() {
        onPool(h -> {
            Transaction txn = h.txnMgr.begin(TransactionOptions.defaults());
            TransactionId wid = h.txnMgr.assignWriteId(txn);
            MiniTransaction m = h.mgr.begin();
            RollPointer insRp = h.undoMgr.beforeInsert(txn, m, TABLE_ID, INDEX_ID, keyOf(100), keyDef(), schema());
            // 该行被本事务更新：旧 image = 更新前全列值 + 更新前隐藏列（DB_ROLL_PTR=insRp，即版本链上一版本）
            HiddenColumns oldHidden = new HiddenColumns(wid, insRp);
            RollPointer updRp = h.undoMgr.beforeUpdate(txn, m, TABLE_ID, INDEX_ID, keyOf(100),
                    List.of(new ColumnValue.IntValue(100)), oldHidden, keyDef(), schema());
            h.mgr.commit(m);

            assertFalse(updRp.isNull());
            assertFalse(updRp.insert(), "update undo roll pointer insert flag must be false");
            UndoContext ctx = txn.undoContext();
            assertEquals(UndoNo.of(2), ctx.lastUndoNo(), "undoNo increments across insert+update");
            assertEquals(updRp, ctx.lastRollPointer());
            assertTrue(ctx.hasUpdateUndo(), "beforeUpdate marks hasUpdateUndo");

            // 读回 update undo：prevRollPointer 串事务回滚链(=insRp)，旧 image 等值
            MiniTransaction r = h.mgr.begin();
            UndoLogSegment seg = h.access.open(r, ctx.undoFirstPageId(), PageLatchMode.SHARED);
            UndoRecord rec = seg.readRecord(updRp, keyDef(), schema());
            h.mgr.rollbackUncommitted(r);
            assertEquals(UndoRecordType.UPDATE_ROW, rec.type());
            assertEquals(insRp, rec.prevRollPointer(), "update undo prevRollPointer chains tx rollback chain");
            assertEquals(oldHidden, rec.oldHiddenColumns(), "old hidden = pre-update version pointer (版本链)");
            assertEquals(List.of(new ColumnValue.IntValue(100)), rec.oldColumnValues());
            h.txnMgr.commit(txn);
        });
    }

    @Test
    void beforeDeleteWritesDeleteMarkUndoChainsAndKeepsSlotOnCommit() {
        onPool(h -> {
            Transaction txn = h.txnMgr.begin(TransactionOptions.defaults());
            TransactionId wid = h.txnMgr.assignWriteId(txn);
            MiniTransaction m = h.mgr.begin();
            RollPointer insRp = h.undoMgr.beforeInsert(txn, m, TABLE_ID, INDEX_ID, keyOf(100), keyDef(), schema());
            HiddenColumns oldHidden = new HiddenColumns(wid, insRp);
            RollPointer delRp = h.undoMgr.beforeDelete(txn, m, TABLE_ID, INDEX_ID, keyOf(100),
                    List.of(new ColumnValue.IntValue(100)), oldHidden, keyDef(), schema());
            h.mgr.commit(m);

            assertFalse(delRp.isNull());
            assertFalse(delRp.insert(), "delete-mark undo roll pointer insert flag must be false");
            UndoContext ctx = txn.undoContext();
            assertEquals(UndoNo.of(2), ctx.lastUndoNo());
            assertTrue(ctx.hasUpdateUndo(), "beforeDelete marks hasUpdateUndo (delete undo must survive commit)");

            MiniTransaction r = h.mgr.begin();
            UndoLogSegment seg = h.access.open(r, ctx.undoFirstPageId(), PageLatchMode.SHARED);
            UndoRecord rec = seg.readRecord(delRp, keyDef(), schema());
            h.mgr.rollbackUncommitted(r);
            assertEquals(UndoRecordType.DELETE_MARK, rec.type());
            assertEquals(insRp, rec.prevRollPointer(), "delete-mark undo chains tx rollback chain");
            assertEquals(oldHidden, rec.oldHiddenColumns());

            // commit 不回收含 delete undo 事务的 slot
            h.txnMgr.prepareCommit(txn);
            h.undoMgr.onCommit(txn);
            h.txnMgr.commit(txn);
            assertEquals(1, h.slots.activeSlotCount(), "slot retained when txn wrote DELETE_MARK undo");
        });
    }

    @Test
    void onCommitKeepsSlotWhenUpdateUndoPresent() {
        onPool(h -> {
            Transaction txn = h.txnMgr.begin(TransactionOptions.defaults());
            TransactionId wid = h.txnMgr.assignWriteId(txn);
            MiniTransaction m = h.mgr.begin();
            RollPointer insRp = h.undoMgr.beforeInsert(txn, m, TABLE_ID, INDEX_ID, keyOf(100), keyDef(), schema());
            h.undoMgr.beforeUpdate(txn, m, TABLE_ID, INDEX_ID, keyOf(100),
                    List.of(new ColumnValue.IntValue(100)), new HiddenColumns(wid, insRp), keyDef(), schema());
            h.mgr.commit(m);
            assertEquals(1, h.slots.activeSlotCount());

            h.txnMgr.prepareCommit(txn);
            h.undoMgr.onCommit(txn);
            h.txnMgr.commit(txn);
            assertEquals(1, h.slots.activeSlotCount(),
                    "slot retained when txn wrote UPDATE undo (T1.4 MVCC/purge needs it)");
        });
    }

    @Test
    void onCommitWritesTransactionStateRedoInCommitMtr() {
        onPool(h -> {
            UndoLogManager durableUndo = h.undoMgr;
            Transaction txn = h.txnMgr.begin(TransactionOptions.defaults());
            h.txnMgr.assignWriteId(txn);
            MiniTransaction m = h.mgr.begin();
            durableUndo.beforeInsert(txn, m, TABLE_ID, INDEX_ID, keyOf(100), keyDef(), schema());
            h.mgr.commit(m);

            h.txnMgr.prepareCommit(txn);
            durableUndo.onCommit(txn);

            List<RedoRecord> records = h.mgr.redoLogManager().bufferedRecords();
            assertTrue(records.stream().anyMatch(record -> record instanceof TransactionStateDeltaRecord delta
                            && delta.transactionId().equals(txn.transactionId())
                            && delta.toState() == TransactionStateDeltaState.COMMITTED
                            && delta.transactionNo().equals(txn.transactionNo())
                            && delta.reason() == TransactionStateDeltaReason.COMMIT),
                    "commit MTR must include diagnostic trx state redo");
            h.txnMgr.commit(txn);
        });
    }

    @Test
    void mixedInsertUpdateSegmentReopenReadsBothByRollPointer() {
        Path path = dir.resolve("undo.ibu");
        RollPointer[] ins = new RollPointer[1];
        RollPointer[] upd = new RollPointer[1];
        PageId[] firstPage = new PageId[1];
        TransactionId[] widHolder = new TransactionId[1];

        try (PageStore store = new FileChannelPageStore();
             BufferPool pool = new LruBufferPool(store, PS, 128);
             RedoLogFileRepository repo = RedoLogFileRepository.open(dir.resolve("undo-manager-mixed-redo.log"))) {
            RedoLogManager redo = RedoLogManager.durable(repo);
            MiniTransactionManager mgr = new MiniTransactionManager(new TablespaceAccessController(), redo);
            DiskSpaceManager disk = new DiskSpaceManager(pool, store, PS);
            DiskSpaceUndoAllocator allocator = new DiskSpaceUndoAllocator(disk);
            UndoLogSegmentAccess access = new UndoLogSegmentAccess(pool, PS, allocator, registry);
            RollbackSegmentSlotManager slots = new RollbackSegmentSlotManager(RollbackSegmentId.of(0), SLOT_CAPACITY);
            UndoFinalizationTestSupport.Components finalization = UndoFinalizationTestSupport.create(
                    mgr, pool, PS, access, allocator, slots);
            UndoLogManager undoMgr = finalization.manager(access, UNDO_SPACE, new HistoryList(), mgr);
            TransactionManager txnMgr = new TransactionManager(new TransactionSystem());

            MiniTransaction boot = mgr.begin();
            disk.createTablespace(boot, UNDO_SPACE, path, PageNo.of(64));
            finalization.format(boot, UNDO_SPACE);
            mgr.commit(boot);

            Transaction txn = txnMgr.begin(TransactionOptions.defaults());
            widHolder[0] = txnMgr.assignWriteId(txn);
            MiniTransaction m = mgr.begin();
            ins[0] = undoMgr.beforeInsert(txn, m, TABLE_ID, INDEX_ID, keyOf(700), keyDef(), schema());
            upd[0] = undoMgr.beforeUpdate(txn, m, TABLE_ID, INDEX_ID, keyOf(700),
                    List.of(new ColumnValue.IntValue(700)), new HiddenColumns(widHolder[0], ins[0]), keyDef(), schema());
            firstPage[0] = txn.undoContext().undoFirstPageId();
            mgr.commit(m);
            txnMgr.commit(txn);
            flushAllDirty(pool, store, redo);
        }

        try (PageStore store = new FileChannelPageStore();
             BufferPool pool = new LruBufferPool(store, PS, 128)) {
            store.open(UNDO_SPACE, path, PS);
            MiniTransactionManager mgr = new MiniTransactionManager();
            DiskSpaceManager disk = new DiskSpaceManager(pool, store, PS);
            UndoLogSegmentAccess access = new UndoLogSegmentAccess(pool, PS, new DiskSpaceUndoAllocator(disk), registry);

            MiniTransaction read = mgr.begin();
            UndoLogSegment seg = access.open(read, firstPage[0], PageLatchMode.SHARED);
            UndoRecord insRec = seg.readRecord(ins[0], keyDef(), schema());
            UndoRecord updRec = seg.readRecord(upd[0], keyDef(), schema());
            mgr.rollbackUncommitted(read);

            assertEquals(UndoRecordType.INSERT_ROW, insRec.type(), "reopen reads insert undo by its roll pointer");
            assertEquals(UndoRecordType.UPDATE_ROW, updRec.type(), "reopen reads update undo by its roll pointer");
            assertEquals(new HiddenColumns(widHolder[0], ins[0]), updRec.oldHiddenColumns());
            assertTrue(ins[0].insert());
            assertFalse(upd[0].insert());
        }
    }

    @Test
    void beforeUpdateOversizedOldImageThrowsOverflow() {
        onPool(h -> {
            TableSchema wide = new TableSchema(1, List.of(
                    new ColumnDef(new ColumnId(0), "id", ColumnType.intType(false, false), 0),
                    new ColumnDef(new ColumnId(1), "payload", ColumnType.varchar(20000, true), 1)), true);
            IndexKeyDef wideKey = new IndexKeyDef(INDEX_ID,
                    List.of(new KeyPartDef(new ColumnId(0), KeyOrder.ASC, 0)));
            Transaction txn = h.txnMgr.begin(TransactionOptions.defaults());
            h.txnMgr.assignWriteId(txn);
            MiniTransaction m = h.mgr.begin();
            // 全量旧 image 超单页（无 extern undo payload，T1.3e 非目标）→ 抛溢出，不静默损坏
            List<ColumnValue> hugeRow = List.of(new ColumnValue.IntValue(1),
                    new ColumnValue.StringValue("y".repeat(16300)));
            assertThrows(UndoPageOverflowException.class, () -> h.undoMgr.beforeUpdate(txn, m, TABLE_ID, INDEX_ID,
                    List.of(new ColumnValue.IntValue(1)), hugeRow,
                    new HiddenColumns(txn.transactionId(), RollPointer.NULL), wideKey, wide));
            h.mgr.rollbackUncommitted(m);
        });
    }

    // ---- harness ----

    private interface Body {
        void run(H h);
    }

    private void onPool(Body body) {
        onPool(UndoFinalizationFaultInjector.none(), body);
    }

    private void onPool(UndoFinalizationFaultInjector faultInjector, Body body) {
        onPool(faultInjector, Function.identity(), body);
    }

    /** 允许 focused concurrency 测试装饰真实 allocator，同时保留其完整分配与 FSP 副作用。 */
    private void onPool(UndoFinalizationFaultInjector faultInjector,
                        Function<UndoSpaceAllocator, UndoSpaceAllocator> allocatorDecorator,
                        Body body) {
        PageStore store = new FileChannelPageStore();
        try (PageStore ignored = store; BufferPool pool = new LruBufferPool(store, PS, 128)) {
            MiniTransactionManager mgr = new MiniTransactionManager();
            DiskSpaceManager disk = new DiskSpaceManager(pool, store, PS);
            UndoSpaceAllocator allocator = allocatorDecorator.apply(new DiskSpaceUndoAllocator(disk));
            UndoLogSegmentAccess access = new UndoLogSegmentAccess(pool, PS, allocator, registry);
            RollbackSegmentSlotManager slots = new RollbackSegmentSlotManager(RollbackSegmentId.of(0), SLOT_CAPACITY);
            UndoFinalizationTestSupport.Components finalization = UndoFinalizationTestSupport.create(
                    mgr, pool, PS, access, allocator, slots, faultInjector);
            UndoLogManager undoMgr = finalization.manager(access, UNDO_SPACE, new HistoryList(), mgr);
            TransactionManager txnMgr = new TransactionManager(new TransactionSystem());

            MiniTransaction boot = mgr.begin();
            disk.createTablespace(boot, UNDO_SPACE, dir.resolve("undo.ibu"), PageNo.of(64));
            finalization.format(boot, UNDO_SPACE);
            mgr.commit(boot);

            body.run(new H(mgr, disk, access, slots, undoMgr, txnMgr, finalization));
        }
    }

    /**
     * 保留真实 allocator 全部行为，只在第一次 drop 入口形成可控并发窗口；第二次尝试在触碰 FSP 前拒绝，避免
     * 回归测试为了证明竞态而真的双重释放同一 inode/page。
     */
    private static final class BlockingDropAllocator implements UndoSpaceAllocator {
        /** 完整真实行为的下游 allocator。 */
        private final UndoSpaceAllocator delegate;
        /** 第一次 drop 已进入的有界等待信号。 */
        private final CountDownLatch firstDropEntered = new CountDownLatch(1);
        /** 允许第一次 drop 继续的显式释放信号。 */
        private final CountDownLatch releaseFirstDrop = new CountDownLatch(1);
        /** 所有 drop 调用次数；第二次在 delegate 前被拒绝。 */
        private final AtomicInteger dropAttempts = new AtomicInteger();

        private BlockingDropAllocator(UndoSpaceAllocator delegate) {
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
        public cn.zhangyis.db.storage.undo.UndoSegmentDropPlan inspectDropPlan(
                MiniTransaction mtr, UndoSegmentHandle handle) {
            return delegate.inspectDropPlan(mtr, handle);
        }

        @Override
        public void dropUndoSegment(MiniTransaction mtr, UndoSegmentHandle handle) {
            int attempt = dropAttempts.incrementAndGet();
            if (attempt > 1) {
                throw new DatabaseRuntimeException("duplicate physical undo drop attempt");
            }
            firstDropEntered.countDown();
            try {
                if (!releaseFirstDrop.await(5, TimeUnit.SECONDS)) {
                    throw new DatabaseRuntimeException("timed out waiting to release first undo drop");
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new DatabaseRuntimeException("interrupted while blocking first undo drop", interrupted);
            }
            delegate.dropUndoSegment(mtr, handle);
        }

        private boolean awaitFirstDrop() {
            try {
                return firstDropEntered.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new DatabaseRuntimeException("interrupted while awaiting first undo drop", interrupted);
            }
        }

        private void releaseFirstDrop() {
            releaseFirstDrop.countDown();
        }

        private int dropAttempts() {
            return dropAttempts.get();
        }
    }

    /** 在真实 create 之前形成预检→持久 claim 的竞态窗口，其余 allocator 行为完整透传。 */
    private static final class BlockingCreateAllocator implements UndoSpaceAllocator {
        /** 完整真实行为的下游 allocator。 */
        private final UndoSpaceAllocator delegate;
        /** writer 已通过 page3 预检并进入 create 的信号。 */
        private final CountDownLatch createEntered = new CountDownLatch(1);
        /** 允许 writer 继续真实分配的信号。 */
        private final CountDownLatch releaseCreate = new CountDownLatch(1);
        /** create 调用计数，防止异常后隐式重建。 */
        private final AtomicInteger createAttempts = new AtomicInteger();

        private BlockingCreateAllocator(UndoSpaceAllocator delegate) {
            this.delegate = delegate;
        }

        @Override
        public UndoSegmentHandle createUndoSegment(MiniTransaction mtr, SpaceId undoSpace) {
            createAttempts.incrementAndGet();
            createEntered.countDown();
            try {
                if (!releaseCreate.await(5, TimeUnit.SECONDS)) {
                    throw new DatabaseRuntimeException("timed out waiting to release undo create");
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new DatabaseRuntimeException("interrupted while blocking undo create", interrupted);
            }
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
        public cn.zhangyis.db.storage.undo.UndoSegmentDropPlan inspectDropPlan(
                MiniTransaction mtr, UndoSegmentHandle handle) {
            return delegate.inspectDropPlan(mtr, handle);
        }

        @Override
        public void dropUndoSegment(MiniTransaction mtr, UndoSegmentHandle handle) {
            delegate.dropUndoSegment(mtr, handle);
        }

        private boolean awaitCreate() {
            try {
                return createEntered.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new DatabaseRuntimeException("interrupted while awaiting undo create", interrupted);
            }
        }

        private void releaseCreate() {
            releaseCreate.countDown();
        }

        private int createAttempts() {
            return createAttempts.get();
        }
    }

    /** 完整透传真实 allocator，并只统计物理 drop 边界是否被 stale finalizer 触达。 */
    private static final class CountingDropAllocator implements UndoSpaceAllocator {
        /** 完整真实行为的下游 allocator。 */
        private final UndoSpaceAllocator delegate;
        /** 成功进入物理 drop 端口的次数。 */
        private final AtomicInteger dropAttempts = new AtomicInteger();

        private CountingDropAllocator(UndoSpaceAllocator delegate) {
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
        public cn.zhangyis.db.storage.undo.UndoSegmentDropPlan inspectDropPlan(
                MiniTransaction mtr, UndoSegmentHandle handle) {
            return delegate.inspectDropPlan(mtr, handle);
        }

        @Override
        public void dropUndoSegment(MiniTransaction mtr, UndoSegmentHandle handle) {
            dropAttempts.incrementAndGet();
            delegate.dropUndoSegment(mtr, handle);
        }

        private int dropAttempts() {
            return dropAttempts.get();
        }
    }

    /** 只用于模拟 commit 后进程消失；生产 finalizer 不安装该 hook。 */
    private static final class SimulatedFinalizationCrashException extends DatabaseRuntimeException {
        private SimulatedFinalizationCrashException(String message) {
            super(message);
        }
    }

    /**
     * 跨 BufferPool/PageStore reload 的测试需要 data file 中真实存在 undo 页；这里显式走 WAL gate 后 flush。
     */
    private static void flushAllDirty(BufferPool pool, PageStore store, RedoLogManager redo) {
        redo.flush();
        FlushCoordinator coordinator = new FlushCoordinator(pool, store, redo, PS,
                new NoDoublewriteStrategy(), Duration.ofMillis(50));
        coordinator.flushList(Lsn.of(Long.MAX_VALUE), pool.capacity());
    }

    private static final class H {
        final MiniTransactionManager mgr;
        final DiskSpaceManager disk;
        final UndoLogSegmentAccess access;
        final RollbackSegmentSlotManager slots;
        final UndoLogManager undoMgr;
        final TransactionManager txnMgr;
        final UndoFinalizationTestSupport.Components finalization;

        H(MiniTransactionManager mgr, DiskSpaceManager disk, UndoLogSegmentAccess access,
          RollbackSegmentSlotManager slots,
          UndoLogManager undoMgr, TransactionManager txnMgr,
          UndoFinalizationTestSupport.Components finalization) {
            this.mgr = mgr;
            this.disk = disk;
            this.access = access;
            this.slots = slots;
            this.undoMgr = undoMgr;
            this.txnMgr = txnMgr;
            this.finalization = finalization;
        }
    }
}
