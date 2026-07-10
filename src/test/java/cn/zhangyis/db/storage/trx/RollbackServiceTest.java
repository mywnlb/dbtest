package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.domain.RollbackSegmentId;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.UndoNo;
import cn.zhangyis.db.domain.UndoSlotId;
import cn.zhangyis.db.storage.api.DiskSpaceManager;
import cn.zhangyis.db.storage.api.DiskSpaceUndoAllocator;
import cn.zhangyis.db.storage.api.index.IndexPageAccess;
import cn.zhangyis.db.storage.api.SegmentRef;
import cn.zhangyis.db.storage.btree.BTreeDeleteMarkResult;
import cn.zhangyis.db.storage.btree.BTreeIndex;
import cn.zhangyis.db.storage.btree.BTreeInsertResult;
import cn.zhangyis.db.storage.btree.BTreeLookupResult;
import cn.zhangyis.db.storage.btree.SplitCapableBTreeIndexService;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.fil.access.TablespaceAccessController;
import cn.zhangyis.db.storage.fil.io.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.fsp.segment.SegmentPurpose;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import cn.zhangyis.db.storage.flush.FlushCoordinator;
import cn.zhangyis.db.storage.flush.doublewrite.NoDoublewriteStrategy;
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
import cn.zhangyis.db.storage.redo.RedoRecord;
import cn.zhangyis.db.storage.redo.RedoLogFileRepository;
import cn.zhangyis.db.storage.redo.RedoLogManager;
import cn.zhangyis.db.storage.redo.TransactionStateDeltaReason;
import cn.zhangyis.db.storage.redo.TransactionStateDeltaRecord;
import cn.zhangyis.db.storage.redo.TransactionStateDeltaState;
import cn.zhangyis.db.storage.undo.UndoLogicalHead;
import cn.zhangyis.db.storage.undo.UndoLogicalHeadConflictException;
import cn.zhangyis.db.storage.undo.UndoLogFormatException;
import cn.zhangyis.db.storage.undo.UndoLogSegment;
import cn.zhangyis.db.storage.undo.UndoLogSegmentAccess;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * T1.3d RollbackService：反向走 INSERT undo 链物理删除未提交聚簇行 + 回收内存 slot。整栈 test-wired
 * （assignWriteId → beforeInsert → insertClustered → rollback），无生产组合根。覆盖：单行/多行 full rollback、
 * orphan undo 幂等、只读/未写事务仅翻状态。
 *
 * <p><b>当前覆盖</b>：完整 rollback、INSERT/UPDATE/DELETE_MARK 反向命令、精确 savepoint 与一次性空边界 rollback。
 * <b>非目标</b>：SQL/session 自动 statement 生命周期、多索引 rollback、undo 页回收。
 */
class RollbackServiceTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId DATA_SPACE = SpaceId.of(41);
    private static final SpaceId UNDO_SPACE = SpaceId.of(77);
    private static final long INDEX_ID = 9L;
    private static final long TABLE_ID = 1L;
    /** UndoRecord payload 中 prevRollPointer 前有 type + undoNo/txnId/tableId/indexId。 */
    private static final int UNDO_PREV_POINTER_IN_PAYLOAD = 1 + 4 * Long.BYTES;

    @TempDir
    Path dir;

    private final TypeCodecRegistry registry = new TypeCodecRegistry();

    @Test
    void singleRowInsertRollbackRemovesRowAndReleasesSlot() {
        onPool(ctx -> {
            ctx.boot();
            SplitCapableBTreeIndexService svc = ctx.service();
            BTreeIndex index = ctx.clusteredIndex();

            Transaction txn = ctx.txnMgr.begin(TransactionOptions.defaults());
            TransactionId wid = ctx.txnMgr.assignWriteId(txn);
            MiniTransaction m = ctx.mgr.begin();
            RollPointer rp = ctx.undoMgr.beforeInsert(txn, m, TABLE_ID, INDEX_ID,
                    key(1), index.keyDef(), index.schema());
            svc.insertClustered(m, index, row(1), wid, rp);
            ctx.mgr.commit(m);
            assertEquals(1, ctx.slots.activeSlotCount(), "slot claimed on first write");

            RollbackSummary summary = ctx.rollbackService.rollback(txn, index);

            assertEquals(1, summary.undoRecordsApplied(), "one INSERT undo applied");
            assertEquals(TransactionState.ROLLED_BACK, txn.state());
            List<RedoRecord> records = ctx.mgr.redoLogManager().bufferedRecords();
            assertTrue(records.stream().anyMatch(record -> record instanceof TransactionStateDeltaRecord delta
                            && delta.transactionId().equals(wid)
                            && delta.toState() == TransactionStateDeltaState.ROLLED_BACK
                            && delta.transactionNo().isNone()
                            && delta.reason() == TransactionStateDeltaReason.ROLLBACK),
                    "rollback completion must write diagnostic trx state redo before finishRollback");
            assertEquals(0, ctx.slots.activeSlotCount(), "slot released after rollback");
            MiniTransaction r = ctx.mgr.begin();
            assertTrue(svc.lookup(r, index, search(1)).isEmpty(), "inserted row removed by rollback");
            ctx.mgr.commit(r);
        });
    }

    /** 前一次 apply 失败会留下 ROLLING_BACK；重试必须从持久/内存链头幂等重走并完成终态。 */
    @Test
    void rollbackResumesTransactionAlreadyRollingBack() {
        onPool(ctx -> {
            ctx.boot();
            SplitCapableBTreeIndexService svc = ctx.service();
            BTreeIndex index = ctx.clusteredIndex();
            Transaction txn = ctx.txnMgr.begin(TransactionOptions.defaults());
            TransactionId wid = ctx.txnMgr.assignWriteId(txn);
            MiniTransaction write = ctx.mgr.begin();
            RollPointer pointer = ctx.undoMgr.beforeInsert(txn, write, TABLE_ID, INDEX_ID,
                    key(1), index.keyDef(), index.schema());
            svc.insertClustered(write, index, row(1), wid, pointer);
            ctx.mgr.commit(write);
            ctx.txnMgr.beginRollback(txn);

            RollbackSummary summary = ctx.rollbackService.rollback(txn, index);

            assertEquals(1, summary.undoRecordsApplied());
            assertEquals(TransactionState.ROLLED_BACK, txn.state());
            assertEquals(0, ctx.slots.activeSlotCount());
        });
    }

    /** 损坏 predecessor 环必须在 ACTIVE→ROLLING_BACK 和任何 index inverse 前被严格下降检查拒绝。 */
    @Test
    void fullRollbackRejectsUndoCycleBeforeChangingStateOrRows() {
        onPool(ctx -> {
            ctx.boot();
            SplitCapableBTreeIndexService svc = ctx.service();
            BTreeIndex index = ctx.clusteredIndex();
            Transaction txn = ctx.txnMgr.begin(TransactionOptions.defaults());
            TransactionId wid = ctx.txnMgr.assignWriteId(txn);
            MiniTransaction first = ctx.mgr.begin();
            RollPointer rp1 = ctx.undoMgr.beforeInsert(txn, first, TABLE_ID, INDEX_ID,
                    key(1), index.keyDef(), index.schema());
            svc.insertClustered(first, index, row(1), wid, rp1);
            ctx.mgr.commit(first);
            MiniTransaction second = ctx.mgr.begin();
            RollPointer rp2 = ctx.undoMgr.beforeInsert(txn, second, TABLE_ID, INDEX_ID,
                    key(2), index.keyDef(), index.schema());
            svc.insertClustered(second, index, row(2), wid, rp2);
            ctx.mgr.commit(second);
            ctx.rewriteUndoPredecessor(rp2, rp2);

            assertThrows(DatabaseRuntimeException.class,
                    () -> ctx.rollbackService.rollback(txn, index));

            assertEquals(TransactionState.ACTIVE, txn.state());
            assertEquals(1, ctx.slots.activeSlotCount());
            MiniTransaction read = ctx.mgr.begin();
            assertTrue(svc.lookup(read, index, search(1)).isPresent());
            assertTrue(svc.lookup(read, index, search(2)).isPresent());
            ctx.mgr.commit(read);
        });
    }

    @Test
    void multipleRowsRollbackReverseWalkRemovesAll() {
        onPool(ctx -> {
            ctx.boot();
            SplitCapableBTreeIndexService svc = ctx.service();
            BTreeIndex index = ctx.clusteredIndex();

            Transaction txn = ctx.txnMgr.begin(TransactionOptions.defaults());
            TransactionId wid = ctx.txnMgr.assignWriteId(txn);
            for (int i = 1; i <= 3; i++) {
                MiniTransaction m = ctx.mgr.begin();
                RollPointer rp = ctx.undoMgr.beforeInsert(txn, m, TABLE_ID, INDEX_ID,
                        key(i), index.keyDef(), index.schema());
                BTreeInsertResult res = svc.insertClustered(m, index, row(i), wid, rp);
                index = res.indexAfterInsert();
                ctx.mgr.commit(m);
            }

            RollbackSummary summary = ctx.rollbackService.rollback(txn, index);

            assertEquals(3, summary.undoRecordsApplied(), "reverse walk applies all three INSERT undo");
            assertEquals(0, ctx.slots.activeSlotCount());
            MiniTransaction r = ctx.mgr.begin();
            for (int i = 1; i <= 3; i++) {
                assertTrue(svc.lookup(r, index, search(i)).isEmpty(), "row " + i + " removed");
            }
            ctx.mgr.commit(r);
        });
    }

    /**
     * inverse MTR 已提交、progress marker 尚未开始时模拟 crash：持久头与运行期头都必须停在当前 record，
     * row3 虽已被删除，重试仍从 rp3 开始并依靠聚簇 inverse 幂等性完成整条链。
     */
    @Test
    void fullRollbackCrashAfterInverseCommitKeepsOldHeadForIdempotentRetry() {
        onPool(ctx -> {
            ctx.boot();
            RollbackFixture fixture = insertRows(ctx, 3);
            AtomicBoolean crashed = new AtomicBoolean();
            RollbackService crashable = ctx.rollbackService((phase, head) -> {
                if (phase == RollbackProgressPhase.AFTER_INVERSE_COMMIT
                        && crashed.compareAndSet(false, true)) {
                    throw new SimulatedRollbackCrashException("crash after inverse commit");
                }
            });

            assertThrows(SimulatedRollbackCrashException.class,
                    () -> crashable.rollback(fixture.transaction(), fixture.index()));

            UndoLogicalHead originalHead = new UndoLogicalHead(UndoNo.of(3), fixture.pointers().get(2));
            assertEquals(TransactionState.ROLLING_BACK, fixture.transaction().state());
            assertEquals(originalHead, fixture.transaction().undoContext().logicalHead(),
                    "memory head must not move before the marker commits");
            assertEquals(originalHead, readLogicalHead(ctx, fixture.transaction().undoContext().undoFirstPageId()),
                    "persistent head must still point at the already-inversed row3");
            assertEquals(1, ctx.slots.activeSlotCount(), "non-terminal rollback keeps the undo slot");
            assertRowsPresent(ctx, fixture.index(), true, true, false);

            RollbackSummary retried = crashable.rollback(fixture.transaction(), fixture.index());

            assertEquals(3, retried.undoRecordsApplied(),
                    "retry revisits row3 then continues row2 -> row1");
            assertEquals(TransactionState.ROLLED_BACK, fixture.transaction().state());
            assertEquals(0, ctx.slots.activeSlotCount());
            assertRowsPresent(ctx, fixture.index(), false, false, false);
        });
    }

    /**
     * 第一条 progress marker 已提交后模拟 crash：持久头与 UndoContext 必须同步前进到 rp2，重试只处理剩余两条。
     */
    @Test
    void fullRollbackCrashAfterProgressCommitResumesFromPersistedPredecessor() {
        onPool(ctx -> {
            ctx.boot();
            RollbackFixture fixture = insertRows(ctx, 3);
            AtomicBoolean crashed = new AtomicBoolean();
            RollbackService crashable = ctx.rollbackService((phase, head) -> {
                if (phase == RollbackProgressPhase.AFTER_PROGRESS_COMMIT
                        && crashed.compareAndSet(false, true)) {
                    throw new SimulatedRollbackCrashException("crash after progress commit");
                }
            });

            assertThrows(SimulatedRollbackCrashException.class,
                    () -> crashable.rollback(fixture.transaction(), fixture.index()));

            UndoLogicalHead progressed = new UndoLogicalHead(UndoNo.of(2), fixture.pointers().get(1));
            assertEquals(TransactionState.ROLLING_BACK, fixture.transaction().state());
            assertEquals(progressed, fixture.transaction().undoContext().logicalHead());
            assertEquals(progressed, readLogicalHead(ctx, fixture.transaction().undoContext().undoFirstPageId()));
            assertEquals(1, ctx.slots.activeSlotCount());
            assertRowsPresent(ctx, fixture.index(), true, true, false);

            RollbackSummary retried = crashable.rollback(fixture.transaction(), fixture.index());

            assertEquals(2, retried.undoRecordsApplied(), "persisted rp2 boundary skips row3 on retry");
            assertEquals(TransactionState.ROLLED_BACK, fixture.transaction().state());
            assertRowsPresent(ctx, fixture.index(), false, false, false);
        });
    }

    /** 最后一条 marker 已持久为空头时 crash，重试不得重放任何 inverse，只完成 diagnostic/slot/事务终态收尾。 */
    @Test
    void fullRollbackCrashAfterEmptyProgressOnlyRetriesTerminalFinalization() {
        onPool(ctx -> {
            ctx.boot();
            RollbackFixture fixture = insertRows(ctx, 1);
            AtomicBoolean crashed = new AtomicBoolean();
            RollbackService crashable = ctx.rollbackService((phase, head) -> {
                if (phase == RollbackProgressPhase.AFTER_PROGRESS_COMMIT && head.isEmpty()
                        && crashed.compareAndSet(false, true)) {
                    throw new SimulatedRollbackCrashException("crash after empty progress");
                }
            });

            assertThrows(SimulatedRollbackCrashException.class,
                    () -> crashable.rollback(fixture.transaction(), fixture.index()));

            assertEquals(TransactionState.ROLLING_BACK, fixture.transaction().state());
            assertEquals(UndoLogicalHead.EMPTY, fixture.transaction().undoContext().logicalHead());
            assertEquals(UndoLogicalHead.EMPTY,
                    readLogicalHead(ctx, fixture.transaction().undoContext().undoFirstPageId()));
            assertEquals(1, ctx.slots.activeSlotCount(), "terminal resources wait until rollback finalization");

            RollbackSummary retried = crashable.rollback(fixture.transaction(), fixture.index());

            assertEquals(0, retried.undoRecordsApplied());
            assertEquals(TransactionState.ROLLED_BACK, fixture.transaction().state());
            assertEquals(0, ctx.slots.activeSlotCount());
        });
    }

    /** recovery 没有 live Transaction；一次 marker 后失败，再构造 service 必须从持久 rp2 而非旧 rp3 继续。 */
    @Test
    void recoveredRollbackResumesFromPerRecordPersistentProgress() {
        onPool(ctx -> {
            ctx.boot();
            RollbackFixture fixture = insertRows(ctx, 3);
            AtomicBoolean crashed = new AtomicBoolean();
            RollbackService crashable = ctx.rollbackService((phase, head) -> {
                if (phase == RollbackProgressPhase.AFTER_PROGRESS_COMMIT
                        && crashed.compareAndSet(false, true)) {
                    throw new SimulatedRollbackCrashException("recovery crash after progress commit");
                }
            });
            UndoContext undoContext = fixture.transaction().undoContext();
            PageId firstPageId = undoContext.undoFirstPageId();

            assertThrows(SimulatedRollbackCrashException.class,
                    () -> crashable.rollbackRecovered(undoContext.slotId(), firstPageId,
                            fixture.transaction().transactionId(), fixture.index()));
            assertEquals(new UndoLogicalHead(UndoNo.of(2), fixture.pointers().get(1)),
                    readLogicalHead(ctx, firstPageId));

            RollbackSummary resumed = ctx.rollbackService.rollbackRecovered(undoContext.slotId(), firstPageId,
                    fixture.transaction().transactionId(), fixture.index());

            assertEquals(2, resumed.undoRecordsApplied());
            assertFalse(ctx.slots.isOccupied(undoContext.slotId()),
                    "recovery rollback finalization releases the recovered slot");
            assertRowsPresent(ctx, fixture.index(), false, false, false);
            assertTrue(ctx.mgr.redoLogManager().bufferedRecords().stream()
                            .anyMatch(record -> record instanceof TransactionStateDeltaRecord delta
                                    && delta.transactionId().equals(fixture.transaction().transactionId())
                                    && delta.fromState() == TransactionStateDeltaState.ACTIVE
                                    && delta.toState() == TransactionStateDeltaState.ROLLED_BACK
                                    && delta.transactionNo().isNone()
                                    && delta.reason() == TransactionStateDeltaReason.RECOVERY_ROLLBACK),
                    "recovery finalization MTR must retain terminal transaction-id evidence after page3 clear");
        });
    }

    /** recovery 终结器必须在任何 FSP 写入前拒绝仍有逻辑 undo 的段，避免提前回收尚未应用的 inverse。 */
    @Test
    void recoveredFinalizationRejectsNonEmptyLogicalHeadBeforeDrop() {
        onPool(ctx -> {
            ctx.boot();
            RollbackFixture fixture = insertRows(ctx, 1);
            UndoContext undoContext = fixture.transaction().undoContext();

            assertThrows(UndoLogFormatException.class,
                    () -> ctx.finalization.finalizer().finalizeRecoveredRollback(
                            undoContext.slotId(), undoContext.undoFirstPageId(),
                            fixture.transaction().transactionId()));

            assertTrue(ctx.slots.isOccupied(undoContext.slotId()),
                    "preflight refusal keeps the in-memory recovery owner");
            assertEquals(new UndoLogicalHead(UndoNo.of(1), fixture.pointers().getFirst()),
                    readLogicalHead(ctx, undoContext.undoFirstPageId()),
                    "preflight refusal cannot move or drop the persisted logical chain");
        });
    }

    @Test
    void rollbackToSavepointRemovesOnlyLaterInsertAndKeepsTransactionActive() {
        onPool(ctx -> {
            ctx.boot();
            SplitCapableBTreeIndexService svc = ctx.service();
            BTreeIndex index = ctx.clusteredIndex();

            Transaction txn = ctx.txnMgr.begin(TransactionOptions.defaults());
            TransactionId wid = ctx.txnMgr.assignWriteId(txn);
            MiniTransaction first = ctx.mgr.begin();
            RollPointer rp1 = ctx.undoMgr.beforeInsert(txn, first, TABLE_ID, INDEX_ID,
                    key(1), index.keyDef(), index.schema());
            svc.insertClustered(first, index, row(1), wid, rp1);
            ctx.mgr.commit(first);
            TransactionSavepoint savepoint = ctx.rollbackService.createSavepoint(txn);

            MiniTransaction second = ctx.mgr.begin();
            RollPointer rp2 = ctx.undoMgr.beforeInsert(txn, second, TABLE_ID, INDEX_ID,
                    key(2), index.keyDef(), index.schema());
            svc.insertClustered(second, index, row(2), wid, rp2);
            ctx.mgr.commit(second);

            RollbackSummary summary = ctx.rollbackService.rollbackToSavepoint(txn, index, savepoint);

            assertEquals(1, summary.undoRecordsApplied(), "only undo after the savepoint is applied");
            MiniTransaction headRead = ctx.mgr.begin();
            UndoLogicalHead persisted = ctx.undoAccess.open(
                    headRead, txn.undoContext().undoFirstPageId(),
                    cn.zhangyis.db.storage.buf.PageLatchMode.SHARED).logicalHead();
            ctx.mgr.commit(headRead);
            assertEquals(new UndoLogicalHead(UndoNo.of(1), rp1), persisted,
                    "partial rollback must persist its boundary before moving the in-memory head");
            assertEquals(TransactionState.ACTIVE, txn.state(), "savepoint rollback must not finish the transaction");
            assertEquals(1, ctx.slots.activeSlotCount(), "partial rollback keeps the undo slot owned by the transaction");
            assertFalse(ctx.mgr.redoLogManager().bufferedRecords().stream()
                            .anyMatch(record -> record instanceof TransactionStateDeltaRecord),
                    "partial rollback must not emit transaction terminal-state redo");
            MiniTransaction r = ctx.mgr.begin();
            assertTrue(svc.lookup(r, index, search(1)).isPresent(), "row before savepoint remains visible");
            assertTrue(svc.lookup(r, index, search(2)).isEmpty(), "row after savepoint is removed");
            ctx.mgr.commit(r);
        });
    }

    /** 恢复期必须从持久逻辑头开始，不能再次消费已由 statement rollback 撤销的物理分支。 */
    @Test
    void recoveredRollbackStartsAtPersistedLogicalHead() {
        onPool(ctx -> {
            ctx.boot();
            SplitCapableBTreeIndexService svc = ctx.service();
            BTreeIndex index = ctx.clusteredIndex();
            Transaction txn = ctx.txnMgr.begin(TransactionOptions.defaults());
            TransactionId wid = ctx.txnMgr.assignWriteId(txn);

            MiniTransaction first = ctx.mgr.begin();
            RollPointer rp1 = ctx.undoMgr.beforeInsert(txn, first, TABLE_ID, INDEX_ID,
                    key(1), index.keyDef(), index.schema());
            svc.insertClustered(first, index, row(1), wid, rp1);
            ctx.mgr.commit(first);
            TransactionSavepoint savepoint = ctx.rollbackService.createSavepoint(txn);

            MiniTransaction second = ctx.mgr.begin();
            RollPointer rp2 = ctx.undoMgr.beforeInsert(txn, second, TABLE_ID, INDEX_ID,
                    key(2), index.keyDef(), index.schema());
            svc.insertClustered(second, index, row(2), wid, rp2);
            ctx.mgr.commit(second);
            ctx.rollbackService.rollbackToSavepoint(txn, index, savepoint);

            UndoContext undoContext = txn.undoContext();
            RollbackSummary recovered = ctx.rollbackService.rollbackRecovered(
                    undoContext.slotId(), undoContext.undoFirstPageId(), txn.transactionId(), index);

            assertEquals(1, recovered.undoRecordsApplied(),
                    "recovery must follow rp1 only; detached physical rp2 was already rolled back");
            assertFalse(ctx.slots.isOccupied(undoContext.slotId()),
                    "successful recovery removes the page3/in-memory recovery authority");
            MiniTransaction read = ctx.mgr.begin();
            assertTrue(svc.lookupIncludingDeleted(read, index, search(1)).isEmpty());
            assertTrue(svc.lookupIncludingDeleted(read, index, search(2)).isEmpty());
            ctx.mgr.commit(read);
        });
    }

    /**
     * 模拟 statement inverse 已写盘、logical-head marker 尚未写盘即 crash：rp2 仍是持久头，recovery 会安全地
     * 重做已经删除的 row2（no-op）后继续撤销 rp1，不能因 marker 落后产生错误结果。
     */
    @Test
    void recoveredRollbackHandlesInversePersistedBeforeMarker() {
        onPool(ctx -> {
            ctx.boot();
            SplitCapableBTreeIndexService svc = ctx.service();
            BTreeIndex index = ctx.clusteredIndex();
            Transaction txn = ctx.txnMgr.begin(TransactionOptions.defaults());
            TransactionId wid = ctx.txnMgr.assignWriteId(txn);
            MiniTransaction first = ctx.mgr.begin();
            RollPointer rp1 = ctx.undoMgr.beforeInsert(txn, first, TABLE_ID, INDEX_ID,
                    key(1), index.keyDef(), index.schema());
            svc.insertClustered(first, index, row(1), wid, rp1);
            ctx.mgr.commit(first);
            MiniTransaction second = ctx.mgr.begin();
            RollPointer rp2 = ctx.undoMgr.beforeInsert(txn, second, TABLE_ID, INDEX_ID,
                    key(2), index.keyDef(), index.schema());
            svc.insertClustered(second, index, row(2), wid, rp2);
            ctx.mgr.commit(second);

            // 只执行 rp2 的 inverse，不调用 rollbackToSavepoint，因此 first-page marker 仍停在 rp2。
            MiniTransaction inverseOnly = ctx.mgr.begin();
            svc.deleteClustered(inverseOnly, index, search(2), wid, rp2);
            ctx.mgr.commit(inverseOnly);

            UndoContext undoContext = txn.undoContext();
            RollbackSummary recovered = ctx.rollbackService.rollbackRecovered(
                    undoContext.slotId(), undoContext.undoFirstPageId(), txn.transactionId(), index);

            assertEquals(2, recovered.undoRecordsApplied(), "stale marker makes recovery revisit rp2 then rp1");
            MiniTransaction read = ctx.mgr.begin();
            assertTrue(svc.lookupIncludingDeleted(read, index, search(1)).isEmpty());
            assertTrue(svc.lookupIncludingDeleted(read, index, search(2)).isEmpty());
            ctx.mgr.commit(read);
        });
    }

    /** marker CAS 发现页内头已变化时必须在写 header 前失败，且不能提前移动运行期 context。 */
    @Test
    void stalePersistentHeadRejectsMarkerWithoutMovingUndoContext() {
        onPool(ctx -> {
            ctx.boot();
            SplitCapableBTreeIndexService svc = ctx.service();
            BTreeIndex index = ctx.clusteredIndex();
            Transaction txn = ctx.txnMgr.begin(TransactionOptions.defaults());
            TransactionId wid = ctx.txnMgr.assignWriteId(txn);
            MiniTransaction first = ctx.mgr.begin();
            RollPointer rp1 = ctx.undoMgr.beforeInsert(txn, first, TABLE_ID, INDEX_ID,
                    key(1), index.keyDef(), index.schema());
            svc.insertClustered(first, index, row(1), wid, rp1);
            ctx.mgr.commit(first);
            TransactionSavepoint savepoint = ctx.rollbackService.createSavepoint(txn);
            MiniTransaction second = ctx.mgr.begin();
            RollPointer rp2 = ctx.undoMgr.beforeInsert(txn, second, TABLE_ID, INDEX_ID,
                    key(2), index.keyDef(), index.schema());
            svc.insertClustered(second, index, row(2), wid, rp2);
            ctx.mgr.commit(second);

            // 模拟另一个写者已把 persistent head 改到 rp1，但运行期 context 仍认为 rp2 是旧头。
            MiniTransaction tamper = ctx.mgr.begin();
            ctx.undoAccess.open(tamper, txn.undoContext().undoFirstPageId(),
                            cn.zhangyis.db.storage.buf.PageLatchMode.EXCLUSIVE)
                    .updateLogicalHead(new UndoLogicalHead(UndoNo.of(2), rp2),
                            new UndoLogicalHead(UndoNo.of(1), rp1), index.keyDef(), index.schema());
            ctx.mgr.commit(tamper);

            assertThrows(UndoLogicalHeadConflictException.class,
                    () -> ctx.rollbackService.rollbackToSavepoint(txn, index, savepoint));
            assertEquals(UndoNo.of(2), txn.undoContext().logicalLastUndoNo());
            assertEquals(rp2, txn.undoContext().lastRollPointer(),
                    "marker failure must not publish the target into in-memory context");
        });
    }

    @Test
    void rollbackToSavepointRejectsForeignSavepointBeforeApplyingUndo() {
        onPool(ctx -> {
            ctx.boot();
            SplitCapableBTreeIndexService svc = ctx.service();
            BTreeIndex index = ctx.clusteredIndex();
            Transaction txn = ctx.txnMgr.begin(TransactionOptions.defaults());
            TransactionId wid = ctx.txnMgr.assignWriteId(txn);
            MiniTransaction m = ctx.mgr.begin();
            RollPointer rp = ctx.undoMgr.beforeInsert(txn, m, TABLE_ID, INDEX_ID,
                    key(1), index.keyDef(), index.schema());
            svc.insertClustered(m, index, row(1), wid, rp);
            ctx.mgr.commit(m);
            TransactionSavepoint foreign = new TransactionSavepoint(txn, UndoNo.NONE, RollPointer.NULL, 99);

            assertThrows(DatabaseValidationException.class,
                    () -> ctx.rollbackService.rollbackToSavepoint(txn, index, foreign));

            assertEquals(TransactionState.ACTIVE, txn.state(), "failed partial rollback keeps transaction ACTIVE");
            MiniTransaction r = ctx.mgr.begin();
            assertTrue(svc.lookup(r, index, search(1)).isPresent(),
                    "foreign savepoint must be rejected before any undo record is applied");
            ctx.mgr.commit(r);
        });
    }

    @Test
    void rollbackToSavepointRejectsDetachedBoundaryInsteadOfResurrectingItsPointer() {
        onPool(ctx -> {
            ctx.boot();
            SplitCapableBTreeIndexService svc = ctx.service();
            BTreeIndex index = ctx.clusteredIndex();
            Transaction txn = ctx.txnMgr.begin(TransactionOptions.defaults());
            TransactionId wid = ctx.txnMgr.assignWriteId(txn);
            MiniTransaction first = ctx.mgr.begin();
            RollPointer rp1 = ctx.undoMgr.beforeInsert(txn, first, TABLE_ID, INDEX_ID,
                    key(1), index.keyDef(), index.schema());
            svc.insertClustered(first, index, row(1), wid, rp1);
            ctx.mgr.commit(first);
            MiniTransaction second = ctx.mgr.begin();
            RollPointer rp2 = ctx.undoMgr.beforeInsert(txn, second, TABLE_ID, INDEX_ID,
                    key(2), index.keyDef(), index.schema());
            svc.insertClustered(second, index, row(2), wid, rp2);
            ctx.mgr.commit(second);
            TransactionSavepoint savepoint = txn.undoContext().createSavepoint(txn);

            // 模拟逻辑链损坏：当前入口直接跳到保存点之前，目标 rp2 已不再从链头可达。
            txn.undoContext().setLastRollPointer(rp1);

            assertThrows(DatabaseRuntimeException.class,
                    () -> ctx.rollbackService.rollbackToSavepoint(txn, index, savepoint));
            assertEquals(rp1, txn.undoContext().lastRollPointer(),
                    "unreachable savepoint must not resurrect its detached roll pointer");
            MiniTransaction read = ctx.mgr.begin();
            assertTrue(svc.lookup(read, index, search(1)).isPresent());
            assertTrue(svc.lookup(read, index, search(2)).isPresent());
            ctx.mgr.commit(read);
        });
    }

    /**
     * 保存点指针若从当前逻辑链断开，RollbackService 必须在执行任何反向命令前完成整条边界预检。
     * 本用例构造「新分支记录 -> 保存点之前记录」的损坏链；旧单遍实现会先删除分支行，再发现跳过保存点。
     */
    @Test
    void rollbackToSavepointPreflightsDetachedBoundaryBeforeApplyingNewerUndo() {
        onPool(ctx -> {
            ctx.boot();
            SplitCapableBTreeIndexService svc = ctx.service();
            BTreeIndex index = ctx.clusteredIndex();
            Transaction txn = ctx.txnMgr.begin(TransactionOptions.defaults());
            TransactionId wid = ctx.txnMgr.assignWriteId(txn);

            MiniTransaction first = ctx.mgr.begin();
            RollPointer rp1 = ctx.undoMgr.beforeInsert(txn, first, TABLE_ID, INDEX_ID,
                    key(1), index.keyDef(), index.schema());
            svc.insertClustered(first, index, row(1), wid, rp1);
            ctx.mgr.commit(first);

            MiniTransaction second = ctx.mgr.begin();
            RollPointer rp2 = ctx.undoMgr.beforeInsert(txn, second, TABLE_ID, INDEX_ID,
                    key(2), index.keyDef(), index.schema());
            svc.insertClustered(second, index, row(2), wid, rp2);
            ctx.mgr.commit(second);
            TransactionSavepoint savepoint = ctx.rollbackService.createSavepoint(txn);

            MiniTransaction branch = ctx.mgr.begin();
            RollPointer rp3 = ctx.undoMgr.beforeInsert(txn, branch, TABLE_ID, INDEX_ID,
                    key(3), index.keyDef(), index.schema());
            svc.insertClustered(branch, index, row(3), wid, rp3);
            ctx.mgr.commit(branch);
            // append 入口会拒绝错误 predecessor；这里显式破坏落盘 payload，构造 rp3 -> rp1、跳过保存点 rp2。
            ctx.rewriteUndoPredecessor(rp3, rp1);

            assertThrows(DatabaseRuntimeException.class,
                    () -> ctx.rollbackService.rollbackToSavepoint(txn, index, savepoint));

            MiniTransaction read = ctx.mgr.begin();
            assertTrue(svc.lookup(read, index, search(1)).isPresent());
            assertTrue(svc.lookup(read, index, search(2)).isPresent());
            assertTrue(svc.lookup(read, index, search(3)).isPresent(),
                    "boundary validation must finish before any newer undo is applied");
            ctx.mgr.commit(read);
        });
    }

    /**
     * 边界预扫描必须逐 pointer 使用短只读 MTR。构建阶段把 10 条大 UPDATE undo 刷到多张页，随后用 4-frame
     * Buffer Pool 重开；若一个 MTR 固定整条页链，扫描到第 5 张 undo 页前就会耗尽 frame。
     */
    @Test
    void emptyBoundaryRollbackScansMoreUndoPagesThanBufferCapacity() {
        Path dataPath = dir.resolve("small-pool-data.ibd");
        Path undoPath = dir.resolve("small-pool-undo.ibu");
        MultiPageRollbackFixture fixture = buildMultiPageRollbackFixture(dataPath, undoPath);

        try (PageStore store = new FileChannelPageStore();
             BufferPool pool = new LruBufferPool(store, PS, 4)) {
            MiniTransactionManager mgr = new MiniTransactionManager();
            DiskSpaceManager disk = new DiskSpaceManager(pool, store, PS);
            disk.openTablespace(DATA_SPACE, dataPath);
            disk.openTablespace(UNDO_SPACE, undoPath);
            IndexPageAccess pageAccess = new IndexPageAccess(pool, PS);
            UndoLogSegmentAccess undoAccess = new UndoLogSegmentAccess(
                    pool, PS, new DiskSpaceUndoAllocator(disk), registry);
            RollbackSegmentSlotManager slots = new RollbackSegmentSlotManager(RollbackSegmentId.of(0), 64);
            TransactionManager txnMgr = new TransactionManager(new TransactionSystem());
            UndoFinalizationTestSupport.Components finalization = UndoFinalizationTestSupport.create(
                    mgr, pool, PS, undoAccess, new DiskSpaceUndoAllocator(disk), slots);
            RollbackService rollback = new RollbackService(
                    new SplitCapableBTreeIndexService(pageAccess, disk, registry),
                    undoAccess, txnMgr, mgr, finalization.finalizer());
            Transaction txn = txnMgr.begin(TransactionOptions.defaults());
            EmptyUndoBoundary boundary = rollback.createEmptyStatementBoundary(txn);
            UndoSlotId slot = fixture.slotId();
            slots.restore(slot, fixture.undoFirstPageId());
            UndoContext restored = new UndoContext(slots.rollbackSegmentId(), slot, fixture.undoFirstPageId());
            restored.setLastUndoNo(fixture.lastUndoNo());
            restored.setLastRollPointer(fixture.lastRollPointer());
            restored.markHasUpdateUndo();
            txn.setUndoContext(restored);

            RollbackSummary summary = rollback.rollbackToEmptyStatementBoundary(txn, fixture.index(), boundary);

            assertEquals(10, summary.undoRecordsApplied());
            assertTrue(restored.lastRollPointer().isNull());
        }
    }

    /** recovery 也必须逐 pointer 短读；旧物理全链 MTR 在 4-frame pool 扫到第五张 undo 页前会耗尽。 */
    @Test
    void recoveredRollbackScansMoreUndoPagesThanBufferCapacityAfterReopen() {
        Path dataPath = dir.resolve("recovery-small-pool-data.ibd");
        Path undoPath = dir.resolve("recovery-small-pool-undo.ibu");
        MultiPageRollbackFixture fixture = buildMultiPageRollbackFixture(dataPath, undoPath);

        try (PageStore store = new FileChannelPageStore();
             BufferPool pool = new LruBufferPool(store, PS, 4)) {
            MiniTransactionManager mgr = new MiniTransactionManager();
            DiskSpaceManager disk = new DiskSpaceManager(pool, store, PS);
            disk.openTablespace(DATA_SPACE, dataPath);
            disk.openTablespace(UNDO_SPACE, undoPath);
            IndexPageAccess pageAccess = new IndexPageAccess(pool, PS);
            DiskSpaceUndoAllocator allocator = new DiskSpaceUndoAllocator(disk);
            UndoLogSegmentAccess undoAccess = new UndoLogSegmentAccess(pool, PS, allocator, registry);
            RollbackSegmentSlotManager slots = new RollbackSegmentSlotManager(RollbackSegmentId.of(0), 64);
            slots.restore(fixture.slotId(), fixture.undoFirstPageId());
            UndoFinalizationTestSupport.Components finalization = UndoFinalizationTestSupport.create(
                    mgr, pool, PS, undoAccess, allocator, slots);
            RollbackService rollback = new RollbackService(
                    new SplitCapableBTreeIndexService(pageAccess, disk, registry), undoAccess,
                    new TransactionManager(new TransactionSystem()), mgr, finalization.finalizer());

            RollbackSummary summary = rollback.rollbackRecovered(fixture.slotId(), fixture.undoFirstPageId(),
                    fixture.creatorTrxId(), fixture.index());

            assertEquals(10, summary.undoRecordsApplied());
        }
    }

    @Test
    void emptyStatementBoundaryIsAnOwnedOneShotCapability() {
        onPool(ctx -> {
            ctx.boot();
            SplitCapableBTreeIndexService svc = ctx.service();
            BTreeIndex index = ctx.clusteredIndex();
            Transaction txn = ctx.txnMgr.begin(TransactionOptions.defaults());
            EmptyUndoBoundary boundary = ctx.rollbackService.createEmptyStatementBoundary(txn);
            TransactionId wid = ctx.txnMgr.assignWriteId(txn);
            MiniTransaction insert = ctx.mgr.begin();
            RollPointer rp = ctx.undoMgr.beforeInsert(txn, insert, TABLE_ID, INDEX_ID,
                    key(1), index.keyDef(), index.schema());
            svc.insertClustered(insert, index, row(1), wid, rp);
            ctx.mgr.commit(insert);

            assertThrows(TransactionStateException.class,
                    () -> ctx.rollbackService.createEmptyStatementBoundary(txn),
                    "an empty boundary cannot be minted after the transaction has started writing undo");
            RollbackSummary summary = ctx.rollbackService.rollbackToEmptyStatementBoundary(
                    txn, index, boundary);

            assertEquals(1, summary.undoRecordsApplied());
            assertEquals(TransactionState.ACTIVE, txn.state());
            MiniTransaction headRead = ctx.mgr.begin();
            UndoLogicalHead persisted = ctx.undoAccess.open(
                    headRead, txn.undoContext().undoFirstPageId(),
                    cn.zhangyis.db.storage.buf.PageLatchMode.SHARED).logicalHead();
            ctx.mgr.commit(headRead);
            assertEquals(UndoLogicalHead.EMPTY, persisted,
                    "empty statement rollback must persist an empty logical chain head");
            assertThrows(TransactionStateException.class,
                    () -> ctx.rollbackService.rollbackToEmptyStatementBoundary(txn, index, boundary),
                    "the same empty-boundary capability cannot be reused for later writes");
        });
    }

    @Test
    void rollbackToLatestSavepointIsNoopAndDoesNotWriteTransactionStateRedo() {
        onPool(ctx -> {
            ctx.boot();
            SplitCapableBTreeIndexService svc = ctx.service();
            BTreeIndex index = ctx.clusteredIndex();
            Transaction txn = ctx.txnMgr.begin(TransactionOptions.defaults());
            TransactionId wid = ctx.txnMgr.assignWriteId(txn);
            MiniTransaction m = ctx.mgr.begin();
            RollPointer rp = ctx.undoMgr.beforeInsert(txn, m, TABLE_ID, INDEX_ID,
                    key(1), index.keyDef(), index.schema());
            svc.insertClustered(m, index, row(1), wid, rp);
            ctx.mgr.commit(m);
            TransactionSavepoint savepoint = txn.undoContext().createSavepoint(txn);

            RollbackSummary summary = ctx.rollbackService.rollbackToSavepoint(txn, index, savepoint);

            assertEquals(0, summary.undoRecordsApplied());
            assertEquals(TransactionState.ACTIVE, txn.state());
            assertFalse(ctx.mgr.redoLogManager().bufferedRecords().stream()
                            .anyMatch(record -> record instanceof TransactionStateDeltaRecord),
                    "no-op savepoint rollback must not emit terminal transaction redo");
            MiniTransaction r = ctx.mgr.begin();
            assertTrue(svc.lookup(r, index, search(1)).isPresent());
            ctx.mgr.commit(r);
        });
    }

    @Test
    void rollbackToSavepointRestoresUpdatedRowAndNextUndoNoDoesNotReuseRolledBackRecord() {
        onPool(ctx -> {
            ctx.boot();
            SplitCapableBTreeIndexService svc = ctx.service();
            BTreeIndex index = ctx.clusteredIndex();
            Transaction txn = ctx.txnMgr.begin(TransactionOptions.defaults());
            TransactionId wid = ctx.txnMgr.assignWriteId(txn);
            MiniTransaction insert = ctx.mgr.begin();
            RollPointer insRp = ctx.undoMgr.beforeInsert(txn, insert, TABLE_ID, INDEX_ID,
                    key(1), index.keyDef(), index.schema());
            svc.insertClustered(insert, index, row(1), wid, insRp);
            ctx.mgr.commit(insert);
            TransactionSavepoint savepoint = txn.undoContext().createSavepoint(txn);
            updateRow(ctx, svc, index, txn, wid, 1, "v2");

            RollbackSummary summary = ctx.rollbackService.rollbackToSavepoint(txn, index, savepoint);

            assertEquals(1, summary.undoRecordsApplied());
            MiniTransaction chk = ctx.mgr.begin();
            assertEquals("payload-1", payloadOf(svc.lookup(chk, index, search(1)).orElseThrow()));
            ctx.mgr.commit(chk);
            assertEquals(UndoNo.of(2), txn.undoContext().lastUndoNo(),
                    "rolled-back update undo remains part of append history");
            assertEquals(UndoNo.of(1), txn.undoContext().logicalLastUndoNo(),
                    "logical chain returns to the insert boundary");

            updateRow(ctx, svc, index, txn, wid, 1, "v3");

            assertEquals(UndoNo.of(3), txn.undoContext().lastUndoNo(),
                    "next write must allocate a fresh undoNo instead of reusing the rolled-back update undoNo");
            assertEquals(UndoNo.of(3), txn.undoContext().logicalLastUndoNo());
        });
    }

    @Test
    void rollbackToSavepointRestoresDeleteMarkedRow() {
        onPool(ctx -> {
            ctx.boot();
            SplitCapableBTreeIndexService svc = ctx.service();
            BTreeIndex index = ctx.clusteredIndex();
            Transaction txn = ctx.txnMgr.begin(TransactionOptions.defaults());
            TransactionId wid = ctx.txnMgr.assignWriteId(txn);
            MiniTransaction insert = ctx.mgr.begin();
            RollPointer insRp = ctx.undoMgr.beforeInsert(txn, insert, TABLE_ID, INDEX_ID,
                    key(1), index.keyDef(), index.schema());
            svc.insertClustered(insert, index, row(1), wid, insRp);
            ctx.mgr.commit(insert);
            TransactionSavepoint savepoint = txn.undoContext().createSavepoint(txn);
            deleteMarkRow(ctx, svc, index, txn, wid, 1);

            RollbackSummary summary = ctx.rollbackService.rollbackToSavepoint(txn, index, savepoint);

            assertEquals(1, summary.undoRecordsApplied());
            assertEquals(TransactionState.ACTIVE, txn.state());
            MiniTransaction r = ctx.mgr.begin();
            BTreeLookupResult found = svc.lookup(r, index, search(1)).orElseThrow();
            ctx.mgr.commit(r);
            assertEquals("payload-1", payloadOf(found), "delete-mark after savepoint is undone");
            assertEquals(insRp, found.record().hiddenColumns().dbRollPtr(),
                    "hidden columns return to the savepoint-era version chain head");
        });
    }

    @Test
    void fullRollbackAfterPartialRollbackWalksOnlyCurrentLogicalChain() {
        onPool(ctx -> {
            ctx.boot();
            SplitCapableBTreeIndexService svc = ctx.service();
            BTreeIndex index = ctx.clusteredIndex();
            Transaction txn = ctx.txnMgr.begin(TransactionOptions.defaults());
            TransactionId wid = ctx.txnMgr.assignWriteId(txn);
            MiniTransaction first = ctx.mgr.begin();
            RollPointer rp1 = ctx.undoMgr.beforeInsert(txn, first, TABLE_ID, INDEX_ID,
                    key(1), index.keyDef(), index.schema());
            svc.insertClustered(first, index, row(1), wid, rp1);
            ctx.mgr.commit(first);
            TransactionSavepoint savepoint = txn.undoContext().createSavepoint(txn);
            MiniTransaction second = ctx.mgr.begin();
            RollPointer rp2 = ctx.undoMgr.beforeInsert(txn, second, TABLE_ID, INDEX_ID,
                    key(2), index.keyDef(), index.schema());
            svc.insertClustered(second, index, row(2), wid, rp2);
            ctx.mgr.commit(second);
            ctx.rollbackService.rollbackToSavepoint(txn, index, savepoint);
            MiniTransaction third = ctx.mgr.begin();
            RollPointer rp3 = ctx.undoMgr.beforeInsert(txn, third, TABLE_ID, INDEX_ID,
                    key(3), index.keyDef(), index.schema());
            svc.insertClustered(third, index, row(3), wid, rp3);
            ctx.mgr.commit(third);

            MiniTransaction chainRead = ctx.mgr.begin();
            UndoLogSegment persisted = ctx.undoAccess.open(
                    chainRead, txn.undoContext().undoFirstPageId(),
                    cn.zhangyis.db.storage.buf.PageLatchMode.SHARED);
            assertEquals(new UndoLogicalHead(UndoNo.of(3), rp3), persisted.logicalHead());
            assertEquals(rp1, persisted.readRecord(rp3, index.keyDef(), index.schema()).prevRollPointer(),
                    "new append must reconnect to the persisted rollback boundary, not detached rp2");
            assertEquals(UndoNo.of(3), persisted.logLastUndoNo(), "physical undoNo high-water never rewinds");
            ctx.mgr.commit(chainRead);

            RollbackSummary summary = ctx.rollbackService.rollback(txn, index);

            assertEquals(2, summary.undoRecordsApplied(),
                    "full rollback walks row3 -> row1; row2 was detached by partial rollback");
            MiniTransaction r = ctx.mgr.begin();
            assertTrue(svc.lookupIncludingDeleted(r, index, search(1)).isEmpty());
            assertTrue(svc.lookupIncludingDeleted(r, index, search(2)).isEmpty());
            assertTrue(svc.lookupIncludingDeleted(r, index, search(3)).isEmpty());
            ctx.mgr.commit(r);
        });
    }

    /**
     * savepoint 回退 rp2 后再 append 的 rp3 直接指向 rp1。逐条 full rollback marker 必须读取真实前驱 undoNo，
     * 第一条提交后持久头应为 (1,rp1)，不能按 3-1 猜成已经 detached 的 undoNo 2。
     */
    @Test
    void fullRollbackProgressUsesRealPredecessorAcrossDetachedUndoNumberGap() {
        onPool(ctx -> {
            ctx.boot();
            SplitCapableBTreeIndexService svc = ctx.service();
            BTreeIndex index = ctx.clusteredIndex();
            Transaction txn = ctx.txnMgr.begin(TransactionOptions.defaults());
            TransactionId wid = ctx.txnMgr.assignWriteId(txn);
            MiniTransaction first = ctx.mgr.begin();
            RollPointer rp1 = ctx.undoMgr.beforeInsert(txn, first, TABLE_ID, INDEX_ID,
                    key(1), index.keyDef(), index.schema());
            svc.insertClustered(first, index, row(1), wid, rp1);
            ctx.mgr.commit(first);
            TransactionSavepoint savepoint = txn.undoContext().createSavepoint(txn);
            MiniTransaction second = ctx.mgr.begin();
            RollPointer rp2 = ctx.undoMgr.beforeInsert(txn, second, TABLE_ID, INDEX_ID,
                    key(2), index.keyDef(), index.schema());
            svc.insertClustered(second, index, row(2), wid, rp2);
            ctx.mgr.commit(second);
            ctx.rollbackService.rollbackToSavepoint(txn, index, savepoint);
            MiniTransaction third = ctx.mgr.begin();
            RollPointer rp3 = ctx.undoMgr.beforeInsert(txn, third, TABLE_ID, INDEX_ID,
                    key(3), index.keyDef(), index.schema());
            svc.insertClustered(third, index, row(3), wid, rp3);
            ctx.mgr.commit(third);

            AtomicBoolean crashed = new AtomicBoolean();
            RollbackService crashable = ctx.rollbackService((phase, head) -> {
                if (phase == RollbackProgressPhase.AFTER_PROGRESS_COMMIT
                        && crashed.compareAndSet(false, true)) {
                    throw new SimulatedRollbackCrashException("crash after branch progress");
                }
            });
            assertThrows(SimulatedRollbackCrashException.class, () -> crashable.rollback(txn, index));

            UndoLogicalHead expected = new UndoLogicalHead(UndoNo.of(1), rp1);
            assertEquals(expected, txn.undoContext().logicalHead());
            assertEquals(expected, readLogicalHead(ctx, txn.undoContext().undoFirstPageId()));
            assertEquals(UndoNo.of(3), txn.undoContext().lastUndoNo(),
                    "physical append high-water must not rewind with rollback progress");

            RollbackSummary resumed = crashable.rollback(txn, index);
            assertEquals(1, resumed.undoRecordsApplied(), "only rp1 remains after rp3 marker commits");
            assertEquals(TransactionState.ROLLED_BACK, txn.state());
        });
    }

    @Test
    void orphanUndoRollbackIsIdempotent() {
        onPool(ctx -> {
            ctx.boot();
            SplitCapableBTreeIndexService svc = ctx.service();
            BTreeIndex index = ctx.clusteredIndex();

            Transaction txn = ctx.txnMgr.begin(TransactionOptions.defaults());
            ctx.txnMgr.assignWriteId(txn);
            // 只写 undo，不写聚簇行：模拟「失败插入」留下的 orphan undo
            MiniTransaction m = ctx.mgr.begin();
            ctx.undoMgr.beforeInsert(txn, m, TABLE_ID, INDEX_ID, key(1), index.keyDef(), index.schema());
            ctx.mgr.commit(m);
            assertEquals(1, ctx.slots.activeSlotCount());

            // rollback 走链：deleteClustered 找不到对应行 → no-op，不抛
            RollbackSummary summary = ctx.rollbackService.rollback(txn, index);

            assertEquals(1, summary.undoRecordsApplied(), "orphan undo still consumed (idempotent no-op delete)");
            assertEquals(TransactionState.ROLLED_BACK, txn.state());
            assertEquals(0, ctx.slots.activeSlotCount(), "slot released even when no row existed");
        });
    }

    @Test
    void readOnlyOrUnwrittenTxnRollbackJustFlipsState() {
        onPool(ctx -> {
            ctx.boot();
            BTreeIndex index = ctx.clusteredIndex();
            Transaction txn = ctx.txnMgr.begin(TransactionOptions.defaults());
            // 未写入：undoContext 为 null

            RollbackSummary summary = ctx.rollbackService.rollback(txn, index);

            assertEquals(0, summary.undoRecordsApplied(), "no undo to apply");
            assertEquals(TransactionState.ROLLED_BACK, txn.state());
            assertEquals(0, ctx.slots.activeSlotCount(), "no slot touched");
        });
    }

    @Test
    void insertThenUpdateRollbackRemovesRowReversingBoth() {
        onPool(ctx -> {
            ctx.boot();
            SplitCapableBTreeIndexService svc = ctx.service();
            BTreeIndex index = ctx.clusteredIndex();
            Transaction txn = ctx.txnMgr.begin(TransactionOptions.defaults());
            TransactionId wid = ctx.txnMgr.assignWriteId(txn);
            MiniTransaction m = ctx.mgr.begin();
            RollPointer insRp = ctx.undoMgr.beforeInsert(txn, m, TABLE_ID, INDEX_ID, key(1), index.keyDef(), index.schema());
            svc.insertClustered(m, index, row(1), wid, insRp);
            ctx.mgr.commit(m);
            updateRow(ctx, svc, index, txn, wid, 1, "v2");
            MiniTransaction chk = ctx.mgr.begin();
            assertEquals("v2", payloadOf(svc.lookup(chk, index, search(1)).orElseThrow()));
            ctx.mgr.commit(chk);

            RollbackSummary summary = ctx.rollbackService.rollback(txn, index);

            assertEquals(2, summary.undoRecordsApplied(), "update + insert undo both reversed");
            assertEquals(TransactionState.ROLLED_BACK, txn.state());
            MiniTransaction r = ctx.mgr.begin();
            assertTrue(svc.lookup(r, index, search(1)).isEmpty(), "row gone: update restored then insert deleted");
            ctx.mgr.commit(r);
        });
    }

    @Test
    void sameRowDoubleUpdateRollbackChainRestoresThenDeletes() {
        onPool(ctx -> {
            ctx.boot();
            SplitCapableBTreeIndexService svc = ctx.service();
            BTreeIndex index = ctx.clusteredIndex();
            Transaction txn = ctx.txnMgr.begin(TransactionOptions.defaults());
            TransactionId wid = ctx.txnMgr.assignWriteId(txn);
            MiniTransaction m = ctx.mgr.begin();
            RollPointer insRp = ctx.undoMgr.beforeInsert(txn, m, TABLE_ID, INDEX_ID, key(1), index.keyDef(), index.schema());
            svc.insertClustered(m, index, row(1), wid, insRp); // payload-1
            ctx.mgr.commit(m);
            updateRow(ctx, svc, index, txn, wid, 1, "v2");
            updateRow(ctx, svc, index, txn, wid, 1, "v3");
            MiniTransaction chk = ctx.mgr.begin();
            assertEquals("v3", payloadOf(svc.lookup(chk, index, search(1)).orElseThrow()));
            ctx.mgr.commit(chk);

            // 反向走链：撤销 v3 update(恢复 v2)→撤销 v2 update(恢复 payload-1)→撤销 insert(物理删)
            RollbackSummary summary = ctx.rollbackService.rollback(txn, index);

            assertEquals(3, summary.undoRecordsApplied(), "two updates + insert reversed in order");
            MiniTransaction r = ctx.mgr.begin();
            assertTrue(svc.lookup(r, index, search(1)).isEmpty(), "fully rolled back to non-existence");
            ctx.mgr.commit(r);
        });
    }

    @Test
    void deleteMarkThenRollbackRestoresLiveRow() {
        onPool(ctx -> {
            ctx.boot();
            SplitCapableBTreeIndexService svc = ctx.service();
            BTreeIndex index = ctx.clusteredIndex();
            // T1 插入并提交（存活、已提交）
            Transaction t1 = ctx.txnMgr.begin(TransactionOptions.defaults());
            TransactionId w1 = ctx.txnMgr.assignWriteId(t1);
            MiniTransaction m = ctx.mgr.begin();
            RollPointer insRp = ctx.undoMgr.beforeInsert(t1, m, TABLE_ID, INDEX_ID, key(1), index.keyDef(), index.schema());
            svc.insertClustered(m, index, row(1), w1, insRp);
            ctx.mgr.commit(m);
            ctx.txnMgr.prepareCommit(t1);
            ctx.undoMgr.onCommit(t1);
            ctx.txnMgr.commit(t1);

            // T2 delete-mark
            Transaction t2 = ctx.txnMgr.begin(TransactionOptions.defaults());
            TransactionId w2 = ctx.txnMgr.assignWriteId(t2);
            deleteMarkRow(ctx, svc, index, t2, w2, 1);
            MiniTransaction chk = ctx.mgr.begin();
            assertTrue(svc.lookup(chk, index, search(1)).isEmpty(), "delete-marked row filtered from lookup");
            ctx.mgr.commit(chk);

            // rollback T2 → 取消标记，行复活
            RollbackSummary summary = ctx.rollbackService.rollback(t2, index);

            assertEquals(1, summary.undoRecordsApplied());
            MiniTransaction r = ctx.mgr.begin();
            BTreeLookupResult found = svc.lookup(r, index, search(1)).orElseThrow();
            ctx.mgr.commit(r);
            assertEquals(w1, found.record().hiddenColumns().dbTrxId(), "un-mark 还原存活版本 + 旧隐藏列");
        });
    }

    @Test
    void insertThenDeleteMarkRollbackRemovesRow() {
        onPool(ctx -> {
            ctx.boot();
            SplitCapableBTreeIndexService svc = ctx.service();
            BTreeIndex index = ctx.clusteredIndex();
            Transaction txn = ctx.txnMgr.begin(TransactionOptions.defaults());
            TransactionId wid = ctx.txnMgr.assignWriteId(txn);
            MiniTransaction m = ctx.mgr.begin();
            RollPointer insRp = ctx.undoMgr.beforeInsert(txn, m, TABLE_ID, INDEX_ID, key(1), index.keyDef(), index.schema());
            svc.insertClustered(m, index, row(1), wid, insRp);
            ctx.mgr.commit(m);
            deleteMarkRow(ctx, svc, index, txn, wid, 1); // 同事务内 insert→delete-mark

            // rollback：先取消标记(复活)再删 insert(物理删) → 行彻底消失
            RollbackSummary summary = ctx.rollbackService.rollback(txn, index);

            assertEquals(2, summary.undoRecordsApplied(), "delete-mark + insert undo 反向应用");
            MiniTransaction r = ctx.mgr.begin();
            assertTrue(svc.lookupIncludingDeleted(r, index, search(1)).isEmpty(), "行被物理移除（含 delete-marked 视角也空）");
            ctx.mgr.commit(r);
        });
    }

    // ---- helpers ----

    /** 前向 DELETE-mark 编排（test-wired，§16.3）：读存活当前版本 → beforeDelete 写 DELETE_MARK undo → setClusteredDeleteMark 置删除位。 */
    private void deleteMarkRow(Ctx ctx, SplitCapableBTreeIndexService svc, BTreeIndex index, Transaction txn,
                              TransactionId wid, long id) {
        MiniTransaction read = ctx.mgr.begin();
        BTreeLookupResult old = svc.lookup(read, index, search(id)).orElseThrow();
        ctx.mgr.commit(read);
        HiddenColumns oldHidden = old.record().hiddenColumns();
        MiniTransaction m = ctx.mgr.begin();
        RollPointer delRp = ctx.undoMgr.beforeDelete(txn, m, TABLE_ID, INDEX_ID, key(id),
                old.record().columnValues(), oldHidden, index.keyDef(), index.schema());
        BTreeDeleteMarkResult res = svc.setClusteredDeleteMark(m, index, search(id), true,
                new HiddenColumns(wid, delRp), oldHidden.dbTrxId(), oldHidden.dbRollPtr());
        ctx.mgr.commit(m);
        assertTrue(res.changed(), "delete-mark applied");
    }

    /** 前向 UPDATE 编排（test-wired，§7.3）：读旧 image → beforeUpdate 写 UPDATE undo → replaceClustered 盖新值。 */
    private void updateRow(Ctx ctx, SplitCapableBTreeIndexService svc, BTreeIndex index, Transaction txn,
                           TransactionId wid, long id, String newPayload) {
        MiniTransaction read = ctx.mgr.begin();
        BTreeLookupResult old = svc.lookup(read, index, search(id)).orElseThrow();
        ctx.mgr.commit(read);
        HiddenColumns oldHidden = old.record().hiddenColumns();
        MiniTransaction m = ctx.mgr.begin();
        RollPointer newRp = ctx.undoMgr.beforeUpdate(txn, m, TABLE_ID, INDEX_ID, key(id),
                old.record().columnValues(), oldHidden, index.keyDef(), index.schema());
        svc.replaceClustered(m, index, search(id),
                new LogicalRecord(1, List.of(new ColumnValue.IntValue(id), new ColumnValue.StringValue(newPayload)),
                        false, RecordType.CONVENTIONAL, new HiddenColumns(wid, newRp)),
                oldHidden.dbTrxId(), oldHidden.dbRollPtr());
        ctx.mgr.commit(m);
    }

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

    /** 构造指定长度的连续 INSERT undo 链，并返回事务、最终索引快照及按写入顺序排列的 record pointer。 */
    private RollbackFixture insertRows(Ctx ctx, int count) {
        SplitCapableBTreeIndexService svc = ctx.service();
        BTreeIndex index = ctx.clusteredIndex();
        Transaction txn = ctx.txnMgr.begin(TransactionOptions.defaults());
        TransactionId wid = ctx.txnMgr.assignWriteId(txn);
        java.util.ArrayList<RollPointer> pointers = new java.util.ArrayList<>(count);
        for (int i = 1; i <= count; i++) {
            MiniTransaction write = ctx.mgr.begin();
            RollPointer pointer = ctx.undoMgr.beforeInsert(txn, write, TABLE_ID, INDEX_ID,
                    key(i), index.keyDef(), index.schema());
            BTreeInsertResult inserted = svc.insertClustered(write, index, row(i), wid, pointer);
            index = inserted.indexAfterInsert();
            ctx.mgr.commit(write);
            pointers.add(pointer);
        }
        return new RollbackFixture(txn, index, List.copyOf(pointers));
    }

    /** 读取 first-page 权威 logical head，返回前提交只读 MTR 释放所有 undo latch/fix。 */
    private UndoLogicalHead readLogicalHead(Ctx ctx, PageId firstPageId) {
        MiniTransaction read = ctx.mgr.begin();
        UndoLogicalHead head = ctx.undoAccess.open(read, firstPageId,
                cn.zhangyis.db.storage.buf.PageLatchMode.SHARED).logicalHead();
        ctx.mgr.commit(read);
        return head;
    }

    /** 按 id=1..N 核对当前聚簇行是否存在；一次短读 MTR 内完成后立即释放 read-path latch。 */
    private void assertRowsPresent(Ctx ctx, BTreeIndex index, boolean... expected) {
        MiniTransaction read = ctx.mgr.begin();
        SplitCapableBTreeIndexService svc = ctx.service();
        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], svc.lookup(read, index, search(i + 1L)).isPresent(),
                    "row " + (i + 1) + " presence");
        }
        ctx.mgr.commit(read);
    }

    /** 一组 live rollback 测试定位值；只保存领域快照，不持有 MTR/page guard。 */
    private record RollbackFixture(Transaction transaction, BTreeIndex index, List<RollPointer> pointers) {
    }

    /** fault injector 专用领域异常；代表进程在一个已提交持久边界之后突然停止。 */
    private static final class SimulatedRollbackCrashException extends DatabaseRuntimeException {

        private SimulatedRollbackCrashException(String message) {
            super(message);
        }
    }

    /** 构建并刷出跨越至少五张页的大 undo 链，供小池重开测试验证扫描 latch 边界。 */
    private MultiPageRollbackFixture buildMultiPageRollbackFixture(Path dataPath, Path undoPath) {
        Path redoPath = dir.resolve("small-pool-redo.log");
        try (PageStore store = new FileChannelPageStore();
             BufferPool pool = new LruBufferPool(store, PS, 64);
             RedoLogFileRepository repo = RedoLogFileRepository.open(redoPath)) {
            RedoLogManager redo = RedoLogManager.durable(repo);
            MiniTransactionManager mgr = new MiniTransactionManager(
                    new TablespaceAccessController(), redo);
            DiskSpaceManager disk = new DiskSpaceManager(pool, store, PS);
            IndexPageAccess pageAccess = new IndexPageAccess(pool, PS);
            DiskSpaceUndoAllocator allocator = new DiskSpaceUndoAllocator(disk);
            UndoLogSegmentAccess undoAccess = new UndoLogSegmentAccess(pool, PS, allocator, registry);
            RollbackSegmentSlotManager slots = new RollbackSegmentSlotManager(RollbackSegmentId.of(0), 64);
            TransactionManager txnMgr = new TransactionManager(new TransactionSystem());
            UndoFinalizationTestSupport.Components finalization = UndoFinalizationTestSupport.create(
                    mgr, pool, PS, undoAccess, allocator, slots);
            UndoLogManager undoMgr = finalization.manager(undoAccess, UNDO_SPACE, new HistoryList(), mgr);

            MiniTransaction boot = mgr.begin();
            disk.createTablespace(boot, DATA_SPACE, dataPath, PageNo.of(64));
            SegmentRef leaf = disk.createSegment(boot, DATA_SPACE, SegmentPurpose.INDEX_LEAF);
            SegmentRef nonLeaf = disk.createSegment(boot, DATA_SPACE, SegmentPurpose.INDEX_NON_LEAF);
            PageId root = disk.allocatePage(boot, leaf);
            pageAccess.createIndexPage(boot, root, INDEX_ID, 0);
            disk.createTablespace(boot, UNDO_SPACE, undoPath, PageNo.of(64));
            finalization.format(boot, UNDO_SPACE);
            mgr.commit(boot);

            TableSchema schema = largeRollbackSchema();
            IndexKeyDef keyDef = idKey();
            BTreeIndex index = new BTreeIndex(INDEX_ID, root, 0, keyDef, schema, true, leaf, nonLeaf);
            Transaction txn = txnMgr.begin(TransactionOptions.defaults());
            TransactionId wid = txnMgr.assignWriteId(txn);
            String largeValue = "x".repeat(7_000);
            for (int i = 1; i <= 10; i++) {
                MiniTransaction write = mgr.begin();
                undoMgr.beforeUpdate(txn, write, TABLE_ID, INDEX_ID, key(i),
                        List.of(new ColumnValue.IntValue(i), new ColumnValue.StringValue(largeValue)),
                        new HiddenColumns(wid, RollPointer.NULL), keyDef, schema);
                mgr.commit(write);
            }
            UndoContext ctx = txn.undoContext();
            redo.flush();
            FlushCoordinator coordinator = new FlushCoordinator(pool, store, redo, PS,
                    new NoDoublewriteStrategy(), Duration.ofMillis(100));
            coordinator.flushList(Lsn.of(Long.MAX_VALUE), pool.capacity());
            return new MultiPageRollbackFixture(
                    index, ctx.slotId(), txn.transactionId(), ctx.undoFirstPageId(),
                    ctx.lastUndoNo(), ctx.lastRollPointer());
        }
    }

    /** 大 old image 让少量 undo record 稳定跨越多张 16KiB undo 页。 */
    private static TableSchema largeRollbackSchema() {
        return new TableSchema(1, List.of(
                new ColumnDef(new ColumnId(0), "id", ColumnType.intType(false, false), 0),
                new ColumnDef(new ColumnId(1), "payload", ColumnType.varchar(8_000, true), 1)), true);
    }

    /** 小池重开所需的持久定位快照；不携带任何 Buffer Pool 句柄。 */
    private record MultiPageRollbackFixture(BTreeIndex index, UndoSlotId slotId, TransactionId creatorTrxId,
                                             PageId undoFirstPageId, UndoNo lastUndoNo,
                                             RollPointer lastRollPointer) {
    }

    private void onPool(Body body) {
        PageStore store = new FileChannelPageStore();
        try (PageStore ignored = store; BufferPool pool = new LruBufferPool(store, PS, 128)) {
            body.run(new Ctx(store, pool));
        }
    }

    private interface Body {
        void run(Ctx ctx);
    }

    private final class Ctx {
        final MiniTransactionManager mgr = new MiniTransactionManager();
        final DiskSpaceManager disk;
        final IndexPageAccess access;
        final DiskSpaceUndoAllocator undoAllocator;
        final UndoLogSegmentAccess undoAccess;
        final RollbackSegmentSlotManager slots;
        final UndoFinalizationTestSupport.Components finalization;
        final UndoLogManager undoMgr;
        final TransactionManager txnMgr = new TransactionManager(new TransactionSystem());
        final RollbackService rollbackService;
        final BufferPool pool;
        private SegmentRef leafSegment;
        private SegmentRef nonLeafSegment;
        private PageId rootPageId;

        private Ctx(PageStore store, BufferPool pool) {
            this.pool = pool;
            this.disk = new DiskSpaceManager(pool, store, PS);
            this.access = new IndexPageAccess(pool, PS);
            this.undoAllocator = new DiskSpaceUndoAllocator(disk);
            this.undoAccess = new UndoLogSegmentAccess(pool, PS, undoAllocator, registry);
            this.slots = new RollbackSegmentSlotManager(RollbackSegmentId.of(0), 64);
            this.finalization = UndoFinalizationTestSupport.create(
                    mgr, pool, PS, undoAccess, undoAllocator, slots);
            this.undoMgr = finalization.manager(undoAccess, UNDO_SPACE, new HistoryList(), mgr);
            this.rollbackService = new RollbackService(
                    service(), undoAccess, txnMgr, mgr, finalization.finalizer());
        }

        /** 仅供损坏链测试改写 record payload 内 predecessor；生产 append 永不允许构造该状态。 */
        private void rewriteUndoPredecessor(RollPointer recordPointer, RollPointer predecessor) {
            MiniTransaction corrupt = mgr.begin();
            corrupt.getPage(pool, PageId.of(UNDO_SPACE, recordPointer.pageNo()),
                            cn.zhangyis.db.storage.buf.PageLatchMode.EXCLUSIVE)
                    .writeBytes(recordPointer.offset() + Short.BYTES + UNDO_PREV_POINTER_IN_PAYLOAD,
                            predecessor.encode());
            mgr.commit(corrupt);
        }

        private SplitCapableBTreeIndexService service() {
            return new SplitCapableBTreeIndexService(access, disk, registry);
        }

        /** 使用同一物理/事务依赖构造带确定性 crash hook 的 rollback service。 */
        private RollbackService rollbackService(RollbackProgressFaultInjector faultInjector) {
            return new RollbackService(
                    service(), undoAccess, txnMgr, mgr, finalization.finalizer(), faultInjector);
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
        }

        private BTreeIndex clusteredIndex() {
            return new BTreeIndex(INDEX_ID, rootPageId, 0, idKey(), clusteredSchema(), true,
                    leafSegment, nonLeafSegment);
        }
    }
}
