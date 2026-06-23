package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.domain.RollbackSegmentId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.UndoNo;
import cn.zhangyis.db.storage.api.DiskSpaceManager;
import cn.zhangyis.db.storage.api.DiskSpaceUndoAllocator;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.fil.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.PageStore;
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
import cn.zhangyis.db.storage.undo.UndoPageOverflowException;
import cn.zhangyis.db.storage.undo.UndoRecord;
import cn.zhangyis.db.storage.undo.UndoRecordType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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

        // build session：建 undo 表空间 + 一个事务一次 insert + commit，然后关闭 store/pool（close 触发 flushAll）
        try (PageStore store = new FileChannelPageStore();
             BufferPool pool = new LruBufferPool(store, PS, 128)) {
            MiniTransactionManager mgr = new MiniTransactionManager();
            DiskSpaceManager disk = new DiskSpaceManager(pool, store, PS);
            DiskSpaceUndoAllocator allocator = new DiskSpaceUndoAllocator(disk);
            UndoLogSegmentAccess access = new UndoLogSegmentAccess(pool, PS, allocator, registry);
            RollbackSegmentSlotManager slots = new RollbackSegmentSlotManager(RollbackSegmentId.of(0), SLOT_CAPACITY);
            UndoLogManager undoMgr = new UndoLogManager(access, slots, UNDO_SPACE);
            TransactionManager txnMgr = new TransactionManager(new TransactionSystem());

            MiniTransaction boot = mgr.begin();
            disk.createTablespace(boot, UNDO_SPACE, path, PageNo.of(64));
            mgr.commit(boot);

            Transaction txn = txnMgr.begin(TransactionOptions.defaults());
            widHolder[0] = txnMgr.assignWriteId(txn);
            MiniTransaction m = mgr.begin();
            holder[0] = undoMgr.beforeInsert(txn, m, TABLE_ID, INDEX_ID, keyOf(700), keyDef(), schema());
            firstPageHolder[0] = txn.undoContext().undoFirstPageId();
            mgr.commit(m);
            txnMgr.commit(txn);
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

            // commit 编排：onCommit 释放 insert undo slot（commit 后不再服务一致性读）
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
            h.undoMgr.onCommit(txn2);
            h.txnMgr.commit(txn2);
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

            h.undoMgr.onCommit(txn);
            h.txnMgr.commit(txn);
            assertEquals(1, h.slots.activeSlotCount(),
                    "slot retained when txn wrote UPDATE undo (T1.4 MVCC/purge needs it)");
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
             BufferPool pool = new LruBufferPool(store, PS, 128)) {
            MiniTransactionManager mgr = new MiniTransactionManager();
            DiskSpaceManager disk = new DiskSpaceManager(pool, store, PS);
            UndoLogSegmentAccess access = new UndoLogSegmentAccess(pool, PS, new DiskSpaceUndoAllocator(disk), registry);
            RollbackSegmentSlotManager slots = new RollbackSegmentSlotManager(RollbackSegmentId.of(0), SLOT_CAPACITY);
            UndoLogManager undoMgr = new UndoLogManager(access, slots, UNDO_SPACE);
            TransactionManager txnMgr = new TransactionManager(new TransactionSystem());

            MiniTransaction boot = mgr.begin();
            disk.createTablespace(boot, UNDO_SPACE, path, PageNo.of(64));
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
        PageStore store = new FileChannelPageStore();
        try (PageStore ignored = store; BufferPool pool = new LruBufferPool(store, PS, 128)) {
            MiniTransactionManager mgr = new MiniTransactionManager();
            DiskSpaceManager disk = new DiskSpaceManager(pool, store, PS);
            DiskSpaceUndoAllocator allocator = new DiskSpaceUndoAllocator(disk);
            UndoLogSegmentAccess access = new UndoLogSegmentAccess(pool, PS, allocator, registry);
            RollbackSegmentSlotManager slots = new RollbackSegmentSlotManager(RollbackSegmentId.of(0), SLOT_CAPACITY);
            UndoLogManager undoMgr = new UndoLogManager(access, slots, UNDO_SPACE);
            TransactionManager txnMgr = new TransactionManager(new TransactionSystem());

            MiniTransaction boot = mgr.begin();
            disk.createTablespace(boot, UNDO_SPACE, dir.resolve("undo.ibu"), PageNo.of(64));
            mgr.commit(boot);

            body.run(new H(mgr, access, slots, undoMgr, txnMgr));
        }
    }

    private static final class H {
        final MiniTransactionManager mgr;
        final UndoLogSegmentAccess access;
        final RollbackSegmentSlotManager slots;
        final UndoLogManager undoMgr;
        final TransactionManager txnMgr;

        H(MiniTransactionManager mgr, UndoLogSegmentAccess access, RollbackSegmentSlotManager slots,
          UndoLogManager undoMgr, TransactionManager txnMgr) {
            this.mgr = mgr;
            this.access = access;
            this.slots = slots;
            this.undoMgr = undoMgr;
            this.txnMgr = txnMgr;
        }
    }
}
