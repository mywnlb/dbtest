package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.domain.RollbackSegmentId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.storage.api.DiskSpaceManager;
import cn.zhangyis.db.storage.api.DiskSpaceUndoAllocator;
import cn.zhangyis.db.storage.api.index.IndexPageAccess;
import cn.zhangyis.db.storage.api.SegmentRef;
import cn.zhangyis.db.storage.btree.BTreeIndex;
import cn.zhangyis.db.storage.btree.BTreeLookupResult;
import cn.zhangyis.db.storage.btree.SplitCapableBTreeIndexService;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.fil.io.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.io.PageStore;
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
import cn.zhangyis.db.storage.undo.UndoLogFormatException;
import cn.zhangyis.db.storage.undo.UndoLogSegmentAccess;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * T1.4 MvccReader 一致性读全栈：assignWriteId→planInsert/appendPlanned/insertClustered、planUpdate/appendPlanned/replaceClustered 构造
 * 版本链，MvccReader 沿 DB_ROLL_PTR→oldHidden.dbRollPtr 反向选可见版本。覆盖：未提交 insert 不可见、
 * committed 旧版本在新版本提交后仍按 RR 快照读回、自身写可见、RC 看到新提交、多版本链回到 v1。
 */
class MvccReaderTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId DATA_SPACE = SpaceId.of(41);
    private static final SpaceId UNDO_SPACE = SpaceId.of(77);
    private static final long INDEX_ID = 9L;
    private static final long TABLE_ID = 1L;

    @TempDir
    Path dir;

    private final TypeCodecRegistry registry = new TypeCodecRegistry();

    @Test
    void uncommittedInsertIsInvisible() {
        onPool(ctx -> {
            ctx.boot();
            BTreeIndex index = ctx.clusteredIndex();
            Transaction writer = ctx.txnMgr.begin(TransactionOptions.defaults());
            ctx.insertRow(writer, index, 1, "v1"); // 未 commit

            Transaction reader = ctx.txnMgr.begin(TransactionOptions.defaults());
            ReadView rv = ctx.txnMgr.readViewManager().openReadView(reader);
            assertTrue(ctx.mvcc.read(rv, index, search(1)).isEmpty(),
                    "另一未提交事务的 insert 对本 ReadView 不可见");
        });
    }

    @Test
    void committedRowVisibleThenLaterUpdateNotVisibleUnderRrSnapshot() {
        onPool(ctx -> {
            ctx.boot();
            BTreeIndex index = ctx.clusteredIndex();
            Transaction t1 = ctx.txnMgr.begin(TransactionOptions.defaults());
            ctx.insertRow(t1, index, 1, "v1");
            ctx.commit(t1);

            Transaction reader = ctx.txnMgr.begin(TransactionOptions.defaults());
            ReadView rv = ctx.txnMgr.readViewManager().openReadView(reader);
            assertEquals("v1", payload(ctx.mvcc.read(rv, index, search(1))), "committed v1 可见");

            // 另一事务 update→v2 并提交（在 reader 的 ReadView 之后）
            Transaction t3 = ctx.txnMgr.begin(TransactionOptions.defaults());
            ctx.updateRow(t3, index, 1, "v2");
            ctx.commit(t3);

            // RR 快照不漂移：仍读回 v1（沿版本链构造旧版本）
            assertEquals("v1", payload(ctx.mvcc.read(rv, index, search(1))),
                    "RR ReadView 在 v2 提交后仍读回快照版本 v1");
        });
    }

    @Test
    void selfWriteIsVisible() {
        onPool(ctx -> {
            ctx.boot();
            BTreeIndex index = ctx.clusteredIndex();
            Transaction txn = ctx.txnMgr.begin(TransactionOptions.defaults());
            ReadView rv = ctx.txnMgr.readViewManager().openReadView(txn); // 分配 creator id
            ctx.insertRow(txn, index, 1, "v1"); // 用 creator id 写
            assertEquals("v1", payload(ctx.mvcc.read(rv, index, search(1))), "事务能看见自己的写");
        });
    }

    @Test
    void readCommittedSeesNewerCommitOnNextReadView() {
        onPool(ctx -> {
            ctx.boot();
            BTreeIndex index = ctx.clusteredIndex();
            Transaction t1 = ctx.txnMgr.begin(TransactionOptions.defaults());
            ctx.insertRow(t1, index, 1, "v1");
            ctx.commit(t1);

            Transaction reader = ctx.txnMgr.begin(new TransactionOptions(IsolationLevel.READ_COMMITTED, false, true));
            ReadView rvA = ctx.txnMgr.readViewManager().openReadView(reader);
            assertEquals("v1", payload(ctx.mvcc.read(rvA, index, search(1))));

            Transaction t3 = ctx.txnMgr.begin(TransactionOptions.defaults());
            ctx.updateRow(t3, index, 1, "v2");
            ctx.commit(t3);

            // RC：下一条一致性读用新 ReadView，看到 v2
            ReadView rvB = ctx.txnMgr.readViewManager().openReadView(reader);
            assertEquals("v2", payload(ctx.mvcc.read(rvB, index, search(1))), "RC 第二次读看到新提交 v2");
        });
    }

    @Test
    void multiUpdateChainReadsBackOldestVisibleVersion() {
        onPool(ctx -> {
            ctx.boot();
            BTreeIndex index = ctx.clusteredIndex();
            Transaction t1 = ctx.txnMgr.begin(TransactionOptions.defaults());
            ctx.insertRow(t1, index, 1, "v1");
            ctx.commit(t1);

            Transaction reader = ctx.txnMgr.begin(TransactionOptions.defaults());
            ReadView rv = ctx.txnMgr.readViewManager().openReadView(reader); // 只见 v1

            Transaction t3 = ctx.txnMgr.begin(TransactionOptions.defaults());
            ctx.updateRow(t3, index, 1, "v2");
            ctx.commit(t3);
            Transaction t4 = ctx.txnMgr.begin(TransactionOptions.defaults());
            ctx.updateRow(t4, index, 1, "v3");
            ctx.commit(t4);

            // 链 v3→v2→v1，rv 只见 v1
            assertEquals("v1", payload(ctx.mvcc.read(rv, index, search(1))),
                    "沿版本链 v3→v2→v1 回到快照可见版本 v1");
        });
    }

    // ---- T1.3f：delete-mark 可见性 + 版本链所有权校验 + MTR 异常清理 ----

    @Test
    void committedDeleteVisibleToNewReadViewShowsGone() {
        onPool(ctx -> {
            ctx.boot();
            BTreeIndex index = ctx.clusteredIndex();
            Transaction t1 = ctx.txnMgr.begin(TransactionOptions.defaults());
            ctx.insertRow(t1, index, 1, "v1");
            ctx.commit(t1);
            Transaction t2 = ctx.txnMgr.begin(TransactionOptions.defaults());
            ctx.deleteMarkRow(t2, index, 1);
            ctx.commit(t2);

            Transaction reader = ctx.txnMgr.begin(TransactionOptions.defaults());
            ReadView rv = ctx.txnMgr.readViewManager().openReadView(reader); // 见 t1 与 t2 提交后
            assertTrue(ctx.mvcc.read(rv, index, search(1)).isEmpty(),
                    "可见的删除 → 行消失");
        });
    }

    @Test
    void oldReadViewSeesPreDeleteVersion() {
        onPool(ctx -> {
            ctx.boot();
            BTreeIndex index = ctx.clusteredIndex();
            Transaction t1 = ctx.txnMgr.begin(TransactionOptions.defaults());
            ctx.insertRow(t1, index, 1, "v1");
            ctx.commit(t1);

            Transaction reader = ctx.txnMgr.begin(TransactionOptions.defaults());
            ReadView rv = ctx.txnMgr.readViewManager().openReadView(reader); // 删除前建快照
            Transaction t2 = ctx.txnMgr.begin(TransactionOptions.defaults());
            ctx.deleteMarkRow(t2, index, 1);
            ctx.commit(t2);

            assertEquals("v1", payload(ctx.mvcc.read(rv, index, search(1))),
                    "不可见的删除 → 沿版本链见删除前存活版本 v1");
        });
    }

    @Test
    void selfDeleteVisibleAsGone() {
        onPool(ctx -> {
            ctx.boot();
            BTreeIndex index = ctx.clusteredIndex();
            Transaction txn = ctx.txnMgr.begin(TransactionOptions.defaults());
            ReadView rv = ctx.txnMgr.readViewManager().openReadView(txn); // 分配 creator
            ctx.insertRow(txn, index, 1, "v1");
            ctx.deleteMarkRow(txn, index, 1);
            assertTrue(ctx.mvcc.read(rv, index, search(1)).isEmpty(), "事务看到自己的删除 → 消失");
        });
    }

    @Test
    void ownerMismatchInVersionChainThrows() {
        onPool(ctx -> {
            ctx.boot();
            BTreeIndex index = ctx.clusteredIndex();
            Transaction t1 = ctx.txnMgr.begin(TransactionOptions.defaults());
            ctx.insertRow(t1, index, 1, "v1");
            ctx.updateRow(t1, index, 1, "v2"); // 当前版本 DB_ROLL_PTR → t1 的 UPDATE undo
            ctx.commit(t1);
            // 注入：当前记录 DB_TRX_ID 改为大 id（不可见），但 DB_ROLL_PTR 仍指 t1 的 undo
            ctx.injectMismatchedTrxId(index, 1, TransactionId.of(999_999L));

            Transaction reader = ctx.txnMgr.begin(TransactionOptions.defaults());
            ReadView rv = ctx.txnMgr.readViewManager().openReadView(reader);
            assertThrows(UndoLogFormatException.class, () -> ctx.mvcc.read(rv, index, search(1)),
                    "版本链 undo.transactionId 与当前版本 DB_TRX_ID 不符 → 损坏快速失败");
        });
    }

    @Test
    void mtrReusableAfterReadException() {
        onPool(ctx -> {
            ctx.boot();
            BTreeIndex index = ctx.clusteredIndex();
            Transaction t1 = ctx.txnMgr.begin(TransactionOptions.defaults());
            ctx.insertRow(t1, index, 1, "v1");
            ctx.updateRow(t1, index, 1, "v2");
            ctx.insertRow(t1, index, 2, "row2");
            ctx.commit(t1);
            ctx.injectMismatchedTrxId(index, 1, TransactionId.of(999_999L));

            Transaction reader = ctx.txnMgr.begin(TransactionOptions.defaults());
            ReadView rv = ctx.txnMgr.readViewManager().openReadView(reader);
            assertThrows(UndoLogFormatException.class, () -> ctx.mvcc.read(rv, index, search(1)));
            // 抛异常后 MTR 必须已释放：同一 mgr 再读 row2 成功（否则 begin() 抛 nested MTR）
            assertEquals("row2", payload(ctx.mvcc.read(rv, index, search(2))),
                    "读异常后 MTR 已清理，后续读可正常开启");
        });
    }

    // ---- 并发：reader（自有 MTR 管理器）与 writer 并行，验证读路径 latch 纪律不与写路径(undo→index)死锁 ----

    @Test
    void concurrentReaderAndCommittedWriterNoDeadlock() throws InterruptedException {
        PageStore store = new FileChannelPageStore();
        try (PageStore ignored = store; BufferPool pool = new LruBufferPool(store, PS, 256)) {
            Ctx ctx = new Ctx(store, pool);
            ctx.boot();
            BTreeIndex index = ctx.clusteredIndex();
            // 先提交 v0，使行始终存在
            Transaction seed = ctx.txnMgr.begin(TransactionOptions.defaults());
            ctx.insertRow(seed, index, 1, "v0");
            ctx.commit(seed);

            // 每个 update 事务建一条 undo 段且 T1 不回收（已知缺口），受 undo 空间 inode 槽上限约束，故取 30 < 42
            int rounds = 30;
            // reader 用独立 MiniTransactionManager（每实例单线程）；buffer pool / TransactionSystem 线程安全共享
            MiniTransactionManager readerMgr = new MiniTransactionManager();
            MvccReader readerMvcc = new MvccReader(readerMgr, ctx.svc, ctx.undoAccess, UNDO_SPACE, 100);

            ExecutorService pool2 = Executors.newFixedThreadPool(2);
            CopyOnWriteArrayList<Throwable> errors = new CopyOnWriteArrayList<>();
            AtomicInteger reads = new AtomicInteger();
            CountDownLatch start = new CountDownLatch(1);
            try {
                pool2.submit(() -> {
                    try {
                        start.await();
                        for (int i = 1; i <= rounds; i++) {
                            Transaction w = ctx.txnMgr.begin(TransactionOptions.defaults());
                            ctx.updateRow(w, index, 1, "v" + i);
                            ctx.commit(w);
                        }
                    } catch (Throwable e) {
                        errors.add(e);
                    }
                });
                pool2.submit(() -> {
                    try {
                        start.await();
                        for (int i = 0; i < rounds; i++) {
                            Transaction r = ctx.txnMgr.begin(
                                    new TransactionOptions(IsolationLevel.READ_COMMITTED, true, true));
                            ReadView rv = ctx.txnMgr.readViewManager().openReadView(r);
                            Optional<LogicalRecord> got = readerMvcc.read(rv, index, search(1));
                            // 行始终存在某个已提交版本，故必有可见版本（payload 非空）
                            payload(got);
                            reads.incrementAndGet();
                        }
                    } catch (Throwable e) {
                        errors.add(e);
                    }
                });
                start.countDown();
            } finally {
                pool2.shutdown();
                assertTrue(pool2.awaitTermination(30, TimeUnit.SECONDS), "reader/writer 在超时内完成（无死锁）");
            }
            assertTrue(errors.isEmpty(), "并发读写应无异常/死锁，实际: " + errors);
            assertEquals(rounds, reads.get(), "reader 完成全部读");
        }
    }

    // ---- 损坏/边界快速失败（更底层的 undo 指针损坏由 UndoLogSegmentTest.readRecordByRollPointer* 覆盖） ----

    @Test
    void nonClusteredIndexRejected() {
        onPool(ctx -> {
            ctx.boot();
            BTreeIndex nonClustered = new BTreeIndex(INDEX_ID, ctx.rootPageId, 0, idKey(),
                    new TableSchema(1, List.of(
                            new ColumnDef(new ColumnId(0), "id", ColumnType.intType(false, false), 0)), false),
                    true);
            Transaction reader = ctx.txnMgr.begin(TransactionOptions.defaults());
            ReadView rv = ctx.txnMgr.readViewManager().openReadView(reader);
            org.junit.jupiter.api.Assertions.assertThrows(
                    cn.zhangyis.db.common.exception.DatabaseValidationException.class,
                    () -> ctx.mvcc.read(rv, nonClustered, search(1)));
        });
    }

    @Test
    void versionChainExceedingMaxHopsFailsFast() {
        onPool(ctx -> {
            ctx.boot();
            BTreeIndex index = ctx.clusteredIndex();
            MvccReader tightReader = new MvccReader(ctx.mgr, ctx.svc, ctx.undoAccess, UNDO_SPACE, 1);

            Transaction t1 = ctx.txnMgr.begin(TransactionOptions.defaults());
            ctx.insertRow(t1, index, 1, "v1");
            ctx.commit(t1);
            Transaction reader = ctx.txnMgr.begin(TransactionOptions.defaults());
            ReadView rv = ctx.txnMgr.readViewManager().openReadView(reader); // 只见 v1
            // 两次 update → 需 2 跳才回到 v1，超过 maxVersionHops=1
            Transaction t3 = ctx.txnMgr.begin(TransactionOptions.defaults());
            ctx.updateRow(t3, index, 1, "v2");
            ctx.commit(t3);
            Transaction t4 = ctx.txnMgr.begin(TransactionOptions.defaults());
            ctx.updateRow(t4, index, 1, "v3");
            ctx.commit(t4);

            org.junit.jupiter.api.Assertions.assertThrows(
                    cn.zhangyis.db.storage.undo.UndoLogFormatException.class,
                    () -> tightReader.read(rv, index, search(1)),
                    "版本链超过 maxVersionHops 必须快速失败，不无限遍历");
        });
    }

    // ---- helpers ----

    private static String payload(Optional<LogicalRecord> rec) {
        return ((ColumnValue.StringValue) rec.orElseThrow().columnValues().get(1)).value();
    }

    private static SearchKey search(long id) {
        return new SearchKey(List.of(new ColumnValue.IntValue(id)));
    }

    private static TableSchema clusteredSchema() {
        return new TableSchema(1, List.of(
                new ColumnDef(new ColumnId(0), "id", ColumnType.intType(false, false), 0),
                new ColumnDef(new ColumnId(1), "payload", ColumnType.varchar(200, true), 1)), true);
    }

    private static IndexKeyDef idKey() {
        return new IndexKeyDef(INDEX_ID, List.of(new KeyPartDef(new ColumnId(0), KeyOrder.ASC, 0)));
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
        final SplitCapableBTreeIndexService svc;
        final UndoLogSegmentAccess undoAccess;
        final RollbackSegmentSlotManager slots;
        final UndoFinalizationTestSupport.Components finalization;
        final UndoLogManager undoMgr;
        final TransactionManager txnMgr = new TransactionManager(new TransactionSystem());
        final MvccReader mvcc;
        private SegmentRef leafSegment;
        private SegmentRef nonLeafSegment;
        private PageId rootPageId;

        private Ctx(PageStore store, BufferPool pool) {
            this.disk = new DiskSpaceManager(pool, store, PS);
            this.access = new IndexPageAccess(pool, PS);
            this.svc = new SplitCapableBTreeIndexService(access, disk, registry);
            DiskSpaceUndoAllocator allocator = new DiskSpaceUndoAllocator(disk);
            this.undoAccess = new UndoLogSegmentAccess(pool, PS, allocator, registry);
            this.slots = new RollbackSegmentSlotManager(RollbackSegmentId.of(0), 64);
            this.finalization = UndoFinalizationTestSupport.create(mgr, pool, PS, undoAccess, allocator, slots);
            this.undoMgr = finalization.manager(undoAccess, UNDO_SPACE, new HistoryList(), mgr);
            this.mvcc = new MvccReader(mgr, svc, undoAccess, UNDO_SPACE, 100);
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

        private void insertRow(Transaction txn, BTreeIndex index, long id, String payload) {
            TransactionId wid = txnMgr.assignWriteId(txn);
            MiniTransaction m = mgr.begin();
            RollPointer rp = UndoTestWrites.insert(undoMgr, txn, m, TABLE_ID, INDEX_ID,
                    List.of(new ColumnValue.IntValue(id)), index.keyDef(), index.schema());
            svc.insertClustered(m, index, new LogicalRecord(1,
                    List.of(new ColumnValue.IntValue(id), new ColumnValue.StringValue(payload)),
                    false, RecordType.CONVENTIONAL), wid, rp);
            mgr.commit(m);
        }

        private void updateRow(Transaction txn, BTreeIndex index, long id, String newPayload) {
            TransactionId wid = txnMgr.assignWriteId(txn);
            MiniTransaction read = mgr.begin();
            BTreeLookupResult old = svc.lookup(read, index, search(id)).orElseThrow();
            mgr.commit(read);
            HiddenColumns oldHidden = old.record().hiddenColumns();
            MiniTransaction m = mgr.begin();
            RollPointer newRp = UndoTestWrites.update(undoMgr, txn, m, TABLE_ID, INDEX_ID,
                    List.of(new ColumnValue.IntValue(id)), old.record().columnValues(), oldHidden,
                    index.keyDef(), index.schema());
            svc.replaceClustered(m, index, search(id), new LogicalRecord(1,
                    List.of(new ColumnValue.IntValue(id), new ColumnValue.StringValue(newPayload)),
                    false, RecordType.CONVENTIONAL, new HiddenColumns(wid, newRp)),
                    oldHidden.dbTrxId(), oldHidden.dbRollPtr());
            mgr.commit(m);
        }

        private void deleteMarkRow(Transaction txn, BTreeIndex index, long id) {
            TransactionId wid = txnMgr.assignWriteId(txn);
            MiniTransaction read = mgr.begin();
            BTreeLookupResult old = svc.lookup(read, index, search(id)).orElseThrow();
            mgr.commit(read);
            HiddenColumns oldHidden = old.record().hiddenColumns();
            MiniTransaction m = mgr.begin();
            RollPointer delRp = UndoTestWrites.delete(undoMgr, txn, m, TABLE_ID, INDEX_ID,
                    List.of(new ColumnValue.IntValue(id)), old.record().columnValues(), oldHidden,
                    index.keyDef(), index.schema());
            svc.setClusteredDeleteMark(m, index, search(id), true,
                    new HiddenColumns(wid, delRp), oldHidden.dbTrxId(), oldHidden.dbRollPtr());
            mgr.commit(m);
        }

        /** 注入：把当前记录 DB_TRX_ID 改为 fake，但保留 DB_ROLL_PTR（制造版本链所有权不一致，测损坏防御）。 */
        private void injectMismatchedTrxId(BTreeIndex index, long id, TransactionId fake) {
            MiniTransaction read = mgr.begin();
            BTreeLookupResult cur = svc.lookup(read, index, search(id)).orElseThrow();
            mgr.commit(read);
            HiddenColumns h = cur.record().hiddenColumns();
            MiniTransaction m = mgr.begin();
            svc.replaceClustered(m, index, search(id), new LogicalRecord(1, cur.record().columnValues(),
                    false, RecordType.CONVENTIONAL, new HiddenColumns(fake, h.dbRollPtr())),
                    h.dbTrxId(), h.dbRollPtr());
            mgr.commit(m);
        }

        private void commit(Transaction txn) {
            txnMgr.prepareCommit(txn);
            undoMgr.onCommit(txn);
            txnMgr.commit(txn);
        }
    }
}
