package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.UndoNo;
import cn.zhangyis.db.storage.api.DiskSpaceManager;
import cn.zhangyis.db.storage.api.DiskSpaceUndoAllocator;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.LruBufferPool;
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
import cn.zhangyis.db.storage.record.type.ColumnValue;
import cn.zhangyis.db.storage.record.type.TypeCodecRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * T1.3b UndoLogSegment 单页：create→append→RollPointer→readRecord、log header 计数、
 * forEach 有序遍历、NULL 指针拒绝。
 */
class UndoLogSegmentTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId UNDO_SPACE = SpaceId.of(77);

    @TempDir
    Path dir;

    private final TypeCodecRegistry registry = new TypeCodecRegistry();

    private static TableSchema schema() {
        return new TableSchema(1, List.of(
                new ColumnDef(new ColumnId(0), "id", ColumnType.intType(false, false), 0),
                new ColumnDef(new ColumnId(1), "name", ColumnType.varchar(64, true), 1)), true);
    }

    private static IndexKeyDef keyDef() {
        return new IndexKeyDef(9L, List.of(new KeyPartDef(new ColumnId(0), KeyOrder.ASC, 0)));
    }

    private UndoRecord rec(long undoNo, long id, RollPointer prev) {
        return UndoRecord.insert(UndoNo.of(undoNo), TransactionId.of(7),
                1L, 9L, List.of(new ColumnValue.IntValue(id)), prev);
    }

    @Test
    void createAppendReadBackSinglePage() {
        onSegment(seg -> {
            UndoRecord r = rec(1, 100, RollPointer.NULL);
            RollPointer rp = seg.append(r, keyDef(), schema());
            assertFalse(rp.isNull());
            assertTrue(rp.insert());
            assertEquals(seg.firstPageId().pageNo(), rp.pageNo());
            assertEquals(r, seg.readRecord(rp, keyDef(), schema()));
            assertEquals(1L, seg.logRecordCount());
            assertEquals(1L, seg.logLastUndoNo().value());
        });
    }

    @Test
    void logHeaderCountsAdvancePerAppend() {
        onSegment(seg -> {
            seg.append(rec(1, 100, RollPointer.NULL), keyDef(), schema());
            seg.append(rec(2, 101, RollPointer.NULL), keyDef(), schema());
            seg.append(rec(3, 102, RollPointer.NULL), keyDef(), schema());
            assertEquals(3L, seg.logRecordCount());
            assertEquals(3L, seg.logLastUndoNo().value());
        });
    }

    @Test
    void forEachRecordReturnsAllInOrderSinglePage() {
        onSegment(seg -> {
            seg.append(rec(1, 100, RollPointer.NULL), keyDef(), schema());
            seg.append(rec(2, 101, RollPointer.NULL), keyDef(), schema());
            List<UndoRecord> got = new ArrayList<>();
            seg.forEachRecord(got::add, keyDef(), schema());
            assertEquals(List.of(rec(1, 100, RollPointer.NULL), rec(2, 101, RollPointer.NULL)), got);
        });
    }

    @Test
    void readRecordRejectsNullPointer() {
        onSegment(seg ->
                assertThrows(UndoLogFormatException.class,
                        () -> seg.readRecord(RollPointer.NULL, keyDef(), schema())));
    }

    private static TableSchema bigSchema() {
        return new TableSchema(1, List.of(
                new ColumnDef(new ColumnId(0), "k", ColumnType.varchar(20000, false), 0)), true);
    }

    private static IndexKeyDef bigKeyDef() {
        return new IndexKeyDef(9L, List.of(new KeyPartDef(new ColumnId(0), KeyOrder.ASC, 0)));
    }

    private UndoRecord bigRec(long undoNo, String key, RollPointer prev) {
        return UndoRecord.insert(UndoNo.of(undoNo), TransactionId.of(7),
                1L, 9L, List.of(new ColumnValue.StringValue(key)), prev);
    }

    private static String bigKey(long undoNo) {
        return String.format("%05d", undoNo) + "x".repeat(4995);
    }

    @Test
    void growthAllocatesLinksNewPageAndReadsAcross() {
        onSegment(seg -> {
            RollPointer rp1 = seg.append(bigRec(1, bigKey(1), RollPointer.NULL), bigKeyDef(), bigSchema());
            seg.append(bigRec(2, bigKey(2), RollPointer.NULL), bigKeyDef(), bigSchema());
            RollPointer rp3 = seg.append(bigRec(3, bigKey(3), RollPointer.NULL), bigKeyDef(), bigSchema());
            RollPointer rp4 = seg.append(bigRec(4, bigKey(4), RollPointer.NULL), bigKeyDef(), bigSchema());
            assertEquals(seg.firstPageId().pageNo(), rp1.pageNo());
            assertEquals(seg.firstPageId().pageNo(), rp3.pageNo());
            assertNotEquals(seg.firstPageId().pageNo(), rp4.pageNo());
            assertEquals(seg.lastPageId().pageNo(), rp4.pageNo());
            assertEquals(4L, seg.logRecordCount());
            assertEquals(4L, seg.logLastUndoNo().value());
            assertEquals(bigRec(4, bigKey(4), RollPointer.NULL), seg.readRecord(rp4, bigKeyDef(), bigSchema()));
            assertEquals(bigRec(1, bigKey(1), RollPointer.NULL), seg.readRecord(rp1, bigKeyDef(), bigSchema()));
        });
    }

    @Test
    void forEachTraversesAllPagesInOrder() {
        onSegment(seg -> {
            List<UndoRecord> expected = new ArrayList<>();
            for (long i = 1; i <= 5; i++) {
                UndoRecord r = bigRec(i, bigKey(i), RollPointer.NULL);
                expected.add(r);
                seg.append(r, bigKeyDef(), bigSchema());
            }
            assertNotEquals(seg.firstPageId().pageNo(), seg.lastPageId().pageNo());
            List<UndoRecord> got = new ArrayList<>();
            seg.forEachRecord(got::add, bigKeyDef(), bigSchema());
            assertEquals(expected, got);
        });
    }

    @Test
    void prevRollPointerChainsAcrossPages() {
        onSegment(seg -> {
            seg.append(bigRec(1, bigKey(1), RollPointer.NULL), bigKeyDef(), bigSchema());
            seg.append(bigRec(2, bigKey(2), RollPointer.NULL), bigKeyDef(), bigSchema());
            RollPointer rp3 = seg.append(bigRec(3, bigKey(3), RollPointer.NULL), bigKeyDef(), bigSchema());
            RollPointer rp4 = seg.append(bigRec(4, bigKey(4), rp3), bigKeyDef(), bigSchema());
            assertNotEquals(rp3.pageNo(), rp4.pageNo());
            UndoRecord back4 = seg.readRecord(rp4, bigKeyDef(), bigSchema());
            assertEquals(rp3, back4.prevRollPointer());
            assertEquals(bigRec(3, bigKey(3), RollPointer.NULL),
                    seg.readRecord(back4.prevRollPointer(), bigKeyDef(), bigSchema()));
        });
    }

    @Test
    void oversizedRecordThrowsWithoutGrowing() {
        onSegment(seg -> {
            String huge = "y".repeat(16300);
            UndoRecord big = UndoRecord.insert(UndoNo.of(1), TransactionId.of(7),
                    1L, 9L, List.of(new ColumnValue.StringValue(huge)), RollPointer.NULL);
            assertThrows(UndoPageOverflowException.class,
                    () -> seg.append(big, bigKeyDef(), bigSchema()));
            assertEquals(seg.firstPageId(), seg.lastPageId());
            assertEquals(0L, seg.logRecordCount());
            assertEquals(0L, seg.logLastUndoNo().value());
            List<UndoRecord> got = new ArrayList<>();
            seg.forEachRecord(got::add, bigKeyDef(), bigSchema());
            assertTrue(got.isEmpty());
        });
    }

    @Test
    void readRecordRejectsPointerFromOtherSegment() {
        onAccess((mgr, access) -> {
            MiniTransaction m1 = mgr.begin();
            UndoLogSegment segB = access.create(m1, UNDO_SPACE, TransactionId.of(8));
            RollPointer rpB = segB.append(rec(1, 100, RollPointer.NULL), keyDef(), schema());
            mgr.commit(m1);

            MiniTransaction m2 = mgr.begin();
            UndoLogSegment segA = access.create(m2, UNDO_SPACE, TransactionId.of(7));
            assertThrows(UndoLogFormatException.class, () -> segA.readRecord(rpB, keyDef(), schema()));
            mgr.commit(m2);
        });
    }

    // ---- T1.4：readRecordByRollPointer（MVCC 跨段直读 + 损坏拒绝） ----

    private UndoRecord updateRec(long undoNo, long id, RollPointer prev) {
        return UndoRecord.update(UndoNo.of(undoNo), TransactionId.of(7), 1L, 9L,
                java.util.List.of(new ColumnValue.IntValue(id)),
                java.util.List.of(new ColumnValue.IntValue(id), new ColumnValue.StringValue("old-" + id)),
                new cn.zhangyis.db.storage.record.format.HiddenColumns(TransactionId.of(3), prev), prev);
    }

    @Test
    void readRecordByRollPointerReadsInsertAndUpdate() {
        onAccess((mgr, access) -> {
            MiniTransaction m = mgr.begin();
            UndoLogSegment seg = access.create(m, UNDO_SPACE, TransactionId.of(7));
            RollPointer insRp = seg.append(rec(1, 100, RollPointer.NULL), keyDef(), schema());
            RollPointer updRp = seg.append(updateRec(2, 100, insRp), keyDef(), schema());
            mgr.commit(m);

            MiniTransaction r = mgr.begin();
            UndoRecord ins = access.readRecordByRollPointer(r, UNDO_SPACE, insRp, keyDef(), schema());
            UndoRecord upd = access.readRecordByRollPointer(r, UNDO_SPACE, updRp, keyDef(), schema());
            mgr.rollbackUncommitted(r);

            assertEquals(UndoRecordType.INSERT_ROW, ins.type());
            assertEquals(UndoRecordType.UPDATE_ROW, upd.type());
            assertEquals(java.util.List.of(new ColumnValue.IntValue(100),
                    new ColumnValue.StringValue("old-100")), upd.oldColumnValues());
        });
    }

    @Test
    void readRecordByRollPointerRejectsCorruption() {
        onAccess((mgr, access) -> {
            MiniTransaction m = mgr.begin();
            UndoLogSegment seg = access.create(m, UNDO_SPACE, TransactionId.of(7));
            RollPointer insRp = seg.append(rec(1, 100, RollPointer.NULL), keyDef(), schema());
            mgr.commit(m);

            MiniTransaction r = mgr.begin();
            // NULL 指针
            assertThrows(UndoLogFormatException.class,
                    () -> access.readRecordByRollPointer(r, UNDO_SPACE, RollPointer.NULL, keyDef(), schema()));
            // insert 位与记录类型不符（INSERT 记录但指针 insert=false）
            RollPointer wrongBit = new RollPointer(false, insRp.pageNo(), insRp.offset());
            assertThrows(UndoLogFormatException.class,
                    () -> access.readRecordByRollPointer(r, UNDO_SPACE, wrongBit, keyDef(), schema()));
            // indexId 不符
            IndexKeyDef otherIndex = new IndexKeyDef(99L, java.util.List.of(
                    new KeyPartDef(new ColumnId(0), KeyOrder.ASC, 0)));
            assertThrows(UndoLogFormatException.class,
                    () -> access.readRecordByRollPointer(r, UNDO_SPACE, insRp, otherIndex, schema()));
            // 越界 offset（超出 record area）
            RollPointer badOffset = new RollPointer(true, insRp.pageNo(), 16000);
            assertThrows(UndoLogFormatException.class,
                    () -> access.readRecordByRollPointer(r, UNDO_SPACE, badOffset, keyDef(), schema()));
            mgr.rollbackUncommitted(r);
        });
    }

    @Test
    void appendStampsRollPointerInsertFlagByRecordType() {
        onSegment(seg -> {
            // INSERT_ROW → insert flag true（不变）
            RollPointer insRp = seg.append(rec(1, 100, RollPointer.NULL), keyDef(), schema());
            assertTrue(insRp.insert(), "INSERT_ROW append must stamp insert=true");
            // UPDATE_ROW → insert flag false（T1.3e：供 MVCC/版本链区分 insert vs update undo）
            UndoRecord upd = UndoRecord.update(UndoNo.of(2), TransactionId.of(7), 1L, 9L,
                    List.of(new ColumnValue.IntValue(100)),
                    List.of(new ColumnValue.IntValue(100), new ColumnValue.StringValue("old")),
                    new cn.zhangyis.db.storage.record.format.HiddenColumns(
                            TransactionId.of(3), new RollPointer(false, PageNo.of(65), 1)),
                    insRp);
            RollPointer updRp = seg.append(upd, keyDef(), schema());
            assertFalse(updRp.insert(), "UPDATE_ROW append must stamp insert=false");
        });
    }

    private interface SegmentBody {
        void run(UndoLogSegment seg);
    }

    private interface AccessBody {
        void run(MiniTransactionManager mgr, UndoLogSegmentAccess access);
    }

    private void onSegment(SegmentBody body) {
        onAccess((mgr, access) -> {
            MiniTransaction m = mgr.begin();
            UndoLogSegment seg = access.create(m, UNDO_SPACE, TransactionId.of(7));
            body.run(seg);
            mgr.commit(m);
        });
    }

    private void onAccess(AccessBody body) {
        PageStore store = new FileChannelPageStore();
        try (PageStore ignored = store; BufferPool pool = new LruBufferPool(store, PS, 128)) {
            MiniTransactionManager mgr = new MiniTransactionManager();
            DiskSpaceManager disk = new DiskSpaceManager(pool, store, PS);
            DiskSpaceUndoAllocator allocator = new DiskSpaceUndoAllocator(disk);
            UndoLogSegmentAccess access = new UndoLogSegmentAccess(pool, PS, allocator, registry);
            MiniTransaction boot = mgr.begin();
            disk.createTablespace(boot, UNDO_SPACE, dir.resolve("undo.ibu"), PageNo.of(64));
            mgr.commit(boot);
            body.run(mgr, access);
        }
    }
}
