package cn.zhangyis.db.storage.btree;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.storage.api.DiskSpaceManager;
import cn.zhangyis.db.storage.api.index.IndexPageAccess;
import cn.zhangyis.db.storage.api.SegmentRef;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * T1.3f btree delete-mark：{@code setClusteredDeleteMark}（前向标记 / 回滚取消标记，外科改 delete 位 + 隐藏列）
 * 与 {@code lookupIncludingDeleted}（不过滤 delete-marked，供 MVCC）。所有权校验 + 翻转合法校验 + 列保留。
 */
class BTreeDeleteMarkTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(41);
    private static final long INDEX_ID = 9L;
    private static final long TRX_A = 100L;
    private static final long TRX_B = 200L;

    @TempDir
    Path dir;

    private final TypeCodecRegistry registry = new TypeCodecRegistry();

    @Test
    void markFiltersFromLookupButVisibleViaIncludingDeleted() {
        onPool(ctx -> {
            ctx.createTablespaceAndRoot();
            SplitCapableBTreeIndexService svc = ctx.service();
            BTreeIndex index = ctx.clusteredIndex();
            RollPointer rpA = new RollPointer(true, PageNo.of(65), 1);
            RollPointer rpDel = new RollPointer(false, PageNo.of(66), 2);

            MiniTransaction m = ctx.mgr.begin();
            svc.insertClustered(m, index, row(1, "v1"), TransactionId.of(TRX_A), rpA);
            ctx.mgr.commit(m);

            MiniTransaction d = ctx.mgr.begin();
            BTreeDeleteMarkResult res = svc.setClusteredDeleteMark(d, index, kId(1), true,
                    new HiddenColumns(TransactionId.of(TRX_B), rpDel), TransactionId.of(TRX_A), rpA);
            ctx.mgr.commit(d);
            assertTrue(res.changed(), "live record delete-marked");

            MiniTransaction r = ctx.mgr.begin();
            assertTrue(svc.lookup(r, index, kId(1)).isEmpty(), "lookup filters delete-marked");
            BTreeLookupResult raw = svc.lookupIncludingDeleted(r, index, kId(1)).orElseThrow();
            ctx.mgr.commit(r);
            assertTrue(raw.record().deleted(), "lookupIncludingDeleted sees the delete-marked record");
            assertEquals("v1", payloadOf(raw), "columns preserved by surgical hidden patch");
            assertEquals(TransactionId.of(TRX_B), raw.record().hiddenColumns().dbTrxId(), "new DB_TRX_ID stamped");
            assertEquals(rpDel, raw.record().hiddenColumns().dbRollPtr(), "new DB_ROLL_PTR stamped");
        });
    }

    @Test
    void unmarkRestoresToLookup() {
        onPool(ctx -> {
            ctx.createTablespaceAndRoot();
            SplitCapableBTreeIndexService svc = ctx.service();
            BTreeIndex index = ctx.clusteredIndex();
            RollPointer rpA = new RollPointer(true, PageNo.of(65), 1);
            RollPointer rpDel = new RollPointer(false, PageNo.of(66), 2);

            MiniTransaction m = ctx.mgr.begin();
            svc.insertClustered(m, index, row(1, "v1"), TransactionId.of(TRX_A), rpA);
            svc.setClusteredDeleteMark(m, index, kId(1), true,
                    new HiddenColumns(TransactionId.of(TRX_B), rpDel), TransactionId.of(TRX_A), rpA);
            ctx.mgr.commit(m);

            // 取消标记：还原存活版本 + 旧隐藏列
            MiniTransaction u = ctx.mgr.begin();
            BTreeDeleteMarkResult res = svc.setClusteredDeleteMark(u, index, kId(1), false,
                    new HiddenColumns(TransactionId.of(TRX_A), rpA), TransactionId.of(TRX_B), rpDel);
            ctx.mgr.commit(u);
            assertTrue(res.changed());

            MiniTransaction r = ctx.mgr.begin();
            BTreeLookupResult found = svc.lookup(r, index, kId(1)).orElseThrow();
            ctx.mgr.commit(r);
            assertEquals("v1", payloadOf(found), "row live again after un-mark");
            assertEquals(TransactionId.of(TRX_A), found.record().hiddenColumns().dbTrxId());
        });
    }

    @Test
    void ownershipMismatchNoChange() {
        onPool(ctx -> {
            ctx.createTablespaceAndRoot();
            SplitCapableBTreeIndexService svc = ctx.service();
            BTreeIndex index = ctx.clusteredIndex();
            RollPointer rpA = new RollPointer(true, PageNo.of(65), 1);

            MiniTransaction m = ctx.mgr.begin();
            svc.insertClustered(m, index, row(1, "v1"), TransactionId.of(TRX_A), rpA);
            ctx.mgr.commit(m);

            MiniTransaction d = ctx.mgr.begin();
            BTreeDeleteMarkResult res = svc.setClusteredDeleteMark(d, index, kId(1), true,
                    new HiddenColumns(TransactionId.of(TRX_B), new RollPointer(false, PageNo.of(66), 2)),
                    TransactionId.of(TRX_A), new RollPointer(true, PageNo.of(65), 999));
            ctx.mgr.commit(d);
            assertFalse(res.changed(), "ownership mismatch must not delete-mark");

            MiniTransaction r = ctx.mgr.begin();
            assertTrue(svc.lookup(r, index, kId(1)).isPresent(), "record still live");
            ctx.mgr.commit(r);
        });
    }

    @Test
    void illegalFlipThrows() {
        onPool(ctx -> {
            ctx.createTablespaceAndRoot();
            SplitCapableBTreeIndexService svc = ctx.service();
            BTreeIndex index = ctx.clusteredIndex();
            RollPointer rpA = new RollPointer(true, PageNo.of(65), 1);

            MiniTransaction m = ctx.mgr.begin();
            svc.insertClustered(m, index, row(1, "v1"), TransactionId.of(TRX_A), rpA);
            ctx.mgr.commit(m);

            // 对存活记录"取消标记"（deleted=false 但已是 false）→ 非法翻转
            MiniTransaction u = ctx.mgr.begin();
            assertThrows(DatabaseValidationException.class, () -> svc.setClusteredDeleteMark(u, index, kId(1), false,
                    new HiddenColumns(TransactionId.of(TRX_A), rpA), TransactionId.of(TRX_A), rpA));
            ctx.mgr.rollbackUncommitted(u);
        });
    }

    @Test
    void levelOneCrossLeafMark() {
        onPool(ctx -> {
            ctx.createTablespaceAndRoot();
            SplitCapableBTreeIndexService svc = ctx.service();
            BTreeIndex current = ctx.clusteredIndex();
            RollPointer[] rps = new RollPointer[5];
            for (long id = 1; id <= 4; id++) {
                MiniTransaction m = ctx.mgr.begin();
                rps[(int) id] = new RollPointer(true, PageNo.of(65), (int) id);
                BTreeInsertResult res = svc.insertClustered(m, current, wideRow(id), TransactionId.of(TRX_A), rps[(int) id]);
                current = res.indexAfterInsert();
                ctx.mgr.commit(m);
            }
            assertEquals(1, current.rootLevel());

            MiniTransaction d = ctx.mgr.begin();
            BTreeDeleteMarkResult res = svc.setClusteredDeleteMark(d, current, kId(4), true,
                    new HiddenColumns(TransactionId.of(TRX_B), new RollPointer(false, PageNo.of(66), 9)),
                    TransactionId.of(TRX_A), rps[4]);
            ctx.mgr.commit(d);
            assertTrue(res.changed());

            MiniTransaction r = ctx.mgr.begin();
            assertTrue(svc.lookup(r, current, kId(4)).isEmpty(), "marked key filtered");
            assertTrue(svc.lookupIncludingDeleted(r, current, kId(4)).orElseThrow().record().deleted());
            assertTrue(svc.lookup(r, current, kId(1)).isPresent(), "others live");
            ctx.mgr.commit(r);
        });
    }

    // ---- helpers ----

    private static String payloadOf(BTreeLookupResult r) {
        return ((ColumnValue.StringValue) r.record().columnValues().get(1)).value();
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

    private static LogicalRecord row(long id, String payload) {
        return new LogicalRecord(1, List.of(new ColumnValue.IntValue(id),
                new ColumnValue.StringValue(payload)), false, RecordType.CONVENTIONAL);
    }

    private static LogicalRecord wideRow(long id) {
        return new LogicalRecord(1, List.of(new ColumnValue.IntValue(id),
                new ColumnValue.StringValue("x".repeat(5000))), false, RecordType.CONVENTIONAL);
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
        private final MiniTransactionManager mgr = new MiniTransactionManager();
        private final DiskSpaceManager disk;
        private final IndexPageAccess access;
        private SegmentRef leafSegment;
        private SegmentRef nonLeafSegment;
        private PageId rootPageId;

        private Ctx(PageStore store, BufferPool pool) {
            this.disk = new DiskSpaceManager(pool, store, PS);
            this.access = new IndexPageAccess(pool, PS);
        }

        private SplitCapableBTreeIndexService service() {
            return new SplitCapableBTreeIndexService(access, disk, registry);
        }

        private void createTablespaceAndRoot() {
            MiniTransaction m = mgr.begin();
            disk.createTablespace(m, SPACE, dir.resolve("clustered.ibd"), PageNo.of(64));
            leafSegment = disk.createSegment(m, SPACE, SegmentPurpose.INDEX_LEAF);
            nonLeafSegment = disk.createSegment(m, SPACE, SegmentPurpose.INDEX_NON_LEAF);
            rootPageId = disk.allocatePage(m, leafSegment);
            access.createIndexPage(m, rootPageId, INDEX_ID, 0);
            mgr.commit(m);
        }

        private BTreeIndex clusteredIndex() {
            return new BTreeIndex(INDEX_ID, rootPageId, 0, idKey(), clusteredSchema(), true,
                    leafSegment, nonLeafSegment);
        }
    }
}
