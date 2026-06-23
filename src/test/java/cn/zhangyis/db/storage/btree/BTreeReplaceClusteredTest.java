package cn.zhangyis.db.storage.btree;

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
 * T1.3e btree {@code replaceClustered}（前向 UPDATE 与 rollback 恢复共用的整记录替换原语）。覆盖：所有权匹配替换、
 * 所有权不匹配幂等不改、改聚簇 PK 抛、level-1 跨 leaf 替换。所有权校验用当前记录 DB_TRX_ID/DB_ROLL_PTR == 期望值，
 * 只改"本事务/本版本"的那一行。
 */
class BTreeReplaceClusteredTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(41);
    private static final long INDEX_ID = 9L;
    private static final long TRX_A = 100L;
    private static final long TRX_B = 200L;

    @TempDir
    Path dir;

    private final TypeCodecRegistry registry = new TypeCodecRegistry();

    @Test
    void matchingOwnershipReplacesRecord() {
        onPool(ctx -> {
            ctx.createTablespaceAndRoot();
            SplitCapableBTreeIndexService svc = ctx.service();
            BTreeIndex index = ctx.clusteredIndex();
            RollPointer rpA = new RollPointer(true, PageNo.of(65), 1);
            RollPointer rpB = new RollPointer(false, PageNo.of(66), 2);

            MiniTransaction m = ctx.mgr.begin();
            svc.insertClustered(m, index, row(1, "v1"), TransactionId.of(TRX_A), rpA);
            ctx.mgr.commit(m);

            MiniTransaction u = ctx.mgr.begin();
            BTreeUpdateResult res = svc.replaceClustered(u, index, kId(1),
                    rowWithHidden(1, "v2", TRX_B, rpB), TransactionId.of(TRX_A), rpA);
            ctx.mgr.commit(u);

            assertTrue(res.replaced(), "matching ownership must replace");
            MiniTransaction r = ctx.mgr.begin();
            BTreeLookupResult found = svc.lookup(r, index, kId(1)).orElseThrow();
            ctx.mgr.commit(r);
            assertEquals("v2", payloadOf(found));
            assertEquals(TransactionId.of(TRX_B), found.record().hiddenColumns().dbTrxId());
            assertEquals(rpB, found.record().hiddenColumns().dbRollPtr(), "DB_ROLL_PTR updated to new version pointer");
        });
    }

    @Test
    void ownershipMismatchDoesNotReplace() {
        onPool(ctx -> {
            ctx.createTablespaceAndRoot();
            SplitCapableBTreeIndexService svc = ctx.service();
            BTreeIndex index = ctx.clusteredIndex();
            RollPointer rpA = new RollPointer(true, PageNo.of(65), 1);

            MiniTransaction m = ctx.mgr.begin();
            svc.insertClustered(m, index, row(1, "v1"), TransactionId.of(TRX_A), rpA);
            ctx.mgr.commit(m);

            // 期望 roll pointer 不符 → 不替换
            MiniTransaction u = ctx.mgr.begin();
            BTreeUpdateResult res = svc.replaceClustered(u, index, kId(1),
                    rowWithHidden(1, "v2", TRX_B, new RollPointer(false, PageNo.of(66), 2)),
                    TransactionId.of(TRX_A), new RollPointer(true, PageNo.of(65), 999));
            ctx.mgr.commit(u);

            assertFalse(res.replaced(), "non-matching DB_ROLL_PTR must not replace");
            MiniTransaction r = ctx.mgr.begin();
            BTreeLookupResult found = svc.lookup(r, index, kId(1)).orElseThrow();
            ctx.mgr.commit(r);
            assertEquals("v1", payloadOf(found), "record unchanged on ownership mismatch");
            assertEquals(TransactionId.of(TRX_A), found.record().hiddenColumns().dbTrxId());
        });
    }

    @Test
    void changingClusterKeyThrows() {
        onPool(ctx -> {
            ctx.createTablespaceAndRoot();
            SplitCapableBTreeIndexService svc = ctx.service();
            BTreeIndex index = ctx.clusteredIndex();
            RollPointer rpA = new RollPointer(true, PageNo.of(65), 1);

            MiniTransaction m = ctx.mgr.begin();
            svc.insertClustered(m, index, row(1, "v1"), TransactionId.of(TRX_A), rpA);
            ctx.mgr.commit(m);

            // 定位 key=1 但 newRecord 的 id=2（改聚簇 PK）→ T1.3e 不支持
            MiniTransaction u = ctx.mgr.begin();
            assertThrows(BTreeUnsupportedStructureException.class, () -> svc.replaceClustered(u, index, kId(1),
                    rowWithHidden(2, "v2", TRX_B, new RollPointer(false, PageNo.of(66), 2)),
                    TransactionId.of(TRX_A), rpA));
            ctx.mgr.rollbackUncommitted(u);
        });
    }

    @Test
    void levelOneCrossLeafReplace() {
        onPool(ctx -> {
            ctx.createTablespaceAndRoot();
            SplitCapableBTreeIndexService svc = ctx.service();
            BTreeIndex current = ctx.clusteredIndex();
            RollPointer[] rps = new RollPointer[5];
            for (long id = 1; id <= 4; id++) {
                MiniTransaction m = ctx.mgr.begin();
                rps[(int) id] = new RollPointer(true, PageNo.of(65), (int) id);
                BTreeInsertResult res = svc.insertClustered(m, current, wideRow(id, "x"),
                        TransactionId.of(TRX_A), rps[(int) id]);
                current = res.indexAfterInsert();
                ctx.mgr.commit(m);
            }
            assertEquals(1, current.rootLevel(), "4 wide rows split the root");

            // 替换 id=4（落右 leaf）
            RollPointer rpNew = new RollPointer(false, PageNo.of(66), 9);
            MiniTransaction u = ctx.mgr.begin();
            BTreeUpdateResult res = svc.replaceClustered(u, current, kId(4),
                    wideRowWithHidden(4, "z", TRX_B, rpNew), TransactionId.of(TRX_A), rps[4]);
            ctx.mgr.commit(u);
            assertTrue(res.replaced());

            MiniTransaction r = ctx.mgr.begin();
            BTreeLookupResult found = svc.lookup(r, current, kId(4)).orElseThrow();
            ctx.mgr.commit(r);
            assertEquals(TransactionId.of(TRX_B), found.record().hiddenColumns().dbTrxId());
        });
    }

    // ---- helpers ----

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

    private static LogicalRecord rowWithHidden(long id, String payload, long trx, RollPointer rp) {
        return new LogicalRecord(1, List.of(new ColumnValue.IntValue(id), new ColumnValue.StringValue(payload)),
                false, RecordType.CONVENTIONAL, new HiddenColumns(TransactionId.of(trx), rp));
    }

    private static LogicalRecord wideRow(long id, String tag) {
        return new LogicalRecord(1, List.of(new ColumnValue.IntValue(id),
                new ColumnValue.StringValue(tag.repeat(5000))), false, RecordType.CONVENTIONAL);
    }

    private static LogicalRecord wideRowWithHidden(long id, String tag, long trx, RollPointer rp) {
        return new LogicalRecord(1, List.of(new ColumnValue.IntValue(id), new ColumnValue.StringValue(tag.repeat(5000))),
                false, RecordType.CONVENTIONAL, new HiddenColumns(TransactionId.of(trx), rp));
    }

    private static String payloadOf(BTreeLookupResult found) {
        return ((ColumnValue.StringValue) found.record().columnValues().get(1)).value();
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
