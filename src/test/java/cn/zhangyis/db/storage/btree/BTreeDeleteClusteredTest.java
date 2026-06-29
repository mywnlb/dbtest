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
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.fil.io.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.fsp.segment.SegmentPurpose;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import cn.zhangyis.db.storage.record.format.HiddenColumns;
import cn.zhangyis.db.storage.record.format.LogicalRecord;
import cn.zhangyis.db.storage.record.format.RecordType;
import cn.zhangyis.db.storage.record.page.RecordPage;
import cn.zhangyis.db.storage.record.page.RecordPageDeleter;
import cn.zhangyis.db.storage.record.page.RecordPageSearch;
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
import java.util.OptionalInt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * T1.3d btree 物理删除 {@code deleteClustered}（rollback 反向走链消费 INSERT undo 的删除原语）。覆盖：
 * 匹配命中删除、未命中幂等、所有权（DB_TRX_ID/DB_ROLL_PTR）不匹配不删、已 delete-marked 直接 purge、
 * level-1 跨 leaf 删除、删后其余 key 仍可查。
 *
 * <p>所有权校验是关键不变量：rollback 只删本 undo 插入的行（dbTrxId+dbRollPtr 同时匹配），否则即便 cluster key
 * 相同也绝不误删（杜绝 orphan undo 场景下删掉别的事务后写入的同 key 行）。
 */
class BTreeDeleteClusteredTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(41);
    private static final long INDEX_ID = 9L;
    private static final long TRX = 123L;

    @TempDir
    Path dir;

    private final TypeCodecRegistry registry = new TypeCodecRegistry();

    @Test
    void matchingDeleteRemovesRecord() {
        onPool(ctx -> {
            ctx.createTablespaceAndRoot();
            SplitCapableBTreeIndexService svc = ctx.service();
            BTreeIndex index = ctx.clusteredIndex();
            RollPointer rp = new RollPointer(true, PageNo.of(65), 1);

            MiniTransaction m = ctx.mgr.begin();
            svc.insertClustered(m, index, row(1), TransactionId.of(TRX), rp);
            ctx.mgr.commit(m);

            MiniTransaction d = ctx.mgr.begin();
            BTreeDeleteResult result = svc.deleteClustered(d, index, kId(1), TransactionId.of(TRX), rp);
            ctx.mgr.commit(d);

            assertTrue(result.removed(), "matching record must be removed");
            MiniTransaction r = ctx.mgr.begin();
            assertTrue(svc.lookup(r, index, kId(1)).isEmpty(), "row gone after delete");
            ctx.mgr.commit(r);
        });
    }

    @Test
    void missingKeyIsIdempotentNoOp() {
        onPool(ctx -> {
            ctx.createTablespaceAndRoot();
            SplitCapableBTreeIndexService svc = ctx.service();
            BTreeIndex index = ctx.clusteredIndex();

            // 从未插入 key 7：orphan undo 兑现路径——找不到记录即 no-op，不抛
            MiniTransaction d = ctx.mgr.begin();
            BTreeDeleteResult result = svc.deleteClustered(d, index, kId(7),
                    TransactionId.of(TRX), new RollPointer(true, PageNo.of(65), 7));
            ctx.mgr.commit(d);

            assertFalse(result.removed(), "missing key must yield removed=false, no exception");
        });
    }

    @Test
    void ownershipMismatchDoesNotDelete() {
        onPool(ctx -> {
            ctx.createTablespaceAndRoot();
            SplitCapableBTreeIndexService svc = ctx.service();
            BTreeIndex index = ctx.clusteredIndex();
            RollPointer rp = new RollPointer(true, PageNo.of(65), 1);

            MiniTransaction m = ctx.mgr.begin();
            svc.insertClustered(m, index, row(1), TransactionId.of(TRX), rp);
            ctx.mgr.commit(m);

            // trx id 不符 → 不删
            MiniTransaction d1 = ctx.mgr.begin();
            assertFalse(svc.deleteClustered(d1, index, kId(1), TransactionId.of(999L), rp).removed(),
                    "different DB_TRX_ID must not delete");
            ctx.mgr.commit(d1);
            // roll pointer 不符 → 不删
            MiniTransaction d2 = ctx.mgr.begin();
            assertFalse(svc.deleteClustered(d2, index, kId(1), TransactionId.of(TRX),
                    new RollPointer(true, PageNo.of(65), 999)).removed(), "different DB_ROLL_PTR must not delete");
            ctx.mgr.commit(d2);

            // 记录仍在
            MiniTransaction r = ctx.mgr.begin();
            assertTrue(svc.lookup(r, index, kId(1)).isPresent(), "record must survive non-matching deletes");
            ctx.mgr.commit(r);
        });
    }

    @Test
    void alreadyDeleteMarkedGoesStraightToPurge() {
        onPool(ctx -> {
            ctx.createTablespaceAndRoot();
            SplitCapableBTreeIndexService svc = ctx.service();
            BTreeIndex index = ctx.clusteredIndex();
            RollPointer rp = new RollPointer(true, PageNo.of(65), 1);

            MiniTransaction m = ctx.mgr.begin();
            svc.insertClustered(m, index, row(1), TransactionId.of(TRX), rp);
            ctx.mgr.commit(m);

            // 手工把记录 delete-mark（模拟半失败/重试场景），deleteClustered 须跳过 mark 直接 purge
            MiniTransaction mark = ctx.mgr.begin();
            RecordPage leaf = ctx.access.openIndexPage(mark, ctx.rootPageId, PageLatchMode.EXCLUSIVE);
            OptionalInt off = new RecordPageSearch(registry).findEqual(leaf, kId(1), idKey(), clusteredSchema());
            assertTrue(off.isPresent(), "row must be present before manual delete-mark");
            new RecordPageDeleter().deleteMark(leaf, off.getAsInt());
            ctx.mgr.commit(mark);

            MiniTransaction d = ctx.mgr.begin();
            BTreeDeleteResult result = svc.deleteClustered(d, index, kId(1), TransactionId.of(TRX), rp);
            ctx.mgr.commit(d);

            assertTrue(result.removed(), "already delete-marked matching record must still be purged");
            MiniTransaction r = ctx.mgr.begin();
            assertTrue(svc.lookup(r, index, kId(1)).isEmpty(), "row gone after purge");
            ctx.mgr.commit(r);
        });
    }

    @Test
    void levelOneCrossLeafDeleteLeavesOthersQueryable() {
        onPool(ctx -> {
            ctx.createTablespaceAndRoot();
            SplitCapableBTreeIndexService svc = ctx.service();
            BTreeIndex current = ctx.clusteredIndex();

            for (long id = 1; id <= 4; id++) {
                MiniTransaction m = ctx.mgr.begin();
                BTreeInsertResult res = svc.insertClustered(m, current, wideRow(id),
                        TransactionId.of(TRX), new RollPointer(true, PageNo.of(65), (int) id));
                current = res.indexAfterInsert();
                ctx.mgr.commit(m);
            }
            assertEquals(1, current.rootLevel(), "4 wide rows split the root to level 1");

            // 删除一个非 root leaf 上的 key（id=4 落在右 leaf）
            MiniTransaction d = ctx.mgr.begin();
            BTreeDeleteResult result = svc.deleteClustered(d, current, kId(4),
                    TransactionId.of(TRX), new RollPointer(true, PageNo.of(65), 4));
            ctx.mgr.commit(d);
            assertTrue(result.removed(), "cross-leaf delete must remove the row");

            MiniTransaction r = ctx.mgr.begin();
            assertTrue(svc.lookup(r, current, kId(4)).isEmpty(), "deleted key gone");
            assertTrue(svc.lookup(r, current, kId(1)).isPresent(), "other keys remain queryable");
            assertTrue(svc.lookup(r, current, kId(2)).isPresent());
            assertTrue(svc.lookup(r, current, kId(3)).isPresent());
            ctx.mgr.commit(r);
        });
    }

    @Test
    void multiLevelClusteredDeleteReplaceMarkPurge() {
        onPool(ctx -> {
            ctx.createTablespaceAndRoot();
            SplitCapableBTreeIndexService svc = ctx.service();
            // 宽聚簇 KEY（5000B varchar 主键）→ node pointer ~5KB → 少量行即长出多层树（idKey 太小，root 容纳上百指针）。
            BTreeIndex current = new BTreeIndex(INDEX_ID, ctx.rootPageId, 0, wideKeyDef(), wideKeySchema(), true,
                    ctx.leafSegment, ctx.nonLeafSegment);

            long id = 1;
            while (current.rootLevel() < 2 && id <= 40) {
                MiniTransaction m = ctx.mgr.begin();
                current = svc.insertClustered(m, current, wideKeyRow(id), TransactionId.of(TRX),
                        new RollPointer(true, PageNo.of(65), (int) id)).indexAfterInsert();
                ctx.mgr.commit(m);
                id++;
            }
            long inserted = id - 1;
            assertTrue(current.rootLevel() >= 2, "wide clustered key grows the tree past level 1");
            assertTrue(inserted >= 8, "enough rows span multiple leaves");

            // delete key 3（所有权 = 插入时 (TRX, rp3)）
            MiniTransaction d = ctx.mgr.begin();
            assertTrue(svc.deleteClustered(d, current, kKey(3), TransactionId.of(TRX),
                    new RollPointer(true, PageNo.of(65), 3)).removed(), "multi-level delete removes the row");
            ctx.mgr.commit(d);

            // replace key 5（同 key 换隐藏列；expected=插入版本）
            MiniTransaction rep = ctx.mgr.begin();
            assertTrue(svc.replaceClustered(rep, current, kKey(5),
                    wideKeyRowWithHidden(5, TransactionId.of(TRX), new RollPointer(true, PageNo.of(66), 105)),
                    TransactionId.of(TRX), new RollPointer(true, PageNo.of(65), 5)).replaced(),
                    "multi-level replace updates the matching row");
            ctx.mgr.commit(rep);

            // delete-mark key 7，再 purge
            MiniTransaction mk = ctx.mgr.begin();
            assertTrue(svc.setClusteredDeleteMark(mk, current, kKey(7), true,
                    new HiddenColumns(TransactionId.of(TRX), new RollPointer(true, PageNo.of(66), 207)),
                    TransactionId.of(TRX), new RollPointer(true, PageNo.of(65), 7)).changed(),
                    "multi-level delete-mark flips the matching row");
            ctx.mgr.commit(mk);

            MiniTransaction look = ctx.mgr.begin();
            assertTrue(svc.lookup(look, current, kKey(7)).isEmpty(), "delete-marked hidden from normal lookup");
            assertTrue(svc.lookupIncludingDeleted(look, current, kKey(7)).isPresent(), "still visible to MVCC read");
            ctx.mgr.commit(look);

            MiniTransaction pg = ctx.mgr.begin();
            assertTrue(svc.purgeDeleteMarkedClustered(pg, current, kKey(7), TransactionId.of(TRX),
                    new RollPointer(true, PageNo.of(66), 207)).removed(), "multi-level purge physically removes");
            ctx.mgr.commit(pg);

            // 终态校验：删/purge 的不在、replace 的在、其余仍可查（多层导航全程正确）。
            MiniTransaction r = ctx.mgr.begin();
            assertTrue(svc.lookup(r, current, kKey(3)).isEmpty(), "deleted gone");
            assertTrue(svc.lookup(r, current, kKey(5)).isPresent(), "replaced present");
            assertTrue(svc.lookupIncludingDeleted(r, current, kKey(7)).isEmpty(), "purged gone");
            assertTrue(svc.lookup(r, current, kKey(1)).isPresent(), "untouched key present");
            assertTrue(svc.lookup(r, current, kKey(inserted)).isPresent(), "last key present");
            ctx.mgr.commit(r);
        });
    }

    @Test
    void underfullLeafMergesIntoSiblingAndRemovesParentPointer() {
        onPool(ctx -> {
            ctx.createTablespaceAndRoot();
            SplitCapableBTreeIndexService svc = ctx.service();
            // 宽聚簇 KEY（5000B）→ 每 leaf ~2-3 行、node pointer ~5KB → 6 行长成 level-1 三叶树。
            BTreeIndex current = new BTreeIndex(INDEX_ID, ctx.rootPageId, 0, wideKeyDef(), wideKeySchema(), true,
                    ctx.leafSegment, ctx.nonLeafSegment);
            for (long id = 1; id <= 6; id++) {
                MiniTransaction m = ctx.mgr.begin();
                current = svc.insertClustered(m, current, wideKeyRow(id), TransactionId.of(TRX),
                        new RollPointer(true, PageNo.of(65), (int) id)).indexAfterInsert();
                ctx.mgr.commit(m);
            }
            assertEquals(1, current.rootLevel(), "6 wide-key rows form a level-1 tree");

            // 删最大 key（落在最右 leaf）使其欠载 → merge 进左兄弟、root 少一指针，但仍 ≥2 leaf（不整树 shrink）。
            MiniTransaction d = ctx.mgr.begin();
            BTreeDeleteResult res = svc.deleteClustered(d, current, kKey(6), TransactionId.of(TRX),
                    new RollPointer(true, PageNo.of(65), 6));
            current = res.indexAfter();
            ctx.mgr.commit(d);

            assertTrue(res.removed(), "key 6 must be removed");
            assertFalse(res.freedPages().isEmpty(), "underfull leaf merged into sibling → victim page freed");
            assertEquals(1, current.rootLevel(), "merging two of three leaves keeps the tree at level 1");

            // 剩余 key 跨 sibling 链有序 scan 回（验证 merge 后链与 parent 结构完整）。
            MiniTransaction r = ctx.mgr.begin();
            List<Long> ids = svc.scan(r, current, new BTreeScanRange(kKey(1), true, kKey(6), true, 50))
                    .stream().map(BTreeDeleteClusteredTest::vOf).toList();
            ctx.mgr.commit(r);
            assertEquals(List.of(1L, 2L, 3L, 4L, 5L), ids, "remaining keys intact and ordered after merge");
        });
    }

    // ---- helpers ----

    /** scan 结果取 payload 列（column1 = int v = id）。 */
    private static long vOf(BTreeLookupResult row) {
        return ((ColumnValue.IntValue) row.record().columnValues().get(1)).value();
    }

    /** 宽聚簇 KEY schema：column0 = varchar(5000) 主键、column1 = int payload；clustered=true。 */
    private static TableSchema wideKeySchema() {
        return new TableSchema(1, List.of(
                new ColumnDef(new ColumnId(0), "k", ColumnType.varchar(5000, true), 0),
                new ColumnDef(new ColumnId(1), "v", ColumnType.intType(false, false), 1)), true);
    }

    private static IndexKeyDef wideKeyDef() {
        return new IndexKeyDef(INDEX_ID, List.of(new KeyPartDef(new ColumnId(0), KeyOrder.ASC, 0)));
    }

    /** id 顺序可排序的宽 key 值：%06d 前缀保证按 id 升序，再补到 5000B。 */
    private static String wideKeyValue(long id) {
        String prefix = String.format("%06d", id);
        return prefix + "x".repeat(5000 - prefix.length());
    }

    private static SearchKey kKey(long id) {
        return new SearchKey(List.of(new ColumnValue.StringValue(wideKeyValue(id))));
    }

    private static LogicalRecord wideKeyRow(long id) {
        return new LogicalRecord(1, List.of(new ColumnValue.StringValue(wideKeyValue(id)),
                new ColumnValue.IntValue(id)), false, RecordType.CONVENTIONAL);
    }

    private static LogicalRecord wideKeyRowWithHidden(long id, TransactionId txnId, RollPointer rp) {
        return new LogicalRecord(1, List.of(new ColumnValue.StringValue(wideKeyValue(id)),
                new ColumnValue.IntValue(id)), false, RecordType.CONVENTIONAL, new HiddenColumns(txnId, rp));
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

    private static LogicalRecord row(long id) {
        return new LogicalRecord(1, List.of(new ColumnValue.IntValue(id),
                new ColumnValue.StringValue("payload-" + id)), false, RecordType.CONVENTIONAL);
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
