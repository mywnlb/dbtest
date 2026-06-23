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
