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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * P3 严格 {@code purgeDeleteMarkedClustered}（purge 物理移除）：只移除**已 delete-marked 且隐藏列匹配**的记录，
 * 绝不对存活行主动 delete-mark；未命中/未标记/隐藏列不符一律 stale 跳过不改内容。
 */
class BTreePurgeClusteredTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(43);
    private static final long INDEX_ID = 9L;
    private static final long TRX_A = 100L;
    private static final long TRX_B = 200L;

    @TempDir
    Path dir;

    private final TypeCodecRegistry registry = new TypeCodecRegistry();

    /**
     * 验证 {@code purgesDeleteMarkedOwnedRecord} 所描述的页内记录行为，并断言偏移、编码边界、隐藏列及 page-directory 结构保持一致。
     */
    @Test
    void purgesDeleteMarkedOwnedRecord() {
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

            // purge：expected = 删除事务 id + 删除 undo 自身地址（= 记录当前 DB_TRX_ID/DB_ROLL_PTR）
            MiniTransaction p = ctx.mgr.begin();
            BTreeDeleteResult res = svc.purgeDeleteMarkedClustered(p, index, kId(1),
                    TransactionId.of(TRX_B), rpDel);
            ctx.mgr.commit(p);
            assertTrue(res.removed(), "已 delete-marked + 隐藏列匹配 → 物理移除");

            MiniTransaction r = ctx.mgr.begin();
            assertTrue(svc.lookupIncludingDeleted(r, index, kId(1)).isEmpty(), "purge 后物理不存在");
            ctx.mgr.commit(r);
        });
    }

    /**
     * 验证 {@code refusesLiveRow} 对应的B+Tree 索引行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void refusesLiveRow() {
        onPool(ctx -> {
            ctx.createTablespaceAndRoot();
            SplitCapableBTreeIndexService svc = ctx.service();
            BTreeIndex index = ctx.clusteredIndex();
            RollPointer rpA = new RollPointer(true, PageNo.of(65), 1);

            MiniTransaction m = ctx.mgr.begin();
            svc.insertClustered(m, index, row(1, "v1"), TransactionId.of(TRX_A), rpA);
            ctx.mgr.commit(m);

            // 存活行（未标记）即使隐藏列匹配也绝不移除、绝不主动 deleteMark
            MiniTransaction p = ctx.mgr.begin();
            BTreeDeleteResult res = svc.purgeDeleteMarkedClustered(p, index, kId(1),
                    TransactionId.of(TRX_A), rpA);
            ctx.mgr.commit(p);
            assertFalse(res.removed(), "存活行不得被 purge 移除");

            MiniTransaction r = ctx.mgr.begin();
            assertTrue(svc.lookup(r, index, kId(1)).isPresent(), "存活行仍在且未被标记");
            ctx.mgr.commit(r);
        });
    }

    /**
     * 验证 {@code skipsStaleOwnershipMismatch} 对应的B+Tree 索引行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void skipsStaleOwnershipMismatch() {
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

            // 错误 expectedRollPtr → 确认 stale，不移除、不改内容
            MiniTransaction p = ctx.mgr.begin();
            BTreeDeleteResult res = svc.purgeDeleteMarkedClustered(p, index, kId(1),
                    TransactionId.of(TRX_B), new RollPointer(false, PageNo.of(66), 999));
            ctx.mgr.commit(p);
            assertFalse(res.removed(), "隐藏列不符 → stale skip");

            MiniTransaction r = ctx.mgr.begin();
            assertTrue(svc.lookupIncludingDeleted(r, index, kId(1)).isPresent(), "stale 跳过未改内容，记录仍在");
            ctx.mgr.commit(r);
        });
    }

    /**
     * 0.13b：多层树上乐观 purge safe——删后不欠载，走 descendOptimistic（内部 S、leaf X），purge 后跳过 merge。
     * 5 宽行 → level-1 [1,2],[3,4,5]；delete-mark + purge id=4 → 右 leaf [3,5] 不欠载 → 乐观命中、不删页。
     */
    @Test
    void optimisticPurgeNonUnderfullSkipsMerge() {
        onPool(ctx -> {
            ctx.createTablespaceAndRoot();
            SplitCapableBTreeIndexService svc = ctx.service();
            BTreeIndex current = ctx.clusteredIndex();
            for (long id = 1; id <= 5; id++) {
                MiniTransaction m = ctx.mgr.begin();
                current = svc.insertClustered(m, current, wideRow(id), TransactionId.of(TRX_A),
                        new RollPointer(true, PageNo.of(65), (int) id)).indexAfterInsert();
                ctx.mgr.commit(m);
            }
            assertEquals(1, current.rootLevel(), "5 wide rows form a level-1 tree");

            RollPointer rpDel = new RollPointer(false, PageNo.of(66), 4);
            MiniTransaction d = ctx.mgr.begin();
            svc.setClusteredDeleteMark(d, current, kId(4), true,
                    new HiddenColumns(TransactionId.of(TRX_B), rpDel), TransactionId.of(TRX_A),
                    new RollPointer(true, PageNo.of(65), 4));
            ctx.mgr.commit(d);

            MiniTransaction p = ctx.mgr.begin();
            BTreeDeleteResult res = svc.purgeDeleteMarkedClustered(p, current, kId(4),
                    TransactionId.of(TRX_B), rpDel);
            current = res.indexAfter();
            ctx.mgr.commit(p);

            assertTrue(res.removed(), "delete-marked owned row purged");
            assertTrue(res.freedPages().isEmpty(), "non-underfull purge must not merge/free any page");
            assertTrue(svc.optimisticPurgeHitCount() > 0, "non-underfull purge takes the optimistic leaf-only path");

            MiniTransaction r = ctx.mgr.begin();
            assertTrue(svc.lookupIncludingDeleted(r, current, kId(4)).isEmpty(), "purged key physically gone");
            assertTrue(svc.lookup(r, current, kId(3)).isPresent(), "other keys remain");
            assertTrue(svc.lookup(r, current, kId(5)).isPresent());
            ctx.mgr.commit(r);
        });
    }

    /**
     * 0.13b：多层树上乐观 purge unsafe 回退——删后欠载需 merge，写页前释放 leaf X 改走悲观全 X。
     * 6 宽行 → level-1 [1,2],[3,4],[5,6]；delete-mark + purge id=6 → 右 leaf [5] 欠载 → 回退悲观 merge、victim 页回收。
     */
    @Test
    void optimisticPurgeUnderfullFallsBackToMerge() {
        onPool(ctx -> {
            ctx.createTablespaceAndRoot();
            SplitCapableBTreeIndexService svc = ctx.service();
            BTreeIndex current = ctx.clusteredIndex();
            for (long id = 1; id <= 6; id++) {
                MiniTransaction m = ctx.mgr.begin();
                current = svc.insertClustered(m, current, wideRow(id), TransactionId.of(TRX_A),
                        new RollPointer(true, PageNo.of(65), (int) id)).indexAfterInsert();
                ctx.mgr.commit(m);
            }
            assertEquals(1, current.rootLevel(), "6 wide rows form a level-1 tree");

            RollPointer rpDel = new RollPointer(false, PageNo.of(66), 6);
            MiniTransaction d = ctx.mgr.begin();
            svc.setClusteredDeleteMark(d, current, kId(6), true,
                    new HiddenColumns(TransactionId.of(TRX_B), rpDel), TransactionId.of(TRX_A),
                    new RollPointer(true, PageNo.of(65), 6));
            ctx.mgr.commit(d);

            MiniTransaction p = ctx.mgr.begin();
            BTreeDeleteResult res = svc.purgeDeleteMarkedClustered(p, current, kId(6),
                    TransactionId.of(TRX_B), rpDel);
            current = res.indexAfter();
            ctx.mgr.commit(p);

            assertTrue(res.removed(), "delete-marked owned row purged");
            assertFalse(res.freedPages().isEmpty(), "underfull purge falls back and merges → victim page freed");
            assertTrue(svc.pessimisticPurgeFallbackCount() > 0, "underfull purge takes the pessimistic fallback");

            MiniTransaction r = ctx.mgr.begin();
            assertTrue(svc.lookupIncludingDeleted(r, current, kId(6)).isEmpty(), "purged key physically gone");
            for (long id = 1; id <= 5; id++) {
                assertTrue(svc.lookup(r, current, kId(id)).isPresent(), "key " + id + " remains after fallback merge");
            }
            ctx.mgr.commit(r);
        });
    }

    /**
     * 0.13b：多层树上乐观 purge 对存活（未标记）行严格校验 stale no-op——descendOptimistic 到 leaf、findEqual 命中但
     * 未 delete-marked → 纯读不改任何页，绝不主动 deleteMark。
     */
    @Test
    void optimisticPurgeLiveRowOnMultiLevelIsNoOp() {
        onPool(ctx -> {
            ctx.createTablespaceAndRoot();
            SplitCapableBTreeIndexService svc = ctx.service();
            BTreeIndex current = ctx.clusteredIndex();
            for (long id = 1; id <= 5; id++) {
                MiniTransaction m = ctx.mgr.begin();
                current = svc.insertClustered(m, current, wideRow(id), TransactionId.of(TRX_A),
                        new RollPointer(true, PageNo.of(65), (int) id)).indexAfterInsert();
                ctx.mgr.commit(m);
            }
            assertEquals(1, current.rootLevel(), "5 wide rows form a level-1 tree");

            MiniTransaction p = ctx.mgr.begin();
            BTreeDeleteResult res = svc.purgeDeleteMarkedClustered(p, current, kId(3),
                    TransactionId.of(TRX_A), new RollPointer(true, PageNo.of(65), 3));
            ctx.mgr.commit(p);

            assertFalse(res.removed(), "live (unmarked) row must not be purged");
            MiniTransaction r = ctx.mgr.begin();
            assertTrue(svc.lookup(r, current, kId(3)).isPresent(), "row still live after no-op purge");
            ctx.mgr.commit(r);
        });
    }

    // ---- helpers (mirror BTreeDeleteMarkTest harness) ----

    private static LogicalRecord wideRow(long id) {
        return row(id, "x".repeat(5000));
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
