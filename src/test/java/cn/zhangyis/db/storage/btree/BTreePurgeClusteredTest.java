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

    // ---- helpers (mirror BTreeDeleteMarkTest harness) ----

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
