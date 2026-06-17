package cn.zhangyis.db.storage.btree;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.storage.api.DiskSpaceManager;
import cn.zhangyis.db.storage.api.IndexPageAccess;
import cn.zhangyis.db.storage.api.SegmentRef;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.fil.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.PageStore;
import cn.zhangyis.db.storage.fsp.SegmentPurpose;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import cn.zhangyis.db.storage.record.format.LogicalRecord;
import cn.zhangyis.db.storage.record.format.RecordType;
import cn.zhangyis.db.storage.record.page.RecordCursor;
import cn.zhangyis.db.storage.record.page.RecordPage;
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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * T1.2 聚簇 insert：begin→insertClustered 盖 DB_TRX_ID/NULL roll ptr，split 跨 leaf 保留隐藏列，
 * node-pointer 根记录不带隐藏区，非法入参拒绝。
 */
class ClusteredInsertTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(41);
    private static final long INDEX_ID = 9L;
    private static final long TRX = 123L;

    @TempDir
    Path dir;

    private final TypeCodecRegistry registry = new TypeCodecRegistry();

    @Test
    void clusteredInsertStampsTrxIdAndNullRollPtr() {
        onPool(ctx -> {
            ctx.createTablespaceAndRoot();
            SplitCapableBTreeIndexService svc = ctx.service();
            BTreeIndex index = ctx.clusteredIndex();

            MiniTransaction m = ctx.mgr.begin();
            svc.insertClustered(m, index, wideRow(1), TransactionId.of(TRX));
            ctx.mgr.commit(m);

            MiniTransaction r = ctx.mgr.begin();
            BTreeLookupResult found = svc.lookup(r, index, kId(1)).orElseThrow();
            ctx.mgr.commit(r);

            assertEquals(TransactionId.of(TRX), found.record().hiddenColumns().dbTrxId());
            assertTrue(found.record().hiddenColumns().dbRollPtr().isNull());
        });
    }

    @Test
    void splitPreservesHiddenColumnsAcrossLeavesAndRootHasNoHidden() {
        onPool(ctx -> {
            ctx.createTablespaceAndRoot();
            SplitCapableBTreeIndexService svc = ctx.service();
            BTreeIndex current = ctx.clusteredIndex();

            for (long id = 1; id <= 4; id++) {
                MiniTransaction m = ctx.mgr.begin();
                BTreeInsertResult result = svc.insertClustered(m, current, wideRow(id), TransactionId.of(TRX));
                current = result.indexAfterInsert();
                ctx.mgr.commit(m);
            }
            assertEquals(1, current.rootLevel(), "4 wide rows should split the root");

            // 两个子 leaf 上的记录都仍带正确 DB_TRX_ID（split materialize→reinsert 保留隐藏列）
            for (long id : new long[]{1, 4}) {
                MiniTransaction r = ctx.mgr.begin();
                BTreeLookupResult found = svc.lookup(r, current, kId(id)).orElseThrow();
                ctx.mgr.commit(r);
                assertEquals(TRX, found.record().hiddenColumns().dbTrxId().value());
            }

            // 根页 node-pointer 记录用非 clustered 派生 schema 物化 → 无隐藏区
            BTreeNodePointerSchema pointerSchema = BTreeNodePointerSchema.from(current);
            MiniTransaction read = ctx.mgr.begin();
            RecordPage root = ctx.access.openIndexPage(read, current.rootPageId(), PageLatchMode.SHARED);
            for (int off : root.recordOffsetsInOrder()) {
                LogicalRecord rec = new RecordCursor(root, off, pointerSchema.schema(), registry).materialize();
                assertNull(rec.hiddenColumns(), "node-pointer records must not carry hidden columns");
            }
            ctx.mgr.commit(read);
        });
    }

    @Test
    void rejectsNoneTransactionId() {
        onPool(ctx -> {
            SplitCapableBTreeIndexService svc = ctx.service();
            BTreeIndex index = ctx.clusteredIndexNoSegments();
            MiniTransaction m = ctx.mgr.begin();
            assertThrows(DatabaseValidationException.class,
                    () -> svc.insertClustered(m, index, wideRow(1), TransactionId.NONE));
            ctx.mgr.rollbackUncommitted(m);
        });
    }

    @Test
    void rejectsNonClusteredIndex() {
        onPool(ctx -> {
            SplitCapableBTreeIndexService svc = ctx.service();
            BTreeIndex nonClustered = new BTreeIndex(INDEX_ID, PageId.of(SPACE, PageNo.of(3)), 0,
                    idKey(), nonClusteredSchema(), true);
            MiniTransaction m = ctx.mgr.begin();
            assertThrows(DatabaseValidationException.class,
                    () -> svc.insertClustered(m, nonClustered, smallRow(1), TransactionId.of(TRX)));
            ctx.mgr.rollbackUncommitted(m);
        });
    }

    // ---- helpers ----

    private static TableSchema clusteredSchema() {
        return new TableSchema(1, List.of(
                new ColumnDef(new ColumnId(0), "id", ColumnType.intType(false, false), 0),
                new ColumnDef(new ColumnId(1), "payload", ColumnType.varchar(5000, true), 1)), true);
    }

    private static TableSchema nonClusteredSchema() {
        return new TableSchema(1, List.of(
                new ColumnDef(new ColumnId(0), "id", ColumnType.intType(false, false), 0)), false);
    }

    private static IndexKeyDef idKey() {
        return new IndexKeyDef(INDEX_ID, List.of(new KeyPartDef(new ColumnId(0), KeyOrder.ASC, 0)));
    }

    private static SearchKey kId(long id) {
        return new SearchKey(List.of(new ColumnValue.IntValue(id)));
    }

    private static LogicalRecord wideRow(long id) {
        return new LogicalRecord(1, List.of(new ColumnValue.IntValue(id),
                new ColumnValue.StringValue("x".repeat(5000))), false, RecordType.CONVENTIONAL);
    }

    private static LogicalRecord smallRow(long id) {
        return new LogicalRecord(1, List.of(new ColumnValue.IntValue(id),
                new ColumnValue.StringValue("v")), false, RecordType.CONVENTIONAL);
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

        /** 拒绝路径用：在校验阶段就抛出，不触页，segment 可为空。 */
        private BTreeIndex clusteredIndexNoSegments() {
            return new BTreeIndex(INDEX_ID, PageId.of(SPACE, PageNo.of(3)), 0, idKey(), clusteredSchema(), true);
        }
    }
}
