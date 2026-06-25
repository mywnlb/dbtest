package cn.zhangyis.db.storage.btree;

import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SegmentId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.api.DiskSpaceManager;
import cn.zhangyis.db.storage.api.index.IndexPageAccess;
import cn.zhangyis.db.storage.api.index.IndexPageHandle;
import cn.zhangyis.db.storage.api.SegmentRef;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.fil.io.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.fsp.segment.SegmentPurpose;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import cn.zhangyis.db.storage.page.FilePageHeader;
import cn.zhangyis.db.storage.page.PageType;
import cn.zhangyis.db.storage.record.format.LogicalRecord;
import cn.zhangyis.db.storage.record.format.RecordType;
import cn.zhangyis.db.storage.record.page.RecordCursor;
import cn.zhangyis.db.storage.record.page.RecordPage;
import cn.zhangyis.db.storage.record.page.RecordRef;
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
import cn.zhangyis.db.storage.redo.RedoApplyContext;
import cn.zhangyis.db.storage.redo.RedoApplyDispatcher;
import cn.zhangyis.db.storage.redo.RedoLogBatch;
import cn.zhangyis.db.storage.redo.RedoLogFileRepository;
import cn.zhangyis.db.storage.redo.RedoRecoveryReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * B3 split-capable B+Tree 测试。覆盖 root leaf split、level-1 root-to-leaf 路由、
 * leaf sibling range scan 与 node pointer 编码，不把测试数据直接写进 leaf 页以绕过空间管理。
 */
class SplitCapableBTreeIndexServiceTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(31);
    private static final long INDEX_ID = 7L;
    private static final PageId LEGACY_ROOT = PageId.of(SPACE, PageNo.of(3));

    @TempDir
    Path dir;

    private final TypeCodecRegistry registry = new TypeCodecRegistry();

    @Test
    void btreeIndexKeepsNoSplitConstructorAndCanCarrySegments() {
        BTreeIndex legacy = new BTreeIndex(INDEX_ID, LEGACY_ROOT, 0, idKey(), wideSchema(), true);
        assertEquals(LEGACY_ROOT, legacy.rootPageId());

        SegmentRef leaf = new SegmentRef(SPACE, 0, SegmentId.of(1));
        SegmentRef nonLeaf = new SegmentRef(SPACE, 1, SegmentId.of(2));
        BTreeIndex split = new BTreeIndex(INDEX_ID, LEGACY_ROOT, 0, idKey(), wideSchema(), true, leaf, nonLeaf);

        assertEquals(leaf, split.leafSegment());
        assertEquals(nonLeaf, split.nonLeafSegment());
    }

    @Test
    void insertResultKeepsOldConstructorAndExposesSplitMetadata() {
        BTreeIndex idx = new BTreeIndex(INDEX_ID, LEGACY_ROOT, 0, idKey(), wideSchema(), true);
        RecordRef ref = new RecordRef(LEGACY_ROOT, 2, 128, 1, INDEX_ID);

        BTreeInsertResult legacy = new BTreeInsertResult(idx, ref);

        assertEquals(idx, legacy.indexAfterInsert());
        assertFalse(legacy.splitOccurred());
        assertEquals(List.of(), legacy.allocatedPages());
    }

    @Test
    void indexPageHandleUpdatesSiblingLinksWithoutRewritingWholeHeader() {
        onBTreePool((ctx) -> {
            ctx.createTablespaceAndRoot();

            MiniTransaction m = ctx.mgr.begin();
            IndexPageHandle handle = ctx.access.openIndexPageHandle(m, ctx.rootPageId, PageLatchMode.EXCLUSIVE);
            handle.writeSiblingLinks(11, 12);
            ctx.mgr.commit(m);

            MiniTransaction read = ctx.mgr.begin();
            IndexPageHandle loaded = ctx.access.openIndexPageHandle(read, ctx.rootPageId, PageLatchMode.SHARED);
            assertEquals(11L, loaded.fileHeader().prevPageNo());
            assertEquals(12L, loaded.fileHeader().nextPageNo());
            assertEquals(INDEX_ID, loaded.recordPage().header().indexId());
            assertEquals(PageType.INDEX, loaded.fileHeader().pageType());
            ctx.mgr.commit(read);
        });
    }

    @Test
    void nodePointerRoundTripsThroughDerivedSchema() {
        BTreeNodePointerSchema pointerSchema = BTreeNodePointerSchema.from(
                new BTreeIndex(INDEX_ID, LEGACY_ROOT, 0, idKey(), wideSchema(), true));
        PageId child = PageId.of(SPACE, PageNo.of(9));
        BTreeNodePointer pointer = new BTreeNodePointer(kId(42), child);

        LogicalRecord encoded = new BTreeNodePointerCodec().toRecord(pointer, pointerSchema);
        BTreeNodePointer decoded = new BTreeNodePointerCodec().fromRecord(encoded, pointerSchema);

        assertEquals(RecordType.NODE_POINTER, encoded.recordType());
        assertEquals(pointer, decoded);
        assertEquals(1, pointerSchema.childSpaceColumnOrdinal());
        assertEquals(2, pointerSchema.childPageColumnOrdinal());
    }

    @Test
    void rootLeafOverflowSplitsStableRootAndLookupFindsBothLeaves() {
        onBTreePool((ctx) -> {
            BTreeIndex current = ctx.insertWideRows(1, 4);

            assertEquals(1, current.rootLevel());
            assertEquals(ctx.rootPageId, current.rootPageId());
            assertFound(ctx, current, 1);
            assertFound(ctx, current, 4);
        });
    }

    @Test
    void rootSplitLinksChildLeavesAndRangeScanCrossesSibling() {
        onBTreePool((ctx) -> {
            BTreeIndex current = ctx.insertWideRows(1, 4);
            BTreeIndexService service = ctx.service();

            MiniTransaction scan = ctx.mgr.begin();
            List<Long> ids = service.scan(scan, current,
                            new BTreeScanRange(kId(1), true, kId(5), false, 20))
                    .stream().map(SplitCapableBTreeIndexServiceTest::idOf).toList();
            ctx.mgr.commit(scan);

            assertEquals(List.of(1L, 2L, 3L, 4L), ids);
            assertLeafLinksAreBidirectional(ctx, current);
        });
    }

    @Test
    void levelOneInsertSplitsOnlyTargetLeafAndUpdatesRootPointers() {
        onBTreePool((ctx) -> {
            BTreeIndex current = ctx.insertWideRows(1, 4);
            BTreeIndexService service = ctx.service();

            for (long id = 5; id <= 8; id++) {
                MiniTransaction m = ctx.mgr.begin();
                BTreeInsertResult result = service.insert(m, current, wideRow(id));
                current = result.indexAfterInsert();
                ctx.mgr.commit(m);
            }

            assertEquals(1, current.rootLevel());
            assertFound(ctx, current, 8);
            assertLeafLinksAreBidirectional(ctx, current);
        });
    }

    @Test
    void parentOverflowGrowsTreeInsteadOfFailing() {
        onBTreePool((ctx) -> {
            ctx.createTablespaceAndRoot();
            // 宽 node pointer 灌满 level-1 root 后，0.11 之前在父页不足时失败；现在 parent/root split 接管，
            // 树长高且全部记录可经多层 scan 取回（有序）。
            BTreeIndex current = new BTreeIndex(INDEX_ID, ctx.rootPageId, 0, payloadKey(), wideSchema(), true,
                    ctx.leafSegment, ctx.nonLeafSegment);
            BTreeIndexService service = ctx.service();
            List<Long> inserted = new ArrayList<>();
            for (long id = 1; id <= 12; id++) {
                MiniTransaction m = ctx.mgr.begin();
                BTreeInsertResult result = service.insert(m, current, payloadKeyRow(id));
                current = result.indexAfterInsert();
                ctx.mgr.commit(m);
                inserted.add(id);
            }

            assertTrue(current.rootLevel() >= 2, "wide node pointers grow the tree past level 1 via parent split");
            MiniTransaction read = ctx.mgr.begin();
            List<Long> ids = service.scan(read, current,
                            new BTreeScanRange(kPayload(1), true, kPayload(99), true, 100))
                    .stream().map(SplitCapableBTreeIndexServiceTest::idOf).toList();
            ctx.mgr.commit(read);
            assertEquals(inserted, ids);
        });
    }

    @Test
    void redoReplayRestoresSplitRootAndSiblingScan() {
        onBTreePool((ctx) -> {
            BTreeIndex current = ctx.insertWideRows(1, 4);
            Path redoPath = dir.resolve("btree-split-redo.log");
            persistRedoBatches(redoPath, ctx.mgr.redoLogManager().bufferedBatches());

            PageStore recoveredStore = new FileChannelPageStore();
            recoveredStore.create(SPACE, dir.resolve("btree-recovered.ibd"), PS,
                    ctx.store.currentSizeInPages(SPACE));
            try (PageStore ignored = recoveredStore;
                 RedoLogFileRepository redo = RedoLogFileRepository.open(redoPath)) {
                RedoRecoveryReader reader = new RedoRecoveryReader(redo);
                RedoApplyDispatcher.pageDispatcher().applyAll(reader.readBatches(),
                        new RedoApplyContext(recoveredStore, PS));

                try (BufferPool recoveredPool = new LruBufferPool(recoveredStore, PS, 128)) {
                    IndexPageAccess recoveredAccess = new IndexPageAccess(recoveredPool, PS);
                    DiskSpaceManager recoveredDisk = new DiskSpaceManager(recoveredPool, recoveredStore, PS);
                    BTreeIndexService recoveredService = new SplitCapableBTreeIndexService(recoveredAccess,
                            recoveredDisk, registry);
                    MiniTransactionManager recoveredMtr = new MiniTransactionManager();
                    MiniTransaction read = recoveredMtr.begin();
                    List<Long> ids = recoveredService.scan(read, current,
                                    new BTreeScanRange(kId(1), true, kId(5), false, 20))
                            .stream().map(SplitCapableBTreeIndexServiceTest::idOf).toList();
                    recoveredMtr.commit(read);
                    assertEquals(List.of(1L, 2L, 3L, 4L), ids);
                }
            }
        });
    }

    @Test
    void rootSplitGrowsTreeToLevelTwo() {
        onBTreePool((ctx) -> {
            ctx.createTablespaceAndRoot();
            // payloadKey()：5000B key → node pointer ~5KB → level-1 root 仅容 ~3 指针，少量行即触发原地 root split。
            BTreeIndex current = new BTreeIndex(INDEX_ID, ctx.rootPageId, 0, payloadKey(), wideSchema(), true,
                    ctx.leafSegment, ctx.nonLeafSegment);
            BTreeIndexService service = ctx.service();
            long id = 1;
            while (current.rootLevel() < 2 && id <= 40) {
                MiniTransaction m = ctx.mgr.begin();
                BTreeInsertResult result = service.insert(m, current, payloadKeyRow(id));
                current = result.indexAfterInsert();
                ctx.mgr.commit(m);
                id++;
            }

            assertEquals(2, current.rootLevel(), "wide node pointers force an in-place root split to level 2");
            assertEquals(ctx.rootPageId, current.rootPageId(), "root page id stays stable across split");

            long inserted = id - 1;
            for (long k = 1; k <= inserted; k++) {
                MiniTransaction read = ctx.mgr.begin();
                BTreeLookupResult found = service.lookup(read, current, kPayload(k)).orElseThrow();
                assertEquals(k, idOf(found));
                ctx.mgr.commit(read);
            }
        });
    }

    @Test
    void internalNonRootSplitPropagatesToLevelThree() {
        onBTreePool((ctx) -> {
            ctx.createTablespaceAndRoot();
            BTreeIndex current = new BTreeIndex(INDEX_ID, ctx.rootPageId, 0, payloadKey(), wideSchema(), true,
                    ctx.leafSegment, ctx.nonLeafSegment);
            BTreeIndexService service = ctx.service();
            // 一路灌到 level 3：途中必然发生非 root 内部页 split（level-1 页满 → separator 上插 level-2 root）。
            long id = 1;
            while (current.rootLevel() < 3 && id <= 60) {
                MiniTransaction m = ctx.mgr.begin();
                BTreeInsertResult result = service.insert(m, current, payloadKeyRow(id));
                current = result.indexAfterInsert();
                ctx.mgr.commit(m);
                id++;
            }

            assertEquals(3, current.rootLevel(), "deep inserts grow the tree to level 3");
            assertEquals(ctx.rootPageId, current.rootPageId(), "root page id stable across all splits");
            long inserted = id - 1;
            for (long k = 1; k <= inserted; k++) {
                MiniTransaction read = ctx.mgr.begin();
                assertEquals(k, idOf(service.lookup(read, current, kPayload(k)).orElseThrow()),
                        "every key reachable through 3-level navigation");
                ctx.mgr.commit(read);
            }
        });
    }

    @Test
    void multiLevelScanReturnsAllInOrder() {
        onBTreePool((ctx) -> {
            ctx.createTablespaceAndRoot();
            BTreeIndex current = new BTreeIndex(INDEX_ID, ctx.rootPageId, 0, payloadKey(), wideSchema(), true,
                    ctx.leafSegment, ctx.nonLeafSegment);
            BTreeIndexService service = ctx.service();
            long id = 1;
            // 长到 level≥2 后再多灌几行，确保多个 leaf 跨 sibling 链
            while ((current.rootLevel() < 2 || id <= 18) && id <= 40) {
                MiniTransaction m = ctx.mgr.begin();
                current = service.insert(m, current, payloadKeyRow(id)).indexAfterInsert();
                ctx.mgr.commit(m);
                id++;
            }
            long inserted = id - 1;
            assertTrue(current.rootLevel() >= 2, "tree is multi-level");

            List<Long> expected = new ArrayList<>();
            for (long k = 1; k <= inserted; k++) {
                expected.add(k);
            }
            MiniTransaction read = ctx.mgr.begin();
            List<Long> ids = service.scan(read, current,
                            new BTreeScanRange(kPayload(1), true, kPayload(9999), true, 200))
                    .stream().map(SplitCapableBTreeIndexServiceTest::idOf).toList();
            ctx.mgr.commit(read);
            assertEquals(expected, ids, "multi-level scan crosses all leaves in key order");
        });
    }

    private void assertFound(BTreeContext ctx, BTreeIndex index, long id) {
        MiniTransaction read = ctx.mgr.begin();
        try {
            BTreeLookupResult found = ctx.service().lookup(read, index, kId(id)).orElseThrow();
            assertEquals(id, idOf(found));
            ctx.mgr.commit(read);
        } catch (Throwable t) {
            ctx.mgr.rollbackUncommitted(read);
            throw t;
        }
    }

    private void assertLeafLinksAreBidirectional(BTreeContext ctx, BTreeIndex index) {
        List<BTreeNodePointer> pointers = rootPointers(ctx, index);
        assertTrue(pointers.size() >= 2, "split root should contain at least two child pointers");
        for (int i = 0; i < pointers.size(); i++) {
            MiniTransaction read = ctx.mgr.begin();
            IndexPageHandle leaf = ctx.access.openIndexPageHandle(read, pointers.get(i).childPageId(),
                    PageLatchMode.SHARED);
            FilePageHeader h = leaf.fileHeader();
            long expectedPrev = i == 0 ? FilePageHeader.FIL_NULL : pointers.get(i - 1).childPageId().pageNo().value();
            long expectedNext = i == pointers.size() - 1
                    ? FilePageHeader.FIL_NULL
                    : pointers.get(i + 1).childPageId().pageNo().value();
            assertEquals(expectedPrev, h.prevPageNo());
            assertEquals(expectedNext, h.nextPageNo());
            ctx.mgr.commit(read);
        }
    }

    private List<BTreeNodePointer> rootPointers(BTreeContext ctx, BTreeIndex index) {
        MiniTransaction read = ctx.mgr.begin();
        BTreeNodePointerSchema pointerSchema = BTreeNodePointerSchema.from(index);
        BTreeNodePointerCodec codec = new BTreeNodePointerCodec();
        List<BTreeNodePointer> pointers = new ArrayList<>();
        try {
            RecordPage root = ctx.access.openIndexPage(read, index.rootPageId(), PageLatchMode.SHARED);
            for (int off : root.recordOffsetsInOrder()) {
                LogicalRecord record = new RecordCursor(root, off, pointerSchema.schema(), registry).materialize();
                pointers.add(codec.fromRecord(record, pointerSchema));
            }
            ctx.mgr.commit(read);
            return pointers;
        } catch (Throwable t) {
            ctx.mgr.rollbackUncommitted(read);
            throw t;
        }
    }

    private static TableSchema wideSchema() {
        return new TableSchema(1, List.of(
                new ColumnDef(new ColumnId(0), "id", ColumnType.intType(false, false), 0),
                new ColumnDef(new ColumnId(1), "payload", ColumnType.varchar(5000, true), 1)));
    }

    private static IndexKeyDef idKey() {
        return new IndexKeyDef(INDEX_ID, List.of(new KeyPartDef(new ColumnId(0), KeyOrder.ASC, 0)));
    }

    private static IndexKeyDef payloadKey() {
        return new IndexKeyDef(INDEX_ID, List.of(new KeyPartDef(new ColumnId(1), KeyOrder.ASC, 0)));
    }

    private static SearchKey kId(long id) {
        return new SearchKey(List.of(new ColumnValue.IntValue(id)));
    }

    private static SearchKey kPayload(long id) {
        return new SearchKey(List.of(new ColumnValue.StringValue(payloadValue(id))));
    }

    private static LogicalRecord wideRow(long id) {
        return new LogicalRecord(1, List.of(new ColumnValue.IntValue(id), new ColumnValue.StringValue("x".repeat(5000))),
                false, RecordType.CONVENTIONAL);
    }

    private static LogicalRecord payloadKeyRow(long id) {
        return new LogicalRecord(1, List.of(new ColumnValue.IntValue(id), new ColumnValue.StringValue(payloadValue(id))),
                false, RecordType.CONVENTIONAL);
    }

    private static String payloadValue(long id) {
        String prefix = String.format("%04d", id);
        return prefix + "x".repeat(5000 - prefix.length());
    }

    private static long idOf(BTreeLookupResult row) {
        return ((ColumnValue.IntValue) row.record().columnValues().get(0)).value();
    }

    private static void persistRedoBatches(Path redoPath, List<RedoLogBatch> batches) {
        try (RedoLogFileRepository repo = RedoLogFileRepository.open(redoPath)) {
            for (RedoLogBatch batch : batches) {
                repo.append(batch);
            }
            repo.force();
        }
    }

    private void onBTreePool(Body body) {
        PageStore store = new FileChannelPageStore();
        try (PageStore ignored = store; BufferPool pool = new LruBufferPool(store, PS, 128)) {
            BTreeContext ctx = new BTreeContext(store, pool);
            body.run(ctx);
        }
    }

    private interface Body {
        void run(BTreeContext ctx);
    }

    private final class BTreeContext {
        private final PageStore store;
        private final BufferPool pool;
        private final MiniTransactionManager mgr = new MiniTransactionManager();
        private final DiskSpaceManager disk;
        private final IndexPageAccess access;
        private SegmentRef leafSegment;
        private SegmentRef nonLeafSegment;
        private PageId rootPageId;

        private BTreeContext(PageStore store, BufferPool pool) {
            this.store = store;
            this.pool = pool;
            this.disk = new DiskSpaceManager(pool, store, PS);
            this.access = new IndexPageAccess(pool, PS);
        }

        private void createTablespaceAndRoot() {
            MiniTransaction m = mgr.begin();
            disk.createTablespace(m, SPACE, dir.resolve("btree-split.ibd"), PageNo.of(64));
            leafSegment = disk.createSegment(m, SPACE, SegmentPurpose.INDEX_LEAF);
            nonLeafSegment = disk.createSegment(m, SPACE, SegmentPurpose.INDEX_NON_LEAF);
            rootPageId = disk.allocatePage(m, leafSegment);
            access.createIndexPage(m, rootPageId, INDEX_ID, 0);
            mgr.commit(m);
        }

        private BTreeIndex splitIndex() {
            return new BTreeIndex(INDEX_ID, rootPageId, 0, idKey(), wideSchema(), true,
                    leafSegment, nonLeafSegment);
        }

        private BTreeIndexService service() {
            return new SplitCapableBTreeIndexService(access, disk, registry);
        }

        private BTreeIndex insertWideRows(long firstInclusive, long lastInclusive) {
            createTablespaceAndRoot();
            BTreeIndex current = splitIndex();
            BTreeIndexService service = service();
            for (long id = firstInclusive; id <= lastInclusive; id++) {
                MiniTransaction m = mgr.begin();
                BTreeInsertResult result = service.insert(m, current, wideRow(id));
                current = result.indexAfterInsert();
                mgr.commit(m);
            }
            return current;
        }
    }
}
