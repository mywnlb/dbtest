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
import cn.zhangyis.db.storage.fil.access.TablespaceAccessController;
import cn.zhangyis.db.storage.fil.io.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.flush.CoordinatedDirtyVictimFlusher;
import cn.zhangyis.db.storage.flush.FlushCoordinator;
import cn.zhangyis.db.storage.flush.doublewrite.NoDoublewriteStrategy;
import cn.zhangyis.db.storage.fsp.exception.NoFreeSpaceException;
import cn.zhangyis.db.storage.fsp.segment.SegmentPurpose;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import cn.zhangyis.db.storage.page.FilePageHeader;
import cn.zhangyis.db.storage.page.PageType;
import cn.zhangyis.db.storage.record.format.LogicalRecord;
import cn.zhangyis.db.storage.record.format.RecordEncoder;
import cn.zhangyis.db.storage.record.format.RecordType;
import cn.zhangyis.db.storage.record.page.RecordCursor;
import cn.zhangyis.db.storage.record.page.RecordKeyOrderCorruptedException;
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
import cn.zhangyis.db.storage.redo.BTreePageDeltaKind;
import cn.zhangyis.db.storage.redo.BTreePageDeltaRecord;
import cn.zhangyis.db.storage.redo.PageBytesRecord;
import cn.zhangyis.db.storage.redo.RedoApplyContext;
import cn.zhangyis.db.storage.redo.RedoApplyDispatcher;
import cn.zhangyis.db.storage.redo.RedoLogBatch;
import cn.zhangyis.db.storage.redo.RedoLogFileRepository;
import cn.zhangyis.db.storage.redo.RedoRecoveryReader;
import cn.zhangyis.db.storage.redo.RedoLogManager;
import cn.zhangyis.db.storage.redo.RedoRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

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

    /** SplitCapable 必须在选择 child 前拒绝物理合法但 low-key 逆序的 internal root。 */
    @Test
    void lookupRejectsSemanticallyCorruptedInternalPageBeforeNavigation() {
        onBTreePool((ctx) -> {
            BTreeIndex current = ctx.insertWideRows(1, 4);
            BTreeNodePointerSchema pointerSchema = BTreeNodePointerSchema.from(current);
            MiniTransaction corrupt = ctx.mgr.begin();
            RecordPage root = ctx.access.openIndexPage(corrupt, current.rootPageId(), PageLatchMode.EXCLUSIVE);
            int first = root.recordOffsetsInOrder().get(0);
            BTreeNodePointer original = new BTreeNodePointerCodec().fromRecord(
                    new RecordCursor(root, first, pointerSchema.schema(), registry).materialize(), pointerSchema);
            LogicalRecord replacement = new BTreeNodePointerCodec().toRecord(
                    new BTreeNodePointer(kId(999), original.childPageId()), pointerSchema);
            replaceKeyField(root, first, replacement, pointerSchema.schema(), new ColumnId(0));
            ctx.mgr.commit(corrupt);

            MiniTransaction read = ctx.mgr.begin();
            assertThrows(RecordKeyOrderCorruptedException.class,
                    () -> ctx.service().lookup(read, current, kId(2)));
            ctx.mgr.rollbackUncommitted(read);
        });
    }

    /** SplitCapable 下降到 leaf 后必须先验证页内顺序，再执行 record search。 */
    @Test
    void lookupRejectsSemanticallyCorruptedLeafAfterDescent() {
        onBTreePool((ctx) -> {
            BTreeIndex current = ctx.insertWideRows(1, 4);
            PageId firstLeaf = rootPointers(ctx, current).get(0).childPageId();
            MiniTransaction corrupt = ctx.mgr.begin();
            RecordPage leaf = ctx.access.openIndexPage(corrupt, firstLeaf, PageLatchMode.EXCLUSIVE);
            int first = leaf.recordOffsetsInOrder().get(0);
            replaceKeyField(leaf, first, wideRow(999), current.schema(), new ColumnId(0));
            ctx.mgr.commit(corrupt);

            MiniTransaction read = ctx.mgr.begin();
            assertThrows(RecordKeyOrderCorruptedException.class,
                    () -> ctx.service().lookup(read, current, kId(1)));
            ctx.mgr.rollbackUncommitted(read);
        });
    }

    /**
     * 0.14b：root leaf split 是真实多页消费者，必须在分配两个 child leaf 前主动预留容量。
     * 初始 root 分配后 page0 currentSize 为 128；第四条宽记录触发 root split，reservation 应先把文件/page0
     * 预扩到 192，再进入 `allocatePage`。未接消费者时 split 直接消耗同一 extent 的空闲页，currentSize 会停在 128。
     */
    @Test
    void rootLeafSplitReservesSpaceBeforeAllocatingChildLeaves() {
        onBTreePool((ctx) -> {
            ctx.createTablespaceAndRoot();
            BTreeIndex current = ctx.splitIndex();
            BTreeIndexService service = ctx.service();
            for (long id = 1; id <= 3; id++) {
                MiniTransaction m = ctx.mgr.begin();
                current = service.insert(m, current, wideRow(id)).indexAfterInsert();
                ctx.mgr.commit(m);
            }
            assertEquals(PageNo.of(128), ctx.currentSize(), "boot root allocation autoextends once");

            MiniTransaction split = ctx.mgr.begin();
            current = service.insert(split, current, wideRow(4)).indexAfterInsert();
            ctx.mgr.commit(split);

            assertEquals(1, current.rootLevel(), "fourth wide row must split the root leaf");
            assertEquals(PageNo.of(192), ctx.currentSize(),
                    "0.14b reservation preextends capacity before split allocatePage calls");
            assertFound(ctx, current, 1);
            assertFound(ctx, current, 4);
        });
    }

    /**
     * 0.14b：若 split reservation 无法把表空间预扩到下一 extent，错误必须发生在任何 split 页内容修改前。
     * 限制 PageStore 最大只能到 128 页；前三条记录已存在，第四条本会 root split。正确实现应在 reserve 阶段抛出
     * `NoFreeSpaceException`，保持 root 仍为 leaf、page0 currentSize 仍为 128，且既有记录仍可读。
     */
    @Test
    void rootLeafSplitReservationFailureLeavesExistingTreeUnchanged() {
        PageStore store = new LimitedPageStore(128);
        try (PageStore ignored = store;
             BufferPool pool = new LruBufferPool(store, PS, 128);
             RedoLogFileRepository repo = RedoLogFileRepository.open(dir.resolve("btree-limited-redo.log"))) {
            RedoLogManager redo = RedoLogManager.durable(repo);
            BTreeContext ctx = new BTreeContext(store, pool, redo);
            ctx.createTablespaceAndRoot();
            BTreeIndex current = ctx.splitIndex();
            BTreeIndexService service = ctx.service();
            for (long id = 1; id <= 3; id++) {
                MiniTransaction m = ctx.mgr.begin();
                current = service.insert(m, current, wideRow(id)).indexAfterInsert();
                ctx.mgr.commit(m);
            }
            assertEquals(PageNo.of(128), ctx.currentSize());

            MiniTransaction split = ctx.mgr.begin();
            BTreeIndex before = current;
            try {
                assertThrows(NoFreeSpaceException.class, () -> service.insert(split, before, wideRow(4)));
            } finally {
                ctx.mgr.rollbackUncommitted(split);
            }

            assertEquals(PageNo.of(128), ctx.currentSize(),
                    "failed reservation must not advance page0 currentSize");
            assertEquals(0, before.rootLevel(), "reservation failure happens before root leaf is reformatted");
            assertFound(ctx, before, 1);
            assertFound(ctx, before, 3);
        }
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
    void rootSplitEmitsBtreeSiblingLinkDeltasWithoutPhysicalSiblingBytes() {
        onBTreePool((ctx) -> {
            BTreeIndex current = ctx.insertWideRows(1, 4);

            List<RedoRecord> records = ctx.mgr.redoLogManager().bufferedRecords();
            List<BTreePageDeltaRecord> siblingDeltas = records.stream()
                    .filter(BTreePageDeltaRecord.class::isInstance)
                    .map(BTreePageDeltaRecord.class::cast)
                    .filter(delta -> delta.indexId() == INDEX_ID)
                    .filter(delta -> delta.kind() == BTreePageDeltaKind.SIBLING_LINKS)
                    .toList();

            assertFalse(siblingDeltas.isEmpty(), "root split must log B+Tree sibling-link logical deltas");
            List<BTreePageDeltaRecord> nodeDeltas = records.stream()
                    .filter(BTreePageDeltaRecord.class::isInstance)
                    .map(BTreePageDeltaRecord.class::cast)
                    .filter(delta -> delta.indexId() == INDEX_ID)
                    .filter(delta -> delta.kind() == BTreePageDeltaKind.NODE_POINTER_AREA)
                    .toList();
            List<BTreePageDeltaRecord> rootDeltas = records.stream()
                    .filter(BTreePageDeltaRecord.class::isInstance)
                    .map(BTreePageDeltaRecord.class::cast)
                    .filter(delta -> delta.indexId() == INDEX_ID)
                    .filter(delta -> delta.kind() == BTreePageDeltaKind.ROOT_LEVEL_OR_HEADER)
                    .toList();
            assertFalse(nodeDeltas.isEmpty(), "root split must log final node-pointer heap/directory images");
            assertFalse(rootDeltas.isEmpty(), "root split must log final root level/index header image");
            for (RedoLogBatch batch : ctx.mgr.redoLogManager().bufferedBatches()) {
                for (BTreePageDeltaRecord delta : batch.records().stream()
                        .filter(BTreePageDeltaRecord.class::isInstance)
                        .map(BTreePageDeltaRecord.class::cast)
                        .filter(delta -> delta.indexId() == INDEX_ID)
                        .filter(delta -> delta.kind() == BTreePageDeltaKind.SIBLING_LINKS
                                || delta.kind() == BTreePageDeltaKind.NODE_POINTER_AREA
                                || delta.kind() == BTreePageDeltaKind.ROOT_LEVEL_OR_HEADER)
                        .toList()) {
                    assertFalse(batch.records().stream().anyMatch(record -> record instanceof PageBytesRecord bytes
                                    && isCoveredBy(delta, bytes)),
                            "covered sibling-link PAGE_BYTES should be filtered for " + delta.pageId());
                }
            }
            assertLeafLinksAreBidirectional(ctx, current);
        });
    }

    private static boolean isCoveredBy(BTreePageDeltaRecord delta, PageBytesRecord bytes) {
        if (!bytes.pageId().equals(delta.pageId())) {
            return false;
        }
        long physicalStart = bytes.offset();
        long physicalEnd = physicalStart + bytes.bytes().length;
        long logicalStart = delta.offset();
        long logicalEnd = logicalStart + delta.afterImage().length;
        if (physicalStart < logicalStart || physicalEnd > logicalEnd) {
            return false;
        }
        byte[] physical = bytes.bytes();
        byte[] logical = delta.afterImage();
        int deltaOffset = (int) (physicalStart - logicalStart);
        for (int i = 0; i < physical.length; i++) {
            if (physical[i] != logical[deltaOffset + i]) {
                return false;
            }
        }
        return true;
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
            assertTrue(ctx.mgr.redoLogManager().bufferedRecords().stream()
                            .filter(BTreePageDeltaRecord.class::isInstance)
                            .map(BTreePageDeltaRecord.class::cast)
                            .anyMatch(delta -> delta.kind() == BTreePageDeltaKind.PAGE_FORMAT_IMAGE),
                    "non-root internal split must log final page-header structure images");
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

    /**
     * 0.13a：写路径乐观下降。一批宽 key 插入既会命中乐观 leaf-only 路径（不撑爆的插入），
     * 又会在 leaf 溢出时回退悲观全 X split。用诊断计数确认两条路径都真的执行，并断言结果完全正确（有序全查得）。
     */
    @Test
    void bulkInsertExercisesOptimisticAndPessimisticPathsAndStaysCorrect() {
        onBTreePool((ctx) -> {
            ctx.createTablespaceAndRoot();
            BTreeIndex current = new BTreeIndex(INDEX_ID, ctx.rootPageId, 0, payloadKey(), wideSchema(), true,
                    ctx.leafSegment, ctx.nonLeafSegment);
            SplitCapableBTreeIndexService service =
                    new SplitCapableBTreeIndexService(ctx.access, ctx.disk, registry);
            List<Long> inserted = new ArrayList<>();
            for (long id = 1; id <= 24; id++) {
                MiniTransaction m = ctx.mgr.begin();
                current = service.insert(m, current, payloadKeyRow(id)).indexAfterInsert();
                ctx.mgr.commit(m);
                inserted.add(id);
            }

            assertTrue(current.rootLevel() >= 1, "wide keys grow the tree past a single leaf");
            assertTrue(service.optimisticInsertHitCount() > 0,
                    "non-splitting inserts must take the optimistic leaf-only path");
            assertTrue(service.pessimisticInsertFallbackCount() > 0,
                    "overflowing inserts must fall back to the pessimistic split path");

            MiniTransaction read = ctx.mgr.begin();
            List<Long> ids = service.scan(read, current,
                            new BTreeScanRange(kPayload(1), true, kPayload(9999), true, 200))
                    .stream().map(SplitCapableBTreeIndexServiceTest::idOf).toList();
            ctx.mgr.commit(read);
            assertEquals(inserted, ids);
        });
    }

    /**
     * 0.13d safe-node（设计 §10.2 step4-5）：悲观 split 下降时，一旦 latch 到「连续空闲 ≥ 半页」的 safe 祖先，
     * 立即释放其以上的全部 X latch（含 root），保留链收缩为「safe 祖先 … leaf」。用 payloadKey 建 ≥2 层树后，凡 split
     * 不传播到 root（leaf 的某层祖先 under-half）的插入都会提前释放 root X（不再持到 commit），放开 root 处写并发。
     * 诊断计数 {@code safeNodeAncestorReleaseCount()>0} 即「有祖先被早释放」的确定性证据；同时断言全部 key 有序无损。
     *
     * <p>RED（未接 safe-node 前）：悲观全路径 X 持到 commit、从不早释放祖先，计数恒 0。
     */
    @Test
    void pessimisticSplitReleasesRootLatchEarlyWhenSplitDoesNotReachRoot() {
        onBTreePool((ctx) -> {
            ctx.createTablespaceAndRoot();
            BTreeIndex current = new BTreeIndex(INDEX_ID, ctx.rootPageId, 0, payloadKey(), wideSchema(), true,
                    ctx.leafSegment, ctx.nonLeafSegment);
            SplitCapableBTreeIndexService service =
                    new SplitCapableBTreeIndexService(ctx.access, ctx.disk, registry);
            List<Long> inserted = new ArrayList<>();
            long id = 1;
            // 建到 ≥2 层后再多灌一批：保证有插入落到某层 under-half 祖先之下、其 leaf split 不传播到 root。
            while ((current.rootLevel() < 2 || id <= 40) && id <= 80) {
                MiniTransaction m = ctx.mgr.begin();
                current = service.insert(m, current, payloadKeyRow(id)).indexAfterInsert();
                ctx.mgr.commit(m);
                inserted.add(id);
                id++;
            }
            assertTrue(current.rootLevel() >= 2, "payload keys grow the tree to at least level 2");
            assertTrue(service.pessimisticInsertFallbackCount() > 0,
                    "overflowing inserts must fall back to the pessimistic split path");
            assertTrue(service.safeNodeAncestorReleaseCount() > 0,
                    "safe-node must release root/ancestor X early when a split does not propagate to root");

            MiniTransaction read = ctx.mgr.begin();
            List<Long> ids = service.scan(read, current,
                            new BTreeScanRange(kPayload(1), true, kPayload(9999), true, 500))
                    .stream().map(SplitCapableBTreeIndexServiceTest::idOf).toList();
            ctx.mgr.commit(read);
            assertEquals(inserted, ids, "all keys stay present and ordered after safe-node splits");
        });
    }

    /**
     * 0.13d SX 下降 + restart-in-X（设计 §10.3 ROOT_LATCHED_SX）：快照树高 ≥2 的悲观 SMO 第一遍以 root <b>SX</b> 下降
     * （与读者/乐观写者的 root S 并存、排它其它 SMO）；safe-node 截断使 root（SX）早释放的 SMO 全程不 X root；
     * 只有链顶仍是 root（SMO 可能写 root，而 SX 禁原地升级）才在<b>零页写入</b>状态整链释放、以 root X 重启第二遍。
     * 80 宽 key 行灌到 level≥2：快照 ≥2 后的悲观 split 全部先走 SX 首遍（sx&gt;0）；其中传播到 root 的（root 收
     * separator / root split）重启（restart&gt;0）；未达 root 的多数停在 safe 节点、不重启（sx &gt; restart）。
     *
     * <p>RED（未接 SX+restart 前）：悲观下降直接 root X、无 SX 首遍也无重启，两计数恒 0。
     */
    @Test
    void pessimisticSmoUsesRootSharedExclusiveFirstPassAndRestartsOnlyWhenReachingRoot() {
        onBTreePool((ctx) -> {
            ctx.createTablespaceAndRoot();
            BTreeIndex current = new BTreeIndex(INDEX_ID, ctx.rootPageId, 0, payloadKey(), wideSchema(), true,
                    ctx.leafSegment, ctx.nonLeafSegment);
            SplitCapableBTreeIndexService service =
                    new SplitCapableBTreeIndexService(ctx.access, ctx.disk, registry);
            List<Long> inserted = new ArrayList<>();
            long id = 1;
            while ((current.rootLevel() < 2 || id <= 40) && id <= 80) {
                MiniTransaction m = ctx.mgr.begin();
                current = service.insert(m, current, payloadKeyRow(id)).indexAfterInsert();
                ctx.mgr.commit(m);
                inserted.add(id);
                id++;
            }
            assertTrue(current.rootLevel() >= 2, "tree grows to at least level 2");
            assertTrue(service.rootSxDescentCount() > 0,
                    "snapshot-level>=2 pessimistic descents must take the root SX first pass");
            assertTrue(service.rootXRestartCount() > 0,
                    "splits that may reach root must restart with root X (SX cannot upgrade in place)");
            assertTrue(service.rootSxDescentCount() > service.rootXRestartCount(),
                    "most SMOs stop at a safe node below root and never restart");

            MiniTransaction read = ctx.mgr.begin();
            List<Long> ids = service.scan(read, current,
                            new BTreeScanRange(kPayload(1), true, kPayload(9999), true, 500))
                    .stream().map(SplitCapableBTreeIndexServiceTest::idOf).toList();
            ctx.mgr.commit(read);
            assertEquals(inserted, ids, "all keys stay present and ordered under SX+restart descent");
        });
    }

    /**
     * 并发正确性：预建多层树后，两线程各插入一段不相交 key。0.13a 乐观 leaf-only 插入安全并发；0.13d safe-node 后
     * 悲观 split <b>不再</b>把 root X 持到 commit——insert 在下降起点短持 root X（遇 safe 内部节点即释放），故任一时刻
     * 只有一个 insert「在 root 处」，两个 insert 只在各自释放 root 后于下层并发；leaf split 触碰的兄弟页恒为右邻
     * （{@code nextPageNo}，单向全序）→ 并发 splitter 间无环、无死锁。断言不做时序假设，只验证全部 key（预建 + 两段）
     * 有序、无丢无损。
     */
    @Test
    void concurrentInsertsAcrossLeavesStayCorrect() {
        onBTreePool((ctx) -> {
            ctx.createTablespaceAndRoot();
            SplitCapableBTreeIndexService service =
                    new SplitCapableBTreeIndexService(ctx.access, ctx.disk, registry);
            BTreeIndex built = new BTreeIndex(INDEX_ID, ctx.rootPageId, 0, payloadKey(), wideSchema(), true,
                    ctx.leafSegment, ctx.nonLeafSegment);
            for (long id = 1; id <= 12; id++) {
                MiniTransaction m = ctx.mgr.begin();
                built = service.insert(m, built, payloadKeyRow(id)).indexAfterInsert();
                ctx.mgr.commit(m);
            }
            assertTrue(built.rootLevel() >= 1, "pre-built tree is multi-level");
            final BTreeIndex snapshot = built; // 不可变，供两线程共享导航（页才是树高权威，快照 level 陈旧无妨）

            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread a = new Thread(() -> insertRangeCommitting(ctx, service, snapshot, 100, 119, failure), "btree-a");
            Thread b = new Thread(() -> insertRangeCommitting(ctx, service, snapshot, 200, 219, failure), "btree-b");
            a.setDaemon(true);
            b.setDaemon(true);
            a.start();
            b.start();
            joinWorkerOrFail(a, Duration.ofSeconds(10));
            joinWorkerOrFail(b, Duration.ofSeconds(10));
            if (failure.get() != null) {
                throw new AssertionError("concurrent insert threw", failure.get());
            }

            List<Long> expected = new ArrayList<>();
            for (long id = 1; id <= 12; id++) {
                expected.add(id);
            }
            for (long id = 100; id <= 119; id++) {
                expected.add(id);
            }
            for (long id = 200; id <= 219; id++) {
                expected.add(id);
            }
            MiniTransaction read = ctx.mgr.begin();
            List<Long> ids = service.scan(read, snapshot,
                            new BTreeScanRange(kPayload(1), true, kPayload(9999), true, 500))
                    .stream().map(SplitCapableBTreeIndexServiceTest::idOf).toList();
            ctx.mgr.commit(read);
            assertEquals(expected, ids);
        });
    }

    /** 并发 split 回归测试必须在有限时间内失败；否则真实死锁会把 Gradle worker 永久挂住。 */
    private static void joinWorkerOrFail(Thread thread, Duration timeout) {
        try {
            thread.join(timeout.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while joining " + thread.getName(), e);
        }
        assertFalse(thread.isAlive(), thread.getName() + " did not finish within " + timeout);
    }

    /** 并发工作线程：逐 key 独立 MTR 插入，失败记录首个异常并回滚当前 MTR。 */
    private void insertRangeCommitting(BTreeContext ctx, SplitCapableBTreeIndexService service,
                                       BTreeIndex snapshot, long firstInclusive, long lastInclusive,
                                       AtomicReference<Throwable> failure) {
        try {
            for (long id = firstInclusive; id <= lastInclusive; id++) {
                MiniTransaction m = ctx.mgr.begin();
                try {
                    service.insert(m, snapshot, payloadKeyRow(id));
                    ctx.mgr.commit(m);
                } catch (Throwable t) {
                    ctx.mgr.rollbackUncommitted(m);
                    throw t;
                }
            }
        } catch (Throwable t) {
            failure.compareAndSet(null, t);
        }
    }

    /**
     * 0.13c 读路径 crab 的确定性证据：多层树上做全量 scan，但只给 buffer pool 极小容量（12 帧）。
     * S-crab 下降只同时持「1 父 + 1 子」，sibling 链 hand-over-hand 只同时持「≤2 leaf」，故任一时刻 fix 帧数≤3，
     * 全量 scan 能在 12 帧池内跑完并返回全部有序 key。若读路径仍全路径 S（root + 全部已扫 leaf 的 latch 持到 commit），
     * 同时 fix 的帧数（root + ~20 leaf）会超过 12 而抛 BufferPoolExhaustedException——因此本用例在非 crab 实现下必失败，是 0.13c 的 RED。
     */
    @Test
    void fullScanFitsInSmallBufferPoolBecauseReadPathCrabs() {
        onBTreePool(12, (ctx) -> {
            BTreeIndex index = ctx.insertWideRows(1, 60);
            assertTrue(index.rootLevel() >= 1, "60 wide rows must build a multi-level tree with many leaves");
            MiniTransaction read = ctx.mgr.begin();
            List<Long> ids = ctx.service().scan(read, index,
                            new BTreeScanRange(kId(1), true, kId(1_000_000), true, 1000))
                    .stream().map(SplitCapableBTreeIndexServiceTest::idOf).toList();
            ctx.mgr.commit(read);
            List<Long> expected = new ArrayList<>();
            for (long id = 1; id <= 60; id++) {
                expected.add(id);
            }
            assertEquals(expected, ids, "full scan must return every key in order without exhausting the small pool");
        });
    }

    /**
     * 0.13c 读并发正确性：多层树上一个 reader 线程反复全量 scan，同时 writer 线程插入不相交 key（触发 split）。
     * S-crab + hand-over-hand 保证 reader 不把 root/祖先 latch 持到 commit，与 writer 的悲观全 X 结构变更串行在
     * page latch 上，故每次 scan 都读到「严格递增、无重复、落在已知界内」的一致子集，绝不穿过正在 split 的页。
     * 断言不做时序假设（沿用 0.13a 约定），只验证每次快照一致 + 终局无丢无损。
     */
    @Test
    void concurrentScansStayConsistentWhileInsertsSplitLeaves() {
        onBTreePool((ctx) -> {
            ctx.createTablespaceAndRoot();
            BTreeIndexService service = ctx.service();
            BTreeIndex built = ctx.splitIndex();
            for (long id = 1; id <= 12; id++) {
                MiniTransaction m = ctx.mgr.begin();
                built = service.insert(m, built, wideRow(id)).indexAfterInsert();
                ctx.mgr.commit(m);
            }
            assertTrue(built.rootLevel() >= 1, "pre-built tree is multi-level");
            final BTreeIndex snapshot = built;

            AtomicReference<Throwable> failure = new AtomicReference<>();
            AtomicBoolean writerDone = new AtomicBoolean(false);
            Thread reader = new Thread(() -> {
                try {
                    while (!writerDone.get()) {
                        MiniTransaction read = ctx.mgr.begin();
                        List<Long> ids;
                        try {
                            ids = service.scan(read, snapshot,
                                            new BTreeScanRange(kId(1), true, kId(1_000_000), true, 1000))
                                    .stream().map(SplitCapableBTreeIndexServiceTest::idOf).toList();
                            ctx.mgr.commit(read);
                        } catch (Throwable t) {
                            ctx.mgr.rollbackUncommitted(read);
                            throw t;
                        }
                        assertSortedDistinctWithinKnownBounds(ids);
                    }
                } catch (Throwable t) {
                    failure.compareAndSet(null, t);
                }
            }, "btree-reader");
            Thread writer = new Thread(() -> {
                try {
                    for (long id = 100; id <= 140; id++) {
                        MiniTransaction m = ctx.mgr.begin();
                        try {
                            service.insert(m, snapshot, wideRow(id));
                            ctx.mgr.commit(m);
                        } catch (Throwable t) {
                            ctx.mgr.rollbackUncommitted(m);
                            throw t;
                        }
                    }
                } catch (Throwable t) {
                    failure.compareAndSet(null, t);
                } finally {
                    writerDone.set(true);
                }
            }, "btree-writer");
            reader.start();
            writer.start();
            joinThread(reader);
            joinThread(writer);
            if (failure.get() != null) {
                throw new AssertionError("concurrent read/write under read crab failed", failure.get());
            }

            List<Long> expected = new ArrayList<>();
            for (long id = 1; id <= 12; id++) {
                expected.add(id);
            }
            for (long id = 100; id <= 140; id++) {
                expected.add(id);
            }
            MiniTransaction read = ctx.mgr.begin();
            List<Long> ids = service.scan(read, snapshot,
                            new BTreeScanRange(kId(1), true, kId(1_000_000), true, 1000))
                    .stream().map(SplitCapableBTreeIndexServiceTest::idOf).toList();
            ctx.mgr.commit(read);
            assertEquals(expected, ids, "final scan must see all pre-built and concurrently inserted keys");
        });
    }

    /** 校验一次并发 scan 快照：严格递增（有序无重复）且每个 key 都属于已知区间（预建 1..12 或 writer 100..140）。 */
    private static void assertSortedDistinctWithinKnownBounds(List<Long> ids) {
        long prev = Long.MIN_VALUE;
        for (long id : ids) {
            assertTrue(id > prev, () -> "scan snapshot must be strictly increasing, got " + ids);
            boolean known = (id >= 1 && id <= 12) || (id >= 100 && id <= 140);
            assertTrue(known, () -> "scan snapshot returned an out-of-range key " + id + " -> " + ids);
            prev = id;
        }
    }

    private static void joinThread(Thread t) {
        try {
            t.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while joining " + t.getName(), e);
        }
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

    /** 保留记录头与其余列，只替换同宽 key slice，确保物理页 validator 仍能通过。 */
    private void replaceKeyField(RecordPage page, int offset, LogicalRecord replacement,
                                 TableSchema schema, ColumnId keyColumn) {
        RecordCursor cursor = new RecordCursor(page, offset, schema, registry);
        int fieldOffset = cursor.columnSlice(keyColumn).offset();
        int fieldLength = cursor.columnSlice(keyColumn).length();
        byte[] original = page.readRecordBytes(offset);
        byte[] encoded = new RecordEncoder(registry).encode(replacement, schema);
        System.arraycopy(encoded, fieldOffset, original, fieldOffset, fieldLength);
        page.writeRecordBytes(offset, original);
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
        onBTreePool(128, body);
    }

    /**
     * 以指定帧容量运行 B+Tree 用例。0.13c 读路径 crab 用极小容量池验证 scan 只同时 fix «父+子» / «≤2 leaf»——
     * 非 crab（root + 全部已扫 leaf 持到 commit）会因同时 fix 帧数超容量而抛 BufferPoolExhaustedException。
     */
    private void onBTreePool(int capacity, Body body) {
        PageStore store = new FileChannelPageStore();
        try (PageStore ignored = store;
             LruBufferPool pool = new LruBufferPool(store, PS, capacity);
             RedoLogFileRepository repo = RedoLogFileRepository.open(dir.resolve("btree-context-redo.log"))) {
            RedoLogManager redo = RedoLogManager.durable(repo);
            FlushCoordinator coordinator = new FlushCoordinator(pool, store, redo, PS,
                    new NoDoublewriteStrategy(), Duration.ofMillis(50));
            CoordinatedDirtyVictimFlusher flusher = new CoordinatedDirtyVictimFlusher(coordinator);
            pool.attachVictimFlusher(pageId -> {
                redo.flush();
                return flusher.flushVictim(pageId);
            });
            BTreeContext ctx = new BTreeContext(store, pool, redo);
            body.run(ctx);
        }
    }

    private interface Body {
        void run(BTreeContext ctx);
    }

    private final class BTreeContext {
        private final PageStore store;
        private final BufferPool pool;
        private final MiniTransactionManager mgr;
        private final DiskSpaceManager disk;
        private final IndexPageAccess access;
        private SegmentRef leafSegment;
        private SegmentRef nonLeafSegment;
        private PageId rootPageId;

        private BTreeContext(PageStore store, BufferPool pool, RedoLogManager redo) {
            this.store = store;
            this.pool = pool;
            this.mgr = new MiniTransactionManager(new TablespaceAccessController(), redo);
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

        private PageNo currentSize() {
            MiniTransaction read = mgr.begin();
            try {
                PageNo size = disk.usage(read, SPACE).currentSizeInPages();
                mgr.commit(read);
                return size;
            } catch (Throwable t) {
                mgr.rollbackUncommitted(read);
                throw t;
            }
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

    /**
     * 限制 ensureCapacity 上限的测试 PageStore，用于稳定模拟 0.14b reservation 阶段 ENOSPC。
     * 其它物理 IO 行为全部委托真实 FileChannelPageStore，避免测试绕过 FSP/Buffer Pool 真实路径。
     */
    private static final class LimitedPageStore implements PageStore {

        private final PageStore delegate = new FileChannelPageStore();
        private final long maxPages;

        private LimitedPageStore(long maxPages) {
            this.maxPages = maxPages;
        }

        @Override
        public void create(SpaceId spaceId, Path path, PageSize pageSize, PageNo initialSizeInPages) {
            delegate.create(spaceId, path, pageSize, initialSizeInPages);
        }

        @Override
        public void open(SpaceId spaceId, Path path, PageSize pageSize) {
            delegate.open(spaceId, path, pageSize);
        }

        @Override
        public void readPage(PageId pageId, ByteBuffer dst) {
            delegate.readPage(pageId, dst);
        }

        @Override
        public void writePage(PageId pageId, ByteBuffer src) {
            delegate.writePage(pageId, src);
        }

        @Override
        public PageNo extend(SpaceId spaceId) {
            return delegate.extend(spaceId);
        }

        @Override
        public PageNo currentSizeInPages(SpaceId spaceId) {
            return delegate.currentSizeInPages(spaceId);
        }

        @Override
        public Path pathOf(SpaceId spaceId) {
            return delegate.pathOf(spaceId);
        }

        @Override
        public void force(SpaceId spaceId) {
            delegate.force(spaceId);
        }

        @Override
        public void forceAll() {
            delegate.forceAll();
        }

        @Override
        public void ensureCapacity(SpaceId spaceId, PageNo minSizeInPages) {
            if (minSizeInPages.value() > maxPages) {
                throw new NoFreeSpaceException("test store cannot grow to " + minSizeInPages.value());
            }
            delegate.ensureCapacity(spaceId, minSizeInPages);
        }

        @Override
        public void truncate(SpaceId spaceId, PageNo targetSizeInPages) {
            delegate.truncate(spaceId, targetSizeInPages);
        }

        @Override
        public void close(SpaceId spaceId) {
            delegate.close(spaceId);
        }

        @Override
        public void close() {
            delegate.close();
        }
    }
}
