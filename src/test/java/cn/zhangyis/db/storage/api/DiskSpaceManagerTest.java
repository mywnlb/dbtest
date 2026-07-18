package cn.zhangyis.db.storage.api;

import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SegmentId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.fil.io.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.fsp.exception.NoFreeSpaceException;
import cn.zhangyis.db.storage.fsp.exception.FspMetadataException;
import cn.zhangyis.db.storage.fsp.segment.SegmentPurpose;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import cn.zhangyis.db.storage.page.FilePageHeader;
import cn.zhangyis.db.storage.page.PageEnvelope;
import cn.zhangyis.db.storage.page.PageType;
import cn.zhangyis.db.storage.redo.FspMetadataDeltaKind;
import cn.zhangyis.db.storage.redo.FspMetadataDeltaRecord;
import cn.zhangyis.db.storage.redo.FspPageFreeRecord;
import cn.zhangyis.db.storage.redo.FspPageAllocationRecord;
import cn.zhangyis.db.storage.redo.PageInitRecord;
import cn.zhangyis.db.storage.redo.RedoRecord;
import cn.zhangyis.db.storage.redo.RedoBudgetPurpose;
import cn.zhangyis.db.storage.trx.UndoRedoBudgetEstimator;
import cn.zhangyis.db.storage.undo.UndoSegmentDropPlan;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * DiskSpaceManager facade 集成测试：建表空间/用量、建段、分配（fragment→extent）、autoextend、NoFreeSpace、释放、drop 回收。
 */
class DiskSpaceManagerTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(7);

    @TempDir
    Path dir;

    private interface Body {
        void run(DiskSpaceManager dsm, MiniTransactionManager mgr);
    }

    private void withDsm(Body body) {
        PageStore store = new FileChannelPageStore();
        // pool=64：D4a 后每次 allocatePage 多 fix 一个数据页 X latch 且持到 commit；批量单 MTR 分配(33 页)需更大池。
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 64)) {
            DiskSpaceManager dsm = new DiskSpaceManager(pool, store, PS);
            MiniTransactionManager mgr = new MiniTransactionManager();
            body.run(dsm, mgr);
        }
    }

    /**
     * 验证 {@code createTablespaceThenUsageAndSegment} 所描述的空间分配或复用路径，并断言 extent/segment 所有权、链表和重复释放边界。
     */
    @Test
    void createTablespaceThenUsageAndSegment() {
        withDsm((dsm, mgr) -> {
            MiniTransaction m = mgr.begin();
            dsm.createTablespace(m, SPACE, dir.resolve("s.ibd"), PageNo.of(128));
            SpaceUsage u = dsm.usage(m, SPACE);
            assertEquals(PageNo.of(128), u.currentSizeInPages());
            assertEquals(PageNo.of(0), u.freeLimitPageNo());
            assertEquals(1L, u.nextSegmentId());
            SegmentRef ref = dsm.createSegment(m, SPACE, SegmentPurpose.INDEX_LEAF);
            assertEquals(0, ref.inodeSlot());
            assertEquals(SegmentId.of(1), ref.segmentId());
            assertEquals(2L, dsm.usage(m, SPACE).nextSegmentId());
            mgr.commit(m);
        });
    }

    /**
     * 验证 {@code allocateFreeReallocateRecyclesPage} 所描述的页内记录行为，并断言偏移、编码边界、隐藏列及 page-directory 结构保持一致。
     */
    @Test
    void allocateFreeReallocateRecyclesPage() {
        withDsm((dsm, mgr) -> {
            MiniTransaction m = mgr.begin();
            dsm.createTablespace(m, SPACE, dir.resolve("s.ibd"), PageNo.of(128));
            SegmentRef ref = dsm.createSegment(m, SPACE, SegmentPurpose.INDEX_LEAF);
            PageId p = dsm.allocatePage(m, ref);
            assertEquals(PageId.of(SPACE, PageNo.of(64)), p);
            dsm.freePage(m, ref, p);
            assertEquals(PageId.of(SPACE, PageNo.of(64)), dsm.allocatePage(m, ref));
            mgr.commit(m);
        });
    }

    /**
     * 验证 {@code allocateAutoextendsWhenExhausted} 所描述的空间分配或复用路径，并断言 extent/segment 所有权、链表和重复释放边界。
     */
    @Test
    void allocateAutoextendsWhenExhausted() {
        withDsm((dsm, mgr) -> {
            MiniTransaction m = mgr.begin();
            dsm.createTablespace(m, SPACE, dir.resolve("s.ibd"), PageNo.of(128));
            SegmentRef ref = dsm.createSegment(m, SPACE, SegmentPurpose.INDEX_LEAF);
            for (int i = 0; i < 32; i++) {
                assertEquals(PageId.of(SPACE, PageNo.of(64 + i)), dsm.allocatePage(m, ref));
            }
            assertEquals(PageId.of(SPACE, PageNo.of(128)), dsm.allocatePage(m, ref));
            assertEquals(PageNo.of(192), dsm.usage(m, SPACE).currentSizeInPages());
            mgr.commit(m);
        });
    }

    /**
     * 验证 {@code allocatePageAcceptsDirectionalHintWithoutChangingLegacyPageInitialization} 所描述的边界场景保持既有领域不变量，不产生方法名明确禁止的副作用。
     */
    @Test
    void allocatePageAcceptsDirectionalHintWithoutChangingLegacyPageInitialization() {
        withDsm((dsm, mgr) -> {
            MiniTransaction m = mgr.begin();
            dsm.createTablespace(m, SPACE, dir.resolve("s.ibd"), PageNo.of(128));
            SegmentRef ref = dsm.createSegment(m, SPACE, SegmentPurpose.INDEX_LEAF);
            PageId p = dsm.allocatePage(m, ref, PageAllocationHint.up(PageNo.of(64), 1L));
            assertEquals(PageId.of(SPACE, PageNo.of(64)), p);
            mgr.commit(m);
        });
    }

    /**
     * 验证 {@code allocateThrowsNoFreeSpaceOnTinyTablespace} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void allocateThrowsNoFreeSpaceOnTinyTablespace() {
        withDsm((dsm, mgr) -> {
            MiniTransaction m = mgr.begin();
            dsm.createTablespace(m, SPACE, dir.resolve("s.ibd"), PageNo.of(4));
            SegmentRef ref = dsm.createSegment(m, SPACE, SegmentPurpose.INDEX_LEAF);
            assertThrows(NoFreeSpaceException.class, () -> dsm.allocatePage(m, ref));
            mgr.commit(m);
        });
    }

    /**
     * 验证 {@code dropSegmentReclaimsAndAllowsSlotReuse} 所描述的空间分配或复用路径，并断言 extent/segment 所有权、链表和重复释放边界。
     */
    @Test
    void dropSegmentReclaimsAndAllowsSlotReuse() {
        withDsm((dsm, mgr) -> {
            MiniTransaction m = mgr.begin();
            dsm.createTablespace(m, SPACE, dir.resolve("s.ibd"), PageNo.of(192));
            SegmentRef ref = dsm.createSegment(m, SPACE, SegmentPurpose.INDEX_LEAF);
            for (int i = 0; i < 33; i++) {
                dsm.allocatePage(m, ref);
            }
            dsm.dropSegment(m, ref);
            SegmentRef ref2 = dsm.createSegment(m, SPACE, SegmentPurpose.INDEX_LEAF);
            assertEquals(0, ref2.inodeSlot());
            PageId again = dsm.allocatePage(m, ref2);
            assertEquals(PageId.of(SPACE, PageNo.of(64)), again);
            mgr.commit(m);
        });
    }

    /** drop plan 必须只读 inode 权威账本，并同时覆盖 fragment 与 extent 两种持有形态。 */
    @Test
    void inspectDropPlanReadsFragmentAndExtentCountsBeforeWriteBatch() {
        withDsm((dsm, mgr) -> {
            MiniTransaction write = mgr.begin();
            dsm.createTablespace(write, SPACE, dir.resolve("s.ibd"), PageNo.of(192));
            SegmentRef ref = dsm.createSegment(write, SPACE, SegmentPurpose.INDEX_LEAF);
            for (int i = 0; i < 33; i++) {
                dsm.allocatePage(write, ref);
            }
            mgr.commit(write);

            MiniTransaction read = mgr.beginReadOnly();
            SegmentDropPlan plan = dsm.inspectDropSegmentPlan(read, ref);
            mgr.commit(read);

            assertEquals(32, plan.fragmentPageCount());
            assertEquals(1, plan.extentCount());
            assertEquals(33, plan.usedPageCount());

            MiniTransaction drop = mgr.begin(mgr.budgetFor(RedoBudgetPurpose.UNDO_FINALIZATION,
                    UndoRedoBudgetEstimator.finalization(new UndoSegmentDropPlan(
                            plan.fragmentPageCount(), plan.extentCount(), plan.usedPageCount()), false)));
            dsm.dropSegment(drop, ref);
            mgr.commit(drop);
        });
    }

    /** 陈旧 handle 不得借 inode 槽复用读取另一 segment 的规模并进入物理 drop。 */
    @Test
    void inspectDropPlanRejectsSegmentIdentityMismatch() {
        withDsm((dsm, mgr) -> {
            MiniTransaction write = mgr.begin();
            dsm.createTablespace(write, SPACE, dir.resolve("s.ibd"), PageNo.of(128));
            SegmentRef ref = dsm.createSegment(write, SPACE, SegmentPurpose.INDEX_LEAF);
            mgr.commit(write);

            MiniTransaction read = mgr.beginReadOnly();
            SegmentRef stale = new SegmentRef(ref.spaceId(), ref.inodeSlot(), SegmentId.of(99));
            assertThrows(FspMetadataException.class, () -> dsm.inspectDropSegmentPlan(read, stale));
            mgr.rollbackUncommitted(read);
        });
    }

    /**
     * 验证 {@code allocatePageEmitsPageInitAndStampsEnvelope} 所描述的页内记录行为，并断言偏移、编码边界、隐藏列及 page-directory 结构保持一致。
     */
    @Test
    void allocatePageEmitsPageInitAndStampsEnvelope() {
        PageStore store = new FileChannelPageStore();
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 64)) {
            DiskSpaceManager dsm = new DiskSpaceManager(pool, store, PS);
            MiniTransactionManager mgr = new MiniTransactionManager();
            MiniTransaction m = mgr.begin();
            dsm.createTablespace(m, SPACE, dir.resolve("s.ibd"), PageNo.of(128));
            SegmentRef ref = dsm.createSegment(m, SPACE, SegmentPurpose.INDEX_LEAF);
            PageId p = dsm.allocatePage(m, ref);
            mgr.commit(m);

            boolean hasInit = mgr.redoLogManager().bufferedRecords().stream()
                    .anyMatch(r -> r instanceof PageInitRecord pir
                            && pir.pageId().equals(p) && pir.pageType() == PageType.ALLOCATED);
            assertTrue(hasInit, "allocatePage emits PAGE_INIT(ALLOCATED)");

            Lsn endLsn = mgr.redoLogManager().currentLsn();
            try (PageGuard g = pool.getPage(p, PageLatchMode.SHARED)) {
                FilePageHeader h = PageEnvelope.readHeader(g);
                assertEquals(SPACE, h.spaceId());
                assertEquals(p.pageNo().value(), h.pageNo());
                assertEquals(PageType.ALLOCATED, h.pageType());
                assertEquals(endLsn, PageEnvelope.readPageLsn(g));
            }
        }
    }

    /**
     * 验证 {@code allocatePageEmitsFspLogicalRedoBeforePageInit} 所描述的恢复场景能够依据持久证据幂等重建状态，且不会重复产生副作用。
     */
    @Test
    void allocatePageEmitsFspLogicalRedoBeforePageInit() {
        PageStore store = new FileChannelPageStore();
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 64)) {
            DiskSpaceManager dsm = new DiskSpaceManager(pool, store, PS);
            MiniTransactionManager mgr = new MiniTransactionManager();
            MiniTransaction m = mgr.begin();
            dsm.createTablespace(m, SPACE, dir.resolve("s.ibd"), PageNo.of(128));
            SegmentRef ref = dsm.createSegment(m, SPACE, SegmentPurpose.INDEX_LEAF);

            PageId p = dsm.allocatePage(m, ref);
            mgr.commit(m);

            List<RedoRecord> records = mgr.redoLogManager().bufferedRecords();
            int fspIndex = indexOfFspAlloc(records, p);
            int initIndex = indexOfPageInit(records, p);
            assertTrue(fspIndex >= 0, "allocatePage must persist FSP allocation intent");
            assertTrue(initIndex > fspIndex, "FSP allocation intent must precede PAGE_INIT for the allocated page");

            FspPageAllocationRecord record = (FspPageAllocationRecord) records.get(fspIndex);
            assertEquals(ref.inodeSlot(), record.inodeSlot());
            assertEquals(ref.segmentId(), record.segmentId());
            assertEquals(false, record.autoExtendRetry());
        }
    }

    /**
     * 验证 {@code autoextendAllocationMarksFspLogicalRedoRetry} 所描述的恢复场景能够依据持久证据幂等重建状态，且不会重复产生副作用。
     */
    @Test
    void autoextendAllocationMarksFspLogicalRedoRetry() {
        PageStore store = new FileChannelPageStore();
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 128)) {
            DiskSpaceManager dsm = new DiskSpaceManager(pool, store, PS);
            MiniTransactionManager mgr = new MiniTransactionManager();
            MiniTransaction m = mgr.begin();
            dsm.createTablespace(m, SPACE, dir.resolve("s.ibd"), PageNo.of(128));
            SegmentRef ref = dsm.createSegment(m, SPACE, SegmentPurpose.INDEX_LEAF);

            PageId p = null;
            for (int i = 0; i < 33; i++) {
                p = dsm.allocatePage(m, ref);
            }
            mgr.commit(m);

            List<RedoRecord> records = mgr.redoLogManager().bufferedRecords();
            FspPageAllocationRecord record = (FspPageAllocationRecord) records.get(indexOfFspAlloc(records, p));
            assertEquals(PageId.of(SPACE, PageNo.of(128)), record.allocatedPageId());
            assertEquals(true, record.autoExtendRetry());
        }
    }

    /**
     * 验证 {@code fspOperationsEmitMetadataDeltaRecordsAndPageFreeIntent} 所描述的页内记录行为，并断言偏移、编码边界、隐藏列及 page-directory 结构保持一致。
     */
    @Test
    void fspOperationsEmitMetadataDeltaRecordsAndPageFreeIntent() {
        PageStore store = new FileChannelPageStore();
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 64)) {
            DiskSpaceManager dsm = new DiskSpaceManager(pool, store, PS);
            MiniTransactionManager mgr = new MiniTransactionManager();
            MiniTransaction m = mgr.begin();
            dsm.createTablespace(m, SPACE, dir.resolve("s.ibd"), PageNo.of(128));
            SegmentRef ref = dsm.createSegment(m, SPACE, SegmentPurpose.INDEX_LEAF);
            PageId p = dsm.allocatePage(m, ref);
            dsm.freePage(m, ref, p);
            mgr.commit(m);

            List<RedoRecord> records = mgr.redoLogManager().bufferedRecords();
            assertTrue(containsDeltaKind(records, FspMetadataDeltaKind.SPACE_HEADER_FIELD),
                    "space header currentSize/freeLimit/nextSegmentId changes must emit metadata delta");
            assertTrue(containsDeltaKind(records, FspMetadataDeltaKind.XDES_FIELD),
                    "XDES state/owner/list field changes must emit metadata delta");
            assertTrue(containsDeltaKind(records, FspMetadataDeltaKind.XDES_BITMAP_BYTE),
                    "XDES page allocation bitmap changes must emit metadata delta");
            assertTrue(containsDeltaKind(records, FspMetadataDeltaKind.INODE_SLOT_IMAGE),
                    "segment creation must emit a full inode-slot after image");
            assertTrue(containsDeltaKind(records, FspMetadataDeltaKind.INODE_FIELD),
                    "inode used/reserved counters must emit field delta");
            assertTrue(containsDeltaKind(records, FspMetadataDeltaKind.INODE_FRAGMENT_SLOT),
                    "fragment slot assignment/free must emit fragment slot delta");
            assertTrue(records.stream().anyMatch(r -> r instanceof FspPageFreeRecord free
                            && free.freedPageId().equals(p)
                            && free.inodeSlot() == ref.inodeSlot()
                            && free.segmentId().equals(ref.segmentId())),
                    "freePage must persist a page free intent before metadata delta carries the account changes");
        }
    }

    /**
     * 验证 {@code reallocateResidentPageReinitializes} 所描述的页内记录行为，并断言偏移、编码边界、隐藏列及 page-directory 结构保持一致。
     */
    @Test
    void reallocateResidentPageReinitializes() {
        PageStore store = new FileChannelPageStore();
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 64)) {
            DiskSpaceManager dsm = new DiskSpaceManager(pool, store, PS);
            MiniTransactionManager mgr = new MiniTransactionManager();
            MiniTransaction m = mgr.begin();
            dsm.createTablespace(m, SPACE, dir.resolve("s.ibd"), PageNo.of(128));
            SegmentRef ref = dsm.createSegment(m, SPACE, SegmentPurpose.INDEX_LEAF);
            PageId p = dsm.allocatePage(m, ref);
            dsm.freePage(m, ref, p);
            PageId again = dsm.allocatePage(m, ref); // 命中同一驻留页 → 重初始化，不抛
            assertEquals(p, again);
            mgr.commit(m);
            try (PageGuard g = pool.getPage(again, PageLatchMode.SHARED)) {
                assertEquals(PageType.ALLOCATED, PageEnvelope.readHeader(g).pageType());
            }
        }
    }

    private static int indexOfFspAlloc(List<RedoRecord> records, PageId pageId) {
        for (int i = 0; i < records.size(); i++) {
            if (records.get(i) instanceof FspPageAllocationRecord record
                    && record.allocatedPageId().equals(pageId)) {
                return i;
            }
        }
        return -1;
    }

    private static int indexOfPageInit(List<RedoRecord> records, PageId pageId) {
        for (int i = 0; i < records.size(); i++) {
            if (records.get(i) instanceof PageInitRecord record
                    && record.pageId().equals(pageId)) {
                return i;
            }
        }
        return -1;
    }

    private static boolean containsDeltaKind(List<RedoRecord> records, FspMetadataDeltaKind kind) {
        return records.stream().anyMatch(record -> record instanceof FspMetadataDeltaRecord delta
                && delta.kind() == kind);
    }

}
