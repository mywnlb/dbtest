package cn.zhangyis.db.storage.fsp;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.fil.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.PageStore;
import cn.zhangyis.db.storage.fil.TablespaceState;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * SpaceHeaderRepository 集成测试：initialize→read 往返（三 list 头为 FlstBase 空链）、标量 setter→read、
 * allocateNextSegmentId 自增与 0 哨兵拒绝、base 地址访问器经 Flst 维护后 read 反映；no-redo，不做 crash recovery 断言。
 */
class SpaceHeaderRepositoryTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(7);

    @TempDir
    Path dir;

    private SpaceHeaderSnapshot freshHeader() {
        return new SpaceHeaderSnapshot(SPACE, PS, 0,
                PageNo.of(64), PageNo.of(64), 1L,
                FlstBase.EMPTY, FlstBase.EMPTY, FlstBase.EMPTY,
                PageNo.of(2), 0L, 80046, 1L);
    }

    @Test
    void initializeThenReadRoundTrips() {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(64));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 8)) {
            SpaceHeaderRepository repo = new SpaceHeaderRepository(pool);
            MiniTransactionManager mgr = new MiniTransactionManager();

            MiniTransaction w = mgr.begin();
            repo.initialize(w, freshHeader());
            mgr.commit(w);

            MiniTransaction r = mgr.begin();
            SpaceHeaderSnapshot got = repo.read(r, SPACE);
            mgr.commit(r);

            assertEquals(freshHeader(), got);
        }
    }

    /**
     * undo 生命周期头位于 page-0 保留区，必须与 FSP header 独立往返，并保留稳定状态码和截断 epoch。
     */
    @Test
    void lifecycleHeaderRoundTripsWithoutChangingFspHeader() {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("undo.ibu"), PS, PageNo.of(64));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 8)) {
            SpaceHeaderRepository repo = new SpaceHeaderRepository(pool);
            MiniTransactionManager mgr = new MiniTransactionManager();
            TablespaceLifecycleHeader expected = new TablespaceLifecycleHeader(
                    TablespaceState.TRUNCATING, PageNo.of(64), 7L, PageNo.of(64), TablespaceState.INACTIVE);

            MiniTransaction w = mgr.begin();
            repo.initialize(w, freshHeader());
            repo.writeLifecycle(w, SPACE, expected);
            mgr.commit(w);

            MiniTransaction r = mgr.begin();
            assertEquals(expected, repo.readLifecycle(r, SPACE).orElseThrow());
            assertEquals(freshHeader(), repo.read(r, SPACE));
            mgr.commit(r);
        }
    }

    /** 旧表空间保留区全零时必须明确返回“无生命周期头”，不能把零误解为 EMPTY。 */
    @Test
    void legacyPageZeroHasNoLifecycleHeader() {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("legacy.ibd"), PS, PageNo.of(64));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 8)) {
            SpaceHeaderRepository repo = new SpaceHeaderRepository(pool);
            MiniTransactionManager mgr = new MiniTransactionManager();
            MiniTransaction w = mgr.begin();
            repo.initialize(w, freshHeader());
            mgr.commit(w);

            MiniTransaction r = mgr.begin();
            assertTrue(repo.readLifecycle(r, SPACE).isEmpty());
            mgr.commit(r);
        }
    }

    @Test
    void scalarSettersUpdateFields() {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(64));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 8)) {
            SpaceHeaderRepository repo = new SpaceHeaderRepository(pool);
            MiniTransactionManager mgr = new MiniTransactionManager();
            MiniTransaction w = mgr.begin();
            repo.initialize(w, freshHeader());
            repo.setCurrentSizeInPages(w, SPACE, PageNo.of(128));
            repo.setFreeLimitPageNo(w, SPACE, PageNo.of(128));
            repo.setFirstInodePageNo(w, SPACE, PageNo.of(2));
            mgr.commit(w);

            MiniTransaction r = mgr.begin();
            SpaceHeaderSnapshot got = repo.read(r, SPACE);
            mgr.commit(r);
            assertEquals(PageNo.of(128), got.currentSizeInPages());
            assertEquals(PageNo.of(128), got.freeLimitPageNo());
        }
    }

    @Test
    void freeExtentListBaseManagedByFlst() {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(64));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 8)) {
            SpaceHeaderRepository repo = new SpaceHeaderRepository(pool);
            Flst flst = new Flst(pool);
            MiniTransactionManager mgr = new MiniTransactionManager();

            MiniTransaction w = mgr.begin();
            repo.initialize(w, freshHeader());
            FileAddress base = repo.freeExtentListBaseAddr(SPACE);
            // 用 page0 XDES 区 entry 1 的 node 槽（与 header 区不重叠）
            FileAddress node = FileAddress.of(PageNo.of(0),
                    ExtentDescriptorLayout.entryOffset(1) + ExtentDescriptorLayout.PREV);
            flst.addLast(w, SPACE, base, node);
            mgr.commit(w);

            MiniTransaction r = mgr.begin();
            SpaceHeaderSnapshot got = repo.read(r, SPACE);
            mgr.commit(r);
            assertEquals(1L, got.freeExtentList().length());
            assertEquals(node, got.freeExtentList().first());
            assertEquals(node, got.freeExtentList().last());
        }
    }

    @Test
    void allocateNextSegmentIdIncrements() {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(64));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 8)) {
            SpaceHeaderRepository repo = new SpaceHeaderRepository(pool);
            MiniTransactionManager mgr = new MiniTransactionManager();
            MiniTransaction w = mgr.begin();
            repo.initialize(w, freshHeader());
            assertEquals(1L, repo.allocateNextSegmentId(w, SPACE));
            assertEquals(2L, repo.allocateNextSegmentId(w, SPACE));
            mgr.commit(w);
        }
    }

    @Test
    void shouldRejectZeroNextSegmentId() {
        assertThrows(DatabaseValidationException.class, () -> new SpaceHeaderSnapshot(SPACE, PS, 0,
                PageNo.of(64), PageNo.of(64), 0L,
                FlstBase.EMPTY, FlstBase.EMPTY, FlstBase.EMPTY,
                PageNo.of(2), 0L, 80046, 1L));
    }

    @Test
    void allocateNextSegmentIdRejectsZeroStoredOnPage() {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(64));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 8)) {
            SpaceHeaderRepository repo = new SpaceHeaderRepository(pool);
            MiniTransactionManager mgr = new MiniTransactionManager();
            MiniTransaction init = mgr.begin();
            repo.initialize(init, freshHeader());
            mgr.commit(init);

            MiniTransaction corrupt = mgr.begin();
            PageGuard g = corrupt.getPage(pool, PageId.of(SPACE, PageNo.of(0)), PageLatchMode.EXCLUSIVE);
            g.writeLong(SpaceHeaderLayout.NEXT_SEGMENT_ID, 0L);
            mgr.commit(corrupt);

            MiniTransaction r = mgr.begin();
            assertThrows(FspMetadataException.class, () -> repo.allocateNextSegmentId(r, SPACE));
            mgr.commit(r);
        }
    }
}
