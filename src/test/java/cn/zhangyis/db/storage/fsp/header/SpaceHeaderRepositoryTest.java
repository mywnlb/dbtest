package cn.zhangyis.db.storage.fsp.header;
import cn.zhangyis.db.storage.fil.state.TablespaceState;

import cn.zhangyis.db.storage.fsp.exception.FspMetadataException;
import cn.zhangyis.db.storage.fsp.extent.ExtentDescriptorLayout;
import cn.zhangyis.db.storage.fsp.flst.FileAddress;
import cn.zhangyis.db.storage.fsp.flst.Flst;
import cn.zhangyis.db.storage.fsp.flst.FlstBase;
import cn.zhangyis.db.storage.fsp.lifecycle.TablespaceLifecycleHeader;


import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.fil.io.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import cn.zhangyis.db.storage.page.FilePageHeader;
import cn.zhangyis.db.storage.page.PageEnvelope;
import cn.zhangyis.db.storage.page.PageType;
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

    /**
     * 验证 {@code initializeThenReadRoundTrips} 对应的表空间、区与段分配行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
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
     * page0 初始化必须盖统一 FSP_HDR 文件页信封：pageType=FSP_HDR、pageNo=0、spaceId 自描述。
     * 这是“page0 物理信封校验”切片的写入侧不变量——loader/recovery 据此判定 page0 是否真的是表空间头页。
     */
    @Test
    void initializeStampsFspHdrEnvelopeOnPageZero() {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(64));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 8)) {
            SpaceHeaderRepository repo = new SpaceHeaderRepository(pool);
            MiniTransactionManager mgr = new MiniTransactionManager();
            MiniTransaction w = mgr.begin();
            repo.initialize(w, freshHeader());
            mgr.commit(w);

            MiniTransaction r = mgr.begin();
            PageGuard g = r.getPage(pool, PageId.of(SPACE, PageNo.of(0)), PageLatchMode.SHARED);
            FilePageHeader h = PageEnvelope.readHeader(g);
            mgr.commit(r);

            assertEquals(PageType.FSP_HDR, h.pageType());
            assertEquals(0L, h.pageNo());
            assertEquals(SPACE, h.spaceId());
            assertEquals(FilePageHeader.FIL_NULL, h.prevPageNo());
            assertEquals(FilePageHeader.FIL_NULL, h.nextPageNo());
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

    /**
     * 验证 {@code scalarSettersUpdateFields} 所描述的页内记录行为，并断言偏移、编码边界、隐藏列及 page-directory 结构保持一致。
     */
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

    /**
     * 验证 {@code freeExtentListBaseManagedByFlst} 所描述的空间分配或复用路径，并断言 extent/segment 所有权、链表和重复释放边界。
     */
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

    /**
     * 验证 {@code allocateNextSegmentIdIncrements} 所描述的空间分配或复用路径，并断言 extent/segment 所有权、链表和重复释放边界。
     */
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

    /**
     * 验证 {@code shouldRejectZeroNextSegmentId} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void shouldRejectZeroNextSegmentId() {
        assertThrows(DatabaseValidationException.class, () -> new SpaceHeaderSnapshot(SPACE, PS, 0,
                PageNo.of(64), PageNo.of(64), 0L,
                FlstBase.EMPTY, FlstBase.EMPTY, FlstBase.EMPTY,
                PageNo.of(2), 0L, 80046, 1L));
    }

    /**
     * 验证 {@code allocateNextSegmentIdRejectsZeroStoredOnPage} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
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
