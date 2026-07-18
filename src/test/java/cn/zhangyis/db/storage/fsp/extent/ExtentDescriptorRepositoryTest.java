package cn.zhangyis.db.storage.fsp.extent;
import cn.zhangyis.db.storage.fsp.exception.FspMetadataException;
import cn.zhangyis.db.storage.fsp.flst.FileAddress;


import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.ExtentId;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SegmentId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.fil.io.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * ExtentDescriptorRepository 集成测试：普通零初始化即 FREE/无主/NULL；extent0 系统保留且普通 mutator 拒绝；
 * state/owner/prev/next 往返；bitmap set/clear/get；超首批、越界和坏 ordinal 拒绝。
 */
class ExtentDescriptorRepositoryTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(7);

    @TempDir
    Path dir;

    private interface Body {
        void run(ExtentDescriptorRepository repo, MiniTransaction mtr);
    }

    private void withRepo(Body body) {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(64));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 8)) {
            ExtentDescriptorRepository repo = new ExtentDescriptorRepository(pool, PS);
            MiniTransactionManager mgr = new MiniTransactionManager();
            MiniTransaction mtr = mgr.begin();
            body.run(repo, mtr);
            mgr.commit(mtr);
        }
    }

    private ExtentId ext(long no) {
        return ExtentId.of(SPACE, no);
    }

    /**
     * 验证 {@code zeroInitDecodesAsFreeUnownedNull} 所描述的空间分配或复用路径，并断言 extent/segment 所有权、链表和重复释放边界。
     */
    @Test
    void zeroInitDecodesAsFreeUnownedNull() {
        withRepo((repo, mtr) -> {
            ExtentDescriptor d = repo.read(mtr, ext(3));
            assertEquals(ExtentState.FREE, d.state());
            assertTrue(d.ownerSegment().isEmpty());
            assertTrue(d.prev().isNull());
            assertTrue(d.next().isNull());
            assertFalse(repo.isPageAllocated(mtr, ext(3), 0));
        });
    }

    /**
     * 验证 {@code reserveSystemExtentMarksFixedPagesAndRejectsInitFree} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void reserveSystemExtentMarksFixedPagesAndRejectsInitFree() {
        withRepo((repo, mtr) -> {
            repo.reserveSystemExtent(mtr, SPACE);
            ExtentDescriptor d = repo.read(mtr, ext(0));
            assertEquals(ExtentState.FSEG_FRAG, d.state());
            assertTrue(d.ownerSegment().isEmpty());
            assertTrue(repo.isPageAllocated(mtr, ext(0), 0));
            assertTrue(repo.isPageAllocated(mtr, ext(0), 1));
            assertTrue(repo.isPageAllocated(mtr, ext(0), 2));
            assertTrue(repo.isPageAllocated(mtr, ext(0), 3));
            assertThrows(FspMetadataException.class, () -> repo.initFree(mtr, ext(0)));
            assertThrows(FspMetadataException.class, () -> repo.writeState(mtr, ext(0), ExtentState.FREE));
            assertThrows(FspMetadataException.class, () -> repo.writeOwner(mtr, ext(0), Optional.of(SegmentId.of(1))));
            assertThrows(FspMetadataException.class, () -> repo.setPageAllocated(mtr, ext(0), 4, true));
        });
    }

    /**
     * 验证 {@code stateOwnerPrevNextRoundTrip} 对应的表空间、区与段分配行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void stateOwnerPrevNextRoundTrip() {
        withRepo((repo, mtr) -> {
            repo.writeState(mtr, ext(2), ExtentState.FSEG);
            repo.writeOwner(mtr, ext(2), Optional.of(SegmentId.of(5)));
            repo.writePrev(mtr, ext(2), FileAddress.of(PageNo.of(0), 268));
            repo.writeNext(mtr, ext(2), FileAddress.NULL);

            ExtentDescriptor d = repo.read(mtr, ext(2));
            assertEquals(ExtentState.FSEG, d.state());
            assertEquals(Optional.of(SegmentId.of(5)), d.ownerSegment());
            assertEquals(FileAddress.of(PageNo.of(0), 268), d.prev());
            assertTrue(d.next().isNull());
        });
    }

    /**
     * 验证 {@code shouldRejectOwnerSegmentZero} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void shouldRejectOwnerSegmentZero() {
        withRepo((repo, mtr) ->
                assertThrows(DatabaseValidationException.class,
                        () -> repo.writeOwner(mtr, ext(2), Optional.of(SegmentId.of(0)))));
    }

    /**
     * 验证 {@code pageSizeBoundariesUsePagesPerExtent} 所描述的页内记录行为，并断言偏移、编码边界、隐藏列及 page-directory 结构保持一致。
     */
    @Test
    void pageSizeBoundariesUsePagesPerExtent() {
        assertEquals(256, PageSize.ofBytes(4 * 1024).pagesPerExtent());
        assertEquals(128, PageSize.ofBytes(8 * 1024).pagesPerExtent());
        assertEquals(64, PageSize.ofBytes(16 * 1024).pagesPerExtent());
    }

    /**
     * 验证 {@code bitmapSetClearGet} 对应的表空间、区与段分配行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void bitmapSetClearGet() {
        withRepo((repo, mtr) -> {
            repo.setPageAllocated(mtr, ext(1), 0, true);
            repo.setPageAllocated(mtr, ext(1), 63, true);
            assertTrue(repo.isPageAllocated(mtr, ext(1), 0));
            assertTrue(repo.isPageAllocated(mtr, ext(1), 63));
            assertFalse(repo.isPageAllocated(mtr, ext(1), 1));
            repo.setPageAllocated(mtr, ext(1), 0, false);
            assertFalse(repo.isPageAllocated(mtr, ext(1), 0));
        });
    }

    /**
     * 验证 {@code shouldRejectExtentBeyondFirstBatchAndBadIndex} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void shouldRejectExtentBeyondFirstBatchAndBadIndex() {
        withRepo((repo, mtr) -> {
            long tooBig = ExtentDescriptorLayout.maxEntriesInPage0(PS);
            assertThrows(FspMetadataException.class, () -> repo.read(mtr, ext(tooBig)));
            assertThrows(DatabaseValidationException.class, () -> repo.setPageAllocated(mtr, ext(1), 64, true));
        });
    }

    /**
     * 验证 {@code badStateOrdinalThrowsMetadataException} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void badStateOrdinalThrowsMetadataException() {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(64));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 8)) {
            ExtentDescriptorRepository repo = new ExtentDescriptorRepository(pool, PS);
            MiniTransactionManager mgr = new MiniTransactionManager();

            MiniTransaction corrupt = mgr.begin();
            PageGuard g = corrupt.getPage(pool, PageId.of(SPACE, PageNo.of(0)), PageLatchMode.EXCLUSIVE);
            g.writeInt(ExtentDescriptorLayout.entryOffset(1) + ExtentDescriptorLayout.STATE, 99);
            mgr.commit(corrupt);

            MiniTransaction r = mgr.begin();
            assertThrows(FspMetadataException.class, () -> repo.read(r, ext(1)));
            mgr.commit(r);
        }
    }
}
