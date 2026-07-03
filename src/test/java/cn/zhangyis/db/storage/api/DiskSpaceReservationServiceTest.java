package cn.zhangyis.db.storage.api;

import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.fil.io.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.fsp.exception.NoFreeSpaceException;
import cn.zhangyis.db.storage.fsp.exception.SpaceReservationExceededException;
import cn.zhangyis.db.storage.fsp.reservation.SpaceReservation;
import cn.zhangyis.db.storage.fsp.reservation.SpaceReservationKind;
import cn.zhangyis.db.storage.fsp.segment.SegmentPurpose;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * DiskSpaceManager 空间预留切片测试：固定 §7.1 reserve-before-multi-page-op 的最小生产语义。
 *
 * <p>这些用例故意从 facade 入口测试，而不是直接测内部 service。原因是预留必须和 page0 currentSize、
 * PageStore 扩容、MTR memo 释放以及 allocatePage 消费配合，单测内部计数无法证明不会半途创建页后再 ENOSPC。
 */
class DiskSpaceReservationServiceTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(19);

    @TempDir
    Path dir;

    /**
     * 预留应在第一个数据页分配前补足物理容量并推进 page0 currentSize；随后 allocatePage 消费一页额度。
     */
    @Test
    void reservationPreextendsBeforeFirstPageAllocationAndConsumesQuota() {
        PageStore store = new FileChannelPageStore();
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 64)) {
            DiskSpaceManager disk = new DiskSpaceManager(pool, store, PS);
            MiniTransactionManager mgr = new MiniTransactionManager();
            MiniTransaction mtr = mgr.begin();
            disk.createTablespace(mtr, SPACE, dir.resolve("s.ibd"), PageNo.of(4));
            SegmentRef segment = disk.createSegment(mtr, SPACE, SegmentPurpose.INDEX_LEAF);

            try (SpaceReservation ignored = disk.reserveSpace(mtr, SPACE, SpaceReservationKind.NORMAL, 1, 0)) {
                assertEquals(PageNo.of(128), disk.usage(mtr, SPACE).currentSizeInPages());
                assertEquals(PageId.of(SPACE, PageNo.of(64)), disk.allocatePage(mtr, segment));
                assertThrows(SpaceReservationExceededException.class, () -> disk.allocatePage(mtr, segment));
            }
            mgr.commit(mtr);
        }
    }

    /**
     * 若底层文件无法扩到形成第一个可分配 extent，reserve 必须先失败；page0 逻辑大小不能被提前推进。
     */
    @Test
    void reservationFailureDoesNotAdvancePageZeroCurrentSize() {
        LimitedPageStore store = new LimitedPageStore(64);
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 64)) {
            DiskSpaceManager disk = new DiskSpaceManager(pool, store, PS);
            MiniTransactionManager mgr = new MiniTransactionManager();
            MiniTransaction mtr = mgr.begin();
            disk.createTablespace(mtr, SPACE, dir.resolve("limited.ibd"), PageNo.of(4));
            disk.createSegment(mtr, SPACE, SegmentPurpose.INDEX_LEAF);

            assertThrows(NoFreeSpaceException.class,
                    () -> disk.reserveSpace(mtr, SPACE, SpaceReservationKind.NORMAL, 1, 0));
            assertEquals(PageNo.of(4), disk.usage(mtr, SPACE).currentSizeInPages());
            mgr.rollbackUncommitted(mtr);
        }
    }

    /**
     * 调用方忘记 close reservation 时，MTR rollback 仍必须释放未消费额度；后续 MTR 可以重新预留同一空间。
     */
    @Test
    void rollbackReleasesUnclosedReservation() {
        PageStore store = new FileChannelPageStore();
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 64)) {
            DiskSpaceManager disk = new DiskSpaceManager(pool, store, PS);
            MiniTransactionManager mgr = new MiniTransactionManager();
            MiniTransaction boot = mgr.begin();
            disk.createTablespace(boot, SPACE, dir.resolve("rollback.ibd"), PageNo.of(4));
            mgr.commit(boot);

            MiniTransaction first = mgr.begin();
            disk.reserveSpace(first, SPACE, SpaceReservationKind.NORMAL, 1, 0);
            mgr.rollbackUncommitted(first);

            MiniTransaction second = mgr.begin();
            try (SpaceReservation ignored = disk.reserveSpace(second, SPACE, SpaceReservationKind.NORMAL, 1, 0)) {
                assertEquals(PageNo.of(128), disk.usage(second, SPACE).currentSizeInPages());
            }
            mgr.commit(second);
        }
    }

    /**
     * 测试用 PageStore：限制 ensureCapacity 的最大页号，用来稳定模拟预留阶段的 ENOSPC。
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
