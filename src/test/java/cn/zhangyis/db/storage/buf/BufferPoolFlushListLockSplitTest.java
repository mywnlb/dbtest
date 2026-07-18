package cn.zhangyis.db.storage.buf;

import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.fil.io.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.page.PageEnvelopeLayout;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 13.1d free/LRU/flush 子锁与真实 flush list 的协作测试。
 *
 * <p>测试只通过 BufferPool 公开 dirty view 验证 flush list 语义；锁守卫测试留在同包，
 * 用于防止后续在持 list 锁时进入 PageStore、PageLoadFuture 或 dirty victim flush。
 */
class BufferPoolFlushListLockSplitTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(96);

    @TempDir
    Path dir;

    /**
     * 验证 {@code dirtyViewKeepsOldestAndRefreshesNewestForRepeatedWrites} 所描述的返回值或状态会按契约保留，并断言原始信息与领域不变量未丢失。
     */
    @Test
    void dirtyViewKeepsOldestAndRefreshesNewestForRepeatedWrites() {
        try (PageStore store = openStore(); BufferPool pool = new LruBufferPool(store, PS, 4)) {
            writePageLsn(pool, page(2), 10);
            writePageLsn(pool, page(2), 30);

            List<DirtyPageCandidate> candidates = pool.dirtyPageCandidates(Lsn.of(99), 10);

            assertEquals(List.of(page(2)), candidates.stream().map(DirtyPageCandidate::pageId).toList());
            assertEquals(Lsn.of(10), candidates.get(0).oldestModificationLsn());
            assertEquals(Lsn.of(30), candidates.get(0).newestModificationLsn());
        }
    }

    /**
     * 验证 {@code flushingPageStaysInDirtyBoundaryButIsSkippedAsFlushCandidate} 所描述的刷脏与持久化协作，并断言 redo durable 边界先覆盖 page LSN、失败后仍保留脏状态。
     */
    @Test
    void flushingPageStaysInDirtyBoundaryButIsSkippedAsFlushCandidate() {
        try (PageStore store = openStore(); BufferPool pool = new LruBufferPool(store, PS, 4)) {
            writePageLsn(pool, page(2), 10);
            pool.snapshotForFlush(page(2)).orElseThrow();

            assertTrue(pool.dirtyPageCandidates(Lsn.of(99), 10).isEmpty(),
                    "FLUSHING 页已经有 IO owner，不能重复进入 flush 候选");
            assertTrue(pool.hasDirtyPages(), "FLUSHING 页尚未 durable，仍是 checkpoint dirty 边界");
            assertEquals(Lsn.of(10), pool.oldestDirtyLsnOr(Lsn.of(99)));
        }
    }

    /**
     * 验证 {@code completeFlushAfterRewriteKeepsPageInDirtyView} 所描述的返回值或状态会按契约保留，并断言原始信息与领域不变量未丢失。
     */
    @Test
    void completeFlushAfterRewriteKeepsPageInDirtyView() {
        try (PageStore store = openStore(); BufferPool pool = new LruBufferPool(store, PS, 4)) {
            writePageLsn(pool, page(2), 10);
            FlushPageSnapshot snapshot = pool.snapshotForFlush(page(2)).orElseThrow();
            writePageLsn(pool, page(2), 30);

            assertFalse(pool.completeFlush(snapshot), "snapshot 后再次写入时不能误清新修改");

            List<DirtyPageCandidate> candidates = pool.dirtyPageCandidates(Lsn.of(99), 10);
            assertEquals(List.of(page(2)), candidates.stream().map(DirtyPageCandidate::pageId).toList());
            assertEquals(Lsn.of(10), candidates.get(0).oldestModificationLsn());
            assertEquals(Lsn.of(30), candidates.get(0).newestModificationLsn());
        }
    }

    /**
     * 验证 {@code latchSetRejectsIoWhileAnyListLockHeld} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void latchSetRejectsIoWhileAnyListLockHeld() {
        BufferPoolInstanceLatchSet latchSet = new BufferPoolInstanceLatchSet();

        latchSet.lockFreeList();
        try {
            assertThrows(BufferPoolLatchViolationException.class,
                    () -> latchSet.assertMetadataUnlocked("page read"));
        } finally {
            latchSet.unlockFreeList();
        }

        latchSet.lockLruList();
        try {
            assertThrows(BufferPoolLatchViolationException.class,
                    () -> latchSet.assertMetadataUnlocked("dirty victim flush"));
        } finally {
            latchSet.unlockLruList();
        }

        latchSet.lockFlushList();
        try {
            assertThrows(BufferPoolLatchViolationException.class,
                    () -> latchSet.assertMetadataUnlocked("legacy page write"));
        } finally {
            latchSet.unlockFlushList();
        }
    }

    private PageStore openStore() {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(16));
        return store;
    }

    private static PageId page(long pageNo) {
        return PageId.of(SPACE, PageNo.of(pageNo));
    }

    private static void writePageLsn(BufferPool pool, PageId pageId, long lsn) {
        try (PageGuard guard = pool.getPage(pageId, PageLatchMode.EXCLUSIVE)) {
            guard.writeLong(PageEnvelopeLayout.PAGE_LSN, lsn);
            guard.writeInt(128, (int) lsn);
        }
    }
}
