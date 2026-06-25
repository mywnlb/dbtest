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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * FLUSHING 帧生命周期测试（Phase B，设计 §5.7/§7.3）。验证 dirty 与 FLUSHING 正交：刷盘中页仍 dirty（计入 checkpoint
 * 边界），但不被重复 snapshot、不进 flush 候选、不被淘汰；失败可回 DIRTY 重试。
 */
class BufferPoolFlushingStateTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);

    @TempDir
    Path dir;

    private PageStore openStore(int pages) {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(pages));
        return store;
    }

    private static PageId page(long no) {
        return PageId.of(SPACE, PageNo.of(no));
    }

    private static void writePageLsn(BufferPool pool, PageId pageId, long lsn) {
        try (PageGuard guard = pool.getPage(pageId, PageLatchMode.EXCLUSIVE)) {
            guard.writeLong(PageEnvelopeLayout.PAGE_LSN, lsn);
        }
    }

    @Test
    void flushingPageExcludedFromCandidatesAndNotDoubleSnapshotted() {
        try (PageStore store = openStore(8); BufferPool pool = new LruBufferPool(store, PS, 4)) {
            writePageLsn(pool, page(2), 10);
            pool.snapshotForFlush(page(2)).orElseThrow(); // DIRTY → FLUSHING

            assertTrue(pool.dirtyPageCandidates(Lsn.of(99), 10).isEmpty(),
                    "FLUSHING 页不应再进 flush 候选（避免重复刷）");
            assertTrue(pool.snapshotForFlush(page(2)).isEmpty(),
                    "FLUSHING 页不应被二次 snapshot（单 IO owner）");
            assertEquals(Lsn.of(10), pool.oldestDirtyLsnOr(Lsn.of(99)),
                    "FLUSHING 页仍 dirty，仍约束 checkpoint oldest-dirty 边界");
        }
    }

    @Test
    void failFlushReturnsFlushingPageToDirty() {
        try (PageStore store = openStore(8); BufferPool pool = new LruBufferPool(store, PS, 4)) {
            writePageLsn(pool, page(2), 10);
            pool.snapshotForFlush(page(2)).orElseThrow(); // FLUSHING

            pool.failFlush(page(2)); // FLUSHING → DIRTY（可重试）

            // 先验候选（只读），再验可再次 snapshot——snapshotForFlush 有副作用（DIRTY→FLUSHING），故放最后。
            assertFalse(pool.dirtyPageCandidates(Lsn.of(99), 10).isEmpty(),
                    "failFlush 后页重新进 flush 候选");
            assertTrue(pool.snapshotForFlush(page(2)).isPresent(),
                    "failFlush 后页回 DIRTY，可再次 snapshot 重试");
        }
    }

    @Test
    void flushingFrameIsNotEvictedAsVictim() {
        try (PageStore store = openStore(8); BufferPool pool = new LruBufferPool(store, PS, 1)) {
            writePageLsn(pool, page(0), 10);
            pool.snapshotForFlush(page(0)).orElseThrow(); // 唯一帧进 FLUSHING

            // FLUSHING 帧处于 IO 中，不能被淘汰；容量 1 时无可用 victim → 耗尽（而非 legacy writeBack 重复刷）。
            assertThrows(BufferPoolExhaustedException.class,
                    () -> pool.getPage(page(1), PageLatchMode.EXCLUSIVE));
        }
    }
}
