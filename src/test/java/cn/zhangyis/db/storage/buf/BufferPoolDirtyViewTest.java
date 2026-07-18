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
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * F1 dirty view 测试：BufferPool 只暴露候选和页镜像，不泄漏 BufferFrame；flush 完成时用 dirtyVersion 防止误清再次变脏的页。
 */
class BufferPoolDirtyViewTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);

    @TempDir
    Path dir;

    /**
     * 验证 {@code dirtyCandidatesAreOrderedByOldestModificationLsn} 所描述的刷脏与持久化协作，并断言 redo durable 边界先覆盖 page LSN、失败后仍保留脏状态。
     */
    @Test
    void dirtyCandidatesAreOrderedByOldestModificationLsn() {
        try (PageStore store = openStore(8); BufferPool pool = new LruBufferPool(store, PS, 4)) {
            writePageLsn(pool, page(2), 30);
            writePageLsn(pool, page(1), 10);
            writePageLsn(pool, page(3), 20);

            List<DirtyPageCandidate> candidates = pool.dirtyPageCandidates(Lsn.of(25), 10);

            assertEquals(List.of(page(1), page(3)),
                    candidates.stream().map(DirtyPageCandidate::pageId).toList());
            assertEquals(Lsn.of(10), candidates.get(0).oldestModificationLsn());
            assertEquals(Lsn.of(20), candidates.get(1).newestModificationLsn());
        }
    }

    /**
     * 验证 {@code completeFlushKeepsDirtyWhenFrameWasModifiedAgain} 所描述的返回值或状态会按契约保留，并断言原始信息与领域不变量未丢失。
     */
    @Test
    void completeFlushKeepsDirtyWhenFrameWasModifiedAgain() {
        try (PageStore store = openStore(8); BufferPool pool = new LruBufferPool(store, PS, 4)) {
            writePageLsn(pool, page(2), 10);
            FlushPageSnapshot snapshot = pool.snapshotForFlush(page(2)).orElseThrow();

            writePageLsn(pool, page(2), 20);

            assertFalse(pool.completeFlush(snapshot));
            assertEquals(Lsn.of(10), pool.oldestDirtyLsnOr(Lsn.of(99)));
            assertTrue(pool.snapshotForFlush(page(2)).isPresent());
        }
    }

    /**
     * 验证 {@code completeFlushMarksCleanWhenSnapshotStillCurrent} 所描述的刷脏与持久化协作，并断言 redo durable 边界先覆盖 page LSN、失败后仍保留脏状态。
     */
    @Test
    void completeFlushMarksCleanWhenSnapshotStillCurrent() {
        try (PageStore store = openStore(8); BufferPool pool = new LruBufferPool(store, PS, 4)) {
            writePageLsn(pool, page(2), 10);
            Optional<FlushPageSnapshot> snapshot = pool.snapshotForFlush(page(2));

            assertTrue(snapshot.isPresent());
            assertTrue(pool.completeFlush(snapshot.orElseThrow()));
            assertEquals(Lsn.of(99), pool.oldestDirtyLsnOr(Lsn.of(99)));
            assertTrue(pool.dirtyPageCandidates(Lsn.of(99), 10).isEmpty());
        }
    }

    private PageStore openStore(int pages) {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(pages));
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
