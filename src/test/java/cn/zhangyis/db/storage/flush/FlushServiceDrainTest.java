package cn.zhangyis.db.storage.flush;

import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.DirtyPageCandidate;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.fil.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.PageStore;
import cn.zhangyis.db.storage.page.PageEnvelopeLayout;
import cn.zhangyis.db.storage.redo.PageBytesRecord;
import cn.zhangyis.db.storage.redo.RedoCapacityPolicy;
import cn.zhangyis.db.storage.redo.RedoLogFileRepository;
import cn.zhangyis.db.storage.redo.RedoLogManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * F2 tablespace drain 测试：flush service 只能 drain 指定 space，且必须在无法清脏时显式 timeout。
 */
class FlushServiceDrainTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE1 = SpaceId.of(1);
    private static final SpaceId SPACE2 = SpaceId.of(2);
    private static final PageId PAGE1 = PageId.of(SPACE1, PageNo.of(2));
    private static final PageId PAGE2 = PageId.of(SPACE2, PageNo.of(2));

    @TempDir
    Path dir;

    @Test
    void drainTablespaceFlushesOnlyTargetSpace() {
        try (PageStore store = new FileChannelPageStore();
             BufferPool pool = new LruBufferPool(store, PS, 8);
             RedoLogFileRepository repo = RedoLogFileRepository.open(dir.resolve("redo.log"))) {
            createSpaces(store);
            RedoLogManager redo = RedoLogManager.durable(repo);
            Lsn lsn1 = appendRedo(redo, PAGE1);
            Lsn lsn2 = appendRedo(redo, PAGE2);
            writeDirty(pool, PAGE1, lsn1);
            writeDirty(pool, PAGE2, lsn2);

            TablespaceDrainResult result = service(pool, store, redo)
                    .drainTablespace(SPACE1, Duration.ofMillis(200));

            assertFalse(result.timedOut());
            assertEquals(SPACE1, result.spaceId());
            assertEquals(1, result.results().size());
            assertEquals(FlushResultStatus.CLEAN, result.results().get(0).status());
            assertEquals(List.of(PAGE2), dirtyPages(pool));
        }
    }

    @Test
    void drainTablespaceReturnsTimeoutWhenTargetPageCannotBeSnapshotted() {
        try (PageStore store = new FileChannelPageStore();
             BufferPool pool = new LruBufferPool(store, PS, 8);
             RedoLogFileRepository repo = RedoLogFileRepository.open(dir.resolve("redo.log"))) {
            createSpaces(store);
            RedoLogManager redo = RedoLogManager.durable(repo);
            Lsn lsn1 = appendRedo(redo, PAGE1);
            writeDirty(pool, PAGE1, lsn1);

            try (PageGuard ignored = pool.getPage(PAGE1, PageLatchMode.SHARED)) {
                TablespaceDrainResult result = service(pool, store, redo)
                        .drainTablespace(SPACE1, Duration.ZERO);

                assertTrue(result.timedOut());
                assertTrue(result.results().isEmpty());
                assertEquals(List.of(PAGE1), dirtyPages(pool));
            }
        }
    }

    private FlushService service(BufferPool pool, PageStore store, RedoLogManager redo) {
        FlushCoordinator coordinator = new FlushCoordinator(pool, store, redo, PS,
                new NoDoublewriteStrategy(), Duration.ofMillis(50));
        CheckpointCoordinator checkpoint = new CheckpointCoordinator(pool, redo);
        return new FlushService(pool, coordinator, checkpoint, redo, RedoCapacityPolicy.fixed(10_000),
                AdaptiveFlushPolicy.fixed(1, 8));
    }

    private void createSpaces(PageStore store) {
        store.create(SPACE1, dir.resolve("s1.ibd"), PS, PageNo.of(4));
        store.create(SPACE2, dir.resolve("s2.ibd"), PS, PageNo.of(4));
    }

    private static Lsn appendRedo(RedoLogManager redo, PageId pageId) {
        Lsn lsn = redo.append(List.of(new PageBytesRecord(pageId, 256, new byte[]{1, 2, 3}))).end();
        redo.flush();
        return lsn;
    }

    private static void writeDirty(BufferPool pool, PageId pageId, Lsn lsn) {
        try (PageGuard guard = pool.getPage(pageId, PageLatchMode.EXCLUSIVE)) {
            guard.writeLong(PageEnvelopeLayout.PAGE_LSN, lsn.value());
            guard.writeBytes(256, new byte[]{1, 2, 3});
        }
    }

    private static List<PageId> dirtyPages(BufferPool pool) {
        return pool.dirtyPageCandidates(Lsn.of(Long.MAX_VALUE), pool.capacity())
                .stream()
                .map(DirtyPageCandidate::pageId)
                .toList();
    }
}
