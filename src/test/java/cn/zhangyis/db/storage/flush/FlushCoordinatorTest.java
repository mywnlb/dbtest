package cn.zhangyis.db.storage.flush;

import cn.zhangyis.db.domain.Lsn;
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
import cn.zhangyis.db.storage.flush.doublewrite.DoublewriteFileRepository;
import cn.zhangyis.db.storage.flush.doublewrite.NoDoublewriteStrategy;
import cn.zhangyis.db.storage.flush.doublewrite.RecoverableDoublewriteStrategy;
import cn.zhangyis.db.storage.page.PageEnvelopeLayout;
import cn.zhangyis.db.storage.redo.PageBytesRecord;
import cn.zhangyis.db.storage.redo.RedoLogFileRepository;
import cn.zhangyis.db.storage.redo.RedoLogManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * F1 flush coordinator 测试：data page write 必须被 redo durable gate 和 doublewrite 顺序保护。
 */
class FlushCoordinatorTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);
    private static final PageId PAGE = PageId.of(SPACE, PageNo.of(2));

    @TempDir
    Path dir;

    @Test
    void flushListSkipsPageWhenRedoIsNotDurable() {
        try (PageStore store = new FileChannelPageStore(); BufferPool pool = new LruBufferPool(store, PS, 4)) {
            store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(4));
            markDirty(pool, 20, 0xCA);
            RedoLogManager redo = new RedoLogManager();
            FlushCoordinator coordinator = new FlushCoordinator(pool, store, redo, PS,
                    new NoDoublewriteStrategy(), Duration.ZERO);

            List<FlushResult> results = coordinator.flushList(Lsn.of(100), 10);

            assertEquals(FlushResultStatus.SKIPPED_REDO_NOT_DURABLE, results.get(0).status());
            assertTrue(pool.snapshotForFlush(PAGE).isPresent());
            byte[] disk = new byte[PS.bytes()];
            store.readPage(PAGE, ByteBuffer.wrap(disk));
            assertEquals(0, disk[200]);
        }
    }

    @Test
    void flushListWritesDoublewriteBeforeDataFileAndMarksClean() {
        try (PageStore store = new FileChannelPageStore();
             BufferPool pool = new LruBufferPool(store, PS, 4);
             RedoLogFileRepository redoRepo = RedoLogFileRepository.open(dir.resolve("redo.log"));
             DoublewriteFileRepository dwRepo = DoublewriteFileRepository.open(dir.resolve("dw.dat"), PS)) {
            store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(4));
            RedoLogManager redo = RedoLogManager.durable(redoRepo);
            LogRangeHolder range = appendRedoCoveringPage(redo);
            markDirty(pool, range.endLsn(), 0xCB);
            redo.flush();
            FlushCoordinator coordinator = new FlushCoordinator(pool, store, redo, PS,
                    new RecoverableDoublewriteStrategy(dwRepo), Duration.ofMillis(10));

            List<FlushResult> results = coordinator.flushList(Lsn.of(1000), 10);

            assertEquals(FlushResultStatus.CLEAN, results.get(0).status());
            assertTrue(pool.dirtyPageCandidates(Lsn.of(1000), 10).isEmpty());
            byte[] disk = new byte[PS.bytes()];
            store.readPage(PAGE, ByteBuffer.wrap(disk));
            assertEquals((byte) 0xCB, disk[200]);
            assertTrue(dwRepo.latestCopy(PAGE).isPresent());
        }
    }

    private static void markDirty(BufferPool pool, long pageLsn, int marker) {
        try (PageGuard guard = pool.getPage(PAGE, PageLatchMode.EXCLUSIVE)) {
            guard.writeInt(PageEnvelopeLayout.SPACE_ID, SPACE.value());
            guard.writeInt(PageEnvelopeLayout.PAGE_NO, (int) PAGE.pageNo().value());
            guard.writeLong(PageEnvelopeLayout.PAGE_LSN, pageLsn);
            guard.writeBytes(200, new byte[]{(byte) marker});
        }
    }

    private static LogRangeHolder appendRedoCoveringPage(RedoLogManager redo) {
        PageBytesRecord record = new PageBytesRecord(PAGE, 200, new byte[]{(byte) 0xCB});
        cn.zhangyis.db.storage.redo.LogRange range = redo.append(List.of(record));
        return new LogRangeHolder(range.end().value());
    }

    private record LogRangeHolder(long endLsn) {
    }
}
