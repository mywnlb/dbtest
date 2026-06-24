package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.RollbackSegmentId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.domain.UndoSlotId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.fil.io.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * RollbackSegmentHeaderRepository：page3 slot 目录 format/writeSlot/read 往返 + 页头/容量/越界校验。
 */
class RollbackSegmentHeaderRepositoryTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId UNDO = SpaceId.of(5);
    private static final RollbackSegmentId RSEG = RollbackSegmentId.of(0);

    @TempDir
    Path dir;

    @Test
    void formatThenReadEmptyDirectory() {
        withRepo((repo, mgr) -> {
            MiniTransaction w = mgr.begin();
            repo.format(w, UNDO, RSEG, 8);
            mgr.commit(w);

            MiniTransaction r = mgr.begin();
            RollbackSegmentHeaderSnapshot snap = repo.read(r, UNDO, RSEG, 8);
            mgr.commit(r);

            assertEquals(8, snap.slotCapacity());
            assertTrue(snap.occupiedSlots().isEmpty());
        });
    }

    @Test
    void writeSlotThenReadRoundTrips() {
        withRepo((repo, mgr) -> {
            PageId firstPage = PageId.of(UNDO, PageNo.of(7));
            MiniTransaction w = mgr.begin();
            repo.format(w, UNDO, RSEG, 8);
            repo.writeSlot(w, UNDO, UndoSlotId.of(2), firstPage);
            mgr.commit(w);

            MiniTransaction r = mgr.begin();
            RollbackSegmentHeaderSnapshot snap = repo.read(r, UNDO, RSEG, 8);
            mgr.commit(r);

            assertEquals(Map.of(UndoSlotId.of(2), firstPage), snap.occupiedSlots());
        });
    }

    @Test
    void writeSlotNullClearsSlot() {
        withRepo((repo, mgr) -> {
            MiniTransaction w = mgr.begin();
            repo.format(w, UNDO, RSEG, 8);
            repo.writeSlot(w, UNDO, UndoSlotId.of(2), PageId.of(UNDO, PageNo.of(7)));
            repo.writeSlot(w, UNDO, UndoSlotId.of(2), null);
            mgr.commit(w);

            MiniTransaction r = mgr.begin();
            assertTrue(repo.read(r, UNDO, RSEG, 8).occupiedSlots().isEmpty());
            mgr.commit(r);
        });
    }

    @Test
    void readRejectsRsegIdAndCapacityMismatch() {
        withRepo((repo, mgr) -> {
            MiniTransaction w = mgr.begin();
            repo.format(w, UNDO, RSEG, 8);
            mgr.commit(w);

            MiniTransaction r = mgr.begin();
            assertThrows(UndoLogFormatException.class, () -> repo.read(r, UNDO, RollbackSegmentId.of(1), 8));
            assertThrows(UndoLogFormatException.class, () -> repo.read(r, UNDO, RSEG, 4));
            mgr.commit(r);
        });
    }

    @Test
    void readRejectsUnformattedPage() {
        withRepo((repo, mgr) -> {
            MiniTransaction r = mgr.begin();
            assertThrows(UndoLogFormatException.class, () -> repo.read(r, UNDO, RSEG, 8));
            mgr.commit(r);
        });
    }

    @Test
    void writeSlotRejectsOutOfRangeSlot() {
        withRepo((repo, mgr) -> {
            MiniTransaction w = mgr.begin();
            repo.format(w, UNDO, RSEG, 4);
            assertThrows(UndoLogFormatException.class,
                    () -> repo.writeSlot(w, UNDO, UndoSlotId.of(10), PageId.of(UNDO, PageNo.of(7))));
            mgr.commit(w);
        });
    }

    @Test
    void formatRejectsCapacityOverflowingPage() {
        withRepo((repo, mgr) -> {
            MiniTransaction w = mgr.begin();
            assertThrows(DatabaseValidationException.class, () -> repo.format(w, UNDO, RSEG, 1_000_000));
            mgr.commit(w);
        });
    }

    private void withRepo(RepoTest body) {
        PageStore store = new FileChannelPageStore();
        store.create(UNDO, dir.resolve("undo.ibu"), PS, PageNo.of(8));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 8)) {
            RollbackSegmentHeaderRepository repo = new RollbackSegmentHeaderRepository(pool, PS);
            body.run(repo, new MiniTransactionManager());
        }
    }

    @FunctionalInterface
    private interface RepoTest {
        void run(RollbackSegmentHeaderRepository repo, MiniTransactionManager mgr);
    }
}
