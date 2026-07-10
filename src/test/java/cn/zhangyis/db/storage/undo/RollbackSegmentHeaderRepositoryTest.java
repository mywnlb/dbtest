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
import cn.zhangyis.db.storage.redo.RedoRecord;
import cn.zhangyis.db.storage.redo.UndoMetadataDeltaKind;
import cn.zhangyis.db.storage.redo.UndoMetadataDeltaRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * RollbackSegmentHeaderRepository：page3 slot 目录 format/claim/clear/read 往返，以及 owner CAS、redo、越界校验。
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
    void claimSlotThenReadRoundTrips() {
        withRepo((repo, mgr) -> {
            PageId firstPage = PageId.of(UNDO, PageNo.of(7));
            MiniTransaction w = mgr.begin();
            repo.format(w, UNDO, RSEG, 8);
            repo.claimSlot(w, UNDO, UndoSlotId.of(2), firstPage);
            mgr.commit(w);

            MiniTransaction r = mgr.begin();
            RollbackSegmentHeaderSnapshot snap = repo.read(r, UNDO, RSEG, 8);
            mgr.commit(r);

            assertEquals(Map.of(UndoSlotId.of(2), firstPage), snap.occupiedSlots());
        });
    }

    /**
     * 首写在分配 undo segment 前必须只读预检 page3；空槽通过，已有 owner 则抛精确领域冲突且不改页。
     */
    @Test
    void requireSlotFreeRejectsPersistentOwnerWithoutMutation() {
        withRepo((repo, mgr) -> {
            PageId owner = PageId.of(UNDO, PageNo.of(7));
            MiniTransaction format = mgr.begin();
            repo.format(format, UNDO, RSEG, 8);
            repo.claimSlot(format, UNDO, UndoSlotId.of(2), owner);
            mgr.commit(format);

            MiniTransaction freeCheck = mgr.begin();
            repo.requireSlotFree(freeCheck, UNDO, UndoSlotId.of(1));
            mgr.commit(freeCheck);

            MiniTransaction occupiedCheck = mgr.begin();
            UndoSlotOwnershipConflictException conflict = assertThrows(
                    UndoSlotOwnershipConflictException.class,
                    () -> repo.requireSlotFree(occupiedCheck, UNDO, UndoSlotId.of(2)));
            assertTrue(conflict.getMessage().contains("current first page=7"));
            mgr.rollbackUncommitted(occupiedCheck);

            MiniTransaction read = mgr.begin();
            assertEquals(Map.of(UndoSlotId.of(2), owner), repo.read(read, UNDO, RSEG, 8).occupiedSlots());
            mgr.commit(read);
        });
    }

    @Test
    void claimSlotAppendsUndoMetadataDeltaRedo() {
        withRepo((repo, mgr) -> {
            PageId firstPage = PageId.of(UNDO, PageNo.of(7));
            MiniTransaction w = mgr.begin();
            repo.format(w, UNDO, RSEG, 8);
            repo.claimSlot(w, UNDO, UndoSlotId.of(2), firstPage);
            mgr.commit(w);

            List<RedoRecord> records = mgr.redoLogManager().bufferedRecords();
            byte[] expected = longBytes(firstPage.pageNo().value());
            assertTrue(records.stream().anyMatch(record -> record instanceof UndoMetadataDeltaRecord delta
                            && delta.pageId().equals(RollbackSegmentHeaderRepository.headerPage(UNDO))
                            && delta.kind() == UndoMetadataDeltaKind.RSEG_SLOT
                            && delta.subjectId() == RSEG.value()
                            && delta.subIndex() == 2
                            && delta.offset() == RollbackSegmentHeaderLayout.slotOffset(2)
                            && Arrays.equals(expected, delta.afterImage())),
                    "rseg slot pageNo after-image must have a logical undo metadata redo record");
        });
    }

    @Test
    void clearSlotRequiresExpectedOwnerAndClearsSlot() {
        withRepo((repo, mgr) -> {
            MiniTransaction w = mgr.begin();
            repo.format(w, UNDO, RSEG, 8);
            PageId firstPage = PageId.of(UNDO, PageNo.of(7));
            repo.claimSlot(w, UNDO, UndoSlotId.of(2), firstPage);
            repo.clearSlot(w, UNDO, UndoSlotId.of(2), firstPage);
            mgr.commit(w);

            MiniTransaction r = mgr.begin();
            assertTrue(repo.read(r, UNDO, RSEG, 8).occupiedSlots().isEmpty());
            mgr.commit(r);
        });
    }

    @Test
    void claimOccupiedAndClearStaleOwnerFailClosed() {
        withRepo((repo, mgr) -> {
            PageId owner = PageId.of(UNDO, PageNo.of(7));
            MiniTransaction format = mgr.begin();
            repo.format(format, UNDO, RSEG, 8);
            repo.claimSlot(format, UNDO, UndoSlotId.of(2), owner);
            mgr.commit(format);

            MiniTransaction duplicate = mgr.begin();
            assertThrows(UndoLogFormatException.class,
                    () -> repo.claimSlot(duplicate, UNDO, UndoSlotId.of(2), PageId.of(UNDO, PageNo.of(8))));
            mgr.rollbackUncommitted(duplicate);

            MiniTransaction staleClear = mgr.begin();
            assertThrows(UndoLogFormatException.class,
                    () -> repo.clearSlot(staleClear, UNDO, UndoSlotId.of(2), PageId.of(UNDO, PageNo.of(8))));
            mgr.rollbackUncommitted(staleClear);

            MiniTransaction read = mgr.begin();
            assertEquals(Map.of(UndoSlotId.of(2), owner), repo.read(read, UNDO, RSEG, 8).occupiedSlots());
            mgr.commit(read);
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
    void claimSlotRejectsOutOfRangeSlot() {
        withRepo((repo, mgr) -> {
            MiniTransaction w = mgr.begin();
            repo.format(w, UNDO, RSEG, 4);
            assertThrows(UndoLogFormatException.class,
                    () -> repo.claimSlot(w, UNDO, UndoSlotId.of(10), PageId.of(UNDO, PageNo.of(7))));
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

    private static byte[] longBytes(long value) {
        return ByteBuffer.allocate(Long.BYTES).putLong(value).array();
    }

    @FunctionalInterface
    private interface RepoTest {
        void run(RollbackSegmentHeaderRepository repo, MiniTransactionManager mgr);
    }
}
