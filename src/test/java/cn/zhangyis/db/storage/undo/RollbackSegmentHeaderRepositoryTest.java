package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.RollbackSegmentId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.domain.UndoSlotId;
import cn.zhangyis.db.domain.TransactionNo;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.buf.PageLatchMode;
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
import java.util.Optional;

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
        assertEquals(4, RollbackSegmentHeaderLayout.FORMAT_VERSION);
        assertEquals(66, RollbackSegmentHeaderLayout.HISTORY_HEAD_PAGE_NO);
        assertEquals(74, RollbackSegmentHeaderLayout.HISTORY_TAIL_PAGE_NO);
        assertEquals(82, RollbackSegmentHeaderLayout.HISTORY_LENGTH);
        assertEquals(90, RollbackSegmentHeaderLayout.LAST_TRANSACTION_NO);
        assertEquals(98, RollbackSegmentHeaderLayout.FREE_HEAD_PAGE_NO);
        assertEquals(106, RollbackSegmentHeaderLayout.FREE_TAIL_PAGE_NO);
        assertEquals(114, RollbackSegmentHeaderLayout.FREE_LENGTH);
        assertEquals(122, RollbackSegmentHeaderLayout.SLOT_ARRAY_BASE);
        withRepo((repo, mgr) -> {
            MiniTransaction w = mgr.begin();
            repo.format(w, UNDO, RSEG, 8);
            mgr.commit(w);

            MiniTransaction r = mgr.begin();
            RollbackSegmentHeaderSnapshot snap = repo.read(r, UNDO, RSEG, 8);
            mgr.commit(r);

            assertEquals(8, snap.slotCapacity());
            assertTrue(snap.occupiedSlots().isEmpty());
            assertEquals(RollbackSegmentHistoryBase.empty(), snap.historyBase());
            assertEquals(RollbackSegmentFreeListBase.empty(), snap.freeListBase());
        });
    }

    /** page3 v4 的 free owner 采用尾插头摘；owner 在 active slot 与 free base 之间原子迁移。 */
    @Test
    void activeOwnersAppendToFreeFifoAndHeadMovesBackToActive() {
        withRepo((repo, mgr) -> {
            PageId first = PageId.of(UNDO, PageNo.of(7));
            PageId second = PageId.of(UNDO, PageNo.of(6));
            MiniTransaction write = mgr.begin();
            repo.format(write, UNDO, RSEG, 8, 0);
            repo.claimSlot(write, UNDO, UndoSlotId.of(0), first);
            RollbackSegmentFreeListBase one = repo.moveActiveSlotsToFree(write, UNDO,
                    RollbackSegmentFreeListBase.empty(), List.of(
                            new RollbackSegmentHeaderRepository.FreePush(UndoSlotId.of(0), first)));
            repo.claimSlot(write, UNDO, UndoSlotId.of(1), second);
            RollbackSegmentFreeListBase two = repo.moveActiveSlotsToFree(write, UNDO, one, List.of(
                    new RollbackSegmentHeaderRepository.FreePush(UndoSlotId.of(1), second)));
            mgr.commit(write);

            assertEquals(first, two.headPageId().orElseThrow());
            assertEquals(second, two.tailPageId().orElseThrow());
            assertEquals(2L, two.length());

            MiniTransaction reuse = mgr.begin();
            RollbackSegmentFreeListBase remaining = repo.moveFreeHeadToActiveSlot(reuse, UNDO, two,
                    first, Optional.of(second), UndoSlotId.of(3));
            mgr.commit(reuse);

            MiniTransaction read = mgr.beginReadOnly();
            RollbackSegmentHeaderSnapshot snapshot = repo.read(read, UNDO, RSEG, 8, 0);
            mgr.commit(read);
            assertEquals(Map.of(UndoSlotId.of(3), first), snapshot.occupiedSlots());
            assertEquals(remaining, snapshot.freeListBase());
            assertEquals(second, remaining.headPageId().orElseThrow());
            assertEquals(second, remaining.tailPageId().orElseThrow());
            assertEquals(1L, remaining.length());
            assertTrue(mgr.redoLogManager().bufferedRecords().stream()
                    .filter(UndoMetadataDeltaRecord.class::isInstance)
                    .map(UndoMetadataDeltaRecord.class::cast)
                    .anyMatch(delta -> delta.kind() == UndoMetadataDeltaKind.RSEG_FREE_BASE));
        });
    }

    /** history base 按物理 append 顺序维护，lastTransactionNo 取最大值且 purge 后不回退。 */
    @Test
    void historyBaseAppendAndHeadRemovalPreserveHighWaterWithoutSorting() {
        withRepo((repo, mgr) -> {
            PageId first = PageId.of(UNDO, PageNo.of(7));
            PageId second = PageId.of(UNDO, PageNo.of(6));
            MiniTransaction format = mgr.begin();
            repo.format(format, UNDO, RSEG, 8, 0);
            repo.claimSlot(format, UNDO, UndoSlotId.of(0), first);
            repo.claimSlot(format, UNDO, UndoSlotId.of(1), second);
            mgr.commit(format);

            MiniTransaction firstAppend = mgr.begin();
            RollbackSegmentHistoryBase one = repo.appendHistory(firstAppend, UNDO,
                    RollbackSegmentHistoryBase.empty(), UndoSlotId.of(0), first, TransactionNo.of(20));
            mgr.commit(firstAppend);
            MiniTransaction secondAppend = mgr.begin();
            RollbackSegmentHistoryBase two = repo.appendHistory(secondAppend, UNDO, one,
                    UndoSlotId.of(1), second, TransactionNo.of(10));
            mgr.commit(secondAppend);

            assertEquals(Optional.of(first), two.headPageId());
            assertEquals(Optional.of(second), two.tailPageId());
            assertEquals(2L, two.length());
            assertEquals(TransactionNo.of(20), two.lastTransactionNo(),
                    "late physical append with a smaller allocated number must not lower the high-water");

            MiniTransaction removeFirst = mgr.begin();
            RollbackSegmentHistoryBase oneLeft = repo.removeHistoryHead(removeFirst, UNDO, two,
                    UndoSlotId.of(0), first, Optional.of(second));
            mgr.commit(removeFirst);
            MiniTransaction removeSecond = mgr.begin();
            RollbackSegmentHistoryBase empty = repo.removeHistoryHead(removeSecond, UNDO, oneLeft,
                    UndoSlotId.of(1), second, Optional.empty());
            mgr.commit(removeSecond);

            assertEquals(0L, empty.length());
            assertEquals(TransactionNo.of(20), empty.lastTransactionNo(),
                    "purge must not regress the restart transaction-number fence");
            assertTrue(mgr.redoLogManager().bufferedRecords().stream()
                    .filter(UndoMetadataDeltaRecord.class::isInstance)
                    .map(UndoMetadataDeltaRecord.class::cast)
                    .anyMatch(delta -> delta.kind() == UndoMetadataDeltaKind.RSEG_HISTORY_BASE));
        });
    }

    /** page3 v4 的 active/cache owner 转移必须保持 LIFO，且 owner 任一时刻只出现在一个目录。 */
    @Test
    void activeOwnersMoveToCacheAndLifoTopMovesBackToActive() {
        withRepo((repo, mgr) -> {
            PageId first = PageId.of(UNDO, PageNo.of(7));
            PageId second = PageId.of(UNDO, PageNo.of(6));
            MiniTransaction write = mgr.begin();
            repo.format(write, UNDO, RSEG, 8, 2);
            repo.claimSlot(write, UNDO, UndoSlotId.of(0), first);
            repo.claimSlot(write, UNDO, UndoSlotId.of(1), second);
            repo.moveActiveSlotsToCache(write, UNDO, List.of(
                    new RollbackSegmentHeaderRepository.CachePush(
                            UndoSlotId.of(0), first, UndoLogKind.INSERT, 0)));
            repo.moveActiveSlotsToCache(write, UNDO, List.of(
                    new RollbackSegmentHeaderRepository.CachePush(
                            UndoSlotId.of(1), second, UndoLogKind.INSERT, 1)));
            mgr.commit(write);

            MiniTransaction cachedRead = mgr.beginReadOnly();
            RollbackSegmentHeaderSnapshot cached = repo.read(cachedRead, UNDO, RSEG, 8, 2);
            mgr.commit(cachedRead);
            assertTrue(cached.occupiedSlots().isEmpty());
            assertEquals(List.of(first, second), cached.cachedInsertSegments());

            MiniTransaction reuse = mgr.begin();
            repo.moveCachedTopToActiveSlot(reuse, UNDO, UndoLogKind.INSERT,
                    2, second, UndoSlotId.of(3));
            mgr.commit(reuse);

            MiniTransaction activeRead = mgr.beginReadOnly();
            RollbackSegmentHeaderSnapshot active = repo.read(activeRead, UNDO, RSEG, 8, 2);
            mgr.commit(activeRead);
            assertEquals(List.of(first), active.cachedInsertSegments());
            assertEquals(Map.of(UndoSlotId.of(3), second), active.occupiedSlots());
            assertTrue(mgr.redoLogManager().bufferedRecords().stream()
                    .filter(UndoMetadataDeltaRecord.class::isInstance)
                    .map(UndoMetadataDeltaRecord.class::cast)
                    .anyMatch(delta -> delta.kind() == UndoMetadataDeltaKind.RSEG_CACHE_ENTRY));
        });
    }

    @Test
    void truncateStyleTopRemovalValidatesExpectedCountAndOrder() {
        withRepo((repo, mgr) -> {
            PageId first = PageId.of(UNDO, PageNo.of(7));
            PageId second = PageId.of(UNDO, PageNo.of(6));
            MiniTransaction write = mgr.begin();
            repo.format(write, UNDO, RSEG, 8, 2);
            repo.claimSlot(write, UNDO, UndoSlotId.of(0), first);
            repo.moveActiveSlotsToCache(write, UNDO, List.of(
                    new RollbackSegmentHeaderRepository.CachePush(
                            UndoSlotId.of(0), first, UndoLogKind.UPDATE, 0)));
            repo.claimSlot(write, UNDO, UndoSlotId.of(1), second);
            repo.moveActiveSlotsToCache(write, UNDO, List.of(
                    new RollbackSegmentHeaderRepository.CachePush(
                            UndoSlotId.of(1), second, UndoLogKind.UPDATE, 1)));
            mgr.commit(write);

            MiniTransaction stale = mgr.begin();
            assertThrows(UndoLogFormatException.class, () -> repo.removeCachedTops(stale, UNDO, List.of(
                    new RollbackSegmentHeaderRepository.CacheTopRemoval(
                            UndoLogKind.UPDATE, 2, List.of(first)))));
            mgr.rollbackUncommitted(stale);

            MiniTransaction remove = mgr.begin();
            repo.removeCachedTops(remove, UNDO, List.of(
                    new RollbackSegmentHeaderRepository.CacheTopRemoval(
                            UndoLogKind.UPDATE, 2, List.of(second, first))));
            mgr.commit(remove);
            MiniTransaction read = mgr.beginReadOnly();
            assertTrue(repo.read(read, UNDO, RSEG, 8, 2).cachedUpdateSegments().isEmpty());
            mgr.commit(read);
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
            assertThrows(UndoLogFormatException.class, () -> repo.read(r, UNDO, RSEG, 8, 7));
            mgr.commit(r);
        });
    }

    /** v1 没有持久 cache owner 区，不能在启用 v2 的进程中按空缓存猜测打开。 */
    @Test
    void readRejectsLegacyV1HeaderFailClosed() {
        PageStore store = new FileChannelPageStore();
        store.create(UNDO, dir.resolve("undo-v1.ibu"), PS, PageNo.of(8));
        try (PageStore ignored = store; BufferPool pool = new LruBufferPool(store, PS, 8)) {
            RollbackSegmentHeaderRepository repo = new RollbackSegmentHeaderRepository(pool, PS);
            MiniTransactionManager mgr = new MiniTransactionManager();
            MiniTransaction format = mgr.begin();
            repo.format(format, UNDO, RSEG, 8, 2);
            mgr.commit(format);

            MiniTransaction corrupt = mgr.begin();
            PageGuard page = corrupt.getPage(pool, RollbackSegmentHeaderRepository.headerPage(UNDO),
                    PageLatchMode.EXCLUSIVE);
            page.writeInt(RollbackSegmentHeaderLayout.FORMAT, 1);
            mgr.commit(corrupt);

            MiniTransaction read = mgr.beginReadOnly();
            assertThrows(UndoLogFormatException.class, () -> repo.read(read, UNDO, RSEG, 8, 2));
            mgr.rollbackUncommitted(read);
        }
    }

    /** page3 v2 没有 history base/lastTransactionNo，v3 进程必须拒绝猜测迁移。 */
    @Test
    void readRejectsLegacyV2HeaderFailClosed() {
        PageStore store = new FileChannelPageStore();
        store.create(UNDO, dir.resolve("undo-v2.ibu"), PS, PageNo.of(8));
        try (PageStore ignored = store; BufferPool pool = new LruBufferPool(store, PS, 8)) {
            RollbackSegmentHeaderRepository repo = new RollbackSegmentHeaderRepository(pool, PS);
            MiniTransactionManager mgr = new MiniTransactionManager();
            MiniTransaction format = mgr.begin();
            repo.format(format, UNDO, RSEG, 8, 2);
            mgr.commit(format);

            MiniTransaction corrupt = mgr.begin();
            corrupt.getPage(pool, RollbackSegmentHeaderRepository.headerPage(UNDO), PageLatchMode.EXCLUSIVE)
                    .writeInt(RollbackSegmentHeaderLayout.FORMAT, 2);
            mgr.commit(corrupt);

            MiniTransaction read = mgr.beginReadOnly();
            assertThrows(UndoLogFormatException.class, () -> repo.read(read, UNDO, RSEG, 8, 2));
            mgr.rollbackUncommitted(read);
        }
    }

    /** page3 v3 没有持久 free base，v4 进程必须 fail-closed。 */
    @Test
    void readRejectsLegacyV3HeaderFailClosed() {
        PageStore store = new FileChannelPageStore();
        store.create(UNDO, dir.resolve("undo-v3.ibu"), PS, PageNo.of(8));
        try (PageStore ignored = store; BufferPool pool = new LruBufferPool(store, PS, 8)) {
            RollbackSegmentHeaderRepository repo = new RollbackSegmentHeaderRepository(pool, PS);
            MiniTransactionManager mgr = new MiniTransactionManager();
            MiniTransaction format = mgr.begin();
            repo.format(format, UNDO, RSEG, 8, 2);
            mgr.commit(format);

            MiniTransaction corrupt = mgr.begin();
            corrupt.getPage(pool, RollbackSegmentHeaderRepository.headerPage(UNDO),
                    PageLatchMode.EXCLUSIVE).writeInt(RollbackSegmentHeaderLayout.FORMAT, 3);
            mgr.commit(corrupt);

            MiniTransaction read = mgr.beginReadOnly();
            assertThrows(UndoLogFormatException.class, () -> repo.read(read, UNDO, RSEG, 8, 2));
            mgr.rollbackUncommitted(read);
        }
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
