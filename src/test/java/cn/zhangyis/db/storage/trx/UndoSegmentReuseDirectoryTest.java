package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.SegmentId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.undo.CachedUndoSegmentRef;
import cn.zhangyis.db.storage.undo.FreeUndoSegmentRef;
import cn.zhangyis.db.storage.undo.UndoLogKind;
import cn.zhangyis.db.storage.undo.UndoSegmentHandle;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 统一复用目录的 cache LIFO、free FIFO 与跨物理 MTR lease 状态机测试。 */
class UndoSegmentReuseDirectoryTest {

    @Test
    void fullCacheRejectsPushWithoutReplacingPublishedOwner() {
        UndoSegmentReuseDirectory directory = new UndoSegmentReuseDirectory(1);
        CachedUndoSegmentRef first = cached(UndoLogKind.INSERT, 9, 1);
        publishCache(directory, first);

        assertTrue(directory.tryReserveCachePush(cached(UndoLogKind.INSERT, 10, 2)).isEmpty());
        assertEquals(first, directory.peekCache(UndoLogKind.INSERT).orElseThrow().segment());
    }

    @Test
    void cacheCandidateHasPriorityAndFreeQueueIsFifoAcrossKinds() {
        UndoSegmentReuseDirectory directory = new UndoSegmentReuseDirectory(1);
        FreeUndoSegmentRef freeFirst = free(10, 1);
        FreeUndoSegmentRef freeSecond = free(11, 2);
        publishFree(directory, List.of(freeFirst, freeSecond));
        CachedUndoSegmentRef cached = cached(UndoLogKind.UPDATE, 12, 3);
        publishCache(directory, cached);

        assertEquals(cached, directory.peekCache(UndoLogKind.UPDATE).orElseThrow().segment());
        assertEquals(freeFirst, directory.peekFree().orElseThrow().segment());

        try (UndoSegmentReuseDirectory.FreePopLease pop = directory.reserveFreePop(
                directory.peekFree().orElseThrow())) {
            pop.physicalMutationStarted();
            pop.complete();
        }
        assertEquals(freeSecond, directory.peekFree().orElseThrow().segment());
    }

    @Test
    void drainUsesCacheTopsThenFreeHeadsInStableBatches() {
        UndoSegmentReuseDirectory directory = new UndoSegmentReuseDirectory(2);
        CachedUndoSegmentRef insert = cached(UndoLogKind.INSERT, 10, 1);
        CachedUndoSegmentRef update = cached(UndoLogKind.UPDATE, 11, 2);
        FreeUndoSegmentRef freeFirst = free(12, 3);
        FreeUndoSegmentRef freeSecond = free(13, 4);
        publishCache(directory, insert);
        publishCache(directory, update);
        publishFree(directory, List.of(freeFirst, freeSecond));

        try (UndoSegmentReuseDirectory.DrainLease drain = directory.tryBeginDrain().orElseThrow()) {
            UndoSegmentReuseDirectory.DrainBatch first = drain.nextBatch(3).orElseThrow();
            assertEquals(List.of(insert), first.insertTopFirst());
            assertEquals(List.of(update), first.updateTopFirst());
            assertEquals(List.of(freeFirst), first.freeHeadFirst());
            drain.physicalMutationStarted(first);
            drain.completeBatch(first);

            UndoSegmentReuseDirectory.DrainBatch second = drain.nextBatch(3).orElseThrow();
            assertEquals(List.of(freeSecond), second.freeHeadFirst());
            drain.physicalMutationStarted(second);
            drain.completeBatch(second);
            assertTrue(drain.nextBatch(3).isEmpty());
            drain.finish();
        }
    }

    private static void publishCache(UndoSegmentReuseDirectory directory, CachedUndoSegmentRef ref) {
        try (UndoSegmentReuseDirectory.CachePushLease push = directory.tryReserveCachePush(ref).orElseThrow()) {
            push.physicalMutationStarted();
            push.complete();
        }
    }

    private static void publishFree(UndoSegmentReuseDirectory directory, List<FreeUndoSegmentRef> refs) {
        try (UndoSegmentReuseDirectory.FreePushLease push = directory.tryReserveFreePush(refs).orElseThrow()) {
            push.physicalMutationStarted();
            push.complete();
        }
    }

    private static CachedUndoSegmentRef cached(UndoLogKind kind, long pageNo, long segmentId) {
        return new CachedUndoSegmentRef(kind, handle(pageNo, segmentId));
    }

    private static FreeUndoSegmentRef free(long pageNo, long segmentId) {
        return new FreeUndoSegmentRef(handle(pageNo, segmentId));
    }

    private static UndoSegmentHandle handle(long pageNo, long segmentId) {
        SpaceId space = SpaceId.of(5);
        PageId page = PageId.of(space, PageNo.of(pageNo));
        return new UndoSegmentHandle(space, 0, SegmentId.of(segmentId), page, page);
    }
}
