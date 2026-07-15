package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.SegmentId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.undo.CachedUndoSegmentRef;
import cn.zhangyis.db.storage.undo.UndoLogKind;
import cn.zhangyis.db.storage.undo.UndoSegmentHandle;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** cache directory 的短锁/lease 状态机测试；不访问页、FSP 或 redo。 */
class UndoSegmentCacheDirectoryTest {

    @Test
    void pushAndPopPublishOnlyAfterPhysicalCompletion() {
        UndoSegmentCacheDirectory directory = new UndoSegmentCacheDirectory(2);
        CachedUndoSegmentRef first = cached(UndoLogKind.INSERT, 10, 1);
        try (UndoSegmentCacheDirectory.PushLease push = directory.tryReservePush(first).orElseThrow()) {
            assertEquals(0, directory.cachedCount(UndoLogKind.INSERT));
            push.physicalMutationStarted();
            push.complete();
        }
        assertEquals(first, directory.peek(UndoLogKind.INSERT).orElseThrow().segment());

        try (UndoSegmentCacheDirectory.PopLease pop = directory.reservePop(
                directory.peek(UndoLogKind.INSERT).orElseThrow())) {
            assertEquals(1, directory.cachedCount(UndoLogKind.INSERT));
            pop.physicalMutationStarted();
            pop.complete();
        }
        assertTrue(directory.peek(UndoLogKind.INSERT).isEmpty());
    }

    @Test
    void fullStackRejectsNewPushWithoutReplacingExistingOwner() {
        UndoSegmentCacheDirectory directory = new UndoSegmentCacheDirectory(1);
        CachedUndoSegmentRef first = cached(UndoLogKind.UPDATE, 10, 1);
        publish(directory, first);

        assertTrue(directory.tryReservePush(cached(UndoLogKind.UPDATE, 11, 2)).isEmpty());
        assertEquals(first, directory.peek(UndoLogKind.UPDATE).orElseThrow().segment());
    }

    @Test
    void drainGateNeverWaitsForInFlightTransitionAndBatchesAreLifo() {
        UndoSegmentCacheDirectory directory = new UndoSegmentCacheDirectory(2);
        CachedUndoSegmentRef insertBottom = cached(UndoLogKind.INSERT, 10, 1);
        CachedUndoSegmentRef insertTop = cached(UndoLogKind.INSERT, 11, 2);
        CachedUndoSegmentRef update = cached(UndoLogKind.UPDATE, 12, 3);
        publish(directory, insertBottom);
        publish(directory, insertTop);
        publish(directory, update);

        UndoSegmentCacheDirectory.PopLease inFlight = directory.reservePop(
                directory.peek(UndoLogKind.INSERT).orElseThrow());
        assertTrue(directory.tryBeginDrain().isEmpty(), "drain 在 transition 忙时必须立即返回而不是等待");
        inFlight.close();

        try (UndoSegmentCacheDirectory.DrainLease drain = directory.tryBeginDrain().orElseThrow()) {
            UndoSegmentCacheDirectory.DrainBatch first = drain.nextBatch(2).orElseThrow();
            assertEquals(List.of(insertTop, insertBottom), first.insertTopFirst());
            assertTrue(first.updateTopFirst().isEmpty());
            drain.physicalMutationStarted(first);
            drain.completeBatch(first);

            UndoSegmentCacheDirectory.DrainBatch second = drain.nextBatch(2).orElseThrow();
            assertEquals(List.of(update), second.updateTopFirst());
            drain.physicalMutationStarted(second);
            drain.completeBatch(second);
            assertTrue(drain.nextBatch(2).isEmpty());
            drain.finish();
        }
        assertEquals(0, directory.cachedCount(UndoLogKind.INSERT));
        assertEquals(0, directory.cachedCount(UndoLogKind.UPDATE));
    }

    private static void publish(UndoSegmentCacheDirectory directory, CachedUndoSegmentRef cached) {
        try (UndoSegmentCacheDirectory.PushLease push = directory.tryReservePush(cached).orElseThrow()) {
            push.physicalMutationStarted();
            push.complete();
        }
    }

    private static CachedUndoSegmentRef cached(UndoLogKind kind, long pageNo, long segmentId) {
        SpaceId space = SpaceId.of(5);
        PageId page = PageId.of(space, PageNo.of(pageNo));
        return new CachedUndoSegmentRef(kind,
                new UndoSegmentHandle(space, 0, SegmentId.of(segmentId), page, page));
    }
}
