package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.common.exception.DatabaseFatalException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.TransactionNo;
import cn.zhangyis.db.domain.UndoSlotId;
import cn.zhangyis.db.storage.api.TablePurgeBarrierTimeoutException;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * P5 HistoryList：committed 队列按提交序 FIFO。purge 先 peek，在 undo 段原子终结成功后再用
 * expected identity 精确完成队首；纯 insert undo 在提交路径直接终结，不再经过独立回收队列。
 */
class HistoryListTest {

    private static HistoryEntry entry(long no, long trx, long pageNo, int slot) {
        return new HistoryEntry(TransactionNo.of(no), TransactionId.of(trx), SpaceId.of(1),
                PageId.of(SpaceId.of(1), PageNo.of(pageNo)), UndoSlotId.of(slot));
    }

    private static HistoryEntry entry(long no, long trx, long pageNo, int slot, long... tableIds) {
        return new HistoryEntry(TransactionNo.of(no), TransactionId.of(trx), SpaceId.of(1),
                PageId.of(SpaceId.of(1), PageNo.of(pageNo)), UndoSlotId.of(slot),
                java.util.Arrays.stream(tableIds).boxed().collect(java.util.stream.Collectors.toSet()));
    }

    @Test
    void committedIsFifoPeekThenCompleteExpectedHead() {
        HistoryList h = new HistoryList();
        HistoryEntry first = entry(1, 100, 65, 0);
        HistoryEntry second = entry(2, 101, 66, 1);
        try (HistoryList.AppendLease append = h.beginAppend(first)) {
            append.physicalMutationStarted();
            append.complete();
        }
        try (HistoryList.AppendLease append = h.beginAppend(second)) {
            append.physicalMutationStarted();
            append.complete();
        }
        assertEquals(2, h.committedSize());
        assertEquals(1L, h.peekCommitted().orElseThrow().transactionNo().value(), "peek 给最老提交，不移除");
        assertEquals(2, h.committedSize());
        try (HistoryList.HeadRemovalLease removal = h.beginHeadRemoval(first)) {
            removal.physicalMutationStarted();
            removal.complete();
        }
        assertEquals(2L, h.peekCommitted().orElseThrow().transactionNo().value());
        try (HistoryList.HeadRemovalLease removal = h.beginHeadRemoval(second)) {
            removal.physicalMutationStarted();
            removal.complete();
        }
        assertTrue(h.peekCommitted().isEmpty());
        assertEquals(0, h.committedSize());
    }

    @Test
    void completeRejectsWrongOrRepeatedHead() {
        HistoryList h = new HistoryList();
        HistoryEntry first = entry(1, 100, 65, 0);
        HistoryEntry second = entry(2, 101, 66, 1);
        h.restore(java.util.List.of(first));

        assertThrows(DatabaseValidationException.class, () -> h.beginHeadRemoval(second),
                "错序完成不能摘除真实队首");
        assertEquals(first, h.peekCommitted().orElseThrow());
        try (HistoryList.HeadRemovalLease removal = h.beginHeadRemoval(first)) {
            removal.physicalMutationStarted();
            removal.complete();
        }
        assertThrows(DatabaseValidationException.class, () -> h.beginHeadRemoval(first),
                "重复完成不能静默成功");
    }

    /** TransactionNo 由 prepareCommit 分配，物理 append 顺序可倒置；运行时必须忠实保留物理链序。 */
    @Test
    void restorePreservesPhysicalOrderWithoutTransactionNumberSorting() {
        HistoryList history = new HistoryList();
        HistoryEntry physicallyFirst = entry(20, 100, 65, 0);
        HistoryEntry physicallySecond = entry(10, 101, 66, 1);

        history.restore(List.of(physicallyFirst, physicallySecond));

        assertEquals(List.of(physicallyFirst, physicallySecond), history.snapshot());
        assertEquals(TransactionNo.of(20), history.peekCommitted().orElseThrow().transactionNo());
    }

    @Test
    void prePhysicalCloseReleasesTransitionButPostPhysicalCloseFencesWriters() {
        HistoryList history = new HistoryList(Duration.ofMillis(30));
        HistoryList.AppendLease cancelled = history.beginAppend(entry(1, 100, 65, 0));
        cancelled.close();

        HistoryList.AppendLease fenced = history.beginAppend(entry(2, 101, 66, 1));
        fenced.physicalMutationStarted();
        assertThrows(DatabaseFatalException.class, fenced::close);
        assertThrows(HistoryTransitionTimeoutException.class,
                () -> history.beginAppend(entry(3, 102, 67, 2)),
                "越过物理边界的未知状态必须保持 fail-stop transition");
    }

    @Test
    void waitingTransitionIsInterruptibleAndRestoresInterruptFlag() throws Exception {
        HistoryList history = new HistoryList(Duration.ofSeconds(2));
        HistoryList.AppendLease blocker = history.beginAppend(entry(1, 100, 65, 0));
        CountDownLatch started = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicBoolean interruptRestored = new AtomicBoolean();
        Thread waiter = Thread.ofPlatform().start(() -> {
            started.countDown();
            try {
                history.beginAppend(entry(2, 101, 66, 1));
            } catch (Throwable error) {
                failure.set(error);
                interruptRestored.set(Thread.currentThread().isInterrupted());
            }
        });
        assertTrue(started.await(1, java.util.concurrent.TimeUnit.SECONDS));
        waiter.interrupt();
        waiter.join(1_000);
        blocker.close();

        assertTrue(failure.get() instanceof HistoryTransitionInterruptedException);
        assertTrue(interruptRestored.get(), "domain exception must not consume the caller's interrupt signal");
    }

    @Test
    void purgeRemovalAndConcurrentCommitAppendPublishWithoutLostUpdate() throws Exception {
        HistoryList history = new HistoryList(Duration.ofSeconds(1));
        HistoryEntry oldHead = entry(1, 100, 65, 0);
        HistoryEntry newCommit = entry(2, 101, 66, 1);
        history.restore(List.of(oldHead));

        try (HistoryList.HeadRemovalLease removal = history.beginHeadRemoval(oldHead);
             var executor = Executors.newSingleThreadExecutor()) {
            CountDownLatch started = new CountDownLatch(1);
            var append = executor.submit(() -> {
                started.countDown();
                try (HistoryList.AppendLease lease = history.beginAppend(newCommit)) {
                    lease.physicalMutationStarted();
                    lease.complete();
                }
            });
            assertTrue(started.await(1, TimeUnit.SECONDS));
            removal.physicalMutationStarted();
            removal.complete();
            append.get(1, TimeUnit.SECONDS);
        }

        assertEquals(List.of(newCommit), history.snapshot());
    }

    /** commit 发布增加表引用，purge finalization 发布减少引用并唤醒等待中的 DROP barrier。 */
    @Test
    void tableBarrierTracksPublishedHistoryAndWakesAfterPurge() throws Exception {
        HistoryList history = new HistoryList(Duration.ofSeconds(1));
        HistoryTablePurgeBarrier barrier = new HistoryTablePurgeBarrier(history);
        HistoryEntry entry = entry(1, 100, 65, 0, 11L, 12L);
        try (HistoryList.AppendLease append = history.beginAppend(entry)) {
            append.physicalMutationStarted();
            append.complete();
        }

        assertEquals(1, barrier.referenceCount(11L));
        assertThrows(TablePurgeBarrierTimeoutException.class,
                () -> barrier.awaitUnreferenced(11L, Duration.ofMillis(20)));

        try (var executor = Executors.newSingleThreadExecutor()) {
            var waiter = executor.submit(() -> barrier.awaitUnreferenced(11L, Duration.ofSeconds(1)));
            try (HistoryList.HeadRemovalLease removal = history.beginHeadRemoval(entry)) {
                removal.physicalMutationStarted();
                removal.complete();
            }
            waiter.get(1, TimeUnit.SECONDS);
        }

        assertEquals(0, barrier.referenceCount(11L));
        assertEquals(0, barrier.referenceCount(12L));
    }
}
