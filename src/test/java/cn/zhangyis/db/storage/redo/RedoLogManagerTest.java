package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.storage.page.PageType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** RedoLogManager：append 分配单调 LSN 区间、buffer 累积、空批退化、快照不可变。 */
class RedoLogManagerTest {

    private static final PageId PID = PageId.of(SpaceId.of(1), PageNo.of(3));

    /**
     * 验证 {@code appendAssignsContiguousMonotonicRanges} 对应的Redo/WAL行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void appendAssignsContiguousMonotonicRanges() {
        RedoLogManager mgr = new RedoLogManager();
        PageBytesRecord a = new PageBytesRecord(PID, 0, new byte[]{1, 2, 3});
        PageInitRecord b = new PageInitRecord(PID, PageType.INDEX);

        LogRange r1 = mgr.append(List.of(a));
        LogRange r2 = mgr.append(List.of(b));

        assertEquals(0L, r1.start().value(), "first LSN at 0");
        assertEquals(r1.start().value() + a.byteLength(), r1.end().value());
        assertEquals(r1.end(), r2.start(), "ranges are contiguous");
        assertEquals(r1.end().value() + b.byteLength(), r2.end().value());
        assertEquals(r2.end(), mgr.currentLsn());
        assertEquals(2, mgr.bufferedRecords().size());
    }

    /**
     * 验证 {@code appendDoesNotAdvanceClosedLsnUntilRangeIsClosed} 所描述的边界场景保持既有领域不变量，不产生方法名明确禁止的副作用。
     */
    @Test
    void appendDoesNotAdvanceClosedLsnUntilRangeIsClosed() {
        RedoLogManager mgr = new RedoLogManager();

        LogRange range = mgr.append(List.of(new PageInitRecord(PID, PageType.INDEX)));

        assertEquals(range.end(), mgr.currentLsn(), "append still reserves the current LSN boundary");
        assertEquals(0L, mgr.closedLsn().value(), "redo is not checkpoint-safe before dirty pages are published");
        mgr.markClosed(range);
        assertEquals(range.end(), mgr.closedLsn(), "closing the range publishes it to checkpoint");
    }

    /**
     * 验证 {@code appendPublishesReadyForWriteLsn} 对应的Redo/WAL行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void appendPublishesReadyForWriteLsn() {
        RedoLogManager mgr = new RedoLogManager();

        LogRange range = mgr.append(List.of(new PageBytesRecord(PID, 16, new byte[]{5})));

        assertEquals(range.end(), mgr.readyForWriteLsn(),
                "synchronous append writes the log buffer before returning");
    }

    /**
     * 验证 {@code recentClosedMergesOutOfOrderRangesOnlyWhenContiguous} 所描述的 B+Tree 定位或结构变化，并断言键序、父子链接、页资源和唯一性不变量。
     */
    @Test
    void recentClosedMergesOutOfOrderRangesOnlyWhenContiguous() {
        RedoLogManager mgr = new RedoLogManager();
        LogRange first = mgr.append(List.of(new PageBytesRecord(PID, 0, new byte[]{1})));
        LogRange second = mgr.append(List.of(new PageBytesRecord(PID, 8, new byte[]{2})));

        mgr.markClosed(second);
        assertEquals(0L, mgr.closedLsn().value(), "later range alone must not create a checkpoint gap");

        mgr.markClosed(first);
        assertEquals(second.end(), mgr.closedLsn(), "closing the missing prefix merges pending later ranges");
    }

    /**
     * 验证 {@code appendDoesNotWaitForOngoingFlushFsync} 所描述的并发场景，并断言等待、唤醒、超时与资源释放顺序。
     *
     * @throws Exception 底层扩展点报告受检失败时抛出；调用方应保留原始 cause 并终止当前编排步骤
     */
    @Test
    void appendDoesNotWaitForOngoingFlushFsync() throws Exception {
        BlockingRedoIo io = new BlockingRedoIo();
        RedoLogManager mgr = new RedoLogManager(io);
        LogRange first = mgr.append(List.of(new PageBytesRecord(PID, 0, new byte[]{1})));
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<Lsn> flush = executor.submit(mgr::flush);
            assertTrue(io.flushEntered.await(1, TimeUnit.SECONDS), "flush must enter the injected fsync wait");

            Future<LogRange> append = executor.submit(
                    () -> mgr.append(List.of(new PageBytesRecord(PID, 8, new byte[]{2}))));

            LogRange second = append.get(200, TimeUnit.MILLISECONDS);
            assertEquals(first.end(), second.start(), "append can reserve the next LSN while fsync is still blocked");

            io.releaseFlush.countDown();
            assertEquals(first.end(), flush.get(1, TimeUnit.SECONDS));
        } finally {
            io.releaseFlush.countDown();
            executor.shutdownNow();
        }
    }

    /**
     * 验证 {@code emptyBatchDoesNotAdvanceLsn} 所描述的边界场景保持既有领域不变量，不产生方法名明确禁止的副作用。
     */
    @Test
    void emptyBatchDoesNotAdvanceLsn() {
        RedoLogManager mgr = new RedoLogManager();
        LogRange r = mgr.append(List.of());
        assertEquals(r.start(), r.end(), "empty batch -> degenerate range");
        assertEquals(0, mgr.bufferedRecords().size());
    }

    /**
     * 验证 {@code bufferedRecordsIsImmutableSnapshot} 所描述的页内记录行为，并断言偏移、编码边界、隐藏列及 page-directory 结构保持一致。
     */
    @Test
    void bufferedRecordsIsImmutableSnapshot() {
        RedoLogManager mgr = new RedoLogManager();
        mgr.append(List.of(new PageInitRecord(PID, PageType.INDEX)));
        List<RedoRecord> snap = mgr.bufferedRecords();
        assertThrows(UnsupportedOperationException.class, () -> snap.add(new PageInitRecord(PID, PageType.INDEX)));
        assertTrue(snap.get(0) instanceof PageInitRecord);
    }

    /** 恢复完成后新 append 必须从 recoveredTo 继续，不能从 0 覆盖已有日志区间。 */
    @Test
    void restoreRecoveredBoundaryBeforeNewAppend() {
        RedoLogManager mgr = new RedoLogManager();
        mgr.restoreRecoveredBoundary(cn.zhangyis.db.domain.Lsn.of(100));

        LogRange range = mgr.append(List.of(new PageInitRecord(PID, PageType.INDEX)));

        assertEquals(100L, range.start().value());
        assertEquals(100L, mgr.flushedToDiskLsn().value());
        assertThrows(RuntimeException.class,
                () -> mgr.restoreRecoveredBoundary(cn.zhangyis.db.domain.Lsn.of(200)));
    }

    private static final class BlockingRedoIo implements RedoLogIo {
        private final CountDownLatch flushEntered = new CountDownLatch(1);
        private final CountDownLatch releaseFlush = new CountDownLatch(1);
        private final AtomicLong written = new AtomicLong();

        @Override
        public Lsn write(RedoLogBatch batch) {
            written.set(batch.range().end().value());
            return batch.range().end();
        }

        @Override
        public Lsn writtenToDiskLsn() {
            return Lsn.of(written.get());
        }

        @Override
        public Lsn flushTo(Lsn target) {
            flushEntered.countDown();
            try {
                if (!releaseFlush.await(2, TimeUnit.SECONDS)) {
                    throw new AssertionError("test fsync was not released");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("test fsync interrupted", e);
            }
            return target;
        }
    }
}
