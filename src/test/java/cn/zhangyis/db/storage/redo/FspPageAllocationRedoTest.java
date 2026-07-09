package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SegmentId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.fil.io.PageStore;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 0.19b：FSP page allocation 逻辑 redo 的持久格式与恢复 handler。
 */
class FspPageAllocationRedoTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(7);
    private static final PageId ALLOCATED = PageId.of(SPACE, PageNo.of(130));

    @Test
    void fspPageAllocationRecordRoundTripsThroughRedoFrameCodec() {
        FspPageAllocationRecord record = new FspPageAllocationRecord(
                ALLOCATED, 3, SegmentId.of(11), true);
        RedoLogBatch batch = batchOf(List.of(record));

        ByteBuffer frame = RedoBatchFrameCodec.encodeFrame(batch);
        List<RedoLogBatch> decoded = RedoBatchFrameCodec.decodeFrames(frame);

        assertEquals(1, decoded.size());
        assertEquals(batch.range(), decoded.get(0).range());
        assertEquals(List.of(record), decoded.get(0).records());
        assertEquals(26, record.byteLength(),
                "tag + pageId + inodeSlot + segmentId + autoExtendRetry must stay aligned with codec");
    }

    @Test
    void fspHandlerEnsuresCapacityWithoutReadingOrWritingPageBytes() {
        RecordingPageStore store = new RecordingPageStore();
        RedoLogBatch batch = batchOf(List.of(new FspPageAllocationRecord(
                ALLOCATED, 2, SegmentId.of(5), false)));

        RedoApplySummary summary = RedoApplyDispatcher.pageDispatcher()
                .apply(batch, new RedoApplyContext(store, PS));

        assertEquals(1, summary.scannedBatchCount());
        assertEquals(1, summary.appliedBatchCount());
        assertEquals(List.of(PageNo.of(131)), store.ensureCapacityCalls);
        assertEquals(0, store.readCount);
        assertEquals(0, store.writeCount);
    }

    @Test
    void forceSkipSkipsFspAllocationBeforeOpeningHandler() {
        RecordingPageStore store = new RecordingPageStore();
        RedoLogBatch batch = batchOf(List.of(new FspPageAllocationRecord(
                ALLOCATED, 2, SegmentId.of(5), false)));

        RedoApplySummary summary = RedoApplyDispatcher.pageDispatcher()
                .apply(batch, new RedoApplyContext(store, PS), pageId -> true);

        assertEquals(1, summary.scannedBatchCount());
        assertEquals(0, summary.appliedBatchCount());
        assertEquals(1, summary.skippedRecordCount());
        assertTrue(store.ensureCapacityCalls.isEmpty());
    }

    private static RedoLogBatch batchOf(List<RedoRecord> records) {
        long end = 100;
        for (RedoRecord record : records) {
            end += record.byteLength();
        }
        return new RedoLogBatch(new LogRange(Lsn.of(100), Lsn.of(end)), records);
    }

    private static final class RecordingPageStore implements PageStore {

        private final List<PageNo> ensureCapacityCalls = new ArrayList<>();
        private int readCount;
        private int writeCount;

        @Override
        public void create(SpaceId spaceId, Path path, PageSize pageSize, PageNo initialSizeInPages) {
            throw new AssertionError("create must not be called by FSP redo handler");
        }

        @Override
        public void open(SpaceId spaceId, Path path, PageSize pageSize) {
            throw new AssertionError("open must not be called by FSP redo handler");
        }

        @Override
        public void readPage(PageId pageId, ByteBuffer dst) {
            readCount++;
            throw new AssertionError("FSP allocation handler must not read page bytes");
        }

        @Override
        public void writePage(PageId pageId, ByteBuffer src) {
            writeCount++;
            throw new AssertionError("FSP allocation handler must not write page bytes");
        }

        @Override
        public PageNo extend(SpaceId spaceId) {
            throw new AssertionError("FSP redo handler must not run foreground autoextend");
        }

        @Override
        public PageNo currentSizeInPages(SpaceId spaceId) {
            return PageNo.of(0);
        }

        @Override
        public Path pathOf(SpaceId spaceId) {
            throw new AssertionError("pathOf must not be called by FSP redo handler");
        }

        @Override
        public void force(SpaceId spaceId) {
            throw new AssertionError("force must not be called by FSP redo handler");
        }

        @Override
        public void forceAll() {
            throw new AssertionError("forceAll must not be called by FSP redo handler");
        }

        @Override
        public void truncate(SpaceId spaceId, PageNo targetSizeInPages) {
            throw new AssertionError("truncate must not be called by FSP redo handler");
        }

        @Override
        public void ensureCapacity(SpaceId spaceId, PageNo minSizeInPages) {
            assertEquals(SPACE, spaceId);
            ensureCapacityCalls.add(minSizeInPages);
        }

        @Override
        public void close(SpaceId spaceId) {
            throw new AssertionError("close(space) must not be called by FSP redo handler");
        }

        @Override
        public void close() {
            // 测试 fake 无资源。
        }
    }
}
