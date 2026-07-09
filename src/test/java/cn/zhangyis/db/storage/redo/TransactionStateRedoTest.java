package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.TransactionNo;
import cn.zhangyis.db.storage.fil.io.PageStore;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * 0.19i：事务状态 redo 是 non-page record。它参与 redo 顺序和诊断，但不应伪造成某个数据页修改。
 */
class TransactionStateRedoTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);

    @Test
    void trxStateDeltaRoundTripsWithoutPageId() {
        TransactionStateDeltaRecord delta = new TransactionStateDeltaRecord(
                TransactionId.of(7), TransactionStateDeltaState.ACTIVE,
                TransactionStateDeltaState.COMMITTED, TransactionNo.of(3),
                TransactionStateDeltaReason.COMMIT);
        RedoLogBatch batch = batchOf(List.of(delta));

        ByteBuffer frame = RedoBatchFrameCodec.encodeFrame(batch);
        List<RedoLogBatch> decoded = RedoBatchFrameCodec.decodeFrames(frame);

        assertEquals(1, decoded.size());
        assertEquals(batch.range(), decoded.get(0).range());
        assertEquals(List.of(delta), decoded.get(0).records());
        assertEquals(1 + 8 + 1 + 1 + 8 + 1, delta.byteLength());
    }

    @Test
    void defaultDispatcherAppliesTrxStateDeltaWithoutPageSkipOrPageStoreIo() {
        TransactionStateDeltaRecord delta = new TransactionStateDeltaRecord(
                TransactionId.of(7), TransactionStateDeltaState.ROLLING_BACK,
                TransactionStateDeltaState.ROLLED_BACK, TransactionNo.NONE,
                TransactionStateDeltaReason.ROLLBACK);
        RedoLogBatch batch = batchOf(List.of(delta));
        AtomicBoolean skipCalled = new AtomicBoolean(false);

        RedoApplySummary summary = RedoApplyDispatcher.pageDispatcher()
                .apply(batch, new RedoApplyContext(new FailingPageStore(), PS), pageId -> {
                    skipCalled.set(true);
                    return true;
                });

        assertEquals(1, summary.appliedBatchCount());
        assertEquals(0, summary.skippedRecordCount());
        assertFalse(skipCalled.get(), "non-page trx redo must not invoke page skip predicate");
    }

    private static RedoLogBatch batchOf(List<RedoRecord> records) {
        long end = 100;
        for (RedoRecord record : records) {
            end += record.byteLength();
        }
        return new RedoLogBatch(new LogRange(Lsn.of(100), Lsn.of(end)), records);
    }

    private static final class FailingPageStore implements PageStore {
        @Override
        public void create(SpaceId spaceId, Path path, PageSize pageSize, PageNo initialSizeInPages) {
            throw new AssertionError("page store must not be touched");
        }

        @Override
        public void open(SpaceId spaceId, Path path, PageSize pageSize) {
            throw new AssertionError("page store must not be touched");
        }

        @Override
        public void readPage(PageId pageId, ByteBuffer dst) {
            throw new AssertionError("page store must not be touched");
        }

        @Override
        public void writePage(PageId pageId, ByteBuffer src) {
            throw new AssertionError("page store must not be touched");
        }

        @Override
        public PageNo extend(SpaceId spaceId) {
            throw new AssertionError("page store must not be touched");
        }

        @Override
        public PageNo currentSizeInPages(SpaceId spaceId) {
            throw new AssertionError("page store must not be touched");
        }

        @Override
        public Path pathOf(SpaceId spaceId) {
            throw new AssertionError("page store must not be touched");
        }

        @Override
        public void force(SpaceId spaceId) {
            throw new AssertionError("page store must not be touched");
        }

        @Override
        public void forceAll() {
            throw new AssertionError("page store must not be touched");
        }

        @Override
        public void truncate(SpaceId spaceId, PageNo targetSizeInPages) {
            throw new AssertionError("page store must not be touched");
        }

        @Override
        public void ensureCapacity(SpaceId spaceId, PageNo minSizeInPages) {
            throw new AssertionError("page store must not be touched");
        }

        @Override
        public void close(SpaceId spaceId) {
        }

        @Override
        public void close() {
        }
    }
}
