package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.UndoNo;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.page.PageEnvelopeLayout;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 0.19g：完整 undo record payload 的逻辑 redo。该 record 只表达页内 record 槽 after-image，
 * undo page header / log header 仍由 UndoMetadataDeltaRecord 负责。
 */
class UndoPayloadRedoTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(9);
    private static final PageId UNDO_PAGE = PageId.of(SPACE, PageNo.of(64));

    /**
     * 验证 {@code undoRecordPayloadRoundTripsThroughRedoFrameCodec} 所描述的恢复场景能够依据持久证据幂等重建状态，且不会重复产生副作用。
     */
    @Test
    void undoRecordPayloadRoundTripsThroughRedoFrameCodec() {
        byte[] slotImage = new byte[]{0, 3, 9, 8, 7};
        UndoRecordPayloadRecord record = new UndoRecordPayloadRecord(
                UNDO_PAGE, TransactionId.of(11), UndoNo.of(4), 128, slotImage);
        RedoLogBatch batch = batchOf(List.of(record));

        ByteBuffer frame = RedoBatchFrameCodec.encodeFrame(batch);
        List<RedoLogBatch> decoded = RedoBatchFrameCodec.decodeFrames(frame);

        assertEquals(1, decoded.size());
        assertEquals(batch.range(), decoded.get(0).range());
        assertEquals(List.of(record), decoded.get(0).records());
        assertEquals(1 + 12 + 8 + 8 + 4 + 4 + slotImage.length, record.byteLength());
    }

    /**
     * 验证 {@code undoRecordPayloadReplaysWithoutPageBytesAndStampsBatchEndLsn} 所描述的恢复场景能够依据持久证据幂等重建状态，且不会重复产生副作用。
     */
    @Test
    void undoRecordPayloadReplaysWithoutPageBytesAndStampsBatchEndLsn() {
        RecordingPageStore store = new RecordingPageStore();
        store.create(SPACE, Path.of("undo.ibu"), PS, PageNo.of(80));
        byte[] slotImage = new byte[]{0, 3, 9, 8, 7};
        RedoLogBatch batch = batchOf(List.of(new UndoRecordPayloadRecord(
                UNDO_PAGE, TransactionId.of(11), UndoNo.of(4), 128, slotImage)));

        RedoApplySummary summary = RedoApplyDispatcher.pageDispatcher()
                .apply(batch, new RedoApplyContext(store, PS));

        byte[] page = store.page(UNDO_PAGE);
        assertEquals(1, summary.appliedBatchCount());
        assertArrayEquals(slotImage, slice(page, 128, slotImage.length));
        assertEquals(batch.range().end().value(), ByteBuffer.wrap(page).getLong(PageEnvelopeLayout.PAGE_LSN));
        assertEquals(1, store.readCount);
        assertEquals(1, store.writeCount);
    }

    private static RedoLogBatch batchOf(List<RedoRecord> records) {
        long end = 100;
        for (RedoRecord record : records) {
            end += record.byteLength();
        }
        return new RedoLogBatch(new LogRange(Lsn.of(100), Lsn.of(end)), records);
    }

    private static byte[] slice(byte[] bytes, int offset, int length) {
        byte[] out = new byte[length];
        System.arraycopy(bytes, offset, out, 0, length);
        return out;
    }

    private static final class RecordingPageStore implements PageStore {

        private final Map<SpaceId, PageNo> sizes = new HashMap<>();
        private final Map<PageId, byte[]> pages = new HashMap<>();
        private int readCount;
        private int writeCount;

        @Override
        public void create(SpaceId spaceId, Path path, PageSize pageSize, PageNo initialSizeInPages) {
            sizes.put(spaceId, initialSizeInPages);
            for (long pageNo = 0; pageNo < initialSizeInPages.value(); pageNo++) {
                pages.put(PageId.of(spaceId, PageNo.of(pageNo)), new byte[pageSize.bytes()]);
            }
        }

        @Override
        public void open(SpaceId spaceId, Path path, PageSize pageSize) {
            throw new AssertionError("open must not be called");
        }

        @Override
        public void readPage(PageId pageId, ByteBuffer dst) {
            readCount++;
            dst.put(page(pageId).clone());
        }

        @Override
        public void writePage(PageId pageId, ByteBuffer src) {
            writeCount++;
            byte[] bytes = new byte[src.remaining()];
            src.get(bytes);
            pages.put(pageId, bytes);
        }

        @Override
        public PageNo extend(SpaceId spaceId) {
            throw new AssertionError("extend must not be called");
        }

        @Override
        public PageNo currentSizeInPages(SpaceId spaceId) {
            return sizes.getOrDefault(spaceId, PageNo.of(0));
        }

        @Override
        public Path pathOf(SpaceId spaceId) {
            return Path.of("unused");
        }

        @Override
        public void force(SpaceId spaceId) {
        }

        @Override
        public void forceAll() {
        }

        @Override
        public void truncate(SpaceId spaceId, PageNo targetSizeInPages) {
            sizes.put(spaceId, targetSizeInPages);
        }

        @Override
        public void ensureCapacity(SpaceId spaceId, PageNo minSizeInPages) {
            sizes.put(spaceId, minSizeInPages);
        }

        @Override
        public void close(SpaceId spaceId) {
        }

        @Override
        public void close() {
        }

        private byte[] page(PageId pageId) {
            byte[] bytes = pages.get(pageId);
            if (bytes == null) {
                throw new AssertionError("page not found: " + pageId);
            }
            return bytes;
        }
    }
}
