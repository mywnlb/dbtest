package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
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
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 0.19e：undo/rseg metadata delta 逻辑 redo 的持久格式与无 PAGE_BYTES 合成重放。
 */
class UndoMetadataRedoTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(9);
    private static final PageId RSEG_PAGE = PageId.of(SPACE, PageNo.of(3));
    /** rseg page3 v3 slot array 起点：固定头 + cache count + history base/high-water。 */
    private static final int RSEG_SLOT_0 = 98;
    /** undo first-page v3 中 logical pair 起点；history links 紧随其后。 */
    private static final int LOGICAL_HEAD_OFFSET = 105;

    @Test
    void undoMetadataDeltaRoundTripsThroughRedoFrameCodec() {
        UndoMetadataDeltaRecord delta = new UndoMetadataDeltaRecord(RSEG_PAGE,
                UndoMetadataDeltaKind.RSEG_SLOT, 0L, 2,
                RSEG_SLOT_0 + 2 * Long.BYTES, longBytes(64L));
        RedoLogBatch batch = batchOf(List.of(delta));

        ByteBuffer frame = RedoBatchFrameCodec.encodeFrame(batch);
        List<RedoLogBatch> decoded = RedoBatchFrameCodec.decodeFrames(frame);

        assertEquals(1, decoded.size());
        assertEquals(batch.range(), decoded.get(0).range());
        assertEquals(List.of(delta), decoded.get(0).records());
        assertEquals(1 + 12 + 1 + 8 + 4 + 4 + 4 + Long.BYTES, delta.byteLength());
    }

    @Test
    void persistentHistoryRedoKindCodesAreAppendOnlyAndRoundTrip() {
        assertEquals(8, UndoMetadataDeltaKind.RSEG_HISTORY_BASE.code());
        assertEquals(9, UndoMetadataDeltaKind.UNDO_HISTORY_LINK_FIELD.code());
        assertEquals(UndoMetadataDeltaKind.RSEG_HISTORY_BASE,
                UndoMetadataDeltaKind.fromCode((byte) 8));
        assertEquals(UndoMetadataDeltaKind.UNDO_HISTORY_LINK_FIELD,
                UndoMetadataDeltaKind.fromCode((byte) 9));
        assertThrows(RedoLogCorruptedException.class,
                () -> UndoMetadataDeltaKind.fromCode((byte) 10));
    }

    @Test
    void undoMetadataDeltaReplaysWithoutPageBytesAndStampsBatchEndLsn() {
        RecordingPageStore store = new RecordingPageStore();
        store.create(SPACE, Path.of("undo.ibu"), PS, PageNo.of(4));
        byte[] after = longBytes(64L);
        RedoLogBatch batch = batchOf(List.of(new UndoMetadataDeltaRecord(RSEG_PAGE,
                UndoMetadataDeltaKind.RSEG_SLOT, 0L, 2, RSEG_SLOT_0 + 2 * Long.BYTES, after)));

        RedoApplySummary summary = RedoApplyDispatcher.pageDispatcher()
                .apply(batch, new RedoApplyContext(store, PS));

        byte[] page = store.page(RSEG_PAGE);
        assertEquals(1, summary.appliedBatchCount());
        assertArrayEquals(after, slice(page, RSEG_SLOT_0 + 2 * Long.BYTES, after.length));
        assertEquals(batch.range().end().value(), ByteBuffer.wrap(page).getLong(PageEnvelopeLayout.PAGE_LSN));
        assertEquals(1, store.readCount);
        assertEquals(1, store.writeCount);
    }

    @Test
    void historyBaseDeltaReplayIsIdempotentByPageLsn() {
        RecordingPageStore store = new RecordingPageStore();
        store.create(SPACE, Path.of("undo-history.ibu"), PS, PageNo.of(4));
        byte[] after = longBytes(64L);
        RedoLogBatch batch = batchOf(List.of(new UndoMetadataDeltaRecord(RSEG_PAGE,
                UndoMetadataDeltaKind.RSEG_HISTORY_BASE, 0L, 0, 66, after)));

        RedoApplyDispatcher dispatcher = RedoApplyDispatcher.pageDispatcher();
        dispatcher.apply(batch, new RedoApplyContext(store, PS));
        dispatcher.apply(batch, new RedoApplyContext(store, PS));

        assertArrayEquals(after, slice(store.page(RSEG_PAGE), 66, Long.BYTES));
        assertEquals(1, store.writeCount, "repeated recovery must skip the already stamped history base delta");
    }

    /** 15B logical head after-image 必须能作为一个 redo record 编码、重放，并以 batch end LSN 幂等跳过。 */
    @Test
    void persistentLogicalHeadPairReplaysAtomicallyAndIdempotently() {
        RecordingPageStore store = new RecordingPageStore();
        store.create(SPACE, Path.of("undo-head.ibu"), PS, PageNo.of(4));
        byte[] pair = ByteBuffer.allocate(Long.BYTES + 7)
                .putLong(9L)
                .put(new byte[]{(byte) 0x80, 0, 0, 0, 7, 0, (byte) 136})
                .array();
        UndoMetadataDeltaRecord delta = new UndoMetadataDeltaRecord(RSEG_PAGE,
                UndoMetadataDeltaKind.UNDO_LOG_HEADER_FIELD, 33L, 2,
                LOGICAL_HEAD_OFFSET, pair);
        RedoLogBatch batch = batchOf(List.of(delta));

        RedoApplyDispatcher dispatcher = RedoApplyDispatcher.pageDispatcher();
        RedoApplySummary first = dispatcher.apply(batch, new RedoApplyContext(store, PS));
        RedoApplySummary repeated = dispatcher.apply(batch, new RedoApplyContext(store, PS));

        byte[] page = store.page(RSEG_PAGE);
        assertEquals(1, first.appliedBatchCount());
        assertEquals(1, repeated.appliedBatchCount(),
                "dispatcher counts a dispatched batch even when the page handler skips by pageLSN");
        assertArrayEquals(pair, slice(page, LOGICAL_HEAD_OFFSET, pair.length));
        assertEquals(batch.range().end().value(), ByteBuffer.wrap(page).getLong(PageEnvelopeLayout.PAGE_LSN));
        assertEquals(1, store.writeCount, "pageLSN must suppress the repeated physical page write");

        RedoLogBatch decoded = RedoBatchFrameCodec.decodeFrames(RedoBatchFrameCodec.encodeFrame(batch)).getFirst();
        assertEquals(List.of(delta), decoded.records(), "15B pair must survive persistent redo frame round-trip");
    }

    private static RedoLogBatch batchOf(List<RedoRecord> records) {
        long end = 100;
        for (RedoRecord record : records) {
            end += record.byteLength();
        }
        return new RedoLogBatch(new LogRange(Lsn.of(100), Lsn.of(end)), records);
    }

    private static byte[] longBytes(long value) {
        return ByteBuffer.allocate(Long.BYTES).putLong(value).array();
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
