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

/**
 * 0.19e：undo/rseg metadata delta 逻辑 redo 的持久格式与无 PAGE_BYTES 合成重放。
 */
class UndoMetadataRedoTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(9);
    private static final PageId RSEG_PAGE = PageId.of(SPACE, PageNo.of(3));
    /** rseg page3 slot array 起点：FIL header(38) + magic/format/rsegId/slotCapacity(16)。 */
    private static final int RSEG_SLOT_0 = PageEnvelopeLayout.FIL_PAGE_HEADER_BYTES + 16;

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
