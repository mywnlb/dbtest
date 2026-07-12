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
 * 0.19h：B+Tree 结构页 delta v1。恢复期只 patch 页内结构字段，不重新执行 split/merge 算法。
 */
class BTreePageDeltaRedoTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(31);
    private static final PageId LEAF = PageId.of(SPACE, PageNo.of(11));

    @Test
    void btreePageDeltaRoundTripsThroughRedoFrameCodec() {
        byte[] links = siblingBytes(10, 12);
        BTreePageDeltaRecord delta = new BTreePageDeltaRecord(
                LEAF, 7L, BTreePageDeltaKind.SIBLING_LINKS, 12L,
                PageEnvelopeLayout.PREV_PAGE_NO, links);
        RedoLogBatch batch = batchOf(List.of(delta));

        ByteBuffer frame = RedoBatchFrameCodec.encodeFrame(batch);
        List<RedoLogBatch> decoded = RedoBatchFrameCodec.decodeFrames(frame);

        assertEquals(1, decoded.size());
        assertEquals(batch.range(), decoded.get(0).range());
        assertEquals(List.of(delta), decoded.get(0).records());
        assertEquals(1 + 12 + 8 + 1 + 8 + 4 + 4 + links.length, delta.byteLength());
    }

    @Test
    void btreePageDeltaReplaysSiblingLinksWithoutPageBytes() {
        RecordingPageStore store = new RecordingPageStore();
        store.create(SPACE, Path.of("idx.ibd"), PS, PageNo.of(64));
        byte[] links = siblingBytes(10, 12);
        RedoLogBatch batch = batchOf(List.of(new BTreePageDeltaRecord(
                LEAF, 7L, BTreePageDeltaKind.SIBLING_LINKS, 12L,
                PageEnvelopeLayout.PREV_PAGE_NO, links)));

        RedoApplySummary summary = RedoApplyDispatcher.pageDispatcher()
                .apply(batch, new RedoApplyContext(store, PS));

        byte[] page = store.page(LEAF);
        assertEquals(1, summary.appliedBatchCount());
        assertArrayEquals(links, slice(page, PageEnvelopeLayout.PREV_PAGE_NO, links.length));
        assertEquals(batch.range().end().value(), ByteBuffer.wrap(page).getLong(PageEnvelopeLayout.PAGE_LSN));
    }

    @Test
    void nodeAndRootDeltaLayoutsRejectMalformedOffsetsOrLengths() {
        assertThrows(cn.zhangyis.db.common.exception.DatabaseValidationException.class,
                () -> new BTreePageDeltaRecord(LEAF, 7L, BTreePageDeltaKind.ROOT_LEVEL_OR_HEADER,
                        2L, 55, new byte[10]));
        assertThrows(cn.zhangyis.db.common.exception.DatabaseValidationException.class,
                () -> new BTreePageDeltaRecord(LEAF, 7L, BTreePageDeltaKind.ROOT_LEVEL_OR_HEADER,
                        2L, 56, new byte[9]));
        assertThrows(cn.zhangyis.db.common.exception.DatabaseValidationException.class,
                () -> new BTreePageDeltaRecord(LEAF, 7L, BTreePageDeltaKind.NODE_POINTER_AREA,
                        2L, 65, new byte[4]));
    }

    @Test
    void nodeAndRootDeltasReplayAsPageLocalAfterImages() {
        RecordingPageStore store = new RecordingPageStore();
        store.create(SPACE, Path.of("idx-node.ibd"), PS, PageNo.of(64));
        byte[] rootIdentity = ByteBuffer.allocate(10).putShort((short) 2).putLong(7L).array();
        byte[] pointerHeap = new byte[]{1, 2, 3, 4, 5, 6};
        RedoLogBatch batch = batchOf(List.of(
                new BTreePageDeltaRecord(LEAF, 7L, BTreePageDeltaKind.ROOT_LEVEL_OR_HEADER,
                        2L, 56, rootIdentity),
                new BTreePageDeltaRecord(LEAF, 7L, BTreePageDeltaKind.NODE_POINTER_AREA,
                        2L, 66, pointerHeap)));

        RedoApplyDispatcher.pageDispatcher().apply(batch, new RedoApplyContext(store, PS));

        assertArrayEquals(rootIdentity, slice(store.page(LEAF), 56, rootIdentity.length));
        assertArrayEquals(pointerHeap, slice(store.page(LEAF), 66, pointerHeap.length));
    }

    @Test
    void nodePointerDeltaCannotPatchFileTrailer() {
        RecordingPageStore store = new RecordingPageStore();
        store.create(SPACE, Path.of("idx-bad-node.ibd"), PS, PageNo.of(64));
        RedoLogBatch batch = batchOf(List.of(new BTreePageDeltaRecord(
                LEAF, 7L, BTreePageDeltaKind.NODE_POINTER_AREA, 1L,
                PS.bytes() - 4, new byte[]{1, 2, 3, 4})));

        assertThrows(RedoLogCorruptedException.class,
                () -> RedoApplyDispatcher.pageDispatcher().apply(batch, new RedoApplyContext(store, PS)));
    }

    private static RedoLogBatch batchOf(List<RedoRecord> records) {
        long end = 100;
        for (RedoRecord record : records) {
            end += record.byteLength();
        }
        return new RedoLogBatch(new LogRange(Lsn.of(100), Lsn.of(end)), records);
    }

    private static byte[] siblingBytes(long prev, long next) {
        return ByteBuffer.allocate(8).putInt((int) prev).putInt((int) next).array();
    }

    private static byte[] slice(byte[] bytes, int offset, int length) {
        byte[] out = new byte[length];
        System.arraycopy(bytes, offset, out, 0, length);
        return out;
    }

    private static final class RecordingPageStore implements PageStore {

        private final Map<SpaceId, PageNo> sizes = new HashMap<>();
        private final Map<PageId, byte[]> pages = new HashMap<>();

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
            dst.put(page(pageId).clone());
        }

        @Override
        public void writePage(PageId pageId, ByteBuffer src) {
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
