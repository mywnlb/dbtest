package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SegmentId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.fsp.header.SpaceHeaderLayout;
import cn.zhangyis.db.storage.page.PageEnvelopeLayout;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 0.19c/0.19d：FSP metadata delta 逻辑 redo 的持久格式、page patch replay、force-skip 与物理字节替代边界。
 */
class FspMetadataRedoTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(7);
    private static final PageId PAGE0 = PageId.of(SPACE, PageNo.of(0));

    /**
     * 验证 {@code fspMetadataDeltaAndFreeIntentRoundTripThroughRedoFrameCodec} 所描述的恢复场景能够依据持久证据幂等重建状态，且不会重复产生副作用。
     */
    @Test
    void fspMetadataDeltaAndFreeIntentRoundTripThroughRedoFrameCodec() {
        FspMetadataDeltaRecord delta = new FspMetadataDeltaRecord(PAGE0,
                FspMetadataDeltaKind.SPACE_HEADER_FIELD, 0L, 0,
                SpaceHeaderLayout.CURRENT_SIZE, longBytes(192L));
        FspPageFreeRecord free = new FspPageFreeRecord(
                PageId.of(SPACE, PageNo.of(64)), 2, SegmentId.of(11));
        RedoLogBatch batch = batchOf(List.of(delta, free));

        ByteBuffer frame = RedoBatchFrameCodec.encodeFrame(batch);
        List<RedoLogBatch> decoded = RedoBatchFrameCodec.decodeFrames(frame);

        assertEquals(1, decoded.size());
        assertEquals(batch.range(), decoded.get(0).range());
        assertEquals(List.of(delta, free), decoded.get(0).records());
        assertEquals(1 + 12 + 1 + 8 + 4 + 4 + 4 + 8, delta.byteLength());
        assertEquals(1 + 12 + 4 + 8, free.byteLength());
    }

    /**
     * 验证 {@code metadataDeltaReplaysWithoutPageBytesAndStampsBatchEndLsn} 所描述的恢复场景能够依据持久证据幂等重建状态，且不会重复产生副作用。
     */
    @Test
    void metadataDeltaReplaysWithoutPageBytesAndStampsBatchEndLsn() {
        RecordingPageStore store = new RecordingPageStore();
        store.create(SPACE, Path.of("s.ibd"), PS, PageNo.of(4));
        byte[] after = longBytes(192L);
        RedoLogBatch batch = batchOf(List.of(new FspMetadataDeltaRecord(PAGE0,
                FspMetadataDeltaKind.SPACE_HEADER_FIELD, 0L, 0,
                SpaceHeaderLayout.CURRENT_SIZE, after)));

        RedoApplySummary summary = RedoApplyDispatcher.pageDispatcher()
                .apply(batch, new RedoApplyContext(store, PS));

        byte[] page = store.page(PAGE0);
        assertEquals(1, summary.appliedBatchCount());
        assertArrayEquals(after, slice(page, SpaceHeaderLayout.CURRENT_SIZE, after.length));
        assertEquals(batch.range().end().value(), ByteBuffer.wrap(page).getLong(PageEnvelopeLayout.PAGE_LSN));
        assertEquals(1, store.readCount);
        assertEquals(1, store.writeCount);
    }

    /**
     * 验证 {@code metadataDeltaAndPageBytesShareOnePagePatchWriteBack} 所描述的页内记录行为，并断言偏移、编码边界、隐藏列及 page-directory 结构保持一致。
     */
    @Test
    void metadataDeltaAndPageBytesShareOnePagePatchWriteBack() {
        RecordingPageStore store = new RecordingPageStore();
        store.create(SPACE, Path.of("s.ibd"), PS, PageNo.of(4));
        RedoLogBatch batch = batchOf(List.of(
                new FspMetadataDeltaRecord(PAGE0, FspMetadataDeltaKind.SPACE_HEADER_FIELD,
                        0L, 0, SpaceHeaderLayout.CURRENT_SIZE, longBytes(192L)),
                new PageBytesRecord(PAGE0, SpaceHeaderLayout.FREE_LIMIT, longBytes(128L))));

        RedoApplyDispatcher.pageDispatcher().apply(batch, new RedoApplyContext(store, PS));

        byte[] page = store.page(PAGE0);
        assertArrayEquals(longBytes(192L), slice(page, SpaceHeaderLayout.CURRENT_SIZE, Long.BYTES));
        assertArrayEquals(longBytes(128L), slice(page, SpaceHeaderLayout.FREE_LIMIT, Long.BYTES));
        assertEquals(1, store.readCount);
        assertEquals(1, store.writeCount);
    }

    /**
     * 验证 {@code forceSkipSkipsMetadataDeltaAndFreeIntentBeforeTouchingPageStore} 所描述的页内记录行为，并断言偏移、编码边界、隐藏列及 page-directory 结构保持一致。
     */
    @Test
    void forceSkipSkipsMetadataDeltaAndFreeIntentBeforeTouchingPageStore() {
        RecordingPageStore store = new RecordingPageStore();
        RedoLogBatch batch = batchOf(List.of(
                new FspMetadataDeltaRecord(PAGE0, FspMetadataDeltaKind.SPACE_HEADER_FIELD,
                        0L, 0, SpaceHeaderLayout.CURRENT_SIZE, longBytes(192L)),
                new FspPageFreeRecord(PageId.of(SPACE, PageNo.of(64)), 1, SegmentId.of(3))));

        RedoApplySummary summary = RedoApplyDispatcher.pageDispatcher()
                .apply(batch, new RedoApplyContext(store, PS), pageId -> true);

        assertEquals(1, summary.scannedBatchCount());
        assertEquals(0, summary.appliedBatchCount());
        assertEquals(2, summary.skippedRecordCount());
        assertEquals(0, store.readCount);
        assertEquals(0, store.writeCount);
        assertTrue(store.ensureCapacityCalls.isEmpty());
    }

    /**
     * 验证 {@code metadataDeltaRejectsOutOfPagePatch} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void metadataDeltaRejectsOutOfPagePatch() {
        RecordingPageStore store = new RecordingPageStore();
        store.create(SPACE, Path.of("s.ibd"), PS, PageNo.of(4));
        RedoLogBatch batch = batchOf(List.of(new FspMetadataDeltaRecord(PAGE0,
                FspMetadataDeltaKind.SPACE_HEADER_FIELD, 0L, 0,
                PS.bytes() - 1, new byte[]{1, 2})));

        assertThrows(RedoLogCorruptedException.class, () ->
                RedoApplyDispatcher.pageDispatcher().apply(batch, new RedoApplyContext(store, PS)));
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
        private final List<PageNo> ensureCapacityCalls = new ArrayList<>();
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
            ensureCapacityCalls.add(minSizeInPages);
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
