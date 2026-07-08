package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.fil.io.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.page.PageEnvelopeLayout;
import cn.zhangyis.db.storage.page.PageType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * FORCE_SKIP_CORRUPT_TABLESPACE 的 redo 层回归测试。redo apply 必须在访问 PageStore 前过滤被跳过
 * 表空间的物理记录，同时未跳过页仍使用原始 batch end LSN 盖 pageLSN，保证 MTR 幂等边界不被过滤动作改变。
 */
class RedoApplyDispatcherSkipTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId ACTIVE_SPACE = SpaceId.of(1);
    private static final SpaceId SKIPPED_SPACE = SpaceId.of(9);
    private static final int PAYLOAD_OFFSET = PageEnvelopeLayout.FIL_PAGE_HEADER_BYTES + 32;

    @TempDir
    Path dir;

    /**
     * 一个 batch 同时包含坏表空间和正常表空间时，dispatcher 必须先按 PageId 过滤，再把剩余记录交给
     * page handler；否则未打开的坏表空间会在 read/write/ensureCapacity 上 fail-fast。
     */
    @Test
    void skipsRecordsBeforeTouchingPageStoreAndKeepsOriginalBatchEndLsn() {
        PageId skipped = PageId.of(SKIPPED_SPACE, PageNo.of(5));
        PageId active = PageId.of(ACTIVE_SPACE, PageNo.of(3));
        byte[] payload = new byte[]{4, 5, 6};
        RedoLogBatch batch = batchOf(List.of(
                new PageInitRecord(skipped, PageType.INDEX),
                new PageBytesRecord(skipped, PAYLOAD_OFFSET, new byte[]{1, 2, 3}),
                new PageInitRecord(active, PageType.INDEX),
                new PageBytesRecord(active, PAYLOAD_OFFSET, payload)));

        try (PageStore store = new FileChannelPageStore()) {
            store.create(ACTIVE_SPACE, dir.resolve("active.ibd"), PS, PageNo.of(2));
            RedoApplySummary summary = RedoApplyDispatcher.pageDispatcher()
                    .applyAll(List.of(batch), new RedoApplyContext(store, PS),
                            pageId -> SKIPPED_SPACE.equals(pageId.spaceId()));

            byte[] page = readPage(store, active);
            assertEquals(1, summary.scannedBatchCount());
            assertEquals(1, summary.appliedBatchCount());
            assertEquals(2, summary.skippedRecordCount());
            assertEquals(batch.range().end().value(), ByteBuffer.wrap(page).getLong(PageEnvelopeLayout.PAGE_LSN));
            assertArrayEquals(payload, slice(page, PAYLOAD_OFFSET, payload.length));
        }
    }

    /**
     * batch 内所有记录都属于被跳过表空间时，恢复应只记录 skip 诊断，不允许为了读页或扩容而触碰该表空间句柄。
     */
    @Test
    void allSkippedBatchDoesNotRequireTablespaceHandle() {
        PageId skipped = PageId.of(SKIPPED_SPACE, PageNo.of(8));
        RedoLogBatch batch = batchOf(List.of(new PageInitRecord(skipped, PageType.INDEX)));

        try (PageStore store = new FileChannelPageStore()) {
            RedoApplySummary summary = RedoApplyDispatcher.pageDispatcher()
                    .applyAll(List.of(batch), new RedoApplyContext(store, PS),
                            pageId -> SKIPPED_SPACE.equals(pageId.spaceId()));

            assertEquals(1, summary.scannedBatchCount());
            assertEquals(0, summary.appliedBatchCount());
            assertEquals(1, summary.skippedRecordCount());
        }
    }

    private static RedoLogBatch batchOf(List<RedoRecord> records) {
        long end = 100;
        for (RedoRecord record : records) {
            end += record.byteLength();
        }
        return new RedoLogBatch(new LogRange(Lsn.of(100), Lsn.of(end)), records);
    }

    private static byte[] readPage(PageStore store, PageId pageId) {
        byte[] page = new byte[PS.bytes()];
        store.readPage(pageId, ByteBuffer.wrap(page));
        return page;
    }

    private static byte[] slice(byte[] bytes, int offset, int length) {
        byte[] out = new byte[length];
        System.arraycopy(bytes, offset, out, 0, length);
        return out;
    }
}
