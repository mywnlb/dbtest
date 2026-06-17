package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.storage.page.FilePageHeader;
import cn.zhangyis.db.storage.page.PageEnvelopeLayout;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * PAGE_INIT/PAGE_BYTES 的物理页回放 handler。它按批次缓存同页修改，先应用批内所有记录，再把 pageLSN 盖到 batch endLsn。
 *
 * <p>这个顺序很关键：D3/D4 的 MTR commit 语义是“批内所有修改共享 endLsn”。若 PAGE_INIT 后立即盖 pageLSN，
 * 同批后续 PAGE_BYTES 会被 pageLSN 幂等判断误跳过，导致恢复页半成品。
 */
public final class PageRedoApplyHandler {

    /** 应用一个批次；每个页只在批次末尾写回一次。 */
    public void apply(RedoLogBatch batch, RedoApplyContext context) {
        if (batch == null || context == null) {
            throw new DatabaseValidationException("page redo apply batch/context must not be null");
        }
        Map<PageId, ReplayPage> pages = new LinkedHashMap<>();
        Set<PageId> skippedPages = new HashSet<>();
        for (RedoRecord record : batch.records()) {
            PageId pageId = pageIdOf(record);
            if (skippedPages.contains(pageId)) {
                continue;
            }
            ReplayPage page = pages.get(pageId);
            if (page == null) {
                byte[] current = readPage(context, pageId);
                if (pageLsn(current).value() >= batch.range().end().value()) {
                    skippedPages.add(pageId);
                    continue;
                }
                page = new ReplayPage(pageId, current);
                pages.put(pageId, page);
            }
            if (record instanceof PageInitRecord pir) {
                page.bytes = initializedPage(context, pir);
                page.touched = true;
            } else if (record instanceof PageBytesRecord pbr) {
                applyBytes(context, page, pbr);
            } else {
                throw new RedoLogCorruptedException("unsupported redo record type: " + record.getClass().getName());
            }
        }
        for (ReplayPage page : pages.values()) {
            if (page.touched) {
                stampPageLsn(page.bytes, batch.range().end());
                context.pageStore().writePage(page.pageId, ByteBuffer.wrap(page.bytes));
            }
        }
    }

    private static void applyBytes(RedoApplyContext context, ReplayPage page, PageBytesRecord record) {
        byte[] bytes = record.bytes();
        long end = (long) record.offset() + bytes.length;
        if (record.offset() < 0 || end > context.pageSize().bytes()) {
            throw new RedoLogCorruptedException("redo PAGE_BYTES out of page bounds: offset="
                    + record.offset() + " length=" + bytes.length + " pageSize=" + context.pageSize().bytes());
        }
        System.arraycopy(bytes, 0, page.bytes, record.offset(), bytes.length);
        page.touched = true;
    }

    private static byte[] readPage(RedoApplyContext context, PageId pageId) {
        byte[] bytes = new byte[context.pageSize().bytes()];
        context.pageStore().readPage(pageId, ByteBuffer.wrap(bytes));
        return bytes;
    }

    private static byte[] initializedPage(RedoApplyContext context, PageInitRecord record) {
        byte[] bytes = new byte[context.pageSize().bytes()];
        ByteBuffer page = ByteBuffer.wrap(bytes);
        page.putInt(PageEnvelopeLayout.SPACE_ID, record.pageId().spaceId().value());
        page.putInt(PageEnvelopeLayout.PAGE_NO, (int) record.pageId().pageNo().value());
        page.putInt(PageEnvelopeLayout.PREV_PAGE_NO, (int) FilePageHeader.FIL_NULL);
        page.putInt(PageEnvelopeLayout.NEXT_PAGE_NO, (int) FilePageHeader.FIL_NULL);
        page.putLong(PageEnvelopeLayout.PAGE_LSN, 0L);
        page.putInt(PageEnvelopeLayout.PAGE_TYPE, record.pageType().code());
        return bytes;
    }

    private static Lsn pageLsn(byte[] page) {
        return Lsn.of(ByteBuffer.wrap(page).getLong(PageEnvelopeLayout.PAGE_LSN));
    }

    private static void stampPageLsn(byte[] page, Lsn lsn) {
        ByteBuffer.wrap(page).putLong(PageEnvelopeLayout.PAGE_LSN, lsn.value());
    }

    private static PageId pageIdOf(RedoRecord record) {
        if (record instanceof PageInitRecord pir) {
            return pir.pageId();
        }
        if (record instanceof PageBytesRecord pbr) {
            return pbr.pageId();
        }
        throw new RedoLogCorruptedException("unsupported redo record type: " + record.getClass().getName());
    }

    private static final class ReplayPage {
        private final PageId pageId;
        private byte[] bytes;
        private boolean touched;

        private ReplayPage(PageId pageId, byte[] bytes) {
            this.pageId = pageId;
            this.bytes = bytes;
        }
    }
}
