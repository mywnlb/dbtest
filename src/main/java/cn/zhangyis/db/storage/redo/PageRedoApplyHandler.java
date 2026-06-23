package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
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
                if (record instanceof PageInitRecord) {
                    // PAGE_INIT 是唯一的建页记录：崩溃后物理文件可能被截短到该页之前，按需扩容使其可写。
                    ensureReplayCapacity(context, pageId);
                } else if (isBeyondFileEnd(context, pageId)) {
                    // 首触是 PAGE_BYTES 却越过物理文件尾：该页从未经 PAGE_INIT 建立，凭空造页属于 redo 损坏，
                    // 不得静默扩容补一个半成品页。正确的 fuzzy checkpoint 下不会出现该情形（未刷的建页 redo 不会被
                    // checkpoint 越过），故判损坏而非容忍。
                    throw new RedoLogCorruptedException(
                            "redo PAGE_BYTES targets uninitialized page beyond data file size: " + pageId);
                }
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

    /**
     * extend-on-demand（仅 PAGE_INIT 触发）：崩溃后物理文件可能因 autoExtend 未 fsync 而短于 redo 写过的页号。
     * 重放一个建页记录前先把物理文件扩到能容纳该页（幂等），避免随后的 readPage/writePage 越界。
     * SPACE_FILE_RECONCILE 阶段会在 replay 之后再按 page0 权威大小补齐 extent 内无 redo 描述的尾部零页。
     */
    private static void ensureReplayCapacity(RedoApplyContext context, PageId pageId) {
        context.pageStore().ensureCapacity(pageId.spaceId(), PageNo.of(pageId.pageNo().value() + 1));
    }

    /** 目标页号是否越过当前物理文件尾（用于判定首触 PAGE_BYTES 是否指向未建立的页）。 */
    private static boolean isBeyondFileEnd(RedoApplyContext context, PageId pageId) {
        return pageId.pageNo().value() >= context.pageStore().currentSizeInPages(pageId.spaceId()).value();
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
