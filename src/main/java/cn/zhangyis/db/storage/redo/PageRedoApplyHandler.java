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
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * PAGE_INIT/PAGE_BYTES/FSP metadata delta/undo metadata delta/undo payload/B+Tree page delta 的物理页回放 handler。
 * 它按批次缓存同页修改，
 * 先应用批内所有记录，再把 pageLSN 盖到 batch endLsn。
 *
 * <p>这个顺序很关键：D3/D4 的 MTR commit 语义是“批内所有修改共享 endLsn”。若 PAGE_INIT 后立即盖 pageLSN，
 * 同批后续 PAGE_BYTES 会被 pageLSN 幂等判断误跳过，导致恢复页半成品。
 */
public final class PageRedoApplyHandler implements RedoApplyHandler {

    @Override
    public boolean supports(RedoRecord record) {
        return record instanceof PageInitRecord
                || record instanceof PageBytesRecord
                || record instanceof FspMetadataDeltaRecord
                || record instanceof UndoMetadataDeltaRecord
                || record instanceof UndoRecordPayloadRecord
                || record instanceof BTreePageDeltaRecord;
    }

    @Override
    public List<PageId> affectedPages(RedoRecord record) {
        return List.of(pageIdOf(record));
    }

    @Override
    public RedoApplyBatchHandler openBatch(LogRange range, RedoApplyContext context) {
        if (range == null || context == null) {
            throw new DatabaseValidationException("page redo apply range/context must not be null");
        }
        return new Batch(range, context);
    }

    /** 应用一个完整 redo 批次；每个页只在批次末尾写回一次。 */
    public void apply(RedoLogBatch batch, RedoApplyContext context) {
        if (batch == null || context == null) {
            throw new DatabaseValidationException("page redo apply batch/context must not be null");
        }
        apply(new RedoApplyBatchView(batch.range(), batch.records()), context);
    }

    /**
     * 应用一个 redo apply view；每个页只在批次末尾写回一次。
     *
     * <p>view 可能只包含原 batch 的部分记录（例如 recovery force-skip 坏表空间），但 {@code range.end()}
     * 必须保持原始 batch end LSN。该 LSN 是 MTR 的幂等边界，写低会导致下一次 recovery 重放已处理记录。
     *
     * @param batch 原始 range 加过滤后记录的内部视图。
     * @param context 页重放上下文。
     */
    void apply(RedoApplyBatchView batch, RedoApplyContext context) {
        if (batch == null || context == null) {
            throw new DatabaseValidationException("page redo apply batch/context must not be null");
        }
        RedoApplyBatchHandler session = openBatch(batch.range(), context);
        for (RedoRecord record : batch.records()) {
            session.apply(record);
        }
        session.finish();
    }

    private static final class Batch implements RedoApplyBatchHandler {

        /** 原始 batch range，finish 时作为所有 touched 页的 pageLSN 幂等边界。 */
        private final LogRange range;

        /** recovery apply 上下文；page handler 只通过 PageStore 读写物理页，不依赖 BufferPool/MTR。 */
        private final RedoApplyContext context;

        /** 本 batch 已装入并可能修改的页，保持首次触达顺序，finish 时逐页写回。 */
        private final Map<PageId, ReplayPage> pages = new LinkedHashMap<>();

        /** pageLSN 已覆盖本 batch 的页；同 batch 后续同页 record 必须整体跳过。 */
        private final Set<PageId> skippedPages = new HashSet<>();

        private Batch(LogRange range, RedoApplyContext context) {
            this.range = range;
            this.context = context;
        }

        @Override
        public void apply(RedoRecord record) {
            PageId pageId = pageIdOf(record);
            if (skippedPages.contains(pageId)) {
                return;
            }
            ReplayPage page = pages.get(pageId);
            if (page == null) {
                if (record instanceof PageInitRecord) {
                    // PAGE_INIT 是唯一的建页记录：崩溃后物理文件可能被截短到该页之前，按需扩容使其可写。
                    ensureReplayCapacity(context, pageId);
                } else if (isBeyondFileEnd(context, pageId)) {
                    // 首触是页 patch 却越过物理文件尾：该页从未经 PAGE_INIT 建立，凭空造页属于 redo 损坏，
                    // 不得静默扩容补一个半成品页。正确的 fuzzy checkpoint 下不会出现该情形（未刷的建页 redo 不会被
                    // checkpoint 越过），故判损坏而非容忍。
                    throw new RedoLogCorruptedException(
                            "redo page patch targets uninitialized page beyond data file size: " + pageId);
                }
                byte[] current = readPage(context, pageId);
                if (pageLsn(current).value() >= range.end().value()) {
                    skippedPages.add(pageId);
                    return;
                }
                page = new ReplayPage(pageId, current);
                pages.put(pageId, page);
            }
            if (record instanceof PageInitRecord pir) {
                page.bytes = initializedPage(context, pir);
                page.touched = true;
            } else if (record instanceof PageBytesRecord pbr) {
                applyBytes(context, page, pbr);
            } else if (record instanceof FspMetadataDeltaRecord fmd) {
                applyMetadataDelta(context, page, fmd);
            } else if (record instanceof UndoMetadataDeltaRecord umd) {
                applyUndoMetadataDelta(context, page, umd);
            } else if (record instanceof UndoRecordPayloadRecord urp) {
                applyUndoRecordPayload(context, page, urp);
            } else if (record instanceof BTreePageDeltaRecord btd) {
                applyBTreePageDelta(context, page, btd);
            } else {
                throw new RedoLogCorruptedException("unsupported redo record type: " + record.getClass().getName());
            }
        }

        @Override
        public void finish() {
            for (ReplayPage page : pages.values()) {
                if (page.touched) {
                    stampPageLsn(page.bytes, range.end());
                    context.pageStore().writePage(page.pageId, ByteBuffer.wrap(page.bytes));
                }
            }
        }
    }

    private static void applyBytes(RedoApplyContext context, ReplayPage page, PageBytesRecord record) {
        applyPatch(context, page, record.offset(), record.bytes(), "redo PAGE_BYTES");
    }

    /**
     * FSP metadata delta 与 PAGE_BYTES 共用同一个 batch page cache。这样同一 MTR 内 metadata delta 和仍需物理
     * 保护的 PAGE_BYTES 指向同一 page0/page2 时，恢复只会读一次、批末写一次，避免两个 handler 各自缓存并覆盖彼此修改。
     */
    private static void applyMetadataDelta(RedoApplyContext context, ReplayPage page, FspMetadataDeltaRecord record) {
        applyPatch(context, page, record.offset(), record.afterImage(), "redo FSP metadata delta");
    }

    /**
     * Undo/rseg metadata delta 同样只做页内 after-image patch。恢复期不能重新执行事务提交、slot release 或 undo append
     * 状态机，否则会把 crash recovery 变成普通运行时逻辑并破坏幂等边界。
     */
    private static void applyUndoMetadataDelta(RedoApplyContext context, ReplayPage page,
                                               UndoMetadataDeltaRecord record) {
        applyPatch(context, page, record.offset(), record.afterImage(), "redo undo metadata delta");
    }

    /**
     * 完整 undo record payload 只做页内槽 after-image patch。恢复期不能重新执行 appendRecord，否则会重复推进
     * undo page header 和 first-page log header。
     */
    private static void applyUndoRecordPayload(RedoApplyContext context, ReplayPage page,
                                               UndoRecordPayloadRecord record) {
        applyPatch(context, page, record.recordOffset(), record.slotImage(), "redo undo record payload");
    }

    /**
     * B+Tree 结构 delta 同样只做页内 after-image patch。恢复期不重新运行 split/merge/root shrink 算法，
     * 避免把 redo replay 变成普通运行时结构调整。
     */
    private static void applyBTreePageDelta(RedoApplyContext context, ReplayPage page,
                                            BTreePageDeltaRecord record) {
        applyPatch(context, page, record.offset(), record.afterImage(), "redo B+Tree page delta");
    }

    private static void applyPatch(RedoApplyContext context, ReplayPage page, int offset, byte[] patch, String what) {
        long end = (long) offset + patch.length;
        if (offset < 0 || end > context.pageSize().bytes()) {
            throw new RedoLogCorruptedException(what + " out of page bounds: offset="
                    + offset + " length=" + patch.length + " pageSize=" + context.pageSize().bytes());
        }
        System.arraycopy(patch, 0, page.bytes, offset, patch.length);
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

    static PageId pageIdOf(RedoRecord record) {
        if (record instanceof PageInitRecord pir) {
            return pir.pageId();
        }
        if (record instanceof PageBytesRecord pbr) {
            return pbr.pageId();
        }
        if (record instanceof FspMetadataDeltaRecord fmd) {
            return fmd.pageId();
        }
        if (record instanceof UndoMetadataDeltaRecord umd) {
            return umd.pageId();
        }
        if (record instanceof UndoRecordPayloadRecord urp) {
            return urp.pageId();
        }
        if (record instanceof BTreePageDeltaRecord btd) {
            return btd.pageId();
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
