package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

/**
 * Redo apply 分发器（R1 简化版）。当前只注册 page 物理 handler，负责 PAGE_INIT/PAGE_BYTES。
 */
public final class RedoApplyDispatcher {

    /** PAGE_INIT/PAGE_BYTES 物理页 handler。 */
    private final PageRedoApplyHandler pageHandler;

    private RedoApplyDispatcher(PageRedoApplyHandler pageHandler) {
        this.pageHandler = pageHandler;
    }

    /** 创建只包含 page handler 的恢复分发器。后续逻辑 redo handler 可在此扩展注册表。 */
    public static RedoApplyDispatcher pageDispatcher() {
        return new RedoApplyDispatcher(new PageRedoApplyHandler());
    }

    /**
     * 应用一个 redo 批次。返回摘要是为了让 crash recovery 在最终报告中暴露诊断信息；
     * 既有调用方可以忽略返回值，默认路径不做任何 skip 过滤。
     *
     * @param batch 原始 MTR redo batch。
     * @param context 页重放上下文。
     * @return 本批次的扫描/应用摘要。
     */
    public RedoApplySummary apply(RedoLogBatch batch, RedoApplyContext context) {
        return apply(batch, context, pageId -> false);
    }

    /**
     * 应用一个 redo 批次，并在触碰 PageStore 前过滤不允许访问的 PageId。
     *
     * <p>过滤后仍保留原始 batch range：page handler 必须用原始 end LSN 盖 pageLSN，不能把过滤后的
     * 子集当成新的 redo batch，否则恢复幂等判断会低估已重放边界。
     *
     * @param batch 原始 MTR redo batch。
     * @param context 页重放上下文。
     * @param shouldSkipPage 返回 true 的页不会被 read/write/ensureCapacity/force。
     * @return 本批次的扫描/应用/跳过记录摘要。
     */
    public RedoApplySummary apply(RedoLogBatch batch, RedoApplyContext context, Predicate<PageId> shouldSkipPage) {
        if (batch == null || context == null) {
            throw new DatabaseValidationException("redo apply batch/context must not be null");
        }
        if (shouldSkipPage == null) {
            throw new DatabaseValidationException("redo apply skip predicate must not be null");
        }
        RedoApplyBatchView view = filter(batch, shouldSkipPage);
        int skipped = batch.records().size() - view.records().size();
        if (view.isEmpty()) {
            return new RedoApplySummary(1, 0, skipped);
        }
        pageHandler.apply(view, context);
        return new RedoApplySummary(1, 1, skipped);
    }

    /**
     * 按文件顺序应用多个 redo 批次。默认路径不跳过任何表空间。
     *
     * @param batches 从 redo 文件顺序读出的 batch 列表。
     * @param context 页重放上下文。
     * @return redo apply 摘要。
     */
    public RedoApplySummary applyAll(List<RedoLogBatch> batches, RedoApplyContext context) {
        return applyAll(batches, context, pageId -> false);
    }

    /**
     * 按文件顺序应用多个 redo 批次，并在每个 batch 内执行页级 skip 过滤。
     *
     * @param batches 从 redo 文件顺序读出的 batch 列表。
     * @param context 页重放上下文。
     * @param shouldSkipPage 返回 true 的页不会被 read/write/ensureCapacity/force。
     * @return redo apply 摘要。
     */
    public RedoApplySummary applyAll(List<RedoLogBatch> batches, RedoApplyContext context,
                                     Predicate<PageId> shouldSkipPage) {
        if (batches == null || context == null) {
            throw new DatabaseValidationException("redo apply batches/context must not be null");
        }
        if (shouldSkipPage == null) {
            throw new DatabaseValidationException("redo apply skip predicate must not be null");
        }
        int scanned = 0;
        int applied = 0;
        int skipped = 0;
        for (RedoLogBatch batch : batches) {
            RedoApplySummary summary = apply(batch, context, shouldSkipPage);
            scanned += summary.scannedBatchCount();
            applied += summary.appliedBatchCount();
            skipped += summary.skippedRecordCount();
        }
        return new RedoApplySummary(scanned, applied, skipped);
    }

    private static RedoApplyBatchView filter(RedoLogBatch batch, Predicate<PageId> shouldSkipPage) {
        List<RedoRecord> records = new ArrayList<>(batch.records().size());
        for (RedoRecord record : batch.records()) {
            PageId pageId = PageRedoApplyHandler.pageIdOf(record);
            if (!shouldSkipPage.test(pageId)) {
                records.add(record);
            }
        }
        return new RedoApplyBatchView(batch.range(), records);
    }
}
