package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/**
 * Redo apply 分发器。0.19a 起支持多 handler registry，但默认生产入口仍只注册 PAGE_INIT/PAGE_BYTES
 * 物理页 handler；新增持久逻辑 redo 类型时只需注册新的 {@link RedoApplyHandler}。
 *
 * <p>分发器必须保持 batch 内 record 的原始顺序：page handler 依赖批末统一 pageLSN，未来 FSP/BTree
 * handler 也可能依赖同一 MTR 内的先后关系。因此这里按 record 顺序解析 handler 并调用 batch session，
 * 只在 batch 末尾调用各 handler 的 {@link RedoApplyBatchHandler#finish()}。
 */
public final class RedoApplyDispatcher {

    /** 已注册 handler，顺序用于诊断与批末 finish 的首次出现顺序；构造后不可变。 */
    private final List<RedoApplyHandler> handlers;

    private RedoApplyDispatcher(List<RedoApplyHandler> handlers) {
        if (handlers == null) {
            throw new DatabaseValidationException("redo apply handlers must not be null");
        }
        for (RedoApplyHandler handler : handlers) {
            if (handler == null) {
                throw new DatabaseValidationException("redo apply handler must not be null");
            }
        }
        this.handlers = List.copyOf(handlers);
    }

    /** 创建只包含 page handler 的恢复分发器。 */
    public static RedoApplyDispatcher pageDispatcher() {
        return withHandlers(List.of(new PageRedoApplyHandler()));
    }

    /**
     * 创建自定义 handler registry 的恢复分发器。每个 record 在 apply 时必须恰好匹配一个 handler。
     *
     * @param handlers handler 列表；允许为空，但实际 apply 会因没有匹配 handler 而失败。
     * @return dispatcher。
     */
    public static RedoApplyDispatcher withHandlers(List<RedoApplyHandler> handlers) {
        return new RedoApplyDispatcher(handlers);
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
        Map<RedoApplyHandler, RedoApplyBatchHandler> sessions = new LinkedHashMap<>();
        int skipped = 0;
        int appliedRecords = 0;
        for (RedoRecord record : batch.records()) {
            RedoApplyHandler handler = resolveHandler(record);
            if (shouldSkip(record, handler, shouldSkipPage)) {
                skipped++;
                continue;
            }
            RedoApplyBatchHandler session = sessions.computeIfAbsent(handler,
                    h -> openSession(h, batch.range(), context));
            session.apply(record);
            appliedRecords++;
        }
        for (RedoApplyBatchHandler session : sessions.values()) {
            session.finish();
        }
        return new RedoApplySummary(1, appliedRecords > 0 ? 1 : 0, skipped);
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

    private RedoApplyHandler resolveHandler(RedoRecord record) {
        RedoApplyHandler matched = null;
        for (RedoApplyHandler handler : handlers) {
            if (handler.supports(record)) {
                if (matched != null) {
                    throw new DatabaseValidationException("multiple redo apply handlers support record: "
                            + record.getClass().getName());
                }
                matched = handler;
            }
        }
        if (matched == null) {
            throw new DatabaseValidationException("no redo apply handler supports record: "
                    + record.getClass().getName());
        }
        return matched;
    }

    private static boolean shouldSkip(RedoRecord record,
                                      RedoApplyHandler handler,
                                      Predicate<PageId> shouldSkipPage) {
        List<PageId> affectedPages = handler.affectedPages(record);
        if (affectedPages == null) {
            throw new DatabaseValidationException("redo apply handler affected pages must not be null");
        }
        for (PageId pageId : affectedPages) {
            if (pageId == null) {
                throw new DatabaseValidationException("redo apply handler affected page must not be null");
            }
            if (shouldSkipPage.test(pageId)) {
                return true;
            }
        }
        return false;
    }

    private static RedoApplyBatchHandler openSession(RedoApplyHandler handler,
                                                     LogRange range,
                                                     RedoApplyContext context) {
        RedoApplyBatchHandler session = handler.openBatch(range, context);
        if (session == null) {
            throw new DatabaseValidationException("redo apply handler opened null batch session: "
                    + handler.getClass().getName());
        }
        return session;
    }
}
