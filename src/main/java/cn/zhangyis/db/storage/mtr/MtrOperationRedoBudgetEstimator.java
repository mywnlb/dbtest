package cn.zhangyis.db.storage.mtr;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.storage.redo.RedoAppendBudget;
import cn.zhangyis.db.storage.redo.RedoBudgetBuilder;
import cn.zhangyis.db.storage.redo.RedoBudgetPurpose;
import cn.zhangyis.db.storage.redo.RedoBudgetWorkload;

/**
 * 当前生产写点的操作级 redo 上界目录。每个 profile 以“最多多少个完整页 after-image 等价量”表达最坏分支，
 * 同时为每个等价量计入独立 PAGE_BYTES header；这比依赖 redo capacity 比例更稳定，并随实例页大小缩放。
 *
 * <p>教学简化：当前 collector 仍按每次 PageGuard 写收集物理 delta，领域 estimator 因而保留重复写余量；
 * B+Tree/Undo/DML 在 begin 前提供 height、首写状态或 segment drop plan 派生的 workload，
 * {@link RedoAppendBudget#requireCovers(java.util.List)} 仍作为同一精确结算边界。
 */
final class MtrOperationRedoBudgetEstimator {

    private final PageSize pageSize;

    MtrOperationRedoBudgetEstimator(PageSize pageSize) {
        if (pageSize == null) {
            throw new DatabaseValidationException("MTR redo estimator page size must not be null");
        }
        this.pageSize = pageSize;
    }

    /** 按固定布局写操作的 profile 计算 admission 上界；动态领域 purpose 必须提供 workload。 */
    RedoAppendBudget estimate(RedoBudgetPurpose purpose) {
        if (purpose == null || purpose == RedoBudgetPurpose.READ_ONLY
                || purpose == RedoBudgetPurpose.TEST_UNBOUNDED) {
            throw new DatabaseValidationException("production write requires a concrete redo budget purpose");
        }
        int pageImageEquivalents = switch (purpose) {
            case ENGINE_BOOT -> 9;
            case ROLLBACK_MARKER -> 6;
            case TRANSACTION_STATE -> 1;
            case UNDO_TRUNCATE_LIFECYCLE -> 4;
            case UNDO_TRUNCATE_REBUILD -> 25;
            case DDL_TABLE_DROP -> 4;
            case CLUSTERED_INSERT, CLUSTERED_UPDATE, CLUSTERED_DELETE, PURGE_INDEX,
                    ROLLBACK_INVERSE, UNDO_COMMIT, UNDO_FINALIZATION, LOB_WRITE, LOB_FREE,
                    DDL_TABLE_CREATE -> throw new DatabaseValidationException(
                    "dynamic redo budget purpose requires a domain workload: " + purpose);
            case READ_ONLY, TEST_UNBOUNDED -> throw new DatabaseValidationException(
                    "non-write redo purpose has no production profile: " + purpose);
        };
        return RedoBudgetBuilder.forPurpose(purpose)
                .addPageBytes(pageSize.bytes(), pageImageEquivalents)
                .build();
    }

    /** 把领域 workload 与实例 page size 组合成最终 logical/physical admission 上界。 */
    RedoAppendBudget estimate(RedoBudgetPurpose purpose, RedoBudgetWorkload workload) {
        if (purpose == null || workload == null || purpose == RedoBudgetPurpose.READ_ONLY
                || purpose == RedoBudgetPurpose.TEST_UNBOUNDED) {
            throw new DatabaseValidationException("dynamic redo budget purpose/workload is invalid");
        }
        switch (purpose) {
            case CLUSTERED_INSERT, CLUSTERED_UPDATE, CLUSTERED_DELETE, PURGE_INDEX,
                    ROLLBACK_INVERSE, ROLLBACK_MARKER, UNDO_COMMIT, UNDO_FINALIZATION, LOB_WRITE, LOB_FREE,
                    DDL_TABLE_CREATE -> {
                // ROLLBACK_MARKER 无 LOB 时仍可走固定 6-page profile；ownership free 时使用动态合并上界。
            }
            default -> throw new DatabaseValidationException(
                    "fixed redo budget purpose does not accept a domain workload: " + purpose);
        }
        return RedoBudgetBuilder.forPurpose(purpose)
                .addPageBytes(pageSize.bytes(), workload.pageImageEquivalents())
                .addLogicalBytes(workload.extraLogicalBytes())
                .build();
    }
}
