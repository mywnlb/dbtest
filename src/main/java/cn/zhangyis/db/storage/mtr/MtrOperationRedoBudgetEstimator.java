package cn.zhangyis.db.storage.mtr;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.storage.redo.RedoAppendBudget;
import cn.zhangyis.db.storage.redo.RedoBudgetBuilder;
import cn.zhangyis.db.storage.redo.RedoBudgetPurpose;

/**
 * 当前生产写点的操作级 redo 上界目录。每个 profile 以“最多多少个完整页 after-image 等价量”表达最坏分支，
 * 同时为每个等价量计入独立 PAGE_BYTES header；这比依赖 redo capacity 比例更稳定，并随实例页大小缩放。
 *
 * <p>教学简化：当前 collector 仍按每次 PageGuard 写收集物理 delta，profile 因而保留重复写余量；后续把各领域
 * estimator 下沉到 B+Tree/Undo plan snapshot 时，可用实际 height、fragment/extent 数替换这里的保守页等价量，
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

    /** 按写操作最坏触页 profile 计算 admission 上界。 */
    RedoAppendBudget estimate(RedoBudgetPurpose purpose) {
        if (purpose == null || purpose == RedoBudgetPurpose.READ_ONLY
                || purpose == RedoBudgetPurpose.TEST_UNBOUNDED) {
            throw new DatabaseValidationException("production write requires a concrete redo budget purpose");
        }
        int pageImageEquivalents = switch (purpose) {
            case ENGINE_BOOT -> 8;
            case CLUSTERED_INSERT -> 32;
            case CLUSTERED_UPDATE -> 16;
            case CLUSTERED_DELETE -> 12;
            case PURGE_INDEX -> 24;
            case ROLLBACK_INVERSE -> 32;
            case ROLLBACK_MARKER -> 6;
            case TRANSACTION_STATE -> 1;
            case UNDO_COMMIT -> 6;
            case UNDO_FINALIZATION -> 32;
            case UNDO_TRUNCATE_LIFECYCLE -> 4;
            case UNDO_TRUNCATE_REBUILD -> 24;
            case READ_ONLY, TEST_UNBOUNDED -> throw new DatabaseValidationException(
                    "non-write redo purpose has no production profile: " + purpose);
        };
        return RedoBudgetBuilder.forPurpose(purpose)
                .addPageBytes(pageSize.bytes(), pageImageEquivalents)
                .build();
    }
}
