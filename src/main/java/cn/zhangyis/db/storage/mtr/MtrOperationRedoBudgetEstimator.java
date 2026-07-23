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

    /**
     * 本对象持有的 {@code pageSize} 页面、记录或布局状态；身份与 schema 必须匹配，访问期间遵守 fix/latch 和字节边界，不能泄漏未发布修改。
     */
    private final PageSize pageSize;

    /**
     * 创建 {@code MtrOperationRedoBudgetEstimator}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param pageSize 调用方提供的长度或容量值对象；不得为 {@code null}，且必须已通过其构造范围校验
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    MtrOperationRedoBudgetEstimator(PageSize pageSize) {
        if (pageSize == null) {
            throw new DatabaseValidationException("MTR redo estimator page size must not be null");
        }
        this.pageSize = pageSize;
    }

    /** 按固定布局写操作的 profile 计算 admission 上界；动态领域 purpose 必须提供 workload。
     *
     * @param purpose 选择 {@code estimate} 分支的 {@code RedoBudgetPurpose} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @return {@code estimate} 构造或定位的 redo 日志对象；成功时不为 {@code null}，LSN、预算和批次边界满足 WAL 顺序
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    RedoAppendBudget estimate(RedoBudgetPurpose purpose) {
        if (purpose == null || purpose == RedoBudgetPurpose.READ_ONLY
                || purpose == RedoBudgetPurpose.TEST_UNBOUNDED) {
            throw new DatabaseValidationException("production write requires a concrete redo budget purpose");
        }
        int pageImageEquivalents = switch (purpose) {
            // system.ibd page0..4、两个 segment inode、undo page0..3 与重复元数据 delta 共处一次 boot MTR。
            case ENGINE_BOOT -> 32;
            case ROLLBACK_MARKER -> 6;
            case TRANSACTION_STATE -> 1;
            case UNDO_TRUNCATE_LIFECYCLE -> 4;
            case UNDO_TRUNCATE_REBUILD -> 25;
            case DDL_TABLE_DROP -> 4;
            case DDL_SDI_WRITE -> 3;
            case CHANGE_BUFFER_APPEND -> 32;
            case CHANGE_BUFFER_MERGE -> 128;
            case CLUSTERED_INSERT, CLUSTERED_UPDATE, CLUSTERED_DELETE, SECONDARY_INDEX, PURGE_INDEX,
                    PURGE_RECORD_PROGRESS,
                    ROLLBACK_INVERSE, UNDO_COMMIT, UNDO_FINALIZATION, LOB_WRITE, LOB_FREE,
                    DDL_TABLE_CREATE, DDL_INDEX_DROP -> throw new DatabaseValidationException(
                    "dynamic redo budget purpose requires a domain workload: " + purpose);
            case READ_ONLY, TEST_UNBOUNDED -> throw new DatabaseValidationException(
                    "non-write redo purpose has no production profile: " + purpose);
        };
        return RedoBudgetBuilder.forPurpose(purpose)
                .addPageBytes(pageSize.bytes(), pageImageEquivalents)
                .build();
    }

    /** 把领域 workload 与实例 page size 组合成最终 logical/physical admission 上界。
     *
     * @param purpose 选择 {@code estimate} 分支的 {@code RedoBudgetPurpose} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @param workload redo 收集、定位或重放所需的日志对象；不得为 {@code null}，其 LSN 范围和记录格式必须连续且属于当前恢复或 MTR 上下文
     * @return {@code estimate} 构造或定位的 redo 日志对象；成功时不为 {@code null}，LSN、预算和批次边界满足 WAL 顺序
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    RedoAppendBudget estimate(RedoBudgetPurpose purpose, RedoBudgetWorkload workload) {
        if (purpose == null || workload == null || purpose == RedoBudgetPurpose.READ_ONLY
                || purpose == RedoBudgetPurpose.TEST_UNBOUNDED) {
            throw new DatabaseValidationException("dynamic redo budget purpose/workload is invalid");
        }
        switch (purpose) {
            case CLUSTERED_INSERT, CLUSTERED_UPDATE, CLUSTERED_DELETE, SECONDARY_INDEX, PURGE_INDEX,
                    PURGE_RECORD_PROGRESS,
                    ROLLBACK_INVERSE, ROLLBACK_MARKER, UNDO_COMMIT, UNDO_FINALIZATION, LOB_WRITE, LOB_FREE,
                    DDL_TABLE_CREATE, DDL_INDEX_DROP -> {
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
