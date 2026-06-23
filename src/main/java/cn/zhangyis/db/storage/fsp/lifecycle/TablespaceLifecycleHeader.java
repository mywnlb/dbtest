package cn.zhangyis.db.storage.fsp.lifecycle;
import cn.zhangyis.db.storage.fil.state.TablespaceState;


import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageNo;

/**
 * page-0 保留区中的表空间生命周期快照。
 *
 * <p>当前只为新建 UNDO 表空间写入。{@code initialSizeInPages} 是截断后的固定物理大小，
 * {@code truncateEpoch} 用于识别重复恢复，{@code targetSizeInPages}/{@code finishState}
 * 使 TRUNCATING 状态在崩溃后具备完整续作输入。普通旧表空间没有该头，由仓储返回空值明确区分。
 *
 * @param state 当前持久化生命周期状态。
 * @param initialSizeInPages 创建时初始页数，截断不得小于该边界。
 * @param truncateEpoch 已开始截断的单调代次，初始为 0。
 * @param targetSizeInPages 本代截断目标页数；稳定状态下等于初始页数。
 * @param finishState 截断完成后发布的 ACTIVE 或 INACTIVE 状态。
 */
public record TablespaceLifecycleHeader(
        TablespaceState state,
        PageNo initialSizeInPages,
        long truncateEpoch,
        PageNo targetSizeInPages,
        TablespaceState finishState) {

    public TablespaceLifecycleHeader {
        if (state == null || initialSizeInPages == null || targetSizeInPages == null || finishState == null) {
            throw new DatabaseValidationException("tablespace lifecycle header fields must not be null");
        }
        if (initialSizeInPages.value() < 1 || targetSizeInPages.value() < 1) {
            throw new DatabaseValidationException("tablespace lifecycle sizes must be positive");
        }
        if (targetSizeInPages.value() > initialSizeInPages.value()) {
            throw new DatabaseValidationException("truncate target must not exceed initial size");
        }
        if (truncateEpoch < 0) {
            throw new DatabaseValidationException("truncate epoch must not be negative: " + truncateEpoch);
        }
        if (finishState != TablespaceState.ACTIVE && finishState != TablespaceState.INACTIVE) {
            throw new DatabaseValidationException("truncate finish state must be ACTIVE or INACTIVE: " + finishState);
        }
    }
}
