package cn.zhangyis.db.storage.fsp.lifecycle;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.storage.fil.state.TablespaceState;

/**
 * page-0 保留区中的表空间生命周期快照。
 *
 * <p>该 marker 同时覆盖 GENERAL 的稳定生命周期和 UNDO truncate 生命周期。GENERAL 仅允许
 * {@link TablespaceState#NORMAL}/{@link TablespaceState#DISCARDED}/{@link TablespaceState#CORRUPTED}，其 {@code truncateEpoch=0}、
 * {@code targetSizeInPages=initialSizeInPages}、{@code finishState=NORMAL}，只表达可用/损坏状态。
 * UNDO 则继续使用 ACTIVE/INACTIVE/TRUNCATING 表达可恢复截断流程。旧表空间没有该头时由仓储返回空值明确区分。
 *
 * @param state 当前持久化生命周期状态。
 * @param initialSizeInPages UNDO 的创建初始页数；GENERAL 复用该槽位保存当前文件页数。
 * @param truncateEpoch 已开始截断的单调代次，初始为 0。
 * @param targetSizeInPages UNDO 本代截断目标页数；GENERAL 稳定状态下必须等于 initialSizeInPages。
 * @param finishState UNDO 截断完成后发布的 ACTIVE/INACTIVE，GENERAL 稳定状态固定为 NORMAL。
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
        boolean stableGeneralState = state == TablespaceState.NORMAL || state == TablespaceState.DISCARDED
                || state == TablespaceState.CORRUPTED;
        boolean undoLifecycleState = state == TablespaceState.ACTIVE
                || state == TablespaceState.INACTIVE
                || state == TablespaceState.TRUNCATING;
        if (!stableGeneralState && !undoLifecycleState) {
            throw new DatabaseValidationException("unsupported tablespace lifecycle state: " + state);
        }
        if (stableGeneralState && finishState != TablespaceState.NORMAL) {
            throw new DatabaseValidationException("general lifecycle finish state must be NORMAL: " + finishState);
        }
        if (stableGeneralState && truncateEpoch != 0) {
            throw new DatabaseValidationException("general lifecycle truncate epoch must be zero: " + truncateEpoch);
        }
        if (stableGeneralState && !targetSizeInPages.equals(initialSizeInPages)) {
            throw new DatabaseValidationException("general lifecycle target size must equal current size: target="
                    + targetSizeInPages.value() + ", current=" + initialSizeInPages.value());
        }
        if (undoLifecycleState && finishState != TablespaceState.ACTIVE
                && finishState != TablespaceState.INACTIVE) {
            throw new DatabaseValidationException("truncate finish state must be ACTIVE or INACTIVE: " + finishState);
        }
    }
}
