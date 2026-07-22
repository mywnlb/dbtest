package cn.zhangyis.db.storage.api.undotruncate;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.Optional;

/**
 * 一次自动 undo truncate 检查的不可变结果。
 *
 * @param status 本次稳定分类；不能为 {@code null}
 * @param observedReclaimablePages 获取 X lease 后观察到的“物理页数－持久 initial size”；必须非负
 * @param completion 仅 {@link UndoTruncationAttemptStatus#COMPLETED} 时存在的持久完成结果
 */
public record UndoTruncationAttemptResult(
        UndoTruncationAttemptStatus status,
        long observedReclaimablePages,
        Optional<UndoTablespaceTruncationResult> completion) {

    /**
     * 交叉校验状态、页数与完成证据，禁止 deferred 结果伪装已推进 epoch。
     *
     * @throws DatabaseValidationException 字段为空、页数为负或完成证据与状态不一致时抛出
     */
    public UndoTruncationAttemptResult {
        if (status == null || completion == null) {
            throw new DatabaseValidationException("undo truncation attempt fields must not be null");
        }
        if (observedReclaimablePages < 0) {
            throw new DatabaseValidationException(
                    "undo truncation reclaimable pages must not be negative: " + observedReclaimablePages);
        }
        if ((status == UndoTruncationAttemptStatus.COMPLETED) != completion.isPresent()) {
            throw new DatabaseValidationException(
                    "undo truncation completion must be present exactly for COMPLETED status");
        }
    }

    /**
     * 创建不带完成证据的阈值跳过或 deferred 结果。
     *
     * @param status 本次阈值或延期分类；不能是 COMPLETED
     * @param reclaimablePages X lease 下观察到的非负可回收页数
     * @return 未推进 truncate epoch 的不可变结果
     */
    static UndoTruncationAttemptResult incomplete(UndoTruncationAttemptStatus status, long reclaimablePages) {
        return new UndoTruncationAttemptResult(status, reclaimablePages, Optional.empty());
    }

    /**
     * 创建带持久 epoch/target 的完成结果。
     *
     * @param reclaimablePages 本轮进入协议前观察到的非负可回收页数
     * @param completion 完整 crash-safe truncate 的持久完成证据；不得为 {@code null}
     * @return 状态固定为 COMPLETED 的不可变结果
     */
    static UndoTruncationAttemptResult completed(long reclaimablePages,
                                                   UndoTablespaceTruncationResult completion) {
        return new UndoTruncationAttemptResult(UndoTruncationAttemptStatus.COMPLETED,
                reclaimablePages, Optional.of(completion));
    }
}
