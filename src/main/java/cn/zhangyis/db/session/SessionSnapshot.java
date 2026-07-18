package cn.zhangyis.db.session;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.Optional;

/** 不暴露 transaction id/undo/MDL ticket 的 Session 诊断快照。
 *
 * @param id 参与 {@code 构造} 的稳定领域标识 {@code SessionId}；不得为 {@code null}，并须由对应值对象构造校验产生
 * @param state 调用方请求的目标状态、阶段或模式；不得为 {@code null}，且必须是当前状态机允许的后继值
 * @param autocommit 会话事务生命周期标志；必须反映权威事务状态，决定语句后提交、仅回滚或是否存在活跃事务
 * @param transactionMode 调用方请求的目标状态、阶段或模式；不得为 {@code null}，且必须是当前状态机允许的后继值
 * @param transactionActive 会话事务生命周期标志；必须反映权威事务状态，决定语句后提交、仅回滚或是否存在活跃事务
 * @param rollbackOnly 会话事务生命周期标志；必须反映权威事务状态，决定语句后提交、仅回滚或是否存在活跃事务
 * @param currentSchema 可选的 {@code currentSchema}；参数本身不得为 {@code null}，空 {@code Optional} 明确表示调用方未提供该领域值
 */
public record SessionSnapshot(SessionId id, SessionState state, boolean autocommit,
                              SessionTransactionMode transactionMode, boolean transactionActive,
                              boolean rollbackOnly, Optional<String> currentSchema) {
    public SessionSnapshot {
        if (id == null || state == null || transactionMode == null || currentSchema == null
                || transactionActive != (transactionMode != SessionTransactionMode.NONE)
                || rollbackOnly && !transactionActive) {
            throw new DatabaseValidationException("invalid session snapshot state");
        }
    }
}
