package cn.zhangyis.db.session;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.Optional;

/** 不暴露 transaction id/undo/MDL ticket 的 Session 诊断快照。 */
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
