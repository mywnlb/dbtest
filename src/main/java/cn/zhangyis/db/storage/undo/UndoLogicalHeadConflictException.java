package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * 持久 logical undo 头与调用方预期不一致。该异常通常表示同一 undo segment 出现并发写者，或内存
 * {@code UndoContext} 已落后于页内权威状态；调用方必须停止 statement commit，并转入完整 rollback/恢复诊断，
 * 不能覆盖较新的持久边界。
 */
public class UndoLogicalHeadConflictException extends DatabaseRuntimeException {

    public UndoLogicalHeadConflictException(String message) {
        super(message);
    }

    public UndoLogicalHeadConflictException(String message, Throwable cause) {
        super(message, cause);
    }
}
