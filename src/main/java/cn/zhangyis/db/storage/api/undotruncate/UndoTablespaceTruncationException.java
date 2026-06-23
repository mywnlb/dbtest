package cn.zhangyis.db.storage.api.undotruncate;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/** undo 截断状态、格式或持久化边界不满足；marker 已落盘时调用方应通过 recovery 续作。 */
public class UndoTablespaceTruncationException extends DatabaseRuntimeException {

    public UndoTablespaceTruncationException(String message) {
        super(message);
    }

    public UndoTablespaceTruncationException(String message, Throwable cause) {
        super(message, cause);
    }
}
