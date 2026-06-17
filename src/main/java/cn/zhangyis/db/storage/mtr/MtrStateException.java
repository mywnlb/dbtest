package cn.zhangyis.db.storage.mtr;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * mini-transaction 状态/绑定异常（设计 §17）。用于非法状态流转、终态后复用、嵌套 begin、
 * 跨线程或未绑定 commit/rollback、无当前 MTR 时 current()、savepoint 跨 MTR 误用。
 * 可恢复运行时异常：调用方应回滚或重建 MTR。
 */
public class MtrStateException extends DatabaseRuntimeException {

    public MtrStateException(String message) {
        super(message);
    }

    public MtrStateException(String message, Throwable cause) {
        super(message, cause);
    }
}
