package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/** Undo 规划与执行之间事务链或持久尾页发生变化；异常发生在首次物理修改前，可由上层重新规划。 */
public final class UndoWriteStalePlanException extends DatabaseRuntimeException {

    /** 创建可通过重新规划解决的 stale 异常。 */
    public UndoWriteStalePlanException(String message) {
        super(message);
    }

    /** 创建保留持久快照读取根因的 stale 异常。 */
    public UndoWriteStalePlanException(String message, Throwable cause) {
        super(message, cause);
    }
}
