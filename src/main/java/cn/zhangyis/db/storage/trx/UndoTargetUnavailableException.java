package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/** live/statement/savepoint rollback 遇到已隔离 DD 对象时拒绝物理访问；只有启动恢复专用入口可跳过。 */
public final class UndoTargetUnavailableException extends DatabaseRuntimeException {

    /** @param tableId undo record 中的稳定目标表身份 */
    public UndoTargetUnavailableException(long tableId) {
        super("undo target is unavailable after recovery isolation: tableId=" + tableId);
    }
}
