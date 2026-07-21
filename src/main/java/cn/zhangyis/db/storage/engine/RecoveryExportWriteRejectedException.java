package cn.zhangyis.db.storage.engine;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/** FORCE 恢复导出实例拒绝普通 SQL、MTR、DDL 或 checkpoint 写入时的稳定领域异常。 */
public final class RecoveryExportWriteRejectedException extends DatabaseRuntimeException {

    /**
     * @param message 被拒绝的写入口及当前访问模式
     */
    public RecoveryExportWriteRejectedException(String message) {
        super(message);
    }
}
