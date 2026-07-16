package cn.zhangyis.db.storage.btree;

import cn.zhangyis.db.common.exception.DatabaseFatalException;

/** prepared 聚簇插入越过物理资源边界后未发布或所有权被破坏。 */
public final class PreparedInsertStateException extends DatabaseFatalException {
    public PreparedInsertStateException(String message) {
        super(message);
    }

    public PreparedInsertStateException(String message, Throwable cause) {
        super(message, cause);
    }
}
