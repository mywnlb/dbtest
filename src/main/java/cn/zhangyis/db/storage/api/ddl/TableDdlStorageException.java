package cn.zhangyis.db.storage.api.ddl;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/** 可回滚或由启动恢复续作的物理 DDL 失败。 */
public final class TableDdlStorageException extends DatabaseRuntimeException {
    public TableDdlStorageException(String message) {
        super(message);
    }

    public TableDdlStorageException(String message, Throwable cause) {
        super(message, cause);
    }
}
