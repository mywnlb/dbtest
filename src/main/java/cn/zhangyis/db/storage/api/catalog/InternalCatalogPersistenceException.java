package cn.zhangyis.db.storage.api.catalog;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * 内部 catalog 文件的打开、追加、force 或关闭失败。cause 保留底层 IO 证据，上层 DD/DDL
 * 再根据自身发布阶段决定补偿、关闭引擎或报错。
 */
public class InternalCatalogPersistenceException extends DatabaseRuntimeException {

    public InternalCatalogPersistenceException(String message) {
        super(message);
    }

    public InternalCatalogPersistenceException(String message, Throwable cause) {
        super(message, cause);
    }
}
