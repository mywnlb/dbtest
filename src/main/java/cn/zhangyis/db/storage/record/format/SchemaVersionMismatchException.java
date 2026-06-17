package cn.zhangyis.db.storage.record.format;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/** 逻辑记录的 schemaVersion 与编码用 TableSchema 不一致（编码期检测）。可恢复，需上层做版本桥接。 */
public class SchemaVersionMismatchException extends DatabaseRuntimeException {

    public SchemaVersionMismatchException(String message) {
        super(message);
    }

    public SchemaVersionMismatchException(String message, Throwable cause) {
        super(message, cause);
    }
}
