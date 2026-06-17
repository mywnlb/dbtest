package cn.zhangyis.db.storage.record.type;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/** 类型系统尚不支持该列类型（无对应 codec）。可恢复。 */
public class UnsupportedColumnTypeException extends DatabaseRuntimeException {

    public UnsupportedColumnTypeException(String message) {
        super(message);
    }

    public UnsupportedColumnTypeException(String message, Throwable cause) {
        super(message, cause);
    }
}
