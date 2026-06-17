package cn.zhangyis.db.storage.record.type;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/** 列值超出类型可表达范围（整数溢出、DECIMAL 精度/scale 超限、定长/变长超长等）。可恢复。 */
public class ColumnValueOutOfRangeException extends DatabaseRuntimeException {

    public ColumnValueOutOfRangeException(String message) {
        super(message);
    }

    public ColumnValueOutOfRangeException(String message, Throwable cause) {
        super(message, cause);
    }
}
