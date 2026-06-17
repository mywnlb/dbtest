package cn.zhangyis.db.storage.record.type;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/** 列值与列类型不相容（如把 StringValue 交给整数列）。可恢复（调用方修正值）。 */
public class InvalidColumnValueException extends DatabaseRuntimeException {

    public InvalidColumnValueException(String message) {
        super(message);
    }

    public InvalidColumnValueException(String message, Throwable cause) {
        super(message, cause);
    }
}
