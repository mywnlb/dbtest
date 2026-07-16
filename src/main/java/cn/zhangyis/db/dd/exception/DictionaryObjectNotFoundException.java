package cn.zhangyis.db.dd.exception;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/** DDL 或 table-open 请求的对象在指定字典版本不可见。 */
public class DictionaryObjectNotFoundException extends DatabaseRuntimeException {
    public DictionaryObjectNotFoundException(String message) {
        super(message);
    }

    public DictionaryObjectNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }
}
