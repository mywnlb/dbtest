package cn.zhangyis.db.dd.exception;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/** canonical name 已被另一个可见字典对象占用。 */
public class DictionaryObjectExistsException extends DatabaseRuntimeException {
    public DictionaryObjectExistsException(String message) {
        super(message);
    }

    public DictionaryObjectExistsException(String message, Throwable cause) {
        super(message, cause);
    }
}
