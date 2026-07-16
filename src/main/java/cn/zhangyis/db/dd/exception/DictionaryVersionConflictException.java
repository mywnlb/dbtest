package cn.zhangyis.db.dd.exception;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/** 字典事务 expected version 已落后或跨号，调用方必须重建 DDL plan。 */
public class DictionaryVersionConflictException extends DatabaseRuntimeException {
    public DictionaryVersionConflictException(String message) {
        super(message);
    }

    public DictionaryVersionConflictException(String message, Throwable cause) {
        super(message, cause);
    }
}
