package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/** DD/物理状态机可由回滚或启动恢复处理的 DDL 异常。 */
public final class DictionaryDdlException extends DatabaseRuntimeException {
    public DictionaryDdlException(String message) {
        super(message);
    }

    public DictionaryDdlException(String message, Throwable cause) {
        super(message, cause);
    }
}
