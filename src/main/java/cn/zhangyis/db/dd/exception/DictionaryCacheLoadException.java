package cn.zhangyis.db.dd.exception;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/** dictionary cache loader 或 single-flight 等待失败。 */
public class DictionaryCacheLoadException extends DatabaseRuntimeException {
    public DictionaryCacheLoadException(String message) {
        super(message);
    }

    public DictionaryCacheLoadException(String message, Throwable cause) {
        super(message, cause);
    }
}
