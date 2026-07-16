package cn.zhangyis.db.dd.exception;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/** DictionaryTransaction 已提交/关闭后再次修改或提交时抛出。 */
public class DictionaryTransactionStateException extends DatabaseRuntimeException {
    public DictionaryTransactionStateException(String message) {
        super(message);
    }

    public DictionaryTransactionStateException(String message, Throwable cause) {
        super(message, cause);
    }
}
