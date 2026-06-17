package cn.zhangyis.db.storage.record.format;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/** 编码后的记录超过 inline 上限（本片以 2 字节 recordLength 上限 65535 近似；overflow 链未实现）。可恢复。 */
public class RecordTooLargeException extends DatabaseRuntimeException {

    public RecordTooLargeException(String message) {
        super(message);
    }

    public RecordTooLargeException(String message, Throwable cause) {
        super(message, cause);
    }
}
