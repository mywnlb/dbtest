package cn.zhangyis.db.storage.record.format;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/** 记录物理格式损坏或非法（列数不符、记录长度不符、坏 record type、null 给非空列等）。可恢复。 */
public class RecordFormatException extends DatabaseRuntimeException {

    public RecordFormatException(String message) {
        super(message);
    }

    public RecordFormatException(String message, Throwable cause) {
        super(message, cause);
    }
}
