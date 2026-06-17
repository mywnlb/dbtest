package cn.zhangyis.db.storage.record.page;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * 页内未找到与 search key 相等的记录（innodb-record-design §15 RecordNotFound）。可恢复——上层据此返回空结果或转插入。
 */
public class RecordNotFoundException extends DatabaseRuntimeException {

    public RecordNotFoundException(String message) {
        super(message);
    }

    public RecordNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }
}
