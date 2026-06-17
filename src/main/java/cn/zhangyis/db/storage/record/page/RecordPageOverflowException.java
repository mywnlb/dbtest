package cn.zhangyis.db.storage.record.page;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * 页内记录区/目录空间不足（innodb-record-design §13.4 RecordPageOverflow 信号）。
 * 这是预期内的可恢复结果，不代表损坏：B+Tree 收到后应释放当前 page latch 并走 split 流程申请新页，再重试插入。
 */
public class RecordPageOverflowException extends DatabaseRuntimeException {

    public RecordPageOverflowException(String message) {
        super(message);
    }

    public RecordPageOverflowException(String message, Throwable cause) {
        super(message, cause);
    }
}
