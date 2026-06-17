package cn.zhangyis.db.storage.record.page;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * 页内结构损坏（innodb-record-design §15 PageDirectoryCorrupted）：目录槽越界、next_record 链成环或越出页体、
 * page header direction code 未知、系统记录类型不符等。属可恢复异常——由上层标记该 page 需 recovery 检查，不静默修复。
 */
public class PageDirectoryCorruptedException extends DatabaseRuntimeException {

    public PageDirectoryCorruptedException(String message) {
        super(message);
    }

    public PageDirectoryCorruptedException(String message, Throwable cause) {
        super(message, cause);
    }
}
