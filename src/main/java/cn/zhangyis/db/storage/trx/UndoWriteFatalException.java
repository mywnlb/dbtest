package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseFatalException;

/**
 * Undo 写已越过空间预留/首次物理修改边界后的失败。当前 MTR 没有 content undo，同进程不得把该异常当成可重试错误。
 */
public final class UndoWriteFatalException extends DatabaseFatalException {

    /** 创建没有底层根因、但已越过物理发布边界的致命异常。 */
    public UndoWriteFatalException(String message) {
        super(message);
    }

    /** 创建保留原始物理写、B+Tree 或 MTR 提交根因的致命异常。 */
    public UndoWriteFatalException(String message, Throwable cause) {
        super(message, cause);
    }
}
