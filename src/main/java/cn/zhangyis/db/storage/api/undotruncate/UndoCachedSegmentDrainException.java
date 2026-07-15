package cn.zhangyis.db.storage.api.undotruncate;

import cn.zhangyis.db.common.exception.DatabaseFatalException;

/**
 * cached undo drain 已越过 FSP/page3 物理修改边界后失败。MTR 没有页内容 undo，同一进程不能猜测目录与 inode
 * 的最终状态；调用方应停止服务并让 crash recovery 从 redo/page3 权威状态重新判定。
 */
public final class UndoCachedSegmentDrainException extends DatabaseFatalException {

    /**
     * 创建保留物理边界失败根因的致命异常。
     *
     * @param message 含表空间上下文的失败描述。
     * @param cause 原始目录、FSP、redo 或 MTR 异常。
     */
    public UndoCachedSegmentDrainException(String message, Throwable cause) {
        super(message, cause);
    }
}
