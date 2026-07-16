package cn.zhangyis.db.storage.api.undotruncate;

import cn.zhangyis.db.common.exception.DatabaseFatalException;

/**
 * cache/free drain 越过 FSP/page3 物理边界后的致命失败。MTR 不支持页内容回滚，此时同进程不得猜测
 * owner 与 inode 的最终状态；调用方应停止服务并让 crash recovery 依据 redo/page3 权威状态重新判定。
 */
public final class UndoReusableSegmentDrainException extends DatabaseFatalException {

    /**
     * 创建保留物理边界失败根因的致命异常。
     *
     * @param message 含目标表空间上下文的失败描述。
     * @param cause 原始目录、FSP、redo 或 MTR 异常。
     */
    public UndoReusableSegmentDrainException(String message, Throwable cause) {
        super(message, cause);
    }
}
