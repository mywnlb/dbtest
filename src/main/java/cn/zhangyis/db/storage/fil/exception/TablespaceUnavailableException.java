package cn.zhangyis.db.storage.fil.exception;
import cn.zhangyis.db.storage.fil.io.PageStore;


import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * 表空间当前不可用于普通 IO 异常。表空间确实存在，但处于 EMPTY(未初始化) 或 INACTIVE(如 undo 待截断) 等
 * 不允许普通 PageStore 访问的生命周期状态。它是可恢复运行时错误：调用方可在状态恢复为 NORMAL/ACTIVE 后重试，
 * 或向上报告，而不是当作表空间不存在(TablespaceNotFoundException)或损坏(TablespaceCorruptedException) 处理。
 */
public class TablespaceUnavailableException extends DatabaseRuntimeException {

    /**
     * 创建表空间不可用异常。
     *
     * @param message 表空间不可用的领域描述，应包含 spaceId 与当前状态。
     */
    public TablespaceUnavailableException(String message) {
        super(message);
    }

    /**
     * 创建保留根因的表空间不可用异常。
     *
     * @param message 表空间不可用的领域描述。
     * @param cause 触发不可用判定的底层原因。
     */
    public TablespaceUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
