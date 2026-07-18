package cn.zhangyis.db.storage.fil.exception;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * 表空间物理文件当前没有可用打开句柄的异常。
 *
 * <p>{@code FileChannelPageStore} 在目标 {@code SpaceId} 未经 create/open 登记时抛出该异常；
 * {@code DataFileHandle} 在句柄已关闭后被继续使用时也会抛出。失败发生在目标页 IO、扩展、
 * 截断或 force 之前。调用方只有在上层字典和生命周期状态仍允许访问时，才能重新 open/create；
 * 对已经 drop/discard 的空间不得通过重新打开绕过状态机。</p>
 */
public class TablespaceNotOpenException extends DatabaseRuntimeException {

    /**
     * 使用物理句柄定位信息创建异常。
     *
     * @param message 应包含目标 SpaceId，以及“未登记”或“句柄已关闭”的诊断状态
     */
    public TablespaceNotOpenException(String message) {
        super(message);
    }

    /**
     * 使用物理句柄定位信息和底层失败原因创建异常。
     *
     * @param message 应包含目标 SpaceId 和失败阶段
     * @param cause 导致句柄无法打开或继续使用的原始原因；不得丢弃
     */
    public TablespaceNotOpenException(String message, Throwable cause) {
        super(message, cause);
    }
}
