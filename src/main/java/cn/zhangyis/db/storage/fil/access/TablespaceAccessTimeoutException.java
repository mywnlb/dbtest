package cn.zhangyis.db.storage.fil.access;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * 表空间共享或独占准入等待未能完成。
 *
 * <p>该异常覆盖有界等待到期和等待线程被中断两种可恢复情形。抛出时调用方尚未取得目标
 * lease，因此不能继续页访问或生命周期变更；应释放当前操作已经持有的其它资源，再选择回滚、
 * 关闭流程或稍后重试。线程中断情形的原始原因会保留为 cause，且控制器会恢复中断标记。</p>
 */
public final class TablespaceAccessTimeoutException extends DatabaseRuntimeException {

    /**
     * 使用可诊断消息创建准入失败异常。
     *
     * @param message 应包含目标表空间及请求模式的诊断信息，便于调用方定位被阻塞的物理操作
     */
    public TablespaceAccessTimeoutException(String message) {
        super(message);
    }

    /**
     * 使用可诊断消息及底层中断原因创建准入失败异常。
     *
     * @param message 应包含目标表空间的诊断信息
     * @param cause 导致等待终止的原始异常；通常为 {@link InterruptedException}，不得丢弃
     */
    public TablespaceAccessTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }
}
