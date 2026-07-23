package cn.zhangyis.db.common.exception;

/**
 * FORCE 恢复导出实例拒绝 SQL、DDL、MTR 或 checkpoint 写入时的跨层稳定领域异常。
 *
 * <p>该异常位于 common 层，使 Session 与 SQL adapter 无需反向依赖 storage.engine 包。</p>
 */
public final class RecoveryExportWriteRejectedException extends DatabaseRuntimeException {

    /**
     * 创建不带底层根因的写准入异常。
     *
     * @param message 被拒绝的写入口及当前只读恢复模式
     */
    public RecoveryExportWriteRejectedException(String message) {
        super(message);
    }

    /**
     * 创建保留底层准入失败根因的异常。
     *
     * @param message 被拒绝的写入口及当前只读恢复模式
     * @param cause 原始失败；调用方可沿异常图进行 fatal 分类
     */
    public RecoveryExportWriteRejectedException(String message, Throwable cause) {
        super(message, cause);
    }
}
