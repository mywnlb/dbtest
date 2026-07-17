package cn.zhangyis.db.storage.api;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/** DROP 等待 history 表引用时线程被中断；实现会恢复中断标记，调用方应取消本次 DDL。 */
public final class TablePurgeBarrierInterruptedException extends DatabaseRuntimeException {

    /**
     * 创建等待中断异常并保留底层线程中断原因。
     *
     * @param message 包含目标 table id 与等待阶段的领域诊断信息。
     * @param cause   触发取消的 {@link InterruptedException}；barrier 实现已恢复线程中断标记。
     */
    public TablePurgeBarrierInterruptedException(String message, Throwable cause) {
        super(message, cause);
    }
}
