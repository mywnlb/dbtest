package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.domain.RollbackSegmentId;

/**
 * rollback segment slot 耗尽（T1.3c）。事务首写时 {@code RollbackSegmentSlotManager} 找不到空闲 slot 登记
 * insert undo segment 首页时抛出。属运行时可恢复异常：调用方可重试（待 slot 回收实现后）或向上报告。
 *
 * <p>不继承 {@code DatabaseFatalException}：slot 耗尽是容量压力而非数据损坏，不破坏系统继续运行安全性。
 */
public final class UndoSlotExhaustedException extends DatabaseRuntimeException {

    /** @param rsegId 耗尽的 rollback segment。 */
    public UndoSlotExhaustedException(RollbackSegmentId rsegId) {
        super("rollback segment " + rsegId.value() + " has no free undo slot");
    }
}
