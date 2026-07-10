package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.UndoSlotId;

/**
 * atomic undo finalization 的包内 crash-point 接缝。调用点只位于 finalization MTR 成功 commit 之后、
 * 内存 slot/history 发布之前；生产组合根固定使用 no-op。
 */
@FunctionalInterface
interface UndoFinalizationFaultInjector {

    /** 在 finalization redo batch 已提交后通知测试。 */
    void afterCommit(UndoFinalizationKind kind, UndoSlotId slotId, PageId firstPageId);

    /** @return 生产默认的无操作 injector。 */
    static UndoFinalizationFaultInjector none() {
        return (kind, slotId, firstPageId) -> { };
    }
}
