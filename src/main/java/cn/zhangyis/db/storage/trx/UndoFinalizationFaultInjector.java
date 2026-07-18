package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.UndoSlotId;

/**
 * atomic undo finalization 的包内 crash-point 接缝。调用点只位于 finalization MTR 成功 commit 之后、
 * 内存 slot/history 发布之前；生产组合根固定使用 no-op。
 */
@FunctionalInterface
interface UndoFinalizationFaultInjector {

    /** 在 finalization redo batch 已提交后通知测试。
     *
     * @param kind 选择 {@code afterCommit} 分支的 {@code UndoFinalizationKind} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @param slotId 参与 {@code afterCommit} 的稳定领域标识 {@code UndoSlotId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param firstPageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     */
    void afterCommit(UndoFinalizationKind kind, UndoSlotId slotId, PageId firstPageId);

    /** @return 生产默认的无操作 injector。 */
    static UndoFinalizationFaultInjector none() {
        return (kind, slotId, firstPageId) -> { };
    }
}
