package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.storage.undo.UndoLogicalHead;

/**
 * 完整回滚 crash-point 测试接缝。调用点只位于 MTR 成功提交之后；它不模拟 MTR COMMITTING 内部失败，
 * 也不能改变生产控制流。测试抛出的异常代表进程在两个已定义持久边界之间突然停止。
 */
@FunctionalInterface
interface RollbackProgressFaultInjector {

    /**
     * 在指定成功提交边界之后通知测试。
     *
     * @param phase 已到达的提交阶段。
     * @param head  该阶段对应的逻辑头：inverse 后为旧 expected，marker 后为已提交 target。
     */
    void after(RollbackProgressPhase phase, UndoLogicalHead head);

    /** @return 生产默认的无操作 injector。 */
    static RollbackProgressFaultInjector none() {
        return (phase, head) -> { };
    }
}
