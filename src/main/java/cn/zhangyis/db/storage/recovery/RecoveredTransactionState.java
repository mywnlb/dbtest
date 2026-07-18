package cn.zhangyis.db.storage.recovery;

import cn.zhangyis.db.storage.redo.TransactionStateDeltaState;

/**
 * 恢复期事务表状态。前六项来自稳定 redo code；{@link #RECOVERED_ACTIVE} 是 page3 交叉校验后产生的内存态，
 * 不进入 redo 二进制枚举。
 */
public enum RecoveredTransactionState {

    /** redo 观察到运行态。 */
    ACTIVE,
    /** redo 观察到提交中。 */
    COMMITTING,
    /** 已提交终态。 */
    COMMITTED,
    /** XA prepared 稳定态；v1 无决议器，出现即 fail closed。 */
    PREPARED,
    /** redo 观察到回滚中。 */
    ROLLING_BACK,
    /** 已回滚终态。 */
    ROLLED_BACK,
    /** page3 ACTIVE 且没有终态冲突，启动期间必须完成 undo rollback。 */
    RECOVERED_ACTIVE;

    /** 把 redo 稳定枚举映射为恢复期状态，不把运行时实现细节写回 redo 包。
     *
     * @param state 调用方请求的目标状态、阶段或模式；不得为 {@code null}，且必须是当前状态机允许的后继值
     * @return {@code fromRedo} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     */
    static RecoveredTransactionState fromRedo(TransactionStateDeltaState state) {
        return switch (state) {
            case ACTIVE -> ACTIVE;
            case COMMITTING -> COMMITTING;
            case COMMITTED -> COMMITTED;
            case PREPARED -> PREPARED;
            case ROLLING_BACK -> ROLLING_BACK;
            case ROLLED_BACK -> ROLLED_BACK;
        };
    }
}
