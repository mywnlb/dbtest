package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 事务首写前 statement 边界的一次性能力令牌。它不伪造 {@link TransactionSavepoint}，也不包含 undoNo 或
 * roll pointer；只有在事务尚未创建 {@link UndoContext} 时，所属 {@link RollbackService} 才能构造该令牌。
 * 后续 rollback/close 必须同时匹配创建它的 service 与事务，从而证明“回滚到空链”确实对应一个先前打开的边界，
 * 而不是任意 ACTIVE 事务对整条逻辑 undo 链的无约束截断。
 *
 * <p>令牌由创建它的 statement 执行线程独占，不提供跨线程同步。成功 rollback 或 close 后进入终态，不能复用；
 * 底层 rollback 抛出前不会消费令牌，具体失败处置由持有它的 DML Guard 决定。
 */
public final class EmptyUndoBoundary {

    /** 铸造令牌的 rollback service；阻止另一套 storage 组合根消费该能力。 */
    private final RollbackService owner;
    /** 边界所属事务实例；首写前可能尚无 TransactionId，因此用运行期对象身份校验。 */
    private final Transaction transaction;
    /** 一次性生命周期状态；只由所属 RollbackService 在同一 statement 线程更新。 */
    private BoundaryState state = BoundaryState.OPEN;

    /** 空 undo 边界的一次性状态机。 */
    private enum BoundaryState {
        /** 边界尚可选择 rollback 或成功 close。 */
        OPEN,
        /** 已用于 statement rollback。 */
        ROLLED_BACK,
        /** 已按语句成功路径关闭。 */
        CLOSED
    }

    /** 仅同包 {@link RollbackService} 可以铸造令牌。
     * @param owner 由组合根提供的 {@code RollbackService} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param transaction 调用方当前事务及其一致性视图或保存点状态；不得为 {@code null}，事务必须由当前会话拥有且处于本操作允许的生命周期阶段
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    EmptyUndoBoundary(RollbackService owner, Transaction transaction) {
        if (owner == null || transaction == null) {
            throw new DatabaseValidationException("empty undo boundary owner/transaction must not be null");
        }
        this.owner = owner;
        this.transaction = transaction;
    }

    /** 校验 service、事务归属与一次性 OPEN 状态，任何状态修改前都必须调用。
     *
     * @param expectedOwner 由组合根提供的 {@code RollbackService} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code requireOpen} 调用
     * @param expectedTransaction 调用方当前事务及其一致性视图或保存点状态；不得为 {@code null}，事务必须由当前会话拥有且处于本操作允许的生命周期阶段
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws TransactionStateException 当前生命周期、版本或所有权与请求不一致时抛出；调用方应重新读取权威状态后回滚或重试
     */
    void requireOpen(RollbackService expectedOwner, Transaction expectedTransaction) {
        if (owner != expectedOwner || transaction != expectedTransaction) {
            throw new DatabaseValidationException("empty undo boundary belongs to a different service or transaction");
        }
        if (state != BoundaryState.OPEN) {
            throw new TransactionStateException("empty undo boundary is already " + state);
        }
    }

    /** 标记该能力已完成 statement rollback；调用方已先完成归属与 OPEN 校验。 */
    void markRolledBack() {
        state = BoundaryState.ROLLED_BACK;
    }

    /** 标记该能力已按语句成功路径关闭；调用方已先完成归属与 OPEN 校验。 */
    void markClosed() {
        state = BoundaryState.CLOSED;
    }
}
