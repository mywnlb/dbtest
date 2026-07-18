package cn.zhangyis.db.storage.trx;

/**
 * 数据库事务状态机（innodb-transaction-mvcc-design §5.1、§7）。
 *
 * <p>普通事务合法转换为 {@code ACTIVE→COMMITTING→COMMITTED}、
 * {@code ACTIVE→ROLLING_BACK→ROLLED_BACK}。XA participant 另有
 * {@code ACTIVE→PREPARED→COMMITTING→COMMITTED} 和
 * {@code PREPARED→PREPARED_ROLLING_BACK→ROLLED_BACK}；独立 prepared rollback 重试态保证失败后仍能
 * 要求物理 undo header 保持 PREPARED，不会误走普通 ACTIVE finalization。
 * 其余皆非法，由 {@code TransactionManager} 经 {@link #canTransitionTo} 校验后抛 {@link TransactionStateException}。
 */
public enum TransactionState {
    /** 活跃：可写入、可获取 id。 */
    ACTIVE,
    /** XA phase one 已持久化；保留 active-table 身份和事务锁，只接受显式 phase-two 决议。 */
    PREPARED,
    /** 提交中：已禁止新操作，正在分配提交序号、收尾。 */
    COMMITTING,
    /** 已提交。 */
    COMMITTED,
    /** 回滚中。 */
    ROLLING_BACK,
    /** prepared 分支回滚中；失败重试仍以 PREPARED first-page 状态为物理前置条件。 */
    PREPARED_ROLLING_BACK,
    /** 已回滚。 */
    ROLLED_BACK;

    /**
     * 本状态是否允许转到 {@code target}。非法转换由 {@code TransactionManager} 据此抛
     * {@link TransactionStateException}，把状态约束集中在状态机而非散落的布尔判断。
     * @param target 选择 {@code canTransitionTo} 分支的 {@code TransactionState} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @return {@code canTransitionTo} 命名的领域事实成立时为 {@code true}，否则为 {@code false}；查询本身不改变权威状态
     */
    public boolean canTransitionTo(TransactionState target) {
        return switch (this) {
            case ACTIVE -> target == PREPARED || target == COMMITTING || target == ROLLING_BACK;
            case PREPARED -> target == COMMITTING || target == PREPARED_ROLLING_BACK;
            case COMMITTING -> target == COMMITTED;
            case ROLLING_BACK -> target == ROLLED_BACK;
            case PREPARED_ROLLING_BACK -> target == ROLLED_BACK;
            case COMMITTED, ROLLED_BACK -> false;
        };
    }
}
