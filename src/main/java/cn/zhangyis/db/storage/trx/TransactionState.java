package cn.zhangyis.db.storage.trx;

/**
 * 数据库事务状态机（innodb-transaction-mvcc-design §5.1、§7）。本片仅 5 个流转态；
 * {@code PREPARED}/{@code RECOVERED_ACTIVE} 待事务恢复片再加，避免暗示尚未实现的恢复语义。
 *
 * <p>合法转换：{@code ACTIVE→COMMITTING→COMMITTED}、{@code ACTIVE→ROLLING_BACK→ROLLED_BACK}。
 * 其余皆非法，由 {@code TransactionManager} 经 {@link #canTransitionTo} 校验后抛 {@link TransactionStateException}。
 */
public enum TransactionState {
    /** 活跃：可写入、可获取 id。 */
    ACTIVE,
    /** 提交中：已禁止新操作，正在分配提交序号、收尾。 */
    COMMITTING,
    /** 已提交。 */
    COMMITTED,
    /** 回滚中。 */
    ROLLING_BACK,
    /** 已回滚。 */
    ROLLED_BACK;

    /**
     * 本状态是否允许转到 {@code target}。非法转换由 {@code TransactionManager} 据此抛
     * {@link TransactionStateException}，把状态约束集中在状态机而非散落的布尔判断。
     */
    public boolean canTransitionTo(TransactionState target) {
        return switch (this) {
            case ACTIVE -> target == COMMITTING || target == ROLLING_BACK;
            case COMMITTING -> target == COMMITTED;
            case ROLLING_BACK -> target == ROLLED_BACK;
            case COMMITTED, ROLLED_BACK -> false;
        };
    }
}
