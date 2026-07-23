package cn.zhangyis.db.sql.binder.bound;

/**
 * Binder 从 SQL locking clause 固定的 SELECT 读取语义。Optimizer 不得改写该模式，Data Port 据此选择
 * ReadView 或 current-read 事务锁。
 */
public enum SelectLockMode {
    /** 使用 MVCC ReadView，不持有 predicate/record lock。 */
    CONSISTENT,
    /** 读取当前版本并持有共享锁到事务终态。 */
    FOR_SHARE,
    /** 读取当前版本并持有排他锁到事务终态。 */
    FOR_UPDATE
}
