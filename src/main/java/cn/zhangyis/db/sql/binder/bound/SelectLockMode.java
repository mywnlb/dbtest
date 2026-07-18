package cn.zhangyis.db.sql.binder.bound;

/**
 * Binder 已验证访问路径后的 SELECT 读取模式。它决定 Gateway 选择 ReadView 还是 current-read 事务锁。
 */
public enum SelectLockMode {
    /** 使用 MVCC ReadView，不持有 predicate/record lock。 */
    CONSISTENT,
    /** 读取当前版本并持有共享锁到事务终态。 */
    FOR_SHARE,
    /** 读取当前版本并持有排他锁到事务终态。 */
    FOR_UPDATE
}
