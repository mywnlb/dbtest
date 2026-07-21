package cn.zhangyis.db.storage.trx.lock;

/**
 * 已授予事务锁的最短合法持有期。枚举值决定 partial rollback 是否可以提前释放，
 * 不改变锁兼容矩阵、等待队列或事务终态的 {@link LockManager#releaseAll} 行为。
 */
public enum LockRetentionKind {
    /** 普通 record/gap/next-key/logical 锁；无论取得时间都必须保留到事务终态。 */
    TRANSACTION,
    /** 对应实体已被 INSERT undo 物理删除后，允许按保存点获取序号释放。 */
    SAVEPOINT_RELEASEABLE,
    /** 明确声明为语句期的协调锁；语句/保存点收口后允许提前释放。 */
    STATEMENT_DURATION
}
