package cn.zhangyis.db.storage.trx.lock;

/**
 * 事务锁请求状态。状态由 {@link LockManager} 在持有对应锁分片 mutex 时推进，是 wait queue 与
 * 观测快照的权威状态，不由调用方直接修改。
 */
public enum TransactionLockState {

    /** 请求已经授予，代表事务可继续执行 current-read 或 DML 临界区。 */
    GRANTED,

    /** 请求正在等待冲突事务释放锁，等待线程挂在分片 Condition 上。 */
    WAITING,

    /** 请求等待超时，已从 wait queue 与 wait-for graph 清理。 */
    TIMEOUT,

    /** 请求参与死锁环并被当前简化策略选为 victim，已清理等待边。 */
    VICTIM,

    /** 已授予锁被显式释放或事务级 releaseAll 清理。 */
    RELEASED
}
