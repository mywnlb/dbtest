package cn.zhangyis.db.storage.trx.lock;

/**
 * InnoDB 风格事务锁模式的教学子集。模式只表达事务级 row/gap 锁语义，不代表 Buffer Pool page latch
 * 或 MTR 物理 latch；调用方在等待这些锁之前必须已经释放 page latch、cursor 和 buffer fix。
 */
public enum TransactionLockMode {

    /** 已存在记录上的共享锁，支持 locking read 的共享读。 */
    REC_S,

    /** 已存在记录上的排他锁，支持 update/delete/current write。 */
    REC_X,

    /** gap 共享锁；本实现与其它 gap 锁兼容，但会阻止插入意向。 */
    GAP_S,

    /** gap 排他锁；本实现与其它 gap 锁兼容，但会阻止插入意向。 */
    GAP_X,

    /** next-key 共享锁，等价于记录共享锁加前序 gap 保护。 */
    NEXT_KEY_S,

    /** next-key 排他锁，等价于记录排他锁加前序 gap 保护。 */
    NEXT_KEY_X,

    /** 插入意向锁；同 gap 内多个插入意向兼容，但会等待覆盖 gap 的 gap/next-key 锁。 */
    INSERT_INTENTION
}
