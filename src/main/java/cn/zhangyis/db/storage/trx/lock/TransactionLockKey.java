package cn.zhangyis.db.storage.trx.lock;

/**
 * 事务锁的逻辑资源键。所有 key 都必须能归属到一个索引 id，LockManager 以该索引 id 做分片路由，
 * 保证同一索引上的 record/gap/next-key/insert-intention 关系在同一分片内判断兼容性。
 */
public sealed interface TransactionLockKey
        permits RecordLockKey, GapLockKey, NextKeyLockKey, InsertIntentionLockKey, SecondaryUniqueKeyLockKey {

    /**
     * 返回锁资源所属索引。该值是锁分片路由的基础；若调用方给出错误 indexId，会导致同一索引内冲突锁被拆到
     * 不同分片而破坏互斥语义，因此构造 key 时必须来自 B+Tree/RecordRef 的权威 metadata。
     *
     * @return 索引 id。
     */
    long indexId();
}
