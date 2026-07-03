package cn.zhangyis.db.storage.btree;

/**
 * B+Tree current-read 的锁定读模式。该枚举只决定事务行锁模式，不表达 MVCC ReadView；
 * consistent read 仍由 trx.MvccReader 负责。
 */
public enum BTreeCurrentReadMode {

    /** 对命中记录取共享语义，后续可映射 SELECT ... FOR SHARE。 */
    FOR_SHARE,

    /** 对命中记录取排他语义，后续可映射 SELECT ... FOR UPDATE / UPDATE / DELETE。 */
    FOR_UPDATE
}
