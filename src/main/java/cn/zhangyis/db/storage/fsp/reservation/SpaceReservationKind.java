package cn.zhangyis.db.storage.fsp.reservation;

/**
 * 空间预留类型，对应 disk-manager 设计 §7.1。
 *
 * <p>首版不按类型做不同限额，只把类型记录到 reservation 中，供后续区分普通 DML、undo grow、purge/merge
 * 和 BLOB 外部页的抢占/限流策略。保留这个枚举能避免未来把“为什么预留”塞进布尔参数。
 */
public enum SpaceReservationKind {

    /** 普通插入、B+Tree split 等用户路径增长。 */
    NORMAL,

    /** Undo log 页增长，后续可由 purge 回收。 */
    UNDO,

    /** purge、物理删除、merge 等清理路径，后续可考虑更高优先级。 */
    CLEANING,

    /** 外部大字段页增长。 */
    BLOB
}
