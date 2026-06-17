package cn.zhangyis.db.storage.trx;

/**
 * 事务隔离级别（SQL 标准四级）。本片（T1.1）仅作 {@code Transaction} 的记录字段，**不驱动任何行为**——
 * 没有 ReadView，隔离语义随后续「可见性读路径」片接入（RR 事务级快照 / RC 每语句快照），
 * SERIALIZABLE 需 LockManager（更后续）。
 */
public enum IsolationLevel {
    /** 读未提交（占位，本片无行为）。 */
    READ_UNCOMMITTED,
    /** 读已提交：每条一致性读新建 ReadView（语义后续片接入）。 */
    READ_COMMITTED,
    /** 可重复读（默认）：事务级 ReadView（语义后续片接入）。 */
    REPEATABLE_READ,
    /** 串行化（占位，需 LockManager，更后续）。 */
    SERIALIZABLE
}
