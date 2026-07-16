package cn.zhangyis.db.sql.executor.storage;

/** v1 已实现一致性读语义的隔离级别；adapter 必须显式映射，不依赖枚举 ordinal。 */
public enum SqlIsolationLevel {
    READ_COMMITTED,
    REPEATABLE_READ
}
