package cn.zhangyis.db.sql.optimizer.logical;

/**
 * 逻辑表修改种类；枚举值只表达 SQL 语义，不暗示 point/range 或存储实现。
 */
public enum ModificationKind {
    /** 插入一行完整 values。 */
    INSERT,
    /** 按 predicate 修改非主键列。 */
    UPDATE,
    /** 按 predicate 执行逻辑 delete-mark。 */
    DELETE
}
