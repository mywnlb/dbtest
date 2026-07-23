package cn.zhangyis.db.sql.binder.bound;

/**
 * 可转换为关系树的 SQL 语义语句。该层持有 statement metadata lease 固定的 DD 定义，但不携带所选索引、
 * 物理范围或存储句柄；Optimizer 是把它转换为访问路径的唯一 owner。
 */
public sealed interface BoundRelationalStatement extends BoundStatement
        permits BoundInsert, BoundSelect, BoundUpdate, BoundDelete {
}
