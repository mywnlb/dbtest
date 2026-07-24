package cn.zhangyis.db.sql.binder.bound;

/**
 * Binder 输出的封闭语义结果。关系语句只描述名称、类型、表达式和读写意图；DDL 命令继续走独立
 * coordinator，任何访问索引、range 或执行算子都不属于本层。
 */
public sealed interface BoundStatement permits BoundRelationalStatement,
        BoundCreateTable, BoundCreateIndex, BoundDropIndex, BoundAlterTablespace, BoundAlterTable,
        BoundCreateSchema, BoundDropTables, BoundDropSchema {
}
