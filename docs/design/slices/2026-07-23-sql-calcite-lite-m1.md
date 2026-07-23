# Slice: SQL Calcite-Lite M1（换骨架、保行为）

依据：`mysql-parser-binder-design.md` §3/§8、`mysql-query-optimizer-design.md` §3/§6/§7、
`mysql-sql-executor-storage-api-design.md` §3/§7/§11，以及
`2026-07-23-calcite-lite-sql-planning.md` ADR。

## 目标

把当前 `Parser -> Binder physical Bound -> Executor -> SqlStorageGateway` 改为：

`Parser -> semantic Bound -> LogicalPlan -> HeuristicQueryOptimizer -> PhysicalPlan
-> Executor -> SqlDataAccessPort`。

完成后 Binder 不选择索引，Executor 不按具体 Bound 类型执行，storage data port 不 import
`sql.binder.bound` 的物理计划类型；当前 SQL、事务、锁、LOB、结果和错误边界保持。

## 关键决策

- 保留现有手写 Lexer/Parser 语法能力；Pratt expression 与新语法不在本片实现。
- semantic Bound 只保存 exact `TableDefinition`、投影 ordinal、typed predicate、assignment 和 read intent。
- logical tree 第一阶段只有 scan/filter/project/values/table-modify，不携带 index id 或 range。
- heuristic optimizer 完整复刻当前 point、non-unique equality、最长连续索引前缀、stable index id 和
  clustered full scan 顺序；矛盾谓词输出 empty physical plan。
- `StatementBindingScope.publish()` 从 Binder 移到 Session，在 physical plan 成功生成后、执行前调用。
- `DatabaseEngine` 组合共享 compiler 并注入 Session，生产路径不在 Session 内固定 optimizer 实现。
- `SqlStorageGateway` 保留为事务/XA/data 组合 Facade；Executor 仅依赖 `SqlDataAccessPort`。
- query leaf 首阶段仍一次调用 adapter 并物化完整列表；公开 `QueryResult` 不改成懒游标。
- range UPDATE/DELETE 仍由 adapter 先锁定并物化全部 clustered identity，再在一个
  `DmlStatementGuard` 内修改，避免 Halloween 和 partial mutation。

## 非目标

- 不实现 memo、cost、statistics、EXPLAIN、optimizer trace 或 plan cache。
- 不增加 OR/IN/LIKE/IS NULL/ORDER BY/LIMIT、join、aggregate 或 subquery。
- 不把 residual evaluation、ReadView 或 LOB hydration 从 adapter 移到 Executor。
- 不改变 XA、savepoint、DDL、autocommit、隔离级别和 durability 语义。

## 验收测试

- Binder 测试证明主键、唯一二级和范围 SQL 都只产生相同形状的 semantic Bound。
- Optimizer 测试证明现有 point/range/full-scan/empty/DML 选择完全确定。
- Compiler 失败不 publish metadata；成功计划由 Session publish 后执行。
- Executor 只消费 PhysicalPlan，查询列/行和 affected rows 与旧行为一致。
- RC/RR/RU/SERIALIZABLE、locking read、range DML、LOB、XA/savepoint 回归全部通过。
- 静态扫描证明 Binder 无 access-path selector，data port 无 Binder 物理类型，SQL/session 无
  storage internal import，storage 无上层反向 import。
- 固定 JDK 25.0.2 + Gradle 9.5.1 全量测试通过，测试数不倒退。

## Current map 更新要求

- SQL flow 改为 Session -> Compiler -> Binder -> Logical converter -> Optimizer -> Executor。
- Package Status 增加 production-wired `sql.optimizer.logical/physical` 和 heuristic optimizer。
- 只把真实生产调用画为实线；后续 memo/cost/cursor/trace 保持 gap，不建立 planned 实线。
