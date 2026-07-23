# ADR: 自研 Calcite-Lite SQL 规划骨架

## Status

Accepted, 2026-07-23.

## Context

当前 SQL Binder 在完成名称和类型绑定后，直接选择主键点查、二级索引范围或聚簇全扫，并输出可由
`DefaultSqlStorageGateway` 执行的物理形状。这样虽然实现简单，但 Binder、Executor 和 storage adapter
共同依赖一组物理型 Bound 类型，无法独立增加规则改写、代价比较、EXPLAIN 或新的执行算子。

直接引入 Apache Calcite 会同时带入完整 SQL/Rel、trait、metadata、规则和 adapter 体系，体量与本教学型
数据库不匹配，也会掩盖本项目希望展示的数据库内核边界。

## Decision

实现自有的 Calcite-Lite 分层：

- Parser 只产生不可变语法 AST；
- Binder 只产生 exact-version table/column、typed predicate 和 assignment 等语义 Bound IR；
- `BoundToLogicalConverter` 构造不可变关系树；
- `QueryOptimizer` 独占访问路径、range 和物理计划选择；
- Executor 只执行 `PhysicalPlan`，storage data port 不再接收 Binder 物理类型；
- `DatabaseEngine` 组合并注入共享 `SqlStatementCompiler`，Session 不固定具体 optimizer；
- Session 在完整物理计划生成后才发布 statement metadata lease。

第一阶段使用确定性 heuristic optimizer，严格保持当前访问路径顺序和结果语义。Memo、cost、真正的
StorageCursor、EXPLAIN 与新语法按后续切片增加。

## Consequences

- Binder、Optimizer、Executor 和 storage adapter 的职责可以分别测试和演进。
- 首个切片仍由 adapter 完整物化查询结果，并让范围 DML 在单个 statement guard 内原子执行；这与目标
  pull/cursor 模型存在明确的阶段性差异，但避免改变 ReadView、LOB、锁和语句回滚边界。
- 物理计划和值对象属于 SQL optimizer 层，不包含 page、record、MTR 或其它存储内部引用。
- 每个后续优化能力必须通过规则或物理实现扩展，不允许把访问路径选择重新塞回 Binder。
