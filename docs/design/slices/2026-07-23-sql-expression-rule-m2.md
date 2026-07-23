# Slice: SQL Calcite-Lite M2（表达式 IR 与规则固定点）

依据：`mysql-parser-binder-design.md` §5/§7/§8、
`mysql-query-optimizer-design.md` §5/§7、
`mysql-sql-executor-storage-api-design.md` §7/§8，以及
`2026-07-23-calcite-lite-sql-planning.md` ADR。

## 目标

把 M1 的 `List<BoundRowPredicate>` 升级为可演进的 sealed `BoundExpression`，
并在 logical plan 与访问路径选择之间加入有界 `RuleProgram`。

生产链变为：

`semantic BoundExpression -> LogicalPlan/PredicateSet -> RuleProgram fixed point
-> PredicateAnalyzer -> HeuristicAccessPathSelector -> PhysicalPlan`。

## 关键决策

- `SqlValue` 下移到 `sql.type`，Binder、Optimizer、Executor 共享值语义而不反向依赖执行包。
- M2 表达式封闭为 column、literal、comparison、AND 和 truth literal；每个列引用携带
  stable column id、ordinal、exact DD type 与源位置。
- `PredicateSet.condition` 是唯一权威；conjunct 与引用列只从 condition 派生，不接收重复状态。
- `SqlBoolean` 显式表达 TRUE/FALSE/UNKNOWN；WHERE 只有 TRUE 命中。
- 默认规则按固定顺序执行 AND 展平、literal-column 规范化与安全三值折叠。
- `RuleProgram` 最多 16 个完整 pass，并拒绝伪 changed、结构循环和不收敛。
- 规则只做不依赖 collation、函数副作用或存储状态的等价改写。
- `PredicateAnalyzer` 只派生安全 equality/range/empty 证明，不改写表达式或选择索引。
- `HeuristicAccessPathSelector` 保持 M1 确定性选路与 stable-id tie-break。
- 每个 SELECT 物理计划携带完整 residual；point key 必须由相同 typed equality 证明。
- adapter 在 MVCC/current-read 选出完整聚簇行后统一求值 residual，RU 聚簇点查也不例外。
- 旧 `BoundRowPredicate` 与 `BoundRowPredicateOperator` 删除，不保留兼容双轨。

## 非目标

- 不增加 OR/NOT/IN/LIKE/IS NULL、函数、算术、join、aggregate、ORDER BY 或 LIMIT 语法。
- 不实现 memo、cost、statistics、trait、EXPLAIN、trace 或 plan cache。
- 不把 residual evaluator、ReadView、LOB hydration 移入 pull-based Executor。
- 不修改 XA、savepoint、DDL、事务、锁、redo/undo 或持久格式。

## 验收测试

- 表达式构造测试覆盖 exact type、nullable、源位置与 SQL 三值真值表。
- 规则测试覆盖固定点引用稳定、AND 展平、comparison 反转、NULL 折叠与循环拒绝。
- Binder 测试证明 SELECT/UPDATE/DELETE 输出完整 condition 且保持用户 conjunction 顺序。
- Optimizer 测试证明 point/range/full-scan/empty/DML 选择与 M1 行为一致。
- 物理计划拒绝 LOB key、未规范 residual 和未由 residual 证明的 point key。
- adapter 集成测试覆盖 RR/RC/RU、primary/secondary、locking/range 与 residual。
- 旧谓词类型和旧 `executor.SqlValue` 在生产/测试中零引用。
- 固定 JDK 25.0.2 + Gradle 9.5.1 全量测试通过且测试数不倒退。

## Current map 更新要求

- Compiler 到 Optimizer 的实线细化为 RuleProgram -> PredicateAnalyzer/AccessPathSelector。
- Package Status 增加 `sql.expression`、`sql.type`、`sql.optimizer.rewrite` 的真实接线状态。
- adapter residual evaluator 标为当前阶段实现；pull cursor/evaluator 迁移保持明确 gap。
- 新生产类型逐项核对调用方；无调用方时必须进入 Reserved / Unwired 表。
