# Slice: SQL Calcite-Lite M3（布尔表达式与安全 Residual）

依据：`mysql-parser-binder-design.md` §6/§7/§8、
`mysql-query-optimizer-design.md` §7、
`mysql-sql-executor-storage-api-design.md` §7，以及
`2026-07-23-calcite-lite-sql-planning.md` ADR。

## 目标

在 M2 sealed expression 和规则固定点之上接入括号、`OR`、`NOT`、
`IS NULL` 与 `IS NOT NULL`，保持 SQL 三值语义和确定性访问路径。

生产链保持：

`boolean AST -> semantic BoundExpression -> PredicateSet -> RuleProgram
-> PredicateAnalyzer -> HeuristicAccessPathSelector -> PhysicalPlan -> residual`。

## 关键决策

- Parser 使用 `NOT > AND > OR` 递归下降；BETWEEN 内部 AND 由原子谓词消费。
- statement AST 用单一 `BooleanExpressionNode condition`，删除旧 predicate list 双轨。
- Bound IR 增加 disjunction、negation、null-test；`SqlBoolean` 增加 OR/NOT。
- 统一 bottom-up expression rewriter 负责 sealed child 遍历，规则只处理当前节点。
- 默认规则保持 16-pass，上线 AND/OR 展平、comparison 规范化和三值折叠。
- 不实施 De Morgan、CNF、谓词重排或去重。
- Analyzer 只从最外层正向 AND comparison 提取安全约束。
- OR、NOT、null-test 是 M3 residual barrier，不是 optimization error。
- 一致性 SELECT 可用已证明完整唯一键 point 定位，再执行额外 residual。
- locking read 继续走 range；point UPDATE/DELETE 仍要求整个条件是精确主键 equality。
- OR 根、NOT 根和单独 null-test 退化为聚簇 full scan。
- M3 不把 SQL NULL 编为 B+Tree endpoint，也不实现 OR range union。
- 物理计划递归校验 canonical boolean residual；point key 不能从 OR/NOT 内取证。
- adapter 在完整聚簇行上求值 AND/OR/NOT/null-test，只有 TRUE 命中。

## 非目标

- 不增加 IN、LIKE、函数、算术、列间比较、`<=>`、子查询或 JOIN。
- 不增加 ORDER BY、LIMIT、memo、cost、statistics、trait、EXPLAIN 或 trace。
- 不迁移 ReadView、LOB hydration 或 residual evaluator 到 pull-based Executor。
- 不修改事务、锁、redo/undo、页格式、索引格式或持久化协议。

## 验收测试

- Parser 覆盖优先级、括号、连续 NOT、BETWEEN/AND、null-test 与错误位置。
- expression/rule 覆盖完整三值真值表、nullable、bottom-up 固定点和规范化。
- Binder 证明 OR 分支可重复引用同列，正向 AND 重复 equality 仍拒绝。
- Optimizer 证明 point+opaque residual、OR 根 full scan 和 DML residual 保留。
- 物理计划拒绝仅由 OR 分支证明的 point key。
- adapter/Session 覆盖 RR/RC/RU、locking read、nullable external LOB 与 range DML。
- 固定 JDK 25.0.2 + Gradle 9.5.1 全量测试通过且测试数不倒退。

## Current map 更新要求

- Parser/Binder、expression/rule/analyzer 和 adapter evaluator 改为 M3 真实接线。
- OR union、NULL endpoint、pull cursor、cost/statistics 继续标为明确 gap。
- 新生产类型逐项核对调用方；无调用方时进入 Reserved / Unwired 表。
- 按 current map 十项清单重新核对实线、状态、依赖和测试证据。
