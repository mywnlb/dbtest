# MiniMySQL MySQL 8.0 风格 Query Optimizer 模块设计

版本：2026-06-05  
实现语言：Java  
参考基线：MySQL 8.0.46 Optimization、EXPLAIN、Optimizer Trace、Optimizer Statistics、Range Optimization、Join Optimization  
关联设计：[mysql-parser-binder-design.md](mysql-parser-binder-design.md)、[mysql-statistics-analyze-design.md](mysql-statistics-analyze-design.md)、[mysql-prepared-statement-plan-cache-design.md](mysql-prepared-statement-plan-cache-design.md)、[mysql-sql-executor-storage-api-design.md](mysql-sql-executor-storage-api-design.md)、[mysql-advanced-executor-operators-design.md](mysql-advanced-executor-operators-design.md)、[mysql-data-dictionary-ddl-design.md](mysql-data-dictionary-ddl-design.md)、[innodb-storage-engine-overview.md](innodb-storage-engine-overview.md)、[innodb-btree-design.md](innodb-btree-design.md)、[innodb-record-design.md](innodb-record-design.md)、[innodb-transaction-mvcc-design.md](innodb-transaction-mvcc-design.md)

## 1. 目标与边界

本设计面向 MiniMySQL 的 SQL 查询优化器模块。它接收绑定后的 SQL statement 和 `QueryBlock`，读取 Data Dictionary 元数据与统计信息，执行规则改写、范围提取、访问路径枚举、基数估算、代价估算、join 顺序选择和物理计划构造，并把 `PhysicalPlan` 交给 SQL Executor 执行。

设计目标：

- 高内聚：逻辑改写、统计信息、range optimizer、access path、join order、cost model、trace/explain 各自独立。
- 低耦合：Optimizer 只读取不可变元数据和统计快照，不直接访问 B+Tree page、BufferFrame、RecordCursor、LockManager 或文件。
- MySQL 8.0 风格：覆盖 range access、ref/eq_ref、covering index、index condition pushdown、filesort、nested loop、hash join、optimizer trace、EXPLAIN。
- 可落地：第一阶段支持单表和有限多表 inner join，先实现 left-deep plan、启发式 pruning 和可解释的 cost model。
- 可观测：每次优化过程都能输出 optimizer trace、EXPLAIN traditional/json/tree 视图。
- 并发清晰：优化过程只持有 MDL 已授予状态下的 dictionary pin 和 statistics snapshot，不参与行锁和 page latch 死锁域。

非目标：

- 不实现 SQL parser 和 name binding；输入必须已经由 Binder 解析。
- 不执行 SQL，不读取数据页，不产生 undo/redo。
- 第一阶段不支持复杂子查询改写、CTE materialization、window function、完整 semijoin 策略、完整 histogram 持久化更新。
- 不实现权限、hint 全语法、partition pruning、fulltext/spatial optimizer。
- 不生成 Java 源码。本文件只定义设计、关系和数据流。

## 2. MySQL 8.0 参考依据

本设计参考 MySQL 8.0 的以下优化器行为：

- MySQL optimizer 负责选择执行计划，`EXPLAIN` 可展示执行计划、join 顺序、访问类型、使用的 key、估算 rows 和 filtered。
- `EXPLAIN ANALYZE` 会执行语句并输出 iterator tree、估算 cost/rows 与实际耗时/行数。
- Range optimizer 将 WHERE 条件转换为单个索引上的一个或多个 index interval，支持单列和多列索引范围。
- MySQL 可以使用 index 满足 `ORDER BY`，不能使用时执行 filesort；有 LIMIT 时排序策略可能不同。
- MySQL 使用统计信息和 histogram 估算选择率；`column_statistics` 保存列直方图信息。
- Optimizer trace 通过 session 变量启用，并可在 `INFORMATION_SCHEMA.OPTIMIZER_TRACE` 查看。
- MySQL join 执行以 nested-loop 及其变体为基础；MySQL 8.0.18 起引入 hash join，8.0.20 后 hash join 替代 block nested loop 的相关场景。
- Index Condition Pushdown 可把部分索引可判断条件下推到存储引擎，减少回表或上层过滤成本。

简化点：

| MySQL 8.0 能力 | MiniMySQL 第一阶段 |
| --- | --- |
| 完整 Hypergraph optimizer | 先实现 QueryBlock 级 left-deep join order |
| 完整 histogram 持久化 | 先读取 DD/统计快照，更新由后续 ANALYZE 模块处理 |
| 完整 optimizer_switch 和 hint | 先支持核心开关：range、ICP、hash join、filesort |
| 完整 semijoin/subquery 策略 | 第一阶段只标记 unsupported 或 fallback materialize 扩展点 |
| 完整 EXPLAIN ANALYZE | 先支持 executor iterator runtime metrics 回填 |
| 复杂外连接和视图合并 | 第一阶段只支持 inner join 和简单 derived table 边界 |

## 3. 总体架构

架构图见 [query-optimizer-architecture.mmd](diagrams/query-optimizer-architecture.mmd)。

优化器输入输出：

- 输入：`BoundStatement`、`QueryBlock`、`TableBinding`、`PredicateSet`、session optimizer switches。
- 读取：`DataDictionaryService` 的 immutable table/index definition，`StatisticsProvider` 的 snapshot。
- 输出：Executor 可执行的 `PhysicalPlan`，以及可选的 `OptimizerTrace` 和 `ExplainPlan`。

核心流程：

1. 规范化 query block。
2. 构造 logical plan。
3. 做规则改写和谓词简化。
4. 从 predicate 中提取 SARGable range。
5. 枚举每个 table 的 access path。
6. 读取统计信息并估算 rows/filter/cost。
7. 多表时选择 join order 和 join algorithm。
8. 选择 ORDER BY / LIMIT / filesort 策略。
9. 构造 physical plan。
10. 记录 trace 和 EXPLAIN 信息。

## 4. 包与职责

| 包 | 职责 | 依赖 | 主要模式 |
| --- | --- | --- | --- |
| `sql.opt.api` | `QueryOptimizer`、`OptimizerResult`、`ExplainService` | logical, physical | Facade |
| `sql.opt.logical` | `LogicalPlan`、`RelNode`、`QueryBlock` | exec bind | Composite |
| `sql.opt.rewrite` | 常量折叠、谓词规范化、等价类、投影裁剪 | logical | Rule Engine |
| `sql.opt.range` | SARGable 条件识别、range interval 构造 | dd index | Strategy |
| `sql.opt.access` | access path 枚举、covering index、ICP 候选 | range, cost | Factory, Strategy |
| `sql.opt.join` | join graph、join order、join algorithm 选择 | cost, access | Dynamic Programming, Greedy |
| `sql.opt.cost` | rows、filtered、IO/CPU/memory cost | stats | Strategy |
| `sql.opt.stats` | `StatisticsProvider` adapter，只读取 `OptimizerStatsSnapshot` | sql.stats.snapshot | Snapshot, Adapter |
| `sql.opt.physical` | PhysicalPlan、executor PlanNode 映射 | exec plan | Builder, Adapter |
| `sql.opt.trace` | optimizer trace、EXPLAIN traditional/json/tree | physical | Observer, Visitor |
| `sql.opt.config` | optimizer switch、hint、预算、超时 | session | Policy |

## 5. 核心领域模型

类关系图见 [query-optimizer-class-relation.mmd](diagrams/query-optimizer-class-relation.mmd)。

### 5.1 计划对象

| 对象 | 职责 |
| --- | --- |
| `QueryBlock` | 一个 SELECT/UPDATE/DELETE 查询块，包含 from、where、order、limit |
| `LogicalPlan` | 逻辑关系树，不绑定具体 access path |
| `RelNode` | scan、filter、project、join、sort、limit 等逻辑节点 |
| `PhysicalPlan` | Executor 可执行计划，包含 physical operators 和 access path |
| `PhysicalOperator` | index scan、table scan、nested-loop join、hash join、filesort |
| `PlanProperty` | 输出列、排序性、唯一性、row estimate、cost |

### 5.2 优化对象

| 对象 | 职责 |
| --- | --- |
| `PredicateSet` | 规范化谓词集合，拆分 conjunct、disjunct、join predicate |
| `EquivalenceClass` | `a=b`、`b=c` 这类等价关系 |
| `RangeInterval` | 单个索引上的 key range，含 open/closed bound |
| `AccessPath` | table/index 访问方式、range、是否覆盖、是否 ICP |
| `JoinGraph` | table 节点和 join predicate 边 |
| `JoinOrderPlan` | join 顺序、每步 access path、join algorithm |
| `CostEstimate` | IO、CPU、memory、row count、startup cost、total cost |
| `CardinalityEstimate` | rows、filtered、selectivity、confidence |

### 5.3 统计对象

| 对象 | 职责 |
| --- | --- |
| `StatisticsSnapshot` | 当前 optimization 使用的不可变统计快照 |
| `TableStatistics` | table row count、page count、avg row length |
| `IndexStatistics` | index cardinality、distinct prefix、height、leaf pages |
| `ColumnHistogram` | 等宽或等频 histogram、NULL 比例、top values |
| `CorrelationHint` | 多列相关性扩展点 |

## 6. 关键数据结构与物理/逻辑区分

### 6.1 逻辑计划与物理计划

| 层面 | 对象 | 是否可执行 | 说明 |
| --- | --- | --- | --- |
| 逻辑 SQL | AST、BoundStatement | 否 | parser/binder 输出 |
| 逻辑关系 | `LogicalPlan`、`RelNode` | 否 | 描述关系代数，不含具体索引 |
| 候选路径 | `AccessPath`、`JoinOrderPlan` | 否 | 可被 cost model 比较 |
| 物理计划 | `PhysicalPlan`、`PhysicalOperator` | 是 | Executor 可映射为 PlanNode |
| 存储访问 | `IndexAccessPlan` | 是，间接 | 交给 Storage Engine 执行 |

### 6.2 AccessPath 类型

| 类型 | MySQL EXPLAIN 类比 | 说明 |
| --- | --- | --- |
| `CONST` | const | 主键/唯一键等值且最多一行 |
| `EQ_REF` | eq_ref | join 内表唯一键等值 |
| `REF` | ref | 非唯一索引等值 |
| `RANGE` | range | 单索引区间扫描 |
| `INDEX_FULL_SCAN` | index | 扫描整个索引，可避免回表或排序 |
| `TABLE_FULL_SCAN` | ALL | 聚簇索引全表扫描 |
| `COVERING_INDEX` | Using index | 二级索引覆盖所需列 |
| `ICP_RANGE` | Using index condition | 索引条件下推 |

## 7. 优化阶段与算法

优化流程见 [query-optimizer-plan-flow.mmd](diagrams/query-optimizer-plan-flow.mmd)。

### 7.1 规则改写

第一阶段规则：

- 常量折叠：`1 + 2` 转为 `3`。
- 谓词规范化：`a = 1 AND b > 2` 拆成 conjunct。
- 等价类推导：`t1.a = t2.a AND t2.a = 1` 推导 `t1.a = 1`。
- 无效谓词消除：`WHERE TRUE` 删除，`WHERE FALSE` 转空结果。
- 投影裁剪：只保留 executor 和 storage 需要的列。
- LIMIT 归一化：负数或非法值在 bind 阶段处理，optimizer 只处理合法 limit。

不做的规则：

- 子查询半连接改写。
- view merge。
- outer join reorder。
- window/aggregate rewrite。

### 7.2 Range Optimizer

访问路径流程见 [query-optimizer-access-path-flow.mmd](diagrams/query-optimizer-access-path-flow.mmd)。

Range 提取规则：

- 单列索引支持 `=`, `<=>`, `IN`, `IS NULL`, `IS NOT NULL`, `<`, `<=`, `>`, `>=`。
- 多列索引按 key part 顺序提取连续前缀；遇到 range key part 后停止使用后续 key part 作为 range bound。
- `LIKE 'abc%'` 可转换为前缀范围，普通 `%abc` 不可转换。
- OR 条件第一阶段只支持同一索引上的 range union；跨索引 index merge 作为扩展点。
- 非 SARGable 条件保留给 Executor filter 或 ICP。

### 7.3 Access Path 枚举

枚举顺序：

1. 主键/唯一索引等值路径。
2. 普通索引 ref 路径。
3. range 路径。
4. covering index scan。
5. order-preserving index scan。
6. clustered full scan。

裁剪规则：

- 禁用的 optimizer switch 不生成对应路径。
- 如果 path 不满足 required columns 且无法回表，则丢弃。
- 如果 path 估算 rows 大于 table scan 且无排序优势，可降权。
- 如果 index order 满足 ORDER BY，可降低 filesort cost。

### 7.4 Join Order

Join order 流程见 [query-optimizer-join-order-flow.mmd](diagrams/query-optimizer-join-order-flow.mmd)。

第一阶段策略：

- 单表 query 跳过 join optimizer。
- `tableCount <= 5` 使用 left-deep dynamic programming。
- `tableCount > 5` 使用 greedy search 加 pruning。
- 只支持 inner join reorder。
- outer join、semijoin、anti join 保留原顺序或标记 unsupported。

Join algorithm：

| 算法 | 适用 | 第一阶段 |
| --- | --- | --- |
| Nested Loop Join | 任意 inner join | 默认支持 |
| Index Nested Loop Join | 内表有 ref/eq_ref/range 路径 | 支持 |
| Hash Join | 等值 join 且 build side 可内存容纳 | Optimizer 输出算法选择，由 [mysql-advanced-executor-operators-design.md](mysql-advanced-executor-operators-design.md) 执行 |
| Batched Key Access | 内表索引 lookup 可批量 | 扩展点 |

### 7.5 ORDER BY、LIMIT 与 filesort

策略：

- 如果 chosen access path 的输出顺序满足 ORDER BY，避免 filesort。
- 如果 join 顺序破坏排序，使用 `FilesortOperator`。
- LIMIT 可降低 filesort 估算成本。
- 覆盖索引扫描如果同时满足 WHERE 和 ORDER BY，可优先保留。

### 7.6 Cost Model

成本组成：

| 成本 | 说明 |
| --- | --- |
| `ioCost` | page read、random/sequential access 估算 |
| `cpuCost` | predicate、comparison、row materialization |
| `memoryCost` | sort buffer、hash table、join buffer |
| `startupCost` | 生成第一行前成本 |
| `totalCost` | 完整执行成本 |

估算公式第一阶段保持简单：

`totalCost = ioCost + cpuCost + memoryPenalty + sortCost + joinCost`

`filteredRows = inputRows * selectivity`

选择率来源优先级：

1. 唯一约束和主键。
2. index cardinality。
3. column histogram。
4. table row count。
5. 默认选择率常量。

## 8. 统计信息设计

`StatisticsProvider` 只读统计信息，统计生命周期以 [mysql-statistics-analyze-design.md](mysql-statistics-analyze-design.md) 为准：

- 从 `sql.stats` 读取 table/index/column statistics snapshot。
- 可读取 StorageEngine estimator 暴露的只读估算结果，但不触发采样或持久化。
- 生成 statement 内不可变 `StatisticsSnapshot` 或适配 `OptimizerStatsSnapshot`。
- snapshot 与 dictionary version 绑定，DDL 发布新版本后旧 snapshot 不再复用。

统计更新不属于 Optimizer 职责。`mysql-statistics-analyze-design.md` 负责 `ANALYZE TABLE`、采样、持久化、cache publish 和 invalidation。

并发规则：

- snapshot 创建时只短持 statistics cache mutex。
- optimization 期间只读 snapshot，不等待 storage IO。
- statistics cache miss 可以返回默认估算，不允许在 optimizer 中扫描真实表。

## 9. EXPLAIN 与 Optimizer Trace

EXPLAIN 流程见 [query-optimizer-explain-flow.mmd](diagrams/query-optimizer-explain-flow.mmd)。

### 9.1 EXPLAIN

输出字段第一阶段：

- `id`
- `select_type`
- `table`
- `type`
- `possible_keys`
- `key`
- `key_len`
- `ref`
- `rows`
- `filtered`
- `Extra`

`Extra` 支持：

- `Using where`
- `Using index`
- `Using index condition`
- `Using filesort`
- `Using temporary`
- `Impossible WHERE`

### 9.2 EXPLAIN ANALYZE

设计为 Executor 回填：

- Optimizer 输出估算 rows/cost。
- Executor 执行并记录 actual rows、loops、first row time、total time。
- `ExplainFormatter` 合并估算和实际指标。

### 9.3 Optimizer Trace

Trace 阶段：

- `join_preparation`
- `condition_processing`
- `range_analysis`
- `considered_access_paths`
- `join_optimization`
- `order_by_optimization`
- `chosen_plan`

Trace 是 session scoped，不持久化到 DD；`INFORMATION_SCHEMA.OPTIMIZER_TRACE` 由 adapter 读取最近 trace buffer。

## 10. 与其它模块的协作

### 10.1 与 SQL Executor

- Executor 调用 `QueryOptimizer.optimize()`。
- Optimizer 返回 `PhysicalPlan`，Executor 映射为 PlanNode tree。
- EXPLAIN ANALYZE 由 Executor 收集实际执行指标。
- Optimizer 不打开 StorageCursor。

### 10.2 与 Data Dictionary

- Optimizer 读取 `TableDefinition`、`IndexDefinition`、column metadata 和 statistics。
- Optimizer 不修改 DD。
- DDL 发布新 schema version 后，旧 plan cache entry 失效。

### 10.3 与 Storage Engine

- Optimizer 可调用 `StorageEngine.estimateAccess()` 获取只读估算。
- Storage estimate 不能获取 page latch 或触发真实 IO。
- B+Tree 只提供 index height、leaf pages、可覆盖列、排序方向等元信息。

### 10.4 与 Transaction / Lock

- Optimizer 不创建 ReadView，不获取 row lock。
- Optimizer 可读取 session isolation 决定 locking read 是否需要保留 order/range 语义。
- Row lock 范围最终由 Executor/Storage Engine 根据 chosen access path 和 read intent 执行。

## 11. 并发与锁顺序

并发状态图见 [query-optimizer-concurrency-state.mmd](diagrams/query-optimizer-concurrency-state.mmd)。

### 11.1 锁和等待对象

| 对象 | 所属模块 | Optimizer 是否持有 | 死锁域 |
| --- | --- | --- | --- |
| MDL ticket | MDL | 已由 Binder/Executor 获取，Optimizer 借用 | MetadataWaitGraph，但 optimizer 不等待 |
| Dictionary pin | DD cache | 是，只读 pin | 不进入死锁图 |
| Statistics snapshot | Optimizer stats | 是，只读 snapshot | 不进入死锁图 |
| Statistics cache mutex | Optimizer stats | 短持有 | timeout/retry |
| Optimizer trace buffer | session trace | 短持有 | 不进入死锁图 |
| Row lock | Transaction | 否 | InnoDB WaitForGraph |
| Page latch / MTR latch | Storage | 否 | timeout/retry |
| Physical file lock | Disk | 否 | timeout/error |

### 11.2 状态与持有变化

| 状态 | 持有者 | 持有资源 | 进入条件 | 退出条件 |
| --- | --- | --- | --- | --- |
| `CONTEXT_CREATED` | Executor/Optimizer | optimizer context | statement 已绑定 | 使用 MDL 和 DD pin |
| `MDL_HELD` | session | 已授予 MDL ticket | Binder 获取 metadata lock | pin dictionary objects |
| `DICT_PINNED` | optimizer context | immutable table/index definition | table binding 完成 | 打开 statistics snapshot |
| `STATS_SNAPSHOT` | optimizer context | immutable stats snapshot | 统计快照创建 | 进入优化阶段 |
| `OPTIMIZING` | optimizer | logical plan、candidate paths | rewrite 和 path 枚举 | 选择 physical plan |
| `TRACE_WRITING` | optimizer trace | session trace buffer | 阶段结束或选择路径 | trace 记录完成 |
| `PLAN_READY` | optimizer | physical plan | 最优候选确定 | 释放 snapshot |
| `ABORTED` | optimizer | cleanup 权 | timeout、kill、unsupported feature | 释放 snapshot 和 trace scope |

持有变化规则：

- `stats snapshot acquire`：只短持 statistics cache mutex，不能等待 storage IO。
- `optimize`：只读 dictionary pin 和 stats snapshot，不持有 row lock、page latch、MTR latch、redo wait 或文件锁。
- `trace write`：trace buffer 是 session 内存，不反向访问 DD repository 或 Storage Engine。
- `plan return`：返回 PhysicalPlan 后 Optimizer 释放 snapshot；MDL 和 dictionary pin 生命周期由 Executor 管理。
- `abort cleanup`：优化失败只清理 optimizer 内部对象，不触碰事务锁。

## 12. 异常处理

异常类型：

- `OptimizerException`
- `UnsupportedOptimizationFeatureException`
- `NoValidAccessPathException`
- `StatisticsUnavailableException`
- `OptimizerTimeoutException`
- `OptimizerKilledException`
- `CostOverflowException`
- `InvalidPlanPropertyException`
- `ExplainFormatException`

异常策略：

- 无有效访问路径：返回明确异常或 fallback table scan，取决于语句类型和配置。
- 统计缺失：使用默认估算并在 trace 中记录低置信度。
- 优化超时：返回当前 best plan，若没有 best plan 则抛出 `OptimizerTimeoutException`。
- unsupported join/subquery：由 planner 返回不支持错误，不静默改写成错误语义。
- cost 溢出：使用 saturating cost，并在 trace 标记。

## 13. API 设计

### 13.1 QueryOptimizer

- `optimize(BoundStatement, OptimizerContext)`
- `optimizeQuery(QueryBlock, OptimizerContext)`
- `optimizeDml(BoundDmlStatement, OptimizerContext)`
- `explain(BoundStatement, ExplainMode, OptimizerContext)`

### 13.2 RuleRewriteEngine

- `rewrite(LogicalPlan, OptimizerContext)`
- `normalizePredicates(PredicateSet)`
- `deriveEquivalenceClasses(PredicateSet)`
- `pushDownPredicates(LogicalPlan)`
- `pruneProjections(LogicalPlan)`

### 13.3 AccessPathEnumerator

- `enumerate(TableBinding, PredicateSet, RequiredProperty)`
- `candidateIndexes(TableDefinition)`
- `isCovering(IndexDefinition, RequiredColumnSet)`
- `attachIndexCondition(AccessPath, PredicateSet)`

### 13.4 JoinOrderOptimizer

- `buildJoinGraph(QueryBlock)`
- `chooseJoinOrder(JoinGraph, AccessPathSet, OptimizerContext)`
- `dynamicProgramming(JoinGraph)`
- `greedySearch(JoinGraph)`

### 13.5 CostModel

- `estimateTableScan(TableStatistics)`
- `estimateIndexRange(IndexStatistics, RangeIntervalSet)`
- `estimatePredicate(Predicate, StatisticsSnapshot)`
- `estimateJoin(JoinOrderPlan)`
- `estimateSort(RowCount, RowWidth, Limit)`

### 13.6 ExplainService

- `formatTraditional(PhysicalPlan)`
- `formatJson(PhysicalPlan, OptimizerTrace)`
- `formatTree(PhysicalPlan, RuntimeMetrics)`
- `recordAnalyzeMetrics(PhysicalPlan, RuntimeMetrics)`

## 14. 设计模式使用清单

| 模式 | 应用点 | 价值 |
| --- | --- | --- |
| Facade | `QueryOptimizer` | 隐藏 rewrite/range/join/cost 子系统 |
| Composite | logical/physical plan tree | 表达关系树和物理 operator |
| Strategy | access path、join algorithm、cost formula | 支持不同优化策略 |
| Factory | `AccessPathEnumerator` | 为 table/index 生成候选路径 |
| Snapshot | `StatisticsSnapshot` | statement 内稳定统计视图 |
| Visitor | `ExplainFormatter` | 遍历 plan tree 输出不同格式 |
| Observer | `OptimizerTraceRecorder` | 记录阶段和选择过程 |
| Policy | `OptimizerSwitch`、timeout、DP limit | 控制优化预算 |
| Adapter | PhysicalPlan 到 Executor PlanNode | 隔离 optimizer 和 executor |

## 15. 高内聚、低耦合约束

- Optimizer 不执行计划，不打开 cursor。
- Optimizer 不修改 Data Dictionary 和统计信息。
- Optimizer 不读取 page、record、undo、redo。
- Cost model 只依赖统计快照和配置参数。
- Range optimizer 只处理索引 key interval，不判断 MVCC 可见性。
- Join optimizer 只选择顺序和算法，不持有任何行锁。
- EXPLAIN ANALYZE 的实际指标由 Executor 提供，Optimizer 只负责合并展示。

## 16. 典型数据流

### 16.1 单表 SELECT

1. Executor/Binder 提供 `BoundSelect` 和 table binding。
2. Optimizer 构造 logical scan + filter + project。
3. Range optimizer 从 WHERE 提取 primary/ref/range 候选。
4. Access path enumerator 生成 index scan、range scan、table scan。
5. Cost model 选择最低成本路径。
6. PhysicalPlan 输出给 Executor。

### 16.2 多表 JOIN

1. 构造 join graph。
2. 对每个 table 生成本地 access path。
3. 推导 join predicate selectivity。
4. DP 或 greedy 选择 left-deep join order。
5. 选择 nested loop、index nested loop 或 hash join。
6. 输出 physical join tree。

### 16.3 ORDER BY / LIMIT

1. 检查 access path 是否满足排序属性。
2. 如果满足，保留 order-preserving plan。
3. 如果不满足，插入 filesort operator。
4. LIMIT 降低排序估算成本，并可作为 executor hint。

### 16.4 EXPLAIN

见 [query-optimizer-explain-flow.mmd](diagrams/query-optimizer-explain-flow.mmd)。关键边界是 EXPLAIN 只运行 optimizer，EXPLAIN ANALYZE 才执行 plan 并回填实际指标。

## 17. 测试设计

- 规则改写测试：常量折叠、谓词拆分、等价类推导、空结果检测、投影裁剪。
- Range 测试：单列范围、多列前缀、IN、IS NULL、LIKE prefix、OR range union。
- Access path 测试：const、eq_ref、ref、range、covering index、ICP、table scan。
- Cost 测试：主键等值成本低于 table scan，filesort cost 受 LIMIT 影响，统计缺失 fallback。
- Join 测试：2-5 表 DP join order，大表 greedy pruning，index nested loop 选择。
- ORDER BY 测试：索引满足排序、filesort 插入、覆盖索引排序。
- Trace 测试：trace 阶段完整，低置信度统计记录，chosen plan 记录。
- EXPLAIN 测试：traditional/json/tree 输出字段一致。
- 并发测试：统计 snapshot 与 DDL version 绑定，optimizer 不等待 row lock/page latch。
- 集成测试：Optimizer 输出的 PhysicalPlan 可被 Executor 执行。

## 18. 后续实现顺序

1. `sql.opt.logical`：LogicalPlan、RelNode、PredicateSet。
2. `sql.opt.config`：optimizer switch、timeout、DP limit。
3. `sql.opt.stats`：StatisticsSnapshot、TableStatistics、IndexStatistics。
4. `sql.opt.rewrite`：常量折叠、谓词规范化、等价类。
5. `sql.opt.range`：RangeInterval 和 SARGable 提取。
6. `sql.opt.access`：单表 access path 枚举。
7. `sql.opt.cost`：基础 IO/CPU/row count 代价模型。
8. `sql.opt.physical`：PhysicalPlan builder 和 Executor adapter。
9. EXPLAIN traditional 输出。
10. ORDER BY / LIMIT plan property。
11. JoinGraph 和 2 表 join order。
12. DP left-deep join order。
13. Greedy join order 和 pruning。
14. OptimizerTrace 和 JSON/TREE EXPLAIN。
15. EXPLAIN ANALYZE runtime metrics 合并。

## 19. 十五轮自检记录

| 轮次 | 检查主题 | 检查结果 |
| --- | --- | --- |
| 1 | 是否只写设计文档 | 本文只新增设计说明和 Mermaid 图，没有生成 Java 实现代码 |
| 2 | 目标与非目标 | 已明确 Optimizer 目标，排除 parser、执行、完整 hypergraph、复杂子查询等范围 |
| 3 | MySQL 8.0 贴合 | 已覆盖 optimization、range、join、ORDER BY、ICP、statistics、EXPLAIN、trace |
| 4 | 高内聚 | rewrite、range、access、join、cost、stats、trace 子系统职责独立 |
| 5 | 低耦合 | Optimizer 只读 DD/统计快照，不访问 page、record、LockManager、redo |
| 6 | 面向对象 | 已定义 QueryBlock、LogicalPlan、AccessPath、JoinGraph、CostEstimate 等对象 |
| 7 | 设计模式 | 已列出 Facade、Composite、Strategy、Factory、Snapshot、Visitor、Observer 等 |
| 8 | 核心领域模型 | 已覆盖计划对象、优化对象和统计对象 |
| 9 | 依赖方向 | 已给出 BoundStatement 到 Optimizer 到 PhysicalPlan 到 Executor 的单向链路 |
| 10 | 物理与逻辑区分 | 已区分逻辑计划、候选路径、物理计划和存储访问计划 |
| 11 | 关键数据流 | 已给出单表 SELECT、多表 JOIN、ORDER BY/LIMIT、EXPLAIN 流程 |
| 12 | 图示 | 已提供架构图、类关系图、优化流程图、访问路径图、join order 图、并发状态图、EXPLAIN 图 |
| 13 | 并发锁状态 | 已说明 Optimizer 只持有 MDL 借用、DD pin、statistics snapshot 和 trace buffer |
| 14 | 异常与恢复 | 已给出统计缺失、优化超时、unsupported、cost overflow 的处理策略 |
| 15 | 测试与顺序 | 已给出测试设计、后续实现顺序，并确认没有未完成标记或空白项 |

## 20. 参考链接

- MySQL 8.0 Reference Manual - Optimization: https://dev.mysql.com/doc/refman/8.0/en/optimization.html
- MySQL 8.0 Reference Manual - EXPLAIN Statement: https://dev.mysql.com/doc/refman/8.0/en/explain.html
- MySQL 8.0 Reference Manual - Tracing the Optimizer: https://dev.mysql.com/doc/refman/8.0/en/optimizer-tracing.html
- MySQL 8.0 Reference Manual - Optimizer Statistics: https://dev.mysql.com/doc/mysql/8.0/en/optimizer-statistics.html
- MySQL 8.0 Reference Manual - Range Optimization: https://dev.mysql.com/doc/refman/8.0/en/range-optimization.html
- MySQL 8.0 Reference Manual - Nested-Loop Join Algorithms: https://dev.mysql.com/doc/refman/8.0/en/nested-loop-joins.html
- MySQL 8.0 Reference Manual - Hash Join Optimization: https://dev.mysql.com/doc/refman/8.0/en/hash-joins.html
- MySQL 8.0 Reference Manual - ORDER BY Optimization: https://dev.mysql.com/doc/refman/8.0/en/order-by-optimization.html
- MySQL 8.0 Reference Manual - Index Condition Pushdown Optimization: https://dev.mysql.com/doc/refman/8.0/en/index-condition-pushdown-optimization.html
