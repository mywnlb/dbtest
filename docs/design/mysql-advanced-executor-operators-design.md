# MiniMySQL MySQL 8.0 风格高级 SQL Executor Operators 模块设计

版本：2026-06-05  
实现语言：Java  
参考基线：MySQL 8.0.46 Join Execution、Hash Join、GROUP BY、Filesort、Internal Temporary Tables、Subquery Optimization、Window Functions  
关联设计：[mysql-sql-executor-storage-api-design.md](mysql-sql-executor-storage-api-design.md)、[mysql-query-optimizer-design.md](mysql-query-optimizer-design.md)、[mysql-parser-binder-design.md](mysql-parser-binder-design.md)、[mysql-prepared-statement-plan-cache-design.md](mysql-prepared-statement-plan-cache-design.md)、[mysql-statistics-analyze-design.md](mysql-statistics-analyze-design.md)、[mysql-data-dictionary-ddl-design.md](mysql-data-dictionary-ddl-design.md)、[innodb-storage-engine-overview.md](innodb-storage-engine-overview.md)

## 1. 目标与边界

本设计面向 MiniMySQL 的高级 SQL 执行算子模块，承接基础 SQL Executor 已保留的 join、aggregation、group by、filesort、temporary table、subquery、derived table、CTE 和 window function 执行能力。基础 Executor 负责 statement 生命周期、单表 DML、Storage API 和事务边界；高级算子负责把 Optimizer 输出的复杂 `PhysicalPlan` 映射为可执行的 iterator tree。

设计目标：

- 高内聚：join、aggregate、sort、temporary table、subquery、window、materialization、runtime metrics 分别收敛在明确子包。
- 低耦合：高级算子只消费 `PhysicalPlan`、`BoundExpression`、`ExecutionContext` 和 Storage API，不访问 B+Tree page、BufferFrame、RecordCursor、redo buffer 或 DD repository。
- MySQL 8.0 风格：对齐 nested-loop join、index nested-loop、hash join、filesort、internal temporary tables、GROUP BY、HAVING、derived table materialization、subquery execution 和 window functions。
- 可落地：第一阶段支持 inner join、hash aggregate、filesort、内存/磁盘临时表、scalar/IN/EXISTS subquery 和基础 window frame。
- 并发安全：明确 operator resource guard、memory quota、spill file lock、temp table handle、StorageCursor、row lock wait 和 MDL/DD pin 的持有变化。
- 可测试：覆盖算法选择、spill、异常 cleanup、并发 deallocate/invalidation、EXPLAIN ANALYZE runtime metrics 和故障注入。

非目标：

- 不选择 join order、access path 或 cost；这些属于 Optimizer。
- 不解析 SQL、不做名称绑定；这些属于 Parser/Binder。
- 不实现 Storage Engine 内部锁、MVCC、undo、redo 或 page latch。
- 不实现完整 server protocol、binlog、replication、trigger、foreign key cascade。
- 不生成 Java 源码。本文件只定义设计、关系和数据流。

## 2. MySQL 8.0 参考依据

本设计参考 MySQL 8.0 的以下行为：

- MySQL 执行计划使用 iterator 风格执行，`EXPLAIN ANALYZE` 可输出 iterator tree 的实际耗时、循环次数和行数。
- MySQL join 执行以 nested-loop 为基础，8.0 支持 hash join，等值 join 在合适场景下可使用 hash join。
- `ORDER BY` 不能由索引顺序满足时使用 filesort；排序可能使用内存 buffer，也可能写临时文件。
- GROUP BY、DISTINCT、UNION、子查询 materialization、window function 等场景可能使用 internal temporary table。
- MySQL 8.0 支持 window functions，包括 `PARTITION BY`、`ORDER BY` 和 frame 子句。
- derived table、CTE、IN/EXISTS subquery 可被 merge、materialize、semijoin 或按 correlated execution 处理；本设计执行 Optimizer 已选择的策略。

简化点：

| MySQL 8.0 能力 | MiniMySQL 第一阶段 |
| --- | --- |
| 完整 outer join / semijoin / antijoin | 先支持 inner join、EXISTS/IN materialization 和 correlated nested loop |
| 完整 hash join executor | 支持内存 hash join 和分区 spill 边界 |
| 完整 TempTable engine | 设计 `TempTableService`，第一阶段可用 row-store temp table |
| 完整 window frame | 先支持 ROWS frame 和常用 ranking/aggregate window 函数 |
| 完整 recursive CTE | 保留 `RecursiveCteExecutor` 扩展点，第一阶段可不启用 |
| 完整 EXPLAIN ANALYZE | 输出 operator runtime metrics，先不做所有 MySQL 字段兼容 |

## 3. 总体架构

架构图见 [advanced-executor-architecture.mmd](diagrams/advanced-executor-architecture.mmd)。

执行链路：

`BoundStatement -> QueryOptimizer -> PhysicalPlan -> AdvancedSqlExecutor -> PlanNode iterator tree -> StorageEngine/TempTable/ResultSet`

职责划分：

- Optimizer 决定 join order、join algorithm、是否 materialize、是否 filesort、是否用 temp table。
- Advanced Executor 按 plan 执行算子，不重新选择计划。
- Storage API 负责真实表扫描、current read、row lock、MVCC 和 DML。
- TempTableService 负责内部临时表、spill file、内存配额和 cleanup。
- ExpressionEvaluator 只求值已绑定表达式。
- RuntimeMetrics 记录 actual rows、loops、time、spill bytes 和 memory peak。

## 4. 包与职责

| 包 | 职责 | 依赖 | 主要模式 |
| --- | --- | --- | --- |
| `sql.exec.advanced.api` | `AdvancedSqlExecutor`、高级执行入口 | plan, runtime | Facade |
| `sql.exec.advanced.join` | nested loop、index nested loop、hash join | storage, expr | Strategy, Iterator |
| `sql.exec.advanced.aggregate` | hash aggregate、sort aggregate、distinct aggregate | expr, temp | Strategy, State |
| `sql.exec.advanced.sort` | filesort、top-n、multi-way merge | temp, memory | Template Method |
| `sql.exec.advanced.temp` | internal temp table、spill run、temp table cleanup | fil/temp storage | Repository, RAII Guard |
| `sql.exec.advanced.subquery` | scalar、IN、EXISTS、correlated subquery | plan, expr | Strategy, Materialization |
| `sql.exec.advanced.window` | partition、order、frame、window function | sort, temp | State, Strategy |
| `sql.exec.advanced.materialize` | derived table、CTE、materialized subplan | temp | Builder, Adapter |
| `sql.exec.advanced.memory` | per-query/operator memory quota、spill policy | runtime | Policy |
| `sql.exec.advanced.metric` | runtime metrics、EXPLAIN ANALYZE 数据 | runtime | Observer |

禁止方向：

- 高级算子不能访问 `BufferFrame`、`PageHandle`、`RecordCursor` 或 `FileChannel`。
- Join/Aggregate/Sort 不能直接获取 MDL 或 DD repository。
- TempTableService 不能解析 SQL AST 或决定优化策略。
- WindowExecutor 不能反向修改 Optimizer plan。

## 5. 核心领域模型

类关系图见 [advanced-executor-class-relation.mmd](diagrams/advanced-executor-class-relation.mmd)。

| 对象 | 职责 |
| --- | --- |
| `AdvancedSqlExecutor` | 执行复杂 `PhysicalPlan` 的门面 |
| `PlanNode` | iterator 接口：`open/next/close` |
| `JoinNode` | join operator 基类 |
| `NestedLoopJoinNode` | outer row 驱动 inner scan |
| `IndexNestedLoopJoinNode` | outer key 驱动 inner index probe |
| `HashJoinNode` | build/probe hash table，支持 spill |
| `AggregateNode` | GROUP BY、aggregate function、HAVING |
| `SortNode` | filesort、top-n、spill run merge |
| `TempTableNode` | materialized intermediate result |
| `SubqueryNode` | scalar/IN/EXISTS/correlated 子查询 |
| `WindowNode` | window partition、frame 和函数状态 |
| `OperatorResourceGuard` | 关闭 child、释放 temp、memory、spill handle |
| `MemoryQuota` | query/operator 内存配额 |
| `SpillRun` | 排序或 hash 分区 spill 文件 |
| `TempTableHandle` | 内部临时表句柄 |
| `RuntimeMetrics` | actual rows、loops、latency、spill、memory |

## 6. 关键数据结构与物理/逻辑区分

### 6.1 Operator Tree

| 层面 | 对象 | 所属模块 | 说明 |
| --- | --- | --- | --- |
| 绑定语义 | `BoundStatement`、`BoundExpression` | Parser/Binder | 已解析名称和类型 |
| 优化结果 | `PhysicalPlan`、`PhysicalOperator` | Optimizer | 指定算子和访问路径 |
| 执行算子 | `PlanNode` tree | Executor | pull 模型执行 |
| 存储访问 | `StorageCursor`、`StorageRowHandle` | Storage API | 屏蔽 B+Tree/page latch |
| 中间结果 | `TempTableHandle`、`MaterializedRow` | Advanced Executor | 内部临时结构 |
| 物理 spill | `SpillRun`、temp file | TempTableService | 只由 temp/spill adapter 访问 |

### 6.2 Row Materialization

`MaterializedRow` 保存：

- projected values。
- row type 和 null bitmap。
- optional sort key。
- optional group key。
- source operator id。

规则：

- `MaterializedRow` 不保存 `StorageRowHandle`。
- 临时表行不包含 page latch、row lock 或事务 undo 信息。
- 大字段第一阶段可以 inline 或引用受控 temp LOB handle；完整 off-page LOB 由后续 LOB 文档细化。

### 6.3 MemoryQuota 与 Spill

内存配额层级：

- query quota。
- operator quota。
- per-row buffer quota。
- temp table quota。

当 operator 超过 quota：

1. SortNode 写 sorted run。
2. HashJoinNode 写 hash partition。
3. AggregateNode 写 partial aggregate temp table。
4. WindowNode materialize partition 到 temp table。

## 7. 核心策略和算法

### 7.1 Join

Join 流程见 [advanced-executor-join-flow.mmd](diagrams/advanced-executor-join-flow.mmd)。

支持算法：

| 算法 | 输入 | 适用 |
| --- | --- | --- |
| `NestedLoopJoinNode` | outer child + inner child | 小表、无可用索引、correlated 子查询 |
| `IndexNestedLoopJoinNode` | outer key + inner index access plan | inner 有 ref/eq_ref/range |
| `HashJoinNode` | build/probe child + equality key | inner join 等值条件 |

Join 执行规则：

- Optimizer 已经决定 join order 和 algorithm。
- JoinNode 只打开 child node，不打开 DD repository。
- 当前读或锁定读的 row lock 由 child StorageNode 处理。
- Hash join build side 不保存 `StorageRowHandle`，只保存 materialized row 和 join key。
- spill partition 按 hash 分区写入 temp file，probe 时逐分区加载。

### 7.2 Aggregation / GROUP BY

聚合流程见 [advanced-executor-aggregate-flow.mmd](diagrams/advanced-executor-aggregate-flow.mmd)。

支持模式：

- `HashAggregateNode`：按 group key 维护 hash map。
- `SortAggregateNode`：输入已按 group key 排序或先 sort 后流式聚合。
- `DistinctAggregateNode`：为 `COUNT(DISTINCT x)` 等维护 dedup state。

规则：

- aggregate function state 是独立对象，例如 `CountState`、`SumState`、`MinMaxState`、`AvgState`。
- HAVING 在 aggregate result 形成后求值。
- group state 超过 memory quota 时写 partial group temp table，再 merge。
- `GROUP BY` 输出顺序不做默认保证，除非 plan 中有 order property。

### 7.3 Filesort 与 Top-N

Filesort / temp table 流程见 [advanced-executor-sort-temp-flow.mmd](diagrams/advanced-executor-sort-temp-flow.mmd)。

策略：

- 如果 child 输出满足 order property，不创建 SortNode。
- `ORDER BY + LIMIT` 可使用 top-n heap，减少内存。
- 内存不足时生成 sorted run。
- 多个 run 使用 k-way merge。
- sort key 和 row payload 分离，减少比较成本。

### 7.4 Internal Temporary Table

临时表用途：

- filesort spill。
- GROUP BY partial state。
- DISTINCT。
- derived table / CTE materialization。
- IN subquery materialization。
- window partition buffering。

TempTable 状态：

- `CREATED`
- `WRITING`
- `READING`
- `SPILLING`
- `CLOSING`
- `DROPPED`

规则：

- temp table 不进入 Data Dictionary。
- temp table 可使用 temporary tablespace 或进程内 spill directory。
- temp table 的物理文件锁不进入事务死锁检测。
- statement 结束必须释放 temp table；session 级 cursor 场景可延长到 cursor close。

### 7.5 Subquery / Derived Table / CTE

Subquery 和 window 流程见 [advanced-executor-subquery-window-flow.mmd](diagrams/advanced-executor-subquery-window-flow.mmd)。

子查询类型：

| 类型 | 执行策略 |
| --- | --- |
| scalar subquery | execute once；correlated 时按 outer row 执行 |
| EXISTS | semi-boolean，找到一行即可短路 |
| IN subquery | materialize set 或 per-row probe |
| derived table | merge 后由 Optimizer 展开，或 materialize 到 temp table |
| non-recursive CTE | materialize 或 inline，由 plan 指定 |
| recursive CTE | 保留扩展点 |

规则：

- SubqueryNode 不能自己选择 semijoin/materialize 策略。
- correlated subquery 通过 `OuterReferenceBinding` 读取 outer row 值。
- materialized subquery 需要 dependency version 和 temp table cleanup。

### 7.6 Window Function

WindowExecutor 支持：

- `PARTITION BY`。
- `ORDER BY`。
- `ROWS BETWEEN ...` 基础 frame。
- ranking：`ROW_NUMBER`、`RANK`、`DENSE_RANK`。
- aggregate window：`SUM`、`COUNT`、`AVG`、`MIN`、`MAX`。

规则：

- window input 需要按 partition/order 排序；不满足时插入 SortNode。
- partition 过大时 materialize 到 temp table。
- frame state 与 aggregate state 分离。
- WindowNode 输出原始 row 加 window columns，不修改底层 storage row。

## 8. 与其它模块的协作

### 8.1 与 Optimizer

- Optimizer 决定 PhysicalPlan、join order、join algorithm、sort/temp requirement。
- Advanced Executor 不重排 join。
- EXPLAIN ANALYZE 的 runtime metrics 回传给 ExplainService。

### 8.2 与 Parser/Binder

- Binder 提供 `BoundExpression`、`OuterReferenceBinding`、result schema、类型和 collation。
- Advanced Executor 不解析 AST。
- 权限和 MDL 由 Binder/Executor resource guard 管理。

### 8.3 与 Storage Engine

- TableScan、IndexScan、DML child node 通过 Storage API 访问真实表。
- Join/Aggregate/Sort 只消费 child node 输出的 `RowView` 或 `MaterializedRow`。
- row lock wait、MVCC、current read 和 release-before-wait 仍由 Storage Engine 内部处理。

### 8.4 与 Prepared Statement / Plan Cache

- `PhysicalPlanTemplate` 可包含高级 operator skeleton。
- Template 不能保存 `MemoryQuota`、temp table、spill file、runtime metrics 或 child cursor。
- DDL/statistics invalidation 只标记 template stale；执行中 operator tree 由 guard 保活。

### 8.5 与 Statistics

- Runtime metrics 可反馈给 EXPLAIN ANALYZE 和调试，不直接修改 persistent stats。
- 统计更新仍由 `mysql-statistics-analyze-design.md` 的 ANALYZE 模块负责。

## 9. 并发与锁顺序

并发状态图见 [advanced-executor-concurrency-state.mmd](diagrams/advanced-executor-concurrency-state.mmd)。

### 9.1 锁与等待对象

| 对象 | 保护资源 | 持有者 | 死锁域 |
| --- | --- | --- | --- |
| MDL ticket | table metadata | statement resource guard | MetadataWaitGraph |
| DD pin | immutable table/index definition | statement resource guard | 不进入死锁图 |
| PlanUseGuard | plan template refCount | executor | 不进入死锁图 |
| OperatorResourceGuard | child cursor、memory、temp、spill | PlanNode | 不进入死锁图 |
| MemoryQuota semaphore | query/operator memory | advanced operator | timeout/cancel |
| TempTableLock | temp table lifecycle | TempTableService | timeout/error |
| SpillFileLock | spill run file IO | Sort/Hash/Temp | timeout/error |
| StorageCursor | storage scan/probe | scan node | 不暴露 page latch |
| Row lock wait | transaction locks | Storage Engine | InnoDB WaitForGraph |

### 9.2 状态与持有变化

| 状态 | 持有者 | 持有资源 | 进入条件 | 退出条件 |
| --- | --- | --- | --- |
| `PLAN_READY` | executor | PhysicalPlan、statement guard | optimizer 完成 | open root |
| `OPERATOR_OPENING` | PlanNode | OperatorResourceGuard | `open()` | child opened 或 failed |
| `CHILD_READING` | PlanNode | child cursor | `next()` | row、end、lock wait、error |
| `ROW_LOCK_WAIT` | Storage Engine | row wait slot | current read 冲突 | grant、deadlock、timeout |
| `MEMORY_HELD` | operator | memory quota | buffer/build/group/window | emit、spill、close |
| `SPILLING` | operator | spill intent、memory snapshot | quota exceeded | temp IO |
| `TEMP_IO` | TempTableService | temp table lock、spill file lock | 写 run 或读 run | IO done/error |
| `RESULT_EMITTING` | PlanNode | current output row | row ready | caller consumed |
| `CLOSING` | resource guard | cleanup 权 | normal end/error/cancel | release resources |
| `RELEASED` | 无 | 无 operator 资源 | cleanup 完成 | 返回 caller |

持有变化规则：

- operator open 不获取 MDL；MDL 已由 statement guard 持有。
- PlanNode 不能在持有 temp file lock 时调用 StorageCursor 获取 row lock。
- row lock wait 发生在 Storage Engine 内部，进入等待前必须释放 page latch、buffer fix、RecordCursor。
- Sort/Hash spill 持有 `SpillFileLock` 时不能调用 Optimizer、DD 或 LockManager。
- MemoryQuota wait 不能持有 child StorageCursor 的当前 row handle。
- `close()` 必须逆序释放 child cursor、temp table、spill file、memory quota、metrics scope。

### 9.3 标准锁顺序

1. statement resource guard：MDL、DD pin。
2. PlanUseGuard。
3. OperatorResourceGuard。
4. child PlanNode open。
5. MemoryQuota。
6. TempTableLock。
7. SpillFileLock。
8. StorageCursor call。
9. Storage Engine 内部 row lock wait。

禁止反向等待：

- Temp/spill IO 不能等待 MDL。
- Storage row lock wait 不能持有 temp file lock。
- Plan cache invalidation 不能等待 operator close。
- EXPLAIN ANALYZE metrics 写入不能持有 temp file lock。

## 10. 异常处理

异常类型：

- `AdvancedExecutionException`
- `JoinExecutionException`
- `AggregateExecutionException`
- `FilesortException`
- `TempTableException`
- `SpillIoException`
- `SubqueryExecutionException`
- `WindowExecutionException`
- `MemoryQuotaExceededException`
- `OperatorCancelledException`

异常策略：

- child open 失败：关闭已打开 child。
- join probe 失败：关闭 outer/inner cursor，释放 memory quota。
- aggregate spill 失败：丢弃 partial state，不发布结果。
- filesort spill 写失败：关闭 spill run，删除临时文件，返回执行错误。
- subquery scalar 返回多行：返回 SQL 错误并清理 subquery cursor。
- window partition 处理失败：释放 partition temp table。
- statement cancel：触发 operator tree 自顶向下 close，释放所有 guard。

## 11. API 设计

### 11.1 AdvancedSqlExecutor

- `execute(PhysicalPlan plan, ExecutionContext context)`
- `openRoot(PhysicalOperator root, ExecutionContext context)`
- `closeRoot(PlanNode root)`
- `runtimeMetrics()`

### 11.2 PlanNode

- `open(ExecutionContext context)`
- `next()`
- `close()`
- `schema()`
- `metrics()`
- `children()`

### 11.3 TempTableService

- `createTempTable(TempTableSchema schema, TempTableOptions options)`
- `append(TempTableHandle handle, MaterializedRow row)`
- `openCursor(TempTableHandle handle)`
- `spillRun(SpillRunRequest request)`
- `drop(TempTableHandle handle)`
- `cleanupStatement(StatementId statementId)`

### 11.4 MemoryQuotaManager

- `reserve(QueryId queryId, OperatorId operatorId, long bytes)`
- `tryGrow(OperatorId operatorId, long bytes)`
- `release(OperatorId operatorId, long bytes)`
- `spillRequired(OperatorId operatorId)`

### 11.5 RuntimeMetricsCollector

- `onOpen(OperatorId)`
- `onRow(OperatorId)`
- `onSpill(OperatorId, long bytes)`
- `onClose(OperatorId)`
- `snapshot()`

## 12. 设计模式使用清单

| 模式 | 应用点 | 价值 |
| --- | --- | --- |
| Facade | `AdvancedSqlExecutor` | 统一复杂计划执行入口 |
| Composite | PlanNode tree | 表达嵌套 operator |
| Iterator | `PlanNode.next()` | 流式拉取结果 |
| Strategy | join、aggregate、sort、subquery、window 策略 | 支持不同物理算法 |
| Template Method | Sort/aggregate/spill 执行骨架 | 固定 open-read-buffer-spill-output-close |
| State | aggregate/window/temp table/operator state | 管理生命周期 |
| Repository | `TempTableService` | 封装临时表和 spill run |
| RAII Guard | `OperatorResourceGuard`、temp/spill handle | 异常路径释放资源 |
| Observer | runtime metrics collector | 支持 EXPLAIN ANALYZE |
| Adapter | PhysicalOperator 到 PlanNode | 隔离 Optimizer 和 Executor |

## 13. 高内聚、低耦合约束

- 高级算子只执行 Optimizer 给出的物理计划，不重新优化。
- JoinNode 不直接访问 DD 或 B+Tree page。
- AggregateNode 不读取 persistent stats。
- SortNode 不打开真实表，只消费 child rows。
- TempTableService 不解析 SQL 表达式。
- WindowNode 不修改 storage row。
- 所有 child cursor、temp table、spill file、memory quota 必须由 guard 管理。
- 执行期对象不得进入 PreparedStatement/PlanCache 模板。
- row lock wait、page latch、MTR、redo wait 仍在 Storage Engine 边界内。

## 14. 典型数据流

### 14.1 Inner Join

1. Optimizer 输出 join order 和 join algorithm。
2. JoinNode open outer/inner child。
3. nested loop 每个 outer row 驱动 inner。
4. index nested loop 根据 outer key 构造 inner probe。
5. hash join build side materialize hash table。
6. 评估 join predicate。
7. 输出 joined row。
8. close 时释放 child 和 memory。

### 14.2 GROUP BY / HAVING

1. AggregateNode 读取 child rows。
2. 求 group key。
3. 更新 aggregate state。
4. 超出内存时 spill partial state。
5. merge partial groups。
6. HAVING 求值。
7. 输出 aggregate row。

### 14.3 ORDER BY / Filesort

1. SortNode 读取 child rows。
2. 生成 sort key 和 payload。
3. 内存足够时直接排序。
4. 内存不足时写 sorted run。
5. 所有 run 完成后 k-way merge。
6. 输出排序结果。

### 14.4 Derived Table / Subquery

1. MaterializeNode 执行子计划。
2. 把输出写入 temp table。
3. 上层计划从 temp table cursor 读取。
4. correlated subquery 通过 outer reference 绑定外层 row。
5. statement close 清理 temp table。

### 14.5 Window Function

1. WindowNode 接收按 partition/order 排序的输入，或要求 SortNode 排序。
2. 按 partition 收集 row。
3. 维护 frame state。
4. 执行 window function。
5. 输出原 row 加 window column。

## 15. 测试设计

- Join 测试：nested loop、index nested loop、hash join、join predicate、空输入、重复 key。
- Join spill 测试：hash build 超出内存，分区 spill 后结果一致。
- Aggregate 测试：COUNT/SUM/AVG/MIN/MAX、GROUP BY、HAVING、DISTINCT aggregate。
- Aggregate spill 测试：partial group spill 和 merge。
- Filesort 测试：内存排序、top-n、multi-run merge、NULL/collation 排序。
- Temp table 测试：create/append/read/drop、statement cleanup、IO failure cleanup。
- Subquery 测试：scalar 单行、多行错误、IN、EXISTS、correlated nested loop。
- Derived/CTE 测试：materialization、重复读取、cleanup。
- Window 测试：partition/order、ROW_NUMBER/RANK、ROWS frame、partition spill。
- 并发测试：执行中 plan invalidation、deallocate、statement cancel、temp cleanup。
- 锁顺序测试：spill file lock 不等待 MDL/row lock，row lock wait 不持有 temp lock。
- EXPLAIN ANALYZE 测试：actual rows、loops、time、spill bytes。
- 故障注入：child error、temp IO error、memory quota error、cancel。

## 16. 后续实现顺序

1. `PlanNode` 高级接口和 `OperatorResourceGuard`。
2. RuntimeMetricsCollector。
3. MemoryQuotaManager。
4. TempTableService 内存表。
5. Spill file adapter。
6. SortNode 和 top-n。
7. NestedLoopJoinNode。
8. IndexNestedLoopJoinNode。
9. HashJoinNode 内存模式。
10. HashJoin spill。
11. HashAggregateNode。
12. Aggregate spill 和 distinct aggregate。
13. MaterializeNode / Derived table。
14. SubqueryNode。
15. WindowNode 和 EXPLAIN ANALYZE 集成测试。

## 17. 十五轮自检记录

| 轮次 | 检查主题 | 检查结果 |
| --- | --- | --- |
| 1 | 是否只写设计文档 | 本文只新增 Markdown 和 Mermaid 设计内容，没有生成 Java 实现代码 |
| 2 | 目标与非目标 | 已明确高级算子承接 join、聚合、排序、临时表、子查询、窗口，不替代 Optimizer/Storage |
| 3 | MySQL 8.0 贴合 | 已覆盖 nested loop、hash join、filesort、internal temporary table、GROUP BY、subquery、window |
| 4 | 高内聚 | join、aggregate、sort、temp、subquery、window、materialize、metrics 子包职责独立 |
| 5 | 低耦合 | 高级算子只使用 PhysicalPlan、BoundExpression、ExecutionContext 和 Storage API |
| 6 | 面向对象 | 已定义 PlanNode、JoinNode、AggregateNode、TempTableHandle、SpillRun、RuntimeMetrics 等对象 |
| 7 | 设计模式 | 已列出 Facade、Composite、Iterator、Strategy、Template Method、State、Repository、Guard 等 |
| 8 | 核心领域模型 | 已覆盖 operator tree、materialized row、memory quota、spill run 和 temp table |
| 9 | 依赖方向 | 已明确 Bound/Optimizer -> PhysicalPlan -> Executor -> Storage/Temp 的单向链路 |
| 10 | 物理与逻辑区分 | 已区分绑定语义、物理计划、执行算子、存储访问、中间结果和 spill 文件 |
| 11 | 关键数据流 | 已给出 join、GROUP BY、filesort、derived/subquery、window 流程 |
| 12 | 图示 | 已提供架构图、类关系图、join、aggregate、sort/temp、subquery/window、并发状态图 |
| 13 | 并发锁状态 | 已定义 MDL/DD pin、PlanUseGuard、OperatorGuard、MemoryQuota、Temp/Spill lock 和 row lock wait 边界 |
| 14 | 异常与恢复 | 已覆盖 join、aggregate、filesort、temp IO、subquery、window、cancel 的 cleanup 策略 |
| 15 | 测试与顺序 | 已给出测试设计、实现顺序，并确认没有未完成标记或空白项 |

## 18. 参考链接

- MySQL 8.0 Reference Manual - Nested-Loop Join Algorithms: https://dev.mysql.com/doc/refman/8.0/en/nested-loop-joins.html
- MySQL 8.0 Reference Manual - Hash Join Optimization: https://dev.mysql.com/doc/refman/8.0/en/hash-joins.html
- MySQL 8.0 Reference Manual - ORDER BY Optimization: https://dev.mysql.com/doc/refman/8.0/en/order-by-optimization.html
- MySQL 8.0 Reference Manual - Internal Temporary Table Use: https://dev.mysql.com/doc/refman/8.0/en/internal-temporary-tables.html
- MySQL 8.0 Reference Manual - GROUP BY Optimization: https://dev.mysql.com/doc/refman/8.0/en/group-by-optimization.html
- MySQL 8.0 Reference Manual - Optimizing Subqueries, Derived Tables, View References, and CTEs: https://dev.mysql.com/doc/refman/8.0/en/subquery-optimization.html
- MySQL 8.0 Reference Manual - Window Functions: https://dev.mysql.com/doc/refman/8.0/en/window-functions.html
- MySQL 8.0 Reference Manual - EXPLAIN ANALYZE: https://dev.mysql.com/doc/refman/8.0/en/explain.html
