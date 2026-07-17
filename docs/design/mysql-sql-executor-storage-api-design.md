# MiniMySQL MySQL 8.0 风格 SQL Executor 与 Storage Engine API 设计

版本：2026-06-05  
实现语言：Java  
参考基线：MySQL 8.0.46 SQL Statements、InnoDB consistent read、locking read、transaction isolation、metadata locking  
关联设计：[mysql-parser-binder-design.md](mysql-parser-binder-design.md)、[mysql-prepared-statement-plan-cache-design.md](mysql-prepared-statement-plan-cache-design.md)、[mysql-query-optimizer-design.md](mysql-query-optimizer-design.md)、[mysql-advanced-executor-operators-design.md](mysql-advanced-executor-operators-design.md)、[mysql-data-dictionary-ddl-design.md](mysql-data-dictionary-ddl-design.md)、[innodb-storage-engine-overview.md](innodb-storage-engine-overview.md)、[innodb-btree-design.md](innodb-btree-design.md)、[innodb-record-design.md](innodb-record-design.md)、[innodb-transaction-mvcc-design.md](innodb-transaction-mvcc-design.md)、[innodb-secondary-index-mvcc-purge-design.md](innodb-secondary-index-mvcc-purge-design.md)、[innodb-redo-log-design.md](innodb-redo-log-design.md)、[innodb-buffer-pool-design.md](innodb-buffer-pool-design.md)、[innodb-disk-manager-design.md](innodb-disk-manager-design.md)、[mysql-session-connection-protocol-design.md](mysql-session-connection-protocol-design.md)、[mysql-lock-observability-deadlock-design.md](mysql-lock-observability-deadlock-design.md)

## 1. 目标与边界

本设计面向 MiniMySQL 的 SQL 执行层和存储引擎访问边界。它定义 SQL statement 从 AST、绑定、简单计划、执行上下文，到 Storage Engine API 调用的完整路径，重点覆盖第一阶段 `SELECT`、`INSERT`、`UPDATE`、`DELETE`。执行器负责语句生命周期、Data Dictionary 绑定、MDL 获取、事务上下文、表达式求值、结果行生成和错误清理；存储引擎负责 B+Tree、Record、Transaction、Buffer Pool、Redo、Disk Manager 的物理执行。

设计目标：

- 高内聚：SQL 语义绑定、计划树执行、表达式求值、事务上下文和存储 API 分别收敛在明确子包。
- 低耦合：Executor 只依赖 `DataDictionaryService` 和 `StorageEngine` 接口，不直接操作 B+Tree page、RecordCursor、BufferFrame 或 LockManager 内部结构。
- MySQL 8.0 风格：对齐 DML statement、consistent nonlocking read、locking read、transaction isolation、InnoDB row lock、metadata lock 的关键行为。
- 可落地：第一阶段先支持单表 DML、简单谓词、投影、limit、order by 内存排序和基础索引访问，保留 optimizer/join/subquery 扩展点。
- 并发安全：明确 MDL、事务行锁、Buffer Pool page latch、MTR latch、物理文件锁的边界和释放顺序。
- 可测试：每个执行路径都能通过 mock StorageEngine 和真实存储模块集成测试验证。

非目标：

- 不实现完整 SQL parser；假设已有 AST 输入。
- 不实现成本优化器和高级关系算子；join、复杂子查询、window function、CTE、聚合和 group by 由 [mysql-advanced-executor-operators-design.md](mysql-advanced-executor-operators-design.md) 承接。
- 不实现权限系统、binlog、replication、trigger、foreign key cascade、stored routine。
- 不在 Executor 中解析 InnoDB record byte layout 或分配 page/extent/segment。
- 不生成 Java 源码。本文件只定义设计、关系和数据流。

## 2. MySQL 8.0 参考依据

本设计参考 MySQL 8.0 的以下行为：

- SQL DML statement 包括 `SELECT`、`INSERT`、`UPDATE`、`DELETE`，执行时需要解析对象名、列、表达式、谓词和结果列。
- InnoDB 默认隔离级别是 `REPEATABLE READ`；普通非锁定 SELECT 使用 consistent read，读取 ReadView 对应快照。
- `READ COMMITTED` 下每个 consistent read 使用新的快照；`REPEATABLE READ` 下同一事务内普通 SELECT 共享首次读取建立的快照。
- `SELECT ... FOR UPDATE` 和 `SELECT ... FOR SHARE` 是 locking read，需要对读取到的索引记录加事务锁。
- `UPDATE` 和 `DELETE` 使用 current read，通常对扫描到的索引记录设置 record 或 next-key lock；具体锁范围受唯一条件、范围条件和隔离级别影响。
- InnoDB row lock 包括 S/X、intention lock、record lock、gap lock、next-key lock、insert intention lock。
- Metadata Lock 保护 table/schema 等对象定义，DML 通常持有 shared metadata lock，DDL 需要 exclusive metadata lock。
- Performance Schema 可观察 InnoDB data locks/waits 和 metadata locks；本项目以诊断快照方式保留类似边界。

简化点：

| MySQL 8.0 能力 | MiniMySQL 第一阶段 |
| --- | --- |
| 完整优化器和代价模型 | `SimplePlanner` 只做主键点查、二级索引范围扫描、全表扫描选择 |
| 多表 join 和 subquery | 基础 Executor 不实现，由 [mysql-advanced-executor-operators-design.md](mysql-advanced-executor-operators-design.md) 承接 |
| 聚合、group by、window function | 基础 Executor 不实现，由 [mysql-advanced-executor-operators-design.md](mysql-advanced-executor-operators-design.md) 承接 |
| 完整表达式系统 | 支持比较、布尔、NULL、常量、列引用、简单算术 |
| 完整 SQL cursor 协议 | 先实现 `ResultSetCursor` 和内部 `StorageCursor` |
| 完整 Performance Schema | 先提供 executor trace、MDL snapshot、row-lock wait snapshot |

## 3. 总体架构

架构图见 [sql-executor-architecture.mmd](diagrams/sql-executor-architecture.mmd)。

执行链路：

1. `Session` 收到 AST 或 parser 输出。
2. `NameResolver` 通过 Data Dictionary 解析 schema/table/column/index。
3. `MetadataLockManager` 获取 statement 所需 MDL。
4. `SimplePlanner` 构造 `QueryPlan` 或 `DmlPlan`。
5. `SqlExecutor` 创建 `ExecutionContext`，绑定 `TransactionContext`。
6. `PlanNode` 树按 pull 模型执行，调用 `StorageEngine`。
7. Storage Engine 通过 B+Tree、Record、Transaction、Buffer Pool、Redo、Disk Manager 完成读写。
8. Executor 清理 cursor、dictionary pin、MDL 和 statement 资源。

依赖方向：

`sql.session -> sql.exec -> sql.dd -> storage.api -> storage.btree/record/trx/buf/redo/fsp/fil`

禁止依赖：

- Executor 不能持有 Buffer Pool page latch。
- Executor 不能直接调用 `LockManager.acquireRecordLock()`；只能通过 Storage API 表达 read/write intent。
- PlanNode 不能读取 `BufferFrame`、`RecordCursor`、`PageHandle`。
- Storage Engine 不反向调用 SQL expression evaluator。
- Expression evaluator 不访问 Data Dictionary repository，只使用绑定后的 `ColumnBinding`。

## 4. 包与职责

| 包 | 职责 | 依赖 | 主要模式 |
| --- | --- | --- | --- |
| `sql.exec.api` | `SqlExecutor`、`ExecutionResult`、`ResultSetCursor` | plan, runtime | Facade |
| `sql.exec.input` | 接收 `BoundStatement`、`PhysicalPlan` 和 statement resource guard | parser/binder, optimizer | Adapter |
| `sql.exec.plan` | `QueryPlan`、`DmlPlan`、访问路径描述 | dd domain | Composite, Strategy |
| `sql.exec.node` | PlanNode 执行树，scan/filter/project/limit/sort/dml | storage api, expr | Iterator, Template Method |
| `sql.exec.expr` | predicate、projection、update assignment 求值 | row model | Interpreter, Strategy |
| `sql.exec.runtime` | `ExecutionContext`、resource guard、diagnostic trace | trx, mdl | RAII Guard, Observer |
| `sql.exec.storage` | `StorageEngine`、`StorageCursor`、table/index access API | storage modules | Facade, Adapter |
| `sql.exec.tx` | statement transaction policy、autocommit、rollback cleanup | trx api | Unit of Work |
| `sql.exec.error` | SQL 执行异常和错误映射 | runtime | Mapper |

## 5. 核心领域模型

类关系图见 [sql-executor-class-relation.mmd](diagrams/sql-executor-class-relation.mmd)。

### 5.1 执行上下文

| 对象 | 职责 |
| --- | --- |
| `SessionContext` | session id、当前 schema、autocommit、isolation、statement timeout |
| `ExecutionContext` | 当前 statement 的事务、字典 read view、MDL tickets、resource guards |
| `TransactionContext` | 当前数据库事务引用、ReadView 策略、statement undo/rollback marker |
| `StatementContext` | statement id、SQL 类型、参数、诊断信息 |
| `ExecutionTrace` | plan、行数、锁等待、重定位次数、异常路径 |

### 5.2 计划对象

| 对象 | 职责 |
| --- | --- |
| `QueryPlan` | SELECT 的 root PlanNode 和结果 schema |
| `DmlPlan` | INSERT/UPDATE/DELETE 的目标表、访问路径和写入动作 |
| `TableAccessPlan` | table id、index id、range、read intent、projection |
| `IndexAccessPlan` | point/range/full scan、scan direction、limit hint |
| `ResultSchema` | 输出列名、类型、nullable、表达式来源 |
| `UpdatePatchPlan` | UPDATE 的 assignment、受影响列、二级索引维护需求 |

### 5.3 行和游标对象

| 对象 | 职责 |
| --- | --- |
| `RowView` | Executor 看到的逻辑行，按 `ColumnBinding` 读取值 |
| `LogicalRow` | INSERT 或 materialized row 的列值集合 |
| `StorageRowHandle` | 存储层当前行定位句柄，只能用于当前 storage cursor 生命周期 |
| `StorageCursor` | 存储层扫描游标，屏蔽 B+Tree cursor 和 page latch |
| `ResultSetCursor` | 返回给客户端的结果游标 |

规则：

- `StorageRowHandle` 不是 `RecordRef`，不能跨 statement 或 cursor 保存。
- Executor 只接触 `RowView` 和 `StorageRowHandle`，不接触 page latch 和 record cursor。
- `LogicalRow` 必须已按 `TableDefinition` 完成默认值、类型校验和列顺序归一化。

## 6. Storage Engine API

Storage API 是 SQL 执行层和 InnoDB 模块之间的唯一写入入口：

| API | 语义 |
| --- | --- |
| `openTable(TableDefinition, AccessMode, TransactionContext)` | 打开表，绑定 table definition 和事务上下文 |
| `closeTable(StorageTableHandle)` | 关闭表 handle，释放存储层短资源 |
| `indexScan(StorageTableHandle, IndexAccessPlan)` | 按 index/range 打开扫描游标 |
| `readByPrimaryKey(StorageTableHandle, KeyTuple, ReadIntent)` | 主键点查 |
| `insertRow(StorageTableHandle, LogicalRow, InsertPolicy)` | 插入逻辑行，维护聚簇和二级索引 |
| `updateRow(StorageRowHandle, UpdatePatch, WriteIntent)` | 更新当前行，维护 undo、隐藏列和二级索引 |
| `deleteRow(StorageRowHandle, DeleteIntent)` | delete-mark 当前行 |
| `lockCurrentRow(StorageRowHandle, RowLockIntent)` | locking read 显式锁当前行 |
| `estimateAccess(TableDefinition, Predicate)` | 简单访问路径估计，供第一阶段 planner 使用 |

`AccessMode`：

- `READ_CONSISTENT`
- `READ_CURRENT`
- `LOCKING_SHARE`
- `LOCKING_UPDATE`
- `INSERT`
- `UPDATE`
- `DELETE`

Storage API 约束：

- Storage Engine 负责在 row-lock wait 前释放 page latch、buffer fix 和 record cursor。
- Storage Engine 负责等待后重新定位记录。
- Executor 只看到 grant、timeout、deadlock victim、row changed、row not found 等结果。
- Storage Engine 不求值 SQL predicate，只能接收可下推的 index range。

## 7. 核心策略和算法

### 7.1 Statement 生命周期

1. `PARSING`：获得 AST。
2. `BINDING`：解析对象名，获取 MDL，pin `TableDefinition`。
3. `PLANNING`：生成 `QueryPlan` 或 `DmlPlan`。
4. `OPENING`：打开 PlanNode 和 StorageCursor。
5. `EXECUTING`：逐行拉取、过滤、投影或写入。
6. `STATEMENT_CLEANUP`：关闭 cursor、释放 statement 资源。
7. `TRANSACTION_END`：autocommit 下 commit/rollback；显式事务下保留事务资源。

### 7.2 SimplePlanner

第一阶段访问路径选择：

| 输入 | 访问路径 |
| --- | --- |
| 主键等值谓词 | `PRIMARY_POINT_LOOKUP` |
| 唯一二级索引等值谓词 | `UNIQUE_SECONDARY_LOOKUP + PRIMARY_LOOKUP` |
| 二级索引前缀范围 | `SECONDARY_RANGE_SCAN` |
| 无可用索引 | `CLUSTERED_FULL_SCAN` |
| ORDER BY 与索引顺序一致 | index scan 保序 |
| ORDER BY 不可由索引满足 | `SortNode` 内存排序 |

Planner 不做：

- join reorder。
- 子查询改写。
- 代价模型。
- 条件下推到 Record 层以外的复杂表达式。

### 7.3 表达式求值

`ExpressionEvaluator` 支持：

- column reference。
- literal。
- comparison：`=`, `<>`, `<`, `<=`, `>`, `>=`。
- boolean：`AND`, `OR`, `NOT`。
- NULL：`IS NULL`, `IS NOT NULL`。
- simple arithmetic。
- update assignment。

规则：

- 类型比较使用 Record/Data Dictionary 提供的 `ColumnType` 和 collation。
- 三值逻辑由 `SqlBoolean` 表达。
- WHERE 谓词在 Executor 层判断，Storage 只接收 index range。

## 8. SELECT 设计

SELECT 流程图见 [sql-select-flow.mmd](diagrams/sql-select-flow.mmd)。

### 8.1 Consistent Read

普通 SELECT：

1. 获取 table `MDL_SHARED_READ`。
2. 绑定 `TableDefinition` 并 pin version。
3. 根据隔离级别获取 ReadView。
4. 打开 StorageCursor，使用 `READ_CONSISTENT`。
5. Storage Engine 调用 MVCC 判断可见性，必要时沿 undo 构造旧版本。
6. Executor 求值 WHERE、投影、排序、LIMIT。
7. 关闭 cursor，释放 statement pin 和 statement duration MDL。

### 8.2 Locking Read

`SELECT ... FOR UPDATE`：

- AccessMode 使用 `LOCKING_UPDATE`。
- Storage Engine 通过 B+Tree/Transaction 获取 record/gap/next-key X lock。
- 如果需要等待，Storage Engine 在进入 LockManager 前释放 page latch 和 buffer fix。
- 等待后重新定位记录，再返回最新 current row。

`SELECT ... FOR SHARE`：

- AccessMode 使用 `LOCKING_SHARE`。
- 获取 record S lock 或 next-key S lock。
- 结果行仍由 Executor 过滤和投影。

### 8.3 ORDER BY / LIMIT

第一阶段：

- 如果 index scan 已满足 ORDER BY，直接流式返回。
- 如果不满足，`SortNode` materialize 结果行到内存后排序。
- LIMIT 可以作为 scan hint 下推，但不能改变锁语义；locking read 仍按扫描范围加锁。

## 9. INSERT / UPDATE / DELETE 设计

DML 流程图见 [sql-dml-flow.mmd](diagrams/sql-dml-flow.mmd)。

### 9.1 INSERT

流程：

1. 获取 table `MDL_SHARED_WRITE`。
2. 开启或复用事务。
3. 构造 `LogicalRow`：默认值、NULL、类型、生成 `DB_ROW_ID` 扩展点。
4. 检查唯一索引和主键冲突。
5. 获取 insert intention lock 和必要 gap lock。
6. Storage Engine 插入聚簇索引记录和二级索引记录。
7. Record 写隐藏列，Transaction 写 undo，MTR 收集 redo。
8. Autocommit 下提交事务并释放 row locks。

### 9.2 UPDATE

流程：

1. 使用 current read 扫描候选行。
2. 对扫描到的 index record 获取 X 或 next-key X lock。
3. Executor 使用最新 current row 求值 WHERE。
4. 构造 `UpdatePatch`。
5. 如果更新聚簇主键，转换为 delete-mark + insert 的受控流程。
6. 如果更新二级索引 key，Storage Engine delete-mark 旧 secondary entry 并插入新 entry。
7. 写 undo、隐藏列、redo。

### 9.3 DELETE

流程：

1. current read 扫描候选行。
2. 获取 record X 或 next-key X lock。
3. WHERE 命中后调用 `deleteRow()`。
4. Record 层 delete-mark 聚簇记录和二级索引项。
5. Purge 后续异步物理删除。

### 9.4 语句级回滚

当 statement 失败：

- autocommit 事务完整 rollback。
- 显式事务中使用 statement undo marker 回滚本语句修改。
- 已授予 row lock 的释放遵守事务模块策略；若回滚整事务则全部释放。
- MDL 按 statement/transaction duration 释放。

## 10. 与其它模块的协作

### 10.1 与 Data Dictionary / MDL

- Parser/Binder 已解析对象名、获取 table binding，并把 MDL/DD pin 交给 statement resource guard。
- DML 获取 `MDL_SHARED_READ` 或 `MDL_SHARED_WRITE`。
- DDL exclusive pending 时，新的 DML 是否排队由 MDL manager 的公平策略控制。
- Executor 不直接改 dictionary object。

### 10.2 与 Transaction / MVCC

- Executor 创建或复用 `TransactionContext`。
- 普通 SELECT 使用 `MvccVisibilityService` 间接读取快照。
- UPDATE/DELETE/locking read 使用 current read。
- Autocommit 由 `StatementTransactionPolicy` 决定 commit/rollback。

### 10.3 与 B+Tree / Record

- Executor 传入 `IndexAccessPlan` 和 `ReadIntent/WriteIntent`。
- B+Tree 负责跨页导航和锁落点。
- Record 负责字段编码、隐藏列和页内修改。
- Executor 不持有 `RecordCursor`。

### 10.4 与 Buffer Pool / Disk / Redo

- Buffer Pool page latch 只存在于 Storage Engine 内部。
- MTR commit 和 redo durable 策略由 Transaction/Redo 模块决定。
- Executor 等待 commit durable 时不能持有 PlanNode 内部 cursor 或 MDL manager mutex。
- Disk IO 错误通过 Storage API 转换为 SQL execution error。

## 11. 并发与锁顺序

SQL Executor 锁状态图见 [sql-executor-lock-state.mmd](diagrams/sql-executor-lock-state.mmd)。

### 11.1 锁对象

| 锁/等待 | 所属模块 | Executor 是否直接持有 | 死锁域 |
| --- | --- | --- | --- |
| MDL | Data Dictionary / MDL | 是，持有 ticket | MetadataWaitGraph |
| Dictionary pin | DD cache | 是，持有 immutable version pin | 不进入死锁图 |
| Transaction row lock | Transaction LockManager | 间接，通过 Storage API | InnoDB WaitForGraph |
| Buffer Pool page latch | Buffer Pool | 否 | timeout/retry |
| MTR latch/fix | MTR / Buffer Pool | 否 | timeout/retry |
| Physical file lock | Disk Manager | 否 | timeout/error |
| Redo wait slot | Redo | 间接，commit wait | error broadcast/timeout |

### 11.2 Executor 状态与持有变化

| 状态 | 持有者 | 持有资源 | 进入条件 | 退出条件 |
| --- | --- | --- | --- | --- |
| `IDLE` | session | 无 statement 资源 | 等待 SQL | 收到 statement |
| `BINDING` | executor | AST、session context | parser 完成 | 请求 MDL |
| `MDL_WAITING` | MDL manager | MDL wait slot | table metadata 冲突 | grant、timeout、victim、killed |
| `MDL_GRANTED` | session/executor | MDL ticket | MDL 兼容 | pin dictionary object |
| `DICT_PINNED` | executor | immutable `TableDefinition` pin | table definition 加载 | plan close 或 transaction end |
| `PLAN_OPEN` | executor | PlanNode tree | planner 完成 | open storage cursor |
| `STORAGE_CURSOR_OPEN` | storage cursor | storage table handle，不含 page latch 暴露 | scan 或 DML 打开 | row available、row lock wait、close |
| `ROW_LOCK_WAIT` | Storage/LockManager | row-lock wait slot | locking read/DML 冲突 | grant、deadlock、timeout |
| `ROW_AVAILABLE` | executor | `RowView`、短期 `StorageRowHandle` | 读取或锁授予 | filter/project 或 DML |
| `MODIFYING` | storage | 当前事务锁、内部 MTR/page latch | DML 命中行 | MTR commit 或异常 |
| `STATEMENT_CLEANUP` | executor | resource guard cleanup 权 | 正常结束或失败 | release cursor/pin/MDL |
| `TRANSACTION_END` | transaction policy | commit/rollback 权 | autocommit 或显式结束 | 释放 row locks |

持有变化规则：

- `acquire MDL`：等待 MDL 时不能持有 row lock、page latch、MTR latch、file lock 或 StorageCursor。
- `pin dictionary`：pin 只保护元数据版本，不保护数据行。
- `open cursor`：StorageCursor 内部可能短持 page latch，但不能暴露给 Executor。
- `row-lock wait`：Storage Engine 负责 release-before-wait 和 relocate-after-grant。
- `commit wait`：Executor 等待 commit/redo durable 前必须关闭 PlanNode 和 StorageCursor。
- `cleanup`：异常路径必须按 cursor -> dictionary pin -> MDL -> statement transaction marker 顺序清理。

### 11.3 全局锁顺序

1. Session statement mutex。
2. Metadata lock。
3. Dictionary cache pin。
4. Transaction object short lock。
5. Storage API call。
6. B+Tree/Record/Buffer Pool/MTR 内部短锁。
7. Transaction row lock wait。
8. Redo wait slot。

注意：第 6 和第 7 的真实等待边界由 Storage Engine 保证：进入 row lock wait 前必须释放 page latch、buffer fix 和 RecordCursor。

## 12. 异常处理

异常类型：

- `SqlExecutionException`
- `TableBindingException`
- `ColumnBindingException`
- `UnsupportedSqlFeatureException`
- `ExpressionEvaluationException`
- `MetadataLockTimeoutSqlException`
- `RowLockTimeoutSqlException`
- `DeadlockVictimSqlException`
- `DuplicateKeySqlException`
- `RowChangedDuringCurrentReadException`
- `StorageExecutionException`
- `StatementRollbackException`

异常策略：

- Bind 失败：释放已获取 MDL 和 dictionary pin，不开启 storage cursor。
- PlanNode open 失败：关闭已打开 child node。
- Row lock timeout/deadlock：通知 transaction policy 执行 statement rollback 或 transaction rollback。
- Duplicate key：INSERT statement rollback；显式事务保留事务但回滚本 statement 修改。
- Storage IO/redo failure：进入严重错误路径，阻止继续提交。

## 13. API 设计

### 13.1 SqlExecutor

- `execute(StatementAst, SessionContext)`
- `executeQuery(QueryAst, SessionContext)`
- `executeInsert(InsertAst, SessionContext)`
- `executeUpdate(UpdateAst, SessionContext)`
- `executeDelete(DeleteAst, SessionContext)`
- `closeStatement(ExecutionContext)`

### 13.2 PlanNode

- `open(ExecutionContext)`
- `next()`
- `close()`
- `schema()`
- `diagnostics()`

### 13.3 StorageEngine

- `openTable(TableDefinition, AccessMode, TransactionContext)`
- `indexScan(StorageTableHandle, IndexAccessPlan)`
- `readByPrimaryKey(StorageTableHandle, KeyTuple, ReadIntent)`
- `insertRow(StorageTableHandle, LogicalRow, InsertPolicy)`
- `updateRow(StorageRowHandle, UpdatePatch, WriteIntent)`
- `deleteRow(StorageRowHandle, DeleteIntent)`
- `closeTable(StorageTableHandle)`

### 13.4 ExpressionEvaluator

- `evalPredicate(RowView, BoundExpression)`
- `evalProjection(RowView, ProjectionList)`
- `evalAssignment(RowView, AssignmentExpression)`
- `coerce(ColumnValue, ColumnType)`
- `compare(ColumnValue, ColumnValue, CollationStrategy)`

## 14. 设计模式使用清单

| 模式 | 应用点 | 价值 |
| --- | --- | --- |
| Facade | `SqlExecutor`, `StorageEngine` | 隐藏执行树和存储内部细节 |
| Composite | PlanNode tree | 统一 scan/filter/project/limit/sort |
| Iterator | `PlanNode.next()`, `StorageCursor.next()` | 流式拉取结果 |
| Strategy | access path、read intent、transaction policy | 隔离策略差异 |
| Template Method | DML execution skeleton | 固定 bind-plan-open-execute-cleanup |
| RAII Guard | `ExecutionResourceGuard` | 异常路径释放 cursor、pin、MDL |
| Interpreter | ExpressionEvaluator | 执行绑定表达式 |
| Adapter | Storage API 到 InnoDB 模块 | 隔离 SQL 和存储层 |
| Unit of Work | TransactionContext | statement/transaction commit rollback |
| State | Executor lock state | 明确并发持有变化 |

## 15. 高内聚、低耦合约束

- SQL Executor 只做 SQL 语义、计划执行和结果输出。
- Storage Engine 只做存储访问，不求值未下推 SQL 表达式。
- Data Dictionary 只提供不可变 table definition 和版本 pin。
- Transaction 模块只管理事务状态、ReadView、undo、row lock，不解析 SQL AST。
- B+Tree 只负责索引导航和物理结构维护。
- Record 只负责页内字段和记录格式。
- Buffer Pool 只负责 page cache 和 latch。

## 16. 典型数据流

### 16.1 SELECT

见 [sql-select-flow.mmd](diagrams/sql-select-flow.mmd)。关键边界是普通 SELECT 走 ReadView，locking read 走 LockManager，并由 Storage Engine 处理 release-before-wait。

### 16.2 INSERT / UPDATE / DELETE

见 [sql-dml-flow.mmd](diagrams/sql-dml-flow.mmd)。关键边界是 Executor 构造逻辑行或 update patch，物理 undo/redo/page 修改由 Storage Engine 内部完成。

### 16.3 Autocommit INSERT

1. 获取 MDL_SHARED_WRITE。
2. 开启 statement transaction。
3. 插入行并写 undo/redo。
4. MTR commit。
5. Transaction commit 等待 redo durable。
6. 释放 row locks、MDL、dictionary pin。

### 16.4 Explicit Transaction UPDATE

1. `BEGIN` 后事务保持 active。
2. UPDATE statement 获取 MDL 和 table definition pin。
3. current read 扫描并加 row lock。
4. 命中行写 undo/redo。
5. statement 结束释放 cursor 和 statement pin。
6. row lock 保留到事务 commit/rollback。

## 17. 测试设计

- Binding 测试：表不存在、列不存在、重复列、ambiguous column、不可写列。
- Planner 测试：主键点查、二级索引范围、全表扫描、ORDER BY index satisfied、LIMIT hint。
- Expression 测试：三值逻辑、NULL 比较、类型转换、collation 比较、update assignment。
- SELECT 测试：consistent read、locking read、FOR UPDATE、FOR SHARE、limit、sort。
- INSERT 测试：默认值、NULL 约束、唯一冲突、二级索引维护、autocommit。
- UPDATE 测试：current read、WHERE 过滤、二级索引 key 变化、主键变化策略。
- DELETE 测试：delete-mark、purge 交接、范围删除锁范围。
- 并发测试：DML 与 DDL MDL 冲突、row lock wait、deadlock victim、statement rollback。
- 资源释放测试：PlanNode open 失败、cursor next 失败、duplicate key、lock timeout 后清理顺序。
- 集成测试：Executor 到 Storage API 到 B+Tree/Record/Transaction/Redo 的完整链路。

## 18. 后续实现顺序

1. `sql.exec.runtime`：SessionContext、ExecutionContext、resource guard。
2. `sql.exec.bind`：TableBinding、ColumnBinding、NameResolver。
3. `sql.exec.expr`：三值逻辑和基础表达式求值。
4. `sql.exec.plan`：QueryPlan、DmlPlan、IndexAccessPlan。
5. `sql.exec.storage`：StorageEngine API 和 mock storage。
6. PlanNode 基类、TableScanNode、IndexLookupNode。
7. FilterNode、ProjectionNode、LimitNode。
8. SELECT consistent read 链路。
9. SELECT locking read 链路。
10. INSERT 链路和 duplicate key 错误映射。
11. UPDATE 链路和 statement rollback marker。
12. DELETE 链路和 delete-mark。
13. SortNode 和 ORDER BY 第一阶段。
14. 并发诊断和 lock wait trace。
15. 集成测试和故障注入。

## 19. 十五轮自检记录

| 轮次 | 检查主题 | 检查结果 |
| --- | --- | --- |
| 1 | 是否只写设计文档 | 本文只新增设计说明和 Mermaid 图，没有生成 Java 实现代码 |
| 2 | 目标与非目标 | 已明确 Executor/Storage API 目标，排除完整 parser、optimizer、join、binlog 等范围 |
| 3 | MySQL 8.0 贴合 | 已覆盖 SELECT/INSERT/UPDATE/DELETE、consistent read、locking read、隔离级别、MDL、InnoDB row lock |
| 4 | 高内聚 | 执行、绑定、计划、表达式、运行时、存储 API、事务策略职责分开 |
| 5 | 低耦合 | Executor 只通过 DD 和 Storage API 协作，不访问存储内部 page/frame/record cursor |
| 6 | 面向对象 | 已定义 ExecutionContext、QueryPlan、PlanNode、StorageCursor、RowView 等对象 |
| 7 | 设计模式 | 已列出 Facade、Composite、Iterator、Strategy、Template Method、RAII Guard 等 |
| 8 | 核心领域模型 | 已覆盖执行上下文、计划对象、行对象、游标对象和 Storage API |
| 9 | 依赖方向 | 已给出 SQL 到 DD 到 Storage API 到 InnoDB 模块的单向依赖 |
| 10 | 物理与逻辑区分 | 已区分 SQL logical row/RowView 与 storage row handle、RecordRef、page latch |
| 11 | 关键数据流 | 已给出 SELECT、DML、autocommit insert、explicit transaction update 流程 |
| 12 | 图示 | 已提供架构图、类关系图、SELECT 流程图、DML 流程图、锁状态图 |
| 13 | 并发锁状态 | 已定义 MDL、dictionary pin、row lock、page latch、MTR、redo wait 的持有变化 |
| 14 | 异常与恢复 | 已给出 bind、open、row lock、duplicate key、storage failure 的清理和 rollback 策略 |
| 15 | 测试与顺序 | 已给出测试设计、后续实现顺序，并确认没有未完成标记或空白项 |

## 20. 参考链接

- MySQL 8.0 Reference Manual - SELECT Statement: https://dev.mysql.com/doc/refman/8.0/en/select.html
- MySQL 8.0 Reference Manual - INSERT Statement: https://dev.mysql.com/doc/refman/8.0/en/insert.html
- MySQL 8.0 Reference Manual - UPDATE Statement: https://dev.mysql.com/doc/refman/8.0/en/update.html
- MySQL 8.0 Reference Manual - DELETE Statement: https://dev.mysql.com/doc/refman/8.0/en/delete.html
- MySQL 8.0 Reference Manual - Transaction Isolation Levels: https://dev.mysql.com/doc/refman/8.0/en/innodb-transaction-isolation-levels.html
- MySQL 8.0 Reference Manual - Consistent Nonlocking Reads: https://dev.mysql.com/doc/refman/8.0/en/innodb-consistent-read.html
- MySQL 8.0 Reference Manual - Locking Reads: https://dev.mysql.com/doc/refman/8.0/en/innodb-locking-reads.html
- MySQL 8.0 Reference Manual - InnoDB Locking: https://dev.mysql.com/doc/refman/8.0/en/innodb-locking.html
- MySQL 8.0 Reference Manual - Locks Set by Different SQL Statements in InnoDB: https://dev.mysql.com/doc/refman/8.0/en/innodb-locks-set.html
- MySQL 8.0 Reference Manual - Metadata Locking: https://dev.mysql.com/doc/refman/8.0/en/metadata-locking.html
