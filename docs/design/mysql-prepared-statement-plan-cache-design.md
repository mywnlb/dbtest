# MiniMySQL MySQL 8.0 风格 Prepared Statement 与 Plan Cache 模块设计

版本：2026-06-05  
实现语言：Java  
参考基线：MySQL 8.0.46 Prepared Statements、Automatic Reprepare、Statement Metadata、Data Dictionary、Optimizer Statistics  
关联设计：[mysql-parser-binder-design.md](mysql-parser-binder-design.md)、[mysql-query-optimizer-design.md](mysql-query-optimizer-design.md)、[mysql-statistics-analyze-design.md](mysql-statistics-analyze-design.md)、[mysql-data-dictionary-ddl-design.md](mysql-data-dictionary-ddl-design.md)、[mysql-sql-executor-storage-api-design.md](mysql-sql-executor-storage-api-design.md)、[mysql-advanced-executor-operators-design.md](mysql-advanced-executor-operators-design.md)、[mysql-session-connection-protocol-design.md](mysql-session-connection-protocol-design.md)

## 1. 目标与边界

本设计面向 MiniMySQL 的 `sql.prepare` 与可选 `sql.plan.cache` 模块。Prepared Statement 负责 session 内 SQL 模板、参数元数据、绑定模板、执行期参数和生命周期管理；Plan Cache 负责在安全版本边界内复用绑定后或优化后的计划模板。

设计目标：

- 高内聚：prepared statement 生命周期、session registry、参数元数据、parse cache、plan cache、invalidation、reprepare 都有独立边界。
- 低耦合：Prepared 模块编排 Parser/Binder、Optimizer、Executor、DD/MDL、Stats Snapshot，但不访问它们的内部结构。
- MySQL 8.0 风格：server-side prepared statement 是 session 级结构，session 结束释放；表/视图元数据变化后下次执行自动 reprepare。
- 准确表述：MiniMySQL 的共享 PlanCache 是受控扩展，不描述成 MySQL 8.0 原生跨 session 全局执行计划缓存。
- 并发安全：执行中模板由 `PlanUseGuard` 和引用计数保活，DDL/statistics invalidation 只标记 stale，不销毁正在执行的模板。
- 可测试：覆盖 PREPARE/EXECUTE/DEALLOCATE、参数类型、schema stale、statistics stale、并发 deallocate 和 session cleanup。

非目标：

- 不实现完整 MySQL client/server binary protocol。
- 不缓存事务 `ReadView`、row lock、StorageCursor、BufferFrame 或执行期参数值。
- 不保证所有参数值共享同一最优计划；第一阶段按参数类型签名缓存，必要时 reoptimize。
- 不让 PlanCache 替代 Optimizer、Stats 或 Data Dictionary。
- 不生成 Java 源码。本文件只定义设计、关系和数据流。

## 2. MySQL 8.0 参考依据

本设计参考 MySQL 8.0 的以下行为：

- Server-side prepared statement 通过 `PREPARE` 创建，通过 `EXECUTE` 执行，通过 `DEALLOCATE PREPARE` 释放。
- Prepared statement 属于 session；session 结束后自动释放。
- 参数 marker 在 prepare 阶段识别，execute 阶段提供参数值。
- 表或视图 metadata 变化时，MySQL 会在下一次执行时自动 reprepare。
- Prepared statement 的内部结构不应跨 session 泄漏事务、锁、游标或执行状态。

MiniMySQL 扩展：

- `ParseCache` 可跨 statement 复用纯语法 AST。
- `PlanCache` 可作为可选共享缓存，但 key 必须包含 SQL mode、schema、参数类型、DD version、statistics version、optimizer switch 和锁语义。

## 3. 总体架构

架构图见 [prepared-statement-plan-cache-architecture.mmd](diagrams/prepared-statement-plan-cache-architecture.mmd)。

核心链路：

`Session -> PreparedStatementManager -> SessionStatementRegistry -> Parser/Binder -> Optimizer -> PlanCache -> Executor`

职责划分：

- `PreparedStatementManager` 是 Facade，处理 prepare、execute、deallocate、reset、closeSession。
- `SessionStatementRegistry` 是 MySQL 风格 prepared statement 生命周期主所有者。
- `ParseCache` 缓存不可变 `ParsedStatementTemplate`，不含 DD pin、MDL、事务上下文。
- `BoundPlanTemplate` 保存绑定和计划模板，不保存执行期资源。
- `PlanCache` 可缓存 `BoundPlanTemplate` 或 `PhysicalPlanTemplate`；复杂计划模板可以包含高级执行算子的不可变骨架，但不能缓存 operator 运行期状态。
- `InvalidationListener` 监听 DD 和 statistics 事件，标记 stale。
- `PlanUseGuard` 管理执行中引用。

## 4. 包与职责

| 包 | 职责 | 依赖 | 主要模式 |
| --- | --- | --- | --- |
| `sql.prepare.api` | PreparedStatementManager、prepare/execute/deallocate | registry, cache | Facade |
| `sql.prepare.session` | session statement registry、配额、cleanup | session | Repository |
| `sql.prepare.template` | parsed/bound plan template、参数元数据 | parser, binder | Flyweight |
| `sql.prepare.param` | parameter metadata、binding set、类型校验 | type | Strategy |
| `sql.prepare.lifecycle` | 状态机、reprepare、deallocate、reset | session | State, Template Method |
| `sql.plan.cache` | plan cache key、entry、segment、eviction | optimizer, stats | Cache, State |
| `sql.plan.invalidate` | DD/statistics invalidation listener | dd, stats | Observer |
| `sql.plan.guard` | plan use guard、refcount、retire | cache | RAII Guard |
| `sql.prepare.metric` | cache hit、reprepare、执行次数 | 无 | Observer |

禁止方向：

- Prepared 模块不能访问 BufferFrame、PageStore、row lock queue 或 StorageCursor。
- ParseCache 不能保存 `DictionaryReadView`、MDL ticket 或 DD pin。
- PlanCacheEntry 不能保存事务、ReadView、执行游标或参数值。
- InvalidationListener 不能在持有 PlanCache lock 时等待 MDL。

## 5. 核心领域模型

类关系图见 [prepared-statement-plan-cache-class-relation.mmd](diagrams/prepared-statement-plan-cache-class-relation.mmd)。

| 对象 | 职责 |
| --- | --- |
| `PreparedStatementId` | session 内 statement id 或名称 |
| `PreparedStatementHandle` | session 内句柄，保存状态、模板、refCount、version |
| `PreparedStatementState` | `PREPARING/PREPARED/EXECUTING/STALE/REPREPARING/DEALLOCATING/DEALLOCATED/FAILED` |
| `ParsedStatementTemplate` | 不可变 AST 模板 |
| `ParameterMetadata` | 参数数量、类型、nullable、collation、source span |
| `ParameterBindingSet` | execute 阶段参数值集合 |
| `BoundPlanTemplate` | BoundStatement、dependency set、parameter metadata、result metadata |
| `PhysicalPlanTemplate` | optimizer 输出的可复用计划模板 |
| `PlanCacheKey` | plan cache key，覆盖 schema、mode、version、statistics、参数类型 |
| `DependencySet` | table/index/column/statistics dependency |
| `PlanUseGuard` | 执行中引用计数 guard |
| `InvalidationEvent` | DDL/statistics/table cache flush 事件 |

## 6. 关键数据结构与缓存键

### 6.1 PreparedStatementHandle

字段：

- `statementId`
- `statementName`
- `state`
- `parsedTemplate`
- `boundTemplate`
- `physicalPlanTemplate`
- `parameterMetadata`
- `dependencySet`
- `dictionaryVersionSet`
- `statisticsVersionSet`
- `refCount`
- `lastReprepareReason`

约束：

- handle 属于 session，不跨 session 共享。
- `DEALLOCATE` 只移除 session registry 中的新 acquire 能力；执行中引用等 refCount 归零后回收。
- handle 不保存执行期 `ReadView`、row lock、cursor 或参数值。

### 6.2 PlanCacheKey

Plan cache key 至少包含：

- normalized SQL hash。
- statement kind。
- default schema。
- SQL mode。
- charset/collation。
- optimizer switch。
- parameter type signature。
- dictionary version set。
- statistics version set。
- isolation/locking read semantic flag。

`FOR UPDATE`、`FOR SHARE`、SERIALIZABLE 读、DML current read 等影响锁语义的标记必须进入 key 或 execute-time validation。

### 6.3 PlanCacheEntry

状态：

- `ABSENT`
- `BUILDING`
- `READY`
- `STALE`
- `REPREPARING`
- `EVICTING`
- `RETIRED`

字段：

- `key`
- `template`
- `dependencySet`
- `refCount`
- `state`
- `createdAt`
- `lastAccessAt`
- `hitCount`

## 7. 核心策略和算法

### 7.1 PREPARE / EXECUTE / DEALLOCATE

生命周期流程见 [prepared-statement-plan-cache-lifecycle-flow.mmd](diagrams/prepared-statement-plan-cache-lifecycle-flow.mmd)。

PREPARE：

1. session 调用 `PreparedStatementManager.prepare()`。
2. ParseCache 返回 `ParsedStatementTemplate` 或 Parser 新建。
3. Binder 推断参数元数据和依赖对象。
4. 可选 Optimizer 构建 `PhysicalPlanTemplate`。
5. `SessionStatementRegistry` 发布 `PreparedStatementHandle`。

EXECUTE：

1. 查找 handle。
2. 获取 statement S lock 或 refcount guard。
3. 检查 dictionary/statistics version 是否 stale。
4. stale 时按 `RepreparePolicy` 重新 bind 和 optimize。
5. 校验 `ParameterBindingSet`。
6. 获取 `PlanUseGuard`。
7. Executor 执行。
8. 释放 guard 和 statement 资源。

DEALLOCATE：

1. 查找 handle。
2. 标记 `DEALLOCATING`，阻止新 execute acquire。
3. 如果 refCount 为 0，立即 retire 模板并移除 registry entry。
4. 如果有执行中引用，等待、超时或延迟 cleanup。

### 7.2 Automatic Reprepare

触发条件：

- DDL 改变依赖 table/view/index/column 的 dictionary version。
- ANALYZE 改变依赖 statistics version。
- default schema、SQL mode、charset/collation、optimizer switch 变化。
- 参数类型签名与模板不兼容。

策略：

- 每次 execute 最多 reprepare 有限次数，避免 DDL 频繁变化导致无限循环。
- reprepare 失败时保持旧模板 stale，不继续执行。
- reprepare 成功后原子替换 handle 的 bound/plan template。
- 执行中的旧模板由 refCount 保活。

### 7.3 Invalidation

失效流程见 [prepared-statement-plan-cache-invalidation-flow.mmd](diagrams/prepared-statement-plan-cache-invalidation-flow.mmd)。

失效来源：

- Data Dictionary DDL publish。
- Statistics/ANALYZE publish。
- table definition cache flush。
- manual plan cache flush。

规则：

- InvalidationListener 按 `DependencySet` 匹配 table/index/column。
- 失效只标记 `STALE`，不直接销毁执行中模板。
- 下次 execute 触发 reprepare。
- evict 只能 retire `refCount == 0` 的 entry。

## 8. 与其它模块的协作

### 8.1 与 Parser/Binder

- PREPARE 复用 Parser/Binder 的 AST、参数推断和 BoundStatement。
- ParseCache 只缓存纯 AST。
- Reprepare 重新调用 Binder，不能只替换 version 字段。

### 8.2 与 Optimizer

- PlanCache 可缓存 Optimizer 输出的 `PhysicalPlanTemplate`。
- Optimizer 读取 StatsSnapshot，不保存到 PreparedStatementHandle 的执行期状态。
- 参数值敏感计划第一阶段按类型签名复用；必要时标记低置信度并 reoptimize。

### 8.3 与 Executor

- Executor 每次执行获得新的 execution context、transaction context 和 parameter binding。
- Executor 不修改 template。
- 执行结束释放 `PlanUseGuard`。

### 8.4 与 Data Dictionary / MDL

- prepare/reprepare 期间获取 MDL 和 DD pin。
- execute 期间按 Executor/DDL 语义持有必要 MDL。
- DDL publish 通过 InvalidationListener 标记 stale。

### 8.5 与 Statistics / ANALYZE

- stats version 是 plan key 的一部分。
- ANALYZE publish 后，PlanCache 标记依赖该 table/column/index 的 entry stale。
- Optimizer 下一次读取新 snapshot。

## 9. 并发与锁顺序

锁状态图见 [prepared-statement-plan-cache-lock-state.mmd](diagrams/prepared-statement-plan-cache-lock-state.mmd)。

### 9.1 锁对象

| 对象 | 保护资源 | 持有者 | 死锁域 |
| --- | --- | --- | --- |
| session registry mutex | session statement map | session thread | 不进入事务死锁图 |
| statement lock | 单个 prepared handle 状态 | prepare/execute/deallocate | timeout/error |
| PlanCache segment lock | plan cache bucket | PlanCache | 不能跨外部调用 |
| MDL | schema/table metadata | binder/executor | MetadataWaitGraph |
| DD pin | immutable dictionary object | statement guard | 不进入死锁图 |
| PlanUseGuard | template refCount | executor | 不进入死锁图 |
| row lock | DML/current read | transaction | Wait-For Graph |

### 9.2 标准锁顺序

1. `SessionStatementRegistry` session mutex。
2. `PreparedStatementHandle` statement lock。
3. `PlanCache` segment lock。
4. MDL acquire。
5. DD immutable pin。
6. Statistics snapshot。
7. Optimizer。
8. Executor / Storage API。
9. Transaction row lock wait。

规则：

- 等待 MDL 前必须释放 PlanCache segment lock。
- 持有 PlanCache segment lock 时不能调用 Binder、Optimizer、DD repository 或 Storage。
- `DEALLOCATE` 获取 statement X lock 或标记 `DEALLOCATING`，执行中的 refCount 归零后 cleanup。
- DDL/statistics invalidation 不等待 row lock，不等待 MDL，只标记 stale。
- `ReadView` 每次 execute 按事务模块策略创建或复用，不缓存到 plan template。

### 9.3 状态与持有变化

| 状态 | 持有者 | 持有资源 | 进入条件 | 退出条件 |
| --- | --- | --- | --- | --- |
| `PREPARING` | session | statement lock X | PREPARE 开始 | template 发布或失败 |
| `PREPARED` | session registry | handle | PREPARE 成功 | execute、stale、deallocate |
| `EXECUTING` | executor | PlanUseGuard、statement S/refcount | EXECUTE acquire | 执行完成 |
| `STALE` | invalidation listener | stale marker | DDL/statistics event | reprepare 或 deallocate |
| `REPREPARING` | session | statement lock X | next execute 发现 stale | success 或 failure |
| `DEALLOCATING` | session | statement lock X 或 retire marker | DEALLOCATE | refCount zero |
| `DEALLOCATED` | 无 | 无 handle | cleanup 完成 | registry 无 entry |
| `FAILED` | cleanup owner | error context | prepare/reprepare 失败 | cleanup |

## 10. 异常处理

异常类型：

- `PreparedStatementNotFoundException`
- `PreparedStatementStateException`
- `ParameterCountMismatchException`
- `ParameterTypeMismatchException`
- `PlanCacheStaleException`
- `ReprepareFailedException`
- `PlanCacheEvictionException`
- `ConcurrentDeallocateException`
- `PreparedStatementQuotaExceededException`

异常策略：

- PREPARE 失败：不发布 handle。
- EXECUTE 参数不匹配：不进入 Executor。
- reprepare 失败：本次执行失败，handle 保持 stale 或 failed，按 policy 决定是否可再次尝试。
- deallocate 与 execute 并发：deallocate 阻止新 acquire，等待或延迟回收旧模板。
- PlanCache eviction 失败：不影响 session handle，只降低缓存命中。
- session close：批量标记 deallocating，等待或强制清理无执行中引用的 handle。

## 11. API 设计

### 11.1 PreparedStatementManager

- `prepare(SessionContext session, SqlText sqlText, PreparedStatementName name)`
- `execute(SessionContext session, PreparedStatementName name, ParameterBindingSet bindings)`
- `deallocate(SessionContext session, PreparedStatementName name)`
- `reset(SessionContext session, PreparedStatementName name)`
- `closeSession(SessionContext session)`

### 11.2 SessionStatementRegistry

- `register(PreparedStatementHandle handle)`
- `lookup(PreparedStatementName name)`
- `markDeallocating(PreparedStatementName name)`
- `remove(PreparedStatementName name)`
- `closeAll(SessionId sessionId)`

### 11.3 PlanCache

- `lookup(PlanCacheKey key)`
- `putIfAbsent(PlanCacheKey key, PhysicalPlanTemplate template)`
- `markStale(InvalidationEvent event)`
- `evict(EvictionPolicy policy)`
- `metricsSnapshot()`

### 11.4 RepreparePolicy

- `requiresReprepare(PreparedStatementHandle handle, ExecutionContext context)`
- `maxAttempts()`
- `onReprepareFailure(ReprepareFailure failure)`

## 12. 设计模式使用清单

| 模式 | 应用点 | 价值 |
| --- | --- | --- |
| Facade | `PreparedStatementManager` | 统一生命周期入口 |
| Template Method | prepare/execute lifecycle | 固定 parse-bind-optimize-publish 和 validate-execute-cleanup |
| Strategy | parameter inference、plan key、reprepare、eviction | 控制可变策略 |
| Observer | invalidation listener | 监听 DD/statistics 变化 |
| Flyweight / Cache | parsed/bound/physical template | 不可变模板复用 |
| State | handle 和 cache entry 状态机 | 明确 stale、reprepare、deallocate |
| Builder | `BoundPlanTemplateBuilder` | 分阶段聚合绑定和优化结果 |
| RAII Guard | `PlanUseGuard` | 执行中引用保活和释放 |

## 13. 高内聚、低耦合约束

- `PreparedStatementManager` 不吞并 Parser、Optimizer、Executor 或 DD 职责。
- `ParseCache` 不缓存 DD pin、MDL 或执行上下文。
- `BoundPlanTemplate` 不缓存 `ReadView`、row lock、StorageCursor、BufferFrame。
- `PlanCache` key 必须包含 dictionary/statistics version。
- Invalidation 只标记 stale，不销毁执行中模板。
- session registry 是 prepared handle 主所有者。
- 共享 PlanCache 是可选扩展，不能写成 MySQL 8.0 的原生全局计划缓存。
- 参数值不写入模板。

## 14. 典型数据流

### 14.1 PREPARE

1. session 提交 SQL text。
2. PreparedStatementManager 查 ParseCache 或调用 Parser。
3. Binder 推断参数和依赖对象。
4. Optimizer 可构建初始 plan template。
5. SessionStatementRegistry 发布 handle。

### 14.2 EXECUTE

1. 查找 prepared handle。
2. 检查状态、version 和参数签名。
3. stale 时 reprepare。
4. 绑定参数值。
5. 获取 PlanUseGuard。
6. Executor 创建新的执行上下文和事务/ReadView。
7. 执行完成释放 guard。

### 14.3 DEALLOCATE

1. session 请求 deallocate。
2. handle 标记 `DEALLOCATING`。
3. 阻止新的 execute acquire。
4. refCount 为 0 时 retire 模板并移除 registry entry。
5. session close 时对所有 handle 执行同样流程。

### 14.4 DDL / ANALYZE 失效

1. DDL 或 ANALYZE publish event。
2. InvalidationListener 匹配 DependencySet。
3. PlanCacheEntry 和 handle 标记 stale。
4. 执行中模板继续运行。
5. 下一次 execute reprepare。

## 15. 测试设计

- PREPARE 测试：命名、重复名、参数元数据、session registry。
- EXECUTE 测试：参数数量、类型、NULLability、执行上下文独立。
- DEALLOCATE 测试：正常释放、找不到 handle、执行中 deallocate。
- Session cleanup 测试：session 结束释放所有 prepared statement。
- ParseCache 测试：schema-free AST 复用，不保存 DD pin。
- PlanCache key 测试：SQL mode、schema、参数类型、DD/statistics version。
- DDL invalidation 测试：alter/drop/rename 后 stale 和 reprepare。
- ANALYZE invalidation 测试：StatsVersion 改变后计划 stale。
- Reprepare 测试：成功替换、失败清理、最大重试次数。
- 并发测试：execute 与 deallocate、invalidation 与 execute、cache eviction 与 refCount。
- 隔离级别测试：ReadView 不缓存，execute-time 创建或复用。
- 故障注入：reprepare 期间 DD 变化、optimizer 失败、executor 失败。

## 16. 后续实现顺序

1. `PreparedStatementId`、`PreparedStatementState`、`ParameterMetadata`。
2. `ParsedStatementTemplate` 和 ParseCache adapter。
3. `SessionStatementRegistry`。
4. `PreparedStatementHandle` 状态机。
5. `PreparedStatementManager.prepare()`。
6. `ParameterBindingSet` 校验。
7. `execute()` 最小路径。
8. `deallocate()` 和 session cleanup。
9. `DependencySet`。
10. `PlanCacheKey`。
11. `PlanCache` segment lock 和 entry 状态。
12. `InvalidationListener` 接入 DD。
13. statistics invalidation 接入。
14. `RepreparePolicy`。
15. 并发、eviction、故障注入测试。

## 17. 十五轮自检记录

| 轮次 | 检查主题 | 检查结果 |
| --- | --- | --- |
| 1 | 是否只写设计文档 | 本文只新增 Markdown 和 Mermaid 设计内容，没有生成 Java 实现代码 |
| 2 | 目标与非目标 | 已明确 prepared statement、parse cache、plan cache 的范围和非目标 |
| 3 | MySQL 8.0 贴合 | 已说明 session 级 prepared statement、deallocate、automatic reprepare |
| 4 | 高内聚 | manager、registry、template、param、plan cache、invalidation、guard 职责独立 |
| 5 | 低耦合 | 不访问 parser/optimizer/executor/DD/storage 内部结构 |
| 6 | 面向对象 | 已定义 handle、template、parameter metadata、plan key、dependency set、guard |
| 7 | 设计模式 | 已列出 Facade、Template Method、Strategy、Observer、Flyweight、State、Builder、Guard |
| 8 | 核心领域模型 | 已覆盖 prepared handle、plan cache entry、parameter binding 和 invalidation event |
| 9 | 依赖方向 | 已明确 Prepared 编排 Parser/Binder、Optimizer、Executor、DD/Stats 的单向接口 |
| 10 | 物理与逻辑区分 | 已区分 SQL template、bound plan、physical plan template、execution context 和 storage resource |
| 11 | 关键数据流 | 已给出 PREPARE、EXECUTE、DEALLOCATE、DDL/ANALYZE 失效流程 |
| 12 | 图示 | 已提供架构图、类关系图、生命周期、失效和锁状态图 |
| 13 | 并发锁状态 | 已定义 session mutex、statement lock、PlanCache lock、MDL、PlanUseGuard 的顺序和状态 |
| 14 | 异常与恢复 | 已覆盖参数错误、reprepare 失败、并发 deallocate、eviction 和 session cleanup |
| 15 | 测试与顺序 | 已给出测试设计、实现顺序，并确认没有未完成标记或空白项 |

## 18. 参考链接

- MySQL 8.0 Reference Manual - Prepared Statements: https://dev.mysql.com/doc/refman/8.0/en/sql-prepared-statements.html
- MySQL 8.0 Reference Manual - Caching of Prepared Statements and Stored Programs: https://dev.mysql.com/doc/refman/8.0/en/statement-caching.html
- MySQL 8.0 Reference Manual - DEALLOCATE PREPARE Statement: https://dev.mysql.com/doc/refman/8.0/en/deallocate-prepare.html
- MySQL 8.0 Source Documentation - `sql_prepare.cc`: https://dev.mysql.com/doc/dev/mysql-server/8.0.46/sql__prepare_8cc.html
- MySQL 8.0 Reference Manual - Metadata Locking: https://dev.mysql.com/doc/refman/8.0/en/metadata-locking.html
