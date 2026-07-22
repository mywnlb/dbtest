# MiniMySQL MySQL 8.0 风格 Data Dictionary、DDL 与 MDL 模块设计

版本：2026-06-05  
实现语言：Java  
参考基线：MySQL 8.0.46 Data Dictionary、Atomic DDL、Metadata Locking  
关联设计：[mysql-parser-binder-design.md](mysql-parser-binder-design.md)、[mysql-statistics-analyze-design.md](mysql-statistics-analyze-design.md)、[innodb-crash-recovery-design.md](innodb-crash-recovery-design.md)、[innodb-storage-engine-overview.md](innodb-storage-engine-overview.md)、[innodb-btree-design.md](innodb-btree-design.md)、[innodb-record-design.md](innodb-record-design.md)、[innodb-transaction-mvcc-design.md](innodb-transaction-mvcc-design.md)、[innodb-secondary-index-mvcc-purge-design.md](innodb-secondary-index-mvcc-purge-design.md)、[innodb-redo-log-design.md](innodb-redo-log-design.md)、[innodb-disk-manager-design.md](innodb-disk-manager-design.md)、[innodb-buffer-pool-design.md](innodb-buffer-pool-design.md)、[mysql-session-connection-protocol-design.md](mysql-session-connection-protocol-design.md)、[mysql-lock-observability-deadlock-design.md](mysql-lock-observability-deadlock-design.md)

## 1. 目标与边界

本设计面向 MiniMySQL 的 SQL/server 元数据层，覆盖 Data Dictionary、DDL 执行编排和 Metadata Lock。它负责把 SQL 层看到的 schema、table、column、index、constraint、tablespace 元数据，转换为存储引擎可使用的稳定定义，并把 DDL 对元数据和 InnoDB 物理结构的修改组织成可恢复、可回滚、可并发控制的流程。

设计目标：

- 高内聚：元数据对象、字典事务、字典缓存、DDL 编排、MDL 锁和 DDL recovery 分别收敛到明确子包。
- 低耦合：SQL 执行器只通过 `DataDictionaryService` 查询元数据；InnoDB 模块只通过 `InnoDBDictionaryBridge` 接收物理创建、删除和绑定请求。
- MySQL 8.0 风格：对齐事务型 data dictionary、dictionary object cache、atomic DDL、MDL、SDI 和 `INFORMATION_SCHEMA` 集成思想。
- Java 可落地：用领域对象、值对象、Repository、Unit of Work、State、Facade、Observer、Command、Strategy 表达元数据和 DDL 生命周期。
- 可恢复：DDL 日志、字典事务、InnoDB 物理操作和 cache publish 必须有明确 crash recovery 顺序。
- 并发清晰：MDL 锁状态、锁持有者、等待边界、死锁检测域和与事务行锁的关系必须明确。

非目标：

- 不实现 SQL parser、optimizer、executor 的完整规则，只定义它们访问字典和发起 DDL 的接口。
- 不直接修改 BufferFrame、record byte、XDES、segment inode 或 redo file。
- 第一阶段不实现完整 online DDL、foreign key cascade、partition pruning、view/stored routine/user/role 元数据。
- 第一阶段不实现 binlog 和 replication；atomic DDL 中的 binlog 阶段作为扩展点保留。
- 不生成 Java 源码。本文件只定义设计、关系和数据流。

## 2. MySQL 8.0 参考依据

本设计参考 MySQL 8.0 的以下行为：

- MySQL 8.0 使用事务型 Data Dictionary，集中保存数据库对象元数据，替代旧版本分散的文件型 metadata 和部分非事务表。
- Data Dictionary 表位于 `mysql` 数据库，字典表存储在单独的 InnoDB tablespace `mysql.ibd` 中；字典数据受 InnoDB commit、rollback 和 crash recovery 保护。
- Data Dictionary 提供对象缓存，`INFORMATION_SCHEMA` 和 `SHOW` 通过字典数据暴露元信息，而不是让用户直接修改字典表。
- Atomic DDL 把 data dictionary 更新、storage engine 操作和 binlog 写入组合成原子操作；本项目第一阶段不实现 binlog，但保留同等阶段边界。
- Atomic DDL 不是事务型 DDL；DDL 执行前会隐式结束当前用户事务。
- InnoDB 支持 Atomic DDL，并通过 DDL log 支持 DDL 操作的 redo 和 rollback。
- Metadata Lock 用于保护 schema、table、tablespace 等对象的并发访问；Performance Schema 中可观察 `PENDING`、`GRANTED`、`VICTIM`、`TIMEOUT`、`KILLED` 等状态。
- MDL lock type 包括 `SHARED_READ`、`SHARED_WRITE`、`SHARED_UPGRADABLE`、`EXCLUSIVE` 等；lock duration 包括 statement、transaction、explicit。
- SDI 为 InnoDB tablespace 文件提供序列化字典信息冗余，DDL 操作会维护 SDI。

简化点：

| MySQL 8.0 能力 | MiniMySQL 第一阶段 |
| --- | --- |
| 完整系统字典表和 upgrade 框架 | 只设计 schema/table/column/index/constraint/tablespace 核心对象 |
| binlog 参与 atomic DDL | 保留 `BinaryLogParticipant` 扩展点，第一阶段不实现 |
| 完整 online DDL 和 instant DDL | 先支持 copy/rebuild 风格和有限 inplace index build |
| 全量 Performance Schema instrumentation | 先暴露 `MetadataLockSnapshot` 和 `DictionaryCacheMetrics` |
| 复杂 foreign key 语义 | 第一阶段只保存 constraint 元数据和依赖锁顺序 |

## 3. 总体架构

架构图见 [data-dictionary-architecture.mmd](diagrams/data-dictionary-architecture.mmd)。

Data Dictionary 位于 SQL/session 与 InnoDB 存储模块之间：

1. SQL parser 产生 DDL AST，`DdlPlanner` 解析对象名、列定义、索引定义和约束定义。
2. `DdlCoordinator` 获取 MDL，完成校验，生成 `DdlPlan`。
3. `DataDictionaryService` 通过 cache 和 repository 读取或发布字典对象。
4. `DictionaryTransaction` 把字典变更作为内部事务提交或回滚。
5. `InnoDBDictionaryBridge` 调用 Disk Manager、B+Tree、Record、Transaction、Redo 完成物理对象创建、删除、root page 绑定和 redo/DDL log。
6. `SerializedDictionaryInfoService` 写入或更新 SDI。
7. `InformationSchemaAdapter` 从字典对象构造只读元数据视图。

依赖方向：

`sql.session -> sql.ddl -> sql.dd -> storage.api -> storage.btree/record/trx/buf/fsp/fil/redo`

禁止反向依赖：

- `storage.btree` 不能反向读取 `DictionaryObjectCache`。
- `storage.record` 不能解析 `TableDefinition` 的 SQL 默认值和 generated column 表达式。
- `storage.buf` 不能缓存 `TableDefinition`。
- `storage.redo` 不能调用 `DataDictionaryService` 执行逻辑 DDL。
- `MetadataLockManager` 不能在持有内部锁时进入 Buffer Pool 或 InnoDB row lock wait。

## 4. 包与职责

| 包 | 职责 | 依赖 | 主要模式 |
| --- | --- | --- | --- |
| `sql.dd.api` | `DataDictionaryService`、查询、发布、失效、schema version | cache, repo | Facade |
| `sql.dd.domain` | schema/table/column/index/constraint/tablespace 值对象 | 无 | Value Object, Aggregate |
| `sql.dd.cache` | 字典对象缓存、pin、evict、version visibility | domain | Repository Cache, Observer |
| `sql.dd.repo` | 字典表读写、object id 分配、名称索引 | trx, btree | Repository |
| `sql.dd.tx` | 字典事务、变更 staging、commit/rollback | repo, trx, redo | Unit of Work |
| `sql.dd.ddl` | DDL plan、DDL coordinator、atomic DDL 阶段编排 | mdl, api, engine | Command, Template Method |
| `sql.dd.mdl` | 元数据锁、等待队列、死锁检测、诊断快照 | session | Mediator, State |
| `sql.dd.sdi` | SDI 编码、写入、校验、导出 | engine bridge | Adapter, Serializer |
| `sql.dd.info_schema` | `INFORMATION_SCHEMA` 和 `SHOW` 只读视图 | api, cache | Adapter |
| `sql.dd.recovery` | DDL log 扫描、未完成 DDL cleanup、cache rebuild | redo, engine | Chain of Responsibility |
| `sql.dd.engine` | 到 InnoDB 物理模块的桥接接口 | storage.api | Bridge, Strategy |

## 5. 核心领域模型

类关系图见 [data-dictionary-class-relation.mmd](diagrams/data-dictionary-class-relation.mmd)。

### 5.1 标识对象

| 对象 | 含义 |
| --- | --- |
| `DictionaryObjectId` | 字典对象全局 ID，内部生成，不随 rename 改变 |
| `SchemaId` | schema 对象 ID |
| `TableId` | table 对象 ID |
| `IndexId` | index 对象 ID |
| `TablespaceId` | tablespace 字典 ID，映射到 `SpaceId` |
| `DictionaryVersion` | 字典对象版本，用于 cache visibility 和 DDL publish |
| `ObjectName` | `catalog.schema.name` 规范化名称 |

设计规则：

- ID 是稳定物理身份，name 是逻辑可变属性。
- DDL rename 只创建新 name mapping 和新 version，不改变 table id。
- SQL 层不能使用 `SpaceId + PageNo` 代表 table；必须使用 `TableId` 或 `TableDefinition`。

### 5.2 逻辑元数据对象

| 对象 | 职责 |
| --- | --- |
| `SchemaDefinition` | schema 名称、默认 charset/collation、创建者、版本 |
| `TableDefinition` | table 名称、列集合、主键、索引、约束、表选项、storage binding |
| `ColumnDefinition` | 列名、类型、NULL、默认值、charset/collation、ordinal、隐藏属性 |
| `IndexDefinition` | index 名、唯一性、key parts、聚簇/二级、可见性、root binding |
| `ConstraintDefinition` | primary key、unique、foreign key、check 的元数据 |
| `TablespaceDefinition` | tablespace 名、engine、space id、文件路径策略、状态 |
| `StorageBinding` | `TableId/IndexId -> SpaceId/SegmentId/rootPageId` 的桥接信息 |

`TableDefinition` 是聚合根。`ColumnDefinition`、`IndexDefinition`、`ConstraintDefinition` 不允许脱离 table 独立发布。

### 5.3 DDL 运行时对象

| 对象 | 职责 |
| --- | --- |
| `DdlRequest` | 从 AST 转换来的 DDL 输入，已经规范化对象名 |
| `DdlPlan` | DDL 阶段、锁需求、字典变更、物理引擎动作 |
| `DdlContext` | 当前 session、statement id、ddl id、timeout、trace |
| `DictionaryMutation` | 对字典对象的 add/update/delete/version publish |
| `DdlLogRecord` | DDL recovery 可重放或回滚的阶段记录 |
| `EngineDdlParticipant` | 存储引擎参与者接口，支持 prepare/commit/rollback |

## 6. 关键数据结构与逻辑/物理映射

### 6.1 字典表设计

第一阶段字典表只作为设计对象，不要求完全复刻 MySQL 内部表结构：

| 字典表 | 主要字段 | 说明 |
| --- | --- | --- |
| `dd_schemata` | `schema_id`, `name`, `default_charset`, `version` | schema 定义 |
| `dd_tables` | `table_id`, `schema_id`, `name`, `engine`, `version`, `state` | table 聚合根 |
| `dd_columns` | `table_id`, `ordinal`, `name`, `type`, `nullable`, `default_expr` | 列定义 |
| `dd_indexes` | `index_id`, `table_id`, `name`, `type`, `unique`, `root_page_id` | index 定义和 root 绑定 |
| `dd_index_columns` | `index_id`, `ordinal`, `column_id`, `prefix_len`, `order` | index key part |
| `dd_constraints` | `constraint_id`, `table_id`, `type`, `referenced_table_id`, `expr` | 约束元数据 |
| `dd_tablespaces` | `tablespace_id`, `space_id`, `name`, `state`, `path_policy` | tablespace 元数据 |
| `dd_storage_bindings` | `object_id`, `space_id`, `segment_id`, `root_page_id` | 逻辑对象到物理结构映射 |
| `dd_ddl_log` | `ddl_id`, `phase`, `object_id`, `payload`, `lsn` | atomic DDL recovery |

### 6.2 逻辑与物理区分

| 层面 | 对象 | 由谁维护 | 说明 |
| --- | --- | --- | --- |
| 逻辑 schema | `SchemaDefinition`、`TableDefinition`、`ColumnDefinition` | Data Dictionary | SQL 可见定义 |
| 逻辑索引 | `IndexDefinition`、key part、约束 | Data Dictionary / B+Tree 协作 | 决定查询和 DML 的索引语义 |
| 物理绑定 | `StorageBinding` | DD + InnoDB bridge | table/index 到 `SpaceId/SegmentId/rootPageId` |
| 物理空间 | tablespace、segment、extent、page | Disk Manager | 不理解 SQL 列定义 |
| 页内格式 | compact/dynamic record、PageDirectory | Record | 不维护 schema cache |
| 跨页结构 | B+Tree root/leaf/non-leaf | B+Tree | 使用 DD 提供的 index metadata |

转换模式：

- `TableDefinition -> StorageBinding`：通过 `EngineDdlParticipant.createTableStorage()` 完成。
- `IndexDefinition -> rootPageId`：通过 `BTreeIndexBuilder.createRoot()` 完成。
- `ColumnDefinition -> RecordLayout`：通过 `RecordLayoutFactory` 和 `TypeCodecRegistry` 完成。
- `TablespaceDefinition -> SpaceId`：通过 `DiskSpaceManager.createTablespace()` 完成。

## 7. 核心策略和算法

### 7.1 字典对象缓存

`DictionaryObjectCache` 目标：

- 避免每条 SQL 都读取字典表。
- 保证 DDL publish 前，旧事务仍可使用已 pin 的旧版本定义。
- 支持按 `ObjectName` 和 `DictionaryObjectId` 查询。
- 支持 DDL commit 后 invalidation 和新 version 发布。

缓存状态：

| 状态 | 含义 |
| --- | --- |
| `ABSENT` | cache 中无对象 |
| `LOADING` | 单线程从 repository 加载，其它线程等待 future |
| `READY` | 可读对象，带 version |
| `PINNED` | 正被 statement 或 transaction 使用，不可淘汰 |
| `STALE` | DDL 发布新版本后旧对象仍被 pin |
| `EVICTED` | 无 pin 后从 cache 移除 |

### 7.2 字典事务

`DictionaryTransaction` 是内部 Unit of Work：

1. 分配 ddl id 和 object id。
2. staging `DictionaryMutation`。
3. 写 `dd_ddl_log(PREPARE)`。
4. 调用 storage engine prepare。
5. 写字典表变更。
6. 提交内部事务并写 redo。
7. 标记 DDL log committed。
8. 发布 cache invalidation 或新 version。

异常路径：

- prepare 前失败：释放 MDL，不写字典事务。
- engine prepare 后失败：调用 engine rollback，标记 DDL log rollback。
- 字典提交后 crash：recovery 根据 DDL log 和字典版本完成 cache rebuild 和 orphan cleanup。

### 7.3 Atomic DDL Template

DDL 流程图见 [data-dictionary-ddl-flow.mmd](diagrams/data-dictionary-ddl-flow.mmd)。

`DdlCoordinator` 使用 Template Method 固定阶段：

1. `implicitCommitUserTransaction()`
2. `acquireMetadataLocks()`
3. `validateDictionaryState()`
4. `buildPlan()`
5. `appendDdlLogPrepare()`
6. `prepareEngineChanges()`
7. `stageDictionaryMutations()`
8. `writeSdiIfNeeded()`
9. `commitDictionaryTransaction()`
10. `markDdlLogCommitted()`
11. `publishCacheVersion()`
12. `releaseMetadataLocks()`

各 DDL 语句只实现 plan 和 engine action 的差异，不能改变公共提交顺序。

## 8. DDL 语句设计

### 8.1 CREATE TABLE

流程：

1. 隐式提交当前用户事务。
2. 获取 schema `MDL_SHARED_READ`，目标 table name `MDL_EXCLUSIVE`。
3. 校验 schema 存在、table name 不存在、列和索引定义合法。
4. 分配 `TableId`、clustered `IndexId`、`TablespaceId` 或复用 file-per-table 策略。
5. 通过 Disk Manager 创建 tablespace 和 segment。
6. 通过 B+Tree 创建聚簇索引 root page。
7. 通过 Record 模块校验 row format 和 column codec。
8. staging table、column、index、storage binding。
9. 写 SDI。
10. 提交字典事务，发布 cache。

失败恢复：

- 若 tablespace 已创建但字典未提交，recovery 依据 DDL log drop orphan tablespace。
- 若字典提交但 cache 未发布，startup rebuild cache。

### 8.2 DROP TABLE

流程：

1. 获取 table `MDL_EXCLUSIVE`。
2. 标记 table state 为 `DROP_PENDING`，阻止新 statement pin。
3. 等待旧 pin 和活跃 statement 退出。
4. staging 删除 table、columns、indexes、constraints、binding。
5. 调用 InnoDB bridge discard/drop storage。
6. 提交字典事务并发布 cache invalidation。

规则：

- drop 不允许在持有 Buffer Pool page latch 或 row lock 时等待 MDL。
- 如果多个 table 一起 drop，全部 table 先按规范化 name 排序获取 MDL，再执行 atomic 计划。
- 第一阶段如果任一 table 不存在且未指定 `IF EXISTS`，整个 drop 失败，不做部分删除。

### 8.3 ALTER TABLE

第一阶段支持：

- add/drop column 需要 rebuild table。
- add/drop secondary index。
- rename table。
- change table option 中只支持 comment 和 charset/collation。

ALTER 策略：

| 类型 | 策略 | 说明 |
| --- | --- | --- |
| metadata-only rename/comment/options | `INSTANT_METADATA` | target SDI作为恢复manifest，FORWARD_ONLY后提交字典/cache |
| single add/drop secondary index | `INPLACE_INDEX` | 复用Online ADD row-log或Online DROP retirement协议 |
| add/drop column、convert charset | `SHADOW_REBUILD_V1` | SU下bounded copy，clustered identity journal收敛并延迟回收旧space |
| multi-index / mixed action | `INPLACE_INDEX`或`SHADOW_REBUILD_V1` | 一个manifest/descriptor generation/target DD version原子发布完整action集合 |
| unsupported instant alter | `UnsupportedAlterStrategy` | 明确异常，不静默降级 |

### 8.4 CREATE INDEX

流程：

1. 获取 table `MDL_SHARED_UPGRADABLE`，短暂升级到 `MDL_EXCLUSIVE`，冻结旧 aggregate、table gate、
   immutable manifest、DDL marker 和 staged descriptor。
2. `CAPTURING` force 后把同一 ticket 降回 `MDL_SHARED_UPGRADABLE`；base scan 分批构建 B+Tree，
   并发 DML 在事务提交/XA PREPARE 前把 before/after candidate force 到 row log。
3. final cutover 再升级到 `MDL_EXCLUSIVE`，seal table gate 和 row log，按当前聚簇真相 reconcile candidate。
4. 双向验证 source/target，满足 WAL 与表空间 force 后持久 `RECONCILED`，再推进 `ENGINE_DONE`。
5. 按 SDI、DD exact add-one-index、cache、footer 和 terminal marker 的顺序发布新 version。
6. crash recovery 在用户流量开放前重建 PREPARED generation，或对 ENGINE_DONE/committed DD 只前滚。

当前 Online ADD INDEX v1 的完整并发、row-log、事务/XA 与恢复协议见
[mysql-online-add-index-design.md](mysql-online-add-index-design.md)。marker v4 schema digest、统一control CAS、
Java/admin可观察/取消、Online DROP retirement以及通用ALTER策略矩阵见
[mysql-online-ddl-evolution-design.md](mysql-online-ddl-evolution-design.md)。其中digest/control、Online DROP、
instant metadata、版本化multi-index descriptor chain与online shadow rebuild均已进入生产；复制binlog、
主键/任意类型/foreign/generated、外排/并行build与持久scan continuation仍未实现。

## 9. 与其它模块的协作

### 9.1 与 SQL Executor

- Executor 在 statement 开始时通过 `DataDictionaryService.lookupTable()` pin `TableDefinition`。
- DML 执行期间持有相应 MDL，例如 `SELECT` 持有 `MDL_SHARED_READ`，`INSERT/UPDATE/DELETE` 持有 `MDL_SHARED_WRITE`。
- Statement 结束或事务结束后释放 MDL 和 dictionary pin。

### 9.2 与 B+Tree

- B+Tree 接收 `IndexDefinition` 和 `StorageBinding`。
- B+Tree 只返回 root page 创建、split/merge、scan、insert/delete 的物理结果。
- B+Tree 不更新字典表，不判断 column default，不维护 MDL。

### 9.3 与 Record

- Record 通过 `RecordLayout` 使用 column type、NULL bitmap、hidden column 定义。
- Record 不读取 `dd_columns`，不缓存 `TableDefinition`。
- DDL 改变 row layout 时，必须发布新 `DictionaryVersion`，旧 statement 不能混用新 layout。

### 9.4 与 Transaction/MVCC

- DDL 执行前隐式提交用户事务。
- 字典事务是内部事务，不和用户事务混合。
- MDL 是元数据逻辑锁，不是 record/gap/next-key lock。
- 字典表自身修改产生 undo/redo，并受 crash recovery 保护。

### 9.5 与 Redo 和 DDL Log

- DDL log 记录 DDL 物理动作和字典提交阶段。
- 修改字典表、DDL log 和 SDI 必须 redo-logged。
- DDL log 的关键阶段 redo 需要强制 durable，避免物理对象变化无法恢复。
- Redo recovery 先恢复字典页和 DDL log，再由 DDL recovery 完成 cleanup。

### 9.6 与 Disk Manager / Buffer Pool

- Disk Manager 创建/删除 tablespace、segment、root page。
- Buffer Pool 只缓存字典表页和用户表页，不理解 dictionary object 语义。
- DDL drop/truncate 必须先通过 MDL 阻止新访问，再通知 Buffer Pool 标记相关 frame stale。

## 10. 并发与锁顺序

MDL 状态图见 [metadata-lock-state.mmd](diagrams/metadata-lock-state.mmd)。

### 10.1 MDL 锁类型

| 锁类型 | 用途 | 典型语句 |
| --- | --- | --- |
| `MDL_INTENTION_EXCLUSIVE` | schema 或 tablespace 层意向写 | CREATE/DROP object |
| `MDL_SHARED_READ` | 允许并发读，阻止 EXCLUSIVE DDL | SELECT |
| `MDL_SHARED_WRITE` | 允许 DML 写，阻止 EXCLUSIVE DDL | INSERT/UPDATE/DELETE |
| `MDL_SHARED_UPGRADABLE` | 可升级的 DDL 准备锁 | CREATE INDEX / ALTER |
| `MDL_SHARED_NO_WRITE` | 阻止 DML 写但允许读 | 部分 ALTER 准备阶段 |
| `MDL_EXCLUSIVE` | 独占元数据修改 | DROP/RENAME/DDL publish |

### 10.2 MDL 状态与持有变化

| 状态 | 持有者 | 持有资源 | 进入条件 | 退出条件 |
| --- | --- | --- | --- | --- |
| `FREE` | 无 | 无 request | session 未请求 MDL | 构造 `MdlRequest` |
| `REQUESTED` | session | request 值对象 | statement/DDL 需要元数据锁 | 进入 manager |
| `PRE_ACQUIRE_NOTIFY` | MDL manager | 通知权 | 准备获取锁 | storage participant 接受或拒绝 |
| `GRANTED` | session / statement / transaction | schema/table/tablespace metadata lock | 与已授予锁兼容 | statement、transaction 或 explicit release |
| `PENDING` | MDL wait queue | wait slot、metadata wait edge | 与已授予锁冲突 | grant、victim、timeout、killed |
| `VICTIM` | MDL deadlock detector | victim 标记 | metadata wait graph 成环 | cleanup request |
| `TIMEOUT` | MDL manager | timeout cleanup 权 | 超过 `lock_wait_timeout` | cleanup request |
| `KILLED` | session killer | killed 标记 | session 被 kill | cleanup request |
| `POST_RELEASE_NOTIFY` | MDL manager | 通知权 | 已释放 granted lock | storage participant 完成通知 |
| `RELEASED` | 无 | 无有效锁 | 正常释放或异常清理 | 回到 free |

持有变化规则：

- `acquire`：获取 MDL 前不能持有 Buffer Pool page latch、物理文件锁、MTR latch 或 row-lock shard lock。
- `wait`：等待 MDL 时只持有 `MdlRequest` 和 wait slot；不持有 InnoDB page latch 或 record cursor。
- `grant`：授予的 MDL 归 session/statement/transaction scope 所有，不归 `TableDefinition` 对象所有。
- `upgrade`：`MDL_SHARED_UPGRADABLE -> MDL_EXCLUSIVE` 失败时进入 `PENDING`，不能边持有 row lock 边等待。
- `release`：DDL 成功或回滚后释放 EXCLUSIVE；DML statement/transaction 按 duration 释放 SHARED。
- `cleanup release`：victim、timeout、killed 都必须从 wait queue 和 diagnostic graph 移除。

### 10.3 锁顺序与死锁检测域

全局顺序：

1. Global read/write gate 或 backup gate。
2. Schema MDL。
3. Table MDL。
4. Tablespace MDL。
5. Dictionary cache object pin。
6. Dictionary transaction short mutex。
7. InnoDB engine 操作中的 page/file/row lock，按存储引擎总览规则执行。

死锁检测：

- MDL 等待进入 `MetadataWaitGraph`。
- record/gap/next-key/insert intention lock 进入 InnoDB `WaitForGraph`。
- 第一阶段两个图分开检测，但通过 `SqlWaitGraphFacade` 输出统一诊断快照。
- DDL 在等待 MDL 时不能持有行锁；DML 在等待行锁前已经持有 MDL，但 MDL 不反向等待该 DML 的 row lock。
- Buffer Pool latch、物理文件锁、MTR latch、redo wait 和 PageLoadFuture 不进入 MDL 死锁图。

## 11. 异常处理与恢复策略

Atomic DDL recovery 流程见 [atomic-ddl-recovery-flow.mmd](diagrams/atomic-ddl-recovery-flow.mmd)。

异常类型：

- `DictionaryObjectNotFoundException`
- `DictionaryObjectExistsException`
- `DictionaryVersionConflictException`
- `MetadataLockTimeoutException`
- `MetadataDeadlockException`
- `DdlValidationException`
- `DdlRollbackException`
- `DdlRecoveryException`
- `SdiMismatchException`
- `UnsupportedDdlException`

恢复顺序：

1. Redo replay 恢复字典表页、DDL log 页、SDI 页和用户表物理页。
2. `DdlRecoveryService` 扫描 `dd_ddl_log`。
3. 对 `COMMITTED` DDL，确认字典对象和 storage binding 完整，并重建 cache。
4. 对 `PREPARE/ENGINE_DONE` 未完成 DDL，根据 operation policy finish 或 rollback。
5. 对 orphan tablespace、orphan root page、drop pending 对象执行 quarantine 或 cleanup。
6. 校验 SDI 和字典 version；不一致时以字典事务 committed 状态为准，SDI 可重写。
7. 完成 recovery 后才允许普通 SQL session 进入。

## 12. API 设计

### 12.1 DataDictionaryService

- `lookupSchema(ObjectName)`
- `lookupTable(ObjectName, DictionaryReadView)`
- `lookupTable(TableId, DictionaryReadView)`
- `lookupIndex(IndexId, DictionaryReadView)`
- `pinTable(TableId, SessionContext)`
- `releasePin(DictionaryPin)`
- `beginDictionaryTransaction(DdlContext)`
- `publish(DictionaryMutationBatch)`
- `invalidate(DictionaryObjectId, DictionaryVersion)`

### 12.2 MetadataLockManager

- `acquire(MdlRequest, Duration)`
- `tryAcquire(MdlRequest)`
- `upgrade(MdlTicket, MdlMode, Duration)`
- `release(MdlTicket)`
- `releaseAll(SessionId)`
- `snapshotLocks()`
- `detectDeadlocks()`

### 12.3 DdlCoordinator

- `execute(DdlRequest, SessionContext)`
- `buildPlan(DdlRequest, DictionaryReadView)`
- `prepareEngine(DdlPlan)`
- `commitAtomic(DdlPlan)`
- `rollbackAtomic(DdlPlan, Throwable)`

### 12.4 InnoDBDictionaryBridge

- `createTableStorage(TableDefinition, DdlContext)`
- `dropTableStorage(TableId, StorageBinding, DdlContext)`
- `createIndexStorage(IndexDefinition, StorageBinding, DdlContext)`
- `rebuildTableStorage(TableDefinition, AlterPlan, DdlContext)`
- `writeSdi(TableDefinition, StorageBinding, DdlContext)`
- `recoverDdl(DdlLogRecord)`

## 13. 设计模式使用清单

| 模式 | 应用点 | 价值 |
| --- | --- | --- |
| Facade | `DataDictionaryService` | 隐藏 cache、repo、transaction 细节 |
| Repository | `DictionaryRepository` | 封装字典表读写 |
| Unit of Work | `DictionaryTransaction` | staging、commit、rollback 字典变更 |
| Template Method | `DdlCoordinator` | 固定 atomic DDL 阶段 |
| Command | `DdlPlan` / `DictionaryMutation` | 把 DDL 操作表达为可恢复命令 |
| State | MDL request、cache entry、DDL log phase | 明确状态转移 |
| Observer | cache invalidation、DDL event listener | DDL commit 后通知依赖模块 |
| Strategy | `AlterTableStrategy`、tablespace policy | 选择 rebuild/inplace/metadata-only |
| Adapter | `InformationSchemaAdapter`、`SerializedDictionaryInfoService` | 把字典对象适配为外部视图或 SDI |
| Bridge | `InnoDBDictionaryBridge` | 隔离 SQL 字典层和 InnoDB 物理层 |

## 14. 高内聚、低耦合约束

- DD 只维护元数据定义和版本，不直接解释 record page bytes。
- DDL coordinator 只编排阶段，不持久化裸页。
- MDL 只保护元数据对象，不替代事务行锁。
- Dictionary cache 只缓存不可变对象版本；DDL publish 用新版本替换，不原地修改被 pin 对象。
- InnoDB bridge 是唯一能把 `TableDefinition` 转成 physical storage request 的入口。
- Recovery 只根据 DDL log 和字典事务状态处理未完成 DDL，不重新执行 SQL parser。

## 15. 典型数据流

### 15.1 SELECT 打开表

1. Session 请求 table `MDL_SHARED_READ`。
2. `DataDictionaryService.lookupTable()` 从 cache 命中或 repository 加载。
3. 返回 pinned `TableDefinition` 和 `DictionaryVersion`。
4. Executor 构造 B+Tree scan 或 point lookup。
5. Statement 结束释放 pin 和 statement duration MDL。

### 15.2 INSERT 打开表

1. Session 请求 table `MDL_SHARED_WRITE`。
2. Executor 获取 `TableDefinition`，使用 column default 和 index metadata 构造逻辑行。
3. B+Tree/Record/Transaction 完成 insert intention、record insert 和 undo/redo。
4. Transaction commit 后释放 row lock；MDL 按 transaction duration 释放。

### 15.3 CREATE TABLE

见 [data-dictionary-ddl-flow.mmd](diagrams/data-dictionary-ddl-flow.mmd)。关键边界是先 MDL 独占目标 name，再准备 InnoDB storage，最后一次性发布字典版本。

### 15.4 Crash Recovery

见 [atomic-ddl-recovery-flow.mmd](diagrams/atomic-ddl-recovery-flow.mmd)。关键边界是 redo 先恢复物理页，再由 DDL recovery 解释 DDL log。

## 16. 测试设计

- 字典对象测试：schema/table/column/index/constraint 构造、版本不可变、rename 不改变 object id。
- Repository 测试：按 name/id 查询、对象不存在、重复对象、字典事务 commit/rollback。
- Cache 测试：loading single flight、pin 后 evict 受阻、DDL publish 后旧版本 stale、新版本可见。
- DDL 测试：CREATE TABLE、DROP TABLE、CREATE INDEX、ALTER metadata-only、ALTER copy rebuild。
- MDL 测试：兼容矩阵、pending queue、upgrade、timeout、victim、release duration。
- 并发测试：DML 持有 shared MDL 时 DDL exclusive 等待；DDL pending 时新 DML 排队策略。
- 死锁测试：多表 rename/alter 的 MDL 等待成环，victim cleanup。
- 恢复测试：DDL prepare 后 crash、engine done 后 crash、dict commit 后 crash、cache publish 前 crash。
- SDI 测试：CREATE/ALTER 后 SDI version 更新，recovery 后 SDI mismatch 可重写。
- 集成测试：CREATE TABLE 后 B+Tree root 存在，DROP TABLE 后 Buffer Pool stale，redo recovery 后字典和 storage binding 一致。

## 17. 后续实现顺序

1. `sql.dd.domain`：ID、name、schema/table/column/index/tablespace 值对象。
2. `sql.dd.mdl`：MDL mode、compatibility matrix、request state、wait queue。
3. `sql.dd.cache`：不可变对象版本、pin、single flight loading。
4. `sql.dd.repo`：最小字典表 schema 和 repository 接口。
5. `sql.dd.tx`：字典事务 staging、commit、rollback。
6. `sql.dd.ddl`：CREATE TABLE atomic DDL template。
7. `sql.dd.engine`：InnoDB bridge 创建 tablespace、segment、root page。
8. DROP TABLE 和 Buffer Pool stale 协作。
9. CREATE INDEX 和 B+Tree 二级索引构建。
10. ALTER TABLE metadata-only 与 copy rebuild。
11. DDL log 与 recovery service。
12. SDI 写入和校验。
13. INFORMATION_SCHEMA adapter。
14. MDL deadlock detector 和诊断快照。
15. 集成测试和故障注入测试。

## 18. 十五轮自检记录

| 轮次 | 检查主题 | 检查结果 |
| --- | --- | --- |
| 1 | 是否只写设计文档 | 本文只新增设计说明和 Mermaid 图，没有生成 Java 实现代码 |
| 2 | 目标与非目标 | 已明确 DD/DDL/MDL 目标、SQL parser/binlog/online DDL 等非目标 |
| 3 | MySQL 8.0 贴合 | 已覆盖事务型 DD、`mysql.ibd`、object cache、atomic DDL、MDL、SDI、DDL log |
| 4 | 高内聚 | DD API、cache、repo、tx、DDL、MDL、SDI、recovery 子包职责独立 |
| 5 | 低耦合 | 已禁止 storage 模块反向读取 DD cache，并通过 InnoDB bridge 解耦 |
| 6 | 面向对象 | 已定义领域对象、值对象、聚合根、服务对象和参与者接口 |
| 7 | 设计模式 | 已列出 Facade、Repository、Unit of Work、Template Method、Command、State 等使用点 |
| 8 | 核心领域模型 | 已定义 schema/table/column/index/constraint/tablespace/storage binding/DDL runtime 对象 |
| 9 | 依赖方向 | 已给出 SQL 到 storage 的单向依赖链和禁止反向依赖 |
| 10 | 物理与逻辑区分 | 已区分逻辑元数据、物理绑定、物理空间、页内格式和 B+Tree |
| 11 | 关键数据流 | 已给出 SELECT、INSERT、CREATE TABLE、crash recovery 流程 |
| 12 | 图示 | 已提供架构图、类关系图、DDL 流程图、MDL 状态图和 Atomic DDL recovery 图 |
| 13 | 并发锁状态 | 已定义 MDL 类型、状态、持有者变化、等待前释放规则和死锁域 |
| 14 | 异常与恢复 | 已给出异常类型、DDL log recovery、orphan cleanup、SDI mismatch 处理 |
| 15 | 测试与顺序 | 已给出测试设计、后续实现顺序，并确认没有未完成标记或空白项 |

## 19. 2026-07-21 当前实现落点

本节只记录已从源码和测试核对的 v1 落点；前文仍是长期目标设计。

- `cn.zhangyis.db.dd.domain/repo/tx/cache/mdl/service/ddl/recovery/sdi` 已落地 schema/table/index 不可变定义、repository、版本cache、MDL、CREATE/DROP TABLE、Online ADD/DROP INDEX、instant metadata、multi-index INPLACE、online shadow ALTER与SDI v1。
- `mysql.dd.ctrl` 使用双 4 KiB CRC32C 槽保存 ID/version high-water；启动时以已提交 catalog 对象/space/version、最大 DDL marker 与 `dictionaryVersion-1` 保守下界反向校正 high-water，防止新槽损坏导致 ID 复用。
- `mysql.ibd` v1 实际是页对齐 append-only catalog sidecar，使用 frame CRC 和 batch SHA manifest；它尚不是前文目标中经 Buffer Pool/MTR/redo 管理的 InnoDB 字典 B+Tree。
- 公共 `DatabaseEngine` 在打开 catalog 前调用 `CatalogBootstrapAdmission`：非空 `mysql.ibd` 只走 existing 格式校验；missing/empty 状态只有在 DD control、single/ring redo、redo/transaction control、任意稳定 undo、三类 doublewrite 与受控表空间候选都不存在时才可初始化。任一证据或扫描失败均抛 `DictionaryCatalogAdmissionException`，且尚未创建 storage、运行 DDL recovery 或 orphan cleanup。
- `PersistentDdlLogRepository` 与普通DD repository共享`mysql.ibd`物理store，但分别只解释`DDL_LOG(7)`单记录批次与`CATALOG_COMMIT`批次；DDL log v4在v3 identity上增加execution protocol、source/intermediate/target schema digest、durable control/cancellation与retirement fence，并兼容解码v1-v3为`LEGACY_PHASE_ONLY`。identity、稳定code、checkpoint策略、phase/control转换、辅助路径/文件identity与UTF-8形状全部fail-closed。
- 字典 version 只要比已发布版本大即合法；先保留后 crash 会留下可解释的版本间隙，不得回退复用。
- `DataDictionaryService.openTable` 按 schema→table 获取 MDL，再持有 cache pin；`TableMetadataLease.close` 统一逆序释放。
- `storage.api.ddl.DdlUndoMarker(ddlOperationId,dictionaryVersion,affectedObjectId)` 是 DD 与未来 dictionary-row undo 共用的稳定数值关联对象；当前 sidecar DD 不把 marker 混入普通用户 row undo/history。
- CREATE 顺序为 `PREPARED -> physical CREATE/empty page3 durable -> ENGINE_DONE -> full SDI durable -> ACTIVE DD -> DICTIONARY_COMMITTED -> cache -> COMMITTED`。启动恢复对无 DD 的 PREPARED/ENGINE_DONE 只删除 marker identity 推导出的 exact path并写 ROLLED_BACK；匹配 ACTIVE 时补齐 terminal，错绑或缺文件阻止 OPEN。
- DROP 在 table X 下先等待 `TablePurgeBarrier`，再按 `PREPARED -> DROP_PENDING -> DICTIONARY_COMMITTED -> physical DROP -> ENGINE_DONE -> DROPPED -> COMMITTED` 推进。恢复对 ACTIVE+PREPARED 回滚，对 DROP_PENDING 再过 barrier 后前滚，对 DROPPED 补 terminal。
- CREATE INDEX v1 支持 `CREATE [UNIQUE] INDEX` 与单动作 `ALTER TABLE ... ADD [UNIQUE] INDEX`，两种语法归一为同一 command。生产组合根默认注入 Online runtime：initial/final X 只冻结 capture/cutover，base scan 在 SU 下允许并发 SELECT/INSERT/UPDATE/DELETE；DML 物理修改后产生 before/after candidate，普通 commit 与 XA PREPARE 在事务终态前 force 自身 row-log high-water。
- Online CREATE INDEX 持久顺序为 `manifest force -> PREPARED -> staged descriptor -> CAPTURING force -> base scan -> SEALED force -> current reconciliation -> source/target verify -> data/WAL force -> RECONCILED force -> ENGINE_DONE -> SDI -> ACTIVE DD exact add-one-index -> DICTIONARY_COMMITTED -> cache -> clear descriptor -> COMMITTED`。容量耗尽先用 terminal reserve force `ABORT_REQUIRED`，reserve格式下限同时覆盖最后一次force watermark；已成功的用户 DML 不随 DDL 回滚。
- CREATE INDEX recovery 以 committed DD、marker、manifest、row-log generation/force watermark 和 exact stage descriptor 交叉裁决：PREPARED+旧 DD 在开放流量前清旧 tree/log generation并从当前聚簇真相重建；ENGINE_DONE+旧 DD 只发布已验证 tree；新 DD 精确包含 index/binding 时只前滚。没有 auxiliary row-log path 的 legacy marker 保留原 blocking recovery。
- Online DROP INDEX v1支持`DROP INDEX index ON table`与单动作`ALTER TABLE table DROP INDEX index`，不支持`IF EXISTS`、多action或删除聚簇索引。prepare阶段持schema IX/table SU，final X排空旧admission/writer并捕获persistent history high-water与exact source metadata version；一次安装retirement fence后竞争`OPEN→FORWARD_ONLY`，target DD发布后降回SU等待history/pin共同安全，再原子回收segment/footer。
- DROP INDEX recovery交叉验证committed DD、source/target digest、control、retirement fence和exact descriptor：source+OPEN/CANCEL回滚descriptor，source+FORWARD_ONLY前滚发布target，target DD只允许继续等待retirement并物理回收。共享绝对timeout覆盖history与pin，超时保留forward证据而不重新加入索引。
- `OnlineDdlControlService`由`DatabaseEngine.onlineDdlControl()`暴露稳定Java/admin facade；`list/find`合并进程内active/history与durable marker，`requestCancel`先做privilege与prepare handoff，再用repository CAS持久化唯一方向，`awaitTerminal`只做有界等待。snapshot不持有MDL、page、文件或row-log资源。
- metadata-only ALTER使用稳定`ALTER_TABLE_INPLACE(11)`与`ONLINE_ALTER_INPLACE_V1`：table X内先写target SDI，随后CAS到FORWARD_ONLY并提交target DD/cache；取消或OPEN恢复从committed source重写SDI，FORWARD_ONLY恢复从exact target SDI解码并校验digest后前滚。
- `OnlineAlterStrategySelector`在任何持久副作用前按完整action集合冻结策略：metadata-only走instant；仅secondary集合走一个INPLACE manifest/descriptor generation；row-layout或mixed action走一个SHADOW manifest/tablespace。主键/任意类型/foreign/generated等未声明能力在副作用前拒绝，不在执行中静默降级。
- 通用INPLACE使用专属versioned descriptor page chain原子拥有多个ADD/DROP资源；通用SHADOW使用clustered identity journal、bounded copy、严格delete/ensure两遍reconcile、双向验证与ReadView/purge retirement barrier。marker、journal、manifest、SDI和descriptor按digest/owner/generation交叉验证。
- 通用ALTER恢复对source+OPEN/CANCEL回滚，对source+FORWARD_ONLY要求READY与RECONCILED后前滚，对target继续retirement/cleanup；任一path、digest、descriptor或fence第三态均保留证据并阻止OPEN。
- `TableStorageBinding.rowFormatVersion` 已与字典对象版本分离；CREATE INDEX 只推进 dictionary version，不改变已有聚簇记录编码。catalog 与 SDI 新格式显式持久化该值，旧格式缺失时仅按旧 table version 兼容推导。
- 任一 DD/marker append 向调用方报错都可能已经 durable，当前进程不猜测补偿；下一次启动从 committed batches 重建双方最新真相。无 DDL log 的旧 DROP_PENDING 与受控命名 orphan cleanup 仍兼容。
- DD discovery 从 ACTIVE/DROP_PENDING table 只返回稳定 storage API binding；公共 `cn.zhangyis.db.engine.DatabaseEngine` 再转换 recovery tablespace 配置，在 storage rollback/RESUME_PURGE 后运行 logged DDL recovery 与 legacy cleanup，全部完成后才发布 OPEN。
- SDI v1 使用 GENERAL 表空间固定 page3 与 page0 `SDI_ROOT=3`；`DictionarySdiCodec` 保存完整 ACTIVE table/column/index/storage binding 聚合，storage 只持 opaque payload + identity/version + CRC32C。page3 尾部保留独立 96-byte index DDL descriptor，footer v2 显式保存 BUILD/DROP action并兼容把 v1 解读为 BUILD；普通 SDI rewrite 不覆盖它。启动逐张比较 committed ACTIVE DD，root=0、空页、逻辑 CRC/内容错配会重写，未知 root、action/CRC/reserved bytes 或物理页损坏均 fail-closed。
- `UndoRecordCodec.peekIdentity`、`IndexMetadataResolver` 和 `engine.adapter.DictionaryIndexMetadataResolver` 已让 rollback/purge 按 tableId/indexId 解析目标 B+Tree，legacy 单索引构造仅供低层兼容。
- 对象级 force recovery 新增稳定 `TableState.RECOVERY_UNAVAILABLE(6)` 与
  `RECOVERY_DISCARDED(7)`；普通 lookup/cache、ACTIVE SDI reconcile 和用户表空间 discovery 均隐藏，
  recovery lookup 与 clean catalog/manifest 保留完整定义和 binding 作为长期诊断、排除与恢复证据。
- `DictionaryRecoveryIsolationPlanner` 在 storage discovery 前证明管理员 SpaceId 与 committed DD 的一对一归属，
  并在单个字典版本/事务内完成 ACTIVE→RECOVERY_UNAVAILABLE；重复 path、共享/未知 space、系统/undo、
  非稳定状态与 unresolved DDL identity/path 相交均不允许产生部分提交。
- recovery object DDL 使用稳定 operation code 8/9/10，严格按 physical-first phase 执行 raw DISCARD、DROP
  与 trusted replacement IMPORT；raw 路径逐级拒绝符号链接且要求无句柄/frame，恢复不读取不可用源的 page0。
- `RecoveryIdentityStore` 懒持久化实例 UUID、256-bit HMAC key 与 CRC；`RecoveryBackupService` 只对 ACTIVE
  表在 MDL X、history/pin drain、space X lease、flush/force 后生成 DISCARDED 副本，并最后原子发布 HMAC manifest。
  IMPORT 只接受固定 `recovery-incoming` pair，重新校验 HMAC、文件/定义 hash、table/space/file identity 和 page0。
- 可信 archive/incoming 独立保留，普通 DROP 不删除备份；恢复身份丢失/损坏只阻断 backup/import，不阻断普通启动。
  Java 管理 facade 已接，未新增 SQL 语法、任意主机路径、跨实例信任或自动修复。

明确保留的差异：

- SDI 仍是单页 v1，不支持多页/B+Tree 或在单表 payload 内保存 schema 默认属性。`mysql.ibd` 丢失后的
  catalog rebuild 已由独立 clean manifest + full-page scrub + 显式隔离/重建 API 闭环；普通启动仍只做
  admission fail-closed，绝不自动采用 SDI。binlog participant、主键/任意类型/foreign/generated ALTER
  未实现；对象级 force recovery 已有 file-per-table v1，但系统/undo/
  共享空间隔离、跨实例信任与自动 repair 不在该切片范围。
- CREATE INDEX v1 已实现持久 row-log 与 final cutover，但仍不支持复制 binlog、并行/外排 build、prefix key part、FULLTEXT/SPATIAL、异步 job/status SQL 或持久 scan continuation；它是教学型 online build，不承诺 MySQL 8.0 的完整 ALGORITHM/LOCK 兼容矩阵。
- marker schema digest、durable cancel/control CAS、Online DROP、instant metadata、multi-index INPLACE与online shadow ALTER已进入生产；可观察性当前仅为Java/admin facade，尚无SQL/网络协议或完整权限系统。
- 独立DDL log v4覆盖稳定operation 1..11并兼容v1-v3；CREATE SCHEMA尚无marker，因此control reconciliation仍用committed dictionary version提供保守下界。temporary undo也未接，在临时表owner/lifecycle与独立temporary tablespace完成前继续拒绝进入普通undo。
- table DROP barrier不持久化独立计数；Online DROP INDEX则持久化history high-water、source version和retired index resource，但恢复仍从page3/undo first-page重建真实history引用。当前仍是单rseg、单线程purge。
- orphan cleanup 只处理 `tables/` 下符合受控命名规则且未被 catalog 引用的文件，不扫描或删除任意路径；missing/empty catalog 遇到同一 glob 候选时会先被 admission guard 阻断，不能用空字典驱动删除。

## 20. 参考链接

- MySQL 8.0 Reference Manual - MySQL Data Dictionary: https://dev.mysql.com/doc/refman/8.0/en/data-dictionary.html
- MySQL 8.0 Reference Manual - Transactional Storage of Dictionary Data: https://dev.mysql.com/doc/refman/8.0/en/data-dictionary-transactional-storage.html
- MySQL 8.0 Reference Manual - Atomic Data Definition Statement Support: https://dev.mysql.com/doc/refman/8.0/en/atomic-ddl.html
- MySQL 8.0 Reference Manual - Performance Schema `metadata_locks` Table: https://dev.mysql.com/doc/refman/8.0/en/performance-schema-metadata-locks-table.html
- MySQL 8.0 Reference Manual - Data Dictionary Usage Differences: https://dev.mysql.com/doc/refman/8.0/en/data-dictionary-usage-differences.html
- MySQL 8.0 Reference Manual - InnoDB Persistent Statistics: https://dev.mysql.com/doc/mysql/8.0/en/innodb-persistent-stats.html
