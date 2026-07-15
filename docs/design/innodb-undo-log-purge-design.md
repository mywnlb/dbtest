# MiniMySQL InnoDB 风格 Undo Log 与 Purge 模块设计

版本：2026-06-06  
实现语言：Java  
参考基线：MySQL 8.0.46 InnoDB 官方手册与源码文档  
关联设计：[innodb-transaction-mvcc-design.md](innodb-transaction-mvcc-design.md)，[innodb-crash-recovery-design.md](innodb-crash-recovery-design.md)，[innodb-redo-log-design.md](innodb-redo-log-design.md)，[innodb-buffer-pool-design.md](innodb-buffer-pool-design.md)，[innodb-btree-design.md](innodb-btree-design.md)，[innodb-record-design.md](innodb-record-design.md)，[innodb-disk-manager-design.md](innodb-disk-manager-design.md)

## 1. 目标与边界

本设计面向 MiniMySQL 存储引擎中的 `storage.trx.undo` 与 `storage.trx.purge` 模块。事务与 MVCC 总设计已经定义事务生命周期、ReadView、行锁和可见性入口；本文把 Undo Log、Rollback Segment、History List、Purge、Rollback、Savepoint、Crash Recovery 协作和 Undo Tablespace 回收边界拆成可独立实现、测试和演进的 Java 面向对象设计。

设计目标：

- 高内聚：undo tablespace、rollback segment、undo segment、undo page、undo record、history list、rollback executor、purge coordinator、purge worker、undo truncation 和 recovery scanner 分别收敛在明确对象或子包内。
- 低耦合：Record、B+Tree、Transaction、Recovery、Redo、Buffer Pool 只依赖 `UndoLogManager`、`RollbackService`、`PurgeCoordinator`、`UndoRecoveryService` 等稳定接口，不直接读取 undo page 链表或 rollback segment slot。
- InnoDB 风格：对齐 MySQL 8.0 的 insert undo、update undo、rollback segment、undo tablespace、history list、ReadView 与 purge view、旧版本构造、delete-mark 清理、未提交事务回滚和恢复后 purge resume。
- Java 可落地：使用领域对象、值对象、仓储、命令对象、状态机、策略、责任链、后台 worker、观察者和 RAII guard 表达生命周期与并发边界。
- 可恢复：undo page、undo log header、rollback segment header、history list 指针和事务状态修改必须由 MTR 与 redo 保护；crash recovery 后能重建 history list、回滚 recovered active transaction 并恢复 purge。
- 可测试：每个逻辑对象可用内存仓储测试，物理页修改可用 PageCursor/MTR 故障注入测试，后台 purge 可用可控时钟和批次边界测试。

非目标：

- 不实现 SQL Parser、优化器、复制、binlog 和 XA 上层协调。
- 不实现 MySQL 二进制 undo page 完全兼容格式，只设计对齐概念和恢复语义的 MiniMySQL 格式。
- 不让 Undo/Purge 直接管理 B+Tree 页分裂、record 编码、BufferFrame 生命周期或裸文件 IO。
- 不让 Purge 决定事务隔离级别或 ReadView 创建规则。
- 不生成 Java 源码。本文只定义设计、关系、数据结构、流程和测试方案。

术语边界：

- `Undo Log` 是事务回滚和 MVCC 旧版本构造所需的逻辑撤销日志；undo page 本身的物理修改仍需要 redo 保护。
- `Rollback Segment` 是 undo log 的持久化容器，负责 slot 分配、undo segment 链表和 history list 入口。
- `Purge` 是后台清理：当所有 ReadView 都不再需要某些 committed update undo 时，清理 delete-mark 记录和回收 undo 空间。
- `Rollback` 是事务语义撤销：按 undo 链反向应用命令，把未提交修改撤回。
- `MTR` 是物理短事务，保护一次页修改的 latch、redo 和 pageLSN，不替代数据库事务。

## 2. MySQL 8.0 参考依据

本设计参考 MySQL 8.0 的以下行为：

- InnoDB 为每个读写事务维护 undo log，undo log 由 undo log record 组成；undo log record 用于回滚事务，也用于 consistent read 构造旧版本。
- Undo log 位于 rollback segment 内，rollback segment 位于 undo tablespace 或系统表空间中。MySQL 8.0 默认使用独立 undo tablespace。
- InnoDB 区分 insert undo log 和 update undo log。insert undo 只需要支持事务回滚，事务提交后不再被一致性读使用；update undo 还服务 MVCC，需要等 purge 边界推进后才能释放。
- 聚簇索引记录包含隐藏列 `DB_TRX_ID` 和 `DB_ROLL_PTR`；`DB_ROLL_PTR` 指向 undo log record。旧版本通过当前记录和 undo 链在内存中构造。
- 一致性读使用 ReadView。若当前记录版本对 ReadView 不可见，沿 `DB_ROLL_PTR` 读取 update undo，构造更早版本，直到找到可见版本或确认记录不存在。
- InnoDB 的删除通常先做 delete mark；当没有 ReadView 需要被删除记录的旧版本时，purge 线程才物理删除记录和关联索引项。
- Purge 使用最老 ReadView 与活跃事务边界判断 history list 中哪些 update undo 可被清理；长事务会阻止 history list 推进。
- Crash recovery 先应用 redo，使 undo page 和 rollback segment header 达到物理一致，再通过 undo 回滚未提交事务，并在恢复完成后恢复 purge。
- MySQL 8.0 支持 undo tablespace truncate；undo tablespace 必须在不再被活跃事务和 purge 需要时才能截断。

简化点：

| MySQL 8.0 能力 | MiniMySQL 第一阶段设计 |
| --- | --- |
| 多个 undo tablespace 在线创建、删除和加密 | 先支持启动配置的多个 undo tablespace，保留在线管理接口和生命周期锁 |
| rollback segment 数量和 slot 分布完全复刻 | 先按配置固定 rollback segment 数量，使用一致哈希或轮询分配事务 slot |
| 复杂 undo record 二进制格式 | 采用可版本化 Mini 格式，字段语义对齐 `DB_TRX_ID`、`DB_ROLL_PTR`、old column image |
| 并行 purge 细粒度调度 | 先按 table/index/page 分片，保证同一聚簇记录只由一个 worker 处理 |
| DDL undo 和 atomic DDL 全量语义 | 本文只定义 DDL 与 row undo 的边界，完整 DDL log 由数据字典设计承接 |
| 临时表 undo 与普通 undo 完整隔离 | 先定义 `TEMPORARY` undo segment kind，第一阶段可在临时表模块接入时启用 |

## 3. 总体架构

架构图见 [undo-purge-architecture.mmd](diagrams/undo-purge-architecture.mmd)。

模块分为八组：

1. `storage.trx.undo.api`：Undo 门面，包括 `UndoLogManager`、`RollbackService`、`SavepointService`。
2. `storage.trx.undo.core`：`UndoContext`、undo 状态机、rollback segment directory、slot 分配。
3. `storage.trx.undo.storage`：undo tablespace repository、rollback segment、undo segment、undo page、undo record。
4. `storage.trx.undo.history`：history list、history cursor、purge boundary、history metrics。
5. `storage.trx.undo.codec`：`RollPointerCodec`、`UndoRecordCodec`、版本化 payload 编解码。
6. `storage.trx.purge`：purge coordinator、purge worker、purge view provider、undo truncation。
7. `storage.trx.undo.recovery`：恢复期 rollback segment 扫描、history list 重建、recovered active rollback。
8. `storage.trx.undo.metric`：undo slot、history length、purge lag、truncate、rollback 成本指标。

核心原则：

- Undo/Purge 不解释 SQL，也不选择执行计划。
- Undo/Purge 不解析 record 物理布局，只保存和交还 `RecordImage`、`ChangedColumns`、`ClusterKey`、隐藏列快照等由 Record 模块定义的值对象。
- Undo/Purge 不分配普通表页、索引页和 extent；页分配通过 Disk Manager/FSP，页访问通过 Buffer Pool 和 MTR。
- Undo record 写入必须发生在被保护的数据页修改之前或同一 MTR 内先写 undo 再改数据页；数据页刷盘前必须满足 redo write-ahead。
- Purge 是后台清理，不改变用户事务已提交结果；purge 失败只影响空间回收和 history list 长度，不改变可见性语义。

## 4. 包与职责

| 包 | 职责 | 主要依赖 | 设计模式 |
| --- | --- | --- | --- |
| `storage.trx.undo.api` | 对事务、MVCC、B+Tree 暴露 undo、rollback、savepoint 门面 | core, storage, history | Facade |
| `storage.trx.undo.core` | undo 上下文、undoNo 分配、状态机、rollback segment slot | transaction domain | State, Repository |
| `storage.trx.undo.storage` | undo tablespace、rollback segment、undo segment/page/record 持久化 | buf, fsp, mtr | Repository, Aggregate |
| `storage.trx.undo.codec` | roll pointer、undo log header、undo record payload 编解码 | record domain | Adapter, Codec Strategy |
| `storage.trx.undo.history` | committed update undo 的 history list、purge cursor、history 边界 | core, storage | Cursor, Snapshot |
| `storage.trx.undo.rollback` | full rollback、statement rollback、savepoint rollback | api, btree, lock | Command, Template Method |
| `storage.trx.purge` | purge view、任务调度、后台 worker、delete-mark 清理、undo 回收 | readview, btree, record | Background Worker, Strategy |
| `storage.trx.undo.recovery` | 扫描 rollback segment、恢复 slot、重建 history list、恢复后 rollback/purge | redo, recovery | Recovery Handler |
| `storage.trx.undo.metric` | undo 空间、history length、purge lag、rollback 行数、截断指标 | 无 | Observer |

推荐依赖方向：

`transaction.core -> undo.api -> undo.core + undo.storage + undo.history`  
`record/btree -> undo.api + purge task callback`  
`mvcc -> undo.api -> undo.codec/storage`  
`purge -> readview + undo.history + undo.api + btree/record`  
`undo.storage -> mtr + buf + fsp + redo`  
`recovery -> undo.recovery -> undo.storage + rollback + purge`

禁止方向：

- `storage.trx.undo` 不能 import SQL parser、optimizer 或 executor。
- `storage.trx.undo.storage` 不能调用 `LockManager` 等待行锁。
- `storage.trx.purge` 不能打开用户事务，也不能加入用户事务 Wait-For Graph。
- `storage.trx.purge` 不能清理仍被最老 ReadView 或 recovered transaction 需要的 update undo。
- `storage.trx.undo.codec` 不能读取 BufferFrame 或 PageStore，只处理字节视图和值对象映射。
- `BufferPool`、`Disk Manager`、`Redo` 不能反向依赖 undo 的业务对象。
- `Record` 和 `B+Tree` 不能直接读写 rollback segment header 或 history list。

## 5. 核心领域模型

类关系图见 [undo-purge-class-relation.mmd](diagrams/undo-purge-class-relation.mmd)。

### 5.1 值对象与枚举

| 对象 | 含义 | 约束 |
| --- | --- | --- |
| `UndoTablespaceId` | undo tablespace 标识 | 不与普通 tablespace 混用 |
| `RollbackSegmentId` | rollback segment 标识 | 在一个 undo tablespace 内唯一 |
| `UndoSegmentId` | insert/update undo segment 标识 | 归属于一个 rollback segment |
| `UndoPageId` | undo page 物理位置 | 包含 space id 和 page no |
| `UndoLogId` | 一个事务的一条 insert 或 update undo log | 绑定 transaction id 和 slot |
| `UndoSlotId` | rollback segment 内事务 slot | 同一时间只归属一个事务 |
| `UndoNo` | 事务内 undo record 序号 | 单调递增，用于 savepoint 边界 |
| `RollPointer` | 指向 undo record 的位置 | 包含 tablespace、rseg、page no、offset、record type |
| `HistoryNodeId` | history list 节点标识 | 指向已提交 update undo log header |
| `PurgeBoundary` | purge 可清理边界 | 基于最老 ReadView、活跃事务和恢复状态 |
| `PurgeBatchId` | purge 批次标识 | 用于幂等诊断和指标 |
| `SavepointId` | 保存点标识 | 绑定事务和 undoNo |

枚举：

| 枚举 | 取值 |
| --- | --- |
| `UndoLogKind` | `INSERT`, `UPDATE`, `TEMPORARY` |
| `UndoRecordType` | `INSERT_ROW`, `UPDATE_ROW`, `DELETE_MARK`, `UPDATE_EXTERN_REF`, `DDL_MARKER` |
| `UndoLogState` | `ACTIVE`, `PREPARED`, `COMMITTED_IN_HISTORY`, `ROLLING_BACK`, `ROLLED_BACK`, `PURGE_READY`, `PURGED`, `REUSABLE` |
| `UndoSegmentState` | `FREE`, `ACTIVE`, `CACHED`, `TO_PURGE`, `PURGING`, `REUSABLE`, `TRUNCATING` |
| `UndoTablespaceState` | `ACTIVE`, `INACTIVE`, `TRUNCATE_CANDIDATE`, `TRUNCATING`, `RESTORING`, `DROPPED` |
| `PurgeTaskState` | `NEW`, `CLAIMED`, `LOCATING_RECORD`, `APPLYING`, `RETRYABLE_FAILED`, `DONE`, `SKIPPED` |
| `RollbackMode` | `FULL_TRANSACTION`, `TO_SAVEPOINT`, `STATEMENT`, `RECOVERY` |

### 5.2 UndoLogManager

`UndoLogManager` 是对外门面：

- 为读写事务分配 `UndoContext`、rollback segment slot、insert/update undo log。
- 在 INSERT/UPDATE/DELETE 修改前先形成不可变 write plan，再在业务 MTR 中追加 record 并返回 `RollPointer`。
- 根据 `RollPointer` 读取 undo record，供 MVCC 构造旧版本。
- 在 commit 时把 update undo log header 挂入 history list，把 insert undo 标记为可释放。
- 在 rollback/savepoint rollback 时按 undo 链反向读取记录，并调用 `RollbackService` 执行撤销。
- 向 recovery 暴露 rollback segment 扫描和 history list 重建能力。

约束：

- `UndoLogManager` 不直接修改用户记录页；它只产出 undo record、roll pointer 和 rollback command。
- `planUpdate/appendPlanned` 必须在当前读已经获得必要行锁后调用。
- undo plan/append 不能在持有 `HistoryListMutex` 时访问 B+Tree 或 Record 模块。
- 已进入 `COMMITTING`、`ROLLING_BACK` 或 `PREPARED` 的事务不能再创建新业务 undo record。

### 5.3 UndoContext

`UndoContext` 是事务聚合内部的 undo 子状态：

- `rollbackSegmentId`
- `lastUndoNo`：事务全局物理 append 高水位。
- 可选 `insertBinding(kind, slotId, firstPageId, logicalHead)`。
- 可选 `updateBinding(kind, slotId, firstPageId, logicalHead)`。
- `savepointStack`
- `modifiedTables`
- `hasExternColumnUndo`

职责：

- 维护事务全局唯一、单调的 undoNo；partial rollback 不回退该高水位。
- 分别维护 insert/update undo log 的持久位置和局部回滚链头。
- 支持语句级 rollback 和 savepoint rollback。
- 在 commit/rollback/recovery 时提供 undo 链入口。

约束：

- `UndoContext` 只能被所属 `Transaction` 和 `UndoLogManager` 修改。
- `UndoRecord.prevRollPointer` 只指向同 kind log 的局部前驱；记录版本链由聚簇记录 `DB_ROLL_PTR` 与
  update undo 的 `oldHiddenColumns.dbRollPtr` 串联，两条链不能混用。
- savepoint 同时记录 INSERT/UPDATE 两个精确 logical heads，不复制 undo record。

### 5.4 RollbackSegment 与 UndoSegment

`RollbackSegment` 是持久聚合根：

- 维护 rseg header page、slot array、active undo log、history list base node。
- 为事务分配 insert/update undo segment。
- 在 commit 时接收 update undo log header 并追加到 history list。
- 在 rollback 或 purge 后释放 slot 和可复用 segment。

`UndoSegment` 表示一条 undo log 的页链：

- insert undo segment 保存未提交插入的撤销信息。
- update undo segment 保存更新前镜像、delete-mark 前镜像和版本链指针。
- temporary undo segment 仅服务临时对象，不进入普通 history list。

约束：

- rollback segment 不理解 record payload，只管理 slot、页链和 log header。
- undo segment 只分配 undo page，不分配普通表页。
- 同一事务的 insert undo 与 update undo 可以在同一 rollback segment 的不同 slot 或不同 segment 中保存；实现可先采用同一 slot 下两条 log。

### 5.5 UndoRecord

`UndoRecord` 是命令对象：

- `type`
- `transactionId`
- `undoNo`
- `prevRollPointer`
- `tableId`
- `indexId`
- `clusterKey`
- `oldDbTrxId`
- `oldDbRollPtr`
- `oldDeleteMark`
- `changedColumnBeforeImages`
- `externColumnRefsBeforeChange`
- `rowOperationHint`

职责：

- 为 MVCC 构造上一个逻辑版本。
- 为 rollback 执行反向命令。
- 为 purge 构造 delete-mark 清理任务和 undo 回收任务。

约束：

- undo record 不直接访问 Buffer Pool；读取由 repository 完成，应用由 rollback/purge executor 完成。
- update undo 必须包含恢复旧隐藏列所需的信息。
- delete-mark undo 必须能区分“撤销删除标记”和“purge 物理删除后只保留诊断”的边界。

### 5.6 HistoryList

`HistoryList` 保存已提交但仍可能被 ReadView 需要的 update undo log：

- 以 rollback segment 为持久链表根。
- 节点是 committed update undo log header，不是单条 undo record。
- page3 保存 head/tail/length/lastTransactionNo，UPDATE undo first page 保存 prev/next。
- 当前单 rseg 实现以 head 作为 purge 下一个候选，不另持久化 cursor。
- `PurgeBoundary` 决定 cursor 能推进到哪里。

约束：

- insert undo 不进入 history list。
- prepared transaction 的 undo 不进入可 purge 区间。
- 链顺序是 append 的物理顺序，不按 TransactionNo 排序；purge 遇到不满足边界的 head 必须停止。
- 内存 `HistoryList` 只是持久链投影，append/unlink 必须先提交 page3 与节点链接，再发布内存队列。
- history list 长度是 purge lag 的主要指标，但不是唯一指标；还需要记录最老 ReadView 年龄和 undo tablespace 可回收页数。

### 5.7 PurgeCoordinator 与 PurgeWorker

`PurgeCoordinator`：

- 周期性或由 commit 唤醒。
- 从 `ReadViewManager`、`TransactionSystem` 和 recovery 状态计算 `PurgeBoundary`。
- 从 history list 读取候选 update undo log。
- 构造 `PurgeTask` 并按 table/index/page 分片。
- 收集 worker 结果，推进 history cursor，触发 undo truncation。

`PurgeWorker`：

- 不打开用户事务。
- 不加入事务死锁检测。
- 通过 B+Tree/Record 稳定接口重新定位 delete-mark 记录。
- 用短 MTR 清理二级索引、聚簇记录和 undo page。
- 遇到 latch 或 page busy 时退避重试，不长时间持有 page latch。

### 5.8 RollbackService 与 Savepoint

`RollbackService` 负责事务语义撤销：

- full rollback：反复选择 INSERT/UPDATE 两个非空局部头中 undoNo 较大者，直到双 EMPTY。
- statement/savepoint rollback：同样归并两个局部链，分别停止在保存的双 head 边界。
- recovery rollback：对 crash recovery 扫描出的 active transaction 执行幂等撤销。

`Savepoint` 是值对象：

- `savepointId`
- `statementId`
- `insertHead(undoNo, rollPointer)`
- `updateHead(undoNo, rollPointer)`
- `createdAt`

约束：

- rollback 到保存点不会释放保存点之前的锁；是否释放保存点之后获取的行锁由 `LockManager` 的 savepoint lock scope 决定。
- rollback 到保存点后事务保持 `ACTIVE`，新修改继续复用同一 `UndoContext`。
- recovery rollback 不依赖 SQL 层，也不重新执行用户语句。

## 6. 关键数据结构、页格式与逻辑/物理区分

版本链示意图见 [undo-purge-record-chain.mmd](diagrams/undo-purge-record-chain.mmd)。

### 6.1 逻辑对象与物理对象边界

| 层面 | 对象 | 所属模块 | 本模块职责 |
| --- | --- | --- | --- |
| 逻辑事务 | `Transaction`, `ReadView`, `TransactionNo` | `storage.trx.core/readview` | 读取边界和状态，不直接维护活跃事务表 |
| 逻辑 undo | `UndoContext`, `UndoRecord`, `Savepoint` | `storage.trx.undo` | 定义撤销语义和版本构造语义 |
| 逻辑 purge | `PurgeBoundary`, `PurgeTask`, `HistoryCursor` | `storage.trx.purge/history` | 判断可清理性和调度任务 |
| 物理 undo | undo tablespace、rseg header、undo page | `storage.trx.undo.storage` + Disk/Buffer | 通过 MTR/redo 持久化 |
| 物理 record | 聚簇/二级索引页、record header | Record/B+Tree/Buffer | 由 rollback/purge 通过稳定接口请求修改 |
| 物理日志 | redo record、pageLSN、checkpoint | Redo/MTR/Flush | undo 修改和 purge 修改都写 redo |

### 6.2 Undo Tablespace

MiniMySQL 的 undo tablespace 是专用表空间：

| 区域 | 内容 | 说明 |
| --- | --- | --- |
| file header | space id、space type、format version、checksum | 由 Disk Manager 统一维护 |
| undo tablespace header | tablespace state、rseg directory page、truncate epoch | 由 undo repository 维护 |
| rollback segment directory | rollback segment id 到 header page 的映射 | 启动恢复时扫描 |
| rollback segment header pages | slot array、history list base、free/cached segment list | redo 保护 |
| undo segment pages | insert/update undo log page 链 | 由 MTR 修改 |
| free extent/page metadata | 可复用页和可截断边界 | 由 FSP 管理 |

状态迁移：

`ACTIVE -> INACTIVE -> TRUNCATE_CANDIDATE -> TRUNCATING -> ACTIVE`  
`ACTIVE -> RESTORING -> ACTIVE`  
`INACTIVE -> DROPPED`

约束：

- `ACTIVE` tablespace 可分配新 undo。
- `INACTIVE` tablespace 不再分配新 undo，但已有事务、purge 和 recovery 仍可访问。
- `TRUNCATING` 期间必须持有 lifecycle X 锁，并阻止新 undo 分配。
- truncate 完成后必须写入新的 truncate epoch，恢复时用 epoch 判断是否需要继续恢复。

### 6.3 Rollback Segment Header

rollback segment header 包含：

| 字段 | 作用 |
| --- | --- |
| `rollbackSegmentId` | rseg 标识 |
| `tablespaceId` | 所属 undo tablespace |
| `formatVersion` | undo header 格式版本 |
| `slotArray` | 活跃事务 undo log slot |
| `historyListHead` / `historyListTail` | committed update undo log 链表 |
| `cachedInsertUndoSegments` | 可复用 insert undo segment |
| `cachedUpdateUndoSegments` | 可复用 update undo segment |
| `freeUndoSegments` | 空闲 segment |
| `lastTransactionNo` | 最近挂入 history 的提交序号 |
| `checksum` | 页校验 |

rollback segment header 只维护元数据，不保存 record old image。

当前单 rseg 教学实现使用 page3 v3：head/tail/length/lastTransactionNo 位于 slot array 之前，
active slot、INSERT cache、UPDATE cache 紧随其后。v1/v2 不含完整 history base，直接 fail-closed；
free undo segment list 与多 rseg directory 仍是后续扩展。

### 6.4 Undo Page 与 Undo Log Header

undo page 逻辑格式：

| 区域 | 字段 | 作用 |
| --- | --- | --- |
| page header | `pageType=UNDO`, `pageLsn`, `pageNo`, `prevPage`, `nextPage` | 页链和恢复幂等 |
| undo page header | `formatVersion=3`, `segmentId`, `undoLogKind`, `freeOffset`, `recordCount`, `lastUndoNo` | segment 内分配；first/chain page 都自描述 kind |
| undo log header | `transactionId`, `undoLogKind`, `state`, `commitNo`, `logicalHead`, `historyPrev`, `historyNext` | 一个事务的一条 undo log；history link 仅 first page 有效 |
| undo record area | 变长 undo record | 按 undoNo 串联 |
| free area | 追加空间 | 不跨页写半条 record |
| trailer | checksum、end marker | 恢复扫描校验 |

约束：

- 单条 undo record 不能跨 page；过大的 old column image 使用 external undo payload 页。
- external payload 不复用业务 LOB ownership：普通 UNDO record 槽只保存版本化根描述符，payload 页使用独立页类型，
  与 root 同属一个 undo segment 但不加入主 UNDO 页链；segment drop 统一回收两类页。
- 根描述符和每个 payload 页都携带事务/undoNo/segment identity、总长、页数与 CRC 证据；读取必须先完整校验链，
  再交给 `UndoRecordCodec` 解码，不能把部分字节暴露给 MVCC、rollback 或 purge。
- 写入前必须冻结 inline/external 决策和精确页数并完成空间/redo admission；超过配置上限在任何页修改前失败，
  进入物理发布阶段后的异常按不可安全补偿处理。
- 普通 UNDO 页格式固定为 v3，record area 从 120 后移到 136；first/chain page 都写 `undoLogKind`，
  first page 额外保存 history prev/next。v1/v2/未知版本不做隐式迁移并 fail-closed。
- 同一 kind 的 page 内 record 按事务全局 `undoNo` 递增追加；INSERT/UPDATE 两条局部链允许出现由另一 kind 消耗的序号间隙。
- `UndoRecord.prevRollPointer` 串起同 kind undo log 的**事务回滚局部链**；聚簇记录旧隐藏列中的
  `DB_ROLL_PTR` 串起**记录版本链**。full/recovery rollback 按两个局部 head 的 `undoNo` 归并逆序消费，
  MVCC 则只沿旧隐藏列的 `DB_ROLL_PTR` 构造同一行的旧版本，二者不得混用。
- undo page 修改必须写 redo；redo 类型由 Redo 模块保存字节级变更，不理解 MVCC。

### 6.5 UndoRecord Payload

| 类型 | 必需字段 | 用途 |
| --- | --- | --- |
| `INSERT_ROW` | table id、cluster key、inserted hidden columns、prev roll pointer | rollback 删除未提交插入；提交后可释放 |
| `UPDATE_ROW` | table id、cluster key、old hidden columns、changed column before image、prev roll pointer | rollback 恢复旧值；MVCC 构造旧版本 |
| `DELETE_MARK` | table id、cluster key、old delete flag、old hidden columns、prev roll pointer | rollback 取消删除标记；purge 物理清理 |
| `UPDATE_EXTERN_REF` | external column id、old external ref、new external ref | rollback 恢复外部列引用；purge 回收旧 external storage |
| `DDL_MARKER` | ddl operation id、dictionary version、affected object id | 只标识边界，完整 DDL 撤销由 DDL log 处理 |

不兼容点：

- MiniMySQL 不复刻 InnoDB 紧凑二进制字段编码。
- MiniMySQL 第一阶段仅要求 clustered record 的 old image 足以恢复逻辑版本；二级索引物理删除由 B+Tree 根据 table metadata 重建 key。
- 外部列 undo 先定义引用恢复语义，具体大字段页回收可随 Record/LOB 模块扩展。

## 7. 核心策略和算法

### 7.1 Undo 分配策略

读写事务对某种 DML 第一次写入时惰性分配对应 kind 的 undo：

1. `TransactionManager` 确保事务有 `TransactionId`。
2. `UndoLogManager` 从 `UndoTablespaceRepository` 选择 `ACTIVE` tablespace。
3. `RollbackSegmentDirectory` 按事务 ID 哈希或轮询选择 rollback segment。
4. 目标 kind 尚无 binding 时，`RollbackSegmentSlotManager` 分配一个独立 slot。
5. insert 创建 INSERT log；update/delete 创建 UPDATE log；mixed transaction 最多占两个 slot。
6. `UndoContext` attach binding；每次 append 只推进目标局部 head，并推进事务全局 last undo no。

选择策略：

| 策略 | 适用阶段 | 说明 |
| --- | --- | --- |
| `RoundRobinRollbackSegmentStrategy` | 第一阶段默认 | 简单均衡 slot |
| `LeastHistoryRollbackSegmentStrategy` | purge lag 优化 | 优先 history list 较短的 rseg |
| `TablespaceAffinityStrategy` | 多 undo tablespace | 同一事务固定到一个 tablespace，减少跨文件恢复 |

### 7.2 INSERT Undo

INSERT 当前读流程：

1. B+Tree 定位插入位置并获取必要 insert intention/gap/record lock。
2. 进入 MTR，pin 聚簇页和必要二级索引页。
3. `UndoLogManager.planInsert` 在 MTR admission 前冻结计划，`appendPlanned` 追加 `INSERT_ROW`。
4. Record 模块写入新记录，设置 `DB_TRX_ID=当前事务`，`DB_ROLL_PTR=insertUndoRollPointer`。
5. MTR commit 写入 undo page redo、index page redo，并发布 dirty page。
6. 事务提交时 insert undo 标记为 `REUSABLE`，不进入 history list。
7. 事务回滚时按 `INSERT_ROW` 删除未提交聚簇记录和二级索引项。

### 7.3 UPDATE 与 DELETE_MARK Undo

UPDATE/DELETE 当前读流程：

1. B+Tree 或执行器定位候选记录。
2. `LockManager` 获取 X、gap X 或 next-key X 锁；如果需要等待，必须释放 page latch、undo latch 和 MTR。
3. 等待返回后重新定位记录，重新判断谓词和可见性。
4. `UndoLogManager.planUpdate/planDelete` 冻结旧 hidden columns 和 before image。
5. `appendPlanned` 追加 `UPDATE_ROW` 或 `DELETE_MARK`；`prevRollPointer` 指向 UPDATE log 的局部前驱，
   记录版本前驱单独保存在 `oldHiddenColumns.dbRollPtr`。
6. Record 模块修改 payload/delete-mark，并把 `DB_TRX_ID` 和 `DB_ROLL_PTR` 改成当前事务与新 undo record。
7. MTR commit 写 redo。

约束：

- update undo 必须在数据页修改前持久化到同一 MTR 的 redo batch 中。
- UPDATE 不保存整行时，必须保存足以重建旧版本的 changed columns 和旧隐藏列。
- DELETE 先写 delete-mark undo，再把记录标记删除；物理删除只能由 purge 完成。

### 7.4 Commit 与 History List

提交流程：

1. 事务状态进入 `COMMITTING`，禁止创建新 undo。
2. 分配 `TransactionNo`。
3. UPDATE 事务先取得 history append transition lease，冻结当前 head/tail/count；锁不跨 IO 持有。
4. 当前实现对单 fragment INSERT segment 尝试移入固定容量 insert undo cache；容量满、忙或多页段则 drop。
5. 同一 MTR 将 update undo header 置 COMMITTED、链接旧 tail/new node、更新 page3 head/tail/length/lastTransactionNo。
6. mixed transaction 在该 MTR 中同时 cache/drop INSERT、完成 UPDATE link，并只写一次事务 terminal redo。
7. 从 active transaction table 移除事务。
8. 关闭 ReadView，释放锁。
9. 唤醒 `PurgeCoordinator`。

MTR commit 后才发布 slot/cache/history 内存投影。提交时只挂 update undo log header，不逐条扫描 undo record；
purge 后续按持久物理链顺序处理。

### 7.5 MVCC 旧版本遍历

版本链规则：

- 当前聚簇记录保存最新版本和 `DB_ROLL_PTR`。
- 如果当前版本对 ReadView 可见，直接返回。
- 如果不可见，按 `DB_ROLL_PTR` 读取 update undo record，应用 before image 构造上一个版本。
- 构造出的旧版本只存在内存中，不能被行锁锁住。
- 继续用 undo record 的 `prevRollPointer` 查找更早版本。
- 如果遇到不可见 insert undo，说明该 ReadView 中记录尚不存在。

边界：

- `READ_UNCOMMITTED` 可读取当前版本，但仍不能读损坏或半写 undo。
- `READ_COMMITTED` 每条一致性读使用新的 ReadView，因此 purge 边界可能更快推进。
- `REPEATABLE_READ` 同一事务复用第一次一致性读 ReadView，长事务会延迟 purge。

### 7.6 Rollback 与 Savepoint

rollback 流程图见 [undo-purge-rollback-flow.mmd](diagrams/undo-purge-rollback-flow.mmd)。

完整 rollback：

1. 事务状态 `ACTIVE -> ROLLING_BACK`。
2. 从 INSERT/UPDATE 两个局部头中选择较大 undoNo 对应的最新 undo record。
3. 对 `INSERT_ROW` 删除未提交插入。
4. 对 `UPDATE_ROW` 恢复 changed columns、`DB_TRX_ID`、`DB_ROLL_PTR` 和 delete flag。
5. 对 `DELETE_MARK` 取消删除标记。
6. 每条撤销使用独立 MTR，以便大事务 rollback 可分批推进并可恢复。
7. 只沿所属 kind 的 `prevRollPointer` 前进，直到两个局部头均为空。
8. 一个 finalization MTR 回收全部 segments/slots，再释放事务锁和 ReadView。
9. 状态 `ROLLING_BACK -> ROLLED_BACK`。

savepoint rollback：

- 分别撤销晚于 `savepoint.insertHead/updateHead` 的 record，并按全局 undoNo 归并执行。
- savepoint 之前的 undo 链保留，用于后续 full rollback 或 commit 后 history。
- savepoint rollback 后事务保持 `ACTIVE`。
- 已经撤销的 undo record 在逻辑上标记为 rolled back，避免 crash recovery 重复应用。

### 7.7 Purge

purge 流程图见 [undo-purge-purge-flow.mmd](diagrams/undo-purge-purge-flow.mmd)。

`PurgeCoordinator` 批处理：

1. 读取最老 ReadView 和活跃读写事务低水位。
2. 计算 `PurgeBoundary`。
3. 从 history list 读取候选 committed update undo log。
4. 如果候选的 transaction no 不小于 purge 边界，停止本批次。
5. 解析 undo record 链，构造 purge task。
6. 按 table/index/page 分片派发给 worker。
7. worker 重新定位 delete-mark 聚簇记录。
8. 再次校验记录 `DB_TRX_ID`、delete-mark 和 purge 边界。
9. 清理二级索引项，再物理移除聚簇记录或释放 record 空间。
10. 回收 undo record、undo page、undo segment。
11. 推进 history cursor，更新 metrics。
12. 判断 undo tablespace 是否满足 truncate 条件。

可清理条件：

- undo log 状态是 `COMMITTED_IN_HISTORY`。
- undo log 的 `TransactionNo` 小于最老 ReadView 需要的边界。
- 不属于 prepared transaction。
- 不属于 recovery 正在 rollback 的 recovered active transaction。
- delete-mark 记录的当前状态仍与 undo record 匹配。

### 7.8 Undo Tablespace Truncation

`UndoTruncationService` 执行空间回收：

1. `PurgeCoordinator` 发现某 undo tablespace 中没有 active slot，history list 不再引用其 update undo，cached segment 可释放。
2. lifecycle S 锁持有者释放后，服务获取 lifecycle X 锁。
3. 状态 `ACTIVE/INACTIVE -> TRUNCATE_CANDIDATE -> TRUNCATING`。
4. 刷出该 tablespace 相关 dirty undo page。
5. 调用 Disk Manager/FSP 截断或重建文件到初始大小。
6. 写入新的 truncate epoch 和 redo/checkpoint 边界。
7. 状态回到 `ACTIVE` 或保持 `INACTIVE`，取决于配置。

约束：

- 至少保留一个 `ACTIVE` undo tablespace 可分配新事务 undo。
- truncate 不等待事务行锁，不进入事务死锁检测。
- crash 发生在 truncation 中间时，recovery 通过 truncate epoch 和 tablespace state 继续或回退到可恢复状态。

### 7.9 DDL Undo 边界

DDL 语义分层：

- 行级 undo 只处理用户表数据记录的插入、更新、delete-mark。
- 数据字典变更、表空间创建删除、索引构建切换由 DDL log 和数据字典模块处理。
- `DDL_MARKER` undo record 只记录当前事务包含 DDL 相关行变更的边界，供 recovery 和诊断关联。
- 在线 DDL 产生的中间索引、临时表和字典对象不由 purge worker 直接删除；purge 只能通过 DDL recovery 暴露的 cleanup API 协作。

## 8. 与其它模块的协作

| 协作模块 | 交互 | 边界 |
| --- | --- | --- |
| Transaction | 分配 `TransactionId`、`TransactionNo`、状态变更、commit/rollback 编排 | Undo 不维护活跃事务表 |
| ReadView/MVCC | 提供最老 ReadView、按 roll pointer 构造旧版本 | Undo 不决定隔离级别 |
| Record | 提供 `RecordImage`、hidden column accessor、changed column before image | Undo 不解析 record byte layout |
| B+Tree | rollback/purge 重新定位聚簇记录和二级索引项 | Undo 不执行页分裂/合并策略 |
| LockManager | DML 写入前获取行锁；rollback 释放事务锁 | Undo page latch 不进入 Wait-For Graph |
| Buffer Pool | pin undo page、加载数据页、page latch、dirty publish | Undo 不直接持有 BufferFrame |
| Disk Manager/FSP | 分配 undo page、维护 undo tablespace 文件和 extent | Undo 不操作裸文件句柄 |
| Redo/MTR | undo page、rseg header、record purge/rollback 修改写 redo | Redo 不理解 undo record 业务语义 |
| Flush/Checkpoint | flush undo dirty page，推进 redo 可回收边界 | Purge 不直接 checkpoint |
| Crash Recovery | 扫描 undo、rollback active、重建 history、resume purge | Recovery 编排阶段，不解析 undo payload |
| Data Dictionary/DDL | table metadata、DDL recovery cleanup、external column policy | Undo 不替代 DDL log |

典型边界：

- DML 在获取行锁后调用 undo；如果行锁等待，必须在等待前释放 page latch 和 MTR。
- MVCC 读取 undo record 时只需要 undo page S latch；构造旧版本后立即释放。
- Purge worker 通过 B+Tree 的 purge-safe API 清理索引，不直接操作 B+Tree 内部 latch stack。
- Recovery rollback 使用与普通 rollback 相同的 undo command，但运行在 `RecoveryTrafficGate` 未开放普通 SQL 的阶段。

## 9. 并发与锁顺序

并发状态图见 [undo-purge-concurrency-state.mmd](diagrams/undo-purge-concurrency-state.mmd)。

### 9.1 锁和保护资源

| 锁或 guard | 保护资源 | 模式 | 进入事务死锁检测 | 等待策略 |
| --- | --- | --- | --- | --- |
| `UndoTablespaceLifecycleLock` | undo tablespace ACTIVE/INACTIVE/TRUNCATING/DROPPED 状态 | S, X | 否 | 短等待和 timeout |
| `RollbackSegmentDirectoryMutex` | rseg directory 内存映射 | X | 否 | 短临界区，不阻塞 IO |
| `RollbackSegmentSlotMutex` | rseg slot array、active slot | X | 否 | 短临界区 |
| `UndoSegmentMutex` | undo segment page list、cached/free 状态 | X | 否 | 短临界区 |
| `UndoPageLatch` | 单个 undo page 内容 | S, X | 否 | page latch timeout 和退避 |
| `HistoryListMutex` | history 运行时 head/tail/count 与唯一 transition flag | X + Condition | 否 | 独立 timeout；锁不跨 IO，lease 跨 IO |
| `PurgeQueueMutex` | purge task queue | X | 否 | worker 本地退避 |
| `PurgeWorkerTableToken` | 同一 table 的 purge 批次顺序 | X | 否 | 不等待行锁，失败重排 |
| `Record/GAP/NEXT_KEY Lock` | 用户记录、gap、next-key 范围 | S, X, gap, next-key, insert intention | 是 | Wait-For Graph 和 victim 选择 |
| `BTreePageLatch` | 索引页结构 | S, X, SX | 否 | latch coupling 规则和 timeout |
| `MtrGuard` | 一次物理修改的 latch/redo 生命周期 | exclusive owner | 否 | 不允许长等待 |

### 9.2 锁状态变化

通用状态：

- `FREE`：无持有者。
- `GRANTED`：锁已授予，记录 owner 为 transaction、purge worker、recovery stage 或 background service。
- `WAITING`：请求与已有 owner 冲突，进入等待队列。
- `CONVERTING`：lifecycle S 请求转换为 X；只允许在未持有 page latch、history mutex 和 row lock 时进入。
- `TIMEOUT`：短锁等待超时，释放 pin、wait slot 和本地 guard。
- `VICTIM`：仅事务行锁等待可能被死锁检测选为 victim。
- `RELEASED`：owner 清空，唤醒下一个兼容 waiter。

持有者变化：

| 操作 | 状态变化 | owner 变化 |
| --- | --- | --- |
| acquire 成功 | `FREE -> GRANTED` | owner 设置为请求方 |
| acquire 冲突 | `GRANTED -> WAITING` | owner 不变，请求方加入 waiters |
| grant waiter | `WAITING -> GRANTED` | owner 从旧持有者切换到 waiter |
| release | `GRANTED -> RELEASED -> FREE/GRANTED` | owner 清空或切换给兼容 waiter |
| upgrade | `GRANTED -> CONVERTING -> GRANTED` | owner 保持不变，模式升级 |
| timeout | `WAITING/CONVERTING -> TIMEOUT -> RELEASED` | waiter 清理自身，不改变原 owner |
| deadlock victim | `WAITING -> VICTIM -> RELEASED` | victim 事务进入 rollback，释放其行锁 |

### 9.3 标准锁顺序

普通 UPDATE/DELETE 写入顺序：

1. 事务状态 guard：确认 `ACTIVE`，预留 undoNo。
2. 获取 record/gap/next-key lock；如果等待，不能持有 page latch、undo page latch、history mutex、rseg mutex 或 MTR。
3. B+Tree 重新定位记录，进入短 MTR。
4. `RollbackSegmentSlotMutex` 或 `UndoSegmentMutex` 分配 undo 空间。
5. `UndoPageLatch X` 追加 undo record。
6. `BTreePageLatch/RecordPageLatch X` 修改聚簇记录和二级索引。
7. MTR commit 写 redo、设置 pageLSN、发布 dirty page。

commit 顺序：

1. 事务状态 `ACTIVE -> COMMITTING`。
2. 分配 `TransactionNo`。
3. `HistoryListMutex` 短持有取得 append lease 后立即释放；超时/中断发生在任何物理修改前。
4. 预检 page3 base/active owner、旧 tail 与新 UPDATE first page 后释放所有读 MTR。
5. 最终 MTR 按 FSP（若 drop INSERT）→page3→普通 UNDO pageNo 排序修改 owner、base 和节点链接，并写 terminal redo。
6. MTR commit 后发布 slot/cache/history；随后从 active table 移除、关闭 ReadView、释放事务锁并唤醒 purge。

purge 顺序：

1. `PurgeCoordinator` 取运行时 head，并在事务系统短锁内复核 creator 已非 active、low water 与所有 live ReadView 可见性。
2. worker 通过 B+Tree purge-safe API 定位并清理 delete-mark；期间不持有 history transition。
3. B+Tree tasks 全部成功后，短持有 `HistoryListMutex` 取得 head-removal lease。
4. 预检 page3 base/head owner、removed/new-head first page；最终 MTR 原子 unlink、cache/drop owner 并写 redo。
5. MTR commit 后才发布 slot/cache/history；失败前不移动内存 head，越过物理边界失败进入 fail-stop fence。
6. 可选 `UndoTablespaceLifecycleLock X` 执行 truncate；获取前必须释放所有 page latch、history mutex 和 purge queue mutex。

recovery 顺序：

1. `RecoveryTrafficGate` 阻断普通 SQL。
2. redo replay 完成后，`UndoRecoveryService` 扫描 rseg header。
3. 对 recovered active transaction 执行 rollback；此时无用户行锁等待。
4. 从 page3 head 沿 first-page next 以一节点一短 MTR 重建 history，校验 exact length、双向链接、slot/状态/identity 与 counter 高水位。
5. 启动 purge resume。

### 9.4 等待与死锁边界

必须释放后再等待：

- 等待 row/gap/next-key lock 前，释放所有 page latch、undo page latch、MTR、history mutex、rseg mutex。
- 等待 undo tablespace lifecycle X 锁前，释放 purge queue mutex、history mutex、undo page latch 和 B+Tree page latch。
- 等待 redo fsync 或 checkpoint pressure 前，释放 undo page latch、record page latch 和 rseg mutex。
- 等待 Buffer Pool loading future 前，不能持有 HistoryListMutex 或 PurgeQueueMutex。

进入事务死锁检测：

- 用户事务的 record lock、gap lock、next-key lock、insert intention lock。
- savepoint rollback 期间若仍等待用户行锁，由所属事务进入 Wait-For Graph。

只允许 timeout 或退避：

- undo page latch。
- rollback segment slot mutex。
- undo segment mutex。
- history list mutex。
- purge queue mutex。
- undo tablespace lifecycle lock。
- truncate 等待 active slot 清空。
- purge worker 等待 B+Tree page latch。

Purge worker 不作为事务节点进入 Wait-For Graph。遇到用户事务持有行锁或 page latch 长时间冲突时，worker 放弃当前 task、记录 retry 计数，并由 coordinator 重新排队或延后。

## 10. 异常处理

| 异常 | 发现点 | 处理策略 |
| --- | --- | --- |
| undo tablespace 空间不足 | `UndoTablespaceRepository.allocatePage` | 请求 flush/purge；仍不足则让当前写事务失败并 rollback |
| rollback segment slot 耗尽 | `RollbackSegmentSlotManager.assignSlot` | 尝试其它 rseg；全部耗尽则等待短周期，超时后返回事务启动失败 |
| undo page checksum 错误 | MVCC/rollback/recovery 读取 undo page | 正常模式下标记表空间损坏并停止；强制诊断模式只读开放受限 |
| roll pointer 指向不存在 | `findUndoRecord` | 报告数据损坏；禁止继续构造旧版本 |
| undo record 格式版本不支持 | codec 解析 | recovery 停止在可诊断状态；普通运行触发表空间错误 |
| rollback 单条 undo 失败 | `RollbackService.applyUndoRecord` | 标记事务 rollback failed，保持流量门控或会话错误，避免提交 |
| purge 单 task 失败 | `PurgeWorker` | 回滚该 task 的 MTR，记录 retry；超过阈值暂停对应 table 的 purge |
| purge 发现记录已变化 | worker 二次校验 | 跳过该 record，保留 undo 回收判断由 coordinator 复核 |
| truncation 中断 | recovery 扫描 tablespace state | 根据 truncate epoch 继续截断或恢复到 `ACTIVE/INACTIVE` |
| redo fsync 失败 | MTR commit 或 commit wait | 当前事务失败；已写内存状态不得对外宣称 durable commit |
| DDL marker 无对应 DDL log | recovery 或 purge | 暂停涉及对象清理，交由 DDL recovery 错误策略处理 |

异常原则：

- Undo 记录损坏属于高风险数据一致性问题，不能静默跳过。
- Purge 清理失败不影响已提交可见性，但会影响空间回收；应暂停局部对象而不是全局无限重试。
- Rollback 失败意味着事务不能安全结束，必须阻止同一事务继续执行。
- Recovery 期发现无法解释的 undo 状态时，普通 SQL 不能开放。

## 11. API 设计

本文只定义接口语义，不提供 Java 实现代码。

### 11.1 UndoLogManager

| 方法 | 输入 | 输出 | 语义 |
| --- | --- | --- | --- |
| `planInsert/planUpdate/planDelete` | transaction, before image | undo write plan | 在 MTR admission 前冻结 kind、binding/header、编码、空间与 redo workload |
| `appendPlanned` | transaction, mtr, write plan | roll pointer | 校验计划后写目标独立 log，并发布目标 head 与全局 undoNo |
| `findUndoRecord` | roll pointer | undo record | 读取并解析 undo record |
| `buildPreviousVersion` | current version, undo record, read view | record version | 构造旧版本 |
| `markCommitted` | transaction, transaction no, mtr | commit undo result | 把 update undo 挂入 history，释放 insert undo |
| `releaseRolledBack` | transaction, mtr | release result | full rollback 后释放 slot 和 undo segment |

### 11.2 RollbackService

| 方法 | 输入 | 输出 | 语义 |
| --- | --- | --- | --- |
| `rollback` | transaction, mode | rollback summary | 完整回滚或 recovery rollback |
| `rollbackToSavepoint` | transaction, savepoint id | rollback summary | 撤销保存点之后的 undo |
| `rollbackStatement` | transaction, statement id | rollback summary | 语句失败时回滚本语句修改 |
| `applyUndoRecord` | undo record, rollback context | apply result | 对单条 undo 执行反向命令 |

### 11.3 PurgeCoordinator

| 方法 | 输入 | 输出 | 语义 |
| --- | --- | --- | --- |
| `start` | purge config | worker handles | 启动后台 purge |
| `requestPurge` | reason | accepted flag | commit、history pressure 或 recovery resume 唤醒 |
| `runBatch` | max records, max millis | purge batch summary | 执行一个可测试批次 |
| `computeBoundary` | none | purge boundary | 基于最老 ReadView 和活跃事务计算边界 |
| `pauseTable` | table id, reason | none | 对局部对象暂停 purge |
| `snapshot` | none | purge metrics snapshot | 返回 history length、lag、worker 状态 |

### 11.4 UndoRecoveryService

| 方法 | 输入 | 输出 | 语义 |
| --- | --- | --- | --- |
| `scanRollbackSegments` | recovery context | recovered rseg set | 扫描 rseg header 和 undo log header |
| `rebuildHistoryList` | recovered rseg set | history snapshot | 重建 committed update undo 链 |
| `rollbackRecoveredActive` | recovered transaction set | rollback summary | 回滚 crash 前未提交事务 |
| `resumePurge` | history snapshot | purge resume result | 恢复 purge cursor 和后台任务 |
| `recoverTruncation` | tablespace state | recovery result | 处理未完成 undo tablespace truncation |

## 12. 设计模式使用清单

| 模式 | 使用点 | 理由 |
| --- | --- | --- |
| Facade | `UndoLogManager`, `RollbackService`, `PurgeCoordinator` | 隔离内部页、slot、history 结构 |
| Repository | `UndoTablespaceRepository`, `RollbackSegmentDirectory` | 隐藏持久页访问和缓存策略 |
| Aggregate Root | `RollbackSegment`, `UndoSegment` | 维护 slot、页链、状态一致性 |
| Value Object | `RollPointer`, `UndoNo`, `PurgeBoundary`, `Savepoint` | 避免裸 long 传递关键边界 |
| Command | `UndoRecord` | 同一记录支持 rollback、version build、purge task |
| State | `UndoLogStateMachine`, `UndoTablespaceState` | 明确 active、committed、purging、reusable 边界 |
| Strategy | rollback segment 选择、purge batch sizing、truncate policy | 支持性能策略替换 |
| Template Method | rollback 单条 undo 应用流程 | 固定定位、校验、MTR、redo、发布步骤 |
| Chain of Responsibility | recovery stage、undo record parser | 分阶段处理恢复和格式演进 |
| Observer | metrics、commit 唤醒 purge、history pressure 事件 | 后台 worker 解耦 |
| Snapshot | `PurgeBoundary`, ReadView 快照 | purge 以稳定边界运行一个批次 |
| RAII Guard | MTR guard、page latch guard、slot guard | 保证异常时释放短锁和 pin |

## 13. 高内聚、低耦合约束

高内聚约束：

- `UndoRecordCodec` 只负责编解码，不决定 purge 可清理性。
- `PurgeCoordinator` 只调度和推进边界，不直接写 record page。
- `PurgeWorker` 只处理已分配 task，不扫描全局 history list。
- `RollbackService` 只执行撤销，不分配新 undo slot。
- `UndoTruncationService` 只处理 undo tablespace 生命周期，不清理用户表记录。
- `UndoRecoveryService` 只恢复 undo/purge 内存态并触发 recovered rollback，不决定 SQL 流量开放顺序。

低耦合约束：

- Record 层只通过值对象提供 old image，不暴露内部 byte array 给 undo 长期持有。
- B+Tree 提供 `purgeDeleteMarkedRecord` 和 `rollbackRecordChange` 语义接口，不暴露 latch 栈。
- Redo 只保存物理 record，不 import undo command。
- Buffer Pool 只提供 page access 和 latch，不知道 history list。
- Transaction 只保存 `UndoContext` 引用，不直接修改 undo page。
- Recovery 只调用 `UndoRecoveryService`，不解析 undo payload。

## 14. 典型数据流

### 14.1 一致性读旧版本构造

1. SQL 普通 SELECT 进入 MVCC consistent read。
2. `ReadViewManager` 创建或复用 ReadView。
3. B+Tree 定位聚簇记录当前版本。
4. Record 读取 `DB_TRX_ID`、`DB_ROLL_PTR`、delete mark。
5. `MvccVisibilityService` 判断当前版本是否可见。
6. 不可见时调用 `UndoLogManager.findUndoRecord`。
7. `UndoRecord.buildPreviousVersion` 构造旧版本。
8. 继续判断旧版本可见性。
9. 找到可见版本则返回；遇到不可见 insert undo 则返回记录不存在。

### 14.2 UPDATE 写入

1. 执行器定位候选行。
2. LockManager 获取 X 或 next-key X。
3. 等待后重新定位，确认谓词仍成立。
4. 进入 MTR。
5. `planUpdate` 在 MTR 外冻结 UPDATE undo 写计划，业务 MTR 内先 `appendPlanned` 再修改聚簇记录。
6. Record 更新 payload 和隐藏列。
7. MTR commit 写 redo。
8. 事务保持 `ACTIVE`，锁保留到 commit/rollback。

### 14.3 Commit 到 Purge

1. commit 分配 transaction no。
2. update undo log header 进入 history list。
3. insert undo 释放或缓存。
4. 事务从 active table 移除。
5. 关闭 ReadView，释放锁。
6. `PurgeCoordinator` 被唤醒。
7. purge 根据最老 ReadView 判断候选 undo。
8. worker 清理 delete-mark 和 undo 空间。
9. history cursor 前进，history length 下降。

### 14.4 Rollback

1. rollback 进入 `ROLLING_BACK`。
2. 从最新 roll pointer 读取 undo record。
3. 根据 undo type 执行反向命令。
4. 每条命令用独立 MTR 写 redo。
5. 更新 rollback progress。
6. undo 链耗尽后释放 slot、锁和 ReadView。
7. 状态进入 `ROLLED_BACK`。

### 14.5 Crash Recovery

恢复流程图见 [undo-purge-recovery-flow.mmd](diagrams/undo-purge-recovery-flow.mmd)。

1. Redo 已恢复 undo page、rseg header、trx system page 到物理一致。
2. `UndoRecoveryService` 扫描 rollback segment directory。
3. 恢复 active、committed、prepared undo log header。
4. committed update undo 重建 history list。
5. recovered active transaction 通过 rollback service 回滚。
6. prepared transaction 保留给上层协调。
7. 计算启动期 purge boundary。
8. resume purge，并恢复未完成 undo truncation。

## 15. 测试设计

### 15.1 单元测试

| 测试对象 | 测试点 |
| --- | --- |
| `RollPointer` | 编解码、比较、非法 page/offset 拒绝 |
| `UndoRecordCodec` | insert/update/delete-mark payload 往返、格式版本升级 |
| `UndoContext` | undoNo 递增、savepoint 边界、last roll pointer 更新 |
| `RollbackSegmentSlotManager` | slot 分配、释放、耗尽、并发分配 |
| `HistoryList` | append、peek boundary、cursor advance、长度统计 |
| `PurgeBoundary` | ReadView 集合、active transaction、recovery 状态组合 |
| `UndoLogStateMachine` | active、commit、rollback、purge、reusable 状态合法性 |

### 15.2 集成测试

- INSERT commit 后 insert undo 可释放，consistent read 不依赖 insert undo。
- UPDATE commit 后旧 ReadView 能通过 update undo 读取旧版本。
- DELETE commit 后旧 ReadView 仍能看到旧版本，新 ReadView 看不到记录。
- purge 在没有旧 ReadView 后物理清理 delete-mark 记录。
- 长事务保持 ReadView 时，history list 不推进到该事务需要的版本之后。
- rollback 大事务分批应用 undo，中途 crash 后 recovery 能继续。
- savepoint rollback 只撤销保存点之后的修改。
- prepared transaction 的 undo 不被 purge。
- undo tablespace inactive 后不再分配新 undo，已有 undo 仍可读。
- undo truncation crash 后 recovery 能识别 epoch 并恢复一致状态。

### 15.3 并发测试

- 多事务并发分配 rollback segment slot，无重复 owner。
- UPDATE 行锁等待前不持有 page latch，通过注入检测 latch 集合为空。
- purge worker 与用户 UPDATE 冲突时退避，不进入 Wait-For Graph。
- undo tablespace truncation 等待 active slot 清空，timeout 后不破坏 active undo。
- history list append 与 purge cursor advance 并发保持链表一致。
- MVCC 读 undo page 与 rollback/purge 并发时，page latch 状态可解释且无悬空 roll pointer。
- 行锁死锁 victim rollback 时，undo 链完整应用，锁释放顺序正确。

### 15.4 Recovery 与故障注入测试

- crash 在 undo record 写入后、record page 修改前：redo 后 undo 可解释，事务 rollback 幂等。
- crash 在 record page 修改后、commit 前：recovery rollback 恢复旧版本。
- crash 在 commit history append 后、active table remove 前：recovery 识别 committed update undo 并进入 history。
- crash 在 purge 删除二级索引后、聚簇记录删除前：purge task 可重试并完成。
- crash 在 undo page 回收后、history cursor 推进前：recovery 不重复暴露已释放 undo。
- crash 在 truncation 中间：根据 tablespace state 和 epoch 继续或回退。
- undo page checksum 错误：普通恢复失败，诊断模式只读开放策略可控。

### 15.5 性质测试与压力测试

- 随机事务序列下，任意 ReadView 读取结果等价于按事务提交序号重放得到的快照。
- 随机 rollback/savepoint/commit 组合下，最终聚簇记录隐藏列和 undo 链一致。
- purge 任意批次大小下不改变用户可见结果。
- history list 长度在无长事务且 purge 开启时最终下降。
- redo recovery 后所有 undo page 的 pageLSN 不小于应用过的 redo record end LSN。

## 16. 后续实现顺序

1. 定义值对象：`UndoTablespaceId`、`RollbackSegmentId`、`UndoPageId`、`UndoNo`、`RollPointer`、`PurgeBoundary`。
2. 实现 `UndoRecordCodec` 和 `RollPointerCodec` 的内存往返测试。
3. 实现 `UndoContext`、savepoint stack 和 undoNo 分配。
4. 实现 rollback segment directory 的内存仓储和 slot manager。
5. 接入 Disk Manager/FSP 的 undo tablespace page 分配接口。
6. 实现 undo page append/read 和 MTR redo 集成。
7. 实现 INSERT undo 写入与 rollback 删除未提交插入。
8. 实现 UPDATE/DELETE_MARK undo 写入与 MVCC 旧版本构造。
9. 实现 commit 时 update undo 挂入 history list 和 insert undo 释放。
10. 实现 full rollback、statement rollback、savepoint rollback。
11. 实现 `PurgeBoundary` 计算和 history list cursor。
12. 实现单线程 purge batch，先清理 delete-mark 聚簇记录，再接入二级索引清理。
13. 扩展为多 worker purge，按 table/index/page 分片。
14. 实现 undo segment/page 回收和 cached segment 复用。
15. 实现 undo tablespace inactive/truncate 生命周期。
16. 接入 crash recovery：扫描 rseg、重建 history、rollback recovered active、resume purge。
17. 增加 metrics、故障注入、长事务 purge lag 诊断。
18. 扩展 DDL marker、temporary undo、external column undo。

## 17. 简化点与扩展路径

| 主题 | 第一阶段 | 扩展路径 |
| --- | --- | --- |
| undo tablespace 数量 | 固定配置，启动时加载 | 支持在线 ADD/DROP undo tablespace |
| rollback segment 分配 | 轮询或哈希 | 根据 slot 压力、history length 和 tablespace IO 负载动态选择 |
| undo record 格式 | Mini 版本化格式 | 增加压缩、external payload、格式迁移 |
| purge worker | table/page 分片 | 引入自适应批次、IO 限速、优先清理大 history rseg |
| undo truncation | 手动或阈值触发 | 根据磁盘压力、checkpoint age、history lag 自动决策 |
| DDL undo | 只定义 marker 和边界 | 与 atomic DDL log 深度集成 |
| temporary undo | 预留 segment kind | 临时表模块接入后进入独立临时 undo tablespace |

## 18. 十五轮自检记录

| 轮次 | 检查主题 | 检查结果 |
| --- | --- | --- |
| 1 | 只写设计文档 | 本次内容只包含 Markdown 设计文档和 Mermaid 图，没有 Java 源码 |
| 2 | 目标与非目标 | 已明确 Undo/Purge 的职责、非目标和与事务/MTR 的边界 |
| 3 | MySQL 8.0 对齐 | 已覆盖 undo log、rollback segment、undo tablespace、history list、ReadView、purge、recovery rollback 和 undo truncation |
| 4 | 高内聚 | undo storage、history、rollback、purge、truncation、recovery、metric 子包职责独立 |
| 5 | 低耦合 | Record、B+Tree、Redo、Buffer Pool、Recovery 均通过稳定接口协作，不访问 undo 内部结构 |
| 6 | 面向对象 | 使用聚合根、值对象、状态对象、命令对象、仓储和后台 worker 表达领域模型 |
| 7 | 设计模式 | 已列出 Facade、Repository、State、Command、Strategy、Template Method、Observer、Snapshot、RAII Guard 等使用点 |
| 8 | 核心领域模型 | 已定义 `UndoContext`、`RollbackSegment`、`UndoSegment`、`UndoRecord`、`HistoryList`、`PurgeCoordinator`、`RollbackService` |
| 9 | 依赖方向 | 已给出推荐依赖方向和禁止方向，避免事务层直接操作裸文件或 BufferFrame |
| 10 | 逻辑与物理区分 | 已区分逻辑事务、逻辑 undo、逻辑 purge、物理 undo page、物理 record page 和 redo |
| 11 | 关键数据流 | 已覆盖一致性读、UPDATE、commit、rollback、purge、crash recovery 数据流 |
| 12 | 图示完整性 | 已提供架构图、类关系图、版本链图、purge 流、rollback 流、并发状态图和恢复流 |
| 13 | 并发锁状态 | 已明确锁保护资源、模式、状态变化、持有者变化、锁顺序、等待前释放规则和死锁边界 |
| 14 | 异常与恢复 | 已覆盖空间不足、slot 耗尽、checksum、roll pointer 损坏、purge 失败、truncation 中断和 recovery 策略 |
| 15 | 测试与实施 | 已覆盖单元、集成、并发、恢复、性质测试和后续实现顺序，并确认没有未完成标记 |

## 19. 参考链接

- MySQL 8.0 Reference Manual - InnoDB Multi-Versioning: https://dev.mysql.com/doc/refman/8.0/en/innodb-multi-versioning.html
- MySQL 8.0 Reference Manual - Consistent Nonlocking Reads: https://dev.mysql.com/doc/refman/8.0/en/innodb-consistent-read.html
- MySQL 8.0 Reference Manual - InnoDB Undo Logs: https://dev.mysql.com/doc/refman/8.0/en/innodb-undo-logs.html
- MySQL 8.0 Reference Manual - Undo Tablespaces: https://dev.mysql.com/doc/refman/8.0/en/innodb-undo-tablespaces.html
- MySQL 8.0 Reference Manual - Purge Configuration: https://dev.mysql.com/doc/refman/8.0/en/innodb-purge-configuration.html
- MySQL 8.0 Reference Manual - InnoDB Locking: https://dev.mysql.com/doc/refman/8.0/en/innodb-locking.html
- MySQL 8.0 Reference Manual - InnoDB Startup Options and System Variables: https://dev.mysql.com/doc/refman/8.0/en/innodb-parameters.html
- MySQL 8.0 Reference Manual - InnoDB Recovery: https://dev.mysql.com/doc/refman/8.0/en/innodb-recovery.html
- MySQL 8.0.46 Source Documentation - `trx0undo`: https://dev.mysql.com/doc/dev/mysql-server/8.0.46/trx0undo_8h.html
- MySQL 8.0.46 Source Documentation - `trx0purge`: https://dev.mysql.com/doc/dev/mysql-server/8.0.46/trx0purge_8h.html
- MySQL 8.0.46 Source Documentation - `trx0rseg`: https://dev.mysql.com/doc/dev/mysql-server/8.0.46/trx0rseg_8h.html
