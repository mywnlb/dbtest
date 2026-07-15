# MiniMySQL InnoDB 风格事务与 MVCC 模块设计

版本：2026-06-04  
实现语言：Java  
参考基线：MySQL 8.0.46 InnoDB 官方手册与源码文档  
关联设计：[innodb-crash-recovery-design.md](innodb-crash-recovery-design.md)，[innodb-disk-manager-design.md](innodb-disk-manager-design.md)，[innodb-buffer-pool-design.md](innodb-buffer-pool-design.md)，[innodb-undo-log-purge-design.md](innodb-undo-log-purge-design.md)，[mysql-lock-observability-deadlock-design.md](mysql-lock-observability-deadlock-design.md)，[mysql-session-connection-protocol-design.md](mysql-session-connection-protocol-design.md)

## 1. 目标与边界

本设计面向 MiniMySQL 存储引擎的数据库事务模块。磁盘管理模块负责表空间、段、区、页和物理 MTR；Buffer Pool 模块负责页缓存、frame、dirty、flush。事务模块负责数据库事务生命周期、隔离级别、ReadView、MVCC 可见性、undo log、rollback、purge、行锁协作与提交顺序。

设计目标：

- 高内聚：事务状态、事务 ID、ReadView、undo、MVCC 版本构造、rollback、purge、锁等待与死锁检测分别收敛在明确子包内。
- 低耦合：记录层和 B+Tree 只依赖 `TransactionManager`、`MvccVisibilityService`、`UndoLogManager`、`LockManager` 等稳定接口，不直接读写活跃事务表或 undo page。
- InnoDB 风格：参考 MySQL 8.0 的 `DB_TRX_ID`、`DB_ROLL_PTR`、ReadView、consistent nonlocking read、当前读、insert/update undo、rollback segment、history list、purge 和 next-key lock。
- Java 可落地：用值对象、领域聚合、状态机、策略、仓储、模板方法、观察者和命令对象表达事务语义。
- 可恢复：提交、回滚、undo 写入和事务状态变更必须与 redo/MTR 配合，保证 crash recovery 后能完成未决事务回滚或 prepared transaction 恢复。

非目标：

- 不实现完整 SQL Parser、查询优化器、复制、XA 两阶段提交的完整上层协议。
- 不实现完整外键、空间索引 predicate lock、全文索引、change buffer。
- 不改变已有磁盘管理和 Buffer Pool 的边界。
- 不生成 Java 源码。本文件只定义设计、关系和数据流。

术语边界：

- `Transaction` 是数据库长事务，跨多条语句，提供 ACID 语义。
- `MiniTransaction` / `MTR` 是物理短事务，只保护页级 latch、redo 和物理一致性。
- 一个数据库事务会启动多个 MTR；MTR 不能替代数据库事务 rollback。

## 2. MySQL 8.0 参考依据

本设计参考以下 MySQL 8.0 行为，文末列出官方链接。

- InnoDB 事务模型把多版本数据库能力与传统两阶段锁结合起来，默认普通查询走非锁定一致性读。
- InnoDB 支持 SQL 标准四个隔离级别：`READ UNCOMMITTED`、`READ COMMITTED`、`REPEATABLE READ`、`SERIALIZABLE`，默认隔离级别是 `REPEATABLE READ`。
- 在 `REPEATABLE READ` 下，同一事务内一致性读使用第一次一致性读创建的快照；在 `READ COMMITTED` 下，每次一致性读创建新的快照。
- InnoDB 在聚簇索引记录中维护隐藏列：`DB_TRX_ID`、`DB_ROLL_PTR`，以及必要时的 `DB_ROW_ID`。删除在内部表现为带 delete-mark 的更新。
- `DB_ROLL_PTR` 指向 rollback segment 中的 undo log record；一致性读需要旧版本时，通过 undo log 还原早期记录版本。
- Undo log 分 insert undo 和 update undo。insert undo 只用于事务回滚，事务提交后可丢弃；update undo 也用于一致性读，只能在没有 ReadView 需要它时由 purge 清理。
- ReadView 记录一致性读不应看见的活跃读写事务 ID 集合，并维护低水位/高水位边界；purge 通过最老 ReadView 判断哪些历史版本可清理。
- 当前读和写操作需要锁。`SELECT ... FOR SHARE` 获取共享锁，`SELECT ... FOR UPDATE` 和 DML 获取排他语义；在 `REPEATABLE READ` 下范围搜索会使用 gap lock 或 next-key lock 阻止幻读。
- 旧版本记录不能被锁定；它们是通过 undo log 在内存中重建出来的。

## 3. 总体架构

架构图见 [transaction-architecture.mmd](diagrams/transaction-architecture.mmd)。

模块分为九组：

1. `storage.trx.api`：事务门面，包括 `TransactionManager`、`TransactionContext`、`TransactionalSession`。
2. `storage.trx.core`：事务对象、状态机、事务 ID 分配、活跃事务表、提交序号。
3. `storage.trx.readview`：ReadView 创建、复用、关闭、最老快照维护。
4. `storage.trx.mvcc`：可见性判断、旧版本构造、当前读与一致性读入口。
5. `storage.trx.undo`：undo log 分配、undo record 编码、rollback segment、history list。
6. `storage.trx.lock`：行锁、表意向锁、gap/next-key lock、等待队列、死锁检测。
7. `storage.trx.purge`：后台 purge、history list 扫描、delete-mark 清理、undo 回收。
8. `storage.trx.recovery`：启动时恢复 active/prepared 事务，重建事务系统内存态。
9. `storage.trx.metric`：事务数、ReadView 数、history list 长度、lock wait、purge lag。

核心原则：

- 事务模块不直接操作文件和裸 byte array，所有页访问通过 `BufferPool`、`PageCursor` 和 MTR。
- 事务模块不实现 B+Tree 查找算法，只在记录访问点提供可见性、锁和 undo 能力。
- 记录层负责解释记录格式；事务模块只定义隐藏列、undo payload 和可见性协议。
- MTR 只保证单次页修改的物理原子性；数据库事务提交/回滚由 `TransactionManager` 编排多个 MTR。
- purge 是后台清理，不改变已提交事务对外语义。

## 4. 包与职责

| 包 | 职责 | 主要依赖 | 设计模式 |
| --- | --- | --- | --- |
| `storage.trx.api` | 事务入口、session 绑定、隔离级别、提交回滚 | `core`, `readview`, `undo`, `lock` | Facade |
| `storage.trx.core` | `Transaction`、状态机、事务 ID、活跃事务表 | `domain`, `redo` | State, Repository |
| `storage.trx.readview` | ReadView 创建/复用/关闭、最老快照 | `core` | Snapshot, Object Pool |
| `storage.trx.mvcc` | 可见性判断、旧版本构造、当前读/快照读 | `readview`, `undo`, `record` | Strategy, Chain of Responsibility |
| `storage.trx.undo` | undo segment、undo record、rollback、history list | `fsp`, `buf`, `mtr`, `redo` | Command, Repository |
| `storage.trx.lock` | 行锁、gap lock、next-key lock、等待图、死锁检测 | `core`, `btree` | Mediator, Wait-For Graph |
| `storage.trx.purge` | 清理 delete-mark、回收 update undo、推进 purge view | `readview`, `undo`, `btree` | Background Worker |
| `storage.trx.recovery` | 恢复事务系统、回滚 active、恢复 prepared | `redo`, `undo`, `core` | Recovery Handler |
| `storage.trx.metric` | 事务与 MVCC 指标 | 无 | Observer |

推荐依赖方向：

`sql/session -> storage.trx.api -> core/readview/mvcc/undo/lock`  
`record/btree -> mvcc + lock + undo`  
`undo -> mtr + buf + fsp + redo`  
`purge -> readview + undo + btree + mtr`

禁止方向：

- `storage.trx.core` 不能依赖 SQL parser 或执行计划。
- `storage.trx.readview` 不能读写记录页。
- `storage.trx.lock` 不能决定 MVCC 可见性。
- `storage.trx.undo` 不能决定隔离级别。
- `storage.trx.purge` 不能清理仍被最老 ReadView 需要的 update undo。
- `storage.buf`、`storage.fil`、`storage.fsp` 不能反向依赖事务语义。

## 5. 核心领域模型

类关系图见 [transaction-class-relation.mmd](diagrams/transaction-class-relation.mmd)。

### 5.1 值对象与枚举

- `TransactionId`：递增事务 ID，对应记录隐藏列 `DB_TRX_ID`。
- `TransactionNo`：事务提交序号，用于 purge 和 history list 边界。
- `UndoNo`：事务内部 undo record 序号。
- `RollPointer`：指向 undo record 的位置，包含 undo tablespace、page no、offset、insert/update 类型。
- `ReadViewId`：ReadView 标识，用于调试与监控。
- `IsolationLevel`：`READ_UNCOMMITTED`、`READ_COMMITTED`、`REPEATABLE_READ`、`SERIALIZABLE`。
- `TransactionState`：`NOT_STARTED`、`ACTIVE`、`COMMITTING`、`COMMITTED`、`ROLLING_BACK`、`ROLLED_BACK`、`PREPARED`、`RECOVERED_ACTIVE`。
- `ReadType`：`CONSISTENT_READ`、`CURRENT_READ`、`LOCKING_READ_SHARE`、`LOCKING_READ_UPDATE`。
- `UndoRecordType`：`INSERT`、`UPDATE`、`DELETE_MARK`、`DELETE_PURGE`。

这些对象必须不可变或由状态机独占修改，避免用裸 `long` 传递事务边界。

### 5.2 Transaction

`Transaction` 是数据库事务聚合根：

- `transactionId`
- `transactionNo`
- `state`
- `isolationLevel`
- `readOnly`
- `autoCommit`
- `startTime`
- `readView`
- `undoSlots`
- `heldLocks`
- `modifiedTables`
- `lastStatementId`
- `rollbackOnly`
- `commitLsn`

职责：

- 维护事务状态和生命周期。
- 持有本事务的 ReadView 引用。
- 持有 undo log slot 和 lock 列表。
- 为语句级 rollback 提供 savepoint。
- 在 commit/rollback 时通知 undo、lock、readview、metric 子模块。

约束：

- 只读事务可以不分配 `TransactionId`，直到需要创建 ReadView 或执行写操作。
- 读写事务第一次写入前必须分配 `TransactionId`。
- 已进入 `COMMITTING` 的事务不允许再创建新 undo 或获取新业务锁。
- `Transaction` 不能直接持有 `BufferFrame` 或 `PageHandle`。

### 5.3 TransactionSystem

`TransactionSystem` 是事务全局协调器：

- `nextTransactionId`
- `nextTransactionNo`
- `activeTransactions`
- `rwTransactionIds`
- `mvcc`
- `rollbackSegments`
- `lockSystem`
- `historyList`
- `purgeCoordinator`

职责：

- 分配事务 ID 和提交序号。
- 维护活跃读写事务集合，供 ReadView 拷贝。
- 查找事务状态，支持锁等待和恢复。
- 暴露最老 ReadView 与 purge low limit。

事务系统的全局锁必须短持有。创建 ReadView 时允许短暂复制活跃事务 ID 集合，但不允许在持有全局锁时访问 Buffer Pool 或等待行锁。

### 5.4 ReadView

`ReadView` 表示一致性读快照：

- `creatorTransactionId`
- `upLimitId`：小于该值的事务 ID 默认可见。
- `lowLimitId`：大于等于该值的事务 ID 不可见。
- `activeTransactionIds`：创建快照时仍活跃的读写事务 ID 集合。
- `lowLimitNo`：purge 可参考的事务提交序号边界。
- `closed`

可见性规则：

1. 记录 `DB_TRX_ID == 当前事务 ID`：可见，事务总能看见自己的修改。
2. `recordTrxId < upLimitId`：可见。
3. `recordTrxId >= lowLimitId`：不可见。
4. `activeTransactionIds` 包含 `recordTrxId`：不可见。
5. 其它情况：可见。

Purge 不能只比较 `lowLimitNo`。事务可能已经分配 `TransactionNo` 并持久化 UPDATE history，
但仍处于 prepare/onCommit 与从 active table 移除之间；最终摘除 head 前必须在事务系统短锁内复核：
creator 已离开 active table、提交号低于所有 live ReadView 的 low limit，且每个 live ReadView 都把该 creator
transaction 视为可见。即使 live 集合为空，也不能省略 active 检查。

隔离级别策略：

- `READ_UNCOMMITTED`：可直接读最新版本，但 delete-mark 记录仍需按当前语义处理。
- `READ_COMMITTED`：每条一致性读语句创建新的 ReadView，语句结束释放。
- `REPEATABLE_READ`：事务第一次一致性读创建 ReadView，事务结束释放。
- `SERIALIZABLE`：普通 `SELECT` 在 autocommit 关闭时转为锁定读；MiniMySQL 可先用 `LOCKING_READ_SHARE` 表达。

### 5.5 RecordVersion

事务模块要求记录层提供隐藏列访问能力：

- `dbTrxId`
- `dbRollPtr`
- `deletedFlag`
- `rowId`
- `clusteredPrimaryKey`
- `payload`

`RecordVersion` 是内存视图，不等同于磁盘记录。当前版本来自聚簇索引页，旧版本通过 undo record 链重建。

二级索引边界：

- MiniMySQL 第一阶段可要求二级索引命中后回表到聚簇索引，再由聚簇记录执行 MVCC 判断。
- 二级索引自身不存完整隐藏列时，不能仅凭二级索引项判断行可见性。
- 二级索引 delete-mark 与 purge 后续阶段再细化。

### 5.6 UndoRecord

`UndoRecord` 是回滚和旧版本构造的命令对象：

- `undoRecordType`
- `transactionId`
- `undoNo`
- `prevRollPointer`
- `tableId`
- `indexId`
- `primaryKey`
- `beforeImage`
- `changedColumns`
- `oldDbTrxId`
- `oldRollPointer`
- `deleteMarkBefore`
- `deleteMarkAfter`

职责：

- `applyRollback(RollbackContext)`：回滚当前事务修改。
- `buildPreviousVersion(VersionBuildContext)`：从当前记录构造上一版本。
- `isPurgeable(ReadView oldestView)`：判断是否可清理。

insert undo 与 update undo 区别：

- insert undo：只用于回滚新插入记录；事务提交后不进入长期 history list。
- update undo：用于回滚，也用于一致性读构造旧版本；提交后挂入 history list，等待 purge。

### 5.7 RollbackSegment

`RollbackSegment` 管理 undo log slot：

- `rollbackSegmentId`
- `spaceId`
- `state`
- `undoSlots`
- `historyListHead`
- `cachedInsertUndoSegments`
- `cachedUpdateUndoSegments`

分配策略：

- 读写事务按 round-robin 选择 rollback segment。
- 普通表写入使用 redo-logged undo tablespace。
- temporary table undo 使用 noredo temporary rollback segment，只支持运行期 rollback，不参与 crash recovery。
- 一个事务按操作类型最多需要 insert/update undo log；MiniMySQL 第一阶段可先支持普通表的 insert/update 两类。

## 6. 记录格式与 MVCC 元数据

聚簇索引记录必须包含：

| 字段 | 作用 |
| --- | --- |
| `DB_TRX_ID` | 最后插入或更新该记录的事务 ID |
| `DB_ROLL_PTR` | 指向 undo record，用于回滚和构造旧版本 |
| `DB_ROW_ID` | 没有显式主键时的隐藏行 ID；MiniMySQL 可保留扩展点 |
| `deleted_flag` | delete-mark，逻辑删除标记 |

更新规则：

- insert：写入新记录，`DB_TRX_ID = 当前事务 ID`，`DB_ROLL_PTR = insert undo`。
- update in-place：先写 update undo，再覆盖记录列值，更新 `DB_TRX_ID` 和 `DB_ROLL_PTR`。
- delete：先写 update undo，再设置 `deleted_flag = true`，更新 `DB_TRX_ID` 和 `DB_ROLL_PTR`。
- purge：在没有 ReadView 需要该历史版本后，物理删除 delete-mark 记录和可清理的 undo。

写入顺序：

1. 创建或定位 undo log slot。
2. 在 MTR 中写 undo record，并生成 redo。
3. 在 MTR 中修改聚簇索引记录隐藏列和 payload，并生成 redo。
4. MTR commit 后，数据库事务仍可能未提交；其它事务通过 ReadView 和 lock 判断是否可见。

## 7. 事务生命周期

状态图见 [transaction-state.mmd](diagrams/transaction-state.mmd)。

### 7.1 Begin

`TransactionManager.begin(options)`：

1. 创建 `Transaction`，状态为 `ACTIVE`。
2. 绑定当前 session/thread。
3. 记录隔离级别、readOnly、autoCommit。
4. 读写事务延迟分配事务 ID，直到第一次写或需要 ReadView creator id。
5. 只读 autocommit 查询可走轻量 `ReadOnlyTransactionContext`。

### 7.2 Commit

提交路径：

1. 禁止事务继续获取新锁和新 undo。
2. 提交当前语句 MTR，确保所有页修改已有 redo。
3. 为事务分配 `transactionNo`。
4. UPDATE log header 与 page3 history base 在同一 commit MTR 持久链接；INSERT log 同批 cache 或释放。
5. 将事务状态设为 `COMMITTING`。
6. 写事务 commit redo 或事务系统状态 redo。
7. 根据 durability policy 等待 redo durable。
8. 从 active transaction table 移除。
9. 关闭 ReadView。
10. 释放所有行锁和表意向锁。
11. 状态改为 `COMMITTED`。
12. 唤醒等待该事务锁的线程，并通知 purge。

关键边界：

- 事务 commit 不直接刷 data page。
- page flush 只要求 redo durable，不要求事务仍在内存中。
- commit 后旧版本是否可物理删除由 purge 决定。

### 7.3 Rollback

回滚路径：

1. 状态改为 `ROLLING_BACK`，禁止新操作。
2. 在独立 INSERT/UPDATE logs 的两个局部头之间按全局 undoNo 归并扫描。
3. 每条 undo record 以独立 MTR 应用反向修改。
4. insert undo：删除本事务插入且未提交的记录。
5. update undo：恢复旧列值、旧 `DB_TRX_ID`、旧 `DB_ROLL_PTR`、旧 delete flag。
6. 双 head 到 EMPTY 后同批清理两条 logs；未提交 UPDATE 不进入 history。
7. 释放锁，关闭 ReadView。
8. 从 active transaction table 移除。
9. 状态改为 `ROLLED_BACK`。

语句级 rollback 使用 `TransactionSavepoint`：

- `statementId`
- `undoNo`
- `lockSavepoint`
- `mtrSavepoint`

失败语句只回滚到该 savepoint，事务可继续；严重错误可设置 `rollbackOnly`。

### 7.4 Prepared

MiniMySQL 第一阶段可不实现完整 XA，但设计保留：

- `PREPARED` 状态。
- prepared undo state。
- recovery 阶段可恢复 prepared transaction 并等待上层决定 commit/rollback。

## 8. ReadView 与可见性

### 8.1 ReadView 创建

`ReadViewManager.openReadView(transaction, readType)`：

- `READ_UNCOMMITTED`：返回 `NoReadView` 或特殊策略对象。
- `READ_COMMITTED`：每条 consistent read statement 创建新的 ReadView。
- `REPEATABLE_READ`：事务级缓存 ReadView，第一次 consistent read 创建。
- `SERIALIZABLE`：普通 select 由上层转 locking read，必要时仍可创建 ReadView 用于内部判断。

创建步骤：

1. 短暂持有 `TransactionSystem` 读写锁。
2. 复制当前活跃读写事务 ID 集合。
3. 设置 `upLimitId = min(activeRwTrxIds)`；若集合空，则为当前 `nextTransactionId`。
4. 设置 `lowLimitId = nextTransactionId`。
5. 设置 `lowLimitNo = min(activeReadView.lowLimitNo, committedLowWatermark)`。
6. 注册到 MVCC active view list。
7. 释放全局锁。

### 8.2 可见性判断

`MvccVisibilityService.isVisible(record, transaction, readView)`：

1. 如果 read type 是 current read，返回最新 committed 或等待锁后返回最新版本。
2. 如果 `record.dbTrxId == transaction.id`，可见。
3. 如果 isolation 是 `READ_UNCOMMITTED`，可见，但 delete-mark 需要过滤。
4. 使用 ReadView 判断 `record.dbTrxId` 是否可见。
5. 如果可见且未 delete-mark，返回当前版本。
6. 如果不可见，调用 `VersionChainBuilder` 沿 `DB_ROLL_PTR` 构造旧版本。
7. 重复可见性判断，直到找到可见版本或版本链结束。
8. 如果可见版本 delete-mark，则对普通查询表现为不存在。

### 8.3 当前读

当前读包括：

- `UPDATE`
- `DELETE`
- `INSERT` duplicate check
- `SELECT ... FOR UPDATE`
- `SELECT ... FOR SHARE`

当前读规则：

- 不使用事务旧 ReadView 返回历史版本。
- 必须对目标记录或范围获取相应锁。
- 如果遇到未提交版本，等待持有者提交/回滚，或根据 `NOWAIT/SKIP_LOCKED` 策略处理。
- 锁等待结束后重新读取最新版本并重新判断谓词。

## 9. Undo 设计

### 9.1 UndoLogManager

`UndoLogManager` 对外能力：

- `planInsert/planUpdate/planDelete(Transaction, BeforeImage)`
- `appendPlanned(Transaction, MTR, UndoWritePlan)`
- `rollbackTo(Transaction, UndoNo)`
- `cleanupOnCommit(Transaction)`
- `scanHistory(PurgeCursor)`

Undo 写入必须通过 MTR：

- undo page 获取 X latch。
- 写 undo record。
- 更新 undo page header 与 undo log header。
- 写 redo。
- 返回 `RollPointer`。

### 9.2 Undo Record 链

每次更新聚簇记录：

1. 新 undo record 的 `prevRollPointer = 同 kind undo log 当前局部 head`。
2. 记录当前列值或差异。
3. 聚簇记录的 `DB_ROLL_PTR` 更新为新 undo record；update undo 的旧版本指针另存于 old hidden columns。
4. 聚簇记录的 `DB_TRX_ID` 更新为当前事务 ID。

这样每条聚簇记录形成从最新版本向旧版本回溯的版本链。

### 9.3 History List

事务提交后：

- insert undo 可释放或缓存。
- update undo 挂入 rollback segment history list。
- history list 保持真实 commit append 的物理顺序，不按事务提交序号重排；`lastTransactionNo` 只保存最大高水位。
- purge cursor 从最老可清理位置向前推进。

history list 不等同于版本链。版本链通过记录的 `DB_ROLL_PTR` 串起来；history list 用于后台找到可 purge 的 undo log。

## 10. 行锁与 MVCC 协作

`LockManager` 第一阶段支持：

- 表级 `IS`、`IX` 意向锁。
- 记录级 `S`、`X` 锁。
- gap lock。
- next-key lock。
- insert intention lock。
- wait queue。
- deadlock detector。

隔离级别下的锁策略：

| 操作 | READ COMMITTED | REPEATABLE READ |
| --- | --- | --- |
| 普通 SELECT | consistent read，每次语句新 ReadView | consistent read，事务级 ReadView |
| SELECT FOR SHARE | 记录 S 锁 | unique lookup 记录 S 锁；范围 scan next-key S 锁 |
| SELECT FOR UPDATE | 记录 X 锁 | unique lookup 记录 X 锁；范围 scan next-key X 锁 |
| UPDATE/DELETE | 锁匹配记录；非匹配记录可释放 | 锁扫描范围，防止幻读 |
| INSERT | 插入意向锁 + 新记录 X 锁 | 插入意向锁 + 新记录 X 锁 |

边界：

- LockManager 不判断记录旧版本是否可见。
- MVCC consistent read 不等待普通行锁。
- 当前读必须通过 LockManager。
- 旧版本记录不能加锁；需要锁时必须锁当前版本或索引范围。

### 10.1 LockManager 内部结构

Lock request 状态图见 [transaction-lock-request-state.mmd](diagrams/transaction-lock-request-state.mmd)。

`LockManager` 按 table/index/page 或 hash 分片，降低热点记录锁竞争：

- `lockShard[]`：分片锁，保护本分片 lock table 和 wait queue。
- `LockTable`：`LockKey -> GrantedLockQueue + WaitingLockQueue`。
- `TransactionLockSet`：每个事务持有的锁集合，用于 commit/rollback 批量释放。
- `WaitForGraph`：`waitingTrx -> blockingTrx[]` 的等待边集合。
- `DeadlockDetector`：在新增等待边时做 bounded DFS 或周期性检测。
- `LockWaiter`：线程等待对象，保存 requested lock、deadline、interrupt/timeout 状态。

`LockKey` 类型：

- `TableLockKey(tableId)`
- `RecordLockKey(indexId, pageId, heapNo 或 logicalKey)`
- `GapLockKey(indexId, leftKey, rightKey)`
- `NextKeyLockKey(recordKey + preceding gap)`
- `InsertIntentionLockKey(gapKey)`

兼容性规则：

- 表级 `IS/IX` 与记录锁配合，用于快速拒绝表级 DDL 排他操作。
- 记录 `S` 与 `S` 兼容，`X` 与任何其它记录锁冲突。
- gap lock 主要阻止插入，不阻止对已有记录的普通读。
- insert intention lock 之间可兼容，但与覆盖同 gap 的 next-key/gap X 锁冲突。
- next-key lock 等价于 record lock 加 preceding gap lock。

等待协议：

1. 调用方构造 lock request。
2. `LockManager` 获取目标 shard lock。
3. 如果兼容，加入 granted queue，并登记到 `TransactionLockSet`。
4. 如果冲突，加入 waiting queue，记录 blocking transactions。
5. 更新 `WaitForGraph` 并触发死锁检测。
6. 释放 shard lock 后等待 `LockWaiter`。
7. 被唤醒后重新获取 shard lock，确认锁已授予、超时、被中断或被选为 victim。

调用方进入等待前不能持有 Buffer Pool 内部 list/hash 锁、物理文件锁或 undo page latch；如仍持有 page latch，必须使用 B+Tree/Record 的重新定位协议，等待后重新校验目标记录。

`LockRequestState`：

| 状态 | 持有者 | 持有资源 | 进入条件 | 退出条件 |
| --- | --- | --- | --- | --- |
| `FREE` | 无 | 无 lock request | request 尚未构造或已经清理 | 构造新请求 |
| `REQUESTED` | 调用线程 | lock request 值对象 | current read、DML 或 DDL 需要逻辑锁 | 获取 `LockManager` shard lock |
| `GRANTED` | `TransactionLockSet` | record/gap/next-key/insert intention/table intention lock | 与已授予锁兼容 | commit、rollback、语句级释放或转换 |
| `WAITING` | LockManager wait queue | waiting request、wait slot、Wait-For Graph edge | 与已授予锁冲突 | blocker 释放、timeout、deadlock victim |
| `CONVERTING` | Transaction + LockManager | 已持有锁和升级请求 | S->X 或 gap/next-key 转换 | 转换授予或进入 wait queue |
| `TIMEOUT` | LockManager cleanup | 等待请求清理权 | 等待超过 deadline | 移除 wait queue 和等待边 |
| `VICTIM` | DeadlockDetector | victim 标记、rollbackOnly | Wait-For Graph 成环且本事务被选中 | rollback 清理所有锁 |
| `RELEASED` | 无或 cleanup 线程 | 无有效锁所有权 | 正常释放、timeout cleanup 或 victim rollback | request 回到 free 状态 |

持有变化规则：

- `request`：B+Tree/Record 只构造逻辑 lock key，不拥有 lock request 生命周期。
- `grant`：授予后所有权登记到 `TransactionLockSet`，由事务结束或语句级策略释放。
- `wait`：进入等待前调用方必须释放 page latch、RecordCursor、buffer fix、物理文件锁、空间管理 latch 和 undo page latch。
- `detect`：只有 `WAITING` 事务锁边进入 Wait-For Graph；Buffer Pool latch、MTR latch、redo wait 和文件锁不进入。
- `victim`：victim 请求从 wait queue 移除，并通过 rollback 释放该事务已授予的全部逻辑锁。
- `wake`：被唤醒后重新获取 shard lock 校验状态，再返回 grant、timeout、victim 或 interrupt。

## 11. Purge 设计

数据流图见 [transaction-data-flow.mmd](diagrams/transaction-data-flow.mmd)。

`PurgeCoordinator` 后台执行：

1. 从 `ReadViewManager` 获取最老 ReadView。
2. 计算提交号 low water，并在最终摘除前复核候选 creator 已非 active 且对所有 live ReadView 可见。
3. 从 rollback segment history list 取可候选 undo log。
4. 如果物理 head 的提交序号或 creator visibility 仍被某个 ReadView 阻挡，停止推进；不能越过 head 重排。
5. 对 delete-mark 记录，调用 B+Tree 删除物理记录和对应二级索引项。
6. 清理不再需要的 update undo record。
7. 回收空 undo page 或 undo segment。
8. 更新 history list 长度和 purge cursor。

purge 安全条件：

- 不能删除任何仍可能被 active ReadView 构造旧版本需要的 undo。
- 不能物理删除当前仍被可见版本引用的聚簇记录。
- purge 使用自己的内部事务和 MTR，不占用用户事务上下文。
- purge 可落后，但不能越界。

## 12. Recovery 设计

启动恢复：

1. redo recovery 先恢复物理页，包括 undo page、聚簇索引页、事务系统页。
2. `TransactionRecoveryService` 扫描事务系统页和 undo log header。
3. 从 page3 history base 沿 UPDATE first-page 双向链按物理顺序重建 history，并与 occupied slots、
   undo header 状态及 transaction counter 证据交叉校验后，再发布 rollback segment 内存态。
4. 对 recovered active transaction 执行 rollback。
5. 对 prepared transaction 保留 `PREPARED`，等待上层事务协调器决定。
6. 初始化 ReadViewManager，启动时没有用户 ReadView。
7. 启动 purge，从恢复出的 history list 继续清理。

恢复原则：

- redo 负责把页恢复到 crash 前物理一致状态。
- undo 负责回滚未提交数据库事务。
- purge 只在 recovery 完成且 ReadView 边界安全后运行。

## 13. API 设计

### 13.1 TransactionManager

对外门面：

- `begin(TransactionOptions)`
- `current()`
- `commit(Transaction)`
- `rollback(Transaction)`
- `setSavepoint(String)`
- `rollbackToSavepoint(String)`
- `markRollbackOnly(Transaction, Throwable)`
- `withTransaction(TransactionOptions, TransactionCallback)`

返回值是 `TransactionContext`，不暴露内部活跃事务表或锁队列。

### 13.2 MvccVisibilityService

记录访问接口：

- `visibleVersion(RecordRef, Transaction, ReadType)`
- `isCurrentVisible(RecordHeader, Transaction, ReadView)`
- `buildPreviousVersion(RecordVersion, RollPointer, ReadView)`
- `openReadViewIfNeeded(Transaction, ReadType)`

### 13.3 UndoLogManager

写路径接口：

- `planInsert/planUpdate/planDelete(Transaction, RecordImage)`
- `appendPlanned(Transaction, MTR, UndoWritePlan)`
- `rollback(Transaction)`
- `rollbackTo(Transaction, TransactionSavepoint)`

### 13.4 LockManager

当前读和写路径接口：

- `lockTable(Transaction, TableId, TableLockMode)`
- `lockRecord(Transaction, IndexRecordRef, RecordLockMode, LockDuration)`
- `lockGap(Transaction, IndexGapRef, GapLockMode)`
- `lockNextKey(Transaction, IndexRecordRef, NextKeyLockMode)`
- `releaseStatementLocks(Transaction)`
- `releaseTransactionLocks(Transaction)`

## 14. 设计模式使用清单

- Facade：`TransactionManager` 聚合事务生命周期、ReadView、undo、lock、purge。
- State：`TransactionStateMachine` 管理状态转换。
- Snapshot：`ReadView` 保存一致性读快照。
- Repository：`ActiveTransactionTable`、`RollbackSegmentRepository`、`UndoLogRepository`。
- Strategy：`IsolationLevelStrategy`、`VisibilityStrategy`、`LockingPolicy`。
- Command：`UndoRecord` 支持 rollback 和旧版本构造。
- Chain of Responsibility：`VersionChainBuilder` 沿 undo 链逐步构造可见版本。
- Mediator：`LockManager` 统一协调锁兼容、等待、唤醒。
- Observer：commit/rollback 事件通知 ReadView、undo cleanup、lock release、purge。
- Background Worker：`PurgeCoordinator` 异步清理历史版本。
- Template Method：`TransactionalPageOperationTemplate` 固定 `lock -> undo -> modify -> redo -> MTR commit` 写路径。

## 15. 高内聚、低耦合约束

强制规则：

- 数据库事务不能直接操作 `BufferFrame` 或 `PageStore`。
- MTR 不能决定数据库事务提交/回滚。
- ReadView 只保存事务 ID 边界，不保存记录副本。
- undo record 是旧版本来源，不能把旧版本常驻在记录页中。
- consistent read 不能等待普通行锁。
- current read 必须走 LockManager，不能只靠 MVCC。
- update/delete 必须先写 undo，再修改聚簇记录隐藏列和 payload。
- `DB_ROLL_PTR` 更新必须与记录修改处于同一个 MTR。
- update undo 进入 history list 后，只能由 purge 回收。
- purge 必须受最老 ReadView 限制。
- 行锁释放由事务结束或语句级策略控制，不由 Buffer Pool 或 B+Tree 自行释放。
- temporary undo 可 no-redo，但普通 undo 必须 redo-logged。

推荐模块边界：

`record/btree -> trx.mvcc + trx.lock + trx.undo`  
`trx.undo -> mtr + buf + fsp + redo`  
`trx.purge -> readview + undo + btree`  
`trx.recovery -> redo + undo + core`

## 16. 典型数据流

### 16.1 一致性读

1. 查询层请求普通 SELECT。
2. `TransactionManager.current()` 获取事务上下文。
3. `ReadViewManager` 按隔离级别创建或复用 ReadView。
4. B+Tree 定位聚簇记录当前版本。
5. `MvccVisibilityService` 读取 `DB_TRX_ID` 和 `deleted_flag`。
6. 如果当前版本对 ReadView 可见且未 delete-mark，返回。
7. 如果不可见，沿 `DB_ROLL_PTR` 读取 undo record。
8. `VersionChainBuilder` 构造上一版本。
9. 重复可见性判断。
10. 找到可见版本或确认不存在。

### 16.2 Update

1. 开启或获取当前事务。
2. B+Tree 通过当前读定位候选记录。
3. LockManager 获取记录 X 锁或 next-key X 锁。
4. 重新读取最新版本并判断谓词。
5. UndoLogManager 写 update undo。
6. 在同一 MTR 中修改记录列值、`DB_TRX_ID`、`DB_ROLL_PTR`。
7. MTR commit 写 redo 并标脏页。
8. 数据库事务继续 ACTIVE，直到用户 commit/rollback。

### 16.3 Delete

1. 当前读定位记录并获取 X/next-key lock。
2. 写 update undo，保存删除前镜像。
3. 设置 `deleted_flag = true`，更新隐藏列。
4. MTR commit。
5. 事务提交前其它事务按 ReadView 判断是否能看见旧版本。
6. 事务提交后，purge 在安全时物理删除记录。

### 16.4 Insert

1. 为事务分配 `TransactionId`。
2. 通过 LockManager 获取 insert intention lock。
3. 唯一键检查使用当前读，必要时等待冲突事务。
4. 写 insert undo。
5. 插入聚簇记录，写 `DB_TRX_ID` 和 `DB_ROLL_PTR`。
6. 插入二级索引项。
7. MTR commit。
8. rollback 时通过 insert undo 删除该新记录；commit 后 insert undo 可释放。

### 16.5 Rollback

1. 用户调用 rollback 或事务被死锁检测选为 victim。
2. TransactionState 进入 `ROLLING_BACK`。
3. UndoLogManager 从 INSERT/UPDATE 两个局部头中选择 undoNo 较大者反向扫描。
4. 对 insert undo 删除未提交插入。
5. 对 update undo 恢复旧值和旧隐藏列。
6. 每次物理修改用独立 MTR 写 redo。
7. 释放锁，关闭 ReadView。
8. 状态变为 `ROLLED_BACK`。

### 16.6 Purge

1. PurgeCoordinator 读取最老 ReadView。
2. 从 history list 取候选 update undo。
3. 判断该 undo 是否仍可能用于旧版本构造。
4. 对安全的 delete-mark 记录执行物理删除。
5. 回收 undo record/page/segment。
6. 推进 purge cursor 和 history list length。

## 17. 并发与锁顺序

事务模块内部短锁顺序：

1. `TransactionSystem` 全局短锁。
2. `Transaction` 对象锁。
3. `LockManager` 分片锁。

这个顺序只描述事务模块自己的短临界区。B+Tree latch、Buffer Pool page latch、undo page latch 和物理文件锁不属于事务锁所有权，不允许在可能阻塞的事务锁等待期间继续持有。

跨模块调用释放协议：

1. B+Tree/Record 定位候选记录时可短暂持有 page latch 和 `RecordCursor`。
2. 构造 `RecordLockKey`、`GapLockKey` 或 `NextKeyLockKey` 后，先释放 page latch、buffer fix 和 cursor。
3. 进入 `LockManager` wait queue 后，只持有事务 wait slot 和 Wait-For Graph edge。
4. 锁授予后重新进入 B+Tree 定位目标记录，再获取 page latch 或 undo page latch。
5. 写 undo 与写数据记录处于同一 MTR，page latch 顺序由 B+Tree/Buffer Pool 规则控制。

规则：

- 创建 ReadView 时只复制事务 ID，不等待页 latch。
- 获取行锁时不能持有 undo page latch。
- 写 undo 和写数据记录在 MTR 内完成，页 latch 顺序由 B+Tree/Buffer Pool 规则控制。
- 死锁检测只检查事务等待图，不进入 Buffer Pool。
- purge 批量处理时每批限制 undo 数和 page 数，避免长期占用 page latch。

死锁处理：

- `WaitForGraph` 记录 `waitingTrx -> blockingTrx`。
- 新增等待边时立即检测当前等待链；后台 detector 可周期性兜底清理长等待。
- 检测范围只包含数据库事务锁，不包含 Buffer Pool page latch、物理文件锁、MTR latch 或 Java monitor。
- 检测到环时选择代价较低事务作为 victim。
- victim 设置 `rollbackOnly` 并唤醒。
- 锁等待超时抛出明确异常，调用方决定重试。

victim 选择成本：

- 已修改行数和 undo 数越少，成本越低。
- 持有锁数量越少，成本越低。
- 事务年龄越短，成本越低。
- 已进入 `PREPARED` 或正在提交的事务不能作为普通 victim。
- 系统事务、purge 内部事务和 recovery 事务只有在明确允许时才能被中断。

victim 处理流程：

1. 将 victim 标记为 `rollbackOnly`。
2. 从 wait queue 移除 victim 的等待请求。
3. 唤醒 victim 线程并抛出 `DeadlockDetectedException`。
4. victim 执行 statement rollback 或完整事务 rollback。
5. rollback 释放锁后唤醒后续等待者。
6. 如果 victim 线程不可用，由事务清理线程接管 rollback。

## 18. 异常处理

异常类型：

- `TransactionStateException`
- `DeadlockDetectedException`
- `LockWaitTimeoutException`
- `SnapshotTooOldException`
- `UndoLogFullException`
- `RollbackFailedException`
- `PurgeLagExceededException`
- `TransactionRollbackOnlyException`
- `SerializationFailureException`

错误策略：

- 死锁 victim 必须完整 rollback。
- 单语句失败优先 rollback 到 statement savepoint。
- undo 空间不足时，当前写操作失败，事务标记 rollback-only。
- 构造旧版本时 undo 缺失属于严重一致性错误，除非明确启用 force recovery。
- purge 失败不影响前台已提交语义，但必须停止推进并报警。

## 19. 测试设计

虽然本次不写代码，后续实现应覆盖：

- 事务状态机测试：begin、commit、rollback、rollback-only、非法状态转换。
- 事务 ID 测试：只读事务延迟分配，读写事务递增分配。
- ReadView 测试：`upLimitId`、`lowLimitId`、active ids、creator trx 可见性。
- 隔离级别测试：`READ COMMITTED` 每语句快照，`REPEATABLE READ` 事务级快照。
- MVCC 可见性测试：当前版本可见、不可见、自己写可见、delete-mark 不可见。
- Undo 链测试：多次 update 后构造多个旧版本。
- Insert undo 测试：rollback 删除未提交插入，commit 后可清理。
- Update undo 测试：rollback 恢复旧值，commit 后进入 history list。
- Delete 测试：delete-mark、旧 ReadView 可见旧版本、purge 后不可见。
- Lock 测试：S/X 兼容矩阵、当前读等待、NOWAIT/SKIP_LOCKED 扩展点。
- Gap/next-key 测试：RR 范围更新阻止幻读插入。
- 死锁测试：等待图成环，victim rollback。
- Purge 测试：最老 ReadView 阻塞 purge，ReadView 关闭后推进。
- Recovery 测试：active transaction crash 后 rollback，history list 恢复后 purge。
- property-based 测试：随机事务读写提交回滚后，所有可见性和唯一键约束保持一致。

## 20. 后续实现顺序

推荐分阶段实现：

1. `TransactionId`、`RollPointer`、`IsolationLevel`、`TransactionState` 等领域对象。
2. `Transaction`、`TransactionManager`、`TransactionStateMachine`。
3. `ActiveTransactionTable` 和读写事务 ID 分配。
4. `ReadView` 与 `ReadViewManager`。
5. 记录隐藏列访问接口：`DB_TRX_ID`、`DB_ROLL_PTR`、delete flag。
6. `UndoLogManager` 最小 insert/update undo 写入。
7. `MvccVisibilityService` 和旧版本构造。
8. commit/rollback 与 undo cleanup。
9. LockManager 最小 S/X 记录锁。
10. 当前读与写路径集成。
11. history list 与 PurgeCoordinator。
12. gap/next-key lock 和死锁检测。
13. recovery 与故障注入测试。

## 21. 十五轮自检记录

| 轮次 | 检查主题 | 检查结果 |
| --- | --- | --- |
| 1 | 不写代码边界 | 只新增 Markdown 和 Mermaid 设计文件，没有生成 Java 源码 |
| 2 | 与磁盘管理衔接 | 已复用 undo tablespace、MTR、PageCursor、redo-before-data 设计 |
| 3 | 与 Buffer Pool 衔接 | 已明确事务不直接操作 BufferFrame/PageStore，通过 BufferPool 与 MTR 访问页 |
| 4 | MySQL 8.0 参考 | 已覆盖 InnoDB transaction model、isolation、MVCC、ReadView、undo、locking reads、purge、源码 `read0read/read0types/trx0undo/trx0purge/lock0lock` |
| 5 | 高内聚 | 事务生命周期、ReadView、MVCC、undo、lock、purge、recovery 独立成包 |
| 6 | 低耦合 | 已禁止事务核心依赖 SQL parser、Buffer Pool 反向依赖事务语义 |
| 7 | 长事务与 MTR 区分 | 已明确数据库事务跨语句，MTR 只处理物理短事务 |
| 8 | ReadView 正确性 | 已定义 `upLimitId`、`lowLimitId`、active trx ids、creator trx 可见性 |
| 9 | 隔离级别 | 已覆盖 RC/RR 的快照差异，并保留 RU/SERIALIZABLE 策略 |
| 10 | Undo 语义 | 已区分 insert undo 与 update undo，以及 rollback/history list/purge 用途 |
| 11 | 版本链 | 已定义 `DB_TRX_ID + DB_ROLL_PTR + undo record` 构造旧版本 |
| 12 | 当前读与锁 | 已区分 consistent read 与 current read，当前读必须走 LockManager |
| 13 | Purge 安全 | 已通过最老 ReadView 限制 purge，不清理仍可能需要的 undo |
| 14 | Recovery | 已说明 redo 恢复物理页、undo 回滚未提交事务、purge 后台继续 |
| 15 | 图与文档一致性 | 四个 Mermaid 图与正文术语一致，并已通过 `npx @mermaid-js/mermaid-cli` 实际渲染 |

## 22. 参考链接

- MySQL 8.0 Reference Manual - InnoDB Transaction Model: https://dev.mysql.com/doc/refman/8.0/en/innodb-transaction-model.html
- MySQL 8.0 Reference Manual - Transaction Isolation Levels: https://dev.mysql.com/doc/refman/8.0/en/innodb-transaction-isolation-levels.html
- MySQL 8.0 Reference Manual - Consistent Nonlocking Reads: https://dev.mysql.com/doc/refman/8.0/en/innodb-consistent-read.html
- MySQL 8.0 Reference Manual - InnoDB Multi-Versioning: https://dev.mysql.com/doc/refman/8.0/en/innodb-multi-versioning.html
- MySQL 8.0 Reference Manual - Undo Logs: https://dev.mysql.com/doc/refman/8.0/en/innodb-undo-logs.html
- MySQL 8.0 Reference Manual - InnoDB Locking: https://dev.mysql.com/doc/refman/8.0/en/innodb-locking.html
- MySQL 8.0 Reference Manual - Locking Reads: https://dev.mysql.com/doc/refman/8.0/en/innodb-locking-reads.html
- MySQL 8.0.46 Source Documentation - `MVCC`: https://dev.mysql.com/doc/dev/mysql-server/8.0.46/classMVCC.html
- MySQL 8.0.46 Source Documentation - `ReadView`: https://dev.mysql.com/doc/dev/mysql-server/8.0.46/classReadView.html
- MySQL 8.0.46 Source Documentation - `trx0trx.cc`: https://dev.mysql.com/doc/dev/mysql-server/8.0.46/trx0trx_8cc.html
- MySQL 8.0.46 Source Documentation - `trx0undo.h`: https://dev.mysql.com/doc/dev/mysql-server/8.0.46/trx0undo_8h.html
- MySQL 8.0.46 Source Documentation - `trx0purge.h`: https://dev.mysql.com/doc/dev/mysql-server/8.0.46/trx0purge_8h.html
- MySQL 8.0.46 Source Documentation - `lock0lock.h`: https://dev.mysql.com/doc/dev/mysql-server/8.0.46/lock0lock_8h.html
