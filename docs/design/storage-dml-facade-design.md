# Storage DML Facade Design

版本：2026-07-04

## 1. 背景

当前 storage 内部已经具备单聚簇索引写路径所需的大部分能力：`StorageEngine` 生产持有 `TransactionManager`、`UndoLogManager`、`SplitCapableBTreeIndexService`、`BTreeCurrentReadService`、`RollbackService`、`LockManager`、durable `RedoLogManager`、后台 redo flusher、page cleaner、purge driver 和 crash recovery 编排。`current-implementation-map.md` 仍明确记录缺口：普通 SQL/session DML facade 尚未接线，`assignWriteId -> beforeX -> clustered write`、commit durability、`UndoLogManager.onCommit`、`RollbackService.rollback`、`LockManager.releaseAll` 仍主要由测试手工驱动。

本设计对应 backlog 2.1：建立 Storage 内单表/单聚簇索引 DML facade。它不是完整 SQL executor，也不接 Data Dictionary 多表/多索引；它先把已经落地的 storage 内核能力组合成一个稳定、可测试、可由未来 executor 调用的写入入口。

## 2. 已核对设计文档

本设计按要求核对 `docs/design` 下所有 `innodb-*` 与 `mysql-*` 文档，并只采纳与 2.1 直接相关的约束。

| 文档 | 对本设计的约束 |
| --- | --- |
| `innodb-storage-engine-overview.md` | StorageEngine 是 SQL/session 下方入口；DML 需要 begin/commit/rollback、lookup、insert、update、delete；current read 需 LockManager；crash recovery 顺序为 redo -> undo rollback -> purge resume。 |
| `innodb-disk-manager-design.md` | DML 不直接操作裸文件；页、空间分配必须通过 MTR、Buffer Pool、Disk Manager；MTR 不是数据库事务 rollback；WAL 是 data page flush 前置约束。 |
| `innodb-buffer-pool-design.md` | facade 不持有 Buffer Pool 内部锁进入 LockManager/Undo/B+Tree；page latch/fix 由 MTR/guard 管；dirty 发布只通过 MTR commit。 |
| `innodb-record-design.md` | Record 只提供隐藏列、delete-mark、当前记录镜像；DML 等待行锁前必须释放 RecordCursor/page latch/buffer fix；事务锁由事务结束释放。 |
| `innodb-btree-design.md` | INSERT 做 current read 唯一检查与 insert intention；UPDATE/DELETE 用 current read 锁；等待后重新定位；B+Tree 不决定 MVCC/deadlock victim。 |
| `innodb-transaction-mvcc-design.md` | 事务生命周期、ReadView、undo、rollback、purge、row lock 必须集中在 transaction/storage facade；写入前分配 DB_TRX_ID；commit 分配 TransactionNo。 |
| `innodb-undo-log-purge-design.md` | beforeInsert/beforeUpdate/beforeDelete 必须在记录修改前写 undo；commit 对 update/delete undo 入 history；rollback 按 undo 链反向应用。 |
| `innodb-redo-log-design.md` | DML 不让 redo 理解 SQL/MVCC；MTR 产生物理 redo；commit 层消费 `DurabilityPolicy` 等待 write/flush/background。 |
| `innodb-flush-checkpoint-doublewrite-design.md` | flush/checkpoint 不执行事务 commit/rollback；DML commit 只能等待 redo durable，不能绕过 FlushService/WAL gate。 |
| `innodb-crash-recovery-design.md` | 开放用户流量前必须完成 redo replay、active rollback、purge resume；普通 DML 入口应尊重 `RecoveryTrafficGate` 状态。 |
| `mysql-sql-executor-storage-api-design.md` | Executor 只通过 Storage API 表达 insert/update/delete/read intent；不能直接碰 page/record/redo/LockManager 内部。 |
| `mysql-session-connection-protocol-design.md` | autocommit 与显式事务由 session 管，storage facade 应允许调用方显式传入 `Transaction`，不使用 ThreadLocal。 |
| `mysql-data-dictionary-ddl-design.md` | 本切片不建立 DD；调用方传入 `BTreeIndex`/schema 快照；多索引、DDL recovery、metadata version 留后续。 |
| `mysql-parser-binder-design.md` | 本切片不解析 SQL；输入必须已经是归一化 row/key/record。 |
| `mysql-query-optimizer-design.md` | 本切片不做访问路径选择；只提供按主键/聚簇索引的低层 DML 能力。 |
| `mysql-advanced-executor-operators-design.md` | 高级算子只消费 Storage API，不接触 B+Tree/page latch；本 facade 为后续算子保留稳定边界。 |
| `mysql-prepared-statement-plan-cache-design.md` | 本切片不处理 plan cache；API 不能保存过期 DD/plan 状态，只消费调用时传入的 index/schema 快照。 |
| `mysql-statistics-analyze-design.md` | 本切片不更新统计信息；DML 影响统计/ANALYZE invalidation 留 DD/executor 阶段。 |
| `mysql-lock-observability-deadlock-design.md` | row-lock 事件和快照由 LockManager/lockobs 已接线；DML facade 要在 commit/rollback/异常清理时释放事务锁，保持观测状态收敛。 |

## 3. 目标

1. 在 `storage.api.dml` 下新增单表/单聚簇索引 DML facade，作为未来 SQL executor 的稳定 storage 写入口。
2. 覆盖 INSERT、UPDATE、DELETE、COMMIT、ROLLBACK 的最小完整生产链路。
3. INSERT 链路串起 `assignWriteId -> unique current-read / insert intention -> beforeInsert -> insertClustered -> MTR commit`。
4. UPDATE 链路串起 `current-read X lock -> beforeUpdate(old image) -> replaceClustered(new hidden) -> MTR commit`。
5. DELETE 链路串起 `current-read X lock -> beforeDelete(old image) -> setClusteredDeleteMark -> MTR commit`。
6. COMMIT 链路串起 `TransactionManager.prepareCommit -> UndoLogManager.onCommit -> TransactionManager.commit -> DurabilityPolicy.awaitCommitDurable -> LockManager.releaseAll`。
7. ROLLBACK 链路串起 `RollbackService.rollback -> LockManager.releaseAll`，确保 undo 反向应用后事务锁释放。
8. 所有可能等待 row lock 的路径必须使用现有 `BTreeCurrentReadService`，等待期间不持 page latch、record cursor、buffer fix 或 undo page latch。
9. 更新 `current-implementation-map.md` 中 Transaction、B+Tree、Engine/Storage API 的真实生产接线状态。

## 4. 非目标

1. 不实现 SQL parser、binder、optimizer、executor 或 session/autocommit 管理。
2. 不引入 Data Dictionary，不做表/索引名解析、metadata lock、metadata version 校验。
3. 不维护二级索引，不做回表 MVCC，不做二级索引 purge。
4. 不支持改聚簇主键；`replaceClustered` 的 `REQUIRES_REINSERT` 继续作为不支持结构抛出。
5. 不实现 statement rollback、savepoint rollback、XA、PREPARED、SERIALIZABLE、READ UNCOMMITTED。
6. 不改 redo 文件格式，不引入逻辑 redo handler。
7. 不改变 recovery 当前依赖显式配置表空间/单聚簇索引的限制。
8. 不把 implementation plan 写入长期计划目录以外的隐式上下文；本次按用户要求显式写完整 plan 文档。

## 5. 推荐方案与备选方案

### 5.1 推荐方案：`storage.api.dml` facade

新增 `cn.zhangyis.db.storage.api.dml.ClusteredDmlService`，由 `StorageEngine` 构造并通过 `dmlService()` 暴露。调用方显式传入 `Transaction`、`BTreeIndex`、`SearchKey`、`LogicalRecord`、锁等待 timeout 和 durability policy。facade 内部只编排 storage 组件，不反向依赖 SQL/session/DD。

优点：符合既有依赖方向 `session -> sql -> dd -> storage.api -> storage internals`；未来 executor 可直接复用；不会把 DML 编排塞进 `StorageEngine` 巨类；测试可聚焦 public facade。

代价：需要新增一组 request/result 值对象。

### 5.2 备选方案 A：直接在 `StorageEngine` 增加 DML 方法

优点：改动少，能快速串线。

缺点：`StorageEngine` 已是复杂组合根，继续放业务 DML 编排会膨胀职责；未来 SQL storage API 仍需再抽一层；不利于测试替换。

### 5.3 备选方案 B：直接实现 SQL executor storage API

优点：更接近最终目标。

缺点：会同时拉入 DD、executor、session/autocommit、MDL 和计划对象，单次切片过大；当前用户已选择 Storage 内完整单表 DML facade。

## 6. 包与文件边界

新增包：`cn.zhangyis.db.storage.api.dml`。

| 类型 | 职责 |
| --- | --- |
| `ClusteredDmlService` | DML facade，编排事务、undo、current-read、B+Tree、MTR、redo durability、lock release。 |
| `ClusteredInsertCommand` | INSERT 输入：事务、聚簇索引、待插入完整逻辑记录、主键 search key、table/index id、lock wait/durability timeout。 |
| `ClusteredUpdateCommand` | UPDATE 输入：事务、聚簇索引、主键 search key、新逻辑记录、table/index id、timeout。 |
| `ClusteredDeleteCommand` | DELETE 输入：事务、聚簇索引、主键 search key、table/index id、timeout。 |
| `DmlCommitCommand` | COMMIT 输入：事务、durability policy、durability timeout。 |
| `DmlRollbackCommand` | ROLLBACK 输入：事务、聚簇索引。 |
| `DmlWriteResult` | INSERT/UPDATE/DELETE 输出：是否改动、提交的 MTR end LSN、当前事务 id、影响行数。 |
| `DmlCommitResult` | COMMIT 输出：transactionNo、是否等待到 durable、释放锁数量。 |
| `DmlRollbackResult` | ROLLBACK 输出：rollback applied count、释放锁数量。 |
| `DmlOperationException` | DML 编排领域异常，包装 durability timeout、relocation、unsupported update 等可恢复运行时错误。 |
| `DmlDuplicateKeyException` | INSERT unique/current-read 发现物理重复 key 时抛出的领域异常。 |

修改文件：

| 文件 | 变更 |
| --- | --- |
| `StorageEngine.java` | 构造 `ClusteredDmlService` 并暴露 `dmlService()`；fresh/existing open 后可用。 |
| `EngineConfig.java` | 如需默认 commit durability，可新增 `DurabilityPolicy defaultDurabilityPolicy`；推荐第一阶段不改 config，commit command 显式传入 policy。 |
| `current-implementation-map.md` | 更新 2.1 DML facade 当前流、包状态、已知缺口。 |

## 7. API 设计

### 7.1 服务构造

`ClusteredDmlService` 构造参数：

| 参数 | 来源 | 用途 |
| --- | --- | --- |
| `TransactionManager transactionManager` | `StorageEngine` | begin/assignWriteId/commit/rollback state machine。 |
| `UndoLogManager undoLogManager` | `StorageEngine` | beforeInsert/beforeUpdate/beforeDelete/onCommit。 |
| `MiniTransactionManager mtrManager` | `StorageEngine` | 每条聚簇写操作一个短 MTR。 |
| `SplitCapableBTreeIndexService btree` | `StorageEngine` | insert/replace/delete-mark。 |
| `BTreeCurrentReadService currentRead` | `StorageEngine` | unique check、UPDATE/DELETE point X lock。 |
| `RollbackService rollbackService` | `StorageEngine` | full transaction rollback。 |
| `LockManager lockManager` | `StorageEngine` | commit/rollback/异常清理 releaseAll。 |
| `RedoLogManager redo` | `StorageEngine` | commit durability wait。 |
| `RecoveryTrafficGate recoveryGate` | `StorageEngine` | DML 入口拒绝 recovery 中/failed 状态。 |

### 7.2 公开方法

```java
public final class ClusteredDmlService {
    public DmlWriteResult insert(ClusteredInsertCommand command);
    public DmlWriteResult update(ClusteredUpdateCommand command);
    public DmlWriteResult delete(ClusteredDeleteCommand command);
    public DmlCommitResult commit(DmlCommitCommand command);
    public DmlRollbackResult rollback(DmlRollbackCommand command);
}
```

### 7.3 命令值对象

命令对象必须使用 Java `record`，构造器中执行 null/范围校验，非法输入抛 `DatabaseValidationException` 或 `TransactionStateException`，不抛裸 `IllegalArgumentException`。

`ClusteredInsertCommand` 字段：

| 字段 | 语义 |
| --- | --- |
| `Transaction transaction` | 调用方显式持有的数据库事务；必须 ACTIVE、非 read-only。 |
| `BTreeIndex index` | 聚簇索引元数据快照；必须 `clustered()==true`。 |
| `SearchKey key` | 主键 search key；必须与 record 的聚簇 key 一致，facade 第一阶段不重新推导。 |
| `LogicalRecord record` | 不带隐藏列或隐藏列可被忽略的用户行；facade 会盖 `DB_TRX_ID/DB_ROLL_PTR`。 |
| `long tableId` | undo record table id。 |
| `Duration lockWaitTimeout` | unique check / insert intention 等待上限。 |

`ClusteredUpdateCommand` 字段：

| 字段 | 语义 |
| --- | --- |
| `Transaction transaction` | ACTIVE 读写事务。 |
| `BTreeIndex index` | 聚簇索引。 |
| `SearchKey key` | 被更新记录主键。 |
| `LogicalRecord newRecord` | 新完整行，必须不改变聚簇 key；facade 会盖新隐藏列。 |
| `long tableId` | undo table id。 |
| `Duration lockWaitTimeout` | current read X lock 等待上限。 |

`ClusteredDeleteCommand` 字段同 update，但不需要 `newRecord`。

`DmlCommitCommand` 字段：`Transaction transaction`、`DurabilityPolicy durabilityPolicy`、`Duration durabilityTimeout`。

`DmlRollbackCommand` 字段：`Transaction transaction`、`BTreeIndex clusteredIndex`。

## 8. 数据流

### 8.1 INSERT

1. `ClusteredDmlService.insert` 校验 engine gate OPEN、事务 ACTIVE、index clustered、record 未 delete-mark。
2. 调 `transactionManager.assignWriteId(txn)`，得到非 NONE `TransactionId`。
3. 调 `currentRead.checkUniqueForInsert(index,key,request)`，物理 duplicate 命中则返回 duplicate/no-op 或抛 duplicate 异常。推荐第一阶段抛 `DmlOperationException`，避免静默覆盖。
4. 该 unique check 会在短 MTR 定位后释放 page latch，再进入 LockManager 等待，成功锁由事务持有直到 commit/rollback。
5. 开启业务写 MTR。
6. 调 `undoLogManager.beforeInsert(txn,mtr,tableId,index.indexId(),key.parts,index.keyDef(),index.schema())` 写 insert undo，返回 insert `RollPointer`。
7. 调 `btree.insertClustered(mtr,index,record,txnId,rollPointer)`，B+Tree 盖隐藏列并插入。
8. `mtrManager.commit(mtr)`，返回 end LSN，释放 page latch/buffer fix/tablespace lease。
9. 返回 `DmlWriteResult(affectedRows=1,endLsn,txnId)`。

异常边界：若 step 6 成功但 step 7 前后失败，MTR rollback 只释放 latch，不做内容 undo；该事务后续 rollback 依靠 orphan undo 幂等清理。facade 不在单语句失败时自动 full rollback，除非调用方显式调用 `rollback`。这是当前 MTR 简化与既有 RollbackService 语义的一致选择。

### 8.2 UPDATE

1. 校验 gate、事务、index、key/newRecord。
2. `assignWriteId(txn)`。
3. 调 `currentRead.lockPoint(index,key,request,FOR_UPDATE)` 获取 REC_X 或 RR gap X；等待期间不持 page latch。
4. 若未命中，返回 `affectedRows=0`。
5. 从 `BTreeLookupResult.record()` 取得旧完整 `LogicalRecord`、旧 `HiddenColumns` 和旧 delete flag；delete-marked 记录按当前读不命中。
6. 开启业务写 MTR。
7. 调 `undoLogManager.beforeUpdate` 写全量旧 image 和旧隐藏列，得到 update roll pointer。
8. 构造带新隐藏列 `(txnId, updateRollPointer)` 的 `LogicalRecord`。
9. 调 `btree.replaceClustered(mtr,index,key,stampedNewRecord, oldHidden.dbTrxId(), oldHidden.dbRollPtr())`。
10. 提交 MTR，返回 affected rows。若 `replaceClustered` 返回 `replaced=false`，说明 current-read 锁后重定位仍发生版本变化或所有权不匹配，facade 第一阶段返回 0 并保留 undo orphan，由事务 rollback 幂等清理；后续可升级为 retry/statement rollback。

### 8.3 DELETE

1. 校验 gate、事务、index、key。
2. `assignWriteId(txn)`。
3. 调 `currentRead.lockPoint(index,key,request,FOR_UPDATE)` 获取必要 X lock。
4. 未命中返回 `affectedRows=0`。
5. 从当前记录取旧完整 row 和旧隐藏列。
6. 开启业务写 MTR。
7. 调 `undoLogManager.beforeDelete` 写 delete-mark undo。
8. 构造新隐藏列 `(txnId, deleteRollPointer)`。
9. 调 `btree.setClusteredDeleteMark(mtr,index,key,true,newHidden,oldHidden.dbTrxId(),oldHidden.dbRollPtr())`。
10. 提交 MTR，返回 affected rows。物理删除由 purge driver 后续按 history/purge low water 执行。

### 8.4 COMMIT

推荐顺序：

1. 校验 gate、事务 ACTIVE。
2. 调 `transactionManager.prepareCommit(txn)`：读写事务预留 `TransactionNo`，但仍保持 ACTIVE、仍在 active table、仍持有 row locks。
3. 调 `undoLogManager.onCommit(txn)`：标 undo first page COMMITTED/COMMIT_NO；纯 insert 清 page3 slot 并入 insert reclaim；update/delete undo 入 history list。
4. 调 `transactionManager.commit(txn)`：ACTIVE -> COMMITTING -> COMMITTED；读写事务移出 active table；释放 ReadView。
5. 取 redo `currentLsn()` 作为 commit 需要覆盖的 LSN。说明：当前没有独立 transaction commit redo record，DML MTR 已经在 step 2 前提交并分配 LSN；`onCommit` 自身也可能提交短 MTR 写 undo state。用 onCommit 后的 `redo.currentLsn()` 覆盖本事务所有 MTR。
6. 调 `durabilityPolicy.awaitCommitDurable(redo, commitLsn, timeout)`。
7. 若等待失败，抛 `DmlOperationException`；事务已经进入 COMMITTED，调用方必须将该异常视为“提交结果不确定/持久性等待失败”，不能再 rollback。
8. 调 `lockManager.releaseAll(txn.transactionId())`，释放 row locks。
9. 返回 `DmlCommitResult(transactionNo,durable, releasedLockCount)`。

为什么先 prepare/onCommit 再真正 commit：`UndoLogManager.onCommit` 对 update/delete undo 依赖 `TransactionNo`，且会追加 undo state redo；但在 undo commit marker 持久化前不能把事务移出 active table 或释放 row locks，否则 `onCommit` 失败后崩溃恢复会把已对外提交的事务按 ACTIVE undo 回滚。durability 等待必须发生在 onCommit 和状态 commit 之后，覆盖所有本事务 MTR redo。

### 8.5 ROLLBACK

1. 校验 gate、事务 ACTIVE、clustered index。
2. 调 `rollbackService.rollback(txn,index)`，它会进入 ROLLING_BACK，按 undo 链反向逐条 MTR 应用 INSERT/UPDATE/DELETE_MARK undo，释放 slot，再 finishRollback。
3. 在 `finally` 中，如果事务已有非 NONE id，调用 `lockManager.releaseAll(txn.transactionId())`。
4. 返回 `DmlRollbackResult(appliedUndoRecords,releasedLockCount)`。

失败边界：如果 rollback 单条 undo 失败，`RollbackService` 保持事务在 ROLLING_BACK，可重试。facade 的 finally 释放锁会让事务不再阻塞其他事务，但 active table 中仍保留该事务直到 rollback 成功；这是前台 rollback 失败的可恢复异常，需要上层关闭 session 或重试 rollback。若后续实现希望 ROLLING_BACK 期间保留锁，应把 releaseAll 移到成功后；第一阶段选择释放锁以避免死锁残留，并在文档/测试明确。

## 9. 并发与锁顺序

1. DML facade 不直接打开 page，不持有 `RecordCursor`。
2. 所有 row-lock 等待都通过 `BTreeCurrentReadService`，其协议为短 MTR 定位 -> 释放 latch/fix -> LockManager 等待 -> 短 MTR 重定位。
3. 写 undo 与写聚簇记录在同一业务 MTR 内完成，但不在等待 row lock 时持有 undo page latch。
4. 事务锁归 `TransactionId`，commit/rollback 使用 `LockManager.releaseAll` 释放。
5. DML facade 不持有 Buffer Pool list/hash/frame 锁。
6. DML facade 不持有 FileChannel/PageStore/flush 物理锁。
7. commit durability wait 只等待 redo write/flush，不等待 data page flush。
8. rollback recovery 路径不经本 facade，仍由 recovery participant 调 `rollbackRecovered`。

## 10. 恢复语义

1. 每条 DML 的物理页修改由 MTR redo 保护。
2. beforeX undo record 与聚簇页修改在同一 MTR 内提交，崩溃后 redo 先恢复 undo page 和 index page。
3. 未提交事务崩溃后由正式 UNDO_ROLLBACK stage 扫 page3/undo first page 回滚。
4. 已提交 update/delete undo 通过 COMMIT_NO 重建 history，后台 purge resume 继续清理。
5. commit durability 策略只决定事务对外返回前等待 redo 到 write/flush/background 的阶段。
6. 本切片不新增 commit record；因此 `redo.currentLsn()` 是 commit 等待边界，而非独立 logical commit LSN。
7. DML facade 入口必须检查 `RecoveryTrafficGate`，在 RECOVERING/FAILED/CLOSED 时拒绝普通 DML。
8. 若 `UndoLogManager.onCommit` 失败，事务保持 ACTIVE 且 row locks 不释放；调用方应 rollback 或关闭 session/engine，不能把该异常当作已提交。

## 11. 错误处理

| 场景 | 行为 |
| --- | --- |
| 非 OPEN recovery gate | 抛 `DmlOperationException` 或 `EngineStateException`，不修改事务。 |
| 只读事务写入 | `TransactionStateException`。 |
| duplicate insert | 抛 `DmlDuplicateKeyException`，不写第二条记录。 |
| lock wait timeout/deadlock | 原样传播 `LockWaitTimeoutException` / `DeadlockDetectedException`；调用方可 rollback。 |
| update/delete miss | 返回 affectedRows=0，不写 undo。 |
| beforeX 后 B+Tree 写失败 | rollback 当前 MTR 释放 latch；事务保留 undo 链，调用方 rollback 幂等清理。 |
| onCommit 失败 | 事务仍 ACTIVE，row locks 不释放；undo marker 未持久化，调用方可 rollback 或关闭连接。 |
| durability wait timeout | 抛 DML commit durability 异常；事务已 committed，释放锁仍应执行。 |
| rollback apply 失败 | 传播异常；事务可能停在 ROLLING_BACK；锁释放策略见 8.5。 |

## 12. 测试策略

新增测试包：`src/test/java/cn/zhangyis/db/storage/api/dml`。

必须覆盖：

1. INSERT 成功：写入 row，隐藏列使用真实 transactionId 和 insert undo roll pointer。
2. INSERT duplicate：unique current-read 命中后不写第二条记录。
3. UPDATE 成功：写 update undo，替换记录隐藏列；rollback 能恢复旧 image。
4. DELETE 成功：delete-mark，commit 后 history/purge 可清理。
5. COMMIT：调用 `onCommit`，update/delete undo 入 history，`DurabilityPolicy` 被消费，`releaseAll` 清空锁。
6. ROLLBACK：insert 被物理删除，update 恢复旧值，delete 取消 delete-mark，锁释放。
7. Lock wait timeout/deadlock 异常传播且不持 page latch。
8. Durability timeout：事务 committed，锁释放，返回异常可诊断。
9. Recovery gate 非 OPEN 拒绝 DML。
10. `current-implementation-map.md` 状态更新不误画未实现 DD/executor 边。

## 13. 20 角度设计自检

| # | 角度 | 结论 |
| --- | --- | --- |
| 1 | 文档覆盖 | 已核对全部 `innodb-*` / `mysql-*` 文档并在第 2 节映射约束。 |
| 2 | 用户选择 | 按用户选择方案 1，不做 SQL executor/DD。 |
| 3 | 分层依赖 | 新 facade 在 `storage.api.dml`，上层可依赖；内部依赖 storage 组件，不反向依赖 SQL。 |
| 4 | 组合根职责 | `StorageEngine` 只构造/暴露 facade，不承载 DML 业务逻辑。 |
| 5 | 事务状态 | 写前 `assignWriteId`，commit/rollback 使用现有 FSM。 |
| 6 | Undo 顺序 | beforeX 在同一 MTR 内先于聚簇页修改。 |
| 7 | B+Tree 边界 | B+Tree 只接收 transactionId/rollPointer/LogicalRecord，不 import trx/undo。 |
| 8 | Row lock 等待 | 只通过 `BTreeCurrentReadService`，等待前释放 page latch/fix。 |
| 9 | Lock 释放 | commit/rollback 统一 `releaseAll`，解决 current map 缺口。 |
| 10 | Durability | commit 消费 `DurabilityPolicy`，等待边界覆盖 onCommit redo。 |
| 11 | WAL | 不直接刷 data page；保持 flush module WAL gate。 |
| 12 | Recovery | `prepareCommit` 只预留提交号；onCommit 失败仍是 ACTIVE，可由 undo rollback；已提交 update/delete undo 进入 history。 |
| 13 | Purge | DELETE 只 delete-mark，物理清理由现有 purge。 |
| 14 | MVCC | UPDATE/DELETE 写旧 image，保留旧版本链；consistent read 可沿 undo 读旧版本。 |
| 15 | 异常原子性 | 承认 MTR rollback 不撤销内容，用 full rollback 幂等清理 orphan undo。 |
| 16 | 只读/非法状态 | 命令构造和 service 校验明确，使用项目异常。 |
| 17 | 多索引/DD | 明确非目标，不伪装已实现。 |
| 18 | 测试可行性 | 可基于现有 StorageEngineTest/BTree/Undo 测试 fixtures 增量验证。 |
| 19 | current map | 需要更新受影响小节，不改全局图。 |
| 20 | 实现风险 | 最大风险为 commit/onCommit/durability 顺序与 rollback 失败锁释放，已在设计中显式标出并要求测试。 |

## 14. 开放问题与决策

| 问题 | 决策 |
| --- | --- |
| commit durability policy 放 config 还是 command | 第一阶段放 `DmlCommitCommand`，避免扩大 EngineConfig blast radius。 |
| duplicate insert 是 result 还是异常 | 第一阶段新增具体异常更接近 SQL 错误语义；测试明确。 |
| update/delete 所有权不匹配时是否重试 | 第一阶段返回 affectedRows=0；后续可增加 bounded retry 或 statement rollback。 |
| rollback 失败是否 releaseAll | 第一阶段 finally releaseAll，避免锁泄漏；风险写入测试和文档。 |
| 是否支持 autocommit | 不在 storage facade 内管理；session 后续调用 insert/update/delete 后自行 commit。 |
