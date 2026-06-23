# Slice: T1.3c — undo 写路径接线（UndoContext + UndoLogManager.beforeInsert + 真 DB_ROLL_PTR）

- 日期：2026-06-18
- 关联设计：`innodb-undo-log-purge-design.md` §5.2/§5.3（UndoLogManager/UndoContext）、§5.4（RollbackSegment/slot）、§6.2（RollbackSegmentId/UndoSlotId）、§7.1/§7.2（undo 分配 + INSERT undo 流程）、§9.3（slot mutex 锁序）。
- 前置：T1.3a/b（`UndoLogSegment(Access)` append→RollPointer / read / forEach、`UndoPage`、`UndoRecordCodec`、`UndoSpaceAllocator` 端口 + `DiskSpaceUndoAllocator` 适配器）；trx 核心（`Transaction`/`TransactionManager`/`TransactionSystem`、`assignWriteId`）；btree `SplitCapableBTreeIndexService.insertClustered`。
- 定位：T1.3 epic 第三片 = **首次接事务语义**。把物理 undo 基座接到事务写路径：建 `UndoContext`、`UndoLogManager.beforeInsert` 写 INSERT undo、用内存 rollback segment slot 目录登记活跃 insert undo segment、`insertClustered` 写真 `DB_ROLL_PTR`（替换恒 NULL）。

## 1. 范围

做：
- `storage.trx`（事务语义）：
  - `UndoContext`（挂 `Transaction` 的事务运行时 undo 子状态）：`rollbackSegmentId`、`slotId`、`insertUndoFirstPageId`、`lastUndoNo`、`lastRollPointer`。
  - `UndoLogManager`（事务 undo 门面，固定放 `cn.zhangyis.db.storage.trx`）：`beforeInsert(Transaction, MiniTransaction, tableId, indexId, clusterKey, IndexKeyDef, TableSchema) -> RollPointer`——惰性 `ensureUndoContext`（首写经 `UndoLogSegmentAccess.create` 建 insert undo segment + 占内存 rseg slot）、分配 `undoNo`（`lastUndoNo+1`）、组 `UndoRecord(INSERT_ROW, prevRollPointer=ctx.lastRollPointer)`、`open(EXCLUSIVE)` append、更新 `ctx.lastUndoNo/lastRollPointer`、返回真 `RollPointer`。维护事务语义；import `storage.undo` 物理设施。
  - `Transaction` 增 `UndoContext` 字段 + 包内可见 mutator（仅 `UndoLogManager` 改）。
- `storage.trx` 内存 rseg 目录：
  - `RollbackSegmentDirectory` / `RollbackSegmentSlotManager`（可合并为小类）：固定一个默认 `RollbackSegmentId`，用内存 slot array 记录 `UndoSlotId -> insertUndoFirstPageId`，仅供本片事务运行时定位和测试断言。
  - slot 分配用一把 `ReentrantLock` 保护内存数组；锁内不做页分配、不访问 BufferPool、不等待 IO。slot 回收、持久化 header、恢复扫描留后续片。
- `storage.undo`（**仅物理设施，不 import `Transaction`**）：
  - 复用 T1.3a/b 已有 `UndoLogSegmentAccess` / `UndoLogSegment` / `UndoSpaceAllocator`；本片不新增 rollback segment header 页格式，也不改 `DiskSpaceUndoAllocator` 分配接口。
- `storage.btree`：`insertClustered(..., RollPointer rollPointer)` 盖 `HiddenColumns(transactionId, rollPointer)`（替换恒 NULL），**不 import trx/undo**。
- `domain`：`RollbackSegmentId`、`UndoSlotId` 小值对象；`RollPointer` 格式**不变**（单 rseg/单 undo space，rseg/slot 存 `UndoContext` + 内存目录，不进指针）。

不做（→ 后续片）：
- 持久 rollback segment header 页格式、rseg directory 持久发现、恢复期扫描 active slot（T1.3d）。
- 真 rollback / 反向走链应用 undo、slot 回收、commit 标 insert undo `REUSABLE`（T1.3d）。
- UPDATE/DELETE undo、history list base、MVCC 旧版本、purge、恢复期 rollback（T1.3e+）。
- 多 rseg 选择策略、多 undo 表空间、`RollPointer` 改格式、savepoint、`storage.api` DML facade。
- 失败插入的完整原子清理：当前 MTR rollback 不做 content undo；本片只在 test-wired happy path 接线，生产 DML facade 的失败边界留后续片，并在 current map 标为缺口。

## 2. 关键决策
1. **首次接事务语义**：物理 undo（T1.3a/b）+ 事务核心已就位，本片只做接线，不再造新的持久页格式。
2. **依赖方向 `storage.trx → storage.undo`**：`UndoLogManager` 在 trx 持事务语义并调 undo 物理设施；undo 不反向 import `Transaction`，不经 `storage.api` 过度缝合。
3. **固定包位置**：`UndoLogManager` 放 `storage.trx`，因为 `Transaction` 的 undo mutator 保持 package-private；不放 Java 子包，避免为跨包访问扩大 public mutator。
4. **insertClustered 与 trx/undo 解耦**：只多收一个 `RollPointer` 参数；orchestration（`assignWriteId → beforeInsert → insertClustered`）在上层。本片整栈仍 test-wired、无生产组合根，故 orchestration 由测试驱动，不新建 api facade。
5. **undo append 与 record 写同 MTR / 同 redo batch**（WAL，§7.2）：复用 D3/D4 物理 redo，不新增 redo 类型/恢复编排。由于 MTR rollback 不撤销页内容，测试只覆盖成功插入路径；失败插入原子性必须在后续 DML facade/rollback 片补齐。
6. **内存 slot 认领**：`RollbackSegmentSlotManager` 持一把 `ReentrantLock` 串行「扫空槽→登记 firstPageId」；无 `synchronized`。锁不包围 `UndoLogSegmentAccess.create/open/append`。

## 3. 验收测试
- `UndoContextTest`：字段 + 惰性初值（未写时 `lastRollPointer=NULL`、`lastUndoNo=NONE`、`insertUndoFirstPageId` 未分配）。
- `RollbackSegmentSlotManagerTest`：内存 slot 初值为空、claim 返回不同 slot、slot 指向 insert undo segment 首页、slot 耗尽抛领域异常、并发 claim 不重复；不测试 reload 持久性。
- `UndoLogManagerTest`（onPool harness 复用 T1.3b 注入：`FileChannelPageStore`+`LruBufferPool`+`DiskSpaceManager`+`DiskSpaceUndoAllocator`）：首写建段 + 占内存 slot + 返回 `DB_ROLL_PTR` 非 NULL 指 insert undo；同 txn 多 insert `undoNo` 递增 + `prevRollPointer` 串链 + `readRecord` 回值；内存 rseg slot 落该段首页；commit + **新 `PageStore`/`BufferPool`** reload 后按 `DB_ROLL_PTR` 读回 undo record 等值原 `clusterKey`（读回依赖 roll pointer + undo first page，不依赖持久 rseg header）。
- `insertClustered` 改签名：既有 `ClusteredInsertTest` / split 测试同步更新（断言隐藏列 `DB_ROLL_PTR`）。
- `UndoWritePathWiringTest`：成功路径显式执行 `assignWriteId → beforeInsert → insertClustered`，断言聚簇记录隐藏列 `DB_TRX_ID` 为事务写 id、`DB_ROLL_PTR` 为 `beforeInsert` 返回值。失败插入路径不在本片接线；测试名或 current map 必须记录 orphan undo 风险。
- 回归：全量 Gradle `test` 不倒退；非聚簇字节路径不受影响。

## 4. current map 更新（实现后）
- **Undo Log Layer Slice + Transaction Layer Slice**：新增 `UndoLogManager`/`UndoContext`/内存 rseg slot 目录接线；`insertClustered` 行由「`DB_ROLL_PTR` 恒 NULL」改为「可写调用方传入的 insert undo 指针」；`DiskSpaceUndoAllocator` 被 `UndoLogManager` 经 `UndoSpaceAllocator` 端口调用，但仍无 SQL/session 生产组合根。
- **Reserved / Unwired**：持久 rseg header、恢复扫描 active slot、slot 回收、失败插入原子清理、生产 DML facade，全部记入缺口并标 T1.3d+。
