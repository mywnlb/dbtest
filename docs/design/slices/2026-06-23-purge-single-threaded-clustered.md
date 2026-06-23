# Slice: Purge — 单线程聚簇 purge + undo 段 drop 回收（§16 步 11/12/14 部分）

- 日期：2026-06-23
- 关联设计：`innodb-undo-log-purge-design.md` §5.6 HistoryList、§5.7 PurgeCoordinator、§7.4 Commit/History、§7.7 Purge（**boundary 按 TransactionNo**，§516）、§11.3、§16 步 11/12/14；`innodb-transaction-mvcc-design.md` §555（purge boundary = TransactionNo）。
- 前置（已实现）：T1.3c–f undo 写/rollback/UPDATE/DELETE-mark；T1.4 ReadView+MVCC；delete-marked 记录存在但从不物理移除；`RollbackSegmentSlotManager`（内存 slot）；`DiskSpaceManager.dropSegment`；`UndoSegmentHandle`（含 inodeSlot/segmentId/firstPageId）。
- 现状缺口：无 history list / 无 purge coordinator；`UndoLogManager.onCommit` 只释放 insert slot（`UndoLogManager.java`），含 update/delete 的已提交事务 slot+段泄漏。
- 本片新增底层 API：`ReadView.lowLimitNo` + `ReadViewLease`、`UndoLogSegment.forEachRecordWithPointer`、btree `purgeDeleteMarkedClustered`（严格）、`UndoSpaceAllocator.dropUndoSegment` + `DiskSpaceUndoAllocator`→`DiskSpaceManager.dropSegment`。
- 定位：§16 步 11（PurgeBoundary+history cursor）+ 步 12（单线程清理 delete-mark 聚簇记录）+ 步 14 的 **drop 回收部分**（cached reuse 推迟）。

## 1. 范围

做：
- **PurgeBoundary（按 TransactionNo，#1）**：`ReadView` 增 `lowLimitNo`（openReadView 时捕获的"下一个待分配 TransactionNo"）；`TransactionSystem.purgeLowWaterNo()`（锁内）= 所有 live ReadView 的 `min(lowLimitNo)`，无 live ReadView 则当前 nextTransactionNo。一条已提交 undo log（`transactionNo = No`）可 purge ⟺ `No < purgeLowWaterNo`。
- **live ReadView 生命周期（#2）**：`ReadViewManager.openReadView` 登记 live view 并关联 `ReadViewLease`(AutoCloseable)，close 注销；**RR** lease 随事务（commit/finishRollback 关，复用现有 release）、**RC** lease 随语句读（读后显式关）；boundary 只统计未关闭的 live view。RC 不再"只清 RR 缓存"导致泄漏。
- **HistoryList（内存 FIFO，按 TransactionNo）**：条目 = {transactionNo, creatorTrxId, undoSpaceId, undoFirstPageId, rollbackSegmentId, slotId}。
- **commit 编排顺序（#3）**：`TransactionManager.commit` 先分配 `TransactionNo`（现状），**再**调 `onCommit`，使其能读到已分配的 No；明确 onCommit 不再"顺序不敏感"，必须在 No 分配后调用（统一编排或 commit facade）。`commit()` public 行为不变。
- **onCommit 改**：`hasUpdateUndo` → 用已分配 No 入 HistoryList（不释放 slot/段，purge 负责）；纯 insert undo → 释放 slot + **dropSegment 全回收**（本片新增段回收）。
- **PurgeCoordinator.runBatch(maxLogs) + PurgeSummary**：算 `purgeLowWaterNo` → 从 head 取 `transactionNo < boundary` 的条目（≥ 即停）→ 逐条 history entry：
  1. 独立只读 MTR `undoAccess.open(firstPageId, SHARED)` + **`forEachRecordWithPointer`（#5）** 收集 `(type, clusterKey, 该 undo 记录自身 RollPointer)`，提交关闭 undo MTR；同时从 open 段的 `UndoSegmentHandle` 取回收用 `SegmentRef`（inodeSlot/segmentId，#7）。
  2. 每 `DELETE_MARK`：独立 index MTR 调 **`purgeDeleteMarkedClustered(mtr,index,key,expectedTrxId=creatorTrxId,expectedRollPtr=该 undo 记录地址)`（#4 严格）**——re-locate key → **必须**仍 delete-marked 且 `DB_TRX_ID==creatorTrxId` 且 `DB_ROLL_PTR==undo 记录地址` 才物理移除；不满足=确认 stale 跳过该 task；**绝不主动 deleteMark live row**。`UPDATE_ROW`/`INSERT_ROW` 对聚簇无动作（仅释放 undo）。
  3. **per-entry 原子（#8）**：整条 entry 所有 DELETE_MARK task 全部成功或确认 stale 后，才 `dropUndoSegment(SegmentRef)` + `RollbackSegmentSlotManager.release(slot)` + 摘除 history 条目；任一 hard failure（页损坏/IO/非 stale 的不一致）保留 history head 并停批/报错，不丢段。
- **latch 纪律（同 `MvccReader`）**：任一时刻只持 index 或 undo 之一，先读 undo→关→再 btree 移除，杜绝 AB-BA。

不做：
- **cached segment 复用推迟**（#6）：需新增"保留 inode + 回收链上多余页 + 重格式化首页"的 FSP/undo reset API + 有界 free-segment cache + 句柄重建；本片只 `dropSegment` 全回收，复用留独立切片。
- 多 worker purge（步 13）、二级索引 purge、后台 purge 线程（runBatch 同步 test-driven）。
- 持久 rseg header / 持久 history / `ReadView.lowLimitNo` 持久化 / crash recovery resume（步 16）。
- purge→undo tablespace truncate 调度（步 15）、savepoint/statement rollback、metrics、DDL/temp/external undo。

## 2. 数据流与不变量

1. commit：分配 `TransactionNo` → `onCommit`：`hasUpdateUndo` 用该 No 入 HistoryList（slot/段不放）；insert-only 释放 slot + dropSegment。
2. purge：`runBatch` → `purgeLowWaterNo` → FIFO 弹 `transactionNo < boundary`（≥ 即停）→ 每条 entry：读 undo（只读 MTR，带指针）→ 逐 DELETE_MARK 独立 index MTR 严格物理移除 → 全成功/stale 才 dropSegment + release slot + 摘条目。
3. 不变量：①purge 仅回收 `transactionNo < min(live ReadView lowLimitNo)` 的已提交 undo log（长 RR/RC live view 在世则边界挡住）；②物理移除严格三重校验（delete-marked + `DB_TRX_ID==creatorTrxId` + `DB_ROLL_PTR==undo 记录地址`），**不满足只跳过、绝不标记 live row**；③per-entry 原子——段/ slot/history 只在该 entry 全部成功或确认 stale 后回收；④幂等：重复 runBatch / 孤立 undo 安全 skip；⑤purge 走提交 undo log 的记录链，不走版本链 `oldHidden.dbRollPtr`（两条链不混用）。

## 3. 错误与并发约束

- 单线程同步 `runBatch`，不入事务死锁检测、不持 page latch 跨 MTR；每条读取/移除独立 MTR，失败 `rollbackUncommitted`。
- per-entry 原子：hard failure 保留 history head 并停批（可重试/恢复期续作留后续片），不在部分成功时 dropSegment/摘 history。
- 校验失败/页损坏抛领域异常（`UndoLogFormatException` 等），保留根因。
- ReadView 登记/注销与 `purgeLowWaterNo` 计算在 `TransactionSystem` 锁内原子，避免边界与 live view 竞态。
- redo：purge 物理写经 MTR→PAGE_BYTES（默认内存、测试 durable）；段 drop 经 FSP MTR 路径。

## 4. 验收测试与 current map

- 边界单元：多 live ReadView `min(lowLimitNo)`、无 live view=nextTransactionNo、长 RR 快照挡 purge、RR release 后推进；**RC lease close 后边界推进（#2 回归）**。
- history/commit：update/delete commit 用已分配 No 入 history、insert-only 不入且即 dropSegment、按 TransactionNo 序。
- purge e2e（delete）：建行→delete→commit→无 live ReadView→`runBatch` 后 `lookupIncludingDeleted` 查不到 + slot/段回收；有 live 旧 ReadView 时不移除、release/close 后再 purge 成功。
- **purge e2e（UPDATE-only，#9）**：update→commit，长 ReadView 存活时 runBatch **不回收**；release 后 runBatch **只 dropSegment 回收 undo 段、不碰聚簇记录**（聚簇当前版本仍在、可查）。
- 严格性（#4）：`purgeDeleteMarkedClustered` 对 live（未标记）行**拒绝**移除（不主动 deleteMark），对隐藏列不匹配/已被改的记录确认 stale skip。
- per-entry 原子（#8）：注入某 task 失败 → 该 entry 段不被 drop、history head 保留、可重试。
- 幂等/孤立：重复 runBatch 安全；孤立 undo skip 不报错。段 drop 后页归还 FSP 可再分配。
- 固定 JDK/Gradle 全量 `test` 不回退；更新 `current-implementation-map.md`：trx/undo slice 加 HistoryList/PurgeCoordinator/PurgeView/ReadViewLease 数据链与 package status，onCommit 行为校正，purge/段回收缺口校正；**purge 仍 test-wired（无生产 DML facade），相关类进 Reserved / Unwired Production Types，不得暗示生产接线（#10）**。
- 已知遗留（留后续）：cached reuse、多 worker、二级索引 purge、持久 rseg/history、recovery resume、truncate 调度、后台线程、metrics。
