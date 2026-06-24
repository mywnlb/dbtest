# Slice: 持久 history + R 1.3 RESUME_PURGE（合一）

依据：`innodb-undo-log-purge-design.md` §5.6（HistoryList）、§7.4（Commit 与 History）、§14.5（Crash Recovery
step 4/7/8）；`innodb-crash-recovery-design.md` §7.7（Purge Resume）；`storage-backlog.md` 1.3。依赖 0.3（持久 rseg
slot 目录）、R 1.2（undo state + 恢复扫描）、0.4（后台 purge driver）均已满足。

## 目标

> **不变量**：崩溃时已提交但未 purge 的 update/delete undo（committed history）在重启恢复期被重建，且
> `TransactionSystem` 计数器复位到不低于已见高水位，使后台 purge driver 自动续作 purge（删除 delete-marked 记录、
> 回收 undo 段）。无需链表——history 节点 = 0.3 page3 slot 目录 + R 1.2 `state=COMMITTED` 的 undo 段。

## 关键决策

- **undo log header 加 `COMMIT_NO`（u64 transactionNo）**：`UndoLogSegment.markCommitted(transactionNo)` 同 MTR 写
  `STATE_COMMITTED` + `COMMIT_NO`（redo 保护）；`committedTransactionNo()` 读。`UndoLogManager.onCommit` 传
  `txn.transactionNo()`（任何已写事务 commit 都已分配 No）。
- **恢复重建 history（扩展 R 1.2 步骤）**：engine `rollbackRecoveredActiveTransactions` 泛化为
  `recoverRollbackSegmentTransactions`——对每个 restored slot 读 undo state：ACTIVE → 回滚（R 1.2）；COMMITTED →
  重建 `HistoryEntry(COMMIT_NO, creatorTrxId=TRANSACTION_ID, undoSpace, firstPage, slotId)`，按 `COMMIT_NO` **升序**
  `submitCommitted`（FIFO 提交序）。
- **复位 `TransactionSystem` 计数器**：从 restored 段算 `nextTrxId = max(TRANSACTION_ID)+1`、
  `nextTrxNo = max(COMMIT_NO over committed)+1`；新增 `TransactionSystem.restoreCounters`（单调，只前进不回退）。否则
  `purgeLowWaterNo`（= nextTransactionNo）太低，重建 history 不满足 boundary、且新事务 id 与 pre-crash 重复。
- **0.4 purge driver（已运行）自动续作**：history 重建 + 计数器复位后，后台 driver 的 `runBatch` 处理重建的 committed
  history → 1.3 RESUME_PURGE 顺带完成；不另建 recovery purge stage。

## 非目标（明确推迟）

- **insert-reclaim 不持久**：纯 insert committed 在 commit 清 page3 slot（0.3），crash 后其 undo 段成 orphan（泄漏
  undo 页，**无正确性问题**——insert undo 从不服务一致性读）；持久 insert-reclaim / orphan 扫描留后续。
- 计数器从 restored 段算（已 purge 的段已消失 → 其 trxId 可能被复用；其记录已被 purge 清除，复用基本无害，记缺口）；
  page3 持久 trx 计数页留后续。
- 多 rseg / 持久 history 链表（§6.3 head/tail）/ 二级索引 / prepared txn。

## 验收测试

- undo state：`markCommitted(no)` 后 `state()`=COMMITTED 且 `committedTransactionNo()`=no（round-trip，经 redo）。
- `TransactionSystem.restoreCounters`：单调前进（更小值不回退）；之后 `allocateWriteId`/`allocateTransactionNo` 从复位值续。
- **money test**（StorageEngine）：e1 配空 → insert+commit、同行 delete-mark+commit（不配 `clusteredIndex` → 无 purge，
  history 留盘）→ checkpoint → close；e2 `configureClusteredIndex` + open → 恢复重建 committed history + 复位计数器 →
  后台 driver → `awaitUntil` 该 delete-marked 行经 `lookupIncludingDeleted` 也查不到（已 purge）。
- 回归：trx/undo/engine/recovery 全量不倒退；既有 engine 测试不配索引 → 无重建/无 purge，行为不变。

## 文档更新要求

- Recovery 数据链：补"恢复扫 rseg → COMMITTED 段重建 committed history + 复位 trx 计数器 → 后台 driver 续作 purge"。
- Package Status：`UndoLogSegment` 加 `COMMIT_NO`/`committedTransactionNo`；`TransactionSystem` 加 `restoreCounters`；
  `HistoryList` 改 production 由 engine 恢复重建。
- Undo/Transaction/Recovery 缺口：把"无恢复期 history 重建 / 无 PURGE_RESUME"改为"history 重建 + purge resume 已接
  （update/delete committed；insert-reclaim 持久、page3 trx 计数页、多 rseg 仍缺）"。
- `storage-backlog.md`：1.3 标 ✅；R crash-recovery 完成度上调；推荐路线给出后续（Tier 0 独立项 / DD 上层）。
