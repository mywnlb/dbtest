# Slice: R 1.2 ROLLBACK_TRX（恢复期回滚未提交事务）

依据：`innodb-undo-log-purge-design.md` §7.6（rollback）、§14.5（crash recovery）；
`innodb-crash-recovery-design.md` §7.6（Transaction Rollback）；`storage-backlog.md` 1.2。依赖 0.3（持久 rseg header +
恢复扫描重建 slot 目录）已满足。本片在恢复期把崩溃时 ACTIVE 的事务回滚。

## 目标

> **不变量**：崩溃时 ACTIVE（未提交）的事务，重启恢复期被回滚（undo 反向应用到**显式配置的聚簇索引**）；
> 已提交事务跳过、其数据保留。

## 关键决策

- **undo-log state 真值化**：`UndoPageLayout.STATE_COMMITTED(=1)`（现 `STATE_ACTIVE=0` 是占位）；`UndoLogSegment`
  加 `markCommitted(mtr)`（写首页 STATE，redo 保护）+ `state()`/`isActive()` 读。恢复期靠 state 分类 active/committed
  （纯 insert 虽已清 page3 slot，update-undo 的 slot 留着，必须靠 state 判 committed）。
- **commit 标记**：`UndoLogManager.onCommit` 在释放/入 history **前**，短 MTR 内 `markCommitted` 首页（与 0.3 的
  page3 release 同短 MTR 路径，`headerRepo`+`mtrManager` present 时生效）。
- **恢复期分类 + 回滚**：`EngineConfig` 加可选 `recoveryRollbackIndex`（`BTreeIndex`，**测试注入**，无 DD）。engine
  existing-open 在 `restoreRollbackSegmentSlots` 后，对每个 restored slot：开 undo 段读 state → ACTIVE 则
  `RollbackService.rollbackRecovered(undoSpace, firstPageId, index)`；COMMITTED 跳过。`recoveryRollbackIndex` 为 null
  时跳过整步（既有 engine 测试不注入索引，行为不变）。
- **`rollbackRecovered`（新方法）**：无 live `Transaction`，直接从 `undoFirstPageId` + 配置 index 走链——
  `forEachRecordWithPointer` 正向收集 (rec, rp) → **反向**逐条独立 MTR `applyUndoRecord`（复用现有 INSERT/UPDATE/
  DELETE_MARK 反向命令）；不走 Transaction 状态机（那是前台 `rollback` 路径）；回滚后内存 `release` 该 slot。
- **幂等**：`deleteClustered`/`replaceClustered`/`setClusteredDeleteMark` 未命中即 no-op → 二次崩溃重复回滚安全；
  故回滚后 page3 slot 清**不持久化**（留后续），re-rollback 幂等。

## 非目标（明确推迟）

- 多索引 / DD `indexId→index/schema` 解析（单显式注入索引，`rec.indexId() != index.indexId()` 抛）；prepared txn；
  statement / savepoint rollback。
- 正式 `RecoveryStageName.UNDO_ROLLBACK` recovery stage（先放 engine 后恢复步，同 0.3）；回滚后 page3 release 持久化。
- `RollbackService/PurgeCoordinator` 前台 release 持久（0.3 已记缺口）；truncate rebuild page3 format（0.3 已记缺口）。

## 验收测试

- undo-log state：`UndoLogSegment` format 后 `isActive()`；`markCommitted` 后 state=COMMITTED（round-trip，经 redo）。
- `UndoLogManager.onCommit` 标 undo 段 committed（纯 insert + update-undo 两路）。
- `RollbackService.rollbackRecovered`：ACTIVE 段反向应用 undo（INSERT 删行），未命中幂等 no-op；indexId 不符抛。
- **money test**（StorageEngine）：active 事务 `beforeInsert`+`insertClustered` 插一行 → 不 commit → checkpoint →
  close → 重开（配 `recoveryRollbackIndex`）→ 恢复回滚 → 该行 `lookup` 查不到（已删）。
- committed 事务对照：commit+`onCommit` 后重开 → 该行保留（state=COMMITTED，恢复跳过，不误删）。
- 回归：trx/undo/engine/recovery 全量不倒退。

## 文档更新要求

- Recovery 数据链：补"恢复扫 rseg → 读 undo state → ACTIVE 经 `rollbackRecovered` 回滚（配置索引）"。
- Undo/Transaction Package Status：`UndoLogSegment` 加 state；`RollbackService` 加 `rollbackRecovered`；`EngineConfig`
  加 `recoveryRollbackIndex`。
- Undo/Transaction/Recovery 缺口：把"恢复期 rollback 仍缺"改为"R 1.2 已接（单配置索引）；多索引/DD、prepared、
  正式 UNDO_ROLLBACK stage 仍缺"。
- `storage-backlog.md`：1.2 标 ✅；推荐路线下一片改 1.3 RESUME_PURGE 或 0.4 purge driver。
