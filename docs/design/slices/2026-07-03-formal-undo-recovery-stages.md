# Slice: formal UNDO_ROLLBACK / RESUME_PURGE recovery stages

## Goal

- 把已经存在的 engine 后恢复事务 undo 逻辑提升为 `CrashRecoveryService` 的正式阶段：
  `UNDO_ROLLBACK` 与 `RESUME_PURGE`。
- 保持当前教学简化：无 DD discovery、无多索引解析、无 prepared/XA，只使用显式配置的单聚簇索引。

## Design Inputs

- `innodb-crash-recovery-design.md` §6.1、§7.6、§7.7：redo 后执行 transaction rollback，再 resume purge，最后开放流量。
- `innodb-undo-log-purge-design.md` §9.3、§11.4：恢复期扫描 rseg header、rollback recovered active、重建 history。
- `current-implementation-map.md`：R 1.2/R 1.3 已有真实逻辑，但之前位于 `CrashRecoveryService.recover()` 之后。

## Key Decisions

- 新增 `TransactionUndoRecoveryParticipant` 作为 recovery 层端口，避免 `CrashRecoveryService` 直接依赖 trx/undo/btree 内部。
- 新增 `TransactionUndoRecoveryResult` 记录 restored slot、rolled-back active、skipped active、rebuilt history 计数。
- `StorageEngine` 用 page3 扫描逻辑实现 participant；正式 trx recovery v1 后改由
  `RecoveryRequest.withTransactionRecovery(context, participant)` 成对注入 sidecar/sink/snapshot 与 undo 恢复。
- `CrashRecoveryService` 在 `OPEN_TRAFFIC` 前调用 participant，并记录 `UNDO_ROLLBACK` / `RESUME_PURGE` stage。
- recovery rollback 可能追加 redo；若 request 携带 `recoveredRedoManager`，开放流量前执行 `redo.flush()`，保证回滚 redo durable。

## Non-Goals

- 不实现 `DISCOVER_TABLESPACE`、`RECOVER_DDL`、prepared/recovered-active 事务状态。
- 不引入 data dictionary、多索引 rollback/purge 或二级索引恢复。
- 不把 active slot release 持久化补齐到所有路径；当前仍保留既有 page3 release 缺口。
- 不改变普通 DML/session/executor 接线。

## Acceptance Tests

- `CrashRecoveryServiceTest.runsTransactionUndoRecoveryStagesBeforeOpeningTraffic`
  验证 fake participant 在 gate `RECOVERING` 时运行，且阶段顺序在 `OPEN_TRAFFIC` 前。
- `CrashRecoveryServiceTest.recoverFailsClosedWhenTransactionUndoRecoveryFails`
  验证 participant 抛错时 recovery fail-closed，不开放普通流量。
- `StorageEngineTest.recoveryRollsBackActiveInsertOnRestart`
  验证真实 ACTIVE rollback 仍删除未提交插入，并报告 `UNDO_ROLLBACK`。
- `StorageEngineTest.recoveryRebuildsCommittedHistoryAndBackgroundPurgeResumes`
  验证 committed history 重建后后台 purge 续作，并报告 `RESUME_PURGE`。

## Current Map Updates

- Recovery flow 改为：doublewrite -> redo -> boundary -> undo tablespace resume -> space reconcile ->
  formal UNDO_ROLLBACK/RESUME_PURGE -> redo flush -> forceAll -> open traffic。
- Recovery/Undo gaps 改为仍缺 DD discovery、多索引 recovery、prepared txn 和 DDL_RECOVERY。
