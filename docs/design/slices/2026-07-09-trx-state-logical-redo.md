# 0.19i Transaction State Logical Redo

## 目标

本 slice 增加 non-page 事务状态 redo，用于记录 commit/rollback 边界的 redo 顺序和诊断信息。

- record 不带 `PageId`，不触发 page skip predicate 或 PageStore IO。
- commit 路径在 undo commit MTR 中追加 `COMMITTED` 状态 redo。
- rollback 路径在 `finishRollback` 前追加 `ROLLED_BACK` 状态 redo。
- 恢复期当前只消费 record 并 no-op，不重建事务表。

## 关键决策

- 新增 `TransactionStateDeltaRecord(TransactionId, fromState, toState, TransactionNo, reason)`。
- redo 包定义稳定磁盘枚举 `TransactionStateDeltaState` / `TransactionStateDeltaReason`。
- trx 包新增 `TransactionStateRedoDeltas` 做运行时状态到 redo 枚举的映射，避免 redo 反向依赖 trx。
- `RedoBatchFrameCodec` 对 `TRX_STATE_DELTA` 不读写 `PageId`。
- `TransactionStateRedoHandler.affectedPages()` 返回空列表，dispatcher 因此不会调用 skip predicate。
- 新增 MTR 本地分类 `TRX_STATE`，只用于诊断，不进入 redo 文件。

## 非目标

- 不实现 PREPARED / RECOVERED_ACTIVE / XA。
- 不让 redo replay 改写 live `TransactionManager` 状态。
- 不替代 undo/rseg header 对恢复期提交/未提交判定的权威地位。
- 不改变 DML facade 的 durability policy 消费位置。

## 验收测试

- `TransactionStateDeltaRecord` codec round-trip 且 `byteLength` 不含 `PageId`。
- 默认 dispatcher 可应用 trx state redo，且不触碰 PageStore、不调用 skip predicate。
- `UndoLogManager.onCommit` 的持久 commit MTR 包含 COMMIT state redo。
- `RollbackService.rollback` 在 finishRollback 前包含 ROLLBACK state redo。
- 既有事务 commit/rollback/undo/purge 测试不倒退。

## current map 更新要求

- Redo replay 行加入 `TransactionStateRedoHandler`。
- Transaction 当前状态说明 commit/rollback diagnostic redo 已接，但恢复重建事务表仍未接。
- Known gaps 保留 prepared/recovered-active 和正式 trx recovery，而不是把事务状态 redo写成恢复权威。
