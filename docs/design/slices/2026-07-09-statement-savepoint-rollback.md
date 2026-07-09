# 1.4 Statement / Savepoint Rollback

## 目标

本 slice 在已有 full rollback、UPDATE/DELETE undo 和 0.19g undo payload redo 之上，实现 storage 内部的 statement/savepoint rollback v1。

- 支持在事务 `ACTIVE` 状态创建保存点，保存当前逻辑 undo 边界。
- 支持回滚到保存点：只撤销保存点之后的 INSERT/UPDATE/DELETE_MARK undo record。
- 回滚后事务保持 `ACTIVE`，undo slot、ReadView、行锁生命周期不按完整事务结束处理。
- 后续写入继续复用同一 `UndoContext`，但 undoNo 必须继续单调递增。
- 暴露 storage 内部 API，供后续 session/executor 的 statement abort 与 SQL SAVEPOINT 接线复用。

## 关键决策

- 新增不可变 `TransactionSavepoint`，记录所属 transaction、逻辑边界 undoNo、边界 roll pointer 和创建序号。
- `UndoContext` 接入 `savepointStack`，保存点只记录边界，不复制 undo record。
- `UndoContext.lastUndoNo` 继续作为 append 高水位，partial rollback 不倒回，避免后续 undoNo 复用。
- `UndoContext.lastRollPointer` 表示当前逻辑回滚链头；rollback-to 成功后重置为保存点的 roll pointer。
- 新增 `UndoContext.logicalLastUndoNo` 表示当前逻辑链头 undoNo；保存点捕获该值，而不是捕获 append 高水位。
- `RollbackService.rollbackToSavepoint(txn, index, savepoint)` 校验事务 ACTIVE 与保存点归属。
- rollback-to 从当前逻辑链头反向读取 undo record，每条仍使用独立 MTR 调现有 `applyUndoRecord`。
- 扫描停止条件是当前 undo record 已到保存点边界：只应用 `rec.undoNo() > savepoint.undoNo()`。
- 成功后修剪保存点栈中目标之后的保存点；目标保存点自身保留，便于重复 rollback-to 同一边界。
- 本 v1 不新增持久 rolled-back marker；已撤销 undo record 通过链头重置从逻辑链断开，恢复期重复扫描依赖现有所有权校验保持幂等。
- 不追加 `TRX_STATE_DELTA(ROLLED_BACK)`，因为事务没有进入 `ROLLING_BACK/ROLLED_BACK`。

## 非目标

- 不接 SQL 层 `SAVEPOINT name` / `ROLLBACK TO SAVEPOINT name` 解析、binder、executor 或 session 自动 statement rollback。
- 不实现 savepoint 之后行锁的精细释放；本 slice 保守保留事务已持有锁，后续由 LockManager savepoint scope 处理。
- 不新增 undo record 持久状态位，也不改 redo record 格式。
- 不实现多索引 rollback、二级索引维护、DD indexId 解析或 prepared/recovered-active 事务。
- 不改变 full rollback、commit/onCommit、purge history list 的既有外部语义。

## 验收测试

- `UndoContextTest` 覆盖空事务、已有 undo 事务的保存点创建、归属校验、栈修剪和 null/跨事务拒绝。
- `RollbackServiceTest` 覆盖 INSERT 后 rollback-to：保存点之后插入被删除，保存点之前记录保留，事务仍 ACTIVE。
- `RollbackServiceTest` 覆盖 UPDATE 后 rollback-to：记录恢复旧 image/旧 hidden columns，后续写入 undoNo 不复用。
- `RollbackServiceTest` 覆盖 DELETE_MARK 后 rollback-to：删除标记被取消，旧 hidden columns 恢复。
- 测试 no-op rollback-to latest savepoint：不写数据页、不释放 slot、不写 rollback-complete trx redo。
- 测试 rollback-to 失败路径：当前 MTR 被释放，事务仍 ACTIVE，`UndoContext` 边界不提前修改。
- 回归 full/recovery rollback：partial 后继续写入，full rollback 只走当前逻辑链；`rollbackRecovered` 反扫已撤销记录保持 no-op 幂等。

## current map 更新要求

- Transaction/MVCC 缺口中把 statement/savepoint rollback 从“未实现”改为“storage 内部 v1 已接”。
- Undo/rollback 行说明 partial rollback 不释放 slot、不结束事务、不写事务终态 redo。
- Known gaps 保留 SQL/session 接线、savepoint lock scope、持久 rolled-back marker、多索引/DD 解析。
- `storage-backlog.md` 将 Tier 1 的 1.4 标为完成，并注明 v1 的简化点与后续项。
