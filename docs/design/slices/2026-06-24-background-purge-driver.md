# Slice: 后台 purge driver（backlog 0.4）

依据：`innodb-undo-log-purge-design.md` §5.7（PurgeCoordinator/PurgeWorker）、§7.7（Purge）、§9（Writer/Flusher/
Notifier 线程模型，复用 worker 形态）；`storage-backlog.md` 0.4。`PurgeCoordinator.runBatch` 已存在（test-only，
单聚簇索引），本片加后台 driver 线程 + engine 接线让 purge 真跑。

## 目标

> **不变量**：engine OPEN 后，后台 `PurgeDriverWorker` 周期/on-demand 驱动 `PurgeCoordinator.runBatch(maxLogs)`，
> 使已提交且过了 purge boundary（`TransactionSystem.purgeLowWaterNo`）的 delete-marked 聚簇记录被物理移除、undo
> 段被回收——无需前台手动调。

## 关键决策

- **`PurgeDriverWorker`（`storage.trx`）沿用 `RedoFlushWorker`/`PageCleanerWorker` 形状**：单 daemon 线程 +
  `ReentrantLock`+`Condition`+`PurgeDriverWorkerState` 枚举，无 `synchronized`；周期 idle tick **或** `requestPurge()`
  合并唤醒后调 `runBatch(maxLogs)`；失败即 FAILED 停机；`stop(timeout)` 幂等有界。
- **复用单配置聚簇索引**：R 1.2 的 `recoveryRollbackIndex`/`configureRecoveryRollback` 泛化为 `clusteredIndex`/
  `configureClusteredIndex`，**同时服务恢复回滚 + purge**（单表无 DD 假设）。通用多索引 = 2.1/DD 留后续。
- **engine 惰性构造 `PurgeCoordinator`**：其 deps `txnSystem`/`history`/`undoAllocator` 升为 engine 字段；`open()` 内、
  索引已配置时构造 `PurgeCoordinator(mgr, txnSystem, history, undoAccess, undoAllocator, rollbackSlots, btree, index)`
  + `PurgeDriverWorker` 并启动（与其它后台 worker 一起，recover 之后、发布 OPEN 前）；索引未配 → 不构造/不启动
  （既有 engine 测试零影响）。
- **boundary 自治**：`runBatch` 内部读 `system.purgeLowWaterNo()`，driver 只管节奏。
- **`close()` 顺序**：停 page cleaner → 停 redo flusher → **停 purge driver** → final `flushThrough`。

## 非目标（明确推迟）

- 多 worker purge 分片、二级索引 purge、持久 history、recovery purge resume（1.3）、purge→undo tablespace truncate 调度。
- `requestPurge()` on-demand 接进上层（提供 + 测试，但不接 commit/DML 触发，周期 tick 已够）。
- 不改 `PurgeCoordinator.runBatch` 既有逻辑（只加 driver 驱动）。

## 验收测试

- `PurgeDriverWorker`（fake 或真 coordinator）：周期驱动 runBatch、`requestPurge` on-demand、空转不报错、stop 幂等、
  runBatch 抛 → FAILED。
- **money test**（StorageEngine）：配 `clusteredIndex` → open → 插一行+commit+onCommit → 同行 delete-mark+commit+
  onCommit（无 live ReadView，boundary 放行）→ 后台 driver 跑 → `awaitUntil` 该 delete-marked 行经
  `lookupIncludingDeleted` 也查不到（已物理移除）。
- 回归：trx/undo/engine/recovery 全量不倒退；既有 engine 测试不配 `clusteredIndex` → 无 purge driver，行为不变。

## 文档更新要求

- Transaction/Engine 数据链：补"`StorageEngine` 启动 `PurgeDriverWorker` 后台周期 `runBatch`，回收 committed undo +
  物理移除 delete-marked 记录"。
- Package Status：`storage.trx` 新增 `PurgeDriverWorker`/`PurgeDriverWorkerState`；`PurgeCoordinator` 改 production-wired
  （when index configured）；`EngineConfig`/engine `configureClusteredIndex`。
- Transaction/Undo 缺口：把"purge 无后台 driver / `new PurgeCoordinator`=0 prod"改为"后台 driver 已接（单配置索引）；
  多 worker/二级索引/持久 history/recovery resume 仍缺"。
- `storage-backlog.md`：0.4 标 ✅；1.3 RESUME_PURGE 依赖（0.3 + 0.4）已满足；推荐路线下一片改 1.3。
