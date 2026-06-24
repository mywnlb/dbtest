# Slice: 后台 redo flusher（RedoFlushWorker）

依据：`innodb-redo-log-design.md` §9（Writer/Flusher/Notifier 线程模型）；`storage-backlog.md` 0.1 的第 1 件
（后台驱动 `redo.flush()`）。这是 backlog 0.1 的最小子集——只做后台刷盘，不拆锁、不修 closedLsn、不动 trx。

## 目标

让 `flushedToDiskLsn` 由后台自动前进，闭合"无人驱动 `redo.flush()`"导致的卡点：当前淘汰/flush 的 WAL gate
（`RedoLogManager.waitFlushed`）只有在前台 `StorageEngine.checkpoint/close`、truncation 时才推进 durable LSN，
平时常命中 `SKIPPED_REDO_NOT_DURABLE` → 脏页刷不出、帧腾不出、checkpoint 推不动（直接拖累刚做的 WAL 安全淘汰）。

> **不变量**：注入并启动 `RedoFlushWorker` 后，只要有未刷 redo、worker 未停止且 `flush()` 成功，
> `flushedToDiskLsn` 会在一次周期 tick 或一次 `requestFlush()` 内覆盖本轮开始时观察到的 `currentLsn`；
> 空闲时不做冗余 fsync。

## 关键决策

- **复用现有 `RedoLogManager.flush()`，不改锁结构**：worker 调 `flush()` 走现有单 `lock`，与 append 的竞争和
  今天前台 flush 完全一样（不恶化已知简化）；`flush()` 已幂等。
- **沿用 `PageCleanerWorker` 形状**：`storage.redo.RedoFlushWorker` 拥有单 daemon 线程，`ReentrantLock` +
  `Condition` + 状态枚举（`RedoFlushWorkerState`），无 `synchronized`；周期 interval tick **或** `requestFlush()`
  合并唤醒后调 `flush()`。
- **空转保护**：tick 时若 `redo.currentLsn() <= redo.flushedToDiskLsn()`（无待刷）则跳过，避免每 tick 一次冗余 fsync。
- **失败即终止**（mirror PageCleanerWorker）：`flush()` 抛 `RedoLogIoException` → worker 进 FAILED 停机，不在失败
  fsync 上重试自旋；engine 可查状态。
- **`requestFlush()` 提供但不接 WAL-gate**：on-demand 唤醒能力本片实现 + 测试，但暂不接进淘汰/flush 等待路径
  （周期 tick 已足够解卡）；on-demand 接线留后续。
- **engine 接线**：`EngineConfig` 加 `redoFlushInterval`（Duration）；fresh 建库或 existing recovery 完成后、
  `StorageEngine` 发布 OPEN 前启动 `startBackgroundRedoFlusher()`（早于 page cleaner 使 durable LSN 先动）；
  `close()` 顺序改为 **停 page cleaner → 停 redo flush worker → 最终 `flushThrough`**（避免 worker 与关停刷盘竞争）。

## 非目标（明确推迟）

- 不拆 LSN 分配锁 vs write/flush 锁、不加 recent_written tracker（无并发 append 压力，留独立后续片）。
- 不修 checkpoint 的 closedLsn 近似（recent_closed tracker，独立正确性片）。
- 不动 trx commit durable policy（FLUSH_ON_COMMIT 等待，属 trx 层）。
- 不接 `requestFlush()` 进 WAL-gate 路径；不做 redo 文件轮转（0.18）。

## 验收测试

- `advancesDurableLsnPeriodically`：durable manager + append（`flushed=0 < current`）→ start 短 interval → await `flushedToDiskLsn` 前进到 `currentLsn`。
- `requestFlushTriggersImmediateFlush`：append → `requestFlush()` → 比周期更快 await 到 durable。
- `idleTickDoesNotFsyncOrAdvance`：无 pending → tick 不前进、不抛异常。
- `stopHaltsWorkerAndIsIdempotent`：stop 后不再刷、幂等、有界 join。
- `flushFailureMovesToFailedState`：注入会在 write/fsync 抛 `RedoLogIoException` 的 redo 仓储 → worker 进 FAILED（若注入太重则覆盖核心三条 + 文档化失败语义）。
- 若空闲计数/失败注入需要测试缝，可让 worker 依赖最小 `RedoFlushTarget` 端口；生产适配只委托 `RedoLogManager` 的
  `currentLsn()`/`flushedToDiskLsn()`/`flush()`，不得顺手引入 DurabilityPolicy 或锁拆分。
- 回归：现有 redo/flush/engine 全量不倒退。

## 文档更新要求

- Redo 数据链 / Capacity pressure 行：补"`StorageEngine` 启动 `RedoFlushWorker` 后台周期 `redo.flush()`，durable LSN 自动前进"。
- Redo Package Status：新增 `storage.redo` `RedoFlushWorker`/`RedoFlushWorkerState`（production-wired by StorageEngine）。
- Engine/Flush 生命周期行：补 redo flusher 的启动顺序、关闭顺序和诊断查询入口（若实现暴露）。
- Redo 缺口"无后台 redo flush / commit durable policy"行：改为"后台周期 flush 已接（`RedoFlushWorker`）；commit durable policy / 拆锁 / recent_written / recent_closed 仍缺"。
- Flush/全局缺口里凡是提到"无后台 redo flusher"处同步更新。
- `storage-backlog.md` 同步拆分 0.1：本片只关闭“后台 redo flusher”；closedLsn 修复、LSN 锁拆分、commit durable policy 保持独立后续项。
