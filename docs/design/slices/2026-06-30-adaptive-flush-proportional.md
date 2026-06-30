# Slice: 真 adaptive flush（§7.4 proportional，0.6a）

依据：`storage-backlog.md` 0.6（adaptive flush 部分）；`innodb-flush-checkpoint-doublewrite-design.md` §7.3/§7.4。

## 背景

`AdaptiveFlushPolicy.fixed` 把 redo capacity pressure 映射为**离散** batch（NONE→0 / ASYNC→min / SYNC→(min+max)/2 /
HARD→max），与实际脏页 backlog 无关：backlog 多时刷得不够、backlog 少时空跑。§7.4 设计版应按 backlog 比例刷：
`targetPages = clamp(basePages + pressureFactor·dirtyPagesBefore(targetLsn), minBatch, maxBatch)`。

## 目标

- `AdaptiveFlushPolicy` 增 `adaptive(basePages, minBatch, maxBatch)` 工厂 + proportional plan：按 pressure 取 factor
  （ASYNC 0.25 / SYNC 0.5 / HARD 1.0），`targetPages = clamp(basePages + round(factor·dirtyPagesBeforeTarget), min, max)`，
  `NONE→0`（容量驱动下无压力不刷）。
- `plan` 签名加 `dirtyPagesBeforeTarget`；`FlushService` 用 `bufferPool.dirtyPageCandidates(decision.targetCheckpointLsn(), capacity).size()`
  算 backlog 并传入（NONE 时跳过计数）。
- `StorageEngine` 生产改用 `adaptive(...)`（design 版）。

## 关键决策

- **保留 `fixed`（离散）** 作为确定性策略，供定向测试与故障隔离；production（engine）用 `adaptive`。`fixed.plan` 忽略
  `dirtyPagesBeforeTarget`，故既有用 `fixed` 构造的测试（FlushServiceCapacity/Drain、PageCleaner、UndoTruncate）零改动——
  FlushService 内部多传 backlog 对 fixed 透明。
- §7.4「第一阶段简化版」：只用 `dirtyPagesBeforeTarget` + per-pressure factor；redo 生成率/实际 flush 率/IO capacity/
  idle percent/neighbor flush 留后续阶段。
- backlog 计数复用 `dirtyPageCandidates(target, capacity).size()`，不新增 BufferPool API（零接口 blast radius）。
- 不做压力 throttle（0.6 Part B：前台反压需 2.1 commit 编排或 MTR-begin log_free_check）。

## 非目标

- 不做 throttle（SYNC 前台等 checkpoint / HARD 暂停 redo reservation）。
- 不引入 redo 生成率/flush 率/IO capacity/idle/neighbor 输入；不改 checkpoint/FlushCoordinator/WAL gate 语义。

## 验收测试

- `adaptiveScalesBatchWithDirtyBacklog`：adaptive(base=2,min=1,max=20)、SYNC，backlog 0/10/40 → 2/7/20。
- `adaptiveHardPressureScalesBacklogToMax`：HARD factor 1.0，backlog 15/50 → 17/20。
- `adaptiveNonePressureFlushesNothing`：NONE → 0、shouldFlush=false。
- `adaptiveFloorsToMinBatchAndClampsToRequestMax`：min floor + requestMax 上限。
- 保留 `fixed` 离散测试（改为 3 参 plan，离散值不变）。
- 回归：`FlushServiceCapacityTest`、`PageCleanerWorkerTest`、`FlushServiceDrainTest`（fixed 构造不变仍绿）。

## 文档更新要求

- `current-implementation-map.md`：`storage.flush.policy` 标 `adaptive`(§7.4 proportional) 已接 production、`fixed` 保留为离散；
  redo capacity 的 Known Gap 注明 throttle(0.6b) 仍缺。
- `storage-backlog.md`：0.6 标 adaptive flush(0.6a) 已落，剩压力 throttle(0.6b)。
- 代码注释说明 factor 含义、NONE 特例、第一阶段简化点（未用生成率/IO capacity）。
