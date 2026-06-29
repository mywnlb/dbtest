# Slice: redo closed LSN 与 recent tracker

依据：`storage-backlog.md` 0.1 剩余项；`innodb-redo-log-design.md` §5.5/§5.6、§8.3、§9、§15；
`innodb-flush-checkpoint-doublewrite-design.md` 的 checkpoint/WAL gate 约束。

## 背景

当前 `RedoLogManager.append` 与 `flush` 共用同一把 `lock`，`flush()` 在锁内写文件并 fsync，后台 flusher
虽然已接，但一次 fsync 仍会阻塞所有 append。`flush.checkpoint.CheckpointCoordinator` 也仍用
`redo.currentLsn()` 近似 closed LSN；当 MTR 已分配 redo LSN、但 dirty page 尚未完成发布到 flush list 时，
checkpoint 理论上可能越过未关闭区间。

## 目标

- 在 `storage.redo` 内建立可测试的 recent written / recent closed 连续边界。
- 拆分 LSN 分配临界区与 redo write/flush 临界区，使 append 不被 fsync 长时间持锁阻塞。
- 增加 `RedoLogManager.closedLsn()`，checkpoint 安全边界改为
  `min(bufferPool.oldestDirtyLsnOr(...), redo.closedLsn(), redo.flushedToDiskLsn())`。
- `MiniTransaction.commit` 在 redo append、pageLSN 盖戳、dirty 发布完成后显式关闭本次 `LogRange`。

## 关键决策

- 新增小型区间追踪器，例如 `ContiguousLsnTracker`：记录乱序完成的 `[start,end)`，只在头部连续时推进边界。
- `recentWritten` 表示 redo batch 已进入可写/待写队列的连续边界；当前同步 append 模型下可随 append 推进，
  但接口保留给后续 writer 线程。
- `recentClosed` 只在 MTR 完成 dirty page 发布后推进；checkpoint 只能读取该连续边界。
- `append` 仍返回 `LogRange`，但不再隐含“可 checkpoint”；调用方必须在发布 dirty 后 `markClosed(range)`。
- 现有测试和恢复 setup 中有不少直接 `redo.append(...)` 的调用；本片需要把这些调用点改成显式
  `markClosed(range)`，或只在测试夹具里提供命名清楚的 closed-append helper，避免把生产语义藏回 `append`。
- 即使当前 dirty view 为空，checkpoint 也只能推进到 `min(closedLsn, flushedLsn)`，避免越过 append 后尚未
  发布 dirty 的 MTR 窗口；closed 覆盖 flushed 时才等价于 flushed。
- 空 redo batch 返回退化区间，`markClosed` 对退化区间为 no-op，避免无写 MTR 干扰边界。
- `restoreRecoveredBoundary` 需要同时恢复 `currentLsn`、`flushedToDiskLsn`、`closedLsn` 和 tracker 起点。
- 所有等待继续使用 `ReentrantLock`/`Condition`，不得引入 `synchronized` 或无界等待。

## 非目标

- 不做 `FLUSH_ON_COMMIT` / `WRITE_ON_COMMIT` / `BACKGROUND_FLUSH` 的 DurabilityPolicy。
- 不做 redo 文件轮转/回收、log block header/trailer checksum。
- 不把 checkpoint worker 改成真正后台线程；本片只修正它读取的安全边界。
- 不重写 redo record 格式，不引入 btree/undo 逻辑 redo handler。

## 验收测试

- `closedLsnDoesNotAdvanceUntilMtrPublishesDirtyPages`：append 后未 mark closed 时 checkpoint 不能越过旧 closed。
- `recentClosedMergesOutOfOrderRanges`：先关闭后段、再关闭前段，`closedLsn` 只在连续后推进。
- `checkpointUsesClosedLsnInsteadOfCurrentLsn`：`current > closed` 且 redo durable 时，safe checkpoint 仍等于 closed。
- `restoreRecoveredBoundaryRestoresClosedBoundary`：恢复后新 checkpoint/append 从 recoveredTo 连续运行。
- `appendDoesNotHoldFlushFsyncCriticalSection`：用阻塞/慢 flusher 注入，验证 append 不等待 fsync 所持锁。
- 回归：`RedoLogManagerTest`、`CheckpointCoordinatorTest`、`MtrRedoAppendTest`、engine recovery/flush 相关测试通过。

## 文档更新要求

- `current-implementation-map.md`：Redo 数据链、Checkpoint advance、Outstanding Gaps 的 currentLsn 近似说明改为 closedLsn。
- `storage-backlog.md`：0.1 标记 closedLsn/recent tracker/拆锁完成，保留 commit durable policy 独立后续项。
- 代码注释需说明 `markClosed` 必须发生在 dirty page 发布之后，否则 checkpoint 会丢失 crash recovery 所需 redo。
