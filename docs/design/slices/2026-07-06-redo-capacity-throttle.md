# Slice: redo capacity 压力 throttle（0.6b）

依据：`storage-backlog.md` 0.6/0.18；`innodb-flush-checkpoint-doublewrite-design.md` §7.3/§7.4；
`innodb-redo-log-design.md` §5.6、§11。前置：0.6a adaptive flush、0.18 redo 文件环、0.20a DurabilityPolicy 已落。

## 背景

当前 redo capacity pressure 只驱动后台 PageCleaner：`StorageEngine` 周期调用 `FlushService.flushForCapacity`，
`RedoCapacityPolicy` 输出 NONE/ASYNC_FLUSH/SYNC_FLUSH/HARD_LIMIT 后由 adaptive flush 决定刷页数量。前台 append / MTR commit 不等待
checkpoint 或 redo reclaim boundary 前进；redo 文件环写满时仍由 `RotatingRedoLogRepository` fail-closed 抛
`RedoLogCapacityExceededException`。这能保护恢复安全，但在 SYNC_FLUSH/HARD_LIMIT 压力下缺少 InnoDB 风格前台反压。

## 目标

- 新增前台 `RedoCapacityThrottle`：在 redo append/reservation 前用 `RedoCapacityPolicy.evaluate`
  检查 checkpoint age，并按 pressure 执行动作。
- `SYNC_FLUSH`：请求 redo flush + page cleaner/flush service 推进到 target checkpoint，等待 checkpoint/reclaim boundary
  前进；等待必须带 timeout，且返回前重新评估 pressure。
- `HARD_LIMIT`：暂停新的 redo reservation/append，触发同样推进流程；若 timeout 后仍 HARD_LIMIT，则抛领域异常，不能覆盖未
  checkpoint 的 redo。
- `ASYNC_FLUSH`：只发 on-demand flush/page-cleaner request，不阻塞当前前台。
- `StorageEngine` 生产接线：durable redo + 文件环默认路径启用 throttle；内存 redo 或测试专用构造可显式 no-op。

## 关键决策

- throttle 放在 redo/engine 边界，不让 redo 文件仓储反向依赖 BufferPool/FlushCoordinator。文件环仍只负责安全写入和
  fail-closed，等待与 flush 编排由 engine 注入协作者完成。
- 等待点必须在进入 redo append IO、Buffer Pool page latch、frame mutex、FSP/fil lease 之前；若某调用路径已经持页 latch，
  必须改成更早的 MTR begin/log-free-check 入口，避免 page latch 等待 checkpoint 导致锁链放大。
- 第一阶段只用 checkpoint age/reclaim boundary，不引入 redo 生成率、IO capacity、neighbor flush 或多 page cleaner。
- HARD_LIMIT timeout 仍保留 fail-closed 语义；throttle 是“先等待再失败”，不是静默覆盖、扩容或降低 durable 边界。

## 非目标

- 不做动态 redo capacity resize，不改 redo 文件格式，不做 LogBlock header/trailer checksum（0.20b）。
- 不实现多 page-cleaner 线程池、复杂 IO capacity 自适应或 group commit。
- 不接 DD/session/executor；不改变 DurabilityPolicy 的 commit 等待语义，只保证 append/reservation 前有容量反压。

## 验收测试

- `asyncPressureRequestsCleanerWithoutBlocking`：ASYNC_FLUSH 只触发 request，前台继续 append。
- `syncPressureWaitsUntilCheckpointAdvances`：SYNC_FLUSH 下等待 checkpoint/reclaim boundary 前进后放行。
- `hardPressureBlocksAppendAndTimesOutFailClosed`：HARD_LIMIT 下等待超时抛领域异常，redo 文件不覆盖未 checkpoint 区间。
- `hardPressureReleasesAfterFlushProgress`：后台 flush 推进 checkpoint 后，HARD_LIMIT 等待者被唤醒并成功 append。
- `throttleDoesNotWaitWhileHoldingPageLatch`：MTR/BufferPool 路径验证等待发生在 page latch/frame lock 之前。
- 回归：`RedoLogManagerTest`、`RotatingRedoLogRepositoryTest`、`FlushServiceCapacityTest`、`StorageEngineTest`。

## 文档更新要求

- `current-implementation-map.md`：Capacity pressure 行从“无 0.6b throttle”改为前台 throttle 已接，并写清等待入口、
  timeout/fail-closed、与 PageCleaner/FlushService/RedoReclaimBoundary 的真实调用链。
- `storage-backlog.md`：0.6 标 0.6b 已落；0.18/Redo 残留从“容量 throttle”移除，保留 LogBlock checksum/逻辑 redo。
- 代码注释说明：SYNC_FLUSH/HARD_LIMIT 等待不持 page latch/frame lock/FSP lease；HARD_LIMIT timeout 仍禁止覆盖恢复所需 redo。
