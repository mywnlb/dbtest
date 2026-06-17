# Spec: F2 - page cleaner, adaptive flush, and tablespace drain

- 日期：2026-06-16
- 关联设计：`docs/design/innodb-flush-checkpoint-doublewrite-design.md` §3-§11、§14-§16；`docs/design/innodb-buffer-pool-design.md` §15、§18；`docs/design/innodb-redo-log-design.md` §10-§11；`docs/design/innodb-crash-recovery-design.md` §7.1、§8.4。
- 前置：F1 WAL-gated `FlushCoordinator` + doublewrite + checkpoint；R2 persistent checkpoint label + redo capacity pressure。
- 状态：把 F1 的同步 flush 原语扩展为可调度的 flush service、简化 adaptive flush policy 和可控后台 page cleaner。

## 1. 范围

**做：**

- 新增 `FlushService` 门面：封装 `FlushCoordinator`、`CheckpointCoordinator`、`BufferPool`、`RedoLogManager` 和 `RedoCapacityPolicy`。
- 新增 `AdaptiveFlushPolicy`：把 R2 `RedoCapacityDecision` 映射为本轮 flush target LSN、目标页数和是否进入同步压力。
- 新增 `PageCleanerWorker`：显式 `start/request/awaitIdle/stop` 的后台 worker，用超时等待和有界队列，不使用 Java monitor。
- 新增 `drainTablespace(SpaceId, Duration)`：按 tablespace 过滤 dirty candidates，循环 single page flush，直到目标 space 无 dirty 页或 timeout。
- 新增 flush cycle 结果对象：记录本轮 pressure、flush 结果、checkpoint 前后 LSN、是否 timed out。

**不做：**

- 不做多 buffer pool instance / 多 page cleaner pool。
- 不做 neighbor flush、LRU dirty tail 专用选择器、flush 速率统计滑窗。
- 不做 redo append 限流、redo 循环文件回收、checkpoint fileId+offset。
- 不做 drop/truncate/discard 的 lifecycle X latch 集成；F2 drain 只提供 flush 层能力。

## 2. 关键决策

1. **FlushService 是门面，不替代 FlushCoordinator**：所有 page IO 仍由 F1 `FlushCoordinator` 做 WAL gate、doublewrite、data file write 和 clean/keep-dirty 回调。
2. **Adaptive policy 先做确定性映射**：`NONE -> 0`，`ASYNC -> minBatch`，`SYNC -> midpoint`，`HARD -> maxBatch`。后续可把 redo generation rate 和 flush rate 加入策略，不改 service API。
3. **PageCleanerWorker 可控启动和停止**：测试和后续 engine startup 必须能显式 stop；等待使用 `Condition.awaitNanos`，每次等待都带 timeout 或 idle interval。
4. **Drain 不新增 BufferPool API**：F2 使用 `dirtyPageCandidates(Long.MAX, bufferPool.capacity())` 取得当前 dirty 快照后按 `SpaceId` 过滤。capacity 是当前实现可驻留 dirty 页上界，避免改动 BufferPool 接口。
5. **Checkpoint 在 service 层推进**：每轮 flush 后调用 `CheckpointCoordinator.advanceCheckpoint()`；如果 dirty oldest 前移，R2 checkpoint label 可持久化前进。

## 3. 数据流

Adaptive flush:
`FlushService.flushForCapacity(maxPages)` -> `RedoCapacityPolicy.evaluate(redo.currentLsn(), checkpoint.lastCheckpointLsn())` -> `AdaptiveFlushPolicy.plan(decision, maxPages)` -> `FlushCoordinator.flushList(targetLsn, pages)` -> `CheckpointCoordinator.advanceCheckpoint()`.

Page cleaner:
`PageCleanerWorker.requestFlush(maxPages)` -> worker thread wakes -> `FlushService.flushForCapacity(maxPages)` -> records last cycle -> signals idle.

Tablespace drain:
`FlushService.drainTablespace(spaceId, timeout)` -> repeatedly select dirty candidates for that space -> `singlePageFlush(page)` -> checkpoint advance -> stop when no candidates remain or timeout.

## 4. 测试

- `AdaptiveFlushPolicyTest`：pressure 到 page count/target LSN 的确定性映射；非法 batch 配置拒绝。
- `FlushServiceCapacityTest`：capacity pressure 触发 flushList 并推进 checkpoint；`NONE` 不刷页。
- `FlushServiceDrainTest`：只 drain 指定 tablespace 的 dirty 页，其他 space 保持 dirty；timeout 时返回 timedOut。
- `PageCleanerWorkerTest`：request 后后台 worker 执行 flush cycle；stop 后拒绝新请求并能在 timeout 内退出。

## 5. 简化点与后续

- F2 adaptive flush 不计算真实 flush rate 和 dirty ratio；只接入 R2 capacity pressure。
- F2 drain 不持有 tablespace lifecycle X latch；drop/truncate 集成时必须由上层先阻止新写入，再调用 drain。
- F2 worker 只支持一个后台线程；后续多实例 buffer pool 可扩展为 worker pool。
