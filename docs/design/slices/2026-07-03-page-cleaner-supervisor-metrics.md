# PageCleaner Supervisor + Metrics Snapshot

## Goal

- 给现有 `PageCleanerWorker` 增加一个轻量 supervisor，使后台 page cleaner 失败后有明确诊断和有限重启策略。
- 暴露 flush 后台线程的只读 metrics snapshot，用于测试、恢复诊断和后续 pressure/throttle 输入。
- 保持 page cleaner 仍只通过 `FlushService.flushForCapacity(maxPages)` 工作，不直接读取 Buffer Pool frame、PageStore 或 Redo 内部状态。
- 让 `StorageEngine` 后台 flush 生命周期从“裸 worker 启停”收敛为“supervisor 启停 worker”，shutdown 顺序仍可解释。

## Non-goals

- 不实现 0.6b redo capacity throttle，不暂停 redo reservation，不改变 MTR begin/commit 协议。
- 不实现 Performance Schema / SQL 视图，不接 session/executor/DD。
- 不改变 adaptive flush 算法、WAL gate、doublewrite、checkpoint safe LSN 规则。
- 不做多 page-cleaner 线程池、按 buffer-pool instance 分派或 IO capacity 动态调参。
- 不把 legacy `BufferPool.flush/flushAll` 迁移为 WAL-safe 生产入口。

## Key Decisions

- 新增 `PageCleanerSupervisor`，职责是创建/启动/停止当前 worker、观察 FAILED、按有限次数重建 worker，并维护 metrics；它不执行 flush IO。
- worker 创建通过 `PageCleanerWorkerFactory` 完成，便于测试注入会失败/会恢复的 worker，不让 supervisor 了解 `FlushService` 细节。
- 新增 `PageCleanerMetricsSnapshot` 不可变值对象，由 supervisor 汇总跨 worker 历史，至少包含：`state`、`restartCount`、`successfulCycles`、`failedCycles`、`lastCyclePresent`、`lastErrorMessage`、`lastStartedAtMillis`、`lastStoppedAtMillis`。
- `PageCleanerWorker` 保持当前状态机；只补最小只读 `snapshot()`，包含单调 `completedCycles`，不把重启逻辑塞入 worker 自身。
- `PageCleanerSupervisor` 自带一个轻量 monitor loop，周期性检查当前 worker 的 snapshot；发现 FAILED 后按策略停止旧 worker、记录 metrics、重建并启动新 worker。
- supervisor 的重启策略第一阶段固定为 `maxRestarts` + `restartBackoff`；失败超过上限后进入 FAILED，不再无限自旋。
- `StorageEngine.close` 顺序不变：先停止 page cleaner supervisor（monitor loop + 当前 worker），再停止 redo flusher，再 final flush/flushThrough；停止失败必须保留诊断但不能吞异常根因。

## Data Flow

- `StorageEngine.open` 构造 `PageCleanerSupervisor(factory,maxRestarts,backoff,monitorInterval)`，启动后 supervisor 创建并启动第一个 `PageCleanerWorker` 与 monitor loop。
- 正常 tick/request：调用方仍通过 supervisor 转发 `requestFlush(maxPages)`，worker 锁外执行 `FlushService.flushForCapacity`。
- 成功 cycle：worker 更新 `lastCycle` 和单调 `completedCycles`；monitor loop 按 `completedCycles` 差值累计成功计数，重启 worker 后历史计数仍保留在 supervisor。
- 失败 cycle：worker 进入 FAILED；monitor loop 记录 `lastErrorMessage`、增加失败计数，若未超过 `maxRestarts`，等待 backoff 后创建新 worker 并启动。
- shutdown：supervisor 设置 STOPPING，停止 monitor loop 和当前 worker，拒绝新请求，发布最终 metrics snapshot。

## Acceptance Tests

- `supervisorReportsMetricsAfterSuccessfulCycle`：一次后台 flush 成功后 snapshot 显示成功 cycle、无 last error、worker state 回到 IDLE。
- `supervisorRestartsFailedWorkerWithinLimit`：第一个 worker 抛 `DatabaseRuntimeException` 后 supervisor 记录失败并重启，第二个 worker 能继续处理 request。
- `supervisorStopsAfterRestartLimitExceeded`：连续失败超过 `maxRestarts` 后 supervisor 进入 FAILED，保留最后错误，后续 request 被拒绝。
- `supervisorStopTerminatesCurrentWorker`：stop 后 worker 线程退出，状态为 STOPPED，不留下后台线程。
- `storageEngineStartsPageCleanerThroughSupervisor`：启用 background flush 时 engine 持有 supervisor metrics；close 时按既有顺序停止。

## Current Map Update

- `Redo Log Layer Slice` 的 capacity pressure / background flush 行补充：`StorageEngine` 启动 PageCleanerSupervisor，worker 失败有有限重启和 metrics snapshot。
- `Buffer Pool + MiniTransaction Slice` 不新增生产边；只保留 flush 通过 BufferPool dirty view 协作的事实。
- `storage-backlog.md` 0.7 项标记 PageCleaner supervisor / metrics snapshot 已完成，保留 `DETECT_ONLY`、legacy flush 去留、RecoveryMode 扩展等其它碎片。

## Verification

- 先按 TDD 新增 supervisor 单元测试并确认 RED。
- 实现后运行 `gradle test --tests cn.zhangyis.db.storage.flush.cleaner.*`。
- 再运行 `gradle test --tests cn.zhangyis.db.storage.flush.* --tests cn.zhangyis.db.storage.engine.*`。
- 最后运行全量 `gradle test`。
- 静态扫描生产代码不新增 `synchronized/wait/notify/notifyAll` 或裸 `IllegalArgumentException/RuntimeException`。

## Ten-pass Self-check

- Pass 1 scope: 单切片只覆盖 page cleaner supervisor 和 metrics，不扩大到 throttle、DD/DML 或多 worker。
- Pass 2 design authority: 依据 `innodb-flush-checkpoint-doublewrite-design.md` page cleaner / metrics 目标，当前状态以 `current-implementation-map.md` 为准。
- Pass 3 layering: supervisor 依赖 flush worker/factory 和 metrics DTO，不读取 Buffer Pool、Redo、PageStore 内部。
- Pass 4 concurrency: worker 继续使用显式 `ReentrantLock + Condition`；supervisor 停止、重启、request 必须有 timeout/状态边界。
- Pass 5 recovery semantics: 不改变 WAL、doublewrite、checkpoint、crash recovery 顺序。
- Pass 6 lifecycle: `StorageEngine` close 顺序保持 page cleaner -> redo flusher -> final flush，不引入反向依赖。
- Pass 7 failure policy: restart 有上限和 backoff，避免失败 worker 无限自旋或吞掉根因。
- Pass 8 tests: 验收测试覆盖成功、失败重启、超限失败、stop、engine 接线。
- Pass 9 docs: 明确 current map 与 backlog 更新点，不把未做的 0.7 碎片写成完成。
- Pass 10 placeholders: 文档无 TBD/TODO/待定，所有非目标和验收标准可执行。
