# MiniMySQL InnoDB 风格 Flush、Doublewrite 与 Checkpoint 协调器设计

版本：2026-06-05  
实现语言：Java  
参考基线：MySQL 8.0.46 Buffer Pool Flushing、Doublewrite Buffer、InnoDB Checkpoints、Redo Log  
关联设计：[innodb-crash-recovery-design.md](innodb-crash-recovery-design.md)、[innodb-storage-engine-overview.md](innodb-storage-engine-overview.md)、[innodb-buffer-pool-design.md](innodb-buffer-pool-design.md)、[innodb-redo-log-design.md](innodb-redo-log-design.md)、[innodb-disk-manager-design.md](innodb-disk-manager-design.md)

## 1. 目标与边界

本设计面向 MiniMySQL 的 `storage.flush` 模块。它不是 Buffer Pool、Redo 或 Disk Manager 的替代品，而是协调 dirty page 刷盘、doublewrite 保护、checkpoint 推进、redo capacity pressure 和 page cleaner 后台工作线程的跨模块协调器。

设计目标：

- 高内聚：dirty page 选择、flush 调度、page cleaner、doublewrite、checkpoint pressure、flush metrics 都收敛在 `storage.flush`。
- 低耦合：只通过 Buffer Pool dirty view、Redo checkpoint API、Disk PageStore API 协作，不读取内部链表、redo buffer 或裸文件结构。
- MySQL 8.0 风格：对齐 page cleaner、flush list、LRU flush、adaptive flushing、doublewrite files、detect-only/detect-and-recover、fuzzy checkpoint。
- 可恢复：写 data file 前先满足 redo durable，再通过 doublewrite 防 torn page；recovery 先修复 partial page write，再 redo replay。
- 并发安全：明确 page cleaner、redo wait、doublewrite file lock、PageStore physical file lock、Buffer Pool page latch 的状态和所有权变化。
- 可测试：支持故障注入验证 redo durable 边界、doublewrite recovery、checkpoint 不越界和并发 flush。

非目标：

- 不维护 Buffer Pool frame、LRU、free list、flush list 的内部结构。
- 不分配 tablespace、segment、extent 或 page。
- 不编码 redo record，不写 redo file。
- 不执行事务 commit/rollback，不进入事务行锁等待。
- 不实现完整 crash recovery 总控；只定义 doublewrite 修复和 checkpoint 输入输出，恢复编排由 Recovery 模块负责。
- 不生成 Java 源码。本文件只定义设计、关系和数据流。

## 2. MySQL 8.0 参考依据

本设计参考 MySQL 8.0 的以下行为：

- InnoDB 后台 page cleaner 线程执行 Buffer Pool dirty page flushing；线程数量由 `innodb_page_cleaners` 控制，并与 buffer pool instance 关联。
- Dirty page flushing 受 dirty page 比例、LRU free page 压力、redo log 生成速度和 redo capacity pressure 影响。
- Adaptive flushing 根据 redo log 生成速度和当前 flush 速度动态调整刷脏速率，避免 redo 空间耗尽导致 sharp checkpoint。
- Doublewrite buffer 在数据页写入最终 data file 位置前保存副本；MySQL 8.0.20 起 doublewrite 存储区域在 doublewrite files 中。
- MySQL 8.0.30 起 doublewrite 支持 `DETECT_AND_RECOVER` 和 `DETECT_ONLY` 模式；detect-only 只写 metadata，不能用副本修复 page 内容。
- Doublewrite 可为 flush list 和 LRU/single page flush 使用不同文件或槽位策略。
- Fuzzy checkpoint 不要求一次性刷完全部脏页；checkpoint LSN 只能推进到 redo durable、redo closed 和 dirty page 边界共同允许的位置。

简化点：

| MySQL 8.0 能力 | MiniMySQL 第一阶段 |
| --- | --- |
| 多 page cleaner 与复杂 IO capacity 自适应 | 支持 page cleaner pool 和简化 adaptive policy |
| Doublewrite file 数量按 instance 和 page size 细分 | 先按 flush list file 和 LRU file 两类抽象 |
| 加密/压缩 tablespace 的 doublewrite page 处理 | 保留 `DoublewritePageCodec` 扩展点 |
| 完整 shutdown fast/slow 语义 | 第一阶段支持 normal shutdown flush 和 force shutdown boundary |
| 完整 Performance Schema 指标 | 先提供 `FlushMetricsSnapshot` |

## 3. 总体架构

架构图见 [flush-checkpoint-architecture.mmd](diagrams/flush-checkpoint-architecture.mmd)。

核心协作链路：

`FlushCoordinator -> BufferPool DirtyView -> RedoLogManager -> DoublewriteService -> PageStore -> CheckpointCoordinator`

职责划分：

- Buffer Pool 提供 dirty candidates、frame fix、page snapshot、mark clean/failed，不决定最终刷盘策略。
- Redo 提供 `flushedToDiskLsn`、`closedLsn`、checkpoint label 写入和 checkpoint pressure，不写 data page。
- Disk Manager 提供 `PageStore.writePage()`、data file fsync 和 physical file locks，不理解 dirty LSN。
- Flush 模块负责编排顺序、批量、策略、doublewrite、checkpoint 反馈和指标。
- Recovery 模块读取 doublewrite 副本和 checkpoint label，Flush 模块只提供恢复工具和数据格式。

## 4. 包与职责

| 包 | 职责 | 依赖 | 主要模式 |
| --- | --- | --- | --- |
| `storage.flush.api` | `FlushService`、请求入口、shutdown flush | scheduler | Facade |
| `storage.flush.coordinator` | 调度 flush batch、处理结果、协调 checkpoint pressure | buf, redo, fil | Mediator |
| `storage.flush.cleaner` | page cleaner worker loop、后台刷脏 | coordinator | Background Worker |
| `storage.flush.selector` | flush list、LRU、single page 候选选择 | buf dirty view | Strategy |
| `storage.flush.snapshot` | frame fix、page image snapshot、checksum、pageLSN 校验 | buf | RAII Guard, Snapshot |
| `storage.flush.doublewrite` | doublewrite file、slot、batch fsync、detect/recover | fil | Strategy, Repository |
| `storage.flush.checkpoint` | safe checkpoint LSN 计算、pressure、label 请求 | redo, buf | Policy, Mediator |
| `storage.flush.policy` | adaptive flush、neighbor flush、IO capacity、batch size | metrics | Strategy |
| `storage.flush.recovery` | doublewrite slot 扫描和 torn page 修复辅助 | fil, redo | Recovery Handler |
| `storage.flush.metric` | flush rate、latency、lag、dirty ratio、checkpoint age | 无 | Observer |

## 5. 核心领域模型

类关系图见 [flush-checkpoint-class-relation.mmd](diagrams/flush-checkpoint-class-relation.mmd)。

| 对象 | 职责 |
| --- | --- |
| `FlushService` | 对外门面，接收 LRU/list/single/shutdown flush 请求 |
| `FlushCoordinator` | 聚合调度、worker、checkpoint 和结果处理 |
| `PageCleanerWorker` | 后台 worker，循环执行 batch flush |
| `DirtyPageSelector` | 从 Buffer Pool dirty view 选择候选 page |
| `FlushBatch` | 一批按类型、target LSN、instance 分组的 flush 任务 |
| `FlushSnapshot` | 稳定 page image、pageLSN、checksum、space version |
| `DoublewriteService` | doublewrite slot 分配、写副本、fsync、恢复扫描 |
| `CheckpointCoordinator` | safe checkpoint LSN 计算、pressure、label 持久化 |
| `FlushPolicy` | IO capacity、adaptive flushing、neighbor flushing 策略 |
| `FlushResult` | clean、kept dirty、skipped、failed、retryable 等结果 |

## 6. 关键数据结构与逻辑/物理区分

### 6.1 Flush 类型

| 类型 | 触发条件 | 候选来源 | 目标 |
| --- | --- | --- | --- |
| `FLUSH_LIST` | checkpoint pressure、redo capacity pressure | Buffer Pool flush list | 推进 oldest dirty LSN |
| `LRU_FLUSH` | free frame 不足、LRU dirty tail | LRU dirty candidate | 腾出 clean frame |
| `SINGLE_PAGE_FLUSH` | 前台极端容量压力 | 指定 page | 避免长时间阻塞 |
| `SHUTDOWN_FLUSH` | normal shutdown | 全部 dirty view | 尽量减少 recovery 工作 |
| `TABLESPACE_DRAIN` | drop/truncate/discard | 指定 tablespace dirty page | 关闭或截断前 drain |

### 6.2 逻辑与物理边界

| 层面 | 对象 | 所属模块 | Flush 模块职责 |
| --- | --- | --- | --- |
| 逻辑 dirty 元数据 | `oldestModificationLsn`, `newestModificationLsn`, `inFlushList` | Buffer Pool | 只读候选，写结果回调 |
| 物理 page image | `FlushSnapshot` | Flush snapshot | 复制稳定 page image 并计算 checksum |
| WAL 边界 | `flushedToDiskLsn`, `closedLsn` | Redo | 校验数据页写盘前 redo durable |
| Doublewrite 副本 | doublewrite slot/file | Flush doublewrite | 先写副本，再写 data file |
| Data file page | tablespace page offset | Disk Manager | 通过 PageStore 写入，不直接操作 file channel |
| Checkpoint label | checkpoint LSN 和 metadata | Redo checkpoint | 提供 safe LSN，调用 redo 持久化 label |

## 7. 核心策略和算法

### 7.1 Dirty Page Flush

Dirty page flush 流程见 [dirty-page-flush-flow.mmd](diagrams/dirty-page-flush-flow.mmd)。

标准顺序：

1. 选择候选 page。
2. 读取 pageLSN 和 redo `flushedToDiskLsn`。
3. 如果 `pageLSN > flushedToDiskLsn`，等待 redo flush 或跳过候选。
4. 固定 frame 或复制 page image。
5. 校验 page type、space version、pageLSN。
6. 写 checksum。
7. 写 doublewrite 副本并按策略 fsync。
8. 写 tablespace data file。
9. 成功后回调 Buffer Pool：未再修改则 clean；被再次修改则保留 dirty 并推进 flushed page LSN。
10. 通知 checkpoint coordinator 重新计算边界。

关键不变量：

`data page write` 必须发生在 `redo durable` 之后。  
`checkpoint LSN` 不能越过仍在 dirty view 中的最老修改。  
`DIRTY_PENDING` 不能被 flush。  
flush 失败不能把 frame 标记为 clean。

### 7.2 Doublewrite

Doublewrite 流程见 [doublewrite-flow.mmd](diagrams/doublewrite-flow.mmd)。

模式：

| 模式 | 行为 | 恢复能力 |
| --- | --- | --- |
| `OFF` | 不写 doublewrite，直接写 data file | 无 torn page 修复 |
| `DETECT_ONLY` | 写 page id、LSN、checksum 等 metadata | 可检测 torn page，不能用副本修复 |
| `DETECT_AND_RECOVER` | 写完整 page 副本 | 可从 doublewrite 修复 torn page |

文件策略：

- `FlushListDoublewriteFile`：面向 flush list batch。
- `LruDoublewriteFile`：面向 LRU flush 和 single page flush。
- `DoublewriteSlot`：保存 page id、space version、pageLSN、checksum、payload pointer。
- `DoublewriteBatch`：按连续 slot 聚合写入，减少随机 IO。

### 7.3 Checkpoint

Checkpoint 流程见 [checkpoint-coordinator-flow.mmd](diagrams/checkpoint-coordinator-flow.mmd)。

安全 checkpoint LSN：

`safeCheckpointLsn = min(bufferPool.oldestDirtyLsnOrCurrent, redo.closedLsn, redo.flushedLsn)`

pressure 策略：

| 条件 | 行为 |
| --- | --- |
| dirty ratio 超过 low water mark | 后台 flush list 小批量刷出 |
| checkpoint age 超过 async threshold | 提高 page cleaner 目标 |
| checkpoint age 超过 sync threshold | 前台 MTR commit 可等待 checkpoint 进展 |
| checkpoint age 接近 hard limit | 暂停新 redo reservation，强制 flush |
| shutdown | 进入 shutdown flush，尽量清空 dirty view |

禁止行为：

- checkpoint 不能只根据 redo flushed LSN 推进。
- checkpoint 不能在持有 Buffer Pool list lock 时等待 page latch。
- checkpoint 不能直接写 data page。
- checkpoint 写 label 失败时保持旧 checkpoint，不发布更高 LSN。

### 7.4 Adaptive Flush

输入：

- redo generation rate。
- current flush rate。
- dirty page ratio。
- checkpoint age。
- IO capacity 和 IO capacity max。
- idle flush percent。

输出：

- 每轮 flush target pages，并在 free frame 紧张时拆分 FlushList/LRU batch。
- flush list 与 LRU flush 配比。
- 是否启用 neighbor flush。
- 是否允许 foreground single page flush。

第一阶段采用简化 policy：

`targetPages = clamp(basePages + pressureFactor * dirtyPagesBefore(targetLsn), minBatch, maxBatch)`

## 8. 与其它模块的协作

### 8.1 与 Buffer Pool

- Flush 通过 dirty view 查询候选，不直接遍历内部链表。
- Flush snapshot 由 Buffer Pool 提供 frame fix 或 page image copy。
- Flush 完成后只能通过 `markFlushDone/Failed` 回写状态。
- Buffer Pool 仍负责 `DIRTY_PENDING -> DIRTY -> FLUSHING -> CLEAN/DIRTY` 状态机。

### 8.2 与 Redo

- Flush 写 data file 前必须调用 `redo.waitFlushed(pageLsn)` 或确认 `flushedToDiskLsn >= pageLsn`。
- Redo checkpoint coordinator 读取 flush 进展，但不写数据页。
- Flush pressure 可由 redo capacity 触发。
- Redo writer/flusher 不回调 Buffer Pool 内部结构，只通过 FlushService 请求刷脏。

### 8.3 与 Disk Manager

- Flush 只通过 `PageStore.writePage()` 写 data file。
- Physical file lock 顺序由 Disk Manager 维护。
- Flush 不能在持有 Buffer Pool list/hash 锁时获取物理文件锁。
- Drop/truncate/discard 通过 `TABLESPACE_DRAIN` 请求 drain dirty page。

### 8.4 与 Recovery

Doublewrite recovery 流程见 [doublewrite-recovery-flow.mmd](diagrams/doublewrite-recovery-flow.mmd)。

- Recovery 启动时先发现 doublewrite files。
- 检测到 torn page 时，优先使用 doublewrite full copy 修复。
- detect-only 模式只能报告可疑 page，后续依赖 redo replay；无法修复时标记 tablespace corrupted。
- page 修复后 redo replay 仍通过 pageLSN 幂等判断。

## 9. 并发与锁顺序

并发状态图见 [flush-concurrency-state.mmd](diagrams/flush-concurrency-state.mmd)。

### 9.1 锁与等待对象

| 对象 | 所属模块 | Flush 是否持有 | 死锁域 |
| --- | --- | --- | --- |
| `flushQueueLock` | Flush scheduler | 短持有 | timeout/retry |
| `pageCleanerPermit` | Flush cleaner | worker 持有 | 不进入死锁图 |
| `frameMutex` / frame fix | Buffer Pool | 短持有或通过 guard | timeout/retry |
| `pageLatch` | Buffer Pool | 可短持有或用 snapshot 代替 | timeout/retry |
| `redoWaitSlot` | Redo | 等待 redo durable 时持有 | error broadcast/timeout |
| `DoublewriteFileLock` | Flush doublewrite | 写 doublewrite batch 时持有 | timeout/error |
| `PageStore` physical file locks | Disk Manager | 通过 PageStore 间接持有 | timeout/error |
| transaction row lock | Transaction | 不持有 | Wait-For Graph |

### 9.2 状态与持有变化

| 状态 | 持有者 | 持有资源 | 进入条件 | 退出条件 |
| --- | --- | --- | --- | --- |
| `SELECTING` | page cleaner | flush queue permit、dirty view cursor | 收到 flush request | 选出 batch 或无候选 |
| `REDO_WAIT` | page cleaner | redo wait slot，不持有 page latch/file lock | pageLSN 尚未 durable | redo flushed、timeout、skip |
| `SNAPSHOTTING` | page cleaner / Buffer Pool guard | frame fix 或 page image copy | redo durable | snapshot 完成 |
| `DOUBLEWRITE_WRITING` | page cleaner | doublewrite slot/file lock | snapshot ready | doublewrite fsync 完成或失败 |
| `DATAFILE_WRITING` | PageStore | physical file locks | doublewrite durable | data file write 完成 |
| `VALIDATING` | page cleaner | frame metadata 短锁 | data write 完成 | clean 或 keep dirty |
| `MARK_CLEAN` | Buffer Pool | frame metadata update | frame 未再修改 | 从 flush list 移除 |
| `KEEP_DIRTY` | Buffer Pool | frame metadata update | frame 有更新修改 | 保留 dirty |
| `CHECKPOINT_NOTIFY` | Flush/Redo | checkpoint event | flush result 发布 | checkpoint 重新计算 |
| `RELEASED` | 无 | 无 flush 短锁 | batch 结束 | 返回 worker loop |

持有变化规则：

- `select`：选择 dirty candidate 时只短持 dirty view 游标，不跨 IO。
- `redo wait`：等待 redo durable 前必须释放 Buffer Pool list/hash/frame locks 和 page latch。
- `snapshot`：获取 page image 后释放 page latch，再进入 doublewrite 和 data file IO。
- `doublewrite`：持有 doublewrite file lock 时不能请求 Buffer Pool page latch。
- `data write`：PageStore 持有物理文件锁时不能进入事务锁、MDL 或 Buffer Pool list wait。
- `checkpoint notify`：只发布 LSN 事件，不反向持有 data file lock。

## 10. 异常处理

异常类型：

- `FlushQueueFullException`
- `RedoNotDurableException`
- `FlushSnapshotInvalidException`
- `DoublewriteWriteException`
- `DoublewriteRecoverException`
- `DataFileFlushException`
- `CheckpointAdvanceException`
- `PageCleanerStoppedException`
- `FlushTimeoutException`

异常策略：

- redo 未 durable：按策略等待、跳过或触发 redo flush。
- snapshot 失效：重新选择候选或保留 dirty。
- doublewrite 写失败：不写 data file，标记 flush failed。
- data file 写失败：保留 dirty，广播 IO error，必要时进入只读/failed 状态。
- checkpoint label 写失败：保持旧 checkpoint，继续运行或在 hard pressure 下阻止新写入。
- shutdown flush 失败：记录不可 clean page，交给 recovery 处理。

## 11. API 设计

### 11.1 FlushService

- `requestFlush(FlushRequest request)`
- `flushList(Lsn targetLsn, int maxPages)`
- `flushLru(BufferPoolInstanceId instanceId, int maxPages)`
- `singlePageFlush(PageId pageId)`
- `drainTablespace(SpaceId spaceId, Duration timeout)`
- `shutdownFlush(ShutdownMode mode)`
- `metricsSnapshot()`

### 11.2 DirtyPageSelector

- `selectFlushList(Lsn targetLsn, int maxPages)`
- `selectLru(BufferPoolInstanceId instanceId, int maxPages)`
- `selectSingle(PageId pageId)`
- `selectTablespace(SpaceId spaceId, int maxPages)`

### 11.3 DoublewriteService

- `writeBeforeDataFile(FlushSnapshot snapshot)`
- `markDataFileWriteDone(FlushSnapshot snapshot)`
- `recoverTornPage(PageId pageId, PageImage brokenImage)`
- `scanDoublewriteFiles(RecoveryMode mode)`
- `mode()`

### 11.4 CheckpointCoordinator

- `computeSafeCheckpointLsn()`
- `requestCheckpoint(CheckpointReason reason)`
- `onFlushResult(FlushResult result)`
- `checkpointPressure()`
- `persistCheckpointLabel(Lsn checkpointLsn)`

## 12. 设计模式使用清单

| 模式 | 应用点 | 价值 |
| --- | --- | --- |
| Facade | `FlushService` | 隐藏 selector、worker、doublewrite、checkpoint |
| Mediator | `FlushCoordinator` | 协调 Buffer Pool、Redo、Disk |
| Strategy | `FlushPolicy`、`DoublewriteStrategy`、`NeighborFlushPolicy` | 支持不同刷盘和可靠性策略 |
| Snapshot | `FlushSnapshot` | 固定 page image，避免跨 IO 持有 page latch |
| RAII Guard | frame fix、flush snapshot guard | 异常路径释放短资源 |
| Observer | `FlushResult` 通知 checkpoint 和 metrics | 解耦刷盘结果与边界推进 |
| Background Worker | `PageCleanerWorker` | 后台平滑刷脏 |
| Repository | `DoublewriteFileRepository` | 封装 doublewrite slot 和文件 |
| Policy | checkpoint pressure 和 IO capacity | 管理吞吐、延迟和恢复成本 |

## 13. 高内聚、低耦合约束

- Flush 只协调 dirty page 刷盘，不维护 Buffer Pool 内部链表。
- Doublewrite 只保护 data page write，不替代 redo。
- Checkpoint 只推进 recovery 起点，不表示所有 dirty page 已刷完。
- Flush 不进入事务行锁、MDL、SQL Executor 或 Optimizer。
- Redo 不写 data file，Disk 不理解 dirty LSN，Buffer Pool 不决定 checkpoint label。
- Recovery 总控不在 Flush 模块内；Flush 只提供 doublewrite repair helper。

## 14. 典型数据流

### 14.1 Flush List 刷脏

1. Checkpoint pressure 请求 `flushList(targetLsn)`。
2. Selector 从 Buffer Pool dirty view 选择 oldest dirty pages。
3. Page cleaner 等待 redo durable。
4. SnapshotService 捕获 page image。
5. DoublewriteService 写副本并 fsync。
6. PageStore 写 data file。
7. Buffer Pool 根据 frame 是否再修改决定 clean 或 keep dirty。
8. CheckpointCoordinator 重新计算 safe LSN。

### 14.2 LRU Flush

1. Buffer Pool free frame 压力触发 LRU flush。
2. Selector 从 LRU dirty tail 选候选。
3. Flush batch 使用 LRU doublewrite file。
4. 成功后 frame 可进入 eviction。

### 14.3 Checkpoint Pressure

1. Redo 发现 checkpoint age 超过阈值。
2. CheckpointCoordinator 计算 target LSN。
3. FlushCoordinator 提高 flush target pages。
4. Dirty view oldest LSN 前移后，checkpoint label 可推进。

### 14.4 Partial Write Recovery

见 [doublewrite-recovery-flow.mmd](diagrams/doublewrite-recovery-flow.mmd)。关键边界是先用 doublewrite 修复 torn page，再用 redo replay 按 pageLSN 幂等补齐修改。

## 15. 测试设计

- Flush list 测试：按 oldestModificationLsn 选择，target LSN 前移。
- LRU flush 测试：dirty LRU victim 刷出后可淘汰。
- Redo durable 测试：`pageLSN > flushedToDiskLsn` 时禁止写 data file。
- Doublewrite 测试：OFF、DETECT_ONLY、DETECT_AND_RECOVER 三种模式。
- Torn page 测试：data file 写一半 crash 后从 doublewrite full copy 修复。
- Checkpoint 测试：safe LSN 不越过 dirty oldest、redo closed、redo flushed。
- 并发测试：flush 期间 page 再次变脏，结果为 keep dirty。
- 锁顺序测试：redo wait 不持有 page latch，data file write 不持有 Buffer Pool list lock。
- Tablespace drain 测试：drop/truncate 前 drain 指定 space dirty page。
- 故障注入：doublewrite fsync 失败、data file write 失败、checkpoint label 写失败。
- Property-based 测试：随机 MTR/flush/checkpoint/crash/recovery 后 pageLSN 和 dirty 边界一致。

## 16. 后续实现顺序

1. `storage.flush.api`：FlushRequest、FlushResult、FlushService。
2. `storage.flush.selector`：dirty view selection。
3. `storage.flush.snapshot`：FlushSnapshot 和 frame guard。
4. `storage.flush.doublewrite`：DoublewriteMode、slot、file repository。
5. `storage.flush.cleaner`：PageCleanerWorker。
6. `storage.flush.coordinator`：batch dispatch 和 result handling。
7. Redo durable wait 接入。
8. PageStore data file write 接入。
9. CheckpointCoordinator safe LSN 计算。
10. AdaptiveFlushPolicy。
11. Tablespace drain。
12. Doublewrite recovery scanner。
13. Metrics snapshot。
14. Shutdown flush。
15. 故障注入和 property-based 集成测试。

## 17. 十五轮自检记录

| 轮次 | 检查主题 | 检查结果 |
| --- | --- | --- |
| 1 | 是否只写设计文档 | 本文只新增设计说明和 Mermaid 图，没有生成 Java 实现代码 |
| 2 | 目标与非目标 | 已明确 Flush 协调器目标，排除 Buffer Pool 内部、Redo 编码、Recovery 总控 |
| 3 | MySQL 8.0 贴合 | 已覆盖 page cleaner、adaptive flushing、doublewrite files、detect-only、fuzzy checkpoint |
| 4 | 高内聚 | selector、snapshot、doublewrite、checkpoint、worker、metrics 职责分离 |
| 5 | 低耦合 | Flush 只通过 Buffer Pool dirty view、Redo API、PageStore API 协作 |
| 6 | 面向对象 | 已定义 FlushService、FlushCoordinator、FlushSnapshot、DoublewriteService、CheckpointCoordinator |
| 7 | 设计模式 | 已列出 Facade、Mediator、Strategy、Snapshot、RAII Guard、Observer 等 |
| 8 | 核心领域模型 | 已覆盖 FlushBatch、FlushSnapshot、FlushResult、DoublewriteSlot、CheckpointState |
| 9 | 依赖方向 | 已明确 flush -> buf dirty view + redo api + fil PageStore，不反向访问内部 |
| 10 | 物理与逻辑区分 | 已区分 dirty 元数据、page image、redo LSN、doublewrite 副本、data file page |
| 11 | 关键数据流 | 已给出 flush list、LRU flush、checkpoint pressure、partial write recovery 流程 |
| 12 | 图示 | 已提供架构图、类关系图、dirty flush、doublewrite、checkpoint、并发状态、recovery 图 |
| 13 | 并发锁状态 | 已定义 REDO_WAIT、SNAPSHOTTING、DOUBLEWRITE_WRITING、DATAFILE_WRITING 等状态和持有者 |
| 14 | 异常与恢复 | 已给出 redo 未 durable、doublewrite 失败、data write 失败、checkpoint 失败和 torn page 修复 |
| 15 | 测试与顺序 | 已给出测试设计、后续实现顺序，并确认没有未完成标记或空白项 |

## 18. 参考链接

- MySQL 8.0 Reference Manual - Configuring Buffer Pool Flushing: https://dev.mysql.com/doc/refman/8.0/en/innodb-buffer-pool-flushing.html
- MySQL 8.0 Reference Manual - Doublewrite Buffer: https://dev.mysql.com/doc/refman/8.0/en/innodb-doublewrite-buffer.html
- MySQL 8.0 Reference Manual - InnoDB Checkpoints: https://dev.mysql.com/doc/refman/8.0/en/innodb-checkpoints.html
- MySQL 8.0 Reference Manual - Redo Log: https://dev.mysql.com/doc/refman/8.0/en/innodb-redo-log.html
