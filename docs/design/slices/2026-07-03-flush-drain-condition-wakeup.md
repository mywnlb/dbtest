# FlushService drainTablespace Condition Wakeup

## Goal

- 将 `FlushService.drainTablespace` 中无进展时的固定 `LockSupport.parkNanos(1ms)` 轮询改为事件唤醒。
- 等待谓词仍以 `dirtyPagesInSpace(spaceId)` 为权威；Condition 只负责在 page flush / dirty 状态变化后缩短空等时间。
- 保持 `FlushService` 不读取 Buffer Pool 内部 frame/list/hash，只通过 `BufferPool` 门面等待 dirty view 变化。
- 和已有 `BufferPool.invalidateTablespace` fixCount drain 分工清晰：本片解决 dirty drain 等待，不重复实现 fixed-frame invalidation 等待。

## Non-goals

- 不实现 PageCleaner supervisor、flush metrics snapshot、`DETECT_ONLY` doublewrite 或 redo capacity throttle。
- 不引入真实 flush list、`freeListLock/lruListLock/flushListLock` 拆分，也不改 `BufferPoolInstance` 淘汰策略。
- 不改变 WAL gate、doublewrite、checkpoint 推进规则；`singlePageFlush` 的成功/跳过/失败语义保持不变。
- 不接 DD/DML facade，不改变 `UndoTablespaceTruncationService` 的 lifecycle X lease 和 `flushThrough` 屏障流程。

## Key Decisions

- 在 `BufferPool` 增加窄等待接口 `awaitDirtyStateChange(Duration timeout)`，返回前不承诺 dirty 已清空，调用方必须重新查询 dirty view。
- `LruBufferPool` 将等待广播分发到所有分片；单分片用已有显式 `ReentrantLock + Condition` 形态承载 dirty-state changed 信号。
- `BufferPoolInstance.release` 标脏、`completeFlush` 清脏/保脏、`failFlush` 回 DIRTY、`resetFrameToFree` 移除旧帧等会影响 dirty view 的路径负责 signal。
- `FlushService.drainTablespace` 仅在一轮 `singlePageFlush` 后 clean 数无增长且目标 space 仍有 dirty page 时等待该接口；醒来、超时片段结束或中断后都回环重新扫描。
- 等待必须支持总 deadline：每次等待传入剩余时间，timeout=0 时不阻塞；中断时恢复线程中断位并返回 `timedOut=true` 的 `TablespaceDrainResult`，保持当前结果型 API，不新增 drain 专用异常。
- `flushThrough` 暂不改动，继续使用现有短 park；本片验收只要求 `drainTablespace` 去 busy-wait。

## Acceptance Tests

- `drainTablespaceWaitsOnDirtyStateCondition`：目标页第一次 flush 被故意跳过，后台稍后清脏并 signal，drain 不依赖 1ms 轮询即可返回成功。
- `drainTablespaceRechecksDirtyPredicateAfterWakeup`：发送无关 signal 或其它 space 清脏后，drain 必须重新扫描并继续等待目标 space dirty page。
- `drainTablespaceHonorsTimeoutWithoutBusyLoop`：没有任何 dirty 进展时按总 timeout 返回 `timedOut=true`，等待次数受 Condition 超时控制而非毫秒自旋。
- `drainTablespaceRestoresInterruptStatus`：等待中断后恢复中断位，并返回 `timedOut=true` 表达未完成 drain。
- 回归覆盖 `UndoTablespaceTruncationService.truncate`：`flushThrough -> invalidateTablespace` 语义不变，fixed-frame 等待仍走 Buffer Pool invalidation 的 `frameReleased` condition。

## Current Map Update

- `Storage Disk Manager Slice` 的 UNDO truncate chain 补充：`FlushService.drainTablespace` 已从固定 1ms park 改为 dirty-state condition wakeup。
- `Buffer Pool + MiniTransaction Slice` 保留已有 fixed-frame `frameReleased` condition 描述，并说明 dirty drain wait 是 FlushService 通过 BufferPool 门面消费的独立等待信号。
- `storage-backlog.md` 的 0.7 项只移除或标记 `drainTablespace busy-wait 改 condition 唤醒`，其它 0.7 碎片仍保留。

## Verification

- 先新增 drain wait 并发测试，确认旧实现因无法观测 condition/等待次数而失败或暴露 busy-wait 行为。
- 实现后运行 `gradle test --tests cn.zhangyis.db.storage.flush.* --tests cn.zhangyis.db.storage.buf.*`。
- 再运行全量 `gradle test`，确认测试数不倒退。
- 静态扫描生产代码不新增 `synchronized/wait/notify/notifyAll`、裸 `IllegalArgumentException` 或裸 `RuntimeException`。

## Five-pass Self-check

- Pass 1 scope: 只定义一个 storage 内小切片，未扩大到 PageCleaner supervisor、metrics 或 DML facade。
- Pass 2 boundaries: Flush 仍经 `BufferPool` 门面观察 dirty view，不读取 `BufferFrame`、page hash、LRU 或 flush list 内部。
- Pass 3 concurrency: 等待使用显式 `ReentrantLock + Condition`，谓词外置并重查，支持 timeout 和 interrupt，无隐式 monitor。
- Pass 4 recovery semantics: 不改变 WAL gate、doublewrite、checkpoint、UNDO truncate lifecycle marker 或 invalidation 两阶段语义。
- Pass 5 documentation: 指定了 `current-implementation-map.md` 与 `storage-backlog.md` 的最小更新点，没有 TBD/TODO 或把未实现项写成已完成。
