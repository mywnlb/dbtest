# Slice: Multi-instance buffer pool（0.10d，单 instance 锁）

依据：`storage-backlog.md` 0.10（read-ahead 已落，剩多 instance + PageHashTable）；`innodb-buffer-pool-design.md` §5.1/§5.2/§5.5/§13.1/§13.2。

## 背景

当前 `LruBufferPool` 是单实例池：单 `poolLock` 串保护 `residentMap`/freeList/LRU/帧元数据短临界区。InnoDB 把大 buffer pool 分为多个 instance（§5.2），各自独立 free list/LRU/page hash/锁，按 `hash(PageId)%instanceCount` 路由，降低单锁争用。本片落地**多 instance 分片 + per-instance PageHashTable**，每 instance **单把 `instanceLock`**（不拆 §13.1 的 pageHash/freeList/lru 子锁）。

## 目标

- `BufferPoolInstance`（`implements FrameReleaser`）：承接今天 `LruBufferPool` 全部 per-pool 逻辑（自有 `instanceLock`+`frameReleased`+`PageHashTable`+freeList+`ReplacementPolicy`+`FrameStateMachine`，共享 `PageStore`/可空 `DirtyVictimFlusher`/loadTimeout）；single-flight LOADING 占位仍在 `acquire`。创建 `PageGuard` 时传 `this`，release 直接回归属 instance。
- `PageHashTable`：`Map<PageId,BufferFrame>` 包装（get/put/remove/containsKey/values/size + countInRange），由 instance 锁在外保护（本片不内置 pageHashLock）。
- `BufferPoolRouter`：纯函数 `route(PageId)=floorMod(mix(spaceId,pageNo),instanceCount)`，确定/无锁/分布均匀；N=1 恒 0、字节级等价今天单池。
- `LruBufferPool` 转 facade（`implements BufferPool`，不再 `FrameReleaser`）：持 `instances[]`+router，单页操作路由到归属 instance；`attachVictimFlusher` 传播各 instance、`attachReadAheadHook` 留 facade（getPage 路由后 recordAccess）。
- 跨切面聚合：`dirtyPageCandidates`(各 instance 取本地 ≤maxPages→合并按 oldestLSN 升序→取 maxPages)、`oldestDirtyLsnOr`(全局 min)、`hasDirtyPages`(OR)、`residentPageIds`(拼接)、`residentCount`/`residentCountInRange`(求和)、`capacity`(求和=总)。
- `invalidateTablespace`：**两阶段，不可逐 instance 边检边删**——先逐 instance `wait(fix=0)`+`check(no-dirty)` 全部通过（任一 dirty/超时即抛，**尚未移除任何帧**），再逐 instance remove；共享 deadline，全程依赖调用方持 tablespace X lease 阻断新流量 → all-or-nothing（避免 A 已删、B 才发现 dirty/timeout 的部分失效）。
- 容量切分：base=cap/N、余 r→前 r 个 +1，要求 `capacity≥instanceCount`。
- `EngineConfig` 加 `bufferPoolInstanceCount`（默认 1）；`StorageEngine` **用 `config.bufferPoolInstanceCount()` 构造池**（config 即生产消费路径，默认 1→生产行为零变；测试可经 config 显式配 N>1，不留无消费者的死配置）。

## 关键决策

- 每 instance 单 `instanceLock`：本片只做分片，不拆 §13.1 子锁（教学渐进，注释标明）。
- **无跨 instance frame stealing**：路由固定后某 instance 满即抛 `BufferPoolExhaustedException`，即便他 instance 仍有空闲帧——这是多 instance 的正常取舍（与 InnoDB 一致，不做 work stealing），文档与单测显式覆盖。
- facade 跨切面操作**一次只持一把 instance 锁**（锁单 instance 取快照→解锁→下一个→锁外合并），绝不同持两把 → 无 instance 间锁序、无死锁。
- 锁序：read-ahead service.lock→instanceLock（逐个不跨持）、victim flusher 释放 instanceLock 后再 flush、flush 回调路由回同一 instance 重取锁——均与今天单池语义一致，单向无环。
- 生产 N=1（config 默认 1），N>1 仅单测，与 random read-ahead 同一路数（机制完整、生产保守）。
- **构造器兼容**：旧构造器语义不变 = instanceCount=1——3 参 `(store,PS,capacity)`（大量生产/测试调用）、包内 4 参 `(...,ReplacementPolicy)`、5 参 `(...,policy,loadTimeout)` 全部保持单 instance、零改动；**新增** `(store,PS,capacity,instanceCount)`，每 instance 用默认 midpoint 策略。`FrameReleaser` 实现从 facade 下移到 `BufferPoolInstance`（需核对无他处把池当 `FrameReleaser` 或裸构造 `PageGuard`）；`LruBufferPool` 是 `BufferPool` 唯一实现，消费方走接口不受影响。

## 非目标

- §13.1 per-instance 拆分锁（pageHashLock/freeListLock/lruListLock/flushListLock/frameMutex）。
- 0.22 stale-frame 版本校验（`TablespaceVersion`/`SpaceLifecycleClock`、drop/truncate 后旧帧隔离）；invalidate 维持现语义。
- 新帧态（DIRTY_PENDING/EVICTING/STALE）；PageHashTable 内置 single-flight（仍留 instance.acquire）。
- 生产默认开多 instance（仍 N=1）。

## 验收测试

- `BufferPoolRouterTest`：路由稳定 + 分布均匀 + N=1 恒 0。
- `LruBufferPoolMultiInstanceTest`：N>1 路由/往返、容量求和、per-instance 独立淘汰（填满 instance B 不淘汰 instance A 热页）、**局部分片耗尽**（填满某 instance 直到抛 `BufferPoolExhaustedException`，即便其它 instance 仍有空闲帧——验证无 stealing）、`dirtyPageCandidates` 跨 instance 合并按 LSN 升序、`oldestDirtyLsnOr` 全局 min、`invalidateTablespace` 跨 instance 移除某空间全部帧 + **all-or-nothing**（某 instance 有 dirty 帧时整体抛、不留部分移除）、`residentCount`/`residentPageIds`/`residentCountInRange` 聚合、read-ahead 经 facade N>1 仍预取、多线程不同 instance 并行 get。
- 回归：全部既有 buf 测试（`LruBufferPoolTest`/`BufferPoolConcurrentLoadTest`/`BufferPoolFlushingStateTest`/`BufferPoolDirtyViewTest`/`BufferPoolTablespaceInvalidationTest`/`ReadAheadServiceTest`/`BufferPoolWarmupTest`/Midpoint/ScanResistance）N=1 零回归；StorageEngine 生命周期（仍 N=1）。

## 文档更新要求

- `current-implementation-map.md`：buf pool core 行补 `BufferPoolInstance`/`PageHashTable`/`BufferPoolRouter` + facade/instance 拆分与跨切面聚合；锁序小节注明「facade 一次只持一把 instance 锁」。**并修正 Known Gaps 中 0.10 那条 gap 行**（现写「random/multi-instance 未做」「775 tests」已陈旧）：random read-ahead 已落（0.10c），本片落地后该行改为多 instance 已落、剩 §13.1 拆分锁 / 0.22 stale 校验。
- `storage-backlog.md`：0.10 标多 instance 已落（剩 §13.1 拆分锁 / 0.22 stale 校验）；BP 完成度行更新。
- 代码注释：单 instance 锁 vs §13.1 简化、路由确定性、跨切面只持一把锁不变量、生产 N=1 默认。
