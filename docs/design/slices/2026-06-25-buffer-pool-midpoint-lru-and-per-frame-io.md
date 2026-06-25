# Slice: Buffer Pool — Midpoint LRU + 扫描抗污染（0.8）与 per-frame IO 移出 poolLock（0.9）

依据：`innodb-buffer-pool-design.md` §6.1/§6.4(midpoint LRU + 扫描抗污染)、§5.6/§5.7(latch vs fix / Frame 状态)、
§7.1/§7.3(同步 getPage / IoState)；`current-implementation-map.md` 缺口「单 `poolLock` 串行化磁盘 IO」(`:591`)、
「页替换策略仅 plain LRU」(`:595`)。Buffer Pool 质量主线，与 crash-safety 主线并行、互不阻塞。

> **一份 spec 两段提交**：Phase A(0.8) 纯独立先落；Phase B(0.9) 是核心并发改动（动 `poolLock` 不变量）在 A 之上单提。
> 同片定序因两段共改 `LruBufferPool.acquire`/`obtainVictim`/替换结构。**本片为承诺的"稍厚双 Phase spec"**：Phase B 的
> 载入握手序与 FLUSHING 生命周期是正确性合约，必须写全，故略超 40-60 行默认。

## 目标

- **Phase A**：plain LRU(`LruReplacementPolicy` 单 `LinkedHashSet`) → midpoint LRU(old/new 子链 + `oldBlocksTime` 提升窗 +
  `youngDistanceThreshold` 抗抖动)。不变量：**一次性大扫描读入页不污染 new 子链，既有 new-子链热页扫描后存活**。
- **Phase B**：把 **生产 miss read** 的 `pageStore.readPage` 移出 `poolLock`，并建显式帧状态机。不变量：
  **生产 miss read 与生产 eviction(经 `DirtyVictimFlusher`) 路径不在持 `poolLock` 期间做盘 IO；不同页 miss 可并发读盘，
  同页 miss 只发一次读；任何 LOADING 等待都有界(超时/中断/失败唤醒)，绝不留永久 loading 占位。** legacy
  `flush`/`flushAll` 与无-flusher 测试池直写（test/close-only）保持现状，**不在 Phase B 承诺、不算违反本不变量**（见非目标）。

## 关键决策

**Phase A**
- **策略自持簿记，不污染 `BufferFrame`**：old/new 归属与进 old 子链时刻由 `ReplacementPolicy` 自有 per-frame node 维护
  （`BufferFrame` 字段不变，仍 poolLock 保护）。策略注入 `common` 时钟(`LongSupplier`)判 `oldBlocksTime`；参数
  `oldBlocksPct=37`、`oldBlocksTime=1000ms`。`victimOrder()` 契约不变（old 尾优先），clean/dirty victim 选择语义不变。
- **读入即进 old 头**：miss 读入页插 old 子链头部而非 MRU；命中 old 页且驻留超 `oldBlocksTime` 才升 new 头；命中 new
  头近邻(<`youngDistanceThreshold`)不改链。完整 `ScanResistancePolicy` 访问型分类待 0.10（无 read-ahead 信号）。

**Phase B**
- **显式 `FrameStateMachine` + `BufferFrameState`(§5.7)**：本片落 `FREE/LOADING/CLEAN/DIRTY/FLUSHING` 五态，转换集中于
  状态机、`poolLock` 串行，取代散落布尔；`DIRTY_PENDING/EVICTING/STALE` 不建模（见非目标）。
- **LOADING 载入握手序（杜绝 waiter 拿到已完成 future 却撞淘汰）**：① owner 在 `poolLock` 内取 victim、置 `LOADING` 并
  **`fixCount=1`(owner 自己的 fix，故载入期不可被淘汰)**、注册 `residentMap`、建 `PageLoadFuture` → 出锁 `readPage`；
  ② 成功后**重取 `poolLock`** 发布 `CLEAN`、complete future(signal)，**owner 保留其 fix**（它即需该页的调用方），出锁取
  page latch 返回 guard；③ **等待者绝不缓存 frame 引用**：见 `LOADING` 即在 future/condition 上等，醒来后**重取 `poolLock`
  按 pageId 重查 `residentMap`**——命中 `CLEAN` 则 `fixCount++` 再用，未命中/载入失败则重跑 `acquire` 循环。LOADING 帧
  `fixCount≥1` 保证载入期 `obtainVictim` 不会选它。
- **有界等待 + 失败清理(§7.3 末条，AGENTS 并发约束)**：等待 `PageLoadFuture` 用 `awaitNanos(配置 load 超时)` 并处理
  `InterruptedException`——超时抛 `BufferPoolLoadTimeoutException`、中断则恢复中断位后抛同类异常，**不无界等**。owner
  `readPage` 抛异常时在 `finally` 重取 `poolLock`：移除 `LOADING` 占位、帧复位 `FREE` 回 free list、**以异常完成 future
  唤醒所有等待者**；故等待者必落"成功复用 / 失败重试 / 超时报错"之一，无悬挂。
- **FLUSHING 生命周期（与 `dirty` 正交，保住现有 dirty-view 合约）**：`dirty` 表"有未落盘修改"，**FLUSHING 期仍 `dirty=true`**
  （未 durable），仅 `completeFlush` 成功才清。`snapshotForFlush`：要求 `fixCount==0 && state==DIRTY`(单 IO owner)→ `DIRTY→FLUSHING`
  并出快照(bytes+dirtyVersion+pageLSN)；刷盘期**仍允许并发读+写**（页 latch 护内容，flush 用快照副本），写经 `markDirty`
  推进 `dirtyVersion`。`completeFlush`：dirtyVersion 匹配→`FLUSHING→CLEAN` 清 dirty；不匹配(期间又改)→`FLUSHING→DIRTY`
  返 false 保持 dirty。`failFlush`：`FLUSHING→DIRTY`(可重试，不再 no-op)。`dirtyPageCandidates`/`obtainVictim` **跳过
  FLUSHING**(避免重复选/重复刷)；`oldestDirtyLsnOr` **仍计入** FLUSHING(durability 边界)。脏 victim 仍走 `DirtyVictimFlusher`，本片只加状态协调、不改 WAL 序。

## 非目标（明确推迟）

- 完整 `ScanResistancePolicy` 访问型分类、Read-Ahead、warmup dump/load、多 instance 分片+专用 `PageHashTable`、升序
  pageId latch 排序（0.10 / 独立缺口）；`DIRTY_PENDING/EVICTING/STALE` 态、真 adaptive flush / page cleaner prefer（0.6）。
- 不改 legacy `flush`/`flushAll`/无-flusher 测试池的持锁直写（test/close-only，本不承诺 WAL 安全）；不改既有 WAL 安全淘汰(0.x)与 doublewrite 序。

## 验收测试

**Phase A**：`freshlyReadPageEntersOldSublistNotMru`(读入排既有 new 页前淘汰)；
`largeScanDoesNotEvictHotWorkingSet`(**核心**：先建 new 热页，扫过 >容量冷页(均 `oldBlocksTime` 内单访)→热页存活、冷页淘汰)；
`pointLookupPromotesAfterOldWindow`(old 页过窗再访升 new)；`reAccessNearNewHeadDoesNotChurn`(近 new 头再访不移链)。

**Phase B**：`concurrentMissOnDistinctPagesReadInParallel`(**核心**：插桩 `PageStore` 用 latch 证两次不同页 read 时间重叠)；
`concurrentMissOnSamePageReadsOnce`(`readPage` 仅 1 次，后到者重查 `residentMap` 拿同帧并自 fix)；
`loadTimeoutAndInterruptDoNotHang`(owner 阻塞→等待者 `awaitNanos` 超时抛 `BufferPoolLoadTimeoutException`；中断恢复中断位后抛)；
`failedLoadClearsLoadingPlaceholderAndWakesWaiters`(读失败后无残留占位、帧回 free list、等待者得异常非悬挂、重试可成功)；
`flushingFrameNotEvictedNotDoubleFlushedStillReadable`(FLUSHING 帧不被 `obtainVictim` 选/不被重复 snapshot、读仍可进)。

**回归**：buf/flush/mtr/btree/recovery 全量绿；**`BufferPoolDirtyViewTest`(snapshot→改→`completeFlush` 保持 dirty、versions
匹配清 clean、candidates 排序) 必须保住**；WAL 安全脏页淘汰、capacity-1 `MiniTransactionTest` 保持绿。

## current map 更新要求

- 缺口 `:595` →「midpoint LRU(old/new 子链)+`oldBlocksTime` 扫描抗污染；read-ahead-aware 分类待 0.10」；`storage.buf replacement`
  行(`:123`)「Plain LRU via `LinkedHashSet`」→「Midpoint LRU(old/new sublist)」。
- 缺口 `:591` →「生产 miss read 已移出 `poolLock`(per-frame `LOADING`+`PageLoadFuture`，有界等待)，仅帧表/LRU/状态短临界区在
  锁内；flush/evict 经 `FrameStateMachine` 协调；legacy `flush`/`flushAll` 仍持锁直写(test/close-only)」。Page fix 行(`:107`)去掉「miss disk IO 在 poolLock 内串行」note。
- `storage.buf pool core` 行(`:122`)：新增 `FrameStateMachine`/`BufferFrameState`/`PageLoadFuture`/`BufferPoolLoadTimeoutException`（FLUSHING 已建模；DIRTY_PENDING/EVICTING/STALE deferred）。
