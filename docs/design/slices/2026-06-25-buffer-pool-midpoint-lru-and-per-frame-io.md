# Slice: Buffer Pool — Midpoint LRU + 扫描抗污染（0.8）与 per-frame IO 移出 poolLock（0.9）

依据：`innodb-buffer-pool-design.md` §6.1(Midpoint LRU)、§6.4(扫描抗污染)、§5.6(latch vs fix)、§5.7(Frame 状态)、
§7.1(同步 getPage 数据流)、§7.3(IoState)；`current-implementation-map.md` 缺口「单 `poolLock` 串行化磁盘 IO」
(`:591`)、「页替换策略仅 plain LRU」(`:595`)。本片是 Buffer Pool 质量主线，与 crash-safety 主线并行、互不阻塞。

> **一份 spec 两段提交**：Phase A(0.8) 纯独立、低风险，先落先提交；Phase B(0.9) 是核心并发改动（动 `poolLock`
> 不变量），在 A 之上单独一次提交。两段共改 `LruBufferPool.acquire`/`obtainVictim`/替换结构，故同片定序避免对同一
> 热点代码两次重构。

## 目标

- **Phase A**：把 plain LRU(`LruReplacementPolicy` 单 `LinkedHashSet`)换成 **midpoint LRU**(old/new 双子链 +
  `oldBlocksTime` 提升窗 + `youngDistanceThreshold` 抗抖动)。不变量：**一次性大扫描读入的页不污染 new 子链，
  既有 new-子链热页在扫描后仍存活（不被冲掉）**。
- **Phase B**：把 miss 的 `pageStore.readPage` 移出 `poolLock`。不变量：**`poolLock` 只覆盖帧表/LRU/状态的短临界区，
  绝不在持锁期间做盘 IO；不同页的 miss 可并发读盘；同页 miss 只发一次读（per-frame `LOADING` + load future），
  读失败必清理 `LOADING` 占位、不留永久 loading、不让等待者悬挂。**

## 关键决策

- **策略自持簿记，不污染 `BufferFrame`**（A）：old/new 归属、进入 old 子链时刻由 `ReplacementPolicy` 自有 per-frame
  node 维护（`BufferFrame` 字段不变，仍 poolLock 保护）。`ReplacementPolicy` 注入 `common` 时钟（`LongSupplier`/`Clock`）
  以判 `oldBlocksTime`；接口默认参数 `oldBlocksPct=37`、`oldBlocksTime=1000ms`。`victimOrder()` 契约不变（old 尾优先）。
- **读入即进 old 头**（A）：miss 读入页插入 old 子链头部，而非 MRU；命中 old 页且驻留超 `oldBlocksTime` 才提升 new 头；
  命中 new 头近邻(<`youngDistanceThreshold`)不改链。clean/dirty victim 选择语义不变（仅遍历顺序变）。
- **显式 `FrameStateMachine` + `BufferFrameState`**（B，§5.7）：本片落 `FREE/LOADING/CLEAN/DIRTY/FLUSHING` 五态，
  状态转换集中在状态机、由 `poolLock` 串行，取代散落布尔。`DIRTY_PENDING/EVICTING/STALE` 本片不建模（见非目标）。
- **per-frame load future + 锁外读盘**（B，§7.1/§7.3）：miss 在 `poolLock` 内取 victim→注册 `LOADING` 占位到
  `residentMap`(同页后到者见 `LOADING` 即等其 future)→出锁 `pageStore.readPage`→重取锁发布 `CLEAN` 并唤醒等待者。
  一帧同一时刻单 IO owner。读失败：出锁路径异常→重取锁移除占位、帧复位 `FREE` 回 free list、以异常唤醒等待者，
  绝不留 loading 占位（§7.3 末条）。
- **`FLUSHING` 与现有 flush 复用**（B）：刷盘期帧置 `FLUSHING`（仍可读，不被并发选为 victim/重复刷），完成回 `CLEAN`，
  对接现 `snapshotForFlush`/`completeFlush`；脏 victim 仍走既有 `DirtyVictimFlusher`(WAL gate)，本片只加状态协调，不改 WAL 序。

## 非目标（明确推迟）

- 完整 `ScanResistancePolicy` 的来源/访问型分类（linear/random read-ahead、sequential vs point）——无 read-ahead/访问型
  信号，本片只做 `oldBlocksTime` 窗 + old/new 子链；read-ahead-aware 分类随 **0.10** 落。
- Read-Ahead、warmup dump/load、多 instance 分片 + 专用 `PageHashTable`、升序 pageId latch 排序（0.10 / 独立缺口）。
- `DIRTY_PENDING`/`EVICTING`/`STALE` 态、真 adaptive flush / page cleaner prefer（0.6）。
- 不改 legacy `flushAll`/无 flusher 测试池直写、不改既有 WAL 安全淘汰(0.x)与 doublewrite 序。

## 验收测试

**Phase A**
- `freshlyReadPageEntersOldSublistNotMru`：读入页排在既有 new-子链页之前被淘汰（证明进 old 头非 MRU）。
- `largeScanDoesNotEvictHotWorkingSet`（**核心**）：先建 new-子链热页，再一次性扫过 > 容量的冷页(均在 `oldBlocksTime`
  内单次访问)→热页存活、冷页被淘汰（plain LRU 下热页会被冲掉）。
- `pointLookupPromotesAfterOldWindow`：old 页过 `oldBlocksTime` 后再访问 → 升 new 头。
- `reAccessNearNewHeadDoesNotChurn`：近 new 头页再访问不产生结构移动（`youngDistanceThreshold`）。

**Phase B**
- `concurrentMissOnDistinctPagesReadInParallel`（**核心**）：插桩 `PageStore` 用 latch 证两次不同页 read 时间重叠
  （非串行）。
- `concurrentMissOnSamePageReadsOnce`：两线程 miss 同页 → `readPage` 仅 1 次，后到者拿同帧。
- `failedLoadClearsLoadingPlaceholder`：读失败后 `residentMap` 无残留、帧回 free list、重试可成功、等待者得异常非悬挂。
- `flushingFrameNotConcurrentlyEvictedAndStillReadable`：`FLUSHING` 帧不被并发选 victim/重复刷，读仍可进。

**回归**：buf/flush/mtr/btree/recovery 全量绿不倒退；WAL 安全脏页淘汰、capacity-1 `MiniTransactionTest` 保持绿。

## current map 更新要求

- 缺口表 `:595` 「页替换策略仅 plain LRU」→「midpoint LRU(old/new 子链)+`oldBlocksTime` 扫描抗污染；read-ahead-aware
  分类待 0.10」。`storage.buf replacement` 行(`:123`)「Plain LRU via `LinkedHashSet`」→「Midpoint LRU(old/new sublist)」。
- 缺口表 `:591` 「单 `poolLock` 串行化磁盘 IO」→「miss read 已移出 `poolLock`（per-frame `LOADING`+load future），
  仅帧表/LRU/状态短临界区在锁内；flush/evict 协调经 `FrameStateMachine`」。Page fix 行(`:107`)同步去掉「miss/evict disk IO 在 poolLock 内串行」note。
- `storage.buf pool core` 行(`:122`)：新增 `FrameStateMachine`/`BufferFrameState`（FLUSHING 已建模；DIRTY_PENDING/EVICTING/STALE deferred）。
