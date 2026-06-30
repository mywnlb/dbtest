# Slice: Linear read-ahead（0.10a）

依据：`storage-backlog.md` 0.10（read-ahead 部分）；`innodb-buffer-pool-design.md` §8.1（read-ahead 调度）、
§8.2（linear read-ahead）、§5.6（read-ahead 页进 old 子链、被真实访问 + 时间窗才提升）。

## 背景

当前 Buffer Pool 只有 demand read（`getPage` miss 同步载入）；无预取。0.9 已有 per-frame `LOADING` 占位 +
`PageLoadFuture` 异步 load 基座、0.8 midpoint LRU（读入进 old、`oldBlocksTime` 提升窗）可复用。

## 目标

- `BufferPool.prefetch(PageId)`：**只用空闲帧**异步载入页到 old LRU，不 fix、不记访问（不提升）；已驻留/`LOADING` 跳过；
  无空闲帧直接丢弃（不挤占前台需求读，§8.1）；载入失败回收占位、不留 `LOADING`。
- `LinearReadAheadTracker`：单顺序流检测——同一 extent 内连续升序访问达 `threshold`（默认 56，可配、范围 0-64）→
  产出下一 extent 的预取请求；跨 extent / 乱序 / 反向重置 run；同一 extent 不重复提交。
- `ReadAheadService`（实现 `ReadAheadHook`）：`recordAccess(PageId)`（前台廉价、喂 tracker），命中阈值入有界队列；
  单 worker 出队调 `pool.prefetch`；`start/stop/awaitIdle`（`ReentrantLock`/`Condition`/`CountDownLatch`，无 `synchronized`，停止后拒新请求）。
- `LruBufferPool.attachReadAheadHook`（set-once，同 `attachVictimFlusher`）；`getPage` 命中/未命中都 `hook.recordAccess`。
- `StorageEngine` 构造/启动/停止 `ReadAheadService`（config 开关，默认开）。

## 关键决策

- prefetch **只用空闲帧**（不淘汰脏页、不与前台竞争）= 设计「frame 不足 read-ahead 可被丢弃」的简化实现。
- read-ahead 页 `onInsert` 进 old、**不 `onAccess`** → 复用 0.8 midpoint：未被真实访问者留 old、最先淘汰（对齐 §5.6/§316）。
- extent 粒度用本地常量 `PAGES_PER_EXTENT=64`，**不依赖 `fsp`**（守「buf 不解析空间结构」）。
- 单顺序流 tracker（多流交错会重置 run）为教学简化，注释标明与 InnoDB per-extent access-bit 的差异。
- pool↔service 解耦：pool 持 `ReadAheadHook` 接口（set-once），service 实现它并持 pool 调 `prefetch`，无环。
- 无生产顺序扫描驱动（executor 未建）：foreground `recordAccess` 由 `getPage` 触发，本片以测试模拟顺序访问驱动（test-wired）。

## 非目标

- random read-ahead、IO budget、`readAheadEvictedWithoutAccess` 统计、neighbor flush、warmup dump/load（0.10 其它特性）。
- 多 instance / `PageHashTable` 分片。
- 不改 demand read / 淘汰 / WAL gate / dirty 语义。

## 验收测试

- `prefetchLoadsPageIntoOldUnfixedAndSkipsResident`、`prefetchDroppedWhenNoFreeFrame`、`prefetchReclaimsFrameOnIoFailure`。
- `trackerEmitsNextExtentAfterThreshold`、`trackerResetsOnNonSequentialAccess`、`trackerDoesNotResubmitSameExtent`。
- `readAheadServicePrefetchesNextExtentOnSequentialAccess`：顺序 `recordAccess` → `awaitIdle` → 断言下一 extent 页驻留、未 fix。
- `readAheadServiceRejectsRequestsAfterStop`。
- 回归：`LruBufferPool`/`BufferPool` 现有测试、engine 生命周期测试。

## 文档更新要求

- `current-implementation-map.md`：`storage.buf` 增 `prefetch` + `ReadAheadService`(linear)；注明 random/warmup/multi-instance 仍缺。
- `storage-backlog.md`：0.10 标 linear read-ahead(0.10a) 已落，剩 random read-ahead / warmup / 多 instance+PageHashTable。
- 代码注释说明：prefetch 不 fix/不提升、只用空闲帧、与 InnoDB 的简化差异（单流、free-frame-only、threshold）。
