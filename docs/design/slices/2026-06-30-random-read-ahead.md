# Slice: Random read-ahead（0.10c）

依据：`storage-backlog.md` 0.10（read-ahead 部分，linear 已落 0.10a）；`innodb-buffer-pool-design.md` §8.1/§8.3。

## 背景

linear read-ahead（0.10a）按顺序访问触发。random read-ahead 是另一触发模式：当**同一 extent 已有足够多页驻留**
（不要求顺序访问）时，异步补取该 extent 的缺失页。MySQL 默认关闭（`innodb_random_read_ahead=OFF`），是较次要的启发式。
复用 0.10a 的 `prefetch` 原语 + `ReadAheadService` 队列/worker。

## 目标

- `BufferPool.residentCountInRange(SpaceId, firstPageNo, pageCount)`：poolLock 内统计某连续页区间内已驻留的页数。
- `RandomReadAheadDetector`：给定 pageId + 其 extent 内驻留页数 + `threshold`，当驻留数 ≥ threshold 且该 extent 未刚发过 →
  产出整 extent 的 `ReadAheadRequest`（prefetch 自身跳过已驻留页）；同一 extent 只做 bounded recent/last-emitted 去重，
  不做永久 set，避免页被淘汰后永远不能再次触发。
- `ReadAheadService` 增 random 路径：构造增 `randomThreshold`（0=禁用，默认）；`recordAccess` 在 linear 之外，若 random 启用
  则查 `bufferPool.residentCountInRange(本页 extent)` 喂 detector，命中则入队；random 检测异常必须吞掉并丢弃本次预取，
  保持 `recordAccess` 不破坏 demand read。
- `StorageEngine` 以 `randomThreshold=0`（禁用，对齐 MySQL 默认 OFF）构造；生产启用留 config（延后）。

## 关键决策

- random 默认禁用（threshold 0）：普通路径**无额外开销**（不查 residentCountInRange）；启用时每次 `recordAccess` 查一次
  extent 驻留数（O(extent)=64 次 map 查，poolLock 内），教学可接受，注释标明。
- 触发条件采用「同一 extent 驻留页数量」而非 InnoDB 更细的连续/访问位启发式；这是教学简化，后续可用 access-bit /
  evicted-without-access 统计细化。
- 锁序：`recordAccess` 持 service.lock 时调 `residentCountInRange`(poolLock)；与 worker `prefetch`(poolLock，不持 service.lock)、
  `getPage`(先放 poolLock 再 recordAccess) 一致为 service.lock→poolLock 单向，无反向、无环。
- 复用 `ReadAheadRequest`/队列/worker/prefetch；extent 粒度沿用 `LinearReadAheadTracker.PAGES_PER_EXTENT=64`。
- 机制完整 + 单测；生产 enable（config flag）延后，与 MySQL 默认 OFF 一致。

## 非目标

- IO budget（`readAheadEvictedWithoutAccess` 统计 / 预取页未访问淘汰计数）。
- 多 instance + 专用 `PageHashTable`（0.10 最后一项）。
- 生产 config 开关（本片 engine 仍默认禁用）。

## 验收测试

- `residentCountInRangeCountsResidentPagesOnly`：区间内已驻留/未驻留页计数正确。
- `randomDetectorEmitsExtentWhenEnoughResident` / `belowThresholdNoEmit` / `dedupSameExtent` / `canEmitAgainAfterRecentWindowMovesOn`。
- `readAheadServiceRandomPrefetchesMissingExtentPages`：同 extent 驻留达阈值的访问 → `awaitIdle` → 缺失页被预取；
  linear 与 random 并存不互相干扰；`randomThreshold=0` 时 random 不触发。
- `randomPathDoesNotThrowWhenResidentCountFails`：resident count 或 detector 异常只丢弃本次 random read-ahead，不影响 demand read。
- 回归：`ReadAheadServiceTest`、`LruBufferPool`/`BufferPool` 现有测试、engine 生命周期。

## 文档更新要求

- `current-implementation-map.md`：buf read-ahead 行补 random（`residentCountInRange` + `RandomReadAheadDetector` + service random 路径，默认禁用）。
- `storage-backlog.md`：0.10 标 read-ahead（linear+random）已落，剩多 instance + `PageHashTable`。
- 代码注释说明：random 默认禁用、启用时的 per-access 开销、与 InnoDB 的简化差异。
