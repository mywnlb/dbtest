# Slice: WAL-safe dirty-page eviction (LRU_FLUSH)

依据：`innodb-flush-checkpoint-doublewrite-design.md` §6.1(LRU_FLUSH)、§7.1(dirty page flush 标准顺序)、
§8.1/§8.3(与 Buffer Pool/Disk Manager 协作)、§9.2(REDO_WAIT 不持 latch/file lock)；
`current-implementation-map.md` Buffer Pool 缺口「`LruBufferPool.writeBack` 无 WAL gate」。

## 目标

闭合淘汰路径的 WAL/doublewrite 正确性洞。当前 `LruBufferPool` 淘汰脏帧时在 `poolLock` 内直接
`pageStore.writePage`，既不等 redo durable（违反 WAL），也不过 doublewrite/checksum。本片建立不变量：

> **注入 `DirtyVictimFlusher`（生产路径，engine bootstrap 注入）后，脏帧只能经 WAL gate + checksum +
> doublewrite 管线写盘；缓冲池自身只复用 clean/free 帧。** 无 flusher 的独立/测试池保留 legacy
> `writeBack`（字节级保持现状，零回归），不在本片承诺 WAL 安全。

## 关键决策

- **复用现成 flush 管线，不重写**：`FlushCoordinator.singlePageFlush` 已实现 §7.1 全序（snapshot→WAL gate
  `waitFlushed`→checksum→doublewrite→write+force→`completeFlush`）。淘汰只需把脏 victim 交给它。
- **依赖反转端口 + 明确返回契约**：buf 新增 `DirtyVictimFlusher.flushVictim(PageId): boolean`（buf 定义，
  `flush` 实现 `CoordinatedDirtyVictimFlusher`，保持 buf 不 import flush，同 `PageWriteListener` DI-seam）。
  适配器按 `FlushResultStatus` 映射：`CLEAN→true`（已干净可复用）；`SKIPPED_NOT_DIRTY`/`SKIPPED_REDO_NOT_DURABLE`/
  `KEPT_DIRTY→false`（本轮未清成，可另选）；**`FAILED→抛出所携带 cause`**（真 IO/doublewrite 失败，绝不能被
  吞成 `BufferPoolExhaustedException`，否则掩盖盘故障）。
- **obtainVictim 选择 + 锁外清理 + 回环**：`acquire` miss 路径在 `poolLock` 内 `obtainVictim`——优先 free/clean
  帧直接返回安装；否则只**记录**首个 unfixed 脏帧的 `PageId`（不返回脏帧、不在锁内写盘），释放 `poolLock` →
  `flushVictim(id)` 锁外做 WAL/doublewrite/写盘/`completeFlush` → 回环重新 `obtainVictim`（此时该帧应已干净，或被
  他人再 fix/再脏则另选）。**无 flusher 时**对脏 victim 回退 legacy `writeBack`（保持
  `shouldEvictLruWriteBackDirtyAndReReadFromDisk`、capacity-1 `MiniTransactionTest` 等现有行为，字节级不变）。
- **本轮 skip set 防空转**：同一次 `acquire` 内维护 skip set，`flushVictim` 返回 false（含 redo 未 durable）的
  `PageId` 本轮不再选；当无 clean/free 帧且全部脏 candidate 已在 skip set → 抛 `BufferPoolExhaustedException`
  （fail-safe，不腐败、不无界自旋）。`FAILED` 直接向上抛，不进 skip set。
- **晚绑定 set-once**：pool 先于 FlushCoordinator 构造（环），用 `attachVictimFlusher` 注入：null 抛校验异常、
  重复 attach 抛校验异常（set-once）；字段 volatile，单线程注入、热路径只读。**注入时机**：必须在 FlushCoordinator
  构造**之后**、任何可能触发淘汰的 pool page access（fresh bootstrap 建系统 undo 表空间 / recovery 后开放流量）
  **之前**完成；生产 `StorageEngine` 必注入，legacy 无 flusher 仅限独立测试池。

## 非目标（明确推迟）

- 不把 `RecoverableDoublewriteStrategy` 接进 engine——engine 仍注入 `NoDoublewriteStrategy`，故本 slice **不提供
  torn-page 恢复能力**：淘汰只是结构上**走过** doublewrite seam（一旦接入真策略即自动获得保护），WAL gate + checksum
  本片即生效，真 torn-page 副本/修复留后续 doublewrite 接线片。
- 不改 legacy `BufferPool.flush/flushAll`（test/close-only，仍直接 writeBack）；生产 close 已走 `FlushService.flushThrough`。
- 不加后台 redo flusher（redo 不 durable 时淘汰按 WAL 正确跳过/阻塞，不本片解决）。
- 不改单 `poolLock` 串行盘 IO 的总体简化（仅把脏 victim 的写盘移出锁）。

## 验收测试

- `evictingDirtyVictimRoutesThroughFlusher`：填满池、脏一页、再 acquire 新页 → flusher 被调用，脏页不经直接 writeBack。
- `redoNotDurableEvictionDoesNotWriteDirtyPage`（**核心正确性**）：durable LSN < victim pageLSN → 数据文件**不含**脏页新字节，WAL 保持。
- `cleanedVictimFrameReused`：redo durable → flushVictim 清脏 → 帧复用装新页；重开校验原脏页已落盘 + checksum 通过。
- `legacyWriteBackWhenNoFlusher`（回归）：无 flusher 时脏 victim 仍走 legacy writeBack——既有
  `shouldEvictLruWriteBackDirtyAndReReadFromDisk`、capacity-1 `MiniTransactionTest` 保持绿，零字节行为变化。
- 回归：现有 buf/flush/mtr/btree 全量不倒退。

## current map 更新要求

- Buffer Pool 数据链 Eviction 行：改为「脏 victim 经 `DirtyVictimFlusher` 锁外 WAL gate+doublewrite，clean/free 帧直接复用；无 flusher 回退 legacy writeBack」。
- Buffer Pool 缺口「writeBack 无 WAL gate」改为已闭合（生产淘汰侧），保留 legacy flushAll/no-flusher 仍直写的 note。
- 全局架构缺口行 `StorageEngine` 顶部「E3a 仍假设无脏页淘汰（容量>工作集）」：放宽为「注入 flusher 后脏页淘汰已 WAL 安全」，但仍无后台 redo flush（redo 未 durable 时淘汰按 WAL 跳过/耗尽）。
- Package Status：`storage.buf` 新增 `DirtyVictimFlusher` 端口；`storage.flush` 新增 `CoordinatedDirtyVictimFlusher` 适配器；`StorageEngine` 注入记一行。
