# Slice: Buffer Pool warmup dump/load（0.10b）

依据：`storage-backlog.md` 0.10（warmup 部分）；`innodb-buffer-pool-design.md` §11.1（dump）、§11.2（load）。

## 背景

重启后 buffer pool 冷启，热工作集要靠 demand read 重新载入，warmup 期慢。warmup 在 close 时把驻留页的定位信息
（SpaceId+PageNo）dump 到文件，open 时读回并预取，缩短 warmup。0.10a 的 `prefetch`（free-frame-only、不 fix）可直接复用。

## 目标

- `BufferPool.residentPageIds()`：在 `poolLock` 内快照当前驻留页 `PageId` 列表（只读定位信息，不含页体）。
- `BufferPoolWarmupService.dump(pool, file)`：快照 `residentPageIds()` → 写 dump 文件
  （`magic + version + count + entries(spaceId int + pageNo long) + crc32`）；最佳努力，IO 失败不抛（warmup 可丢弃）。
- `BufferPoolWarmupService.load(pool, file)`：读 dump（缺失/损坏 → 空），对每个 `PageId` 调 `pool.prefetch`
  （跳过不存在/未打开空间/无空闲帧，由 prefetch 自身保证）；返回读到的条目数。
- `StorageEngine`：close 时 dump 到 `baseDir/buffer-pool.dump`（停后台 worker 之后、关 pool 之前）；open 时 load（bootstrap/recover 之后）。

## 关键决策

- 只存 `SpaceId+PageNo`（§11.1），不存页体；dump 不参与 crash recovery、损坏可丢弃（§11.2）→ load 读损坏/缺失返回空、最佳努力。
- load **同步**预取（prefetch 只用空闲帧、不挤占前台需求读）；§11.2 的 IO 速率控制 / 分批 / 后台 load worker 为简化点延后。
- 未打开空间的页：`prefetch` 经 `readPage` 抛 `TablespaceNotOpenException` → prefetch 内部吞掉丢弃（warmup 只暖已打开空间）。
- dump 在停后台 read-ahead/cleaner 之后取快照，避免并发改 `residentMap`。

## 非目标

- IO 速率控制 / 分批 / 后台 load worker / space version 校验（§11.2 高级项）。
- random read-ahead、多 instance + 专用 `PageHashTable`（0.10 其它特性）。

## 验收测试

- `dumpThenLoadWarmsPages`：pool1 驻留 P1..P3 → dump → pool2 load → `getPage(P1)` 命中不再读盘（`CountingPageStore` 计读）。
- `loadOnMissingDumpIsNoOp`、`loadOnCorruptDumpIsNoOp`：缺失/损坏 dump → load 返回 0、不抛。
- `residentPageIdsSnapshotsResidentPages`：快照含已驻留页、不含未载入页。
- 回归：`StorageEngineTest` 生命周期——close 写出 dump 文件、reopen 不破坏、数据可读。

## 文档更新要求

- `current-implementation-map.md`：`storage.buf` read-ahead 行补 warmup（dump/load + `residentPageIds`）；Known Gap 注 random/multi-instance 仍缺。
- `storage-backlog.md`：0.10 标 warmup(0.10b) 已落，剩 random read-ahead、多 instance+PageHashTable。
- 代码注释说明：只存定位信息、损坏可丢弃、未打开空间页丢弃、IO 速率控制延后。
