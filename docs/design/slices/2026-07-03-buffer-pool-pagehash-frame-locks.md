# Buffer Pool 13.1c pageHashLock + frameMutex

## Goal

- 将 `BufferPoolInstance` 从单一 metadata lock 推进到 §13.1 的第一阶段真实子锁。
- `pageHashLock` 只保护 `PageId -> BufferFrame` 映射与 LOADING single-flight 占位发布。
- `frameMutex` 保护单个 frame 的 `state/fixCount/dirty/LSN/dirtyVersion/loadFuture/spaceVersion`。
- PageStore IO、`PageLoadFuture` 等待、dirty victim flush、legacy page write 进入前不得持有 hash/frame 锁。

## Non-goals

- 不拆 `freeListLock/lruListLock/flushListLock`，也不宣称 §13.1 全部完成。
- 不引入 `DIRTY_PENDING/EVICTING/STALE`，细状态留后续 slice。
- 不改变 `BufferPool` public API。
- 不把 legacy `flush/flushAll` 升级成 WAL-safe 生产入口。

## Key Decisions

- `BufferPoolInstanceLatchSet` 成为子锁入口：本 slice 增加 hash 锁、frame 锁断言和独立 drain wait 锁。
- `BufferFrame` 增加 `frameMutex`，字段注释必须明确 frame 元数据由它保护。
- free list、LRU、dirty candidate 仍由一个兼容性 list/meta 短锁保护，下一 slice 再拆成真实 list/flush locks。
- 锁顺序固定为 `pageHashLock -> frameMutex -> list/meta compatibility lock -> pageLatch`。
- 等待 `PageLoadFuture` 前只缓存 future，不缓存可直接使用的 frame 结果；醒来后重新查 hash。

## Acceptance Tests

- 同页并发 miss 仍 single-flight，只发生一次物理 read。
- 同一页 LOADING waiter 等待 future 时，不持有 `pageHashLock` 或 `frameMutex`，其它 resident hit 可继续。
- 阻塞某个 frame 的 `frameMutex` 不阻塞其它页 resident hit。
- legacy flush 进入 PageStore.writePage 前不持有 `pageHashLock` 或目标 frame `frameMutex`。
- 并发 close guard 后 `fixCount` 不为负，invalidation drain 能被 release 唤醒。

## Current Map Update

- `Buffer Pool + MiniTransaction Slice` 标记 13.1c 已拆 `pageHashLock + frameMutex`。
- 明确 `freeListLock/lruListLock/flushListLock`、真实 flush list、`DIRTY_PENDING/EVICTING/STALE` 仍未完成。
- `Reserved / Unwired Production Types` 中 legacy `flush/flushAll` 保持 test/close fallback 语义。

## Verification

- 先运行新增 13.1c 测试并确认 RED。
- 实现后运行 `gradle test --tests cn.zhangyis.db.storage.buf.*`。
- 再运行全量 `gradle test`。
- 静态扫描生产代码不新增 `synchronized/wait/notify/notifyAll` 或裸 `IllegalArgumentException/RuntimeException`。
