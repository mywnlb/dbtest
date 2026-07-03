# Slice: Buffer Pool legacy flush 收敛（13.1b-pre）

依据：`innodb-buffer-pool-design.md` §13.2、§13.4、§13.5；
`current-implementation-map.md` Buffer Pool Known Gaps；`storage-backlog.md` 0.10 / BP 缺口。
前置：0.9 per-frame LOADING + `snapshotForFlush/completeFlush/failFlush` 已落；
13.1a `BufferPoolInstanceLatchSet` 已守卫 page read、loading wait、dirty victim flush 前释放 metadata lock。

## 目标

- 收敛 `BufferPoolInstance.flush(PageId)`、`flushAll()` 与 no-flusher dirty victim fallback 的 legacy 同步写盘路径。
- 任何 `PageStore.writePage/force` 调用前都不得持有 `BufferPoolInstance` metadata lock。
- 复用现有 `FlushPageSnapshot`、`dirtyVersion`、`completeFlush`、`failFlush` 协议，避免 snapshot 后并发重新变脏被误清。
- 保持 public `BufferPool` API 不变；本片只调整内部实现和测试。
- 让 §13.1 真正子锁拆分前不再有“metadata lock 下物理 IO”的例外路径。

## 关键决策

- `writeBack(BufferFrame)` 改为删除或降级为只处理 snapshot 写出的私有 helper；不得再接收裸 frame 后持锁写盘。
- `flush(pageId)` 流程：短锁内通过 `snapshotForFlush(pageId)` 复制页体并置 `FLUSHING`，出锁写 `PageStore`，成功后 `completeFlush`，失败后 `failFlush` 并保留 cause。
- `flushAll()` 流程：循环收集一批当前未 fixed 且 `DIRTY` 的 page id，逐页走同一 snapshot 写盘协议；每轮重新读取候选，直到没有可刷 dirty frame。
- no-flusher dirty victim fallback 不在 metadata lock 内直写 victim；它记录 victim page id，出锁执行一次 legacy snapshot flush，成功后回到 victim 选择循环。
- snapshot 为 empty 时视为本轮被并发 fix、已 clean、正在 FLUSHING 或版本不匹配；调用方回环重试或跳过，不强行持锁等待。
- 写盘失败必须调用 `failFlush(pageId)`，把 `FLUSHING` 复位为 dirty 可重试状态，再抛 `DatabaseRuntimeException` 或既有领域异常。
- 本片不为 legacy `flush/flushAll` 增加 WAL gate；生产 WAL-safe flush 仍由 `FlushCoordinator` 负责，legacy 路径只解决锁边界和 dirtyVersion 正确性。
- 注释更新必须明确：这是 13.1b-pre 收敛片，仍不是 `pageHashLock/freeListLock/lruListLock/flushListLock/frameMutex` 真拆分。

## 非目标

- 不实现 §13.1 子锁拆分，不引入真正 `pageHashLock/freeListLock/lruListLock/flushListLock/frameMutex`。
- 不改变 `FlushCoordinator`、doublewrite、checkpoint、WAL gate 的生产语义。
- 不新增 flush batch dispatcher、FlushList/LRU 双文件、PageCleaner supervisor 或 metrics。
- 不改变 `BufferPool.flush/flushAll` 的 public 方法签名，也不让上层绕过 `FlushCoordinator` 取得 WAL-safe 承诺。
- 不处理 warmup IO 限速、random read-ahead 配置、DROP/DISCARD DDL lifecycle。

## 验收测试

- `legacyFlushReleasesMetadataLockBeforePageStoreWrite`：阻塞 `PageStore.writePage` 时，同分片其它驻留页可被 `getPage` 命中并释放。
- `legacyFlushAllReleasesMetadataLockBeforeEachWrite`：`flushAll` 写第一张脏页阻塞时，同分片其它 page miss 可注册 LOADING，证明未持 metadata lock 写盘。
- `legacyFlushDoesNotClearPageDirtiedAfterSnapshot`：flush snapshot 后并发重新标脏并 bump `dirtyVersion`，写盘完成后 frame 仍保持 dirty。
- `dirtyVictimNoFlusherFallbackWritesOutsideMetadataLock`：未注入 `DirtyVictimFlusher` 的小池淘汰脏 victim 时，阻塞写盘不阻塞同分片 clean hit。
- `legacyFlushFailureRestoresDirtyState`：`PageStore.writePage` 抛错后 `failFlush` 复位，后续成功 flush 可重新写出该页。
- 既有 `BufferPoolMetadataLockBoundaryTest`、`BufferPoolTablespaceInvalidationTest` 与全量 Gradle `test` 不倒退。

## current map 更新要求

- Buffer Pool Page fix / Eviction / Known Gaps 更新：legacy flush 已不再持 metadata lock 做物理 IO。
- Package status 说明 13.1b-pre 只收敛 legacy flush 边界，`BufferPoolInstanceLatchSet` 仍是一把 metadata lock。
- storage-backlog BP 行把 “legacy flush 收敛” 从主要缺口移到已落状态；保留 §13.1 真正子锁拆分。
