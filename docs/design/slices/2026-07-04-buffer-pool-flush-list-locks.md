# Buffer Pool 13.1d free/LRU/flush 子锁 + 真实 flush list

## Goal

- 完成 Buffer Pool §13.1 剩余的 list 子锁拆分：`freeListLock`、`lruListLock`、`flushListLock`。
- 引入真实 `DirtyPageList`，让 dirty view 从全 frame 扫描改为 flush-list 快照 + frame 复核。
- 保持 `BufferPool` public API、`FlushCoordinator` WAL gate/doublewrite 协作协议不变。
- 所有 PageStore IO、PageLoadFuture wait、dirty victim flush 入口前继续断言不持任何 Buffer Pool 内部锁。

## Key Decisions

- `BufferPoolInstanceLatchSet` 保留旧 `lockMetadata` 作为包内测试兼容入口，生产路径使用具体 list 锁。
- `DirtyPageList` 只保存 `PageId`、oldest LSN、newest LSN，不保存 `BufferFrame`，避免 flush 模块持有内部状态。
- `dirtyPageCandidates` 两阶段执行：先持 `flushListLock` 复制候选，再释放 list 锁，通过 `pageHashLock + frameMutex` 复核状态；fixed DIRTY 页仍作为 dirty view 暴露，是否可写盘由 `snapshotForFlush` 决定。
- `FLUSHING` 页仍留在 flush list 中约束 checkpoint；候选枚举跳过 FLUSHING，避免重复刷同一页。
- LRU victim 选择两阶段执行：先持 `lruListLock` 复制 victim 顺序，再释放 LRU 锁逐帧复核，避免 list 锁等待 frame 锁。
- free list 仅保护空闲 frame 队列；frame 出队后由 miss/newPage 线程在 `frameMutex` 下重新绑定。

## Non-goals

- 不新增 `DIRTY_PENDING`、`EVICTING`、`STALE` 状态。
- 不改变 legacy `flush/flushAll` 的 WAL/doublewrite 语义；生产 WAL-safe flush 仍走 flush 模块。
- 不做 FlushList/LRU 双 doublewrite 文件、生产 batch dispatch、warmup IO 限速或 random read-ahead 配置化。
- 不改变 `BufferPool`、`FlushCoordinator`、`CheckpointCoordinator` 的公开接口。

## Acceptance Tests

- `DirtyPageListTest`：同页 upsert 去重、oldest 保持、newest 刷新、remove 后清 dirty 边界。
- `BufferPoolFlushListLockSplitTest`：重复写同页只返回一个候选，FLUSHING 跳过候选但仍约束 checkpoint，snapshot 后重写不会被 completeFlush 误清。
- list 锁守卫测试：持 free/LRU/flush 任一锁进入 IO 守卫应抛 `BufferPoolLatchViolationException`。
- 既有 `BufferPoolDirtyViewTest`、`BufferPoolFlushingStateTest`、`LruBufferPoolMultiInstanceTest`、flush 包测试不倒退。

## Current Map Update

- Buffer Pool current flow 标记 13.1d 已拆 free/LRU/flush list 子锁。
- Dirty page mark / checkpoint feed 改为基于 `DirtyPageList` 的真实 flush list。
- Known gaps 中删除 “freeList/lruList/flushList 子锁与真实 flush list”，保留 `DIRTY_PENDING/EVICTING/STALE`、legacy flush 去留和 DDL lifecycle。

## Verification

- 先运行新增测试并确认 RED，再实现。
- GREEN 后运行 `gradle test --tests cn.zhangyis.db.storage.buf.*`。
- 再运行 `gradle test --tests cn.zhangyis.db.storage.flush.*` 与全量 `gradle test`。
- 静态扫描生产代码不新增 Java monitor 调用或裸 `IllegalArgumentException/RuntimeException`。
