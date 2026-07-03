# Slice: Buffer Pool stale-frame 版本语义（0.22）

依据：`innodb-buffer-pool-design.md` §5.5、§5.7、§13.4、§13.5；
`innodb-disk-manager-design.md` §8；`current-implementation-map.md` Buffer Pool + Disk Manager 缺口。
前置：0.10d 多 instance + `PageHashTable` 已落；`invalidateTablespace` 已能两阶段 drain/remove；
`TablespaceAccessController` 已提供 per-space S/X operation lease。

## 目标

- 为 Buffer Pool 引入按表空间递增的生命周期版本和短维护窗口，区分同一 `SpaceId` 在 truncate/drop/discard 前后的 page frame。
- `PageHashTable` 命中、miss 注册、LOADING 发布、等待线程重查、read-ahead prefetch 都必须复核当前 space version。
- `invalidateTablespace` 在持有表空间 X lease 的维护窗口内先阻止新 frame admission；drain 成功后再发布新版本，使旧 frame 即使仍被等待者或旧引用观察到，也不能返回普通读路径。
- 保持现有两阶段 all-or-nothing invalidate 语义：任一分片 dirty/timeout 失败时不发布版本、不移除 frame。

## 关键决策

- 新增 `TablespaceVersion` 值对象和 `SpaceLifecycleClock`，由 `LruBufferPool` facade 持有并注入所有 `BufferPoolInstance`。
- `SpaceLifecycleClock` 暴露 begin/advance/finish/abort invalidation：begin 阻止新 admission 但不推进版本；advance 在 drain+clean 成功后推进版本且继续关闭 admission；finish 在全部分片移除后重新开放；abort 在失败时开放且不 bump。
- `BufferFrame` 记录 `spaceVersion`：frame 绑定 page 时写入当前版本；FREE 时清空或重置为无效版本。
- `PageHashTable` 不直接访问 registry；它只按 `PageId` 查表，版本判断由 instance 在同一 `instanceLock` 临界区完成。
- 命中路径：`pageHash.get(pageId)` 后若 frame 版本不等于 clock 当前版本，先从 hash/LRU 隔离旧 frame，再按 miss 处理。
- admission 路径：foreground `getPage/newPage` 若命中维护窗口则抛新增 `BufferPoolStalePageException`；prefetch 命中维护窗口则静默跳过。
- LOADING 等待路径：等待线程只保留 `PageLoadFuture`，醒来后回环重新读取 hash，并再次校验版本；不复用旧 frame 引用。
- loader 发布路径：读盘完成后重入 `instanceLock`，若当前版本仍等于注册版本且不在维护窗口才发布 CLEAN；否则移除占位、复位 FREE、通知等待者重试。
- prefetch 路径按同一规则处理；版本变化或空间被维护时静默丢弃预取，不影响前台访问。
- 版本发布只发生在 invalidate drain+clean 全部成功后、移除 frame 前；维护窗口持续到全部旧 frame 移除完成，失败路径不会让正常空间产生无意义版本跳变。
- 0.22 只解决 Buffer Pool 内旧 frame 可见性，不改变 `TablespaceRegistry` 的状态准入职责。

## 非目标

- 不实现完整 DROP/DISCARD DDL、不删除 data file、不持久化 GENERAL lifecycle；这些仍留 DDL / Disk Manager 后续切片。
- 不做 §13.1 per-instance 拆分锁；仍沿用每个 `BufferPoolInstance` 一把 `instanceLock`。
- 不扩展 recovery discovery、page0 checksum、warmup IO 限速或 random read-ahead 配置。
- 不改变 B+Tree、Record、Redo、MTR 的公开语义；它们只通过普通 page fix 自动获得 stale 防护。

## 验收测试

- `invalidateTablespacePublishesNewVersionAndRejectsOldFrame`：驻留 clean frame 后 invalidate，旧 frame 被移除，后续 `getPage` 重新 miss/read，不返回旧内容引用。
- `invalidationWindowRejectsNewForegroundAdmission`：invalidate begin 后，直接 `getPage/newPage` 不能注册新 LOADING，占位不进入 hash。
- `waitingThreadRechecksVersionAfterLoadFutureCompletes`：第二线程等待同页 LOADING，future 完成后若 invalidate 已移除该页，必须回环重查，不直接 latch 旧 frame。
- `prefetchSkipsDuringInvalidationWindow`：read-ahead 命中维护窗口时静默跳过，resident list 不包含旧页。
- `invalidateFailureDoesNotBumpVersion`：仍有 dirty/fixed frame 时 invalidate 抛异常，space version 不变，既有 frame 仍可按当前规则访问。
- 全量回归不倒退，重点覆盖 `UndoTablespaceTruncationService` 与 buffer pool 多 instance 测试。

## current map 更新要求

- Buffer Pool 小节把 0.22 从缺口更新为已接：说明 `SpaceLifecycleClock`、frame version、hash/load/prefetch 复核链路。
- Disk Manager / UNDO truncate 链路补充：X lease + flushThrough + invalidate 成功后发布新 buffer space version。
- Known gaps 保留 DROP/DISCARD DDL、普通 lifecycle 持久化、§13.1 拆分锁和完整 discovery，不把它们写成已实现。
