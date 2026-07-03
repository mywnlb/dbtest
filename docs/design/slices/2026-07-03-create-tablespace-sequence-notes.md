# createTablespace Method Sequence Notes

本文记录 `DiskSpaceManager#createTablespace` 的当前源码时序分析，方便复制和复查。它不是新的实现计划，不改变目标架构，也不表示新增切片范围。

## Scope

- 入口方法：`src/main/java/cn/zhangyis/db/storage/api/DiskSpaceManager.java:142`
- 默认重载：`createTablespace(mtr, spaceId, path, initialSizePages)` 只转调 5 参重载，并默认 `TablespaceType.GENERAL`。
- 分析范围覆盖物理文件创建、page0 FSP header 写入、UNDO lifecycle marker、extent0 保留、runtime registry 发布，以及外层 MTR commit 的脏页与 redo 语义。

## Main Sequence

1. `DiskSpaceManager#createTablespace` 先校验 `mtr`、`spaceId`、`path`、`initialSizePages`、`type`。这一步还没有创建文件，也没有访问 Buffer Pool。

2. `DiskSpaceManager` 调 `pageStore.create(spaceId, path, pageSize, initialSizePages)` 创建物理表空间文件。

3. 当前 `PageStore` 实现为 `FileChannelPageStore#create`：先检查 `handles` 是否已有相同 `spaceId`，再调 `DataFileHandle.create`，最后用 `handles.putIfAbsent` 原子登记物理句柄。若输给并发 create，会关闭刚创建的句柄并尝试删除孤儿文件。

4. `DataFileHandle.create` 使用 `CREATE_NEW + READ + WRITE` 打开文件，并调用 `zeroFill(channel, 0, pages, pageBytes)` 从第 0 页到初始页数逐页写零。文件存在或 IO 失败会抛 `DataFilePhysicalException`。

5. 物理文件创建成功后，`DiskSpaceManager` 构造 `SpaceHeaderSnapshot fresh`。快照包含 `spaceId`、`pageSize`、由 `TablespaceTypeFlags.encode(type)` 得到的 `spaceFlags`、`currentSizeInPages=initialSizePages`、`freeLimitPageNo=0`、`nextSegmentId=1`、三个空 FLST base、`firstInodePageNo=2`、`serverVersion=80046`、`spaceVersion=1`。

6. `DiskSpaceManager` 调 `headerRepo.initialize(mtr, fresh)` 初始化 page0。

7. `SpaceHeaderRepository#initialize` 通过 `mtr.getPage(pool, page0, EXCLUSIVE)` 获取 page0 的 X latch，然后写 `FSP_HDR` 物理页信封和 FSP header 字段。写入字段包括 `SPACE_ID`、`PAGE_SIZE_BYTES`、`SPACE_FLAGS`、`CURRENT_SIZE`、`FREE_LIMIT`、`NEXT_SEGMENT_ID`、三个 extent list base、`FIRST_INODE_PAGE`、`SDI_ROOT`、`SERVER_VERSION`、`SPACE_VERSION`。

8. `MiniTransaction#getPage` 进入 `fix`：确认 MTR 为 `ACTIVE`，先获取表空间共享 operation lease，检查同一 MTR 内的 S/SX 到 X 非法升级，然后调用 `pool.getPage(pageId, EXCLUSIVE)`。

9. `LruBufferPool` 将 page0 路由到所属 `BufferPoolInstance`。`BufferPoolInstance#acquire` 做表空间版本准入，查 page hash。page0 未驻留时选 victim、注册 `LOADING` 占位、出内部锁读盘、成功后发布为 `CLEAN`；page0 已驻留时直接增加 `fixCount`。随后按 `EXCLUSIVE` 模式获取 frame 的 write lock，返回 `PageGuard`。

10. `MiniTransaction#fix` 在拿到 `PageGuard` 后调用 `guard.attachWriteListener(collector)`，把本 MTR 的 `MtrRedoCollector` 挂到 guard 上，然后把 guard 压入 `MtrMemo`。从这一刻开始，后续 `PageGuard.writeInt/writeLong/writeBytes` 都会回调 collector 记录 redo。

11. `PageEnvelope.writeHeader` 通过 `PageGuard.writeInt/writeLong` 写页信封字段。`SpaceHeaderRepository#initialize` 的其它字段写入也都走 `PageGuard.write*`。

12. `PageGuard.write*` 要求当前 guard 是 `EXCLUSIVE`，做边界检查，写入 frame buffer，设置 `wrote=true`，然后 `notifyWrite(offset, length)`。`notifyWrite` 从 frame buffer 读回刚写入的字节，并调用 `MtrRedoCollector.onWrite`。

13. `MtrRedoCollector.onWrite` 为每次物理字段写追加一条 `PageBytesRecord(pageId, offset, newBytes)`，并把 page0 加入 `touchedPages`。因此 create tablespace 的 page0 header 写入已经在 MTR 内形成 redo records，但尚未 append 到 redo manager。

14. 如果 `type == TablespaceType.UNDO`，`DiskSpaceManager` 将初始状态设为 `ACTIVE`，并调用 `headerRepo.writeLifecycle` 写 UNDO lifecycle header。该方法再次通过 `mtr.getPage(pool, page0, EXCLUSIVE)` 拿 page0 X guard，同一 MTR 内 X 到 X 可重入；写 lifecycle magic、format、state、initial size、epoch、target size、finish state。这些写同样形成 `PageBytesRecord`。

15. 如果 `type != TablespaceType.UNDO`，初始状态为 `NORMAL`，不写 lifecycle header。

16. `DiskSpaceManager` 调 `xdes.reserveSystemExtent(mtr, spaceId)` 保留 extent0。

17. `ExtentDescriptorRepository#reserveSystemExtent` 通过 `mtr.getPage(pool, page0, EXCLUSIVE)` 修改 page0 内嵌 XDES entry：将 extent0 状态写为 `FSEG_FRAG`，owner 写 0，prev/next 写 NULL，bitmap 清零，然后把 page0、page1、page2、page3 标记为已分配。所有字段写仍通过 `PageGuard.write*` 进入 redo collector。

18. `DiskSpaceManager` 最后调用 `registry.replace(tablespaceMetadata(...))` 发布 runtime metadata。`tablespaceMetadata` 使用建表参数直接构造 metadata，不走 page0 loader，因为 page0 可能还只在本 MTR 的 Buffer Pool 修改中，尚未刷盘。

19. `CachingTablespaceRegistry#replace` 用 metadata 构造 `TablespaceHandle` 并 `handles.put(metadata.spaceId(), handle)`。从此后续普通空间管理 API 可以通过 registry 看见该表空间状态。

## Commit Sequence Outside createTablespace

`createTablespace` 本身不提交 MTR。典型调用方如 `StorageEngine` fresh bootstrap 会在调用后执行 `miniTransactionManager.commit(boot)`。

1. `MiniTransaction#commit` 先把状态切到 `COMMITTING`。

2. 调 `redoLogManager.append(collector.records())` 追加本 MTR 收集的 `PageBytesRecord`，得到 `LogRange` 和 `endLsn`。

3. 调 `collector.disable()`，避免后续 pageLSN 盖戳本身再次进入 redo。

4. 遍历 `collector.touchedPages()`，对 page0 调 `PageEnvelope.stampPageLsn(memo.guardFor(page0), endLsn)`。`memo.guardFor` 要求 page0 仍有 X guard 在 memo 中，这也是 written page 不允许提前释放 X guard 的原因。

5. 调 `memo.releaseAll()` 按 LIFO 释放资源。`PageGuard.close` 先释放 page latch，再回调 `BufferPoolInstance.release(frame, wrote)`。

6. `BufferPoolInstance.release` 在 `frameMutex` 下处理 release。若 `wrote=true`，调用 `markDirty(frame)`。

7. `markDirty` 从页头读取刚盖好的 pageLSN，更新 `oldestModificationLsn`、`newestModificationLsn`、`dirtyVersion`，设置 `dirty=true`，并把 frame 状态转为 `DIRTY`。

8. `MiniTransaction#commit` 在 memo 释放完成后调用 `redoLogManager.markClosed(range)`，最后状态切到 `COMMITTED` 并返回 `endLsn`。

## Important Semantics

- 物理文件创建先发生，page0/FSP 元数据写入后发生。
- page0 写入不是裸文件写，而是通过 `MiniTransaction -> BufferPool -> PageGuard` 修改 Buffer Pool frame。
- page0 的每个字段写都会通过 `PageGuard` 回调 `MtrRedoCollector`，记录为 `PageBytesRecord`。
- `createTablespace` 结束时，redo records 只是收集在 MTR 内；append redo、盖 pageLSN、释放 latch/fix、标记 dirty 都发生在外层 MTR commit。
- registry 发布在 page0 初始化、UNDO lifecycle 写入、extent0 保留之后。
- `GENERAL` 表空间发布状态为 `NORMAL`；`UNDO` 表空间写持久化 lifecycle header，并发布状态为 `ACTIVE`。
- 当前方法没有完整 DDL 事务补偿：物理文件创建成功后，如果后续 page0 初始化或 registry 发布失败，方法内没有显式删除文件或关闭句柄的补偿逻辑。
