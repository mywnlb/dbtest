# Spec：F1 — flush/checkpoint/doublewrite first slice

- 日期：2026-06-16
- 关联设计：`docs/design/innodb-disk-manager-design.md` §8.1、§10-§12、§18；`docs/design/innodb-flush-checkpoint-doublewrite-design.md` §1-§16；`docs/design/innodb-redo-log-design.md` §5.9、§7、§10-§12、§15-§17、§20-§21。
- 前置：R1（redo runtime + recovery page replay）、D4b（record.page MTR 生产入口）。
- 状态：在现有 BufferPool/PageStore/RedoLogManager 之上实现最小可测试 F1：脏页 flush 前强制 redo durable，通过 doublewrite 防 torn page，并以 fuzzy checkpoint 安全边界暴露后续 recovery 起点。

## 1. 范围

**做：**
- 扩展 `PageStore` / `DataFileHandle`，提供 data file `force(SpaceId)`，让 flush 在 data page write 后具备可测试的物理持久化边界。
- 扩展 `BufferPool` dirty view：脏页候选、flush snapshot、flush 完成 clean/keep-dirty 回调、oldest dirty LSN 查询。
- 新增 page image checksum 工具，支持对 flush snapshot 的 byte[] 盖 header checksum、trailer checksum 和 trailer low32 LSN。
- 新增 `storage.flush` 最小协调器：`FlushCoordinator.flushList(targetLsn,maxPages)` 与 `singlePageFlush(pageId)`。
- 新增 doublewrite 最小实现：`OFF` 与 `DETECT_AND_RECOVER`；写 data file 前先写完整 page 副本并 force doublewrite file。
- 新增 doublewrite recovery scanner：发现 data file torn/corrupt 时优先用 doublewrite full copy 修复，再交给 redo replay。
- 新增 in-memory `CheckpointCoordinator`：`safeCheckpointLsn = min(oldestDirtyLsnOrCurrent, redo.currentLsn, redo.flushedToDiskLsn)`；R1 同步 append 下 `currentLsn` 代表 closed LSN 的简化边界。

**不做：**
- 不做后台 page cleaner 线程、adaptive flushing、IO capacity、neighbor flush、tablespace drain。
- 不做 persistent checkpoint label、redo capacity pressure、redo 文件回收。
- 不做 detect-only 模式的完整告警工作流；F1 只保留 enum 扩展点，不把 detect-only 作为默认路径。
- 不改 record/btree/operator API；flush 只处理物理页。

## 2. 设计依据压缩

来自三份设计文档的硬约束：

- Disk Manager：flush 只能通过 `PageStore.writePage()` 写 data file，不能直接碰 `FileChannel`；physical file locks 由 `storage.fil` 维护；data file flush 不能持有 space header、XDES、inode page latch。
- Flush/Doublewrite/Checkpoint：标准顺序必须是 dirty candidate -> redo durable gate -> snapshot -> checksum -> doublewrite durable -> data file write -> BufferPool clean/keep-dirty -> checkpoint notify。
- Redo：数据页落盘前必须满足 `redo.flushedToDiskLsn >= page.pageLsn`；checkpoint 不能只看 redo flushed LSN，必须同时看 dirty oldest 与 closed/current LSN；doublewrite 修 torn page，redo 补逻辑上已记录但未落盘的物理修改。

## 3. 关键对象

### 3.1 BufferPool dirty view

- `DirtyPageCandidate(PageId pageId, Lsn oldestModificationLsn, Lsn newestModificationLsn)`：flush list 候选，只暴露逻辑边界，不暴露 `BufferFrame`。
- `FlushPageSnapshot(PageId pageId, Lsn pageLsn, long dirtyVersion, byte[] pageImage)`：flush 期间的稳定整页副本。snapshot 只在 frame 未 fixed 且 dirty 时产生，产生后不跨 IO 持有 page latch 或 pool lock。
- `BufferPool.dirtyPageCandidates(Lsn targetLsn, int maxPages)`：按 oldestModificationLsn 升序选候选。
- `BufferPool.snapshotForFlush(PageId pageId)`：返回 `Optional<FlushPageSnapshot>`；fixed、clean、evicted 页返回 empty。
- `BufferPool.completeFlush(FlushPageSnapshot snapshot)`：若 frame 仍 dirty、未 fixed、dirtyVersion/pageLsn 未变化则 clean；否则 keep dirty。
- `BufferPool.oldestDirtyLsnOr(Lsn cleanBoundary)`：无脏页时返回调用方给定边界。

### 3.2 Flush coordinator

- `FlushCoordinator` 只依赖 `BufferPool`、`PageStore`、`RedoLogManager`、`PageSize`、`DoublewriteStrategy`。
- WAL gate：若 `snapshot.pageLsn > redo.flushedToDiskLsn`，调用 `redo.waitFlushed(snapshot.pageLsn, redoWaitTimeout)`；等待失败则跳过该页且不写 data file。
- 写入顺序：对 snapshot byte[] 盖 checksum/trailer -> doublewrite before -> `pageStore.writePage` -> `pageStore.force(spaceId)` -> `bufferPool.completeFlush`。
- 异常策略：doublewrite 或 data file 写失败时不 clean，返回 `FAILED`，并保留 cause。

### 3.3 Doublewrite

- `DoublewriteMode.OFF`：测试或低可靠性模式，不写副本。
- `DoublewriteMode.DETECT_AND_RECOVER`：写完整 page 副本，slot metadata 包含 magic、format、spaceId、pageNo、pageLsn、pageSize、payloadCrc。
- `DoublewriteFileRepository` 通过自己的 `FileChannel`、`ReentrantLock` 和 `force(true)` 管理 doublewrite 文件；不复用 `PageStore`，因为它不是 tablespace data file。
- `DoublewriteFileRepository.latestCopy(PageId)` 返回最新一份 checksum 合法的 full-copy page image，供测试与 recovery scanner 复用。
- `DoublewriteRecoveryScanner` 扫描最后有效 full-copy slot，读取 data file 对应页，若 data page checksum/trailer 不合法而 doublewrite 副本合法，则用副本修复 data file 并 force。

### 3.4 Checkpoint

- F1 checkpoint 是内存边界，不写 redo control label。
- `CheckpointCoordinator.computeSafeCheckpointLsn()`：
  - `oldest = bufferPool.oldestDirtyLsnOr(redo.currentLsn())`
  - `closed = redo.currentLsn()`，这是 R1 同步 append/dirty publish 的简化 closed LSN。
  - `flushed = redo.flushedToDiskLsn()`
  - `safe = min(oldest, closed, flushed)`
- `advanceCheckpoint()` 只能单调推进 `lastCheckpointLsn`，不能倒退。

## 4. 测试矩阵

- `BufferPoolDirtyViewTest.dirtyCandidatesAreOrderedByOldestModificationLsn`：多脏页按 oldest LSN 排序且受 target/max 限制。
- `BufferPoolDirtyViewTest.completeFlushKeepsDirtyWhenFrameWasModifiedAgain`：snapshot 后再次修改，completeFlush 返回 keep dirty。
- `PageImageChecksumTest.stampAndVerifyPageImageChecksumAndTrailer`：byte[] 盖 checksum、trailer checksum、trailer low32 LSN 后 verify 通过。
- `PageStoreForceTest.forceExistingTablespaceSucceedsAndUnknownTablespaceFails`：data file force 有明确异常边界。
- `FlushCoordinatorTest.flushListSkipsPageWhenRedoIsNotDurable`：`pageLsn > flushedToDiskLsn` 且等待超时，不写 data file、不 clean。
- `FlushCoordinatorTest.flushListWritesDoublewriteBeforeDataFileAndMarksClean`：redo durable 后，doublewrite 文件先有副本，data file 写入，dirty view 变 clean。
- `DoublewriteRecoveryTest.recoverableDoublewriteRepairsCorruptDataPage`：data file checksum 损坏时，用 doublewrite full copy 修复。
- `CheckpointCoordinatorTest.safeCheckpointDoesNotPassOldestDirtyOrRedoDurable`：safe LSN 不越过 oldest dirty、current/closed、flushed 三者任一边界。

## 5. 简化点与后续

- R1 没有 recent-closed tracker，F1 用 `redo.currentLsn()` 作为 closed LSN；后续 redo R2 引入 closed tracker 后替换。
- F1 `PageStore.force(spaceId)` 直接 force data file；后续可加入 `FsyncLock` 并限制并发 fsync 数。
- F1 doublewrite 只实现单文件 append slot；后续按 flush list / LRU file、slot reuse、detect-only metadata 扩展。
- F1 checkpoint 只内存推进；后续写 redo checkpoint label，并驱动 redo 文件回收与 capacity pressure。
