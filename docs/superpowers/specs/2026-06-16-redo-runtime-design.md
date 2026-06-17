# Spec：R1 — redo runtime + recovery page replay

- 日期：2026-06-16
- 关联设计：`docs/design/innodb-redo-log-design.md` §5-§13、§17、§20-§24；`docs/design/innodb-crash-recovery-design.md` §7.4、§8.1；`docs/design/innodb-flush-checkpoint-doublewrite-design.md` §8.2。
- 前置：D3（MTR redo append）、D4a（allocatePage PAGE_INIT）、D4b（IndexPageAccess）。
- 状态：把 D3 内存 redo 扩展为可持久化、可等待 durable、可扫描并回放 PAGE_INIT/PAGE_BYTES 的最小恢复闭环。

## 1. 范围

**做：**
- 新增 redo 文件仓储 `RedoLogFileRepository`，按 MTR batch 追加写入单个 redo 文件。
- 新增同步 `RedoLogWriter` / `RedoLogFlusher`，由 `RedoLogManager.flush()` 显式驱动写入和 fsync。
- `RedoLogManager` 保留 D3 默认内存模式；通过 `RedoLogManager.durable(repo)` 创建持久化模式，新增 `flushedToDiskLsn()`、`waitFlushed(target, timeout)`、`flush()`。
- 新增 `RedoRecoveryReader` 扫描完整 batch，遇到不完整尾部停止。
- 新增 `RedoApplyDispatcher` + `PageRedoApplyHandler`，按 batch 回放 `PAGE_INIT` / `PAGE_BYTES`，用 pageLSN 幂等跳过。

**不做：**
- 不做循环 redo 文件、capacity pressure、checkpoint label、recent written/recent closed、后台 writer/flusher 线程。
- 不做 checksum/trailer LSN 盖戳；pageLSN 仍只写 header，F1 负责 checksum/trailer/doublewrite。
- 不做逻辑 redo、事务状态 redo、undo redo。

## 2. 关键决策

1. **批次是恢复幂等边界**：D3 commit 的所有 record 共享 batch endLsn。回放时不能在 `PAGE_INIT` 后立即盖 pageLSN，否则同批后续 `PAGE_BYTES` 会被误跳过；必须批内同页记录全部应用后统一盖 batch endLsn。
2. **默认构造器保持内存语义**：`new RedoLogManager()` 不写文件，避免影响 MTR/FSP 既有测试；持久化只通过 `durable(repo)` opt-in。
3. **等待必须有 timeout**：`waitFlushed` 只等待 condition，不主动 flush；返回 false 表示 timeout 或中断，避免无界等待。
4. **不完整尾部停止，完整损坏失败**：reader 扫描到 frame header/payload 不完整时停止，模拟 crash torn tail；magic/length/checksum/record tag 损坏视为 `RedoLogCorruptedException`。
5. **Recovery 不依赖 BufferPool/MTR**：page handler 直接通过 `PageStore` 读写整页，保持 crash recovery 物理层边界。

## 3. 数据流

MTR commit / 测试 append：
`RedoLogManager.append(records)` → 分配 `LogRange` → 记录 `RedoLogBatch` → durable 模式放入 pending 队列。

Durability：
`flush()` → `RedoLogWriter.write(batch)` 追加 redo 文件 → `RedoLogFlusher.flushTo(writtenLsn)` force → 推进 `flushedToDiskLsn` → `signalAll` 唤醒 `waitFlushed`。

Recovery：
`RedoRecoveryReader.readBatches()` → `RedoApplyDispatcher.applyAll` → `PageRedoApplyHandler` 按 batch 缓存页 → pageLSN 已覆盖 batch endLsn 的页跳过 → 应用 `PAGE_INIT/PAGE_BYTES` → 批末盖 pageLSN → `PageStore.writePage`。

## 4. 测试

- `flushPersistsBatchAndWaitsForDurability`：append 后 `flushedToDiskLsn=0`，flush 后 durable，重开 reader 能读回 batch。
- `waitFlushedTimesOutWhenNoFlusherAdvances`：不调用 flush 时 wait 超时返回 false。
- `recoveryAppliesPageInitAndBytesThenStampsBatchEndLsn`：回放后页类型、payload、pageLSN 均正确。
- `recoverySkipsPageWhosePageLsnAlreadyCoversBatch`：pageLSN 已覆盖 batch 的页不被重写。
- `recoveryReaderStopsAtIncompleteTail`：完整 batch 后追加 torn tail，reader 只返回完整 batch。
- `batchRejectsRangeNotMatchingRecordLength`：`RedoLogBatch` 的 `LogRange` 必须精确等于批内 record 字节数，避免恢复 reader 接受错位批次。
- `RedoRecordTest` 固定 `PAGE_INIT` 和 `PAGE_BYTES` 的 `byteLength()` 与 R1 文件编码一致：`PAGE_INIT=17`，`PAGE_BYTES=21+payloadLength`。
- `waitFlushedDoesNotOverflowTimeoutWhenTargetAlreadyDurable`：目标 LSN 已 durable 时先快速返回，避免极大 timeout 转纳秒溢出。
- `pageBytesOverflowIsReportedAsRedoCorruption`：`PAGE_BYTES` 偏移加长度使用 long 边界校验，越界必须暴露为 redo 损坏异常。
- `readerWrapsCompleteInvalidPayloadAsRedoCorruption`：完整 frame 中的非法 payload 必须统一包装为 `RedoLogCorruptedException`，不能泄漏领域校验异常。

## 5. 简化点与后续

R1 是 redo runtime 的最小闭环，不替代后续 F1/R2：
- F1 接入 dirty page flush、doublewrite、checksum/trailer low32 LSN、checkpoint safe LSN。
- 后续 redo R2 可加入 checkpoint label、capacity pressure、后台 writer/flusher、恢复阶段总控。
