# Slice: Engine Bootstrap E1 — 最小 durable StorageEngine 组合根 + WAL

- 日期：2026-06-23
- 关联设计：`innodb-storage-engine-overview.md`（八层架构/组合）；`innodb-disk-manager-design.md` §3/§13；`innodb-redo-log-design.md`（durable redo/WAL）；`innodb-flush-checkpoint-doublewrite-design.md`（WAL gate/`FlushService.flushThrough` barrier）。
- 前置（已实现，均 test-wired，生产构造点=0）：`fil.io.FileChannelPageStore`、`RedoLogFileRepository`(AutoCloseable)/`RedoLogManager.durable`、`RedoRecoveryReader`、`fil.access.TablespaceAccessController`、`LruBufferPool`、`MiniTransactionManager(controller, redo)`、`DiskSpaceManager(pool, store, pageSize, controller)`、`TransactionSystem`/`TransactionManager`、undo（`UndoLogSegmentAccess`/`RollbackSegmentSlotManager`/`HistoryList`/`UndoLogManager`）、`api.index.IndexPageAccess`/`SplitCapableBTreeIndexService`、`MvccReader`/`RollbackService`、`FlushCoordinator`/`flush.doublewrite.NoDoublewriteStrategy`/`flush.checkpoint.CheckpointCoordinator`/`RedoCheckpointStore`/`FlushService`(+`RedoCapacityPolicy`/`flush.policy.AdaptiveFlushPolicy`)。
- 定位：engine bootstrap 第一片（E1）。把散落在测试里的组件接线收进一个生产组合根，使 **WAL 顺序在生产 flush/checkpoint 路径与 commit 之间成立**（redo durable 必先于 data page 写数据文件）。E2 启动崩溃恢复 / E3 后台 driver / E4 DML facade 建在它之上。
- WAL 限制（评审 #1，明确写出）：当前 `LruBufferPool` 脏页淘汰（`writeBack`）直接写 data page、**不走 WAL gate**（current map 已知缺口）。E1 **不**改这条高扇出路径，而是假设 E1 工作负载下不发生脏页淘汰（buffer 容量 > 工作集；前台、无后台 flush，dirty 仅在 close/显式 checkpoint 清空）。WAL-safe 淘汰（evict-clean-only 或淘汰路径接 WAL gate）留独立切片。

## 1. 范围

做：
- 新增 `cn.zhangyis.db.storage.engine.EngineConfig`（record）+ 默认/文件布局：`baseDir`、`pageSize`、`bufferPoolCapacityFrames`、`undoSpaceId`、`undoSpaceInitialPages`、`slotCapacity`、`maxVersionHops`、`flushTimeout`(Duration)、`redoCapacityBytes`。文件布局固定在 baseDir 下：`redo.log`、`redo-control`、`undo_<undoSpaceId>.ibu`；数据表空间文件路径由调用方建/开（无 data dictionary）。构造校验非空/正数。
- 新增 `cn.zhangyis.db.storage.engine.StorageEngine`（组合根 + 生命周期）+ `EngineState`{NEW,OPEN,CLOSED} + `EngineStateException`。按依赖方向接线全部组件，**共享单一 `TablespaceAccessController`** 注入 MTR/loader/disk/flush；redo = `RedoLogManager.durable(repo)`，引擎**持有 `RedoLogFileRepository`/`RedoCheckpointStore`（AutoCloseable）并在 close 关闭它们**（`RedoLogManager` 非 closeable，评审 #3）。
- `open()`：fresh（baseDir 无 `redo.log`）→ 建 redo 文件 + 建系统 undo 表空间（`undoSpaceId`，`createTablespace(... UNDO)`）+ 接线 → OPEN；existing → 开 redo 文件 + `openTablespace(undoSpaceId)` + **安装 redo 边界**（评审 #5：`RedoRecoveryReader(repo, checkpointLabel.checkpointLsn()).recoveredToLsn()` → `redo.restoreRecoveredBoundary(...)`，**只读边界不 replay/不 repair**）→ 接线 → OPEN。**E1 不跑崩溃恢复**（clean-shutdown 模型）；边界安装使重开后续写 LSN 连续、不重叠。
- `checkpoint()` / `close()`：**经 `FlushService.flushThrough(redo.currentLsn(), flushTimeout)`** 完成 WAL 顺序持久——内部先 `redo.flush()`（durable）再经 WAL-gated `FlushCoordinator` 刷出 oldest≤marker 的全部 dirty page，再推进/持久 checkpoint。`close()` 末**先确认 buffer 无 dirty**（flushThrough 已清空）再关 pool（使 `LruBufferPool.close→flushAll` legacy 路径为 no-op，不绕 WAL gate，评审 #2），再关 store/redo repo/checkpoint store；CLOSED。
- 访问器：`transactionManager()`/`miniTransactionManager()`/`diskSpaceManager()`/`btreeService()`/`undoLogManager()`/`mvccReader()`/`rollbackService()`，供测试与未来 DML facade 驱动事务；E1 组合根自身不含 DML 逻辑。

不做：
- 崩溃恢复启动（E2）：`open()` 不 redo replay、不 doublewrite repair、不 undo rollback（用 `NoDoublewriteStrategy`）；E1 仅 clean-shutdown 持久 + 重开边界连续。
- 后台线程（E3）：无 `PageCleanerWorker`/checkpoint tick/purge driver；flush/checkpoint 仅在 `close()`/显式 `checkpoint()` 前台触发。
- WAL-safe 脏页淘汰（评审 #1）：不改 `LruBufferPool.writeBack`；E1 假设无脏页淘汰（容量 > 工作集），留独立切片。
- `PurgeCoordinator` 接线（需 per-index，留 E4/DML）；`HistoryList` 仍接线（`UndoLogManager` 依赖）。
- DML facade/session（E4）；多数据表空间编目（无 data dictionary）。

## 2. 数据流与不变量

1. 写：调用方经访问器 `begin → assignWriteId → undoMgr.beforeX → svc.Xclustered → txnMgr.commit + undoMgr.onCommit`；commit 只 append durable redo（内存累积），dirty page 留 buffer。
2. 持久（close/checkpoint）：`FlushService.flushThrough(currentLsn)` → **redo.flush() 必先于任何 data page 写数据文件**（WAL）→ 持久 checkpoint。
3. 重开（existing open）：读 checkpoint label + `RedoRecoveryReader.recoveredToLsn` → 安装 redo 边界（不 replay）；数据来自 clean close 已刷数据文件；后续 append 从边界连续。
4. 不变量：①WAL 顺序在 flush/checkpoint 与 commit 之间成立（redo durable 先于 data page），**前提是无脏页淘汰**（评审 #1，已写为 E1 假设/缺口）；②clean close 后数据文件含已提交行，重开（不 replay）即可读回；③共享一个 `TablespaceAccessController`；④`open` 后 OPEN、`close` 后 CLOSED、CLOSED 上操作抛 `EngineStateException`；⑤E1 不声明崩溃安全（仅 clean-shutdown + 重开边界连续），crash recovery 由 E2 补齐。

## 3. 错误与并发约束

- 生命周期非法转换（重复 open、CLOSED 上操作）抛 `EngineStateException`；配置非法抛 `DatabaseValidationException`。
- `close()` 幂等：已 CLOSED 再 close 为 no-op；close 中途 IO 失败保留根因，尽量关闭其余 AutoCloseable 句柄后汇总抛。
- E1 前台单线程使用假设（无后台线程）；共享 `TablespaceAccessController` 的 lease 超时/释放语义沿用既有。
- redo flush 持锁跨 fsync、`flushThrough` 的 drain/checkpoint barrier 语义沿用既有，不在本片改。

## 4. 验收测试与 current map

- 组合/生命周期：`open()` 后各访问器非空、状态 OPEN；`close()` 幂等；CLOSED 上访问/操作抛 `EngineStateException`；配置校验（null baseDir/非正 pageSize/容量）被拒。
- **durable 往返（核心）**：`open(fresh, tempDir)` → 经访问器建数据表空间 + 聚簇 root → 一事务写一行 → `close()` → 新 `StorageEngine` `open(existing, 同 dir)` + `openTablespace(data)` → btree `lookup` / `mvccReader` 读回该行（值正确）。
- **重开后续写连续（评审 #5）**：上述重开后再写第二行 + `close()` → 第三次 open + 读回两行；断言 redo 批次 LSN 连续（不从 0 重叠）。
- WAL 顺序：close/checkpoint 后 `redo.flushedToDiskLsn() ≥` 被刷脏页 pageLsn；且 `flushThrough` 后 buffer 无 dirty（pool.close 的 flushAll 为 no-op）。
- 固定 JDK/Gradle 全量 `test` 不回退；更新 `current-implementation-map.md`：新增 “Engine Bootstrap Slice”（StorageEngine/EngineConfig 数据链 + 生命周期 + WAL 成立点 + 脏页淘汰缺口），把 buf/mtr/disk/redo/flush/trx/undo 各 Reserved/Unwired 的“无生产组合根”项校正为“E1 StorageEngine 已接线”（保留 E2/E3/E4 缺口 + WAL-safe 淘汰缺口）。
- 已知遗留（留后续）：崩溃恢复启动（E2）、后台 driver/purge driver（E3）、DML facade（E4）、WAL-safe 脏页淘汰、doublewrite 修复、多表空间编目。
