# Current Implementation Map

本文档记录当前生产代码的真实接线和已知缺口。全局设计文档和 `docs/design/diagrams/*.mmd` 表达目标架构；本文件表达当前实现状态。两者不一致时，开发判断当前代码行为以本文件和源码为准，目标演进以对应设计文档为准。

## Maintenance Rules

- 实线只表示当前生产代码已经存在的调用、持有、写入或读写关系。
- 虚线只表示已经决定但尚未闭环的 `planned`、`partial` 或 `unwired` 关系。
- 每个 `unwired` 生产类型都必须写清现状、保留理由和下一步动作。
- 每次实现切片结束后，只更新受影响的小节；只有目标架构变化时才更新全局架构图。
- 本文件不得用未决占位词替代明确判断；如果状态不确定，应写出需要核对的源码入口。

## Storage Disk Manager Slice

### Current Flow

```mermaid
flowchart TD
  Upper["B+Tree / Undo / tests"] --> DSM["storage.api DiskSpaceManager"]
  Upper --> PageAccess["typed page access: api.index IndexPageAccess / UndoPageAccess"]

  DSM --> FSP["storage.fsp repositories and services"]
  DSM --> Store["fil.io PageStore"]
  DSM --> Registry["fil.meta TablespaceRegistry"]
  DSM --> Access["fil.access TablespaceAccessController S lease via MTR"]
  Registry --> Loader["api.tablespace PageZeroTablespaceMetadataLoader"]
  Loader --> Access
  Loader --> Store
  FSP --> BP["storage.buf BufferPool through MiniTransaction"]
  PageAccess --> BP
  PageAccess --> Access
  PageAccess --> Registry
  BP --> Store
  Store --> ChannelStore["fil.io FileChannelPageStore"]
  ChannelStore --> Handle["fil.io DataFileHandle"]
  Handle --> Gateway["fil.io DataFileGateway (create/extend range allocation)"]
  Handle --> DataFile["tablespace data file"]

  Truncate["api.undotruncate UndoTablespaceTruncationService (test-wired)"] --> Access
  Truncate --> UndoCache["UndoReusableSegmentTruncationCoordinator"]
  UndoCache --> RsegPage3["RollbackSegmentHeaderRepository page3 v4"]
  UndoCache --> FSP
  Truncate --> Registry
  Truncate --> FSP
  Truncate --> BP
  Truncate --> Store

```

### Current Data Chains

| Flow | Current production chain | Current state |
| --- | --- | --- |
| Create tablespace | `DiskSpaceManager.createTablespace` -> `PageStore.create` -> `DataFileHandle.create`; then `SpaceHeaderRepository.initialize`（含 page0 FSP_HDR 信封盖戳）; GENERAL writes `TablespaceLifecycleHeader(NORMAL,currentSize,epoch=0)`; UNDO writes `TablespaceLifecycleHeader(ACTIVE,initialSize,epoch=0)`; reserve extent0 and `TablespaceRegistry.replace` | Implemented; GENERAL publishes/persists NORMAL，UNDO publishes/persists ACTIVE；4-arg overload仍默认 GENERAL；page0 现携带统一 FSP_HDR FilePageHeader 信封 |
| Open tablespace | `DiskSpaceManager.openTablespace` -> `PageStore.open`; `TablespaceRegistry.open` -> `api.tablespace.PageZeroTablespaceMetadataLoader` 持 S lease raw 读 page0 -> FSP_HDR 信封校验(pageType==FSP_HDR/pageNo==0，否则 `TablespaceCorruptedException`) -> checksum/trailer 校验（新盖戳页严格校验；历史 header/trailer checksum 同为 0 的 page0 兼容）-> physical/lifecycle codecs | Implemented for already-known path；GENERAL 从 page0 恢复 NORMAL/CORRUPTED，普通访问继续拒绝 CORRUPTED；新 UNDO 恢复持久 ACTIVE/INACTIVE/TRUNCATING，旧 UNDO 无 lifecycle header 时按 NORMAL 打开但禁止 truncate |
| Recovery open | `DiskSpaceManager.openTablespaceForRecovery` -> `PageStore.open` -> `TablespaceRegistry.requireForRecovery` -> page0 loader（同样做 FSP_HDR 信封 + checksum/trailer 校验） | Implemented；允许加载 GENERAL CORRUPTED 与 UNDO TRUNCATING，供启动恢复、诊断和续作 |
| Space-management admission | `DiskSpaceManager.createSegment/allocatePage/freePage/dropSegment/usage` -> `MiniTransaction.acquireTablespaceLease(S)` (`MiniTransaction.java:108`) -> `TablespaceRegistry.require`（lease 后复核）-> FSP | Implemented；拒绝 CORRUPTED/INACTIVE/TRUNCATING/DISCARDED，消除状态先检后等待竞态 |
| Segment drop plan | `UndoSegmentFinalizer` 独立只读 MTR -> `UndoSpaceAllocator.inspectDropPlan` -> `DiskSpaceUndoAllocator` -> `DiskSpaceManager.inspectDropSegmentPlan` -> inode page2 identity + 32 fragment slots + 三条 extent list length | Implemented；plan MTR 在 finalization 写 MTR 前提交，只返回不可变 fragment/extent/used-page 快照；identity/计数损坏 fail-closed，不跨返回持 page2 latch/fix |
| Space reservation | `DiskSpaceManager.reserveSpace` -> ordinary access lease + Registry require -> `SpaceReservationService.reserve` -> page0/FLST 容量快照（不持账本锁）-> `PageStore.ensureCapacity` + `SpaceHeaderRepository.setCurrentSizeInPages` if needed -> capacity counter publish -> `MiniTransaction.enlistResource` | Implemented core + consumers（0.14a/0.14b/1.6 extern undo）；per-process in-memory capacity counters + `SpaceReservationKind`；capacity counter lock 只保护内存承诺计数，不包住 Buffer Pool/page latch/file extend；B+Tree split/root split 以 `NORMAL` 预算调用，Undo 主链 grow 与 external payload 按预规划精确页数以 `UNDO` 预算调用，失败发生在真正 page allocation 和页内容修改前 |
| Allocate page | `DiskSpaceManager.allocatePage(mtr, ref[, PageAllocationHint])` -> `TablespaceRegistry.require` -> optional `SpaceReservationService.consumePageIfReserved` -> `SegmentPageAllocator.allocatePage(..., ExtentAllocationDirection, hintPageNo, pagesNeeded)` -> `DefaultExtentAllocationPolicy` -> `SegmentSpaceService.assignExtentToSegment` -> `FreeExtentService.acquireFreeExtent`; on no space without reservation, `PageStore.extend` then `SpaceHeaderRepository.setCurrentSizeInPages` and retry -> `MiniTransaction.appendLogicalRedo(FspPageAllocationRecord)` -> `mtr.newPage(..., PageType.ALLOCATED)` | Implemented for current FSP model；0.19b 起 allocation intent 以 `FSP_PAGE_ALLOC` 持久化并在 `PAGE_INIT(ALLOCATED)` 前进入同一 MTR batch，recovery handler 只 `ensureCapacity` 不重跑 allocator；0.19c 起 SpaceHeader/XDES/INODE/FLST 写点同时追加 `FspMetadataDeltaRecord`，`freePage`/`dropSegment` fragment 释放追加 `FspPageFreeRecord`；0.19d 起提交视图会过滤被 metadata delta after-image 精确覆盖的 FSP `PAGE_BYTES`，未覆盖的页信封/生命周期字节仍保留物理 redo；no-hint API delegates to `PageAllocationHint.none()` and keeps fragment→segment extent→autoextend behavior；direction hint only affects new extent selection/batch count after fragment slots and existing segment extents are exhausted；`INDEX_LEAF` + UP/DOWN may acquire 2-4 extents, UNDO/non-leaf/NO_DIRECTION stays single extent；active reservation 会先消费 page quota（当前 MTR reservation 的 atomic quota，无全局账本锁，避免持 index page latch 时阻塞在 reservation lock），耗尽则在分配前抛 `SpaceReservationExceededException`；registry size snapshot is not updated after autoextend because page0 remains the size authority；autoextend 现 crash-safe：恢复期 `FSP_PAGE_ALLOC`/`PAGE_INIT` 与 `SPACE_FILE_RECONCILE` 都可把物理文件长度重对齐到 redo 恢复出的逻辑边界 |
| Typed INDEX/UNDO access | production `StorageEngine` 注入共享 `TablespaceRegistry`：`api.index.IndexPageAccess` / `UndoPageAccess` -> `MiniTransaction.acquireTablespaceLease(S)` -> `TablespaceRegistry.require` -> `MiniTransaction.getPage/newPage` -> `BufferPool` -> `PageStore` | Implemented；生产 typed access 在 lease 后拒绝稳定 INACTIVE/CORRUPTED/DISCARDED；两参构造仍保留给低层页格式测试，不做 registry 准入 |
| Dirty page flush | `FlushCoordinator` 持同 space S lease -> snapshot -> WAL gate -> doublewrite -> `PageStore.writePage/force` -> `DataFileHandle.force` 持 per-file `FsyncLock` -> complete | Implemented；与 truncate X 互斥；同一 data file 并发 force 经 `FsyncLock` 串行化 |
| UNDO truncate | `UndoTablespaceTruncationService.truncate` -> X lease -> `UndoReusableSegmentTruncationCoordinator.drainStableSpace` 先拒绝非空 persistent history，再非阻塞取得统一 reuse drain gate、拒绝 active slots、每批最多 8 段按 INSERT cache top→UPDATE cache top→free head 同 MTR FSP drop + page3 owner removal，并在仍有 free 节点时清新 head.prev -> 空 inode -> marker -> `FlushService.flushThrough` -> `LruBufferPool.invalidateTablespace` -> `DataFileHandle.truncateTo` -> `UndoTablespaceFspRebuilder.rebuild` + page3 v4 format -> final state/Registry publish | Implemented (recovery production-wired；主动调用 test-wired)；同 epoch 可故障续作；已有 TRUNCATING marker 必须再次验证 page3 history/active/cache/free 与 FSP inode 均空；稳定文件已等于 initial size 时保持幂等 no-op；reuse gate 忙不在 X 下等待 |
| Redo replay | `RedoApplyDispatcher registry` -> `FspPageAllocationRedoHandler` / `PageRedoApplyHandler` / `TransactionStateRedoHandler` batch sessions -> page records 写 `PageStore`；trx record 经 `TransactionStateDeltaSink` 交给 `RecoveredTransactionTable` | Implemented physical replay path + FSP intent/metadata delta + undo/rseg metadata delta + undo payload + B+Tree structure delta + non-page trx state recovery；page handler 批末一次写回，transaction handler 不触碰 PageStore/事务状态机，正式 `StorageEngine` dispatcher 注入 recovery context sink，通用 dispatcher 保留 no-op sink；recovery discovery is not fully wired to registry |

### Package Status

| Package area | Representative classes | Current state | Notes |
| --- | --- | --- | --- |
| `storage.api` disk facade | `DiskSpaceManager`, `PageAllocationHint`, `SegmentRef`, `SpaceUsage`, `DiskSpaceUndoAllocator` | Implemented | DiskSpaceManager 管普通 FSP；`PageAllocationHint` 是上层页分配方向/邻近页/页需求的稳定 API，DiskSpaceManager 转换为 FSP 内部方向；SegmentRef/SpaceUsage 是门面值对象；undo allocator 是 undo 端口适配器 |
| `storage.api.undotruncate` lifecycle orchestration | `UndoTablespaceTruncationService`, `UndoTablespaceTruncationRecovery`, `UndoReusableSegmentTruncationCoordinator` | Implemented; recovery wired by `StorageEngine` E2 | 可恢复 UNDO 物理收缩与 cache/free owner 排空；E2 existing open 构造恢复参与者用于 TRUNCATING 续作；主动 truncate 仍待 purge/DML 调度 |
| `storage.api.index` typed index page entry | `IndexPageAccess`, `IndexPageHandle` | Implemented | Bridges B+Tree/record code to `MiniTransaction`-owned page guards；生产三参构造注入 registry 后先 lease+require 再 fix/new page |
| `storage.api.tablespace` metadata adapter | `PageZeroTablespaceMetadataLoader` | Implemented | Registry 懒加载协作者；留在 api 侧以避免 `fil` 直接编排 `fsp` page0/lifecycle codec；打开/恢复时先做 page0 FSP_HDR 信封校验(pageType/pageNo)，再做 checksum/trailer 校验；GENERAL marker 接受 NORMAL/CORRUPTED/DISCARDED，UNDO marker 只接受 ACTIVE/INACTIVE/TRUNCATING；历史未盖 checksum 的 page0 仅在两个 checksum 字段同为 0 时兼容 |
| `storage.api.dml` table/clustered DML facades | `TableDmlService`, `ClusteredDmlService`, `TableUpdatePatchCommand`, table/clustered commands, `DmlStatementGuard`, terminal commands/results | Implemented; production-wired by `StorageEngine` and SQL gateway | `TableDmlService` 以聚簇 undo 为逻辑 anchor，按 index id 用独立短 MTR 维护全部 secondary；point UPDATE 在同一次 FOR_UPDATE 锁定旧行后应用 typed patch，未赋值 external 引用原样保留；SQL INSERT/UPDATE/DELETE 均经 statement guard 与 exact-version LOB binding 进入表级服务 |
| `storage.fsp.flst` file-list primitives | `FileAddress`, `Flst`, `FlstBase`, `FlstNode` | Implemented | FSP/XDES/INODE 链表指针与 base/node 编解码；不接触文件 IO；0.19c 起 FLST base/node 写入经 `FspRedoDeltas` 追加 metadata delta，0.19d 起对应物理 `PAGE_BYTES` 被提交过滤器替代 |
| `storage.fsp.header` space header | `SpaceHeaderRepository`, `SpaceHeaderSnapshot`, `SpaceHeaderRawCodec`, `SpaceHeaderPhysical` | Implemented | page0 header 读写与 raw metadata 加载；`initialize` 盖 page0 FSP_HDR FilePageHeader 信封头；layout 常量供 extent/lifecycle codec 共享；0.19c 起 space header 字段写入追加 `FspMetadataDeltaRecord`，0.19d 起被 delta 覆盖的字段字节不再持久化 `PAGE_BYTES`；lifecycle truncate marker 和 page0 信封仍走物理 `PAGE_BYTES` |
| `storage.fsp.reservation` space reservation | `SpaceReservationService`, `SpaceReservation`, `SpaceReservationKind` | Implemented | `DiskSpaceManager.reserveSpace` 生产接线；内存态容量账本，预扩物理文件/page0 currentSize；0.14b 已接 B+Tree split/root split 与 Undo grow 真实消费者；2026-07-03 修正锁边界：reserve 不持账本锁等待 page0/FLST，consume 不取全局账本锁而只改当前 reservation 原子页额度 |
| `storage.fsp.extent` extent management | `ExtentDescriptorRepository`, `ExtentState`, `FreeExtentService`, `ExtentAllocationPolicy`, `ExtentAllocationDirection`, `ExtentAllocationRequest` | Implemented | XDES state/owner/bitmap + 全局 FREE/FREE_FRAG/FULL_FRAG 分配；不打开文件；direction hint 在已材料化 FREE 链中选 UP/DOWN 最近候选，UP 可推进 freeLimit 材料化右侧候选，NO_DIRECTION 保持链头语义；0.19c 起 XDES field/bitmap 写入追加 metadata delta，0.19d 起对应物理 `PAGE_BYTES` 被提交过滤器替代 |
| `storage.fsp.segment` segment management | `SegmentInodeRepository`, `SegmentPurpose`, `SegmentSpaceService`, `SegmentPageAllocator` | Implemented | INODE slot、segment extent list、fragment 页和 segment 页分配；`SegmentPageAllocator` 在需要新 extent 时构造 `ExtentAllocationRequest`，leaf 顺序 hint 可批量挂 2-4 个 extent；0.19c 起 inode slot image/field/fragment slot 写入追加 metadata delta，0.19d 起对应物理 `PAGE_BYTES` 被提交过滤器替代 |
| `storage.fsp.lifecycle` lifecycle marker | `TablespaceLifecycleHeader`, `TablespaceLifecycleRawCodec` | Implemented | page0 198–237 持久化 GENERAL NORMAL/CORRUPTED/DISCARDED marker 与 UNDO ACTIVE/INACTIVE/TRUNCATING marker；GENERAL 稳定状态拒绝 truncation epoch/target，DDL DROP 用 DISCARDED 作文件删除前的 durable intent |
| `storage.fsp.undo` undo rebuild | `UndoTablespaceFspRebuilder` | Implemented | 物理 truncate 后清零并重建 page0/page2/extent0 |
| `storage.fsp.exception` exceptions | `FspMetadataException`, `NoFreeSpaceException`, `SpaceReservationExceededException` | Implemented | FSP 元数据损坏/空间耗尽/预留额度耗尽领域异常 |
| `storage.fil.io` physical IO | `PageStore`, `FileChannelPageStore`, `DataFileDescriptor`, `DataFileHandle`, `AutoExtendPolicy`, `DataFileGateway`, `ZeroFillDataFileGateway`, `PreallocationStrategy` | Implemented | State/registry-free；单文件 `truncate`（缩短）与 `ensureCapacity`（幂等扩到至少 N，crash recovery 用），均持 physical Lifecycle->FileSize(X)；create/extend/ensureCapacity 的新页范围初始化委托 `DataFileGateway`，默认 `ZeroFillDataFileGateway` 保持零填充行为，`PreallocationStrategy` seam 已存在但平台 native adapter 未接；`force/forceAll` 经 `FsyncLock` 串行化同一 data file 的 `FileChannel.force(true)` |
| `storage.fil.lock` physical locks | `TablespaceLifecycleLatch`, `FileSizeLock`, `ResourceGuard`, `FsyncLock` | Implemented | lifecycle/file-size/fsync 均由 `DataFileHandle` 使用；未接线的 `DataFileHandleLock`/`PageIoRangeLock` 已删除，物理层暂不建句柄替换锁或 page-range 合并锁 |
| `storage.fil.access` operation admission | `TablespaceAccessController`, `TablespaceAccessLease` | Implemented | controller 每 SpaceId 公平显式 RW lease；`StorageEngine` E1/E2 创建单实例并注入 MTR/loader/disk/flush/recovery undo truncate |
| `storage.fil.meta` runtime metadata | `TablespaceRegistry`, `CachingTablespaceRegistry`, `TablespaceMetadata`, `Tablespace`, `TablespaceHandle` | Implemented for runtime admission | registry 保存当前进程打开视图；UNDO lifecycle 与 GENERAL NORMAL/CORRUPTED/DISCARDED 由 page0 恢复；DROP 经 DD/physical DDL 进入 X lease、drain、DISCARDED、invalidate、close/delete，独立 DISCARD TABLESPACE 语句尚未实现 |
| `storage.fil.state` type/state values | `TablespaceState`, `TablespaceType`, `TablespaceTypeFlags`, `SpaceFlags` | Implemented | 表空间类型与状态编码值对象；状态转换由 api/engine 层编排 |
| `storage.fil.exception` exceptions | `TablespaceNotFoundException`, `TablespaceUnavailableException`, `DataFilePhysicalException`, `PageOutOfBoundsException` | Implemented | 表空间文件缺失、越界、损坏、不可用等 fil 领域异常 |
| `storage.page` physical envelope | `PageEnvelopeLayout`, `FilePageHeader`, `PageEnvelope`, `PageChecksum`, `PageImageChecksum`, `PageType` | Implemented | Shared header/trailer/checksum helpers over raw page bytes；`PageImageChecksum.verify` 同时校验 header checksum、trailer checksum 与 trailer low32 LSN；1.6 新增稳定 `UNDO_PAYLOAD=9`，既有 0..8 code 不变 |

## Buffer Pool + MiniTransaction Slice

### Current Flow

```mermaid
flowchart TD
  Caller["api.index IndexPageAccess / UndoPageAccess / FSP repos"] --> Mtr["MiniTransaction.getPage/newPage"]
  Mtr --> Access["TablespaceAccessController S lease (one per space)"]
  Mtr --> Pool["LruBufferPool facade (BufferPoolRouter)"]
  Pool --> Inst["BufferPoolInstance shard (pageHashLock + frameMutex + PageHashTable)"]
  Inst --> Store["PageStore readPage/writePage on miss/evict"]
  Inst --> Frame["BufferFrame (ReentrantReadWriteLock pageLatch)"]
  Inst --> Policy["MidpointLruReplacementPolicy (old/new sublists)"]
  Mtr --> Collector["MtrRedoCollector implements PageWriteListener"]
  Collector --> RedoEntries["MtrRedoEntry diagnostics (local category only)"]
  Collector --> RedoRecords["PageBytesRecord / PageInitRecord / logical metadata records"]
  Mgr["MiniTransactionManager"] --> Mtr
  Mgr --> Budget["RedoAppendBudget + operation estimator"]
  Budget --> Throttle["RedoCapacityThrottle logical admission + physical batch-fit"]
  Mgr --> Throttle
  Mgr --> RedoMgr["RedoLogManager (in-memory, no writer/flusher)"]
  Mtr -->|append at commit| RedoMgr
  Guard["PageGuard (RAII AutoCloseable)"] --> Frame
  Guard -->|close -> FrameReleaser| Inst
```

### Current Data Chains

| Flow | Current production chain | Current state |
| --- | --- | --- |
| Page fix (INDEX) | `api.index.IndexPageAccess.openIndexPage` -> optional production `MiniTransaction.acquireTablespaceLease(S)` + `TablespaceRegistry.require` -> `MiniTransaction.getPage` -> `LruBufferPool.getPage`（facade 经 `BufferPoolRouter` 路由到归属 `BufferPoolInstance`）-> `instance.getPage` -> `acquire` (`BufferPoolInstanceLatchSet`: `pageHashLock` 查/注册 `PageId -> frame`，目标 `frameMutex` 固定/检查 LOADING/dirty/state；miss 时通过 `freeListLock` 取空闲帧，或经 `lruListLock` 复制 victim 顺序后逐帧复核；命中 LOADING 出锁等 `PageLoadFuture`；装 LOADING 占位后**出所有内部锁** `readAndPublish` 读盘) -> `pageLatch.lock` -> `new PageGuard`(releaser=instance) -> `attachWriteListener(MtrRedoCollector)` + `memo.pushPageGuard`；facade 再调 read-ahead 钩子 | Implemented；生产组合根使用 registry-aware typed access，测试两参构造仍可只测页格式；**0.10d 多 instance（默认 N=1，生产 N=1）**：facade 路由单页操作到分片、跨切面查询逐分片聚合；miss 读盘移出内部锁（Phase B：per-frame LOADING + load future）；**13.1d** 已拆 `pageHashLock + frameMutex + freeListLock/lruListLock/flushListLock`，dirty view 由 `DirtyPageList` 真实 flush list 承载 |
| Page fix (UNDO) | `UndoPageAccess.openUndoPage` -> optional production lease+Registry require -> `MiniTransaction.getPage` (same path); page-type gate rejects non-UNDO pages | Implemented |
| New page (INDEX) | `api.index.IndexPageAccess.createIndexPage` -> optional production lease+Registry require -> `MiniTransaction.newPage` -> `LruBufferPool.newPage` -> `acquire(readFromDisk=false)` -> zero-fill under X latch; MTR `collector.recordInit(pageId, PageType.INDEX)` | Implemented |
| New page (UNDO) | `UndoPageAccess.newUndoEnvelope` -> optional production lease+Registry require -> `MiniTransaction.newPage(...,PageType.UNDO)` | Implemented |
| New page (UNDO external payload) | `UndoPayloadStorage.write` -> `UndoSpaceAllocator.allocatePage` -> `MiniTransaction.newPage(...,PageType.UNDO_PAYLOAD)` -> `UndoPayloadPage.format` | Implemented；与 root 同属 FSP undo segment，但不加入主 UNDO FIL chain；页内保存 owner/segment identity/index/count/length/CRC，segment drop 统一释放 |
| MTR begin/commit | 生产读路径 `MiniTransactionManager.beginReadOnly()`（零预算）；固定布局写走 `budgetFor(purpose)`，动态写走 `budgetFor(purpose, RedoBudgetWorkload)` -> `begin(RedoAppendBudget)` -> throttle 在页/FSP 资源前按 logical 上界准入并校验 physical LogBlock file-fit -> `MiniTransaction.commit` 冻结一次 persisted records -> 精确结算/上界校验 -> append 后 reservation ownership transfer -> disable collector -> stamp touched pages -> `memo.releaseAll()` LIFO（发布 dirty/pageLSN）-> `RedoLogManager.markClosed(range)` | Implemented；默认 no-op 测试 manager 保留匿名无界预算；只读零预算不触发 flush；B+Tree height、DML 首写、rollback undo 类型与 undo drop plan 均在 begin 前物化；动态 purpose 误走固定入口会在取页前拒绝；预算低估在 append 前抛 `RedoBudgetExceededException` 并保持 COMMITTING fail-stop |
| MTR rollback | `MiniTransaction.rollbackUncommitted` -> `memo.releaseAll()` only (`:167`) | Implemented; dirty pages stay dirty — no buffer-content undo (documented simplification `:18-19`) |
| Dirty page mark | `PageGuard.close` -> `FrameReleaser.release(frame, wrote)` -> `BufferPoolInstance.release`（归属分片，0.10d 起 `FrameReleaser` 由 instance 实现）OR-dirties via `markDirty` under target `frameMutex`; sets `oldestModificationLsn`/`newestModificationLsn`/bumps `dirtyVersion` and publishes/upserts `DirtyPageList` under `flushListLock` | Implemented；**13.1d**：同页重复修改只保留一条 flush list 记录，oldest LSN 约束 checkpoint，newest LSN 随页面最新 LSN 更新；**E1 修 bug**：`markDirty` 改用 `oldestModificationLsn==null` 守卫（原 `!dirty`）——newPage 对驻留页重初始化先置 dirty=true（无 LSN），双 newPage 同 MTR（allocatePage+createIndexPage 同根页）后 commit markDirty 会因 dirty 已真而漏设 oldestMod，留 dirty+null oldestMod 帧致 flush/checkpoint NPE。flush-after-双newPage 之前潜伏（既有 btree 测试不 flush 未触发） |
| Eviction | `BufferPoolInstance.obtainVictim(cleanSkip)`（分片本地，无跨分片 stealing）优先 free/clean 帧直接复用；仅有脏 unfixed 帧时记 PageId、出所有内部锁经 `DirtyVictimFlusher.flushVictim`（→`FlushCoordinator.singlePageFlush`：WAL gate+checksum+doublewrite+`completeFlush`）刷干净后回环重选；本轮 `cleanSkip` 防空转，无干净帧抛 `BufferPoolExhaustedException`；无 `DirtyVictimFlusher` 时遇到脏 victim 直接抛 `BufferPoolExhaustedException`，不再 fallback 直写 PageStore | Implemented；注入 flusher（生产 `StorageEngine`）后脏页淘汰 WAL 安全：redo 未 durable→`flushVictim` 返回 false→不写盘；`FAILED`→抛根因不吞。2026-07-05 已移除 legacy no-flusher direct-write fallback |
| Checkpoint feed | `CheckpointCoordinator` 算 safe LSN -> `CheckpointMetadataParticipant` 捕获 `TransactionSystem.snapshotCounters` 并 force `TransactionRecoveryCheckpointStore` -> force `RedoCheckpointStore` label -> 发布内存 checkpoint -> 锁外 `RedoReclaimBoundary` | Implemented；sidecar→redo label→reclaim 顺序保证 fuzzy checkpoint/redo ring 回收后仍有事务高水位；sidecar/label 失败都不发布或回收。`FLUSHING` 页仍留在 `DirtyPageList`，safe LSN 不能越过正在写出的页 |
| Tablespace invalidation | `UndoTablespaceTruncationService` 持 X lease -> `LruBufferPool.invalidateTablespace` -> `SpaceLifecycleClock.beginInvalidation` 关闭该 space 新前台准入/预读 -> 各分片 Condition 等待 fixCount=0 -> dirty 则拒绝并 abort 版本窗口 -> 全分片 drain+check 通过后 `advanceInvalidation` 推进 `TablespaceVersion` -> 移除旧版本 frame -> finish 重新开放 | Implemented；fixed 等待有 timeout/interrupt；不隐式绕过 WAL flush；并发前台 get/new 命中失效窗口抛 `BufferPoolStalePageException`，prefetch 直接跳过，LOADING 发布前会复核版本并清占位；dirty drain 等待独立走 facade 级 `awaitDirtyStateChange`，由 flush/guard release/reset 等 dirty-view 变化路径 signal |

### Package Status

| Package area | Representative classes | Current state | Notes |
| --- | --- | --- | --- |
| `storage.buf` pool core | `BufferPool`, `LruBufferPool`(facade), `BufferPoolInstance`, `BufferPoolInstanceLatchSet`, `PageHashTable`, `BufferPoolRouter`, `BufferFrame`, `PageGuard`, `PageLatchMode`, `DirtyVictimFlusher`, `DirtyPageList`, `BufferFrameState`, `FrameStateMachine`, `PageLoadFuture`, `BufferPoolLoadTimeoutException`, `BufferPoolLatchViolationException`, `SpaceLifecycleClock`, `TablespaceVersion`, `BufferPoolStalePageException` | Implemented (production-wired) | **0.10d 多 instance 分片**：`LruBufferPool` 转 facade，经 `BufferPoolRouter`(`hash(PageId)%N` 确定路由) 把单页操作转发到归属 `BufferPoolInstance`、跨切面查询（dirty 候选合并按 oldestLSN 升序 / oldest LSN 全局 min / hasDirty / residentCount / residentCountInRange / 截断）逐分片聚合；分片间无 work stealing（某分片满即抛 `BufferPoolExhaustedException`）；`invalidateTablespace` 两阶段（全分片 drain+check 通过后再移除）保 all-or-nothing。容量按 base+前 r 个+1 切分（capacity≥N）。生产 N=1（`EngineConfig.bufferPoolInstanceCount` 默认 1，`StorageEngine` 经此构造）。**Phase B + 13.1a/b/c/d + legacy flush removal**：`FrameStateMachine`(FREE/LOADING/CLEAN/DIRTY/FLUSHING) + LOADING single-flight；`PageHashTable` 由 `pageHashLock` 保护，`BufferFrame` 当前绑定/状态/fix/dirty/LSN/loadFuture/spaceVersion 由 `frameMutex` 保护；free list、LRU、flush list 分别由 `freeListLock`/`lruListLock`/`flushListLock` 保护；`DirtyPageList` 保存 `PageId + oldest/newest LSN` 而非 frame 引用，候选枚举走 flush-list 快照后再 pageHash/frame 复核，fixed DIRTY 页仍出候选供 drain 看到 dirty view，`FLUSHING` 页留链约束 checkpoint 但不重复出候选；Buffer Pool 不再提供 `flush/flushAll` 直写 PageStore API，所有脏页物理写出只走 `FlushCoordinator`；miss 读盘、`PageLoadFuture` wait、dirty victim flush 前均由 `BufferPoolInstanceLatchSet.assertMetadataUnlocked` 守卫无内部锁。**0.22 stale-frame 版本语义**：每个 resident/LOADING frame 带 `TablespaceVersion`，`SpaceLifecycleClock` 在 truncate/drop invalidation 窗口拒绝前台准入、跳过 prefetch，并在 lookup/LOADING 发布前复核版本；已过期 clean unfixed frame 只隔离不复活。剩余 `DIRTY_PENDING/EVICTING/STALE` 态仍待后续。**0.13d SX latch**：`PageLatchMode` 增 `SHARED_EXCLUSIVE`（SIX），分层实现=帧内 `pageLatch.readLock()`（与 S 共存、排它 X）+ 每帧 `pageIntentLatch`(`ReentrantLock`，排它另一 SX/X)；`BufferPoolInstance.acquire` 对 SX 先取 read latch 再取 intent latch，`PageGuard` 持双闩、close 逆序先放 intent 后放 read；SX 只授只读内容（写仍须 X，`requireExclusive` 拦截），不支持原地 SX→X 升级（RRWL 无升级会自死锁）。**已接入 btree 悲观 SMO 下降**（root SX 首遍 + restart-in-X，见 B+Tree 小节） |
| `storage.buf` replacement | `ReplacementPolicy`, `MidpointLruReplacementPolicy` | Implemented (production-wired) | Midpoint LRU(old/new 子链)：读入进 old 头、`oldBlocksTime`(注入毫秒时钟) 提升窗 + `youngDistanceThreshold`(young 子链 1/4) 抗抖动 → 抗扫描污染（Phase A 0.8）；sole impl，injection ctor `LruBufferPool(...,ReplacementPolicy)` 供测试注入可控时钟；read-ahead-aware 分类、`oldBlocksPct` 配比再平衡待 0.10 |
| `storage.buf` flush support | `DirtyPageCandidate`, `FlushPageSnapshot`, `BufferPoolExhaustedException` | Implemented | Value objects consumed by flush module；`failFlush` 现 FLUSHING→DIRTY（Phase B，不再 no-op）；`snapshotForFlush` DIRTY→FLUSHING、`completeFlush` 版本符→CLEAN/不符→DIRTY；`FlushCoordinator` WAL-gate skip 路径补 `failFlush` 复位 |
| `storage.buf` write listener | `PageWriteListener` | Implemented | DI seam; only production impl is `MtrRedoCollector`; `NO_OP` path has no production caller |
| `storage.buf` read-ahead + warmup | `BufferPool.prefetch`/`residentPageIds`/`residentCountInRange`, `LinearReadAheadTracker`, `RandomReadAheadDetector`, `ReadAheadRequest`, `ReadAheadHook`, `ReadAheadService`, `ReadAheadState`, `BufferPoolWarmupService` | Implemented (production-wired) | 0.10a linear read-ahead：`prefetch`=free-frame-only 载入 old 不 fix 不提升（跳过驻留/无空闲帧丢弃/IO 失败回收）；`LinearReadAheadTracker`(单顺序流，同 extent 连续达 threshold→预取下一 extent，`PAGES_PER_EXTENT=64`)；`ReadAheadService`(实现 `ReadAheadHook`，前台 `recordAccess` 喂检测器+有界队列、单 worker `prefetch`)；`attachReadAheadHook`+`getPage` 回调；engine 后台启停（linear threshold 56、门控 `backgroundFlushEnabled`）。**0.10c random read-ahead（默认禁用）**：`BufferPool.residentCountInRange`(page hash 短锁内逐页查区间驻留数，O(extent)) + `RandomReadAheadDetector`(同 extent 驻留数达 threshold→补取整 extent，bounded recent 窗去重而非永久 set)；`ReadAheadService` 4 参构造增 `randomThreshold`(0=禁用→不构造检测器/普通路径不查 residentCountInRange)，`recordAccess` 持 service.lock 时查 residentCountInRange(page hash 短锁，单向无环) 喂 detector、命中入队，random 检测异常吞掉只丢本次预取；`StorageEngine` 以 `RANDOM_READ_AHEAD_THRESHOLD=0` 构造（对齐 MySQL `innodb_random_read_ahead=OFF`），生产启用留 config（延后）。0.10b warmup：`BufferPoolWarmupService` dump(residentPageIds→文件 magic+crc32)/load(读回→prefetch，缺失/损坏 no-op)，`StorageEngine` close 写 / open 预取。简化：单流、free-frame-only、random 触发用「extent 驻留数」启发式(非 access-bit)、warmup 同步预取/无 IO 速率控制/space version；多 instance 分片 + 专用 `PageHashTable` 已由 0.10d 闭合 |
| `storage.mtr` transaction | `MiniTransaction`, `MiniTransactionManager`, `MtrOperationRedoBudgetEstimator`, `MiniTransactionState`, `MtrSavepoint`, `MtrRedoCategoryScope` | Implemented (production-wired by engine) | Manager 注入共享 controller + durable redo + throttle + 实例页大小；固定 profile 与领域 `RedoBudgetWorkload` 分入口；commit 精确验证并返回 end LSN；默认测试构造仍内存 redo/no-op reservation |
| `storage.mtr` memo + collector | `MtrMemo`, `MtrRedoCollector`, `MtrRedoEntry`, `MtrRedoCategory`, `MtrLatchOrderScope`, `MtrStateException` | Implemented | memo 同时持 page guard 与 per-space lease；LIFO 保证 latch/fix 先释放、lease 最后释放。**0.13d SX**：`fix` 的同页升级防护由「S→X 禁」扩为「S 或 SHARED_EXCLUSIVE 仍持时求 X 且未持 X 即禁」（两者都持该页 readLock，再求 X 会自死锁）。**0.23a MTR page latch ordering**：默认独立多页 latch 必须按 `(spaceId,pageNo)` 升序获取；同页重入和已提前释放页不计入违规；违反时在进入 Buffer Pool 等待前抛 `MtrStateException`。`allowOutOfOrderPageLatch(reason)` 只给 B+Tree root/child/sibling/SMO allocation-format-free、Undo grow/FIL 链读/rseg page3 slot 等有局部无环证明的路径短暂开例外，作用域关闭后恢复默认守卫。**0.23b MTR/Redo 纪律**：savepoint 不允许释放 touched page；commit 固定 append -> stamp pageLSN -> release/dirty publish -> markClosed；collector 维护本地分类诊断（默认 `PAGE_BYTES_GENERIC`，`PAGE_INIT` 由 newPage 固定产生）。**0.19d/0.19f/0.19g/0.19h logical redo 去重**：提交给 redo manager 的视图会删除被 FSP metadata、undo metadata、完整 undo payload、B+Tree sibling delta after-image 精确覆盖的物理 `PAGE_BYTES`，但 touched page 仍由真实页写维护；`TRX_STATE` 分类承载 non-page 正式事务终态/高水位 logical redo，恢复时仍与 page3 物理证据交叉校验 |

## Record Layer Slice

### Current Flow

```mermaid
flowchart TD
  ApiIndex["api.index IndexPageAccess"] -->|existing-page open once| Validator["RecordPageStructureValidator"]
  Validator --> RPage["record.page RecordPage"]
  ApiIndex -->|create + format| RPage
  Handle["api.index IndexPageHandle"] -->|validated handle view| RPage
  BTreeLeaf["LeafOnlyBTreeIndexService"] --> RPage
  BTreeSplit["SplitCapableBTreeIndexService"] --> RPage
  BTreeLeaf --> KeyOrderValidator["RecordPageKeyOrderValidator"]
  BTreeSplit --> KeyOrderValidator
  KeyOrderValidator --> RPage
  KeyOrderValidator --> RComparator["RecordComparator"]
  BTreeLeaf --> RSearch["RecordPageSearch"]
  BTreeLeaf --> RComparator
  BTreeLeaf --> RInserter["RecordPageInserter"]
  BTreeSplit --> RSearch
  BTreeSplit --> RInserter
  BTreeSplit --> RComparator
  BTreeSplit --> REncoder["RecordEncoder"]
  BTreeSplit --> KeyCmp["SearchKeyComparator"]
  BTreeSplit --> PtrCodec["BTreeNodePointerCodec"]
  UndoCodec["UndoRecordCodec"] --> Registry["TypeCodecRegistry"]
  RSearch --> Registry
  RComparator --> Registry
  RInserter --> Registry
  REncoder --> Registry
  Registry --> Codecs["Integer/Floating/Decimal/Bytes/Temporal/Bit/Enum/Set/Lob codecs"]
  Registry --> Characters["CharacterTypeRegistry"]
  Codecs --> Characters
  Characters --> Collations["Binary / ASCII-CI / UnicodeWeightV1"]
  Engine["StorageEngine"] --> LobStorage["api.lob LobStorage"]
  LobStorage --> Disk["DiskSpaceManager reserve(BLOB) / allocate(BLOB) / free"]
  LobStorage --> LobPages["PageType.BLOB chain + CRC32"]
  LobStorage --> Registry
  RPage --> RHeader["RecordHeader / IndexPageHeader"]
  REncoder --> Hidden["HiddenColumns (DB_TRX_ID + DB_ROLL_PTR; T1.3c 起可为真 insert undo 指针)"]
```

### Current Data Chains

| Flow | Current production chain | Current state |
| --- | --- | --- |
| INDEX page record access | 新页：`IndexPageAccess.createIndexPage` -> `mtr.newPage(INDEX)` -> `RecordPage.format`；既有页：`openIndexPage/openIndexPageHandle` -> `mtr.getPage(S/X)` -> `RecordPageStructureValidator.validate` -> `IndexPageHandle.recordPage` | Implemented；既有页每次 fix 后、返回 handle 前校验一次 header/system records/live chain/heap identity+range/PageDirectory+nOwned，并校验 FREE fragment chain 与 `linkedFreeBytes<=GARBAGE<=physicalDeadUpper`；损坏早于 B+Tree 字段解析、underflow/merge-fit 和写页 fail-closed；高扇出 `recordPage()` 不重复校验 |
| INDEX page schema-aware key order | LeafOnly `openRootLeaf` / SplitCapable `openBTreePageOutOfOrder(index)` -> indexId 核对 -> 按实际 page level 选择 leaf schema/keyDef/CONVENTIONAL 或 `BTreeNodePointerSchema`/NODE_POINTER -> `RecordPageKeyOrderValidator.validate` -> `RecordComparator.compare(record,record)` | Implemented (0.21d)；既有页在 search/scan/SMO 写入前逐链校验 record type、完整 CHAR/VARCHAR 严格 charset 解码与相邻 key 非降序；NULL/DESC/byte-prefix/collation 复用 0.21c；重复/等价 key 与 delete-mark 合法。物理 validator 继续 schema-free，fresh create/format 空页不重复扫描 |
| In-page search | `LeafOnlyBTreeIndexService`/`SplitCapableBTreeIndexService` -> `new RecordPageSearch(registry)` (`:49`/`:72`) -> `search.findEqual/findInsertPosition` -> `RecordCursor` per row | Implemented |
| Index key comparison | leaf `RecordComparator` / node-pointer `SearchKeyComparator` -> shared `EncodedKeyPartComparator` -> NULL order -> `TypeCodec.compareKeyPart`；CHAR/VARCHAR/TEXT -> `CharacterTypeRegistry.collationFor(charset,collation)` | Implemented；0.21c 起两条链共享 nullable/prefix/ASC-DESC；0.21g 追加 stable-id Unicode weight v1 与 UTF-8 code-point-safe prefix；0.21h TEXT/BLOB 只允许显式 prefix 且 external 值只比较 32B inline prefix，JSON/full LOB fail-closed |
| Inline temporal scalar codec | `RecordEncoder` / `RecordFieldResolver` / `UndoRecordCodec` / leaf+node key comparator -> shared `TypeCodecRegistry` -> `TemporalCodec` | Implemented (0.21e1)；DATE=4B signed epochDay、TIME=8B signed duration millis、DATETIME=8B signed epoch millis、TIMESTAMP=8B signed UTC epoch millis、YEAR=2B unsigned；均可直接按 unsigned encoded bytes 保序比较 |
| BIT / ENUM / SET codec | Record/Undo/B+Tree shared `TypeCodecRegistry` -> `BitCodec` / `EnumCodec` / `SetCodec` | Implemented (0.21e2/0.21f)；BIT(1..64) 左对齐 canonical 尾位、ENUM 1-based ordinal、SET 最多 64 member bitmap；均有 schema 边界与保序比较，不能误用 prefix |
| Off-page LOB plan/write/read/free | `LobStorage.planWrite` 无 IO 冻结 payload/pageCount/CRC/prefix/workload -> `writePlanned` reserve/allocate/format；`planFreeBatch/executeFreeBatch` 先整批校验 chain/segment/重叠再释放；read 逐页 S latch/release + chain/CRC | Implemented (0.21h + DML ownership lifecycle)；INSERT 自动 externalize；UPDATE replacement 的新链由 rollback owner、旧链由 committed purge owner；DELETE 把旧 external 链交给 purge。未赋值 external 列由 `TableUpdatePatchCommand` 保留原引用，不经 SQL hydrate/rewrite |
| In-page insert | btree service -> `new RecordPageInserter(registry)` (`:51`/`:73`) -> `inserter.insert` -> `HeapSpaceManager` alloc + `RecordPageDirectory` slot maintenance; `RecordPageOverflowException` triggers btree split | Implemented |
| Clustered record encode | `SplitCapableBTreeIndexService.insertClustered` -> stamps `new HiddenColumns(transactionId, rollPointer)`（T1.3c 起为调用方传入的真 insert undo 指针，非 NULL） (`:105`) -> `RecordEncoder.encode` (`:426`) | Implemented; `DB_ROLL_PTR` 可写真 INSERT undo 指针（由 `UndoLogManager.planInsert/appendPlanned` 返回）；未接 undo 的路径仍传 `RollPointer.NULL` |
| Undo record codec | `UndoRecordCodec` -> shared type codecs；INSERT 可追加 `LO/v1` ownership；UPDATE/DELETE 可追加 `LV/v1` old/new version ownership；随后可组合 `SI/v1` secondary tail | Implemented；旧 EOF、旧 LO/SI 组合继续兼容；LV 对 ordinal/flag/type/external envelope 严格校验。UPDATE rollback 只释放新链，committed purge 只释放旧链；DELETE 只声明 purge-old |
| Record decode | `RecordFieldResolver` -> `TypeCodecRegistry` -> per-column `FieldSlice`/`ColumnValue`; reached via `RecordCursor` (btree scan/lookup) | Implemented; standalone `RecordDecoder` has no production caller (test-only) |

### Package Status

| Package area | Representative classes | Current state | Notes |
| --- | --- | --- | --- |
| `record.schema` | `TableSchema`, `ColumnType`, `IndexKeyDef`, `ColumnDef`, `KeyPartDef`, `KeyOrder`, `TypeId` | Implemented | 28 `TypeId`；0.21e1/e2/f/h 只在尾部追加 TIME/TIMESTAMP/YEAR/BIT/ENUM/SET/TEXT/BLOB/JSON family，既有枚举顺序不变且磁盘不存 ordinal；`StorageKind.OVERFLOW_CAPABLE` 明确 inline/external 边界 |
| `record.type` | `TypeCodecRegistry`, `CharacterTypeRegistry`, `EncodedKeyPartComparator`, `TypeCodec`, `ColumnValue`, codecs/collations | Implemented (0.21 type subset) | 内联标量、BIT/ENUM/SET、TEXT/BLOB/JSON envelope 均生产可解析；Unicode V1 是项目版本化教学 weight，不是 MySQL UCA/locale tailoring；TIME/TIMESTAMP SQL 范围/时区和 MySQL binary JSON 属上层/后续语义 |
| `record.format` | `RecordEncoder`, `RecordFieldResolver`, `LogicalRecord`, `RecordHeader`, `RecordType`, `HiddenColumns`, `HiddenColumnLayout`, `NullBitmap`, `VarLenDirectory` | Partial | `VARIABLE`/`OVERFLOW_CAPABLE` 共用变长目录；单条 record 仍受 `MAX_RECORD_LENGTH=65535`，但 LOB 完整 payload 可通过 external reference 落页链；`RecordDecoder` test-only；header 为教学 8B、非 InnoDB binary-compatible |
| `record.page` | `RecordPage`, validators/search/insert/update/delete/purge/reorganize, `RecordComparator`, `IndexPageHeader` | Partial | 0.21a-d validator/排序与 0.21e-h 类型均经共享 codec 进入 leaf/node pointer；页内算子完整，但仍缺 `PAGE_MAX_TRX_ID`/`PAGE_BTR_SEG_*`，split/merge 决策继续归 btree |
| `api.lob` | `LobStorage`, `LobWritePlan`, `LobWriteAllocation`, `LobFreeBatchPlan`, `LobFreeTarget`, page/layout/exceptions | Implemented; DML/rollback/purge production-wired | `StorageEngine` 生产持有；写入、读取与批量释放复用 BLOB reservation、authoritative table LOB segment、MTR/WAL/recovery。批量释放在首次 FSP 修改前验证整批 chain 与页集合，防止重复 ownership 或部分可预见失败 |

## B+Tree Layer Slice

### Current Flow

```mermaid
flowchart TD
  Facade["BTreeIndexService (interface)"]
  Facade --> Leaf["LeafOnlyBTreeIndexService (B1/B2 rootLevel=0)"]
  Facade --> Split["SplitCapableBTreeIndexService (任意高度)"]
  Leaf --> PageAccess["api.index IndexPageAccess"]
  Split --> PageAccess
  Leaf --> KeyOrderValidator["record.page RecordPageKeyOrderValidator (leaf/internal)"]
  Split --> KeyOrderValidator
  Split --> Disk["DiskSpaceManager.reserveSpace + allocatePage(PageAllocationHint) (leaf split)"]
  Split --> RecordEncoder["RecordEncoder (clustered)"]
  Split --> PtrCodec["BTreeNodePointerCodec"]
  Split --> KeyCmp["SearchKeyComparator"]
  Leaf --> RSearch["RecordPageSearch"]
  Split --> RSearch
  Leaf --> RInserter["RecordPageInserter"]
  Split --> RInserter
  Split --> Descend["findLeaf / descendPath (N 层导航)"]
  Split --> SplitEngine["splitLeafAndPropagate / insertSeparator / growRootWithInternal (自底向上 split + 原地 root split)"]
  Engine["StorageEngine"] --> CurrentRead["BTreeCurrentReadService (2.7a point/unique + 2.7b range)"]
  Engine --> DML["storage.api.dml TableDmlService"]
  Engine --> ClusteredDML["ClusteredDmlService anchor/terminal"]
  DML --> ClusteredDML
  DML --> CurrentRead
  DML --> Split
  Engine --> SecondaryMvcc["SecondaryMvccReader"]
  Engine --> SecondaryCurrent["SecondaryCurrentReadService"]
  SecondaryMvcc --> Split
  SecondaryCurrent --> Split
  SecondaryCurrent --> CurrentRead
  SecondaryCurrent --> LockMgr
  CurrentRead --> Split
  CurrentRead --> LockMgr["storage.trx.lock LockManager"]
  Tests["tests only"] --> Facade
```

### Current Data Chains

| Flow | Current production chain | Current state |
| --- | --- | --- |
| Existing-page semantic open | `IndexPageAccess` 物理校验 -> LeafOnly root header / SplitCapable unified helper indexId 核对 -> 按实际 page level 选择 leaf 或 derived node-pointer schema -> `RecordPageKeyOrderValidator` | Implemented (0.21d)；12 个 SplitCapable existing-page 打开点与 LeafOnly 单一 root 入口均在业务读取/修改前 fail-closed；3 个 fresh create/format 空页直开点保持不重复扫描；校验期只读且仍持当前 S/X latch |
| Point lookup | `BTreeIndexService.lookup` -> SplitCapable `findLeafSharedCrab`（N 层 `chooseChild` **S-crab** 下降：持父 S→latch 子 S→放父 S，祖先早释放，0.13c）-> `search.findEqual` -> `RecordCursor` -> `materialize` | Implemented；SplitCapable 任意高度（0.11）；读路径 S-crab（0.13c）；LeafOnly 仍 level 0 |
| Point current-read (2.7a) | `BTreeCurrentReadService.lockPoint` -> 短 MTR 定位 record/gap -> commit 释放 page latch/fix -> `LockManager.acquire` -> 短 MTR 重定位校验；RC miss 不锁 gap，RR miss 按模式锁 gap | Implemented；SQL INSERT 消费 unique-check/insert-intention；SQL point UPDATE/DELETE 经 `TableDmlService` 消费 FOR_UPDATE record lock；普通 point SELECT 仍走 MVCC，point `FOR SHARE/UPDATE` 尚未接 |
| Unique insert current-read check (2.7a) | `BTreeCurrentReadService.checkUniqueForInsert` -> 物理 duplicate 命中取 `REC_S` 并重定位确认；miss 取 `INSERT_INTENTION` 到目标 gap 并重定位确认 -> `BTreeUniqueCheckResult` | Implemented；2.1 起 `ClusteredDmlService.insert` 调用；仍是物理唯一检查（delete-marked 同 key 算 duplicate），不做 MVCC 逻辑唯一 |
| Secondary logical key DML check | `TableDmlService.insert/update/delete` -> `SecondaryUniqueCheckService.lockLogicalKey/check` 以 collation/prefix 规范化 logical key -> `SecondaryLogicalKeyLockKey` X（事务终态释放）；unique 再 `scanSecondaryPrefixIncludingDeleted`，non-unique 检查完整 physical identity；key-changing UPDATE 同时锁 old/new prefix，DELETE 锁 old prefix，任一 NULL part 跳过 logical lock | Implemented；unique 检查、防 non-unique range phantom 与 DML 发布/标删共用同一稳定 logical-prefix 资源；完整 physical key 仍由 B+Tree `physicalUnique` 保护 |
| Non-unique secondary logical-prefix read | consistent：`SecondaryMvccReader.readRange` -> including-deleted prefix candidate 短 MTR -> 聚簇 MVCC/undo -> 可见完整行重算 key/按 clustered identity 去重；locking：`SecondaryCurrentReadService.readRange` -> logical-prefix S/X -> candidate 重扫 -> 聚簇 `lockPoint` S/X -> 当前完整行重算 key | Implemented；SQL 完整单列、无 prefix、non-unique secondary equality 返回多行；ReadView 覆盖 LOB hydration，locking 等待不持 page latch，锁到事务终态；4096+1 candidate fail-closed，不静默截断 |
| Range current-read (2.7b) | `BTreeCurrentReadService.lockRange` -> 短 MTR `SplitCapableBTreeIndexService.locateRangeForCurrentRead`（扫描 range records，构造 `RecordLockKey`/`NextKeyLockKey` 与 terminal `GapLockKey`）-> commit 释放 page latch/fix -> RC 对返回记录取 `REC_S/REC_X`，RR 取 `NEXT_KEY_S/X` + `GAP_S/X` -> 短 MTR 重扫 range 并校验；失败尝试释放已授予锁 | Implemented；批量 range 结果，不实现长期 cursor；terminal gap 仍是页级简化；SQL/session/executor range DML 尚未接 |
| Bounded scan | `SplitCapableBTreeIndexService.scan` -> `descendSharedCrab(lowerKey)` **S-crab** 定位起始 leaf -> sibling loop via `fileHeader().nextPageNo()`（`FIL_NULL` 终止，**hand-over-hand**：先 latch 后继 leaf 再 `releaseHandle` 前驱）-> `scanLeafPage` per page | Implemented；任意高度；读路径 S-crab + sibling hand-over-hand（0.13c），任一时刻只持「父+子」/「≤2 leaf」|
| Insert (no split) | SplitCapable `insert` -> **乐观** `tryOptimisticInsert`：`descendOptimistic`（内部层 S-crab、leaf X）-> unique check -> `inserter.insert`（放得下即成，仅 leaf 持 X）；溢出=unsafe 释放 leaf X 回退 `pessimisticInsert`。**悲观 insert 走 safe-node 下降（0.13d）**：`descendPathInsertSafeNode` 全 X 下降但每 latch 到内部 child 若 safe（`freeSpace ≥ maxSeparatorSize`=该索引 node pointer 编码严格上界）即释放其以上全部祖先 X（含 root）→ split 不传播到 root 时 **root X 不再持到 commit**；只判内部页（leaf 恒保留），既有 split 传播引擎在截断后的保留链上零改动正确。LeafOnly 仍 `descendPath` X-latch root→leaf | Implemented；overflow → `BTreeSplitRequiredException`（LeafOnly）或悲观 split 传播（SplitCapable，写路径 latch coupling 0.13a + safe-node 0.13d）；诊断计数 `safeNodeAncestorReleaseCount()`。insert/delete/purge 的 safe-node 下降共用 `descendPathSafeNode`（谓词参数化）。**0.13d SX+restart（§10.3 ROOT_LATCHED_SX）**：快照树高 ≥2 的悲观 SMO 首遍 root 取 **SX**（与读者/乐观写者 root S 并存、排它其它 SMO），safe 节点吸收 → 全程不 X root；首遍链顶仍是 root（SMO 可能写 root，SX 禁升级）→ 零写整链释放、root X 重启第二遍（至多一次，重启即全新导航天然正确）；level 0/1 树必写 root 故跳过 SX 首遍直取 X；计数 `rootSxDescentCount()`/`rootXRestartCount()` |
| Insert split 传播（0.11/0.14b/0.15/0.23a） | `insert` overflow -> `pessimisticInsert` 计算 split 预算 -> 释放未写保留链 -> `reserveSplitSpace(NORMAL)`（按 leaf split + 可能 parent/root split 最坏页数预算，且至少一 extent，失败在任何页改写前）-> 重新下降并复核 unique/leaf 容量 -> `splitLeafAndPropagate`。leaf 即 root → `splitRootLeaf`（原地 level0→1，若插入 key 高于旧 root 最大 key 且无右 sibling，则用 `PageAllocationHint.up` 分配两个新 leaf；低于最小 key 且无左 sibling 则 `down`）；否则 `splitNonRootLeaf`（旧 leaf=左半 + 新右兄弟 + sibling 链，边界顺序插入且对应方向无 sibling 时传 leaf hint）→ `insertSeparator` 上插父页 -> 父满则内部 split：root→`growRootWithInternal`（两 level-L 新子页 + root 页号不变重建 level L+1）、非 root→`splitNonRootInternal` 递归上插。leaf 行/内部 pointer 统一对半切，separator=右半 lowKey | Implemented（任意高度）；内部/root-split 子页自 `nonLeafSegment` 分配且继续无方向 hint；root 页号稳定；返回 `BTreeInsertResult(after.withRootLevel, allocatedPages)`；0.14b 起 split/root split 不再半途 immediate allocation ENOSPC；0.15 起 leaf split 可给 DiskSpaceManager 传保守方向 hint，随机中间 split 和已有 sibling 的 split 仍 none；0.23a 起预留不在持 index page latch 时触碰 page0/FLST，SMO 新页分配/格式化和 child/sibling hand-over-hand 打开通过 `allowOutOfOrderPageLatch(reason)` 记录局部无环证明 |
| Clustered insert | `SplitCapableBTreeIndexService.insertClustered(mtr, index, record, transactionId, rollPointer)` (`:91`) -> stamps `new HiddenColumns(transactionId, rollPointer)`（调用方传入真 INSERT undo 指针，替换恒 NULL） -> delegates `insert` (`:106`) | Implemented; `DB_ROLL_PTR` 由上层 orchestration（`assignWriteId → planInsert → appendPlanned → insertClustered`）传入；B+Tree 不 import trx/undo |
| Clustered delete (T1.3d；0.12 merge；0.13a latch coupling) | `SplitCapableBTreeIndexService.deleteClustered(...)` -> **乐观** `tryOptimisticDelete`：`descendOptimistic`（内部层 S-crab、leaf X）-> `findEqual`（未命中/所有权不符=幂等 no-op）-> `deleteWouldUnderflow` 预判（同 `isUnderfull` 公式、freed 取上界偏保守）：不欠载则 `deleteMark`+`purge` **跳过 `reclaimAfterRemoval`**（仅 leaf 持 X）；欠载=unsafe **写页前**释放 leaf X 回退悲观 **`descendPathDeleteSafeNode`（0.13d safe-node：X 下降遇「摘一最大指针后仍不欠载」的 safe 内部节点即释放其以上祖先 X 含 root，保留链=「safe 节点…leaf」）** -> `deleteInLeaf`（`findEqual`->所有权校验->`deleteMark`/`purge`->`reclaimAfterRemoval` 带 merge，只在保留链内传播）-> `BTreeDeleteResult(removed, indexAfter, freedPages)` | Implemented (StorageEngine service root + `RollbackService` + tests); 幂等（未命中/不匹配=no-op）；不 import trx/undo；**0.12 起删成功触发 merge + 原地 root shrink + free page（仅悲观路径）**；**0.13a 乐观不欠载删除仅 leaf 持 X、跳过 merge**；**0.13d merge 不传播到 root 时 root X 不再持到 commit**，诊断计数 `safeNodeDeleteAncestorReleaseCount()` |
| Clustered purge (T1.3d；0.12 merge；0.13b latch coupling) | `SplitCapableBTreeIndexService.purgeDeleteMarkedClustered(...)` -> **乐观** `tryOptimisticPurge`：`descendOptimistic`（内部 S、leaf X）-> `findEqual` 严格校验（命中 + 仍 delete-marked + 隐藏列匹配，任一不符=stale no-op）-> `deleteWouldUnderflow` 预判：不欠载则 `purger.purge` **跳过 `reclaimAfterRemoval`**（仅 leaf 持 X）；欠载=unsafe 写页前释放 leaf X 回退悲观 `descendPathDeleteSafeNode`（0.13d safe-node，与 delete 同）+`purgeInLeaf`（带 merge）-> `BTreeDeleteResult(removed, indexAfter, freedPages)` | Implemented (StorageEngine service root + `PurgeCoordinator` + tests)；stale=no-op；与 delete 共用 0.12 欠载回收 + 0.13b 乐观预判 + 0.13d safe-node 下降/计数 |
| Clustered replace (T1.3e；0.13b latch coupling) | `SplitCapableBTreeIndexService.replaceClustered(...)` -> **乐观** `tryOptimisticReplace`：`descendOptimistic`（内部 S、leaf X）-> `replaceInLeaf`：`findEqual` -> 所有权校验 -> `updater.update` 整记录替换；root 即 leaf 交悲观 `findLeaf(X)`。**恒 safe**（原地/页内搬迁，永不 split/merge）；REQUIRES_REINSERT(改 PK)→`BTreeUnsupportedStructureException`、搬迁溢出→`RecordPageOverflowException`（leaf 未改，与路径无关直接上抛）-> `BTreeUpdateResult(replaced)` | Implemented; 前向 UPDATE 与 rollback 恢复共用；幂等；不 import trx/undo；**0.13b 乐观 leaf-only，无 unsafe 回退** |
| Clustered delete-mark (T1.3f；0.13b latch coupling) | `SplitCapableBTreeIndexService.setClusteredDeleteMark(...)` -> **乐观** `tryOptimisticMark`：`descendOptimistic`（内部 S、leaf X）-> `markInLeaf` plan-then-execute：`findEqual`(含已标记)→所有权校验→翻转合法校验→`setDeleted`+`writeHiddenColumns`(等长两步纯写)；root 即 leaf 交悲观。**恒 safe**（等长纯写、无 size 变化/无结构变更）-> `BTreeDeleteMarkResult(changed)`；`lookupIncludingDeleted` 不过滤 delete-marked | Implemented; 前向删除与 rollback 取消标记共用；幂等、非法翻转抛；不 import trx/undo；**0.13b 乐观 leaf-only，无 unsafe 回退** |
| Secondary entry operations | `SplitCapableBTreeIndexService.insertSecondary` (`:222`) / `deletePublishedSecondary` (`:564`) / `purgeDeleteMarkedSecondary` (`:580`) / `setSecondaryDeleteMark` (`:927`) / `scanSecondaryPrefixIncludingDeleted` (`:1142`) -> 完整 physical key（logical parts + 完整聚簇主键后缀）定位；物理删除复用同一 underflow merge/redistribute/root-shrink | Implemented；DML、rollback、purge、unique check 与 secondary MVCC 均生产调用；`ABSENT` 是 crash 重试完成证据，live/marked 状态冲突显式返回 |
| Underflow reclaim (0.12 merge+shrink / 0.12b redistribute，delete+purge 共用) | `deleteInLeaf`/`purgeInLeaf` 物理删除成功 -> `reclaimAfterRemoval` -> `considerMerge(path, depth)`：`isUnderfull`(可回收空闲 `freeSpace+garbage` > 页半) -> `chooseMergePair`（parent pointer 顺序，survivor=左/victim=右）-> `mergeFits`(reclaimable)？**fit** → `reorganize` survivor 压实 + 并入 victim（leaf 修 FIL 链）-> `removePointerFromParent`(deleteMark+purge) -> `freeSmoPage(victim)` -> 传播 `considerMerge(depth-1)` / parent 是 root 剩 1 pointer 则 `shrinkRoot`（吸收唯一 child、树高-1、级联）；**fit 不下** → `redistribute`：合并相邻对对半重分到两页（`splitRows`/`splitPointers`）+ 只更新 parent 中 right 成员 lowKey（删旧插新）| Implemented (StorageEngine service root + tests)；min-key-pointer 约定下 survivor/left 父 pointer key 不变（merge 无 separator 更新、redistribute 仅改 right lowKey）；**redistribute 不删页/不传播/不改树高**（leaf+internal 统一，0.12b）；root 页号稳定；额外 sibling/远兄弟/child latch 入 MTR memo；**0.13d 起 path 可为 safe-node 截断保留链**：`considerMerge` 的 root 判定改按 `parentHandle.pageId()==rootPageId`（页号跨 split/shrink 稳定）而非链下标——防止对非 root 的 safe 链顶误做 `shrinkRoot`；safe 链顶保证摘一指针后不欠载 → merge 传播必停在链顶；0.23a 起回收页触碰 FSP 元页经 `freeSmoPage` 使用带理由的 MTR ordering 例外（FSP 不反向等待 index latch）|

### Package Status

| Package area | Representative classes | Current state | Notes |
| --- | --- | --- | --- |
| `storage.btree` facade | `BTreeIndexService` (interface), `BTreeIndex`, table/secondary metadata, clustered/secondary result types | Implemented (split-capable production-wired) | `StorageEngine` 暴露 concrete `SplitCapableBTreeIndexService`；TableDml/rollback/purge/MVCC 生产调用 clustered 与 secondary 原语。旧 `BTreeIndexService` interface 与 leaf-only 实现仍主要是教学/回归抽象 |
| `storage.btree` current-read | `BTreeCurrentReadService` point/physical-unique/range + `SecondaryUniqueCheckService` logical DML key + `SecondaryCurrentReadService` logical-prefix locking read | Implemented; production-held by `StorageEngine` | 聚簇 current-read 仍按短 MTR 定位→事务锁→重定位；secondary logical key 以规范化 S/X 资源协调 locking SELECT 与 INSERT/key-change/DELETE。全部事务锁由统一 terminal `releaseAll` 释放；任意比较范围、point locking SELECT 与 global precise gap 仍未接 |
| `storage.btree` leaf-only | `LeafOnlyBTreeIndexService` | Implemented (test-wired) | B1/B2 rootLevel=0 only; point lookup + in-page scan + insert-no-split; retained for regression/teaching tests while production root uses split-capable service |
| `storage.btree` split-capable | `SplitCapableBTreeIndexService`, `BTreeRootSnapshotService`, node pointer/redo snapshot types | Implemented (StorageEngine + table DML/rollback/purge/MVCC) | 任意高度 clustered/secondary split、merge/root shrink、redistribute、latch coupling、safe-node/SX restart 与 MTR ordering 均生产接线；结构写前从稳定 root 页头刷新 level，redo 只 patch 页面不重跑 SMO。仍缺 B-link/OLC 与 `PAGE_MAX_TRX_ID`/segment 辅助页头 |
| `storage.btree` exceptions | `BTreeException` + 6 subclasses | Implemented | `BTreeCurrentReadRelocationException`（授锁后多次重定位失败）, `BTreeDuplicateKeyException` (physical unique check), `BTreeSplitRequiredException`, `BTreeStructureCorruptedException`, `BTreeUnsupportedStructureException`；`BTreeRootChangedException` 自 0.12 起**不再由 `openRoot` 的 level guard 抛出**（导航按实际 root level；reserved 供 0.13/2.7 并发重定位协议）|

## Redo Log Layer Slice

### Current Flow

```mermaid
flowchart TD
  MtrBegin["MiniTransactionManager.beginReadOnly / begin(RedoAppendBudget)"] -->|logical reservation + physical fit before latch/lease| Throttle["RedoCapacityThrottle"]
  MtrCommit["MiniTransaction.commit"] -->|append records| Mgr["RedoLogManager"]
  MgrMgr["MiniTransactionManager"] -->|owns new RedoLogManager| Mgr
  Mgr -->|memory mode: no writer/flusher| Buffer["in-memory buffer + batches"]
  Mgr -->|durable() factory: StorageEngine + tests| Writer["RedoLogWriter"]
  Writer -.-> Repo["RedoLogFileRepository.append"]
  Mgr -->|flush(): StorageEngine checkpoint/close + recovery/truncate + tests| Flusher["RedoLogFlusher"]
  Flusher -.-> Repo
  Repo --> Block["RedoLogBlockCodec / RedoLogBlockScanner"]
  Block --> Frame["nested RLG1 RedoBatchFrameCodec"]
  Block -.-> File["single file / redo ring v2"]
  Collector["MtrRedoCollector"] -->|onWrite + local category| Entry["MtrRedoEntry"]
  Collector -->|onWrite| PBR["PageBytesRecord"]
  Collector -->|recordInit| PIR["PageInitRecord"]
  Collector -->|recordLogical| LogicalRecords["FSP / Undo / BTree / Trx logical records"]
  FlushCoord["FlushCoordinator"] -->|flushedToDiskLsn / waitFlushed WAL gate| Mgr
  Checkpoint["CheckpointCoordinator"] -->|currentLsn / flushedToDiskLsn| Mgr
  Recovery["CrashRecoveryService (StorageEngine E2 + tests)"] --> Reader["RedoRecoveryReader"]
  Reader --> Repo
  Recovery --> Dispatcher["RedoApplyDispatcher registry"]
  Dispatcher --> Handler["RedoApplyHandler sessions"]
  Handler --> FspHandler["FspPageAllocationRedoHandler"]
  Handler --> PageHandler["PageRedoApplyHandler"]
  Handler --> TrxHandler["TransactionStateRedoHandler"]
  FspHandler -->|ensureCapacity| PageStore["PageStore"]
  PageHandler -->|readPage / writePage| PageStore["PageStore"]
  TrxHandler -->|TransactionStateDeltaSink| TrxTable["RecoveredTransactionTable"]
  Checkpoint -->|write label| CkptStore["RedoCheckpointStore"]
  Recovery -->|read + validate format| CkptStore
```

### Current Data Chains

| Flow | Current production chain | Current state |
| --- | --- | --- |
| Redo collect (MTR) | PageGuard writes -> `PageBytesRecord`; explicit FSP/Undo/BTree/Trx logical records -> collector；commit view 精确过滤被最终 after-image 覆盖的物理 bytes | Implemented；B+Tree delta 已覆盖 sibling、internal header/used pointer heap/directory、root header/identity；未覆盖或与最终 image 不同的中间态写继续保留，leaf row bytes 仍为 physical redo |
| Redo append (MTR commit) | `beginReadOnly()` 或 `budgetFor(purpose)` + `begin(budget)` -> `RedoCapacityThrottle.reserveAppendBudget` -> reservation 挂入 memo -> commit 冻结 `collector.records()` 一次并 `budget.requireCovers` -> `redoLogManager.append(records)` 分配 `[start,end)` -> reservation `transferToAppend` -> disable/stamp -> `memo.releaseAll()` 发布 dirty -> `markClosed(range)` | Implemented；生产 manager 禁匿名 begin；只读零预算不参与压力判断；logical 预算参与 LSN capacity，physical 上界在 begin 拒绝超过 ring 单文件的 sealed batch；actual 低估在 append 前 fatal，append 后立即解除 reservation 与 real current LSN 的双计数 |
| Durable write | `RedoLogManager.write()` / `flush()` -> `ioLock` serializes repository append/force -> repository 以 `RedoLogBlockCodec` 把每个非空 MTR batch 的嵌套 `RLG1` frame 封成独立 512B block chain -> 单文件或 ring v2 -> 单调推进 written/flushed LSN | Implemented；header 32B + payload 472B + trailer 8B，blockNo 全局连续；batch 可跨 block 但不跨 ring 文件、不同 batch 不复用尾块；逻辑 LSN/pageLSN 不含物理 padding且保持不变；`DurabilityPolicy` 可选择 wait-written、wait-flushed 或后台策略 |
| WAL gate (flush module) | `FlushCoordinator.flushPage` -> `redo.flushedToDiskLsn()` (`FlushCoordinator.java:91`) + `redo.waitFlushed(pageLsn, timeout)` (`:92`) | Implemented；`StorageEngine` durable redo 路径可通过 WAL gate；memory-mode 组合中 durable LSN 恒 0，会跳过脏页 |
| Checkpoint read | `flush.checkpoint.CheckpointCoordinator.advanceCheckpoint` -> safe LSN -> `RedoCheckpointStore.write(RedoCheckpointLabel.of(..., redoFormatVersion=1))` -> 双 4KiB 隔离 slot v2（总长 8192B，generation + CRC） | Implemented；control v2 同时绑定 redo data format；旧 control v1 明确格式拒绝；READ_ONLY_VALIDATE 只读打开且不创建/预分配/force |
| Redo replay (recovery) | public `DatabaseEngine.open(existing)` 先从 DD binding 构造 recovery tablespace 列表 -> `StorageEngine.open(existing)` -> 只读/读写打开 redo data + control -> `CrashRecoveryService.recover` 校验并 replay -> UNDO resume / SPACE_FILE_RECONCILE / transaction rollback+purge resume -> force recovery redo/data pages -> 返回 public engine 做 DDL cleanup 后 open traffic | Implemented production path；低层 `StorageEngine` 仍支持显式 recovery list，公共组合根已用 DD 发现替代手工列表；`RedoRecoveryScan` 保留非零 checkpoint+torn tail 语义，CRC 正确但语义错误、非末损坏、LSN/blockNo gap 均 fail-closed；READ_ONLY_VALIDATE 对 redo data/control 无写副作用 |
| Capacity pressure | `StorageEngine.open` constructs throttle with policy/current/checkpoint/flush callbacks/timeout + ring `fileBytes` -> write `begin(RedoAppendBudget)` atomically aggregates logical outstanding budget before any MTR page latch/lease and validates physical batch-fit; ASYNC -> page cleaner request; SYNC/HARD -> `redo.flush()` + `FlushService.flushForCapacity(...)` until pressure drops or timeout | Implemented production path；read-only zero budget never triggers flush；operation profile replaces capacity/8；append ownership transfer prevents outstanding budget and current LSN double counting；timeout/physical oversize fail-closed；前台同步刷页预算独立于 background maxPages |
| Background redo flush | `StorageEngine.open` starts `RedoFlushWorker` (when `backgroundFlushEnabled`) -> periodic/on-demand `RedoFlushTarget.flush()` (-> `RedoLogManagerFlushTarget` -> `RedoLogManager.flush()`) -> 推进 `flushedToDiskLsn` + 唤醒 `waitFlushed` | Implemented production path；空转跳过（`currentLsn<=flushedToDiskLsn` 不 fsync）；失败即 FAILED；engine 在 page cleaner 前启动、close 时先停（停 page cleaner→停 redo flusher→final flushThrough）；解淘汰/flush WAL gate 因无人 flush 而跳过的根因 |

### Package Status

| Package area | Representative classes | Current state | Notes |
| --- | --- | --- | --- |
| `storage.redo` core | `RedoLogManager`, `ContiguousLsnTracker`, `RedoLogIo`, `DurabilityPolicy`, batches/ranges/records (`PAGE_INIT`/`PAGE_BYTES`/`FSP_PAGE_ALLOC`/`FSP_METADATA_DELTA`/`FSP_PAGE_FREE`/`UNDO_METADATA_DELTA`/`UNDO_RECORD_PAYLOAD`/`BTREE_PAGE_DELTA`/`TRX_STATE_DELTA`) | Partial | 默认 manager 为 memory mode；`StorageEngine`/truncation/DML facade 组合注入 durable manager；支持 recovery boundary 恢复与连续续写；recent written/closed 连续边界已接，append 与 fsync 状态锁已拆分；三阶段 append→`write()`(OS cache)→`flush()`(fsync) 原语齐备（`writtenToDiskLsn`/`waitWritten`，守 `flushed<=written`）；2.1 `ClusteredDmlService.commit` 已消费 `DurabilityPolicy`，但 `TransactionManager.commit` 本身仍保持纯内存状态机 |
| `storage.redo` durable IO | `RedoLogWriter`, `RedoLogFlusher`, `RedoLogFileRepository`, `SingleFileRedoLogRepository`, `RotatingRedoLogRepository`, `RedoLogBlockCodec`, `RedoLogBlockScanner`, `RedoBatchFrameCodec` | Implemented | 单文件与默认 ring 共用固定 512B LogBlock v1；内部仍嵌套 RLG1 frame CRC。ring header 已升 v2，`fileBytes` 为 512B 对齐 block 区容量，batch 不跨文件；文件集合、跨文件 LSN/blockNo、末尾 torn 位置均校验。旧 data/ring 格式拒绝且无迁移；只读工厂不创建/修复/force |
| `storage.redo` checkpoint | `RedoCheckpointStore`, `RedoCheckpointLabel` | Implemented | redo-control v2 使用偏移 0/4096 的双槽和 8192B 固定文件，slot 含 redo format、generation、CRC；先选最高 checkpoint 再选 generation；旧 v1/格式不匹配 fail-closed；只读工厂不产生文件副作用 |
| `storage.redo` recovery | `RedoRecoveryReader`, `RedoRecoveryScan`, `RedoLogBlockScanner`, `RedoApplyDispatcher`, `RedoApplyHandler`, `RedoApplyBatchHandler`, `RedoApplyContext`, `FspPageAllocationRedoHandler`, `PageRedoApplyHandler`, `TransactionStateRedoHandler` | Implemented; production-wired by `StorageEngine` E2 | engine 先验证 control/data format；repository 原子返回 batches + retained 边界，scanner 组装完整 batch chain，reader 再验证 checkpoint 覆盖与批次 LSN 无 gap/overlap。dispatcher 的 FSP/page/trx handler 行为不变；READ_ONLY_VALIDATE 走只读 redo data/control channel；只恢复已打开/显式配置的表空间 |
| `storage.redo` capacity | `RedoCapacityPolicy`, `RedoCapacityPressure`, `RedoCapacityDecision`, `RedoCapacityThrottle`, `RedoCapacityThrottle.Reservation`, `RedoCapacityThrottleTimeoutException` | Implemented | `StorageEngine` 和 tests 使用 fixed capacity；4 pressure levels NONE/ASYNC_FLUSH/SYNC_FLUSH/HARD_LIMIT; consumed by `FlushService` and foreground MTR begin reservation throttle；reservation tracks outstanding foreground budgets only, not authoritative LSN ranges |
| `storage.redo` background flush | `RedoFlushWorker`, `RedoFlushWorkerState`, `RedoFlushTarget`, `RedoLogManagerFlushTarget` | Implemented; production-wired by `StorageEngine` | 单 daemon 线程周期/on-demand 驱动 `redo.flush()`，空转跳过、失败即 FAILED；worker 依赖 `RedoFlushTarget` 端口（生产用 `RedoLogManagerFlushTarget` 适配，便于测试注入 fake）；`RedoLogManager` 已拆 state lock 与 `ioLock` |
| `storage.redo` exceptions | `RedoLogIoException` (runtime), `RedoLogCorruptedException` / `RedoLogFormatException` (fatal) | Implemented | 介质/语义损坏与明确的不支持持久格式分别表达；repo/reader/recovery 都 fail-closed |

## Flush + Doublewrite + Checkpoint Slice

### Current Flow

```mermaid
flowchart TD
  Engine["StorageEngine.open/close"] -->|start/stop| Supervisor["PageCleanerSupervisor"]
  Supervisor -->|create/monitor/restart| Worker["PageCleanerWorker (owns Thread)"]
  Worker -->|flushForCapacity| Svc["FlushService"]
  Svc -->|evaluate| CapPolicy["RedoCapacityPolicy"]
  Svc -->|plan| AdaptPolicy["AdaptiveFlushPolicy"]
  Svc -->|flushList / singlePageFlush| Coord["FlushCoordinator"]
  Svc -->|advanceCheckpoint after flush or clean tick| Ckpt["CheckpointCoordinator"]
  Coord -->|dirtyPageCandidates / snapshotForFlush / completeFlush / failFlush| BP["BufferPool"]
  Coord -->|flushedToDiskLsn / waitFlushed WAL gate| Redo["RedoLogManager"]
  Coord -->|stamp| Chksum["PageImageChecksum"]
  Coord -->|beforeDataFileWrite / afterDataFileWrite| DW["DoublewriteStrategy"]
  DW -->|append full-copy or detect-only metadata + force + releaseSlot| DWFile["DoublewriteFileRepository"]
  Batch["DoublewriteBatch (repository primitive)"] -. "test-wired; production dispatch still per-page" .-> DWFile
  Coord -->|writePage + force| Store["PageStore"]
  Ckpt -->|oldestDirtyLsnOr| BP
  Ckpt -->|currentLsn / flushedToDiskLsn| Redo
  Ckpt -->|write label| CkptStore["RedoCheckpointStore"]
  Recover["RecoverableDoublewriteStrategy"] --> DWFile
  Scanner["DoublewriteRecoveryScanner"] -->|latestCopy / scanEntries| DWFile
  Scanner -->|readPage / writePage / force| Store
  Recovery["CrashRecoveryService (StorageEngine E2 + tests)"] -->|scanPageIfNeeded when scanner configured| Scanner
```

### Current Data Chains

| Flow | Current production chain | Current state |
| --- | --- | --- |
| Capacity-driven flush | `StorageEngine.open` -> `PageCleanerSupervisor(factory, maxRestarts=1, backoff=interval, monitorInterval=interval)` -> creates `PageCleanerWorker(flushService, queue, interval, maxPages)` -> worker idle timeout 或显式 request -> `FlushService.flushForCapacity` -> `RedoCapacityPolicy.evaluate(redo.currentLsn(), checkpointLsn)` -> `AdaptiveFlushPolicy.plan(decision,maxPages)` -> if pressure: `FlushCoordinator.flushList` -> per page WAL gate/doublewrite/data file -> checkpoint；if no pressure and dirty view empty: checkpoint-only tick | Implemented production path (E3a + 0.6b)；`StorageEngine.close` 先 `PageCleanerSupervisor.stop(timeout)`（停 monitor + 当前 worker）再停 redo flusher/final `flushThrough`；supervisor 暴露 `PageCleanerMetricsSnapshot`，worker 失败有限重启；foreground reservation throttle 的 ASYNC_FLUSH 只 request supervisor，SYNC_FLUSH/HARD_LIMIT 在 MTR begin 前同步执行 `redo.flush()` + `flushForCapacity(foregroundCapacityFlushMaxPages)`；foreground max pages uses buffer pool capacity and is not capped by background maxPages |
| Single page flush | `FlushCoordinator.flushPage` 持 space S lease -> snapshot -> WAL gate -> checksum/doublewrite -> data write+force -> complete | Implemented foreground path；与 truncate X lease 互斥；WAL gate 仍逐页同步 |
| Tablespace drain | `FlushService.drainTablespace(spaceId, duration)` -> loop `bufferPool.dirtyPageCandidates(MAX, capacity)` filtered by spaceId -> per page `FlushCoordinator.singlePageFlush` -> `advanceCheckpoint()`；当仍有目标 space dirty page 但刷页无进展时调用 `BufferPool.awaitDirtyStateChange(timeout)` | Implemented code; no production caller; dirty-state condition wake-up 已接，`release/completeFlush/failFlush/resetFrameToFree` 会 signal；`flushThrough` 仍保留短 `parkNanos` 路径 |
| Lifecycle flush barrier | `FlushService.flushThrough(marker,timeout)` -> redo flush -> 刷出所有 space 中 oldest<=marker 的 dirty page -> `flush.checkpoint.CheckpointCoordinator.advanceCheckpoint` 直到 checkpoint>=marker | Implemented；truncate 和 `StorageEngine.close/checkpoint` 在物理关闭/缩短前强制调用 |
| Checkpoint advance | `flush.checkpoint.CheckpointCoordinator.advanceCheckpoint` -> no dirty: safe=`min(redo.closedLsn(), redo.flushedToDiskLsn())`; dirty: safe=`min(bufferPool.oldestDirtyLsnOr(flushed), redo.closedLsn(), redo.flushedToDiskLsn())` -> if safe > last: if `checkpointStore != null` -> `checkpointStore.write(RedoCheckpointLabel.of(safe, redo.currentLsn(), now))`, then publish `lastCheckpointLsn = safe` | Implemented code; called by `FlushService` from tests, `StorageEngine` foreground lifecycle, and E3a periodic page cleaner tick；checkpoint 不再用 `currentLsn()` 近似 closed boundary |
| Doublewrite write | recoverable: `RecoverableDoublewriteStrategy.beforeDataFileWrite` -> `repository.append(snapshot)`（内部走 single `DoublewriteBatch`）-> `repository.force()`；detect-only: `DetectOnlyDoublewriteStrategy.beforeDataFileWrite` -> `repository.appendDetectOnly(snapshot)` -> `repository.force()`；data file force 成功后 `afterDataFileWrite` -> `repository.releaseSlot(snapshot)` | Implemented; production `StorageEngine` 默认仍注入 recoverable full-copy；detect-only strategy/repository path 已测试接线。0.5 后 `DoublewriteFileRepository` 默认 1024 个固定 slot 循环复用，in-flight slot 在 data file force 前不可覆盖；0.7 新写 slot 统一 v2 header，scanner 仍兼容 v1 full-copy；`appendBatch/releaseBatch` 连续 slot 原语仍仅 test-wired，生产 `FlushCoordinator` 仍逐页调用 |
| Doublewrite repair/detect | recovery participant 先修显式配置 UNDO page0/读 marker；普通 scanner 对 pageNo>= 当前文件大小的越界页跳过（交 redo 重建）、对 TRUNCATING space 的 pageNo>=target 跳过；其余 checksum-invalid 页经 `scanPageIfNeeded` 区分 `REPAIRED_FROM_COPY` / `DETECTED_ONLY` / `CLEAN_OR_NOT_COVERED` | **Implemented production path（0.2 + 0.7 detect-only）**：`StorageEngine` E2 配 `DoublewriteRecoveryScanner` + `DoublewriteFileRepository.pageIds()`（过滤到恢复已打开空间）；full-copy 可真正修复 torn data/undo 页，detect-only metadata 只报告并计入 `RecoveryReport.detectedOnlyPageCount`，不写 data file；未打开空间的 torn 页留待该空间打开/discovery |

### Package Status

| Package area | Representative classes | Current state | Notes |
| --- | --- | --- | --- |
| `storage.flush` facade/coordinator | `FlushService`, `FlushCoordinator`, `FlushCycleResult`, `FlushResult`, `FlushResultStatus`, `TablespaceDrainResult`, `CoordinatedDirtyVictimFlusher` | Implemented | Ties redo capacity -> flush -> checkpoint；`StorageEngine` 构造 foreground barrier + E3a background page cleaner path；`CoordinatedDirtyVictimFlusher` 适配 buf 淘汰端口到 `singlePageFlush`（CLEAN→true/skip→false/FAILED→抛），`StorageEngine` 注入 pool |
| `storage.flush.policy` adaptive policy | `AdaptiveFlushPolicy`, `FlushAdvice` | Implemented | §7.4 比例版：production(`StorageEngine`) 用 `adaptive`=`clamp(basePages + factor·dirtyPagesBeforeTarget, min, max)`（factor 随压力 0.25/0.5/1.0，NONE→0）；`fixed` 离散档位保留供定向测试；`FlushService` 经 dirty view 计 `dirtyPagesBeforeTarget` 传入 |
| `storage.flush.checkpoint` checkpoint | `CheckpointCoordinator` | Implemented | Fuzzy checkpoint = min(oldestDirty, current, flushed); optional `RedoCheckpointStore` persistence；`StorageEngine` 注入 checkpoint store |
| `storage.flush.doublewrite` doublewrite | `DoublewriteStrategy`, `RecoverableDoublewriteStrategy`, `DetectOnlyDoublewriteStrategy`, `NoDoublewriteStrategy`, `DoublewriteBatch`, `DoublewriteFileRepository`(+`pageIds()`/`scanEntries()`), `DoublewriteRecoveryScanner`, `DoublewriteRecoveryResult`, `DoublewriteRecoveryOutcome`, `DoublewriteMode` | Implemented; **recoverable 模式 production-wired（0.2）+ bounded slot reuse（0.5）+ detect-only metadata/report（0.7）+ repository batch primitive（test-wired）** | `StorageEngine` 默认注入 `RecoverableDoublewriteStrategy`（前向整页副本+fsync，data file force 后释放 in-flight slot）+ E2 配 scanner + `DoublewriteFileRepository.pageIds()`（恢复待检查页来源）；repository 读取兼容 v1 full-copy，新写 full-copy/detect-only 统一 v2 header 并通过 payload 校验区分 `FULL_COPY` / `DETECT_ONLY_METADATA`；`DetectOnlyDoublewriteStrategy` 已有仓储/恢复统计测试，但尚未作为 engine 配置开关暴露；`DoublewriteBatch` 可在同一文件锁内连续写 slot 并一次 force，但生产 `FlushCoordinator` 尚未批量 dispatch；flush-list 与 LRU 双文件/全空间 discovery deferred |
| `storage.flush.cleaner` page cleaner | `PageCleanerSupervisor`, `PageCleanerWorker`, `PageCleanerWorkerHandle`, `PageCleanerWorkerFactory`, `PageCleanerWorkerSnapshot`, `PageCleanerMetricsSnapshot`, `PageCleanerState`, `PageCleanerStoppedException` | Implemented; production-wired by `StorageEngine` E3a | Supervisor daemon monitors worker snapshot, records metrics, restarts FAILED worker up to configured limit；worker remains single daemon `Thread` "minimysql-page-cleaner" with bounded explicit queue + periodic idle tick；no multi-worker dispatch |
| `storage.flush` exceptions | `FlushWriteException`, `FlushBarrierTimeoutException` | Implemented | Root flush exceptions shared by coordinator/doublewrite/cleaner/barrier |

## Transaction Layer Slice

### Current Flow

```mermaid
flowchart TD
  Tests["tests only"] --> Mgr["TransactionManager"]
  Tests --> UndoMgr["UndoLogManager"]
  Engine["StorageEngine"] --> DML["storage.api.dml TableDmlService"]
  Engine --> ClusteredDML["ClusteredDmlService"]
  DML --> ClusteredDML
  DML --> Mgr
  DML --> UndoMgr
  DML --> RollbackSvc["RollbackService"]
  Engine --> Purge["PurgeCoordinator"]
  Engine --> SecondaryMvcc["SecondaryMvccReader"]
  Engine --> SecondaryCurrent["SecondaryCurrentReadService"]
  DML --> Guard["DmlStatementGuard"]
  Guard --> RollbackSvc
  DML --> LockMgr
  DML --> RowGuard["PurgeDmlRowGuardManager"]
  Purge --> RowGuard
  DML --> RedoMgr["RedoLogManager + DurabilityPolicy"]
  Mgr -->|begin| Txn["Transaction"]
  Mgr -->|assignWriteId / commit / rollback| Sys["TransactionSystem"]
  Sys --> ATT["ActiveTransactionTable (pkg-private)"]
  Sys -->|allocateWriteId| TxId["TransactionId (domain)"]
  Sys -->|allocateTransactionNo| TxNo["TransactionNo (domain)"]
  Txn --> State["TransactionState (enum FSM)"]
  Txn --> Opts["TransactionOptions"]
  Opts --> Iso["IsolationLevel (drives ReadView policy)"]
  Txn -->|"lazy setUndoContext (pkg-private)"| Ctx["UndoContext (T1.3c)"]
  UndoMgr -->|planX / appendPlanned / publish UndoContext| Txn
  UndoMgr -->|snapshot / plan / reserve / create / open / append| Access["UndoLogSegmentAccess (storage.undo)"]
  UndoMgr -->|reserve / preflight / bind owner| Slots["RollbackSegmentSlotManager (runtime state machine)"]
  UndoMgr -->|cache-first/free FIFO peek + lease publish| Cache["UndoSegmentReuseDirectory"]
  UndoMgr -->|INSERT/UPDATE/mixed commit| Finalizer["UndoSegmentFinalizer"]
  RollbackSvc -->|live / recovery finalization| Finalizer
  Purge -->|committed history finalization| Finalizer
  Finalizer -->|drop ineligible segment| Allocator["UndoSpaceAllocator"]
  Finalizer -->|owner + history/free base CAS| RsegHeader["RollbackSegmentHeaderRepository page3 v4"]
  Finalizer -->|globally sorted history/cache/free first-page batch| Access
  UndoMgr -->|begin append lease| History["HistoryList persistent-chain projection"]
  Purge -->|peek + begin head-removal lease| History
  Finalizer -->|MTR 后 publish| History
  History --> Barrier["HistoryTablePurgeBarrier -> storage.api TablePurgeBarrier"]
  Finalizer -->|reserve push / commit 后 publish| Cache
  Finalizer -->|begin lease / commit 后 complete| Slots
  Tests --> LockMgr["LockManager (0.17)"]
  Engine --> LockMgr
  LockMgr --> LockKeys["Record/GAP/Next-key/Insert-intention keys"]
  LockMgr --> LockSnap["LockSnapshot / wait-for edges"]
  LockMgr --> Sink["RowLockEventSink 事件端口 (storage.trx.lock)"]
  LockObs["server.lockobs LockObservationService 实现"] -->|implements 向下依赖| Sink
  Engine --> LockObs
  Engine --> LockDiag["lockDiagnosticSnapshot(data_locks/data_lock_waits)"]
  LockDiag --> LockObs
  Slots -->|rollbackSegmentId| RsegId["RollbackSegmentId (domain)"]
  Ctx --> SlotId["UndoSlotId (domain)"]
  RollbackSvc --> Access
  RollbackSvc --> Mgr
```

### Current Data Chains

| Flow | Current production chain | Current state |
| --- | --- | --- |
| Begin | `SessionTransactionPolicy.ensureTransaction` -> `SqlStorageGateway.begin` -> `DefaultSqlStorageGateway.begin` -> `TransactionManager.begin(options)` -> `new Transaction(options, now)` state `ACTIVE`; **no id allocation** (lazy) | Implemented；Session 持 opaque handle，不越层接触 storage transaction；低层调用方仍可显式持事务调用 DML facade |
| Assign write id | `TableDmlService.insert/update/delete` (`TableDmlService.java:130/193/266`) -> `ClusteredDmlService` anchor -> `TransactionManager.assignWriteId(txn)` -> requires `ACTIVE`, rejects read-only -> `system.allocateWriteId()` -> `txn.setTransactionId`; idempotent if already set | Implemented；表级和低层单聚簇入口共享同一 active transaction table |
| Commit | `TableDmlService/ClusteredDmlService.commit` -> `prepareCommit` 分配 no -> `UndoLogManager.onCommit`：有 UPDATE 时冻结 history base，并把当前 reachable logical chain 的 `affectedTableIds` 写入 `HistoryEntry` -> finalizer 单 MTR 处理 owner/history/free/terminal -> commit 后发布 slot/reuse/history table counters -> transaction commit -> durability -> release locks | Implemented；同一 history entry 对同一 table 只计一次；纯 INSERT 不进 history；transition lock 不跨 IO，失败边界保持原有 fail-stop 语义 |
| Prepare / commit prepared | `StorageEngine.preparedTransactionService` -> `PreparedTransactionService.prepare` -> `UndoSegmentFinalizer.prepareTransaction` 同 MTR 把全部 INSERT/UPDATE first-page 标为 PREPARED + `PREPARE` delta -> `TransactionManager.finishPrepare` 释放 ReadView但保留 active/row locks -> redo fsync；phase-two `commitPrepared` -> 预留 transaction no -> drop prepared INSERT + UPDATE append history + `PREPARED_COMMIT` delta -> COMMITTED -> terminal redo fsync -> release locks | Implemented storage participant v1；prepare/phase-two 均固定强持久；durability 确认失败保留 PREPARED 或 terminal+locks，相同决议可重试；XID registry/SQL XA 不在 storage |
| Rollback prepared | `PreparedTransactionService.rollbackPrepared`（legacy index 或 DD resolved 命令）-> `RollbackService.rollbackPrepared` -> PREPARED_ROLLING_BACK -> 双 persistent head按 undoNo 归并 inverse/marker -> `UndoSegmentFinalizer.finalizePreparedRollback` 同 MTR drop全部 owner、clear page3、写 `PREPARED_ROLLBACK` -> ROLLED_BACK -> terminal redo fsync -> release locks | Implemented；普通 rollback 拒绝 PREPARED；中途失败保留 prepared rollback 重试态、active membership 与锁，不能切换相反决议 |
| Rollback (multi-index) | production terminal -> `ResolvedDmlRollbackCommand` -> `TableDmlService.rollback` (`TableDmlService.java:338`) -> `RollbackService` 预检双 persistent head -> 每条 undo 解析 exact-version `TableIndexMetadata` (`DictionaryIndexMetadataResolver.java:37`) -> 共用 row guard，secondary inverse 按 index id 各自短 MTR、clustered inverse 最后、logical-head marker 再后 -> 双链 EMPTY 后 batch finalization/terminal | Implemented for statement/full/recovery；INSERT/UPDATE/DELETE secondary inverse、clustered inverse 与 marker-lag 均幂等；`RollbackService.java:842` 的 stable hook 验证首棵 secondary commit 后 persistent head 不提前移动 |
| Statement/savepoint rollback | SQL INSERT/UPDATE/DELETE -> `DefaultSqlStorageGateway` -> `DmlStatementGuard` -> `RollbackService.createSavepoint` 快照双 head；TableDml/LOB/secondary 任一失败由 guard 调同一 multi-index inverse，再用 marker MTR CAS logical head | Implemented；UPDATE rollback 释放 replacement 新 LOB 并恢复旧版本/secondary，DELETE rollback revive；局部链高水位不回退，事务保持 ACTIVE；命名 SAVEPOINT 与 savepoint 后锁精细释放未接 |
| Undo write (INSERT/UPDATE/DELETE) | `TableDmlService` (`TableDmlService.java:130/193/266`) 在聚簇首 MTR 前冻结按 index id 排序的 `SecondaryUndoMutation`（INSERT_ENTRY / CHANGE_KEY / DELETE_MARK_ENTRY）-> `ClusteredDmlService` 调 `UndoLogManager.planX/appendPlanned` 写同一行级 undo anchor -> 聚簇写成功后各 secondary 独立短 MTR 发布 (`TableDmlService.java:370/420`) | Implemented；codec secondary tail 与 LOB tail 可组合，旧记录解码为空 mutation 列表；失败由 statement/full/recovery rollback 从同一 tail 收敛 |
| Slot claim | `RollbackSegmentSlotManager.reserveClaim` 短锁 `FREE -> RESERVED` -> 当前业务 MTR `RollbackSegmentHeaderRepository.requireSlotFree` 以 S latch 预检 page3 并在返回前释放 -> `access.create` -> claim lease `bind(firstPageId)` 变 ACTIVE -> 同一业务 MTR 在局部 latch-order 例外内 `claimSlot(slot,firstPageId)` X-latch CAS -> append/publish context | Implemented；RESERVED 计入占用但无 owner，未 bind lease close 才可退回 FREE；持久 claim 再次 CAS 防异常漂移；page3 header/slot 写有 `UndoMetadataDeltaRecord(RSEG_HEADER_FIELD/RSEG_SLOT)` |
| Atomic slot release / segment finalization | `RollbackSegmentSlotManager.beginBatchFinalization` all-or-none 取 FINALIZING leases -> `UndoSegmentFinalizer` 短读 owner/kind/drop plan/history/free evidence -> 精确单 fragment 先尝试同 kind cache，未接纳则进入 free FIFO，其余 drop -> 动态预算 MTR 固定 FSP page0/page2→page3→全部普通 first pages 全局排序，原子修改 owner、history/free base/link 与 terminal delta -> commit 后发布 slot/reuse directory/history | Implemented for INSERT/mixed commit、live/recovery 双段 rollback、committed UPDATE purge；cache 容量满/0/transition busy 可降级 free，只有不合格 segment 才 drop；history/reuse lease 不持锁跨 IO；物理前失败恢复运行时 transition，物理后失败保留 fail-stop fence |
| ReadView 创建 (T1.4) | `ReadViewManager.openReadView(txn)` -> 按隔离级别 RR 缓存到 `Transaction.readView` / RC 新建 / RU·SERIALIZABLE 抛 -> `TransactionSystem.openReadViewSnapshot(txn)`（锁内：可写事务分配 creator id + 原子捕获 {activeIds, nextId, **nextTransactionNo→ReadView.lowLimitNo**} 建 `ReadView` 并登记 live 集合，purge 用） | Implemented and SQL point/range-read production-wired；commit/finishRollback 调 `release` 清 RR 缓存并注销 live view；gateway 的 point/range consistent read 都在 finally 注销 RC view，且 view 生命周期覆盖 secondary scan、聚簇/undo、external LOB hydration 与公开投影 |
| Version-safe secondary/LOB purge | `PurgeCoordinator.runBatch` -> eligible history head -> 从持久 logical head 逐记录构造 secondary/clustered/LOB task -> row guard zero-wait -> secondary safety/clustered remove -> `PURGE_RECORD_PROGRESS` MTR 批量释放 purge-old LOB 并 CAS 到 predecessor -> EMPTY 后 finalizer/unlink | Implemented；A→B→A 不误删，DELETE 固定 secondary-first；索引 task durable 后可幂等重试，LOB free 与 head advance 同批，progress durable 后不会重复释放已可复用页；history/table barrier 在 finalization 后推进 |
| Consistent read (primary + unique/non-unique secondary) | SQL SELECT -> RR/RC ReadView -> 主键走 `MvccReader`；unique secondary 走 `SecondaryMvccReader.readUnique`；non-unique complete logical key 走 `readRange`：含 marked prefix scan 的 secondary MTR 先提交，再逐候选回聚簇 MVCC、从可见完整行重算 key并按 clustered identity 去重 -> 同一 view 内 LOB hydration/投影 -> RC finally close | Implemented；point 仍主键优先/最小 unique id；range 选择完整单列无 prefix 的最小 non-unique id；NULL equality 直接空；secondary/clustered/undo latch 不重叠，range candidate 超过 4096 显式失败 |
| Secondary locking current read | SQL `FOR SHARE/FOR UPDATE` non-unique logical-prefix SELECT -> RW transaction/write id -> `SecondaryCurrentReadService` normalized prefix REC_S/REC_X -> including-deleted candidate 短 MTR -> 每个 clustered key 走 `BTreeCurrentReadService.lockPoint` REC_S/REC_X -> 当前完整行重算 predicate -> gateway LOB hydrate/project | Implemented；prefix 与 clustered 锁保持到 autocommit/显式事务终态；同 prefix INSERT、key-changing UPDATE、DELETE 在页操作前申请 logical X；锁等待共享 absolute budget 且不持 page latch。point locking、任意上下界范围、SERIALIZABLE 未接 |
| Table purge barrier / DROP | commit/recovery publish `HistoryEntry.affectedTableIds` -> `HistoryList.awaitTableUnreferenced` 在同一显式锁下维护 per-table 引用计数/Condition -> `HistoryTablePurgeBarrier.awaitUnreferenced` (`HistoryTablePurgeBarrier.java:36`) -> DD `DictionaryDdlService.dropTable` (`DictionaryDdlService.java:303`) 在 table MDL X 下、写 PREPARED/发布 DROP_PENDING 前等待；logged/legacy pending recovery (`DictionaryDdlRecoveryService.java:314`) 在物理删除前再次等待 | Implemented；timeout 保持 ACTIVE 或 DROP_PENDING+文件及当前 marker phase；purge finalization 原子摘 history 后唤醒；无独立持久计数，page3/first-page history 始终是重启真相 |
| Row-lock diagnostics (2.8a) | `LockManager.acquire/release/releaseAll` -> `RowLockEventSink` 事件端口（端口与事件载荷 `RowLockObservation`/`RowLockBlocker`/`ThreadEventId` 定义在 `storage.trx.lock`，server.lockobs 向下实现，无反向依赖）；`StorageEngine.lockDiagnosticSnapshot` -> `lockManager.snapshot()` -> `DefaultLockObservationService.captureSnapshot` -> `data_locks` / `data_lock_waits` rows | Implemented; row-lock only；不授锁、不 release、不 rollback；session/statement id 为 0，DD 未接所以 schema/table 为空、index 名为 `index#<indexId>` |

### Package Status

| Package area | Representative classes | Current state | Notes |
| --- | --- | --- | --- |
| `storage.api.trx` prepared facade | `PreparedTransactionService` + phase-one/commit/legacy-resolved rollback commands/results | Implemented; production-held and exposed by `StorageEngine` | 强制 redo fsync、terminal 后锁收尾；只表达 resource-manager participant，不保存/暴露 XID；resolved rollback 不向上暴露 B+Tree/page |
| `storage.trx` facade | transaction/undo/finalizer/rollback, `MvccReader`, `SecondaryMvccReader`, `SecondaryCurrentReadService`, `PurgeCoordinator`, `HistoryList`, `HistoryTablePurgeBarrier` | Implemented; production-held by `StorageEngine`; DML/gateway/DD-wired | 多索引 undo/rollback、prepared phase-one/two、primary/unique/non-unique secondary MVCC、logical-prefix locking read、version-safe purge、persistent-history DROP barrier 均接入；仍无命名 SAVEPOINT、RU/SERIALIZABLE、多 worker purge |
| `storage.trx` system | `TransactionSystem`, `ActiveTransactionTable`, `TransactionCounterSnapshot` | Implemented; production-held by `StorageEngine` | `ReentrantLock` 短锁保护 id/no/active/read views；checkpoint 用 `snapshotCounters()` 原子读取两个 next-counter（不消费号码），recovery 用 `restoreCounters` 只前进不回退，并在 gate 关闭期间短暂恢复 PREPARED active owner |
| `storage.trx` aggregate | `Transaction`, `TransactionState`, `TransactionOptions`, `IsolationLevel`, `UndoContext`, `TransactionSavepoint`, `EmptyUndoBoundary`, `ReadView` | Implemented; DML/prepared facade consumes explicit transaction | ACTIVE/PREPARED/COMMITTING/ROLLING_BACK/PREPARED_ROLLING_BACK/terminal 状态机 + rollback-only；惰性双 `UndoContext`、savepoint/empty boundary、RR/RC ReadView；recovery 可从 first-page 重建最小 PREPARED 聚合；RU/SERIALIZABLE 仍拒绝 |
| `storage.trx.lock` lock core | existing record/gap/next-key/insert-intention keys + `SecondaryLogicalKeyLockKey`, snapshots/events/exceptions | Implemented; production-held by `StorageEngine`; DML/locking-read-wired | 按 indexId 分片显式锁表、Condition 有界等待、wait-for/deadlock 与 `releaseAll` 不变；normalized secondary logical prefix 支持 S/S、S/X、X/X 矩阵，SELECT 与 DML 共用同一事务持锁集合；LockManager 不反向依赖 DD/SQL |
| `server.lockobs` row-lock diagnostics | `LockObservationService`(extends `storage.trx.lock.RowLockEventSink`), `DefaultLockObservationService`, `SnapshotRequest`, `DataLockRow`, `DataLockWaitRow`, `LockDiagnosticSnapshot`, `WaitSlotSnapshot`, `DeadlockReport` | Implemented; production-wired by `StorageEngine` | 第一阶段只**向下依赖** `storage.trx.lock` 的事件端口与只读快照类型：实现 `RowLockEventSink` 消费 row-lock 事件，并追加 `captureSnapshot`/`latestDeadlocks` 生成 Java API 级 `data_locks` / `data_lock_waits` 当前快照与最近 deadlock report；不实现 SQL 视图、MDL、物理 latch/mutex/condition 采集或跨域 victim |
| `storage.trx` undo context | `UndoContext`, `UndoLogBinding`, `TransactionSavepoint`, `EmptyUndoBoundary` | Implemented; table DML/rollback/history-wired | `lastUndoNo` 为全局 append 高水位；双 binding/savepoint 语义不变；UPDATE reachable logical head 同步维护 `undoNo -> tableId` 投影，savepoint/full rollback 剪枝后 `affectedTableIds()` 精确生成 commit history 表集合 |
| `storage.trx` rseg slot | `RollbackSegmentSlotManager`, `UndoSlotExhaustedException` | Implemented | 固定单一默认 rseg 的内存投影；显式 `FREE/RESERVED/ACTIVE/FINALIZING`，claim/finalization RAII lease 与 restore 都由 `ReentrantLock` 短临界区保护，锁内无 IO；RESERVED/FINALIZING 均计占用，恢复按 page3 下标重建 ACTIVE |
| `storage.trx` exception | `TransactionStateException`, `UndoSlotExhaustedException`, `UndoWriteStalePlanException`, `UndoWriteFatalException`, `UndoClaimPublicationException`, `UndoFinalizationException`, lock exceptions；`storage.undo.UndoSlotOwnershipConflictException` | Implemented; production-reachable | All extend project exception hierarchy；stale plan/reservation 前错误可重试；page3 预检 owner 冲突在物理分配前可恢复；external/root 物理写开始后、segment bind 后 owner/context 发布失败与 finalization 物理边界后的不确定失败均抛 fatal，调用方不得同进程重试 |

## Recovery Layer Slice

### Current Flow

```mermaid
flowchart TD
  Request["RecoveryRequest (input bundle)"] --> Svc["CrashRecoveryService.recover"]
  Svc -->|1. closeForRecovery| Gate["RecoveryTrafficGate"]
  Svc -->|2. scanPageIfNeeded per page| Scanner["DoublewriteRecoveryScanner (flush)"]
  Svc -->|3. readLatest| CkptStore["RedoCheckpointStore (redo)"]
  Svc -->|"3. initialize baseline"| TrxContext["TransactionRecoveryContext -> two-slot sidecar"]
  Svc -->|3. new RedoRecoveryReader| Reader["RedoRecoveryReader (redo)"]
  Svc -->|3. readBatches| Reader
  Svc -->|3. applyAll batches, ctx| Dispatcher["RedoApplyDispatcher registry (redo)"]
  Dispatcher --> Handler["RedoApplyHandler batch sessions"]
  Handler --> FspHandler["FspPageAllocationRedoHandler -> ensureCapacity (FSP_PAGE_ALLOC) / no-op (FSP_PAGE_FREE)"]
  Handler --> PageHandler["PageRedoApplyHandler -> PAGE_INIT / PAGE_BYTES / FSP_METADATA_DELTA patch + PageStore"]
  Handler --> TrxContext
  Svc -->|"4. restoreRecoveredBoundary (if recoveredRedoManager)"| Boundary["REDO_BOUNDARY_INSTALL"]
  Svc -->|"5. resumeAfterRedo (if undo participant)"| UndoResume["UNDO_TABLESPACE_RESUME"]
  Svc -->|"6. reconcileSpaceFiles (if spacesToReconcile)"| Reconcile["SPACE_FILE_RECONCILE: validate+page0 size -> PageStore.ensureCapacity"]
  Reconcile --> Codec["SpaceHeaderRawCodec.readPhysical"]
  Svc -->|"7. recoverAfterRedo"| TrxUndo["UNDO_ROLLBACK + rebuild persistent history/table refs"]
  TrxUndo --> TrxWork["page3 v4 -> validate cache/free/FSP/history -> PREPARED decision -> ACTIVE multi-index rollback"]
  Svc -->|"8. resumePurgeAfterRedo"| ResumePurge["RESUME_PURGE real batches"]
  ResumePurge --> TrxWork
  Svc -->|"9. flush recovered redo"| RedoFlush["RedoLogManager.flush"]
  Svc -->|"10. forceAll (durability barrier)"| Force["PageStore.forceAll"]
  Svc -->|11. openForUserTraffic| Gate
  Svc -->|fail| Fail["failClosed -> RecoveryStartupException"]
  Gate --> States["RecoveryState: CLOSED/RECOVERING/OPEN/READ_ONLY/FAILED"]
```

### Current Data Chains

| Flow | Current production chain | Current state |
| --- | --- | --- |
| Recovery orchestration | `DatabaseEngine(config, preparedDecisionProvider)` -> rebuild DD + DDL log并校正 control high-water -> DD discovery -> `StorageEngine(config, provider).open(existing)` doublewrite/redo/undo resume/reconcile -> `PersistentHistoryRecovery.rebuild` 重建 affected-table history -> provider 对每个 PREPARED 给 COMMIT/ROLLBACK（UNRESOLVED fail-closed）-> ACTIVE multi-index rollback -> `StorageEngine.resumePurgeAfterRedo` 独立运行真实 `RESUME_PURGE` -> redo flush/force/open -> `DictionaryDdlRecoveryService` 按 ddl id 收敛 CREATE/DROP marker、兼容续作 legacy DROP_PENDING、对账/修复 ACTIVE 表 SDI，最后执行 orphan cleanup -> public OPEN | Implemented production path；现有单参构造安装默认 unresolved provider；PREPARED 在 gate 关闭期重建最小 transaction/undo context并复用 live phase-two；CREATE/DROP/CREATE INDEX recovery 语义不变；仍缺 server XID/SQL XA、catalog-loss 场景的 SDI 发现/重建与对象级 force recovery |
| Space file reconcile (autoextend crash-safety) | undo resume 后若 `spacesToReconcile()` 非空：`reconcileSpaceFiles` 逐空间 `PageStore.readPage(page0)` -> `SpaceHeaderRawCodec.readPhysical` -> `validateReconcileHeader`（spaceId/pageSize 一致、size>0、偏移不溢出，否则 `TablespaceCorruptedException`）-> 幂等 `PageStore.ensureCapacity`；replay 期 `PageRedoApplyHandler` 仅对 PAGE_INIT extend-on-demand，首触越界 PAGE_BYTES 判 `RedoLogCorruptedException` | Implemented; `StorageEngine` E2 对系统 UNDO + 显式配置数据表空间执行；只恢复物理文件长度，不重建 FSP bitmap；弥补 autoExtend 不 fsync 在崩溃后留下的"物理短于 page0 逻辑"背离 |
| Failure path | any `DatabaseRuntimeException` / `RuntimeException` inside an active stage -> `RecoveryProgressJournal.stageFailed`（memory + JSONL sink）-> `failClosed(mode, e)` -> `gate.failClosed(error)` -> state `FAILED` -> FAILED `RecoveryReport` with zeroed LSNs/counts -> throw `RecoveryStartupException` | Implemented; gate stays closed on failure; failed stage is visible through `StorageEngine.recoveryDiagnostics()` and `EngineConfig.recoveryProgressFile()`；若 failure progress 本身写文件失败，会作为 suppressed cause 保留但仍继续 fail-closed；`RecoveryStartupException` extends `DatabaseFatalException` |

### Package Status

| Package area | Representative classes | Current state | Notes |
| --- | --- | --- | --- |
| `storage.recovery` facade | `CrashRecoveryService`, `RecoveryTrafficGate`, `RecoveryState`, `RecoveryProgressJournal`, `RecoveryProgressSink`, `FileRecoveryProgressSink`, `RecoveryDiagnosticsSnapshot` | Implemented; production-wired by `StorageEngine` E2 | `DatabaseEngine` 只在 storage recovery + DDL recovery 成功后构造 parser/binder/session registry 并发布 OPEN；普通 storage accessor/DML 仍要求 recovery gate OPEN，`openSession` 在 NEW/OPENING/CLOSING/FAILED/CLOSED 全部拒绝；`RecoveryTrafficGate.enterReadOnlyDiagnostic()` 不开放普通 Session |
| `storage.recovery` request/report | `RecoveryRequest`, `RecoveryReport`, `RecoveryMode`, `RecoveryStageName`, `RecoveryProgressEvent` | Implemented; production-wired by `StorageEngine` E2 | `StorageEngine` builds NORMAL request with redo repo/checkpoint/dispatcher/context/recovered manager/undo participant/reconcile spaces + **doublewrite scanner + `dwRepo.pageIds()`（过滤到恢复已打开空间）（0.2）**；`RecoveryRequest.readOnlyValidate` + `EngineConfig.withRecoveryMode(READ_ONLY_VALIDATE)` build the non-mutating scan branch；non-NORMAL recoveryMode is rejected on fresh open before redo/doublewrite/undo formatting；`RecoveryReport` remains final snapshot, while progress events record stage start/complete/fail in memory and JSONL file；progress file is diagnostic-only and is never read to skip recovery；read-only validation reports `appliedBatchCount=0` |
| `storage.recovery` prepared decision | `PreparedTransactionDecisionProvider`, `PreparedTransactionDecision`, `RecoveredUndoState.PREPARED`, reconciliation lists | Implemented; provider injected by `StorageEngine` constructor | redo/page3/first-page 同态闭包；baseline 可覆盖已回收 PREPARE delta；COMMIT/ROLLBACK 在 OPEN 前执行并 fsync新 terminal redo；默认 UNRESOLVED 阻止 OPEN；不重建已丢失的 live row-lock handles |
| `storage.recovery` exception | `RecoveryStartupException` | Implemented | Extends `DatabaseFatalException`; thrown by `CrashRecoveryService` on fail-closed |

## Undo Log Layer Slice

### Current Flow

```mermaid
flowchart TD
  Engine["StorageEngine production composition"] --> Dml["TableDmlService + ClusteredDmlService"]
  Engine --> Recovery["TransactionUndoRecoveryParticipant"]
  Engine --> Purge["PurgeCoordinator"]
  Dml -->|planInsert / planUpdate / planDelete / appendPlanned / onCommit| UndoMgr["UndoLogManager (trx)"]
  Dml -->|full / statement rollback| Rollback["RollbackService (trx)"]
  Engine --> Mvcc["MvccReader + SecondaryMvccReader"]
  Engine --> SecondaryCurrent["SecondaryCurrentReadService"]
  UndoMgr -->|snapshot / plan / reserve / create / open / append| Access
  Rollback -->|current/predecessor short read / per-record logical-head CAS| Access
  Recovery -->|ACTIVE rollback via RollbackService| Rollback
  Purge -->|logicalHead / readRecord + secondary mutation tasks| Access
  Mvcc -->|readRecordByRollPointer| Access
  Tests["tests"] -.-> Access
  Access -->|create / open| Segment["UndoLogSegment (multi-page handle)"]
  Access -->|createUndoSegment| Adapter["DiskSpaceUndoAllocator (api, implements UndoSpaceAllocator)"]
  Adapter -->|createSegment UNDO + allocatePage| DSM["DiskSpaceManager"]
  Adapter -->|new UndoSegmentHandle| Handle["UndoSegmentHandle (VO)"]
  Access -->|new UndoPageAccess| PageAccess["UndoPageAccess"]
  Access -->|new UndoRecordCodec| Codec["UndoRecordCodec"]
  Access --> Resolver["UndoStoredRecordResolver"]
  Resolver --> Codec
  Resolver --> Payload["UndoPayloadStorage / UNDO_PAYLOAD pages"]
  Segment -->|appendPlanned: inline or descriptor| Codec
  Segment -->|append root| Page["UndoPage"]
  Segment --> Payload
  Segment -->|planned reservation + growAndAppendReserved| Adapter
  Segment -->|growAndAppendReserved| PageAccess
  PageAccess -->|mtr.newPage UNDO / mtr.getPage| Mtr["MiniTransaction"]
  Segment -->|returns RollPointer| RP["RollPointer (domain)"]
  RP -->|T1.3c: written into clustered records| Clustered["DB_ROLL_PTR via insertClustered"]
  UndoMgr -->|reserve / preflight / bind / persist| Slots["RollbackSegmentSlotManager (trx runtime FSM)"]
  UndoMgr -->|same-kind cache first / free FIFO second| Cache["UndoSegmentReuseDirectory"]
  Cache -->|persistent owner/history/free authority| Page3["RollbackSegmentHeaderRepository page3 v4"]
  UndoMgr -->|attach INSERT/UPDATE binding| TxnCtx["UndoContext on Transaction (dual local heads)"]
```

### Current Data Chains

| Flow | Current production chain | Current state |
| --- | --- | --- |
| Undo segment acquire | `UndoLogManager.planX` 按 record 类型选择 INSERT/UPDATE binding；目标 kind 尚无 log 时先由 `UndoSegmentReuseDirectory.peekCache(kind)` 冻结同 kind LIFO top/count，未命中再冻结 free FIFO head/successor/tail/length，依次形成 `REUSE_CACHED` / `REUSE_FREE` / `ALLOCATE_NEW` -> 业务 MTR reserve active slot；cache reuse 原子 page3 top→slot + activate，free reuse 原子 page3 摘头→slot、按全局页序清 successor.prev 并以新 kind/txn 激活，fresh 才创建 FSP segment；随后 append、bind、attach | Implemented and production-reachable；同一事务最多一条 INSERT 与一条 UPDATE log；cache 按 kind 有界 LIFO，free 跨 kind FIFO；reuse 只为 external payload 预留新页，stale owner/base/link 在物理前失败，物理后失败 fail-stop；仍是单系统 undo space/单默认 rseg |
| Undo append | `UndoLogSegmentAccess.planRecord` -> `UndoRecordWritePlan` 冻结完整 codec bytes（含 INSERT ownership 尾部）、fresh-root inline/external 决策、页数与 CRC -> `plannedNewPages` 算 root grow + payload 精确页数 -> execution `UndoLogSegment.appendPlanned` -> `preflightAppend` 校验 creator/index/物理高水位/persistent logical predecessor -> external 先写 payload 链再在普通 UNDO 槽发布 35B descriptor，inline 直接写编码 -> 构造 `RollPointer` -> 同一 MTR 更新 record count、物理 `LOG_LAST_UNDO_NO` 与 `UndoLogicalHead` | Implemented and production-reachable；ownership 不绕过既有 external undo payload/CRC/reservation；root 槽仍由 `UNDO_RECORD_PAYLOAD` redo，payload 页由 `PAGE_INIT(UNDO_PAYLOAD)+PAGE_BYTES` 保护，header count/high-water/head pair 由 metadata after-image 保护；物理高水位在 partial rollback 后不回退 |
| Undo main-chain growth | 已规划 root payload 若尾页放不下 -> consume 已预留 quota -> `allocateChainPageForAppend` -> `createChainPageForAppend` -> current/new/first FIL 链元数据更新 -> `appendRecord` | Implemented；reservation 已在任何 root/payload 页修改前覆盖 grow + external 总页数；`ALLOCATED->UNDO` double-newPage 复用通用 redo；main chain 只含 `PageType.UNDO`，external payload 页不改变 `lastPageId` |
| External undo payload write/read | write: `UndoPayloadStorage.write` 按计划分块 -> 同 segment `allocatePage` -> `UndoPayloadPage.format(PageType.UNDO_PAYLOAD)`，页保存 prev/next、chunk index、segment/inode identity、transactionId、undoNo、total length/page count/CRC；read: `UndoStoredRecordResolver` 识别 root tag `0x7F` -> `UndoPayloadStorage.read` 逐页短 fix 并严格验证全部证据/链接/配置上限 -> 整值 CRC -> `UndoRecordCodec.decode` 严格消费全部字节 -> descriptor identity 复核 | Implemented；root descriptor v1 固定 35B；`EngineConfig.maxExternalUndoPayloadPages` 默认 16 且不大于最小 buffer-pool instance；payload 与业务 `PageType.BLOB`/`LobReference` ownership 完全分离，同 FSP undo segment 的 drop 自动统一回收 |
| Undo page create | `UndoPageAccess.createFirstPage`/`createChainPage` -> production lease+Registry require -> `mtr.newPage(..., PageType.UNDO)` + envelope -> `UndoPage.format*` 写 flags format version 3；first page 初始化 EMPTY logical head 与 NULL history prev/next，chain 页复制同一 `UndoLogKind`，record area=136 | Implemented；v1/v2/未知版本不迁移且 open fail-closed；每张普通 UNDO 页可独立做 kind/type 守门，history link 只在 first page 读写 |
| Undo page open | `UndoPageAccess.openUndoPage` -> production lease+Registry require -> `mtr.getPage` -> PageType.UNDO gate -> `UndoPage.requireCurrentFormat` | Implemented；first/chain 任一页都执行 version gate，避免 direct roll-pointer read 绕过兼容性校验 |
| Reusable page reset/activation | finalization 对单普通页、`used=fragment=1, extent=0` 的 segment 先尝试 `STATE_CACHED`，cache 不接纳则 `STATE_FREE` 尾插 FIFO；两者清事务/commit/count/head，旧 record bytes 留在不可寻址区。cache activation 保持 kind，free activation 覆盖新 kind；FREE 复用 history prev/next 物理槽保存 free links，并用独立 redo kind | Implemented；cache/free/header/FSP 证据恢复期交叉验证；多页、external 或 extent segment 不 shrink，直接 drop |
| Persistent history/reuse node mutation | commit、rollback、purge 按场景把 history old/new、cache reset、free old tail/new nodes 一次收集，并在 page3 owner/base 写后按 `(spaceId,pageNo)` 全局升序获取全部普通 undo 首页；history 写 prev/next/state/commitNo，free 写 tail.next 与新节点 prev/next，purge/free reuse 清新 head.prev | Implemented and production-reachable；全部证据先校验后写；没有多个 helper 分别获取局部首页集合；history/free links 使用稳定独立 metadata redo kind |
| Undo record/logical-chain read | `UndoLogSegment.readRecord` / `UndoLogSegmentAccess.readRecordByRollPointer` / `forEachRecord` -> root page segment/creator/index/pointer 校验 -> `UndoStoredRecordResolver` 对 inline 直接 codec、对 descriptor 完整加载 external chain 后 codec；rollback/recovery 共用 `RollbackService.readUndoRecord` 短读 current/真实 predecessor，purge 与 MVCC 均经同一 access 入口 | Implemented and production-reachable；full/recovery 在 inverse 前验证 current=head pair、前驱归属及 undoNo 严格下降；external 逐页用 MTR savepoint 释放已读 payload 页，任一 B+Tree 逆操作前不持 undo latch/fix；`forEachRecord*` 仅物理诊断/测试使用 |
| Transaction undo write (1.6 dual log) | `planInsert` 只读 INSERT binding/reuse directory，`planUpdate/planDelete` 只读 UPDATE binding/reuse directory -> immutable `UndoWritePlan(kind,acquisition,globalHighWater,targetSnapshot/cacheOrFreeCandidate)` -> DML admission/begin -> `appendPlanned` -> record predecessor=同 kind 局部 head -> 同 MTR append/header update -> `UndoContext.publishAppend` 更新目标 binding head 与事务全局 `lastUndoNo` | Implemented and DML production-reachable；事务 undoNo 全局唯一递增；`ALLOCATE_NEW/REUSE_CACHED/REUSE_FREE/APPEND_EXISTING` 四路预算不同；旧 `beforeX` API 已删除 |
| Single-page undo (T1.3a) | `UndoLog.append(page, rec, keyDef, schema)` -> `codec.encode` -> `page.appendRecord` -> `new RollPointer` (`UndoLog.java:29-35`) | Implemented (test-only); superseded by `UndoLogSegment` for multi-page; only `UndoLogStoreTest` uses it |

### Package Status

| Package area | Representative classes | Current state | Notes |
| --- | --- | --- | --- |
| `storage.undo` access | `UndoLogSegmentAccess`, `UndoLogSegment`, `UndoAppendSnapshot`, `UndoHistoryNodeSnapshot`, `UndoFreeListNodeSnapshot`, `UndoRecordWritePlan`, `UndoStoredRecordResolver`, `UndoPayloadStorage`, `UndoLogicalHead`, `UndoLog`, `UndoSegmentHandle`, allocation ports | Implemented; production-held by `StorageEngine` | create/open/plan/append、external chain、commit header、logical-head CAS、history/free node inspect/link/unlink/activate、按 pointer 直读均生产可达；rollback/recovery/purge/MVCC 共用 resolver；`UndoLog` 单页 predecessor 与 `forEachRecord*` 仅测试/诊断 |
| `storage.undo` page | `UndoPageAccess`, `UndoPage`, `UndoPageLayout`, `UndoLogState`, `UndoPayloadPage`, `UndoPayloadPageLayout`, `UndoRedoDeltas` | Implemented | 普通 UNDO v3 flags gate + first-page 15-byte logical pair + state-discriminated history/free prev/next + record area=136 + every-page kind；external 页仍使用 `UNDO_PAYLOAD=9` 与 `UEP1/v1` body header；普通 v1/v2/未知版本 fail-closed |
| `storage.undo` rseg header | `RollbackSegmentHeaderRepository`, `RollbackSegmentHeaderLayout`, `RollbackSegmentHeaderSnapshot`, `RollbackSegmentHeaderCapacity`, `RollbackSegmentHistoryBase`, `RollbackSegmentFreeListBase` | Implemented; production-wired by `StorageEngine` | 固定 **page3 v4**：history base/high-water + free head/tail/length + active slots + INSERT/UPDATE cache arrays；fresh/rebuild、slot/cache/free owner CAS、history append/remove、truncate/recovery 共用；active/cache/free owner 唯一，history 是 occupied UPDATE 关系而非额外 owner；expected base/owner/count/top fail-closed；v1-v3/未知版本拒绝；多 rseg 未实现 |
| `storage.undo` record | `UndoRecord`, codec/types, `SecondaryUndoMutation`, `LobVersionOwnership`, external payload descriptor | Partial | 三类行记录、secondary tail 与 LOB version ownership 均已编解码并由 rollback/purge 消费；Atomic DDL marker 已作为 `storage.api.ddl.DdlUndoMarker` 写独立 catalog DDL log，不混入普通 row undo/history。partial 仅指未来改聚簇键/更多专用 row 格式与 temporary undo 未实现 |
| `storage.undo` exceptions | `UndoPageOverflowException`, `UndoPayloadTooLargeException`, `UndoLogFormatException`, `UndoLogicalHeadConflictException` | Implemented | ordinary root 槽异常、external 配置上限、descriptor/page/link/owner/CRC/codec 损坏和 logical-head stale 均使用领域异常；页数上限在 reservation 前拒绝，物理发布后的事务编排不确定性由 trx fatal 异常表达 |
| `storage.api` undo adapter | `DiskSpaceUndoAllocator` | Implemented | Implements `UndoSpaceAllocator`; delegates to `DiskSpaceManager.createSegment(UNDO)`/`allocatePage` and maps exact planned reservation to `DiskSpaceManager.reserveSpace(UNDO)`；同时服务主 UNDO chain grow 与同 segment external payload 页分配 |

## Domain + Common Slice

### Package Status

| Package area | Representative classes | Current state | Notes |
| --- | --- | --- | --- |
| `domain` page locator | `PageId` `(SpaceId, PageNo)`, `SpaceId` `(int)`, `PageNo` `(long)`, `PageSize` `(int)` | Implemented | Immutable records; `PageId.offset(PageSize)` for positional IO; `PageSize.extentSizeBytes()`/`pagesPerExtent()`; consumed by api/buf/fil/fsp/flush/btree/redo |
| `domain` segment/extent | `SegmentId` `(long)`, `ExtentId` `(SpaceId, long)` | Implemented | `ExtentId.from(PageId, PageSize)`/`firstPageNo(PageSize)`; consumed by api (`DiskSpaceManager`/`DiskSpaceUndoAllocator`/`SegmentRef`), fsp |
| `domain` LSN | `Lsn` `(long)` | Implemented | Redo LSN; WAL/pageLSN/checkpoint boundary; consumed by buf/flush/mtr/redo/recovery |
| `domain` transaction | `TransactionId` `(long)` `NONE=0`, `TransactionNo` `(long)` `NONE=0` | Implemented | `TransactionId` = DB_TRX_ID writer id；`TransactionNo` = commit sequence，已由 prepareCommit、persistent history、purge eligibility 与 recovery high-water 消费；物理 history 顺序不要求 no 单调 |
| `domain` undo pointer | `RollPointer` `(boolean insert, PageNo, int offset)` 7B codec, `NULL`, `UndoNo` `(long)` `NONE=0`, `RollbackSegmentId` `(int)`, `UndoSlotId` `(int)` | Implemented | `RollPointer.insert` 同时参与 INSERT/UPDATE log kind 守门；聚簇 `DB_ROLL_PTR` 由 `planX/appendPlanned` 返回；rseg/slot 仅存在 `UndoLogBinding`，不进入指针编码；`UndoNo` 为 per-txn 全局序号，两个局部 log 共享高水位 |
| `common.exception` | `DatabaseRuntimeException` (root), `DatabaseFatalException` (fatal), `DatabaseValidationException` (validation) | Implemented | Project exception hierarchy root；模块异常统一沿 `DatabaseRuntimeException` 层次派生；当前 10 个 direct fatal 叶子覆盖 data/redo/recovery/undo publication 与 `LobPageCorruptedException`；`DatabaseValidationException` replaces `IllegalArgumentException` across all packages |
| `common.logging` | `ColoredLevelConverter` | Implemented | Logback ANSI color converter: ERROR red, WARN yellow (`:25-30`) |
| `common` | `package-info` | Implemented | Only package-info; clock/config/util interfaces not yet added |

## Data Dictionary + Physical DDL Slice

### Current Flow

```mermaid
flowchart TD
  Engine["DatabaseEngine"] --> Control["DictionaryControlStore mysql.dd.ctrl"]
  Engine --> Catalog["FileInternalCatalogStore mysql.ibd"]
  Catalog --> Repo["PersistentDictionaryRepository"]
  Repo -->|"constructs (:50)"| DdlLog["PersistentDdlLogRepository"]
  DdlLog -->|"append/read DDL_LOG(7) (:137/:145)"| Catalog
  Engine --> Discovery["DictionaryTablespaceDiscovery"]
  Discovery -->|ACTIVE / DROP_PENDING bindings| Storage["StorageEngine.open(recoveryTablespaces)"]
  Engine --> DdlRecovery["DictionaryDdlRecoveryService"]
  DdlRecovery -->|"unresolved/transition (:126+)"| DdlLog
  DdlRecovery --> Physical["TableDdlStorageService"]
  DdlRecovery --> Sdi["SerializedDictionaryInfoService / DictionarySdiCodec"]
  DdService["DataDictionaryService"] --> Mdl["MetadataLockManager"]
  DdService --> Cache["DictionaryObjectCache"]
  Cache --> Repo
  Ddl["DictionaryDdlService"] --> Mdl
  Ddl --> Repo
  Ddl -->|"prepare/transition (:241-363)"| DdlLog
  Ddl --> Barrier["storage.api TablePurgeBarrier"]
  Ddl --> Sdi
  Sdi -->|"opaque table/version/payload"| Physical
  Ddl --> Physical
  Physical --> SdiPage["storage.sdi page0 SDI_ROOT -> fixed page3"]
  Resolver["DictionaryIndexMetadataResolver"] --> Repo
  Resolver --> Mapper["DictionaryStorageMetadataMapper"]
  Mapper --> Factory["BTreeIndexMetadataFactory"]
  Storage -->|rollback / purge identity| Resolver
  Storage -->|persistent history table refs| Barrier
```

### Current Data Chains

| Flow | Current production chain | Current state |
| --- | --- | --- |
| Public bootstrap | `DatabaseEngine.open` -> open control/catalog并分别重建 DD snapshot 与 DDL log latest phases -> 以对象/space/version、最大 DDL marker及 version-1 保守下界校正 control -> DD discovery 返回稳定 API binding -> `StorageEngine.open` -> logged table/index DDL recovery -> legacy pending -> 逐张 ACTIVE 表 SDI reconcile -> orphan cleanup -> publish OPEN | Implemented；ACTIVE binding 缺文件、marker/DD identity 或 exact path 不一致、binding 越出 `tables/`、catalog/control/DDL history、未知 SDI root/物理 envelope 损坏均 fail-closed；`mysql.ibd` 是 append catalog sidecar，不伪装成 StorageEngine tablespace |
| Statement metadata lease | `DataDictionaryService.openTable(owner,name,TableAccessIntent,timeout)` -> schema MDL(SR) -> READ/table SR 或 WRITE/table SW -> cache single-flight load + pin -> `TableMetadataLease`；close 逆序 pin→table→schema | Implemented；访问意图无隐式默认值；DDL publish 后旧 pinned 版本可保持 stale，DROP_PENDING/DROPPED 不再由普通 lookup 打开 |
| CREATE TABLE | schema IX -> table X -> reserve object/space/ddl/version -> DDL log `PREPARED` -> `TableDdlStorageService.createTable` schema/LOB/redo admission + 单 MTR 创建 GENERAL/FSP、index segments/root、可选 LOB segment及空 SDI page3 -> redo durable -> `ENGINE_DONE` -> `SerializedDictionaryInfoService.write` 完整 table/binding snapshot durable -> repository ACTIVE commit -> `DICTIONARY_COMMITTED` -> cache publish -> `COMMITTED` | Implemented for file-per-table + exactly one clustered primary index；SDI 超出单页或持久化失败时不发布 ACTIVE，ENGINE_DONE orphan 由 recovery 删除；append outcome 不确定时停止当前 DDL且不猜补偿；startup 对无 DD 的 PREPARED/ENGINE_DONE 只删除 marker exact path并写 ROLLED_BACK，对匹配 ACTIVE 补齐提交；错绑或 ACTIVE 缺文件 fail-closed |
| DROP TABLE | schema IX/table X resolve ACTIVE/binding -> `TablePurgeBarrier.awaitUnreferenced` -> DDL log `PREPARED` -> cache version barrier + repository `DROP_PENDING` -> `DICTIONARY_COMMITTED` -> await pins -> physical DISCARDED/flush/invalidate/delete -> `ENGINE_DONE` -> repository `DROPPED` -> `COMMITTED` | Implemented；barrier timeout 发生在 marker/cache/catalog 修改前，表保持 ACTIVE；recovery 对 ACTIVE+PREPARED 写 ROLLED_BACK，对 DROP_PENDING 再次过 barrier并前滚，对 DROPPED 补 terminal；无 marker 的旧 pending/orphan 路径仍兼容 |
| CREATE secondary index | `CREATE [UNIQUE] INDEX` / `ALTER TABLE ... ADD [UNIQUE] INDEX` -> Session 先完成语法绑定和用户事务隐式提交 -> 独立 DDL owner 获取 schema IX/table X -> reserve index/ddl/version + `CREATE_INDEX/PREPARED` -> `beginSecondaryIndexBuild` durable stage descriptor -> 聚簇全量扫描并用逐行短 MTR backfill -> `ENGINE_DONE` -> SDI 新 aggregate durable -> DD exact add-one-index commit -> `DICTIONARY_COMMITTED` -> cache publish -> clear descriptor -> `COMMITTED` | Implemented blocking v1；ASC/DESC、unique NULL 语义、已有行回填及重启查询已接；全程 table X，不实现 online row log；重复 non-NULL unique key 在发布前回滚 staged segments。恢复在旧 DD 时只按 descriptor 回滚，在新 DD 精确包含 index/binding 时前滚并清 footer，descriptor 本身无 catalog 发布权 |
| DDL log / undo marker | `DictionaryDdlService` 构造稳定 `storage.api.ddl.DdlUndoMarker(ddlId, dictionaryVersion, tableId)`，CREATE_INDEX 另存 `secondaryObjectId=indexId` -> `PersistentDdlLogRepository` expected-phase CAS -> `DdlLogCatalogCodec` -> 与普通 DD 共用 `InternalCatalogStore` 的独立 `DDL_LOG(7)` 单记录 batch；恢复按 ddl id 读取非终态记录 | Implemented v2 for CREATE/DROP TABLE + CREATE INDEX，兼容读取 v1 table marker；key/payload 双 identity、stable code、path/UTF-8/尾随字节与 durable transition 均 fail-closed；marker 不写普通 row undo/rseg history，temporary undo 仍未接 |
| SDI v1 | `DictionarySdiCodec.encode(ACTIVE TableDefinition)` -> `SerializedDictionaryInfoService` -> `TableDdlStorageService.writeSerializedDictionaryInfo` -> SDI write MTR(page0 root→page3 body) -> `flushThrough` -> tablespace force；startup 对每张 committed ACTIVE 表执行 exact identity/version/payload compare | Implemented；GENERAL extent0 固定 page3、`PageType.SDI=4`、`SDI1/v1` header + CRC32C；逻辑 payload v2 显式保存独立 `rowFormatVersion` 且兼容读 v1。page3 尾部 96 bytes 可保存 CREATE_INDEX stage descriptor，普通 SDI rewrite 保留它；root=0/空/逻辑错配按 committed DD 重写，未知 root/物理损坏 fail-closed；不从 SDI/descriptor 反向发布 catalog |
| Index/LOB metadata mapping | binder scope pins exact `TableDefinition` -> gateway mapper 生成 `TableIndexMetadata`（clustered + 按 id 排序 secondary）/LOB binding；rollback/purge -> undo identity -> `UndoTargetMetadataResolver` -> same mapper/factory | Implemented；SQL table INSERT、primary/unique-secondary SELECT、multi-index rollback/purge 共用 exact-version layout/root/binding；旧 catalog empty binding 保留而不猜测能力 |

### Package Status

| Package area | Representative classes | Current state | Notes |
| --- | --- | --- | --- |
| `dd.domain/repo/tx` | immutable schema/table/index definitions, `DictionaryControlStore`, `PersistentDictionaryRepository`, `PersistentDdlLogRepository`, `DictionaryTransaction` | Implemented v1 catalog + DDL log v2 | control 双槽 CRC；catalog page-aligned append frame + batch manifest；binding tail 显式保存 row-format version并兼容旧 EOF；普通 DD 与 DDL_LOG 共享物理 store但各自忽略对方 batch；DDL log v2 增 index identity并兼容读 v1 |
| `dd.cache/mdl/service` | `DictionaryObjectCache`, `MetadataLockManager`, `DataDictionaryService`, `TableAccessIntent`, `TableMetadataLease` | Implemented | cache single-flight/pin/stale + DROP version barrier；显式 READ→table SR、WRITE→table SW，schema 恒 SR；MDL 六模式矩阵、FIFO、upgrade、timeout、deadlock wait graph；schema→table 锁序 |
| `dd.ddl/recovery` | `DictionaryDdlService`, `CreateSecondaryIndexCommand`, `DdlLogRecord/Operation/Phase`, `DictionaryTablespaceDiscovery`, `DictionaryDdlRecoveryService` | Production-wired blocking v1; feature-partial | CREATE/DROP TABLE 与 CREATE INDEX atomic DDL log、purge-aware barrier、stage rollback/finish、legacy lifecycle recovery 与 ACTIVE SDI reconcile 已接；DROP INDEX、其余 ALTER、binlog/online DDL 未接 |
| `dd.sdi` | `DictionarySdiCodec`, `SerializedDictionaryInfoService`, `DictionarySdiCorruptionException` | Implemented v1; production-wired | DD 拥有完整 table 聚合确定性编码；CREATE 在 ACTIVE publish 前写，recovery 只以 committed DD 比较/重写；不接 page/BufferFrame，也不把 SDI 反向发布进 repository |
| `storage.api.catalog` / `storage.fil.catalog` | `InternalCatalogStore`, catalog records/exceptions, `FileInternalCatalogStore` | Implemented v1 | 稳定 storage API 与 file backend；物理实现只依赖 common + storage API，不反向 import DD 异常或 repository |
| `storage.api.ddl` + `storage.sdi` | storage schema DTO/binding/mapper, `TableDdlStorageService`, `SecondaryIndexBuildDescriptor`, `DdlUndoMarker`, `SerializedDictionaryInfo`, `SdiPageRepository/Layout/Snapshot` | Implemented v1 + blocking index build | physical create/drop/index stage/backfill/rollback、redo durability、DISCARDED marker与固定 page3 SDI/footer；storage 只解释数值 identity/version/opaque payload 和页完整性，不反向依赖 DD；聚簇扫描先物化、逐 secondary insert 各用短 MTR |
| `engine` | `DatabaseEngine`, execution gate, DD metadata resolver/mapper, `MappedTableStorage`, `DefaultSqlStorageGateway`, `DefaultSqlDdlGateway` | Implemented public composition root | DD resolver 同时实现 index 与 table target；StorageEngine 的 table purge barrier 注入普通 DDL 和 DDL recovery；DML 与 DDL 各走稳定 outbound gateway，生命周期 gate/关闭顺序不变 |

## SQL Point Access + Secondary Prefix Range + CREATE INDEX Session Slice

### Current Flow

```mermaid
flowchart TD
  Engine["DatabaseEngine.openSession"] --> Registry["SessionRegistry"]
  Engine --> Session["DefaultSqlSession"]
  Engine --> Gate["EngineSessionExecutionGate"]
  Session -->|"enter / failClosed"| Gate
  Session --> Deadline["SqlStatementDeadline"]
  Session --> Parser["DefaultSqlParser"]
  Session --> Policy["SessionTransactionPolicy"]
  Session --> Binder["DefaultSqlBinder"]
  Session --> Executor["DefaultSqlExecutor"]
  Session --> DdlPort["SqlDdlGateway"]
  DdlPort --> DdlAdapter["engine.adapter.DefaultSqlDdlGateway"]
  DdlAdapter --> Ddl["DictionaryDdlService"]
  Policy --> Metadata["TransactionMetadataScope"]
  Deadline --> Metadata
  Metadata --> DD["DataDictionaryService.openTable READ/WRITE"]
  Executor --> Port["SqlStorageGateway"]
  Port --> Adapter["engine.adapter.DefaultSqlStorageGateway"]
  Deadline --> Adapter
  Adapter --> Mapper["DictionaryStorageMetadataMapper"]
  Adapter --> DML["TableDmlService INSERT + clustered terminal"]
  Adapter --> MVCC["ReadViewManager + primary/secondary MVCC"]
  Adapter --> LOB["LobStorage hydration"]
  Engine --> Close["Session snapshot/close before StorageEngine.close"]
  Close -->|"write quiescence"| Gate
```

### Current Data Chains

| Flow | Current production chain | Current state |
| --- | --- | --- |
| Session admission | `DatabaseEngine.openSession` under lifecycle lock -> `SessionRegistry.register` -> `DefaultSqlSession`；每次 `execute` 先 `EngineSessionExecutionGate.enter` 取 read permit、再取 Session operation lock | Implemented；只允许 Engine OPEN；close 在 snapshot 同一 lifecycle 临界区切 CLOSING，gate 复核拒绝既有 Session 的新语句；锁序固定 gate→Session，避免 shutdown write gate 反向等待 |
| Parse / bind | `DefaultSqlSession.execute` -> 单一 `SqlStatementDeadline` -> parser（DML/SELECT/CREATE INDEX/ALTER ADD INDEX）-> 普通语句走 transaction policy + metadata scope；index DDL 先生成无 DD pin 的 `BoundCreateIndex`，再执行 implicit-commit 边界 | Implemented v1；两种 index SQL 归一为同一 AST/command；locking SELECT 仍以 RW transaction + WRITE intent 绑定；DDL 的 schema/table/column 校验由独立 DDL owner 在 coordinator 的 table X 下完成，避免把用户事务 MDL 带入阻塞 build |
| INSERT / point writes | `DefaultSqlExecutor.execute` -> `DefaultSqlStorageGateway.insert/update/delete` -> exact DD `TableIndexMetadata`/LOB mapping -> `DmlStatementGuard` -> `TableDmlService` -> 聚簇 undo/row anchor + 全部 secondary 按 id 独立发布/标记 | Implemented；28 类型、复合 PK INSERT、完整主键 UPDATE typed patch/DELETE、external LOB ownership、logical unique/NULL 与多索引 statement rollback 均走生产链；不支持 range DML 或改主键 |
| Point SELECT | `DefaultSqlBinder.bindSelect` (`DefaultSqlBinder.java:78`) 完整主键优先，否则选择完整无 prefix 的最小 id logical-unique secondary -> `DefaultSqlExecutor.execute` (`DefaultSqlExecutor.java:35`) -> `DefaultSqlStorageGateway.selectPoint` (`DefaultSqlStorageGateway.java:136`) 创建 ReadView -> primary `MvccReader` 或 `SecondaryMvccReader.readUnique` (`SecondaryMvccReader.java:82`) 回表复核 -> view 内 LOB hydration/投影 -> RC finally close | Implemented；RR/RC、未提交版本、A→B/A→B→A、marked candidate 与唯一可见性损坏均覆盖；NULL equality 返回空；不泄露 storage reference |
| Non-unique secondary prefix range SELECT | point access 不匹配时，Binder 选择完整单列无 prefix 的最小 id non-unique secondary -> `BoundSecondaryRangeSelect` -> gateway `selectRange`；consistent 走 `SecondaryMvccReader.readRange` + ReadView，`FOR SHARE/UPDATE` 走 `SecondaryCurrentReadService` prefix S/X + clustered current-read -> 同一保护边界内 LOB hydrate/project | Implemented；一个 SQL equality 对应 physical `logical key + clustered suffix` 多行 range；autocommit locking SELECT 使用 RW handle 并在语句 commit 释放，显式事务持锁到 terminal；任意 `< <= > >=`、composite/prefix range 与 point locking 未接 |
| CREATE secondary index SQL | parser -> `CreateIndexStatementNode` -> `DefaultSqlBinder.bindDdl` -> `BoundCreateIndex` -> `SessionTransactionPolicy.prepareDdl` 隐式提交并释放用户事务 metadata -> `SqlDdlGateway` -> `DefaultSqlDdlGateway` -> `DictionaryDdlService.createSecondaryIndex`；若 autocommit=false，DDL 结束后重建空 implicit transaction | Implemented；支持 `CREATE [UNIQUE] INDEX ... ON ...` 与 `ALTER TABLE ... ADD [UNIQUE] INDEX ...`、复合 key ASC/DESC；DDL 不进入普通 Executor/storage gateway；语法/绑定错误发生在 implicit commit 前，storage/DD 失败遵循 atomic recovery，不把已结束用户事务复活 |
| Transaction terminal | `SessionTransactionPolicy` -> gateway `commit` (`DefaultSqlStorageGateway.java:192`) / `rollback` (`:222`) -> DML facade terminal + durability/row-lock release -> close transaction metadata scope | Implemented；storage 终态先于 MDL/pin 释放；outcome unknown 进入 FAILED；close rollback；rollback-only 只允许 rollback/close |
| Fatal propagation | storage fatal（即使位于 cause/suppressed）-> `DefaultSqlStorageGateway.adapt` / `DatabaseFailureClassifier` 保持 fatal -> `DefaultSqlSession` 进入 FAILED并 `SessionExecutionAdmission.failClosed` -> `DatabaseEngine` 发布 FAILED | Implemented；fatal 不再降级为可重试 `SqlStorageException`，组合根立即拒绝新 Session/statement；活动线程不递归 close，显式 close 再按 quiescence 顺序收敛 |
| Engine close | `DatabaseEngine.close` -> lifecycle 发布 CLOSING/snapshot -> `EngineSessionExecutionGate.awaitQuiescence` write permit -> `closeSessions` virtual-thread bounded convergence -> `StorageEngine.close` -> DD resources | Implemented；write permit 证明全部 execute 已退出并覆盖 Session rollback + resource close；timeout 保持 CLOSING 且不关闭 storage；失败用 suppressed 聚合 |

### Package Status

| Package area | Representative classes | Current state | Notes |
| --- | --- | --- | --- |
| `sql.parser` | lexer/token/source position/AST/`DefaultSqlParser` | Implemented v1 | 单语句 INSERT/UPDATE/DELETE、等值 SELECT、尾部 FOR SHARE/UPDATE、两种 CREATE INDEX grammar、BEGIN/COMMIT/ROLLBACK、SET autocommit；错误含位置；无 comparison range/其余 DDL/prepared grammar |
| `common.json` | `StrictJsonValidator` | Implemented and production-wired | binder 与 record LOB codec 共享 RFC 8259 严格文本校验；零对象树、保留原始文本/数字精度；拒绝宽松扩展并限制嵌套深度；不实现 MySQL binary JSON |
| `sql.binder` | `DefaultSqlBinder`, point/write/range bound plans, `BoundCreateIndex`, coercion/metadata scopes | Implemented v1 | DD 28 类型转换、严格 JSON、transaction metadata；point/range access path保持既有能力；index DDL 只规范化对象/key/order并输出 coordinator command，不选择物理 build 策略 |
| `sql.executor` + `.storage` | public values/results, `DefaultSqlExecutor`, `SqlStatementDeadline`, `SqlStorageGateway`, `SqlDdlGateway` | Implemented v1 | SQL/session 对 storage 零 import；DML/query 经普通 executor，CREATE INDEX 由 Session 在 implicit-commit 边界后走独立 DDL port；两路共享绝对 deadline |
| `session` | `DefaultSqlSession`, `SessionExecutionAdmission`, `SessionTransactionPolicy`, `SessionRegistry`, state/options/snapshot | Implemented in-process v1 | Engine permit→fair Session lock 固定顺序；autocommit 0/1、implicit/explicit transaction、DDL implicit commit/恢复 implicit transaction、rollback-only/FAILED/fatal fail-close/close；无 network connection/prepared statement/plan cache |
| `engine.adapter` | `DefaultSqlStorageGateway`, `DefaultSqlDdlGateway`, `EngineSqlTransactionHandle` | Implemented v1 | DML/query 映射与 DDL coordinator port 分离；CREATE INDEX 使用保留 DDL owner namespace，point/range/LOB/handle/deadline/fatal 语义不变 |

## Reserved / Unwired Production Types

> 以下类型存在于生产源码中，但尚未完全闭环、无直接生产调用，或只有部分能力接线。按 AGENTS.md 要求，每个必须写清现状、保留理由和下一步动作。

### api / lifecycle 无生产组合根

| Type | Current caller | Why it exists | Next action |
| --- | --- | --- | --- |
| `UndoTablespaceTruncationService` / `UndoTablespaceTruncationRecovery` | `StorageEngine.open(existing)` 构造 recovery participant；tests；主动 truncate 仍无 purge 调用方 | 可由未来 purge 调用的 crash-safe UNDO 物理收缩与启动续作 | 已在 engine recovery bootstrap 共享 controller/redo/flush/registry 并注入；purge 片接主动 truncate 调用方 |

### DD / DDL 保留能力

| Type | Current caller | Why it exists | Next action |
| --- | --- | --- | --- |
| `CatalogEntityKind.SCHEMA_TOMBSTONE/TABLE_TOMBSTONE` | None | 预留 append catalog 的对象级删除记录 | 需 catalog compaction/通用 delete mutation 时落 codec；当前 table 删除用 `DROPPED` lifecycle version |
| `DictionaryDdlFaultInjector` | 生产 `DictionaryDdlService` 固定注入 `NO_OP`；`DictionaryDdlServiceTest` 使用阶段 override | 只为确定性钉住 PREPARED/ENGINE_DONE/DD committed 等 durable crash window，不参与 DDL 裁决 | 保持 no-op 测试接缝；只有新增真实持久 phase 时扩展 default 方法，不作为业务 callback |

### buf + mtr 层部分预留类型

| Type | Current caller | Why it exists | Next action |
| --- | --- | --- | --- |
| `MtrMemo.push(AutoCloseable)` | None | Generic non-page resource push; all production code uses `pushPageGuard` | Use for non-page latch/fix reservations if needed |
| `RandomReadAheadDetector` + `ReadAheadService` random 路径（`maybeScheduleRandom`）| 生产代码已接（`ReadAheadService.maybeScheduleRandom`），但 `StorageEngine` 以 `RANDOM_READ_AHEAD_THRESHOLD=0` 构造 → 运行时检测器仅由测试实例化 | 0.10c：random read-ahead 机制完整 + 单测；生产**默认禁用**对齐 MySQL `innodb_random_read_ahead=OFF`（禁用时 `recordAccess` 不查 residentCountInRange、零额外开销）| 把 `RANDOM_READ_AHEAD_THRESHOLD` 升为 config flag 并设非 0 即激活；后续可加 IO budget（预取未访问淘汰计数）/ access-bit 启发式细化 |

### record 层 test-only 算子

| Type | Current caller | Why it exists | Next action |
| --- | --- | --- | --- |
| `RecordDecoder` | Tests only (3 test classes) | Standalone decoder; production decode path goes through `RecordFieldResolver` via `RecordCursor` | Wire into a read/scan path, or fold into `RecordFieldResolver` if redundant |
| `RecordPageDeleter` | `SplitCapableBTreeIndexService.deleteClustered` (StorageEngine service root + rollback tests) + tests | In-page delete-mark operator | rollback 已用；SQL v1 不含 DELETE，executor delete facade 仍待 |
| `RecordPagePurger` | clustered delete/purge 与 secondary rollback/purge physical remove 共用，经 `SplitCapableBTreeIndexService` 生产调用 | In-page physical unlink + directory/garbage fixup | 已生产接线到 multi-index rollback/purge；后续只需其它 page-compaction/admin 消费者 |
| `RecordPageReorganizer` | `SplitCapableBTreeIndexService` merge（`mergeLeaf`/`mergeInternal` 压实 survivor，0.12，StorageEngine service root + tests）+ tests | In-page dense rewrite + GarbageList reclaim + dir/n_owned rebuild | merge 已用；其余 page-compaction admin op 仍待 |
| `RecordPageUpdater` | `SplitCapableBTreeIndexService.replaceClustered` (StorageEngine service root + `RollbackService`) + tests | In-page update: in-place / move / reinsert-required | UPDATE 写 + rollback 恢复已用；SQL v1 不含 UPDATE，executor update facade 仍待；改聚簇 PK(REQUIRES_REINSERT) 抛 unsupported |
| `UpdateResult` / `UpdateOutcome` | `RecordPageUpdater` (via `replaceClustered`, T1.3e) + tests | Update result value objects | live；REQUIRES_REINSERT → `BTreeUnsupportedStructureException` |

### btree 层保留的 legacy / teaching 类型

| Type | Current caller | Why it exists | Next action |
| --- | --- | --- | --- |
| `BTreeIndexService` (interface) | Tests/legacy abstraction only; production SQL DML 已经由稳定 gateway 间接使用 `StorageEngine` 暴露的 concrete `SplitCapableBTreeIndexService` | Old BTree facade contract kept for teaching/regression while split-capable API grew more operations | 明确选择保留为教学接口，或在回归覆盖证明无独立价值后删除；不得为迁就旧接口削弱 concrete secondary API |
| `LeafOnlyBTreeIndexService` | Tests only | Root-level-only B1/B2 implementation retained as small reference path and regression target | Remove after split-capable tests fully cover the same cases, or keep explicitly as teaching fixture |
| `BTreeRootChangedException` | None（0.12 起 `openRoot` 不再抛）| 旧 root snapshot-lag guard；导航改按 root 页实际 level 后 level 相等断言去除（root shrink 使批量 rollback/purge 的快照合法陈旧）| 0.13/2.7 并发下以 latch coupling + 版本校验重定位时复用，否则移除 |

### redo 层持久化/恢复/容量路径

| Type | Current caller | Why it exists | Next action |
| --- | --- | --- | --- |
| `RedoLogWriter` / `RedoLogFlusher` / `RedoLogFileRepository` / 单文件/ring repository / `RedoLogBlockCodec` / `RedoLogBlockScanner` / `RedoBatchFrameCodec` / `RedoReclaimBoundary` | `StorageEngine` + tests | Durable redo write/flush/file IO（**文件环默认** + 单文件 opt-out）；0.20b 起共用 512B LogBlock v1；0.20c 起 operation budget 在 begin 校验单文件 physical fit；领域 workload 已按 B+Tree/Undo plan 接线 | Add richer capacity diagnostics |
| `RedoCheckpointStore` / `RedoCheckpointLabel` / `TransactionRecoveryCheckpointStore` / `TransactionRecoveryCheckpoint` | `StorageEngine` + tests | redo-control v2 与事务高水位 sidecar 都使用独立 4KiB 双槽 CRC；redo label 绑定 data format，checkpoint 严格 sidecar→label→reclaim | Add richer checkpoint diagnostics；旧非零 checkpoint 无 sidecar 明确要求重建 |
| `RedoRecoveryReader` / `RedoRecoveryScan` / dispatcher/handler types / `TransactionStateDeltaSink` | `StorageEngine.open(existing)` + tests | retained scan；page-local patch（含 B+Tree sibling/node/root）到 PageStore；trx handler 保序交付 recovery context；FORCE_SKIP 前置过滤 | Add tablespace discovery；trx recovery v1 已接 |
| `RedoCapacityPolicy` / `RedoCapacityPressure` / `RedoCapacityDecision` / `RedoCapacityThrottle` | `StorageEngine` + tests | Redo capacity pressure evaluation + foreground reservation throttle | Add config-driven thresholds / richer diagnostics if needed |

### flush 层部分未接线能力

| Type | Current caller | Why it exists | Next action |
| --- | --- | --- | --- |
| `FlushService` / `FlushCoordinator` / `CheckpointCoordinator` / `CheckpointMetadataParticipant` | `StorageEngine` composition root + `PageCleanerWorker` + tests | Flush/WAL/doublewrite executor + checkpoint barrier；metadata participant 在 redo label/reclaim 前 force 事务 counter baseline | Add configurable flush/backoff knobs and future checkpoint metadata participants |
| `flush.doublewrite.NoDoublewriteStrategy` | Tests only（生产已改用 recoverable） | OFF 模式占位 / 不想要 doublewrite 开销的定向测试 | 引入 `DoublewriteMode` 配置开关时作为 OFF 实现；否则保留为测试桩 |
| `flush.doublewrite.DetectOnlyDoublewriteStrategy` | Tests only（生产默认仍用 recoverable） | DETECT_ONLY 模式实现；写 metadata slot 并让恢复报告可疑页，不保存完整页副本 | 后续引入 `DoublewriteMode` engine 配置开关时接入生产策略选择 |
| `flush.policy.AdaptiveFlushPolicy` | `StorageEngine` + tests | Maps `RedoCapacityDecision` + dirty backlog -> `FlushAdvice` | production 已用 §7.4 proportional adaptive；0.6b 前台 throttle 已接；后续：config 化 factor/阈值、引入 redo 生成率/IO capacity/idle 输入 |

### trx 层生产持有但上层入口仍有限

| Type | Current caller | Why it exists | Next action |
| --- | --- | --- | --- |
| `TransactionManager` / `TransactionSystem` / transaction value/state types | `StorageEngine` production-held; gateway/session + DML/prepared + checkpoint/recovery | In-memory lifecycle + active table；PREPARED/重试态与 recovered aggregate 已接；opaque SQL handle 隔离上层；`snapshotCounters` 给 sidecar，`restoreCounters` 消费 recovery high water | server XID/SQL XA、RU/SERIALIZABLE 仍是后续 |
| undo/rollback/finalizer types | `StorageEngine` + table/clustered DML + SQL gateway + formal recovery | 双 log、secondary/LV ownership tail、statement/full/recovery multi-index inverse、逐记录 purge progress 与 batch finalization；SQL INSERT/UPDATE/DELETE rollback 已接 | Named SAVEPOINT、savepoint lock scope、多 worker/multi-rseg purge |
| `LockManager` / physical lock keys / `SecondaryLogicalKeyLockKey` / lock snapshot types / lock exceptions | `StorageEngine` production-held；SQL DML、BTree point/range、non-unique prefix locking SELECT、terminal releaseAll；tests | physical record/gap/next-key/insert-intention + normalized logical-prefix S/X 共用分片、wait-for/deadlock 与 observer；普通 SELECT 仍走 MVCC | 接 point/comparison-range locking SELECT、range UPDATE/DELETE 与 SERIALIZABLE；global comparator-aware gap 精确化另列后续 |
| `PurgeCoordinator` / `PurgeDriverWorker` / `HistoryList` / `HistoryTablePurgeBarrier` / purge safety/guard types | `StorageEngine` production-wired；recovery、DD DROP 与 DML 共用 | persistent history runtime projection、affected-table counters、version-safe secondary-first purge、real RESUME_PURGE 与 DROP barrier 已闭环 | 多 worker、多 rseg、blocked-head 调度优化与 purge→truncate 自动调度 |
| `TableDmlProgressFaultInjector` / `TableDmlProgressPhase` / `TableDmlSecondaryOperation` / `PurgeProgressFaultInjector` / `PurgeProgressPhase` | 生产 `TableDmlService`/`PurgeCoordinator` 固定调用 package-private `NO_OP` seam；同包故障测试临时安装 injector | 在不公开测试 API 的前提下钉住“短 MTR 已 durable、logical marker/history 尚未推进”等 crash 边界，验证重试幂等与恢复收敛 | 保持 package-private 与生产默认 no-op；只有新增真实持久化阶段时才扩展 phase，禁止作为业务回调使用 |

### recovery 层已由 Engine E2 接入

| Type | Current caller | Why it exists | Next action |
| --- | --- | --- | --- |
| recovery service/request/gate/progress + transaction recovery context/table/snapshot/reconciler/evidence + `PersistentHistoryRecovery` | `StorageEngine.open(existing)` + public `DatabaseEngine` + session integration tests | doublewrite→redo + trx sink→undo resume/reconcile→PREPARED外部决议→DD-resolved ACTIVE rollback/purge→force/open；随后 DD 层按独立 DDL log 收敛 CREATE/DROP并校验/修复 ACTIVE SDI，成功后才构造 Session registry；engine close 先 rollback Session | server XID/SQL XA、catalog-loss SDI discovery/rebuild、对象级 force recovery 与更丰富 Session gate diagnostics |

### undo 层底层类型生产入口

| Type | Current caller | Why it exists | Next action |
| --- | --- | --- | --- |
| `DiskSpaceUndoAllocator` | `UndoLogManager` / `UndoLogSegment` main grow + external payload path + tests | Adapter implementing `UndoSpaceAllocator` port; bridges undo -> `DiskSpaceManager` including exact UNDO reservation | 已被 `UndoLogManager` 经端口调用；表级及低层 DML 的 plan/append 路径在页写前精确预留 root grow + payload 总页数 |
| `UndoLogSegmentAccess` / `UndoLogSegment` / `UndoRecordWritePlan` / `UndoStoredRecordResolver` / `UndoPayloadStorage` / `UndoLogicalHead` / history/free snapshot / page/record/codec/handle/allocation ports | `UndoLogManager` + `RollbackService` + `MvccReader` + recovery/purge production paths + tests | Multi-page root + external payload、v3 every-page kind/state-discriminated history/free links、cache/free reset/activation、persistent local head/history/free CAS、统一 resolver | 已生产接线；后续只需多 rseg/undo space、多页 shrink 与未来格式迁移方案 |
| `UndoLog` | `UndoLogStoreTest` only | 早期单页 undo predecessor，保留用于教学对照和单页 codec 测试；生产已由 `UndoLogSegment` 取代 | 若教学对照价值消失则删除；不得重新接入生产 rollback/recovery/purge |
| `UndoRecordType.DELETE_MARK` | `TableDmlService.delete` -> clustered anchor + `DELETE_MARK_ENTRY` secondary tail；rollback/MVCC/purge/recovery 消费 | 行删除逻辑证据 | 已实现：secondary 与 clustered 都先标记，purge 安全边界满足后按 secondary-first 物理移除 |
| `UndoLogKind.TEMPORARY` | None (reserved enum constant) | Temporary-object undo kind | 普通 undo create/binding/recovery 明确拒绝；待临时表模块接独立临时 undo tablespace |

## Known Implementation Gaps

### 全局架构缺口

| Gap | Current consequence | Preferred resolution |
| --- | --- | --- |
| 公共 DD+storage+SQL Session 组合根已接 | `DatabaseEngine` 先打开 control/catalog并重建 DD+DDL log，从 DD 发现 recovery spaces并注入表级 resolver；storage PREPARED决议、ACTIVE rollback/RESUME_PURGE、table/index atomic DDL recovery 与 ACTIVE SDI reconcile 全成功后才开放 Session | SQL v1 含表级 DML、point/prefix read 与 blocking CREATE/ALTER ADD INDEX；仍缺 comparison/composite range、DROP INDEX/其余 ALTER、online DDL 与 server XID/SQL XA |
| 后台 redo flush + recent tracker/closedLsn + DurabilityPolicy 已接；Session commit 已消费 | `SessionTransactionPolicy` 把 durability mode 经 gateway 传入 `ClusteredDmlService.commit`，在 `UndoLogManager.onCommit` 后按 FLUSH/WRITE/BACKGROUND 等待；`MiniTransaction.commit`/`TransactionManager.commit` 本身仍不携带上层 durability 语义 | 后续协议层只映射 Session option，不绕过该 gateway/DML 终态链 |
| public recovery 已从 DD 发现表空间，低层入口仍接受显式列表 | `DatabaseEngine` 用字典 binding 生成 `EngineConfig.recoveryTablespaces`；`StorageEngine` 保留显式列表作为稳定低层 API。doublewrite 页仍来自 doublewrite repository 枚举并过滤到已打开空间，不是全 data-dir checksum scan | 保持该分层；如需离线损坏扫描，单独实现全空间 checksum discovery，不让 fil 反向读 DD |
| Recovery/closing gate 已覆盖 Session admission 与 DML | `DatabaseEngine` 仅在 recovery + DDL recovery 后发布 OPEN；`openSession` 要求 OPEN，CLOSING 先拒绝新流量；普通 storage accessor 和 DML facade 仍要求 recovery gate OPEN（含拒绝 READ_ONLY diagnostic） | 补 worker resume 结果、恢复锁/等待快照和更丰富 Session diagnostics |

### Disk Manager 缺口

| Gap | Current consequence | Preferred resolution |
| --- | --- | --- |
| Global `architecture.mmd` shows `PageStore --> TablespaceRegistry` | Misleading if read as current wiring | Treat global graph as target architecture; current implementation uses `PageStore` registry-free |
| SpaceReservation 三类生产消费者已接 | B+Tree split/root split 消费 NORMAL、Undo grow 消费 UNDO、0.21h `LobStorage.write` 按精确 chain page count 消费 BLOB；reservation 仍按页/extent 保底而非动态 reserveFactor | 后续按 IO capacity/extent locality 引入动态 reserveFactor 与观测，不再需要新增占位消费者 |
| DROP lifecycle + purge-history barrier 已接，独立 DISCARD 语义未接 | DD 在 DROP_PENDING 前等待 table history 引用归零；physical DROP 继续持 X lease、durable DISCARDED、flush/invalidate/close/delete，启动续作再次过 barrier | 后续 DISCARD/IMPORT 复用 marker，但需独立字典状态与 file identity/import 验证 |
| Registry type for existing 4-arg create defaults to GENERAL | Existing undo test harnesses that call 4-arg `createTablespace` register as GENERAL even when segment purpose is UNDO | Switch undo harnesses or undo allocator setup to typed `TablespaceType.UNDO` when that semantic distinction becomes required |
| 平台 native preallocation adapter 尚未接 | `DataFileGateway` seam 已接入 `DataFileHandle.create/extend/ensureCapacity`，默认 `ZeroFillDataFileGateway` 保持原零填充语义；当前 `PreallocationStrategy` 仅有 no-op/测试替身，没有 `posix_fallocate` 或 Windows 平台适配 | 后续按平台能力补 native preallocation strategy；仍保持 PageStore registry-free 和现有 Lifecycle/FileSize/Fsync 锁顺序 |

### Buffer Pool + MTR 缺口

| Gap | Current consequence | Preferred resolution |
| --- | --- | --- |
| miss 读盘已移出 Buffer Pool 内部锁；多 instance 分片已落（0.10d）；13.1a/13.1b-pre/13.1c/13.1d 锁边界已落；legacy `BufferPool.flush/flushAll` 已移除 | per-frame LOADING 占位 + `PageLoadFuture` 有界等待：不同页 miss 并发读、同页只读一次、读失败清占位、命中 LOADING 超时/中断不悬挂；脏 victim 刷盘亦在锁外。0.10d：facade 路由 + 每分片实例。13.1d：`PageHashTable` 由 `pageHashLock` 保护，frame 元数据由 `frameMutex` 保护，free/LRU/flush list 分别由专用短锁保护，dirty view 由 `DirtyPageList` 提供真实 flush list 快照；page read、PageLoadFuture wait、dirty victim flush 前均断言无内部锁。2026-07-05：`BufferPool` public API 不再提供 direct-write flush；无 flusher 的脏 victim 淘汰显式失败。**剩余**：读写并发期更细的 IO_FIX/DIRTY_PENDING/EVICTING/STALE 态 | 按需细分 IO 状态 |
| MTR rollback 不撤销 buffer 内容 | `rollbackUncommitted` 只 `releaseAll`，会释放 page guard / tablespace lease / space reservation 等 memo 资源，但脏页保持脏；已写页的事务级回滚依赖 undo/rollback/recovery 路径 | 依赖事务 undo 或后续更细 content undo 设计 |
| FSP/Undo/B+Tree/Trx logical/page-local redo 已替代或补充物理 bytes | MTR 纪律、FSP/undo/trx delta、B+Tree sibling/internal node/root after-image、LogBlock 与 operation budget 均已接；同值物理 bytes 精确过滤 | page0 信封、lifecycle marker、leaf/record row bytes 等未覆盖内容仍走 `PAGE_BYTES` |
| 脏页淘汰 WAL gate 已闭合（生产侧）| 注入 `DirtyVictimFlusher` 后脏 victim 经 WAL gate+checksum+doublewrite 刷盘；未注入 flusher 的独立池遇到脏 victim 直接失败，不再直写 PageStore | recoverable doublewrite（0.2）+ 后台 redo flusher（0.1）已接；淘汰现获真 torn-page 防护且无 legacy direct-write fallback |
| 替换策略 = midpoint LRU（Phase A 0.8 已落） | `MidpointLruReplacementPolicy`：old/new 双子链 + `oldBlocksTime` 提升窗 + `youngDistanceThreshold` 抗抖动，已抗一次性大扫描污染（`largeScanDoesNotEvictHotWorkingSet` 验证）；仍缺 read-ahead-aware 访问型分类、`oldBlocksPct` 容量配比再平衡 | 补访问型分类（预取页/扫描页区别对待）与配比再平衡 |
| 0.10 全落：read-ahead（linear+warmup+random）+ 多 instance 分片 + 专用 PageHashTable | 0.10a linear + 0.10b warmup + 0.10c random read-ahead（默认禁用）+ **0.10d 多 instance（facade+`BufferPoolRouter`+per-shard `BufferPoolInstance`/`PageHashTable`，默认 N=1）** 已落并生产接线（`StorageEngine` 经 `EngineConfig.bufferPoolInstanceCount`）；简化为单顺序流、random extent 驻留数启发式、warmup 同步预取；0.22 已补 `TablespaceVersion` / `SpaceLifecycleClock` stale-frame 版本复核；13.1d 已补 pageHash/frame/list 子锁和真实 flush list | random 生产 config 开关；warmup IO 速率控制 |
| DROP 已消费 stale-frame 语义，DISCARD/IMPORT 未接 | physical DROP 在 durable marker 后调用 `invalidateTablespace`，复用 `SpaceLifecycleClock` 的准入/版本复核并要求 dirty/fixed frame drain；文件删除后不可复活旧 frame | DISCARD/IMPORT 需新的 DD lifecycle 和 file identity 验证；继续保持 buffer pool 不理解字典对象 |

### Record 缺口

| Gap | Current consequence | Preferred resolution |
| --- | --- | --- |
| 28 类型 SQL binder 已接，物理简化仍保留 | `SqlTypeCoercion` 已补 TIME/YEAR 范围、Session 时区 TIMESTAMP、ENUM/SET 名称、BIT declared-width canonical；JSON 仍是严格 UTF-8 text，非 MySQL binary JSON | 未来 binary JSON/partial-update 必须追加明确格式版本，不得改变既有 Record stable encoding |
| charset/collation v1 已接（0.21c/0.21g） | UTF8/LATIN1 + BINARY/charset-specific ASCII_CI + UTF8 Unicode weight v1 已生产接线；Unicode v1 固定 case/accent 子集与 code-point fallback，不含完整 UCA normalization/locale tailoring | 新增 weight 行为必须追加 stable collation id；不能静默改变已建索引顺序 |
| INSERT/UPDATE/DELETE external LOB ownership 已接 | DDL table binding 提供 authoritative LOB segment；INSERT rollback 释放新链；UPDATE replacement 区分 rollback-new/purge-old；DELETE 交旧链给 purge；point SELECT hydrate 完整值，旧 catalog empty binding 的大值 fail-closed | 后续 binary JSON/partial LOB update 需新格式与 ownership 协议；不得从引用猜 segment 或给旧表偷建 segment |
| Delete/Purge/Reorganize/Update 算子已被多索引 SQL DML/rollback/purge 消费 | SQL INSERT/主键点 UPDATE/DELETE 同步维护全部 secondary，并为 non-NULL old/new logical prefix 取 X 以协调 locking read；statement/full/recovery rollback 和 secondary-first/LOB-aware purge 已接 | range UPDATE/DELETE 与 admin compaction 仍待 |
| `DB_ROLL_PTR` 版本链已被 primary/secondary MVCC 与 purge safety 消费 | `MvccReader` 构造可见旧版本；`SecondaryMvccReader.readUnique/readRange` 回表复核；`SecondaryPurgeSafetyChecker` 到 target 前判断较新 live identity | comparison/composite range、`PAGE_MAX_TRX_ID` 与更强 object-level corruption diagnostics 仍待 |
| 索引页头缺 `PAGE_MAX_TRX_ID` / `PAGE_BTR_SEG_LEAF` / `PAGE_BTR_TOP` | MVCC 可见性 / B+Tree segment 指针不支持 | 在 MVCC / B+Tree segment 切片补充 |
| prefix index 比较与 secondary 紧凑物化已接 | leaf/node-pointer/unique-lock/layout 共用 codec/collation；`SecondaryIndexLayout` 按 byte prefix 截断且回退完整 UTF-8 code point，补齐缺失聚簇主键后缀。**简化**：prefix 仍按字节而非 MySQL 字符数 | 完整 UCA/更多 charset 另用新 stable id；SQL unique-secondary point access 当前只选择无 prefix 索引 |

### B+Tree 缺口

| Gap | Current consequence | Preferred resolution |
| --- | --- | --- |
| 0.13d 主体已完成，仍缺 B-link/OLC 通用版本重定位 | **写：全部聚簇写算子乐观 S-crab 下降 + 悲观回退（0.13a/0.13b）**；**读：`lookup`/`scan`/current-read 定位经 `descendSharedCrab` 全 S hand-over-hand 下降 + scan sibling hand-over-hand（0.13c）**；**0.13d 已接 SX latch、safe-node 早释放祖先、root SX 下降 + restart-in-X**。当前仍无 B-link/OLC 版本页重定位，2.7a/2.7b current-read 只有授锁后重定位 | B-link/OLC 版本重启长期 deferred；除非并发性能目标明确，否则优先做 DD/session/redo/MVCC 辅助字段 |
| B+Tree sibling/internal node/root page-local redo 已接 | 结构动作完成后 snapshot header/used heap/directory；root leaf 只 snapshot identity；固定 kind 布局校验，恢复只 patch；同值 physical bytes 过滤 | leaf row semantic redo、PAGE_MAX_TRX_ID/segment header 未做；当前物理 redo仍安全覆盖 |
| B+Tree current-read 已接 point/physical-unique/range；secondary logical-prefix current-read 已接 SQL | 聚簇唯一继续用 record/gap/insert-intention；SQL point UPDATE/DELETE 消费 FOR_UPDATE；non-unique prefix SELECT 与 DML 共用 normalized S/X，随后锁 clustered current row；普通 SELECT 走 primary/secondary MVCC | 接 point/comparison-range locking SELECT 与 range UPDATE/DELETE；global gap ref/cursor 化仍待 |
| Secondary logical unique v1 已接但采用保守 marked 语义 | 其它主键的 delete-marked candidate 仍判 duplicate；同一主键 marked 可 revive，NULL parts 不冲突。它不沿候选 undo 判断“删除是否对当前事务可见” | 若需更贴近 InnoDB，可在保持 logical-key 锁的前提下引入事务可见性判定与等待策略 |
| `nonLeafSegment` 已用于分配（0.11） | 内部页/root-split 子页经 `index.nonLeafSegment()` 分配（`requireNonLeafSegment` 校验）；root 仍页号稳定原地重建 | — |

### Redo 缺口

| Gap | Current consequence | Preferred resolution |
| --- | --- | --- |
| 0.19 page-local/logical redo + trx recovery、LogBlock、operation budget 已闭合 | FSP/undo/B+Tree sibling+node+root 可独立 patch；trx delta 合并 baseline/page3；固定 block 与预算保护输入 | 按领域 plan snapshot 收紧 profile；按需 leaf row semantic redo |
| Session commit 已消费 storage durability policy | Session durability option -> gateway commit request -> `ClusteredDmlService.commit` -> FLUSH/WRITE/BACKGROUND；无写事务由 gateway 直接 `TransactionManager.commit` | network/protocol 层后续复用 Session option；底层 MTR/TransactionManager 继续不携带上层提交策略 |
| redo 环满仍保留最终 fail-closed，领域 workload 仍是安全上界 | 默认 ring 已用 LogBlock v1 跨文件扫描；0.20c 在 MTR begin 前聚合 operation logical budget、校验 physical file-fit；B+Tree height、DML 首写、rollback undo 类型、undo fragment/extent plan 已接；commit 验证 actual，timeout、低估或无可回收区间均 fail-closed | 按需补更细 capacity diagnostics 与实际/预算偏差观测 |
| 生产默认 handler 已覆盖现有 record，public engine 已从 DD 发现 replay 目标空间 | page handler 通吃 B+Tree sibling/node/root及 SDI page0/page3 的 PAGE_INIT/PAGE_BYTES，trx handler 使用 recovery sink；DDL log 使用 catalog 原子批次而非 redo semantic handler；恢复均不重跑业务状态机 | SDI 仍只做物理 replay，逻辑 compare/rewrite 固定在 redo 后的 DD recovery；后续不得在 redo handler 执行 DDL SQL |

### Flush 缺口

| Gap | Current consequence | Preferred resolution |
| --- | --- | --- |
| doublewrite 生产仍为单文件逐页 dispatch | 0.5 已把 append-only 改成固定 slot 复用，并提供仓储级 `DoublewriteBatch` 连续 slot 原语；但 flush list 与 LRU/single page 仍共用一个文件，`FlushCoordinator/FlushService` 生产链仍逐页调用策略 | 添加 FlushList/LRU 双文件与生产 batch dispatch 策略 |
| `DoublewriteMode.DETECT_ONLY` 尚未成为 engine 配置开关 | mode、strategy、metadata slot 与恢复报告已实现并测试；`StorageEngine` 生产默认仍固定 recoverable full-copy | 后续在 engine/session 配置层接入 `DoublewriteMode` 策略选择 |
| `PageCleanerSupervisor` 策略仍固定 | 已由 `PageCleanerSupervisor` 监控并有限重启；超过上限后 supervisor 进入 FAILED 并拒绝 request | 后续可把重启次数、backoff、监控间隔配置化并接诊断输出 |

### Transaction 缺口

| Gap | Current consequence | Preferred resolution |
| --- | --- | --- |
| MVCC ReadView + primary/secondary 一致性读已接 | `MvccReader` 保持 delete/ownership/短 MTR 语义；`SecondaryMvccReader` 的 unique point/non-unique range 都先物化含 marked 候选，再回聚簇并用可见完整行复核 logical key，ReadView 覆盖 LOB hydration | RU/SERIALIZABLE、comparison/composite range 与长期 cursor 留后续 |
| LockManager 已接 SQL DML、聚簇 current-read、secondary logical prefix、locking SELECT、DML 释放与诊断 | logical-prefix S/X 与 record/gap 锁同属事务，commit/rollback/close 才 `releaseAll`；purge/DML 行 guard 是独立短物理协调，不进入事务 wait-for graph | 接 point/comparison-range locking、range DML、SERIALIZABLE 与更完整 Performance Schema 语义 |
| rollback 消费 multi-index/LV ownership undo 与 PREPARED owner 已接 | live/statement/recovery/prepared 共用 per-record progress；secondary inverse→clustered inverse→LOB free+marker 顺序，EMPTY 后按 ACTIVE/PREPARED 独立 atomic finalization/terminal | storage crash 边界已闭合；剩余 server XID/SQL XA |
| SQL point writes 自动 multi-index statement guard 已接，命名 SAVEPOINT 未接 | INSERT/UPDATE/DELETE 的 secondary/LOB 任一失败均回到 statement head；autocommit/显式事务终态复用 resolved rollback | 命名 SAVEPOINT 与 savepoint 后 row-lock 精细释放 |
| SQL gateway 已接表级 INSERT、主键点 UPDATE/DELETE、primary/unique point SELECT 与 non-unique prefix range/locking SELECT | DD exact-version metadata、opaque transaction、typed patch、RR/RC、logical/clustered lock、LOB replacement/hydration 均生产接线 | comparison/composite range、point locking、range DML、optimizer/prepared statement 未接 |
| storage PREPARED participant 与 recovery 决议已接，server XA 未接 | `storage.api.trx` 提供强持久 phase one/two；recovery table/page3/first-page 识别 PREPARED并由 provider裁决，默认 UNRESOLVED fail-closed；不保存 XID、不解析 XA SQL | 后续在 server/session 层实现持久 XID registry、XA SQL/RECOVER 与 coordinator 生命周期 |
| 正式 trx recovery table v1 已接，尚无逐事务持久系统页 | sidecar 保存 counter baseline，redo 保存 terminal evidence，page3 保存仍占用 undo 的事务；三者交叉校验后恢复 counter/recovered-active/history | 若未来需要 XA/长 prepared，再设计持久 transaction system page/完整事务明细，不在 v1 扩张 |

### Recovery 缺口

| Gap | Current consequence | Preferred resolution |
| --- | --- | --- |
| atomic DDL_RECOVERY/discovery/PREPARED decision/multi-index rollback+purge+DROP barrier+SDI reconcile 已接 | public engine 恢复 ACTIVE/DROP_PENDING space，先决议 transaction PREPARED、rollback ACTIVE、重建/推进 history，再以 DDL marker + committed DD 裁决 CREATE/DROP TABLE 与 CREATE INDEX并逐张修复 ACTIVE SDI；index stage footer 只提供精确 rollback/finish identity，不拥有发布权；非法 phase/identity/path/root或未决事务 fail-closed | 补 server XID/SQL XA、catalog-loss SDI discovery/rebuild、object-level force recovery，以及 DROP INDEX/其余 ALTER 的 operation-specific phase |
| `RecoveryMode.FORCE_SKIP_CORRUPT_TABLESPACE` 仍只支持显式 SpaceId 集合 | DD 发现已接，但 public engine 尚未把 skipped space 发布为字典对象不可用状态；`CrashRecoveryService` 仍按 per-space 过滤 doublewrite/redo/reconcile，且系统 undo 不可 skip | 补 object-level unavailable lifecycle、DDL recovery 与多索引 transaction undo skip 语义 |
| DD tablespace discovery 已接，doublewrite 仍不做全 data-dir checksum discovery | public engine 用 catalog binding 填充 recovery spaces；doublewrite repair 仍从 `dwRepo.pageIds()` 枚举候选并过滤到已打开空间，不主动扫描所有数据页 | 若需离线全库校验，新建明确的 scrubber，不把高成本扫描混入每次 startup |
| Recovery control-plane/observability 不完整 | gate/status/report/progress events 已可诊断；Session 只在 public engine OPEN 后创建，CLOSING 拒绝新请求，active Session 先 rollback；仍无恢复后 worker resume 诊断、恢复期锁/等待快照 | 补 worker resume 结果、恢复锁快照与 Session/statement 关联诊断 |

### Undo 缺口

| Gap | Current consequence | Preferred resolution |
| --- | --- | --- |
| 双链 + secondary/LV tail rollback 消费者与 SQL point-write lifecycle 已接 | full/recovery 每步选择较大 head undoNo；每条记录 exact-version 解析全部索引与 LOB binding并按 secondary→clustered→LOB free+marker 收敛；Session statement/full/close rollback 复用 | 命名 SAVEPOINT、savepoint row-lock scope仍缺 |
| page3 v4 owner/history/free 目录、恢复分组与多段 atomic finalization 已接 | mixed transaction 占两个 active slots；终结同批处理 owner、history/free base 与全部首页；recovery 分开校验 active/cache/free 证据，再严格遍历 committed UPDATE history 与 free FIFO；truncate drain/rebuild 覆盖 page3 且非空 history 拒绝 | 多页 free shrink、多 rseg/tablespace 选择另列后续 |
| Secondary/LOB purge 协调已接 | undo tail 重建 exact physical key；UPDATE 用聚簇版本链证明 REMOVE/RETAIN，DELETE secondary-first；每条 logical record 用 progress MTR 原子消费 purge-old LOB 与推进 head，EMPTY 才 finalization | 多 worker、跨 rseg blocked-head 调度与更细 purge diagnostics 仍待 |
| 多表/多索引/LOB/prepared rollback 定位与 inverse 已接 | `UndoTargetMetadataResolver` 返回表级 metadata 与 authoritative LOB binding；resolved prepared facade/recovery 不暴露物理索引，legacy 单索引构造只供低层测试 | 改聚簇 PK 仍需新的 ownership 迁移语义；完整 XA 还需 server XID registry |
| 单线程 table-aware purge + 后台 driver + real recovery resume 已接 | task 失败保留 COMMITTED owner/history/table barrier；task commit 后 crash 由状态/ABSENT 幂等；finalization 后才摘 history并唤醒 DROP | 多 worker / blocked-head 调度优化需多 rseg；purge→truncate 自动调度未接 |
| Undo page v3 与旧教学文件不兼容 | v3 record area=136，first/chain 每页持久 kind，first page 增 history prev/next；v1/v2/未知版本在任意普通 UNDO page open 时 fail-closed | 当前测试/教学数据重建；未来若保留用户数据需单独迁移，不在 open 路径猜测布局 |
| truncate 机制已实现但无 purge→truncate 调度 | `UndoTablespaceTruncationService` 可恢复收缩并由 recovery 续作；后台 purge driver 已接（0.4）但**未驱动 undo tablespace truncate**（purge 只 dropUndoSegment 回收段页，不判 tablespace 死亡触发物理收缩）；活动 inode 会拒绝 | purge→undo tablespace truncate 调度（判 undo 死亡 → truncate）留后续片 |
| 版本链已被 primary/secondary MVCC 与 secondary purge safety 消费 | UPDATE/DELETE 全量 old image + ownership 校验；secondary unique point 与 non-unique logical-prefix range 都先物化 marked candidate 再回表，purge checker 在 target 前检测较新 live identity；链脱节/环/上限均 fail-closed | 任意比较/composite range 与更复杂 purge 调度仍待 |
| cache + persistent free undo segment reuse 已接 | INSERT/UPDATE cache 各自固定容量 LIFO；cache 满/0/忙时，合格单页段进入无配置上限的持久 FIFO 并可跨 kind 复用；page3 v4 是 owner/history/free 恢复权威，重启校验首页/链/FSP 后一次恢复统一目录；truncate 非阻塞 drain 并重建 page3 | 多页 segment shrink、多 rseg/tablespace 需要分区 reuse directory 与选择策略；不在当前 `RollPointer` 中猜测 space/rseg |
| Extern undo record payload 已接；仍无改聚簇 PK | 1.6 起过大完整 `UndoRecordCodec` 字节由普通 root 槽中的 `0x7F/v1` descriptor 指向同 segment `UNDO_PAYLOAD=9` 页链；统一 resolver 校验 link/owner/identity/count/length/CRC，MVCC/rollback/purge/recovery 无旁路；DML begin 前冻结计划并精确 admission。Record LOB reference 与该链仍是两套 ownership | 改聚簇 PK update 仍需 delete+insert、多索引与锁语义；不要把业务 LOB chain 和 undo payload chain 合并 |
| 单 writer 假设 | `UndoLogSegment` 假设同一事务/同 kind 单 EXCLUSIVE append 会话 | 实现并发 multi-writer 锁序 / rseg slot 选择 |
| 单 undo 表空间假设 | `RollPointer` 只编 pageNo+offset；T1.3c 固定单一默认 rseg，rseg/slot 存 `UndoContext` + 内存目录不进指针 | 扩展多 rseg/多 undo 表空间编码 |
| recovery 已接 DD discovery / table target resolver / transaction PREPARED provider / atomic DDL log / purge-aware DROP / ACTIVE SDI reconcile | public engine恢复多表空间、多索引 ACTIVE rollback/PREPARED决议、affected-table history、real RESUME_PURGE，再按 DDL marker 收敛 CREATE/DROP TABLE 与 CREATE INDEX并以 committed DD 修复 page3；非法组合/root/未决事务均 fail-closed | 接 server XID/SQL XA、catalog-loss SDI discovery/rebuild、object-level force recovery 与 DROP INDEX |

## 2026-07-18 Blocking CREATE Secondary Index v1 5-Pass Review Log

本轮以 [mysql-create-secondary-index-v1-design.md](mysql-create-secondary-index-v1-design.md) 为切片设计依据，
最终结论逐项回到生产源码、持久格式、崩溃测试、SQL 重启测试和固定工具链报告核对；implementation plan
只保留在 agent todo，未新增持久 plan，未使用 GitNexus。

| Pass | Result | Evidence |
| --- | --- | --- |
| 1. 生产调用链与分层 | PASS | 从源码重核 `DefaultSqlSession -> CreateIndexStatementNode -> BoundCreateIndex -> SqlDdlGateway -> DictionaryDdlService -> TableDdlStorageService -> SplitCapableBTreeIndexService`，以及 `DatabaseEngine.open -> DictionaryDdlRecoveryService`；SQL/session 不 import storage internal，storage 不反向 import SQL/session/engine/DD；`DefaultSqlExecutor` 明确拒绝绕过 Session DDL implicit-commit 边界 |
| 2. 持久格式与兼容 | PASS | DDL log v2 在 key/payload 双份保存 index id并兼容 v1 table marker；DD catalog 与 DDS payload 显式持久 `rowFormatVersion`，旧 EOF/v1 payload 按旧 table version 推导；page3 的 96-byte descriptor 有 identity/root/双 segment/CRC，普通 SDI rewrite 保留 footer；未知、截断、尾随与 identity mismatch 均 fail-closed |
| 3. 并发、资源与发布 | PASS | v1 全程持有 schema IX/table X，不假装 online DDL；Session 先完成 syntax bind 再隐式提交用户事务，DDL 使用独立 owner；聚簇扫描在短 MTR 中物化并释放 latch/fix，secondary 每行独立短 MTR，durable wait 有正 timeout；DD 只允许 ACTIVE aggregate 精确追加一个 secondary，row format version、旧 index/binding 与 LOB 均不可变 |
| 4. TDD 与 crash recovery | PASS | RED->GREEN 覆盖两种 grammar、binder、implicit commit、已有行 backfill、unique duplicate/NULL/ASC-DESC、footer 跨 SDI rewrite、normal publish、engine restart query；ENGINE_DONE+旧 DD 回滚 exact staged segments，DD committed+新 aggregate 前滚并清 footer，descriptor 单独存在绝不发布 catalog |
| 5. 地图、静态与全量 | PASS | current map 10 项检查清单复核：新增实线均有生产调用、状态/缺口准确、无新增 Reserved/Unwired 类型遗漏；backlog 与 DD 厚设计已同步，明确 online row log、DROP INDEX/其余 ALTER、catalog-loss rebuild 仍缺；相关源码无 executable monitor、裸 `IllegalArgumentException/RuntimeException`、TODO/TBD 或越层 import；`git diff --check` 仅 CRLF 提示；固定 JDK 25.0.2 + Gradle 9.5.1 `test --rerun-tasks --no-daemon`：277 suites / **1539 tests**，0 failure/error/skip |

## 2026-07-17 SDI v1 5-Pass Review Log

本轮以 [mysql-sdi-v1-design.md](mysql-sdi-v1-design.md) 为持久格式与恢复边界依据，最终结论重新回到
生产源码、物理页/MTR/flush 调用链和故障测试核对；implementation plan 只保留在 agent todo，未生成持久 plan，
未使用 GitNexus。

| Pass | Dimension | Result | Evidence / correction |
| --- | --- | --- | --- |
| 1 | 持久格式 / crash window | PASS | GENERAL extent0 固定 page3，page0 `SDI_ROOT=3`；`SDI1/v1` header 保存 table/version/length/CRC32C，DD payload `DDS1/v1` 保存完整聚合；CREATE 固定为 physical/empty page3 durable→ENGINE_DONE→full SDI durable→ACTIVE DD，单页超限不发布 DD并由 marker recovery 删除 orphan |
| 2 | WAL / 并发 / 资源 | PASS | page0/page3 修改只经 MTR/PAGE_INIT/PAGE_BYTES，写后 `flushThrough(commitLsn)` 再 force；普通写锁序 page0→page3，CREATE 唯一逆序位于尚未发布、无业务并发的 space 并使用有理由的短 `MtrLatchOrderScope`；MTR shared tablespace lease 与 DROP X lease 互斥，所有 durable wait 有正 timeout |
| 3 | 分层 / API / 异常 | PASS | `DictionarySdiCodec` 只在 DD 层解释聚合，`SerializedDictionaryInfo` 以防御数组和 content equality 暴露稳定 opaque DTO，`storage.sdi` 不 import DD；binding/opened path、table identity、root、envelope、长度和 CRC 均使用项目异常 fail-closed，无 `synchronized`、裸 `RuntimeException` 或 `IllegalArgumentException` |
| 4 | 恢复 / 兼容 / 安全 | PASS | recovery 在 logged/legacy DDL 收敛后逐张处理 ACTIVE；root=0、空页、逻辑 header/CRC、version/payload mismatch 只按 committed DD 重写；较高 SDI version 无反向发布权；未知 root、物理 envelope/checksum 与 ACTIVE 缺文件阻止 OPEN；UNDO page3 继续是 RSEG_HEADER |
| 5 | 测试 / 文档 / 真实接线 | PASS | codec round-trip/未知格式/截断/尾随、固定 page3、durable API、path 错绑、正常 CREATE、超限不发布、错版本修复、逻辑 CRC 重启修复、legacy root=0 升级、未知 root fail-closed 均有断言；固定 Gradle 全量 277 suites / 1522 tests，0 failure/error/skip；current map/backlog/DD/disk/recovery 文档已同步，catalog-loss rebuild 明确保留 |

## 2026-07-17 CREATE/DROP Atomic DDL Log / Undo Marker 5-Pass Review Log

本轮按最终源码、持久格式、故障测试与当前地图独立复核五遍；implementation plan 仅保留在 agent todo，
未新增持久 plan。调用链由设计文档、源码、`rg`、Git diff 与固定 Gradle 报告直接核对，未使用 GitNexus。

| Pass | Result | Evidence |
| --- | --- | --- |
| 1. 分层、格式与兼容 | PASS | `storage.api.ddl.DdlUndoMarker` 只携带数值 identity，storage 无 DD 反向依赖；`PersistentDdlLogRepository` 与普通 DD repository 共享 `InternalCatalogStore` 但只解释 `DDL_LOG(7)` 单记录 batch；key/payload 双 identity、stable operation/phase code、path 上限/UTF-8/尾随字节/平台非法路径全部 fail-closed；旧 catalog 无 marker 时 legacy pending/orphan recovery 保留 |
| 2. 状态机与崩溃裁决 | PASS | CREATE 固定 PREPARED→ENGINE_DONE→DICTIONARY_COMMITTED→COMMITTED 或前两阶段 ROLLED_BACK；DROP 固定 PREPARED→DICTIONARY_COMMITTED→ENGINE_DONE→COMMITTED 或 ACTIVE+PREPARED ROLLED_BACK；恢复只以 committed DD + marker identity/version/space/exact path 裁决，错绑、ACTIVE 缺文件、跨提交阶段退回 ACTIVE 均阻止 OPEN |
| 3. 并发、资源与 high-water | PASS | DDL repository writer lock 只覆盖 expected-phase CAS+单 batch append+snapshot publish，不跨 MDL、purge barrier、物理 DDL 或 dictionary transaction；DROP 等待仍不持 page/file/catalog 锁；control 回退同时消费最大 marker 与 `dictionaryVersion-1` 保守下界，CREATE SCHEMA 无 marker 也不复用 DDL id，DROP/recovery 多版本只产生安全空洞 |
| 4. TDD 与故障窗口 | PASS | RED→GREEN 覆盖合法/非法 phase、终态不可推进、跨重启重建、普通 DD batch 隔离、损坏 codec；CREATE PREPARED/ENGINE_DONE/ACTIVE 三边界和 DROP PREPARED/DROP_PENDING/ENGINE_DONE/DROPPED 四边界分别验证 rollback/finish；另覆盖歧义 DD commit、exact-path 拒绝、purge barrier timeout 与 control 双槽回退 |
| 5. 文档、静态与全量 | PASS | design、DD/undo/recovery 厚设计落点、current map 与 backlog 已按最终生产调用链同步；`DdlId/DDL_LOG` 从 Reserved 移除，`UndoLogKind.TEMPORARY` 继续明确 unwired；current map 10 项清单复核无 planned 边伪装实线；变更范围无 executable Java monitor、裸 `IllegalArgumentException/RuntimeException`、TODO/TBD 或越层 import；`git diff --check` 通过（仅 CRLF 提示）；固定 JDK 25.0.2 + Gradle 9.5.1：276 suites / **1514 tests**，0 failure/error/skip |

## 2026-07-17 Non-Unique Secondary Logical-Prefix Range / Locking Read 5-Pass Review Log

本轮按最终源码、测试、文档与固定工具链报告复核五遍；实现计划只保留在 agent todo，不新增持久 plan，
所有调用链均直接由本仓库设计、源码、`rg`、Git diff 与测试结果核对，未使用 GitNexus。

| Pass | Result | Evidence |
| --- | --- | --- |
| 1. SQL/Session/production chain | PASS | 从源码重核 `DefaultSqlSession → Parser → Binder → Executor → SqlStorageGateway → SecondaryMvccReader/SecondaryCurrentReadService`；locking clause 只为完整单列、无 prefix、non-unique secondary equality 发布 RW plan，point locking、comparison/composite range 与 residual predicate fail-closed；SQL/session 不 import storage internal |
| 2. MVCC/LOB/result completeness | PASS | consistent range 先在独立短 MTR 物化 including-deleted prefix candidates，再逐候选走聚簇 MVCC/undo、从可见完整行重算 logical key并按 clustered identity 去重；同一 RC/RR view 覆盖全部 external LOB hydration/投影；4096+1 溢出显式失败，不返回截断列表 |
| 3. Locking/concurrency/resource order | PASS | `SecondaryLogicalKeyLockKey` 支持 normalized REC_S/REC_X；locking read 先锁 logical prefix，再扫描候选并逐行走 clustered point current-read，所有锁等待均共享 absolute timeout 且不持 page latch/fix；INSERT、key-changing UPDATE 与 DELETE 在物理 secondary 操作前申请同 key X，事务 terminal 统一 `releaseAll` |
| 4. TDD/behavior coverage | PASS | RED→GREEN 覆盖 parser clause、Binder 确定性 access path、Executor/Gateway 多行与 LOB、旧/新 ReadView key-change、logical S/X 兼容、空/non-empty `FOR SHARE` 分别阻塞同 prefix INSERT、key-changing UPDATE 与 DELETE、Session RO/RW 路由、autocommit/显式事务终态，以及真实 Session `FOR UPDATE` 持锁到 COMMIT 后插入重试成功 |
| 5. 文档/静态/全量 | PASS | design、current map 与 backlog 已按最终生产调用链同步；旧 unique-only lock key/current gap 表述已清理；受影响生产源码无 executable Java monitor、裸 `IllegalArgumentException/RuntimeException`、TODO/TBD；`git diff --check` 通过（仅 CRLF 提示）；固定 JDK 25.0.2 + Gradle 9.5.1 `test --rerun-tasks --no-daemon`：275 suites / **1500 tests**，0 failure/error/skip |

## 2026-07-17 Secondary Index MVCC / Purge Closure 5-Pass Review Log

本轮按最终源码、测试与恢复接线独立复核五遍；所有调用链均直接由本仓库源码、`rg`、Git diff 和固定 Gradle 报告核对，未使用 GitNexus。

| Pass | Result | Evidence |
| --- | --- | --- |
| 1. 生产调用链与分层 | PASS | 从源码重核 `DefaultSqlExecutor → DefaultSqlStorageGateway → TableDmlService/SecondaryMvccReader`、`CrashRecoveryService → PersistentHistoryRecovery/RESUME_PURGE`、`DictionaryDdlService/RecoveryService → TablePurgeBarrier`；SQL/session 无 storage internal import，storage 无 SQL/session/engine/DD 反向 import，未把 SQL UPDATE/DELETE、non-unique range MVCC 或多 worker purge 画成实线 |
| 2. 物理格式、metadata 与兼容 | PASS | `SecondaryIndexLayout` 固定 logical parts + 去重后的完整 clustered-key suffix；`BTreeIndex.physicalUnique` 与 logical unique 分离；root level 每次从稳定 root 页头刷新；undo secondary v1 tail 保留旧 EOF、LOB+secondary 双尾，并对未知/截断/尾随格式 fail-closed；A→B→A 与 marked identity 均有回归 |
| 3. 并发、锁序与资源释放 | PASS | logical unique 使用统一 `LockManager` 事务级 X 锁，NULL 不冲突；1024 fair stripe 只协调 DML/purge 同行物理操作；所有等待有 timeout/zero-wait，锁序为 current-read/unique lock → row guard → 单树短 MTR，secondary/clustered/undo latch 不重叠；生产源码无 executable Java monitor |
| 4. rollback、purge 与恢复 | PASS | statement/full/recovery rollback 按 secondary → clustered → logical marker 收敛；故障测试覆盖 secondary/cluster/marker durable 边界；purge 用聚簇版本链证明旧 secondary 可删，DELETE secondary-first，task commit 后崩溃不提前推进 history；affected-table projection 可由 persistent history 重建，DROP 与 DROP_PENDING recovery 都等待 barrier |
| 5. 注释、文档、静态与全量 | PASS | 完整 design/implementation plan 已持久化；current map/backlog/相关厚设计按真实接线同步，fault seam 进入 Reserved/Unwired 表；新增/修改核心类、字段、方法和复杂测试补齐中文契约与阶段化数据流；无裸 `IllegalArgumentException/RuntimeException`、越层 import 或 feature 占位；固定 JDK 25.0.2 + Gradle 9.5.1 `test --rerun-tasks --no-daemon`：275 suites / **1476 tests**，0 failure/error/skip；`git diff --check` 通过 |

## 2026-07-16 Primary-Point SQL / Session v1 Corrective 5-Pass Review Log

| Pass | Result | Evidence |
| --- | --- | --- |
| 1. 生产调用链与分层 | PASS | 从源码重核 `DatabaseEngine.openSession → DefaultSqlSession → parser/policy/binder/executor → SqlStorageGateway → engine.adapter → DD/DML/MVCC/LOB`；新增 deadline/admission 均位于既有向下依赖方向，SQL/session 对 storage 零 import，storage 无反向 import；未把 optimizer/network/prepared/UPDATE/DELETE/range locking 画成实线 |
| 2. SQL/DD/类型与身份 | PASS | 28 DD types 与 external LOB 既有覆盖保留；JSON 改为 binder/record 共用严格 RFC 8259 validator；Session MDL owner 进入保留高半区，DDL facade 拒绝保留 owner，测试证明相同普通数字不能绕过 MDL 等待 |
| 3. LOB/MVCC/Undo crash 边界 | PASS | RC ReadView 的 finally 移到 external LOB hydration/投影之后；INSERT ownership rollback 以精确 durable hook 验证 inverse 后 marker 前 LOB 仍可读、marker 后已释放，两侧重试分别幂等重放或只终结，不再用正常 close/reopen 冒充该故障窗口 |
| 4. Session/Engine/timeout/fatal | PASS | 锁序固定 Engine read permit→Session operation lock；CLOSING 既拒新 Session 也拒既有 Session 新语句，close 持 write quiescence 后才关闭 Session/storage；共享 absolute deadline 贯穿 handle/row lock/LOB；cause/suppressed fatal 保持严重度并发布 Engine FAILED |
| 5. 文档/静态/全量 | PASS | current map 只更新受影响 SQL/transaction/engine 小节并按 10 项清单复核实线/状态/Reserved/Gap；2.8 保持 complete，2.2/2.3 与 LOB replacement purge 仍是 gap；无 executable Java monitor、裸 runtime exception、TODO/TBD 或越层 import；固定 JDK 25.0.2 + Gradle 9.5.1 `test --rerun-tasks --no-daemon`：267 suites / **1419 tests**，0 failure/error/skip；`git diff --check` 通过（仅 Git CRLF 提示） |

## 2026-07-15 Data Dictionary + Physical DDL v1 5-Pass Review Log

直接核对设计文档、生产源码调用链、真实文件/MTR 路径、测试与当前实现地图；未使用 GitNexus 或生成式调用图。

| Pass | Dimension | Result | Evidence / correction |
| --- | --- | --- | --- |
| 1 | 持久化 / crash window | PASS | 核对 control 双槽、catalog manifest、version gap、CREATE 物理先于 DD publish、DROP_PENDING→DISCARDED→DROPPED；修正 CREATE 歧义提交误删物理文件与 DROP_PENDING 歧义提交重载旧 ACTIVE 的窗口，并补两侧重启裁决、high-water 反向校正、durable DISCARDED 后崩溃/重试及 binding/opened path 不符拒绝测试 |
| 2 | 并发 / 资源边界 | PASS with documented limit | schema→table MDL，cache pin 逆序释放，tablespace X lease 后再 drain/MTR；等待均有 timeout；明确 DROP 尚缺 table-level persistent purge-history barrier |
| 3 | 分层 / 异常 / 不变量 | PASS | 修正 `storage.fil.catalog`→DD 异常反向 import，并把 DD discovery→`storage.engine` 配置转换上移到公共组合根；DD→BTree 转换留在 `engine.adapter`；补齐 logical/storage/binding 的 index id/name 唯一、clustered unique 与 root/segment 同 space 校验；cache loader 的 JVM `Error` 原样传播并清理 single-flight；无 `synchronized`、裸 runtime/argument 异常或未决占位 |
| 4 | 测试 / 兼容 | PASS | 保留 `TableDefinition` 旧构造与 legacy 单索引 rollback/purge 构造；二次复核补多索引 root、事务 rollback、ACTIVE 缺文件、CREATE/DROP 歧义提交、purge identity resolver、durable marker 续作、损坏 binding 与分层契约测试；固定工具链全量 249 suites / 1348 tests，0 failure/error/skip |
| 5 | 文档 / 真实接线 | PASS | 从源码重画 public bootstrap、DDL 和 undo resolver 实线；将 DDL_LOG/SDI/purge-aware DROP 留在 partial/reserved；同步厚设计落点、slice spec 和 backlog |

## 2026-07-13 Extern Undo Record Payload 5-Pass Review Log

直接核对设计、生产源码调用链、页格式/失败边界、测试与文档；未使用 GitNexus、Superpowers 或生成式调用图。

| Pass | Dimension | Result | Evidence / correction |
| --- | --- | --- | --- |
| 1 | 持久格式 / 损坏拒绝 | PASS | `PageType.UNDO_PAYLOAD=9`、35B `0x7F/v1` descriptor 与 `UEP1/v1` payload header 均钉死；补正 FIL_NULL/越界 next/cyclic page 在构造 PageId 前 fail-closed，codec 拒绝尾随字节 |
| 2 | 规划 / reservation / fail-stop | PASS | `UndoWritePlan` 冻结 physical/logical head、完整编码、root grow、external 页数与 redo workload；复核后把 first-write claim 的不可逆栅栏移到 reservation 成功之后，容量申请失败不会泄漏 RESERVED slot；stale 双计划专项通过 |
| 3 | 生产调用链 / 回收恢复 | PASS | `rg` 核对生产 DML 只走 `planX→appendPlanned`；segment/direct pointer/rollback/purge/MVCC 均经 `UndoStoredRecordResolver`；payload 与 root 同 FSP segment、与主 FIL chain/LOB ownership 分离，既有 finalizer drop plan 自动覆盖全部页 |
| 4 | 配置 / 静态规则 / 文档 | PASS | `EngineConfig` 默认 16、wither/旧构造兼容、最小分片上限有测试；slice 49 行；thick design/backlog/current map 已同步；变更范围无 Java monitor、裸 `IllegalArgumentException/RuntimeException`、TODO/TBD，`git diff --check` 通过（仅 CRLF 提示） |
| 5 | 定向 + 强制全量回归 | PASS | 格式/codec/segment/manager/config/DML 定向测试通过；固定 JDK 25.0.2 + Gradle 9.5.1 `test --rerun-tasks`：232 suites / **1249 tests**，0 failure/error/skip；包含 oversized 主链不增长、planned 精确页数、flush-close-reopen 外置链读回 |

## 10-Pass Review Checklist

本文件于 2026-06-18 首次生成（仅 Disk Manager Slice），同日大幅扩展为 10 模块全量 map。每次大幅改写后必须按以下 10 项复核，并对每个模块独立过一遍。15 遍独立复核记录见下方 15-Pass Review Log。

| Check | Requirement | Scope |
| --- | --- | --- |
| 1 | 每个 `Current Flow` 中的实线边对应当前生产代码且有 `file:line` 引用 | per module |
| 2 | 每个 planned/partial/unwired 边用虚线且标注 `planned`/`partial`/`unwired` | per module |
| 3 | 依赖方向合规：上层不 import 下层内部实现；底层不 import SQL/session/executor | per module |
| 4 | `PageStore` 保持 registry-free/state-free；`TablespaceRegistry` 只经 `DiskSpaceManager`/loader 接线 | Disk Manager |
| 5 | 每个 `Reserved / Unwired` 类型写清现状、保留理由、下一步动作 | per module |
| 6 | 每个 `Known Implementation Gaps` 条目写清后果和解决方向 | per module |
| 7 | 无未决占位词（TBD/TODO/待定）；状态不确定时写出需核对的源码入口 | whole doc |
| 8 | `Package Status` 中的 Implemented/Partial/test-only 标签与 `Reserved` 表一致 | per module |
| 9 | 跨模块调用链一致（如 MTR commit 在 buf/mtr/redo/flush 小节的描述吻合） | cross module |
| 10 | 本文件只描述当前实现，不改写目标设计 | whole doc |

## 2026-07-10 Undo Slot Concurrency Hardening 5-Pass Review Log

本切片在实现和独立 code review 完成后，按用户要求以最新源码再执行五遍成品检查；每遍使用不同事实面，
直接核对生产调用点、锁/状态、故障测试、current map 与固定 Gradle 报告，未使用 GitNexus。

| Pass | Dimension | Result | Evidence / correction |
| --- | --- | --- | --- |
| 1 | 生产调用链 / blast radius | PASS | 创建链只有 `UndoLogManager` 执行 reserve/preflight/create/bind/persist；四路终态只有 `UndoSegmentFinalizer` 进入 allocator/page3 clear；无直接 slot release 旁路 |
| 2 | 状态机 / 并发 / 锁序 | PASS | `FREE/RESERVED/ACTIVE/FINALIZING` 仅在 `ReentrantLock` 短临界区转换，锁内无 IO；double-finalize 与真实 stale/reuse 均在 allocator 前拒绝；无 Java monitor 调用 |
| 3 | 异常 / crash / recovery | PASS | 分配前 owner 冲突取消 RESERVED；bind 后发布冲突 fatal 并保留 ACTIVE；物理边界后与 commit-after-crash 保留 FINALIZING；live/recovery rollback、二次启动、purge resume 专项通过 |
| 4 | 文档 / 当前事实 | PASS | 三个 2026-07-10 slice 为 50/49/53 行；transaction/undo 两处 flow 均与源码一致；无旧 `InsertReclaim`、直接 release 或占位表述；backlog 下一推荐不变 |
| 5 | 全量回归 / 静态规则 | PASS | 固定 JDK 25.0.2 + Gradle 9.5.1 `test --rerun-tasks`：211 suites / **1090 tests**，0 failure/error/skip；`git diff --check` 仅换行提示；无裸 `IllegalArgumentException/RuntimeException` |

## 2026-07-10 Atomic Undo Segment Finalization 10-Pass Review Log

按用户要求分两轮各复核五遍：设计/开工前五遍用于确认边界，代码与文档完成后再以最新工作区复核五遍。
全过程直接核对本地设计、源码、测试、`rg`、Git 与 Gradle 报告，未使用 GitNexus 或 Superpowers。

| Pass | Phase / dimension | Result | Evidence / correction |
| --- | --- | --- | --- |
| 1 | 设计预检：FSP/page latch 顺序 | PASS | `dropSegment` 固定 page0→page2，XDES 内嵌 page0；finalizer 随后取 page3，不需要新的 ordering 例外 |
| 2 | 设计预检：事务证据边界 | PASS | 确认已清 slot 的纯 insert 不再给 counter scan 留证据；没有误宣称本片解决，backlog 下一推荐明确为正式 trx recovery table v1 |
| 3 | 设计预检：异常传播 | PASS | `UndoFinalizationException` 为 fatal 项目异常；DML 原样失败且事务不发布 COMMITTED，purge target 异常会使 worker 进入 FAILED |
| 4 | 设计预检：恢复权威 | PASS | page3 occupied slots 是 ACTIVE/COMMITTED undo 重建权威；slot clear 与 FSP drop 必须同批，内存 slot/history 只作提交后投影 |
| 5 | 设计预检：blast radius | PASS | 直接扫描 UndoLogManager/RollbackService/PurgeCoordinator 构造与 34 处 insert-reclaim 代码/测试/文档引用后再修改；未使用高层推断工具 |
| 6 | 完成复核：生产调用链 | PASS | 四个终态调用点全部收敛到 `UndoSegmentFinalizer`；生产中只有 finalizer 调 `dropUndoSegment`、page3 clear 和 expected-owner memory release；旧 reclaim 类型/队列删除 |
| 7 | 完成复核：锁序 / 资源 | PASS | 预检 MTR page3→first page 后全部释放；最终 MTR FSP page0/page2→page3；HistoryList/slot 短锁内无 IO，B+Tree inverse/purge 前不持 undo latch |
| 8 | 完成复核：redo / crash | PASS | 专项测试断言 `FspPageFreeRecord`、FSP metadata、`RSEG_SLOT` clear 与 trx delta 同 batch；insert 与 purge 均覆盖 commit 后、内存发布前 crash window；物理写阶段失败 fail-stop |
| 9 | 完成复核：recovery / extent | PASS | 非空 rollback head 在 drop 前拒绝；ACTIVE 三行 engine recovery 后 page3 slot 为 0，二次启动仍为 0；70×约7KiB undo 跨 fragment/extent，在 4-frame reopen 下终结并断言 XDES release delta |
| 10 | 完成复核：测试 / 文档 / 静态规则 | PASS | 固定 JDK 25.0.2 + Gradle 9.5.1 `test --rerun-tasks`：211 suites / 1078 tests，0 failure/error/skip；slice 50 行；`git diff --check` 通过；生产 Java 无 monitor 调用形态、裸 `IllegalArgumentException/RuntimeException` 或新增 TODO/TBD |

## 2026-07-10 Full Rollback Persistent Progress 5-Pass Review Log

本切片完成后按生产源码、redo/MTR 边界、故障测试和 Gradle XML 报告连续复核五遍；只使用本地设计文档、
current map、源码、测试、`rg` 与 Git，未使用 GitNexus 或 Superpowers。每遍均以最新工作区为准，未把
page3 release、正式 trx recovery table 或多索引/DD 等非目标写成已接生产边。

| Pass | Check dimension | Result | Notes |
| --- | --- | --- | --- |
| 1 | 逻辑链 / 数据语义 | PASS | live/recovery 共用短读入口；current 必须匹配 expected pair，target 从真实 predecessor record 构造，覆盖 partial rollback 后新 append 的 `3→1` 非连续 undoNo 链，不以算术减一复活 detached branch |
| 2 | WAL / crash 顺序 | PASS | 每条固定 current+predecessor read→inverse commit→marker CAS commit→live context publish；hook 只在成功 commit 后触发，inverse 后 crash 最多幂等重做当前 record，marker 后 crash 从 predecessor/EMPTY 继续；未声称解决 MTR COMMITTING fail-stop |
| 3 | 资源 / 失败边界 | PASS | undo record 在独立短读 MTR 中物化，进入 B+Tree inverse 前已释放 undo latch/fix；predecessor 损坏在 inverse 前失败；非终态失败保留 ROLLING_BACK、slot 与 row locks，到 EMPTY 后才做诊断 redo、release/finish |
| 4 | 生产调用链 / 文档分层 | PASS | 公开 `RollbackService` 五参构造不变并固定 no-op injector；包内 phase/injector 由生产 service 持有而非 Reserved；只更新 current map、storage backlog 与 49 行 slim slice，未修改全局目标图或持久 implementation plan |
| 5 | TDD / 回归 / 静态规则 | PASS | 先观察缺失 injector 的 RED 编译失败；固定 JDK 25.0.2 + Gradle 9.5.1 `test --rerun-tasks`：211 suites / 1074 tests，0 failure/error/skip；diff check 通过；生产 Java 无 monitor 调用形态或裸 `IllegalArgumentException/RuntimeException`，本切片无新增 TODO/TBD/待定 |

## 2026-07-10 Persistent Logical Undo Head 5-Pass Review Log

本切片在 slim slice 完成后按源码、redo batch、测试报告和 `rg` 调用点连续复核五遍；未使用 GitNexus。
每遍发现的问题先修正，再以最新工作区重新核对，不把设计目标当作已接生产边。

| Pass | Check dimension | Result | Notes |
| --- | --- | --- | --- |
| 1 | 语义 / 页格式 | PASS | v1 flags 对 first/chain 每页守门；header `[63,120)` 与 record area 120 无重叠；`UndoLogicalHead` 强制 `NONE/NULL` pair，legacy/未知版本 fail-closed |
| 2 | WAL / crash | PASS | append 在同一 MTR 发布 record、物理 count/high-water 与 15B persistent head；partial rollback 固定 inverse→marker→memory 顺序；marker 落后可幂等重做、不会领先数据修改；redo replay/pageLSN 测试通过 |
| 3 | 资源 / 失败边界 | PASS | full/partial/recovery/purge 均逐 pointer 短读并在 B+Tree 修改前释放 undo latch/fix；full rollback 可从 ROLLING_BACK 重试，非终态失败保留 slot/row locks；purge 先验链后副作用，insert reclaim 为 peek→drop→poll |
| 4 | 生产调用链 / current map | PASS | 源码核对 `StorageEngine/ClusteredDmlService/UndoLogManager/RollbackService/MvccReader/PurgeCoordinator` 真实边；recovery/purge 无 `forEachRecord*` production caller；修正旧 tests-only flow、Reserved 与 backlog，未宣称 SQL/session 或持久 history 已接 |
| 5 | 测试 / 静态规则 | PASS | 固定 JDK 25.0.2 + Gradle 9.5.1 `test --rerun-tasks`：211 suites / 1069 tests，0 failure/error/skip；定向 7 suites / 96 tests；slice 51 行；diff check 通过；无 Java monitor 调用形态、裸 `IllegalArgumentException/RuntimeException` 或新增 TODO/TBD 占位 |

## 15-Pass Review Log

2026-06-18 对扩展后的 10 模块全量 map 执行 15 遍独立复核。每遍聚焦一个维度，覆盖全部 10 个模块。工具：gitnexus MCP（已提交代码）+ grep/Read（未提交新文件）。

| Pass | Check dimension | Result | Notes |
| --- | --- | --- | --- |
| 1 | 实线边 `file:line` 引用准确性 | PASS | 9 个模块各抽 1 条引用验证，全部匹配源码行内容 |
| 2 | 虚线边标注 `planned`/`partial`/`unwired` | PASS | 8 条虚线边；3 条为 test-only 链延续边，隐式继承首边标注（`durable() factory, test-only` / `flush(), test-only`） |
| 3 | 依赖方向合规 | PASS | `record.page` → `buf.PageGuard` 是设计内接口（`PageGuard` 是公开 RAII 句柄，非内部 `BufferFrame`）；`fil`/`buf`/`redo` 无上层 import |
| 4 | `PageStore` registry-free/state-free | PASS | `PageStore.java`/`FileChannelPageStore.java` 仅 javadoc 注释含 "registry-无关"，无 registry import/字段 |
| 5 | 每个 Reserved 类型有下一步动作 | PASS | 全部 Reserved 表行 4 列完整（Type \| Current caller \| Why \| Next action） |
| 6 | 每个 Known Gap 有后果+解决方向 | PASS | 53 行 gap 数据，全部 3 列完整 |
| 7 | 无未决占位词 | PASS | "占位" 2 处匹配是规则描述本身（"不得用未决占位词"），非实际占位 |
| 8 | Package Status 标签与 Reserved 表一致 | PASS | Implemented/Partial/test-only 标签在两个表中描述一致 |
| 9 | 跨模块调用链一致 | PASS | MTR commit（`:154`）、WAL gate（`FlushCoordinator:91-92`）、page access 链在 buf/mtr/redo/flush/disk 小节描述吻合 |
| 10 | 只描述当前实现，不改写目标设计 | PASS | "目标架构" 仅在 header 引用全局设计文档时出现，map 本身无目标设计声明 |
| 11 | 生产代码无 `synchronized`/`wait`/`notify` | PASS | `src/main/java` 全量 grep 0 匹配 |
| 12 | test-only 构造声明验证 | PASS | 2026-06-18 当时 test-only 表已排除 E1 接线的 `FlushService`/`IndexPageAccess`/redo IO；E3a 后 `PageCleanerWorker` 已生产接线，当前状态见 Flush Package Status |
| 13 | 异常层次验证 | PASS | 30 个异常类：27 extends `DatabaseRuntimeException`（含 2 个 base 类），3 extends `DatabaseFatalException`（`RedoLogCorruptedException`/`DataFileCorruptedException`/`RecoveryStartupException`） |
| 14 | domain VO 消费范围验证 | PASS | `UndoNo` 仅被 `storage.undo` 4 文件 import；`TransactionNo` 仅被 `storage.trx` 2 文件 import；与 map 描述一致 |
| 15 | 最终通读 | PASS | 10 模块小节结构一致（Current Flow + Data Chains + Package Status）；全局 Reserved/Unwired 按模块分组；Known Gaps 按模块分组；无遗漏模块 |

## 2026-07-01 LockManager 5-Pass Review Log

本切片完成后按源码真实调用链复核，不按 slice 计划臆造生产接线。验证命令包含目标测试与固定 Gradle 全量测试。

| Pass | Check dimension | Result | Notes |
| --- | --- | --- | --- |
| 1 | TDD / 验收测试 | PASS | 先新增 `LockManagerCoreTest` 并观察缺失 API 编译失败；实现后目标测试通过，覆盖 record/gap/next-key/insert-intention、timeout、两/三事务死锁 |
| 2 | 生产接线真实性 | PASS | 0.17 当时 `storage.trx.lock` 仅 tests 调用；2.7a 后当前状态见上方 `storage.trx.lock`/`storage.btree current-read` 行 |
| 3 | 锁序与等待边界 | PASS | `LockManager` 使用 indexId 分片 `ReentrantLock` + `Condition`；wait-for graph 独立锁且只按“分片锁 -> graph 锁”获取；等待前 page latch 释放仍留后续 current-read 片 |
| 4 | 静态规则 | PASS | `src/main/java` 无 `synchronized(...)`/`wait()`/`notify()`/`notifyAll()` 调用形态；无直接 `throw new IllegalArgumentException/RuntimeException`；锁异常扩展 `DatabaseRuntimeException` |
| 5 | 回归 | PASS | 固定 Gradle/JDK 全量 `test` 通过，测试报告 828 tests；current map 与 storage backlog 已同步 0.17 状态 |

## 2026-07-01 B+Tree Current-Read 5-Pass Review Log

本切片按源码调用链复核：当时只把 point current-read / unique-check 标为已接，不把 range scan、SQL/session DML 或事务自动释放写成已实现；2.1 后单聚簇 DML 已释放 facade 持有的行锁，SQL/session 仍未接。

| Pass | Check dimension | Result | Notes |
| --- | --- | --- | --- |
| 1 | TDD / RED-GREEN | PASS | 先写 `BTreeCurrentReadServiceTest` 并观察缺失 API 编译失败；实现后目标测试通过，覆盖 latch 释放、重定位、RC/RR miss、unique duplicate/gap、timeout/deadlock 异常传播、重定位异常释放已授予锁 |
| 2 | 等待边界 | PASS | `BTreeCurrentReadService` 每次定位都用短 MTR 并在 `LockManager.acquire` 前 commit/rollback；测试在等待行锁时可另取 root X latch |
| 3 | 重定位与 stale lock | PASS | root split 后旧 `RecordLockKey` 失效，服务释放 stale lock 并重新锁新 record；若授锁后重定位抛异常，会释放刚授予的锁；成功锁仍由事务级 `releaseAll` 收尾 |
| 4 | 当前接线真实性 | PASS | 2.7a 当时 `StorageEngine` production-held `LockManager` + `BTreeCurrentReadService` 且 legacy `lookup/scan/insert` 未改；2.7b 后当前状态见上方 `storage.btree current-read` 行 |
| 5 | 回归与静态规则 | PASS | 固定 Gradle/JDK `test --rerun-tasks` 通过，测试报告 837 tests；新 btree/engine/trx.lock 代码无裸 `IllegalArgumentException/RuntimeException`、无 Java monitor 调用 |

## 2026-07-01 B+Tree Range Current-Read 5-Pass Review Log

本切片按源码调用链复核：当时只把 `BTreeCurrentReadService.lockRange` storage 内入口标为已接；2.1 后单聚簇 DML 已调用 point/unique current-read 并在 facade commit/rollback 释放锁，SQL/session/executor range DML 仍未接。

| Pass | Check dimension | Result | Notes |
| --- | --- | --- | --- |
| 1 | TDD / RED-GREEN | PASS | 先新增 range current-read 测试并观察缺失 `lockRange` 编译失败；实现后目标测试覆盖 RR next-key/terminal gap、RR empty gap、RC record-only、等待释放 latch、重定位变化、timeout/deadlock 清理 |
| 2 | 等待边界 | PASS | `lockRange` 经短 MTR 定位并在 `LockManager.acquire` 前 commit/rollback；测试在 range 等待 insert intention 冲突时可另取 root X latch |
| 3 | 多锁释放 | PASS | range 尝试中 timeout、deadlock、重定位变化、重定位异常都会关闭本次尝试已授予 handles；成功锁仍由事务级 `releaseAll` 收尾 |
| 4 | 当前接线真实性 | PASS | `StorageEngine` production-held `BTreeCurrentReadService`；legacy `scan` 未改；新增 `SplitCapableBTreeIndexService.locateRangeForCurrentRead` 仅被 current-read 服务调用 |
| 5 | 回归与静态规则 | PASS | 固定 Gradle/JDK `test --rerun-tasks` 通过，测试报告 844 tests；新 btree/engine/trx.lock 代码无裸 `IllegalArgumentException/RuntimeException`、无 Java monitor 调用 |

## 2026-07-01 lockobs 依赖方向修正（审查后续，零语义变更）

对 0.17/2.7a/2.7b/2.8a 未提交切片按源码调用链复核时发现分层违规并修复，只改结构不改行为。

| 项 | 内容 |
| --- | --- |
| 问题 | `storage.trx.lock` ⇄ `server.lockobs` 双向包循环：`LockManager` 及其快照 DTO(`Granted/Waiting/WaitForEdge`) 反向 import 上层 `server.lockobs` 的观测接口与事件值对象，违反「避免反向依赖和循环依赖」「底层不能 import 上层」 |
| 修复 | 观测端口下沉进 storage：新增 `storage.trx.lock.RowLockEventSink`(+`NoopRowLockEventSink`)，并把 `ThreadEventId`/`RowLockObservation`/`RowLockBlocker` 从 `server.lockobs.domain` 迁入 `storage.trx.lock`；`server.lockobs.LockObservationService` 改为 `extends RowLockEventSink` 只追加 `captureSnapshot`/`latestDeadlocks`；删除已无引用的 `NoopLockObservationService` |
| 顺带清理 | 删除 `LockManager` 中从未被调用的死方法 `blockingOwners` |
| 依赖核对 | `rg "import cn.zhangyis.db.server" src/main/java/.../storage/trx` 为空；storage→server 仅剩组合根 `StorageEngine`→`server.lockobs` 单向边，`server` 不 import `storage.engine`，无环 |
| 回归 | 固定 Gradle/JDK `test` 通过，179 suites / 849 tests，0 失败/错误/跳过 |

## 2026-07-01 B+Tree 读路径 crab 5-Pass Review Log（0.13c）

按源码调用链复核：只把 read-path S-crab 标为已接，不把 SX latch、乐观读重启或通用 root/version 重定位写成已实现。

| Pass | Check dimension | Result | Notes |
| --- | --- | --- | --- |
| 1 | TDD / RED-GREEN | PASS | 先加 `fullScanFitsInSmallBufferPoolBecauseReadPathCrabs`（cap 12 池 + ~20 leaf 全量 scan）：非 crab 因同时 fix root+全部 leaf 抛 `BufferPoolExhaustedException`（RED 已复现），实现 crab 后 GREEN；另加 reader/writer 并发一致性用例 |
| 2 | latch 早释放 | PASS | 点读/scan/current-read 定位改经 `descendSharedCrab` 全 S hand-over-hand，祖先经 `releaseHandle`→`releaseLatch` 早释放；scan sibling 链先 latch 后继再放前驱；写路径 `findLeaf(EXCLUSIVE)`/`descendPath` 未改 |
| 3 | 并发正确性 | PASS | 结构变更走悲观全 X 且持 root X 到 commit，与读者 root S 冲突串行；hand-over-hand 保证释放祖先/前驱前已 latch 到仍有效的子/后继页；并发 scan-vs-insert 每次快照有序无重、终局无丢无损 |
| 4 | 语义不变 | PASS | `lookup`/`scan` 返回等价物化结果；current-read 定位值对象不变；MVCC/purge/rollback/current-read 依赖 `lookup`/`scan` 的既有用例全绿 |
| 5 | 回归与静态规则 | PASS | 固定 Gradle/JDK 全量 `test` 通过，179 suites / **851 tests**，0 失败/错误/跳过；新代码无裸 `IllegalArgumentException/RuntimeException`、无 Java monitor 调用 |

## 2026-07-01 prefix index 比较 Review Log（0.13d 分片）

0.13d 是一整包，本次只落其中最自洽的 prefix index 比较；不碰 SX latch / root retry（大 epic）、btree redo handler、页头 MVCC 字段。

| 项 | 内容 |
| --- | --- |
| TDD RED-GREEN | 先加 4 个前缀比较用例（`RecordComparatorTest` 3 + `SearchKeyComparatorTest` 3），RED：整列比使 `'application'` vs `'apple'` prefix(3) 判不等、数值列 prefix 不拒绝；实现后 GREEN |
| 实现 | 新增 `record.type.KeyPrefix.apply(FieldSlice,ColumnType,prefixBytes)`：字节类型截前 N 字节、非字节类型 `prefixBytes>0` 抛 `DatabaseValidationException`；`RecordComparator` 与 `SearchKeyComparator` 两侧编码切片同截再 `codec.compare`，保持 leaf/node-pointer 同序 |
| 覆盖面 | `RecordPageSearch.findEqual/findInsertPosition`、btree scan 边界（走 `RecordComparator`）、node pointer 排序（走 `SearchKeyComparator`）自动覆盖 |
| 简化 | prefixBytes 以字节计非字符（binary collation）；存储仍存整值，只在比较时截前缀（未做存储层前缀截断键） |
| 回归 | 固定 Gradle/JDK 全量 `test` 通过，179 suites / **857 tests**（851+6），0 失败/错误/跳过 |

## 2026-07-02 SX（SHARED_EXCLUSIVE / SIX）page latch Review Log（0.13d 分片）

0.13d 剩余「SX latch + root/version retry」大 epic 分阶段推进：本次只落 SX latch 基础设施（新增第三种 page latch 模式与兼容矩阵），**不碰** root X 释放/OLC 版本校验/B-link 右链/乐观读重启（下一阶段独立切片）。SX 语义=可与多个 S 并存、但排它另一 SX 与 X，只授只读内容访问。

| 项 | 内容 |
| --- | --- |
| TDD RED-GREEN | 先加 `SharedExclusiveLatchTest`（5 例：S↔SX 双向共存、SX 排 SX、SX 排 X、X 排 SX、SX 只读内容）+ `MiniTransactionTest.sharedExclusiveToExclusiveUpgradeIsForbidden` + `BufExceptionTest` 断言模式数 2→3；RED：无 `SHARED_EXCLUSIVE` 常量不编译，实现后 GREEN |
| 实现 | `PageLatchMode` 增 `SHARED_EXCLUSIVE`（附兼容矩阵 Javadoc）；`BufferFrame` 增每帧 `pageIntentLatch`(`ReentrantLock`)；`BufferPoolInstance.acquire` 对 SX 先 `pageLatch.readLock()` 后 `pageIntentLatch.lock()`；`PageGuard` 加 `heldIntentLatch` 字段+5 参构造，close 逆序先放 intent 后放 read latch；写仍经 `requireExclusive` 只认 EXCLUSIVE |
| 分层实现理由 | 不替换核心 `ReentrantReadWriteLock`（改动面大、风险高），改用「readLock 并存 + 独立 per-frame intent lock 互斥」叠加实现 SIX；additive、对既有 S/X 路径零行为变化 |
| MTR 防护 | `MiniTransaction.fix` 同页升级防护扩为「持 S 或 SHARED_EXCLUSIVE 再求 X 即禁」（两者都持 readLock，再取 writeLock 自死锁）；本切片不支持原地 SX→X 升级 |
| 简化/非目标 | 未接入任何 B+Tree 写路径（SMO 尚用悲观全 X）；未做 root X 释放 + 版本重定位（下一阶段）；SX→X 原地升级、B-link 右链、OLC 乐观读 deferred |
| 回归 | 固定 Gradle/JDK 全量 `test` 通过，180 suites / **863 tests**（857+6），0 失败/错误/跳过 |

## 2026-07-02 悲观 insert safe-node 早释放祖先 Review Log（0.13d 分片）

0.13d 剩余大 epic 分阶段第二步：落 safe-node（设计 §10.2 step4-5）——悲观 insert 遇 safe 内部节点即释放其以上祖先 X（含 root），root X 不再持到 commit。**只覆盖 insert**；delete/purge safe-node、SX 下降+restart、B-link/OLC deferred。

| 项 | 内容 |
| --- | --- |
| TDD RED-GREEN | 先加 `pessimisticSplitReleasesRootLatchEarlyWhenSplitDoesNotReachRoot`（payloadKey 建 ≥2 层树，断言 `safeNodeAncestorReleaseCount()>0` + 全 key 有序无损）；RED：safe-node 前恒 0，实现后 GREEN |
| 实现 | `descendPathInsertSafeNode(mtr,index,key,maxSeparatorSize)` 替换 `pessimisticInsert` 的 `descendPath(X)`：每 latch 到内部 child 若 `writeDescendSafe`（`freeSpace ≥ maxSeparatorSize`）即 `releaseRetainedAncestors`（释放并清空保留链、计数）；`maxSeparatorSize` 按索引 key 列各类型最大字节编码一个 node pointer 取长 + margin；只判内部页（leaf 不判、恒保留） |
| 正确性 | 保留链顶恒为「最近 safe 内部节点」或真 root；`splitLeafAndPropagate`/`insertSeparator` 沿保留链自底向上遇 safe 顶（`pointerFits` 必真，因上界 ≥ 实际 separator 且持 X 期间 freeSpace 不变）即吸收返回，绝不越过访问已释放祖先，仅顶恰是 root 且放不下才 growRoot；每保留节点至多收 1 separator。既有 split 传播引擎零改动 |
| 死锁自由 | insert-only：下降起点短持 root X（遇 safe 即放）⟹ 任一时刻仅一个 insert「在 root 处」；leaf split 只触右邻兄弟（`nextPageNo`，单向全序）⟹ 并发 splitter 无环。读者/乐观 S-crab 正确性靠 hand-over-hand、**不依赖** root X 持到 commit（同步修正 `descendOptimistic`/`descendSharedCrab` Javadoc + `concurrentInsertsAcrossLeavesStayCorrect` 测试说明）。2026-07-03 修正 `SpaceReservationService` 账本锁边界：并发 split 的 `allocatePage -> consumePageIfReserved` 不再等待全局 reservation lock，`reserve` 也不再持该锁等待 page0，从而消除 reservation lock ↔ page0/index page latch 环 |
| 简化/非目标 | delete/purge 仍 `descendPath` 全 X 持到 commit（merge safe-node 需 `considerMerge` 的 `parentIsRoot` 按 rootPageId 重构 + 兄弟 latch 定序）；未用 0.13d SX latch（下阶段 SX 下降+restart 才用）；`maxSeparatorSize` 每次悲观 insert 现算（未缓存） |
| 回归 | 固定 Gradle/JDK 全量 `test` 通过，181 suites / **864 tests**（863+1），0 失败/错误/跳过；含并发 insert/scan 正确性用例不倒退 |

## 2026-07-02 delete/purge safe-node 早释放祖先 Review Log（0.13d 分片）

0.13d safe-node 第二步：把 insert-only 的 safe-node 扩展到 delete/purge 悲观 merge 路径。**至此全部悲观 SMO 路径（split+merge）都不再把 root X 持到 commit**（SMO 传播到 root 时除外）。

| 项 | 内容 |
| --- | --- |
| TDD RED-GREEN | 先加 `pessimisticMergeReleasesRootLatchEarlyWhenMergeDoesNotReachRoot`（10 宽 key 行 → level-2 树 root 子 [2ptr,3ptr]，删 key10 使 3ptr 子树最右 leaf 欠载 → 悲观回退经过 delete-safe 的 3ptr 节点，断言 `safeNodeDeleteAncestorReleaseCount()>0` + merge 停在子树内 + 全 key 有序无损）；RED：计数恒 0，实现后 GREEN |
| 实现 | 通用化 `descendPathSafeNode(mtr,index,key,Predicate<RecordPage>,LongAdder)`（insert/delete 谓词参数化、计数分开）；`deleteDescendSafe` = `(freeSpace+garbage+maxSeparatorSize)*2 <= pageSize`（与 `isUnderfull` 同公式、被摘指针取上界→偏保守）；`maxSeparatorSize(index)` 改为按列类型**合成** worst-case 值（变长填满 length、定长任一合法值，13 类 TypeId 全覆盖）——不再依赖调用方实际值，insert（有记录）与 delete/purge（只有 SearchKey 可为前缀）共用；`deleteClustered`/`purgeDeleteMarkedClustered` 悲观回退改走 `descendPathDeleteSafeNode` |
| 关键修正 | `considerMerge` 的 root 判定由链下标（`depth-1==0`）改为 **页号**（`parentHandle.pageId()==rootPageId`，root 页号跨 split/shrink 稳定）——safe-node 截断后链顶可能是非 root 的 safe 节点，按下标判会对它误做 `shrinkRoot`（结构损坏）；`depth==0` 链顶语义更新为「真 root 或 delete-safe 节点均收工」 |
| 正确性 | safe 链顶「摘一最大指针后仍不欠载」⟹ merge 传播到链顶时 `isUnderfull` 必假、必停；root shrink 只在链顶恰为真 root（无 safe 节点、链=全路径）时可达——若存在 safe 节点则 root 不失指针、本就无需 shrink。结构上 safe 节点必有 ≥3 指针（2 指针页 free 恒过半 → 不 safe），摘一后 ≥2、不会产生 1-ptr 退化链顶 |
| 死锁自由 | 三类跨 SMO 等待边均无环：① 下降阻塞边（被等者不回头申请上方页）② 同父兄弟边（保留链连续 ⟹ 任意保留节点的父页恒被本 SMO 持有，同父兄弟只可能被 leaf-only 乐观算子持有，乐观算子终结、绝不再申请 latch）③ FIL 右邻边（split 右兄弟/merge 远邻修 prev，恒指向更右 leaf，全序）。论证写入 `descendPathSafeNode` Javadoc |
| 简化/非目标 | redistribute 在父页「删旧插新 pointer」时新 lowKey 更大可能溢出——**继承既有风险**（全路径 X 时代同样存在），safe-node 不加剧；SX 下降+restart-in-X、B-link/OLC 仍 deferred |
| 回归 | 固定 Gradle/JDK 全量 `test` 通过，181 suites / **865 tests**（864+1），0 失败/错误/跳过；0.12 merge/shrink/redistribute 与并发用例全数不倒退 |

## 2026-07-02 root SX 下降 + restart-in-X Review Log（0.13d 分片）

0.13d 第四步：兑现设计 §10.3 `ROOT_LATCHED_SX` + 「upgrade 失败则 release and restart」——悲观 SMO 下降起点由 root X 改 root SX，SX latch 基础设施（前一分片）首次接入 btree 生产路径。**至此 0.13d 的 SX latch + root retry 主体完成**；剩余 B-link/OLC（设计未规定）、btree 专用 redo。

| 项 | 内容 |
| --- | --- |
| TDD RED-GREEN | 先加 `pessimisticSmoUsesRootSharedExclusiveFirstPassAndRestartsOnlyWhenReachingRoot`（80 宽 key 行到 level≥2，断言 sx>0、restart>0、sx>restart + 全 key 有序）；RED：无 SX 首遍两计数恒 0，实现后 GREEN；delete safe-node 用例补增量断言（快照 level2 悲观 merge 走 SX 首遍且 B delete-safe 零重启，用 before/after 差值确定性判定） |
| 实现 | `descendPathSafeNode` 改两遍制：快照 `rootLevel>=2` → `descendOnce(root=SHARED_EXCLUSIVE)`（内部 child/leaf 恒 X、safe-node 截断照旧）；链顶≠root → 返回（本 SMO 全程未 X 过 root、读者从未被 root 阻塞）；链顶=root → `releaseChain`（下降只读、全链非 touched）→ `descendOnce(root=EXCLUSIVE)` 重启。level 0/1 树跳过 SX 首遍直取 X（此类树任何 SMO 必写 root，首遍必重启纯浪费）；快照陈旧只影响收益不影响正确性 |
| 正确性 | 重启前**零页写入**（下降只读）故整链可干净释放；重启即全新导航（chooseChild 重新执行），无需版本校验天然正确；重启至多一次（第二遍 root X 可写 root）。SX 与读者 S 并存 ⟹ SMO 下降窗口不再阻塞读者，root 内容在 SX 持有期不可被写（X 被排斥）⟹ chooseChild 导航稳定 |
| 死锁自由 | root SX/X 起点互斥保持「任一时刻只有一个 SMO 在 root 处」；restart 重取 root X 前已释放全链 ⟹ 无 hold-and-wait；其余三类等待边论证（下降阻塞/同父兄弟-乐观算子终结/FIL 右邻全序）与 safe-node 分片不变，写入 `descendPathSafeNode` Javadoc |
| 简化/非目标 | 无 index-level latch（root 页 latch 兼任 §10.1 的「root latch」资源，教学简化）；无原地 SX→X 升级；乐观/读路径不变；B-link/OLC 不做 |
| 回归 | 固定 Gradle/JDK 全量 `test` 通过，181 suites / **866 tests**（865+1），0 失败/错误/跳过；root split（level 2→3）/全删 shrink 到 level 0 等 root 级 SMO 用例全数经 restart 路径通过 |

## 2026-07-03 MTR page latch ordering Review Log（0.23a）

0.23a 只落 MTR 默认 page latch 全序守卫与现有系统设计需要的局部例外；不改变 commit ordering、redo record 分类或 savepoint 语义。

| 项 | 内容 |
| --- | --- |
| TDD RED-GREEN | 先加 `MiniTransactionTest` 3 例：高页后取低页违规、同页重入/释放后低页放行、`allowOutOfOrderPageLatch` 仅作用域内放行；RED 为缺少 API 编译失败，实现后目标测试通过 |
| 默认守卫 | `MiniTransaction.fix` 在 Buffer Pool 等待前检查仍持有的最大 PageId；独立多页必须按 `(spaceId,pageNo)` 升序，同页重入和 `releaseLatch`/savepoint 已释放页不参与；违规抛 `MtrStateException` |
| 局部例外 | `MtrLatchOrderScope` 只在带理由的 try-with-resources 中递增例外深度。生产例外集中在：B+Tree root/child/sibling hand-over-hand、SMO allocate/format/free；Undo grow/FIL 链读；UndoLogManager rseg page3 slot 持久登记。finalizer 清理由 FSP page0/page2 到 page3 遵守默认升序。测试白盒结构 walk 单独标注 test reason |
| 系统设计一致性 | 保留 undo-before-index 写协议（同一 MTR 先写 undo，再改聚簇页）、B+Tree 逻辑树序/FIL 链序、FSP 元数据不反向等待 index/undo 页 latch、rseg page3 不反向等待 undo 页 latch 等局部无环前提；不把例外下沉成全局关闭 |
| 回归与静态规则 | 固定 Gradle/JDK 全量 `test` 通过，**894 tests**，0 失败；精确生产代码扫描无 Java monitor 调用、无 `throw new IllegalArgumentException/RuntimeException`；`git diff --check` 仅 CRLF 工作区提示 |

## 2026-06-22 Undo Truncation 10-Pass Review Log

本切片完成后按源码、测试和磁盘/恢复不变量执行 10 遍独立复核；不是只对照 slice spec。

| Pass | Check dimension | Result | Notes |
| --- | --- | --- | --- |
| 1 | slice 决策逐项映射 | PASS（修正） | 补齐 page0 loader 共享 operation lease，明确默认/共享 controller 构造边界 |
| 2 | 锁序与资源释放 | PASS（修正） | 验证 S/X drain、MTR LIFO；补跨线程 lease close 拒绝，防止错误线程把 lease 标成已关闭 |
| 3 | WAL/marker/checkpoint 顺序 | PASS | marker commit 返回精确 end LSN；物理 truncate 前 `flushThrough` 保证 redo durable + checkpoint 覆盖 |
| 4 | FSP/inode 重建可用性 | PASS（补测） | page0/page2/extent0 重建后实际重新 createSegment + allocatePage 成功 |
| 5 | 状态机/磁盘兼容 | PASS | stable state code、magic/format、epoch/target/finish 往返；旧 UNDO 不猜 initial size |
| 6 | 错误与失败边界 | PASS（补测） | GENERAL、活动 inode、旧 UNDO、配置空间未打开均在物理缩短前失败 |
| 7 | API/依赖方向 | PASS | PageStore 仍 state/registry-free；recovery 通过 participant 扩展点，不让底层依赖 session/SQL |
| 8 | 真重启恢复 | PASS（修正） | 关闭第一套对象后重开 redo/data；发现并修复恢复后空 flush 把 durable LSN 从 recoveredTo 降到 0 的 bug |
| 9 | doublewrite/redo 连续性 | PASS | page0 先修、tail pageNo>=target 跳过；恢复后新 redo 批次从 recoveredTo 连续追加 |
| 10 | map/静态规则/全量回归 | PASS | 无直接 IllegalArgumentException/RuntimeException、无 synchronized/wait/notify、无 TODO/TBD；固定 Gradle 全量测试通过 |

## 2026-07-12 Per-operation Redo Budget 5-Pass Review Log

本切片按源码真实调用链复核五遍；未使用 GitNexus 或 Superpowers。

| Pass | Check dimension | Result | Notes |
| --- | --- | --- | --- |
| 1 | 尺寸公式 / 设计不变量 | PASS | zero/单块/跨块公式与真实 LogBlock codec 一致；删除 capacity/8；commit 只冻结一次 persisted records |
| 2 | 生产调用链 / map | PASS（修正） | 25 个生产匿名 begin 清零；读路径零预算、写路径显式 purpose；复核时修正 redo/MTR 小节四处旧 fixed-reservation 表述 |
| 3 | 并发 / 失败边界 | PASS | atomic outstanding ledger；append ownership transfer 与 close 同一 one-shot 门；timeout、physical oversize、预算低估均 fail-closed；无 Java monitor |
| 4 | 定向验证 | PASS | sizing、throttle、MTR strict/fail-stop、StorageEngine 小容量/flush 集成测试通过 |
| 5 | diff / 文档 / 全量回归 | PASS（修正） | 清理 backlog/current map 残留“下一步预算”表述；固定 JDK/Gradle `test --rerun-tasks`：218 suites / 1155 tests，0 failure/error/skip；`git diff --check` 仅行尾提示 |

## 2026-07-12 B+Tree Node / Root Structure Redo 5-Pass Review Log

本切片按源码真实写点、页布局、恢复边界和测试复核五遍；未使用 GitNexus 或 Superpowers。

| Pass | Check dimension | Result | Notes |
| --- | --- | --- | --- |
| 1 | 写点 / snapshot 几何 | PASS | root split/grow、non-root split、separator insert、merge、redistribute、root shrink 均在完整动作末尾捕获；header/used heap/directory 排除 free-space |
| 2 | 持久格式 / 过滤 / 恢复 | PASS（加固） | 复用稳定 kind 2/3/4；固定 header/root layout fail-closed；同值 physical bytes 才过滤；复核补 node area 不得跨 file trailer 的恢复校验与失败测试 |
| 3 | latch / 依赖 / 失败边界 | PASS | snapshot 只读且发生在既有 X latch 内，不新增锁、IO、等待或上层依赖；recovery 只 patch，不运行 B+Tree SMO；无 Java monitor/裸异常 |
| 4 | 结构分支测试 | PASS | root leaf split、internal root grow、non-root internal split、internal redistribute、merge/root shrink、codec/replay、非法布局均覆盖 |
| 5 | map / backlog / 全量回归 | PASS（修正） | 清理所有“node/root redo 未实现”旧表述；固定 JDK/Gradle `test --rerun-tasks`：218 suites / 1158 tests，0 failure/error/skip；`git diff --check` 仅行尾提示 |

## 2026-07-12 Domain-scaled Redo Budget 5-Pass Review Log

本切片按生产源码、inode 账本、MTR 锁序与真实测试报告复核五遍；未使用 GitNexus、Superpowers 或子代理。

| Pass | Check dimension | Result | Notes |
| --- | --- | --- | --- |
| 1 | 生产调用链 / blast radius | PASS | 六个动态 purpose 的生产调用全部改走 workload 入口；固定入口仅保留显式拒绝测试；`UndoSpaceAllocator` 的生产适配器与四个测试 wrapper 均已实现新只读端口 |
| 2 | 公式 / 溢出 / 单调性 | PASS（补测） | workload 合并、B+Tree height、DML 首写、terminal delta、fragment/extent 均有精确值与单调断言；所有乘加 checked；大 segment 超出单批上限会在写 MTR 前 fail-closed |
| 3 | latch / lease / 失败边界 | PASS | finalization 严格执行 page3+undo 预检提交、page2 plan 提交、写 MTR 三段；不跨返回持 page latch/fix；plan/预算失败发生在 `physicalMutationStarted` 前；无 Java monitor、裸异常或 TODO/TBD |
| 4 | 定向协作与异常验证 | PASS | DML normal/split、rollback、purge、四路 finalization、引擎恢复、真实 32 fragment+1 extent drop 与 stale segment identity 均通过；commit 继续执行 actual≤budget 精确结算 |
| 5 | map / backlog / 全量回归 | PASS | current map 与 backlog 已删除“下一步收紧 profile”旧状态；下一推荐切换到 0.21 Record 页内校验；固定 JDK/Gradle `test --rerun-tasks`：222 suites / 1169 tests，0 failure/error/skip；`git diff --check` 仅行尾提示 |

## 2026-07-12 Record Page Structure Validation 5-Pass Review Log

本切片按生产 open 调用链、现有页操作算法与真实测试报告复核五遍；未使用 GitNexus、Superpowers 或子代理。

| Pass | Check dimension | Result | Notes |
| --- | --- | --- | --- |
| 1 | 生产调用链 / blast radius | PASS | validator 只接 `IndexPageAccess.openIndexPageHandle` 一处；覆盖 LeafOnly 1 个与 SplitCapable 4 个直接入口；未修改 B+Tree 内 40 处 `recordPage()` 视图调用，create/format 路径独立 |
| 2 | 页内结构不变量 | PASS | header geometry、系统记录、链 0/环/越界、record length/live range、heapNo/nRecs、sentinel/internal owner/nOwned 均逐项实现并有失败测试；合法 insert/update/delete-mark/purge/reorganize 逐阶段通过 |
| 3 | latch / 依赖 / 异常边界 | PASS | 校验发生在既有 S/X fix 后且早于本次 B+Tree 解析/写入；只读 RecordPage，不新增锁/等待/IO，不反向依赖 api/btree/mtr；低层格式/边界异常保留 cause；无 Java monitor、裸异常或 TODO/TBD |
| 4 | 受影响回归 | PASS | 完整 record/B+Tree、api.index、storage DML、rollback/purge 与 StorageEngine 测试通过；leaf/internal、split/merge/root shrink、garbage heapNo 复用和恢复链均兼容 |
| 5 | map / backlog / 全量回归 | PASS | current map/backlog 已标记 0.21a 接线并把下一推荐切换为 0.21b GarbageList 账本；固定 JDK/Gradle `test --rerun-tasks`：223 suites / 1175 tests，0 failure/error/skip；`git diff --check` 仅行尾提示 |

## 2026-07-12 Record Page Garbage Accounting Validation 5-Pass Review Log

本切片按 FREE/GARBAGE 真实写点、B+Tree 空间决策与生产 open 调用链复核五遍；未使用 GitNexus、Superpowers 或子代理。

| Pass | Check dimension | Result | Notes |
| --- | --- | --- | --- |
| 1 | 生产调用链 / blast radius | PASS | 只扩展既有 validator，生产仍只有 `IndexPageAccess.openIndexPageHandle` 一个调用点；`HeapSpaceManager`、IndexPageAccess 与 B+Tree 40 处 `recordPage()` 均未改 |
| 2 | 账本公式 / 合法状态 | PASS | 核对 free/add、reuse/subtract、in-place shrink 三个写点；实现 `linkedFreeBytes<=GARBAGE<=heapSpan-liveRecordBytes`，覆盖 multi-fragment、first-fit skip、exact/oversized reuse、moved update、shrink 与 reorganize |
| 3 | identity / range / 异常边界 | PASS | FREE offset/type/length/环、live/free heapNo、free/free heapNo、live/free 物理区间与 checked byte sum 均 fail-closed；低层 cause 保留；无 Java monitor、裸异常、上层反向依赖或 TODO/TBD |
| 4 | 受影响回归 | PASS | 完整 record/B+Tree、api.index、storage DML、rollback/purge 与 StorageEngine 测试通过；underflow/merge-fit、split/merge/root shrink 和恢复链兼容 |
| 5 | map / backlog / 全量回归 | PASS | current map/backlog 已标记 0.21b 并把下一推荐切换为 0.21c charset/collation；固定 JDK/Gradle `test --rerun-tasks`：223 suites / 1180 tests，0 failure/error/skip；`git diff --check` 仅行尾提示 |

## 2026-07-12 Record Charset / Collation / Ordering 5-Pass Review Log

本切片按生产 registry 注入链、默认磁盘兼容与 leaf/node-pointer 排序矩阵复核五遍；未使用 GitNexus、Superpowers 或子代理。

| Pass | Check dimension | Result | Notes |
| --- | --- | --- | --- |
| 1 | 生产调用链 / blast radius | PASS | `StorageEngine` 仍单点创建 `TypeCodecRegistry`；16 个生产文件的既有注入 API 未改；只在字符 codec 内持只读 `CharacterTypeRegistry`，两个高扇出比较器共同持 `EncodedKeyPartComparator` |
| 2 | charset / collation / 兼容性 | PASS | UTF8/LATIN1 与三种 collation 使用显式 stable id；四个合法 pair 精确注册、错配拒绝；encoder/decoder 均 REPORT；既有 UTF8+BINARY factory、payload、page/undo/redo 格式与 binary 次序不变 |
| 3 | NULL / direction / prefix 同序 | PASS | 单一比较器负责 NULL、byte-prefix、codec/collation 与 ASC/DESC sign；Record/SearchKey 两条链保留哨兵与短 key；BINARY/ASCII_CI × full/prefix × ASC/DESC × NULL 矩阵及 leaf/node sign 一致均有测试 |
| 4 | 依赖 / 异常 / 并发纪律 | PASS | Record 未反向依赖 SQL/DD/session；无运行时 registry mutation、Java monitor、裸异常或 TODO/TBD；不可映射/损坏字符与缺失 pair 均抛保留 cause 的领域异常；Record/B+Tree/Undo 协作回归通过 |
| 5 | map / backlog / 全量回归 | PASS | current map/backlog 已标记 0.21c 并把下一推荐切换为 0.21d schema-aware key 顺序校验；固定 JDK/Gradle `test --rerun-tasks`：225 suites / 1191 tests，0 failure/error/skip；`git diff --check` 仅行尾提示 |

## 2026-07-13 Record Schema-aware Key Order Validation 5-Pass Review Log

本切片按物理/schema 校验分层、leaf/internal 生产打开链与损坏构造复核五遍；遵守用户要求未使用 GitNexus、Superpowers 或子代理。

| Pass | Check dimension | Result | Notes |
| --- | --- | --- | --- |
| 1 | 生产调用链 / blast radius | PASS | LeafOnly 只有 `openRootLeaf` 一个入口；SplitCapable 12 个 existing-page 调用统一经带 `BTreeIndex` 的 `openBTreePageOutOfOrder`，另 3 个直开点均为 fresh create/format 空页；高扇出 `IndexPageAccess` API 未改 |
| 2 | schema / record type / key order | PASS | 实际 page level=0 使用 leaf schema/keyDef/CONVENTIONAL，level>0 使用派生 node-pointer schema/NODE_POINTER；相邻比较允许重复、prefix/collation 等价与 delete-mark，拒绝逆序和类型混入 |
| 3 | charset / 物理损坏 / 异常边界 | PASS | CHAR/VARCHAR 在 byte-prefix 前严格解码完整字段，单记录坏 UTF-8 也拒绝并保留 cause；`recordOffsetsInOrder` 物理链错误继续保持 `PageDirectoryCorruptedException`，不被语义异常遮蔽 |
| 4 | latch / redo / 依赖纪律 | PASS | 校验只在既有 S/X page latch 内只读执行，不新增 latch/lock/wait、dirty、redo 或修复动作；Record 不依赖 B+Tree 元数据类型，物理 validator 继续 schema-free；无 Java monitor、裸异常或 TODO/TBD |
| 5 | 测试 / map / backlog | PASS | Record/B+Tree 包回归及固定 JDK/Gradle 全量 `test --rerun-tasks` 通过：226 suites / 1203 tests，0 failure/error/skip；current map/backlog 标记 0.21d 并把下一推荐切换为 0.21e inline scalar types |

## 2026-07-13 Record Inline Temporal Types 5-Pass Review Log

本切片按 TypeId 高扇出、保序字节兼容、共享生产 registry 与跨模块往返复核五遍；遵守用户要求未使用 GitNexus、Superpowers 或子代理。

| Pass | Check dimension | Result | Notes |
| --- | --- | --- | --- |
| 1 | 生产调用链 / blast radius | PASS | 新增 TypeId 前逐一扫描穷尽分支：`ColumnType` metadata、`TypeCodecRegistry`、`KeyPrefix` 与 SplitCapable 最坏键预算；新常量追加在 DATETIME 后，既有 enum 顺序不变；Record/Undo/B+Tree 无 registry API 改动 |
| 2 | 物理编码 / 范围 / 兼容性 | PASS | TIME/TIMESTAMP 为 8B signed sign-flip big-endian；YEAR 为 2B unsigned big-endian 且只接受 0..65535；DATE/DATETIME 的 epoch=0 golden bytes 保持不变，完整 long/边界/保序均有测试 |
| 3 | Record / Undo / B+Tree 协作 | PASS | Record 整记录、Undo self-framing cluster key、SearchKeyComparator ASC/DESC 均经共享 registry 验证；split redo budget 的 worst-case value 已覆盖三种新 fixed 类型；KeyPrefix default 继续拒绝所有非 byte 类型 |
| 4 | 分层 / 异常 / 并发纪律 | PASS | 时区、MySQL TIME/YEAR SQL 合法范围留在 SQL/session；Record 只校验物理 kind/type/range；无页格式、redo/undo framing、latch/lock/wait 或可变共享状态变化；无 Java monitor、裸异常或反向依赖 |
| 5 | 测试 / map / backlog | PASS | 固定 JDK/Gradle 全量 `test --rerun-tasks`：226 suites / 1210 tests，0 failure/error/skip；slice 为 51 行；current map/backlog 标记 0.21e1 并把下一推荐切换为 0.21e2 BIT(n) |

## 2026-07-13 Record 0.21 Completion 10-Pass Review Log

本轮按源码重新复核 0.21e2/f/g/h 与此前 a-e1 的组合结果；未使用 GitNexus、Superpowers 或子代理。

| Pass | Dimension | Result | Evidence / correction |
| --- | --- | --- | --- |
| 1 | 高扇出 / 兼容 | PASS | `TypeId` 只在尾部追加到 28 类；扫描并补齐 `ColumnType`、registry、B+Tree worst-case key 的 exhaustive switch；既有磁盘编码与 stable id 不变 |
| 2 | 依赖方向 | PASS | Record 只持 `LobReference`/envelope；跨 record/buf/FSP 的协作位于 `storage.api.lob.LobStorage`；FSP/redo/recovery 不依赖 SQL/session |
| 3 | inline 类型 | PASS | BIT 1..64 canonical、ENUM 1-based ordinal、SET 64-bit bitmap 均由 Record/Undo/leaf/node-pointer 共享 codec，并覆盖非法范围与 prefix 拒绝 |
| 4 | charset / 排序 | PASS | Unicode V1 使用新 stable collation id、严格 UTF-8 与 code-point fallback；leaf/node 共用 `compareKeyPart`，UTF-8 prefix 不截断 code point |
| 5 | LOB 页链 / 锁序 | PASS | write 先按 page0→page2 X 复核 LOB segment，再 reserve/allocate；read 逐页 S latch 后立即 release；free 先全链校验再 FSP 修改，无 row-lock wait |
| 6 | WAL / recovery | PASS | 分配产生 FSP allocation intent + PAGE_INIT(BLOB)，body/link 产生 PAGE_BYTES；free 产生 FSP free + PAGE_INIT(ALLOCATED)；通用 dispatcher 实际重放 BLOB type/body |
| 7 | 损坏 / 失败路径 | PASS | 页 type/magic/version/index/link/count/segment/length/canonical chunk/CRC 全部 fail-closed；错误 segment 在 reserve/allocation 前拒绝；部分分配清理保留 suppressed cause |
| 8 | Record / Undo 协作 | PASS | `OVERFLOW_CAPABLE` 进入变长目录；inline/external envelope 经 Record round-trip；UPDATE undo 只保存 external reference，不复制 payload；undo record 自身跨页仍明确为 gap |
| 9 | map / reserved / non-goal | PASS | current map 实线逐项回到 `StorageEngine→LobStorage→DiskSpaceManager/LobPage/TypeCodecRegistry` 核对；移除已被 LOB comparator 使用的旧 reserved exception；自动 DML ownership 保持明确 gap |
| 10 | 文档 / 回归 | PASS | slice 行数：temporal 51、BIT 50、ENUM/SET 50、Unicode 51、LOB 49；固定 JDK/Gradle 全量 `test --rerun-tasks`：231 suites / 1242 tests，0 failure/error/skip；并收紧 page-cleaner 既有 restart 测试的 replacement-publication 等待条件；`git diff --check` 仅行尾提示 |

## 2026-07-14 Independent INSERT / UPDATE Undo Log v2 5-Pass Review Log

本切片按页格式、事务状态、原子终结、恢复清理和源码真实调用链独立复核五遍；未使用 GitNexus、Superpowers 或子代理。

| Pass | Dimension | Result | Evidence / correction |
| --- | --- | --- | --- |
| 1 | 页格式 / 类型守门 | PASS | 普通 UNDO 升 v2，first/chain 每页持久 kind；v1/未知版本 fail-closed；append、direct read、segment read/traversal 都拒绝 record type 与 kind 错配 |
| 2 | 事务状态 / 写计划 | PASS（加固） | `UndoContext` 用双 `UndoLogBinding` + 全局 `lastUndoNo`；I-U-I/U-I-U 验证局部 predecessor 跨序号间隙；复核补 binding 非空 head 的 RollPointer kind 校验，slotCapacity=1 第二类首写无污染失败 |
| 3 | commit / batch finalization | PASS（加固） | mixed commit 同 MTR drop/clear INSERT、标 UPDATE COMMITTED、单 terminal delta；双段 rollback all-or-none lease；复核补 batch lease 重复 physical/complete 防护并删除旧单段终结入口 |
| 4 | rollback / recovery / purge | PASS（加固） | partial/full/recovery 从双 head 按全局 undoNo 归并；partial 单 marker batch；恢复按 creator 聚合双 ACTIVE 后原子清两 slot；purge 只收 COMMITTED UPDATE；复核把 live persistent/context 头漂移提前到任何 inverse 前拒绝 |
| 5 | 文档 / 静态规则 / 全量回归 | PASS | slim slice 51 行；current map、两份厚设计、DML 设计与 backlog 已同步；固定 JDK/Gradle `test --rerun-tasks`：233 suites / 1261 tests，0 failure/error/skip；无新增 monitor、裸异常、TODO/TBD；`git diff --check` 通过（仅行尾转换提示） |

## 2026-07-14 Bounded Cached Undo Segment Reuse 5-Pass Review Log

本切片按生产调用链、page3 owner、并发锁序、恢复/截断边界和真实测试报告复核五遍；未使用 GitNexus、
Superpowers 或子代理。

| Pass | Dimension | Result | Evidence / correction |
| --- | --- | --- | --- |
| 1 | 生产调用链 / blast radius | PASS | `StorageEngine` 单点创建共享 cache directory 并注入 manager/finalizer/truncate；生产 segment drop 仅剩 finalizer 与 truncate coordinator；配置/format/read 均显式传同一容量 |
| 2 | page3 / owner / reuse | PASS（修正） | page3 v2 精确校验 slot+双 cache 布局、连续 count 与跨 active/cache owner 唯一性；active↔cache 同 MTR；首页 reset/activate 全字段 redo 化；复核清理了仍写“始终 drop”的旧注释 |
| 3 | 并发 / latch / 失败边界 | PASS（加固） | cache 短 `ReentrantLock` 内无 IO；per-kind push/pop 与全局 drain 均用 lease/fence；写序 FSP page0/page2→page3→undo 首页；复核把 truncate 前置校验从仅 count 加固为完整双栈顺序比对，并补同数量不同 owner 回归 |
| 4 | recovery / truncate / crash | PASS | 恢复分别短读 page3、cached 首页和 FSP inode，只接受空单 fragment owner，cache 不进入事务 reconciliation；truncate 先拒绝 active、每 MTR 最多回收 8 段、marker 后恢复断言空 owner，rebuild 同批恢复 page0/page2/page3 |
| 5 | 文档 / 静态规则 / 全量回归 | PASS | slim slice 49 行；current map、backlog 与相关厚设计已同步；固定 JDK/Gradle `test --rerun-tasks`：235 suites / 1276 tests，0 failure/error/skip；无新增 monitor、裸异常或 TODO/TBD；`git diff --check` 通过（仅行尾转换提示） |

## 2026-07-14 Persistent Rollback-Segment History List v1 5-Pass Review Log

本切片按生产调用链、格式/redo、并发失败边界、恢复/truncate 与测试文档五个事实面复核；未使用 GitNexus、
Superpowers 或子代理。每轮都回到当前源码和测试，不根据计划文件推断生产接线。

| Pass | Dimension | Result | Evidence / correction |
| --- | --- | --- | --- |
| 1 | 生产调用链 / blast radius | PASS | 生产 history append/unlink 只由 `UndoSegmentFinalizer` 调 page3 repository 与 first-page access；`UndoLogManager`/`PurgeCoordinator` 只取得 transition lease；恢复唯一发布入口是 `StorageEngine→PersistentHistoryRecovery→HistoryList.restore`；无旧 submit/complete 旁路 |
| 2 | page3/UNDO v3 / redo / latch order | PASS（修正） | page3 base offsets 66/74/82/90、slot base 98，UNDO links 120/128、record area 136；stable redo codes 8/9；最终 MTR 固定 FSP→page3→普通 first-page 全序；复核修正 `UndoPage`、package-info 与 truncate 的旧 v2/120 注释 |
| 3 | 并发 / purge 安全 / crash fence | PASS（加固） | `HistoryList` 短显式锁 + Condition timeout，锁不跨 IO；pre-physical close 可重试、post-physical close fail-stop；并发 purge/remove 与 commit/append 无丢更新；复核新增 creator 仍在 active table 时即使无 live ReadView 也拒绝 purge，失败测试先红后绿 |
| 4 | recovery / truncate / counter | PASS（加固） | 按物理 next 链逐节点短 MTR 校验 exact length、双向链接、cycle、slot/state/identity；all-purged 仍取 `lastTransactionNo+1`，溢出拒绝；marker 前/TRUNCATING 均守 history 空；复核补 ACTIVE 二次快照的 state/kind/creator/commitNo 交叉校验，失败测试先红后绿 |
| 5 | 文档 / 静态规则 / 全量回归 | PASS | slim slice 49 行；current map、backlog、transaction/undo/DML 设计同步；固定 JDK 25.0.2 + Gradle 9.5.1 `test --rerun-tasks`：236 suites / **1297 tests**，0 failure/error/skip；生产源码无 monitor 调用、裸 `IllegalArgumentException/RuntimeException` 或 feature TODO/TBD；`git diff --check` 无错误（仅行尾转换提示） |
