# Storage Backlog（依赖排序的缺口清单）

> 长期参考资产，不是实现计划。聚合九份厚设计文档的目标架构与当前实现的差距，按依赖关系排序，
> 用于"开工前定序"。**权威缺口与实现状态以 `current-implementation-map.md` 和源码为准**；本文件只做跨文档、
> 按依赖的归并排序。条目落地后应从对应 Tier 中删除或下移。
>
> 覆盖文档：`innodb-flush-checkpoint-doublewrite-design.md`(F)、`innodb-undo-log-purge-design.md`(U)、
> `innodb-transaction-mvcc-design.md`(T)、`innodb-crash-recovery-design.md`(R)、`innodb-btree-design.md`(B)、
> `innodb-buffer-pool-design.md`(BP)、`innodb-disk-manager-design.md`(D)、`innodb-record-design.md`(Rec)、
> `innodb-redo-log-design.md`(Redo)。
> 其余模块（DD/DDL、lock observability）只在作为依赖出现时引用。
>
> 最近校对：2026-07-24（基于当前源码与 `current-implementation-map.md`：唯一二级 delete-marked current-read、
> comparison/composite/full-scan、
> 四隔离级别、命名 SAVEPOINT、persistent server XA、受控 DISCARD/IMPORT、table options/defaults 与
> general blocking ALTER shadow rebuild、对象级 force recovery + trusted replacement、Online ADD/DROP INDEX、
> marker v5（兼容 v1-v4）、原子多表/级联 DROP、AUTO_INCREMENT page0 high-water、分堆/索引排序、
> instant metadata、multi-index INPLACE与online shadow ALTER均已进入
> 生产链，purge 驱动单 undo space 自动截断已闭环；temporary undo、复制binlog与主键/类型/foreign/generated变更仍未完成）。

## 九份文档的当前完成度（粗估，表达"核心在、外围缺"）

| 文档 | 已闭环的核心 | 主要缺口 | 粗估 |
| --- | --- | --- | --- |
| F flush/checkpoint/doublewrite | FLUSH_LIST / SINGLE_PAGE / SHUTDOWN / **LRU_FLUSH(WAL 安全淘汰)**、WAL gate、recoverable doublewrite 生产接线、**bounded doublewrite slot reuse（0.5）**、仓储级 `DoublewriteBatch` 连续 slot 原语、**生产 FlushList/LRU batch dispatch（v1）**、**FlushList/LRU 双物理 doublewrite channel**、**engine 配置化 `DoublewriteMode`**、**DETECT_ONLY metadata/report（0.7）**、fuzzy checkpoint + 持久 label、PageCleanerWorker、**速率/IO capacity adaptive flush（v1）**、**0.6b 前台 reservation throttle**、**legacy BufferPool flush API 移除（2026-07-05）** | 全空间 doublewrite discovery、动态 slot/IO 配置 | ~88% |
| U undo/purge | 既有 undo/history/finalization 能力 + **secondary/LOB tail、statement/named-savepoint/full/recovery inverse、version-safe multi-worker purge、affected-table history、real recovery resume、purge 驱动自动 truncate 与 unavailable-target recovery skip** | 多 rseg/tablespace、多页 free shrink、temporary undo | ~97% |
| T transaction/mvcc | 既有事务/ReadView/LockManager 能力 + **四隔离级别、命名 SAVEPOINT retention boundary、comparison/composite/full-scan read/locking DML、persistent server XA + storage PREPARED** | RC residual-miss 提前释放、global gap 精确化、XA active branch migration/compaction | ~98% |
| R crash-recovery | 既有恢复阶段 + **catalog-loss recovery、persistent XA决议、multi-index rollback、real RESUME_PURGE、table/index/transfer/rebuild/Online ADD/DROP、通用INPLACE/SHADOW ALTER recovery、ACTIVE SDI reconcile、对象级force isolation/export/trusted replacement** | worker/lock diagnostics、全实例/系统与undo空间checksum scrub | ~99% |
| B btree | 既有多层结构能力 + **紧凑 secondary layout、insert/mark/revive/remove/prefix scan、root snapshot、multi-level secondary shrink** | B-link/OLC、global gap 精确化、PAGE_MAX_TRX_ID/segment header、leaf row semantic redo | ~89% |
| BP buffer-pool | get/new/fix、page latch、`PageGuard`、page hash/free/flush list、WAL-safe eviction、DIRTY_PENDING/EVICTING/STALE、midpoint LRU、LOADING IO 边界、read-ahead、多 instance、**DROP/DISCARD/IMPORT/rebuild stale-frame generation 与 drain/invalidate** | read-ahead/warmup 动态配置与更细 IO 调度 | ~95% |
| D disk-manager | fil/fsp/segment/extent/page 分配、page0/FSP checksum、autoextend、undo truncation、GENERAL lifecycle、reservation、fsync/preallocation seam、FSP redo、**DD discovery + DROP/DISCARD/IMPORT exact identity/path lifecycle + fixed page3 SDI + AUTO_INCREMENT high-water** | 平台 native preallocation、动态 reserveFactor/IO capacity、跨设备 transfer 由 admin 预处理 | ~95% |
| Rec record | `TableSchema`/`IndexKeyDef`/**28 类 `TypeId`**、字段 codec、record header/PageDirectory/hidden columns、页内全套原语、undo old image codec、**0.21a-h validator/排序/inline scalar/BIT/ENUM/SET/Unicode weight/TEXT-BLOB-JSON envelope/off-page LOB chain**、**LOB UPDATE/DELETE replacement ownership+purge** | 完整 UCA/更多 charset、MySQL binary JSON、PAGE_MAX_TRX_ID/segment header | ~94% |
| Redo redo-log | `RedoLogManager` 内存/durable 双模式、9 类 redo record 编码、append/write/flush、后台 flusher、written/closed tracker、WAL/checkpoint、LogBlock v1、ring/control v2、连续恢复扫描、page/trx handlers、capacity throttle、DurabilityPolicy、operation budget、**B+Tree sibling/node/root page-local delta** | 按 height/segment plan snapshot 收紧预算 profile、leaf row 等更细 semantic redo | ~88% |
| DD/DDL | 既有DD/DDL能力 + **catalog-loss recovery、table options/defaults/comment/generation、purge-aware DROP、DDL log v5 batch manifest、原子多表/级联 DROP、受控transfer、Online DDL可观察取消、Online ADD/DROP INDEX、instant metadata、multi-index INPLACE、online shadow ALTER/recovery、recovery unavailable/discarded lifecycle、op8/9/10与HMAC backup/import** | binlog、主键/类型/foreign/generated ALTER、catalog B+Tree/redo化、跨实例recovery trust | ~99% |

当前 storage、DD、进程内 SQL Session 与 persistent XA 组合根已经闭环。SQL 支持 comparison/composite/full
scan、ORDER BY/LIMIT、批量 INSERT/AUTO_INCREMENT、基础 schema/table DDL、四隔离级别、命名 SAVEPOINT、
XA、受控 tablespace transfer 和通用Online ALTER。主要跨层卡点转为：
复制binlog、主键/类型/foreign/generated ALTER，以及必须等待临时表
owner/lifecycle 的 **temporary undo**。

## Tier 0 — 现在可独立做（storage 内闭环，无新上层依赖）

按杠杆从高到低。

| # | 项 | 源 | 规模 | 价值 / 备注 |
| --- | --- | --- | --- | --- |
| 0.1 | ✅ **后台 redo flusher 已接**（`RedoFlushWorker`，2026-06-24）——周期/on-demand 驱动 `redo.flush()`，淘汰/flush 不再因 redo 未 durable 长时间跳过。✅ **append/fsync 拆锁 + recent_written/recent_closed tracker + closedLsn checkpoint 修正已接**（2026-06-29）：`RedoLogManager` 以 state lock 分配 LSN/维护 pending，以 `ioLock` 串行 write/fsync；append 不再被 fsync 持有状态锁阻塞；MTR dirty 发布后 `markClosed`，checkpoint 读 `closedLsn`。✅ **commit durability 原语已下沉到 0.20a DurabilityPolicy 并由 2.1 storage DML facade 消费**（2026-07-04），不再作为 Tier 0 独立项 | F/T/Redo | — | 后台 flush、并发边界、checkpoint 边界与 commit durability 策略抽象已闭合 |
| 0.2 | ✅ **recoverable doublewrite 已接进 engine**（2026-06-24~07-18）：前向 strategy + E2 scanner、候选过滤、`DoublewriteMode` engine 配置与双 channel 均已接；e2e torn-page 恢复通过。**剩余**：全空间 discovery | F | — | R 的 1.1 REPAIR_DOUBLEWRITE 已随本片落地 |
| 0.3 | ✅ **持久 rseg page3 v4 + owner/history/free CAS + batch atomic finalization 已接**（2026-06-24~2026-07-15）：active slots、双 cache 栈、history base/high-water 与 free head/tail/length 同页持久；单页 segment 按 cache→free→drop 终结、按 same-kind cache→free FIFO→fresh 获取；恢复严格校验 history/free 双链、FSP identity 与 owner 去重；truncate 统一 drain cache/free 并 rebuild page3 v4。**剩余**：多页 free shrink、多 rseg | U/T | — | active/cache/free owner 唯一；mixed commit、rollback、purge、recovery 与 truncate 不暴露无归属 inode |
| 0.4 | ✅ **后台/恢复 purge + 自动 undo truncate 已接**（2026-06-24~2026-07-22）：`PurgeTarget` + driver；DD table resolver 驱动 secondary safety/physical tasks；`PurgeWorkerPool` 以 affected-table completion DAG 执行有界物理前缀，dispatcher strict-head finalization 后发布 table counters；recovery RESUME_PURGE 真正运行 batch；成功 batch 后 scheduler 按默认 1 extent/30s 策略零等待尝试单系统 undo space，普通忙 deferred、真实存储失败 fail-stop。剩余：跨 rseg blocked-head 与多 undo space 候选调度 | U/T | — | 二级索引协调、multi-worker、DROP barrier 与 crash-safe 物理回收已闭环 |
| 0.5 | ✅ **bounded doublewrite slot reuse + 仓储级 `DoublewriteBatch` 已接**（2026-06-29），✅ **生产 FlushList/LRU batch dispatch v1 已接**（2026-07-18），✅ **双物理 channel + source-aware recovery + 部分失败 reservation 回收已接**（2026-07-18）：两文件独立 slot/force，恢复按最高 pageLSN 合并副本，旧单文件只作兼容输入。**剩余**：全空间 discovery、动态 slot/IO 配置 |
| 0.6 | ✅ **0.6a 比例 adaptive flush**、✅ **0.6b 前台 reservation throttle**、✅ **0.20c operation budget**，✅ **redo/flush 速率 + IO capacity adaptive v1 已接**（2026-07-18）：采样器、速率缺口补偿、ASYNC idle cap、free ratio LRU 分配；neighbor 和动态 engine config 留后续 | F | — | admission 仍早于 page latch/fix/FSP lease；WAL/checkpoint 边界不变 |
| 0.7 | 碎片打包：✅ `DETECT_ONLY` metadata/report 已接（2026-07-03：`DetectOnlyDoublewriteStrategy` + detect-only slot 枚举 + `DoublewriteRecoveryScanner.scanPageIfNeeded` + `RecoveryReport.detectedOnlyPageCount`；scanner 兼容 v1 full-copy，新写 full-copy/detect-only 统一 v2 header；engine 默认仍 recoverable full-copy）、✅ PageCleaner supervisor 重启 + metrics snapshot 已接（2026-07-03：`PageCleanerSupervisor` 托管/有限重启 worker，`PageCleanerMetricsSnapshot` 暴露诊断并保留历史 lastCycle 语义）、✅ legacy `BufferPool.flush/flushAll` API 已移除（2026-07-05：dirty page 物理写出统一走 FlushCoordinator；无 flusher 脏 victim 显式失败）、✅ `drainTablespace` busy-wait 已改 dirty-state condition 唤醒（2026-07-03：`BufferPool.awaitDirtyStateChange`，guard release/flush/reset signal；等待 timeout 后回环重扫 dirty 谓词；`flushThrough` 仍保留短 park）、✅ `RecoveryMode.READ_ONLY_VALIDATE` 已接（2026-07-06：scan-only doublewrite/redo、`RecoveryState.READ_ONLY`、`EngineConfig.withRecoveryMode`、`StorageEngine` read-only lifecycle）、✅ `RecoveryMode.FORCE_SKIP_CORRUPT_TABLESPACE` 显式 SpaceId 集合已接（2026-07-08：`RecoverySkipPolicy`、redo apply skip summary、doublewrite/redo/reconcile 过滤、`EngineConfig.withForceSkipRecovery`、StorageEngine 只打开 non-skipped recovery tablespace） | F/R | — | 0.7 恢复/诊断碎片包已收束；剩余对象级 force recovery 依赖 DD/discovery，不再归入 Tier 0 |
| 0.8 | ✅ **Midpoint LRU（old/new 子链）+ 扫描抗污染已落**（2026-06-25，Phase A）：`MidpointLruReplacementPolicy` 读入进 old 头、`oldBlocksTime`(注入毫秒时钟)提升窗 + `youngDistanceThreshold`(young 1/4)抗抖动，默认接线、`largeScanDoesNotEvictHotWorkingSet` 验证；删除 plain `LruReplacementPolicy`。**剩余**：read-ahead-aware 访问型分类、`oldBlocksPct` 容量配比再平衡（随 0.10） | BP | 中 | buffer pool 质量最直接提升 |
| 0.9 | ✅ **per-frame IO/loading 状态 + 显式 FrameStateMachine 已落**（2026-06-25，Phase B）：`BufferFrameState`(FREE/LOADING/CLEAN/DIRTY/FLUSHING)+`FrameStateMachine`；miss 读盘移出 `poolLock`（LOADING 占位 + `PageLoadFuture` 有界等待，不同页并发读/同页只读一次/失败清占位/超时·中断不悬挂）；FLUSHING 与 dirty 正交；13.1b-pre 已把 legacy `flush/flushAll` 与 no-flusher victim fallback 收敛为 snapshot 后锁外 PageStore 写盘。**剩余**：DIRTY_PENDING/EVICTING/STALE 态 | BP | 中-大 | buffer pool 最大并发瓶颈已解（单 `poolLock` 不再串行 miss 盘 IO）|
| 0.10 | ✅ **0.10a linear read-ahead 已落**（2026-06-30）：`BufferPool.prefetch`(free-frame-only 载入 old、不 fix/不提升) + `LinearReadAheadTracker`(单顺序流，同 extent 连续达 threshold→预取下一 extent) + `ReadAheadService`(`ReadAheadHook`+有界队列+单 worker) + `LruBufferPool.attachReadAheadHook`/`getPage` 回调 + `StorageEngine` 后台启停（threshold 56、门控 `backgroundFlushEnabled`）。✅ **0.10b warmup dump/load 已落**（2026-06-30）：`BufferPool.residentPageIds` + `BufferPoolWarmupService`(dump 热页定位→文件 magic+crc32 / load 读回→prefetch，缺失/损坏 no-op) + `StorageEngine` close 写 dump、open 预取。✅ **0.10c random read-ahead 已落（默认禁用）**（2026-06-30）：`BufferPool.residentCountInRange`(page hash 短锁内逐页查区间驻留数) + `RandomReadAheadDetector`(同 extent 驻留数达 threshold→补取整 extent、bounded recent 窗去重) + `ReadAheadService` 4 参构造增 `randomThreshold`(0=禁用→零额外开销，命中入队、检测异常吞掉) + `StorageEngine` 以 `RANDOM_READ_AHEAD_THRESHOLD=0` 构造（对齐 MySQL `innodb_random_read_ahead=OFF`，生产启用留 config）。✅ **0.10d 多 instance 分片 + 专用 PageHashTable 已落（默认 N=1）**（2026-06-30）：`LruBufferPool` 转 facade + `BufferPoolRouter`(`hash(PageId)%N` 确定路由) + per-shard `BufferPoolInstance`；单页操作路由、跨切面查询逐分片聚合（dirty 候选合并按 oldestLSN 升序 / oldest LSN 全局 min / residentCount/range 求和）；`invalidateTablespace` 两阶段 all-or-nothing；无跨分片 stealing；容量 base+前 r 个+1 切分(capacity≥N)；`EngineConfig.bufferPoolInstanceCount`(默认 1) 经 `StorageEngine` 构造（生产 N=1）。✅ **13.1a metadata lock boundary prep 已落**（2026-07-02）：`BufferPoolInstanceLatchSet` 统一 lock/condition 入口，并用 `BufferPoolLatchViolationException` 守卫 page read、PageLoadFuture wait、dirty victim flush 前必须释放内部锁。✅ **13.1b-pre legacy flush 收敛已落**（2026-07-02）：旧路径已收敛到 snapshot/dirtyVersion 协议。✅ **13.1c pageHashLock + frameMutex 已落**（2026-07-03）：`PageHashTable` 由 `pageHashLock` 保护，`BufferFrame` 元数据由 `frameMutex` 保护，IO/wait/dirty victim 前断言无内部锁。✅ **13.1d free/LRU/flush 子锁 + 真实 flush list 已落**（2026-07-04）：`freeListLock` 保护空闲队列、`lruListLock` 保护 midpoint LRU、`flushListLock` 保护 `DirtyPageList`；dirty 候选从 flush list 快照 + frame 复核产生，FLUSHING 页留链约束 checkpoint。✅ **legacy `BufferPool.flush/flushAll` API 移除已落**（2026-07-05）：dirty page 物理写出统一走 FlushCoordinator，无 flusher 脏 victim 显式失败。**剩余**：`DIRTY_PENDING`/`EVICTING`/`STALE` 状态细化 | BP | 各中 | 0.10 全落；0.22 stale 校验已单列落地；13.1d 已完成 list/flush 子锁与真实 flush list；legacy direct-write flush 已移除；latch 排序单列 0.23 |
| 0.11 | ✅ **parent split（解锁树高 >1）已落**（2026-06-25）：自底向上 split 传播 + 内部页 split + 原地 root split（任意层 +1）+ N 层 `findLeaf` 导航；所有现有操作（insert/lookup/scan/delete/replace/deleteMark/purge）多层可用；`nonLeafSegment` 分配内部/root-split 子页；删 `BTreeParentSplitRequiredException`。验证 level-2/level-3、多层 scan、多层聚簇写 | B | 中-大 | 0.12/0.12b 已基于此前置完成；后续转 0.13/2.7 |
| 0.12 | ✅ **merge + 原地 root shrink 已落**（2026-06-29）：删/purge 后 underflow（可回收空闲>页半）→ merge 同父相邻兄弟（reorganize survivor + 并入 victim + 摘 parent pointer + `disk.freePage`）→ 自底向上传播 → root 剩 1 pointer 原地 shrink（吸收唯一 child、树高-1、级联）；导航改按 root 页实际 level（`openRoot` 去 level 断言）；`BTreeDeleteResult` 带 `indexAfter`/`freedPages` | B | 中 | 多层树收缩 + 空页回收已解；min-key-pointer 约定下 survivor 父 pointer key 不变（无 separator 更新） |
| 0.12b | ✅ **redistribute 对半再平衡已落**（2026-06-29）：`considerMerge` 的 `!mergeFits` 分支由「留欠载」改 `redistribute`——合并相邻同父对全部条目对半重分到两页（`splitRows`/`splitPointers`）、只更新 parent 中 right 成员 lowKey（min-key-pointer 下 left 不变）、不删页/不传播/不改树高；leaf+internal 统一。消除 merge fit 不下时的欠载/1-pointer 退化页 | B | 中 | 借法=对半（非按记录数阈值）；进入条件保证 total>1 页、平分必双健康 |
| 0.13a | ✅ **写路径 latch coupling（乐观→悲观，insert+deleteClustered）已落**（2026-06-30）：`MtrMemo.release`(非 LIFO 按身份释放)+`MiniTransaction.releaseLatch`(touched 防护)+`IndexPageAccess.releaseHandle`；`SplitCapableBTreeIndexService.descendOptimistic`(内部层 S-crab、leaf X)；`insert`→`tryOptimisticInsert`(inserter 溢出前零改动试错)/`deleteClustered`→`tryOptimisticDelete`(`deleteWouldUnderflow` 预判、偏保守)，unsafe 零页修改释放 leaf X 回退悲观全 X（split/merge 引擎不变）；并发正确性依赖结构变更走悲观全路径 X 持 root X 到 commit（tree-wide 串行于 root）；诊断计数 + bulk/并发/欠载回退测试（815 tests） | B | 中 | 生产 SplitCapable 服务；读路径与其余写算子仍全路径 latch |
| 0.13b | ✅ **写路径 latch coupling 收尾（其余聚簇写算子）已落**（2026-07-01）：`replaceClustered`/`setClusteredDeleteMark`/`purgeDeleteMarkedClustered` 走 `tryOptimistic*`+`descendOptimistic`。replace/mark 恒 safe（原地/页内搬迁、等长纯写，永不结构变更，只有 root 即 leaf 交悲观）；purge 与 delete 同构复用 `deleteWouldUnderflow`（不欠载跳 merge / 欠载回退悲观）。精化 `MiniTransaction.releaseLatch` 只拦 touched 页的 X guard（放行 SHARED，支持同一 MTR 多算子乐观 crab）。诊断计数 + 多层树 hit/fallback/stale 测试（821 tests） | B | 中 | 至此**全部聚簇写算子写路径 latch coupling 全覆盖** | 
| 0.13c | ✅ **读路径 crab 已落**（2026-07-01）：新增 `descendSharedCrab`（全 S hand-over-hand 下降，无 unsafe 回退）+ `findLeafSharedCrab`；`lookup`/`lookupIncludingDeleted`/`locatePointForCurrentRead` 与 `scan`/`locateRangeForCurrentRead`/`terminalGapForRange` 改经它，scan sibling 链亦 hand-over-hand（先 latch 后继再放前驱）；写路径 X 悲观全路径不变。RED：cap 12 小池全量 scan（非 crab 抛 `BufferPoolExhaustedException`）+ 并发 scan-vs-insert 一致性 | B | 小-中 | 缩短内部页/leaf S 持有窗口；结构变更持 root X 到 commit 保证与读者 root S 串行 |
| 0.13d | ✅ **主体已分阶段落地**：prefix index、SX latch、insert/delete/purge safe-node、root SX + restart-in-X；2026-07-12 又完成 internal node/root structure redo。**剩余不再作为 0.13d 主线推进**：B-link/OLC、`PAGE_MAX_TRX_ID`/`PAGE_BTR_SEG_*` | B/Rec | — | B-link/OLC ROI 低且设计未细化；MVCC/segment 辅助页头随后续事务/DD 推进 |
| 0.14 | ✅ **SpaceReservationService core + 三类真实消费者已落**：B+Tree split/root split 用 NORMAL；Undo 主链 grow 与 extern payload 用 UNDO，1.6 起在写前按计划精确预留；0.21h `LobStorage.write` 按精确 chain page count 用 BLOB。reserve 预扩文件/page0 currentSize，allocate 消费 quota，MTR memo/RAII 释放 | D | — | 多页写不再沿 immediate allocation 半途 ENOSPC；动态 reserveFactor/IO capacity 留后续优化 |
| 0.15 | ✅ **ExtentAllocationPolicy direction + leaf 顺序增长多 extent 已落（2026-07-08）**：`PageAllocationHint` 暴露 UP/DOWN/NO_DIRECTION；仅 `INDEX_LEAF` 且明确方向启用 2-4 extent 批量挂段；B+Tree 边界 leaf split 传 hint | D | — | LOB 页链已用 NO_DIRECTION 分配；未做右分裂优化、独立 XDES 管理页、LOB locality hint、动态 reserveFactor |
| 0.16 | ✅ **fil.lock、GENERAL lifecycle 与 file gateway seam 已落（2026-07-06~20）**：per-file `FsyncLock`、durable lifecycle、DROP/DISCARD/IMPORT/rebuild 消费者与 preallocation seam 均接线 | D | — | 剩余平台 native preallocation adapter，不再归入本条 |
| 0.16b | ✅ **GENERAL lifecycle 持久化已落（2026-07-08/20）**：NORMAL/CORRUPTED/DISCARDED 进 page0；DROP/DISCARD/IMPORT 与 ALTER shadow swap 均消费 generation/drain/invalidate，普通访问拒绝非 NORMAL，恢复入口可续作 | D | — | 旧 no-MTR `markTablespaceCorrupted` 仍是 runtime-only 兼容入口；跨设备 IMPORT 由 admin 预复制到受控 incoming |
| 0.16c | ✅ **`DataFileGateway`/`PreallocationStrategy` seam 已落（2026-07-08）**：`DataFileHandle.create/extend/ensureCapacity` 通过 gateway 初始化新增页范围；默认 `ZeroFillDataFileGateway` 保持既有零填充语义，测试可注入 gateway/strategy 校验调用顺序 | D | — | 平台 native preallocation / `posix_fallocate` adapter 仍是后续优化；PageStore 继续 registry-free，并保持 Lifecycle -> FileSize -> Fsync 锁顺序 |
| 0.17 | ✅ **LockManager 内核与 SQL 消费已落**（2026-07-01~20）：record/gap/next-key/insert-intention/logical-prefix S/X、分片显式锁表、Condition 有界等待、wait-for/deadlock/observer；point/general locking read、range DML、SERIALIZABLE 自动提升与 savepoint acquisition/retention boundary 已接 | T | 大 | RC residual-miss scoped-candidate 已设计但未接线；global gap 精确化与更完整 Performance Schema 仍独立保留 |
| 0.18 | ✅ **0.18a redo 文件环机制 + 接线已落**（2026-06-29）：`RotatingRedoLogRepository`（固定文件环 + 文件头 startLsn + 轮转 + checkpoint 回收 + 跨文件恢复扫描 + 环满 fail-closed `RedoLogCapacityExceededException`）；`RedoLogFileRepository` 提升为接口（单文件 + ring）。✅ **0.18b 生产接线 + 默认翻转已落**（2026-06-29/30）。✅ **0.20b LogBlock scanner/header v2**（2026-07-10）。✅ **0.20c operation budget**（2026-07-12）在 ring 写前拒绝 physical 上界超过单文件的 batch，并用 logical 上界参与并发 reservation | Redo | — | 有界 redo、block 恢复完整性与前台准入已闭合；总物理 occupancy/bin-packing 仍明确不与逻辑 LSN 混算 |
| 0.19 | ✅ **0.19a–0.19i + B+Tree structure 扩展已落（2026-07-08~12）**：FSP/undo/rseg、完整 undo payload、B+Tree sibling/internal node/root page-local after-image、non-page trx state；恢复只 patch 页面/状态 sink，不重跑业务算法 | Redo | — | node/root 复用既有 `BTREE_PAGE_DELTA` tag 与预留 kind，无格式升级；leaf row bytes 仍保留物理 redo |
| 0.20 | ✅ **0.20a DurabilityPolicy 已落**（2026-06-30）。✅ **0.20b LogBlock checksum/header-trailer v1 已落**（2026-07-10）。✅ **0.20c per-operation redo budget + 领域 plan 收紧已落**（2026-07-12）：`RedoAppendBudget/Usage/Builder/LogBlockSizing/Workload`；production manager 禁匿名 begin；固定/动态 purpose 分入口；DML 组合 B+Tree height 与 undo 首写，purge/rollback 按结构分支，finalization 从 inode 读取 fragment/extent drop plan；throttle 聚合并发 logical reservation、校验 physical file-fit；commit 验证 actual≤budget，低估保持 COMMITTING fail-stop | Redo | — | 删除 `redoCapacity/8`；logical LSN/pageLSN/WAL 不变；后续只剩 capacity diagnostics/偏差观测类增强 |
| 0.21 | ✅ **Record 层补齐包全部完成（2026-07-13）**：0.21a/b 物理结构+garbage validator；0.21c/d 统一 charset/collation/key order；0.21e1 TIME/TIMESTAMP/YEAR；0.21e2 BIT(1..64) canonical；0.21f ENUM ordinal/SET bitmap；0.21g stable Unicode weight v1 + UTF-8 safe prefix；0.21h TEXT/BLOB/JSON inline/external envelope + `LobStorage` BLOB reservation、LOB segment、多页链/CRC、free、PAGE_INIT/PAGE_BYTES recovery | Rec/B/D/Redo | — | 既有编码/stable id 不变；JSON v1 是 UTF-8 text；LOB write 单 MTR 会 pin 全链；DML 自动 externalize/版本链回收需 DD ownership，作为后续跨模块接线而非 0.21 未完成项 |
| 0.22 | ✅ **BufferPool tablespace stale-frame 版本语义与 DDL 消费者已落**（2026-07-02~20）：generation 守住 admission/lookup/load/prefetch；DROP/DISCARD/IMPORT/rebuild 在 durable phase 下 drain/invalidate，旧 frame 不复活 | BP/D | — | transfer identity/lifecycle 由 DD/fil 校验，Buffer Pool 不读取字典 |
| 0.23 | ✅ **0.23a MTR page latch ordering 已落（2026-07-03）**：`MiniTransaction.fix` 在进入 Buffer Pool latch 等待前执行独立多页 `(spaceId,pageNo)` 升序守卫；同页重入、已释放页放行；`MtrLatchOrderScope` / `allowOutOfOrderPageLatch(reason)` 只给 B+Tree root/child/sibling/SMO allocation-format-free、Undo grow/chain-read/rseg page3 slot 等有局部无环证明的路径短暂例外。✅ **0.23b MTR/Redo 剩余纪律已落（2026-07-08）**：补 savepoint touched-page 边界、commit dirty/pageLSN 发布后再 `markClosed` 的测试保护、read-only MTR 空 range 不关闭外部 gap、`MtrRedoCategory`/`MtrRedoEntry` 本地分类接缝。0.19b 起 FSP allocation intent 已进入持久 redo；完整 MLOG 仍归 0.19 后续 | BP/MTR/Redo | — | 跨页 latch、savepoint、closed LSN 与 collector 分类纪律已闭合；完整 MLOG 仍独立推进 |

## Tier 1 — 依赖 Tier 0 的 storage 件，但仍不碰 DD/DML

| # | 项 | 源 | 依赖 | 规模 |
| --- | --- | --- | --- | --- |
| 1.1 | ✅ **REPAIR_DOUBLEWRITE 生产阶段**（随 0.2 落地：恢复期 scanner + `dwRepo.pageIds()` 真正修复 torn 页）| R | 0.2 | — |
| 1.2 | ✅ **formal multi-index UNDO_ROLLBACK + PREPARED resolution 已接**：恢复按 creator 聚合双 ACTIVE/PREPARED slot；ACTIVE 从双 head 归并 rollback，PREPARED 由外部 provider 决议后复用 prepared commit/rollback，终点 atomic finalizer/terminal | R/T | 0.3 | — |
| 1.3 | ✅ **formal RESUME_PURGE 已接**：恢复先重建 persistent history、决议 PREPARED、rollback ACTIVE，再以 production multi-worker coordinator 执行 version-safe secondary/clustered purge，并在 OPEN 前 flush/force | R/U/T | 0.3 + 0.4 | — |
| 1.4 | ✅ **statement/named-savepoint/full/recovery rollback + persistent progress + atomic finalization 已接（2026-07-10~20）**：Session 名称映射 opaque boundary；同名替换、ROLLBACK TO 保留目标、RELEASE 单目标；undo partial/full/recovery 与 lock retention 白名单均闭环 | U/T | 现有 rollback | — |
| 1.5 | ✅ **有界 multi-worker purge 已接（2026-07-22）**：默认 4 worker、16-log window、5s batch timeout；整 history log 为 task，affected-table token 保证同表 FIFO/多表联合 fence，异表并行；secondary→clustered→LOB/head progress 保持记录内串行，worker 不摘 history，dispatcher 只 finalization 连续 READY 物理前缀；关闭不强制中断记录内 MTR | U/T | 0.4 | index/page 内细分、自适应 IO 限速和跨 rseg 选择 deferred |
| 1.6 | ✅ **extern payload + 独立 INSERT/UPDATE Undo Log + cache/free reuse 已落（2026-07-13~15）**：payload chain/CRC/owner、immutable plan、精确 reservation/redo workload；每事务最多两 slot/segment，事务全局 undoNo + kind-local predecessor；单 fragment 段先进入按 kind 固定容量 LIFO，cache 不接纳时尾插跨 kind 持久 FIFO，重启恢复并可由另一 kind 摘头激活；truncate 统一 drain/rebuild，多页仍 drop。**剩余**：多 rseg / 多 undo tablespace（`RollPointer` 编码扩展）、改聚簇 PK | U/T | storage 扩展 | 当前单 rseg/reuse owner 边界闭环；其余大，可拆 |
| 1.7 | ✅ **page0 checksum/trailer 校验已落（2026-07-06）**：`PageZeroTablespaceMetadataLoader` 在 FSP_HDR 信封校验后调用 `PageImageChecksum.verify`，校验 header checksum、trailer checksum 与 trailer low32 LSN；历史未盖 checksum 的 page0 仅在 header/trailer checksum 同为 0 时兼容 | D | — | 已有 flush 路径盖 `PageImageChecksum`，新页走严格校验；兼容分支只为早期切片文件 |
| 1.8 | ✅ **typed page access lease 后 Registry 状态复核已落（2026-07-06）**：生产 `StorageEngine` 给 `IndexPageAccess` / `UndoLogSegmentAccess` 注入共享 `TablespaceRegistry`；INDEX/UNDO open/create 先持 S lease，再 `registry.require`，稳定 INACTIVE/CORRUPTED/DISCARDED 不再进入 Buffer Pool | D | — | 两参低层测试构造仍保留，专注页格式与链路行为 |
| 1.9 | ✅ **Recovery/control-plane + Session admission gate 已接**（2026-07-06/16）：storage accessor/DML 要求 recovery gate OPEN；public engine 只在 storage+DDL recovery 后开放 Session，CLOSING 拒绝新流量并先收敛 Session；progress JSONL 仅诊断。**剩余**：后台 worker resume 结果、恢复锁/等待快照与 Session 关联诊断 | R/BP/T | E2 gate + engine bootstrap | 小-中 |
| 1.10 | ✅ **正式 trx recovery table + persistent server XA 已接**：双槽/redo/page3/first-page 证据；storage phase one/two 强制 fsync；`mysql.xa` registry 提供 startup decision，未决 PREPARED fail-closed | T/R/Redo | 0.19i + atomic finalization | storage 仍只认识 TransactionId；XID ownership 在 engine/session 层 |

## Tier 2 — 被尚未建立的上层卡住（不要硬接）

| # | 项 | 源 | 阻塞于 |
| --- | --- | --- | --- |
| 2.1 | ✅ **table-level DML + SQL point/range INSERT/UPDATE/DELETE 已接**：typed patch 在 FOR_UPDATE/current range 锁定后应用；先物化 identity 防 Halloween/partial；全部 secondary、LOB、statement/named/full/recovery rollback 与 purge progress 闭环 | U/T | 剩余主键更新 |
| 2.2 | ✅ **二级索引 purge / 回表 MVCC / delete-marked unique current-read 已接（2026-07-17~23）**：logical-prefix S/X、事务终态等待后 current 重扫、multi-index undo/purge、unique point、comparison/composite/full scan、multi-worker 与 real RESUME_PURGE | U/T | 聚簇 marked-key reuse 的 update-class undo/secondary inverse 已完成设计，仍为 unwired |
| 2.3 | 🟡 **DDL marker v5 + atomic DDL log 已接（2026-07-20~24）**：operation 1..13 的 identity/phase、schema digest、protocol/control/fence；v5 以分块 batch manifest 支持原子多表 DROP 与 DROP SCHEMA CASCADE，decoder 兼容 v1-v4；**temporary undo仍保留** | U/T/R | 临时表owner/lifecycle与独立temporary tablespace尚未建立，禁止塞入普通history/page3 |
| 2.4 | ✅ **DD DISCOVER_TABLESPACE v1 已接（2026-07-15）**：ACTIVE/DROP_PENDING binding 生成 recovery spaces，ACTIVE 缺文件 fail-closed；2.9 离线工具另对 manifest ACTIVE file-per-table 做全页扫描 | R | 普通启动不做全 data-dir checksum scrub；系统/undo/未受 manifest 管理空间的离线 scrub 仍未接 |
| 2.5 | ✅ **RECOVER_DDL atomic + SDI reconcile已接（2026-07-20~22）**：独立marker裁决table/index/transfer/rebuild/Online ADD/DROP、通用INPLACE/SHADOW ALTER与recovery-object op8/9/10；通用恢复交叉验证manifest/journal/descriptor/fence，按source/target与OPEN/CANCEL/FORWARD_ONLY单向收敛 | R | legacy pending/orphan继续兼容；全实例离线scrub另列 |
| 2.6 | ✅ **四隔离级别、named savepoint 与 persistent server XA 已接（2026-07-20，07-21 复审加固）**：RU current read、RR/RC ReadView、SERIALIZABLE promotion、lock retention boundary、fsync XID registry、XA SQL/RECOVER/startup gate/offline decision；per-XID phase 共用绝对 deadline，registry I/O 失败 fail-stop | T/R | RC residual-miss 精确句柄释放已设计但未接线；XA compaction/heuristic/active migration |
| 2.7a | ✅ **B+Tree point current-read + logical key 锁接入已落**（2026-07-01~20）：短 MTR 定位 -> 无 page latch 等锁 -> 重定位；SQL DML、显式 point locking SELECT 与 SERIALIZABLE promotion 共用 physical/logical locks | B/T | 普通非锁定 point SELECT 按隔离级别走 MVCC/RU current；global gap 精确化另列 |
| 2.7b | ✅ **B+Tree range current-read + SQL comparison/composite range/range DML 已落**（2026-07-01~20，07-21 复审加固）：open/closed/unbounded、256-row continuation、最长连续复合前缀/full scan、MVCC/RU/current 回表、locking read 与 Halloween/partial 防线均接线；逐行锁/terminal gap/relocation 共享单一绝对预算 | B/T | RC residual-miss additive scoped API 已设计、尚未实现；global gap 精确化另列 |
| 2.8 | ✅ **General SQL / Session transaction v1 已接线（2026-07-16~20）**：DML、point/comparison/composite/full-scan、explicit locking、四隔离级别、named savepoint、persistent XA、28 类型、MDL、LOB、rollback 与 engine gate | B/T | optimizer/network/prepared statement、SET TRANSACTION ISOLATION LEVEL |
| 2.9 | ✅ **catalog-loss recovery v1 已闭环并完成复审加固（2026-07-20）**：普通启动 fail-closed；独立 durable clean manifest/witness 保存 schema、完整目录与高水位；离线 API 完整枚举并 full-page scrub 候选，NOFOLLOW channel 与 XDES state/owner/list/bitmap 均严格校验，token 绑定全部证据；零长度 manifest 先保留，管理员显式隔离冲突后，以 safe control + clean-digest 稳定临时 baseline 原子发布 catalog，随后仍由普通引擎严格打开 | D | v1 不从无 manifest 的散落 SDI 猜 schema、不自动启动、不修 torn page、不承诺 Java/Windows 不可移植的目录 fsync |
| 2.10 | ✅ **DROP + controlled DISCARD/IMPORT lifecycle 已接**：purge barrier、DD pending states、exact transfer paths、page0/SDI/file identity、spaceVersion、flush/invalidate 与 startup resume 均闭环 | D/DD/R | 跨设备来源由 admin 预复制；完整目录 fsync 可移植性不承诺 |
| 2.11 | ✅ **Online ALTER evolution A-F已接（2026-07-21~22）**：单ADD/DROP与instant metadata之外，通用INPLACE以versioned descriptor chain+multi-target capture原子发布多个secondary action；通用SHADOW以clustered identity journal、bounded copy、两遍reconcile、ReadView/purge barrier和old-space retirement发布row-layout/mixed action；两路均有digest/control/tracker/cancel/recovery | DD/B/T/R | binlog、主键/任意类型/foreign/generated、prefix/FULLTEXT/SPATIAL、并行/外排/持久断点仍待 |
| 2.12 | ✅ **object-level force recovery v1 已接（2026-07-21）**：DD 原子隔离、管理员/DD union exclusion、redo/doublewrite/reconcile/undo/purge skip、导出只读写闸门、raw DISCARD/DROP、实例 HMAC clean backup 与固定 incoming trusted replacement | DD/R/U/T/D | v1 仅用户 file-per-table；系统/undo/共享空间、跨实例 trust、自动 repair 与 SQL/权限管理语法不支持 |

## 推荐路线

**已完成链：0.1 redo flusher ✅ → 0.2 recoverable doublewrite ✅（含 R 1.1）→ 0.3 持久 rseg header ✅ →
R 1.2 ROLLBACK_TRX ✅（恢复回滚，可由 DD resolver 定位索引）→ 0.4 后台 purge driver ✅ → 1.3 RESUME_PURGE ✅**（恢复期
重建 committed history、复位事务计数、后台自动 purge）；**formal UNDO_ROLLBACK/RESUME_PURGE stage 也已接入**。
**page3 slot release、四路 segment finalization、正式 trx recovery table v1、0.20b LogBlock v1 与 0.20c operation redo budget 已闭合**。
**B+Tree node/root 更细结构 redo 也已完成**：internal 页按 header/used heap/directory 收集最终 after-image，root
split/shrink 记录 level/index identity，恢复不重跑结构算法。**按 B+Tree height、DML 首写、rollback 类型与 undo
segment drop plan 收紧 operation redo profile 也已完成**。**0.21 Record 补齐包现已全部完成**：页内结构/garbage
validator、稳定 charset/collation 与 schema-aware key order、TIME/TIMESTAMP/YEAR、BIT、ENUM/SET、Unicode weight v1，
以及 TEXT/BLOB/JSON inline/external envelope 与显式 off-page LOB 页链均已闭环；BLOB reservation、LOB segment、CRC、
PAGE_INIT/PAGE_BYTES 通用 recovery 和 external reference undo 往返都有测试。0.21 的边界是 Record/storage 物理能力，
不会在没有列到 LOB segment ownership 映射时假装自动接入 external DML/purge。**1.6 extern payload、独立 INSERT/UPDATE Undo Log、cache + persistent free reuse 与持久 history 均已完成**：
普通超大旧行可经独立 `UNDO_PAYLOAD` 页链保存；同事务两类 log 独立占 slot/segment，并用全局 undoNo 归并回滚；
空单 fragment 段可按 kind 跨事务/重启复用，truncate 与恢复 owner 边界已闭环。
持久 free undo segment list 与 DD/catalog/MDL/physical CREATE-DROP/discovery/index resolver v1 都已完成。
**1.5 multi-worker purge 已完成**：生产完整 DD resolver 路径使用可配置有界线程池，legacy 低层构造保持
direct 串行；table-token DAG 允许异表并行并阻止同表/多表日志越序，history removal 仍严格按物理 head。
**purge→undo tablespace truncate 自动调度已完成**：复用 driver 线程、默认 1 extent/30s、共享 recovery/live
truncate service，并暴露 skip/deferred/completed/failure metrics。undo/purge 下一条可独立切片优先是
**多页 reusable undo segment shrink**；跨 rseg blocked-head 和多 undo space 候选选择必须等待持久 identity/
`RollPointer` 扩展，不能在当前单 rseg 模型上伪造。
**2.8 Primary-point SQL / Session、2.2 secondary closure 与 point-write LOB lifecycle 均已完成**：SQL INSERT/UPDATE/DELETE 维护全部索引；唯一二级点查走真实回表 MVCC；replacement 新链由 rollback 回收，旧链由逐记录 purge progress 回收；affected-table history、real RESUME_PURGE 与 DROP barrier 已进入生产组合根。
**comparison/composite/full-scan、ORDER BY/LIMIT、批量 INSERT/AUTO_INCREMENT、基础 schema/table DDL、
四隔离级别、named SAVEPOINT、persistent server XA、受控
DISCARD/IMPORT、table options/defaults、Online ADD/DROP INDEX、marker v5 digest/control/batch manifest、
通用multi-index INPLACE/online shadow ALTER与object-level force recovery均已完成（2026-07-22）**。
下一条高价值切片可从复制binlog参与者、主键/类型/foreign/generated ALTER，或Online DDL并行/外排与持久
scan continuation中选择；这些能力都需要独立设计其提交、恢复与兼容边界。
复制binlog应先补独立server/replication设计和提交参与者，
不应塞进 row-log。temporary undo 仍必须等待临时表 owner/lifecycle 与独立 temporary tablespace，不能塞进普通
history/page3。

**btree / buffer-pool / record 的 Tier 0 项（0.8–0.13、0.21–0.23）与上面这条路线并行、互不阻塞**，但不是当前 crash-safety 主线，按需挑：
若转向"让 buffer pool 更接近生产质量"，0.8/0.9、0.10a/b/c/d（linear + warmup + random read-ahead + 多 instance 分片）、0.22 stale-frame 版本语义、13.1a/13.1b-pre/13.1c/13.1d 锁边界与真实 flush list、legacy `BufferPool.flush/flushAll` API 移除、0.23a MTR page latch ordering 与 0.23b MTR/Redo 剩余纪律已落；下一步可处理 warmup IO 速率控制、random read-ahead 生产 config，或继续细化 `DIRTY_PENDING`/`EVICTING`/`STALE` 状态；
B+Tree 多层结构、merge/root shrink、redistribute、全部读写 latch coupling、safe-node/SX restart 均已落；
**sibling-link + internal node/root structure redo 也已接入**。剩余主项是 B-link/OLC（长期 deferred）与
MVCC/segment 辅助字段（`PAGE_MAX_TRX_ID`/`PAGE_BTR_SEG_*`）。
B+Tree 的 point/unique/range current-read 已有 storage 内闭环（2.7a/2.7b）；SQL DML、显式 locking SELECT
与 SERIALIZABLE promotion 共用聚簇/secondary logical locks，comparison/composite/full-scan 支持
RR/RC/RU read、locking read 与 range DML。剩余是 RC residual-miss 提前解锁和 global gap 精确化。

**redo 本体 Tier 0 项（0.18–0.20）**：后台 flusher、并发 tracker、有界文件环、0.19 logical/page-local redo、DurabilityPolicy、LogBlock v1、operation budget、领域 plan workload 与 B+Tree node/root delta 均已接。后续仅按需增加更丰富 capacity diagnostics/偏差观测。

**transaction/current-read 主线**：LockManager、point/general current-read、table DML、secondary MVCC/purge、
唯一二级 delete-marked current-read、四隔离级别、named savepoint、storage PREPARED 与 persistent server XA 已有。
聚簇主键 delete-marked reuse 与 RC residual-miss 解锁均已形成 2026-07-23 slim slice，但生产仍是 unwired；实现顺序固定为
先完成 reuse 的 undo/MVCC/恢复正确性，再接只影响 RC 的候选精确解锁。global gap reference 继续作为独立并发精度优化。

**Tier 2 的 DD、Session、XA 与 blocking DDL 已解锁**：discovery、atomic table/index/transfer/rebuild
lifecycle、general SQL、secondary closure、prepared resolution 与 catalog-loss recovery 均有 production v1。
后续扩展仍保持上层只走 DD lease 与稳定 gateway，不越过 storage API 读页。
