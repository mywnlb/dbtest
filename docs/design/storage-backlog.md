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
> 最近校对：2026-06-29（基于当前源码与 `current-implementation-map.md`：0.12/0.12b B+Tree 回收、BP 0.8/0.9、redo flusher、recoverable doublewrite 状态同步；补入 Record/backlog 维度与 recovery/control-plane 缺口）。

## 九份文档的当前完成度（粗估，表达"核心在、外围缺"）

| 文档 | 已闭环的核心 | 主要缺口 | 粗估 |
| --- | --- | --- | --- |
| F flush/checkpoint/doublewrite | FLUSH_LIST / SINGLE_PAGE / SHUTDOWN / **LRU_FLUSH(WAL 安全淘汰)**、WAL gate、recoverable doublewrite 生产接线、**bounded doublewrite slot reuse（0.5）**、仓储级 `DoublewriteBatch` 连续 slot 原语、fuzzy checkpoint + 持久 label、PageCleanerWorker | FlushList/LRU 双文件、生产 batch dispatch、DETECT_ONLY、真 adaptive、压力 throttle、PageCleaner supervisor、metrics/diagnostics、legacy flush 收敛 | ~69% |
| U undo/purge | undo 写(INSERT/UPDATE/DELETE_MARK)、real rollback、MVCC 旧版本读、单线程聚簇 purge、后台 purge driver、可恢复 truncation、恢复期 committed history 重建 | **无 session/executor 驱动的生产 DML**、多 worker、二级索引、statement/savepoint rollback、extern payload、全 release 路径持久 | ~58% |
| T transaction/mvcc | `TransactionManager/System`、事务状态机、RR/RC `ReadView`、MVCC 旧版本读、rollback service、purge low water、engine 服务级接线、恢复期 counter restore | **无 LockManager / current read**、无 session/executor DML facade、RU/SERIALIZABLE、statement/savepoint rollback、PREPARED/RECOVERED_ACTIVE/XA、formal recovery stage | ~55% |
| R crash-recovery | 编排骨架(gate/stage/report)、REDO_REPLAY 真回放、REPAIR_DOUBLEWRITE 生产接线、UNDO_TABLESPACE_RESUME、SPACE_FILE_RECONCILE、ROLLBACK_TRX engine 后恢复步、RESUME_PURGE engine 后恢复步、forceAll、fail-closed | DISCOVER_TABLESPACE、RECOVER_DDL、formal UNDO_ROLLBACK/PURGE_RESUME stages、多索引/DD/prepared txn、非 NORMAL mode | ~55% |
| B btree | 值对象 + BTreePage、root→leaf 查找、point lookup、range scan + sibling、**insert split 传播(任意树高，0.11)**、delete-mark + purge 物理删除、**merge + 原地 root shrink(0.12)**、**redistribute 对半再平衡(0.12b)** | btree 专用 redo、latch coupling/current-read 锁、executor/DD/DML facade 接线、MVCC 逻辑唯一、PAGE_MAX_TRX_ID/segment header、prefix index 比较 | ~68% |
| BP buffer-pool | get/new/fix、page latch、`PageGuard`、residentMap + free list、MTR memo + 标脏、flush 协作接口、clean 淘汰 + **dirty 淘汰(WAL 安全)**、midpoint LRU + 扫描抗污染、per-frame LOADING + miss IO 出 `poolLock`、FrameStateMachine(FLUSHING) | read-ahead、warmup dump/load、多 instance 分片、专用 PageHashTable、truncate/drop stale-frame 版本校验、legacy flush 收敛 | ~65% |
| D disk-manager | fil/fsp/segment/extent/page 分配、`DiskSpaceManager` facade、page0 header + **FSP_HDR 信封校验**、**autoextend crash-safe**、undo tablespace 可恢复 truncation、registry 运行时准入 | SpaceReservation(§7.1)、`ExtentAllocationPolicy` direction/multi-extent、`DataFileHandleLock`/`PageIoRangeLock`/`FsyncLock` 接线或删除、普通 lifecycle 持久化、page0 checksum、完整 discovery(DD) | ~70% |
| Rec record | `TableSchema`/`IndexKeyDef`/13 类 `TypeId`、字段 codec、record header/PageDirectory/hidden columns、in-page insert/search/delete/purge/update/reorganize 原语、undo old image codec | 类型系统扩展、charset/collation、ASC/DESC/NULL 排序、prefix key 比较、overflow/BLOB chain、PAGE_MAX_TRX_ID/segment header 配合 | ~55% |
| Redo redo-log | `RedoLogManager` 内存/durable 双模式、`PAGE_INIT`/`PAGE_BYTES` 编码、append+flush 同步落盘、后台 `RedoFlushWorker`、append/fsync 状态锁拆分、recent written/closed tracker、`closedLsn` checkpoint 边界、`flushedToDiskLsn` WAL gate、`RedoCheckpointStore` 两槽 fuzzy checkpoint+CRC32、`RedoRecoveryReader` checkpoint 扫描、`RedoApplyDispatcher` 物理重放、recovery boundary 连续续写、`RedoCapacityPolicy` 4 级压力 | commit durable policy、redo 文件轮转/回收、多 apply handler/逻辑 redo、DurabilityPolicy、LogBlock checksum/header-trailer | ~52% |

这些缺口彼此咬合，共同卡点：**生产 driver/组合根**（DML facade、DD/executor 接线、RecoveryTrafficGate 入口拦截）、**持久化 + 恢复重建**（tablespace/DD discovery、redo 文件轮转、formal recovery stages）、**未建模块**（DD/DDL、LockManager/current read）。

## Tier 0 — 现在可独立做（storage 内闭环，无新上层依赖）

按杠杆从高到低。

| # | 项 | 源 | 规模 | 价值 / 备注 |
| --- | --- | --- | --- | --- |
| 0.1 | ✅ **后台 redo flusher 已接**（`RedoFlushWorker`，2026-06-24）——周期/on-demand 驱动 `redo.flush()`，淘汰/flush 不再因 redo 未 durable 长时间跳过。✅ **append/fsync 拆锁 + recent_written/recent_closed tracker + closedLsn checkpoint 修正已接**（2026-06-29）：`RedoLogManager` 以 state lock 分配 LSN/维护 pending，以 `ioLock` 串行 write/fsync；append 不再被 fsync 持有状态锁阻塞；MTR dirty 发布后 `markClosed`，checkpoint 读 `closedLsn`。**剩余（独立后续项）**：commit durable policy（`FLUSH_ON_COMMIT`/等待） | F/T/Redo | 小-中 | 后台 flush、并发边界与 checkpoint 正确性已闭合；剩余是事务提交持久性策略 |
| 0.2 | ✅ **recoverable doublewrite 已接进 engine**（2026-06-24）：`StorageEngine` 注入 `RecoverableDoublewriteStrategy`（前向）+ E2 配 `DoublewriteRecoveryScanner` + 新增 `DoublewriteFileRepository.pageIds()`（过滤到恢复已打开空间）；e2e torn-page 恢复验证通过。**剩余**：`DETECT_ONLY`、全空间 discovery | F | — | 打开了真 torn-page 防护；R 的 1.1（REPAIR_DOUBLEWRITE 生产阶段）已随本片落地 |
| 0.3 | ✅ **持久 rollback segment header + 恢复期 rseg/slot 扫描已接**（2026-06-24）：undo **page3** = `RSEG_HEADER`（`reserveSystemExtent` 预留），`RollbackSegmentHeaderRepository` MTR 读写 slot 目录（redo 保护）；`UndoLogManager` claim/onCommit-release 持久；engine fresh format + 恢复扫描 `restore` 重建。**剩余**：§6.3 history/cached segment 富字段、`RollbackService`/`PurgeCoordinator` release 持久、truncate rebuild 重格式化 page3 | U/T | — | 解锁 R 1.2/1.3；active-vs-committed 判定本身在 R 1.2 |
| 0.4 | ✅ **后台 purge driver 已接**（2026-06-24）：`PurgeTarget` 端口 + `PurgeDriverWorker`（沿用 RedoFlushWorker 形态，周期/on-demand `runBatch`、失败 FAILED）；`StorageEngine` 配 `clusteredIndex`（`configureClusteredIndex`，同 R 1.2 复用）时构造 `PurgeCoordinator` + 启动 driver；money test 后台自动 purge committed delete-mark。剩余：多 worker、二级索引、持久 history、purge→truncate 调度 | U/T | — | 通用生产化仍需 2.1/DD index metadata |
| 0.5 | ✅ **bounded doublewrite slot reuse + 仓储级 `DoublewriteBatch` 已接**（2026-06-29）：`DoublewriteFileRepository` 默认 1024 个固定 full-copy slot，测试可注入小 slot 数；`RecoverableDoublewriteStrategy.afterDataFileWrite` 释放 in-flight slot，data file force 前不可覆盖；`latestCopy` 按最高 pageLSN 选同页最新有效副本，损坏 slot 跳过；`appendBatch/releaseBatch` 在单个 doublewrite 文件锁内连续写 slot。**剩余**：FlushList/LRU 双文件 + 生产 batch dispatch | F | 小-中 | 已去掉 append-only 无界增长并补仓储 batch 原语；剩余是把 flush list/LRU 策略接入生产调度 |
| 0.6 | 真 adaptive flush（redo 生成率/IO capacity/neighbor/idle percent）+ 压力 throttle（sync→前台等 checkpoint、hard→暂停 redo reservation） | F | 中 | 把 `fixed` 简化策略换成设计版 |
| 0.7 | 碎片打包：`DETECT_ONLY` 模式、PageCleaner supervisor 重启、metrics snapshot、清理 legacy `BufferPool.flush`、`drainTablespace` busy-wait 改 condition 唤醒（现 `LockSupport.parkNanos(1ms)` 无 BufferPool 唤醒）、`RecoveryMode` 的 `READ_ONLY_VALIDATE`/`FORCE_SKIP_CORRUPT` | F/R | 小 | 韧性/可观测/收尾，可拼成一片 |
| 0.8 | ✅ **Midpoint LRU（old/new 子链）+ 扫描抗污染已落**（2026-06-25，Phase A）：`MidpointLruReplacementPolicy` 读入进 old 头、`oldBlocksTime`(注入毫秒时钟)提升窗 + `youngDistanceThreshold`(young 1/4)抗抖动，默认接线、`largeScanDoesNotEvictHotWorkingSet` 验证；删除 plain `LruReplacementPolicy`。**剩余**：read-ahead-aware 访问型分类、`oldBlocksPct` 容量配比再平衡（随 0.10） | BP | 中 | buffer pool 质量最直接提升 |
| 0.9 | ✅ **per-frame IO/loading 状态 + 显式 FrameStateMachine 已落**（2026-06-25，Phase B）：`BufferFrameState`(FREE/LOADING/CLEAN/DIRTY/FLUSHING)+`FrameStateMachine`；miss 读盘移出 `poolLock`（LOADING 占位 + `PageLoadFuture` 有界等待，不同页并发读/同页只读一次/失败清占位/超时·中断不悬挂）；FLUSHING 与 dirty 正交。**剩余**：legacy `flush`/`flushAll` 持锁直写、DIRTY_PENDING/EVICTING/STALE 态、多 instance 分片（0.10） | BP | 中-大 | buffer pool 最大并发瓶颈已解（单 `poolLock` 不再串行 miss 盘 IO）|
| 0.10 | Read-Ahead（linear/random）、warmup dump/load、多 instance 分片 + 专用 `PageHashTable` | BP | 各中 | 独立特性，可分片做；stale validation 单列 0.22，latch 排序单列 0.23 |
| 0.11 | ✅ **parent split（解锁树高 >1）已落**（2026-06-25）：自底向上 split 传播 + 内部页 split + 原地 root split（任意层 +1）+ N 层 `findLeaf` 导航；所有现有操作（insert/lookup/scan/delete/replace/deleteMark/purge）多层可用；`nonLeafSegment` 分配内部/root-split 子页；删 `BTreeParentSplitRequiredException`。验证 level-2/level-3、多层 scan、多层聚簇写 | B | 中-大 | 0.12/0.12b 已基于此前置完成；后续转 0.13/2.7 |
| 0.12 | ✅ **merge + 原地 root shrink 已落**（2026-06-29）：删/purge 后 underflow（可回收空闲>页半）→ merge 同父相邻兄弟（reorganize survivor + 并入 victim + 摘 parent pointer + `disk.freePage`）→ 自底向上传播 → root 剩 1 pointer 原地 shrink（吸收唯一 child、树高-1、级联）；导航改按 root 页实际 level（`openRoot` 去 level 断言）；`BTreeDeleteResult` 带 `indexAfter`/`freedPages` | B | 中 | 多层树收缩 + 空页回收已解；min-key-pointer 约定下 survivor 父 pointer key 不变（无 separator 更新） |
| 0.12b | ✅ **redistribute 对半再平衡已落**（2026-06-29）：`considerMerge` 的 `!mergeFits` 分支由「留欠载」改 `redistribute`——合并相邻同父对全部条目对半重分到两页（`splitRows`/`splitPointers`）、只更新 parent 中 right 成员 lowKey（min-key-pointer 下 left 不变）、不删页/不传播/不改树高；leaf+internal 统一。消除 merge fit 不下时的欠载/1-pointer 退化页 | B | 中 | 借法=对半（非按记录数阈值）；进入条件保证 total>1 页、平分必双健康 |
| 0.13 | btree 专用 redo handler、索引页头 `PAGE_MAX_TRX_ID`/`PAGE_BTR_SEG_*`、prefix index 比较、`LatchCouplingController`（乐观→悲观下降） | B/Rec | 各小-中 | `nonLeafSegment` 分配已随 0.11 落地；剩余是恢复精度、MVCC 辅助字段和并发重定位 |
| 0.14 | **SpaceReservationService（§7.1 reserve-before-multi-page-op）+ SegmentReservation kinds（NORMAL/UNDO/CLEANING/BLOB）** | D | 中-大 | 设计强制的多页操作预留；现 split/grow/allocate 即时分配可半途 ENOSPC。已有消费者（`allocatePage`/btree split/undo grow），跨切面 |
| 0.15 | `ExtentAllocationPolicy` direction（UP/DOWN/NO_DIRECTION）+ 大 segment 顺序增长多 extent（2-4）（§7.3） | D | 中 | 现 direction 恒 RIGHT、单 extent |
| 0.16 | 碎片打包（D）：`DataFileHandleLock` / `PageIoRangeLock` / `FsyncLock` 明确接线或删除；普通 GENERAL `CORRUPTED` 状态持久化到 page0 + 打开校验；`DataFileGateway`/`PreallocationStrategy`(posix_fallocate adapter) | D | 各小 | 三个 fil.lock 预留锁都要落到具体 owner，不再只跟踪 `FsyncLock`；DISCARD/DROP 持久化留 Tier 2（需 DDL）|
| 0.17 | **最小 LockManager 内核**：record/gap/next-key/insert-intention lock、wait queue、timeout、bounded deadlock detector、事务持锁集合 | T | 大 | 可 storage 内闭环单测；解锁 current read、SERIALIZABLE、唯一检查等待/重定位；lock observability 快照可后续接独立设计 |
| 0.18 | ✅ **0.18a redo 文件环机制 + 接线已落**（2026-06-29）：`RotatingRedoLogRepository`（固定文件环 + 文件头 startLsn + 轮转 + checkpoint 回收 + 跨文件恢复扫描 + 环满 fail-closed `RedoLogCapacityExceededException`）；`RedoBatchFrameCodec` 抽出帧编解码供单文件/文件环共用；`RedoLogFileRepository` 提升为接口（`SingleFileRedoLogRepository` + ring 两实现），经 `RedoLogManager.durable`/`RedoRecoveryReader` 集成测试验证（744 tests）。✅ **0.18b 生产接线 + 默认翻转已落**（2026-06-29/30）：`RedoReclaimBoundary` 端口 + `CheckpointCoordinator` 4 参构造（checkpoint 持久并单调前进后、锁外推进回收边界）；`RedoRotationConfig`(+`defaults()` 8×8MiB) + `EngineConfig` 默认即文件环 + `withSingleFileRedo()` 显式 opt-out；`StorageEngine.open` 默认 `openRing` 并把环作回收边界端口传 checkpoint，恢复经接口跨文件读（748 tests，既有 engine 测试已迁移）。**残留（已是独立 backlog 项）**：环满容量分级 throttle = 0.6；log block header/trailer checksum = 0.20 | Redo | — | 单文件无界增长已彻底解决；redo 本体 Tier 0 收口 |
| 0.19 | **逻辑 redo record type + 多 apply handler**（§5.4/§6/§13）：当前仅 `PAGE_INIT`/`PAGE_BYTES` 物理重放，无 MLOG 逻辑 redo（btree split、space header、xdes、undo record insert、trx state）；`RedoApplyDispatcher` 单 handler | Redo | 中 | 减少恢复体积、提升 btree/undo 恢复精度；按需随 0.11/0.13 btree 稳定后逐条加 |
| 0.20 | **DurabilityPolicy 抽象 + LogBlock checksum**（§5.8/§8）：当前无 commit 刷盘策略抽象（恒同步 flush，无 `FLUSH_ON_COMMIT`/`WRITE_ON_COMMIT`/`BACKGROUND_FLUSH` 区分）；redo 块无 header/trailer 校验，torn log write 只靠 record checksum | Redo | 小-中 | commit 路径性能 + redo 文件韧性；DurabilityPolicy 与 0.1 后台 flusher 配合 |
| 0.21 | **Record 层补齐包**：类型系统扩展（TIME/TIMESTAMP/YEAR/TEXT/BLOB/ENUM/SET/JSON/bitstring）、charset/collation、ASC/DESC/NULL 排序、PageDirectory/record header 校验增强、overflow/BLOB chain | Rec/B | 中-大 | record 已能支撑当前 btree/undo；这些是更贴近 MySQL/InnoDB 的页内格式与比较语义 |
| 0.22 | **BufferPool truncate/drop stale-frame 语义**：`TablespaceVersion` / `SpaceLifecycleClock`、waiter recheck、专用 `PageHashTable` stale validation、drop/truncate 后旧 frame 隔离 | BP/D | 中 | 当前 per-space invalidate 可用，但缺版本化 stale 防护；与 0.10 PageHashTable、多 instance 分片协同 |
| 0.23 | **MTR 纪律细化包**：一般升序 page latch 排序、savepoint 使用边界测试、commit ordering/closed LSN 协议、redo collector 命令分类（从 PAGE_BYTES 过渡到更细 MLOG） | BP/MTR/Redo | 中 | `MtrSavepoint` 已有；无跨页 latch 顺序仍是当前并发风险，commit ordering 与 0.1 剩余项关联 |

## Tier 1 — 依赖 Tier 0 的 storage 件，但仍不碰 DD/DML

| # | 项 | 源 | 依赖 | 规模 |
| --- | --- | --- | --- | --- |
| 1.1 | ✅ **REPAIR_DOUBLEWRITE 生产阶段**（随 0.2 落地：恢复期 scanner + `dwRepo.pageIds()` 真正修复 torn 页）| R | 0.2 | — |
| 1.2 | ✅ **ROLLBACK_TRX 恢复已接**（2026-06-24）：undo-log `STATE_COMMITTED` 标记（onCommit）；`RollbackService.rollbackRecovered`（无 live Transaction，forward-collect→reverse-apply）；engine 恢复扫 rseg→读 state→ACTIVE 段用**显式配置聚簇索引**回滚。剩余：多索引/DD、prepared txn、正式 UNDO_ROLLBACK stage | R/T | 0.3 | — |
| 1.3 | ✅ **RESUME_PURGE 恢复已接**（2026-06-24）：undo first 页持久 `COMMIT_NO`；engine 恢复扫 page3 restored slots，COMMITTED 段按 commit no 重建 history，`TransactionSystem.restoreCounters` 复位 id/no，高水位覆盖 history 后后台 purge driver 自动续作。剩余：formal RESUME_PURGE stage、多索引/DD、prepared txn、持久 history 链表 | R/U/T | 0.3 + 0.4 | — |
| 1.4 | statement / savepoint rollback（`UndoContext.savepointStack` 已预留） | U/T | 现有 rollback | 中 |
| 1.5 | 多 worker purge 分片（table/index/page） | U/T | 0.4 | 中 |
| 1.6 | undo 段/页回收 cached reuse、拆独立 insert/update undo log、多 rseg / 多 undo tablespace（`RollPointer` 编码扩展）、extern payload + 改聚簇 PK | U/T | storage 扩展 | 各中-大，可拆 |
| 1.7 | page0 checksum/trailer 校验（page0 FSP_HDR 信封已校验，checksum 仍 deferred） | D | "写盘统一盖 checksum"（与 F 写盘路径相关，清理 legacy `flushAll`/no-flusher 直写后）| 小-中 |
| 1.8 | typed page access（`IndexPageAccess`/`UndoPageAccess`）lease 后 Registry 状态复核（现持 S lease 但不拒稳定 INACTIVE/CORRUPTED）| D | 生产 storage facade | 小 |
| 1.9 | **Recovery/control-plane 收口**：`RecoveryTrafficGate` 在 storage facade/session 入口强制拒绝恢复中访问；恢复进度 journal；恢复期后台 worker resume 结果记录；恢复锁/等待快照与诊断 | R/BP/T | E2 gate + engine bootstrap | 小-中 |

## Tier 2 — 被尚未建立的上层卡住（不要硬接）

| # | 项 | 源 | 阻塞于 |
| --- | --- | --- | --- |
| 2.1 | **生产 DML facade**（`assignWriteId→beforeX→Xclustered` + commit 编排 + rollback 入口）= T1.5 | U/T | session/executor 或 storage-API DML 层——让 transaction/undo/purge 真正被生产驱动的总开关 |
| 2.2 | 二级索引 purge / 回表 MVCC | U/T | 二级索引 + DD index metadata |
| 2.3 | DDL undo marker / temporary undo | U/T | DD / DDL 模块 |
| 2.4 | DISCOVER_TABLESPACE | R | DD / tablespace catalog |
| 2.5 | RECOVER_DDL | R | DD / DDL 模块 |
| 2.6 | RU/SERIALIZABLE、locking SELECT/current read 语义接线、prepared/recovered-active 事务状态、XA | T/R | 0.17 LockManager + session/executor + trx recovery |
| 2.7 | B+Tree current read lock ref + 重定位协议 + MVCC 逻辑唯一检查（§7.3/§9.4，唯一检查现为物理重复，delete-marked 同 key 仍算重复） | B/T | 0.17 LockManager + executor/current-read facade |
| 2.8 | B+Tree 普通查询/DML 上层生产接线（engine/trx 已有服务级接线；session/executor/DD 尚未驱动普通用户索引访问） | B | executor / DD |
| 2.9 | 完整 tablespace discovery（扫描 data dir + DD/SDI 重建 registry）| D | DD / SDI（与 R 2.4 DISCOVER_TABLESPACE 同根）|
| 2.10 | 普通 lifecycle DISCARD/DROP 持久化 + drop tablespace 文件生命周期 | D | DDL discard/drop 语义 |

## 推荐路线

**已完成链：0.1 redo flusher ✅ → 0.2 recoverable doublewrite ✅（含 R 1.1）→ 0.3 持久 rseg header ✅ →
R 1.2 ROLLBACK_TRX ✅（恢复回滚，显式配置索引）→ 0.4 后台 purge driver ✅ → 1.3 RESUME_PURGE ✅**（恢复期
重建 committed history、复位事务计数、后台自动 purge）。下一步不要再做 1.3；可在 crash-safety 主线上继续补
formal UNDO_ROLLBACK/RESUME_PURGE recovery stage、多索引/DD/prepared txn，或切回 Tier 0 独立项（0.1 commit durable policy、
0.6 adaptive flush、0.7 flush/recovery 碎片、0.14 SpaceReservation、0.16 fil.lock 清理、0.21 Record、0.22 BP stale-frame、0.23 MTR 纪律等）。

**btree / buffer-pool / record 的 Tier 0 项（0.8–0.13、0.21–0.23）与上面这条路线并行、互不阻塞**，但不是当前 crash-safety 主线，按需挑：
若转向"让 buffer pool 更接近生产质量"，0.8/0.9 已落，下一步是 **0.10 read-ahead/PageHashTable/multi-instance** 或 **0.22 stale-frame 版本语义**；
B+Tree 多层结构（**0.11 parent split ✅**）、删后收缩（**0.12 merge + root shrink ✅**）与 **0.12b redistribute ✅** 已落；继续做 **0.13** btree 专用 redo / latch coupling / MVCC 辅助字段。
B+Tree 的 current-read 语义与 executor/DD 上层接线（2.7/2.8）被 LockManager / executor 卡，和 U/T 的 2.1 一样属"先立上层"。

**redo 本体 Tier 0 项（0.18–0.20）**：0.1 的后台 flusher、append/fsync 拆锁、recent_written/recent_closed 和 closedLsn 已接；剩余 0.1 是 commit durable policy。随着后台 flusher 长跑，**0.18 文件轮转** 成为必要（**已全部落地并设为引擎默认**：文件环 + checkpoint 回收 + 跨文件恢复，`EngineConfig` 默认即文件环、`withSingleFileRedo()` opt-out；残留仅环满容量分级 throttle=0.6、log block checksum=0.20，均为独立项）；0.19 逻辑 redo handler 可随 0.13 btree 稳定后按需加；0.20 DurabilityPolicy + LogBlock checksum 是 commit 性能与 redo 韧性收尾。三者均 storage 内闭环，不阻塞 crash-safety 主线。

**transaction/current-read 主线**：若转向锁定读、SERIALIZABLE 或 MVCC 逻辑唯一检查，先做 **0.17 最小 LockManager 内核**；
它是 storage 内可独立验证的前置，之后再把 B+Tree 重定位协议和 executor/DML facade 接上。

**Tier 2 暂不碰**：要么需先立 DD/DML facade（2.1 是关键总开关，但本身是独立 epic），要么需 LockManager；
硬接会破坏当前干净的分层。
