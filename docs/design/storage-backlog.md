# Storage Backlog（依赖排序的缺口清单）

> 长期参考资产，不是实现计划。聚合八份厚设计文档的目标架构与当前实现的差距，按依赖关系排序，
> 用于"开工前定序"。**权威缺口与实现状态以 `current-implementation-map.md` 和源码为准**；本文件只做跨文档、
> 按依赖的归并排序。条目落地后应从对应 Tier 中删除或下移。
>
> 覆盖文档：`innodb-flush-checkpoint-doublewrite-design.md`(F)、`innodb-undo-log-purge-design.md`(U)、
> `innodb-transaction-mvcc-design.md`(T)、`innodb-crash-recovery-design.md`(R)、`innodb-btree-design.md`(B)、
> `innodb-buffer-pool-design.md`(BP)、`innodb-disk-manager-design.md`(D)、`innodb-redo-log-design.md`(Redo)。
> 其余模块（DD/DDL、lock observability）只在作为依赖出现时引用。
>
> 最近校对：2026-06-24（基于 commit 8b9214a 之后的未提交工作树：page0 FSP_HDR 校验、WAL 安全脏页淘汰已完成；redo 本体覆盖补入）。

## 七份文档的当前完成度（粗估，表达"核心在、外围缺"）

| 文档 | 已闭环的核心 | 主要缺口 | 粗估 |
| --- | --- | --- | --- |
| F flush/checkpoint/doublewrite | FLUSH_LIST / SINGLE_PAGE / SHUTDOWN / **LRU_FLUSH(WAL 安全淘汰)**、WAL gate、fuzzy checkpoint + 持久 label、PageCleanerWorker | doublewrite 生产未接线(NoDoublewrite)、slot 回收、DETECT_ONLY、真 adaptive、压力 throttle、后台 redo flusher、metrics | ~60% |
| U undo/purge | undo 写(INSERT/UPDATE/DELETE_MARK)、real rollback、MVCC 旧版本读、单线程聚簇 purge、后台 purge driver、可恢复 truncation、恢复期 committed history 重建 | **无 session/executor 驱动的生产 DML**、多 worker、二级索引、statement/savepoint rollback、extern payload、全 release 路径持久 | ~58% |
| T transaction/mvcc | `TransactionManager/System`、事务状态机、RR/RC `ReadView`、MVCC 旧版本读、rollback service、purge low water、engine 服务级接线、恢复期 counter restore | **无 LockManager / current read**、无 session/executor DML facade、RU/SERIALIZABLE、statement/savepoint rollback、PREPARED/RECOVERED_ACTIVE/XA、formal recovery stage | ~55% |
| R crash-recovery | 编排骨架(gate/stage/report)、REDO_REPLAY 真回放、REPAIR_DOUBLEWRITE 生产接线、UNDO_TABLESPACE_RESUME、SPACE_FILE_RECONCILE、ROLLBACK_TRX engine 后恢复步、RESUME_PURGE engine 后恢复步、forceAll、fail-closed | DISCOVER_TABLESPACE、RECOVER_DDL、formal UNDO_ROLLBACK/PURGE_RESUME stages、多索引/DD/prepared txn、非 NORMAL mode | ~55% |
| B btree | 值对象 + BTreePage、root→leaf S 查找、point lookup、range scan + sibling、insert no-split、**root-leaf split→level-1**、delete-mark + purge 物理删除 | **parent split(树高>1)**、merge/redistribute/root shrink、btree 专用 redo、current-read 锁、executor/DD/DML facade 接线、MVCC 逻辑唯一 | ~55% |
| BP buffer-pool | get/new/fix、page latch、`PageGuard`、residentMap + free list、MTR memo + 标脏、flush 协作接口、clean 淘汰 + **dirty 淘汰(WAL 安全)** | **midpoint LRU + 扫描抗污染**、**IO 移出 poolLock(per-frame loading)**、`FrameStateMachine`(FLUSHING)、read-ahead、warmup dump/load、多 instance 分片 | ~55-60% |
| D disk-manager | fil/fsp/segment/extent/page 分配、`DiskSpaceManager` facade、page0 header + **FSP_HDR 信封校验**、**autoextend crash-safe**、undo tablespace 可恢复 truncation、registry 运行时准入 | SpaceReservation(§7.1)、`ExtentAllocationPolicy` direction/multi-extent、`FsyncLock` 接线、普通 lifecycle 持久化、page0 checksum、完整 discovery(DD) | ~70% |
| Redo redo-log | `RedoLogManager` 内存/durable 双模式、`PAGE_INIT`/`PAGE_BYTES` 编码、append+flush 同步落盘、`flushedToDiskLsn` WAL gate、`RedoCheckpointStore` 两槽 fuzzy checkpoint+CRC32、`RedoRecoveryReader` checkpoint 扫描、`RedoApplyDispatcher` 物理重放、recovery boundary 连续续写、`RedoCapacityPolicy` 4 级压力 | **无后台 writer/flusher**（append 与 fsync 同 `lock` 串行）、无 redo 文件轮转/回收、单 redo handler（仅物理 PAGE_INIT/PAGE_BYTES）、无 recent written/closed tracker、无 DurabilityPolicy 抽象、无 LogBlock checksum/header-trailer | ~40% |

这些缺口彼此咬合，共同卡点：**生产 driver/组合根**（purge driver、后台 redo flusher、recoverable doublewrite 接线、DML facade）、**持久化 + 恢复重建**（持久 rseg、tablespace/DD discovery、redo 文件轮转）、**未建模块**（DD/DDL、LockManager/current read）。

## Tier 0 — 现在可独立做（storage 内闭环，无新上层依赖）

按杠杆从高到低。

| # | 项 | 源 | 规模 | 价值 / 备注 |
| --- | --- | --- | --- | --- |
| 0.1 | ✅ **后台 redo flusher 已接**（`RedoFlushWorker`，2026-06-24）——周期/on-demand 驱动 `redo.flush()`，淘汰/flush 不再因 redo 未 durable 长时间跳过。**剩余（独立后续项）**：① commit durable policy（`FLUSH_ON_COMMIT`/等待）；② 拆 LSN 分配锁 vs write/flush 锁 + recent_written/recent_closed tracker（§5.5/§5.6，解并发 LSN 乱序、使 append 不被 fsync 阻塞）；③ 修 checkpoint 用 `currentLsn()` 近似 `closedLsn` 的越界窗口 | F/T/Redo | 中 | 后台 flush 已解淘汰/flush 卡顿；剩余项是并发吞吐 + checkpoint 正确性，可拆多片（②③需并发负载才出价值/才好测） |
| 0.2 | ✅ **recoverable doublewrite 已接进 engine**（2026-06-24）：`StorageEngine` 注入 `RecoverableDoublewriteStrategy`（前向）+ E2 配 `DoublewriteRecoveryScanner` + 新增 `DoublewriteFileRepository.pageIds()`（过滤到恢复已打开空间）；e2e torn-page 恢复验证通过。**剩余**：slot 回收（0.5）、`DETECT_ONLY`、全空间 discovery | F | — | 打开了真 torn-page 防护；R 的 1.1（REPAIR_DOUBLEWRITE 生产阶段）已随本片落地 |
| 0.3 | ✅ **持久 rollback segment header + 恢复期 rseg/slot 扫描已接**（2026-06-24）：undo **page3** = `RSEG_HEADER`（`reserveSystemExtent` 预留），`RollbackSegmentHeaderRepository` MTR 读写 slot 目录（redo 保护）；`UndoLogManager` claim/onCommit-release 持久；engine fresh format + 恢复扫描 `restore` 重建。**剩余**：§6.3 history/cached segment 富字段、`RollbackService`/`PurgeCoordinator` release 持久、truncate rebuild 重格式化 page3 | U/T | — | 解锁 R 1.2/1.3；active-vs-committed 判定本身在 R 1.2 |
| 0.4 | ✅ **后台 purge driver 已接**（2026-06-24）：`PurgeTarget` 端口 + `PurgeDriverWorker`（沿用 RedoFlushWorker 形态，周期/on-demand `runBatch`、失败 FAILED）；`StorageEngine` 配 `clusteredIndex`（`configureClusteredIndex`，同 R 1.2 复用）时构造 `PurgeCoordinator` + 启动 driver；money test 后台自动 purge committed delete-mark。剩余：多 worker、二级索引、持久 history、purge→truncate 调度 | U/T | — | 通用生产化仍需 2.1/DD index metadata |
| 0.5 | doublewrite slot 回收 + FlushList/Lru 双文件 + DoublewriteBatch（`afterDataFileWrite` 现 no-op→有界） | F | 中 | 0.2 细化，去无界增长 |
| 0.6 | 真 adaptive flush（redo 生成率/IO capacity/neighbor/idle percent）+ 压力 throttle（sync→前台等 checkpoint、hard→暂停 redo reservation） | F | 中 | 把 `fixed` 简化策略换成设计版 |
| 0.7 | 碎片打包：`DETECT_ONLY` 模式、PageCleaner supervisor 重启、metrics snapshot、清理 legacy `BufferPool.flush`、`drainTablespace` busy-wait 改 condition 唤醒（现 `LockSupport.parkNanos(1ms)` 无 BufferPool 唤醒）、`RecoveryMode` 的 `READ_ONLY_VALIDATE`/`FORCE_SKIP_CORRUPT` | F/R | 小 | 韧性/可观测/收尾，可拼成一片 |
| 0.8 | **Midpoint LRU（old/new 子链）+ 扫描抗污染**（§6.1/§6.4） | BP | 中 | 现 plain LRU(`LinkedHashSet`)，大扫描冲刷工作集；buffer pool 质量最直接提升 |
| 0.9 | **per-frame IO/loading 状态（盘 IO 移出 `poolLock`）+ 显式 `FrameStateMachine`（FLUSHING 态）**（§5.6/§5.7/§7.3） | BP | 中-大 | 解 buffer pool 最大并发瓶颈——单 `poolLock` 串行 miss/evict/flush 盘 IO |
| 0.10 | Read-Ahead（linear/random）、warmup dump/load、多 instance 分片 + 专用 `PageHashTable`、升序 pageId latch 排序 | BP | 各中 | 独立特性，可分片做 |
| 0.11 | **parent split（解锁树高 >1）**（§8.2） | B | 中-大 | btree 最高杠杆内部缺口；现 `ensureRootHasRoomForPointer` 满抛 `BTreeParentSplitRequiredException`、`rootLevel>1` 拒绝 |
| 0.12 | redistribute / merge / root shrink（§8.3） | B | 中 | 删后空 leaf 回收、多层树收缩；依赖 0.11 的多层结构 |
| 0.13 | btree 专用 redo handler、`nonLeafSegment` 分配、索引页头 `PAGE_MAX_TRX_ID`/`PAGE_BTR_SEG_*`、prefix index 比较、`LatchCouplingController`（乐观→悲观下降） | B | 各小-中 | 碎片，部分随 0.11/0.12 一起 |
| 0.14 | **SpaceReservationService（§7.1 reserve-before-multi-page-op）+ SegmentReservation kinds（NORMAL/UNDO/CLEANING/BLOB）** | D | 中-大 | 设计强制的多页操作预留；现 split/grow/allocate 即时分配可半途 ENOSPC。已有消费者（`allocatePage`/btree split/undo grow），跨切面 |
| 0.15 | `ExtentAllocationPolicy` direction（UP/DOWN/NO_DIRECTION）+ 大 segment 顺序增长多 extent（2-4）（§7.3） | D | 中 | 现 direction 恒 RIGHT、单 extent |
| 0.16 | 碎片打包（D）：`FsyncLock` 接入 `DataFileHandle.force`/`PageStore.force`（§8.1）；普通 GENERAL `CORRUPTED` 状态持久化到 page0 + 打开校验；`DataFileGateway`/`PreallocationStrategy`(posix_fallocate adapter) | D | 各小 | `FsyncLock` 是最小独立项；DISCARD/DROP 持久化留 Tier 2（需 DDL）|
| 0.17 | **最小 LockManager 内核**：record/gap/next-key/insert-intention lock、wait queue、timeout、bounded deadlock detector、事务持锁集合 | T | 大 | 可 storage 内闭环单测；解锁 current read、SERIALIZABLE、唯一检查等待/重定位；lock observability 快照可后续接独立设计 |
| 0.18 | **Redo 文件轮转/回收**（§5.7/§10）：当前单 append-only 文件无上限；checkpoint 推进后旧 redo 区间不可回收 | Redo | 中-大 | 长跑必要；与 0.1 后台 flusher 配合，否则 redo 文件无界增长 |
| 0.19 | **逻辑 redo record type + 多 apply handler**（§5.4/§6/§13）：当前仅 `PAGE_INIT`/`PAGE_BYTES` 物理重放，无 MLOG 逻辑 redo（btree split、space header、xdes、undo record insert、trx state）；`RedoApplyDispatcher` 单 handler | Redo | 中 | 减少恢复体积、提升 btree/undo 恢复精度；按需随 0.11/0.13 btree 稳定后逐条加 |
| 0.20 | **DurabilityPolicy 抽象 + LogBlock checksum**（§5.8/§8）：当前无 commit 刷盘策略抽象（恒同步 flush，无 `FLUSH_ON_COMMIT`/`WRITE_ON_COMMIT`/`BACKGROUND_FLUSH` 区分）；redo 块无 header/trailer 校验，torn log write 只靠 record checksum | Redo | 小-中 | commit 路径性能 + redo 文件韧性；DurabilityPolicy 与 0.1 后台 flusher 配合 |

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
formal UNDO_ROLLBACK/RESUME_PURGE recovery stage、多索引/DD/prepared txn，或切回 Tier 0 独立项（0.1 拆锁/closedLsn、
0.2 slot 回收、0.8/0.9 BP、0.11 parent split、0.14 SpaceReservation 等）。

**btree / buffer-pool 的 Tier 0 项（0.8–0.13）与上面这条路线并行、互不阻塞**，但不是当前 crash-safety 主线，按需挑：
若转向"让 buffer pool 更接近生产质量"，**0.8 midpoint LRU + 0.9 IO 移出 poolLock** 杠杆最高（栈底、纯独立）；
若转向"让 B+Tree 能承载多层真实数据"，**0.11 parent split** 是前置（解锁树高 >1，再做 0.12 merge）。
B+Tree 的 current-read 语义与 executor/DD 上层接线（2.7/2.8）被 LockManager / executor 卡，和 U/T 的 2.1 一样属"先立上层"。

**redo 本体 Tier 0 项（0.18–0.20）**：0.1 是 redo 的最高杠杆（后台 flusher + LSN 锁拆分），完成后 redo 文件会开始无界增长，**0.18 文件轮转** 成为长跑必要；0.19 逻辑 redo handler 可随 0.11/0.13 btree 稳定后按需加；0.20 DurabilityPolicy + LogBlock checksum 是 commit 性能与 redo 韧性收尾。三者均 storage 内闭环，不阻塞 crash-safety 主线。

**transaction/current-read 主线**：若转向锁定读、SERIALIZABLE 或 MVCC 逻辑唯一检查，先做 **0.17 最小 LockManager 内核**；
它是 storage 内可独立验证的前置，之后再把 B+Tree 重定位协议和 executor/DML facade 接上。

**Tier 2 暂不碰**：要么需先立 DD/DML facade（2.1 是关键总开关，但本身是独立 epic），要么需 LockManager；
硬接会破坏当前干净的分层。
