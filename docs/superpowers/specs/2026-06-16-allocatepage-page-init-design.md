# Spec：D4a — allocatePage 页初始化（PAGE_INIT + 信封）

- 日期：2026-06-16
- 关联设计：`innodb-disk-manager-design.md`（第 4 步 page allocation 接 MTR/redo）、D3 spec（`docs/superpowers/specs/2026-06-16-mtr-redo-append-skeleton-design.md` §9 衔接点）。
- 上游依赖：storage.api（DiskSpaceManager）、storage.buf（LruBufferPool/PageGuard）、storage.mtr（MiniTransaction.newPage）、storage.page（PageEnvelope/PageType/FilePageHeader）、storage.fsp（分配器）。
- 前置：D3 全绿（MTR redo append；mtr.newPage 已产 PAGE_INIT；commit 盖 pageLSN）。
- 状态：D4a 让 D3 的 PAGE_INIT 机制在真实分配路径上被触发——数据页分配时创建并初始化页（信封 + PAGE_INIT）。

## 1. 背景与范围

D3 让经 MTR 的页写自动产 PAGE_BYTES、newPage 产 PAGE_INIT、commit 盖 pageLSN。但当前 `DiskSpaceManager.allocatePage` 只翻 XDES 位图返回 PageId，**不调 mtr.newPage、不写信封**，所以 PAGE_INIT 从未实际产生、分配出的数据页没有信封头。

D4a 补这一步：`allocatePage` 分配出数据页后，对该页走「页创建」——`mtr.newPage(pageId, X, ALLOCATED)` + `PageEnvelope.writeHeader`，于是产生 `PAGE_INIT(ALLOCATED)` + 信封 PAGE_BYTES，commit 盖 pageLSN。

**做**：`allocatePage` 数据页初始化；放宽 `buf.newPage` 以容忍「页已驻留」（页创建重初始化语义）。

**不做**（注释标注，后续片）：FSP 结构页（page0 FSP_HDR、page2 INODE）的信封 + PAGE_INIT、tablespace-create redo（与 R1 file-create redo 纠缠）；freePage/dropSegment 不新增页级处理（其位图改动 D3 已产 PAGE_BYTES）；数据页类型仍为 ALLOCATED（消费者 btree 后续 format 时改 INDEX，本仓库暂无 btree）。

## 2. 关键问题：页创建必须容忍「页已驻留」

`LruBufferPool.newPage`（= `acquire(readFromDisk=false)`）对**已驻留**页抛 `DatabaseValidationException("newPage for resident page")`。这是保守保护，且**当前无生产调用者依赖它**（D3 前 newPage 仅 MTR 测试用）。但 D4a 后，allocate→free→realloc **同一页**（同 MTR，见 `allocateFreeReallocateRecyclesPage`/`dropSegmentReclaimsAndAllowsSlotReuse` 两测试）会让第二次创建命中驻留页 → 抛错、测试破。

InnoDB `buf_page_create` 对此是**重新初始化**（清零帧 + 复用），不报错。本片采同一语义。

## 3. 关键决策（写进代码注释）

1. **residency = 重初始化（approach 1），但清零必须在取得 X latch 之后做**：`LruBufferPool.newPage` 对驻留页改为「复用帧 + fix + 重初始化（清零）」，语义对齐 InnoDB buf_page_create。**关键并发约束**：现有 `acquire` 在 `poolLock` 内选帧、释放 `poolLock` 后才取 page latch；驻留页的内容清零**不能在 `poolLock` 内做**（那时未持 page latch，会绕过 page latch 语义、与持锁读者撞车）。正确顺序：`poolLock` 内只 `fixCount++`、`dirty=true`、`onAccess`、记 `resetAfterLatch=true`；**释放 `poolLock`、取得 X latch 后**再 `Arrays.fill(chosen.data, 0)`。清零仍**不经 PageGuard → 不产 PAGE_BYTES**；清零的恢复语义由 PAGE_INIT 承担。非驻留路径（victim 清零）仍在 `poolLock` 内（新帧尚未发布、无其它持锁者，安全），不改。
2. **数据页类型 = ALLOCATED**：分配时页未定用途，按 InnoDB 取 `FIL_PAGE_TYPE_ALLOCATED`；消费者后续 format 时再改类型（PAGE_BYTES 写 PAGE_TYPE）。
3. **信封 pageLsn 写 0，commit 盖真值**：`PageEnvelope.writeHeader` 写 `pageLsn=0`（会被 collector 捕获成一条 offset=20 的 PAGE_BYTES=0）；commit 的 `stampPageLsn` 用 endLsn 覆盖（collector 已 disable，不再捕获）。replay：PAGE_INIT 建页 → PAGE_BYTES 填信封(含 lsn=0) → recovery 置 pageLSN=endLsn（D3 契约）。lsn=0 的冗余 redo 可接受（不另改 writeHeader）。
4. **page 创建在 facade（DiskSpaceManager.allocatePage）做**：它持 `pool`、拿到 autoextend 后最终的 pageId、且在调用方传入的同一 mtr 内；分配器（SegmentPageAllocator）只管位图，不碰数据页帧，职责不变。

## 4. buf 改动（D4a-1）

`LruBufferPool` 的页创建语义调整：

**(a) newPage 必须持 X latch**：`newPage`（或 acquire 的 `!readFromDisk` 入口）校验 `mode == EXCLUSIVE`，否则抛 `DatabaseValidationException`。页创建/重初始化是写操作，不允许 `newPage(page, SHARED)` 走清零语义。（现有 newPage 调用方均用 EXCLUSIVE，安全。）

**(b) 驻留页重初始化（X latch 后清零）**：`acquire` 的 resident 分支，`!readFromDisk` 时：
- 原：抛 `DatabaseValidationException("newPage for resident page: ...")`。
- 改：`poolLock` 内 `resident.fixCount++`、`resident.dirty = true`、`policy.onAccess(resident)`、`chosen = resident`、置局部 `boolean resetAfterLatch = true`。
- `acquire` 末尾取得 latch 后（newPage 恒 X）：`if (resetAfterLatch) Arrays.fill(chosen.data, (byte) 0);` 再 `return new PageGuard(...)`。
- 这样清零在 X latch 保护下进行、不经 PageGuard（不产 PAGE_BYTES）。
- 非驻留分支（victim + poolLock 内 `Arrays.fill`）不变。

**(c) Javadoc**：`newPage` 与 `BufferPool.newPage` 接口标注——newPage 为「页创建」语义、要求 X latch；驻留页将被**重初始化（清零复用）**（对齐 InnoDB buf_page_create），调用方须确保该页确实在被（重新）分配，不能误覆盖在用页。

## 5. allocatePage 改动（D4a-2）

`DiskSpaceManager.allocatePage(mtr, ref)`：现有逻辑得到 `PageId p`（含一次 autoextend 重试）后，新增：
```
PageGuard g = mtr.newPage(pool, p, PageLatchMode.EXCLUSIVE, PageType.ALLOCATED);
PageEnvelope.writeHeader(g, new FilePageHeader(
        p.spaceId(), p.pageNo().value(),
        FilePageHeader.FIL_NULL, FilePageHeader.FIL_NULL, 0L, PageType.ALLOCATED));
return p;
```
- 不自行 `close(g)`（mtr memo 持有，commit 释放）。
- `NoFreeSpaceException` 路径不变（在 newPage 之前抛出，未创建任何页）。
- import 增 `storage.buf.PageLatchMode`（已存在）、`storage.buf.PageGuard`、`storage.page.PageEnvelope`、`storage.page.FilePageHeader`、`storage.page.PageType`。

## 6. 产出的 redo（每次 allocatePage）

- XDES 位图改动 → `PAGE_BYTES`（page0，D3 已自动）。
- 新数据页 → `PAGE_INIT(p, ALLOCATED)` + `PAGE_BYTES`（信封字段，含 pageLsn=0）。
- commit → 给所有 touched 页（page0 + 新数据页 + 其它被改的 FSP 页）盖同一 `endLsn`。

## 7. 异常与并发

- 复用 `DatabaseValidationException`、`NoFreeSpaceException`、`MtrStateException`。
- 并发：allocatePage 在调用方单 MTR 内，数据页 X latch 入 memo 持到 commit；buf 重初始化在 `poolLock` 内（沿用 acquire 现有临界区），不引入新锁。
- 同 MTR 回收（allocate→free→realloc 同页）：第二次 newPage 走重初始化路径，memo 出现该页两条 X 槽（重入 X 合法），commit 各释放、guardFor 取最近一条盖 pageLSN 一次。

## 8. 测试

- **改写既有测试**：`LruBufferPoolTest.newPageShouldRejectResidentPage`（断言驻留页 newPage 抛异常）随 D4a-1 行为反转——**删除/改写**为 `newPageReinitializesResidentPage`：先 getPage(X) 写若干字节使页驻留+有内容并释放，再 `newPage(同页, X)` → 返回帧已清零、不抛、可写。执行计划必须显式处理此旧测试，勿遗漏。
- buf：`newPageRejectsSharedMode`——`newPage(page, SHARED)` 抛 `DatabaseValidationException`（页创建须 X）。
- buf 并发边界：`newPageOnResidentBlocksUntilSharedReleasedThenZeroes`——线程 A 持驻留页 SHARED guard 且页内有非零内容；线程 B `newPage(同页, EXCLUSIVE)` 必须**阻塞**（A 未释放前 B 取不到 X latch），且**A 释放前页内容不被清零**（A 仍能读到原内容）；A 释放后 B 返回**零页**。钉死「先取 X latch 后清零」的实现顺序。（用 CountDownLatch/超时同步两线程，避免无界等待。）
- api：`allocatePageEmitsPageInitAndStampsEnvelope`——allocate → commit；redo buffer 含 `PageInitRecord(p, ALLOCATED)` + 信封 PAGE_BYTES；重新 `pool.getPage(p, SHARED)` 读 `PageEnvelope.readHeader`：spaceId/pageNo 正确、type=ALLOCATED、pageLSN==endLsn。
- api：`reallocateResidentPageReinitializes`——复刻 `allocateFreeReallocateRecyclesPage` 同 MTR 回收，断言第二次 allocate 不抛、回收页号正确、信封被重置（type=ALLOCATED）。
- 回归：DiskSpaceManagerTest 全绿；MTR/redo 测试全绿。**注意 pool 容量**：D4a 后每次 `allocatePage` 多 fix 一个数据页 X latch 且**持到 mtr commit**（commit 才盖 pageLSN），故「单 MTR 批量分配 N 页」会同时占 N 个数据页帧 + FSP 工作集。现有 `allocateAutoextendsWhenExhausted`(33)、`dropSegmentReclaimsAndAllowsSlotReuse`(33) 在一个 MTR 内分配 33 页，`withDsm` 的 `pool=16` 会 `BufferPoolExhaustedException`。**修法**：把 `withDsm` 的 pool 由 16 调到 64（仅测试基建；不影响断言；DiskSpaceManagerTest 不测 buffer 淘汰）。这是 D3「commit 才盖 pageLSN ⇒ touched 页 guard 持到 commit」模型的自然结果（真实代码应短 MTR、按页分配，不在一个 MTR 内批量持有几十页 latch）。

## 9. Impact（编辑前跑，CLAUDE.md）

- `LruBufferPool`/`BufferPool.newPage`（buf，**CRITICAL 区**）：行为变更（驻留页 throw→reinit）。先跑 `gitnexus_impact` 告警；确认无测试依赖旧 throw（grep `"newPage for resident"`）。
- `DiskSpaceManager.allocatePage`（api）。

## 10. 批次拆分（writing-plans 细化）

- **D4a-1**：buf.newPage 要求 X latch + 驻留页「取 X latch 后清零」重初始化（acquire 加 resetAfterLatch）；**改写旧测试 `newPageShouldRejectResidentPage`** → `newPageReinitializesResidentPage`，新增 `newPageRejectsSharedMode` 与并发边界 `newPageOnResidentBlocksUntilSharedReleasedThenZeroes`。impact：LruBufferPool/BufferPool（CRITICAL，告警 + 全量回归）。
- **D4a-2**：allocatePage newPage+writeHeader + api 测试（PAGE_INIT/信封/回收）。impact：DiskSpaceManager.allocatePage。
- 每批 TDD → 全量 `clean test` → `gitnexus_detect_changes`。D4a 全绿后报告；D4b 单独确认再开。
