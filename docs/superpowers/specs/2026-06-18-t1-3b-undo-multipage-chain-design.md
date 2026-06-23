# Spec: T1.3b — 多页 undo 链物理基座（undo log segment + 跨页 append/遍历 + 端口隔离分配）

- 日期：2026-06-18
- 关联设计：`docs/design/innodb-undo-log-purge-design.md` §5.4（RollbackSegment/UndoSegment 边界）、§6.4（undo page header 与 undo log header 拆分）、§6.5（INSERT_ROW payload）、§9.3/§9.4（undo 分配/page latch 锁顺序与等待边界）、§16（实现顺序 step 5/6）；`docs/design/innodb-disk-manager-design.md` §5.5/§6.4（Segment/SegmentInode）、§7.2（SegmentPageAllocator fragment→extent 路径）、§9（MTR memo）。
- 前置：T1.3a（`UndoNo`、`UndoRecord`、`UndoRecordCodec`、`UndoPage`/`UndoPageAccess`/`UndoLog`、`PageType.UNDO`、`UndoPageOverflowException`/`UndoLogFormatException`）；record R1-R5；redo D3/D4（`PAGE_INIT`/`PAGE_BYTES` 物理 redo + commit pageLSN）；FSP `DiskSpaceManager`（`createSegment(UNDO)`/`allocatePage`/`SegmentRef`）；MTR `MiniTransaction`（`getPage`/`newPage` fresh fix、禁 S→X 升级、`writeSiblingLinks`）。
- 状态：T1.3「物理 undo + 真 rollback」epic 的第二子片（**仍为横切物理基座**）。目标是把 T1.3a 单页 undo 扩成可跨页生长的 undo log segment：append 溢出时分配并 FIL 链入新 undo 页、拆分 undo page header / undo log header、整链正向遍历、持久化重读。**完全不接事务/btree/rollback；聚簇 `DB_ROLL_PTR` 仍恒 NULL。**

## 0. epic 拆分与本片定位（在 T1.3a 基础上重排）

「物理 undo + 真 rollback」按以下子片推进，各自独立 spec → plan → 实现：

- **T1.3a（已完成）：undo 存储基座（单页）。** undo page 格式、`UndoPageAccess`、`UndoRecordCodec`、`RollPointer` 真实寻址、`UndoNo`。单 undo page 内 `append→RollPointer`、`read(RollPointer)→record`；页满抛 `UndoPageOverflowException`。
- **T1.3b（本片）：多页 undo 链物理基座。** undo log segment（一条跨页 undo log 的页链）、append 溢出生长（FSP 分配 + FIL 链接）、undo page header / undo log header 拆分、整链正向遍历、持久化重读。端口隔离 undo→api 的分配依赖。**不接 `Transaction`/`UndoContext`/rollback segment/`insertClustered`/真 `DB_ROLL_PTR`。**
- **T1.3c：undo 写路径接线。** rollback segment header/slot、`UndoContext`（挂 `Transaction`）、`UndoLogManager.assignUndoContext`/`beforeInsert`，让 `insertClustered` 写 INSERT undo 并把 `DB_ROLL_PTR` 改成真实指针（替换当前恒 NULL）。
- **T1.3d：真 rollback。** btree 物理删除路径、`RollbackService.rollback(FULL)` 反向走 insert undo 链、`TransactionManager.rollback` 驱动 undo 应用并释放 undo；commit 把 insert undo 标记 reusable。
- **T1.3e+：** UPDATE/DELETE undo + history list + commit-to-history → MVCC 读 → purge → 崩溃恢复 rollback → truncation/多 rseg/多 undo 表空间。

（T1.3a spec §0 曾把「多页链」与「写路径」合并称作 T1.3b；本片按用户决定**只取物理多页链**，写路径后移为 T1.3c，其余顺延。）

## 1. 范围

**做：**

- `cn.zhangyis.db.storage.undo` 新增：
  - `UndoSpaceAllocator`（端口接口，**undo 自有**）：用 `domain` 类型暴露「建 undo segment」「在 segment 内分配一页」，使 `storage.undo` 不依赖 `storage.api`。
  - `UndoSegmentHandle`（undo 自有值对象）：`spaceId`/`inodeSlot`/`segmentId`/`firstPageId`/`lastPageId`，**不暴露 `SegmentRef`**。
  - `UndoLogSegment`：一条跨页 undo log 的 MTR 内句柄（持 handle + first 页视图 + current/last 页视图 + 本会话已持页表）；`append`/`readRecord`/`forEachRecord`/header 访问器。
  - `UndoLogSegmentAccess`：MTR 生产入口（仿 `UndoPageAccess`），持 `BufferPool`+`PageSize`+`UndoSpaceAllocator`+`UndoRecordCodec`；`create`/`open`。
- `cn.zhangyis.db.storage.api` 新增：
  - `DiskSpaceUndoAllocator`（适配器，**实现 undo 的端口**）：把 `UndoSegmentHandle` 转 `SegmentRef`（`storage/api/SegmentRef.java`）再调 `DiskSpaceManager`（`createSegment(UNDO)`/`allocatePage`）。`storage.api → storage.undo` 的端口 import 允许；反向不出现。
- `cn.zhangyis.db.storage.undo` 修改：
  - `UndoPageLayout`：拆分为 undo **page** header（每页）+ undo **log** header（仅 first 页填，**所有页预留同宽**），`RECORD_AREA_START` 仍为单一常量。
  - `UndoPage`：增 `segmentId()`/`inodeSlot()`/`isFirstPage()`、log-header 访问器（仅 first 页有效）、FIL 链 `prevPageNo()`/`nextPageNo()`；`format` 拆为 `formatFirstPage(...)`/`formatChainPage(...)`；header 写按拆分后偏移。
  - `UndoPageAccess`：`createUndoPage` 区分 first / chain 页（first 写 log header，chain 只写 page header + 预留清零 log header 区）。
  - `package-info`：更新包职责说明。

**不做（本片非目标，移交 T1.3c+）：**

- 不接 `Transaction`/`TransactionManager`/`UndoContext`/`UndoLogManager`/`insertClustered`；**不写真 `DB_ROLL_PTR`（聚簇记录 `DB_ROLL_PTR` 仍恒 NULL）**。
- 不引入 rollback segment header / slot array / slot directory / history list / cached-segment 复用。
- 不做 undo 页 free / undo segment 回收 / undo tablespace truncation（→ purge 片）。
- 不做 rollback、不做 UPDATE/DELETE undo、不做 MVCC 旧版本构造、不做恢复期 rollback。
- 不做单条超页 undo record 的跨页/external payload（保持「单 record 不跨页」，超空页仍抛 `UndoPageOverflowException`）。
- 不做并发 append：同一 undo segment 同时只有一个 writer（见 §6）。
- 不改 `RollPointer` 格式、不做多 rseg-id / 多 undo 表空间编码。

## 2. 关键决策

1. **横切物理基座续作**：复刻 T1.1/T1.2/T1.3a「先落承重物理格式 + 全量回归保护，再接线」节奏。多页链是 undo 的承重物理件，隔离实现与测试、零事务接线风险；写路径与真 rollback 在 T1.3c/d 接入。

2. **端口 + 适配器隔离 undo→api 依赖（承重约束）**：append 溢出生长必须在 undo 内部触发页分配，但 CLAUDE.md 依赖方向为 `storage.api → … → undo`，`storage.undo` 不得 import `storage.api.DiskSpaceManager`/`SegmentRef`。故：
   - `UndoSpaceAllocator`（端口）置 `storage.undo`，方法签名**不暴露任何 `storage.api` 类型**（无 `SegmentRef`/`DiskSpaceManager`）；允许使用 `MiniTransaction`（`storage.mtr`）、`domain` 值对象（`SpaceId`/`SegmentId`/`PageId`）与 undo 自有 `UndoSegmentHandle`。`NoFreeSpaceException`（`storage.fsp` unchecked）由适配器透传。
   - `UndoLogSegmentAccess` 只依赖 `UndoSpaceAllocator`，**不知道 `DiskSpaceManager` 存在**。
   - `DiskSpaceUndoAllocator`（适配器）置 `storage.api`，内部把 `UndoSegmentHandle` 转 `SegmentRef` 再调 `DiskSpaceManager.createSegment(UNDO)`/`allocatePage`。
   - 允许方向：`storage.api → storage.undo`（适配器 import 端口）。禁止方向：`storage.undo → storage.api`。`UndoSegmentHandle` 保留 `spaceId`/`inodeSlot`/`segmentId`/`firstPageId`/`lastPageId`，**不暴露 `SegmentRef`**。

3. **undo page header / undo log header 拆分（设计 §6.4），但统一 `RECORD_AREA_START`**：多页链下每页都有 page header（segment 归属 + 页内分配游标），仅 first 页有 log header（事务 + 链端点）。为简化 offset 数学，**所有页都预留 log header 同宽空间**：非 first 页的 log header 区**必须清零**（`formatChainPage` 清零），读取时只按 `PAGE_FLAGS.isFirstPage` 决定是否解析 log header。换来：`recordAt(offset)` 不区分 first/non-first、`RollPointer.offset` 语义稳定、redo replay / reopen / forward scan 边界易测、页格式损坏检查简单。代价：非 first 页浪费约 34B（教学取舍，写进注释）。

4. **页链复用 FIL prev/next + log header 端点指针**：per-page 链用 FIL header `PREV_PAGE_NO`/`NEXT_PAGE_NO`（`PageEnvelope.writeSiblingLinks`，与 B+Tree leaf 兄弟链同机制）。first 页 log header 另存 `FIRST_PAGE_NO`/`LAST_PAGE_NO`，使 append/reopen 能 O(1) 跳到尾页续写，不必 O(pages) 走链。正向遍历从 first 页沿 FIL next 链。

5. **读已持页直读、未持页才 S-fix（latch reentrancy 边界）**：`MiniTransaction.getPage` 每次取**全新** fix+latch（不复用 memo），且禁 S→X 升级。故 `readRecord`/`forEachRecord` 对**本会话已持有的页直接用其 `UndoPage` 视图读**（不再 `getPage`），只对**未持有的页**用 `mtr.getPage(S)`。避免「对已持 X 的页再 `getPage(S)`」导致的二次 fix 与 commit 期 `guardFor` 取到 S guard 无法盖 pageLSN 的风险。`UndoLogSegment` 维护「本会话 pageNo→UndoPage」表收纳所有自己持有的页（first + 生长出的每页）。

6. **undo 页写全程经 MTR-owned `PageGuard`**：`create`/`formatFirstPage`/`formatChainPage`/`appendRecord`/链接/log header 更新全部经 guard 字节写，产 `PAGE_INIT(UNDO)`/`PAGE_BYTES`、commit 盖 pageLSN。复用 D3/D4，本片不新增 redo 类型/恢复编排。

7. **`DB_ROLL_PTR` 仍恒 NULL（反复钉死）**：本片不触碰 `insertClustered`、不写聚簇隐藏列；undo log 与聚簇记录尚无任何指针关联。该约束在范围、测试、自检中重复声明。

## 3. 端口、适配器与值对象

### 3.1 `UndoSegmentHandle`（`storage.undo` 值对象）
- 不可变 `record UndoSegmentHandle(SpaceId spaceId, int inodeSlot, SegmentId segmentId, PageId firstPageId, PageId lastPageId)`。
- 校验：字段非 null；`inodeSlot >= 0`；`segmentId.value() > 0`；`firstPageId`/`lastPageId` 的 `spaceId` 等于 `spaceId`。
- `withLastPage(PageId)` 返回更新 `lastPageId` 的新实例（生长时用；不可变）。
- 语义：一条 undo log segment 的逻辑+物理定位。`inodeSlot`/`segmentId` 落盘到每页 page header，使 reopen 能重建 handle 续分配；`firstPageId`/`lastPageId` 是链端点。

### 3.2 `UndoSpaceAllocator`（`storage.undo` 端口接口）
| 方法 | 签名 | 语义 |
| --- | --- | --- |
| `createUndoSegment` | `(MiniTransaction mtr, SpaceId undoSpace) -> UndoSegmentHandle` | 建一个 UNDO segment 并分配首页（裸 ALLOCATED 页，未格式化）；返回 handle（`firstPageId==lastPageId==`首页） |
| `allocatePage` | `(MiniTransaction mtr, SpaceId undoSpace, int inodeSlot, SegmentId segmentId) -> PageId` | 在该 segment 内分配一页（裸 ALLOCATED 页），供生长 |

- 签名不暴露任何 `storage.api` 类型（无 `SegmentRef`/`DiskSpaceManager`）；允许使用 `MiniTransaction`（`storage.mtr`）、`domain` 值对象与 undo 自有 handle。空间不足由实现抛 `NoFreeSpaceException`（`storage.fsp` 的 unchecked 异常，由适配器透传，不在 undo 重新包装）。

### 3.3 `DiskSpaceUndoAllocator`（`storage.api` 适配器，实现端口）
- 构造持 `DiskSpaceManager`。
- `createUndoSegment`：`SegmentRef ref = dsm.createSegment(mtr, undoSpace, SegmentPurpose.UNDO)`；`PageId first = dsm.allocatePage(mtr, ref)`；返回 `new UndoSegmentHandle(undoSpace, ref.inodeSlot(), ref.segmentId(), first, first)`。
- `allocatePage`：`SegmentRef ref = new SegmentRef(undoSpace, inodeSlot, segmentId)`；`return dsm.allocatePage(mtr, ref)`。
- 适配器是 `storage.api → storage.undo` 唯一接触点；不持久化、无状态、线程安全。

### 3.4 `RollPointer`（复用，不改）
- 维持 7B codec（insert flag, pageNo u32, offset u16）。多页链下 `pageNo` 指向链上**任意页**，`offset` 指向该页 record 槽起点；单 undo space 假设下足以寻址。

## 4. undo page 格式（头部拆分）

### 4.1 布局（FIL 信封内，单一真相 `UndoPageLayout`）

```
[FIL header 38B][undo page header 25B][undo log header 34B（仅 first 页填，余页清零预留）][undo record area → 向后追加][free][FIL trailer 8B]
```

undo **page** header（每页，紧接 FIL body 起始 = `PageEnvelopeLayout.FIL_PAGE_HEADER_BYTES` = 38）：

| 偏移 | 字段 | 宽度 | 语义 |
| --- | --- | --- | --- |
| 38 | `FREE_OFFSET` | u16 | 下一条记录追加位置；format 初始化为 `RECORD_AREA_START` |
| 40 | `RECORD_COUNT` | u16 | **本页**已追加 undo record 数 |
| 42 | `PAGE_LAST_UNDO_NO` | u64 | **本页**最近一条 record 的 undoNo（0=本页空） |
| 50 | `SEGMENT_ID` | u64 | 所属 UNDO segment id（reopen 重建 handle + readRecord 校验用） |
| 58 | `INODE_SLOT` | u32 | SegmentRef.inodeSlot（reopen 续分配 + readRecord 校验用） |
| 62 | `PAGE_FLAGS` | u8 | bit0 = `isFirstPage`（是否含 log header），其余预留 0 |

undo **log** header（仅 first 页填；非 first 页该区清零；紧接 page header = 63）：

| 偏移 | 字段 | 宽度 | 语义 |
| --- | --- | --- | --- |
| 63 | `TRANSACTION_ID` | u64 | 该 undo log 所属事务 id（本片测试给字面值） |
| 71 | `UNDO_KIND` | u8 | `UndoLogKind` ordinal（本片恒 `INSERT`） |
| 72 | `STATE` | u8 | undo log 状态占位（本片恒 `ACTIVE`） |
| 73 | `FIRST_PAGE_NO` | u32 | 链首页号（= 自身页号） |
| 77 | `LAST_PAGE_NO` | u32 | 链尾（当前 append）页号；生长时推进 |
| 81 | `LOG_RECORD_COUNT` | u64 | **整链** undo record 总数 |
| 89 | `LOG_LAST_UNDO_NO` | u64 | **整链**最近一条 record 的 undoNo |

- `RECORD_AREA_START = 97`（= 63 + 34，**单一常量，所有页一致**）。
- per-page 链 prev/next 用 FIL header `PREV_PAGE_NO`/`NEXT_PAGE_NO`（不在 undo header 内）。
- undo record 槽 = `[len u16][payload len 字节]`（同 T1.3a）。

### 4.2 `UndoPage`（PageGuard 视图，修改）

- `formatFirstPage(UndoLogKind kind, TransactionId txnId, UndoSegmentHandle handle)`（要求 X）：写 page header（`FREE_OFFSET=RECORD_AREA_START`、`RECORD_COUNT=0`、`PAGE_LAST_UNDO_NO=0`、`SEGMENT_ID`/`INODE_SLOT`、`PAGE_FLAGS.isFirstPage=1`）+ log header（`TRANSACTION_ID`、`UNDO_KIND`、`STATE=ACTIVE`、`FIRST_PAGE_NO=LAST_PAGE_NO=`自身、`LOG_RECORD_COUNT=0`、`LOG_LAST_UNDO_NO=0`）。
- `formatChainPage(UndoSegmentHandle handle)`（要求 X）：写 page header（同上但 `isFirstPage=0`）+ **清零 log header 区 [63, 97)**（避免残留字节被误解析）。
- `appendRecord(byte[] payload, UndoNo undoNo) -> int`（要求 X）：同 T1.3a（校验 `undoNo` 非 NONE、溢出判定 `free + 2 + len > pageSize - FIL_TRAILER` 抛 `UndoPageOverflowException`、写 `[len][payload]`、推进 `FREE_OFFSET`/`RECORD_COUNT`/`PAGE_LAST_UNDO_NO`），返回槽起点 offset。**不更新 log header**（log header 由 `UndoLogSegment` 在 first 页更新，见 §5.2）。
- `recordAt(int offset) -> byte[]`：同 T1.3a（按本页 `FREE_OFFSET` 校验 `offset`/`len` 出界抛 `UndoLogFormatException`）。
- 链接：`linkNextTo(PageNo next)`（要求 X，写 FIL `NEXT_PAGE_NO`，**保留 PREV**——读自身 header prev 再 `writeSiblingLinks(guard, prevExisting, next)`）；`linkPrevTo(PageNo prev)` 同理保留 NEXT。
- log header 更新器（仅 first 页，要求 X）：`setLastPageNo(PageNo)`、`setLogRecordCount(long)`、`setLogLastUndoNo(long)`。调用前 `requireFirstPage()`，非 first 页抛 `UndoLogFormatException`。
- 访问器：`segmentId()`/`inodeSlot()`/`isFirstPage()`/`pageFlags()`/`freeOffset()`/`recordCount()`/`pageLastUndoNo()`/`prevPageNo()`/`nextPageNo()`；first-only：`transactionId()`/`undoKind()`/`state()`/`firstPageNo()`/`lastPageNo()`/`logRecordCount()`/`logLastUndoNo()`（非 first 页调用抛 `UndoLogFormatException`）。

### 4.3 `UndoPageAccess`（修改）

- `createFirstPage(mtr, pageId, kind, txnId, handle) -> UndoPage`：`mtr.newPage(X, UNDO)` → `PageEnvelope.writeHeader(UNDO, prev=next=FIL_NULL)` → `UndoPage.formatFirstPage(...)`。
- `createChainPage(mtr, pageId, handle) -> UndoPage`：`mtr.newPage(X, UNDO)` → `writeHeader(UNDO, prev=next=FIL_NULL)` → `UndoPage.formatChainPage(handle)`。
- `openUndoPage(mtr, pageId, mode) -> UndoPage`：`mtr.getPage(mode)` → 读信封校验 `pageType==UNDO`（否则 `UndoLogFormatException`）→ 包成 `UndoPage`。（保留 T1.3a 行为；多页链下用于按 `RollPointer.pageNo` 取未持页。）
- 校验全部前置于写页前；**破坏性入口**（newPage 清零重初始化）仅用于新分配页。

## 5. `UndoLogSegment` 与 `UndoLogSegmentAccess`

### 5.1 `UndoLogSegmentAccess`（MTR 生产入口）
- 构造持 `BufferPool`+`PageSize`+`UndoSpaceAllocator`+`UndoRecordCodec`。
- `create(mtr, SpaceId undoSpace, TransactionId txnId) -> UndoLogSegment`：
  1. `handle = allocator.createUndoSegment(mtr, undoSpace)`（建 segment + 首页，裸 ALLOCATED）。
  2. `firstPage = pageAccess.createFirstPage(mtr, handle.firstPageId(), INSERT, txnId, handle)`（同页第二次 newPage：ALLOCATED→UNDO，与 T1.3a 双 newPage 同款，redo 顺序最终态 UNDO）。
  3. `return new UndoLogSegment(mtr, handle, firstPage, codec, allocator, pageAccess, pool, X)`（current=firstPage）。
- `open(mtr, PageId firstPageId, PageLatchMode mode) -> UndoLogSegment`：
  - `firstPage = pageAccess.openUndoPage(mtr, firstPageId, mode)`；校验 `isFirstPage()` 为真（否则 `UndoLogFormatException`：firstPageId 不是 log 首页）。
  - 重建 `handle`：`segmentId=firstPage.segmentId()`、`inodeSlot=firstPage.inodeSlot()`、`firstPageId`、`lastPageId=PageId(space, firstPage.lastPageNo())`。
  - **mode 语义（用户约束 2）**：`SHARED` 只读/遍历（仅持 first 页 S）；`EXCLUSIVE` 续 append——**额外打开 last 页 X**（`pageAccess.openUndoPage(mtr, lastPageId, X)`）作为 current，并**校验 last 页 `segmentId()`/`inodeSlot()` 与按 first header 重建的 handle 一致**，不符抛 `UndoLogFormatException`（防 first 页 `LAST_PAGE_NO` 损坏时把另一个 UNDO 页当尾页续写，向错误 segment 追加）；若 `lastPageId==firstPageId` 则复用 first 页 guard 不重复 fix（同页且 first 已是本 segment，免重复校验）。
  - `return new UndoLogSegment(...)`（mode=传入）。

### 5.2 `UndoLogSegment`（跨页 undo log 句柄）
- 内部状态（均 MTR 内有效）：`mtr`、`handle`、`firstPage`（持有，log header 入口）、`current`（持有，append 目标）、`mode`、`heldPages`（`Map<PageNo, UndoPage>`：本会话已 fix 的页缓存——初始含 first，X-open 再含 last，生长时追加新页；`resolvePage` 读到的非 held 页也缓存进来。first/current/生长页以 X 持有，按需 resolved 页以只读 S 持有）、`codec`、`allocator`、`pageAccess`、`pool`、`pageSize`。
- **`append(UndoRecord rec, IndexKeyDef keyDef, TableSchema schema) -> RollPointer`（要求 mode=X）**：
  1. `requireExclusive()`（mode 非 X 抛 `DatabaseValidationException`——只读会话调 append 属使用/状态误用，非物理损坏）。
  2. `payload = codec.encode(rec, keyDef, schema)`。
  3. 尝试 `int off = current.appendRecord(payload, rec.undoNo())`；
     - 捕获 `UndoPageOverflowException`（current 页放不下）：
       - **生长前 preflight（必须，防半改页）**：先判 `2 + payload.length <= pageSize - FIL_TRAILER - RECORD_AREA_START`（即一张全新空页能否容纳本条 record 槽）。**不满足则立即抛 `UndoPageOverflowException`，不 allocate、不 createChainPage、不改 FIL 链、不动 first 页 header**。原因：`MiniTransaction.rollbackUncommitted()` 不做 content undo，任何「先 allocate/链接/改 header 再抛异常」都会在 buffer 里留下半生长的脏链（新页已 PAGE_INIT、FIL next 已改、`LAST_PAGE_NO` 已推进），即便上层放弃也无法回退。preflight 把「能否生长成功」前置到任何页修改之前。
       - preflight 通过后才生长：`newId = allocator.allocatePage(mtr, handle.spaceId(), handle.inodeSlot(), handle.segmentId())` → `newPage = pageAccess.createChainPage(mtr, newId, handle)` → 链接 `current.linkNextTo(newId.pageNo())`、`newPage.linkPrevTo(current.pageId().pageNo())`（new.next 保持 FIL_NULL）→ `firstPage.setLastPageNo(newId.pageNo())`、`handle = handle.withLastPage(newId)` → `heldPages.put(newId.pageNo(), newPage)`、`current = newPage`。
       - `off = current.appendRecord(payload, rec.undoNo())`（新空页重试；preflight 已保证可容纳，此处不会再溢出）。
  4. **每次成功 append 都更新 first 页 log header（用户约束 1）**：`firstPage.setLogRecordCount(firstPage.logRecordCount() + 1)`；`firstPage.setLogLastUndoNo(rec.undoNo().value())`。
  5. `return new RollPointer(true, current.pageId().pageNo(), off)`。
- **`readRecord(RollPointer rp, IndexKeyDef keyDef, TableSchema schema) -> UndoRecord`**：
  1. 校验 `!rp.isNull()`（否则 `UndoLogFormatException`）。
  2. `UndoPage page = resolvePage(rp.pageNo())`（见 §5.3）。
  3. **段一致性校验（用户约束 3）**：`page.segmentId() == handle.segmentId() && page.inodeSlot() == handle.inodeSlot()`，不符抛 `UndoLogFormatException`（RollPointer 指向了别的 undo segment / 损坏）。`openUndoPage` 已校验 `pageType==UNDO`。
  4. `byte[] payload = page.recordAt(rp.offset())` → `codec.decode(payload, 0, keyDef, schema)`。
- **`forEachRecord(UndoRecordConsumer consumer, IndexKeyDef keyDef, TableSchema schema)`**：从 `firstPage` 起，沿 FIL `nextPageNo()` 走链；每页按 `[RECORD_AREA_START, freeOffset())` 用 `[len]` 前缀逐槽切片 `codec.decode` 还原，按 append(undoNo) 序回调；`nextPageNo()==FIL_NULL` 终止。页用 `resolvePage` 取（已持直读、未持 S-fix）。链中下一页 `segmentId/inodeSlot` 不符或非 UNDO → `UndoLogFormatException`。
- 访问器：`transactionId()`/`undoKind()`/`firstPageId()`/`lastPageId()`/`logRecordCount()`/`logLastUndoNo()`（读 first 页）。

### 5.3 `resolvePage(PageNo)`（latch reentrancy 边界，决策 5）
- 若 `heldPages` 含该 pageNo → 返回已缓存 `UndoPage`（**不再 `getPage`**；含 first/current/生长页与先前已 S-resolved 的缓存页）。
- 否则 `mtr.getPage(pool, PageId(space, pageNo), SHARED)` 取页（读路径**恒 `SHARED`**）→ 包 `UndoPage` → **放入 `heldPages` 缓存**，使同一 MTR 内对该非 held 页的重复 `readRecord`/`forEachRecord` 复用这次只读 fix（不重复 S-fix）→ 返回。
- 关键不变量：`resolvePage` 永不对「本会话已持 **X**」的页再发 `getPage`（已持页一律走第一分支直读），故不触发 S→X 自死锁，也不产生「已持 X 再取 S」的二次 fix / commit 期 `guardFor` 取到 S guard 的隐患；S-resolved 缓存页是只读的，绝不用于写（append 只写 `current`/`firstPage`，二者在建/开会话时即以 X 持有）。所有缓存 fix 随 MTR commit/rollback 统一释放。

## 6. 并发与边界

- **本片单 writer 假设（用户约束 4，写进类注释）**：同一 undo segment 同一时刻只有一个 writer；「持当前页 X latch 后再 `allocator.allocatePage`」在单 writer 下可接受。多 writer 并发 append、更严格的 undo 分配锁顺序（FSP 元页 page0/page2/inode 与 undo 数据页的全局序、`UndoSegmentMutex`/`RollbackSegmentSlotMutex`）留 T1.3c+。
- 生长在同一 MTR 内、持 current 页 X latch 时调 `allocator.allocatePage`（内部 `mtr.newPage`/FSP 元页 latch 全入 mtr memo），沿用 T1.3a「同 MTR allocatePage + createUndoPage」既有模式。无 `synchronized`、无 `wait/notify`、无后台 worker、无可阻塞等待。
- **不得在已持 undo page latch 后发起可能阻塞的外部等待**：buffer miss 读盘、`allocatePage` 文件扩展发生在取目标页 latch 的过程中，由 buffer pool fix / FSP autoextend 承担（沿用 record/btree/T1.3a 边界）。
- `UndoLogSegmentAccess`/`UndoSpaceAllocator`/`DiskSpaceUndoAllocator` 无跨调用可变状态；`UndoLogSegment` 仅在其 MTR 内有效（持 guard，**勿自行 close**；commit/rollback 释放）。

## 7. 恢复边界

- 不新增 redo 类型、不新增恢复编排：undo 页 `PAGE_INIT(UNDO)` + 信封/page header/log header/record/FIL 链字段的 `PAGE_BYTES` 由 D3/D4 物理 redo 幂等覆盖，commit 盖 pageLSN。
- 性质：MTR commit 后 evict/reload 整条 undo 链，page header / log header（含 `LAST_PAGE_NO`/`LOG_RECORD_COUNT`）与每页 record 逐字节完好；FIL prev/next 链可重走；`open(firstPageId)` 能重建 handle 并遍历全部 record。
- 重建 undo 的**逻辑语义**（rollback/history/purge resume）不在本片，留 T1.3d+。

## 8. 错误模型

- `DatabaseValidationException`：null 入参；`UndoNo` 负值；`appendRecord`/`UndoRecord` 的 `undoNo==NONE`；`UndoSegmentHandle` 形状非法（slot<0、segId<=0、page space 不一致）；`append` 时 mode 非 X（只读会话写属使用误用）。
- `UndoPageOverflowException`（T1.3a，复用）：单条 undo record 放不下**空页**（已生长后仍溢出）。
- `UndoLogFormatException`（T1.3a，复用）：解码越界/损坏；`recordAt` offset 出 area；`readRecord`/`forEachRecord` 目标页 `segmentId`/`inodeSlot` 与 handle 不符或非 UNDO；`open` 的 firstPageId 非 first 页（`isFirstPage()==false`）；非 first 页调用 log-header 访问器/更新器；链中 `nextPageNo` 悬空（指向非 UNDO/越界页）。
- `NoFreeSpaceException`（fsp，透传）：undo 表空间 autoextend 后仍无空间。
- 不新增其它异常；新增校验归 `storage.undo`，保留 message+cause。

## 9. 测试

- `UndoSegmentHandleTest`：字段校验（slot<0、segId<=0、page space 不一致拒绝）、`withLastPage` 返回新实例且只改 lastPageId、相等性。
- `UndoPageLayoutTest`/`UndoPageTest`（扩 T1.3a）：拆分后 header 偏移钉死（page header [38,63)、log header [63,97)、`RECORD_AREA_START=97`）；`formatFirstPage` 后 page+log header 初值正确、`isFirstPage()==true`；`formatChainPage` 后 `isFirstPage()==false` 且 **log header 区清零**；非 first 页调用 log-header 访问器抛 `UndoLogFormatException`；`appendRecord` 推进 page header（不动 log header）；链接器只改 FIL next/prev 不动其它头字段。
- `DiskSpaceUndoAllocatorTest`（`storage.api`，onPool harness）：`createUndoSegment` 返回 handle（firstPageId 是真实分配页、inodeSlot/segmentId 合法、first==last）；`allocatePage` 在同 segment 再分配不同页号；handle→SegmentRef 往返不暴露 SegmentRef 给上层。
- `UndoLogSegmentTest`（onPool harness，复用 T1.3a `FileChannelPageStore`+`LruBufferPool`+`DiskSpaceManager`+ `DiskSpaceUndoAllocator` 注入）：
  - **单页路径**：`create` → `append` 数条（均落 first 页）→ `RollPointer` 非 NULL/指 first 页 → `readRecord(rp)` 等值；first 页 `logRecordCount`/`logLastUndoNo` 每条递增。
  - **跨页生长（核心）**：append 定长 record 直至 first 页满 → 下一条触发 `allocator.allocatePage` + `createChainPage` → 该条落新页（`rp.pageNo==`新页）→ `readRecord(rp)` 等值；断言：新页 `prevPageNo==`first、first 页 `nextPageNo==`新页、first 页 log header `LAST_PAGE_NO==`新页、`LOG_RECORD_COUNT==`总数、各页 `RECORD_COUNT` 之和==总数。
  - **整链正向遍历**：`forEachRecord` 跨多页按 undoNo 序回收全部 record，等值原序列。
  - **跨页 prev 链**：`prevRollPointer` 串前后两条（分别在不同页）后能从后者读到前者并 `readRecord` 回前者。
  - **段一致性守门**：构造指向另一 undo segment 页的 `RollPointer` → `readRecord` 抛 `UndoLogFormatException`（segmentId/inodeSlot 不符）。
  - **单条超页 + 无副作用（preflight）**：`append` 一条 payload > 空页容量 → `UndoPageOverflowException`；**且失败后 first 页 `LAST_PAGE_NO`/`LOG_RECORD_COUNT`/`LOG_LAST_UNDO_NO`、current 页 `nextPageNo`（FIL next）、`RECORD_COUNT` 全部不变**（验证 preflight 在任何页修改前抛出，未留半生长脏链）。
  - **mode 守门**：`open(SHARED)` 后 `append` 抛（只读会话不可写）；`open` 一个非 first 页作为 log 首页抛 `UndoLogFormatException`。
- `UndoLogSegmentReopenTest`（持久化）：建链 append 多条跨页 + commit → **新 `PageStore`/`BufferPool`** `store.open` + `open(firstPageId, SHARED)` → `logRecordCount`/`lastPageId` 与写时一致、`forEachRecord` 全部 record 完好、各页 FIL 链与 segment 归属字段完好（覆盖 pageLSN/落盘路径与 reopen 重建 handle）。
- 双 `newPage` 顺序：`create` 内同 MTR `allocatePage`(ALLOCATED) 后 `createFirstPage`(UNDO)，commit + reload 后首页类型 UNDO、header 为 format 初值（钉死 redo 顺序最终态）。
- 回归：全量 Gradle `test` 通过，测试数只增不减；**T1.3a `UndoPageTest`/`UndoLogStoreTest` 因 header 偏移拆分需同步更新断言（属本片改动，非倒退）**；`PageTypeTest`、record R1-R5、B1/B2/B3、redo/recovery、T1.1/T1.2 聚簇路径不受影响（非聚簇字节、`DB_ROLL_PTR` 仍 NULL）。

## 10. 简化点与后续

- **不接事务**：`DB_ROLL_PTR` 仍恒 NULL；`UndoContext`/rollback segment slot/`UndoLogManager`/`insertClustered` 写真指针留 T1.3c。
- **无 rollback segment 容器**：undo log segment 直接经 `UndoSpaceAllocator` 建（无 rseg header/slot/history list/cached 复用）；slot 目录、按事务哈希/轮询选 rseg 留 T1.3c。
- **单 writer / 单 undo space**：并发 append、多 writer 锁顺序、多 rseg-id/多 undo 表空间 RollPointer 编码留后续；`RollPointer` 不改格式。
- **统一 `RECORD_AREA_START`**：非 first 页预留并清零 log header 区（浪费约 34B/页）换 offset 数学统一。
- **单 record 不跨页**：超空页抛溢出；external/跨页大 payload 留 T1.3e+。
- **无 undo 回收**：free page / undo segment 回收 / truncation / cached segment 复用全留 purge 片。
- **`UndoLogKind`/`state` 仍占位**：恒 `INSERT`/`ACTIVE`；状态机留 T1.3c/d。
- **`UndoLog`(T1.3a 单页 facade) 保留**：其 `append(UndoPage,…)`/`readRecord(UndoPage,…)` API 不变（不依赖 header 偏移，只用 `appendRecord`/`recordAt`/`pageId`），多页逻辑在 `UndoLogSegment`。注意：`UndoLogStoreTest` 的页创建 harness 因 `UndoPageAccess.createUndoPage` 拆为 `createFirstPage`/`createChainPage`（且 first 页需 `UndoSegmentHandle`）而同步更新（§9）；这是本片头拆分的必然改动，非 `UndoLog` 逻辑倒退。二者若重叠过多，T1.3c 再评估收敛。

## 11. 自检

1. 范围严格限定 undo 物理多页链（segment 生长 + 头拆分 + 遍历 + 端口隔离分配 + redo）；事务/rollback segment/`UndoContext`/`insertClustered`/真 `DB_ROLL_PTR`/UPDATE/DELETE/history/purge/recovery/回收全列非目标。
2. 横切续作，承重物理件隔离测试；写路径与真 rollback 在 T1.3c/d。
3. 端口+适配器：`UndoSpaceAllocator`/`UndoSegmentHandle` 在 `storage.undo`（签名不暴露 `storage.api` 类型，用 `MiniTransaction`/`domain`/handle），`DiskSpaceUndoAllocator` 在 `storage.api`；`storage.api → storage.undo` 允许、反向禁止；handle 不暴露 `SegmentRef`；`NoFreeSpaceException` 适配器透传。
4. 头拆分偏移具体可落地（page header [38,63)、log header [63,97)、`RECORD_AREA_START=97`），非 first 页 log header 清零且按 `isFirstPage` 决定是否解析。
5. 生长流程明确且**先 preflight 后改页**：溢出 handler 先判「全新空页能否容纳本条」，不满足直接抛 `UndoPageOverflowException`（不 allocate/不 createChainPage/不改链/不动 header），满足才 allocate→createChainPage→FIL 双向链+保留对侧指针→first 页 `LAST_PAGE_NO` 推进→新页重试。杜绝「MTR 无 content undo 下半生长脏链」；测试钉死失败后 `LAST_PAGE_NO`/`LOG_RECORD_COUNT`/FIL next 不变。
6. **每次成功 append 更新 first 页 `LOG_RECORD_COUNT`/`LOG_LAST_UNDO_NO`**（非仅溢出时）。
7. **`open(SHARED)` 只读遍历；`open(EXCLUSIVE)` 续 append 打开 first+last 页**；append 在非 X 会话拒绝。
8. **`readRecord` 除页类型外校验目标页 `SEGMENT_ID`/`INODE_SLOT` 与 handle 一致**。
9. latch reentrancy：`resolvePage` 已持页（含先前 S-resolved 缓存页）直读、未持页 S-fix 后**缓存进 heldPages**（同 MTR 重复读复用 fix），永不对已持 X 页再 `getPage`，规避「已持 X 再 getPage(S)」二次 fix 与 pageLSN 隐患（`getPage` fresh fix、禁 S→X 升级已核）。
10. **单 writer 简化写明**：持 current X latch 后 allocate 可接受；多 writer/严格锁顺序留后续；无 `synchronized`/可阻塞等待。
11. undo 写经 MTR-owned guard，复用 D3/D4 物理 redo，不新增 redo 类型/恢复编排；reopen 重建 handle + 遍历由持久化测试钉死。
12. 错误模型区分 validation/overflow/format（含段不符、非 first 页、链悬空），`NoFreeSpaceException` 透传；新增异常归 `storage.undo`，保留 message+cause。
13. 测试覆盖 handle 校验、头拆分偏移、适配器、单页/跨页生长/遍历/prev 链/段守门/超页/mode 守门、reopen 持久化、双 newPage redo 顺序、全量回归（含 T1.3a 测试同步更新说明）。
14. **`DB_ROLL_PTR` 仍 NULL** 在范围/决策/测试/自检多处重复钉死；简化点逐条标注后续片归属，无 TODO 占位。
