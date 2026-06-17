# Spec：fsp 元数据仓储层（SpaceHeader + XDES + SegmentInode）

- 日期：2026-06-11
- 关联设计：`C:\coding\java\self\dbtest\dbtest\docs\design\innodb-disk-manager-design.md`（§5.4、§5.5、§6.1–6.4、§14、§18）
- 上游约束：本仓库 `AGENTS.md` / `CLAUDE.md`
- 依赖切片：buf（`BufferPool`/`PageGuard`）、mtr（`MiniTransaction`）、fil 物理 IO 均已完成
- 状态：brainstorming 评审通过（含「fsp 是逻辑层；本切片 XDES/SpaceHeader 落物理空间账本、INODE 落逻辑 segment 归属，分仓储不混」的边界确认），待自查 + 用户复核

## 1. 背景与目标

落地 §6.2–6.4 三类首区管理页的**布局 + 单页 CRUD 仓储**：page 0 `SpaceHeaderPage`（含紧随其后的 XDES entries）、page 2 `SegmentInodePage`。三个 Repository 经 `MiniTransaction.getPage/newPage` 拿 `PageGuard`，按偏移读写。**物理/逻辑分仓储**：SpaceHeader/XDES 管物理空间账本（哪些物理 extent/page 空闲、占用、归属哪个 segment 的物理位图），SegmentInode 管逻辑 segment 归属（fragment slots + extent list 头）。

## 2. 范围与非目标

**做**：三类页布局常量 + 值对象 + Repository 单页读写（含 XDES bitmap 位操作、XDES list-node 指针读写、inode slot 分配/释放与字段读写、SpaceHeader 字段更新）；给 buf `PageGuard` 补 `readLong/writeLong`。

**不做**（slice 2 / 后续，注释标简化点）：
- free-list **链表算法**（FLST insert/remove/iterate，跨页走链）、`ExtentAllocationPolicy`、`SegmentPageAllocator`、`SpaceReservation`、`NoFreeSpaceException`——下一切片。
- 超过 page 0 内嵌首批的 XDES 管理页（首版只覆盖 page 0 按 `maxEntriesInPage0(pageSize)` 公式可容纳的首批 extent；实际容量由页大小和 `XDES_ENTRY_SIZE` 决定，不承诺固定 256MB）。
- `IBUF_BITMAP`(page1) / `SDI`(page3) 页内容；inode page list（首版只用单个 page 2）。
- FilePageHeader/Trailer 的 checksum、pageLSN、加密/压缩字段解释——页首预留 `FIL_PAGE_DATA` 字节（本切片填零、不解析）。
- crash-safe redo/WAL（本切片是 no-redo 原型：写页经 MTR 标脏，但不生成 redo record，不声明崩溃可恢复；后续 redo 切片必须接入 `UPDATE_SPACE_HEADER`、`UPDATE_XDES`、`UPDATE_SEGMENT_INODE` 后才能用于 crash-safe 路径）。**注意：disk-manager §15 把「所有 segment/extent/page 修改必须产生 redo」列为强制规则，本切片在普通（非 temporary）表空间上写页不产 redo，属于对该规则的"推迟满足"而非满足；在 redo 切片接入前，相关类注释与测试名一律不得出现 crash-safe／崩溃可恢复字样。**

## 3. 包与依赖方向

- 新增类位于 `cn.zhangyis.db.storage.fsp`；另对 buf 的 `PageGuard` 增补两个方法。
- 依赖：`fsp → buf`（BufferPool/PageGuard/PageLatchMode）、`fsp → mtr`（MiniTransaction）、`fsp → domain`（SpaceId/PageId/PageNo/PageSize/ExtentId/SegmentId）、`fsp → common.exception`。**禁止** import `fil` 内部（经 buf）、`btree`/`record`/`trx`/`sql`/`session`/`dd`（§15）。依赖图 `fsp→buf→fil`、`fsp→mtr→buf` 无环。

## 4. buf 增补（前置 Task）

给 `cn.zhangyis.db.storage.buf.PageGuard` 增：
- `long readLong(int offset)`：`ensureOpen` + `checkBounds(offset, Long.BYTES)` + `frame.buffer.getLong(offset)`（绝对，BIG_ENDIAN）。S/X 均可。
- `void writeLong(int offset, long value)`：`requireExclusive` + `checkBounds(offset, Long.BYTES)` + `frame.buffer.putLong(offset, value)` + `wrote=true`。
- 补 `PageGuardTest` 用例：X 下 writeLong/readLong 往返；S 下 writeLong → DatabaseValidationException；越界 → DatabaseValidationException。

## 5. 共享类型

### 5.1 `FileAddress`（fsp，public final）
表达 free-list 头与 XDES list-node 的 prev/next（InnoDB `fil_addr_t` 最小版）：页内地址 = `PageNo pageNo + int offset`，或 NULL 哨兵。**只存取、不走链**。
- `static final FileAddress NULL`（哨兵）。
- `static FileAddress of(PageNo pageNo, int offset)`（pageNo 非空、offset≥0）。
- `boolean isNull()`、`PageNo pageNo()`（NULL 调用抛 Mtr/Validation 异常）、`int offset()`。
- 编码 12 字节：`pageNo`(long 8) + `offset`(int 4)。**NULL = 全零编码**（pageNoRaw==0 且 offset==0）：使刚创建（全零）的 page 0 XDES 区与 page 2 inode 区的 list 指针天然解码为 NULL，无需显式初始化。真实 list 节点偏移恒 ≥ `FIL_PAGE_DATA`(38)，绝不落在 (page0,offset0)，故无碰撞；`of(pageNo,offset)` 校验拒绝 (pageNo==0 且 offset==0)，要表达空请用 `FileAddress.NULL`。
- `void writeTo(PageGuard guard, int at)` / `static FileAddress readFrom(PageGuard guard, int at)`：用 guard write/readLong+Int 编解码（NULL 检测 `pageNoRaw==0 && offset==0`）。
- equals/hashCode。

### 5.2 `ExtentState`（枚举）
`FREE`、`FREE_FRAG`、`FULL_FRAG`、`FSEG`、`FSEG_FRAG`（§5.4）。XDES 存其 ordinal（int）。**`FREE` 必须是 ordinal 0**，使零初始化的 XDES 解码为 FREE（测试钉死 ordinal）。

### 5.3 零初始化即有效默认（贯穿设计）
所有哨兵取 0，使「刚 `PageStore.create` 出来的全零页」可被安全解码：普通 XDES entry 的零态解码为「FREE / 无主(owner=0) / prev=next=NULL / bitmap 全空」；inode entry 的零态解码为「used=0 空闲 / fragment 槽全空(=0)」。因此无需对每个普通 extent/inode 显式 init 才能读。前提约定：**segment id 从 1 起**（0 唯一表「无主」），`SpaceHeader.nextSegmentId` 也必须从 1 起，**page 0 恒为 FSP_HDR、绝不作 fragment 页**（0 唯一表「空 fragment 槽」）。实现必须在 `writeOwner`、`allocateSlot`、`allocateNextSegmentId` 等入口拒绝或报告 0，避免把哨兵当作真实 segment。

零态 XDES 只表示「该 descriptor 可以被安全读取」，不等价于「该 extent 一定在当前物理文件内且可分配」。后续 allocator 必须同时检查 `SpaceHeader.currentSizeInPages/freeLimitPageNo` 和文件大小边界，不能把 page 0 中所有零态 XDES 都直接当作可分配空间。

### 5.4 系统 extent 保留规则
`extentNo==0` 是首区系统保留 extent，包含 page 0 `FSP_HDR/XDES`、page 1 `IBUF_BITMAP`、page 2 `INODE`、page 3 `SDI` 等固定管理页，**不得进入普通 `FSP_FREE` 分配路径，也不得被 `initFree` 重置为普通空闲 extent**。`ExtentDescriptorRepository` 提供 `reserveSystemExtent(mtr, spaceId)` 初始化/修复该 entry：state 写为 `FSEG_FRAG`（表示非普通 free-list extent）、owner 写 0（系统保留不暴露为普通 SegmentId）、prev/next 写 NULL，bitmap 至少把 page 0..3 标记为已分配。除 `reserveSystemExtent` 外，普通 `writeState`/`writeOwner`/`writePrev`/`writeNext`/`setPageAllocated` 均不得修改 extent 0，防止测试或后续 allocator 绕过系统 extent 保护。后续 `SegmentPageAllocator` 必须无条件跳过 `extentNo==0`；测试钉死该约束。

**与 InnoDB / disk-manager §5.4 的差异（简化点）**：真实 InnoDB 首 extent 挂在 `FSP_FREE_FRAG` 链中，page 4+ 仍可作为 fragment page 分出去；本切片把整段 extent 0 标为保留并让 allocator 无条件跳过，因此 **page 4..(pagesPerExtent-1) 被浪费**，且浪费量随页变小而显著放大（16KB 页浪费 60 页，4KB 页浪费 252 页）。此外，复用 `FSEG_FRAG`（§5.4 语义为「属于某 segment 且按 fragment 管理」）+ owner=0 来表达「系统保留、无主」属于语义重载；后续若要回收 extent 0 内的 fragment 页，或引入独立的「系统保留」状态枚举，需同步修订 `reserveSystemExtent`、allocator 跳过规则，并补迁移测试。

### 5.5 `SegmentPurpose`（枚举）
`INDEX_LEAF`、`INDEX_NON_LEAF`、`LOB`、`UNDO`、`SYSTEM`（§5.5）。INODE 存其 ordinal（int）。

### 5.6 页首预留
常量 `PageLayouts.FIL_PAGE_DATA = 38`：每页首预留 38 字节给未来 FilePageHeader（本切片填零、不读写），所有元数据字段从该偏移之后开始。

## 6. SpaceHeaderPage（page 0）

### 6.1 `SpaceHeaderLayout`（偏移常量，相对页首；均在 FIL_PAGE_DATA 之后）
| 字段 | 偏移 | 类型/字节 |
| --- | --- | --- |
| spaceId | 38 | int 4 |
| pageSizeBytes | 42 | int 4 |
| spaceFlags | 46 | int 4 |
| currentSizeInPages | 50 | long 8 |
| freeLimitPageNo | 58 | long 8 |
| nextSegmentId | 66 | long 8 |
| freeExtentListHead | 74 | FileAddress 12 |
| freeFragExtentListHead | 86 | FileAddress 12 |
| fullFragExtentListHead | 98 | FileAddress 12 |
| firstInodePageNo | 110 | long 8 |
| sdiRootPageNo | 118 | long 8 |
| serverVersion | 126 | int 4 |
| spaceVersion | 130 | long 8 |

- `XDES_BASE = 200`（header 之后预留到 200，XDES entries 从此起）。
- 简化点：省略 `firstXdesEntryOffset`（= 常量 `XDES_BASE`，首版不支持独立 XDES 管理页，故不落盘）和 `encryptionMetadataOffset`（加密未实现）。这是与 disk-manager 设计字段列表的显式差异；后续支持多 XDES 管理页或加密元数据时必须扩展 SpaceHeader 版本并补迁移测试。

### 6.2 `SpaceHeaderSnapshot`（record）
携带上表全部字段（spaceId:SpaceId、pageSize:PageSize、spaceFlags:int、currentSizeInPages/freeLimitPageNo:PageNo、nextSegmentId:long、三个 list 头:FileAddress、firstInodePageNo:PageNo、sdiRootPageNo:long、serverVersion:int、spaceVersion:long）。构造时要求 `nextSegmentId > 0`。

### 6.3 `SpaceHeaderRepository`
构造 `(BufferPool pool)`。所有方法签名带 `MiniTransaction mtr`，内部 `mtr.getPage(pool, page0Id, X/S)`。`page0Id = PageId.of(spaceId, PageNo.of(0))`。
- `void initialize(MiniTransaction mtr, SpaceHeaderSnapshot init)`：X 取 page0，按布局写全部 header 字段；普通 XDES entry 可保持零态，但表空间初始化流程必须在同一 MTR 内调用 `ExtentDescriptorRepository.reserveSystemExtent(mtr, spaceId)`，确保 extent 0 不会被当作普通 FREE。新建表空间的 init 快照应取 `nextSegmentId=1`（0 留作 XDES 无主哨兵）、`firstInodePageNo=2`。
- `SpaceHeaderSnapshot read(MiniTransaction mtr, SpaceId spaceId)`：S 取 page0，组装 snapshot。
- `void setCurrentSizeInPages(mtr, spaceId, PageNo)` / `setFreeLimitPageNo` / `setFreeExtentListHead(FileAddress)` / `setFreeFragExtentListHead` / `setFullFragExtentListHead` / `setFirstInodePageNo`。
- `long allocateNextSegmentId(mtr, spaceId)`：X 取 page0，读 nextSegmentId；若值 `<=0`，说明页上元数据破坏了 0 哨兵不变量，抛 `FspMetadataException`；否则写回 +1、返回旧值（segment id 分配；幂等性见 §9 说明——非幂等，调用方在 MTR 内一次）。

## 7. XDES（ExtentDescriptor）

### 7.1 `ExtentDescriptorLayout`
XDES entries 内嵌 page 0，从 `XDES_BASE=200` 起，每条 `XDES_ENTRY_SIZE = 68`：
| 字段 | 条内偏移 | 类型 |
| --- | --- | --- |
| state | 0 | int 4（ExtentState ordinal） |
| ownerSegmentRaw | 4 | long 8（0 = 无主；真实 SegmentId 必须从 1 起） |
| prev | 12 | FileAddress 12 |
| next | 24 | FileAddress 12 |
| pageBitmap | 36 | 32 字节（256 位，1 位/页：1=已分配）|
- `maxEntriesInPage0(PageSize) = (pageSize.bytes() - XDES_BASE) / XDES_ENTRY_SIZE`。
- `slotOffset(extentNo) = XDES_BASE + extentNo * XDES_ENTRY_SIZE`。
- 简化点：1 位/页（不分 used/clean 两位）；bitmap 固定 32 字节（覆盖至 256 页/extent，仅前 pagesPerExtent 位有效）。

### 7.2 `ExtentDescriptor`（record）
`(ExtentId extentId, ExtentState state, long ownerSegmentRaw, FileAddress prev, FileAddress next)`（bitmap 不进 record，单独按位访问，避免大数组拷贝）。

### 7.3 `ExtentDescriptorRepository`
构造 `(BufferPool pool, PageSize pageSize)`。XDES entries 在 page 0；`extentNo >= maxEntriesInPage0` → `FspMetadataException`（首版不支持超首批）。
- `ExtentDescriptor read(mtr, ExtentId)`：S 取 page0，按 slotOffset 读 state/owner/prev/next；state ordinal 越界表示页上元数据损坏，抛 `FspMetadataException`，不能泄漏 JDK `ArrayIndexOutOfBoundsException`。
- `void writeState(mtr, ExtentId, ExtentState)` / `writeOwner(mtr, ExtentId, Optional<SegmentId>)`（空 = ownerSegmentRaw 0；非空 `SegmentId.value()` 必须 > 0）/ `writePrev(mtr, ExtentId, FileAddress)` / `writeNext(...)`。这些普通 mutator 均拒绝 `extentNo==0`；系统 extent 只能通过 `reserveSystemExtent` 改写。`ExtentDescriptor` 提供 `Optional<SegmentId> ownerSegment()`（raw==0 → 空）。
- bitmap：`boolean isPageAllocated(mtr, ExtentId, int pageIndexInExtent)`（S）、`void setPageAllocated(mtr, ExtentId, int idx, boolean)`（X）。普通 `setPageAllocated` 拒绝 `extentNo==0`；`idx` 越界（≥pagesPerExtent）→ DatabaseValidationException。
- `void initFree(mtr, ExtentId)`：仅允许普通 extent（`extentNo > 0`），写 state=FREE、owner=none、prev/next=NULL、bitmap 清零（新普通 extent 初始化）；`extentNo==0` 抛 `FspMetadataException`，必须使用 `reserveSystemExtent`。
- `void reserveSystemExtent(mtr, SpaceId)`：X 取 page0，初始化/修复 extent 0 的系统保留状态，至少标记 page 0..3 已分配，并保证该 extent 不在普通 free-list 中。

## 8. SegmentInode（page 2）

### 8.1 `SegmentInodeLayout`
INODE entries 从 `INODE_BASE = FIL_PAGE_DATA(38)` 起，每条 `INODE_ENTRY_SIZE = 324`：
| 字段 | 条内偏移 | 类型 |
| --- | --- | --- |
| used | 0 | int 4（0=空闲,1=在用） |
| segmentId | 4 | long 8 |
| purpose | 12 | int 4（SegmentPurpose ordinal） |
| usedPageCount | 16 | long 8 |
| reservedPageCount | 24 | long 8 |
| freeExtentListHead | 32 | FileAddress 12 |
| notFullExtentListHead | 44 | FileAddress 12 |
| fullExtentListHead | 56 | FileAddress 12 |
| fragmentPageSlots | 68 | 32 槽 × long 8 = 256 字节（0=空；page 0 为 FSP_HDR，绝不作为 fragment page） |
- `FRAGMENT_SLOT_COUNT = 32`。`maxInodesInPage(PageSize) = (pageSize.bytes() - INODE_BASE) / INODE_ENTRY_SIZE`。`slotOffset(inodeSlot) = INODE_BASE + inodeSlot * INODE_ENTRY_SIZE`。

### 8.2 `SegmentInode`（record）
`(int inodeSlot, SegmentId segmentId, SegmentPurpose purpose, long usedPageCount, long reservedPageCount, FileAddress freeExtentListHead, FileAddress notFullExtentListHead, FileAddress fullExtentListHead)`（fragment slots 单独按槽访问）。

### 8.3 `SegmentInodeRepository`
构造 `(BufferPool pool, PageSize pageSize)`。inode 在 page 2（`PageId.of(spaceId, PageNo.of(2))`）。
- `int allocateSlot(mtr, SpaceId, SegmentId, SegmentPurpose)`：X 取 page2，先校验 `segmentId.value() > 0`，再扫 used==0 的首槽 → 写 used=1/segmentId/purpose、counts=0、三 list 头=NULL、32 fragment 槽=0，返回 inodeSlot；无空槽 → `FspMetadataException`。
- `SegmentInode read(mtr, SpaceId, int inodeSlot)`：S 取 page2 读字段；used==0 → `FspMetadataException`（空槽不可读）；purpose ordinal 越界表示 inode 元数据损坏，抛 `FspMetadataException`。
- `void freeSlot(mtr, SpaceId, int inodeSlot)`：X 取 page2，把整条 inode entry 重置为零态（used=0、segmentId/purpose/counts=0、三 list 头=NULL、32 fragment 槽=0）。`used=0` 是空槽的权威判断，清零其余字段是为了避免残留调试信息被误读。
- 字段更新：`setUsedPageCount` / `setReservedPageCount` / `setFreeExtentListHead` / `setNotFullExtentListHead` / `setFullExtentListHead`。
- fragment 槽（**空哨兵 = 0**，因 page 0 恒为 FSP_HDR、绝不作 fragment 页，零初始化即全空）：`Optional<PageNo> getFragmentPage(mtr, SpaceId, inodeSlot, int fragIdx)`（槽值 0 → 空）、`void setFragmentPage(mtr, SpaceId, inodeSlot, fragIdx, Optional<PageNo>)`（空写 0；非空 PageNo 必须 > 0）、`int requireFreeFragmentSlot(mtr, SpaceId, inodeSlot)`（返回首个值为 0 的槽下标，满则抛 `FspMetadataException`）。`fragIdx` 越界（≥32）→ DatabaseValidationException。

## 9. 数据流、并发与恢复

- 读：`mtr.getPage(pool, pageId, SHARED)` → 按 Layout 偏移 read* → 组 record/值。写：`mtr.getPage(pool, pageId, EXCLUSIVE)` → write* → MTR commit 时 page latch 释放、页标脏。
- 并发：全程在 MTR 内经 page latch（S 读 / X 写）；同页多字段写在一个 X guard 内完成。写元数据页须持 X（§6.2「写入须在 MTR 中持有 page X latch」）。跨页修改必须遵守 disk-manager §18 锁顺序：space header/page0（含首批 XDES）→ 后续 XDES page → inode page → data page；首版 SpaceHeader 与首批 XDES 同在 page0，因此同时更新 page0 与 page2 时先取 page0 X，再取 page2 X。
- 同一 MTR 内禁止对同一页先取 SHARED 再取 EXCLUSIVE；当前 `MiniTransaction`/`ReentrantReadWriteLock` 不支持 S→X 升级，调用方若后续要写，必须一开始走写方法直接取 X latch，或用 savepoint 释放 S guard 后重新定位并取 X。
- 恢复：字段级 setter 设计为**幂等**（同输入重复写得同结果，无读改写依赖），为后续 recovery 重放预留（§6.3）。例外：`allocateNextSegmentId`、`allocateSlot`、`requireFreeFragmentSlot` 是「读改写/查找」型，非幂等，属分配语义；本切片提供它们但调用方须在单个 MTR 内一次性使用，redo/recovery 接入后再纳入幂等重放框架（注释标注）。
- redo 对应关系：`setCurrentSizeInPages`/`setFreeLimitPageNo`/list 头 setter/`allocateNextSegmentId` 对应未来 `UPDATE_SPACE_HEADER`；XDES state/owner/list/bitmap/`initFree`/`reserveSystemExtent` 对应未来 `UPDATE_XDES`；inode slot、计数、list head、fragment slot 更新对应未来 `UPDATE_SEGMENT_INODE`。在这些 redo record 和 pageLSN 接入前，本切片只能作为 no-redo 原型，不能用于 crash-safe 验证。

## 10. 异常

- `FspMetadataException extends DatabaseRuntimeException`：extent 超首批 XDES 区、对 extent 0 执行普通 mutator、读空 inode 槽、无空 inode 槽、无空 fragment 槽、页上枚举 ordinal 越界、`nextSegmentId<=0` 等元数据约束或页内容损坏错误。
- `DatabaseValidationException`：null 参数、slot/extent/frag 下标越界、非法值。
- buf/mtr 抛出的异常透传。

## 11. 测试（buf `LruBufferPool` + fil `FileChannelPageStore` + mtr `MiniTransactionManager` + @TempDir 真实驱动）

- `PageGuardTest`（buf）：readLong/writeLong 往返、S 写拒绝、越界。
- `FileAddressTest`：of/NULL/isNull、writeTo/readFrom 往返（含 NULL）、越界。
- `ExtentStateTest` / `SegmentPurposeTest`：ordinal 稳定（值顺序固定，避免改序破坏已写盘数据——用断言钉死 ordinal）。
- `SpaceHeaderRepositoryTest`：initialize→read 往返；各 setter→read 一致；allocateNextSegmentId 连续自增；`nextSegmentId=0` 拒绝；no-redo 原型注释/测试名称明确“不做 crash recovery 断言”。
- `ExtentDescriptorRepositoryTest`：initFree→read（FREE/无主/NULL 链）；zero-init 普通 extent 读取；reserveSystemExtent 标记 page0..3 allocated 且 extent0 不可 `initFree`，普通 mutator 也拒绝 extent0；writeState/owner/prev/next→read；writeOwner 拒绝 SegmentId 0；bitmap set/clear/get；超首批 extentNo→FspMetadataException；page idx 越界→校验异常；坏 state ordinal→FspMetadataException；4K/8K/16K 下 `pagesPerExtent` 边界。
- `SegmentInodeRepositoryTest`：allocateSlot→read 往返；allocateSlot 拒绝 SegmentId 0；freeSlot 后整 entry 清零且槽可复用；读空槽→FspMetadataException；各 setter→read；fragment 槽 set/get/requireFree/复用；fragment PageNo 0 拒绝；满槽→FspMetadataException；坏 purpose ordinal→FspMetadataException；下标越界→校验异常。
- `MtrLatchOrderTest`（或现有 MTR/Repository 测试中覆盖）：同一 MTR 内 S→X 升级风险应通过 MTR 重复 fix 检测或 repository 写路径直接取 X 的约束测试固定；不要用会残留阻塞线程的裸超时测试。跨 page0/page2 的组合写按 page0→page2 顺序执行。
- 测试通过 `MiniTransactionManager.begin()` 拿 mtr，操作后 `commit`；页须先经 fil `PageStore.create` 建好（≥4 页：0/1/2/3），因为 extent 0 保留规则显式覆盖 SDI page 3。

## 12. 简化点（注释标注）

页首 FIL header 预留填零不解析；XDES 只覆盖 page 0 按公式可容纳的首批 extent；1 位/页 bitmap；单 inode 页 page 2；省略 SDI/IBUF/加密字段；list 头/节点只存取不走链；extent 0 系统保留且 allocator 必须跳过；非幂等的分配型方法待 redo/recovery 纳入重放框架；本切片 no-redo、无 pageLSN，不具备 crash-safe 语义。

## 13. 自查修正记录

1. **零初始化解码 bug（重要）**：原哨兵取 -1（NULL/owner/fragment），使刚 create 的全零 XDES/inode 区解码错误（owner=segment0、prev/next=(page0,off0)、fragment=page0），逼每个 extent/inode 必须先显式 init 才能读。改为**全部哨兵取 0**（§5.3）：`FileAddress.NULL`=全零、`ExtentState.FREE`=ordinal 0、XDES 无主=0（segment id 从 1 起）、fragment 空=0（page 0 永不作 fragment）。普通零态可安全读取；extent 0 另由 §5.4 系统保留规则排除普通分配。
2. **FileAddress 碰撞防护**：`of(pageNo,offset)` 拒绝 (0,0)；真实节点偏移恒 ≥ FIL_PAGE_DATA，绝不与全零 NULL 碰撞。
3. **owner/fragment 用 Optional 暴露**：`ownerSegment()→Optional<SegmentId>`、`getFragmentPage()→Optional<PageNo>`，避免裸哨兵泄漏到调用方。
4. **幂等 vs 分配型方法区分**（§9）：字段 setter 幂等可重放；`allocateNextSegmentId`/`allocateSlot`/`requireFreeFragmentSlot` 为读改写/查找，非幂等，注释标注待 redo 切片纳入重放，并明确未来 redo record 类型映射。
5. **page0 单 latch 保护 SpaceHeader+XDES**：二者同在 page 0，一次 X latch 覆盖；slice 2 分配在一个 MTR 内同时更新 page0 与 page2 时按 page0→page2 顺序取 latch，禁止 S→X 升级。
6. **二次边界收紧**：`nextSegmentId` 也必须从 1 起，页上读到 0 视为元数据损坏；extent 0 除 `reserveSystemExtent` 外拒绝普通 XDES mutator；落盘 enum ordinal 越界必须抛 `FspMetadataException`，不能泄漏 JDK 数组越界异常。

## 14. 后续切片衔接

slice 2 用本层仓储实现：FLST 链表算法（用 SpaceHeader/INODE 的 list 头 + XDES prev/next 走链）、`ExtentAllocationPolicy`、`SegmentPageAllocator`（fragment<32 / extent / 最多 4 extent / autoextend 重试，且无条件跳过 extent 0）、`SpaceReservation`、`NoFreeSpaceException`。redo 切片把非幂等分配纳入 WAL 重放，并写页首 FilePageHeader（checksum/pageLSN）。
