# Spec：fsp extent 管理与页分配原语（slice 2b）

- 日期：2026-06-11
- 关联设计：`C:\coding\java\self\dbtest\dbtest\docs\design\innodb-disk-manager-design.md`（§5.4 extent state/list、§6.3 XDES、§7 分配、§8 freeLimit/扩展、§18 锁序）
- 上游依赖：slice 1（SpaceHeader/XDES/SegmentInode 仓储）、slice 2a（`Flst` 链表层 + 各 repo 的 base 地址访问器）均已完成
- 状态：brainstorming 评审通过（2b 边界 = extent 机制 + 页分配原语；2c 负责 SegmentPageAllocator 编排/policy/reservation/autoextend/facade），待 spec 自查

## 1. 背景与目标

slice 2a 提供了通用 `Flst` 链表与 base 地址访问器，但还没有“把 extent 在各链之间迁移、从 extent 取/还页”的逻辑。slice 2b 落地 **extent 级机制 + 页分配原语**：freeLimit 填充、FSP_FREE 取/还、全局链（FREE↔FREE_FRAG↔FULL_FRAG）与 segment 链（SEG_FREE↔NOT_FULL↔FULL）迁移、从 FREE_FRAG 分配 fragment 页、从 segment extent 分配页、释放页回收。全程维护 XDES state/owner/bitmap、FLST 链、inode 计数与 fragment 槽的一致。

2c 将在此之上实现 `SegmentPageAllocator`（fragment<32 vs extent 决策、最多 4 extent、hint/方向、autoextend 重试）、`SpaceReservation`、`NoFreeSpaceException` 与对上 facade。

## 2. 范围与非目标

**做**：
- `ExtentDescriptorRepository` 扩展：extent↔链节点地址互转、bitmap 查询（首个空闲页、已分配计数、满/空判定）。
- `FreeExtentService`：freeLimit 填充、acquire/return FREE extent、全局 fragment 页分配 + 全局链迁移。
- `SegmentSpaceService`：fragment 页分配（含 inode 记录）、给 segment 分配 extent、从 segment extent 分配页、释放页（两路回收）。

**不做**（留 2c / 后续，注释标简化点）：
- `SegmentPageAllocator`（fragment<32 vs extent 的选择、最多 4 extent、hint/方向）、`ExtentAllocationPolicy`、`SpaceReservation`、`NoFreeSpaceException`、autoextend 触发与对上 facade。
- 不在 2b 决定“用 fragment 还是 extent”；2b 仅提供两类原语，由 2c 选择。
- redo/WAL：本片仍 no-redo（写页标脏、不产 redo、不声明 crash-safe）。
- 部分尾部 extent（currentSize 非 extent 对齐时的残余页）不作为可分配空间；2b 只材料化**整 extent**。
- 多 inode 页、独立 XDES 管理页：承袭 slice-1/2a 的单 inode 页 page2、XDES 内嵌 page0 首批。

## 3. 包与依赖方向

- 全部位于 `cn.zhangyis.db.storage.fsp`。
- 依赖：`fsp → buf`（BufferPool/PageGuard/PageLatchMode，用于跨页预闩）、`fsp → mtr`、`fsp → domain`、`fsp → common.exception`，并复用 slice-1/2a 的 fsp 内部类型（`Flst`、`FileAddress`、三个 repo、`ExtentState`/`SegmentPurpose`）。无新增反向依赖。

## 4. 不变量（全程维护）

- **state ⇔ 所在链**：`FREE`⇔FSP_FREE；`FREE_FRAG`⇔FSP_FREE_FRAG；`FULL_FRAG`⇔FSP_FULL_FRAG；`FSEG`⇔该 segment 的 SEG_FREE/SEG_NOT_FULL/SEG_FULL 之一；`FSEG_FRAG`=系统 extent0（不在任何链）。任一时刻一个 extent 至多在一条链上。
- **owner**：FREE/FREE_FRAG/FULL_FRAG/系统 extent0 → owner 0；FSEG → owner=该 segmentId。
- **bitmap**：1 位/页，1=已分配；`FREE_FRAG` 恒非满（满则必为 FULL_FRAG）；`SEG_FREE` 恒全空（assign 后未分配），分配首页即转 SEG_NOT_FULL。
- **usedPageCount**：= 该 segment 已用页总数（fragment 页 + 其 FSEG extent 内已用页）；每次分配 +1、释放 -1。
- **freeLimitPageNo**：恒为 pagesPerExtent 的整数倍；= 尚未材料化进 free-list 机制的首页；填充按整 extent 推进，跳过 extent0，受 currentSizeInPages 约束。

## 5. `ExtentDescriptorRepository` 扩展

新增（均不改 slice-1 既有方法签名）：
- `FileAddress listNodeAddr(ExtentId extentId)`：返回该 extent 的 FLST 节点起址 = `FileAddress.of(page0, entryOffset(extentNo) + ExtentDescriptorLayout.PREV)`；复用既有 `entryOffset`（超首批抛 `FspMetadataException`）。`extentNo==0` 允许返回地址（调用方不会把系统 extent 入链）。
- `ExtentId extentIdOfNode(SpaceId spaceId, FileAddress nodeAddr)`：反向。要求 `nodeAddr.pageNo()==0` 且 `(offset - XDES_BASE - PREV)` 非负且能被 `ENTRY_SIZE` 整除，否则 `FspMetadataException`（页上链指针损坏）；返回 `ExtentId.of(spaceId, extentNo)`。
- `OptionalInt firstFreePageIndex(MiniTransaction mtr, ExtentId extentId)`：在 `[0, pagesPerExtent)` 内扫 bitmap，返回首个未分配位下标；满则 `OptionalInt.empty()`（S latch）。
- `int allocatedPageCount(MiniTransaction mtr, ExtentId extentId)`：bitmap 在 `[0, pagesPerExtent)` 的置位数（S）。
- `boolean isFull(mtr, extentId)` = count==pagesPerExtent；`boolean isEmpty(mtr, extentId)` = count==0。

> 这些是 XDES 读/纯计算，归 `ExtentDescriptorRepository` 自然。`pagesPerExtent` 取自 repo 已持有的 `pageSize`。

## 6. `FreeExtentService`（全局，仅 page0）

构造 `(BufferPool pool, SpaceHeaderRepository headerRepo, ExtentDescriptorRepository xdes, Flst flst)`。

**锁序前置**：每个会写 page0 的 op 开头先 `mtr.getPage(pool, page0, EXCLUSIVE)` 预闩，使后续 `getFirst`/`firstFreePageIndex` 等读取在已持 X 上降级，避免同页 S→X 被 MTR 拒绝（slice-1 已证：先持 X 后 S 再 X 合法）。FreeExtentService 只碰 page0，无跨页逆序问题。

- `Optional<ExtentId> fillFreeListStep(mtr, spaceId)`：
  - `pe = pageSize.pagesPerExtent()`；循环：`extentNo = freeLimit / pe`；若 `(extentNo+1)*pe > currentSize` → 返回 empty；否则 `setFreeLimitPageNo(freeLimit+pe)`；若 `extentNo==0` 跳过（不入链）继续循环；否则 `xdes.initFree(extentNo)` + `flst.addLast(headerRepo.freeExtentListBaseAddr, xdes.listNodeAddr(extentNo))`，返回 `ExtentId(extentNo)`。
- `Optional<ExtentId> acquireFreeExtent(mtr, spaceId)`：`head = flst.getFirst(freeBase)`；非空则 `ext = xdes.extentIdOfNode(head)`、`flst.remove(freeBase, head)`、返回 ext；空则 `fillFreeListStep`，若有则再 `getFirst`+`remove` 弹出返回，否则 empty。
- `void returnFreeExtent(mtr, spaceId, ExtentId ext)`：`xdes.initFree(ext)`（state FREE/owner 0/bitmap 清零/prev-next NULL）+ `flst.addLast(freeBase, listNodeAddr(ext))`。调用方须保证 ext 已不在其它链（先 remove）。
- `Optional<PageId> allocateFragmentPage(mtr, spaceId)`：
  - `ffBase = headerRepo.freeFragExtentListBaseAddr`；`head = flst.getFirst(ffBase)`；
  - 若非空 `frag = extentIdOfNode(head)`；否则 `acquireFreeExtent`（empty 则返回 empty）→ `frag`，`xdes.writeState(frag, FREE_FRAG)`，`flst.addLast(ffBase, listNodeAddr(frag))`；
  - `idx = firstFreePageIndex(frag).getAsInt()`（FREE_FRAG 恒非满，必有）；`setPageAllocated(frag, idx, true)`；`pageNo = frag.firstPageNo(pageSize)+idx`；
  - 若 `isFull(frag)`：`flst.remove(ffBase, node)` + `writeState(frag, FULL_FRAG)` + `flst.addLast(fullFragBase, node)`；
  - 返回 `PageId.of(spaceId, PageNo.of(pageNo))`。

## 7. `SegmentSpaceService`（segment 侧，page0↔page2）

构造 `(BufferPool pool, SegmentInodeRepository inodeRepo, ExtentDescriptorRepository xdes, Flst flst, FreeExtentService freeExtents)`。

**锁序前置**：每个公开 op 开头调用私有 `latchSpaceThenInode(mtr, spaceId)`：先 `mtr.getPage(pool, page0, X)` 再 `mtr.getPage(pool, page2, X)`（§18 顺序）。之后所有 repo/Flst 读写都在已持 X 上可重入（读取的 S 视为降级），既不逆序也不触发 MTR 同页 S→X 拒绝（slice-1 已证：先持 X 后 S 再 X 合法）。

- `Optional<PageNo> allocateFragmentPage(mtr, spaceId, int inodeSlot)`：
  - `slotIdx = inodeRepo.requireFreeFragmentSlot(inodeSlot)`（32 满抛 `FspMetadataException`——边界由 2c 用 <32 决策避免）；
  - `pageOpt = freeExtents.allocateFragmentPage(spaceId)`；empty → 返回 empty（无空间，未改 inode）；
  - `pageNo = pageOpt.pageNo`；`inodeRepo.setFragmentPage(inodeSlot, slotIdx, Optional.of(pageNo))`；`bumpUsed(inodeSlot, +1)`；返回 pageNo。
- `Optional<ExtentId> assignExtentToSegment(mtr, spaceId, int inodeSlot)`：
  - `segId = inodeRepo.read(inodeSlot).segmentId()`；`acq = freeExtents.acquireFreeExtent(spaceId)`；empty → empty；
  - `xdes.writeState(ext, FSEG)`；`xdes.writeOwner(ext, Optional.of(segId))`；`flst.addLast(inodeRepo.freeExtentListBaseAddr(inodeSlot), listNodeAddr(ext))`；返回 ext。
- `Optional<PageNo> allocatePageFromSegmentExtents(mtr, spaceId, int inodeSlot)`：
  - `notFull = notFullExtentListBaseAddr(inodeSlot)`、`segFree = freeExtentListBaseAddr(inodeSlot)`；
  - `head = flst.getFirst(notFull)`；若非空 `ext = extentIdOfNode(head)`、`fromFree=false`；否则 `head = flst.getFirst(segFree)`，空则返回 empty，`ext = extentIdOfNode(head)`、`fromFree=true`；
  - `idx = firstFreePageIndex(ext).getAsInt()`；`setPageAllocated(ext, idx, true)`；`pageNo = ext.firstPageNo+idx`；
  - 迁移：`fromFree` → `flst.remove(segFree, node)`，然后按 `isFull(ext)?` 加入 SEG_FULL 否则 SEG_NOT_FULL；非 `fromFree` 且 `isFull(ext)` → `flst.remove(notFull, node)` + `flst.addLast(segFull, node)`；
  - `bumpUsed(inodeSlot, +1)`；返回 pageNo。
- `void freePage(mtr, spaceId, int inodeSlot, PageId pageId)`：
  - `ext = ExtentId.from(pageId, pageSize)`；`ext.extentNo==0` → `FspMetadataException`（系统页不可释放）；`idxInExtent = pageId.pageNo - ext.firstPageNo`；`state = xdes.read(ext).state()`；
  - **fragment extent**（`FREE_FRAG`/`FULL_FRAG`）：`setPageAllocated(ext, idx, false)`；扫 inode 32 frag 槽找值==pageNo 者 `setFragmentPage(slot, empty)`，找不到 → `FspMetadataException`（非本段 fragment 页）；`bumpUsed(-1)`；若原 `FULL_FRAG` → remove(fullFrag)+writeState(FREE_FRAG)+addLast(freeFrag)；若 `isEmpty(ext)` → remove(freeFrag)+`freeExtents.returnFreeExtent(ext)`。
  - **segment extent**（`FSEG`）：校 `owner==read(inodeSlot).segmentId` 否则 `FspMetadataException`；`wasFull=isFull(ext)`；`setPageAllocated(ext, idx, false)`；`bumpUsed(-1)`；若 `wasFull` → remove(segFull)+addLast(segNotFull)；若 `isEmpty(ext)` → remove(segNotFull)+writeState(FREE)+writeOwner(empty)+addLast(FSP_FREE)。
  - 其它 state（`FREE`/`FSEG_FRAG`）→ `FspMetadataException`（释放未分配/系统区页）。
- 私有 `bumpUsed(inodeSlot, delta)`：`read` 现值 → `setUsedPageCount(+delta)`（delta=-1 时若现值 0 → `FspMetadataException`，计数损坏）。

## 8. 数据流、并发与恢复

- 所有写经 MTR page latch（跨页 op 先 page0 X 再 page2 X 预闩），commit 时 LIFO 释放、页标脏。
- 全局 op（FreeExtentService）只碰 page0；segment op（SegmentSpaceService）碰 page0+page2，按 §18 page0→page2。
- 这些 op 是“读改写/查找”型、**非幂等**（与 slice-1 分配型方法同类），redo 切片接入后再纳入重放框架；本片 no-redo、不声明 crash-safe（§15 推迟满足）。
- `Optional.empty()` 表“当前无空间”，是正常返回（2c 决定 autoextend 或抛 `NoFreeSpaceException`），不抛异常。

## 9. 异常

- `DatabaseValidationException`：null 参数、非法 inodeSlot/pageId 等（多数经下层 repo/Flst 校验透传）。
- `FspMetadataException`：链节点偏移不对齐、释放系统/未分配页、fragment 页不属本段、owner 不符、usedPageCount 下溢、页上 state/链账本损坏。
- 下层（repo/Flst/buf/mtr）异常透传。

## 10. 测试（buf+fil+mtr+@TempDir 真实驱动；tablespace ≥128 页 = ≥2 extent，使 extent1 可填）

- `ExtentDescriptorRepository` 扩展：`listNodeAddr`↔`extentIdOfNode` 往返、坏偏移拒绝；`firstFreePageIndex`/`allocatedPageCount`/`isFull`/`isEmpty`（空/部分/满，4K/16K 边界）。
- `FreeExtentServiceTest`：fill 跳过 extent0、按 extent 推进 freeLimit、currentSize 边界返回 empty；acquire（含 fill）与重复 acquire 顺序；allocateFragmentPage（新建 FREE_FRAG、连续取页、满则迁 FULL_FRAG）；returnFreeExtent 回 FSP_FREE。
- `SegmentSpaceServiceTest`：allocateFragmentPage 记 inode 槽 + usedPageCount；assignExtentToSegment（FSEG/owner/SEG_FREE）；allocatePageFromSegmentExtents（SEG_FREE→NOT_FULL、填满→FULL）；freePage 两路（fragment：清槽 + FULL_FRAG→FREE_FRAG + 全空回 FSP_FREE；segment：FULL→NOT_FULL + 全空回 FSP_FREE/owner 清）；释放系统/未分配页 → FspMetadataException；跨页锁序不死锁、不残留阻塞线程。
- 协作回归：分配 → 释放 → 再分配，bitmap/链/计数自洽（property 风格小循环）。

## 11. 简化点（注释标注）

- 只材料化整 extent（不处理 currentSize 非对齐的尾部残页）。
- fragment 页全局共享自 FSP_FREE_FRAG（owner 0），归属经 inode fragment 槽记录；与 InnoDB 思路一致、非二进制兼容。
- 单 inode 页 page2、XDES 内嵌 page0 首批（承袭上游）。
- 非幂等分配/释放、no-redo、无 pageLSN，不具备 crash-safe；list/bitmap/inode 改动未来归 `UPDATE_XDES`/`UPDATE_SPACE_HEADER`/`UPDATE_SEGMENT_INODE`。
- 锁序靠服务层显式预闩（page0 X→page2 X）保证；多页 node 跨页等更复杂场景留后续。

## 12. 后续切片衔接（2c）

- `SegmentPageAllocator.allocatePage(segment, hint, direction, mtr)`：fragment 槽<32 → `SegmentSpaceService.allocateFragmentPage`；否则 `allocatePageFromSegmentExtents`，无可用 extent 则 `assignExtentToSegment`（policy 决定 1~4 extent、hint/方向）后重试；全空间不足则 `AutoExtendPolicy` 扩 currentSize 后重试一次，仍失败抛 `NoFreeSpaceException`；无条件跳过 extent0。
- `SpaceReservation`（RAII）、`NoFreeSpaceException`、对上 `DiskSpaceManager` facade。
- redo 切片：把本片非幂等 op 纳入 WAL 重放，并写页首 FilePageHeader（checksum/pageLSN）。
