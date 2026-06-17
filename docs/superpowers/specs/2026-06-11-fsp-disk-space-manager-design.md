# Spec：SegmentPageAllocator + DiskSpaceManager facade（slice 2c）

- 日期：2026-06-11
- 关联设计：`C:\coding\java\self\dbtest\dbtest\docs\design\innodb-disk-manager-design.md`（§7 分配、§8 扩展、§13.1 DiskSpaceManager、§14 Facade、§16.3 split 分配链、§18 锁序）
- 上游依赖：slice 1（仓储）、2a（Flst）、2b（FreeExtentService/SegmentSpaceService/ExtentDescriptor 扩展）均已完成
- 状态：brainstorming 评审通过——2c 完整含 facade；**autoextend 放 facade**；**SpaceReservation 延后**；PageCursor 与 hint/direction 不在本片，待 record/placement 切片。

## 1. 背景与目标

2b 提供了 extent/页分配原语，但还没有“给定 segment 自动选择 fragment vs extent、必要时扩文件、对上提供门面”的编排。2c 落地：
- `SegmentPageAllocator`：把 2b 原语编排成“为 segment 分配一个页”——fragment<32 走 fragment 路径，否则走 segment-extent 路径（无可用 extent 则按 `ExtentAllocationPolicy` 一次取 1..4 个 extent 再试）。**纯分配**：只在当前 currentSize 内分配，无空间返回 `Optional.empty()`，不扩文件、不抛 NoFreeSpace。
- `DiskSpaceManager`（storage.api Facade）：tablespace/segment 生命周期 + 分配/释放 + autoextend 重试 + 用量查询，是上层（btree/dd）使用磁盘空间的稳定入口。

## 2. 范围与非目标

**做**：
- `fsp.NoFreeSpaceException`、`fsp.ExtentAllocationPolicy` + `DefaultExtentAllocationPolicy`、`fsp.SegmentInodeRepository.hasFreeFragmentSlot`、`fsp.SegmentPageAllocator`。
- 新包 `cn.zhangyis.db.storage.api`：`DiskSpaceManager` facade、`SegmentRef` 句柄、`SpaceUsage` 用量记录。

**不做**（延后，注释标注）：
- `SpaceReservation`/预留（无消费者 B+Tree split，YAGNI；待引入消费者的切片）。
- `PageCursor`（属 record/storage.api 类型化页访问，设计 §13.3，与分配无关）。
- `allocatePage` 的 hint/direction 就近放置（当前 2b assign 不支持 hint；待 placement 策略切片再加签名与逻辑）。
- redo/WAL（本片仍 no-redo，不声明 crash-safe）。
- 多文件表空间、独立 XDES 管理页、change buffer 等（承袭上游）。

## 3. 包与依赖方向

- `fsp` 新增类位于 `cn.zhangyis.db.storage.fsp`；facade 位于新包 `cn.zhangyis.db.storage.api`。
- 依赖：`api → fsp + fil(PageStore) + buf + mtr + domain`；`fsp.SegmentPageAllocator → buf(BufferPool 预闩) + fsp(2b 服务/仓储/Flst) + domain`，**不依赖 fil**（autoextend 在 facade）。符合 §4 与 §15 推荐链 `api -> fsp -> buf -> fil`。

## 4. `NoFreeSpaceException`

`fsp.NoFreeSpaceException extends DatabaseRuntimeException`（可恢复：调用方可放弃当前操作/回滚）。由 `DiskSpaceManager.allocatePage` 在 autoextend 一次后仍无空间时抛出。构造保留 message 与 (message, cause)。

## 5. `ExtentAllocationPolicy`（Strategy）

```
interface ExtentAllocationPolicy { int extentsToAcquire(long ownedExtentCount); }  // 返回 1..4
```
`DefaultExtentAllocationPolicy`：`ownedExtentCount <= 0 → 1`；否则 `min(4, ownedExtentCount)`（顺序增长封顶 4，对应 §7.2 step3）。值表：0→1,1→1,2→2,3→3,4→4,10→4。`ownedExtentCount` 由 allocator 用三条 segment 链长之和提供。

## 6. `SegmentInodeRepository.hasFreeFragmentSlot`（新增方法）

`boolean hasFreeFragmentSlot(MiniTransaction mtr, SpaceId spaceId, int inodeSlot)`：S 取 page2，扫 32 个 fragment 槽，存在值为 0 的槽返回 true。供 allocator 判“fragment 已用 <32”。（与既有 `requireFreeFragmentSlot` 区别：非抛错的布尔判定。）

## 7. `SegmentPageAllocator`

构造 `(BufferPool pool, SegmentInodeRepository inodeRepo, Flst flst, SegmentSpaceService segSpace, ExtentAllocationPolicy policy)`（不持 PageStore）。

**锁序**：`allocatePage` 开头 `latchSpaceThenInode`（page0 X→page2 X），使后续 `hasFreeFragmentSlot`/`Flst.length`（S 降级）与 2b 原语（reentrant X）不逆序、不触发同页 S→X。

- `Optional<PageId> allocatePage(MiniTransaction mtr, SpaceId spaceId, int inodeSlot)`：
  1. 预闩 page0 X、page2 X。
  2. `hasFreeFragmentSlot` 为真 → `segSpace.allocateFragmentPage(inodeSlot)`，返回 `map(pn → PageId.of(spaceId, pn))`（empty 透传）。
  3. 否则 extent 路径：`segSpace.allocatePageFromSegmentExtents(inodeSlot)`，有则返回。
  4. 无则 `n = policy.extentsToAcquire(ownedExtentCount)`；循环 `segSpace.assignExtentToSegment` 最多 n 次（任一 empty 即停，记 assignedAny）；未分到任何 extent → 返回 empty；否则再 `allocatePageFromSegmentExtents` 返回（map 成 PageId）。
- `ownedExtentCount` = `Flst.length(segFreeBase) + length(notFullBase) + length(fullBase)`（base 取自 inodeRepo 访问器）。

**纯净性**：本类只在 currentSize 内分配，从不扩文件、不抛 NoFreeSpace；“无空间”一律 `Optional.empty()`。

## 8. `cn.zhangyis.db.storage.api`

### 8.1 `SegmentRef`（record）
`(SpaceId spaceId, int inodeSlot, SegmentId segmentId)`，构造校验非空、inodeSlot≥0、segmentId.value>0。

### 8.2 `SpaceUsage`（record）
`(PageNo currentSizeInPages, PageNo freeLimitPageNo, long nextSegmentId)`，用量快照。

### 8.3 `DiskSpaceManager`（Facade）
构造 `(BufferPool pool, PageStore pageStore, PageSize pageSize)`，内部构建 `SpaceHeaderRepository`、`ExtentDescriptorRepository`、`SegmentInodeRepository`、`Flst`、`FreeExtentService`、`SegmentSpaceService`、`SegmentPageAllocator(DefaultExtentAllocationPolicy)`。
- `void createTablespace(MiniTransaction mtr, SpaceId spaceId, Path path, PageNo initialSizePages)`：`pageStore.create(spaceId, path, pageSize, initialSizePages)`（fil，先于任何 getPage）→ `headerRepo.initialize(mtr, fresh)`（currentSize=initialSize、freeLimit=0、nextSegmentId=1、三链 EMPTY、firstInode=2、serverVersion 固定、spaceVersion=1）→ `xdes.reserveSystemExtent(mtr, spaceId)`。
- `void openTablespace(SpaceId, Path)` / `void closeTablespace(SpaceId)`：委托 `pageStore.open/close`（无 mtr）。
- `SegmentRef createSegment(MiniTransaction mtr, SpaceId spaceId, SegmentPurpose purpose)`：`segId = headerRepo.allocateNextSegmentId`（page0 X）→ `slot = inodeRepo.allocateSlot(SegmentId.of(segId), purpose)`（page2 X，page0→page2 顺序天然）→ `new SegmentRef(spaceId, slot, SegmentId.of(segId))`。
- `PageId allocatePage(MiniTransaction mtr, SegmentRef ref)`：`allocator.allocatePage` → 有则返回；否则 `newSize = pageStore.extend(spaceId)` + `headerRepo.setCurrentSizeInPages(mtr, spaceId, newSize)` → 再 `allocator.allocatePage` → 有则返回；仍无 → `NoFreeSpaceException`。extend 是 fil 文件增长（FileSizeLock，不反等 page latch，§18 允许），在 allocator 已持 page0/page2 X 之后调用。
- `void freePage(MiniTransaction mtr, SegmentRef ref, PageId pageId)`：委托 `segSpace.freePage`。
- `void dropSegment(MiniTransaction mtr, SegmentRef ref)`：预闩 page0 X→page2 X；① 遍历 32 fragment 槽，非空者 `segSpace.freePage`（清 bitmap/槽 + 全局 frag 链迁移/回收）；② 对三条 SEG 链各 `while getFirst≠null { node→ext=xdes.extentIdOfNode; flst.remove(base,node); freeExtents.returnFreeExtent(ext) }`；③ `inodeRepo.freeSlot`（整条清零，usedPageCount 一并归零）。
- `SpaceUsage usage(MiniTransaction mtr, SpaceId spaceId)`：读 header → `new SpaceUsage(currentSizeInPages, freeLimitPageNo, nextSegmentId)`。

## 9. 数据流、并发与恢复

- 所有 page 写经 MTR latch；跨 page0/page2 的 op 先 page0 X 再 page2 X（allocator 与 dropSegment 显式预闩；createSegment 天然 page0→page2）。
- autoextend：facade 在 allocator 返回 empty 后 `extend`（fil）+ `setCurrentSizeInPages`（page0 X 重入）+ 重试一次；extend 永久增长文件，no-redo 下不随 MTR 回滚撤销（教学取舍）。
- 这些 op 非幂等（读改写/查找/扩展），redo 切片后再纳入重放；本片 no-redo、不声明 crash-safe（§15 推迟满足）。

## 10. 异常

- `NoFreeSpaceException`：autoextend 一次后仍无空间。
- `FspMetadataException`：下层（2b/仓储）页上账本损坏透传。
- `DatabaseValidationException`：null/非法参数（SegmentRef 校验、facade 参数）。
- 下层 buf/mtr/fil 异常透传。

## 11. 测试（buf+fil+mtr+@TempDir 真实驱动）

- `ExtentAllocationPolicyTest`：Default 值表 0→1/1→1/2→2/4→4/10→4。
- `SegmentInodeRepository.hasFreeFragmentSlot`：空槽→true、填满 32→false（并入 SegmentInode 既有测试或新测试）。
- `SegmentPageAllocatorTest`：fragment 路径（前 32 页为 fragment）；满 32 后 extent 路径（assign 1 extent 再分配）；无空间返回 `Optional.empty()`（不扩文件、不抛）；owned 增长触发 policy 多 extent（用 stub policy 返回 >1 验证一次 assign 多个）。
- `DiskSpaceManagerTest`：createTablespace→usage（currentSize/freeLimit/nextSegmentId）；createSegment（segId=1、slot=0）；allocatePage 连续分配（fragment→extent）；**autoextend**：填满 128 页表空间后再分配 → currentSize 增至 192 且返回 extent2 首页（page 128）；**NoFreeSpace**：size=4 表空间（无可用整 extent，extend 仅 +1 页仍不足）→ allocatePage 抛 `NoFreeSpaceException`；freePage 往返；dropSegment 后 fragment 槽清零、segment 链空、extent 归还 FSP_FREE、inode 槽可复用。

## 12. 简化点（注释标注）

- autoextend 在 facade、纯 allocator；extent 增长策略简单封顶 4；hint/direction 与就近放置未做。
- SpaceReservation 延后（无消费者）。
- no-redo、无 pageLSN、不 crash-safe；扩展不随 MTR 回滚撤销。
- 单 inode 页 page2、XDES 内嵌 page0 首批（承袭上游）。
- `DefaultIbdAutoExtendPolicy` 下 ≥1 extent 的表空间扩展恒补满 extent，故 NoFreeSpace 仅在 <1 extent 的极小表空间出现（测试用 size=4 触发）。

## 13. 后续切片衔接

- placement：给 `allocatePage` 加 hint/direction + 就近 extent 选择策略。
- SpaceReservation：引入 B+Tree split 等多页消费者时落地（RAII + 预留账本）。
- PageCursor / record 层类型化页访问。
- redo 切片：把 2a/2b/2c 非幂等 op 纳入 WAL，并写页首 FilePageHeader。
