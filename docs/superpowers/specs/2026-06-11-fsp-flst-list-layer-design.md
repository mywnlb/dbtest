# Spec：fsp FLST 跨页双向链表层（slice 2a）

- 日期：2026-06-11
- 关联设计：`C:\coding\java\self\dbtest\dbtest\docs\design\innodb-disk-manager-design.md`（§5.4 extent list、§6.2–6.4 page0/page2 元数据、§7 分配、§18 锁序）
- 上游约束：本仓库 `AGENTS.md` / `CLAUDE.md`
- 依赖切片：slice 1（fsp 元数据仓储：SpaceHeader/XDES/SegmentInode + buf/mtr/fil）已完成
- 状态：brainstorming 评审通过（决定：采用**完整 InnoDB FLST base（first+last+len）**，并在本片内迁移 slice-1 的 6 个 list 头），待 spec 自查 + 用户复核

## 1. 背景与目标

slice 1 落地了三类首区管理页的单页 CRUD，但每条 extent list 头只存了**单个 `FileAddress`**（无 tail、无 len），且 list「头/节点只存取、不走链」。slice 2a 落地 InnoDB `flst0flst` 风格的**跨页双向链表原语**：一个通用 `Flst`，按 `FileAddress` 寻址 base 与 node，全局自由链（FSP_FREE / FSP_FREE_FRAG / FSP_FULL_FRAG）与 segment extent 链（SEG_FREE / SEG_NOT_FULL / SEG_FULL）复用同一套实现（对应 InnoDB 一处 flst 实现处处复用）。

因为选定**完整 FLST base（len + first + last）**模型，本片必须把 slice-1 的 6 个单 `FileAddress` list 头迁移为 32 字节 base 节点，使数据模型在 2a 结束时自洽。2b（extent 分配/回收）、2c（SegmentPageAllocator + reservation + autoextend）将直接调用本层。

目标：
- 通用、可复用、页无关的 `Flst`：addFirst/addLast/remove/getFirst/getLast/length/getNext/getPrev，跨页走链。
- O(1) 头插、O(1) 尾插（base.last）、O(1) 任意删（node.prev/next 双向）、O(1) length（base.len）。
- 与 slice-1 锁序、MTR 同页 S→X 拒绝、LIFO 释放一致。
- 迁移 slice-1 list 头到 `FlstBase`，保持其 initialize→read 往返与 allocateSlot 清零语义。

## 2. 范围与非目标

**做**：
- `FlstBase`、`FlstNode` 值对象 + `FlstBaseLayout` / `FlstNodeLayout` 偏移常量。
- `Flst` 链表算法（经 MTR + BufferPool 读写 base/node 字段）。
- slice-1 迁移：`SpaceHeaderLayout` / `SpaceHeaderSnapshot` / `SpaceHeaderRepository`、`SegmentInodeLayout` / `SegmentInode` / `SegmentInodeRepository` 的 list 头 `FileAddress`→`FlstBase`；repo 暴露各 base 的 `FileAddress` 访问器供 Flst/2b 使用。
- 同步更新受影响的 slice-1 测试。

**不做**（留 2b / 2c / 后续，注释标简化点）：
- extent 状态迁移与分配逻辑（拆 FSP_FREE→FREE_FRAG/FSEG、fragment page、free 回收）——2b。
- `ExtentAllocationPolicy`、`SegmentPageAllocator`、`SpaceReservation`、autoextend 重试、`NoFreeSpaceException`——2c。
- 把 XDES entry 包装成「extent 节点地址」的便捷构造（2b 负责由 `ExtentId` 算出 node 地址）；2a 的 `Flst` 只接收裸 `FileAddress`。
- redo/WAL：本片仍是 no-redo 原型（写页标脏、不产 redo、不声明 crash-safe）。
- 节点跨任意多页：本片约定**同一条链的所有 node 位于同一页**（XDES node 均在 page 0），base 位于 page 0（全局链）或 page 2（segment 链）；多页 node 的锁序留后续。

## 3. 包与依赖方向

- 新增类位于 `cn.zhangyis.db.storage.fsp`；迁移改动同包。
- 依赖：`fsp → buf`（BufferPool/PageGuard/PageLatchMode）、`fsp → mtr`（MiniTransaction）、`fsp → domain`（SpaceId/PageId/PageNo）、`fsp → common.exception`。无新增反向依赖；`fsp→buf→fil`、`fsp→mtr→buf` 仍无环。

## 4. 共享类型

### 4.1 `FlstNode`（record）
表达一个链表节点的 prev/next 指针对（InnoDB FLST node = 两个 fil_addr）。
- 字段：`(FileAddress prev, FileAddress next)`；空链位置用 `FileAddress.NULL`。
- 编码 24 字节：`prev`(12) + `next`(12)。零态（全零页）解码为 `(NULL, NULL)`，即「不在任何链中」。

### 4.2 `FlstBase`（record）
表达一条链的 base（表头），含长度与首尾指针（InnoDB FLST base = len + first + last）。
- 字段：`(long length, FileAddress first, FileAddress last)`；空链 = `(0, NULL, NULL)`。
- 编码 32 字节：`length`(long 8) + `first`(12) + `last`(12)。零态解码为空链，使零初始化页天然是空链（与 slice-1「零初始化即有效默认」一致）。
- 校验分两层，避免把磁盘损坏错当成程序错误：
  - **record 构造**只校验 `length >= 0`（违反属调用方程序错误 → `DatabaseValidationException`）。
  - **磁盘解码**（在 Flst/repo 的 read 路径，构造 record 之前）额外校验空链一致性：`length==0` ⇔ `first` 与 `last` 均 NULL；不一致视为页上链账本损坏 → `FspMetadataException`（§8）。record 本身不强制该一致性，以保持值对象简单且让损坏归类清晰。

### 4.3 `FlstBaseLayout`（条内偏移）
| 字段 | 偏移 | 字节 |
| --- | --- | --- |
| LEN | 0 | long 8 |
| FIRST | 8 | FileAddress 12 |
| LAST | 20 | FileAddress 12 |
- `SIZE = 32`。

### 4.4 `FlstNodeLayout`（条内偏移）
| 字段 | 偏移 | 字节 |
| --- | --- | --- |
| PREV | 0 | FileAddress 12 |
| NEXT | 12 | FileAddress 12 |
- `SIZE = 24`。
- 约定：一个 node 由其**起址**（指向 PREV 字段）定位。XDES extent entry 的 prev/next 恰好是一个 node（`ExtentDescriptorLayout.PREV=12`、`NEXT=24`），故 extent e 的 node 起址 = `FileAddress.of(page0, ExtentDescriptorLayout.entryOffset(e) + ExtentDescriptorLayout.PREV)`；node 内 PREV/NEXT(0/12) 映射到 entry 的 +12/+24。该换算由 2b 负责，2a 不依赖 XDES 细节。

## 5. `Flst`（链表算法）

构造 `Flst(BufferPool pool)`。所有方法签名 `(MiniTransaction mtr, SpaceId spaceId, FileAddress baseAddr, FileAddress nodeAddr)`（读取类无 nodeAddr 或以 nodeAddr 为输入）。`baseAddr`/`nodeAddr` 是空间内地址（PageNo+offset），`Flst` 用 `PageId.of(spaceId, addr.pageNo())` + `mtr.getPage` 读写。`baseAddr`、`nodeAddr` 不得为 `FileAddress.NULL`（否则 `DatabaseValidationException`）。

### 5.1 锁序（关键约束）
mutator 在读写任何字段前，**按 pageNo 升序对涉及页取 X latch**：先取 `nodeAddr.pageNo` 与 `baseAddr.pageNo` 的升序去重集合（page0 先于 page2，符合 design §18 / slice-1 §9）。本片约定「同链 node 同页」，故邻居节点（oldFirst/oldLast/prev/next）与 `nodeAddr` 同页、已被覆盖，不会引入逆序取闩。读取类（getFirst/getLast/length/getNext/getPrev）取 S。复用 MTR 同页 S→X 拒绝与 LIFO 释放；同页多次 getPage 走 RRWL 可重入。

### 5.2 操作语义
- `void addLast(mtr, spaceId, baseAddr, nodeAddr)`：尾插。
  - 读 base；`node.prev = base.last; node.next = NULL`。
  - base 空（`base.last.isNull()`）：`base.first = nodeAddr`；否则 `oldLast.next = nodeAddr`。
  - `base.last = nodeAddr; base.length += 1`。写 node、（oldLast.next）、base。
- `void addFirst(mtr, spaceId, baseAddr, nodeAddr)`：头插（对称：用 base.first / oldFirst.prev）。
- `void remove(mtr, spaceId, baseAddr, nodeAddr)`：解链。
  - 读 `node.prev`、`node.next`。
  - `node.prev` 为 NULL：`base.first = node.next`；否则 `prev.next = node.next`。
  - `node.next` 为 NULL：`base.last = node.prev`；否则 `next.prev = node.prev`。
  - `base.length -= 1`（若 base.length 已为 0 → `FspMetadataException`，链账本损坏）。
  - 清 `node.prev = node.next = NULL`（移除后节点指针置空，避免悬挂被误读）。
- `FileAddress getFirst(...)` / `getLast(...)`：读 base.first / base.last。
- `long length(...)`：读 base.len。
- `FileAddress getNext(mtr, spaceId, nodeAddr)` / `getPrev(...)`：读 node.next / node.prev（S）。

### 5.3 不变量
- base.length == 实际链长；空链 ⇔ first/last 均 NULL ⇔ length==0。
- 双向一致：非头节点 `cur.prev.next == cur`；非尾节点 `cur.next.prev == cur`（由 add/remove 维护，不在每次操作中校验，避免 O(n)）。
- 节点是否「属于某链」由调用方保证（与 InnoDB 一致，2a 不做 O(n) 成员检测）；2b 通过 extent 状态机保证只对正确的链增删。

## 6. slice-1 迁移

### 6.1 SpaceHeaderLayout（page 0）
3 个 list 头 12B→32B base；后续字段顺移：
| 字段 | 新偏移 | 字节 |
| --- | --- | --- |
| SPACE_ID | 38 | 4 |
| PAGE_SIZE_BYTES | 42 | 4 |
| SPACE_FLAGS | 46 | 4 |
| CURRENT_SIZE | 50 | 8 |
| FREE_LIMIT | 58 | 8 |
| NEXT_SEGMENT_ID | 66 | 8 |
| FREE_EXTENT_LIST_BASE | 74 | 32 |
| FREE_FRAG_LIST_BASE | 106 | 32 |
| FULL_FRAG_LIST_BASE | 138 | 32 |
| FIRST_INODE_PAGE | 170 | 8 |
| SDI_ROOT | 178 | 8 |
| SERVER_VERSION | 186 | 4 |
| SPACE_VERSION | 190 | 8（end 198）|
- `XDES_BASE`：200 → **256**（留头；198 ≤ 256）。slice-1 XDES 测试经 `ExtentDescriptorLayout.entryOffset/maxEntriesInPage0` 计算 → 自适应。

### 6.2 SpaceHeaderSnapshot
- 3 个 `FileAddress` 字段 → `FlstBase`：`freeExtentList`、`freeFragExtentList`、`fullFragExtentList`。
- 新建表空间 init 取三者为 `FlstBase` 空链 `(0,NULL,NULL)`；其余字段不变（nextSegmentId=1、firstInodePageNo=2）。

### 6.3 SpaceHeaderRepository
- `initialize` 写三个空 base（用 `FlstBaseLayout`）。
- `read` 解码三个 base 为 `FlstBase`。
- **移除**直接 `setFreeExtentListHead(FileAddress)` 等 3 个 setter（base 由 `Flst` 维护，不再整体覆盖）。
- 新增 base 地址访问器（纯计算，无 IO）：`FileAddress freeExtentListBaseAddr(SpaceId)`、`freeFragExtentListBaseAddr`、`fullFragExtentListBaseAddr`，各返回 `FileAddress.of(page0, 对应偏移)`，供 `Flst` 与 2b 调用。

### 6.4 SegmentInodeLayout（page 2）
3 个 list 头 12B→32B base；fragment 槽顺移：
| 字段 | 新偏移 | 字节 |
| --- | --- | --- |
| USED | 0 | 4 |
| SEGMENT_ID | 4 | 8 |
| PURPOSE | 12 | 4 |
| USED_PAGE_COUNT | 16 | 8 |
| RESERVED_PAGE_COUNT | 24 | 8 |
| FREE_EXTENT_LIST_BASE | 32 | 32 |
| NOT_FULL_EXTENT_LIST_BASE | 64 | 32 |
| FULL_EXTENT_LIST_BASE | 96 | 32 |
| FRAGMENT_SLOTS | 128 | 256（32 槽 × 8）|
- `FRAGMENT_SLOT_COUNT = 32`；`ENTRY_SIZE = 384`；`maxInodesInPage(pageSize) = (bytes - 38) / 384`（16KB → 42）。

### 6.5 SegmentInode + SegmentInodeRepository
- `SegmentInode` 3 个 list 头 `FileAddress`→`FlstBase`：`freeExtentList`、`notFullExtentList`、`fullExtentList`。
- `allocateSlot` 写三个空 base（替代原 NULL FileAddress）。
- `read` 解码三个 base。`freeSlot` 仍整条清零（零态 = used 0 + 三空 base + 槽全 0）。
- **移除**直接 `setFreeExtentListHead(FileAddress)` 等 3 个 setter；新增 base 地址访问器：`FileAddress freeExtentListBaseAddr(SpaceId, inodeSlot)` 等，返回 `FileAddress.of(page2, slotOffset+偏移)`，供 `Flst`/2b 用。

## 7. 数据流、并发与恢复

- 写链：`mtr.getPage(pool, page, X)`（按 §5.1 升序）→ 改 base/node 字段 → MTR commit 标脏、释放。读链：相应字段取 S。
- 跨页链（segment 链：base 在 page2、node 在 page0）：mutator 先 X page0、再 X page2，禁止逆序；与 design §18「space header/XDES(page0) → inode(page2)」一致。
- 恢复：字段级写设计为可重放，留待 redo 切片；本片 no-redo，不声明 crash-safe（§15 推迟满足）。Flst 的 add/remove 不是幂等的「读改写」，与 slice-1 的分配型方法同类——redo 切片接入后再纳入重放框架。

## 8. 异常

- `DatabaseValidationException`：null 参数、`baseAddr`/`nodeAddr` 为 `FileAddress.NULL`。
- `FspMetadataException`：`remove` 时 base.length 已为 0（链账本损坏）；`FlstBase` 解码出 `length==0` 却 first/last 非 NULL（或反之）等不变量破坏。
- buf/mtr 抛出的异常透传。

## 9. 测试（buf `LruBufferPool` + fil `FileChannelPageStore` + mtr `MiniTransactionManager` + @TempDir 真实驱动）

- `FlstNodeTest` / `FlstBaseTest`：编解码往返（含 NULL/空链）、零态解码为空、base 不变量破坏拒绝。
- `FlstTest`（scratch 页驱动，节点放在某数据页的固定槽位上，每槽 24B）：
  - 空链 addFirst / addLast → getFirst/getLast/length。
  - 多节点顺序：连续 addLast 后从 getFirst 沿 getNext 走出插入序；连续 addFirst 走出逆序。
  - 头删 / 尾删 / 中删后链与 length 正确，移除节点 prev/next 归 NULL。
  - 跨页：base 在 page2、node 在 page0，addLast/remove 正确且按 page0→page2 取闩（不死锁、不残留阻塞线程）。
  - remove 空链 → `FspMetadataException`；null/NULL 地址 → `DatabaseValidationException`。
- 迁移回归：`SpaceHeaderRepositoryTest` / `SegmentInodeRepositoryTest` 更新为 `FlstBase` 语义——initialize/allocateSlot→read 三个 base 均为空链；删去原单 FileAddress setter 断言，改为「经 `Flst` 对 base 增删后 read 反映 first/last/length」的协作断言。

## 10. 简化点（注释标注）

- FLST base 用 `long` len（InnoDB 为 4B），FileAddress 12B（InnoDB fil_addr 6B）；功能等价、非二进制兼容。
- 同一条链 node 同页（XDES 均 page0）、单 inode 页 page2；多页 node 锁序留后续。
- 不做 O(n) 成员/双向一致性校验（信任调用方，与 InnoDB 一致）。
- 本片 no-redo、无 pageLSN，不具备 crash-safe；list 改动未来归 `UPDATE_XDES`/`UPDATE_SPACE_HEADER`/`UPDATE_SEGMENT_INODE`。
- `XDES_BASE` 上调到 256，仍只覆盖 page 0 内嵌首批 XDES（不支持独立 XDES 管理页，承袭 slice-1）。

## 11. 后续切片衔接

- 2b：用 `Flst` + repo 的 base 访问器实现 extent 状态迁移与分配/回收（FSP_FREE↔FREE_FRAG↔FULL_FRAG、segment SEG_FREE↔NOT_FULL↔FULL、fragment page、free 回收），并由 `ExtentId` 计算 XDES node 地址。
- 2c：`ExtentAllocationPolicy`、`SegmentPageAllocator`（fragment<32 / extent / 最多 4 extent / autoextend 重试、无条件跳过 extent 0）、`SpaceReservation`、`NoFreeSpaceException`。
- redo 切片：把 Flst add/remove 与 slice-1 非幂等分配纳入 WAL 重放，并写页首 FilePageHeader（checksum/pageLSN）。
