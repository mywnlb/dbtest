# Slice: B+Tree parent split — 解锁树高 >1（0.11）

依据：`innodb-btree-design.md` §8.1/§8.2（Insert/Split：递归 split、separator propagation、root split 复制旧 root 到新 child）、
§9.1（leaf/non-leaf segment 分配）；`current-implementation-map.md` B+Tree 缺口「parent split（树高>1）缺：`splitLevelOneLeaf`
父满抛 `BTreeParentSplitRequiredException`、`rootLevel>1` 拒绝」。是 0.12 merge/redistribute 的前置。

## 目标

把 insert split 从「leaf split + root-leaf split（level 0→1）」推广为 **自底向上 split 传播 + 内部页 split + 原地 root split
（任意层 +1）**，并把所有现有操作的导航推广到 N 层。不变量：**任意高度的树对 insert / lookup / scan / delete / replace /
deleteMark / purge 全部正确**；root 页号跨 split 稳定。

## 关键决策

- **统一 split 引擎**：insert X 下降记录 `path`(root→leaf handles)；溢出→`splitNode(path, depth, 合并排序条目, isLeaf)`：
  非 root = 节点改写左半 + 分配新右兄弟 + separator(`lowKey(右)`,新Id)插父（父满则对父递归 splitNode）；root = 原地两新子 +
  root **页号不变** 重建为层 +1 两 pointer。leaf 行与内部 pointer **统一**：列表对半切、separator=`lowKey(右)`、右半保留全部
  （min-key-pointer 约定，同现有 leaf split）；父页指向「变成左半」的旧节点的旧 pointer 仍有效（左半保留最小 key）。重构现有
  `splitRootLeaf`/`splitLevelOneLeaf` 进此引擎。
- **导航统一**：`findLeaf(mtr,index,key,mode)` 逐层 `chooseChild`(node-pointer schema 与层级无关)到 level 0；读 S 写 X；
  替换 lookup/scan/delete/replace/deleteMark/purge 中每处 `rootLevel==0/1 … else throw`。scan = `findLeaf(lowerKey)` + 现有 sibling 链顺扫。
- **页分配**：leaf→`leafSegment`，内部/root-split 子页→`nonLeafSegment`(新增 `requireNonLeafSegment`)。核对 engine `createClusteredIndex` 提供 nonLeafSegment，缺则补接线。
- **latch**：悲观全路径（写 X root→leaf、读 S），MTR 持有到 commit；**不做** latch-coupling/乐观下降（0.13），注释标注简化点。
- **redo 不变**：物理 `PAGE_INIT`/`PAGE_BYTES` + commit 盖 pageLSN（btree 专用逻辑 redo 留 0.13/0.19）。
- **结果契约**：root split 返回 `indexAfterInsert=index.withRootLevel(L+1)`、`splitOccurred=true`、`allocatedPages`；非 root split 树高不变。
- 删 `BTreeParentSplitRequiredException` 与所有 `rootLevel > 1` 拒绝（由 parent/root split 取代）。

## 非目标（明确推迟）

merge / redistribute / root shrink / 空页回收（0.12，delete 只就地删、可能留半满页）；latch coupling / 乐观→悲观下降（0.13）；
btree 专用逻辑 redo（0.13/0.19）；改聚簇 PK（仍抛 `BTreeUnsupportedStructureException`）；二级索引、MVCC 逻辑唯一/current-read（2.7/2.8）。

## 验收测试

- `rootSplitGrowsTreeToLevelTwo`（**核心**）：小页 size 灌入足够行 → root leaf split 到 level-1 → 继续灌满 root pointer → 触发
  **原地 root split** → `indexAfterInsert.rootLevel()==2`；插入的全部 key 经 `lookup` 可查。
- `internalNonRootSplitPropagatesToLevelThree`：灌到 level-3，存在非 root 内部页 split；结构完整、全 key 可 lookup（验证传播跨多层）。
- `multiLevelScanReturnsAllInOrder`：height≥2 的有界/全量 `scan` 返回全部记录且有序。
- `multiLevelDeleteReplaceMarkPurgeWork`：多层树上 `deleteClustered`/`replaceClustered`/`setClusteredDeleteMark`/`purgeDeleteMarkedClustered`
  正确，所有权(DB_TRX_ID/DB_ROLL_PTR)校验语义不变。
- 回归：现有 height-0/1 的 btree 全套绿；duplicate（unique）、聚簇隐藏列保留、MVCC 读语义不变。

## current map 更新要求

- B+Tree 缺口行：「parent split（树高>1）缺 / `rootLevel>1` 拒绝」→ 改为已落（任意高度 insert split 传播 + 原地 root split + N 层导航），剩余 merge/shrink（0.12）、latch coupling（0.13）。
- `SplitCapableBTreeIndexService` Package Status：范围从「level-1」改为「任意高度：自底向上 split 传播 + 原地 root split + N 层 `findLeaf` 导航」。
- 删 `BTreeParentSplitRequiredException`（连同其 Reserved/Unwired 表项，如有）。

---

## 实现计划（TDD，inline 执行；本项目不另存 plan 文件，应用户要求附此）

> 执行用 superpowers:executing-plans / TDD：每步 RED→GREEN→commit。全部改动集中在
> `src/main/java/cn/zhangyis/db/storage/btree/SplitCapableBTreeIndexService.java` 与
> `src/test/java/cn/zhangyis/db/storage/btree/SplitCapableBTreeIndexServiceTest.java`（复用其 `BTreeContext` harness）。
> 测试：`$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "cn.zhangyis.db.storage.btree.*"`。
> 关键测试杠杆：`payloadKey()`(5000B key) 使 node pointer ~5KB → level-1 root ~3 pointer；~12 行到 level 2、~25-30 行到 level 3。

### Task 1 — N 层导航 `findLeaf`（行为保持重构，解读侧 rootLevel>1）

**Files:** Modify `SplitCapableBTreeIndexService.java`。
- [ ] 抽 `private LeafLocation findLeaf(MiniTransaction mtr, BTreeIndex index, SearchKey key, PageLatchMode mode)`：
  打开 root 校验 header；`while (page.level()>0) { childId=chooseChild(page,index,key); page=openIndexPage/Handle(childId,mode); validate child header.indexId }`；
  到 level 0 返回 `(leafHandle, leafId)`。读用 SHARED、写用 EXCLUSIVE。
- [ ] 用 `findLeaf` 重写 `doLookup`/`scan`(起始 leaf)/`deleteClustered`/`purgeDeleteMarkedClustered`/`replaceClustered`/
  `setClusteredDeleteMark`：删掉各自的 `if rootLevel==0 … else if==1 … else throw` 三分支，仅保留「定位 leaf → 原 in-leaf 逻辑」。
- [ ] `openRoot` 去掉 `rootLevel>1` 拒绝；保留 indexId/level 一致性校验。
- [ ] 运行 `btree.*` 全绿（现有 height-0/1 测试即回归保证；本任务无新行为，纯导航推广）。Commit：`refactor(btree): findLeaf N 层导航，解读侧 rootLevel>1`。

### Task 2 — split 传播引擎（parent split + 原地 root split）

**Files:** Modify service + test。
- [ ] **RED** 新增 `rootSplitGrowsTreeToLevelTwo`（复用 `BTreeContext`，`payloadKey()`+`payloadKeyRow`）：循环 insert id=1..14，
  `current=result.indexAfterInsert()`；断言某次后 `current.rootLevel()==2`、`current.rootPageId()==ctx.rootPageId`，且 `assertFound(1..14)` 全中。
  现状抛 `BTreeParentSplitRequiredException` → 失败。
- [ ] **GREEN** 在 insert overflow 路径引入统一引擎，替换 `splitLevelOneLeaf`/`ensureRootHasRoomForPointer`：
  - insert 先 `descendWithPath`(X) 得 `List<IndexPageHandle> path`(root→leaf)；leaf 满 → `materializeLeafRecords+inserted` 排序得 `rows`，调 `splitNode(path, path.size()-1, rows, /*isLeaf*/true, inserted)`。
  - `splitNode(path, depth, entries, isLeaf, inserted)`：`SplitRows s=splitRows(entries)`；
    **非 root**(depth>0)：改写 `path[depth]`=左半（leaf 用 `insertAll` 保 `insertedRef`，内部用 `writePointers`），分配右兄弟(`leaf?leafSegment:nonLeafSegment`)写右半，leaf 维护 sibling 链（沿用 `splitLevelOneLeaf` 链接逻辑），`separator=new BTreeNodePointer(lowKey(s.right(),index 或 pointer 首 key), newSiblingId)` → `insertPointerIntoParent(path, depth-1, separator)`。
    **root**(depth==0)：分配两新子(leftId/rightId，按 isLeaf 选 segment)写左右半 + leaf sibling 链，`root.format(indexId, oldLevel+1)`，`writeSiblingLinks(NULL,NULL)`，`after=index.withRootLevel(oldLevel+1)`，`insertRootPointer(root, after, (lowKey(left),leftId))`+`(lowKey(right),rightId)`；返回带 `after/splitOccurred/allocatedPages` 的结果。
  - `insertPointerIntoParent(path, depth, pointer)`：`parent=path[depth]`；`required=encode(pointer)+8`；
    `if required<=parent.freeSpace()` → `insertRootPointer(parent, indexAtThatLevel, pointer)`；
    else → `materializePointers(parent)+pointer` 排序 → `splitNode(path, depth, combinedPointers, /*isLeaf*/false, null)`。
  - 新私有：`materializePointers(RecordPage non-leaf)`→`List<BTreeNodePointer>`(沿 `recordOffsetsInOrder` + `BTreeNodePointerSchema`/codec)；
    `writePointers(RecordPage, PageId, List<BTreeNodePointer>, BTreeIndex)`(format 已由调用方做 → 逐个 `insertRootPointer`)；
    `lowKeyOfPointers(List<BTreeNodePointer>)`→首 pointer 的 key。
  - 删 `ensureRootHasRoomForPointer` 与其 `BTreeParentSplitRequiredException` 抛出；`splitRootLeaf` 退化为 `splitNode(path={root},0,rows,true,inserted)` 的调用（或直接复用其逻辑）。
  - node pointer 的 separator key：内部 split 的 `lowKey(右)` = 右半首 pointer 的 `key()`（min-key-pointer 约定，右半全保留）。
- [ ] **GREEN** 运行：`rootSplitGrowsTreeToLevelTwo` + 现有 split 测试（`rootLeafOverflowSplits…`/`levelOneInsertSplits…`/`rootSplitLinksChildLeaves…`/`redoReplayRestoresSplitRoot…`）全绿。Commit：`feat(btree): 自底向上 split 传播 + 原地 root split（解锁树高2）`。

### Task 3 — 多层硬化（level 3 / scan / delete·replace·mark·purge）

**Files:** Modify test（+按需修 service bug）。
- [ ] 改写 obsolete `parentOverflowFailsBeforeLeafRewrite` → `parentOverflowGrowsToLevelTwoOrThree`：insert id=1..14（payloadKey），
  断言不再抛、`rootLevel()>=2`、scan 回 `inserted` 有序。（移除对 `BTreeParentSplitRequiredException` 的 import/引用。）
- [ ] 新增 `internalNonRootSplitPropagatesToLevelThree`：insert id=1..30（payloadKey），断言出现 `rootLevel()==3`、`assertFound` 抽查多个 key 全中。
- [ ] 新增 `multiLevelScanReturnsAllInOrder`：在 level≥2 树上 `scan(kPayload(min),true,kPayload(max),true,100)` 返回全部 id 升序。
- [ ] 新增 `multiLevelDeleteReplaceMarkPurgeWork`：用聚簇 idKey + `insertClustered`(txnId,rollPtr) 建 level≥2 树（wideRow），
  对若干 key 走 `deleteClustered`/`replaceClustered`/`setClusteredDeleteMark`(+`lookupIncludingDeleted`)/`purgeDeleteMarkedClustered`，断言所有权校验、删除/替换结果与 lookup 一致。
- [ ] 全绿（若暴露传播/导航 bug，systematic-debugging 修后回归）。Commit：`test(btree): 多层 scan/delete/replace/mark/purge + level3 硬化`。

### Task 4 — 清理 + 文档 + engine 接线核对

**Files:** delete `BTreeParentSplitRequiredException.java`；Modify current-map/backlog；核对 `StorageEngine.createClusteredIndex`。
- [ ] `Grep BTreeParentSplitRequiredException` 确认无引用后 `git rm` 该类。
- [ ] 核对 `StorageEngine.createClusteredIndex`（或等价 engine 接线）是否给 `BTreeIndex` 提供 `nonLeafSegment`；缺则补 `createSegment(INDEX_NON_LEAF)` 并填入。
- [ ] 更新 `current-implementation-map.md`（B+Tree 缺口行 + SplitCapable Package Status + 删 exception 表项）与 `storage-backlog.md`（0.11 标 ✅）。
- [ ] **全量** `gradle test` 0 失败、测试数不倒退。Commit：`chore(btree): 删 ParentSplitRequired + current map/backlog 0.11 ✅`。

### 自检（写计划后复核）
- 覆盖：spec 的 insert 传播 / 内部 split / 原地 root split / N 层导航(读+写) / 6 个现有操作 / nonLeafSegment 分配 / 删 exception 均有 Task。✓
- 一致命名：`findLeaf`/`descendWithPath`/`splitNode`/`insertPointerIntoParent`/`materializePointers`/`writePointers` 跨 Task 一致。✓
- 风险点：MTR 持全路径 X latch + split 新页，pool 容量 128 足够（harness 已用）；深树单 MTR latch 数随高度线性，验收 ≤level3 受控。
