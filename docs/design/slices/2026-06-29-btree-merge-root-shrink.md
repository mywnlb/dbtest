# Slice: B+Tree merge + root shrink（0.12，no redistribute）

依据：`innodb-btree-design.md` §8.3（Delete/Purge/Merge：低水位→优先 redistribute→merge→更新 parent→free page）、
§5.6（PageMergePlan）；backlog 0.12「redistribute / merge / root shrink」。前置 0.11 多层结构已落。
本切片只做 **merge + root shrink（不做 redistribute → 0.12b）**，`deleteClustered` 与 `purgeDeleteMarkedClustered`
物理删除后都触发欠载回收。

## 目标

聚簇删除/purge 物理摘除记录后，叶/内部页低于填充阈值时 **merge 相邻同父兄弟 + 回收 victim 页 + 自底向上传播 +
原地 root shrink**。不变量：任意高度树删除后结构完整、剩余 key 全部可 lookup/scan、所有权(DB_TRX_ID/DB_ROLL_PTR)语义不变、
root 页号稳定；删空整棵树后 `rootLevel()==0` 且 root 页可复用。

## 关键决策

- **min-key-pointer 约定 → 统一 merge**：survivor = 相邻对的**左**成员、victim = **右**成员；把 victim 条目并入 survivor、
  从 parent 摘除 **victim 的 node pointer**。survivor 的父 pointer key 不变 → **无 separator 下拉/更新**。叶页额外修 FIL
  prev/next（survivor.next=victim.next；victim.next.prev=survivor，需打开远兄弟 X）。内部页不参与 FIL 链。
- **兄弟选择经 parent 的 pointer 顺序**（保证同父）：victim childId 在 parent 的有序 pointer 中找下标 i；有左兄弟取
  (i-1, i)，否则取 (i, i+1)。survivor 恒为对的左者。
- **欠载阈值（MERGE_THRESHOLD≈50%）**：`page.freeSpace()*2 > pageSize.bytes()`（空页恒欠载）。leaf 与内部页同式。
  `IndexPageAccess` 新增 `pageSize()` 访问器供 service 取阈值。
- **fit 检查**：survivor 须容下全部 victim 条目（`Σ encode+slot ≤ survivor.freeSpace()`）；不容 → no-op 留欠载（rebalance=0.12b）。
- **摘 parent pointer**：复用 `deleter.deleteMark + purger.purge`（node pointer 非系统记录，可标记后摘）。
- **传播 / root shrink**：摘 pointer 后 parent 非 root 且欠载 → 递归 considerMerge(depth-1)；parent 是 root 且剩 1 pointer →
  原地 root shrink（root 吸收唯一 child 内容、`format(level-1)`、free child，级联；child 为 leaf 时 root 退回 level 0）。
- **导航高度权威**：`openRoot` 采用 root 页**实际 level** 导航、只校 `indexId`；删去 `header.level()==index.rootLevel()` 严格断言
  （无测试依赖；并发重定位协议留 0.13/2.7）。`BTreeDeleteResult` 增 `indexAfter`/`freedPages`（对称 `BTreeInsertResult`，
  供观测与 caller 刷新快照，非导航必需，解决 Rollback/Purge 跨批 root-shrink 快照陈旧）。`BTreeRootChangedException` 改为
  reserved（不再由 level guard 抛出）。
- **latch / redo 不变**：悲观全路径 X + sibling/远兄弟 X，MTR 持至 commit（无 latch coupling=0.13）；物理 PAGE_BYTES/PAGE_INIT redo。

## 非目标（明确推迟）

redistribute / borrow 再平衡（0.12b，fit 不下即留欠载/留 1-pointer 退化内部页）；latch coupling / 乐观下降（0.13）；
btree 专用逻辑 redo（0.13/0.19）；二级索引 merge、MVCC 逻辑唯一/current-read（2.7/2.8）；merge 与 purge→truncate 调度。

## 验收测试（`BTreeDeleteClusteredTest`，wideKey 多层 harness）

- `deleteAllRowsShrinksTreeToLevelZero`（**核心**）：建 level≥2，逐个 delete 全部 key → `rootLevel()==0`、root 页号稳定、
  全 lookup 空、freed pages 非空；之后再 insert 一行仍可查（树可复用）。
- `underfullLeafMergesIntoSiblingAndRemovesParentPointer`：3 leaf 的 level-1 树，删到某 leaf 欠载 → merge 进同父兄弟、
  parent 少一 pointer、FIL 链双向一致、剩余 key 全可查（保持 ≥2 leaf 不整树 shrink）。
- `internalUnderflowPropagatesAndShrinksRoot`：level≥2 树删大半 → 内部页 merge 传播 + root shrink，`rootLevel()` 至少降 1、
  剩余 key 全可查。
- `mergeSkippedWhenCombinedDoesNotFit`：构造欠载页其 merge 目标已满 → no-op（两页都在、rootLevel 不变、相关 key 全可查）。
- 回归：现有 721 测试绿；ownership/幂等/MVCC 读/隐藏列语义不变。

## current map 更新要求

- B+Tree slice：delete/purge 数据链补「物理删除后 underflow→merge(同父兄弟)→摘 parent pointer→free page→传播→原地 root shrink」；
  0.11 行「剩余 merge/redistribute/root shrink → 0.12」改为「merge+root shrink 已落（0.12），redistribute 留 0.12b」。
- `SplitCapableBTreeIndexService` Package Status：范围补「merge/root shrink（任意高度、无 redistribute）」。
- `BTreeDeleteResult` 由 `(removed)` 扩为 `(removed, indexAfter, freedPages)`；`BTreeRootChangedException` 标 reserved（level guard 不再抛）。
- `storage-backlog.md` 0.12 标 partial：merge+shrink ✅、redistribute=新增 0.12b。

---

## 实现计划（TDD，inline；本项目不另存 plan 文件，应用户要求附此）

> 改动集中在 `SplitCapableBTreeIndexService.java`、`BTreeDeleteResult.java`、`IndexPageAccess.java`（加 `pageSize()`）、
> `BTreeDeleteClusteredTest.java`。测试：`$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "cn.zhangyis.db.storage.btree.*"`。

### Task A — 导航高度权威 + 结果 API（行为保持重构）
- [ ] `IndexPageAccess` 加 `public PageSize pageSize()`。
- [ ] `openRoot` 采用实际 level、删 `BTreeRootChangedException` level 断言（保留 indexId 校验）。
- [ ] `BTreeDeleteResult` → `(boolean removed, BTreeIndex indexAfter, List<PageId> freedPages)`；加便捷工厂；
  `deleteInLeaf`/`purgeInLeaf`/no-op 站点回填 `index` 与空 freed；callers（Rollback/Purge）忽略新字段即可。
- [ ] `btree.*` 全绿（纯重构 + 字段扩展）。Commit：`refactor(btree): 导航采用实际 root level + BTreeDeleteResult 增 indexAfter/freedPages`。

### Task B — merge 引擎（leaf + 内部页，无 root shrink/传播）
- [ ] **RED** `underfullLeafMergesIntoSiblingAndRemovesParentPointer`（3 leaf level-1）。
- [ ] **GREEN** `considerMerge(mtr,index,path,depth,freed)`：`isUnderfull`(freeSpace*2>pageSize) → `chooseMergePair`(parent pointer 顺序，survivor=左/victim=右) → `mergeFits` → 叶 `mergeLeaf`/内部 `mergeInternal`（移条目 + 叶修 FIL + `removePointerFromParent`(deleteMark+purge) + `disk.freePage(victim)`）。`deleteInLeaf`/`purgeInLeaf` 改用 `descendPath` 拿 path，removal 成功后调 considerMerge。
- [ ] Commit：`feat(btree): leaf/内部页 merge（同父兄弟 + 摘 parent pointer + free page）`。

### Task C — root shrink + 传播
- [ ] **RED** `deleteAllRowsShrinksTreeToLevelZero` + `internalUnderflowPropagatesAndShrinksRoot` + `mergeSkippedWhenCombinedDoesNotFit`。
- [ ] **GREEN** 摘 pointer 后：parent 非 root 欠载→递归 considerMerge(depth-1)；parent 是 root 且 1 pointer→`shrinkRoot`（吸收 child、`root.format(level-1)`、free child、级联；child=leaf→level0 + writeSiblingLinks(NULL,NULL)）。返回 `indexAfter`(withRootLevel)。
- [ ] Commit：`feat(btree): 自底向上 merge 传播 + 原地 root shrink（树高收缩）`。

### Task D — 文档 + 全量回归
- [ ] 更新 current-map + backlog（见上）。全量 `gradle test` 0 失败、测试数不倒退。Commit：`chore(btree): current map/backlog 0.12 merge+shrink ✅`。
