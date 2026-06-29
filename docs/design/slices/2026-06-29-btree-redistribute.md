# Slice: B+Tree redistribute（借/再平衡，0.12b）

依据 `innodb-btree-design.md` §8.3「优先 redistribute、无法时 merge」；是 0.12 的补：merge fit 不下时不再
留欠载/退化页，改 **对半再平衡** 相邻同父对。前置 0.12（merge + root shrink）已落。

## 目标

`considerMerge` 的 `!mergeFits` 分支：欠载页与同父相邻兄弟合计 > 一页时，**对半 rebalance** 两页使双方都脱离欠载；
只更新 parent 中 **right 成员** 的 lowKey，**不删页、不传播、不改树高**。leaf + internal 统一。不变量：redistribute 后
两页都不欠载、剩余 key 全部有序可查、所有权语义不变；deleteAll 仍能塌缩到 level 0。

## 关键决策

- **redistribute = merge 的 fallback**：`mergeFits=false` ⟺ 两页合计 > 一页 ⟺ 可分成两个 ≥半满页 → 与经典
  B+Tree「兄弟满则 borrow、兄弟空则 merge」等价（用「能否合并成一页」区分，省去记录数阈值）。
- **借法 = 对半**（复用 `splitRows`/`splitPointers`）：合并相邻对全部条目 → 对半 → 两页各 `format` 后重灌；
  两页都健康，减少未来再欠载。平分安全：欠载页 < 0.5 页 ⟹ total < 1.5 页 ⟹ 每半 < 0.75 页 < 一页。
- **只更新 right pointer 的 lowKey**：min-key-pointer 约定下 left 的 lowKey = 左半首条 = 原 left 首条（不变），
  仅 right 的最小 key 变 → `removePointerFromParent(rightId)` + `insertPointer(newRightLowKey→rightId)`（parent pointer 数不变）。
- **FIL 链不动**（不删页；`format` 只碰 `[38,size-8)` 不碰信封区）；**不传播、不 free**（`freedPages` 空、`rootLevel` 不变）。
- **leaf + internal 对称**：internal 移 node pointer、更新其在 parent（= 上一层）中 right 成员的 lowKey，同一 `considerMerge` 递归层复用。
- 防御：合并后条目 < 2 不应发生（进入条件保证 total > 1 页），万一则退回留欠载（不抛）。

## 非目标（推迟）

latch coupling / 乐观下降（0.13）；btree 专用逻辑 redo（0.13/0.19）；按记录数精确阈值的 borrow；二级索引/MVCC（2.7/2.8）。

## 验收测试（`BTreeDeleteClusteredTest`，wideKey harness）

- `underfullLeafBorrowsFromFullSiblingViaRedistribute`（改自 0.12 `mergeSkippedWhenCombinedDoesNotFit`）：
  1..5 → `[1,2],[3,4,5]`；del 1 → `[2]` 欠载、右满 fit 不下 → redistribute 平分 → 两 leaf 各 2 条（**白盒读 nRecs** 验证
  从 1+3 变 2+2），`freedPages` 空、`rootLevel` 1、`scan=[2,3,4,5]` 有序。
- `internalRedistributeKeepsSubtreesBalanced`：level-2 树定向删 → 某 level-1 内部页与满兄弟 redistribute → **白盒断言
  root 的各 level-1 子页 pointer 数无 1-pointer 退化页**、剩余 key 全可查、`rootLevel` 不塌到 0。
- 回归：`deleteAllRowsShrinksTreeToLevelZero` 仍 → level0（空页仍走 merge 塌缩）；全量绿（725 +N）。

## current map / backlog 更新

- delete/purge「Underflow reclaim」链：`!mergeFits` 由「留欠载」改「**redistribute 对半 + 更新 right pointer lowKey（不删页/不传播）**」；
- split-capable Package Status 补 redistribute；redistribute 后无 1-pointer 退化内部页。
- `storage-backlog.md` 0.12b 标 ✅。

---

## 实现计划（TDD，inline）

> 改动集中 `SplitCapableBTreeIndexService.java` + `BTreeDeleteClusteredTest.java`。
> 测试：`$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "cn.zhangyis.db.storage.btree.*"`。

### Task A — redistribute 引擎 + leaf 测试
- [ ] **RED** 改写 `mergeSkippedWhenCombinedDoesNotFit` → `underfullLeafBorrowsFromFullSiblingViaRedistribute`（加白盒 nRecs 平衡断言 + root pointers helper）。
- [ ] **GREEN** `considerMerge` 的 `!mergeFits` 分支调 `redistribute(mtr,index,pair,leaf,parent,parentId)`：合并相邻对 → `splitRows`/`splitPointers` 对半 → `left.format` 重灌左半 + `right.format` 重灌右半 → `removePointerFromParent(rightId)` + `insertPointer(newRightLowKey→rightId)` → 返回 index。
- [ ] Commit：`feat(btree): redistribute 对半再平衡（merge fit 不下的 fallback，0.12b）`。

### Task B — internal redistribute 测试 + 硬化
- [ ] 新增 `internalRedistributeKeepsSubtreesBalanced`；确认 `deleteAllRowsShrinksTreeToLevelZero` 仍绿。
- [ ] Commit：`test(btree): internal redistribute + deleteAll 回归`。

### Task C — 文档 + 全量回归
- [ ] 更新 current-map + backlog（见上）+ MEMORY。全量 `gradle test` 不倒退。Commit：`chore(btree): current map/backlog 0.12b redistribute ✅`。
