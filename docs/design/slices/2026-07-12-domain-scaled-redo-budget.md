# Domain-scaled Redo Budget Slice

## 目标

- 用 B+Tree height、DML 首写状态、rollback undo 类型和 undo segment drop plan 替换动态写路径的固定 page-image profile。
- 所有 workload 仍在 `MiniTransactionManager.begin(budget)` 前物化，容量等待继续早于 page latch/FSP lease。
- 保留固定布局操作的固定 profile，以及 commit 的 actual≤budget 致命校验。

## 通用 workload

- `RedoBudgetWorkload` 是不可变值：pageImageEquivalents + extraLogicalBytes，可 checked `plus`。
- redo/mtr 层只把 workload 与实例 page size 转为 `RedoAppendBudget`，不 import BTree/Undo/DML 类型。
- `budgetFor(purpose, workload)` 服务动态操作；无 workload 的 `budgetFor(purpose)` 只允许固定布局 purpose。
- 动态 purpose 若误走固定入口，在取得任何页资源前抛领域校验异常。

## BTree / DML 估算

- BTree estimator 只读取 `BTreeIndex.rootLevel`，height=`rootLevel+1`，不打开任何页。
- point rewrite 使用固定小 workload；insert/split 与 physical delete/merge 按 height 线性增长。
- INSERT inverse 与 purge 使用 structural-delete workload；UPDATE/DELETE_MARK inverse 使用 point rewrite。
- DML estimator 组合 BTree workload 与 undo append workload。
- 事务无 `UndoContext` 时按首写建段/claim/FSP 元数据上界；已有 context 时按单页 append/grow 上界。
- update/delete 已在 current-read 后得到旧记录，但预算不解析 record bytes，payload 由 page-image 等价量覆盖。

## Undo finalization plan

- 新增 `UndoSegmentDropPlan(fragmentPageCount, extentCount, usedPageCount)` 值对象。
- `UndoSpaceAllocator.inspectDropPlan(readMtr, handle)` 是只读端口；生产适配器委托 DiskSpaceManager。
- DiskSpaceManager 从 inode page2 读取 segment identity、32 个 fragment 槽和三条 extent list length。
- identity 不匹配、计数溢出或 fragment>used 均在写 MTR 前 fail-closed。
- finalizer 的既有 page3/undo first-page 预检提交后，再用独立只读 MTR取得 drop plan，避免 page3→page2 逆序。
- finalization workload = 固定 page0/page2/page3/terminal 开销 + fragment/extent 数的线性上界。
- finalization lease 横跨两个只读 plan 和最终写 MTR；内存 slot lock 不跨 IO，既有 FINALIZING 语义不变。

## 公式与安全余量

- BTree insert：基础 + 每树层 split/parent/root 最坏触页；首写 undo 另加建段余量。
- BTree structural delete：基础 + 每树层 merge/redistribute/parent/root shrink 最坏触页。
- Undo drop：固定元页余量 + `2*fragmentPageCount + 4*extentCount` page-image equivalents。
- 公式是安全上界，不承诺等于 actual；实际尺寸仍由 commit 精确结算验证。
- 深树/大 segment 的预算可以高于旧固定 profile，使配置不足在物理修改前失败，而非 commit 才发现低估。

## 非目标

- 不预测具体 split point、merge sibling、PageDirectory 槽数或 redo 物理 ring bin-packing。
- 不改变 B+Tree/Undo 页格式、segment drop 算法、MTR content-undo、LSN/WAL/checkpoint 语义。
- 不把 capacity throttle、flush/checkpoint 或 redo repository 依赖注入 BTree/Undo estimator。

## 验收

- workload checked arithmetic；同 purpose 随 height/first-write/fragment/extent 单调增长。
- 动态 purpose 禁止固定入口；固定 purpose 保持现有行为。
- DML normal/split、purge/rollback 三类 undo、finalization 空/fragment/extent plan 均 actual≤budget。
- drop plan 在写 MTR 前读取，且不持 page3/undo latch；identity/计数损坏 fail-closed。
- 更新 current map/backlog；五遍源码复核；固定 JDK/Gradle 全量回归通过。
