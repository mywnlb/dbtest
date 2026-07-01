# Slice: B+Tree 写路径 latch coupling（乐观→悲观，insert+delete，0.13a）

依据 `innodb-btree-design.md` §10.1/§10.2/§10.3（latch 类型、latch coupling 规则、safe page 判定、latch 状态机）；
storage-backlog.md 0.13。当前 `SplitCapableBTreeIndexService` 全部写操作走 `descendPath` **悲观全路径 X**——root→leaf 每层
X latch 全部入 MTR memo 持到 commit，并发写彼此串行在 root。本片落地**写路径 latch crabbing + 乐观下降**：
绝大多数不触发 split/merge 的 insert/delete 只在 leaf 持 X，内部层 S-crab 后立即释放，放开 root 处的写并发。

## 目标

- `MtrMemo.release(PageGuard)`：非 LIFO 选择性释放——按**身份**在 deque 中定位该 guard 槽位、`close`（放 latch+fix）、移除；
  未找到即视为不变量破坏抛 `MtrStateException`。既有 LIFO `releaseTo/releaseAll` 不变。
- `MiniTransaction.releaseLatch(PageGuard)`：`ensureActive` → **touched 防护**（该页在 `collector.touchedPages()` 则抛，
  与 `rollbackToSavepoint` 一致：已写页不得提前释放，否则 commit `guardFor` 盖 pageLSN 取不到）→ `memo.release(guard)`。
  乐观 crab 只早释放内部 **S** guard（从不写），天然不 touched；防护仅挡误用。
- `IndexPageAccess.releaseHandle(mtr, IndexPageHandle)` + `IndexPageHandle.guard()`（**包内**可见，仅 `api.index` 见 guard）：
  btree 服务只经句柄早释放，仍不接触裸 guard/frame（维持 §14 封装）。
- `SplitCapableBTreeIndexService.descendOptimistic(mtr,index,key)`：内部层 **S-crab**（持 parent S → latch child →
  释放 parent S），leaf 取 **X**，返回 leaf 句柄。既有 `descendPath`（全路径 X）**不变**，专供悲观重启与其余操作。
- `insert` 重构为 `tryOptimisticInsert` → 失败回退**现有悲观 `insert` 全体**（descendPath 全 X + `splitLeafAndPropagate`）：
  乐观命中 leaf X → `ensureUniqueAbsent`（dup 抛 `BTreeDuplicateKeyException`）→ `inserter.insert`：成功=safe（仅 leaf 变更，
  caller commit）；抛 `RecordPageOverflowException`=unsafe（**leaf 未改**，`RecordPageInserter` 在 heap 分配处、任何页写之前抛）→
  `releaseLatch(leaf)` → 返回 false → 悲观重启。
- `deleteClustered` 重构同构：乐观命中 leaf X → `findEqual`（未命中=幂等 no-op，safe）→ 所有权校验（DB_TRX_ID/DB_ROLL_PTR）→
  **underflow 预判** `wouldUnderflow`（按 `isUnderfull` 同一公式投影 purge 后可回收空闲：`(freeSpace+garbage+recordLen)*2>pageSize`，
  保守）：safe → `deleteMark`+`purge`、**跳过** `reclaimAfterRemoval`（预判已保证不欠载）、commit；unsafe → **写页前** `releaseLatch(leaf)` →
  悲观重启（现有 `deleteInLeaf`+`reclaimAfterRemoval` 带 merge）。

## 关键决策

- **crab 一致性**：任一时刻至多持「1 个内部 parent + 1 个 child」；latch 到 child 后才放 parent，下降链不断裂（§10.2）。
- **safe 判定 = 试错**：insert 靠 `RecordPageInserter` 溢出前零改动（已核实 line 65-73：分配失败在落字节前抛）→ 直接 try；
  delete 靠 underflow 预判在 `deleteMark` 前拦截。二者都保证 **unsafe 分支零页修改**，悲观重启干净、无需 content undo。
- 乐观内部 **S** / leaf **X**；unsafe→悲观全路径 **X**（split/merge 引擎与 parent split/merge/redistribute 一字不改）。
- 早释放走 touched 防护，复用 commit 盖 pageLSN 不变量；乐观内部 S guard 不 touched，与之相容。
- 锁序不变：MTR 内下降始终 root→leaf 单向（crab 不逆向），与悲观、与 buffer instanceLock 无环。

## 非目标（推迟）

- 仅 `insert`+`deleteClustered` 乐观；`replaceClustered`/`setClusteredDeleteMark`/`purgeDeleteMarkedClustered` 仍悲观全 X。
- 读路径（`lookup`/`scan`）不改，仍全路径 S（S-only 无写并发问题，crab 收益小，留后续）。
- 无 SX latch（§10.1 的乐观下降优化，本片只用 S/X）；无 btree 专用逻辑 redo（0.13/0.19）；无版本校验重定位（0.22/2.7）。
- 不改 split/merge/redistribute/root shrink 引擎；不改 leaf-only 服务。

## 验收测试

- `MtrMemoTest`：选择性 `release(guard)` 移中间槽、放对资源、余栈 LIFO 完整；释放不存在 guard 抛。
- `MiniTransactionTest`：`releaseLatch` 放中间层 latch 后余 latch 仍 commit；早释放 touched 页抛（防护）。
- `SplitCapableBTreeIndexServiceTest`/`ClusteredInsertTest`：乐观 insert **safe**（leaf 放得下，仅 leaf 变更、树高不变）、
  乐观 insert **unsafe**（触发 leaf/root split，经悲观重启，结果与今日一致）。
- `BTreeDeleteClusteredTest`：乐观 delete **safe**（不欠载，无 merge、`freedPages` 空）、乐观 delete **unsafe**（欠载→悲观重启走 merge）。
- 正确性：批量 insert/随机 delete 后 `lookup`+`scan` 全部有序命中；unique dup 仍抛。
- 并发：两线程写**不同 leaf**（同 root 分叉）并行，root 不再互相阻塞、结果均可查（对照悲观全 X 版本）。
- 回归：`btree` 全部既有测试（`SplitCapable*`/`ClusteredInsert`/`BTreeDeleteClustered`/`BTreeDeleteMark`/`BTreePurgeClustered`/
  `BTreeReplaceClustered`/`BTreeIndexClustered`/`LeafOnly*`）+ mtr 全部绿；全量 `gradle test` 不倒退。

## 文档更新要求

- `current-implementation-map.md`：btree Package Status 补 `descendOptimistic` + 乐观→悲观写路径；Page fix/锁序小节注明
  「写路径内部 S-crab 早释放、leaf X；unsafe 悲观全 X 重启」。**修正 Known Gaps 中「btree 全路径 X、无 latch coupling（0.13）」那条**：
  insert/deleteClustered 已 latch coupling，剩 replace/mark/purge/读路径 + SX latch + btree 专用 redo。
- `storage-backlog.md`：0.13 拆出 0.13a（insert+delete 乐观）已落，剩 0.13b（其余写算子/读路径/SX/专用 redo）。
- 代码注释：crab 释放点、safe 试错依据（inserter 溢出前零改动 / delete underflow 预判）、unsafe 零页修改重启不变量、touched 防护理由。
