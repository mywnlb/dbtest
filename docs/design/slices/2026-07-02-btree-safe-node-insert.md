# Slice: B+Tree 悲观 insert safe-node 早释放祖先（0.13d）

依据：`innodb-btree-design.md` §10.2（safe-node latch coupling：latch child→判 safe→safe 释放 parent）、
§10.3（`ROOT_LATCHED_*`→`CHILD_LATCHED`→`PARENT_RELEASED`）。
前置：0.13a 写路径乐观 S-crab + `MiniTransaction.releaseLatch` 选择性早释放；0.13d SX latch 基础设施（本片未用）。

## 目标

- 悲观 insert 下降由「全路径 X 持到 commit」改为 **safe-node 早释放**：每 latch 到一个<b>内部</b> child，若它 safe
  （连续空闲 ≥ 该索引任一 node pointer 的编码长度严格上界）则立即释放其以上全部祖先 X latch（含 root），
  保留链收缩为「safe 内部节点 … leaf」。split 不传播到 root 时 root X 提前释放、不再持到 commit，放开 root 处写并发。
- 既有 split 传播引擎（`splitLeafAndPropagate`/`insertSeparator`/`growRoot*`）零改动即在截断后的保留链上正确工作。

## 关键决策

- 新增 `descendPathInsertSafeNode(mtr,index,key,maxSeparatorSize)`；`pessimisticInsert` 改用它（delete/purge 仍走
  `descendPath` 悲观全 X 持到 commit，本片不动）。
- **只判内部页**：leaf 容纳判据是整条 leaf 记录（非 node pointer），且 leaf 恒为保留链末项、永不触发释放。
- safe 判据 = `node.freeSpace() >= maxSeparatorSize`，与 `pointerFits` 同用 `freeSpace()` 度量、上界 ≥ 实际 separator
  ⟹「判 safe ⟹ 传播时 `pointerFits` 必真」；对保留节点持 X 从下降到传播不放手 ⟹ freeSpace 不变。
- `maxSeparatorSize`：node pointer key 列取各类型最大字节（变长列填满 `length`，定长列用实际值即最大）编码取长 + margin。
- **正确性**：保留链顶恒为「最近 safe 内部节点」或真 root；传播遇 safe 顶即吸收返回，绝不越过访问已释放祖先；
  仅当顶恰是 root 且放不下才 growRoot（root 未释放）。每保留节点至多收 1 个 separator。
- **死锁自由（insert-only）**：insert 下降起点短持 root X（遇 safe 即放）⟹ 任一时刻仅一个 insert「在 root 处」；
  leaf split 只触右邻兄弟（`nextPageNo`，单向全序）⟹ 并发 splitter 无环。读者/乐观 S-crab 正确性靠 hand-over-hand，
  **不依赖** root X 持到 commit（本片同步修正相关 Javadoc）。

## 非目标

- 不做 delete/purge 的 safe-node（需 `considerMerge` 的 `parentIsRoot` 按 rootPageId 判定重构 + merge 兄弟 latch 定序）。
- 不做 SX 下降 + restart-in-X、B-link 右链、OLC 版本重启（0.13d 后续阶段）。
- 不改 redo/undo/页格式、current-read 授锁后重定位协议、SQL/executor 接线。

## 验收测试

- `pessimisticSplitReleasesRootLatchEarlyWhenSplitDoesNotReachRoot`（新增）：payloadKey 建 ≥2 层树后，
  `safeNodeAncestorReleaseCount()>0`（RED：safe-node 前恒 0）且全 key 有序无损；`pessimisticInsertFallbackCount()>0`。
- 既有并发 `concurrentInsertsAcrossLeavesStayCorrect` / `concurrentScansStayConsistentWhileInsertsSplitLeaves` 不倒退
  （验证 safe-node 下并发 insert/scan 仍无丢无损）。
- 全量回归不倒退（864 tests）。

## current map 更新要求

- B+Tree 写路径小节：悲观 insert 标注 safe-node 早释放（root X 不再持到 commit；delete/purge 仍全 X 到 commit）。
- Known gaps 保留 delete/purge safe-node、SX 下降+restart、B-link/OLC、btree 专用 redo。
- 完成后按源码调用链复核，只把 insert-only safe-node 标为已接，不把 delete safe-node / restart 协议写成已实现。
