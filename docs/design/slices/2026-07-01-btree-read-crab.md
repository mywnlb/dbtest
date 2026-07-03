# Slice: B+Tree 读路径 latch coupling / crab（0.13c）

依据：`innodb-btree-design.md` §10（latch coupling / crabbing）、§9.4；
`current-implementation-map.md` B+Tree 小节（写路径 crab 0.13a/0.13b 已落，读路径仍全路径 S）。
前置：0.13a/0.13b `descendOptimistic` S-crab + `MiniTransaction.releaseLatch` 选择性早释放已实现。

## 目标

- 把 point read（`lookup`/`lookupIncludingDeleted`/`locatePointForCurrentRead`）与 range scan
  （`scan`/`locateRangeForCurrentRead`/`terminalGapForRange`）的 root→leaf 下降改为 **S-crab**：
  持父页 S → latch 子页 S → 释放父页 S，至多同时持「1 父 + 1 子」，越过的祖先立即释放。
- range scan 的 sibling 链改为 **hand-over-hand**：读出 `next` 后先 latch 后继 leaf，再释放当前 leaf。
- 不再把 root/内部页与全部已扫 leaf 的 S latch 持有到 MTR commit。

## 关键决策

- 新增私有 `descendLeafSharedCrab(mtr,index,key)` 返回 leaf `IndexPageHandle`（S），`findLeafSharedCrab` 包成 `LeafLocation`；
  与写路径 `descendOptimistic` 同构，但**全 S、无 unsafe 回退**（读从不结构变更），root 即 leaf 直接返回 root（S）。
- 只改 SHARED 读下降；写路径 `findLeaf(EXCLUSIVE)`/`descendPath` 全 X 悲观路径**不动**。
- 早释放经 `IndexPageAccess.releaseHandle`→`MiniTransaction.releaseLatch`；读页从不 touched，释放 SHARED guard 恒安全。
- 并发正确性沿用写路径论证：结构变更走悲观全 X 且持 root X 到 commit → 与读者 root S 冲突而串行；
  hand-over-hand 保证「释放祖先/前驱前已 latch 到仍有效的子/后继页」，读者绝不穿过正在 split/merge 的页。
- 语义不变：`lookup`/`scan` 仍返回等价物化结果；`current-read` 定位值对象不变。

## 非目标

- 不做 SX latch、乐观读失败重启、root/version 通用重定位（留 0.13d）。
- 不改 legacy 写路径、redo/undo/page 格式、current-read 授锁后重定位协议。
- 不接 SQL/executor/DML facade。

## 验收测试

- **小 buffer pool 全量 scan**：~20 leaf 多层树 + cap 12 池，`scan(full)` 返回全部有序 key 且**不抛** `BufferPoolExhaustedException`
  （非 crab 会同时 fix root+全部 leaf 触发 exhaustion，是本片 RED）。
- 读并发正确：reader 反复 scan/lookup 与 writer 插入（触发 split）并发，每次 scan 有序/无重/在界内，最终看到全量。
- 多层 lookup 命中正确（crab 下降定位不误页）。
- 全量回归不倒退。

## current map 更新要求

- B+Tree 小节把「读路径（lookup/scan）仍全路径 S」更新为「读路径 S-crab + sibling hand-over-hand（0.13c）」。
- Known gaps 保留 SX latch/通用版本重定位（0.13d）、btree 专用 redo。
- 完成后按源码调用链复核，只把 read-path crab 标为已接，不把 SX/重启协议写成已实现。
