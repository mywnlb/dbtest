# Slice: B+Tree 写路径 latch coupling 收尾（其余写算子，0.13b）

依据 `innodb-btree-design.md` §10.2（latch coupling）；承接 0.13a（`docs/design/slices/2026-06-30-btree-write-latch-coupling.md`）。
0.13a 已让 `insert`/`deleteClustered` 走乐观 `descendOptimistic` S-crab（内部 S、leaf X）+ unsafe 回退悲观全 X。
本片把**其余全部聚簇写算子**（`replaceClustered`/`setClusteredDeleteMark`/`purgeDeleteMarkedClustered`）接上同一模式，
使写路径 latch coupling 全覆盖。复用 0.13a 的 `descendOptimistic` 与 `deleteWouldUnderflow`，不新增下降/预判原语。

## 目标

三个算子重构为「乐观优先，root 即 leaf 或 unsafe 回退悲观」，分两类：

- **`replaceClustered`（恒 safe，无 unsafe 回退）**：`tryOptimisticReplace` → `descendOptimistic` → leaf X → `replaceInLeaf`
  （`findEqual`→所有权校验→`updater.update`）。`updater.update` 是纯 leaf-only（原地/页内搬迁），**从不 split/merge**。
  `descendOptimistic` 返回 null（root 即 leaf）→ 悲观 `findLeaf(X)` → `replaceInLeaf`。异常与路径无关、直接上抛：
  改 PK→`BTreeUnsupportedStructureException`、搬迁页满→`RecordPageOverflowException`（updater 已核实 overflow 前零改动，
  leaf 未改；replace 本就不 split，悲观也抛，无需回退）。
- **`setClusteredDeleteMark`（恒 safe，无 unsafe 回退）**：`tryOptimisticMark` → `descendOptimistic` → leaf X → `markInLeaf`
  （plan-then-execute：`findEqual` 含已标记→所有权校验→翻转合法校验→`setDeleted`+`writeHiddenColumns`）。**等长纯写**、
  无 size 变化/无 overflow/无结构变更。null → 悲观。非法翻转抛（与路径无关）。
- **`purgeDeleteMarkedClustered`（有 safe/unsafe，同 0.13a delete）**：`tryOptimisticPurge` → `descendOptimistic` → leaf X →
  `findEqual` 严格校验（未命中/未标记/隐藏列不符=stale no-op，safe，纯读）→ **复用 `deleteWouldUnderflow`**：不欠载则
  `purger.purge` **跳过 `reclaimAfterRemoval`**（仅 leaf 持 X）；欠载=unsafe **写页前**释放 leaf X → 悲观 `descendPath`+`purgeInLeaf`（带 merge）。
- 诊断计数镜像 0.13a：`optimisticReplaceHits`/`optimisticMarkHits`/`optimisticPurgeHits` + `pessimisticPurgeFallbacks`（LongAdder + 包内 getter）。

## 关键决策

- **replace/mark 恒 safe**：二者永不结构变更（replace 页内搬迁/原地、mark 等长），故乐观路径无「unsafe 回退」分支——
  只有 root 即 leaf 时交悲观（单页无 crab 收益）。overflow/REQUIRES_REINSERT/非法翻转都是与路径无关的异常，leaf 未改即上抛。
- **purge = delete 同构**：purge 唯一区别是不主动 `deleteMark`（严格校验未标记即 stale no-op）；欠载判定/回退/跳过 merge 与 0.13a delete 一字不差，直接复用 `deleteWouldUnderflow`。
- **并发正确性不变**（同 0.13a）：唯一结构变更（purge 的 merge）只走悲观全路径 X（持 root X 到 commit → tree-wide 串行于 root）；replace/mark 永不结构变更；crab hand-over-hand 保证被放掉的祖先即使并发被改，本线程已先 latch 到仍有效子页。
- **不改** `descendOptimistic`/`deleteWouldUnderflow`/`replaceInLeaf`/`markInLeaf`/`purgeInLeaf`/merge 引擎（只在算子入口套乐观外壳 + purge safe 分支跳过 reclaim）。

## 非目标（推迟）

- 读路径 crab（`lookup`/`scan` 仍全路径 S）→ 0.13c。
- SX latch（§10.1 乐观下降优化）、root/version retry（放松 root-X tree-wide 串行）→ 0.13d。
- btree 专用逻辑 redo（0.19）；二级索引/MVCC 逻辑唯一。

## 验收测试

- `BTreeReplaceClusteredTest`：多层树乐观 replace 命中（行整替换、树高不变、`optimisticReplaceHitCount>0`）；miss/所有权不符=no-op；改 PK 抛 `BTreeUnsupportedStructureException`。
- `BTreeDeleteMarkTest`：多层树乐观 delete-mark 翻转（`lookup` 隐藏 / `lookupIncludingDeleted` 可见、`optimisticMarkHitCount>0`）；幂等（所有权不符=changed false）。
- `BTreePurgeClusteredTest`：多层树乐观 purge safe（不欠载跳 merge、`freedPages` 空、`optimisticPurgeHitCount>0`）；unsafe（欠载回退悲观 merge、`freedPages` 非空、`pessimisticPurgeFallbackCount>0`）；stale（未标记/隐藏列不符）=no-op。
- 回归：`multiLevelClusteredDeleteReplaceMarkPurge` + 全部既有 btree/mtr 测试绿；全量 `gradle test` 不倒退。

## 文档更新要求

- `current-implementation-map.md`：Clustered replace/delete-mark/purge 三条 chain 行补「乐观 `descendOptimistic`（内部 S、leaf X）+ root 即 leaf/（purge）欠载回退悲观」；Package Status split-capable 行「写路径 latch coupling」句由「insert/deleteClustered」扩为「全部聚簇写算子」；修正 Known Gaps 那条 gap（写路径 latch coupling 全覆盖，剩读路径 crab + SX + 版本重定位 = 0.13c/0.13d）。
- `storage-backlog.md`：0.13b 标其余写算子已落，剩 0.13c 读路径 crab、0.13d SX/版本重定位。
- 代码注释：replace/mark 恒 safe（无结构变更故无回退）、purge 与 delete 同构复用 underflow 预判、并发不变量沿用 0.13a。
