# Slice: B+Tree 悲观 SMO root SX 下降 + restart-in-X（0.13d）

依据：`innodb-btree-design.md` §10.1（root latch S/SX/X）、§10.3（`ROOT_LATCHED_SX`=「可能结构修改」；
「upgrade root latch 只允许短时间转换，失败则 release and restart」）。
前置：0.13d SX latch 基础设施（SX 与 S 并存、排它 SX/X、禁原地 SX→X 升级）+ safe-node 全覆盖（insert/delete/purge）。

## 目标

- 悲观 SMO（split/merge 回退路径）的下降起点由 root **X** 改为 root **SX**（快照树高 ≥2 时）：读者与乐观写者的
  root S 与 SMO 的下降窗口并存、不再被 root 阻塞；其它 SMO 仍在 root 上互斥（SX-SX/SX-X 冲突，串行不变）。
- SMO 可能写 root（growRoot/shrinkRoot/root 收 separator）时以 release-and-restart 兑现 §10.3：SX 禁原地升级
  ⟹ 首遍链顶仍是 root 即在**零页写入**状态整链释放，以 root X 重启第二遍（至多一次）。

## 关键决策

- `descendPathSafeNode` 改两遍制：快照 `rootLevel>=2` → 首遍 `descendOnce(root=SHARED_EXCLUSIVE)`（内部 child/leaf 恒 X、
  safe-node 截断照旧）；链顶 ≠ root（safe 节点吸收）→ 直接返回，本 SMO 全程未 X 过 root；链顶 = root → `releaseChain`
  （下降只读、全链非 touched，干净释放）→ `descendOnce(root=EXCLUSIVE)` 重启。
- **level 0/1 树跳过 SX 首遍直取 X**：此类树任何 SMO 必写 root（leaf 即 root，或 separator/victim 直达 root），
  首遍必重启、纯浪费。快照陈旧只影响模式选择收益、不影响正确性（虚高→多一次重启；虚低→root 取 X 偏保守）。
- 重启后重新导航（chooseChild 全新执行），无需版本校验——重启即全量重定位，天然正确。
- 死锁自由增量论证：root SX/X 起点互斥保持「任一时刻只有一个 SMO 在 root 处」；restart 重取 root X 前已释放全链
  ⟹ 无 hold-and-wait；其余三类等待边（下降阻塞/同父兄弟/FIL 右邻）与 safe-node 切片论证不变。
- 诊断计数：`rootSxDescentCount()`（SX 首遍数）/ `rootXRestartCount()`（重启数；越少说明 safe-node 吸收越充分）。

## 非目标

- 不做原地 SX→X 升级（RRWL 结构不支持，见 SX latch 切片）；不做 B-link 右链 / OLC 版本重启；不改乐观路径与读路径；
  不做 index-level latch（教学简化：root 页 latch 兼任 §10.1 的「root latch」资源）。

## 验收测试

- `pessimisticSmoUsesRootSharedExclusiveFirstPassAndRestartsOnlyWhenReachingRoot`（新增，RED=计数恒 0）：80 宽 key 行
  灌到 level≥2，断言 `rootSxDescentCount()>0`、`rootXRestartCount()>0`（root 收 separator/root split 必重启）、
  `sx > restart`（多数 SMO 停在 safe 节点不重启）+ 全 key 有序无损。
- delete safe-node 用例补增量断言：快照 level 2 的悲观 merge 走 SX 首遍且（B delete-safe）零重启。
- root split（level 2→3）/ 全删 shrink 到 level 0 等既有用例经 restart 路径全数通过。全量回归 866 tests。

## current map 更新要求

- B+Tree 写路径行补「root SX 首遍 + restart-in-X」；SX latch 状态由「未接入 btree」改为「已接入悲观 SMO 下降」。
- Known gaps 保留 B-link/OLC、btree 专用 redo、index-level latch。
