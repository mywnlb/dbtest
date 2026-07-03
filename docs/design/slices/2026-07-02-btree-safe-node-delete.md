# Slice: B+Tree 悲观 delete/purge safe-node 早释放祖先（0.13d）

依据：`innodb-btree-design.md` §10.2 step4-5（safe page 定义含「删除/merge 时不会低于 merge 阈值」）。
前置：`2026-07-02-btree-safe-node-insert.md`（insert-only safe-node，本片补齐其非目标 ①）。

## 目标

- 悲观 delete/purge（merge 回退路径）由「`descendPath` 全 X 持到 commit」改为 **safe-node 早释放**：X 下降遇到
  「被摘走一个最大 pointer 后仍不欠载」的 safe 内部节点即释放其以上全部祖先 X（含 root），保留链=「safe 节点…leaf」。
  merge 不传播到 root 时 root X 不再持到 commit。至此**全部悲观 SMO 路径（split+merge）都覆盖 safe-node**。

## 关键决策

- 通用化 `descendPathSafeNode(mtr,index,key,Predicate<RecordPage> internalChildSafe,LongAdder counter)`；
  insert/delete 各自薄包装（`insertDescendSafe`=连续空闲≥maxSeparatorSize / `deleteDescendSafe`=
  `(freeSpace+garbage+maxSeparatorSize)*2<=pageSize`，与 `isUnderfull` 同公式、被摘指针取上界偏保守）。
- `maxSeparatorSize(index)` 改为按列类型**合成** worst-case key（变长填满 length、定长任一合法值，13 TypeId 全覆盖）：
  insert（有完整记录）与 delete/purge（只有 SearchKey、可为前缀）共用同一严格上界。
- **`considerMerge` root 判定改页号**：`parentHandle.pageId()==rootPageId`（root 页号跨 split/shrink 稳定）替代链下标
  `depth-1==0`——safe-node 截断后链顶可为非 root，按下标判会误做 `shrinkRoot`。`depth==0` 链顶=「真 root 或 safe 节点」均收工。
- 正确性：safe 链顶摘一指针后不欠载 ⟹ 传播到链顶 `isUnderfull` 必假必停；root shrink 只在链=全路径（无 safe 节点）时可达，
  而存在 safe 节点时 root 不失指针、无需 shrink。safe 节点必有 ≥3 指针（2 指针页 free 恒过半），摘后 ≥2、无退化链顶。
- 死锁自由（insert/delete/purge 并发）：① 下降阻塞边不回头 ② 同父兄弟边——保留链连续 ⟹ 保留节点的父页恒被本 SMO 持有，
  同父兄弟只可能被 leaf-only 乐观算子（终结、不再申请 latch）持有 ③ FIL 右邻边恒向右、全序。论证入 Javadoc。

## 非目标

- redistribute 父页「删旧插新 pointer」的溢出风险为既有继承（全路径 X 时代同样存在），本片不修。
- SX 下降 + restart-in-X、B-link 右链 / OLC 版本重启、btree 专用 redo（0.13d 后续）。

## 验收测试

- `pessimisticMergeReleasesRootLatchEarlyWhenMergeDoesNotReachRoot`（新增）：level-2 树（root 子 [2ptr,3ptr]）删 key10
  使 3ptr 子树 leaf 欠载 → `safeNodeDeleteAncestorReleaseCount()>0` + merge 停在子树内（树高不变）+ 余 key 有序无损。
- 0.12 全部 merge/shrink/redistribute 用例（含全删收缩到 level 0、内部欠载传播 shrink）不倒退——验证页号 root 判定与
  safe-node 不破坏 shrink 可达性。全量回归 865 tests 不倒退。

## current map 更新要求

- Clustered delete/purge 行与 Underflow reclaim 行标注 safe-node 下降 + 页号 root 判定；Insert 行移除「delete/purge 未覆盖」。
- Known gaps 保留 SX 下降+restart、B-link/OLC、btree 专用 redo。
