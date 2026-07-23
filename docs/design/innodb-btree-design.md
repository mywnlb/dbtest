# MiniMySQL InnoDB 风格 B+Tree 模块设计

版本：2026-07-23
实现语言：Java  
参考基线：MySQL 8.0.46 InnoDB 官方手册与源码文档  
关联设计：[innodb-storage-engine-overview.md](innodb-storage-engine-overview.md)、[innodb-disk-manager-design.md](innodb-disk-manager-design.md)、[innodb-buffer-pool-design.md](innodb-buffer-pool-design.md)、[innodb-record-design.md](innodb-record-design.md)、[innodb-redo-log-design.md](innodb-redo-log-design.md)、[innodb-transaction-mvcc-design.md](innodb-transaction-mvcc-design.md)、[innodb-undo-log-purge-design.md](innodb-undo-log-purge-design.md)、[innodb-secondary-index-mvcc-purge-design.md](innodb-secondary-index-mvcc-purge-design.md)

## 1. 目标与边界

本设计面向 MiniMySQL 存储引擎的 `storage.btree` 模块。B+Tree 负责聚簇索引和二级索引的跨页导航、root 管理、leaf/non-leaf 结构维护、range scan、split、merge、root split/shrink，以及 record/gap/next-key 锁落点构造。页内记录格式由 Record 模块负责，page 缓存由 Buffer Pool 负责，空间分配由 Disk Manager 负责，事务锁和 MVCC 由 Transaction 模块负责。

设计目标：

- 高内聚：root-to-leaf 查找、latch coupling、range scan、split、merge、separator propagation、root split/shrink 都收敛在 `storage.btree`。
- 低耦合：B+Tree 只通过 `RecordPageAccessor` 操作页内记录，通过 `BufferPool` 获取 page，通过 `DiskSpaceManager` 分配/释放 page，通过 `LockManager` 获取逻辑锁。
- InnoDB 风格：对齐 `PAGE_LEVEL`、leaf page `FIL_PAGE_PREV/NEXT`、leaf/non-leaf segment、node pointer record、root page、right split、delete-mark/purge/merge、next-key lock。
- 可恢复：结构修改在 MTR 内完成，产生可重放的 redo payload；recovery handler 只按 pageLSN 幂等重放物理修改，不执行逻辑搜索。
- 并发安全：明确 root latch、page latch coupling、行锁等待前释放 page latch、等待后重新定位、split/merge 的持有者变化。

非目标：

- 不实现 SQL parser、optimizer、executor。
- 不解析 record byte layout，不维护 `next_record`、`n_owned`、PageDirectory。
- 不直接操作 `BufferFrame`、XDES、segment inode、redo file 或事务活跃表。
- 第一阶段不实现 change buffer、adaptive hash index、压缩页、空间索引 predicate lock、全文索引、完整 online DDL。
- 不生成 Java 源码。本文件只定义设计、关系和数据流。

## 2. MySQL 8.0 参考依据

本设计参考以下 MySQL 8.0 InnoDB 行为：

- 每个 InnoDB index 是一棵 B+Tree；聚簇索引 leaf 存储完整行，二级索引 leaf 存储二级 key 和聚簇主键。
- 每个 index 使用 leaf segment 和 non-leaf segment，以提升叶子页顺序性。
- Index page 通过 `PAGE_LEVEL` 区分 leaf 和 non-leaf；leaf level 为 0。
- Leaf page 通过 `FIL_PAGE_PREV`、`FIL_PAGE_NEXT` 串联，支持范围扫描。
- Non-leaf page 中存储 node pointer record，用 separator key 定位 child page。
- 页内查找使用 Page Directory 二分和 `next_record` 组内扫描；B+Tree 只负责跨页导航。
- Insert 可能触发 page split，顺序插入可优先 right split；随机插入通常按 split point 搬迁记录。
- Delete 先 delete-mark；purge 安全后物理删除，页低水位时可 redistribute 或 merge。
- 当前读和写操作需要 record/gap/next-key/insert intention lock；等待行锁后必须重新定位记录。

## 3. 总体架构

架构图见 [btree-architecture.mmd](diagrams/btree-architecture.mmd)。

`storage.btree` 分为十二组：

1. `storage.btree.api`：`BTreeIndexService` 门面，提供 lookup、scan、insert、delete、update key。
2. `storage.btree.domain`：`BTreeIndex`、`IndexId`、`SearchKey`、`SearchBound`、`TreePath`、`ChildPointer`。
3. `storage.btree.page`：`BTreePage` 类型化页视图，解释 `PAGE_LEVEL`、sibling link、node pointer。
4. `storage.btree.search`：root-to-leaf descent、lower/upper bound、unique lookup、child pointer 选择。
5. `storage.btree.cursor`：`BTreeCursor`、`CursorPosition`、重新定位和短生命周期 page handle。
6. `storage.btree.scan`：range scan、leaf sibling 遍历、read-ahead hint、锁定读扫描策略。
7. `storage.btree.split`：`PageSplitPlan`、right split、median split、separator propagation、root split。
8. `storage.btree.merge`：`PageMergePlan`、redistribute、merge、parent separator 更新、root shrink、page free。
9. `storage.btree.latch`：`LatchCouplingContext`、root latch、crabbing、restart policy。
10. `storage.btree.lockref`：构造 `IndexRecordRef`、`IndexGapRef`、next-key 范围。
11. `storage.btree.recovery`：`BtreePageRedoHandler`，只做物理页恢复。
12. `storage.btree.metric`：树高、split/merge、扫描页数、重定位次数、锁等待重试统计。

核心原则：

- B+Tree 负责跨页结构，不负责页内 record 编码。
- B+Tree 不持有裸 `BufferFrame`，只使用 `PageHandle`、`PageCursor` 和 `BTreePage`。
- B+Tree 只选择事务锁落点，不判断 MVCC 可见性，不维护 Wait-For Graph。
- B+Tree 结构修改必须在 MTR 内完成。
- B+Tree recovery handler 只应用物理页修改，不重新执行 split/merge 决策。

## 4. 包与职责

| 包 | 职责 | 主要依赖 | 设计模式 |
| --- | --- | --- | --- |
| `storage.btree.api` | 索引门面、lookup、scan、insert、delete、update key | `domain`, `trx`, `mtr` | Facade |
| `storage.btree.domain` | index 元数据、search key、bound、path、pointer | `record.schema` | Value Object |
| `storage.btree.page` | BTree page 视图、level、sibling、child pointer | `buf`, `record` | Adapter |
| `storage.btree.search` | root-to-leaf 查找、lower/upper bound | `page`, `latch` | Template Method |
| `storage.btree.cursor` | cursor position、短生命周期定位、重新定位 | `search`, `page` | Cursor, Snapshot |
| `storage.btree.scan` | range scan、read-ahead hint、锁定读策略 | `search`, `trx` | Strategy |
| `storage.btree.split` | split plan、separator、parent propagation | `fsp`, `record`, `mtr` | Command, Policy |
| `storage.btree.merge` | redistribute、merge、root shrink、free page | `fsp`, `record`, `mtr` | Command, Policy |
| `storage.btree.latch` | latch coupling、root latch、restart | `buf`, `mtr` | State, RAII Guard |
| `storage.btree.lockref` | record/gap/next-key lock target 构造 | `trx.lock` | Adapter |
| `storage.btree.recovery` | btree page redo handler | `redo`, `buf` | Recovery Handler |
| `storage.btree.metric` | 树高、split/merge、重试指标 | 无 | Observer |

推荐依赖方向：

`sql/executor -> btree.api -> search/scan/split/merge -> page -> record + buf`  
`btree -> trx.lock + trx.mvcc + trx.undo`  
`btree -> fsp + mtr -> redo`  

禁止方向：

- `btree` 不能 import `storage.fil` 或直接使用 `FileChannel`。
- `btree` 不能修改 XDES、segment inode、space header。
- `btree` 不能解析 record field layout、NULL bitmap、var-length directory。
- `btree` 不能维护事务等待图或选择 deadlock victim。
- `btree.recovery` 不能调用普通 search/split 逻辑。

## 5. 核心领域模型

类关系图见 [btree-class-relation.mmd](diagrams/btree-class-relation.mmd)。

### 5.1 BTreeIndex

`BTreeIndex` 是索引聚合根：

- `indexId`
- `tableId`
- `rootPageId`
- `rootLevel`
- `leafSegment`
- `nonLeafSegment`
- `indexKeyDef`
- `clustered`
- `unique`
- `nullableKeyParts`
- `schemaVersion`

规则：

- 聚簇索引 leaf 保存完整行和隐藏列。
- 二级索引 leaf 保存二级 key part 和聚簇主键。
- 第一阶段二级索引命中后回表到聚簇索引做 MVCC 判断。
- root page 可以保持稳定；root split 时复制旧 root 到新 child，再重建 root。

### 5.2 BTreePage

`BTreePage` 是借用 `PageHandle` 的类型化页视图：

- `pageId`
- `level`
- `isLeaf`
- `prevPageId`
- `nextPageId`
- `recordAccessor`
- `childPointers`
- `pageLsn`

规则：

- `BTreePage` 不拥有 latch 或 buffer fix 生命周期。
- 页内 record 查找、插入、删除、reorganize 交给 `RecordPageAccessor`。
- Non-leaf page 的 child pointer record 由 Record 模块提供物理访问，B+Tree 解释其 child page 语义。

### 5.3 SearchKey 与 SearchBound

`SearchKey` 表示复合索引 key：

- `keyParts`
- `prefixLength`
- `nullFlags`
- `sortOrder`
- `collationContext`

`SearchBound` 表示范围扫描边界：

- `lowerKey`
- `upperKey`
- `lowerInclusive`
- `upperInclusive`
- `direction`
- `limit`

比较规则由 `RecordComparator` 和 `IndexKeyDef` 提供，B+Tree 不直接比较 Java 对象值。

### 5.4 BTreeCursor 与 CursorPosition

`BTreeCursor` 保存稳定定位信息：

- `indexId`
- `stableSearchKey`
- `position`
- `readType`
- `transaction`
- `released`

`CursorPosition`：

- `pageId`
- `recordRef` 或 `gapRef`
- `pageLsn`
- `schemaVersion`
- `deleteFlagSnapshot`

规则：

- Cursor 不长期持有 page latch。
- 等待行锁后必须重新定位，旧 `CursorPosition` 只能用于校验。
- page split、merge、reorganize 后，旧位置可能失效。

### 5.5 PageSplitPlan

`PageSplitPlan`：

- `oldPageId`
- `newPageId`
- `splitType`: `RIGHT`, `MEDIAN`, `ROOT`
- `splitPoint`
- `movedRecordRange`
- `separatorKey`
- `parentInsert`
- `requiresParentSplit`
- `siblingLinkChanges`

策略：

- 顺序插入优先 right split。
- 随机插入按 record size 和 key 分布选择 median split。
- Root split 保持 root page id 稳定，降低数据字典更新需求。

### 5.6 PageMergePlan

`PageMergePlan`：

- `victimPageId`
- `siblingPageId`
- `parentPageId`
- `action`: `REDISTRIBUTE`, `MERGE`, `ROOT_SHRINK`, `NO_ACTION`
- `recordsToMove`
- `separatorUpdate`
- `freePageId`

策略：

- 页填充率低于阈值才进入 merge 评估。
- 优先 redistribute，减少 page free 和 parent 修改。
- merge 后必须更新 leaf sibling link 和 parent separator。

### 5.7 LatchCouplingContext

`LatchCouplingContext`：

- `mtr`
- `pathStack`
- `rootLatchMode`
- `parentHandle`
- `childHandle`
- `operationMode`
- `restartCount`
- `timeoutPolicy`

用途：

- 统一 root latch、parent/child page latch 的获取和释放。
- 根据 child 是否 safe 决定释放 parent 或重启。
- 写路径遇到 unsafe child 时可切换到 pessimistic 模式。

## 6. B+Tree 页结构

B+Tree page 复用 Index Page envelope：

| 区域 | B+Tree 语义 |
| --- | --- |
| FileHeader | `FIL_PAGE_PREV/NEXT`、page type、pageLSN、checksum |
| PageHeader | `PAGE_LEVEL`、record count、heap top、garbage、directory slots |
| Infimum/Supremum | 页内边界 record |
| UserRecords | leaf row/index entry 或 non-leaf node pointer record |
| FreeSpace | record 插入和 page reorganize 空间 |
| PageDirectory | 页内稀疏目录，由 Record 模块维护 |
| FileTrailer | checksum 和低位 LSN |

Leaf page：

- `PAGE_LEVEL = 0`。
- 聚簇索引 leaf 保存完整行。
- 二级索引 leaf 保存二级 key 和聚簇主键。
- `FIL_PAGE_PREV/NEXT` 连接同 level leaf page，支持 range scan。

Non-leaf page：

- `PAGE_LEVEL > 0`。
- 记录保存 separator key 和 child page pointer。
- child page level 必须比当前 page level 小 1。

## 7. 查找与扫描策略

查找流程见 [btree-search-flow.mmd](diagrams/btree-search-flow.mmd)。

### 7.1 Point Lookup

1. 获取 root S latch。
2. 在 non-leaf page 内用 Record 模块查找 child pointer。
3. 获取 child latch。
4. 如果 child safe，释放 parent latch。
5. 重复直到 leaf page。
6. 在 leaf page 内查找目标 key。
7. Consistent read 交给 MVCC 判断。
8. Locking read 捕获 lock ref，释放 page latch 后获取事务锁，再重新定位。

### 7.2 Range Scan

1. 使用 lower bound 定位起始 leaf page。
2. 在当前 leaf 内扫描到 upper bound 或页尾。
3. 需要继续时沿 `FIL_PAGE_NEXT` 获取下一 leaf。
4. 普通 consistent read 不等待普通行锁。
5. `SELECT FOR UPDATE`、`UPDATE`、`DELETE` 在范围内按隔离级别获取 record/gap/next-key lock。
6. 扫描可向 Buffer Pool 提交 read-ahead hint。

READ COMMITTED 的 residual-miss 优化使用 additive scoped-candidate API：existing `lockPoint/lockRange` 继续返回
事务期锁语义；新入口把已重定位的记录快照与该次精确 `LockHandle` 绑定。只有上层用当前完整聚簇行证明最终
residual 为 false 后才能释放该句柄；未分类、异常、RR/SERIALIZABLE 和 gap/next-key 均 fail-closed 保留。

### 7.3 Search Restart

以下情况必须 restart：

- 等待事务锁后。
- page split/merge/reorganize 导致 `CursorPosition` 失效。
- latch timeout。
- child page unsafe 且当前 latch 模式不足。
- root page level 变化。

Restart 从 root 或确定安全的 latch coupling 点开始，不能复用旧 `RecordCursor`。

## 8. 写入、Split 与 Merge

写入流程见 [btree-write-flow.mmd](diagrams/btree-write-flow.mmd)。

Merge 流程见 [btree-merge-flow.mmd](diagrams/btree-merge-flow.mmd)。

### 8.1 Insert

1. 构造 `BTreeOperationContext`。
2. 唯一索引用 current read 做冲突检查。
3. 对目标 gap 获取 insert intention lock。
4. 等待后重新定位目标 leaf。
5. leaf 空间足够时调用 Record insert。
6. 空间不足时构造 `PageSplitPlan`。
7. MTR commit 后释放 page latch 和 buffer fix。

聚簇主键命中 delete-marked 记录时不能直接当 duplicate，也不能物理 purge 后插入。state-aware current-read 以
`ABSENT/LIVE/DELETE_MARKED` 分类：live 使用 record S，marked 使用 record X，absent 使用 insert intention；等待后
状态变化必须按同一 deadline 重试。row guard 内 marked 仍稳定时，B+Tree 以旧隐藏列作 CAS，使用 update-class undo
roll pointer 替换同 key 聚簇记录并清除 delete mark；marked 已被 purge 则退化为普通 insert。替换前必须一次性验证
deleted flag、相同主键、编码长度/页容量，失败不得留下半写记录。

### 8.2 Split

Split 路径：

1. `SpaceReservationService.reserve()` 预留 page 和 parent 修改空间。
2. `DiskSpaceManager.allocatePage()` 从 leaf 或 non-leaf segment 分配新 page。
3. `BufferPool.createPage()` 初始化新 index page。
4. Record 搬迁 record 并重建 PageDirectory。
5. B+Tree 更新 sibling link。
6. 向 parent 插入 separator key。
7. parent 空间不足时递归 split。
8. root split 时复制旧 root 到新 child，再重建 root page。

### 8.3 Delete、Purge 与 Merge

用户 delete：

- 事务层写 update undo。
- Record 设置 delete-mark。
- B+Tree 不立即物理删除。

Purge 物理删除：

1. Purge 确认最老 ReadView 不再需要该版本。
2. 获取 leaf X latch。
3. Record 物理摘除 record。
4. 若页低水位，评估 sibling。
5. 优先 redistribute。
6. 无法 redistribute 时 merge，并更新 parent。
7. victim page 释放给 Disk Manager。

### 8.4 Update Key

- 非 key 字段更新由 Record/Transaction 处理。
- 主键或索引 key 变化等价于 old entry delete-mark + new entry insert。
- 二级索引 key 变化需要同步删除旧二级项并插入新二级项。
- 聚簇主键变化可能导致所有二级索引回表 key 变化，第一阶段可限制为不支持或转换为 delete + insert。

## 9. 与其它模块的协作

### 9.1 与 Disk Manager

- B+Tree 只调用 `reserve()`、`allocatePage()`、`freePage()`。
- Leaf page 使用 leaf segment，non-leaf page 使用 non-leaf segment。
- B+Tree 不修改 XDES、INODE、space header。

### 9.2 与 Buffer Pool

- B+Tree 只通过 `getPage()`、`createPage()` 获取 page。
- B+Tree 不接触 `BufferFrame`。
- 等待事务锁前必须释放 page latch 和 buffer fix。
- Range scan 可向 Buffer Pool 提交 prefetch/read-ahead hint。

### 9.3 与 Record

- Record 负责页内 record 查找、插入、delete-mark、purge、reorganize。
- B+Tree 负责跨页路径、sibling link、parent separator。
- B+Tree 不直接修改 `next_record`、`n_owned`、PageDirectory。

### 9.4 与 Transaction/MVCC

- B+Tree 构造 `IndexRecordRef`、`IndexGapRef`、next-key 范围。
- LockManager 决定兼容性、等待、死锁检测和 victim。
- MVCC 决定版本可见性。
- UndoLogManager 负责 insert/update undo。

### 9.5 与 Redo/MTR

- B+Tree 结构修改必须在 MTR 内完成。
- Redo payload 可以表达 page bytes 或更高层 page operation。
- Recovery handler 只做幂等物理应用，不重新选择 split point。

## 10. 并发与锁顺序

Latch 状态图见 [btree-latch-state.mmd](diagrams/btree-latch-state.mmd)。

### 10.1 锁类型

| 资源 | 类型 | 模式 | 死锁域 |
| --- | --- | --- | --- |
| root latch | latch | S/SX/X | timeout/restart |
| page latch | latch | S/X | timeout/restart |
| sibling latch | latch | S/X，按 `PageId` 排序 | timeout/restart |
| record lock | transaction lock | REC_S/REC_X | Wait-For Graph |
| gap lock | transaction lock | GAP_S/GAP_X | Wait-For Graph |
| next-key lock | transaction lock | NEXT_KEY_S/NEXT_KEY_X | Wait-For Graph |
| insert intention | transaction lock | INSERT_INTENTION | Wait-For Graph |
| Buffer Pool internal mutex | mutex | exclusive | timeout/retry |

### 10.2 Latch Coupling

规则：

1. 获取 parent latch。
2. 选择 child pointer。
3. 获取 child latch。
4. 判断 child 是否 safe。
5. safe 时释放 parent。
6. unsafe 写路径重启或进入 pessimistic 模式。

Safe page 定义：

- 插入时有足够空间容纳目标 record。
- 删除/merge 时不会低于 merge 阈值或可安全延后。
- 非叶节点 parent 有足够空间接收 separator。

### 10.3 锁状态与持有变化

| 状态 | 持有者 | 持有资源 | 进入条件 | 退出条件 |
| --- | --- | --- | --- | --- |
| `START` | 无 | 无 | 操作开始 | 获取 root latch |
| `ROOT_LATCHED_S` | 当前线程/MTR | root S latch | 普通查找 | latch child |
| `ROOT_LATCHED_SX` | 当前线程/MTR | root SX latch | 可能结构修改 | latch child 或 restart |
| `CHILD_LATCHED` | 当前线程/MTR | parent + child latch | crabbing 下降 | child safe 后释放 parent |
| `PARENT_RELEASED` | 当前线程/MTR | child latch | child safe | 继续下降或到达 leaf |
| `LEAF_LATCHED` | 当前线程/MTR | leaf latch | 到达 leaf | capture ref 或修改 |
| `REF_CAPTURED` | 当前线程 | lock ref 值对象 | 需要事务锁 | 释放 page latch |
| `PAGE_RELEASED` | 无 page latch | 只保留 stable key/ref | 等待事务锁前 | 进入 LockManager |
| `WAITING_ROW_LOCK` | LockManager | wait queue edge | 事务锁冲突 | grant、timeout、victim |
| `ROW_LOCK_GRANTED` | Transaction | 事务逻辑锁 | LockManager grant | 重新定位 |
| `RELOCATING` | 当前线程 | 事务锁 | 等待后重进 B+Tree | 获取目标 latch |
| `MODIFYING` | 当前线程/MTR | page X latch、buffer fix、事务锁 | 校验成功 | MTR commit |
| `MTR_COMMITTING` | MTR | page latch、buffer fix、redo memo | 修改完成 | release memo |
| `RELEASED` | 无 | 无短锁 | 正常结束、timeout、victim | 操作结束或重试 |

持有变化规则：

- `acquire root/page latch`：进入 MTR memo。
- `upgrade root latch`：只允许短时间转换，失败则 release and restart。
- `wait row lock`：等待前必须 release page latch、RecordCursor、buffer fix、父页 latch。
- `grant row lock`：事务锁归 Transaction 所有，不归 BTreeCursor 所有。
- `relocate`：锁授予后重新定位，校验 key、heapNo、delete flag、pageLSN、schemaVersion。
- `release`：MTR 释放 latch/fix；事务锁由 statement 或 transaction 生命周期释放。

## 11. 异常处理

异常类型：

- `BTreeSearchException`
- `BTreeStructureCorruptedException`
- `BTreeLatchTimeoutException`
- `BTreeRelocationRequiredException`
- `BTreeSplitRequiredException`
- `BTreeMergeConflictException`
- `BTreeRootChangedException`
- `BTreeDuplicateKeyException`
- `BTreeRecoveryException`

错误策略：

- Latch timeout 不进入事务死锁检测，调用方按 retry policy 重试。
- 等待行锁后定位失效时抛出 relocation 异常并重启查找。
- 结构损坏时交给 recovery/corruption 标记，不能静默修复。
- Duplicate key 必须在 current read 和必要锁保护下确认。
- Recovery replay 遇到 pageLSN 已覆盖的记录必须幂等跳过。

## 12. API 设计

### 12.1 BTreeIndexService

- `lookup(BTreeIndex, SearchKey, ReadContext)`
- `scan(BTreeIndex, SearchBound, ScanContext)`
- `insert(BTreeIndex, LogicalRecord, BTreeOperationContext)`
- `deleteMark(BTreeIndex, SearchKey, BTreeOperationContext)`
- `purge(BTreeIndex, CursorPosition, BTreeOperationContext)`
- `updateKey(BTreeIndex, SearchKey oldKey, LogicalRecord newRecord, BTreeOperationContext)`

### 12.2 BTreeCursor

- `position()`
- `current()`
- `next()`
- `previous()`
- `relocate()`
- `release()`

### 12.3 LatchCouplingController

- `descendToLeaf(BTreeIndex, SearchKey, OperationMode, MTR)`
- `restartWithStrongerLatch(TreePath, OperationMode)`
- `releaseAncestors(TreePath)`
- `validatePosition(CursorPosition)`

## 13. 设计模式使用清单

- Facade：`BTreeIndexService`。
- Template Method：root-to-leaf search、insert、split、merge。
- Strategy：split policy、merge policy、scan lock policy、restart policy。
- State：latch coupling state、cursor position state。
- Command：`PageSplitPlan`、`PageMergePlan`、BTree redo operation。
- Repository：index metadata repository、root page locator。
- Adapter：`BTreePage` 适配 `PageHandle/PageCursor`。
- RAII Guard：Tree path latch guard、page handle guard。
- Snapshot：`CursorPosition` 保存重定位校验快照。
- Observer：metric observer 记录 split/merge/retry。

## 14. 高内聚、低耦合约束

强制规则：

- B+Tree 不解析 record 字段编码。
- B+Tree 不直接分配 extent 或修改空间管理元数据。
- B+Tree 不持有 `BufferFrame`。
- B+Tree 不在持有 page latch 时等待事务锁。
- B+Tree 不决定 MVCC 可见性和 deadlock victim。
- B+Tree recovery 不执行逻辑搜索。
- 所有结构修改必须进入 MTR 和 redo 边界。

## 15. 典型数据流

### 15.1 Point Lookup

1. 读取 root。
2. Latch coupling 下降到 leaf。
3. Record 查找目标 key。
4. Consistent read 交给 MVCC。
5. Locking read 捕获 ref，释放 latch，获取事务锁，重新定位。

### 15.2 Range Scan

1. 定位 lower bound。
2. 在 leaf 内扫描。
3. 沿 `FIL_PAGE_NEXT` 访问下一 leaf。
4. RR 锁定读对范围记录和 gap 加 next-key/gap lock。
5. 返回可见版本或当前版本。

### 15.3 Insert Split

1. 获取 insert intention lock。
2. 重新定位 leaf。
3. 空间不足时 reserve/allocate new page。
4. Split 移动 records。
5. Parent 插入 separator。
6. 必要时 root split。
7. MTR commit。

### 15.4 Purge Merge

1. Purge 发现 delete-mark record 可物理删除。
2. Record 摘除 record。
3. B+Tree 判断页填充率。
4. Redistribute 或 merge。
5. Parent 更新。
6. Disk Manager free victim page。

## 16. 测试设计

单元测试：

- SearchKey 比较和 bound 计算。
- child pointer 选择。
- latch coupling safe/unsafe 判定。
- split point 选择。
- separator key 构造。
- merge/redistribute 决策。

集成测试：

- root-to-leaf point lookup。
- range scan 跨 leaf sibling。
- unique insert 与 duplicate key。
- leaf split、parent split、root split。
- delete-mark 后 purge 物理删除。
- merge 后 parent separator 和 sibling link 正确。
- 等待行锁后重新定位。
- pageLSN 幂等 redo replay。

并发测试：

- 多线程查找与插入。
- latch timeout 后 restart。
- split 与 range scan 并发。
- purge merge 与 current read 并发。
- record/gap/next-key deadlock 由 LockManager 选择 victim。

性质测试：

- 随机插入删除后 leaf key 有序。
- 所有 leaf sibling link 双向一致。
- non-leaf separator 覆盖 child key range。
- crash point 后 redo replay 不破坏树结构。

## 17. 后续实现顺序

1. `BTreeIndex`、`SearchKey`、`SearchBound`、`CursorPosition` 值对象。
2. `BTreePage` 适配 `PageHandle` 和 Record accessor。
3. root-to-leaf S latch search。
4. point lookup 与 consistent read 集成。
5. range scan 与 leaf sibling 遍历。
6. current read lock ref 构造和重新定位协议。
7. insert without split。
8. leaf split 和 parent insert。
9. root split。
10. delete-mark 调用和 purge physical delete。
11. redistribute/merge/root shrink。
12. redo/recovery handler。
13. 并发压力测试和故障注入。

## 18. 十五轮自检记录

| 轮次 | 检查主题 | 检查结果 |
| --- | --- | --- |
| 1 | 不写代码边界 | 只新增 Markdown 和 Mermaid 设计内容，没有生成 Java 源码 |
| 2 | 目标与非目标 | 已明确 B+Tree 负责跨页索引结构，不负责 record 编码、事务死锁或文件 IO |
| 3 | MySQL 8.0 贴合 | 已覆盖 InnoDB root/leaf/non-leaf、`PAGE_LEVEL`、sibling link、node pointer、split/merge、next-key lock |
| 4 | 高内聚 | 查找、扫描、split、merge、latch coupling 和 cursor 都收敛在 `storage.btree` |
| 5 | 低耦合 | 通过 Record、Buffer Pool、Disk Manager、Transaction、MTR/Redo 门面协作 |
| 6 | 面向对象 | 已定义 `BTreeIndex`、`BTreePage`、`BTreeCursor`、`PageSplitPlan`、`PageMergePlan`、`LatchCouplingContext` |
| 7 | 设计模式 | 已覆盖 Facade、Template Method、Strategy、State、Command、Repository、Adapter、RAII Guard、Snapshot |
| 8 | 核心领域模型 | 已覆盖 index/page/cursor/search key/split/merge/latch/lock ref |
| 9 | 依赖方向 | 已明确 `btree -> record/buf/fsp/trx/mtr`，并列出禁止依赖 |
| 10 | 物理/逻辑区分 | 已区分跨页结构、页内 record、事务逻辑锁和物理 redo recovery |
| 11 | 关键数据流 | 已覆盖 lookup、range scan、insert、split、purge merge、update key |
| 12 | 图示 | 已新增并引用架构图、类关系图、查找流、写入流、merge 流和 latch 状态图 |
| 13 | 并发锁状态 | 已说明 root/page latch、row lock wait、重新定位、MTR commit 的状态和持有变化 |
| 14 | 异常与恢复 | 已定义 latch timeout、relocation、structure corruption、duplicate key 和 redo replay 幂等策略 |
| 15 | 测试与实现顺序 | 已覆盖单元、集成、并发、性质测试和后续实现顺序，并确认不含未完成占位文本 |

## 19. 参考链接

- MySQL 8.0 Reference Manual - InnoDB Indexes: https://dev.mysql.com/doc/refman/8.0/en/innodb-indexes.html
- MySQL 8.0 Reference Manual - Clustered and Secondary Indexes: https://dev.mysql.com/doc/refman/8.0/en/innodb-index-types.html
- MySQL 8.0 Reference Manual - InnoDB Locking: https://dev.mysql.com/doc/refman/8.0/en/innodb-locking.html
- MySQL 8.0 Reference Manual - InnoDB Row Formats: https://dev.mysql.com/doc/refman/8.0/en/innodb-row-format.html
- MySQL 8.0.46 Source Documentation - `btr0btr.h`: https://dev.mysql.com/doc/dev/mysql-server/8.0.46/btr0btr_8h.html
- MySQL 8.0.46 Source Documentation - `btr0cur.h`: https://dev.mysql.com/doc/dev/mysql-server/8.0.46/btr0cur_8h.html
- MySQL 8.0.46 Source Documentation - `page0cur.h`: https://dev.mysql.com/doc/dev/mysql-server/8.0.46/page0cur_8h.html
