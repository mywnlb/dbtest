# MiniMySQL 二级索引、回表 MVCC 与 Purge 生产闭环设计

版本：2026-07-23
关联设计：`innodb-btree-design.md`、`innodb-transaction-mvcc-design.md`、
`innodb-undo-log-purge-design.md`、`mysql-data-dictionary-ddl-design.md`、
`mysql-sql-executor-storage-api-design.md`

## 1. 背景与权威边界

本设计是 `storage-backlog.md` 2.2 的跨模块权威设计。现有 DD 已能持久化一张表的多个 index binding，B+Tree
也能创建多个 root，但生产 DML、rollback、MVCC 和 purge 仍只维护聚簇索引。二级 root 存在不代表二级记录已经
进入生产链，本设计禁止用测试直调冒充生产接线。

模块边界保持：DD 提供不可变逻辑定义和物理 binding；Record 定义页内 entry；B+Tree 维护结构；Transaction/Undo
维护原子性与版本；Purge 回收历史；SQL 只经 outbound gateway 访问 storage。二级索引不保存完整聚簇隐藏列，
一致性判断必须回表。

## 2. 目标与非目标

目标：

- 二级 leaf 使用紧凑 entry，并由 INSERT/UPDATE/DELETE 同步维护。
- statement/full/recovery rollback 能幂等撤销部分完成的多索引写。
- 唯一二级等值 SELECT 走真实 secondary scan -> clustered MVCC -> LOB hydration。
- purge 同时清理 UPDATE 旧 key 与 DELETE 的全部二级 entry。
- DROP 在持久 history 仍引用表元数据时不得删除表空间。

非目标：SQL UPDATE/DELETE、聚簇主键更新、LOB/JSON 二级 key、在线 CREATE/DROP INDEX、普通非唯一二级 SQL
范围查询、optimizer、prepared statement、多 worker purge、MySQL 二进制兼容。

## 3. 核心决策

### 3.1 物理 key 与逻辑唯一分离

二级物理字段按“声明的二级 key parts + 完整聚簇主键 parts”排列。主键列即使已出现在二级 key 中也再次附加，
从而每个 entry 都能无歧义回表。每个物理字段拥有独立连续 ordinal；layout 另存源表 ordinal 映射。

`BTreeIndex` 的布尔唯一语义改名为 `physicalUnique`。聚簇索引为 true；二级索引因为带完整主键后缀也为 true。
DD 的 logical unique 保存在 `SecondaryIndexMetadata`，不能再传给 B+Tree 物理 duplicate 检查。

### 3.2 紧凑 secondary schema

`SecondaryIndexLayout` 持有 entry schema、物理 key definition、logical key part 数、cluster-key 后缀位置和源 ordinal
映射。secondary conventional record 不带 `HiddenColumns`，delete mark 继续使用 record header。prefix 影响比较，entry
保存完整内联字段。TEXT/BLOB/JSON key 在 DD 构造和 mapper 打开两处拒绝。

### 3.3 root level 的权威来源

root page id 稳定，但 root level 会因 split/shrink 变化。DD binding 中的 level 只是创建时提示；结构写、rollback、
purge 在冻结 redo budget 前必须用短只读 MTR 读取 root 页头并生成刷新后的 `BTreeIndex` 快照。

### 3.4 一个逻辑 undo、多个短 MTR

首个业务 MTR 写 undo、LOB 和聚簇记录；每棵二级树各用独立 MTR。这样不在一个 MTR 内跨多棵树持页 latch，避免
锁序和 Buffer Pool pin 放大。事务只在全部 secondary MTR 成功后提交；任一步失败由 statement/full/recovery
rollback 收敛。

## 4. 领域模型

- `SecondaryIndexLayout`：全行与紧凑 entry/逻辑 key/聚簇 key 间的纯映射。
- `SecondaryIndexMetadata`：BTree 物理描述符、layout、logical unique。
- `TableIndexMetadata`：table id/schema version、唯一聚簇索引、按 index id 排序的二级索引。
- `SecondaryUndoMutation`：本次行 undo 对一个二级索引的反向证据。
- `PurgeDmlRowGuardManager`：DML 与 purge 的短物理协调 guard，不是事务锁。
- `SecondaryMvccReader`：secondary 候选回表、可见版本复核和去重。
- `SecondaryPurgeSafetyChecker`：从当前版本扫描到目标 roll pointer，判断 entry 是否仍被较新版本需要。
- `TablePurgeBarrier`：DD 只依赖的稳定 API；实现状态由 persistent history 重建。

## 5. DML 数据流

### 5.1 INSERT

1. 分配 write id，检查聚簇主键；对所有非 NULL logical unique secondary key 取得事务级 X 锁并在等待后重扫当前前缀。
2. 用待写聚簇行主键取得行 guard；进入前不持 page/undo latch 或 MTR。
3. 冻结 INSERT undo secondary mutation 列表。
4. 首个 MTR 写 undo、LOB、聚簇行并提交。
5. 按 index id 逐个 MTR 插入 secondary entry；唯一二级在其它主键只留下 marked 历史时从 ABSENT 进入 live。
6. 全部完成后返回，事务锁与 unique range lock 保留到事务终态。

### 5.2 UPDATE

1. current-read 获取聚簇 X lock，等待后重定位并物化当前完整行。
2. 用 index comparator/prefix 语义比较 old/new logical key；等价 key 不产生 mutation。
3. 对变化的非 NULL unique key 取得 logical-prefix X 锁，等待后扫描包含 delete-marked 的全部当前候选。
4. 从物化行提取主键取得行 guard，不能使用用户搜索字面量作 guard identity；进入前不持页 latch/MTR。
5. 初查为同主键 DELETE_MARKED 时，在 row guard 内 exact 重读其状态，抵御初查后 purge 物理删除竞态；把最终
   `ABSENT` 或 `DELETE_MARKED` 写入 undo mutation。
6. 首个 MTR 写 UPDATE undo 并替换聚簇行。
7. 每个变化索引先插入/复活新 entry，再 delete-mark 旧 entry。

### 5.3 DELETE

1. current-read 获取聚簇 X lock并物化当前行。
2. 取得行 guard，冻结全部二级 DELETE_MARK mutation。
3. 首个 MTR 写 DELETE_MARK undo并标记聚簇记录。
4. 按 index id 标记全部 secondary entry。

## 6. 唯一二级检查

NULL 参与 logical unique key 时允许多行，不做 logical duplicate 检查。非 NULL key 先取得 collation/prefix 归一化的
logical-key X 锁并持有到事务终态；删除或改出旧 key 的事务也持有同一锁，因此等待结束后重扫 B+Tree 当前前缀即可
观察提交或回滚结果。PREPARED 事务继续持锁，直到二阶段 commit/rollback 才唤醒等待者。

唯一约束属于 current-read 判断，不使用调用事务的旧 ReadView，也不沿候选 undo 返回历史版本。扫描必须包含
delete-marked entry：任意主键的 live 候选都报告重复；其它主键只留下的 marked 历史不再冲突；目标完整物理
identity 的 marked entry 只供 UPDATE 复活，INSERT 新主键按 ABSENT 发布新 suffix。DELETE 提交后 key 可在 purge 前复用，
DELETE 回滚后恢复的 live entry 仍冲突。

单次扫描读取安全容量 1024 加一个 overflow 候选。发现 live 可立即报告重复；否则超过容量即抛领域容量异常并
fail-closed，禁止用截断前缀发布。UPDATE 初查同主键 marked 后、取得 row guard 前，purge 可能物理删除该 entry；因此
必须在 row guard 内 exact 重读，把 ABSENT/DELETE_MARKED 的稳定前态写入 undo。该重读不再次进入 LockManager。

## 7. Undo secondary tail

`UndoRecord.secondaryMutations` 按 index id 严格递增：

- `INSERT_ENTRY`：INSERT 创建的 entry。
- `CHANGE_KEY`：UPDATE 的 old mark + new publish；保存 new entry 修改前为 `ABSENT` 或 `DELETE_MARKED`。
- `DELETE_MARK_ENTRY`：DELETE 标记的 entry。
- `PUBLISH_ENTRY`：聚簇 marked-key reuse 为新行发布 entry；保存 `ABSENT/DELETE_MARKED`，rollback 删除或重新标记，
  purge 不把它解释成旧 entry 清理任务。

固定 identity、cluster key、旧 image 和现有 INSERT LOB tail 不变。secondary tail 使用独立 magic/version/count；
INSERT 同时有两个尾部时顺序为 LOB -> secondary。旧 record EOF 解码为空 mutation。未知 magic/version、重复索引、
非法 action/state、截断和尾随一律 fail-closed，不提升 undo page format version。

## 8. Rollback

单条 undo 的物理顺序固定为“全部 secondary inverse -> clustered inverse -> logical-head marker”。

- INSERT：从当前聚簇行重建 entry，物理删除全部二级 entry，最后删除聚簇行。
- UPDATE：ABSENT 的新 entry 物理删除；原为 DELETE_MARKED 的新 entry 恢复标记；旧 entry 恢复 live；最后恢复
  聚簇 old image。
- DELETE：先恢复全部二级 entry live，最后取消聚簇 delete mark。

聚簇 inverse 是本条 undo 的完成证明。若 crash 发生在聚簇 inverse 后、marker 前，重试必须验证 INSERT 行已不存在，
或 UPDATE/DELETE 行精确等于 old image/old hidden columns，才能跳过二级 inverse并推进 marker；其它状态视为损坏。

## 9. 回表 MVCC

secondary scan including deleted 物化紧凑候选后立即释放二级页资源。随后按 entry 的完整聚簇主键调用现有
`MvccReader`，再从可见完整行重算 logical secondary key，用相同类型、collation、prefix 和方向语义复核谓词。
结果按聚簇主键去重；同一非 NULL unique key 得到多个不同可见行时 fail-closed。ReadView 必须覆盖 secondary scan、
clustered version traversal、LOB hydration 和公开投影。

Binder 在现有 equality predicate 语法上确定访问路径：完整主键优先；否则选择完整、无 prefix 的 unique secondary；
多个候选取最小 index id。`column = NULL` 直接为空。额外谓词、普通非唯一索引和 prefix unique SQL 点查仍拒绝。

## 10. Secondary purge

Purge 解析 UPDATE_ROW 和 DELETE_MARK。每个 task 携带目标 roll pointer、旧完整行和 secondary metadata。worker 零等待
取得行 guard，从当前聚簇版本向后遍历：检查当前 live 行和较新 undo 的 old image；到达目标 pointer 时停止且不把目标
旧版本计作保留理由。任一较新版本仍映射到目标物理 entry则保留；链无法到达目标 pointer 时 fail-closed。

安全后精确移除 delete-marked secondary entry：缺失是幂等成功，仍 live 是损坏。DELETE_MARK 必须先完成全部
secondary task，再物理删除聚簇记录；UPDATE_ROW 只清理旧 secondary entry。任何失败都保留 history head，全部任务
成功后才进入既有原子 head removal/finalization。

## 11. 行协调与锁顺序

`PurgeDmlRowGuardManager` 使用 1024 个公平 `ReentrantLock` 分片。既有记录的 identity 来自物化聚簇行中的主键；INSERT
用待写行主键并由主键唯一检查串行化等价 key。DML 有界等待 guard，purge 只 `tryAcquire`。

锁序：事务 record/unique locks -> row guard -> 单个 MTR/page latch。等待事务锁前不得持 row guard；purge 不申请事务锁、
不进入 Wait-For Graph，也不持 history transition 执行 B+Tree/undo IO。

## 12. Table purge barrier 与 DROP

`HistoryEntry.affectedTableIds` 表示一个 committed UPDATE undo log 涉及的表集合。前台在 planUpdate/planDelete 时记录；
commit 发布 history 时增加 barrier 引用，finalization 发布后减少。恢复期扫描 persistent history 的 undo identity 重建集合，
不另存可能漂移的独立计数。

DROP 在持 schema/table MDL X 后、发布 DROP_PENDING 前有界等待 barrier 清零；超时保持 ACTIVE。DROP_PENDING recovery
物理删除前再次要求清零。DROP_PENDING metadata 在物理删除前仍可被 rollback/purge resolver 读取。

## 13. Redo、恢复与异常

每个 secondary MTR独立冻结 operation budget，structural insert/delete 使用刷新后的 root level。WAL、pageLSN、dirty publish
沿用既有 MTR。任一物理修改后的结果不确定都进入项目 fatal/fail-stop 路径，不能把 MTR rollback 误称为页内容撤销。

新增异常覆盖 secondary layout/unique corruption、mutation format、row guard timeout、version-chain mismatch 和 purge barrier
timeout；全部继承项目异常并保留 cause。

## 14. 验收

- SQL INSERT 同步维护全部二级索引，唯一二级等值 SELECT 经过真实回表 MVCC。
- 唯一二级 key 等待 delete 事务终态后重扫：commit 可在 purge 前复用，rollback 恢复 live 冲突，PREPARED 等到二阶段。
- 多个 marked 历史不会遮住后方 live 冲突；1024+1 超限 fail-closed；UPDATE 与并发 purge 后 rollback 可正确收敛。
- statement/full/recovery rollback 在任意 secondary MTR 中断点可重试收敛。
- A->B->A 与较新版本仍需旧 entry 时 purge 不误删，后续对应 undo 最终回收。
- DROP 不越过 persistent history；重启可重建 barrier并续作 pending drop。
- current map 只按最终源码生产调用链更新；固定 JDK/Gradle 全量测试数不下降且全部通过。
