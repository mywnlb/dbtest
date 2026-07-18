# Primary-point UPDATE/DELETE 与 LOB 版本生命周期设计

## 1. 文档定位

本文定义主键点 UPDATE/DELETE 从 SQL Session 到表级 DML，以及 external LOB 在 UPDATE、DELETE、rollback、
purge 和 crash recovery 中的完整 ownership 协议。它是
`mysql-primary-point-sql-session-design.md` 后续写路径、`innodb-undo-log-purge-design.md` 和
`innodb-secondary-index-mvcc-purge-design.md` 的交叉落地设计。

实现步骤与验收状态见 `mysql-point-update-delete-lob-lifecycle-implementation-plan.md`；当前设计已于
2026-07-17 完成生产接线。

当前源码已经具备 SQL INSERT、主键/唯一二级点查、表级多索引 UPDATE/DELETE、statement/full/recovery
rollback、secondary-first purge 和 INSERT LOB ownership。本设计只补齐尚未生产接线的 SQL 点写和旧 LOB
版本回收，不重新设计事务、B+Tree、DD 或 Session 状态机。

## 2. 目标与非目标

### 2.1 目标

1. 支持完整聚簇主键等值定位的单行 UPDATE/DELETE。
2. UPDATE 只接受 `column = literal` assignment，禁止修改聚簇主键。
3. SQL 写路径复用 exact DD version、transaction-duration MDL、statement guard、autocommit 和显式事务。
4. external LOB 替换时，新链由 rollback owner，旧链由 committed purge owner。
5. LOB free 与 persistent undo logical-head 推进形成一个 crash-safe redo 原子组。
6. purge 可逐 logical record 持久推进，崩溃后不得重复释放已可能复用的 LOB 页。
7. 保留旧 undo EOF/SI 编码和既有整行 storage UPDATE API。

### 2.2 非目标

1. 主键 UPDATE、唯一二级点写、范围 UPDATE/DELETE、ORDER BY/LIMIT、多表写。
2. locking/range SELECT、optimizer、prepared statement、网络协议和临时 undo。
3. MySQL changed-row 精确语义；v1 命中行即返回 affectedRows=1。
4. 比较新旧完整 LOB 内容以消除同值写；显式赋值的大值按 replacement 处理。
5. 在线升级 undo page format、多 rseg 或多 undo tablespace。

## 3. SQL 语法与绑定

支持语法：

```sql
UPDATE [schema.]table
SET column = literal [, column = literal ...]
WHERE pk_column = literal [AND pk_column = literal ...]

DELETE FROM [schema.]table
WHERE pk_column = literal [AND pk_column = literal ...]
```

Parser 只负责生成 assignment 和 equality predicate AST；以下约束由 Binder 基于 exact DD version 校验：

1. WHERE 列集合必须与完整、无 prefix 的聚簇主键列集合完全一致。
2. predicate、assignment 均不得重复；UPDATE assignment 不得为空。
3. UPDATE 不允许赋值任何聚簇主键列，即使字面量与旧值相同。
4. 所有字面量通过现有 `SqlTypeCoercion` 转换；主键 NULL 和非空列 NULL 继续由类型系统拒绝。
5. Bound assignment 按 column ordinal 排序；主键值按 primary index key-part 顺序排列。

Session 把 UPDATE/DELETE 视为 `readOnly=false` 的 data statement。Binder 获取 WRITE metadata lease，Executor
只做 exhaustive dispatch，Storage Gateway 继续拥有 transaction handle、statement guard 和内部物理类型映射。

## 4. Storage API 与 current-read

现有 `TableUpdateCommand` 保留整行替换语义，增加可选 LOB segment 并保留旧构造器。SQL 使用新增的
`TableUpdatePatchCommand`：

- `transaction`：ACTIVE transaction。
- `metadata`：同一 exact schema version 的表级索引聚合。
- `clusterKey`：完整聚簇主键。
- `assignments`：按 ordinal 严格递增的 `TableColumnAssignment`。
- `lobSegment`：来自同一 DD table binding 的可选 authoritative segment。
- `lockWaitTimeout`：所有事务锁和 row guard 的有界预算。

`TableColumnAssignment` 只接受完整逻辑 `ColumnValue`，拒绝 `ExternalValue`，防止上层注入物理 ownership。
整行兼容入口若携带 `ExternalValue`，该值必须与锁定旧行同 ordinal 的 external envelope 完全相同；其它 external
引用视为伪造 ownership。

表级 UPDATE 数据流固定为：

1. FOR_UPDATE current-read 获取事务行锁并物化旧完整行；等待期间不持 page latch、MTR 或 row guard。
2. Patch 在锁定旧行上生成目标完整逻辑行；整行入口直接使用调用方目标行。
3. 从旧/新完整行规划 secondary key 变化和 logical unique 检查。
4. 所有可能阻塞的事务锁结束后取得 purge/DML row guard。
5. 聚簇首 MTR 写 undo、新版本和新 LOB；之后每个 secondary 仍使用独立短 MTR。
6. 任一 secondary 失败由 statement rollback 沿同一 undo record 收敛。

DELETE 沿用相同 current-read、行锁和 row guard 边界，只从锁定旧行投影 secondary entry 和 LOB purge ownership。

## 5. LOB replacement 状态机

只有 TEXT/BLOB/JSON family 使用该协议，inline 判定继续由现有 `LobStorage.requiresExternalization` 决定。

| 旧值 | 新值 | Rollback 释放 | Committed purge 释放 |
| --- | --- | --- | --- |
| inline/NULL | inline/NULL | 无 | 无 |
| inline/NULL | external | 新链 | 无 |
| external | external | 新链 | 旧链 |
| external | inline/NULL | 无 | 旧链 |
| external 未赋值 | 同一旧引用 | 无 | 无 |
| DELETE external | 无新值 | 无 | 旧链 |

显式赋值的大 LOB 即使内容与旧值相同也创建新链。未赋值列直接复制旧 `ExternalValue`，不得 hydrate 后重写。

任何需要写新链或回收旧链的计划都必须携带 authoritative LOB segment。reference 自带的 space/segment 仅用于
一致性校验，不能作为释放授权。

## 6. UPDATE prepared 物理边界

UPDATE 新 external reference 在实际分配前未知，因此引入 `DeferredUpdateUndoPlan`。placeholder reference 的类型、
page count、总长度、CRC 和 inline prefix 与实际计划一致，只使用不可能发布为真实引用的占位 page number。

业务写开始前必须完成：

1. 全部列 codec dry validation。
2. LOB externalization 计划、segment identity/purpose 短 MTR 预检。
3. old/new ownership 列表、secondary tail 和 undo encoded length 冻结。
4. undo root/payload、B+Tree point rewrite 和全部新 LOB 页的 redo workload admission。

业务 MTR 顺序固定为：

1. prepare undo append，冻结 slot/segment/record placement。
2. prepare clustered replace，固定目标页、槽位和旧隐藏列 CAS 证据。
3. 在 index prepare guard 存活期间写全部计划内 LOB 页链。
4. 用真实 external envelope 完成 actual UPDATE undo。
5. 发布带新 DB_TRX_ID/DB_ROLL_PTR 的聚簇版本。
6. 转移 LOB allocation ownership，关闭 prepared guards 后提交 MTR。

prepare 之后的异常必须逆序关闭 allocation、clustered replace 和 undo guards。由于 MTR 没有 buffer content undo，
越过物理 prepare 边界的失败统一按 `UndoWriteFatalException` fail-stop，不能在同一进程继续事务。

DELETE 不分配新 LOB，旧 external ownership 可直接进入普通 planned DELETE undo。

## 7. Undo LOB version tail

`UndoRecord` 增加 `lobVersionOwnerships`，元素为：

- `columnOrdinal`：schema ordinal，严格递增且唯一。
- `purgeOldValue`：旧 image 的 external value 在 committed purge 时释放。
- `rollbackNewValue`：可选的新 external envelope，在 rollback marker 时释放。

至少一个动作必须存在。`purgeOldValue=true` 时 `oldColumnValues[columnOrdinal]` 必须为 schema 类型匹配的
`ExternalValue`；DELETE 不允许 `rollbackNewValue`。INSERT 继续只允许 `insertedLobs`，不能携带 LV tail。

编码顺序：

- INSERT：fixed body → optional `LO/v1` → optional `SI/v1`。
- UPDATE/DELETE：fixed body + old image → optional `LV/v1` → optional `SI/v1`。

`LV/v1` 使用独立 magic，单项编码 ordinal、flags 和可选 external envelope。空列表不写 magic；未知版本/flag、
空 tail、重复 ordinal、inline value、类型错配、截断或尾随垃圾均作为 `UndoLogFormatException` fail-closed。
旧 UPDATE/DELETE 的 EOF 或直接 SI tail 继续可读，不升级 undo page format。

## 8. 批量 LOB free

逐 ownership 调用 `LobStorage.free` 会在后续链损坏时留下前面链已经修改的 buffer 内容，因此 rollback/purge 必须
使用批量协议。

`LobFreeBatchPlan` 按 authoritative segment 分组并冻结全部 `LobFreeTarget`。在第一次 FSP 写之前必须：

1. 校验每个 external envelope 的 schema type 和 segment identity。
2. 完整读取全部 chain，校验 page type、owner、link、长度和 CRC。
3. 校验整批 page id 全局不重复，拒绝重叠或重复 ownership。
4. 计算所有 page free 的动态 redo workload并完成 admission。
5. 在写 MTR 中再次完成整批只读验证，之后才允许释放第一张页。

多个 segment 的 batch 按稳定 SegmentRef/PageId 顺序执行。first-page X latch 后访问 LOB/FSP 时复用现有显式
latch-order scope，并记录“LOB/FSP 不等待 undo page”的单向约束。

越过首次 FSP/PAGE_INIT 修改后发生的异常是 fail-stop；`rollbackUncommitted` 只释放 latch/fix/memo，不宣称恢复页内容。

## 9. Rollback ownership

每条记录继续按 secondary inverse → clustered inverse → marker 执行。marker 前用独立短读完成：

1. expected logical head 与当前 record identity 校验。
2. predecessor pointer、undoNo 单调性和 predecessor exact metadata 解析。
3. authoritative LOB segment 解析及完整 free batch 预检。
4. marker redo workload admission。

marker MTR 先按 PageId 固定一个或两个 undo first page X latch，再批量释放 INSERT `insertedLobs` 和 UPDATE
`rollbackNewValue`，最后 CAS 写入 predecessor head。free 与 logical-head 修改进入同一 redo batch。

clustered inverse 后、marker 前崩溃时，新 LOB 仍存在且 head 未推进；恢复可幂等重做 inverse。marker durable 后，
head 已越过该 record，任何恢复路径都不会再次释放该链。

## 10. Committed purge progress

现有 purge 从 history head 读取整条 logical chain 并在全部任务完成后 finalization。LOB free 本身不可幂等，因此改为
逐 persistent logical head 推进：

1. 短 MTR 读取当前 head record、exact target 和 predecessor。
2. 零等待取得该行 purge row guard；busy 时不改变 head。
3. UPDATE/DELETE 的 secondary task 继续做版本链安全证明并幂等删除。
4. DELETE 的 clustered physical remove 固定在全部 secondary 之后。
5. 短读预检旧 LOB free batch 和 logical-head transition。
6. `PURGE_RECORD_PROGRESS` MTR 固定 undo first page X，重复核对 expected head，批量释放旧 LOB，CAS 到 predecessor。
7. progress commit 后通知 `AFTER_RECORD_PROGRESS_COMMIT` crash hook，再处理下一 record。
8. logical head 为空后才进入现有 history/slot/segment finalization。

同一 record 的 secondary、clustered 和 progress 均处于同一 row guard 生命周期。各 B+Tree 写仍是独立短 MTR，
LOB free 与 head advance 是另一个短 MTR，不把多棵树和 FSP 页同时固定。

`UndoSegmentFinalizer.preparePurge` 必须独立校验 persistent logical head 为 EMPTY，不能只信任 coordinator 调用纪律。

## 11. Crash 与恢复语义

| 崩溃位置 | 持久结果 | 恢复动作 |
| --- | --- | --- |
| secondary/clustered task 前 | head 未变 | 正常执行全部任务 |
| task commit 后、progress 前 | 索引可能已删，head 未变 | ABSENT/stale 幂等重做，再执行 progress |
| progress commit 前 | free/head 均无 durable 结果 | 从同一 head 重试 |
| progress commit 后 | LOB 已 free 且 head 已越过 | 从 predecessor 继续，不重复 free |
| EMPTY 后、finalization 前 | COMMITTED owner 仍在 history | recovery 重建空 affected-table 集合并直接 finalization |
| finalization commit 后 | page3/history owner 已转移 | 沿现有 finalization recovery 权威收敛 |

运行期 `HistoryEntry.affectedTableIds` 在整条 log finalization 前保持原集合，允许保守阻塞 DROP。重启只从剩余
logical chain 重建集合；已经 durable progress 的 table 不再需要 barrier。EMPTY committed history node 不是损坏，
仍必须恢复并交给 purge finalizer。

## 12. 并发、锁序与失败分类

1. Session/MDL → transaction row lock → purge/DML row guard → 单个短 MTR。
2. 进入事务锁等待前不得持 row guard、page latch、buffer fix、LOB/FSP latch 或 MTR。
3. purge row guard 使用 zero-wait；busy 仅延后，不进入事务 wait-for graph。
4. UPDATE prepared MTR 使用 index prepare → LOB/FSP 的既有单向局部顺序。
5. rollback/purge marker 使用 undo first-page → LOB/FSP 的显式例外 scope；LOB/FSP 永不反等 undo page。
6. codec、DD、segment、chain、predecessor 和 redo admission 错误必须在物理边界前排除。
7. 物理边界后的不确定失败使用项目 fatal exception，发布 Engine fail-close。

## 13. 测试与验收

1. Parser/Binder：合法单/复合主键，重复/缺失/额外 predicate，主键 assignment，非法 shape。
2. Codec：旧 EOF、旧 SI、新 LV、LV+SI、unknown flag/version、ordinal/type/length corruption。
3. DML：所有 inline/external 转换、多列 LOB、未赋值 external 复用、二级变键和 miss。
4. Rollback：statement/full/recovery rollback，只释放新链，旧 ReadView 仍可 hydrate。
5. Purge：ReadView horizon、UPDATE old LOB、DELETE secondary-first、批量链重叠损坏。
6. Crash：secondary、clustered、progress、finalization 每个 durable 边界及二次启动。
7. Recovery：partially advanced head、EMPTY committed head、affected-table DROP barrier。
8. Session：autocommit、显式事务、rollback-only、锁超时和完整 SQL 结果。
9. 固定 JDK 25/Gradle 9.5.1 全量测试，测试数不得倒退；复核无 monitor、裸 RuntimeException 和越层 import。

## 14. 当前实现地图更新要求

完成后只更新 `current-implementation-map.md` 中 SQL gateway、table DML、LOB ownership、undo/rollback、purge、
recovery 和 gaps 小节，并按文件内检查清单从生产源码复核调用链。`storage-backlog.md` 将 SQL UPDATE/DELETE 与
LOB replacement ownership/purge 标记完成；目标架构依赖方向未变化，不更新全局架构图。
