# Atomic DDL Log / Undo Marker v1 Design

## 1. 目标与设计依据

本设计实现 CREATE/DROP TABLE 的独立持久 DDL log，并把 undo 设计中的 DDL marker 落成稳定跨模块值对象。
依据：

- `mysql-data-dictionary-ddl-design.md` §7.2、§7.3、§9.5、§11
- `innodb-undo-log-purge-design.md` §6.5、§7.9
- `innodb-crash-recovery-design.md` 的 `RECOVER_DDL` 阶段
- `current-implementation-map.md` 中已接线的 CREATE/DROP lifecycle、catalog discovery 和 orphan cleanup

目标不是把 append-only `mysql.ibd` 伪装成 InnoDB 字典 B+Tree，而是在现有稳定
`InternalCatalogStore` 原子批次上建立可替换的 DDL recovery 证据。

## 2. 当前事实与切片边界

当前 CREATE 已遵循 physical durable → ACTIVE publish，DROP 已遵循
DROP_PENDING → physical drop → DROPPED。恢复能根据 table lifecycle 续作 DROP，并按受控文件名清 orphan，
但不能回答“哪个物理动作属于哪个 DDL”“最后 durable 阶段是什么”，`DdlId` 也无法从 catalog 反向校正。

本切片完成：

- CREATE TABLE / DROP TABLE 的独立 DDL log v1。
- `DdlUndoMarker(ddlOperationId, dictionaryVersion, affectedObjectId)` 稳定 storage API 值对象。
- PREPARED、ENGINE_DONE、DICTIONARY_COMMITTED、COMMITTED、ROLLED_BACK 阶段机。
- CREATE 未提交物理文件的精确 cleanup，DROP 的 finish/rollback 恢复裁决。
- 从 durable DDL log 反向校正 control 的 DDL id high-water。
- 与既有 lifecycle/discovery/orphan cleanup 兼容，旧 catalog 没有 DDL log 仍可恢复。

明确非目标：

- ALTER TABLE、CREATE INDEX、online DDL、SDI、binlog participant。
- 普通用户 transaction 中混入 DDL；DDL 仍位于独立内部事务边界。
- 临时表与 temporary undo tablespace。当前没有临时表 owner/lifecycle，把 `TEMPORARY` 塞进普通 page3、
  history 或 free FIFO 会破坏现有恢复不变量，因此继续 fail-closed。
- 在普通 row undo page 中编码 `DDL_MARKER`。当前 DD 是 sidecar catalog，不通过用户表 DML/rollback segment
  更新；marker 作为每条 durable DDL log record 的公共关联字段。后续 DD B+Tree 化时可复用同一值对象写
  dictionary-row undo，不改变 DDL log 格式。

## 3. 领域模型

`DdlUndoMarker` 位于 `storage.api.ddl`，只含三个正数：

```text
ddlOperationId + dictionaryVersion + affectedObjectId
```

它不携带 DD 类，storage 不反向依赖 `dd`。

`DdlLogRecord` 位于 `dd.ddl`：

```text
marker
operation: CREATE_TABLE | DROP_TABLE
phase: PREPARED | ENGINE_DONE | DICTIONARY_COMMITTED | COMMITTED | ROLLED_BACK
spaceId
absolute normalized controlled path
```

同一 ddl id 的 operation、marker、space 和 path 创建后不可变；后续记录只推进 phase。

## 4. 持久格式

每个阶段使用一个 `InternalCatalogStore.append` 原子批次，批次只含一个 `DDL_LOG(7)` record。
FileInternalCatalogStore 已提供 frame CRC、batch SHA-256、data force → header force，因此 DDL log
repository 不再另造半可靠提交位。

key 复用 DD catalog 的 33-byte big-endian 结构：

```text
[kind=7 u8][operation u64][ddlId u64][dictionaryVersion u64][phase u32][chunk=0 u32]
```

payload v1：

```text
[magic "DDL1" u32][version u8]
[ddlId u64][dictionaryVersion u64][affectedObjectId u64]
[operation u8][phase u8][spaceId u32][pathLength u16][UTF-8 path]
```

key/payload identity 必须逐字段一致；未知 version/code、非法路径长度、尾随字节、同 id identity 漂移、
非法阶段跳转都按 catalog corruption fail-closed。

`DictionaryCatalogCodec` 遇到 DDL_LOG 批次继续返回 empty，不把 marker 误解为字典 version；
DDL log codec 遇到普通 CATALOG_COMMIT 批次同样跳过。两个逻辑仓储共享物理 store，但不共享语义快照。

## 5. 阶段机

CREATE：

```text
PREPARED -> ENGINE_DONE -> DICTIONARY_COMMITTED -> COMMITTED
PREPARED/ENGINE_DONE -> ROLLED_BACK
```

DROP：

```text
PREPARED -> DICTIONARY_COMMITTED -> ENGINE_DONE -> COMMITTED
PREPARED -> ROLLED_BACK
```

COMMITTED/ROLLED_BACK 是终态，禁止再次推进。repository 在 append 前从 durable batches 重建最新状态并执行
expected-phase CAS；append 返回后才发布内存结果。异常返回时调用方不得猜测是否 durable，必须停止 DDL，
由重启重新扫描。

## 6. CREATE 数据流

1. 持 schema/table MDL 完成重复检查，durable 预留 table/index/space/ddl/version。
2. 形成 marker 与受控 path，写 PREPARED；此时尚无用户 tablespace。
3. 执行物理 CREATE/redo durability，写 ENGINE_DONE。
4. 提交 ACTIVE 字典版本，写 DICTIONARY_COMMITTED。
5. 发布 cache，写 COMMITTED。

失败规则：

- PREPARED 前失败：无日志、无文件。
- PREPARED 后物理失败：保留 marker；recovery 精确删除该受控 path 后写 ROLLED_BACK。
- ENGINE_DONE 后字典未提交：同上，不把文件按模糊目录扫描决定归属。
- ACTIVE 已提交但后续 marker 失败：recovery 以 committed DD 为裁决真相，补齐阶段并写 COMMITTED。
- ACTIVE 的 tableId/space/path 与 marker 不一致：阻止 OPEN，不删除任何文件。

## 7. DROP 数据流

1. 持 schema/table MDL 解析 ACTIVE/binding并等待 purge barrier。
2. durable 预留 ddl id 和 pending/dropped versions，写 PREPARED。
3. 建 cache barrier并提交 DROP_PENDING，写 DICTIONARY_COMMITTED。
4. 等旧 pin，执行 physical DROP，写 ENGINE_DONE。
5. 提交 DROPPED，写 COMMITTED。

失败规则：

- 只有 PREPARED 且 DD 仍 ACTIVE：DROP 没有越过字典提交点，recovery 写 ROLLED_BACK。
- DD 为 DROP_PENDING：无论 marker 停在 PREPARED/DICTIONARY_COMMITTED/ENGINE_DONE，都复核 purge barrier，
  幂等完成 physical DROP、发布 DROPPED并补齐阶段。
- DD 已 DROPPED：验证 identity/path 后补写 COMMITTED；文件 residue 仍由受控 cleanup 删除。
- marker 宣称越过提交点但 DD 退回 ACTIVE，或 DD identity 与 marker 不符：视为不可解释损坏并阻止 OPEN。

## 8. 恢复顺序与兼容

公共启动顺序保持：

```text
catalog/control open
-> DD tablespace discovery
-> StorageEngine doublewrite/redo/undo rollback/purge
-> DDL log recovery
-> legacy DROP_PENDING/orphan cleanup
-> Session OPEN
```

DDL log recovery 只解释 durable marker 与 committed DD，不重新解析 SQL。CREATE cleanup 的 path 必须通过
`DictionaryTablespaceDiscovery.checkedPath`，只能位于受控 `tables/` 且匹配当前 marker identity。

旧版本没有任何 DDL log：

- ACTIVE 正常保留。
- DROP_PENDING 继续走既有 lifecycle recovery。
- 受控 orphan 继续走最终兜底扫描。

扫描出的最大 ddl id 参与 `mysql.dd.ctrl` high-water reconciliation。v1 尚未记录 CREATE SCHEMA，
因此 reconciliation 同时使用 `committed dictionary version - 1` 作为保守 DDL identity 下界；DROP 的双版本和
recovery-only version 只会产生安全空洞。即使最新 control 槽损坏回退，后续 DDL 也不会复用已经分配但没有
table marker 的 schema DDL identity。

## 9. 并发与资源边界

- MDL 顺序仍为 schema → table。
- DDL log append 发生在持 MDL 期间，但 catalog store 锁内不进入 storage/page/MDL 回调。
- DDL repository writer lock 只保护 phase CAS + 单次 append，不跨 physical DDL 或 dictionary transaction。
- 恢复期 Session gate 尚未 OPEN；DDL recovery 单线程执行。
- purge barrier 等待不持 catalog/store/file/page lock。
- marker append outcome uncertain 后立即停止，不在同进程执行补偿删除或继续下一阶段。

## 10. TDD 验收

- codec：稳定码、往返、未知版本/code、key/payload mismatch、路径上限、尾随垃圾。
- repository：两条 operation 合法序列、非法跳转、终态不可变、跨重启重建、普通 DD batch 隔离。
- control：损坏最新槽回退后，由 DDL log 最大 id 推进 high-water。
- CREATE crash：PREPARED、ENGINE_DONE、DD committed 后三个边界分别 rollback cleanup 或 finish。
- DROP crash：PREPARED rollback、DROP_PENDING finish、physical drop 后 finish、DROPPED terminalize。
- 兼容：无 DDL log 的旧 DROP_PENDING/orphan recovery 保持通过。
- 资源：purge barrier 前不写 PREPARED；marker 等待/append 不持 page latch。
- 全量 Gradle 回归与 current implementation map/backlog 更新。
