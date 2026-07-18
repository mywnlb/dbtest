# Primary-point UPDATE/DELETE 与 LOB 版本生命周期实现计划

## 1. 目标

按 `mysql-point-update-delete-lob-lifecycle-design.md` 落地完整聚簇主键等值 UPDATE/DELETE，并复用既有
Session、DD lease、table DML、undo/rollback/purge 与 recovery 组合根。实现结束后，公开 SQL 必须支持：

```sql
UPDATE t SET non_pk_column = literal [, ...] WHERE complete_primary_key;
DELETE FROM t WHERE complete_primary_key;
```

UPDATE replacement external LOB 的新链由 rollback 回收，旧链由 committed purge 回收；DELETE 的旧链只由 purge
回收。任何持久进度都不得让 crash recovery 重复释放已经可能复用的页。

## 2. 实施原则

1. 每阶段先增加失败测试，再做最小生产改动。
2. SQL 层只持 exact DD version、typed value 与 opaque transaction handle，不 import storage internal。
3. UPDATE patch 必须在 storage FOR_UPDATE 锁定旧行后合并，禁止 gateway 先 SELECT 再整行替换。
4. 未赋值 external 列复制旧引用；显式赋值的大值创建 replacement 新链。
5. statement/full/recovery rollback 复用同一 undo evidence；purge 只从持久 logical head 逐记录推进。
6. 保留旧构造器和旧 undo EOF/LO/SI 解码兼容，新增能力使用显式 LV tail 和 typed command。

## 3. TDD 阶段

### Phase A — Parser 与 AST

- 新增 `AssignmentNode`、`UpdateStatementNode`、`DeleteStatementNode`。
- 支持多 assignment、AND 等值谓词、可选分号与严格 EOF。
- sealed `StatementNode` 与 Session exhaustive dispatch 同步扩展。
- 测试：合法 UPDATE/DELETE、复合谓词、非法 framing。

状态：完成。

### Phase B — Binder 与 bound contract

- 新增 `BoundUpdate`、`BoundDelete`，直接携带 primary-key values，不复用带 projection/secondary 语义的
  `BoundPointSelect`。
- WHERE 必须精确覆盖完整、无 prefix、非 LOB 的聚簇主键。
- assignment canonical 去重、拒绝主键、完成类型转换后按 DD ordinal 排序。
- WRITE metadata lease 继续由现有 `StatementBindingScope` 发布。
- 测试：复合主键乱序、assignment 排序、重复/缺失/额外谓词、主键赋值、prefix/LOB 主键拒绝。

状态：完成。

### Phase C — 无竞态 storage patch

- 新增 `TableColumnAssignment` 与 `TableUpdatePatchCommand`。
- `TableDmlService.update(patch)` 在同一次 FOR_UPDATE current-read 锁定的旧行上合并 patch。
- `TableUpdateCommand`/`TableDeleteCommand` 增 authoritative optional LOB segment，并保留旧构造器。
- 表级服务向 `ClusteredUpdateCommand`/`ClusteredDeleteCommand` 透传 exact binding。
- 测试：typed patch、miss、主键不可修改、未赋值 external 引用保留。

状态：完成；公开 gateway/E2E 已覆盖 patch、secondary 与 external 引用行为。

### Phase D — Executor 与 Gateway

- `SqlStorageGateway` 增 `update/delete`。
- `DefaultSqlExecutor` exhaustive dispatch 两种写计划并返回 `UpdateResult`。
- `DefaultSqlStorageGateway` 完成 DD→storage key/assignment 转换、statement guard、deadline、rollback-only/fatal
  分类与 `handle.wrote` 发布。
- UPDATE/DELETE 均调用 table-level DML，不旁路 secondary、undo 或 LOB ownership。
- 所有 fake gateway 同步扩展，保持 Session policy 测试可编译。

状态：完成。

### Phase E — LOB ownership、rollback 与 purge

- UPDATE/DELETE undo 使用 LV tail 表达 `rollbackNewValue` / `purgeOldValue`。
- replacement 写入采用 deferred UPDATE undo 和 prepared clustered replace，物理边界后失败保持 fail-stop。
- rollback 在 logical marker 前批量释放 replacement 新链。
- purge 逐 persistent logical head 处理 secondary/clustered task，再在 `PURGE_RECORD_PROGRESS` MTR 中批量释放旧链并
  CAS 到 predecessor；EMPTY 后才允许 finalization。
- recovery 从剩余持久 head 继续，不重复释放 progress 已越过的 ownership。

状态：完成；该阶段的生产实现与测试在本 SQL 接线前已进入工作树，本切片完成公开组合根接线和旧 finalizer 测试协议校正。

### Phase F — Session/E2E

- autocommit UPDATE 同时改变 unique secondary 和 external LOB，旧 secondary 不可见，新 secondary 回表返回新值。
- autocommit DELETE 后 primary/secondary 均不可见。
- 显式事务 UPDATE/DELETE rollback 恢复旧 secondary 与旧 external LOB。
- miss 返回 affectedRows=0；事务终态与 transaction-duration MDL 仍由既有 policy 管理。

状态：完成。

## 4. 验收命令

```text
./gradlew.bat test --tests cn.zhangyis.db.sql.binder.DefaultSqlBinderTest
./gradlew.bat test --tests cn.zhangyis.db.engine.adapter.DefaultSqlStorageGatewayTest
./gradlew.bat test --tests cn.zhangyis.db.engine.DatabaseEngineSessionIntegrationTest
./gradlew.bat test --tests cn.zhangyis.db.storage.api.dml.TableDmlServiceTest
./gradlew.bat test --tests cn.zhangyis.db.storage.api.dml.ClusteredDmlServiceTest
./gradlew.bat test
git diff --check
```

最终验收：JDK 25.0.2 / Gradle 9.5.1，1491 tests，0 failure/error；`git diff --check` 无 whitespace error
（仅工作区 CRLF 转换提示）。

## 5. 完成边界与后续

本计划只完成单行、完整聚簇主键点写。以下仍是独立后续：

- non-unique secondary range scan 与 range UPDATE/DELETE；
- `SELECT ... FOR SHARE/FOR UPDATE` 与 SERIALIZABLE；
- 主键 UPDATE；
- binary JSON / partial LOB update；
- named SAVEPOINT 与 savepoint 后 row-lock 精细释放；
- XA/PREPARED、多 worker/multi-rseg purge。
