# Primary-Point SQL / Session Production Wiring Implementation Plan

版本：2026-07-16

> 本计划按用户明确要求持久化，配套 `mysql-primary-point-sql-session-design.md`。它不是当前实现状态；
> 每个任务完成后仍必须从生产源码核对调用链，不能按勾选项推断功能已接线。

**Goal:** 实现进程内 Session、单行 INSERT、聚簇主键一致性点查、完整 autocommit/事务控制、28 类型绑定，
并为 external LOB INSERT 建立可恢复的 ownership 和 rollback 协议。

**Architecture:** SQL/session 只依赖自身 outbound `SqlStorageGateway` 与 DD lease；
`engine.adapter.DefaultSqlStorageGateway` 把 pinned `TableDefinition` 映射到真实 transaction/B+Tree/record/LOB 服务。
LOB、INSERT undo 和聚簇行在同一业务 MTR 提交；rollback 用“删除行 MTR → free LOB + logical-head marker MTR”推进。

**Tech Stack:** Java 25、Gradle 9.5.1、JUnit Jupiter、现有 DD/storage/transaction/redo/undo 组件、
显式 `java.util.concurrent` 工具、UTF-8。

## Global Constraints

- 开始和每次继续实施前先读 `AGENTS.md`、本设计、`current-implementation-map.md`，再只读当前任务相关厚设计。
- 禁止 GitNexus；调用链、blast radius 和状态全部用 `rg`、源码、测试、Git 核对。
- 不使用 `synchronized`、`wait`、`notify`、`notifyAll`，不引入全局大锁或无界等待。
- 生产代码不抛裸 `IllegalArgumentException`/`RuntimeException`；包装异常保留 cause。
- SQL/session 不 import 任何 `cn.zhangyis.db.storage` 类型；storage 不 import SQL/session。所有 storage 映射只在
  `engine.adapter` 实现。
- 页修改、undo、LOB、rollback、WAL、锁顺序和状态方法写完整中文 Javadoc/字段说明/数据流注释。
- MTR rollback 不撤销 page content；任何 post-mutation 失败必须按设计补偿或 fail-stop，测试不能只覆盖 happy path。
- 编辑 `UndoRecord`、`TableStorageBinding`、`DatabaseEngine` 等高扇出类型前先报告直接调用方和风险，再修改。
- 每个任务先写测试并观察预期 RED，再做最小实现到 GREEN；不删除或弱化既有断言换取通过。
- 固定命令：`$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" ...`。
- 不提交 build 输出、临时数据库文件或 IDE 文件；不覆盖工作区中不属于本计划的改动。

## Delivery Boundaries

| 阶段 | 任务 | 可独立验收结果 |
| --- | --- | --- |
| A：事务化 LOB prerequisite | 0–6 | 新表有 LOB segment；LOB-aware INSERT、undo ownership、live/recovery rollback 闭环。 |
| B：SQL binding/execution | 7–10 | Parser/Binder/Executor/Gateway 可在显式 transaction handle 上执行 INSERT/point SELECT。 |
| C：Session production wiring | 11–13 | autocommit/BEGIN/COMMIT/ROLLBACK、engine lifecycle、并发与恢复集成闭环。 |
| D：收尾 | 14 | current map/backlog、静态检查、全量回归和测试数复核完成。 |

阶段 A、B、C 分别允许形成提交，但任何阶段失败不得把 backlog 2.8 标为完成。

---

## Task 0: Baseline、Blast Radius 与测试计数

**Files:** 无生产修改。

**Interfaces:** 记录实施开始时的代码事实，禁止从旧 plan 推断现状。

- [x] **Step 1: 检查工作区与最近提交**

运行：

```powershell
git status --short --branch
git log -5 --oneline
```

把未提交/未跟踪文件按“本计划、用户已有、无关”分类；不清理用户改动。

- [x] **Step 2: 扫描高扇出调用点并向用户报告**

```powershell
rg -n "new UndoRecord|UndoRecord\.insert|insertedLobs|new TableStorageBinding|openTable\(|new DatabaseEngine|DmlRollbackCommand" src/main/java src/test/java
rg -n "UndoRecordCodec|UndoWritePlan|planInsert|appendPlanned|applyUndoRecord|persistLogicalHead" src/main/java src/test/java
```

报告至少包括：直接构造点、codec/recovery/purge/MVCC 消费者、磁盘兼容风险、全量回归要求。

- [x] **Step 3: 固定基线测试数**

```powershell
$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"
& "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --rerun-tasks
```

记录 suites/tests/failures/errors/skips；如果基线失败，先区分既有失败与环境失败，不进入格式修改。

---

## Task 1: DD READ/WRITE Access Intent 与精确版本 Mapper

**Files:**

- Create: `src/main/java/cn/zhangyis/db/dd/service/TableAccessIntent.java`
- Create: `src/main/java/cn/zhangyis/db/engine/adapter/DictionaryStorageMetadataMapper.java`
- Create: `src/main/java/cn/zhangyis/db/engine/adapter/MappedTableStorage.java`
- Modify: `src/main/java/cn/zhangyis/db/dd/service/DataDictionaryService.java`
- Modify: `src/main/java/cn/zhangyis/db/engine/adapter/DictionaryIndexMetadataResolver.java`
- Test: `src/test/java/cn/zhangyis/db/dd/service/DataDictionaryServiceTest.java`
- Test: `src/test/java/cn/zhangyis/db/engine/adapter/DictionaryStorageMetadataMapperTest.java`

**Interfaces:**

- Produces: `openTable(owner, name, intent, timeout)`；删除三参数隐式 READ 入口并更新全部调用点，访问意图不得靠默认值隐藏。
- Produces: 从调用方提供的精确 `TableDefinition` 映射聚簇/指定索引、schema 和当前物理 binding；LOB segment 映射
  在 Task 2 扩展，rollback target resolver 在 Task 6 建立，保证本 Task 可独立编译与 GREEN。
- Consumes: 既有 DD immutable aggregate、`BTreeIndexMetadataFactory` 和 storage DTO。

- [x] **Step 1: 写 Access Intent RED 测试**

覆盖：SELECT/READ 取得 schema SR + table SR；INSERT/WRITE 取得 schema SR + table SW；非法 null intent 拒绝；
获取第二张锁失败时反序释放第一张；DDL X 在 lease close 前等待。

- [x] **Step 2: 写 Mapper RED 测试**

构造两个 version 的同表定义，断言 mapper 严格使用传入 version/root/binding，不回 repository 读取最新版本；
缺 binding、缺 index、DROPPED、binding identity 不一致均抛 DD/storage mapping 领域异常；不同 table version 必须
严格映射各自携带的 root/binding，不能回 repository 偷读最新版本。

- [x] **Step 3: 运行定向测试确认 RED**

```powershell
& "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "cn.zhangyis.db.dd.service.DataDictionaryServiceTest" --tests "cn.zhangyis.db.engine.adapter.DictionaryStorageMetadataMapperTest"
```

- [x] **Step 4: 实现 intent 与 mapper**

`TableAccessIntent.READ/WRITE` 唯一负责 MdlMode 映射。mapper 复用当前 resolver 中 column/index/type 显式映射；
`DictionaryIndexMetadataResolver` 改为 repository 定位后调用 mapper，recovery 行为不变。禁止让 storage.api import DD。

- [x] **Step 5: 定向 GREEN 与 DD/engine 回归**

运行上述测试以及 `DataDictionaryDomainTest`、`DatabaseEngineTest`。

---

## Task 2: Table LOB Segment 与 Catalog 向后兼容

**Files:**

- Modify: `src/main/java/cn/zhangyis/db/storage/api/ddl/TableStorageBinding.java`
- Modify: `src/main/java/cn/zhangyis/db/storage/api/ddl/TableDdlStorageService.java`
- Modify: `src/main/java/cn/zhangyis/db/dd/repo/DictionaryCatalogCodec.java`
- Modify: `src/main/java/cn/zhangyis/db/engine/adapter/DictionaryStorageMetadataMapper.java`
- Test: `src/test/java/cn/zhangyis/db/storage/api/ddl/TableDdlStorageServiceTest.java`
- Test: `src/test/java/cn/zhangyis/db/dd/domain/DataDictionaryDomainTest.java`
- Test: `src/test/java/cn/zhangyis/db/dd/repo/PersistentDictionaryRepositoryTest.java`

**Interfaces:** `TableStorageBinding(..., indexes, Optional<SegmentRef> lobSegment)`；更新所有源码构造点，不保留无必要的
四参数兼容构造器；磁盘兼容只由 codec 明确处理。

- [x] **Step 1: 写 binding/DDL RED 测试**

覆盖：含任一 TEXT/BLOB/JSON 列只创建一个 `SegmentPurpose.LOB`；普通表不创建；LOB segment 必须属于同一 space、
不能与 index segment 重复；CREATE redo budget 包含额外 segment；DROP 仍删除整个 space。

- [x] **Step 2: 写 catalog golden/compatibility RED 测试**

固定旧 table payload bytes，解码得到 empty LOB segment；新 payload round-trip 保留 segment；未知/截断/尾随尾部
fail-closed。旧 LOB 表 binding empty 允许 catalog open，但 mapper 标记 externalization unavailable。

- [x] **Step 3: 运行定向测试确认 RED**

```powershell
& "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "cn.zhangyis.db.storage.api.ddl.TableDdlStorageServiceTest" --tests "cn.zhangyis.db.dd.*"
```

- [x] **Step 4: 实现 CREATE 与 codec 尾部**

所有 schema 派生和 segment 数预算在建文件/MTR 前完成；LOB segment 与 index segments 在同一 CREATE MTR 创建。
decoder 只以 EOF 判断旧 payload，不靠 catalog version 猜测；新尾部之后继续要求完全消费。

- [x] **Step 5: 定向 GREEN 与真实 reopen 测试**

增加临时目录 create→close→reopen→mapper 读取 LOB binding 测试，随后运行 DD/DDL/DatabaseEngine 相关 suites。

---

## Task 3: INSERT Undo LOB Ownership 值对象与磁盘格式

**Files:**

- Create: `src/main/java/cn/zhangyis/db/storage/undo/InsertedLobOwnership.java`
- Modify: `src/main/java/cn/zhangyis/db/storage/undo/UndoRecord.java`
- Modify: `src/main/java/cn/zhangyis/db/storage/undo/UndoRecordCodec.java`
- Modify: `src/main/java/cn/zhangyis/db/storage/undo/UndoRecordWritePlan.java`
- Modify: 所有 `UndoRecord` 直接构造/工厂调用点
- Test: `src/test/java/cn/zhangyis/db/storage/undo/UndoRecordTest.java`
- Test: `src/test/java/cn/zhangyis/db/storage/undo/UndoRecordCodecTest.java`

**Interfaces:** `UndoRecord` 增加 immutable `insertedLobs`；INSERT 可非空，UPDATE/DELETE 强制 empty；固定前缀 identity 不变。

- [x] **Step 1: 再次执行并报告 UndoRecord blast radius**

在真正编辑 record component 前保存 `rg` 结果；特别核对 MVCC、purge、rollback、recovery、extern undo payload 和测试 fixture。

- [x] **Step 2: 写领域不变量 RED 测试**

覆盖：ordinal 非负、递增唯一、只允许 INSERT、value 必须 ExternalValue、defensive copy、空 ownership 与旧行为一致。
column schema/type 匹配放在 codec 测试，因为 `UndoRecord` 本身不持 TableSchema。

- [x] **Step 3: 写 codec RED/golden 测试**

覆盖：旧 INSERT bytes 解码 empty；新单/多 ownership round-trip；magic/version/count/ordinal/length 损坏；
UPDATE/DELETE 旧 golden bytes 不变；`peekIdentity` 对新尾部结果不变。

- [x] **Step 4: 实现兼容尾部**

external bytes 必须复用 schema 列的 `LobCodec`，不另写 LobReference codec。key 后 EOF 是唯一旧格式；有字节时必须完整
解析新 magic/version，任何模糊状态按 `UndoLogFormatException` 处理。

- [x] **Step 5: 定向 GREEN 与 Undo/MVCC/Purge 回归**

```powershell
& "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "cn.zhangyis.db.storage.undo.*" --tests "cn.zhangyis.db.storage.trx.MvccReaderTest" --tests "cn.zhangyis.db.storage.trx.PurgeCoordinatorTest"
```

---

## Task 4: 可冻结 LobWritePlan 与失败补偿 Guard

**Files:**

- Create: `src/main/java/cn/zhangyis/db/storage/api/lob/LobWritePlan.java`
- Create: `src/main/java/cn/zhangyis/db/storage/api/lob/LobWriteAllocation.java`
- Modify: `src/main/java/cn/zhangyis/db/storage/api/lob/LobStorage.java`
- Test: `src/test/java/cn/zhangyis/db/storage/api/lob/LobStorageTest.java`

**Interfaces:**

- `planWrite(segment, type, rawValue)`：无 IO，冻结 payload copy、长度、页数、CRC、prefix 和 workload。
- `writePlanned(mtr, plan)`：返回持有新 allocation 的 Guard；`value()` 给出 ExternalValue，`transferOwnership()` 后 close 不回收。
- 现有 `write` 可委托新 API 并立即 transfer，保持低层测试入口。

- [x] **Step 1: 写纯计划 RED 测试**

断言相同输入计划确定、payload 防御性复制、workload/pageCount 精确、inline/空/错误类型拒绝。纯计划阶段不读取
FSP inode，segment purpose 在 `writePlanned` 开始任何 reserve/allocate 前复核为 LOB。

- [x] **Step 2: 写 allocation guard RED 测试**

覆盖：未 transfer 的 close 在同一 active MTR 反序回收全部新页；transfer 后保留；多 allocation 后一项失败时前项可补偿；
重复 close 幂等；MTR 非 ACTIVE 时拒绝伪补偿并保留根因。

- [x] **Step 3: 实现 plan/write/guard**

Guard 由创建线程和 MTR 独占，不使用隐式 monitor。`writePlanned` 复核实际 pageCount/CRC/prefix 与计划；自有分配失败沿用
现有 partial reclaim。补偿接口只处理当前 MTR 新 allocation，不允许释放已发布 record 的普通 LOB。

- [x] **Step 4: 定向 GREEN 与 FSP/redo 回归**

运行 `LobStorageTest`、`LobCodecTest`、相关 FSP allocation 和 redo budget suites。

---

## Task 5: Deferred INSERT Undo Plan 与 LOB-aware DML

**Files:**

- Create: `src/main/java/cn/zhangyis/db/storage/trx/DeferredInsertUndoPlan.java`
- Modify: `src/main/java/cn/zhangyis/db/storage/trx/UndoLogManager.java`
- Modify: `src/main/java/cn/zhangyis/db/storage/trx/UndoWritePlan.java`
- Modify: `src/main/java/cn/zhangyis/db/storage/undo/UndoRecordWritePlan.java`
- Modify: `src/main/java/cn/zhangyis/db/storage/undo/UndoLogSegment.java`
- Create: `src/main/java/cn/zhangyis/db/storage/trx/PreparedUndoAppend.java`
- Create: `src/main/java/cn/zhangyis/db/storage/btree/PreparedClusteredInsert.java`
- Modify: `src/main/java/cn/zhangyis/db/storage/btree/SplitCapableBTreeIndexService.java`
- Modify: `src/main/java/cn/zhangyis/db/storage/api/dml/ClusteredInsertCommand.java`
- Modify: `src/main/java/cn/zhangyis/db/storage/api/dml/DmlRedoBudgetEstimator.java`
- Modify: `src/main/java/cn/zhangyis/db/storage/api/dml/ClusteredDmlService.java`
- Test: `src/test/java/cn/zhangyis/db/storage/trx/UndoLogManagerTest.java`
- Test: `src/test/java/cn/zhangyis/db/storage/api/dml/ClusteredDmlServiceTest.java`

**Interfaces:** `ClusteredInsertCommand` 增加可选 LOB segment；`planDeferredInsert` 冻结 exact shape/location/snapshots；
`prepareUndoAppend` 在低位 undo space 预留并固定全部 root/payload 页但不写 placeholder record；
`prepareClusteredInsert` 以定长 placeholder row 固定导航/SMO 资源但不发布行；两个 prepare handle 都由同一 MTR 独占。

- [x] **Step 1: 报告 ClusteredInsertCommand/planInsert blast radius**

扫描所有命令构造、DML fixture、redo estimator 和 engine integration tests；修改全部调用点，不用 nullable 兼容字段。

- [x] **Step 2: 写 deferred plan RED 测试**

覆盖：placeholder 与实际不同 pageNo 但 encoded shape 相同可经 prepared handle append；数量/ordinal/type/length/identity
漂移拒绝；计划后 undo head/slot/page snapshot 变化拒绝；external undo payload 的全部页在 B+Tree/LOB latch 前固定且预算准确。

- [x] **Step 3: 写 DML RED 测试**

覆盖：全部 inline 不建 LOB；阈值+1 自动 externalize；同一行多个 LOB；无 segment/错误 purpose 在业务 MTR 前拒绝且
无页/undo/row 副作用；combined workload 在 begin 前 admission；undo prepare → B+Tree prepare → LOB 的锁序并发测试；
prepare 物理边界后的任意失败即使完成 LOB 补偿也抛 fatal。

- [x] **Step 4: 实现单 MTR 流**

严格按 `prepare undo pages/owner → prepare B+Tree insert → LOB allocate/format → actual undo append → row publish → commit`。
prepare handle 必须在 close 时验证已 publish 或处于 fatal 收尾；B+Tree prepare 后访问 FSP 的越序 scope 必须写明“所有
LOB-aware writer 均先持 index prepare guard、普通 SMO 同样 index→FSP、读者不回等 FSP”的无环证明。禁止在异常时
扩大 redo budget 重试。

- [x] **Step 5: 定向 GREEN 与既有 DML 全回归**

运行 UndoLogManager、ClusteredDmlService、statement guard/savepoint、B+Tree current-read 和 redo admission tests。

---

## Task 6: LOB Rollback、Logical Head 原子推进与 Recovery

**Files:**

- Modify: `src/main/java/cn/zhangyis/db/storage/trx/RollbackService.java`
- Create: `src/main/java/cn/zhangyis/db/storage/trx/UndoTargetMetadata.java`
- Create: `src/main/java/cn/zhangyis/db/storage/trx/UndoTargetMetadataResolver.java`
- Modify: `src/main/java/cn/zhangyis/db/engine/adapter/DictionaryIndexMetadataResolver.java`
- Create: `src/main/java/cn/zhangyis/db/storage/api/dml/ResolvedDmlRollbackCommand.java`
- Modify: `src/main/java/cn/zhangyis/db/storage/api/dml/ClusteredDmlService.java`
- Modify: `src/main/java/cn/zhangyis/db/storage/engine/StorageEngine.java`
- Test: `src/test/java/cn/zhangyis/db/storage/trx/RollbackServiceTest.java`
- Test: `src/test/java/cn/zhangyis/db/storage/recovery/CrashRecoveryServiceTest.java`
- Test: `src/test/java/cn/zhangyis/db/storage/api/dml/ClusteredDmlServiceTest.java`

**Interfaces:** resolver 模式新增无 fallback index 的 full rollback；resolver 返回 BTreeIndex 与 authoritative LOB segment；
INSERT inverse 返回 ownership work；LOB free 与所属 logical-head CAS 在同一 marker MTR。

- [x] **Step 1: 写 resolver rollback RED 测试**

一个事务向两张表写入后，不提供“最后 index”也能逐 undo identity 解析并完整回滚；resolver 缺失时新 API fail-closed；
无 undo 事务直接进入 ROLLED_BACK。篡改 ownership 指向另一合法 LOB segment 时必须在 free 前因 DD binding mismatch 拒绝，
不得释放被指向 segment 的任何页。

- [x] **Step 2: 写两 MTR 协议 RED 测试**

覆盖：A 删除 row，B 同时 free 全部 ownership + 推 head；A 后故障重试；B commit 前故障；B durable 后模拟响应失败并重启；
row 已不存在时仍执行 B；ownership 损坏不推进 marker。

- [x] **Step 3: 写 statement/full/recovery 三入口协作测试**

三种入口必须共用同一 free+marker 方法。statement rollback 后事务保持 ACTIVE；full/recovery 到 EMPTY 后仍走既有
atomic finalization。超出单 marker MTR budget 在副作用前拒绝并标 rollback-only/fail-stop。

- [x] **Step 4: 实现 rollback 重构**

undo 短读后释放 latch，再执行 inverse MTR A；只有 INSERT ownership 非空时，从 resolver target 取得并校验权威
LOB segment。marker MTR B 必须先固定 expected undo first-page X guard，再执行 LOB 校验/free，最后通过同一 guard
写 logical head；预算合并 `LobStorage.freeWorkload` 与 logical-head redo。B commit 后才发布内存 head。
UPDATE/DELETE 和无 LOB INSERT 保持既有行为与预算。

- [x] **Step 5: 定向 GREEN 与真实 restart**

使用临时目录关闭第一套对象、重新打开 redo/data 执行 recovery，断言 record、FSP allocation、LOB chain、undo head、slot
全部收敛；随后运行 rollback/recovery/undo/dml 全组测试。

**Stage A checkpoint:** 运行全量测试。只有全绿且测试数不下降，才允许提交“transactional LOB insert ownership”阶段。

---

## Task 7: SQL 公共值、结果与异常

**Files:**

- Create: `src/main/java/cn/zhangyis/db/sql/executor/SqlExecutionResult.java`
- Create: `src/main/java/cn/zhangyis/db/sql/executor/QueryResult.java`
- Create: `src/main/java/cn/zhangyis/db/sql/executor/UpdateResult.java`
- Create: `src/main/java/cn/zhangyis/db/sql/executor/CommandResult.java`
- Create: `src/main/java/cn/zhangyis/db/sql/executor/SqlValue.java`
- Create: `src/main/java/cn/zhangyis/db/sql/executor/SqlRow.java`
- Create: `src/main/java/cn/zhangyis/db/sql/executor/ResultColumn.java`
- Create: `src/main/java/cn/zhangyis/db/sql/executor/TransactionStatus.java`
- Create: executor exception classes under `sql.executor.exception`
- Test: `src/test/java/cn/zhangyis/db/sql/executor/SqlExecutionModelTest.java`

**Interfaces:** 公开值不含 storage types；bytes/bit 防御性复制；result list immutable。

- [x] **Step 1: 写模型不变量 RED 测试**

覆盖 null、重复 result column、row width mismatch、防御性复制、sealed variant、完整 unsigned BIGINT 的 `BigInteger`
投影和 transaction status 合法组合。

- [x] **Step 2: 实现最小公共模型**

禁止为了实现方便把 `Transaction`/`LogicalRecord` 放进 public result/value。异常全部继承项目层次。

- [x] **Step 3: 运行 package 定向测试与依赖静态扫描**

```powershell
rg -n "cn\.zhangyis\.db\.storage" src/main/java/cn/zhangyis/db/sql src/main/java/cn/zhangyis/db/session
```

Expected: 0 matches。

---

## Task 8: Lexer、Parser 与 AST

**Files:**

- Create: lexer/token/source-position types under `src/main/java/cn/zhangyis/db/sql/parser`
- Create: AST sealed hierarchy under `src/main/java/cn/zhangyis/db/sql/parser/ast`
- Create: `src/main/java/cn/zhangyis/db/sql/parser/DefaultSqlParser.java`
- Create: parser exceptions under `src/main/java/cn/zhangyis/db/sql/parser/exception`
- Test: `src/test/java/cn/zhangyis/db/sql/parser/DefaultSqlParserTest.java`

**Interfaces:** `StatementNode parse(String sql)`；纯函数语义、无 DD/Session/storage 依赖。

- [x] **Step 1: 写 grammar matrix RED 测试**

覆盖 design §7 的全部合法形式，以及：空输入、超限输入、未闭合 quote/backtick、奇数 hex、非法 bit、多 tuple、
缺显式列、SELECT 非等值/OR/额外谓词、多语句、事务命令尾随 token。

- [x] **Step 2: 写位置与 normalization RED 测试**

关键字大小写、裸/反引号标识符、单引号 doubled escape、line/column/offset、末尾分号、原始 numeric lexeme。

- [x] **Step 3: 实现 lexer 后运行 lexer tests**

Lexer 每次实例只持输入 cursor；不使用正则灾难回溯。先限制输入长度，再 token 化。

- [x] **Step 4: 实现 recursive-descent parser 后运行全 parser tests**

每个分支显式消费 EOF/可选分号；unsupported shape 抛语法/shape 领域异常，不生成半合法 AST。

---

## Task 9: Transaction Metadata Scope、Binder 与 28 类型转换

**Files:**

- Create: `src/main/java/cn/zhangyis/db/sql/binder/TransactionMetadataScope.java`
- Create: `src/main/java/cn/zhangyis/db/sql/binder/StatementBindingScope.java`
- Create: `src/main/java/cn/zhangyis/db/sql/binder/SqlBindingContext.java`
- Create: `src/main/java/cn/zhangyis/db/sql/binder/DefaultSqlBinder.java`
- Create: `src/main/java/cn/zhangyis/db/sql/binder/SqlTypeCoercion.java`
- Create: bound statement/value types under `src/main/java/cn/zhangyis/db/sql/binder/bound`
- Create: binder exceptions under `src/main/java/cn/zhangyis/db/sql/binder/exception`
- Test: `src/test/java/cn/zhangyis/db/sql/binder/TransactionMetadataScopeTest.java`
- Test: `src/test/java/cn/zhangyis/db/sql/binder/DefaultSqlBinderTest.java`
- Test: `src/test/java/cn/zhangyis/db/sql/binder/SqlTypeCoercionTest.java`

**Interfaces:** Binder 接受 AST + binding context；bound table 保存 exact `TableDefinition`/version，不保存裸 MDL ticket；
transaction scope 是 lease owner，statement scope 提供 publish/abort。

- [x] **Step 1: 写 metadata scope RED 测试**

覆盖新 lease bind 失败关闭、bind 成功转入 transaction、同表 READ 复用、READ→WRITE 升级、close 反序、重复 close、
获取升级失败保留旧 READ、DDL X 等待事务终结。等待使用真实 timeout/CountDownLatch，不使用 sleep 判正确性。

- [x] **Step 2: 写名称/shape RED 测试**

覆盖 current schema、def 三段名、canonical column、列全集、复合主键、projection、prefix/Lob PK 拒绝、表非 ACTIVE/无 binding。

- [x] **Step 3: 写 28 类型矩阵 RED 测试**

每个类型至少正常、边界和非法三类；特别覆盖 unsigned、decimal scale、TIME/YEAR SQL 范围、TIMESTAMP zone/DST、
hex/bit framing、ENUM/SET、JSON、NULL、externalization 仍保留 raw full value。

- [x] **Step 4: 实现 scope/binder/coercion**

先 resolve/convert 全部值，再发布 statement leases。Binder 不调用 B+Tree/LOB，也不 import Record codec；Gateway 映射为
storage value 时再触发物理 codec 最终校验，bound public API 不暴露 storage value。

- [x] **Step 5: 定向 GREEN 与 DD 并发回归**

运行全部 binder tests、DataDictionaryServiceTest、MetadataLockManager tests。

---

## Task 10: DefaultSqlStorageGateway 与 Executor

**Files:**

- Create: `src/main/java/cn/zhangyis/db/sql/executor/storage/SqlStorageGateway.java`
- Create: `src/main/java/cn/zhangyis/db/sql/executor/storage/SqlTransactionHandle.java`
- Create: `src/main/java/cn/zhangyis/db/sql/executor/storage/SqlIsolationLevel.java`
- Create: `src/main/java/cn/zhangyis/db/sql/executor/storage/SqlDurabilityMode.java`
- Create: `src/main/java/cn/zhangyis/db/sql/executor/storage/SqlTransactionRequest.java`
- Create: `src/main/java/cn/zhangyis/db/sql/executor/storage/SqlCommitRequest.java`
- Create: `src/main/java/cn/zhangyis/db/sql/executor/storage/SqlWriteOutcome.java`
- Create: `src/main/java/cn/zhangyis/db/sql/executor/storage/SqlCommitOutcome.java`
- Create: `src/main/java/cn/zhangyis/db/sql/executor/storage/SqlRollbackOutcome.java`
- Create: `src/main/java/cn/zhangyis/db/engine/adapter/DefaultSqlStorageGateway.java`
- Create: `src/main/java/cn/zhangyis/db/engine/adapter/EngineSqlTransactionHandle.java`（package-private）
- Create: `src/main/java/cn/zhangyis/db/sql/executor/DefaultSqlExecutor.java`
- Modify: `src/main/java/cn/zhangyis/db/engine/adapter/DictionaryStorageMetadataMapper.java`
- Modify: `src/main/java/cn/zhangyis/db/storage/api/dml/ClusteredDmlService.java`（只消费 Stage A 新接口）
- Test: `src/test/java/cn/zhangyis/db/engine/adapter/DefaultSqlStorageGatewayTest.java`
- Test: `src/test/java/cn/zhangyis/db/sql/executor/DefaultSqlExecutorTest.java`

**Interfaces:** Gateway 隐藏真实 transaction/MTR；SQL request 使用 SQL 层 isolation/durability 枚举，由 adapter 显式映射；
Executor switch exhaustive 处理两种 bound DML/query，不处理事务控制 AST。

- [x] **Step 1: 写 transaction handle RED 测试**

begin read-only/read-write、跨 gateway handle、重复 commit/rollback、无 undo rollback、resolver full rollback、terminal handle 拒绝。

- [x] **Step 2: 写 INSERT executor RED 测试**

使用真实 DD table + StorageEngine：bound row→ColumnValue→SearchKey→statement guard→DML。成功 close guard；任何后续异常先
guard.rollback；rollback 失败映射 rollback-only/outcome-uncertain。

- [x] **Step 3: 写 SELECT executor RED 测试**

RR/RC ReadView、miss/visible/invisible/delete-mark、projection order、复合主键、external TEXT/BLOB/JSON hydration、RC view finally close。

- [x] **Step 4: 实现 gateway/executor**

read-only/未首写 commit 直接走 transaction lifecycle，不等待无关 redo；有写事务复用 DML commit durability。
LOB read 每个 value 使用有界短 MTR；异常时不返回 partial row。Gateway 在每个入口复核 storage recovery gate OPEN。

- [x] **Step 5: 定向 GREEN 与分层扫描**

运行 executor/gateway、DML、MvccReader、LobStorage tests；再次确认 SQL/session 对 storage internals import 为 0。

**Stage B checkpoint:** 运行全量测试；以 gateway integration test 证明 SQL bound statement 已真实进入 storage，不把 Session 标为已接。

---

## Task 11: SqlSession 事务状态机

**Files:**

- Create: `src/main/java/cn/zhangyis/db/session/SessionId.java`
- Create: `src/main/java/cn/zhangyis/db/session/SessionOptions.java`
- Create: `src/main/java/cn/zhangyis/db/session/SessionState.java`
- Create: `src/main/java/cn/zhangyis/db/session/SessionTransactionMode.java`
- Create: `src/main/java/cn/zhangyis/db/session/SessionSnapshot.java`
- Create: `src/main/java/cn/zhangyis/db/session/SqlSession.java`
- Create: `src/main/java/cn/zhangyis/db/session/DefaultSqlSession.java`
- Create: `src/main/java/cn/zhangyis/db/session/SessionTransactionPolicy.java`
- Create: session exceptions under `src/main/java/cn/zhangyis/db/session/exception`
- Test: `src/test/java/cn/zhangyis/db/session/DefaultSqlSessionTest.java`
- Test: `src/test/java/cn/zhangyis/db/session/SessionTransactionPolicyTest.java`

**Interfaces:** `execute(String)` 负责 parse→transaction preparation→binding scope→execute→statement/transaction cleanup；
Session transaction 同时拥有 opaque SQL storage handle 和 metadata scope；SessionOptions 只引用 SQL 层枚举。

- [x] **Step 1: 写状态表 RED 参数化测试**

逐行覆盖 design §11.3：autocommit statement、SET 0/1、BEGIN implicit commit、COMMIT/ROLLBACK 后是否立即新建 IMPLICIT、
NONE 下 no-op、close rollback。断言变量只在前置 commit/begin 成功后发布。

- [x] **Step 2: 写错误状态 RED 测试**

parse/bind/select error、statement rollback success/failure、rollback-only 命令白名单、commit 前失败、commit terminal 后 durability
失败进入 FAILED、rollback outcome-uncertain、close suppressed 聚合。

- [x] **Step 3: 写单 Session 并发 RED 测试**

第一个 execute 用可控 latch 阻塞，第二个 execute 在 timeout 后抛 `SessionBusyException`；关闭与执行竞争有明确结果；
不使用 `Thread.sleep` 作为唯一同步证据。

- [x] **Step 4: 实现状态机**

用公平 ReentrantLock + deadline。先 parse 决定语句种类，再按事务策略创建/复用 transaction 和 metadata scope。
storage terminal 后才 close metadata。FAILED 只允许 close；rollback-only 只允许 ROLLBACK/close。

- [x] **Step 5: 定向 GREEN**

运行全部 session tests，使用 fake gateway 验证调用顺序，再使用真实 gateway 补一组小型 integration。

---

## Task 12: DatabaseEngine Session Registry 与关闭顺序

**Files:**

- Create: `src/main/java/cn/zhangyis/db/session/SessionRegistry.java`
- Modify: `src/main/java/cn/zhangyis/db/engine/DatabaseEngineState.java`
- Modify: `src/main/java/cn/zhangyis/db/engine/DatabaseEngine.java`
- Test: `src/test/java/cn/zhangyis/db/engine/DatabaseEngineTest.java`
- Create: `src/test/java/cn/zhangyis/db/engine/DatabaseEngineSessionIntegrationTest.java`

**Interfaces:** `DatabaseEngine.openSession(SessionOptions)`；state 增加 CLOSING；registry 使用原子 ID + ConcurrentHashMap，
Session close callback 幂等 deregister。

- [x] **Step 1: 报告 DatabaseEngine blast radius**

扫描所有构造/open/close/accessor/state 断言。说明 CLOSING enum 对 exhaustive switch/测试的影响后再编辑。

- [x] **Step 2: 写 open/gate RED 测试**

NEW/OPENING/FAILED/CLOSING/CLOSED 均拒绝 openSession；只有 DDL recovery 完成并进入 OPEN 后允许；底层 recovery gate 非 OPEN
时 execute 仍拒绝。

- [x] **Step 3: 写 close-order RED 测试**

活动 Session 持未提交 INSERT + LOB；engine close 先拒绝新语句、rollback Session、释放 MDL/row locks，再关闭 StorageEngine；
重开后记录与 LOB 不存在。多个 Session 关闭失败使用 suppressed 聚合且继续处理剩余 Session。

- [x] **Step 4: 实现组合根接线**

在 DDL recovery 成功后创建 mapper/gateway/parser/binder/executor/registry。close 在 lifecycle lock 内只切 CLOSING 和快照，
释放锁后请求 cooperative close，并在 lifecycle lock 外等待 active execute completion latch。只有全部执行线程 quiesce 后
才能进入底层资源关闭；超时保持 CLOSING、返回异常并允许稍后重试，不能关闭仍被活动线程访问的 StorageEngine。

- [x] **Step 5: 定向 GREEN 与 engine/recovery 回归**

运行 DatabaseEngine、DictionaryDdlRecovery、StorageEngine lifecycle 和新增 session integration tests。

---

## Task 13: 端到端事务、MVCC、并发与 28 类型验收

**Files:**

- Create: `src/test/java/cn/zhangyis/db/session/SqlSessionEndToEndTest.java`
- Create: `src/test/java/cn/zhangyis/db/session/SqlSessionMvccConcurrencyTest.java`
- Create: `src/test/java/cn/zhangyis/db/session/SqlSessionCrashRecoveryTest.java`

端到端测试不得绕过公开 Session API；若暴露实现缺陷，回到对应 Task 的生产文件和定向测试修正，再重新执行本任务。

- [x] **Step 1: 全 28 类型端到端**

经 DDL 创建一张覆盖全部类型的表，Session INSERT 后按复合主键 SELECT；逐列比较公开 SqlValue。TEXT/BLOB/JSON 同时覆盖
inline 与 external；TIMESTAMP 使用非 UTC zone；ENUM/SET/BIT 使用边界值。

- [x] **Step 2: autocommit 与跨表事务**

覆盖 autocommit true、SET 0、多语句、BEGIN implicit commit、COMMIT、ROLLBACK、Session close；同一事务写两表后 rollback，
证明 resolver 没有依赖最后 index。

- [x] **Step 3: 两 Session MVCC/锁/MDL**

未提交 INSERT 对另一 Session 不可见；commit 后 RC 可见；RR 已建立 view 仍不可见；rollback 后不可见；duplicate insert row-lock
timeout；DROP 的 MDL X 等到事务 commit/rollback 后才通过。

- [x] **Step 4: crash matrix**

至少覆盖业务 MTR commit 前、commit 后、rollback A 后、rollback B durable 后四个故障点。每次关闭第一套进程对象并从同一目录
重新 open，断言 redo/undo/LOB/FSP/row/transaction state 收敛。

- [x] **Step 5: timeout 与 cleanup matrix**

parser/binder/MDL/row lock/durability/session busy 各自超时；断言没有泄漏 metadata pin、MDL ticket、ReadView、row lock、MTR memo、
LOB allocation 或 Session registry entry。

**Stage C checkpoint:** 全量测试通过后，生产调用链才具备把 2.8 标为完成的候选资格。

---

## Task 14: Current Map、Backlog、静态检查与全量回归

**Files:**

- Modify: `docs/design/current-implementation-map.md`
- Modify: `docs/design/storage-backlog.md`
- Modify: `docs/design/mysql-primary-point-sql-session-design.md`（仅记录实施中已确认的设计修正）
- Modify: 本计划 checkbox 与验证记录

- [x] **Step 1: 从源码重画真实调用链**

用 `rg` 核对 `DatabaseEngine.openSession`、Session parse/bind/execute、metadata scope、gateway、DML/MVCC/LOB、commit/rollback、close。
current map 实线必须带生产 `file:line` 证据；测试-only 和仅预留类型进入 Reserved/Unwired 表。

- [x] **Step 2: 更新受影响小节**

至少更新 Engine、DD/MDL、SQL Parser/Binder、Executor/Storage API、Session、DML、Transaction/MVCC、Undo/Recovery、LOB、Known Gaps。
仍未实现的 UPDATE/DELETE SQL、二级索引、旧表 LOB upgrade、LOB update/delete purge、network/prepared 保持 `planned/partial/unwired`。

- [x] **Step 3: 更新 backlog**

只有所有验收通过才把 2.8 标为完成；否则精确标为 2.8a/2.8b partial 并说明生产缺口。2.2/2.3 顺序按设计 §22 保持。

- [x] **Step 4: 静态规则扫描**

```powershell
rg -n "synchronized|\.wait\(|\.notify\(|\.notifyAll\(" src/main/java
rg -n "throw new (IllegalArgumentException|RuntimeException)" src/main/java
rg -n "TODO|TBD" src/main/java
rg -n "cn\.zhangyis\.db\.storage" src/main/java/cn/zhangyis/db/sql src/main/java/cn/zhangyis/db/session
git diff --check
```

Expected: 新代码无违规；仓库既有匹配若存在，单独列出且不能由本计划新增。

- [x] **Step 5: 全量回归并复核测试数**

```powershell
$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"
& "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --rerun-tasks
```

记录最终 suites/tests/failures/errors/skips，与 Task 0 基线比较；tests 不得下降，0 failure/error/skip。

- [x] **Step 6: 最终 diff 与提交边界复核**

```powershell
git status --short
git diff --stat
git diff -- docs/design/current-implementation-map.md docs/design/storage-backlog.md
```

确认无 build/IDE/临时数据；文档没有把 planned 边写成当前实线。按阶段提交时，每个提交消息必须说明可独立验收结果，
最终提交包含 current map/backlog 同步。

## Final Acceptance Checklist

- [x] 进程内 Session 是唯一端到端测试入口，没有测试手工拼 storage 内部对象冒充生产接线。
- [x] 28 类型 INSERT/point SELECT 全部通过，external LOB 返回完整值。
- [x] statement/full/recovery rollback 均回收未提交 INSERT LOB，无 double-free 或页泄漏。
- [x] autocommit、BEGIN、COMMIT、ROLLBACK、SET 0/1 和 close 状态表全部有断言。
- [x] RR/RC ReadView、row lock、MDL transaction duration 和所有 timeout 有并发测试。
- [x] 多表事务 full rollback 使用 DD resolver，无 last-index 假设。
- [x] recovery/closing gate 和 engine close order 有真实重启测试。
- [x] SQL/session 无 storage internals import，storage 无 SQL/session import。
- [x] current map/backlog 与生产源码一致；剩余 2.2/2.3/LOB purge 缺口明确。
- [x] 固定 Gradle/JDK 全量测试通过且测试数不下降。

## Final Validation Record

- 2026-07-16，固定 JDK 25.0.2 + Gradle 9.5.1：264 suites / 1409 tests / 0 failures / 0 errors / 0 skipped。
- executable Java monitor、生产裸 runtime exception、TODO/TBD、SQL/session→storage 与 storage→SQL/session import 均为 0。
- `git diff --check` 通过；全量首次暴露的 page-cleaner restart publication 竞态已用既有 RED 用例修复并再次全量 GREEN。
