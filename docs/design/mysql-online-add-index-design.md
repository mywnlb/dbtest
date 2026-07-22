# MySQL / InnoDB 风格 Online ADD INDEX 完整设计

## 1. 文档定位

本文定义本项目 `CREATE [UNIQUE] INDEX` 与单动作
`ALTER TABLE ... ADD [UNIQUE] INDEX` 的在线构建、DML 捕获、持久 row log、最终
MDL cutover 与崩溃恢复语义。本文是
[`mysql-data-dictionary-ddl-design.md`](mysql-data-dictionary-ddl-design.md) 中 CREATE INDEX
章节的 operation-specific 扩展；通用 DD、事务、B+Tree、redo、flush 和恢复约束仍分别以对应厚设计为准。

本设计不追求 MySQL 物理格式兼容，但必须维持以下 InnoDB 核心思想：

- 构建期间以 `MDL_SHARED_UPGRADABLE` 排除同表其它 DDL，同时允许普通 DML。
- 任何在线变化先进入独立 row log，最终 cutover 才把 staged index 发布到 DD。
- 普通 COMMIT 与 XA PREPARE 不得越过其 candidate row log 的持久高水位。
- committed DD 仍是索引是否已发布的唯一提交真相，page3 descriptor 只表达 staged 资源所有权。
- 数据页写盘继续遵守 WAL；row log 不能替代数据页 redo、事务 undo 或复制 binlog。

## 2. 范围和非目标

### 2.1 本次范围

- 现有 `CREATE INDEX` 和 `CREATE UNIQUE INDEX` 默认在线。
- 单动作 `ALTER TABLE ... ADD [UNIQUE] INDEX` 继续由 parser 归一成相同 command。
- base scan 期间允许 SELECT、INSERT、UPDATE、DELETE。
- 支持事务 rollback、statement rollback、named savepoint rollback 与 XA PREPARED。
- UNIQUE 冲突在最终静默窗口确定；冲突只回滚 DDL，不撤销已经提交的用户 DML。
- row log 容量耗尽时持久发布 `ABORT_REQUIRED`，随后 DML 继续。
- PREPARED online build 在 crash recovery 中同步续作，完成前不开放普通流量。

### 2.2 非目标

- 不新增 `ALGORITHM`、`LOCK`、异步 job/status SQL。
- 不实现 Online DROP INDEX、通用 Online ALTER、主键更新、prefix SQL、FULLTEXT 或 SPATIAL。
- 不实现外排排序、并行 scan/build、change buffer 或复制 binlog。
- 不改变现有 PREPARED XA 无权威决议时阻止 OPEN 的 fail-closed 规则。
- 不持久化 scan continuation；崩溃后 PREPARED generation 从空 staged tree 重新扫描。

## 3. 模块边界

| 模块 | 新职责 | 明确禁止 |
| --- | --- | --- |
| `dd.ddl.online` | logical manifest、DDL 状态机、MDL 与 DD 发布编排 | 直接操作 FileChannel、BufferFrame |
| `storage.api.ddl.online` | build/gate/change-log 稳定接口和值对象 | import DD、SQL、session |
| `storage.fil.online` | 受控 row-log 文件、frame CRC、force 高水位 | 解释事务可见性、IndexDefinition |
| `storage.api.dml` | clustered mutation 前捕获 candidate，commit/rollback 接线 | 发布 DD 或 staged index |
| `storage.api.ddl` | bounded scan、staged tree 写入、对账与物理验证 | 决定 committed DD 状态 |
| `dd.recovery` | 根据 DD/marker/manifest/descriptor 同步前滚或回滚 | 重跑普通 SQL |

依赖仍保持 `dd -> storage.api -> storage implementation`。storage file repository 只认识
opaque manifest/candidate bytes、identity、sequence 与文件生命周期，不能反向依赖 DD。

## 4. 领域对象

### 4.1 OnlineIndexBuildId

storage 层使用独立正值对象 `OnlineIndexBuildId`，与 DD 的 `DdlId` 在边界显式转换。
这避免 storage import `dd.domain.DdlId`，同时防止把普通 long 误当表、索引或事务身份。

### 4.2 OnlineIndexBuildDefinition

运行期不可变定义至少包含：

- build id、table id、index id；
- source dictionary version、target dictionary version、row-format version；
- `SecondaryIndexMetadata`；
- 受控 row-log path；
- manifest digest。

definition 由 DD 在 initial table X 内创建；普通 DML 只消费冻结快照，不查询未提交 DD。

### 4.3 OnlineIndexCandidate

一条 candidate 表示一个 clustered mutation 可能让 staged tree 观察到的二级物理 entry：

- INSERT：after entry；
- DELETE：before entry；
- UPDATE：目标 physical key 变化时的 before + after；
- 未命中和非目标列更新：无 candidate。

entry 已由 `SecondaryIndexLayout` 投影，包含 logical secondary key 与 appended clustered key。
当前 DML 不支持聚簇主键更新，因此同一 UPDATE 的 clustered identity 稳定。

## 5. MDL 协议

### 5.1 Live 构建

1. 取得 schema `INTENTION_EXCLUSIVE` 和 table `SHARED_UPGRADABLE`。
2. SU 升级为 `EXCLUSIVE`。
3. 冻结 storage table gate，排空旧写事务、purge history 和 metadata pin。
4. 建立 manifest、DDL marker、staged descriptor 与 durable capture 状态。
5. 发布 capture 并解除 storage gate 冻结。
6. 原 ticket 从 X 原子降级为 SU。
7. 在 SU 下扫描和构建；普通 SR/SW 可进入，同表 SU/X DDL 不可进入。
8. base scan 完成后 SU 再升级 X。
9. seal capture，完成 final reconciliation、验证和 DD publish。

### 5.2 Downgrade

`MetadataLockManager.downgrade(ticket, SHARED_UPGRADABLE)` 只支持
`EXCLUSIVE -> SHARED_UPGRADABLE`：

- 在原 shard 的 `ReentrantLock` 内定位 granted request；
- 原子更新 internal request 与原 `MdlTicket`；
- 不释放再申请，不生成第二张 ticket；
- 用既有 FIFO 逻辑唤醒兼容 waiter；
- 不建立 wait-for graph 边，因为 downgrade 不等待。

### 5.3 锁顺序

DDL 顺序：`schema/table MDL -> table gate state -> row-log state -> short MTR/page latch`。
gate 锁内不做 FileChannel、redo、Buffer Pool、DD 或 B+Tree 操作。

DML 顺序：`Session MDL -> table admission -> transaction row lock -> row-log append -> short MTR`。
row-log append 前不得持有 page latch、buffer fix 或 MTR memo。普通事务行锁按事务语义保留到终态。

COMMIT 顺序：`transaction row locks -> row-log files(by build id) -> undo terminal MTR -> redo durability`。
DDL final seal 不持 row-log 文件锁等待事务终态，因此不存在 row-log→row-lock 反向边。

## 6. Table gate

`OnlineDdlTableGate` 从 StorageEngine open 时构造，始终记录所有写事务涉及的 table，不能等
online DDL 开始才追踪。它在一把短 `ReentrantLock` 下维护：

- transaction 到 affected table 集合；
- table 的 in-flight clustered DML 数；
- table 的 committed terminal redo LSN high-water；
- 当前可选 build 与运行态 phase；
- transaction 到 build highest candidate sequence；
- append/force lease 数量。

状态：

```text
ABSENT -> ACTIVATING -> CAPTURING -> SEALING -> SEALED
                         |              |
                         +-> ABORTING <-+
                                |
                                +-> ABSENT
```

`ACTIVATING` 阻止新 admission，等待既有 in-flight DML 与已写事务终结。`CAPTURING`
允许 admission 并返回 target definition。`SEALING` 阻止新 clustered mutation并等待事务、append、force
归零。所有 Condition 等待有 timeout、处理中断并循环复核谓词。

提交完成后 transaction 引用可释放，但每张表保留最大 terminal redo LSN。initial activation 与 final
cutover 都必须强制 redo durable LSN 覆盖该 high-water，避免弱 durability commit 在 index 发布后因 crash
被当作 loser 回滚。

## 7. DML 捕获协议

### 7.1 捕获位置

捕获接在 `ClusteredDmlService`，覆盖 SQL table facade 与受支持的直接 clustered API。

固定步骤：

1. 校验命令并 `assignWriteId`。
2. 在 current-read/row-lock wait 前取得 `OnlineDmlAdmission`。
3. current-read 成功后，用 admission 冻结的 layout 投影 candidate。
4. 在创建业务 MTR、写 undo 或修改聚簇页前 append candidate。
5. 执行原有物理 DML。
6. finally 关闭 admission；事务/table 引用保留到事务终态。

INSERT 在 clustered unique check 通过后记录 after。UPDATE/DELETE 需要 current-read 的权威 old image。
索引列不能是 LOB/JSON，因此 candidate 不依赖尚未分配的 external LOB page identity。

### 7.2 为什么允许多余记录

candidate 先于物理 mutation，因此失败语句、statement rollback、savepoint rollback 或 full rollback
可能留下多余 frame。最终 reconciliation 不把 frame 当作 committed event，而只把它当作“需要从 staged tree
清除的候选集合”；随后以 cutover 时的当前聚簇行重新生成唯一正确 entry。因此不需要修改 UndoRecord 格式，
也不需要给 row log 编码 statement/savepoint inverse。

### 7.3 捕获失败

- capacity overflow：使用预留空间写并 force `ABORT_REQUIRED`，随后当前 DML 正常继续；
- 可明确持久 abort 的普通 I/O 失败：事务 rollback-only，DDL abort，当前 DML 失败；
- 无法证明 abort durable：`DatabaseFatalException`，禁止在未知文件尾后继续。

## 8. COMMIT、rollback 与 XA

### 8.1 普通 COMMIT

1. `TransactionManager.prepareCommit` 预留提交号，事务仍 ACTIVE。
2. gate 按 build id 升序 force 本事务 candidate highest sequence。
3. `UndoLogManager.onCommit` 持久化 undo/history/terminal redo。
4. `TransactionManager.commit` 发布 COMMITTED。
5. gate 发布 affected table 的 terminal redo LSN并解除 active引用。
6. 执行用户选择的现有 durability policy。
7. 释放事务锁。

force candidate 失败发生在 undo commit 之前，事务仍可 rollback。

### 8.2 Rollback

statement/savepoint rollback 不解除 transaction/table 引用。full rollback 只有进入 `ROLLED_BACK` 后才
解除。rollback 不必单独 force candidate；正常 final seal 会 force整份row-log high-water。build进入
ABORTING 后，旧事务的 sequence requirement 被明确作废，commit 不访问已关闭日志。

### 8.3 XA

XA PREPARE 在 `UndoLogManager.onPrepare` 之前 force全部 candidate。phase-two commit/rollback在terminal
redo durable 后解除 gate引用。crash recovery 仍由 `FileXaRegistry` 或外部 provider给出权威决议；无决议
时阻止 OPEN，管理员离线决议并重启后先完成phase-two，再执行online DDL recovery。

## 9. Row-log 文件

### 9.1 路径与 owner

固定路径：

```text
<baseDir>/online-ddl/online-index-<ddlId>.log
```

路径只从 `EngineConfig.baseDir` 与正 build id派生，使用 `NOFOLLOW_LINKS`并验证受控父目录。
DDL marker 的 `auxiliaryPath` 固定为该规范路径，整个 DDL phase history 中不可改变。

### 9.2 创建顺序

1. 预留 ddl/index/version identity。
2. 创建并 force immutable manifest 文件。
3. 写 DDL PREPARED marker，引用该路径。
4. 创建 staged segments/root/page3 descriptor。
5. append/force `GENERATION_STARTED` 与 `CAPTURING`。
6. 发布 runtime capture。

manifest 早于 marker，因此 marker durable 时恢复输入必已尝试建立；marker前崩溃只形成无 catalog owner
的孤儿文件，恢复需在确认没有 marker/page3 owner 后精确删除。

### 9.3 Header 与 manifest

header 包含 magic、format version、ddl/table/index/version identity、manifest length、SHA-256 与 CRC32C。
DD manifest 保存完整 IndexDefinition、旧/目标 dictionary version、source row-format version、旧 binding
digest 与名称/key part。storage file 层只保存 opaque bytes，不解释 DD 对象。

### 9.4 Frame

稳定 frame 字段：length、version、type、generation、sequence、transactionId、payloadLength、payload、CRC32C。

类型：

- `GENERATION_STARTED`
- `CAPTURING`
- `CANDIDATE`
- `FORCE_WATERMARK`
- `SEALED`
- `RECONCILED`
- `ABORT_REQUIRED`

candidate payload 使用目标 entry schema 与现有类型 codec 编码 ColumnValue，禁止 Java serialization。

### 9.5 损坏与尾部规则

- header/manifest/identity/length/sequence/CRC 严格校验；
- 只允许最后一个不完整 frame 截断；
- 中间损坏、sequence倒退或identity变化不能跳过；
- 删除文件不是提交点，也不依赖 Windows 目录 fsync；terminal DDL marker与page3 descriptor才是正确性证据。

## 10. Force 与容量

默认 `OnlineDdlConfig`：

- `maxRowLogBytes = 128 MiB`
- `scanBatchRows = 256`
- `abortReserveBytes = 4 KiB`

每个日志用独立 `ReentrantLock`、highestAppended与highestForced串行化append/force。v1取得文件锁的
调用者把当前高水位写成 `FORCE_WATERMARK` 并执行一次 `FileChannel.force`；后续已被该watermark覆盖的
调用直接返回。未来可以在不改变sequence契约的前提下增加Condition follower合并。force期间不持gate锁、
页闩或MTR。

普通 candidate 不得消费terminal reserve。reserve除`ABORT_REQUIRED`外还必须覆盖最后一次
`FORCE_WATERMARK`；格式下限为256 bytes，当前默认4 KiB。容量不足时在同一文件锁内写
`ABORT_REQUIRED`，force成功后才允许DML继续。

## 11. Base scan

替换当前 `Integer.MAX_VALUE` 全量物化：

1. 首批 `BTreeScanRange.unbounded(256)`；
2. 后续以最后一行完整 clustered physical key调用 `BTreeScanRange.after(key, 256)`；
3. 每批独立只读MTR，返回前释放全部 latch/fix；
4. 每行独立短写MTR写 staged tree；
5. split后刷新root binding；
6. 批次边界检查中断、abort和engine close。

staged tree提供：

- `ensureSecondaryLive`：absent插入、deleted revive、live same no-op；
- `removeSecondaryCandidate`：live physical delete、deleted purge、absent no-op；
- bounded `scanIncludingDeleted`：最终证明没有delete-marked残留。

扫描期不做权威logical UNIQUE检查，避免把并发未提交重复误判为最终冲突。

## 12. Final reconciliation

base scan完成后，SU升级X并seal gate：

1. 等待in-flight DML、事务、append和force归零；
2. force目标表 committed redo high-water；
3. force row-log highest sequence；
4. append/force `SEALED(highWater)`。

两遍对账：

1. 第一遍顺序读取所有candidate，把before/after完整physical entry从staged tree精确移除；
2. 第二遍按candidate clustered key读取当前聚簇行，live时重新投影并 `ensureSecondaryLive`。

重复candidate/clustered key允许重复处理，所有操作必须幂等。

最终双向验证：

- 每条live clustered row必须精确命中一个正确target entry；
- 每条target entry必须live，能反查live clustered row且等于重新投影；
- target live count等于clustered live count；
- UNIQUE且logical key不含NULL时不得对应多个clustered identity；
- NULL logical key允许重复；
- B+Tree结构必须通过验证。

完成后flush redo、force tablespace，append/force `RECONCILED`，再推进DDL `ENGINE_DONE`。

## 13. DD 发布

固定顺序：

1. staged tree + `RECONCILED` durable；
2. DDL `PREPARED -> ENGINE_DONE`；
3. 写包含新index的SDI；
4. invalidate旧table cache；
5. durable commit新DD aggregate；
6. DDL `ENGINE_DONE -> DICTIONARY_COMMITTED`；
7. publish committed cache；
8. exact-CAS清page3 build descriptor；
9. DDL `DICTIONARY_COMMITTED -> COMMITTED`；
10. 注销capture并关闭/删除row log；
11. 发布clean dictionary snapshot。

越过ENGINE_DONE后普通异常不得猜测反向结果；由DD recovery根据committed DD、marker和descriptor收敛。

## 14. Abort

容量、用户中断、最终MDL timeout、UNIQUE冲突或可持久capture错误触发abort。

未持最终X时：

1. durable `ABORT_REQUIRED`；
2. gate进入ABORTING，不再接受candidate；
3. 等待in-flight append/force退出；
4. 作废事务对该build的sequence requirement；
5. 解除capture，让DML继续；
6. 在仍持SU时回收staged segments和page3 descriptor；
7. DDL转 `ROLLED_BACK`；
8. 关闭并精确删除row log。

新index从未进入DD，staged tree也不被普通DML访问，因此cleanup不需要重新争抢X；FSP/page3由现有显式锁和
MTR保护。cleanup失败必须保留marker、descriptor和row log，不能伪造ROLLED_BACK。

## 15. Crash recovery

恢复顺序保持：doublewrite、redo、PREPARED决议、ACTIVE rollback、purge、DDL recovery、OPEN。
Online DDL不新增后台worker。

### 15.1 PREPARED + 旧DD

- valid `ABORT_REQUIRED`：回收staged并ROLLED_BACK；
- manifest或中间frame损坏：安全回收并ROLLED_BACK；
- 最后不完整frame：截断；
- 有旧descriptor：exact rollback；
- row log截断到immutable manifest并force；
- 创建新generation和空staged tree；
- 在关闭流量状态重新全量scan、验证、force、RECONCILED；
- 推进ENGINE_DONE并完成DD publish。

重置generation避免部分staged tree携带未force ACTIVE loser candidate产生幽灵entry。

### 15.2 ENGINE_DONE + 旧DD

交叉校验manifest、descriptor和marker，对现有staged tree执行完整双向验证。`RECONCILED/ENGINE_DONE`
已经越过live不可回退点：成功则前滚DD，证据缺失或验证失败则fail-closed并保留资源，不能转回
`ROLLED_BACK`。

### 15.3 新DD已包含index

- ENGINE_DONE补DICTIONARY_COMMITTED；
- DICTIONARY_COMMITTED直接完成cache/descriptor/COMMITTED；
- descriptor已清视为cleanup已部分完成；
- row-log损坏不推翻committed DD，使用DD binding与物理验证收口；
- 新DD+PREPARED或旧DD+DICTIONARY_COMMITTED属于不可解释顺序，fail-closed。

### 15.4 Legacy 与 orphan

没有 auxiliary row-log path 的旧CREATE_INDEX marker继续使用现有blocking recovery兼容策略。
无marker的online-log只有在catalog和所有page3都不存在对应owner时才能精确删除。terminal marker后遗留文件
由启动cleanup重试，删除失败不改变已提交/回滚结论。

## 16. 可观察性

使用SLF4J记录：build identity、table/index、phase、generation、scan row count、candidate count、
appended/forced sequence、capacity、seal等待、最终结果与abort reason。普通逐行happy path不打印日志。

统一的planned snapshot、durable cancel CAS、Online DROP与通用Online ALTER演进见
[mysql-online-ddl-evolution-design.md](mysql-online-ddl-evolution-design.md)。本文v1不把日志输出误当已实现的
status/control facade。

## 17. 测试与验收

实现严格TDD：codec/config、file repository、gate、DML capture、transaction durability、MDL、physical
build、coordinator、recovery依次先写红灯测试，再实现最小代码和重构。

测试必须覆盖：

- manifest/frame/CRC/尾截断/中间损坏；
- force覆盖、容量与terminal reserve；
- gate状态、timeout、中断、并发事务；
- INSERT/UPDATE/DELETE、rollback/savepoint；
- weak commit durability与XA PREPARED；
- scan期间并发DML；
- UNIQUE final conflict与NULL；
- 每个durable边界的crash重启；
- PREPARED generation reset、ENGINE_DONE前滚、legacy兼容和orphan cleanup；
- 双向索引一致性、无segment/footer/log handle泄漏。

最终使用仓库固定JDK/Gradle运行全量测试，源码复核生产调用链后更新
`current-implementation-map.md`，不能只按本文或计划宣称完成。
