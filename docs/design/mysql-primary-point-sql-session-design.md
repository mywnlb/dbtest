# MiniMySQL 主键点查、INSERT 与进程内 Session 生产接线设计

版本：2026-07-16

## 1. 背景

`storage-backlog.md` 2.8 要求把已经存在的 Data Dictionary、Transaction/MVCC、单聚簇索引 DML、
B+Tree、LOB、redo/undo 和 recovery gate 接到普通用户入口。当前生产代码已有以下能力：

- `DatabaseEngine` 已按 DD discovery → StorageEngine recovery → DDL recovery 的顺序启动，并只在全部成功后发布 `OPEN`。
- `DataDictionaryService.openTable` 已用 MDL 与 cache pin 返回 `TableMetadataLease`，但当前固定取得 `SHARED_READ`。
- `ClusteredDmlService` 已实现单聚簇索引 INSERT/UPDATE/DELETE、statement guard、commit durability 与 full rollback，调用方仍需显式传入内部 `Transaction`、`BTreeIndex`、`LogicalRecord` 和 `SearchKey`。
- `MvccReader` 已实现聚簇主键一致性点读，RR 复用事务级 ReadView，RC 使用语句级 ReadView。
- Record/DD 已覆盖 28 种类型；`LobStorage` 已能在调用方 MTR 中写、读、释放 external LOB 页链。
- `sql.parser`、`sql.binder`、`sql.executor` 和 `session` 当前只有 `package-info.java`，没有生产入口。

本范围同时包含完整 `autocommit=0` 状态机和全部 28 种类型。大于 inline 上限的 TEXT/BLOB/JSON
必须自动 externalize，且未提交 INSERT 回滚时必须回收 LOB 页链。因此它已经不适合继续写成 40–60 行的
slice spec；本文件按用户明确要求作为完整设计，与 [implementation plan](mysql-primary-point-sql-session-implementation-plan.md) 配套。

## 2. 设计依据与优先级

| 文档 | 本设计采用的约束 |
| --- | --- |
| `current-implementation-map.md` | 当前只有 storage 单聚簇入口；SQL/session 尚未接线，所有 planned 边必须保持虚线语义直到源码落地。 |
| `mysql-parser-binder-design.md` | Parser 只产生纯语法 AST；Binder 负责名称、类型和元数据版本绑定；执行期间必须持有 MDL 与 DD pin。 |
| `mysql-sql-executor-storage-api-design.md` | Executor 不能访问 page、frame、裸 B+Tree/record/transaction；语句失败必须有 statement cleanup。 |
| `mysql-session-connection-protocol-design.md` | Session 是事务和变量的逻辑 owner；同一 Session 只执行一个活动语句；autocommit 状态转换必须显式。 |
| `mysql-data-dictionary-ddl-design.md` | 名称访问经 MDL；逻辑表定义与物理 binding 分离；DDL X 必须等待事务持有的表 MDL。 |
| `innodb-transaction-mvcc-design.md` | RR/RC ReadView 生命周期、事务状态、row lock 和 rollback-only 规则由事务层集中维护。 |
| `innodb-undo-log-purge-design.md` | before-image/INSERT undo 必须在记录修改前写入；rollback/recovery 必须按持久 logical head 幂等推进。 |
| `innodb-record-design.md` | SQL 范围校验与时区语义留在上层；Record codec 是物理编码、长度、charset/collation 的最终守门者。 |
| `docs/design/slices/2026-07-13-record-lob-overflow.md` | external LOB 记录只保存引用；LOB 页链 ownership 与 undo external payload 是两套独立语义。 |
| `storage-dml-facade-design.md` | 复用既有 DML 编排和 fail-stop 边界，不在 executor 重写 transaction/undo/B+Tree 算法。 |

优先级保持为：厚设计文档定义长期边界，本文件收敛 2.8 的可实现决策，源码与测试决定当前真实状态。

## 3. 已确定的产品范围

### 3.1 目标

1. 提供进程内 `DatabaseEngine.openSession(SessionOptions)`，不引入网络协议。
2. 支持单行 INSERT、聚簇主键完整等值点查和事务控制 SQL。
3. Parser → Binder → Executor → SQL storage gateway → DD/storage 形成真实生产调用链。
4. Session 正确实现 autocommit on/off、显式事务、statement rollback、commit/rollback 和关闭回滚。
5. 绑定和执行期间固定同一个 DD table version，并把表级 MDL 保持到数据库事务终结。
6. 支持 DD 已定义的全部 28 种列类型；新建 LOB-capable 表的大值自动写 external LOB。
7. external LOB INSERT 的 statement/full/recovery rollback 不泄漏页链。
8. engine recovery/closing 状态拒绝新 Session 和新语句，关闭时先收敛 Session 再关闭 StorageEngine。
9. 所有等待都有 timeout，所有资源均有反序释放路径，无 Java monitor 锁。

### 3.2 非目标

1. 不实现 MySQL wire protocol、COM_QUERY packet、认证、TLS、worker pool 或网络 backpressure。
2. 不实现 UPDATE、DELETE、范围扫描、locking SELECT、JOIN、聚合、排序、子查询或 optimizer。
3. 不实现多行 VALUES、默认值、生成列、函数、算术表达式、参数标记或 prepared statement。
4. 不实现命名 SAVEPOINT、RU、SERIALIZABLE、XA/PREPARED、KILL 或 plan cache。
5. 不维护二级索引，不实现二级索引回表 MVCC/purge；这些仍属于 2.2 及其 DML 前置。
6. 不为旧表在线补建 LOB segment。旧 catalog 可读；缺少 LOB segment 的旧表只允许 inline 值，大值 fail-closed。
7. 不回收已提交行后续 UPDATE/DELETE 产生的旧 LOB；本 SQL v1 不提供 UPDATE/DELETE，相关 ownership 留后续 purge 设计。
8. 不改变全局目标架构图；完成实现后只按真实调用链更新 `current-implementation-map.md`。

## 4. 关键设计决策

### 4.1 使用 SQL 层拥有的 outbound port

新增 `cn.zhangyis.db.sql.executor.storage.SqlStorageGateway`。接口只出现 SQL/DD 可见的值对象和不透明事务句柄；
实现放在 `cn.zhangyis.db.engine.adapter`，允许组合根同时访问 DD 与 storage 内部类型。由此保持：

```text
session → parser/binder/executor → SqlStorageGateway (port)
                                      ↑
                    engine.adapter.DefaultSqlStorageGateway
                                      ↓
       DD lease + storage.api.dml + MvccReader + LobStorage
```

SQL/session 不 import 任何 `cn.zhangyis.db.storage` 类型；transaction/btree/record/mtr/redo 枚举和值对象全部由
engine adapter 隔离。storage 底层也不反向依赖 SQL/session。

### 4.2 无 optimizer 的确定性计划

Binder 直接产生两种物理意图：

- `BoundClusteredInsert`：完整列序、已转换 SQL 值、聚簇 key 和被固定的表版本。
- `BoundPrimaryPointSelect`：投影列序、完整聚簇 key 和被固定的表版本。

不存在访问路径选择，因此不创建伪 optimizer 或单候选成本模型。未来 optimizer 可消费同一 bound statement，
当前 executor 直接执行确定性 primary-point plan。

### 4.3 MDL 必须保持到事务结束

不能在每条 SQL 执行结束时直接关闭 `TableMetadataLease`。autocommit=0 下，INSERT 完成后仍可能 full rollback；
若此时释放表 MDL，DROP 可删除 rollback/recovery 仍需访问的 tablespace。

每个活动 SQL 事务因此持有一个 `TransactionMetadataScope`：

1. 语句绑定先创建 `StatementBindingScope`。
2. scope 复用事务已经持有的表租约，或暂存本语句新取得的租约。
3. bind/execute 成功时把新租约发布到事务 scope；bind 失败时只关闭本语句新取得的租约。
4. storage commit/rollback 完成后，事务 scope 才反序关闭 pin 和 MDL。
5. READ→WRITE 升级先取得 `SHARED_WRITE` 新租约，成功后替换旧 READ 租约；同一 Session 不并发执行语句。

`DataDictionaryService.openTable` 增加 `TableAccessIntent`：READ 映射 `SHARED_READ`，WRITE 映射
`SHARED_WRITE`；schema 仍为 `SHARED_READ`。MDL owner 使用 Session 的稳定 `MdlOwnerId`。

### 4.4 LOB 写入与 INSERT undo 必须在同一业务 MTR

LOB 不能先用独立 MTR 提交再写 undo，否则两个 MTR 之间崩溃会留下无 owner 页链。但业务 MTR 也不能采用
`LOB/FSP → undo → B+Tree`：现有 MTR 以 `(spaceId,pageNo)` 全序守门，B+Tree SMO 又遵循 index latch → FSP，
反向执行既会触发 latch-order guard，也可能与并发 split 构成 FSP ↔ index 等待环。

因此引入三种冻结/准备对象：`LobWritePlan`、`DeferredInsertUndoPlan`、`PreparedClusteredInsert`。MTR 外冻结
payload、placeholder external envelope、undo 编码 shape、目标 undo 快照和总 redo workload，并用独立短只读 MTR
校验 LOB segment identity/purpose。业务 MTR 内严格执行：

1. `prepareUndoAppend` 按低表空间优先原则预留并固定 undo root/payload 页；需要新建/复用 segment 时在此完成 owner
   转移，但不写 placeholder undo record。
2. `prepareClusteredInsert` 用定长 placeholder row 完成 B+Tree 导航、空间判定以及必要的 SMO 页预分配，返回只允许
   本 MTR 使用的写 Guard；该阶段不发布用户记录。
3. 在已持 index prepare guard 的前提下分配/格式化 LOB 页。该局部 index → FSP 顺序与现有 SMO 一致；所有
   LOB-aware writer 都必须经过相同 prepare 协议。
4. 用真实引用物化实际 undo plan，经已固定的 undo 页写入，再由 prepared insert guard 写入聚簇行，最后提交 MTR。

prepare 阶段一旦发生 undo owner/page 或 B+Tree SMO 物理修改，后续任意失败都沿现有 fail-stop 规则处理；MTR rollback
不宣称撤销 buffer content。所有 codec、长度、LOB segment 和 redo admission 可预测错误必须在进入该边界前排除。

### 4.5 完整 rollback 使用 DD resolver，不依赖“最后一张表”

autocommit=0 事务可以依次写多张表。现有 `DmlRollbackCommand` 仍要求一个 fallback `BTreeIndex`，不能作为
Session 的正确完整回滚入口。新增 `UndoTargetMetadataResolver`，逐条从 undo 固定前缀的 tableId/indexId 解析
`UndoTargetMetadata(BTreeIndex, Optional<SegmentRef> lobSegment)`；LOB free 必须用其中的权威 binding 校验引用，
不能只根据可损坏的 undo reference 反构造释放目标。`RollbackService` 和 DML facade 增加 resolver-required overload；
没有 undo 的事务直接由 TransactionManager 终结。现有显式单索引 overload 保留给低层测试和无 DD 的
StorageEngine 使用方式，LOB-aware rollback 则必须显式携带权威 LOB segment。

## 5. 包与职责

| 包 | 新增职责 |
| --- | --- |
| `sql.parser` | lexer、token stream、v1 AST、语法错误定位；不访问 DD。 |
| `sql.binder` | 名称解析、事务级 metadata scope、列/主键绑定、28 类型强制转换、bound statement。 |
| `sql.executor` | 执行 bound INSERT/point SELECT，构造公开结果；不管理物理页或真实事务对象。 |
| `sql.executor.storage` | SQL 层拥有的 storage outbound port、不透明 transaction handle 和 gateway DTO。 |
| `session` | `SqlSession`、options、单语句互斥、变量、事务策略、Session 状态与注册关闭。 |
| `engine.adapter` | 精确 DD version→BTree/schema/LOB binding 映射，`SqlStorageGateway` 默认实现。 |
| `dd.service` | READ/WRITE table access intent；仍只返回 RAII metadata lease。 |
| `storage.api.ddl` | table binding 的可选 LOB segment；CREATE 时为 LOB-capable 表创建 segment。 |
| `storage.api.dml` | LOB-aware INSERT、resolver-based rollback 与既有 statement guard 协作。 |
| `storage.undo` | INSERT LOB ownership、兼容 codec、deferred insert undo 计划。 |
| `storage.trx` | rollback 时删除 INSERT 行，并在推进 logical head 的 MTR 中回收其 LOB。 |

## 6. 进程内公开 API

```java
public interface SqlSession extends AutoCloseable {
    SessionId id();
    SqlExecutionResult execute(String sql);
    SessionSnapshot snapshot();
    @Override void close();
}

public final class DatabaseEngine {
    public SqlSession openSession(SessionOptions options);
}
```

`SessionOptions` 是不可变值对象，包含：

| 字段 | 默认值/规则 |
| --- | --- |
| `Optional<String> currentSchema` | 默认空；未限定表名在空 schema 下绑定失败。 |
| `boolean autocommit` | 默认 `true`。 |
| `SqlIsolationLevel isolationLevel` | SQL 层稳定枚举，默认 `REPEATABLE_READ`；v1 只包含 RR/RC。 |
| `SqlDurabilityMode durabilityMode` | SQL 层稳定枚举，默认 `FLUSH_ON_COMMIT`。 |
| `ZoneId zoneId` | 默认 UTC。 |
| `Duration statementTimeout` | 正值；覆盖同一 execute 的总 deadline。 |
| `Duration metadataLockTimeout` | 正值且不超过 statement 剩余时间。 |
| `Duration rowLockTimeout` | 正值且不超过 statement 剩余时间。 |
| `Duration durabilityTimeout` | 正值且不超过 statement 剩余时间。 |

`SqlExecutionResult` 是 sealed hierarchy：

- `QueryResult(List<ResultColumn>, List<SqlRow>, TransactionStatus)`；本版最多一行。
- `UpdateResult(long affectedRows, TransactionStatus)`。
- `CommandResult(TransactionStatus)`。

`SqlValue` 只表达完整用户值，不暴露 external LOB 引用。二进制/bit 值必须防御性复制。
它包含 NULL、`BigInteger` integer、double floating、`BigDecimal` decimal、string、bytes、temporal、bit、enum 和 set
十类非存储值投影；`ResultColumn` 同时携带声明的 DD type，避免仅凭 Java variant 反推 SQL 类型。
`SqlIsolationLevel`/`SqlDurabilityMode` 位于 `sql.executor.storage`，由 engine adapter 显式映射到 storage 的
`IsolationLevel`/`DurabilityPolicy`；Session/public API 不 import storage 包。

## 7. SQL 文法 v1

### 7.1 通用规则

- 关键字大小写不敏感。
- 标识符支持裸形式和反引号形式；对象身份最终交给 `ObjectName` 做 NFC + Locale.ROOT canonicalization。
- 表名支持 `table`、`schema.table`、`def.schema.table`；三段名的 catalog 只能是 `def`。
- 字符串使用单引号，连续两个单引号表示一个单引号；v1 不解释反斜杠转义。
- 允许一个可选末尾分号，拒绝多语句和分号后的非空 token。
- 语法错误包含 token 位置、期望 token 集合和安全截断后的输入上下文。

### 7.2 INSERT

```sql
INSERT INTO table_name (column_name [, ...])
VALUES (literal [, ...])
```

规则：只允许一个 tuple；必须显式列清单；每一表列恰好出现一次；不支持 DEFAULT、表达式和子查询。
非 nullable 列不能为 NULL。聚簇主键列不能为 NULL。

### 7.3 主键点查

```sql
SELECT * | column_name [, ...]
FROM table_name
WHERE primary_column = literal [AND primary_column = literal ...]
```

规则：WHERE 必须恰好覆盖聚簇主键全部 key part，顺序任意、不得重复，不允许额外谓词。投影列不得重复。
v1 拒绝带 prefix key part 的聚簇索引以及 LOB/JSON 聚簇 key，避免把 prefix 命中误报为唯一逻辑行。

### 7.4 事务控制

```sql
SET autocommit = 0
SET autocommit = 1
BEGIN
START TRANSACTION
COMMIT
ROLLBACK
```

`BEGIN` 与 `START TRANSACTION` 等价。SET 只接受整数 0/1，不接受名称、布尔字符串或多变量赋值。

### 7.5 Literal AST

Parser 只产生 `NullLiteral`、`NumericLiteral`、`StringLiteral`、`HexLiteral` 和 `BitLiteral`，保留原始 lexeme；
它不知道目标列类型。正负号只允许附着在 numeric literal 上。Binder 根据目标 `ColumnTypeDefinition` 完成转换。

## 8. 28 类型绑定规则

所有转换采用严格模式：除 FLOAT/DOUBLE 按目标 IEEE 754 类型执行明确的有限值舍入外，不能证明无损时拒绝；
Record `TypeCodecRegistry` 仍执行最终物理范围、长度、charset、collation 和 JSON 语法校验。

| DD 类型 | 接受的 SQL literal | SQL 层规则 |
| --- | --- | --- |
| TINYINT/SMALLINT/INT/BIGINT | Numeric | 只接受整数形式；用 `BigInteger` 按 signed/unsigned 和物理位宽精确校验。unsigned BIGINT 完整支持 0..2^64-1，进入 Record 时显式转换为 `IntValue` 的原始 long bits，查询结果再还原为非负 `BigInteger`。 |
| FLOAT/DOUBLE | Numeric | 使用十进制 lexeme 解析；拒绝 NaN、Infinity 和溢出。 |
| DECIMAL | Numeric | 用 `BigDecimal`；scale/precision 不能靠静默四舍五入满足。 |
| CHAR/VARCHAR | String | 不做数字到字符隐式转换；长度和字符集由 codec 复核。 |
| BINARY/VARBINARY | Hex | `X'ABCD'`，必须偶数个 hex digit；不把普通字符串隐式转 bytes。 |
| DATE | String | 严格 `yyyy-MM-dd`，范围 1000-01-01..9999-12-31，不支持 zero date。 |
| DATETIME | String | `yyyy-MM-dd HH:mm:ss[.SSS]`，按相同本地字段归一化到 UTC epoch millis，不应用 Session 时区。 |
| TIME | String | `[+-]HHH:mm:ss[.SSS]`，范围 -838:59:59.999..838:59:59.999。 |
| TIMESTAMP | String | 本地格式按 Session ZoneId 转 UTC；也接受带显式 offset 的 ISO 形式。DST gap 拒绝，ambiguous local time 要求显式 offset。范围采用 1970-01-01T00:00:01Z..2038-01-19T03:14:07.999Z。 |
| YEAR | Numeric/String | 只接受 0 或 1901..2155；Record 层更宽的 2B unsigned 范围不是 SQL 合法范围。 |
| BIT | Bit | `B'0101'`；位数不得超过声明宽度，左侧补零形成 canonical bit string。 |
| ENUM | String | 与 symbols 做 v1 精确匹配并映射 1-based ordinal；不做数字 ordinal 输入和 collation 折叠。 |
| SET | String | 逗号分隔 symbol，拒绝未知项与重复项，映射最多 64 位 bitmap；空字符串表示空集合。 |
| TINYTEXT/TEXT/MEDIUMTEXT/LONGTEXT | String | UTF 字符值；inline 超限时自动 externalize。 |
| TINYBLOB/BLOB/MEDIUMBLOB/LONGBLOB | Hex | 二进制值；inline 超限时自动 externalize。 |
| JSON | String | 保留文本形式，复用 `LobCodec`/Hutool JSON 严格语法校验；不实现 MySQL binary JSON。 |

NULL 只有在目标列 nullable 时接受。SELECT 主键谓词不接受 NULL，即使 DD 错误地声明了 nullable 主键列。

## 9. Parser、Binder 与资源生命周期

### 9.1 Parser

`DefaultSqlParser.parse(String)` 每次返回一个不可变 `StatementNode`。Lexer/Parser 无共享可变状态，可跨 Session 复用。
输入长度应有 Session 配置上限；超限在分配大量 token 前拒绝。

### 9.2 Binder

绑定上下文包含：当前 schema、Session `MdlOwnerId`、statement deadline、`StatementBindingScope` 和 Session 时区。
表绑定顺序为：

1. 规范化限定名；无当前 schema 的未限定名立即失败。
2. 按 SELECT/INSERT 选择 READ/WRITE access intent。
3. 从 statement metadata scope 获取 `TableMetadataLease` 或复用事务租约。
4. 验证表状态为 ACTIVE 且存在物理 `TableStorageBinding`。
5. 建立 canonical column-name→ordinal 映射。
6. INSERT 绑定完整行并做类型转换；SELECT 绑定投影和完整 primary key。
7. 记录 DD version、table id/index id 和精确不可变 `TableDefinition`。

绑定失败时，statement scope 关闭本次新租约；既有事务租约不受影响。执行完成后 bound statement 不得逃逸 Session，
也不能进入全局缓存。

## 10. SQL Storage Gateway

建议接口形态：

```java
public interface SqlStorageGateway {
    SqlTransactionHandle begin(SqlTransactionRequest request);
    SqlWriteOutcome insert(SqlTransactionHandle transaction, BoundClusteredInsert statement);
    Optional<SqlRow> selectPoint(SqlTransactionHandle transaction, BoundPrimaryPointSelect statement);
    SqlCommitOutcome commit(SqlTransactionHandle transaction, SqlCommitRequest request);
    SqlRollbackOutcome rollback(SqlTransactionHandle transaction);
}
```

`SqlTransactionHandle` 是 SQL 层定义的不透明接口；默认实现是 engine.adapter 私有类并持有真实 `Transaction`。
Gateway 对错误句柄实现、跨 engine 句柄、重复终结和终态句柄全部抛领域异常。
`SqlTransactionRequest` 和 commit request 只使用 `SqlIsolationLevel`、`SqlDurabilityMode` 与通用 Duration，不出现
storage 枚举。

`DictionaryStorageMetadataMapper` 从 bound statement 中的精确 `TableDefinition` 映射：

- `StorageTableDefinition` / `TableSchema`
- 聚簇 `BTreeIndex`
- `Optional<SegmentRef>` LOB segment
- SQL ordinal 与 Record ordinal

现有 `DictionaryIndexMetadataResolver` 改为复用该 mapper，但 recovery resolver 仍按 repository 查询；SQL 路径绝不
二次查询“最新表定义”。该 resolver 同时实现 `UndoTargetMetadataResolver`，向 rollback 返回同一 committed table binding
中的权威 LOB segment；purge 原有 `IndexMetadataResolver` 行为不变。

## 11. Session 与事务状态机

### 11.1 Session 状态

```text
OPEN ──execute tryLock──> EXECUTING ──success/error cleanup──> OPEN
  │                            │ outcome uncertain
  │ close                      └──────────────────────> FAILED
  v                                                     │ close
CLOSING ───────────────────────────────────────────────> CLOSED
```

Session 使用公平 `ReentrantLock`，`execute` 以 statement timeout 做 `tryLock`。没有无界等待；不使用
`synchronized`。FAILED 只允许 `close`，避免提交结果不确定后继续复用连接。

### 11.2 事务模式

Session 内部区分：

- `NONE`：无活动事务。
- `AUTOCOMMIT_STATEMENT`：仅服务当前语句的临时事务。
- `IMPLICIT`：`autocommit=0` 自动存在的事务。
- `EXPLICIT`：BEGIN/START TRANSACTION 开启的事务。

活动事务同时拥有 `SqlTransactionHandle` 和 `TransactionMetadataScope`。终结顺序始终是 storage commit/rollback，
然后关闭 metadata scope；不得提前释放 MDL。

### 11.3 状态转换

| 输入 | 当前状态 | 行为 |
| --- | --- | --- |
| 普通 SELECT，autocommit=true，NONE | 无事务 | 开只读临时事务；成功 commit，失败 rollback。 |
| 普通 INSERT，autocommit=true，NONE | 无事务 | 开读写临时事务；statement 成功后 commit，失败先 statement rollback 再 full rollback。 |
| 普通语句，IMPLICIT/EXPLICIT | 活动事务 | 复用事务；成功不提交；可确认的语句失败只回滚 statement。 |
| `SET autocommit=0`，当前 true | NONE | 先修改变量，再立即创建 IMPLICIT 读写事务；begin 失败则恢复变量。 |
| `SET autocommit=1`，当前 false | IMPLICIT/EXPLICIT | 先提交活动事务；成功后关闭 metadata scope、设 true、进入 NONE。 |
| `BEGIN`/`START TRANSACTION` | NONE | 创建 EXPLICIT 读写事务，autocommit 变量不变。 |
| `BEGIN`/`START TRANSACTION` | IMPLICIT/EXPLICIT | 先隐式提交当前事务，再创建新的 EXPLICIT 事务。 |
| `COMMIT` | 活动事务 | 提交并释放 metadata；autocommit=false 时立即创建新 IMPLICIT，否则 NONE。 |
| `ROLLBACK` | 活动事务 | 完整回滚并释放 metadata；autocommit=false 时立即创建新 IMPLICIT，否则 NONE。 |
| `COMMIT`/`ROLLBACK` | NONE | 成功 no-op。 |
| `close` | 活动事务 | 完整回滚；随后释放 metadata 并从 registry 注销。 |

只读或尚未首写的事务不等待 redo durability；已有写 id/undo 的事务使用 Session durability policy。

### 11.4 错误状态

- parse/bind/SELECT 错误没有物理修改；显式事务保持可用。
- INSERT 错误必须调用 statement guard rollback。成功后显式事务可继续。
- statement rollback 失败后真实事务为 rollback-only；Session 只接受 `ROLLBACK` 或 `close`。
- commit 在 `transactionManager.commit` 前失败时事务仍 ACTIVE，可执行完整 rollback。
- commit 已进入 COMMITTED 但 durability 等待或响应阶段失败时，结果不确定；释放 row locks，Session 进入 FAILED。
- rollback MTR outcome-uncertain 或事务停在 ROLLING_BACK 时，保留 row locks/MDL，Session 进入 FAILED；关闭引擎时按 fail-stop 路径处理，重启 recovery 续作。

## 12. INSERT 执行流

### 12.1 MTR 外预检与冻结

1. Session 已持事务和 WRITE metadata lease。
2. Gateway 从精确表版本创建聚簇 BTreeIndex、TableSchema、完整 LogicalRecord 和 SearchKey。
3. `ClusteredDmlService.beginStatement` 固定 undo statement boundary。
4. 分配 write id，执行 unique current-read；等待 row lock 前没有 page/record/undo latch。
5. 所有列先通过 codec dry validation；LOB analyzer 找出超过 `LobCodec.INLINE_PAYLOAD_LIMIT` 的值。
6. 每个外部值生成 `LobWritePlan`，冻结 payload、CRC、页数、inline prefix、segment identity 和 redo workload。
7. 用定长 placeholder reference 计算 INSERT undo ownership 尾部和聚簇记录的精确 encoded shape，冻结 undo append 与
   B+Tree insert 的资源计划。
8. 用独立短只读 MTR 校验 authoritative LOB segment 的 identity/purpose；释放全部页 latch/fix 后再继续。
9. 聚合 LOB + undo + B+Tree prepare/写入的 redo workload，通过 admission 后才开始业务 MTR。

### 12.2 单业务 MTR

```text
prepare undo append pages/owner (no placeholder record bytes)
  → prepare clustered insert with fixed-size placeholder row (no published row)
  → LOB segment/FSP allocation and page format
  → materialize actual LobReference list
  → append actual INSERT undo through prepared undo handle
  → publish clustered record through prepared insert guard
  → MTR commit
```

`PreparedUndoAppend.appendActual` 必须验证实际 ownership 数量、column ordinal、type、encoded length、inline/external
分支、external page count、undo identity 与冻结计划一致；`PreparedClusteredInsert.publish` 必须验证实际 row encoded
length 与 placeholder 完全相同。不一致是计划器错误并 fail-stop，不能改用临时更大预算继续写。

若某个 LOB write 自身失败，`LobWriteAllocation` guard 仍尽力反序回收本次新 allocation 并保留 suppressed 根因；
但 prepare undo/B+Tree 已跨过物理修改边界时，清理成功也不能把错误降级为普通可重试异常，整体仍进入
`UndoWriteFatalException` fail-stop。MTR rollback 只释放 memo/reservation，不宣称撤销 buffer 内容。

### 12.3 语句成功与失败

- MTR commit 成功后关闭 statement guard，保留事务 undo 供 commit/full rollback。
- MTR commit 前的普通失败执行 statement rollback；没有 durable MTR 时应为 no-op 或只消费 boundary。
- MTR commit 后 executor/结果构造失败仍执行 statement rollback，删除行并回收 LOB。
- autocommit 临时事务在 statement 成功后 commit；commit 失败按 §11.4 处理。

## 13. INSERT undo 的 LOB ownership 格式

`UndoRecord` 新增 `List<InsertedLobOwnership> insertedLobs`：

```java
public record InsertedLobOwnership(int columnOrdinal, ColumnValue.ExternalValue value) { }
```

构造约束：

- 只有 `INSERT_ROW` 可以携带 ownership；UPDATE_ROW/DELETE_MARK 必须为空。
- ordinal 唯一、递增、指向 LOB-capable 列；value type 必须与 schema 列匹配。
- DML producer 与 rollback consumer 必须分别把 reference 的 space/segment 与权威表 binding 的 LOB segment 比对；
  `UndoRecord` 自身不持 DD binding，不能单独完成该校验。

INSERT undo 的公共前缀和 key framing 不变。旧格式在 key 后立即结束；新格式追加：

```text
[lobTailMagic u16][lobTailVersion u8][lobCount u16]
repeat lobCount:
  [columnOrdinal u16][externalEncodedLength u16][external value bytes]
```

external value bytes 复用目标列的 `LobCodec`，不重复定义 `LobReference` 磁盘协议。解码规则：

1. key 后无剩余字节：旧 INSERT undo，ownership 为空。
2. 有剩余字节：必须命中 magic/version，逐项按 schema 解码并完全消费。
3. 未知版本、重复 ordinal、inline value、非 LOB 列、尾随字节均按 undo corruption fail-closed。
4. `peekIdentity` 只读固定前缀，不受尾部扩展影响。

## 14. LOB segment 与 catalog 兼容

`TableStorageBinding` 增加 `Optional<SegmentRef> lobSegment`。新建表只要含任意 TEXT/BLOB/JSON 列，
`TableDdlStorageService.createTable` 就在同一 CREATE MTR 中创建一个 `SegmentPurpose.LOB` segment；DROP 仍删除整个
tablespace，不需要逐 LOB segment 回收。

Catalog table payload 在现有 index bindings 后追加：

```text
[hasLobSegment boolean][lobSegment?]
```

decoder 在 index bindings 后已经 EOF 时解释为旧 catalog、`Optional.empty()`；有剩余字节时解析新尾部并继续要求
完全消费。旧表的 inline LOB 正常读写；需要 externalize 而 binding 为空时抛 `LobSegmentUnavailableException`，
错误明确指出需要重建/迁移表，不在普通 INSERT 下偷建 segment。

## 15. 主键一致性点读

1. Parser/Binder 取得 READ metadata lease，绑定投影和完整聚簇 key。
2. Gateway 取得事务 ReadView：RR 复用事务 view，RC 每条 SELECT 创建新 view。
3. 从同一 pinned table version 映射 BTreeIndex/SearchKey，调用 `MvccReader.read`。
4. `MvccReader` 完成当前版本与 undo 版本遍历，并在返回前释放 B+Tree/undo MTR。
5. 对可见行中的每个 `ExternalValue`，Gateway 使用独立只读 MTR 调 `LobStorage.read`，校验 chain identity/CRC。
6. 按投影顺序转为公开 `SqlValue`；结果中不出现隐藏列、delete mark、roll pointer 或 LOB reference。
7. RC view 在 finally 中显式 `closeReadView`；RR view 留到事务 commit/rollback。

ReadView 活跃期间 purge low water 保护旧版本；metadata lease 保护 table/tablespace 不被 DDL 删除。LOB hydration 失败
按数据损坏/IO 领域异常传播，不能返回 prefix 或部分行。

## 16. LOB rollback 与 crash recovery

带 LOB 的 INSERT undo 不能先 free 再单独推进 logical head；若两者之间崩溃，重试会重复释放可能已复用的页。
每条 INSERT rollback 使用：

1. **Inverse MTR A**：按 key、transactionId 和 roll pointer 删除未提交聚簇行；未命中或已删除是幂等 no-op。
2. **Ownership MTR B**：从 `UndoTargetMetadataResolver` 取得权威 LOB segment并与 undo ownership reference 比对；
   MTR 内必须先按 expected head 打开并固定 undo first-page X latch，再校验并释放全部 LOB chain，最后在已持 guard
   上写入新 logical head。不得先持用户表 FSP/LOB 页再回头获取低位 undo 页，也不得仅从 reference 反构造 segment。
3. MTR B commit 后才发布内存 logical head。

崩溃边界：

| 崩溃点 | 重启行为 |
| --- | --- |
| INSERT 业务 MTR 前/中且未提交 | 无 durable LOB、undo 或 row；redo 不重放。 |
| INSERT 业务 MTR 已提交 | redo 同时恢复 LOB/FSP、undo 和聚簇行；active transaction recovery 可完整回滚。 |
| rollback MTR A 前 | 正常执行 A、B。 |
| A 后、B 前 | A 重做为 no-op，B free + marker。 |
| B commit outcome 已 durable | redo 原子恢复 free + marker；logical head 已越过该 undo，不再重复 free。 |

full rollback、recovery rollback 和单行 SQL statement rollback必须共用该协议。若 partial rollback 一次累计的 external
页数超过单 MTR redo/buffer 上限，预检即拒绝并把事务置 rollback-only；SQL v1 每条 INSERT 只有一行，从语法层限定该上界。

已提交 INSERT 的 INSERT undo 可按现有策略 cache/drop，LOB ownership 随 record 成为存活数据。DROP 删除整个 tablespace。
未来 UPDATE/DELETE 引入 LOB replacement 后，必须另行设计旧版本 ownership 和 purge 回收，不能复用 INSERT ownership 猜测。

## 17. 并发、锁顺序与 owner

| 共享状态 | Owner | 保护机制 |
| --- | --- | --- |
| DatabaseEngine lifecycle | DatabaseEngine | 现有 `ReentrantLock`；新增 CLOSING 状态。 |
| Session registry | DatabaseEngine | `ConcurrentHashMap<SessionId, SqlSession>` + 原子 ID；不在 registry 操作中执行 rollback。 |
| Session state/variables/transaction | SqlSession | 公平 `ReentrantLock.tryLock(deadline)`；单活动语句。 |
| Transaction metadata leases | 当前 Session transaction | 只在 Session execute lock 内变更；lease 内部自行保证幂等 close。 |
| DD cache/version | DD cache | 现有 pin/single-flight；上层只持 lease。 |
| MDL | `MdlOwnerId` | MetadataLockManager 显式 lock/condition/timeout。 |
| Row locks | `TransactionId` | LockManager；只在事务 terminal 后 releaseAll。 |
| page latch/fix | MiniTransaction | MTR memo；任何 row-lock/MDL 等待前不得持有。 |
| LOB/FSP pages | MiniTransaction | 既有 page0→page2→LOB/index latch discipline 与显式例外 scope。 |

标准顺序：

```text
Session execute lock
  → schema/table MDL + DD pin
  → transaction/ReadView
  → row lock（等待时无 page/undo/FSP latch）
  → short MTR page/FSP/undo/B+Tree work
  → redo durability wait
  → row lock release
  → transaction metadata lease release
  → Session execute lock release
```

Session execute lock 只序列化同一连接，不是数据库全局锁。Engine close 先在 lifecycle lock 下切换 CLOSING 并快照
Session，随后释放 lifecycle lock，再请求 Session cooperative close 并等待 active execute completion latch。等待上限由
活动语句原 deadline 与 engine close deadline 的较早者决定；不得用 `Thread.interrupt` 假装 MTR 已安全终止。

## 18. DatabaseEngine 生命周期

`DatabaseEngineState` 增加 `CLOSING`。`openSession` 和每次 `execute` 都要求 engine 为 OPEN；底层 gateway 仍检查
StorageEngine recovery gate，形成组合根与存储层双重守门。

启动顺序保持：

```text
DD open/discovery → StorageEngine crash recovery → DDL recovery
→ construct mapper/gateway/parser/binder/executor/session registry → OPEN
```

关闭顺序调整为：

```text
OPEN → CLOSING → reject new sessions/statements
→ snapshot and close sessions (rollback active transaction)
→ close StorageEngine
→ close DD control/catalog
→ CLOSED
```

关闭异常使用 suppressed 聚合。已经完成 quiescence、但 rollback outcome 未确认时不声称优雅关闭成功；可以继续关闭
底层资源进入 fail-stop，重启由 crash recovery 收敛。若任一 active execute 在 deadline 前没有 quiesce，则保持
`CLOSING`、拒绝新流量并返回关闭超时，**不得**关闭仍可能被执行线程访问的 StorageEngine；调用方可在执行线程退出后
重试 close。Session deregistration 必须幂等。

## 19. 异常模型

新增异常按层管理并继承项目异常：

- parser：`SqlSyntaxException`、`SqlInputTooLargeException`
- binder：`SqlBindingException`、`UnknownColumnException`、`SqlTypeConversionException`、`UnsupportedSqlShapeException`
- executor：`SqlExecutionException`、`SqlStorageContractException`
- session：`SessionStateException`、`SessionBusyException`、`TransactionOutcomeUnknownException`
- storage/LOB：`LobSegmentUnavailableException`、必要的 insert-plan/ownership corruption 异常

语法、绑定、重复键、锁超时属于 `DatabaseRuntimeException` 路径；持久 undo/LOB ownership 损坏、redo budget
低估和 MTR outcome-uncertain 按既有 fatal/fail-stop 层次处理。包装底层异常必须保留 cause 和表/索引/Session/事务
身份，日志只记录有诊断价值的生命周期或异常边界。

## 20. 测试设计

### 20.1 Parser/Binder

- 所有支持语法、关键字大小写、反引号、转义、末尾分号、非法多语句和错误位置。
- INSERT 列缺失/重复/未知、NULL、复合主键、SELECT 谓词缺失/重复/额外条件和 projection 顺序。
- 28 类型各自的正常值、边界值、越界、错误 literal kind、charset/collation、JSON、时区/DST、ENUM/SET。
- current schema、二/三段名、非 `def` catalog、prefix/Lob primary key 拒绝。
- bind 失败释放新租约；显式事务复用租约；READ→WRITE 升级；DDL X 等待到事务终结。

### 20.2 LOB/Undo/Recovery

- 新 LOB 表 CREATE 生成一个 LOB segment；非 LOB 表不生成；DROP 整体删除。
- 新旧 catalog payload 兼容、尾随/损坏尾部 fail-closed、旧 LOB 表大值拒绝。
- 新旧 INSERT undo codec、多个 external 列、重复 ordinal/type/segment mismatch、identity peek 不变。
- inline 上限两侧、跨多页 LOB、combined redo admission、多个 LOB 中途失败补偿。
- statement/full/recovery rollback 回收；在 A 后/B 前和 B commit 边界故障注入后重启验证幂等。
- 业务 MTR crash 前后分别验证“全部没有”或“LOB+undo+row 全部恢复”。

### 20.3 Session/Executor/MVCC

- autocommit INSERT/SELECT、SET 0/1、BEGIN、COMMIT、ROLLBACK、重复控制命令、close rollback。
- 显式事务多语句和跨表写后 resolver-based rollback。
- 语句失败后 statement rollback，rollback-only 只允许 rollback/close，commit durability 结果不确定使 Session FAILED。
- 两 Session：未提交 INSERT 不可见，commit 后可见，rollback 后不可见；RR snapshot 稳定，RC 下一语句可见新提交。
- point SELECT 还原 external TEXT/BLOB/JSON，结果不暴露内部引用。
- session busy timeout、row-lock timeout、MDL timeout、engine recovery/closing gate、engine close 与活动 Session 竞争。

### 20.4 回归门槛

- 目标测试先 RED 后 GREEN。
- 修改高扇出 `UndoRecord`、`TableStorageBinding`、`DatabaseEngine` 前先 `rg` 全部生产/测试调用点并报告 blast radius。
- 每个阶段运行对应定向测试；最终固定 JDK 25.0.2 + Gradle 9.5.1 执行全量 `test --rerun-tasks`。
- 测试总数不得下降；生产代码静态扫描无 Java monitor、裸 `IllegalArgumentException`/`RuntimeException` 和占位词。

## 21. 验收标准

1. 新建含全部 28 类型的表，可经 Session INSERT 一行并按复合主键 SELECT 得到等值结果。
2. 超过 inline 上限的 TEXT/BLOB/JSON 写为 external LOB，查询返回完整值。
3. autocommit、显式 rollback、Session close 和 crash recovery 都不会留下未提交行或其 LOB 页链。
4. autocommit=0 下 metadata lease 和 MDL 保持到 commit/rollback，DDL X 不会越过活动事务。
5. SQL/session 源码不 import storage 内部 record/btree/trx/mtr 类型；storage 不 import SQL/session。
6. 所有等待有 timeout；异常路径释放 statement 资源，非终态 rollback 失败不提前释放 row lock/MDL。
7. engine 只在 recovery/DDL recovery 后开放 Session，关闭时先收敛 Session。
8. `current-implementation-map.md` 按源码标出真实实线、partial/unwired 与剩余缺口，backlog 2.8 状态同步。
9. 固定 Gradle/JDK 全量测试通过，测试数不倒退。

## 22. 与 backlog 2.2、2.3 的关系

- 2.2 仍需先增加二级索引 DML/undo producer；当前切片只建立真实用户聚簇入口，不把 secondary purge 伪装成已接。
- 2.3 的 DDL undo marker 依赖独立 DDL_LOG，可作为并行支线；temporary undo 依赖本设计建立的 Session 生命周期，放在本切片之后。
- 本设计完成后推荐主线为：二级索引同步写入与 undo → 2.2 secondary purge/回表 MVCC → temporary undo。
- LOB UPDATE/DELETE ownership 与 purge 也应和二级索引写放大一起设计，避免多个 purge owner 协议互相覆盖。

## 23. 文档维护要求

本文件是 2.8 的完整设计，不替代 parser/binder、executor、session、DD、undo 等长期厚设计。实现过程中若发现必须
改变模块职责或依赖方向，先修订本文件并说明取舍；普通类名或局部算法调整只更新 implementation plan 和源码注释。

每个可运行阶段完成后都从生产源码重新核对调用链，并只更新 `current-implementation-map.md` 受影响小节。只有全部
验收通过后，才能把 backlog 2.8 标为完成；LOB prerequisite、SQL parser/binder、executor、Session 中任一未接时都应标为
`partial`，不能用测试专用调用冒充生产实线。

## 24. 实施中确认的设计修正（2026-07-16）

1. 同一事务可交错修改多张表并共用同一种 undo log。`UndoLogSegment` 校验非空 predecessor 时不能用“本次待写表”的
   `TableSchema/IndexKeyDef` 完整解码旧记录，否则跨表第二次 append 会误判损坏。生产实现改为只读固定、schema-free 的
   `UndoRecordIdentity`，核对 predecessor undoNo、creator transaction 与 roll pointer；真正消费旧记录时仍由 DD resolver
   提供该记录自身的精确 schema/index 解码。该修正不放宽 corruption 检查。
2. BIT 的 SQL canonical 先按声明宽度在左侧补零，再按最高有效位优先、首字节左对齐打包，并在公开 `BitValue` 中保留
   declared bit length。例如 `BIT(5) B'101'` 的公开/record canonical bytes 为 `0x28`，不是右对齐的 `0x05`。
3. Engine close 对 Session 使用同一绝对 deadline 下的有界并行收敛；只有全部 execute/rollback 已 quiesce 才关闭
   `StorageEngine`。若 deadline 到期，engine 保持 `CLOSING`，这不是可继续释放底层资源的普通 close failure。
