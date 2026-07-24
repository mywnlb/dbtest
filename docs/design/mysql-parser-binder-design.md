# MiniMySQL MySQL 8.0 风格 SQL Parser 与 Binder 模块设计

版本：2026-06-05  
实现语言：Java  
参考基线：MySQL 8.0.46 SQL Parser、Prepared Statement、Metadata Locking、Data Dictionary  
关联设计：[mysql-sql-executor-storage-api-design.md](mysql-sql-executor-storage-api-design.md)、[mysql-query-optimizer-design.md](mysql-query-optimizer-design.md)、[mysql-advanced-executor-operators-design.md](mysql-advanced-executor-operators-design.md)、[mysql-data-dictionary-ddl-design.md](mysql-data-dictionary-ddl-design.md)、[innodb-storage-engine-overview.md](innodb-storage-engine-overview.md)、[mysql-session-connection-protocol-design.md](mysql-session-connection-protocol-design.md)

## 1. 目标与边界

本设计面向 MiniMySQL 的 SQL Parser 与 Binder 模块。Parser 负责把 SQL 文本转换为纯语法 AST；Binder 负责在 Data Dictionary、MDL、类型系统和权限钩子的帮助下，把 AST 转换为 `BoundStatement`。后续 Optimizer 基于 `BoundStatement` 构建逻辑计划，Executor 基于执行计划或 DDL request 执行语句。

设计目标：

- 高内聚：Lexer、Parser、AST、NameResolver、TypeResolver、ParameterBinder、PrivilegeHook、StatementBinder 分别收敛在明确子包。
- 低耦合：Parser 不访问 Data Dictionary；Binder 不选择访问路径；Optimizer 不重新解析 SQL 文本；Executor 不自己做名称解析。
- MySQL 8.0 风格：对齐 SQL mode、charset/collation、prepared statement 参数、MDL、Data Dictionary version、类型转换和权限检查入口。
- 面向对象：使用不可变 AST、语义绑定对象、scope 栈、表达式类型对象、参数元数据和资源 guard 表达生命周期。
- 可并发：parse cache 只缓存 schema-free AST；绑定结果携带 dictionary version；MDL/DD pin 所有权由 `StatementResourceGuard` 管理。
- 可测试：覆盖语法、名称解析、类型推导、参数绑定、schema version stale、MDL 等待和清理路径。

非目标：

- 不实现查询优化、join order、access path 或 cost model。
- 不执行 SQL，不访问 B+Tree、Record、Buffer Pool 或事务行锁。
- 不实现完整账号权限系统，只提供 `PrivilegeHook` 和权限需求对象。
- 不完整复刻 MySQL grammar 的所有语法，第一阶段聚焦 SELECT、INSERT、UPDATE、DELETE、CREATE/DROP/ALTER 基础语句。
- 不生成 Java 源码。本文件只定义设计、关系和数据流。

## 2. MySQL 8.0 参考依据

本设计参考 MySQL 8.0 的以下行为：

- MySQL 解析 SQL 时受 `sql_mode`、character set、collation、identifier quote 等 session 属性影响。
- Prepared Statement 的 `PREPARE` 阶段解析语句并识别参数；`EXECUTE` 阶段绑定参数值并执行，schema 变化可能触发 reprepare。
- Metadata Lock 在语义绑定和执行期间保护表、schema、tablespace 等元数据对象。
- Data Dictionary 提供不可变对象版本和对象 cache，DDL publish 后旧版本可能仍被正在执行的 statement pin。
- 类型推导需要处理 numeric/string/date/time、NULLability、collation、比较和赋值转换。
- DDL 执行需要通过 Data Dictionary / DDL coordinator，而不是进入普通 optimizer access path。

简化点：

| MySQL 8.0 能力 | MiniMySQL 第一阶段 |
| --- | --- |
| 完整 MySQL grammar | 先覆盖核心 DQL/DML/DDL 和 EXPLAIN |
| 完整权限系统 | `PrivilegeHook` 先支持 allow-all/mock 和需求收集 |
| 完整 collation 规则 | 先支持默认 collation 和显式 charset/collation 标记 |
| 完整 prepared reprepare | 先按 `DictionaryVersion` stale 触发同步 rebind |
| 复杂子查询和窗口语义 | Binder 定义语义边界，执行细节由 [mysql-advanced-executor-operators-design.md](mysql-advanced-executor-operators-design.md) 承接 |

## 3. 总体架构

架构图见 [parser-binder-architecture.mmd](diagrams/parser-binder-architecture.mmd)。

转换链路：

`SqlText -> TokenStream -> StatementAst -> BoundStatement -> LogicalPlan -> PhysicalPlan -> Executor`

DDL command 从 `BoundStatement` 旁路进入 `DdlRequest` / DDL coordinator，不进入关系优化与执行链。

核心原则：

- `StatementAst` 只保存纯语法结构、source span 和原始标识符，不保存 `TableId`、`ColumnId`、`DictionaryVersion`。
- `BoundStatement` 保存语义绑定结果，包括 table/column binding、参数元数据、表达式类型、MDL/DD pin 需求和 dictionary version。
- `LogicalPlan` 由 Optimizer 构建；Binder 只提供语义输入，不选择索引、不估算代价。
- DDL AST 转换为 `DdlRequest`，进入 Data Dictionary / DDL coordinator，不走普通 optimizer。
- Prepared Statement 的模板保存 AST 和绑定模板，执行期参数值单独管理。

## 4. 包与职责

| 包 | 职责 | 依赖 | 主要模式 |
| --- | --- | --- | --- |
| `sql.parser.lexer` | 字符流到 token，注释、字符串、标识符、位置 span | session config | Strategy |
| `sql.parser.core` | token 到不可变 AST | ast | Parser, Factory |
| `sql.parser.ast` | AST node、statement node、expression node | 无 | Composite, Visitor |
| `sql.expression` | sealed bound expression、exact type/nullability/source position | dd domain, parser position | Composite |
| `sql.type` | SQL typed value 与三值 boolean | common | Value Object |
| `sql.binder.api` | Parser/Binder 门面、绑定请求和输出 | parser, binder | Facade |
| `sql.binder.scope` | scope stack、table alias、column namespace | dd domain | Repository |
| `sql.binder.name` | schema/table/column/name resolution | dd, mdl | Chain of Responsibility |
| `sql.binder.type` | 类型推导、NULLability、collation、coercion | type domain | Strategy |
| `sql.binder.param` | `?` 参数 ordinal、期望类型、执行期校验 | session | Builder |
| `sql.binder.privilege` | 权限需求收集和检查钩子 | dd/session | Adapter |
| `sql.binder.cache` | AST cache、prepared template cache | parser | Cache, State |
| `sql.binder.resource` | MDL ticket、DD pin、statement cleanup | dd, mdl | RAII Guard |

禁止方向：

- `sql.parser.*` 不能依赖 Data Dictionary、MDL、Optimizer、Executor。
- `sql.binder.*` 不能依赖 B+Tree、Record、Buffer Pool、Redo 或 physical page。
- `sql.binder.type` 不能访问 DD repository，只处理类型和表达式。
- `sql.executor` 不能反向修改 AST 或绑定对象。

## 5. 核心领域模型

类关系图见 [parser-binder-class-relation.mmd](diagrams/parser-binder-class-relation.mmd)。

| 对象 | 职责 |
| --- | --- |
| `SqlText` | 原始 SQL 和 session 语法配置 |
| `Token` / `TokenStream` | token type、literal、source span、charset hint |
| `StatementAst` | 不可变语法树根，按 SELECT/DML/DDL/EXPLAIN/PREPARE 分类 |
| `AstNode` | 表达式、表引用、谓词、排序、投影等语法节点 |
| `BindingRequest` | session、current schema、sql_mode、timeout、privilege context |
| `BindingContext` | MDL tickets、DictionaryReadView、scope stack、resource guard |
| `BoundStatement` | 绑定后的语义语句基类 |
| `BoundQuery` | SELECT/EXPLAIN 的绑定结果，包含 `QueryBlock` |
| `BoundDmlStatement` | INSERT/UPDATE/DELETE 的绑定结果 |
| `DdlRequest` | DDL coordinator 输入 |
| `TableBinding` | table id、alias、dictionary version、storage binding |
| `ColumnBinding` | column id、table binding、type、nullable、ordinal |
| `BoundExpression` | sealed 语义表达式；列引用携带 stable column id、ordinal、exact DD type 和 source position |
| `ParameterMetadata` | 参数 ordinal、期望类型、nullable、执行期约束 |
| `StatementResourceGuard` | 管理 MDL ticket、DD pin、statement cleanup |

## 6. SQL 语法、AST 与绑定数据结构

### 6.1 Token 与 AST

Lexer 处理：

- 关键字、标识符、quoted identifier。
- 字符串、数值、日期时间 literal。
- 注释和 whitespace。
- `?` parameter marker。
- source span，用于错误定位。

Parser 输出：

- `SelectAst`
- `InsertAst`
- `UpdateAst`
- `DeleteAst`
- `CreateTableAst`
- `DropTableAst`
- `AlterTableAst`
- `CreateIndexAst`
- `ExplainAst`
- `PrepareAst` / `ExecuteAst`

AST 不允许包含语义绑定结果。相同 SQL 文本在不同 schema、不同 DD version、不同权限上下文下可以复用 AST，但不能复用绑定对象。

当前 M3 生产 Parser 对单表 SELECT/UPDATE/DELETE 的 WHERE 使用 sealed
`BooleanExpressionNode`，按 `NOT > AND > OR` 构造不可变树，并支持括号、
comparison、BETWEEN、`IS NULL` 与 `IS NOT NULL`。statement 只保存一个
`condition`，不再同时维护 predicate list；IN、LIKE、函数、算术和通用 Pratt
scalar expression 仍未接线。

### 6.2 BoundStatement

`BoundStatement` 统一字段：

- `statementKind`
- `sourceSpan`
- `dictionaryVersions`
- `requiredMetadataLocks`
- `requiredPrivileges`
- `parameterMetadata`
- `resultColumns`
- `statementResourceGuard`

`BoundQuery` 额外保存：

- `queryBlocks`
- `fromBindings`
- `projectionBindings`
- `predicateBindings`
- `orderByBindings`
- `limitBinding`

`BoundDmlStatement` 额外保存：

- target table binding。
- insert/update column mapping。
- assignment coercion。
- write privilege requirement。
- current read / locking read requirement。

## 7. 核心策略和算法

### 7.1 Parse Cache

AST cache key：

`sql_text + sql_mode + charset + collation + identifier_quote_mode`

规则：

- AST cache 只缓存 `StatementAst`。
- AST cache 不因 DDL invalidation 清理，因为它不保存 schema 语义。
- cache 命中后仍必须重新 bind。
- parse cache lock 只短持有，不允许跨 MDL wait。

### 7.2 Name Resolution

NameResolver 输入 AST 和 `DictionaryReadView`：

1. 解析 current schema 和显式 schema name。
2. 为 FROM table 获取 MDL 和 DD pin。
3. 构造 `TableBinding` 并放入 scope。
4. 展开 `*` 和 `table.*`。
5. 解析 column reference，处理 alias、ambiguous column、unknown column。
6. 为子查询或派生表创建子 scope。
7. 输出 `ColumnBinding` 和 `BoundExpression`。

### 7.3 Type Resolution

TypeResolver 处理：

- literal 类型。
- column reference 类型。
- arithmetic、comparison、boolean expression。
- NULLability。
- string collation 和 charset。
- insert/update assignment coercion。
- parameter marker 的期望类型。

类型转换使用 Strategy：

- `NumericCoercionStrategy`
- `StringCollationStrategy`
- `TemporalCoercionStrategy`
- `NullabilityStrategy`
- `AssignmentCoercionStrategy`

### 7.4 Parameter Binding

`ParameterBinder` 在 bind 阶段只生成 `ParameterMetadata`：

- ordinal 从 1 开始。
- 记录期望类型和 nullable。
- 对出现在不同上下文的同一参数，合并或拒绝冲突类型。
- 执行期只绑定参数值，不修改 AST。
- 参数值属于 session prepared statement execution context，不允许跨 session 共享。

### 7.5 Privilege Hook

`PrivilegeHook` 接收权限需求对象：

- `SELECT` columns。
- `INSERT` target columns。
- `UPDATE` target columns。
- `DELETE` target table。
- DDL object privilege。

第一阶段可用 allow-all 实现，但 Binder 必须输出权限需求，避免后续接入权限系统时重写绑定流程。

## 8. 与其它模块的协作

### 8.1 与 Data Dictionary / MDL

- Binder 在语义绑定时获取 MDL，再打开 `DictionaryReadView`。
- `BoundStatement` 记录依赖的 `DictionaryVersion`。
- 旧 statement 的 DD pin 由 `StatementResourceGuard` 释放。
- DDL publish 后，新 statement 绑定到新 version；旧 statement 按 pin 生命周期结束。

### 8.2 与 Optimizer

- Optimizer 读取 `BoundQuery`、`TableBinding`、`ColumnBinding`、`BoundExpression`。
- Optimizer 负责逻辑改写、access path、join order 和 cost。
- Binder 不创建物理执行计划。
- 当前 M3 关系 Binder 递归输出 AND/OR/NOT/null-test 的单一 typed boolean condition；
  `PredicateSet` 的 conjunct/引用列均由该 condition 派生，`RuleProgram` 规范化之后才允许
  `PredicateAnalyzer` 从最外层正向 AND comparison 提取安全 range 约束。
- 当前 JOIN v1 Parser 接受恰好两个输入的 `[INNER] JOIN ... ON qualifier.column =
  qualifier.column`，projection、WHERE 和 ORDER BY 支持一段/两段列引用。Binder 先规范化
  两个表名并按 canonical key 取得 lease，随后以 alias/table qualifier 作用域解析列：
  未限定列必须唯一命中，ON 两列必须 exact type 相等。`BoundColumnReference` 携带
  relation ordinal、stable column id、local ordinal 与 exact DD type；`BoundJoinSelect`
  只保存语义和扁平 statement schema，不选择 join algorithm 或 inner index。

### 8.3 与 Executor

- 关系语句的 Executor 只接收 Optimizer 输出的 `PhysicalPlan`；DDL coordinator 可直接接收独立的 Bound command。
- Executor 不做名称解析和类型推导。
- Executor 通过 `StatementResourceGuard` 确保 statement 结束释放 MDL/DD pin。

### 8.4 与 DDL Coordinator

- DDL Binder 把 DDL AST 转换为 `DdlRequest`。
- `DdlRequest` 包含规范化 object name、列定义、索引定义、锁需求和 source span。
- Atomic DDL 阶段由 DD/DDL 文档定义。

### 8.5 与 Prepared Statement / Plan Cache

- Parser/Binder 负责 prepared statement 的 AST 和参数模板。
- Plan cache 负责计划复用和 version invalidation。
- Prepared statement 的执行期参数值不写入 AST 或 `BoundStatement` 模板。

## 9. 并发与锁顺序

并发状态图见 [parser-binder-concurrency-state.mmd](diagrams/parser-binder-concurrency-state.mmd)。

### 9.1 锁与等待对象

| 对象 | 保护资源 | 持有者 | 死锁域 |
| --- | --- | --- | --- |
| session statement mutex | 当前 session statement 生命周期 | session thread | 不进入事务死锁图 |
| prepared statement mutex | session 内 prepared handle | session thread | timeout/error |
| parse cache lock | AST cache map | parser cache | 不跨 MDL wait |
| MDL ticket | schema/table/tablespace metadata | session/statement | MetadataWaitGraph |
| Dictionary pin | immutable dictionary object version | statement guard | 不进入死锁图 |
| binding scope context | 当前绑定过程 | binder thread | 无等待 |

### 9.2 状态与持有变化

| 状态 | 持有者 | 持有资源 | 进入条件 | 退出条件 |
| --- | --- | --- | --- | --- |
| `IDLE` | 无 | 无 statement 资源 | session 空闲 | 收到 SQL |
| `PARSING` | parser thread | parse cache 短锁可选 | SQL text 需要 parse | AST ready 或 parse error |
| `AST_READY` | parser | immutable AST | parse 完成 | bind 开始 |
| `MDL_REQUESTED` | binder | MDL request 值对象 | 需要语义绑定 | granted 或 waiting |
| `MDL_WAITING` | MDL manager | wait slot、metadata wait edge | 与已有 MDL 冲突 | grant、timeout、victim、killed |
| `MDL_GRANTED` | statement | MDL ticket | 锁兼容 | statement/transaction release |
| `DICT_PINNED` | statement guard | DD object pin、dictionary read view | MDL granted | guard close |
| `BINDING` | binder | scope/type context | DD pin 完成 | bound ready 或 bind error |
| `BOUND_READY` | statement guard | bound statement、MDL、DD pin | bind 成功 | executor/DDL 完成 |
| `RELEASED` | 无 | 无 statement 资源 | guard close、timeout 或 error | 返回 session |

持有变化规则：

- parse cache lock 不能跨 MDL wait。
- 等待 MDL 前不能持有 Buffer Pool page latch、row lock、MTR latch、redo wait 或物理文件锁。
- DD pin 所有权归 `StatementResourceGuard`，不归 AST 或 `TableBinding`。
- prepared statement execute 获取 handle mutex 后，检查 schema version；需要 reprepare 时释放旧 plan 引用再重新 bind。

### 9.3 标准锁顺序

1. session statement mutex。
2. prepared statement handle mutex。
3. parse cache 短锁。
4. schema MDL。
5. table MDL。
6. dictionary object pin。
7. binder scope/type context。

`parse cache lock`、`binder scope context` 是短临界区，不能持有它们进入外部等待。

## 10. 异常处理

异常类型：

- `SqlParseException`
- `UnsupportedSqlSyntaxException`
- `ObjectNameResolutionException`
- `AmbiguousColumnException`
- `UnknownColumnException`
- `TypeResolutionException`
- `CollationConflictException`
- `ParameterTypeConflictException`
- `PrivilegeCheckException`
- `MetadataLockTimeoutException`
- `DictionaryVersionStaleException`
- `PreparedStatementReprepareException`

异常策略：

- parse error 只释放 parse 临时对象，不触发 MDL。
- bind error 必须释放已获取的 MDL ticket 和 DD pin。
- schema version stale：prepared statement 可按 policy rebind；普通 statement 重新绑定或报错。
- privilege failure：释放资源后返回权限错误，不进入 optimizer。
- type conflict：保留 source span，便于错误定位。

## 11. API 设计

### 11.1 SqlParserBinder

- `parse(SqlText sqlText)`
- `bind(StatementAst ast, BindingRequest request)`
- `parseAndBind(SqlText sqlText, BindingRequest request)`
- `prepare(SqlText sqlText, PrepareRequest request)`
- `rebind(PreparedStatementTemplate template, BindingRequest request)`

### 11.2 StatementBinder

- `supports(StatementAst ast)`
- `requiredMetadataLocks(StatementAst ast, BindingContext context)`
- `bind(StatementAst ast, BindingContext context)`
- `cleanupOnFailure(BindingContext context)`

### 11.3 NameResolver

- `resolveTable(TableNameAst, BindingContext)`
- `resolveColumn(ColumnRefAst, ScopeStack)`
- `expandWildcard(WildcardAst, ScopeStack)`
- `openDictionaryReadView(BindingRequest)`

### 11.4 TypeResolver

- `resolveExpression(ExpressionAst, BindingContext)`
- `coerceForComparison(BoundExpression, BoundExpression)`
- `coerceForAssignment(ColumnBinding, BoundExpression)`
- `inferParameterType(ParameterMarkerAst, ExpectedTypeContext)`

## 12. 设计模式使用清单

| 模式 | 应用点 | 价值 |
| --- | --- | --- |
| Facade | `SqlParserBinder` | 统一 parse、bind、prepare、rebind |
| Composite | AST node tree | 表达 SQL 语法层级 |
| Visitor | AST traversal | 不同 binder/resolver 复用 AST 结构 |
| Builder | `BoundStatementBuilder`、`QueryBlockBuilder` | 分阶段构建不可变绑定对象 |
| Chain of Responsibility | name binding phases | schema、table、alias、column 分阶段解析 |
| Strategy | SQL mode、type coercion、collation | 支持不同 session 语义 |
| Snapshot | `DictionaryReadView` | 绑定时读取稳定元数据版本 |
| RAII Guard | `StatementResourceGuard` | 异常路径释放 MDL 和 DD pin |
| State | prepared statement/cache entry | 管理 prepared lifecycle 和 stale 状态 |
| Observer | DDL invalidation listener | 支持 bound/plan cache 失效 |

## 13. 高内聚、低耦合约束

- Lexer 只产生 token，不理解表结构。
- Parser 只产生 AST，不访问 DD、MDL 或权限系统。
- AST 必须不可变，且不保存语义 ID。
- Binder 只做语义绑定，不选择 access path。
- TypeResolver 不读取字典表，只处理已绑定列和表达式。
- ParameterBinder 不保存执行期参数值到 AST。
- PrivilegeHook 只输出和检查权限需求，不实现账号系统。
- Optimizer 接收 `BoundQuery`，不能重新解析 SQL 文本。
- Executor 不能自行补做名称解析。
- 所有 MDL/DD pin 必须由 `StatementResourceGuard` 释放。

## 14. 典型数据流

绑定流程图见 [parser-binder-flow.mmd](diagrams/parser-binder-flow.mmd)。

### 14.1 SELECT

1. SQL text 进入 parse cache。
2. cache miss 时 Lexer 和 Parser 构建 `SelectAst`。
3. Binder 获取 table MDL 和 DD pin。
4. NameResolver 解析 FROM、alias、column 和 wildcard。
5. TypeResolver 推导 projection、predicate、order by 类型。
6. PrivilegeHook 检查 SELECT 需求。
7. 输出 `BoundQuery`。
8. Optimizer 构建 `LogicalPlan`。
9. Executor 执行后释放 statement guard。

### 14.2 INSERT/UPDATE/DELETE

1. Parser 输出 DML AST。
2. Binder 获取 target table `MDL_SHARED_WRITE`。
3. 解析 target columns 和表达式。
4. TypeResolver 执行 assignment coercion。
5. 输出 `BoundDmlStatement`。
6. Optimizer 选择 access path。
7. Executor 调用 storage API。

### 14.3 DDL

1. Parser 输出 DDL AST。
2. DDL Binder 规范化 object name 和列/索引定义。
3. 输出 `DdlRequest`。
4. DDL Coordinator 按 atomic DDL template 获取 MDL、执行 storage action、提交字典事务。

### 14.4 PREPARE / EXECUTE

1. PREPARE parse SQL，生成 AST 和 parameter metadata template。
2. 可选执行初次 bind，记录 dictionary version。
3. EXECUTE 绑定参数值，检查类型和 schema version。
4. 若 stale，按 reprepare policy 重新 bind。
5. 执行完成后释放本次 statement guard，不释放 prepared template。

## 15. 测试设计

- Lexer 测试：关键字、quoted identifier、字符串、注释、parameter marker、source span。
- Parser 测试：SELECT、DML、DDL、EXPLAIN、PREPARE/EXECUTE 基础 AST。
- AST 不可变测试：cache 命中 AST 不被 bind 修改。
- NameResolver 测试：schema/table/alias/column、ambiguous column、unknown column、wildcard。
- TypeResolver 测试：numeric/string/temporal、NULLability、collation、assignment coercion。
- Parameter 测试：ordinal、期望类型、冲突类型、执行期值校验。
- MDL 测试：SELECT/DML/DDL 锁需求、等待、timeout、释放。
- DD version 测试：DDL 后新 statement 绑定新 version，旧 statement pin 旧 version。
- Prepared 测试：schema stale reprepare、参数绑定、deallocate cleanup。
- Privilege 测试：需求对象生成、mock allow/deny。
- 异常 cleanup 测试：bind 中途失败释放 MDL/DD pin。
- 并发测试：parse cache 命中不跨 MDL wait，多个 session 并发绑定。

## 16. 后续实现顺序

1. `Token`、`TokenStream`、`SqlText`、source span。
2. 核心 Lexer。
3. SELECT/DML/DDL 最小 AST。
4. Parser 和 AST visitor。
5. AST parse cache。
6. BindingRequest、BindingContext、StatementResourceGuard。
7. MDL/DD 绑定接口。
8. NameResolver。
9. TypeResolver 和基础 type domain。
10. ParameterBinder。
11. PrivilegeHook。
12. Query/DML/DDL binder。
13. Prepared statement template 和 reprepare。
14. Optimizer/Executor adapter。
15. 并发、异常清理和 property-based 测试。

## 17. 十五轮自检记录

| 轮次 | 检查主题 | 检查结果 |
| --- | --- | --- |
| 1 | 是否只写设计文档 | 本文只新增 Markdown 和 Mermaid 设计内容，没有生成 Java 实现代码 |
| 2 | 目标与非目标 | 已明确 Parser/Binder 不做 optimizer、executor、storage 和账号权限实现 |
| 3 | MySQL 8.0 贴合 | 已覆盖 sql_mode、prepared statement、MDL、Data Dictionary、类型转换和 collation |
| 4 | 高内聚 | Lexer、Parser、AST、NameResolver、TypeResolver、ParameterBinder、PrivilegeHook 职责独立 |
| 5 | 低耦合 | Parser 不依赖 DD，Binder 不依赖 storage，Optimizer 接收语义绑定结果，Executor 只接收物理计划 |
| 6 | 面向对象 | 已定义 Token、AST、BoundStatement、BindingContext、TableBinding、ColumnBinding 等对象 |
| 7 | 设计模式 | 已列出 Facade、Composite、Visitor、Builder、Strategy、Snapshot、Guard 等模式 |
| 8 | 核心领域模型 | 已覆盖语法对象、绑定对象、参数元数据和资源 guard |
| 9 | 依赖方向 | 已明确 parse/bind -> DD/MDL -> optimizer/executor/DDL 的单向链路 |
| 10 | 物理与逻辑区分 | 已区分 SQL text、AST、BoundStatement、LogicalPlan 和 storage physical page |
| 11 | 关键数据流 | 已给出 SELECT、DML、DDL、PREPARE/EXECUTE 流程 |
| 12 | 图示 | 已提供架构图、类关系图、绑定流程图和并发状态图 |
| 13 | 并发锁状态 | 已定义 MDL、parse cache、DD pin、prepared handle 的状态和持有变化 |
| 14 | 异常与恢复 | 已定义 parse/bind/schema stale/privilege/MDL timeout 的清理策略 |
| 15 | 测试与顺序 | 已给出测试设计、实现顺序，并确认没有未完成标记或空白项 |

## 18. CREATE / INSERT / ORDER BY / 基础 DDL 完整语法契约（2026-07-24）

### 18.1 CREATE TABLE 兼容边界

本阶段语法层必须完整接受教学项目选定的 MySQL 8.0 DDL 子集：

- 整数类型允许显示宽度，例如 `BIGINT(20)`、`BIGINT(15)`、`TINYINT(1)`、`TINYINT(4)`；显示宽度只属于
  输入兼容信息，Binder 统一归一化为物理 `length=0`，`TINYINT(1)` 不转换为布尔类型。
- 列约束允许 `NULL`、`NOT NULL`、`DEFAULT <literal>`、`AUTO_INCREMENT`、`COMMENT '<text>'`；
  `AUTO_INCREMENT` 与显式 `DEFAULT` 互斥。
- 表约束允许 `PRIMARY KEY`、`UNIQUE [KEY|INDEX] [name]`、普通 `KEY|INDEX`，并允许
  `USING BTREE` 位于索引列列表之前或之后。当前只接受 `BTREE`，其它算法必须在 parse/bind 阶段显式拒绝。
- `CREATE TABLE (...) COMMENT='<text>'` 写入表选项。列注释按 UTF-8 字节最多 1024，表注释最多 2048；
  长度校验由领域对象再次执行，不能只信任 Parser。
- 暂不支持 `ZEROFILL`、表达式默认值、生成列、外键、分区、FULLTEXT/SPATIAL 和表级字符集覆盖。

AST 必须分别保存列注释、生成方式、表注释和索引算法，不能把这些信息拼回 SQL 文本。Binder 负责验证：
整张表至多一个自增列；其类型必须是有符号或无符号整数、`NOT NULL`，并且是聚簇主键第一列。

### 18.2 默认值规范化

Parser 保留字面量种类，Binder 使用列类型执行严格强制转换并生成唯一 canonical literal。为兼容常见
MySQL 导出语句，数值列的引号数值（例如 `DEFAULT '1'`、`DEFAULT '-1'`）允许转换为数值 canonical literal；
不能完整消费、越界或精度不符时拒绝。字符串 canonical literal 使用单引号且通过双写单引号转义；
`DECIMAL` 使用非科学计数法，时间与时间戳使用稳定格式。Catalog 解码后通过同一 literal parser 重新物化，
保证重启前后默认值语义一致。

### 18.3 多行 INSERT

支持：

```sql
INSERT INTO t (c1, c2) VALUES (v11, v12), (v21, v22);
INSERT INTO t VALUES (v11, v12), (v21, v22);
```

AST 使用“可选列名列表 + 非空行列表”，每行保留独立字面量列表。Binder 先解析目标列映射，再对每一行补齐
未提供列：自增列绑定为 `AutoIncrement`，常量默认值绑定为 canonical 常量，隐式可空列绑定为 `NULL`，
无默认的必填列报错。显式 `NULL` 或数值 `0` 在自增列上同样绑定为 `AutoIncrement`；正整数保留并推进
high-water，负数拒绝。本阶段不支持单元格 `DEFAULT`、`INSERT ... SELECT` 与 `ON DUPLICATE KEY UPDATE`。

### 18.4 ORDER BY / LIMIT

`SELECT` 尾部支持多个源表列的 `ORDER BY column [ASC|DESC]`，以及 `LIMIT count`、
`LIMIT count OFFSET offset`、`LIMIT offset,count`。所有数值必须是可装入非负 `long` 的整数字面量。
AST 保留排序项顺序和方向；Binder 将每项绑定为稳定列标识，不接受表达式、输出别名、序号和
`NULLS FIRST/LAST`。空值语义固定为 ASC 时在前、DESC 时在后。

### 18.5 基础 DDL v2

Parser/Binder 还支持 `CREATE SCHEMA|DATABASE [IF NOT EXISTS]`、`CREATE TABLE [IF NOT EXISTS]`、
`DROP TABLE [IF EXISTS] t1[,t2...]` 与 `DROP SCHEMA|DATABASE [IF EXISTS] name`。名称在 Binder 中按
当前 identifier 规范归一化；同一 `DROP TABLE` 列表中的重复目标必须拒绝。`IF` 只改变“对象已存在/
不存在”的结果为 warning，不吞掉权限、恢复隔离、元数据损坏或锁超时错误。

## 19. 参考链接

- MySQL 8.0 Reference Manual - Prepared Statements: https://dev.mysql.com/doc/refman/8.0/en/sql-prepared-statements.html
- MySQL 8.0 Reference Manual - Character Sets and Collations: https://dev.mysql.com/doc/refman/8.0/en/charset.html
- MySQL 8.0 Reference Manual - Server SQL Modes: https://dev.mysql.com/doc/refman/8.0/en/sql-mode.html
- MySQL 8.0 Reference Manual - Metadata Locking: https://dev.mysql.com/doc/refman/8.0/en/metadata-locking.html
- MySQL 8.0 Reference Manual - MySQL Data Dictionary: https://dev.mysql.com/doc/refman/8.0/en/data-dictionary.html
- MySQL 8.0 Reference Manual - Type Conversion in Expression Evaluation: https://dev.mysql.com/doc/refman/8.0/en/type-conversion.html
