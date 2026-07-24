# MiniMySQL MySQL 8.0 风格 Session / Connection / Protocol 抽象设计

版本：2026-06-06  
实现语言：Java  
参考基线：MySQL 8.0.46 Classic Client/Server Protocol、Connection Phase、Command Phase、Prepared Statements、KILL、InnoDB autocommit  
关联设计：[mysql-parser-binder-design.md](mysql-parser-binder-design.md)、[mysql-prepared-statement-plan-cache-design.md](mysql-prepared-statement-plan-cache-design.md)、[mysql-sql-executor-storage-api-design.md](mysql-sql-executor-storage-api-design.md)、[mysql-data-dictionary-ddl-design.md](mysql-data-dictionary-ddl-design.md)、[innodb-transaction-mvcc-design.md](innodb-transaction-mvcc-design.md)、[innodb-storage-engine-overview.md](innodb-storage-engine-overview.md)

## 1. 目标与边界

本设计面向 MiniMySQL 的 MySQL Session、Connection 和 Classic Protocol 抽象层。该层负责接入客户端连接、完成握手、创建 session、解析 command packet、分发 `COM_QUERY` 与 server-side prepared statement 命令、维护 session variables、管理 statement 生命周期、组织结果流式返回，并在 `KILL QUERY`、`KILL CONNECTION`、断连和异常路径中清理 statement、prepared statement、事务、MDL、DD pin 和网络资源。

设计目标：

- 高内聚：网络连接、协议编解码、session 状态、command 分发、结果流、kill/cleanup、session variable、事务边界分别收敛在明确子包。
- 低耦合：协议层不解析 SQL；Session 不访问 BufferFrame、Record、B+Tree 或裸文件；事务层不生成 packet；Executor 不读写 socket。
- MySQL 8.0 风格：对齐 classic protocol 的 connection phase、command phase、OK/ERR/resultset packet、`COM_QUERY`、`COM_STMT_PREPARE/EXECUTE/CLOSE`、`COM_QUIT`、session `sql_mode`、`autocommit` 和 `KILL` 行为。
- 面向对象：用 `Session`、`ProtocolConnection`、`CommandContext`、`StatementContext`、`ResultStreamer`、`SessionResourceGuard`、`KillToken` 等对象表达状态和生命周期。
- 并发清晰：明确 per-connection worker、session mutex、statement mutex、prepared handle lock、result stream lock、kill flag、事务资源释放顺序。
- 可测试：可以用 fake socket、mock executor、mock transaction service 和 packet golden case 覆盖正常、错误、kill、断连和清理路径。

非目标：

- 不实现完整账号、密码、权限和 TLS/压缩算法，只定义认证、TLS、压缩的 Adapter 边界。
- 不实现 MySQL X Protocol、replication protocol、binlog dump、LOAD DATA LOCAL INFILE 和 stored routine protocol 细节。
- 不替代 Parser/Binder、Prepared Statement、Optimizer、Executor、Transaction、Data Dictionary 或 InnoDB 物理模块。
- 不保证第一阶段能被所有 MySQL 官方客户端无差异接入；第一阶段以协议抽象正确、测试可控、后续可扩展为目标。
- 不生成 Java 源码。本文件只定义设计、关系和数据流。

## 2. MySQL 8.0 参考依据

本设计参考 MySQL 8.0 的以下行为：

- Classic Client/Server Protocol 分为 Connection Phase 和 Command Phase；Connection Phase 交换 capability、可选建立 SSL、完成认证，Command Phase 接收并执行客户端命令。
- `COM_QUERY` 使用 text protocol 发送 SQL，服务端收到后立即执行，并返回 OK、ERR、LOCAL INFILE request 或 text resultset。
- `COM_STMT_PREPARE` 为 SQL 文本创建 prepared statement，成功时返回 statement id、结果列数量和参数数量。
- `COM_STMT_EXECUTE` 通过 statement id 执行 prepared statement，并以 binary protocol value 发送参数值。
- `COM_STMT_CLOSE` 释放 prepared statement，服务端不返回响应 packet。
- OK packet 包含 affected rows、last insert id、status flags、warnings，并可携带 session state tracking 信息；ERR packet 包含 error code、SQL state 和错误消息。
- Text resultset 先发送列定义，再发送 row packet，最终以 EOF 或 OK 结束；如果 metadata 已发送但后续出错，可以用 ERR packet 终止结果。
- `KILL QUERY` 只终止目标连接当前执行的 statement，连接本身保持可用；`KILL CONNECTION` 先终止当前 statement，再关闭连接。
- InnoDB 中用户活动处于事务中；默认 `autocommit` 打开，每条成功 SQL statement 自动提交；关闭 `autocommit` 后 session 总是有事务，连接结束且未显式提交时回滚。
- system variables 有 global 和 session 值，session 变量在连接时从 global 初始化；`sql_mode` 可按 session 设置并影响 SQL 语法和数据校验。

简化点：

| MySQL 8.0 能力 | MiniMySQL 第一阶段 |
| --- | --- |
| 完整 TLS、压缩、认证插件链 | 只定义 `TlsAdapter`、`CompressionAdapter`、`AuthenticationProvider`，默认实现可为无 TLS、无压缩、mock auth |
| 完整 capability matrix | 支持核心 flags：`CLIENT_PROTOCOL_41`、`CLIENT_SSL`、`CLIENT_PLUGIN_AUTH`、`CLIENT_DEPRECATE_EOF`、`CLIENT_SESSION_TRACK`、`CLIENT_MULTI_STATEMENTS` |
| 完整 command set | 先支持 `COM_QUERY`、`COM_STMT_PREPARE`、`COM_STMT_EXECUTE`、`COM_STMT_CLOSE`、`COM_PING`、`COM_INIT_DB`、`COM_QUIT` |
| 完整 binary protocol type | 先覆盖 Parser/Executor 第一阶段支持的数据类型，并保留 type codec 扩展点 |
| 多结果集、server cursor、query attributes | 抽象中保留状态 flags 和 ResultsetEnvelope，第一阶段可关闭或限制 |
| thread-per-connection 完全复刻 | Java 中采用 per-connection worker 抽象，可由 virtual thread、固定线程池或 reactor adapter 承载 |

## 3. 总体架构

架构图见 [session-protocol-architecture.mmd](diagrams/session-protocol-architecture.mmd)。

核心链路：

`ConnectionAcceptor -> ProtocolConnection -> ProtocolAdapter -> SessionManager -> Session -> CommandDispatcher -> Parser/Binder / PreparedStatementManager / SqlExecutor -> ResultStreamer -> PacketWriter`

职责边界：

- `ConnectionAcceptor` 只接受物理连接并注册 `ProtocolConnection`，不保存 SQL、事务或 prepared statement。
- `ProtocolAdapter` 只处理 classic packet 的读取、序列号、capability、OK/ERR/resultset 编码，不解析 SQL 语义。
- `Session` 是逻辑会话聚合根，拥有 session variables、当前 schema、prepared statement registry、transaction handle 引用和当前 statement guard。
- `CommandDispatcher` 把 command byte 转换为 `Command` 值对象，再委托给对应 `CommandHandler`。
- `StatementLifecycleController` 固定 parse/bind/execute/stream/cleanup 的生命周期模板。
- `ResultStreamer` 把 Executor 的逻辑结果转换为 text 或 binary protocol resultset，并处理网络 backpressure、客户端断开和中途错误。
- `KillService` 通过 `ConnectionRegistry` 定位 session，设置 `KillToken`，由各等待点和执行循环协作中止。

逻辑与物理区分：

| 层面 | 对象 | 说明 |
| --- | --- | --- |
| 物理连接 | `ListenerSocket`、`TcpChannel`、`PacketReader`、`PacketWriter`、`NetBuffer` | 只维护字节流、packet sequence、read/write timeout |
| 协议消息 | `HandshakePacket`、`CommandPacket`、`OkEnvelope`、`ErrEnvelope`、`ResultsetEnvelope` | 对 MySQL classic protocol 的结构化表达 |
| 逻辑 session | `Session`、`SessionVariables`、`SqlMode`、`StatementContext`、`PreparedStatementRegistry` | SQL server 语义状态，不保存 socket buffer |
| 执行资源 | `StatementResourceGuard`、`TransactionScope`、`MdlTicket`、`DictionaryPin`、`ResultCursor` | 由 statement 生命周期管理，异常和 kill 路径必须释放 |
| 存储物理实现 | `StorageCursor`、B+Tree page、Record、BufferFrame、Redo、Disk | 只通过 Executor/Storage API 间接访问 |

## 4. 包与职责

| 包 | 职责 | 依赖 | 主要模式 |
| --- | --- | --- | --- |
| `sql.server.net` | accept、socket/channel lifecycle、read/write timeout、connection registry | 无 | Acceptor, Repository |
| `sql.protocol.codec` | packet header、sequence id、capability、OK/ERR/resultset 编解码 | net | Adapter, Strategy |
| `sql.protocol.handshake` | handshake、SSL request、auth exchange、connection attributes | codec, auth | Template Method |
| `sql.protocol.command` | command packet 解析、`Command` 值对象、handler registry | codec, session | Command, Factory |
| `sql.session.core` | `Session` 聚合根、状态机、session id、current schema | variable, tx, prepare | State |
| `sql.session.variable` | session/global variables、`sql_mode`、charset/collation、session state tracking | dd hook | Snapshot, Observer |
| `sql.session.tx` | autocommit、explicit transaction、statement transaction policy | transaction api | Unit of Work, Strategy |
| `sql.session.prepare` | protocol statement id 到 PreparedStatementManager 的 session 适配 | prepared design | Adapter |
| `sql.session.runtime` | `StatementContext`、`SessionResourceGuard`、diagnostics、warning area | executor, mdl | RAII Guard |
| `sql.session.kill` | kill flag、kill reason、kill checkpoint、connection close 编排 | registry, runtime | State, Observer |
| `sql.result.stream` | text/binary resultset metadata、row streaming、backpressure、final OK/ERR | protocol, executor | Iterator, Template Method |
| `sql.protocol.error` | SQL/engine/protocol exception 到 error code/SQL state 映射 | error domain | Mapper |
| `sql.auth.api` | 认证 provider、account context、connection privilege hook | user repo | Strategy, Chain of Responsibility |

禁止方向：

- `sql.protocol.*` 不能依赖 B+Tree、Record、Buffer Pool、Redo、Disk Manager。
- `sql.protocol.codec` 不能调用 Parser/Binder、Optimizer 或 Executor。
- `sql.session.variable` 不能直接修改 Data Dictionary，只发布变量变化事件。
- `sql.session.tx` 不能编码 OK/ERR packet。
- `sql.result.stream` 不能持有 Buffer Pool page latch、RecordCursor 或 MTR latch 等物理短锁跨网络写。
- `sql.exec.*` 不能读取 socket，也不能直接修改 packet sequence。

## 5. 核心领域模型

类关系图见 [session-protocol-class-relation.mmd](diagrams/session-protocol-class-relation.mmd)。

| 对象 | 职责 |
| --- | --- |
| `ConnectionId` | processlist id / session-visible connection id，用于 `CONNECTION_ID()` 和 `KILL` 定位 |
| `ProtocolConnection` | 物理连接生命周期，绑定 channel、packet reader/writer、capabilities |
| `HandshakeContext` | handshake nonce、server/client capability、auth plugin、connection attributes |
| `CapabilitySet` | server 与 client capability 的交集 |
| `AuthenticationContext` | user、host、auth plugin、认证结果、account policy |
| `SessionId` | 内部 session 身份，不随 protocol connection 重用 |
| `Session` | session 聚合根，持有 variables、prepared registry、transaction scope、current statement |
| `SessionState` | `CONNECTING/AUTHENTICATING/READY/RUNNING/KILLING/CLOSING/CLOSED` |
| `SessionVariables` | 当前 schema、`autocommit`、`sql_mode`、charset/collation、timeout、max packet |
| `SessionVariableSnapshot` | statement 开始时读取的稳定变量快照，供 parser/binder/executor 使用 |
| `CommandPacket` | command byte 和 payload 的结构化值对象 |
| `CommandContext` | 当前 command 的 session、capability、sequence、kill token、timeout |
| `StatementContext` | statement id、SQL text、kind、warning area、diagnostic area、resource guard |
| `StatementResourceGuard` | 释放 statement 级 MDL、DD pin、cursor、temporary resource、execution context |
| `PreparedStatementProtocolHandle` | protocol statement id 与 prepared 模块 handle 的映射 |
| `ResultStreamer` | result metadata、row、OK/ERR terminator 的发送控制器 |
| `KillToken` | `NONE/QUERY/CONNECTION` kill intent、reason、requester、version |
| `ConnectionRegistry` | connection id 到 session/connection 的可并发查询索引 |

## 6. 关键数据结构与协议抽象

### 6.1 连接与 packet 物理对象

物理对象只负责字节边界：

| 对象 | 字段或职责 | 约束 |
| --- | --- | --- |
| `NetBuffer` | readable bytes、writable bytes、max packet size | 不保存 SQL 语义 |
| `PacketHeader` | payload length、sequence id | command 开始时 sequence reset |
| `PacketReader` | 读取完整 packet，校验长度和 sequence | read timeout 只触发 connection/protocol error |
| `PacketWriter` | 写入 packet，维护 sequence | 不能在持有存储物理短锁时阻塞写 |
| `CompressionAdapter` | 压缩帧和未压缩 packet 的转换 | 第一阶段可禁用 |
| `TlsAdapter` | SSL request 后升级 channel | 第一阶段可提供 no-op/mock |

### 6.2 协议逻辑对象

| 对象 | 字段或职责 |
| --- | --- |
| `HandshakePacket` | protocol version、server version、connection id、auth data、capabilities、auth plugin |
| `HandshakeResponse` | client capabilities、max packet、charset、username、auth response、database、connection attributes |
| `CommandPacket` | command byte、payload、packet sequence、raw length |
| `OkEnvelope` | affected rows、last insert id、status flags、warnings、info、session state changes |
| `ErrEnvelope` | error code、SQL state、message、fatal flag |
| `ResultsetEnvelope` | metadata policy、column definitions、row format、terminator policy |
| `SessionStateChange` | changed schema、changed system variables、tracking flag |

### 6.3 Command 支持矩阵

| Command | 输入 | 输出 | 主要处理器 |
| --- | --- | --- | --- |
| `COM_QUERY` | text SQL 和可选 query attributes | OK、ERR 或 text resultset | `ComQueryHandler` |
| `COM_STMT_PREPARE` | SQL text | prepare OK 或 ERR | `ComStmtPrepareHandler` |
| `COM_STMT_EXECUTE` | statement id、flags、参数类型和值 | OK、ERR 或 binary resultset | `ComStmtExecuteHandler` |
| `COM_STMT_CLOSE` | statement id | 无响应 | `ComStmtCloseHandler` |
| `COM_INIT_DB` | schema name | OK 或 ERR | `ComInitDbHandler` |
| `COM_PING` | 空 payload | OK | `ComPingHandler` |
| `COM_QUIT` | 空 payload | 关闭连接或 ERR | `ComQuitHandler` |

### 6.4 Session variables

第一阶段 session variables：

- `autocommit`
- `sql_mode`
- `character_set_client`
- `character_set_connection`
- `collation_connection`
- `time_zone`
- `transaction_isolation`
- `lock_wait_timeout`
- `net_read_timeout`
- `net_write_timeout`
- `max_allowed_packet`
- `current_schema`

规则：

- session 创建时从 `GlobalVariableRepository` 复制默认值。
- 每个 statement 开始时创建 `SessionVariableSnapshot`，Parser/Binder/Executor 读取 snapshot，不读取可变 map。
- `SET SESSION sql_mode`、`SET autocommit`、`USE db` 成功后更新 session variables，并通过 OK packet 的 session state tracking 返回变化。
- 改变影响语义绑定的变量后，prepared statement execute 必须重新检查模板是否 stale。

## 7. 核心策略和算法

### 7.1 ConnectionAcceptor 与握手

连接建立流程：

1. `ConnectionAcceptor` 接受 socket，分配 `ConnectionId`，注册到 `ConnectionRegistry`。
2. `ProtocolConnection` 初始化 `PacketReader`、`PacketWriter`、`CapabilitySet` 和 packet sequence。
3. `HandshakeService` 发送 initial handshake。
4. client 可发送 SSL request，`TlsAdapter` 决定是否升级 channel。
5. client 发送 handshake response，服务端计算 capability 交集。
6. `AuthenticationProvider` 校验 user/host/auth response。
7. 认证成功后 `SessionManager` 创建 `Session`，初始化 session variables、current schema、transaction policy 和 prepared registry。
8. 服务端发送 OK packet，进入 command loop。

失败策略：

- capability 不足、auth 失败、packet 格式错误，返回 ERR 后关闭连接。
- 握手阶段还未完成 capability 协商时，ERR packet 可能不携带 SQL state。
- 认证和 TLS 失败不创建 `Session`，只清理 `ProtocolConnection`。

### 7.2 CommandDispatcher

Command 流程图见 [session-protocol-command-flow.mmd](diagrams/session-protocol-command-flow.mmd)。

处理流程：

1. command loop 在 `READY` 状态读取一个 command packet。
2. `CommandPacketDecoder` 解析 command byte 和 payload。
3. `CommandDispatcher` 查找 `CommandHandler`。
4. handler 创建 `CommandContext` 和 `StatementContext`。
5. statement 开始时生成 `SessionVariableSnapshot`，绑定 `KillToken` 当前版本。
6. handler 调用 Parser/Binder、PreparedStatementManager 或 Executor。
7. `ResultStreamer` 写出 OK、ERR 或 resultset。
8. `StatementResourceGuard` 清理 statement 资源。
9. 如果 `KillToken` 为 `QUERY`，清理后 session 回到 `READY`；如果为 `CONNECTION` 或网络断开，进入 `CLOSING`。

规则：

- command handler 不能直接编码 packet，必须返回 `CommandResult` 或交给 `ResultStreamer`。
- 一个 session 同一时刻只允许一个 active statement。
- `COM_QUIT` 不进入 SQL parser，不开启事务。
- 未知 command 返回 protocol ERR，并由 policy 决定是否关闭连接。

### 7.3 `COM_QUERY`

`COM_QUERY` 是 text protocol 入口：

1. 从 payload 解码 SQL text。
2. `StatementLifecycleController` 设置 statement 状态为 `PARSING`。
3. Parser/Binder 使用 session variable snapshot，包括 `sql_mode`、charset、current schema、timeout。
4. DDL、transaction control、SET、USE、DML/DQL 进入不同 handler。
5. 对 SELECT，Executor 返回 `ResultCursor` 和 `ResultSchema`，`ResultStreamer` 发送 text resultset。
6. 对 INSERT/UPDATE/DELETE/DDL/SET/USE，发送 OK packet，携带 affected rows、warnings、status flags 和 session state changes。
7. 出错时映射为 ERR packet；已发送 metadata 后的 streaming error 以 ERR terminator 或连接关闭表示。

### 7.4 Prepared Statement 协议

Prepared 流程图见 [session-protocol-prepared-flow.mmd](diagrams/session-protocol-prepared-flow.mmd)。

`COM_STMT_PREPARE`：

1. 解码 SQL text。
2. 调用 [mysql-prepared-statement-plan-cache-design.md](mysql-prepared-statement-plan-cache-design.md) 中的 `PreparedStatementManager.prepare()`。
3. 在 session 内分配 protocol statement id。
4. 返回 prepare OK：statement id、parameter count、result column count、warning count。
5. 按 capability 决定是否发送参数和列 metadata。

`COM_STMT_EXECUTE`：

1. 根据 statement id 查找 `PreparedStatementProtocolHandle`。
2. 解码 binary parameter values 和 NULL bitmap。
3. 校验参数数量、类型、unsigned flag、charset/collation。
4. 调用 Prepared 模块 execute；如果依赖 version stale，由 prepared 模块执行 reprepare。
5. 结果使用 binary resultset 或 OK/ERR 返回。
6. 每次 execute 都创建新的 `StatementContext` 和 statement resource guard，不复用上次执行的 cursor、ReadView 或 row lock。

`COM_STMT_CLOSE`：

1. 查找 statement id。
2. 标记 prepared handle `DEALLOCATING`，阻止新的 execute acquire。
3. 释放 prepared registry 映射和本 session 持有的模板引用。
4. 不发送响应 packet。

Session close：

- 批量关闭全部 prepared statement。
- 执行中的 prepared statement 先走 statement cleanup，再释放 handle 引用。
- prepared template 不持有事务、MDL、DD pin、StorageCursor 或参数值。

### 7.5 Session variables、SQL mode 与状态追踪

变量更新策略：

- `SET SESSION` 和 `SET @@SESSION` 修改当前 session 变量。
- `SET GLOBAL` 通过 GlobalVariableRepository 修改全局默认值，新连接继承，当前 session 除非显式 SET 不自动改变。
- `sql_mode` 改变后，新 statement 的 parser/binder snapshot 使用新值。
- `autocommit` 从 0 改为 1 时，如果当前 session 有打开事务，`SessionTransactionPolicy` 先提交该事务，再更新变量。
- `USE db` 改变 `current_schema`，成功后通过 OK packet 记录 schema state change。

状态追踪：

- `SessionStateTracker` 记录本 command 内 schema、system variable、transaction state 是否改变。
- 如果 client capability 包含 `CLIENT_SESSION_TRACK`，OK packet 带 state change blocks。
- ERR packet 不提交变量变更，除非语句语义明确要求部分状态已经成功发布；第一阶段使用原子 command 规则避免部分发布。

### 7.6 Autocommit 与事务边界

Autocommit 流程图见 [session-protocol-transaction-autocommit-flow.mmd](diagrams/session-protocol-transaction-autocommit-flow.mmd)。

规则：

- 新连接默认 `autocommit = ON`。
- `autocommit = ON` 且不在显式事务中，DML/DDL 之外的普通 statement 按事务策略决定是否创建只读 statement transaction；DML 成功后自动 commit，失败后 rollback。
- `START TRANSACTION` 或 `BEGIN` 建立 explicit transaction，直到 `COMMIT` 或 `ROLLBACK`。
- `autocommit = OFF` 时，session 总是持有一个 active transaction；`COMMIT` 或 `ROLLBACK` 后立即打开下一事务。
- 连接关闭时，如果 session 仍有未提交事务，必须 rollback，并释放 row locks、MDL、undo statement marker 和 temporary resources。
- DDL 的隐式提交由 [mysql-data-dictionary-ddl-design.md](mysql-data-dictionary-ddl-design.md) 的 DDL coordinator 执行；Session 层只调用事务策略接口，不直接改字典事务。

### 7.7 Result streaming 与 backpressure

Result 流程图见 [session-protocol-result-stream-flow.mmd](diagrams/session-protocol-result-stream-flow.mmd)。

流式返回规则：

1. `ResultStreamer` 先发送 column count 和 column definitions。
2. 如果 client 未声明 `CLIENT_DEPRECATE_EOF`，metadata 后发送 EOF；否则省略 EOF。
3. `ResultCursor` 逐行拉取逻辑行。
4. 每行先在 Executor 层物化为 `ProtocolRow`，再进入 `PacketWriter`。
5. 网络写阻塞时，不能持有 page latch、buffer fix、RecordCursor、MTR latch 或 row-lock manager shard lock。
6. 结果耗尽后，根据 capability 发送 OK 或 EOF terminator，并设置 status flags、warnings、more results 标记。
7. 如果 metadata 已发送后发生执行错误，优先发送 ERR terminator；如果 channel 已失效，关闭连接并执行 cleanup。

资源持有边界：

- statement duration 的 MDL/DD pin 可覆盖 result streaming，保证结果 schema 和 cursor 依赖对象稳定。
- storage 物理短锁不能覆盖 packet write。
- `ResultStreamer` 关闭时必须关闭 `ResultCursor`，再关闭 statement resource guard。

### 7.8 Kill query、连接关闭与 cleanup

Kill 状态图见 [session-protocol-kill-cleanup-state.mmd](diagrams/session-protocol-kill-cleanup-state.mmd)。

`KILL QUERY`：

- `KillService` 根据 processlist id 找到目标 session。
- 权限检查通过后，对目标 session 的 `KillToken` 设置 `QUERY` intent。
- 当前 statement 在 parser loop、MDL wait、row lock wait、executor loop、sort loop、result streaming loop、network write timeout checkpoint 检查 kill flag。
- 命中 kill 后抛出 `QueryKilledException`，statement rollback/cleanup 后 session 回到 `READY`。
- prepared statement handle 保留，执行期参数和 cursor 被释放。

`KILL CONNECTION`：

- 设置 `CONNECTION` intent，并请求 `ProtocolConnection` 停止读取新 command。
- 当前 statement 先按 kill query 路径清理。
- 清理所有 prepared statement、temporary resources、未提交事务和 session MDL。
- 关闭 packet writer、channel，从 `ConnectionRegistry` 移除。

断连与 `COM_QUIT`：

- `COM_QUIT` 是客户端主动关闭，优先走正常 close，不返回 OK。
- 网络断开时不能再尝试写 ERR；直接执行 session cleanup。
- session cleanup 必须幂等，允许 kill、断连、statement error 同时触发。

## 8. 与其它模块的协作

### 8.1 与 Parser/Binder

- `COM_QUERY` 和 `COM_STMT_PREPARE` 把 SQL text、`SessionVariableSnapshot`、current schema、timeout 传给 Parser/Binder。
- Parser/Binder 返回 `BoundStatement` 和 `StatementResourceGuard`。
- Session 不修改 AST 或 `BoundStatement`。

### 8.2 与 Prepared Statement / Plan Cache

- Session 只维护 protocol statement id 到 prepared handle 的映射。
- Prepared 模块负责参数元数据、模板、reprepare、deallocate 和 plan stale 判断。
- Session cleanup 调用 prepared 模块 `closeSession()`，不直接销毁共享 plan cache entry。

### 8.3 与 SQL Executor

- Executor 接收 `StatementContext`、`SessionVariableSnapshot`、transaction scope 和 prepared 参数值。
- Executor 返回 `ExecutionResult`：update count、result cursor、warning area、diagnostic area。
- Executor 不知道 packet sequence，也不写 OK/ERR。

### 8.4 与 Transaction / MVCC

- `SessionTransactionPolicy` 根据 `autocommit`、explicit transaction、statement kind 创建或复用 `TransactionContext`。
- statement 成功、失败、kill、断连分别调用 commit、statement rollback、transaction rollback 或 close rollback。
- Row locks、ReadView、undo marker 的实际释放由事务模块完成。

### 8.5 与 Data Dictionary / MDL

- Session 通过 Parser/Binder/Executor 间接获取 MDL 和 DD pin。
- `KILL QUERY` 命中 MDL wait 时，MDL manager 移除 wait slot 并返回 killed。
- Session cleanup 调用 resource guard 释放 statement/transaction duration MDL，不直接操作 DD cache 内部结构。

### 8.6 与诊断和 processlist

- `ConnectionRegistry` 提供 processlist snapshot：connection id、user、host、current schema、command、state、time、info。
- `StatementContext` 更新可观测状态，例如 `starting`、`executing`、`Sending data`、`Killed`、`Cleaning up`。
- 诊断输出只读 snapshot，不持有 session mutex 进入网络或存储等待。

## 9. 并发与锁顺序

### 9.1 线程与 worker 模型

第一阶段采用 per-connection worker 抽象：

- 每个已认证 connection 在同一时间只有一个 command active。
- worker 可以映射为 Java platform thread、virtual thread 或 reactor task。
- kill、processlist 和 connection close 可由其它管理线程发起，必须通过线程安全的 `KillToken`、`ConnectionRegistry` 和 session lifecycle lock 协作。
- 网络读写阻塞只影响当前 connection worker，不持有全局 registry lock。

### 9.2 锁对象

| 锁/等待 | 保护资源 | 持有者 | 死锁域 |
| --- | --- | --- | --- |
| `ConnectionRegistryLock` | connection id -> session 索引 | acceptor、killer、processlist | 不进入事务死锁图 |
| `ConnectionLifecycleLock` | channel open/closing/closed 状态 | connection worker、killer | timeout/幂等 close |
| `SessionMutex` | session 状态、current statement 指针 | connection worker、kill/cleanup owner | 不进入事务死锁图 |
| `StatementMutex` | 单个 statement 生命周期和 guard | connection worker | timeout/error |
| `PreparedRegistryMutex` | session 内 prepared id map | connection worker、cleanup owner | timeout/error |
| `PreparedHandleLock` | 单个 prepared handle 状态 | execute、close、cleanup | timeout/error |
| `SessionVariableLock` | variable map 和 version | connection worker | 短锁，不跨外部等待 |
| `PacketWriteLock` | packet sequence 和 writer buffer | result streamer | socket timeout，不进入死锁图 |
| `KillToken` | kill intent 和 version | killer、connection worker | CAS，无等待 |
| `MDL` | schema/table metadata | statement/transaction | MetadataWaitGraph |
| `RowLock` | record/gap/next-key | transaction | InnoDB WaitForGraph |

### 9.3 锁状态与持有变化

| 状态 | 持有者 | 持有资源 | 进入条件 | 退出条件 |
| --- | --- | --- | --- | --- |
| `FREE` | 无 | 无锁或无 wait slot | 初始状态 | acquire |
| `GRANTED` | worker 或 cleanup owner | mutex、handle lock、packet writer、MDL ticket | 锁兼容或 CAS 成功 | release、downgrade、cleanup |
| `WAITING` | wait queue owner | wait slot、timeout handle | MDL/row lock/packet write/socket read 冲突或阻塞 | grant、timeout、kill、disconnect |
| `CONVERTING` | worker | prepared handle S->X 或 MDL upgrade request | execute/close 或 DDL 需要升级 | grant、timeout、kill |
| `TIMEOUT` | timeout owner | cleanup right | read/write/lock 超时 | cleanup release |
| `VICTIM` | deadlock detector | victim marker | MDL 或 row lock 死锁检测选中 | rollback release |
| `RELEASED` | 无 | 无有效锁 | 正常 release、rollback release、cleanup release | 回到 FREE |

持有变化规则：

- `acquire`：connection worker 先获取 session/statement 逻辑锁，再进入 Parser/Binder/Executor。
- `upgrade`：prepared execute 持有 handle S 引用；`COM_STMT_CLOSE` 或 cleanup 需要 X/retire marker。升级失败不能持有 packet writer 或 storage 物理短锁等待。
- `downgrade`：reprepare 成功发布新模板后，handle X lock 降级为执行引用或直接释放。
- `wait`：等待 MDL、row lock、socket write 前，必须释放 `ConnectionRegistryLock`、`SessionVariableLock`、parser cache lock 和 prepared registry map 短锁。
- `grant`：MDL/row lock grant 后由对应模块返回给 statement guard 或 transaction context。
- `release`：正常 statement 完成按 result cursor -> statement guard -> statement mutex 的顺序释放。
- `rollback release`：statement error、deadlock victim、kill query 先通知 transaction policy rollback，再释放 row locks、cursor、MDL 和 DD pin。
- `cleanup release`：disconnect、kill connection、session close 关闭 command loop，清理 prepared registry，回滚未提交事务，释放 MDL/DD pin，关闭 channel。

### 9.4 标准锁顺序

1. `ConnectionRegistryLock`。
2. `ConnectionLifecycleLock`。
3. `SessionMutex`。
4. `StatementMutex`。
5. `PreparedRegistryMutex`。
6. `PreparedHandleLock`。
7. `SessionVariableLock`，只用于创建 snapshot 或应用 SET 结果。
8. Parser cache 短锁。
9. MDL。
10. Dictionary pin。
11. Transaction context short lock。
12. Storage API 内部 B+Tree/Record/Buffer/MTR 短锁。
13. Transaction row lock wait。
14. `PacketWriteLock`，只在已有逻辑 row 或 final envelope 后获取。

禁止等待：

- 持有 `ConnectionRegistryLock` 时不能等待 socket、MDL、row lock、prepared handle 或 Executor。
- 持有 `SessionVariableLock` 时不能进入 Parser/Binder、DD、Executor 或网络写。
- 持有 `PacketWriteLock` 时不能调用 StorageCursor.next() 或等待 row lock。
- 持有 Buffer Pool page latch、RecordCursor、MTR latch、物理文件锁时不能执行 packet write、MDL wait 或 session mutex wait。

死锁边界：

- MDL 等待进入 `MetadataWaitGraph`。
- record/gap/next-key/insert intention 等行锁进入 InnoDB `WaitForGraph`。
- session mutex、prepared mutex、packet writer、socket read/write、kill token 不进入事务死锁检测，只能 timeout、disconnect 或幂等 cleanup。
- `KILL QUERY` 不是死锁 victim 选择，它是显式取消信号；命中后必须走 statement rollback 和 cleanup。

## 10. 异常处理

异常类型：

- `ProtocolHandshakeException`
- `AuthenticationRejectedException`
- `CapabilityNegotiationException`
- `MalformedPacketException`
- `PacketTooLargeException`
- `UnknownCommandException`
- `CommandDispatchException`
- `SessionVariableException`
- `SqlModeException`
- `StatementExecutionException`
- `ResultStreamingException`
- `ClientDisconnectedException`
- `QueryKilledException`
- `ConnectionKilledException`
- `PreparedStatementProtocolException`
- `AutocommitTransitionException`
- `SessionCleanupException`

异常策略：

- 握手失败：未创建 session 时只关闭 `ProtocolConnection`；可返回 ERR 时返回 ERR。
- command packet 解析失败：返回 protocol ERR；严重 sequence 或 packet size 错误关闭连接。
- `COM_QUERY` parse/bind/execute 失败：释放 statement guard，按事务策略 rollback，返回 ERR。
- `COM_STMT_PREPARE` 失败：不分配或发布 protocol statement id。
- `COM_STMT_EXECUTE` 参数失败：不进入 Executor，返回 ERR。
- `COM_STMT_CLOSE` 找不到 statement id：第一阶段按兼容策略静默关闭或记录 warning，不返回 packet；严格测试模式可暴露内部诊断。
- result streaming 中网络断开：不再写 ERR，直接关闭 cursor 并执行 session cleanup。
- result streaming 中执行错误且 channel 可写：发送 ERR terminator，然后清理 statement。
- kill query：映射为可识别 SQL error，statement rollback 后 session 保持可用。
- kill connection：清理后关闭 channel，不继续读取 command。
- session close：回滚未提交事务，释放 row locks、prepared statements、temporary resources、MDL、DD pin 和 registry entry。

## 11. API 设计

### 11.1 ConnectionAcceptor

- `start(ServerEndpoint endpoint)`
- `acceptOne()`
- `register(ProtocolConnection connection)`
- `shutdownGracefully()`
- `shutdownNow()`

### 11.2 ProtocolAdapter

- `sendHandshake(HandshakeContext context)`
- `readHandshakeResponse()`
- `readCommandPacket(Session session)`
- `writeOk(OkEnvelope envelope)`
- `writeErr(ErrEnvelope envelope)`
- `writeResultset(ResultsetEnvelope envelope, ResultCursor cursor)`
- `closeConnection(CloseReason reason)`

### 11.3 SessionManager / Session

- `createSession(ProtocolConnection connection, AuthenticationContext auth)`
- `lookup(ConnectionId connectionId)`
- `closeSession(Session session, CloseReason reason)`
- `beginCommand(CommandPacket packet)`
- `finishCommand(CommandResult result)`
- `snapshotVariables()`
- `applyVariableChanges(SessionVariableChanges changes)`

### 11.4 CommandDispatcher

- `dispatch(CommandContext context)`
- `register(CommandCode code, CommandHandler handler)`
- `handleUnknown(CommandPacket packet)`

### 11.5 CommandHandler

- `supports(CommandCode code)`
- `decodePayload(CommandPacket packet, CommandContext context)`
- `execute(CommandContext context)`
- `cleanup(CommandContext context, Throwable failure)`

### 11.6 ResultStreamer

- `streamText(ResultSchema schema, ResultCursor cursor, StatementContext context)`
- `streamBinary(ResultSchema schema, ResultCursor cursor, StatementContext context)`
- `sendOk(CommandResult result, StatementContext context)`
- `sendErr(Throwable failure, StatementContext context)`
- `closeActiveStream(CloseReason reason)`

### 11.7 KillService

- `killQuery(ConnectionId target, Session requester)`
- `killConnection(ConnectionId target, Session requester)`
- `checkpoint(StatementContext context)`
- `clearAfterStatement(Session session)`

### 11.8 SessionTransactionPolicy

- `beforeStatement(Session session, StatementKind kind)`
- `afterStatementSuccess(Session session, StatementContext context)`
- `afterStatementFailure(Session session, StatementContext context, Throwable failure)`
- `startExplicitTransaction(Session session)`
- `commit(Session session)`
- `rollback(Session session)`
- `closeSessionRollback(Session session)`

## 12. 设计模式使用清单

| 模式 | 应用点 | 价值 |
| --- | --- | --- |
| Facade | `SessionManager`、`ProtocolAdapter`、`ResultStreamer` | 对外提供稳定入口，隐藏内部状态机 |
| Adapter | TLS、compression、auth、prepared manager、executor result 到 protocol | 隔离外部模块和协议细节 |
| Strategy | command handler、packet codec、auth provider、autocommit policy、EOF/OK terminator policy | 支持能力协商和配置差异 |
| Command | `CommandPacket`、`CommandHandler` | 把 protocol command 表达为可分发请求 |
| State | `SessionState`、statement lifecycle、kill cleanup state | 明确运行、kill、关闭和清理转换 |
| Template Method | handshake、statement lifecycle、result streaming | 固定公共阶段，允许命令差异扩展 |
| Observer | session state tracker、processlist diagnostics、kill notification | 解耦状态变化与观察输出 |
| Repository | `ConnectionRegistry`、`SessionVariableRepository` | 统一查询连接和变量默认值 |
| Snapshot | `SessionVariableSnapshot` | statement 使用稳定 session 语义 |
| RAII Guard | `StatementResourceGuard`、`SessionCloseGuard`、`ResultStreamGuard` | 异常和 kill 路径释放资源 |
| Unit of Work | `SessionTransactionPolicy` | 组织 statement/transaction commit rollback |
| Chain of Responsibility | auth provider chain、error mapper chain | 多认证方式和错误映射可扩展 |
| Factory | `CommandHandlerFactory`、`PacketCodecFactory` | 根据 command/capability 创建处理器 |

## 13. 高内聚、低耦合约束

- ConnectionAcceptor 只管理物理连接，不创建 SQL plan 或事务。
- Protocol codec 只做 packet 编解码，不解析 SQL 语义。
- Session 是逻辑会话聚合根，不保存 NetBuffer 内部数组和 packet 读写细节。
- CommandDispatcher 只分发命令，不执行 Parser/Binder、Optimizer 或 Storage 内部逻辑。
- Prepared protocol adapter 只处理 statement id 和参数 packet，不保存执行期 cursor、row lock 或 ReadView。
- ResultStreamer 只写结果，不选择访问路径，不持有存储物理短锁跨网络写。
- Transaction policy 只编排事务边界，不生成 OK/ERR packet。
- Parser/Binder/Executor 不依赖 socket、packet sequence 或 client capability。
- InnoDB 模块不依赖 session variables map，只接收已规范化的 transaction context 和 storage request。
- 所有 statement 级资源必须挂在 `StatementResourceGuard` 或 `SessionCloseGuard` 下，不允许散落在 handler 局部变量后遗失。

## 14. 典型数据流

### 14.1 连接建立

1. `ConnectionAcceptor` 接受 TCP 连接。
2. `ProtocolConnection` 分配 connection id，注册 processlist。
3. `HandshakeService` 发送 handshake，协商 capability、SSL 和认证。
4. `AuthenticationProvider` 返回 account context。
5. `SessionManager` 创建 session，并从 global variables 初始化 session variables。
6. 发送 OK packet，进入 command loop。

### 14.2 `COM_QUERY`

见 [session-protocol-command-flow.mmd](diagrams/session-protocol-command-flow.mmd)。

1. 读取 command packet。
2. 创建 `StatementContext` 和 variable snapshot。
3. Parser/Binder 解析和绑定 SQL。
4. Transaction policy 创建或复用事务。
5. Executor 执行。
6. ResultStreamer 发送 OK、ERR 或 text resultset。
7. Statement guard 清理资源。
8. Autocommit 成功提交或失败回滚。

### 14.3 Prepared statement

见 [session-protocol-prepared-flow.mmd](diagrams/session-protocol-prepared-flow.mmd)。

1. `COM_STMT_PREPARE` 创建 session 内 handle 并返回 statement id。
2. `COM_STMT_EXECUTE` 解码参数并调用 prepared 模块 execute。
3. stale 时 prepared 模块 reprepare。
4. ResultStreamer 使用 binary resultset 返回结果。
5. `COM_STMT_CLOSE` 释放 handle 且不返回 packet。
6. Session close 释放所有剩余 handle。

### 14.4 Autocommit DML

见 [session-protocol-transaction-autocommit-flow.mmd](diagrams/session-protocol-transaction-autocommit-flow.mmd)。

1. DML statement 开始，`autocommit = ON` 且无显式事务。
2. Transaction policy 打开 statement transaction。
3. Executor 写 undo/redo 并返回 affected rows。
4. 成功时 commit 并释放 row locks。
5. 失败或 kill 时 rollback 并释放资源。
6. ResultStreamer 返回 OK 或 ERR。

### 14.5 Result streaming

见 [session-protocol-result-stream-flow.mmd](diagrams/session-protocol-result-stream-flow.mmd)。

1. 发送 metadata。
2. 从 `ResultCursor` 拉取 row。
3. 物化为 protocol row。
4. 获取 packet write lock，发送 row packet。
5. 网络 backpressure 只阻塞当前 worker。
6. 完成后发送 OK/EOF terminator。
7. 清理 cursor 和 statement guard。

### 14.6 Kill 与关闭

见 [session-protocol-kill-cleanup-state.mmd](diagrams/session-protocol-kill-cleanup-state.mmd)。

1. 管理 session 执行 `KILL QUERY` 或 `KILL CONNECTION`。
2. `KillService` 设置目标 session kill token。
3. 目标 worker 在 checkpoint 发现 kill intent。
4. statement rollback，释放 cursor、MDL、DD pin、temporary resource。
5. `KILL QUERY` 后 session 回到 ready。
6. `KILL CONNECTION` 或断连继续执行 session close，释放 prepared registry 和事务，再关闭 channel。

## 15. 测试设计

- Handshake 测试：capability 协商、SSL request 分支、auth 成功、auth 失败、capability 不足。
- Packet codec 测试：packet header、sequence reset、max packet、OK/ERR、text resultset、binary resultset、EOF/OK terminator。
- Command dispatcher 测试：`COM_QUERY`、`COM_STMT_PREPARE`、`COM_STMT_EXECUTE`、`COM_STMT_CLOSE`、`COM_INIT_DB`、`COM_PING`、`COM_QUIT`、未知 command。
- Session variables 测试：连接初始化、`SET SESSION sql_mode`、`SET autocommit`、`USE db`、state tracking。
- SQL mode 测试：snapshot 被 Parser/Binder 使用，statement 执行期间修改变量不影响当前 statement。
- Autocommit 测试：默认 autocommit、显式事务、`autocommit` 0->1 自动提交、连接关闭回滚。
- Prepared protocol 测试：prepare OK metadata、execute 参数数量和类型、close 无响应、session close 释放全部 handle。
- Result streaming 测试：metadata、row、terminator、中途执行错误发送 ERR terminator、网络断开 cleanup。
- Kill query 测试：parser checkpoint、MDL wait、row lock wait、executor loop、sort loop、result streaming loop 命中 kill。
- Kill connection 测试：停止读取新 command、当前 statement rollback、prepared registry cleanup、未提交事务 rollback、registry 移除。
- 并发测试：processlist snapshot 与 session 执行并发、execute 与 close prepared 并发、kill 与 disconnect 并发、cleanup 幂等。
- 资源释放测试：parse error、bind error、executor error、stream error、socket timeout、client disconnect 均释放 guard。
- 协作测试：mock Parser/Binder、Prepared、Executor、Transaction、MDL 验证调用顺序。
- Golden packet 测试：固定输入 packet 与输出 packet 字节序列，覆盖常用 capability 组合。

## 16. 后续实现顺序

1. `ConnectionId`、`ProtocolConnection`、`ConnectionRegistry` 和 fake channel。
2. Packet header、sequence、OK/ERR codec、max packet 校验。
3. HandshakeContext、CapabilitySet、AuthenticationProvider mock。
4. `Session`、`SessionVariables`、`SessionVariableSnapshot`。
5. `CommandPacket`、`CommandDispatcher`、handler registry。
6. `COM_PING`、`COM_QUIT`、`COM_INIT_DB` 最小命令。
7. `COM_QUERY` 到 Parser/Binder/Executor mock 的最小链路。
8. `ResultStreamer` text resultset 和 OK/ERR terminator。
9. `SessionTransactionPolicy` 和 autocommit/explicit transaction。
10. Prepared protocol id registry 和 `COM_STMT_PREPARE`。
11. `COM_STMT_EXECUTE` binary 参数解码和 binary resultset。
12. `COM_STMT_CLOSE`、deallocate、session close cleanup。
13. `KillToken`、`KillService` 和 checkpoint。
14. processlist snapshot、diagnostic area、warning area。
15. 并发、断连、timeout、golden packet 和端到端集成测试。

## 16.1 2026-07-21 恢复导出只读会话边界

- 公共组合根在 FORCE 对象隔离后发布 `RECOVERY_EXPORT_READ_ONLY`，在普通启动仍有隔离对象时发布
  `DEGRADED`；模式与不可用表清单是本次 open 的稳定快照。
- 导出只读只允许无锁普通 SELECT；目标表由 DD 返回 `TableRecoveryUnavailableException`，健康表仍可导出。
- `FOR SHARE/FOR UPDATE`、INSERT/UPDATE/DELETE、DDL、COMMIT/ROLLBACK/SAVEPOINT、XA 等可能建立写事务、
  事务锁或持久状态的命令，在 Session 分类和 `DefaultSqlStorageGateway` 二次守门处均被拒绝。
- 低层 `StorageWriteAdmission` 继续封锁 MTR/checkpoint/后台 worker，防止新增 Session 语法或 Java 调用绕过
  Session 分类。本 v1 没有权限系统、网络协议管理命令或 SQL 级 recovery backup/import 语法。

## 17. 十五轮自检记录

| 轮次 | 检查主题 | 检查结果 |
| --- | --- | --- |
| 1 | 是否只写设计文档 | 本文只新增 Markdown 和 Mermaid 设计内容，没有生成 Java 源码 |
| 2 | 目标与非目标 | 已明确 session/connection/protocol 目标，排除完整认证、X Protocol、replication 和 storage 实现 |
| 3 | MySQL 8.0 贴合 | 已覆盖 connection phase、command phase、`COM_QUERY`、prepared 命令、OK/ERR/resultset、`KILL`、`autocommit`、`sql_mode` |
| 4 | 高内聚 | 网络、协议、session、变量、事务边界、结果流、kill cleanup、错误映射职责独立 |
| 5 | 低耦合 | 协议不解析 SQL，Session 不访问存储物理对象，Executor 不写 socket |
| 6 | 面向对象 | 已定义 `ProtocolConnection`、`Session`、`CommandContext`、`StatementContext`、`ResultStreamer`、`KillToken` 等对象 |
| 7 | 设计模式 | 已列出 Facade、Adapter、Strategy、Command、State、Template Method、Observer、Guard、Unit of Work 等模式 |
| 8 | 核心领域模型 | 已覆盖 connection、handshake、session variables、command、statement、prepared handle、result stream、kill token |
| 9 | 模块依赖方向 | 已明确 `net -> protocol -> session -> parser/prepared/executor -> storage api` 的单向协作 |
| 10 | 物理与逻辑区分 | 已区分 socket/packet/net buffer、protocol envelope、logical session、statement resource 和 InnoDB physical objects |
| 11 | 关键数据流 | 已给出连接建立、`COM_QUERY`、prepared、autocommit、result streaming、kill cleanup 流程 |
| 12 | 必要图示 | 已提供架构图、类关系图、command flow、prepared flow、transaction flow、kill state、result stream flow |
| 13 | 并发锁状态 | 已定义 FREE、GRANTED、WAITING、CONVERTING、TIMEOUT、VICTIM、RELEASED 状态和 acquire/upgrade/wait/release/rollback/cleanup 持有变化 |
| 14 | 异常处理和恢复 | 已覆盖 handshake、packet、SQL、prepared、stream、kill、disconnect 和 session close cleanup 策略 |
| 15 | 测试与实现顺序 | 已给出测试设计、后续实现顺序，并确认没有未完成标记或空白项 |

## 18. DDL warning、生成键与会话边界（2026-07-24）

### 18.1 结果传播

内部 `CommandResult` 携带不可变 warning 列表；DML `UpdateResult` 携带可选第一生成键。无论 Session
追加 transaction status、autocommit 状态或协议 flags，都必须保留这两个 payload。协议层后续可以把
warning count 和生成键编码进 OK packet；当前 Java API 已保证信息不在分层间丢失。

### 18.2 DDL 隐式提交

CREATE/DROP（包括 `IF EXISTS/IF NOT EXISTS` 命中的 no-op）统一遵循：

1. 语句执行前结束当前显式事务；提交失败则不进入 DDL。
2. DDL 使用自己的 statement/DDL guard 完成或恢复到可判定状态。
3. 语句后 session 回到无活动事务；warning 不改变成功状态。

删除当前 session 的 current schema 后立即清空选择；其它 session 不被主动改写，但下一次 bind 必须通过
DD 版本/存在性校验失败，不能继续使用缓存对象。

### 18.3 排序取消

每个排序语句从 session cancellation token 派生独立 sort guard。客户端断开、KILL、执行异常和 result
consumer 提前关闭都必须关闭 executor tree，并在 session 资源释放完成前删除该语句的 spill 文件。
临时文件清理失败记录带实例/statement identity 的诊断日志，但不得覆盖原始 SQL 异常。

## 19. 参考链接

- MySQL 8.0.46 Source Documentation - Client/Server Protocol: https://dev.mysql.com/doc/dev/mysql-server/8.0.46/PAGE_PROTOCOL.html
- MySQL 8.0.46 Source Documentation - Connection Phase: https://dev.mysql.com/doc/dev/mysql-server/8.0.46/page_protocol_connection_phase.html
- MySQL 8.0.46 Source Documentation - COM_QUERY: https://dev.mysql.com/doc/dev/mysql-server/8.0.46/page_protocol_com_query.html
- MySQL 8.0.46 Source Documentation - COM_STMT_PREPARE: https://dev.mysql.com/doc/dev/mysql-server/8.0.46/page_protocol_com_stmt_prepare.html
- MySQL 8.0.46 Source Documentation - COM_STMT_EXECUTE: https://dev.mysql.com/doc/dev/mysql-server/8.0.46/page_protocol_com_stmt_execute.html
- MySQL 8.0.46 Source Documentation - COM_STMT_CLOSE: https://dev.mysql.com/doc/dev/mysql-server/8.0.46/page_protocol_com_stmt_close.html
- MySQL 8.0.46 Source Documentation - OK_Packet: https://dev.mysql.com/doc/dev/mysql-server/8.0.46/page_protocol_basic_ok_packet.html
- MySQL 8.0.46 Source Documentation - ERR_Packet: https://dev.mysql.com/doc/dev/mysql-server/8.0.46/page_protocol_basic_err_packet.html
- MySQL 8.0.46 Source Documentation - Text Resultset: https://dev.mysql.com/doc/dev/mysql-server/8.0.46/page_protocol_com_query_response_text_resultset.html
- MySQL 8.0 Reference Manual - KILL Statement: https://dev.mysql.com/doc/refman/8.0/en/kill.html
- MySQL 8.0 Reference Manual - autocommit, Commit, and Rollback: https://dev.mysql.com/doc/refman/8.0/en/innodb-autocommit-commit-rollback.html
- MySQL 8.0 Reference Manual - Using System Variables: https://dev.mysql.com/doc/refman/8.0/en/using-system-variables.html
- MySQL 8.0 Reference Manual - Server SQL Modes: https://dev.mysql.com/doc/refman/8.0/en/sql-mode.html
