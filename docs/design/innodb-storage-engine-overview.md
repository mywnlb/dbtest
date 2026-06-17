# MiniMySQL InnoDB 风格存储引擎总览设计

版本：2026-06-05  
实现语言：Java  
参考基线：MySQL 8.0.46 InnoDB 官方手册与源码文档  
关联设计：[mysql-parser-binder-design.md](mysql-parser-binder-design.md)、[mysql-prepared-statement-plan-cache-design.md](mysql-prepared-statement-plan-cache-design.md)、[mysql-statistics-analyze-design.md](mysql-statistics-analyze-design.md)、[mysql-query-optimizer-design.md](mysql-query-optimizer-design.md)、[mysql-sql-executor-storage-api-design.md](mysql-sql-executor-storage-api-design.md)、[mysql-advanced-executor-operators-design.md](mysql-advanced-executor-operators-design.md)、[mysql-data-dictionary-ddl-design.md](mysql-data-dictionary-ddl-design.md)、[innodb-crash-recovery-design.md](innodb-crash-recovery-design.md)、[innodb-btree-design.md](innodb-btree-design.md)、[innodb-flush-checkpoint-doublewrite-design.md](innodb-flush-checkpoint-doublewrite-design.md)、[innodb-disk-manager-design.md](innodb-disk-manager-design.md)、[innodb-buffer-pool-design.md](innodb-buffer-pool-design.md)、[innodb-record-design.md](innodb-record-design.md)、[innodb-redo-log-design.md](innodb-redo-log-design.md)、[innodb-transaction-mvcc-design.md](innodb-transaction-mvcc-design.md)、[innodb-undo-log-purge-design.md](innodb-undo-log-purge-design.md)、[mysql-session-connection-protocol-design.md](mysql-session-connection-protocol-design.md)、[mysql-lock-observability-deadlock-design.md](mysql-lock-observability-deadlock-design.md)

## 1. 目标与边界

本设计是 MiniMySQL Java 存储引擎设计文档的全局入口。目标是把 Disk Manager、Buffer Pool、Record、Redo、Transaction/MVCC、Flush、Recovery 以及后续 B+Tree、Undo、Data Dictionary 等模块放到同一张分层图和同一套术语中，明确跨模块依赖、数据流、锁边界、恢复顺序和测试范围。

设计目标：

- 统一项目分层：说明 SQL/session、B+Tree、Record、Transaction、Buffer Pool、Disk Manager、Redo、Flush、Recovery 的职责边界。
- 统一依赖方向：规定允许依赖和禁止反向依赖，避免模块互相读取内部结构。
- 统一数据链路：给出查找、插入、更新、删除、page split、flush、checkpoint、recovery 的跨模块流程。
- 统一并发术语：区分 latch、lock、mutex、condition、wait slot、deadlock domain。
- 统一恢复语义：明确 redo replay、undo rollback、dirty flush、checkpoint 和 purge 的顺序。
- 统一后续文档要求：为 B+Tree、Undo、Data Dictionary 等后续模块提供边界参照。

非目标：

- 不替代各模块详细设计。
- 不定义 Java 源码、接口签名的完整实现。
- 不替代 SQL parser、optimizer、executor 等上层模块的详细设计。
- 不追求与 InnoDB 二进制格式完全兼容。
- 不把所有流程集中到一个 manager；每个模块仍以自己的文档为准。

## 2. MySQL 8.0 参考依据

本总览遵循 MySQL 8.0 InnoDB 的以下核心约束：

- InnoDB 以 tablespace、segment、extent、page 管理物理空间。
- Buffer Pool 以 page 为缓存单位，内部有 page hash、LRU、free list、flush list、dirty LSN。
- Record 在索引页内按 heap 存储，逻辑顺序由 `next_record` 和 Page Directory 表达。
- B+Tree 负责 root 到 leaf 的跨页导航，leaf page 通过 `FIL_PAGE_PREV/NEXT` 支持范围扫描。
- MTR 保护短物理临界区，收集 redo，并管理 page latch、buffer fix 的释放。
- Redo 使用 WAL，数据页刷盘前必须保证对应 redo durable。
- Transaction/MVCC 通过 ReadView、`DB_TRX_ID`、`DB_ROLL_PTR`、undo 和 purge 构造一致性读。
- 当前读和写操作使用 record/gap/next-key/insert intention lock，事务死锁检测只检查 LockManager 等待图。
- Crash recovery 先做 redo replay，恢复物理页一致性，再由事务模块 rollback 未提交事务，最后恢复 purge。

## 3. 全局模块分层架构

架构图见 [storage-engine-overview-architecture.mmd](diagrams/storage-engine-overview-architecture.mmd)。

| 层级 | 模块 | 职责 |
| --- | --- | --- |
| SQL/session | 已设计 | 接收 SQL、管理 session、发起事务和执行计划 |
| Session/连接/协议 | 已设计 | 连接握手、认证、COM 命令分发、autocommit 与事务边界、结果集流式、KILL 与会话清理 |
| Parser/Binder | 已设计 | SQL 文本解析、AST、名称绑定、类型推导、参数元数据、MDL/DD version |
| Prepared Statement / Plan Cache | 已设计 | session 级 prepared statement、模板复用、自动 reprepare、计划缓存失效 |
| Statistics / ANALYZE | 已设计 | ANALYZE TABLE、持久统计、histogram、OptimizerStatsSnapshot |
| Optimizer | 已设计 | 逻辑改写、访问路径选择、代价估算、join order、EXPLAIN |
| Advanced Executor Operators | 已设计 | join、aggregation、filesort、internal temporary table、subquery、window function |
| Data Dictionary | 已设计 | 管理 schema/table/index/version、DDL、MDL、元数据持久化 |
| B+Tree | 已设计 | 跨页索引导航、root/leaf/non-leaf、split/merge、range scan |
| `storage.record` | 已设计 | 页内记录格式、字段编码、PageDirectory、隐藏列、RecordCursor |
| `storage.trx` | 已设计 | 数据库事务、ReadView、MVCC、undo、行锁、purge、rollback |
| Undo/Purge | 已设计 | undo segment/page、insert/update undo、版本链构造、rollback、purge 协调与后台清理 |
| 锁观测/死锁诊断 | 已设计 | 锁与等待事件登记、Wait-For Graph 构建、死锁检测与 victim 选择、锁与事务观测快照 |
| `storage.buf` | 已设计 | page 缓存、frame 生命周期、LRU、dirty tracking、read-ahead、warmup |
| `storage.fsp` | 已设计 | segment/extent/page 分配释放、space header、XDES、INODE |
| `storage.fil` | 已设计 | tablespace registry、PageStore、物理文件扩展和 IO 锁 |
| `storage.mtr` | 已设计 | 短物理事务、memo stack、page latch、buffer fix、redo 收集 |
| `storage.redo` | 已设计 | WAL、LSN、redo buffer、writer/flusher、checkpoint、redo recovery |
| `storage.flush` | 已设计 | dirty page 选择、doublewrite、data file 写出、checkpoint 协作 |
| `storage.recovery` | 已设计 | redo replay、doublewrite 修复、DDL recovery、事务 rollback、purge resume |

## 4. 包与职责总览

| 包 | 对外能力 | 不允许做的事 | 主要模式 |
| --- | --- | --- | --- |
| `storage.api` | 存储引擎门面，连接上层 SQL/session | 暴露内部 frame、inode、redo buffer | Facade |
| `storage.btree` | point lookup、range scan、insert、split、merge | 解析 record byte、分配 extent、判断 MVCC 可见性 | Facade, Strategy, State |
| `storage.record` | 页内 record 查找、编码、隐藏列访问、delete-mark/purge | 分配 page、访问 XDES、持有长期事务锁 | Template Method, Strategy, Adapter |
| `storage.trx` | Transaction、ReadView、LockManager、Undo、Purge | 访问裸文件和 BufferFrame | Unit of Work, Snapshot, Mediator |
| `storage.buf` | `getPage/createPage`、page latch、dirty view、LRU | 解析 segment、record、SQL 语义 | Repository, State, Guard |
| `storage.fsp` | segment/extent/page 分配释放 | 直接使用 FileChannel、解析 record | Repository, Policy |
| `storage.fil` | `PageStore`、data file、autoextend | 修改 XDES/INODE、理解 page body | Adapter, Strategy |
| `storage.mtr` | page latch/fix memo、redo batch、commit order | 数据库事务 rollback、MVCC 判断 | Unit of Work |
| `storage.redo` | LSN、redo encode/write/flush/replay | 调事务锁、访问具体 repository | Command, Ring Buffer |
| `storage.flush` | dirty page 刷盘和 checkpoint 推进 | 绕过 redo durable 边界 | Strategy, Observer |
| `storage.recovery` | crash recovery 编排 | 接收普通用户请求 | Chain of Responsibility |

## 5. 全局核心领域对象

| 对象 | 所属模块 | 语义 |
| --- | --- | --- |
| `SpaceId` | domain/fsp/fil | tablespace 唯一标识 |
| `PageNo` | domain | tablespace 内 page 编号 |
| `PageId` | domain/buf/redo/fsp | `SpaceId + PageNo`，所有页级操作定位键 |
| `ExtentId` | fsp | 连续 page 组成的物理区 |
| `SegmentId` | fsp | 逻辑 segment 归属，由 inode 描述 |
| `RecordRef` | record/btree/trx | 页内记录短期定位值 |
| `IndexRecordRef` | btree/trx | 事务锁的逻辑 record 锁目标 |
| `IndexGapRef` | btree/trx | gap/next-key/insert intention 的逻辑范围目标 |
| `TransactionId` | trx | 数据库事务 ID，对应 `DB_TRX_ID` |
| `RollPointer` | trx/record | 指向 undo record，对应 `DB_ROLL_PTR` |
| `Lsn` | redo/buf/mtr | redo 逻辑序列号 |
| `MiniTransaction` | mtr | 物理短事务，不等于数据库事务 |
| `PageHandle` | buf | page latch 和 buffer fix 的 RAII 风格访问令牌 |
| `PageCursor` | buf/record/fsp | page body 字节访问入口 |

## 6. 全局依赖方向与禁止依赖

依赖图见 [storage-engine-overview-dependency.mmd](diagrams/storage-engine-overview-dependency.mmd)。

推荐调用链：

`sql/session -> trx.api -> btree -> record.api + trx.lock/mvcc/undo + buf.api + fsp.api`

底层链路：

`btree/record/fsp -> buf -> fil -> io`  
`mtr -> redo.api`  
`buf.dirty -> redo durable LSN`  
`flush -> buf dirty view + redo + fil + doublewrite`  
`redo.recovery -> RedoApplyDispatcher -> PageStore / recovery-safe BufferPool`  
`trx.recovery -> redo + undo + core`

禁止依赖：

- Buffer Pool 不能解析 record header、PageDirectory、segment inode 或 SQL 语义。
- Record 不能分配 page、extent、segment，不能直接访问 `BufferFrame` 或裸文件。
- B+Tree 不能修改 XDES、INODE、redo file、事务活跃表。
- Transaction 不能直接操作 `BufferFrame`、`FileChannel`、XDES bitmap。
- Redo record 定义不能依赖具体 repository 实现。
- FIL 不能 import FSP；PageStore 只按 `PageId` 定位文件偏移。
- Flush 不能刷出 redo 尚未 durable 的 page。
- Recovery 不能执行普通 SQL 语义；只恢复物理页和事务状态。

## 7. Page、Record、Undo、Redo 生命周期关系

Page 生命周期：

1. Disk Manager 在 MTR 内分配 page，并更新 space header、XDES、segment inode。
2. Buffer Pool `createPage()` 初始化 page header/body/trailer，并返回 X latch handle。
3. Record/B+Tree/FSP 修改 page body。
4. MTR 收集 redo，commit 后 Buffer Pool 发布 dirty page。
5. Flush 等待 redo durable 后写 doublewrite 和 tablespace data file。
6. Checkpoint 在 dirty page 和 redo closed 边界允许时推进。
7. Recovery 用 pageLSN 幂等判断是否需要重放 redo。

Record 生命周期：

1. B+Tree 定位 leaf page。
2. Record 根据 schema 编码 physical record，维护 heap、`next_record`、PageDirectory。
3. Transaction 写 undo，并更新 `DB_TRX_ID`、`DB_ROLL_PTR`、delete flag。
4. Delete 先 delete-mark，purge 后物理摘除并回收页内空间。
5. Page reorganize 可能使 `heapNo/pageOffset` 失效，长期定位必须回到 key。

Undo/Redo 关系：

- Undo 是逻辑旧版本和 rollback 来源，普通 undo page 自身也需要 redo。
- Redo 是物理 WAL，保证 crash 后 page 可以恢复到一致状态。
- 数据库事务 rollback 依赖 undo；物理页恢复先依赖 redo。

## 8. MTR、WAL、Dirty Page 与 Flush 协议

MTR commit 顺序：

1. 禁止继续获取新 latch。
2. 收集 page operation 和 redo record。
3. Redo 分配连续 LSN。
4. Redo 写入 log buffer，并按策略等待 write/flush。
5. Buffer Pool dirty marker 发布 pageLSN、oldest/newest modification LSN。
6. MTR memo LIFO 释放 page latch、buffer fix、reservation。

WAL 约束：

- `page.newestModificationLsn <= redo.flushedToDiskLsn` 后才允许 data page flush。
- Redo durable 不等于数据库事务一定对外提交；提交语义由 Transaction 模块控制。
- Checkpoint 只能推进到 redo closed、redo flushed、dirty page oldest LSN 的安全交集。

## 9. Transaction、ReadView、Undo、Lock 与 Purge 协议

读协议：

- Consistent read 使用 ReadView，不等待普通行锁。
- Current read 使用最新版本，必须通过 LockManager 获取必要 record/gap/next-key lock。
- 如果当前版本不可见，MVCC 沿 `DB_ROLL_PTR` 读取 undo record 构造旧版本。

写协议：

- 写入前先通过 B+Tree 定位目标 record/gap。
- 进入可能阻塞的行锁等待前，必须释放 page latch、RecordCursor、buffer fix、文件锁、空间管理 latch、undo page latch。
- 行锁授予后必须重新进入 B+Tree 定位并校验记录。
- Update/delete 必须先写 undo，再修改聚簇记录隐藏列和 payload。

Purge 协议：

- Purge 只清理不再被最老 ReadView 需要的 update undo。
- Delete-mark 记录物理删除前必须确认无旧版本需要。
- Purge 使用内部事务和 MTR，批量大小受限，避免长期占用 page latch。

Undo segment/page、insert/update undo、版本链构造、rollback 与 purge 协调的详细设计见 [innodb-undo-log-purge-design.md](innodb-undo-log-purge-design.md)。

## 10. 跨模块典型数据流

一致性读和当前读流程见 [storage-engine-read-flow.mmd](diagrams/storage-engine-read-flow.mmd)。

写入流程见 [storage-engine-write-flow.mmd](diagrams/storage-engine-write-flow.mmd)。

Page split 流程见 [storage-engine-page-split-flow.mmd](diagrams/storage-engine-page-split-flow.mmd)。

Flush 与 checkpoint 流程见 [storage-engine-flush-checkpoint-flow.mmd](diagrams/storage-engine-flush-checkpoint-flow.mmd)。

Crash recovery 流程见 [storage-engine-recovery-flow.mmd](diagrams/storage-engine-recovery-flow.mmd)。

### 10.1 查找

1. Transaction 根据隔离级别创建或复用 ReadView。
2. B+Tree 从 root 下降到 leaf。
3. Buffer Pool 以 S latch 返回 page handle。
4. Record 使用 PageDirectory 二分和 `next_record` 扫描。
5. MVCC 判断隐藏列，必要时读取 undo 构造旧版本。

### 10.2 插入

1. B+Tree 使用 current read 做唯一键检查。
2. LockManager 获取 insert intention lock。
3. 等待后重新定位目标 gap。
4. Record 编码并插入页内 heap。
5. UndoLogManager 写 insert undo。
6. MTR 收集 redo，Buffer Pool 发布 dirty。

### 10.3 更新与删除

1. Current read 定位候选记录。
2. LockManager 获取 record X 或 next-key X。
3. 写 update undo。
4. Record 修改 payload、隐藏列或 delete flag。
5. Key 变化时由 B+Tree 转换为旧 entry delete-mark + 新 entry insert。
6. Purge 后台安全时物理清理 delete-mark 记录。

### 10.4 Page Split

1. B+Tree 发现 leaf 空间不足。
2. Disk Manager 预留并分配新 page。
3. Buffer Pool 创建新 page。
4. Record 移动记录并重建 PageDirectory。
5. B+Tree 更新 sibling link 和 parent separator。
6. MTR redo 记录 allocation、page bytes、parent changes。

## 11. 全局并发锁层级与死锁检测边界

锁层级图见 [storage-engine-lock-hierarchy.mmd](diagrams/storage-engine-lock-hierarchy.mmd)。

统一术语：

| 术语 | 含义 |
| --- | --- |
| `Latch` | 短临界区保护内存结构或 page body，不进入事务死锁检测 |
| `Lock` | 数据库事务逻辑锁，进入 LockManager 等待队列和 Wait-For Graph |
| `Mutex` | 模块内部结构保护，不能跨层等待业务锁 |
| `Condition/Future` | 内部条件等待，如 PageLoadFuture、FreeFrameCondition、redo wait slot |
| `LockRequestState` | `FREE/REQUESTED/GRANTED/WAITING/CONVERTING/TIMEOUT/VICTIM/RELEASED` |
| `OwnershipEvent` | `acquire/upgrade/downgrade/wait/grant/timeout/victim/release/rollbackRelease/cleanupRelease` |

全局顺序：

1. 物理文件 latch：`TablespaceLifecycleLatch -> DataFileHandleLock -> FileSizeLock -> PageIoRangeLock -> FsyncLock`
2. 空间管理 latch：tablespace latch -> space header page latch -> XDES page latch -> segment inode page latch -> data page latch
3. Buffer Pool 内部锁：`pageHashLock -> frameMutex -> freeList/lruList/flushListLock -> pageLatch`
4. 事务锁短锁：`TransactionSystem` -> `Transaction` -> `LockManager` shard lock -> wait queue
5. Redo 内部锁：reservation/CAS -> log buffer segment -> recent trackers -> file write/flush -> checkpoint

死锁检测边界：

- 进入 Wait-For Graph：表意向锁、record lock、gap lock、next-key lock、insert intention lock。
- 不进入 Wait-For Graph：物理文件锁、Buffer Pool latch/mutex、MTR memo、undo page latch、redo wait、PageLoadFuture、FreeFrameCondition、Java monitor。
- 不进入 Wait-For Graph 的等待必须有 timeout、错误广播或重试策略。
- 行锁等待前必须释放 page latch、RecordCursor、buffer fix、文件锁、空间管理 latch、undo page latch。

全局锁持有变化：

| 阶段 | 持有者 | 允许持有 | 必须释放或禁止持有 |
| --- | --- | --- | --- |
| Page read | 当前线程/MTR | page S latch、buffer fix | Buffer Pool list/hash 锁不能跨 IO 等待 |
| Page write | 当前线程/MTR | page X latch、buffer fix、MTR memo | 不能等待事务行锁 |
| Row-lock wait | Transaction/LockManager | 事务等待边、wait slot | page latch、RecordCursor、buffer fix、文件锁、undo page latch |
| Redo wait | 前台事务或 flush 线程 | redo wait slot | redo file lock、Buffer Pool list lock、page latch |
| Flush write | page cleaner | fixed frame 或 page image snapshot、PageStore 文件锁 | 业务事务锁、space metadata latch |
| Autoextend | Disk Manager | `FileSizeLock(X)`、生命周期 S latch | Buffer Pool page latch 等待 |
| Drop/truncate | DDL/cleanup 线程 | `TablespaceLifecycleLatch(X)` | 新 page handle、普通 page IO、dirty page 未 drain 状态 |
| Recovery replay | recovery 线程 | recovery-safe page latch、redo apply context | 用户事务锁等待、普通 SQL 执行 |

锁与等待事件的登记、Wait-For Graph 构建、死锁检测与 victim 选择，以及锁/事务观测快照的详细设计见 [mysql-lock-observability-deadlock-design.md](mysql-lock-observability-deadlock-design.md)。

## 12. Crash Recovery 启动编排

恢复阶段：

1. 加载 tablespace registry 和 redo checkpoint label。
2. 修复 doublewrite 可恢复的 partial page write。
3. 从 checkpoint LSN 后扫描 redo log。
4. RedoApplyDispatcher 按 record type 分发。
5. 通过 pageLSN 幂等跳过已应用 record。
6. 恢复 undo page、事务系统页和索引页物理一致性。
7. TransactionRecoveryService 扫描 recovered active/prepared transaction。
8. 回滚 active transaction。
9. 初始化 ReadViewManager。
10. 启动 purge 和 page cleaner。
11. 接受用户流量。

原则：

- redo recovery 不判断 MVCC 可见性。
- undo rollback 不重放 SQL，只应用 undo command。
- purge 必须等 recovery 完成并确认 ReadView 边界。
- recovery 期间普通用户线程不能进入可修改路径。

## 13. API 门面与模块协作点

| 门面 | 调用方 | 主要能力 |
| --- | --- | --- |
| `StorageEngine` | SQL/session | begin/commit/rollback、open table、lookup、insert、update、delete |
| `BTreeIndexService` | SQL executor / transaction | index lookup、range scan、insert entry、delete-mark、purge physical delete |
| `RecordPageAccessor` | B+Tree | 页内查找、编码、修改、reorganize |
| `TransactionManager` | session / B+Tree | transaction context、ReadView、commit、rollback |
| `LockManager` | B+Tree / Transaction | record/gap/next-key/insert intention lock |
| `BufferPool` | B+Tree / Record / FSP / Flush / Recovery | getPage/createPage/prefetch/dirty candidates |
| `DiskSpaceManager` | B+Tree / FSP clients | allocate/free page, create/drop segment |
| `RedoLogManager` | MTR / Flush / Recovery | append/waitFlushed/checkpoint/replay |
| `FlushCoordinator` | background worker | dirty page flush and checkpoint pressure |
| `RecoveryService` | startup | redo replay、undo rollback、purge resume |

SQL/session 入口的连接握手、认证、协议命令分发、autocommit 与事务边界、结果集流式以及 KILL/会话清理的详细设计见 [mysql-session-connection-protocol-design.md](mysql-session-connection-protocol-design.md)。

## 14. 设计模式使用清单

- Facade：`StorageEngine`、`BTreeIndexService`、`BufferPool`、`TransactionManager`。
- Repository：`PageHashTable`、`TablespaceRegistry`、`SpaceHeaderRepository`、`ExtentDescriptorRepository`。
- Strategy：锁策略、隔离级别策略、extent allocation、replacement、flush neighbor、record codec。
- Template Method：MTR page operation、record operation、redo write、B+Tree split/merge。
- Command：RedoRecord、UndoRecord、RecordPageOperation、Recovery handler。
- State：Transaction state、BufferFrame state、LockRequest state、Redo lifecycle。
- Observer：DirtyPageMarker、FlushObserver、CheckpointObserver、MetricObserver。
- RAII Guard：`PageHandle`、`BufferFixGuard`、`SpaceReservation`。
- Adapter：`PageCursorFieldReader`、`FileChannelPageStore`、doublewrite strategy。
- Chain of Responsibility：Redo apply dispatcher、Recovery handler chain、VersionChainBuilder。

## 15. 高内聚、低耦合约束

强制规则：

- 每个模块只管理自己的内存结构和生命周期。
- 跨模块只能通过门面或值对象传递信息。
- 禁止把跨模块协调集中到一个巨大的 manager。
- 持有内部锁时不能调用可能跨层阻塞的外部模块。
- 进入事务锁等待前必须释放所有 page/file/undo/Buffer Pool 内部短锁。
- 所有会修改持久页的操作必须进入 MTR 和 redo 边界。
- 所有 crash recovery handler 必须幂等。
- 所有设计文档必须包含并发、异常、测试、自检和必要图示。

## 16. 集成测试设计

集成测试应覆盖：

- 查找：聚簇索引点查、二级索引回表、consistent read 旧版本构造。
- 插入：唯一检查、insert intention lock、record insert、redo durable、dirty publish。
- 更新：原地更新、变长搬迁、key 变化 delete-mark + insert。
- 删除：delete-mark、purge 物理删除、page merge/free。
- Page split：leaf split、parent propagation、root split、redo replay。
- Flush：redo durable 前禁止写 data file，doublewrite 后写 tablespace。
- Checkpoint：dirty page 与 redo closed/flushed 边界。
- Recovery：redo replay 幂等、active transaction rollback、purge resume。
- 并发：row lock deadlock、page latch timeout、redo wait、drop/truncate 与 page IO。
- Property-based：随机读写/flush/crash/recovery 后索引顺序、pageLSN、dirty/redo 边界一致。

## 17. 后续实现顺序

推荐顺序：

1. 固定 domain 值对象：`PageId`、`Lsn`、`TransactionId`、`RollPointer`、`RecordRef`。
2. 实现最小 PageStore 和 Disk Manager。
3. 实现最小 Buffer Pool、page latch、PageCursor。
4. 实现 MTR 和 Redo append。
5. 实现 Record 格式和页内查找。
6. 实现 B+Tree lookup 和 insert。
7. 接入 Transaction、ReadView、Undo、LockManager。
8. 实现 dirty tracking、flush、checkpoint。
9. 实现 recovery startup 编排。
10. 扩展 purge、merge、secondary index 和 schema version。

## 18. 十五轮自检记录

| 轮次 | 检查主题 | 检查结果 |
| --- | --- | --- |
| 1 | 不写代码边界 | 只新增 Markdown 和 Mermaid 设计内容，没有生成 Java 源码 |
| 2 | 目标与非目标 | 已明确本文件是全局总览，不替代模块详细设计 |
| 3 | MySQL 8.0 贴合 | 已覆盖 InnoDB page/extent/segment、Buffer Pool、record、MTR、redo、MVCC、lock、recovery |
| 4 | 高内聚 | 每个模块职责独立列出，没有把职责集中到单一 manager |
| 5 | 低耦合 | 已给出允许依赖链和禁止反向依赖 |
| 6 | 面向对象 | 已梳理核心值对象、领域对象和门面接口 |
| 7 | 设计模式 | 已列出 Facade、Repository、Strategy、Template Method、Command、State、Observer、RAII Guard 等模式 |
| 8 | 核心领域模型 | 已覆盖 `PageId`、`Lsn`、`TransactionId`、`RollPointer`、`RecordRef`、`MTR` |
| 9 | 模块依赖方向 | 已明确 `btree/record/fsp -> buf -> fil`、`mtr -> redo`、`flush -> buf+redo+fil` |
| 10 | 物理/逻辑区分 | 已区分物理 page/extent/file、逻辑 segment/record/transaction/lock |
| 11 | 关键数据流 | 已覆盖 read、write、split、flush/checkpoint、recovery |
| 12 | 图示 | 已新增并引用 8 张全局 Mermaid 图 |
| 13 | 并发锁状态 | 已定义统一锁术语、锁层级、死锁检测边界和等待释放规则 |
| 14 | 异常与恢复 | 已说明 redo replay、undo rollback、purge resume 和用户流量开放顺序 |
| 15 | 测试与实现顺序 | 已给出集成测试设计和后续实现顺序，并确认不含未完成占位文本 |

## 19. 参考链接

- MySQL 8.0 Reference Manual - InnoDB Architecture: https://dev.mysql.com/doc/refman/8.0/en/innodb-architecture.html
- MySQL 8.0 Reference Manual - InnoDB File Space Management: https://dev.mysql.com/doc/refman/8.0/en/innodb-file-space.html
- MySQL 8.0 Reference Manual - InnoDB Buffer Pool: https://dev.mysql.com/doc/refman/8.0/en/innodb-buffer-pool.html
- MySQL 8.0 Reference Manual - InnoDB Redo Log: https://dev.mysql.com/doc/refman/8.0/en/innodb-redo-log.html
- MySQL 8.0 Reference Manual - InnoDB Locking: https://dev.mysql.com/doc/refman/8.0/en/innodb-locking.html
- MySQL 8.0 Reference Manual - InnoDB Recovery: https://dev.mysql.com/doc/refman/8.0/en/innodb-recovery.html
