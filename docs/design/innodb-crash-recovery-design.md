# MiniMySQL InnoDB 风格 Crash Recovery 启动编排设计

版本：2026-06-05  
实现语言：Java  
参考基线：MySQL 8.0.46 InnoDB Recovery、Redo Log、Doublewrite、Undo、Atomic DDL  
关联设计：[innodb-storage-engine-overview.md](innodb-storage-engine-overview.md)、[innodb-flush-checkpoint-doublewrite-design.md](innodb-flush-checkpoint-doublewrite-design.md)、[innodb-redo-log-design.md](innodb-redo-log-design.md)、[innodb-transaction-mvcc-design.md](innodb-transaction-mvcc-design.md)、[mysql-data-dictionary-ddl-design.md](mysql-data-dictionary-ddl-design.md)、[innodb-buffer-pool-design.md](innodb-buffer-pool-design.md)、[innodb-disk-manager-design.md](innodb-disk-manager-design.md)、[innodb-undo-log-purge-design.md](innodb-undo-log-purge-design.md)

## 1. 目标与边界

本设计面向 MiniMySQL 的 `storage.recovery` 模块。它不是 redo、doublewrite、事务、DDL 或 Buffer Pool 的替代品，而是数据库启动时的恢复总控，负责把各模块已经定义的恢复能力按 MySQL/InnoDB 风格串成一个可验证、可中断、可诊断的启动流程。

设计目标：

- 高内聚：启动门控、恢复阶段编排、恢复上下文、恢复进度、错误策略和恢复指标都收敛在 `storage.recovery`。
- 低耦合：Recovery 只调用 Redo、Flush doublewrite、Disk PageStore、Buffer Pool recovery access、Transaction recovery、DD recovery 的稳定接口，不读取内部链表、redo buffer、row lock queue 或字典 cache 私有结构。
- MySQL 8.0 风格：对齐 tablespace discovery、doublewrite 修复、checkpoint 后 redo replay、未提交事务 rollback、prepared transaction 保留、purge resume 和用户流量开放顺序。
- 可恢复：所有恢复 handler 必须幂等，使用 pageLSN、DDL log phase、transaction state 和 history list 边界判断是否已经应用。
- 并发清晰：恢复期普通 SQL 被 `RecoveryTrafficGate` 阻断，物理文件锁、Buffer Pool latch、redo reader、DDL recovery、事务 rollback 的锁状态和持有者变化必须明确。
- 可测试：支持故障注入、crash point 重启、损坏页、损坏 redo、未完成 DDL、active transaction rollback、prepared transaction 恢复和 purge resume 测试。

非目标：

- 不解析普通 SQL，不重新执行用户语句。
- 不定义 redo record 的二进制编码细节；该职责属于 `storage.redo`。
- 不实现 doublewrite 文件格式；该职责属于 `storage.flush.doublewrite`。
- 不实现 undo record 具体编码和 MVCC 可见性；该职责属于 `storage.trx.undo` 和 `storage.trx.mvcc`。
- 不替代 DDL log recovery；Recovery 只决定阶段顺序并调用 `DdlRecoveryService`。
- 不生成 Java 源码。本文件只定义设计、关系和数据流。

## 2. MySQL 8.0 参考依据

本设计参考 MySQL 8.0 的以下行为：

- InnoDB crash recovery 在 server 接受连接前执行，核心包括 tablespace discovery、redo log application 和未提交事务 rollback。
- Redo recovery 从最近 checkpoint label 后扫描 redo log，遇到不完整尾部时停止在最后一个完整可校验 record。
- Redo application 使用 pageLSN 判断幂等性；pageLSN 已经覆盖目标 redo record 时跳过。
- Doublewrite 用于修复 torn page；修复后仍要按 redo 从 checkpoint 后补齐已写入 redo 的修改。
- Undo page、事务系统页和索引页先通过 redo 恢复到物理一致状态，然后事务恢复模块识别 committed、active、prepared transaction。
- Active transaction 通过 undo rollback 撤销；prepared transaction 在完整 XA 或上层协调器未决定前保留。
- Atomic DDL 通过 DDL log 处理崩溃点，redo 先恢复字典表页和 DDL log 页，再由 DDL recovery finish 或 cleanup。
- Purge 在恢复完成后继续处理 history list；启动时没有用户 ReadView，purge 边界由恢复出的事务系统状态决定。

简化点：

| MySQL 8.0 能力 | MiniMySQL 第一阶段 |
| --- | --- |
| 完整 force recovery 等级 | 先支持 `NORMAL`、`READ_ONLY_VALIDATE`、`FORCE_SKIP_CORRUPT_TABLESPACE` |
| 完整 XA 和 binlog 协调 | 保留 prepared transaction，binlog participant 作为 DD 文档扩展点 |
| 复杂 parallel redo apply | 先定义可按 page 分片的接口，第一阶段可串行执行 |
| 加密、压缩、压缩页恢复 | 保留 page codec 和 checksum strategy 扩展点 |
| 全量 Performance Schema recovery 诊断 | 先提供 `RecoveryMetricsSnapshot` 和阶段进度 |

## 3. 总体架构

架构图见 [crash-recovery-architecture.mmd](diagrams/crash-recovery-architecture.mmd)。

核心链路：

`RecoveryBootstrap -> RecoveryTrafficGate -> CrashRecoveryService -> RecoveryStageRegistry -> stage chain -> open user traffic`

恢复阶段：

1. `TablespaceDiscoveryStage`：加载 tablespace registry、space header、space version、redo checkpoint label。
2. `DoublewriteRepairStage`：扫描 doublewrite slot，修复可恢复 torn page。
3. `RedoReplayStage`：从 checkpoint LSN 后扫描并应用物理 redo。
4. `DdlRecoveryStage`：扫描 DDL log，finish/rollback 未完成 DDL，重建字典 cache。
5. `TransactionRollbackStage`：重建事务系统，rollback recovered active transaction，保留 prepared transaction。
6. `PurgeResumeStage`：重建 history list cursor，启动 purge 前置检查。
7. `BackgroundWorkerResumeStage`：启动 page cleaner、checkpoint worker、buffer pool warmup，再开放用户流量。

职责划分：

- Recovery 负责阶段顺序、门控、上下文、进度和错误策略。
- Redo 负责读取、解析、校验和按 record type 分发 redo。
- Flush doublewrite 负责 doublewrite slot 扫描和 torn page 修复。
- Disk Manager 负责 tablespace discovery、PageStore 读写和物理文件锁。
- Buffer Pool 只提供 recovery-safe page access，不开放普通 LRU/dirty 业务路径。
- Transaction 模块负责 undo rollback、prepared transaction 和 purge state。
- Data Dictionary 模块负责 DDL log 解释和 dictionary cache rebuild。

## 4. 包与职责

| 包 | 职责 | 依赖 | 主要模式 |
| --- | --- | --- | --- |
| `storage.recovery.api` | `CrashRecoveryService`、启动入口、恢复状态查询 | stage, gate | Facade |
| `storage.recovery.stage` | 恢复阶段接口、顺序注册、阶段结果 | context | Chain of Responsibility, Template Method |
| `storage.recovery.context` | checkpoint、mode、tablespace、错误策略、metrics | domain | Context Object |
| `storage.recovery.gate` | 阻断和开放用户流量、只读诊断门控 | server/session | State |
| `storage.recovery.lock` | 恢复期锁协调、锁状态快照、timeout 策略 | fil, buf, redo | Mediator |
| `storage.recovery.progress` | 阶段进度、可观测事件、恢复日志 | metric | Observer, Journal |
| `storage.recovery.policy` | force recovery、corruption、retry、fail-fast 策略 | config | Strategy |
| `storage.recovery.metric` | redo apply 数、rollback 数、耗时、失败点 | 无 | Observer |

禁止方向：

- `storage.recovery` 不能 import SQL parser、optimizer 或 executor。
- `storage.recovery` 不能直接访问 `BufferFrame`、redo log buffer、LockManager wait queue、DictionaryObjectCache 内部节点。
- `storage.recovery` 不能在持有物理文件锁时等待事务行锁或 MDL。
- `storage.recovery` 不能决定 MVCC 可见性，只能要求事务模块 rollback 或 resume purge。

## 5. 核心领域模型

类关系图见 [crash-recovery-class-relation.mmd](diagrams/crash-recovery-class-relation.mmd)。

| 对象 | 职责 |
| --- | --- |
| `RecoveryRequest` | 启动恢复输入，包括 mode、force policy、target data directory |
| `RecoveryContext` | 贯穿所有阶段的不可乱写上下文，保存 checkpoint、recoveredToLsn、tablespace set、错误策略 |
| `RecoveryStage` | 阶段接口，固定 `run()`、失败清理和进度上报 |
| `RecoveryStageResult` | 阶段输出：完成、跳过、失败、进入只读诊断 |
| `RecoveryTrafficGate` | 启动门控，控制普通 SQL、只读诊断和后台 worker 的开放时机 |
| `RecoveryLockCoordinator` | 记录恢复期短锁持有状态，保证物理锁、page latch、redo reader 顺序 |
| `RecoveryProgressJournal` | 内存和可选文件中的阶段进度记录，用于诊断不是用于跳过恢复 |
| `RecoveryErrorPolicy` | 根据 mode 决定 corrupt page、缺失 tablespace、损坏 DDL log 的处理 |
| `RecoveredTransactionSet` | 事务恢复输出，区分 committed、active、prepared、history list |
| `RecoveredDictionaryState` | DD recovery 输出，保存 cache rebuild 和 orphan cleanup 结果 |

`RecoveryContext` 只能由当前阶段通过受控 setter 更新阶段结果，不能被子模块直接长期保存。子模块只接收当前阶段需要的值对象和回调。

## 6. 关键数据结构与逻辑/物理区分

### 6.1 RecoveryStage

阶段定义：

| 阶段 | 输入 | 输出 | 幂等依据 |
| --- | --- | --- | --- |
| `DISCOVER_TABLESPACE` | data directory、DD space hint、space header | `RecoveredTablespaceRegistry` | space id、space version、file header checksum |
| `REPAIR_DOUBLEWRITE` | doublewrite files、data page checksum | repaired page set | page id、pageLSN、checksum |
| `REPLAY_REDO` | checkpoint LSN、redo files、PageStore | recoveredToLsn | pageLSN >= record.endLsn |
| `RECOVER_DDL` | restored DD pages、DDL log | dictionary state | DDL id、phase、object version |
| `ROLLBACK_TRX` | restored undo/trx system pages | recovered transaction set | transaction state、undo no |
| `RESUME_PURGE` | history list、oldest view boundary | purge cursor | transaction no、history list pointer |
| `OPEN_TRAFFIC` | all stage results | engine ready | gate state |

当前实现落点（2026-07-20）：公共组合根在 storage 的 doublewrite/redo/undo rollback/RESUME_PURGE 完成后，
调用 DD 层 `DictionaryDdlRecoveryService` 消费独立 CREATE/DROP TABLE DDL log。恢复以 committed DD +
`DdlUndoMarker` 的 ddl id/phase/object version/space/exact path 联合裁决 rollback 或 finish，再逐张校验
ACTIVE 表固定 page3 SDI；root=0、空页、逻辑 CRC/版本/内容错配按 committed DD 重写，未知 root 或物理页损坏
阻止 Session OPEN。无 marker 的旧 DROP_PENDING/orphan cleanup 保留兼容路径。

`mysql.ibd` 丢失不并入上述普通 crash-recovery 阶段。公共组合根先持有 instance file lock 并由 admission
fail-closed；管理员只能在实例关闭时使用 `CatalogRecoveryService`，以独立 clean manifest 证明 schema/目录
边界、full-page scrub 全部 ACTIVE 候选、显式隔离冲突并原子发布 baseline catalog。发布后仍须重新进入上述
普通 recovery 链，工具本身不开放 Session、不 replay redo，也不修复 torn page。没有 clean manifest、
存在未裁决 catalog mutation intent 或 expected 文件损坏时一律停止。

### 6.2 逻辑与物理边界

| 层面 | 对象 | 所属模块 | Recovery 模块职责 |
| --- | --- | --- | --- |
| 物理文件 | data file、redo file、doublewrite file | fil/redo/flush | 按阶段调用稳定接口，记录锁状态 |
| 物理页 | page header、pageLSN、checksum、page type | buf/fil/redo handler | 要求幂等 replay，不解释 SQL |
| 逻辑表空间 | tablespace registry、space version | fil/dd | 发现并校验，不直接修改 DD 对象 |
| 逻辑 DDL | DDL log、dictionary version、storage binding | sql.dd.recovery | 调用 DDL recovery，不重新解析 DDL SQL |
| 数据库事务 | active/prepared/committed、undo chain | trx.recovery | 调用 rollback/resume，不判断业务可见性 |
| 用户流量 | session、statement、后台 worker | server/recovery.gate | 阻断、只读诊断、恢复完成后开放 |

## 7. 核心策略和算法

### 7.1 启动恢复流程

启动流程见 [crash-recovery-startup-flow.mmd](diagrams/crash-recovery-startup-flow.mmd)。

标准顺序：

1. `RecoveryTrafficGate` 进入 `CLOSED/RECOVERY_RUNNING`，普通 SQL session 只能排队或收到启动中错误。
2. `TablespaceDiscoveryStage` 加载系统表空间、undo tablespace、file-per-table tablespace 和 redo checkpoint label。
3. 校验 redo control、space header、page size、space version 和必要文件存在性。
4. `DoublewriteRepairStage` 先修复 torn page，避免 redo 应用在破损 page image 上。
5. `RedoReplayStage` 从 checkpoint LSN 后扫描 redo block，遇到最后不完整 record 时停止。
6. `RedoApplyDispatcher` 按 record type 调用 page handler，使用 pageLSN 幂等跳过已应用 record。
7. `DdlRecoveryStage` 扫描恢复后的 DDL log，完成 committed DDL publish 或 cleanup 未完成物理对象。
8. `TransactionRollbackStage` 扫描事务系统页和 undo header，rollback recovered active transaction。
9. `PurgeResumeStage` 重建 history list cursor，启动时没有用户 ReadView，purge 可从安全低水位继续。
10. 启动 page cleaner、checkpoint worker、Buffer Pool warmup，再开放用户流量。

### 7.2 Tablespace Discovery

Discovery 输入：

- data directory 配置。
- redo checkpoint label 中的 space hint。
- Data Dictionary 或 SDI 可恢复出的 storage binding。
- 系统表空间和 undo tablespace 固定入口。

处理规则：

- 读取每个候选 tablespace 的 page 0，校验 magic、page size、space id、space version、checksum。
- 对缺失 tablespace，根据 `RecoveryMode` 决定失败、跳过 corrupt space 或进入只读诊断。
- 对 drop/truncate/discard 中的 tablespace，交给 DDL recovery 根据 DDL log 判断 cleanup。
- 发现阶段只持有 `TablespaceLifecycleLatch(S)` 和必要 file handle lock，不进入 Buffer Pool 普通路径。

### 7.3 Doublewrite Repair

Doublewrite repair 必须先于 redo replay：

1. `DoublewriteService.scanDoublewriteFiles(mode)` 读取 slot metadata。
2. 对 data file 中 checksum 不匹配、page id 不匹配或 trailer LSN 不一致的 page，判断是否 torn。
3. 如果存在 full copy 且 page id、space version、checksum、pageLSN 合法，则写回 data file。
4. detect-only 模式只能标记 suspicious page，后续依赖 redo；无法修复的 required page 进入 corruption policy。
5. 修复后不更新 redo checkpoint；redo replay 仍按 checkpoint 后全量扫描。

### 7.4 Redo Replay

Redo/Undo/DDL 串联流程见 [crash-recovery-redo-undo-flow.mmd](diagrams/crash-recovery-redo-undo-flow.mmd)。

Redo replay 规则：

- redo reader 只顺序读取 redo file，不读取 Buffer Pool LRU、flush list 或事务锁。
- 每条 redo record 校验 header、payload checksum、page id、page type。
- 对 page record，先通过 recovery-safe PageStore 或受限 Buffer Pool 读取目标页。
- 如果 `page.pageLsn >= record.endLsn`，返回 `SKIPPED_BY_PAGE_LSN`。
- 如果 `page.pageLsn < record.endLsn`，handler 应用 payload，更新 pageLSN 和 checksum。
- B+Tree、Record、FSP、Undo page handler 只做物理页恢复，不做唯一键检查、MVCC 可见性或 SQL 谓词判断。
- redo replay 完成后，Buffer Pool 中由恢复路径加载的页可以按策略保留为 clean，或释放到普通缓存体系。

### 7.5 DDL Recovery

DDL recovery 在 redo replay 后运行：

- redo 已经恢复字典表页、DDL log 页、SDI 页和用户表物理页。
- `DdlRecoveryService` 扫描 `dd_ddl_log`，按 ddl id 和 phase 分组。
- 对 `COMMITTED`：确认字典对象、storage binding、SDI、cache publish 结果。
- 对 `PREPARE/ENGINE_DONE`：根据 DDL operation policy finish 或 rollback。
- 对最终 ACTIVE snapshot：比较 SDI identity/version/payload；committed DD 是唯一逻辑真相，可覆盖 root=0、
  空页和逻辑损坏，但不能猜测未知 root 或绕过页 checksum/envelope。
- 对 orphan tablespace/root page：quarantine 后 cleanup，不能直接开放给 SQL。
- DDL recovery 不能重新执行 SQL parser，只解释持久化 DDL log 和字典事务状态。

### 7.6 Transaction Rollback 与 Prepared Transaction

事务恢复分两步：

1. 物理恢复：redo 先恢复 undo page、事务系统页、聚簇索引页和二级索引页。
2. 逻辑恢复：事务模块扫描 recovered transaction state。

处理规则：

| 事务状态 | 恢复处理 |
| --- | --- |
| `COMMITTED` | 保留已提交修改，update undo 进入 history list，等待 purge |
| `ACTIVE` / `RECOVERED_ACTIVE` | 通过 undo command 反向回滚，回滚修改自身写 redo |
| `ROLLING_BACK` | 从记录的 undo no 继续回滚 |
| `PREPARED` | 以 redo/page3/first-page 同态证据重建最小 participant；调用 `PreparedTransactionDecisionProvider`，COMMIT/ROLLBACK 在 gate 关闭期完成，UNRESOLVED fail-closed |
| `CORRUPTED` | 根据 error policy 失败或进入只读诊断 |

Recovery rollback 不进入普通用户事务死锁图。启动阶段没有并发用户事务；多个 recovered active/prepared
transaction 按 `TransactionId` 顺序处理，避免互相等待。recovered prepared 不重建已丢失的 row-lock handle，
因此 v1 必须在开放流量前取得权威决议；它不作为死锁 victim，也不能由存储层超时猜测结果。

### 7.7 Purge Resume

Purge resume 条件：

- redo replay 完成。
- DDL recovery 完成。
- active transaction rollback 完成。
- ReadViewManager 已初始化，启动时 active ReadView 集合为空。
- history list 和 rollback segment header 已从事务恢复结果重建。

规则：

- purge 从 recovered history list cursor 继续，不从头扫描全表。
- purge 仍必须遵守最老 ReadView 边界；启动后如果用户流量已开放，purge 读取新的 active view low limit。
- purge 使用内部事务和 MTR，不能长时间持有 page latch 或物理文件锁。

### 7.8 Recovery Mode

| 模式 | 行为 |
| --- | --- |
| `NORMAL` | 必须恢复所有 required tablespace 和 redo；任何必需页损坏即失败 |
| `READ_ONLY_VALIDATE` | 扫描、校验、生成诊断，不修改 data file，不开放写流量 |
| `FORCE_SKIP_CORRUPT_TABLESPACE` | 跳过指定 corrupt tablespace，标记不可用，仅用于导出其它可读对象 |

force 模式必须显式配置并记录在 `RecoveryProgressJournal`。默认启动永远是 `NORMAL`。

## 8. 与其它模块的协作

### 8.1 与 Redo

- Recovery 通过 `RedoRecoveryReader` 获取 checkpoint label、redo record stream 和 recoveredToLsn。
- Redo apply handler 由 Redo 模块注册，Recovery 只驱动 dispatcher。
- Redo replay 完成后，Recovery 把 recoveredToLsn 交给 checkpoint worker 作为启动边界。

### 8.2 与 Flush / Doublewrite

- Recovery 调用 `DoublewriteService.scanDoublewriteFiles()` 和 `recoverTornPage()`。
- Doublewrite 只修复 torn page，不替代 redo。
- page cleaner 在 redo/transaction recovery 完成前不能运行普通 flush batch。

### 8.3 与 Disk Manager

- Recovery 发现 tablespace 时通过 `TablespaceRegistryRecovery` 和 `PageStore`。
- 物理文件锁顺序沿用 Disk Manager：`TablespaceLifecycleLatch -> DataFileHandleLock -> FileSizeLock -> PageIoRangeLock -> FsyncLock`。
- drop/truncate/discard 遗留对象由 DDL recovery 决定，不由 Recovery 直接删除。

### 8.4 与 Buffer Pool

- Recovery 使用 `RecoveryBufferAccess`，只提供按 `PageId` 读写、pageLSN 更新、checksum 写回。
- Recovery 期间普通 Buffer Pool warmup 延后，避免抢占 redo replay IO。
- 如果 recovery-safe Buffer Pool 缓存了页，开放流量前必须转为普通 clean frame 或释放。

### 8.5 与 Transaction/MVCC

- Recovery 只调用 `TransactionRecoveryService.rebuild()` 和 `rollbackRecoveredActive()`。
- MVCC ReadView 在 rollback 完成后初始化。
- recovery rollback 不等待用户 row lock，也不把物理 latch 等待加入事务 Wait-For Graph。

### 8.6 与 Data Dictionary / DDL

- DD recovery 在 redo replay 后、事务 rollback 前运行，使 storage binding 和 DDL log 先稳定。
- DDL recovery 不能重新解析 SQL AST。
- cache rebuild 后，SQL Executor 和 Optimizer 才能读取 `TableDefinition`。

## 9. 并发与锁顺序

恢复期锁状态图见 [crash-recovery-lock-state.mmd](diagrams/crash-recovery-lock-state.mmd)。用户流量门控见 [crash-recovery-traffic-gate-state.mmd](diagrams/crash-recovery-traffic-gate-state.mmd)。

### 9.1 恢复期锁与等待对象

| 对象 | 保护资源 | 恢复期持有者 | 死锁域 |
| --- | --- | --- | --- |
| `RecoveryTrafficGate` | 用户 session 进入存储引擎的入口 | recovery 主线程 | 不进入事务死锁图 |
| `TablespaceLifecycleLatch` | data file 生命周期 | discovery、doublewrite、redo apply | timeout/error |
| `DataFileHandleLock` | 文件句柄 | PageStore | timeout/error |
| `PageIoRangeLock` | 指定 page 或 range IO | doublewrite repair、redo apply | timeout/error |
| `RecoveryPageLatch` | recovery-safe page body 修改 | redo apply handler | timeout/error |
| `RedoRecoveryReaderLock` | redo reader 顺序扫描 | redo replay stage | 不等待事务锁 |
| `DdlRecoveryMutex` | DDL log phase 处理 | DDL recovery stage | timeout/error |
| `TransactionRecoveryMutex` | recovered transaction set | trx recovery stage | timeout/error |
| `PurgeResumeLatch` | history list cursor 初始化 | purge resume stage | timeout/error |

### 9.2 状态与持有变化

| 状态 | 持有者 | 持有资源 | 进入条件 | 退出条件 |
| --- | --- | --- | --- | --- |
| `BOOT_BLOCKED` | recovery 主线程 | `RecoveryTrafficGate(CLOSED)` | server startup | discovery 开始 |
| `DISCOVERING_FILES` | recovery 主线程 | lifecycle S、file handle 短锁 | 读取 space header 和 redo control | registry 稳定或失败 |
| `DOUBLEWRITE_REPAIRING` | recovery + PageStore | doublewrite file lock、page IO range lock | 发现可疑 page | 修复完成、detect-only 标记或失败 |
| `REDO_REPLAYING` | redo replay worker | redo reader、recovery page latch、PageStore IO | checkpoint 后存在 redo | recoveredToLsn 完成 |
| `DDL_RECOVERING` | DD recovery | DDL recovery mutex、dictionary tx recovery context | redo 恢复 DD 页后 | dictionary state 稳定 |
| `TRX_ROLLBACKING` | trx recovery | transaction recovery mutex、undo page latch、MTR memo | recovered active 存在 | rollback 完成 |
| `PURGE_RESUMING` | purge coordinator | purge resume latch、history cursor | active rollback 完成 | purge worker 可启动 |
| `WORKERS_STARTING` | recovery 主线程 | worker registry 短锁 | 后台 worker 准备 | worker ready |
| `USER_TRAFFIC_OPEN` | server | gate open | 所有必需阶段完成 | 普通 SQL 可进入 |
| `FAILED` | recovery 主线程 | gate failed closed | 必需阶段失败 | 管理员处理或重启 |

持有变化规则：

- `gate acquire`：启动早期立即关闭普通 SQL 入口，避免恢复期间出现用户事务。
- `file discovery`：读取文件头时只持有物理文件短锁，不进入 Buffer Pool 普通 page hash。
- `doublewrite repair`：持有 doublewrite file lock 时不能请求事务行锁、MDL 或普通 Buffer Pool list lock。
- `redo apply`：按 page id 获取 recovery page latch；同一 page 的多个 redo record 串行应用，不持有 row lock。
- `DDL recovery`：可以获取 MDL recovery 专用上下文，但不能等待普通用户 session 持有的 MDL，因为 gate 尚未开放。
- `rollback active`：使用 undo command 和内部 MTR；不进入普通 LockManager wait queue。
- `open gate`：必须在 page cleaner、checkpoint worker 注册后执行，避免开放流量后没有后台刷脏能力。

### 9.3 锁顺序

恢复期标准顺序：

1. `RecoveryTrafficGate`
2. tablespace discovery / registry lock
3. physical file locks：`TablespaceLifecycleLatch -> DataFileHandleLock -> PageIoRangeLock -> FsyncLock`
4. recovery-safe Buffer Pool page access：page hash recovery view -> recovery page latch
5. redo reader / redo apply context
6. DDL recovery mutex
7. transaction recovery mutex / undo page latch / MTR memo
8. purge resume latch
9. worker registry

禁止：

- 持有物理文件锁时等待事务行锁或 MDL。
- 持有 Buffer Pool page latch 时等待 redo file fsync。
- DDL recovery 等待用户 session；启动阶段用户 session 尚未进入。
- recovery rollback 把等待加入普通事务 Wait-For Graph。

### 9.4 事务死锁检测边界

恢复阶段和运行阶段的死锁域必须分开：

- 恢复阶段没有普通用户事务，`LockManager` 的 Wait-For Graph 不接收 recovery rollback 等待。
- recovered active transaction 统一 rollback，不做互相等待的 current read。
- 如果 rollback 需要访问被另一个 recovered active transaction 修改的记录，按 undo 链和 transaction id 顺序执行，不进入等待图。
- prepared transaction 不被 recovery 自动选为 victim。
- 用户流量开放后，普通 row lock、gap lock、next-key lock、insert intention lock 仍由 Transaction 文档定义的 Wait-For Graph 和 DeadlockDetector 处理。

## 10. 异常处理

异常类型：

- `RecoveryStartupException`
- `RecoveryModeRejectedException`
- `TablespaceDiscoveryException`
- `RedoRecoveryException`
- `DoublewriteRepairException`
- `UnrecoverablePageCorruptionException`
- `DdlRecoveryException`
- `TransactionRecoveryException`
- `RecoveryRollbackException`
- `PurgeResumeException`
- `RecoveryTrafficGateException`

错误策略：

- redo control 或 checkpoint label 损坏：`NORMAL` 模式失败，`READ_ONLY_VALIDATE` 输出诊断。
- 必需 tablespace 缺失：`NORMAL` 失败；force 模式可跳过并标记 object unavailable。
- doublewrite 修复失败：如果 redo 仍可从 pageLSN 前恢复，继续；否则按 corruption policy 处理。
- redo 中间 record 损坏：失败；redo 尾部不完整 record 可停止扫描。
- DDL log 无法解释：不开放用户流量，除非 force 模式隔离相关对象。
- active transaction rollback 失败：恢复失败，不允许带未回滚 active transaction 开放写流量。
- purge resume 失败：可开放只读诊断；写流量开放需要明确策略允许。

## 11. API 设计

### 11.1 CrashRecoveryService

- `recover(RecoveryRequest request)`
- `state()`
- `metricsSnapshot()`
- `lastError()`
- `isUserTrafficAllowed()`

### 11.2 RecoveryStage

- `name()`
- `dependencies()`
- `run(RecoveryContext context)`
- `rollbackOnFailure(RecoveryContext context)`
- `progressSnapshot()`

### 11.3 RecoveryTrafficGate

- `closeForRecovery()`
- `enterReadOnlyDiagnostic()`
- `openForUserTraffic()`
- `failClosed(Throwable cause)`
- `awaitOpen(SessionContext session, Duration timeout)`

### 11.4 RecoveryLockCoordinator

- `acquireTablespaceForRecovery(SpaceId, LockMode)`
- `acquireRecoveryPage(PageId, LatchMode)`
- `recordOwnership(RecoveryLockEvent)`
- `snapshotLocks()`
- `releaseAllForStage(RecoveryStageName)`

### 11.5 RecoveryPolicy

- `onMissingTablespace(MissingTablespaceEvent)`
- `onCorruptPage(CorruptPageEvent)`
- `onRedoTailIncomplete(RedoTailEvent)`
- `onDdlLogConflict(DdlLogConflictEvent)`
- `onRollbackFailure(RollbackFailureEvent)`

## 12. 设计模式使用清单

| 模式 | 应用点 | 价值 |
| --- | --- | --- |
| Facade | `CrashRecoveryService` | 隐藏阶段注册、gate、policy、metrics |
| Chain of Responsibility | `RecoveryStageRegistry` | 固定恢复阶段顺序，并允许扩展阶段 |
| Template Method | `RecoveryStage` | 统一 run、进度、失败清理和锁释放 |
| Strategy | `RecoveryErrorPolicy`、`RecoveryMode` | 区分 normal、validate、force skip |
| State | `RecoveryTrafficGate`、`RecoveryStageState` | 明确启动、失败、只读诊断、开放状态 |
| Mediator | `RecoveryLockCoordinator` | 统一记录恢复期锁所有权和顺序 |
| Observer | `RecoveryProgressJournal`、metrics listener | 输出恢复进度和诊断 |
| Command | redo apply handler、undo rollback command、DDL recovery command | 恢复动作可幂等重放 |
| Adapter | `RecoveryBufferAccess`、`PageStoreRecoveryAdapter` | 隔离普通运行路径和恢复路径 |
| Snapshot | `RecoveryMetricsSnapshot`、`RecoveredTransactionSet` | 恢复结果可诊断且不可变发布 |

## 13. 高内聚、低耦合约束

- Recovery 只编排 crash recovery，不执行普通 SQL。
- Recovery 不读取或修改 Buffer Pool 内部 LRU、free list、flush list。
- Recovery 不解析 redo payload；redo handler 自己解释物理 payload。
- Recovery 不判断 MVCC 可见性；事务模块负责 undo rollback 和 ReadView 初始化。
- Recovery 不更新 Data Dictionary 逻辑对象；DD recovery 根据 DDL log 和字典事务更新。
- Recovery 不直接删除 orphan tablespace；必须由 DDL recovery 或 Disk Manager cleanup policy 决定。
- Recovery 期间所有普通 session 被 gate 阻断。
- 所有 recovery handler 必须幂等，并写清楚幂等依据。
- 所有等待物理锁和 page latch 的路径必须有 timeout 或 fail-fast 策略。
- 开放用户流量前必须确认 redo replay、DDL recovery、active rollback、purge resume 前置检查完成。

## 14. 典型数据流

### 14.1 普通崩溃恢复

1. server 启动后 `RecoveryTrafficGate` 关闭普通 SQL。
2. Discovery 加载 redo checkpoint label 和 tablespace registry。
3. Doublewrite 修复 torn page。
4. Redo 从 checkpoint 后扫描并应用物理页修改。
5. DDL recovery 处理未完成 DDL 和 cache rebuild。
6. Transaction recovery rollback active transaction。
7. Purge resume 恢复 history list cursor。
8. 启动 page cleaner 和 checkpoint worker。
9. gate 打开，session 可以进入。

### 14.2 Torn Page 恢复

1. Doublewrite scanner 发现 data file page checksum/trailer 不一致。
2. 查找 matching doublewrite slot。
3. full copy 合法则写回 data file。
4. redo replay 继续从 checkpoint 后应用 record。
5. pageLSN 已覆盖的 redo record 幂等跳过。

### 14.3 Active Transaction Rollback

1. redo 恢复 undo page 和事务系统页。
2. 事务恢复识别 recovered active transaction。
3. UndoLogManager 从最新 undo record 反向扫描。
4. 每条 undo command 通过内部 MTR 修改页并写 redo。
5. rollback 完成后释放恢复出的逻辑锁状态，事务标记 rolled back。

### 14.4 Atomic DDL Crash Point

1. redo 先恢复字典表页、DDL log 页和可能已经创建的 storage page。
2. DDL recovery 读取 `dd_ddl_log` phase。
3. `COMMITTED` DDL 确认 cache publish 和 SDI。
4. `PREPARE/ENGINE_DONE` 未完成 DDL 根据 policy finish 或 rollback。
5. orphan storage object quarantine 后 cleanup。

## 15. 测试设计

- 阶段顺序测试：gate close、discovery、doublewrite、redo、DDL、trx、purge、worker、gate open。
- Redo 幂等测试：同一 redo record 重放两次，第二次通过 pageLSN 跳过。
- Doublewrite 测试：torn data page 使用 full copy 修复，detect-only 模式只标记。
- Redo 损坏测试：尾部不完整 record 停止扫描，中间损坏失败。
- Tablespace discovery 测试：缺失 space、space id mismatch、page size mismatch、drop pending。
- DDL recovery 测试：prepare 后 crash、engine done 后 crash、committed cache publish 前 crash。
- Transaction recovery 测试：active rollback、rolling back continue、prepared 保留。
- Purge resume 测试：history list cursor 恢复，启动后最老 ReadView 为空。
- 并发门控测试：恢复期间普通 session 阻塞或失败，开放后可进入。
- 锁顺序测试：doublewrite repair 不持有 Buffer Pool list lock，redo apply 不等待 row lock。
- 物理文件锁测试：lifecycle S/X、PageIoRangeLock、drop/truncate drain 与 recovery repair。
- Buffer Pool recovery access 测试：recovery-safe page latch 和普通 page hash 分离。
- force mode 测试：只读校验不写 data file，skip corrupt space 标记不可用。
- 故障注入：每个阶段中断后重启，最终状态一致。
- property-based 测试：随机事务、flush、checkpoint、crash 后，索引顺序、pageLSN、history list 和字典 binding 一致。

## 16. 后续实现顺序

1. `RecoveryMode`、`RecoveryState`、`RecoveryStageName`、`RecoveryRequest` 值对象。
2. `RecoveryTrafficGate` 和 server startup 集成。
3. `RecoveryContext`、`RecoveryStage`、`RecoveryStageRegistry`。
4. `TablespaceDiscoveryStage`。
5. `DoublewriteRepairStage` 接入 flush doublewrite recovery helper。
6. `RedoReplayStage` 接入 RedoRecoveryReader 和 RedoApplyDispatcher。
7. `RecoveryBufferAccess` 和 PageStore recovery adapter。
8. `DdlRecoveryStage` 接入 `DdlRecoveryService`。
9. `TransactionRollbackStage` 接入 `TransactionRecoveryService`。
10. `PurgeResumeStage`。
11. `BackgroundWorkerResumeStage`。
12. `RecoveryLockCoordinator` 和锁快照。
13. `RecoveryProgressJournal` 和 metrics。
14. recovery force mode。
15. 故障注入和 crash point 集成测试。

## 16.1 2026-07-21 对象级 Force Recovery v1 落点

- `DatabaseEngine` 在 `StorageEngine` discovery 前运行 `DictionaryRecoveryIsolationPlanner`：管理员 SpaceId
  必须唯一映射到 committed `ACTIVE/RECOVERY_UNAVAILABLE` file-per-table 对象；系统/undo、未知、共享
  SpaceId/物理路径、非稳定状态和未决 DDL 相交全部在 DD 写入前 fail-closed。
- FORCE 将全部新目标在一个字典事务内提交为 `RECOVERY_UNAVAILABLE`。普通启动从 DD 重建长期排除集合；
  `RecoverySpaceExclusionPolicy` 再与管理员集合取并集，统一过滤 doublewrite、redo、文件 reconcile、undo 和 purge。
- recovery rollback/purge 只有在完整解码当前 undo、验证 predecessor 与目标 disposition 后，才允许跳过不可用
  对象的链首并推进持久进度；live rollback/purge 遇到同一目标仍抛异常，不能掩盖在线一致性错误。
- FORCE 打开后使用 `RECOVERY_EXPORT_READ_ONLY`：Session、storage gateway、MTR、checkpoint 及后台 worker
  共同拒绝写入；普通启动存在隔离对象时使用 `DEGRADED`，健康对象仍可服务。
- 隔离对象的 DISCARD/DROP 走 raw/offline 物理路径，不读取 page0；可信 replacement import 以本实例
  UUID/HMAC identity、完整文件 hash、定义 hash和 page0 identity 为证据，固定 op 8/9/10 DDL phase 可跨重启续作。
- 本 v1 只支持用户 file-per-table，且访问模式在单次 open 生命周期内保持稳定；不修坏页、不隔离系统/undo/
  共享空间，也不提供跨实例信任或 SQL 管理语法。

## 17. 十五轮自检记录

| 轮次 | 检查主题 | 检查结果 |
| --- | --- | --- |
| 1 | 是否只写设计文档 | 本文只新增 Markdown 和 Mermaid 设计内容，没有生成 Java 实现代码 |
| 2 | 目标与非目标 | 已明确 Recovery 是启动编排，不替代 redo、doublewrite、transaction、DDL 和 Buffer Pool |
| 3 | MySQL 8.0 贴合 | 已覆盖 tablespace discovery、doublewrite、redo replay、undo rollback、prepared transaction、purge resume |
| 4 | 高内聚 | gate、stage、context、lock、progress、policy、metric 子包职责独立 |
| 5 | 低耦合 | Recovery 只通过各模块 recovery API 协作，不访问内部链表、redo buffer 或锁队列 |
| 6 | 面向对象 | 已定义 RecoveryRequest、RecoveryContext、RecoveryStage、TrafficGate、LockCoordinator 等对象 |
| 7 | 设计模式 | 已列出 Facade、Chain of Responsibility、Template Method、Strategy、State、Mediator 等使用点 |
| 8 | 核心领域模型 | 已覆盖 stage、context、traffic gate、progress journal、error policy、recovered transaction set |
| 9 | 依赖方向 | 已明确 recovery -> redo/flush/fil/buf/trx/dd 的单向依赖和禁止反向访问 |
| 10 | 物理与逻辑区分 | 已区分物理文件/page/redo 与逻辑 DDL、事务、purge、用户流量 |
| 11 | 关键数据流 | 已给出普通崩溃恢复、torn page、active rollback、atomic DDL crash point |
| 12 | 图示 | 已提供架构图、类关系图、启动流程、redo/undo/DDL 流程、锁状态和流量门控图 |
| 13 | 并发锁状态 | 已定义恢复期状态、锁对象、持有者变化、锁顺序和事务死锁检测边界 |
| 14 | 异常与恢复 | 已列出恢复异常和 normal/validate/force 模式下的处理策略 |
| 15 | 测试与顺序 | 已给出测试设计、实现顺序，并确认没有未完成标记或空白项 |

## 18. 参考链接

- MySQL 8.0 Reference Manual - InnoDB Recovery: https://dev.mysql.com/doc/refman/8.0/en/innodb-recovery.html
- MySQL 8.0 Reference Manual - InnoDB Redo Log: https://dev.mysql.com/doc/refman/8.0/en/innodb-redo-log.html
- MySQL 8.0 Reference Manual - InnoDB Doublewrite Buffer: https://dev.mysql.com/doc/refman/8.0/en/innodb-doublewrite-buffer.html
- MySQL 8.0 Reference Manual - InnoDB Undo Logs: https://dev.mysql.com/doc/refman/8.0/en/innodb-undo-logs.html
- MySQL 8.0 Reference Manual - Atomic Data Definition Statement Support: https://dev.mysql.com/doc/refman/8.0/en/atomic-ddl.html
- MySQL 8.0 Source Documentation - InnoDB recovery: https://dev.mysql.com/doc/dev/mysql-server/8.0.46/
