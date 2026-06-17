# MiniMySQL MySQL 8.0 风格全局锁观测、等待图与死锁诊断模块设计

版本：2026-06-06  
实现语言：Java  
参考基线：MySQL 8.0.46 Performance Schema、InnoDB Locking、Metadata Locking、Deadlock Detection  
关联设计：[innodb-transaction-mvcc-design.md](innodb-transaction-mvcc-design.md)、[mysql-data-dictionary-ddl-design.md](mysql-data-dictionary-ddl-design.md)、[mysql-sql-executor-storage-api-design.md](mysql-sql-executor-storage-api-design.md)、[innodb-buffer-pool-design.md](innodb-buffer-pool-design.md)、[innodb-disk-manager-design.md](innodb-disk-manager-design.md)

## 1. 目标与边界

本设计面向 MiniMySQL 的全局锁观测、等待图与死锁诊断模块。它不授予锁、不替代事务锁或 MDL，而是把 SQL 层、Data Dictionary、InnoDB 事务锁、Buffer Pool latch、文件锁、mutex、condition 和 wait slot 的等待事实收集为一致诊断快照，并对可自动处理的逻辑锁等待执行死锁检测。

设计目标：

- 高内聚：观测事件登记、等待图、死锁检测、诊断快照、Performance Schema 风格视图和诊断报告分别收敛到独立子包。
- 低耦合：业务锁模块只发布不可变事件或轻量快照，不把内部队列、frame、page latch、MDL hash bucket 暴露给观测层。
- MySQL 8.0 风格：对齐 Performance Schema 的 `data_locks`、`data_lock_waits`、`metadata_locks`、wait event、mutex instance 观测思想，并保留 `INFORMATION_SCHEMA` 兼容视图。
- 分层清晰：明确区分逻辑锁、物理 latch、mutex、condition、wait slot 和诊断快照。
- 并发安全：定义锁状态变化、锁持有者变化、Wait-For Graph 更新、deadlock victim 选择、MDL 与 row lock 的等待边界。
- 低侵入：默认只登记锁模块已有事实，避免诊断路径长期持有业务锁；高频物理等待可采样或聚合。
- 可测试：每个事件、图边、快照和死锁报告都可通过确定性测试、并发测试和故障注入验证。

非目标：

- 不实现 SQL parser、optimizer、事务回滚、MDL 授权或 Buffer Pool latch 本身。
- 不让观测层直接扫描 BufferFrame、RecordCursor、MDL bucket 或裸文件锁内部结构。
- 不对物理 latch、mutex、condition 自动选择事务 victim。
- 不保证诊断快照等价于全局暂停后的强一致镜像；默认提供带 epoch 的一致性边界。
- 不生成 Java 源码。本文件只定义设计、关系、数据流和接口边界。

术语边界：

- 逻辑锁：会表达用户事务或 metadata 语义的锁，包括 InnoDB row/table intention/gap/next-key/insert intention lock 和 SQL MDL。
- 物理 latch：保护内存或文件物理结构的短锁，包括 page latch、B+Tree latch、Buffer Pool list/hash latch、file lifecycle/size/page IO/fsync latch。
- mutex：保护 Java 对象短临界区的互斥对象，对齐 Performance Schema `mutex_instances` 风格。
- condition：线程等待条件满足的同步点，例如 page load future、flush completion、checkpoint advance。
- wait slot：线程或 session 当前等待位置，承载 thread id、event id、等待对象、开始时间、超时和 kill 标记。
- 诊断快照：按 snapshot epoch 采集的一组 lock rows、wait edges、wait events 和报告，不拥有业务锁。

## 2. MySQL 8.0 参考依据

本设计参考 MySQL 8.0 的以下行为：

- Performance Schema `data_locks` 暴露存储引擎数据锁，包含 engine、engine lock id、engine transaction id、thread/event、object、index、lock type、lock mode、lock status 和 lock data。
- Performance Schema `data_lock_waits` 暴露请求锁与阻塞锁的关系，可通过 engine lock id 连接到 `data_locks`。
- Performance Schema `metadata_locks` 暴露 metadata lock，状态包括 `PENDING`、`GRANTED`、`VICTIM`、`TIMEOUT`、`KILLED`、`PRE_ACQUIRE_NOTIFY`、`POST_RELEASE_NOTIFY`，类型包括 shared、shared read/write/upgradable、exclusive 等。
- Performance Schema wait event 表以 thread id 和 event id 标识当前等待，记录 event name、source、timer、object instance 等信息。
- Performance Schema mutex instance 表记录 mutex instrument name、object instance 和当前 owning thread，可与 wait event 一起诊断同步瓶颈。
- InnoDB 死锁检测基于 lock table waits-for graph；高并发下可禁用死锁检测并依赖 lock wait timeout。
- InnoDB 检测等待图时存在搜索深度和锁数量边界，过深或过长的等待链会按死锁处理，避免检测本身失控。
- MySQL 8.0 中旧 `INFORMATION_SCHEMA.INNODB_LOCKS/INNODB_LOCK_WAITS` 已被 Performance Schema lock tables 替代；MiniMySQL 可提供只读兼容视图方便教学和迁移理解。

简化点：

| MySQL 8.0 能力 | MiniMySQL 第一阶段 |
| --- | --- |
| 全量 Performance Schema 消费者、过滤器和历史表 | 先实现当前快照、最近死锁报告和有限 ring buffer |
| 精确内部地址 `OBJECT_INSTANCE_BEGIN` | 使用稳定诊断对象 ID，不暴露 JVM 地址 |
| InnoDB 内部全部 lock mode 细节 | 覆盖 S、X、IS、IX、gap、next-key、insert intention、MDL 核心类型 |
| 完整 PFS instrumentation 开关 | 使用模块级 `InstrumentationPolicy` 控制采样、脱敏和快照大小 |
| 物理同步对象死锁自动恢复 | 只诊断物理 latch/mutex/condition 等待，不自动回滚事务 |
| 跨 MDL 与 row lock 自动 victim | 第一阶段只自动处理各自逻辑域，跨域环生成诊断报告并依赖 timeout 或 kill |

## 3. 总体架构

架构图见 [lock-observability-architecture.mmd](diagrams/lock-observability-architecture.mmd)。

模块位于 SQL/server 诊断层，接收来自 SQL session、MDL、事务锁、Buffer Pool、Disk Manager 和执行器的事件：

1. `LockEventRegistry` 是统一事件入口，登记 acquire、wait、grant、release、timeout、victim、kill、condition wait 等事件。
2. `WaitSlotRegistry` 维护每个 thread/session 的当前等待，生成 thread id 与 event id。
3. `WaitForGraph` 只保存可进入检测域的逻辑等待边，包括 row lock graph 和 MDL graph。
4. `DeadlockDetector` 在新增等待边时即时检测，并由后台任务做周期性兜底。
5. `LockSnapshotService` 按 epoch 从各 registry 获取只读快照，构造 Performance Schema 风格行。
6. `PerformanceSchemaLockAdapter` 暴露 `data_locks`、`data_lock_waits`、`metadata_locks`、`events_waits_current`、`mutex_instances` 等只读表。
7. `InformationSchemaLockAdapter` 提供 `innodb_trx`、`innodb_lock_waits` 等兼容视图。
8. `DeadlockReportRepository` 保存最近死锁报告、victim 选择理由和等待链。

依赖方向：

`sql.session -> server.lockobs.api`  
`sql.dd.mdl -> server.lockobs.api`  
`storage.trx.lock -> server.lockobs.api`  
`storage.buf/storage.fil -> server.lockobs.api`  
`server.lockobs.views -> server.lockobs.snapshot -> registry/graph/report`

禁止方向：

- `server.lockobs` 不能调用 `LockManager.acquire()` 或 `MetadataLockManager.acquire()` 参与授锁。
- `server.lockobs` 不能持有 Buffer Pool page latch、frame mutex、file lock 或 MDL bucket lock 后等待其它模块。
- `storage.trx.lock` 不能依赖 Performance Schema adapter，只能依赖观测 API。
- `storage.buf` 和 `storage.fil` 只能发布物理等待事件，不允许把物理等待加入事务 deadlock graph。
- `LockSnapshotService` 不能返回业务模块内部可变对象。

## 4. 包与职责

| 包 | 职责 | 主要依赖 | 设计模式 |
| --- | --- | --- | --- |
| `server.lockobs.api` | 观测门面、事件发布接口、快照查询接口 | domain | Facade |
| `server.lockobs.domain` | lock id、resource key、actor、event、状态和值对象 | 无 | Value Object |
| `server.lockobs.registry` | lock event、wait slot、mutex、condition、latch registry | domain | Repository, Observer |
| `server.lockobs.graph` | Wait-For Graph、边更新、图快照、环检测输入 | domain | Graph Repository |
| `server.lockobs.deadlock` | 即时检测、后台检测、victim 选择、报告生成 | graph, registry | Strategy, Chain of Responsibility |
| `server.lockobs.snapshot` | 诊断快照、epoch、一致性裁剪、脱敏 | registry, graph | Snapshot, Builder |
| `server.lockobs.views` | Performance Schema 与 Information Schema 只读视图 | snapshot | Adapter |
| `server.lockobs.policy` | instrumentation、采样、大小限制、权限脱敏 | domain | Strategy, Policy |
| `server.lockobs.report` | 最近死锁、长等待、锁等待摘要 | deadlock | Repository |
| `server.lockobs.metric` | wait count、timeout、deadlock、snapshot cost | registry | Observer |

推荐依赖方向：

`api -> registry + snapshot`  
`registry -> graph`  
`deadlock -> graph + report`  
`views -> snapshot`  
`policy -> domain`

禁止方向：

- `views` 不允许反向访问业务锁模块。
- `deadlock` 不允许访问 Buffer Pool、Disk Manager 或文件 IO。
- `graph` 不存储 `Transaction`、`Session`、`BufferFrame` 的对象引用，只保存不可变 actor id。
- `registry` 不解释 record 格式、page bytes 或 DD 对象内容。

## 5. 核心领域模型

类关系图见 [lock-observability-class-relation.mmd](diagrams/lock-observability-class-relation.mmd)。

### 5.1 标识和值对象

| 对象 | 含义 |
| --- | --- |
| `DiagnosticLockId` | 观测层稳定锁 ID，格式由模块生成，外部不得解析 |
| `DiagnosticResourceKey` | 锁资源键，包含 domain、object、index、page、record、gap、metadata object 等维度 |
| `WaitActorId` | 等待参与者，类型为 transaction、session、thread、system task |
| `ThreadEventId` | `threadId + eventId`，标识一次 wait event |
| `SnapshotEpoch` | 诊断快照版本，避免跨 registry 行混淆 |
| `LockOwnerToken` | 锁持有者令牌，只包含 owner id、duration、source，不包含业务对象引用 |
| `WaitReason` | row lock、MDL、page latch、file IO、mutex、condition、kill wait、timeout wait |
| `DiagnosticSource` | 模块名、类名、逻辑行号或操作名，用于定位埋点来源 |

这些对象必须不可变。锁模块可以重复生成同一资源的快照行，但 `DiagnosticLockId` 在单次锁请求生命周期内稳定。

### 5.2 锁类别

| 类别 | 示例 | 进入 Wait-For Graph | 自动 victim | 观测表 |
| --- | --- | --- | --- | --- |
| `ROW_TRANSACTION_LOCK` | record、gap、next-key、insert intention、table intention | 是，事务域 | 是 | `data_locks`, `data_lock_waits` |
| `METADATA_LOCK` | schema/table/tablespace MDL | 是，MDL 域 | 是，按 MDL 规则 | `metadata_locks` |
| `PAGE_LATCH` | B+Tree page S/X latch、buffer frame latch | 否 | 否 | `events_waits_current`, `rwlock_instances` |
| `FILE_LATCH` | file lifecycle、size、page IO range、fsync | 否 | 否 | `events_waits_current`, `rwlock_instances` |
| `MUTEX` | lock shard mutex、LRU list mutex、flush list mutex | 否 | 否 | `mutex_instances`, wait events |
| `CONDITION` | page load future、flush completion、checkpoint wait | 否 | 否 | `events_waits_current` |
| `WAIT_SLOT` | session 当前等待槽 | 否，本身不是锁 | 否 | `events_waits_current` |

设计边界：

- 逻辑锁有业务所有权和可回滚语义，因此可进入对应等待图。
- 物理 latch 和 mutex 只保护内存或文件短临界区，不能触发事务回滚。
- condition 通常没有稳定 blocker，最多记录 signal source、future id 和等待原因。
- wait slot 是诊断索引，用于把 thread、statement、transaction、lock event 串起来。

### 5.3 LockEvent

`LockEvent` 是观测层事件事实：

- `eventId`
- `threadId`
- `sessionId`
- `transactionId`
- `lockId`
- `resourceKey`
- `lockCategory`
- `lockMode`
- `lockStatus`
- `ownerToken`
- `waitStartTime`
- `waitDeadline`
- `source`
- `snapshotEligible`

事件类型：

- `REQUESTED`
- `GRANTED`
- `WAITING`
- `CONVERTING`
- `TIMEOUT`
- `VICTIM`
- `KILLED`
- `RELEASED`
- `CLEANUP_RELEASED`
- `ROLLBACK_RELEASED`

事件规则：

- 业务模块在自己的锁状态已经改变后发布事件；观测层不作为真相来源。
- `WAITING` 事件必须带 blocker 列表或可延迟查询的 blocker supplier。
- `RELEASED` 事件必须能删除对应 graph edge 和 wait slot。
- 对高频物理 latch，允许只发布 wait start/wait end，不保留每个 granted row。

### 5.4 WaitSlot

`WaitSlot` 表示一个 thread/session 当前等待：

- `threadId`
- `eventId`
- `sessionId`
- `transactionId`
- `statementId`
- `waitState`
- `waitCategory`
- `objectInstanceId`
- `eventName`
- `timerStart`
- `timerEnd`
- `timerWait`
- `killRequested`
- `deadline`

状态：

- `IDLE`
- `WAITING`
- `COMPLETED`
- `TIMEOUT`
- `KILLED`
- `DEADLOCK_VICTIM`
- `CLEANED`

所有权变化：

1. thread 准备阻塞前创建 wait slot。
2. wait slot 绑定当前 statement 和事务。
3. 等待成功时填充 timer end 并进入 `COMPLETED`。
4. timeout、kill 或 victim 时写入结束状态。
5. statement cleanup 或 thread reuse 时进入 `CLEANED`。

### 5.5 WaitForGraph

`WaitForGraph` 只保存逻辑等待边：

- vertex：`WaitActorId`
- edge：`waitingActor -> blockingActor`
- edge attributes：`lockId`、`resourceKey`、`lockMode`、`domain`、`waitStartTime`、`requestEventId`

图域：

| 图域 | 顶点 | 边来源 | 检测结果 |
| --- | --- | --- | --- |
| `ROW_LOCK_GRAPH` | `TransactionId` | InnoDB row/table transaction lock wait | 选择事务 victim 并触发 rollback |
| `MDL_GRAPH` | `SessionId` | Metadata lock wait | 标记 MDL request victim，通常终止 statement |
| `CROSS_DOMAIN_DIAGNOSTIC_GRAPH` | transaction/session/thread 混合 actor | 由快照构造 | 只报告，不自动回滚 |

更新规则：

- 新增 row lock 等待时，事务锁模块提供 blocking transactions，观测层写入 row graph。
- 新增 MDL 等待时，MDL 模块提供 blocking sessions，观测层写入 MDL graph。
- grant、timeout、kill、release、rollback cleanup 时必须删除等待者出边。
- blocker release 后，如果等待者被授予锁，先删除边，再发布 `GRANTED`。

## 6. 关键数据结构与观测表格式

### 6.1 `performance_schema.data_locks`

第一阶段字段：

| 字段 | 含义 |
| --- | --- |
| `ENGINE` | 固定为 `INNODB` 或内部引擎名 |
| `ENGINE_LOCK_ID` | `DiagnosticLockId` |
| `ENGINE_TRANSACTION_ID` | 事务 ID；只读事务未分配时为空 |
| `THREAD_ID` | 请求或持有锁的 thread id |
| `EVENT_ID` | 对应 wait 或 grant event id |
| `OBJECT_SCHEMA` | schema 名 |
| `OBJECT_NAME` | table 名 |
| `INDEX_NAME` | index 名，表锁为空 |
| `OBJECT_INSTANCE_ID` | 观测对象 ID |
| `LOCK_TYPE` | `TABLE`、`RECORD`、`GAP`、`NEXT_KEY`、`INSERT_INTENTION` |
| `LOCK_MODE` | `S`、`X`、`IS`、`IX`、`S,GAP`、`X,GAP`、`X,REC_NOT_GAP` 等 |
| `LOCK_STATUS` | `GRANTED`、`WAITING`、`CONVERTING`、`TIMEOUT`、`VICTIM`、`KILLED` |
| `LOCK_DATA` | 主键、heap no、supremum/infimum、gap range 摘要；可按权限脱敏 |

约束：

- `LOCK_DATA` 用于诊断，不作为应用协议。
- `ENGINE_LOCK_ID` 格式内部可变，测试只能断言稳定性和唯一性，不断言字符串结构。
- 已 release 的锁不出现在当前视图，可在历史 ring buffer 中查看。

### 6.2 `performance_schema.data_lock_waits`

字段：

| 字段 | 含义 |
| --- | --- |
| `ENGINE` | 请求锁的引擎 |
| `REQUESTING_ENGINE_LOCK_ID` | 等待中的 lock id |
| `REQUESTING_ENGINE_TRANSACTION_ID` | 等待事务 |
| `REQUESTING_THREAD_ID` | 等待 thread |
| `REQUESTING_EVENT_ID` | 等待 event |
| `REQUESTING_OBJECT_INSTANCE_ID` | 等待锁对象 ID |
| `BLOCKING_ENGINE_LOCK_ID` | 阻塞锁 id |
| `BLOCKING_ENGINE_TRANSACTION_ID` | 阻塞事务 |
| `BLOCKING_THREAD_ID` | 阻塞 thread |
| `BLOCKING_EVENT_ID` | 阻塞 event |
| `BLOCKING_OBJECT_INSTANCE_ID` | 阻塞锁对象 ID |

生成规则：

- 一条等待可能被多个 granted lock 阻塞，因此可产生多行。
- 如果 blocker 已在采集期间 release，快照保留 requesting row，并把 blocking row 标记为裁剪缺失。
- `data_lock_waits` 来源是 `ROW_LOCK_GRAPH` 当前边，不扫描事务内部 held lock list。

### 6.3 `performance_schema.metadata_locks`

字段：

| 字段 | 含义 |
| --- | --- |
| `OBJECT_TYPE` | `GLOBAL`、`SCHEMA`、`TABLE`、`TABLESPACE`、`BACKUP_LOCK` |
| `OBJECT_SCHEMA` | schema 名 |
| `OBJECT_NAME` | 对象名 |
| `OBJECT_INSTANCE_ID` | 诊断对象 ID |
| `LOCK_TYPE` | `INTENTION_EXCLUSIVE`、`SHARED_READ`、`SHARED_WRITE`、`SHARED_UPGRADABLE`、`EXCLUSIVE` 等 |
| `LOCK_DURATION` | `STATEMENT`、`TRANSACTION`、`EXPLICIT` |
| `LOCK_STATUS` | `PENDING`、`GRANTED`、`VICTIM`、`TIMEOUT`、`KILLED`、`PRE_ACQUIRE_NOTIFY`、`POST_RELEASE_NOTIFY` |
| `SOURCE` | 埋点来源 |
| `OWNER_THREAD_ID` | 拥有或请求线程 |
| `OWNER_EVENT_ID` | 对应事件 |

边界：

- MDL 使用 `PENDING` 表达等待；row lock 使用 `WAITING` 表达等待。
- DDL 获取 `EXCLUSIVE` MDL 前必须按 Data Dictionary 文档释放不应跨等待持有的物理 latch。
- MDL victim 通常终止当前 statement；是否回滚整个事务由 SQL transaction policy 决定。

### 6.4 wait、mutex、rwlock 与 condition 视图

第一阶段提供以下只读诊断视图：

| 视图 | 来源 | 主要字段 |
| --- | --- | --- |
| `events_waits_current` | `WaitSlotRegistry` | thread id、event id、event name、source、timer、object instance、operation |
| `mutex_instances` | `MutexInstrumentationRegistry` | name、object instance id、locked by thread id |
| `rwlock_instances` | `LatchInstrumentationRegistry` | name、object instance id、read owners count、write owner thread id |
| `condition_waits_current` | `ConditionWaitRegistry` | condition name、waiter thread、future id、signal source、deadline |
| `lock_diagnostics_current` | `LockSnapshotService` | snapshot epoch、graph domain、long wait、deadlock suspect、truncated flag |

物理同步对象视图用于定位瓶颈和潜在编码错误，不进入事务死锁 victim 流程。

### 6.5 `information_schema` 兼容视图

为了教学和迁移理解，第一阶段提供只读兼容视图：

| 视图 | 来源 | 说明 |
| --- | --- | --- |
| `INNODB_TRX` | transaction snapshot | 活跃事务、状态、开始时间、当前 wait slot |
| `INNODB_LOCK_WAITS` | `data_lock_waits` adapter | 映射 requesting/blocking transaction 与 lock id |
| `INNODB_LOCKS` | `data_locks` adapter | 只显示当前 granted/waiting 事务数据锁 |
| `METADATA_LOCK_INFO` | `metadata_locks` adapter | MiniMySQL 扩展视图，不模拟旧插件行为 |

这些视图不允许写入，不参与 optimizer 代价估计。

## 7. 核心策略和算法

### 7.1 事件登记策略

事件登记流程见 [lock-observability-wait-graph-flow.mmd](diagrams/lock-observability-wait-graph-flow.mmd)。

逻辑锁等待：

1. 调用方构造 lock request，并在业务锁模块内完成兼容性判断。
2. 如果可立即授予，业务锁模块更新自身状态，再发布 `GRANTED` 事件。
3. 如果需要等待，调用方在阻塞前释放 page latch、file latch、MTR latch、record cursor、frame mutex 等物理短资源。
4. 业务锁模块把 request 放入 wait queue，并提供 blocker actor 列表。
5. `LockEventRegistry` 创建 wait slot，发布 `WAITING` 或 `PENDING`。
6. `WaitForGraph` 写入等待边。
7. `DeadlockDetector` 对新增边执行即时检测。
8. 等待结束后发布 grant、timeout、victim、kill 或 cleanup release。

物理等待：

1. 模块尝试获取 latch/mutex/condition。
2. 超过自旋阈值或准备 park 时发布 wait start。
3. `WaitSlotRegistry` 记录当前 wait event。
4. 获取成功、timeout 或取消时发布 wait end。
5. 观测层只更新 wait event 和实例 owner，不写逻辑 Wait-For Graph。

### 7.2 Wait-For Graph 检测

死锁检测流程见 [lock-observability-deadlock-detection-flow.mmd](diagrams/lock-observability-deadlock-detection-flow.mmd)。

即时检测：

- 新增边 `A -> B` 后，从 `B` 开始沿出边查找是否能回到 `A`。
- 每个图域独立检测，row lock graph 不访问 MDL graph。
- 检测持有 graph shard 短锁，不访问事务 undo、Buffer Pool 或文件 IO。
- 搜索设定最大顶点数、最大边数和最大耗时；超过边界时按保守策略处理当前 checker。

后台检测：

- 周期性扫描长等待边。
- 对每个 graph shard 复制轻量邻接表后释放 shard 锁。
- 使用 Tarjan 或 bounded DFS 查找环。
- 对重复报告去重，只在 wait chain 变化或 victim 变化时写新报告。

边删除：

- grant、timeout、kill、victim、release、rollback cleanup 都必须删除等待者出边。
- session 断开时删除该 session、thread、transaction 关联的所有边。
- recovery cleanup 不依赖业务线程，直接按 owner token 清理。

### 7.3 victim 选择

row lock victim 成本：

| 因素 | 影响 |
| --- | --- |
| undo record 数 | 越少越适合 rollback |
| 修改行数 | 越少成本越低 |
| 持有锁数量 | 越少释放影响越小 |
| 事务年龄 | 越短优先级越低，越适合作为 victim |
| autocommit 单语句 | 通常比显式长事务更适合作为 victim |
| user priority | 管理员或系统任务可提高保留优先级 |
| prepared/committing/recovery | 默认不可作为普通 victim |

MDL victim 成本：

| 因素 | 影响 |
| --- | --- |
| statement 是否可重试 | 可重试 DDL/DML 更适合终止 |
| lock duration | statement duration 优先于 transaction/explicit |
| DDL 阶段 | 已进入 atomic commit 阶段不作为普通 victim |
| pending 时间 | 防止新请求总是击败老请求 |
| session killable | 不可 kill 系统 session 不作为 victim |

处理流程：

1. detector 构造 cycle report。
2. `VictimSelectionPolicy` 计算候选成本。
3. 选中 victim 后写入 `DeadlockReportRepository`。
4. row lock victim 设置事务 `rollbackOnly`，移除等待请求，唤醒线程抛出 deadlock 异常。
5. MDL victim 标记 request `VICTIM`，终止当前 statement，并按 SQL transaction policy 回滚语句或事务。
6. victim cleanup 发布 `ROLLBACK_RELEASED` 或 `CLEANUP_RELEASED` 事件，删除 graph edge。

### 7.4 MDL 与 row lock 等待边界

MDL 与 row lock 都是逻辑锁，但检测域不同：

- DML 进入执行前通常持有 `SHARED_READ` 或 `SHARED_WRITE` MDL，再进入 InnoDB row lock。
- DDL 需要 `EXCLUSIVE` MDL，等待期间不能持有 Buffer Pool page latch、file latch 或 row lock shard mutex。
- Row lock detector 只处理事务等待事务，不选择仅等待 MDL 的 session。
- MDL detector 只处理 metadata wait，不扫描 row lock table。
- 跨域诊断快照可以构造 `session holds MDL -> transaction waits row lock -> session waits MDL` 的混合链，但第一阶段不自动选择跨域 victim。
- 混合链的自动恢复策略是 lock wait timeout、statement timeout、管理员 kill query 或 kill session。

跨域报告必须写明：

- 哪个 session 持有 MDL。
- 哪个 transaction 在等待 row lock。
- 哪个 DDL 或 DML 在等待 MDL。
- 哪些等待可由死锁 detector 自动处理，哪些只能 timeout 或 kill。

### 7.5 低侵入采样策略

`InstrumentationPolicy` 控制采集成本：

| 策略项 | 默认 |
| --- | --- |
| row lock granted/waiting | 全量，因为来自事务锁表已有事实 |
| MDL granted/pending | 全量，因为来自 MDL subsystem 已有事实 |
| mutex wait | 超过自旋阈值后记录 |
| latch wait | 超过短等待阈值后记录 |
| condition wait | 全量记录当前等待，历史 ring buffer 限长 |
| lock data | 默认保留主键摘要，敏感字段按权限脱敏 |
| snapshot row limit | 超限裁剪并标记 `truncated` |
| deadlock reports | 保留最近 N 条和每域计数 |

观测事件写入必须是非阻塞或有界阻塞：

- registry 分片锁只保护本地 map，不跨模块调用。
- ring buffer 满时按策略丢弃低优先级物理等待历史，不丢弃当前 row lock/MDL 等待。
- 快照请求不能迫使前台线程同步格式化大字符串；`LOCK_DATA` 可延迟格式化。

## 8. 状态机设计

状态图见 [lock-observability-state-machine.mmd](diagrams/lock-observability-state-machine.mmd)。

### 8.1 逻辑锁请求状态

| 状态 | 含义 |
| --- | --- |
| `FREE` | 无请求或已清理 |
| `REQUESTED` | 调用方构造请求 |
| `GRANTED` | 锁已授予，有 owner |
| `WAITING` | row lock 等待 |
| `PENDING` | MDL 等待 |
| `CONVERTING` | 已持有锁正在升级 |
| `TIMEOUT` | 等待到期 |
| `VICTIM` | 被 deadlock detector 选中 |
| `KILLED` | 被 kill query/session 取消 |
| `RELEASED` | 正常释放 |
| `CLEANUP_RELEASED` | timeout/kill/session cleanup 释放 |
| `ROLLBACK_RELEASED` | victim rollback 释放 |

锁持有者变化：

1. `REQUESTED -> GRANTED`：owner 从无到 transaction/session。
2. `GRANTED -> CONVERTING`：owner 保留原模式，同时登记 upgrade request。
3. `CONVERTING -> GRANTED`：owner 模式替换为新模式。
4. `WAITING/PENDING -> GRANTED`：request 从 wait queue 移到 owner set。
5. `GRANTED -> RELEASED`：owner token 删除。
6. `VICTIM -> ROLLBACK_RELEASED`：事务回滚释放 owned locks。
7. `TIMEOUT/KILLED -> CLEANUP_RELEASED`：pending request 删除，不获得 owner。

### 8.2 WaitSlot 状态

| 状态 | 进入条件 | 退出条件 |
| --- | --- | --- |
| `IDLE` | thread 无等待 | 创建新 wait event |
| `WAITING` | 线程准备 park 或阻塞 | grant、signal、timeout、kill、victim |
| `COMPLETED` | 等待成功结束 | statement cleanup |
| `TIMEOUT` | deadline 到达 | cleanup |
| `KILLED` | kill flag 生效 | cleanup |
| `DEADLOCK_VICTIM` | detector 选中 | rollback cleanup |
| `CLEANED` | wait slot 可复用 | 下一次等待 |

规则：

- 一个 thread 同时只能有一个 current wait slot。
- 嵌套物理等待只允许记录最外层阻塞等待，内部短自旋计数归入 metric。
- wait slot cleanup 必须在 thread 复用前完成。

## 9. 与其它模块的协作

### 9.1 SQL Executor

- Executor 在 statement 开始时注册 `StatementDiagnosticContext`。
- 等待 MDL、row lock、condition 或 kill 时，wait slot 绑定当前 statement id。
- `kill query` 设置当前 statement wait slot 的 kill flag，并由等待模块转成 `KILLED`。
- Executor 通过 snapshot view 返回诊断查询结果，不直接读 registry。

### 9.2 Data Dictionary 与 MDL

- `MetadataLockManager` 是 MDL 真相来源。
- MDL request 入队后发布 `PENDING`，授予后发布 `GRANTED`。
- MDL release duration 由 DD/SQL 层决定，观测层只记录。
- MDL deadlock detector 可以复用 `WaitForGraph`，但 victim callback 回到 MDL manager 执行。

### 9.3 Transaction 与 Row Lock

- `LockManager` 是 row/table transaction lock 真相来源。
- row lock wait 前必须释放 page latch、record cursor、buffer fix、undo page latch。
- row lock grant 后调用方必须重新定位记录。
- deadlock victim callback 回到事务模块，设置 `rollbackOnly` 并唤醒等待线程。

### 9.4 Buffer Pool 与 B+Tree latch

- Buffer Pool 可登记 page latch、frame mutex、page load future。
- B+Tree 可登记 page split/merge latch 等待。
- 这些等待只进入 wait event 和 latch instance 视图。
- 若物理等待时间过长，诊断报告给出 owner thread 和 source，但不回滚事务。

### 9.5 Disk Manager 与文件锁

- Disk Manager 可登记 file lifecycle lock、file size lock、page IO range lock、fsync lock。
- drop/truncate/discard 与普通 IO 的互斥只作为文件锁事件暴露。
- 文件锁等待不进入事务 deadlock graph，只允许 timeout、IO cancel 或 session kill。

### 9.6 Recovery、Purge 和后台任务

- recovery 线程、purge 线程和 flush 线程使用 `SYSTEM_TASK` actor。
- 系统任务等待可出现在 wait events，但默认不作为 row lock victim。
- recovery 阶段 Performance Schema view 可只读暴露恢复等待，普通 SQL session 尚未开放时不会有用户查询。

## 10. 并发与锁顺序

### 10.1 观测层内部锁顺序

观测层内部短锁顺序：

1. `WaitSlotRegistry` shard。
2. `LockEventRegistry` shard。
3. `WaitForGraph` shard。
4. `DeadlockReportRepository` append lock。
5. `Metrics` striped counter。

规则：

- 不允许在持有 registry 或 graph shard 时调用业务模块。
- 不允许在持有业务锁模块内部锁时反向查询 snapshot view。
- graph 检测只能读取 graph 内部不可变 actor id 和边属性。
- report 生成中需要的事务成本必须来自业务模块在事件中携带的 `VictimCostHint`，不能同步查询 undo 或 Buffer Pool。

### 10.2 跨模块标准锁顺序

推荐顺序：

1. SQL session state。
2. MDL。
3. Data Dictionary pin。
4. Transaction object。
5. Row lock request。
6. B+Tree page latch。
7. Buffer Pool frame latch。
8. File page IO range lock。
9. Redo/MTR short latch。

等待前释放规则：

- 等待 MDL 前，不持有 row lock shard mutex、page latch、file latch、MTR latch。
- 等待 row lock 前，释放 page latch、buffer fix、record cursor、undo page latch。
- 等待 page IO 或 page load future 前，释放 Buffer Pool page hash/list 短锁。
- 等待 fsync 前，不持有 file size lock 或 Buffer Pool flush list mutex。
- 等待 condition 前，只保留必要 wait slot，不保留会阻止 signal 方前进的 mutex。

### 10.3 死锁检测边界

进入死锁检测：

- row/table transaction logical lock。
- MDL logical lock。

只允许 timeout、kill 或报警：

- page latch。
- Buffer Pool hash/list/free/LRU/flush mutex。
- file lifecycle/size/page IO/fsync latch。
- redo writer mutex、checkpoint condition。
- Java monitor 或 executor 内部 condition。

混合链处理：

- `CROSS_DOMAIN_DIAGNOSTIC_GRAPH` 可以把逻辑锁和物理等待放在一张报告中。
- 报告不会直接调用 rollback。
- 如果混合链中同时存在 row lock 子环或 MDL 子环，对应 detector 可以独立处理其子环。

## 11. 异常处理

异常类型：

- `DeadlockDetectedException`：row lock victim 或 MDL victim 的业务异常映射。
- `LockWaitTimeoutException`：逻辑锁等待超过 deadline。
- `MetadataLockTimeoutException`：MDL 等待超时。
- `QueryKilledException`：kill query/session 取消等待。
- `DiagnosticSnapshotTooLargeException`：诊断查询要求强一致且超过大小限制。
- `InstrumentationBackpressureException`：非关键历史事件被丢弃时写入 metric，不影响业务路径。
- `LockObservationCorruptionException`：registry 不变量破坏，进入只读降级模式并报警。

错误策略：

- row lock deadlock victim 必须进入事务 rollback 或 statement rollback 策略。
- MDL victim 必须释放 pending request 和 wait slot。
- timeout 不自动选择 blocker，只清理当前等待请求。
- kill query 只取消当前 statement 等待；kill session 释放 session 级资源。
- snapshot 采集期间遇到模块不可用，返回部分快照并标记 `partial`，除非调用方要求严格模式。
- 观测失败不能阻断锁授予或释放；只有 graph 不变量破坏时禁用自动 deadlock detector 并依赖 timeout。

## 12. API 设计

### 12.1 LockObservationService

- `publish(LockEvent)`
- `openWaitSlot(WaitSlotRequest)`
- `completeWaitSlot(ThreadEventId, WaitCompletion)`
- `captureSnapshot(SnapshotRequest)`
- `latestDeadlocks(DeadlockReportFilter)`
- `setInstrumentationPolicy(InstrumentationPolicy)`

### 12.2 LockEventRegistry

- `onRequested(LockEvent)`
- `onGranted(LockEvent)`
- `onWaiting(LockEvent, BlockingSet)`
- `onConverting(LockEvent, BlockingSet)`
- `onTimeout(LockEvent)`
- `onVictim(LockEvent, DeadlockReportId)`
- `onKilled(LockEvent)`
- `onReleased(LockEvent, ReleaseReason)`
- `snapshotLocks(SnapshotEpoch, SnapshotFilter)`

### 12.3 WaitForGraph

- `addEdge(GraphDomain, WaitEdge)`
- `removeWaitingActor(GraphDomain, WaitActorId)`
- `removeEdgesByLock(DiagnosticLockId)`
- `copyShard(GraphDomain, GraphShardId)`
- `findCycleFrom(GraphDomain, WaitActorId, DetectionBudget)`
- `snapshotEdges(GraphDomain, SnapshotEpoch)`

### 12.4 DeadlockDetector

- `onEdgeAdded(GraphDomain, WaitEdge)`
- `scan(GraphDomain, DetectionBudget)`
- `selectVictim(DeadlockCycle, VictimSelectionContext)`
- `markVictim(DeadlockVictim)`
- `buildReport(DeadlockCycle, DeadlockVictim)`

### 12.5 LockSnapshotService

- `captureCurrent(SnapshotRequest)`
- `dataLocks(LockDiagnosticSnapshot)`
- `dataLockWaits(LockDiagnosticSnapshot)`
- `metadataLocks(LockDiagnosticSnapshot)`
- `eventsWaitsCurrent(LockDiagnosticSnapshot)`
- `mutexInstances(LockDiagnosticSnapshot)`
- `rwlockInstances(LockDiagnosticSnapshot)`
- `informationSchemaViews(LockDiagnosticSnapshot)`

API 约束：

- 所有返回对象都是只读快照。
- 查询视图不允许调用业务锁 manager。
- snapshot request 可指定 `includePhysicalWaits`、`includeLockData`、`maxRows`、`strictConsistency`、`redactionPolicy`。

## 13. 设计模式使用清单

| 模式 | 应用点 | 价值 |
| --- | --- | --- |
| Facade | `LockObservationService` | 为各模块提供统一观测入口 |
| Observer | 业务锁模块发布 `LockEvent` | 观测层被动接收状态变化 |
| Repository | `LockEventRegistry`、`WaitSlotRegistry`、`DeadlockReportRepository` | 管理诊断事实和报告 |
| Snapshot | `LockDiagnosticSnapshot` | 提供 epoch 一致的只读诊断视图 |
| Strategy | `VictimSelectionPolicy`、`InstrumentationPolicy`、`RedactionPolicy` | 可替换 victim、采样和脱敏策略 |
| Chain of Responsibility | `DeadlockReportBuilder` | 按 row lock、MDL、混合链生成报告 |
| State | lock request、wait slot、MDL status | 明确状态和所有权变化 |
| Adapter | Performance Schema / Information Schema views | 把内部快照适配为 SQL 可查询表 |
| Builder | snapshot row builder | 延迟格式化和裁剪大快照 |
| Template Method | deadlock detection scan | 固定复制图、检测环、选择 victim、写报告流程 |

## 14. 高内聚、低耦合约束

- 观测层只维护诊断事实，不维护业务锁真相。
- Wait-For Graph 只保存逻辑等待边，不保存 physical latch 等待边。
- Performance Schema adapter 只读取 snapshot，不读取 LockManager 或 MDL manager。
- Data Dictionary、Transaction、Buffer Pool、Disk Manager 只依赖 `server.lockobs.api`。
- 物理等待采样策略不能改变原锁获取顺序。
- `LOCK_DATA` 只用于诊断显示，不允许被 executor 或 optimizer 反向依赖。
- deadlock victim 选择只使用事件携带的成本摘要，不同步访问 undo、record、page 或 file。
- 快照构造不返回内部集合引用，避免诊断查询修改业务状态。
- 跨域诊断图只用于报告，不把物理等待提升为可回滚事务语义。

## 15. 典型数据流

### 15.1 row lock wait

1. Executor 调用 Storage API 执行 current read 或 DML。
2. B+Tree/Record 定位候选记录，构造 record/gap/next-key lock key。
3. 调用方释放 page latch、buffer fix 和 record cursor。
4. `LockManager` 判断冲突，将 request 入队。
5. `LockEventRegistry` 发布 `WAITING`，`WaitForGraph` 写入 `waitingTrx -> blockingTrx`。
6. `DeadlockDetector` 即时检测新增边。
7. 无死锁时线程进入 wait slot 阻塞。
8. blocker commit/rollback 释放锁后，等待请求授予，删除 graph edge。
9. 调用方重新定位记录，继续执行。

### 15.2 MDL wait

1. Session 绑定对象名，请求 table/schema MDL。
2. `MetadataLockManager` 判断与已持有 MDL 冲突。
3. request 进入 pending queue，发布 `PENDING`。
4. `WaitForGraph` 写入 `waitingSession -> blockingSession`。
5. MDL detector 检测 metadata 环。
6. 如果无环，等待 blocker release。
7. grant 后状态更新为 `GRANTED`，statement 获得 dictionary pin。

### 15.3 physical latch long wait

1. Buffer Pool 线程尝试获取 frame X latch。
2. 短自旋未成功后打开 wait slot。
3. `LatchInstrumentationRegistry` 记录 object instance 和当前 owner thread。
4. 等待结束后更新 wait event 计时。
5. 快照查询可看到等待 thread、owner thread、source 和等待时长。
6. detector 不选择 victim；超时和 kill 由调用方策略处理。

### 15.4 diagnostic snapshot

快照流程见 [lock-observability-snapshot-flow.mmd](diagrams/lock-observability-snapshot-flow.mmd)。

1. 用户查询 Performance Schema 风格视图。
2. `LockSnapshotService` 分配 `SnapshotEpoch`。
3. 按固定顺序采集 wait slot、lock registry、graph、physical registry 和 report 摘要。
4. 对 graph edge 与 lock rows 做关联。
5. 对不存在或已释放的 blocker 做裁剪标记。
6. 根据权限对 `LOCK_DATA` 脱敏。
7. 返回只读表行。

### 15.5 deadlock victim

1. `WaitForGraph` 新增边后发现环。
2. `DeadlockDetector` 复制环内边属性。
3. `VictimSelectionPolicy` 按成本选择 victim。
4. report repository 写入 deadlock report。
5. victim request 标记 `VICTIM`。
6. 等待线程唤醒并抛出 deadlock 异常。
7. 事务或 statement cleanup 释放锁并删除 graph edge。

### 15.6 kill query

1. 管理命令设置 session 当前 statement kill flag。
2. `WaitSlotRegistry` 标记 wait slot `KILLED`。
3. 业务等待模块被唤醒，移除 pending lock request。
4. 观测层发布 `KILLED` 和 `CLEANUP_RELEASED`。
5. Executor 执行 statement cleanup；显式事务是否继续由 SQL policy 决定。

## 16. 测试设计

虽然本次不写代码，后续实现应覆盖：

- 事件登记测试：requested、granted、waiting、released、timeout、victim、killed 顺序和幂等清理。
- WaitSlot 测试：thread current event id 唯一、等待完成计时、cleanup 后可复用。
- data_locks 视图测试：TABLE/RECORD/GAP/NEXT_KEY/INSERT_INTENTION 字段映射、脱敏策略。
- data_lock_waits 测试：一对一、一对多 blocker、blocker release 后裁剪标记。
- metadata_locks 视图测试：MDL type、duration、status、owner event 映射。
- mutex/rwlock/condition 视图测试：owner thread、waiter thread、condition source。
- row lock deadlock 测试：两个事务交叉更新形成环，选择低成本 victim 并释放边。
- MDL deadlock 测试：两个 session 多表 DDL 形成 metadata 环，victim request 终止。
- 混合链诊断测试：MDL 与 row lock 交织时只报告跨域链，不自动回滚跨域 victim。
- wait timeout 测试：timeout 清理 wait slot、pending request 和 graph edge。
- kill query 测试：kill 等待中的 row lock、MDL 和 condition。
- snapshot 一致性测试：并发 release/grant 中 snapshot 不抛出异常，epoch 和 partial/truncated 标记正确。
- 低侵入测试：高频 mutex wait 采样不阻塞业务锁 release。
- 权限脱敏测试：无诊断权限用户看不到完整 `LOCK_DATA`。
- 故障注入测试：registry shard 异常时禁用 detector 并依赖 timeout。
- property-based 测试：随机 acquire/wait/grant/release 后 graph 无悬空边，当前视图与 registry 不变量一致。

## 17. 后续实现顺序

1. `server.lockobs.domain`：标识对象、状态枚举、事件和值对象。
2. `WaitSlotRegistry`：thread event id、当前等待槽和 cleanup。
3. `LockEventRegistry`：row lock 与 MDL 的 granted/waiting/release 事件。
4. `WaitForGraph`：分片邻接表、边增加、边删除和图快照。
5. row lock immediate deadlock detector 和 victim report。
6. MDL graph 与 MDL victim callback。
7. `LockSnapshotService`：epoch、row builder、partial/truncated 标记。
8. Performance Schema 风格 `data_locks` 与 `data_lock_waits` adapter。
9. Performance Schema 风格 `metadata_locks` adapter。
10. wait event、mutex、rwlock、condition 观测。
11. Information Schema 兼容视图。
12. cross-domain diagnostic graph 和混合链报告。
13. low intrusion sampling、redaction、snapshot row limit policy。
14. kill query/session 与 timeout 清理集成。
15. 并发压测、故障注入和文档示例查询。

## 18. 十五轮自检记录

| 轮次 | 检查主题 | 检查结果 |
| --- | --- | --- |
| 1 | 是否只写设计文档 | 本次内容只定义 Markdown 设计和 Mermaid 图，没有生成 Java 源码 |
| 2 | 目标与非目标 | 已明确观测、等待图、死锁诊断目标，并排除授锁、事务回滚实现和物理锁自动 victim |
| 3 | MySQL 8.0 贴合 | 已覆盖 Performance Schema `data_locks`、`data_lock_waits`、`metadata_locks`、wait event、mutex instance 和 InnoDB deadlock detection |
| 4 | 高内聚 | 事件登记、wait slot、graph、detector、snapshot、view、report、policy 分包职责独立 |
| 5 | 低耦合 | 已规定业务模块只发布事件，观测层不访问 LockManager、MDL、BufferFrame 或文件锁内部结构 |
| 6 | 面向对象 | 已定义值对象、领域事件、registry、graph、snapshot、policy、report repository 和 adapter |
| 7 | 设计模式 | 已列出 Facade、Observer、Repository、Snapshot、Strategy、State、Adapter、Builder 等使用点 |
| 8 | 核心领域模型 | 已定义 `DiagnosticLockId`、`WaitActorId`、`LockEvent`、`WaitSlot`、`WaitForGraph` 等模型 |
| 9 | 模块依赖方向 | 已给出 SQL、DD、事务、Buffer Pool、Disk Manager 到观测 API 的单向依赖和禁止方向 |
| 10 | 物理与逻辑区分 | 已区分 row/MDL 逻辑锁、page/file latch、mutex、condition、wait slot 和诊断快照 |
| 11 | 关键数据流 | 已给出 row lock wait、MDL wait、physical latch wait、snapshot、deadlock victim、kill query 流程 |
| 12 | 必要图示 | 已提供架构图、类关系图、等待图流程、死锁检测流程、快照流程和状态机 6 张图 |
| 13 | 并发锁状态 | 已说明锁状态、wait slot 状态、持有者变化、锁顺序、等待前释放规则和死锁检测域 |
| 14 | 异常与恢复策略 | 已定义 deadlock、timeout、kill、snapshot 过大、观测降级和 graph 清理策略 |
| 15 | 测试与实现顺序 | 已给出测试设计、后续实现顺序，并检查没有未完成标记或空白章节 |

## 19. 参考链接

- MySQL 8.0 Reference Manual - Performance Schema `data_locks` Table: https://dev.mysql.com/doc/refman/8.0/en/performance-schema-data-locks-table.html
- MySQL 8.0 Reference Manual - Performance Schema `data_lock_waits` Table: https://dev.mysql.com/doc/refman/8.0/en/performance-schema-data-lock-waits-table.html
- MySQL 8.0 Reference Manual - Performance Schema `metadata_locks` Table: https://dev.mysql.com/doc/refman/8.0/en/performance-schema-metadata-locks-table.html
- MySQL 8.0 Reference Manual - Performance Schema Wait Event Tables: https://dev.mysql.com/doc/refman/8.0/en/performance-schema-wait-tables.html
- MySQL 8.0 Reference Manual - Performance Schema `events_waits_current` Table: https://dev.mysql.com/doc/refman/8.0/en/performance-schema-events-waits-current-table.html
- MySQL 8.0 Reference Manual - Performance Schema `mutex_instances` Table: https://dev.mysql.com/doc/refman/8.0/en/performance-schema-mutex-instances-table.html
- MySQL 8.0 Reference Manual - InnoDB Deadlock Detection: https://dev.mysql.com/doc/refman/8.0/en/innodb-deadlock-detection.html
- MySQL 8.0 Reference Manual - InnoDB Transaction and Locking Information: https://dev.mysql.com/doc/refman/8.0/en/innodb-information-schema-examples.html
- MySQL 8.0 Reference Manual - InnoDB Locking: https://dev.mysql.com/doc/refman/8.0/en/innodb-locking.html
- MySQL 8.0 Reference Manual - Metadata Locking: https://dev.mysql.com/doc/refman/8.0/en/metadata-locking.html
