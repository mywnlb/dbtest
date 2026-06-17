# MiniMySQL MySQL 8.0 风格 Statistics 与 ANALYZE TABLE 模块设计

版本：2026-06-05  
实现语言：Java  
参考基线：MySQL 8.0.46 ANALYZE TABLE、Optimizer Statistics、InnoDB Persistent Statistics、Data Dictionary、Metadata Locking  
关联设计：[mysql-query-optimizer-design.md](mysql-query-optimizer-design.md)、[mysql-data-dictionary-ddl-design.md](mysql-data-dictionary-ddl-design.md)、[mysql-sql-executor-storage-api-design.md](mysql-sql-executor-storage-api-design.md)、[mysql-parser-binder-design.md](mysql-parser-binder-design.md)、[innodb-storage-engine-overview.md](innodb-storage-engine-overview.md)、[innodb-btree-design.md](innodb-btree-design.md)

## 1. 目标与边界

本设计面向 MiniMySQL 的 `sql.stats` 模块，覆盖 `ANALYZE TABLE`、持久化统计信息、索引基数估算、列直方图、统计缓存、优化器统计快照和统计失效。它是统计生命周期的所有者；Optimizer 只读取不可变快照，不在自身模块内维护统计更新、持久化或失效流程。

设计目标：

- 高内聚：ANALYZE 执行、采样、histogram 构建、持久化、缓存、快照和失效都收敛在 `sql.stats`。
- 低耦合：Stats 通过 Data Dictionary 和 Storage/B+Tree 统计接口协作，不读取 BufferFrame、record byte 或 optimizer 内部计划。
- MySQL 8.0 风格：对齐 `ANALYZE TABLE`、InnoDB persistent statistics、histogram statistics、dictionary version 和 MDL。
- 面向对象：使用 `AnalyzeRequest`、`TableStats`、`IndexStats`、`ColumnHistogram`、`StatsVersion`、`OptimizerStatsSnapshot` 等不可变对象。
- 并发安全：ANALYZE 与 DDL/DML/Optimizer 读取之间的 MDL、StatsUpdateLock、cache snapshot 状态和所有权变化必须明确。
- 可测试：覆盖采样、持久化、缓存替换、DDL 失效、plan cache 失效和 crash recovery。

非目标：

- 不构建查询执行计划，不决定 join order 或 access path。
- 不直接扫描 Buffer Pool 内部结构。
- 不实现完整自动统计重算策略；第一阶段保留 dirty row counter 和 auto recalc 扩展点。
- 不让用户直接修改统计表。
- 不生成 Java 源码。本文件只定义设计、关系和数据流。

## 2. MySQL 8.0 参考依据

本设计参考 MySQL 8.0 的以下行为：

- `ANALYZE TABLE` 用于更新表统计信息，InnoDB 会重新估算 key distribution。
- InnoDB persistent optimizer statistics 保存表和索引统计，典型数据包括 row count、clustered/index page count、索引前缀 distinct 估算和 sample size。
- MySQL histogram statistics 由 server 维护，支持 singleton 和 equi-height histogram，bucket 数可配置。
- histogram 主要用于常量谓词选择率估算；range/index dive 的估算在可用时应优先。
- Data Dictionary 和 `INFORMATION_SCHEMA.COLUMN_STATISTICS` 暴露统计信息，用户不应直接修改内部 DD 统计表。
- DDL 改变列、索引、表版本时，旧统计和旧计划必须失效。

简化点：

| MySQL 8.0 能力 | MiniMySQL 第一阶段 |
| --- | --- |
| 自动统计重算 | 先支持显式 `ANALYZE TABLE`，保留 dirty row counter |
| 复杂 index dive 和 sampling | 先通过 Storage API/B+Tree sampling plan 抽象 |
| 完整 histogram 类型和 JSON 格式 | 支持 singleton、equi-height 和紧凑持久格式 |
| 全量 INFORMATION_SCHEMA 输出 | 先提供统计快照和诊断 DTO |
| 在线 ANALYZE 与 DML 并发 | 第一阶段采用保守 `MDL_SHARED_NO_WRITE` |

## 3. 总体架构

架构图见 [statistics-analyze-architecture.mmd](diagrams/statistics-analyze-architecture.mmd)。

核心链路：

`AnalyzeTableExecutor -> StatsCollector -> HistogramBuilder -> PersistentStatsStore -> StatsCache -> OptimizerStatsSnapshot`

职责划分：

- `AnalyzeTableExecutor` 处理 SQL 语句、MDL、权限、结果集和资源释放。
- `StatsCollector` 通过 Storage/B+Tree 稳定接口采样，不访问页缓存内部。
- `HistogramBuilder` 构建列级 histogram，不写 DD。
- `PersistentStatsStore` 事务化写入 DD 统计表。
- `StatsCache` 管理不可变快照、single-flight loading、publish 和 invalidation。
- `OptimizerStatsSnapshot` 是 Optimizer statement 期间稳定的只读视图。

## 4. 包与职责

| 包 | 职责 | 依赖 | 主要模式 |
| --- | --- | --- | --- |
| `sql.stats.api` | 统计模块门面、snapshot provider | cache, snapshot | Facade |
| `sql.stats.analyze` | `ANALYZE TABLE` 执行、结果集、锁编排 | dd, collect | Template Method |
| `sql.stats.collect` | table/index/page sampling、采样计划 | storage api | Strategy |
| `sql.stats.histogram` | bucket 构建、采样率、NULL 比例 | type domain | Builder, Strategy |
| `sql.stats.store` | DD 统计表读写、事务化持久化 | dd, trx | Repository |
| `sql.stats.cache` | 快照缓存、single flight、失效、发布 | store | State, Cache |
| `sql.stats.snapshot` | Optimizer 只读快照 | domain | Snapshot |
| `sql.stats.invalidate` | DDL/ANALYZE/statistics event 处理 | dd, plan cache | Observer |
| `sql.stats.metric` | analyze 耗时、采样页、cache 命中 | 无 | Observer |

禁止方向：

- `sql.stats` 不能访问 `BufferFrame`、flush list、page hash 或 redo buffer。
- Optimizer 不能反向更新 stats store。
- StatsCollector 不能修改 DD、MDL 或 PlanCache。
- HistogramBuilder 不能访问 Storage API。

## 5. 核心领域模型

类关系图见 [statistics-analyze-class-relation.mmd](diagrams/statistics-analyze-class-relation.mmd)。

| 对象 | 职责 |
| --- | --- |
| `AnalyzeRequest` | table、column、histogram options、sample policy |
| `AnalyzeResultRow` | `ANALYZE TABLE` 返回给用户的状态行 |
| `StatsVersion` | 统计版本，绑定 `DictionaryVersion` |
| `TableStats` | row count estimate、page count、modified counter、sample pages |
| `IndexStats` | index size、leaf pages、distinct prefix estimates、sample size |
| `ColumnHistogram` | bucket、type、NULL ratio、sampling rate、collation |
| `IndexSamplingPlan` | 采样页数、索引、采样方式、随机种子 |
| `StatsMutationBatch` | 一次 ANALYZE 要持久化的统计变更 |
| `OptimizerStatsSnapshot` | Optimizer statement 内稳定的只读统计集合 |
| `StatsCacheEntry` | cache state、snapshot、pin count、version |

## 6. 统计表、快照与直方图格式

### 6.1 持久化统计表

第一阶段设计为 DD 内部表：

| 表 | 主要字段 | 说明 |
| --- | --- | --- |
| `dd_table_stats` | `table_id`, `dictionary_version`, `stats_version`, `row_count`, `page_count`, `sample_pages` | 表级估算 |
| `dd_index_stats` | `index_id`, `table_id`, `n_diff_pfx`, `leaf_pages`, `size_pages`, `sample_size` | 索引基数 |
| `dd_column_histograms` | `table_id`, `column_id`, `stats_version`, `histogram_type`, `bucket_count`, `payload` | 列直方图 |

### 6.2 逻辑与物理边界

| 层面 | 对象 | 所属模块 | Stats 模块职责 |
| --- | --- | --- | --- |
| 物理采样 | page count、leaf page、sample cursor | Storage/B+Tree | 只通过统计接口读取 |
| 页内记录 | record decode、column value | Record/Storage API | 只接收抽象行样本或统计 DTO |
| 持久统计 | DD stats tables | stats.store + DD | 事务化读写 |
| 逻辑快照 | `OptimizerStatsSnapshot` | stats.snapshot | 不可变、可 pin |
| 执行计划 | plan cache/optimizer | Optimizer/PlanCache | 只接收失效事件 |

### 6.3 Histogram

Histogram 类型：

- `SINGLETON`：适合 distinct 值较少的列。
- `EQUI_HEIGHT`：适合连续或高基数字段。

字段：

- `columnId`
- `histogramType`
- `bucketCount`
- `nullRatio`
- `samplingRate`
- `collation`
- `buckets`
- `statsVersion`

bucket 数默认 100，范围 1 到 1024。第一阶段可限制可建 histogram 的类型为 numeric、string、date/time。

## 7. 核心策略和算法

### 7.1 ANALYZE TABLE

流程见 [statistics-analyze-flow.mmd](diagrams/statistics-analyze-flow.mmd)。

标准流程：

1. Parser/Binder 生成 `AnalyzeRequest`。
2. `AnalyzeTableExecutor` 获取目标表 `MDL_SHARED_NO_WRITE`。
3. pin `TableDefinition` 和 `DictionaryVersion`。
4. 获取 `StatsUpdateLock(X)`，序列化同表并发 ANALYZE。
5. `StatsCollector` 构建 `IndexSamplingPlan`。
6. 收集 table/index stats。
7. 如果用户请求 histogram，`HistogramBuilder` 构建列直方图。
8. `PersistentStatsStore` 在字典事务内写入统计。
9. `StatsCache` copy-on-write 发布新 snapshot。
10. 通知 PlanCache 失效相关计划。
11. 返回 `AnalyzeResultRow`。

### 7.2 Index Cardinality Sampling

采样输入：

- index id。
- leaf page estimate。
- sample page count。
- random seed。
- prefix count list。

输出：

- `n_diff_pfx01`、`n_diff_pfx02` 等前缀 distinct 估算。
- `leafPages`
- `sizePages`
- `sampleSize`
- confidence。

StatsCollector 只能调用 B+Tree/Storage 的统计接口，不能自己遍历 Buffer Pool 或解释 page directory。

### 7.3 Optimizer Snapshot

Optimizer 读取规则：

- 每个 optimizer statement acquire 一个 `OptimizerStatsSnapshot`。
- snapshot 不原地修改。
- ANALYZE 发布新 snapshot 时，旧 snapshot 进入 `REPLACED`，已 pin 的 Optimizer 继续使用旧对象。
- cache miss 时可从 store load；load 失败时返回默认低置信度估算，不阻塞优化器太久。

## 8. ANALYZE TABLE 语句设计

第一阶段支持：

- `ANALYZE TABLE table_name`
- `ANALYZE TABLE table_name UPDATE HISTOGRAM ON column_list WITH N BUCKETS`
- `ANALYZE TABLE table_name DROP HISTOGRAM ON column_list`

语义：

- 无 histogram 子句时更新 table/index key distribution，不删除已有 histogram。
- update histogram 只更新指定 column histogram。
- drop histogram 删除指定 histogram，并发布新 stats version。
- ANALYZE 结果以 result set 返回状态和 message。

## 9. 与其它模块的协作

### 9.1 与 Parser/Binder

- Binder 将 ANALYZE AST 转换为 `AnalyzeRequest`。
- Binder 负责规范化 table/column 名称和基础权限需求。
- Stats 模块负责具体执行和统计持久化。

### 9.2 与 Data Dictionary / DDL

- stats store 是 DD 内部数据，绑定 `DictionaryVersion`。
- DDL commit 发布 `DictionaryVersionChangedEvent`，StatsCache 标记旧统计 stale。
- column type、collation、index key part 变化后旧 histogram/index stats 不可复用。

### 9.3 与 Optimizer

- Optimizer 通过 `StatisticsProvider` adapter 读取 `OptimizerStatsSnapshot`。
- Optimizer 不更新 stats cache。
- histogram 用于选择率估算；range/index 估算可用且置信度更高时优先。

### 9.4 与 Plan Cache

- ANALYZE commit 发布 `StatsVersionChangedEvent`。
- PlanCache 对依赖该 table/index/column 的计划标记 stale。
- 正在执行的计划由自身引用 guard 保活。

### 9.5 与 Storage / B+Tree

- Storage API 提供 table/index sampling DTO。
- B+Tree 可提供 leaf page count、height、distinct prefix 采样入口。
- StatsCollector 不获取 page latch 后等待 MDL 或 stats lock。

## 10. 并发与锁顺序

统计快照状态图见 [statistics-snapshot-concurrency-state.mmd](diagrams/statistics-snapshot-concurrency-state.mmd)。

### 10.1 锁与等待对象

| 对象 | 保护资源 | 持有者 | 死锁域 |
| --- | --- | --- | --- |
| MDL `SHARED_NO_WRITE` | 目标表元数据和 DML 写入互斥 | ANALYZE session | MetadataWaitGraph |
| `StatsUpdateLock(X)` | 同表统计更新 | AnalyzeTableExecutor | timeout/retry |
| `StatsCacheMutex` | cache entry map | StatsCache | 不跨外部调用 |
| `OptimizerSnapshotPin` | snapshot 生命周期 | Optimizer statement | 不进入死锁图 |
| DD transaction lock | 统计表持久化 | PersistentStatsStore | DD 内部事务 |
| Storage sampling cursor | 采样过程 | StatsCollector | 不进入事务死锁图 |

### 10.2 状态与持有变化

| 状态 | 持有者 | 持有资源 | 进入条件 | 退出条件 |
| --- | --- | --- | --- | --- |
| `REQUESTED` | session | AnalyzeRequest | 收到 ANALYZE | 请求 MDL |
| `MDL_WAITING` | MDL manager | wait slot | DDL 或 DML 冲突 | grant、timeout、killed |
| `MDL_GRANTED` | ANALYZE session | table MDL | 可执行 ANALYZE | cleanup |
| `DICT_PINNED` | statement guard | TableDefinition pin | MDL granted | cleanup |
| `UPDATE_LOCK_WAITING` | AnalyzeTableExecutor | stats update wait slot | 同表已有 ANALYZE | grant 或 timeout |
| `COLLECTING` | StatsCollector | sampling cursor 短持 | stats lock granted | 样本收集完成 |
| `BUILDING_HISTOGRAM` | HistogramBuilder | in-memory sample | histogram requested | bucket 完成 |
| `PERSISTING` | PersistentStatsStore | DD transaction | mutation batch ready | commit 或 rollback |
| `PUBLISHING_CACHE` | StatsCache | cache mutex 短持 | store commit 成功 | snapshot swap 完成 |
| `RESULT_READY` | executor | result rows | publish 完成 | release |
| `FAILED` | cleanup owner | error context | 任一阶段失败 | cleanup |
| `RELEASED` | 无 | 无统计锁 | cleanup 完成 | 返回 |

规则：

- 等待 MDL 或 StatsUpdateLock 前不能持有 StorageCursor、page latch、MTR latch、Buffer Pool mutex 或 row lock。
- `StatsCacheMutex` 只短持有，不能跨 DD repository load、Storage sampling 或 histogram build。
- Optimizer 读取 snapshot 不等待 ANALYZE 完成。
- DDL invalidation 不销毁已 pin snapshot，只标记 stale。

## 11. 缓存失效与快照替换

缓存失效流程见 [statistics-cache-invalidation-flow.mmd](diagrams/statistics-cache-invalidation-flow.mmd)。

失效来源：

- ANALYZE commit 发布新 `StatsVersion`。
- DDL commit 发布新 `DictionaryVersion`。
- DROP/ALTER column/index/table。
- manual cache flush。

规则：

- `StatsCache` key 包含 `TableId + DictionaryVersion + StatsVersion`。
- 新 snapshot 用 copy-on-write 发布。
- 旧 snapshot 状态从 `READY/PINNED` 转为 `REPLACED` 或 `STALE`。
- pin count 归零后允许 evict。
- PlanCache 接收 statistics/dictionary event 后标记相关 plan stale。

## 12. 异常处理与恢复策略

异常类型：

- `AnalyzeTableException`
- `StatsCollectionException`
- `HistogramBuildException`
- `PersistentStatsStoreException`
- `StatsCachePublishException`
- `StatsSnapshotStaleException`
- `StatsUpdateTimeoutException`
- `UnsupportedHistogramTypeException`

恢复策略：

- 采样失败：本次 ANALYZE 失败，不发布部分统计。
- histogram build 失败：不写 histogram mutation，已有 histogram 保持不变。
- 持久化失败：回滚 DD 统计事务，不发布 cache。
- cache publish 失败：持久化已成功时，启动或下一次 cache miss 从 store rebuild。
- crash 发生在 ANALYZE 持久化前：旧 stats version 保持可用。
- crash 发生在持久化后 cache publish 前：Recovery 后 StatsCache 从 store 重新加载新 version。

## 13. API 设计

### 13.1 AnalyzeTableExecutor

- `execute(AnalyzeRequest request, SessionContext session)`
- `analyzeTable(TableId tableId, AnalyzeOptions options)`
- `updateHistogram(TableId tableId, List<ColumnId> columns, HistogramOptions options)`
- `dropHistogram(TableId tableId, List<ColumnId> columns)`

### 13.2 StatsCollector

- `collectTableStats(TableDefinition table, SamplingOptions options)`
- `collectIndexStats(IndexDefinition index, IndexSamplingPlan plan)`
- `collectColumnSamples(ColumnDefinition column, SamplingOptions options)`

### 13.3 PersistentStatsStore

- `loadSnapshot(TableId tableId, DictionaryVersion version)`
- `persist(StatsMutationBatch batch, DictionaryTransaction tx)`
- `deleteForDroppedObject(DictionaryObjectId objectId, DictionaryTransaction tx)`

### 13.4 StatsCache

- `getSnapshot(TableId tableId, DictionaryVersion version)`
- `publish(OptimizerStatsSnapshot snapshot)`
- `invalidate(StatsInvalidationEvent event)`
- `pin(StatsCacheKey key)`
- `release(OptimizerStatsSnapshot snapshot)`

## 14. 设计模式使用清单

| 模式 | 应用点 | 价值 |
| --- | --- | --- |
| Facade | `StatisticsService` / `AnalyzeTableExecutor` | 隐藏采样、store、cache |
| Repository | `PersistentStatsStore` | 封装 DD 统计表 |
| Strategy | sampling、histogram、selectivity fallback | 支持不同统计策略 |
| Snapshot | `OptimizerStatsSnapshot` | Optimizer statement 内稳定 |
| Observer | DDL/ANALYZE event listener | 触发 cache 和 plan invalidation |
| State | `StatsCacheEntry` | 管理 loading、ready、stale、replaced |
| Builder | `HistogramBuilder`、`StatsMutationBatchBuilder` | 分阶段构建统计对象 |
| RAII Guard | snapshot pin、MDL/DD pin | 异常路径释放资源 |

## 15. 高内聚、低耦合约束

- Stats 模块拥有统计生命周期，Optimizer 只读 snapshot。
- StatsCollector 不访问 Buffer Pool 内部结构。
- HistogramBuilder 不写 DD，不访问 Storage。
- PersistentStatsStore 不构建 histogram。
- StatsCache 不执行采样。
- ANALYZE 不持有底层 page latch 等待 MDL 或 stats update lock。
- 统计对象必须绑定 DictionaryVersion。
- DDL version 改变后旧统计不可用于新绑定语句。

## 16. 典型数据流

### 16.1 ANALYZE TABLE

1. Parser/Binder 输出 `AnalyzeRequest`。
2. Executor 获取 MDL 和 DD pin。
3. StatsCollector 采样 table/index。
4. HistogramBuilder 构建需要的 histogram。
5. PersistentStatsStore 提交新 stats version。
6. StatsCache 发布 snapshot。
7. PlanCache 标记相关计划 stale。

### 16.2 Optimizer 读取统计

1. Optimizer 请求 `StatisticsProvider.snapshot(tableId, dictionaryVersion)`。
2. StatsCache 命中则 pin snapshot。
3. cache miss 则从 PersistentStatsStore load，或返回低置信度默认估算。
4. Optimizer statement 结束释放 snapshot pin。

### 16.3 DDL 失效

1. DDL commit 发布 dictionary version event。
2. StatsCache 找到同 table/index/column 的旧 entry。
3. 旧 entry 标记 stale。
4. PlanCache 标记依赖旧 version 的计划 stale。
5. 新 statement 需要重新绑定并读取新 version 统计。

### 16.4 Crash Recovery

1. Redo/DDL recovery 恢复 DD 统计表。
2. StatsCache 启动为空。
3. 第一次 optimizer request 从 store 加载 snapshot。
4. 如果 store 中有不匹配 dictionary version 的统计，标记 stale 并忽略。

## 17. 测试设计

- TableStats 测试：row count、page count、sample pages。
- IndexStats 测试：distinct prefix、leaf pages、sample size。
- Histogram 测试：singleton、equi-height、NULL ratio、bucket count。
- ANALYZE 测试：无 histogram、update histogram、drop histogram。
- Store 测试：persist/load/delete、事务 rollback。
- Cache 测试：single-flight loading、publish、pin、stale、evict。
- Optimizer 协作测试：读取 snapshot、默认估算、旧 snapshot 被 replaced 后仍可使用。
- DDL 失效测试：drop column/index/table、alter type/collation、rename。
- PlanCache 失效测试：stats version 改变后相关 plan stale。
- 并发测试：ANALYZE 与 optimizer 读取并发、并发 ANALYZE 同表串行。
- 锁顺序测试：等待 MDL/StatsUpdateLock 时不持有 StorageCursor。
- 故障注入：采样失败、持久化失败、cache publish 失败、crash 后 reload。

## 18. 后续实现顺序

1. `StatsVersion`、`TableStats`、`IndexStats`、`ColumnHistogram`。
2. `OptimizerStatsSnapshot` 和 snapshot pin。
3. `PersistentStatsStore` 接口。
4. `StatsCache` 和状态机。
5. `StatsInvalidationEvent`。
6. Optimizer `StatisticsProvider` adapter。
7. `AnalyzeRequest` 和 Binder 接入。
8. `AnalyzeTableExecutor`。
9. `StatsCollector` 最小 table/index stats。
10. `HistogramBuilder`。
11. DD 统计表事务化写入。
12. PlanCache invalidation 接入。
13. crash recovery reload。
14. metrics 和 diagnostic snapshot。
15. 故障注入和并发集成测试。

## 19. 十五轮自检记录

| 轮次 | 检查主题 | 检查结果 |
| --- | --- | --- |
| 1 | 是否只写设计文档 | 本文只新增 Markdown 和 Mermaid 设计内容，没有生成 Java 实现代码 |
| 2 | 目标与非目标 | 已明确 Stats 不替代 Optimizer、DD、Executor 或 Storage 内部采样实现 |
| 3 | MySQL 8.0 贴合 | 已覆盖 ANALYZE TABLE、persistent stats、histogram、sampling、dictionary version |
| 4 | 高内聚 | ANALYZE、collect、histogram、store、cache、snapshot、invalidation 子包职责独立 |
| 5 | 低耦合 | Optimizer 只读 snapshot，Stats 不访问 BufferFrame 或 optimizer plan 内部 |
| 6 | 面向对象 | 已定义 AnalyzeRequest、TableStats、IndexStats、ColumnHistogram、StatsVersion 等对象 |
| 7 | 设计模式 | 已列出 Facade、Repository、Strategy、Snapshot、Observer、State、Builder 等模式 |
| 8 | 核心领域模型 | 已覆盖 table stats、index stats、histogram、mutation batch、cache entry |
| 9 | 依赖方向 | 已明确 SQL -> stats -> DD/storage，Optimizer -> stats snapshot only |
| 10 | 物理与逻辑区分 | 已区分 page sampling、DD 持久统计、optimizer snapshot 和 plan cache |
| 11 | 关键数据流 | 已给出 ANALYZE、optimizer 读取、DDL 失效、crash recovery |
| 12 | 图示 | 已提供架构图、类关系图、ANALYZE 流程、快照状态、缓存失效图 |
| 13 | 并发锁状态 | 已定义 MDL、StatsUpdateLock、cache mutex、snapshot pin 的状态和持有变化 |
| 14 | 异常与恢复 | 已覆盖采样失败、持久化失败、cache 发布失败和 crash 后 reload |
| 15 | 测试与顺序 | 已给出测试设计、实现顺序，并确认没有未完成标记或空白项 |

## 20. 参考链接

- MySQL 8.0 Reference Manual - ANALYZE TABLE Statement: https://dev.mysql.com/doc/refman/8.0/en/analyze-table.html
- MySQL 8.0 Reference Manual - InnoDB Persistent Statistics: https://dev.mysql.com/doc/refman/8.0/en/innodb-persistent-stats.html
- MySQL 8.0 Reference Manual - Optimizer Statistics: https://dev.mysql.com/doc/refman/8.0/en/optimizer-statistics.html
- MySQL 8.0 Reference Manual - InnoDB ANALYZE TABLE Complexity: https://dev.mysql.com/doc/refman/8.0/en/innodb-analyze-table-complexity.html
- MySQL 8.0 Reference Manual - Metadata Locking: https://dev.mysql.com/doc/refman/8.0/en/metadata-locking.html
