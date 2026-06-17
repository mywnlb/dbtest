# MiniMySQL InnoDB 风格 Redo Log 模块设计

版本：2026-06-04  
实现语言：Java  
参考基线：MySQL 8.0.46 InnoDB 官方手册与源码文档  
关联设计：[innodb-crash-recovery-design.md](innodb-crash-recovery-design.md)，[innodb-flush-checkpoint-doublewrite-design.md](innodb-flush-checkpoint-doublewrite-design.md)，[innodb-disk-manager-design.md](innodb-disk-manager-design.md)，[innodb-buffer-pool-design.md](innodb-buffer-pool-design.md)，[innodb-transaction-mvcc-design.md](innodb-transaction-mvcc-design.md)，[innodb-undo-log-purge-design.md](innodb-undo-log-purge-design.md)

## 1. 目标与边界

本设计面向 MiniMySQL 存储引擎的 `storage.redo` 模块。磁盘管理模块已经定义 MTR、页、表空间与文件；Buffer Pool 已经定义 dirty page、flush list 与 `pageLSN`；事务模块已经定义 undo、提交和恢复边界。Redo Log 模块负责 write-ahead logging、LSN 分配、redo record 编码、log buffer、redo 文件写入、刷盘等待、checkpoint 和 crash recovery 的物理重放。

设计目标：

- 高内聚：LSN、redo record、log buffer、writer/flusher、文件轮转、checkpoint、恢复扫描和指标都收敛在 `storage.redo` 内部。
- 低耦合：MTR、Buffer Pool、Flush、Transaction、Recovery 只依赖 `RedoLogManager`、`RedoLogWriter`、`CheckpointService`、`RedoRecoveryReader` 等稳定接口，不直接操作 redo 文件、log block 或内部 ring buffer。
- InnoDB 风格：参考 MySQL 8.0 的 redo log files、log buffer、LSN、log writer/flusher、write/flush wait、recent written/closed、fuzzy checkpoint、redo capacity 和 recovery replay。
- Java 可落地：用门面、值对象、命令对象、策略、状态机、后台 worker、观察者和责任链表达核心机制。
- 可恢复：任何可持久化数据页修改都必须先写 redo，再允许数据页刷盘；恢复阶段从 checkpoint LSN 后扫描 redo，并以 pageLSN 保证幂等重放。

非目标：

- 不实现 MySQL 二进制 redo 格式兼容，也不复刻所有 InnoDB `MLOG_*` 类型。
- 不实现 binlog、复制、XA 完整协议和备份归档的全部细节。
- 不让 Redo 模块理解 SQL、B+Tree 结构、MVCC 可见性或 undo 业务语义。
- 不生成 Java 源码。本文件只定义设计、关系和数据流。

术语边界：

- `Redo Log` 是物理重做日志，保证 crash 后能把数据页推进到 crash 前已经写入 redo 的状态。
- `Undo Log` 是事务回滚和 MVCC 版本链，不属于 Redo 模块，但普通 undo page 的修改本身需要 redo 保护。
- `MiniTransaction` / `MTR` 是 redo record 的收集者和提交边界；Redo 模块负责分配 LSN、写入、刷盘和恢复读取。
- `Checkpoint` 是 redo 可回收边界，不等于“所有脏页立刻刷完”。本设计采用 fuzzy checkpoint。

## 2. MySQL 8.0 参考依据

本设计参考以下 MySQL 8.0 行为，文末列出官方链接。

- InnoDB redo log 是磁盘结构，用于 crash recovery；正常运行时，表数据修改和低层 API 修改被编码为 redo record。
- Redo 以不断增长的 LSN 表示日志位置。redo 数据追加写入，随着 checkpoint 前进，旧 redo 在逻辑上可被覆盖或回收。
- MySQL 8.0.30 起使用 `innodb_redo_log_capacity` 控制 redo log 文件总容量；redo 文件位于 `#innodb_redo` 目录，InnoDB 尝试维护约 32 个 redo 文件。
- Log buffer 是内存中的 redo 缓冲区。大事务可通过更大的 log buffer 减少提交前被迫写盘的次数。
- MySQL 8.0.11 引入专用 log writer 线程；8.0.22 起可通过 `innodb_log_writer_threads` 调整。源码中 writer、flusher、write notifier、flush notifier 分离。
- 源码文档说明用户线程只写 log buffer，后台线程负责写/刷 redo 文件；用户线程需要 durable redo 时等待后台线程。
- 并发写入时，LSN 预留顺序、写入 log buffer 顺序、dirty page 加入 flush list 顺序可能不同。InnoDB 用 recent written 和 recent closed 之类结构追踪哪些 LSN 区间已经可写、可 checkpoint。
- InnoDB 使用 fuzzy checkpoint：后台小批量刷脏页并推进 checkpoint；恢复时从 checkpoint label 指向的位置向前扫描并应用 redo。
- crash recovery 包括 tablespace discovery、redo log application 和未提交事务 rollback。redo 应用发生在接受连接之前。
- Redo 写盘性能受 redo capacity、log buffer size、write-ahead block size、writer threads、提交刷盘策略影响。

## 3. 总体架构

架构图见 [redo-architecture.mmd](diagrams/redo-architecture.mmd)。

模块分为九组：

1. `storage.redo.api`：Redo 门面，包括 `RedoLogManager`、`RedoAppender`、`DurabilityWaiter`、`CheckpointService`。
2. `storage.redo.lsn`：`Lsn`、`Sn`、`LogRange`、LSN 分配、比较和容量窗口计算。
3. `storage.redo.record`：redo record 类型、payload、编码、校验和恢复命令。
4. `storage.redo.buffer`：log buffer、reservation、recent written、recent closed、block formatter。
5. `storage.redo.file`：redo file directory、file segment、block IO、文件轮转、容量治理。
6. `storage.redo.writer`：log writer、log flusher、write notifier、flush notifier、等待事件。
7. `storage.redo.checkpoint`：checkpoint LSN 计算、checkpoint label 写入、checkpoint pressure。
8. `storage.redo.recovery`：redo 扫描、解析、校验、按 page 分组、幂等重放。
9. `storage.redo.metric`：写入量、刷盘延迟、checkpoint age、log wait、恢复进度。

核心原则：

- Redo 模块只认识 `PageId`、`PageType`、`Lsn`、`RedoType`、`LogRange` 和二进制 payload，不解释记录格式和事务可见性。
- MTR 在 commit 时把收集到的 redo record 交给 Redo 模块，Redo 模块返回连续 `LogRange`。
- Buffer Pool 根据 MTR commit 事件发布 dirty page，并维护 `oldestModificationLsn/newestModificationLsn/pageLsn`。
- Flush 模块刷页前必须确认 `pageLsn <= redo.flushedToDiskLsn()`。
- Checkpoint 模块只推进“恢复从哪里开始”的边界，不直接刷页；刷页由 `FlushCoordinator` 执行。
- Recovery 阶段由 Redo 模块读取和解析 redo，具体 page 修改通过 `RedoApplyDispatcher` 分发给已注册的 page redo handler。

## 4. 包与职责

| 包 | 职责 | 主要依赖 | 设计模式 |
| --- | --- | --- | --- |
| `storage.redo.api` | 对 MTR、flush、recovery 暴露稳定门面 | `lsn`, `record`, `writer`, `checkpoint` | Facade |
| `storage.redo.lsn` | LSN/SN、范围、容量窗口、比较 | `domain` | Value Object |
| `storage.redo.record` | record 类型、payload、编码、校验、apply command | `domain` | Command, Codec |
| `storage.redo.buffer` | log buffer、预留、recent written/closed、block 格式化 | `lsn`, `record` | Ring Buffer, Reservation |
| `storage.redo.file` | redo 目录、文件集合、block IO、轮转、容量治理 | `io`, `config` | Repository, Strategy |
| `storage.redo.writer` | writer/flusher/notifier、等待 durable LSN | `buffer`, `file`, `lsn` | Background Worker, Observer |
| `storage.redo.checkpoint` | fuzzy checkpoint、checkpoint label、pressure 计算 | `lsn`, `writer`, `buf` | Mediator, Policy |
| `storage.redo.recovery` | redo 扫描、解析、重放、恢复统计 | `file`, `record`, `fil`, `buf` | Chain of Responsibility |
| `storage.redo.metric` | redo 指标和诊断快照 | 无 | Observer |

推荐依赖方向：

`mtr -> redo.api -> buffer + writer + checkpoint`  
`flush -> redo.api + redo.checkpoint`  
`recovery -> redo.recovery -> record + fil + buf`  
`redo.checkpoint -> buf.dirty + writer`  
`redo.writer -> file + buffer`

禁止方向：

- `storage.redo` 不能 import `storage.sql`、`storage.executor`、`storage.trx.mvcc`。
- `storage.redo.record` 不能依赖 B+Tree repository 或事务锁管理。
- `storage.redo.file` 不能读取 Buffer Pool 链表，也不能决定 dirty page 刷盘顺序。
- `storage.redo.checkpoint` 不能直接写数据页，只能请求 flush 并等待 flush 进展。
- `storage.buf` 不能直接写 redo 文件，只能通过 `RedoLogManager` 查询 durable LSN 或等待。
- `storage.trx` 不能绕过 MTR 直接写 page redo。

## 5. 核心领域模型

类关系图见 [redo-class-relation.mmd](diagrams/redo-class-relation.mmd)。

### 5.1 值对象与枚举

- `Lsn`：redo 逻辑序列号，单调递增，可比较、可计算距离。
- `Sn`：只枚举 redo 数据字节的序号。简化实现可不暴露给上层，但用于解释 log block header 和容量计算。
- `LogRange`：`startLsn + endLsn`，表示一次 MTR 或一次 record 占用的 redo 区间，区间为左闭右开。
- `RedoBlockNo`：redo 文件内部 block 编号。
- `RedoFileId`：redo 文件编号，映射到 `#ib_redoN` 或 MiniMySQL 的 `redo-N.log`。
- `RedoRecordType`：`PAGE_INIT`、`PAGE_BYTES`、`SPACE_HEADER_UPDATE`、`XDES_UPDATE`、`SEGMENT_INODE_UPDATE`、`UNDO_RECORD_INSERT`、`TRX_STATE_UPDATE`、`BTREE_PAGE_SPLIT`、`BTREE_PAGE_DELETE_MARK` 等。
- `RedoLogMode`：`REDO`、`NO_REDO_TEMPORARY`、`NO_REDO_RECOVERY_ONLY`。
- `FlushPolicy`：`AT_COMMIT_FLUSH`、`AT_COMMIT_WRITE`、`BACKGROUND_ONLY`。
- `CheckpointReason`：`PERIODIC`、`CAPACITY_PRESSURE`、`SHUTDOWN`、`EXPLICIT_REQUEST`。
- `RecoveryMode`：`NORMAL`、`FORCE_SKIP_CORRUPT_TABLESPACE`、`READ_ONLY_VALIDATE`。

约束：

- `Lsn` 不允许倒退。
- `LogRange.endLsn` 必须大于等于 `startLsn`。
- 空 MTR 可以返回空 `LogRange`，但不能推进 pageLSN。
- `NO_REDO_TEMPORARY` 只允许 temporary tablespace 和临时 undo。

### 5.2 RedoLogManager

`RedoLogManager` 是对 MTR、Flush、Recovery 暴露的门面：

- `append(MtrRedoBatch, DurabilityPolicy)`：为一个 MTR redo batch 分配 LSN、编码、写入 log buffer，返回 `CommitLogResult`。
- `waitWritten(Lsn)`：等待 redo 至少写到 OS/page cache。
- `waitFlushed(Lsn)`：等待 redo 至少 fsync 到磁盘。
- `currentLsn()`：当前已分配到的最大 LSN。
- `writtenLsn()`：后台 writer 已写出的 LSN。
- `flushedToDiskLsn()`：后台 flusher 已 fsync 的 LSN。
- `lastCheckpointLsn()`：最近持久化 checkpoint LSN。
- `requestCheckpoint(CheckpointReason)`：请求后台 checkpoint。
- `openRecoveryReader()`：启动恢复阶段读取 redo。

职责限制：

- 不创建业务 redo record。
- 不持有 page latch。
- 不触发事务 commit/rollback。
- 不直接操作 Buffer Pool 的 LRU/free/flush list。

### 5.3 MtrRedoBatch

`MtrRedoBatch` 是 MTR commit 传给 Redo 的不可变批次：

- `mtrId`
- `records`
- `modifiedPages`
- `logMode`
- `commitTimestamp`
- `requiresFlushAtCommit`

设计规则：

- `records` 顺序与 MTR 内物理修改顺序一致。
- 同一 MTR 内的 record 获得连续 LSN 区间。
- 每条 record 编码后有独立长度、类型、page id、payload checksum。
- batch 成功写入 log buffer 后，才能通知 Buffer Pool 发布 dirty page。

### 5.4 RedoRecord

`RedoRecord` 是 Command 模式对象：

- `type`
- `pageId`
- `pageType`
- `payload`
- `logicalLength`
- `encode(RedoRecordEncoder)`
- `apply(RedoApplyContext)`

record 粒度策略：

| 类型 | 用途 | 恢复幂等条件 |
| --- | --- | --- |
| `PAGE_INIT` | 初始化新页 header、trailer、page type | `pageLSN < record.endLsn` |
| `PAGE_BYTES` | 页内连续字节修改 | `pageLSN < record.endLsn` |
| `SPACE_HEADER_UPDATE` | tablespace header 元数据 | `pageLSN < record.endLsn` |
| `XDES_UPDATE` | extent descriptor bitmap/list 修改 | `pageLSN < record.endLsn` |
| `SEGMENT_INODE_UPDATE` | segment inode list、size、fragment page | `pageLSN < record.endLsn` |
| `UNDO_RECORD_INSERT` | 普通 undo page 追加 undo record | `pageLSN < record.endLsn` |
| `TRX_STATE_UPDATE` | 事务系统页或 undo header 状态 | `pageLSN < record.endLsn` |
| `BTREE_PAGE_SPLIT` | 可选高层聚合 record，便于教学调试 | handler 内拆解并校验 |

推荐最小实现：

- 初期使用 `PAGE_BYTES` + 少量 page metadata record 即可覆盖磁盘和事务模块。
- B+Tree 稳定后再增加语义更强的 record 类型，减少恢复阶段解释复杂度。
- record payload 不保存 Java 对象图，只保存可持久化的紧凑二进制字段。

### 5.5 RedoLogBuffer

`RedoLogBuffer` 是 redo 内存缓冲区：

- 环形 byte buffer。
- `reserve(bytes)` 返回 `RedoReservation`。
- `write(reservation, encodedRecords)` 把 redo bytes 写入已预留区间。
- `markWrittenToBuffer(LogRange)` 标记并发写入完成。
- `markClosed(LogRange)` 表示相关 dirty page 已加入 flush list，可以参与 checkpoint 判断。

内部状态：

- `currentLsn`：用户线程已预留到的位置。
- `readyForWriteLsn`：所有并发写入已完成，可被 writer 写出的边界。
- `closedLsn`：所有相关 dirty page 已发布到 flush list 的边界。
- `writeLsn`：已经写到 redo 文件的边界。
- `flushedLsn`：已经 fsync 的边界。

设计要点：

- LSN 预留需要全局有序，但写入 log buffer 可并发。
- Writer 只能写到 `readyForWriteLsn`。
- Checkpoint 不能越过 `closedLsn`，因为可能存在 redo 已写入但 dirty page 尚未进入 flush list 的短窗口。
- log buffer 空间不足时，用户线程等待 writer 推进 `writeLsn` 或等待 flusher/checkpoint 释放文件空间。

### 5.6 RecentWritten 与 RecentClosed

为了允许高并发 MTR commit，本设计引入两个区间追踪器：

`RecentWrittenTracker`：

- 记录已完成 log buffer 写入的 `LogRange`。
- 当连续区间从头部合并后，推进 `readyForWriteLsn`。
- 被 writer 线程读取。

`RecentClosedTracker`：

- 记录 dirty page 发布完成的 `LogRange`。
- 当连续区间从头部合并后，推进 `closedLsn`。
- 被 checkpoint coordinator 读取。

状态不变量：

`currentLsn >= readyForWriteLsn >= writeLsn >= flushedLsn`  
`readyForWriteLsn >= closedLsn` 在瞬时并发下可能成立，也可能相等；checkpoint 只能取更保守边界。  
`lastCheckpointLsn <= availableForCheckpointLsn <= flushedLsn`

MiniMySQL 可以比 MySQL 简化：

- 单 writer 模式下仍保留 tracker 接口，方便后续扩展。
- 单线程测试可用 `SynchronousRedoWriter`，但公开 API 不变化。

### 5.7 RedoFileRepository

`RedoFileRepository` 管理 redo 文件集合：

- 创建或发现 redo 目录。
- 维护 active file 与 spare file。
- 把 `Lsn` 映射到 `RedoFileId + offset`。
- 写入 log block header/trailer 和数据。
- 文件满时轮转到下一个文件。
- 根据 checkpoint 逻辑回收旧区间。
- 支持 redo capacity 动态调整的简化策略。

推荐文件布局：

| 文件 | 用途 |
| --- | --- |
| `redo-control` | magic、format version、checkpoint label 副本、capacity、checksum |
| `redo-000001.log` | redo data block |
| `redo-000002.log` | redo data block |
| `redo-N.log.tmp` | spare 或 resize 过程中的临时文件 |

与 MySQL 的差异：

- 不强制使用 `#innodb_redo/#ib_redoN` 命名，但文档和配置保留映射关系。
- 默认文件数可配置为 8 或 32。教学实现建议 8，压力测试建议 32。
- 支持固定容量优先；动态 resize 可作为后续增强。

### 5.8 LogBlock

Redo 文件按 log block 写入，建议 block size 为 512 bytes：

- `blockNo`
- `startLsn`
- `dataLength`
- `firstRecordOffset`
- `checkpointMarker`
- `checksum`
- `payload`

写入规则：

- record 可跨 block。
- block header/trailer 用于恢复扫描时检测 torn log write。
- 不完整 block 后面的 redo 不可用于恢复。
- flush 边界可以落在 block 内，但 recovery 只承认校验通过的完整 record。

### 5.9 CheckpointCoordinator

`CheckpointCoordinator` 负责 fuzzy checkpoint：

- 周期性读取 Buffer Pool dirty list 的最小 `oldestModificationLsn`。
- 读取 Redo 的 `closedLsn` 和 `flushedLsn`。
- 计算 `availableForCheckpointLsn`。
- 根据 checkpoint age 触发 flush pressure。
- 在安全时写 checkpoint label。
- 持久化 `lastCheckpointLsn`。

安全 checkpoint LSN：

`min(buf.oldestDirtyLsnOrCurrent, redo.closedLsn, redo.flushedLsn)`

如果没有 dirty page：

`availableForCheckpointLsn` 可以推进到 `redo.flushedLsn`。

checkpoint label 写入内容：

- `checkpointLsn`
- `currentLsnAtCheckpoint`
- `redoFormatVersion`
- `fileId + offset`
- `checksum`
- `createdAt`

### 5.10 RedoRecoveryReader

`RedoRecoveryReader` 只在启动恢复阶段使用：

- 读取 control file 和 redo file header。
- 找到最新有效 checkpoint label。
- 从 `checkpointLsn` 对应位置开始扫描。
- 校验 log block 和 record checksum。
- 解析 redo record。
- 按 record group 或 page id 输出给 `RedoApplyDispatcher`。
- 遇到不完整尾部时停止，并返回最后可恢复 LSN。

恢复输出：

- `recoveredToLsn`
- `appliedRecordCount`
- `skippedRecordCount`
- `corruptBlock`
- `tablespaceDiscoveryRequests`
- `pendingTransactionStates`

## 6. Redo Record 编码

编码目标：

- 可顺序扫描。
- 可跳过未知 record 类型。
- 可检测部分写和 payload 损坏。
- 可按 `PageId` 找到恢复目标。
- 可在 pageLSN 判断下幂等应用。

通用 record header：

| 字段 | 含义 |
| --- | --- |
| `type` | record 类型 |
| `flags` | 压缩、跨页、temporary、logical hint |
| `spaceId` | tablespace id |
| `pageNo` | page no |
| `pageType` | page type |
| `payloadLength` | payload 长度 |
| `startLsnDelta` | 相对 batch start 的偏移 |
| `checksum` | header + payload checksum |

payload 策略：

- 小范围页内修改：`offset + length + afterImageBytes`。
- 多段修改：`segmentCount + [offset, length, bytes]`。
- page init：`pageType + pageSize + initializedHeaderFields`。
- metadata update：优先保存字段级别变化，而不是完整页镜像。
- large payload：允许压缩，但压缩策略由 `RedoPayloadCompressionStrategy` 决定。

异常策略：

- payload 长度越界：恢复阶段停止扫描当前尾部或进入 corruption handler。
- 未知 type：如果 header 标记可跳过，则跳过；否则恢复失败。
- checksum 错误：若位于最后一个不完整 block，可停止；若位于 checkpoint 后中间位置，恢复失败。

## 7. MTR Commit 与 Redo 协作

MTR commit 是 Redo 的主要写入入口。

提交顺序：

1. MTR 已持有被修改页的 X latch，并已完成物理修改。
2. `RedoRecordCollector` 冻结为 `MtrRedoBatch`。
3. `RedoLogManager.append(batch, policy)` 预留连续 LSN 区间。
4. Redo 编码并写入 log buffer。
5. Redo 更新 `readyForWriteLsn`。
6. Redo 返回 `CommitLogResult(startLsn, endLsn, durableRequirement)`。
7. MTR 通知 Buffer Pool：涉及页从 `DIRTY_PENDING` 发布为 `DIRTY`，设置 pageLSN 和 oldest/newest modification LSN。
8. Buffer Pool 完成 dirty page 发布后，MTR 调用 `redo.close(range)` 或由 `DirtyPageMarker` 触发 close 事件。
9. 根据 durability policy，MTR 或事务提交线程等待 write/flush。
10. MTR 按 memo LIFO 释放 latch 和 buffer fix。

关键约束：

- 在 dirty page 发布完成前，checkpoint 不能越过该 MTR 的 redo range。
- 数据页刷盘前必须满足 `pageLSN <= flushedToDiskLsn`。
- 如果 redo append 成功但 dirty 发布失败，MTR 必须进入不可恢复错误路径，不能继续释放成 clean。
- 如果 redo append 失败，MTR 必须回滚未发布的内存页修改，并释放 latch。

空 MTR：

- 没有 redo record 时不分配 LSN。
- 只读 MTR 不触发 dirty marker。
- temporary no-redo MTR 只能修改 temporary tablespace 页。

## 8. Durability Policy

本设计把 MySQL 的提交刷盘策略抽象为 `DurabilityPolicy`：

| 策略 | 行为 | 适用场景 |
| --- | --- | --- |
| `FLUSH_ON_COMMIT` | commit 等待 redo fsync 到磁盘 | 默认强持久性 |
| `WRITE_ON_COMMIT` | commit 等待 redo 写到 OS/page cache，不等待 fsync | 延迟较低但宕机可能丢最近事务 |
| `BACKGROUND_FLUSH` | commit 只写 log buffer，由后台写/刷 | 测试或低可靠模式 |
| `SYNC_EVERY_N_MS` | 后台周期 fsync，commit 可选等待 write | 类似折中配置 |

事务提交建议：

- 默认使用 `FLUSH_ON_COMMIT`，对应强 ACID。
- 单元测试可使用 `SynchronousRedoWriter` 强制确定性。
- benchmark 可使用 `WRITE_ON_COMMIT` 或 `BACKGROUND_FLUSH`，但文档和指标必须明确风险。

与事务的关系：

- `TransactionManager.commit()` 决定是否等待 durable。
- Redo 模块只提供 `waitWritten` 和 `waitFlushed`，不决定事务是否对外提交成功。
- commit redo record 写入后，锁释放必须遵守事务模块的提交顺序策略。

## 9. Writer、Flusher 与 Notifier

线程模型：

- `LogWriter`：把 log buffer 中 `readyForWriteLsn` 之前的 bytes 写到 redo file。
- `LogFlusher`：对已写入文件的 redo 执行 fsync，推进 `flushedLsn`。
- `WriteNotifier`：唤醒等待 `writtenLsn` 的用户线程。
- `FlushNotifier`：唤醒等待 `flushedLsn` 的用户线程。
- `CheckpointWorker`：周期计算 checkpoint，并在压力下请求 FlushCoordinator。

简化实现：

- 初期可把 writer 和 flusher 合并为 `SingleRedoIoWorker`。
- API 保留 `writtenLsn` 与 `flushedLsn` 的区别。
- notifier 可用 `Condition`、`CompletableFuture` 或分片 wait slot。

等待设计：

- 等待目标是 LSN，不是具体线程。
- 用户线程先短暂自旋，再 park，避免高并发下频繁上下文切换。
- wait slot 根据 LSN hash 分片，减少全局锁争用。
- IO 失败后，所有等待者收到明确异常。

后台线程状态：

状态图见 [redo-state.mmd](diagrams/redo-state.mmd)。

状态包括：

- `CREATED`
- `INITIALIZING`
- `RECOVERING`
- `RUNNING`
- `CHECKPOINTING`
- `RESIZING`
- `SHUTTING_DOWN`
- `CLOSED`
- `FAILED`

## 10. Checkpoint 与容量治理

Redo 文件是循环可复用资源。checkpoint 太慢会导致日志空间耗尽；checkpoint 太激进会导致大量不必要刷脏。

关键指标：

- `checkpointAge = currentLsn - lastCheckpointLsn`
- `asyncFlushAge = capacity * 0.70`
- `syncFlushAge = capacity * 0.85`
- `hardLimitAge = capacity * 0.95`
- `availableForCheckpointLsn`
- `oldestDirtyLsn`

治理策略：

| 区间 | 行为 |
| --- | --- |
| `< asyncFlushAge` | 正常后台 checkpoint |
| `>= asyncFlushAge` | 提高 page cleaner 目标，优先 flush list |
| `>= syncFlushAge` | 前台 MTR commit 可能等待 checkpoint 进展 |
| `>= hardLimitAge` | 暂停新写入，强制 flush/checkpoint |

checkpoint 流程：

1. `CheckpointWorker` 读取 `currentLsn`、`flushedLsn`、`closedLsn`。
2. 从 Buffer Pool 查询 `oldestDirtyLsn`。
3. 计算安全 checkpoint LSN。
4. 如果安全 LSN 大于 `lastCheckpointLsn`，写 checkpoint label。
5. 通知 `RedoFileRepository` 逻辑释放旧 redo 区间。
6. 如果 checkpoint age 过大，请求 `FlushCoordinator.flushList(targetLsn)`。

禁止行为：

- 不允许 checkpoint 越过未关闭的 MTR redo range。
- 不允许 checkpoint 只看 redo flushed LSN，而忽略 dirty page。
- 不允许删除或覆盖 recovery 仍可能需要的 redo 文件。

## 11. Flush 与 Doublewrite 协作

Redo 模块不执行数据页写盘，但为 flush 提供边界。

FlushCoordinator 刷页前：

1. 从 Buffer Pool 选择 dirty frame。
2. 读取 page 的 `newestModificationLsn` 或 `pageLsn`。
3. 调用 `redo.waitFlushed(pageLsn)` 或确认 `flushedToDiskLsn >= pageLsn`。
4. 按 Buffer Pool 设计复制 page image。
5. 通过 doublewrite 策略写副本。
6. 写 tablespace data file。
7. 成功后回调 Buffer Pool 标记 clean 或推进 flushed page LSN。
8. CheckpointWorker 看到 oldest dirty LSN 推进后再写 checkpoint。

WAL 不变量：

`redo.flushedToDiskLsn >= page.pageLsn` 必须先于数据页落盘成立。

doublewrite 关系：

- Redo 解决“丢失未刷数据页修改后如何重放”。
- Doublewrite 解决“数据页本身 torn write”。
- recovery 阶段先用 doublewrite 修复 torn page，再根据 pageLSN 应用 redo。

## 12. Recovery 设计

数据流程见 [redo-data-flow.mmd](diagrams/redo-data-flow.mmd)。

启动恢复顺序：

1. `RecoveryService` 发现 tablespace 和 redo 文件。
2. `RedoRecoveryReader` 读取最新有效 checkpoint label。
3. 从 checkpoint LSN 对应位置向前扫描 redo block。
4. 校验 block checksum、record header、payload checksum。
5. 解析 record group，恢复到最后一个完整 record。
6. `RedoApplyDispatcher` 按 `RedoRecordType` 分发给 handler。
7. handler 通过 `PageStore` 或 recovery 专用 Buffer Pool 读取目标页。
8. 如果 `pageLSN >= record.endLsn`，跳过。
9. 如果 `pageLSN < record.endLsn`，应用 redo 并更新 pageLSN。
10. 所有 redo 应用完成后，事务恢复模块根据 undo 和事务系统页回滚未提交事务。
11. 启动 purge 和正常后台 checkpoint。

恢复幂等：

- page redo handler 必须能重复调用。
- 每次应用后更新 pageLSN。
- 同一 record 不能依赖 Java 内存态。
- 缺失 tablespace 时根据恢复模式决定失败、跳过或延迟处理。

恢复模式：

| 模式 | 行为 |
| --- | --- |
| `NORMAL` | 任意必须 tablespace 缺失或 redo 损坏即失败 |
| `READ_ONLY_VALIDATE` | 只扫描和校验 redo，不写数据页 |
| `FORCE_SKIP_CORRUPT_TABLESPACE` | 记录错误并跳过指定 tablespace，用于导出可读数据 |

## 13. Redo Apply Handler

`RedoApplyDispatcher` 是恢复阶段的责任链入口。

handler 分组：

- `PageInitRedoHandler`
- `PageBytesRedoHandler`
- `SpaceHeaderRedoHandler`
- `XdesRedoHandler`
- `SegmentInodeRedoHandler`
- `UndoPageRedoHandler`
- `TransactionSystemRedoHandler`
- `BtreePageRedoHandler`
- `TablespaceLifecycleRedoHandler`

handler 接口语义：

- 判断是否支持 record type。
- 根据 `PageId` 加载页。
- 校验 page type 与 record page type。
- 校验 pageLSN。
- 应用 payload。
- 更新 pageLSN 和 checksum。
- 返回 `ApplyResult`。

ApplyResult：

- `APPLIED`
- `SKIPPED_BY_PAGE_LSN`
- `SKIPPED_TEMPORARY`
- `DEFERRED_TABLESPACE_MISSING`
- `FAILED_CORRUPT_PAGE`
- `FAILED_UNSUPPORTED_TYPE`

解耦规则：

- Redo handler 可以调用 page cursor 的 recovery-safe API。
- Redo handler 不能调用事务可见性、锁管理或 SQL 执行器。
- B+Tree handler 只做物理页修复，不执行逻辑查找和约束检查。

## 14. 与 Transaction/MVCC 的关系

事务模块依赖 Redo，但 Redo 不理解事务隔离。

事务写路径：

1. `Transaction` 开启或复用 MTR。
2. 记录层生成 undo record。
3. undo page 修改产生 redo。
4. 聚簇索引记录修改产生 redo。
5. MTR commit 返回 `LogRange`。
6. 事务 commit 时写事务状态 redo 或 undo header 状态 redo。
7. 根据 durability policy 等待 redo durable。
8. 事务释放锁。

恢复路径：

- redo 先把 undo page、事务系统页、聚簇索引页恢复到 crash 前物理状态。
- 事务恢复再识别 active/prepared/committed 状态。
- active 事务通过 undo rollback。
- update undo 进入 history list 后由 purge 清理。

约束：

- 普通 undo page 的修改必须 redo-logged。
- temporary undo 可以 no-redo，但不能参与 crash recovery。
- commit durable 由事务模块决定，Redo 只提供 LSN 等待能力。

## 15. 与 Buffer Pool 的关系

Buffer Pool 依赖 Redo 的 durable LSN 和 MTR commit 事件。

接口：

- `RedoLogManager.flushedToDiskLsn()`
- `RedoLogManager.waitFlushed(Lsn)`
- `RedoLogManager.currentLsn()`
- `CheckpointService.availableForCheckpointLsn()`
- `MtrCommitListener.onRedoAppended(LogRange, modifiedPages)`
- `MtrCommitListener.onRedoClosed(LogRange)`

Buffer Pool 负责：

- dirty page 进入 flush list。
- 维护 `oldestModificationLsn`。
- 维护 `newestModificationLsn`。
- 提供 `oldestDirtyLsn()` 给 checkpoint。
- 保证 `DIRTY_PENDING` 不被 flush。

Redo 负责：

- 分配 LSN。
- 写入和刷盘 redo。
- 告诉 flush 是否可以写某个 pageLSN。
- 计算 checkpoint pressure。

禁止耦合：

- Redo 不遍历 LRU。
- Redo 不持有 page latch 后等待 IO。
- Buffer Pool 不解析 redo record。

## 16. 与 Disk/FIL 的关系

Redo 文件由 `storage.redo.file` 管理，数据文件由 `storage.fil` 管理。两者必须分开。

Redo 文件 IO：

- 顺序追加为主。
- block 校验。
- 文件轮转。
- fsync。
- checkpoint label 写入。

Data file IO：

- 由 `storage.fil.PageStore` 管理。
- 随机读写 page。
- page checksum。
- doublewrite。

恢复时：

- RedoRecoveryReader 从 redo file 读取 record。
- RedoApplyHandler 通过 PageStore 读取和写回 page。
- FIL 不反向依赖 redo parser。

## 17. 公共 API 设计

### 17.1 RedoLogManager

对 MTR：

- `append(MtrRedoBatch batch, DurabilityPolicy policy)`
- `close(LogRange range)`
- `currentLsn()`

对事务：

- `waitWritten(Lsn lsn)`
- `waitFlushed(Lsn lsn)`
- `flushedToDiskLsn()`

对 flush：

- `isDurable(Lsn pageLsn)`
- `waitUntilDurable(Lsn pageLsn)`
- `checkpointAge()`

对 recovery：

- `openRecoveryReader()`
- `recoverToLastCompleteRecord(RecoveryMode mode)`

### 17.2 CheckpointService

- `requestCheckpoint(CheckpointReason reason)`
- `lastCheckpointLsn()`
- `availableForCheckpointLsn()`
- `checkpointAge()`
- `checkpointPressure()`
- `registerDirtyPageProvider(DirtyPageLsnProvider provider)`

### 17.3 RedoRecordFactory

- `pageInit(PageId, PageType, PageHeaderImage)`
- `pageBytes(PageId, PageType, PageDelta)`
- `spaceHeaderUpdate(PageId, SpaceHeaderDelta)`
- `xdesUpdate(PageId, XdesDelta)`
- `segmentInodeUpdate(PageId, InodeDelta)`
- `undoRecordInsert(PageId, UndoRecordBytes)`
- `trxStateUpdate(PageId, TrxStateDelta)`

Factory 只创建 record，不执行写入。

### 17.4 RedoApplyRegistry

- `register(RedoRecordType, RedoApplyHandler)`
- `handlerFor(RedoRecordType)`
- `apply(RedoRecord, RedoApplyContext)`

Registry 只在恢复阶段使用。

## 18. 设计模式

- Facade：`RedoLogManager` 隐藏 buffer、writer、file、checkpoint 细节。
- Value Object：`Lsn`、`LogRange`、`RedoFileId`、`RedoBlockNo`。
- Command：`RedoRecord` 同时支持编码和恢复应用。
- Strategy：`DurabilityPolicy`、`RedoCapacityPolicy`、`RedoFileNamingStrategy`、`ChecksumStrategy`。
- Repository：`RedoFileRepository` 管理 redo 文件集合。
- Ring Buffer：`RedoLogBuffer` 支持顺序预留和并发写入。
- Reservation：`RedoReservation` 表达 LSN 区间占用。
- Observer：writer/flusher/notifier/checkpoint 监听 LSN 推进事件。
- Background Worker：`LogWriter`、`LogFlusher`、`CheckpointWorker`。
- Chain of Responsibility：`RedoApplyDispatcher` 分发恢复 record。
- State：`RedoLogSystemState` 管理初始化、恢复、运行、关闭和失败。
- Template Method：`RedoWriteTemplate` 固定 `reserve -> encode -> write buffer -> publish -> wait`。
- Mediator：`CheckpointCoordinator` 调和 redo capacity、dirty page 和 flush pressure。

## 19. 高内聚与低耦合约束

高内聚：

- LSN 分配只在 `storage.redo.lsn`。
- redo 文件格式只在 `storage.redo.file`。
- record 编码只在 `storage.redo.record`。
- writer/flusher 等待只在 `storage.redo.writer`。
- checkpoint 计算只在 `storage.redo.checkpoint`。
- recovery 扫描只在 `storage.redo.recovery`。

低耦合：

- MTR 只交出 `MtrRedoBatch`，不关心 redo 文件。
- Buffer Pool 只关心 LSN 边界，不关心 redo bytes。
- Transaction 只等待 durable LSN，不关心 writer 线程。
- Disk/FIL 只提供 page IO，不关心 redo capacity。
- Recovery 通过 handler registry 扩展 record 类型。

禁止跨层：

`redo.file -> buf` 禁止  
`redo.record -> trx.lock` 禁止  
`redo.writer -> btree` 禁止  
`redo.checkpoint -> page body` 禁止  
`trx -> redo.file` 禁止  
`buf -> redo.buffer` 禁止

## 20. 数据流程

### 20.1 MTR 写入 Redo

1. Page operation 在 MTR 中修改 page。
2. `RedoRecordCollector` 收集 record。
3. MTR commit 冻结 batch。
4. Redo 预留 LSN 区间。
5. Redo 编码 record。
6. Redo 写 log buffer。
7. RecentWritten 推进 `readyForWriteLsn`。
8. Buffer Pool 发布 dirty page。
9. RecentClosed 推进 `closedLsn`。
10. Writer 写 redo file。
11. Flusher fsync。
12. 等待者被 notifier 唤醒。

### 20.2 Transaction Commit

1. 事务内所有数据页修改已经通过各自 MTR 写入 redo。
2. commit MTR 写事务状态 redo。
3. Redo 返回 commit record end LSN。
4. `TransactionManager` 根据策略等待 write 或 flush。
5. durable 条件满足后事务进入 committed。
6. 释放锁。

### 20.3 Dirty Page Flush

1. FlushCoordinator 选择 dirty page。
2. 读取 pageLSN。
3. 等待 `redo.flushedToDiskLsn >= pageLSN`。
4. doublewrite 写副本。
5. data file 写 page。
6. Buffer Pool 标记 clean。
7. CheckpointWorker 看到 oldest dirty LSN 推进。

### 20.4 Checkpoint

1. 周期或压力触发 checkpoint。
2. 读取 current/flushed/closed LSN。
3. 读取 oldest dirty LSN。
4. 计算 available checkpoint LSN。
5. 写 checkpoint label。
6. 释放 redo 文件旧区间。
7. 如空间仍不足，请求更多 flush。

### 20.5 Crash Recovery

1. 读取 redo control 和 checkpoint label。
2. 从 checkpoint LSN 扫描 log block。
3. 解析完整 redo record。
4. 按 page id 加载 page。
5. 用 pageLSN 判断跳过或应用。
6. 更新 pageLSN。
7. redo replay 完成后交给事务恢复 rollback active transaction。

## 21. 并发与锁顺序

Redo 并发等待状态图见 [redo-lock-wait-state.mmd](diagrams/redo-lock-wait-state.mmd)。

推荐锁顺序：

1. `RedoReservationLock` 或 LSN allocator CAS。
2. `RedoLogBufferSegmentLock`
3. `RecentWrittenLock`
4. `RecentClosedLock`
5. `RedoFileWriteLock`
6. `RedoFileFlushLock`
7. `CheckpointLock`

禁止：

- 持有 page latch 时等待 checkpoint 长时间推进。
- 持有 redo file lock 时请求 Buffer Pool flush。
- 持有 Buffer Pool flush list lock 时写 redo file。
- writer 持有 file lock 后回调 MTR。
- recovery apply 持有 page latch 后等待用户事务锁。

等待原则：

- 前台线程等待 LSN，不等待具体后台线程对象。
- checkpoint pressure 可阻塞新的 redo reservation，但不能破坏已预留区间。
- IO 错误必须广播给所有等待者。

### 21.1 锁状态与持有变化

Redo 模块的并发核心不是数据库事务锁，而是 LSN 区间所有权、log buffer 写入权、redo file 写入权和 durable 等待条件。

| 状态 | 持有者 | 持有资源 | 进入条件 | 退出条件 |
| --- | --- | --- | --- | --- |
| `IDLE` | 无 | 无 redo reservation | MTR 尚未提交 redo | reserve LSN |
| `RESERVING` | 前台 MTR 线程 | `RedoReservationLock` 或 allocator CAS | 申请连续 LSN range | 获得 `LogRange` |
| `RESERVED` | MTR redo batch | LSN range 所有权 | LSN 预留成功 | 开始写 log buffer |
| `WRITING_BUFFER` | 前台 MTR 线程 | 目标 `RedoLogBufferSegmentLock` | 编码 redo record | 标记 recent written |
| `RECENT_WRITTEN_PENDING` | recent written tracker | 已写 buffer 但可能乱序的区间 | log buffer 写入完成 | 区间连续后推进 `readyForWriteLsn` |
| `RECENT_CLOSED_PENDING` | recent closed tracker | MTR close 但可能乱序的区间 | MTR publish dirty 完成 | 区间连续后推进 checkpoint 候选 |
| `FILE_WRITING` | log writer | `RedoFileWriteLock` | `readyForWriteLsn` 超过 `writeLsn` | OS/page cache 写入完成 |
| `FILE_FLUSHING` | log flusher | `RedoFileFlushLock` | flush policy 要求 durable | fsync 完成并推进 `flushedLsn` |
| `WAITING_WRITTEN` | 前台或后台等待者 | wait slot，不持有 file lock | 调用 `waitWritten(lsn)` | `writeLsn >= lsn` 或 IO error |
| `WAITING_FLUSHED` | 事务提交或 flush 等待者 | wait slot，不持有 file lock | 调用 `waitFlushed(lsn)` | `flushedLsn >= lsn` 或 IO error |
| `CHECKPOINT_ADVANCING` | checkpoint coordinator | `CheckpointLock` | dirty page 和 redo 边界允许推进 | 写出 checkpoint label |
| `ERROR_BROADCAST` | redo error handler | 全部 wait slot 通知权 | write/fsync/recovery 错误 | 等待者收到异常 |

锁持有变化规则：

- `reserve`：前台线程只短持有 reservation 锁或 CAS 循环，不能等待 IO。
- `write buffer`：MTR 线程只持有目标 buffer segment lock，不持有 redo file lock。
- `publish written`：写完 log buffer 后更新 recent written，并唤醒 writer。
- `close MTR`：dirty page 发布后更新 recent closed，checkpoint 只能使用连续 closed 边界。
- `write file`：log writer 持有 redo file write lock，不回调 MTR、Buffer Pool 或事务模块。
- `flush file`：log flusher 持有 redo file flush lock，只推进 durable LSN 并唤醒等待者。
- `wait`：等待者只挂 wait slot，不持有 redo file lock、Buffer Pool list lock 或 page latch。
- `checkpoint`：checkpoint coordinator 读取 dirty 边界和 redo closed 边界，不能在持有 checkpoint lock 时等待 page latch。
- `error cleanup`：IO 错误广播给所有等待者，等待者由调用方决定 rollback、重试或进入 recovery。

## 22. 错误处理

错误类型：

- `RedoLogFullException`：checkpoint 无法及时释放空间。
- `RedoLogIoException`：redo file write/fsync 失败。
- `RedoCorruptionException`：恢复时发现不可接受的损坏。
- `RedoUnsupportedRecordException`：无法识别必须应用的 record。
- `RedoCapacityResizeException`：容量调整失败。
- `RedoCheckpointException`：checkpoint label 写入失败。
- `RedoIllegalNoRedoException`：普通 tablespace 使用 no-redo。

处理策略：

- redo append 失败：MTR 不得发布 dirty page。
- redo write/fsync 失败：系统进入 `FAILED`，阻止新写入，唤醒等待者。
- checkpoint 失败：保持旧 checkpoint，有空间时继续运行；空间紧张时阻止新写入。
- recovery 中间 record 损坏：正常模式失败。
- recovery 尾部不完整：停止于最后完整 record。

## 23. 测试设计

单元测试：

- LSN 比较、距离和 overflow 防护。
- `LogRange` 连续性。
- redo record 编码/解码。
- checksum 错误检测。
- log buffer 预留、并发写入、wrap-around。
- recent written 合并乱序区间。
- recent closed 合并乱序区间。
- writer 推进 `writeLsn`。
- flusher 推进 `flushedLsn`。
- waitWritten/waitFlushed 唤醒。
- checkpoint LSN 计算。
- redo capacity pressure。
- unknown record skip/fail 策略。

集成测试：

- MTR commit 后 pageLSN 与 redo range 一致。
- redo durable 前数据页禁止 flush。
- dirty page flush 后 checkpoint 可推进。
- crash 后从 checkpoint 扫描 redo 并幂等重放。
- redo record 重放两次不改变结果。
- torn redo block 尾部停止扫描。
- doublewrite 修复 page 后 redo 继续应用。
- active transaction crash 后 redo 先恢复 undo，再由事务模块 rollback。
- temporary no-redo page 不进入普通恢复。

压力与性质测试：

- 随机并发 MTR commit 后 LSN 无重叠、无空洞。
- 随机 writer/flusher 延迟下 wait 结果正确。
- 随机 checkpoint 和 flush 交错下 `lastCheckpointLsn` 不越界。
- 随机 crash point 下恢复后 pageLSN 不小于已应用 record LSN。

## 24. 后续实现顺序

1. `Lsn`、`LogRange`、redo 配置和值对象。
2. `RedoRecord`、基础 `PAGE_INIT/PAGE_BYTES` 编码。
3. `RedoLogBuffer`、reservation、recent written。
4. 同步 `RedoLogManager.append()`。
5. `LogWriter` 和 `LogFlusher`。
6. `waitWritten/waitFlushed`。
7. 与 MTR commit 集成。
8. 与 Buffer Pool dirty marker 集成。
9. `CheckpointCoordinator`。
10. `RedoFileRepository` 文件轮转和容量压力。
11. `RedoRecoveryReader` 扫描和 record 解码。
12. `RedoApplyDispatcher` 与 page handler。
13. crash recovery 集成。
14. 指标和诊断快照。

## 25. 十五轮自检记录

| 序号 | 检查项 | 结论 |
| --- | --- | --- |
| 1 | 目标边界 | 已明确 Redo 只负责 WAL、LSN、写入、刷盘、checkpoint、recovery replay，不接管事务和页缓存内部 |
| 2 | 高内聚 | 已按 api/lsn/record/buffer/file/writer/checkpoint/recovery/metric 拆包 |
| 3 | 低耦合 | 已明确 MTR、Buffer Pool、Flush、Transaction、Recovery 通过接口交互，不直接访问 redo 内部结构 |
| 4 | MySQL 8.0 参考 | 已覆盖 redo log、log buffer、writer/flusher、fuzzy checkpoint、8.0.30 redo capacity、recovery |
| 5 | Java 面向对象 | 已定义值对象、聚合、门面、命令、仓储、策略、状态和后台 worker |
| 6 | 设计模式 | 已给出 Facade、Command、Strategy、Repository、Ring Buffer、Observer、State、责任链等模式 |
| 7 | WAL 约束 | 已明确数据页刷盘前必须满足 redo durable |
| 8 | MTR 协作 | 已定义 `MtrRedoBatch`、LSN 分配、dirty 发布、redo close 顺序 |
| 9 | Buffer Pool 协作 | 已定义 pageLSN、oldestDirtyLsn、dirty flush 与 checkpoint 的接口 |
| 10 | 事务协作 | 已说明 commit redo、undo redo、durability wait 与 rollback recovery 顺序 |
| 11 | Checkpoint | 已设计 fuzzy checkpoint、capacity pressure 和 safe checkpoint LSN 计算 |
| 12 | Recovery | 已设计 checkpoint 扫描、record 解析、pageLSN 幂等应用和 handler 分发 |
| 13 | 并发 | 已覆盖并发 LSN reservation、recent written/closed、writer/flusher/notifier 和锁顺序 |
| 14 | 测试 | 已包含单元、集成、压力、性质测试和 crash point 测试 |
| 15 | 图表 | 四个 Mermaid 图与正文术语一致，并已通过 `npx @mermaid-js/mermaid-cli` 实际渲染 |

## 26. 参考链接

- MySQL 8.0 Reference Manual - Redo Log: https://dev.mysql.com/doc/refman/8.0/en/innodb-redo-log.html
- MySQL 8.0 Reference Manual - Optimizing InnoDB Redo Logging: https://dev.mysql.com/doc/refman/8.0/en/optimizing-innodb-logging.html
- MySQL 8.0 Reference Manual - InnoDB Checkpoints: https://dev.mysql.com/doc/refman/8.0/en/innodb-checkpoints.html
- MySQL 8.0 Reference Manual - InnoDB Recovery: https://dev.mysql.com/doc/refman/8.0/en/innodb-recovery.html
- MySQL 8.0.46 Source - `log0buf.cc`: https://dev.mysql.com/doc/dev/mysql-server/8.0.46/log0buf_8cc.html
- MySQL 8.0.46 Source - `log0write.cc`: https://dev.mysql.com/doc/dev/mysql-server/8.0.46/log0write_8cc.html
- MySQL 8.0.46 Source - `log0chkp.cc`: https://dev.mysql.com/doc/dev/mysql-server/8.0.46/log0chkp_8cc.html
- MySQL 8.0.46 Source - `log0recv.cc`: https://dev.mysql.com/doc/dev/mysql-server/8.0.46/log0recv_8cc.html
- MySQL 8.0.46 Source - `mtr0mtr.h`: https://dev.mysql.com/doc/dev/mysql-server/8.0.46/mtr0mtr_8h_source.html
