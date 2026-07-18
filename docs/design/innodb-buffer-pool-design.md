# MiniMySQL InnoDB 风格 Buffer Pool 模块设计

版本：2026-06-04  
实现语言：Java  
参考基线：MySQL 8.0.46 InnoDB 官方手册与源码文档  
关联设计：[innodb-crash-recovery-design.md](innodb-crash-recovery-design.md)、[innodb-flush-checkpoint-doublewrite-design.md](innodb-flush-checkpoint-doublewrite-design.md)、[innodb-disk-manager-design.md](innodb-disk-manager-design.md)、[innodb-record-design.md](innodb-record-design.md)、[innodb-undo-log-purge-design.md](innodb-undo-log-purge-design.md)、[mysql-lock-observability-deadlock-design.md](mysql-lock-observability-deadlock-design.md)

## 1. 目标与边界

本设计面向 MiniMySQL 存储引擎的 `storage.buf` 模块。磁盘管理模块已经定义了 `Tablespace -> Segment -> Extent -> Page` 的空间管理关系；Buffer Pool 负责把这些磁盘页安全、高效地缓存到内存，并为 B+Tree、记录层、MTR、flush、recovery 提供受控的页访问能力。

设计目标：

- 高内聚：页缓存命中、读入、frame 生命周期、page latch、buf fix、LRU、dirty tracking、flush list、read-ahead、warmup 都收敛在 `storage.buf` 内部。
- 低耦合：上层只能通过 `BufferPool`、`PageHandle`、`PageCursor` 获取页，不直接接触 `BufferFrame`、链表节点、page hash 或 IO 状态。
- InnoDB 风格：参考 MySQL 8.0 的 buffer pool instances、page hash、free list、LRU old/new 子链、midpoint insertion、buf fix、io fix、flush list、read-ahead、buffer pool dump/load。
- Java 可落地：用门面、仓储、策略、状态对象、模板方法、观察者和 RAII 风格 page guard 表达核心机制。
- 可恢复：Buffer Pool 不决定 redo 语义，但必须保证 dirty page 的 `oldestLsn/newestLsn/pageLsn` 与 MTR commit、flush、checkpoint 的顺序一致。

非目标：

- 不实现完整 SQL 层、事务隔离、MVCC、change buffer、adaptive hash index 和压缩页。
- 不追求与 InnoDB C++ 内部结构二进制兼容。
- 不生成 Java 源码。本文件只定义设计、关系和数据流。

## 2. MySQL 8.0 参考依据

本设计参考以下 MySQL 8.0 行为，文末列出官方链接。

- InnoDB Buffer Pool 是主内存区域，用于缓存表和索引页；Buffer Pool 按 page 管理，并使用 LRU 变体淘汰很少访问的数据。
- InnoDB LRU 使用 midpoint insertion。新读入页默认插入 LRU 中点，也就是 old 子链头部；频繁访问页进入 new 子链头部。默认 old 子链占 3/8，对应 `innodb_old_blocks_pct=37`。
- `innodb_old_blocks_time` 默认 1000ms，用于避免全表扫描或 read-ahead 页在短时间内被立即提升为 hot page，降低扫描污染。
- 大 Buffer Pool 可划分为多个 buffer pool instance。每个 instance 独立维护 free list、LRU、flush list、hash 等结构，MySQL 8.0 后进一步拆分 list/hash 保护锁以降低争用。
- InnoDB 源码中 `buf_page_t` 记录 page id、page size、buf fix count、page state、io fix、LRU 节点、flush list 节点、old/new 标记、访问时间、dirty LSN 等元数据；`buf_block_t` 在其上挂载真实 frame、读写锁和页内容。
- 读页路径会设置 `io_fix` 并持有 frame X latch，IO 完成后由 handler 清除 `io_fix` 并释放 latch。本设计在 Java 中抽象为 `IoState` 与 `PageLoadFuture`。
- Flush list 按 dirty page 的 oldest modification LSN 管理。刷脏必须先保证对应 redo 已经 durable，再通过 doublewrite 和 tablespace data file 写出。
- Read-ahead 分为 linear read-ahead 与 random read-ahead。linear read-ahead 根据同一 extent 内顺序访问阈值触发，默认阈值 56；random read-ahead 可在同一 extent 已有足够连续页位于 Buffer Pool 时预取剩余页。
- Buffer Pool dump/load 只保存 `SpaceId + PageNo` 这类定位信息，后台线程在启动或在线操作时异步重新加载页，避免重启后长时间 warmup。

## 3. 总体架构

架构图见 [bufferpool-architecture.mmd](diagrams/bufferpool-architecture.mmd)。

模块分为八组：

1. `storage.buf.api`：对外门面，包括 `BufferPool`、`PageHandle`、`PageCursorFactory`。
2. `storage.buf.instance`：`BufferPoolInstance`、instance 路由、容量与分片内锁。
3. `storage.buf.frame`：`BufferFrame`、`FrameDescriptor`、`PageImage`、`PageLatch`、`BufferFix`。
4. `storage.buf.page_table`：`PageHashTable`，负责 `PageId -> BufferFrame` 查找与加载去重。
5. `storage.buf.lru`：free list、LRU old/new 子链、淘汰候选选择、scan-resistant 策略。
6. `storage.buf.dirty`：dirty marker、flush list、dirty page 统计、checkpoint 候选。
7. `storage.buf.io`：page read service、read-ahead、IO 状态、异步加载、校验失败处理。
8. `storage.buf.warmup`：buffer pool dump/load，保存和恢复热页定位信息。

核心原则：

- `storage.buf` 不理解 segment、extent、B+Tree 记录格式。它只认识 `PageId`、`PageType`、`PageImage`、`LatchMode`、`Lsn`。
- `storage.fsp` 与 `storage.btree` 只能拿到 `PageHandle` 或 `PageCursor`，不能持有裸 `BufferFrame`。
- `storage.record` 通过 `PageCursor` 解释页内记录、PageDirectory 和字段编码；Buffer Pool 不解析 record。
- `storage.fil` 只负责按 `PageId` 读写物理页，不参与 LRU、dirty、latch、page hash。
- `storage.mtr` 负责登记 `BUFFER_FIX` 和 page latch，统一释放顺序；Buffer Pool 不提交 redo。
- `storage.flush` 选择刷脏策略，但脏页集合和 pageLSN 元数据由 Buffer Pool 维护。

## 4. 包与职责

| 包 | 职责 | 主要依赖 | 设计模式 |
| --- | --- | --- | --- |
| `storage.buf.api` | Buffer Pool 门面、PageHandle、PageCursor 创建 | `domain`, `mtr` | Facade, RAII Guard |
| `storage.buf.instance` | instance 分片、容量、局部并发结构 | `domain`, `config` | Repository, Sharding |
| `storage.buf.frame` | frame 元数据、页镜像、latch、buf fix | `domain` | State, Guard |
| `storage.buf.page_table` | page hash、miss 合并、加载占位 | `frame` | Repository, Single Flight |
| `storage.buf.lru` | free list、old/new LRU、淘汰策略 | `frame`, `config` | Strategy, Policy |
| `storage.buf.dirty` | dirty 标记、flush list、dirty ratio | `frame`, `redo` | Observer, Repository |
| `storage.buf.io` | 同步/异步读页、校验、read-ahead | `fil`, `frame` | Template Method, Strategy |
| `storage.buf.warmup` | 热页 dump/load、加载节流 | `api`, `fil` | Snapshot, Background Worker |
| `storage.buf.metric` | 命中率、young/non-young、读写统计 | 无 | Observer |

推荐依赖方向：

`api -> instance -> page_table -> frame`  
`instance -> lru + dirty + io`  
`io -> fil`  
`dirty -> redo`  
`flush -> buf.api + buf.dirty + fil + redo`

禁止方向：

- `buf` 不能 import `storage.fsp`、`storage.btree`、`storage.record`、`storage.sql`。
- `frame` 不能依赖 `fil`、`redo`、`flush`。
- `lru` 不能读写 page body。
- `buf` 不能解析 record header、PageDirectory、字段编码或隐藏列。
- `dirty` 不能决定 redo record 内容。
- `io` 不能修改 B+Tree 或 segment 元数据。

## 5. 核心领域模型

类关系图见 [bufferpool-class-relation.mmd](diagrams/bufferpool-class-relation.mmd)。

### 5.1 BufferPool

`BufferPool` 是对外门面：

- `getPage(PageId, LatchMode, MiniTransaction)`
- `tryGetPage(PageId, LatchMode, MiniTransaction)`
- `createPage(PageId, PageType, MiniTransaction)`
- `prefetch(PageRange, ReadAheadReason)`
- `onMtrCommit(MtrCommitEvent)`
- `release(PageHandle)`
- `evictForCapacity(BufferPoolInstance)`
- `snapshotHotPages(DumpPolicy)`

职责：

- 根据 `PageId` 路由到 `BufferPoolInstance`。
- 对调用方隐藏 page hash、LRU、free list、IO 状态和 frame 复用。
- 把成功获得的 `PageHandle` 注册到 MTR memo stack。
- 为 flush 和 warmup 提供受限视图，而不是暴露内部链表。

### 5.2 BufferPoolInstance

`BufferPoolInstance` 是并发隔离单元：

- `instanceId`
- `capacityInFrames`
- `pageTable`
- `freeList`
- `lruList`
- `flushList`
- `readAheadTracker`
- `metrics`
- `instanceLocks`

分片策略：

- `instanceId = hash(PageId) % instanceCount`。
- 每个 instance 独立维护 free list、LRU、flush list、page hash、dirty count。
- MiniMySQL 默认 1 个 instance；当总 frame 数足够大时可启用多个 instance。
- instance 数量是配置项，不允许业务层依赖具体路由结果。

### 5.3 BufferFrame

`BufferFrame` 是页缓存的内部实体，不对上层暴露：

- `frameId`
- `pageId`
- `pageType`
- `pageImage`
- `state`
- `bufFixCount`
- `ioState`
- `latch`
- `accessTime`
- `oldFlag`
- `inPageHash`
- `inFreeList`
- `inLruList`
- `inFlushList`
- `oldestModificationLsn`
- `newestModificationLsn`
- `flushType`
- `freedPageClock`

约束：

- 只有 `PageHandle` 可以把 frame 暂时交给调用方。
- `pageImage` 不能在未持有 X latch 时写入。
- `bufFixCount > 0` 的 frame 不能被淘汰。
- `ioState != NONE` 的 frame 不能被淘汰，也不能被读路径当作可修改页返回。
- committed dirty frame 必须在 flush list 中，直到刷盘成功并调用 `setClean()`。

### 5.4 PageHandle 与 PageCursor

`PageHandle` 是 RAII 风格的页访问令牌：

- `pageId`
- `frameRef`
- `latchMode`
- `mtr`
- `released`
- `cursor()`
- `upgradeLatch()`
- `downgradeLatch()`
- `release()`

`PageCursor` 通过 `PageHandle` 创建：

- 读操作要求至少持有 S latch。
- 写操作要求持有 X latch，并通过 MTR 追加对应 redo 或把变更交给上层 page operation。
- cursor 不拥有 frame 生命周期；handle release 后 cursor 必须失效。

Java 中建议让 `PageHandle` 实现 `AutoCloseable`，但主要释放路径仍由 MTR memo stack 管理。手动 `close()` 只用于短生命周期读页或无 MTR 的后台任务。

### 5.5 PageHashTable

`PageHashTable` 负责 `PageId -> BufferFrame` 映射：

- 命中时返回已存在 frame，并增加 buf fix。
- miss 时注册 loading 占位，保证同一 page 的并发 miss 只发起一次物理读。
- IO 失败时移除占位，唤醒等待线程并返回明确异常。
- page 被 truncate/drop 后，通过 `TablespaceVersion` 或 `SpaceLifecycleClock` 识别 stale frame。

设计上使用 Single Flight 模式：

1. 第一个 miss 线程成为 loader。
2. 其它线程等待 `PageLoadFuture`。
3. loader 完成读盘和校验后发布 frame。
4. 等待线程重新检查 page id、space version、io state，再获取 latch。

### 5.6 Page Latch 与 Buffer Fix

两个概念必须分开：

- Page latch：保护页内容的并发读写，分为 `S`、`X`，可扩展 `SX`。
- Buffer fix：保护 frame 生命周期，表示当前线程正在使用该 frame，防止淘汰。

访问页的标准顺序：

1. 从 page hash 找到或加载 frame。
2. 增加 `bufFixCount`。
3. 获取 page latch。
4. 把 `BUFFER_FIX` 和 latch 依次压入 MTR memo。
5. 返回 `PageHandle`。

释放顺序：

1. 释放 page latch。
2. 减少 `bufFixCount`。
3. 根据访问结果更新 LRU 位置。
4. 如 frame 已被标记 freed/stale 且 fix count 归零，允许进入回收路径。

### 5.7 Frame 状态

状态图见 [bufferpool-frame-state.mmd](diagrams/bufferpool-frame-state.mmd)。

`BufferFrameState`：

- `FREE`：frame 不绑定任何 page，位于 free list。
- `LOADING`：page hash 中有占位或 frame 正在读盘。
- `CLEAN`：绑定 page，内容与磁盘一致，位于 LRU。
- `DIRTY_PENDING`：page image 已被持有 X latch 的活跃 MTR 修改，但 redo LSN 尚未分配，不在 flush list 中，不能刷盘；MTR 发布 pageLSN 后才转为 DIRTY。
- `DIRTY`：绑定 page，存在未刷盘修改，位于 LRU 和 flush list。
- `FLUSHING`：dirty page 正在写出，仍可被读；写路径需遵守 page latch 和 flush observer。
- `EVICTING`：淘汰候选已摘出 LRU/hash，等待最后检查和复用。
- `STALE`：所属 tablespace 被 drop/truncate/discard，不能再返回给普通读路径。

状态转换只能由 `FrameStateMachine` 执行，避免多个服务直接修改布尔字段造成不一致。

## 6. LRU 与淘汰策略

### 6.1 Midpoint LRU

`LruList` 按 InnoDB 思路划分：

- new sublist：热页，位于 LRU 头部。
- old sublist：冷页和新读入页，位于 LRU 尾部。
- midpoint：new 尾与 old 头之间的边界。

默认参数：

- `oldBlocksPct = 37`，近似 3/8。
- `oldBlocksTime = 1000ms`。
- `youngDistanceThreshold`：命中新子链但离头部太近时不移动，避免热点页频繁改链。

读入页处理：

1. 用户 miss 读入页后，先插入 old 子链头部。
2. 如果调用方立即访问，是否提升为 young 由 `OldBlockPromotionPolicy` 判断。
3. read-ahead 页默认只进入 old 子链，不立即提升。
4. 大扫描读到的页在 `oldBlocksTime` 窗口内重复访问也不提升。

### 6.2 淘汰候选

`EvictionCandidateSelector` 从 LRU 尾部扫描：

可淘汰条件：

- `bufFixCount == 0`
- `ioState == NONE`
- 没有线程持有 page latch
- frame 不在 flush observer 保护范围内
- frame 不属于当前正在恢复、drop、truncate 的特殊保护集合

处理路径：

- clean page：从 page hash 和 LRU 摘除，reset frame，回到 free list。
- dirty page：加入 LRU flush 请求，优先让 page cleaner 刷出；前台线程只在容量紧张时同步等待。
- stale clean page：直接丢弃。
- stale dirty page：如果 tablespace 已 drop 且 redo 边界允许，可丢弃；否则交给 recovery/drop cleanup 策略处理。

### 6.3 Free List

`FreeList` 存放未绑定 page 的 frame。

分配 frame 的顺序：

1. 优先从 free list 取。
2. free list 空时从 LRU 尾部淘汰 clean frame。
3. 如果只找到 dirty frame，触发 flush batch。
4. 仍无 frame 时等待 `FreeFrameCondition`，并记录 foreground wait metric。

Buffer Pool 初始化阶段所有 frame 都在 free list。Warmup/load 也只通过普通 get/prefetch 路径填充，不绕过 free list 和 LRU。

### 6.4 扫描抗污染策略

`ScanResistancePolicy` 综合以下信息：

- 页来源：user demand read、linear read-ahead、random read-ahead、warmup load。
- 访问时间：是否超过 `oldBlocksTime`。
- 访问模式：sequential scan、range scan、point lookup。
- 页位置：old/new 子链、与 LRU 头部距离。

规则：

- point lookup 命中 old page，超过时间窗后提升到 new 头部。
- range scan 页默认保守提升，避免一次大范围查询冲掉 OLTP 热页。
- read-ahead 页只有被真实访问且超过时间窗才有资格提升。
- warmup load 页插入 old 子链头部或 midpoint，不直接污染 new 头。

## 7. 读页与加载

### 7.1 同步 getPage 数据流

数据流图见 [bufferpool-data-flow.mmd](diagrams/bufferpool-data-flow.mmd)。

`BufferPool.getPage(pageId, latchMode, mtr)`：

1. `BufferPoolRouter` 根据 `PageId` 选择 instance。
2. `PageHashTable` 查找 frame。
3. 命中且非 stale：增加 buf fix，按 latchMode 获取 latch，更新访问统计和 LRU，返回 handle。
4. 命中但 `ioState == READ`：等待 `PageLoadFuture`，完成后重试。
5. miss：向 `FrameAllocator` 申请 frame。
6. 在 page hash 注册 loading frame 或 loading placeholder。
7. 调用 `PageReadService.read(pageId)` 从 `PageStore` 读取 page image。
8. 校验 checksum、space id、page no、page type 基本合法性。
9. 发布 frame 状态为 `CLEAN`，插入 LRU old 子链。
10. 增加 buf fix，获取 latch，注册到 MTR，返回 handle。

### 7.2 创建新页

`BufferPool.createPage(pageId, pageType, mtr)` 用于磁盘管理模块分配新页后初始化：

1. 要求调用方已经在 MTR 中完成空间分配 redo 的收集。
2. page hash 中不能存在同一 `PageId` 的有效 clean/dirty frame；如果存在 stale frame，必须先隔离。
3. 从 free list 或 eviction 获得 frame。
4. 初始化 `FilePageHeader`、`PageBody`、`FilePageTrailer`。
5. 设置 frame 为 `DIRTY_PENDING`，`oldestModificationLsn/newestModificationLsn` 待 MTR commit 分配，commit 前禁止 flush。
6. 插入 page hash 和 LRU old 子链。
7. 返回 X latch 的 `PageHandle`。

### 7.3 IO 状态

`IoState`：

- `NONE`
- `READ`
- `WRITE`
- `READ_AHEAD`
- `FLUSH`

规则：

- `READ/READ_AHEAD` 中的 frame 不允许被普通调用方修改。
- `WRITE/FLUSH` 中的 frame 仍可被读，但刷出快照必须由 `FlushCoordinator` 固定。
- 一个 frame 同一时间只能有一个 IO owner。
- IO owner 完成后负责清理 `ioState`、发布结果、唤醒等待者。
- IO 失败必须恢复 page hash 和 frame 状态，不能留下永久 loading 占位。

## 8. Read-Ahead 设计

### 8.1 ReadAheadController

`ReadAheadController` 只观察 page 访问模式，不改变调用方语义：

- `recordAccess(PageId, AccessType)`
- `maybeScheduleLinearReadAhead(PageId)`
- `maybeScheduleRandomReadAhead(PageId)`
- `submit(PageRange, ReadAheadReason)`

read-ahead 请求进入后台队列：

- 不阻塞当前用户线程。
- 不提升 read-ahead 页到 new 子链。
- 如果目标页已在 page hash 中，跳过。
- 如果 frame 不足，read-ahead 可被丢弃，不能挤占前台需求读。

### 8.2 Linear Read-Ahead

触发条件：

- 同一 extent 内出现连续访问。
- 连续访问页数达到 `readAheadThreshold`，默认 56，范围 0 到 64。
- 当前访问方向稳定，且下一个 extent 存在。

动作：

- 异步预取下一个 extent 的候选页。
- 每个页仍按普通 `PageReadService` 加载和校验。
- 插入 LRU old 子链。

### 8.3 Random Read-Ahead

触发条件：

- 配置 `randomReadAheadEnabled=true`。
- 同一 extent 中已有足够多页在 Buffer Pool 中。
- 预取剩余页不会突破 read-ahead IO budget。

动作：

- 异步读取同一 extent 的缺失页。
- 如果预取页未被访问即老化到 LRU 尾部，可直接淘汰。
- 统计 `readAheadEvictedWithoutAccess`，用于后续调参。

## 9. Dirty Page 与 MTR 协作

### 9.1 标脏时机

页内容修改发生在两步：

1. 持有 X latch 的调用方通过 `PageCursor` 或类型化 page object 修改 page image。
2. 修改后的 frame 先进入 `DIRTY_PENDING`，仍由当前 MTR 的 X latch 和 buffer fix 保护，不允许 flush。
3. MTR commit 为 redo record 分配 LSN 后，`DirtyPageMarker` 把涉及 frame 发布为 committed dirty 并写入 `newestModificationLsn`。

若 frame 从 clean 变 dirty：

- `oldestModificationLsn = firstRedoLsnOfThisPage`
- 插入 flush list，按 oldest LSN 排序。
- dirty page count 增加。

若 frame 已 dirty：

- 保留最早的 `oldestModificationLsn`。
- 更新 `newestModificationLsn`。
- flush list 顺序按 oldest LSN 不变。

若 MTR 在发布 dirty 前失败：

- `PageCursor` 必须通过 MTR-local before image 或 page operation undo 恢复 page image。
- frame 回到修改前状态：原先 clean 则回到 `CLEAN`，原先 dirty 则保留原 dirty LSN。
- 不允许留下没有 redo LSN 的 committed dirty page。

### 9.2 与 MiniTransaction 的边界

MTR 负责：

- page latch 生命周期。
- buffer fix 生命周期。
- redo record 收集与 LSN 分配。
- commit 时通知 `DirtyPageMarker`。

Buffer Pool 负责：

- frame 是否 dirty。
- dirty page 是否进入 flush list。
- pageLSN 和 flush metadata 的内存态维护。
- 禁止脏页在 redo durable 前刷盘。

边界规则：

- Buffer Pool 不生成业务 redo record。
- MTR 不直接操作 LRU/free/flush list。
- `DirtyPageMarker` 只接受 MTR commit 事件，不接受普通业务层直接标脏；`BufferPool.onMtrCommit()` 是观察者入口，不是业务 API。
- temporary tablespace 可启用 `NO_REDO`，但仍必须进入 dirty tracking，直到被刷出或释放。

### 9.3 Flush List

`FlushList` 是 instance 内部结构：

- 按 `oldestModificationLsn` 从小到大排序。
- 支持按 checkpoint 目标 LSN 批量选页。
- 支持查询某个 tablespace 的 dirty page 数，用于 drop/truncate 前 drain。
- 支持 flush observer，保护 bulk create index 等特殊场景。

Flush list 只存 frame descriptor，不存 page body 副本。真正写盘前由 flush 模块在持有必要 latch 或一致性快照的前提下复制 page image。

## 10. Flush、Doublewrite 与 Checkpoint

Buffer Pool 不负责最终写盘策略，但提供必要接口：

- `dirtyPagesBefore(Lsn limit, int maxPages)`
- `lruFlushCandidates(int maxPages)`
- `markFlushStarted(BufferFrame, FlushType)`
- `markFlushDone(BufferFrame, Lsn flushedLsn)`
- `markFlushFailed(BufferFrame, Throwable)`
- `dirtyRatio()`

Flush 路径：

1. `FlushCoordinator` 从 flush list 或 LRU dirty 尾部选择候选页。
2. 校验 `page.newestModificationLsn <= redoDurableLsn`。
3. 固定 frame，阻止淘汰。
4. 获取适当 page latch 或复制稳定 page image。
5. 计算 checksum，写入 pageLSN。
6. 调用 doublewrite strategy。
7. 写 tablespace data file。
8. 成功后，如果 frame 没有新的修改，则从 flush list 移除并设为 clean；如果期间又被修改，只推进已刷 LSN，保留 dirty。
9. 通知 checkpoint coordinator 更新候选 checkpoint LSN。

与 MySQL 8.0 对齐的策略点：

- `FlushType.LRU`：为腾出 free frame，从 LRU 尾部刷脏。
- `FlushType.LIST`：为推进 checkpoint，按 oldest LSN 刷脏。
- `FlushType.SINGLE_PAGE`：前台极端容量紧张时单页刷出。
- `flushNeighborsPolicy`：默认不刷邻居页，适合 SSD；可配置为刷同一 extent 连续脏页。

## 11. Warmup：Buffer Pool Dump/Load

### 11.1 Dump

`BufferPoolDumpService` 负责保存热页定位信息：

- 默认保存每个 instance 中最近使用的一部分页。
- 只保存 `SpaceId + PageNo + PageType + optional checksum hint`，不保存 page body。
- dump 来源是 LRU 热端快照，不能长时间持有 LRU 锁。
- dump 文件不参与 crash recovery；损坏时可丢弃。

### 11.2 Load

`BufferPoolLoadService` 在后台执行：

1. 读取 dump 文件。
2. 跳过不存在、已 drop、space version 不匹配的页。
3. 按 instance 和 tablespace 分批调度 prefetch。
4. 控制 IO 速率，避免启动阶段抢占 redo recovery 和用户请求。
5. load 页插入 old 子链，不直接提升为 hot。
6. load 可被取消，取消只停止后续调度，不影响已加载页。

Warmup 必须复用普通 read path。这样 checksum、page hash、LRU、free list、IO failure、stale page 处理都保持一致。

## 12. 与 Disk Manager 的协作

Disk Manager 通过 Buffer Pool 访问元数据页和新分配页：

- `SpaceHeaderRepository`、`ExtentDescriptorRepository`、`SegmentInodeRepository` 调用 `getPage(..., X/S, mtr)`。
- `SegmentPageAllocator` 分配新页后调用 `createPage(pageId, pageType, mtr)`。
- `freePage(pageId, mtr)` 释放磁盘空间后，必须通知 Buffer Pool `markPageFreed(pageId, spaceVersion)`。

Buffer Pool 处理 freed page：

- 如果 page 不在缓存中，无动作。
- 如果 page 在缓存且 `bufFixCount == 0`，从 page hash 删除并回收 frame。
- 如果 page 正被使用，设置 `filePageWasFreed=true` 或 `STALE`，最后一个 handle 释放时回收。
- 如果 page dirty，必须根据 MTR/redo 边界决定丢弃、刷出还是等待 recovery cleanup。

边界：

- Buffer Pool 不更新 XDES bitmap、segment inode。
- Disk Manager 不摘除 LRU 节点。
- Page free 与 frame stale 使用事件接口传递，避免两个模块互相访问内部结构。

## 13. 并发与锁顺序

Buffer Pool latch 和 IO 等待状态图见 [bufferpool-latch-io-state.mmd](diagrams/bufferpool-latch-io-state.mmd)。

### 13.1 Buffer Pool 内部锁

每个 instance 拆分锁：

- `pageHashLock`：保护 `PageId -> frame/loading` 映射。
- `freeListLock`：保护 free list。
- `lruListLock`：保护 LRU old/new 子链。
- `flushListLock`：保护 flush list。
- `frameMutex`：保护单个 frame 的 state、bufFixCount、ioState、dirty flags。
- `pageLatch`：保护 page body。

拆锁目标是降低争用。锁粒度保持在 Buffer Pool 内部，外部模块不感知。

### 13.2 标准锁顺序

内部锁顺序：

1. instance routing 不加锁。
2. `pageHashLock`
3. `frameMutex`
4. `freeListLock` / `lruListLock` / `flushListLock`
5. `pageLatch`

注意：

- 普通页内容读写只应长期持有 `pageLatch`，不应同时持有 list/hash 锁。
- IO 等待前必须释放 list/hash 锁。
- flush batch 线程不能持有业务页 latch 后再等待其它页 latch。
- 同一批多个 page latch 按 `PageId` 排序。
- MTR memo 释放顺序必须与获取顺序相反。

### 13.3 前台等待策略

前台线程允许等待：

- 同一 page 的 loading future。
- free frame condition。
- 必要的 page latch。

前台线程不应执行大批量刷脏或大批量 read-ahead。容量紧张时最多触发小批 flush 或 single page flush，然后交还给 page cleaner。

### 13.4 IO、Flush 与事务锁边界

Buffer Pool 内部锁只保护 frame 生命周期、hash/list 结构、dirty 元数据和 page body latch。它不参与数据库事务锁等待，也不参与事务死锁检测。

IO 并发规则：

- page miss 合并使用 Single Flight；等待 `PageLoadFuture` 前必须释放 `pageHashLock`、`freeListLock`、`lruListLock` 和 `flushListLock`。
- 发起物理读前，frame 进入 `LOADING`，`ioState = READ`，并挂入 page hash loading 占位。
- 读完成发布 frame 时，只短暂持有 `frameMutex` 和必要 list/hash 锁；发布后等待线程重新获取 page latch。
- flush 写盘前必须固定 frame，记录 flush snapshot 或持有必要 page latch，然后释放 LRU/flush list 锁再进入 `PageStore`。
- read-ahead 不等待 free frame condition 太久；若拿不到 frame 或发现目标 page 正在 loading，直接跳过。
- `ioState != NONE` 的 frame 不允许进入 free list，也不能被 eviction 复用。

与事务锁的边界：

- 当前读需要行锁时，应由 B+Tree/Record 层在进入可能阻塞的 `LockManager` 等待前释放 page latch 或使用重新定位协议。
- Buffer Pool 不能在持有 list/hash/frame 锁时调用 `LockManager`、`UndoLogManager` 或 B+Tree split/merge。
- 事务死锁检测只检查事务等待图；Buffer Pool 的 page latch timeout 以 `PageLatchTimeoutException` 暴露，调用方决定重启 MTR 或重试语句。
- MTR memo 只管理 page latch 和 buffer fix 的释放顺序，不表示数据库事务锁所有权。

与物理文件锁的边界：

- `PageReadService` 和 flush 写出通过 `PageStore` 进入 `storage.fil`，只能在释放 Buffer Pool list/hash 锁后获取物理文件锁。
- drop/truncate/discard 先通过 Disk Manager 获取表空间生命周期 X latch，再通知 Buffer Pool 将相关 frame 标记为 `STALE`。
- Buffer Pool 不直接持有 `FileSizeLock` 或 `DataFileHandleLock`；文件大小和句柄状态由 `PageStore` 屏蔽。

### 13.5 Page Latch、PageLoadFuture 与 Flush 持有变化

| 状态 | 持有者 | 持有资源 | 进入条件 | 退出条件 |
| --- | --- | --- | --- | --- |
| `HASH_LOCKED` | 当前线程 | `pageHashLock` | `getPage()` 查找 page id | 命中 frame、注册 loading 或发现已有 loading |
| `LOADING_OWNER` | 首个 miss 线程 | loading 占位、reserved frame、`ioState=READ` | page miss 且成功注册 Single Flight | 发起 `PageStore.readPage()` |
| `WAIT_LOADING` | 后续 miss 线程 | `PageLoadFuture` wait slot | 发现目标 page 正在 loading | future 完成、timeout 或 IO error |
| `FILE_IO` | IO owner | PageStore 物理 IO，不持有 list/hash 锁 | loading owner 释放内部锁后 | 发布 frame 或广播错误 |
| `PUBLISH_FRAME` | IO owner | 短持有 `frameMutex` 和必要 hash/list 锁 | 读盘成功 | 清理 `ioState` 并唤醒等待者 |
| `PAGE_LATCHED` | 当前线程/MTR | page S/X latch、buffer fix | 命中或 loading 完成后重新校验 | MTR memo 释放或手动 close |
| `FLUSH_FIXED` | page cleaner | frame fix 或 page image snapshot | dirty page 被选中刷出 | 释放 flush/LRU list 锁后进入 PageStore |
| `PAGESTORE_IO` | page cleaner | PageStore 文件 IO | redo durable LSN 已满足 | 写盘完成、错误或 page 又被修改 |
| `ERROR_BROADCAST` | IO owner | future/error 通知权 | read/write 失败 | 唤醒等待者并清理占位 |
| `RELEASED` | 无 | 无 Buffer Pool 短锁 | 正常释放、timeout 或异常清理 | 返回调用方 |

持有变化规则：

- `lookup`：`pageHashLock` 只保护 hash/loading 映射，不能跨物理 IO 或事务锁等待。
- `single flight wait`：等待 `PageLoadFuture` 前释放 `pageHashLock`、list lock 和 `frameMutex`。
- `publish`：IO owner 发布 frame 时短持有内部锁，等待线程醒来后必须重新检查 page id、space version 和 `ioState`。
- `page latch`：page latch 只保护 page body；进入可能阻塞的 `LockManager` 等待前，调用方必须释放 page latch 和 buffer fix。
- `flush`：flush 线程固定 frame 或复制 page image 后释放 LRU/flush list 锁，再进入 `PageStore` 获取物理文件锁。
- `error cleanup`：IO 错误通过 future 广播；Buffer Pool 不把 page latch/mutex 等待加入事务 Wait-For Graph。

## 14. 异常处理

异常类型：

- `BufferPoolFullException`
- `PageLoadException`
- `PageIoInProgressException`
- `PageChecksumMismatchException`
- `StalePageException`
- `InvalidPageStateException`
- `PageLatchTimeoutException`
- `BufferPoolWarmupException`

错误策略：

- 读页 checksum 失败时，先交给 recovery/doublewrite 修复策略；不可修复则抛出 `PageChecksumMismatchException`。
- page load 失败必须清理 page hash loading 占位并归还 frame。
- latch timeout 不自动重试无限次，调用方决定是否重启 MTR。
- buffer pool 满且无法刷出/淘汰时抛出 `BufferPoolFullException`，不得静默扩大内存。
- stale page 不能返回给普通查询路径。
- warmup 失败只影响预热，不阻止数据库启动，除非配置为强制预热。

## 15. API 设计

### 15.1 BufferPool

对外门面：

- 获取已有页。
- 创建新页。
- 尝试获取页。
- 标记 page freed/stale。
- 提交 MTR dirty 通知。
- 为 flush 提供 dirty candidates。
- 为 warmup 提供 hot page snapshot。

返回值只使用 `PageHandle`、`PageCursor`、统计 DTO，不返回裸 frame 或内部链表节点。

### 15.2 PageHandle

职责：

- 暴露 `PageId`、`PageType`、`LatchMode`。
- 创建 `PageCursor`。
- 管理 latch 与 fix 的释放。
- 防止 release 后继续访问。

### 15.3 BufferPoolMaintenance

后台维护接口：

- `evictOne(instanceId)`
- `flushLruTail(instanceId, maxPages)`
- `resize(targetFrameCount)`
- `dumpHotPages(policy)`
- `loadHotPages(policy)`

这些能力不进入普通业务门面，避免上层误用。

## 16. 设计模式使用清单

- Facade：`BufferPool` 隐藏 instance、page hash、LRU、IO、dirty 细节。
- Repository：`PageHashTable` 管理 `PageId -> frame`。
- State：`BufferFrameState` 与 `FrameStateMachine` 固化 frame 生命周期。
- Strategy：`ReplacementPolicy`、`ScanResistancePolicy`、`ReadAheadPolicy`、`FlushNeighborPolicy`。
- Policy：`OldBlockPromotionPolicy`、`EvictionCandidatePolicy`、`WarmupDumpPolicy`。
- Single Flight：同一 page miss 合并为一次物理读。
- RAII Guard：`PageHandle` / `BufferFixGuard` 保证 release。
- Template Method：`PageReadTemplate` 固定 `reserve frame -> hash placeholder -> read -> verify -> publish`。
- Observer：`MtrCommitObserver`、`FlushObserver`、`BufferPoolMetricObserver`。
- Snapshot：`BufferPoolDumpService` 保存热页 ID 快照。
- Factory：`BufferFrameFactory`、`PageHandleFactory`、`BufferPoolInstanceFactory`。

## 17. 高内聚、低耦合约束

强制规则：

- `BufferFrame` 只能在 `storage.buf` 包内可见。
- 上层模块不得缓存 `PageCursor` 超过 `PageHandle` 生命周期。
- 所有写页路径必须经过 MTR 或明确的 recovery/temporary 特权路径。
- committed dirty page 必须同时满足 `state == DIRTY` 和 `inFlushList == true`；`DIRTY_PENDING` 只能由当前 MTR 持有，不能进入 flush list。
- `bufFixCount > 0` 的 frame 不允许从 page hash 删除。
- `ioState != NONE` 的 frame 不允许进入 free list。
- read-ahead 不能阻塞前台 demand read。
- warmup load 不能绕过 checksum 和 stale page 检查。
- flush 不能刷出 redo 尚未 durable 的修改。
- Disk Manager 只能通过事件通知 page freed/stale，不直接操作 LRU。
- Buffer Pool 不读取或修改 extent bitmap、segment inode、B+Tree record directory。

推荐模块边界：

`btree/record/fsp -> BufferPool -> PageStore`  
`MTR -> DirtyPageMarker -> FlushList`  
`FlushCoordinator -> BufferPool dirty view -> PageStore`  
`Recovery -> BufferPool restricted access -> PageStore`

## 18. 典型数据流

### 18.1 读命中

1. B+Tree 调用 `BufferPool.getPage(pageId, S, mtr)`。
2. instance router 定位分片。
3. page hash 命中 clean/dirty frame。
4. frame buf fix 加一。
5. 获取 S latch。
6. MTR memo 记录 latch 和 fix。
7. LRU policy 根据位置和时间决定是否 make young。
8. 返回 PageHandle。

### 18.2 读 miss

1. page hash 未命中。
2. FrameAllocator 从 free list 或 LRU 淘汰取得 frame。
3. page hash 注册 loading。
4. PageReadService 从 PageStore 读盘。
5. 校验 page header、checksum、pageLSN。
6. 发布 frame 为 clean。
7. 插入 LRU old 子链头部。
8. 获取 latch/fix，返回 PageHandle。
9. ReadAheadController 观察访问模式，必要时后台预取。

### 18.3 写页与标脏

1. 调用方以 X latch 获取 PageHandle。
2. PageCursor 修改 page image。
3. 上层 page operation 或 cursor 追加 redo record 到 MTR。
4. MTR commit 分配 LSN 并写 redo log buffer。
5. redo 根据策略刷盘或等待。
6. DirtyPageMarker 接收 MTR commit 事件。
7. frame 从 clean 转 dirty，进入 flush list；或更新 dirty frame 的 newest LSN。
8. MTR 释放 latch/fix。

### 18.4 LRU 淘汰

1. free list 空。
2. EvictionCandidateSelector 从 LRU 尾部扫描。
3. 跳过 fixed、latched、IO 中、dirty 且不能立即处理的 frame。
4. clean candidate 从 LRU 和 page hash 摘除。
5. frame reset 为 FREE。
6. 分配给新的 page load。

### 18.5 Dirty 淘汰

1. LRU 尾部候选是 dirty page。
2. Buffer Pool 发出 LRU flush request。
3. FlushCoordinator 确认 redo durable。
4. 通过 doublewrite 写出 page。
5. 如果写出期间没有新修改，frame 转 clean 并可被淘汰。
6. 如果写出期间又被修改，保留 dirty，继续留在 flush list。

### 18.6 Buffer Pool Warmup

1. shutdown 或在线 dump 获取每个 instance LRU 热端快照。
2. dump 文件只写 page id。
3. startup 后后台 load 读取 page id。
4. 跳过不存在或 stale page。
5. 调用 prefetch/load 普通读页路径。
6. load 页进入 old 子链。
7. 用户真实访问后再根据 promotion policy 进入 new 子链。

## 19. 测试设计

虽然本次不写代码，后续实现应覆盖：

- 值对象和路由测试：`PageId -> instanceId` 稳定且分布均匀。
- page hash 测试：命中、miss、并发 single flight、IO 失败清理占位。
- frame 状态测试：所有合法状态转换、非法转换拒绝。
- buf fix 测试：fixed frame 不可淘汰，release 后可回收。
- latch 测试：S/S 兼容、S/X 互斥、X 写保护。
- LRU 测试：midpoint insertion、old/new 边界、oldBlocksTime、make young、scan resistance。
- eviction 测试：clean 淘汰、dirty 跳过、dirty flush 后淘汰、stale 回收。
- dirty tracking 测试：oldest/newest LSN、flush list 顺序、重复修改。
- flush 协作测试：redo durable 前禁止刷页、写出期间再变脏。
- IO 并发测试：同页 miss single flight、loading 等待前释放内部锁、flush 期间并发读、drop/truncate stale frame。
- read-ahead 测试：linear 阈值、random 开关、prefetch 不阻塞前台。
- warmup 测试：dump page id、load 跳过 stale、不污染 new 子链。
- disk manager 协作测试：page free 后缓存 frame stale 与回收。
- recovery 协作测试：checksum 失败、doublewrite 修复、redo replay 后 pageLSN。
- 并发测试：多线程 get 同页、不同 instance 并发、前台等待上限。
- property-based 测试：随机 get/release/dirty/flush/evict 后 hash、LRU、free、flush list 不变量成立。

## 20. 后续实现顺序

推荐分阶段实现：

1. `domain` 复用 `PageId`、`Lsn`、`PageSize`，补齐 `LatchMode`、`BufferPoolConfig`。
2. `frame`：`BufferFrame`、`PageImage`、`PageLatch`、`FrameStateMachine`。
3. `PageHandle` 与 `PageCursor` 生命周期。
4. `BufferPoolInstance`、`PageHashTable`、free list。
5. 同步 `getPage/createPage`，先不做异步 read-ahead。
6. LRU old/new 子链和 midpoint insertion。
7. MTR memo 集成和 dirty marker。
8. flush list 与 flush coordinator 协作接口。
9. clean/dirty eviction。
10. read-ahead controller。
11. buffer pool dump/load。
12. 多 instance 和并发压测。

## 21. 十五轮自检记录

| 轮次 | 检查主题 | 检查结果 |
| --- | --- | --- |
| 1 | 不写代码边界 | 只新增 Markdown 和 Mermaid 设计文件，没有生成 Java 源码 |
| 2 | 与磁盘管理衔接 | 已明确 `fsp` 通过 `BufferPool` 访问元数据页和新分配页 |
| 3 | MySQL 8.0 参考 | 已覆盖 Buffer Pool、LRU、multiple instances、flushing、dump/load、read-ahead、源码 `buf0buf/buf0lru/buf0rea/buf0flu/buf0dump` |
| 4 | 高内聚 | 命中、加载、frame、LRU、dirty、read-ahead、warmup 都收敛在 `storage.buf` 子包 |
| 5 | 低耦合 | 已禁止 `buf` 依赖 `fsp/btree/record/sql`，并隐藏 `BufferFrame` |
| 6 | 领域模型 | 已定义 `BufferPool`、`BufferPoolInstance`、`BufferFrame`、`PageHandle`、`PageHashTable` |
| 7 | 生命周期 | 已给出 frame 状态机和状态转换约束 |
| 8 | 并发控制 | 已区分 page latch 与 buffer fix，并给出内部锁顺序 |
| 9 | LRU 策略 | 已覆盖 midpoint insertion、old/new 子链、3/8 old ratio、old blocks time |
| 10 | 淘汰策略 | 已区分 clean、dirty、stale、IO 中、fixed frame 的处理 |
| 11 | MTR 协作 | 已说明 commit 后标脏、oldest/newest LSN、redo durable 前禁止 flush |
| 12 | Flush 协作 | 已覆盖 flush list、LRU flush、single page flush、doublewrite、checkpoint |
| 13 | Read-ahead | 已覆盖 linear 和 random read-ahead，且限制不阻塞前台读 |
| 14 | Warmup | 已覆盖 dump/load 只保存 page id，load 复用普通读路径 |
| 15 | 图与文档一致性 | 四个 Mermaid 图与正文术语一致，并已通过 `npx @mermaid-js/mermaid-cli` 实际渲染 |

## 22. 参考链接

- MySQL 8.0 Reference Manual - Buffer Pool: https://dev.mysql.com/doc/refman/8.0/en/innodb-buffer-pool.html
- MySQL 8.0 Reference Manual - Configuring Multiple Buffer Pool Instances: https://dev.mysql.com/doc/refman/8.0/en/innodb-multiple-buffer-pools.html
- MySQL 8.0 Reference Manual - Making the Buffer Pool Scan Resistant: https://dev.mysql.com/doc/refman/8.0/en/innodb-performance-midpoint_insertion.html
- MySQL 8.0 Reference Manual - Configuring InnoDB Buffer Pool Prefetching (Read-Ahead): https://dev.mysql.com/doc/refman/8.0/en/innodb-performance-read_ahead.html
- MySQL 8.0 Reference Manual - Configuring Buffer Pool Flushing: https://dev.mysql.com/doc/refman/8.0/en/innodb-buffer-pool-flushing.html
- MySQL 8.0 Reference Manual - Saving and Restoring the Buffer Pool State: https://dev.mysql.com/doc/refman/8.0/en/innodb-preload-buffer-pool.html
- MySQL 8.0.46 Source Documentation - `buf0buf.h`: https://dev.mysql.com/doc/dev/mysql-server/8.0.46/buf0buf_8h.html
- MySQL 8.0.46 Source Documentation - `buf_page_t`: https://dev.mysql.com/doc/dev/mysql-server/8.0.46/classbuf__page__t.html
- MySQL 8.0.46 Source Documentation - `buf_block_t`: https://dev.mysql.com/doc/dev/mysql-server/8.0.46/structbuf__block__t.html
- MySQL 8.0.46 Source Documentation - `buf0lru.h`: https://dev.mysql.com/doc/dev/mysql-server/8.0.46/buf0lru_8h.html
- MySQL 8.0.46 Source Documentation - `buf0rea.h`: https://dev.mysql.com/doc/dev/mysql-server/8.0.46/buf0rea_8h.html
- MySQL 8.0.46 Source Documentation - `buf0flu.h`: https://dev.mysql.com/doc/dev/mysql-server/8.0.46/buf0flu_8h.html
- MySQL 8.0.46 Source Documentation - `buf0dump.cc`: https://dev.mysql.com/doc/dev/mysql-server/8.0.46/buf0dump_8cc.html
