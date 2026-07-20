# MiniMySQL InnoDB 风格磁盘管理模块设计

版本：2026-06-03  
实现语言：Java  
参考基线：MySQL 8.0.46 InnoDB 官方手册与源码文档  
本地参考：`C:\coding\java\self\miniproject\miniproject\mysql\InnoDB存储结构与数据链路.html`
关联设计：[innodb-crash-recovery-design.md](innodb-crash-recovery-design.md)、[innodb-flush-checkpoint-doublewrite-design.md](innodb-flush-checkpoint-doublewrite-design.md)、[innodb-record-design.md](innodb-record-design.md)、[innodb-undo-log-purge-design.md](innodb-undo-log-purge-design.md)、[mysql-lock-observability-deadlock-design.md](mysql-lock-observability-deadlock-design.md)

## 1. 目标与边界

本设计面向一个用 Java 编写的 MiniMySQL 存储引擎磁盘管理模块。目标不是完整复刻 InnoDB，而是在保持教学和工程可实现性的前提下，抽象出 InnoDB 8.0 中表空间、段、区、页、MTR、redo、恢复和文件扩展的核心思想，形成高内聚、低耦合的模块边界。

设计目标：

- 高内聚：空间分配、表空间文件、页缓存、MTR、redo、恢复各自独立成包，每个包只维护自己的状态和规则。
**- 低耦合：上层 B+Tree、记录管理、事务管理只依赖 `DiskSpaceManager`、`PageCursor`、`MiniTransactionManager` 等稳定接口，不直接操作文件、bitmap 或 inode 页。**
- InnoDB 风格：按逻辑分配关系 `Tablespace -> Segment -> Extent -> Page` 建模，同时保留 segment 前 32 个 fragment page 的特殊路径、leaf/non-leaf segment、extent list、reservation、MTR memo stack、redo-before-data 的设计。
- Java 可落地：用值对象、接口、策略对象、仓储对象和模板方法表达领域概念，避免把所有逻辑堆到一个 manager。
- 可恢复：所有会改变磁盘元数据或页内容的操作必须在 MTR 中产生 redo record；恢复阶段从 checkpoint 后重放 redo。

非目标：

- 不实现 SQL 层、记录格式、MVCC、锁管理和完整 B+Tree；记录格式由 [innodb-record-design.md](innodb-record-design.md) 单独定义。
- 不实现完整 InnoDB 二进制文件格式兼容。
- 不生成 Java 源码。本文件只定义设计、关系和数据流。

## 2. MySQL 8.0 参考依据

本设计参考以下 MySQL 8.0 行为，文末列出官方链接。

- InnoDB 每个 tablespace 由 page 组成；默认 page size 为 16KB，也支持 4KB、8KB、32KB、64KB。page size 在实例内保持一致。
- 物理空间上，连续 page 被组织成 extent。page size 不超过 16KB 时，一个 extent 为 1MB；32KB page 对应 2MB extent；64KB page 对应 4MB extent。
- 逻辑分配上，segment 是 tablespace 内部的“文件”。segment 初始增长时先逐页分配前 32 个 fragment page，之后按完整 extent 分配。大型 segment 可以一次增加最多 4 个 extent，以提升顺序性。
- 每个 InnoDB index 使用两个 segment：leaf segment 存放叶子页，non-leaf segment 存放非叶子页。
- tablespace 中部分 page 存储 bitmap、space header、extent descriptor、segment inode 等元数据，因此部分 extent 不能整体分给业务 segment。
- file-per-table 和 general tablespace 默认按当前大小自动扩展：小于 1 个 extent 时逐页扩展，大于 1 个 extent 且小于 32 个 extent 时每次 1 个 extent，大于 32 个 extent 后每次 4 个 extent；MySQL 8.0.23 起支持 `AUTOEXTEND_SIZE`。
- MySQL 8.0.26 引入 `innodb_segment_reserve_factor`，默认保留 12.5% segment page 作为未来增长空间。本设计将其抽象为 `SegmentReservePolicy`。
- MTR 维护 redo log buffer、memo stack、savepoint、log mode 和 latch。提交时写入 redo 并释放 memo 中的 latch/block。
- InnoDB 在新 tablespace page 分配前写 redo。崩溃恢复时，未完成的 page allocation 通过 redo 重放。
- MySQL 8.0 使用 fuzzy checkpoint；恢复时从 checkpoint label 后扫描 redo 并应用修改。
- MySQL 8.0.20 后 doublewrite 存储区域位于独立 doublewrite files。本设计把 doublewrite 作为 flush 子模块的可插拔策略。
- 本地参考文件强调两套视角必须分开：物理上只能直接定位文件、页和页内字节；逻辑上由 INODE entry 记录 segment 对 fragment page 和 extent list 的归属。

## 3. 总体架构

架构图见 [architecture.mmd](diagrams/architecture.mmd)。

模块分为八层：

1. `storage.api`：对 B+Tree、记录层、DDL 层暴露稳定门面。
2. `storage.mtr`：MTR 生命周期、memo、savepoint、redo 收集和 latch 释放。
3. `storage.buf`：页缓存、frame、page latch、dirty tracking。磁盘管理依赖它获得受控页访问。
4. `storage.fsp`：file space management，负责 segment/extent/page 分配、释放、预留和空间元数据。
5. `storage.fil`：tablespace registry、data file 管理、自动扩展、page IO。
6. `storage.record`：记录格式、页内目录、字段编码、逻辑记录与物理记录转换。
7. `storage.redo`：redo record、redo writer、LSN、checkpoint 协调。
8. `storage.recovery`：启动恢复，重放 redo，修复 page allocation、space header、segment inode、extent bitmap。

核心原则：

- 上层只请求“为某个 index segment 分配 page”，不关心 extent list 和 inode page。
- `fsp` 只通过 `fil` 的 `PageStore` 读写物理文件，不直接使用 `FileChannel`。
- `record` 只通过 `PageCursor` 解释索引页内记录，不直接分配 page、extent 或 segment。
- `mtr` 不懂业务语义，只管理短事务内的 latch、buffer page、redo record 和提交顺序。
- `redo` 不反向依赖 `fsp` 或 `buf`。恢复阶段由 `RecoveryApplier` 根据 redo command 类型分发给对应 page redo handler。

## 4. 包与职责

| 包 | 职责 | 主要依赖 | 设计模式 |
| --- | --- | --- | --- |
| `storage.api` | 对外门面、用例入口、错误模型 | `mtr`, `fsp`, `buf` | Facade |
| `storage.domain` | 值对象和领域枚举 | 无 | Value Object |
| `storage.fil` | tablespace 注册、文件扩展、page store | `domain`, `io`, `redo` | Repository, Strategy, Adapter |
| `storage.fsp` | segment/extent/page 分配释放 | `domain`, `fil`, `buf`, `mtr` | Strategy, Repository, Policy |
| `storage.buf` | buffer frame、latch、dirty page | `domain`, `fil` | Repository, State |
| `storage.record` | 物理记录、逻辑记录、页内目录、类型编码与比较 | `buf`, `mtr`, `trx` | Strategy, Factory, Template Method |
| `storage.mtr` | mini transaction、memo、savepoint | `buf`, `redo` | Unit of Work, Command |
| `storage.redo` | redo record 编码、写入、LSN | `domain` | Command, Template Method |
| `storage.flush` | checkpoint、doublewrite、刷脏 | `buf`, `fil`, `redo` | Strategy, Observer |
| `storage.recovery` | crash recovery、redo replay | `redo`, `fil`, `fsp`, `buf` | Chain of Responsibility |
| `storage.config` | page size、reserve factor、autoextend | 无 | Immutable Config |

## 5. 核心领域模型

类关系图见 [class-relation.mmd](diagrams/class-relation.mmd)。

### 5.1 值对象

- `SpaceId`：tablespace 唯一标识。
- `PageNo`：tablespace 内页号。
- `PageId`：`SpaceId + PageNo`，所有 buffer、redo、fsp 操作以它定位页。
- `PageSize`：实例级页大小，支持 4KB、8KB、16KB、32KB、64KB。
- `ExtentId`：由 `SpaceId + extentNo` 组成。`extentNo = pageNo / pagesPerExtent`。
- `SegmentId`：tablespace 内 segment 标识。
- `Lsn`：redo 逻辑序列号。

这些对象必须不可变，构造时校验边界，避免原始 `long`、`int` 在模块间传递导致语义混乱。

### 5.2 Tablespace

`Tablespace` 是领域聚合根，描述逻辑表空间：

- `spaceId`
- `name`
- `type`: `SYSTEM`, `FILE_PER_TABLE`, `GENERAL`, `UNDO`, `TEMPORARY`
- `pageSize`
- `state`: `NORMAL`, `DISCARDED`, `CORRUPTED`, `ACTIVE`, `INACTIVE`, `EMPTY`
- `dataFiles`
- `autoExtendPolicy`
- `spaceFlags`

`TablespaceRegistry` 负责根据 `SpaceId` 找到 `Tablespace`，并屏蔽数据字典、配置文件和磁盘 header 的加载细节。

### 5.3 Page

所有磁盘页共享统一 envelope：

- `FilePageHeader`
  - `checksum`
  - `spaceId`
  - `pageNo`
  - `prevPageNo`
  - `nextPageNo`
  - `pageLsn`
  - `pageType`
- `PageBody`
- `FilePageTrailer`
  - `checksumTrailer`
  - `low32Lsn`

`Page` 本身不直接暴露可变 byte array。访问页内容必须通过 `PageCursor` 或类型化 page 对象，例如 `SpaceHeaderPage`、`XdesPage`、`SegmentInodePage`、`IndexPage`、`UndoPage`。

### 5.4 Extent

`ExtentDescriptor` 描述一个 extent 的分配状态：

- `extentId`
- `state`: `FREE`, `FREE_FRAG`, `FULL_FRAG`, `FSEG`, `FSEG_FRAG`
- `pageBitmap`
- `ownerSegmentId`
- `listNode`

全局 extent list：

- `FSP_FREE`：完全空闲 extent。
- `FSP_FREE_FRAG`：可按 fragment page 分配的 extent。
- `FSP_FULL_FRAG`：fragment page 已满的 extent。

segment 内 extent list：

- `SEG_FREE`：属于该 segment 但完全未使用。
- `SEG_NOT_FULL`：属于该 segment 且仍有空闲 page。
- `SEG_FULL`：属于该 segment 且已满。

### 5.5 Segment

`Segment` 表示 tablespace 内部的逻辑连续空间：

- `segmentId`
- `spaceId`
- `purpose`: `INDEX_LEAF`, `INDEX_NON_LEAF`, `LOB`, `UNDO`, `SYSTEM`
- `inodePageId`
- `inodeSlot`
- `fragmentPages`
- `freeExtents`
- `notFullExtents`
- `fullExtents`
- `reservedPages`

`Segment` 是磁盘 `SegmentInode`（见 6.4）的只读内存投影。权威状态以 inode 页和 XDES 为准；分配/释放必须先经由 `SegmentInodeRepository` 更新 inode 与 `ExtentDescriptorRepository` 更新 XDES，再刷新内存投影。崩溃恢复后内存 `Segment` 由 redo 重建，禁止把它当作权威数据源。

每个索引由 `IndexSpace` 管理两个 segment：

- `leafSegment`：叶子页，承载实际行或二级索引记录。
- `nonLeafSegment`：内部节点页。

这样可以保持叶子页更可能连续，顺序扫描更友好。

索引的 leaf / non-leaf segment 入口（指向各自 `SegmentInode` 的 FSEG header，即 `inodePageId + inodeSlot`）保存在该索引 **root page** 的 page header 中，对应 InnoDB 的 `PAGE_BTR_SEG_LEAF` 与 `PAGE_BTR_SEG_TOP`。因此打开索引时必须先读 root page，才能定位这两个 segment 的 inode。

## 6. 磁盘元数据页布局

本设计不追求二进制兼容 InnoDB，但保留功能等价的元数据页。

### 6.1 首区固定管理页

独立表空间文件开头按固定页号承载管理信息。本设计保留这些语义，但允许内部编码简化：

| 页号 | InnoDB 语义 | 设计职责 |
| --- | --- | --- |
| page 0 | `FSP_HDR`，并内嵌首 256MB 范围的 XDES entries | 表空间总入口、大小、Space ID、extent free list、inode page list、首批 extent descriptor |
| page 1 | `IBUF_BITMAP` | 保留 change buffer bitmap 扩展点；MiniMySQL 可先不启用 change buffer |
| page 2 | `INODE` | segment inode array；每个 segment 一条 entry，记录 fragment pages 与 FREE/NOT_FULL/FULL extent list |
| page 3 | `SDI` | GENERAL 表空间保存单页 SDI v1 完整表聚合快照；UNDO 表空间在同一固定页使用 `RSEG_HEADER`，由 tablespace type 区分 |
| page 4+ | `INDEX` / `BLOB` | 聚簇索引、二级索引 B+Tree 页，以及大字段溢出页 |

当前创建路径在同一 MTR 中为 page0、page1、page2 写入 FSP_HDR、IBUF_BITMAP、INODE 物理信封；
catalog-loss 离线 scrub 因此可验证固定管理页 identity/type。仅历史教学文件允许“extent0 已保留但
page1 仍全零”的窄兼容；当前新文件不会再生成该形状。scrubber 只顺序读取 manifest 声明的 GENERAL
file-per-table，属性探测与 channel 打开都使用 NOFOLLOW；在消费 bitmap 前先校验 XDES state、owner、
同状态/同 owner 双向 list 地址和 EOF 分配边界。不挂载 registry、不修改页面，也不扩张为
系统/undo/全 data-dir 自动校验。

超过首 256MB 后，每隔固定范围会出现新的 extent descriptor 管理区域。本设计由 `ExtentDescriptorRepository` 屏蔽“XDES entries 在 page 0 内嵌还是在独立管理页中”的差异。

物理定位公式：

- `pageOffset = pageNo * pageSize`
- `pagesPerExtent = extentSizeBytes / pageSize`；`extentSizeBytes` 在页 ≤16KB 时为 1MB，32KB 为 2MB，64KB 为 4MB，因此 4KB 页为 256、8KB 页为 128、16KB/32KB/64KB 页为 64。
- `extentFirstPageNo = extentNo * pagesPerExtent`（不要在分配代码里写死 64，仅 16KB/32KB/64KB 才恰好是 64）。
- 磁盘可直接定位的是文件、页、页内字节；segment 不是连续物理区域。

### 6.2 Page 0: SpaceHeaderPage

`SpaceHeaderPage` 保存表空间级信息：

- `spaceId`
- `pageSize`
- `spaceFlags`
- `currentSizeInPages`
- `freeLimitPageNo`
- `nextSegmentId`
- `freeExtentListHead`
- `freeFragExtentListHead`
- `fullFragExtentListHead`
- `firstInodePageNo`
- `firstXdesEntryOffset`
- `sdiRootPageNo`，保留给未来数据字典副本。
- `serverVersion`
- `spaceVersion`
- `encryptionMetadataOffset`，保留扩展点。

访问方式：

- 只允许 `SpaceHeaderRepository` 读写。
- 写入必须在 MTR 中持有 page X latch 或 tablespace SX/X latch。

### 6.3 XDES Entries

XDES 是 extent descriptor entry 集合，不应被设计成永远独立的页类型。首批 XDES entries 嵌在 page 0 的 `FSP_HDR` 中，后续范围可能由独立的 XDES 管理页承载。`ExtentDescriptorRepository` 根据 `ExtentId` 定位 descriptor 所在页与槽位。

职责：

- 读取和更新 extent state。
- 设置 page bitmap。
- 维护 extent list node。
- 为恢复流程提供幂等更新接口。

### 6.4 SegmentInodePage

`SegmentInodePage` 保存多个 `SegmentInode`：

- segment id
- segment purpose
- fragment page slot array
- free/notFull/full extent list head
- used page count
- reserved page count

职责：

- 创建 segment。
- 分配 fragment page。
- 将 extent 挂入 segment list。
- 释放 segment 时归还 extent 和 fragment page。

SegmentInodePage 是 segment 逻辑存在的证据。磁盘上不存在一段连续区域叫 leaf segment 或 non-leaf segment；segment 只是 inode entry 中的 fragment page slots 与 extent list head 所描述的归属集合。

### 6.5 Index Page 内部结构

索引页是上层 B+Tree 与磁盘页管理的交界。本模块不实现完整记录格式，但设计必须为上层提供正确的页内操作边界。

完整记录格式、字段类型编码和物理/逻辑记录转换见 [innodb-record-design.md](innodb-record-design.md)。本节只保留 Disk Manager 需要知道的页内空间边界。

索引页从头到尾包含：

| 区域 | 作用 |
| --- | --- |
| `FileHeader` | 页号、页类型、`FIL_PAGE_PREV/NEXT`、page LSN、checksum |
| `PageHeader` | 记录数、槽数、`PAGE_HEAP_TOP`、`PAGE_FREE`、`PAGE_GARBAGE`、`PAGE_LEVEL`、插入方向等 |
| `Infimum` / `Supremum` | 虚拟最小/最大记录，简化链表边界 |
| `UserRecords` | 实际记录，物理上按插入顺序放入 heap |
| `FreeSpace` | 新记录 append 或页内重组后的可用空间 |
| `PageDirectory` | 页尾向前增长的稀疏槽数组 |
| `FileTrailer` | checksum 和低位 LSN，用于检测 partial page write |

关键约束：

- `heap_no` 表示记录在 heap 中的物理序号，用户记录按插入顺序增长，不等同于 key 顺序。
- `next_record` 将记录按 key 升序串成单向链表，页内逻辑顺序由它决定。
- `n_owned` 只在每组最后一条记录上记录组内成员数。
- `PageDirectory` 只指向每组最后一条记录，槽数组按 key 有序；中间组通常保持 4 到 8 条记录。
- 查找页内记录时先二分 directory slot，再从上一槽对应记录沿 `next_record` 扫描，组内扫描上限很小。
- InnoDB 的 Page Directory 不存 key 前缀；本设计也不在槽中冗余 key，以保持与 InnoDB 思路一致。

## 7. 分配策略

### 7.1 预留

所有可能一次使用多个 page 的操作必须先调用 `SpaceReservationService.reserve()`。例如 B+Tree split 至少预留：

- 新页本身。
- 可能的 parent split 页。
- segment inode 或 extent descriptor 变更所需的元数据页访问。

预留类型：

- `NORMAL`：插入、split 等可能增长空间的操作。
- `UNDO`：undo log 分配，长期可能被 purge 回收。
- `CLEANING`：purge、物理删除、merge 等清理操作。
- `BLOB`：外部大字段页。

预留对象用 RAII 风格表达为 `SpaceReservation`，Java 中用 `try-with-resources` 使用。MTR commit 后或异常路径必须释放未消耗的预留。

### 7.2 Page 分配

`SegmentPageAllocator` 根据 segment 状态选择路径：

1. segment 已使用 fragment page 少于 32 页：从 `FSP_FREE_FRAG` 找可用 page；没有则从 `FSP_FREE` 拆出一个 extent 变成 `FREE_FRAG`。
2. segment 达到 32 页后：优先从 `SEG_NOT_FULL` 分配；没有则向 segment 分配一个或多个完整 extent。
3. 大 segment 且顺序写入明显：`ExtentAllocationPolicy` 最多一次分配 4 个 extent。
4. 如果 tablespace 空间不足：调用 `AutoExtendPolicy` 扩展数据文件，再重试一次。
5. 分配成功后：初始化 page header、写 redo record、标记 buffer page dirty。

### 7.3 Extent 分配

`ExtentAllocationPolicy` 输入：

- `segment`
- `hintPageNo`
- `direction`: `UP`, `DOWN`, `NO_DIRECTION`
- `pagesNeeded`
- `tablespaceSize`
- `reserveFactor`

策略：

- 对 leaf segment，优先按 hint 附近寻找连续 extent。
- 对 non-leaf segment，优先低碎片和元数据局部性。
- 对普通增长，分配 1 个 extent。
- 对大 segment 顺序增长，分配 2 到 4 个 extent。
- 永远跳过存放 space header、xdes、inode 等元数据 page 的 extent。

### 7.4 释放

释放 page 时：

1. MTR 获取目标 page X latch。
2. `SegmentInodeRepository` 找到 owner segment。
3. 更新 segment inode 的 page 计数和 list。
4. 更新 `ExtentDescriptor` bitmap。
5. 如 extent 从 full 变为 not full，移动到 `SEG_NOT_FULL`。
6. 如 extent 全空且不属于 fragment 管理，移动到 `FSP_FREE`。
7. 追加 redo record。

释放 segment 时：

- fragment page 逐个归还给 `FSP_FREE_FRAG`。
- segment extent list 全部归还 `FSP_FREE`。
- inode slot 标记为空闲。
- 追加 `DROP_SEGMENT` redo。

## 8. Tablespace 扩展策略

`AutoExtendPolicy` 是策略接口：

- `DefaultIbdAutoExtendPolicy`
- `ConfiguredAutoExtendPolicy`
- `UndoAutoExtendPolicy`
- `FixedSizeTablespacePolicy`

默认 file-per-table/general tablespace 行为：

- 当前大小小于 1 个 extent：一次扩展 1 个 page。
- 当前大小大于等于 1 个 extent 且小于 32 个 extent：一次扩展 1 个 extent。
- 当前大小大于等于 32 个 extent：一次扩展 4 个 extent。
- 如果配置了 `AUTOEXTEND_SIZE`：以配置值为主，但必须对齐 page 和 extent。

undo tablespace 行为：

- 初始大小按 MySQL 8.0.23+ 语义设计为 16MiB。
- 运行时扩展从 16MiB 起，根据短时间内连续扩展情况倍增，最大 256MiB。
- 如果显式配置 `AUTOEXTEND_SIZE`，取配置值和动态扩展值的较大者。

文件扩展由 `DataFileGateway` 完成。Windows 和普通文件通道默认写零初始化；Linux 可选 `PreallocationStrategy`，用 adapter 封装 `posix_fallocate` 风格能力，避免 `fsp` 感知平台差异。

### 8.1 物理文件锁与 IO 并发

`storage.fil` 必须把表空间生命周期、文件大小变化和 page IO 分开保护。物理文件锁不进入数据库事务锁系统，也不进入行锁死锁检测；它们只保护文件句柄、文件长度和表空间生命周期。

物理文件锁状态图见 [disk-file-lock-state.mmd](diagrams/disk-file-lock-state.mmd)。

锁对象：

- `TablespaceLifecycleLatch`：保护 open、close、discard、drop、truncate 和普通 IO 的生命周期关系。
- `DataFileHandleLock`：保护 `FileChannel`、mapped buffer 或预分配句柄的打开、关闭、替换。
- `FileSizeLock`：保护 `currentSizeInPages`、`freeLimitPageNo`、autoextend 和文件尾部零填充。
- `PageIoRangeLock`：可选的页范围锁，用于同一 page 或相邻 page 的写入合并、truncate 边界和故障注入测试。
- `FsyncLock`：限制同一 data file 上并发 fsync 数，避免多个后台线程重复刷同一文件。

锁语义：

- 普通 page read：持有 `TablespaceLifecycleLatch(S)`，短暂读取稳定的 file size，然后用 positional IO 读取目标 page。
- 普通 page write / flush：持有 `TablespaceLifecycleLatch(S)`，确认 pageNo 小于稳定 file size，写出完整 page；同一 page 的并发写由 Buffer Pool page latch、flush snapshot 和 `ioState` 控制。
- autoextend：持有 `TablespaceLifecycleLatch(S)` 和 `FileSizeLock(X)`，扩展并零初始化新范围，再发布新的 `currentSizeInPages`。
- truncate/drop/discard：持有 `TablespaceLifecycleLatch(X)`；进入前必须先阻止新 page 分配，drain dirty page，通知 Buffer Pool stale，再关闭或截断 data file。
- recovery 修复 partial write：持有 `TablespaceLifecycleLatch(S)`，按 pageNo 顺序写 doublewrite 副本或 redo 修复内容，不参与业务行锁等待。

物理文件锁顺序：

1. `TablespaceLifecycleLatch`
2. `DataFileHandleLock`
3. `FileSizeLock`
4. `PageIoRangeLock`
5. `FsyncLock`

约束：

- 持有 `FileSizeLock` 时不能等待 Buffer Pool page latch，避免 autoextend 与 flush 互相阻塞。
- 持有 `PageIoRangeLock` 时不能调用 `DiskSpaceManager.allocatePage()`，避免 IO 路径反向进入空间分配。
- 文件扩展完成前，新 page 不能对普通读路径可见。
- data file close/drop 前必须让正在进行的 read/write/fsync 离开 `TablespaceLifecycleLatch(S)`。
- 物理文件锁只处理存储引擎内部资源等待；数据库事务死锁只由 `LockManager` 处理。

物理文件锁持有变化：

| 状态 | 持有者 | 持有资源 | 进入条件 | 退出条件 |
| --- | --- | --- | --- | --- |
| `IDLE` | 无 | 无文件锁 | 无 IO 或生命周期操作 | page IO、autoextend、drop/truncate/discard |
| `LIFECYCLE_S` | 前台线程、page cleaner 或 recovery 线程 | `TablespaceLifecycleLatch(S)` | 普通 read/write/fsync/recovery repair | IO 完成、错误或扩展路径结束 |
| `LIFECYCLE_X` | DDL/cleanup 线程 | `TablespaceLifecycleLatch(X)` | drop、truncate、discard、close | drain 完成并发布新 space version |
| `HANDLE_LOCKED` | Disk Manager | `DataFileHandleLock` | 校验或替换 file handle | 获得稳定 handle 或失败 |
| `SIZE_CHECKED` | Disk Manager | 稳定 file size 快照 | pageNo 边界检查 | 进入 page IO 或返回越界错误 |
| `EXTENDING` | Disk Manager | `FileSizeLock(X)`、生命周期 S latch | autoextend 或预分配 | 零填充新范围并发布 `currentSizeInPages` |
| `PAGE_RANGE_LOCKED` | IO owner | 可选 `PageIoRangeLock` | 需要限制同 page 或 truncate 边界 IO | positional IO 完成 |
| `FSYNC_LIMITED` | page cleaner 或 commit helper | `FsyncLock` | flush policy 要求 data file fsync | fsync 完成或 IO error |
| `DRAINING_IO` | DDL/cleanup 线程 | 生命周期 X latch | 阻止新 IO 后等待旧 S holder 离开 | 可关闭、截断或丢弃文件 |
| `RELEASED` | 无 | 无物理文件锁 | 成功、timeout、IO error 或 cleanup | 返回调用方 |

持有变化规则：

- `acquire lifecycle S`：普通 page IO 只持有表空间生命周期 S latch，不持有事务锁。
- `acquire lifecycle X`：drop/truncate/discard 先阻止新 page 分配和新 page handle，再 drain 旧 IO。
- `autoextend`：只在生命周期 S latch 内持有 `FileSizeLock(X)`，发布新大小前新 page 不可见。
- `page IO`：进入 `PageStore` 前 Buffer Pool 已释放 list/hash 锁；同 page 并发由 Buffer Pool page latch 或 flush snapshot 控制。
- `fsync`：`FsyncLock` 只限制 data file fsync 并发，不回调 Buffer Pool 或 LockManager。
- `release`：物理文件锁不进入 Wait-For Graph；等待必须通过 timeout、IO error 或生命周期 drain 完成。

## 9. MTR 设计

MTR 是磁盘管理模块的一致性边界，类关系图中以 `MiniTransaction` 表示。

### 9.1 状态

`MiniTransaction` 状态机：

- `NEW`
- `ACTIVE`
- `COMMITTING`
- `COMMITTED`
- `ROLLED_BACK`

MTR 不提供数据库事务级 rollback。异常时它只释放 latch、丢弃未提交 redo、撤销内存中未发布的临时状态。已经持久化的修改必须依赖 redo 恢复保证物理一致。

### 9.2 Memo Stack

`MtrMemoStack` 保存短事务期间获得的资源：

- `PAGE_S_LATCH`
- `PAGE_X_LATCH`
- `TABLESPACE_SX_LATCH`
- `TABLESPACE_X_LATCH`
- `BUFFER_FIX`
- `SPACE_RESERVATION`

释放顺序为后进先出。`Savepoint` 记录 memo stack 的位置，用于提前释放局部 latch，例如父页 latch 已不需要时可以在 commit 前释放。

### 9.3 Redo 收集

`RedoRecordCollector` 收集 command：

- `INIT_FILE_PAGE`
- `ALLOCATE_PAGE`
- `FREE_PAGE`
- `UPDATE_SPACE_HEADER`
- `UPDATE_XDES`
- `CREATE_SEGMENT`
- `DROP_SEGMENT`
- `UPDATE_SEGMENT_INODE`
- `WRITE_PAGE_BYTES`

每条 redo record 必须包含：

- record type
- page id 或 tablespace id
- before/after 所需的最小物理信息
- mtr id
- prev lsn
- checksum

设计上使用 Command 模式：每个 redo record 既能编码到 redo log，也能在恢复阶段 `apply(RecoveryContext)`。

### 9.4 Commit 顺序

MTR commit 顺序：

1. 禁止继续获取新 latch。
2. 校验 memo stack 与 redo collector。
3. 为 redo record 分配连续 LSN。
4. 将 redo record 写入 log buffer。
5. 根据 flush policy 决定是否等待 redo flush。
6. 将修改过的 buffer frame 标为 dirty，并记录 pageLSN。
7. 释放 memo stack 中的 latch、buffer fix、reservation。
8. 状态改为 `COMMITTED`。

写数据页到表空间文件不能早于对应 redo 可恢复，这是 write-ahead logging 的约束。

## 10. Buffer Pool 与 Disk Manager 的关系

`BufferPool` 不属于纯磁盘空间分配，但必须作为协作模块出现：

- `fsp` 通过 `BufferPool.getPage(pageId, latchMode, mtr)` 获取受 MTR 管理的页。
- `buf` 负责读页、创建页、frame 生命周期、LRU、dirty list。
- `mtr` 负责把 latch 和 buffer fix 放入 memo。
- `flush` 从 dirty list 选择页，先写 doublewrite，再写 tablespace data file。

关键边界：

- `fsp` 不直接持有 `BufferFrame` 生命周期。
- `buf` 不理解 segment 或 extent 语义。
- `fil` 不理解 page 内容，只负责按 `PageId` 定位文件偏移。

## 11. Flush、Doublewrite 与 Checkpoint

`FlushCoordinator` 负责刷脏：

1. 从 `DirtyPageQueue` 选择 page。
2. 确保 pageLSN 对应 redo 已经刷盘。
3. 调用 `DoublewriteStrategy.beforeWrite(page)`。
4. 写入 tablespace data file。
5. 调用 `DoublewriteStrategy.afterWrite(page)`。
6. 更新 checkpoint 候选 LSN。

`DoublewriteStrategy`：

- `NoDoublewriteStrategy`：测试环境或低可靠性模式。
- `DetectOnlyDoublewriteStrategy`：只写 metadata，用于检测 partial write。
- `RecoverableDoublewriteStrategy`：写完整 page 副本，恢复时可修复 torn page。

`CheckpointCoordinator` 实现 fuzzy checkpoint：

- 不一次性刷完整 buffer pool。
- 记录 checkpoint LSN。
- recovery 从 checkpoint LSN 后扫描 redo。

## 12. Recovery 设计

恢复流程见 [data-flow.mmd](diagrams/data-flow.mmd)。

启动恢复：

1. 读取 checkpoint label。
2. 加载 tablespace registry。
3. 扫描 redo log，过滤 checkpoint LSN 后的 record。
4. 对每条 redo record 找到 `RedoRecordHandler`。
5. handler 获取目标 page 或 space metadata。
6. 若 pageLSN 已经大于等于 record LSN，跳过，保证幂等。
7. 应用 redo。
8. 修复可能的 partial write：优先 doublewrite 副本，其次 redo 重放。
9. 重建内存态 free list cache、dirty state、tablespace state。

恢复处理器分组：

- `PageInitRedoHandler`
- `SpaceHeaderRedoHandler`
- `XdesRedoHandler`
- `SegmentInodeRedoHandler`
- `PageBytesRedoHandler`
- `TablespaceLifecycleRedoHandler`

## 13. API 设计

### 13.1 DiskSpaceManager

对上层暴露的门面：

- 创建/打开/关闭 tablespace。
- 创建/删除 segment。
- 为 segment 分配 page。
- 释放 page。
- 查询 tablespace 使用率。
- 查询 segment 统计信息。
- 创建 `PageCursor` 访问 page。

返回值使用领域对象，不返回裸页号或 byte array。

### 13.2 MiniTransactionManager

职责：

- `begin()`
- `beginReadOnly()`
- `beginSync()`
- `current()`
- `commit(mtr)`
- `rollbackUncommitted(mtr)`

MTR 绑定当前线程，但不允许静默嵌套。需要嵌套时必须显式创建 child MTR，并继承 parent 的 log free check 约束。

### 13.3 PageCursor

`PageCursor` 是页内容访问的类型化入口：

- `readInt(offset)`
- `writeInt(offset, value)`
- `readBytes(offset, length)`
- `writeBytes(offset, bytes)`
- `pageType()`
- `pageLsn()`

写操作必须要求 MTR 持有 X latch，并自动生成 `WRITE_PAGE_BYTES` redo。

## 14. 设计模式使用清单

- Facade：`DiskSpaceManager` 聚合 `fsp`、`fil`、`buf`、`mtr`。
- Strategy：`ExtentAllocationPolicy`、`AutoExtendPolicy`、`DoublewriteStrategy`、`ChecksumStrategy`。
- Repository：`SpaceHeaderRepository`、`ExtentDescriptorRepository`、`SegmentInodeRepository`、`TablespaceRegistry`。
- Unit of Work：`MiniTransaction` 收集短事务内修改和资源。
- Command：`RedoRecord` 同时支持编码和恢复应用。
- Template Method：`PageOperationTemplate` 固定 `begin MTR -> latch -> modify -> redo -> commit` 流程。
- Adapter：`FileChannelPageStore`、`MappedByteBufferPageStore`、`PreallocatingPageStore`。
- Observer：`FlushObserver`、`CheckpointObserver` 接收 MTR commit 和 flush 事件。
- State：`TablespaceState`、`MiniTransactionState`、`ExtentState`。
- Factory：`TablespaceFactory`、`PageFactory`、`SegmentFactory`。

## 15. 高内聚、低耦合约束

强制规则：

- `storage.fsp` 不能 import `storage.record`、`storage.btree`、`storage.sql`。
- `storage.fil` 不能 import `storage.fsp`。
- `storage.redo` 的 record 定义不能依赖具体 repository 实现。
- `storage.record` 不能直接修改 extent bitmap、segment inode 或 tablespace file。
- `storage.buf` 不能判断 extent 或 segment 状态。
- `PageCursor` 不能绕过 MTR 写页。
- 所有 segment/extent/page 修改必须产生 redo，除非 MTR log mode 是明确的 `NO_REDO` 且 page 属于 temporary tablespace。
- 表空间扩展必须先写 allocation redo，再进行物理 page 初始化。
- `AutoExtendPolicy` 的默认边界必须按 MySQL 8.0 手册语义实现；边界页数用单元测试固定，避免后续实现中误写成模糊的闭区间。

推荐包依赖方向：

`api -> fsp -> buf -> fil -> io`  
`fil -> redo`（仅文件操作 redo：create/rename/delete/extend）  
`mtr -> redo`  
`flush -> buf + fil + redo`  
`recovery -> redo + fil + fsp + buf`

## 16. 典型数据流

### 16.1 查找记录的数据链路

查找路径不一定修改磁盘，但它定义了页、段、Buffer Pool 的访问边界：

1. 由表定义找到聚簇索引 root page no。
2. `PageStore` 按 `pageOffset = pageNo * pageSize` 定位物理页；如果 Buffer Pool 命中则不读盘。
3. root page 属于 non-leaf segment，`PAGE_LEVEL >= 1`。
4. 在 non-leaf page 内查找子页指针，逐层下降。
5. 到达 `PAGE_LEVEL = 0` 的 leaf page；leaf page 属于 leaf segment。
6. 页内先二分 `PageDirectory`，找到覆盖目标 key 的 group。
7. 从上一槽对应记录开始沿 `next_record` 扫描，组内最多少量记录。
8. 命中目标记录或确认不存在。

关键结论：

- 跨页查找走 B+Tree；页内查找走 `PageDirectory + next_record`。
- physical page no 顺序和 leaf page 逻辑顺序是两回事；范围扫描依赖 leaf page 的 `FIL_PAGE_PREV/NEXT` 以及 leaf segment 尽量连续的 extent。

### 16.2 插入记录的数据链路

普通插入先尽量在当前 leaf page 内完成：

1. 自顶向下定位 leaf page。
2. 页内二分找到插入位置。
3. 优先复用 `PAGE_FREE` 垃圾链表中的空间；否则从 `PAGE_HEAP_TOP` 所指 free space append。
4. 写入记录，分配新的 `heap_no`。`heap_no` 按插入顺序增长，不代表 key 顺序。
5. 调整前后记录的 `next_record` 指针。
6. 更新所属 group 的 `n_owned`；超过上限时拆分 group 并新增 directory slot。
7. 更新 `PAGE_N_RECS`、`PAGE_LAST_INSERT`、`PAGE_DIRECTION` 等 page header 字段。

空间不足时分两级处理：

1. 页内重组：清理 `PAGE_GARBAGE` 标记的空间，整理记录和 directory slot，再尝试插入。
2. 页分裂：向所属 segment 申请新 page，移动部分记录，调整 leaf page 的 `FIL_PAGE_PREV/NEXT`，并向 non-leaf segment 插入上层索引项，可能级联分裂。

顺序插入可走右分裂优化，尽量让旧页保持高填充率；随机主键更容易触发半页搬迁和物理碎片。

### 16.3 B+Tree split 分配新页

1. B+Tree 层调用 `MiniTransactionManager.begin()`。
2. 调用 `SpaceReservationService.reserve(NORMAL, pages=2, extents=1)`。
3. 调用 `DiskSpaceManager.allocatePage(index.leafSegment, hint, UP, mtr)`。
4. `SegmentPageAllocator` 获取 space header、inode、xdes 页 latch。
5. 分配 fragment page 或 extent page。
6. 初始化新 page。
7. 追加 `ALLOCATE_PAGE`、`UPDATE_XDES`、`UPDATE_SEGMENT_INODE`、`INIT_FILE_PAGE` redo。
8. B+Tree 层写入 page 内容，`PageCursor` 追加 `WRITE_PAGE_BYTES` redo。
9. MTR commit 写 redo、标脏、释放 latch。

### 16.4 删除记录与释放空页

InnoDB 风格删除分两阶段：

1. 同步阶段：事务执行时定位到 leaf page 和目标记录，将记录头 `deleted_flag` 置为 1，记录仍留在原位。
2. 同步阶段同时写 undo，供 MVCC 一致性读和事务回滚使用。
3. 异步阶段：purge 确认没有活跃快照需要旧版本后，真正从 `next_record` 链表摘除记录。
4. 被清理空间挂入 `PAGE_FREE`，`PAGE_GARBAGE` 累加，`n_owned` 减少；group 过小时合并 directory slot。
5. 如果 B+Tree merge 后发现页可释放，MTR 获取目标 page X latch。
6. `DiskSpaceManager.freePage(pageId, mtr)` 更新 xdes bitmap 和 segment inode。
7. 如果 extent 变为空，移动到 free list。
8. 追加 redo。
9. commit 后 page 不再可被普通读取路径返回。

### 16.5 修改记录的数据链路

更新分两类：

1. 原地更新：未修改主键，且新记录长度不变或变小。只覆盖列值，写 undo，更新隐藏列 `DB_TRX_ID`、`DB_ROLL_PTR`，不调整链表和 directory slot。
2. 非原地更新：修改主键，或新记录变长且原位放不下。旧记录走 delete-mark，新记录走完整 insert 链路，可能落到不同 leaf page，并可能触发 page split。

二级索引列值变化时按 delete-mark + insert 处理，因为 key 变化意味着索引位置变化。

### 16.6 崩溃恢复 page allocation

1. 读取 checkpoint LSN。
2. 扫描 redo。
3. 发现 `ALLOCATE_PAGE`。
4. 如果目标 pageLSN 小于 record LSN，重放 allocation。
5. 初始化 page 或修复 header。
6. 继续重放后续 `WRITE_PAGE_BYTES`。

## 17. 异常处理

异常类型：

- `TablespaceNotFoundException`
- `TablespaceCorruptedException`
- `NoFreeSpaceException`
- `PageChecksumMismatchException`
- `MtrStateException`
- `RedoLogFullException`
- `RecoveryRequiredException`

错误策略：

- 可恢复错误返回明确异常，不吞掉。
- 页校验失败时先尝试 doublewrite/recovery；失败后标记 tablespace `CORRUPTED`。
- 空间不足时只自动扩展一次；仍失败则抛出 `NoFreeSpaceException`。
- MTR commit 失败时不允许继续复用该 MTR。
- recovery 期间所有 redo handler 必须幂等。

## 18. 并发与锁顺序

磁盘管理包含两类锁：`storage.fil` 的物理文件锁，以及 `storage.fsp`/`storage.buf` 的表空间和 page latch。物理文件锁保护文件句柄、文件长度和 IO 生命周期；page latch 保护页内容和空间管理元数据。

锁层级从大到小：

1. `TablespaceLifecycleLatch`
2. `DataFileHandleLock`
3. `FileSizeLock`
4. Tablespace latch
5. Space header page latch
6. XDES page latch
7. Segment inode page latch
8. Data page latch

规则：

- 必须按层级加锁，释放由 MTR memo LIFO 负责。
- 同层多个 page 按 `PageId` 排序加锁，避免死锁。
- 读路径使用 S latch；元数据修改使用 X latch；只更新 space-level 状态时可用 SX latch。
- 长事务锁不进入 MTR memo；MTR 只管理物理短临界区。
- page allocation 允许在持有 page latch 时触发表空间扩展，但扩展路径不能反向等待业务 page latch。
- flush 写出 page 时不能持有 space header、XDES 或 inode page latch。
- drop/truncate/discard 必须先拿 `TablespaceLifecycleLatch(X)`，再阻止 Buffer Pool 返回普通 page handle。
- 任何等待行锁或事务锁的路径都不能持有物理文件锁或空间管理 page latch。

## 19. 测试设计

虽然本次不写代码，后续实现时应按以下层次测试：

- 值对象测试：page/extent 边界、page size 换算。
- repository 测试：space header、xdes、inode 页读写。
- allocation policy 测试：前 32 页 fragment 分配、超过 32 页 extent 分配、大 segment 最多 4 extent。
- MTR 测试：memo LIFO 释放、savepoint 提前释放、commit LSN、log mode。
- crash recovery 测试：redo 重放 page allocation、幂等 replay、partial write 修复。
- 并发测试：锁顺序、并发分配、并发释放、autoextend 与 flush 并发、drop/truncate 与 page IO 互斥。
- property-based 测试：随机分配/释放后 free list、bitmap、segment inode 计数一致。

## 20. 后续实现顺序

推荐分阶段实现：

1. `domain` 值对象和配置。
2. `fil` page store 与 tablespace registry。
3. `buf` 最小 buffer pool、page latch。
4. `mtr` memo stack、redo collector、commit skeleton。
5. `fsp` space header、xdes、segment inode repository。
6. segment/extent/page allocation。
7. redo writer 与 recovery applier。
8. flush、checkpoint、doublewrite。
9. 并发和故障注入测试。

## 22. GENERAL 表空间 DISCARD/IMPORT v1

DISCARD/IMPORT 是 DD 与 storage API 共同编排的控制面操作。DDL log v3 持久化 canonical path、
quarantine/source path 和 `TablespaceFileIdentity`；page0 继续使用 v1 `NORMAL/DISCARDED` marker，
不扩张 page0 格式。DISCARD 在 WAL-safe marker、BufferPool drain/invalidate、PageStore close 后
以 `ATOMIC_MOVE` 移入受控 discarded 目录；IMPORT 校验 page0 envelope/checksum、SpaceId、页大小、
类型、lifecycle 和 spaceVersion，复制到临时目标后原子发布，写回 NORMAL 并递增 spaceVersion。
DISCARDED/pending DD 状态不进入普通 discovery，启动由 DDL recovery 按双路径 identity 续作；
quarantine 目录不参与普通 orphan cleanup。跨设备 rename 和目录 fsync 不作为 v1 强保证，
文件 force、DDL phase durable 边界和 fail-closed 双路径裁决共同保证可重试恢复。

## 21. 十五轮自检记录

本节记录参考本地 HTML 后的十五轮实际自检结果。

| 轮次 | 检查主题 | 检查结果 |
| --- | --- | --- |
| 1 | 目标文件存在 | 主文档和三个 Mermaid 图文件均存在 |
| 2 | 本地参考文件存在且被引用 | 已引用 `InnoDB存储结构与数据链路.html` |
| 3 | 错误层次链清除 | 不再出现把 extent 放在 segment 上层的错误链路 |
| 4 | 物理/逻辑双视角 | 已区分物理 `tablespace -> extent -> page` 与逻辑 `tablespace -> segment -> extent/page` |
| 5 | MySQL 8.0 基线 | 已标明 8.0.46，并保留官方手册与源码文档链接 |
| 6 | 首区固定管理页 | 已覆盖 `FSP_HDR`、`IBUF_BITMAP`、`INODE`、`SDI`、page 4+ |
| 7 | XDES 表述 | 已说明 XDES 是 descriptor entry 集合，首批嵌在 page 0 |
| 8 | Segment 逻辑归属 | 已说明 segment 由 INODE entry 描述，不是连续物理区域 |
| 9 | Extent 状态和链表 | 已覆盖全局 extent list 与 segment 内 extent list |
| 10 | 页内结构 | 已覆盖 FileHeader、PageHeader、Infimum/Supremum、UserRecords、FreeSpace、PageDirectory、FileTrailer |
| 11 | 记录头与 Page Directory | 已覆盖 `heap_no`、`n_owned`、`next_record`、4 到 8 条分组、槽不存 key 前缀 |
| 12 | 查找链路 | 已覆盖 root、non-leaf、leaf、Page Directory 二分和链表扫描 |
| 13 | 插入链路 | 已覆盖 `PAGE_FREE`、`PAGE_HEAP_TOP`、`PAGE_GARBAGE`、页内重组、页分裂和右分裂优化 |
| 14 | 删除/修改链路 | 已覆盖 delete-mark、purge、undo、原地更新、delete-mark + insert |
| 15 | 占位符与图渲染 | 常见占位标记命中为 0，三个 Mermaid 图均可渲染 |

## 22. 参考链接

- 本地参考 - InnoDB 存储结构与增删改查数据链路: `C:\coding\java\self\miniproject\miniproject\mysql\InnoDB存储结构与数据链路.html`
- MySQL 8.0 Reference Manual - File Space Management: https://dev.mysql.com/doc/refman/8.0/en/innodb-file-space.html
- MySQL 8.0 Reference Manual - Tablespace AUTOEXTEND_SIZE Configuration: https://dev.mysql.com/doc/refman/8.0/en/innodb-tablespace-autoextend-size.html
- MySQL 8.0 Reference Manual - Undo Tablespaces: https://dev.mysql.com/doc/refman/8.0/en/innodb-undo-tablespaces.html
- MySQL 8.0 Reference Manual - Optimizing Tablespace Space Allocation on Linux: https://dev.mysql.com/doc/refman/8.0/en/innodb-optimize-tablespace-page-allocation.html
- MySQL 8.0 Reference Manual - Redo Log: https://dev.mysql.com/doc/refman/8.0/en/innodb-redo-log.html
- MySQL 8.0 Reference Manual - Doublewrite Buffer: https://dev.mysql.com/doc/refman/8.0/en/innodb-doublewrite-buffer.html
- MySQL 8.0 Reference Manual - InnoDB Checkpoints: https://dev.mysql.com/doc/refman/8.0/en/innodb-checkpoints.html
- MySQL 8.0.46 Source Documentation - `fsp0fsp.h`: https://dev.mysql.com/doc/dev/mysql-server/8.0.46/fsp0fsp_8h.html
- MySQL 8.0.46 Source Documentation - `mtr0mtr.h`: https://dev.mysql.com/doc/dev/mysql-server/8.0.46/mtr0mtr_8h.html
- MySQL 8.0.46 Source Documentation - `mtr_t`: https://dev.mysql.com/doc/dev/mysql-server/8.0.46/structmtr__t.html
- MySQL 8.0.46 Source Documentation - `mtr_memo_slot_t`: https://dev.mysql.com/doc/dev/mysql-server/8.0.46/structmtr__memo__slot__t.html
- MySQL 8.0.46 Source Documentation - `buf0buf.h`: https://dev.mysql.com/doc/dev/mysql-server/8.0.46/buf0buf_8h.html
