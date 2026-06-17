# Spec：fil 物理 IO 层（首版切片）

- 日期：2026-06-10
- 关联设计：`C:\coding\java\self\minimysqldesign\docs\innodb-disk-manager-design.md`（§5、§6.1、§8、§8.1、§10、§14、§15、§17、§18、§20）
- 上游约束：本仓库 `AGENTS.md` / `CLAUDE.md`
- 状态：已通过 brainstorming 评审；spec 自查后做了 3 处修正（见 §10），待用户复核

## 1. 背景与目标

`storage.fil` 已完成「逻辑元数据」部分（`TablespaceRegistry` / `CachingTablespaceRegistry` / `TablespaceMetadataLoader` / `Tablespace` 等）。本切片实现 `fil` 缺失的「物理 IO」部分（设计 §20 推荐顺序第 2 步剩余内容）：按 `PageId` 定位文件偏移、positional 读写整页、autoextend 扩展文件，并落地 §8.1 物理文件锁的**核心子集**。

目标：让上层（未来 `buf` / `fsp` / `recovery`）能通过稳定的 `PageStore` 接口读写物理页，而不接触 `FileChannel`、文件长度或平台差异。

## 2. 范围与非目标（物理 vs 逻辑边界）

**本切片只做物理视角**：`PageId(SpaceId + PageNo) → 文件偏移 → 整页字节`。

明确**不做**（保持物理/逻辑分离，§10）：

- 不解析页内容（不读 `FilePageHeader`/`FileTrailer`、不懂 record/segment/extent）。
- 不算 checksum、不做 partial-write 修复、不做 doublewrite。
- 不产 redo；不保证崩溃持久化与 WAL ordering（留 redo/flush 切片）。本版只保证**进程内**写后可读、扩展后可读。
- 不懂 segment/extent/XDES/INODE 分配——那是后续 `fsp`（逻辑视角）。
- 不实现 mmap / 预分配 adapter（接口留点）。
- 不实现物理文件锁 #2/#4/#5 实体（留接口 + 简化点）。
- 不支持多文件表空间跨文件路由（先单文件；`DataFileDescriptor` 已留 `startPageNo` 扩展点）。
- 不实现 Configured/Undo/FixedSize autoextend 策略（接口留点）。
- **不在物理层做逻辑状态准入**（NORMAL/ACTIVE 白名单），理由见下。

### 2.1 关键取舍：PageStore 与逻辑层的关系（B′）

物理句柄放哪？候选：
- (A) 把 `DataFileHandle` 塞进 `TablespaceHandle`。
- (B) `PageStore` 自维护 `SpaceId→DataFileHandle`，**靠 `registry` 解析元数据并复用 require 白名单**。
- (B′) `PageStore` 自维护 `SpaceId→DataFileHandle`，但**完全 registry-无关、state-无关**：通过显式 `create/open(spaceId, path, pageSize, ...)` 注入元数据并登记句柄；IO 只按 spaceId 查已开句柄。

**选 B′。** 否决 B 的原因是一个会致 bug 的分层错误：`require` 的 NORMAL/ACTIVE 白名单是**逻辑访问策略**，若塞进物理 `readPage/writePage`，会挡死 **recovery 写 CORRUPTED 表空间**、**初始化写 EMPTY 表空间**（前序工作已把 EMPTY/INACTIVE 设为普通 IO 不可达）。物理层（§10「只负责按 PageId 定位文件偏移」）不应感知逻辑状态。

因此：
- `PageStore` 不依赖 `TablespaceRegistry`，不读 `TablespaceState`。pageSize/path 由编排方在 `create/open` 时注入。
- NORMAL/ACTIVE 白名单仍是 `registry.require` 的**逻辑闸**，由未来上层逻辑调用方（storage.api/fsp）在发起普通 IO 前自行把关。
- 编排关系（谁在 open 时从 registry 取 path/pageSize、autoextend 后如何回写 `Tablespace` 快照）属逻辑层，留后续切片（§11）。

## 3. 包与依赖方向

- 全部新增类位于 `cn.zhangyis.db.storage.fil`。
- 依赖方向：`fil → domain`、`fil → common.exception`、`fil → java.nio`、`java.util.concurrent`。**禁止** import `fsp`/`buf`/上层（§15）。
- **PageStore 不再依赖 registry**，分层更干净。
- 日志用 Lombok `@Slf4j`（AGENTS.md）。

## 4. 组件设计

### 4.1 `PageStore`（接口，Facade/Adapter seam，`AutoCloseable`）

```
public interface PageStore extends AutoCloseable {
    /** 创建表空间物理文件并登记句柄。文件必须不存在，否则 DataFilePhysicalException。按 initialSizeInPages 零填充。 */
    void create(SpaceId spaceId, Path path, PageSize pageSize, PageNo initialSizeInPages);

    /** 打开已存在的表空间物理文件并登记句柄。文件必须存在且整页对齐；size 由文件长度推导。 */
    void open(SpaceId spaceId, Path path, PageSize pageSize);

    /** 读取整页到 dst；dst.remaining() 必须 == pageSize。未 open/create 抛领域异常；越界抛 PageOutOfBoundsException。 */
    void readPage(PageId pageId, ByteBuffer dst);

    /** 把 src 整页写入；src.remaining() 必须 == pageSize。同页并发写串行化是上层 page latch 的责任，本层不串行化。 */
    void writePage(PageId pageId, ByteBuffer src);

    /** 对表空间执行一次 AutoExtendPolicy 增量扩展，返回扩展后的 currentSizeInPages。 */
    PageNo extend(SpaceId spaceId);

    /** 查询当前物理文件大小页数（越界检查与 fsp 分配的物理依据）。 */
    PageNo currentSizeInPages(SpaceId spaceId);

    /** 关闭并注销单个表空间句柄。 */
    void close(SpaceId spaceId);

    /** 关闭全部句柄。 */
    @Override void close();
}
```

约束：

- `ByteBuffer` 契约：`remaining() == pageSize`，否则 `DatabaseValidationException`；读写**恰好** pageSize 字节。
- 偏移用现有 `PageId.offset(PageSize)`（`Math.multiplyExact`，已防溢出），不另写。
- `readPage/writePage/extend/currentSizeInPages` 要求句柄已登记，否则 `TablespaceNotOpenException`（领域异常）。

### 4.2 `FileChannelPageStore`（实现）

- 字段：
  - `private final AutoExtendPolicy autoExtendPolicy;`（默认注入 `DefaultIbdAutoExtendPolicy`）
  - `private final ConcurrentMap<SpaceId, DataFileHandle> handles;`
- `create/open`：构造 `DataFileHandle` 后 `handles.putIfAbsent`；重复登记同一 spaceId 抛领域异常（或幂等校验一致性——本版直接拒绝重复，简化）。
- `readPage/writePage/extend/currentSizeInPages`：`handles.get(spaceId)`，缺失抛 `TablespaceNotOpenException`，否则委托。
- `close()`：遍历关闭全部句柄。
- mmap/预分配作为未来 adapter，类级 Javadoc 标简化点。

### 4.3 `DataFileHandle`（每表空间一个物理单元，`AutoCloseable`）

封装单个表空间数据文件的物理状态与锁，真正做 IO 的地方。

- 字段（均需中文 Javadoc，写明语义/owner/保护它的锁）：
  - `private final SpaceId spaceId;` / `private final Path path;` / `private final PageSize pageSize;`
  - `private final FileChannel channel;`（构造时打开，整生命周期持有；句柄替换是 #2 的职责，本版不替换）
  - `private final TablespaceLifecycleLatch lifecycleLatch;`（#1，S/X）
  - `private final FileSizeLock fileSizeLock;`（#3，X）
  - `private volatile long currentSizeInPages;`（权威物理大小；读路径读该 volatile 快照，扩展在 `fileSizeLock` 下零填充后再 volatile 发布，保证 happens-before 且新页发布前不可见）
  - `private volatile boolean closed;`（close() 在 lifecycle(X) 下置位；IO 路径在 lifecycle(S) 下检查，已关闭抛领域异常，防止用已关 channel）
- 工厂：
  - `static DataFileHandle create(SpaceId, Path, PageSize, PageNo initialSizeInPages)`：文件**必须不存在**（`Files.exists` 为真→`DataFilePhysicalException`）；创建，零填充 `initialSize` 页，`size = initialSize`。
  - `static DataFileHandle open(SpaceId, Path, PageSize)`：文件**必须存在**；`fileLength % pageSizeBytes != 0`→`DataFileCorruptedException`（物理损坏：非整页对齐）；`size = fileLength / pageSizeBytes`。
- 方法：
  - `void readPage(PageNo, ByteBuffer dst)`：`lifecycleLatch(S)` → 检 `closed` → 读 volatile size → `pageNo >= size`→`PageOutOfBoundsException` → `readFully(dst, offset)` →（try/finally 释放 S）。
  - `void writePage(PageNo, ByteBuffer src)`：`lifecycleLatch(S)` → 检 `closed` → 越界检查 → `writeFully(src, offset)`。
  - `long autoExtend(AutoExtendPolicy)`：`lifecycleLatch(S)` + `fileSizeLock(X)` → 检 `closed` → `inc = policy.nextIncrementPages(currentSizeInPages, pageSize)` → 对 `[oldSize, oldSize+inc)` 写零页 → volatile 发布 `currentSizeInPages = oldSize+inc` → 返回新 size（try/finally 逆序释放）。**不 force**（持久化留后续）。
  - `void close()`：`lifecycleLatch(X)`（写锁获取即 drain 掉所有 S 持有者）→ `closed = true` → 关 `channel`。
  - `private readFully/writeFully(ByteBuffer, long offset)`：positional `channel.read/write` 循环直到 pageSize 字节；EOF 仍不足→`DataFilePhysicalException`（包 IOException 作 cause）。
  - 零填充可用一个复用的 `pageSize` 字节零缓冲循环写，避免逐页分配。
- IO 异常统一包 `DataFilePhysicalException`（保留 `IOException` 根因）。

### 4.4 `TablespaceLifecycleLatch`（#1，S/X）

- 基于 `ReentrantReadWriteLock`（禁 `synchronized`）。
- API（RAII Guard，配合 try-with-resources）：`AutoCloseable acquireShared()` / `acquireExclusive()`，`close()` 释放对应锁。
- Javadoc：保护 open/close/discard/drop/truncate 与普通 IO 的生命周期关系；不进事务死锁检测。

### 4.5 `FileSizeLock`（#3，X-only）

- 基于 `ReentrantLock`。API：`AutoCloseable acquire()`。
- Javadoc：保护 `currentSizeInPages` 与文件尾零填充；持有它时**不得等待任何 page latch**（§8.1）。

### 4.6 预留锁接口（#2/#4/#5，留接口 + 简化点，无实现）

- `DataFileHandleLock`、`PageIoRangeLock`、`FsyncLock`：占位接口/类型，仅 Javadoc 说明职责、为何本版不启用、在加锁顺序中的位置（`Lifecycle → DataFileHandle → FileSize → PageIoRange → Fsync`）。

### 4.7 `AutoExtendPolicy`（接口，Strategy）+ `DefaultIbdAutoExtendPolicy`

```
public interface AutoExtendPolicy {
    /** 给定当前大小和页大小，返回本次扩展的页数（>=1）。 */
    long nextIncrementPages(long currentSizeInPages, PageSize pageSize);
}
```

`DefaultIbdAutoExtendPolicy`（MySQL 8.0 file-per-table/general 语义，§8），**直接用现有 `pageSize.pagesPerExtent()`**：

- 令 `ppe = pageSize.pagesPerExtent()`。
- `currentSizeInPages < ppe` → 返回 `1`（逐页）。
- `ppe <= currentSizeInPages < 32 * ppe` → 返回 `ppe`（1 个 extent）。
- `currentSizeInPages >= 32 * ppe` → 返回 `4L * ppe`（4 个 extent）。

`pagesPerExtent` 期望值（由 `PageSize` 既有实现保证，单测复核，§15）：4KB→256、8KB→128、16KB→64、32KB→64、64KB→64。Configured/Undo/FixedSize 仅接口预留 + 简化点。

## 5. 数据流

- **create/open**：编排方（测试/未来 fsp/recovery）注入 `(spaceId, path, pageSize[, initialSize])` → 构造 `DataFileHandle`（创建零填充或按文件长度推导 size）→ 登记 `handles`。
- **读页**：`readPage(pageId)` → `handles.get(spaceId)`（缺失→`TablespaceNotOpenException`）→ `DataFileHandle.readPage` → `lifecycle(S)` → 检 closed → volatile size 越界检查 → `pageId.offset(pageSize)` → `readFully` → 释放 S。
- **写页**：同上路径，`writePage` → `lifecycle(S)` → 检 closed → 越界检查 → `writeFully`。
- **autoextend**：`extend(spaceId)` → 路由 → `DataFileHandle.autoExtend(policy)` → `lifecycle(S)+fileSize(X)` → 增量→零填充→volatile 发布→返回。
- **close**：`lifecycle(X)` drain → 置 closed → 关 channel。

## 6. 并发与锁语义

- 本切片锁层级只两层：`TablespaceLifecycleLatch → FileSizeLock`（§18 子集）。
- 普通 read/write 持 Lifecycle(S)，可并发；autoextend 持 Lifecycle(S)+FileSize(X)，与读并发、互斥于其它扩展；close 持 Lifecycle(X)，排他于一切 IO。
- `currentSizeInPages` volatile 发布：读无锁读快照，扩展在 FileSize(X) 下零填充后再 volatile 写，保证「新页发布前对读不可见」且单调增长。
- **同页并发写不在本层串行化**——由上层 Buffer Pool page latch / flush snapshot 负责（§8.1）。本层测试不做同页并发写。
- 每个共享状态的 owner/保护锁/释放路径在字段 Javadoc 写明。全部锁 try/finally 或 try-with-resources 释放；持 FileSize(X) 不等 page latch；物理锁不进 Wait-For Graph，等待靠 timeout/IO error/drain。

## 7. 异常（沿用项目异常层次，禁裸 RuntimeException/IllegalArgumentException）

- `PageOutOfBoundsException extends DatabaseRuntimeException`：pageNo 越过 currentSize；可恢复（调用方先 extend 再重试）。
- `TablespaceNotOpenException extends DatabaseRuntimeException`：对未 open/create 的 spaceId 发起 IO。
- `DataFilePhysicalException extends DatabaseRuntimeException`：positional IO/扩展/创建 IO 失败，包 `IOException` 作 cause。
- `DataFileCorruptedException extends DatabaseFatalException`：文件长度非整页对齐等物理结构损坏，致命。
- 参数/缓冲长度非法 → 复用 `DatabaseValidationException`。
- **不在本切片**：`NoFreeSpaceException`（属 `fsp` 分配语义，见 §10）。

## 8. 测试计划（JUnit Jupiter，临时目录真实文件，`@TempDir` 结束清理）

- `DefaultIbdAutoExtendPolicyTest`：5 种 pageSize 的 `pagesPerExtent` 复核；三档边界（`<1ext→1`、`=ppe`/`<32ext→ppe`、`=32*ppe`/`>32ext→4*ppe`）；临界值（`ppe`、`32*ppe`）。
- `DataFileHandleTest`：
  - round-trip：create→writePage→readPage 字节一致。
  - 越界读/写 → `PageOutOfBoundsException`。
  - 缓冲 `remaining() != pageSize` → `DatabaseValidationException`。
  - autoExtend：新 size == old + 增量；新页可读且为零；扩展前读新页越界、扩展后可读（验证发布前不可见）。
  - `create` 对已存在文件 → `DataFilePhysicalException`；`open` 对缺失文件 → `DataFilePhysicalException`；`open` 非整页对齐文件 → `DataFileCorruptedException`。
  - close 后再 IO → 领域异常。
  - 并发：N 线程并发 readPage 已有页 + 1 线程多次 autoExtend，断言无异常、size 单调、并发读不越界。
- `FileChannelPageStoreTest`（**不依赖 registry**）：
  - create→write→read 经门面字节一致；extend 经门面返回新 size 且后续可读新页。
  - 对未 open 的 spaceId 读写 → `TablespaceNotOpenException`。
  - 重复 create/open 同一 spaceId → 领域异常。
  - `close(spaceId)` 后该 spaceId IO → `TablespaceNotOpenException`。

  注：物理层每个 spaceId 对应单个文件（`create/open` 只收一个 `Path`）；多文件表空间的跨文件路由属编排层，不在本层测试。

## 9. 简化点（注释中标注，与设计文档差异）

- 物理文件锁只实现 #1/#3；#2/#4/#5 留接口。
- 单文件表空间；多文件路由未实现。
- 不算 checksum、不做 partial-write/doublewrite、不产 redo、不保证崩溃持久化（不 force）。
- autoextend 只实现 `DefaultIbdAutoExtendPolicy`。
- mmap/预分配 adapter 未实现。

## 10. 自查修正记录（相对 brainstorming 口头设计）

1. **取舍 B→B′（重要）**：物理 `PageStore` 不再依赖 `TablespaceRegistry`、不读 `TablespaceState`、不在物理层做 NORMAL/ACTIVE 白名单。原因：白名单是逻辑访问策略，塞进物理层会挡死 recovery 写 CORRUPTED / 初始化写 EMPTY，且违反物理/逻辑分离。改为显式 `create/open` 注入元数据；白名单仍由 registry 留给上层逻辑调用方。
2. **复用既有 domain 能力**：`pagesPerExtent` 用现成的 `PageSize.pagesPerExtent()`（已实现），不在策略内重算；偏移用现成 `PageId.offset(PageSize)`。删去「提升到 PageSize」简化点（已无意义）。
3. **持久化与契约细节**：autoExtend 去掉 `force`（崩溃持久化/WAL 留 redo 切片，避免 `force(false)` 不刷元数据的半正确）；新增 `volatile closed` 关闭后 IO 抛异常；`create` vs `open` 显式区分避免读路径隐式建文件掩盖缺失；明确同页并发写串行化属上层 page latch。
4. **延续上一版细化点**：`PageStore.extend` 为单次增量原语 `extend(SpaceId)→新size`，不带 target；本切片不引入 `NoFreeSpaceException`（下沉 fsp）；越界用 `PageOutOfBoundsException`，调用方先 extend 再重试。

## 11. 后续切片衔接

- `fsp` 逻辑分配层消费 `PageStore`（物理）+ `buf`（受控页访问）实现 segment/extent/page 分配，引入 `NoFreeSpaceException`、reservation，并负责 open 时从 `registry` 取 path/pageSize、autoextend 后回写 `Tablespace` 快照。
- 普通 IO 的 NORMAL/ACTIVE 白名单由 storage.api/fsp 在调 `PageStore` 前经 `registry.require` 把关。
- `DataFileHandle` 后续挂 #2 `DataFileHandleLock`（句柄替换）、recovery 的 partial-write 修复、fil create/extend redo 与崩溃持久化。
