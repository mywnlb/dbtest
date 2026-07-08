# Disk Manager 0.16b/0.16c Design

## Goal

落地 storage backlog 中 disk-manager 0.16b 与 0.16c：补齐 GENERAL 表空间 `CORRUPTED` 状态的 page0 持久化与普通打开校验，并把 `DataFileHandle` 中的文件增长/初始化逻辑抽象为 `DataFileGateway` / `PreallocationStrategy`。本设计保持 storage 内闭环，不引入 DD、DDL、SQL/session/executor 依赖。

## Context

- `docs/design/storage-backlog.md` 将 0.16b 定义为普通 GENERAL `CORRUPTED` 状态持久化到 page0 + 普通打开校验。
- `docs/design/storage-backlog.md` 将 0.16c 定义为 `DataFileGateway` / `PreallocationStrategy`，Windows 下可降级零填充。
- `docs/design/current-implementation-map.md` 当前记录：UNDO lifecycle 已持久化；GENERAL `markCorrupted/discard` 仍 runtime-only；`DataFileHandle` 直接封装 `FileChannel` 并循环零填充。
- `docs/design/innodb-disk-manager-design.md` §8/§8.1 要求文件扩展由 `DataFileGateway` 完成，并保持物理锁顺序 `Lifecycle -> FileSize -> Fsync`。

## Non-Goals

- 不实现 `DISCARDED` / DROP / DISCARD 持久化和文件删除/重建编排；这些依赖 DDL 生命周期，仍留 Tier 2。
- 不实现 DD/tablespace discovery，不改变 `StorageEngine.open(existing)` 对显式表空间配置的依赖。
- 不引入 native `posix_fallocate` 或 JNA/JNI；0.16c 只提供平台预分配适配点，默认仍零填充。
- 不改变 `PageStore` 对外接口和 registry-free 边界。
- 不改变 WAL、doublewrite、flush/checkpoint 语义。

## Approach

推荐按 0.16b -> 0.16c 顺序实现。0.16b 是持久语义补齐，0.16c 是 fil.io 内部重构；分开测试能降低回归定位成本。

## 0.16b Design

GENERAL lifecycle 使用现有 page0 lifecycle 预留区，而不是新增第二套布局。旧 GENERAL 文件 lifecycle magic 为 0 时仍视为 `NORMAL`，保持兼容；新 GENERAL 文件创建时写入 `NORMAL` lifecycle header；持久标记损坏时写入 `CORRUPTED` lifecycle header。

`TablespaceLifecycleHeader` 从“UNDO truncate 专用”扩展为“page0 lifecycle marker”。它仍保留 UNDO truncate 需要的 `truncateEpoch`、`targetSizeInPages`、`finishState` 字段；GENERAL 稳定状态下这些字段写成无截断语义的稳定值：`truncateEpoch=0`，`targetSizeInPages=currentSizeInPages`，`finishState=NORMAL`。

`DiskSpaceManager.createTablespace` 对 `TablespaceType.GENERAL` 写入 `TablespaceLifecycleHeader(NORMAL, initialSizePages, 0, initialSizePages, NORMAL)`。UNDO 仍维持现有 `ACTIVE` lifecycle header 语义。

`DiskSpaceManager` 新增持久化重载 `markTablespaceCorrupted(MiniTransaction mtr, SpaceId spaceId, String reason)`。该方法在调用方提供的 MTR 中先取得 page0 X latch 读取 `SpaceHeaderSnapshot`（使用 `readForUpdate`，避免同一 MTR 内 S->X 升级），写 page0 lifecycle header，再发布 registry `CORRUPTED` 状态。现有 `markTablespaceCorrupted(SpaceId, String)` 保留为 runtime-only，并在 Javadoc 明确不保证重启后保留，用于尚无 MTR 的测试/诊断路径。

`PageZeroTablespaceMetadataLoader` 读取 page0 时先完成 FSP_HDR envelope 与 checksum/trailer 校验，再解码 lifecycle marker。GENERAL 表空间若无 lifecycle marker，发布 `NORMAL`；若 marker 为 `NORMAL` / `CORRUPTED`，发布对应状态；若 marker 为 GENERAL 不允许的临时/截断状态，抛领域元数据异常并关闭物理句柄。UNDO 表空间保留旧文件无 marker 时按 `NORMAL` 打开的兼容语义；若存在 lifecycle marker，则只允许 `ACTIVE` / `INACTIVE` / `TRUNCATING`，防止 GENERAL 稳定状态被误解释成 UNDO 生命周期。

普通 `openTablespace` 不需要立刻抛出 `TablespaceCorruptedException`。它应打开物理文件并让 registry 发布 `CORRUPTED` 快照；后续普通空间管理、typed page access、flush 等路径经 `registry.require` 拒绝。`openTablespaceForRecovery` 继续通过 `requireForRecovery` 允许 corrupted 状态，供恢复和诊断读取。

## 0.16c Design

`DataFileHandle` 仍是单个 data file 的物理生命周期 owner，继续持有 `FileChannel`、`TablespaceLifecycleLatch`、`FileSizeLock`、`FsyncLock` 与 `currentSizeInPages`。0.16c 不引入句柄替换锁，也不恢复 0.16a 已删除的 `DataFileHandleLock` / `PageIoRangeLock`。

新增 `storage.fil.io.DataFileGateway`，负责把已打开 `FileChannel` 的指定 page range 初始化或保证已分配。默认实现 `ZeroFillDataFileGateway` 复用当前循环写零语义。新增 `PreallocationStrategy` 作为平台预分配适配点，默认 `NoOpPreallocationStrategy` 不做 native 调用。新增 `PreallocatingDataFileGateway` 先尝试策略，再走零填充，确保跨平台新页读出为零；生产默认仍直接使用 `ZeroFillDataFileGateway`，避免把 no-op 策略放进热路径。

锁顺序保持不变：`create` 在句柄发布前调用 gateway 初始化初始范围；`autoExtend` 与 `ensureCapacity` 在 `Lifecycle(S) -> FileSizeLock(X)` 下调用 gateway，然后才 volatile 发布 `currentSizeInPages`；`truncateTo` 和 `force` 不经 gateway，仍按现有 `FsyncLock` 语义执行。

gateway 内禁止回调 Buffer Pool、DiskSpaceManager、registry、redo、flush 或事务锁。它只处理物理文件范围初始化和可选预分配，所有数据库语义仍由上层负责。

## Interfaces

接口放在 `cn.zhangyis.db.storage.fil.io` 包内，首版使用 package-private 可见性，避免把 fil.io 内部预分配适配点暴露成跨模块 API：

```java
interface DataFileGateway {
    void initialize(FileChannel channel, long fromPageInclusive, long toPageExclusive, PageSize pageSize, Path path);

    void ensureAllocated(FileChannel channel, long fromPageInclusive, long toPageExclusive, PageSize pageSize, Path path);
}
```

```java
interface PreallocationStrategy {
    void preallocate(FileChannel channel, long offsetBytes, long lengthBytes, Path path);
}
```

`FileChannelPageStore` 保留现有构造器，并新增可注入 gateway 的构造器。默认构造器使用 `DefaultIbdAutoExtendPolicy` 与 `ZeroFillDataFileGateway`，保证现有行为不变。

## Error Handling

- lifecycle marker magic 非 0 且不匹配格式时，按 FSP 元数据损坏处理。
- GENERAL 打开遇到持久 `CORRUPTED` 不吞掉状态；registry 发布 `CORRUPTED`，普通 require 抛 `TablespaceCorruptedException`。
- create 初始化失败时关闭 channel，抛 `DataFilePhysicalException`，不发布 handle。
- autoExtend / ensureCapacity 初始化失败时不更新 `currentSizeInPages`；物理文件可能部分增长，但逻辑大小仍不可见，后续 retry 或 recovery reconcile 收敛。
- preallocation unsupported 时可回退零填充；真实 IO 失败必须保留 cause 并抛项目异常。

## Testing

- GENERAL create 后 page0 lifecycle marker 持久为 `NORMAL`。
- 持久 `markTablespaceCorrupted(mtr, spaceId, reason)` 后关闭重开，registry state 为 `CORRUPTED`，普通 API 拒绝。
- `openTablespaceForRecovery` 能加载持久 `CORRUPTED` 表空间。
- 旧 GENERAL 文件 lifecycle magic 为 0 时仍按 `NORMAL` 打开。
- `ZeroFillDataFileGateway` 初始化指定 page range 后文件长度和字节内容正确。
- `DataFileHandle.create`、`autoExtend`、`ensureCapacity` 委托 gateway，失败时不发布新 size。
- `FileChannelPageStore` 默认构造保持零填充行为。

## Current Map Update

实现完成后更新 `docs/design/current-implementation-map.md` 的 Disk Manager 小节：

- Create tablespace：GENERAL/UNDO 均写 page0 lifecycle marker，GENERAL 初始 `NORMAL`。
- Open tablespace：loader 恢复 GENERAL persisted `CORRUPTED`，普通 require 拒绝。
- Package status：`storage.fsp.lifecycle` 覆盖 GENERAL `NORMAL/CORRUPTED` 与 UNDO truncate lifecycle。
- Package status：`storage.fil.io` 通过 `DataFileGateway` 初始化/扩展文件，默认 zero-fill，preallocation seam 已存在。
- Known gaps：普通 `DISCARDED` / DROP lifecycle 仍待 DDL；native/platform preallocation 仍为 future adapter。

## Acceptance Criteria

- 0.16b/0.16c targeted tests pass。
- Full Gradle `test` pass。
- 未新增 `synchronized`、`wait()`、`notify()`、`notifyAll()`。
- 未新增生产代码裸 `IllegalArgumentException` 或裸 `RuntimeException`。
- `PageStore` 仍不依赖 registry。
- `DataFileHandle` 仍保持 Lifecycle/FileSize/Fsync 锁顺序。
