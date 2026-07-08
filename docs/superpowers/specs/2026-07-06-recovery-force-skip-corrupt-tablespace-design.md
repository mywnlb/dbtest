# Recovery FORCE_SKIP_CORRUPT_TABLESPACE Design

## Goal

实现 `RecoveryMode.FORCE_SKIP_CORRUPT_TABLESPACE` 的设计：启动恢复时显式跳过配置为损坏的表空间，使其它已打开且未损坏的表空间仍可完成 doublewrite scan、redo replay、space-file reconcile、undo rollback / purge resume，并最终开放普通流量。默认 `NORMAL` 仍 fail-closed；force-skip 只能由显式配置启用，并必须在恢复报告和进度 journal 中记录被跳过的空间。

## References

- `docs/design/innodb-storage-engine-overview.md`：恢复顺序必须保持 doublewrite repair -> redo replay -> active transaction rollback -> purge resume；Recovery 不能执行普通 SQL。
- `docs/design/innodb-crash-recovery-design.md`：`FORCE_SKIP_CORRUPT_TABLESPACE` 是已定义 recovery mode，语义是跳过指定 corrupt tablespace 并标记不可用。
- `docs/design/innodb-flush-checkpoint-doublewrite-design.md`：doublewrite detect-only 只能报告可疑 page；无法修复时由 recovery corruption policy 决定。
- `docs/design/innodb-disk-manager-design.md`：恢复修复 page IO 使用 `TablespaceLifecycleLatch(S)`，物理文件锁不进入事务死锁域。
- `docs/design/current-implementation-map.md`：当前 production recovery 只覆盖显式配置表空间；`FORCE_SKIP_CORRUPT_TABLESPACE` 仍 reserved；redo apply 只有 `PageRedoApplyHandler`；doublewrite 待检查页来自 `DoublewriteFileRepository.pageIds()` 且由 engine 过滤到已打开空间。
- `docs/design/storage-backlog.md`：0.7 剩余项是 `FORCE_SKIP_CORRUPT_TABLESPACE`，需要 per-space 过滤链。

## Current State

- `RecoveryMode.FORCE_SKIP_CORRUPT_TABLESPACE` enum 已存在。
- `StorageEngine.recoverExisting` 目前遇到该 mode 直接抛 `DatabaseValidationException`。
- `CrashRecoveryService.recover` 也会在进入 doublewrite/redo 前拒绝该 mode。
- `RecoveryRequest` 当前没有 skipped space set。
- `RecoveryReport` 当前只记录 repaired/detected/applied batch 数，没有 skipped space/page 诊断。
- `DoublewriteRecoveryScanner` 只能按单页 scan/repair；外层 `CrashRecoveryService` 负责遍历 `pagesToRepair`。
- `RedoApplyDispatcher` 只有 page handler，`PageRedoApplyHandler` 会直接通过 `PageStore` 读写目标 `PageId`。
- `SPACE_FILE_RECONCILE` 遍历 `spacesToReconcile`，逐空间读 page0 并 `ensureCapacity`。
- `TransactionUndoRecoveryParticipant` 目前由 `StorageEngine` 实现，只扫描系统 undo page3；没有多数据表空间 DD discovery。

## Scope

本设计只实现显式表空间集上的 force-skip，不做全目录 discovery。调用方必须提供要跳过的 `SpaceId` 集合；该集合通常来自后续 0.16b 持久 `CORRUPTED` 状态或管理员诊断配置。force-skip 会跳过目标空间的 doublewrite 页、redo page records、space-file reconcile 项、普通数据空间相关恢复参与者。被跳过空间在本次启动恢复中不通过 `DiskSpaceManager.openTablespaceForRecovery` 打开，避免 page0 raw read 或最终 `PageStore.forceAll` 触达损坏文件；恢复后该空间因未打开或 registry corrupted 状态继续被普通访问拒绝。

## Non-Goals

- 不自动扫描 data directory 或 DD/SDI 推导 corrupt space。
- 不修复损坏表空间，也不把 skipped space 重新标记为 `NORMAL`。
- 不开放被跳过表空间的读取、写入、flush、purge 或 DML。
- 不实现 DDL recovery、drop/discard cleanup 或 orphan 文件删除。
- 不实现多索引/多表 transaction undo recovery；没有 DD 时只保护当前显式 recovery set。
- 不改变 redo 文件格式，不新增逻辑 redo record。

## Approach Options

**Recommended: request-level skip policy.** 在 `RecoveryRequest` 增加不可变 `skippedSpaces`，所有 recovery stage 在遍历 page/space 前调用统一谓词过滤。优点是最小改动、语义集中、符合当前显式配置恢复模式；缺点是 redo apply 需要一个过滤入口。

**Alternative: PageStore wrapper.** 给 redo/doublewrite 注入一个跳过指定 space 的 `PageStore` wrapper。优点是能阻止误触达 skipped space；缺点是 `PageRedoApplyHandler` 会在读写时才发现跳过，无法统计 skipped redo records，也容易把预期 skip 和真实文件错误混淆。

**Alternative: dispatcher-level wrapper only.** 只包装 `RedoApplyDispatcher` 过滤 page records。优点是 redo 侧清晰；缺点是 doublewrite/reconcile/undo 仍需要各自过滤，策略分散。

采用 request-level skip policy，并在 redo dispatcher 增加显式 page-record filter。这样每个 stage 都能在进入物理 IO 前跳过对应 space，并能产生一致诊断。

## Core Model

新增值对象命名为 `RecoverySkipPolicy`，放在 `storage.recovery` 包内：

```java
public record RecoverySkipPolicy(Set<SpaceId> skippedSpaces) {
    public RecoverySkipPolicy {
        if (skippedSpaces == null) {
            throw new DatabaseValidationException("recovery skipped spaces must not be null");
        }
        skippedSpaces = Set.copyOf(skippedSpaces);
    }

    public static RecoverySkipPolicy none();

    public boolean shouldSkip(SpaceId spaceId);

    public boolean shouldSkip(PageId pageId);
}
```

`RecoveryRequest` 增加字段 `RecoverySkipPolicy skipPolicy`，默认 `RecoverySkipPolicy.none()`。新增静态工厂 `forceSkip(RedoCheckpointStore, RedoLogFileRepository, RedoApplyDispatcher, RedoApplyContext, Set<SpaceId>)`，只允许 `FORCE_SKIP_CORRUPT_TABLESPACE` 创建非空 skipped set；`NORMAL` 和 `READ_ONLY_VALIDATE` 工厂始终创建空 skip policy，record 构造器也要拒绝非 force 模式携带非空 skip policy，避免默认启动误跳过损坏数据。系统 undo、单聚簇恢复索引等受保护空间由 `StorageEngine` 组合根校验，因为 `RecoveryRequest` 本身没有这些引擎配置上下文。

`RecoveryReport` 增加诊断字段：

```java
Set<SpaceId> skippedSpaces
int skippedDoublewritePageCount
int skippedRedoRecordCount
int skippedReconcileSpaceCount
```

字段为不可变快照，用于测试和启动诊断。实现时通过 `RecoveryReport` 的 `normal`、`readOnlyValidate`、`forceSkip` 三个静态工厂集中构造，避免散落重载构造导致默认计数遗漏。`RecoveryProgressJournal` 需要新增可写 detail 的完成事件或专用 diagnostic 事件，在 force-skip 成功/失败路径记录 skipped space set 与各 skip count；仅放进 `RecoveryReport` 不满足“progress journal 留痕”的目标。

## Stage Semantics

### Traffic Gate

force-skip 启动仍先 `gate.closeForRecovery()`。如果 skip set 为空，mode 必须拒绝启动；force-skip 没有“自动猜测损坏空间”的隐式行为。

### Doublewrite Stage

`CrashRecoveryService.repairDoublewritePages` 遍历 `request.pagesToRepair()` 时先判断 `skipPolicy.shouldSkip(pageId)`。命中则不调用 `DoublewriteRecoveryScanner.scanPageIfNeeded`，计入 `skippedDoublewritePageCount`。这样不会对 skipped space 执行 `PageStore.readPage/writePage/force`。

UNDO truncation participant 的 `prepareDoublewrite` 当前只处理系统 undo。首版要求 skip set 不得包含系统 undo space；该校验在 `StorageEngine` 组装 force-skip 请求前完成。原因是系统 undo 被跳过会破坏 formal UNDO_ROLLBACK/RESUME_PURGE，当前没有只恢复部分事务系统的安全语义。

### Redo Replay Stage

redo replay 必须在进入 `PageRedoApplyHandler` 前过滤 skipped spaces。推荐扩展 `RedoApplyDispatcher`，让 dispatcher 负责过滤和统计，page handler 仍只处理已经筛选过的 page records：

```java
public RedoApplySummary applyAll(List<RedoLogBatch> batches, RedoApplyContext context, Predicate<PageId> skipPage);
```

首版只支持 `PAGE_INIT` / `PAGE_BYTES`，所以 dispatcher 可以按 record 的 `PageId` 过滤。对 skipped space 的 record 不调用 page handler，计入 `skippedRedoRecordCount`。如果一个 batch 同时包含 skipped 与 non-skipped records，不能用原 `RedoLogBatch` 构造子批次，因为 `RedoLogBatch` 会校验 `LogRange` 与 records byte length 精确匹配。实现应新增 `RedoApplyBatchView(LogRange originalRange, List<RedoRecord> recordsToApply)` 或等价包内类型，由 page handler 使用 `originalRange.end()` stamp pageLSN，同时只遍历 `recordsToApply`。

如果未来加入非 page 逻辑 redo，未声明 spaceId 的 record 在 force-skip 下必须 fail-closed，直到该 redo type 定义 skip 语义。

### Redo Boundary Install

即使跳过了部分 space 的 redo records，也必须安装 `recoveredToLsn`。理由是 redo reader 已扫描到该边界，后续新 redo 不能覆盖历史文件。跳过空间的页在该边界之前仍不可信，必须通过 skipped registry state 阻止普通访问。

### Undo Tablespace Resume

系统 undo space 不允许跳过。若 skip set 包含 undo space，`StorageEngine` 组装阶段在打开任何 data file 前拒绝。这样 `UNDO_TABLESPACE_RESUME` 语义不变。

### Space File Reconcile

`CrashRecoveryService.reconcileSpaceFiles` 遍历 `spacesToReconcile` 时过滤 skipped spaces，命中则不读 page0、不 `ensureCapacity`，计入 `skippedReconcileSpaceCount`。非 skipped space 仍严格校验 page0；其损坏仍 fail-closed。

### Transaction Undo Recovery

首版只处理系统 undo page3 和配置的单聚簇索引。force-skip 下如果 recovered undo 指向 skipped data space 的聚簇索引，rollback 不应访问该 index；当前没有 DD/multi-index 定位能力，所以本设计约束为：配置的 `clusteredIndex` 所在 space 若在 skip set 中，事务 undo recovery 阶段拒绝 force-skip 请求。未来 DD 接入后可以按 table/index 维度跳过相关 rollback 并保留对象 unavailable。

### Open Traffic

force-skip 成功后 `gate.openForUserTraffic()`，`RecoveryState.OPEN`。被跳过空间必须保持 registry `CORRUPTED` 或 unavailable/not-open 状态，普通 `DiskSpaceManager`、typed page access、storage DML facade 继续拒绝该空间。

## StorageEngine Composition

`EngineConfig` 增加显式 `forceSkippedSpaces` 配置，默认空。为保持现有构造器兼容，新增 record 字段应放在尾部，既有构造器传 `Set.of()`，并提供 `withForceSkippedSpaces(Set<SpaceId>)` 便利方法。`withRecoveryMode(FORCE_SKIP_CORRUPT_TABLESPACE)` 不应单独足够；调用方还必须配置非空 skipped spaces。

`StorageEngine.recoverExisting` 先从 `forceSkippedSpaces` 构造 skip policy 并校验：skip set 非空、不得包含系统 undo、不得包含当前配置的单聚簇恢复索引 `clusteredIndex.rootPageId().spaceId()`。随后只打开系统 undo 和 `recoveryTablespaces` 中 **不在 skip set** 的文件；对 skipped data space 不调用 `DiskSpaceManager.openTablespaceForRecovery`，否则当前 loader 会读 page0，且最终 `PageStore.forceAll` 会 force 已打开句柄，都会违反 skip 语义。

force-skip 下应区分两个集合：

- `configuredRecoverySpaces`：系统 undo + 配置的 `recoveryTablespaces`，包含 skipped space，用于 doublewrite page 枚举和 reconcile skip 计数。
- `openedRecoverySpaces`：系统 undo + 非 skipped recovery tablespaces，只包含真正打开且允许 IO 的空间，用于 PageStore 触达。

doublewrite pages 过滤条件变成：space 在 `configuredRecoverySpaces` 中；后续由 `CrashRecoveryService` 的 skip policy 决定计数或扫描。force-skip 分支使用 `RecoveryRequest.forceSkip(checkpointStore, redoRepository, dispatcher, applyContext, skippedSpaces)` 创建请求，再追加 doublewrite repair、redo boundary install、undo tablespace recovery、space-file reconcile（传 configured set 以便计数）和 transaction undo recovery 参与者。

如果 skipped space 不在 `configuredRecoverySpaces` 中，允许作为诊断项保留在 report/journal 中，但不参与 doublewrite/reconcile 计数；redo 仍可按 record spaceId 统计 skipped record。这样管理员可以跳过一个未显式打开的损坏空间，而当前无 discovery 的恢复路径仍只触达显式空间。

## Error Handling

- `FORCE_SKIP_CORRUPT_TABLESPACE` + 空 skip set：`DatabaseValidationException`。
- `StorageEngine` force-skip 配置包含系统 undo space：`DatabaseValidationException`。
- `StorageEngine` force-skip 配置包含当前配置的单聚簇 DML/recovery index space：`DatabaseValidationException`。
- non-skipped space 在 doublewrite/redo/reconcile 中损坏：保持现有 fail-closed。
- redo batch 内含无法判断 `PageId` 的 future record：force-skip 下 fail-closed。
- progress journal 写失败仍作为 suppressed cause，不改变 fail-closed/open 语义。

## Concurrency And Durability

force-skip 不引入新的后台线程和锁。所有过滤发生在 recovery 主线程进入物理 IO 前。跳过的 data space 不会被 `StorageEngine` 打开，跳过的 page 不进入 `DoublewriteRecoveryScanner`、`PageRedoApplyHandler` 或 `PageStore.ensureCapacity`，因此不会持有该 space 的 file lock，也不会被最终 `PageStore.forceAll` 触达。非 skipped space 继续沿用现有 recovery 锁顺序和最终 `PageStore.forceAll` durability barrier。

## Testing

- Request / engine validation：force-skip 空 skip set 拒绝；NORMAL/READ_ONLY_VALIDATE 携带 skip set 拒绝；`StorageEngine` 配置 skip 系统 undo 或配置的单聚簇恢复索引 space 时拒绝。
- Doublewrite skip：pagesToRepair 同时包含 skipped 和 normal space，只扫描 normal space，report 记录 skipped doublewrite page count。
- Redo skip：redo batch 同时包含 skipped 与 normal page records，只应用 normal records，normal pageLSN 使用原 batch end LSN；测试必须覆盖不能伪造 `RedoLogBatch` 子批次这一约束，report 记录 skipped redo record count。
- Reconcile skip：spacesToReconcile 同时包含 skipped 和 normal space，只对 normal 调 `ensureCapacity`。
- Non-skipped corruption：normal space page0 mismatch 仍 fail-closed。
- Engine composition：`EngineConfig` force-skip + skippedSpaces 组装 request，不再在 `StorageEngine.recoverExisting` 中 reserved-fail；skipped recovery table 文件不会被 `openTablespaceForRecovery` 打开。
- Gate/report：成功后 state 为 `OPEN`，completed stages 与 NORMAL 一致，report 含 skipped spaces 和计数。
- Progress journal：force-skip 成功/失败事件 detail 含 skipped spaces 和 skip count 诊断。
- Regression：READ_ONLY_VALIDATE 仍不写 data file；NORMAL 不允许 skip。

## Current Map Update

实现后更新 `docs/design/current-implementation-map.md` 的 Recovery Layer Slice：

- `RecoveryRequest` / `RecoveryReport` 增加 skip policy 与 skip 诊断字段。
- `StorageEngine.recoverExisting` 对 `FORCE_SKIP_CORRUPT_TABLESPACE` 生产接线，不再 reserved fail。
- `StorageEngine.recoverExisting` 只打开 non-skipped recovery tablespaces；skipped spaces 仅作为诊断和过滤输入。
- `CrashRecoveryService` doublewrite / redo / reconcile 阶段增加 per-space skip 过滤。
- Known gaps 将 `FORCE_SKIP_CORRUPT_TABLESPACE` 从未实现改为“显式 space set 支持；仍无 DD discovery / DDL object-level force recovery”。

## Acceptance Criteria

- force-skip 模式必须显式配置非空 skipped spaces。
- skipped space 不发生 PageStore read/write/force/ensureCapacity，也不通过 `DiskSpaceManager.openTablespaceForRecovery` 触发 page0 loader。
- non-skipped spaces 仍执行完整 doublewrite、redo、reconcile、undo recovery 和 forceAll。
- recovery report 能列出 skipped spaces 和三个 skip count。
- 默认 NORMAL 和 READ_ONLY_VALIDATE 行为不变。
- 不新增 SQL/session/DD 依赖，不实现 tablespace discovery。
