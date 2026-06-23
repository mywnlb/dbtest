# Slice: Crash-safe 空间管理 — autoextend 文件长度重对齐

- 日期：2026-06-22
- 关联设计：`innodb-disk-manager-design.md` §8、§8.1、§9.4、§12、§15、§16.6；`innodb-crash-recovery-design.md`（redo replay 阶段）。
- 前置：FSP 元数据（page0 SpaceHeader、page2 inode、XDES bitmap）写入已走 MTR → 物理 `PAGE_BYTES`/`PAGE_INIT` redo；`DataFileHandle.autoExtend` 零填充后只发布 `currentSizeInPages`，不 `force`；`DataFileHandle` 注释（177–180）已把"物理大小与逻辑大小暂时背离"显式留给本片收敛。
- 定位：兑现 `DiskSpaceManager` "本片 no-redo，不声明 crash-safe"承诺中与 autoextend 相关的部分。复用已有物理 redo，不新增 redo 记录类型；crash-safety 全部落在恢复侧。
- 评审修订（2026-06-23）：依代码评审补入 extend-on-demand 收紧、阶段重排、page0 校验、doublewrite 越界跳过、通用 redo 边界安装、恢复末尾 durability force 六项加固（见下）。

## 1. 范围

做：
- 新增 `PageStore.ensureCapacity(SpaceId, PageNo minSizeInPages)` + `FileChannelPageStore` 实现 + `DataFileHandle.ensureCapacity(PageNo)`：幂等"扩到至少 N"——`min <= current` 即 no-op；否则零填充 `[current, min)` 并发布 `currentSizeInPages = min`。持 `Lifecycle(S)+FileSize(X)`，是 `truncateTo` 的镜像；不 `force`（与 `autoExtend` 一致，依赖恢复幂等 + 恢复末尾 forceAll）。
- `PageRedoApplyHandler` extend-on-demand **仅在 PAGE_INIT** 首触越界页时 `ensureCapacity(spaceId, pageNo+1)`；首触是 PAGE_BYTES 且越当前文件尾（无建页记录）抛 `RedoLogCorruptedException`，不静默造半成品页。
- 新增恢复阶段 `SPACE_FILE_RECONCILE`，置于 `UNDO_TABLESPACE_RESUME` **之后**（undo 续作会把被截断 undo 表空间 page0 重建为新小尺寸，reconcile 必须读到重建后的尺寸，否则把刚截断文件重新撑大/磁盘不足阻断截断）；`RecoveryRequest.withSpaceFileReconcile(List<SpaceId>)` 携带空间集（默认空=no-op）。
- `CrashRecoveryService.reconcileSpaceFiles` 逐空间读恢复后 page0 → `validateReconcileHeader`（spaceId/pageSize 一致、size>0、字节偏移不溢出，否则 `TablespaceCorruptedException`）→ 幂等 `ensureCapacity(size)`，覆盖 extent 内尾部零页（无 redo 描述）。
- 新增恢复阶段 `REDO_BOUNDARY_INSTALL`（置于 `REDO_REPLAY` 之后、`UNDO_TABLESPACE_RESUME` 之前）：`RecoveryRequest.withRedoBoundaryInstall(RedoLogManager)` 提供本进程续写的 redo manager，对其 `restoreRecoveredBoundary(recoveredTo)`，使新 MTR 从 recoveredTo 续写而非从 0 覆盖；与 undo 参与者对同一 LSN 的安装幂等共存。
- `DoublewriteRecoveryScanner.repairPageIfNeeded` 对 pageNo>= 当前文件大小的越界页直接跳过（物理不存在、谈不上 torn；doublewrite 在 redo 之前执行，文件可能仍短），交 redo replay 重建。
- 恢复在开放流量前 `PageStore.forceAll()`（新增接口方法）落盘全部恢复写（replay/repair/reconcile 都绕过 Buffer Pool dirty、自身不 fsync），作为 durability 屏障——此后任何越过 recoveredToLsn 的 checkpoint 都安全。
- `DiskSpaceManager` 前向分配路径不改动（crash-safety 纯恢复侧）。

不做：
- 不新增逻辑 redo 记录类型（`FILE_EXTEND`/`ALLOCATE_PAGE`/`UPDATE_XDES` 等 §9.3 命令留后续片）。
- 不为 `freePage`/`createSegment`/`dropSegment` 单独写 crash 恢复测试（共用同一 `PAGE_BYTES` 重放机制，本片不验证）。
- 不引入生产 engine bootstrap / 组合根；durable redo、`recoveredRedoManager`、reconcile/force 入参仍在测试注入（与现有 test-wired 栈一致）。
- 不在 `ensureCapacity` 或前向 `autoExtend` 加 `force`，不改前向扩展顺序（durability 由恢复末尾 forceAll 保证）。
- 不回收 `L > S`（物理大于 page0 逻辑）泄漏的尾页；reconcile page0 大小不设绝对页数上界（无实例级配置，留后续）。

## 2. 数据流与不变量

1. 前向（不变）：`allocatePage` → 分配器在 currentSize 内取页；空间不足 → `pageStore.extend()`（按 `DefaultIbdAutoExtendPolicy` 整 extent 增长、发布 size、不 force）→ `setCurrentSizeInPages`（page0 PAGE_BYTES redo）→ 重试 → `initAllocatedPage`（页 N 的 PAGE_INIT+PAGE_BYTES）。MTR commit 追加 redo；测试用 durable manager + `flush()` 满足 WAL。
2. 恢复（7 阶段，方括号条件触发）：`TRAFFIC_CLOSED` → `DOUBLEWRITE_REPAIR`（越界页跳过）→ `REDO_REPLAY`（PAGE_INIT 越界页 extend-on-demand）→ `[REDO_BOUNDARY_INSTALL]` → `[UNDO_TABLESPACE_RESUME]` → `[SPACE_FILE_RECONCILE]`（校验+`ensureCapacity(page0 size)`）→ `forceAll` → `OPEN_TRAFFIC`。
3. 不变量（阶段后，逐表空间）：物理文件页数 `>=` 恢复后 `page0.currentSizeInPages`，且所有 `< page0.size` 的页物理存在（未写过则为零页）⇒ 后续 FSP 分配不越界。
4. `L < S`（redo 更大、extent 因未 fsync 丢失）：本片目标场景，reconcile 重新扩展。`L > S`（extent durable、page0 redo 丢失）：`ensureCapacity` no-op，多余物理页良性（分配器可能跳过；非损坏），记为简化点。
5. 幂等：`ensureCapacity` 在 `>=` 时 no-op；replay 按 pageLSN 跳过；reconcile 重读 page0。重复运行恢复安全。

## 3. 错误与并发约束

- `ensureCapacity` 参数校验（`min >= 1`、非空）抛 `DatabaseValidationException`；IO 失败沿用 `DataFilePhysicalException`。
- `SPACE_FILE_RECONCILE` 阶段失败经既有 `failClosed → RecoveryStartupException`，gate 保持关闭。
- `ensureCapacity` 锁序与 `autoExtend` 一致：`TablespaceLifecycleLatch(S)` → `FileSizeLock(X)`，零填充在 FileSize(X) 内完成后再发布 `currentSizeInPages`；不在持锁期间等待 Buffer Pool page latch。

## 4. 验收测试与 current map

- 单元（`ensureCapacity`，`FileChannelPageStoreEnsureCapacityTest`）：`>=` 时 no-op、短缺时扩展+零填充、`min < 1` 拒绝；为 `truncateTo` 测试的镜像。
- 崩溃恢复（`SpaceFileReconcileRecoveryTest`）：durable `RedoLogManager`+`DiskSpaceManager`；建 1 extent 表空间；`allocatePage` 触发整 extent autoextend（仅分配页 N，`postExtend > preExtend+1` 确保有尾部零页）；`flush()` redo durable。模拟崩溃=`truncate` 数据文件回扩展前长度 + 以短长度重开。`recover(withSpaceFileReconcile)` 后断言：(a) 文件页数==恢复后 page0 size；(b) 页 N 可读且 type=`ALLOCATED`（extend-on-demand）；(c) 尾页可读零页（reconcile）；(d) 阶段含 `SPACE_FILE_RECONCILE` 且在 `OPEN_TRAFFIC` 前。
- 加固测试：`PageRedoApplyExtendOnDemandTest`（PAGE_INIT 越界重建 / 首触越界 PAGE_BYTES 判损坏）；`DoublewriteRecoveryScannerBoundsTest`（越界页跳过）；`RecoveryHardeningTest`（`REDO_BOUNDARY_INSTALL` 装边界使续写从 recoveredTo / reconcile page0 spaceId 不一致 fail closed）。
- 固定 JDK/Gradle 全量 `test`；同步更新 `current-implementation-map.md`：Recovery 小节 7 阶段顺序+数据链+package status，`fil` 加 `ensureCapacity`/`forceAll`，doublewrite repair 加越界跳过，Disk Manager 标注 autoextend 经恢复重对齐已 crash-safe，并把 `DataFileHandle` 注释 177–180 指向本片。
- 已知遗留：reconcile page0 大小绝对上界、生产组合根（durable redo/boundary/reconcile/force 的真实接线）属 engine bootstrap 范围，留后续片。
