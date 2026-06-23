# Spec: TablespaceRegistry 接入 DiskSpaceManager + 空间管理准入（page-0 type 持久化 + loader + recovery 准入）

- 日期：2026-06-18
- 关联设计：`docs/design/innodb-disk-manager-design.md` §5.2（Tablespace 聚合根/状态）、§6.2（SpaceHeaderPage/FSP_HDR）、§8.1（生命周期锁，本片以 registry 桶级原子替代）、§13.1/§13.3（DiskSpaceManager Facade / PageStore 物理 IO）；`docs/superpowers/specs/2026-06-10-fil-physical-io-layer-design.md`（TablespaceRegistry/State/Metadata/PageStore registry-无关取舍 B′）。
- 前置：fil `TablespaceRegistry`/`CachingTablespaceRegistry`/`TablespaceState`/`TablespaceMetadata`/`Tablespace`/`TablespaceMetadataLoader`/`PageStore`/`FileChannelPageStore`/`DataFileDescriptor` 已存在；fsp `SpaceHeaderRepository`/`SpaceHeaderSnapshot`/`SpaceHeaderLayout`、`DiskSpaceManager`(api) 已存在；T1.x 全绿（468 tests）。
- 状态：独立切片（与 undo epic 解耦）。目标：把已建好但**未接入**的 `TablespaceRegistry` 接进 `DiskSpaceManager`，落地**空间管理准入**：建/开表空间注册权威 metadata、空间管理 API 经 `require()` 状态准入、运行时生命周期标记、recovery `requireForRecovery` 准入；`type` 持久化进 page-0 `spaceFlags`，reopen 由 page-0 loader 从磁盘重建。

## 1. 范围

**做：**

- fil：`PageStore` 增 `Path pathOf(SpaceId)`（返回已开文件路径）；`FileChannelPageStore` 实现（由 `DataFileHandle` 取 path）。`TablespaceType` 增 `code()`/`fromCode()`（稳定落盘 code，见 §2.1）。`TablespaceTypeFlags`（`type ↔ spaceFlags int`，仅 bits 装配，code 取自 `TablespaceType.code()`）。`TablespaceRegistry` + `CachingTablespaceRegistry` 增 `markInactive(SpaceId)`（复用既有 `compute`+`transitTo(INACTIVE)` 模式，与 `markCorrupted`/`markDiscarded` 同款；`TablespaceState` 已允许 NORMAL/ACTIVE→INACTIVE）。
- fsp：`SpaceHeaderRawCodec`（**public**，从 `ByteBuffer` 解 page-0 物理字段，封装 `SpaceHeaderLayout` 偏移）+ 值对象 `SpaceHeaderPhysical`。
- api：`PageZeroTablespaceMetadataLoader`（实现 `TablespaceMetadataLoader`，raw `readPage(page0)` + `SpaceHeaderRawCodec` + `pathOf` → `TablespaceMetadata`）。
- api：`DiskSpaceManager` 改 —— 持 `TablespaceRegistry`（默认内建 `CachingTablespaceRegistry(pageZeroLoader)`，保留 3 参构造器 + 加注入重载）；`createTablespace` 写 type 进 flags + `registry.replace(NORMAL)`（保留 4 参，加 5 参 type 重载）；`openTablespace` + `registry.open`（失败 close 防半开）；空间管理 API（`createSegment`/`allocatePage`/`freePage`/`dropSegment`/`usage`）经 `registry.require` 准入；运行时生命周期标记 `markTablespaceInactive`/`markTablespaceCorrupted(reason)`/`discardTablespace`（仅转 registry 状态，见 §8）；recovery startup 入口 `openTablespaceForRecovery(spaceId, path)`（文件未开时 `pageStore.open` + `requireForRecovery`，不过状态门）；查询 `tablespaceState(spaceId)`。

**不做（本片非目标）：**

- **不做全普通 IO 准入**：本片只准入 `DiskSpaceManager` 空间管理 API。`IndexPageAccess`/`UndoPageAccess` → `BufferPool` → `PageStore` 的普通数据页读写**不经 DiskSpaceManager、本片不拦**（见 §2.3）。operation-level lease / storage facade gate 留后续片；**门绝不进 PageStore**。
- **不持久化 state**：lifecycle 标记（INACTIVE/CORRUPTED/DISCARDED）仅运行时 registry，**跨重启不存活**；page-0 只持久化 `type`。state 持久化 + truncation/discard 落盘留后续片。
- 不维护 registry 的 size/freeLimit 实时一致（权威 size 在 page 0；见 §2.2）。
- 不做 discovery/loadAll、不做多文件表空间、不补 page-0 FilePageHeader 信封（见 §2.4 小坑2）、不做 DD↔FSP_HDR 跨源对账。
- 不改 PageStore 物理 IO 语义、不在 buf 引入 registry/state 知识。

## 2. 关键决策

### 2.1 type 编进 `spaceFlags`（零 page-0 格式变更）

`SpaceHeaderRepository.initialize/read` 已 round-trip `SPACE_FLAGS`(int)。故 `type` 编进 flags，无新字段、无布局变更：

- **code 权威在 enum 自身**：给 `TablespaceType` 加 `code()`/`fromCode()`（`SYSTEM=0`/`FILE_PER_TABLE=1`/`GENERAL=2`/`UNDO=3`/`TEMPORARY=4`，与当前 ordinal 对齐但**显式钉死、不随 enum 重排漂移**，对齐 `UndoRecordType`/`PageType` 风格）。`fromCode` 未知 code 抛 `DatabaseValidationException`。`TablespaceTypeFlags` 不持有映射，只装配 bits。
- **bits 0..2**：`type.code()`（5 个 type 占 0..4）。
- **bits 3..31**：reserved / future flags（压缩、加密等）。
- `TablespaceTypeFlags.encode(type) -> int`：低 3 位 = `type.code()`，高位 0。`decode(int) -> TablespaceType`：`TablespaceType.fromCode(flags & 0x7)`；**未知 type code 由 `fromCode` 抛**；**高位保留位被忽略/保留**（不因高位非 0 而拒绝——否则将来无法扩展 flags）。
- `createTablespace` 当前写 `spaceFlags=0` → 改为 `TablespaceTypeFlags.encode(type)`。

### 2.2 loader 从磁盘 raw 读 + path 来源 + size 非实时

- **path 来源（修正草案缺口）**：`TablespaceMetadata` 必含 `DataFileDescriptor(path,...)`，而 raw page 0 无 path。解决：`PageStore` 增 `Path pathOf(SpaceId)`，loader 用它取已开文件 path。**优于 `TablespacePathResolver`/DiskSpaceManager catalog**：PageStore 是开文件的人、必然知道 path，且覆盖「直接 `store.open` 绕过 `openTablespace`」的 reopen 路径（catalog 覆盖不到），使 `require()` 懒加载能重建完整 metadata。
- **loader**：`PageZeroTablespaceMetadataLoader(PageStore, PageSize)`，`load(spaceId)`：`pageStore.readPage(PageId(spaceId,0), buf)`（**raw 物理读，不开 MTR/不经 BufferPool**，匹配 loader「从权威源读快照」语义且避免 require() 被外层 MTR 调用时的 MTR 嵌套）→ `SpaceHeaderRawCodec.readPhysical(buf)` → `type=TablespaceTypeFlags.decode(flags)`、`state=NORMAL`、`dataFiles=[DataFileDescriptor(pathOf(spaceId), PageNo(0), currentSizeInPages)]`、`name` 派生（`"space-"+id`）→ `TablespaceMetadata`。**健壮性**：解出的 spaceId ≠ 请求 spaceId、或 pageSize 非法 → 抛（替代缺失的 PageType 信封校验，见 §2.4）；文件未开/读失败 → `Optional.empty()`（registry 转 `TablespaceNotFoundException`）。
- **createTablespace 走 `registry.replace`（不走 loader）**：create 时 page 0 还在 BufferPool/MTR（未刷盘），raw loader 读盘读不到最新内容；故 create 用已知参数直接 `replace(metadataFromCreateArgs)` 登记 NORMAL。loader 只服务 open/reopen（page 0 已落盘）与 require() 懒加载。
- **registry size 快照非实时（修正 autoextend 坑）**：`allocatePage` 的 `pageStore.extend` + `headerRepo.setCurrentSizeInPages` **不**同步 registry handle 的 `currentSizeInPages/freeLimitPageNo`。**权威 size 在 page 0（headerRepo）**；registry 快照仅 state/type 权威，size 可 stale。准入只用 state，stale size 无害。明确写进简化点（§8）。

### 2.3 门设在空间管理 API 层，PageStore 保持 state-free（最关键分层判断）

- 准入门在 `DiskSpaceManager.createSegment/allocatePage/freePage/dropSegment/usage` 入口调 `registry.require(spaceId)`；`PageStore` 继续只做物理 IO、**不 import registry、不拒 CORRUPTED/EMPTY**（否则 recovery 与初始化被挡）。
- **诚实声明（修正草案过度承诺）**：这是**「空间管理准入」**，不是全普通 IO 准入。`IndexPageAccess`/`UndoPageAccess` 的页读写经 `BufferPool→PageStore` 不过 `DiskSpaceManager`，本片**不拦**。「标记 CORRUPTED 后所有普通数据读写都拦住」需后续 operation-level lease / storage facade gate（BTree/Undo 打开页前过一次 tablespace 准入），仍不进 PageStore。本片把准入定位在「拿到空间管理权（建段/分配页/删段/用量）」这一层。

### 2.4 两个小坑

- **`SpaceHeaderLayout` 是 package-private**：api loader 不能直接用偏移常量。**不**散落 public 化常量；新增 **public `fsp.SpaceHeaderRawCodec.readPhysical(ByteBuffer) -> SpaceHeaderPhysical`**，内部用 `SpaceHeaderLayout` 偏移解 spaceId/pageSize/spaceFlags/currentSizeInPages/freeLimitPageNo/spaceVersion（**只解 loader 需要的物理字段，不解 3 个 FLST base**——FlstBase 解码依赖 PageGuard，raw ByteBuffer 路径不复刻）。loader 只依赖这一个 codec，不碰偏移。
- **page 0 无 FilePageHeader/`PageType.FSP_HDR` 信封**（既有 gap：`createTablespace` 只写 SpaceHeader body）：loader 只读 SpaceHeader body，不受影响；以「spaceId 自洽 + pageSize 合法」做 sanity 替代 PageType 校验。补 page-0 信封是**独立既有 gap，本片不做**（避免无关重构），列为 follow-up。

### 2.5 向后兼容（保护 30 处既有调用）

保留 `new DiskSpaceManager(pool,store,ps)`（内部建默认 registry）、`createTablespace(mtr,spaceId,path,initialSize)`（type 默认 `GENERAL`）；新增 `new DiskSpaceManager(pool,store,ps,registry)`、`createTablespace(mtr,spaceId,path,initialSize,TablespaceType)`。8 文件 30 处既有调用零改动；新增准入失败路径由新测试覆盖。`openTablespace` 失败 → `pageStore.close(spaceId)` 防半开。

## 3. 组件

| 组件 | 包 | 职责 |
| --- | --- | --- |
| `TablespaceType`（改） | fil | 加 `code()`/`fromCode()`（稳定落盘 code，权威在 enum） |
| `TablespaceTypeFlags` | fil | `type ↔ spaceFlags int` bits 装配（低 3 位=`type.code()`，高位保留） |
| `TablespaceRegistry`/`CachingTablespaceRegistry`（改） | fil | 增 `markInactive(SpaceId)`（`compute`+`transitTo(INACTIVE)`，与 markCorrupted/markDiscarded 同款） |
| `PageStore.pathOf(SpaceId)` | fil | 返回已开文件 path（loader 取 path 用） |
| `SpaceHeaderPhysical` | fsp | page-0 物理字段值对象（spaceId/pageSize/flags/currentSize/freeLimit/version） |
| `SpaceHeaderRawCodec` | fsp（public） | `readPhysical(ByteBuffer) -> SpaceHeaderPhysical`（封装偏移） |
| `PageZeroTablespaceMetadataLoader` | api | raw 读 page0 + codec + pathOf → `TablespaceMetadata` |
| `DiskSpaceManager`（改） | api | 持 registry；create/open 注册；空间管理 API 准入；lifecycle 标记；recovery 准入 |

## 4. 数据流

- **建**：`createTablespace(...,type)` → `pageStore.create` → 写 page0（`spaceFlags=encode(type)`）→ `reserveSystemExtent` → `registry.replace(metadata(NORMAL,type,path,initialSize))`。后续 `createSegment`/`allocatePage` → `require(NORMAL)` 通过。
- **开/重开**：`openTablespace(spaceId,path)` → `pageStore.open` → `registry.open(spaceId)`（loader raw 读 page0 → 解 type、NORMAL）。或测试直接 `store.open` → 首次 `allocatePage` → `require()` 懒触发 loader（`pathOf` 取 path）→ 重建 metadata → 通过。
- **准入拦截**：`markTablespaceCorrupted(reason)` → registry 转 CORRUPTED → 后续 `createSegment`/`allocatePage`/... `require()` 抛 `TablespaceCorruptedException`；`openTablespaceForRecovery`/`requireForRecovery` 仍返回句柄。
- **autoextend**：`allocatePage` 空间不足 → `pageStore.extend` + `headerRepo.setCurrentSizeInPages`（权威）；registry size 快照不同步（§2.2）。

## 5. 错误模型

- 准入失败复用 fil 既有：`TablespaceCorruptedException`(CORRUPTED)、`TablespaceNotFoundException`(DISCARDED/未找到)、`TablespaceUnavailableException`(EMPTY/INACTIVE)。
- `TablespaceTypeFlags.decode` 未知 type code → `DatabaseValidationException`。
- loader：spaceId 不自洽 / pageSize 非法 → `DatabaseValidationException`；文件未开/读失败 → `Optional.empty()` → registry `TablespaceNotFoundException`。
- `openTablespace` 失败透传异常前 `pageStore.close(spaceId)`。
- 不新增异常类型；新增校验保留 message+cause。

## 6. 并发与边界

- registry 并发由 `CachingTablespaceRegistry` 既有 `ConcurrentHashMap` + `computeIfAbsent`/`compute` 桶级原子保证（§8.1 真实 lifecycle latch 留后续）。本片不新增锁、无 `synchronized`。
- loader raw `readPage` 不持 page latch、不开 MTR，故 `require()` 可在 `allocatePage` 的外层 MTR 内安全调用（无 MTR 嵌套、无 latch 重入）。
- 准入校验（`require`）在进入 fsp/buf/pageStore 写操作**之前**，不在持 page latch 后发起。

## 7. 测试

- `TablespaceTypeTest`：`code()`/`fromCode()` 往返；5 个 code 钉死（SYSTEM=0/FILE_PER_TABLE=1/GENERAL=2/UNDO=3/TEMPORARY=4）；`fromCode` 未知 code 抛 `DatabaseValidationException`。
- `TablespaceTypeFlagsTest`：5 type 编解码往返；`decode` 未知 type code（如低 3 位=5/6/7）抛；**高保留位不影响 type**（`decode(encode(UNDO) | 0x100) == UNDO`，钉死 future-flags 可扩展）。
- `CachingTablespaceRegistryTest`（增量）：`markInactive` 后 `require()` 抛 `TablespaceUnavailableException`、`requireForRecovery` 仍返回 INACTIVE 句柄、`find().state()==INACTIVE`。
- `SpaceHeaderRawCodecTest`：经 `headerRepo.initialize`(MTR) 写 page0 + commit/flush → raw 读字节 → `readPhysical` 字段与写入一致（spaceId/pageSize/flags/currentSize/freeLimit/version）。
- `PageZeroTablespaceMetadataLoaderTest`：`createTablespace(type)` + commit + pool close（刷盘）→ 新 store `open` → `loader.load` 得 metadata（type 复原、state=NORMAL、currentSize、path=pathOf）；未开文件 → empty；page0 spaceId 不符 → 抛。
- `DiskSpaceManagerTest` 增量：
  - `createTablespace` 后 `tablespaceState==NORMAL`，`createSegment`/`allocatePage` 通过。
  - `markTablespaceCorrupted` 后 `createSegment`/`allocatePage`/`usage` 抛 `TablespaceCorruptedException`；`markTablespaceInactive` 抛 `TablespaceUnavailableException`；`discardTablespace` 抛 `TablespaceNotFoundException`。
  - `openTablespaceForRecovery` 冒烟（fresh-store 重开：session1 `createTablespace`+flush+close → session2 新 store `openTablespaceForRecovery(spaceId,path)` 返回 NORMAL 句柄，证明 recovery 路径已接 `requireForRecovery`）；CORRUPTED 旁路语义（`requireForRecovery` 无视状态返回句柄）由既有 `CachingTablespaceRegistryTest` 覆盖——state 持久化后才在 DiskSpaceManager 层出现「从磁盘读到 CORRUPTED 仍可恢复访问」的可观测分叉（后续片）。
  - **reopen 懒加载**：session1 `createTablespace(type=UNDO)`+allocate+commit；session2 新 store `open`（直接 store.open）+ 新 `DiskSpaceManager` + `allocatePage` → `require()` 懒触发 loader（`pathOf` 取 path）→ NORMAL 通过，且 `tablespaceState==NORMAL`、（可校验 loader 解出的 type==UNDO）。
  - typed `createTablespace(UNDO)` 注册后类型经 flags 持久化、reopen 复原 UNDO。
- 回归：8 文件 30 处既有 `new DiskSpaceManager`/`createTablespace`/`openTablespace` 因重载零改动仍绿；record/btree/undo/redo/flush/recovery 全量绿（既有测试都先 `createTablespace`→NORMAL，gate 透明通过；直接 `store.open` 的 reopen 测试经 `pathOf`+loader 懒加载通过）。测试数只增不减。

## 8. 简化点与后续

- **空间管理准入，非全 IO 准入**：per-page 数据 IO（IndexPageAccess/UndoPageAccess）不拦 → operation-level lease / storage facade gate 后续片（仍不进 PageStore）。
- **state 不持久化**：lifecycle 标记仅运行时，跨重启不存活；page0 只存 type。state 落盘 + truncation/discard 留后续。
- **registry size/freeLimit 非实时**：权威在 page 0；快照仅 state/type 权威。
- **type 默认 GENERAL**：未用 typed 重载的调用方（含现有 undo/btree 测试 harness）注册为 GENERAL；undo 空间接 `UNDO` 是可选 follow-up（改 harness，不在本片强推以控 blast radius）。
- **page0 无 FSP_HDR 信封**：既有 gap，本片不补；loader 以 spaceId/pageSize sanity 替代 PageType 校验。补信封 = 独立 follow-up。
- **lifecycle 标记 = 运行时 registry 状态转换（不动文件）**：`markTablespaceInactive`=`registry.markInactive`（→INACTIVE）、`markTablespaceCorrupted(reason)`=`registry.markCorrupted`（→CORRUPTED）、`discardTablespace`=`registry.markDiscarded`（→DISCARDED）。本片**不关文件、不清 Buffer Pool stale**（沿用 registry 既有 deferred 设计，真实文件关闭/stale 由后续生命周期服务补）。注意区分：`openTablespace` 失败时的 `pageStore.close(spaceId)` 是「半开清理」，与 discard 不关文件不矛盾。
- loader 单条 `load`、无 discovery/loadAll；单文件表空间；无 DD↔FSP_HDR 跨源对账（沿用 fil 首版简化）。
- §8.1 真实 lifecycle latch 仍以 registry 桶级原子替代。

## 9. 自检

1. 范围限定「空间管理准入 + type 持久化 + page-0 loader + recovery 准入」；全 IO 准入/state 持久化/size 实时/信封/discovery 全列非目标。
2. type 编进既有 `spaceFlags`（低 3 位稳定 code、高位保留），零 page-0 格式变更；`decode` 拒未知 code、不拒高位（future-flags 可扩展），测试钉死。
3. loader 补齐 path 来源（`PageStore.pathOf`，覆盖直接 store.open）；raw 读不开 MTR（避免 require() 在外层 MTR 内嵌套）；create 走 `replace`（page0 未刷盘），open/懒加载走 loader。
4. autoextend 后 registry size 非实时——明确写进简化点；准入只用 state。
5. 门在 DiskSpaceManager 空间管理 API、PageStore 保持 state-free（不 import registry、不拒状态）；诚实声明非全普通 IO 门，per-page IO 不拦，后续 lease/facade gate。
6. 向后兼容重载保护 30 处既有调用；新增准入失败路径由 corrupted/inactive/discarded/recovery 测试覆盖；openTablespace 失败 close 防半开。
7. `SpaceHeaderLayout` 不散落 public 化——新增 public `SpaceHeaderRawCodec.readPhysical(ByteBuffer)`（只解物理字段、不解 FLST base）封装偏移，loader 只依赖它。
8. page0 无 FSP_HDR 信封作为既有 gap 显式列 follow-up，不在本片做无关重构；loader 用 spaceId/pageSize sanity 替代。
9. 依赖方向：DiskSpaceManager(api)→fil/fsp；loader(api)→fil(metadata/PageStore)+fsp(codec)；fsp 保持 fil-无关（page0 只存 int flags，type↔flags 在 fil/api）。
10. 并发沿用 registry 桶级原子，无新锁、无 synchronized、无 MTR 嵌套、无持 latch 阻塞等待。
11. 测试覆盖 type flags（含 future-flags）、raw codec、loader（含 reopen/path/sanity）、DiskSpaceManager 准入（NORMAL 通过 / corrupted-inactive-discarded 拦截 / recovery 旁路 / reopen 懒加载 / typed UNDO）、全量回归零倒退。
12. 简化点逐条标注后续片归属，无 TODO 占位。
