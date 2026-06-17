# Spec：D3 — MTR Redo Append Skeleton（物理字节区间 redo 最小闭环）

- 日期：2026-06-16
- 关联设计：`innodb-disk-manager-design.md`（第 4 步 MTR + redo append）、`innodb-redo-log-design.md`（仅前半：Lsn/redo record/append/pageLSN；writer/flusher/file/recovery 不做）、`innodb-record-design.md` §5.3（页信封）。
- 上游依赖：domain、common.exception、storage.buf（PageGuard/BufferPool）、storage.mtr（既有 MiniTransaction）、storage.fsp（既有信封，将迁出）。
- 前置：fsp 空间栈 + record R1–R5 全绿；MTR 已存在但为 no-redo skeleton；`FilePageHeaderLayout.PAGE_LSN` 槽已就位（offset 20）。
- 状态：D3 是 redo 模块的最小子集 —— 让「页写入产生物理字节区间 redo、MTR commit 盖 pageLSN」成立，但**不持久化、不回放**。

## 1. 背景与范围

MTR（`MiniTransaction`）现在只做 latch/fix memo + LIFO 释放，Javadoc 明言「commit 暂不产 redo / LSN / pageLSN」。D3 把这块补上，采用**物理字节区间 redo**：任何经 MTR 取得的页，其写入被捕获为 `PAGE_BYTES`；新页创建产生 `PAGE_INIT`；commit 给本 MTR 改过的页盖 `pageLSN`。

**做**：
- 新增共享包 `storage.page`，把页信封从 fsp 毕业过来（消除依赖反向与复刻）。
- `domain.Lsn`、`redo` 包（LogRange、RedoRecord、RedoLogManager 同步内存 append）。
- `buf` 加 `PageWriteListener` 钩子（默认 NO_OP，向后兼容）。
- retrofit `MiniTransaction`：collector 捕获写、`newPage(type)` 产 PAGE_INIT、commit append + 盖 pageLSN。

**不做**（注释标注，后续片）：
- writer/flusher、redo 文件、durability（`flushedToDiskLsn`）、recovery reader/applier → **R1**。
- `DiskSpaceManager.allocatePage()` 接 `mtr.newPage()` + 信封初始化 → **D4a**（见 §7，否则 PAGE_INIT 不会被实际触发）。
- record.page 的 MTR 生产入口 → **D4b**。
- 逻辑 redo（INSERT_RECORD/UPDATE_XDES 等语义记录）→ byte redo 稳定后可选加。
- buffer pool 的 dirty/oldest-LSN flush list → **F1**。

## 2. 依赖分层（关键：无环）

```
domain  <  buf  <  storage.page  <  redo  <  mtr  <  fsp / record
```
- **buf 不依赖 storage.page**（环就此避免）：pageLSN 盖戳由 mtr（可依赖 storage.page）做，不由 buf 做；buf 只提供通用字节读写 + 一个写监听钩子。
- `storage.page → buf`（访问器层用 PageGuard）；`redo → storage.page`（仅依赖纯层 `PageType`）；`mtr → buf/storage.page/redo`。
- fsp/record 改从 storage.page 取信封；二者互不依赖。

## 3. 关键决策（写进代码注释）

1. **物理字节区间 redo，phase-1 记录集 = `PAGE_BYTES` + `PAGE_INIT`（带页类型）**。逻辑 redo 后置。
2. **storage.page 分两层**：纯层（无 PageGuard import）`PageType` / 信封布局常量 / `FilePageHeader` 值对象；访问器层（import PageGuard）`PageEnvelope`（`stampPageLsn/readPageLsn` + 头读写）。**`RedoRecord` 只依赖纯层 `PageType`，禁止依赖 `PageEnvelope`**（编译期保证 redo 不间接拉入 buf）。
3. **挂 listener 不由 mtr 构造 PageGuard**：用 `PageGuard.attachWriteListener(PageWriteListener)`；mtr 在 `pool.getPage/newPage` 返回 guard 后挂 collector。
4. **commit 不读 `PageGuard.wrote` 私有状态**：`MtrRedoCollector` 维护 `touchedPages`（收到 PAGE_BYTES 或 PAGE_INIT 即标记），commit 只给 touchedPages 盖 pageLSN —— PAGE_INIT 后即便无额外字节写也能盖。
5. **LSN 粒度**：一次 `RedoLogManager.append(batch)` 分配一个 `LogRange(startLsn, endLsn)`，本 MTR 的所有 redo records 共享该 batch、所有 touched pages 盖 `endLsn`。`RedoRecord` 本身 D3 **不带 LSN 字段**；per-record/batch 的回放幂等元数据留 R1。
6. **pageLSN 盖戳排除出 redo**：commit 在 append 后 `collector.disable()`，再 `PageEnvelope.stampPageLsn(guard, endLsn)`——此写不被 collector 记录（决策来自前序讨论：先分配 LSN 再盖 pageLSN，盖戳不进本 MTR 的 redo batch）。
7. **写捕获按 readback**：listener armed 时，PageGuard 在写完后从 frame 读回 `[offset, offset+len)` 的实际字节交给 listener，保证 redo 字节与页上字节逐字节一致（免去 int/long 序列化端序假设）。

## 4. storage.page（页信封毕业）

新建包 `cn.zhangyis.db.storage.page`。从 `fsp` 迁入并按层归位：

**纯层（不 import PageGuard）：**
- `PageType`（enum，从 fsp 迁入；`fromCode` 改抛 `DatabaseValidationException`，脱离 `FspMetadataException`）。
- `PageEnvelopeLayout`（信封偏移常量：CHECKSUM/SPACE_ID/PAGE_NO/PREV/NEXT/PAGE_LSN/PAGE_TYPE + `FIL_PAGE_HEADER_BYTES=38`、`FIL_PAGE_TRAILER_BYTES=8`；= 现 fsp `FilePageHeaderLayout`/`FilePageTrailerLayout`/`PageLayouts` 合并）。
- `FilePageHeader`（值 record：spaceId/pageNo/prev/next/pageLsn/pageType + 校验 + `FIL_NULL`；**移除 PageGuard 版 writeTo/readFrom**，I/O 下沉到访问器层）。

**访问器层（import buf.PageGuard）：**
- `PageEnvelope`（静态工具）：
  - `void writeHeader(PageGuard, FilePageHeader)`、`FilePageHeader readHeader(PageGuard)`（= 原 FilePageHeader.writeTo/readFrom 的逻辑）。
  - `void stampPageLsn(PageGuard, Lsn)`（**仅写 header `PAGE_LSN` 偏移**）、`Lsn readPageLsn(PageGuard)`。
    - **范围澄清（避免误解）**：D3 的 pageLSN 只是恢复幂等用的 **header LSN**；InnoDB 还在页尾 trailer 存 LSN 低 32 位（FIL_PAGE_END_LSN）并算 checksum。D3 **不同步 trailer LSN / 不重算 checksum**——这些是 flush/checksum 切片（F1）的职责。D3 无 flush、纯内存，故页尾一致性留后续，勿误以为 D3 已维护完整页尾。
- `PageChecksum`（从 fsp 迁入；若其基于 PageGuard 则归访问器层，纯 byte[] 部分可归纯层）。

**迁移副作用：**
- fsp 改 import（`FilePageHeader`/`PageType`/`PageChecksum` 从 storage.page），调用点 `header.writeTo(g)` → `PageEnvelope.writeHeader(g, header)`、`FilePageHeader.readFrom(g)` → `PageEnvelope.readHeader(g)`。
- `record.IndexPageLayout` 的 `FIL_PAGE_HEADER_BYTES`/`FIL_PAGE_TRAILER_BYTES` 复刻改为引用 `PageEnvelopeLayout`，删对应钉死断言（不再复刻）。
- 迁移前对 `PageType`/`FilePageHeader`/`PageChecksum` 跑 `gitnexus_impact`；全量回归绿。各批的 impact 目标见 §11（PageGuard 已知 CRITICAL、MiniTransaction HIGH、MtrMemo MEDIUM，必须按 CLAUDE.md 在编辑前跑 impact 并对 HIGH/CRITICAL 先告警）。

## 5. domain.Lsn + redo 包

### 5.1 domain.Lsn
- 不可变值对象：`record Lsn(long value)`，`value>=0`；`ZERO`；`isAfter/compareTo`、`plus(long)`。单调递增的日志序号。

### 5.2 redo.LogRange
- `record LogRange(Lsn start, Lsn end)`，`end>=start`；表示一次 append 占据 `[start, end)`。`endLsn()` 即本批后第一个空闲 LSN。

### 5.3 redo.RedoRecord（sealed，纯值）
- `sealed interface RedoRecord permits PageBytesRecord, PageInitRecord`。
- `record PageBytesRecord(PageId pageId, int offset, byte[] bytes)`（offset/len 非负）。**防御性 copy 必须钉到实现**：canonical constructor `bytes = bytes.clone()`，并 override `byte[] bytes() { return bytes.clone(); }`（Java record 数组 accessor 默认返回内部数组，不 override 会泄漏可变状态——镜像 `ColumnValue.BinaryValue` 现有写法）。
- `record PageInitRecord(PageId pageId, PageType pageType)`。
- 仅依赖 `domain` + storage.page **纯层 PageType**；不依赖任何 repository/PageGuard（合规：redo record 定义不依赖具体实现）。
- `byteLength()`：估算字节数，供 append 推进 LSN（PAGE_BYTES = 头 + offset + bytes.length；PAGE_INIT = 头 + type）。具体编码格式（落盘）留 R1；D3 只需一致的长度推进规则。

### 5.4 redo.RedoLogManager
- `LogRange append(List<RedoRecord> records)`：在锁/原子下分配 LSN —— `start = nextLsn; end = start + Σ record.byteLength(); nextLsn = end`；把 records 按 append 顺序追加到**内存 buffer**（`List<RedoRecord>`，D3 不给单条记录附 LSN——per-record LSN 元数据留 R1）；返回 `LogRange(start, end)`。
- D3 **无文件、无 fsync、无后台线程**。并发：单 `ReentrantLock` 或 `AtomicLong` 保护 nextLsn + buffer 追加（无 synchronized）。
- 暴露只读视图供测试：`List<RedoRecord> bufferedRecords()`（返回不可变快照 `List.copyOf`，元素 PageBytesRecord 的 `bytes()` 已返回 clone）、`Lsn currentLsn()`。

## 6. buf 改动（一个钩子，向后兼容）

- 新增 `PageWriteListener`（接口）：`void onWrite(PageId pageId, int offset, byte[] newBytes)`；常量 `NO_OP`。**签名只用 domain + 原语，不含 redo 类型**（依赖倒置的接缝）。
- `PageGuard`：
  - 持字段 `PageWriteListener listener = PageWriteListener.NO_OP;`
  - `void attachWriteListener(PageWriteListener)`（mtr 在 fix 后调用；null 视为 NO_OP）。
  - `writeInt/writeLong/writeBytes`：写完后若 `listener != NO_OP`，按 readback 取 `[offset, len)` 实际字节，调 `listener.onWrite(frame.pageId, offset, bytes)`。`markDirty()` 不触发（无字节变化）。
- 非 MTR 的 `pool.getPage`（现有 R1–R5 测试）listener 恒 NO_OP → **零行为变化**。
- buf **不** import storage.page / mtr / redo。

## 7. mtr 改动（retrofit MiniTransaction）

### 7.1 MtrRedoCollector（mtr 内，implements buf.PageWriteListener）
- 状态：`List<RedoRecord> records`、`Set<PageId> touchedPages`、`boolean enabled = true`。
- `onWrite(pageId, offset, bytes)`：`if(!enabled) return;` 追加 `PageBytesRecord(pageId, offset, bytes.clone())`；`touchedPages.add(pageId)`。
- `recordInit(pageId, type)`：`PageInitRecord` 追加；`touchedPages.add(pageId)`。
- `disable()`：置 `enabled=false`（commit 盖 pageLSN 前调用，排除 LSN 写）。
- 每个 MiniTransaction 持有一个 collector 实例（成员）。

### 7.2 getPage / newPage
- `getPage(pool, pageId, mode)`：现逻辑取 guard 后 `guard.attachWriteListener(collector)`，再入 memo、返回。
- `newPage(pool, pageId, mode, PageType type)`：**新增 type 参**；取 guard 后 `attachWriteListener(collector)`、`collector.recordInit(pageId, type)`、入 memo、返回。（**当前生产路径无 `mtr.newPage` 调用**——经核实，唯一的 `.newPage(` 在 `MiniTransaction` 内部调 `pool.newPage`；`MiniTransaction.newPage(...)` 只有 MTR 测试在用。故 D3 只改 MTR 测试以传 type；接入 FSP/DSM 分配路径是 **D4a**。）

### 7.3 commit（ACTIVE→COMMITTING→COMMITTED）
顺序（单页 X latch 内，确定性）：
1. `LogRange range = redoLogManager.append(collector.records);`（空 records 则 append 空批，range 退化为 [cur,cur)，无 touched 页则跳过盖戳）。
2. `Lsn endLsn = range.end();`
3. `collector.disable();`（其后所有页写不再进 redo）。
4. 对 `collector.touchedPages` 每个 pageId：经 memo 取该页（X）guard，`PageEnvelope.stampPageLsn(guard, endLsn)`。
5. `memo.releaseAll();` → COMMITTED。
- `MiniTransaction` 构造接 `RedoLogManager`（由 `MiniTransactionManager` 注入）。`rollbackUncommitted` 不 append、不盖 pageLSN（丢弃 collector.records；页内容不撤销，沿用现有「无 content undo」语义）。
- memo 按 pageId 取 guard：`MtrMemo` 增 `PageGuard guardFor(PageId)`（返回该页最近一次 X fix 的 guard；无则异常——touchedPages 必有对应 X guard，见 §7.5 保证）。

### 7.5 rollbackToSavepoint 与 touched page 的不变量（必修，否则 commit 盖戳取不到 guard）
现状 `rollbackToSavepoint` 调 `memo.releaseTo(depth)` 会 close 保存点之后获取的 guard。若一个**已写过（touched）**的页 guard 在该范围内被释放，commit 时 `guardFor(pageId)` 取不到它 → 无法盖 pageLSN。
- **规则（最小实现）**：`rollbackToSavepoint` 释放前先检查——若待释放（depth>savepoint.depth）的任一 guard 的 pageId ∈ `collector.touchedPages`，则**拒绝**，抛 `MtrStateException`（把现有 Javadoc 的「建议只对未修改页使用」升级为强制不变量）。保存点之前固定、之后才写的页不受影响（其 guard 在保存点下方，不在释放范围）。
- 测试：savepoint → getPage(X)+写该页（touched，guard 在保存点上方）→ `rollbackToSavepoint` 抛 `MtrStateException`；未写的页在保存点后取再 rollback 则正常释放。

### 7.4 recovery 契约（仅注释，R1 实现）
apply 一条 batch 的 records 后，把相关页 `pageLSN` 推到该 batch 的 `endLsn`；回放幂等以 `pageLSN >= 待应用 LSN` 跳过。per-record LSN 元数据形态 R1 定。

## 8. 异常与并发

- 复用 `DatabaseValidationException`（参数/状态）、`MtrStateException`（MTR 生命周期）。`PageType.fromCode` 未知码抛 `DatabaseValidationException`。
- 并发：`RedoLogManager` 用显式锁/Atomic 保护 LSN 分配与 buffer 追加；禁 synchronized。MTR 单线程拥有（现状）。PageGuard listener 单线程（guard 本就单线程用）。
- WAL 不变量：D3 只盖 on-page pageLSN，不刷盘；flush 的 WAL gate 在 F1（届时校验 `redo.flushedToDiskLsn >= page.pageLSN`）。

## 9. D3 不做但必须显式记录的衔接点

- **D4a 前置**：`DiskSpaceManager.allocatePage()` 当前只在 XDES 位图标记分配并返回 PageId，**不调 `mtr.newPage()`、不写 FilePageHeader**。D4a 必须让「分配 + 首次使用」路径走 `mtr.newPage(pageId, X, type)` 并由 `PageEnvelope.writeHeader` 盖信封，PAGE_INIT 与首批 PAGE_BYTES 才会真正产生。否则 D3 的 PAGE_INIT 机制空转。
- **D4b**：record.page 经 MTR-owned guard 创建 RecordPage 的生产入口（R3–R5 算子签名不变）。
- **R1**：writer/flusher、redo 文件、durability、recovery reader/apply dispatcher、per-record LSN 幂等。

## 10. 测试（自包含，驱动 MiniTransaction，不碰 fsp/record 生产路径）

经真实 BufferPool/PageGuard（16KB 页）+ 注入的 RedoLogManager：
> 注意：commit 会 release memo → `PageGuard.close()`，**关闭后再读会抛异常**。凡 commit 后要验证页内容/pageLSN 的断言，必须**重新 `pool.getPage(pageId, SHARED)`** 读取，读完关闭。
- `appendsPageBytesOnWrite`：begin → getPage(X) → writeInt/writeBytes 若干 → commit → 断言 `bufferedRecords` 内 PAGE_BYTES 的 (pageId/offset/bytes) 与所写一致、顺序正确、`currentLsn` 推进。
- `stampsPageLsnOnTouchedPages`：commit 后**重新 getPage(SHARED)** → `PageEnvelope.readPageLsn(g2)`==endLsn；**且 redo buffer 中没有为 PAGE_LSN 偏移产生的 PAGE_BYTES**（验证盖戳排除）。
- `newPageEmitsPageInit`：`newPage(type)` → 断言一条 `PageInitRecord(pageId,type)`；即便其后无额外字节写，commit 后**重新 getPage(SHARED)** 验证该页 pageLSN==endLsn（验证 touchedPages 含 PAGE_INIT 页）。
- `multiPageMtrStampsAllTouched`：两页各写 → commit 后逐页**重新 getPage(SHARED)** 验证都盖同一 endLsn；只读（S）页不产 redo、不在 touchedPages、不盖戳。
- `rollbackToSavepointRejectsTouchedPage`：savepoint → getPage(X)+写 → `rollbackToSavepoint` 抛 `MtrStateException`（§7.5）。
- `noListenerNoRedo`：直接 `pool.getPage`（非 MTR）写 → NO_OP → redo buffer 为空（向后兼容）。
- `lsnMonotonic`：连续两个 MTR 的 LogRange 首尾相接、单调；空 records 的 commit 不前移/正确退化。
- 迁移回归：storage.page 迁移后 fsp 与 record 全量测试绿（信封语义不变）。

## 11. 批次拆分（writing-plans 细化）

每批开工前对「编辑目标」跑 `gitnexus_impact`，HIGH/CRITICAL 先向用户告警再改（CLAUDE.md 强制）：

- **D3a**：storage.page 包（迁移 + 分层 + PageEnvelope.stampPageLsn/readPageLsn）；fsp/record import 更新；回归。
  - impact：`PageType`、`FilePageHeader`、`PageChecksum`、`FilePageHeaderLayout`/`PageLayouts`（信封迁移波及 fsp 全体 + record IndexPageLayout）。
- **D3b**：domain.Lsn + redo（LogRange/RedoRecord/RedoLogManager 内存 append）。纯新增，无 impact。
- **D3c**：buf PageWriteListener + PageGuard.attachWriteListener + 写回调。
  - impact：**`PageGuard`（CRITICAL，~89 impacted / 45 direct）**——改动为纯增量（新增字段/方法 + 写后回调；既有签名/语义不变，listener 默认 NO_OP），blast radius 大但行为兼容；编辑前告警 + 全量回归把关。
- **D3d**：mtr retrofit（MtrRedoCollector、newPage(type)、commit append+stamp、memo.guardFor、§7.5 savepoint 不变量）。
  - impact：**`MiniTransaction`（HIGH，~68 impacted / 19 direct）**、**`MtrMemo`（MEDIUM）**；`newPage` 加 type 参会改其调用方（经核实仅 MTR 测试）。编辑前告警。
- 每批 TDD → 全量 `clean test`（固定 JDK/Gradle）→ `gitnexus_detect_changes` 核对范围 → 收口刷 `npx gitnexus analyze`。D3 全绿后报告；D4a 单独确认再开。
