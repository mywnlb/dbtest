# Spec: T1.3a — 物理 Undo 存储基座（undo page + INSERT undo codec + RollPointer 寻址）

- 日期：2026-06-17
- 关联设计：`docs/design/innodb-undo-log-purge-design.md` §5.1（`UndoNo`/`RollPointer` 值对象）、§6.2（undo tablespace）、§6.4（undo page 与 undo log header）、§6.5（`INSERT_ROW` payload）、§16（实现顺序 step 1/2/5/6）；`docs/design/innodb-record-design.md` §5.3（FIL 信封）、§7（page header 子集）。
- 前置：T1.1+T1.2（`TransactionId`、`RollPointer`（7B NULL-only codec）、`HiddenColumns`、聚簇 `insertClustered` 盖戳）；record R1-R5；redo D3/D4（`PAGE_INIT`/`PAGE_BYTES` 物理 redo + commit pageLSN）；FSP `DiskSpaceManager`（`createTablespace`/`createSegment(UNDO)`/`allocatePage`）；MTR `MiniTransactionManager`。
- 状态：T1.3「物理 undo + 真 rollback」epic 的第一子片（**横切基座**）。目标是一个 redo 保护的物理 undo 日志：能把一条 INSERT undo record 追加到真实 undo 页、得到非 NULL `RollPointer`、再由该指针读回。**完全不接事务/btree**。

## 0. epic 拆分与本片定位

「物理 undo + 真 rollback」按以下子片推进，各自独立 spec → plan → 实现：

- **T1.3a（本片）：undo 存储基座。** undo 表空间 + UNDO segment、undo page 格式、`UndoPageAccess`（MTR/redo）、`UndoRecordCodec`（INSERT undo 字节往返）、`RollPointer` 真实寻址（单 undo space）、`UndoNo`。`append(record)→RollPointer`、`read(RollPointer)→record`。不碰 `Transaction`/rollback/btree。
- **T1.3b：undo 分配 + 写路径。** rollback segment header/slot、`UndoContext`（挂 `Transaction`）、`UndoLogManager.beforeInsert`，让 `insertClustered` 写 INSERT undo 并把 `DB_ROLL_PTR` 改成真实指针（替换当前恒 NULL）。
- **T1.3c：真 rollback。** btree 物理删除路径、`RollbackService.rollback(FULL)` 反向走 insert undo 链、`TransactionManager.rollback` 驱动 undo 应用并释放 undo；commit 把 insert undo 标记 reusable。
- **T1.3d+：** UPDATE/DELETE undo + history list + commit-to-history → MVCC 读 → purge → 崩溃恢复 rollback → truncation/多 rseg/多 undo 表空间。

## 1. 范围

**做：**

- 值对象 `cn.zhangyis.db.domain.UndoNo`（`long value`，≥0，0=`NONE`，单调由写者分配；本片测试给字面值）。
- `cn.zhangyis.db.storage.page.PageType` 追加常量 `UNDO(6)`（append，不改既有 code，`PageTypeTest` 不倒退）。
- `cn.zhangyis.db.storage.undo` 新增：
  - 枚举 `UndoLogKind`（`INSERT`/`UPDATE`/`TEMPORARY`，本片 header 仅用 `INSERT`）、`UndoRecordType`（`INSERT_ROW`/`UPDATE_ROW`/`DELETE_MARK`，codec 仅实现 `INSERT_ROW`）。
  - `UndoRecord`（`INSERT_ROW` 命令对象，见 §4）。
  - `UndoRecordCodec`（`UndoRecord ↔ byte[]`，复用 `TypeCodecRegistry` 编 cluster key）。
  - `UndoPageLayout`（undo page header 字段偏移常量）。
  - `UndoPage`（PageGuard 上的 undo 页视图：`format`/`appendRecord`/`recordAt`/header 访问器）。
  - `UndoPageAccess`（MTR 生产入口 `createUndoPage`/`openUndoPage`，仿 `IndexPageAccess`）。
  - `UndoLog`（串接三者：`append(UndoPage, UndoRecord)→RollPointer`、`readRecord(UndoPage, RollPointer)→UndoRecord`；单 undo space + 单页假设落此）。
  - 异常 `UndoPageOverflowException`、`UndoLogFormatException`。

**不做（本片非目标，移交 T1.3b/c+）：**

- 不接 `Transaction`/`TransactionManager`/`UndoContext`/`UndoLogManager.beforeInsert`。
- 不改 `insertClustered`、不写 `DB_ROLL_PTR`（聚簇记录 `DB_ROLL_PTR` 仍恒 NULL）。
- 不做 btree 物理删除、不做 rollback、不做 UPDATE/DELETE undo。
- 不做 rollback segment slot directory、不做 history list、不做 MVCC 旧版本构造、不做 purge、不做恢复期 rollback、不做 undo truncation。
- **不做多页 undo 链**：单 undo page 内追加；页满抛 `UndoPageOverflowException`。格式预留 FIL next 链字段供 T1.3b 接多页。
- 不做多 rollback segment / 多 undo 表空间：单 undo space、单 undo log 假设。

## 2. 关键决策

1. **横切基座先行**：复刻 T1.1/T1.2「先落承重物理格式、全量回归保护，再接线」的节奏。undo page 格式 + codec + redo 是承重物理件，隔离实现与测试、零接线风险；写路径与真 rollback 在后续子片接入。
2. **单页、单 undo log 假设**：本片只在一个 undo page 内追加；undo page header 与 undo log header **合并为一个定长头**（含 `transactionId`/`undoKind`/`state`）。InnoDB 中二者分离（segment 首页才有 undo log header，后续页只有 undo page header）——合并是单页简化点，T1.3b 引入 segment 页链 + rollback segment 时再拆分。
3. **`RollPointer` 不改格式**：现有 `{insert flag, pageNo u32, offset u16}` 7B codec 在「单 undo space」假设下已能寻址（space 由唯一 undo 表空间隐含）。本片只是首次**产出非 NULL** 指针：`append` 返回指向新 undo record 槽起点的 `RollPointer`。多 rseg/多 undo 表空间的 rseg-id 编码留 T1.3d+。
4. **undo 页写全程经 MTR-owned `PageGuard`**：`createUndoPage` 走 `mtr.newPage(X, UNDO)` 产 `PAGE_INIT(UNDO)`；`appendRecord`/header 写经 guard 字节写产 `PAGE_BYTES`；commit 盖 pageLSN。与 INDEX 页同机制，物理 redo 幂等、crash-safe 由现有 D3/D4 保证，本片不新增 redo 类型。
5. **`UndoPageAccess` 置 `storage.undo`**（非 `storage.api`）：它是 undo 内部页生产入口，不是对上层 SQL 的稳定 Facade；`storage.api` 仅保留 `IndexPageAccess` 等跨模块稳定 API。
6. **cluster key 复用 `TypeCodecRegistry`，但 codec 自带 per-column framing**：`INSERT_ROW` 的 cluster key 逐列按类型编码，每列自带 `[nullFlag][len][bytes]`（undo record 无 NullBitmap/变长目录，不能依赖 record 路径的 null/长度处理）。codec 入参需 `IndexKeyDef`（定位 key 列与序）+ `TableSchema`（提供 `ColumnType`，因 `IndexKeyDef` 不含类型），不在 undo record 内自描述类型。

## 3. 值对象

### `UndoNo`（`cn.zhangyis.db.domain`）
- 不可变 `record UndoNo(long value)`，`value >= 0`，越界抛 `DatabaseValidationException`。
- `0` 为 `NONE` 哨兵；`UndoNo.NONE`、`isNone()`、`of(long)`。
- 语义：事务内 undo record 序号，单调，用于后续 savepoint 边界（T1.3c+）。本片仅作 undo record 字段与 page header `lastUndoNo` 落盘值，测试给字面值。
- **真实 undo record 的 `undoNo` 必须 > 0（非 `NONE`）**：与 page header `lastUndoNo=0` 表「空页」语义不冲突——`NONE` 仅用于「无 undo」哨兵，不得落到真实 record。该约束在 `UndoRecord` 构造器与 `UndoPage.appendRecord` 双重前置校验（见 §4.2、§4.3）。

### `RollPointer`（复用，不改）
- 维持现有 7B codec。本片新增**使用方式**：`append` 产出 `new RollPointer(true, undoPageNo, recordSlotOffset)`，`isNull()==false`。
- `offset` 指向 undo record **槽起点**（即长度前缀 `len u16` 的位置），与 `UndoPage.recordAt(offset)` 约定一致。

## 4. undo page 格式与 INSERT undo record

### 4.1 undo page 布局（FIL 信封内）

```
[FIL header 38B][undo page header 22B][undo record area → 向后追加][free][FIL trailer 8B]
```

undo page header（`UndoPageLayout`，紧接 FIL body 起始 = 38，同 `IndexPageLayout.PAGE_HEADER_START`）：

| 偏移 | 字段 | 宽度 | 语义 |
| --- | --- | --- | --- |
| 38 | `UNDO_KIND` | u8 | `UndoLogKind` ordinal（本片恒 `INSERT`） |
| 39 | `STATE` | u8 | undo log 状态占位（本片恒 `ACTIVE`，状态机留 T1.3b+） |
| 40 | `TRANSACTION_ID` | u64 | 该 undo log 所属事务 id（本片测试给字面值） |
| 48 | `FREE_OFFSET` | u16 | 下一条记录追加位置；`format` 初始化为 record area 起点 60 |
| 50 | `RECORD_COUNT` | u16 | 已追加 undo record 数 |
| 52 | `LAST_UNDO_NO` | u64 | 最近一条 record 的 `undoNo`（0=空；与 record payload 内 `undoNo` 同宽，避免截断歧义） |

- record area 起点 `RECORD_AREA_START = 60`。
- 多页链的 `nextPageNo` 复用 FIL header next 链（本片保留不写，T1.3b 接多页）。
- undo record 槽 = `[len u16][payload len 字节]`；`len` 为 `UndoRecordCodec` 产出的 payload 字节数（不含 len 前缀）。

### 4.2 `UndoPage`（PageGuard 视图）

- `format(UndoLogKind kind, TransactionId txnId)`：写 header（kind/state=ACTIVE/txnId、freeOffset=60、recordCount=0、lastUndoNo=0）。要求页已由 `createUndoPage` 建为 UNDO 页。
- `appendRecord(byte[] payload, UndoNo undoNo) -> int`：先校验 `undoNo` 非 `NONE`（否则 `DatabaseValidationException`，避免污染 `lastUndoNo=0` 的空页语义）；在 `freeOffset` 写 `[len][payload]`；推进 `freeOffset += 2 + len`、`recordCount += 1`、`lastUndoNo = undoNo`；返回槽起点 offset。**溢出判定**：若 `freeOffset + 2 + len > pageSize - FIL_TRAILER`（`FIL_TRAILER` 取 `PageEnvelopeLayout` 尾部常量），抛 `UndoPageOverflowException`（写页前判定，不留半改页）。
- `recordAt(int offset) -> byte[]`：读 `len u16`，返回 `[offset+2, offset+2+len)`；`offset`/`len` 越界（出 record area 或越 trailer）抛 `UndoLogFormatException`。
- header 访问器：`undoKind()`/`state()`/`transactionId()`/`freeOffset()`/`recordCount()`/`lastUndoNo()`。
- 无状态于页之外：所有状态在页字节里，要求调用方持页 X latch（写）或 S latch（读）。

### 4.3 `UndoRecord`（`INSERT_ROW`）

不可变对象，字段：

| 字段 | 类型 | 语义 |
| --- | --- | --- |
| `type` | `UndoRecordType` | 本片恒 `INSERT_ROW` |
| `undoNo` | `UndoNo` | 事务内序号 |
| `transactionId` | `TransactionId` | 写入该 undo 的事务 |
| `tableId` | `long` | 表 id（T1.3c rollback 定位用） |
| `indexId` | `long` | 聚簇索引 id（T1.3c 定位用） |
| `clusterKey` | `List<ColumnValue>` | 主键列值（顺序对应 `IndexKeyDef.parts()`），供 rollback 反查并物理删除未提交插入；可含 null 列值 |
| `prevRollPointer` | `RollPointer` | 事务反向 undo 链前驱（本片仅字段往返；NULL 与非 NULL 各测） |

校验：`type==INSERT_ROW`；非空字段非 null；`clusterKey` 非空列表；**`undoNo` 非 `NONE`（> 0）**。

### 4.4 `UndoRecordCodec`

**为什么需要 `TableSchema`（不可只传 `IndexKeyDef`）**：`IndexKeyDef.parts()` 只有 `ColumnId`/`KeyOrder`/`prefixBytes`，**无 `ColumnType`**；而 `TypeCodecRegistry.codecFor(ColumnType)` 必须按类型取 codec。且 `TypeCodec` 约定 **NULL 不由 codec 处理**（record 路径靠 NullBitmap）、变长 codec（`VarBytesCodec`）**不自带长度**（record 路径靠变长目录）。undo record 没有 NullBitmap/变长目录，故 codec 必须**自带 per-column framing**，并由 `TableSchema` 提供类型。

- `encode(UndoRecord rec, IndexKeyDef keyDef, TableSchema schema) -> byte[]`：布局
  `[type u8][undoNo u64][transactionId u64][tableId u64][indexId u64][prevRollPointer 7B][keyColCount u8]` 后跟 `keyColCount` 个 **自带 framing 的 key 列**：
  - 每列：`[nullFlag u8]`；`nullFlag==0` 时再写 `[len u16][bytes len 字节]`，`bytes` = 该列 `TypeCodec.encode` 输出；`nullFlag==1` 时无后续字节。
  - 第 i 列类型解析：`type_i = schema.column(keyDef.parts().get(i).columnId().value()).type()`（`columnId==ordinal==列序`，由 `TableSchema` 不变量保证）。定长类型 `len` 冗余但保留，换取 fixed/variable 统一解码路径与健壮性。
- `decode(byte[] buf, int off, IndexKeyDef keyDef, TableSchema schema) -> UndoRecord`：逆向解析；逐列读 `nullFlag`→（非 null）`len`→切片→`TypeCodec.decode` 还原 `ColumnValue`（null 列还原为对应 null `ColumnValue`）。落盘 type 字节非 `INSERT_ROW`、缓冲不足、`keyColCount` 与 `keyDef.parts().size()` 不符、`len` 越界 → `UndoLogFormatException`。
- `encode` 收到 `UPDATE_ROW`/`DELETE_MARK` 抛 `DatabaseValidationException`（"unsupported undo record type in T1.3a"）。

### 4.5 `UndoPageAccess`（MTR 生产入口，仿 `IndexPageAccess`）

- `createUndoPage(mtr, pageId, UndoLogKind, TransactionId) -> UndoPage`：`mtr.newPage(X, PageType.UNDO)` → 写信封(UNDO) → `UndoPage.format(...)`。校验全部前置于写页前。**破坏性入口**（走 newPage 会清零重初始化），仅用于新分配/有意重初始化的 undo 页。
  - **同一 MTR 内双 `newPage` 语义（承重，须显式 + 测试）**：测试路径先 `DiskSpaceManager.allocatePage`（内部 `mtr.newPage(ALLOCATED)` + 写信封），再 `createUndoPage`（`mtr.newPage(UNDO)`），同页在一个 MTR 批次产生两条 `PAGE_INIT`（ALLOCATED 后 UNDO）。这是**既有 INDEX 路径的同款模式**——`ClusteredInsertTest.createTablespaceAndRoot` 在同一 MTR 内 `allocatePage` 后 `createIndexPage`，由 D4a（newPage 对驻留页清零重初始化）+ redo 顺序覆盖保证最终态为 UNDO。本片须有测试断言 commit + reload 后页类型为 UNDO、header 为 format 后初值（覆盖该 redo 顺序），不靠隐式假设。
- `openUndoPage(mtr, pageId, mode) -> UndoPage`：`mtr.getPage(mode)` → 读 `PageEnvelope` 的 `FilePageHeader.pageType()`，**非 `PageType.UNDO` 抛 `UndoLogFormatException`**（防止把 ALLOCATED/INDEX 页按 undo header 误解释）→ 包成 `UndoPage`，用于已存在 undo 页的追加（X）或读回（S）。

### 4.6 `UndoLog`（基座 Facade）

- `append(UndoPage page, UndoRecord rec, IndexKeyDef keyDef, TableSchema schema) -> RollPointer`：`codec.encode(rec, keyDef, schema)` → `page.appendRecord(payload, rec.undoNo())` 得 offset → 返回 `new RollPointer(true, page.pageId().pageNo(), offset)`。
- `readRecord(UndoPage page, RollPointer rp, IndexKeyDef keyDef, TableSchema schema) -> UndoRecord`：校验 `!rp.isNull()` 且 `rp.pageNo().equals(page.pageId().pageNo())`（值对象用 `equals`/`value()` 比较，**不可用 `==`**；不符抛 `UndoLogFormatException`）→ `page.recordAt(rp.offset())` → `codec.decode(payload, 0, keyDef, schema)`。
- 持有 `UndoRecordCodec`，无其它可变状态。单 undo space 假设（不校验 space）写进类注释。

## 5. 错误模型

- `DatabaseValidationException`：null 入参；`UndoNo` 负值；真实 `UndoRecord`/`appendRecord` 的 `undoNo==NONE`；`UndoRecordCodec.encode` 遇非 `INSERT_ROW` 类型；`UndoRecord` 形状非法（空 clusterKey 等）。
- `UndoPageOverflowException`（新增，extends `DatabaseRuntimeException`）：undo record 放不下当前页（写页前判定）。
- `UndoLogFormatException`（新增，extends `DatabaseRuntimeException`）：解码越界/损坏、`keyColCount` 不符、`recordAt` offset 出 record area、`readRecord` 的 `RollPointer` 与页不符、`openUndoPage` 信封页类型非 `UNDO`、落盘 type 非法。
- 复用 `PageType.fromCode` 既有损坏异常；不新增其它物理页异常。

## 6. 并发与边界

- `UndoPage`/`UndoPageAccess`/`UndoLog`/`UndoRecordCodec` 均无跨调用可变状态；并发模型沿用 record/btree：调用方在其 MTR 内持 undo 页 X latch（追加）或 S latch（读回），latch 入 mtr memo，commit/rollback 释放。
- 本片不引入任何事务锁、行锁、后台 worker、可阻塞等待；无 `synchronized`（沿用项目约束）。
- **不得在已持 undo page latch 后发起可能阻塞的外部等待**；buffer miss 的读盘、`allocatePage` 的文件扩展发生在获取目标页 latch 的过程中或之前，由 buffer pool fix / FSP 流程承担（沿用 record/btree 同款边界），不是在持有 undo 页 latch 后再发起的阻塞等待。

## 7. 恢复边界

- 本片不新增 redo 类型、不新增恢复编排：undo 页的 `PAGE_INIT(UNDO)` + 信封/header/record 的 `PAGE_BYTES` 由现有 D3/D4 物理 redo 幂等覆盖，commit 盖 pageLSN。
- 性质：MTR commit 后 evict/reload 该 undo 页，已追加记录逐字节完好（pageLSN ≥ 应用过的 redo end LSN）。崩溃后基于物理 redo replay 重建 undo 页字节一致——但**重建 undo 的逻辑语义（rollback/history/purge resume）不在本片**，留 T1.3c+ 与恢复片。

## 8. 测试

- `UndoNoTest`：`NONE` 为 0、`isNone()`、负值拒绝、`of`/`value` 往返。
- `PageTypeTest`（既有，回归）：`UNDO` code=6、既有 code 不变、`fromCode(6)==UNDO`、未知 code 仍抛。
- `UndoRecordCodecTest`：INSERT undo 往返（传 `IndexKeyDef`+`TableSchema`）——多列 typed cluster key（int + varchar），含 **null key 列**（验 `[nullFlag]` framing）、`prevRollPointer` NULL 与非 NULL、`undoNo`/ids 全字段等值；`encode(UPDATE_ROW)` 抛 `DatabaseValidationException`；`undoNo==NONE` 的 `UndoRecord` 构造抛 `DatabaseValidationException`；截断缓冲/`keyColCount` 不符 `decode` 抛 `UndoLogFormatException`。
- `UndoPageTest`（可用最小 PageGuard/内存帧 harness 或 onPool）：`format` 后 header 初值正确（freeOffset=60、recordCount=0、lastUndoNo=0）；append N 条后 `recordCount`/`freeOffset`/`lastUndoNo` 推进；`recordAt(offset)` 取回追加字节；`appendRecord(undoNo=NONE)` 抛 `DatabaseValidationException`；append 至溢出抛 `UndoPageOverflowException`；`recordAt` 越界抛 `UndoLogFormatException`。
- `UndoLogStoreTest`（onPool harness，复用 `ClusteredInsertTest` 的 `FileChannelPageStore`+`LruBufferPool`+`DiskSpaceManager` 模式）：建 undo 表空间 → `createSegment(UNDO)` → `allocatePage` → `createUndoPage` → `UndoLog.append` 得 `RollPointer`（`isNull()==false`、`pageNo`/`offset` 指向真实槽）→ 同 MTR 内 `readRecord(rp)` 等值原 `UndoRecord`；追加多条、各自 `RollPointer` 读回正确；`prevRollPointer` 串两条记录后能从后者读到前者指针。
- 双 `newPage` 顺序：同一 MTR `allocatePage`(ALLOCATED) 后 `createUndoPage`(UNDO)，commit + 新 MTR reload 后页类型为 `UNDO`、header 为 format 初值（钉死两条 `PAGE_INIT` 的 redo 顺序最终态）。
- 页类型守门：`openUndoPage` 打开一个 ALLOCATED 或 INDEX 页抛 `UndoLogFormatException`。
- 持久化：append+commit 后新 MTR `openUndoPage` 重读，记录完好（覆盖 pageLSN/buffer 落盘路径）。
- 回归：全量 Gradle `test` 通过，测试数只增不减（具体数由实际输出记录，不写死）。

## 9. 简化点与后续

- 单 undo page、单 undo log：多页链（`nextPageNo` FIL 链）、segment 页分配在 T1.3b 写路径接入；undo page header 与 undo log header 届时拆分。
- `RollPointer` 单 undo space 寻址；多 rseg-id/多 undo 表空间编码留 T1.3d+。
- `UndoRecordType` 仅 `INSERT_ROW`；`UPDATE_ROW`/`DELETE_MARK` payload（old image/changed columns）留 T1.3d。
- `UndoLogKind`/`state` 仅落盘占位；undo log 状态机（`ACTIVE`/`COMMITTED_IN_HISTORY`/...）留 T1.3b/c。
- `transactionId`/`tableId`/`indexId`/`clusterKey` 落盘但本片不被消费；T1.3c rollback 用 `indexId`+`clusterKey` 反查并物理删除未提交插入。
- **INSERT_ROW payload 不存 inserted hidden columns（DB_TRX_ID/DB_ROLL_PTR），与设计 §6.5 的简化点**：insert undo 的旧版本不存在，回滚只需「按 key 定位并物理删除该未提交插入」。T1.3c 的「确认找到的是同一条未提交插入」校验不依赖落盘的 hidden columns，而是：按 `(indexId, clusterKey)` 定位聚簇记录后，读其页内 `DB_TRX_ID` 与 undo record 的 `transactionId` 比对（应相等，因 insert 时盖的就是本事务 id），并确认记录未被 delete-mark；不符则视为该插入已被其它路径处理/记录损坏，按 T1.3c 错误策略处理（不静默删错记录）。该校验语义在 T1.3c spec 细化。

## 10. 自检

1. 范围严格限定 undo 物理基座（页格式 + INSERT codec + RollPointer 寻址 + redo），事务/btree/rollback/UPDATE/DELETE/history/purge/recovery 全列为非目标。
2. 横切先行，承重物理件隔离测试；写路径与真 rollback 在 T1.3b/c。
3. `RollPointer` 不改格式，单 undo space 假设明确；多 rseg 编码后移。
4. undo 页写经 MTR-owned guard，复用 D3/D4 物理 redo，不新增 redo 类型/恢复编排。
5. `PageType.UNDO(6)` append 不破既有 code；`PageTypeTest` 回归守护。
6. undo page header 字段/偏移具体可落地（22B，[38,60)，record area 起 60），溢出/越界判定明确。
7. INSERT undo payload 字段集足以支撑 T1.3c 物理删除（indexId+clusterKey）；codec 入参 `IndexKeyDef`+`TableSchema`（`IndexKeyDef` 无 `ColumnType`），每列自带 `[nullFlag][len][bytes]` framing（不依赖 NullBitmap/变长目录）；不存 inserted hidden columns 的简化点与 T1.3c 校验语义已在 §9 注明。
8. `UndoPageAccess` 置 `storage.undo`，依赖方向 undo→buf/mtr/page，不反向依赖上层；`openUndoPage` 校验信封 `PageType.UNDO`。
9. 错误模型区分 validation（入参/类型/`undoNo==NONE`）/overflow（页满）/format（损坏/页类型不符/越界）；新增异常归 `storage.undo`，保留 message+cause。
10. 并发沿用单页 latch 模型，无新锁、无可阻塞等待、无 `synchronized`；不在持 undo 页 latch 后发起阻塞等待。
11. 测试覆盖值对象、codec 往返（含 null key 列与 `undoNo==NONE` 拒绝）、页追加/溢出/越界、双 `newPage` redo 顺序、`openUndoPage` 页类型守门、onPool 端到端 append→RollPointer→read、持久化重读、`PageTypeTest` 回归。
12. 简化点（单页、单 log、单 space、仅 INSERT、header 合并、不存 inserted hidden columns、`undoNo>0` 约束）逐条标注后续片归属，无 TODO 占位。
