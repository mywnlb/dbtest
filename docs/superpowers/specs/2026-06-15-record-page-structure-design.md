# Spec：记录层 R3（record.page 页内结构）— innodb-record-design.md §5.1/§7/§10.1/§14

- 日期：2026-06-15
- 关联设计：`C:\coding\java\self\dbtest\dbtest\docs\design\innodb-record-design.md`（§5.1 RecordRef、§5.2 PhysicalRecord 头、§7 页内记录结构、§10.1 insert 数据流、§10.4 reorganize、§13.4 页内锁顺序、§15 异常、§16 测试、§17 step4）
- 上游依赖：domain（PageId/PageSize 等值对象）、common.exception、storage.buf（`PageGuard` 受控页访问）、record.format（`RecordHeader`/`RecordType`）。
- **不依赖** storage.fsp 的空间管理（XDES/segment/space header）；不依赖 SQL/btree/trx。
- 前置：R1+R2（schema+type+format）已落地并全绿；§5.3 页信封（`FilePageHeader`/`PageType.INDEX`/`PageChecksum`，位于 storage.fsp）已落地。
- 状态：R3 范围沿用上一阶段已锁定的 R3/R4 拆分（R3=页内结构原语；R4=cursor + key 有序 insert/lookup）。设计文档为权威，本 spec 记录落地决策与简化点。

## 1. 背景与范围

R3 落地 §17 step4：`record.page` —— INDEX 页内记录区的**物理结构与低层导航原语**，建立在 §5.3 页信封之上。R3 让一个 16KB 页能够：初始化为空 INDEX 页（写 INDEX page header + infimum/supremum + 初始 PageDirectory）、从 free space 分配 heap 空间、读写记录的 `next_record` 指针、沿链从 infimum 走到 supremum、读写 PageDirectory 槽。

R3 **不含**任何「key 语义」：不做 key 比较、不做 key 有序插入定位、不做二分查找、不建 `RecordCursor`。这些依赖 `RecordComparator`（key 比较）与游标生命周期，归 R4。R3 只提供结构原语，测试通过「手工按序 wire next_record + 断言结构不变量」来验证链/目录/heap 行为，保持 R3 与 R4 边界清晰。

**做**：
- INDEX page header（PAGE HEADER 教学子集）读写值对象。
- infimum/supremum 系统记录布局 + 空页初始化 `RecordPage.format()`。
- heap 空间分配原语（从 free space 切出记录空间、分配 heapNo、free space 不足抛 overflow）。
- `next_record` 指针读写（绝对页内 u16 偏移）；infimum→…→supremum 链遍历。
- `PageDirectory` 槽数组读写（slot=组末记录偏移）、槽增删、`n_owned` 维护。
- free space / heapTop / 目录起点的几何关系与不变量。

**不做**（注释标注，归后续片）：
- key 有序 insert 定位、PageDirectory 二分查找、`findInsertPosition/findEqual`、`RecordCursor`、字段级读取 → **R4**（依赖 `RecordComparator`）。
- update / delete-mark / purge / GarbageList **复用**算法 / page reorganize → R4+（R3 仅保留 free list head/garbage size 头字段与槽增删原语，不实现复用与重组）。
- 隐藏列（DB_TRX_ID/DB_ROLL_PTR/DB_ROW_ID）+ MVCC、`PAGE_MAX_TRX_ID`、redo/undo payload → 后续（trx/redo 已暂停）。
- 段指针头字段（`PAGE_BTR_SEG_LEAF/TOP`）→ 归 fsp/btree，不在 record.page。
- compact/redundant 行格式区分位（`PAGE_N_HEAP` 最高位）→ 教学简化，只用 compact-风格 forward layout。

## 2. 包与依赖

新增包 `cn.zhangyis.db.storage.record.page`，依赖：`record.format`（RecordHeader/RecordType）、`storage.buf`（PageGuard/PageLatchMode）、`domain`（PageId/PageSize/PageNo/SpaceId）、`common.exception`。无环：`btree → record.page → record.format → record.type → record.schema`；`record.page → buf` 与 `record.page → domain` 为允许的下层依赖。

**不**依赖 fsp：信封（FilePageHeader/PageType）由调用方（btree/fsp 建页时）盖；R3 只拥有页体记录区 `[FIL_PAGE_HEADER_BYTES, pageSize - FIL_PAGE_TRAILER_BYTES)`。

## 3. 页几何与常量（`IndexPageLayout`，包私有）

页体布局（16KB 页，偏移单位字节）：

```
[0,38)            FilePageHeader（§5.3 信封，R3 不碰，调用方盖）
[38, 66)          INDEX page header（PAGE HEADER 教学子集，见 §4）
[66, 82)          INFIMUM 系统记录（8 头 + 8 标签 "infimum\0"）
[82, 98)          SUPREMUM 系统记录（8 头 + 8 标签 "supremum"）
[98, heapTop)     UserRecordHeap（向高地址增长）
[heapTop, dirStart)  FreeSpace（连续可用）
[dirStart, pageSize-8)  PageDirectory（向低地址增长，每槽 2 字节 u16）
[pageSize-8, pageSize)  FilePageTrailer（§5.3 信封，R3 不碰）
```

常量（镜像 §5.3，**教学简化：本地复刻**，因 fsp 的 `FilePageHeaderLayout.SIZE`/trailer 大小为包私有；注释标注「必须与 §5.3 一致」，并由 `IndexPageLayoutTest` 钉死期望值；后续可考虑把信封提到共享 `storage.page` 包消除复刻）：
- `FIL_PAGE_HEADER_BYTES = 38`、`FIL_PAGE_TRAILER_BYTES = 8`。
- `PAGE_HEADER_START = 38`、`PAGE_HEADER_END = 66`（INDEX header 占 28 字节）。
- `INFIMUM_OFFSET = 66`、`SUPREMUM_OFFSET = 82`、`SYS_REC_BYTES = 16`、`USER_RECORDS_START = 98`。
- `DIR_SLOT_BYTES = 2`。
- 系统记录标签：`INFIMUM_LABEL = "infimum\0"`（8 字节 UTF-8）、`SUPREMUM_LABEL = "supremum"`（8 字节）。

### 关键简化决策（与 InnoDB 差异，须在代码注释标注）
1. **next_record 用绝对页内 u16 偏移**（指向下一记录 `RecordHeader` 起始），而非 InnoDB 的相对偏移（指向记录 origin）。绝对偏移避免有符号相对运算，配合 R2 的 forward layout（头在记录起始）。supremum.next = 0 表示链尾。
2. **记录偏移 = RecordHeader 起始**（forward layout），非 InnoDB 的 record origin（body 起点、头在负偏移）。与 R2 `RecordEncoder`（头写在 buf[0]）一致。
3. **PageDirectory 槽序**：slot[0] 紧贴 trailer（最高地址），新增槽向低地址（heap 方向）增长；逻辑上 slot[0]=infimum 组、slot[n-1]=supremum 组，与 InnoDB 一致。slot 值 = 该组**最后一条记录**的页内偏移。
4. heapNo：infimum=0、supremum=1、首条用户记录=2，按 heap 分配顺序递增（= 分配时的旧 `n_heap`）。

## 4. INDEX page header（`IndexPageHeaderLayout` 偏移 + `IndexPageHeader` 值对象）

偏移（基于 `PAGE_HEADER_START=38`，除 indexId 外均 u16，大端）：

| 字段 | 偏移 | 宽 | 语义 |
| --- | --- | --- | --- |
| `N_DIR_SLOTS` | 38 | u16 | PageDirectory 槽数（初始 2） |
| `HEAP_TOP` | 40 | u16 | 首个空闲字节偏移（heap 顶） |
| `N_HEAP` | 42 | u16 | heap 记录总数（含 infimum/supremum；初始 2） |
| `FREE` | 44 | u16 | GarbageList 头记录偏移（0=空；R3 只读写字段，不实现复用） |
| `GARBAGE` | 46 | u16 | 删除记录占用字节总数（R3 只读写字段） |
| `LAST_INSERT` | 48 | u16 | 上次插入记录偏移（0=无） |
| `DIRECTION` | 50 | u16 | 插入方向（`IndexPageDirection` code） |
| `N_DIRECTION` | 52 | u16 | 同方向连续插入计数 |
| `N_RECS` | 54 | u16 | 用户记录数（不含 infimum/supremum；含 delete-marked，对齐 InnoDB PAGE_N_RECS） |
| `LEVEL` | 56 | u16 | B+Tree 层（0=leaf） |
| `INDEX_ID` | 58 | u64 | 索引 id（占 8 字节，结束于 66） |

- `IndexPageDirection` 枚举：`NO_DIRECTION(0)`、`LEFT(1)`、`RIGHT(2)`；`fromCode` 未知值 → `PageDirectoryCorruptedException`（头损坏归类）。
- `IndexPageHeader`（record，11 个字段）：`writeTo(PageGuard)` 要求 X；`readFrom(PageGuard)` S/X 均可。构造校验：各 u16 字段 0..65535、indexId≥0、direction 非空、level/nDirSlots/nHeap 合理（nDirSlots≥2、nHeap≥2）。

## 5. 系统记录与空页初始化（`RecordPage`）

`RecordPage` 是绑定到 `PageGuard` 的页内记录区访问器（不拥有 latch/fix 生命周期，调用方保证 PageGuard 有效；§14 RecordPageAccessor 的结构子集）。

- `static void format(PageGuard guard, long indexId, int level)`（要求 X）：初始化空 INDEX 页记录区：
  - 写 `IndexPageHeader{nDirSlots=2, heapTop=USER_RECORDS_START(98), nHeap=2, free=0, garbage=0, lastInsert=0, direction=NO_DIRECTION, nDirection=0, nRecs=0, level, indexId}`。
  - 写 INFIMUM：`RecordHeader{deleted=false, minRec=false, type=INFIMUM, heapNo=0, nOwned=1, next=SUPREMUM_OFFSET(82), recordLength=16}` + 标签 "infimum\0"。
  - 写 SUPREMUM：`RecordHeader{type=SUPREMUM, heapNo=1, nOwned=1, next=0, recordLength=16}` + 标签 "supremum"。
  - 写 PageDirectory：slot[0]=INFIMUM_OFFSET(66)、slot[1]=SUPREMUM_OFFSET(82)。
  - **不**盖 FilePageHeader（调用方负责 PageType.INDEX）。
- 读：`header()`→IndexPageHeader；`infimumOffset()/supremumOffset()`（常量）；`recordHeaderAt(int offset)`→读 RecordHeader；`systemLabelAt(int offset)`→读 8 字节标签（校验用）。
- 几何：`freeSpace(PageSize)` = `dirStart - heapTop`，`dirStart` = `pageSize - 8 - nDirSlots*2`。

## 6. heap 分配 + next_record 链（`RecordPage` 续）

- `int allocateFromFreeSpace(int recordBytes, PageSize pageSize)`（要求 X）：从 free space 顶部切 `recordBytes`：
  - 校验 `recordBytes>0`；`freeSpace(pageSize) >= recordBytes` 否则抛 `RecordPageOverflowException`（§13.4 RecordPageOverflow 信号，B+Tree 据此决定 split）。
  - 返回新记录偏移 = 旧 `heapTop`；更新 `heapTop += recordBytes`、`nHeap += 1`（新记录 heapNo = 旧 nHeap）。
  - **不**改 next_record / n_recs / directory（key 有序 wire 归 R4 insert；R3 仅给原语）。
  - 提供配套 `int nextHeapNo()`（= 当前 nHeap）便于调用方/测试取将分配的 heapNo。
- `int nextRecord(int offset)`：读该记录头 `nextRecordOffset`。
- `void setNextRecord(int offset, int targetOffset)`（要求 X）：改写该记录头 `nextRecordOffset`（仅改 2 字节字段，配合 markDirty）。
- `List<Integer> recordOffsetsInOrder()`：从 infimum 起沿 next_record 走到 supremum，返回中间用户记录偏移（不含 infimum/supremum）。防御：步数 > nHeap 判为环 → `PageDirectoryCorruptedException`；偏移越出页体 `[USER_RECORDS_START, dirStart)`（supremum 除外）→ `PageDirectoryCorruptedException`。

## 7. PageDirectory（`RecordPageDirectory`）

绑定 `PageGuard` 的目录视图（槽数权威来源 = page header `N_DIR_SLOTS` 字段）：
- `int slotCount()`：读 `N_DIR_SLOTS`。
- `int slot(int i)`：读第 i 槽偏移（`i∈[0,slotCount)`，越界 `PageDirectoryCorruptedException`）；地址 = `dirEnd - (i+1)*2`，`dirEnd = pageSize - 8`。
- `void setSlot(int i, int recordOffset)`（X）：改写第 i 槽。
- `void insertSlot(int at, int recordOffset, PageSize)`（X）：在 at 处插槽，[at, n) 槽整体向低地址移一格，`N_DIR_SLOTS += 1`；插前校验 free space 够放新槽（`dirStart` 下移 2 字节不得撞 `heapTop`）否则 `RecordPageOverflowException`。
- `void removeSlot(int at, PageSize)`（X）：删 at 槽，[at+1, n) 上移一格，`N_DIR_SLOTS -= 1`（不得 < 2）。
- 注：`n_owned` 存在「组末记录」的 RecordHeader 上，由调用方（R4 insert/purge）维护；R3 通过 `RecordPage.recordHeaderAt` 暴露读写能力，目录本身只管槽数组。

构造 `RecordPageDirectory` 与 `RecordPage` 都需要 `PageSize`（计算 dirEnd/dirStart）。`PageSize` 从 domain 取，由调用方传入（PageGuard 不暴露页大小）。

## 8. 异常（record.page 包，继承 DatabaseRuntimeException）

- `RecordPageOverflowException`：页内记录区/目录空间不足（§13.4 信号；caller 据此 split）。
- `PageDirectoryCorruptedException`：目录槽越界、next_record 链成环/越界、direction code 未知、系统记录类型不符（§15）。

（`RecordFormatException` 复用 record.format 既有的。）

## 9. 并发与 latch（§13.1/§13.4）

- 所有写原语（format/allocate/setNextRecord/setSlot/insertSlot/removeSlot）要求 `PageGuard` 为 EXCLUSIVE；PageGuard 在 mode 不符时自身抛 `DatabaseValidationException`，R3 直接复用其校验（不重复造轮子）。
- 读原语（header/nextRecord/recordOffsetsInOrder/slot）S/X 均可。
- `RecordPage`/`RecordPageDirectory` 不拥有 latch/buffer fix，不缓存超出 PageGuard 生命周期的状态（§13.1）；PageGuard close 后再用 → PageGuard 自身抛已关闭异常。
- R3 不进入事务锁等待，不涉及 §13.3 重新定位协议（那是 R4 当前读路径）。

## 10. 测试设计（§16 相关项）

经**真实 PageGuard**（LruBufferPool + FileChannelPageStore + @TempDir，复用 §5.3 测试同款 harness），16KB 页：
- `IndexPageLayoutTest`：常量钉死（38/8/66/82/98、header 偏移连续无重叠、INDEX_ID 结束=66）。
- `IndexPageHeaderTest`：header 经 PageGuard 往返一致；构造校验（u16 越界、nDirSlots<2、direction null）；`IndexPageDirection.fromCode` 往返 + 未知 code 抛异常。
- `RecordPageFormatTest`：`format()` 后 header 各字段、infimum/supremum 头（类型/heapNo/nOwned/next/len）+ 标签、directory 2 槽、heapTop=98、freeSpace=期望值；空页 `recordOffsetsInOrder()` 返回空。
- `RecordPageHeapTest`：连续 `allocateFromFreeSpace` 推进 heapTop、heapNo 递增（2,3,4…）、freeSpace 递减；free space 不足抛 `RecordPageOverflowException`；`allocate` 不动 next_record/directory。
- `RecordPageChainTest`：分配 3 条 raw 记录 + 手工 `setNextRecord` 按序串 infimum→r→r→r→supremum，`recordOffsetsInOrder()` 返回按串入序的偏移；制造环 → `PageDirectoryCorruptedException`；next 指向页外 → `PageDirectoryCorruptedException`。
- `RecordPageDirectoryTest`：`slot/setSlot` 读写；`insertSlot` 在中间插槽后 slotCount+1 且原槽逻辑右移、值正确；`removeSlot` 反向；插槽到无空间 → `RecordPageOverflowException`；removeSlot 不得 <2。
- 异常测试：非 EXCLUSIVE guard 上写 → 复用 PageGuard 的 `DatabaseValidationException`（断言被拒）。

## 11. 批次拆分（writing-plans 细化）

- **R3 batch 1（页 header + 系统记录 + 空页 format）**：`IndexPageLayout`、`IndexPageDirection`、`IndexPageHeaderLayout`、`IndexPageHeader`、page-local u16 helper、`RecordPage`（仅 format + header/系统记录读 + 几何）、`PageDirectoryCorruptedException`。测试：Layout/Header/Format。
- **R3 batch 2（heap + 链 + 目录）**：`RecordPage` 续（allocateFromFreeSpace/nextRecord/setNextRecord/recordOffsetsInOrder）、`RecordPageDirectory`、`RecordPageOverflowException`。测试：Heap/Chain/Directory + 异常。

每批 TDD（先红后绿）→ 全量 `clean test` → 收口刷 `npx gitnexus analyze`。R4（cursor + key 有序 insert/lookup）单独确认后再开。
