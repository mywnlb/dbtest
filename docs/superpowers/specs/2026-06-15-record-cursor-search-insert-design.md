# Spec：记录层 R4（cursor + 比较/查找 + key 有序 insert）— innodb-record-design.md §5.1/§6/§7/§10.1/§11/§14

- 日期：2026-06-15
- 关联设计：`docs/design/innodb-record-design.md`（§5.1 RecordRef、§6 物理↔逻辑、§7 页内查找、§10.1 insert、§11 比较/排序/索引 key、§14.1 RecordPageAccessor、§14.2 RecordCursor、§15 异常、§16 测试、§17 step5-6）
- 上游依赖：record.page（R3，RecordPage/RecordPageDirectory/IndexPageHeader）、record.format（RecordEncoder/RecordDecoder/RecordHeader）、record.type（TypeCodec/TypeCodecRegistry/FieldSlice/FieldWriter/BinaryCollation）、record.schema、storage.buf（PageGuard）、domain。
- 前置：R1+R2+R3 全绿。
- 状态：沿用既定 R3/R4 拆分，R4 = 「记录落在页上可读可有序写」的闭环：读游标 + key 比较/查找 + key 有序 insert + PageDirectory 维护。

## 1. 背景与范围

R4 落地 §17 step5-6：让页内记录可**字段级读取**、可按 **key 比较/二分查找**、可按 **key 有序插入**并维护 PageDirectory（slot + n_owned）。这是 R3 纯结构之上的「逻辑序」层，B+Tree 叶子页 CRUD 的读路径与插入路径自此可用。

**做**：
- `RecordFieldResolver`（record.format）：物理记录字节按 schema 解析各列 NULL/FieldSlice/ColumnValue（抽出 RecordDecoder 的 offset 逻辑，DRY 复用）。
- `RecordRef`（§5.1）、`RecordCursor`（§14.2 读子集）：绑定 PageGuard 的字段级读访问。
- `SearchKey` + `RecordComparator`（§11）：记录 vs key 的保序比较（按 encoded slice，复用 codec.compare），ASC/DESC、NULL 序、infimum/supremum 边界。
- 页内查找（§7）：PageDirectory 二分定位 group + 组内 next_record 线扫 → `findEqual` / `findInsertPosition`。
- key 有序 insert（§10.1）：encode → allocate → wire next_record → 维护 n_recs/last_insert/direction + PageDirectory（owner n_owned++，组超限 split 加 slot）。

**不做**（注释标注，后续片）：
- update / delete-mark / purge / GarbageList 复用 / page reorganize → R5+。
- 隐藏列（DB_TRX_ID/DB_ROLL_PTR/DB_ROW_ID）+ MVCC、redo/undo payload → 后续（trx/redo 暂停）。
- 跨页导航 / split / merge / root（B+Tree 职责）：insert 页满只抛 `RecordPageOverflowException`，不分配新页。
- prefix index 比较（KeyPartDef.prefixBytes>0）：本片按整列比较，prefixBytes 暂忽略（简化点，注释标注）。
- 当前读行锁等待 / §13.3 重新定位协议（事务层路径）。

## 2. 包与依赖

- `record.format` 新增 `RecordFieldResolver`；`RecordDecoder` 重构为委托它（DRY，单一布局真相）。
- `record.page` 新增 `RecordRef`、`RecordCursor`、`SearchKey`、`RecordComparator`、`RecordPageSearch`（查找）、`RecordPageInserter`（insert）、`RecordNotFoundException`。
- 无环：`btree → record.page → record.format → record.type → record.schema`；record.page → buf/domain 为允许下层依赖。

## 3. 关键决策（设计文档留白处的落地选择，写进代码注释）

1. **比较走 encoded slice**（§11「优先比较 encoded key slice」）：记录侧用 `RecordFieldResolver` 取列的 FieldSlice（页字节子数组上，保序编码）；key 侧用列 codec 把 `ColumnValue` 编码进临时 buffer 得 FieldSlice；`codec.compare(recordSlice, keySlice)` 即类型自然序。不另造 ColumnValue 级比较器。
2. **NULL 序**（§11）：ASC 中 NULL < 非 NULL；DESC 反转。两侧皆 NULL 视为相等。比较在调用 codec.compare 前先判 NULL。
3. **infimum/supremum 边界**：比较器识别 `RecordType.INFIMUM`→恒小于任何 key、`SUPREMUM`→恒大于；用于二分时 slot 记录的哨兵语义。
4. **SearchKey** = `List<ColumnValue>`（前 k 个 key part 的值，k ≤ parts.size）；比较按提供的前缀逐 part 进行，key 用尽即返回 0（前缀匹配）。findEqual 传完整 key。
5. **PageDirectory ownership**（§7）：slot 指向 group 末记录（最大 key），`n_owned` 只记在 group 末记录头。常量 `MIN_N_OWNED=4`、`MAX_N_OWNED=8`（对齐 InnoDB PAGE_DIR_SLOT_MIN/MAX）。insert 后 owner.n_owned++；超 MAX 则 split：在组内第 4 条记录处插一新 slot，原 owner 与新 slot 各分约半数。infimum 槽恒 owns 1（自身）。
6. **insert 重复 key**：插入到「最后一条 key ≤ newKey 的记录」之后（稳定，允许重复；唯一性由上层/索引保证，本片不判重）。
7. **RecordRef** 仅作短期定位值（§5.1）：page reorganize / split 后须重新校验；本片不长期持有。

## 4. R4a：RecordFieldResolver + RecordRef + RecordCursor

### 4.1 RecordFieldResolver（record.format）
- `RecordFieldResolver(TypeCodecRegistry registry)`。
- `Resolved resolve(byte[] recordBytes, TableSchema schema)`：读 RecordHeader（校验 recordLength==bytes.length，复用现逻辑）；按 NULL bitmap + 变长目录 + 定长宽度算各列：`boolean[] isNull`、`FieldSlice[] slices`（NULL 列槽为 null）。布局同 RecordDecoder：`[header][nullbitmap][vardir][fixed][var]`。
  - `Resolved` 提供 `boolean isNull(int ordinal)`、`FieldSlice slice(int ordinal)`（NULL 抛 `RecordFormatException`）、`ColumnValue value(int ordinal)`（NULL→NullValue，否则 codec.decode）、`LogicalRecord materialize(schema, header)`（全列解码，= 现 RecordDecoder 输出）。
- **RecordDecoder 重构**：`decode(bytes, schema)` 改为 `resolver.resolve(bytes, schema).materialize(...)`，保持现有测试全绿（行为不变）。改前跑 GitNexus impact（decode 调用方仅测试 + 将来 cursor）。

### 4.2 RecordRef（record.page，§5.1）
不可变 record：`PageId pageId, int heapNo, int pageOffset, long schemaVersion, long indexId`。构造校验非负/非空。Javadoc 标注 pageOffset 物理、reorganize/split 后须重新校验。

### 4.3 RecordCursor（record.page，§14.2 读子集）
- `RecordCursor(RecordPage page, int recordOffset, TableSchema schema, TypeCodecRegistry registry)`：构造时**只读 RecordHeader**（recordLength/heapNo/type/deleted）并把 `[offset, offset+recordLength)` 读入本地 byte[]（一次拷贝）；**resolver 延迟构造**——首次字段访问（isNull/columnSlice/readColumn/materialize）才 resolve。
  - **为何延迟（再检查补）**：infimum/supremum 系统记录是「8 头 + 8 标签」、不是 schema 编码记录，对其立即 resolve 会把标签字节误当 NULL bitmap/字段解析。比较器对哨兵记录走 recordType 分支提前返回、不触字段访问，故延迟 resolve 让「对系统记录建 cursor」安全。
  - 不拥有 latch/fix（§13.1），调用方保证 PageGuard 有效。
- `RecordHeader recordHeader()`、`int heapNo()`、`boolean isDeleted()`、`RecordType recordType()`。
- `boolean isNull(ColumnId)`、`FieldSlice columnSlice(ColumnId)`、`ColumnValue readColumn(ColumnId)`、`ColumnValue readKeyPart(KeyPartDef)`（= readColumn(part.columnId())）。
- `LogicalRecord materialize()`（全列）、`RecordRef recordRef(PageId, long indexId)`。
- 简化：无 `release()`（cursor 不持锁，GC 即可；保留方法名为空操作以贴合 §14.2 可选）。

## 5. R4b：SearchKey + RecordComparator + 页内查找

### 5.1 SearchKey（record.page）
不可变：`List<ColumnValue> values`（≤ keyDef.parts；防御副本）。`size()`、`value(i)`。

### 5.2 RecordComparator（record.page）
- `RecordComparator(TypeCodecRegistry registry)`。
- `int compare(RecordCursor record, SearchKey key, IndexKeyDef keyDef, TableSchema schema)`：返回 record 相对 key 的序（<0 record 在前）。
  - record 是 INFIMUM → 返回 -1；SUPREMUM → +1（哨兵，不看 key）。
  - 否则逐 part i（0..min(key.size, parts.size)-1）：
    - `KeyPartDef part = parts.get(i)`；`ColumnType ct = schema.column(part.columnId().value()).type()`。
    - `recordNull = record.isNull(part.columnId())`；`keyNull = key.value(i) instanceof NullValue`。
    - 两 null → 续；一方 null → ASC: null 较小（record null→-1，key null→+1），DESC 取反；返回。
    - 皆非 null：`recordSlice = record.columnSlice(part.columnId())`；`keySlice = encodeKey(key.value(i), ct)`；`c = codec.compare(recordSlice, keySlice, ct)`；DESC 取反；`c!=0` 返回 c。
  - 全部相等（key 前缀匹配）→ 返回 0。
- `encodeKey(ColumnValue, ColumnType)`：`registry.validate`；`byte[] buf = new byte[codec.encodedLength(v,ct)]`；`codec.encode(v,ct,new FieldWriter(buf,0))`；返回 `new FieldSlice(buf,0,buf.length)`。

### 5.3 RecordPageSearch（record.page，§7）
- `RecordPageSearch(TypeCodecRegistry registry)`，方法接 `(RecordPage page, PageId, SearchKey, IndexKeyDef, TableSchema)`。
- **目录访问（再检查补）**：search/inserter 需 RecordPageDirectory，但只有 RecordPage 句柄。RecordPage 新增 `RecordPageDirectory directory()`（返回绑定同 guard/pageSize 的目录视图）作桥；search 通过 `page.directory()` 取 slot/slotCount。
- 二分定位起始 group：slot 数 n（`page.directory().slotCount()`）；`low=0, high=n-1`；`while (high-low>1){ mid=(low+high)/2; 对 slot(mid) 记录建 cursor; if cmp(slotRec, key) < 0 → low=mid else high=mid }`。
  - **哨兵安全（再检查补）**：循环不变式 high-low>1 ⇒ `mid` 严格落在 (low,high) 内，且初始 low=0、high=n-1，故 **mid 恒为真实用户记录、永不命中 slot(0)=infimum 或 slot(n-1)=supremum**——二分不会对系统记录做字段比较。comparator 的哨兵分支只为防御 + 配合下面扫描的边界判断。
  - 收敛：infimum 恒 cmp<0、supremum 恒 cmp≥0（哨兵），保证 low/high 夹逼到 high=low+1。扫描起点 = slot(low) 记录。
- `OptionalInt findEqual(...)`：`cur = page.nextRecord(slot(low))`（slot(low) 记录 key<newKey 或为 infimum，故从其 next 开始比较）；`while (cur != supremum){ c=cmp(cursor(cur),key); if c==0 return cur; if c>0 return empty; cur=nextRecord(cur);} return empty;`。`cur!=supremum` 守卫保证**不对 supremum 建字段比较**。
- `int findInsertPosition(...)`：返回新记录前驱 offset（key ≤ newKey 的最后一条，至少为 infimum）。`prev = slot(low)`；`while (next(prev)!=supremum && cmp(cursor(next(prev)), key) <= 0) prev = next(prev)`；返回 prev。`next(prev)!=supremum` 守卫同样避免对 supremum 建字段比较。
- `findEqualCursor(...)`：findEqual 命中则返回 `RecordCursor`，否则抛 `RecordNotFoundException`。（同时保留 `findEqual` 的 `OptionalInt` 版供上层自决。）
- 注：ownership 维护**不**用二分的 high（见 §6.2/§6.3 改正——用链上「走到首个 n_owned>0」定位 owner，避免重复 key 在槽边界时二分槽与实际链位置不一致）。

## 6. R4c：key 有序 insert + PageDirectory 维护

### 6.1 RecordPage 新增原语（汇总 R4 各批对 RecordPage 的补充）
- R4a：`void writeRecordBytes(int offset, byte[] bytes)`（X，= guard.writeBytes）；`byte[] readRecordBytes(int offset)`（读 header 取 recordLength → guard.readBytes(offset, recordLength)）；`void setHeapNo(int offset, int heapNo)`（X，写 offset+HEAP_NO(1) 处 u16）。
- R4b：`RecordPageDirectory directory()`（返回绑定同 guard/pageSize 的目录视图，供 search/inserter 取 slot）。
- R4c：`void setNOwned(int offset, int nOwned)`（X，写 offset+N_OWNED(3) 处 1 字节；0..255，越界 DatabaseValidationException）。
- 常量补 `IndexPageLayout.REC_HEAPNO_FIELD_OFFSET=1`、`REC_NOWNED_FIELD_OFFSET=3`（镜像 RecordHeaderLayout 的 HEAP_NO/N_OWNED）。

### 6.2 RecordPageInserter（record.page，§10.1）
- `RecordPageInserter(TypeCodecRegistry registry)`；`RecordEncoder encoder`、`RecordPageSearch search`、`RecordComparator` 内部组合。
- `RecordRef insert(RecordPage page, PageId, LogicalRecord rec, IndexKeyDef keyDef, TableSchema schema)`（要求 X）：
  1. `byte[] bytes = encoder.encode(rec, schema)`（含校验、schemaVersion、长度）。
  2. `SearchKey key = keyOf(rec, keyDef)`（取 rec 的 key part 列值）。
  3. `int prev = search.findInsertPosition(page, pageId, key, keyDef, schema)`。
  4. `int heapNo = page.nextHeapNo(); int off = page.allocateFromFreeSpace(bytes.length)`（页满抛 overflow）。
  5. 写记录字节：`guard.writeBytes(off, bytes)`（经 page 暴露 `writeRecordBytes(off, bytes)`）；`page.setHeapNo(off, heapNo)`；`page.setNextRecord(off, page.nextRecord(prev))`；`page.setNextRecord(prev, off)`（链入）。
  6. 维护 header：`nRecs++`、`lastInsert=off`、方向（off>prev 的 key 顺序近似——简化：设 RIGHT，nDirection 连续 +1；非严格）。
  7. PageDirectory owner 维护（**按链定位 owner**）：从新记录起沿 next 走到首个 `n_owned>0` 的记录 = owner（组末记录）：`ownerOff = off; while (recordHeaderAt(ownerOff).nOwned()==0) ownerOff = nextRecord(ownerOff);`（supremum 恒 n_owned≥1，循环必终止；owner 永不为 infimum，因前向走不回头）。`newOwned = nOwned(ownerOff)+1; setNOwned(ownerOff, newOwned);` 若 `newOwned > MAX_N_OWNED(8)` → §6.3 split。
  8. 返回 `new RecordRef(pageId, heapNo, off, schema.schemaVersion(), keyDef.indexId())`。
- `keyOf(LogicalRecord, IndexKeyDef)`：按 parts 取 `rec.columnValues().get(part.columnId().value())` 组 SearchKey。

### 6.3 owner slot 与组维护细节
- owner = §6.2 step7 按链定位到的组末记录 `ownerOff`（n_owned>0）。n_owned 读写经 `page.recordHeaderAt(off).nOwned()` / `page.setNOwned(off, n)`。
- group 语义：owner 所在组 = `(前一槽记录, owner]`（左开右闭，含组末 owner）。n_owned(owner) = 组内记录数（含 owner，对 supremum 含其自身）。
- **split（再检查改正 off-by-one + 按链定位）**：owner 增到 `newOwned (=oldOwned+1) > MAX(8)` 时切分，使新组各占约半：
  - 定位 owner 的槽下标 `h`：线扫目录找 `dir.slot(h)==ownerOff`（owner 必是某槽记录，h≥1，因 owner 非 infimum）。`base = dir.slot(h-1)` 记录（组前一槽末记录；h-1=0 时 base=infimum）。
  - **从 base 起沿 next 走 `MIN_N_OWNED(4)` 步**得 `mid`（= base 之后第 4 条记录，即新组末记录；新组 = base 之后前 4 条，恰 4 条）。注意是从 base 走 4 步，**不是**从 base.next 走 4 步（后者会得第 5 条、新组 5 条）。
  - `dir.insertSlot(h, mid)`（mid.key < owner.key，故新槽放 index h、原 owner 顺移到 h+1）；`page.setNOwned(mid, MIN_N_OWNED)`；`page.setNOwned(ownerOff, newOwned - MIN_N_OWNED)`。例：newOwned=9 → mid 组 4、owner 组 5，二者 ∈[4..8]。
  - 用**记录 offset**（base/mid/ownerOff）持有，不用 slot index——insertSlot 后 index 漂移，offset 稳定。
- **split 尽力而为（再检查补）**：若 `dir.insertSlot` 因页尾无空间抛 `RecordPageOverflowException`，**吞掉并跳过 split**（记录已成功链入，组暂超 MAX 不影响查找正确性，只是该组线扫变长，待将来 reorganize 整理）。这样避免「记录已插入但 split 半途失败」的部分失败；insert 整体仍成功返回 RecordRef。注意：记录本身的 `allocateFromFreeSpace`（§6.2 step4）若 overflow 则在任何修改前抛出，是干净的整体失败路径，与此处不同。

## 7. 异常
- `RecordNotFoundException`（record.page，extends DatabaseRuntimeException）：findEqualCursor 未命中。
- 复用：`RecordPageOverflowException`（页满）、`PageDirectoryCorruptedException`（链/槽损坏）、`RecordFormatException`（解析）、`RecordTooLargeException`（编码超限）。

## 8. 并发与 latch（§13.1/§13.4）
- 读路径（cursor/search/compare）至少 S；写路径（insert + 头字段 setter + insertSlot）要求 X（PageGuard 自校验）。
- cursor 把记录字节一次性拷入本地数组，不在 PageGuard 生命周期外缓存对页的引用（§13.1）。
- insert 全程单页 X latch 内完成（本片不跨页、不写 undo、不等行锁；§13.4 的 undo/锁步骤归后续）。
- 不使用 synchronized；无共享可变状态（comparator/search/inserter 无状态，registry 只读）。

## 9. 测试（§16 相关项）
经真实 PageGuard（16KB 页），TypeCodecRegistry：
- `RecordFieldResolverTest`：定长/变长/NULL 列解析 slice、value、materialize 与 RecordDecoder 一致；RecordDecoder 回归仍绿。
- `RecordCursorTest`：手工放置 encoded 记录（encode→allocate→writeRecordBytes→setHeapNo），cursor readColumn/columnSlice/isNull/isDeleted/materialize 往返；heapNo 正确。
- `RecordComparatorTest`：单/复合 key；ASC/DESC；NULL 序；infimum/supremum 哨兵；前缀 key；各类型（int/varchar/decimal/date）序正确。
- `RecordPageSearchTest`：手工建有序页（多记录 + 目录），findEqual 命中/未命中/边界（最小/最大/中间）；findInsertPosition 在头/中/尾/重复 key 处正确。
- `RecordPageInserterTest`：
  - 顺序 insert 1..N → recordOffsetsInOrder 为 key 升序；nRecs==N。
  - 随机序 insert → recordOffsetsInOrder 仍 key 升序（property-style，固定种子）。
  - 触发 group split：插 > 8 条 → slotCount 增长；slot(0)(infimum) n_owned==1、中间 slot n_owned ∈ [MIN..MAX]、slot(n-1)(supremum) n_owned ∈ [1..MAX]；findEqual 仍能找到全部。
  - 页满 → RecordPageOverflowException。
  - 含 NULL key、含变长列的 insert 与查回。
- 复合/边界回归：infimum.next 与 supremum 链尾在 insert 后保持一致。

## 10. 批次拆分（writing-plans 细化）
- **R4a**：RecordFieldResolver（+ RecordDecoder 重构）、RecordRef、RecordCursor。测试：Resolver/Cursor + Decoder 回归。
- **R4b**：SearchKey、RecordComparator、RecordPageSearch、RecordNotFoundException。测试：Comparator/Search。
- **R4c**：RecordPage 头字段 setter、RecordPageInserter + 目录 ownership/split。测试：Inserter（顺序/随机/split/overflow/NULL/变长）。

每批 TDD → 全量 `clean test` → 收口刷 `npx gitnexus analyze`。R5（update/delete-mark/purge/reorganize）单独确认后再开。
