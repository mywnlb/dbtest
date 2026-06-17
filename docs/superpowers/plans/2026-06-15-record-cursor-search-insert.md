# Record 层 R4（cursor + 查找 + key 有序 insert）Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:executing-plans（仓库惯例：inline 自治执行、批末跑全量 `clean test`）。**项目规则：不 commit**——「Commit」步 → 全量回归 checkpoint。

**Goal:** 让页内记录可字段级读取、按 key 比较/二分查找、按 key 有序插入并维护 PageDirectory。

**Architecture:** 建在 R3 record.page 之上：RecordCursor 读、RecordComparator+RecordPageSearch 查、RecordPageInserter 写；比较走保序 encoded slice（复用 codec.compare）；RecordFieldResolver 抽出 RecordDecoder 布局逻辑供单列访问（DRY）。单页 X latch 内完成 insert，不跨页/不 undo/不锁等待。

**Tech Stack:** Java 25、JUnit Jupiter；固定 JDK `C:\Program Files\Java\jdk-25.0.2` + 固定 Gradle `D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat`。

**测试命令:** `$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" clean test --console=plain`

**PageGuard 测试 harness:** 同 R3（FileChannelPageStore + LruBufferPool + @TempDir，`PS=PageSize.ofBytes(16*1024)`、`SPACE=SpaceId.of(1)`、page 3、EXCLUSIVE）。

**放置 encoded 记录的测试辅助（R4a/b 手工建页）:**
```java
RecordPage rp = new RecordPage(g, PS); rp.format(indexId, 0);
byte[] bytes = new RecordEncoder(reg).encode(logical, schema);
int heapNo = rp.nextHeapNo();
int off = rp.allocateFromFreeSpace(bytes.length);
rp.writeRecordBytes(off, bytes);   // R4c 新增；R4a 期间可用 g.writeBytes(off, bytes)
rp.setHeapNo(off, heapNo);         // R4c 新增
// 串接：rp.setNextRecord(prev, off); rp.setNextRecord(off, next);
```

---

## Batch R4a：RecordFieldResolver + RecordRef + RecordCursor

### Task A1：RecordFieldResolver（record.format）+ RecordDecoder 重构

**Files:**
- Create: `src/main/java/cn/zhangyis/db/storage/record/format/RecordFieldResolver.java`
- Modify: `src/main/java/cn/zhangyis/db/storage/record/format/RecordDecoder.java`
- Test: `src/test/java/cn/zhangyis/db/storage/record/format/RecordFieldResolverTest.java`

- [ ] **Step 1 GitNexus impact**：`gitnexus_impact({target:"RecordDecoder.decode", direction:"upstream"})`；报告 blast radius（预期仅测试 + 未来 cursor，低风险）。
- [ ] **Step 2 写失败测试** `RecordFieldResolverTest`：构造混合 schema（int 非空、varchar 可空、decimal）；`RecordEncoder.encode` 得 bytes；`Resolved r = new RecordFieldResolver(reg).resolve(bytes, schema)`：断言 `r.isNull(ordinal)`、`r.value(ordinal)` 与原值一致（BinaryValue 比字节、Decimal 比 compareTo）、`r.slice(ordinal)` 非空列长度正确、NULL 列 `slice` 抛 `RecordFormatException`、`r.materialize()` 等于 `new RecordDecoder(reg).decode(bytes, schema)`。
- [ ] **Step 3 跑确认失败**。
- [ ] **Step 4 实现 `RecordFieldResolver`**：`resolve(byte[],TableSchema)`：读 RecordHeader（`RecordHeader.readFrom`），校验 `recordLength==bytes.length` 否则 `RecordFormatException`；按 `[header][nullbitmap][vardir][fixed][var]` 复算（移植 RecordDecoder 现逻辑）得 `boolean[] isNull` + `FieldSlice[] slices`（NULL 槽 null）；返回内部 `Resolved`（持 bytes/schema/header/isNull/slices/registry）暴露 `isNull/slice/value/materialize`。`value`：NULL→`ColumnValue.NullValue.INSTANCE`，否则 `codec.decode(slice, ct)`。`materialize`：逐列 value 组 `LogicalRecord(schema.schemaVersion(), values, header.deletedFlag(), header.recordType())`。
- [ ] **Step 5 重构 RecordDecoder.decode**：改为 `return new RecordFieldResolver(registry).resolve(buf, schema).materialize();`（保留 null 校验与既有签名）。
- [ ] **Step 6 跑确认通过**（含 RecordCodecTest/RecordFieldResolverTest 全绿，回归不破）。

### Task A2：RecordRef（record.page）

**Files:**
- Create: `src/main/java/cn/zhangyis/db/storage/record/page/RecordRef.java`
- Test: `src/test/java/cn/zhangyis/db/storage/record/page/RecordRefTest.java`

- [ ] **Step 1 写失败测试**：构造合法 RecordRef 取值；`pageId` null、heapNo<0、pageOffset<0、indexId<0 → `DatabaseValidationException`。
- [ ] **Step 2 跑确认失败**。
- [ ] **Step 3 实现** record `RecordRef(PageId pageId, int heapNo, int pageOffset, long schemaVersion, long indexId)` + 构造校验 + Javadoc（pageOffset 物理、reorganize/split 后须重校验）。
- [ ] **Step 4 跑确认通过**。

### Task A3：RecordCursor（record.page）

**Files:**
- Create: `src/main/java/cn/zhangyis/db/storage/record/page/RecordCursor.java`
- Modify: `RecordPage.java`（加 `byte[] readRecordBytes(int offset)` 便于 cursor 读整条；及 `writeRecordBytes`/`setHeapNo` 留到 R4c，可在 A3 提前加 `setHeapNo` 与 `writeRecordBytes` 以支撑测试）
- Test: `src/test/java/cn/zhangyis/db/storage/record/page/RecordCursorTest.java`

- [ ] **Step 1 写失败测试** `RecordCursorTest`：format 页；encode 一条记录 bytes；`int off=allocate; rp.writeRecordBytes(off,bytes); rp.setHeapNo(off, heapNo)`；`RecordCursor c = new RecordCursor(rp, off, schema, reg)`：`c.heapNo()`==heapNo、`c.recordType()`==CONVENTIONAL、`c.isDeleted()`==false、`c.readColumn(id)` 各列往返、`c.columnSlice(id)` 长度正确、可空列 NULL 时 `c.isNull(id)`、`c.materialize()` 等于原 logical（值级断言）。
- [ ] **Step 2 跑确认失败**。
- [ ] **Step 3 实现**：
  - `RecordPage.writeRecordBytes(int off, byte[] bytes)`（X）= `guard.writeBytes(off, bytes)`；`setHeapNo(int off,int heapNo)`（X）= `PageU16.put(guard, off+IndexPageLayout.REC_HEAPNO_FIELD_OFFSET, heapNo)`；`readRecordBytes(int off)`：读 header 取 recordLength，`return guard.readBytes(off, recordLength)`；`RecordPageDirectory directory()`：`return new RecordPageDirectory(guard, pageSize)`（供 search/inserter 取 slot）。补常量 `IndexPageLayout.REC_HEAPNO_FIELD_OFFSET=1`、`REC_NOWNED_FIELD_OFFSET=3`。
  - `RecordCursor(RecordPage,int offset,TableSchema,TypeCodecRegistry)`：`this.bytes=page.readRecordBytes(offset)`；`this.header=RecordHeader.readFrom(bytes,0)`；**resolved 延迟**（`private Resolved resolved; private Resolved resolved(){ if(resolved==null) resolved=new RecordFieldResolver(reg).resolve(bytes,schema); return resolved; }`）——对 infimum/supremum 建 cursor 只读 header 不解析字段。方法：`recordHeader()`/`heapNo()`/`isDeleted()`/`recordType()` 用 header；`isNull(ColumnId)`=resolved().isNull(id.value())、`columnSlice`/`readColumn`/`readKeyPart`/`materialize()` 经 resolved()；`recordRef(PageId,long indexId)`；`release()`（空操作 + 注释）。
- [ ] **Step 4 跑确认通过**。

### Checkpoint A（替代 commit）
- [ ] 全量 `clean test`，BUILD SUCCESSFUL；不 commit。

---

## Batch R4b：SearchKey + RecordComparator + 页内查找

### Task B1：SearchKey + RecordComparator

**Files:**
- Create: `src/main/java/cn/zhangyis/db/storage/record/page/SearchKey.java`
- Create: `src/main/java/cn/zhangyis/db/storage/record/page/RecordComparator.java`
- Test: `src/test/java/cn/zhangyis/db/storage/record/page/RecordComparatorTest.java`

- [ ] **Step 1 写失败测试** `RecordComparatorTest`：单 key（int ASC）记录 vs key：小/等/大返回 <0/0/>0；DESC 反转；复合 key（int,varchar）次序；NULL 序（ASC record-null < key-nonnull）；前缀 key（key 只给第一 part）；infimum/supremum 哨兵（手工放 infimum cursor → 恒 <0）。
- [ ] **Step 2 跑确认失败**。
- [ ] **Step 3 实现**：
  - `SearchKey`：record `List<ColumnValue> values`（List.copyOf，可空但 ≤ parts）；`size()`、`value(i)`。
  - `RecordComparator(TypeCodecRegistry registry)`：`compare(RecordCursor,SearchKey,IndexKeyDef,TableSchema)` 按 spec §5.2（哨兵→±1；逐 part NULL 序 + codec.compare(encoded slice)，DESC 取反）；`encodeKey(ColumnValue,ColumnType)` 私有（validate→encode 入临时 buf→FieldSlice）。
- [ ] **Step 4 跑确认通过**。

### Task B2：RecordPageSearch + RecordNotFoundException

**Files:**
- Create: `src/main/java/cn/zhangyis/db/storage/record/page/RecordNotFoundException.java`
- Create: `src/main/java/cn/zhangyis/db/storage/record/page/RecordPageSearch.java`
- Test: `src/test/java/cn/zhangyis/db/storage/record/page/RecordPageSearchTest.java`

- [ ] **Step 1 写失败测试** `RecordPageSearchTest`：手工建有序页——format 后顺序放 N 条 int-key 记录（key=10,20,30,...），手工 wire next_record（infimum→...→supremum），并按需 setSlot/insertSlot 让目录覆盖（或仅 2 槽，依赖组内线扫）；断言：`findEqual(key=20)` 命中其 offset；`findEqual(15)` empty；`findEqual(5)`(最小前) empty；`findEqual(最大值)` 命中；`findInsertPosition(15)` 返回 key=10 记录 offset；`findInsertPosition(5)` 返回 infimum；`findInsertPosition(35)`(尾) 返回 key=30；重复 key（放两条 20）`findInsertPosition(20)` 返回后一条 20 的 offset。
- [ ] **Step 2 跑确认失败**。
- [ ] **Step 3 实现**：
  - `RecordNotFoundException extends DatabaseRuntimeException`（message+cause）。
  - `RecordNotFoundException` 与 `OptionalInt`：import `java.util.OptionalInt`。
  - `RecordPageSearch(TypeCodecRegistry registry)`（内含 RecordComparator）。`RecordPageDirectory dir = page.directory()`。私有 `int startGroupSlotLow(page,pageId,key,keyDef,schema)`：`n=dir.slotCount()`；二分 `low=0,high=n-1`，`while(high-low>1){ mid=(low+high)>>>1; cur=cursorAt(page,dir.slot(mid),schema); if(cmp(cur,key)<0) low=mid; else high=mid; }`，返回 low。mid∈(low,high) 故不触 infimum/supremum 槽。`cursorAt(page,off,schema)`=`new RecordCursor(page,off,schema,reg)`。
  - `OptionalInt findEqual(...)`：`sup=page.supremumOffset(); cur=page.nextRecord(dir.slot(startLow))`；`while(cur!=sup){ c=cmp(cursorAt(cur),key); if(c==0) return OptionalInt.of(cur); if(c>0) return OptionalInt.empty(); cur=page.nextRecord(cur);} return empty;`（`cur!=sup` 守卫，不对 supremum 比字段）。
  - `int findInsertPosition(...)`：`prev=dir.slot(startLow)`；`while(page.nextRecord(prev)!=sup && cmp(cursorAt(page.nextRecord(prev)),key)<=0) prev=page.nextRecord(prev);`；返回 prev。
  - `findEqualCursor(...)`：findEqual 命中 → `new RecordCursor(page,off,schema,reg)`；否则抛 `RecordNotFoundException`。
- [ ] **Step 4 跑确认通过**。

### Checkpoint B（替代 commit）
- [ ] 全量 `clean test`，BUILD SUCCESSFUL；不 commit。

---

## Batch R4c：key 有序 insert + PageDirectory ownership/split

### Task C1：RecordPage.setNOwned + RecordPageInserter

**Files:**
- Modify: `RecordPage.java`（加 `setNOwned(int off,int n)`）
- Create: `src/main/java/cn/zhangyis/db/storage/record/page/RecordPageInserter.java`
- Test: `src/test/java/cn/zhangyis/db/storage/record/page/RecordPageInserterTest.java`

- [ ] **Step 1 写失败测试** `RecordPageInserterTest`：
  - **顺序**：insert key=1..6（int 单 key，含一 varchar 非 key 列），`rp.recordOffsetsInOrder()` 解出的 key 升序==[1..6]；`header().nRecs()`==6；每条 `findEqual` 命中。
  - **随机**：固定种子打乱 1..20 insert → recordOffsetsInOrder 的 key==[1..20]；findEqual 全部命中。
  - **split**：insert 1..12（>8）→ `directory.slotCount()` > 2；`slot(0)` 记录(infimum) n_owned==1；中间各 slot(1..n-2) 记录 n_owned ∈ [MIN..MAX]=[4..8]；`slot(n-1)` 记录(supremum) n_owned ∈ [1..8]；findEqual(每个 key 1..12) 命中。
  - **overflow**：用大 varchar 反复 insert 直到 `RecordPageOverflowException`（断言抛出且此前插入仍可 findEqual）。
  - **NULL key**：可空 int key，插 NULL 与非 NULL，NULL 排最前（ASC）。
- [ ] **Step 2 跑确认失败**。
- [ ] **Step 3 实现**：
  - `RecordPage.setNOwned(int off,int n)`（X）：`n` 0..255 否则 `DatabaseValidationException`；`guard.writeBytes(off+IndexPageLayout.REC_NOWNED_FIELD_OFFSET, new byte[]{(byte)n})`。
  - `RecordPageInserter(TypeCodecRegistry registry)`：组合 `RecordEncoder`、`RecordPageSearch`、`RecordComparator`、常量 `MIN_N_OWNED=4`、`MAX_N_OWNED=8`。
  - `insert(RecordPage,PageId,LogicalRecord,IndexKeyDef,TableSchema)`（X）按 spec §6.2：
    1. `bytes=encoder.encode(rec,schema)`；`key=keyOf(rec,keyDef)`；`prev=search.findInsertPosition(...)`。
    2. `heapNo=page.nextHeapNo(); off=page.allocateFromFreeSpace(bytes.length)`（满抛 overflow，干净整体失败）。
    3. `page.writeRecordBytes(off,bytes); page.setHeapNo(off,heapNo); page.setNextRecord(off, page.nextRecord(prev)); page.setNextRecord(prev, off);`。
    4. header：`IndexPageHeader h=page.header(); 重写 nRecs+1、lastInsert=off`（构造新 IndexPageHeader.writeTo；direction 简化设 RIGHT、nDirection+1）。
    5. **owner 按链定位**：`ownerOff=off; while(page.recordHeaderAt(ownerOff).nOwned()==0) ownerOff=page.nextRecord(ownerOff);`（终止于首个 n_owned>0，即 supremum 或某 mid；owner 非 infimum）。`newOwned=page.recordHeaderAt(ownerOff).nOwned()+1; page.setNOwned(ownerOff,newOwned);`。
    6. **split（newOwned>MAX_N_OWNED）**：线扫 `dir` 找 `h` 使 `dir.slot(h)==ownerOff`；`base=dir.slot(h-1)`；**从 base 起 `page.nextRecord` 走 `MIN_N_OWNED` 步得 `midOff`**（base 之后第 4 条；新组恰 4 条——不是从 base.next 走 4 步）；`try { dir.insertSlot(h, midOff); page.setNOwned(midOff, MIN_N_OWNED); page.setNOwned(ownerOff, newOwned-MIN_N_OWNED);} catch(RecordPageOverflowException e){ /* 尽力而为：页尾无槽位则跳过 split，组暂超 MAX，查找仍正确，待 reorganize */ }`。
    7. 返回 `new RecordRef(pageId, heapNo, off, schema.schemaVersion(), keyDef.indexId())`。
  - `keyOf(LogicalRecord,IndexKeyDef)`：parts 取 `rec.columnValues().get(part.columnId().value())`。
  - owner/base/mid 全程用 **记录 offset** 持有（insertSlot 后 slot index 漂移，offset 稳定）。
- [ ] **Step 4 跑确认通过**。

### Checkpoint C（替代 commit）
- [ ] 全量 `clean test`，BUILD SUCCESSFUL；不 commit。
- [ ] `npx gitnexus analyze` 刷新索引。
- [ ] `gitnexus_detect_changes()` 确认改动范围符合预期。

---

## Self-Review（spec 覆盖）
- §6 resolver/§5.1 ref/§14.2 cursor → A1/A2/A3。 §11 比较 → B1。 §7 查找 → B2。 §10.1 insert + 目录 → C1。 §15 异常 → RecordNotFound(B2)、复用 overflow/corrupt。 §13 latch → 各写步 X（PageGuard 校验）。 §16 测试 → 各 Task。
- 类型一致性：`RecordCursor(RecordPage,int,TableSchema,TypeCodecRegistry)`、`RecordComparator.compare(RecordCursor,SearchKey,IndexKeyDef,TableSchema)`、`RecordPageSearch.findEqual→OptionalInt`/`findInsertPosition→int`、`RecordPageInserter.insert→RecordRef`、page 新增 `writeRecordBytes/readRecordBytes/setHeapNo/setNOwned` 全程一致。
- 无 placeholder：每步给出关键逻辑/期望。
- DRY：RecordDecoder 委托 RecordFieldResolver；比较复用 codec.compare；search 复用 comparator；insert 复用 search。
