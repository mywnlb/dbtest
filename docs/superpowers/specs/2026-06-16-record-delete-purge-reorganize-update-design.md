# Spec：记录层 R5（delete-mark + GarbageList 复用 + purge + reorganize + update）— innodb-record-design.md §7/§10.2/§10.3/§10.4

- 日期：2026-06-16
- 关联设计：`docs/design/innodb-record-design.md`（§7 GarbageList/FreeSpace、§10.1 HeapSpaceManager 复用、§10.2 Update、§10.3 Delete-Mark/Purge、§10.4 Page Reorganize、§14 操作接口、§15 异常、§16 测试、§17 step7/step10）
- 上游依赖：record.page（R3 RecordPage/RecordPageDirectory/IndexPageHeader、R4 RecordCursor/RecordComparator/RecordPageSearch/RecordPageInserter）、record.format（RecordEncoder/RecordHeader）、record.type、record.schema、storage.buf（PageGuard）、domain。
- 前置：R1+R2+R3+R4 全绿。
- 状态：R5 = 「记录在页上可删、可回收空间、可重组、可更新」的闭环，覆盖 §17 step7（update/delete-mark/purge）+ step10（reorganize）的**物理/结构层子集**。

## 1. 背景与范围

R4 让记录可读、可按 key 有序插入。R5 补齐页内记录的其余生命周期操作：**delete-mark**（逻辑删除，保留链中）、**GarbageList 空间回收与复用**、**purge**（物理摘链 + 回收）、**page reorganize**（碎片整理）、**update**（原地 / 页内搬迁 / key 变化信号）。

**强制简化（trx/MVCC/undo/隐藏列暂停所致，必须写进代码注释）：**
- 不写 undo、不读写 `DB_TRX_ID`/`DB_ROLL_PTR`（隐藏列 R4 已推迟）。所有操作只改物理记录与页结构。
- purge **无 MVCC 安全门**：本片 purge 是「调用方决定即物理摘除」；§10.3 step1 的「purge view 安全」属 trx 层职责，留占位注释，本片仅要求目标已 delete-marked。
- update **key 变化不做跨页重定位**（无 B+Tree）：返回 `REQUIRES_REINSERT` 信号，交调用方按 lifecycle 处理 —— 本片简化层为 **`deleteMark(旧) → purge(旧) → insert(新)`**（purge 前置要求已 delete-marked，故先 deleteMark）；贴近 MVCC 时则为 `deleteMark(旧) + insert(新)`、purge 延后由 trx 层决定。不可直接 `purge(旧)`（未 delete-marked 会被 purge 前置校验拒绝）。
- 不新增异常类；复用 `RecordPageOverflowException`/`PageDirectoryCorruptedException`/`DatabaseValidationException`。`RecordPageReorganizedException`/`RecordPurgeConflictException` 等留到 trx/btree 集成。

**做**：delete-mark、GarbageList+HeapSpaceManager（含改造 inserter 复用）、purge（含 n_owned/目录维护与组合并）、reorganize、update（三模式）。

**不做**（注释标注，后续片）：undo/隐藏列/MVCC purge view、跨页 split/merge/重定位（B+Tree）、RecordRef 重校验层（reorganize 后失效检测）、prefix index 比较（沿用 R4，prefixBytes 忽略）。

## 2. 包与依赖

- `record.page` 新增：`RecordPageDeleter`、`HeapSpaceManager`、`RecordPagePurger`、`RecordPageReorganizer`、`RecordPageUpdater`、`UpdateOutcome`（enum）、`UpdateResult`（record）。
- `record.page` 修改：`RecordPage` 增 `setDeleted(int,boolean)`、`findPredecessor(int)`；`RecordPageInserter` 改走 `HeapSpaceManager.allocate`。
- 无环：依赖方向不变（record.page → record.format/type/schema → buf/domain）。

## 3. 关键决策（再检查后的落地选择，写进代码注释）

1. **delete-mark 只翻 flags 位**：`setDeleted` 读-改-写记录起始 flags 字节的 bit0，保留 bit1（min-rec）与 bit2-3（recordType）。不可用 `writeRecordBytes`（会覆盖整头）。
2. **GarbageList 表示**：header `FREE`=空闲碎片链头（0=空），各碎片用 `next_record` 字段串接（碎片已离开用户链，复用该字段安全）。碎片**容量来自保留的 `RecordHeader.recordLength`**（free 时不动 recordLength）。`GARBAGE`=**已跟踪垃圾字节数**（reorganize 触发启发式计数），= `free()` 累加的整条 recordLength − 复用时扣减的整条碎片容量 + 原地缩短累加的死字节；**不等于物理总死空间**（oversized first-fit 复用的余量是未跟踪内部碎片，不计入 GARBAGE）。测试只断言每操作的 GARBAGE 增量与 reorganize 后归零，不在 oversized 复用后断言它等于所有物理碎片之和。
3. **复用策略 first-fit + 整块消费**：`HeapSpaceManager.allocate(need)` 扫 FREE 链找首个 `recordLength≥need` 的碎片→摘下、**沿用其 heapNo**、`nHeap` 不变、`GARBAGE-=该碎片容量`（余量为未跟踪内部碎片，留 reorganize 回收）；找不到则 `page.allocateFromFreeSpace`（新 heapNo、推进 heapTop/nHeap）。FREE 链遍历带 cycle/bounds 守卫（maxSteps=nHeap，越界/成环抛 `PageDirectoryCorruptedException`）。
4. **物理位置与链序解耦**：复用碎片使记录可落在页内任意空位；逻辑序由 `next_record`（key 序）决定，`recordOffsetsInOrder` 按链走，与物理 offset 无关。
5. **purge 要求目标已 delete-marked**（否则 `DatabaseValidationException`），拒绝 infimum/supremum；purge 后 `nRecs--`。
6. **reorganize 重排 heapNo**（稠密 2..n+1），使所有 `RecordRef` 失效（本片不提供失效检测，文档说明）；保留 delete-marked 记录（仍在链中），自然丢弃已 purge（已离链）记录与全部 garbage。
7. **update key 变化返回 `REQUIRES_REINSERT`**（控制流，非异常）；原地/搬迁返回新 `RecordRef`。
8. **owner / n_owned 维护沿用 R4 不变量**：`MIN_N_OWNED=4`/`MAX_N_OWNED=8`（复用 `RecordPageInserter` 常量），infimum 槽恒 owns 1，中间组 ∈[MIN..MAX]，supremum 组 ∈[1..MAX]；owner = 沿链首个 `n_owned>0` 的记录。

## 4. RecordPage 新增原语

- `void setDeleted(int offset, boolean deleted)`（X）：读 `offset` 处 flags 字节，置/清 bit0 后写回；保留其余位。
- `int findPredecessor(int offset)`（S/X）：从 infimum 沿 `next_record` 找到 `next==offset` 的记录偏移（offset 为首条用户记录时返回 `infimumOffset()`）。带 cycle/bounds 守卫（步数>nHeap 或越界抛 `PageDirectoryCorruptedException`）；找不到（offset 不在链中）抛 `PageDirectoryCorruptedException`。供 purge / update 搬迁定位前驱与重写前驱链。

> `FREE`/`GARBAGE` 的读写由 `HeapSpaceManager` 经 `RecordPage.header()`/`writeHeader()` 做整 header read-modify-write，不在 `RecordPage` 上加细粒度 setter（保持 RecordPage 精简；单 X latch 内串行，RMW 安全）。

## 5. R5a：delete-mark（RecordPageDeleter）

- `RecordPageDeleter`（无状态）。
- `void deleteMark(RecordPage page, int recordOffset)`（X）：
  - 校验：`recordHeaderAt(off).recordType()` 为用户记录（非 INFIMUM/SUPREMUM），且当前未 delete-marked（否则 `DatabaseValidationException`）。
  - `page.setDeleted(off, true)`。
  - **不改** `next_record`/`n_owned`/`nRecs`/目录（记录仍在链中，供历史版本；nRecs 含 delete-marked，§7）。
- 测试：标记后 `cursor.isDeleted()==true`、仍在 `recordOffsetsInOrder`、仍可 `findEqual`；nRecs 不变；重复标记/标记系统记录抛异常。

## 6. R5b：GarbageList + HeapSpaceManager + Inserter 改造

### 6.1 HeapSpaceManager（分配策略）
- `HeapSpaceManager(RecordPage page)`（绑定单页，单 X latch 内使用）。
- `record Allocation(int offset, int heapNo, boolean reused)`。
- `Allocation allocate(int neededBytes)`（X）：
  1. 读 `FREE`；沿碎片链 first-fit 找 `recordHeaderAt(frag).recordLength() >= neededBytes` 的首个碎片 `frag`（带守卫）。
  2. 命中：从 FREE 链摘除 `frag`（前驱碎片 next 改指 `frag` 的 next，或 FREE 头改指）；`heapNo=recordHeaderAt(frag).heapNo()`；`GARBAGE-=recordLength(frag)`；写回 header；返回 `Allocation(frag, heapNo, true)`。
  3. 未命中：`heapNo=page.nextHeapNo()`；`offset=page.allocateFromFreeSpace(neededBytes)`（页满抛 `RecordPageOverflowException`）；返回 `Allocation(offset, heapNo, false)`。
- `void free(int offset)`（X）：把 `offset` 记录空间压入 FREE 链头：`setNextRecord(offset, FREE)`；`FREE=offset`；`GARBAGE+=recordHeaderAt(offset).recordLength()`；写回 header。调用方须保证 `offset` 已离开用户链（purge/update 搬迁负责先摘链）。
- 读 `FREE`/`GARBAGE` 经 `page.header()`，写经 `page.writeHeader(...)`（整 header RMW）。

### 6.2 RecordPageInserter 改造
- step4 改为 `HeapSpaceManager.allocate(bytes.length)`：用返回的 `offset`/`heapNo` 落记录（`setHeapNo(off, alloc.heapNo())`）。其余（findInsertPosition、链入、header 计数、ownership/split）不变。
- 改前跑 `gitnexus_impact`(RecordPageInserter/RecordPage)；保持 R4 `RecordPageInserterTest` 全绿（新建空页无 garbage 时走 FreeSpace 分支，行为等价）。
- 测试（本批补）：先插入若干→对其中一条走「free（经下文 purge 或直接 HeapSpaceManager.free 测试桩）」→再插入一条 ≤ 碎片容量者→断言复用了该 offset、heapNo 沿用、GARBAGE 归零、FreeSpace 未被消耗；插入 > 碎片容量者→回退 FreeSpace。

## 7. R5c：purge（RecordPagePurger）

- `RecordPagePurger(...)`（持 `HeapSpaceManager` 经 page 构造，或方法内构造）。
- `void purge(RecordPage page, int recordOffset)`（X）。**结构：先 plan（全为读 + 校验 + 计算 prev/owner/槽下标/合并决策），plan 全部成功后再进 execute（连续写，不再失败）**——避免「摘链后定位槽失败」留下半改页。
  - **Plan 阶段（只读 + 校验，任何失败在写页前抛出）：**
    1. 校验：用户记录、已 delete-marked（否则 `DatabaseValidationException`）。
    2. `prev = page.findPredecessor(recordOffset)`（找不到/损坏抛 `PageDirectoryCorruptedException`）。
    3. `owner =` 沿 `next_record` 从 `recordOffset` 起首个 `n_owned>0` 的记录。
    4. 若 `target==owner`：定位槽 `h`（`slot(h)==target`，找不到抛 `PageDirectoryCorruptedException`），记 `cnt=nOwned(target)`。否则定位 owner 的槽下标 `hh`（用于合并判断）。
    5. 计算合并决策（不写）：受影响 owner 的最终 `n_owned` 是否 `<MIN_N_OWNED` 且可与后一组（`nextOwner=slot(hh+1)`）合并（和 `<=MAX_N_OWNED` 且 owner 非 supremum）。
  - **Execute 阶段（连续写）：**
    6. 摘链：`setNextRecord(prev, nextRecord(recordOffset))`。
    7. 目录/n_owned 维护：
       - **target == owner**：`cnt==1`（组仅此一员）→ `directory.removeSlot(h)`；`cnt>=2` → `newOwner=prev`（链上前一条，必在本组内）、`directory.setSlot(h, newOwner)`、`setNOwned(newOwner, cnt-1)`；受影响 owner=newOwner、hh=h。（target 本身将被 free，其残留 n_owned 不影响——已离页。）
       - **target != owner**（interior）：`setNOwned(owner, nOwned(owner)-1)`；受影响 owner=owner、hh=其槽下标。
    8. 组合并（按 plan 决策；仅中间组、受影响 owner 非 supremum、和 `<=MAX`）：`setNOwned(nextOwner, nOwned(nextOwner)+nOwned(owner))`、**`setNOwned(owner, 0)`**（旧 owner 降为 interior，必须清零——否则 inserter 的 owner-walk 会在它处误判 owner，破坏目录不变量）、`directory.removeSlot(hh)`。否则留小组（简化，不 borrow 再分配）。
    9. `HeapSpaceManager.free(recordOffset)`（压入 GarbageList、GARBAGE+=len）。
    10. `nRecs--`（经 header RMW）。
- 测试：purge interior（owner.n_owned--、链摘除、GARBAGE+、nRecs--、仍可查其余 key）；purge 组末记录（槽改指 / 移交 n_owned）；purge 致组过小→合并（slotCount 减）；purge 后空间被后续 insert 复用（与 R5b 联测）；purge 非 delete-marked / 系统记录抛异常；purge 后 `recordOffsetsInOrder` 不含该记录。

## 8. R5d：page reorganize（RecordPageReorganizer）

- `RecordPageReorganizer(...)`。
- `void reorganize(RecordPage page)`（X）：
  1. 读 `header()` 取 `indexId`/`level`。
  2. 按链快照：`for off in recordOffsetsInOrder(): bytes=readRecordBytes(off)`（含 delete-marked，保留其 flags；已 purge 者不在链中自然丢弃）。
  3. `page.format(indexId, level)`（重置：infimum/supremum、2 槽、heapTop=98、nHeap=2、nRecs=0、FREE=0、GARBAGE=0）。
  4. 稠密重排：对每条快照 bytes 顺序 `off2=allocateFromFreeSpace(len)`、`writeRecordBytes(off2,bytes)`、`setHeapNo(off2, 该次 heapNo)`（heapNo 由 allocate 推进，稠密 2..）、**`setNOwned(off2, 0)`**（快照 bytes 带来旧布局的 n_owned，必须显式清零——否则旧 owner 在新目录里非组末却残留正数，破坏 inserter 的 owner-walk 不变量）；串 `infimum→r1→...→rn→supremum`（`setNextRecord`）。
  5. **重建目录 + n_owned**（确定性规则，保证不变量；建立在 step4 已把所有用户记录 n_owned 清零之上）：**每第 `MAX_N_OWNED` 条用户记录（第 8、16、24…条）作为一个中间组末**，`directory.insertSlot(slotCount()-1, 该记录)`（插在 supremum 槽前）并 `setNOwned(=MAX_N_OWNED)`；剩余的尾部不足 `MAX_N_OWNED` 条记录归 supremum 组，`setNOwned(supremum, 尾部条数+1)`。这样中间组恒 =MAX（∈[MIN..MAX] ✓），supremum 组 ∈[1..MAX] ✓。零用户记录时仅 format 后即止（supremum 保持 n_owned=1）。
  6. `nRecs=` 用户记录数（经 header RMW；delete-marked 计入）。
- 测试：含 delete-marked 的页 reorganize 后链/目录一致、delete-marked 保留、`GARBAGE==0`、`FREE==0`、heapNo 稠密、heapTop 收紧、所有 key 仍可查；purge 产生 garbage 后 reorganize 回收（freeSpace 增大）；property：随机 insert/delete-mark/purge 后 reorganize，全不变量成立。

## 9. R5e：update（RecordPageUpdater）

- `RecordPageUpdater(TypeCodecRegistry registry)`（持 `RecordEncoder`/`RecordComparator`/`HeapSpaceManager` 经 page）。
- `enum UpdateOutcome { IN_PLACE, MOVED, REQUIRES_REINSERT }`。
- `record UpdateResult(UpdateOutcome outcome, RecordRef newRef)`（REQUIRES_REINSERT 时 newRef=null）。
- `UpdateResult update(RecordPage page, PageId pageId, int recordOffset, LogicalRecord newRecord, IndexKeyDef keyDef, TableSchema schema)`（X）。**同样 plan-then-execute：所有读/校验/分支判定与（搬迁时）prev、owner 槽下标都在写页前算好，再进入写。**
  1. **校验**：`recordOffset` 为用户记录（非 INFIMUM/SUPREMUM）且**未 delete-marked**（否则 `DatabaseValidationException`）——本片 update 只更新存活的普通记录，不复活 delete-marked 记录（`RecordEncoder` 按 `newRecord.deleted()` 写 header，默认 false 会清除删除标记，故须前置拒绝）。
  2. 读旧记录 cursor / 旧 `RecordHeader`（捕获 `oldLen/oldHeapNo/oldNext/oldNOwned`）。
  3. **key 变化检测**：由 `newRecord` 构 `SearchKey`，`RecordComparator.compare(oldCursor, newKey, keyDef, schema) != 0` → 返回 `UpdateResult(REQUIRES_REINSERT, null)`（不改页）。
  4. `newBytes = encoder.encode(newRecord, schema)`；`newLen=newBytes.length`。
  5. **原地**（`newLen<=oldLen`）：`writeRecordBytes(off, newBytes)`；**恢复** `setHeapNo(off, oldHeapNo)`、`setNextRecord(off, oldNext)`、`setNOwned(off, oldNOwned)`（writeRecordBytes 覆盖整头，必须回写这三者；newBytes 自带 recordLength 生效，deleted 恒 false 因 step1 已拒绝 delete-marked）；若 `newLen<oldLen`：`GARBAGE += (oldLen-newLen)`（仅统计，不入 FREE 链）。返回 `IN_PLACE` + 原位 RecordRef。
  6. **搬迁**（`newLen>oldLen`）——先把可失败/只读步骤前置：
     - **plan**：`prev = findPredecessor(off)`（只读，失败抛 `PageDirectoryCorruptedException`）；若 `oldNOwned>0` 先定位槽 `h`（slot(h)==off，失败抛 `PageDirectoryCorruptedException`）。
     - `alloc = heap.allocate(newLen)`（页满抛 `RecordPageOverflowException`；这是搬迁路径**第一处写页**，此前 plan 全为读——若它抛出，页未被修改；命中碎片会改 FREE，属可提交副作用，其后步骤不再失败）。
     - 写新：`writeRecordBytes(alloc.offset, newBytes)`、`setHeapNo(alloc.offset, alloc.heapNo)`、`setNextRecord(alloc.offset, oldNext)`。
     - 改前驱：`setNextRecord(prev, alloc.offset)`。
     - owner 处理：`oldNOwned>0` → `setSlot(h, alloc.offset)`、`setNOwned(alloc.offset, oldNOwned)`；interior（oldNOwned==0）→ 新记录 n_owned 保持 0（newBytes 默认），owner 与计数不变。
     - `heap.free(off)`（旧空间入 GarbageList）。
     - 返回 `MOVED` + 新位置 RecordRef（heapNo 已变）。
- `nRecs` 不变（记录数不变）。
- 测试：原地（等长/变短，payload 更新、heapNo/next/n_owned 保持、变短累计 GARBAGE）；搬迁（变长且有空间：新位置、前驱链正确、旧空间入 garbage、owner 槽改指、可查回）；key 变化→REQUIRES_REINSERT 且页不变；搬迁页满→RecordPageOverflowException 且页不变；update 组末记录搬迁后目录一致；update 系统记录 / 已 delete-marked 记录→`DatabaseValidationException`。

## 10. 异常

- 复用：`RecordPageOverflowException`（页满）、`PageDirectoryCorruptedException`（链/槽损坏、findPredecessor 失败）、`DatabaseValidationException`（非法 purge/delete-mark 目标、参数）。
- 本片**不新增**异常类。MVCC purge 门（`RecordPurgeConflictException`）、reorganize 后 ref 失效（`RecordPageReorganizedException`）留到 trx/btree 集成。

## 11. 并发与 latch（§13）

- 全部操作要求单页 **X latch**（PageGuard 自校验）；不跨页、不写 redo/undo、不等行锁。
- 无 synchronized；算子无状态（HeapSpaceManager 绑定单 page、单 latch 内用，不跨 latch 缓存）。
- `RecordRef` 仅短期定位（§5.1）：reorganize / update 搬迁 / purge 组重排后须重新校验（本片不提供校验层，调用方注意）。

## 12. 批次拆分（writing-plans 细化）

- **R5a**：RecordPage.setDeleted + RecordPageDeleter。测试：delete-mark。
- **R5b**：RecordPage.findPredecessor + HeapSpaceManager + RecordPageInserter 改造（impact + 回归）。测试：free/allocate first-fit/回退、inserter 复用。
- **R5c**：RecordPagePurger（摘链 + n_owned/目录 + 组合并 + free + nRecs--）。测试：interior/组末/合并/复用/异常。
- **R5d**：RecordPageReorganizer。测试：garbage 回收、delete-marked 保留、heapNo 稠密、不变量、property。
- **R5e**：RecordPageUpdater + UpdateOutcome/UpdateResult。测试：原地/搬迁/key 变化/overflow。

每批 TDD → 全量 `clean test`（固定 JDK/Gradle）→ 收口刷 `npx gitnexus analyze`。R5 全绿后报告；R6（含隐藏列/MVCC 前的剩余项或跨页 B+Tree）单独确认再开。redo/recovery 仍暂停。
