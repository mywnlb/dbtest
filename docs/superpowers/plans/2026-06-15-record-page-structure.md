# Record 层 R3（record.page 页内结构）Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:executing-plans（本仓库惯例：inline 自治执行、批次末跑全量 `clean test`）。步骤用 `- [ ]`。**项目规则：不 commit**——「Commit」步替换为「全量回归 checkpoint」，工作保持未提交。

**Goal:** 让 16KB INDEX 页具备页内记录区结构与低层导航原语：空页初始化、INDEX page header、infimum/supremum、heap 分配、next_record 链、PageDirectory 槽。

**Architecture:** 新增 `cn.zhangyis.db.storage.record.page`，绑定 `PageGuard`（buf 层）在页体 `[38, pageSize-8)` 上工作；复用 R2 `RecordHeader` 写系统记录；不依赖 fsp（信封由调用方盖）。无 key 语义（比较/有序插入/游标归 R4）。

**Tech Stack:** Java 25、JUnit Jupiter、固定 JDK `C:\Program Files\Java\jdk-25.0.2` + 固定 Gradle `D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat`。

**测试命令（PowerShell）:** `$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --console=plain`

**真实 PageGuard 测试 harness（复用 §5.3 FilePageHeaderTest 同款）:**
```java
PageStore store = new FileChannelPageStore();
store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(4));
try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 4)) {
    try (PageGuard g = pool.getPage(PageId.of(SPACE, PageNo.of(3)), PageLatchMode.EXCLUSIVE)) {
        // ... 操作 g ...
    }
}
// PS = PageSize.ofBytes(16*1024); SPACE = SpaceId.of(1); @TempDir Path dir;
```

---

## Batch 1：页 header + 系统记录 + 空页 format

### Task 1.1：`IndexPageLayout` 常量 + 钉死测试

**Files:**
- Create: `src/main/java/cn/zhangyis/db/storage/record/page/IndexPageLayout.java`
- Test: `src/test/java/cn/zhangyis/db/storage/record/page/IndexPageLayoutTest.java`

- [ ] **Step 1: 写失败测试** `IndexPageLayoutTest`：断言 `FIL_PAGE_HEADER_BYTES==38`、`FIL_PAGE_TRAILER_BYTES==8`、`PAGE_HEADER_START==38`、`PAGE_HEADER_END==66`、`INFIMUM_OFFSET==66`、`SUPREMUM_OFFSET==82`、`SYS_REC_BYTES==16`、`USER_RECORDS_START==98`、`DIR_SLOT_BYTES==2`；`INFIMUM_LABEL`/`SUPREMUM_LABEL` 各 8 字节（UTF-8）。
- [ ] **Step 2: 跑测试确认编译失败**（类不存在）。
- [ ] **Step 3: 实现 `IndexPageLayout`**（包私有 final、私有构造）。常量按 spec §3；中文 Javadoc 标注「`FIL_PAGE_HEADER_BYTES`/`FIL_PAGE_TRAILER_BYTES` 必须与 §5.3 `FilePageHeaderLayout.SIZE`/trailer 一致——教学简化本地复刻，后续可提到共享 `storage.page` 包」。
- [ ] **Step 4: 跑测试确认通过**。

> 注：`IndexPageLayout` 与 `Test` 同包但测试在 `src/test`，包私有可见——测试包名须为 `cn.zhangyis.db.storage.record.page`。

### Task 1.2：`PageU16` 页内 u16 读写助手

**Files:**
- Create: `src/main/java/cn/zhangyis/db/storage/record/page/PageU16.java`
- Test: `src/test/java/cn/zhangyis/db/storage/record/page/PageU16Test.java`

- [ ] **Step 1: 写失败测试**：经真实 PageGuard（X），`PageU16.put(g, 100, 0xABCD); assertEquals(0xABCD, PageU16.get(g, 100));`；`put(g,102,0)`、`put(g,104,0xFFFF)` 往返；越界值（-1、0x10000）→ `DatabaseValidationException`。
- [ ] **Step 2: 跑确认失败**。
- [ ] **Step 3: 实现 `PageU16`**（包私有）：`get(PageGuard,int)` = `((readBytes(off,2)[0]&0xFF)<<8)|(b[1]&0xFF)`；`put(PageGuard,int,int)`：校验 0..0xFFFF（否则 `DatabaseValidationException`），`writeBytes(off, new byte[]{(byte)(v>>8),(byte)v})`（大端，writeBytes 自动 markDirty 且要求 X）。
- [ ] **Step 4: 跑确认通过**。

### Task 1.3：`IndexPageDirection` 枚举

**Files:**
- Create: `src/main/java/cn/zhangyis/db/storage/record/page/IndexPageDirection.java`
- Create: `src/main/java/cn/zhangyis/db/storage/record/page/PageDirectoryCorruptedException.java`
- Test: `src/test/java/cn/zhangyis/db/storage/record/page/IndexPageDirectionTest.java`

- [ ] **Step 1: 写失败测试**：`fromCode(code())` 往返 NO_DIRECTION/LEFT/RIGHT；`fromCode(99)` → `PageDirectoryCorruptedException`。
- [ ] **Step 2: 跑确认失败**。
- [ ] **Step 3: 实现**：`PageDirectoryCorruptedException extends DatabaseRuntimeException`（message + cause 构造）；`IndexPageDirection{NO_DIRECTION(0),LEFT(1),RIGHT(2)}` + `code()`/`fromCode()`（未知 → PageDirectoryCorruptedException）。
- [ ] **Step 4: 跑确认通过**。

### Task 1.4：`IndexPageHeaderLayout` 偏移 + `IndexPageHeader` 值对象

**Files:**
- Create: `src/main/java/cn/zhangyis/db/storage/record/page/IndexPageHeaderLayout.java`
- Create: `src/main/java/cn/zhangyis/db/storage/record/page/IndexPageHeader.java`
- Test: `src/test/java/cn/zhangyis/db/storage/record/page/IndexPageHeaderTest.java`

- [ ] **Step 1: 写失败测试**：构造合法 `IndexPageHeader`（nDirSlots=2,heapTop=98,nHeap=2,free=0,garbage=0,lastInsert=0,dir=NO_DIRECTION,nDirection=0,nRecs=0,level=0,indexId=42），经 PageGuard `writeTo`/`readFrom` 往返 `assertEquals`；构造校验：u16 越界（heapTop=0x10000）→ `DatabaseValidationException`、nDirSlots=1 → 拒绝、nHeap=1 → 拒绝、direction null → 拒绝、indexId<0 → 拒绝。
- [ ] **Step 2: 跑确认失败**。
- [ ] **Step 3: 实现** `IndexPageHeaderLayout`（包私有常量，偏移按 spec §4，基于 38；INDEX_ID 为 long@58）；`IndexPageHeader`（record，11 字段 + 构造校验 + `writeTo(PageGuard)`（X，逐字段 `PageU16.put` / `guard.writeLong(INDEX_ID,...)`）+ `readFrom(PageGuard)`）。中文 Javadoc 标注各字段语义 + 「FREE/GARBAGE 本片只读写字段，复用算法归 R4」。
- [ ] **Step 4: 跑确认通过**。

### Task 1.5：`RecordPage.format()` + 系统记录读 + 几何

**Files:**
- Create: `src/main/java/cn/zhangyis/db/storage/record/page/RecordPage.java`
- Test: `src/test/java/cn/zhangyis/db/storage/record/page/RecordPageFormatTest.java`

- [ ] **Step 1: 写失败测试** `RecordPageFormatTest`：`RecordPage.format(g, /*indexId*/7, /*level*/0)` 后：
  - `RecordPage rp = new RecordPage(g, PS);` `IndexPageHeader h = rp.header();` 断言 nDirSlots=2、heapTop=98、nHeap=2、nRecs=0、level=0、indexId=7、direction=NO_DIRECTION。
  - `rp.recordHeaderAt(rp.infimumOffset())`：type=INFIMUM、heapNo=0、nOwned=1、next=82、recordLength=16；标签 `rp.systemLabelAt(66)` 等于 "infimum\0" 字节。
  - supremum（82）：type=SUPREMUM、heapNo=1、nOwned=1、next=0；标签 "supremum"。
  - directory：`new RecordPageDirectory(g, PS)` 暂未建 → 本任务改为直接 `PageU16.get` 读 slot：slot[0]@(16384-8-2)=66、slot[1]@(16384-8-4)=82。
  - `rp.freeSpace()` == 16372-98 == 16274；`rp.recordOffsetsInOrder()` 为空 list。
- [ ] **Step 2: 跑确认失败**。
- [ ] **Step 3: 实现 `RecordPage`（batch1 子集）**：字段 `PageGuard guard`、`PageSize pageSize`；`static void format(PageGuard,long indexId,int level)`（X）：写 header、infimum/supremum（用 `RecordHeader(...).writeTo(tmp8,0)` 后 `guard.writeBytes(off,tmp8)`，标签 `guard.writeBytes(off+8, label)`）、两槽（`PageU16.put` 到 `dirEnd-2`、`dirEnd-4`，`dirEnd=pageSize.bytes()-8`）；`header()`、`infimumOffset()`=66、`supremumOffset()`=82、`recordHeaderAt(int)`（`RecordHeader.readFrom(guard.readBytes(off,8),0)`）、`systemLabelAt(int)`（`guard.readBytes(off+8,8)`）、`freeSpace()`（dirStart-heapTop，dirStart=`dirEnd - nDirSlots*2`，读 header.nDirSlots/heapTop）。`recordOffsetsInOrder()` 留到 batch2（batch1 可先返回空遍历——实现简单版即可，batch2 加防御）。
  - 注：`format` 不盖 FilePageHeader（中文注释标注调用方负责 PageType.INDEX）。
- [ ] **Step 4: 跑确认通过**。

### Checkpoint B1（替代 commit）
- [ ] 跑全量 `clean test --console=plain`，确认 BUILD SUCCESSFUL；不 commit。

---

## Batch 2：heap 分配 + next_record 链 + PageDirectory

### Task 2.1：`RecordPageOverflowException` + `allocateFromFreeSpace`

**Files:**
- Create: `src/main/java/cn/zhangyis/db/storage/record/page/RecordPageOverflowException.java`
- Modify: `RecordPage.java`（加 `allocateFromFreeSpace`、`nextHeapNo`）
- Test: `src/test/java/cn/zhangyis/db/storage/record/page/RecordPageHeapTest.java`

- [ ] **Step 1: 写失败测试** `RecordPageHeapTest`：format 后：
  - `int off1 = rp.allocateFromFreeSpace(16);` 断言 off1==98；`rp.header().heapTop()`==114；`rp.header().nHeap()`==3；`off1` 之前 `rp.nextHeapNo()`==2。
  - 第二次 `allocateFromFreeSpace(16)`==114、heapTop==130、nHeap==4。
  - allocate 后 infimum.next 仍==82、directory 槽数仍==2、nRecs 仍==0（断言 allocate 不动链/目录/nRecs）。
  - `allocateFromFreeSpace(0)` → `DatabaseValidationException`；`allocateFromFreeSpace(freeSpace()+1)` → `RecordPageOverflowException`。
- [ ] **Step 2: 跑确认失败**。
- [ ] **Step 3: 实现**：`RecordPageOverflowException extends DatabaseRuntimeException`（message+cause）。`RecordPage.allocateFromFreeSpace(int bytes)`（X）：`bytes<=0` → `DatabaseValidationException`；读 header；`bytes > freeSpace()` → `RecordPageOverflowException`；`int off = heapTop`；`PageU16.put(HEAP_TOP, off+bytes)`、`PageU16.put(N_HEAP, nHeap+1)`；返回 off。`nextHeapNo()` = 读 N_HEAP。中文注释标注「不 wire next_record/directory/nRecs——key 有序串接归 R4 insert」。
- [ ] **Step 4: 跑确认通过**。

### Task 2.2：`nextRecord` / `setNextRecord` / `recordOffsetsInOrder`（防御）

**Files:**
- Modify: `RecordPage.java`
- Test: `src/test/java/cn/zhangyis/db/storage/record/page/RecordPageChainTest.java`

- [ ] **Step 1: 写失败测试** `RecordPageChainTest`：format 后分配 3 条 16 字节记录 off1/off2/off3（手工给每条写一个 RecordHeader：`new RecordHeader(false,false,CONVENTIONAL,heapNo,0,0,16).writeTo(tmp,0); g.writeBytes(off,tmp);`）；手工串：infimum.next=off1、off1.next=off2、off2.next=off3、off3.next=supremum(82)（`rp.setNextRecord(...)`）；断言 `rp.recordOffsetsInOrder()` == `[off1,off2,off3]`；`rp.nextRecord(off1)`==off2。
  - 环测试：`rp.setNextRecord(off3, off1)`（指回 off1）→ `recordOffsetsInOrder()` 抛 `PageDirectoryCorruptedException`。
  - 越界测试：format 新页，`rp.setNextRecord(infimum, 99999)` → `recordOffsetsInOrder()` 抛 `PageDirectoryCorruptedException`（next 越出页体且非 supremum）。
- [ ] **Step 2: 跑确认失败**。
- [ ] **Step 3: 实现**：`nextRecord(int off)` = 读 `off + RecordHeaderLayout.NEXT_RECORD_OFFSET` 处 u16（`PageU16.get`）。`setNextRecord(int off,int target)`（X）= `PageU16.put(off+NEXT_RECORD_OFFSET, target)`。`recordOffsetsInOrder()`：从 `nextRecord(infimumOffset())` 起循环，遇 `supremumOffset()` 停；每步校验 `cur` 在 `[USER_RECORDS_START, dirStart())` 内否则 `PageDirectoryCorruptedException`；步数 > `nHeap` → `PageDirectoryCorruptedException`（环防御）；收集 cur。
  - 复用 `cn.zhangyis.db.storage.record.format.RecordHeaderLayout.NEXT_RECORD_OFFSET`（包私有？——确认可见性）。若 `RecordHeaderLayout` 包私有不可跨包引用，则在 `IndexPageLayout` 定义 `REC_NEXT_FIELD_OFFSET=4` 并注释「= RecordHeaderLayout.NEXT_RECORD_OFFSET」。
- [ ] **Step 4: 跑确认通过**。

### Task 2.3：`RecordPageDirectory`

**Files:**
- Create: `src/main/java/cn/zhangyis/db/storage/record/page/RecordPageDirectory.java`
- Test: `src/test/java/cn/zhangyis/db/storage/record/page/RecordPageDirectoryTest.java`

- [ ] **Step 1: 写失败测试** `RecordPageDirectoryTest`：format 后 `RecordPageDirectory d = new RecordPageDirectory(g, PS);`：
  - `d.slotCount()`==2、`d.slot(0)`==66、`d.slot(1)`==82；`d.slot(2)` → `PageDirectoryCorruptedException`。
  - `d.setSlot(1, 200); assertEquals(200, d.slot(1));`。
  - `insertSlot`：format 新页，`d.insertSlot(1, 150, PS)`：slotCount==3、slot(0)==66、slot(1)==150、slot(2)==82（原 slot1=supremum 逻辑右移）。
  - `removeSlot(1, PS)`：回到 slotCount==2、slot(1)==82。
  - `removeSlot` 到 2 时再 remove → `PageDirectoryCorruptedException`（不得 <2）。
  - overflow：把 heapTop 撑到接近 dirStart（allocate 大块）后 `insertSlot` → `RecordPageOverflowException`。
- [ ] **Step 2: 跑确认失败**。
- [ ] **Step 3: 实现 `RecordPageDirectory`**：字段 guard/pageSize；`dirEnd()`=`pageSize.bytes()-8`；`slotAddr(i)`=`dirEnd()-(i+1)*2`。`slotCount()`=读 header N_DIR_SLOTS。`slot(i)`：i 越界 → corrupt；`PageU16.get(slotAddr(i))`。`setSlot(i,off)`（X）：i 越界 → corrupt；`PageU16.put`。`insertSlot(int at, int off, PageSize)`（X）：`at∈[0,n]`；校验 free space 容新槽（`dirStart-2 >= heapTop` 否则 `RecordPageOverflowException`）；从尾向 at 把槽逐个下移一格（slotAddr 重算），N_DIR_SLOTS+1，写新槽。`removeSlot(int at, PageSize)`（X）：n>2 否则 corrupt；at∈[0,n)；上移、N_DIR_SLOTS-1。
  - 中文注释标注 slot 序（slot0 紧贴 trailer）与 n_owned 由调用方维护（R4）。
- [ ] **Step 4: 跑确认通过**。

### Checkpoint B2（替代 commit）
- [ ] 跑全量 `clean test --console=plain`，确认 BUILD SUCCESSFUL；不 commit。
- [ ] 跑 `npx gitnexus analyze` 刷新索引（FTS 警告为环境性，可忽略）。

---

## Self-Review（spec 覆盖）
- §3 几何/常量 → Task 1.1。 §4 header → 1.3/1.4。 §5 系统记录+format → 1.5。 §6 heap+链 → 2.1/2.2。 §7 directory → 2.3。 §8 异常 → 1.3(PageDirectoryCorrupted)/2.1(Overflow)。 §9 latch → 复用 PageGuard 校验（各写步要求 X）。 §10 测试 → 各 Task 测试。 §11 批次 → batch1/2。
- 类型一致性：`RecordPage(PageGuard,PageSize)`、`RecordPageDirectory(PageGuard,PageSize)`、`format(PageGuard,long,int)`、`PageU16.get/put(PageGuard,int[,int])` 全程一致。
- 无 placeholder：每步给出关键逻辑与期望值。
