# 记录层 R5（delete-mark + GarbageList + purge + reorganize + update）Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 给 INDEX 页内记录补齐 delete-mark、空间回收复用（GarbageList）、purge、page reorganize、update 五类物理/结构操作，闭合「记录在页上可删、可回收、可重组、可更新」。

**Architecture:** 沿用 R3/R4 的 `record.page` 结构层：每个操作是无状态算子，绑定单页 X latch 的 `RecordPage`，复用既有原语（header RMW、next_record 链、PageDirectory、n_owned）。新增两枚 `RecordPage` 原语（`setDeleted`/`findPredecessor`）与一个目录查找（`RecordPageDirectory.indexOf`）。purge/update 一律 **plan-then-execute**（先把全部读+校验+槽/合并决策算好，再连续写页），避免半改页。

**Tech Stack:** Java 25、JUnit Jupiter、固定 JDK `C:\Program Files\Java\jdk-25.0.2`、固定 Gradle `D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1`。

**关联 spec：** `docs/superpowers/specs/2026-06-16-record-delete-purge-reorganize-update-design.md`

**项目规则（覆盖默认）：**
- **不提交 git**（工作保留在 master 未提交）。本计划用「全量 `clean test` 绿」作为每个 Task 的 checkpoint，**不含 commit 步骤**。
- 禁用 `synchronized`/`wait`/`notify`；禁裸 `IllegalArgumentException`/`RuntimeException`（用 `DatabaseValidationException`/`DatabaseRuntimeException` 或既有领域异常）。
- 中文 Javadoc，解释数据库语义/并发边界/简化点。
- 改既有符号前先跑 `gitnexus_impact`；R5 全绿后跑 `npx gitnexus analyze`。

**Checkpoint 命令（每 Task 末）：**
```
$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "cn.zhangyis.db.storage.record.*" --console=plain
```

---

## File Structure

**新建（production）：**
- `src/main/java/cn/zhangyis/db/storage/record/page/RecordPageDeleter.java` — delete-mark 算子。
- `src/main/java/cn/zhangyis/db/storage/record/page/HeapSpaceManager.java` — GarbageList first-fit 分配/回收策略。
- `src/main/java/cn/zhangyis/db/storage/record/page/RecordPagePurger.java` — purge 算子（摘链 + n_owned/目录 + 组合并 + free）。
- `src/main/java/cn/zhangyis/db/storage/record/page/RecordPageReorganizer.java` — 页重组算子。
- `src/main/java/cn/zhangyis/db/storage/record/page/UpdateOutcome.java` — update 结果枚举。
- `src/main/java/cn/zhangyis/db/storage/record/page/UpdateResult.java` — update 结果值对象。
- `src/main/java/cn/zhangyis/db/storage/record/page/RecordPageUpdater.java` — update 算子（原地/搬迁/key 变化信号）。

**修改（production）：**
- `RecordPage.java` — 新增 `setDeleted(int,boolean)`、`findPredecessor(int)`。
- `IndexPageLayout.java` — 新增 `REC_FLAGS_FIELD_OFFSET=0`。
- `IndexPageHeader.java` — 新增 `withFree/withGarbage/withNRecs` 拷贝方法（减少 11 字段重建噪声）。
- `RecordPageDirectory.java` — 新增 `indexOf(int)`。
- `RecordPageInserter.java` — 分配改走 `HeapSpaceManager.allocate`。

**新建（test）：**
- `RecordPageDeleterTest.java`、`RecordPageFindPredecessorTest.java`、`HeapSpaceManagerTest.java`、`RecordPagePurgerTest.java`、`RecordPageReorganizerTest.java`、`RecordPageUpdaterTest.java`，并在 `RecordPageInserterTest.java` 增 1 个复用用例。

所有 test 复用既有模式：`@TempDir` + `FileChannelPageStore` + `LruBufferPool` + `PageGuard(EXCLUSIVE)` + `RecordPage.format`。

---

## Task 1 (R5a)：delete-mark — RecordPage.setDeleted + RecordPageDeleter

**Files:**
- Modify: `src/main/java/cn/zhangyis/db/storage/record/page/IndexPageLayout.java`
- Modify: `src/main/java/cn/zhangyis/db/storage/record/page/RecordPage.java`
- Create: `src/main/java/cn/zhangyis/db/storage/record/page/RecordPageDeleter.java`
- Test: `src/test/java/cn/zhangyis/db/storage/record/page/RecordPageDeleterTest.java`

- [ ] **Step 1: Write the failing test**

Create `RecordPageDeleterTest.java`:

```java
package cn.zhangyis.db.storage.record.page;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.fil.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.PageStore;
import cn.zhangyis.db.storage.record.format.LogicalRecord;
import cn.zhangyis.db.storage.record.format.RecordType;
import cn.zhangyis.db.storage.record.schema.ColumnDef;
import cn.zhangyis.db.storage.record.schema.ColumnId;
import cn.zhangyis.db.storage.record.schema.ColumnType;
import cn.zhangyis.db.storage.record.schema.IndexKeyDef;
import cn.zhangyis.db.storage.record.schema.KeyOrder;
import cn.zhangyis.db.storage.record.schema.KeyPartDef;
import cn.zhangyis.db.storage.record.schema.TableSchema;
import cn.zhangyis.db.storage.record.type.ColumnValue;
import cn.zhangyis.db.storage.record.type.TypeCodecRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** RecordPageDeleter delete-mark：标记后仍在链/可查、isDeleted、nRecs 不变；重复/系统记录拒绝。 */
class RecordPageDeleterTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);
    private static final PageId PAGE = PageId.of(SPACE, PageNo.of(3));

    @TempDir
    Path dir;

    private final TypeCodecRegistry registry = new TypeCodecRegistry();
    private final RecordPageInserter inserter = new RecordPageInserter(registry);
    private final RecordPageSearch search = new RecordPageSearch(registry);
    private final RecordPageDeleter deleter = new RecordPageDeleter();

    private static TableSchema schema() {
        return new TableSchema(1, List.of(
                new ColumnDef(new ColumnId(0), "id", ColumnType.intType(false, false), 0),
                new ColumnDef(new ColumnId(1), "name", ColumnType.varchar(20, true), 1)));
    }

    private static IndexKeyDef idKey() {
        return new IndexKeyDef(7L, List.of(new KeyPartDef(new ColumnId(0), KeyOrder.ASC, 0)));
    }

    private static SearchKey k(long id) {
        return new SearchKey(List.of(new ColumnValue.IntValue(id)));
    }

    private static LogicalRecord row(long id, String name) {
        return new LogicalRecord(1, List.of(new ColumnValue.IntValue(id), new ColumnValue.StringValue(name)),
                false, RecordType.CONVENTIONAL);
    }

    private interface PageBody { void run(RecordPage rp, TableSchema schema); }

    private void onPage(PageBody body) {
        TableSchema schema = schema();
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(4));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 4)) {
            try (PageGuard g = pool.getPage(PAGE, PageLatchMode.EXCLUSIVE)) {
                RecordPage rp = new RecordPage(g, PS);
                rp.format(7, 0);
                body.run(rp, schema);
            }
        }
    }

    @Test
    void deleteMarkKeepsRecordInChainAndQueryable() {
        onPage((rp, schema) -> {
            IndexKeyDef kd = idKey();
            inserter.insert(rp, PAGE, row(10, "n10"), kd, schema);
            int off = inserter.insert(rp, PAGE, row(20, "n20"), kd, schema).pageOffset();
            inserter.insert(rp, PAGE, row(30, "n30"), kd, schema);
            int before = rp.header().nRecs();

            deleter.deleteMark(rp, off);

            RecordCursor c = new RecordCursor(rp, off, schema, registry);
            assertTrue(c.isDeleted(), "flag set");
            assertTrue(search.findEqual(rp, k(20), kd, schema).isPresent(), "still findable");
            assertTrue(rp.recordOffsetsInOrder().contains(off), "still in chain");
            assertEquals(before, rp.header().nRecs(), "nRecs unchanged (delete-marked counted)");
        });
    }

    @Test
    void rejectsDoubleDeleteMarkAndSystemRecord() {
        onPage((rp, schema) -> {
            IndexKeyDef kd = idKey();
            int off = inserter.insert(rp, PAGE, row(1, "a"), kd, schema).pageOffset();
            deleter.deleteMark(rp, off);
            assertThrows(DatabaseValidationException.class, () -> deleter.deleteMark(rp, off), "double delete-mark");
            assertThrows(DatabaseValidationException.class,
                    () -> deleter.deleteMark(rp, rp.supremumOffset()), "system record");
            // setDeleted(false) 复位后可再次 mark（验证位级 toggle 保留 recordType）。
            rp.setDeleted(off, false);
            assertFalse(new RecordCursor(rp, off, schema, registry).isDeleted());
            assertEquals(RecordType.CONVENTIONAL, new RecordCursor(rp, off, schema, registry).recordType());
        });
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:
```
$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "cn.zhangyis.db.storage.record.page.RecordPageDeleterTest" --console=plain
```
Expected: 编译失败（`RecordPageDeleter`、`RecordPage.setDeleted` 不存在）。

- [ ] **Step 3a: Add IndexPageLayout.REC_FLAGS_FIELD_OFFSET**

在 `IndexPageLayout.java` 的 `REC_HEAPNO_FIELD_OFFSET` 之前插入：
```java
    /** 记录头内 flags 字节偏移（u8；= record.format.RecordHeaderLayout.FLAGS；bit0 deleted）。 */
    static final int REC_FLAGS_FIELD_OFFSET = 0;
```

- [ ] **Step 3b: Add RecordPage.setDeleted**

在 `RecordPage.java` 的 `setHeapNo` 之后插入：
```java
    /**
     * 置/清某记录的 delete-mark（要求 X）。**只读-改-写 flags 字节的 bit0**，保留 bit1（min-rec）与 bit2-3（recordType）——
     * 不能用 {@link #writeRecordBytes}（会覆盖整头）。delete-mark 保留记录在 next_record 链中（供历史版本/后续 purge）。
     */
    public void setDeleted(int offset, boolean deleted) {
        int flags = guard.readBytes(offset + IndexPageLayout.REC_FLAGS_FIELD_OFFSET, 1)[0] & 0xFF;
        flags = deleted ? (flags | 0x01) : (flags & ~0x01);
        guard.writeBytes(offset + IndexPageLayout.REC_FLAGS_FIELD_OFFSET, new byte[]{(byte) flags});
    }
```

- [ ] **Step 3c: Create RecordPageDeleter**

```java
package cn.zhangyis.db.storage.record.page;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.record.format.RecordHeader;
import cn.zhangyis.db.storage.record.format.RecordType;

/**
 * 页内 delete-mark 算子（innodb-record-design §10.3 delete-mark 子集）。逻辑删除：只把记录头 deleted 位置 1，
 * 记录仍留在 next_record 链与 PageDirectory 中（nRecs 含 delete-marked，§7）；物理摘除/空间回收归 {@link RecordPagePurger}。
 *
 * <p>简化（trx/MVCC 暂停）：不写 undo、不更新隐藏列（DB_TRX_ID/DB_ROLL_PTR 未实现）。无状态、线程安全；要求调用方持页 X latch。
 */
public final class RecordPageDeleter {

    /**
     * 对 {@code recordOffset} 处的用户记录置 delete-mark（要求 X）。
     *
     * @throws DatabaseValidationException 目标为 infimum/supremum 系统记录，或已被 delete-mark（强制 lifecycle，避免重复删除）。
     */
    public void deleteMark(RecordPage page, int recordOffset) {
        RecordHeader header = page.recordHeaderAt(recordOffset);
        RecordType type = header.recordType();
        if (type == RecordType.INFIMUM || type == RecordType.SUPREMUM) {
            throw new DatabaseValidationException("cannot delete-mark system record at offset " + recordOffset);
        }
        if (header.deletedFlag()) {
            throw new DatabaseValidationException("record already delete-marked at offset " + recordOffset);
        }
        page.setDeleted(recordOffset, true);
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run:
```
$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "cn.zhangyis.db.storage.record.page.RecordPageDeleterTest" --console=plain
```
Expected: PASS（2 tests）。

- [ ] **Step 5: Checkpoint — full record-layer suite**

Run the Checkpoint 命令（`--tests "cn.zhangyis.db.storage.record.*"`）。Expected: BUILD SUCCESSFUL，无回归。

---

## Task 2 (R5b-1)：RecordPage.findPredecessor

**Files:**
- Modify: `src/main/java/cn/zhangyis/db/storage/record/page/RecordPage.java`
- Test: `src/test/java/cn/zhangyis/db/storage/record/page/RecordPageFindPredecessorTest.java`

- [ ] **Step 1: Write the failing test**

```java
package cn.zhangyis.db.storage.record.page;

import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.fil.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.PageStore;
import cn.zhangyis.db.storage.record.format.LogicalRecord;
import cn.zhangyis.db.storage.record.format.RecordType;
import cn.zhangyis.db.storage.record.schema.ColumnDef;
import cn.zhangyis.db.storage.record.schema.ColumnId;
import cn.zhangyis.db.storage.record.schema.ColumnType;
import cn.zhangyis.db.storage.record.schema.IndexKeyDef;
import cn.zhangyis.db.storage.record.schema.KeyOrder;
import cn.zhangyis.db.storage.record.schema.KeyPartDef;
import cn.zhangyis.db.storage.record.schema.TableSchema;
import cn.zhangyis.db.storage.record.type.ColumnValue;
import cn.zhangyis.db.storage.record.type.TypeCodecRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** RecordPage.findPredecessor：首条用户记录前驱为 infimum；中间记录前驱正确；不在链中抛损坏异常。 */
class RecordPageFindPredecessorTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);
    private static final PageId PAGE = PageId.of(SPACE, PageNo.of(3));

    @TempDir
    Path dir;

    private final TypeCodecRegistry registry = new TypeCodecRegistry();
    private final RecordPageInserter inserter = new RecordPageInserter(registry);

    private static TableSchema schema() {
        return new TableSchema(1, List.of(
                new ColumnDef(new ColumnId(0), "id", ColumnType.intType(false, false), 0),
                new ColumnDef(new ColumnId(1), "name", ColumnType.varchar(20, true), 1)));
    }

    private static IndexKeyDef idKey() {
        return new IndexKeyDef(7L, List.of(new KeyPartDef(new ColumnId(0), KeyOrder.ASC, 0)));
    }

    private static LogicalRecord row(long id, String name) {
        return new LogicalRecord(1, List.of(new ColumnValue.IntValue(id), new ColumnValue.StringValue(name)),
                false, RecordType.CONVENTIONAL);
    }

    @Test
    void findsPredecessorAlongChain() {
        TableSchema schema = schema();
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(4));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 4)) {
            try (PageGuard g = pool.getPage(PAGE, PageLatchMode.EXCLUSIVE)) {
                RecordPage rp = new RecordPage(g, PS);
                rp.format(7, 0);
                IndexKeyDef kd = idKey();
                int off10 = inserter.insert(rp, PAGE, row(10, "n10"), kd, schema).pageOffset();
                int off20 = inserter.insert(rp, PAGE, row(20, "n20"), kd, schema).pageOffset();
                int off30 = inserter.insert(rp, PAGE, row(30, "n30"), kd, schema).pageOffset();

                assertEquals(rp.infimumOffset(), rp.findPredecessor(off10), "first record -> infimum");
                assertEquals(off10, rp.findPredecessor(off20));
                assertEquals(off20, rp.findPredecessor(off30));
                // 不在链中的偏移（用 supremum 之外的伪值：取 off30 的 next 不可，构造一个明显不存在的 heap 区偏移）。
                assertThrows(PageDirectoryCorruptedException.class, () -> rp.findPredecessor(off30 + 1));
            }
        }
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:
```
$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "cn.zhangyis.db.storage.record.page.RecordPageFindPredecessorTest" --console=plain
```
Expected: 编译失败（`findPredecessor` 不存在）。

- [ ] **Step 3: Implement RecordPage.findPredecessor**

在 `RecordPage.java` 的 `nextRecord`/`setNextRecord` 之后插入：
```java
    /**
     * 从 infimum 沿 next_record 找到 {@code next == offset} 的记录偏移（即 offset 的链上前驱）。offset 为首条用户记录时返回
     * {@link #infimumOffset()}。供 purge / update 搬迁定位并重写前驱链。
     *
     * <p>守卫：步数超过 nHeap 判成环、遍历到 supremum 仍未命中判 offset 不在链中，均抛 {@link PageDirectoryCorruptedException}
     * （不静默修复）。要求 offset 为链中的用户记录；不接受 infimum/supremum 作为 offset。
     */
    public int findPredecessor(int offset) {
        int maxSteps = PageU16.get(guard, IndexPageHeaderLayout.N_HEAP);
        int cur = infimumOffset();
        int supremum = supremumOffset();
        int steps = 0;
        while (true) {
            int next = nextRecord(cur);
            if (next == offset) {
                return cur;
            }
            if (next == supremum) {
                throw new PageDirectoryCorruptedException("predecessor not found; offset not in chain: " + offset);
            }
            if (steps++ > maxSteps) {
                throw new PageDirectoryCorruptedException("next_record chain cycle while finding predecessor of " + offset);
            }
            cur = next;
        }
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run the same `--tests RecordPageFindPredecessorTest` 命令。Expected: PASS（1 test）。

- [ ] **Step 5: Checkpoint** — full record-layer suite。Expected: BUILD SUCCESSFUL。

---

## Task 3 (R5b-2)：HeapSpaceManager + IndexPageHeader.with* + RecordPageDirectory.indexOf

**Files:**
- Modify: `src/main/java/cn/zhangyis/db/storage/record/page/IndexPageHeader.java`
- Modify: `src/main/java/cn/zhangyis/db/storage/record/page/RecordPageDirectory.java`
- Create: `src/main/java/cn/zhangyis/db/storage/record/page/HeapSpaceManager.java`
- Test: `src/test/java/cn/zhangyis/db/storage/record/page/HeapSpaceManagerTest.java`

- [ ] **Step 1: Write the failing test**

```java
package cn.zhangyis.db.storage.record.page;

import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.fil.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.PageStore;
import cn.zhangyis.db.storage.record.format.LogicalRecord;
import cn.zhangyis.db.storage.record.format.RecordEncoder;
import cn.zhangyis.db.storage.record.format.RecordType;
import cn.zhangyis.db.storage.record.schema.ColumnDef;
import cn.zhangyis.db.storage.record.schema.ColumnId;
import cn.zhangyis.db.storage.record.schema.ColumnType;
import cn.zhangyis.db.storage.record.schema.TableSchema;
import cn.zhangyis.db.storage.record.type.ColumnValue;
import cn.zhangyis.db.storage.record.type.TypeCodecRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** HeapSpaceManager：free 入 GarbageList（GARBAGE+）、allocate first-fit 复用（沿用 heapNo、GARBAGE-整块）、容量不足回退 FreeSpace。 */
class HeapSpaceManagerTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);
    private static final PageId PAGE = PageId.of(SPACE, PageNo.of(3));

    @TempDir
    Path dir;

    private final TypeCodecRegistry registry = new TypeCodecRegistry();

    private static TableSchema schema() {
        return new TableSchema(1, List.of(
                new ColumnDef(new ColumnId(0), "id", ColumnType.intType(false, false), 0),
                new ColumnDef(new ColumnId(1), "name", ColumnType.varchar(50, true), 1)));
    }

    /** 直接在 heap 切一条编码记录（不串入用户链），返回 offset；模拟一块可被 free 的记录区。 */
    private int place(RecordPage rp, TableSchema schema, long id, String name) {
        LogicalRecord logical = new LogicalRecord(1, List.of(
                new ColumnValue.IntValue(id), new ColumnValue.StringValue(name)), false, RecordType.CONVENTIONAL);
        byte[] bytes = new RecordEncoder(registry).encode(logical, schema);
        int heapNo = rp.nextHeapNo();
        int off = rp.allocateFromFreeSpace(bytes.length);
        rp.writeRecordBytes(off, bytes);
        rp.setHeapNo(off, heapNo);
        return off;
    }

    private interface PageBody { void run(RecordPage rp, TableSchema schema); }

    private void onPage(PageBody body) {
        TableSchema schema = schema();
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(4));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 4)) {
            try (PageGuard g = pool.getPage(PAGE, PageLatchMode.EXCLUSIVE)) {
                RecordPage rp = new RecordPage(g, PS);
                rp.format(7, 0);
                body.run(rp, schema);
            }
        }
    }

    @Test
    void freeThenAllocateReusesFragment() {
        onPage((rp, schema) -> {
            int off = place(rp, schema, 1, "hello-world-payload");
            int heapNo = rp.recordHeaderAt(off).heapNo();
            int cap = rp.recordHeaderAt(off).recordLength();
            HeapSpaceManager heap = new HeapSpaceManager(rp);

            heap.free(off);
            assertEquals(cap, rp.header().garbage(), "garbage += capacity");
            assertEquals(off, rp.header().free(), "free head = fragment");

            HeapSpaceManager.Allocation a = heap.allocate(cap);
            assertTrue(a.reused(), "reused fragment");
            assertEquals(off, a.offset());
            assertEquals(heapNo, a.heapNo(), "reuses fragment heapNo");
            assertEquals(0, rp.header().garbage(), "garbage -= full capacity");
            assertEquals(0, rp.header().free(), "free list empty");
        });
    }

    @Test
    void allocateFallsBackToFreeSpaceWhenFragmentTooSmall() {
        onPage((rp, schema) -> {
            int off = place(rp, schema, 1, "tiny");
            int cap = rp.recordHeaderAt(off).recordLength();
            HeapSpaceManager heap = new HeapSpaceManager(rp);
            heap.free(off);
            int garbageBefore = rp.header().garbage();
            int heapTopBefore = rp.header().heapTop();

            HeapSpaceManager.Allocation a = heap.allocate(cap + 100);
            assertFalse(a.reused(), "fragment too small -> FreeSpace");
            assertTrue(a.offset() >= heapTopBefore, "carved from FreeSpace (>= old heapTop)");
            assertEquals(garbageBefore, rp.header().garbage(), "garbage unchanged on fallback");
        });
    }

    @Test
    void firstFitPicksFirstFittingFragment() {
        onPage((rp, schema) -> {
            int small = place(rp, schema, 1, "s");          // 容量较小
            int big = place(rp, schema, 2, "big-payload-here-x"); // 容量较大
            int bigCap = rp.recordHeaderAt(big).recordLength();
            HeapSpaceManager heap = new HeapSpaceManager(rp);
            heap.free(small); // FREE head = small
            heap.free(big);   // FREE head = big -> small

            // 需要 bigCap 字节：small 容量不足被跳过，命中 big。
            HeapSpaceManager.Allocation a = heap.allocate(bigCap);
            assertTrue(a.reused());
            assertEquals(big, a.offset(), "first-fit skips too-small head, picks big");
            // small 仍在 FREE 链上（big 被摘除后 FREE head 应回到 small）。
            assertEquals(small, rp.header().free());
        });
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run `--tests HeapSpaceManagerTest`。Expected: 编译失败（`HeapSpaceManager`、`IndexPageHeader.withFree/withGarbage` 不存在）。

- [ ] **Step 3a: Add IndexPageHeader copy helpers**

在 `IndexPageHeader.java` 的 `readFrom` 之后插入：
```java
    /** 返回仅 {@code free}（GarbageList 头）不同的副本（其余字段不变）。供 HeapSpaceManager 做 header RMW。 */
    public IndexPageHeader withFree(int newFree) {
        return new IndexPageHeader(nDirSlots, heapTop, nHeap, newFree, garbage, lastInsert, direction,
                nDirection, nRecs, level, indexId);
    }

    /** 返回仅 {@code garbage}（已跟踪垃圾字节数）不同的副本。 */
    public IndexPageHeader withGarbage(int newGarbage) {
        return new IndexPageHeader(nDirSlots, heapTop, nHeap, free, newGarbage, lastInsert, direction,
                nDirection, nRecs, level, indexId);
    }

    /** 返回仅 {@code nRecs}（用户记录数，含 delete-marked）不同的副本。供 purge/reorganize 维护计数。 */
    public IndexPageHeader withNRecs(int newNRecs) {
        return new IndexPageHeader(nDirSlots, heapTop, nHeap, free, garbage, lastInsert, direction,
                nDirection, newNRecs, level, indexId);
    }
```

- [ ] **Step 3b: Add RecordPageDirectory.indexOf**

在 `RecordPageDirectory.java` 的 `slot` 之后插入：
```java
    /** 线扫返回指向 {@code recordOffset} 的槽下标；未找到返回 -1。供 purge/update 定位 owner 的槽。 */
    public int indexOf(int recordOffset) {
        int n = slotCount();
        for (int i = 0; i < n; i++) {
            if (slot(i) == recordOffset) {
                return i;
            }
        }
        return -1;
    }
```

- [ ] **Step 3c: Create HeapSpaceManager**

```java
package cn.zhangyis.db.storage.record.page;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.record.format.RecordHeader;

/**
 * 页内 heap 空间分配/回收策略（innodb-record-design §7 GarbageList + §10.1 step4，Strategy）。绑定单页、单 X latch 内使用。
 *
 * <p>GarbageList：header {@code FREE}=空闲碎片链头（0=空），各碎片用 {@code next_record} 字段串接（碎片已离开用户链，
 * 复用该字段安全），碎片容量取其保留的 {@link RecordHeader#recordLength()}。{@code GARBAGE}=已跟踪垃圾字节数
 * （= free 累加整条 − 复用扣减整条 + 原地缩短累加；非物理总死空间，oversized 复用余量不计入，见 spec §3.2）。
 *
 * <p>分配 first-fit + 整块消费：{@link #allocate} 先扫 FREE 链找首个容量≥需求的碎片复用（沿用其 heapNo、nHeap 不变、
 * GARBAGE 扣整块），余量作未跟踪内部碎片留 reorganize 回收；找不到回退 {@link RecordPage#allocateFromFreeSpace}（新 heapNo）。
 */
public final class HeapSpaceManager {

    /** 分配结果：记录落点偏移、其 heapNo、是否复用 garbage 碎片。 */
    public record Allocation(int offset, int heapNo, boolean reused) {
    }

    private final RecordPage page;

    public HeapSpaceManager(RecordPage page) {
        if (page == null) {
            throw new DatabaseValidationException("heap space manager page must not be null");
        }
        this.page = page;
    }

    /**
     * 为一条 {@code neededBytes} 的新记录分配空间（要求 X）。first-fit 复用 GarbageList，否则回退 FreeSpace。
     *
     * @throws RecordPageOverflowException FreeSpace 不足（由 {@link RecordPage#allocateFromFreeSpace} 抛出）。
     * @throws PageDirectoryCorruptedException FREE 链成环。
     */
    public Allocation allocate(int neededBytes) {
        int maxSteps = page.header().nHeap();
        int prevFrag = 0; // 0 表示当前考察的是 FREE 链头本身
        int frag = page.header().free();
        int steps = 0;
        while (frag != 0) {
            if (steps++ > maxSteps) {
                throw new PageDirectoryCorruptedException("garbage free-list cycle detected");
            }
            RecordHeader fh = page.recordHeaderAt(frag);
            int cap = fh.recordLength();
            int nextFrag = page.nextRecord(frag);
            if (cap >= neededBytes) {
                // 从 FREE 链摘除 frag：头则改 FREE 字段，非头则改前驱碎片的 next。
                if (prevFrag == 0) {
                    page.writeHeader(page.header().withFree(nextFrag));
                } else {
                    page.setNextRecord(prevFrag, nextFrag);
                }
                page.writeHeader(page.header().withGarbage(page.header().garbage() - cap));
                return new Allocation(frag, fh.heapNo(), true);
            }
            prevFrag = frag;
            frag = nextFrag;
        }
        int heapNo = page.nextHeapNo();
        int offset = page.allocateFromFreeSpace(neededBytes);
        return new Allocation(offset, heapNo, false);
    }

    /**
     * 把 {@code offset} 记录占用的空间压入 GarbageList 头（要求 X）：{@code next_record(offset)=旧 FREE}、{@code FREE=offset}、
     * {@code GARBAGE += recordLength}。调用方须保证该记录已离开用户 next_record 链（purge/update 搬迁先摘链）。
     */
    public void free(int offset) {
        int cap = page.recordHeaderAt(offset).recordLength();
        int oldHead = page.header().free();
        page.setNextRecord(offset, oldHead);
        page.writeHeader(page.header().withFree(offset).withGarbage(page.header().garbage() + cap));
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run `--tests HeapSpaceManagerTest`。Expected: PASS（3 tests）。

- [ ] **Step 5: Checkpoint** — full record-layer suite。Expected: BUILD SUCCESSFUL。

---

## Task 4 (R5b-3)：RecordPageInserter 改走 HeapSpaceManager

**Files:**
- Modify: `src/main/java/cn/zhangyis/db/storage/record/page/RecordPageInserter.java:~90-95`（step4 分配处）
- Test: `src/test/java/cn/zhangyis/db/storage/record/page/RecordPageInserterTest.java`（增 1 用例）

- [ ] **Step 1: Run impact analysis (前置)**

Run（MCP）：`gitnexus_impact({ target: "RecordPageInserter", direction: "upstream", repo: "dbtest" })`，并对 `RecordPage` 同样跑一次。报告 blast radius；预期 LOW（仅测试调用）。若 HIGH/CRITICAL 先暂停报告。

- [ ] **Step 2: Write the failing test (新增复用用例)**

在 `RecordPageInserterTest.java` 末尾（最后一个 `}` 前）追加：
```java
    @Test
    void insertReusesFreedGarbageFragment() {
        onPage(schema(), (rp, schema) -> {
            IndexKeyDef kd = idKey();
            // 在 heap 切一块"碎片"（不串入用户链），再 free 入 GarbageList。
            LogicalRecord throwaway = row(999, "a-reusable-fragment-x");
            byte[] bytes = new cn.zhangyis.db.storage.record.format.RecordEncoder(registry).encode(throwaway, schema);
            int fragHeapNo = rp.nextHeapNo();
            int frag = rp.allocateFromFreeSpace(bytes.length);
            rp.writeRecordBytes(frag, bytes);
            rp.setHeapNo(frag, fragHeapNo);
            new HeapSpaceManager(rp).free(frag);

            // 插入一条 ≤ 碎片容量的记录：应复用 frag 偏移与其 heapNo。
            RecordRef ref = inserter.insert(rp, PAGE, row(5, "n5"), kd, schema);
            assertEquals(frag, ref.pageOffset(), "insert reuses freed fragment offset");
            assertEquals(fragHeapNo, ref.heapNo(), "insert reuses freed fragment heapNo");
            assertEquals(0, rp.header().garbage(), "garbage reclaimed");
            assertTrue(search.findEqual(rp, kId(5), kd, schema).isPresent(), "reused record findable");
        });
    }
```
（`RecordPageInserterTest` 已 import `assertEquals`/`assertTrue`、`RecordRef`、`LogicalRecord`、`IndexKeyDef`、`row`、`kId`、`onPage`、`schema`、`registry`、`PAGE`。仅 `RecordEncoder` 用全限定名，无需新 import。）

- [ ] **Step 3: Run test to verify it fails**

Run `--tests "cn.zhangyis.db.storage.record.page.RecordPageInserterTest"`。Expected: `insertReusesFreedGarbageFragment` FAIL（当前 inserter 用 `allocateFromFreeSpace`，不复用 → `ref.pageOffset()!=frag`）。其余既有用例仍 PASS。

- [ ] **Step 4: Refactor inserter allocation**

在 `RecordPageInserter.java` 的 `insert` 中，将：
```java
        // 4. 分配 heap 空间：页满在此抛 overflow，此前未做任何修改，失败是整体干净的（无部分链入）。
        int heapNo = page.nextHeapNo();
        int off = page.allocateFromFreeSpace(bytes.length);
```
替换为：
```java
        // 4. 分配 heap 空间：优先复用 GarbageList 碎片（first-fit），否则 FreeSpace；页满在此抛 overflow，
        //    此前未做任何修改，失败是整体干净的（无部分链入）。复用碎片沿用其 heapNo，物理落点与链序解耦（链=key 序）。
        HeapSpaceManager.Allocation alloc = new HeapSpaceManager(page).allocate(bytes.length);
        int heapNo = alloc.heapNo();
        int off = alloc.offset();
```
（其后 `page.writeRecordBytes(off, bytes); page.setHeapNo(off, heapNo);` 等不变。）

- [ ] **Step 5: Run tests to verify pass + regression**

Run `--tests "cn.zhangyis.db.storage.record.page.RecordPageInserterTest"`。Expected: 全 PASS（含新用例 + R4 原 5 用例回归）。

- [ ] **Step 6: Checkpoint** — full record-layer suite。Expected: BUILD SUCCESSFUL。

---

## Task 5 (R5c)：purge — RecordPagePurger

**Files:**
- Create: `src/main/java/cn/zhangyis/db/storage/record/page/RecordPagePurger.java`
- Test: `src/test/java/cn/zhangyis/db/storage/record/page/RecordPagePurgerTest.java`

- [ ] **Step 1: Write the failing test**

```java
package cn.zhangyis.db.storage.record.page;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.fil.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.PageStore;
import cn.zhangyis.db.storage.record.format.LogicalRecord;
import cn.zhangyis.db.storage.record.format.RecordType;
import cn.zhangyis.db.storage.record.schema.ColumnDef;
import cn.zhangyis.db.storage.record.schema.ColumnId;
import cn.zhangyis.db.storage.record.schema.ColumnType;
import cn.zhangyis.db.storage.record.schema.IndexKeyDef;
import cn.zhangyis.db.storage.record.schema.KeyOrder;
import cn.zhangyis.db.storage.record.schema.KeyPartDef;
import cn.zhangyis.db.storage.record.schema.TableSchema;
import cn.zhangyis.db.storage.record.type.ColumnValue;
import cn.zhangyis.db.storage.record.type.TypeCodecRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** RecordPagePurger：摘链 + n_owned/目录维护 + 组合并 + 空间回收；前置校验；purge 后空间被复用。 */
class RecordPagePurgerTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);
    private static final PageId PAGE = PageId.of(SPACE, PageNo.of(3));

    @TempDir
    Path dir;

    private final TypeCodecRegistry registry = new TypeCodecRegistry();
    private final RecordPageInserter inserter = new RecordPageInserter(registry);
    private final RecordPageSearch search = new RecordPageSearch(registry);
    private final RecordPageDeleter deleter = new RecordPageDeleter();
    private final RecordPagePurger purger = new RecordPagePurger();

    private static TableSchema schema() {
        return new TableSchema(1, List.of(
                new ColumnDef(new ColumnId(0), "id", ColumnType.intType(false, false), 0),
                new ColumnDef(new ColumnId(1), "name", ColumnType.varchar(20, true), 1)));
    }

    private static IndexKeyDef idKey() {
        return new IndexKeyDef(7L, List.of(new KeyPartDef(new ColumnId(0), KeyOrder.ASC, 0)));
    }

    private static SearchKey k(long id) {
        return new SearchKey(List.of(new ColumnValue.IntValue(id)));
    }

    private static LogicalRecord row(long id, String name) {
        return new LogicalRecord(1, List.of(new ColumnValue.IntValue(id), new ColumnValue.StringValue(name)),
                false, RecordType.CONVENTIONAL);
    }

    private interface PageBody { void run(RecordPage rp, TableSchema schema); }

    private void onPage(PageBody body) {
        TableSchema schema = schema();
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(4));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 4)) {
            try (PageGuard g = pool.getPage(PAGE, PageLatchMode.EXCLUSIVE)) {
                RecordPage rp = new RecordPage(g, PS);
                rp.format(7, 0);
                body.run(rp, schema);
            }
        }
    }

    @Test
    void purgeInteriorRecordUnlinksAndReclaims() {
        onPage((rp, schema) -> {
            IndexKeyDef kd = idKey();
            inserter.insert(rp, PAGE, row(10, "n10"), kd, schema);
            int off = inserter.insert(rp, PAGE, row(20, "n20"), kd, schema).pageOffset();
            inserter.insert(rp, PAGE, row(30, "n30"), kd, schema);
            int len = rp.recordHeaderAt(off).recordLength();
            int nRecsBefore = rp.header().nRecs();

            deleter.deleteMark(rp, off);
            purger.purge(rp, off);

            assertFalse(rp.recordOffsetsInOrder().contains(off), "unlinked from chain");
            assertTrue(search.findEqual(rp, k(20), kd, schema).isEmpty(), "no longer found");
            assertTrue(search.findEqual(rp, k(10), kd, schema).isPresent(), "others intact");
            assertTrue(search.findEqual(rp, k(30), kd, schema).isPresent());
            assertEquals(nRecsBefore - 1, rp.header().nRecs(), "nRecs--");
            assertEquals(len, rp.header().garbage(), "space into GarbageList");
        });
    }

    @Test
    void purgedSpaceIsReusedByInsert() {
        onPage((rp, schema) -> {
            IndexKeyDef kd = idKey();
            inserter.insert(rp, PAGE, row(10, "n10"), kd, schema);
            int off = inserter.insert(rp, PAGE, row(20, "same-length-name-x"), kd, schema).pageOffset();
            deleter.deleteMark(rp, off);
            purger.purge(rp, off);

            // 插入一条 ≤ 被回收容量的记录 → 复用该偏移。
            RecordRef ref = inserter.insert(rp, PAGE, row(25, "n25"), kd, schema);
            assertEquals(off, ref.pageOffset(), "reuses purged fragment");
            assertEquals(0, rp.header().garbage(), "garbage reclaimed on reuse");
        });
    }

    @Test
    void purgeGroupEndRepointsSlot() {
        onPage((rp, schema) -> {
            IndexKeyDef kd = idKey();
            // 插 8 条触发一次 split：产生一个中间槽（组末记录 = 第 4 条 id=4）。
            for (int i = 1; i <= 8; i++) {
                inserter.insert(rp, PAGE, row(i, "n" + i), kd, schema);
            }
            int slotCountBefore = rp.directory().slotCount();
            assertTrue(slotCountBefore >= 3, "split produced a middle slot");
            // 找到中间槽 owner（slot(1)）记录并 purge 它（owner==target 路径）。
            int owner = rp.directory().slot(1);
            deleter.deleteMark(rp, owner);
            purger.purge(rp, owner);

            assertFalse(rp.recordOffsetsInOrder().contains(owner), "owner unlinked");
            assertEquals(7, rp.recordOffsetsInOrder().size(), "one record removed");
            // 目录仍自洽：slot(0)=infimum 仍 owns 1，中间/尾组 n_owned 合法。
            RecordPageDirectory d = rp.directory();
            assertEquals(1, rp.recordHeaderAt(d.slot(0)).nOwned());
            for (int i = 1; i < d.slotCount() - 1; i++) {
                int owned = rp.recordHeaderAt(d.slot(i)).nOwned();
                assertTrue(owned >= 1 && owned <= RecordPageInserter.MAX_N_OWNED, "slot " + i + " owned=" + owned);
            }
            // 链按 key 升序未乱（逐 key 存在性见 purgeManyKeepsInvariantsAndMerges）。
            long prev = Long.MIN_VALUE;
            for (int off : rp.recordOffsetsInOrder()) {
                long id = ((ColumnValue.IntValue) new RecordCursor(rp, off, schema, registry)
                        .readColumn(new ColumnId(0))).value();
                assertTrue(id > prev, "ascending after purge");
                prev = id;
            }
        });
    }

    @Test
    void purgeManyKeepsInvariantsAndMerges() {
        onPage((rp, schema) -> {
            IndexKeyDef kd = idKey();
            for (int i = 1; i <= 20; i++) {
                inserter.insert(rp, PAGE, row(i, "n" + i), kd, schema);
            }
            // delete-mark + purge 偶数 id（约一半），逼组变小触发合并。
            for (int i = 2; i <= 20; i += 2) {
                int off = search.findEqual(rp, k(i), kd, schema).getAsInt();
                deleter.deleteMark(rp, off);
                purger.purge(rp, off);
            }
            // 奇数仍在、偶数已无。
            for (int i = 1; i <= 20; i++) {
                boolean present = search.findEqual(rp, k(i), kd, schema).isPresent();
                assertEquals(i % 2 == 1, present, "id " + i);
            }
            assertEquals(10, rp.header().nRecs());
            // 目录不变量。
            RecordPageDirectory d = rp.directory();
            assertEquals(1, rp.recordHeaderAt(d.slot(0)).nOwned(), "infimum owns 1");
            for (int i = 1; i < d.slotCount() - 1; i++) {
                int owned = rp.recordHeaderAt(d.slot(i)).nOwned();
                assertTrue(owned >= 1 && owned <= RecordPageInserter.MAX_N_OWNED, "slot " + i + " owned=" + owned);
            }
            int sup = rp.recordHeaderAt(d.slot(d.slotCount() - 1)).nOwned();
            assertTrue(sup >= 1 && sup <= RecordPageInserter.MAX_N_OWNED);
        });
    }

    @Test
    void rejectsNonDeletedAndSystemRecord() {
        onPage((rp, schema) -> {
            IndexKeyDef kd = idKey();
            int off = inserter.insert(rp, PAGE, row(1, "a"), kd, schema).pageOffset();
            assertThrows(DatabaseValidationException.class, () -> purger.purge(rp, off), "not delete-marked");
            assertThrows(DatabaseValidationException.class,
                    () -> purger.purge(rp, rp.supremumOffset()), "system record");
        });
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run `--tests RecordPagePurgerTest`。Expected: 编译失败（`RecordPagePurger` 不存在）。

- [ ] **Step 3: Create RecordPagePurger**

```java
package cn.zhangyis.db.storage.record.page;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.record.format.RecordHeader;
import cn.zhangyis.db.storage.record.format.RecordType;

/**
 * 页内 purge 算子（innodb-record-design §10.3 purge 子集）：把已 delete-marked 的记录从 next_record 链物理摘除，
 * 维护 group 的 {@code n_owned} 与 PageDirectory（组末记录被删则改槽/删槽，组过小则与后一组合并），把空间挂回 GarbageList。
 *
 * <p>简化（trx/MVCC 暂停）：无 purge view 安全门（§10.3 step1）——本片要求调用方已确保安全，仅校验目标已 delete-marked；
 * 不写 undo。组合并只在「与后一组合并后 ≤MAX」时进行，否则留小组（不 borrow 再分配）。无状态、线程安全；要求页 X latch。
 *
 * <p>**plan-then-execute**：先把校验 + 前驱/owner/槽下标 + 合并判定全部算好（任何失败在写页前抛出），再连续写页，
 * 避免「摘链后定位槽失败」留下半改页。
 */
public final class RecordPagePurger {

    /**
     * 物理 purge {@code recordOffset} 处记录（要求 X）。
     *
     * @throws DatabaseValidationException 目标为系统记录或未 delete-marked。
     * @throws PageDirectoryCorruptedException 前驱/owner 槽定位失败（页损坏）。
     */
    public void purge(RecordPage page, int recordOffset) {
        // ---------- plan：只读 + 校验 ----------
        RecordHeader target = page.recordHeaderAt(recordOffset);
        RecordType type = target.recordType();
        if (type == RecordType.INFIMUM || type == RecordType.SUPREMUM) {
            throw new DatabaseValidationException("cannot purge system record at offset " + recordOffset);
        }
        if (!target.deletedFlag()) {
            throw new DatabaseValidationException("can only purge delete-marked record at offset " + recordOffset);
        }
        int prev = page.findPredecessor(recordOffset);
        int targetNext = page.nextRecord(recordOffset);
        // owner = 沿链首个 n_owned>0 的记录（supremum 恒 n_owned≥1，必终止）。
        int owner = recordOffset;
        while (page.recordHeaderAt(owner).nOwned() == 0) {
            owner = page.nextRecord(owner);
        }
        boolean targetIsOwner = (owner == recordOffset);
        RecordPageDirectory dir = page.directory();
        int ownerSlot = dir.indexOf(owner);
        if (ownerSlot < 1) {
            throw new PageDirectoryCorruptedException("owner slot not found for offset " + owner);
        }
        int cnt = page.recordHeaderAt(owner).nOwned();

        // ---------- execute：连续写 ----------
        page.setNextRecord(prev, targetNext); // 摘链

        int affectedOwner;
        int affectedSlot;
        if (targetIsOwner) {
            if (cnt == 1) {
                // 组仅此一员：整槽删除。
                dir.removeSlot(ownerSlot);
                affectedOwner = -1;
                affectedSlot = -1;
            } else {
                // 槽改指链上前一条（新组末，必在本组内），移交 n_owned-1。target 即将被 free，残留 n_owned 不影响。
                dir.setSlot(ownerSlot, prev);
                page.setNOwned(prev, cnt - 1);
                affectedOwner = prev;
                affectedSlot = ownerSlot;
            }
        } else {
            page.setNOwned(owner, cnt - 1);
            affectedOwner = owner;
            affectedSlot = ownerSlot;
        }

        // 组合并：仅中间组（非 supremum 槽），且与后一组合并后 ≤MAX。
        if (affectedOwner != -1 && affectedSlot >= 1 && affectedSlot < dir.slotCount() - 1) {
            int curOwned = page.recordHeaderAt(affectedOwner).nOwned();
            if (curOwned < RecordPageInserter.MIN_N_OWNED) {
                int nextOwner = dir.slot(affectedSlot + 1);
                int sum = curOwned + page.recordHeaderAt(nextOwner).nOwned();
                if (sum <= RecordPageInserter.MAX_N_OWNED) {
                    page.setNOwned(nextOwner, sum);
                    page.setNOwned(affectedOwner, 0); // 旧 owner 降为 interior，必须清零（否则 owner-walk 误判）
                    dir.removeSlot(affectedSlot);
                }
            }
        }

        new HeapSpaceManager(page).free(recordOffset); // 空间挂回 GarbageList、GARBAGE+=len
        page.writeHeader(page.header().withNRecs(page.header().nRecs() - 1));
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run `--tests RecordPagePurgerTest`。Expected: PASS（5 tests，含组末/合并/复用/异常）。若 `purgeGroupEndRepointsSlot` 占位断言报错，按 Step 1 注释简化。

- [ ] **Step 5: Checkpoint** — full record-layer suite。Expected: BUILD SUCCESSFUL。

---

## Task 6 (R5d)：page reorganize — RecordPageReorganizer

**Files:**
- Create: `src/main/java/cn/zhangyis/db/storage/record/page/RecordPageReorganizer.java`
- Test: `src/test/java/cn/zhangyis/db/storage/record/page/RecordPageReorganizerTest.java`

- [ ] **Step 1: Write the failing test**

```java
package cn.zhangyis.db.storage.record.page;

import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.fil.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.PageStore;
import cn.zhangyis.db.storage.record.format.LogicalRecord;
import cn.zhangyis.db.storage.record.format.RecordType;
import cn.zhangyis.db.storage.record.schema.ColumnDef;
import cn.zhangyis.db.storage.record.schema.ColumnId;
import cn.zhangyis.db.storage.record.schema.ColumnType;
import cn.zhangyis.db.storage.record.schema.IndexKeyDef;
import cn.zhangyis.db.storage.record.schema.KeyOrder;
import cn.zhangyis.db.storage.record.schema.KeyPartDef;
import cn.zhangyis.db.storage.record.schema.TableSchema;
import cn.zhangyis.db.storage.record.type.ColumnValue;
import cn.zhangyis.db.storage.record.type.TypeCodecRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** RecordPageReorganizer：回收 garbage、保留 delete-marked、heapNo 稠密、链/目录不变量、key 仍可查。 */
class RecordPageReorganizerTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);
    private static final PageId PAGE = PageId.of(SPACE, PageNo.of(3));

    @TempDir
    Path dir;

    private final TypeCodecRegistry registry = new TypeCodecRegistry();
    private final RecordPageInserter inserter = new RecordPageInserter(registry);
    private final RecordPageSearch search = new RecordPageSearch(registry);
    private final RecordPageDeleter deleter = new RecordPageDeleter();
    private final RecordPagePurger purger = new RecordPagePurger();
    private final RecordPageReorganizer reorganizer = new RecordPageReorganizer();

    private static TableSchema schema() {
        return new TableSchema(1, List.of(
                new ColumnDef(new ColumnId(0), "id", ColumnType.intType(false, false), 0),
                new ColumnDef(new ColumnId(1), "name", ColumnType.varchar(20, true), 1)));
    }

    private static IndexKeyDef idKey() {
        return new IndexKeyDef(7L, List.of(new KeyPartDef(new ColumnId(0), KeyOrder.ASC, 0)));
    }

    private static SearchKey k(long id) {
        return new SearchKey(List.of(new ColumnValue.IntValue(id)));
    }

    private static LogicalRecord row(long id, String name) {
        return new LogicalRecord(1, List.of(new ColumnValue.IntValue(id), new ColumnValue.StringValue(name)),
                false, RecordType.CONVENTIONAL);
    }

    private interface PageBody { void run(RecordPage rp, TableSchema schema); }

    private void onPage(PageBody body) {
        TableSchema schema = schema();
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(4));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 4)) {
            try (PageGuard g = pool.getPage(PAGE, PageLatchMode.EXCLUSIVE)) {
                RecordPage rp = new RecordPage(g, PS);
                rp.format(7, 0);
                body.run(rp, schema);
            }
        }
    }

    private List<Long> idsInOrder(RecordPage rp, TableSchema schema) {
        List<Long> ids = new ArrayList<>();
        for (int off : rp.recordOffsetsInOrder()) {
            ids.add(((ColumnValue.IntValue) new RecordCursor(rp, off, schema, registry)
                    .readColumn(new ColumnId(0))).value());
        }
        return ids;
    }

    @Test
    void reorganizeReclaimsGarbageAndKeepsOrder() {
        onPage((rp, schema) -> {
            IndexKeyDef kd = idKey();
            for (int i = 1; i <= 12; i++) {
                inserter.insert(rp, PAGE, row(i, "name-" + i), kd, schema);
            }
            // purge 3 条产生 garbage。
            for (int i : new int[]{3, 6, 9}) {
                int off = search.findEqual(rp, k(i), kd, schema).getAsInt();
                deleter.deleteMark(rp, off);
                purger.purge(rp, off);
            }
            assertTrue(rp.header().garbage() > 0, "garbage accumulated");
            int freeBefore = rp.freeSpace();

            reorganizer.reorganize(rp);

            assertEquals(0, rp.header().garbage(), "garbage reclaimed");
            assertEquals(0, rp.header().free(), "free list cleared");
            assertTrue(rp.freeSpace() > freeBefore, "compaction grew FreeSpace");
            assertEquals(List.of(1L, 2L, 4L, 5L, 7L, 8L, 10L, 11L, 12L), idsInOrder(rp, schema), "key order kept");
            assertEquals(9, rp.header().nRecs());
            for (int i : new int[]{1, 2, 4, 5, 7, 8, 10, 11, 12}) {
                assertTrue(search.findEqual(rp, k(i), kd, schema).isPresent(), "id " + i);
            }
            // 目录不变量。
            RecordPageDirectory d = rp.directory();
            assertEquals(1, rp.recordHeaderAt(d.slot(0)).nOwned());
            for (int i = 1; i < d.slotCount() - 1; i++) {
                int owned = rp.recordHeaderAt(d.slot(i)).nOwned();
                assertTrue(owned >= RecordPageInserter.MIN_N_OWNED && owned <= RecordPageInserter.MAX_N_OWNED);
            }
        });
    }

    @Test
    void reorganizeKeepsDeleteMarkedRecords() {
        onPage((rp, schema) -> {
            IndexKeyDef kd = idKey();
            for (int i = 1; i <= 5; i++) {
                inserter.insert(rp, PAGE, row(i, "n" + i), kd, schema);
            }
            int off3 = search.findEqual(rp, k(3), kd, schema).getAsInt();
            deleter.deleteMark(rp, off3); // delete-mark 但不 purge

            reorganizer.reorganize(rp);

            assertEquals(List.of(1L, 2L, 3L, 4L, 5L), idsInOrder(rp, schema), "delete-marked kept in chain");
            assertEquals(5, rp.header().nRecs(), "delete-marked counted");
            int newOff3 = search.findEqual(rp, k(3), kd, schema).getAsInt();
            assertTrue(new RecordCursor(rp, newOff3, schema, registry).isDeleted(), "delete flag preserved");
        });
    }

    @Test
    void reorganizeDensifiesHeapNo() {
        onPage((rp, schema) -> {
            IndexKeyDef kd = idKey();
            for (int i = 1; i <= 4; i++) {
                inserter.insert(rp, PAGE, row(i, "n" + i), kd, schema);
            }
            reorganizer.reorganize(rp);
            // 重排后 heapNo 稠密 2,3,4,5（infimum=0,supremum=1）。
            List<Integer> heapNos = new ArrayList<>();
            for (int off : rp.recordOffsetsInOrder()) {
                heapNos.add(rp.recordHeaderAt(off).heapNo());
            }
            assertEquals(List.of(2, 3, 4, 5), heapNos);
        });
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run `--tests RecordPageReorganizerTest`。Expected: 编译失败（`RecordPageReorganizer` 不存在）。

- [ ] **Step 3: Create RecordPageReorganizer**

```java
package cn.zhangyis.db.storage.record.page;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.ArrayList;
import java.util.List;

/**
 * 页内重组算子（innodb-record-design §10.4）：按逻辑 key 顺序把记录稠密重写，回收 GarbageList 与所有内部碎片、
 * 重建 next_record 链与 PageDirectory（含 n_owned），并 **重排 heapNo**（稠密 2..n+1）。
 *
 * <p>语义：保留 delete-marked 记录（仍在 next_record 链中，按 key 序重写并保留其 deleted 位）；已 purge 的记录已离链，
 * 自然丢弃。重排 heapNo 会使所有旧 {@link RecordRef} 失效（本片不提供失效检测，调用方在重组后须重新定位）。
 * 无状态、线程安全；要求页 X latch。
 *
 * <p>目录重建规则（确定性，保证不变量）：每第 {@code MAX_N_OWNED} 条用户记录作一个中间组末（slot），n_owned=MAX；
 * 尾部不足 MAX 的记录归 supremum 组（n_owned=尾数+1）。故中间组恒=MAX∈[MIN..MAX]，supremum 组∈[1..MAX]。
 */
public final class RecordPageReorganizer {

    /**
     * 重组 page（要求 X）。先按链快照（含 delete-marked），format 重置，再稠密重排 + 重建目录。
     */
    public void reorganize(RecordPage page) {
        if (page == null) {
            throw new DatabaseValidationException("reorganize page must not be null");
        }
        IndexPageHeader h0 = page.header();
        long indexId = h0.indexId();
        int level = h0.level();

        // 1. 按链（key）序快照记录字节（含 delete-marked，保留 flags）。
        List<byte[]> records = new ArrayList<>();
        for (int off : page.recordOffsetsInOrder()) {
            records.add(page.readRecordBytes(off));
        }

        // 2. 重置页（infimum/supremum、2 槽、heapTop=98、nHeap=2、nRecs=0、FREE=0、GARBAGE=0）。
        page.format(indexId, level);

        // 3. 稠密重排 + 串链。每条显式 setNOwned(0)（清掉快照里旧布局的 n_owned）。
        int prev = page.infimumOffset();
        List<Integer> offsets = new ArrayList<>(records.size());
        for (byte[] bytes : records) {
            int heapNo = page.nextHeapNo();
            int off2 = page.allocateFromFreeSpace(bytes.length);
            page.writeRecordBytes(off2, bytes);
            page.setHeapNo(off2, heapNo);
            page.setNOwned(off2, 0);
            page.setNextRecord(prev, off2);
            prev = off2;
            offsets.add(off2);
        }
        page.setNextRecord(prev, page.supremumOffset());

        // 4. 重建目录 + n_owned。
        RecordPageDirectory dir = page.directory();
        int count = offsets.size();
        int lastGroupEnd = 0; // 已成中间组的记录数
        for (int i = 0; i < count; i++) {
            if ((i + 1) % RecordPageInserter.MAX_N_OWNED == 0) {
                dir.insertSlot(dir.slotCount() - 1, offsets.get(i)); // 插在 supremum 槽前
                page.setNOwned(offsets.get(i), RecordPageInserter.MAX_N_OWNED);
                lastGroupEnd = i + 1;
            }
        }
        int tail = count - lastGroupEnd; // 末中间组之后归 supremum 组的记录数
        page.setNOwned(page.supremumOffset(), tail + 1);

        // 5. nRecs = 用户记录数（含 delete-marked）。
        page.writeHeader(page.header().withNRecs(count));
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run `--tests RecordPageReorganizerTest`。Expected: PASS（3 tests）。

- [ ] **Step 5: Checkpoint** — full record-layer suite。Expected: BUILD SUCCESSFUL。

---

## Task 7 (R5e)：update — UpdateOutcome + UpdateResult + RecordPageUpdater

**Files:**
- Create: `src/main/java/cn/zhangyis/db/storage/record/page/UpdateOutcome.java`
- Create: `src/main/java/cn/zhangyis/db/storage/record/page/UpdateResult.java`
- Create: `src/main/java/cn/zhangyis/db/storage/record/page/RecordPageUpdater.java`
- Test: `src/test/java/cn/zhangyis/db/storage/record/page/RecordPageUpdaterTest.java`

- [ ] **Step 1: Write the failing test**

```java
package cn.zhangyis.db.storage.record.page;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.fil.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.PageStore;
import cn.zhangyis.db.storage.record.format.LogicalRecord;
import cn.zhangyis.db.storage.record.format.RecordType;
import cn.zhangyis.db.storage.record.schema.ColumnDef;
import cn.zhangyis.db.storage.record.schema.ColumnId;
import cn.zhangyis.db.storage.record.schema.ColumnType;
import cn.zhangyis.db.storage.record.schema.IndexKeyDef;
import cn.zhangyis.db.storage.record.schema.KeyOrder;
import cn.zhangyis.db.storage.record.schema.KeyPartDef;
import cn.zhangyis.db.storage.record.schema.TableSchema;
import cn.zhangyis.db.storage.record.type.ColumnValue;
import cn.zhangyis.db.storage.record.type.TypeCodecRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** RecordPageUpdater：原地（等长/变短）、搬迁（变长）、key 变化信号、overflow、系统/已删拒绝。 */
class RecordPageUpdaterTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);
    private static final PageId PAGE = PageId.of(SPACE, PageNo.of(3));

    @TempDir
    Path dir;

    private final TypeCodecRegistry registry = new TypeCodecRegistry();
    private final RecordPageInserter inserter = new RecordPageInserter(registry);
    private final RecordPageSearch search = new RecordPageSearch(registry);
    private final RecordPageDeleter deleter = new RecordPageDeleter();
    private final RecordPageUpdater updater = new RecordPageUpdater(registry);

    private static TableSchema schema() {
        return new TableSchema(1, List.of(
                new ColumnDef(new ColumnId(0), "id", ColumnType.intType(false, false), 0),
                new ColumnDef(new ColumnId(1), "name", ColumnType.varchar(200, true), 1)));
    }

    private static IndexKeyDef idKey() {
        return new IndexKeyDef(7L, List.of(new KeyPartDef(new ColumnId(0), KeyOrder.ASC, 0)));
    }

    private static SearchKey k(long id) {
        return new SearchKey(List.of(new ColumnValue.IntValue(id)));
    }

    private static LogicalRecord row(long id, String name) {
        return new LogicalRecord(1, List.of(new ColumnValue.IntValue(id), new ColumnValue.StringValue(name)),
                false, RecordType.CONVENTIONAL);
    }

    private interface PageBody { void run(RecordPage rp, TableSchema schema); }

    private void onPage(PageBody body) {
        TableSchema schema = schema();
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(4));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 4)) {
            try (PageGuard g = pool.getPage(PAGE, PageLatchMode.EXCLUSIVE)) {
                RecordPage rp = new RecordPage(g, PS);
                rp.format(7, 0);
                body.run(rp, schema);
            }
        }
    }

    private String name(int n) {
        return "x".repeat(n);
    }

    @Test
    void inPlaceWhenSameOrShorter() {
        onPage((rp, schema) -> {
            IndexKeyDef kd = idKey();
            inserter.insert(rp, PAGE, row(10, name(50)), kd, schema);
            int off = inserter.insert(rp, PAGE, row(20, name(50)), kd, schema).pageOffset();
            inserter.insert(rp, PAGE, row(30, name(50)), kd, schema);

            UpdateResult r = updater.update(rp, PAGE, off, row(20, name(20)), kd, schema); // 变短
            assertEquals(UpdateOutcome.IN_PLACE, r.outcome());
            assertEquals(off, r.newRef().pageOffset(), "stays in place");
            // payload 更新、链完整、可查。
            RecordCursor c = new RecordCursor(rp, off, schema, registry);
            assertEquals(new ColumnValue.StringValue(name(20)), c.readColumn(new ColumnId(1)));
            assertTrue(search.findEqual(rp, k(20), kd, schema).isPresent());
            assertTrue(search.findEqual(rp, k(10), kd, schema).isPresent());
            assertTrue(search.findEqual(rp, k(30), kd, schema).isPresent());
        });
    }

    @Test
    void movesWhenLonger() {
        onPage((rp, schema) -> {
            IndexKeyDef kd = idKey();
            inserter.insert(rp, PAGE, row(10, name(20)), kd, schema);
            int off = inserter.insert(rp, PAGE, row(20, name(20)), kd, schema).pageOffset();
            inserter.insert(rp, PAGE, row(30, name(20)), kd, schema);
            int len = rp.recordHeaderAt(off).recordLength();

            UpdateResult r = updater.update(rp, PAGE, off, row(20, name(150)), kd, schema); // 变长
            assertEquals(UpdateOutcome.MOVED, r.outcome());
            assertNotEquals(off, r.newRef().pageOffset(), "moved to new offset");
            // 新位置可查、payload 正确、旧空间入 garbage、链完整。
            int newOff = search.findEqual(rp, k(20), kd, schema).getAsInt();
            assertEquals(newOff, r.newRef().pageOffset());
            assertEquals(new ColumnValue.StringValue(name(150)),
                    new RecordCursor(rp, newOff, schema, registry).readColumn(new ColumnId(1)));
            assertEquals(len, rp.header().garbage(), "old space -> GarbageList");
            assertEquals(List.of(10L, 20L, 30L), idsInOrder(rp, schema), "chain intact + ordered");
        });
    }

    @Test
    void keyChangeReturnsRequiresReinsertWithoutMutating() {
        onPage((rp, schema) -> {
            IndexKeyDef kd = idKey();
            int off = inserter.insert(rp, PAGE, row(20, name(10)), kd, schema).pageOffset();
            int garbageBefore = rp.header().garbage();

            UpdateResult r = updater.update(rp, PAGE, off, row(25, name(10)), kd, schema); // id 改变
            assertEquals(UpdateOutcome.REQUIRES_REINSERT, r.outcome());
            assertEquals(null, r.newRef());
            // 页未变：旧 key 仍在、新 key 不在、garbage 不变。
            assertTrue(search.findEqual(rp, k(20), kd, schema).isPresent());
            assertTrue(search.findEqual(rp, k(25), kd, schema).isEmpty());
            assertEquals(garbageBefore, rp.header().garbage());
        });
    }

    @Test
    void moveOverflowLeavesPageUnchanged() {
        onPage((rp, schema) -> {
            IndexKeyDef kd = idKey();
            // 填到接近满，再 update 变长触发 overflow。
            int off = inserter.insert(rp, PAGE, row(1, name(100)), kd, schema).pageOffset();
            // 用大 varchar 把剩余空间占满。
            int i = 2;
            try {
                while (true) {
                    inserter.insert(rp, PAGE, row(i++, name(200)), kd, schema);
                }
            } catch (RecordPageOverflowException expected) {
                // 页已满。
            }
            int idsBefore = rp.recordOffsetsInOrder().size();
            int garbageBefore = rp.header().garbage();

            assertThrows(RecordPageOverflowException.class,
                    () -> updater.update(rp, PAGE, off, row(1, name(200)), kd, schema));
            assertEquals(idsBefore, rp.recordOffsetsInOrder().size(), "chain unchanged");
            assertEquals(garbageBefore, rp.header().garbage(), "no mutation on overflow");
        });
    }

    @Test
    void rejectsSystemAndDeleteMarked() {
        onPage((rp, schema) -> {
            IndexKeyDef kd = idKey();
            int off = inserter.insert(rp, PAGE, row(1, name(10)), kd, schema).pageOffset();
            assertThrows(DatabaseValidationException.class,
                    () -> updater.update(rp, PAGE, rp.infimumOffset(), row(1, name(10)), kd, schema));
            deleter.deleteMark(rp, off);
            assertThrows(DatabaseValidationException.class,
                    () -> updater.update(rp, PAGE, off, row(1, name(10)), kd, schema), "delete-marked");
        });
    }

    private List<Long> idsInOrder(RecordPage rp, TableSchema schema) {
        java.util.List<Long> ids = new java.util.ArrayList<>();
        for (int off : rp.recordOffsetsInOrder()) {
            ids.add(((ColumnValue.IntValue) new RecordCursor(rp, off, schema, registry)
                    .readColumn(new ColumnId(0))).value());
        }
        return ids;
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run `--tests RecordPageUpdaterTest`。Expected: 编译失败（`RecordPageUpdater`/`UpdateOutcome`/`UpdateResult` 不存在）。

- [ ] **Step 3a: Create UpdateOutcome**

```java
package cn.zhangyis.db.storage.record.page;

/**
 * update 结果分类（innodb-record-design §10.2）。
 */
public enum UpdateOutcome {
    /** 原地更新：新 encoded 长度 ≤ 旧长度、key 未变，直接覆盖 payload（保留 heapNo/next/n_owned）。 */
    IN_PLACE,
    /** 页内搬迁：新记录变长但页内有空间，迁到新位置并修正前驱链与目录。 */
    MOVED,
    /** key 变化：record 层不跨页重定位，交调用方按 deleteMark→purge→insert 处理（无 B+Tree）。 */
    REQUIRES_REINSERT
}
```

- [ ] **Step 3b: Create UpdateResult**

```java
package cn.zhangyis.db.storage.record.page;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * update 结果值对象。
 *
 * @param outcome 结果分类。
 * @param newRef  更新后记录的定位（IN_PLACE 为原位、MOVED 为新位置）；REQUIRES_REINSERT 时为 null。
 */
public record UpdateResult(UpdateOutcome outcome, RecordRef newRef) {

    public UpdateResult {
        if (outcome == null) {
            throw new DatabaseValidationException("update outcome must not be null");
        }
        if (outcome == UpdateOutcome.REQUIRES_REINSERT && newRef != null) {
            throw new DatabaseValidationException("REQUIRES_REINSERT must carry null ref");
        }
        if (outcome != UpdateOutcome.REQUIRES_REINSERT && newRef == null) {
            throw new DatabaseValidationException(outcome + " must carry a non-null ref");
        }
    }
}
```

- [ ] **Step 3c: Create RecordPageUpdater**

```java
package cn.zhangyis.db.storage.record.page;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.storage.record.format.RecordEncoder;
import cn.zhangyis.db.storage.record.format.RecordHeader;
import cn.zhangyis.db.storage.record.format.RecordType;
import cn.zhangyis.db.storage.record.schema.IndexKeyDef;
import cn.zhangyis.db.storage.record.schema.KeyPartDef;
import cn.zhangyis.db.storage.record.schema.TableSchema;
import cn.zhangyis.db.storage.record.type.ColumnValue;
import cn.zhangyis.db.storage.record.type.TypeCodecRegistry;

import java.util.ArrayList;
import java.util.List;

/**
 * 页内 update 算子（innodb-record-design §10.2）。三模式：原地（新长≤旧长且 key 未变）、页内搬迁（变长且页内有空间）、
 * key 变化（返回 {@link UpdateOutcome#REQUIRES_REINSERT}，交调用方重定位）。
 *
 * <p>简化（trx/MVCC 暂停）：不写 undo、不改隐藏列；只更新存活的普通记录（拒绝系统记录与已 delete-marked 记录，
 * 后者避免 {@link RecordEncoder} 按 {@code newRecord.deleted()}=false 误"复活"删除标记）。无状态、线程安全；要求页 X latch。
 *
 * <p>**plan-then-execute**：校验、key 变化判定、（搬迁时）前驱与 owner 槽下标都在写页前算好；搬迁的第一处写页是
 * {@link HeapSpaceManager#allocate}（页满抛 overflow 时页未被修改）。
 */
public final class RecordPageUpdater {

    private final RecordEncoder encoder;
    private final RecordComparator comparator;
    private final TypeCodecRegistry registry;

    public RecordPageUpdater(TypeCodecRegistry registry) {
        if (registry == null) {
            throw new DatabaseValidationException("type codec registry must not be null");
        }
        this.registry = registry;
        this.encoder = new RecordEncoder(registry);
        this.comparator = new RecordComparator(registry);
    }

    /**
     * 更新 {@code recordOffset} 处记录为 {@code newRecord}（要求 X）。返回 {@link UpdateResult}。
     *
     * @throws DatabaseValidationException 目标为系统记录或已 delete-marked。
     * @throws RecordPageOverflowException 搬迁所需空间不足（页未被修改）。
     * @throws PageDirectoryCorruptedException 搬迁定位前驱/owner 槽失败。
     */
    public UpdateResult update(RecordPage page, PageId pageId, int recordOffset, LogicalRecord newRecord,
                              IndexKeyDef keyDef, TableSchema schema) {
        // ---------- plan：校验 + key 变化判定 ----------
        RecordHeader oldHeader = page.recordHeaderAt(recordOffset);
        RecordType type = oldHeader.recordType();
        if (type == RecordType.INFIMUM || type == RecordType.SUPREMUM) {
            throw new DatabaseValidationException("cannot update system record at offset " + recordOffset);
        }
        if (oldHeader.deletedFlag()) {
            throw new DatabaseValidationException("cannot update delete-marked record at offset " + recordOffset);
        }
        RecordCursor oldCursor = new RecordCursor(page, recordOffset, schema, registry);
        SearchKey newKey = keyOf(newRecord, keyDef);
        if (comparator.compare(oldCursor, newKey, keyDef, schema) != 0) {
            return new UpdateResult(UpdateOutcome.REQUIRES_REINSERT, null);
        }
        byte[] newBytes = encoder.encode(newRecord, schema);
        int newLen = newBytes.length;
        int oldLen = oldHeader.recordLength();
        int oldHeapNo = oldHeader.heapNo();
        int oldNext = oldHeader.nextRecordOffset();
        int oldNOwned = oldHeader.nOwned();

        if (newLen <= oldLen) {
            // ---------- 原地 ----------
            page.writeRecordBytes(recordOffset, newBytes);
            // writeRecordBytes 覆盖整头：恢复 heapNo/next/n_owned（deleted 恒 false，前已拒绝 delete-marked）。
            page.setHeapNo(recordOffset, oldHeapNo);
            page.setNextRecord(recordOffset, oldNext);
            page.setNOwned(recordOffset, oldNOwned);
            if (newLen < oldLen) {
                page.writeHeader(page.header().withGarbage(page.header().garbage() + (oldLen - newLen)));
            }
            return new UpdateResult(UpdateOutcome.IN_PLACE,
                    new RecordRef(pageId, oldHeapNo, recordOffset, schema.schemaVersion(), keyDef.indexId()));
        }

        // ---------- 搬迁（newLen>oldLen）----------
        // plan（只读）：前驱、owner 槽下标。
        int prev = page.findPredecessor(recordOffset);
        RecordPageDirectory dir = page.directory();
        int slotH = -1;
        if (oldNOwned > 0) {
            slotH = dir.indexOf(recordOffset);
            if (slotH < 1) {
                throw new PageDirectoryCorruptedException("owner slot not found for moved record at " + recordOffset);
            }
        }
        // execute：allocate 为第一处写页（overflow 时页未改）。
        HeapSpaceManager.Allocation alloc = new HeapSpaceManager(page).allocate(newLen);
        int newOff = alloc.offset();
        int newHeapNo = alloc.heapNo();
        page.writeRecordBytes(newOff, newBytes);
        page.setHeapNo(newOff, newHeapNo);
        page.setNextRecord(newOff, oldNext);
        page.setNextRecord(prev, newOff);
        if (oldNOwned > 0) {
            dir.setSlot(slotH, newOff);
            page.setNOwned(newOff, oldNOwned);
        }
        new HeapSpaceManager(page).free(recordOffset);
        return new UpdateResult(UpdateOutcome.MOVED,
                new RecordRef(pageId, newHeapNo, newOff, schema.schemaVersion(), keyDef.indexId()));
    }

    /** 按 keyDef 的 part 顺序取 newRecord 的列值组 SearchKey（用于 key 变化检测）。 */
    private SearchKey keyOf(LogicalRecord rec, IndexKeyDef keyDef) {
        List<ColumnValue> vals = new ArrayList<>(keyDef.parts().size());
        for (KeyPartDef part : keyDef.parts()) {
            vals.add(rec.columnValues().get(part.columnId().value()));
        }
        return new SearchKey(vals);
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run `--tests RecordPageUpdaterTest`。Expected: PASS（5 tests）。

> 注：`moveOverflowLeavesPageUnchanged` 依赖 update 搬迁在 `allocate` overflow 前不写页。若该用例因 in-place 复用碎片而意外有空间，可改用更大的 `name(...)` 长度确保确实变长且无碎片可复用。

- [ ] **Step 5: Checkpoint** — full record-layer suite。Expected: BUILD SUCCESSFUL。

---

## Task 8：收口 — 全量测试 + GitNexus

- [ ] **Step 1: Full suite**

Run:
```
$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --console=plain
```
Expected: BUILD SUCCESSFUL（全模块无回归）。

- [ ] **Step 2: 验证新测试计数**

读 `build/test-results/test/*.xml`，确认 `RecordPageDeleterTest`/`RecordPageFindPredecessorTest`/`HeapSpaceManagerTest`/`RecordPagePurgerTest`/`RecordPageReorganizerTest`/`RecordPageUpdaterTest` 均 `failures=0 errors=0 skipped=0` 且 tests>0；`RecordPageInserterTest` 计数 +1。

- [ ] **Step 3: Refresh GitNexus**

Run: `npx gitnexus analyze`。Expected: 索引刷新（node/edge 数增长）。失败则记录原因，不假装已索引。

- [ ] **Step 4: 更新进度记忆**

更新 `C:\Users\李波\.claude\projects\C--coding-java-self-dbtest-dbtest\memory\storage-build-sequence.md`：标 R5 完成，列下一片（R6：隐藏列+MVCC 前的剩余项 / 跨页 B+Tree，需用户确认）；redo/recovery 仍暂停。

---

## Self-Review（writing-plans 自检）

**Spec coverage：** R5a→Task1；R5b（findPredecessor/HeapSpaceManager/inserter 改造）→Task2/3/4；R5c purge→Task5；R5d reorganize→Task6；R5e update→Task7；收口/gitnexus→Task8。spec §3 决策（setDeleted 位级、first-fit 整块复用、plan-then-execute、purge 合并清零旧 owner、reorganize 清 n_owned、update 校验、GARBAGE 语义）均落到对应 Task 代码与注释。

**Placeholder scan：** 已清除 Task1/Task5 早期草稿里的占位/误写；现各 code step 均为可直接落地的完整代码与命令，无 TBD/TODO。Task4 Step1 为 MCP `gitnexus_impact`（前置评估，非代码）；Task7 `moveOverflowLeavesPageUnchanged` 附一条调参提示（非占位）。

**Type consistency：** `HeapSpaceManager.Allocation(offset, heapNo, reused)`、`UpdateResult(outcome, newRef)`、`RecordRef(pageId, heapNo, pageOffset, schemaVersion, indexId)`、`IndexPageHeader.withFree/withGarbage/withNRecs`、`RecordPageDirectory.indexOf`、`RecordPage.setDeleted/findPredecessor`、常量 `RecordPageInserter.MIN_N_OWNED/MAX_N_OWNED` 在各 Task 引用一致。`page.header().free()/garbage()/nRecs()/nHeap()/heapTop()/indexId()/level()` 均与 R3 `IndexPageHeader` 访问器一致。
