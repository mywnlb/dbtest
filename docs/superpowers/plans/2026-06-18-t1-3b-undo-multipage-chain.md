# T1.3b 多页 undo 链物理基座 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: 用 superpowers:subagent-driven-development（推荐）或 superpowers:executing-plans 按任务逐个执行。步骤用 checkbox（`- [x]`）跟踪。

**Goal:** 把 T1.3a 单页 undo 扩成可跨页生长的 undo log segment：append 溢出（先 preflight 再生长）分配并 FIL 链入新 undo 页、拆分 undo page/undo log header、整链正向遍历、持久化重读；端口隔离 undo→api 的分配依赖。**不接事务/rollback，DB_ROLL_PTR 仍 NULL。**

**Architecture:** 自底向上：值对象 `UndoSegmentHandle` → 端口 `UndoSpaceAllocator`(undo) + 适配器 `DiskSpaceUndoAllocator`(api) → undo page/log header 拆分（`UndoPageLayout`/`UndoPage`/`UndoPageAccess`，统一 `RECORD_AREA_START=97`）→ `UndoLogSegment`(跨页 append/read/forEach，溢出前 preflight) + `UndoLogSegmentAccess`(create/open)。undo 页写复用 D3/D4 物理 redo，不新增 redo 类型。

**Tech Stack:** Java 25、JUnit Jupiter、Lombok、固定 JDK `C:\Program Files\Java\jdk-25.0.2`、固定 Gradle `D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1`。

**Spec:** `docs/superpowers/specs/2026-06-18-t1-3b-undo-multipage-chain-design.md`

**项目约束（每个任务都适用）：**
- TDD：先写失败测试 → 跑红 → 最小实现 → 跑绿。
- **无 `synchronized`/`wait`/`notify`**；本片单 writer、无并发新增。
- 生产代码不抛裸 `IllegalArgumentException`/`RuntimeException`，用 `DatabaseValidationException`/`DatabaseRuntimeException`(`UndoPageOverflowException`/`UndoLogFormatException`) 层次。
- 中文 Javadoc/字段注释，解释 undo 物理布局、header 拆分、FIL 链、preflight、latch 边界、依赖方向、DB_ROLL_PTR=NULL 简化点。
- **依赖方向**：`storage.undo` 不得 import `storage.api`（`SegmentRef`/`DiskSpaceManager`）；适配器在 `storage.api` 反向 import 端口。
- **项目 no-commit**：每个任务以「跑绿相关测试」收口，不执行 `git commit`。
- Task 3 改 `UndoPageLayout`/`UndoPage`/`UndoPageAccess`（高扇出，T1.3a 依赖）：同任务内同步更新 `UndoPageTest`/`UndoLogStoreTest`，跑绿后再继续。

**测试命令（PowerShell）：**
```
$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "<pattern>" --console=plain
```

---

## 文件结构

**新建（生产）：**
- `src/main/java/cn/zhangyis/db/storage/undo/UndoSegmentHandle.java` — undo segment 定位值对象（spaceId/inodeSlot/segmentId/firstPageId/lastPageId）
- `src/main/java/cn/zhangyis/db/storage/undo/UndoSpaceAllocator.java` — 端口接口（undo 自有，不暴露 api 类型）
- `src/main/java/cn/zhangyis/db/storage/api/DiskSpaceUndoAllocator.java` — 适配器（实现端口，转 SegmentRef 调 DiskSpaceManager）
- `src/main/java/cn/zhangyis/db/storage/undo/UndoLogSegment.java` — 跨页 undo log MTR 内句柄
- `src/main/java/cn/zhangyis/db/storage/undo/UndoLogSegmentAccess.java` — MTR 生产入口（create/open）

**修改（生产）：**
- `src/main/java/cn/zhangyis/db/storage/undo/UndoPageLayout.java` — 头部拆分（page header [38,63) + log header [63,97)，RECORD_AREA_START=97）
- `src/main/java/cn/zhangyis/db/storage/undo/UndoPage.java` — formatFirstPage/formatChainPage、link、新访问器、FIL prev/next、log header 更新器
- `src/main/java/cn/zhangyis/db/storage/undo/UndoPageAccess.java` — createFirstPage/createChainPage（替换 createUndoPage），保留 openUndoPage
- `src/main/java/cn/zhangyis/db/storage/undo/package-info.java` — 更新包职责

**新建（测试）：**
- `src/test/java/cn/zhangyis/db/storage/undo/UndoSegmentHandleTest.java`
- `src/test/java/cn/zhangyis/db/storage/api/DiskSpaceUndoAllocatorTest.java`
- `src/test/java/cn/zhangyis/db/storage/undo/UndoLogSegmentTest.java`
- `src/test/java/cn/zhangyis/db/storage/undo/UndoLogSegmentReopenTest.java`

**修改（测试）：**
- `src/test/java/cn/zhangyis/db/storage/undo/UndoPageTest.java` — 重写为新布局
- `src/test/java/cn/zhangyis/db/storage/undo/UndoLogStoreTest.java` — harness 改 createFirstPage

---

## Task 1: UndoSegmentHandle 值对象

**Files:**
- Create: `src/main/java/cn/zhangyis/db/storage/undo/UndoSegmentHandle.java`
- Test: `src/test/java/cn/zhangyis/db/storage/undo/UndoSegmentHandleTest.java`

- [x] **Step 1: 写失败测试**

```java
package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.SegmentId;
import cn.zhangyis.db.domain.SpaceId;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class UndoSegmentHandleTest {

    private static final SpaceId SPACE = SpaceId.of(77);

    private static PageId page(long no) {
        return PageId.of(SPACE, PageNo.of(no));
    }

    @Test void buildsAndExposesFields() {
        UndoSegmentHandle h = new UndoSegmentHandle(SPACE, 3, SegmentId.of(9), page(10), page(10));
        assertEquals(SPACE, h.spaceId());
        assertEquals(3, h.inodeSlot());
        assertEquals(9L, h.segmentId().value());
        assertEquals(page(10), h.firstPageId());
        assertEquals(page(10), h.lastPageId());
    }

    @Test void withLastPageReturnsNewInstanceOnlyChangingLast() {
        UndoSegmentHandle h = new UndoSegmentHandle(SPACE, 3, SegmentId.of(9), page(10), page(10));
        UndoSegmentHandle h2 = h.withLastPage(page(20));
        assertEquals(page(20), h2.lastPageId());
        assertEquals(page(10), h2.firstPageId());
        assertEquals(h.spaceId(), h2.spaceId());
        assertEquals(h.inodeSlot(), h2.inodeSlot());
        assertEquals(h.segmentId(), h2.segmentId());
        assertEquals(page(10), h.lastPageId()); // 原实例不可变
    }

    @Test void rejectsNegativeSlot() {
        assertThrows(DatabaseValidationException.class,
                () -> new UndoSegmentHandle(SPACE, -1, SegmentId.of(9), page(10), page(10)));
    }

    @Test void rejectsNonPositiveSegmentId() {
        assertThrows(DatabaseValidationException.class,
                () -> new UndoSegmentHandle(SPACE, 0, SegmentId.of(0), page(10), page(10)));
    }

    @Test void rejectsPageSpaceMismatch() {
        PageId other = PageId.of(SpaceId.of(88), PageNo.of(10));
        assertThrows(DatabaseValidationException.class,
                () -> new UndoSegmentHandle(SPACE, 0, SegmentId.of(9), other, page(10)));
    }
}
```

- [x] **Step 2: 跑红**

Run: `$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "cn.zhangyis.db.storage.undo.UndoSegmentHandleTest" --console=plain`
Expected: 编译失败（`UndoSegmentHandle` 不存在）。

- [x] **Step 3: 实现 UndoSegmentHandle**

```java
package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.SegmentId;
import cn.zhangyis.db.domain.SpaceId;

/**
 * 一条 undo log segment 的逻辑+物理定位（undo 自有，**不暴露 {@code SegmentRef}**，以隔离 undo→api 依赖）。
 * {@code inodeSlot}/{@code segmentId} 落盘到每页 page header，使 reopen 能重建本 handle 续分配；
 * {@code firstPageId}/{@code lastPageId} 是链端点。不可变，{@link #withLastPage} 生长时换尾页。
 *
 * @param spaceId     所属 undo 表空间（单 undo space 假设）。
 * @param inodeSlot   FSP segment inode 槽下标（重建 SegmentRef 续分配用）。
 * @param segmentId   UNDO segment 逻辑编号（&gt;0）。
 * @param firstPageId 链首页（含 undo log header）。
 * @param lastPageId  链尾（当前 append）页。
 */
public record UndoSegmentHandle(SpaceId spaceId, int inodeSlot, SegmentId segmentId,
                                PageId firstPageId, PageId lastPageId) {

    public UndoSegmentHandle {
        if (spaceId == null || segmentId == null || firstPageId == null || lastPageId == null) {
            throw new DatabaseValidationException("undo segment handle fields must not be null");
        }
        if (inodeSlot < 0) {
            throw new DatabaseValidationException("inode slot must be non-negative: " + inodeSlot);
        }
        if (segmentId.value() <= 0) {
            throw new DatabaseValidationException("segment id must be positive: " + segmentId.value());
        }
        if (!firstPageId.spaceId().equals(spaceId) || !lastPageId.spaceId().equals(spaceId)) {
            throw new DatabaseValidationException("undo segment handle page space mismatch with " + spaceId);
        }
    }

    /** 返回仅替换 {@code lastPageId} 的新实例（生长时推进链尾）。 */
    public UndoSegmentHandle withLastPage(PageId newLast) {
        return new UndoSegmentHandle(spaceId, inodeSlot, segmentId, firstPageId, newLast);
    }
}
```

- [x] **Step 4: 跑绿**

Run: `$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "cn.zhangyis.db.storage.undo.UndoSegmentHandleTest" --console=plain`
Expected: PASS。项目 no-commit，跑绿即完成。

---

## Task 2: UndoSpaceAllocator 端口 + DiskSpaceUndoAllocator 适配器

**Files:**
- Create: `src/main/java/cn/zhangyis/db/storage/undo/UndoSpaceAllocator.java`, `src/main/java/cn/zhangyis/db/storage/api/DiskSpaceUndoAllocator.java`
- Test: `src/test/java/cn/zhangyis/db/storage/api/DiskSpaceUndoAllocatorTest.java`

- [x] **Step 1: 写失败测试（onPool harness，验证 createUndoSegment + allocatePage）**

```java
package cn.zhangyis.db.storage.api;

import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.fil.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.PageStore;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import cn.zhangyis.db.storage.undo.UndoSegmentHandle;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/** T1.3b 适配器：createUndoSegment 建 UNDO segment + 首页；allocatePage 续分配不同页号；handle 不暴露 SegmentRef。 */
class DiskSpaceUndoAllocatorTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId UNDO_SPACE = SpaceId.of(77);

    @TempDir Path dir;

    @Test void createUndoSegmentReturnsHandleWithFirstPage() {
        onPool((mgr, allocator) -> {
            MiniTransaction m = mgr.begin();
            UndoSegmentHandle h = allocator.createUndoSegment(m, UNDO_SPACE);
            assertEquals(UNDO_SPACE, h.spaceId());
            assertTrue(h.inodeSlot() >= 0);
            assertTrue(h.segmentId().value() > 0);
            assertEquals(UNDO_SPACE, h.firstPageId().spaceId());
            assertEquals(h.firstPageId(), h.lastPageId());
            mgr.commit(m);
        });
    }

    @Test void allocatePageGivesDistinctPageInSameSegment() {
        onPool((mgr, allocator) -> {
            MiniTransaction m = mgr.begin();
            UndoSegmentHandle h = allocator.createUndoSegment(m, UNDO_SPACE);
            PageId p2 = allocator.allocatePage(m, UNDO_SPACE, h.inodeSlot(), h.segmentId());
            assertEquals(UNDO_SPACE, p2.spaceId());
            assertNotEquals(h.firstPageId().pageNo(), p2.pageNo());
            PageId p3 = allocator.allocatePage(m, UNDO_SPACE, h.inodeSlot(), h.segmentId());
            assertNotEquals(p2.pageNo(), p3.pageNo());
            mgr.commit(m);
        });
    }

    private interface PoolBody { void run(MiniTransactionManager mgr, DiskSpaceUndoAllocator allocator); }

    private void onPool(PoolBody body) {
        PageStore store = new FileChannelPageStore();
        try (PageStore ignored = store; BufferPool pool = new LruBufferPool(store, PS, 128)) {
            MiniTransactionManager mgr = new MiniTransactionManager();
            DiskSpaceManager disk = new DiskSpaceManager(pool, store, PS);
            DiskSpaceUndoAllocator allocator = new DiskSpaceUndoAllocator(disk);
            MiniTransaction boot = mgr.begin();
            disk.createTablespace(boot, UNDO_SPACE, dir.resolve("undo.ibu"), PageNo.of(64));
            mgr.commit(boot);
            body.run(mgr, allocator);
        }
    }
}
```

- [x] **Step 2: 跑红**

Run: `$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "cn.zhangyis.db.storage.api.DiskSpaceUndoAllocatorTest" --console=plain`
Expected: 编译失败（`UndoSpaceAllocator`/`DiskSpaceUndoAllocator` 不存在）。

- [x] **Step 3: 实现端口 UndoSpaceAllocator**

```java
package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.SegmentId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.mtr.MiniTransaction;

/**
 * undo 页分配端口（undo 自有，**不暴露任何 {@code storage.api} 类型**——无 {@code SegmentRef}/{@code DiskSpaceManager}）。
 * 使 {@link UndoLogSegmentAccess} 能在 undo 内部触发页分配而不上行依赖 api。实现见 api 层的 {@code DiskSpaceUndoAllocator}。
 *
 * <p>允许签名类型：{@link MiniTransaction}（storage.mtr）、{@code domain} 值对象、undo 自有 {@link UndoSegmentHandle}。
 * 空间不足由实现抛 {@code NoFreeSpaceException}（storage.fsp unchecked），由适配器透传，undo 不重新包装。
 */
public interface UndoSpaceAllocator {

    /** 建一个 UNDO segment 并分配首页（裸 ALLOCATED 页，未格式化）；返回 handle（firstPageId==lastPageId==首页）。 */
    UndoSegmentHandle createUndoSegment(MiniTransaction mtr, SpaceId undoSpace);

    /** 在该 segment 内再分配一页（裸 ALLOCATED 页），供 undo log 跨页生长。 */
    PageId allocatePage(MiniTransaction mtr, SpaceId undoSpace, int inodeSlot, SegmentId segmentId);
}
```

- [x] **Step 4: 实现适配器 DiskSpaceUndoAllocator**

```java
package cn.zhangyis.db.storage.api;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.SegmentId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.fsp.SegmentPurpose;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.undo.UndoSegmentHandle;
import cn.zhangyis.db.storage.undo.UndoSpaceAllocator;

/**
 * {@link UndoSpaceAllocator} 的磁盘实现（适配器）。把 undo 自有 {@link UndoSegmentHandle} 转 {@link SegmentRef}
 * 再调 {@link DiskSpaceManager}。这是 {@code storage.api → storage.undo}（反向 import 端口）的唯一接触点；
 * undo 侧不知道 {@code DiskSpaceManager}/{@code SegmentRef} 存在。无状态、线程安全。
 */
public final class DiskSpaceUndoAllocator implements UndoSpaceAllocator {

    private final DiskSpaceManager diskSpaceManager;

    public DiskSpaceUndoAllocator(DiskSpaceManager diskSpaceManager) {
        if (diskSpaceManager == null) {
            throw new DatabaseValidationException("disk space manager must not be null");
        }
        this.diskSpaceManager = diskSpaceManager;
    }

    @Override
    public UndoSegmentHandle createUndoSegment(MiniTransaction mtr, SpaceId undoSpace) {
        SegmentRef ref = diskSpaceManager.createSegment(mtr, undoSpace, SegmentPurpose.UNDO);
        PageId first = diskSpaceManager.allocatePage(mtr, ref);
        return new UndoSegmentHandle(undoSpace, ref.inodeSlot(), ref.segmentId(), first, first);
    }

    @Override
    public PageId allocatePage(MiniTransaction mtr, SpaceId undoSpace, int inodeSlot, SegmentId segmentId) {
        SegmentRef ref = new SegmentRef(undoSpace, inodeSlot, segmentId);
        return diskSpaceManager.allocatePage(mtr, ref);
    }
}
```

- [x] **Step 5: 跑绿**

Run: `$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "cn.zhangyis.db.storage.api.DiskSpaceUndoAllocatorTest" --console=plain`
Expected: PASS。

---

## Task 3: 头部拆分 — UndoPageLayout + UndoPage + UndoPageAccess（+ 更新 T1.3a 测试）

**Files:**
- Modify: `src/main/java/cn/zhangyis/db/storage/undo/UndoPageLayout.java`
- Modify: `src/main/java/cn/zhangyis/db/storage/undo/UndoPage.java`
- Modify: `src/main/java/cn/zhangyis/db/storage/undo/UndoPageAccess.java`
- Test (rewrite): `src/test/java/cn/zhangyis/db/storage/undo/UndoPageTest.java`
- Test (改 harness): `src/test/java/cn/zhangyis/db/storage/undo/UndoLogStoreTest.java`

> 本任务一次性把 header 拆分落到布局+页视图+生产入口，并同步更新两个 T1.3a 测试，保证任务末尾全绿。`UndoLog`（T1.3a 单页 facade）源码不变（只用 appendRecord/recordAt/pageId）。

- [x] **Step 1: 重写 UndoPageTest（先红，钉死新布局）**

```java
package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.UndoNo;
import cn.zhangyis.db.storage.api.DiskSpaceManager;
import cn.zhangyis.db.storage.api.IndexPageAccess;
import cn.zhangyis.db.storage.api.SegmentRef;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.fil.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.PageStore;
import cn.zhangyis.db.storage.fsp.SegmentPurpose;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import cn.zhangyis.db.storage.page.FilePageHeader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/** T1.3b UndoPage：first/chain 页格式、header 拆分初值、append 推进 page header、链接、first-only 守门、页类型守门。 */
class UndoPageTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId UNDO_SPACE = SpaceId.of(77);

    @TempDir Path dir;

    @Test void formatFirstPageInitsBothHeaders() {
        onFirstPage((page, handle) -> {
            assertEquals(97, page.freeOffset());
            assertEquals(0, page.recordCount());
            assertEquals(0L, page.pageLastUndoNo().value());
            assertTrue(page.isFirstPage());
            assertEquals(handle.segmentId().value(), page.segmentId().value());
            assertEquals(handle.inodeSlot(), page.inodeSlot());
            assertEquals(UndoLogKind.INSERT, page.undoKind());
            assertEquals(7L, page.transactionId().value());
            assertEquals(page.pageId().pageNo().value(), page.firstPageNo());
            assertEquals(page.pageId().pageNo().value(), page.lastPageNo());
            assertEquals(0L, page.logRecordCount());
            assertEquals(0L, page.logLastUndoNo().value());
        });
    }

    @Test void appendAdvancesPageHeaderOnlyAndReadsBack() {
        onFirstPage((page, handle) -> {
            byte[] a = {1, 2, 3};
            int offA = page.appendRecord(a, UndoNo.of(1));
            byte[] b = {9, 9};
            int offB = page.appendRecord(b, UndoNo.of(2));
            assertEquals(97, offA);
            assertEquals(97 + 2 + 3, offB);
            assertEquals(2, page.recordCount());
            assertEquals(2L, page.pageLastUndoNo().value());
            assertArrayEquals(a, page.recordAt(offA));
            assertArrayEquals(b, page.recordAt(offB));
            assertEquals(0L, page.logRecordCount());     // append 不动 log header
            assertEquals(0L, page.logLastUndoNo().value());
        });
    }

    @Test void appendRejectsNoneUndoNo() {
        onFirstPage((page, handle) ->
                assertThrows(DatabaseValidationException.class,
                        () -> page.appendRecord(new byte[]{1}, UndoNo.NONE)));
    }

    @Test void appendOverflowThrows() {
        onFirstPage((page, handle) ->
                assertThrows(UndoPageOverflowException.class,
                        () -> page.appendRecord(new byte[PS.bytes()], UndoNo.of(1))));
    }

    @Test void recordAtRejectsOutOfArea() {
        onFirstPage((page, handle) -> {
            page.appendRecord(new byte[]{1, 2}, UndoNo.of(1));
            assertThrows(UndoLogFormatException.class, () -> page.recordAt(10_000));
        });
    }

    @Test void chainPageIsNotFirstAndLogHeaderAccessorsThrow() {
        onPool((mgr, disk, undoAccess, pool) -> {
            MiniTransaction m = mgr.begin();
            SegmentRef seg = disk.createSegment(m, UNDO_SPACE, SegmentPurpose.UNDO);
            PageId p1 = disk.allocatePage(m, seg);
            PageId p2 = disk.allocatePage(m, seg);
            UndoSegmentHandle handle = new UndoSegmentHandle(UNDO_SPACE, seg.inodeSlot(), seg.segmentId(), p1, p1);
            undoAccess.createFirstPage(m, p1, UndoLogKind.INSERT, TransactionId.of(7), handle);
            UndoPage chain = undoAccess.createChainPage(m, p2, handle);
            assertFalse(chain.isFirstPage());
            assertEquals(97, chain.freeOffset());
            assertEquals(handle.segmentId().value(), chain.segmentId().value());
            assertThrows(UndoLogFormatException.class, chain::transactionId);
            assertThrows(UndoLogFormatException.class, chain::undoKind);
            assertThrows(UndoLogFormatException.class, chain::logRecordCount);
            assertThrows(UndoLogFormatException.class, () -> chain.setLastPageNo(PageNo.of(5)));
            mgr.commit(m);
        });
    }

    @Test void linkNextPreservesPrevAndViceVersa() {
        onPool((mgr, disk, undoAccess, pool) -> {
            MiniTransaction m = mgr.begin();
            SegmentRef seg = disk.createSegment(m, UNDO_SPACE, SegmentPurpose.UNDO);
            PageId p1 = disk.allocatePage(m, seg);
            PageId p2 = disk.allocatePage(m, seg);
            UndoSegmentHandle handle = new UndoSegmentHandle(UNDO_SPACE, seg.inodeSlot(), seg.segmentId(), p1, p1);
            UndoPage first = undoAccess.createFirstPage(m, p1, UndoLogKind.INSERT, TransactionId.of(7), handle);
            UndoPage chain = undoAccess.createChainPage(m, p2, handle);
            first.linkNextTo(p2.pageNo());
            chain.linkPrevTo(p1.pageNo());
            assertEquals(p2.pageNo().value(), first.nextPageNo());
            assertEquals(FilePageHeader.FIL_NULL, first.prevPageNo());
            assertEquals(p1.pageNo().value(), chain.prevPageNo());
            assertEquals(FilePageHeader.FIL_NULL, chain.nextPageNo());
            mgr.commit(m);
        });
    }

    @Test void openUndoPageRejectsAllocatedType() {
        onPool((mgr, disk, undoAccess, pool) -> {
            MiniTransaction m = mgr.begin();
            SegmentRef seg = disk.createSegment(m, UNDO_SPACE, SegmentPurpose.UNDO);
            PageId pid = disk.allocatePage(m, seg);
            mgr.commit(m);
            MiniTransaction r = mgr.begin();
            assertThrows(UndoLogFormatException.class,
                    () -> undoAccess.openUndoPage(r, pid, PageLatchMode.SHARED));
            mgr.rollbackUncommitted(r);
        });
    }

    @Test void openUndoPageRejectsIndexType() {
        onPool((mgr, disk, undoAccess, pool) -> {
            IndexPageAccess idx = new IndexPageAccess(pool, PS);
            MiniTransaction m = mgr.begin();
            SegmentRef seg = disk.createSegment(m, UNDO_SPACE, SegmentPurpose.INDEX_LEAF);
            PageId pid = disk.allocatePage(m, seg);
            idx.createIndexPage(m, pid, 1L, 0);
            mgr.commit(m);
            MiniTransaction r = mgr.begin();
            assertThrows(UndoLogFormatException.class,
                    () -> undoAccess.openUndoPage(r, pid, PageLatchMode.SHARED));
            mgr.rollbackUncommitted(r);
        });
    }

    // ---- harness ----

    private interface FirstPageBody { void run(UndoPage page, UndoSegmentHandle handle); }
    private interface PoolBody { void run(MiniTransactionManager mgr, DiskSpaceManager disk,
                                          UndoPageAccess undoAccess, BufferPool pool); }

    private void onFirstPage(FirstPageBody body) {
        onPool((mgr, disk, undoAccess, pool) -> {
            MiniTransaction m = mgr.begin();
            SegmentRef seg = disk.createSegment(m, UNDO_SPACE, SegmentPurpose.UNDO);
            PageId pid = disk.allocatePage(m, seg);
            UndoSegmentHandle handle = new UndoSegmentHandle(UNDO_SPACE, seg.inodeSlot(), seg.segmentId(), pid, pid);
            UndoPage page = undoAccess.createFirstPage(m, pid, UndoLogKind.INSERT, TransactionId.of(7), handle);
            body.run(page, handle);
            mgr.commit(m);
        });
    }

    private void onPool(PoolBody body) {
        PageStore store = new FileChannelPageStore();
        try (PageStore ignored = store; BufferPool pool = new LruBufferPool(store, PS, 128)) {
            MiniTransactionManager mgr = new MiniTransactionManager();
            DiskSpaceManager disk = new DiskSpaceManager(pool, store, PS);
            UndoPageAccess undoAccess = new UndoPageAccess(pool, PS);
            MiniTransaction boot = mgr.begin();
            disk.createTablespace(boot, UNDO_SPACE, dir.resolve("undo.ibu"), PageNo.of(64));
            mgr.commit(boot);
            body.run(mgr, disk, undoAccess, pool);
        }
    }
}
```

- [x] **Step 2: 跑红**

Run: `$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "cn.zhangyis.db.storage.undo.UndoPageTest" --console=plain`
Expected: 编译失败（`createFirstPage`/`createChainPage`/`isFirstPage`/`pageLastUndoNo`/`linkNextTo` 等不存在）。

- [x] **Step 3: 重写 UndoPageLayout（头部拆分）**

```java
package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.storage.page.PageEnvelopeLayout;

/**
 * undo page 布局（设计 §6.4 拆分）。**统一 RECORD_AREA_START=97**：page header [38,63) 每页都有；
 * undo log header [63,97) 仅 first 页填，非 first 页清零预留（offset 数学不分叉，读时按 PAGE_FLAGS.isFirstPage 决定解析）。
 * per-page 链 prev/next 用 FIL header（不在此），log header 另存 FIRST/LAST_PAGE_NO 端点。undo record 槽=[len u16][payload]。
 */
final class UndoPageLayout {

    private UndoPageLayout() {
    }

    // ---- undo page header（每页，紧接 FIL body=38）----
    /** 下一条 record 追加位置（u16）；format 初始化为 RECORD_AREA_START。 */
    static final int FREE_OFFSET = PageEnvelopeLayout.FIL_PAGE_HEADER_BYTES; // 38
    /** 本页已追加 record 数（u16）。 */
    static final int RECORD_COUNT = FREE_OFFSET + 2;          // 40
    /** 本页最近 record 的 undoNo（u64，0=本页空）。 */
    static final int PAGE_LAST_UNDO_NO = RECORD_COUNT + 2;    // 42
    /** 所属 UNDO segment id（u64，reopen 重建 handle + readRecord 段一致性校验）。 */
    static final int SEGMENT_ID = PAGE_LAST_UNDO_NO + 8;      // 50
    /** SegmentRef.inodeSlot（u32，reopen 续分配 + 段一致性校验）。 */
    static final int INODE_SLOT = SEGMENT_ID + 8;             // 58
    /** 页标志（u8）：bit0=isFirstPage（是否含 log header）。 */
    static final int PAGE_FLAGS = INODE_SLOT + 4;             // 62
    /** undo page header 末（= undo log header 起）。 */
    static final int PAGE_HEADER_END = PAGE_FLAGS + 1;        // 63

    // ---- undo log header（仅 first 页填，非 first 页清零）----
    /** 该 undo log 所属事务 id（u64）。 */
    static final int TRANSACTION_ID = PAGE_HEADER_END;        // 63
    /** UndoLogKind ordinal（u8，本片恒 INSERT）。 */
    static final int UNDO_KIND = TRANSACTION_ID + 8;          // 71
    /** undo log 状态占位（u8，本片恒 ACTIVE）。 */
    static final int STATE = UNDO_KIND + 1;                   // 72
    /** 链首页号（u32，= first 页自身）。 */
    static final int FIRST_PAGE_NO = STATE + 1;               // 73
    /** 链尾（当前 append）页号（u32，生长推进）。 */
    static final int LAST_PAGE_NO = FIRST_PAGE_NO + 4;        // 77
    /** 整链 record 总数（u64）。 */
    static final int LOG_RECORD_COUNT = LAST_PAGE_NO + 4;     // 81
    /** 整链最近 record 的 undoNo（u64）。 */
    static final int LOG_LAST_UNDO_NO = LOG_RECORD_COUNT + 8; // 89
    /** undo log header 末。 */
    static final int LOG_HEADER_END = LOG_LAST_UNDO_NO + 8;   // 97

    /** record area 起点（统一常量，所有页一致）。 */
    static final int RECORD_AREA_START = LOG_HEADER_END;      // 97

    /** state 占位常量：ACTIVE。 */
    static final int STATE_ACTIVE = 0;
    /** PAGE_FLAGS bit0：first 页标志。 */
    static final int FLAG_FIRST_PAGE = 0x01;
}
```

- [x] **Step 4: 重写 UndoPage（拆分 format、link、新访问器、FIL 链、log header 更新器）**

```java
package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SegmentId;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.UndoNo;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.page.PageEnvelope;
import cn.zhangyis.db.storage.page.PageEnvelopeLayout;

/**
 * undo 页视图（PageGuard 之上）。所有状态在页字节里；写经 MTR-owned X latch guard，自动收 PAGE_BYTES redo、
 * commit 盖 pageLSN。page header 每页都有；log header 仅 first 页（{@code formatChainPage} 清零非 first 页的 log 区，
 * first-only 访问器 {@code requireFirstPage} 守门）。FIL prev/next 承载 per-page 链。
 */
public final class UndoPage {

    private final PageGuard guard;
    private final PageSize pageSize;

    UndoPage(PageGuard guard, PageSize pageSize) {
        if (guard == null || pageSize == null) {
            throw new DatabaseValidationException("undo page guard/pageSize must not be null");
        }
        this.guard = guard;
        this.pageSize = pageSize;
    }

    /** 初始化 first 页：page header（isFirstPage=1）+ log header（事务/kind/state/链端点=自身/计数 0）。要求 X。 */
    void formatFirstPage(UndoLogKind kind, TransactionId txnId, UndoSegmentHandle handle) {
        if (kind == null || txnId == null || handle == null) {
            throw new DatabaseValidationException("undo first page format args must not be null");
        }
        writePageHeader(handle, true);
        guard.writeLong(UndoPageLayout.TRANSACTION_ID, txnId.value());
        setU8(UndoPageLayout.UNDO_KIND, kind.ordinal());
        setU8(UndoPageLayout.STATE, UndoPageLayout.STATE_ACTIVE);
        long self = guard.pageId().pageNo().value();
        setU32(UndoPageLayout.FIRST_PAGE_NO, self);
        setU32(UndoPageLayout.LAST_PAGE_NO, self);
        guard.writeLong(UndoPageLayout.LOG_RECORD_COUNT, 0L);
        guard.writeLong(UndoPageLayout.LOG_LAST_UNDO_NO, 0L);
    }

    /** 初始化 chain 页：page header（isFirstPage=0）+ 清零 log header 区 [63,97)。要求 X。 */
    void formatChainPage(UndoSegmentHandle handle) {
        if (handle == null) {
            throw new DatabaseValidationException("undo chain page format handle must not be null");
        }
        writePageHeader(handle, false);
        guard.writeBytes(UndoPageLayout.TRANSACTION_ID,
                new byte[UndoPageLayout.LOG_HEADER_END - UndoPageLayout.TRANSACTION_ID]);
    }

    private void writePageHeader(UndoSegmentHandle handle, boolean first) {
        setU16(UndoPageLayout.FREE_OFFSET, UndoPageLayout.RECORD_AREA_START);
        setU16(UndoPageLayout.RECORD_COUNT, 0);
        guard.writeLong(UndoPageLayout.PAGE_LAST_UNDO_NO, 0L);
        guard.writeLong(UndoPageLayout.SEGMENT_ID, handle.segmentId().value());
        setU32(UndoPageLayout.INODE_SLOT, handle.inodeSlot());
        setU8(UndoPageLayout.PAGE_FLAGS, first ? UndoPageLayout.FLAG_FIRST_PAGE : 0);
    }

    /**
     * 在 freeOffset 追加一条 record（要求 X）。校验 undoNo&gt;0 → 溢出判定（写页前）→ 写 [len][payload] →
     * 推进 page header FREE_OFFSET/RECORD_COUNT/PAGE_LAST_UNDO_NO → 返回槽起点 offset。**不动 log header**。
     */
    int appendRecord(byte[] payload, UndoNo undoNo) {
        if (payload == null || undoNo == null) {
            throw new DatabaseValidationException("undo append payload/undoNo must not be null");
        }
        if (undoNo.isNone()) {
            throw new DatabaseValidationException("undo append undoNo must be > 0 (not NONE)");
        }
        int free = getU16(UndoPageLayout.FREE_OFFSET);
        int need = 2 + payload.length;
        int limit = pageSize.bytes() - PageEnvelopeLayout.FIL_PAGE_TRAILER_BYTES;
        if (free + need > limit) {
            throw new UndoPageOverflowException("undo record (" + need + "B) does not fit at free="
                    + free + " limit=" + limit);
        }
        setU16(free, payload.length);
        guard.writeBytes(free + 2, payload);
        setU16(UndoPageLayout.FREE_OFFSET, free + need);
        setU16(UndoPageLayout.RECORD_COUNT, getU16(UndoPageLayout.RECORD_COUNT) + 1);
        guard.writeLong(UndoPageLayout.PAGE_LAST_UNDO_NO, undoNo.value());
        return free;
    }

    /** 读 offset 处 record payload（S/X 均可）。offset/len 出 record area 抛 {@link UndoLogFormatException}。 */
    byte[] recordAt(int offset) {
        int free = getU16(UndoPageLayout.FREE_OFFSET);
        if (offset < UndoPageLayout.RECORD_AREA_START || offset + 2 > free) {
            throw new UndoLogFormatException("undo record offset out of area: " + offset + " free=" + free);
        }
        int len = getU16(offset);
        if (offset + 2 + len > free) {
            throw new UndoLogFormatException("undo record length out of area: off=" + offset + " len=" + len);
        }
        return guard.readBytes(offset + 2, len);
    }

    /** 设 FIL NEXT 为 next，**保留 PREV**。要求 X。 */
    void linkNextTo(PageNo next) {
        if (next == null) {
            throw new DatabaseValidationException("undo link next must not be null");
        }
        long prev = PageEnvelope.readHeader(guard).prevPageNo();
        PageEnvelope.writeSiblingLinks(guard, prev, next.value());
    }

    /** 设 FIL PREV 为 prev，**保留 NEXT**。要求 X。 */
    void linkPrevTo(PageNo prev) {
        if (prev == null) {
            throw new DatabaseValidationException("undo link prev must not be null");
        }
        long next = PageEnvelope.readHeader(guard).nextPageNo();
        PageEnvelope.writeSiblingLinks(guard, prev.value(), next);
    }

    /** 推进 first 页 LAST_PAGE_NO。要求 first 页 + X。 */
    void setLastPageNo(PageNo last) {
        requireFirstPage();
        if (last == null) {
            throw new DatabaseValidationException("undo last page no must not be null");
        }
        setU32(UndoPageLayout.LAST_PAGE_NO, last.value());
    }

    /** 设整链 record 总数。要求 first 页 + X。 */
    void setLogRecordCount(long count) {
        requireFirstPage();
        guard.writeLong(UndoPageLayout.LOG_RECORD_COUNT, count);
    }

    /** 设整链最近 undoNo。要求 first 页 + X。 */
    void setLogLastUndoNo(long undoNo) {
        requireFirstPage();
        guard.writeLong(UndoPageLayout.LOG_LAST_UNDO_NO, undoNo);
    }

    PageId pageId() { return guard.pageId(); }

    SegmentId segmentId() { return SegmentId.of(guard.readLong(UndoPageLayout.SEGMENT_ID)); }

    int inodeSlot() { return guard.readInt(UndoPageLayout.INODE_SLOT); }

    boolean isFirstPage() { return (getU8(UndoPageLayout.PAGE_FLAGS) & UndoPageLayout.FLAG_FIRST_PAGE) != 0; }

    int pageFlags() { return getU8(UndoPageLayout.PAGE_FLAGS); }

    int freeOffset() { return getU16(UndoPageLayout.FREE_OFFSET); }

    int recordCount() { return getU16(UndoPageLayout.RECORD_COUNT); }

    UndoNo pageLastUndoNo() { return UndoNo.of(guard.readLong(UndoPageLayout.PAGE_LAST_UNDO_NO)); }

    /** FIL NEXT 页号（无则 FIL_NULL）。 */
    long nextPageNo() { return PageEnvelope.readHeader(guard).nextPageNo(); }

    /** FIL PREV 页号（无则 FIL_NULL）。 */
    long prevPageNo() { return PageEnvelope.readHeader(guard).prevPageNo(); }

    TransactionId transactionId() {
        requireFirstPage();
        return TransactionId.of(guard.readLong(UndoPageLayout.TRANSACTION_ID));
    }

    UndoLogKind undoKind() {
        requireFirstPage();
        int idx = getU8(UndoPageLayout.UNDO_KIND);
        UndoLogKind[] all = UndoLogKind.values();
        if (idx < 0 || idx >= all.length) {
            throw new UndoLogFormatException("undo kind ordinal out of range: " + idx);
        }
        return all[idx];
    }

    int state() { requireFirstPage(); return getU8(UndoPageLayout.STATE); }

    long firstPageNo() { requireFirstPage(); return getU32(UndoPageLayout.FIRST_PAGE_NO); }

    long lastPageNo() { requireFirstPage(); return getU32(UndoPageLayout.LAST_PAGE_NO); }

    long logRecordCount() { requireFirstPage(); return guard.readLong(UndoPageLayout.LOG_RECORD_COUNT); }

    UndoNo logLastUndoNo() { requireFirstPage(); return UndoNo.of(guard.readLong(UndoPageLayout.LOG_LAST_UNDO_NO)); }

    private void requireFirstPage() {
        if (!isFirstPage()) {
            throw new UndoLogFormatException("undo page is not the log first page: " + guard.pageId());
        }
    }

    private int getU8(int off) { return guard.readBytes(off, 1)[0] & 0xFF; }

    private void setU8(int off, int v) { guard.writeBytes(off, new byte[]{(byte) v}); }

    private int getU16(int off) {
        byte[] b = guard.readBytes(off, 2);
        return ((b[0] & 0xFF) << 8) | (b[1] & 0xFF);
    }

    private void setU16(int off, int v) {
        guard.writeBytes(off, new byte[]{(byte) (v >>> 8), (byte) v});
    }

    private long getU32(int off) { return guard.readInt(off) & 0xFFFFFFFFL; }

    private void setU32(int off, long v) { guard.writeInt(off, (int) v); }
}
```

- [x] **Step 5: 重写 UndoPageAccess（createFirstPage/createChainPage 替换 createUndoPage）**

```java
package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.page.FilePageHeader;
import cn.zhangyis.db.storage.page.PageEnvelope;
import cn.zhangyis.db.storage.page.PageType;

/**
 * undo 页的 MTR 生产入口（仿 {@code IndexPageAccess}）。建/开 undo 页绑定 MTR-owned guard，使 PAGE_INIT/PAGE_BYTES
 * 自动产 redo、commit 盖 pageLSN。返回的 UndoPage 由 mtr memo 持 guard，**勿自行 close**。
 */
public final class UndoPageAccess {

    private final BufferPool pool;
    private final PageSize pageSize;

    public UndoPageAccess(BufferPool pool, PageSize pageSize) {
        if (pool == null || pageSize == null) {
            throw new DatabaseValidationException("undo page access pool/pageSize must not be null");
        }
        this.pool = pool;
        this.pageSize = pageSize;
    }

    /** 建并格式化 undo log first 页：newPage(X,UNDO)+写信封(UNDO,prev=next=FIL_NULL)+formatFirstPage。校验全部前置。 */
    public UndoPage createFirstPage(MiniTransaction mtr, PageId pageId, UndoLogKind kind,
                                    TransactionId txnId, UndoSegmentHandle handle) {
        if (mtr == null || pageId == null || kind == null || txnId == null || handle == null) {
            throw new DatabaseValidationException("createFirstPage args must not be null");
        }
        PageGuard g = newUndoEnvelope(mtr, pageId);
        UndoPage page = new UndoPage(g, pageSize);
        page.formatFirstPage(kind, txnId, handle);
        return page;
    }

    /** 建并格式化 undo chain（非 first）页：newPage(X,UNDO)+写信封+formatChainPage（清零 log header 区）。 */
    public UndoPage createChainPage(MiniTransaction mtr, PageId pageId, UndoSegmentHandle handle) {
        if (mtr == null || pageId == null || handle == null) {
            throw new DatabaseValidationException("createChainPage args must not be null");
        }
        PageGuard g = newUndoEnvelope(mtr, pageId);
        UndoPage page = new UndoPage(g, pageSize);
        page.formatChainPage(handle);
        return page;
    }

    /** 打开已存在 undo 页（X 追加 / S 读回）。校验信封 pageType==UNDO，否则 {@link UndoLogFormatException}。 */
    public UndoPage openUndoPage(MiniTransaction mtr, PageId pageId, PageLatchMode mode) {
        if (mtr == null || pageId == null || mode == null) {
            throw new DatabaseValidationException("openUndoPage args must not be null");
        }
        PageGuard g = mtr.getPage(pool, pageId, mode);
        FilePageHeader h = PageEnvelope.readHeader(g);
        if (h.pageType() != PageType.UNDO) {
            throw new UndoLogFormatException("page " + pageId + " is not an UNDO page: " + h.pageType());
        }
        return new UndoPage(g, pageSize);
    }

    private PageGuard newUndoEnvelope(MiniTransaction mtr, PageId pageId) {
        PageGuard g = mtr.newPage(pool, pageId, PageLatchMode.EXCLUSIVE, PageType.UNDO);
        PageEnvelope.writeHeader(g, new FilePageHeader(pageId.spaceId(), pageId.pageNo().value(),
                FilePageHeader.FIL_NULL, FilePageHeader.FIL_NULL, 0L, PageType.UNDO));
        return g;
    }
}
```

- [x] **Step 6: 更新 UndoLogStoreTest 的 harness（createUndoPage → createFirstPage）**

把 `UndoLogStoreTest.freshUndoPage` 方法体替换为（`UndoLogStoreTest` 与 `UndoSegmentHandle` 同包 `cn.zhangyis.db.storage.undo`，无需新增 import；`SegmentRef`/`TransactionId`/`UndoLogKind` 已在原文件 import）：

```java
    private UndoPage freshUndoPage(MiniTransaction m, DiskSpaceManager disk, UndoPageAccess access) {
        var seg = disk.createSegment(m, UNDO_SPACE, SegmentPurpose.UNDO);
        PageId pid = disk.allocatePage(m, seg);
        UndoSegmentHandle handle = new UndoSegmentHandle(UNDO_SPACE, seg.inodeSlot(), seg.segmentId(), pid, pid);
        return access.createFirstPage(m, pid, UndoLogKind.INSERT, TransactionId.of(7), handle);
    }
```

其余 `UndoLogStoreTest` 断言（`reopened.undoKind()`/`reopened.recordCount()`/`UndoLog.append`/`readRecord`）不变——它们读的是 first 页，访问器仍有效。

- [x] **Step 7: 跑绿（UndoPageTest + UndoLogStoreTest）**

Run: `$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "cn.zhangyis.db.storage.undo.UndoPageTest" --tests "cn.zhangyis.db.storage.undo.UndoLogStoreTest" --console=plain`
Expected: PASS（新布局 first/chain 页、append/link/守门、T1.3a 单页 facade 端到端 + reopen 仍绿）。

---

## Task 4: UndoLogSegmentAccess.create + UndoLogSegment（单页 append/read/forEach）

**Files:**
- Create: `src/main/java/cn/zhangyis/db/storage/undo/UndoLogSegment.java`, `src/main/java/cn/zhangyis/db/storage/undo/UndoLogSegmentAccess.java`
- Test: `src/test/java/cn/zhangyis/db/storage/undo/UndoLogSegmentTest.java`

> 本任务只做单页路径（append 不溢出）；跨页生长在 Task 5、open 在 Task 6 接入。

- [x] **Step 1: 写失败测试（单页 create/append/readRecord/forEach/log header 计数/NULL 指针）**

```java
package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.UndoNo;
import cn.zhangyis.db.storage.api.DiskSpaceManager;
import cn.zhangyis.db.storage.api.DiskSpaceUndoAllocator;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.fil.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.PageStore;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
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

import static org.junit.jupiter.api.Assertions.*;

/** T1.3b UndoLogSegment 单页：create→append→RollPointer→readRecord、log header 计数、forEach 有序、NULL 指针拒绝。 */
class UndoLogSegmentTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId UNDO_SPACE = SpaceId.of(77);

    @TempDir Path dir;

    private final TypeCodecRegistry registry = new TypeCodecRegistry();

    private static TableSchema schema() {
        return new TableSchema(1, List.of(
                new ColumnDef(new ColumnId(0), "id", ColumnType.intType(false, false), 0),
                new ColumnDef(new ColumnId(1), "name", ColumnType.varchar(64, true), 1)), true);
    }
    private static IndexKeyDef keyDef() {
        return new IndexKeyDef(9L, List.of(new KeyPartDef(new ColumnId(0), KeyOrder.ASC, 0)));
    }
    private UndoRecord rec(long undoNo, long id, RollPointer prev) {
        return new UndoRecord(UndoRecordType.INSERT_ROW, UndoNo.of(undoNo), TransactionId.of(7),
                1L, 9L, List.of(new ColumnValue.IntValue(id)), prev);
    }

    @Test void createAppendReadBackSinglePage() {
        onSegment(seg -> {
            UndoRecord r = rec(1, 100, RollPointer.NULL);
            RollPointer rp = seg.append(r, keyDef(), schema());
            assertFalse(rp.isNull());
            assertTrue(rp.insert());
            assertEquals(seg.firstPageId().pageNo(), rp.pageNo());
            assertEquals(r, seg.readRecord(rp, keyDef(), schema()));
            assertEquals(1L, seg.logRecordCount());
            assertEquals(1L, seg.logLastUndoNo().value());
        });
    }

    @Test void logHeaderCountsAdvancePerAppend() {
        onSegment(seg -> {
            seg.append(rec(1, 100, RollPointer.NULL), keyDef(), schema());
            seg.append(rec(2, 101, RollPointer.NULL), keyDef(), schema());
            seg.append(rec(3, 102, RollPointer.NULL), keyDef(), schema());
            assertEquals(3L, seg.logRecordCount());
            assertEquals(3L, seg.logLastUndoNo().value());
        });
    }

    @Test void forEachRecordReturnsAllInOrderSinglePage() {
        onSegment(seg -> {
            seg.append(rec(1, 100, RollPointer.NULL), keyDef(), schema());
            seg.append(rec(2, 101, RollPointer.NULL), keyDef(), schema());
            List<UndoRecord> got = new ArrayList<>();
            seg.forEachRecord(got::add, keyDef(), schema());
            assertEquals(List.of(rec(1, 100, RollPointer.NULL), rec(2, 101, RollPointer.NULL)), got);
        });
    }

    @Test void readRecordRejectsNullPointer() {
        onSegment(seg ->
                assertThrows(UndoLogFormatException.class,
                        () -> seg.readRecord(RollPointer.NULL, keyDef(), schema())));
    }

    // ---- harness ----

    private interface SegmentBody { void run(UndoLogSegment seg); }

    private void onSegment(SegmentBody body) {
        PageStore store = new FileChannelPageStore();
        try (PageStore ignored = store; BufferPool pool = new LruBufferPool(store, PS, 128)) {
            MiniTransactionManager mgr = new MiniTransactionManager();
            DiskSpaceManager disk = new DiskSpaceManager(pool, store, PS);
            DiskSpaceUndoAllocator allocator = new DiskSpaceUndoAllocator(disk);
            UndoLogSegmentAccess access = new UndoLogSegmentAccess(pool, PS, allocator, registry);
            MiniTransaction boot = mgr.begin();
            disk.createTablespace(boot, UNDO_SPACE, dir.resolve("undo.ibu"), PageNo.of(64));
            mgr.commit(boot);
            MiniTransaction m = mgr.begin();
            UndoLogSegment seg = access.create(m, UNDO_SPACE, TransactionId.of(7));
            body.run(seg);
            mgr.commit(m);
        }
    }
}
```

- [x] **Step 2: 跑红**

Run: `$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "cn.zhangyis.db.storage.undo.UndoLogSegmentTest" --console=plain`
Expected: 编译失败（`UndoLogSegment`/`UndoLogSegmentAccess` 不存在）。

- [x] **Step 3: 实现 UndoLogSegment（单页版，无生长）**

```java
package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.UndoNo;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.page.FilePageHeader;
import cn.zhangyis.db.storage.record.schema.IndexKeyDef;
import cn.zhangyis.db.storage.record.schema.TableSchema;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * 一条跨页 undo log 的 MTR 内句柄（设计 §5.4 UndoSegment 物理面，本片只做物理多页链）。持 {@link UndoSegmentHandle}
 * + first 页（log header 入口）+ current/last 页（append 目标）+ 本会话已 fix 页缓存（{@code heldPages}）。
 * append 溢出生长（Task 5）、open 续写/遍历（Task 6）。**单 writer 假设**：同一 segment 同时只一个 writer。
 * **不接事务/rollback，DB_ROLL_PTR 仍 NULL。**
 */
public final class UndoLogSegment {

    private final MiniTransaction mtr;
    private final PageSize pageSize;
    private final UndoSpaceAllocator allocator;
    private final UndoRecordCodec codec;
    private final UndoPageAccess pageAccess;
    private final PageLatchMode mode;
    /** first 页（log header 权威入口；append/生长都在此更新整链计数与 LAST_PAGE_NO）。 */
    private final UndoPage firstPage;
    /** 本会话已 fix 页缓存：first/current/生长页（X）+ resolvePage 读到的非 held 页（S）；规避对已持页二次 getPage。 */
    private final Map<Long, UndoPage> heldPages = new HashMap<>();
    /** undo segment 定位（生长时 withLastPage 推进）。 */
    private UndoSegmentHandle handle;
    /** 当前 append 目标页（生长后切到新尾页）。 */
    private UndoPage current;

    UndoLogSegment(MiniTransaction mtr, PageSize pageSize, UndoSpaceAllocator allocator, UndoRecordCodec codec,
                   UndoPageAccess pageAccess, UndoSegmentHandle handle, UndoPage firstPage, UndoPage current,
                   PageLatchMode mode) {
        if (mtr == null || pageSize == null || allocator == null || codec == null || pageAccess == null
                || handle == null || firstPage == null || current == null || mode == null) {
            throw new DatabaseValidationException("undo log segment fields must not be null");
        }
        this.mtr = mtr;
        this.pageSize = pageSize;
        this.allocator = allocator;
        this.codec = codec;
        this.pageAccess = pageAccess;
        this.handle = handle;
        this.firstPage = firstPage;
        this.current = current;
        this.mode = mode;
        heldPages.put(firstPage.pageId().pageNo().value(), firstPage);
        heldPages.put(current.pageId().pageNo().value(), current);
    }

    /**
     * 追加一条 undo record（要求 X 会话）。codec 编码 → current 页 appendRecord → 更新 first 页 log header
     * （LOG_RECORD_COUNT++/LOG_LAST_UNDO_NO）→ 组装 insert RollPointer。本任务单页：放不下直接抛
     * {@link UndoPageOverflowException}（Task 5 接入溢出生长）。
     */
    public RollPointer append(UndoRecord rec, IndexKeyDef keyDef, TableSchema schema) {
        if (mode != PageLatchMode.EXCLUSIVE) {
            throw new DatabaseValidationException("append requires an EXCLUSIVE (writable) undo log segment session");
        }
        if (rec == null) {
            throw new DatabaseValidationException("undo append record must not be null");
        }
        byte[] payload = codec.encode(rec, keyDef, schema);
        int off = current.appendRecord(payload, rec.undoNo());
        firstPage.setLogRecordCount(firstPage.logRecordCount() + 1);
        firstPage.setLogLastUndoNo(rec.undoNo().value());
        return new RollPointer(true, current.pageId().pageNo(), off);
    }

    /**
     * 按 RollPointer 读回 undo record。校验非 NULL → resolvePage 定位（已持直读/未持 S-fix）→ **段一致性校验**
     * （目标页 segmentId/inodeSlot 与 handle 一致，防别的 segment 页混入）→ recordAt → decode。
     */
    public UndoRecord readRecord(RollPointer rp, IndexKeyDef keyDef, TableSchema schema) {
        if (rp == null) {
            throw new DatabaseValidationException("undo readRecord roll pointer must not be null");
        }
        if (rp.isNull()) {
            throw new UndoLogFormatException("cannot read undo record from NULL roll pointer");
        }
        UndoPage page = resolvePage(rp.pageNo());
        requireSameSegment(page, "roll pointer page " + rp.pageNo());
        byte[] payload = page.recordAt(rp.offset());
        return codec.decode(payload, 0, keyDef, schema);
    }

    /**
     * 整链正向遍历：从 first 页沿 FIL next 链，每页按 [RECORD_AREA_START, freeOffset) 用 [len] 前缀逐槽 decode，
     * 按 append(undoNo) 序回调。链中页段不符/非 UNDO → {@link UndoLogFormatException}。
     */
    public void forEachRecord(Consumer<UndoRecord> consumer, IndexKeyDef keyDef, TableSchema schema) {
        if (consumer == null) {
            throw new DatabaseValidationException("undo forEachRecord consumer must not be null");
        }
        long pageNoVal = handle.firstPageId().pageNo().value();
        while (true) {
            UndoPage page = resolvePage(PageNo.of(pageNoVal));
            requireSameSegment(page, "undo chain page " + pageNoVal);
            int free = page.freeOffset();
            int off = UndoPageLayout.RECORD_AREA_START;
            while (off < free) {
                byte[] payload = page.recordAt(off);
                consumer.accept(codec.decode(payload, 0, keyDef, schema));
                off += 2 + payload.length;
            }
            long next = page.nextPageNo();
            if (next == FilePageHeader.FIL_NULL) {
                break;
            }
            pageNoVal = next;
        }
    }

    /** 链首页 id。 */
    public PageId firstPageId() { return handle.firstPageId(); }

    /** 链尾页 id。 */
    public PageId lastPageId() { return handle.lastPageId(); }

    /** 所属事务。 */
    public TransactionId transactionId() { return firstPage.transactionId(); }

    /** undo log 种类。 */
    public UndoLogKind undoKind() { return firstPage.undoKind(); }

    /** 整链 record 总数。 */
    public long logRecordCount() { return firstPage.logRecordCount(); }

    /** 整链最近 undoNo。 */
    public UndoNo logLastUndoNo() { return firstPage.logLastUndoNo(); }

    /**
     * 定位页：已持（first/current/生长页/先前 S-resolved）直读；否则 S-fix 并缓存进 heldPages（同 MTR 重复读复用）。
     * 永不对已持 X 页再 getPage，规避二次 fix / pageLSN 隐患（设计 §5.3）。
     */
    private UndoPage resolvePage(PageNo pageNo) {
        UndoPage held = heldPages.get(pageNo.value());
        if (held != null) {
            return held;
        }
        UndoPage page = pageAccess.openUndoPage(mtr, PageId.of(handle.spaceId(), pageNo), PageLatchMode.SHARED);
        heldPages.put(pageNo.value(), page);
        return page;
    }

    private void requireSameSegment(UndoPage page, String what) {
        if (!page.segmentId().equals(handle.segmentId()) || page.inodeSlot() != handle.inodeSlot()) {
            throw new UndoLogFormatException(what + " not in undo segment " + handle.segmentId().value()
                    + "/slot " + handle.inodeSlot());
        }
    }
}
```

- [x] **Step 4: 实现 UndoLogSegmentAccess（仅 create；open 见 Task 6）**

```java
package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.record.type.TypeCodecRegistry;

/**
 * 跨页 undo log 的 MTR 生产入口（仿 {@code IndexPageAccess}/{@link UndoPageAccess}）。只依赖端口
 * {@link UndoSpaceAllocator}（不知道 {@code DiskSpaceManager} 存在）。create 建新 undo log segment；
 * open（Task 6）按 firstPageId 重开。返回的 {@link UndoLogSegment} 由 mtr memo 持 guard，勿自行 close。
 */
public final class UndoLogSegmentAccess {

    private final PageSize pageSize;
    private final UndoSpaceAllocator allocator;
    private final UndoRecordCodec codec;
    private final UndoPageAccess pageAccess;

    public UndoLogSegmentAccess(BufferPool pool, PageSize pageSize, UndoSpaceAllocator allocator,
                                TypeCodecRegistry registry) {
        if (pool == null || pageSize == null || allocator == null || registry == null) {
            throw new DatabaseValidationException("undo log segment access args must not be null");
        }
        this.pageSize = pageSize;
        this.allocator = allocator;
        this.codec = new UndoRecordCodec(registry);
        this.pageAccess = new UndoPageAccess(pool, pageSize);
    }

    /**
     * 建一条新 undo log segment：allocator 建 UNDO segment + 首页（裸 ALLOCATED）→ createFirstPage 格式化为 UNDO
     * first 页（同页第二次 newPage：ALLOCATED→UNDO，与 T1.3a 双 newPage 同款，redo 顺序最终态 UNDO）→ 返回 X 会话句柄。
     */
    public UndoLogSegment create(MiniTransaction mtr, SpaceId undoSpace, TransactionId txnId) {
        if (mtr == null || undoSpace == null || txnId == null) {
            throw new DatabaseValidationException("undo log segment create args must not be null");
        }
        UndoSegmentHandle handle = allocator.createUndoSegment(mtr, undoSpace);
        UndoPage firstPage = pageAccess.createFirstPage(mtr, handle.firstPageId(), UndoLogKind.INSERT, txnId, handle);
        return new UndoLogSegment(mtr, pageSize, allocator, codec, pageAccess, handle, firstPage, firstPage,
                PageLatchMode.EXCLUSIVE);
    }
}
```

- [x] **Step 5: 跑绿**

Run: `$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "cn.zhangyis.db.storage.undo.UndoLogSegmentTest" --console=plain`
Expected: PASS（单页 create/append/readRecord/forEach/log header 计数/NULL 指针）。

---

## Task 5: 多页生长（溢出 + preflight）+ 跨页测试

**Files:**
- Modify: `src/main/java/cn/zhangyis/db/storage/undo/UndoLogSegment.java`（append 加 try/catch + growAndAppend）
- Test: `src/test/java/cn/zhangyis/db/storage/undo/UndoLogSegmentTest.java`（追加跨页测试 + 大记录 helper + onAccess harness）

- [x] **Step 1: 在 UndoLogSegmentTest 追加大记录 helper、跨页测试与 onAccess harness（先红）**

在 `UndoLogSegmentTest` 类体内追加（import 已含 `ArrayList`/`List`/各 schema 类；新增 `UndoLogFormatException`/`UndoPageOverflowException` 同包免 import）：

```java
    // 大 key schema：varchar 单列，便于少量记录填满 16KB 页触发生长。
    private static TableSchema bigSchema() {
        return new TableSchema(1, List.of(
                new ColumnDef(new ColumnId(0), "k", ColumnType.varchar(20000, false), 0)), true);
    }
    private static IndexKeyDef bigKeyDef() {
        return new IndexKeyDef(9L, List.of(new KeyPartDef(new ColumnId(0), KeyOrder.ASC, 0)));
    }
    private UndoRecord bigRec(long undoNo, String key, RollPointer prev) {
        return new UndoRecord(UndoRecordType.INSERT_ROW, UndoNo.of(undoNo), TransactionId.of(7),
                1L, 9L, List.of(new ColumnValue.StringValue(key)), prev);
    }
    // 5000 字节 key（前缀含 undoNo 以区分）：payload≈5044、slot≈5046；16KB 页 record area≈16279，3 条满、第 4 条生长。
    private static String bigKey(long undoNo) {
        return String.format("%05d", undoNo) + "x".repeat(4995);
    }

    @Test void growthAllocatesLinksNewPageAndReadsAcross() {
        onSegment(seg -> {
            RollPointer rp1 = seg.append(bigRec(1, bigKey(1), RollPointer.NULL), bigKeyDef(), bigSchema());
            seg.append(bigRec(2, bigKey(2), RollPointer.NULL), bigKeyDef(), bigSchema());
            RollPointer rp3 = seg.append(bigRec(3, bigKey(3), RollPointer.NULL), bigKeyDef(), bigSchema());
            RollPointer rp4 = seg.append(bigRec(4, bigKey(4), RollPointer.NULL), bigKeyDef(), bigSchema());
            // rec1-3 在 first 页，rec4 生长到第 2 页
            assertEquals(seg.firstPageId().pageNo(), rp1.pageNo());
            assertEquals(seg.firstPageId().pageNo(), rp3.pageNo());
            assertNotEquals(seg.firstPageId().pageNo(), rp4.pageNo());
            assertEquals(seg.lastPageId().pageNo(), rp4.pageNo());
            assertEquals(4L, seg.logRecordCount());
            assertEquals(4L, seg.logLastUndoNo().value());
            // 跨页 readRecord
            assertEquals(bigRec(4, bigKey(4), RollPointer.NULL), seg.readRecord(rp4, bigKeyDef(), bigSchema()));
            assertEquals(bigRec(1, bigKey(1), RollPointer.NULL), seg.readRecord(rp1, bigKeyDef(), bigSchema()));
        });
    }

    @Test void forEachTraversesAllPagesInOrder() {
        onSegment(seg -> {
            List<UndoRecord> expected = new ArrayList<>();
            for (long i = 1; i <= 5; i++) {
                UndoRecord r = bigRec(i, bigKey(i), RollPointer.NULL);
                expected.add(r);
                seg.append(r, bigKeyDef(), bigSchema());
            }
            assertNotEquals(seg.firstPageId().pageNo(), seg.lastPageId().pageNo()); // 已跨页
            List<UndoRecord> got = new ArrayList<>();
            seg.forEachRecord(got::add, bigKeyDef(), bigSchema());
            assertEquals(expected, got); // 链接正确 + 顺序正确
        });
    }

    @Test void prevRollPointerChainsAcrossPages() {
        onSegment(seg -> {
            seg.append(bigRec(1, bigKey(1), RollPointer.NULL), bigKeyDef(), bigSchema());
            seg.append(bigRec(2, bigKey(2), RollPointer.NULL), bigKeyDef(), bigSchema());
            RollPointer rp3 = seg.append(bigRec(3, bigKey(3), RollPointer.NULL), bigKeyDef(), bigSchema());
            RollPointer rp4 = seg.append(bigRec(4, bigKey(4), rp3), bigKeyDef(), bigSchema());
            assertNotEquals(rp3.pageNo(), rp4.pageNo()); // 前驱在 first 页、本条在 page2
            UndoRecord back4 = seg.readRecord(rp4, bigKeyDef(), bigSchema());
            assertEquals(rp3, back4.prevRollPointer());
            assertEquals(bigRec(3, bigKey(3), RollPointer.NULL),
                    seg.readRecord(back4.prevRollPointer(), bigKeyDef(), bigSchema()));
        });
    }

    @Test void oversizedRecordThrowsWithoutGrowing() {
        onSegment(seg -> {
            String huge = "y".repeat(16300); // 单条 payload > 空页容量
            UndoRecord big = new UndoRecord(UndoRecordType.INSERT_ROW, UndoNo.of(1), TransactionId.of(7),
                    1L, 9L, List.of(new ColumnValue.StringValue(huge)), RollPointer.NULL);
            assertThrows(UndoPageOverflowException.class,
                    () -> seg.append(big, bigKeyDef(), bigSchema()));
            // 零副作用：first 仍是 last、整链计数 0、无 next 链 → forEach 空
            assertEquals(seg.firstPageId(), seg.lastPageId());
            assertEquals(0L, seg.logRecordCount());
            assertEquals(0L, seg.logLastUndoNo().value());
            List<UndoRecord> got = new ArrayList<>();
            seg.forEachRecord(got::add, bigKeyDef(), bigSchema());
            assertTrue(got.isEmpty());
        });
    }

    @Test void readRecordRejectsPointerFromOtherSegment() {
        onAccess((mgr, access) -> {
            // segB：建 + append + commit，记其 RollPointer
            MiniTransaction m1 = mgr.begin();
            UndoLogSegment segB = access.create(m1, UNDO_SPACE, TransactionId.of(8));
            RollPointer rpB = segB.append(rec(1, 100, RollPointer.NULL), keyDef(), schema());
            mgr.commit(m1);
            // segA：新 mtr，用指向 segB 页的指针读 → 段不符（在 recordAt 前抛）
            MiniTransaction m2 = mgr.begin();
            UndoLogSegment segA = access.create(m2, UNDO_SPACE, TransactionId.of(7));
            assertThrows(UndoLogFormatException.class, () -> segA.readRecord(rpB, keyDef(), schema()));
            mgr.commit(m2);
        });
    }

    private interface AccessBody { void run(MiniTransactionManager mgr, UndoLogSegmentAccess access); }

    private void onAccess(AccessBody body) {
        PageStore store = new FileChannelPageStore();
        try (PageStore ignored = store; BufferPool pool = new LruBufferPool(store, PS, 128)) {
            MiniTransactionManager mgr = new MiniTransactionManager();
            DiskSpaceManager disk = new DiskSpaceManager(pool, store, PS);
            DiskSpaceUndoAllocator allocator = new DiskSpaceUndoAllocator(disk);
            UndoLogSegmentAccess access = new UndoLogSegmentAccess(pool, PS, allocator, registry);
            MiniTransaction boot = mgr.begin();
            disk.createTablespace(boot, UNDO_SPACE, dir.resolve("undo.ibu"), PageNo.of(64));
            mgr.commit(boot);
            body.run(mgr, access);
        }
    }
```

- [x] **Step 2: 跑红**

Run: `$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "cn.zhangyis.db.storage.undo.UndoLogSegmentTest" --console=plain`
Expected: FAIL（`growthAllocatesLinksNewPageAndReadsAcross` 等：当前 append 不生长，第 4 条抛 `UndoPageOverflowException`）。

- [x] **Step 3: 给 UndoLogSegment.append 加溢出生长（preflight 优先）**

在 `UndoLogSegment` 顶部 import 区加：
```java
import cn.zhangyis.db.storage.page.PageEnvelopeLayout;
```
把 `append` 方法整体替换，并新增 `growAndAppend`：

```java
    public RollPointer append(UndoRecord rec, IndexKeyDef keyDef, TableSchema schema) {
        if (mode != PageLatchMode.EXCLUSIVE) {
            throw new DatabaseValidationException("append requires an EXCLUSIVE (writable) undo log segment session");
        }
        if (rec == null) {
            throw new DatabaseValidationException("undo append record must not be null");
        }
        byte[] payload = codec.encode(rec, keyDef, schema);
        int off;
        try {
            off = current.appendRecord(payload, rec.undoNo());
        } catch (UndoPageOverflowException overflow) {
            off = growAndAppend(payload, rec.undoNo(), overflow);
        }
        firstPage.setLogRecordCount(firstPage.logRecordCount() + 1);
        firstPage.setLogLastUndoNo(rec.undoNo().value());
        return new RollPointer(true, current.pageId().pageNo(), off);
    }

    /**
     * current 页放不下时生长一页再写。**先 preflight**：若全新空页都容不下（单条超页）→ 直接抛 overflow，
     * 不 allocate、不 createChainPage、不改 FIL 链、不动 first 页 header（MTR 无 content undo，杜绝半生长脏链）。
     * 通过后：allocate → createChainPage → FIL 双向链（保留对侧指针）→ first 页 LAST_PAGE_NO 推进 →
     * handle.withLastPage → 切 current → 新空页重试（已保证可容纳）。
     */
    private int growAndAppend(byte[] payload, UndoNo undoNo, UndoPageOverflowException overflow) {
        int need = 2 + payload.length;
        int freshCapacity = pageSize.bytes() - PageEnvelopeLayout.FIL_PAGE_TRAILER_BYTES
                - UndoPageLayout.RECORD_AREA_START;
        if (need > freshCapacity) {
            throw overflow;
        }
        PageId newId = allocator.allocatePage(mtr, handle.spaceId(), handle.inodeSlot(), handle.segmentId());
        UndoPage newPage = pageAccess.createChainPage(mtr, newId, handle);
        current.linkNextTo(newId.pageNo());
        newPage.linkPrevTo(current.pageId().pageNo());
        firstPage.setLastPageNo(newId.pageNo());
        handle = handle.withLastPage(newId);
        heldPages.put(newId.pageNo().value(), newPage);
        current = newPage;
        return current.appendRecord(payload, undoNo);
    }
```

- [x] **Step 4: 跑绿**

Run: `$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "cn.zhangyis.db.storage.undo.UndoLogSegmentTest" --console=plain`
Expected: PASS（单页 + 跨页生长/遍历/prev 链/超页零副作用/跨段指针拒绝）。

---

## Task 6: UndoLogSegmentAccess.open（SHARED 读 / EXCLUSIVE 续写 + last 页校验）+ 持久化重读

**Files:**
- Modify: `src/main/java/cn/zhangyis/db/storage/undo/UndoLogSegmentAccess.java`（加 open）
- Test: `src/test/java/cn/zhangyis/db/storage/undo/UndoLogSegmentReopenTest.java`

- [x] **Step 1: 写失败测试（reopen 读、续写、非 first 页守门、损坏 last 页守门）**

```java
package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.storage.api.DiskSpaceManager;
import cn.zhangyis.db.storage.api.DiskSpaceUndoAllocator;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.fil.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.PageStore;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
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

import static org.junit.jupiter.api.Assertions.*;

/** T1.3b UndoLogSegment 持久化/重开：跨 store/pool reopen 读整链、open(X) 续写、非 first 页 + 损坏 last 页守门。 */
class UndoLogSegmentReopenTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId UNDO_SPACE = SpaceId.of(77);

    @TempDir Path dir;

    private final TypeCodecRegistry registry = new TypeCodecRegistry();

    private static TableSchema bigSchema() {
        return new TableSchema(1, List.of(
                new ColumnDef(new ColumnId(0), "k", ColumnType.varchar(20000, false), 0)), true);
    }
    private static IndexKeyDef bigKeyDef() {
        return new IndexKeyDef(9L, List.of(new KeyPartDef(new ColumnId(0), KeyOrder.ASC, 0)));
    }
    private UndoRecord bigRec(long undoNo, String key) {
        return new UndoRecord(UndoRecordType.INSERT_ROW, UndoNo.of(undoNo), TransactionId.of(7),
                1L, 9L, List.of(new ColumnValue.StringValue(key)), RollPointer.NULL);
    }
    private static String bigKey(long undoNo) {
        return String.format("%05d", undoNo) + "x".repeat(4995);
    }

    @Test void reopenSharedReadsMultiPageChain() {
        Path path = dir.resolve("undo.ibu");
        List<UndoRecord> expected = new ArrayList<>();
        PageId firstPageId = buildSession(path, 1, 5, expected);

        try (PageStore store = new FileChannelPageStore();
             BufferPool pool = new LruBufferPool(store, PS, 128)) {
            store.open(UNDO_SPACE, path, PS);
            MiniTransactionManager mgr = new MiniTransactionManager();
            UndoLogSegmentAccess access = accessOn(store, pool);
            MiniTransaction r = mgr.begin();
            UndoLogSegment seg = access.open(r, firstPageId, PageLatchMode.SHARED);
            assertEquals(5L, seg.logRecordCount());
            assertNotEquals(seg.firstPageId().pageNo(), seg.lastPageId().pageNo()); // 多页
            List<UndoRecord> got = new ArrayList<>();
            seg.forEachRecord(got::add, bigKeyDef(), bigSchema());
            assertEquals(expected, got);
            mgr.rollbackUncommitted(r);
        }
    }

    @Test void reopenExclusiveContinuesAppend() {
        Path path = dir.resolve("undo.ibu");
        List<UndoRecord> expected = new ArrayList<>();
        PageId firstPageId = buildSession(path, 1, 5, expected); // 5 条（2 页）

        try (PageStore store = new FileChannelPageStore();
             BufferPool pool = new LruBufferPool(store, PS, 128)) {
            store.open(UNDO_SPACE, path, PS);
            MiniTransactionManager mgr = new MiniTransactionManager();
            UndoLogSegmentAccess access = accessOn(store, pool);
            // open(X) 续写 2 条（跨到第 3 页）
            MiniTransaction m = mgr.begin();
            UndoLogSegment seg = access.open(m, firstPageId, PageLatchMode.EXCLUSIVE);
            for (long i = 6; i <= 7; i++) {
                UndoRecord rr = bigRec(i, bigKey(i));
                expected.add(rr);
                seg.append(rr, bigKeyDef(), bigSchema());
            }
            assertEquals(7L, seg.logRecordCount());
            mgr.commit(m);
            // 同 pool 新 mtr reopen(S) 验证 7 条全在、有序
            MiniTransaction r = mgr.begin();
            UndoLogSegment ro = access.open(r, firstPageId, PageLatchMode.SHARED);
            assertEquals(7L, ro.logRecordCount());
            List<UndoRecord> got = new ArrayList<>();
            ro.forEachRecord(got::add, bigKeyDef(), bigSchema());
            assertEquals(expected, got);
            mgr.rollbackUncommitted(r);
        }
    }

    @Test void openRejectsNonFirstPage() {
        Path path = dir.resolve("undo.ibu");
        List<UndoRecord> expected = new ArrayList<>();
        PageId firstPageId = buildSession(path, 1, 5, expected);

        try (PageStore store = new FileChannelPageStore();
             BufferPool pool = new LruBufferPool(store, PS, 128)) {
            store.open(UNDO_SPACE, path, PS);
            MiniTransactionManager mgr = new MiniTransactionManager();
            UndoLogSegmentAccess access = accessOn(store, pool);
            MiniTransaction r1 = mgr.begin();
            PageId page2 = access.open(r1, firstPageId, PageLatchMode.SHARED).lastPageId(); // 非 first 尾页
            mgr.rollbackUncommitted(r1);
            assertNotEquals(firstPageId.pageNo(), page2.pageNo());
            MiniTransaction r2 = mgr.begin();
            assertThrows(UndoLogFormatException.class,
                    () -> access.open(r2, page2, PageLatchMode.SHARED));
            mgr.rollbackUncommitted(r2);
        }
    }

    @Test void openExclusiveRejectsCorruptLastPage() {
        // 同 pool 内：segA、segB 各建 1 页 commit；篡改 segA.first 的 LAST_PAGE_NO 指向 segB.first；open(X) 校验段不符
        try (PageStore store = new FileChannelPageStore();
             BufferPool pool = new LruBufferPool(store, PS, 128)) {
            MiniTransactionManager mgr = new MiniTransactionManager();
            DiskSpaceManager disk = new DiskSpaceManager(pool, store, PS);
            UndoLogSegmentAccess access = new UndoLogSegmentAccess(pool, PS, new DiskSpaceUndoAllocator(disk), registry);
            UndoPageAccess pageAccess = new UndoPageAccess(pool, PS);
            MiniTransaction boot = mgr.begin();
            disk.createTablespace(boot, UNDO_SPACE, dir.resolve("undo.ibu"), PageNo.of(64));
            mgr.commit(boot);

            MiniTransaction m1 = mgr.begin();
            UndoLogSegment a = access.create(m1, UNDO_SPACE, TransactionId.of(7));
            UndoLogSegment b = access.create(m1, UNDO_SPACE, TransactionId.of(8));
            PageId aFirst = a.firstPageId();
            PageNo bFirstNo = b.firstPageId().pageNo();
            mgr.commit(m1);

            MiniTransaction m2 = mgr.begin();
            UndoPage aFirstPage = pageAccess.openUndoPage(m2, aFirst, PageLatchMode.EXCLUSIVE);
            aFirstPage.setLastPageNo(bFirstNo); // 模拟损坏：LAST_PAGE_NO 指向别的 segment 页
            mgr.commit(m2);

            MiniTransaction m3 = mgr.begin();
            assertThrows(UndoLogFormatException.class,
                    () -> access.open(m3, aFirst, PageLatchMode.EXCLUSIVE));
            mgr.rollbackUncommitted(m3);
        }
    }

    // ---- harness ----

    /** session1：建表空间 + 建 segment + append [from,to] 大记录 + commit；返回 firstPageId，并把记录加入 expected。 */
    private PageId buildSession(Path path, long from, long to, List<UndoRecord> expected) {
        try (PageStore store = new FileChannelPageStore();
             BufferPool pool = new LruBufferPool(store, PS, 128)) {
            MiniTransactionManager mgr = new MiniTransactionManager();
            DiskSpaceManager disk = new DiskSpaceManager(pool, store, PS);
            UndoLogSegmentAccess access = new UndoLogSegmentAccess(pool, PS, new DiskSpaceUndoAllocator(disk), registry);
            MiniTransaction boot = mgr.begin();
            disk.createTablespace(boot, UNDO_SPACE, path, PageNo.of(64));
            mgr.commit(boot);
            MiniTransaction m = mgr.begin();
            UndoLogSegment seg = access.create(m, UNDO_SPACE, TransactionId.of(7));
            PageId firstPageId = seg.firstPageId();
            for (long i = from; i <= to; i++) {
                UndoRecord rr = bigRec(i, bigKey(i));
                expected.add(rr);
                seg.append(rr, bigKeyDef(), bigSchema());
            }
            mgr.commit(m);
            return firstPageId;
        }
    }

    private UndoLogSegmentAccess accessOn(PageStore store, BufferPool pool) {
        DiskSpaceManager disk = new DiskSpaceManager(pool, store, PS);
        return new UndoLogSegmentAccess(pool, PS, new DiskSpaceUndoAllocator(disk), registry);
    }
}
```

- [x] **Step 2: 跑红**

Run: `$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "cn.zhangyis.db.storage.undo.UndoLogSegmentReopenTest" --console=plain`
Expected: 编译失败（`UndoLogSegmentAccess.open` 不存在）。

- [x] **Step 3: 实现 UndoLogSegmentAccess.open**

在 `UndoLogSegmentAccess` 增加方法（import `PageId`/`PageNo`/`PageLatchMode` 按需补；`PageLatchMode` 已 import）：

```java
    /**
     * 重开一条已存在 undo log segment。读 first 页（校验 isFirstPage）→ 按其 SEGMENT_ID/INODE_SLOT/LAST_PAGE_NO
     * 重建 {@link UndoSegmentHandle}。{@code SHARED}=只读/遍历（仅持 first 页 S）；{@code EXCLUSIVE}=续 append——
     * 额外打开 last 页 X，并**校验 last 页段归属与 handle 一致**（防 LAST_PAGE_NO 损坏把别的 UNDO 页当尾页续写）。
     */
    public UndoLogSegment open(MiniTransaction mtr, PageId firstPageId, PageLatchMode mode) {
        if (mtr == null || firstPageId == null || mode == null) {
            throw new DatabaseValidationException("undo log segment open args must not be null");
        }
        UndoPage firstPage = pageAccess.openUndoPage(mtr, firstPageId, mode);
        if (!firstPage.isFirstPage()) {
            throw new UndoLogFormatException("page " + firstPageId + " is not the undo log first page");
        }
        long lastPageNoVal = firstPage.lastPageNo();
        PageId lastPageId = PageId.of(firstPageId.spaceId(), PageNo.of(lastPageNoVal));
        UndoSegmentHandle handle = new UndoSegmentHandle(firstPageId.spaceId(), firstPage.inodeSlot(),
                firstPage.segmentId(), firstPageId, lastPageId);
        UndoPage current = firstPage;
        if (mode == PageLatchMode.EXCLUSIVE && lastPageNoVal != firstPageId.pageNo().value()) {
            UndoPage last = pageAccess.openUndoPage(mtr, lastPageId, PageLatchMode.EXCLUSIVE);
            if (!last.segmentId().equals(handle.segmentId()) || last.inodeSlot() != handle.inodeSlot()) {
                throw new UndoLogFormatException("undo last page " + lastPageId + " segment mismatch with "
                        + handle.segmentId().value() + "/slot " + handle.inodeSlot());
            }
            current = last;
        }
        return new UndoLogSegment(mtr, pageSize, allocator, codec, pageAccess, handle, firstPage, current, mode);
    }
```

并在 `UndoLogSegmentAccess` import 区补：
```java
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
```

- [x] **Step 4: 跑绿**

Run: `$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "cn.zhangyis.db.storage.undo.UndoLogSegmentReopenTest" --console=plain`
Expected: PASS（reopen-S 读整链、open(X) 续写、非 first 页守门、损坏 last 页守门）。

---

## Task 7: package-info + 全量回归 + 收口

**Files:**
- Modify: `src/main/java/cn/zhangyis/db/storage/undo/package-info.java`

- [x] **Step 1: 更新 undo 包职责说明**

```java
/**
 * Undo 日志物理存储基座（T1.3a 单页 + T1.3b 多页链）：undo page/log header 格式、INSERT undo record 编解码、
 * RollPointer 寻址、跨页 undo log segment（生长/遍历/重读）、端口隔离的页分配。
 *
 * <p>T1.3a：{@link cn.zhangyis.db.storage.undo.UndoPage}/{@link cn.zhangyis.db.storage.undo.UndoPageAccess}/
 * {@link cn.zhangyis.db.storage.undo.UndoLog} 单页 append→{@link cn.zhangyis.db.domain.RollPointer}→read。
 * T1.3b：{@link cn.zhangyis.db.storage.undo.UndoLogSegment}/{@link cn.zhangyis.db.storage.undo.UndoLogSegmentAccess}
 * 跨页生长（溢出先 preflight 再分配 + FIL 链）、整链正向遍历、持久化重读；页分配经端口
 * {@link cn.zhangyis.db.storage.undo.UndoSpaceAllocator}（适配器 {@code DiskSpaceUndoAllocator} 在 storage.api，
 * 隔离 undo→api 依赖）。undo 页写复用 D3/D4 物理 redo，不新增 redo 类型。
 *
 * <p>非目标（后续片）：rollback、UndoContext、rollback segment header/slot、history list、MVCC 旧版本、purge、
 * 恢复期 rollback、undo 回收/truncation、并发 append、多 rseg/多 undo 表空间、真 DB_ROLL_PTR（本片仍恒 NULL）。
 */
package cn.zhangyis.db.storage.undo;
```

- [x] **Step 2: 全量回归**

Run: `$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --console=plain`
Expected: BUILD SUCCESSFUL，failures=0 errors=0；测试数较 T1.3a 基线（445）只增不减（新增 `UndoSegmentHandleTest`、`DiskSpaceUndoAllocatorTest`、`UndoLogSegmentTest`、`UndoLogSegmentReopenTest`，`UndoPageTest` 方法增量）。**关键回归**：record R1-R5 / B1/B2/B3 / redo / recovery / T1.1/T1.2 聚簇路径全绿；`UndoLog`（T1.3a 单页 facade）经 header 拆分后端到端 + reopen 仍绿；`PageType`/非聚簇字节/`DB_ROLL_PTR=NULL` 不变。

> 用实际测试数核对：用 `Get-ChildItem build/test-results/test/*.xml` 聚合 `tests`/`failures`/`errors`（见 T1.3a 收口）。failures+errors 必须为 0。

- [x] **Step 3: 收口**

项目 no-commit：不执行 `git commit`。在 `MEMORY.md` / `storage-build-sequence.md` 追加「T1.3b 多页 undo 链完成」要点（实际测试数、新增类清单：`UndoSegmentHandle`、`UndoSpaceAllocator`、`DiskSpaceUndoAllocator`、`UndoLogSegment`、`UndoLogSegmentAccess`；改：`UndoPageLayout`/`UndoPage`/`UndoPageAccess` 头拆分 + `UndoLogStoreTest`/`UndoPageTest` 同步）；下一步 = T1.3c undo 写路径接线（UndoContext + insertClustered 写真 undo + 真 DB_ROLL_PTR）。

---

## 自检（写计划后对照 spec）

1. **spec 覆盖**：`UndoSegmentHandle`(Task1)、端口+适配器(Task2)、头拆分 page/log header + 统一 RECORD_AREA_START=97 + 非 first 清零(Task3)、`UndoLogSegment` 单页 append/read/forEach + 段一致性校验 + resolvePage 缓存(Task4)、多页生长 preflight-先于改页 + 跨页遍历/prev/超页零副作用/跨段拒绝(Task5)、open(SHARED/EXCLUSIVE last 页校验)+非 first 守门 + reopen 持久化(Task6)、package-info + 回归(Task7)。spec §1-§9 各节均有对应任务。
2. **placeholder 扫描**：已移除 Task6 的 `peekFirst`/`lastPageOf` 占位；无 TODO/TBD/“按实际签名调整”。
3. **类型/签名一致**：`UndoSegmentHandle(SpaceId,int,SegmentId,PageId,PageId)`、`withLastPage(PageId)`；端口 `createUndoSegment(mtr,SpaceId)→handle`、`allocatePage(mtr,SpaceId,int,SegmentId)→PageId`；`UndoPage.formatFirstPage(kind,txnId,handle)`/`formatChainPage(handle)`/`appendRecord(byte[],UndoNo)→int`/`recordAt(int)→byte[]`/`linkNextTo(PageNo)`/`linkPrevTo(PageNo)`/`setLastPageNo(PageNo)`/`setLogRecordCount(long)`/`setLogLastUndoNo(long)`/`nextPageNo()→long`/`segmentId()→SegmentId`/`inodeSlot()→int`；`UndoPageAccess.createFirstPage(mtr,PageId,kind,txnId,handle)`/`createChainPage(mtr,PageId,handle)`/`openUndoPage(mtr,PageId,mode)`；`UndoLogSegmentAccess(BufferPool,PageSize,UndoSpaceAllocator,TypeCodecRegistry)`、`create(mtr,SpaceId,txnId)`、`open(mtr,PageId,mode)`；`UndoLogSegment.append/readRecord/forEachRecord(Consumer<UndoRecord>)`、`firstPageId()/lastPageId()/logRecordCount()/logLastUndoNo()`；header 偏移 38/40/42/50/58/62/63/71/72/73/77/81/89/97 跨任务一致。
4. **依赖方向**：`storage.undo` 不 import `storage.api`；`DiskSpaceUndoAllocator`(api) 反向 import 端口 + `SegmentRef` + `DiskSpaceManager`。
5. **风险点**：header 拆分高扇出由 Task3 同步更新 `UndoPageTest`/`UndoLogStoreTest` + Task7 全量回归守护；preflight-先于改页由 Task5 `oversizedRecordThrowsWithoutGrowing` 钉死零副作用；reopen 重建 handle + 续写由 Task6 持久化测试钉死；latch reentrancy 由 `resolvePage`（已持直读、未持 S-fix 并缓存）规避，跨段读测试经 commit 后新 mtr 取页避开二次 fix。
6. **测试可落地**：大 key（varchar(20000)、5000B 串）确定性触发生长（16KB 页 3 条满第 4 条生长）；超页用 16300B 串；`onSegment`/`onAccess`/`buildSession`/`accessOn` harness 复用 T1.3a `FileChannelPageStore`+`LruBufferPool`+`DiskSpaceManager` 模式。
7. **DB_ROLL_PTR 仍 NULL**：本片不触碰 `insertClustered`/聚簇隐藏列；范围/package-info/回归说明多处重复钉死。

