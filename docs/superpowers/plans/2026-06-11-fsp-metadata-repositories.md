# fsp 元数据仓储层 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 落地 §6.2–6.4 三类首区管理页（SpaceHeader/XDES/SegmentInode）的布局 + 单页 CRUD 仓储，经 buf+mtr 读写；并给 buf `PageGuard` 补 `readLong/writeLong`。

**Architecture:** 每类页一个偏移常量类（`*Layout`）+ 值对象/record + Repository，经 `MiniTransaction.getPage(pool,pageId,X/S)` 拿 `PageGuard` 按偏移读写。所有哨兵取 0（FileAddress.NULL=全零、ExtentState.FREE=0、XDES 无主=0、fragment 空=0；真实 segment id 与 `nextSegmentId` 从 1 起并在入口校验）。物理/逻辑分仓储：SpaceHeader/XDES 物理账本、SegmentInode 逻辑归属；extent 0 是系统保留 extent，普通分配、`initFree` 和普通 XDES mutator 必须跳过。本计划仍是 no-redo 原型，不声明 crash-safe。

**Tech Stack:** Java 25、JUnit Jupiter（buf+fil+mtr + `@TempDir` 真实驱动）、`ByteBuffer` 绝对读写。

**Spec:** `docs/superpowers/specs/2026-06-11-fsp-metadata-repositories-design.md`

**通用约定：**
- fsp 类位于 `cn.zhangyis.db.storage.fsp`；Task 1 改 `buf.PageGuard`。
- 中文 Javadoc；禁 `synchronized`；禁裸异常（用项目异常）。**不提交**（master）。
- 写路径直接获取 EXCLUSIVE，禁止同一 MTR 内对同页 S→X 升级；组合更新 page0/page2 时按 page0→page2 顺序取 latch。
- no-redo 原型：相关类注释和测试名称不得声称 crash-safe；未来 redo 对应 `UPDATE_SPACE_HEADER`、`UPDATE_XDES`、`UPDATE_SEGMENT_INODE`。
- 测试运行：`$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --console=plain --tests "<FQCN>"`；全量去 `--tests`。
- 集成测试通用前置：`PageStore store = new FileChannelPageStore(); store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(N));`（N≥4，使 page 0/1/2/3 存在）；try-with-resources 中先声明 `PageStore s = store`，再声明 `BufferPool pool = new LruBufferPool(...)`，确保关闭时先 flush/close buffer pool、后关闭物理 PageStore；`MiniTransactionManager mgr = new MiniTransactionManager();`；`mtr = mgr.begin(); ...repo ops...; mgr.commit(mtr);`。

---

## File Structure

生产 `src/main/java/cn/zhangyis/db/storage/`：

| 文件 | 职责 |
| --- | --- |
| `buf/PageGuard.java`（改） | 加 `readLong/writeLong` |
| `mtr/MtrMemo.java` / `mtr/MiniTransaction.java`（改） | 记录 memo 中的 page latch 模式，显式拒绝同一 MTR 内同页 S→X 升级 |
| `fsp/PageLayouts.java` | `FIL_PAGE_DATA` 常量 |
| `fsp/FileAddress.java` | 页内地址 + NULL（全零编码）|
| `fsp/ExtentState.java` / `fsp/SegmentPurpose.java` | 枚举 |
| `fsp/FspMetadataException.java` | 元数据约束异常 |
| `fsp/SpaceHeaderLayout.java` / `SpaceHeaderSnapshot.java` / `SpaceHeaderRepository.java` | page 0 头部 |
| `fsp/ExtentDescriptorLayout.java` / `ExtentDescriptor.java` / `ExtentDescriptorRepository.java` | XDES |
| `fsp/SegmentInodeLayout.java` / `SegmentInode.java` / `SegmentInodeRepository.java` | page 2 inode |

测试：`buf/PageGuardTest.java`（增）、`mtr/MiniTransactionTest.java`（增 S→X 升级拒绝）、`fsp/FileAddressTest`、`fsp/FspEnumTest`、`fsp/SpaceHeaderRepositoryTest`、`fsp/ExtentDescriptorRepositoryTest`、`fsp/SegmentInodeRepositoryTest`。

---

## Task 1: buf PageGuard readLong / writeLong

> ✅ **已完成 — 执行时跳过本 Task** — `PageGuard.readLong/writeLong` 已存在于 `src/main/java/cn/zhangyis/db/storage/buf/PageGuard.java`，对应 3 个测试已存在于 `PageGuardTest.java`。下方代码仅供核对，**切勿重复插入方法或测试方法（重复定义会编译失败）**。

**Files:**
- Modify: `src/main/java/cn/zhangyis/db/storage/buf/PageGuard.java`
- Modify (test): `src/test/java/cn/zhangyis/db/storage/buf/PageGuardTest.java`

- [ ] **Step 1: 在 `PageGuardTest.java` 加 3 个测试方法**（插在最后一个 `@Test` 方法之后、类结束 `}` 之前）：

```java
    @Test
    void shouldRoundTripLongUnderExclusive() {
        BufferFrame frame = new BufferFrame(PS);
        PageGuard guard = exclusiveGuard(frame, (f, w) -> { });
        guard.writeLong(16, 0x1122334455667788L);
        assertEquals(0x1122334455667788L, guard.readLong(16));
        guard.close();
    }

    @Test
    void shouldRejectWriteLongUnderShared() {
        BufferFrame frame = new BufferFrame(PS);
        PageGuard guard = sharedGuard(frame, (f, w) -> { });
        assertThrows(DatabaseValidationException.class, () -> guard.writeLong(0, 1L));
        guard.close();
    }

    @Test
    void shouldRejectLongOutOfBounds() {
        BufferFrame frame = new BufferFrame(PS);
        PageGuard guard = exclusiveGuard(frame, (f, w) -> { });
        assertThrows(DatabaseValidationException.class, () -> guard.readLong(PS.bytes() - 4));
        guard.close();
    }
```

- [ ] **Step 2: 运行确认失败** — `--tests "cn.zhangyis.db.storage.buf.PageGuardTest"`，编译失败（readLong/writeLong 不存在）。

- [ ] **Step 3: 在 `PageGuard.java` 的 `writeInt` 方法之后插入两个方法**（紧跟 `public void writeInt(...) { ... }` 的闭合括号后）：

```java
    /** 读 8 字节大端长整数。S/X 均可。 */
    public long readLong(int offset) {
        ensureOpen();
        checkBounds(offset, Long.BYTES);
        return frame.buffer.getLong(offset);
    }

    /** 写 8 字节大端长整数。要求 EXCLUSIVE。 */
    public void writeLong(int offset, long value) {
        requireExclusive();
        checkBounds(offset, Long.BYTES);
        frame.buffer.putLong(offset, value);
        wrote = true;
    }
```

- [ ] **Step 4: 运行确认通过**（PageGuardTest 全部）。不提交。

---

## Task 1.5: MTR 同页 S→X 升级拒绝

> ⚠️ **现有注释与实现不一致，本 Task 是补齐实现** — `MiniTransaction.java` 的类/方法 Javadoc（约 16、56 行）**已声称**"同页禁 S→X 升级（ReentrantReadWriteLock 无升级，会自死锁）"，但 `fix()`（约 75–83 行）**并未实现该检测**，仅直接 `pool.getPage(pageId, mode)` 后 `memo.push(guard)`；`MtrMemo` 也仍是裸 `Deque<AutoCloseable>`、不带 page/mode 元数据。本 Task 才真正落地该约束，使注释与实现一致。实现要点：① `MtrMemo` 升级为带 `(resource, pageId, mode)` 的栈并提供 `holds(pageId, mode)`；② `fix()` 在取 latch 前判定 `mode==X && holds(pageId,S) && !holds(pageId,X)` 才拒绝（X→S 降级、已持 X 再取 X 均放行）。完成后顺手核对 `MiniTransaction` 的 Javadoc 仍与新实现相符，避免再次出现"注释先行、实现滞后"。

**Files:**
- Modify: `src/main/java/cn/zhangyis/db/storage/mtr/MtrMemo.java`
- Modify: `src/main/java/cn/zhangyis/db/storage/mtr/MiniTransaction.java`
- Modify (test): `src/test/java/cn/zhangyis/db/storage/mtr/MiniTransactionTest.java`

- [x] **Step 1: 在 `MiniTransactionTest.java` 加 2 个测试**

```java
    @Test
    void sameMtrShouldRejectSharedThenExclusiveOnSamePage() {
        try (PageStore store = openStore(8)) {
            BufferPool pool = new LruBufferPool(store, PS, 4);
            MiniTransaction mtr = activeMtr(1);
            mtr.getPage(pool, page(0), PageLatchMode.SHARED);
            assertThrows(MtrStateException.class, () -> mtr.getPage(pool, page(0), PageLatchMode.EXCLUSIVE));
            mtr.rollbackUncommitted();
            pool.close();
        }
    }

    @Test
    void exclusiveAfterSavepointReleaseShouldBeAllowed() {
        try (PageStore store = openStore(8)) {
            BufferPool pool = new LruBufferPool(store, PS, 4);
            MiniTransaction mtr = activeMtr(1);
            MtrSavepoint sp = mtr.savepoint();
            mtr.getPage(pool, page(0), PageLatchMode.SHARED);
            mtr.rollbackToSavepoint(sp);
            PageGuard g = mtr.getPage(pool, page(0), PageLatchMode.EXCLUSIVE);
            g.writeInt(0, 0x12345678);
            mtr.commit();
            pool.close();
        }
    }
```

- [x] **Step 2: 运行确认失败** — `--tests "cn.zhangyis.db.storage.mtr.MiniTransactionTest"`，第一个测试在当前实现中会阻塞或失败。若执行环境可能阻塞，不直接运行旧实现；先做静态确认 `MiniTransaction.fix` 未检测同页 S→X，再实现检测。

- [x] **Step 3: 改 `MtrMemo`，从裸 `AutoCloseable` 栈升级为带 page/mode 元数据的栈**

实现要点：
- 增加包内 record：`record MemoEntry(AutoCloseable resource, PageId pageId, PageLatchMode mode) {}`。
- `push(AutoCloseable)` 保留给非 page 资源；新增 `pushPageGuard(PageGuard guard, PageId pageId, PageLatchMode mode)`。
- `releaseTo()` / `releaseAll()` 仍只按 LIFO close `entry.resource()`，由于 entry 被 pop，savepoint 释放后 `holds(pageId, mode)` 自然不再看到已释放 guard。
- 新增 `boolean holds(PageId pageId, PageLatchMode mode)`：遍历当前 stack，只匹配 `pageId` 相等且 `mode` 相等的 entry。
- 所有 null 参数用 `DatabaseValidationException`。

- [x] **Step 4: 改 `MiniTransaction.fix`，在真正取 latch 前拒绝 S→X**

```java
if (mode == PageLatchMode.EXCLUSIVE
        && memo.holds(pageId, PageLatchMode.SHARED)
        && !memo.holds(pageId, PageLatchMode.EXCLUSIVE)) {
    throw new MtrStateException("S to X page latch upgrade is forbidden in one MTR: " + pageId);
}
PageGuard guard = existing ? pool.getPage(pageId, mode) : pool.newPage(pageId, mode);
memo.pushPageGuard(guard, pageId, mode);
return guard;
```

说明：X→S→X 允许，因为当前线程已持 X，可重入；S→X 拒绝，避免 `ReentrantReadWriteLock` 升级自死锁。这个检测必须发生在 `pool.getPage()` 前，否则阻塞已经发生。

- [x] **Step 5: 运行确认通过**（MiniTransactionTest 全部）。不提交。

> 📌 实现备注：全量回归时发现一处**与本 Task 无关的既有失败** `FileAddressTest.shouldRoundTripThroughPageGuard`（Task 2 产物）——其 try-with-resources 把 `pool` 声明在 `PageStore` 之前，关闭顺序反了，导致 `pool.close()` flush 脏页时 `PageStore` 已关闭，抛 `TablespaceNotOpenException`。已按本计划"通用约定"（先声明 `PageStore s = store` 再声明 `pool`）顺手修正该一行，使全量 `gradle test` 绿。

---

## Task 2: PageLayouts + FileAddress

> ✅ **已完成 — 执行时跳过本 Task** — `fsp/PageLayouts.java`、`fsp/FileAddress.java`、`fsp/FileAddressTest.java` 均已存在。下方代码仅供核对，**切勿用 Write 重建这些文件**。差异：现有 `FileAddress.of` 对 null `pageNo` 用 `Objects.requireNonNull`（抛 NPE，而非示例中的 `DatabaseValidationException`），`hashCode` 用 `Objects.hash`；功能等价，不影响现有测试。

**Files:**
- Create: `src/main/java/cn/zhangyis/db/storage/fsp/PageLayouts.java`
- Create: `src/main/java/cn/zhangyis/db/storage/fsp/FileAddress.java`
- Test: `src/test/java/cn/zhangyis/db/storage/fsp/FileAddressTest.java`

- [ ] **Step 1: 写失败测试**

```java
package cn.zhangyis.db.storage.fsp;

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
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * FileAddress 测试：of/NULL/isNull/equals、(0,0) 碰撞拒绝；writeTo/readFrom 经真实 PageGuard 往返（含 NULL）。
 */
class FileAddressTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);

    @TempDir
    Path dir;

    @Test
    void ofAndNullSemantics() {
        FileAddress a = FileAddress.of(PageNo.of(5), 100);
        assertFalse(a.isNull());
        assertEquals(PageNo.of(5), a.pageNo());
        assertEquals(100, a.offset());
        assertTrue(FileAddress.NULL.isNull());
        assertEquals(FileAddress.NULL, FileAddress.NULL);
        assertEquals(FileAddress.of(PageNo.of(5), 100), a);
    }

    @Test
    void shouldRejectReservedZeroAndNegativeOffset() {
        assertThrows(DatabaseValidationException.class, () -> FileAddress.of(PageNo.of(0), 0));
        assertThrows(DatabaseValidationException.class, () -> FileAddress.of(PageNo.of(5), -1));
        assertThrows(DatabaseValidationException.class, FileAddress.NULL::pageNo);
    }

    @Test
    void shouldRoundTripThroughPageGuard() {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(4));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 4)) {
            try (PageGuard g = pool.getPage(PageId.of(SPACE, PageNo.of(0)), PageLatchMode.EXCLUSIVE)) {
                FileAddress a = FileAddress.of(PageNo.of(7), 250);
                a.writeTo(g, 100);
                assertEquals(a, FileAddress.readFrom(g, 100));

                FileAddress.NULL.writeTo(g, 200);
                assertTrue(FileAddress.readFrom(g, 200).isNull());
            }
        }
    }
}
```

- [ ] **Step 2: 运行确认失败。**

- [ ] **Step 3: 写 PageLayouts**

```java
package cn.zhangyis.db.storage.fsp;

/**
 * fsp 元数据页共享布局常量。每页首预留 FIL_PAGE_DATA 字节给未来 FilePageHeader（本切片填零、不解析），
 * 元数据字段从该偏移之后开始。
 */
final class PageLayouts {

    private PageLayouts() {
    }

    /** 页首预留给 FilePageHeader（checksum/pageLSN 等）的字节数；首版填零。 */
    static final int FIL_PAGE_DATA = 38;
}
```

- [ ] **Step 4: 写 FileAddress**

```java
package cn.zhangyis.db.storage.fsp;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.storage.buf.PageGuard;

/**
 * 页内地址（InnoDB fil_addr_t 最小版）：PageNo + 页内 offset，或 NULL 哨兵。只存取、不走链（链表算法属 fsp 分配切片）。
 *
 * <p>编码 12 字节：pageNo(long 8) + offset(int 4)。**NULL = 全零编码**（pageNoRaw==0 且 offset==0），
 * 使刚 create 的全零页的 list 指针天然解码为 NULL；真实节点偏移恒 ≥ FIL_PAGE_DATA，绝不落在 (page0,offset0)，无碰撞。
 */
public final class FileAddress {

    /** NULL 哨兵：全零编码，表示空 free-list 头或链尾。 */
    public static final FileAddress NULL = new FileAddress(null, 0, true);

    private final PageNo pageNo;
    private final int offset;
    private final boolean nil;

    private FileAddress(PageNo pageNo, int offset, boolean nil) {
        this.pageNo = pageNo;
        this.offset = offset;
        this.nil = nil;
    }

    /**
     * 创建非空页内地址。
     *
     * @param pageNo 所在页。
     * @param offset 页内偏移（≥0）；(page0,offset0) 保留作 NULL，拒绝。
     * @return 页内地址。
     */
    public static FileAddress of(PageNo pageNo, int offset) {
        if (pageNo == null) {
            throw new DatabaseValidationException("file address pageNo must not be null");
        }
        if (offset < 0) {
            throw new DatabaseValidationException("file address offset must be non-negative: " + offset);
        }
        if (pageNo.value() == 0 && offset == 0) {
            throw new DatabaseValidationException("(page0, offset0) is reserved as NULL; use FileAddress.NULL");
        }
        return new FileAddress(pageNo, offset, false);
    }

    /** 是否为 NULL 哨兵。 */
    public boolean isNull() {
        return nil;
    }

    /** 所在页；NULL 调用抛异常。 */
    public PageNo pageNo() {
        if (nil) {
            throw new DatabaseValidationException("NULL file address has no pageNo");
        }
        return pageNo;
    }

    /** 页内偏移；NULL 调用抛异常。 */
    public int offset() {
        if (nil) {
            throw new DatabaseValidationException("NULL file address has no offset");
        }
        return offset;
    }

    /** 编码 12 字节写入 guard（要求 X latch）：pageNoRaw(long)+offset(int)。NULL→全零。 */
    public void writeTo(PageGuard guard, int at) {
        if (nil) {
            guard.writeLong(at, 0L);
            guard.writeInt(at + Long.BYTES, 0);
        } else {
            guard.writeLong(at, pageNo.value());
            guard.writeInt(at + Long.BYTES, offset);
        }
    }

    /** 从 guard 解码 12 字节。全零→NULL。 */
    public static FileAddress readFrom(PageGuard guard, int at) {
        long pageNoRaw = guard.readLong(at);
        int off = guard.readInt(at + Long.BYTES);
        if (pageNoRaw == 0 && off == 0) {
            return NULL;
        }
        return of(PageNo.of(pageNoRaw), off);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof FileAddress that)) {
            return false;
        }
        if (nil || that.nil) {
            return nil == that.nil;
        }
        return offset == that.offset && pageNo.equals(that.pageNo);
    }

    @Override
    public int hashCode() {
        return nil ? 0 : 31 * pageNo.hashCode() + offset;
    }

    @Override
    public String toString() {
        return nil ? "FileAddress.NULL" : "FileAddress(" + pageNo.value() + "," + offset + ")";
    }
}
```

- [ ] **Step 5: 运行确认通过**，不提交。

---

## Task 3: ExtentState + SegmentPurpose + FspMetadataException

> ✅ **已完成 — 全部 Step 通过** — `ExtentState.java`、`SegmentPurpose.java`、`FspMetadataException.java` 已创建，`FspEnumTest` 绿（RED→GREEN：先确认编译失败，再补三类后通过）；全量 `gradle test` 绿、GitNexus 索引已刷新。未提交。

**Files:**
- Create: `src/main/java/cn/zhangyis/db/storage/fsp/ExtentState.java`
- Create: `src/main/java/cn/zhangyis/db/storage/fsp/SegmentPurpose.java`
- Create: `src/main/java/cn/zhangyis/db/storage/fsp/FspMetadataException.java`
- Test: `src/test/java/cn/zhangyis/db/storage/fsp/FspEnumTest.java`

- [ ] **Step 1: 写失败测试**

```java
package cn.zhangyis.db.storage.fsp;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/**
 * 钉死 XDES/INODE 落盘依赖的枚举 ordinal（改序会破坏已写盘数据），并固定 FspMetadataException 可恢复分类。
 */
class FspEnumTest {

    @Test
    void extentStateOrdinalsAreStable() {
        assertEquals(0, ExtentState.FREE.ordinal());
        assertEquals(1, ExtentState.FREE_FRAG.ordinal());
        assertEquals(2, ExtentState.FULL_FRAG.ordinal());
        assertEquals(3, ExtentState.FSEG.ordinal());
        assertEquals(4, ExtentState.FSEG_FRAG.ordinal());
    }

    @Test
    void segmentPurposeOrdinalsAreStable() {
        assertEquals(0, SegmentPurpose.INDEX_LEAF.ordinal());
        assertEquals(1, SegmentPurpose.INDEX_NON_LEAF.ordinal());
        assertEquals(2, SegmentPurpose.LOB.ordinal());
        assertEquals(3, SegmentPurpose.UNDO.ordinal());
        assertEquals(4, SegmentPurpose.SYSTEM.ordinal());
    }

    @Test
    void metadataExceptionIsRecoverable() {
        assertInstanceOf(DatabaseRuntimeException.class, new FspMetadataException("x"));
    }
}
```

- [ ] **Step 2: 运行确认失败。**

- [ ] **Step 3: 写 ExtentState**

```java
package cn.zhangyis.db.storage.fsp;

/**
 * Extent 分配状态（设计 §5.4）。ordinal 落盘到 XDES，顺序不可改（FspEnumTest 钉死）。FREE 必须为 ordinal 0，
 * 使零初始化的 XDES entry 解码为 FREE。
 */
public enum ExtentState {
    /** 完全空闲。 */
    FREE,
    /** 可按 fragment page 分配。 */
    FREE_FRAG,
    /** fragment page 已满。 */
    FULL_FRAG,
    /** 完整属于某 segment。 */
    FSEG,
    /** 属于某 segment 但按 fragment 管理。 */
    FSEG_FRAG
}
```

- [ ] **Step 4: 写 SegmentPurpose**

```java
package cn.zhangyis.db.storage.fsp;

/**
 * Segment 用途（设计 §5.5）。ordinal 落盘到 INODE，顺序不可改（FspEnumTest 钉死）。
 */
public enum SegmentPurpose {
    /** 叶子页 segment。 */
    INDEX_LEAF,
    /** 非叶子页 segment。 */
    INDEX_NON_LEAF,
    /** 大字段溢出页 segment。 */
    LOB,
    /** undo segment。 */
    UNDO,
    /** 系统 segment。 */
    SYSTEM
}
```

- [ ] **Step 5: 写 FspMetadataException**

```java
package cn.zhangyis.db.storage.fsp;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * fsp 元数据约束异常（设计 §17 风格）：extent 超出 page 0 首批 XDES 区、普通路径误用 extent0、坏枚举 ordinal、
 * nextSegmentId 破坏 0 哨兵不变量、读空 inode 槽、无空 inode/fragment 槽等。
 * 可恢复运行时异常。
 */
public class FspMetadataException extends DatabaseRuntimeException {

    public FspMetadataException(String message) {
        super(message);
    }

    public FspMetadataException(String message, Throwable cause) {
        super(message, cause);
    }
}
```

- [ ] **Step 6: 运行确认通过**，不提交。

---

## Task 4: SpaceHeader（Layout + Snapshot + Repository）

> ✅ **已完成 — 全部 Step 通过** — `SpaceHeaderLayout.java`、`SpaceHeaderSnapshot.java`、`SpaceHeaderRepository.java` 已创建，`SpaceHeaderRepositoryTest` 绿（RED→GREEN：先确认编译失败，再补三类后通过）；全量 `gradle test` 绿、GitNexus 索引已刷新。未提交。

**Files:**
- Create: `src/main/java/cn/zhangyis/db/storage/fsp/SpaceHeaderLayout.java`
- Create: `src/main/java/cn/zhangyis/db/storage/fsp/SpaceHeaderSnapshot.java`
- Create: `src/main/java/cn/zhangyis/db/storage/fsp/SpaceHeaderRepository.java`
- Test: `src/test/java/cn/zhangyis/db/storage/fsp/SpaceHeaderRepositoryTest.java`

- [ ] **Step 1: 写失败测试**

```java
package cn.zhangyis.db.storage.fsp;

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
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * SpaceHeaderRepository 集成测试：initialize→read 往返、setter→read、allocateNextSegmentId 自增与 0 哨兵拒绝；
 * 本切片 no-redo，不做 crash recovery 断言。
 */
class SpaceHeaderRepositoryTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(7);

    @TempDir
    Path dir;

    private SpaceHeaderSnapshot freshHeader() {
        return new SpaceHeaderSnapshot(SPACE, PS, 0,
                PageNo.of(64), PageNo.of(64), 1L,
                FileAddress.NULL, FileAddress.NULL, FileAddress.NULL,
                PageNo.of(2), 0L, 80046, 1L);
    }

    @Test
    void initializeThenReadRoundTrips() {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(64));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 8)) {
            SpaceHeaderRepository repo = new SpaceHeaderRepository(pool);
            MiniTransactionManager mgr = new MiniTransactionManager();

            MiniTransaction w = mgr.begin();
            repo.initialize(w, freshHeader());
            mgr.commit(w);

            MiniTransaction r = mgr.begin();
            SpaceHeaderSnapshot got = repo.read(r, SPACE);
            mgr.commit(r);

            assertEquals(freshHeader(), got);
        }
    }

    @Test
    void settersUpdateFields() {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(64));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 8)) {
            SpaceHeaderRepository repo = new SpaceHeaderRepository(pool);
            MiniTransactionManager mgr = new MiniTransactionManager();
            MiniTransaction w = mgr.begin();
            repo.initialize(w, freshHeader());
            repo.setCurrentSizeInPages(w, SPACE, PageNo.of(128));
            repo.setFreeLimitPageNo(w, SPACE, PageNo.of(128));
            repo.setFreeExtentListHead(w, SPACE, FileAddress.of(PageNo.of(0), 200));
            mgr.commit(w);

            MiniTransaction r = mgr.begin();
            SpaceHeaderSnapshot got = repo.read(r, SPACE);
            mgr.commit(r);
            assertEquals(PageNo.of(128), got.currentSizeInPages());
            assertEquals(PageNo.of(128), got.freeLimitPageNo());
            assertEquals(FileAddress.of(PageNo.of(0), 200), got.freeExtentListHead());
        }
    }

    @Test
    void allocateNextSegmentIdIncrements() {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(64));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 8)) {
            SpaceHeaderRepository repo = new SpaceHeaderRepository(pool);
            MiniTransactionManager mgr = new MiniTransactionManager();
            MiniTransaction w = mgr.begin();
            repo.initialize(w, freshHeader());
            assertEquals(1L, repo.allocateNextSegmentId(w, SPACE));
            assertEquals(2L, repo.allocateNextSegmentId(w, SPACE));
            mgr.commit(w);
        }
    }

    @Test
    void shouldRejectZeroNextSegmentId() {
        assertThrows(DatabaseValidationException.class, () -> new SpaceHeaderSnapshot(SPACE, PS, 0,
                PageNo.of(64), PageNo.of(64), 0L,
                FileAddress.NULL, FileAddress.NULL, FileAddress.NULL,
                PageNo.of(2), 0L, 80046, 1L));
    }

    @Test
    void allocateNextSegmentIdRejectsZeroStoredOnPage() {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(64));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 8)) {
            SpaceHeaderRepository repo = new SpaceHeaderRepository(pool);
            MiniTransactionManager mgr = new MiniTransactionManager();
            MiniTransaction init = mgr.begin();
            repo.initialize(init, freshHeader());
            mgr.commit(init);

            MiniTransaction corrupt = mgr.begin();
            PageGuard g = corrupt.getPage(pool, PageId.of(SPACE, PageNo.of(0)), PageLatchMode.EXCLUSIVE);
            g.writeLong(SpaceHeaderLayout.NEXT_SEGMENT_ID, 0L);
            mgr.commit(corrupt);

            MiniTransaction r = mgr.begin();
            assertThrows(FspMetadataException.class, () -> repo.allocateNextSegmentId(r, SPACE));
            mgr.commit(r);
        }
    }
}
```

- [ ] **Step 2: 运行确认失败。**

- [ ] **Step 3: 写 SpaceHeaderLayout**

```java
package cn.zhangyis.db.storage.fsp;

/**
 * SpaceHeaderPage（page 0）字段偏移（相对页首，均在 FIL_PAGE_DATA 之后）。XDES entries 从 XDES_BASE 起。
 */
final class SpaceHeaderLayout {

    private SpaceHeaderLayout() {
    }

    static final int SPACE_ID = PageLayouts.FIL_PAGE_DATA;            // 38 int
    static final int PAGE_SIZE_BYTES = SPACE_ID + 4;                  // 42 int
    static final int SPACE_FLAGS = PAGE_SIZE_BYTES + 4;              // 46 int
    static final int CURRENT_SIZE = SPACE_FLAGS + 4;                // 50 long
    static final int FREE_LIMIT = CURRENT_SIZE + 8;                 // 58 long
    static final int NEXT_SEGMENT_ID = FREE_LIMIT + 8;             // 66 long
    static final int FREE_EXTENT_LIST_HEAD = NEXT_SEGMENT_ID + 8;  // 74 FileAddress(12)
    static final int FREE_FRAG_LIST_HEAD = FREE_EXTENT_LIST_HEAD + 12; // 86
    static final int FULL_FRAG_LIST_HEAD = FREE_FRAG_LIST_HEAD + 12;   // 98
    static final int FIRST_INODE_PAGE = FULL_FRAG_LIST_HEAD + 12;     // 110 long
    static final int SDI_ROOT = FIRST_INODE_PAGE + 8;               // 118 long
    static final int SERVER_VERSION = SDI_ROOT + 8;                // 126 int
    static final int SPACE_VERSION = SERVER_VERSION + 4;           // 130 long (ends 138)

    /** XDES entries 内嵌 page 0 的起始偏移（header 之后预留到 200）。 */
    static final int XDES_BASE = 200;
}
```

- [ ] **Step 4: 写 SpaceHeaderSnapshot**

```java
package cn.zhangyis.db.storage.fsp;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;

/**
 * SpaceHeaderPage 字段快照（§6.2）。新建表空间应取 nextSegmentId=1（0 留作 XDES 无主哨兵）、firstInodePageNo=2。
 * 初始化表空间时还必须在同一 MTR 内调用 ExtentDescriptorRepository.reserveSystemExtent，避免 extent 0 被当作普通 FREE。
 */
public record SpaceHeaderSnapshot(
        SpaceId spaceId,
        PageSize pageSize,
        int spaceFlags,
        PageNo currentSizeInPages,
        PageNo freeLimitPageNo,
        long nextSegmentId,
        FileAddress freeExtentListHead,
        FileAddress freeFragExtentListHead,
        FileAddress fullFragExtentListHead,
        PageNo firstInodePageNo,
        long sdiRootPageNo,
        int serverVersion,
        long spaceVersion) {

    public SpaceHeaderSnapshot {
        if (spaceId == null || pageSize == null || currentSizeInPages == null || freeLimitPageNo == null
                || freeExtentListHead == null || freeFragExtentListHead == null || fullFragExtentListHead == null
                || firstInodePageNo == null) {
            throw new DatabaseValidationException("space header snapshot fields must not be null");
        }
        if (nextSegmentId <= 0) {
            throw new DatabaseValidationException("next segment id must be positive; 0 is reserved as owner sentinel");
        }
    }
}
```

- [ ] **Step 5: 写 SpaceHeaderRepository**

```java
package cn.zhangyis.db.storage.fsp;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.mtr.MiniTransaction;

/**
 * SpaceHeaderPage（page 0）仓储（设计 §6.2）。经 MTR 持 page 0 latch 读写 header 字段；写须 X latch。
 * 物理空间账本入口：size/freeLimit/free-extent-list 头/nextSegmentId。字段 setter 幂等；allocateNextSegmentId 为读改写、非幂等。
 */
public final class SpaceHeaderRepository {

    private final BufferPool pool;

    public SpaceHeaderRepository(BufferPool pool) {
        if (pool == null) {
            throw new DatabaseValidationException("buffer pool must not be null");
        }
        this.pool = pool;
    }

    private static PageId page0(SpaceId spaceId) {
        return PageId.of(spaceId, PageNo.of(0));
    }

    /** 写入全部 header 字段（X）。普通 XDES entry 可留零态；extent 0 必须由 ExtentDescriptorRepository.reserveSystemExtent 单独保留。 */
    public void initialize(MiniTransaction mtr, SpaceHeaderSnapshot h) {
        requireMtr(mtr);
        if (h == null) {
            throw new DatabaseValidationException("space header snapshot must not be null");
        }
        PageGuard g = mtr.getPage(pool, page0(h.spaceId()), PageLatchMode.EXCLUSIVE);
        g.writeInt(SpaceHeaderLayout.SPACE_ID, h.spaceId().value());
        g.writeInt(SpaceHeaderLayout.PAGE_SIZE_BYTES, h.pageSize().bytes());
        g.writeInt(SpaceHeaderLayout.SPACE_FLAGS, h.spaceFlags());
        g.writeLong(SpaceHeaderLayout.CURRENT_SIZE, h.currentSizeInPages().value());
        g.writeLong(SpaceHeaderLayout.FREE_LIMIT, h.freeLimitPageNo().value());
        g.writeLong(SpaceHeaderLayout.NEXT_SEGMENT_ID, h.nextSegmentId());
        h.freeExtentListHead().writeTo(g, SpaceHeaderLayout.FREE_EXTENT_LIST_HEAD);
        h.freeFragExtentListHead().writeTo(g, SpaceHeaderLayout.FREE_FRAG_LIST_HEAD);
        h.fullFragExtentListHead().writeTo(g, SpaceHeaderLayout.FULL_FRAG_LIST_HEAD);
        g.writeLong(SpaceHeaderLayout.FIRST_INODE_PAGE, h.firstInodePageNo().value());
        g.writeLong(SpaceHeaderLayout.SDI_ROOT, h.sdiRootPageNo());
        g.writeInt(SpaceHeaderLayout.SERVER_VERSION, h.serverVersion());
        g.writeLong(SpaceHeaderLayout.SPACE_VERSION, h.spaceVersion());
    }

    /** 读出全部 header 字段（S）。 */
    public SpaceHeaderSnapshot read(MiniTransaction mtr, SpaceId spaceId) {
        requireMtr(mtr);
        requireSpace(spaceId);
        PageGuard g = mtr.getPage(pool, page0(spaceId), PageLatchMode.SHARED);
        return new SpaceHeaderSnapshot(
                SpaceId.of(g.readInt(SpaceHeaderLayout.SPACE_ID)),
                PageSize.ofBytes(g.readInt(SpaceHeaderLayout.PAGE_SIZE_BYTES)),
                g.readInt(SpaceHeaderLayout.SPACE_FLAGS),
                PageNo.of(g.readLong(SpaceHeaderLayout.CURRENT_SIZE)),
                PageNo.of(g.readLong(SpaceHeaderLayout.FREE_LIMIT)),
                g.readLong(SpaceHeaderLayout.NEXT_SEGMENT_ID),
                FileAddress.readFrom(g, SpaceHeaderLayout.FREE_EXTENT_LIST_HEAD),
                FileAddress.readFrom(g, SpaceHeaderLayout.FREE_FRAG_LIST_HEAD),
                FileAddress.readFrom(g, SpaceHeaderLayout.FULL_FRAG_LIST_HEAD),
                PageNo.of(g.readLong(SpaceHeaderLayout.FIRST_INODE_PAGE)),
                g.readLong(SpaceHeaderLayout.SDI_ROOT),
                g.readInt(SpaceHeaderLayout.SERVER_VERSION),
                g.readLong(SpaceHeaderLayout.SPACE_VERSION));
    }

    public void setCurrentSizeInPages(MiniTransaction mtr, SpaceId spaceId, PageNo value) {
        writeLongField(mtr, spaceId, SpaceHeaderLayout.CURRENT_SIZE, requireValue(value).value());
    }

    public void setFreeLimitPageNo(MiniTransaction mtr, SpaceId spaceId, PageNo value) {
        writeLongField(mtr, spaceId, SpaceHeaderLayout.FREE_LIMIT, requireValue(value).value());
    }

    public void setFirstInodePageNo(MiniTransaction mtr, SpaceId spaceId, PageNo value) {
        writeLongField(mtr, spaceId, SpaceHeaderLayout.FIRST_INODE_PAGE, requireValue(value).value());
    }

    public void setFreeExtentListHead(MiniTransaction mtr, SpaceId spaceId, FileAddress head) {
        writeAddrField(mtr, spaceId, SpaceHeaderLayout.FREE_EXTENT_LIST_HEAD, head);
    }

    public void setFreeFragExtentListHead(MiniTransaction mtr, SpaceId spaceId, FileAddress head) {
        writeAddrField(mtr, spaceId, SpaceHeaderLayout.FREE_FRAG_LIST_HEAD, head);
    }

    public void setFullFragExtentListHead(MiniTransaction mtr, SpaceId spaceId, FileAddress head) {
        writeAddrField(mtr, spaceId, SpaceHeaderLayout.FULL_FRAG_LIST_HEAD, head);
    }

    /** 读 nextSegmentId、写回 +1、返回旧值（segment id 分配，非幂等，调用方一个 MTR 内使用）。 */
    public long allocateNextSegmentId(MiniTransaction mtr, SpaceId spaceId) {
        requireMtr(mtr);
        requireSpace(spaceId);
        PageGuard g = mtr.getPage(pool, page0(spaceId), PageLatchMode.EXCLUSIVE);
        long current = g.readLong(SpaceHeaderLayout.NEXT_SEGMENT_ID);
        if (current <= 0) {
            throw new FspMetadataException("invalid next segment id on disk: " + current);
        }
        g.writeLong(SpaceHeaderLayout.NEXT_SEGMENT_ID, current + 1);
        return current;
    }

    private void writeLongField(MiniTransaction mtr, SpaceId spaceId, int offset, long value) {
        requireMtr(mtr);
        requireSpace(spaceId);
        PageGuard g = mtr.getPage(pool, page0(spaceId), PageLatchMode.EXCLUSIVE);
        g.writeLong(offset, value);
    }

    private void writeAddrField(MiniTransaction mtr, SpaceId spaceId, int offset, FileAddress head) {
        requireMtr(mtr);
        requireSpace(spaceId);
        if (head == null) {
            throw new DatabaseValidationException("list head must not be null (use FileAddress.NULL)");
        }
        PageGuard g = mtr.getPage(pool, page0(spaceId), PageLatchMode.EXCLUSIVE);
        head.writeTo(g, offset);
    }

    private static void requireMtr(MiniTransaction mtr) {
        if (mtr == null) {
            throw new DatabaseValidationException("mini transaction must not be null");
        }
    }

    private static void requireSpace(SpaceId spaceId) {
        if (spaceId == null) {
            throw new DatabaseValidationException("space id must not be null");
        }
    }

    private static PageNo requireValue(PageNo value) {
        if (value == null) {
            throw new DatabaseValidationException("page no value must not be null");
        }
        return value;
    }
}
```

- [ ] **Step 6: 运行确认通过**，不提交。

---

## Task 5: XDES（Layout + ExtentDescriptor + Repository）

> ✅ **已完成 — 全部 Step 通过** — `ExtentDescriptorLayout.java`、`ExtentDescriptor.java`、`ExtentDescriptorRepository.java` 已创建，`ExtentDescriptorRepositoryTest` 绿（RED→GREEN：先确认编译失败，再补三类后通过；含零态 FREE、extent0 系统保留、bitmap、超首批/越界/坏 ordinal 拒绝）；全量 `gradle test` 绿、GitNexus 索引已刷新。未提交。

**Files:**
- Create: `src/main/java/cn/zhangyis/db/storage/fsp/ExtentDescriptorLayout.java`
- Create: `src/main/java/cn/zhangyis/db/storage/fsp/ExtentDescriptor.java`
- Create: `src/main/java/cn/zhangyis/db/storage/fsp/ExtentDescriptorRepository.java`
- Test: `src/test/java/cn/zhangyis/db/storage/fsp/ExtentDescriptorRepositoryTest.java`

- [ ] **Step 1: 写失败测试**

```java
package cn.zhangyis.db.storage.fsp;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.ExtentId;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SegmentId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.fil.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.PageStore;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * ExtentDescriptorRepository 集成测试：普通零初始化即 FREE/无主/NULL；extent0 系统保留且普通 mutator 拒绝；
 * state/owner/prev/next 往返；bitmap set/clear/get；超首批、越界和坏 ordinal 拒绝。
 */
class ExtentDescriptorRepositoryTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(7);

    @TempDir
    Path dir;

    private interface Body {
        void run(ExtentDescriptorRepository repo, MiniTransaction mtr);
    }

    private void withRepo(Body body) {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(64));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 8)) {
            ExtentDescriptorRepository repo = new ExtentDescriptorRepository(pool, PS);
            MiniTransactionManager mgr = new MiniTransactionManager();
            MiniTransaction mtr = mgr.begin();
            body.run(repo, mtr);
            mgr.commit(mtr);
        }
    }

    private ExtentId ext(long no) {
        return ExtentId.of(SPACE, no);
    }

    @Test
    void zeroInitDecodesAsFreeUnownedNull() {
        withRepo((repo, mtr) -> {
            ExtentDescriptor d = repo.read(mtr, ext(3));
            assertEquals(ExtentState.FREE, d.state());
            assertTrue(d.ownerSegment().isEmpty());
            assertTrue(d.prev().isNull());
            assertTrue(d.next().isNull());
            assertFalse(repo.isPageAllocated(mtr, ext(3), 0));
        });
    }

    @Test
    void reserveSystemExtentMarksFixedPagesAndRejectsInitFree() {
        withRepo((repo, mtr) -> {
            repo.reserveSystemExtent(mtr, SPACE);
            ExtentDescriptor d = repo.read(mtr, ext(0));
            assertEquals(ExtentState.FSEG_FRAG, d.state());
            assertTrue(d.ownerSegment().isEmpty());
            assertTrue(repo.isPageAllocated(mtr, ext(0), 0));
            assertTrue(repo.isPageAllocated(mtr, ext(0), 1));
            assertTrue(repo.isPageAllocated(mtr, ext(0), 2));
            assertTrue(repo.isPageAllocated(mtr, ext(0), 3));
            assertThrows(FspMetadataException.class, () -> repo.initFree(mtr, ext(0)));
            assertThrows(FspMetadataException.class, () -> repo.writeState(mtr, ext(0), ExtentState.FREE));
            assertThrows(FspMetadataException.class, () -> repo.writeOwner(mtr, ext(0), Optional.of(SegmentId.of(1))));
            assertThrows(FspMetadataException.class, () -> repo.setPageAllocated(mtr, ext(0), 4, true));
        });
    }

    @Test
    void stateOwnerPrevNextRoundTrip() {
        withRepo((repo, mtr) -> {
            repo.writeState(mtr, ext(2), ExtentState.FSEG);
            repo.writeOwner(mtr, ext(2), Optional.of(SegmentId.of(5)));
            repo.writePrev(mtr, ext(2), FileAddress.of(PageNo.of(0), 268));
            repo.writeNext(mtr, ext(2), FileAddress.NULL);

            ExtentDescriptor d = repo.read(mtr, ext(2));
            assertEquals(ExtentState.FSEG, d.state());
            assertEquals(Optional.of(SegmentId.of(5)), d.ownerSegment());
            assertEquals(FileAddress.of(PageNo.of(0), 268), d.prev());
            assertTrue(d.next().isNull());
        });
    }

    @Test
    void shouldRejectOwnerSegmentZero() {
        withRepo((repo, mtr) ->
                assertThrows(DatabaseValidationException.class,
                        () -> repo.writeOwner(mtr, ext(2), Optional.of(SegmentId.of(0)))));
    }

    @Test
    void pageSizeBoundariesUsePagesPerExtent() {
        assertEquals(256, PageSize.ofBytes(4 * 1024).pagesPerExtent());
        assertEquals(128, PageSize.ofBytes(8 * 1024).pagesPerExtent());
        assertEquals(64, PageSize.ofBytes(16 * 1024).pagesPerExtent());
    }

    @Test
    void bitmapSetClearGet() {
        withRepo((repo, mtr) -> {
            repo.setPageAllocated(mtr, ext(1), 0, true);
            repo.setPageAllocated(mtr, ext(1), 63, true);
            assertTrue(repo.isPageAllocated(mtr, ext(1), 0));
            assertTrue(repo.isPageAllocated(mtr, ext(1), 63));
            assertFalse(repo.isPageAllocated(mtr, ext(1), 1));
            repo.setPageAllocated(mtr, ext(1), 0, false);
            assertFalse(repo.isPageAllocated(mtr, ext(1), 0));
        });
    }

    @Test
    void shouldRejectExtentBeyondFirstBatchAndBadIndex() {
        withRepo((repo, mtr) -> {
            long tooBig = ExtentDescriptorLayout.maxEntriesInPage0(PS);
            assertThrows(FspMetadataException.class, () -> repo.read(mtr, ext(tooBig)));
            assertThrows(DatabaseValidationException.class, () -> repo.setPageAllocated(mtr, ext(1), 64, true));
        });
    }

    @Test
    void badStateOrdinalThrowsMetadataException() {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(64));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 8)) {
            ExtentDescriptorRepository repo = new ExtentDescriptorRepository(pool, PS);
            MiniTransactionManager mgr = new MiniTransactionManager();

            MiniTransaction corrupt = mgr.begin();
            PageGuard g = corrupt.getPage(pool, PageId.of(SPACE, PageNo.of(0)), PageLatchMode.EXCLUSIVE);
            g.writeInt(ExtentDescriptorLayout.entryOffset(1) + ExtentDescriptorLayout.STATE, 99);
            mgr.commit(corrupt);

            MiniTransaction r = mgr.begin();
            assertThrows(FspMetadataException.class, () -> repo.read(r, ext(1)));
            mgr.commit(r);
        }
    }
}
```

- [ ] **Step 2: 运行确认失败。**

- [ ] **Step 3: 写 ExtentDescriptorLayout**

```java
package cn.zhangyis.db.storage.fsp;

import cn.zhangyis.db.domain.PageSize;

/**
 * XDES entry 布局：内嵌 page 0、从 SpaceHeaderLayout.XDES_BASE 起、每条 ENTRY_SIZE 字节（条内偏移如下）。
 * bitmap 固定 32 字节（256 位，1 位/页，1=已分配），仅前 pagesPerExtent 位有效。
 */
final class ExtentDescriptorLayout {

    private ExtentDescriptorLayout() {
    }

    static final int STATE = 0;                 // int（ExtentState ordinal）
    static final int OWNER_SEGMENT = STATE + 4; // 4 long（0=无主）
    static final int PREV = OWNER_SEGMENT + 8;  // 12 FileAddress(12)
    static final int NEXT = PREV + 12;          // 24 FileAddress(12)
    static final int BITMAP = NEXT + 12;        // 36
    static final int BITMAP_BYTES = 32;         // 256 位
    static final int ENTRY_SIZE = BITMAP + BITMAP_BYTES; // 68

    static long maxEntriesInPage0(PageSize pageSize) {
        return (long) (pageSize.bytes() - SpaceHeaderLayout.XDES_BASE) / ENTRY_SIZE;
    }

    static int entryOffset(long extentNo) {
        return Math.toIntExact(SpaceHeaderLayout.XDES_BASE + extentNo * ENTRY_SIZE);
    }
}
```

- [ ] **Step 4: 写 ExtentDescriptor**

```java
package cn.zhangyis.db.storage.fsp;

import cn.zhangyis.db.domain.ExtentId;
import cn.zhangyis.db.domain.SegmentId;

import java.util.Optional;

/**
 * XDES entry 投影（不含 bitmap，bitmap 按位单独访问，避免大数组拷贝）。ownerSegmentRaw==0 表无主。
 */
public record ExtentDescriptor(
        ExtentId extentId,
        ExtentState state,
        long ownerSegmentRaw,
        FileAddress prev,
        FileAddress next) {

    /** 拥有者 segment；raw==0 → 空（无主）。 */
    public Optional<SegmentId> ownerSegment() {
        return ownerSegmentRaw == 0 ? Optional.empty() : Optional.of(SegmentId.of(ownerSegmentRaw));
    }
}
```

- [ ] **Step 5: 写 ExtentDescriptorRepository**

```java
package cn.zhangyis.db.storage.fsp;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.ExtentId;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SegmentId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.mtr.MiniTransaction;

import java.util.Optional;

/**
 * XDES（extent descriptor）仓储（设计 §6.3）。首版 XDES entries 内嵌 page 0；按 ExtentId.extentNo 定位 slot。
 * 物理空间账本：extent 状态 / 归属 segment / list-node 指针 / page 分配位图。读写经 page 0 latch（写 X）。
 * extent 0 是系统保留 extent，不能走普通 initFree/free-list 分配路径，也不能被普通 XDES mutator 改写。
 * extentNo 超 page 0 首批容量 → FspMetadataException（首版不支持独立 XDES 管理页）。
 */
public final class ExtentDescriptorRepository {

    private final BufferPool pool;
    private final PageSize pageSize;

    public ExtentDescriptorRepository(BufferPool pool, PageSize pageSize) {
        if (pool == null || pageSize == null) {
            throw new DatabaseValidationException("pool/pageSize must not be null");
        }
        this.pool = pool;
        this.pageSize = pageSize;
    }

    private int entryOffset(ExtentId extentId) {
        requireExtent(extentId);
        if (extentId.extentNo() >= ExtentDescriptorLayout.maxEntriesInPage0(pageSize)) {
            throw new FspMetadataException("extent beyond first XDES region not supported: " + extentId.extentNo());
        }
        return ExtentDescriptorLayout.entryOffset(extentId.extentNo());
    }

    private PageId page0(ExtentId extentId) {
        return PageId.of(extentId.spaceId(), PageNo.of(0));
    }

    public ExtentDescriptor read(MiniTransaction mtr, ExtentId extentId) {
        requireMtr(mtr);
        int base = entryOffset(extentId);
        PageGuard g = mtr.getPage(pool, page0(extentId), PageLatchMode.SHARED);
        ExtentState state = decodeState(g.readInt(base + ExtentDescriptorLayout.STATE));
        long owner = g.readLong(base + ExtentDescriptorLayout.OWNER_SEGMENT);
        FileAddress prev = FileAddress.readFrom(g, base + ExtentDescriptorLayout.PREV);
        FileAddress next = FileAddress.readFrom(g, base + ExtentDescriptorLayout.NEXT);
        return new ExtentDescriptor(extentId, state, owner, prev, next);
    }

    /** 重置普通 extent 为零态（FREE/无主/NULL/bitmap 清零）。extent0 系统保留，禁止普通初始化。 */
    public void initFree(MiniTransaction mtr, ExtentId extentId) {
        requireMtr(mtr);
        if (extentId != null && extentId.extentNo() == 0) {
            throw new FspMetadataException("extent 0 is system-reserved; use reserveSystemExtent");
        }
        int base = entryOffset(extentId);
        PageGuard g = mtr.getPage(pool, page0(extentId), PageLatchMode.EXCLUSIVE);
        g.writeInt(base + ExtentDescriptorLayout.STATE, ExtentState.FREE.ordinal());
        g.writeLong(base + ExtentDescriptorLayout.OWNER_SEGMENT, 0L);
        FileAddress.NULL.writeTo(g, base + ExtentDescriptorLayout.PREV);
        FileAddress.NULL.writeTo(g, base + ExtentDescriptorLayout.NEXT);
        g.writeBytes(base + ExtentDescriptorLayout.BITMAP, new byte[ExtentDescriptorLayout.BITMAP_BYTES]);
    }

    /** 初始化/修复 extent0 系统保留状态：page0..3 固定管理页标记已分配，避免普通 allocator 误用。 */
    public void reserveSystemExtent(MiniTransaction mtr, SpaceId spaceId) {
        requireMtr(mtr);
        if (spaceId == null) {
            throw new DatabaseValidationException("space id must not be null");
        }
        ExtentId extentId = ExtentId.of(spaceId, 0);
        int base = entryOffset(extentId);
        PageGuard g = mtr.getPage(pool, page0(extentId), PageLatchMode.EXCLUSIVE);
        g.writeInt(base + ExtentDescriptorLayout.STATE, ExtentState.FSEG_FRAG.ordinal());
        g.writeLong(base + ExtentDescriptorLayout.OWNER_SEGMENT, 0L);
        FileAddress.NULL.writeTo(g, base + ExtentDescriptorLayout.PREV);
        FileAddress.NULL.writeTo(g, base + ExtentDescriptorLayout.NEXT);
        g.writeBytes(base + ExtentDescriptorLayout.BITMAP, new byte[ExtentDescriptorLayout.BITMAP_BYTES]);
        for (int page = 0; page < 4; page++) {
            int byteOffset = base + ExtentDescriptorLayout.BITMAP + page / 8;
            byte b = g.readBytes(byteOffset, 1)[0];
            g.writeBytes(byteOffset, new byte[] {(byte) (b | (1 << (page % 8)))});
        }
    }

    public void writeState(MiniTransaction mtr, ExtentId extentId, ExtentState state) {
        requireMtr(mtr);
        if (state == null) {
            throw new DatabaseValidationException("extent state must not be null");
        }
        requireOrdinaryMutableExtent(extentId);
        int base = entryOffset(extentId);
        PageGuard g = mtr.getPage(pool, page0(extentId), PageLatchMode.EXCLUSIVE);
        g.writeInt(base + ExtentDescriptorLayout.STATE, state.ordinal());
    }

    public void writeOwner(MiniTransaction mtr, ExtentId extentId, Optional<SegmentId> owner) {
        requireMtr(mtr);
        if (owner == null) {
            throw new DatabaseValidationException("owner optional must not be null");
        }
        requireOrdinaryMutableExtent(extentId);
        long raw = owner.map(SegmentId::value).orElse(0L);
        if (raw == 0 && owner.isPresent()) {
            throw new DatabaseValidationException("segment id 0 is reserved as XDES owner sentinel");
        }
        int base = entryOffset(extentId);
        PageGuard g = mtr.getPage(pool, page0(extentId), PageLatchMode.EXCLUSIVE);
        g.writeLong(base + ExtentDescriptorLayout.OWNER_SEGMENT, raw);
    }

    public void writePrev(MiniTransaction mtr, ExtentId extentId, FileAddress prev) {
        writeAddr(mtr, extentId, ExtentDescriptorLayout.PREV, prev);
    }

    public void writeNext(MiniTransaction mtr, ExtentId extentId, FileAddress next) {
        writeAddr(mtr, extentId, ExtentDescriptorLayout.NEXT, next);
    }

    public boolean isPageAllocated(MiniTransaction mtr, ExtentId extentId, int pageIndexInExtent) {
        requireMtr(mtr);
        int base = entryOffset(extentId);
        requireBitIndex(pageIndexInExtent);
        PageGuard g = mtr.getPage(pool, page0(extentId), PageLatchMode.SHARED);
        byte b = g.readBytes(base + ExtentDescriptorLayout.BITMAP + pageIndexInExtent / 8, 1)[0];
        return (b & (1 << (pageIndexInExtent % 8))) != 0;
    }

    public void setPageAllocated(MiniTransaction mtr, ExtentId extentId, int pageIndexInExtent, boolean allocated) {
        requireMtr(mtr);
        requireOrdinaryMutableExtent(extentId);
        int base = entryOffset(extentId);
        requireBitIndex(pageIndexInExtent);
        PageGuard g = mtr.getPage(pool, page0(extentId), PageLatchMode.EXCLUSIVE);
        int byteOffset = base + ExtentDescriptorLayout.BITMAP + pageIndexInExtent / 8;
        int mask = 1 << (pageIndexInExtent % 8);
        byte b = g.readBytes(byteOffset, 1)[0];
        byte nb = (byte) (allocated ? (b | mask) : (b & ~mask));
        g.writeBytes(byteOffset, new byte[] {nb});
    }

    private void writeAddr(MiniTransaction mtr, ExtentId extentId, int fieldOffset, FileAddress addr) {
        requireMtr(mtr);
        if (addr == null) {
            throw new DatabaseValidationException("file address must not be null (use FileAddress.NULL)");
        }
        requireOrdinaryMutableExtent(extentId);
        int base = entryOffset(extentId);
        PageGuard g = mtr.getPage(pool, page0(extentId), PageLatchMode.EXCLUSIVE);
        addr.writeTo(g, base + fieldOffset);
    }

    private static ExtentState decodeState(int ordinal) {
        ExtentState[] values = ExtentState.values();
        if (ordinal < 0 || ordinal >= values.length) {
            throw new FspMetadataException("invalid extent state ordinal on disk: " + ordinal);
        }
        return values[ordinal];
    }

    private void requireBitIndex(int idx) {
        if (idx < 0 || idx >= pageSize.pagesPerExtent()) {
            throw new DatabaseValidationException("page index in extent out of range: " + idx);
        }
    }

    private static void requireMtr(MiniTransaction mtr) {
        if (mtr == null) {
            throw new DatabaseValidationException("mini transaction must not be null");
        }
    }

    private static void requireExtent(ExtentId extentId) {
        if (extentId == null) {
            throw new DatabaseValidationException("extent id must not be null");
        }
    }

    private static void requireOrdinaryMutableExtent(ExtentId extentId) {
        requireExtent(extentId);
        if (extentId.extentNo() == 0) {
            throw new FspMetadataException("extent 0 is system-reserved; use reserveSystemExtent");
        }
    }
}
```

- [ ] **Step 6: 运行确认通过**，不提交。

---

## Task 6: SegmentInode（Layout + record + Repository）

> ✅ **已完成 — 全部 Step 通过（含 Step 7 全量回归、Step 8 索引刷新）** — `SegmentInodeLayout.java`、`SegmentInode.java`、`SegmentInodeRepository.java` 已创建，`SegmentInodeRepositoryTest` 绿（RED→GREEN：allocateSlot→read、segmentId 0 拒绝、freeSlot 清零复用、读空槽/坏 purpose 抛 `FspMetadataException`、fragment 槽 set/get/requireFree、fragment page0 与下标越界拒绝）；`clean test` 全量绿、GitNexus 索引已刷新。未提交。

**Files:**
- Create: `src/main/java/cn/zhangyis/db/storage/fsp/SegmentInodeLayout.java`
- Create: `src/main/java/cn/zhangyis/db/storage/fsp/SegmentInode.java`
- Create: `src/main/java/cn/zhangyis/db/storage/fsp/SegmentInodeRepository.java`
- Test: `src/test/java/cn/zhangyis/db/storage/fsp/SegmentInodeRepositoryTest.java`

- [ ] **Step 1: 写失败测试**

```java
package cn.zhangyis.db.storage.fsp;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SegmentId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.fil.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.PageStore;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * SegmentInodeRepository 集成测试：allocateSlot→read、segmentId 0 拒绝、freeSlot 清零复用、读空槽拒绝、setter、
 * fragment 槽 set/get/requireFree，以及坏 purpose ordinal 拒绝。
 */
class SegmentInodeRepositoryTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(7);

    @TempDir
    Path dir;

    private interface Body {
        void run(SegmentInodeRepository repo, MiniTransaction mtr);
    }

    private void withRepo(Body body) {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(64));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 8)) {
            SegmentInodeRepository repo = new SegmentInodeRepository(pool, PS);
            MiniTransactionManager mgr = new MiniTransactionManager();
            MiniTransaction mtr = mgr.begin();
            body.run(repo, mtr);
            mgr.commit(mtr);
        }
    }

    @Test
    void allocateThenReadRoundTrips() {
        withRepo((repo, mtr) -> {
            int slot = repo.allocateSlot(mtr, SPACE, SegmentId.of(3), SegmentPurpose.INDEX_LEAF);
            assertEquals(0, slot);
            SegmentInode inode = repo.read(mtr, SPACE, slot);
            assertEquals(SegmentId.of(3), inode.segmentId());
            assertEquals(SegmentPurpose.INDEX_LEAF, inode.purpose());
            assertEquals(0L, inode.usedPageCount());
            assertTrue(inode.freeExtentListHead().isNull());
        });
    }

    @Test
    void shouldRejectSegmentIdZero() {
        withRepo((repo, mtr) ->
                assertThrows(DatabaseValidationException.class,
                        () -> repo.allocateSlot(mtr, SPACE, SegmentId.of(0), SegmentPurpose.SYSTEM)));
    }

    @Test
    void freeSlotAllowsReuseAndReadingFreeSlotThrows() {
        withRepo((repo, mtr) -> {
            int slot = repo.allocateSlot(mtr, SPACE, SegmentId.of(1), SegmentPurpose.UNDO);
            repo.setUsedPageCount(mtr, SPACE, slot, 5L);
            repo.setFragmentPage(mtr, SPACE, slot, 0, Optional.of(PageNo.of(40)));
            repo.freeSlot(mtr, SPACE, slot);
            assertThrows(FspMetadataException.class, () -> repo.read(mtr, SPACE, slot));
            int reused = repo.allocateSlot(mtr, SPACE, SegmentId.of(2), SegmentPurpose.SYSTEM);
            assertEquals(slot, reused);
            assertTrue(repo.getFragmentPage(mtr, SPACE, reused, 0).isEmpty());
        });
    }

    @Test
    void settersAndFragmentSlots() {
        withRepo((repo, mtr) -> {
            int slot = repo.allocateSlot(mtr, SPACE, SegmentId.of(9), SegmentPurpose.INDEX_NON_LEAF);
            repo.setUsedPageCount(mtr, SPACE, slot, 7L);
            repo.setFreeExtentListHead(mtr, SPACE, slot, FileAddress.of(PageNo.of(0), 200));
            assertEquals(0, repo.requireFreeFragmentSlot(mtr, SPACE, slot));
            repo.setFragmentPage(mtr, SPACE, slot, 0, Optional.of(PageNo.of(40)));
            assertEquals(Optional.of(PageNo.of(40)), repo.getFragmentPage(mtr, SPACE, slot, 0));
            assertEquals(1, repo.requireFreeFragmentSlot(mtr, SPACE, slot));

            SegmentInode inode = repo.read(mtr, SPACE, slot);
            assertEquals(7L, inode.usedPageCount());
            assertEquals(FileAddress.of(PageNo.of(0), 200), inode.freeExtentListHead());
        });
    }

    @Test
    void fullFragmentSlotsAndPageZeroThrow() {
        withRepo((repo, mtr) -> {
            int slot = repo.allocateSlot(mtr, SPACE, SegmentId.of(1), SegmentPurpose.LOB);
            assertThrows(DatabaseValidationException.class,
                    () -> repo.setFragmentPage(mtr, SPACE, slot, 0, Optional.of(PageNo.of(0))));
            for (int i = 0; i < SegmentInodeLayout.FRAGMENT_SLOT_COUNT; i++) {
                repo.setFragmentPage(mtr, SPACE, slot, i, Optional.of(PageNo.of(40 + i)));
            }
            assertThrows(FspMetadataException.class, () -> repo.requireFreeFragmentSlot(mtr, SPACE, slot));
        });
    }

    @Test
    void badFragmentIndexThrows() {
        withRepo((repo, mtr) -> {
            int slot = repo.allocateSlot(mtr, SPACE, SegmentId.of(1), SegmentPurpose.LOB);
            assertThrows(DatabaseValidationException.class,
                    () -> repo.getFragmentPage(mtr, SPACE, slot, 32));
        });
    }

    @Test
    void badPurposeOrdinalThrowsMetadataException() {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(64));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 8)) {
            SegmentInodeRepository repo = new SegmentInodeRepository(pool, PS);
            MiniTransactionManager mgr = new MiniTransactionManager();
            MiniTransaction init = mgr.begin();
            int slot = repo.allocateSlot(init, SPACE, SegmentId.of(1), SegmentPurpose.LOB);
            mgr.commit(init);

            MiniTransaction corrupt = mgr.begin();
            PageGuard g = corrupt.getPage(pool, PageId.of(SPACE, PageNo.of(2)), PageLatchMode.EXCLUSIVE);
            g.writeInt(SegmentInodeLayout.slotOffset(slot) + SegmentInodeLayout.PURPOSE, 99);
            mgr.commit(corrupt);

            MiniTransaction r = mgr.begin();
            assertThrows(FspMetadataException.class, () -> repo.read(r, SPACE, slot));
            mgr.commit(r);
        }
    }
}
```

- [ ] **Step 2: 运行确认失败。**

- [ ] **Step 3: 写 SegmentInodeLayout**

```java
package cn.zhangyis.db.storage.fsp;

import cn.zhangyis.db.domain.PageSize;

/**
 * SegmentInode entry 布局（page 2，从 INODE_BASE 起，每条 ENTRY_SIZE 字节）。used==0 空闲槽；fragment 槽空哨兵=0。
 */
final class SegmentInodeLayout {

    private SegmentInodeLayout() {
    }

    static final int INODE_BASE = PageLayouts.FIL_PAGE_DATA; // 38

    static final int USED = 0;                          // int（0=空闲,1=在用）
    static final int SEGMENT_ID = USED + 4;             // 4 long
    static final int PURPOSE = SEGMENT_ID + 8;          // 12 int（SegmentPurpose ordinal）
    static final int USED_PAGE_COUNT = PURPOSE + 4;     // 16 long
    static final int RESERVED_PAGE_COUNT = USED_PAGE_COUNT + 8; // 24 long
    static final int FREE_EXTENT_LIST = RESERVED_PAGE_COUNT + 8; // 32 FileAddress(12)
    static final int NOT_FULL_EXTENT_LIST = FREE_EXTENT_LIST + 12; // 44
    static final int FULL_EXTENT_LIST = NOT_FULL_EXTENT_LIST + 12; // 56
    static final int FRAGMENT_SLOTS = FULL_EXTENT_LIST + 12;       // 68
    static final int FRAGMENT_SLOT_COUNT = 32;
    static final int ENTRY_SIZE = FRAGMENT_SLOTS + FRAGMENT_SLOT_COUNT * 8; // 324

    static long maxInodesInPage(PageSize pageSize) {
        return (long) (pageSize.bytes() - INODE_BASE) / ENTRY_SIZE;
    }

    static int slotOffset(int inodeSlot) {
        return INODE_BASE + inodeSlot * ENTRY_SIZE;
    }

    static int fragmentSlotOffset(int inodeSlot, int fragIdx) {
        return slotOffset(inodeSlot) + FRAGMENT_SLOTS + fragIdx * 8;
    }
}
```

- [ ] **Step 4: 写 SegmentInode**

```java
package cn.zhangyis.db.storage.fsp;

import cn.zhangyis.db.domain.SegmentId;

/**
 * SegmentInode 投影（fragment 槽按槽单独访问）。逻辑 segment 归属：fragment slots + 三个 extent list 头 + 计数。
 */
public record SegmentInode(
        int inodeSlot,
        SegmentId segmentId,
        SegmentPurpose purpose,
        long usedPageCount,
        long reservedPageCount,
        FileAddress freeExtentListHead,
        FileAddress notFullExtentListHead,
        FileAddress fullExtentListHead) {
}
```

- [ ] **Step 5: 写 SegmentInodeRepository**

```java
package cn.zhangyis.db.storage.fsp;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SegmentId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.mtr.MiniTransaction;

import java.util.Optional;

/**
 * SegmentInode（page 2）仓储（设计 §6.4）。逻辑 segment 归属：分配/释放 inode 槽、读写 purpose/计数/extent list 头/fragment 槽。
 * 首版单个 inode 页 page 2。inode 槽分配（allocateSlot）与 requireFreeFragmentSlot 为查找型、非幂等。
 */
public final class SegmentInodeRepository {

    private final BufferPool pool;
    private final PageSize pageSize;

    public SegmentInodeRepository(BufferPool pool, PageSize pageSize) {
        if (pool == null || pageSize == null) {
            throw new DatabaseValidationException("pool/pageSize must not be null");
        }
        this.pool = pool;
        this.pageSize = pageSize;
    }

    private static PageId inodePage(SpaceId spaceId) {
        return PageId.of(spaceId, PageNo.of(2));
    }

    /** 扫首个 used==0 槽，写入 inode（used=1/segmentId/purpose、counts=0、三 list=NULL、32 fragment 槽=0），返回槽下标。无空槽抛异常。 */
    public int allocateSlot(MiniTransaction mtr, SpaceId spaceId, SegmentId segmentId, SegmentPurpose purpose) {
        requireMtr(mtr);
        requireSpace(spaceId);
        if (segmentId == null || purpose == null) {
            throw new DatabaseValidationException("segmentId/purpose must not be null");
        }
        if (segmentId.value() <= 0) {
            throw new DatabaseValidationException("segment id 0 is reserved as empty owner sentinel");
        }
        long max = SegmentInodeLayout.maxInodesInPage(pageSize);
        PageGuard g = mtr.getPage(pool, inodePage(spaceId), PageLatchMode.EXCLUSIVE);
        for (int slot = 0; slot < max; slot++) {
            int base = SegmentInodeLayout.slotOffset(slot);
            if (g.readInt(base + SegmentInodeLayout.USED) == 0) {
                g.writeInt(base + SegmentInodeLayout.USED, 1);
                g.writeLong(base + SegmentInodeLayout.SEGMENT_ID, segmentId.value());
                g.writeInt(base + SegmentInodeLayout.PURPOSE, purpose.ordinal());
                g.writeLong(base + SegmentInodeLayout.USED_PAGE_COUNT, 0L);
                g.writeLong(base + SegmentInodeLayout.RESERVED_PAGE_COUNT, 0L);
                FileAddress.NULL.writeTo(g, base + SegmentInodeLayout.FREE_EXTENT_LIST);
                FileAddress.NULL.writeTo(g, base + SegmentInodeLayout.NOT_FULL_EXTENT_LIST);
                FileAddress.NULL.writeTo(g, base + SegmentInodeLayout.FULL_EXTENT_LIST);
                for (int f = 0; f < SegmentInodeLayout.FRAGMENT_SLOT_COUNT; f++) {
                    g.writeLong(SegmentInodeLayout.fragmentSlotOffset(slot, f), 0L);
                }
                return slot;
            }
        }
        throw new FspMetadataException("no free inode slot on page 2 (max " + max + ")");
    }

    public SegmentInode read(MiniTransaction mtr, SpaceId spaceId, int inodeSlot) {
        requireMtr(mtr);
        requireSpace(spaceId);
        int base = requireSlot(spaceId, inodeSlot);
        PageGuard g = mtr.getPage(pool, inodePage(spaceId), PageLatchMode.SHARED);
        if (g.readInt(base + SegmentInodeLayout.USED) == 0) {
            throw new FspMetadataException("inode slot is free: " + inodeSlot);
        }
        return new SegmentInode(
                inodeSlot,
                SegmentId.of(g.readLong(base + SegmentInodeLayout.SEGMENT_ID)),
                decodePurpose(g.readInt(base + SegmentInodeLayout.PURPOSE)),
                g.readLong(base + SegmentInodeLayout.USED_PAGE_COUNT),
                g.readLong(base + SegmentInodeLayout.RESERVED_PAGE_COUNT),
                FileAddress.readFrom(g, base + SegmentInodeLayout.FREE_EXTENT_LIST),
                FileAddress.readFrom(g, base + SegmentInodeLayout.NOT_FULL_EXTENT_LIST),
                FileAddress.readFrom(g, base + SegmentInodeLayout.FULL_EXTENT_LIST));
    }

    public void freeSlot(MiniTransaction mtr, SpaceId spaceId, int inodeSlot) {
        requireMtr(mtr);
        requireSpace(spaceId);
        int base = requireSlot(spaceId, inodeSlot);
        PageGuard g = mtr.getPage(pool, inodePage(spaceId), PageLatchMode.EXCLUSIVE);
        g.writeBytes(base, new byte[SegmentInodeLayout.ENTRY_SIZE]);
    }

    public void setUsedPageCount(MiniTransaction mtr, SpaceId spaceId, int inodeSlot, long value) {
        writeLongField(mtr, spaceId, inodeSlot, SegmentInodeLayout.USED_PAGE_COUNT, value);
    }

    public void setReservedPageCount(MiniTransaction mtr, SpaceId spaceId, int inodeSlot, long value) {
        writeLongField(mtr, spaceId, inodeSlot, SegmentInodeLayout.RESERVED_PAGE_COUNT, value);
    }

    public void setFreeExtentListHead(MiniTransaction mtr, SpaceId spaceId, int inodeSlot, FileAddress head) {
        writeAddrField(mtr, spaceId, inodeSlot, SegmentInodeLayout.FREE_EXTENT_LIST, head);
    }

    public void setNotFullExtentListHead(MiniTransaction mtr, SpaceId spaceId, int inodeSlot, FileAddress head) {
        writeAddrField(mtr, spaceId, inodeSlot, SegmentInodeLayout.NOT_FULL_EXTENT_LIST, head);
    }

    public void setFullExtentListHead(MiniTransaction mtr, SpaceId spaceId, int inodeSlot, FileAddress head) {
        writeAddrField(mtr, spaceId, inodeSlot, SegmentInodeLayout.FULL_EXTENT_LIST, head);
    }

    /** fragment 槽：值 0 → 空。 */
    public Optional<PageNo> getFragmentPage(MiniTransaction mtr, SpaceId spaceId, int inodeSlot, int fragIdx) {
        requireMtr(mtr);
        requireSpace(spaceId);
        requireSlot(spaceId, inodeSlot);
        requireFrag(fragIdx);
        PageGuard g = mtr.getPage(pool, inodePage(spaceId), PageLatchMode.SHARED);
        long raw = g.readLong(SegmentInodeLayout.fragmentSlotOffset(inodeSlot, fragIdx));
        return raw == 0 ? Optional.empty() : Optional.of(PageNo.of(raw));
    }

    public void setFragmentPage(MiniTransaction mtr, SpaceId spaceId, int inodeSlot, int fragIdx, Optional<PageNo> pageNo) {
        requireMtr(mtr);
        requireSpace(spaceId);
        requireSlot(spaceId, inodeSlot);
        requireFrag(fragIdx);
        if (pageNo == null) {
            throw new DatabaseValidationException("fragment pageNo optional must not be null");
        }
        long raw = pageNo.map(PageNo::value).orElse(0L);
        if (raw == 0 && pageNo.isPresent()) {
            throw new DatabaseValidationException("page 0 is reserved as empty fragment sentinel");
        }
        PageGuard g = mtr.getPage(pool, inodePage(spaceId), PageLatchMode.EXCLUSIVE);
        g.writeLong(SegmentInodeLayout.fragmentSlotOffset(inodeSlot, fragIdx), raw);
    }

    /** 返回首个空（值为 0）fragment 槽下标；满则抛 FspMetadataException。 */
    public int requireFreeFragmentSlot(MiniTransaction mtr, SpaceId spaceId, int inodeSlot) {
        requireMtr(mtr);
        requireSpace(spaceId);
        requireSlot(spaceId, inodeSlot);
        PageGuard g = mtr.getPage(pool, inodePage(spaceId), PageLatchMode.SHARED);
        for (int f = 0; f < SegmentInodeLayout.FRAGMENT_SLOT_COUNT; f++) {
            if (g.readLong(SegmentInodeLayout.fragmentSlotOffset(inodeSlot, f)) == 0) {
                return f;
            }
        }
        throw new FspMetadataException("no free fragment slot in inode slot: " + inodeSlot);
    }

    private void writeLongField(MiniTransaction mtr, SpaceId spaceId, int inodeSlot, int fieldOffset, long value) {
        requireMtr(mtr);
        requireSpace(spaceId);
        int base = requireSlot(spaceId, inodeSlot);
        PageGuard g = mtr.getPage(pool, inodePage(spaceId), PageLatchMode.EXCLUSIVE);
        g.writeLong(base + fieldOffset, value);
    }

    private void writeAddrField(MiniTransaction mtr, SpaceId spaceId, int inodeSlot, int fieldOffset, FileAddress head) {
        requireMtr(mtr);
        requireSpace(spaceId);
        int base = requireSlot(spaceId, inodeSlot);
        if (head == null) {
            throw new DatabaseValidationException("list head must not be null (use FileAddress.NULL)");
        }
        PageGuard g = mtr.getPage(pool, inodePage(spaceId), PageLatchMode.EXCLUSIVE);
        head.writeTo(g, base + fieldOffset);
    }

    private static SegmentPurpose decodePurpose(int ordinal) {
        SegmentPurpose[] values = SegmentPurpose.values();
        if (ordinal < 0 || ordinal >= values.length) {
            throw new FspMetadataException("invalid segment purpose ordinal on disk: " + ordinal);
        }
        return values[ordinal];
    }

    private int requireSlot(SpaceId spaceId, int inodeSlot) {
        if (inodeSlot < 0 || inodeSlot >= SegmentInodeLayout.maxInodesInPage(pageSize)) {
            throw new DatabaseValidationException("inode slot out of range: " + inodeSlot);
        }
        return SegmentInodeLayout.slotOffset(inodeSlot);
    }

    private static void requireFrag(int fragIdx) {
        if (fragIdx < 0 || fragIdx >= SegmentInodeLayout.FRAGMENT_SLOT_COUNT) {
            throw new DatabaseValidationException("fragment index out of range: " + fragIdx);
        }
    }

    private static void requireMtr(MiniTransaction mtr) {
        if (mtr == null) {
            throw new DatabaseValidationException("mini transaction must not be null");
        }
    }

    private static void requireSpace(SpaceId spaceId) {
        if (spaceId == null) {
            throw new DatabaseValidationException("space id must not be null");
        }
    }
}
```

- [ ] **Step 6: 运行单类测试通过。**
- [ ] **Step 7: 全量回归**（去 `--tests`），期望 BUILD SUCCESSFUL。不提交。
- [ ] **Step 8: 刷新 GitNexus 索引** — `npx gitnexus analyze`。

---

## Self-Review（写计划后自查，已执行）

**1. Spec 覆盖：** §4 buf readLong/writeLong→Task1；§9 S→X 升级风险→Task1.5；§5.1 FileAddress→Task2；§5.2/5.5/5.6 枚举/FIL_PAGE_DATA→Task2/3；§5.3 零初始化哨兵→各 Layout/Repository（FREE=0/NULL=全零/owner=0/fragment=0，nextSegmentId>0）；§5.4 系统 extent→reserveSystemExtent，普通 XDES mutator 拒绝 extent0；§6 SpaceHeader→Task4；§7 XDES→Task5；§8 SegmentInode→Task6；§10 异常→Task3+各仓储；§9 幂等/非幂等与 no-redo 原型边界→注释。

**2. Placeholder 扫描：** 无 TBD/TODO；每步含完整代码与命令。

**3. 类型一致性：** Layout 常量名、`FileAddress.{of,NULL,isNull,pageNo,offset,writeTo,readFrom}`、`SpaceHeaderSnapshot` 13 字段、`ExtentDescriptor.{state,ownerSegmentRaw,ownerSegment,prev,next}`、`SegmentInode` 8 字段、各 Repository 方法签名在测试与实现间一致；复用 `PageGuard.{readInt,writeInt,readLong,writeLong,readBytes,writeBytes}`、`ExtentId.{of,spaceId,extentNo}`、`SegmentId.{of,value}`、`MiniTransaction.getPage`、`MiniTransactionManager.{begin,commit}`、`LruBufferPool`、`FileChannelPageStore`。

**4. 歧义：** XDES bitmap 1 位/页、位序 `byte=idx/8, bit=idx%8`；owner/fragment 0=空哨兵且真实 SegmentId/PageNo 入口拒绝 0；extent0 系统保留且 allocator 后续必须跳过，普通 mutator 也拒绝；read 通过 `decodeState` / `decodePurpose` 检查落盘 ordinal，越界抛 `FspMetadataException`（ordinal 顺序仍由 FspEnumTest 钉死）；page0 单 latch 同时覆盖 header+XDES，组合 page0/page2 按 page0→page2 取 latch；本计划 no-redo、不声明 crash-safe。
