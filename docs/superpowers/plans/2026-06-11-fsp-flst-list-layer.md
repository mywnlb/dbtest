# fsp FLST 跨页双向链表层（slice 2a）Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 落地 InnoDB FLST 风格的跨页双向链表原语（`FlstBase`=len+first+last、`FlstNode`=prev+next、通用 `Flst`），并把 slice-1 的 6 个单 `FileAddress` list 头迁移为 32 字节 `FlstBase`。

**Architecture:** 一个页无关、MTR 驱动的通用 `Flst`，按 `FileAddress` 寻址 base 与 node，全局自由链与 segment extent 链复用同一实现。mutator 取 X、读取取 S；跨页 mutator 按 pageNo 升序取闩（page0 先于 page2）。base 节点存于 page0（SpaceHeader）与 page2（SegmentInode），repo 暴露 base 地址访问器供 Flst/2b 调用。

**Tech Stack:** Java 25、JUnit Jupiter（buf `LruBufferPool` + fil `FileChannelPageStore` + mtr `MiniTransactionManager` + `@TempDir` 真实驱动）、`ByteBuffer` 绝对读写。

**Spec:** `docs/superpowers/specs/2026-06-11-fsp-flst-list-layer-design.md`

**通用约定：**
- 类位于 `cn.zhangyis.db.storage.fsp`。中文 Javadoc；禁 `synchronized`；禁裸异常（用项目异常 `DatabaseValidationException`/`FspMetadataException`）。
- **不提交**（master）——沿用 slice-1 约定，每步只写代码 + 跑测试，不 `git commit`。
- 写路径直接取 EXCLUSIVE，禁同一 MTR 内同页 S→X 升级（Task 1.5 已落地的 MTR 约束）；组合 page0/page2 时按 page0→page2 取闩。**调用方（含测试）若已持 page2 再触 page0 会构成逆序**——测试用 commit 分隔 allocateSlot（page2）与跨页 addLast（page0→page2）。
- no-redo 原型：相关类注释与测试名不得声称 crash-safe；list 改动未来归 `UPDATE_XDES`/`UPDATE_SPACE_HEADER`/`UPDATE_SEGMENT_INODE`。
- 单类测试：`$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --console=plain --tests "<FQCN>"`；全量去 `--tests`。
- 集成测试前置：`PageStore store = new FileChannelPageStore(); store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(N));`（N≥5，使 page 0..4 存在）；`try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 8))`（先声明 store、后声明 pool，关闭时 pool 先 flush、store 后关）；`MiniTransactionManager mgr = new MiniTransactionManager();`。

---

## File Structure

生产 `src/main/java/cn/zhangyis/db/storage/fsp/`：

| 文件 | 职责 |
| --- | --- |
| `FlstNodeLayout.java`（新） | node 条内偏移 PREV/NEXT/SIZE |
| `FlstNode.java`（新） | 节点值对象 prev/next + 编解码 |
| `FlstBaseLayout.java`（新） | base 条内偏移 LEN/FIRST/LAST/SIZE |
| `FlstBase.java`（新） | base 值对象 len/first/last + 编解码（解码校验空链一致性） |
| `Flst.java`（新） | 链表算法 addFirst/addLast/remove/getFirst/getLast/length/getNext/getPrev |
| `SpaceHeaderLayout.java`（改） | 3 个 list 头 12B→32B base；XDES_BASE 200→256 |
| `SpaceHeaderSnapshot.java`（改） | 3 字段 FileAddress→FlstBase |
| `SpaceHeaderRepository.java`（改） | 读写 base；移除 3 个 list-head setter；加 3 个 base 地址访问器 |
| `SegmentInodeLayout.java`（改） | 3 个 list 头 12B→32B base；ENTRY_SIZE 324→384 |
| `SegmentInode.java`（改） | 3 字段 FileAddress→FlstBase |
| `SegmentInodeRepository.java`（改） | 读写 base；移除 3 个 list-head setter；加 3 个 base 地址访问器 |

测试 `src/test/java/cn/zhangyis/db/storage/fsp/`：`FlstNodeTest`（新）、`FlstBaseTest`（新）、`FlstTest`（新）、`SpaceHeaderRepositoryTest`（改）、`SegmentInodeRepositoryTest`（改）。

---

## Task 1: FlstNodeLayout + FlstNode

**Files:**
- Create: `src/main/java/cn/zhangyis/db/storage/fsp/FlstNodeLayout.java`
- Create: `src/main/java/cn/zhangyis/db/storage/fsp/FlstNode.java`
- Test: `src/test/java/cn/zhangyis/db/storage/fsp/FlstNodeTest.java`

- [ ] **Step 1: 写失败测试 `FlstNodeTest.java`**

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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * FlstNode 测试：null 拒绝、经真实 PageGuard 编解码往返（含全零→(NULL,NULL)）。
 */
class FlstNodeTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);

    @TempDir
    Path dir;

    @Test
    void shouldRejectNullPointers() {
        assertThrows(DatabaseValidationException.class, () -> new FlstNode(null, FileAddress.NULL));
        assertThrows(DatabaseValidationException.class, () -> new FlstNode(FileAddress.NULL, null));
    }

    @Test
    void shouldRoundTripThroughPageGuard() {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(4));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 4)) {
            try (PageGuard g = pool.getPage(PageId.of(SPACE, PageNo.of(3)), PageLatchMode.EXCLUSIVE)) {
                FlstNode n = new FlstNode(FileAddress.of(PageNo.of(0), 100), FileAddress.of(PageNo.of(0), 200));
                n.writeTo(g, 300);
                assertEquals(n, FlstNode.readFrom(g, 300));

                assertEquals(new FlstNode(FileAddress.NULL, FileAddress.NULL), FlstNode.readFrom(g, 400));
                assertTrue(FlstNode.readFrom(g, 400).prev().isNull());
            }
        }
    }
}
```

- [ ] **Step 2: 运行确认失败** — `--tests "cn.zhangyis.db.storage.fsp.FlstNodeTest"`，编译失败（FlstNode/FlstNodeLayout 不存在）。

- [ ] **Step 3: 写 `FlstNodeLayout.java`**

```java
package cn.zhangyis.db.storage.fsp;

/**
 * FLST 节点条内偏移：prev/next 各一个 FileAddress(12)。一个 node 由其起址（指向 PREV）定位。
 * XDES extent entry 的 prev/next 恰好就是一个 node（entryOffset+ExtentDescriptorLayout.PREV 为 node 起址）。
 */
final class FlstNodeLayout {

    private FlstNodeLayout() {
    }

    static final int PREV = 0;          // FileAddress 12
    static final int NEXT = PREV + 12;  // 12, FileAddress 12
    static final int SIZE = NEXT + 12;  // 24
}
```

- [ ] **Step 4: 写 `FlstNode.java`**

```java
package cn.zhangyis.db.storage.fsp;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.buf.PageGuard;

/**
 * FLST 链表节点（InnoDB FLST node = 两个 fil_addr）：prev/next 指针对。链外/链端位置用 {@link FileAddress#NULL}。
 *
 * @param prev 前驱节点地址；链头节点为 NULL。
 * @param next 后继节点地址；链尾节点为 NULL。
 */
public record FlstNode(FileAddress prev, FileAddress next) {

    public FlstNode {
        if (prev == null || next == null) {
            throw new DatabaseValidationException("flst node prev/next must not be null (use FileAddress.NULL)");
        }
    }

    /** 编码 24 字节写入 guard（要求 X latch）：prev(12)+next(12)。 */
    public void writeTo(PageGuard guard, int at) {
        prev.writeTo(guard, at + FlstNodeLayout.PREV);
        next.writeTo(guard, at + FlstNodeLayout.NEXT);
    }

    /** 从 guard 解码 24 字节。全零→(NULL,NULL)，即不在任何链中。 */
    public static FlstNode readFrom(PageGuard guard, int at) {
        return new FlstNode(
                FileAddress.readFrom(guard, at + FlstNodeLayout.PREV),
                FileAddress.readFrom(guard, at + FlstNodeLayout.NEXT));
    }
}
```

- [ ] **Step 5: 运行确认通过**（FlstNodeTest 全部）。不提交。

---

## Task 2: FlstBaseLayout + FlstBase

**Files:**
- Create: `src/main/java/cn/zhangyis/db/storage/fsp/FlstBaseLayout.java`
- Create: `src/main/java/cn/zhangyis/db/storage/fsp/FlstBase.java`
- Test: `src/test/java/cn/zhangyis/db/storage/fsp/FlstBaseTest.java`

- [ ] **Step 1: 写失败测试 `FlstBaseTest.java`**

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
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * FlstBase 测试：构造校验（length>=0、first/last 非 null）、编解码往返、零态→EMPTY、解码空链一致性损坏拒绝。
 */
class FlstBaseTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);

    @TempDir
    Path dir;

    @Test
    void constructorValidates() {
        assertThrows(DatabaseValidationException.class, () -> new FlstBase(-1, FileAddress.NULL, FileAddress.NULL));
        assertThrows(DatabaseValidationException.class, () -> new FlstBase(0, null, FileAddress.NULL));
        assertThrows(DatabaseValidationException.class, () -> new FlstBase(0, FileAddress.NULL, null));
        assertEquals(new FlstBase(0, FileAddress.NULL, FileAddress.NULL), FlstBase.EMPTY);
    }

    @Test
    void roundTripAndZeroDecodesEmpty() {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(4));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 4)) {
            try (PageGuard g = pool.getPage(PageId.of(SPACE, PageNo.of(3)), PageLatchMode.EXCLUSIVE)) {
                FlstBase b = new FlstBase(2, FileAddress.of(PageNo.of(0), 100), FileAddress.of(PageNo.of(0), 200));
                b.writeTo(g, 300);
                assertEquals(b, FlstBase.readFrom(g, 300));

                // 全零槽位解码为 EMPTY
                assertEquals(FlstBase.EMPTY, FlstBase.readFrom(g, 500));
            }
        }
    }

    @Test
    void decodeRejectsLengthEndpointInconsistency() {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(4));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 4)) {
            try (PageGuard g = pool.getPage(PageId.of(SPACE, PageNo.of(3)), PageLatchMode.EXCLUSIVE)) {
                // length>0 但 first/last 全零（NULL）：不一致 → FspMetadataException
                g.writeLong(700 + FlstBaseLayout.LEN, 5L);
                assertThrows(FspMetadataException.class, () -> FlstBase.readFrom(g, 700));
            }
        }
    }
}
```

- [ ] **Step 2: 运行确认失败** — `--tests "cn.zhangyis.db.storage.fsp.FlstBaseTest"`，编译失败。

- [ ] **Step 3: 写 `FlstBaseLayout.java`**

```java
package cn.zhangyis.db.storage.fsp;

/**
 * FLST base（表头）条内偏移：length(long) + first(FileAddress) + last(FileAddress)（InnoDB FLST base）。
 */
final class FlstBaseLayout {

    private FlstBaseLayout() {
    }

    static final int LEN = 0;            // long 8
    static final int FIRST = LEN + 8;    // 8, FileAddress 12
    static final int LAST = FIRST + 12;  // 20, FileAddress 12
    static final int SIZE = LAST + 12;   // 32
}
```

- [ ] **Step 4: 写 `FlstBase.java`**

```java
package cn.zhangyis.db.storage.fsp;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.buf.PageGuard;

/**
 * FLST 链表 base（表头，InnoDB FLST base = len + first + last）。空链 = {@link #EMPTY} = (0, NULL, NULL)。
 *
 * <p>校验分两层，避免把磁盘损坏当成程序错误：record 构造只校验 length>=0（{@link DatabaseValidationException}）；
 * 磁盘解码（{@link #readFrom}）额外校验空链一致性 length==0 ⇔ first/last 均 NULL，不一致抛 {@link FspMetadataException}。
 *
 * @param length 链长（节点数，非负）。
 * @param first  首节点地址；空链为 NULL。
 * @param last   尾节点地址；空链为 NULL。
 */
public record FlstBase(long length, FileAddress first, FileAddress last) {

    /** 空链常量：(0, NULL, NULL)。零初始化页天然解码为它。 */
    public static final FlstBase EMPTY = new FlstBase(0L, FileAddress.NULL, FileAddress.NULL);

    public FlstBase {
        if (first == null || last == null) {
            throw new DatabaseValidationException("flst base first/last must not be null (use FileAddress.NULL)");
        }
        if (length < 0) {
            throw new DatabaseValidationException("flst base length must be non-negative: " + length);
        }
    }

    /** 编码 32 字节写入 guard（要求 X latch）：len(8)+first(12)+last(12)。 */
    public void writeTo(PageGuard guard, int at) {
        guard.writeLong(at + FlstBaseLayout.LEN, length);
        first.writeTo(guard, at + FlstBaseLayout.FIRST);
        last.writeTo(guard, at + FlstBaseLayout.LAST);
    }

    /**
     * 从 guard 解码 32 字节，并校验空链一致性：length==0 ⇔ first/last 均 NULL；length>0 ⇔ first/last 均非 NULL。
     * 不一致视为页上链账本损坏，抛 {@link FspMetadataException}（与构造期 {@link DatabaseValidationException} 区分）。
     */
    public static FlstBase readFrom(PageGuard guard, int at) {
        long length = guard.readLong(at + FlstBaseLayout.LEN);
        FileAddress first = FileAddress.readFrom(guard, at + FlstBaseLayout.FIRST);
        FileAddress last = FileAddress.readFrom(guard, at + FlstBaseLayout.LAST);
        if (length < 0) {
            throw new FspMetadataException("invalid flst base length on disk: " + length);
        }
        boolean bothNull = first.isNull() && last.isNull();
        boolean bothSet = !first.isNull() && !last.isNull();
        boolean consistent = length == 0 ? bothNull : bothSet;
        if (!consistent) {
            throw new FspMetadataException("flst base length/endpoints inconsistency on disk: length=" + length
                    + " first.null=" + first.isNull() + " last.null=" + last.isNull());
        }
        return new FlstBase(length, first, last);
    }
}
```

- [ ] **Step 5: 运行确认通过**（FlstBaseTest 全部）。不提交。

---

## Task 3: Flst（链表算法）

**Files:**
- Create: `src/main/java/cn/zhangyis/db/storage/fsp/Flst.java`
- Test: `src/test/java/cn/zhangyis/db/storage/fsp/FlstTest.java`

- [ ] **Step 1: 写失败测试 `FlstTest.java`**

```java
package cn.zhangyis.db.storage.fsp;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.fil.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.PageStore;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Flst 集成测试（scratch 页驱动）：空表头/尾插、顺序遍历、头/尾/中删、length 维护、跨页 add/remove（锁序 page 小→大）、
 * 非法地址拒绝、空表 remove 拒绝。节点放在数据页固定槽位（每槽 24B，互不重叠、≥FIL_PAGE_DATA、非 (0,0)）。
 */
class FlstTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);

    @TempDir
    Path dir;

    // 单页场景：base 与 node 均在 page 3
    private static final FileAddress BASE = FileAddress.of(PageNo.of(3), 38);
    private static final FileAddress A = FileAddress.of(PageNo.of(3), 100);
    private static final FileAddress B = FileAddress.of(PageNo.of(3), 200);
    private static final FileAddress C = FileAddress.of(PageNo.of(3), 300);

    private interface Body {
        void run(Flst flst, MiniTransaction mtr);
    }

    private void withFlst(Body body) {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(5));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 8)) {
            Flst flst = new Flst(pool);
            MiniTransactionManager mgr = new MiniTransactionManager();
            MiniTransaction mtr = mgr.begin();
            body.run(flst, mtr);
            mgr.commit(mtr);
        }
    }

    @Test
    void addLastBuildsInsertionOrder() {
        withFlst((flst, mtr) -> {
            flst.addLast(mtr, SPACE, BASE, A);
            flst.addLast(mtr, SPACE, BASE, B);
            flst.addLast(mtr, SPACE, BASE, C);
            assertEquals(3L, flst.length(mtr, SPACE, BASE));
            assertEquals(A, flst.getFirst(mtr, SPACE, BASE));
            assertEquals(C, flst.getLast(mtr, SPACE, BASE));
            assertEquals(B, flst.getNext(mtr, SPACE, A));
            assertEquals(C, flst.getNext(mtr, SPACE, B));
            assertTrue(flst.getNext(mtr, SPACE, C).isNull());
            assertEquals(B, flst.getPrev(mtr, SPACE, C));
            assertTrue(flst.getPrev(mtr, SPACE, A).isNull());
        });
    }

    @Test
    void addFirstBuildsReverseOrder() {
        withFlst((flst, mtr) -> {
            flst.addFirst(mtr, SPACE, BASE, A);
            flst.addFirst(mtr, SPACE, BASE, B);
            assertEquals(2L, flst.length(mtr, SPACE, BASE));
            assertEquals(B, flst.getFirst(mtr, SPACE, BASE));
            assertEquals(A, flst.getLast(mtr, SPACE, BASE));
            assertEquals(A, flst.getNext(mtr, SPACE, B));
        });
    }

    @Test
    void removeHeadMiddleTailMaintainsLinks() {
        withFlst((flst, mtr) -> {
            flst.addLast(mtr, SPACE, BASE, A);
            flst.addLast(mtr, SPACE, BASE, B);
            flst.addLast(mtr, SPACE, BASE, C);
            // 中删 B
            flst.remove(mtr, SPACE, BASE, B);
            assertEquals(2L, flst.length(mtr, SPACE, BASE));
            assertEquals(C, flst.getNext(mtr, SPACE, A));
            assertEquals(A, flst.getPrev(mtr, SPACE, C));
            assertTrue(flst.getNext(mtr, SPACE, B).isNull());
            assertTrue(flst.getPrev(mtr, SPACE, B).isNull());
            // 头删 A
            flst.remove(mtr, SPACE, BASE, A);
            assertEquals(C, flst.getFirst(mtr, SPACE, BASE));
            assertTrue(flst.getPrev(mtr, SPACE, C).isNull());
            // 尾删 C → 空链
            flst.remove(mtr, SPACE, BASE, C);
            assertEquals(0L, flst.length(mtr, SPACE, BASE));
            assertTrue(flst.getFirst(mtr, SPACE, BASE).isNull());
            assertTrue(flst.getLast(mtr, SPACE, BASE).isNull());
        });
    }

    @Test
    void crossPageAddRemoveRespectsLockOrder() {
        // base 在 page4、node 在 page3：np(3) < bp(4)，Flst 应先取 page3 再取 page4（升序），不死锁、不残留阻塞线程。
        FileAddress base = FileAddress.of(PageNo.of(4), 38);
        FileAddress n1 = FileAddress.of(PageNo.of(3), 100);
        FileAddress n2 = FileAddress.of(PageNo.of(3), 200);
        withFlst((flst, mtr) -> {
            flst.addLast(mtr, SPACE, base, n1);
            flst.addLast(mtr, SPACE, base, n2);
            assertEquals(2L, flst.length(mtr, SPACE, base));
            assertEquals(n1, flst.getFirst(mtr, SPACE, base));
            assertEquals(n2, flst.getLast(mtr, SPACE, base));
            flst.remove(mtr, SPACE, base, n1);
            assertEquals(1L, flst.length(mtr, SPACE, base));
            assertEquals(n2, flst.getFirst(mtr, SPACE, base));
        });
    }

    @Test
    void rejectsNullAndEmptyRemove() {
        withFlst((flst, mtr) -> {
            assertThrows(DatabaseValidationException.class, () -> flst.addLast(mtr, SPACE, FileAddress.NULL, A));
            assertThrows(DatabaseValidationException.class, () -> flst.addLast(mtr, SPACE, BASE, FileAddress.NULL));
            assertThrows(FspMetadataException.class, () -> flst.remove(mtr, SPACE, BASE, A));
        });
    }
}
```

- [ ] **Step 2: 运行确认失败** — `--tests "cn.zhangyis.db.storage.fsp.FlstTest"`，编译失败（Flst 不存在）。

- [ ] **Step 3: 写 `Flst.java`**

```java
package cn.zhangyis.db.storage.fsp;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.mtr.MiniTransaction;

/**
 * FLST 跨页双向链表原语（InnoDB flst0flst 风格，设计 §5.4 extent list）。按 {@link FileAddress} 寻址 base 与 node，
 * 全局自由链与 segment extent 链复用同一实现。mutator 取 X、读取取 S。
 *
 * <p>锁序：mutator 涉及 base 页与 node 页时，按 pageNo 升序取 X（page0 先于 page2，符合 design §18 / slice-1 §9）。
 * 依赖「同链 node 同页」不变量——邻居节点（oldFirst/oldLast/prev/next）与目标 node 同页、已被覆盖，不引入逆序取闩。
 * 调用方若已持高页号 latch 再触发更低页号的 Flst 操作会构成逆序，须由调用方按页号升序编排（2b/测试遵守）。
 *
 * <p>简化点：本片 no-redo（写页只标脏、不产 redo，§15 推迟满足）；不做 O(n) 成员/双向一致性校验（信任调用方，
 * 与 InnoDB 一致）。节点是否属于该链由调用方经 extent 状态机保证。
 */
public final class Flst {

    /** 受控页来源；经 MTR.getPage 读写 base/node 字段，不直接碰裸文件。 */
    private final BufferPool pool;

    public Flst(BufferPool pool) {
        if (pool == null) {
            throw new DatabaseValidationException("buffer pool must not be null");
        }
        this.pool = pool;
    }

    /** 尾插：node 接到 base.last 之后，更新 base.last 与 length。 */
    public void addLast(MiniTransaction mtr, SpaceId spaceId, FileAddress baseAddr, FileAddress nodeAddr) {
        requireArgs(mtr, spaceId, baseAddr, nodeAddr);
        PageGuard[] g = latchAscending(mtr, spaceId, baseAddr, nodeAddr);
        PageGuard baseG = g[0];
        PageGuard nodeG = g[1];
        FlstBase base = FlstBase.readFrom(baseG, baseAddr.offset());
        if (base.last().isNull()) {
            new FlstNode(FileAddress.NULL, FileAddress.NULL).writeTo(nodeG, nodeAddr.offset());
            new FlstBase(1L, nodeAddr, nodeAddr).writeTo(baseG, baseAddr.offset());
            return;
        }
        FileAddress oldLast = base.last();
        PageGuard oldLastG = mtr.getPage(pool, pageId(spaceId, oldLast), PageLatchMode.EXCLUSIVE);
        FlstNode oln = FlstNode.readFrom(oldLastG, oldLast.offset());
        new FlstNode(oldLast, FileAddress.NULL).writeTo(nodeG, nodeAddr.offset());
        new FlstNode(oln.prev(), nodeAddr).writeTo(oldLastG, oldLast.offset());
        new FlstBase(base.length() + 1L, base.first(), nodeAddr).writeTo(baseG, baseAddr.offset());
    }

    /** 头插：node 接到 base.first 之前，更新 base.first 与 length。 */
    public void addFirst(MiniTransaction mtr, SpaceId spaceId, FileAddress baseAddr, FileAddress nodeAddr) {
        requireArgs(mtr, spaceId, baseAddr, nodeAddr);
        PageGuard[] g = latchAscending(mtr, spaceId, baseAddr, nodeAddr);
        PageGuard baseG = g[0];
        PageGuard nodeG = g[1];
        FlstBase base = FlstBase.readFrom(baseG, baseAddr.offset());
        if (base.first().isNull()) {
            new FlstNode(FileAddress.NULL, FileAddress.NULL).writeTo(nodeG, nodeAddr.offset());
            new FlstBase(1L, nodeAddr, nodeAddr).writeTo(baseG, baseAddr.offset());
            return;
        }
        FileAddress oldFirst = base.first();
        PageGuard oldFirstG = mtr.getPage(pool, pageId(spaceId, oldFirst), PageLatchMode.EXCLUSIVE);
        FlstNode ofn = FlstNode.readFrom(oldFirstG, oldFirst.offset());
        new FlstNode(FileAddress.NULL, oldFirst).writeTo(nodeG, nodeAddr.offset());
        new FlstNode(nodeAddr, ofn.next()).writeTo(oldFirstG, oldFirst.offset());
        new FlstBase(base.length() + 1L, nodeAddr, base.last()).writeTo(baseG, baseAddr.offset());
    }

    /** 解链：从 base 所属链移除 node，修复 prev/next 与 base.first/last/length；移除后 node 指针置空。 */
    public void remove(MiniTransaction mtr, SpaceId spaceId, FileAddress baseAddr, FileAddress nodeAddr) {
        requireArgs(mtr, spaceId, baseAddr, nodeAddr);
        PageGuard[] g = latchAscending(mtr, spaceId, baseAddr, nodeAddr);
        PageGuard baseG = g[0];
        PageGuard nodeG = g[1];
        FlstBase base = FlstBase.readFrom(baseG, baseAddr.offset());
        if (base.length() <= 0) {
            throw new FspMetadataException("remove from empty flst base: " + baseAddr);
        }
        FlstNode node = FlstNode.readFrom(nodeG, nodeAddr.offset());
        FileAddress newFirst = base.first();
        FileAddress newLast = base.last();
        if (node.prev().isNull()) {
            newFirst = node.next();
        } else {
            PageGuard prevG = mtr.getPage(pool, pageId(spaceId, node.prev()), PageLatchMode.EXCLUSIVE);
            FlstNode prev = FlstNode.readFrom(prevG, node.prev().offset());
            new FlstNode(prev.prev(), node.next()).writeTo(prevG, node.prev().offset());
        }
        if (node.next().isNull()) {
            newLast = node.prev();
        } else {
            PageGuard nextG = mtr.getPage(pool, pageId(spaceId, node.next()), PageLatchMode.EXCLUSIVE);
            FlstNode next = FlstNode.readFrom(nextG, node.next().offset());
            new FlstNode(node.prev(), next.next()).writeTo(nextG, node.next().offset());
        }
        new FlstNode(FileAddress.NULL, FileAddress.NULL).writeTo(nodeG, nodeAddr.offset());
        long newLen = base.length() - 1L;
        if (newLen == 0L) {
            new FlstBase(0L, FileAddress.NULL, FileAddress.NULL).writeTo(baseG, baseAddr.offset());
        } else {
            new FlstBase(newLen, newFirst, newLast).writeTo(baseG, baseAddr.offset());
        }
    }

    /** 读链头地址（S）。空链返回 NULL。 */
    public FileAddress getFirst(MiniTransaction mtr, SpaceId spaceId, FileAddress baseAddr) {
        requireRead(mtr, spaceId, baseAddr);
        PageGuard baseG = mtr.getPage(pool, pageId(spaceId, baseAddr), PageLatchMode.SHARED);
        return FlstBase.readFrom(baseG, baseAddr.offset()).first();
    }

    /** 读链尾地址（S）。空链返回 NULL。 */
    public FileAddress getLast(MiniTransaction mtr, SpaceId spaceId, FileAddress baseAddr) {
        requireRead(mtr, spaceId, baseAddr);
        PageGuard baseG = mtr.getPage(pool, pageId(spaceId, baseAddr), PageLatchMode.SHARED);
        return FlstBase.readFrom(baseG, baseAddr.offset()).last();
    }

    /** 读链长（S）。 */
    public long length(MiniTransaction mtr, SpaceId spaceId, FileAddress baseAddr) {
        requireRead(mtr, spaceId, baseAddr);
        PageGuard baseG = mtr.getPage(pool, pageId(spaceId, baseAddr), PageLatchMode.SHARED);
        return FlstBase.readFrom(baseG, baseAddr.offset()).length();
    }

    /** 读 node 后继地址（S）。链尾返回 NULL。 */
    public FileAddress getNext(MiniTransaction mtr, SpaceId spaceId, FileAddress nodeAddr) {
        requireRead(mtr, spaceId, nodeAddr);
        PageGuard nodeG = mtr.getPage(pool, pageId(spaceId, nodeAddr), PageLatchMode.SHARED);
        return FlstNode.readFrom(nodeG, nodeAddr.offset()).next();
    }

    /** 读 node 前驱地址（S）。链头返回 NULL。 */
    public FileAddress getPrev(MiniTransaction mtr, SpaceId spaceId, FileAddress nodeAddr) {
        requireRead(mtr, spaceId, nodeAddr);
        PageGuard nodeG = mtr.getPage(pool, pageId(spaceId, nodeAddr), PageLatchMode.SHARED);
        return FlstNode.readFrom(nodeG, nodeAddr.offset()).prev();
    }

    /**
     * 按 pageNo 升序对 base/node 两页取 X（去重），返回 [baseGuard, nodeGuard]（同页时为同一 guard）。
     * 保证 page0 先于 page2，符合 design §18 锁序。
     */
    private PageGuard[] latchAscending(MiniTransaction mtr, SpaceId spaceId, FileAddress baseAddr, FileAddress nodeAddr) {
        long bp = baseAddr.pageNo().value();
        long np = nodeAddr.pageNo().value();
        PageGuard baseG;
        PageGuard nodeG;
        if (bp == np) {
            baseG = mtr.getPage(pool, pageId(spaceId, baseAddr), PageLatchMode.EXCLUSIVE);
            nodeG = baseG;
        } else if (bp < np) {
            baseG = mtr.getPage(pool, pageId(spaceId, baseAddr), PageLatchMode.EXCLUSIVE);
            nodeG = mtr.getPage(pool, pageId(spaceId, nodeAddr), PageLatchMode.EXCLUSIVE);
        } else {
            nodeG = mtr.getPage(pool, pageId(spaceId, nodeAddr), PageLatchMode.EXCLUSIVE);
            baseG = mtr.getPage(pool, pageId(spaceId, baseAddr), PageLatchMode.EXCLUSIVE);
        }
        return new PageGuard[] {baseG, nodeG};
    }

    private static PageId pageId(SpaceId spaceId, FileAddress addr) {
        return PageId.of(spaceId, addr.pageNo());
    }

    private static void requireArgs(MiniTransaction mtr, SpaceId spaceId, FileAddress baseAddr, FileAddress nodeAddr) {
        requireRead(mtr, spaceId, baseAddr);
        requireConcrete(nodeAddr, "node address");
    }

    private static void requireRead(MiniTransaction mtr, SpaceId spaceId, FileAddress addr) {
        if (mtr == null) {
            throw new DatabaseValidationException("mini transaction must not be null");
        }
        if (spaceId == null) {
            throw new DatabaseValidationException("space id must not be null");
        }
        requireConcrete(addr, "address");
    }

    private static void requireConcrete(FileAddress addr, String what) {
        if (addr == null) {
            throw new DatabaseValidationException(what + " must not be null");
        }
        if (addr.isNull()) {
            throw new DatabaseValidationException(what + " must not be FileAddress.NULL");
        }
    }
}
```

- [ ] **Step 4: 运行确认通过**（FlstTest 全部）。不提交。

---

## Task 4: 迁移 SpaceHeader 到 FlstBase

**Files:**
- Modify: `src/main/java/cn/zhangyis/db/storage/fsp/SpaceHeaderLayout.java`
- Modify: `src/main/java/cn/zhangyis/db/storage/fsp/SpaceHeaderSnapshot.java`
- Modify: `src/main/java/cn/zhangyis/db/storage/fsp/SpaceHeaderRepository.java`
- Modify (test): `src/test/java/cn/zhangyis/db/storage/fsp/SpaceHeaderRepositoryTest.java`

- [ ] **Step 1: 用新语义整体替换 `SpaceHeaderRepositoryTest.java`**（list 头改 FlstBase；移除单 FileAddress setter 断言；新增 Flst 协作断言）

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
 * SpaceHeaderRepository 集成测试：initialize→read 往返（三 list 头为 FlstBase 空链）、标量 setter→read、
 * allocateNextSegmentId 自增与 0 哨兵拒绝、base 地址访问器经 Flst 维护后 read 反映；no-redo，不做 crash recovery 断言。
 */
class SpaceHeaderRepositoryTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(7);

    @TempDir
    Path dir;

    private SpaceHeaderSnapshot freshHeader() {
        return new SpaceHeaderSnapshot(SPACE, PS, 0,
                PageNo.of(64), PageNo.of(64), 1L,
                FlstBase.EMPTY, FlstBase.EMPTY, FlstBase.EMPTY,
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
    void scalarSettersUpdateFields() {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(64));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 8)) {
            SpaceHeaderRepository repo = new SpaceHeaderRepository(pool);
            MiniTransactionManager mgr = new MiniTransactionManager();
            MiniTransaction w = mgr.begin();
            repo.initialize(w, freshHeader());
            repo.setCurrentSizeInPages(w, SPACE, PageNo.of(128));
            repo.setFreeLimitPageNo(w, SPACE, PageNo.of(128));
            repo.setFirstInodePageNo(w, SPACE, PageNo.of(2));
            mgr.commit(w);

            MiniTransaction r = mgr.begin();
            SpaceHeaderSnapshot got = repo.read(r, SPACE);
            mgr.commit(r);
            assertEquals(PageNo.of(128), got.currentSizeInPages());
            assertEquals(PageNo.of(128), got.freeLimitPageNo());
        }
    }

    @Test
    void freeExtentListBaseManagedByFlst() {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(64));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 8)) {
            SpaceHeaderRepository repo = new SpaceHeaderRepository(pool);
            Flst flst = new Flst(pool);
            MiniTransactionManager mgr = new MiniTransactionManager();

            MiniTransaction w = mgr.begin();
            repo.initialize(w, freshHeader());
            FileAddress base = repo.freeExtentListBaseAddr(SPACE);
            // 用 page0 XDES 区 entry 1 的 node 槽（与 header 区不重叠）
            FileAddress node = FileAddress.of(PageNo.of(0),
                    ExtentDescriptorLayout.entryOffset(1) + ExtentDescriptorLayout.PREV);
            flst.addLast(w, SPACE, base, node);
            mgr.commit(w);

            MiniTransaction r = mgr.begin();
            SpaceHeaderSnapshot got = repo.read(r, SPACE);
            mgr.commit(r);
            assertEquals(1L, got.freeExtentList().length());
            assertEquals(node, got.freeExtentList().first());
            assertEquals(node, got.freeExtentList().last());
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
                FlstBase.EMPTY, FlstBase.EMPTY, FlstBase.EMPTY,
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

- [ ] **Step 2: 运行确认失败** — `--tests "cn.zhangyis.db.storage.fsp.SpaceHeaderRepositoryTest"`，编译失败（Snapshot 字段类型与 base 访问器未迁移）。

- [ ] **Step 3: 改 `SpaceHeaderLayout.java`**（3 头 12B→32B base，XDES_BASE→256）

整体替换为：

```java
package cn.zhangyis.db.storage.fsp;

/**
 * SpaceHeaderPage（page 0）字段偏移（相对页首，均在 FIL_PAGE_DATA 之后）。三个 extent list 头为 FLST base（32B）。
 * XDES entries 从 XDES_BASE 起。
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
    static final int FREE_EXTENT_LIST_BASE = NEXT_SEGMENT_ID + 8;  // 74 FlstBase(32)
    static final int FREE_FRAG_LIST_BASE = FREE_EXTENT_LIST_BASE + FlstBaseLayout.SIZE; // 106
    static final int FULL_FRAG_LIST_BASE = FREE_FRAG_LIST_BASE + FlstBaseLayout.SIZE;   // 138
    static final int FIRST_INODE_PAGE = FULL_FRAG_LIST_BASE + FlstBaseLayout.SIZE;      // 170 long
    static final int SDI_ROOT = FIRST_INODE_PAGE + 8;               // 178 long
    static final int SERVER_VERSION = SDI_ROOT + 8;                // 186 int
    static final int SPACE_VERSION = SERVER_VERSION + 4;           // 190 long (ends 198)

    /** XDES entries 内嵌 page 0 的起始偏移（base 区之后预留到 256）。 */
    static final int XDES_BASE = 256;
}
```

- [ ] **Step 4: 改 `SpaceHeaderSnapshot.java`**（3 字段 FileAddress→FlstBase）

整体替换为：

```java
package cn.zhangyis.db.storage.fsp;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;

/**
 * SpaceHeaderPage 字段快照（§6.2）。新建表空间应取 nextSegmentId=1（0 留作 XDES 无主哨兵）、firstInodePageNo=2、
 * 三个 extent list base 为 {@link FlstBase#EMPTY}。初始化表空间时还须在同一 MTR 内调用
 * ExtentDescriptorRepository.reserveSystemExtent，避免 extent 0 被当作普通 FREE。
 *
 * @param spaceId            表空间编号。
 * @param pageSize           实例级页大小。
 * @param spaceFlags         表空间标志位（0 表无特殊标志）。
 * @param currentSizeInPages 表空间当前总页数。
 * @param freeLimitPageNo    已纳入空间管理的页号上界。
 * @param nextSegmentId      下一个待分配 segment id；必须 > 0。
 * @param freeExtentList     全局 FSP_FREE extent 链 base。
 * @param freeFragExtentList 全局 FSP_FREE_FRAG extent 链 base。
 * @param fullFragExtentList 全局 FSP_FULL_FRAG extent 链 base。
 * @param firstInodePageNo   首个 INODE 页页号（首版固定 page 2）。
 * @param sdiRootPageNo      SDI 根页号，保留扩展（0 未启用）。
 * @param serverVersion      写入该表空间的 server 版本号。
 * @param spaceVersion       表空间格式版本号。
 */
public record SpaceHeaderSnapshot(
        SpaceId spaceId,
        PageSize pageSize,
        int spaceFlags,
        PageNo currentSizeInPages,
        PageNo freeLimitPageNo,
        long nextSegmentId,
        FlstBase freeExtentList,
        FlstBase freeFragExtentList,
        FlstBase fullFragExtentList,
        PageNo firstInodePageNo,
        long sdiRootPageNo,
        int serverVersion,
        long spaceVersion) {

    public SpaceHeaderSnapshot {
        if (spaceId == null || pageSize == null || currentSizeInPages == null || freeLimitPageNo == null
                || freeExtentList == null || freeFragExtentList == null || fullFragExtentList == null
                || firstInodePageNo == null) {
            throw new DatabaseValidationException("space header snapshot fields must not be null");
        }
        if (nextSegmentId <= 0) {
            throw new DatabaseValidationException("next segment id must be positive; 0 is reserved as owner sentinel");
        }
    }
}
```

- [ ] **Step 5: 改 `SpaceHeaderRepository.java`**（base 读写 + 移除 3 个 list-head setter + 加 3 个 base 访问器）

整体替换为：

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
 * 三个 extent list 头为 FLST base，由 {@link Flst} 维护——repo 只负责 initialize/read 整 base 与暴露 base 地址访问器。
 *
 * <p>简化点：本切片 no-redo，写页只标脏、不产 redo，不声明 crash-safe（设计 §15 redo 规则推迟满足）。
 */
public final class SpaceHeaderRepository {

    /** 受控页来源；本仓储只经 MTR.getPage 拿 page 0 的 PageGuard。 */
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

    /** 写入全部 header 字段（X）。三个 extent list base 按 snapshot 写入（新建表空间应为 EMPTY）。 */
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
        h.freeExtentList().writeTo(g, SpaceHeaderLayout.FREE_EXTENT_LIST_BASE);
        h.freeFragExtentList().writeTo(g, SpaceHeaderLayout.FREE_FRAG_LIST_BASE);
        h.fullFragExtentList().writeTo(g, SpaceHeaderLayout.FULL_FRAG_LIST_BASE);
        g.writeLong(SpaceHeaderLayout.FIRST_INODE_PAGE, h.firstInodePageNo().value());
        g.writeLong(SpaceHeaderLayout.SDI_ROOT, h.sdiRootPageNo());
        g.writeInt(SpaceHeaderLayout.SERVER_VERSION, h.serverVersion());
        g.writeLong(SpaceHeaderLayout.SPACE_VERSION, h.spaceVersion());
    }

    /** 读出全部 header 字段（S）；三个 list base 经 FlstBase.readFrom 解码（含空链一致性校验）。 */
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
                FlstBase.readFrom(g, SpaceHeaderLayout.FREE_EXTENT_LIST_BASE),
                FlstBase.readFrom(g, SpaceHeaderLayout.FREE_FRAG_LIST_BASE),
                FlstBase.readFrom(g, SpaceHeaderLayout.FULL_FRAG_LIST_BASE),
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

    /** FSP_FREE 链 base 地址（page0 内固定偏移），供 Flst/2b 维护链。 */
    public FileAddress freeExtentListBaseAddr(SpaceId spaceId) {
        requireSpace(spaceId);
        return FileAddress.of(PageNo.of(0), SpaceHeaderLayout.FREE_EXTENT_LIST_BASE);
    }

    /** FSP_FREE_FRAG 链 base 地址。 */
    public FileAddress freeFragExtentListBaseAddr(SpaceId spaceId) {
        requireSpace(spaceId);
        return FileAddress.of(PageNo.of(0), SpaceHeaderLayout.FREE_FRAG_LIST_BASE);
    }

    /** FSP_FULL_FRAG 链 base 地址。 */
    public FileAddress fullFragExtentListBaseAddr(SpaceId spaceId) {
        requireSpace(spaceId);
        return FileAddress.of(PageNo.of(0), SpaceHeaderLayout.FULL_FRAG_LIST_BASE);
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

- [ ] **Step 6: 运行确认通过**（SpaceHeaderRepositoryTest 全部）。不提交。

---

## Task 5: 迁移 SegmentInode 到 FlstBase

**Files:**
- Modify: `src/main/java/cn/zhangyis/db/storage/fsp/SegmentInodeLayout.java`
- Modify: `src/main/java/cn/zhangyis/db/storage/fsp/SegmentInode.java`
- Modify: `src/main/java/cn/zhangyis/db/storage/fsp/SegmentInodeRepository.java`
- Modify (test): `src/test/java/cn/zhangyis/db/storage/fsp/SegmentInodeRepositoryTest.java`

- [ ] **Step 1: 用新语义整体替换 `SegmentInodeRepositoryTest.java`**

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
 * SegmentInodeRepository 集成测试：allocateSlot→read（三 list 头为 FlstBase 空链）、segmentId 0 拒绝、
 * freeSlot 清零复用、读空槽拒绝、标量/fragment setter、坏 purpose ordinal 拒绝，以及 base 访问器经 Flst 跨页维护。
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
            assertEquals(FlstBase.EMPTY, inode.freeExtentList());
            assertEquals(FlstBase.EMPTY, inode.notFullExtentList());
            assertEquals(FlstBase.EMPTY, inode.fullExtentList());
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
    void scalarSettersAndFragmentSlots() {
        withRepo((repo, mtr) -> {
            int slot = repo.allocateSlot(mtr, SPACE, SegmentId.of(9), SegmentPurpose.INDEX_NON_LEAF);
            repo.setUsedPageCount(mtr, SPACE, slot, 7L);
            repo.setReservedPageCount(mtr, SPACE, slot, 3L);
            assertEquals(0, repo.requireFreeFragmentSlot(mtr, SPACE, slot));
            repo.setFragmentPage(mtr, SPACE, slot, 0, Optional.of(PageNo.of(40)));
            assertEquals(Optional.of(PageNo.of(40)), repo.getFragmentPage(mtr, SPACE, slot, 0));
            assertEquals(1, repo.requireFreeFragmentSlot(mtr, SPACE, slot));

            SegmentInode inode = repo.read(mtr, SPACE, slot);
            assertEquals(7L, inode.usedPageCount());
            assertEquals(3L, inode.reservedPageCount());
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

    @Test
    void segmentExtentListBaseManagedByFlstCrossPage() {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(64));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 8)) {
            SegmentInodeRepository repo = new SegmentInodeRepository(pool, PS);
            Flst flst = new Flst(pool);
            MiniTransactionManager mgr = new MiniTransactionManager();

            // allocateSlot 取 page2；与跨页 addLast（page0→page2）用 commit 分隔，避免 page2 先于 page0 的逆序。
            MiniTransaction a = mgr.begin();
            int slot = repo.allocateSlot(a, SPACE, SegmentId.of(3), SegmentPurpose.INDEX_LEAF);
            mgr.commit(a);

            MiniTransaction w = mgr.begin();
            FileAddress base = repo.notFullExtentListBaseAddr(SPACE, slot);   // page2
            FileAddress node = FileAddress.of(PageNo.of(0),
                    ExtentDescriptorLayout.entryOffset(2) + ExtentDescriptorLayout.PREV); // page0
            flst.addLast(w, SPACE, base, node);
            mgr.commit(w);

            MiniTransaction r = mgr.begin();
            SegmentInode inode = repo.read(r, SPACE, slot);
            mgr.commit(r);
            assertEquals(1L, inode.notFullExtentList().length());
            assertEquals(node, inode.notFullExtentList().first());
            assertEquals(node, inode.notFullExtentList().last());
        }
    }
}
```

- [ ] **Step 2: 运行确认失败** — `--tests "cn.zhangyis.db.storage.fsp.SegmentInodeRepositoryTest"`，编译失败。

- [ ] **Step 3: 改 `SegmentInodeLayout.java`**（3 头 12B→32B base，ENTRY_SIZE 324→384）

整体替换为：

```java
package cn.zhangyis.db.storage.fsp;

import cn.zhangyis.db.domain.PageSize;

/**
 * SegmentInode entry 布局（page 2，从 INODE_BASE 起，每条 ENTRY_SIZE 字节）。used==0 空闲槽；fragment 槽空哨兵=0。
 * 三个 extent list 头为 FLST base（32B）。
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
    static final int FREE_EXTENT_LIST_BASE = RESERVED_PAGE_COUNT + 8;            // 32 FlstBase(32)
    static final int NOT_FULL_EXTENT_LIST_BASE = FREE_EXTENT_LIST_BASE + FlstBaseLayout.SIZE; // 64
    static final int FULL_EXTENT_LIST_BASE = NOT_FULL_EXTENT_LIST_BASE + FlstBaseLayout.SIZE; // 96
    static final int FRAGMENT_SLOTS = FULL_EXTENT_LIST_BASE + FlstBaseLayout.SIZE;            // 128
    static final int FRAGMENT_SLOT_COUNT = 32;
    static final int ENTRY_SIZE = FRAGMENT_SLOTS + FRAGMENT_SLOT_COUNT * 8; // 384

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

- [ ] **Step 4: 改 `SegmentInode.java`**（3 字段 FileAddress→FlstBase）

整体替换为：

```java
package cn.zhangyis.db.storage.fsp;

import cn.zhangyis.db.domain.SegmentId;

/**
 * SegmentInode 投影（fragment 槽按槽单独访问）。逻辑 segment 归属：fragment slots + 三个 extent list base + 计数。
 *
 * @param inodeSlot         该 inode 在 page 2 中的槽下标。
 * @param segmentId         segment 逻辑编号。
 * @param purpose           segment 用途。
 * @param usedPageCount     已使用页计数。
 * @param reservedPageCount 预留页计数（reserve factor 未来使用）。
 * @param freeExtentList    SEG_FREE extent 链 base。
 * @param notFullExtentList SEG_NOT_FULL extent 链 base。
 * @param fullExtentList    SEG_FULL extent 链 base。
 */
public record SegmentInode(
        int inodeSlot,
        SegmentId segmentId,
        SegmentPurpose purpose,
        long usedPageCount,
        long reservedPageCount,
        FlstBase freeExtentList,
        FlstBase notFullExtentList,
        FlstBase fullExtentList) {
}
```

- [ ] **Step 5: 改 `SegmentInodeRepository.java`**（base 读写 + 移除 3 个 list-head setter + 加 3 个 base 访问器）

整体替换为：

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
 * SegmentInode（page 2）仓储（设计 §6.4）。逻辑 segment 归属：分配/释放 inode 槽、读写 purpose/计数/extent list base/fragment 槽。
 * 首版单个 inode 页 page 2。三个 extent list 头为 FLST base，由 {@link Flst} 维护——repo 负责 allocateSlot/read 整 base
 * 与暴露 base 地址访问器。allocateSlot / requireFreeFragmentSlot 为查找型、非幂等。
 *
 * <p>简化点：单 inode 页 page 2；本切片 no-redo，写页只标脏、不产 redo，不声明 crash-safe（§15 推迟满足）。
 */
public final class SegmentInodeRepository {

    /** 受控页来源；inode entries 在 page 2，经 MTR.getPage 拿 page 2 的 PageGuard。 */
    private final BufferPool pool;

    /** 实例级页大小；决定 page 2 可容纳的 inode 槽数（maxInodesInPage）。 */
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

    /** 扫首个 used==0 槽，写入 inode（used=1/segmentId/purpose、counts=0、三 list=EMPTY base、32 fragment 槽=0），返回槽下标。无空槽抛异常。 */
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
                FlstBase.EMPTY.writeTo(g, base + SegmentInodeLayout.FREE_EXTENT_LIST_BASE);
                FlstBase.EMPTY.writeTo(g, base + SegmentInodeLayout.NOT_FULL_EXTENT_LIST_BASE);
                FlstBase.EMPTY.writeTo(g, base + SegmentInodeLayout.FULL_EXTENT_LIST_BASE);
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
                FlstBase.readFrom(g, base + SegmentInodeLayout.FREE_EXTENT_LIST_BASE),
                FlstBase.readFrom(g, base + SegmentInodeLayout.NOT_FULL_EXTENT_LIST_BASE),
                FlstBase.readFrom(g, base + SegmentInodeLayout.FULL_EXTENT_LIST_BASE));
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

    /** SEG_FREE 链 base 地址（page2 内 inode 槽偏移），供 Flst/2b 维护链。 */
    public FileAddress freeExtentListBaseAddr(SpaceId spaceId, int inodeSlot) {
        requireSpace(spaceId);
        int base = requireSlot(spaceId, inodeSlot);
        return FileAddress.of(PageNo.of(2), base + SegmentInodeLayout.FREE_EXTENT_LIST_BASE);
    }

    /** SEG_NOT_FULL 链 base 地址。 */
    public FileAddress notFullExtentListBaseAddr(SpaceId spaceId, int inodeSlot) {
        requireSpace(spaceId);
        int base = requireSlot(spaceId, inodeSlot);
        return FileAddress.of(PageNo.of(2), base + SegmentInodeLayout.NOT_FULL_EXTENT_LIST_BASE);
    }

    /** SEG_FULL 链 base 地址。 */
    public FileAddress fullExtentListBaseAddr(SpaceId spaceId, int inodeSlot) {
        requireSpace(spaceId);
        int base = requireSlot(spaceId, inodeSlot);
        return FileAddress.of(PageNo.of(2), base + SegmentInodeLayout.FULL_EXTENT_LIST_BASE);
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

- [ ] **Step 6: 运行确认通过**（SegmentInodeRepositoryTest 全部）。不提交。

---

## Task 6: 全量回归 + GitNexus

- [ ] **Step 1: 全量回归** — `clean test`（去 `--tests`），期望 BUILD SUCCESSFUL（含 slice-1 迁移后的全部测试 + 新增 Flst 测试）。
- [ ] **Step 2: 刷新 GitNexus 索引** — `npx gitnexus analyze`；若失败记录原因并重试。
- [ ] **Step 3: 不提交。**

---

## Self-Review（写计划后自查，已执行）

**1. Spec 覆盖：** §4.1 FlstNode→Task1；§4.2 FlstBase（含解码空链一致性）→Task2；§4.3/4.4 Layout→Task1/2；§5 Flst（addFirst/addLast/remove/getFirst/getLast/length/getNext/getPrev + §5.1 升序锁序）→Task3；§6.1–6.3 SpaceHeader 迁移→Task4；§6.4–6.5 SegmentInode 迁移→Task5；§8 异常（DatabaseValidationException / FspMetadataException）→各 Task；§9 测试（编解码、scratch、跨页、迁移回归）→Task1-5；§10 简化点→注释；Task6 全量回归 + 索引。

**2. Placeholder 扫描：** 无 TBD/TODO；每步含完整代码与命令。

**3. 类型一致性：** `FlstBase.{EMPTY,length,first,last,writeTo,readFrom}`、`FlstNode.{prev,next,writeTo,readFrom}`、`Flst.{addFirst,addLast,remove,getFirst,getLast,length,getNext,getPrev}`、`SpaceHeaderSnapshot` 13 字段（3 个 FlstBase）、`SegmentInode` 8 字段（3 个 FlstBase）、repo base 访问器（`freeExtentListBaseAddr` 等）在测试与实现间一致；偏移常量 `SpaceHeaderLayout`（XDES_BASE=256，end 198）、`SegmentInodeLayout`（ENTRY_SIZE=384）、`FlstBaseLayout.SIZE=32`、`FlstNodeLayout.SIZE=24` 自洽；复用 `PageGuard.{readInt,writeInt,readLong,writeLong,readBytes,writeBytes}`、`FileAddress.{of,NULL,isNull,writeTo,readFrom}`、`ExtentDescriptorLayout.{entryOffset,PREV}`、`MiniTransaction.getPage`、`MiniTransactionManager.{begin,commit}`。

**4. 锁序与并发：** Flst mutator 按 pageNo 升序取 X（latchAscending）；跨页测试用 base 高页号 + node 低页号触发 np<bp 分支；SegmentInode 跨页测试用 commit 分隔 allocateSlot(page2) 与 addLast(page0→page2) 避免逆序；读写在单 MTR 内「先写后读」走 X→S 降级，不触发 S→X 拒绝。
