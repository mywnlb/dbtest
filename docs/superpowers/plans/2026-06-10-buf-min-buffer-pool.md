# buf 最小 Buffer Pool Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 `fil.PageStore` 之上实现最小 Buffer Pool：固定(fix) + S/X page latch + 容量受限 LRU 淘汰 + 脏页淘汰写回 + newPage + flush，经 RAII `PageGuard` 受控访问页内容。

**Architecture:** 单 `poolLock` 保护帧表/空闲列表/LRU/帧元数据，miss/evict/flush 的盘 IO 在 poolLock 内串行（首版简化）；每帧 `ReentrantReadWriteLock` 作 page latch，在 poolLock 之外获取、永不嵌套；`fixCount>0` 不可淘汰；脏帧淘汰时经 `PageStore.writePage` 写回。不含 MTR/redo/WAL/doublewrite/PageCursor。

**Tech Stack:** Java 25、JUnit Jupiter（`@TempDir` + `FileChannelPageStore` 真实驱动）、`java.util.concurrent.locks`、`ByteBuffer` 绝对读写、Lombok `@Slf4j`。

**Spec:** `docs/superpowers/specs/2026-06-10-buf-min-buffer-pool-design.md`

**通用约定：**
- 新增类位于包 `cn.zhangyis.db.storage.buf`（生产）/ 同包测试。
- 中文 Javadoc（解释并发归属/简化点），禁机械注释；禁 `synchronized`；禁裸 `IllegalArgumentException`/`RuntimeException`（用项目异常）。
- **不提交**（在 master，commit 延后）；每个 Task 末尾不跑 git，留改动在工作区。
- 运行单类测试：`$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --console=plain --tests "<FQCN>"`
- 全量：去掉 `--tests`。「verify it fails」= 编译失败（类不存在）。

---

## File Structure

生产（`src/main/java/cn/zhangyis/db/storage/buf/`）：

| 文件 | 职责 | 可见性 |
| --- | --- | --- |
| `PageLatchMode.java` | S/X 页访问模式枚举 | public |
| `BufferPoolExhaustedException.java` | 帧耗尽异常（Runtime） | public |
| `BufferFrame.java` | 一帧驻留页（数据holder） | 包内 |
| `ReplacementPolicy.java` | 替换策略接口（Strategy） | 包内 |
| `LruReplacementPolicy.java` | LRU 实现 | 包内 |
| `FrameReleaser.java` | PageGuard 关闭回调接口 | 包内 |
| `PageGuard.java` | RAII 受控访问句柄 | public |
| `BufferPool.java` | Buffer Pool 门面接口 | public |
| `LruBufferPool.java` | 实现（poolLock + LRU + 写回 + FrameReleaser） | public |

测试（`src/test/java/cn/zhangyis/db/storage/buf/`）：`BufExceptionTest`、`LruReplacementPolicyTest`、`PageGuardTest`、`LruBufferPoolTest`。

---

## Task 1: PageLatchMode + BufferPoolExhaustedException

**Files:**
- Create: `src/main/java/cn/zhangyis/db/storage/buf/PageLatchMode.java`
- Create: `src/main/java/cn/zhangyis/db/storage/buf/BufferPoolExhaustedException.java`
- Test: `src/test/java/cn/zhangyis/db/storage/buf/BufExceptionTest.java`

- [ ] **Step 1: 写失败测试**

`BufExceptionTest.java`:

```java
package cn.zhangyis.db.storage.buf;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/**
 * 固定 buf 基础类型：page latch 模式枚举值与帧耗尽异常的可恢复分类。
 */
class BufExceptionTest {

    @Test
    void exhaustedShouldBeRecoverableRuntime() {
        Throwable cause = new IllegalStateException("all fixed");
        BufferPoolExhaustedException ex = new BufferPoolExhaustedException("exhausted", cause);
        assertInstanceOf(DatabaseRuntimeException.class, ex);
        assertEquals(cause, ex.getCause());
    }

    @Test
    void pageLatchModeShouldHaveSharedAndExclusive() {
        assertEquals(2, PageLatchMode.values().length);
        assertEquals(PageLatchMode.SHARED, PageLatchMode.valueOf("SHARED"));
        assertEquals(PageLatchMode.EXCLUSIVE, PageLatchMode.valueOf("EXCLUSIVE"));
    }
}
```

- [ ] **Step 2: 运行确认失败** — `--tests "cn.zhangyis.db.storage.buf.BufExceptionTest"`，期望编译失败。

- [ ] **Step 3: 写 PageLatchMode**

`PageLatchMode.java`:

```java
package cn.zhangyis.db.storage.buf;

/**
 * 页内容访问模式（page latch 模式）。SHARED 允许多读并发；EXCLUSIVE 排他，写入必须持有它。
 */
public enum PageLatchMode {
    /** 共享读：多个持有者可并发读同一页内容。 */
    SHARED,
    /** 排他写：独占页内容，写操作必须持有此模式。 */
    EXCLUSIVE
}
```

- [ ] **Step 4: 写 BufferPoolExhaustedException**

`BufferPoolExhaustedException.java`:

```java
package cn.zhangyis.db.storage.buf;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * Buffer Pool 帧耗尽异常。需要新帧但所有帧都被 fix（无可淘汰受害者）时抛出。
 * 可恢复：调用方释放 PageGuard 后可重试。
 */
public class BufferPoolExhaustedException extends DatabaseRuntimeException {

    public BufferPoolExhaustedException(String message) {
        super(message);
    }

    public BufferPoolExhaustedException(String message, Throwable cause) {
        super(message, cause);
    }
}
```

- [ ] **Step 5: 运行确认通过**，不提交。

---

## Task 2: BufferFrame + ReplacementPolicy + LruReplacementPolicy

**Files:**
- Create: `src/main/java/cn/zhangyis/db/storage/buf/BufferFrame.java`
- Create: `src/main/java/cn/zhangyis/db/storage/buf/ReplacementPolicy.java`
- Create: `src/main/java/cn/zhangyis/db/storage/buf/LruReplacementPolicy.java`
- Test: `src/test/java/cn/zhangyis/db/storage/buf/LruReplacementPolicyTest.java`

- [ ] **Step 1: 写失败测试**

`LruReplacementPolicyTest.java`:

```java
package cn.zhangyis.db.storage.buf;

import cn.zhangyis.db.domain.PageSize;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * LruReplacementPolicy 测试固定 LRU 顺序：插入序即初始淘汰序，访问把帧移到 MRU，移除剔出。
 */
class LruReplacementPolicyTest {

    private static final PageSize PS = PageSize.ofBytes(4 * 1024);

    @Test
    void victimOrderShouldFollowLruThenMru() {
        LruReplacementPolicy policy = new LruReplacementPolicy();
        BufferFrame a = new BufferFrame(PS);
        BufferFrame b = new BufferFrame(PS);
        BufferFrame c = new BufferFrame(PS);

        policy.onInsert(a);
        policy.onInsert(b);
        policy.onInsert(c);
        assertEquals(List.of(a, b, c), drain(policy));

        policy.onAccess(a); // a 移到 MRU 尾
        assertEquals(List.of(b, c, a), drain(policy));

        policy.onRemove(b);
        assertEquals(List.of(c, a), drain(policy));
    }

    private static List<BufferFrame> drain(LruReplacementPolicy policy) {
        List<BufferFrame> out = new ArrayList<>();
        for (BufferFrame f : policy.victimOrder()) {
            out.add(f);
        }
        return out;
    }
}
```

- [ ] **Step 2: 运行确认失败。**

- [ ] **Step 3: 写 BufferFrame**

`BufferFrame.java`:

```java
package cn.zhangyis.db.storage.buf;

import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageSize;

import java.nio.ByteBuffer;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Buffer Pool 一帧，承载单个驻留页的内容与状态。内部数据结构（包内可见），字段直接由同包协作者访问。
 *
 * <p>并发归属（AGENTS.md 要求逐字段写明）：
 * <ul>
 *   <li>pageId / dirty / fixCount：由 BufferPool 的 poolLock 保护。</li>
 *   <li>data / buffer 内容：由 pageLatch(S/X) 保护；仅在 fixCount==0（无活跃 PageGuard）时由 pool 在 poolLock 下读写（载入/写回/清零）。</li>
 *   <li>pageLatch：协调同一驻留页活跃 fixer 的读写并发；淘汰/flush 只作用于 fixCount==0 的帧，不取此闩。</li>
 * </ul>
 */
final class BufferFrame {

    /** 当前驻留页号；null 表示空闲帧。由 poolLock 保护。 */
    PageId pageId;

    /** 页内容字节，帧创建时按 pageSize 分配一次、跨驻留复用。内容由 pageLatch 保护。 */
    final byte[] data;

    /** 绝对访问视图，wrap 同一 data 数组；PageGuard 用绝对 get/put（不动 position）读写，故可并发只读。 */
    final ByteBuffer buffer;

    /** 是否含未落盘修改。由 poolLock 保护：PageGuard.close 按是否写过 OR 置位，flush/写回后清零，淘汰时读取。 */
    boolean dirty;

    /** 固定计数，>0 不可被淘汰。由 poolLock 保护。 */
    int fixCount;

    /** 页内容 S/X 闩。 */
    final ReentrantReadWriteLock pageLatch = new ReentrantReadWriteLock();

    BufferFrame(PageSize pageSize) {
        this.data = new byte[pageSize.bytes()];
        this.buffer = ByteBuffer.wrap(data);
    }
}
```

- [ ] **Step 4: 写 ReplacementPolicy**

`ReplacementPolicy.java`:

```java
package cn.zhangyis.db.storage.buf;

/**
 * Buffer Pool 页面替换策略（Strategy）。所有方法由 BufferPool 在 poolLock 下调用，实现无需自身线程安全。
 */
interface ReplacementPolicy {

    /** 记录一次对帧的访问/固定，更新最近度。 */
    void onAccess(BufferFrame frame);

    /** 记录帧成为驻留。 */
    void onInsert(BufferFrame frame);

    /** 记录帧被淘汰移除。 */
    void onRemove(BufferFrame frame);

    /**
     * 按淘汰优先序（LRU 在前）遍历当前驻留帧。调用方找到首个 fixCount==0 即 break，
     * 不得在迭代过程中调用 onRemove 改动内部结构（否则 ConcurrentModificationException）。
     */
    Iterable<BufferFrame> victimOrder();
}
```

- [ ] **Step 5: 写 LruReplacementPolicy**

`LruReplacementPolicy.java`:

```java
package cn.zhangyis.db.storage.buf;

import java.util.LinkedHashSet;

/**
 * LRU 替换策略：用访问序集合维护驻留帧，迭代序即 LRU→MRU。onAccess 通过 remove+add 把帧移到 MRU 尾。
 * 不自身加锁，依赖 BufferPool 的 poolLock 串行化。
 */
final class LruReplacementPolicy implements ReplacementPolicy {

    /** 访问序集合；头部为最久未访问（淘汰优先）。 */
    private final LinkedHashSet<BufferFrame> order = new LinkedHashSet<>();

    @Override
    public void onAccess(BufferFrame frame) {
        order.remove(frame);
        order.add(frame);
    }

    @Override
    public void onInsert(BufferFrame frame) {
        order.add(frame);
    }

    @Override
    public void onRemove(BufferFrame frame) {
        order.remove(frame);
    }

    @Override
    public Iterable<BufferFrame> victimOrder() {
        return order;
    }
}
```

- [ ] **Step 6: 运行确认通过**，不提交。

---

## Task 3: FrameReleaser + PageGuard

**Files:**
- Create: `src/main/java/cn/zhangyis/db/storage/buf/FrameReleaser.java`
- Create: `src/main/java/cn/zhangyis/db/storage/buf/PageGuard.java`
- Test: `src/test/java/cn/zhangyis/db/storage/buf/PageGuardTest.java`

- [ ] **Step 1: 写失败测试**

`PageGuardTest.java`:

```java
package cn.zhangyis.db.storage.buf;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import org.junit.jupiter.api.Test;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * PageGuard 测试用真实 BufferFrame + 假 FrameReleaser 固定：字节/整数读写、X 模式约束、越界、close 释放与回调、幂等。
 */
class PageGuardTest {

    private static final PageSize PS = PageSize.ofBytes(4 * 1024);

    private PageGuard exclusiveGuard(BufferFrame frame, FrameReleaser releaser) {
        Lock latch = frame.pageLatch.writeLock();
        latch.lock();
        return new PageGuard(releaser, frame, PageLatchMode.EXCLUSIVE, latch);
    }

    private PageGuard sharedGuard(BufferFrame frame, FrameReleaser releaser) {
        Lock latch = frame.pageLatch.readLock();
        latch.lock();
        return new PageGuard(releaser, frame, PageLatchMode.SHARED, latch);
    }

    @Test
    void shouldRoundTripIntAndBytesUnderExclusive() {
        BufferFrame frame = new BufferFrame(PS);
        PageGuard guard = exclusiveGuard(frame, (f, w) -> { });
        guard.writeInt(0, 0x01020304);
        guard.writeBytes(8, new byte[] {9, 8, 7});
        assertEquals(0x01020304, guard.readInt(0));
        assertArrayEquals(new byte[] {9, 8, 7}, guard.readBytes(8, 3));
        guard.close();
    }

    @Test
    void shouldRejectWriteUnderShared() {
        BufferFrame frame = new BufferFrame(PS);
        PageGuard guard = sharedGuard(frame, (f, w) -> { });
        assertThrows(DatabaseValidationException.class, () -> guard.writeInt(0, 1));
        assertThrows(DatabaseValidationException.class, () -> guard.markDirty());
        guard.close();
    }

    @Test
    void shouldRejectOutOfBoundsAccess() {
        BufferFrame frame = new BufferFrame(PS);
        PageGuard guard = exclusiveGuard(frame, (f, w) -> { });
        assertThrows(DatabaseValidationException.class, () -> guard.readInt(PS.bytes() - 2));
        assertThrows(DatabaseValidationException.class, () -> guard.writeBytes(PS.bytes() - 1, new byte[] {1, 2}));
        guard.close();
    }

    @Test
    void closeShouldReleaseLatchAndReportWrote() {
        BufferFrame frame = new BufferFrame(PS);
        AtomicBoolean called = new AtomicBoolean(false);
        AtomicReference<Boolean> wroteSeen = new AtomicReference<>();
        FrameReleaser releaser = (f, w) -> { called.set(true); wroteSeen.set(w); };

        PageGuard guard = exclusiveGuard(frame, releaser);
        guard.writeInt(0, 42);
        guard.close();

        assertTrue(called.get());
        assertEquals(Boolean.TRUE, wroteSeen.get());
        assertFalse(frame.pageLatch.isWriteLocked(), "page latch must be released after close");

        guard.close(); // 幂等：第二次不再回调
    }

    @Test
    void closeWithoutWriteShouldReportNotWrote() {
        BufferFrame frame = new BufferFrame(PS);
        AtomicReference<Boolean> wroteSeen = new AtomicReference<>();
        PageGuard guard = sharedGuard(frame, (f, w) -> wroteSeen.set(w));
        guard.readInt(0);
        guard.close();
        assertEquals(Boolean.FALSE, wroteSeen.get());
        assertEquals(0, frame.pageLatch.getReadLockCount());
    }

    @Test
    void shouldExposePageId() {
        BufferFrame frame = new BufferFrame(PS);
        frame.pageId = PageId.of(SpaceId.of(1), PageNo.of(5));
        PageGuard guard = sharedGuard(frame, (f, w) -> { });
        assertEquals(PageId.of(SpaceId.of(1), PageNo.of(5)), guard.pageId());
        guard.close();
    }
}
```

- [ ] **Step 2: 运行确认失败。**

- [ ] **Step 3: 写 FrameReleaser**

`FrameReleaser.java`:

```java
package cn.zhangyis.db.storage.buf;

/**
 * PageGuard 关闭时回调 BufferPool 释放帧的内部接口：在 poolLock 下按是否写过 OR 置脏，并递减 fixCount。
 * 抽出此接口使 PageGuard 与具体 pool 解耦，便于单测注入假实现。
 */
interface FrameReleaser {

    /**
     * 释放一帧。由 PageGuard.close() 在已释放 page latch 之后调用。
     *
     * @param frame 被释放的帧。
     * @param wrote 本次持有期间是否写过页内容（用于 OR 置 dirty）。
     */
    void release(BufferFrame frame, boolean wrote);
}
```

- [ ] **Step 4: 写 PageGuard**

`PageGuard.java`:

```java
package cn.zhangyis.db.storage.buf;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;

import java.util.concurrent.locks.Lock;

/**
 * 受控页访问的 RAII 句柄。getPage/newPage 返回它，持有一次 buffer fix 和一把已锁定的 page latch（S 或 X）。
 * 用 try-with-resources：close() 先释放 page latch，再回调 pool 释放 fix。
 *
 * <p>非线程安全：仅由获取它的线程使用与 close（page latch 的 lock/unlock 须同线程）。
 * 写操作（writeInt/writeBytes/markDirty）要求 EXCLUSIVE 模式，并记录“写过”以便 close 时置脏。
 */
public final class PageGuard implements AutoCloseable {

    /** 关闭回调，回到 pool 在 poolLock 下 OR 脏并 unfix。 */
    private final FrameReleaser releaser;

    /** 被访问的帧。 */
    private final BufferFrame frame;

    /** 本句柄持有的 latch 模式。 */
    private final PageLatchMode mode;

    /** 已锁定、待 close 释放的 page latch（read 或 write lock）。 */
    private final Lock heldLatch;

    /** 持有期间是否写过；close 时 OR 进 frame.dirty。 */
    private boolean wrote;

    /** 幂等关闭标志。 */
    private boolean closed;

    PageGuard(FrameReleaser releaser, BufferFrame frame, PageLatchMode mode, Lock heldLatch) {
        this.releaser = releaser;
        this.frame = frame;
        this.mode = mode;
        this.heldLatch = heldLatch;
    }

    /** 当前页号。 */
    public PageId pageId() {
        ensureOpen();
        return frame.pageId;
    }

    /** 读 4 字节大端整数。S/X 均可。 */
    public int readInt(int offset) {
        ensureOpen();
        checkBounds(offset, Integer.BYTES);
        return frame.buffer.getInt(offset);
    }

    /** 读 length 字节副本。S/X 均可。 */
    public byte[] readBytes(int offset, int length) {
        ensureOpen();
        checkBounds(offset, length);
        byte[] dst = new byte[length];
        frame.buffer.get(offset, dst, 0, length);
        return dst;
    }

    /** 写 4 字节大端整数。要求 EXCLUSIVE。 */
    public void writeInt(int offset, int value) {
        requireExclusive();
        checkBounds(offset, Integer.BYTES);
        frame.buffer.putInt(offset, value);
        wrote = true;
    }

    /** 写字节。要求 EXCLUSIVE。 */
    public void writeBytes(int offset, byte[] src) {
        requireExclusive();
        if (src == null) {
            throw new DatabaseValidationException("write source must not be null");
        }
        checkBounds(offset, src.length);
        frame.buffer.put(offset, src, 0, src.length);
        wrote = true;
    }

    /** 显式标记本页将被置脏（用于不经 writeBytes 的修改场景）。要求 EXCLUSIVE。 */
    public void markDirty() {
        requireExclusive();
        wrote = true;
    }

    /** 释放：先放 page latch，再回调 pool 在 poolLock 下 OR 脏并 unfix。幂等。 */
    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        heldLatch.unlock();
        releaser.release(frame, wrote);
    }

    private void requireExclusive() {
        ensureOpen();
        if (mode != PageLatchMode.EXCLUSIVE) {
            throw new DatabaseValidationException("page write requires EXCLUSIVE latch, but held " + mode);
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new DatabaseValidationException("page guard already closed");
        }
    }

    private void checkBounds(int offset, int length) {
        int pageBytes = frame.data.length;
        // 先挡负数与 offset 越界，再用 length > pageBytes - offset 判断，避免 offset+length 整数溢出绕过检查。
        if (offset < 0 || length < 0 || offset > pageBytes || length > pageBytes - offset) {
            throw new DatabaseValidationException("page access out of bounds: offset=" + offset
                    + " length=" + length + " pageSize=" + pageBytes);
        }
    }
}
```

- [ ] **Step 5: 运行确认通过**，不提交。

---

## Task 4: BufferPool + LruBufferPool

**Files:**
- Create: `src/main/java/cn/zhangyis/db/storage/buf/BufferPool.java`
- Create: `src/main/java/cn/zhangyis/db/storage/buf/LruBufferPool.java`
- Test: `src/test/java/cn/zhangyis/db/storage/buf/LruBufferPoolTest.java`

依赖 Task 1-3 + 已有 `fil.FileChannelPageStore`。

- [ ] **Step 1: 写失败测试**

`LruBufferPoolTest.java`:

```java
package cn.zhangyis.db.storage.buf;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.fil.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.PageStore;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * LruBufferPool 集成测试：用 FileChannelPageStore + 临时文件真实驱动，固定读写往返、LRU 淘汰+脏页写回+读穿、
 * 帧耗尽、newPage、flush 落盘。
 */
class LruBufferPoolTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);

    @TempDir
    Path dir;

    private PageStore openStore(int pages) {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(pages));
        return store;
    }

    private PageId page(long no) {
        return PageId.of(SPACE, PageNo.of(no));
    }

    @Test
    void shouldRoundTripPageThroughBufferPool() {
        try (PageStore store = openStore(8)) {
            LruBufferPool pool = new LruBufferPool(store, PS, 4);
            try (PageGuard w = pool.getPage(page(2), PageLatchMode.EXCLUSIVE)) {
                w.writeInt(0, 0xCAFEBABE);
            }
            try (PageGuard r = pool.getPage(page(2), PageLatchMode.SHARED)) {
                assertEquals(0xCAFEBABE, r.readInt(0));
            }
            pool.close();
        }
    }

    @Test
    void shouldEvictLruWriteBackDirtyAndReReadFromDisk() {
        try (PageStore store = openStore(8)) {
            LruBufferPool pool = new LruBufferPool(store, PS, 2);
            try (PageGuard g = pool.getPage(page(0), PageLatchMode.EXCLUSIVE)) {
                g.writeInt(0, 0xAA);
            }
            try (PageGuard g = pool.getPage(page(1), PageLatchMode.EXCLUSIVE)) {
                g.writeInt(0, 0xBB);
            }
            // capacity=2 已满；访问 page2 触发淘汰 LRU=page0（脏 → 写回）
            try (PageGuard g = pool.getPage(page(2), PageLatchMode.EXCLUSIVE)) {
                g.writeInt(0, 0xCC);
            }
            assertEquals(2, pool.residentCount());
            // 再读 page0：已被淘汰 → 从盘读回，应见写回的 0xAA
            try (PageGuard g = pool.getPage(page(0), PageLatchMode.SHARED)) {
                assertEquals(0xAA, g.readInt(0));
            }
            pool.close();
        }
    }

    @Test
    void shouldThrowWhenAllFramesFixed() {
        try (PageStore store = openStore(8)) {
            LruBufferPool pool = new LruBufferPool(store, PS, 1);
            PageGuard held = pool.getPage(page(0), PageLatchMode.SHARED); // 不关闭，保持 fix
            assertThrows(BufferPoolExhaustedException.class,
                    () -> pool.getPage(page(1), PageLatchMode.SHARED));
            held.close();
            pool.close();
        }
    }

    @Test
    void newPageShouldNotReadDiskAndPersistAfterFlush() {
        try (PageStore store = openStore(8)) {
            LruBufferPool pool = new LruBufferPool(store, PS, 4);
            try (PageGuard g = pool.newPage(page(3), PageLatchMode.EXCLUSIVE)) {
                assertEquals(0, g.readInt(0)); // 新页为零
                g.writeInt(0, 0x1234);
            }
            pool.flush(page(3));
            // 用新池验证已落盘
            LruBufferPool pool2 = new LruBufferPool(store, PS, 4);
            try (PageGuard g = pool2.getPage(page(3), PageLatchMode.SHARED)) {
                assertEquals(0x1234, g.readInt(0));
            }
            pool.close();
            pool2.close();
        }
    }

    @Test
    void newPageShouldRejectResidentPage() {
        try (PageStore store = openStore(8)) {
            LruBufferPool pool = new LruBufferPool(store, PS, 4);
            try (PageGuard g = pool.getPage(page(2), PageLatchMode.SHARED)) {
                // page2 已驻留
            }
            assertThrows(DatabaseValidationException.class,
                    () -> pool.newPage(page(2), PageLatchMode.EXCLUSIVE));
            pool.close();
        }
    }

    @Test
    void shouldRejectInvalidConstruction() {
        try (PageStore store = openStore(2)) {
            assertThrows(DatabaseValidationException.class, () -> new LruBufferPool(store, PS, 0));
            assertThrows(DatabaseValidationException.class, () -> new LruBufferPool(null, PS, 1));
        }
    }
}
```

- [ ] **Step 2: 运行确认失败。**

- [ ] **Step 3: 写 BufferPool 接口**

`BufferPool.java`:

```java
package cn.zhangyis.db.storage.buf;

import cn.zhangyis.db.domain.PageId;

/**
 * Buffer Pool 门面：在 fil.PageStore 之上提供受控页访问（fix + S/X page latch + LRU 淘汰 + 脏页写回）。
 * 消费方（未来 fsp）经它拿受控页，不直接接触 PageStore 或文件。
 *
 * <p>简化点：不带 MTR；flush 不做 WAL 门控 / doublewrite；miss/evict/flush 的盘 IO 在内部 poolLock 串行。
 */
public interface BufferPool extends AutoCloseable {

    /**
     * 取得页（命中或读穿），固定并按 mode 取 page latch，返回 RAII 句柄。
     *
     * @param pageId 目标页。
     * @param mode S 或 X。
     * @return 受控页句柄；用完 close。
     */
    PageGuard getPage(PageId pageId, PageLatchMode mode);

    /**
     * 为新页建立“不读盘”的零帧（页须已被 PageStore.extend 在盘上分配/零填充，由调用方保证）。
     *
     * @param pageId 新页；若已驻留则抛 DatabaseValidationException。
     * @param mode S 或 X（通常 X）。
     * @return 受控页句柄。
     */
    PageGuard newPage(PageId pageId, PageLatchMode mode);

    /** 若该页驻留、未 fix 且为脏，则写回 PageStore 并清脏。 */
    void flush(PageId pageId);

    /** 写回所有未 fix 的脏页。 */
    void flushAll();

    /** 帧总容量。 */
    int capacity();

    /** 当前驻留帧数。 */
    int residentCount();

    /** 关闭：flushAll 后释放（假设无活跃句柄）。 */
    @Override
    void close();
}
```

- [ ] **Step 4: 写 LruBufferPool**

`LruBufferPool.java`:

```java
package cn.zhangyis.db.storage.buf;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.storage.fil.PageStore;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * LRU Buffer Pool 实现。单 poolLock 保护帧表/空闲列表/LRU/帧元数据，miss/evict/flush 的盘 IO 在 poolLock 内串行
 * （首版简化点：后续引入 per-frame loading 状态把 IO 移出池锁）。每帧 page latch 在 poolLock 之外获取，不嵌套。
 * fixCount>0 不可淘汰；脏帧淘汰经 PageStore.writePage 写回。
 *
 * <p>同时实现 FrameReleaser：PageGuard.close() 回调 release 在 poolLock 下 OR 脏并 unfix。
 */
@Slf4j
public final class LruBufferPool implements BufferPool, FrameReleaser {

    private final PageStore pageStore;
    private final PageSize pageSize;
    private final int capacity;
    private final ReplacementPolicy policy;

    /** 保护 residentMap / freeList / policy / 各帧 pageId·dirty·fixCount；首版 miss/evict/flush 的盘 IO 也在其内串行。 */
    private final ReentrantLock poolLock = new ReentrantLock();
    private final Map<PageId, BufferFrame> residentMap = new HashMap<>();
    private final Deque<BufferFrame> freeList = new ArrayDeque<>();

    public LruBufferPool(PageStore pageStore, PageSize pageSize, int capacity) {
        this(pageStore, pageSize, capacity, new LruReplacementPolicy());
    }

    LruBufferPool(PageStore pageStore, PageSize pageSize, int capacity, ReplacementPolicy policy) {
        if (pageStore == null) {
            throw new DatabaseValidationException("page store must not be null");
        }
        if (pageSize == null) {
            throw new DatabaseValidationException("page size must not be null");
        }
        if (capacity < 1) {
            throw new DatabaseValidationException("capacity must be >= 1: " + capacity);
        }
        if (policy == null) {
            throw new DatabaseValidationException("replacement policy must not be null");
        }
        this.pageStore = pageStore;
        this.pageSize = pageSize;
        this.capacity = capacity;
        this.policy = policy;
        for (int i = 0; i < capacity; i++) {
            freeList.add(new BufferFrame(pageSize));
        }
    }

    @Override
    public PageGuard getPage(PageId pageId, PageLatchMode mode) {
        return acquire(pageId, mode, true);
    }

    @Override
    public PageGuard newPage(PageId pageId, PageLatchMode mode) {
        return acquire(pageId, mode, false);
    }

    /**
     * getPage/newPage 公共骨架：poolLock 内取得 target 帧（命中固定 / 未命中取受害者并载入），
     * 释放 poolLock 后取 page latch，返回 guard。载入失败回收 victim 到空闲列表，不泄漏帧/锁。
     */
    private PageGuard acquire(PageId pageId, PageLatchMode mode, boolean readFromDisk) {
        if (pageId == null) {
            throw new DatabaseValidationException("page id must not be null");
        }
        if (mode == null) {
            throw new DatabaseValidationException("page latch mode must not be null");
        }
        BufferFrame chosen;
        poolLock.lock();
        try {
            BufferFrame resident = residentMap.get(pageId);
            if (resident != null) {
                if (!readFromDisk) {
                    throw new DatabaseValidationException(
                            "newPage for resident page: " + pageId.pageNo().value());
                }
                resident.fixCount++;
                policy.onAccess(resident);
                chosen = resident;
            } else {
                BufferFrame victim = obtainVictim();
                if (victim.pageId != null) {
                    if (victim.dirty) {
                        writeBack(victim);
                    }
                    residentMap.remove(victim.pageId);
                    policy.onRemove(victim);
                }
                try {
                    if (readFromDisk) {
                        pageStore.readPage(pageId, ByteBuffer.wrap(victim.data));
                    } else {
                        Arrays.fill(victim.data, (byte) 0);
                    }
                } catch (RuntimeException loadError) {
                    victim.pageId = null;
                    victim.dirty = false;
                    freeList.add(victim);
                    throw loadError;
                }
                victim.pageId = pageId;
                victim.dirty = false;
                victim.fixCount = 1;
                residentMap.put(pageId, victim);
                policy.onInsert(victim);
                chosen = victim;
            }
        } finally {
            poolLock.unlock();
        }
        Lock latch = (mode == PageLatchMode.EXCLUSIVE)
                ? chosen.pageLatch.writeLock()
                : chosen.pageLatch.readLock();
        latch.lock();
        return new PageGuard(this, chosen, mode, latch);
    }

    /** 取受害者帧：优先空闲列表；否则 LRU 序首个未 fix 帧；都没有则抛耗尽。调用须持 poolLock。 */
    private BufferFrame obtainVictim() {
        BufferFrame free = freeList.poll();
        if (free != null) {
            return free;
        }
        for (BufferFrame frame : policy.victimOrder()) {
            if (frame.fixCount == 0) {
                return frame; // 找到即返回，迭代结束后调用方再 onRemove，避免迭代中改 LRU
            }
        }
        throw new BufferPoolExhaustedException("buffer pool exhausted: all " + capacity + " frames are fixed");
    }

    /** 写回脏帧到 PageStore 并清脏。调用须持 poolLock 且帧 fixCount==0（内容稳定）。 */
    private void writeBack(BufferFrame frame) {
        pageStore.writePage(frame.pageId, ByteBuffer.wrap(frame.data));
        frame.dirty = false;
    }

    @Override
    public void release(BufferFrame frame, boolean wrote) {
        poolLock.lock();
        try {
            if (wrote) {
                frame.dirty = true;
            }
            frame.fixCount--;
        } finally {
            poolLock.unlock();
        }
    }

    @Override
    public void flush(PageId pageId) {
        if (pageId == null) {
            throw new DatabaseValidationException("page id must not be null");
        }
        poolLock.lock();
        try {
            BufferFrame frame = residentMap.get(pageId);
            if (frame != null && frame.fixCount == 0 && frame.dirty) {
                writeBack(frame);
            }
        } finally {
            poolLock.unlock();
        }
    }

    @Override
    public void flushAll() {
        poolLock.lock();
        try {
            for (BufferFrame frame : residentMap.values()) {
                if (frame.fixCount == 0 && frame.dirty) {
                    writeBack(frame);
                }
            }
        } finally {
            poolLock.unlock();
        }
    }

    @Override
    public int capacity() {
        return capacity;
    }

    @Override
    public int residentCount() {
        poolLock.lock();
        try {
            return residentMap.size();
        } finally {
            poolLock.unlock();
        }
    }

    @Override
    public void close() {
        flushAll();
    }
}
```

- [ ] **Step 5: 运行确认通过** — `--tests "cn.zhangyis.db.storage.buf.LruBufferPoolTest"`。

- [ ] **Step 6: 全量回归** — 去掉 `--tests`，期望 BUILD SUCCESSFUL。不提交。

- [ ] **Step 7: 刷新 GitNexus 索引** — `npx gitnexus analyze`。

---

## Self-Review（写计划后自查，已执行）

**1. Spec 覆盖：** §4.1 PageLatchMode→Task1；§4.2 BufferFrame→Task2；§4.3 PageGuard→Task3；§4.4 ReplacementPolicy/Lru→Task2；§4.5 BufferPool/LruBufferPool→Task4；§4.6 BufferPoolExhaustedException→Task1；§5 数据流（含失败处理）→Task4 acquire；§6 并发模型→Task4 实现 + 注释；§8 测试逐条覆盖（往返/读穿/LRU/写回/耗尽/newPage/越界/S 写/fix/构造校验）。FrameReleaser 是实现 §4.3 close 回调的内部 seam（spec 未单列，属实现细节）。

**2. Placeholder 扫描：** 无 TBD/TODO；每步含完整代码与命令。

**3. 类型一致性：** `PageLatchMode.{SHARED,EXCLUSIVE}`、`BufferFrame.{pageId,data,buffer,dirty,fixCount,pageLatch}`、`ReplacementPolicy.{onAccess,onInsert,onRemove,victimOrder}`、`FrameReleaser.release(BufferFrame,boolean)`、`PageGuard.{pageId,readInt,readBytes,writeInt,writeBytes,markDirty,close}`、`BufferPool.{getPage,newPage,flush,flushAll,capacity,residentCount,close}`、`LruBufferPool` 构造 `(PageStore,PageSize,int[,ReplacementPolicy])` 在测试与实现间一致；复用既有 `PageId.of/pageNo/value`、`PageNo.of/value`、`PageSize.ofBytes/bytes`、`SpaceId.of`、`PageStore.create/readPage/writePage/close`、`FileChannelPageStore`。

**4. 歧义：** newPage 命中已驻留页 → DatabaseValidationException；越界用 length>pageBytes-offset 防溢出；flush 只刷未 fix 帧；page latch 在 poolLock 外获取——均与 spec §5/§6 一致。
