# mtr 最小 mini-transaction Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 实现最小 mini-transaction：memo 收集 buf 的 `PageGuard`（page latch + buffer fix），commit/rollback 时 LIFO 释放；savepoint 提前释放局部资源；`MiniTransactionManager` 绑定线程、禁静默嵌套。

**Architecture:** `MiniTransactionState` 状态机驱动 begin/commit/rollback；`MtrMemo` 是 LIFO `AutoCloseable` 栈；`MiniTransaction` 包装 `BufferPool.getPage/newPage` 把 guard 收进 memo，commit/rollback 时 LIFO close（释 latch+fix、按 wrote 标脏）；`MiniTransactionManager` 用 ThreadLocal 绑定。不含 redo/LSN/WAL/content-undo。

**Tech Stack:** Java 25、JUnit Jupiter（buf `LruBufferPool` + fil `FileChannelPageStore` + `@TempDir` 真实驱动）、`java.util.concurrent.atomic`、`ArrayDeque`。

**Spec:** `docs/superpowers/specs/2026-06-10-mtr-min-transaction-design.md`

**通用约定：**
- 新增类位于 `cn.zhangyis.db.storage.mtr`（生产）/ 同包测试。
- 中文 Javadoc（解释状态机/资源边界/简化点）；禁 `synchronized`；禁裸 `IllegalArgumentException`/`RuntimeException`（用项目异常）。
- **不提交**（master，commit 延后），改动留工作区。
- 单类测试：`$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --console=plain --tests "<FQCN>"`；全量去掉 `--tests`。「verify it fails」= 编译失败。

---

## File Structure

生产（`src/main/java/cn/zhangyis/db/storage/mtr/`）：

| 文件 | 职责 | 可见性 |
| --- | --- | --- |
| `MiniTransactionState.java` | 生命周期状态枚举 + 流转校验 | public |
| `MtrStateException.java` | 状态/绑定异常（Runtime） | public |
| `MtrSavepoint.java` | 类型化保存点（record） | public |
| `MtrMemo.java` | LIFO AutoCloseable 栈 + savepoint | 包内 |
| `MiniTransaction.java` | MTR 生命周期 + getPage/newPage/savepoint | public |
| `MiniTransactionManager.java` | 线程绑定 + begin/current/commit/rollback | public |

测试：`MiniTransactionStateTest`、`MtrMemoTest`、`MiniTransactionTest`、`MiniTransactionManagerTest`。

---

## Task 1: MiniTransactionState + MtrStateException

**Files:**
- Create `src/main/java/cn/zhangyis/db/storage/mtr/MiniTransactionState.java`
- Create `src/main/java/cn/zhangyis/db/storage/mtr/MtrStateException.java`
- Test `src/test/java/cn/zhangyis/db/storage/mtr/MiniTransactionStateTest.java`

- [ ] **Step 1: 写失败测试**

```java
package cn.zhangyis.db.storage.mtr;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MiniTransactionState 测试固定 MTR 生命周期状态机，避免 begin/commit/rollback 各自乱改状态。
 */
class MiniTransactionStateTest {

    @Test
    void shouldAllowLifecycleTransitions() {
        assertTrue(MiniTransactionState.NEW.canTransitTo(MiniTransactionState.ACTIVE));
        assertTrue(MiniTransactionState.ACTIVE.canTransitTo(MiniTransactionState.COMMITTING));
        assertTrue(MiniTransactionState.ACTIVE.canTransitTo(MiniTransactionState.ROLLED_BACK));
        assertTrue(MiniTransactionState.COMMITTING.canTransitTo(MiniTransactionState.COMMITTED));
    }

    @Test
    void shouldRejectIllegalTransitions() {
        assertFalse(MiniTransactionState.NEW.canTransitTo(MiniTransactionState.COMMITTING));
        assertFalse(MiniTransactionState.ACTIVE.canTransitTo(MiniTransactionState.COMMITTED));
        assertFalse(MiniTransactionState.COMMITTED.canTransitTo(MiniTransactionState.ACTIVE));
        assertFalse(MiniTransactionState.ROLLED_BACK.canTransitTo(MiniTransactionState.ACTIVE));
        assertThrows(MtrStateException.class,
                () -> MiniTransactionState.COMMITTED.validateTransitTo(MiniTransactionState.ACTIVE));
    }
}
```

- [ ] **Step 2: 运行确认失败。**

- [ ] **Step 3: 写 MtrStateException**

```java
package cn.zhangyis.db.storage.mtr;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * mini-transaction 状态/绑定异常（设计 §17）。用于非法状态流转、终态后复用、嵌套 begin、
 * 跨线程或未绑定 commit/rollback、无当前 MTR 时 current()、savepoint 跨 MTR 误用。
 * 可恢复运行时异常：调用方应回滚或重建 MTR。
 */
public class MtrStateException extends DatabaseRuntimeException {

    public MtrStateException(String message) {
        super(message);
    }

    public MtrStateException(String message, Throwable cause) {
        super(message, cause);
    }
}
```

- [ ] **Step 4: 写 MiniTransactionState**

```java
package cn.zhangyis.db.storage.mtr;

/**
 * mini-transaction 生命周期状态（设计 §9.1）。NEW 构造态、ACTIVE 活跃态、COMMITTING 提交中、
 * COMMITTED/ROLLED_BACK 终态。COMMITTING 在释放失败时为不可复用半终态（§17）。
 */
public enum MiniTransactionState {
    NEW,
    ACTIVE,
    COMMITTING,
    COMMITTED,
    ROLLED_BACK;

    /**
     * 判断是否允许流转到 next。保护 begin/commit/rollback 共享的状态不变量。
     *
     * @param next 目标状态。
     * @return 允许返回 true。
     */
    public boolean canTransitTo(MiniTransactionState next) {
        if (next == null) {
            return false;
        }
        return switch (this) {
            case NEW -> next == ACTIVE;
            case ACTIVE -> next == COMMITTING || next == ROLLED_BACK;
            case COMMITTING -> next == COMMITTED;
            case COMMITTED, ROLLED_BACK -> false;
        };
    }

    /**
     * 校验流转合法，非法抛 MtrStateException，避免绕过状态机构造不安全状态。
     *
     * @param next 目标状态。
     */
    public void validateTransitTo(MiniTransactionState next) {
        if (!canTransitTo(next)) {
            throw new MtrStateException("illegal mini transaction state transition: " + this + " -> " + next);
        }
    }
}
```

- [ ] **Step 5: 运行确认通过**，不提交。

---

## Task 2: MtrSavepoint + MtrMemo

**Files:**
- Create `src/main/java/cn/zhangyis/db/storage/mtr/MtrSavepoint.java`
- Create `src/main/java/cn/zhangyis/db/storage/mtr/MtrMemo.java`
- Test `src/test/java/cn/zhangyis/db/storage/mtr/MtrMemoTest.java`

- [ ] **Step 1: 写失败测试**

```java
package cn.zhangyis.db.storage.mtr;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MtrMemo 测试固定 LIFO 释放序、savepoint 局部释放、越界 no-op、释放异常聚合、push 校验。
 */
class MtrMemoTest {

    /** 记录关闭顺序的假资源；fail=true 时关闭抛异常但仍记录。 */
    private static final class Recorder implements AutoCloseable {
        private final String name;
        private final List<String> log;
        private final boolean fail;

        Recorder(String name, List<String> log, boolean fail) {
            this.name = name;
            this.log = log;
            this.fail = fail;
        }

        @Override
        public void close() {
            log.add(name);
            if (fail) {
                throw new RuntimeException("boom " + name);
            }
        }
    }

    @Test
    void releaseAllShouldCloseInLifoOrder() {
        List<String> log = new ArrayList<>();
        MtrMemo memo = new MtrMemo();
        memo.push(new Recorder("a", log, false));
        memo.push(new Recorder("b", log, false));
        memo.push(new Recorder("c", log, false));
        assertEquals(3, memo.depth());

        memo.releaseAll();

        assertEquals(List.of("c", "b", "a"), log);
        assertEquals(0, memo.depth());
    }

    @Test
    void releaseToShouldReleaseOnlyAboveSavepoint() {
        List<String> log = new ArrayList<>();
        MtrMemo memo = new MtrMemo();
        memo.push(new Recorder("a", log, false));
        int sp = memo.depth();
        memo.push(new Recorder("b", log, false));
        memo.push(new Recorder("c", log, false));

        memo.releaseTo(sp);

        assertEquals(List.of("c", "b"), log);
        assertEquals(1, memo.depth());
    }

    @Test
    void releaseToBeyondDepthShouldBeNoOp() {
        List<String> log = new ArrayList<>();
        MtrMemo memo = new MtrMemo();
        memo.push(new Recorder("a", log, false));

        memo.releaseTo(5);

        assertTrue(log.isEmpty());
        assertEquals(1, memo.depth());
    }

    @Test
    void releaseToNegativeShouldThrow() {
        MtrMemo memo = new MtrMemo();
        assertThrows(DatabaseValidationException.class, () -> memo.releaseTo(-1));
    }

    @Test
    void releaseShouldContinueAndAggregateOnCloseFailure() {
        List<String> log = new ArrayList<>();
        MtrMemo memo = new MtrMemo();
        memo.push(new Recorder("a", log, false));
        memo.push(new Recorder("b", log, true)); // 顶部，关闭抛异常

        assertThrows(MtrStateException.class, memo::releaseAll);

        assertEquals(List.of("b", "a"), log); // 异常后仍继续释放 a
        assertEquals(0, memo.depth());
    }

    @Test
    void pushNullShouldThrow() {
        MtrMemo memo = new MtrMemo();
        assertThrows(DatabaseValidationException.class, () -> memo.push(null));
    }
}
```

- [ ] **Step 2: 运行确认失败。**

- [ ] **Step 3: 写 MtrSavepoint**

```java
package cn.zhangyis.db.storage.mtr;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 类型化保存点：记录所属 MTR id 与 memo 深度。只应由 {@link MiniTransaction#savepoint()} 创建，
 * 不要手工构造；rollbackToSavepoint 会用 mtrId 校验归属、用 depth 决定释放到哪一层。
 *
 * @param mtrId 所属 mini-transaction id。
 * @param depth 保存点对应的 memo 深度（非负）。
 */
public record MtrSavepoint(long mtrId, int depth) {

    public MtrSavepoint {
        if (depth < 0) {
            throw new DatabaseValidationException("savepoint depth must be non-negative: " + depth);
        }
    }
}
```

- [ ] **Step 4: 写 MtrMemo**

```java
package cn.zhangyis.db.storage.mtr;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * mini-transaction memo 栈：LIFO 持有短临界区资源（page latch + buffer fix 等，均表现为 AutoCloseable），
 * commit/rollback/savepoint 时按后进先出释放。仅由属主线程访问，无需自身加锁（设计 §9.2）。
 */
final class MtrMemo {

    /** 资源栈；push/pop 头部，构成 LIFO。 */
    private final Deque<AutoCloseable> stack = new ArrayDeque<>();

    /**
     * 压入一个待释放资源（通常是 buf 的 PageGuard）。
     *
     * @param resource 非空资源。
     */
    void push(AutoCloseable resource) {
        if (resource == null) {
            throw new DatabaseValidationException("memo resource must not be null");
        }
        stack.push(resource);
    }

    /** 当前栈深，用作 savepoint 标记。 */
    int depth() {
        return stack.size();
    }

    /**
     * 释放到指定深度（LIFO）。当前深度 ≤ targetDepth 时 no-op；逐个 close，即便某资源 close 抛异常也继续释放其余，
     * 最后把首个异常包成 MtrStateException 抛出（其余 addSuppressed），避免泄漏 latch/fix。
     *
     * @param targetDepth 目标深度（非负）。
     */
    void releaseTo(int targetDepth) {
        if (targetDepth < 0) {
            throw new DatabaseValidationException("memo target depth must be non-negative: " + targetDepth);
        }
        Exception firstError = null;
        while (stack.size() > targetDepth) {
            AutoCloseable resource = stack.pop();
            try {
                resource.close();
            } catch (Exception e) {
                if (firstError == null) {
                    firstError = e;
                } else {
                    firstError.addSuppressed(e);
                }
            }
        }
        if (firstError != null) {
            throw new MtrStateException("failed to release memo resource(s)", firstError);
        }
    }

    /** 释放全部资源（LIFO）。 */
    void releaseAll() {
        releaseTo(0);
    }
}
```

- [ ] **Step 5: 运行确认通过**，不提交。

---

## Task 3: MiniTransaction

**Files:**
- Create `src/main/java/cn/zhangyis/db/storage/mtr/MiniTransaction.java`
- Test `src/test/java/cn/zhangyis/db/storage/mtr/MiniTransactionTest.java`

依赖 Task 1/2 + 已有 buf（`BufferPool`/`LruBufferPool`/`PageGuard`/`PageLatchMode`/`BufferPoolExhaustedException`）、fil（`FileChannelPageStore`/`PageStore`）。

- [ ] **Step 1: 写失败测试**

```java
package cn.zhangyis.db.storage.mtr;

import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.BufferPoolExhaustedException;
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
 * MiniTransaction 集成测试：用 buf+fil 真实驱动，固定 commit/rollback/savepoint 释放、终态保护、写后落盘。
 */
class MiniTransactionTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);

    @TempDir
    Path dir;

    private PageStore openStore(int pages) {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(pages));
        return store;
    }

    private PageId page(long n) {
        return PageId.of(SPACE, PageNo.of(n));
    }

    private MiniTransaction activeMtr(long id) {
        MiniTransaction mtr = new MiniTransaction(id);
        mtr.activate();
        return mtr;
    }

    @Test
    void commitShouldReleaseHeldFixes() {
        try (PageStore store = openStore(8)) {
            BufferPool pool = new LruBufferPool(store, PS, 1);
            MiniTransaction mtr = activeMtr(1);
            mtr.getPage(pool, page(0), PageLatchMode.SHARED); // 占住唯一帧
            assertThrows(BufferPoolExhaustedException.class,
                    () -> pool.getPage(page(1), PageLatchMode.SHARED));
            mtr.commit();
            assertEquals(MiniTransactionState.COMMITTED, mtr.state());
            try (PageGuard g = pool.getPage(page(1), PageLatchMode.SHARED)) {
                assertEquals(page(1), g.pageId());
            }
            pool.close();
        }
    }

    @Test
    void savepointShouldReleaseResourcesAcquiredAfterIt() {
        try (PageStore store = openStore(8)) {
            BufferPool pool = new LruBufferPool(store, PS, 2);
            MiniTransaction mtr = activeMtr(1);
            mtr.getPage(pool, page(0), PageLatchMode.SHARED);
            MtrSavepoint sp = mtr.savepoint();
            mtr.getPage(pool, page(1), PageLatchMode.SHARED);
            assertThrows(BufferPoolExhaustedException.class,
                    () -> pool.getPage(page(2), PageLatchMode.SHARED));
            mtr.rollbackToSavepoint(sp); // 释放 page1
            try (PageGuard g = pool.getPage(page(2), PageLatchMode.SHARED)) {
                assertEquals(page(2), g.pageId());
            }
            mtr.commit(); // 释放 page0
            pool.close();
        }
    }

    @Test
    void rollbackUncommittedShouldReleaseAndTerminate() {
        try (PageStore store = openStore(8)) {
            BufferPool pool = new LruBufferPool(store, PS, 1);
            MiniTransaction mtr = activeMtr(1);
            mtr.getPage(pool, page(0), PageLatchMode.SHARED);
            mtr.rollbackUncommitted();
            assertEquals(MiniTransactionState.ROLLED_BACK, mtr.state());
            try (PageGuard g = pool.getPage(page(1), PageLatchMode.SHARED)) {
                assertEquals(page(1), g.pageId());
            }
            pool.close();
        }
    }

    @Test
    void operationsAfterTerminalShouldThrow() {
        try (PageStore store = openStore(8)) {
            BufferPool pool = new LruBufferPool(store, PS, 4);
            MiniTransaction mtr = activeMtr(1);
            mtr.commit();
            assertThrows(MtrStateException.class, () -> mtr.getPage(pool, page(0), PageLatchMode.SHARED));
            assertThrows(MtrStateException.class, mtr::savepoint);
            assertThrows(MtrStateException.class, mtr::commit);
            pool.close();
        }
    }

    @Test
    void writesShouldBeDirtyAndPersistAfterCommitAndFlush() {
        try (PageStore store = openStore(8)) {
            BufferPool pool = new LruBufferPool(store, PS, 4);
            MiniTransaction mtr = activeMtr(1);
            PageGuard g = mtr.getPage(pool, page(2), PageLatchMode.EXCLUSIVE);
            g.writeInt(0, 0x99);
            mtr.commit(); // close g → 标脏
            pool.flush(page(2));

            BufferPool pool2 = new LruBufferPool(store, PS, 4);
            try (PageGuard r = pool2.getPage(page(2), PageLatchMode.SHARED)) {
                assertEquals(0x99, r.readInt(0));
            }
            pool.close();
            pool2.close();
        }
    }

    @Test
    void savepointFromAnotherMtrShouldBeRejected() {
        try (PageStore store = openStore(8)) {
            BufferPool pool = new LruBufferPool(store, PS, 4);
            MiniTransaction mtrA = activeMtr(1);
            MiniTransaction mtrB = activeMtr(2);
            MtrSavepoint spA = mtrA.savepoint();
            assertThrows(MtrStateException.class, () -> mtrB.rollbackToSavepoint(spA));
            mtrA.commit();
            mtrB.commit();
            pool.close();
        }
    }
}
```

- [ ] **Step 2: 运行确认失败。**

- [ ] **Step 3: 写 MiniTransaction**

```java
package cn.zhangyis.db.storage.mtr;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.buf.PageLatchMode;

/**
 * mini-transaction：短物理临界区的一致性边界（设计 §9）。memo 收集 page latch + buffer fix，
 * commit/rollback 时 LIFO 释放；savepoint 提前释放局部资源。
 *
 * <p>单线程拥有，非线程安全。简化点：commit 暂不产 redo / LSN / pageLSN / WAL 排序；
 * rollback 不撤销已写入 buffer 的页内容（MTR 无 content undo，留 redo/recovery 切片）。
 *
 * <p>并发约束：同页禁 S→X 锁升级（ReentrantReadWriteLock 无升级，会自死锁）；将写的页直接取 EXCLUSIVE。
 *
 * <p>构造与 commit/rollback/activate 为包内可见，仅由 {@link MiniTransactionManager} 编排，保证线程绑定不变量。
 */
public final class MiniTransaction {

    /** MTR 标识，用于诊断与 savepoint 归属校验。 */
    private final long id;

    /** 短临界区资源栈。 */
    private final MtrMemo memo = new MtrMemo();

    /** 生命周期状态；仅属主线程改。 */
    private MiniTransactionState state = MiniTransactionState.NEW;

    MiniTransaction(long id) {
        this.id = id;
    }

    /** NEW→ACTIVE。由 Manager.begin 调用。 */
    void activate() {
        transitTo(MiniTransactionState.ACTIVE);
    }

    /** MTR id。 */
    public long id() {
        return id;
    }

    /** 当前状态。 */
    public MiniTransactionState state() {
        return state;
    }

    /**
     * 取已存在页，固定并按 mode 取 page latch，收进 memo 持到 commit/savepoint 释放。
     * 返回的 guard 由本 MTR 拥有生命周期，调用方读写但不要自行 close。
     *
     * @param pool buffer pool。
     * @param pageId 目标页。
     * @param mode S 或 X（将写则直接 X，禁 S→X 升级）。
     * @return 受控页句柄。
     */
    public PageGuard getPage(BufferPool pool, PageId pageId, PageLatchMode mode) {
        return fix(pool, pageId, mode, true);
    }

    /**
     * 取新页（不读盘，页须已被 PageStore.extend 分配），同样收进 memo。
     *
     * @param pool buffer pool。
     * @param pageId 新页。
     * @param mode 通常 X。
     * @return 受控页句柄。
     */
    public PageGuard newPage(BufferPool pool, PageId pageId, PageLatchMode mode) {
        return fix(pool, pageId, mode, false);
    }

    private PageGuard fix(BufferPool pool, PageId pageId, PageLatchMode mode, boolean existing) {
        ensureActive();
        if (pool == null) {
            throw new DatabaseValidationException("buffer pool must not be null");
        }
        PageGuard guard = existing ? pool.getPage(pageId, mode) : pool.newPage(pageId, mode);
        memo.push(guard);
        return guard;
    }

    /** 记录当前 memo 深度为保存点。 */
    public MtrSavepoint savepoint() {
        ensureActive();
        return new MtrSavepoint(id, memo.depth());
    }

    /**
     * 释放保存点之后获取的 latch/fix（§9.2 提前释放局部 latch）。不撤销页内容；释放的页若写过仍标脏。
     * 建议只对未修改页使用。
     *
     * @param savepoint 本 MTR 的保存点。
     */
    public void rollbackToSavepoint(MtrSavepoint savepoint) {
        ensureActive();
        if (savepoint == null) {
            throw new DatabaseValidationException("savepoint must not be null");
        }
        if (savepoint.mtrId() != id) {
            throw new MtrStateException("savepoint does not belong to this mini transaction: "
                    + savepoint.mtrId() + " vs " + id);
        }
        memo.releaseTo(savepoint.depth());
    }

    /** ACTIVE→COMMITTING→（LIFO 释放 memo，按 wrote 标脏）→COMMITTED。释放失败则停在 COMMITTING（不可复用）。 */
    void commit() {
        transitTo(MiniTransactionState.COMMITTING);
        memo.releaseAll();
        transitTo(MiniTransactionState.COMMITTED);
    }

    /** ACTIVE→ROLLED_BACK，LIFO 释放 memo；不撤销已写入 buffer 的内容。 */
    void rollbackUncommitted() {
        transitTo(MiniTransactionState.ROLLED_BACK);
        memo.releaseAll();
    }

    private void transitTo(MiniTransactionState next) {
        state.validateTransitTo(next);
        state = next;
    }

    private void ensureActive() {
        if (state != MiniTransactionState.ACTIVE) {
            throw new MtrStateException("mini transaction not active: " + state);
        }
    }
}
```

- [ ] **Step 4: 运行确认通过**，不提交。

---

## Task 4: MiniTransactionManager

**Files:**
- Create `src/main/java/cn/zhangyis/db/storage/mtr/MiniTransactionManager.java`
- Test `src/test/java/cn/zhangyis/db/storage/mtr/MiniTransactionManagerTest.java`

- [ ] **Step 1: 写失败测试**

```java
package cn.zhangyis.db.storage.mtr;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * MiniTransactionManager 测试固定线程绑定、禁静默嵌套、commit/rollback 解绑、跨线程拒绝。
 */
class MiniTransactionManagerTest {

    @Test
    void beginShouldActivateAndBind() {
        MiniTransactionManager mgr = new MiniTransactionManager();
        MiniTransaction mtr = mgr.begin();
        assertEquals(MiniTransactionState.ACTIVE, mtr.state());
        assertSame(mtr, mgr.current());
        mgr.commit(mtr);
    }

    @Test
    void nestedBeginShouldThrow() {
        MiniTransactionManager mgr = new MiniTransactionManager();
        MiniTransaction mtr = mgr.begin();
        assertThrows(MtrStateException.class, mgr::begin);
        mgr.commit(mtr);
    }

    @Test
    void currentWithoutActiveShouldThrow() {
        MiniTransactionManager mgr = new MiniTransactionManager();
        assertThrows(MtrStateException.class, mgr::current);
    }

    @Test
    void commitShouldUnbindAndAllowRebegin() {
        MiniTransactionManager mgr = new MiniTransactionManager();
        MiniTransaction first = mgr.begin();
        mgr.commit(first);
        assertThrows(MtrStateException.class, mgr::current);
        MiniTransaction second = mgr.begin(); // 解绑后可再 begin，不报嵌套
        assertEquals(MiniTransactionState.ACTIVE, second.state());
        mgr.commit(second);
    }

    @Test
    void commitUnboundTransactionShouldThrow() {
        MiniTransactionManager mgr = new MiniTransactionManager();
        MiniTransaction mtr = mgr.begin();
        mgr.commit(mtr);
        assertThrows(MtrStateException.class, () -> mgr.commit(mtr)); // 已解绑
    }

    @Test
    void rollbackShouldUnbind() {
        MiniTransactionManager mgr = new MiniTransactionManager();
        MiniTransaction mtr = mgr.begin();
        mgr.rollbackUncommitted(mtr);
        assertEquals(MiniTransactionState.ROLLED_BACK, mtr.state());
        assertThrows(MtrStateException.class, mgr::current);
    }

    @Test
    void commitFromAnotherThreadShouldBeRejected() throws InterruptedException {
        MiniTransactionManager mgr = new MiniTransactionManager();
        MiniTransaction mtr = mgr.begin();
        AtomicReference<Throwable> error = new AtomicReference<>();
        Thread other = new Thread(() -> {
            try {
                mgr.commit(mtr);
            } catch (Throwable t) {
                error.set(t);
            }
        });
        other.start();
        other.join();
        assertInstanceOf(MtrStateException.class, error.get());
        mgr.commit(mtr); // 在原线程清理
    }
}
```

- [ ] **Step 2: 运行确认失败。**

- [ ] **Step 3: 写 MiniTransactionManager**

```java
package cn.zhangyis.db.storage.mtr;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.concurrent.atomic.AtomicLong;

/**
 * mini-transaction 管理器（设计 §13.2）：把 MTR 绑定到当前线程，禁止静默嵌套。
 * begin/commit/rollback 必须在同一线程；commit/rollback 用 try/finally 保证解绑，即使释放抛异常也不泄漏绑定。
 */
public final class MiniTransactionManager {

    /** 当前线程绑定的 MTR；天然按线程隔离。 */
    private final ThreadLocal<MiniTransaction> current = new ThreadLocal<>();

    /** MTR id 分配器。 */
    private final AtomicLong idSequence = new AtomicLong();

    /**
     * 开启并绑定一个 MTR。已有当前 MTR 则抛异常（禁静默嵌套，需嵌套应显式建 child）。
     *
     * @return 已 ACTIVE 的 MTR。
     */
    public MiniTransaction begin() {
        if (current.get() != null) {
            throw new MtrStateException("nested mini transaction not allowed on this thread; create an explicit child");
        }
        MiniTransaction mtr = new MiniTransaction(idSequence.incrementAndGet());
        mtr.activate();
        current.set(mtr);
        return mtr;
    }

    /**
     * 返回当前线程绑定的 MTR；无则抛异常。
     *
     * @return 当前 MTR。
     */
    public MiniTransaction current() {
        MiniTransaction mtr = current.get();
        if (mtr == null) {
            throw new MtrStateException("no active mini transaction on this thread");
        }
        return mtr;
    }

    /**
     * 提交并解绑。mtr 必须是当前线程绑定的那个；释放资源后无论成败都解绑。
     *
     * @param mtr 待提交 MTR。
     */
    public void commit(MiniTransaction mtr) {
        requireBound(mtr);
        try {
            mtr.commit();
        } finally {
            current.remove();
        }
    }

    /**
     * 回滚未提交 MTR 并解绑（不撤销 buffer 改动）。
     *
     * @param mtr 待回滚 MTR。
     */
    public void rollbackUncommitted(MiniTransaction mtr) {
        requireBound(mtr);
        try {
            mtr.rollbackUncommitted();
        } finally {
            current.remove();
        }
    }

    private void requireBound(MiniTransaction mtr) {
        if (mtr == null) {
            throw new DatabaseValidationException("mini transaction must not be null");
        }
        if (current.get() != mtr) {
            throw new MtrStateException("mini transaction not bound to current thread");
        }
    }
}
```

- [ ] **Step 4: 运行单类测试，确认通过。**
- [ ] **Step 5: 全量回归**（去掉 `--tests`），期望 BUILD SUCCESSFUL。不提交。
- [ ] **Step 6: 刷新 GitNexus 索引** — `npx gitnexus analyze`。

---

## Self-Review（写计划后自查，已执行）

**1. Spec 覆盖：** §4.1 状态→Task1；§4.2 异常→Task1；§4.3 MtrMemo→Task2；§4.4 MtrSavepoint→Task2；§4.5 MiniTransaction→Task3；§4.6 Manager→Task4；§5 数据流→Task3/4；§6 并发（单线程拥有、S→X 禁升级）→Task3 注释；§8 测试逐条覆盖（状态机/memo LIFO+聚合/commit 释放/savepoint/rollback/终态/写后落盘/跨 MTR savepoint/嵌套/跨线程/解绑）。

**2. Placeholder 扫描：** 无 TBD/TODO；每步含完整代码与命令。

**3. 类型一致性：** `MiniTransactionState.{NEW,ACTIVE,COMMITTING,COMMITTED,ROLLED_BACK,canTransitTo,validateTransitTo}`、`MtrStateException`、`MtrSavepoint(long mtrId,int depth)`、`MtrMemo.{push,depth,releaseTo,releaseAll}`、`MiniTransaction.{id,state,activate,getPage,newPage,savepoint,rollbackToSavepoint,commit,rollbackUncommitted}`、`MiniTransactionManager.{begin,current,commit,rollbackUncommitted}` 在测试与实现间一致；复用既有 `BufferPool.getPage/newPage/flush`、`PageGuard.writeInt/readInt/pageId`、`PageLatchMode`、`LruBufferPool` 构造、`FileChannelPageStore`、`PageId/PageNo/PageSize/SpaceId`。

**4. 歧义：** commit/rollback/activate 包内可见仅 Manager+同包测试调；状态机驱动所有流转（commit 非 ACTIVE→transitTo 抛错）；savepoint 跨 MTR / releaseTo 越界 no-op / 负深度校验——均与 spec §4/§10 一致。
