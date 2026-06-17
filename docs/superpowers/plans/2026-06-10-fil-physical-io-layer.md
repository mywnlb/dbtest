# fil 物理 IO 层 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 给 `storage.fil` 落地纯物理的页 IO 层——按 `PageId` 定位文件偏移、positional 读写整页、autoextend 扩展文件，并实现 §8.1 物理文件锁核心子集（#1/#3）。

**Architecture:** `PageStore`（门面，registry-无关、state-无关）按 `SpaceId` 路由到每表空间一个的 `DataFileHandle`（持 `FileChannel` + `TablespaceLifecycleLatch`(S/X) + `FileSizeLock`(X) + volatile 物理 size）。扩展页数由 `AutoExtendPolicy`（默认 `DefaultIbdAutoExtendPolicy`，复用 `PageSize.pagesPerExtent()`）决定。物理层不解析页内容、不算 checksum、不产 redo、不做逻辑状态准入。

**Tech Stack:** Java 25、JUnit Jupiter（`@TempDir` 真实临时文件）、`java.nio.channels.FileChannel` positional IO、`java.util.concurrent.locks`、Lombok `@Slf4j`。固定 JDK/Gradle 见 AGENTS.md。

**Spec:** `docs/superpowers/specs/2026-06-10-fil-physical-io-layer-design.md`

**通用约定（每个 Task 都适用）：**
- 所有新增类位于包 `cn.zhangyis.db.storage.fil`（生产）/ 同包测试。
- 注释遵循 AGENTS.md：核心类/接口/枚举、公开方法、字段都要中文 Javadoc，解释语义/并发边界/简化点；禁止机械注释。
- 禁 `synchronized`/`wait`/`notify`；显式锁 try-with-resources 释放。
- 禁裸 `IllegalArgumentException`/`RuntimeException`，用项目异常层次。
- 运行全部测试：`$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --console=plain`
- 运行单个测试类：上面命令追加 `--tests "cn.zhangyis.db.storage.fil.<ClassName>"`
- 「verify it fails」在 Gradle 下通常表现为**编译失败（被测类/方法不存在）**，这就是 TDD 的红。
- 提交信息按 AGENTS.md，结尾加：`Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`。提交前先与用户确认 git 操作（本仓库尚无 commit 基线）。

---

## File Structure

生产（`src/main/java/cn/zhangyis/db/storage/fil/`）：

| 文件 | 职责 |
| --- | --- |
| `AutoExtendPolicy.java` | 扩展页数策略接口（Strategy） |
| `DefaultIbdAutoExtendPolicy.java` | MySQL 8.0 file-per-table/general 默认扩展边界 |
| `ResourceGuard.java` | RAII 释放守卫接口（`close()` 无受检异常） |
| `TablespaceLifecycleLatch.java` | #1 生命周期闩（S/X，基于 `ReentrantReadWriteLock`） |
| `FileSizeLock.java` | #3 文件大小锁（X，基于 `ReentrantLock`） |
| `DataFileHandleLock.java` / `PageIoRangeLock.java` / `FsyncLock.java` | #2/#4/#5 预留标记接口（无实现，仅 Javadoc 标简化点与锁序） |
| `PageOutOfBoundsException.java` | 页号越界（Runtime） |
| `TablespaceNotOpenException.java` | 对未 open/create 的 spaceId 发起 IO（Runtime） |
| `DataFilePhysicalException.java` | 物理 IO/创建/扩展失败，包 IOException（Runtime） |
| `DataFileCorruptedException.java` | 文件非整页对齐等物理损坏（Fatal） |
| `DataFileHandle.java` | 每表空间一个物理单元：create/open/read/write/autoExtend/close |
| `PageStore.java` | 物理页 IO 门面接口 |
| `FileChannelPageStore.java` | `PageStore` 的 FileChannel 实现，按 SpaceId 路由 |

测试（`src/test/java/cn/zhangyis/db/storage/fil/`）：
`DefaultIbdAutoExtendPolicyTest`、`FilLockTest`、`FilPhysicalExceptionTest`、`DataFileHandleTest`、`FileChannelPageStoreTest`。

---

## Task 1: AutoExtendPolicy + DefaultIbdAutoExtendPolicy

**Files:**
- Create: `src/main/java/cn/zhangyis/db/storage/fil/AutoExtendPolicy.java`
- Create: `src/main/java/cn/zhangyis/db/storage/fil/DefaultIbdAutoExtendPolicy.java`
- Test: `src/test/java/cn/zhangyis/db/storage/fil/DefaultIbdAutoExtendPolicyTest.java`

- [ ] **Step 1: 写失败测试**

`src/test/java/cn/zhangyis/db/storage/fil/DefaultIbdAutoExtendPolicyTest.java`：

```java
package cn.zhangyis.db.storage.fil;

import cn.zhangyis.db.domain.PageSize;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * DefaultIbdAutoExtendPolicy 测试钉死 MySQL 8.0 默认扩展边界，避免后续误写成模糊闭区间（§15）。
 */
class DefaultIbdAutoExtendPolicyTest {

    private final AutoExtendPolicy policy = new DefaultIbdAutoExtendPolicy();

    @Test
    void shouldExposePagesPerExtentFromPageSize() {
        assertEquals(256, PageSize.ofBytes(4 * 1024).pagesPerExtent());
        assertEquals(128, PageSize.ofBytes(8 * 1024).pagesPerExtent());
        assertEquals(64, PageSize.ofBytes(16 * 1024).pagesPerExtent());
        assertEquals(64, PageSize.ofBytes(32 * 1024).pagesPerExtent());
        assertEquals(64, PageSize.ofBytes(64 * 1024).pagesPerExtent());
    }

    @Test
    void shouldGrowByOnePageWhenSmallerThanOneExtent() {
        PageSize ps = PageSize.ofBytes(16 * 1024); // ppe = 64
        assertEquals(1, policy.nextIncrementPages(0, ps));
        assertEquals(1, policy.nextIncrementPages(63, ps));
    }

    @Test
    void shouldGrowByOneExtentBetweenOneAndThirtyTwoExtents() {
        PageSize ps = PageSize.ofBytes(16 * 1024); // ppe = 64
        assertEquals(64, policy.nextIncrementPages(64, ps));   // == 1 extent 边界
        assertEquals(64, policy.nextIncrementPages(2047, ps)); // < 32 extents
    }

    @Test
    void shouldGrowByFourExtentsAtOrAboveThirtyTwoExtents() {
        PageSize ps = PageSize.ofBytes(16 * 1024); // ppe = 64
        assertEquals(256, policy.nextIncrementPages(2048, ps)); // == 32 extents 边界
        assertEquals(256, policy.nextIncrementPages(5000, ps));
    }

    @Test
    void shouldScaleBoundariesWithPageSize() {
        PageSize ps = PageSize.ofBytes(4 * 1024); // ppe = 256
        assertEquals(1, policy.nextIncrementPages(255, ps));
        assertEquals(256, policy.nextIncrementPages(256, ps));
        assertEquals(1024, policy.nextIncrementPages(32 * 256, ps));
    }
}
```

- [ ] **Step 2: 运行确认失败**

Run: `... --tests "cn.zhangyis.db.storage.fil.DefaultIbdAutoExtendPolicyTest"`
Expected: 编译失败（`AutoExtendPolicy` / `DefaultIbdAutoExtendPolicy` 不存在）。

- [ ] **Step 3: 写接口**

`AutoExtendPolicy.java`：

```java
package cn.zhangyis.db.storage.fil;

import cn.zhangyis.db.domain.PageSize;

/**
 * 表空间自动扩展策略（Strategy）。根据当前文件大小和页大小决定一次扩展多少页。
 * 实现必须返回 >=1 的页数，且边界值用单元测试钉死，避免误写成模糊闭区间（设计 §8、§15）。
 */
public interface AutoExtendPolicy {

    /**
     * 计算本次扩展页数。
     *
     * @param currentSizeInPages 当前文件大小页数（非负）。
     * @param pageSize 实例级页大小，用于推导 extent 页数。
     * @return 本次应扩展的页数，>=1。
     */
    long nextIncrementPages(long currentSizeInPages, PageSize pageSize);
}
```

- [ ] **Step 4: 写最小实现**

`DefaultIbdAutoExtendPolicy.java`：

```java
package cn.zhangyis.db.storage.fil;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageSize;

/**
 * MySQL 8.0 file-per-table / general tablespace 默认自动扩展策略（设计 §8）：
 * 当前大小小于 1 个 extent 时逐页扩展；介于 1 与 32 个 extent 之间每次 1 个 extent；
 * 达到或超过 32 个 extent 后每次 4 个 extent，以提升大文件顺序性。
 *
 * <p>extent 页数复用 {@link PageSize#pagesPerExtent()}（4KB→256、8KB→128、16KB/32KB/64KB→64），
 * 不在此写死 64（设计 §6.1）。
 *
 * <p>简化点：Configured/Undo/FixedSize 等其它策略后续单独实现；AUTOEXTEND_SIZE 暂不支持。
 */
public final class DefaultIbdAutoExtendPolicy implements AutoExtendPolicy {

    /**
     * 切换到“每次 4 个 extent”所需的 extent 个数阈值（含）。
     */
    private static final long FOUR_EXTENT_THRESHOLD = 32L;

    @Override
    public long nextIncrementPages(long currentSizeInPages, PageSize pageSize) {
        if (currentSizeInPages < 0) {
            throw new DatabaseValidationException("current size must be non-negative: " + currentSizeInPages);
        }
        if (pageSize == null) {
            throw new DatabaseValidationException("page size must not be null");
        }
        long pagesPerExtent = pageSize.pagesPerExtent();
        if (currentSizeInPages < pagesPerExtent) {
            return 1L;
        }
        if (currentSizeInPages < FOUR_EXTENT_THRESHOLD * pagesPerExtent) {
            return pagesPerExtent;
        }
        return 4L * pagesPerExtent;
    }
}
```

- [ ] **Step 5: 运行确认通过**

Run: `... --tests "cn.zhangyis.db.storage.fil.DefaultIbdAutoExtendPolicyTest"`
Expected: PASS。

- [ ] **Step 6: 提交**

```bash
git add src/main/java/cn/zhangyis/db/storage/fil/AutoExtendPolicy.java \
        src/main/java/cn/zhangyis/db/storage/fil/DefaultIbdAutoExtendPolicy.java \
        src/test/java/cn/zhangyis/db/storage/fil/DefaultIbdAutoExtendPolicyTest.java
git commit -m "feat(fil): add AutoExtendPolicy with MySQL 8.0 default boundaries"
```

---

## Task 2: 锁守卫 + 生命周期闩 + 文件大小锁 + 预留锁接口

**Files:**
- Create: `ResourceGuard.java`、`TablespaceLifecycleLatch.java`、`FileSizeLock.java`
- Create: `DataFileHandleLock.java`、`PageIoRangeLock.java`、`FsyncLock.java`（预留接口）
- Test: `src/test/java/cn/zhangyis/db/storage/fil/FilLockTest.java`

- [ ] **Step 1: 写失败测试**

`FilLockTest.java`：

```java
package cn.zhangyis.db.storage.fil;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * FilLockTest 固定物理文件锁的核心语义：S 可并发、X 排他于 S、Guard 释放正确。
 * 不进事务死锁检测；这里只验证物理锁本身的互斥与释放路径。
 */
class FilLockTest {

    @Test
    void exclusiveShouldBeAvailableAfterSharedGuardClosed() {
        TablespaceLifecycleLatch latch = new TablespaceLifecycleLatch();
        try (ResourceGuard ignored = latch.acquireShared()) {
            // 持有 S
        }
        // S 已释放，X 应能立即拿到（用一个独立线程 tryExclusive 验证不阻塞）
        assertTrue(tryExclusive(latch), "exclusive should be acquirable after shared released");
    }

    @Test
    void exclusiveShouldBeBlockedWhileSharedHeld() throws InterruptedException {
        TablespaceLifecycleLatch latch = new TablespaceLifecycleLatch();
        try (ResourceGuard ignored = latch.acquireShared()) {
            assertFalse(tryExclusive(latch), "exclusive must not be acquirable while shared held");
        }
    }

    @Test
    void fileSizeLockShouldBeReleasedByGuard() {
        FileSizeLock lock = new FileSizeLock();
        try (ResourceGuard ignored = lock.acquire()) {
            assertFalse(tryFileSize(lock), "file size lock must be held inside guard");
        }
        assertTrue(tryFileSize(lock), "file size lock must be released after guard closed");
    }

    private boolean tryExclusive(TablespaceLifecycleLatch latch) {
        AtomicBoolean acquired = new AtomicBoolean(false);
        CountDownLatch done = new CountDownLatch(1);
        Thread t = new Thread(() -> {
            ResourceGuard g = latch.tryAcquireExclusive(50, TimeUnit.MILLISECONDS);
            if (g != null) {
                acquired.set(true);
                g.close();
            }
            done.countDown();
        });
        t.start();
        try {
            done.await(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return acquired.get();
    }

    private boolean tryFileSize(FileSizeLock lock) {
        AtomicBoolean acquired = new AtomicBoolean(false);
        CountDownLatch done = new CountDownLatch(1);
        Thread t = new Thread(() -> {
            ResourceGuard g = lock.tryAcquire(50, TimeUnit.MILLISECONDS);
            if (g != null) {
                acquired.set(true);
                g.close();
            }
            done.countDown();
        });
        t.start();
        try {
            done.await(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return acquired.get();
    }
}
```

- [ ] **Step 2: 运行确认失败**

Run: `... --tests "cn.zhangyis.db.storage.fil.FilLockTest"`
Expected: 编译失败（锁类/方法不存在）。

- [ ] **Step 3: 写 ResourceGuard**

`ResourceGuard.java`：

```java
package cn.zhangyis.db.storage.fil;

/**
 * RAII 风格释放守卫。配合 try-with-resources 使用，{@link #close()} 收窄为不抛受检异常，
 * 保证锁/资源在作用域结束时确定性释放（AGENTS.md Guard 模式）。
 */
public interface ResourceGuard extends AutoCloseable {

    /**
     * 释放守卫持有的资源（如解锁）。不得抛受检异常，便于在 try-with-resources 中无样板地释放。
     */
    @Override
    void close();
}
```

- [ ] **Step 4: 写 TablespaceLifecycleLatch**

`TablespaceLifecycleLatch.java`：

```java
package cn.zhangyis.db.storage.fil;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * #1 表空间生命周期闩（S/X）。保护 open/close/discard/drop/truncate 与普通 page IO 之间的生命周期关系：
 * 普通 read/write/flush/recovery 持 S（共享、可并发）；drop/truncate/discard/close 持 X（排他，获取即 drain 掉所有 S 持有者）。
 *
 * <p>它是物理文件锁，不进入数据库事务锁系统，也不进死锁检测；等待只靠 timeout/IO error/drain。
 * 基于 {@link ReentrantReadWriteLock}（禁用 synchronized，AGENTS.md）。
 *
 * <p>加锁顺序（设计 §8.1/§18）：Lifecycle → DataFileHandle(#2,预留) → FileSize → PageIoRange(#4,预留) → Fsync(#5,预留)。
 */
public final class TablespaceLifecycleLatch {

    /**
     * 共享锁：普通 IO 持有，允许多读并发。
     */
    private final ReentrantReadWriteLock.ReadLock sharedLock;

    /**
     * 排他锁：生命周期变更持有，排他于一切 IO。
     */
    private final ReentrantReadWriteLock.WriteLock exclusiveLock;

    public TablespaceLifecycleLatch() {
        ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        this.sharedLock = lock.readLock();
        this.exclusiveLock = lock.writeLock();
    }

    /**
     * 获取共享 S 闩，用于普通 page read/write/autoextend。
     *
     * @return 释放守卫，close 时解 S。
     */
    public ResourceGuard acquireShared() {
        sharedLock.lock();
        return sharedLock::unlock;
    }

    /**
     * 获取排他 X 闩，用于 drop/truncate/discard/close。写锁获取即等待所有 S 持有者离开（drain）。
     *
     * @return 释放守卫，close 时解 X。
     */
    public ResourceGuard acquireExclusive() {
        exclusiveLock.lock();
        return exclusiveLock::unlock;
    }

    /**
     * 限时尝试获取排他 X 闩。物理锁不进 Wait-For Graph，等待必须有超时上界（设计 §8.1）。
     *
     * @param timeout 超时时长。
     * @param unit 时间单位。
     * @return 成功返回释放守卫，超时返回 null。
     */
    public ResourceGuard tryAcquireExclusive(long timeout, TimeUnit unit) {
        try {
            if (exclusiveLock.tryLock(timeout, unit)) {
                return exclusiveLock::unlock;
            }
            return null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }
}
```

- [ ] **Step 5: 写 FileSizeLock**

`FileSizeLock.java`：

```java
package cn.zhangyis.db.storage.fil;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * #3 文件大小锁（X-only）。保护 currentSizeInPages、freeLimit 与文件尾零填充：autoextend 在持有 Lifecycle(S)
 * 的同时排他持有它，串行化文件增长。持有它时不得等待任何 page latch（设计 §8.1），避免 autoextend 与 flush 互锁。
 *
 * <p>物理文件锁，不进死锁检测；基于 {@link ReentrantLock}。
 */
public final class FileSizeLock {

    /**
     * 文件大小排他锁。owner 为正在执行 autoextend 的线程。
     */
    private final ReentrantLock lock = new ReentrantLock();

    /**
     * 获取文件大小锁。
     *
     * @return 释放守卫，close 时解锁。
     */
    public ResourceGuard acquire() {
        lock.lock();
        return lock::unlock;
    }

    /**
     * 限时尝试获取文件大小锁。
     *
     * @param timeout 超时时长。
     * @param unit 时间单位。
     * @return 成功返回释放守卫，超时返回 null。
     */
    public ResourceGuard tryAcquire(long timeout, TimeUnit unit) {
        try {
            if (lock.tryLock(timeout, unit)) {
                return lock::unlock;
            }
            return null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }
}
```

- [ ] **Step 6: 写预留锁接口（无实现）**

`DataFileHandleLock.java`：

```java
package cn.zhangyis.db.storage.fil;

/**
 * #2 文件句柄锁（预留，本版不启用）。设计用于保护 FileChannel/mmap/预分配句柄的打开、关闭、替换。
 * 首版每个表空间整生命周期持有单个 FileChannel、不替换句柄，因此尚未行使此锁；后续接入句柄替换/恢复重开时实现。
 * 加锁顺序位于 Lifecycle 之后、FileSize 之前。
 */
public interface DataFileHandleLock {
}
```

`PageIoRangeLock.java`：

```java
package cn.zhangyis.db.storage.fil;

/**
 * #4 页范围锁（预留、可选，本版不启用）。设计用于同页/相邻页写入合并、truncate 边界、故障注入。
 * 同页并发写正常由 Buffer Pool page latch 与 flush snapshot 控制（设计 §8.1），物理层一般不需要；故预留。
 * 加锁顺序位于 FileSize 之后、Fsync 之前。
 */
public interface PageIoRangeLock {
}
```

`FsyncLock.java`：

```java
package cn.zhangyis.db.storage.fil;

/**
 * #5 fsync 限流锁（预留，本版不启用）。设计用于限制同一 data file 上并发 fsync 数量，避免后台 page cleaner
 * 重复刷同一文件。需待 flush/checkpoint 模块引入后台刷脏后才有意义；故预留。加锁顺序位于末位。
 */
public interface FsyncLock {
}
```

- [ ] **Step 7: 运行确认通过**

Run: `... --tests "cn.zhangyis.db.storage.fil.FilLockTest"`
Expected: PASS。

- [ ] **Step 8: 提交**

```bash
git add src/main/java/cn/zhangyis/db/storage/fil/ResourceGuard.java \
        src/main/java/cn/zhangyis/db/storage/fil/TablespaceLifecycleLatch.java \
        src/main/java/cn/zhangyis/db/storage/fil/FileSizeLock.java \
        src/main/java/cn/zhangyis/db/storage/fil/DataFileHandleLock.java \
        src/main/java/cn/zhangyis/db/storage/fil/PageIoRangeLock.java \
        src/main/java/cn/zhangyis/db/storage/fil/FsyncLock.java \
        src/test/java/cn/zhangyis/db/storage/fil/FilLockTest.java
git commit -m "feat(fil): add physical file lock core subset (lifecycle latch + file size lock)"
```

---

## Task 3: fil 物理 IO 异常

**Files:**
- Create: `PageOutOfBoundsException.java`、`TablespaceNotOpenException.java`、`DataFilePhysicalException.java`、`DataFileCorruptedException.java`
- Test: `src/test/java/cn/zhangyis/db/storage/fil/FilPhysicalExceptionTest.java`

- [ ] **Step 1: 写失败测试**

`FilPhysicalExceptionTest.java`：

```java
package cn.zhangyis.db.storage.fil;

import cn.zhangyis.db.common.exception.DatabaseFatalException;
import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/**
 * 固定 fil 物理 IO 异常的分类与根因保留：可恢复错误归 Runtime，物理结构损坏归 Fatal。
 */
class FilPhysicalExceptionTest {

    @Test
    void recoverableExceptionsShouldExtendRuntime() {
        assertInstanceOf(DatabaseRuntimeException.class, new PageOutOfBoundsException("oob"));
        assertInstanceOf(DatabaseRuntimeException.class, new TablespaceNotOpenException("not open"));
        assertInstanceOf(DatabaseRuntimeException.class, new DataFilePhysicalException("io"));
    }

    @Test
    void corruptedShouldBeFatalAndKeepCause() {
        Throwable cause = new IllegalStateException("misaligned");
        DataFileCorruptedException ex = new DataFileCorruptedException("corrupt", cause);
        assertInstanceOf(DatabaseFatalException.class, ex);
        assertEquals(cause, ex.getCause());
    }

    @Test
    void physicalExceptionShouldKeepCause() {
        Throwable cause = new java.io.IOException("disk");
        DataFilePhysicalException ex = new DataFilePhysicalException("write failed", cause);
        assertEquals(cause, ex.getCause());
    }
}
```

- [ ] **Step 2: 运行确认失败**

Run: `... --tests "cn.zhangyis.db.storage.fil.FilPhysicalExceptionTest"`
Expected: 编译失败（异常类不存在）。

- [ ] **Step 3: 写四个异常类**

`PageOutOfBoundsException.java`：

```java
package cn.zhangyis.db.storage.fil;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * 页号越界异常。请求的 pageNo 超过表空间当前物理大小。可恢复：调用方可先 extend 再重试。
 */
public class PageOutOfBoundsException extends DatabaseRuntimeException {

    public PageOutOfBoundsException(String message) {
        super(message);
    }

    public PageOutOfBoundsException(String message, Throwable cause) {
        super(message, cause);
    }
}
```

`TablespaceNotOpenException.java`：

```java
package cn.zhangyis.db.storage.fil;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * 表空间物理文件未打开异常。对未经 create/open 登记（或已 close）的 SpaceId 发起物理 IO。
 * 可恢复：调用方应先 open/create 对应表空间文件。
 */
public class TablespaceNotOpenException extends DatabaseRuntimeException {

    public TablespaceNotOpenException(String message) {
        super(message);
    }

    public TablespaceNotOpenException(String message, Throwable cause) {
        super(message, cause);
    }
}
```

`DataFilePhysicalException.java`：

```java
package cn.zhangyis.db.storage.fil;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * 数据文件物理 IO 异常。positional 读写、文件创建或扩展失败时抛出，必须把底层 IOException 作为 cause 保留。
 * 可恢复：调用方可重试、上报或关闭资源。
 */
public class DataFilePhysicalException extends DatabaseRuntimeException {

    public DataFilePhysicalException(String message) {
        super(message);
    }

    public DataFilePhysicalException(String message, Throwable cause) {
        super(message, cause);
    }
}
```

`DataFileCorruptedException.java`：

```java
package cn.zhangyis.db.storage.fil;

import cn.zhangyis.db.common.exception.DatabaseFatalException;

/**
 * 数据文件物理结构损坏异常。例如文件长度非整页对齐，意味着无法安全按页定位。归为致命异常，
 * 普通 IO 不能继续，须交恢复或人工处理。
 */
public class DataFileCorruptedException extends DatabaseFatalException {

    public DataFileCorruptedException(String message) {
        super(message);
    }

    public DataFileCorruptedException(String message, Throwable cause) {
        super(message, cause);
    }
}
```

- [ ] **Step 4: 运行确认通过**

Run: `... --tests "cn.zhangyis.db.storage.fil.FilPhysicalExceptionTest"`
Expected: PASS。

- [ ] **Step 5: 提交**

```bash
git add src/main/java/cn/zhangyis/db/storage/fil/PageOutOfBoundsException.java \
        src/main/java/cn/zhangyis/db/storage/fil/TablespaceNotOpenException.java \
        src/main/java/cn/zhangyis/db/storage/fil/DataFilePhysicalException.java \
        src/main/java/cn/zhangyis/db/storage/fil/DataFileCorruptedException.java \
        src/test/java/cn/zhangyis/db/storage/fil/FilPhysicalExceptionTest.java
git commit -m "feat(fil): add physical IO exception hierarchy"
```

---

## Task 4: DataFileHandle

**Files:**
- Create: `src/main/java/cn/zhangyis/db/storage/fil/DataFileHandle.java`
- Test: `src/test/java/cn/zhangyis/db/storage/fil/DataFileHandleTest.java`

依赖 Task 1（AutoExtendPolicy）、Task 2（锁）、Task 3（异常）。

- [ ] **Step 1: 写失败测试**

`DataFileHandleTest.java`：

```java
package cn.zhangyis.db.storage.fil;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * DataFileHandle 测试用真实临时文件固定物理读写/扩展/越界/关闭语义，不解析页内容。
 */
class DataFileHandleTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(7);

    @TempDir
    Path dir;

    @Test
    void shouldRoundTripPageBytes() {
        Path file = dir.resolve("t.ibd");
        try (DataFileHandle handle = DataFileHandle.create(SPACE, file, PS, PageNo.of(4))) {
            byte[] payload = pattern(PS.bytes(), (byte) 0xAB);
            handle.writePage(PageNo.of(2), ByteBuffer.wrap(payload));

            ByteBuffer dst = ByteBuffer.allocate(PS.bytes());
            handle.readPage(PageNo.of(2), dst);
            assertArrayEquals(payload, dst.array());
        }
    }

    @Test
    void shouldRejectBufferWithWrongSize() {
        Path file = dir.resolve("t.ibd");
        try (DataFileHandle handle = DataFileHandle.create(SPACE, file, PS, PageNo.of(2))) {
            ByteBuffer tooSmall = ByteBuffer.allocate(PS.bytes() - 1);
            assertThrows(DatabaseValidationException.class, () -> handle.readPage(PageNo.of(0), tooSmall));
        }
    }

    @Test
    void shouldRejectOutOfBoundsAccess() {
        Path file = dir.resolve("t.ibd");
        try (DataFileHandle handle = DataFileHandle.create(SPACE, file, PS, PageNo.of(2))) {
            ByteBuffer buf = ByteBuffer.allocate(PS.bytes());
            assertThrows(PageOutOfBoundsException.class, () -> handle.readPage(PageNo.of(2), buf));
        }
    }

    @Test
    void shouldExtendZeroFilledAndMakeNewPagesReadable() {
        Path file = dir.resolve("t.ibd");
        try (DataFileHandle handle = DataFileHandle.create(SPACE, file, PS, PageNo.of(1))) {
            ByteBuffer before = ByteBuffer.allocate(PS.bytes());
            assertThrows(PageOutOfBoundsException.class, () -> handle.readPage(PageNo.of(1), before));

            long newSize = handle.autoExtend(new DefaultIbdAutoExtendPolicy()); // size 1 < ppe → +1 page
            assertEquals(2, newSize);

            ByteBuffer after = ByteBuffer.allocate(PS.bytes());
            handle.readPage(PageNo.of(1), after);
            assertArrayEquals(new byte[PS.bytes()], after.array()); // 新页为零
        }
    }

    @Test
    void shouldRejectCreateWhenFileExists() throws IOException {
        Path file = dir.resolve("exists.ibd");
        Files.write(file, new byte[PS.bytes()]);
        assertThrows(DataFilePhysicalException.class, () -> DataFileHandle.create(SPACE, file, PS, PageNo.of(1)));
    }

    @Test
    void shouldRejectOpenWhenFileMissing() {
        Path file = dir.resolve("missing.ibd");
        assertThrows(DataFilePhysicalException.class, () -> DataFileHandle.open(SPACE, file, PS));
    }

    @Test
    void shouldRejectOpenWhenFileNotPageAligned() throws IOException {
        Path file = dir.resolve("misaligned.ibd");
        Files.write(file, new byte[PS.bytes() + 7]);
        assertThrows(DataFileCorruptedException.class, () -> DataFileHandle.open(SPACE, file, PS));
    }

    @Test
    void shouldDeriveSizeFromExistingFileLength() throws IOException {
        Path file = dir.resolve("sized.ibd");
        try (FileChannel ch = FileChannel.open(file, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            ch.write(ByteBuffer.allocate(PS.bytes() * 3));
        }
        try (DataFileHandle handle = DataFileHandle.open(SPACE, file, PS)) {
            assertEquals(3, handle.currentSizeInPages());
        }
    }

    @Test
    void shouldRejectIoAfterClose() {
        Path file = dir.resolve("closed.ibd");
        DataFileHandle handle = DataFileHandle.create(SPACE, file, PS, PageNo.of(2));
        handle.close();
        ByteBuffer buf = ByteBuffer.allocate(PS.bytes());
        assertThrows(TablespaceNotOpenException.class, () -> handle.readPage(PageNo.of(0), buf));
    }

    @Test
    void concurrentReadsDuringExtendShouldStayConsistent() throws InterruptedException {
        Path file = dir.resolve("concurrent.ibd");
        AtomicReference<Throwable> failure = new AtomicReference<>();
        try (DataFileHandle handle = DataFileHandle.create(SPACE, file, PS, PageNo.of(64))) {
            Thread reader = new Thread(() -> {
                try {
                    for (int i = 0; i < 2000; i++) {
                        ByteBuffer buf = ByteBuffer.allocate(PS.bytes());
                        handle.readPage(PageNo.of(0), buf); // 始终在 size 内
                    }
                } catch (Throwable t) {
                    failure.set(t);
                }
            });
            Thread extender = new Thread(() -> {
                try {
                    AutoExtendPolicy policy = new DefaultIbdAutoExtendPolicy();
                    long last = 64;
                    for (int i = 0; i < 20; i++) {
                        long s = handle.autoExtend(policy);
                        assertTrue(s >= last, "size must be monotonic");
                        last = s;
                    }
                } catch (Throwable t) {
                    failure.set(t);
                }
            });
            reader.start();
            extender.start();
            reader.join();
            extender.join();
        }
        assertNull(failure.get(), () -> "concurrent read/extend failed: " + failure.get());
    }

    private static byte[] pattern(int len, byte b) {
        byte[] a = new byte[len];
        java.util.Arrays.fill(a, b);
        return a;
    }
}
```

- [ ] **Step 2: 运行确认失败**

Run: `... --tests "cn.zhangyis.db.storage.fil.DataFileHandleTest"`
Expected: 编译失败（`DataFileHandle` 不存在）。

- [ ] **Step 3: 写 DataFileHandle**

`DataFileHandle.java`：

```java
package cn.zhangyis.db.storage.fil;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * 每个表空间一个的物理数据文件单元。封装打开的 FileChannel、生命周期闩(#1)、文件大小锁(#3) 和权威物理大小，
 * 真正做 positional 整页读写与文件扩展。它是纯物理视角：按 pageNo 计算文件偏移，不解析页内容、不算 checksum、
 * 不懂 segment/record（设计 §10）。
 *
 * <p>并发：read/write 持 Lifecycle(S) 可并发；autoExtend 持 Lifecycle(S)+FileSize(X)；close 持 Lifecycle(X)。
 * currentSizeInPages 用 volatile 发布，保证扩展后零填充的新页“发布前对读不可见”。同页并发写不在本层串行化，
 * 由上层 Buffer Pool page latch 负责（设计 §8.1）。
 *
 * <p>简化点：单文件；不 force（崩溃持久化/WAL 留 redo 切片）；句柄不替换（#2 预留）。
 */
final class DataFileHandle implements AutoCloseable {

    /**
     * 所属表空间编号；仅用于诊断与构造 PageId 计算偏移。
     */
    private final SpaceId spaceId;

    /**
     * 数据文件路径；诊断用，IO 走已打开的 channel。
     */
    private final Path path;

    /**
     * 实例级页大小；决定偏移换算与越界单位。
     */
    private final PageSize pageSize;

    /**
     * 已打开的文件通道；整生命周期持有，由生命周期闩保护其使用与关闭。
     */
    private final FileChannel channel;

    /**
     * #1 生命周期闩。
     */
    private final TablespaceLifecycleLatch lifecycleLatch = new TablespaceLifecycleLatch();

    /**
     * #3 文件大小锁。
     */
    private final FileSizeLock fileSizeLock = new FileSizeLock();

    /**
     * 权威物理大小（页数）。读路径读该 volatile 快照做越界检查；autoExtend 在 fileSizeLock 下零填充后再发布。
     */
    private volatile long currentSizeInPages;

    /**
     * 关闭标志。close() 在 Lifecycle(X) 下置位；IO 路径在 Lifecycle(S) 下检查，防止使用已关闭 channel。
     */
    private volatile boolean closed;

    private DataFileHandle(SpaceId spaceId, Path path, PageSize pageSize, FileChannel channel, long currentSizeInPages) {
        this.spaceId = spaceId;
        this.path = path;
        this.pageSize = pageSize;
        this.channel = channel;
        this.currentSizeInPages = currentSizeInPages;
    }

    /**
     * 创建新数据文件并零填充 initialSizeInPages 页。文件必须不存在，否则视为重复创建错误。
     *
     * @param spaceId 表空间编号。
     * @param path 文件路径。
     * @param pageSize 页大小。
     * @param initialSizeInPages 初始页数（非负）。
     * @return 已登记物理大小的句柄。
     */
    static DataFileHandle create(SpaceId spaceId, Path path, PageSize pageSize, PageNo initialSizeInPages) {
        validate(spaceId, path, pageSize);
        if (initialSizeInPages == null) {
            throw new DatabaseValidationException("initial size must not be null");
        }
        if (Files.exists(path)) {
            throw new DataFilePhysicalException("data file already exists: " + path);
        }
        FileChannel channel = null;
        try {
            channel = FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.READ, StandardOpenOption.WRITE);
            long pages = initialSizeInPages.value();
            zeroFill(channel, 0, pages, pageSize.bytes());
            return new DataFileHandle(spaceId, path, pageSize, channel, pages);
        } catch (IOException e) {
            closeQuietly(channel);
            throw new DataFilePhysicalException("create data file failed: " + path, e);
        }
    }

    /**
     * 打开已存在数据文件，size 由文件长度推导。文件必须存在且整页对齐。
     *
     * @param spaceId 表空间编号。
     * @param path 文件路径。
     * @param pageSize 页大小。
     * @return 已登记物理大小的句柄。
     */
    static DataFileHandle open(SpaceId spaceId, Path path, PageSize pageSize) {
        validate(spaceId, path, pageSize);
        if (!Files.exists(path)) {
            throw new DataFilePhysicalException("data file not found: " + path);
        }
        FileChannel channel = null;
        try {
            channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
            long length = channel.size();
            int pageBytes = pageSize.bytes();
            if (length % pageBytes != 0) {
                closeQuietly(channel);
                throw new DataFileCorruptedException("data file not page-aligned: " + path + " length=" + length);
            }
            return new DataFileHandle(spaceId, path, pageSize, channel, length / pageBytes);
        } catch (IOException e) {
            closeQuietly(channel);
            throw new DataFilePhysicalException("open data file failed: " + path, e);
        }
    }

    /**
     * 读取整页到 dst。持 Lifecycle(S) → 检关闭 → volatile size 越界检查 → positional 读满 pageSize 字节。
     *
     * @param pageNo 表空间内页号。
     * @param dst 目标缓冲，remaining 必须 == pageSize。
     */
    void readPage(PageNo pageNo, ByteBuffer dst) {
        requirePageSized(dst);
        try (ResourceGuard ignored = lifecycleLatch.acquireShared()) {
            ensureOpen();
            long offset = boundedOffset(pageNo);
            readFully(dst, offset);
        }
    }

    /**
     * 写入整页。持 Lifecycle(S) → 检关闭 → 越界检查 → positional 写满 pageSize 字节。
     * 同页并发写的互斥由上层 page latch 负责，本层不串行化。
     *
     * @param pageNo 表空间内页号。
     * @param src 源缓冲，remaining 必须 == pageSize。
     */
    void writePage(PageNo pageNo, ByteBuffer src) {
        requirePageSized(src);
        try (ResourceGuard ignored = lifecycleLatch.acquireShared()) {
            ensureOpen();
            long offset = boundedOffset(pageNo);
            writeFully(src, offset);
        }
    }

    /**
     * 按策略扩展一次：持 Lifecycle(S)+FileSize(X) → 计算增量 → 对 [oldSize, oldSize+inc) 写零页 → volatile 发布新 size。
     * 不 force（崩溃持久化留后续 redo 切片）。新页在 size 发布前对读不可见。
     *
     * @param policy 扩展策略。
     * @return 扩展后的 currentSizeInPages。
     */
    long autoExtend(AutoExtendPolicy policy) {
        if (policy == null) {
            throw new DatabaseValidationException("auto extend policy must not be null");
        }
        try (ResourceGuard s = lifecycleLatch.acquireShared(); ResourceGuard x = fileSizeLock.acquire()) {
            ensureOpen();
            long oldSize = currentSizeInPages;
            long inc = policy.nextIncrementPages(oldSize, pageSize);
            if (inc < 1) {
                throw new DatabaseValidationException("auto extend increment must be >= 1: " + inc);
            }
            zeroFill(channel, oldSize, oldSize + inc, pageSize.bytes());
            currentSizeInPages = oldSize + inc;
            return currentSizeInPages;
        }
    }

    /**
     * 当前物理大小页数（越界检查与上层分配的物理依据）。
     *
     * @return currentSizeInPages。
     */
    long currentSizeInPages() {
        return currentSizeInPages;
    }

    /**
     * 关闭文件。持 Lifecycle(X)（获取即 drain 所有 S 持有者）→ 置 closed → 关 channel。
     */
    @Override
    public void close() {
        try (ResourceGuard ignored = lifecycleLatch.acquireExclusive()) {
            if (closed) {
                return;
            }
            closed = true;
            try {
                channel.close();
            } catch (IOException e) {
                throw new DataFilePhysicalException("close data file failed: " + path, e);
            }
        }
    }

    private long boundedOffset(PageNo pageNo) {
        if (pageNo == null) {
            throw new DatabaseValidationException("page no must not be null");
        }
        long size = currentSizeInPages;
        if (pageNo.value() >= size) {
            throw new PageOutOfBoundsException("page out of bounds: space=" + spaceId.value()
                    + " pageNo=" + pageNo.value() + " size=" + size);
        }
        return PageId.of(spaceId, pageNo).offset(pageSize);
    }

    private void ensureOpen() {
        if (closed) {
            throw new TablespaceNotOpenException("data file handle closed: space=" + spaceId.value());
        }
    }

    private void requirePageSized(ByteBuffer buffer) {
        if (buffer == null) {
            throw new DatabaseValidationException("page buffer must not be null");
        }
        if (buffer.remaining() != pageSize.bytes()) {
            throw new DatabaseValidationException("page buffer remaining must equal page size: expected "
                    + pageSize.bytes() + " got " + buffer.remaining());
        }
    }

    private void readFully(ByteBuffer dst, long offset) {
        long pos = offset;
        try {
            while (dst.hasRemaining()) {
                int n = channel.read(dst, pos);
                if (n < 0) {
                    throw new DataFilePhysicalException("unexpected EOF reading page at offset " + offset + " of " + path);
                }
                pos += n;
            }
        } catch (IOException e) {
            throw new DataFilePhysicalException("read page failed at offset " + offset + " of " + path, e);
        }
    }

    private void writeFully(ByteBuffer src, long offset) {
        long pos = offset;
        try {
            while (src.hasRemaining()) {
                pos += channel.write(src, pos);
            }
        } catch (IOException e) {
            throw new DataFilePhysicalException("write page failed at offset " + offset + " of " + path, e);
        }
    }

    private static void zeroFill(FileChannel channel, long fromPage, long toPage, int pageBytes) {
        ByteBuffer zero = ByteBuffer.allocate(pageBytes);
        try {
            for (long page = fromPage; page < toPage; page++) {
                zero.clear();
                long pos = Math.multiplyExact(page, (long) pageBytes);
                while (zero.hasRemaining()) {
                    pos += channel.write(zero, pos);
                }
            }
        } catch (IOException e) {
            throw new DataFilePhysicalException("zero-fill failed [" + fromPage + "," + toPage + ")", e);
        }
    }

    private static void validate(SpaceId spaceId, Path path, PageSize pageSize) {
        if (spaceId == null) {
            throw new DatabaseValidationException("space id must not be null");
        }
        if (path == null) {
            throw new DatabaseValidationException("data file path must not be null");
        }
        if (pageSize == null) {
            throw new DatabaseValidationException("page size must not be null");
        }
    }

    private static void closeQuietly(FileChannel channel) {
        if (channel != null) {
            try {
                channel.close();
            } catch (IOException ignored) {
                // 创建/打开失败的清理路径，原始异常更重要，关闭失败忽略。
            }
        }
    }
}
```

- [ ] **Step 4: 运行确认通过**

Run: `... --tests "cn.zhangyis.db.storage.fil.DataFileHandleTest"`
Expected: PASS（含并发读/扩展一致性）。

- [ ] **Step 5: 提交**

```bash
git add src/main/java/cn/zhangyis/db/storage/fil/DataFileHandle.java \
        src/test/java/cn/zhangyis/db/storage/fil/DataFileHandleTest.java
git commit -m "feat(fil): add DataFileHandle physical page IO with lifecycle/size locks"
```

---

## Task 5: PageStore + FileChannelPageStore

**Files:**
- Create: `src/main/java/cn/zhangyis/db/storage/fil/PageStore.java`
- Create: `src/main/java/cn/zhangyis/db/storage/fil/FileChannelPageStore.java`
- Test: `src/test/java/cn/zhangyis/db/storage/fil/FileChannelPageStoreTest.java`

依赖 Task 4。

- [ ] **Step 1: 写失败测试**

`FileChannelPageStoreTest.java`：

```java
package cn.zhangyis.db.storage.fil;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * FileChannelPageStore 测试固定门面路由与生命周期，不依赖 TablespaceRegistry（物理层 registry-无关、state-无关）。
 */
class FileChannelPageStoreTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);

    @TempDir
    Path dir;

    @Test
    void shouldRoundTripThroughFacade() {
        SpaceId space = SpaceId.of(3);
        try (PageStore store = new FileChannelPageStore()) {
            store.create(space, dir.resolve("a.ibd"), PS, PageNo.of(4));
            byte[] payload = new byte[PS.bytes()];
            Arrays.fill(payload, (byte) 0x5C);
            store.writePage(PageId.of(space, PageNo.of(1)), ByteBuffer.wrap(payload));

            ByteBuffer dst = ByteBuffer.allocate(PS.bytes());
            store.readPage(PageId.of(space, PageNo.of(1)), dst);
            assertArrayEquals(payload, dst.array());
        }
    }

    @Test
    void shouldExtendThroughFacadeAndExposeNewSize() {
        SpaceId space = SpaceId.of(3);
        try (PageStore store = new FileChannelPageStore()) {
            store.create(space, dir.resolve("b.ibd"), PS, PageNo.of(1));
            PageNo newSize = store.extend(space);
            assertEquals(PageNo.of(2), newSize);
            assertEquals(PageNo.of(2), store.currentSizeInPages(space));

            ByteBuffer dst = ByteBuffer.allocate(PS.bytes());
            store.readPage(PageId.of(space, PageNo.of(1)), dst); // 新页可读
        }
    }

    @Test
    void shouldRejectIoOnUnopenedSpace() {
        try (PageStore store = new FileChannelPageStore()) {
            ByteBuffer dst = ByteBuffer.allocate(PS.bytes());
            assertThrows(TablespaceNotOpenException.class,
                    () -> store.readPage(PageId.of(SpaceId.of(99), PageNo.of(0)), dst));
        }
    }

    @Test
    void shouldRejectDuplicateRegistration() {
        SpaceId space = SpaceId.of(3);
        try (PageStore store = new FileChannelPageStore()) {
            store.create(space, dir.resolve("c.ibd"), PS, PageNo.of(1));
            assertThrows(DatabaseValidationException.class,
                    () -> store.create(space, dir.resolve("c2.ibd"), PS, PageNo.of(1)));
        }
    }

    @Test
    void shouldRejectIoAfterClose() {
        SpaceId space = SpaceId.of(3);
        try (PageStore store = new FileChannelPageStore()) {
            store.create(space, dir.resolve("d.ibd"), PS, PageNo.of(2));
            store.close(space);
            ByteBuffer dst = ByteBuffer.allocate(PS.bytes());
            assertThrows(TablespaceNotOpenException.class,
                    () -> store.readPage(PageId.of(space, PageNo.of(0)), dst));
        }
    }
}
```

- [ ] **Step 2: 运行确认失败**

Run: `... --tests "cn.zhangyis.db.storage.fil.FileChannelPageStoreTest"`
Expected: 编译失败（`PageStore`/`FileChannelPageStore` 不存在）。

- [ ] **Step 3: 写 PageStore 接口**

`PageStore.java`：

```java
package cn.zhangyis.db.storage.fil;

import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;

import java.nio.ByteBuffer;
import java.nio.file.Path;

/**
 * 物理页 IO 门面（设计 §13.3 的物理读写部分 + §10）。按 PageId 定位文件偏移做 positional 整页读写，
 * 并提供 autoextend。它是纯物理视角：registry-无关、state-无关，不解析页内容、不算 checksum、不产 redo。
 *
 * <p>普通 IO 的 NORMAL/ACTIVE 逻辑准入由上层（storage.api/fsp）经 TablespaceRegistry.require 把关，不在本层。
 * pageSize/path 由编排方在 create/open 时注入。
 */
public interface PageStore extends AutoCloseable {

    /**
     * 创建表空间物理文件并登记句柄。文件必须不存在，按 initialSizeInPages 零填充。
     *
     * @param spaceId 表空间编号。
     * @param path 数据文件路径。
     * @param pageSize 页大小。
     * @param initialSizeInPages 初始页数。
     */
    void create(SpaceId spaceId, Path path, PageSize pageSize, PageNo initialSizeInPages);

    /**
     * 打开已存在表空间物理文件并登记句柄。size 由文件长度推导，须整页对齐。
     *
     * @param spaceId 表空间编号。
     * @param path 数据文件路径。
     * @param pageSize 页大小。
     */
    void open(SpaceId spaceId, Path path, PageSize pageSize);

    /**
     * 读取整页到 dst。dst.remaining() 必须 == pageSize；未登记抛 TablespaceNotOpenException；越界抛 PageOutOfBoundsException。
     *
     * @param pageId 物理页定位键。
     * @param dst 目标缓冲。
     */
    void readPage(PageId pageId, ByteBuffer dst);

    /**
     * 写入整页。src.remaining() 必须 == pageSize。同页并发写串行化由上层 page latch 负责。
     *
     * @param pageId 物理页定位键。
     * @param src 源缓冲。
     */
    void writePage(PageId pageId, ByteBuffer src);

    /**
     * 对表空间执行一次自动扩展，返回扩展后的 currentSizeInPages。
     *
     * @param spaceId 表空间编号。
     * @return 扩展后的物理大小页数。
     */
    PageNo extend(SpaceId spaceId);

    /**
     * 查询当前物理大小页数。
     *
     * @param spaceId 表空间编号。
     * @return 当前物理大小页数。
     */
    PageNo currentSizeInPages(SpaceId spaceId);

    /**
     * 关闭并注销单个表空间句柄。
     *
     * @param spaceId 表空间编号。
     */
    void close(SpaceId spaceId);

    /**
     * 关闭全部句柄。
     */
    @Override
    void close();
}
```

- [ ] **Step 4: 写 FileChannelPageStore 实现**

`FileChannelPageStore.java`：

```java
package cn.zhangyis.db.storage.fil;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * {@link PageStore} 的 FileChannel 实现。维护 SpaceId 到 {@link DataFileHandle} 的映射，按 PageId 路由 positional IO。
 * registry-无关、state-无关：元数据由 create/open 注入，逻辑准入由上层负责（设计取舍 B′）。
 *
 * <p>并发：handles 用 ConcurrentHashMap 保护映射；单个表空间的物理生命周期与 IO 互斥由其 DataFileHandle 内的
 * 生命周期闩/文件大小锁负责。
 *
 * <p>简化点：mmap/预分配 adapter 未实现；单文件表空间，多文件跨文件路由属编排层。
 */
@Slf4j
public final class FileChannelPageStore implements PageStore {

    /**
     * 自动扩展策略；默认 MySQL 8.0 file-per-table 边界。
     */
    private final AutoExtendPolicy autoExtendPolicy;

    /**
     * 已登记物理句柄。key 为表空间编号。
     */
    private final ConcurrentMap<SpaceId, DataFileHandle> handles = new ConcurrentHashMap<>();

    public FileChannelPageStore() {
        this(new DefaultIbdAutoExtendPolicy());
    }

    public FileChannelPageStore(AutoExtendPolicy autoExtendPolicy) {
        if (autoExtendPolicy == null) {
            throw new DatabaseValidationException("auto extend policy must not be null");
        }
        this.autoExtendPolicy = autoExtendPolicy;
    }

    @Override
    public void create(SpaceId spaceId, Path path, PageSize pageSize, PageNo initialSizeInPages) {
        validateSpaceId(spaceId);
        if (handles.containsKey(spaceId)) {
            throw new DatabaseValidationException("tablespace already registered: " + spaceId.value());
        }
        DataFileHandle handle = DataFileHandle.create(spaceId, path, pageSize, initialSizeInPages);
        if (handles.putIfAbsent(spaceId, handle) != null) {
            handle.close();
            throw new DatabaseValidationException("tablespace already registered: " + spaceId.value());
        }
        log.info("created tablespace data file: space={} path={}", spaceId.value(), path);
    }

    @Override
    public void open(SpaceId spaceId, Path path, PageSize pageSize) {
        validateSpaceId(spaceId);
        if (handles.containsKey(spaceId)) {
            throw new DatabaseValidationException("tablespace already registered: " + spaceId.value());
        }
        DataFileHandle handle = DataFileHandle.open(spaceId, path, pageSize);
        if (handles.putIfAbsent(spaceId, handle) != null) {
            handle.close();
            throw new DatabaseValidationException("tablespace already registered: " + spaceId.value());
        }
        log.info("opened tablespace data file: space={} path={}", spaceId.value(), path);
    }

    @Override
    public void readPage(PageId pageId, ByteBuffer dst) {
        validatePageId(pageId);
        require(pageId.spaceId()).readPage(pageId.pageNo(), dst);
    }

    @Override
    public void writePage(PageId pageId, ByteBuffer src) {
        validatePageId(pageId);
        require(pageId.spaceId()).writePage(pageId.pageNo(), src);
    }

    @Override
    public PageNo extend(SpaceId spaceId) {
        validateSpaceId(spaceId);
        return PageNo.of(require(spaceId).autoExtend(autoExtendPolicy));
    }

    @Override
    public PageNo currentSizeInPages(SpaceId spaceId) {
        validateSpaceId(spaceId);
        return PageNo.of(require(spaceId).currentSizeInPages());
    }

    @Override
    public void close(SpaceId spaceId) {
        validateSpaceId(spaceId);
        DataFileHandle handle = handles.remove(spaceId);
        if (handle != null) {
            handle.close();
        }
    }

    @Override
    public void close() {
        // 逐个关闭并清空；remove 后 close 避免并发重复关闭同一句柄。
        for (SpaceId spaceId : handles.keySet()) {
            close(spaceId);
        }
    }

    private DataFileHandle require(SpaceId spaceId) {
        DataFileHandle handle = handles.get(spaceId);
        if (handle == null) {
            throw new TablespaceNotOpenException("tablespace not open: " + spaceId.value());
        }
        return handle;
    }

    private void validatePageId(PageId pageId) {
        if (pageId == null) {
            throw new DatabaseValidationException("page id must not be null");
        }
    }

    private void validateSpaceId(SpaceId spaceId) {
        if (spaceId == null) {
            throw new DatabaseValidationException("space id must not be null");
        }
    }
}
```

- [ ] **Step 5: 运行确认通过**

Run: `... --tests "cn.zhangyis.db.storage.fil.FileChannelPageStoreTest"`
Expected: PASS。

- [ ] **Step 6: 全量回归 + 提交**

Run 全量：`$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --console=plain`
Expected: BUILD SUCCESSFUL，全部测试通过。

```bash
git add src/main/java/cn/zhangyis/db/storage/fil/PageStore.java \
        src/main/java/cn/zhangyis/db/storage/fil/FileChannelPageStore.java \
        src/test/java/cn/zhangyis/db/storage/fil/FileChannelPageStoreTest.java
git commit -m "feat(fil): add PageStore facade with FileChannel routing"
```

- [ ] **Step 7: 刷新 GitNexus 索引（AGENTS.md 步骤 6）**

Run: `npx gitnexus analyze`
Expected: 索引刷新成功（节点/边数增加）。

---

## Self-Review（写计划后自查，已执行）

**1. Spec 覆盖**：
- §4.1 PageStore 接口 → Task 5；§4.2 FileChannelPageStore → Task 5；§4.3 DataFileHandle → Task 4；§4.4/§4.5 锁 + §4.6 预留接口 → Task 2；§4.7 AutoExtendPolicy → Task 1；§7 异常 → Task 3。
- §5 数据流、§6 并发语义 → 体现在 Task 4 实现 + Task 4 并发测试。
- §8 测试计划 → 各 Task 测试逐条覆盖（policy 边界、round-trip、越界、缓冲长度、扩展可见性、create/open 校验、close 后 IO、并发读+扩展、门面路由、未 open、重复登记）。
- §9 简化点、§10 修正点 → 落在各类 Javadoc。

**2. Placeholder 扫描**：无 TBD/TODO；每个 step 含完整可编译代码与确切命令。

**3. 类型一致性**：`ResourceGuard.close()`、`acquireShared/acquireExclusive/tryAcquireExclusive`、`FileSizeLock.acquire/tryAcquire`、`DataFileHandle.create/open/readPage/writePage/autoExtend/currentSizeInPages/close`、`PageStore.create/open/readPage/writePage/extend/currentSizeInPages/close`、异常类名在测试与实现间一致；`PageNo`/`PageSize`/`PageId`/`SpaceId` 用既有 API（`of`、`value`、`bytes`、`pagesPerExtent`、`offset`）。

**4. 歧义**：`extend` 单次增量、无 target、不引入 `NoFreeSpaceException`；越界用 `PageOutOfBoundsException`；create vs open 显式区分——均与 spec §10 一致。
