# SegmentPageAllocator + DiskSpaceManager facade（slice 2c）Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 落地 `SegmentPageAllocator`（fragment<32 vs extent 决策 + policy 取 1..4 extent，纯分配返回 Optional）与 `cn.zhangyis.db.storage.api.DiskSpaceManager` facade（tablespace/segment 生命周期 + 分配/释放 + autoextend 重试 + 用量）。

**Architecture:** allocator 编排 2b 原语、只在 currentSize 内分配（无空间 empty）；facade 持 PageStore，在 allocator 返回 empty 时 `extend`+`setCurrentSize` 重试一次，仍无则抛 `NoFreeSpaceException`。SpaceReservation/PageCursor/hint 延后。

**Tech Stack:** Java 25、JUnit Jupiter（buf+fil+mtr+@TempDir）、`ByteBuffer` 绝对读写。

**Spec:** `docs/superpowers/specs/2026-06-11-fsp-disk-space-manager-design.md`

**通用约定：**
- fsp 类位于 `cn.zhangyis.db.storage.fsp`；facade 位于新包 `cn.zhangyis.db.storage.api`。中文 Javadoc；禁 `synchronized`；禁裸异常。
- **不提交**（master）。跨页 op 先 page0 X 再 page2 X 预闩；无空间 `Optional.empty()`，autoextend 失败抛 `NoFreeSpaceException`。no-redo，不声明 crash-safe。
- 单类测试：`$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --console=plain --tests "<FQCN>"`；全量 `clean test`。

---

## File Structure

| 文件 | 职责 |
| --- | --- |
| `fsp/NoFreeSpaceException.java`（新） | 扩展一次仍无空间的可恢复异常 |
| `fsp/ExtentAllocationPolicy.java` / `DefaultExtentAllocationPolicy.java`（新） | extent 取数策略（1..4） |
| `fsp/SegmentInodeRepository.java`（改） | 加 `hasFreeFragmentSlot` |
| `fsp/SegmentPageAllocator.java`（新） | 分配编排（纯，返回 Optional） |
| `api/package-info.java` / `SegmentRef.java` / `SpaceUsage.java` / `DiskSpaceManager.java`（新） | facade + 句柄 + 用量 |

测试：`fsp/ExtentAllocationPolicyTest`、`fsp/SegmentInodeRepositoryTest`（增 1 方法）、`fsp/SegmentPageAllocatorTest`、`api/DiskSpaceManagerTest`。

---

## Task 1: NoFreeSpaceException + ExtentAllocationPolicy

**Files:**
- Create: `src/main/java/cn/zhangyis/db/storage/fsp/NoFreeSpaceException.java`
- Create: `src/main/java/cn/zhangyis/db/storage/fsp/ExtentAllocationPolicy.java`
- Create: `src/main/java/cn/zhangyis/db/storage/fsp/DefaultExtentAllocationPolicy.java`
- Test: `src/test/java/cn/zhangyis/db/storage/fsp/ExtentAllocationPolicyTest.java`

- [ ] **Step 1: 写失败测试**

```java
package cn.zhangyis.db.storage.fsp;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/**
 * ExtentAllocationPolicy 值表与 NoFreeSpaceException 可恢复分类。
 */
class ExtentAllocationPolicyTest {

    @Test
    void defaultPolicyClampsToFour() {
        ExtentAllocationPolicy p = new DefaultExtentAllocationPolicy();
        assertEquals(1, p.extentsToAcquire(0));
        assertEquals(1, p.extentsToAcquire(1));
        assertEquals(2, p.extentsToAcquire(2));
        assertEquals(3, p.extentsToAcquire(3));
        assertEquals(4, p.extentsToAcquire(4));
        assertEquals(4, p.extentsToAcquire(10));
    }

    @Test
    void noFreeSpaceIsRecoverable() {
        assertInstanceOf(DatabaseRuntimeException.class, new NoFreeSpaceException("x"));
    }
}
```

- [ ] **Step 2: 运行确认失败**（编译失败）。

- [ ] **Step 3: 写 `NoFreeSpaceException.java`**

```java
package cn.zhangyis.db.storage.fsp;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * 表空间扩展一次后仍无可用空间（设计 §17）。可恢复：调用方可放弃/回滚当前操作。
 */
public class NoFreeSpaceException extends DatabaseRuntimeException {

    public NoFreeSpaceException(String message) {
        super(message);
    }

    public NoFreeSpaceException(String message, Throwable cause) {
        super(message, cause);
    }
}
```

- [ ] **Step 4: 写 `ExtentAllocationPolicy.java`**

```java
package cn.zhangyis.db.storage.fsp;

/**
 * extent 取数策略（Strategy，设计 §7.2/§7.3）：给定 segment 已占 extent 数，返回本次应一次性获取的 extent 数（1..4）。
 */
public interface ExtentAllocationPolicy {

    /**
     * @param ownedExtentCount segment 当前已占 extent 数（三条 SEG 链长之和）。
     * @return 本次获取 extent 数，1..4。
     */
    int extentsToAcquire(long ownedExtentCount);
}
```

- [ ] **Step 5: 写 `DefaultExtentAllocationPolicy.java`**

```java
package cn.zhangyis.db.storage.fsp;

/**
 * 默认 extent 取数策略：顺序增长、封顶 4（设计 §7.2 step3 大 segment 一次最多 4 extent）。
 * ownedExtentCount<=0→1；否则 min(4, ownedExtentCount)。
 */
public final class DefaultExtentAllocationPolicy implements ExtentAllocationPolicy {

    @Override
    public int extentsToAcquire(long ownedExtentCount) {
        if (ownedExtentCount <= 0) {
            return 1;
        }
        return (int) Math.min(4L, ownedExtentCount);
    }
}
```

- [ ] **Step 6: 运行确认通过**，不提交。

---

## Task 2: SegmentInodeRepository.hasFreeFragmentSlot

**Files:**
- Modify: `src/main/java/cn/zhangyis/db/storage/fsp/SegmentInodeRepository.java`
- Modify (test): `src/test/java/cn/zhangyis/db/storage/fsp/SegmentInodeRepositoryTest.java`

- [ ] **Step 1: 在 `SegmentInodeRepositoryTest.java` 增 import 与测试**：import 区加 `import static org.junit.jupiter.api.Assertions.assertFalse;`；在最后一个 `@Test` 后、类结束 `}` 前插入：

```java
    @Test
    void hasFreeFragmentSlotReflectsUsage() {
        withRepo((repo, mtr) -> {
            int slot = repo.allocateSlot(mtr, SPACE, SegmentId.of(1), SegmentPurpose.INDEX_LEAF);
            assertTrue(repo.hasFreeFragmentSlot(mtr, SPACE, slot));
            for (int i = 0; i < SegmentInodeLayout.FRAGMENT_SLOT_COUNT; i++) {
                repo.setFragmentPage(mtr, SPACE, slot, i, java.util.Optional.of(PageNo.of(40 + i)));
            }
            assertFalse(repo.hasFreeFragmentSlot(mtr, SPACE, slot));
        });
    }
```

- [ ] **Step 2: 运行确认失败** — `--tests "cn.zhangyis.db.storage.fsp.SegmentInodeRepositoryTest"`，编译失败（hasFreeFragmentSlot 不存在）。

- [ ] **Step 3: 在 `SegmentInodeRepository.java` 的 `requireFreeFragmentSlot` 方法之后插入**

```java
    /** 是否存在空 fragment 槽（值为 0），即该 segment 已用 fragment 页 &lt; 32。供分配层做 fragment vs extent 决策（S）。 */
    public boolean hasFreeFragmentSlot(MiniTransaction mtr, SpaceId spaceId, int inodeSlot) {
        requireMtr(mtr);
        requireSpace(spaceId);
        requireSlot(spaceId, inodeSlot);
        PageGuard g = mtr.getPage(pool, inodePage(spaceId), PageLatchMode.SHARED);
        for (int f = 0; f < SegmentInodeLayout.FRAGMENT_SLOT_COUNT; f++) {
            if (g.readLong(SegmentInodeLayout.fragmentSlotOffset(inodeSlot, f)) == 0) {
                return true;
            }
        }
        return false;
    }
```

- [ ] **Step 4: 运行确认通过**，不提交。

---

## Task 3: SegmentPageAllocator

**Files:**
- Create: `src/main/java/cn/zhangyis/db/storage/fsp/SegmentPageAllocator.java`
- Test: `src/test/java/cn/zhangyis/db/storage/fsp/SegmentPageAllocatorTest.java`

- [ ] **Step 1: 写失败测试**

```java
package cn.zhangyis.db.storage.fsp;

import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SegmentId;
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
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * SegmentPageAllocator 集成测试：fragment 路径（前 32 页）、满 32 转 extent 路径、无空间返回 empty、policy 多 extent。
 */
class SegmentPageAllocatorTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(7);

    @TempDir
    Path dir;

    private SpaceHeaderSnapshot fresh(long sizePages) {
        return new SpaceHeaderSnapshot(SPACE, PS, 0,
                PageNo.of(sizePages), PageNo.of(0), 1L,
                FlstBase.EMPTY, FlstBase.EMPTY, FlstBase.EMPTY,
                PageNo.of(2), 0L, 80046, 1L);
    }

    interface Ctx {
        void run(SpaceHeaderRepository header, SegmentInodeRepository inode, Flst flst,
                 SegmentPageAllocator alloc, MiniTransactionManager mgr);
    }

    private void withAlloc(long sizePages, ExtentAllocationPolicy policy, Ctx body) {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(sizePages));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 16)) {
            SpaceHeaderRepository header = new SpaceHeaderRepository(pool);
            ExtentDescriptorRepository xdes = new ExtentDescriptorRepository(pool, PS);
            SegmentInodeRepository inode = new SegmentInodeRepository(pool, PS);
            Flst flst = new Flst(pool);
            FreeExtentService free = new FreeExtentService(pool, PS, header, xdes, flst);
            SegmentSpaceService seg = new SegmentSpaceService(pool, PS, header, inode, xdes, flst, free);
            SegmentPageAllocator alloc = new SegmentPageAllocator(pool, inode, flst, seg, policy);
            MiniTransactionManager mgr = new MiniTransactionManager();
            MiniTransaction init = mgr.begin();
            header.initialize(init, fresh(sizePages));
            mgr.commit(init);
            body.run(header, inode, flst, alloc, mgr);
        }
    }

    @Test
    void fragmentFirst32ThenExtentPath() {
        withAlloc(192, new DefaultExtentAllocationPolicy(), (header, inode, flst, alloc, mgr) -> {
            MiniTransaction a = mgr.begin();
            int slot = inode.allocateSlot(a, SPACE, SegmentId.of(1), SegmentPurpose.INDEX_LEAF);
            mgr.commit(a);

            MiniTransaction m = mgr.begin();
            for (int i = 0; i < 32; i++) {
                assertEquals(Optional.of(PageId.of(SPACE, PageNo.of(64 + i))),
                        alloc.allocatePage(m, SPACE, slot));
            }
            // 第 33 次：fragment 槽满 → extent 路径 → assign extent2 → page 128
            assertEquals(Optional.of(PageId.of(SPACE, PageNo.of(128))), alloc.allocatePage(m, SPACE, slot));
            mgr.commit(m);
        });
    }

    @Test
    void returnsEmptyWhenNoSpaceAndNoAutoextend() {
        withAlloc(64, new DefaultExtentAllocationPolicy(), (header, inode, flst, alloc, mgr) -> {
            MiniTransaction a = mgr.begin();
            int slot = inode.allocateSlot(a, SPACE, SegmentId.of(1), SegmentPurpose.INDEX_LEAF);
            mgr.commit(a);

            MiniTransaction m = mgr.begin();
            assertTrue(alloc.allocatePage(m, SPACE, slot).isEmpty()); // 仅 extent0 系统，无可用空间，且 allocator 不扩文件
            mgr.commit(m);
        });
    }

    @Test
    void honorsPolicyAcquiringMultipleExtents() {
        ExtentAllocationPolicy two = ownedExtentCount -> 2;
        withAlloc(256, two, (header, inode, flst, alloc, mgr) -> {
            MiniTransaction a = mgr.begin();
            int slot = inode.allocateSlot(a, SPACE, SegmentId.of(1), SegmentPurpose.INDEX_LEAF);
            mgr.commit(a);

            MiniTransaction m = mgr.begin();
            for (int i = 0; i < 32; i++) {
                alloc.allocatePage(m, SPACE, slot); // 32 fragment 页
            }
            // 第 33 次：extent 路径，policy 返回 2 → assign extent2+extent3，从 extent2 分配 page 128
            assertEquals(Optional.of(PageId.of(SPACE, PageNo.of(128))), alloc.allocatePage(m, SPACE, slot));
            // owned = 2：extent2 在 NOT_FULL，extent3 在 SEG_FREE
            assertEquals(1L, flst.length(m, SPACE, inode.notFullExtentListBaseAddr(SPACE, slot)));
            assertEquals(1L, flst.length(m, SPACE, inode.freeExtentListBaseAddr(SPACE, slot)));
            mgr.commit(m);
        });
    }
}
```

- [ ] **Step 2: 运行确认失败**（编译失败，SegmentPageAllocator 不存在）。

- [ ] **Step 3: 写 `SegmentPageAllocator.java`**

```java
package cn.zhangyis.db.storage.fsp;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.mtr.MiniTransaction;

import java.util.Optional;

/**
 * Segment 页分配编排（设计 §7.2）。fragment 已用 &lt;32 → fragment 路径；否则 segment-extent 路径
 * （无可用 extent 则按 {@link ExtentAllocationPolicy} 一次取 1..4 个 extent 再试）。
 * <b>纯分配</b>：只在当前 currentSize 内分配，无空间返回 {@link Optional#empty()}，不扩文件、不抛 NoFreeSpace
 * （autoextend 在 DiskSpaceManager facade）。
 *
 * <p>锁序：allocatePage 开头预闩 page0 X→page2 X（§18），后续 hasFreeFragmentSlot/Flst.length（S 降级）与
 * 2b 原语（reentrant X）不逆序、不触发同页 S→X。本片 no-redo。
 */
public final class SegmentPageAllocator {

    private final BufferPool pool;
    private final SegmentInodeRepository inodeRepo;
    private final Flst flst;
    private final SegmentSpaceService segSpace;
    private final ExtentAllocationPolicy policy;

    public SegmentPageAllocator(BufferPool pool, SegmentInodeRepository inodeRepo, Flst flst,
                                SegmentSpaceService segSpace, ExtentAllocationPolicy policy) {
        if (pool == null || inodeRepo == null || flst == null || segSpace == null || policy == null) {
            throw new DatabaseValidationException("SegmentPageAllocator dependencies must not be null");
        }
        this.pool = pool;
        this.inodeRepo = inodeRepo;
        this.flst = flst;
        this.segSpace = segSpace;
        this.policy = policy;
    }

    /**
     * 为 segment 分配一个页，仅当前 currentSize 内。fragment 槽未满走 fragment 路径，否则 extent 路径
     * （必要时按 policy 取 1..4 extent 再试）。无空间返回 empty（facade 负责 autoextend）。
     */
    public Optional<PageId> allocatePage(MiniTransaction mtr, SpaceId spaceId, int inodeSlot) {
        if (mtr == null) {
            throw new DatabaseValidationException("mini transaction must not be null");
        }
        if (spaceId == null) {
            throw new DatabaseValidationException("space id must not be null");
        }
        latchSpaceThenInode(mtr, spaceId);
        if (inodeRepo.hasFreeFragmentSlot(mtr, spaceId, inodeSlot)) {
            return segSpace.allocateFragmentPage(mtr, spaceId, inodeSlot)
                    .map(pageNo -> PageId.of(spaceId, pageNo));
        }
        Optional<PageNo> fromExtent = segSpace.allocatePageFromSegmentExtents(mtr, spaceId, inodeSlot);
        if (fromExtent.isPresent()) {
            return Optional.of(PageId.of(spaceId, fromExtent.get()));
        }
        long owned = ownedExtentCount(mtr, spaceId, inodeSlot);
        int toAcquire = policy.extentsToAcquire(owned);
        boolean assignedAny = false;
        for (int i = 0; i < toAcquire; i++) {
            if (segSpace.assignExtentToSegment(mtr, spaceId, inodeSlot).isPresent()) {
                assignedAny = true;
            } else {
                break;
            }
        }
        if (!assignedAny) {
            return Optional.empty();
        }
        return segSpace.allocatePageFromSegmentExtents(mtr, spaceId, inodeSlot)
                .map(pageNo -> PageId.of(spaceId, pageNo));
    }

    private long ownedExtentCount(MiniTransaction mtr, SpaceId spaceId, int inodeSlot) {
        return flst.length(mtr, spaceId, inodeRepo.freeExtentListBaseAddr(spaceId, inodeSlot))
                + flst.length(mtr, spaceId, inodeRepo.notFullExtentListBaseAddr(spaceId, inodeSlot))
                + flst.length(mtr, spaceId, inodeRepo.fullExtentListBaseAddr(spaceId, inodeSlot));
    }

    private void latchSpaceThenInode(MiniTransaction mtr, SpaceId spaceId) {
        mtr.getPage(pool, PageId.of(spaceId, PageNo.of(0)), PageLatchMode.EXCLUSIVE);
        mtr.getPage(pool, PageId.of(spaceId, PageNo.of(2)), PageLatchMode.EXCLUSIVE);
    }
}
```

- [ ] **Step 4: 运行确认通过**，不提交。

---

## Task 4: api 包 — SegmentRef + SpaceUsage + DiskSpaceManager

**Files:**
- Create: `src/main/java/cn/zhangyis/db/storage/api/package-info.java`
- Create: `src/main/java/cn/zhangyis/db/storage/api/SegmentRef.java`
- Create: `src/main/java/cn/zhangyis/db/storage/api/SpaceUsage.java`
- Create: `src/main/java/cn/zhangyis/db/storage/api/DiskSpaceManager.java`
- Test: `src/test/java/cn/zhangyis/db/storage/api/DiskSpaceManagerTest.java`

- [ ] **Step 1: 写失败测试**

```java
package cn.zhangyis.db.storage.api;

import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SegmentId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.fil.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.PageStore;
import cn.zhangyis.db.storage.fsp.NoFreeSpaceException;
import cn.zhangyis.db.storage.fsp.SegmentPurpose;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * DiskSpaceManager facade 集成测试：建表空间/用量、建段、分配（fragment→extent）、autoextend、NoFreeSpace、释放、drop 回收。
 */
class DiskSpaceManagerTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(7);

    @TempDir
    Path dir;

    private interface Body {
        void run(DiskSpaceManager dsm, MiniTransactionManager mgr);
    }

    private void withDsm(Body body) {
        PageStore store = new FileChannelPageStore();
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 16)) {
            DiskSpaceManager dsm = new DiskSpaceManager(pool, store, PS);
            MiniTransactionManager mgr = new MiniTransactionManager();
            body.run(dsm, mgr);
        }
    }

    @Test
    void createTablespaceThenUsageAndSegment() {
        withDsm((dsm, mgr) -> {
            MiniTransaction m = mgr.begin();
            dsm.createTablespace(m, SPACE, dir.resolve("s.ibd"), PageNo.of(128));
            SpaceUsage u = dsm.usage(m, SPACE);
            assertEquals(PageNo.of(128), u.currentSizeInPages());
            assertEquals(PageNo.of(0), u.freeLimitPageNo());
            assertEquals(1L, u.nextSegmentId());
            SegmentRef ref = dsm.createSegment(m, SPACE, SegmentPurpose.INDEX_LEAF);
            assertEquals(0, ref.inodeSlot());
            assertEquals(SegmentId.of(1), ref.segmentId());
            assertEquals(2L, dsm.usage(m, SPACE).nextSegmentId());
            mgr.commit(m);
        });
    }

    @Test
    void allocateFreeReallocateRecyclesPage() {
        withDsm((dsm, mgr) -> {
            MiniTransaction m = mgr.begin();
            dsm.createTablespace(m, SPACE, dir.resolve("s.ibd"), PageNo.of(128));
            SegmentRef ref = dsm.createSegment(m, SPACE, SegmentPurpose.INDEX_LEAF);
            PageId p = dsm.allocatePage(m, ref);
            assertEquals(PageId.of(SPACE, PageNo.of(64)), p);
            dsm.freePage(m, ref, p);
            assertEquals(PageId.of(SPACE, PageNo.of(64)), dsm.allocatePage(m, ref));
            mgr.commit(m);
        });
    }

    @Test
    void allocateAutoextendsWhenExhausted() {
        withDsm((dsm, mgr) -> {
            MiniTransaction m = mgr.begin();
            dsm.createTablespace(m, SPACE, dir.resolve("s.ibd"), PageNo.of(128));
            SegmentRef ref = dsm.createSegment(m, SPACE, SegmentPurpose.INDEX_LEAF);
            for (int i = 0; i < 32; i++) {
                assertEquals(PageId.of(SPACE, PageNo.of(64 + i)), dsm.allocatePage(m, ref));
            }
            // 第 33 次：128 页耗尽 → autoextend → 192 → extent2 首页 128
            assertEquals(PageId.of(SPACE, PageNo.of(128)), dsm.allocatePage(m, ref));
            assertEquals(PageNo.of(192), dsm.usage(m, SPACE).currentSizeInPages());
            mgr.commit(m);
        });
    }

    @Test
    void allocateThrowsNoFreeSpaceOnTinyTablespace() {
        withDsm((dsm, mgr) -> {
            MiniTransaction m = mgr.begin();
            dsm.createTablespace(m, SPACE, dir.resolve("s.ibd"), PageNo.of(4));
            SegmentRef ref = dsm.createSegment(m, SPACE, SegmentPurpose.INDEX_LEAF);
            assertThrows(NoFreeSpaceException.class, () -> dsm.allocatePage(m, ref));
            mgr.commit(m);
        });
    }

    @Test
    void dropSegmentReclaimsAndAllowsSlotReuse() {
        withDsm((dsm, mgr) -> {
            MiniTransaction m = mgr.begin();
            dsm.createTablespace(m, SPACE, dir.resolve("s.ibd"), PageNo.of(192));
            SegmentRef ref = dsm.createSegment(m, SPACE, SegmentPurpose.INDEX_LEAF);
            for (int i = 0; i < 33; i++) {
                dsm.allocatePage(m, ref); // 32 fragment + 1 extent 页（extent2）
            }
            dsm.dropSegment(m, ref);
            // 槽复用：再建段拿回 slot 0
            SegmentRef ref2 = dsm.createSegment(m, SPACE, SegmentPurpose.INDEX_LEAF);
            assertEquals(0, ref2.inodeSlot());
            // drop 后仍可在回收空间上继续分配（不抛）
            PageId again = dsm.allocatePage(m, ref2);
            assertEquals(PageId.of(SPACE, PageNo.of(64)), again);
            mgr.commit(m);
        });
    }
}
```

- [ ] **Step 2: 运行确认失败**（编译失败，api 类不存在）。

- [ ] **Step 3: 写 `api/package-info.java`**

```java
/**
 * 存储引擎对上层（btree/dd）暴露的稳定门面层（设计 §4 storage.api、§13.1）。
 * 仅返回领域对象与句柄（SegmentRef/SpaceUsage/PageId），不暴露内部 frame、裸文件、redo buffer。
 */
package cn.zhangyis.db.storage.api;
```

- [ ] **Step 4: 写 `api/SegmentRef.java`**

```java
package cn.zhangyis.db.storage.api;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.SegmentId;
import cn.zhangyis.db.domain.SpaceId;

/**
 * Segment 句柄：定位某表空间内一个 segment 的 inode 槽与逻辑编号。由 createSegment 返回，allocate/free/drop 传入。
 *
 * @param spaceId   所属表空间。
 * @param inodeSlot page2 inode 槽下标。
 * @param segmentId segment 逻辑编号（>0）。
 */
public record SegmentRef(SpaceId spaceId, int inodeSlot, SegmentId segmentId) {

    public SegmentRef {
        if (spaceId == null || segmentId == null) {
            throw new DatabaseValidationException("segment ref fields must not be null");
        }
        if (inodeSlot < 0) {
            throw new DatabaseValidationException("inode slot must be non-negative: " + inodeSlot);
        }
        if (segmentId.value() <= 0) {
            throw new DatabaseValidationException("segment id must be positive: " + segmentId.value());
        }
    }
}
```

- [ ] **Step 5: 写 `api/SpaceUsage.java`**

```java
package cn.zhangyis.db.storage.api;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageNo;

/**
 * 表空间用量快照：当前大小、freeLimit、下一个待分配 segment id。
 *
 * @param currentSizeInPages 当前物理大小页数。
 * @param freeLimitPageNo    已纳入 free-list 机制的页号上界。
 * @param nextSegmentId      下一个待分配 segment id。
 */
public record SpaceUsage(PageNo currentSizeInPages, PageNo freeLimitPageNo, long nextSegmentId) {

    public SpaceUsage {
        if (currentSizeInPages == null || freeLimitPageNo == null) {
            throw new DatabaseValidationException("space usage page fields must not be null");
        }
    }
}
```

- [ ] **Step 6: 写 `api/DiskSpaceManager.java`**

```java
package cn.zhangyis.db.storage.api;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.ExtentId;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SegmentId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.fil.PageStore;
import cn.zhangyis.db.storage.fsp.DefaultExtentAllocationPolicy;
import cn.zhangyis.db.storage.fsp.ExtentDescriptorRepository;
import cn.zhangyis.db.storage.fsp.FileAddress;
import cn.zhangyis.db.storage.fsp.Flst;
import cn.zhangyis.db.storage.fsp.FlstBase;
import cn.zhangyis.db.storage.fsp.FreeExtentService;
import cn.zhangyis.db.storage.fsp.NoFreeSpaceException;
import cn.zhangyis.db.storage.fsp.SegmentInodeRepository;
import cn.zhangyis.db.storage.fsp.SegmentPageAllocator;
import cn.zhangyis.db.storage.fsp.SegmentPurpose;
import cn.zhangyis.db.storage.fsp.SegmentSpaceService;
import cn.zhangyis.db.storage.fsp.SpaceHeaderRepository;
import cn.zhangyis.db.storage.fsp.SpaceHeaderSnapshot;
import cn.zhangyis.db.storage.mtr.MiniTransaction;

import java.nio.file.Path;
import java.util.Optional;

/**
 * 磁盘空间管理门面（设计 §13.1、§14 Facade）。聚合 fsp 仓储/服务/分配器 + fil PageStore，对上层提供
 * tablespace/segment 生命周期、页分配/释放（含 autoextend 重试）、用量查询。返回领域对象与句柄，不暴露内部页。
 *
 * <p>autoextend：allocator 仅在当前 currentSize 内分配，返回 empty 时本门面 {@code extend} 文件 + 更新 currentSize 后
 * 重试一次，仍无则 {@link NoFreeSpaceException}。本片 no-redo，不声明 crash-safe（§15 推迟满足）。
 */
public final class DiskSpaceManager {

    /** 新建表空间写入的 server 版本号（诊断用，固定值）。 */
    private static final int SERVER_VERSION = 80046;

    private final BufferPool pool;
    private final PageStore pageStore;
    private final PageSize pageSize;
    private final SpaceHeaderRepository headerRepo;
    private final ExtentDescriptorRepository xdes;
    private final SegmentInodeRepository inodeRepo;
    private final Flst flst;
    private final FreeExtentService freeExtents;
    private final SegmentSpaceService segSpace;
    private final SegmentPageAllocator allocator;

    public DiskSpaceManager(BufferPool pool, PageStore pageStore, PageSize pageSize) {
        if (pool == null || pageStore == null || pageSize == null) {
            throw new DatabaseValidationException("DiskSpaceManager dependencies must not be null");
        }
        this.pool = pool;
        this.pageStore = pageStore;
        this.pageSize = pageSize;
        this.headerRepo = new SpaceHeaderRepository(pool);
        this.xdes = new ExtentDescriptorRepository(pool, pageSize);
        this.inodeRepo = new SegmentInodeRepository(pool, pageSize);
        this.flst = new Flst(pool);
        this.freeExtents = new FreeExtentService(pool, pageSize, headerRepo, xdes, flst);
        this.segSpace = new SegmentSpaceService(pool, pageSize, headerRepo, inodeRepo, xdes, flst, freeExtents);
        this.allocator = new SegmentPageAllocator(pool, inodeRepo, flst, segSpace, new DefaultExtentAllocationPolicy());
    }

    /** 建表空间：物理建文件（fil）→ 初始化 page0 header（currentSize=initialSize、freeLimit=0、nextSegmentId=1、三链空）→ 保留系统 extent0。 */
    public void createTablespace(MiniTransaction mtr, SpaceId spaceId, Path path, PageNo initialSizePages) {
        requireMtr(mtr);
        requireSpace(spaceId);
        if (path == null) {
            throw new DatabaseValidationException("path must not be null");
        }
        if (initialSizePages == null) {
            throw new DatabaseValidationException("initial size must not be null");
        }
        pageStore.create(spaceId, path, pageSize, initialSizePages);
        SpaceHeaderSnapshot fresh = new SpaceHeaderSnapshot(spaceId, pageSize, 0,
                initialSizePages, PageNo.of(0), 1L,
                FlstBase.EMPTY, FlstBase.EMPTY, FlstBase.EMPTY,
                PageNo.of(2), 0L, SERVER_VERSION, 1L);
        headerRepo.initialize(mtr, fresh);
        xdes.reserveSystemExtent(mtr, spaceId);
    }

    /** 打开已存在表空间物理文件（fil）。 */
    public void openTablespace(SpaceId spaceId, Path path) {
        requireSpace(spaceId);
        if (path == null) {
            throw new DatabaseValidationException("path must not be null");
        }
        pageStore.open(spaceId, path, pageSize);
    }

    /** 关闭表空间物理句柄（fil）。 */
    public void closeTablespace(SpaceId spaceId) {
        requireSpace(spaceId);
        pageStore.close(spaceId);
    }

    /** 建 segment：分配 segment id（page0）+ inode 槽（page2），返回句柄。 */
    public SegmentRef createSegment(MiniTransaction mtr, SpaceId spaceId, SegmentPurpose purpose) {
        requireMtr(mtr);
        requireSpace(spaceId);
        if (purpose == null) {
            throw new DatabaseValidationException("segment purpose must not be null");
        }
        long segId = headerRepo.allocateNextSegmentId(mtr, spaceId);
        int slot = inodeRepo.allocateSlot(mtr, spaceId, SegmentId.of(segId), purpose);
        return new SegmentRef(spaceId, slot, SegmentId.of(segId));
    }

    /** 为 segment 分配一个页；当前空间不足则扩展文件一次再试，仍不足抛 NoFreeSpaceException。 */
    public PageId allocatePage(MiniTransaction mtr, SegmentRef ref) {
        requireMtr(mtr);
        requireRef(ref);
        Optional<PageId> first = allocator.allocatePage(mtr, ref.spaceId(), ref.inodeSlot());
        if (first.isPresent()) {
            return first.get();
        }
        PageNo newSize = pageStore.extend(ref.spaceId());
        headerRepo.setCurrentSizeInPages(mtr, ref.spaceId(), newSize);
        Optional<PageId> second = allocator.allocatePage(mtr, ref.spaceId(), ref.inodeSlot());
        if (second.isPresent()) {
            return second.get();
        }
        throw new NoFreeSpaceException("no free space for segment " + ref.segmentId().value()
                + " in tablespace " + ref.spaceId().value());
    }

    /** 释放一个属于该 segment 的页。 */
    public void freePage(MiniTransaction mtr, SegmentRef ref, PageId pageId) {
        requireMtr(mtr);
        requireRef(ref);
        segSpace.freePage(mtr, ref.spaceId(), ref.inodeSlot(), pageId);
    }

    /** 删除 segment：释放其全部 fragment 页与 extent（归还 FSP_FREE）后清空 inode 槽。 */
    public void dropSegment(MiniTransaction mtr, SegmentRef ref) {
        requireMtr(mtr);
        requireRef(ref);
        SpaceId spaceId = ref.spaceId();
        int slot = ref.inodeSlot();
        mtr.getPage(pool, PageId.of(spaceId, PageNo.of(0)), PageLatchMode.EXCLUSIVE);
        mtr.getPage(pool, PageId.of(spaceId, PageNo.of(2)), PageLatchMode.EXCLUSIVE);
        for (int f = 0; f < 32; f++) {
            Optional<PageNo> fragment = inodeRepo.getFragmentPage(mtr, spaceId, slot, f);
            if (fragment.isPresent()) {
                segSpace.freePage(mtr, spaceId, slot, PageId.of(spaceId, fragment.get()));
            }
        }
        releaseSegmentExtents(mtr, spaceId, inodeRepo.freeExtentListBaseAddr(spaceId, slot));
        releaseSegmentExtents(mtr, spaceId, inodeRepo.notFullExtentListBaseAddr(spaceId, slot));
        releaseSegmentExtents(mtr, spaceId, inodeRepo.fullExtentListBaseAddr(spaceId, slot));
        inodeRepo.freeSlot(mtr, spaceId, slot);
    }

    /** 用量快照。 */
    public SpaceUsage usage(MiniTransaction mtr, SpaceId spaceId) {
        requireMtr(mtr);
        requireSpace(spaceId);
        SpaceHeaderSnapshot h = headerRepo.read(mtr, spaceId);
        return new SpaceUsage(h.currentSizeInPages(), h.freeLimitPageNo(), h.nextSegmentId());
    }

    /** 把一条 segment extent 链上的所有 extent 逐个摘下并归还 FSP_FREE。 */
    private void releaseSegmentExtents(MiniTransaction mtr, SpaceId spaceId, FileAddress base) {
        while (true) {
            FileAddress head = flst.getFirst(mtr, spaceId, base);
            if (head.isNull()) {
                break;
            }
            ExtentId ext = xdes.extentIdOfNode(spaceId, head);
            flst.remove(mtr, spaceId, base, head);
            freeExtents.returnFreeExtent(mtr, spaceId, ext);
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

    private static void requireRef(SegmentRef ref) {
        if (ref == null) {
            throw new DatabaseValidationException("segment ref must not be null");
        }
    }
}
```

- [ ] **Step 7: 运行确认通过**（DiskSpaceManagerTest 全部）。不提交。

---

## Task 5: 全量回归 + GitNexus

- [ ] **Step 1: 全量回归** — `clean test`，期望 BUILD SUCCESSFUL。
- [ ] **Step 2: 刷新 GitNexus 索引** — `npx gitnexus analyze`；失败记录并重试。
- [ ] **Step 3: 不提交。**

---

## Self-Review（写计划后自查，已执行）

**1. Spec 覆盖：** §4 NoFreeSpaceException→Task1；§5 policy→Task1；§6 hasFreeFragmentSlot→Task2；§7 SegmentPageAllocator→Task3；§8 SegmentRef/SpaceUsage/DiskSpaceManager（create/open/close/createSegment/allocate+autoextend/freePage/dropSegment/usage）→Task4；§10 异常→各 Task；§11 测试（policy 值、fragment→extent、empty、autoextend、NoFreeSpace size=4、drop 回收+槽复用）→Task1/3/4。

**2. Placeholder 扫描：** 无 TBD/TODO；每步含完整代码与命令。

**3. 类型一致性：** `ExtentAllocationPolicy.extentsToAcquire(long)→int`；`SegmentPageAllocator.allocatePage→Optional<PageId>`；`DiskSpaceManager.{createTablespace,openTablespace,closeTablespace,createSegment→SegmentRef,allocatePage→PageId,freePage,dropSegment,usage→SpaceUsage}`；复用 2a/2b：`SegmentSpaceService.{allocateFragmentPage,allocatePageFromSegmentExtents,assignExtentToSegment,freePage}`、`FreeExtentService.returnFreeExtent`、`ExtentDescriptorRepository.extentIdOfNode`、`Flst.{getFirst,remove,length}`、`SegmentInodeRepository.{allocateSlot,freeSlot,getFragmentPage,setFragmentPage,hasFreeFragmentSlot,freeExtentListBaseAddr,notFullExtentListBaseAddr,fullExtentListBaseAddr}`、`SpaceHeaderRepository.{initialize,read,allocateNextSegmentId,setCurrentSizeInPages}`、`PageStore.{create,open,close,extend}`、`SpaceHeaderSnapshot` 13 字段、`FlstBase.EMPTY`。

**4. 锁序/并发：** allocator 与 dropSegment 预闩 page0 X→page2 X；createSegment 天然 page0(allocateNextSegmentId)→page2(allocateSlot)；facade.allocatePage 在 allocator 持锁后 extend(fil)+setCurrentSize(page0 X 重入)+retry，§18 允许持页 latch 时扩展。NoFreeSpace 仅 size<1extent（测试 size=4）可达。
