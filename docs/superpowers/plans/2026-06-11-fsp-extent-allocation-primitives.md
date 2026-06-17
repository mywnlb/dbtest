# fsp extent 管理与页分配原语（slice 2b）Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 落地 extent 级机制 + 页分配原语：freeLimit 填充、FSP_FREE 取/还、全局与 segment 链迁移、fragment 页分配、segment extent 页分配、释放回收，全程维护 XDES state/owner/bitmap、FLST 链、inode 计数/fragment 槽一致。

**Architecture:** 在 slice-1 仓储 + slice-2a `Flst` 之上加 3 块：`ExtentDescriptorRepository` 扩展（extent↔节点地址、bitmap 查询）、`FreeExtentService`（全局 free 生命周期，仅 page0）、`SegmentSpaceService`（segment 侧分配/释放，page0↔page2）。跨页 op 按 §18 先 page0 X 再 page2 X 预闩，避免逆序与同页 S→X。

**Tech Stack:** Java 25、JUnit Jupiter（buf `LruBufferPool` + fil `FileChannelPageStore` + mtr `MiniTransactionManager` + `@TempDir`）、`ByteBuffer` 绝对读写。

**Spec:** `docs/superpowers/specs/2026-06-11-fsp-extent-allocation-primitives-design.md`

**通用约定：**
- 类位于 `cn.zhangyis.db.storage.fsp`。中文 Javadoc；禁 `synchronized`；禁裸异常（用 `DatabaseValidationException`/`FspMetadataException`）。
- **不提交**（master），每步只写代码 + 跑测试。
- 跨页 op 开头预闩 page0 X（FreeExtentService）或 page0 X→page2 X（SegmentSpaceService）；无空间返回 `Optional.empty()`（不抛异常），损坏/非法抛领域异常。
- no-redo 原型：注释/测试名不得声称 crash-safe。
- 单类测试：`$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --console=plain --tests "<FQCN>"`；全量去 `--tests` 用 `clean test`。
- 测试前置：`store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(N))`（N=128 → extent0 系统 + extent1 可用；N=256 → extent1/2/3 可用）；`try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 16))`；先 `SpaceHeaderRepository.initialize`（currentSize=N、freeLimit=0、三链 EMPTY、nextSegmentId=1、firstInode=2）。

---

## File Structure

| 文件 | 职责 |
| --- | --- |
| `ExtentDescriptorRepository.java`（改） | 加 listNodeAddr/extentIdOfNode + bitmap 查询（firstFreePageIndex/allocatedPageCount/isFull/isEmpty） |
| `FreeExtentService.java`（新） | freeLimit 填充、acquire/return free extent、全局 fragment 页分配 + 全局链迁移 |
| `SegmentSpaceService.java`（新） | fragment 页分配（含 inode 记录）、assign extent、segment extent 页分配、释放页 |

测试：`ExtentDescriptorAllocTest`（新）、`FreeExtentServiceTest`（新）、`SegmentSpaceServiceTest`（新）。

---

## Task 1: ExtentDescriptorRepository 扩展（地址互转 + bitmap 查询）

**Files:**
- Modify: `src/main/java/cn/zhangyis/db/storage/fsp/ExtentDescriptorRepository.java`
- Test: `src/test/java/cn/zhangyis/db/storage/fsp/ExtentDescriptorAllocTest.java`

- [ ] **Step 1: 写失败测试 `ExtentDescriptorAllocTest.java`**

```java
package cn.zhangyis.db.storage.fsp;

import cn.zhangyis.db.domain.ExtentId;
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
import java.util.OptionalInt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * ExtentDescriptorRepository 2b 扩展测试：节点地址互转、bitmap 首空/计数/满空判定。
 */
class ExtentDescriptorAllocTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(7);

    @TempDir
    Path dir;

    private interface Body {
        void run(ExtentDescriptorRepository repo, MiniTransaction mtr);
    }

    private void withRepo(Body body) {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(128));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 8)) {
            ExtentDescriptorRepository repo = new ExtentDescriptorRepository(pool, PS);
            MiniTransactionManager mgr = new MiniTransactionManager();
            MiniTransaction mtr = mgr.begin();
            body.run(repo, mtr);
            mgr.commit(mtr);
        }
    }

    @Test
    void nodeAddrRoundTrip() {
        withRepo((repo, mtr) -> {
            ExtentId e = ExtentId.of(SPACE, 1);
            FileAddress node = repo.listNodeAddr(e);
            assertEquals(e, repo.extentIdOfNode(SPACE, node));
            ExtentId e2 = ExtentId.of(SPACE, 0);
            assertEquals(e2, repo.extentIdOfNode(SPACE, repo.listNodeAddr(e2)));
        });
    }

    @Test
    void extentIdOfNodeRejectsBadOffsetOrPage() {
        withRepo((repo, mtr) -> {
            assertThrows(FspMetadataException.class,
                    () -> repo.extentIdOfNode(SPACE, FileAddress.of(PageNo.of(0), 999999)));
            assertThrows(FspMetadataException.class,
                    () -> repo.extentIdOfNode(SPACE, FileAddress.of(PageNo.of(1), 300)));
        });
    }

    @Test
    void bitmapQueriesReflectAllocation() {
        withRepo((repo, mtr) -> {
            ExtentId e = ExtentId.of(SPACE, 1);
            repo.initFree(mtr, e);
            assertTrue(repo.isEmpty(mtr, e));
            assertFalse(repo.isFull(mtr, e));
            assertEquals(0, repo.allocatedPageCount(mtr, e));
            assertEquals(OptionalInt.of(0), repo.firstFreePageIndex(mtr, e));

            repo.setPageAllocated(mtr, e, 0, true);
            repo.setPageAllocated(mtr, e, 1, true);
            assertEquals(2, repo.allocatedPageCount(mtr, e));
            assertEquals(OptionalInt.of(2), repo.firstFreePageIndex(mtr, e));

            int pe = PS.pagesPerExtent();
            for (int i = 0; i < pe; i++) {
                repo.setPageAllocated(mtr, e, i, true);
            }
            assertTrue(repo.isFull(mtr, e));
            assertEquals(OptionalInt.empty(), repo.firstFreePageIndex(mtr, e));
            assertEquals(pe, repo.allocatedPageCount(mtr, e));
        });
    }
}
```

- [ ] **Step 2: 运行确认失败** — `--tests "cn.zhangyis.db.storage.fsp.ExtentDescriptorAllocTest"`，编译失败（新方法不存在）。

- [ ] **Step 3: 在 `ExtentDescriptorRepository.java` 增方法**：先在 import 区加 `import java.util.OptionalInt;`（已 import `java.util.Optional`）。然后在类内（`reserveSystemExtent` 方法之后、`writeState` 之前任意处，建议紧接 `read` 之后）插入：

```java
    /** 该 extent 的 FLST 链节点起址 = page0 内 entry 的 prev 字段偏移；供 Flst/分配层把 extent 入链。 */
    public FileAddress listNodeAddr(ExtentId extentId) {
        int base = entryOffset(extentId);
        return FileAddress.of(PageNo.of(0), base + ExtentDescriptorLayout.PREV);
    }

    /** 反向：由链节点地址还原 ExtentId。节点必在 page0、偏移须按 ENTRY_SIZE 对齐，否则视为页上链指针损坏。 */
    public ExtentId extentIdOfNode(SpaceId spaceId, FileAddress nodeAddr) {
        if (spaceId == null) {
            throw new DatabaseValidationException("space id must not be null");
        }
        if (nodeAddr == null || nodeAddr.isNull()) {
            throw new DatabaseValidationException("node address must be concrete");
        }
        if (nodeAddr.pageNo().value() != 0) {
            throw new FspMetadataException("xdes list node must be on page 0: " + nodeAddr);
        }
        int rel = nodeAddr.offset() - SpaceHeaderLayout.XDES_BASE - ExtentDescriptorLayout.PREV;
        if (rel < 0 || rel % ExtentDescriptorLayout.ENTRY_SIZE != 0) {
            throw new FspMetadataException("misaligned xdes list node offset: " + nodeAddr.offset());
        }
        return ExtentId.of(spaceId, rel / ExtentDescriptorLayout.ENTRY_SIZE);
    }

    /** extent 内首个未分配页下标（S）；满则 empty。仅扫前 pagesPerExtent 位。 */
    public java.util.OptionalInt firstFreePageIndex(MiniTransaction mtr, ExtentId extentId) {
        requireMtr(mtr);
        int base = entryOffset(extentId);
        int pe = pageSize.pagesPerExtent();
        PageGuard g = mtr.getPage(pool, page0(extentId), PageLatchMode.SHARED);
        byte[] bm = g.readBytes(base + ExtentDescriptorLayout.BITMAP, ExtentDescriptorLayout.BITMAP_BYTES);
        for (int i = 0; i < pe; i++) {
            if ((bm[i / 8] & (1 << (i % 8))) == 0) {
                return java.util.OptionalInt.of(i);
            }
        }
        return java.util.OptionalInt.empty();
    }

    /** extent 内已分配页数（S），仅统计前 pagesPerExtent 位。 */
    public int allocatedPageCount(MiniTransaction mtr, ExtentId extentId) {
        requireMtr(mtr);
        int base = entryOffset(extentId);
        int pe = pageSize.pagesPerExtent();
        PageGuard g = mtr.getPage(pool, page0(extentId), PageLatchMode.SHARED);
        byte[] bm = g.readBytes(base + ExtentDescriptorLayout.BITMAP, ExtentDescriptorLayout.BITMAP_BYTES);
        int count = 0;
        for (int i = 0; i < pe; i++) {
            if ((bm[i / 8] & (1 << (i % 8))) != 0) {
                count++;
            }
        }
        return count;
    }

    /** extent 是否所有页已分配。 */
    public boolean isFull(MiniTransaction mtr, ExtentId extentId) {
        return allocatedPageCount(mtr, extentId) == pageSize.pagesPerExtent();
    }

    /** extent 是否全空。 */
    public boolean isEmpty(MiniTransaction mtr, ExtentId extentId) {
        return allocatedPageCount(mtr, extentId) == 0;
    }
```

说明：`entryOffset`、`page0`、`requireMtr`、`pool`、`pageSize` 均为本类已有私有成员/方法；`ExtentDescriptorLayout.{PREV,BITMAP,BITMAP_BYTES,ENTRY_SIZE}`、`SpaceHeaderLayout.XDES_BASE` 同包可见。

- [ ] **Step 4: 运行确认通过**（ExtentDescriptorAllocTest 全部）。不提交。

---

## Task 2: FreeExtentService

**Files:**
- Create: `src/main/java/cn/zhangyis/db/storage/fsp/FreeExtentService.java`
- Test: `src/test/java/cn/zhangyis/db/storage/fsp/FreeExtentServiceTest.java`

- [ ] **Step 1: 写失败测试 `FreeExtentServiceTest.java`**

```java
package cn.zhangyis.db.storage.fsp;

import cn.zhangyis.db.domain.ExtentId;
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
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * FreeExtentService 集成测试：freeLimit 跳过 extent0、按 extent 推进、currentSize 边界；acquire/return；
 * fragment 页分配（新建 FREE_FRAG、满迁 FULL_FRAG）。no-redo，不做 crash recovery 断言。
 */
class FreeExtentServiceTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(7);
    private static final int PE = 64; // 16KB pagesPerExtent

    @TempDir
    Path dir;

    private SpaceHeaderSnapshot fresh(long sizePages) {
        return new SpaceHeaderSnapshot(SPACE, PS, 0,
                PageNo.of(sizePages), PageNo.of(0), 1L,
                FlstBase.EMPTY, FlstBase.EMPTY, FlstBase.EMPTY,
                PageNo.of(2), 0L, 80046, 1L);
    }

    private interface Body {
        void run(SpaceHeaderRepository header, ExtentDescriptorRepository xdes, Flst flst,
                 FreeExtentService svc, MiniTransactionManager mgr, BufferPool pool);
    }

    private void withSvc(long sizePages, Body body) {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(sizePages));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 16)) {
            SpaceHeaderRepository header = new SpaceHeaderRepository(pool);
            ExtentDescriptorRepository xdes = new ExtentDescriptorRepository(pool, PS);
            Flst flst = new Flst(pool);
            FreeExtentService svc = new FreeExtentService(pool, PS, header, xdes, flst);
            MiniTransactionManager mgr = new MiniTransactionManager();
            MiniTransaction init = mgr.begin();
            header.initialize(init, fresh(sizePages));
            mgr.commit(init);
            body.run(header, xdes, flst, svc, mgr, pool);
        }
    }

    @Test
    void fillSkipsSystemExtentAndAdvancesByExtent() {
        withSvc(192, (header, xdes, flst, svc, mgr, pool) -> {
            MiniTransaction m = mgr.begin();
            Optional<ExtentId> first = svc.fillFreeListStep(m, SPACE);
            assertEquals(Optional.of(ExtentId.of(SPACE, 1)), first); // extent0 skipped
            Optional<ExtentId> second = svc.fillFreeListStep(m, SPACE);
            assertEquals(Optional.of(ExtentId.of(SPACE, 2)), second);
            Optional<ExtentId> none = svc.fillFreeListStep(m, SPACE); // 192/64=3 extents, extent3 would exceed
            assertTrue(none.isEmpty());
            // freeLimit advanced past extent2 (192 pages = 3*64)
            assertEquals(PageNo.of(192), header.read(m, SPACE).freeLimitPageNo());
            // both on FSP_FREE
            assertEquals(2L, flst.length(m, SPACE, header.freeExtentListBaseAddr(SPACE)));
            mgr.commit(m);
        });
    }

    @Test
    void acquireFillsThenPopsAndReturnsRecycle() {
        withSvc(192, (header, xdes, flst, svc, mgr, pool) -> {
            MiniTransaction m = mgr.begin();
            Optional<ExtentId> a = svc.acquireFreeExtent(m, SPACE);
            assertEquals(Optional.of(ExtentId.of(SPACE, 1)), a);
            Optional<ExtentId> b = svc.acquireFreeExtent(m, SPACE);
            assertEquals(Optional.of(ExtentId.of(SPACE, 2)), b);
            Optional<ExtentId> c = svc.acquireFreeExtent(m, SPACE);
            assertTrue(c.isEmpty()); // exhausted
            // return extent 1 -> FSP_FREE has it again
            svc.returnFreeExtent(m, SPACE, ExtentId.of(SPACE, 1));
            assertEquals(1L, flst.length(m, SPACE, header.freeExtentListBaseAddr(SPACE)));
            assertEquals(Optional.of(ExtentId.of(SPACE, 1)), svc.acquireFreeExtent(m, SPACE));
            mgr.commit(m);
        });
    }

    @Test
    void allocateFragmentPagesFromFreeFragThenFull() {
        withSvc(128, (header, xdes, flst, svc, mgr, pool) -> {
            MiniTransaction m = mgr.begin();
            // first fragment page creates a FREE_FRAG extent (extent 1), page 64
            Optional<PageId> p0 = svc.allocateFragmentPage(m, SPACE);
            assertEquals(Optional.of(PageId.of(SPACE, PageNo.of(64))), p0);
            assertEquals(ExtentState.FREE_FRAG, xdes.read(m, ExtentId.of(SPACE, 1)).state());
            assertEquals(1L, flst.length(m, SPACE, header.freeFragExtentListBaseAddr(SPACE)));
            // allocate remaining PE-1 pages -> extent becomes FULL_FRAG
            for (int i = 1; i < PE; i++) {
                assertEquals(Optional.of(PageId.of(SPACE, PageNo.of(64 + i))), svc.allocateFragmentPage(m, SPACE));
            }
            assertEquals(ExtentState.FULL_FRAG, xdes.read(m, ExtentId.of(SPACE, 1)).state());
            assertEquals(0L, flst.length(m, SPACE, header.freeFragExtentListBaseAddr(SPACE)));
            assertEquals(1L, flst.length(m, SPACE, header.fullFragExtentListBaseAddr(SPACE)));
            assertTrue(xdes.isFull(m, ExtentId.of(SPACE, 1)));
            // no more space (only extent1 existed)
            assertTrue(svc.allocateFragmentPage(m, SPACE).isEmpty());
            mgr.commit(m);
        });
    }
}
```

- [ ] **Step 2: 运行确认失败** — 编译失败（FreeExtentService 不存在）。

- [ ] **Step 3: 写 `FreeExtentService.java`**

```java
package cn.zhangyis.db.storage.fsp;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.ExtentId;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.mtr.MiniTransaction;

import java.util.Optional;
import java.util.OptionalInt;

/**
 * 表空间全局空间生命周期（设计 §7、§8）：freeLimit 填充把原始文件页材料化为 FREE extent 入 FSP_FREE，
 * acquire/return FREE extent，从 FSP_FREE_FRAG 分配 fragment 页并维护 FREE↔FREE_FRAG↔FULL_FRAG 迁移。
 * 仅碰 page0；每个写 op 开头预闩 page0 X，使后续读取（getFirst/firstFreePageIndex）降级、避免同页 S→X。
 *
 * <p>简化点：只材料化整 extent（不处理 currentSize 非对齐尾页）；跳过系统 extent0；本片 no-redo，写页只标脏、
 * 不产 redo、不声明 crash-safe（设计 §15 推迟满足）。无空间以 {@link Optional#empty()} 表达，由 2c 决定 autoextend。
 */
public final class FreeExtentService {

    /** 受控页来源，用于跨方法的 page0 预闩。 */
    private final BufferPool pool;

    /** 实例级页大小；决定 pagesPerExtent 与 extent 首页号。 */
    private final PageSize pageSize;

    /** SpaceHeader 仓储：freeLimit/currentSize 读写、三个全局链 base 地址。 */
    private final SpaceHeaderRepository headerRepo;

    /** XDES 仓储：extent state/bitmap/节点地址。 */
    private final ExtentDescriptorRepository xdes;

    /** FLST 链表原语。 */
    private final Flst flst;

    public FreeExtentService(BufferPool pool, PageSize pageSize, SpaceHeaderRepository headerRepo,
                             ExtentDescriptorRepository xdes, Flst flst) {
        if (pool == null || pageSize == null || headerRepo == null || xdes == null || flst == null) {
            throw new DatabaseValidationException("FreeExtentService dependencies must not be null");
        }
        this.pool = pool;
        this.pageSize = pageSize;
        this.headerRepo = headerRepo;
        this.xdes = xdes;
        this.flst = flst;
    }

    private void latchPage0(MiniTransaction mtr, SpaceId spaceId) {
        mtr.getPage(pool, PageId.of(spaceId, PageNo.of(0)), PageLatchMode.EXCLUSIVE);
    }

    /**
     * 材料化下一个非系统、整体在 currentSize 之内的 extent 到 FSP_FREE，并推进 freeLimit；越界返回 empty。
     * 数据流：读 header(freeLimit/currentSize/pageSize) → 跳过 extent0 → initFree + addLast(FSP_FREE) → 推进 freeLimit。
     */
    public Optional<ExtentId> fillFreeListStep(MiniTransaction mtr, SpaceId spaceId) {
        requireArgs(mtr, spaceId);
        latchPage0(mtr, spaceId);
        long pe = pageSize.pagesPerExtent();
        SpaceHeaderSnapshot h = headerRepo.read(mtr, spaceId);
        long currentSize = h.currentSizeInPages().value();
        long freeLimit = h.freeLimitPageNo().value();
        while (true) {
            long extentNo = freeLimit / pe;
            if ((extentNo + 1) * pe > currentSize) {
                return Optional.empty();
            }
            freeLimit += pe;
            headerRepo.setFreeLimitPageNo(mtr, spaceId, PageNo.of(freeLimit));
            if (extentNo == 0) {
                continue; // 系统 extent0 不入 free-list
            }
            ExtentId ext = ExtentId.of(spaceId, extentNo);
            xdes.initFree(mtr, ext);
            flst.addLast(mtr, spaceId, headerRepo.freeExtentListBaseAddr(spaceId), xdes.listNodeAddr(ext));
            return Optional.of(ext);
        }
    }

    /** 取一个可用 FREE extent：弹 FSP_FREE 头；空则先 fill 再弹；真满返回 empty。 */
    public Optional<ExtentId> acquireFreeExtent(MiniTransaction mtr, SpaceId spaceId) {
        requireArgs(mtr, spaceId);
        latchPage0(mtr, spaceId);
        FileAddress freeBase = headerRepo.freeExtentListBaseAddr(spaceId);
        FileAddress head = flst.getFirst(mtr, spaceId, freeBase);
        if (!head.isNull()) {
            ExtentId ext = xdes.extentIdOfNode(spaceId, head);
            flst.remove(mtr, spaceId, freeBase, head);
            return Optional.of(ext);
        }
        Optional<ExtentId> filled = fillFreeListStep(mtr, spaceId);
        if (filled.isEmpty()) {
            return Optional.empty();
        }
        FileAddress head2 = flst.getFirst(mtr, spaceId, freeBase);
        flst.remove(mtr, spaceId, freeBase, head2);
        return filled;
    }

    /** 回收一个 extent 为 FREE（initFree：state FREE/owner 0/bitmap 清/prev-next NULL）并入 FSP_FREE。调用方须先把它移出原链。 */
    public void returnFreeExtent(MiniTransaction mtr, SpaceId spaceId, ExtentId extentId) {
        requireArgs(mtr, spaceId);
        if (extentId == null) {
            throw new DatabaseValidationException("extent id must not be null");
        }
        latchPage0(mtr, spaceId);
        xdes.initFree(mtr, extentId); // extent0 会被 initFree 拒绝
        flst.addLast(mtr, spaceId, headerRepo.freeExtentListBaseAddr(spaceId), xdes.listNodeAddr(extentId));
    }

    /**
     * 从 FSP_FREE_FRAG 分配一个 fragment 页。无 FREE_FRAG 则从 FSP_FREE acquire 一个并置 FREE_FRAG。
     * 取首个空闲页置位；若 extent 因此变满，迁 FSP_FREE_FRAG→FSP_FULL_FRAG。无空间返回 empty。
     */
    public Optional<PageId> allocateFragmentPage(MiniTransaction mtr, SpaceId spaceId) {
        requireArgs(mtr, spaceId);
        latchPage0(mtr, spaceId);
        FileAddress ffBase = headerRepo.freeFragExtentListBaseAddr(spaceId);
        FileAddress head = flst.getFirst(mtr, spaceId, ffBase);
        ExtentId frag;
        if (!head.isNull()) {
            frag = xdes.extentIdOfNode(spaceId, head);
        } else {
            Optional<ExtentId> acq = acquireFreeExtent(mtr, spaceId);
            if (acq.isEmpty()) {
                return Optional.empty();
            }
            frag = acq.get();
            xdes.writeState(mtr, frag, ExtentState.FREE_FRAG);
            flst.addLast(mtr, spaceId, ffBase, xdes.listNodeAddr(frag));
        }
        OptionalInt idxOpt = xdes.firstFreePageIndex(mtr, frag);
        if (idxOpt.isEmpty()) {
            throw new FspMetadataException("FREE_FRAG extent unexpectedly full: " + frag.extentNo());
        }
        int idx = idxOpt.getAsInt();
        xdes.setPageAllocated(mtr, frag, idx, true);
        long pageNo = frag.firstPageNo(pageSize).value() + idx;
        if (xdes.isFull(mtr, frag)) {
            flst.remove(mtr, spaceId, ffBase, xdes.listNodeAddr(frag));
            xdes.writeState(mtr, frag, ExtentState.FULL_FRAG);
            flst.addLast(mtr, spaceId, headerRepo.fullFragExtentListBaseAddr(spaceId), xdes.listNodeAddr(frag));
        }
        return Optional.of(PageId.of(spaceId, PageNo.of(pageNo)));
    }

    private static void requireArgs(MiniTransaction mtr, SpaceId spaceId) {
        if (mtr == null) {
            throw new DatabaseValidationException("mini transaction must not be null");
        }
        if (spaceId == null) {
            throw new DatabaseValidationException("space id must not be null");
        }
    }
}
```

- [ ] **Step 4: 运行确认通过**（FreeExtentServiceTest 全部）。不提交。

---

## Task 3: SegmentSpaceService 分配

**Files:**
- Create: `src/main/java/cn/zhangyis/db/storage/fsp/SegmentSpaceService.java`
- Test: `src/test/java/cn/zhangyis/db/storage/fsp/SegmentSpaceServiceTest.java`

- [ ] **Step 1: 写失败测试 `SegmentSpaceServiceTest.java`**

```java
package cn.zhangyis.db.storage.fsp;

import cn.zhangyis.db.domain.ExtentId;
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
 * SegmentSpaceService 分配测试：fragment 页分配 + inode 记录 + usedPageCount；assign extent；从 segment extent 分配页与 SEG 链迁移。
 */
class SegmentSpaceServiceTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(7);
    private static final int PE = 64;

    @TempDir
    Path dir;

    private SpaceHeaderSnapshot fresh(long sizePages) {
        return new SpaceHeaderSnapshot(SPACE, PS, 0,
                PageNo.of(sizePages), PageNo.of(0), 1L,
                FlstBase.EMPTY, FlstBase.EMPTY, FlstBase.EMPTY,
                PageNo.of(2), 0L, 80046, 1L);
    }

    interface Ctx {
        void run(SpaceHeaderRepository header, ExtentDescriptorRepository xdes, SegmentInodeRepository inode,
                 Flst flst, FreeExtentService free, SegmentSpaceService seg, MiniTransactionManager mgr);
    }

    private void withCtx(long sizePages, Ctx body) {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(sizePages));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 16)) {
            SpaceHeaderRepository header = new SpaceHeaderRepository(pool);
            ExtentDescriptorRepository xdes = new ExtentDescriptorRepository(pool, PS);
            SegmentInodeRepository inode = new SegmentInodeRepository(pool, PS);
            Flst flst = new Flst(pool);
            FreeExtentService free = new FreeExtentService(pool, PS, header, xdes, flst);
            SegmentSpaceService seg = new SegmentSpaceService(pool, PS, header, inode, xdes, flst, free);
            MiniTransactionManager mgr = new MiniTransactionManager();
            MiniTransaction init = mgr.begin();
            header.initialize(init, fresh(sizePages));
            mgr.commit(init);
            body.run(header, xdes, inode, flst, free, seg, mgr);
        }
    }

    @Test
    void allocateFragmentPageRecordsInInodeAndCounts() {
        withCtx(128, (header, xdes, inode, flst, free, seg, mgr) -> {
            MiniTransaction a = mgr.begin();
            int slot = inode.allocateSlot(a, SPACE, SegmentId.of(1), SegmentPurpose.INDEX_LEAF);
            mgr.commit(a);

            MiniTransaction m = mgr.begin();
            Optional<PageNo> p = seg.allocateFragmentPage(m, SPACE, slot);
            assertEquals(Optional.of(PageNo.of(64)), p);
            assertEquals(Optional.of(PageNo.of(64)), inode.getFragmentPage(m, SPACE, slot, 0));
            assertEquals(1L, inode.read(m, SPACE, slot).usedPageCount());
            mgr.commit(m);
        });
    }

    @Test
    void assignExtentSetsFsegOwnerAndSegFreeList() {
        withCtx(128, (header, xdes, inode, flst, free, seg, mgr) -> {
            MiniTransaction a = mgr.begin();
            int slot = inode.allocateSlot(a, SPACE, SegmentId.of(5), SegmentPurpose.INDEX_LEAF);
            mgr.commit(a);

            MiniTransaction m = mgr.begin();
            Optional<ExtentId> ext = seg.assignExtentToSegment(m, SPACE, slot);
            assertEquals(Optional.of(ExtentId.of(SPACE, 1)), ext);
            assertEquals(ExtentState.FSEG, xdes.read(m, ExtentId.of(SPACE, 1)).state());
            assertEquals(Optional.of(SegmentId.of(5)), xdes.read(m, ExtentId.of(SPACE, 1)).ownerSegment());
            assertEquals(1L, flst.length(m, SPACE, inode.freeExtentListBaseAddr(SPACE, slot)));
            mgr.commit(m);
        });
    }

    @Test
    void allocatePageFromSegmentExtentMovesFreeToNotFullThenFull() {
        withCtx(128, (header, xdes, inode, flst, free, seg, mgr) -> {
            MiniTransaction a = mgr.begin();
            int slot = inode.allocateSlot(a, SPACE, SegmentId.of(1), SegmentPurpose.INDEX_LEAF);
            seg.assignExtentToSegment(a, SPACE, slot);
            mgr.commit(a);

            MiniTransaction m = mgr.begin();
            // first page: SEG_FREE -> SEG_NOT_FULL, page 64
            Optional<PageNo> p0 = seg.allocatePageFromSegmentExtents(m, SPACE, slot);
            assertEquals(Optional.of(PageNo.of(64)), p0);
            assertEquals(0L, flst.length(m, SPACE, inode.freeExtentListBaseAddr(SPACE, slot)));
            assertEquals(1L, flst.length(m, SPACE, inode.notFullExtentListBaseAddr(SPACE, slot)));
            // fill the rest -> SEG_NOT_FULL -> SEG_FULL
            for (int i = 1; i < PE; i++) {
                assertEquals(Optional.of(PageNo.of(64 + i)), seg.allocatePageFromSegmentExtents(m, SPACE, slot));
            }
            assertEquals(0L, flst.length(m, SPACE, inode.notFullExtentListBaseAddr(SPACE, slot)));
            assertEquals(1L, flst.length(m, SPACE, inode.fullExtentListBaseAddr(SPACE, slot)));
            assertEquals((long) PE, inode.read(m, SPACE, slot).usedPageCount());
            // no segment extent has free page now
            assertTrue(seg.allocatePageFromSegmentExtents(m, SPACE, slot).isEmpty());
            mgr.commit(m);
        });
    }
}
```

- [ ] **Step 2: 运行确认失败** — 编译失败（SegmentSpaceService 不存在）。

- [ ] **Step 3: 写 `SegmentSpaceService.java`**（含分配方法 + 私有 latch/bumpUsed；freePage 在 Task 4 加入）

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
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.mtr.MiniTransaction;

import java.util.Optional;
import java.util.OptionalInt;

/**
 * segment 侧空间分配/释放原语（设计 §7）：fragment 页分配（含 inode 记录）、给 segment 分配 extent、
 * 从 segment extent 分配页、释放页回收。维护 XDES state/owner/bitmap、segment FLST 链、inode 计数/fragment 槽一致。
 *
 * <p>锁序：每个公开 op 先 {@link #latchSpaceThenInode}（page0 X→page2 X，§18 顺序），后续 repo/Flst 读写在已持 X 上可重入，
 * 既不逆序也不触发 MTR 同页 S→X 拒绝。无空间以 {@link Optional#empty()} 表达；损坏/非法抛领域异常。本片 no-redo。
 */
public final class SegmentSpaceService {

    /** 受控页来源，用于跨页预闩。 */
    private final BufferPool pool;

    /** 实例级页大小；决定 extent 首页号。 */
    private final PageSize pageSize;

    /** SpaceHeader 仓储：全局链 base（释放回收时用）。 */
    private final SpaceHeaderRepository headerRepo;

    /** SegmentInode 仓储：segment 链 base、fragment 槽、计数。 */
    private final SegmentInodeRepository inodeRepo;

    /** XDES 仓储：extent state/owner/bitmap/节点地址。 */
    private final ExtentDescriptorRepository xdes;

    /** FLST 链表原语。 */
    private final Flst flst;

    /** 全局 free 服务：acquire/return/fragment 页。 */
    private final FreeExtentService freeExtents;

    public SegmentSpaceService(BufferPool pool, PageSize pageSize, SpaceHeaderRepository headerRepo,
                               SegmentInodeRepository inodeRepo, ExtentDescriptorRepository xdes,
                               Flst flst, FreeExtentService freeExtents) {
        if (pool == null || pageSize == null || headerRepo == null || inodeRepo == null
                || xdes == null || flst == null || freeExtents == null) {
            throw new DatabaseValidationException("SegmentSpaceService dependencies must not be null");
        }
        this.pool = pool;
        this.pageSize = pageSize;
        this.headerRepo = headerRepo;
        this.inodeRepo = inodeRepo;
        this.xdes = xdes;
        this.flst = flst;
        this.freeExtents = freeExtents;
    }

    /** §18 锁序：先 page0 X 再 page2 X，建立顺序并使后续读取降级、避免同页 S→X。 */
    private void latchSpaceThenInode(MiniTransaction mtr, SpaceId spaceId) {
        mtr.getPage(pool, PageId.of(spaceId, PageNo.of(0)), PageLatchMode.EXCLUSIVE);
        mtr.getPage(pool, PageId.of(spaceId, PageNo.of(2)), PageLatchMode.EXCLUSIVE);
    }

    /**
     * 为 segment 分配一个 fragment 页：从全局 FREE_FRAG 取页，记入 inode 首个空 fragment 槽，usedPageCount+1。
     * 32 槽已满 → FspMetadataException（边界由 2c 用 <32 决策规避）；无空间 → empty（未改 inode）。
     */
    public Optional<PageNo> allocateFragmentPage(MiniTransaction mtr, SpaceId spaceId, int inodeSlot) {
        requireArgs(mtr, spaceId);
        latchSpaceThenInode(mtr, spaceId);
        int slotIdx = inodeRepo.requireFreeFragmentSlot(mtr, spaceId, inodeSlot);
        Optional<PageId> pageOpt = freeExtents.allocateFragmentPage(mtr, spaceId);
        if (pageOpt.isEmpty()) {
            return Optional.empty();
        }
        PageNo pageNo = pageOpt.get().pageNo();
        inodeRepo.setFragmentPage(mtr, spaceId, inodeSlot, slotIdx, Optional.of(pageNo));
        bumpUsed(mtr, spaceId, inodeSlot, 1);
        return Optional.of(pageNo);
    }

    /** 给 segment 分配一个完整 extent：acquire FREE extent → 置 FSEG/owner=segId → 入该段 SEG_FREE 链。无空间 → empty。 */
    public Optional<ExtentId> assignExtentToSegment(MiniTransaction mtr, SpaceId spaceId, int inodeSlot) {
        requireArgs(mtr, spaceId);
        latchSpaceThenInode(mtr, spaceId);
        SegmentId segId = inodeRepo.read(mtr, spaceId, inodeSlot).segmentId();
        Optional<ExtentId> acq = freeExtents.acquireFreeExtent(mtr, spaceId);
        if (acq.isEmpty()) {
            return Optional.empty();
        }
        ExtentId ext = acq.get();
        xdes.writeState(mtr, ext, ExtentState.FSEG);
        xdes.writeOwner(mtr, ext, Optional.of(segId));
        flst.addLast(mtr, spaceId, inodeRepo.freeExtentListBaseAddr(spaceId, inodeSlot), xdes.listNodeAddr(ext));
        return Optional.of(ext);
    }

    /**
     * 从 segment 自有 extent 分配一个页：优先 SEG_NOT_FULL 头，否则 SEG_FREE 头；取首个空闲页置位。
     * 迁移：SEG_FREE→SEG_NOT_FULL（或满→SEG_FULL）、SEG_NOT_FULL→SEG_FULL（变满时）。无可用 extent → empty（2c 先 assign）。
     */
    public Optional<PageNo> allocatePageFromSegmentExtents(MiniTransaction mtr, SpaceId spaceId, int inodeSlot) {
        requireArgs(mtr, spaceId);
        latchSpaceThenInode(mtr, spaceId);
        FileAddress notFull = inodeRepo.notFullExtentListBaseAddr(spaceId, inodeSlot);
        FileAddress segFree = inodeRepo.freeExtentListBaseAddr(spaceId, inodeSlot);
        FileAddress segFull = inodeRepo.fullExtentListBaseAddr(spaceId, inodeSlot);
        FileAddress head = flst.getFirst(mtr, spaceId, notFull);
        boolean fromFree = false;
        if (head.isNull()) {
            head = flst.getFirst(mtr, spaceId, segFree);
            if (head.isNull()) {
                return Optional.empty();
            }
            fromFree = true;
        }
        ExtentId ext = xdes.extentIdOfNode(spaceId, head);
        OptionalInt idxOpt = xdes.firstFreePageIndex(mtr, ext);
        if (idxOpt.isEmpty()) {
            throw new FspMetadataException("segment extent on non-full list is full: " + ext.extentNo());
        }
        int idx = idxOpt.getAsInt();
        xdes.setPageAllocated(mtr, ext, idx, true);
        long pageNo = ext.firstPageNo(pageSize).value() + idx;
        FileAddress node = xdes.listNodeAddr(ext);
        if (fromFree) {
            flst.remove(mtr, spaceId, segFree, node);
            if (xdes.isFull(mtr, ext)) {
                flst.addLast(mtr, spaceId, segFull, node);
            } else {
                flst.addLast(mtr, spaceId, notFull, node);
            }
        } else if (xdes.isFull(mtr, ext)) {
            flst.remove(mtr, spaceId, notFull, node);
            flst.addLast(mtr, spaceId, segFull, node);
        }
        bumpUsed(mtr, spaceId, inodeSlot, 1);
        return Optional.of(PageNo.of(pageNo));
    }

    /** 读现值并 +delta 写回 usedPageCount；delta<0 且现值<=0 视为计数损坏。 */
    private void bumpUsed(MiniTransaction mtr, SpaceId spaceId, int inodeSlot, long delta) {
        long cur = inodeRepo.read(mtr, spaceId, inodeSlot).usedPageCount();
        if (delta < 0 && cur <= 0) {
            throw new FspMetadataException("usedPageCount underflow on inode slot " + inodeSlot);
        }
        inodeRepo.setUsedPageCount(mtr, spaceId, inodeSlot, cur + delta);
    }

    private static void requireArgs(MiniTransaction mtr, SpaceId spaceId) {
        if (mtr == null) {
            throw new DatabaseValidationException("mini transaction must not be null");
        }
        if (spaceId == null) {
            throw new DatabaseValidationException("space id must not be null");
        }
    }
}
```

- [ ] **Step 4: 运行确认通过**（SegmentSpaceServiceTest 全部）。不提交。

---

## Task 4: SegmentSpaceService.freePage（两路回收）

**Files:**
- Modify: `src/main/java/cn/zhangyis/db/storage/fsp/SegmentSpaceService.java`
- Modify (test): `src/test/java/cn/zhangyis/db/storage/fsp/SegmentSpaceServiceTest.java`

- [ ] **Step 1: 在 `SegmentSpaceServiceTest.java` 加释放测试**（插在最后一个 `@Test` 之后、类结束 `}` 之前；需要新增 import：`import cn.zhangyis.db.domain.ExtentDescriptor;` 不需要，用 xdes.read；以及 `import static org.junit.jupiter.api.Assertions.assertThrows;`、`import static org.junit.jupiter.api.Assertions.assertFalse;`）：

```java
    @Test
    void freeFragmentPageClearsSlotAndRecyclesExtentWhenEmpty() {
        withCtx(128, (header, xdes, inode, flst, free, seg, mgr) -> {
            MiniTransaction a = mgr.begin();
            int slot = inode.allocateSlot(a, SPACE, SegmentId.of(1), SegmentPurpose.INDEX_LEAF);
            mgr.commit(a);

            MiniTransaction m = mgr.begin();
            PageNo p = seg.allocateFragmentPage(m, SPACE, slot).orElseThrow();
            assertEquals(PageNo.of(64), p);
            // free it -> inode slot cleared, usedPageCount 0, extent empty -> back to FSP_FREE
            seg.freePage(m, SPACE, slot, PageId.of(SPACE, p));
            assertTrue(inode.getFragmentPage(m, SPACE, slot, 0).isEmpty());
            assertEquals(0L, inode.read(m, SPACE, slot).usedPageCount());
            assertEquals(ExtentState.FREE, xdes.read(m, ExtentId.of(SPACE, 1)).state());
            assertEquals(1L, flst.length(m, SPACE, header.freeExtentListBaseAddr(SPACE)));
            assertEquals(0L, flst.length(m, SPACE, header.freeFragExtentListBaseAddr(SPACE)));
            mgr.commit(m);
        });
    }

    @Test
    void freeFragmentPageMovesFullFragBackToFreeFrag() {
        withCtx(128, (header, xdes, inode, flst, free, seg, mgr) -> {
            MiniTransaction a = mgr.begin();
            int slotA = inode.allocateSlot(a, SPACE, SegmentId.of(1), SegmentPurpose.INDEX_LEAF);
            mgr.commit(a);

            MiniTransaction m = mgr.begin();
            // fill extent1 fully via fragment pages (32 slots max -> use two segments not needed; PE=64 > 32 slots)
            // allocate 32 fragment pages into slotA (fills 32 frag slots), then more would exceed slots.
            // To make the extent FULL_FRAG we need PE allocations; use a second segment for the remaining.
            int slotB = inode.allocateSlot(m, SPACE, SegmentId.of(2), SegmentPurpose.INDEX_LEAF);
            for (int i = 0; i < 32; i++) {
                seg.allocateFragmentPage(m, SPACE, slotA);
            }
            for (int i = 0; i < PE - 32; i++) {
                seg.allocateFragmentPage(m, SPACE, slotB);
            }
            assertEquals(ExtentState.FULL_FRAG, xdes.read(m, ExtentId.of(SPACE, 1)).state());
            // free one page (page 64, recorded in slotA frag 0) -> FULL_FRAG back to FREE_FRAG
            seg.freePage(m, SPACE, slotA, PageId.of(SPACE, PageNo.of(64)));
            assertEquals(ExtentState.FREE_FRAG, xdes.read(m, ExtentId.of(SPACE, 1)).state());
            assertEquals(1L, flst.length(m, SPACE, header.freeFragExtentListBaseAddr(SPACE)));
            assertEquals(0L, flst.length(m, SPACE, header.fullFragExtentListBaseAddr(SPACE)));
            mgr.commit(m);
        });
    }

    @Test
    void freeSegmentExtentPageMovesFullToNotFullAndRecyclesWhenEmpty() {
        withCtx(128, (header, xdes, inode, flst, free, seg, mgr) -> {
            MiniTransaction a = mgr.begin();
            int slot = inode.allocateSlot(a, SPACE, SegmentId.of(1), SegmentPurpose.INDEX_LEAF);
            seg.assignExtentToSegment(a, SPACE, slot);
            mgr.commit(a);

            MiniTransaction m = mgr.begin();
            for (int i = 0; i < PE; i++) {
                seg.allocatePageFromSegmentExtents(m, SPACE, slot);
            }
            assertEquals(1L, flst.length(m, SPACE, inode.fullExtentListBaseAddr(SPACE, slot)));
            // free one -> FULL -> NOT_FULL
            seg.freePage(m, SPACE, slot, PageId.of(SPACE, PageNo.of(64)));
            assertEquals(0L, flst.length(m, SPACE, inode.fullExtentListBaseAddr(SPACE, slot)));
            assertEquals(1L, flst.length(m, SPACE, inode.notFullExtentListBaseAddr(SPACE, slot)));
            // free the rest -> extent empty -> back to FSP_FREE, owner cleared
            for (int i = 1; i < PE; i++) {
                seg.freePage(m, SPACE, slot, PageId.of(SPACE, PageNo.of(64 + i)));
            }
            assertEquals(0L, flst.length(m, SPACE, inode.notFullExtentListBaseAddr(SPACE, slot)));
            assertEquals(ExtentState.FREE, xdes.read(m, ExtentId.of(SPACE, 1)).state());
            assertTrue(xdes.read(m, ExtentId.of(SPACE, 1)).ownerSegment().isEmpty());
            assertEquals(1L, flst.length(m, SPACE, header.freeExtentListBaseAddr(SPACE)));
            assertEquals(0L, inode.read(m, SPACE, slot).usedPageCount());
            mgr.commit(m);
        });
    }

    @Test
    void freeRejectsSystemAndUnallocatedPages() {
        withCtx(128, (header, xdes, inode, flst, free, seg, mgr) -> {
            MiniTransaction a = mgr.begin();
            int slot = inode.allocateSlot(a, SPACE, SegmentId.of(1), SegmentPurpose.INDEX_LEAF);
            mgr.commit(a);
            MiniTransaction m = mgr.begin();
            assertThrows(FspMetadataException.class,
                    () -> seg.freePage(m, SPACE, slot, PageId.of(SPACE, PageNo.of(0)))); // system extent0
            assertThrows(FspMetadataException.class,
                    () -> seg.freePage(m, SPACE, slot, PageId.of(SPACE, PageNo.of(64)))); // extent1 FREE/unallocated
            mgr.commit(m);
        });
    }
```

- [ ] **Step 2: 运行确认失败** — `--tests "cn.zhangyis.db.storage.fsp.SegmentSpaceServiceTest"`，编译失败（freePage 不存在）。

- [ ] **Step 3: 在 `SegmentSpaceService.java` 增 `freePage` 与私有 `clearFragmentSlot`**（插在 `bumpUsed` 方法之前；并在 import 区加 `import cn.zhangyis.db.domain.ExtentDescriptor;`）：

```java
    /**
     * 释放一个页并回收（设计 §7.4）。按 extent 状态分两路：
     * fragment extent（FREE_FRAG/FULL_FRAG）：清 bitmap + 清对应 inode fragment 槽 + 计数-1；原 FULL_FRAG→FREE_FRAG；全空→FSP_FREE。
     * segment extent（FSEG）：校 owner + 清 bitmap + 计数-1；原满→SEG_NOT_FULL；全空→摘段链 + 置 FREE/清 owner + FSP_FREE。
     * 系统 extent0 或 FREE/未分配区页 → FspMetadataException。
     */
    public void freePage(MiniTransaction mtr, SpaceId spaceId, int inodeSlot, PageId pageId) {
        requireArgs(mtr, spaceId);
        if (pageId == null) {
            throw new DatabaseValidationException("page id must not be null");
        }
        latchSpaceThenInode(mtr, spaceId);
        ExtentId ext = ExtentId.from(pageId, pageSize);
        if (ext.extentNo() == 0) {
            throw new FspMetadataException("cannot free a system-extent page: " + pageId.pageNo().value());
        }
        int idxInExtent = (int) (pageId.pageNo().value() - ext.firstPageNo(pageSize).value());
        ExtentDescriptor desc = xdes.read(mtr, ext);
        ExtentState state = desc.state();
        FileAddress node = xdes.listNodeAddr(ext);
        switch (state) {
            case FREE_FRAG, FULL_FRAG -> {
                xdes.setPageAllocated(mtr, ext, idxInExtent, false);
                clearFragmentSlot(mtr, spaceId, inodeSlot, pageId.pageNo());
                bumpUsed(mtr, spaceId, inodeSlot, -1);
                if (state == ExtentState.FULL_FRAG) {
                    flst.remove(mtr, spaceId, headerRepo.fullFragExtentListBaseAddr(spaceId), node);
                    xdes.writeState(mtr, ext, ExtentState.FREE_FRAG);
                    flst.addLast(mtr, spaceId, headerRepo.freeFragExtentListBaseAddr(spaceId), node);
                }
                if (xdes.isEmpty(mtr, ext)) {
                    flst.remove(mtr, spaceId, headerRepo.freeFragExtentListBaseAddr(spaceId), node);
                    freeExtents.returnFreeExtent(mtr, spaceId, ext);
                }
            }
            case FSEG -> {
                SegmentId segId = inodeRepo.read(mtr, spaceId, inodeSlot).segmentId();
                if (desc.ownerSegment().isEmpty() || !desc.ownerSegment().get().equals(segId)) {
                    throw new FspMetadataException("extent owner mismatch on free: extent " + ext.extentNo());
                }
                boolean wasFull = xdes.isFull(mtr, ext);
                xdes.setPageAllocated(mtr, ext, idxInExtent, false);
                bumpUsed(mtr, spaceId, inodeSlot, -1);
                FileAddress notFull = inodeRepo.notFullExtentListBaseAddr(spaceId, inodeSlot);
                FileAddress segFull = inodeRepo.fullExtentListBaseAddr(spaceId, inodeSlot);
                if (wasFull) {
                    flst.remove(mtr, spaceId, segFull, node);
                    flst.addLast(mtr, spaceId, notFull, node);
                }
                if (xdes.isEmpty(mtr, ext)) {
                    flst.remove(mtr, spaceId, notFull, node);
                    xdes.writeState(mtr, ext, ExtentState.FREE);
                    xdes.writeOwner(mtr, ext, Optional.empty());
                    flst.addLast(mtr, spaceId, headerRepo.freeExtentListBaseAddr(spaceId), node);
                }
            }
            default -> throw new FspMetadataException(
                    "cannot free page in extent state " + state + ": page " + pageId.pageNo().value());
        }
    }

    /** 扫 32 个 fragment 槽，清掉值等于 pageNo 的槽；找不到说明该页不是本段 fragment 页（损坏）。 */
    private void clearFragmentSlot(MiniTransaction mtr, SpaceId spaceId, int inodeSlot, PageNo pageNo) {
        for (int f = 0; f < SegmentInodeLayout.FRAGMENT_SLOT_COUNT; f++) {
            Optional<PageNo> cur = inodeRepo.getFragmentPage(mtr, spaceId, inodeSlot, f);
            if (cur.isPresent() && cur.get().equals(pageNo)) {
                inodeRepo.setFragmentPage(mtr, spaceId, inodeSlot, f, Optional.empty());
                return;
            }
        }
        throw new FspMetadataException("fragment page not recorded in segment: " + pageNo.value());
    }
```

- [ ] **Step 4: 运行确认通过**（SegmentSpaceServiceTest 全部）。不提交。

---

## Task 5: 全量回归 + GitNexus

- [ ] **Step 1: 全量回归** — `clean test`，期望 BUILD SUCCESSFUL（slice-1/2a 全部 + 2b 新增）。
- [ ] **Step 2: 刷新 GitNexus 索引** — `npx gitnexus analyze`；失败记录并重试。
- [ ] **Step 3: 不提交。**

---

## Self-Review（写计划后自查，已执行）

**1. Spec 覆盖：** §5 repo 扩展→Task1；§6 FreeExtentService（fill/acquire/return/fragment）→Task2；§7 SegmentSpaceService（fragment/assign/extent-alloc）→Task3、freePage 两路→Task4；§4 不变量、§8 锁序（page0→page2 预闩）、§9 异常→各 Task 实现 + 测试；§10 测试项→Task1-4；Task5 回归 + 索引。

**2. Placeholder 扫描：** 无 TBD/TODO；每步含完整代码与命令。

**3. 类型一致性：** `ExtentDescriptorRepository.{listNodeAddr,extentIdOfNode,firstFreePageIndex(OptionalInt),allocatedPageCount,isFull,isEmpty}`；`FreeExtentService.{fillFreeListStep,acquireFreeExtent,returnFreeExtent,allocateFragmentPage}` 返回 `Optional<ExtentId>`/`Optional<PageId>`；`SegmentSpaceService.{allocateFragmentPage→Optional<PageNo>,assignExtentToSegment→Optional<ExtentId>,allocatePageFromSegmentExtents→Optional<PageNo>,freePage}`；复用 slice-1/2a：`SpaceHeaderRepository.{initialize,read,setFreeLimitPageNo,freeExtentListBaseAddr,freeFragExtentListBaseAddr,fullFragExtentListBaseAddr}`、`SegmentInodeRepository.{allocateSlot,read,setUsedPageCount,getFragmentPage,setFragmentPage,requireFreeFragmentSlot,freeExtentListBaseAddr,notFullExtentListBaseAddr,fullExtentListBaseAddr}`、`ExtentDescriptorRepository.{read,initFree,writeState,writeOwner,setPageAllocated}`、`Flst.{getFirst,addLast,remove,length}`、`ExtentId.{of,from,extentNo,firstPageNo}`、`FlstBase.EMPTY`、`ExtentState.{FREE,FREE_FRAG,FULL_FRAG,FSEG}`。`SegmentInodeLayout.FRAGMENT_SLOT_COUNT` 同包可见。

**4. 锁序/并发：** FreeExtentService 仅 page0、开头预闩 page0 X；SegmentSpaceService 预闩 page0 X→page2 X；读取在已持 X 上降级，写为可重入 X，无逆序、无 S→X；测试用 commit 分隔 allocateSlot 与跨页 op（withCtx 中 init 与各 op 分 MTR）。fragment FULL_FRAG 测试用两个 segment 把 64 页占满（单段 fragment 槽仅 32）。
