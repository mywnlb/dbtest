# D4a — allocatePage 页初始化（PAGE_INIT + 信封）Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让 `DiskSpaceManager.allocatePage` 分配出的数据页走「页创建」（`mtr.newPage(p, X, ALLOCATED)` + `PageEnvelope.writeHeader`），从而产生 `PAGE_INIT(ALLOCATED)` + 信封 PAGE_BYTES，commit 盖 pageLSN。

**Architecture:** 先放宽 `buf.newPage` 为「页创建」语义（要求 X latch；驻留页在取得 X latch 后清零重初始化，对齐 InnoDB buf_page_create），再在 `allocatePage` 调用它并写信封。清零直接打在 `frame.data`（不经 PageGuard → 不产 PAGE_BYTES，恢复由 PAGE_INIT 承担）。

**Tech Stack:** Java 25、JUnit Jupiter、固定 JDK `C:\Program Files\Java\jdk-25.0.2`、固定 Gradle `D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1`。

**关联 spec：** `docs/superpowers/specs/2026-06-16-allocatepage-page-init-design.md`

**项目规则（覆盖默认）：**
- **不提交 git**；每个 Task 末「全量 `clean test` 绿」作 checkpoint，**无 commit 步骤**。
- 禁 `synchronized`/`wait`/`notify`（生产并发用 j.u.c；测试可用 CountDownLatch/Thread.sleep）；禁裸 `IllegalArgumentException`/`RuntimeException`。
- 中文 Javadoc。
- **改 CRITICAL/HIGH 符号前跑 `gitnexus_impact` 告警**；批末 `gitnexus_detect_changes`。

**Checkpoint 命令：**
```
$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --console=plain
```

---

## File Structure

**修改（production）：**
- `buf/LruBufferPool.java` — `acquire` 加 newPage X 校验 + 驻留页「取 X latch 后清零」重初始化。
- `buf/BufferPool.java` — `newPage` 接口 Javadoc 同步（X latch + 驻留重初始化）。
- `api/DiskSpaceManager.java` — `allocatePage` 加 `mtr.newPage`+`PageEnvelope.writeHeader`。

**测试：**
- `buf/LruBufferPoolTest.java` — **改写** `newPageShouldRejectResidentPage` → `newPageReinitializesResidentPage`；新增 `newPageRejectsSharedMode`、`newPageOnResidentBlocksUntilSharedReleasedThenZeroes`。
- `api/DiskSpaceManagerTest.java` — `withDsm` pool 16→64；新增 `allocatePageEmitsPageInitAndStampsEnvelope`、`reallocateResidentPageReinitializes`。

---

## Task D4a-1：buf.newPage 页创建语义（X latch + 驻留重初始化）

**Impact（编辑前必跑，CRITICAL 告警）：**
```
gitnexus_impact({target:"LruBufferPool", direction:"upstream", repo:"dbtest"})
```
**已知 buf 为 CRITICAL 区。本批行为变更：newPage 驻留页 throw→reinit、且要求 X latch。向用户复述 blast radius；确认无生产代码依赖旧 throw（D3 前 newPage 仅 MTR 测试用），全量回归把关。**

- [ ] **Step 1: 改写旧测试 + 新增 buf 测试（先写，确认失败/编译失败）**

在 `LruBufferPoolTest.java`：

(1) 顶部 import 区补：
```java
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
```

(2) 把现有 `newPageShouldRejectResidentPage` **整体替换**为：
```java
    @Test
    void newPageReinitializesResidentPage() {
        try (PageStore store = openStore(8)) {
            LruBufferPool pool = new LruBufferPool(store, PS, 4);
            try (PageGuard g = pool.getPage(page(2), PageLatchMode.EXCLUSIVE)) {
                g.writeInt(0, 0x12345678); // page2 驻留且有内容
            }
            try (PageGuard g = pool.newPage(page(2), PageLatchMode.EXCLUSIVE)) {
                assertEquals(0, g.readInt(0), "resident page reinitialized to zero");
                g.writeInt(0, 0x55);
                assertEquals(0x55, g.readInt(0));
            }
            pool.close();
        }
    }

    @Test
    void newPageRejectsSharedMode() {
        try (PageStore store = openStore(8)) {
            LruBufferPool pool = new LruBufferPool(store, PS, 4);
            assertThrows(DatabaseValidationException.class,
                    () -> pool.newPage(page(2), PageLatchMode.SHARED));
            pool.close();
        }
    }

    @Test
    void newPageOnResidentBlocksUntilSharedReleasedThenZeroes() throws Exception {
        try (PageStore store = openStore(8)) {
            LruBufferPool pool = new LruBufferPool(store, PS, 4);
            try (PageGuard w = pool.getPage(page(2), PageLatchMode.EXCLUSIVE)) {
                w.writeInt(0, 0xABCD); // page2 驻留且有非零内容
            }
            PageGuard shared = pool.getPage(page(2), PageLatchMode.SHARED); // A 持 S
            CountDownLatch bStarted = new CountDownLatch(1);
            AtomicInteger bReadAfter = new AtomicInteger(-1);
            AtomicBoolean bDone = new AtomicBoolean(false);
            Thread b = new Thread(() -> {
                bStarted.countDown();
                try (PageGuard g = pool.newPage(page(2), PageLatchMode.EXCLUSIVE)) { // 阻塞至 A 释放 S
                    bReadAfter.set(g.readInt(0));
                }
                bDone.set(true);
            });
            b.start();
            assertTrue(bStarted.await(2, TimeUnit.SECONDS));
            Thread.sleep(150); // 给 B 时间尝试取 X（应阻塞）
            assertFalse(bDone.get(), "newPage(X) must block while SHARED held");
            assertEquals(0xABCD, shared.readInt(0), "content must not be zeroed before X latch acquired");
            shared.close(); // 释放 A 的 S
            b.join(2000);
            assertTrue(bDone.get(), "newPage proceeds after S released");
            assertEquals(0, bReadAfter.get(), "newPage returns zeroed page");
            pool.close();
        }
    }
```

- [ ] **Step 2: 运行确认失败**

Run: `... gradle.bat test --tests "cn.zhangyis.db.storage.buf.LruBufferPoolTest" --console=plain`
Expected: `newPageReinitializesResidentPage`/`newPageRejectsSharedMode`/`newPageOnResidentBlocksUntilSharedReleasedThenZeroes` FAIL（当前 newPage 驻留抛异常、SHARED 不拒绝）。

- [ ] **Step 3: 改 `LruBufferPool.acquire`**

把现有 `acquire` 方法整体替换为（加 X 校验、resident reinit、post-latch 清零）：
```java
    private PageGuard acquire(PageId pageId, PageLatchMode mode, boolean readFromDisk) {
        if (pageId == null) {
            throw new DatabaseValidationException("page id must not be null");
        }
        if (mode == null) {
            throw new DatabaseValidationException("page latch mode must not be null");
        }
        // 页创建/重初始化是写操作，必须持 X latch；不允许 newPage(page, SHARED) 走清零语义。
        if (!readFromDisk && mode != PageLatchMode.EXCLUSIVE) {
            throw new DatabaseValidationException("newPage requires EXCLUSIVE latch but got " + mode);
        }
        BufferFrame chosen;
        boolean resetAfterLatch = false;
        poolLock.lock();
        try {
            BufferFrame resident = residentMap.get(pageId);
            if (resident != null) {
                if (!readFromDisk) {
                    // 页创建命中驻留页：重初始化（复用帧），对齐 InnoDB buf_page_create。
                    // 清零延后到取得 X latch 之后做（见方法末尾）——不能在 poolLock 内、未持 page latch 时改页内容，
                    // 否则会与持锁读者撞车、绕过 page latch 语义。dirty 在 poolLock 下置位（dirty 由 poolLock 保护）。
                    resident.dirty = true;
                    resetAfterLatch = true;
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
        // 驻留页重初始化：在 X latch 保护下清零（不经 PageGuard → 不产 PAGE_BYTES；清零恢复语义由 PAGE_INIT 承担）。
        if (resetAfterLatch) {
            Arrays.fill(chosen.data, (byte) 0);
        }
        return new PageGuard(this, chosen, mode, latch);
    }
```

- [ ] **Step 4: 同步 `BufferPool.newPage` Javadoc**

把 `BufferPool.java` 的 `newPage` 方法 Javadoc 替换为：
```java
    /**
     * 页创建：为页建立"不读盘"的零帧（页须已被 PageStore.extend 在盘上分配/零填充，由调用方保证）。
     * **要求 X latch**（创建/重初始化是写操作）；若该页已驻留，则**重初始化**（在取得 X latch 后清零、复用帧，
     * 对齐 InnoDB buf_page_create）——调用方须确保该页确实在被（重新）分配，不能误覆盖在用页。
     *
     * @param pageId 新页（驻留则被重初始化清零）。
     * @param mode   必须为 EXCLUSIVE，否则抛 DatabaseValidationException。
     * @return 受控页句柄（X latch）。
     */
    PageGuard newPage(PageId pageId, PageLatchMode mode);
```

- [ ] **Step 5: 运行确认通过**

Run: `... gradle.bat test --tests "cn.zhangyis.db.storage.buf.LruBufferPoolTest" --console=plain`
Expected: PASS（含新 3 用例 + 既有 buf 用例回归）。

- [ ] **Step 6: 全量回归 + detect_changes** — Checkpoint 命令 BUILD SUCCESSFUL；`gitnexus_detect_changes({repo:"dbtest", scope:"all"})`（无 commit 报 HEAD 缺失属预期）。

---

## Task D4a-2：allocatePage 页初始化 + 信封

**Impact（编辑前必跑）：**
```
gitnexus_impact({target:"allocatePage", direction:"upstream", file_path:"src/main/java/cn/zhangyis/db/storage/api/DiskSpaceManager.java", repo:"dbtest"})
```
预期：DiskSpaceManager.allocatePage 的调用方（测试 + 未来 btree）；行为是「分配后多初始化数据页」，对现有断言无破坏（除 pool 容量，见 Step 1）。

- [ ] **Step 1: 调大 withDsm pool + 新增 api 失败测试**

在 `DiskSpaceManagerTest.java`：

(1) 顶部 import 区补：
```java
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.page.FilePageHeader;
import cn.zhangyis.db.storage.page.PageEnvelope;
import cn.zhangyis.db.storage.page.PageType;
import cn.zhangyis.db.storage.redo.PageInitRecord;

import static org.junit.jupiter.api.Assertions.assertTrue;
```

(2) 把 `withDsm` 的 pool 容量 16→64（每次 allocatePage 现多 fix 一个数据页 X latch 且持到 commit；33 页单 MTR 批量分配需更大池）：
```java
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 64)) {
```

(3) 末尾（最后一个 `}` 前）新增两个测试（自带 pool 引用以便 commit 后重读页）：
```java
    @Test
    void allocatePageEmitsPageInitAndStampsEnvelope() {
        PageStore store = new FileChannelPageStore();
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 64)) {
            DiskSpaceManager dsm = new DiskSpaceManager(pool, store, PS);
            MiniTransactionManager mgr = new MiniTransactionManager();
            MiniTransaction m = mgr.begin();
            dsm.createTablespace(m, SPACE, dir.resolve("s.ibd"), PageNo.of(128));
            SegmentRef ref = dsm.createSegment(m, SPACE, SegmentPurpose.INDEX_LEAF);
            PageId p = dsm.allocatePage(m, ref);
            mgr.commit(m);

            boolean hasInit = mgr.redoLogManager().bufferedRecords().stream()
                    .anyMatch(r -> r instanceof PageInitRecord pir
                            && pir.pageId().equals(p) && pir.pageType() == PageType.ALLOCATED);
            assertTrue(hasInit, "allocatePage emits PAGE_INIT(ALLOCATED)");

            Lsn endLsn = mgr.redoLogManager().currentLsn();
            try (PageGuard g = pool.getPage(p, PageLatchMode.SHARED)) {
                FilePageHeader h = PageEnvelope.readHeader(g);
                assertEquals(SPACE, h.spaceId());
                assertEquals(p.pageNo().value(), h.pageNo());
                assertEquals(PageType.ALLOCATED, h.pageType());
                assertEquals(endLsn, PageEnvelope.readPageLsn(g));
            }
        }
    }

    @Test
    void reallocateResidentPageReinitializes() {
        PageStore store = new FileChannelPageStore();
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 64)) {
            DiskSpaceManager dsm = new DiskSpaceManager(pool, store, PS);
            MiniTransactionManager mgr = new MiniTransactionManager();
            MiniTransaction m = mgr.begin();
            dsm.createTablespace(m, SPACE, dir.resolve("s.ibd"), PageNo.of(128));
            SegmentRef ref = dsm.createSegment(m, SPACE, SegmentPurpose.INDEX_LEAF);
            PageId p = dsm.allocatePage(m, ref);
            dsm.freePage(m, ref, p);
            PageId again = dsm.allocatePage(m, ref); // 命中同一驻留页 → 重初始化，不抛
            assertEquals(p, again);
            mgr.commit(m);
            try (PageGuard g = pool.getPage(again, PageLatchMode.SHARED)) {
                assertEquals(PageType.ALLOCATED, PageEnvelope.readHeader(g).pageType());
            }
        }
    }
```
（`SegmentRef` 在 `cn.zhangyis.db.storage.api`，与测试同包，无需 import。）

- [ ] **Step 2: 运行确认失败**

Run: `... gradle.bat test --tests "cn.zhangyis.db.storage.api.DiskSpaceManagerTest" --console=plain`
Expected: `allocatePageEmitsPageInitAndStampsEnvelope`/`reallocateResidentPageReinitializes` FAIL（当前 allocatePage 不产 PAGE_INIT、不写信封；reallocate 同页因第二次未走 newPage 暂不命中 reinit——本测试依赖 D4a-2 实现）。

- [ ] **Step 3: 改 `DiskSpaceManager.allocatePage`**

加 import：
```java
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.page.FilePageHeader;
import cn.zhangyis.db.storage.page.PageEnvelope;
import cn.zhangyis.db.storage.page.PageType;
```
（`PageLatchMode` 已 import。）

把 `allocatePage` 改为（保留现有分配/autoextend 逻辑，得 PageId 后初始化数据页）：
```java
    /** 为 segment 分配一个页；当前空间不足则扩展文件一次再试，仍不足抛 NoFreeSpaceException。
     * 分配成功后对该数据页做「页创建」：mtr.newPage(X)+写信封（type=ALLOCATED）→ 产 PAGE_INIT + 信封 PAGE_BYTES，
     * commit 盖 pageLSN。数据页 X latch 入 mtr memo，持到 commit（盖 pageLSN 需其 guard）。 */
    public PageId allocatePage(MiniTransaction mtr, SegmentRef ref) {
        requireMtr(mtr);
        requireRef(ref);
        PageId allocated = doAllocatePage(mtr, ref);
        initAllocatedPage(mtr, allocated);
        return allocated;
    }

    /** 现有分配逻辑（fragment→extent，autoextend 一次重试），只决定页号、不碰数据页帧。 */
    private PageId doAllocatePage(MiniTransaction mtr, SegmentRef ref) {
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

    /** 页创建：newPage(X) 取零帧（驻留则重初始化）+ 写信封（type=ALLOCATED，pageLsn=0 由 commit 盖真值）。不自行 close（mtr 持有）。 */
    private void initAllocatedPage(MiniTransaction mtr, PageId p) {
        PageGuard g = mtr.newPage(pool, p, PageLatchMode.EXCLUSIVE, PageType.ALLOCATED);
        PageEnvelope.writeHeader(g, new FilePageHeader(
                p.spaceId(), p.pageNo().value(),
                FilePageHeader.FIL_NULL, FilePageHeader.FIL_NULL, 0L, PageType.ALLOCATED));
    }
```

- [ ] **Step 4: 运行确认通过**

Run: `... gradle.bat test --tests "cn.zhangyis.db.storage.api.DiskSpaceManagerTest" --console=plain`
Expected: PASS（新 2 用例 + 既有用例回归，pool=64 容纳批量分配）。

- [ ] **Step 5: 全量回归 + detect_changes** — Checkpoint 命令 BUILD SUCCESSFUL；`gitnexus_detect_changes`。

---

## Task D4a-3：收口

- [ ] **Step 1: 全量 clean test**

Run:
```
$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --console=plain
```
Expected: BUILD SUCCESSFUL。

- [ ] **Step 2: 验证测试计数**

读 `build/test-results/test/*.xml`，确认 `LruBufferPoolTest`（含新 3 用例、无 `newPageShouldRejectResidentPage`）、`DiskSpaceManagerTest`（+2 用例）均 `failures=0 errors=0`。

- [ ] **Step 3: GitNexus 刷新** — `npx gitnexus analyze`（失败记录原因）。

- [ ] **Step 4: 更新进度记忆** — 在 `C:\Users\李波\.claude\projects\C--coding-java-self-dbtest-dbtest\memory\storage-build-sequence.md` 标 D4a 完成（allocatePage 产 PAGE_INIT(ALLOCATED)+信封；buf.newPage 页创建语义=X latch+驻留重初始化；批量单 MTR 分配需大 pool）；下一步 D4b（record.page 经 MTR-owned guard 生产入口）需用户确认；R1/F1 后置。

---

## Self-Review（writing-plans 自检）

**Spec coverage：** §2/§3 residency+X-latch-后清零→D4a-1 Step3；§4(a) newPage 要 X→D4a-1 Step3 校验；§4(b) 驻留 reinit→D4a-1 Step3 resetAfterLatch；§4(c) Javadoc→D4a-1 Step4；§5 allocatePage→D4a-2 Step3；§6 redo→D4a-2 测试断言；§8 测试（改写旧测试/SHARED 拒绝/并发边界/api PAGE_INIT/reallocate/pool 调大）→D4a-1 Step1 + D4a-2 Step1；§9 impact→各 Task Impact 段。

**Placeholder scan：** 无 TBD/TODO；acquire 给出完整方法体；测试给完整代码；Impact step 为 MCP 调用。

**Type consistency：** `mtr.newPage(BufferPool, PageId, PageLatchMode, PageType)`（D3 已定）、`PageEnvelope.writeHeader(PageGuard, FilePageHeader)`/`readHeader`/`readPageLsn`、`FilePageHeader(SpaceId,long,long,long,long,PageType)`+`FIL_NULL`、`PageType.ALLOCATED`、`PageInitRecord.pageId()/pageType()`、`mgr.redoLogManager().bufferedRecords()/currentLsn()`、`LruBufferPool` acquire 内 `Arrays`/`Lock`（已 import）一致；`DiskSpaceManager` 新增私有 `doAllocatePage`/`initAllocatedPage` 与 public `allocatePage` 签名不变（调用方无感）。
