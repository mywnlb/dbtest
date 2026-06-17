# F1 — flush/checkpoint/doublewrite first slice Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the first durable dirty-page flush path: BufferPool dirty view, WAL-gated flush, recoverable doublewrite, data-file force, and an in-memory fuzzy checkpoint boundary.

**Architecture:** Keep `storage.flush` as a coordinator layer over existing `BufferPool`, `PageStore`, and `RedoLogManager`. Extend lower layers only with narrow physical capabilities: dirty snapshots in `storage.buf` and `force(SpaceId)` in `storage.fil`. F1 does synchronous foreground flush only; background page cleaner, persistent checkpoint labels, capacity pressure, and adaptive flushing remain later slices.

**Tech Stack:** Java 25, JUnit Jupiter, fixed JDK `C:\Program Files\Java\jdk-25.0.2`, fixed Gradle `D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1`.

**Project overrides:** no git commits; TDD required; no `synchronized`; no `Thread.sleep`; no bare runtime exceptions; Chinese Javadocs; run GitNexus impact before editing existing symbols.

---

## File Structure

- Create: `src/main/java/cn/zhangyis/db/storage/buf/DirtyPageCandidate.java` — dirty view 候选值对象。
- Create: `src/main/java/cn/zhangyis/db/storage/buf/FlushPageSnapshot.java` — flush 用稳定页镜像值对象。
- Modify: `src/main/java/cn/zhangyis/db/storage/buf/BufferPool.java` — 增加 dirty view/snapshot/completeFlush API。
- Modify: `src/main/java/cn/zhangyis/db/storage/buf/BufferFrame.java` — 增加 oldest/newest dirty LSN 与 dirtyVersion 字段。
- Modify: `src/main/java/cn/zhangyis/db/storage/buf/LruBufferPool.java` — 实现 dirty view、snapshot、clean/keep-dirty。
- Create: `src/main/java/cn/zhangyis/db/storage/page/PageImageChecksum.java` — byte[] page image checksum/trailer 工具。
- Modify: `src/main/java/cn/zhangyis/db/storage/fil/PageStore.java` — 增加 `force(SpaceId)`。
- Modify: `src/main/java/cn/zhangyis/db/storage/fil/FileChannelPageStore.java` — 路由 data-file force。
- Modify: `src/main/java/cn/zhangyis/db/storage/fil/DataFileHandle.java` — 在生命周期 S latch 内执行 `channel.force(true)`。
- Create: `src/main/java/cn/zhangyis/db/storage/flush/FlushResultStatus.java` — flush 结果状态枚举。
- Create: `src/main/java/cn/zhangyis/db/storage/flush/FlushResult.java` — 单页 flush 结果。
- Create: `src/main/java/cn/zhangyis/db/storage/flush/FlushWriteException.java` — flush IO/策略失败包装异常。
- Create: `src/main/java/cn/zhangyis/db/storage/flush/DoublewriteMode.java` — doublewrite 模式枚举。
- Create: `src/main/java/cn/zhangyis/db/storage/flush/DoublewriteStrategy.java` — data-file write 前后的策略接口。
- Create: `src/main/java/cn/zhangyis/db/storage/flush/NoDoublewriteStrategy.java` — OFF 模式。
- Create: `src/main/java/cn/zhangyis/db/storage/flush/DoublewriteFileRepository.java` — recoverable doublewrite 文件仓储。
- Create: `src/main/java/cn/zhangyis/db/storage/flush/RecoverableDoublewriteStrategy.java` — DETECT_AND_RECOVER 模式。
- Create: `src/main/java/cn/zhangyis/db/storage/flush/DoublewriteRecoveryScanner.java` — doublewrite 修复 data file helper。
- Create: `src/main/java/cn/zhangyis/db/storage/flush/FlushCoordinator.java` — WAL-gated flush list / single page flush。
- Create: `src/main/java/cn/zhangyis/db/storage/flush/CheckpointCoordinator.java` — in-memory fuzzy checkpoint boundary。
- Create test: `src/test/java/cn/zhangyis/db/storage/buf/BufferPoolDirtyViewTest.java`。
- Create test: `src/test/java/cn/zhangyis/db/storage/page/PageImageChecksumTest.java`。
- Create test: `src/test/java/cn/zhangyis/db/storage/fil/PageStoreForceTest.java`。
- Create test: `src/test/java/cn/zhangyis/db/storage/flush/FlushCoordinatorTest.java`。
- Create test: `src/test/java/cn/zhangyis/db/storage/flush/DoublewriteRecoveryTest.java`。
- Create test: `src/test/java/cn/zhangyis/db/storage/flush/CheckpointCoordinatorTest.java`。

## Task F1-1: BufferPool dirty view

- [x] **Step 1: Run impact analysis before editing BufferPool symbols**

Use GitNexus before touching existing classes:

```text
gitnexus_impact(repo:"dbtest", target:"BufferPool", direction:"upstream")
gitnexus_impact(repo:"dbtest", target:"LruBufferPool", direction:"upstream")
gitnexus_impact(repo:"dbtest", target:"BufferFrame", direction:"upstream")
```

Proceed only after reporting risk. If HIGH or CRITICAL appears, state the blast radius before editing.

- [x] **Step 2: Write failing dirty-view tests**

Create `src/test/java/cn/zhangyis/db/storage/buf/BufferPoolDirtyViewTest.java`:

```java
package cn.zhangyis.db.storage.buf;

import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.fil.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.PageStore;
import cn.zhangyis.db.storage.page.PageEnvelopeLayout;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * F1 dirty view 测试：BufferPool 只暴露候选和页镜像，不泄漏 BufferFrame；flush 完成时用 dirtyVersion 防止误清再次变脏的页。
 */
class BufferPoolDirtyViewTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);

    @TempDir
    Path dir;

    @Test
    void dirtyCandidatesAreOrderedByOldestModificationLsn() {
        try (PageStore store = openStore(8); BufferPool pool = new LruBufferPool(store, PS, 4)) {
            writePageLsn(pool, page(2), 30);
            writePageLsn(pool, page(1), 10);
            writePageLsn(pool, page(3), 20);

            List<DirtyPageCandidate> candidates = pool.dirtyPageCandidates(Lsn.of(25), 10);

            assertEquals(List.of(page(1), page(3)),
                    candidates.stream().map(DirtyPageCandidate::pageId).toList());
            assertEquals(Lsn.of(10), candidates.get(0).oldestModificationLsn());
            assertEquals(Lsn.of(20), candidates.get(1).newestModificationLsn());
        }
    }

    @Test
    void completeFlushKeepsDirtyWhenFrameWasModifiedAgain() {
        try (PageStore store = openStore(8); BufferPool pool = new LruBufferPool(store, PS, 4)) {
            writePageLsn(pool, page(2), 10);
            FlushPageSnapshot snapshot = pool.snapshotForFlush(page(2)).orElseThrow();

            writePageLsn(pool, page(2), 20);

            assertFalse(pool.completeFlush(snapshot));
            assertEquals(Lsn.of(10), pool.oldestDirtyLsnOr(Lsn.of(99)));
            assertTrue(pool.snapshotForFlush(page(2)).isPresent());
        }
    }

    @Test
    void completeFlushMarksCleanWhenSnapshotStillCurrent() {
        try (PageStore store = openStore(8); BufferPool pool = new LruBufferPool(store, PS, 4)) {
            writePageLsn(pool, page(2), 10);
            Optional<FlushPageSnapshot> snapshot = pool.snapshotForFlush(page(2));

            assertTrue(snapshot.isPresent());
            assertTrue(pool.completeFlush(snapshot.orElseThrow()));
            assertEquals(Lsn.of(99), pool.oldestDirtyLsnOr(Lsn.of(99)));
            assertTrue(pool.dirtyPageCandidates(Lsn.of(99), 10).isEmpty());
        }
    }

    private PageStore openStore(int pages) {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(pages));
        return store;
    }

    private static PageId page(long pageNo) {
        return PageId.of(SPACE, PageNo.of(pageNo));
    }

    private static void writePageLsn(BufferPool pool, PageId pageId, long lsn) {
        try (PageGuard guard = pool.getPage(pageId, PageLatchMode.EXCLUSIVE)) {
            guard.writeLong(PageEnvelopeLayout.PAGE_LSN, lsn);
            guard.writeInt(128, (int) lsn);
        }
    }
}
```

- [x] **Step 3: Run dirty-view tests and verify RED**

Run:

```powershell
$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "cn.zhangyis.db.storage.buf.BufferPoolDirtyViewTest" --console=plain
```

Expected: compile fails because `DirtyPageCandidate`, `FlushPageSnapshot`, and new `BufferPool` methods do not exist.

- [x] **Step 4: Add dirty view value objects and API**

Create `DirtyPageCandidate.java`:

```java
package cn.zhangyis.db.storage.buf;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;

/**
 * Buffer Pool 提供给 Flush 模块的脏页候选。它只表达 flush list 排序需要的 LSN 边界，不暴露 frame、latch 或页内容。
 *
 * @param pageId 物理页定位键。
 * @param oldestModificationLsn 该页首次变脏时的 LSN，用于 checkpoint/flush-list 推进。
 * @param newestModificationLsn 当前页 header pageLSN，用于 WAL gate。
 */
public record DirtyPageCandidate(PageId pageId, Lsn oldestModificationLsn, Lsn newestModificationLsn) {
    public DirtyPageCandidate {
        if (pageId == null || oldestModificationLsn == null || newestModificationLsn == null) {
            throw new DatabaseValidationException("dirty page candidate fields must not be null");
        }
        if (oldestModificationLsn.value() > newestModificationLsn.value()) {
            throw new DatabaseValidationException("dirty oldest LSN must not exceed newest LSN");
        }
    }
}
```

Create `FlushPageSnapshot.java`:

```java
package cn.zhangyis.db.storage.buf;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;

import java.util.Arrays;

/**
 * Flush 期间使用的稳定页镜像。创建后不得再依赖 BufferFrame；doublewrite 和 data file write 都写这个副本。
 *
 * @param pageId 页定位键。
 * @param pageLsn snapshot 时页头中的 pageLSN。
 * @param dirtyVersion BufferFrame 的脏版本，用于完成 flush 时判断页是否再次被修改。
 * @param pageImage 整页副本；构造与访问都复制，避免调用方修改 BufferPool 内存。
 */
public record FlushPageSnapshot(PageId pageId, Lsn pageLsn, long dirtyVersion, byte[] pageImage) {
    public FlushPageSnapshot {
        if (pageId == null || pageLsn == null || pageImage == null) {
            throw new DatabaseValidationException("flush page snapshot fields must not be null");
        }
        if (dirtyVersion < 0) {
            throw new DatabaseValidationException("dirty version must not be negative: " + dirtyVersion);
        }
        pageImage = Arrays.copyOf(pageImage, pageImage.length);
    }

    @Override
    public byte[] pageImage() {
        return Arrays.copyOf(pageImage, pageImage.length);
    }
}
```

Modify `BufferPool.java` with imports for `Lsn`, `List`, and `Optional`, then add these methods:

```java
List<DirtyPageCandidate> dirtyPageCandidates(Lsn targetLsn, int maxPages);

Optional<FlushPageSnapshot> snapshotForFlush(PageId pageId);

boolean completeFlush(FlushPageSnapshot snapshot);

void failFlush(PageId pageId);

Lsn oldestDirtyLsnOr(Lsn cleanBoundary);
```

- [x] **Step 5: Implement dirty metadata in LruBufferPool**

Modify `BufferFrame.java` with these fields and Chinese field comments:

```java
Lsn oldestModificationLsn;
Lsn newestModificationLsn;
long dirtyVersion;
```

Modify `LruBufferPool.release` so a write reads `PageEnvelopeLayout.PAGE_LSN` from `frame.data`, sets oldest/newest LSN, increments dirtyVersion, and marks dirty. Add helpers:

```java
private Lsn pageLsn(BufferFrame frame) {
    return Lsn.of(ByteBuffer.wrap(frame.data).getLong(PageEnvelopeLayout.PAGE_LSN));
}

private void markDirty(BufferFrame frame) {
    Lsn pageLsn = pageLsn(frame);
    if (!frame.dirty) {
        frame.oldestModificationLsn = pageLsn;
    }
    frame.newestModificationLsn = pageLsn;
    frame.dirtyVersion++;
    frame.dirty = true;
}
```

Implement dirty view methods under `poolLock`. `snapshotForFlush` returns empty when frame missing, fixed, or clean. `completeFlush` clears dirty only when pageId, dirtyVersion, and pageLSN still match.

- [x] **Step 6: Run dirty-view tests and existing buffer tests**

Run:

```powershell
$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "cn.zhangyis.db.storage.buf.BufferPoolDirtyViewTest" --tests "cn.zhangyis.db.storage.buf.LruBufferPoolTest" --console=plain
```

Expected: pass.

## Task F1-2: Data-file force and page image checksum

- [x] **Step 1: Run impact analysis before editing FIL/page symbols**

```text
gitnexus_impact(repo:"dbtest", target:"PageStore", direction:"upstream")
gitnexus_impact(repo:"dbtest", target:"FileChannelPageStore", direction:"upstream")
gitnexus_impact(repo:"dbtest", target:"DataFileHandle", direction:"upstream")
```

- [x] **Step 2: Write failing force and checksum tests**

Create `PageImageChecksumTest.java`:

```java
package cn.zhangyis.db.storage.page;

import cn.zhangyis.db.domain.PageSize;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * F1 page image checksum 测试：flush 写的是 byte[] snapshot，不能依赖活跃 PageGuard 盖 checksum/trailer。
 */
class PageImageChecksumTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);

    @Test
    void stampAndVerifyPageImageChecksumAndTrailer() {
        byte[] page = new byte[PS.bytes()];
        ByteBuffer.wrap(page).putLong(PageEnvelopeLayout.PAGE_LSN, 0x1_0000_00AAL);
        page[200] = 7;

        PageImageChecksum.stamp(page, PS);

        int checksum = ByteBuffer.wrap(page).getInt(PageEnvelopeLayout.CHECKSUM);
        int trailer = ByteBuffer.wrap(page).getInt(PageEnvelopeLayout.trailerOffset(PS) + PageEnvelopeLayout.TRAILER_CHECKSUM);
        int low32 = ByteBuffer.wrap(page).getInt(PageEnvelopeLayout.trailerOffset(PS) + PageEnvelopeLayout.TRAILER_LOW32_LSN);
        assertEquals(checksum, trailer);
        assertEquals(0x000000AA, low32);
        assertTrue(PageImageChecksum.verify(page, PS));
    }
}
```

Create `PageStoreForceTest.java`:

```java
package cn.zhangyis.db.storage.fil;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * F1 data file force 测试：FlushCoordinator 写 data file 后需要明确 fsync 边界。
 */
class PageStoreForceTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);

    @TempDir
    Path dir;

    @Test
    void forceExistingTablespaceSucceedsAndUnknownTablespaceFails() {
        try (PageStore store = new FileChannelPageStore()) {
            store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(4));
            store.force(SPACE);
            assertThrows(TablespaceNotOpenException.class, () -> store.force(SpaceId.of(99)));
            assertThrows(DatabaseValidationException.class, () -> store.force(null));
        }
    }
}
```

- [x] **Step 3: Run tests and verify RED**

Run:

```powershell
$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "cn.zhangyis.db.storage.page.PageImageChecksumTest" --tests "cn.zhangyis.db.storage.fil.PageStoreForceTest" --console=plain
```

Expected: compile fails because `PageImageChecksum` and `PageStore.force` do not exist.

- [x] **Step 4: Implement `PageImageChecksum`**

Create `src/main/java/cn/zhangyis/db/storage/page/PageImageChecksum.java` with byte[] validation, CRC32 over `[4,pageSize-8)`, `stamp`, and `verify`. It must copy no data and must throw `DatabaseValidationException` for wrong page size.

- [x] **Step 5: Implement `PageStore.force`**

Add `void force(SpaceId spaceId)` to `PageStore`, route it in `FileChannelPageStore`, and add `DataFileHandle.force()`:

```java
void force() {
    try (ResourceGuard ignored = lifecycleLatch.acquireShared()) {
        ensureOpen();
        try {
            channel.force(true);
        } catch (IOException e) {
            throw new DataFilePhysicalException("force data file failed: " + path, e);
        }
    }
}
```

- [x] **Step 6: Run force/checksum tests**

Run the same command from Step 3.

Expected: pass.

## Task F1-3: Doublewrite recoverable file

- [x] **Step 1: Write failing doublewrite recovery test**

Create `src/test/java/cn/zhangyis/db/storage/flush/DoublewriteRecoveryTest.java`:

```java
package cn.zhangyis.db.storage.flush;

import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.FlushPageSnapshot;
import cn.zhangyis.db.storage.fil.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.PageStore;
import cn.zhangyis.db.storage.page.PageEnvelopeLayout;
import cn.zhangyis.db.storage.page.PageImageChecksum;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * F1 doublewrite recovery 测试：data file 出现 torn/corrupt page 时，恢复阶段先用 doublewrite full copy 修复。
 */
class DoublewriteRecoveryTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);
    private static final PageId PAGE = PageId.of(SPACE, PageNo.of(2));

    @TempDir
    Path dir;

    @Test
    void recoverableDoublewriteRepairsCorruptDataPage() {
        try (PageStore store = new FileChannelPageStore();
             DoublewriteFileRepository dw = DoublewriteFileRepository.open(dir.resolve("dw.dat"), PS)) {
            store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(4));
            byte[] image = validPageImage();
            FlushPageSnapshot snapshot = new FlushPageSnapshot(PAGE, Lsn.of(44), 1, image);
            new RecoverableDoublewriteStrategy(dw).beforeDataFileWrite(snapshot);

            byte[] broken = image.clone();
            broken[200] = 99;
            store.writePage(PAGE, ByteBuffer.wrap(broken));
            store.force(SPACE);

            DoublewriteRecoveryScanner scanner = new DoublewriteRecoveryScanner(dw, store, PS);
            assertTrue(scanner.repairPageIfNeeded(PAGE));

            byte[] repaired = new byte[PS.bytes()];
            store.readPage(PAGE, ByteBuffer.wrap(repaired));
            assertTrue(PageImageChecksum.verify(repaired, PS));
            assertEquals(7, repaired[200]);
        }
    }

    private static byte[] validPageImage() {
        byte[] page = new byte[PS.bytes()];
        ByteBuffer buf = ByteBuffer.wrap(page);
        buf.putInt(PageEnvelopeLayout.SPACE_ID, SPACE.value());
        buf.putInt(PageEnvelopeLayout.PAGE_NO, (int) PAGE.pageNo().value());
        buf.putLong(PageEnvelopeLayout.PAGE_LSN, 44L);
        page[200] = 7;
        PageImageChecksum.stamp(page, PS);
        return page;
    }
}
```

- [x] **Step 2: Run test and verify RED**

Run:

```powershell
$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "cn.zhangyis.db.storage.flush.DoublewriteRecoveryTest" --console=plain
```

Expected: compile fails because doublewrite classes do not exist.

- [x] **Step 3: Implement doublewrite classes**

Create:

```java
public enum DoublewriteMode {
    OFF,
    DETECT_AND_RECOVER
}
```

```java
public interface DoublewriteStrategy {
    DoublewriteMode mode();
    void beforeDataFileWrite(FlushPageSnapshot snapshot);
    default void afterDataFileWrite(FlushPageSnapshot snapshot) { }
}
```

`NoDoublewriteStrategy` returns `OFF` and no-ops.

`DoublewriteFileRepository` writes append-only slots and exposes `Optional<byte[]> latestCopy(PageId pageId)` for tests and recovery:

```text
magic int
format int
spaceId int
pageNo long
pageLsn long
pageSize int
payloadCrc int
payload byte[pageSize]
```

It must hold a `ReentrantLock` around append, scan, and force; it must throw `DatabaseValidationException` for null arguments and `FlushWriteException` for IO failures.

`latestCopy(pageId)` scans valid slots for the matching page and returns a defensive copy of the newest payload whose CRC and page size are valid; it returns `Optional.empty()` when no valid copy exists.

`RecoverableDoublewriteStrategy.beforeDataFileWrite(snapshot)` appends `snapshot.pageImage()` then forces the doublewrite file before returning.

`DoublewriteRecoveryScanner.repairPageIfNeeded(pageId)` reads the current data page, returns false when checksum is already valid, otherwise scans latest matching valid doublewrite slot, writes it to `PageStore`, forces the space, and returns true.

- [x] **Step 4: Run doublewrite recovery test**

Run the same command from Step 2.

Expected: pass.

## Task F1-4: FlushCoordinator WAL-gated data page flush

- [x] **Step 1: Write failing flush coordinator tests**

Create `src/test/java/cn/zhangyis/db/storage/flush/FlushCoordinatorTest.java`:

```java
package cn.zhangyis.db.storage.flush;

import cn.zhangyis.db.domain.Lsn;
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
import cn.zhangyis.db.storage.page.PageEnvelopeLayout;
import cn.zhangyis.db.storage.redo.PageBytesRecord;
import cn.zhangyis.db.storage.redo.RedoLogFileRepository;
import cn.zhangyis.db.storage.redo.RedoLogManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * F1 flush coordinator 测试：data page write 必须被 redo durable gate 和 doublewrite 顺序保护。
 */
class FlushCoordinatorTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);
    private static final PageId PAGE = PageId.of(SPACE, PageNo.of(2));

    @TempDir
    Path dir;

    @Test
    void flushListSkipsPageWhenRedoIsNotDurable() {
        try (PageStore store = new FileChannelPageStore(); BufferPool pool = new LruBufferPool(store, PS, 4)) {
            store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(4));
            markDirty(pool, 20, 0xCA);
            RedoLogManager redo = new RedoLogManager();
            FlushCoordinator coordinator = new FlushCoordinator(pool, store, redo, PS,
                    new NoDoublewriteStrategy(), Duration.ZERO);

            List<FlushResult> results = coordinator.flushList(Lsn.of(100), 10);

            assertEquals(FlushResultStatus.SKIPPED_REDO_NOT_DURABLE, results.get(0).status());
            assertTrue(pool.snapshotForFlush(PAGE).isPresent());
            byte[] disk = new byte[PS.bytes()];
            store.readPage(PAGE, ByteBuffer.wrap(disk));
            assertEquals(0, disk[200]);
        }
    }

    @Test
    void flushListWritesDoublewriteBeforeDataFileAndMarksClean() {
        try (PageStore store = new FileChannelPageStore();
             BufferPool pool = new LruBufferPool(store, PS, 4);
             RedoLogFileRepository redoRepo = RedoLogFileRepository.open(dir.resolve("redo.log"));
             DoublewriteFileRepository dwRepo = DoublewriteFileRepository.open(dir.resolve("dw.dat"), PS)) {
            store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(4));
            RedoLogManager redo = RedoLogManager.durable(redoRepo);
            LogRangeHolder range = appendRedoCoveringPage(redo);
            markDirty(pool, range.endLsn(), 0xCB);
            redo.flush();
            FlushCoordinator coordinator = new FlushCoordinator(pool, store, redo, PS,
                    new RecoverableDoublewriteStrategy(dwRepo), Duration.ofMillis(10));

            List<FlushResult> results = coordinator.flushList(Lsn.of(1000), 10);

            assertEquals(FlushResultStatus.CLEAN, results.get(0).status());
            assertTrue(pool.dirtyPageCandidates(Lsn.of(1000), 10).isEmpty());
            byte[] disk = new byte[PS.bytes()];
            store.readPage(PAGE, ByteBuffer.wrap(disk));
            assertEquals((byte) 0xCB, disk[200]);
            assertTrue(dwRepo.latestCopy(PAGE).isPresent());
        }
    }

    private static void markDirty(BufferPool pool, long pageLsn, int marker) {
        try (PageGuard guard = pool.getPage(PAGE, PageLatchMode.EXCLUSIVE)) {
            guard.writeInt(PageEnvelopeLayout.SPACE_ID, SPACE.value());
            guard.writeInt(PageEnvelopeLayout.PAGE_NO, (int) PAGE.pageNo().value());
            guard.writeLong(PageEnvelopeLayout.PAGE_LSN, pageLsn);
            guard.writeBytes(200, new byte[]{(byte) marker});
        }
    }

    private static LogRangeHolder appendRedoCoveringPage(RedoLogManager redo) {
        PageBytesRecord record = new PageBytesRecord(PAGE, 200, new byte[]{(byte) 0xCB});
        cn.zhangyis.db.storage.redo.LogRange range = redo.append(List.of(record));
        return new LogRangeHolder(range.end().value());
    }

    private record LogRangeHolder(long endLsn) { }
}
```

- [x] **Step 2: Run test and verify RED**

Run:

```powershell
$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "cn.zhangyis.db.storage.flush.FlushCoordinatorTest" --console=plain
```

Expected: compile fails because `FlushCoordinator`, `FlushResult`, and `FlushResultStatus` do not exist.

- [x] **Step 3: Implement flush coordinator result types**

Create `FlushResultStatus`:

```java
public enum FlushResultStatus {
    CLEAN,
    KEPT_DIRTY,
    SKIPPED_NOT_DIRTY,
    SKIPPED_REDO_NOT_DURABLE,
    FAILED
}
```

Create `FlushResult` record with `pageId`, `pageLsn`, `status`, and optional `DatabaseRuntimeException failure`; validate status/page fields.

Create `FlushWriteException extends DatabaseRuntimeException` with message and message+cause constructors.

- [x] **Step 4: Implement `FlushCoordinator`**

Implement constructor validation and:

```java
public List<FlushResult> flushList(Lsn targetLsn, int maxPages)
public FlushResult singlePageFlush(PageId pageId)
```

Per page algorithm:

```text
candidate -> snapshotForFlush
if no snapshot: SKIPPED_NOT_DIRTY
if pageLsn > redo.flushedToDiskLsn and waitFlushed false: SKIPPED_REDO_NOT_DURABLE
stamp checksum on snapshot image
doublewrite.beforeDataFileWrite
pageStore.writePage
pageStore.force(spaceId)
clean = bufferPool.completeFlush(snapshot)
doublewrite.afterDataFileWrite
return CLEAN or KEPT_DIRTY
```

Catch `DatabaseRuntimeException` around doublewrite/data-file write, call `bufferPool.failFlush(pageId)`, and return `FAILED` with cause.

- [x] **Step 5: Run flush coordinator tests**

Run the same command from Step 2.

Expected: pass.

## Task F1-5: In-memory checkpoint coordinator

- [x] **Step 1: Write failing checkpoint tests**

Create `src/test/java/cn/zhangyis/db/storage/flush/CheckpointCoordinatorTest.java`:

```java
package cn.zhangyis.db.storage.flush;

import cn.zhangyis.db.domain.Lsn;
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
import cn.zhangyis.db.storage.page.PageEnvelopeLayout;
import cn.zhangyis.db.storage.redo.PageBytesRecord;
import cn.zhangyis.db.storage.redo.RedoLogFileRepository;
import cn.zhangyis.db.storage.redo.RedoLogManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * F1 checkpoint 测试：safe checkpoint LSN 是 dirty oldest、redo current/closed、redo flushed 的安全交集。
 */
class CheckpointCoordinatorTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);
    private static final PageId PAGE = PageId.of(SPACE, PageNo.of(2));

    @TempDir
    Path dir;

    @Test
    void safeCheckpointDoesNotPassOldestDirtyOrRedoDurable() {
        try (PageStore store = new FileChannelPageStore();
             BufferPool pool = new LruBufferPool(store, PS, 4);
             RedoLogFileRepository repo = RedoLogFileRepository.open(dir.resolve("redo.log"))) {
            store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(4));
            RedoLogManager redo = RedoLogManager.durable(repo);
            redo.append(List.of(new PageBytesRecord(PAGE, 200, new byte[]{1})));
            redo.flush();
            long durable = redo.flushedToDiskLsn().value();
            writeDirty(pool, durable);

            CheckpointCoordinator coordinator = new CheckpointCoordinator(pool, redo);

            assertEquals(Lsn.of(durable), coordinator.computeSafeCheckpointLsn());
            assertEquals(Lsn.of(durable), coordinator.advanceCheckpoint());
            assertEquals(Lsn.of(durable), coordinator.lastCheckpointLsn());
        }
    }

    @Test
    void safeCheckpointUsesRedoFlushedWhenThereAreNoDirtyPages() {
        try (PageStore store = new FileChannelPageStore();
             BufferPool pool = new LruBufferPool(store, PS, 4);
             RedoLogFileRepository repo = RedoLogFileRepository.open(dir.resolve("redo.log"))) {
            store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(4));
            RedoLogManager redo = RedoLogManager.durable(repo);
            redo.append(List.of(new PageBytesRecord(PAGE, 200, new byte[]{1})));
            redo.flush();

            CheckpointCoordinator coordinator = new CheckpointCoordinator(pool, redo);

            assertEquals(redo.flushedToDiskLsn(), coordinator.computeSafeCheckpointLsn());
        }
    }

    private static void writeDirty(BufferPool pool, long lsn) {
        try (PageGuard guard = pool.getPage(PAGE, PageLatchMode.EXCLUSIVE)) {
            guard.writeLong(PageEnvelopeLayout.PAGE_LSN, lsn);
            guard.writeBytes(200, new byte[]{1});
        }
    }
}
```

- [x] **Step 2: Run test and verify RED**

Run:

```powershell
$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "cn.zhangyis.db.storage.flush.CheckpointCoordinatorTest" --console=plain
```

Expected: compile fails because `CheckpointCoordinator` does not exist.

- [x] **Step 3: Implement `CheckpointCoordinator`**

Create `CheckpointCoordinator` with `ReentrantLock`, `lastCheckpointLsn`, `computeSafeCheckpointLsn()`, `advanceCheckpoint()`, and `lastCheckpointLsn()`. The safe LSN is the minimum of:

```text
bufferPool.oldestDirtyLsnOr(redo.currentLsn())
redo.currentLsn()
redo.flushedToDiskLsn()
```

`advanceCheckpoint()` updates `lastCheckpointLsn` only when the computed safe LSN is greater.

- [x] **Step 4: Run checkpoint tests**

Run the same command from Step 2.

Expected: pass.

## Task F1-6: Regression and closeout

- [x] **Step 1: Run targeted package tests**

Run:

```powershell
$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "cn.zhangyis.db.storage.buf.*" --tests "cn.zhangyis.db.storage.fil.*" --tests "cn.zhangyis.db.storage.flush.*" --tests "cn.zhangyis.db.storage.redo.*" --console=plain
```

Expected: pass.

- [x] **Step 2: Run full clean test**

Run:

```powershell
$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" clean test --console=plain
```

Expected: build success and zero test failures/errors.

- [x] **Step 3: Inspect test XML counts and log parser output**

Run:

```powershell
$tests=0; $failures=0; $errors=0; Get-ChildItem -LiteralPath 'build\test-results\test' -Filter 'TEST-*.xml' | ForEach-Object { [xml]$x=Get-Content -Raw -LiteralPath $_.FullName; $tests += [int]$x.testsuite.tests; $failures += [int]$x.testsuite.failures; $errors += [int]$x.testsuite.errors }; Write-Output "tests=$tests failures=$failures errors=$errors"
rg -n 'PARSER_ERROR|Failed to instantiate converter|ERROR in ch\.qos\.logback|BUILD FAILED|failures="[1-9]|errors="[1-9]' build\test-results\test
```

Expected: failures/errors are 0; `rg` returns no matches.

- [x] **Step 4: Run GitNexus scope checks**

Run:

```text
gitnexus_detect_changes(repo:"dbtest", scope:"all")
```

Expected in this repository: may fail with `fatal: ambiguous argument 'HEAD'` until the repo has a valid `HEAD`; record the exact result.

- [x] **Step 5: Refresh GitNexus index**

Run:

```powershell
npx gitnexus analyze --force
```

Expected: repository indexed successfully. Record nodes/edges/flows; if indexing fails, record the error.

## Self-Review

Spec coverage: F1 plan maps dirty view, redo durable gate, page image checksum/trailer, doublewrite full-copy repair, data-file force, and safe checkpoint LSN to explicit tests and implementation tasks.

Marker scan: every task has concrete file paths, commands, expected results, and named APIs.

Type consistency: plan consistently uses `DirtyPageCandidate`, `FlushPageSnapshot`, `PageImageChecksum`, `DoublewriteFileRepository`, `RecoverableDoublewriteStrategy`, `FlushCoordinator`, and `CheckpointCoordinator`.
