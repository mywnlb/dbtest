# Disk Manager 0.16b/0.16c Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement disk-manager 0.16b and 0.16c: persisted GENERAL `CORRUPTED` lifecycle state and `DataFileGateway` / `PreallocationStrategy` file growth abstraction.

**Architecture:** Extend the existing page0 lifecycle marker so GENERAL tablespaces can persist `NORMAL` / `CORRUPTED` without changing `PageStore` boundaries. Extract file range initialization from `DataFileHandle` into a fil.io gateway while keeping `DataFileHandle` as the lock owner and preserving `Lifecycle -> FileSize -> Fsync` ordering.

**Tech Stack:** Java 25, Gradle 9.5.1, JUnit Jupiter, explicit `java.util.concurrent` locks, project exception hierarchy.

## Global Constraints

- Read and follow `docs/superpowers/specs/2026-07-06-disk-manager-0.16bc-design.md` before coding.
- Use `docs/design/innodb-disk-manager-design.md`, `docs/design/storage-backlog.md`, and `docs/design/current-implementation-map.md` as authority for module boundaries.
- Do not add `synchronized`, `wait()`, `notify()`, or `notifyAll()`.
- Do not throw bare `IllegalArgumentException` or bare `RuntimeException` in production code.
- Preserve `PageStore` registry-free; `storage.fil.io` must not import registry, FSP, Buffer Pool, redo, flush, SQL, or session modules.
- Preserve data-file physical lock order: `TablespaceLifecycleLatch -> FileSizeLock -> FsyncLock`.
- Write failing tests before production changes.
- Use fixed Gradle first: `$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test`.
- Update `docs/design/current-implementation-map.md` and `docs/design/storage-backlog.md` after implementation.

---

## File Structure

- Modify `src/main/java/cn/zhangyis/db/storage/fsp/lifecycle/TablespaceLifecycleHeader.java`: broaden page0 lifecycle validation to support GENERAL stable `NORMAL/CORRUPTED` markers while preserving UNDO truncate invariants.
- Modify `src/main/java/cn/zhangyis/db/storage/fsp/lifecycle/TablespaceLifecycleRawCodec.java`: update Javadoc from UNDO-only wording to page0 lifecycle marker wording; keep strict decoding and old-format `magic=0` compatibility.
- Modify `src/main/java/cn/zhangyis/db/storage/api/DiskSpaceManager.java`: write GENERAL lifecycle on create; add persisted `markTablespaceCorrupted(MiniTransaction, SpaceId, String)` overload; clarify existing runtime-only method.
- Modify `src/main/java/cn/zhangyis/db/storage/api/tablespace/PageZeroTablespaceMetadataLoader.java`: map GENERAL persisted lifecycle state into loaded `TablespaceMetadata`.
- Create `src/main/java/cn/zhangyis/db/storage/fil/io/DataFileGateway.java`: gateway interface for physical page range initialization/allocation.
- Create `src/main/java/cn/zhangyis/db/storage/fil/io/ZeroFillDataFileGateway.java`: default cross-platform zero-fill implementation.
- Create `src/main/java/cn/zhangyis/db/storage/fil/io/PreallocationStrategy.java`: platform preallocation seam used by `PreallocatingDataFileGateway`; default production construction still uses zero-fill directly.
- Create `src/main/java/cn/zhangyis/db/storage/fil/io/NoOpPreallocationStrategy.java`: default no-op preallocation strategy.
- Create `src/main/java/cn/zhangyis/db/storage/fil/io/PreallocatingDataFileGateway.java`: adapter that tries preallocation then guarantees zero-filled pages; production default remains `ZeroFillDataFileGateway`.
- Modify `src/main/java/cn/zhangyis/db/storage/fil/io/DataFileHandle.java`: delegate create/extend/ensureCapacity range initialization to gateway.
- Modify `src/main/java/cn/zhangyis/db/storage/fil/io/FileChannelPageStore.java`: wire default gateway and test injection constructor.
- Add tests under `src/test/java/cn/zhangyis/db/storage/api`, `src/test/java/cn/zhangyis/db/storage/fsp/lifecycle`, and `src/test/java/cn/zhangyis/db/storage/fil/io`.

---

### Task 1: Add 0.16b Red Tests

**Files:**
- Create or modify: `src/test/java/cn/zhangyis/db/storage/api/DiskSpaceManagerGeneralLifecyclePersistenceTest.java`
- Create or modify: `src/test/java/cn/zhangyis/db/storage/fsp/lifecycle/TablespaceLifecycleRawCodecTest.java`

**Interfaces:**
- Consumes: existing `DiskSpaceManager.createTablespace`, `DiskSpaceManager.openTablespace`, `DiskSpaceManager.openTablespaceForRecovery`, `DiskSpaceManager.tablespaceState`.
- Produces: failing test expectations for persisted GENERAL lifecycle state and corrupted ordinary-access rejection.

- [ ] **Step 1: Inspect existing disk-manager test helpers**

Use Glob/Read to inspect nearby tests before adding setup code:

```text
src/test/java/cn/zhangyis/db/storage/api/*DiskSpaceManager*Test.java
src/test/java/cn/zhangyis/db/storage/fil/io/*Test.java
```

- [ ] **Step 2: Write GENERAL normal lifecycle persistence test**

Add a test method named `generalTablespacePersistsNormalLifecycleOnCreate` that:

```java
@Test
void generalTablespacePersistsNormalLifecycleOnCreate() {
    // Arrange: create temporary data file, BufferPool/PageStore/MTR/DiskSpaceManager using the durable redo + flushAllDirty pattern from DiskSpaceManagerTablespaceAdmissionTest.
    // Act: create GENERAL tablespace, commit MTR, call flushAllDirty(pool, store, redo), close, reopen.
    // Assert: disk.tablespaceState(spaceId) == TablespaceState.NORMAL.
    // Assert: raw page0 lifecycle codec returns present header with state NORMAL.
}
```

Keep setup identical to existing `DiskSpaceManagerTablespaceAdmissionTest` style to avoid introducing new harness abstractions.

- [ ] **Step 3: Write persisted corrupted reopen test**

Add a test method named `persistedCorruptedBlocksOrdinaryAccessAfterReopen` that:

```java
@Test
void persistedCorruptedBlocksOrdinaryAccessAfterReopen() {
    // Arrange: create GENERAL tablespace with durable redo and commit.
    // Act: call new persisted overload markTablespaceCorrupted(mtr, spaceId, "checksum mismatch"), commit, call flushAllDirty(pool, store, redo), close, reopen.
    // Assert: disk.tablespaceState(spaceId) == TablespaceState.CORRUPTED.
    // Assert: ordinary space-management API such as createSegment(mtr, spaceId, SegmentPurpose.INDEX_LEAF) throws TablespaceCorruptedException.
}
```

- [ ] **Step 4: Write recovery-open corrupted test**

Add a test method named `recoveryOpenAllowsPersistedCorruptedTablespace` that:

```java
@Test
void recoveryOpenAllowsPersistedCorruptedTablespace() {
    // Arrange: create GENERAL tablespace, persist CORRUPTED, call flushAllDirty(pool, store, redo), close.
    // Act: open the same file using openTablespaceForRecovery.
    // Assert: recovery open does not throw.
    // Assert: runtime state is CORRUPTED if observable through tablespaceState or registry-backed helper.
}
```

- [ ] **Step 5: Write raw codec old-format compatibility test**

Add a test method named `oldFormatLifecycleMagicZeroReturnsEmpty`:

```java
@Test
void oldFormatLifecycleMagicZeroReturnsEmpty() {
    ByteBuffer page = ByteBuffer.allocate(PageSize.ofBytes(16 * 1024).bytes());
    assertTrue(TablespaceLifecycleRawCodec.read(page).isEmpty());
}
```

Use the project’s existing `PageSize.ofBytes(16 * 1024)` factory.

- [ ] **Step 6: Run red tests**

Run:

```powershell
$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "*DiskSpaceManagerGeneralLifecyclePersistenceTest" --tests "*TablespaceLifecycleRawCodecTest"
```

Expected: compile or assertion failures because persisted GENERAL lifecycle and the new overload are not implemented.

---

### Task 2: Implement 0.16b Lifecycle Persistence

**Files:**
- Modify: `src/main/java/cn/zhangyis/db/storage/fsp/lifecycle/TablespaceLifecycleHeader.java`
- Modify: `src/main/java/cn/zhangyis/db/storage/api/DiskSpaceManager.java`
- Modify: `src/main/java/cn/zhangyis/db/storage/api/tablespace/PageZeroTablespaceMetadataLoader.java`
- Modify: `src/main/java/cn/zhangyis/db/storage/fsp/lifecycle/TablespaceLifecycleRawCodec.java` to update Javadoc from UNDO-only wording to page0 lifecycle marker wording.

**Interfaces:**
- Consumes: existing `SpaceHeaderRepository.writeLifecycle`, `TablespaceLifecycleHeader`, `TablespaceLifecycleRawCodec`, `TablespaceRegistry`.
- Produces: `DiskSpaceManager.markTablespaceCorrupted(MiniTransaction mtr, SpaceId spaceId, String reason)` persisted overload.

- [ ] **Step 1: Update lifecycle header validation**

Modify `TablespaceLifecycleHeader` compact constructor so it accepts stable GENERAL states:

```java
boolean stableGeneralState = state == TablespaceState.NORMAL || state == TablespaceState.CORRUPTED;
boolean undoLifecycleState = state == TablespaceState.ACTIVE
        || state == TablespaceState.INACTIVE
        || state == TablespaceState.TRUNCATING;
if (!stableGeneralState && !undoLifecycleState) {
    throw new DatabaseValidationException("unsupported tablespace lifecycle state: " + state);
}
if (stableGeneralState && finishState != TablespaceState.NORMAL) {
    throw new DatabaseValidationException("general lifecycle finish state must be NORMAL: " + finishState);
}
if (undoLifecycleState && finishState != TablespaceState.ACTIVE && finishState != TablespaceState.INACTIVE) {
    throw new DatabaseValidationException("truncate finish state must be ACTIVE or INACTIVE: " + finishState);
}
```

Preserve existing null, positive-size, target-size, and non-negative epoch checks.

- [ ] **Step 2: Write GENERAL normal marker during create**

In `DiskSpaceManager.createTablespace`, after `headerRepo.initialize(mtr, fresh)` and before `xdes.reserveSystemExtent`, write lifecycle for GENERAL:

```java
if (type == TablespaceType.UNDO) {
    headerRepo.writeLifecycle(mtr, spaceId, new TablespaceLifecycleHeader(
            initialState, initialSizePages, 0L, initialSizePages, TablespaceState.ACTIVE));
} else if (type == TablespaceType.GENERAL) {
    headerRepo.writeLifecycle(mtr, spaceId, new TablespaceLifecycleHeader(
            TablespaceState.NORMAL, initialSizePages, 0L, initialSizePages, TablespaceState.NORMAL));
}
```

- [ ] **Step 3: Add persisted corrupted overload**

In `DiskSpaceManager`, add:

```java
/**
 * 持久标记 GENERAL 表空间 CORRUPTED。该方法在调用方提供的 MTR 中写 page0 lifecycle marker，
 * 再发布 registry 状态；重启后 page0 loader 会恢复 CORRUPTED 并让普通 require 拒绝访问。
 */
public void markTablespaceCorrupted(MiniTransaction mtr, SpaceId spaceId, String reason) {
    requireMtr(mtr);
    requireSpace(spaceId);
    if (reason == null || reason.isBlank()) {
        throw new DatabaseValidationException("corruption reason must not be blank");
    }
    requireOrdinaryAccess(mtr, spaceId);
    // 该方法随后会写 page0 lifecycle marker，必须直接用 X latch 读取，避免同一 MTR 内 S->X 升级触发 MTR 防护。
    SpaceHeaderSnapshot snapshot = headerRepo.readForUpdate(mtr, spaceId);
    TablespaceType type = TablespaceTypeFlags.decode(snapshot.spaceFlags());
    if (type != TablespaceType.GENERAL) {
        throw new DatabaseValidationException("persistent corrupted marker is only supported for GENERAL tablespace: " + type);
    }
    headerRepo.writeLifecycle(mtr, spaceId, new TablespaceLifecycleHeader(
            TablespaceState.CORRUPTED,
            snapshot.currentSizeInPages(),
            0L,
            snapshot.currentSizeInPages(),
            TablespaceState.NORMAL));
    registry.markCorrupted(spaceId, reason);
}
```

`SpaceHeaderSnapshot` is a Java record; use `snapshot.spaceFlags()` and `snapshot.currentSizeInPages()`. Use
`readForUpdate` rather than `read`: `MiniTransaction` explicitly forbids same-page S/SX -> X latch upgrade.

- [ ] **Step 4: Clarify runtime-only method**

Update the existing `markTablespaceCorrupted(SpaceId, String)` Javadoc to say it only changes registry runtime state and does not persist to page0.

- [ ] **Step 5: Update page0 metadata loader**

In `PageZeroTablespaceMetadataLoader`, replace the current non-UNDO lifecycle rejection with type-aware validation after decoding lifecycle:

```java
TablespaceState state = lifecycle.map(TablespaceLifecycleHeader::state).orElse(TablespaceState.NORMAL);
if (type == TablespaceType.GENERAL) {
    if (state != TablespaceState.NORMAL && state != TablespaceState.CORRUPTED) {
        throw new FspMetadataException("invalid GENERAL lifecycle state: " + state);
    }
} else if (type == TablespaceType.UNDO && lifecycle.isPresent()) {
    if (state != TablespaceState.ACTIVE
            && state != TablespaceState.INACTIVE
            && state != TablespaceState.TRUNCATING) {
        throw new FspMetadataException("invalid UNDO lifecycle state: " + state);
    }
} else if (lifecycle.isPresent()) {
    throw new FspMetadataException("unsupported lifecycle marker for tablespace type: " + type);
}
```

Apply this at the point where the loader currently derives `TablespaceState` from `TablespaceLifecycleRawCodec.read(page)`.
Remove the existing `if (type != TablespaceType.UNDO && lifecycle.isPresent())` guard, otherwise GENERAL markers will still be rejected.
Add the `FspMetadataException` import if the file does not already have it. Preserve old UNDO files with no lifecycle marker by keeping
the absent-marker default as `NORMAL`.

- [ ] **Step 6: Run 0.16b tests**

Run:

```powershell
$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "*DiskSpaceManagerGeneralLifecyclePersistenceTest" --tests "*TablespaceLifecycleRawCodecTest"
```

Expected: PASS.

- [ ] **Step 7: Run disk-manager related regression**

Run:

```powershell
$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "*DiskSpaceManager*" --tests "*TablespaceLifecycle*" --tests "*PageZeroTablespaceMetadataLoader*"
```

Expected: PASS.

---

### Task 3: Add 0.16c Red Tests

**Files:**
- Create: `src/test/java/cn/zhangyis/db/storage/fil/io/DataFileGatewayTest.java`
- Create: `src/test/java/cn/zhangyis/db/storage/fil/io/DataFileHandleGatewayTest.java`
- Create: `src/test/java/cn/zhangyis/db/storage/fil/io/FileChannelPageStoreGatewayTest.java`

**Interfaces:**
- Consumes: existing `DataFileHandle`, `FileChannelPageStore` behavior.
- Produces: failing expectations for `DataFileGateway`, `ZeroFillDataFileGateway`, and gateway injection.

- [ ] **Step 1: Add zero-fill gateway test**

Create `DataFileGatewayTest` in package `cn.zhangyis.db.storage.fil.io` with a test named `zeroFillGatewayWritesFullPageRange`:

```java
@Test
void zeroFillGatewayWritesFullPageRange(@TempDir Path tempDir) throws IOException {
    Path file = tempDir.resolve("space.ibd");
    try (FileChannel channel = FileChannel.open(file, StandardOpenOption.CREATE_NEW,
            StandardOpenOption.READ, StandardOpenOption.WRITE)) {
        DataFileGateway gateway = new ZeroFillDataFileGateway();
        gateway.initialize(channel, 0, 3, PageSize.ofBytes(16 * 1024), file);
        assertEquals(3L * 16 * 1024, channel.size());
        ByteBuffer page = ByteBuffer.allocate(16 * 1024);
        channel.read(page, 2L * 16 * 1024);
        page.flip();
        while (page.hasRemaining()) {
            assertEquals(0, page.get());
        }
    }
}
```

Use `PageSize.ofBytes(16 * 1024)` consistently in this test file.

- [ ] **Step 2: Add create delegation test**

Create a recording gateway in the test file:

```java
final class RecordingGateway implements DataFileGateway {
    long from = -1;
    long to = -1;
    int calls;

    @Override
    public void initialize(FileChannel channel, long fromPageInclusive, long toPageExclusive, PageSize pageSize, Path path) {
        calls++;
        from = fromPageInclusive;
        to = toPageExclusive;
        new ZeroFillDataFileGateway().initialize(channel, fromPageInclusive, toPageExclusive, pageSize, path);
    }

    @Override
    public void ensureAllocated(FileChannel channel, long fromPageInclusive, long toPageExclusive, PageSize pageSize, Path path) {
        initialize(channel, fromPageInclusive, toPageExclusive, pageSize, path);
    }
}
```

Add `createDelegatesInitializationToGateway`:

```java
@Test
void createDelegatesInitializationToGateway(@TempDir Path tempDir) {
    RecordingGateway gateway = new RecordingGateway();
    DataFileHandle handle = DataFileHandle.create(SpaceId.of(1), tempDir.resolve("space.ibd"),
            PageSize.ofBytes(16 * 1024), PageNo.of(4), gateway);
    try (handle) {
        assertEquals(1, gateway.calls);
        assertEquals(0, gateway.from);
        assertEquals(4, gateway.to);
        assertEquals(4, handle.currentSizeInPages());
    }
}
```

- [ ] **Step 3: Add extend failure publish test**

Add `autoExtendDoesNotPublishSizeWhenGatewayFails` using a gateway that throws `DataFilePhysicalException`:

```java
@Test
void autoExtendDoesNotPublishSizeWhenGatewayFails(@TempDir Path tempDir) {
    DataFileGateway failing = new DataFileGateway() {
        @Override
        public void initialize(FileChannel channel, long fromPageInclusive, long toPageExclusive, PageSize pageSize, Path path) {
            new ZeroFillDataFileGateway().initialize(channel, fromPageInclusive, toPageExclusive, pageSize, path);
        }

        @Override
        public void ensureAllocated(FileChannel channel, long fromPageInclusive, long toPageExclusive, PageSize pageSize, Path path) {
            throw new DataFilePhysicalException("injected gateway failure");
        }
    };
    DataFileHandle handle = DataFileHandle.create(SpaceId.of(1), tempDir.resolve("space.ibd"),
            PageSize.ofBytes(16 * 1024), PageNo.of(2), failing);
    try (handle) {
        assertThrows(DataFilePhysicalException.class, () -> handle.autoExtend((current, pageSize) -> 1));
        assertEquals(2, handle.currentSizeInPages());
    }
}
```

- [ ] **Step 4: Add ensureCapacity idempotence test**

Add `ensureCapacityUsesGatewayOnlyWhenGrowing`:

```java
@Test
void ensureCapacityUsesGatewayOnlyWhenGrowing(@TempDir Path tempDir) {
    RecordingGateway gateway = new RecordingGateway();
    DataFileHandle handle = DataFileHandle.create(SpaceId.of(1), tempDir.resolve("space.ibd"),
            PageSize.ofBytes(16 * 1024), PageNo.of(2), gateway);
    try (handle) {
        gateway.calls = 0;
        handle.ensureCapacity(PageNo.of(5));
        assertEquals(1, gateway.calls);
        assertEquals(2, gateway.from);
        assertEquals(5, gateway.to);
        handle.ensureCapacity(PageNo.of(5));
        handle.ensureCapacity(PageNo.of(3));
        assertEquals(1, gateway.calls);
    }
}
```

- [ ] **Step 5: Add PageStore default behavior test**

Create `FileChannelPageStoreGatewayTest.defaultConstructorKeepsZeroFillBehavior` using existing `FileChannelPageStore` create/extend/read APIs. Assert an extended page reads as all zero bytes.

- [ ] **Step 6: Run red tests**

Run:

```powershell
$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "*DataFileGatewayTest" --tests "*DataFileHandleGatewayTest" --tests "*FileChannelPageStoreGatewayTest"
```

Expected: compile failures because gateway types and overloads do not exist.

---

### Task 4: Implement 0.16c Gateway

**Files:**
- Create: `src/main/java/cn/zhangyis/db/storage/fil/io/DataFileGateway.java`
- Create: `src/main/java/cn/zhangyis/db/storage/fil/io/ZeroFillDataFileGateway.java`
- Create: `src/main/java/cn/zhangyis/db/storage/fil/io/PreallocationStrategy.java`
- Create: `src/main/java/cn/zhangyis/db/storage/fil/io/NoOpPreallocationStrategy.java`
- Create: `src/main/java/cn/zhangyis/db/storage/fil/io/PreallocatingDataFileGateway.java`
- Modify: `src/main/java/cn/zhangyis/db/storage/fil/io/DataFileHandle.java`
- Modify: `src/main/java/cn/zhangyis/db/storage/fil/io/FileChannelPageStore.java`

**Interfaces:**
- Consumes: `FileChannel`, `PageSize`, `Path`, existing `DataFileHandle` lifecycle locks.
- Produces: `DataFileGateway`, `ZeroFillDataFileGateway`, injectable `FileChannelPageStore(AutoExtendPolicy, DataFileGateway)`.

- [ ] **Step 1: Create DataFileGateway**

```java
package cn.zhangyis.db.storage.fil.io;

import cn.zhangyis.db.domain.PageSize;

import java.nio.channels.FileChannel;
import java.nio.file.Path;

/**
 * data-file 物理范围初始化网关。调用方已经持有 DataFileHandle 的生命周期/文件大小锁；
 * 本接口只处理 FileChannel 上的 page range 初始化或预分配，不回调 Buffer Pool、registry、redo 或 flush。
 */
interface DataFileGateway {
    void initialize(FileChannel channel, long fromPageInclusive, long toPageExclusive, PageSize pageSize, Path path);

    void ensureAllocated(FileChannel channel, long fromPageInclusive, long toPageExclusive, PageSize pageSize, Path path);
}
```

- [ ] **Step 2: Create ZeroFillDataFileGateway**

Move the current `DataFileHandle.zeroFill` implementation into this class. Keep validation and wrap `IOException` as `DataFilePhysicalException` with path/cause.

- [ ] **Step 3: Create PreallocationStrategy and NoOp implementation**

```java
interface PreallocationStrategy {
    void preallocate(FileChannel channel, long offsetBytes, long lengthBytes, Path path);
}

final class NoOpPreallocationStrategy implements PreallocationStrategy {
    @Override
    public void preallocate(FileChannel channel, long offsetBytes, long lengthBytes, Path path) {
        // 默认跨平台实现不做 native 预分配；DataFileGateway 仍会零填充保证新页内容确定。
    }
}
```

- [ ] **Step 4: Create PreallocatingDataFileGateway**

Implement it as a concrete adapter that calls `strategy.preallocate(channel, offsetBytes, lengthBytes, path)` and then delegates to `ZeroFillDataFileGateway` so page contents remain deterministic. The no-arg `FileChannelPageStore` must still use `ZeroFillDataFileGateway` directly; this adapter exists for future platform-specific injection and tests.

- [ ] **Step 5: Modify DataFileHandle constructors and factories**

Add field:

```java
private final DataFileGateway gateway;
```

Add overloads while preserving existing package callers:

```java
static DataFileHandle create(SpaceId spaceId, Path path, PageSize pageSize, PageNo initialSizeInPages) {
    return create(spaceId, path, pageSize, initialSizeInPages, new ZeroFillDataFileGateway());
}

static DataFileHandle create(SpaceId spaceId, Path path, PageSize pageSize, PageNo initialSizeInPages,
                             DataFileGateway gateway) {
    validate(spaceId, path, pageSize);
    if (initialSizeInPages == null) {
        throw new DatabaseValidationException("initial size must not be null");
    }
    if (gateway == null) {
        throw new DatabaseValidationException("data file gateway must not be null");
    }
    if (Files.exists(path)) {
        throw new DataFilePhysicalException("data file already exists: " + path);
    }
    FileChannel channel = null;
    try {
        channel = FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.READ, StandardOpenOption.WRITE);
        long pages = initialSizeInPages.value();
        gateway.initialize(channel, 0, pages, pageSize, path);
        return new DataFileHandle(spaceId, path, pageSize, channel, pages, gateway);
    } catch (IOException e) {
        closeQuietly(channel);
        throw new DataFilePhysicalException("create data file failed: " + path, e);
    } catch (RuntimeException e) {
        closeQuietly(channel);
        throw e;
    }
}
```

Add the corresponding open overload:

```java
static DataFileHandle open(SpaceId spaceId, Path path, PageSize pageSize, DataFileGateway gateway) {
    validate(spaceId, path, pageSize);
    if (gateway == null) {
        throw new DatabaseValidationException("data file gateway must not be null");
    }
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
        return new DataFileHandle(spaceId, path, pageSize, channel, length / pageBytes, gateway);
    } catch (IOException e) {
        closeQuietly(channel);
        throw new DataFilePhysicalException("open data file failed: " + path, e);
    } catch (RuntimeException e) {
        closeQuietly(channel);
        throw e;
    }
}
```

- [ ] **Step 6: Replace zeroFill calls**

In `create`, replace:

```java
zeroFill(channel, 0, pages, pageSize.bytes());
```

with:

```java
gateway.initialize(channel, 0, pages, pageSize, path);
```

In `autoExtend`, replace zero fill with:

```java
gateway.ensureAllocated(channel, oldSize, oldSize + inc, pageSize, path);
```

In `ensureCapacity`, replace zero fill with:

```java
gateway.ensureAllocated(channel, current, target, pageSize, path);
```

Remove `zeroFill` from `DataFileHandle` after all callers move.

- [ ] **Step 7: Modify FileChannelPageStore constructors**

Add field:

```java
private final DataFileGateway dataFileGateway;
```

Wire constructors:

```java
public FileChannelPageStore() {
    this(new DefaultIbdAutoExtendPolicy(), new ZeroFillDataFileGateway());
}

public FileChannelPageStore(AutoExtendPolicy autoExtendPolicy) {
    this(autoExtendPolicy, new ZeroFillDataFileGateway());
}

FileChannelPageStore(AutoExtendPolicy autoExtendPolicy, DataFileGateway dataFileGateway) {
    if (autoExtendPolicy == null || dataFileGateway == null) {
        throw new DatabaseValidationException("page store dependencies must not be null");
    }
    this.autoExtendPolicy = autoExtendPolicy;
    this.dataFileGateway = dataFileGateway;
}
```

Update create/open to pass `dataFileGateway` into `DataFileHandle`.

- [ ] **Step 8: Run 0.16c tests**

Run:

```powershell
$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "*DataFileGatewayTest" --tests "*DataFileHandleGatewayTest" --tests "*FileChannelPageStoreGatewayTest"
```

Expected: PASS.

- [ ] **Step 9: Run fil.io regression**

Run:

```powershell
$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "cn.zhangyis.db.storage.fil.io.*"
```

Expected: PASS.

---

### Task 5: Update Design Status Docs

**Files:**
- Modify: `docs/design/current-implementation-map.md`
- Modify: `docs/design/storage-backlog.md`

**Interfaces:**
- Consumes: verified production code call chain.
- Produces: updated authoritative implementation map and backlog status.

- [ ] **Step 1: Update Disk Manager current data chains**

In `current-implementation-map.md`, update rows for create/open/recovery open:

```text
Create tablespace: GENERAL now writes page0 lifecycle NORMAL; UNDO keeps ACTIVE lifecycle.
Open tablespace: loader restores GENERAL persisted CORRUPTED and registry ordinary require rejects it.
Recovery open: requireForRecovery still allows CORRUPTED for recovery/diagnostics.
```

- [ ] **Step 2: Update package status**

Update `storage.fsp.lifecycle` note:

```text
page0 lifecycle marker now covers GENERAL NORMAL/CORRUPTED plus UNDO ACTIVE/INACTIVE/TRUNCATING truncate lifecycle.
```

Update `storage.fil.io` note:

```text
DataFileHandle delegates file range initialization/growth to DataFileGateway; default ZeroFillDataFileGateway preserves cross-platform zero-fill behavior; PreallocationStrategy seam exists but native strategies are future work.
```

- [ ] **Step 3: Update known gaps**

Replace the GENERAL runtime-only gap with:

```text
DISCARDED/DROP lifecycle remains runtime-only / unwired because full file lifecycle depends on DDL.
```

Replace the gateway missing gap with:

```text
Native/platform preallocation strategies remain future adapters; default gateway still zero-fills.
```

- [ ] **Step 4: Update storage backlog**

Mark 0.16b and 0.16c complete with date and short summary. Update the D disk-manager summary and recommended route only where directly affected.

- [ ] **Step 5: Review docs for misleading production edges**

Ensure no map line implies DD/DDL/session接线 was added. Keep DROP/DISCARD and discovery as future gaps.

---

### Task 6: Final Verification

**Files:**
- No code edits unless verification fails.

**Interfaces:**
- Consumes: all previous tasks.
- Produces: evidence that implementation is complete.

- [ ] **Step 1: Run targeted 0.16b/0.16c tests**

```powershell
$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "*DiskSpaceManagerGeneralLifecyclePersistenceTest" --tests "*TablespaceLifecycleRawCodecTest" --tests "*DataFileGatewayTest" --tests "*DataFileHandleGatewayTest" --tests "*FileChannelPageStoreGatewayTest"
```

Expected: PASS.

- [ ] **Step 2: Run full test suite**

```powershell
$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test
```

Expected: PASS.

- [ ] **Step 3: Scan for prohibited concurrency primitives**

Use Grep for:

```text
synchronized|wait\(|notify\(|notifyAll\(
```

Expected: no new production-code uses introduced by this work package.

- [ ] **Step 4: Scan for bare runtime exceptions in touched production files**

Use Grep on touched files for:

```text
new IllegalArgumentException|new RuntimeException
```

Expected: no bare exceptions introduced.

- [ ] **Step 5: Inspect git diff**

Run:

```powershell
git diff -- src/main/java src/test/java docs/design docs/superpowers
```

Expected: only intended files changed; no build output, IDE files, or unrelated edits.
