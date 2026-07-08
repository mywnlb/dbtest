# Recovery Force Skip Corrupt Tablespace Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement `RecoveryMode.FORCE_SKIP_CORRUPT_TABLESPACE` so crash recovery can explicitly skip configured corrupt data spaces while preserving fail-closed behavior for all non-skipped spaces.

**Architecture:** Add an immutable request-level `RecoverySkipPolicy`, propagate it through `RecoveryRequest`/`RecoveryReport`, and filter skipped spaces before every physical IO stage. Extend redo apply with a page-record filter that preserves original batch LSN boundaries, then wire `StorageEngine.recoverExisting` so skipped recovery tablespaces are not opened.

**Tech Stack:** Java 25, Gradle 9.5.1, JUnit Jupiter, project exception hierarchy, existing recovery/redo/fil APIs only.

## Global Constraints

- Read and follow `docs/superpowers/specs/2026-07-06-recovery-force-skip-corrupt-tablespace-design.md` before coding.
- Use `docs/design/innodb-storage-engine-overview.md`, `docs/design/innodb-crash-recovery-design.md`, `docs/design/innodb-flush-checkpoint-doublewrite-design.md`, `docs/design/innodb-disk-manager-design.md`, `docs/design/current-implementation-map.md`, and `docs/design/storage-backlog.md` as authority for recovery order and module boundaries.
- Do not split this work into separate repository specs; this plan implements the full force-skip design as one work package.
- Do not add SQL/session/DD dependencies to recovery, redo, fil, fsp, or engine internals.
- Do not add `synchronized`, `wait()`, `notify()`, or `notifyAll()`.
- Do not throw bare `IllegalArgumentException` or bare `RuntimeException` in production code.
- `FORCE_SKIP_CORRUPT_TABLESPACE` must require a non-empty explicit skipped `SpaceId` set.
- Skipped spaces must not be touched by `DiskSpaceManager.openTablespaceForRecovery`, `PageStore.readPage`, `PageStore.writePage`, `PageStore.ensureCapacity`, `PageStore.force`, or final `PageStore.forceAll`.
- Non-skipped spaces must keep existing fail-closed recovery behavior.
- Write failing tests before production changes.
- Use fixed Gradle first: `$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test`.
- Update `docs/design/current-implementation-map.md` and `docs/design/storage-backlog.md` after implementation.

---

## File Structure

- Create `src/main/java/cn/zhangyis/db/storage/recovery/RecoverySkipPolicy.java`: immutable skipped-space predicate used by all recovery stages.
- Modify `src/main/java/cn/zhangyis/db/storage/recovery/RecoveryRequest.java`: add `skipPolicy`, add `forceSkip(...)`, keep `normal(...)` and `readOnlyValidate(...)` skip-free.
- Modify `src/main/java/cn/zhangyis/db/storage/recovery/RecoveryReport.java`: add skipped-space diagnostics and static factories.
- Modify `src/main/java/cn/zhangyis/db/storage/recovery/RecoveryProgressJournal.java`: add completed-stage overload with diagnostic detail.
- Create `src/main/java/cn/zhangyis/db/storage/redo/RedoApplySummary.java`: immutable counts for scanned batches, applied batches, and skipped redo records.
- Create `src/main/java/cn/zhangyis/db/storage/redo/RedoApplyBatchView.java`: package-private view over original batch range plus filtered records.
- Modify `src/main/java/cn/zhangyis/db/storage/redo/RedoApplyDispatcher.java`: add `applyAll(..., Predicate<PageId>)` and page-record filtering.
- Modify `src/main/java/cn/zhangyis/db/storage/redo/PageRedoApplyHandler.java`: apply filtered batch views while stamping pages with original `LogRange.end()`.
- Modify `src/main/java/cn/zhangyis/db/storage/recovery/CrashRecoveryService.java`: remove reserved failure, apply skip policy to doublewrite, redo, reconcile, report, and journal.
- Modify `src/main/java/cn/zhangyis/db/storage/engine/EngineConfig.java`: add default-empty `forceSkippedSpaces` and `withForceSkippedSpaces(Set<SpaceId>)`.
- Modify `src/main/java/cn/zhangyis/db/storage/engine/StorageEngine.java`: validate force-skip config, open only non-skipped recovery tablespaces, build force-skip recovery request.
- Add tests under `src/test/java/cn/zhangyis/db/storage/recovery`, `src/test/java/cn/zhangyis/db/storage/redo`, and `src/test/java/cn/zhangyis/db/storage/engine`.

---

### Task 1: Add Recovery Skip Model, Request, Report, And Journal Detail

**Files:**
- Create: `src/main/java/cn/zhangyis/db/storage/recovery/RecoverySkipPolicy.java`
- Modify: `src/main/java/cn/zhangyis/db/storage/recovery/RecoveryRequest.java`
- Modify: `src/main/java/cn/zhangyis/db/storage/recovery/RecoveryReport.java`
- Modify: `src/main/java/cn/zhangyis/db/storage/recovery/RecoveryProgressJournal.java`
- Test: `src/test/java/cn/zhangyis/db/storage/recovery/RecoveryForceSkipModelTest.java`
- Test: `src/test/java/cn/zhangyis/db/storage/recovery/RecoveryProgressJournalTest.java`

**Interfaces:**
- Consumes: existing `RecoveryMode`, `RecoveryRequest.normal(...)`, `RecoveryRequest.readOnlyValidate(...)`, `RecoveryReport` constructor, `RecoveryProgressJournal.stageCompleted(...)`.
- Produces: `RecoverySkipPolicy.none()`, `RecoverySkipPolicy.of(Set<SpaceId>)`, `RecoverySkipPolicy.shouldSkip(SpaceId)`, `RecoverySkipPolicy.shouldSkip(PageId)`, `RecoveryRequest.forceSkip(...)`, `RecoveryRequest.skipPolicy()`, `RecoveryReport.forceSkip(...)`, `RecoveryReport.skippedSpaces()`, `RecoveryProgressJournal.stageCompleted(..., String detail)`.

- [ ] **Step 1: Write red tests for skip policy and request validation**

Create `RecoveryForceSkipModelTest` with the core assertions:

```java
package cn.zhangyis.db.storage.recovery;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.fil.io.FileChannelPageStore;
import cn.zhangyis.db.storage.redo.RedoApplyContext;
import cn.zhangyis.db.storage.redo.RedoApplyDispatcher;
import cn.zhangyis.db.storage.redo.RedoCheckpointStore;
import cn.zhangyis.db.storage.redo.RedoLogFileRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RecoveryForceSkipModelTest {
    private static final PageSize PS = PageSize.ofBytes(16 * 1024);

    @TempDir
    Path dir;

    @Test
    void skipPolicyMatchesBySpaceAndPage() {
        RecoverySkipPolicy policy = RecoverySkipPolicy.of(Set.of(SpaceId.of(7)));

        assertTrue(policy.shouldSkip(SpaceId.of(7)));
        assertTrue(policy.shouldSkip(PageId.of(SpaceId.of(7), PageNo.of(3))));
        assertFalse(policy.shouldSkip(SpaceId.of(8)));
        assertEquals(Set.of(SpaceId.of(7)), policy.skippedSpaces());
    }

    @Test
    void forceSkipRequestRequiresNonEmptySkippedSpaces() throws Exception {
        try (FileChannelPageStore store = new FileChannelPageStore();
             RedoLogFileRepository redoRepo = RedoLogFileRepository.open(dir.resolve("redo.log"));
             RedoCheckpointStore checkpointStore = RedoCheckpointStore.open(dir.resolve("redo-control"))) {
            RedoApplyContext context = new RedoApplyContext(store, PS);

            assertThrows(DatabaseValidationException.class, () ->
                    RecoveryRequest.forceSkip(checkpointStore, redoRepo,
                            RedoApplyDispatcher.pageDispatcher(), context, Set.of()));
        }
    }

    @Test
    void forceSkipRequestCarriesImmutablePolicy() throws Exception {
        try (FileChannelPageStore store = new FileChannelPageStore();
             RedoLogFileRepository redoRepo = RedoLogFileRepository.open(dir.resolve("redo.log"));
             RedoCheckpointStore checkpointStore = RedoCheckpointStore.open(dir.resolve("redo-control"))) {
            RecoveryRequest request = RecoveryRequest.forceSkip(checkpointStore, redoRepo,
                    RedoApplyDispatcher.pageDispatcher(), new RedoApplyContext(store, PS),
                    Set.of(SpaceId.of(11)));

            assertEquals(RecoveryMode.FORCE_SKIP_CORRUPT_TABLESPACE, request.mode());
            assertTrue(request.skipPolicy().shouldSkip(SpaceId.of(11)));
            assertThrows(UnsupportedOperationException.class,
                    () -> request.skipPolicy().skippedSpaces().add(SpaceId.of(12)));
        }
    }

    @Test
    void normalAndReadOnlyRequestsDoNotCarrySkipPolicy() throws Exception {
        try (FileChannelPageStore store = new FileChannelPageStore();
             RedoLogFileRepository redoRepo = RedoLogFileRepository.open(dir.resolve("redo.log"));
             RedoCheckpointStore checkpointStore = RedoCheckpointStore.open(dir.resolve("redo-control"))) {
            RedoApplyContext context = new RedoApplyContext(store, PS);

            assertTrue(RecoveryRequest.normal(checkpointStore, redoRepo,
                    RedoApplyDispatcher.pageDispatcher(), context).skipPolicy().isEmpty());
            assertTrue(RecoveryRequest.readOnlyValidate(checkpointStore, redoRepo,
                    RedoApplyDispatcher.pageDispatcher(), context).skipPolicy().isEmpty());
        }
    }
}
```

- [ ] **Step 2: Write red tests for report and progress detail**

Extend `RecoveryForceSkipModelTest`:

```java
@Test
void forceSkipReportCapturesSkippedDiagnostics() {
    RecoveryReport report = RecoveryReport.forceSkip(RecoveryState.OPEN,
            Lsn.of(1), Lsn.of(9), 2, 1, 3,
            Set.of(SpaceId.of(7)), 4, 5, 6,
            List.of(RecoveryStageName.TRAFFIC_CLOSED, RecoveryStageName.OPEN_TRAFFIC));

    assertEquals(Set.of(SpaceId.of(7)), report.skippedSpaces());
    assertEquals(4, report.skippedDoublewritePageCount());
    assertEquals(5, report.skippedRedoRecordCount());
    assertEquals(6, report.skippedReconcileSpaceCount());
}
```

Add imports for `Lsn`, `List`, and `RecoveryStageName`.

Extend `RecoveryProgressJournalTest`:

```java
@Test
void persistentJournalWritesCompletedDetail() throws Exception {
    Path path = dir.resolve("force-skip-progress.jsonl");
    RecoveryProgressJournal journal = RecoveryProgressJournal.persistent(path);

    journal.stageCompleted(RecoveryMode.FORCE_SKIP_CORRUPT_TABLESPACE,
            RecoveryStageName.OPEN_TRAFFIC, RecoveryState.OPEN, Lsn.of(77),
            "skippedSpaces=[7], skippedRedoRecords=3");

    String line = Files.readString(path);
    assertTrue(line.contains("\"kind\":\"COMPLETED\""));
    assertTrue(line.contains("\"mode\":\"FORCE_SKIP_CORRUPT_TABLESPACE\""));
    assertTrue(line.contains("skippedSpaces=[7]"));
    assertTrue(line.contains("skippedRedoRecords=3"));
}
```

- [ ] **Step 3: Run model red tests**

Run:

```powershell
$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "*RecoveryForceSkipModelTest" --tests "*RecoveryProgressJournalTest"
```

Expected: compile failures because `RecoverySkipPolicy`, `RecoveryRequest.forceSkip`, report diagnostics, and journal detail overload do not exist.

- [ ] **Step 4: Create RecoverySkipPolicy**

Create `RecoverySkipPolicy.java`:

```java
package cn.zhangyis.db.storage.recovery;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.SpaceId;

import java.util.Comparator;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * force-skip 恢复策略。它只表达管理员显式声明的损坏表空间集合，
 * recovery 各阶段必须在进入物理 IO 前用该谓词过滤 page/space。
 *
 * @param skippedSpaces 不可变表空间集合；为空表示普通恢复路径不跳过任何空间。
 */
public record RecoverySkipPolicy(Set<SpaceId> skippedSpaces) {

    public RecoverySkipPolicy {
        if (skippedSpaces == null) {
            throw new DatabaseValidationException("recovery skipped spaces must not be null");
        }
        for (SpaceId spaceId : skippedSpaces) {
            if (spaceId == null) {
                throw new DatabaseValidationException("recovery skipped space must not be null");
            }
        }
        skippedSpaces = Set.copyOf(skippedSpaces);
    }

    /** 创建不跳过任何空间的策略，供 NORMAL 和 READ_ONLY_VALIDATE 使用。 */
    public static RecoverySkipPolicy none() {
        return new RecoverySkipPolicy(Set.of());
    }

    /** 创建显式跳过策略；调用方负责根据 recovery mode 判断是否允许非空集合。 */
    public static RecoverySkipPolicy of(Set<SpaceId> skippedSpaces) {
        return new RecoverySkipPolicy(skippedSpaces);
    }

    /** 策略是否为空，便于普通恢复路径走无过滤分支。 */
    public boolean isEmpty() {
        return skippedSpaces.isEmpty();
    }

    /** 判断一个表空间是否应被跳过；传入空值说明调用方恢复输入错误，直接拒绝。 */
    public boolean shouldSkip(SpaceId spaceId) {
        if (spaceId == null) {
            throw new DatabaseValidationException("space id for recovery skip check must not be null");
        }
        return skippedSpaces.contains(spaceId);
    }

    /** 判断物理页所属空间是否应被跳过。 */
    public boolean shouldSkip(PageId pageId) {
        if (pageId == null) {
            throw new DatabaseValidationException("page id for recovery skip check must not be null");
        }
        return shouldSkip(pageId.spaceId());
    }

    /** 生成稳定诊断文本，避免 journal 中 Set 迭代顺序导致测试和排障不稳定。 */
    public String describeSkippedSpaces() {
        return skippedSpaces.stream()
                .sorted(Comparator.comparingInt(SpaceId::value))
                .map(spaceId -> Integer.toString(spaceId.value()))
                .collect(Collectors.joining(", ", "[", "]"));
    }
}
```

- [ ] **Step 5: Modify RecoveryRequest**

Add `RecoverySkipPolicy skipPolicy` as the final record component, update Javadoc, and update compact constructor:

```java
if (skipPolicy == null) {
    skipPolicy = RecoverySkipPolicy.none();
}
if (mode != RecoveryMode.FORCE_SKIP_CORRUPT_TABLESPACE && !skipPolicy.isEmpty()) {
    throw new DatabaseValidationException("skip policy is only allowed in FORCE_SKIP_CORRUPT_TABLESPACE mode");
}
if (mode == RecoveryMode.FORCE_SKIP_CORRUPT_TABLESPACE && skipPolicy.isEmpty()) {
    throw new DatabaseValidationException("FORCE_SKIP_CORRUPT_TABLESPACE requires skipped spaces");
}
```

Update every existing constructor call in the factory and `with...` methods to pass the current policy:

```java
return new RecoveryRequest(mode, checkpointStore, redoRepository, dispatcher, applyContext,
        scanner, pages, undoTablespaceRecovery, spacesToReconcile,
        recoveredRedoManager, transactionUndoRecovery, skipPolicy);
```

Add factory:

```java
/**
 * 创建 FORCE_SKIP_CORRUPT_TABLESPACE 请求。该模式必须由调用方显式传入非空 skippedSpaces；
 * 具体哪些系统空间不可跳过由 StorageEngine 结合实例配置校验。
 */
public static RecoveryRequest forceSkip(RedoCheckpointStore checkpointStore,
                                        RedoLogFileRepository redoRepository,
                                        RedoApplyDispatcher dispatcher,
                                        RedoApplyContext applyContext,
                                        Set<SpaceId> skippedSpaces) {
    return new RecoveryRequest(RecoveryMode.FORCE_SKIP_CORRUPT_TABLESPACE,
            checkpointStore, redoRepository, dispatcher, applyContext,
            null, List.of(), null, List.of(), null, null,
            RecoverySkipPolicy.of(skippedSpaces));
}
```

`normal(...)` and `readOnlyValidate(...)` must pass `RecoverySkipPolicy.none()`.

- [ ] **Step 6: Modify RecoveryReport**

Add record components:

```java
Set<SpaceId> skippedSpaces,
int skippedDoublewritePageCount,
int skippedRedoRecordCount,
int skippedReconcileSpaceCount
```

Update constructor validation:

```java
if (skippedSpaces == null) {
    throw new DatabaseValidationException("recovery report skipped spaces must not be null");
}
if (skippedDoublewritePageCount < 0 || skippedRedoRecordCount < 0 || skippedReconcileSpaceCount < 0) {
    throw new DatabaseValidationException("recovery skipped counts must not be negative");
}
skippedSpaces = Set.copyOf(skippedSpaces);
```

Add factories and use them in later tasks:

```java
public static RecoveryReport normal(RecoveryState state, Lsn checkpointLsn, Lsn recoveredToLsn,
                                    int repairedPageCount, int detectedOnlyPageCount,
                                    int appliedBatchCount, List<RecoveryStageName> completedStages) {
    return new RecoveryReport(RecoveryMode.NORMAL, state, checkpointLsn, recoveredToLsn,
            repairedPageCount, detectedOnlyPageCount, appliedBatchCount, completedStages,
            Set.of(), 0, 0, 0);
}

public static RecoveryReport readOnlyValidate(RecoveryState state, Lsn checkpointLsn, Lsn recoveredToLsn,
                                              int detectedOnlyPageCount,
                                              List<RecoveryStageName> completedStages) {
    return new RecoveryReport(RecoveryMode.READ_ONLY_VALIDATE, state, checkpointLsn, recoveredToLsn,
            0, detectedOnlyPageCount, 0, completedStages, Set.of(), 0, 0, 0);
}

public static RecoveryReport forceSkip(RecoveryState state, Lsn checkpointLsn, Lsn recoveredToLsn,
                                       int repairedPageCount, int detectedOnlyPageCount,
                                       int appliedBatchCount, Set<SpaceId> skippedSpaces,
                                       int skippedDoublewritePageCount, int skippedRedoRecordCount,
                                       int skippedReconcileSpaceCount,
                                       List<RecoveryStageName> completedStages) {
    return new RecoveryReport(RecoveryMode.FORCE_SKIP_CORRUPT_TABLESPACE, state, checkpointLsn, recoveredToLsn,
            repairedPageCount, detectedOnlyPageCount, appliedBatchCount, completedStages,
            skippedSpaces, skippedDoublewritePageCount, skippedRedoRecordCount, skippedReconcileSpaceCount);
}

public static RecoveryReport failed(RecoveryMode mode, Set<SpaceId> skippedSpaces) {
    return new RecoveryReport(mode, RecoveryState.FAILED, Lsn.of(0), Lsn.of(0),
            0, 0, 0, List.of(), skippedSpaces == null ? Set.of() : skippedSpaces, 0, 0, 0);
}
```

- [ ] **Step 7: Add journal completed detail overload**

In `RecoveryProgressJournal`, keep the existing method and delegate:

```java
public void stageCompleted(RecoveryMode mode, RecoveryStageName stageName,
                           RecoveryState state, Lsn recoveredToLsn) {
    stageCompleted(mode, stageName, state, recoveredToLsn, "");
}

public void stageCompleted(RecoveryMode mode, RecoveryStageName stageName,
                           RecoveryState state, Lsn recoveredToLsn, String detail) {
    append(new RecoveryProgressEvent(nextSequence(), mode, stageName,
            RecoveryProgressEventKind.COMPLETED, state, recoveredToLsn,
            detail == null ? "" : detail));
}
```

If the class currently builds events through a private helper, add the `detail` parameter to that helper instead of duplicating JSON writing logic.

- [ ] **Step 8: Run Task 1 tests**

Run:

```powershell
$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "*RecoveryForceSkipModelTest" --tests "*RecoveryProgressJournalTest"
```

Expected: PASS.

---

### Task 2: Add Redo Apply Filtering For Skipped Page Records

**Files:**
- Create: `src/main/java/cn/zhangyis/db/storage/redo/RedoApplySummary.java`
- Create: `src/main/java/cn/zhangyis/db/storage/redo/RedoApplyBatchView.java`
- Modify: `src/main/java/cn/zhangyis/db/storage/redo/RedoApplyDispatcher.java`
- Modify: `src/main/java/cn/zhangyis/db/storage/redo/PageRedoApplyHandler.java`
- Test: `src/test/java/cn/zhangyis/db/storage/redo/RedoApplyDispatcherSkipTest.java`

**Interfaces:**
- Consumes: `RedoLogBatch`, `RedoRecord`, `PageInitRecord.pageId()`, `PageBytesRecord.pageId()`, `PageRedoApplyHandler.apply(RedoLogBatch, RedoApplyContext)`.
- Produces: `RedoApplySummary`, `RedoApplyDispatcher.applyAll(List<RedoLogBatch>, RedoApplyContext, Predicate<PageId>)`, package-private `RedoApplyBatchView`.

- [ ] **Step 1: Write red test for mixed skipped and applied records**

Create `RedoApplyDispatcherSkipTest`:

```java
package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.fil.io.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.flush.doublewrite.DoublewriteFileRepository;
import cn.zhangyis.db.storage.flush.doublewrite.DoublewriteRecoveryScanner;
import cn.zhangyis.db.storage.page.PageEnvelopeLayout;
import cn.zhangyis.db.storage.page.PageType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class RedoApplyDispatcherSkipTest {
    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId NORMAL_SPACE = SpaceId.of(1);
    private static final SpaceId SKIPPED_SPACE = SpaceId.of(2);
    private static final PageId NORMAL_PAGE = PageId.of(NORMAL_SPACE, PageNo.of(3));
    private static final PageId SKIPPED_PAGE = PageId.of(SKIPPED_SPACE, PageNo.of(3));
    private static final int OFFSET = PageEnvelopeLayout.FIL_PAGE_HEADER_BYTES + 96;

    @TempDir
    Path dir;

    @Test
    void applyAllSkipsRecordsBeforePageHandlerAndPreservesOriginalBatchEndLsn() {
        try (PageStore store = new FileChannelPageStore()) {
            store.create(NORMAL_SPACE, dir.resolve("normal.ibd"), PS, PageNo.of(8));
            store.create(SKIPPED_SPACE, dir.resolve("skipped.ibd"), PS, PageNo.of(8));
            RedoApplyContext context = new RedoApplyContext(store, PS);
            List<RedoRecord> records = List.of(
                    new PageInitRecord(SKIPPED_PAGE, PageType.INDEX),
                    new PageInitRecord(NORMAL_PAGE, PageType.INDEX),
                    new PageBytesRecord(SKIPPED_PAGE, OFFSET, new byte[]{9, 9}),
                    new PageBytesRecord(NORMAL_PAGE, OFFSET, new byte[]{4, 5}));
            int bytes = records.stream().mapToInt(RedoRecord::byteLength).sum();
            LogRange range = new LogRange(Lsn.of(10), Lsn.of(10 + bytes));
            RedoLogBatch batch = new RedoLogBatch(range, records);

            RedoApplySummary summary = RedoApplyDispatcher.pageDispatcher()
                    .applyAll(List.of(batch), context, pageId -> pageId.spaceId().equals(SKIPPED_SPACE));

            byte[] normal = read(store, NORMAL_PAGE);
            byte[] skipped = read(store, SKIPPED_PAGE);
            assertArrayEquals(new byte[]{4, 5}, new byte[]{normal[OFFSET], normal[OFFSET + 1]});
            assertEquals(range.end().value(), ByteBuffer.wrap(normal).getLong(PageEnvelopeLayout.PAGE_LSN));
            assertEquals(0, ByteBuffer.wrap(skipped).getInt(PageEnvelopeLayout.PAGE_TYPE));
            assertEquals(1, summary.scannedBatchCount());
            assertEquals(1, summary.appliedBatchCount());
            assertEquals(2, summary.skippedRecordCount());
        }
    }

    private static byte[] read(PageStore store, PageId pageId) {
        byte[] page = new byte[PS.bytes()];
        store.readPage(pageId, ByteBuffer.wrap(page));
        return page;
    }
}
```

- [ ] **Step 2: Write red test for fully skipped batch**

Add to the same class:

```java
@Test
void fullySkippedBatchDoesNotStampOrApplyAnyPage() {
    try (PageStore store = new FileChannelPageStore()) {
        store.create(SKIPPED_SPACE, dir.resolve("skipped-only.ibd"), PS, PageNo.of(8));
        RedoApplyContext context = new RedoApplyContext(store, PS);
        List<RedoRecord> records = List.of(new PageInitRecord(SKIPPED_PAGE, PageType.INDEX));
        LogRange range = new LogRange(Lsn.of(30), Lsn.of(30 + records.get(0).byteLength()));
        RedoLogBatch batch = new RedoLogBatch(range, records);

        RedoApplySummary summary = RedoApplyDispatcher.pageDispatcher()
                .applyAll(List.of(batch), context, pageId -> pageId.spaceId().equals(SKIPPED_SPACE));

        byte[] skipped = read(store, SKIPPED_PAGE);
        assertEquals(0, ByteBuffer.wrap(skipped).getInt(PageEnvelopeLayout.PAGE_TYPE));
        assertEquals(1, summary.scannedBatchCount());
        assertEquals(0, summary.appliedBatchCount());
        assertEquals(1, summary.skippedRecordCount());
    }
}
```

- [ ] **Step 3: Run redo red tests**

Run:

```powershell
$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "*RedoApplyDispatcherSkipTest"
```

Expected: compile failures because `RedoApplySummary` and filtered `applyAll` do not exist.

- [ ] **Step 4: Create RedoApplySummary**

```java
package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * redo apply 统计。force-skip 使用它区分“扫描到的批次”和“真正包含可应用记录的批次”，
 * 既保留现有 report 的批次数语义，又能记录被跳过的 page records。
 *
 * @param scannedBatchCount redo reader 交给 dispatcher 的批次数。
 * @param appliedBatchCount 至少包含一条未跳过记录并进入 page handler 的批次数。
 * @param skippedRecordCount 因 force-skip 策略被过滤的 redo record 数。
 */
public record RedoApplySummary(int scannedBatchCount, int appliedBatchCount, int skippedRecordCount) {
    public RedoApplySummary {
        if (scannedBatchCount < 0 || appliedBatchCount < 0 || skippedRecordCount < 0) {
            throw new DatabaseValidationException("redo apply summary counts must not be negative");
        }
        if (appliedBatchCount > scannedBatchCount) {
            throw new DatabaseValidationException("applied redo batches must not exceed scanned batches");
        }
    }
}
```

- [ ] **Step 5: Create RedoApplyBatchView**

```java
package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.List;

/**
 * 过滤后的 redo 批次视图。records 可以是原批次子集，但 range 必须保持原始批次范围，
 * 因为 page handler 需要用原始 end LSN stamp pageLSN，不能伪造新的 RedoLogBatch。
 */
record RedoApplyBatchView(LogRange range, List<RedoRecord> records) {
    RedoApplyBatchView {
        if (range == null || records == null) {
            throw new DatabaseValidationException("redo apply batch view range/records must not be null");
        }
        records = List.copyOf(records);
    }
}
```

- [ ] **Step 6: Modify PageRedoApplyHandler**

Keep the existing public/package method and delegate:

```java
void apply(RedoLogBatch batch, RedoApplyContext context) {
    if (batch == null) {
        throw new DatabaseValidationException("redo apply batch must not be null");
    }
    apply(new RedoApplyBatchView(batch.range(), batch.records()), context);
}

void apply(RedoApplyBatchView batch, RedoApplyContext context) {
    if (batch == null || context == null) {
        throw new DatabaseValidationException("redo apply batch/context must not be null");
    }
    // Move the existing record loop here and replace batch.records()/batch.range()
    // with the view accessors. Stamp pageLSN with batch.range().end().
}
```

When moving the loop, preserve the existing page initialization, byte patching, checksum, and `PAGE_LSN` behavior. The only semantic change is that `batch.records()` may be a filtered subset while `batch.range().end()` remains the original LSN boundary.

- [ ] **Step 7: Modify RedoApplyDispatcher**

Add imports for `PageId`, `ArrayList`, and `Predicate`. Keep existing `applyAll(List, context)` as a void compatibility method:

```java
public void applyAll(List<RedoLogBatch> batches, RedoApplyContext context) {
    applyAll(batches, context, pageId -> false);
}

public RedoApplySummary applyAll(List<RedoLogBatch> batches, RedoApplyContext context,
                                 Predicate<PageId> skipPage) {
    if (batches == null || context == null || skipPage == null) {
        throw new DatabaseValidationException("redo apply batches/context/skip predicate must not be null");
    }
    int applied = 0;
    int skipped = 0;
    for (RedoLogBatch batch : batches) {
        List<RedoRecord> recordsToApply = new ArrayList<>(batch.records().size());
        for (RedoRecord record : batch.records()) {
            PageId pageId = pageIdOf(record);
            if (skipPage.test(pageId)) {
                skipped++;
            } else {
                recordsToApply.add(record);
            }
        }
        if (!recordsToApply.isEmpty()) {
            pageHandler.apply(new RedoApplyBatchView(batch.range(), recordsToApply), context);
            applied++;
        }
    }
    return new RedoApplySummary(batches.size(), applied, skipped);
}

private static PageId pageIdOf(RedoRecord record) {
    if (record instanceof PageInitRecord pageInit) {
        return pageInit.pageId();
    }
    if (record instanceof PageBytesRecord pageBytes) {
        return pageBytes.pageId();
    }
    throw new DatabaseValidationException("unsupported redo record without page id: " + record.getClass().getName());
}
```

The final `throw` is defensive for future sealed-interface expansion. Current permitted record types are page-scoped.

- [ ] **Step 8: Run redo tests**

Run:

```powershell
$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "*RedoApplyDispatcherSkipTest" --tests "*PageRedoApplyExtendOnDemandTest"
```

Expected: PASS.

---

### Task 3: Wire Force-Skip Through CrashRecoveryService Stages

**Files:**
- Modify: `src/main/java/cn/zhangyis/db/storage/recovery/CrashRecoveryService.java`
- Test: `src/test/java/cn/zhangyis/db/storage/recovery/CrashRecoveryForceSkipTest.java`

**Interfaces:**
- Consumes: `RecoveryRequest.skipPolicy()`, `RedoApplySummary`, `RecoveryReport.forceSkip(...)`, `RecoveryProgressJournal.stageCompleted(..., detail)`.
- Produces: force-skip doublewrite filtering, redo filtering, reconcile filtering, skipped counts, final force-skip report.

- [ ] **Step 1: Write red integration test for doublewrite, redo, reconcile, and report**

Create `CrashRecoveryForceSkipTest` using the existing `CrashRecoveryServiceTest` style:

```java
package cn.zhangyis.db.storage.recovery;

import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.fil.io.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.flush.doublewrite.DoublewriteRecoveryScanner;
import cn.zhangyis.db.storage.page.PageEnvelopeLayout;
import cn.zhangyis.db.storage.page.PageType;
import cn.zhangyis.db.storage.redo.LogRange;
import cn.zhangyis.db.storage.redo.PageBytesRecord;
import cn.zhangyis.db.storage.redo.PageInitRecord;
import cn.zhangyis.db.storage.redo.RedoApplyContext;
import cn.zhangyis.db.storage.redo.RedoApplyDispatcher;
import cn.zhangyis.db.storage.redo.RedoCheckpointLabel;
import cn.zhangyis.db.storage.redo.RedoCheckpointStore;
import cn.zhangyis.db.storage.redo.RedoLogFileRepository;
import cn.zhangyis.db.storage.redo.RedoLogManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CrashRecoveryForceSkipTest {
    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId NORMAL_SPACE = SpaceId.of(1);
    private static final SpaceId SKIPPED_SPACE = SpaceId.of(2);
    private static final PageId NORMAL_PAGE = PageId.of(NORMAL_SPACE, PageNo.of(3));
    private static final PageId SKIPPED_PAGE = PageId.of(SKIPPED_SPACE, PageNo.of(3));
    private static final int OFFSET = PageEnvelopeLayout.FIL_PAGE_HEADER_BYTES + 128;

    @TempDir
    Path dir;

    @Test
    void forceSkipFiltersEveryIoStageAndReportsSkippedCounts() throws Exception {
        Path redoPath = dir.resolve("redo.log");
        Path controlPath = dir.resolve("redo-control");
        LogRange range;
        try (RedoLogFileRepository redoRepo = RedoLogFileRepository.open(redoPath)) {
            RedoLogManager redo = RedoLogManager.durable(redoRepo);
            range = redo.append(List.of(
                    new PageInitRecord(SKIPPED_PAGE, PageType.INDEX),
                    new PageInitRecord(NORMAL_PAGE, PageType.INDEX),
                    new PageBytesRecord(SKIPPED_PAGE, OFFSET, new byte[]{8, 8}),
                    new PageBytesRecord(NORMAL_PAGE, OFFSET, new byte[]{1, 2})));
            redo.flush();
        }
        try (RedoCheckpointStore checkpointStore = RedoCheckpointStore.open(controlPath)) {
            checkpointStore.write(RedoCheckpointLabel.of(Lsn.of(0), range.end(), 1));
        }

        try (PageStore store = new FileChannelPageStore();
             DoublewriteFileRepository doublewriteRepo =
                     DoublewriteFileRepository.open(dir.resolve("doublewrite.dwb"), PS);
             RedoLogFileRepository redoRepo = RedoLogFileRepository.open(redoPath);
             RedoCheckpointStore checkpointStore = RedoCheckpointStore.open(controlPath)) {
            store.create(NORMAL_SPACE, dir.resolve("normal.ibd"), PS, PageNo.of(8));
            Path journalPath = dir.resolve("progress.jsonl");
            RecoveryProgressJournal journal = RecoveryProgressJournal.persistent(journalPath);
            DoublewriteRecoveryScanner scanner = new DoublewriteRecoveryScanner(doublewriteRepo, store, PS);
            RecoveryRequest request = RecoveryRequest.forceSkip(checkpointStore, redoRepo,
                            RedoApplyDispatcher.pageDispatcher(), new RedoApplyContext(store, PS),
                            Set.of(SKIPPED_SPACE))
                    .withDoublewriteRepair(scanner, List.of(SKIPPED_PAGE))
                    .withSpaceFileReconcile(List.of(NORMAL_SPACE, SKIPPED_SPACE));

            RecoveryReport report = new CrashRecoveryService(new RecoveryTrafficGate(), journal).recover(request);

            byte[] normal = read(store, NORMAL_PAGE);
            assertArrayEquals(new byte[]{1, 2}, new byte[]{normal[OFFSET], normal[OFFSET + 1]});
            assertEquals(range.end().value(), ByteBuffer.wrap(normal).getLong(PageEnvelopeLayout.PAGE_LSN));
            assertEquals(RecoveryMode.FORCE_SKIP_CORRUPT_TABLESPACE, report.mode());
            assertEquals(Set.of(SKIPPED_SPACE), report.skippedSpaces());
            assertEquals(1, report.skippedDoublewritePageCount());
            assertEquals(2, report.skippedRedoRecordCount());
            assertEquals(1, report.skippedReconcileSpaceCount());
            assertEquals(1, report.appliedBatchCount());
            String persisted = Files.readString(journalPath);
            assertTrue(persisted.contains("skippedSpaces=[2]"));
            assertTrue(persisted.contains("skippedRedoRecords=2"));
            assertFalse(persisted.contains("reserved and not implemented"));
        }
    }

    private static byte[] read(PageStore store, PageId pageId) {
        byte[] page = new byte[PS.bytes()];
        store.readPage(pageId, ByteBuffer.wrap(page));
        return page;
    }
}
```

The test intentionally does not create `SKIPPED_SPACE` in `PageStore`. If force-skip misses any doublewrite, redo, reconcile, or final-force path, recovery will touch a missing tablespace handle and fail.

- [ ] **Step 2: Write red test for non-skipped corruption still fail-closed**

Add a test that passes `FORCE_SKIP_CORRUPT_TABLESPACE` with `Set.of(SKIPPED_SPACE)` but corrupts page0 for `NORMAL_SPACE` before `SPACE_FILE_RECONCILE`:

```java
@Test
void forceSkipDoesNotHideNonSkippedReconcileCorruption() throws Exception {
    Path redoPath = dir.resolve("bad-page0-redo.log");
    Path controlPath = dir.resolve("bad-page0-control");
    try (RedoLogFileRepository redoRepo = RedoLogFileRepository.open(redoPath);
         RedoCheckpointStore checkpointStore = RedoCheckpointStore.open(controlPath);
         PageStore store = new FileChannelPageStore()) {
        store.create(NORMAL_SPACE, dir.resolve("bad-page0-normal.ibd"), PS, PageNo.of(8));
        byte[] page0 = read(store, PageId.of(NORMAL_SPACE, PageNo.of(0)));
        ByteBuffer.wrap(page0).putInt(PageEnvelopeLayout.SPACE_ID, SKIPPED_SPACE.value());
        store.writePage(PageId.of(NORMAL_SPACE, PageNo.of(0)), ByteBuffer.wrap(page0));
        store.force(NORMAL_SPACE);

        RecoveryTrafficGate gate = new RecoveryTrafficGate();
        RecoveryRequest request = RecoveryRequest.forceSkip(checkpointStore, redoRepo,
                        RedoApplyDispatcher.pageDispatcher(), new RedoApplyContext(store, PS),
                        Set.of(SKIPPED_SPACE))
                .withSpaceFileReconcile(List.of(NORMAL_SPACE));

        assertThrows(RecoveryStartupException.class,
                () -> new CrashRecoveryService(gate).recover(request));
        assertEquals(RecoveryState.FAILED, gate.state());
    }
}
```

Use the existing corruption pattern from `SpaceFileReconcileRecoveryTest`: write page0 bytes through `store.writePage(PageId.of(NORMAL_SPACE, PageNo.of(0)), ByteBuffer.wrap(page0))`, then call recovery. The assertion must prove the non-skipped space still fails closed.

- [ ] **Step 3: Run recovery red tests**

Run:

```powershell
$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "*CrashRecoveryForceSkipTest"
```

Expected: compile failure or runtime failure because `CrashRecoveryService` still rejects force-skip and does not filter stages.

- [ ] **Step 4: Add skipped counts to CrashRecoveryService local summaries**

Change the private summary records:

```java
private record DoublewriteRepairSummary(int repairedPageCount, int detectedOnlyPageCount,
                                        int skippedPageCount) {
}

private record SpaceFileReconcileSummary(int skippedSpaceCount) {
}

private record ForceSkipDiagnostics(int skippedDoublewritePages,
                                    int skippedRedoRecords,
                                    int skippedReconcileSpaces) {
    private String describe(RecoverySkipPolicy policy) {
        return "skippedSpaces=" + policy.describeSkippedSpaces()
                + ", skippedDoublewritePages=" + skippedDoublewritePages
                + ", skippedRedoRecords=" + skippedRedoRecords
                + ", skippedReconcileSpaces=" + skippedReconcileSpaces;
    }
}
```

Update all existing `new DoublewriteRepairSummary(...)` call sites to pass `0` for the skipped count in normal and read-only paths.

- [ ] **Step 5: Remove reserved force-skip rejection and select redo path**

Delete this block:

```java
if (request.mode() == RecoveryMode.FORCE_SKIP_CORRUPT_TABLESPACE) {
    throw new DatabaseValidationException(
            "FORCE_SKIP_CORRUPT_TABLESPACE is reserved and not implemented");
}
```

Replace redo apply in `recover(...)`:

```java
RedoApplySummary redoSummary;
if (request.skipPolicy().isEmpty()) {
    request.dispatcher().applyAll(batches, request.applyContext());
    redoSummary = new RedoApplySummary(batches.size(), batches.size(), 0);
} else {
    redoSummary = request.dispatcher().applyAll(batches, request.applyContext(),
            request.skipPolicy()::shouldSkip);
}
```

Preserve `reader.recoveredToLsn()` and redo boundary installation even when some records are skipped.

- [ ] **Step 6: Filter doublewrite before scanner IO**

Modify `repairDoublewritePages`:

```java
for (PageId pageId : request.pagesToRepair()) {
    if (request.skipPolicy().shouldSkip(pageId)) {
        skipped++;
        continue;
    }
    if (request.undoTablespaceRecovery() != null
            && !request.undoTablespaceRecovery().shouldRepairDoublewritePage(pageId)) {
        continue;
    }
    DoublewriteRecoveryResult result = request.doublewriteScanner().scanPageIfNeeded(pageId);
    ...
}
return new DoublewriteRepairSummary(repaired, detectedOnly, skipped);
```

Keep undo participant `prepareDoublewrite(...)` before the loop because Task 4 prevents skipping the system undo space at engine composition time.

- [ ] **Step 7: Filter reconcile before reading page0**

Change `reconcileSpaceFiles` to return `SpaceFileReconcileSummary`:

```java
private SpaceFileReconcileSummary reconcileSpaceFiles(RecoveryRequest request) {
    int skipped = 0;
    PageStore pageStore = request.applyContext().pageStore();
    PageSize pageSize = request.applyContext().pageSize();
    for (SpaceId spaceId : request.spacesToReconcile()) {
        if (request.skipPolicy().shouldSkip(spaceId)) {
            skipped++;
            continue;
        }
        byte[] page0 = new byte[pageSize.bytes()];
        pageStore.readPage(PageId.of(spaceId, PageNo.of(0)), ByteBuffer.wrap(page0));
        SpaceHeaderPhysical header = SpaceHeaderRawCodec.readPhysical(ByteBuffer.wrap(page0));
        validateReconcileHeader(spaceId, pageSize, header);
        pageStore.ensureCapacity(spaceId, header.currentSizeInPages());
    }
    return new SpaceFileReconcileSummary(skipped);
}
```

In `recover(...)`, initialize `SpaceFileReconcileSummary reconcileSummary = new SpaceFileReconcileSummary(0);` before the optional stage and assign it when the stage runs.

- [ ] **Step 8: Build force-skip report and final journal detail**

At report construction:

```java
ForceSkipDiagnostics diagnostics = new ForceSkipDiagnostics(
        doublewriteSummary.skippedPageCount(),
        redoSummary.skippedRecordCount(),
        reconcileSummary.skippedSpaceCount());
RecoveryReport report = request.mode() == RecoveryMode.FORCE_SKIP_CORRUPT_TABLESPACE
        ? RecoveryReport.forceSkip(state, checkpoint.checkpointLsn(), reader.recoveredToLsn(),
                doublewriteSummary.repairedPageCount(), doublewriteSummary.detectedOnlyPageCount(),
                redoSummary.scannedBatchCount(), request.skipPolicy().skippedSpaces(),
                diagnostics.skippedDoublewritePages(), diagnostics.skippedRedoRecords(),
                diagnostics.skippedReconcileSpaces(), stages)
        : RecoveryReport.normal(state, checkpoint.checkpointLsn(), reader.recoveredToLsn(),
                doublewriteSummary.repairedPageCount(), doublewriteSummary.detectedOnlyPageCount(),
                redoSummary.scannedBatchCount(), stages);
```

Change the `OPEN_TRAFFIC` completion call to include detail only for force-skip:

```java
String detail = request.mode() == RecoveryMode.FORCE_SKIP_CORRUPT_TABLESPACE
        ? diagnostics.describe(request.skipPolicy())
        : "";
tracker.complete(state, reader.recoveredToLsn(), detail);
```

Add overload in `RecoveryStageTracker`:

```java
private void complete(RecoveryState state, Lsn recoveredToLsn, String detail) {
    if (currentStage == null) {
        throw new DatabaseValidationException("recovery stage completion without active stage");
    }
    completedStages.add(currentStage);
    journal.stageCompleted(mode, currentStage, state, recoveredToLsn, detail);
    currentStage = null;
}
```

Keep the existing two-argument completion method delegating to `complete(state, recoveredToLsn, "")`.

- [ ] **Step 9: Update read-only and fail-closed report construction**

Use the factories added in Task 1:

```java
RecoveryReport report = RecoveryReport.readOnlyValidate(state, checkpoint.checkpointLsn(),
        reader.recoveredToLsn(), doublewriteSummary.detectedOnlyPageCount(),
        tracker.completedStages());
```

In `failClosed(...)`:

```java
lastReport = RecoveryReport.failed(mode, Set.of());
```

In a later Task 4 adjustment, pass the request skip set into failure reporting if needed. At this task boundary, failed force-skip before report construction is still diagnosable through progress journal and thrown exception.

- [ ] **Step 10: Run recovery tests**

Run:

```powershell
$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "*CrashRecoveryForceSkipTest" --tests "*CrashRecoveryServiceTest" --tests "*SpaceFileReconcileRecoveryTest"
```

Expected: PASS.

---

### Task 4: Wire EngineConfig And StorageEngine Composition

**Files:**
- Modify: `src/main/java/cn/zhangyis/db/storage/engine/EngineConfig.java`
- Modify: `src/main/java/cn/zhangyis/db/storage/engine/StorageEngine.java`
- Test: `src/test/java/cn/zhangyis/db/storage/engine/EngineConfigTest.java`
- Test: `src/test/java/cn/zhangyis/db/storage/engine/StorageEngineForceSkipRecoveryTest.java`

**Interfaces:**
- Consumes: `RecoverySkipPolicy`, `RecoveryRequest.forceSkip(...)`, `EngineTablespaceConfig`, `StorageEngine.recoverExisting()`, `clusteredIndex.rootPageId().spaceId()`.
- Produces: `EngineConfig.forceSkippedSpaces()`, `EngineConfig.withForceSkippedSpaces(Set<SpaceId>)`, force-skip recovery request assembly, non-open of skipped recovery tablespaces.

- [ ] **Step 1: Add EngineConfig red tests**

Extend `EngineConfigTest`:

```java
@Test
void forceSkippedSpacesDefaultEmptyAndImmutable() {
    EngineConfig c = valid();

    assertEquals(Set.of(), c.forceSkippedSpaces());
    EngineConfig force = c.withForceSkippedSpaces(Set.of(SpaceId.of(7)));
    assertEquals(Set.of(SpaceId.of(7)), force.forceSkippedSpaces());
    assertEquals(Set.of(), c.forceSkippedSpaces());
    assertThrows(UnsupportedOperationException.class,
            () -> force.forceSkippedSpaces().add(SpaceId.of(8)));
}

@Test
void forceSkippedSpacesRejectNullInputs() {
    EngineConfig c = valid();

    assertThrows(DatabaseValidationException.class, () -> c.withForceSkippedSpaces(null));
    assertThrows(DatabaseValidationException.class, () -> c.withForceSkippedSpaces(new java.util.HashSet<>(java.util.Arrays.asList(SpaceId.of(7), null))));
}
```

Add imports for `Set`, `HashSet`, and `Arrays` as needed.

- [ ] **Step 2: Add StorageEngine red test for skipped tablespace not opened**

Create `StorageEngineForceSkipRecoveryTest`:

```java
package cn.zhangyis.db.storage.engine;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.recovery.RecoveryMode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StorageEngineForceSkipRecoveryTest {
    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId UNDO = SpaceId.of(5);
    private static final SpaceId SKIPPED = SpaceId.of(21);

    @TempDir
    Path dir;

    @Test
    void forceSkipRecoveryDoesNotOpenSkippedConfiguredTablespace() {
        EngineConfig fresh = baseConfig(List.of()).withRecoveryMode(RecoveryMode.NORMAL);
        StorageEngine clean = new StorageEngine(fresh);
        clean.open();
        clean.close();
        Path missingSkippedFile = dir.resolve("missing-skipped.ibd");
        EngineConfig force = baseConfig(List.of(new EngineTablespaceConfig(SKIPPED, missingSkippedFile)))
                .withRecoveryMode(RecoveryMode.FORCE_SKIP_CORRUPT_TABLESPACE)
                .withForceSkippedSpaces(Set.of(SKIPPED));

        StorageEngine recovered = new StorageEngine(force);
        assertDoesNotThrow(recovered::open);
        assertFalse(Files.exists(missingSkippedFile),
                "skipped tablespace must not be created or opened during recovery");
        recovered.close();
    }

    @Test
    void forceSkipRejectsUndoSpaceBeforeRecoveryOpensFiles() {
        EngineConfig fresh = baseConfig(List.of()).withRecoveryMode(RecoveryMode.NORMAL);
        StorageEngine clean = new StorageEngine(fresh);
        clean.open();
        clean.close();
        EngineConfig config = baseConfig(List.of())
                .withRecoveryMode(RecoveryMode.FORCE_SKIP_CORRUPT_TABLESPACE)
                .withForceSkippedSpaces(Set.of(UNDO));
        StorageEngine recovered = new StorageEngine(config);

        assertThrows(DatabaseValidationException.class, recovered::open);
    }

    private EngineConfig baseConfig(List<EngineTablespaceConfig> recoveryTablespaces) {
        return new EngineConfig(dir, PS, 256, UNDO,
                PageNo.of(64), 64, 100, Duration.ofSeconds(5), 64L * 1024 * 1024,
                recoveryTablespaces);
    }
}
```

- [ ] **Step 3: Run engine red tests**

Run:

```powershell
$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "*EngineConfigTest" --tests "*StorageEngineForceSkipRecoveryTest"
```

Expected: compile failure because `forceSkippedSpaces` and force-skip engine wiring do not exist.

- [ ] **Step 4: Modify EngineConfig record and constructors**

Append `Set<SpaceId> forceSkippedSpaces` as the final record component. Do not insert it before existing components because several tests and constructors use positional record construction.

In the canonical constructor:

```java
if (forceSkippedSpaces == null) {
    throw new DatabaseValidationException("force skipped spaces must not be null");
}
for (SpaceId spaceId : forceSkippedSpaces) {
    if (spaceId == null) {
        throw new DatabaseValidationException("force skipped space must not be null");
    }
}
forceSkippedSpaces = Set.copyOf(forceSkippedSpaces);
```

Update all existing convenience constructors to pass `Set.of()` for the new final argument.

Add wither:

```java
/**
 * 配置 FORCE_SKIP_CORRUPT_TABLESPACE 的显式跳过空间集合。仅设置集合不会改变 recovery mode；
 * 调用方仍必须显式选择 FORCE_SKIP_CORRUPT_TABLESPACE，避免默认启动误跳过数据。
 */
public EngineConfig withForceSkippedSpaces(Set<SpaceId> spaces) {
    return new EngineConfig(baseDir, pageSize, bufferPoolCapacityFrames, undoSpaceId,
            undoSpaceInitialPages, slotCapacity, maxVersionHops, flushTimeout, redoCapacityBytes,
            recoveryTablespaces, backgroundFlushEnabled, pageCleanerQueueCapacity,
            backgroundFlushInterval, backgroundFlushMaxPages, backgroundFlushStopTimeout,
            redoRotation, bufferPoolInstanceCount, recoveryMode, spaces);
}
```

Update `withRecoveryMode(...)`, `withRedoRotation(...)`, `withSingleFileRedo()`, `withBufferPoolInstanceCount(...)`, and any other wither so they pass the current `forceSkippedSpaces`.

- [ ] **Step 5: Split configured and opened recovery spaces in StorageEngine**

In `StorageEngine.recoverExisting`, before opening configured recovery tablespaces:

```java
Set<SpaceId> skippedSpaces = config.forceSkippedSpaces();
boolean forceSkip = config.recoveryMode() == RecoveryMode.FORCE_SKIP_CORRUPT_TABLESPACE;
if (forceSkip) {
    validateForceSkipConfiguration(skippedSpaces);
} else if (!skippedSpaces.isEmpty()) {
    throw new DatabaseValidationException("force skipped spaces require FORCE_SKIP_CORRUPT_TABLESPACE mode");
}
```

Add helper:

```java
private void validateForceSkipConfiguration(Set<SpaceId> skippedSpaces) {
    if (skippedSpaces == null || skippedSpaces.isEmpty()) {
        throw new DatabaseValidationException("FORCE_SKIP_CORRUPT_TABLESPACE requires skipped spaces");
    }
    if (skippedSpaces.contains(config.undoSpaceId())) {
        throw new DatabaseValidationException("system undo tablespace cannot be force-skipped");
    }
    if (clusteredIndex != null && skippedSpaces.contains(clusteredIndex.rootPageId().spaceId())) {
        throw new DatabaseValidationException("configured clustered index space cannot be force-skipped");
    }
}
```

When opening recovery tablespaces:

```java
for (EngineTablespaceConfig tablespace : config.recoveryTablespaces()) {
    if (skippedSpaces.contains(tablespace.spaceId())) {
        continue;
    }
    diskSpaceManager.openTablespaceForRecovery(tablespace.spaceId(), tablespace.path());
}
```

Build two lists:

```java
List<SpaceId> configuredRecoverySpaces = recoverySpaceIds();
List<SpaceId> openedRecoverySpaces = configuredRecoverySpaces.stream()
        .filter(spaceId -> !skippedSpaces.contains(spaceId))
        .toList();
```

Use `configuredRecoverySpaces` for doublewrite page filtering and `withSpaceFileReconcile(...)` so skipped spaces are counted. Use `openedRecoverySpaces` only for logic that must represent files actually opened and allowed for IO.

- [ ] **Step 6: Build force-skip RecoveryRequest in StorageEngine**

Replace the force-skip reserved branch with:

```java
case FORCE_SKIP_CORRUPT_TABLESPACE -> RecoveryRequest.forceSkip(checkpointStore, redoRepository,
                RedoApplyDispatcher.pageDispatcher(), applyContext, skippedSpaces)
        .withDoublewriteRepair(doublewriteScanner, pagesToRepair)
        .withRedoBoundaryInstall(redo)
        .withUndoTablespaceRecovery(buildUndoTablespaceRecovery())
        .withSpaceFileReconcile(configuredRecoverySpaces)
        .withTransactionUndoRecovery(this::recoverTransactionUndoAfterRedo);
```

Keep `NORMAL` and `READ_ONLY_VALIDATE` behavior unchanged except for constructor signature updates from earlier tasks.

Doublewrite pages should be filtered to configured spaces before entering service:

```java
List<PageId> pagesToRepair = doublewriteRepository.pageIds().stream()
        .filter(pageId -> configuredRecoverySpaces.contains(pageId.spaceId()))
        .toList();
```

The service will then count and skip entries whose `spaceId` is in the force-skip set.

- [ ] **Step 7: Ensure skipped opened handles are never forced**

Because skipped data spaces were not opened, existing `request.applyContext().pageStore().forceAll()` should not touch them. Add a code comment above the force-skip open loop:

```java
// force-skip 的核心安全边界：被跳过的数据表空间不进入 PageStore registry，
// 后续 redo/reconcile/filter 只能统计它们，最终 forceAll 也只能触达已打开空间。
```

Do not add a `PageStore` wrapper in production for this task. The skip policy already prevents skipped page IO in recovery service, and unopened handles prevent final force.

- [ ] **Step 8: Run engine tests**

Run:

```powershell
$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "*EngineConfigTest" --tests "*StorageEngineForceSkipRecoveryTest" --tests "*StorageEngineTest"
```

Expected: PASS.

---

### Task 5: Update Current Implementation Map And Backlog

**Files:**
- Modify: `docs/design/current-implementation-map.md`
- Modify: `docs/design/storage-backlog.md`

**Interfaces:**
- Consumes: verified production code call chain from Tasks 1-4.
- Produces: updated authoritative map and backlog status for future context recovery.

- [ ] **Step 1: Update recovery current map**

In `docs/design/current-implementation-map.md`, update the recovery section to state:

```text
FORCE_SKIP_CORRUPT_TABLESPACE is production-wired for explicit SpaceId sets:
StorageEngine validates config, does not open skipped recovery tablespaces, and builds RecoveryRequest.forceSkip.
CrashRecoveryService filters skipped spaces before doublewrite scan, redo page apply, and space-file reconcile.
RecoveryReport and RecoveryProgressJournal record skipped spaces plus doublewrite/redo/reconcile skip counts.
```

- [ ] **Step 2: Update recovery known gaps**

Replace any line that says force-skip is reserved or unimplemented with:

```text
Explicit force-skip exists for configured SpaceId sets only. Remaining gaps: no DD/data-directory discovery,
no object-level DDL force recovery, no automatic transition from persisted CORRUPTED registry state into skip config.
```

Keep any unrelated gaps about DDL, SQL, purge, or multi-index transaction undo unchanged.

- [ ] **Step 3: Update storage backlog**

In `docs/design/storage-backlog.md`, mark the 0.7 force-skip item complete with a concise note:

```text
Completed: FORCE_SKIP_CORRUPT_TABLESPACE supports explicit skipped SpaceId sets, avoids opening skipped recovery files,
filters doublewrite/redo/reconcile, and reports skipped diagnostics. Discovery and DD object-level recovery remain future work.
```

If the backlog has a detailed 0.16b/0.16c dependency note, preserve it and add that force-skip can consume administrator-provided or future persisted-corruption space ids.

- [ ] **Step 4: Review docs for misleading edges**

Run:

```powershell
rg -n "FORCE_SKIP_CORRUPT_TABLESPACE|force-skip|reserved|unimplemented|skipped" docs/design/current-implementation-map.md docs/design/storage-backlog.md
```

Expected: no line claims `FORCE_SKIP_CORRUPT_TABLESPACE` is still reserved; remaining gaps clearly say discovery/object-level recovery are future work.

---

### Task 6: Final Verification

**Files:**
- No code edits unless verification fails.

**Interfaces:**
- Consumes: all previous tasks.
- Produces: evidence that force-skip implementation is complete and did not weaken normal recovery behavior.

- [ ] **Step 1: Run targeted force-skip tests**

```powershell
$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "*RecoveryForceSkipModelTest" --tests "*RedoApplyDispatcherSkipTest" --tests "*CrashRecoveryForceSkipTest" --tests "*StorageEngineForceSkipRecoveryTest"
```

Expected: PASS.

- [ ] **Step 2: Run recovery and redo regression tests**

```powershell
$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "*CrashRecoveryServiceTest" --tests "*SpaceFileReconcileRecoveryTest" --tests "*RecoveryProgressJournalTest" --tests "*PageRedoApplyExtendOnDemandTest" --tests "*RedoRuntimeRecoveryTest" --tests "*StorageEngineTest" --tests "*EngineConfigTest"
```

Expected: PASS.

- [ ] **Step 3: Run full test suite**

```powershell
$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test
```

Expected: PASS.

- [ ] **Step 4: Scan for prohibited concurrency primitives in touched production files**

```powershell
rg -n "synchronized|wait\\(|notify\\(|notifyAll\\(" src/main/java/cn/zhangyis/db/storage/recovery src/main/java/cn/zhangyis/db/storage/redo src/main/java/cn/zhangyis/db/storage/engine
```

Expected: no new production-code uses introduced by this work package.

- [ ] **Step 5: Scan for bare runtime exceptions in touched production files**

```powershell
rg -n "new IllegalArgumentException|new RuntimeException" src/main/java/cn/zhangyis/db/storage/recovery src/main/java/cn/zhangyis/db/storage/redo src/main/java/cn/zhangyis/db/storage/engine
```

Expected: no bare exceptions in touched production code.

- [ ] **Step 6: Inspect planned diff**

```powershell
git diff -- src/main/java/cn/zhangyis/db/storage/recovery src/main/java/cn/zhangyis/db/storage/redo src/main/java/cn/zhangyis/db/storage/engine src/test/java/cn/zhangyis/db/storage/recovery src/test/java/cn/zhangyis/db/storage/redo src/test/java/cn/zhangyis/db/storage/engine docs/design docs/superpowers/plans/2026-07-06-recovery-force-skip-corrupt-tablespace-plan.md
```

Expected: only intended force-skip code, tests, current-map/backlog docs, and this plan changed; no build output or IDE files.

---

## Self-Review

- Spec coverage: the plan covers explicit skipped spaces, request/report/journal diagnostics, doublewrite skip, redo page-record skip with original LSN boundary, redo boundary installation, undo-space rejection, reconcile skip, transaction undo clustered-index rejection, traffic gate behavior, engine non-open of skipped files, current-map/backlog updates, and full verification.
- Placeholder scan: the plan uses concrete files, method names, snippets, commands, and expected outcomes. It does not rely on vague implementation notes.
- Type consistency: `RecoverySkipPolicy`, `RecoveryRequest.forceSkip`, `RedoApplySummary`, `RedoApplyBatchView`, `RecoveryReport.forceSkip`, and `EngineConfig.withForceSkippedSpaces` are introduced before later tasks consume them.
