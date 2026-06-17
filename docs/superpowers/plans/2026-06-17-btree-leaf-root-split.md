# B3 B+Tree Leaf Root Split Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the first split-capable B+Tree implementation: root leaf split, level-1 root-to-leaf lookup, sibling range scan, and level-1 leaf split without parent split.

**Architecture:** Keep `LeafOnlyBTreeIndexService` as the B1/B2 narrow facade and add `SplitCapableBTreeIndexService` for B3. B+Tree uses `DiskSpaceManager` for page allocation, `IndexPageAccess` for MTR-owned INDEX pages, record-layer inserter/search/cursor for page-local data, and physical `PAGE_INIT/PAGE_BYTES` redo for recovery.

**Tech Stack:** Java 25, Gradle/JUnit Jupiter, existing `storage.btree`, `storage.api`, `storage.page`, `storage.record`, `storage.mtr`, `storage.redo`, `storage.flush`.

---

## Files

- Modify: `src/main/java/cn/zhangyis/db/storage/btree/BTreeIndex.java`
- Modify: `src/main/java/cn/zhangyis/db/storage/btree/BTreeInsertResult.java`
- Modify: `src/main/java/cn/zhangyis/db/storage/btree/BTreeIndexService.java`
- Modify: `src/main/java/cn/zhangyis/db/storage/api/IndexPageAccess.java`
- Modify: `src/main/java/cn/zhangyis/db/storage/page/PageEnvelope.java`
- Modify: `src/main/java/cn/zhangyis/db/storage/btree/BTreeSplitRequiredException.java`
- Create: `src/main/java/cn/zhangyis/db/storage/api/IndexPageHandle.java`
- Create: `src/main/java/cn/zhangyis/db/storage/btree/BTreeRootChangedException.java`
- Create: `src/main/java/cn/zhangyis/db/storage/btree/BTreeParentSplitRequiredException.java`
- Create: `src/main/java/cn/zhangyis/db/storage/btree/BTreeNodePointer.java`
- Create: `src/main/java/cn/zhangyis/db/storage/btree/BTreeNodePointerSchema.java`
- Create: `src/main/java/cn/zhangyis/db/storage/btree/BTreeNodePointerCodec.java`
- Create: `src/main/java/cn/zhangyis/db/storage/btree/SearchKeyComparator.java`
- Create: `src/main/java/cn/zhangyis/db/storage/btree/SplitCapableBTreeIndexService.java`
- Test: `src/test/java/cn/zhangyis/db/storage/btree/SplitCapableBTreeIndexServiceTest.java`

## Execution Notes

- Before editing existing symbols, run GitNexus impact for `BTreeIndex`, `BTreeInsertResult`, `BTreeIndexService`, `IndexPageAccess`, `PageEnvelope`, and `BTreeSplitRequiredException`.
- Use fixed Gradle/JDK:

```powershell
$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "cn.zhangyis.db.storage.btree.*" --console=plain
```

- If `gitnexus_detect_changes` fails because the repo has no `HEAD`, record that failure and continue.
- Do not commit; this repo currently has no valid `HEAD`.

## Implementation Status - 2026-06-17

- Completed B3 implementation in current workspace: `SplitCapableBTreeIndexService`, node-pointer schema/codec, `IndexPageHandle`, sibling-link narrow writes, BTree API metadata compatibility, root leaf split, level-1 leaf split, sibling scan, parent-overflow preflight, and redo replay coverage.
- Verification passed:
  - `gradle test --tests "cn.zhangyis.db.storage.btree.SplitCapableBTreeIndexServiceTest"`
  - `gradle test --tests "cn.zhangyis.db.storage.btree.*"`
  - `gradle test --tests "cn.zhangyis.db.storage.btree.*" --tests "cn.zhangyis.db.storage.record.page.*" --tests "cn.zhangyis.db.storage.api.IndexPageAccessTest" --tests "cn.zhangyis.db.storage.redo.RedoRuntimeRecoveryTest"`
  - `gradle clean test`
- `gitnexus_detect_changes(scope=all)` failed because this repository has no valid `HEAD` (`git diff HEAD` cannot resolve HEAD).
- `npx gitnexus analyze --force` completed after final code verification: 6,029 nodes / 17,028 edges / 127 clusters / 286 flows. `npx gitnexus status` reports up-to-date.
- GitNexus MCP context still showed the old in-memory counts after reindex; the CLI status confirms the on-disk index is current and MCP likely needs process reload to pick up new stats.

---

### Task 1: API Compatibility and Metadata

**Files:**
- Modify: `src/main/java/cn/zhangyis/db/storage/btree/BTreeIndex.java`
- Modify: `src/main/java/cn/zhangyis/db/storage/btree/BTreeInsertResult.java`
- Modify: `src/main/java/cn/zhangyis/db/storage/btree/BTreeIndexService.java`
- Create: `src/main/java/cn/zhangyis/db/storage/btree/BTreeRootChangedException.java`
- Create: `src/main/java/cn/zhangyis/db/storage/btree/BTreeParentSplitRequiredException.java`
- Test: `src/test/java/cn/zhangyis/db/storage/btree/SplitCapableBTreeIndexServiceTest.java`

- [ ] **Step 1: Write failing API compatibility tests**

Add tests that require the new source-compatible constructors and scan method:

```java
@Test
void btreeIndexKeepsNoSplitConstructorAndCanCarrySegments() {
    BTreeIndex legacy = new BTreeIndex(7L, ROOT, 0, idKey(), schema(), true);
    assertEquals(ROOT, legacy.rootPageId());

    SegmentRef leaf = new SegmentRef(SPACE, 0, SegmentId.of(1));
    SegmentRef nonLeaf = new SegmentRef(SPACE, 1, SegmentId.of(2));
    BTreeIndex split = new BTreeIndex(7L, ROOT, 0, idKey(), schema(), true, leaf, nonLeaf);
    assertEquals(leaf, split.leafSegment());
    assertEquals(nonLeaf, split.nonLeafSegment());
}

@Test
void insertResultKeepsOldConstructorAndExposesSplitMetadata() {
    RecordRef ref = new RecordRef(ROOT, 2, 128, 1, 7L);
    BTreeInsertResult legacy = new BTreeInsertResult(index(), ref);
    assertEquals(index(), legacy.indexAfterInsert());
    assertFalse(legacy.splitOccurred());
    assertEquals(List.of(), legacy.allocatedPages());
}
```

- [ ] **Step 2: Run RED for API tests**

Run:

```powershell
$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "cn.zhangyis.db.storage.btree.SplitCapableBTreeIndexServiceTest" --console=plain
```

Expected: FAIL at compile time because segment-aware constructors and `indexAfterInsert/splitOccurred/allocatedPages` do not exist.

- [ ] **Step 3: Implement minimal API changes**

Implement:

```java
public record BTreeIndex(long indexId, PageId rootPageId, int rootLevel,
                         IndexKeyDef keyDef, TableSchema schema, boolean unique,
                         SegmentRef leafSegment, SegmentRef nonLeafSegment) {
    public BTreeIndex(long indexId, PageId rootPageId, int rootLevel,
                      IndexKeyDef keyDef, TableSchema schema, boolean unique) {
        this(indexId, rootPageId, rootLevel, keyDef, schema, unique, null, null);
    }
}
```

and:

```java
public record BTreeInsertResult(BTreeIndex index, RecordRef recordRef,
                                BTreeIndex indexAfterInsert, boolean splitOccurred,
                                List<PageId> allocatedPages) {
    public BTreeInsertResult(BTreeIndex index, RecordRef recordRef) {
        this(index, recordRef, index, false, List.of());
    }
}
```

Add `default List<BTreeLookupResult> scan(...) { return scanLeaf(...); }` to `BTreeIndexService` and update `scanLeaf` Javadoc.

- [ ] **Step 4: Run GREEN for API tests**

Run the same targeted btree test command.

Expected: PASS for the new API tests and existing B1/B2 tests still compile.

---

### Task 2: INDEX Page Handle and Sibling Links

**Files:**
- Modify: `src/main/java/cn/zhangyis/db/storage/page/PageEnvelope.java`
- Modify: `src/main/java/cn/zhangyis/db/storage/api/IndexPageAccess.java`
- Create: `src/main/java/cn/zhangyis/db/storage/api/IndexPageHandle.java`
- Test: `src/test/java/cn/zhangyis/db/storage/btree/SplitCapableBTreeIndexServiceTest.java`

- [ ] **Step 1: Write failing sibling-link test**

Add a test that opens an INDEX page handle and writes only sibling links:

```java
@Test
void indexPageHandleUpdatesSiblingLinksWithoutRewritingWholeHeader() {
    onBTreePool((ctx) -> {
        ctx.createRoot();
        MiniTransaction m = ctx.mgr.begin();
        IndexPageHandle handle = ctx.access.openIndexPageHandle(m, ROOT, PageLatchMode.EXCLUSIVE);
        handle.writeSiblingLinks(11, 12);
        ctx.mgr.commit(m);

        MiniTransaction read = ctx.mgr.begin();
        IndexPageHandle loaded = ctx.access.openIndexPageHandle(read, ROOT, PageLatchMode.SHARED);
        assertEquals(11L, loaded.fileHeader().prevPageNo());
        assertEquals(12L, loaded.fileHeader().nextPageNo());
        assertEquals(7L, loaded.recordPage().header().indexId());
        assertEquals(PageType.INDEX, loaded.fileHeader().pageType());
        ctx.mgr.commit(read);
    });
}
```

- [ ] **Step 2: Run RED for sibling-link test**

Run the targeted test.

Expected: FAIL at compile time because `IndexPageHandle` and `openIndexPageHandle` do not exist.

- [ ] **Step 3: Implement handle and narrow envelope writer**

Implement:

```java
public static void writeSiblingLinks(PageGuard guard, long prevPageNo, long nextPageNo) {
    if (guard == null) {
        throw new DatabaseValidationException("page guard must not be null");
    }
    if (prevPageNo < 0 || nextPageNo < 0) {
        throw new DatabaseValidationException("prev/next page no must be non-negative or FIL_NULL");
    }
    guard.writeInt(PageEnvelopeLayout.PREV_PAGE_NO, (int) prevPageNo);
    guard.writeInt(PageEnvelopeLayout.NEXT_PAGE_NO, (int) nextPageNo);
}
```

Create `IndexPageHandle` with `pageId`, `fileHeader`, `recordPage`, and `writeSiblingLinks`.

- [ ] **Step 4: Run GREEN for sibling-link test**

Run targeted btree tests.

Expected: PASS.

---

### Task 3: Node Pointer Encoding and Key Comparison

**Files:**
- Create: `src/main/java/cn/zhangyis/db/storage/btree/BTreeNodePointer.java`
- Create: `src/main/java/cn/zhangyis/db/storage/btree/BTreeNodePointerSchema.java`
- Create: `src/main/java/cn/zhangyis/db/storage/btree/BTreeNodePointerCodec.java`
- Create: `src/main/java/cn/zhangyis/db/storage/btree/SearchKeyComparator.java`
- Test: `src/test/java/cn/zhangyis/db/storage/btree/SplitCapableBTreeIndexServiceTest.java`

- [ ] **Step 1: Write failing node-pointer test**

Add:

```java
@Test
void nodePointerRoundTripsThroughDerivedSchema() {
    BTreeNodePointerSchema pointerSchema = BTreeNodePointerSchema.from(index());
    PageId child = PageId.of(SPACE, PageNo.of(9));
    BTreeNodePointer pointer = new BTreeNodePointer(kId(42), child);

    LogicalRecord encoded = new BTreeNodePointerCodec().toRecord(pointer, pointerSchema);
    assertEquals(RecordType.NODE_POINTER, encoded.recordType());

    BTreeNodePointer decoded = new BTreeNodePointerCodec().fromRecord(encoded, pointerSchema);
    assertEquals(pointer, decoded);
    assertEquals(2, pointerSchema.childSpaceColumnOrdinal());
    assertEquals(3, pointerSchema.childPageColumnOrdinal());
}
```

- [ ] **Step 2: Run RED for node-pointer test**

Expected: FAIL at compile time because node-pointer support classes do not exist.

- [ ] **Step 3: Implement node-pointer support**

Implement `BTreeNodePointer` as a record with non-null validation.

Implement `BTreeNodePointerSchema.from(BTreeIndex index)` deriving key columns from `index.keyDef().parts()` and adding `BIGINT UNSIGNED` child columns.

Implement `BTreeNodePointerCodec.toRecord/fromRecord`.

Implement `SearchKeyComparator` using `TypeCodecRegistry`, key part definitions, and `ColumnValue` comparison rules.

- [ ] **Step 4: Run GREEN for node-pointer test**

Run targeted btree tests.

Expected: PASS.

---

### Task 4: Root Leaf Split

**Files:**
- Create: `src/main/java/cn/zhangyis/db/storage/btree/SplitCapableBTreeIndexService.java`
- Test: `src/test/java/cn/zhangyis/db/storage/btree/SplitCapableBTreeIndexServiceTest.java`

- [ ] **Step 1: Write failing root split tests**

Add tests:

```java
@Test
void rootLeafOverflowSplitsRootAndLookupFindsBothLeaves() {
    onBTreePool((ctx) -> {
        ctx.createTablespaceAndSegments();
        BTreeIndexService service = new SplitCapableBTreeIndexService(ctx.access, ctx.disk, registry);
        BTreeIndex idx = ctx.splitIndex();
        BTreeIndex current = idx;

        for (long id = 1; id <= 8; id++) {
            MiniTransaction m = ctx.mgr.begin();
            BTreeInsertResult r = service.insert(m, current, wideRow(id));
            current = r.indexAfterInsert();
            ctx.mgr.commit(m);
        }

        assertEquals(1, current.rootLevel());
        assertEquals(ROOT, current.rootPageId());

        assertFound(service, current, 1);
        assertFound(service, current, 8);
    });
}

@Test
void rootSplitLinksChildLeavesAndRangeScanCrossesSibling() {
    onBTreePool((ctx) -> {
        BTreeIndex current = ctx.insertWideRowsUntilRootSplit(1, 8);
        BTreeIndexService service = new SplitCapableBTreeIndexService(ctx.access, ctx.disk, registry);

        MiniTransaction scan = ctx.mgr.begin();
        List<Long> ids = service.scan(scan, current, new BTreeScanRange(kId(1), true, kId(9), false, 20))
                .stream().map(SplitCapableBTreeIndexServiceTest::idOf).toList();
        ctx.mgr.commit(scan);

        assertEquals(List.of(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L), ids);
        assertLeafLinksAreBidirectional(ctx, current);
    });
}
```

- [ ] **Step 2: Run RED for root split tests**

Expected: FAIL at compile time because `SplitCapableBTreeIndexService` does not exist or at runtime because root split is not implemented.

- [ ] **Step 3: Implement minimal root split**

Implement the service with:

- leaf-level path using existing B1/B2 logic
- `insert` catching `RecordPageOverflowException`
- materialize root rows before reformatting
- median split by record count
- allocate two child leaves through `DiskSpaceManager.allocatePage`
- format child leaves and stable root through `IndexPageAccess.createIndexPage`
- sibling links through `IndexPageHandle.writeSiblingLinks`
- root node pointers through `BTreeNodePointerCodec`

- [ ] **Step 4: Run GREEN for root split tests**

Run targeted btree tests.

Expected: PASS.

---

### Task 5: Level-1 Leaf Split and Parent Overflow Preflight

**Files:**
- Modify: `src/main/java/cn/zhangyis/db/storage/btree/SplitCapableBTreeIndexService.java`
- Test: `src/test/java/cn/zhangyis/db/storage/btree/SplitCapableBTreeIndexServiceTest.java`

- [ ] **Step 1: Write failing level-1 split tests**

Add:

```java
@Test
void levelOneInsertSplitsOnlyTargetLeafAndUpdatesRootPointers() {
    onBTreePool((ctx) -> {
        BTreeIndex current = ctx.insertWideRowsUntilRootSplit(1, 8);
        BTreeIndexService service = new SplitCapableBTreeIndexService(ctx.access, ctx.disk, registry);

        for (long id = 9; id <= 12; id++) {
            MiniTransaction m = ctx.mgr.begin();
            BTreeInsertResult r = service.insert(m, current, wideRow(id));
            current = r.indexAfterInsert();
            ctx.mgr.commit(m);
        }

        assertEquals(1, current.rootLevel());
        assertFound(service, current, 12);
        assertLeafLinksAreBidirectional(ctx, current);
    });
}

@Test
void parentOverflowFailsBeforeLeafRewrite() {
    onBTreePool((ctx) -> {
        BTreeIndex current = ctx.indexWithTinyRootPointerCapacity();
        BTreeIndexService service = new SplitCapableBTreeIndexService(ctx.access, ctx.disk, registry);
        MiniTransaction m = ctx.mgr.begin();
        assertThrows(BTreeParentSplitRequiredException.class, () -> service.insert(m, current, wideRow(99)));
        ctx.mgr.rollbackUncommitted(m);
    });
}
```

- [ ] **Step 2: Run RED for level-1 tests**

Expected: FAIL until level-1 leaf split and preflight are implemented.

- [ ] **Step 3: Implement level-1 leaf split**

Implement:

- root-to-leaf descent using root node pointers
- preflight root has room for one pointer before leaf reformat
- split target leaf into old + one new leaf
- update old/new/right sibling links
- insert new node pointer into root
- throw `BTreeParentSplitRequiredException` for parent overflow

- [ ] **Step 4: Run GREEN for level-1 tests**

Run targeted btree tests.

Expected: PASS.

---

### Task 6: Redo Replay and Full Regression

**Files:**
- Test: `src/test/java/cn/zhangyis/db/storage/btree/SplitCapableBTreeIndexServiceTest.java`

- [ ] **Step 1: Write failing redo replay split test**

Add a test that uses durable redo, performs a split, replays redo into a fresh `PageStore`, and verifies lookup/scan structure is visible after replay.

- [ ] **Step 2: Run RED for redo replay test**

Expected: FAIL if split redo does not include all page bytes or pageLSN stamping is wrong.

- [ ] **Step 3: Fix any missing physical writes**

If the replay test fails, ensure all destructive writes use `PageGuard` write APIs and happen inside MTR-owned guards.

- [ ] **Step 4: Run targeted regression**

Run:

```powershell
$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "cn.zhangyis.db.storage.btree.*" --tests "cn.zhangyis.db.storage.record.page.*" --tests "cn.zhangyis.db.storage.api.IndexPageAccessTest" --console=plain
```

Expected: PASS.

- [ ] **Step 5: Run full verification**

Run:

```powershell
$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" clean test --console=plain
```

Expected: PASS with zero failures and zero errors.

- [ ] **Step 6: Refresh GitNexus**

Run:

```powershell
npx gitnexus analyze --force
```

Expected: analyzer completes successfully. Record node/edge/flow counts in memory.
