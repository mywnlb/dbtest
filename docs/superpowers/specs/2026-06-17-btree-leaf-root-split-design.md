# B3 - B+Tree Leaf and Root Split Design

## 1. Scope

This slice extends the current leaf-only B+Tree into a minimal two-level tree:

- root leaf insert still works when the root has free space
- root leaf overflow performs a root split while keeping the root page id stable
- after root split, the root becomes a level-1 non-leaf page
- leaf pages are linked by `FIL_PAGE_PREV/FIL_PAGE_NEXT`
- lookup descends from root to leaf
- range scan starts at the lower-bound leaf and follows leaf siblings
- insert into a level-1 tree can split a full leaf and insert a new child pointer into the root

This slice does not implement parent split above level 1, merge, redistribute, delete-mark integration, purge, transaction locks, MVCC visibility, undo, concurrent latch coupling, read-ahead, or a B+Tree-specific logical redo handler.

Recovery remains physical: all structure changes are made inside the caller MTR and are recovered through existing `PAGE_INIT` and `PAGE_BYTES` records with pageLSN idempotence.

## 2. Design References

This spec is constrained by:

- `docs/design/innodb-storage-engine-overview.md`: page split must allocate new pages, rebuild page contents, update sibling links, update parent separators, and remain MTR/redo protected.
- `docs/design/innodb-btree-design.md`: B+Tree owns cross-page navigation, leaf sibling links, split point choice, parent separator propagation, and root split while record owns page-local layout.
- `docs/design/innodb-record-design.md`: B+Tree must use record layer operations for page-local record ordering, materialization, insert, and directory rebuild.
- `docs/design/innodb-disk-manager-design.md`: new leaf/non-leaf pages are allocated through segment-aware `DiskSpaceManager`, not by B+Tree modifying XDES or inode pages.
- `docs/design/innodb-buffer-pool-design.md`: B+Tree must use MTR-owned page guards and must not access `BufferFrame`.
- `docs/design/innodb-redo-log-design.md`: every persistent page modification must be in MTR redo, and recovery handlers must not execute logical split decisions.
- `docs/design/innodb-flush-checkpoint-doublewrite-design.md`: data-page flush remains WAL-gated; B+Tree does not bypass flush or checkpoint.
- `docs/design/innodb-crash-recovery-design.md`: redo recovery applies physical records and never re-runs B+Tree search or split policy.
- `docs/design/innodb-transaction-mvcc-design.md`: current-read lock waits must release page latches and relocate; this slice avoids row-lock waits entirely.

## 3. Public API Changes

### `BTreeIndex`

`BTreeIndex` remains the immutable index descriptor, but B3 adds the page-allocation information required by split:

- `SegmentRef leafSegment`
- `SegmentRef nonLeafSegment`

The canonical descriptor stores both segment references. The existing six-argument constructor remains as a source-compatible no-split constructor and fills these fields with `null`; `LeafOnlyBTreeIndexService` and old B1/B2 tests can keep using it. `SplitCapableBTreeIndexService` validates both segment references before any operation that may allocate a page. A split path cannot guess which segment owns a new page because that would violate the Disk Manager boundary.

`rootPageId` stays stable across root split. `rootLevel` is the caller's metadata view. When a root split changes the actual root level from 0 to 1, the insert result returns an updated descriptor. Later calls must use the returned descriptor or reload metadata from the data dictionary when that module exists.

### `BTreeInsertResult`

B3 extends the insert result with:

- `BTreeIndex indexAfterInsert`
- `boolean splitOccurred`
- `List<PageId> allocatedPages`

`BTreeInsertResult` remains source-compatible by adding an auxiliary two-argument constructor:

- `new BTreeInsertResult(index, recordRef)` delegates to the full constructor with `indexAfterInsert=index`, `splitOccurred=false`, and `allocatedPages=List.of()`

Existing callers that only need the inserted `RecordRef` keep working through the existing constructor and `recordRef` accessor. Split-aware callers use `indexAfterInsert` for subsequent lookup/scan/insert.

### `BTreeIndexService`

The service keeps existing methods and adds a clearer range-scan entry:

- `List<BTreeLookupResult> scan(MiniTransaction mtr, BTreeIndex index, BTreeScanRange range)`

`scanLeaf` remains as a compatibility alias during this slice, but its implementation delegates to `scan`; after B3 it may cross leaf siblings despite the historical method name.

`BTreeIndexService` Javadocs must be updated at the same time: `scanLeaf` can no longer promise "root page only" once B3 is present. Its contract becomes "range scan entry retained for source compatibility; implementations may traverse leaf siblings when the index root is non-leaf".

### `SplitCapableBTreeIndexService`

B3 adds a split-capable implementation next to the existing `LeafOnlyBTreeIndexService`. The leaf-only implementation stays as the B1/B2 regression target and as a narrow no-allocation facade. New B3 tests use `SplitCapableBTreeIndexService`. It depends on:

- `IndexPageAccess`
- `DiskSpaceManager`
- `TypeCodecRegistry`

It still does not start or commit MTRs. The caller owns MTR lifecycle.

## 4. New B+Tree Support Objects

### `IndexPageHandle`

`IndexPageAccess` adds a handle-style method for B+Tree pages:

- `openIndexPageHandle(mtr, pageId, mode)`

The returned object exposes:

- `pageId()`
- `recordPage()`
- `fileHeader()`
- `writeSiblingLinks(prevPageNo, nextPageNo)`

It wraps the same MTR-owned `PageGuard` and does not expose `BufferFrame` or raw byte arrays. Existing `openIndexPage` remains for record-only callers.

This is needed because sibling links live in `FilePageHeader`, while record contents live in `RecordPage`.

`writeSiblingLinks` must not call `PageEnvelope.writeHeader`, because that method rewrites the whole file header and can accidentally reset unrelated fields. B3 adds a narrow `PageEnvelope.writeSiblingLinks(PageGuard guard, long prevPageNo, long nextPageNo)` helper that writes only `PREV_PAGE_NO` and `NEXT_PAGE_NO` through two `PageGuard.writeInt` calls. Those writes still produce `PAGE_BYTES` redo through the MTR write listener.

### `BTreeNodePointer`

A node pointer is an immutable value:

- `SearchKey lowKey`
- `PageId childPageId`

For this slice, each level-1 root record stores the lowest key contained by a leaf child plus the child page id. Child selection uses the greatest `lowKey <= searchKey`; if the search key is lower than the first low key, the first child is selected.

This is a deliberate educational simplification of InnoDB separator records. It still preserves the required invariant that root records cover child key ranges in sorted order, and it keeps B+Tree from parsing record bytes directly.

### `BTreeNodePointerCodec`

The codec converts between `BTreeNodePointer` and record-layer `LogicalRecord` with `RecordType.NODE_POINTER`.

### `BTreeNodePointerSchema`

Root non-leaf pages cannot use `index.schema()` and `index.keyDef()`, because node-pointer records contain key columns plus child-page columns rather than the leaf row payload. B3 therefore adds a derived schema object:

- `TableSchema schema`
- `IndexKeyDef keyDef`
- `int keyColumnCount`
- `int childSpaceColumnOrdinal`
- `int childPageColumnOrdinal`

The node-pointer schema is derived from the leaf index key:

- one column per key part, in key-part order
- `child_space_id BIGINT UNSIGNED`
- `child_page_no BIGINT UNSIGNED`

The derived schema uses `index.schema().schemaVersion()` as its schema version so node-pointer records remain tied to the same metadata snapshot. The derived key definition uses the first `keyColumnCount` columns with `index.indexId()` as its index id, not the original table column ordinals. This prevents child pointer records from depending on non-key payload columns.

`SplitCapableBTreeIndexService` must use this derived `schema/keyDef` for all root non-leaf page operations:

- materializing node pointers
- selecting a child from the root
- inserting a new child pointer into the root
- checking whether root has room for one additional pointer

Leaf pages continue to use `index.schema()` and `index.keyDef()`.

### `SearchKeyComparator`

B3 introduces a pure in-memory comparator for `SearchKey` values. It uses `IndexKeyDef`, `TableSchema`, and `TypeCodecRegistry` to apply the same ASC/DESC, NULL, and type ordering used by page-local record comparison.

It is used for:

- placing a new logical record into a materialized sorted list before split
- choosing a median split point
- sorting and validating node pointers
- computing each leaf child's `lowKey`

Child selection from an actual root page should prefer `RecordPageSearch.findInsertPosition` against the root's derived node-pointer `schema/keyDef`, because that operation already means "last record with key <= target". `SearchKeyComparator` is reserved for in-memory materialized lists, where there is no `RecordPage` to search. This keeps page search and page ordering on the existing record comparator path.

## 5. Data Flow

### Lookup

1. Validate non-null inputs.
2. Open the root with S latch through `IndexPageAccess`.
3. Read the root `IndexPageHeader`.
4. If `level == 0`, search the root leaf exactly as B1/B2 did.
5. If `level == 1`, use `RecordPageSearch.findInsertPosition` on the root's derived node-pointer `schema/keyDef` to choose the last pointer whose `lowKey <= searchKey`.
6. Materialize that selected `NODE_POINTER` record and decode its child page id.
7. Open the chosen leaf with S latch in the same MTR.
8. Validate leaf header `indexId` and `level == 0`.
9. Use `RecordPageSearch.findEqual`.
10. Return a materialized `BTreeLookupResult`, skipping delete-marked records.

If `index.rootLevel()` differs from the actual root header level, lookup throws `BTreeRootChangedException`. This makes stale metadata visible instead of silently using a descriptor that no longer matches disk.

### Range Scan

1. Use the lower bound to descend to the starting leaf.
2. Scan the current leaf in key order.
3. Stop at the upper bound or when `limit` rows have been returned.
4. If the upper bound was not reached, read the current leaf file header and follow `nextPageNo`.
5. Open the next leaf with S latch and continue.
6. Stop when `nextPageNo == FIL_NULL`.

The scan materializes results while each leaf is latched and does not retain `RecordPage`, `RecordCursor`, or page guard references after returning.

### Insert Without Split

If the target leaf has space, B3 keeps the B1/B2 behavior:

1. Descend to target leaf with X latches.
2. For unique indexes, check physical duplicate key in the target leaf.
3. Call `RecordPageInserter.insert`.
4. Return a `BTreeInsertResult` with `splitOccurred=false`.

This duplicate check remains physical. A delete-marked equal key still blocks unique insert until MVCC and purge are connected.

### Root Leaf Split

When the root is a level-0 leaf and insertion overflows:

1. Materialize all non-system root records in key order, including delete-marked records.
2. Insert the new logical record into the in-memory sorted list.
3. Split the list at the median count boundary.
4. Preflight that a freshly formatted root level-1 page can hold the two derived `NODE_POINTER` records.
5. Allocate two new leaf pages from `index.leafSegment`.
6. Format both pages as INDEX level 0 through `IndexPageAccess.createIndexPage`.
7. Reinsert the left half into the left leaf and the right half into the right leaf using `RecordPageInserter`.
8. Set leaf sibling links: left.prev = `FIL_NULL`, left.next = right, right.prev = left, right.next = `FIL_NULL`.
9. Reformat the stable root page as INDEX level 1.
10. Insert two `NODE_POINTER` records into root:
   - left leaf low key -> left child page
   - right leaf low key -> right child page
11. Return `BTreeInsertResult` with `indexAfterInsert.rootLevel=1`, `splitOccurred=true`, and the allocated child pages.

The original root leaf contents are copied into new child leaves before the root is reformatted. This preserves root page id stability without losing the old root rows.

### Level-1 Leaf Split

When the root is level 1 and the target leaf overflows:

1. Materialize the target leaf records and insert the new record into the sorted list.
2. Split at median count.
3. Preflight that the current root has enough free space for one additional derived `NODE_POINTER`; if not, throw `BTreeParentSplitRequiredException`.
4. Allocate one new leaf from `index.leafSegment`.
5. Reformat the old leaf and new leaf as INDEX level 0.
6. Reinsert left half into the old leaf and right half into the new leaf.
7. Update sibling links:
   - old leaf keeps its previous leaf
   - new leaf follows the old leaf
   - old leaf's former next leaf, if any, gets `prev = new leaf`
8. Insert a new node pointer into the level-1 root for the new leaf's low key.

This slice does not split a full level-1 root into a level-2 tree.

The root capacity check must happen before reformatting or modifying the old leaf. MTR rollback does not undo page bytes, so a level-1 insert that cannot fit the new root pointer must fail before any destructive leaf rewrite, new leaf format, or sibling update.

## 6. Page Allocation and MTR Boundary

B3 uses `DiskSpaceManager.allocatePage(mtr, segmentRef)` for all new child pages. The current implementation initializes the page as `PageType.ALLOCATED`. B3 then calls `IndexPageAccess.createIndexPage(mtr, newPageId, indexId, level)` to intentionally reinitialize the newly allocated page as an INDEX page.

This is safe only because the page was just allocated in the same split path. The spec explicitly forbids calling `createIndexPage` on an existing live index page except for the stable root reformat during root split, where the old contents have already been materialized and copied into child leaves.

All modified pages stay in the same caller MTR:

- root page
- old target leaf
- new leaf pages
- neighboring leaf when `prev` link must be updated
- FSP metadata pages touched by allocation

Commit appends the collected physical redo and stamps pageLSN for every touched page. Rollback only releases latches and does not undo page contents, so split code must validate all inputs before destructive reformatting.

Preflight before any destructive reformat must include:

- non-null MTR, index, schema, key definition, and split-required segment references
- root/header index id and level match
- target leaf/header index id and level match
- node-pointer derived schema/keyDef can encode child pointers
- the in-memory materialized record list plus the new record is sorted and non-empty
- for root split, the rebuilt root has enough space for exactly two node pointers
- for level-1 leaf split, the current root has enough space for exactly one additional node pointer

## 7. Concurrency Boundary

B3 uses a conservative latch strategy:

- lookup holds root S and leaf S until MTR commit
- insert holds root X and target leaf X until MTR commit
- split holds every affected page X until MTR commit
- no transaction row-lock wait path exists in this slice

This serializes B+Tree writes at the root and avoids latch-coupling restarts until a later concurrency slice. It is intentionally simpler than the final design and must be documented in class Javadocs.

The intended latch acquisition order for split is:

1. root page
2. target leaf page
3. FSP metadata and newly allocated page through `DiskSpaceManager.allocatePage`
4. new leaf INDEX page format through `IndexPageAccess.createIndexPage`
5. right sibling page only if its `prev` link must be updated

Tests must use a buffer pool large enough to hold root, target leaf, new leaf pages, optional right sibling, and FSP metadata pages fixed in one MTR.

Because there is no row-lock wait, B3 never waits on `LockManager` while holding a page latch.

## 8. Error Model

B3 reuses existing exceptions and adds:

- `BTreeRootChangedException`: caller metadata `rootLevel` does not match the root page header.
- `BTreeParentSplitRequiredException`: level-1 root has no room for an additional child pointer, and height greater than 1 is outside this slice.

`BTreeSplitRequiredException` remains the general "a split would be required but this code path did not perform it" exception. `BTreeParentSplitRequiredException` extends it and is used only for parent/root overflow above the height supported by B3.

Error rules:

- structure mismatch between expected index id and page header throws `BTreeStructureCorruptedException`
- root level greater than 1 throws `BTreeUnsupportedStructureException`
- root pointer page with no node pointers throws `BTreeStructureCorruptedException`
- child page whose level is not 0 throws `BTreeStructureCorruptedException`
- unique duplicate key throws `BTreeDuplicateKeyException`
- parent overflow above height 1 throws `BTreeParentSplitRequiredException`, not silent partial split

All new exceptions extend `DatabaseRuntimeException` through the existing B+Tree exception hierarchy.

## 9. Recovery Boundary

B3 does not add logical split redo.

Crash recovery observes only physical records:

- `PAGE_INIT` for allocated and formatted pages
- `PAGE_BYTES` for root reformat, record inserts, sibling link updates, and node pointer inserts
- pageLSN for idempotent replay

Redo recovery must not run B+Tree search, recompute a split point, check uniqueness, rebuild siblings from logical keys, or allocate pages. If a crash happens after redo append, replay restores exactly the page images produced by the committed MTR.

## 10. Tests

Add `SplitCapableBTreeIndexServiceTest` covering:

- root leaf overflow performs root split and returns `indexAfterInsert.rootLevel == 1`
- lookup finds keys on both child leaves after root split
- range scan follows `FIL_PAGE_NEXT` across child leaves
- root page id remains stable after root split
- child leaf `prev/next` links are consistent
- insert into an existing level-1 tree splits only the target leaf and updates root pointers
- unique duplicate check still rejects an existing key after split, including a duplicate at the median split boundary
- parent overflow above height 1 reports split-required instead of corrupting pages
- full MTR redo grows during split and committed pages have non-zero pageLSN
- physical redo replay of the split state restores lookup/scan-visible structure

Keep the existing B1/B2 tests as regression coverage.

## 11. Non-Goals and Simplifications

- No height greater than 1.
- No parent split beyond the stable root becoming level 1.
- No merge, redistribute, root shrink, or page free.
- No latch coupling, optimistic descent, restart policy, or latch timeout handling.
- No transaction locks, MVCC visibility, undo, ReadView, or purge safety gate.
- No B+Tree-specific redo record or recovery handler.
- No right-split optimization; median split by record count is deterministic and easier to test.
- Node pointers use `lowKey -> childPageId` records rather than a byte-compatible InnoDB node pointer format.
- Root leaf split allocates two new leaf child pages and rebuilds the stable root as non-leaf. This uses one more page than the common InnoDB-style "old root copied to one child plus one new sibling" shape, but it keeps the implementation deterministic for the first split slice.
- `scanLeaf` remains as a compatibility alias even though `scan` may traverse multiple leaves.

## 12. Fifteen-Point Self-Review

1. Scope is B3 only: leaf split and root split to height 1.
2. The design follows B+Tree docs by keeping root page id stable.
3. New pages are allocated through `DiskSpaceManager`, not by B+Tree modifying FSP internals.
4. Sibling links are in `FilePageHeader` and require a handle that can update envelope fields.
5. Page-local records are rebuilt through `RecordPageInserter`, not hand-written by B+Tree.
6. Node pointers are record-layer `LogicalRecord` values with `RecordType.NODE_POINTER`.
7. B+Tree interprets child page semantics only after materialization; it does not parse record bytes.
8. Root split copies old root rows before destructive root reformat.
9. Level-1 leaf split updates old/new/next sibling links.
10. Parent overflow above height 1 is explicitly rejected.
11. Unique checks remain physical and simplified.
12. MTR ownership remains caller-controlled; the service does not commit or rollback.
13. Recovery remains physical `PAGE_INIT/PAGE_BYTES` replay.
14. No row-lock wait is introduced, so page latches are not held across transaction waits.
15. Tests cover split behavior, sibling traversal, redo/pageLSN, and replay-visible structure.
