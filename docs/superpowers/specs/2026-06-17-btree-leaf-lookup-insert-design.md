# B1/B2 - Leaf-Only B+Tree Lookup and Insert Design

## 1. Scope

This slice implements the first runnable `storage.btree` facade over the existing record page, MTR, redo, buffer pool, and page envelope work.

It intentionally supports only a single root page that is also a leaf page:

- `rootLevel == 0`
- point lookup by index key
- bounded scan inside that one leaf page
- insert without split
- duplicate-key rejection for unique indexes
- page-overflow translation into a B+Tree split-required signal

It does not implement non-leaf child pointers, leaf sibling traversal, split, merge, root split, transaction locks, MVCC visibility, undo, or a B+Tree-specific redo handler. Structural recovery still relies on existing physical `PAGE_INIT` and `PAGE_BYTES` redo records.

## 2. Design References

This spec is constrained by:

- `docs/design/innodb-storage-engine-overview.md`: global order says B+Tree lookup/insert follows record and MTR/redo.
- `docs/design/innodb-btree-design.md`: B+Tree owns cross-page structure, while record owns page-local format and search.
- `docs/design/innodb-crash-recovery-design.md`: recovery must not execute logical search/split; this slice adds no recovery logic.
- `docs/design/innodb-transaction-mvcc-design.md`: current-read waits must release page latch and relocate; this slice avoids transaction-lock waits entirely.
- `docs/design/innodb-record-design.md`: record page search/inserter own page directory, next-record, and encoded comparison.
- `docs/design/innodb-disk-manager-design.md`: root pages are created through existing page allocation and `IndexPageAccess`.
- `docs/design/innodb-buffer-pool-design.md`: B+Tree must use controlled page guards via MTR, not `BufferFrame`.
- `docs/design/innodb-redo-log-design.md`: all persistent page writes must flow through MTR redo collection.
- `docs/design/innodb-flush-checkpoint-doublewrite-design.md`: B+Tree does not bypass WAL-gated flush.

## 3. Public API

Production code is added under `cn.zhangyis.db.storage.btree`.

### `BTreeIndex`

Immutable index descriptor:

- `long indexId`
- `PageId rootPageId`
- `int rootLevel`
- `IndexKeyDef keyDef`
- `TableSchema schema`
- `boolean unique`

`BTreeIndex` accepts `rootLevel >= 0` for future non-leaf phases. `LeafOnlyBTreeIndexService` rejects non-zero levels with `BTreeUnsupportedStructureException`.

### `BTreeScanRange`

Immutable bounded leaf scan input:

- `SearchKey lowerKey`
- `boolean lowerInclusive`
- `SearchKey upperKey`
- `boolean upperInclusive`
- `int limit`

This first slice requires both lower and upper keys. Open-ended and cross-leaf scans are deferred.

### `BTreeLookupResult`

Immutable materialized lookup/scan result:

- `BTreeIndex index`
- `RecordRef recordRef`
- `LogicalRecord record`

The result copies record bytes through `RecordCursor.materialize()` while the page latch is still held. It does not retain a page latch, buffer fix, `RecordPage`, or `RecordCursor`.

### `BTreeInsertResult`

Immutable insert result:

- `BTreeIndex index`
- `RecordRef recordRef`

The returned `RecordRef` is a short-lived page-local locator. Later reorganize/split phases can invalidate it; callers must relocate by key for long-lived access.

### `BTreeIndexService`

Facade methods:

- `Optional<BTreeLookupResult> lookup(MiniTransaction mtr, BTreeIndex index, SearchKey key)`
- `List<BTreeLookupResult> scanLeaf(MiniTransaction mtr, BTreeIndex index, BTreeScanRange range)`
- `BTreeInsertResult insert(MiniTransaction mtr, BTreeIndex index, LogicalRecord record)`

The service never starts or commits an MTR. The caller owns the MTR lifecycle so B+Tree can compose with future transaction and split logic.

### `LeafOnlyBTreeIndexService`

First implementation. It depends on:

- `IndexPageAccess`
- `TypeCodecRegistry`
- `RecordPageSearch`
- `RecordPageInserter`
- `RecordComparator`

It uses `IndexPageAccess.openIndexPage(mtr, rootPageId, SHARED)` for lookup/scan and `EXCLUSIVE` for insert. It validates the page header `indexId` and `level` before interpreting data.

## 4. Data Flow

### Point Lookup

1. Validate non-null inputs and `rootLevel == 0`.
2. Open the root page through `IndexPageAccess` with S latch in the caller MTR.
3. Validate page header `indexId == index.indexId()` and `level == 0`.
4. Use `RecordPageSearch.findEqual`.
5. If no offset exists, return `Optional.empty()`.
6. Build `RecordCursor`, skip delete-marked records, materialize a `LogicalRecord`, and return `BTreeLookupResult`.

Delete-marked records are not returned by user-facing lookup in this slice. There is no MVCC old-version construction yet.

### Leaf Scan

1. Validate range keys and `limit >= 0`.
2. Open the root leaf with S latch.
3. Iterate `RecordPage.recordOffsetsInOrder()`.
4. For each conventional non-deleted record, compare it to lower and upper bounds with `RecordComparator`.
5. Skip below-lower records.
6. Stop when the record passes the upper bound.
7. Materialize up to `limit` results.

`limit == 0` returns an empty immutable list without interpreting records beyond page/header validation.

### Insert Without Split

1. Validate non-null inputs and `rootLevel == 0`.
2. Open the root leaf with X latch.
3. Validate page header `indexId` and `level`.
4. If `index.unique()` and `RecordPageSearch.findEqual` finds any physical matching key, throw `BTreeDuplicateKeyException`.
5. Call `RecordPageInserter.insert`.
6. If `RecordPageOverflowException` is thrown, wrap it as `BTreeSplitRequiredException`.
7. Return `BTreeInsertResult`.

Duplicate checking is physical in this slice. A delete-marked equal key still blocks unique insert because transaction/MVCC and purge are not connected yet.

## 5. Error Model

All B+Tree exceptions extend `DatabaseRuntimeException`.

- `BTreeException`: module base class.
- `BTreeDuplicateKeyException`: unique insert found an existing physical equal key.
- `BTreeSplitRequiredException`: leaf lacks space; future split phase should handle it.
- `BTreeUnsupportedStructureException`: non-leaf root or unsupported scan shape.
- `BTreeStructureCorruptedException`: page header does not match the expected index/level.

Validation errors that are simple caller argument issues use `DatabaseValidationException`.

## 6. Concurrency and Recovery Boundaries

This slice has no transaction-lock wait path. Therefore it never waits on `LockManager` while holding a page latch.

All page access is owned by the caller's MTR:

- lookup/scan: S latch, no redo if no writes occur
- insert: X latch, writes through `RecordPageInserter`, redo collected by the MTR-owned `PageGuard`

Recovery does not gain any B+Tree-specific logical handler. `PAGE_INIT` and `PAGE_BYTES` replay restore leaf root pages physically; pageLSN controls idempotence.

## 7. Tests

Add `LeafOnlyBTreeIndexServiceTest` covering:

- lookup returns empty for a missing key
- insert through B+Tree emits redo and later lookup returns the row
- scan returns sorted bounded results from one root leaf
- unique duplicate insert throws `BTreeDuplicateKeyException`
- filling the leaf until record insertion overflows throws `BTreeSplitRequiredException`
- non-leaf `rootLevel > 0` throws `BTreeUnsupportedStructureException`

All tests use temporary `PageStore`, `LruBufferPool`, `IndexPageAccess`, and `MiniTransactionManager`, following existing `IndexPageAccessTest` setup.

## 8. Fifteen-Point Self-Review

1. Scope is a single root leaf, not a full B+Tree.
2. The spec follows the overview's next implementation step.
3. Non-leaf search and child pointers are explicitly rejected.
4. Split and root split are represented only by `BTreeSplitRequiredException`.
5. Range scan is page-local and bounded, not sibling traversal.
6. B+Tree does not parse record bytes directly.
7. B+Tree does not access `BufferFrame` or `PageStore`.
8. B+Tree does not modify FSP/XDES/INODE metadata.
9. B+Tree service does not create, commit, or roll back MTRs.
10. Insert writes stay inside MTR and existing redo collection.
11. Recovery stays physical and pageLSN-idempotent.
12. Transaction/MVCC lock waits are deferred, not stubbed.
13. Duplicate checking is explicitly physical and simplified.
14. Exceptions use project domain exceptions.
15. Tests cover read, write, ordering, duplicate, unsupported structure, and overflow behavior.
