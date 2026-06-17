# B1/B2 - Leaf-Only B+Tree Lookup and Insert Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the first runnable B+Tree facade for a single root leaf page: point lookup, bounded leaf scan, unique insert without split, and split-required signaling.

**Architecture:** Keep B+Tree as a thin cross-page facade over existing `IndexPageAccess`, `RecordPageSearch`, and `RecordPageInserter`. The first implementation rejects non-leaf roots and does not own MTR commit/rollback, transaction locks, MVCC, or recovery handlers.

**Tech Stack:** Java 25, JUnit Jupiter, fixed JDK `C:\Program Files\Java\jdk-25.0.2`, fixed Gradle `D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1`.

**Project overrides:** no git commits because the repository currently has no valid `HEAD`; TDD required; run GitNexus impact before modifying existing symbols; new symbols do not need impact analysis.

---

## File Structure

- Create `src/main/java/cn/zhangyis/db/storage/btree/BTreeException.java`: base B+Tree runtime exception.
- Create `src/main/java/cn/zhangyis/db/storage/btree/BTreeDuplicateKeyException.java`: unique key violation.
- Create `src/main/java/cn/zhangyis/db/storage/btree/BTreeSplitRequiredException.java`: page full and split needed.
- Create `src/main/java/cn/zhangyis/db/storage/btree/BTreeUnsupportedStructureException.java`: non-leaf/root/scope rejection.
- Create `src/main/java/cn/zhangyis/db/storage/btree/BTreeStructureCorruptedException.java`: page header/index mismatch.
- Create `src/main/java/cn/zhangyis/db/storage/btree/BTreeIndex.java`: immutable index descriptor.
- Create `src/main/java/cn/zhangyis/db/storage/btree/BTreeScanRange.java`: bounded leaf scan descriptor.
- Create `src/main/java/cn/zhangyis/db/storage/btree/BTreeLookupResult.java`: materialized lookup/scan result.
- Create `src/main/java/cn/zhangyis/db/storage/btree/BTreeInsertResult.java`: insert result.
- Create `src/main/java/cn/zhangyis/db/storage/btree/BTreeIndexService.java`: facade interface.
- Create `src/main/java/cn/zhangyis/db/storage/btree/LeafOnlyBTreeIndexService.java`: first implementation.
- Create `src/test/java/cn/zhangyis/db/storage/btree/LeafOnlyBTreeIndexServiceTest.java`: integration tests over real page store, buffer pool, MTR, and record page.
- Modify `src/main/java/cn/zhangyis/db/storage/btree/package-info.java`: replace the one-line English comment with Chinese module docs for the leaf-only first slice.

## Task B1: API and Read Path

- [x] **Step 1: Write failing lookup and scan tests**
  - Add `LeafOnlyBTreeIndexServiceTest` with helper setup copied from `IndexPageAccessTest`: temporary `FileChannelPageStore`, `LruBufferPool`, `IndexPageAccess`, and `MiniTransactionManager`.
  - Test `lookupReturnsEmptyWhenKeyMissing`.
  - Test `scanLeafReturnsBoundedRowsInKeyOrder`.

- [x] **Step 2: Run read-path tests and verify failure**
  - Command:
    ```powershell
    $env:JAVA_HOME='C:\Program Files\Java\jdk-25.0.2'; & 'D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat' test --tests 'cn.zhangyis.db.storage.btree.LeafOnlyBTreeIndexServiceTest' --console=plain
    ```
  - Expected: compile failure because `BTreeIndex`, `BTreeScanRange`, `BTreeIndexService`, and `LeafOnlyBTreeIndexService` do not exist.

- [x] **Step 3: Implement minimal API and read path**
  - Add records/classes listed in File Structure.
  - `LeafOnlyBTreeIndexService.lookup` opens root with `PageLatchMode.SHARED`, validates `header.indexId()` and `header.level()`, calls `RecordPageSearch.findEqual`, materializes a result, and returns `Optional.empty()` for missing or delete-marked records.
  - `LeafOnlyBTreeIndexService.scanLeaf` opens root with `SHARED`, iterates `recordOffsetsInOrder`, compares with `RecordComparator`, skips delete-marked records, honors both inclusive flags, and caps at `limit`.

- [x] **Step 4: Run read-path tests and verify pass**
  - Command: same fixed Gradle command from Step 2.
  - Expected: tests pass.

## Task B2: Insert Without Split

- [x] **Step 1: Add failing insert tests**
  - Add `insertThroughBTreeEmitsRedoAndCanBeLookedUp`.
  - Add `uniqueInsertRejectsDuplicatePhysicalKey`.

- [x] **Step 2: Run insert tests and verify failure**
  - Command:
    ```powershell
    $env:JAVA_HOME='C:\Program Files\Java\jdk-25.0.2'; & 'D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat' test --tests 'cn.zhangyis.db.storage.btree.LeafOnlyBTreeIndexServiceTest' --console=plain
    ```
  - Expected: failure because `insert` is not implemented or duplicate exception classes are missing.

- [x] **Step 3: Implement insert path**
  - `LeafOnlyBTreeIndexService.insert` opens root with `PageLatchMode.EXCLUSIVE`.
  - For unique indexes, use `RecordPageSearch.findEqual` before insert; any matching physical key throws `BTreeDuplicateKeyException`.
  - Call `RecordPageInserter.insert`.
  - Catch `RecordPageOverflowException` and throw `BTreeSplitRequiredException` with the original exception as cause.

- [x] **Step 4: Run insert tests and verify pass**
  - Command: same fixed Gradle command from Step 2.
  - Expected: tests pass.

## Task B3: Unsupported Structure and Overflow

- [x] **Step 1: Add failing boundary tests**
  - Add `nonLeafRootIsRejected`.
  - Add `leafOverflowIsReportedAsSplitRequired`.

- [x] **Step 2: Run boundary tests and verify failure**
  - Command:
    ```powershell
    $env:JAVA_HOME='C:\Program Files\Java\jdk-25.0.2'; & 'D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat' test --tests 'cn.zhangyis.db.storage.btree.LeafOnlyBTreeIndexServiceTest' --console=plain
    ```
  - Expected: failure until non-leaf rejection and overflow mapping are complete.

- [x] **Step 3: Implement boundary handling**
  - Reject `index.rootLevel() != 0` before touching pages.
  - Reject root pages whose `IndexPageHeader.level() != 0` or `indexId()` mismatches the descriptor.
  - Keep overflow mapping in insert path.

- [x] **Step 4: Run boundary tests and verify pass**
  - Command: same fixed Gradle command from Step 2.
  - Expected: tests pass.

## Task B4: Regression and Closeout

- [x] **Step 1: Run targeted btree/record/api tests**
  - Command:
    ```powershell
    $env:JAVA_HOME='C:\Program Files\Java\jdk-25.0.2'; & 'D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat' test --tests 'cn.zhangyis.db.storage.btree.*' --tests 'cn.zhangyis.db.storage.record.page.*' --tests 'cn.zhangyis.db.storage.api.IndexPageAccessTest' --console=plain
    ```
  - Expected: pass.

- [x] **Step 2: Run full suite**
  - Command:
    ```powershell
    $env:JAVA_HOME='C:\Program Files\Java\jdk-25.0.2'; & 'D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat' clean test --console=plain
    ```
  - Expected: pass.

- [x] **Step 3: Scan for forbidden production patterns**
  - Command:
    ```powershell
    rg -n "\bsynchronized\b|\b(wait|notify|notifyAll)\s*\(|\bIllegalArgumentException\b|new RuntimeException|throw new RuntimeException|\bTODO\b|\bTBD\b" src\main\java\cn\zhangyis\db\storage\btree src\test\java\cn\zhangyis\db\storage\btree docs\superpowers\specs\2026-06-17-btree-leaf-lookup-insert-design.md docs\superpowers\plans\2026-06-17-btree-leaf-lookup-insert.md
    ```
  - Expected: no production/test hit except intentional prose if any; any hit must be reviewed and removed unless it is a false positive in documentation.

- [x] **Step 4: GitNexus closeout**
  - Run `gitnexus_detect_changes({repo:"dbtest", scope:"all"})`; if it fails because `HEAD` is missing, record the exact failure.
  - Run:
    ```powershell
    npx gitnexus analyze --force
    ```
  - Expected: repository indexed successfully.

- [x] **Step 5: Update progress memory**
  - Update `C:\Users\李波\.claude\projects\C--coding-java-self-dbtest-dbtest\memory\storage-build-sequence.md`.
  - Update `C:\Users\李波\.claude\projects\C--coding-java-self-dbtest-dbtest\memory\MEMORY.md`.

## Self-Review

Spec coverage: tasks cover API, lookup, scan, insert, duplicate, overflow, unsupported non-leaf, regression, GitNexus, and memory update.

Placeholder scan: no placeholder work remains; split/merge/MVCC are explicitly out of scope.

Type consistency: plan consistently uses `BTreeIndex`, `BTreeScanRange`, `BTreeLookupResult`, `BTreeInsertResult`, `BTreeIndexService`, and `LeafOnlyBTreeIndexService`.
