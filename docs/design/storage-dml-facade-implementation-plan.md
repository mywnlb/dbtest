# Storage DML Facade Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a storage-layer single-clustered-index DML facade that production-wires transaction id assignment, undo before-images, B+Tree writes, commit durability, rollback, and row-lock release without pulling in SQL executor or Data Dictionary.

**Architecture:** Add `cn.zhangyis.db.storage.api.dml` as the stable facade package. `StorageEngine` constructs one `ClusteredDmlService` from existing storage collaborators and exposes it through `dmlService()`. The facade composes existing `TransactionManager`, `UndoLogManager`, `BTreeCurrentReadService`, `SplitCapableBTreeIndexService`, `RollbackService`, `LockManager`, and `RedoLogManager`; it does not parse SQL and does not access page/frame internals.

**Tech Stack:** Java 25, Gradle 9.5.1, JUnit Jupiter, existing project exceptions, explicit `java.util.concurrent` primitives only, UTF-8.

## Global Constraints

- Follow `AGENTS.md`: read relevant design docs before implementation; update `docs/design/current-implementation-map.md` after the slice.
- Do not introduce SQL/session/DD dependencies into storage internals.
- Do not use `synchronized`, `wait`, `notify`, or `notifyAll`.
- Do not throw bare `IllegalArgumentException` or `RuntimeException` in production code.
- All core public classes, methods, fields, and complex tests need Chinese Javadoc/comments explaining database semantics and simplified differences.
- All DML row-lock waits must happen through `BTreeCurrentReadService`, which releases page latch/buffer fix before `LockManager.acquire`.
- MTR rollback does not undo page content; DML failure boundaries must rely on transaction rollback / recovery rollback and must be tested.
- Use fixed Gradle/JDK for final verification: `$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test`.
- Existing worktree is dirty; do not revert unrelated changes.

---

## File Structure

Create:

| File | Responsibility |
| --- | --- |
| `src/main/java/cn/zhangyis/db/storage/api/dml/ClusteredDmlService.java` | Public facade for single-clustered-index INSERT/UPDATE/DELETE/COMMIT/ROLLBACK orchestration. |
| `src/main/java/cn/zhangyis/db/storage/api/dml/ClusteredInsertCommand.java` | Immutable INSERT input with validation. |
| `src/main/java/cn/zhangyis/db/storage/api/dml/ClusteredUpdateCommand.java` | Immutable UPDATE input with validation. |
| `src/main/java/cn/zhangyis/db/storage/api/dml/ClusteredDeleteCommand.java` | Immutable DELETE input with validation. |
| `src/main/java/cn/zhangyis/db/storage/api/dml/DmlCommitCommand.java` | Immutable COMMIT input with durability policy. |
| `src/main/java/cn/zhangyis/db/storage/api/dml/DmlRollbackCommand.java` | Immutable ROLLBACK input. |
| `src/main/java/cn/zhangyis/db/storage/api/dml/DmlWriteResult.java` | INSERT/UPDATE/DELETE result. |
| `src/main/java/cn/zhangyis/db/storage/api/dml/DmlCommitResult.java` | COMMIT result. |
| `src/main/java/cn/zhangyis/db/storage/api/dml/DmlRollbackResult.java` | ROLLBACK result. |
| `src/main/java/cn/zhangyis/db/storage/api/dml/DmlOperationException.java` | Recoverable DML orchestration exception. |
| `src/main/java/cn/zhangyis/db/storage/api/dml/DmlDuplicateKeyException.java` | Duplicate clustered/unique key exception. |
| `src/test/java/cn/zhangyis/db/storage/api/dml/ClusteredDmlServiceTest.java` | Unit/integration tests for facade behavior using real storage components. |
| `src/test/java/cn/zhangyis/db/storage/api/dml/ClusteredDmlEngineIntegrationTest.java` | StorageEngine-level integration tests for `dmlService()` and durability/lock release. |

Modify:

| File | Responsibility |
| --- | --- |
| `src/main/java/cn/zhangyis/db/storage/engine/StorageEngine.java` | Construct and expose `ClusteredDmlService`; pass `recoveryGate`. |
| `docs/design/current-implementation-map.md` | Update DML facade flow, transaction layer status, btree current-read production caller, and known gaps. |

No planned changes:

| File | Reason |
| --- | --- |
| `EngineConfig.java` | Durability policy is explicit per `DmlCommitCommand` for first phase. |
| `TransactionManager.java` | Existing commit/rollback FSM remains pure; facade composes it. |
| `UndoLogManager.java` | Existing beforeInsert/beforeUpdate/beforeDelete/onCommit are sufficient. |
| `SplitCapableBTreeIndexService.java` | Existing clustered insert/replace/delete-mark APIs are sufficient. |

## Task 1: Command/Result Value Objects

**Files:**

- Create: `src/main/java/cn/zhangyis/db/storage/api/dml/ClusteredInsertCommand.java`
- Create: `src/main/java/cn/zhangyis/db/storage/api/dml/ClusteredUpdateCommand.java`
- Create: `src/main/java/cn/zhangyis/db/storage/api/dml/ClusteredDeleteCommand.java`
- Create: `src/main/java/cn/zhangyis/db/storage/api/dml/DmlCommitCommand.java`
- Create: `src/main/java/cn/zhangyis/db/storage/api/dml/DmlRollbackCommand.java`
- Create: `src/main/java/cn/zhangyis/db/storage/api/dml/DmlWriteResult.java`
- Create: `src/main/java/cn/zhangyis/db/storage/api/dml/DmlCommitResult.java`
- Create: `src/main/java/cn/zhangyis/db/storage/api/dml/DmlRollbackResult.java`
- Create: `src/main/java/cn/zhangyis/db/storage/api/dml/DmlOperationException.java`
- Create: `src/main/java/cn/zhangyis/db/storage/api/dml/DmlDuplicateKeyException.java`
- Test: `src/test/java/cn/zhangyis/db/storage/api/dml/ClusteredDmlServiceTest.java`

**Interfaces:**

- Produces: immutable command/result records consumed by Task 2+.
- Consumes: existing `Transaction`, `BTreeIndex`, `SearchKey`, `LogicalRecord`, `DurabilityPolicy`, `Lsn`, `TransactionId`, `TransactionNo`.

- [ ] **Step 1: Write failing validation tests**

Add tests to `ClusteredDmlServiceTest`:

```java
@Test
@DisplayName("DML command objects reject null required fields")
void commandObjectsRejectNullRequiredFields() {
    Duration timeout = Duration.ofSeconds(1);
    assertThrows(DatabaseValidationException.class,
            () -> new DmlCommitCommand(null, DurabilityPolicy.FLUSH_ON_COMMIT, timeout));
    assertThrows(DatabaseValidationException.class,
            () -> new DmlCommitCommand(transaction(), null, timeout));
    assertThrows(DatabaseValidationException.class,
            () -> new DmlCommitCommand(transaction(), DurabilityPolicy.FLUSH_ON_COMMIT, null));
    assertThrows(DatabaseValidationException.class,
            () -> new DmlRollbackCommand(null, clusteredIndex()));
    assertThrows(DatabaseValidationException.class,
            () -> new DmlRollbackCommand(transaction(), null));
}
```

- [ ] **Step 2: Run targeted test and verify compile failure**

Run: `$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "cn.zhangyis.db.storage.api.dml.ClusteredDmlServiceTest"`

Expected: FAIL to compile because DML command/result types do not exist.

- [ ] **Step 3: Implement command/result records and exceptions**

Use these signatures:

```java
public record ClusteredInsertCommand(Transaction transaction, BTreeIndex index, SearchKey key,
                                     LogicalRecord record, long tableId, Duration lockWaitTimeout) { }
public record ClusteredUpdateCommand(Transaction transaction, BTreeIndex index, SearchKey key,
                                     LogicalRecord newRecord, long tableId, Duration lockWaitTimeout) { }
public record ClusteredDeleteCommand(Transaction transaction, BTreeIndex index, SearchKey key,
                                     long tableId, Duration lockWaitTimeout) { }
public record DmlCommitCommand(Transaction transaction, DurabilityPolicy durabilityPolicy,
                               Duration durabilityTimeout) { }
public record DmlRollbackCommand(Transaction transaction, BTreeIndex clusteredIndex) { }
public record DmlWriteResult(boolean changed, int affectedRows, Lsn endLsn, TransactionId transactionId) { }
public record DmlCommitResult(TransactionNo transactionNo, boolean durable, int releasedLockCount) { }
public record DmlRollbackResult(RollbackSummary rollbackSummary, int releasedLockCount) { }
```

Each compact constructor must validate nulls, clustered index where applicable, positive timeouts, and non-negative table id/index id. Exceptions extend `DatabaseRuntimeException` or `DatabaseValidationException` according to existing project convention.

- [ ] **Step 4: Run targeted tests**

Expected: command validation tests PASS.

## Task 2: ClusteredDmlService Skeleton and StorageEngine Exposure

**Files:**

- Create: `src/main/java/cn/zhangyis/db/storage/api/dml/ClusteredDmlService.java`
- Modify: `src/main/java/cn/zhangyis/db/storage/engine/StorageEngine.java`
- Test: `src/test/java/cn/zhangyis/db/storage/api/dml/ClusteredDmlEngineIntegrationTest.java`

**Interfaces:**

- Consumes: records from Task 1.
- Produces: `StorageEngine.dmlService()` and `ClusteredDmlService` constructor.

- [ ] **Step 1: Write failing engine exposure test**

```java
@Test
@DisplayName("StorageEngine exposes clustered DML facade after open")
void engineExposesDmlFacadeAfterOpen(@TempDir Path dir) {
    StorageEngine engine = new StorageEngine(config(dir));
    engine.open();
    try {
        assertNotNull(engine.dmlService());
    } finally {
        engine.close();
    }
}
```

- [ ] **Step 2: Run targeted test**

Expected: FAIL to compile because `dmlService()` does not exist.

- [ ] **Step 3: Implement skeleton**

`ClusteredDmlService` fields:

```java
private final TransactionManager transactionManager;
private final UndoLogManager undoLogManager;
private final MiniTransactionManager mtrManager;
private final SplitCapableBTreeIndexService btree;
private final BTreeCurrentReadService currentRead;
private final RollbackService rollbackService;
private final LockManager lockManager;
private final RedoLogManager redo;
private final RecoveryTrafficGate recoveryGate;
```

Methods initially throw `new DmlOperationException("... not implemented")`, except constructor validation.

Modify `StorageEngine`:

```java
private ClusteredDmlService dmlService;
```

After existing service construction:

```java
this.dmlService = new ClusteredDmlService(transactionManager, undoLogManager, miniTransactionManager,
        btreeService, btreeCurrentReadService, rollbackService, lockManager, redo, recoveryGate);
```

Add public accessor:

```java
public ClusteredDmlService dmlService() {
    requireOpen();
    return dmlService;
}
```

- [ ] **Step 4: Run targeted engine exposure test**

Expected: PASS.

## Task 3: INSERT Path

**Files:**

- Modify: `ClusteredDmlService.java`
- Test: `ClusteredDmlServiceTest.java`

**Interfaces:**

- Consumes: `ClusteredInsertCommand`.
- Produces: `ClusteredDmlService.insert(ClusteredInsertCommand)`.

- [ ] **Step 1: Write failing INSERT integration test**

Test flow:

1. Create storage fixture with fresh `StorageEngine` and clustered index root.
2. Begin transaction via `engine.transactionManager().begin(TransactionOptions.defaults())`.
3. Call `engine.dmlService().insert(command)`.
4. Lookup via `btree.lookupIncludingDeleted` in a short MTR.
5. Assert row exists, `DB_TRX_ID == txn.transactionId()`, `DB_ROLL_PTR` non-null, affectedRows=1.

- [ ] **Step 2: Run targeted test**

Expected: FAIL because `insert` skeleton throws.

- [ ] **Step 3: Implement INSERT minimal path**

Implementation outline:

```java
public DmlWriteResult insert(ClusteredInsertCommand command) {
    requireOpenForDml();
    Transaction txn = command.transaction();
    TransactionId txnId = transactionManager.assignWriteId(txn);
    BTreeCurrentReadRequest request = new BTreeCurrentReadRequest(
            txnId, txn.options().isolationLevel(), command.lockWaitTimeout(), DEFAULT_RELOCATION_RETRIES);
    BTreeUniqueCheckResult unique = currentRead.checkUniqueForInsert(command.index(), command.key(), request);
    if (unique.duplicate()) {
        throw new DmlDuplicateKeyException("duplicate clustered key for index " + command.index().indexId());
    }
    MiniTransaction mtr = mtrManager.begin();
    try {
        RollPointer rp = undoLogManager.beforeInsert(txn, mtr, command.tableId(), command.index().indexId(),
                command.key().values(), command.index().keyDef(), command.index().schema());
        btree.insertClustered(mtr, command.index(), command.record(), txnId, rp);
        Lsn end = mtrManager.commit(mtr);
        return new DmlWriteResult(true, 1, end, txnId);
    } catch (DatabaseRuntimeException e) {
        mtrManager.rollbackUncommitted(mtr);
        throw e;
    }
}
```

Use `txn.options().isolationLevel()` and `command.key().values()`; these names match current source (`TransactionOptions` and `SearchKey`).

- [ ] **Step 4: Add duplicate insert test**

Insert once, attempt insert same key in another transaction, assert `DmlDuplicateKeyException` and only one physical record.

- [ ] **Step 5: Run targeted tests**

Expected: INSERT and duplicate tests PASS.

## Task 4: UPDATE Path

**Files:**

- Modify: `ClusteredDmlService.java`
- Test: `ClusteredDmlServiceTest.java`

**Interfaces:**

- Consumes: `ClusteredUpdateCommand`.
- Produces: `ClusteredDmlService.update(ClusteredUpdateCommand)`.

- [ ] **Step 1: Write failing UPDATE + rollback test**

Flow:

1. Insert and commit base row.
2. Begin tx2.
3. Call `update` changing non-key payload.
4. Assert lookup sees new payload and hidden columns owned by tx2.
5. Call `rollback(new DmlRollbackCommand(tx2,index))`.
6. Assert lookup sees original payload and original hidden columns.

- [ ] **Step 2: Run targeted test**

Expected: FAIL because update skeleton throws.

- [ ] **Step 3: Implement UPDATE**

Data flow:

1. `assignWriteId`.
2. `currentRead.lockPoint(..., FOR_UPDATE)`.
3. If empty, return `new DmlWriteResult(false, 0, redo.currentLsn(), txnId)`; `Lsn` currently has no `ZERO` constant.
4. Extract old record and old hidden columns from lookup result.
5. Begin MTR.
6. `beforeUpdate` with old column values and old hidden columns.
7. Create `LogicalRecord` with new column values, deleted=false, same record type, hidden `(txnId,rp)`.
8. `btree.replaceClustered` with expected old hidden columns.
9. Commit MTR.

- [ ] **Step 4: Add missing-row update test**

Call update on absent key and assert affectedRows=0, no undo context allocated if possible by checking `txn.undoContext()==null`.

- [ ] **Step 5: Run targeted tests**

Expected: UPDATE tests PASS.

## Task 5: DELETE Path

**Files:**

- Modify: `ClusteredDmlService.java`
- Test: `ClusteredDmlServiceTest.java`

**Interfaces:**

- Consumes: `ClusteredDeleteCommand`.
- Produces: `ClusteredDmlService.delete(ClusteredDeleteCommand)`.

- [ ] **Step 1: Write failing DELETE + rollback test**

Flow:

1. Insert and commit base row.
2. Begin tx2.
3. Call `delete`.
4. Assert normal `lookup` misses but `lookupIncludingDeleted` sees delete-marked row with tx2 hidden columns.
5. Call rollback.
6. Assert row is visible again and delete flag false.

- [ ] **Step 2: Run targeted test**

Expected: FAIL because delete skeleton throws.

- [ ] **Step 3: Implement DELETE**

Data flow:

1. `assignWriteId`.
2. `currentRead.lockPoint(..., FOR_UPDATE)`.
3. If empty, return affectedRows=0.
4. Extract old record and hidden columns.
5. Begin MTR.
6. `beforeDelete` with old column values and old hidden columns.
7. `btree.setClusteredDeleteMark(mtr,index,key,true,new HiddenColumns(txnId,rp), oldTrxId, oldRollPtr)`.
8. Commit MTR.

- [ ] **Step 4: Add missing-row delete test**

Expected: affectedRows=0, no undo context allocated.

- [ ] **Step 5: Run targeted tests**

Expected: DELETE tests PASS.

## Task 6: COMMIT Path with Durability and Lock Release

**Files:**

- Modify: `ClusteredDmlService.java`
- Test: `ClusteredDmlServiceTest.java`
- Test: `ClusteredDmlEngineIntegrationTest.java`

**Interfaces:**

- Consumes: `DmlCommitCommand`.
- Produces: `ClusteredDmlService.commit(DmlCommitCommand)`.

- [ ] **Step 1: Write failing commit releases locks test**

Flow:

1. Tx1 insert/update/delete to acquire lock.
2. Assert `engine.lockManager().snapshot()` has tx1 granted locks.
3. `dml.commit(new DmlCommitCommand(tx1, DurabilityPolicy.FLUSH_ON_COMMIT, timeout))`.
4. Assert transaction state COMMITTED.
5. Assert snapshot has no tx1 locks.
6. Assert commit result `durable==true` and transactionNo non-NONE for read-write transaction.

- [ ] **Step 2: Run targeted test**

Expected: FAIL because commit skeleton throws.

- [ ] **Step 3: Implement COMMIT**

Implementation sequence:

```java
public DmlCommitResult commit(DmlCommitCommand command) {
    requireOpenForDml();
    Transaction txn = command.transaction();
    TransactionId txnId = txn.transactionId();
    boolean transactionCommitted = false;
    try {
        transactionManager.prepareCommit(txn);
        undoLogManager.onCommit(txn);
        transactionManager.commit(txn);
        transactionCommitted = true;
        Lsn commitLsn = redo.currentLsn();
        boolean durable = command.durabilityPolicy().awaitCommitDurable(redo, commitLsn, command.durabilityTimeout());
        if (!durable) {
            throw new DmlOperationException("commit redo did not reach durability policy before timeout");
        }
    } catch (DatabaseRuntimeException e) {
        if (transactionCommitted && !txnId.isNone()) {
            lockManager.releaseAll(txnId);
        }
        throw e;
    }
    int released = txnId.isNone() ? 0 : lockManager.releaseAll(txnId);
    return new DmlCommitResult(txn.transactionNo(), true, released);
}
```

This deliberately separates `UndoLogManager.onCommit` from durability wait failure: onCommit failure means the persistent undo commit marker is not guaranteed, so the transaction stays ACTIVE and keeps row locks; durability wait failure happens after COMMITTED and remains commit-uncertain, so the facade releases row locks and propagates a project exception instead of attempting rollback.

- [ ] **Step 4: Add update/delete history commit test**

After delete commit, run purge driver/coordinator if already available and assert committed delete can be purged, or assert history length via available APIs if exposed. If no public history length exists, verify recovery/commit behavior through existing purge test helper pattern.

- [ ] **Step 5: Run targeted tests**

Expected: COMMIT tests PASS.

## Task 7: ROLLBACK Path and Lock Release

**Files:**

- Modify: `ClusteredDmlService.java`
- Test: `ClusteredDmlServiceTest.java`

**Interfaces:**

- Consumes: `DmlRollbackCommand`.
- Produces: `ClusteredDmlService.rollback(DmlRollbackCommand)`.

- [ ] **Step 1: Write failing rollback release test**

Flow:

1. Tx1 insert/update/delete and hold row locks.
2. Assert locks present.
3. Call `dml.rollback(new DmlRollbackCommand(tx1,index))`.
4. Assert transaction state ROLLED_BACK.
5. Assert locks absent.
6. Assert row state restored according to undo chain.

- [ ] **Step 2: Run targeted test**

Expected: FAIL because rollback skeleton throws.

- [ ] **Step 3: Implement ROLLBACK**

Implementation:

```java
public DmlRollbackResult rollback(DmlRollbackCommand command) {
    requireOpenForDml();
    Transaction txn = command.transaction();
    TransactionId txnId = txn.transactionId();
    try {
        RollbackSummary summary = rollbackService.rollback(txn, command.clusteredIndex());
        int released = txnId.isNone() ? 0 : lockManager.releaseAll(txnId);
        return new DmlRollbackResult(summary, released);
    } catch (DatabaseRuntimeException e) {
        if (!txnId.isNone()) {
            lockManager.releaseAll(txnId);
        }
        throw e;
    }
}
```

If rollback fails, `RollbackService` may leave the transaction in `ROLLING_BACK`; this facade still releases row locks to avoid leaked waits and propagates the project exception for session-level cleanup/retry.

- [ ] **Step 4: Add mixed undo rollback test**

Same transaction does insert -> update -> delete on one row, then rollback. Assert row absent if it was inserted by the same transaction; for committed base row update -> delete -> rollback, assert base row restored.

- [ ] **Step 5: Run targeted tests**

Expected: rollback tests PASS.

## Task 8: Recovery Gate and Exception Boundary Tests

**Files:**

- Modify: `ClusteredDmlService.java`
- Test: `ClusteredDmlServiceTest.java`

**Interfaces:**

- Consumes: `RecoveryTrafficGate`.
- Produces: deterministic DML rejection when gate is not OPEN.

- [ ] **Step 1: Write failing recovery gate test**

Construct `ClusteredDmlService` with a fresh `RecoveryTrafficGate` left CLOSED and minimal fake or real collaborators. Call insert/commit and assert `DmlOperationException` without changing transaction state.

- [ ] **Step 2: Run targeted test**

Expected: FAIL if skeleton does not check gate.

- [ ] **Step 3: Implement `requireOpenForDml`**

```java
private void requireOpenForDml() {
    RecoveryState state = recoveryGate.state();
    if (state != RecoveryState.OPEN) {
        throw new DmlOperationException("DML rejected while recovery gate is " + state);
    }
}
```

- [ ] **Step 4: Add lock timeout propagation test**

Use two transactions and existing `LockManager` patterns to force a row-lock timeout: tx1 locks a point with `FOR_UPDATE`, tx2 calls DML on the same key with a very short `lockWaitTimeout`, assert `LockWaitTimeoutException` propagates, then rollback tx1 and run another lookup/update to prove no page latch/fix was leaked. Deadlock propagation is already covered by `BTreeCurrentReadService`/`LockManager` tests and does not need a new synthetic DML deadlock in this slice.

- [ ] **Step 5: Run targeted tests**

Expected: recovery gate and exception propagation tests PASS.

## Task 9: StorageEngine Integration and Existing Tests

**Files:**

- Modify: `StorageEngine.java`
- Test: `ClusteredDmlEngineIntegrationTest.java`
- Test: existing `StorageEngineTest.java` only if new accessor affects existing fixture.

**Interfaces:**

- Consumes: complete `ClusteredDmlService`.
- Produces: production-held facade reachable from engine.

- [ ] **Step 1: Write engine-level DML smoke test**

Use real `StorageEngine`, configure clustered index before open if needed, insert/update/delete/commit through `engine.dmlService()`, close/reopen, verify committed state survives recovery.

- [ ] **Step 2: Run engine integration test**

Expected: FAIL until all wiring and recovery config are correct.

- [ ] **Step 3: Fix engine wiring details**

Ensure `ClusteredDmlService` is constructed after all collaborators exist and before state OPEN; ensure `dmlService()` calls `requireOpen()`.

- [ ] **Step 4: Run focused storage tests**

Run:

`$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "cn.zhangyis.db.storage.api.dml.*" --tests "cn.zhangyis.db.storage.engine.StorageEngineTest"`

Expected: PASS.

## Task 10: Current Implementation Map Update

**Files:**

- Modify: `docs/design/current-implementation-map.md`

**Interfaces:**

- Consumes: final source code call chains.
- Produces: accurate current map entries.

- [ ] **Step 1: Update Transaction Layer Slice**

Change test-only wording for DML orchestration to show production chain:

`StorageEngine.dmlService -> ClusteredDmlService -> TransactionManager/UndoLogManager/BTreeCurrentReadService/SplitCapableBTreeIndexService/RollbackService/LockManager/RedoLogManager`.

- [ ] **Step 2: Update B+Tree Slice**

Show `BTreeCurrentReadService` now used by `ClusteredDmlService` for INSERT unique check and UPDATE/DELETE lockPoint.

- [ ] **Step 3: Update Engine/Storage API Known Gaps**

Remove or revise “无生产 DML facade”; keep explicit remaining gaps: no SQL executor/DD/session, no secondary indexes, no statement/savepoint rollback, no SERIALIZABLE/RU, no MVCC logical unique.

- [ ] **Step 4: Run doc grep sanity**

Run `rg "无生产 DML facade|test-only" docs/design/current-implementation-map.md` and verify stale phrases are either removed or qualified.

## Task 11: Full Verification

**Files:**

- No production changes unless tests reveal issues.

**Interfaces:**

- Consumes: all prior tasks.
- Produces: verified implementation.

- [ ] **Step 1: Run full test suite**

Run:

`$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test`

Expected: PASS.

- [ ] **Step 2: Inspect git diff**

Run: `git diff -- src/main/java/cn/zhangyis/db/storage/api/dml src/main/java/cn/zhangyis/db/storage/engine/StorageEngine.java docs/design/current-implementation-map.md src/test/java/cn/zhangyis/db/storage/api/dml`

Expected: Only intended DML facade, engine accessor, tests, and current map changes.

- [ ] **Step 3: Check no forbidden primitives**

Run: `rg "synchronized|wait\(|notify\(|notifyAll\(|new RuntimeException|IllegalArgumentException" src/main/java/cn/zhangyis/db/storage/api/dml`

Expected: no matches.

## 20-Angle Plan Self-Review

| # | Check | Result |
| --- | --- | --- |
| 1 | Matches selected scope | PASS: Storage-only single-clustered-index DML facade. |
| 2 | Covers all design goals | PASS: insert/update/delete/commit/rollback tasks present. |
| 3 | Avoids SQL/DD/session | PASS: no planned imports from SQL/session/DD. |
| 4 | Uses storage.api boundary | PASS: new package under `storage.api.dml`. |
| 5 | Keeps StorageEngine small | PASS: only construction/accessor planned. |
| 6 | TDD order | PASS: each task starts with failing tests. |
| 7 | Exact files | PASS: create/modify/test paths listed. |
| 8 | Command/result consistency | PASS: signatures listed once and reused. |
| 9 | Commit ordering | PASS: `TransactionManager.prepareCommit -> UndoLogManager.onCommit -> TransactionManager.commit -> DurabilityPolicy -> releaseAll`. |
| 10 | Rollback ordering | PASS: `RollbackService.rollback` then releaseAll in finally. |
| 11 | Row-lock wait boundary | PASS: all write operations use `BTreeCurrentReadService` before MTR write. |
| 12 | Undo before data | PASS: beforeX occurs before B+Tree modifications in same MTR. |
| 13 | WAL/durability | PASS: no data flush in commit; durability waits redo only. |
| 14 | Recovery gate | PASS: dedicated task and helper. |
| 15 | Exception behavior | PASS: duplicate, lock timeout/deadlock, durability timeout, rollback failure covered. |
| 16 | MTR rollback simplification | PASS: orphan undo / full rollback cleanup called out. |
| 17 | Tests cover hidden columns | PASS: insert/update/delete tests inspect DB_TRX_ID/DB_ROLL_PTR. |
| 18 | current map update | PASS: explicit task. |
| 19 | No placeholders | PASS: no TBD/TODO; lock timeout test is mandatory and deadlock remains covered by lower-level existing tests. |
| 20 | Dirty worktree safety | PASS: plan touches new DML files plus engine/current map only; do not revert unrelated existing changes. |

## Execution Recommendation

Use subagent-driven development by task. Do not start implementation until the design and this plan are approved. Before executing, re-run `git status --short` and treat existing unrelated changes as user/other-agent work.
