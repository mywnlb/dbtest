# Recovery READ_ONLY_VALIDATE slice

## Goal

- Implement the `RecoveryMode.READ_ONLY_VALIDATE` path as a small recovery hardening slice.
- A read-only validation recovery scans recovery inputs and publishes diagnostics without modifying data files.
- Successful validation must leave the recovery gate in an explicit read-only diagnostic state, not `OPEN`.

## Design Inputs

- `docs/design/innodb-crash-recovery-design.md` defines `READ_ONLY_VALIDATE` as scan/validate/report only.
- `docs/design/innodb-redo-log-design.md` requires normal redo replay to be physical and idempotent, but read-only validation must not write pages.
- `docs/design/current-implementation-map.md` currently records `RecoveryMode` non-`NORMAL` values as extension points.

## Key Decisions

- Add `RecoveryState.READ_ONLY` to distinguish "validation succeeded" from `CLOSED`, `OPEN`, and `FAILED`.
- Add `RecoveryTrafficGate.enterReadOnlyDiagnostic()`; ordinary write paths remain rejected because they already require `RecoveryState.OPEN`.
- Add `RecoveryStageName.READ_ONLY_DIAGNOSTIC_OPEN` so reports do not reuse `OPEN_TRAFFIC` for a non-writable state.
- Add a `RecoveryRequest` construction path for `READ_ONLY_VALIDATE`, and an `EngineConfig` recovery mode option defaulting to `NORMAL`.
- Add a scan-only doublewrite path that never writes back full-copy pages; full-copy and detect-only hits are diagnostic only in this mode.
- In `CrashRecoveryService`, `READ_ONLY_VALIDATE` reads checkpoint and redo batches, counts/records diagnostics, but does not call `RedoApplyDispatcher.applyAll`.
- In `StorageEngine`, an existing-open `READ_ONLY_VALIDATE` publishes `EngineState.READ_ONLY`, skips background workers/warmup, and `close()` only releases opened handles without `flushThrough`.
- Reject non-`NORMAL` recovery modes on fresh open before formatting redo, doublewrite, or system undo files.

## Non-Goals

- Do not implement `RecoveryMode.FORCE_SKIP_CORRUPT_TABLESPACE`; it needs per-space filtering across doublewrite, redo apply, reconcile, and undo recovery.
- Do not add a full recovery stage registry or large recovery framework refactor.
- Do not implement read-only SQL/session access. This slice only exposes the storage recovery gate state for later callers.
- Do not change `NORMAL` crash recovery behavior or stage ordering.
- Do not make doublewrite detect-only the default production mode; this slice only adds a read-only validation use.

## Expected Flow

- `NORMAL`: existing flow remains `TRAFFIC_CLOSED -> DOUBLEWRITE_REPAIR -> REDO_REPLAY -> ... -> OPEN_TRAFFIC`.
- `READ_ONLY_VALIDATE`:
  - close the gate for recovery;
  - scan doublewrite pages without repair;
  - read checkpoint and scan redo batches;
  - skip redo apply, redo boundary install, undo resume, file reconcile, undo rollback, purge resume, and `forceAll`;
  - enter `READ_ONLY`;
  - return a `RecoveryReport` with mode `READ_ONLY_VALIDATE`, state `READ_ONLY`, `appliedBatchCount == 0`, and stage `READ_ONLY_DIAGNOSTIC_OPEN`.

## Acceptance Tests

- `CrashRecoveryServiceTest`: read-only validation over a broken data page with valid full-copy doublewrite reports the page but does not repair the file.
- `CrashRecoveryServiceTest`: read-only validation over redo records scans to `recoveredToLsn` but does not modify the data page and reports `appliedBatchCount == 0`.
- `RecoveryTrafficGateTest` or equivalent: `enterReadOnlyDiagnostic()` sets state `READ_ONLY` and clears failure.
- `ClusteredDmlServiceTest` or engine-level assertion: `READ_ONLY` is not accepted by write/DML entry points because only `OPEN` allows writes.
- Existing `NORMAL` recovery tests continue to pass unchanged.
- `StorageEngineTest`: existing-open with `EngineConfig.withRecoveryMode(READ_ONLY_VALIDATE)` publishes `EngineState.READ_ONLY`, reports `RecoveryState.READ_ONLY`, rejects ordinary accessors, and closes without a writable lifecycle flush.
- `StorageEngineTest`: fresh open with `READ_ONLY_VALIDATE` fails before creating redo/doublewrite/undo files.

## Current Map Updates

- Update recovery request/report row to mark `READ_ONLY_VALIDATE` implemented and `FORCE_SKIP_CORRUPT_TABLESPACE` still reserved.
- Update recovery orchestration row to describe the non-mutating validation branch.
- Update Reserved / Unwired Production Types row for `RecoveryMode`, removing `READ_ONLY_VALIDATE` from the reserved gap while keeping force-skip as future work.
- Update `storage-backlog.md` 0.7 row to show read-only validate completed, with force-skip still pending.
