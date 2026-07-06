# Recovery Control Plane Slice

## Goal

- Add the first production-wired recovery control-plane surface without introducing SQL/session/DD dependencies.
- Ordinary `StorageEngine` service accessors must require both `EngineState.OPEN` and `RecoveryState.OPEN`.
- Recovery must publish a structured in-memory progress journal so diagnostics can inspect stage starts, completions, and failures.
- `StorageEngine` must expose a read-only recovery diagnostics snapshot that remains available in read-only or failed recovery states.

## Design Inputs

- `innodb-crash-recovery-design.md` requires `RecoveryTrafficGate` to block ordinary sessions during recovery and describes progress/metrics as recovery observability.
- `current-implementation-map.md` currently records that `ClusteredDmlService` checks the gate, but other storage accessors do not.
- `StorageEngine` already exposes `recoveryState()` and `lastRecoveryReport()`; this slice extends that diagnostic surface instead of adding session APIs.

## Key Decisions

- Add `RecoveryProgressJournal`, `RecoveryProgressEvent`, and `RecoveryProgressEventKind` in `storage.recovery`.
- Use an in-memory journal only. It records monotonic sequence, mode, stage, event kind, state, recovered LSN, and detail text.
- Instrument `CrashRecoveryService` around the existing hard-coded stages; do not introduce a full `RecoveryStageRegistry`.
- Add `RecoveryDiagnosticsSnapshot` and `StorageEngine.recoveryDiagnostics()` with gate state, last report, last failure message, and progress events.
- Keep `state()`, `recoveryState()`, `lastRecoveryReport()`, worker state/metrics, and `recoveryDiagnostics()` readable outside user traffic.
- Upgrade ordinary `StorageEngine` accessors through the existing `requireOpen()` guard so they also reject `RecoveryState.RECOVERING`, `READ_ONLY`, `FAILED`, or `CLOSED`.
- Add a package-private `StorageEngine` constructor only for tests to inject a gate/journal; public construction remains unchanged.

## Non-Goals

- Do not implement persistent recovery progress files or cross-crash recovery decisions from the journal.
- Do not implement `FORCE_SKIP_CORRUPT_TABLESPACE`, tablespace discovery, DD/DDL recovery, or prepared transaction recovery.
- Do not add SQL/session blocking or waiting APIs such as `awaitOpen(SessionContext, Duration)`.
- Do not add recovery lock coordinator or worker/lock snapshots in this slice.
- Do not change NORMAL recovery stage ordering or READ_ONLY_VALIDATE mutability rules.

## Acceptance Tests

- `CrashRecoveryServiceTest`: NORMAL recovery records started/completed progress for key stages, including `OPEN_TRAFFIC`.
- `CrashRecoveryServiceTest`: corrupted redo records a failed progress event for `REDO_REPLAY` and leaves the gate failed closed.
- `CrashRecoveryServiceTest` or `StorageEngineTest`: READ_ONLY_VALIDATE records `READ_ONLY_DIAGNOSTIC_OPEN` completion with `RecoveryState.READ_ONLY`.
- `StorageEngineTest`: when the injected recovery gate moves away from `OPEN` after engine open, ordinary accessors reject access while diagnostics remain readable.
- Existing NORMAL and READ_ONLY_VALIDATE recovery tests continue to pass.

## Current Map Updates

- Update the recovery facade/request-report rows to mention the in-memory progress journal and diagnostics snapshot.
- Update the engine bootstrap/global gap rows to show ordinary storage accessors now check the recovery gate.
- Update the backlog `1.9 Recovery/control-plane` row to show gate accessor enforcement and in-memory progress diagnostics as completed, with persistent journal and worker/lock snapshots still pending.
