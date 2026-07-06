# Recovery Persistent Progress Journal Slice

## Goal

- Persist recovery progress events to a small append-only diagnostic file.
- Keep the existing in-memory `RecoveryProgressJournal` as the runtime snapshot source for `StorageEngine.recoveryDiagnostics()`.
- Preserve the design rule that progress records are for diagnosis only, not for skipping or deciding recovery work.

## Context

- `innodb-crash-recovery-design.md` defines `RecoveryProgressJournal` as memory plus optional file records used for diagnostics.
- `current-implementation-map.md` currently marks in-memory progress diagnostics as implemented and persistent progress journal as a remaining gap.
- `StorageEngine` already owns the recovery journal and constructs `CrashRecoveryService` with it.

## Decisions

- Add a small `RecoveryProgressSink` port in `storage.recovery`.
- Add `FileRecoveryProgressSink` that appends one UTF-8 JSON line per `RecoveryProgressEvent`.
- Add `EngineConfig.recoveryProgressFile()` as the default file layout under `baseDir`.
- Public `StorageEngine` construction uses a persistent journal by default; package-private test injection can still pass an in-memory journal.
- File append failures are project runtime exceptions. Recovery must remain fail-closed if progress persistence fails.

## Non-Goals

- Do not parse the persistent journal during startup.
- Do not use the journal to resume, skip, or reorder recovery stages.
- Do not add binary record CRC, rotation, truncation, compaction, or retention policy.
- Do not add worker resume diagnostics, lock snapshots, session/executor gate wiring, or force recovery behavior.

## Acceptance Tests

- `RecoveryProgressJournalTest`: persistent journal appends JSONL events in sequence with mode, stage, kind, state, and recovered LSN.
- `CrashRecoveryServiceTest`: normal recovery writes persistent STARTED/COMPLETED stage events.
- `CrashRecoveryServiceTest`: corrupted redo writes a persistent FAILED `REDO_REPLAY` event before fail-closed startup error.
- `StorageEngineTest`: existing-open recovery creates the default progress file under `baseDir`.
- Existing in-memory diagnostics tests still pass.

## Current Map Updates

- Update recovery package rows to mention file-backed progress sink and default engine wiring.
- Update global gaps/backlog so persistent progress journal is no longer listed as missing in 1.9.
- Keep worker resume diagnostics, recovery lock/wait snapshots, and session/executor gate wiring listed as remaining gaps.
