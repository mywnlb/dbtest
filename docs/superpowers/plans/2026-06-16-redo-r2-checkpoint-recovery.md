# R2 - persistent checkpoint label + recovery startup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Persist fuzzy checkpoint labels, use them during redo recovery scanning, expose redo capacity pressure, and add a minimal crash recovery startup facade.

**Architecture:** Keep R1 redo data file format stable and add a separate redo control file for checkpoint labels. Keep F1 checkpoint backward-compatible by adding an optional checkpoint store constructor. Recovery startup only composes existing doublewrite and redo replay APIs; it does not implement tablespace discovery, DDL recovery, transaction rollback, or purge.

**Tech Stack:** Java 25, JUnit Jupiter, fixed JDK `C:\Program Files\Java\jdk-25.0.2`, fixed Gradle `D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1`.

**Project overrides:** no git commits; TDD required; no `synchronized`; no bare runtime exceptions; Chinese Javadocs; run GitNexus impact before editing existing symbols.

---

## File Structure

- Add production: `RedoCheckpointLabel`, `RedoCheckpointStore`, `RedoCapacityPressure`, `RedoCapacityDecision`, `RedoCapacityPolicy`.
- Modify production: `RedoRecoveryReader`, `CheckpointCoordinator`.
- Add production under `storage.recovery`: `RecoveryMode`, `RecoveryState`, `RecoveryStageName`, `RecoveryTrafficGate`, `RecoveryRequest`, `RecoveryReport`, `CrashRecoveryService`, `RecoveryStartupException`.
- Add tests: `RedoCheckpointStoreTest`, `RedoCheckpointRecoveryReaderTest`, `CheckpointCoordinatorPersistentTest`, `RedoCapacityPolicyTest`, `CrashRecoveryServiceTest`.

## Task R2-1: RED tests for checkpoint label and reader filtering

- [x] **Step 1: Add failing tests**
  - `RedoCheckpointStoreTest` covers empty control, latest-valid slot selection, and single-slot corruption tolerance.
  - `RedoCheckpointRecoveryReaderTest` covers checkpoint filtering old batches.

- [x] **Step 2: Run tests and verify failure**
  - Command: fixed Gradle `test --tests "cn.zhangyis.db.storage.redo.RedoCheckpointStoreTest" --tests "cn.zhangyis.db.storage.redo.RedoCheckpointRecoveryReaderTest" --console=plain`.
  - Expected: compile failure because checkpoint store/label and checkpoint reader constructor do not exist.

- [x] **Step 3: Implement checkpoint label/store and reader filter**
  - `RedoCheckpointStore.open(path)` creates parent directory and reads/writes two fixed-size slots.
  - `readLatest()` returns `RedoCheckpointLabel.initial()` when no valid slot exists.
  - `write(label)` alternates slots and force-writes the control file.
  - `RedoRecoveryReader(repository, checkpointLsn)` filters out batches whose range end is not greater than checkpoint.

- [x] **Step 4: Run R2-1 tests**
  - Expected: both tests pass.

## Task R2-2: checkpoint persistence and capacity pressure

- [x] **Step 1: Add failing tests**
  - `CheckpointCoordinatorPersistentTest` verifies safe checkpoint writes a label and never persists a lower LSN.
  - `RedoCapacityPolicyTest` verifies NONE/ASYNC/SYNC/HARD thresholds and validation failures.

- [x] **Step 2: Run tests and verify failure**
  - Command: fixed Gradle `test --tests "cn.zhangyis.db.storage.flush.CheckpointCoordinatorPersistentTest" --tests "cn.zhangyis.db.storage.redo.RedoCapacityPolicyTest" --console=plain`.
  - Expected: compile failure because optional store constructor and capacity policy do not exist.

- [x] **Step 3: Implement persistence hook and pressure policy**
  - Add `CheckpointCoordinator(BufferPool, RedoLogManager, RedoCheckpointStore)` while preserving existing constructor.
  - Persist `RedoCheckpointLabel` only when `lastCheckpointLsn` advances.
  - Add `RedoCapacityPolicy.fixed(capacityBytes)` with thresholds 50% async, 75% sync, 90% hard.

- [x] **Step 4: Run R2-2 tests**
  - Expected: both tests pass.

## Task R2-3: crash recovery startup facade

- [x] **Step 1: Add failing tests**
  - `CrashRecoveryServiceTest` verifies stage order, doublewrite repair before redo replay, checkpoint-aware replay, and gate fail-closed on redo corruption.

- [x] **Step 2: Run test and verify failure**
  - Command: fixed Gradle `test --tests "cn.zhangyis.db.storage.recovery.CrashRecoveryServiceTest" --console=plain`.
  - Expected: compile failure because recovery facade classes do not exist.

- [x] **Step 3: Implement recovery facade**
  - `RecoveryTrafficGate` exposes close/open/fail state with explicit `ReentrantLock`.
  - `RecoveryRequest` carries mode, checkpoint store, redo repository, dispatcher/context, optional doublewrite scanner, and pages to repair.
  - `CrashRecoveryService.recover()` runs `DOUBLEWRITE_REPAIR` before `REDO_REPLAY`, opens gate only after success, and records `RecoveryReport`.

- [x] **Step 4: Run R2-3 tests**
  - Expected: recovery test passes.

## Task R2-4: regression and closeout

- [x] **Step 1: Run targeted package tests**
  - Command: fixed Gradle `test --tests "cn.zhangyis.db.storage.redo.*" --tests "cn.zhangyis.db.storage.flush.*" --tests "cn.zhangyis.db.storage.recovery.*" --console=plain`.
  - Expected: pass.

- [x] **Step 2: Run full test suite**
  - Command: fixed Gradle `clean test --console=plain`.
  - Expected: pass.

- [x] **Step 3: GitNexus closeout**
  - Run `gitnexus_detect_changes({repo:"dbtest", scope:"all"})`; if it fails due missing `HEAD`, record the exact failure.
  - Run `npx gitnexus analyze --force` and record node/edge/flow counts.

- [x] **Step 4: Update progress memory**
  - Update storage build sequence memory with R2 completed scope, verification, and remaining simplifications.

## Self-Review

Spec coverage: checkpoint label persistence, checkpoint-aware recovery scan, capacity pressure, and startup recovery ordering each map to a task and test.

Placeholder scan: no TODO/TBD/fill-later placeholders.

Type consistency: plan uses `RedoCheckpointStore`, `RedoCheckpointLabel`, `RedoCapacityPolicy`, `CrashRecoveryService`, `RecoveryRequest`, and `RecoveryReport` consistently across tasks.
