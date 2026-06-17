# F2 - page cleaner, adaptive flush, and tablespace drain Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a flush service facade, deterministic adaptive flush policy, tablespace drain, and a controllable page cleaner worker over the existing F1/R2 primitives.

**Architecture:** Keep `FlushCoordinator` as the only component that writes page images. Add `FlushService` as the orchestration facade that evaluates redo capacity pressure, invokes flush cycles, advances checkpoint, and drains a tablespace by filtering dirty candidates. Add `PageCleanerWorker` as a small background worker using explicit locks/conditions and bounded requests.

**Tech Stack:** Java 25, JUnit Jupiter, fixed JDK `C:\Program Files\Java\jdk-25.0.2`, fixed Gradle `D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1`.

**Project overrides:** no git commits; TDD required; no `synchronized`; no bare runtime exceptions; Chinese Javadocs; run GitNexus impact before editing existing symbols.

---

## File Structure

- Add production: `AdaptiveFlushPolicy`, `FlushAdvice`, `FlushCycleResult`, `TablespaceDrainResult`, `PageCleanerWorker`, `PageCleanerState`, `PageCleanerStoppedException`, `FlushService`.
- Add tests: `AdaptiveFlushPolicyTest`, `FlushServiceCapacityTest`, `FlushServiceDrainTest`, `PageCleanerWorkerTest`.
- Modify docs/memory only after verification.

## Task F2-1: adaptive flush policy

- [x] **Step 1: Add failing tests**
  - Add `AdaptiveFlushPolicyTest` for NONE/ASYNC/SYNC/HARD mapping and invalid config rejection.

- [x] **Step 2: Run test and verify failure**
  - Command: fixed Gradle `test --tests "cn.zhangyis.db.storage.flush.AdaptiveFlushPolicyTest" --console=plain`.
  - Expected: compile failure because policy/advice classes do not exist.

- [x] **Step 3: Implement policy**
  - `AdaptiveFlushPolicy.fixed(minBatch, maxBatch)` validates `1 <= min <= max`.
  - `plan(decision, maxPages)` clamps planned pages by request max and returns target LSN from decision.
  - NONE returns pages `0`.

- [x] **Step 4: Run F2-1 test**
  - Expected: pass.

## Task F2-2: FlushService capacity cycles

- [x] **Step 1: Add failing tests**
  - Add `FlushServiceCapacityTest`: HARD/SYNC pressure flushes dirty pages and advances checkpoint; NONE does not flush.

- [x] **Step 2: Run test and verify failure**
  - Command: fixed Gradle `test --tests "cn.zhangyis.db.storage.flush.FlushServiceCapacityTest" --console=plain`.
  - Expected: compile failure because `FlushService` and `FlushCycleResult` do not exist.

- [x] **Step 3: Implement FlushService capacity path**
  - Constructor takes `BufferPool`, `FlushCoordinator`, `CheckpointCoordinator`, `RedoLogManager`, `RedoCapacityPolicy`, `AdaptiveFlushPolicy`.
  - `flushForCapacity(maxPages)` evaluates capacity from `redo.currentLsn()` and `checkpoint.lastCheckpointLsn()`, plans pages, flushes if pages > 0, advances checkpoint, returns result.

- [x] **Step 4: Run F2-2 test**
  - Expected: pass.

## Task F2-3: tablespace drain

- [x] **Step 1: Add failing tests**
  - Add `FlushServiceDrainTest`: drain target space only; timeout returns timedOut when a target page remains fixed or redo is not durable.

- [x] **Step 2: Run test and verify failure**
  - Command: fixed Gradle `test --tests "cn.zhangyis.db.storage.flush.FlushServiceDrainTest" --console=plain`.
  - Expected: compile/test failure because `drainTablespace` is not implemented.

- [x] **Step 3: Implement drain**
  - `drainTablespace(spaceId, timeout)` loops until target-space dirty candidates are empty or deadline passes.
  - Uses `FlushCoordinator.singlePageFlush(pageId)` for each target page and calls checkpoint advance after each loop.
  - Returns `TablespaceDrainResult(spaceId, results, timedOut, checkpointLsn)`.

- [x] **Step 4: Run F2-3 test**
  - Expected: pass.

## Task F2-4: page cleaner worker

- [x] **Step 1: Add failing tests**
  - Add `PageCleanerWorkerTest`: request triggers background flush; stop transitions to STOPPED; request after stop throws `PageCleanerStoppedException`.

- [x] **Step 2: Run test and verify failure**
  - Command: fixed Gradle `test --tests "cn.zhangyis.db.storage.flush.PageCleanerWorkerTest" --console=plain`.
  - Expected: compile failure because worker classes do not exist.

- [x] **Step 3: Implement worker**
  - Worker uses `ReentrantLock` + `Condition`, not Java monitors.
  - `requestFlush(maxPages)` enqueues bounded work.
  - `awaitIdle(timeout)` returns false on timeout.
  - `stop(timeout)` requests shutdown and joins worker thread within timeout.

- [x] **Step 4: Run F2-4 test**
  - Expected: pass.

## Task F2-5: regression and closeout

- [x] **Step 1: Run targeted package tests**
  - Command: fixed Gradle `test --tests "cn.zhangyis.db.storage.flush.*" --tests "cn.zhangyis.db.storage.redo.*" --console=plain`.
  - Expected: pass.

- [x] **Step 2: Run full test suite**
  - Command: fixed Gradle `clean test --console=plain`.
  - Expected: pass.

- [x] **Step 3: GitNexus closeout**
  - Run `gitnexus_detect_changes({repo:"dbtest", scope:"all"})`; if it fails due missing `HEAD`, record the exact failure.
  - Run `npx gitnexus analyze --force` and record node/edge/flow counts.

- [x] **Step 4: Update progress memory**
  - Update storage build sequence memory with F2 completed scope, verification, and remaining simplifications.

## Self-Review

Spec coverage: adaptive pressure, flush service, drain, and page cleaner worker each map to a task and tests.

Placeholder scan: no TODO/TBD/fill-later placeholders.

Type consistency: plan consistently uses `AdaptiveFlushPolicy`, `FlushAdvice`, `FlushService`, `FlushCycleResult`, `TablespaceDrainResult`, `PageCleanerWorker`, and `PageCleanerStoppedException`.
