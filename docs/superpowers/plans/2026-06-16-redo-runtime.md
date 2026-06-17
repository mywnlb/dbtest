# R1 — redo runtime + recovery page replay Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the smallest durable redo runtime: write/fsync redo batches, expose `flushedToDiskLsn`/`waitFlushed`, scan redo after crash, and replay `PAGE_INIT`/`PAGE_BYTES` idempotently by pageLSN.

**Architecture:** Keep `RedoLogManager` backward-compatible in D3 memory mode, and add an opt-in durable mode via `RedoLogManager.durable(RedoLogFileRepository)`. Persist `RedoLogBatch` frames to one append-only redo file. Recovery scans complete frames and applies batches through a page-only dispatcher that writes pages through `PageStore`, not BufferPool/MTR.

**Tech Stack:** Java 25, JUnit Jupiter, fixed JDK `C:\Program Files\Java\jdk-25.0.2`, fixed Gradle `D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1`.

**Project overrides:** no git commits; TDD required; no `synchronized`; no bare runtime exceptions; Chinese Javadocs; run GitNexus impact before editing existing symbols.

---

## File Structure

- Add production: `RedoLogBatch`, `RedoRecordType`, `RedoLogFileRepository`, `RedoLogWriter`, `RedoLogFlusher`, `RedoRecoveryReader`, `RedoApplyContext`, `RedoApplyDispatcher`, `PageRedoApplyHandler`, `RedoLogIoException`, `RedoLogCorruptedException`.
- Modify production: `RedoLogManager` only, preserving `append/currentLsn/bufferedRecords`.
- Add test: `src/test/java/cn/zhangyis/db/storage/redo/RedoRuntimeRecoveryTest.java`.
- Modify resource: `src/main/resources/logback.xml` to use `%-5coloredLevel` with the existing ClassicConverter.

## Task R1-1: RED tests

- [x] **Step 1: Add failing tests**
  - Add five tests in `RedoRuntimeRecoveryTest`: durable flush/readback, wait timeout, apply page init/bytes, pageLSN skip, incomplete tail stop.

- [x] **Step 2: Run test and verify failure**
  - Run fixed Gradle with `--tests "cn.zhangyis.db.storage.redo.RedoRuntimeRecoveryTest"`.
  - Expected: compile failure because R1 runtime classes/API do not exist.

## Task R1-2: durable redo file + manager

- [x] **Step 1: Add batch/file/writer/flusher classes**
  - Implement framed redo file format: magic + payload length + crc32 + payload.
  - Encode batch start/end LSN and PAGE_INIT/PAGE_BYTES records.
  - Stop on incomplete tail; throw `RedoLogCorruptedException` for complete corrupted frames.

- [x] **Step 2: Extend `RedoLogManager`**
  - Add `durable(repo)`, `flush()`, `flushedToDiskLsn()`, `waitFlushed(target, timeout)`, and `bufferedBatches()`.
  - Keep default constructor as D3 memory-only mode.

- [x] **Step 3: Run R1 tests**
  - Expected: durable flush/readback and wait tests pass.

## Task R1-3: recovery reader + page replay

- [x] **Step 1: Add recovery reader and apply dispatcher**
  - `RedoRecoveryReader.readBatches()` returns complete batches and updates `recoveredToLsn`.
  - `RedoApplyDispatcher.pageDispatcher()` delegates to `PageRedoApplyHandler`.

- [x] **Step 2: Add page handler**
  - Read current pageLSN once per page per batch.
  - Skip page if current pageLSN already covers batch end.
  - Apply all same-page records in memory, then stamp pageLSN to batch end and write once.

- [x] **Step 3: Run R1 tests**
  - Expected: five R1 tests pass.

## Task R1-4: cleanup and regression

- [x] **Step 1: Fix logback converter usage**
  - Change `%coloredLevel(%-5level)` to `%-5coloredLevel`, because `ColoredLevelConverter` is a ClassicConverter, not a composite converter.

- [x] **Step 2: Run redo package tests**
  - Command: fixed Gradle `test --tests "cn.zhangyis.db.storage.redo.*" --console=plain`.
  - Expected: pass.

- [x] **Step 3: Run full tests**
  - Command: fixed Gradle `test --console=plain`.
  - Expected: pass.

- [x] **Step 4: GitNexus closeout**
  - `gitnexus_detect_changes({repo:"dbtest", scope:"all"})` failed because this repository has no valid `HEAD` (`fatal: ambiguous argument 'HEAD'`), matching the existing project state.
  - `npx gitnexus analyze --force` succeeded and refreshed the index to 4,964 nodes / 13,542 edges / 271 flows; FTS extension remains unavailable, but graph indexing completed.

## Task R1-5: five-pass hardening follow-up

- [x] **Step 1: Add hardening RED tests**
  - Add `RedoRuntimeHardeningTest` covering batch range mismatch, huge wait timeout fast path, `PAGE_BYTES` integer-overflow bounds, and complete invalid payload wrapping.

- [x] **Step 2: Verify RED**
  - Command: fixed Gradle `test --tests "cn.zhangyis.db.storage.redo.RedoRuntimeHardeningTest" --console=plain`.
  - Expected: four targeted failures before fixes, proving each test exercises an existing gap.

- [x] **Step 3: Harden production code**
  - `RedoLogBatch` validates `range.end == range.start + Σ record.byteLength()` and rejects LSN overflow.
  - `PageInitRecord` and `PageBytesRecord` make `byteLength()` match the R1 file encoding (`spaceId` + `pageNo`), not the earlier 8-byte pageId estimate.
  - `RedoLogManager.waitFlushed` checks already-durable targets before timeout conversion and clamps extreme duration-to-nanos conversion.
  - `RedoLogFileRepository` validates record count against remaining payload bytes and wraps invalid decoded domain values as `RedoLogCorruptedException`.
  - `PageRedoApplyHandler` computes `offset + length` as long before copying bytes into the page image.

- [x] **Step 4: Run hardening and redo package tests**
  - Commands: fixed Gradle `test --tests "cn.zhangyis.db.storage.redo.RedoRuntimeHardeningTest" --console=plain`, `test --tests "cn.zhangyis.db.storage.redo.RedoRecordTest" --console=plain`, and `test --tests "cn.zhangyis.db.storage.redo.*" --console=plain`.
  - Expected: pass.

## Self-Review

Spec coverage: durable writer/flusher, waitFlushed, reader, page dispatcher, pageLSN idempotence, torn tail handling, complete corruption wrapping, batch range integrity, wait timeout overflow, and page byte bounds all mapped to tests and production classes.

Marker scan: no unfinished-work markers.

Type consistency: tests use `RedoLogManager.durable`, `RedoLogFileRepository.open`, `RedoRecoveryReader`, `RedoLogBatch`, `RedoApplyContext`, and `RedoApplyDispatcher.pageDispatcher`, all implemented with matching names.
