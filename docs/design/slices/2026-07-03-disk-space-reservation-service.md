# Disk SpaceReservationService Slice

## Goal

Implement the first Disk/Tablespace production-quality slice from `storage-backlog.md` item 0.14:
multi-page storage operations can reserve space before they start allocating pages, so ENOSPC is reported before
the operation has already created some pages.

## Key Decisions

- Add a storage-internal `SpaceReservationService` owned by `DiskSpaceManager`.
- Expose `DiskSpaceManager.reserveSpace(mtr, spaceId, kind, pages, extents)` returning `SpaceReservation`.
- Reservation kinds are `NORMAL`, `UNDO`, `CLEANING`, and `BLOB`, matching the disk-manager design.
- The first implementation is in-memory and per-process; it pre-extends the physical file and page0 `currentSize`
  before page allocation, then tracks active quota under an explicit `ReentrantLock`.
- `allocatePage` remains source-compatible for existing callers. If an active reservation exists for the same
  MTR and space, it must consume one reserved page; if quota is exhausted, allocation fails with a reservation
  domain exception.
- `SpaceReservation` is idempotent `AutoCloseable`. Callers may use try-with-resources; MTR memo also releases
  unclosed reservations on commit/rollback.

## Non-Goals

- Do not wire B+Tree split or Undo grow callers in this slice.
- Do not implement direction-aware extent allocation; that remains 0.15.
- Do not persist reservation counters to page0; crash recovery treats reservation as an in-flight volatile guard.
- Do not implement DD/DDL lifecycle or discovery.

## Acceptance Tests

- Reserving one page in a too-small but extendable tablespace grows the file/page0 before the first `allocatePage`.
- A page allocation inside an active reservation consumes quota; a second allocation after quota exhaustion fails.
- Reservation failure from a bounded store happens before page0 `currentSize` is advanced or any page allocation runs.
- An unclosed reservation is released by MTR rollback so a later MTR can reserve the same capacity.

## Current Map Update

- Mark 0.14 core reservation service as implemented with storage-internal, in-memory counters.
- Keep B+Tree split and Undo grow reservation consumers as remaining work.
