# Buffer Pool remove legacy flush API

## Goal

- Remove public `BufferPool.flush(PageId)` and `BufferPool.flushAll()` so Buffer Pool no longer exposes a direct PageStore write path.
- Make WAL-safe flushing exclusively owned by `storage.flush`: `FlushCoordinator` performs WAL gate, checksum, doublewrite, data-file write/force, then `completeFlush/failFlush`.
- Keep Buffer Pool responsible only for dirty view, snapshot, clean/keep-dirty callbacks, eviction metadata and page lifetime.
- Fail explicitly when a standalone/test `BufferPoolInstance` must evict a dirty victim without an attached `DirtyVictimFlusher`.

## Key Decisions

- Break the `BufferPool` public API in this storage-only slice instead of preserving an unsafe compatibility method.
- Delete legacy `BufferPoolInstance.flushLegacyPage` / `flushAll` direct-write behavior and related local legacy candidate scanning.
- `LruBufferPool.close()` will not implicitly write dirty pages; higher layers must run `FlushService.flushThrough(...)` or an equivalent WAL-safe shutdown path before close.
- Dirty victim eviction remains WAL-safe in production through the existing `DirtyVictimFlusher` injected by `StorageEngine`.
- Tests that need persistence will use a small flush helper around `FlushCoordinator.singlePageFlush/flushList`, not `BufferPool.flush*`.

## Non-goals

- Do not change `FlushCoordinator` WAL/doublewrite protocol or `FlushService` scheduling policy.
- Do not add batch dispatch, FlushList/LRU doublewrite files, new `DoublewriteMode` engine config, or page0 checksum.
- Do not introduce `DIRTY_PENDING`, `EVICTING`, or `STALE` states in this slice.
- Do not change SQL/session/DD behavior; this remains a storage-internal API cleanup.

## Acceptance Tests

- Compile-level regression: no production or test code calls `BufferPool.flush(...)` or `flushAll()`.
- Existing flush tests still pass through `FlushCoordinator` / `FlushService`.
- Existing tests that previously used `pool.flush*` are migrated to WAL-safe flush helpers or adjusted to avoid physical writeback.
- New/updated buffer-pool test verifies dirty victim eviction without `DirtyVictimFlusher` fails explicitly instead of writing PageStore directly.
- Run `gradle test --tests cn.zhangyis.db.storage.buf.*`, `--tests cn.zhangyis.db.storage.flush.*`, `--tests cn.zhangyis.db.storage.mtr.*`, then full `gradle test`.

## Current Map Update

- Remove `BufferPool.flush(PageId)` / `flushAll()` from Reserved / Unwired Production Types and legacy-gap rows.
- Update dirty page flush chain to state that only `FlushCoordinator` writes dirty data pages.
- Update Buffer Pool known gaps: legacy flush去留 is resolved; remaining items are state refinement, warmup/random config, page0 checksum coordination, and broader DDL lifecycle.
