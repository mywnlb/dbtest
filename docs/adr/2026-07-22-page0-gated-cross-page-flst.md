# ADR: page0-gated cross-page FLST

## Status

Accepted, 2026-07-22.

## Context

The original FLST implementation relied on every XDES node being embedded in page0. Once descriptors move to primary/overflow XDES pages, a free or segment list can link nodes on several physical pages. Strictly acquiring every newly discovered neighbor in ascending page-number order is impossible during arbitrary forward/backward traversal: the persisted next pointer can point to a lower page than a node already fixed by the same MTR.

Using an unrestricted latch-order exception would hide real deadlocks. Releasing each previous node before following its pointer would also make the pointer unstable unless a higher-level list latch excluded writers.

## Decision

Use the tablespace page0 latch as the FLST list gate:

- readers acquire page0 S before reading a base or node;
- writers acquire page0 X before modifying a base, node, or neighbor;
- initial base/node acquisition remains in ascending PageId order;
- a cross-page neighbor revisit may enter `allowOutOfOrderPageLatch` only while the page0 gate is held, with an explicit proof string;
- MTR memo retains all guards and releases them in reverse order.

`ExtentDescriptorRepository` uses the same page0-first rule before accessing a standalone XDES page. Thus no FLST writer can hold a remote XDES page and then wait for page0, and readers cannot observe a pointer while a writer changes the same list.

## Consequences

- Cross-page traversal and removal are deterministic and testable without a global engine lock.
- FLST operations within one tablespace are serialized for writers; this is intentionally coarse for the teaching implementation.
- Callers must enter FSP before data-page latches. A caller already holding a higher page without page0 is rejected by the normal MTR order guard.
- Future finer concurrency may replace the page0 gate with a dedicated per-space FLST latch, but it must preserve the same acquisition position and recovery-visible list invariants.
