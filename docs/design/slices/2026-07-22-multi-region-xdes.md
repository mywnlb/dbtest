# Multi-region XDES / repeated bitmap slice

## Goal

- Remove the page0 XDES capacity ceiling without changing any existing page0 entry address.
- Materialize repeated XDES and `IBUF_BITMAP` management pages before ordinary extent allocation crosses a region.
- Keep allocation, redo recovery, offline scrub, and Change Buffer on one physical addressing formula.

## Persistent layout decisions

- `C = floor((pageSize - 256) / 68)` is the immutable descriptor-page capacity.
- `G = pageSize.bytes / pagesPerExtent` is the number of extents covered by one management region.
- Extents `[0,C)` stay in page0 at `256 + extentNo * 68`.
- Region 0 overflow descriptors use page5 when `G > C`.
- Region `r>0` uses primary XDES at `r * pageSize.bytes`, bitmap at `+1`, and optional overflow XDES at `+5`.
- `PageType.XDES` uses appended persistent code 13; prior page type codes are unchanged.
- Entry stride remains 68 bytes, but only `ceil(pagesPerExtent/8)` bitmap bytes are active.

## Allocation and ownership

- `ExtentManagementRegionLayout` is the single pure ExtentId/FileAddress mapping authority.
- `ExtentManagementRegionInitializer` runs under the tablespace page0 X gate before freeLimit advances.
- Every region-first extent is `FSEG_FRAG`, owner 0, absent from FLST, and rejected by ordinary XDES mutators.
- Primary page 0, bitmap page 1, and optional overflow page 5 are the only allocated bits in later management extents.
- Region 0 page5 is reserved lazily when the first compatible page0-external descriptor is needed.
- UP direction may materialize at most one ordinary extent per allocation MTR; it then falls back to the free-list head.
- Reservation capacity does not count region-first management extents as business capacity.

## Concurrency and recovery

- FLST readers take page0 S; FLST writers take page0 X before any base or node page.
- Cross-page neighbor revisits use a narrow MTR order exception only while that gate remains held.
- New fixed management pages are preflighted as all-zero or exact-format before any peer is modified.
- Blank management pages emit `PAGE_INIT` before header bytes and XDES metadata deltas.
- Recovery extends on `PAGE_INIT`, patches physical after-images, and never reruns the allocator.
- Ordinary open validates every management region below freeLimit; recovery open caches page0-only metadata as recovery-only until strict reload.
- Legacy content or owner/list evidence at a future fixed location fails closed and requires offline rebuild.

## Non-goals

- No binary compatibility claim with MySQL/InnoDB XDES page bytes.
- No online relocation of a legacy business page occupying a newly fixed management position.
- No second allocation bitmap format or two-bit clean/free encoding.
- No change to segment fragment threshold, extent size, or B+Tree allocation policy.

## Acceptance tests

- 4K/8K/16K/32K/64K layout round-trip for 100,000 extents per page size.
- Page0 address compatibility, group0 page5 overflow, later primary/bitmap reservation, and cross-page FLST removal.
- Legacy bitmap/owner conflict rejection before partial formatting.
- PAGE_INIT plus standalone XDES metadata replay beyond EOF.
- Full scrub accepts valid page5 XDES and rejects checksummed header corruption.
- Ordinary open rejects missing crossed-region pages and a canonical management descriptor referenced by a global FLST base.
- Full Gradle suite remains at or above the prior test count with zero failures.

## Current map update

- Update create/allocation, FLST, extent-management, scrub, and Change Buffer gap rows.
- Do not add target-architecture edges that are not present in production source.
