-- Issue #418 (ADR-0016): a resumed definition's rebuild now runs through
-- `backfill_chunks` too, so "was this definition ever resumed" no longer tells
-- a chunk planned before the resume apart from one planned for the rebuild.
-- Each chunk records the definition's `fuse_rearmed_at` as of the discharge
-- that planned it. A resume stamps a new `fuse_rearmed_at`, so a chunk whose
-- copy differs was planned before it and is stale.
alter table backfill_chunks
    add column if not exists fuse_rearmed_at timestamptz;
