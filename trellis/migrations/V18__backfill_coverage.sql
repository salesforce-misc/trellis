-- Direct-backfill coverage record (issue #79, bug B).
--
-- When a definition is stood up via the fast direct-backfill path
-- (`defs::backfill::backfill_definition` -> `backfill_relationship_one_to_one`
-- / `backfill_one_to_one` / `backfill_aggregate`), the target is computed
-- server-side straight from the *current* state of the definition's own source
-- table and every to-side relationship table it reads. The ring is never
-- touched for that build. But those same tables still have to join the CDC
-- publication so future changes flow through as deltas — and when they do,
-- `reconcile_publication` stages a `pending_backfill` marker whose
-- `run_pending_backfills` discharge enumerates *every* pre-existing row of the
-- table as an image-less `Recompute`. For a to-side table that no direct
-- transform reads as its own source, that enumeration exists only to feed the
-- reverse-recompute machinery in `staging::apply` — re-deriving parents whose
-- values the direct build already computed correctly. On a demo with 1M posts
-- and 4.5M comments that is ~5.5M markers of pure waste (see the issue).
--
-- A row here records that, as of `fence_snapshot`, `table_name`'s contents were
-- fully folded into an already-built target by a direct backfill, and that the
-- table held exactly `covered_row_count` rows at that instant. `coverage_covers`
-- (in `trellis::intake::markers`) consults this before enumerating a
-- pending_backfill and, when the table provably has not changed since the
-- fence, skips the enumeration entirely. See that module for the fence /
-- change-detection reasoning (a whole-snapshot fence comparison alone cannot
-- decide this — the build always precedes the publication join in time).
--
-- `table_name` is the fully-qualified `"schema.table"` string used everywhere
-- else in the intake layer (`publication::qualify`), so it matches
-- `pending_backfill.table_name` for a direct lookup.
create table if not exists backfill_coverage (
    table_name text primary key,
    fence_snapshot pg_snapshot not null,
    covered_row_count bigint not null,
    recorded_at timestamptz not null default now()
);
