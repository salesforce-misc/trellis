-- Durable, claimable backfill-chunk work queue (public API design's
-- ADR-0007 amendment — see docs/decisions/0007-direct-set-based-backfill.md's
-- "Backgrounding and resumability" section, and docs/public-api-design.md's
-- decision 1).
--
-- `defs::catalog::install_definition`'s plain 1-1 direct-build path no
-- longer walks every PK-range chunk back-to-back in one call. Instead it
-- enumerates the same boundaries `defs::backfill::discover_pk_ranges` always
-- computed and persists each one here, then returns immediately — the
-- definition row (already inserted with status `backfilling`, see
-- V19__transform_status.sql) sits with its work items pending until a drain
-- (application) worker claims and executes them
-- (`trellis::client`'s `app_worker_loop`).
--
-- Modeled closely on `seg_claims`/`drainers` (V7__claims_and_buckets.sql)'s
-- claim/heartbeat/reclaim-stale idiom, collapsed onto a single table: unlike
-- a sealed ring segment, a backfill chunk is never split into buckets shared
-- by several workers at once, so there is no separate claims table to join
-- against — `claimed_by`/`claimed_at` live directly on the row. `claimed_by
-- is null` is unclaimed; a non-null claim older than the reclaim TTL is
-- reclaimable by `trellis::staging::reclaim_stale`'s new sibling for this
-- table (see `trellis::defs::chunk_queue::reclaim_stale_chunks`) exactly the
-- way a stale `seg_claims` row is today; `done` is the terminal state once
-- the chunk's write has committed.
--
-- `lo`/`hi` mirror `discover_pk_ranges`'s own `(Option<String>, String)`
-- boundary representation: the source primary key's value rendered through
-- `::text`, so one column pair works regardless of the PK's underlying
-- Postgres type (`int`, `bigint`, `uuid`, ...) — the claiming worker casts
-- them back the same way `defs::backfill::pk_range_where` already does.
-- `lo is null` means "the first chunk" (`pk <= hi`, no lower bound). Only a
-- 1-1 definition is chunked this way — an aggregate definition still builds
-- fully synchronously inside `install_definition`, unchanged (see
-- `trellis::defs::chunk_queue`'s module doc for why: their staging tables are
-- connection-scoped, which doesn't survive independent drain workers
-- claiming separate chunks on separate connections).
create table if not exists backfill_chunks (
    id bigserial primary key,
    definition_id bigint not null references transform_definitions (id) on delete cascade,
    lo text,
    hi text not null,
    done boolean not null default false,
    claimed_by text,
    claimed_at timestamptz,
    created_at timestamptz not null default now()
);

-- The claim query (`chunk_queue::claim_chunks`) scans for unclaimed, undone
-- work ordered by id; a partial index keeps that scan cheap regardless of
-- how much completed history piles up in this table over an instance's
-- life.
create index if not exists backfill_chunks_claimable_idx
    on backfill_chunks (id)
    where not done and claimed_by is null;

-- `chunk_queue::finish_chunk`'s "any left for this definition?" check when a
-- chunk completes, and the reclaim sweep's per-definition bookkeeping.
create index if not exists backfill_chunks_definition_idx
    on backfill_chunks (definition_id);
