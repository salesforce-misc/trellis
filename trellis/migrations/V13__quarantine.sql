-- Quarantine (issue #16, stage 06's other half): isolate, evict, park,
-- release. See docs/staging-and-claiming/06-cleanup-and-reclaim.md's
-- "Quarantine" section and docs/decisions/0003-quarantine-storage-and-api.md
-- for the storage shape this migration fixes.
--
-- Three tables, per the design doc's "Three-table poison track":
--   `poison`      — the marker: which (src_table, key) pairs are currently
--                   evicted, so the fold excludes them globally.
--   `poison_held` — the parked work: every excluding batch's own folded
--                   contribution for an already-poisoned key, kept so a
--                   healthy later change to that key isn't lost when the
--                   batch that excluded it retires. Keyed (src_table, key,
--                   seg_seq) — one row per batch per key, idempotent on
--                   that triple (`ON CONFLICT ... DO NOTHING`).
--   `key_deaths`  — the counter: how many times a key has been attributed
--                   an isolated, non-transient failure. Lives off the
--                   ring's own rows (which are immutable and carry no
--                   counter of their own), cleared by a clean drain.
--
-- Plus `halting_stops`: the "real metric (counter + last reason)" the
-- halting-schema-diagnosis class needs (docs/.../05-apply-and-exactly-once-
-- deltas.md's failure-classification table) — a single-row counter table,
-- following this crate's existing structured-logging-plus-a-table
-- convention (`pause_leases`, `drainers`) rather than introducing a
-- Prometheus-style dependency this crate has none of today.

create table if not exists poison (
    src_table text not null,
    key text not null,
    poisoned_at timestamptz not null default now(),
    last_error text not null,
    primary key (src_table, key)
);

-- `held_seq` is a tie-breaker only — the primary ordering key for release
-- (docs: "replay every held row for the key, in batch order then position
-- order") is `seg_seq` itself, since this table already holds at most one
-- row per (src_table, key, seg_seq): a batch's whole folded contribution for
-- a key, not per-source-row detail. `lsn`/`origin_lsn` are nullable exactly
-- like the ring tables' own columns (`V3__staging_ring.sql`), since a parked
-- image-less recompute trigger carries neither.
create table if not exists poison_held (
    src_table text not null,
    key text not null,
    seg_seq bigint not null,
    op text not null check (op in ('insert', 'update', 'delete', 'recompute')),
    lsn pg_lsn,
    old_image jsonb,
    new_image jsonb,
    origin_lsn pg_lsn,
    src_changed timestamptz,
    hop_gen integer not null default 0,
    held_seq bigserial,
    primary key (src_table, key, seg_seq)
);
create index if not exists poison_held_key_order on poison_held (src_table, key, seg_seq, held_seq);

create table if not exists key_deaths (
    src_table text not null,
    key text not null,
    deaths integer not null default 0,
    last_error text,
    last_death_at timestamptz,
    primary key (src_table, key)
);

create table if not exists halting_stops (
    -- A single boolean-PK row, matching the "one row, no real key" shape a
    -- process-wide counter needs — the same trick as any singleton-config
    -- table, without a surrogate id nothing else ever compares against.
    id boolean primary key default true check (id),
    stop_count bigint not null default 0,
    last_reason text,
    last_stopped_at timestamptz
);
insert into halting_stops (id) values (true) on conflict (id) do nothing;
