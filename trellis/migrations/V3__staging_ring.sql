-- The staging ring (issue #6 / stage 02): the append-only substrate every
-- producer writes into and every consumer (sealing, claiming) reads out of.
-- See docs/staging-and-claiming/02-the-staging-ring.md.
--
-- The central guarantee: producers never coordinate with each other or with
-- consumers, and an append takes no lock beyond its own row insert. That
-- holds only because nothing on the hot path is ever UPDATEd or DELETEd —
-- merging happens at read time, in later stages.

-- ---------------------------------------------------------------------------
-- The ring: seg_0 .. seg_3 (N = 4, fixed for now — issue #6 asks whether N
-- should be operator-tunable).
--
-- `fillfactor = 100, autovacuum_enabled = off` is safe only because these
-- tables are append + TRUNCATE, never UPDATE: no dead tuples, no free space
-- to reserve. The catch: it also disables autoANALYZE, so these tables carry
-- no planner statistics unless something writes them by hand (see
-- docs/staging-and-claiming/07-convergence-and-await.md) — a later stage's
-- problem, flagged here so it isn't a surprise.
-- ---------------------------------------------------------------------------

-- `op` distinguishes image-bearing CDC rows ('insert'|'update'|'delete')
-- from image-less recompute triggers ('recompute'), produced by the three
-- non-CDC producers. The fold (stage 04) treats them differently: a
-- recompute row asserts nothing about state, only "recompute this key" —
-- conflating the two is the bug stage 04 is built to avoid.
--
-- `row_txid`, `change_id`, `appended_at`, and `route` are store-assigned
-- (default or generated, never client-supplied) so all four producers —
-- three rendering client-side VALUES, one (backfill) using server-side
-- INSERT ... SELECT — compute them identically:
--   * row_txid must be the writer's real top-level xid (not a
--     subtransaction's) — it is the fence stage 03 reads. `xid8`/
--     `pg_current_xact_id()`, matching `fence_snapshot`'s `pg_snapshot`
--     family — see the registry section below.
--   * change_id records the intra-transaction append order that would
--     otherwise be lost: commit `lsn`, `row_txid`, and `appended_at` are all
--     constant across every row of one source transaction, so none of them
--     can tell an INSERT from a later UPDATE of the same key in that txn.
--     The fold (stage 04) orders by `lsn` first and falls back to
--     `change_id` only to break ties within one commit. A shared sequence,
--     not per-table IDENTITY, because it must stay globally monotonic and
--     survive a ring-slot TRUNCATE (IDENTITY resets with the table).
--   * appended_at is the latency origin; stamping it on the registry row
--     instead would make an in-flight writer hold a row a seal must flip,
--     turning clean backpressure into a hard block.
--   * route is the partition key stage 04 claims against, STORED not
--     recomputed: hashtextextended isn't guaranteed stable across major
--     Postgres versions, and a batch drained partly under one hash and
--     finished under another would re-apply every row that changed bucket.
create sequence if not exists staging_change_id_seq;

create table if not exists seg_0 (
    src_table text not null,
    key text not null,
    op text not null,
    lsn pg_lsn,
    old_image jsonb,
    new_image jsonb,
    origin_lsn pg_lsn,
    src_changed timestamptz,
    hop_gen integer not null default 0,
    row_txid xid8 not null default pg_current_xact_id(),
    change_id bigint not null default nextval('staging_change_id_seq'),
    appended_at timestamptz not null default now(),
    route bigint not null generated always as (
        hashtextextended(src_table || E'\x1f' || key, 0) & 2147483647
    ) stored,
    constraint seg_0_op_check check (op in ('insert', 'update', 'delete', 'recompute')),
    -- A recompute trigger carries no images. Insert/update/delete image
    -- shapes are left unconstrained rather than encoded as CHECKs that would
    -- just duplicate what the decoder (stage 01) already guarantees.
    constraint seg_0_recompute_has_no_images check (
        op <> 'recompute' or (old_image is null and new_image is null)
    )
) with (fillfactor = 100, autovacuum_enabled = off);

create table if not exists seg_1 (
    src_table text not null,
    key text not null,
    op text not null,
    lsn pg_lsn,
    old_image jsonb,
    new_image jsonb,
    origin_lsn pg_lsn,
    src_changed timestamptz,
    hop_gen integer not null default 0,
    row_txid xid8 not null default pg_current_xact_id(),
    change_id bigint not null default nextval('staging_change_id_seq'),
    appended_at timestamptz not null default now(),
    route bigint not null generated always as (
        hashtextextended(src_table || E'\x1f' || key, 0) & 2147483647
    ) stored,
    constraint seg_1_op_check check (op in ('insert', 'update', 'delete', 'recompute')),
    constraint seg_1_recompute_has_no_images check (
        op <> 'recompute' or (old_image is null and new_image is null)
    )
) with (fillfactor = 100, autovacuum_enabled = off);

create table if not exists seg_2 (
    src_table text not null,
    key text not null,
    op text not null,
    lsn pg_lsn,
    old_image jsonb,
    new_image jsonb,
    origin_lsn pg_lsn,
    src_changed timestamptz,
    hop_gen integer not null default 0,
    row_txid xid8 not null default pg_current_xact_id(),
    change_id bigint not null default nextval('staging_change_id_seq'),
    appended_at timestamptz not null default now(),
    route bigint not null generated always as (
        hashtextextended(src_table || E'\x1f' || key, 0) & 2147483647
    ) stored,
    constraint seg_2_op_check check (op in ('insert', 'update', 'delete', 'recompute')),
    constraint seg_2_recompute_has_no_images check (
        op <> 'recompute' or (old_image is null and new_image is null)
    )
) with (fillfactor = 100, autovacuum_enabled = off);

create table if not exists seg_3 (
    src_table text not null,
    key text not null,
    op text not null,
    lsn pg_lsn,
    old_image jsonb,
    new_image jsonb,
    origin_lsn pg_lsn,
    src_changed timestamptz,
    hop_gen integer not null default 0,
    row_txid xid8 not null default pg_current_xact_id(),
    change_id bigint not null default nextval('staging_change_id_seq'),
    appended_at timestamptz not null default now(),
    route bigint not null generated always as (
        hashtextextended(src_table || E'\x1f' || key, 0) & 2147483647
    ) stored,
    constraint seg_3_op_check check (op in ('insert', 'update', 'delete', 'recompute')),
    constraint seg_3_recompute_has_no_images check (
        op <> 'recompute' or (old_image is null and new_image is null)
    )
) with (fillfactor = 100, autovacuum_enabled = off);

-- ---------------------------------------------------------------------------
-- The pointer: exactly one row, naming which ring slot is currently active.
--
-- Writers read this inside their writing transaction and never lock it (see
-- `trellis::staging::append`) — locking would serialize every append against
-- every seal, and this is the hottest row in the system. A stale read is
-- expected and accounted for by the fence (stage 03). Paying for stale reads
-- once, in a read-time fence, instead of coordinating on every append is the
-- point of the whole design.
--
-- `fillfactor = 90` plus aggressive autovacuum: this row is UPDATEd once per
-- seal, but every append reads it, so a bloated pointer row is a bloated hot
-- read path. Scale factor 0 with a tiny threshold means "vacuum after a
-- handful of updates," independent of table size (always 1 row).
create table if not exists segment_pointer (
    id boolean primary key default true check (id),
    active_seq bigint not null,
    ring_slot smallint not null
) with (
    fillfactor = 90,
    autovacuum_vacuum_scale_factor = 0.0,
    autovacuum_vacuum_threshold = 1,
    autovacuum_analyze_scale_factor = 0.0,
    autovacuum_analyze_threshold = 1
);

insert into segment_pointer (id, active_seq, ring_slot)
values (true, 1, 0)
on conflict (id) do nothing;

-- ---------------------------------------------------------------------------
-- The registry: one row per live batch (state, fence, bucket split,
-- timestamps). `seg_seq` is monotonic and NEVER reused; the physical
-- `ring_slot` is reused as the ring wraps. Every ordering argument is
-- expressed in `seg_seq`, never slot number — that is what makes slot reuse
-- safe, and every later stage reading this table must keep it that way.
--
-- `state` lists 'active' and 'sealed' — the two this stage produces.
-- Stage 04 adds 'draining', stage 05 adds 'drained'; those transitions are
-- out of scope here (see `trellis::staging::state::SegmentState`, the one
-- function that owns the full lifecycle graph). This migration only seeds
-- the one active batch.
--
-- Txid-family note for stage 03 (#9), resolved: `row_txid` and
-- `fence_snapshot` are both the `pg_snapshot`/`xid8` family —
-- `pg_visible_in_snapshot(row_txid, fence_snapshot)` is the fence's
-- visibility test, no bridging cast needed.
--
-- `fence_snapshot` is the design doc's `seal_snapshot` (S_k): the published,
-- write-once fence, filled by the seal's phase 2 — never inside the flip
-- transaction that produces `seal_step1`. See
-- docs/staging-and-claiming/03-sealing-and-the-fence.md.
--
-- `seal_step1`/`seal_step2` are the two-phase seal's own bookkeeping:
-- `seal_step1` is this batch's flip transaction's xid, stamped when phase 1
-- seals it (and, read back after a crash between phase 1 and phase 2, what
-- recovery reconstructs `fence_snapshot` from); `seal_step2` is written into
-- the *predecessor's* row by the same phase 1, recording which flip closed
-- it out.
--
-- `fillfactor = 70` plus aggressive autovacuum, same reasoning as
-- `segment_pointer`: a small table UPDATEd per state transition that must
-- not bloat, since `ring_slot_is_free` reads it on every seal attempt.
create table if not exists segments (
    seg_seq bigint primary key,
    ring_slot smallint not null,
    state text not null,
    bucket_mask bigint not null default 0,
    created_at timestamptz not null default now(),
    sealed_at timestamptz,
    fence_snapshot pg_snapshot,
    seal_step1 xid8,
    seal_step2 xid8,
    constraint segments_state_check check (state in ('active', 'sealed'))
) with (
    fillfactor = 70,
    autovacuum_vacuum_scale_factor = 0.0,
    autovacuum_vacuum_threshold = 10,
    autovacuum_analyze_scale_factor = 0.0,
    autovacuum_analyze_threshold = 10
);

insert into segments (seg_seq, ring_slot, state)
values (1, 0, 'active')
on conflict (seg_seq) do nothing;
