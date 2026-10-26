-- The staging ring: the append-only substrate every producer writes into and
-- every consumer (sealing, claiming, draining) reads out of.
-- See docs/staging-and-claiming/02-the-staging-ring.md.
--
-- Producers never coordinate with each other or with consumers, and an append
-- takes no lock beyond its own row insert. That holds only because nothing on
-- the hot path is ever UPDATEd or DELETEd: merging happens at read time.

-- Globally monotonic and surviving a ring-slot TRUNCATE (a per-table IDENTITY
-- would reset with the table).
create sequence staging_change_id_seq;

-- The ring: seg_0 .. seg_3.
--
-- `fillfactor = 100, autovacuum_enabled = off` is safe because these tables
-- are append + TRUNCATE, never UPDATE: no dead tuples, no free space to
-- reserve. It also disables autoANALYZE, so they carry no planner statistics
-- unless something writes them by hand.
--
-- `op` distinguishes image-bearing CDC rows ('insert' | 'update' | 'delete')
-- from rows that carry no state to fold: 'recompute' asserts nothing, only
-- "recompute this key"; 'truncate' empties a source; 'schema_changed' marks
-- that a capture function met a renamed or dropped column (always with a
-- `new_image` naming what is missing); 'node_truncate' marks a truncated
-- partition node (always with a `new_image`). Insert/update/delete image
-- shapes are left unconstrained: the decoder already guarantees them.
--
-- `row_txid`, `change_id`, `appended_at` and `route` are store-assigned, so
-- every producer computes them identically:
--   * row_txid is the writer's real top-level xid, the fence the seal reads.
--     `xid8` matches `fence_snapshot`'s `pg_snapshot` family, so
--     `pg_visible_in_snapshot(row_txid, fence_snapshot)` needs no cast.
--   * change_id keeps the intra-transaction append order that commit `lsn`,
--     `row_txid` and `appended_at` (all constant within one transaction) lose.
--   * appended_at is the latency origin.
--   * route is the partition key a drain claims against. It is STORED, not
--     recomputed: hashtextextended is not guaranteed stable across major
--     Postgres versions, and a batch drained partly under one hash and
--     finished under another would re-apply every row that changed bucket.
--
-- `transition_images` carries the images of the intermediate states of a key
-- changed more than once in a transaction.
do $ring$
declare
    n integer;
begin
    for n in 0..3 loop
        execute format($t$
            create table seg_%1$s (
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
                transition_images jsonb,
                constraint seg_%1$s_op_check check (op in (
                    'insert', 'update', 'delete', 'recompute', 'truncate',
                    'schema_changed', 'node_truncate')),
                constraint seg_%1$s_recompute_has_no_images check (
                    (op <> 'truncate' or (old_image is null and new_image is null))
                    and (op <> 'recompute' or new_image is null)),
                constraint seg_%1$s_schema_changed_shape check (
                    op <> 'schema_changed' or (old_image is null and new_image is not null)),
                constraint seg_%1$s_node_truncate_shape check (
                    op <> 'node_truncate' or (old_image is null and new_image is not null))
            ) with (fillfactor = 100, autovacuum_enabled = off)
        $t$, n);
        execute format('create index seg_%1$s_origin_lsn_idx on seg_%1$s (origin_lsn)', n);
    end loop;
end
$ring$;

-- The pointer: exactly one row, naming which ring slot is active.
--
-- Writers read it inside their writing transaction and never lock it:
-- locking would serialize every append against every seal. A stale read is
-- accounted for by the fence.
--
-- `fillfactor = 90` plus aggressive autovacuum: the row is UPDATEd once per
-- seal but read by every append, so a bloated pointer is a bloated hot path.
create table segment_pointer (
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

insert into segment_pointer (id, active_seq, ring_slot) values (true, 1, 0);

-- The registry: one row per live batch. `seg_seq` is monotonic and never
-- reused; the physical `ring_slot` is reused as the ring wraps. Every
-- ordering argument is expressed in `seg_seq`, never in slot number.
--
-- `fence_snapshot` is the published, write-once fence of the seal's second
-- phase. `seal_step1` is the batch's flip transaction's xid (what recovery
-- rebuilds `fence_snapshot` from after a crash between the phases);
-- `seal_step2` is written into the predecessor's row by the same flip.
-- `bucket_count` is the batch's bucket split, `drained_mask` the buckets
-- drained so far, `has_truncate` and `has_schema_change` whether the fenced
-- window holds a truncate or schema-change marker, `row_count` the rows the
-- seal counted, and `late_rows` the rows that arrived after the fence.
--
-- `fillfactor = 70` plus aggressive autovacuum, as for `segment_pointer`.
create table segments (
    seg_seq bigint primary key,
    ring_slot smallint not null,
    state text not null,
    created_at timestamptz not null default now(),
    sealed_at timestamptz,
    fence_snapshot pg_snapshot,
    seal_step1 xid8,
    seal_step2 xid8,
    bucket_count smallint not null default 1,
    drained_mask bigint not null default 0,
    has_truncate boolean not null default false,
    row_count bigint not null default 0,
    has_schema_change boolean not null default false,
    late_rows bigint,
    constraint segments_state_check check (state in ('active', 'sealed', 'draining', 'drained'))
) with (
    fillfactor = 70,
    autovacuum_vacuum_scale_factor = 0.0,
    autovacuum_vacuum_threshold = 10,
    autovacuum_analyze_scale_factor = 0.0,
    autovacuum_analyze_threshold = 10
);

insert into segments (seg_seq, ring_slot, state) values (1, 0, 'active');

-- Which drainer holds which bucket of a batch.
create table seg_claims (
    seg_seq bigint not null references segments (seg_seq) on delete cascade,
    bucket smallint not null,
    claimed_by text not null,
    claimed_at timestamptz not null default now(),
    primary key (seg_seq, bucket)
);

create table drainers (
    drainer_id text primary key,
    last_seen timestamptz not null default now()
);

-- Where a bucket's drain resumes: the last (route, src_table, key) applied.
create table drain_cursor (
    seg_seq bigint not null references segments (seg_seq) on delete cascade,
    bucket smallint not null,
    after_route bigint not null,
    after_src_table text not null,
    after_key text not null,
    primary key (seg_seq, bucket)
);

-- What holds a batch's drain back: the buckets and tables that failed, the
-- error, and how long and how often.
create table drain_holdups (
    seg_seq bigint primary key references segments (seg_seq) on delete cascade,
    buckets smallint[] not null check (cardinality(buckets) > 0),
    tables text[] not null,
    error text not null,
    sqlstate text,
    since timestamptz not null,
    last_seen timestamptz not null,
    attempts integer not null check (attempts > 0)
);

-- The truncate floor of a target on its ledger: a source `TRUNCATE` empties
-- the target's ledger and every group row and records the truncate's ring
-- `lsn` here, one row per target (its qualified target table). A change at or
-- below the floor predates the truncate and is skipped. Deleted with the
-- definition.
create table ledger_truncate_floor (
    target_table text primary key,
    floor pg_lsn not null
);

-- A snapshot-independent copy of `segment_pointer.ring_slot` for ring writers
-- to read. A writer at REPEATABLE READ or SERIALIZABLE reads
-- `segment_pointer` from its transaction snapshot, which can be flips old, and
-- its row would land in a sealed slot above that slot's fence. Sequence reads
-- are never transactional, so `pg_sequence_last_value('ring_slot_mirror')`
-- returns the latest value at every isolation level; only the seal's second
-- phase writes it (`setval` is non-transactional too), after the flip has
-- committed. `segment_pointer` stays the registry's authority.
-- `minvalue 0` so slot 0 is representable, `maxvalue 3` because the ring has
-- four slots, and the `setval(..., true)` makes the last value non-NULL from
-- the start.
create sequence ring_slot_mirror as smallint minvalue 0 maxvalue 3 no cycle;
select setval('ring_slot_mirror', (select ring_slot from segment_pointer), true);
