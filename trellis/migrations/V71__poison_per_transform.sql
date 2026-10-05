-- Whole-key poison per transform (issue #799).
--
-- `poison`, `poison_held` and `key_deaths` (V13) were keyed by
-- `(src_table, key)`, and the fold left a poisoned key out for every reader
-- of its table. So a whole-key failure in one definition (a target key copy
-- too narrow for a new value, a constraint on its target, RLS on its writes)
-- froze that key in every other definition on the same source too.
--
-- Each table now names the definition whose apply failed, and the drain
-- skips the key for that definition only: every other reader applies it
-- normally (`staging::apply::compute`). `transform_definitions(id)` is the
-- definition's identity, and `on delete cascade` makes a dropped
-- definition's quarantine go with it. The whole-transform fuse counts a
-- definition's own evicted keys, so its gate (V30) is keyed by the
-- definition too, and a resume deletes the definition's own rows
-- (`staging::quarantine::resume_transform`) before its fresh build.
--
-- Trellis is unreleased, so there are only ephemeral installs: the tables
-- are recreated empty rather than backfilled, since an existing row names no
-- definition.

drop table poison;
create table poison (
    transform_id bigint not null references transform_definitions (id) on delete cascade,
    src_table text not null,
    key text not null,
    poisoned_at timestamptz not null default now(),
    last_error text not null,
    primary key (transform_id, src_table, key)
);
-- The drain's exclusion read (`quarantine::poisoned_keys_among`) looks keys
-- up by table and key, across definitions.
create index poison_src_key on poison (src_table, key);

drop table poison_held;
create table poison_held (
    transform_id bigint not null references transform_definitions (id) on delete cascade,
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
    group_key text[],
    held_seq bigserial,
    primary key (transform_id, src_table, key, seg_seq)
);
create index poison_held_key_order
    on poison_held (transform_id, src_table, key, seg_seq, held_seq);

drop table key_deaths;
create table key_deaths (
    transform_id bigint not null references transform_definitions (id) on delete cascade,
    src_table text not null,
    key text not null,
    deaths integer not null default 0,
    last_error text,
    last_death_at timestamptz,
    primary key (transform_id, src_table, key)
);

drop table transform_fuse_gate;
create table transform_fuse_gate (
    transform_id bigint primary key references transform_definitions (id) on delete cascade,
    checks bigint not null default 0,
    last_checked_at timestamptz not null default now()
);
