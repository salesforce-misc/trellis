-- Capture state: what Trellis recorded about the tables it captures, so the
-- capture pass can tell a change it made from a change made under it.
--
-- `partition_sources`, `capture_baselines` and `capture_triggers` store the
-- oid of their own table (or a value derived from it). A dump and restore
-- recreates the table to bring its rows back, so a stored oid that no longer
-- names the table holding the row says the rows were restored. Never rebuild
-- one of these tables by create-copy-rename in a migration: the copied rows
-- would read as a restore. Use ALTER.

-- One row per partitioned root the reconcile pass captures: the nodes it last
-- saw (oid, parent, bound, `DEFAULT` flag, the recorded partition constraint
-- text and the key columns' types and collations), the two fingerprints
-- (`lifecycle_fp`, `compliance_fp`) that say whether the tree changed since,
-- `stale` (the root was renamed away while recorded) and `noncompliant` (the
-- pass paused readers because the source stopped complying, so every pass
-- checks it again). `catalog_oid` is the oid of this table; a row whose
-- stored value differs from the live one was restored, so the pass pauses
-- every definition that reads the source and runs nothing routed from the
-- recorded state. `system_identifier` identifies the cluster that wrote it.
create table partition_sources (
    table_name text primary key,
    root_oid bigint not null,
    lifecycle_fp text not null,
    compliance_fp text not null,
    nodes jsonb not null,
    stale boolean not null default false,
    noncompliant boolean not null default false,
    system_identifier text,
    catalog_oid oid not null
);

comment on table partition_sources is
    'Trellis: never rebuild this table by create-copy-rename in a migration; use ALTER. '
    'Copied rows look exactly like a restore (their catalog_oid no longer names this '
    'table), which pauses every definition.';

-- The durable queue a detected change to a partitioned source leaves for the
-- correction engine: one row per node to correct. `new_leaf` is a leaf the
-- pass found created, attached, re-attached, or whose `DEFAULT` siblings
-- changed (`cause` says which); `gone_node` the topmost node of a dropped or
-- detached subtree, with the recorded constraint text its groups route by;
-- `truncated` a truncated node; `complement` the keys outside a node's
-- recorded bound. `queued_lsn` is the WAL position when the pass queued it: a
-- consumer corrects only once the ring is folded through it. The row is
-- committed with the fingerprints that detected it, so a restart resumes it.
-- A node queued again before any reader starts it adds nothing.
--
-- `node_default` says the node is a `DEFAULT` partition; its recorded
-- constraint text is NULL when it has no siblings (every group routes to it),
-- which a gone node with no usable text also has. `parent_oid` is, for a gone
-- node, its parent's oid when it left the tree: a `HASH` text embeds it, so
-- once the parent is dropped the text cannot be evaluated, and the parent's
-- own correction covers the node's groups.
create table partition_corrections (
    id bigint generated always as identity primary key,
    table_name text not null,
    kind text not null check (kind in ('new_leaf', 'gone_node', 'truncated', 'complement')),
    node_oid bigint not null,
    node_name text not null,
    cause text not null,
    constraint_text text,
    queued_lsn pg_lsn not null default pg_current_wal_insert_lsn(),
    queued_at timestamptz not null default now(),
    node_default boolean not null default false,
    parent_oid bigint
);

create index partition_corrections_identity
    on partition_corrections (table_name, kind, node_oid);

-- One row per correction and aggregate definition reading the source: how
-- far the correction's key set has been re-derived. A restart resumes from
-- `cursor`; `keys_done` and `keys_total` are the keys remaining the status
-- reports.
--
-- `phase` is `fence` while the engine waits for the transactions open when it
-- stamped the definition's `build_seg` to end (`fence_xid`), then `ledger`
-- while it walks the live ledger entries whose group routes to the node, then
-- `node` while it walks the node's current rows in primary-key order, then
-- `done`. `cursor` is the last key of the phase's walk, NULL before the first
-- page.
create table partition_correction_progress (
    correction_id bigint not null references partition_corrections (id) on delete cascade,
    definition_id bigint not null references transform_definitions (id) on delete cascade,
    phase text not null check (phase in ('fence', 'ledger', 'node', 'done')),
    fence_xid xid8,
    cursor text,
    keys_done bigint not null default 0,
    keys_total bigint,
    primary key (correction_id, definition_id)
);

-- What each column a captured table's capture images looked like when
-- Trellis last looked, one row per (table, node, column). A node is the table
-- itself, or each leaf of a partitioned table.
--
-- An install, widen or narrow records the columns its spec images in its own
-- transaction. The capture pass compares the live `pg_attribute` row and file
-- node with each row, acts on what it finds, and writes what it read back in
-- the same transaction:
--   * `attnum`, `type_oid`/`type_name` and `typmod`: the column's identity,
--     for a column dropped and re-added, or retyped;
--   * `attr_xmin`, `attr_hash` and `filenode`: the `xmin` of the column's
--     `pg_attribute` row, a hash of the rest of that row with its default
--     expression, and the node's file node, for a same-type
--     `ALTER COLUMN ... TYPE ... USING` that rewrote values.
create table capture_baselines (
    table_name text not null,
    node oid not null,
    column_name text not null,
    filenode oid,
    attnum smallint not null,
    type_oid oid not null,
    type_name text not null,
    typmod integer not null,
    attr_xmin text not null,
    attr_hash text not null,
    primary key (table_name, node, column_name)
);

comment on table capture_baselines is
    'Trellis: never rebuild this table by create-copy-rename in a migration; use ALTER. '
    'Copied rows look exactly like a restore: the oid of the table holding them changes.';

-- What each of a captured table's Trellis capture triggers looked like when
-- Trellis last installed, widened, narrowed or repaired its capture, one row
-- per (table, node, trigger); `node_oid` is the oid of the table the trigger
-- sits on, and 0 for a plain table, which is judged by its name. The capture
-- pass compares the live `pg_trigger` rows with these and pauses the table's
-- readers when any differs: the trigger is gone, has a new oid or `xmin`
-- (recreated, or altered by name), or is not `ENABLE ALWAYS`. Every install,
-- widen, narrow and repair writes the rows in its own transaction, so
-- Trellis's own changes never read as a disruption.
--
-- `system_identifier` and `catalog_oid` (the oid of this table) say whether
-- the rows survived a dump and restore or `pg_upgrade`, which recreate every
-- trigger under a new oid: a row whose stored value differs from the live one
-- is re-recorded, not acted on.
create table capture_triggers (
    table_name text not null,
    trigger_name text not null,
    trigger_oid oid not null,
    trigger_xmin text not null,
    enabled "char" not null,
    system_identifier text not null,
    catalog_oid oid not null,
    node_oid oid not null,
    primary key (table_name, node_oid, trigger_name)
);

comment on table capture_triggers is
    'Trellis: never rebuild this table by create-copy-rename in a migration; use ALTER. '
    'Copied rows look exactly like a restore (their catalog_oid no longer names this '
    'table), which pauses every definition.';

-- Why a definition is paused by the capture pass, as `Trellis::status`
-- reports it. `error` is the sentence reported, written where the cause is
-- known. `kind` says what paused it: `capture` (a column it reads was
-- renamed, dropped or retyped, or its source's primary key was redefined),
-- `halt` (a halting failure), or `disruption` (Trellis's capture was changed
-- outside Trellis: a trigger or capture function altered, disabled or
-- recreated, or a read table gone; the pauses a fixture load or bulk loader
-- leaves, so a test helper resumes exactly them).
--
-- `caused_by` is the definition whose resume re-typed the columns this pause
-- is about, when that resume is what paused it, so following it finds every
-- definition a resume paused, transitively. It has no foreign key: taking a
-- key lock on the upstream definition's row from the pause's transaction
-- would add a lock to an order that has none. `restored` is set when the
-- definition was paused because the database was restored from a logical
-- dump; its ordering state holds the original database's WAL positions and
-- transaction ids, so a resume resets it before the rebuild. `relabel_lsn` is
-- the WAL insert position the pass read when it found a relabel of an enum
-- type the definition reads: a resume waits until no change staged with the
-- old label is pending.
create table capture_failures (
    transform_id bigint primary key references transform_definitions (id) on delete cascade,
    source_table text not null,
    columns text[] not null,
    detected_at timestamptz not null default now(),
    error text not null,
    kind text not null default 'capture',
    caused_by bigint,
    restored boolean not null default false,
    relabel_lsn pg_lsn,
    constraint capture_failures_kind_check check (kind in ('capture', 'halt', 'disruption'))
);

-- What holds each table's capture back, as the staging worker's latest
-- reconcile pass found it, so `Trellis::status` reports it from any process.
-- Either a lock wait (the install, widen or uninstall could not take the
-- table's lock: `operation`, `lock_mode`, `observed_at`, `blockers`) or
-- another failure (no primary key, a failed statement: `error`, `columns`).
-- Every pass rewrites the table: a row goes as soon as a pass brings its
-- table current, pauses its readers, or no longer captures it. `since` is
-- when a pass first met this wait or this failure.
create table capture_holdups (
    table_name text primary key,
    since timestamptz not null,
    operation text,
    lock_mode text,
    observed_at timestamptz,
    blockers text[],
    error text,
    columns text[],
    constraint capture_holdups_check check (
        (operation is not null and lock_mode is not null and observed_at is not null
            and blockers is not null and error is null and columns is null)
        or (operation is null and lock_mode is null and observed_at is null
            and blockers is null and error is not null and columns is not null))
);

-- The capture gate an install or widen records: the WAL position below which
-- a table's old trigger body staged its rows. It holds a definition's build
-- until no change at or below it is pending, and is deleted by the first
-- capture pass to find it released. One row per table.
create table capture_gates (
    table_name text primary key,
    gate_lsn pg_lsn not null
);

-- One row per target column a resume re-typed, written in the re-type's own
-- transaction so a crash does not lose the cause. A resume that re-types the
-- columns of a definition's target can pause the definitions chained off that
-- target, whose own columns were typed from the old types; the capture pass
-- records on each such pause which definition's resume caused it. A later
-- re-type of the same column replaces the row. The pass reads a row as the
-- cause only while the column still has the type (by OID) the row names and
-- the definition it names still writes the table. No foreign key, as for
-- `retype_releases`. `old_type` and `new_type` are only the words of the
-- message.
create table retype_causes (
    table_name text not null,
    column_name text not null,
    transform_id bigint not null,
    old_type text not null,
    new_type text not null,
    new_type_oid bigint not null,
    retyped_at timestamptz not null default now(),
    primary key (table_name, column_name)
);
