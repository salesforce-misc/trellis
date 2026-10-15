-- ADR-0017 P3 (#1061): what the reconcile pass records about each
-- partitioned source, and the corrections it queues.
--
-- `partition_sources` has one row per partitioned root the pass captures:
-- the nodes it last saw (oid, parent, bound, `DEFAULT` flag, the recorded
-- `pg_get_partition_constraintdef` text and the key columns' types and
-- collations), the two fingerprints (`lifecycle_fp`, `compliance_fp`) that say
-- whether the tree changed since, `stale` (the root was renamed away while
-- recorded) and `noncompliant` (the pass paused readers because the source
-- stopped complying, so every pass checks it again).
create table partition_sources (
    table_name text primary key,
    root_oid bigint not null,
    lifecycle_fp text not null,
    compliance_fp text not null,
    nodes jsonb not null,
    stale boolean not null default false,
    noncompliant boolean not null default false
);

-- `partition_corrections` is the durable queue a detected change leaves for
-- the correction engine (P4): one row per node to correct. `new_leaf` is a
-- leaf the pass found created, attached, re-attached, or whose `DEFAULT`
-- siblings changed (`cause` says which), and `gone_node` the topmost node of a
-- dropped or detached subtree, with the recorded constraint text its groups
-- route by. `queued_lsn` is the WAL position when the pass queued it: a
-- consumer corrects only once the ring is folded through it. The row is
-- committed with the fingerprints that detected it, so a restart resumes it.
-- A node queued again before the consumer takes it adds nothing, unless it
-- left the tree under another recorded text: its groups route by each.
create table partition_corrections (
    id bigint generated always as identity primary key,
    table_name text not null,
    kind text not null check (kind in ('new_leaf', 'gone_node')),
    node_oid bigint not null,
    node_name text not null,
    cause text not null,
    constraint_text text,
    queued_lsn pg_lsn not null default pg_current_wal_insert_lsn(),
    queued_at timestamptz not null default now()
);

create unique index partition_corrections_once on partition_corrections
    (table_name, kind, node_oid, (md5(coalesce(constraint_text, ''))));
