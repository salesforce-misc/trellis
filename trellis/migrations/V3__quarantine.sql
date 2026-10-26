-- Quarantine: isolate, evict, park, release. See
-- docs/staging-and-claiming/06-cleanup-and-reclaim.md ("Quarantine") and
-- docs/decisions/0003-quarantine-storage-and-api.md.
--
-- Whole-key quarantine is per definition: a key that fails in one
-- definition's apply is held for that definition only, and every other
-- reader of the source applies it normally. `transform_definitions (id)` is
-- the definition's identity, and `on delete cascade` takes a dropped
-- definition's quarantine with it.

-- The marker: which (definition, source table, key) is currently evicted, so
-- the fold excludes it for that definition. `sqlstate` is the SQLSTATE of
-- the failure that held the key (null where it had none), so a release can
-- tell a value too long (22001) or a numeric overflow (22003) from every
-- other failure, whatever language the server words its messages in.
-- `retry_attempts` counts the automatic releases of the key by the staging
-- worker; the next retry is due `least(initial * 2^retry_attempts, cap)`
-- after `poisoned_at`, so the schedule derives from the retry option.
create table poison (
    transform_id bigint not null references transform_definitions (id) on delete cascade,
    src_table text not null,
    key text not null,
    poisoned_at timestamptz not null default now(),
    last_error text not null,
    sqlstate text,
    retry_attempts integer not null default 0,
    primary key (transform_id, src_table, key)
);

-- The drain's exclusion read looks keys up by table and key across
-- definitions.
create index poison_src_key on poison (src_table, key);

-- The parked work: one row per held key, the fold of every change parked for
-- it, so a key held for good does not gain rows without limit. Every park
-- merges into the row:
--   * `seg_seq`, `old_image`: the earliest parked change's segment and
--     pre-image (the state readers last saw), which the release hands on as
--     its prior image;
--   * `origin_lsn`: the earliest, and null (unknown) once any parked
--     change's is;
--   * `src_changed`: the earliest known;
--   * `hop_gen`: the deepest;
--   * `agg_groups`: on a group-basis aggregate, one object of the
--     definition's `GROUP BY` columns per group the parked changes feed, as
--     text, deduplicated; null for a definition that is not on the group
--     basis. The release recomputes each.
create table poison_held (
    transform_id bigint not null references transform_definitions (id) on delete cascade,
    src_table text not null,
    key text not null,
    seg_seq bigint not null,
    old_image jsonb,
    origin_lsn pg_lsn,
    src_changed timestamptz,
    hop_gen integer not null default 0,
    agg_groups text[],
    primary key (transform_id, src_table, key)
);

-- The counter: how many times a key has been attributed an isolated,
-- non-transient failure. It lives off the ring's own rows (immutable, with
-- no counter of their own) and is cleared by a clean drain.
-- `retry_attempts` carries the automatic-retry count across a release, so a
-- retried key that fails again is poisoned on its first failure.
create table key_deaths (
    transform_id bigint not null references transform_definitions (id) on delete cascade,
    src_table text not null,
    key text not null,
    deaths integer not null default 0,
    last_error text,
    last_death_at timestamptz,
    retry_attempts integer not null default 0,
    primary key (transform_id, src_table, key)
);

-- The whole-transform fuse's serialization point. The fuse check counts a
-- definition's `poison` rows inside the transaction of the eviction that
-- triggers it; two concurrent evictions would each miss the other's
-- uncommitted row. The check's first statement upserts this row, so
-- concurrent evictions queue on its lock and the second counts the first's
-- committed row. The lock, not the value, is the point; `checks` and
-- `last_checked_at` are bookkeeping. Rows are never deleted.
create table transform_fuse_gate (
    transform_id bigint primary key references transform_definitions (id) on delete cascade,
    checks bigint not null default 0,
    last_checked_at timestamptz not null default now()
);

-- A process-wide counter of the stops the halting class of failures caused:
-- one row (a boolean primary key) with the count and the last reason.
create table halting_stops (
    id boolean primary key default true check (id),
    stop_count bigint not null default 0,
    last_reason text,
    last_stopped_at timestamptz
);

insert into halting_stops (id) values (true);

-- Column-level quarantine: a failure in one calculated column pauses that
-- column, not the whole transform. A transform with no rows here has every
-- column live.
--
-- `local_fuse` is true when the pair's own fuse tripped, false when it is
-- paused only because an upstream column it reads was paused (a pair can be
-- both). `awaiting_capture` marks an `ALTER TRANSFORM` field whose formula
-- reads a source column the capture triggers do not image yet: it stays
-- paused until the widened capture is installed and drained.
-- `cascade_pending` marks a pause whose walk over its dependents has not
-- finished; the staging worker's capture pass completes any walk still
-- marked.
create table column_status (
    transform_table text not null,
    column_name text not null,
    paused_at timestamptz not null default now(),
    last_error text,
    local_fuse boolean not null default false,
    awaiting_capture boolean not null default false,
    cascade_pending boolean not null default false,
    primary key (transform_table, column_name)
);

create index column_status_cascade_pending
    on column_status (transform_table, column_name) where cascade_pending;

-- The column fuse's incrementally maintained counter of distinct poisoned
-- rows (not failure events; see `column_failures`).
create table column_deaths (
    transform_table text not null,
    column_name text not null,
    deaths integer not null default 0,
    last_error text,
    last_death_at timestamptz,
    primary key (transform_table, column_name)
);

-- The column fuse's per-row failure detail. A row here has not been evicted
-- from folding; it contributed one failure toward its column's fuse count.
-- The primary key makes a retried probe of an already-recorded key a no-op,
-- which is what makes `column_deaths` count distinct rows. It is the data
-- source of the "sample quarantined rows" read and is cleared when the
-- column is resumed.
create table column_failures (
    transform_table text not null,
    column_name text not null,
    src_table text not null,
    key text not null,
    error text not null,
    failed_at timestamptz not null default now(),
    primary key (transform_table, column_name, src_table, key)
);

-- Cascade edges: when an upstream column pauses, every downstream column
-- whose formula reads it pauses too. Recorded so a resume can tell a
-- downstream column paused only by this upstream from one with an independent
-- reason (`column_status.local_fuse`, or another remaining edge).
create table column_pause_cascades (
    downstream_transform text not null,
    downstream_column text not null,
    upstream_transform text not null,
    upstream_column text not null,
    primary key (downstream_transform, downstream_column, upstream_transform, upstream_column)
);

create index column_pause_cascades_upstream_idx
    on column_pause_cascades (upstream_transform, upstream_column);

-- A pending `RESUME`, completed by the staging worker's capture pass. When a
-- resumed definition's typed copies no longer have the type define would
-- give them from the live schema, `RESUME` returns at once and leaves the
-- definition paused with this row; the pass re-types the copies
-- (`ALTER ... TYPE`, under `ACCESS EXCLUSIVE`) and completes the resume,
-- deleting the row in the same transaction. A crash between the two leaves
-- the row for the next pass. `timeout_cancels` counts the re-types cancelled
-- by `statement_timeout` (SQLSTATE 57014) since the request; the request
-- ends once it reaches its limit, and each `RESUME` starts it at 0.
create table resume_requests (
    transform_id bigint primary key references transform_definitions (id) on delete cascade,
    requested_at timestamptz not null default now(),
    timeout_cancels integer not null default 0
);

-- One row per definition and SQLSTATE whose held keys a re-type of a column
-- Trellis created (the source widened without a rewrite) released the cause
-- of, written in the re-type's own transaction so a crash does not lose the
-- release. The staging worker releases the keys after each pass and deletes
-- the row once they are released and its window has passed. It has no
-- foreign key: the re-type holds `ACCESS EXCLUSIVE` on a table a drop takes
-- after the definition's row, so it takes no lock on that row. The release
-- deletes a row whose definition is gone.
create table retype_releases (
    transform_id bigint not null,
    sqlstate text not null,
    requested_at timestamptz not null default now(),
    primary key (transform_id, sqlstate)
);

-- A completed build of a Trellis target asks for the release of every key
-- each direct reader of that target holds: a reader can hold a key for a
-- failure the upstream caused while it was being rebuilt. One row per reader,
-- written in the transaction that completes the upstream's build. The staging
-- worker releases the keys after each capture pass, with the automatic
-- retry's semantics, and deletes the row once they are released and its
-- window has passed. `first_read_at` is when the first release after
-- `requested_at` read the held keys: a later release of the same request
-- takes only keys held since before then. No foreign key, as for
-- `retype_releases`.
create table upstream_releases (
    transform_id bigint primary key,
    requested_at timestamptz not null default now(),
    first_read_at timestamptz
);
