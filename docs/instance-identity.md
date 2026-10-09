# Instance Identity

Every Trellis instance owns exactly one **named PostgreSQL schema**, and all of
its internal state lives there: the [staging
ring](staging-and-claiming/02-the-staging-ring.md), the migration ledger, and —
as the project grows — the transform catalog and other bookkeeping. Nothing
Trellis owns lives in `public` or under unqualified names.

## Why a named schema

* **A clean footprint.** Everything Trellis creates is qualified by one schema,
  so an install can be inspected, backed up, or dropped as a unit without
  touching the application tables sharing the database.
* **Multiple instances per cluster.** The schema name is configurable, so
  several Trellis instances can coexist in one cluster — even one database —
  each isolated within its own schema.

## Default and configuration

Trellis qualifies its records under a schema named `trellis` by default,
configurable via the `TRELLIS_SCHEMA` environment variable.

The schema is created if absent when migrations run, and the connection pool
pins `search_path` to it on every connection, so Trellis's own SQL never
qualifies names by hand. Resolution and defaulting live in
`trellis/src/config.rs` (`DEFAULT_SCHEMA`); the `search_path` seam lives in
`trellis/src/pool.rs`. `Config::schema()` exposes the resolved schema, and
`Config`'s `Display` impl gives a one-line summary for logging.

`config::validate_schema_name` rejects an unusable name before it reaches SQL:
empty or all-whitespace, containing a NUL byte, or longer than Postgres's
63-byte `NAMEDATALEN` limit (which Postgres would silently truncate, risking a
collision between two configured names). Beyond that the character set is
unrestricted — every place the name reaches SQL goes through
`pool::quote_ident` as a quoted identifier, so Postgres decides what's
acceptable. `Config`'s `dsn` and `schema` fields are private, and every
constructor (`Config::resolve` and `Config::from_dsn`, both delegating to
`Config::with_schema`) validates the schema, so no `Config` can carry an
unvalidated name.

## Detecting a foreign or incompatible schema

A shared name doesn't prove two attaches are the same instance — the schema
could have been recreated, restored under the wrong name, or never been
Trellis's. So each instance writes a single-row identity marker,
`trellis_instance` (migration V9), recording its schema name and an
`instance_format_version` (`trellis::identity::INSTANCE_FORMAT_VERSION`).

Before the migration runner touches the schema, `trellis::identity::prepare_attach`
(called from `trellis::migrate`) checks it:

* **Fresh, or ours mid-migration** — schema absent, empty, or holding
  Trellis's migration ledger (`refinery_schema_history`) but no marker yet.
  Safe to (re)take. The mid-migration case matters for crash recovery:
  `migrate` isn't one transaction (refinery commits each migration separately,
  and the marker seed is a separate statement), so a process death between
  creating `trellis_instance` and committing the seed leaves our tables and
  ledger with an empty marker table. The ledger's presence is what
  distinguishes "ours, unfinished" from "genuinely foreign," so this is
  treated as a fresh attach.
* **Same instance** — marker present, schema name matches, format version
  understood. A clean idempotent no-op.
* **Refused** — a mismatched schema name in the marker, a format version newer
  than this build understands, or a pre-existing schema with foreign objects
  and *neither* a marker *nor* a migration ledger. Each surfaces as a typed
  `Error::IncompatibleInstance`.

After the runner ensures `trellis_instance` exists, `trellis::identity::seed_marker`
always runs an `insert ... on conflict (singleton) do update`. That makes
seeding idempotent across four cases: a clean re-attach (row already matches),
crash recovery (row absent, so inserted), a second client of the same instance
(the row exists, so nothing changes), and a changed target schema (see below).

### A catalog schema that belongs to another instance

Several instances can share a database, so an instance's catalog schema must
not be a schema another instance owns or writes to. Besides the cases above,
`prepare_attach` refuses, before it creates anything:

1. a catalog schema of `public`, which is the application's schema;
2. a catalog schema equal to this instance's own target schema
   (`Config::target_schema`);
3. a target schema that holds another instance's catalog, that is, one with a
   `trellis_instance` table;
4. a catalog schema that is another instance's target schema.

Each refusal is an `Error::IncompatibleInstance` naming the schema and, for 3
and 4, the other instance's catalog schema. The first two read only the
`Config`. The others scan the database for `trellis_instance` tables outside
the instance's own schema. Rule 4 needs the other instance's target schema, so
the marker records it (`trellis_instance.target_schema`, migration V83) and
`seed_marker` rewrites it on every attach. The target schema is configuration
that can change between deploys, so a changed value updates the marker; it
never refuses. The scan skips the instance's own marker, so a second client of
the same instance, whose marker matches, attaches as it always has. Whichever
of two conflicting instances attaches second is the one refused.

The scan sees a marker only if the role attaching can read it, and an
instance's marker only once that instance has attached at V83 or later. A role
without `USAGE` on another instance's schema still sees rule 3 (the table
exists), but not rule 4. Rule 3 and rule 4 compare the catalog schema with
`Config::target_schema` only; a transform whose target names a different schema
than `Config::target_schema` isn't covered.

A marker is written at the end of an attach, so the check must not run while
another attach is between its own check and its marker write. `migrate` holds
one database-wide advisory lock (`identity::AttachLock`, a session lock on a
dedicated connection, so it can't outlive the attach) from before the check to
after the marker write. Attaches to one database therefore run one at a time,
and a second one waits for the first for up to five minutes. Without it, two
conflicting instances starting together would each scan before the other wrote
its marker and both pass, and two clients of one instance starting together
would both run the migration runner on the same new schema.

`trellis::identity::Identity::resolved` reports the identity (schema + format
version) this build would attach as, without touching the database.

## What "coexisting instances" actually requires

The named schema is necessary but not sufficient. Anything an instance treats
as privately its own has to be scoped by that schema too, or two instances in
one database will collide on it even though every table they own is separate.
Issue #234's two-instance generative property
(`generative/tests/two_instance_noise.rs`) runs two instances side by side in
one database to keep this honest; it found two real gaps, both since closed:

* **The producer singleton is keyed by schema.** Postgres advisory locks are
  keyed by `(database, key)` — a session's `search_path` is not part of the
  lock tag. So the "exactly one staging worker" guard
  (`staging::session`) must derive its key from the instance schema, as
  `staging::session::producer_singleton_lock_key` now does; a global constant
  made the guard fire across instances.
* **A configured schema must be carried, not re-resolved.**
  `Client::start_with_config` takes the caller's already-resolved `Config`.
  Its DSN-only sibling `Client::start` resolves the schema from the process
  environment, which makes the instance identity a property of the *process* —
  fine for a one-instance process, wrong for anything holding a `Config` (like
  `Trellis`) and impossible for two instances in one process.

Capture triggers are named after the instance schema
(`capture::sql::trigger_name`), and their functions live in it, so two
instances can capture one table without replacing each other's triggers.

The wake channel is named after it too. `LISTEN/NOTIFY` channels are
per database, so a shared channel name would make every seal, drain and
backfill in one instance wake the idle workers of every other. By default an
instance's channel is `<schema>_wake` (`client::default_wake_channel`), so the
default instance listens on `trellis_wake`. A schema too long for that to fit in
63 bytes keeps a prefix of its name plus a hash of the whole name, the way
trigger names do. `ClientOptions::wake_channel` overrides the default; every
`pg_notify` and the workers' `LISTEN` use the one resolved name
(`ClientOptions::wake_channel_for`).

One thing remains the operator's responsibility, not the engine's:

* **Distinct transform target schemas** (`Config::target_schema`) if the two
  instances materialize similarly-named targets. Target tables are
  application data, deliberately outside the instance schema (see
  `DEFAULT_TARGET_SCHEMA`), so nothing keeps two instances' targets apart
  automatically. Only the targets you name need this. The tables Trellis
  generates for itself, such as a to-one relationship's projection
  (`_trellis_rel_projection_<id>`, numbered per instance), live in the instance
  schema, so two instances sharing a target schema can't collide on them
  (issue #435).

An instance can read another instance's 1-1 target as a source, exactly as it
would any other table with a primary key. It cannot read another instance's
aggregate target. Trellis requires a source table to have a primary key
([transforms — Supported sources and targets](transforms.md#supported-sources-and-targets)), and an aggregate
target has none: its grouping columns may be `NULL`, so its identity is a
`UNIQUE NULLS NOT DISTINCT` constraint. Inside the owning instance that never
matters, because an instance hands each write to one of its own targets on to
that target's readers in the writing transaction, without capture triggers.
That internal path stops at the instance boundary. The reading instance's only
view of the table is its own capture triggers, and they have no primary key
to identify a change by, so `CREATE TRANSFORM` rejects the definition with
`SourceNotChangeKeyed` (issue #376). To chain off an aggregate target, define
the downstream transform in the instance that owns it. The same goes for a relationship: `CREATE
RELATIONSHIP` rejects an endpoint that is another instance's aggregate target
with `RelationshipEndpointNotChangeKeyed` (issue #375), since the relationship
would capture it just the same.

Co-tenant instances don't slow each other's convergence waits.
`staging::watermark_token` is `pg_current_wal_insert_lsn()`, a cluster-wide LSN, but
an instance's wait reads only its own ring. A change it captures is in the ring
when the change commits, so there is nothing to wait for in WAL a neighbour
wrote.
