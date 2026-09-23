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
always runs an `insert ... on conflict (singleton) do nothing`. That makes
seeding idempotent across three cases: a clean re-attach (row already matches),
crash recovery (row absent, so inserted), and two processes racing to migrate
the same new schema (the loser's insert becomes a no-op, not a unique-violation).

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
  lock tag. So the "exactly one CDC intake producer" guard
  (`staging::session`) must derive its key from the instance schema, as
  `staging::session::producer_singleton_lock_key` now does; a global constant
  made the guard fire across instances.
* **A configured schema must be carried, not re-resolved.**
  `Client::start_with_config` takes the caller's already-resolved `Config`.
  Its DSN-only sibling `Client::start` resolves the schema from the process
  environment, which makes the instance identity a property of the *process* —
  fine for a one-instance process, wrong for anything holding a `Config` (like
  `Trellis`) and impossible for two instances in one process.

Two things remain the operator's responsibility, not the engine's:

* **Distinct replication slot and publication names** per instance
  (`ClientOptions::slot`/`publication`). A slot name is unique cluster-wide;
  a publication name is unique per database. Neither is schema-qualified.
* **Distinct transform target schemas** (`Config::target_schema`) if the two
  instances materialize similarly-named targets. Target tables are
  application data, deliberately outside the instance schema (see
  `DEFAULT_TARGET_SCHEMA`), so nothing keeps two instances' targets apart
  automatically.

An instance can read another instance's 1-1 target as a source, exactly as it
would any other table with a primary key. It cannot read another instance's
aggregate target. Trellis requires a source table to have a primary key
([transforms — Source tables](transforms.md#source-tables)), and an aggregate
target has none: its grouping columns may be `NULL`, so its identity is a
`UNIQUE NULLS NOT DISTINCT` constraint. Inside the owning instance that never
matters, because an instance hands each write to one of its own targets on to
that target's readers in the writing transaction, without logical replication.
That internal path stops at the instance boundary. The reading instance's only
view of the table is CDC through its own publication, and it has no primary key
to identify a change by, so `CREATE TRANSFORM` rejects the definition with
`SourceNotChangeKeyed` (issue #376). Publishing the table would also make
Postgres refuse the owning instance's own updates to it, since it has no replica
identity. To chain off an aggregate target, define the downstream transform in
the instance that owns it. The same goes for a relationship: `CREATE
RELATIONSHIP` rejects an endpoint that is another instance's aggregate target
with `RelationshipEndpointNotChangeKeyed` (issue #375), since the relationship
would publish it just the same.

One behaviour to expect rather than debug: convergence latency is coupled
across co-tenant instances. `staging::watermark_token` is
`pg_current_wal_lsn()`, a cluster-wide LSN, so "has this instance converged?"
is asked against WAL the instance's own publication filters out. Confirming
past that WAL waits for a keepalive, whose persist is throttled to
`intake::KEEPALIVE_PERSIST_INTERVAL`. A busy neighbour therefore adds up to
that interval to an instance's observed convergence time. Correctness is
unaffected.
