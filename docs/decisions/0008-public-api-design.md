---
status: accepted
date: 2026-09-12
deciders: Michael Ries
---

# Public API Design

The public API is a "deep interface": one connection, one minimal set of entrypoints,
the same grammar for defining a transform as a relationship — mirroring how Postgres
exposes only SQL, not a protocol per feature. It is what lets Trellis be embedded in
Rails/Elixir over an FFI boundary ([ADR-0010](0010-embeddable-clients.md)).

`trellis/src/app.rs`'s `Trellis` facade is that interface: `connect(config, options)`,
one grammar-driven `apply(text)` for every definition change (see
[ADR-0012](0012-curate-public-api-demote-engine-modules.md)), typed reads such as
`definitions()`, `relationships()`, `status()`, `request_backfill(table)` and
`poisoned_since(watermark)`, and `shutdown()`. This ADR records the choices behind it:
crossing FFI, the grammar-vs-typed-method split, the error shape, and
quarantine/status observability.

## Decisions

### 1. Synchronous calls at the FFI boundary

`Trellis`'s methods are `async fn`, requiring the caller inside a Tokio runtime
— fine for Rust, but #87 puts a host language on the other side of an FFI call,
and NIF calling conventions (Rustler, Magnus/rutie) are synchronous. Rustler
can bridge to async via dirty schedulers; Ruby has no equivalent. Two
async-bridging strategies aren't worth it when neither host needs concurrent
in-flight calls here.

**Decision:** the FFI surface is synchronous and blocks only on *registration*,
never backfill. `define` creates the target table, its ledger tables and its chunk
plan, persists the definition, and returns. It captures no fence and reads no source
row; drain workers build in the background
([ADR-0002](0002-async-data-flow.md#a-build-is-re-derive-over-chunks-and-applies-from-its-first-chunk)).

The reason is scale: a build takes real wall-clock time on a billion-row table, and
blocking that long means an interrupted process loses all progress. Backfill is
therefore always background and resumable: the chunked writes are a durable, claimable
work queue that running drain threads (`drain_threads`) execute — the same
claim/heartbeat/reclaim-stale machinery used for sealed ring segments. The staging
worker (`staging`) is separate: it installs capture and maintains the ring.

This is the transform status lifecycle (`waiting_to_backfill` → `backfilling` → `live`,
plus `catching_up`, `quarantined` and `paused`): a host defines a transform, then polls
`status(transform)` (see #4) until `live`. Progress requires a staging worker and at
least one drain thread running *somewhere* in the fleet; a connection that only ever
defines sees its definition sit in `waiting_to_backfill` indefinitely
([embedding](../embedding.md#the-silent-stall-hazard-issue-144)).

`BlockingTrellis` (`trellis/src/blocking.rs`) is an additive wrapper in the `trellis`
crate, not a separate shim. `Trellis` stays async-native; `BlockingTrellis` owns a
dedicated thread with its own Tokio runtime (the pattern `Client::start` already uses)
and returns `TrellisError::CalledFromAsyncContext` rather than panicking when called
from a thread that already has a runtime entered.

### 2. Grammar scope: definitional statements only

**Decision:** the grammar carries declarative, write-shaped statements (`TRANSFORM`,
`RELATIONSHIP`, `ALTER`, `PAUSE`, `RESUME`, `DROP`; see
[ADR-0014](0014-pause-and-drop-a-transform.md)), and status/listing reads stay typed
methods.

A status read isn't a declaration — it's a filtered, paginated query
("everything paused," "page 2 of poisoned rows"), and the highest-frequency
call in the API (the status poller from #1 hits it repeatedly). `TRANSFORM`/
`RELATIONSHIP` earn a grammar because they need declarative structure (source
table, joins, field expressions); a status query's structure is
filters/sort/pagination. Expressing that as text means growing the grammar into
a `WHERE`-clause sublanguage — a far bigger parsing surface, paid on the
hottest, most latency-sensitive path. `poisoned_since()` already draws this
line; ADR-0003 extends it. A deliberate exception to "everything through one
grammar," not an oversight.

### 3. Stable, FFI-safe error representation

`TrellisError` / `ClientError` wrap native Rust types (`tokio_postgres::Error`,
nested enums) that don't cross FFI cleanly — no stable layout, no meaningful
`Display` per host language.

**Decision:** adopt a stable representation now — a code plus a human-readable
message — rather than translating only at the shim later (which would turn into
a giant `match` over every internal variant).

**Settled:** `ErrorCode` (`trellis/src/error_code.rs`) is a small,
`#[non_exhaustive]` enum of coarse categories an FFI caller would branch on
(`Parse`, `Validation`, `Connectivity`, `Conflict`, `NotFound`, `Timeout`,
`Internal`). `Timeout` exists because `await_converged` running out of time is an
expected, retryable outcome that a host must be able to tell from a bug.
Every caller-facing error type (`TrellisError`, `ClientError`, `CatalogError`,
`ApplyError`, ...) has a `code()` method, with SQLSTATE-based
`classify_pg_error` for raw Postgres errors. `source()`/chaining stays
Rust-idiomatic internally (`thiserror`, `#[from]`) and does not cross FFI; a
caller gets `code()` plus the `Display` message, not a chain to walk.

### 4. Resource caps: out of scope

#82 calls for "a fixed resource cap (≤1GB memory, fixed threads)" per client.
`ClientOptions` has knobs (`application_threads`, the drain batch cap, `heartbeat`, ...)
that bound thread count and memory.

**Decision:** out of scope. The existing knobs give operators the levers;
turning "≤1GB" into a single enforced budget the engine translates into
individual knobs is a tuning/deployment problem, not an API-shape one. Revisit
if operators struggle to hit the target with today's knobs.

### 5. Quarantine/status: row-and-column granularity

Whole-transform quarantine alone is too coarse: one broken formula shouldn't force
every healthy column in the same `TRANSFORM` into quarantine.

**Decision:** see [ADR-0003](0003-quarantine-storage-and-api.md) for the full
storage and fuse design. What it settles (`V21__column_quarantine.sql`):

* Whole-key fuse exception detail is one record per **source row** poisoned
  for a transform (`poison`). Column-grain detail lives in a separate
  `column_failures` table, one row per `(transform, column, src_table, key)`.
* A new `column_status` table tracks which columns are `paused`, separate from
  and finer than the transform's overall lifecycle status. A `live` transform
  can have individually paused columns.
* The fuse trips per `(transform, column)`; the original whole-transform fuse
  remains as a coarser fallback for failures not attributable to one column.
* **Addressing:** a target is `transform` (whole keyspace) or `transform.column`
  — reusing the grammar's existing `table.column` shape.
* **Client API** (typed reads, per #2): `quarantined()`, `quarantine_status()` and
  `sample_quarantined()` on `Trellis`/`BlockingTrellis`. Resuming a column is the
  grammar's `RESUME TRANSFORM <target>.<column>`.
* Threshold, escalation, paused-value semantics, and propagation to dependents
  are settled in [ADR-0003](0003-quarantine-storage-and-api.md).

## Related

[ADR-0010](0010-embeddable-clients.md) (the handle model and the FFI boundary),
[ADR-0012](0012-curate-public-api-demote-engine-modules.md) (the one `apply`
entrypoint), [ADR-0009](0009-observability-decisions.md) (metrics and status),
[ADR-0015](0015-transform-redefinition.md) (`ALTER TRANSFORM`).
