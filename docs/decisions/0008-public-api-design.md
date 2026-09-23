---
status: accepted
date: 2026-09-12
deciders: Michael Ries
---

# Public API Design

Issue #82 asks for a "deep interface": one connection, one minimal set of
entrypoints, the same grammar for defining a transform as a relationship —
mirroring how Postgres exposes only SQL, not a protocol per feature. It's the
blocker for #87 (embedding Trellis into Rails/Elixir over an FFI boundary via
Rustler or Magnus/rutie), which can't pick an embedding mechanism until the
shape of what's being embedded is settled.

Most of it already exists. `trellis/src/app.rs`'s `Trellis` facade (commit
`5e55ee0`) implements the planned interactions: `connect(config, options)`,
grammar-driven `define(text)`/`define_relationship(text)` (via `defs::parse`),
typed `definitions()`/`relationships()`/`request_backfill(table)`/`poisoned_since(watermark)`,
and `shutdown()`. This ADR settles what wasn't: crossing FFI (#87), the
grammar-vs-typed-method split, and quarantine/status observability (its own
epic, #49).

## Decisions

### 1. Synchronous calls at the FFI boundary

`Trellis`'s methods are `async fn`, requiring the caller inside a Tokio runtime
— fine for Rust, but #87 puts a host language on the other side of an FFI call,
and NIF calling conventions (Rustler, Magnus/rutie) are synchronous. Rustler
can bridge to async via dirty schedulers; Ruby has no equivalent. Two
async-bridging strategies aren't worth it when neither host needs concurrent
in-flight calls here.

**Decision:** the FFI surface is synchronous and blocks only on *registration*,
never backfill. `define()` blocks just long enough to create the target table,
capture the coverage fence, and persist the definition / enumerate its backfill
work — then returns before a single target row is built.

> [ADR-0016](0016-single-background-capture-path.md) narrows this further:
> `define()` creates the target table and persists the definition as
> `waiting_to_backfill`, and nothing else. It captures no fence and enumerates
> nothing. The backfill discharge does both in the background.

The reason is scale: even the chunked direct-build path
([ADR-0007](0007-direct-set-based-backfill.md)) takes real wall-clock time on a
billion-row table, and blocking that long means an interrupted process loses
all progress. Backfill is therefore always background and resumable: the chunked
writes are a durable, claimable work queue that running `application_threads`
execute — the same claim/heartbeat/reclaim-stale machinery used for sealed ring
segments. (`staging_worker` is unrelated; it only keeps up with the replication
slot.)

This is issue #55's transform status lifecycle (`waiting_to_backfill` →
`backfilling` → `live`, plus `quarantined`): a host defines a transform, then
polls `status(transform)` (see #4) until `live` — the poll-for-completion
pattern embedding wants. Progress requires at least one `application_threads > 0`
worker running *somewhere* in the fleet; a connection that only ever calls
`define()` will see its definition sit in `waiting_to_backfill` indefinitely —
document this for embedders.

**Settled:** `BlockingTrellis` (`trellis/src/blocking.rs`) is an additive
wrapper in the `trellis` crate, not a separate shim. `Trellis` stays
async-native; `BlockingTrellis` owns a dedicated thread with its own Tokio
runtime (the pattern `Client::start` already uses) and returns
`TrellisError::CalledFromAsyncContext` rather than panicking when called from a
thread that already has a runtime entered.

### 2. Grammar scope: definitional statements only

**Decision:** keep the grammar for declarative, write-shaped statements
(`TRANSFORM`, `RELATIONSHIP`, plausibly future `PAUSE`/`RESUME`), but keep
status/listing reads as typed methods.

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
(`Parse`, `Validation`, `Connectivity`, `Conflict`, `NotFound`, `Internal`).
Every caller-facing error type (`TrellisError`, `ClientError`, `CatalogError`,
`ApplyError`, ...) gained a `code()` method, with SQLSTATE-based
`classify_pg_error` for raw Postgres errors. `source()`/chaining stays
Rust-idiomatic internally (`thiserror`, `#[from]`) and does not cross FFI; a
caller gets `code()` plus the `Display` message, not a chain to walk.

### 4. Resource caps: out of scope

#82 calls for "a fixed resource cap (≤1GB memory, fixed threads)" per client.
`ClientOptions` already has knobs (`application_threads`, `spill_threshold`,
`hard_cap`, `heartbeat`, ...) that bound thread count and memory.

**Decision:** out of scope. The existing knobs give operators the levers;
turning "≤1GB" into a single enforced budget the engine translates into
individual knobs is a tuning/deployment problem, not an API-shape one. Revisit
if operators struggle to hit the target with today's knobs.

### 5. Quarantine/status: row-and-column granularity

Working through #82's "watching for quarantine events" surfaced that
whole-transform quarantine is too coarse: one broken formula shouldn't force
every healthy column in the same `TRANSFORM` into quarantine.

**Decision:** see [ADR-0003](0003-quarantine-storage-and-api.md) for the full
storage and fuse design. What it settles (implemented, `V21__column_quarantine.sql`):

* Whole-key fuse exception detail stays one record per poisoned **source row**
  (`poison`, unchanged). Column-grain detail lives in a separate
  `column_failures` table, one row per `(transform, column, src_table, key)`.
* A new `column_status` table tracks which columns are `paused`, separate from
  and finer than the transform's overall lifecycle status. A `live` transform
  can have individually paused columns.
* The fuse trips per `(transform, column)`; the original whole-transform fuse
  remains as a coarser fallback for failures not attributable to one column.
* **Addressing:** a target is `transform` (whole keyspace) or `transform.column`
  — reusing the grammar's existing `table.column` shape.
* **Client API** (typed methods, per #2): `quarantined()`,
  `quarantine_status()`, `sample_quarantined()`, `resume_column()` on
  `Trellis`/`BlockingTrellis`.
* Threshold, escalation, paused-value semantics, and propagation to dependents
  are settled in [ADR-0003](0003-quarantine-storage-and-api.md).

## Open questions

* **Multi-instance lifetime for FFI:** one long-lived `Trellis` handle per app
  boot (shared via opaque handle/refcount) or a fresh connection per call?
  Bears on #87's mechanism choice.
* How `PAUSE`/`RESUME` would be phrased, and whether they need their own ADR
  given they mutate ADR-0003's column-status table.
* Whether the observability epic (#49: metrics registry, Prometheus via #53,
  structured logs via #56) exposes through this `Trellis` facade or a separate
  handle — #82's "prometheus instrumentation" pattern isn't reconciled here yet.
* Redefinition (#12, in-place column edits) isn't addressed here at all.

## Related issues

#82 (this design), #87 (embedding, blocked on #82), #49/#51/#53/#55/#56
(observability, adjacent), #12 (redefinition, adjacent).
