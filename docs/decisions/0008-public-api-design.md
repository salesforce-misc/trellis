---
status: accepted
date: 2026-09-12
deciders: Michael Ries
---

# Public API Design

Issue #82 ("Design Public API") asked for this design. This ADR records it:
five decisions about the public API's shape, all now implemented (each section
points to what shipped). The "Open questions not yet worked through" section at
the end lists items that remain genuinely open — future scope for adjacent
issues (#87, #49, #12), not this document's own decisions.

## Why now

Issue #82 asks for a "deep interface": one connection, one minimal set of
entrypoints, the same grammar for defining a transform as for defining a
relationship — mirroring how Postgres itself only has SQL, not a different
protocol per feature. It's also the explicit blocker for issue #87 (embedding
Trellis into a host language — Rails/Elixir — over an FFI boundary via
something like Rustler or Magnus/rutie): #87 can't pick an embedding mechanism
until the shape of what's being embedded is settled.

## What already exists

`trellis/src/app.rs`'s `Trellis` facade (from commit `5e55ee0`) already
implements most of the "planned interactions" #82 lists:

* `Trellis::connect(config, options)` — one entrypoint, `TrellisOptions`
  controls whether this connection runs the staging worker and/or drain
  threads.
* `migrate()`, `define(text)`, `define_relationship(text)` — grammar-driven
  (`defs::parse`) for the two declarative statement kinds.
* `definitions()`, `relationships()` — typed listing methods, not grammar.
  `request_backfill(table)`, `poisoned_since(watermark)` — typed action/read
  methods, not grammar.
* `shutdown()`.

So the "deep interface" isn't a green-field design — it's mostly already
built. This ADR settles what wasn't: (a) how it crosses an FFI boundary for
#87, (b) whether the grammar-vs-typed-method split above is the intended
long-term shape, and (c) how quarantine/status observability (a named use case
in #82, and its own epic in #49) plugs into this facade. The decisions below
cover each.

## Decisions

### 1. Blocking (synchronous) calls at the FFI boundary

`Trellis`'s methods are `async fn` today, requiring the caller to already be
inside a Tokio runtime. That's fine for Rust callers, but #87's whole premise
is a host language on the *other* side of an FFI call, and NIF calling
conventions (Rustler for Elixir, Magnus/rutie for Ruby) are fundamentally
synchronous — a call blocks until it returns a value. Rustler can bridge to
async Rust via dirty schedulers; Ruby has no comparable idiom. Building two
different async-bridging strategies for one API, when neither host language
actually needs concurrent in-flight calls at this boundary, isn't worth it.

**Decision:** the FFI-facing surface is synchronous, and it blocks only on
*registration*, never on backfill — regardless of which build path a
definition uses. `define()` blocks (on a dedicated thread owning its own
runtime — the same pattern `Client::start` already uses internally) just long
enough to create the target table, capture the coverage fence, and persist the
definition/enumerate its backfill work; it returns before a single row of the
target is actually built.

The reasoning is scale: even the fast, chunked direct-build path
([ADR-0007](0007-direct-set-based-backfill.md)) takes real wall-clock time on a
billion-row table, and blocking a call — or an in-call loop — for however long
that takes means an interrupted process (the FFI caller's, or the one doing
the building) loses all progress. Backfill is therefore *always* background and
resumable for both build paths: the chunked writes ADR-0007 breaks the direct
build into are a durable, claimable work queue that running drain (application)
threads execute — the same claim/heartbeat/reclaim-stale machinery they already
use for sealed ring segments — rather than an in-call loop on whatever
connection happened to call `define()`. `staging_worker` is unrelated to this
(it only owns keeping up with the logical replication slot); it's
`application_threads` that finishes transform work, backfill included.

**Follow-on:** this is exactly what issue #55's transform status lifecycle
(`waiting_to_backfill` → `backfilling` → `live`, plus `quarantined`) is for. A
host-language caller defines a transform (sync call returns once registered
and its backfill work is queued), then polls `status(transform)` (see #4
below) until it's `live` — the same poll-for-completion pattern requested for
embedding. Note this now means progress requires at least one drain-thread
worker running *somewhere* in the fleet; a connection that only ever calls
`define()` and never runs any client with `application_threads > 0` will see
its definition sit in `waiting_to_backfill` indefinitely — worth calling out
in whatever documentation eventually covers this for embedders.

**Settled:** the synchronous wrapper (`BlockingTrellis`, `trellis/src/blocking.rs`)
lives directly in the `trellis` crate alongside `Trellis`, not in a separate
shim crate. `trellis` remains async-native (`Trellis`'s own methods are untouched);
`BlockingTrellis` is an additive wrapper that owns a dedicated thread running
its own Tokio runtime (the same pattern `Client::start` already uses
internally) and guards against being called from a thread that already has a
runtime entered (`TrellisError::CalledFromAsyncContext`), rather than
panicking.

### 2. Grammar scope: definitional statements only

#82's "same grammar" framing raised the question of whether *every*
operation — not just `TRANSFORM`/`RELATIONSHIP` — should route through
`defs::parse`, including status queries, backfill requests, and poison
inspection.

**Decision:** keep the grammar for declarative, write-shaped statements
(`TRANSFORM`, `RELATIONSHIP`, and plausibly future imperative one-shot
commands like `PAUSE`/`RESUME` for a column) but keep **status/listing reads**
as typed method calls, not grammar text.

Reasoning: a status read isn't a declaration, it's a filtered, paginated
query — "everything currently paused," "page 2 of this transform's poisoned
rows" — and it's going to be the highest-frequency call in the API (a status
poller, per #1 above, hits it repeatedly). `TRANSFORM`/`RELATIONSHIP` earn a
grammar because they need real declarative structure (source table, joins,
field expressions); a status query's structure is filters/sort/pagination,
and expressing that as text means growing the grammar into an actual
`WHERE`-clause sublanguage — a much bigger parsing/validation surface than any
DDL-style statement needed, paid on every call of the hottest, most
latency-sensitive path in the API. `poisoned_since()` already draws this line
correctly today; ADR-0003 (see below) extends it rather than replacing it.

This is a deliberate exception to "everything through one grammar," not an
oversight — worth flagging in case #82's eventual writeup wants to call it out
explicitly as a design tradeoff rather than let it look inconsistent.

### 3. Stable, FFI-safe error representation

`TrellisError` (and `ClientError` beneath it) currently wraps native Rust
types directly — `tokio_postgres::Error`, nested crate-specific enums. None of
that crosses an FFI boundary cleanly (no stable layout, no meaningful
`Display` in every host language without reimplementing formatting).

**Decision:** design a stable error representation now — a code (stable
string or small int enum, not the wrapped Rust type) plus a human-readable
message — and have `TrellisError` itself adopt it, rather than leaving
Rust-idiomatic nested enums in place and translating only at the FFI shim
later. Settling this now avoids the FFI shim work in #87 turning into a giant
`match` over every internal error variant that ever gets added.

**Settled:** `ErrorCode` (`trellis/src/error_code.rs`) is a small,
`#[non_exhaustive]` enum of coarse categories (`Parse`, `Validation`,
`Connectivity`, `Conflict`, `NotFound`, `Internal`) — one code per broad kind
of failure an FFI caller would actually branch on, not one per internal Rust
error variant. Every error type in the crate that can surface to a caller
(`TrellisError`, `ClientError`, `CatalogError`, `ApplyError`, and others) gained
a `code()` method mapping itself into this taxonomy, with SQLSTATE-based
classification (`classify_pg_error`) for raw Postgres errors. `source()`/error
chaining stays Rust-idiomatic internally (`std::error::Error`, `#[from]` via
`thiserror`) — it does not itself cross the FFI boundary; a caller gets the
stable `code()` plus the `Display` message, not a chain to walk.

### 4. Resource caps: out of scope for the API shape

#82 calls for "a fixed resource cap (i.e. fixed number of threads and never
more than 1GB of memory usage)" per client. `ClientOptions` already has many
separate knobs (`application_threads`, `spill_threshold`, `hard_cap`,
`heartbeat`, ...) that between them bound both thread count and memory, but no
single unified budget.

**Decision:** out of scope for this design. The existing knobs already give an
operator the levers to hit a target; turning "≤1GB" into a first-class,
enforced API concept (a single budget number the engine translates into the
individual knobs itself) is a tuning/deployment-guidance problem, not a public
API shape problem. Revisit if operators actually struggle to hit the target
with today's knobs.

### 5. Quarantine/status: row-and-column granularity

Originally #82 only asked for "watching for quarantine events" as one of the
planned interaction patterns. Working through it surfaced that table-level
(whole-transform) quarantine is too coarse: a `TRANSFORM` can compute several
calculated columns, and one broken formula shouldn't force every other healthy
column into quarantine.

**Decision:** see [ADR-0003](0003-quarantine-storage-and-api.md) for the full
storage and fuse design. Summary of what it settles (all now implemented, `V21__column_quarantine.sql`):

* Exception detail for the existing whole-key fuse stays one record per
  poisoned **source row** (`poison`, unchanged). Column-grain failure detail
  is tracked separately in a dedicated `column_failures` table, one row per
  `(transform, column, src_table, key)` that failed.
* A new `column_status` table tracks which columns are currently `paused`,
  separate from — and finer-grained than — a transform's overall lifecycle
  status (`waiting_to_backfill`/`backfilling`/`live`/`quarantined`). A `live`
  transform can have individually paused columns.
* The fuse now trips per `(transform, column)`, not per transform. The
  original whole-transform fuse still exists as a coarser fallback tier for
  failures that aren't attributable to one column.
* **Addressing:** a target is either `transform` (whole keyspace) or
  `transform.column` (one field) — reusing the `table.column` shape the
  grammar already has elsewhere, not inventing a new addressing idiom.
* **Client library API** (typed methods, per decision #2 above): list
  everything currently paused/quarantined across every transform and column;
  get status for one address; page sample poisoned rows for one address.
  Implemented on `Trellis`/`BlockingTrellis` as `quarantined()`,
  `quarantine_status()`, `sample_quarantined()`, `resume_column()`.
* Threshold, counter mechanism, escalation, paused-value semantics, and
  propagation to dependents are settled in
  [ADR-0003](0003-quarantine-storage-and-api.md).

## Open questions not yet worked through

* Multi-instance lifetime for FFI: does an embedding host process create one
  `Trellis` handle per app boot (long-lived, shared across calls via an opaque
  handle/reference count) or is a fresh connection expected per call? Bears
  directly on #87's embedding mechanism choice.
* How `PAUSE`/`RESUME` (mentioned above as plausible grammar statements) would
  actually be phrased, and whether they need their own ADR given they mutate
  the column-status table from ADR-0003.
* Whether the observability epic (#49: metrics registry, Prometheus exposition
  via #53, structured logs via #56) exposes through this same `Trellis` facade
  or a separate handle — #82's "pulling instrumentation data for prometheus"
  interaction pattern hasn't been reconciled with this doc yet.
* Redefinition (#12, in-place column edits) isn't addressed here at all yet.

## Related issues

#82 (this design), #87 (blocked on #82, embedding mechanism), #49/#51/#53/#55/#56
(observability epic, adjacent), #12 (redefinition API, adjacent).
