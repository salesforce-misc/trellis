---
status: accepted
date: 2026-09-09
deciders: Michael Ries
consulted:
informed:
---

# Direct, Set-Based Backfill Bypasses the Staging Ring

Building a target from an already-populated source is a different problem from
reconciling live CDC. Routing the initial build through the ring — enumerate
every source row, append to the staging ring, claim/fold/compute/apply — is
correct but pathologically slow: issue #63 measured a 1M-row / 100k-group
aggregate at ~55s, dominated by ring bookkeeping over data that is fully present
up front and has no concurrent deltas to reconcile.

## Decision

The initial build is computed **directly, set-based, source→target**, bypassing
the ring. The ring does only what it is uniquely good at: reconciling live CDC
deltas after the build fence. Implemented in [`trellis::defs::backfill`]:

- **1-1 (calc) transforms** walk the source PK in half-open `(lo, hi]` ranges:
  `INSERT INTO target SELECT <exprs> FROM source WHERE <pk> > lo AND <pk> <= hi
  ON CONFLICT (<pk>) DO UPDATE`. Bounds come from `max()`-over-`LIMIT`, so every
  row falls in exactly one range regardless of key gaps.
- **Aggregates** scan the source **once** into a temp staging table (`CREATE TEMP
  TABLE … AS SELECT <group_cols>, <aggs> FROM source GROUP BY <group_cols>` —
  NULL-keyed groups included, issue #128/#110), then chunk the **writes** from
  staging into the target by group-key range (`INSERT INTO target SELECT …
  FROM staging WHERE (<group_cols>) > lo AND (<group_cols>) <= hi ON CONFLICT
  DO UPDATE`) for the non-NULL-keyed groups, plus one final unchunked write for
  every NULL-keyed group (see [below](#null-group-keys-are-built-in-one-unchunked-pass)).
  A non-NULL group is a point in group-key space, so it lands wholly in one
  chunk.

Each chunk write is one bounded transaction. Chunks are not run in an in-call
loop — they are enumerated as a durable, claimable work queue executed by drain
threads, so `install_definition` returns after registering the definition and
its chunk work items (see [Backgrounding and resumability](#backgrounding-and-resumability)).

## Single-pass aggregation, then chunked writes — not chunked aggregation

The aggregate build must **not** chunk by group-key range directly over the
*source*. An earlier revision did, and each chunk's `WHERE (<group_cols>) …
GROUP BY` did a full sequential scan of the whole source (no index on the GROUP
BY columns — one was tried and abandoned, see ADR 0005). With `C` chunks that is
`O(C × source_size)` — the exact per-chunk-rescan pathology milestones 1 and 2
fixed elsewhere in #63, reintroduced.

The accepted design scans the source once; every later read (boundary discovery,
each chunk write) hits the *group-count*-sized staging table. A primary key on
staging's group columns makes each write an index range scan, so even at
near-one-group-per-row cardinality the writes never degrade into full scans.
Total scan work is `O(source_size)` + `O(group_count)`, never `O(C × source_size)`.
Staging is dropped before creation (a crashed prior backfill on a reused pooled
connection may have left one) and after the writes, so it never leaks.

## Overwrite-by-group-key, not additive-by-PK

Issue #63 sketched an *additive* merge (`col = target.col + excluded.col`)
walking the source PK. We instead **overwrite** (`col = excluded.col`) and chunk
by group key:

- **Non-additive fields.** `MIN`/`MAX` and composed/`RecomputeOnly` expressions
  can't merge additively across chunks. Overwriting a whole group computes each
  field with its own aggregate, exactly as the ring's bulk-recompute path does.
- **Idempotency / crash recovery.** Overwrite is order-independent and
  idempotent, so re-running after a crash recomputes rather than double-counts —
  the same concurrency-safety profile as the ring's image-less recompute path.
- **Bounded work per statement.** Group-key chunking bounds the hash-aggregate
  working set, and the server-side `INSERT … SELECT` carries only range bounds as
  bind parameters, so there is no bind-parameter ceiling (unlike #58's
  VALUES-list writes).

## NULL group keys are built, in one unchunked pass

Postgres `GROUP BY` folds every `NULL` in a column into one ordinary group
(three-valued-logic's usual exception: `GROUP BY` treats `NULL = NULL` as true
for grouping purposes even though `NULL = NULL` is `NULL` everywhere else), so
a NULL-keyed group is real and must be built like any other. It used to be
true that Postgres forbids a NULL in a `PRIMARY KEY`, and the target's
`GROUP BY` columns were keyed that way — issue #128 replaced that with a
`UNIQUE NULLS NOT DISTINCT` constraint instead (`ddl.rs`'s
`create_aggregate_target_table`), specifically so a NULL-keyed group's target
row is representable, and the direct backfill build was fixed to match:
the single-pass staging aggregation includes NULL-keyed groups (no `WHERE
<keys> IS NOT NULL` filter), but they're excluded from the range-chunked
write loop above and written afterward in one final unchunked
`INSERT … WHERE not (<keys> is not null)` instead. That's for a narrower
reason than "can't exist": a `NULL` component makes Postgres's row-value
comparison operators (`<`, `<=`, `>`) return `NULL` rather than
`true`/`false` (three-valued logic again, the *other* direction from
`GROUP BY`'s), which would silently exclude that row from every chunk's range
`WHERE` clause — so NULL-keyed groups are written through the target's
`NULLS NOT DISTINCT` conflict arbiter instead, which needs no row-value
comparison at all. NULL keys are expected to be a small minority of groups,
so skipping the chunking optimization for them costs little.

Issue #110 closed the sibling gap this decision's initial version didn't
cover: a NULL-keyed group being built here is a from-scratch backfill of one
target in isolation, not the live-CDC *downstream propagation* of a NULL-keyed
group's creation/update/extinction into a *chained* definition reading that
target as its own source — see that issue for the shared key-encoding fix
(`staging::apply_aggregate::derive_group_key`,
`staging::apply::read_live_rows_batch`) this backfill path didn't need,
since it writes directly to the target rather than through the ring.

## Wiring

`trellis::defs::install_definition` is the real entry point: it creates the
target table, tries `backfill_definition` (direct path), persists via
`create_definition_without_backfill` on success, and falls back to
`create_definition` (ring enumeration) on `BackfillError::Unsupported`. It is
shared by real callers and the generative harness's `ManualBackend`
(`generative/src/backend/manual.rs`).

Relationship-enriched 1-1 definitions were at first all `Unsupported` — too
broad, since that caught the very shape that motivated this issue: a
`KeySpace::OneToOne` transform whose fields `SUM`/`COUNT`/`AVG`/`MIN`/`MAX` over a
to-many relationship (e.g. `authors` aggregating over related `posts`/`comments`).
`backfill_relationship_one_to_one` now direct-builds that shape: one
`GROUP BY`-aggregated temp staging table per referenced to-many relationship,
`LEFT JOIN`ed back to the source PK and chunked by PK range (reusing the plain
1-1 walk), matching the oracle's no-match semantics (`COUNT` → 0, others → NULL
on an empty child set). Still `Unsupported`, falling back to the ring: a bare
to-one lookup, an aggregate over a to-one relationship, or a relationship
reference nested in a larger expression.

## Consequences

- The M0 benchmark's aggregate phase drops from ~55s to ~0.75s (100k groups) /
  ~0.04s (100 groups); cost now tracks group count, so the cardinalities diverge.
  Regression ceilings tightened to 10s / 5s.
- The ring is off the critical path for a from-scratch build, only handling live
  deltas after it.
- `OneToOne` to-many aggregates are also off the ring: the `relationship-aggregate`
  benchmark (100k authors, 1M posts, 4.5M comments, `SUM`/`COUNT` over both)
  measures ~0.6-0.7s on the direct path. The "~90x" vs ~1 minute is provenance
  from the original poc report, not a same-benchmark before/after (the suite
  carries no unpatched ring-path variant to re-measure against).

## Backgrounding and resumability

A synchronous, complete-on-return build doesn't scale to the sizes the public API
([ADR-0008](0008-public-api-design.md)) must support: a 1B-row build takes real
wall-clock time, and an in-call loop loses everything if the process is
interrupted. So chunk execution is a durable, claimable work queue picked up by
running drain threads — the same way they already claim sealed ring segments.
Every algorithmic decision above is unchanged; only *who runs the chunks and
when* changes.

**Chunks are a durable work queue, not an in-call loop.** Once the target table
exists and the coverage fence is captured (both fast, metadata-only — see #79
bug B's fence-before-build requirement), `install_definition` persists the chunk
boundaries as pending work items and returns immediately. The definition is
recorded and visible (`definitions()` lists it, status `waiting_to_backfill`)
well before a single target row is built.

**Drain threads execute the queue.** `staging_worker` only keeps up with the
replication slot (intake + ring maintenance); `application_threads` — the drain
workers — own finishing transform work, backfill included. A backfill chunk
becomes a second kind of claimable unit alongside a sealed segment, reusing the
exact claim/heartbeat/reclaim-stale machinery in `trellis/src/client.rs`'s
app-worker loop (`register_drainer`, `next_claimable_segments`,
`HeartbeatDaemon`, `staging::reclaim_stale`) — including drain threads in other
processes across the fleet. This is also a throughput win: a huge table's chunks
work in parallel across every running drain thread, not just whichever
connection called `define()`.

**Crash safety** falls out of reusing that machinery: a claimed-but-uncommitted
chunk is protected by the same heartbeat + `reclaim_stale` TTL, so a drain thread
dying mid-chunk doesn't strand the range — another reclaims and redoes it once
the claim goes stale. Redoing is safe because each write is `overwrite`.

**CDC deltas during the build are parked, not released per-chunk.** While a
definition sits in `waiting_to_backfill`/`backfilling`, deltas for its source
tables park in the ring exactly as `pending_backfill` parks them for the
ring-fallback path — deliberately not released per finished chunk (that would
re-derive the fence/ordering guarantee per chunk, real complexity with no
immediate need). Once every chunk is committed, the definition flips to `live` in
one step and parked deltas discharge in one shot, via the same
`run_pending_backfills`-shaped event, triggered by "every chunk claimed and done"
rather than "ring enumeration finished." The tradeoff: a very long build means a
long queue of parked deltas to fold on discharge — acceptable for now.

**Open problem — the aggregate staging table.** The single-pass `GROUP BY`
aggregates into a **connection-scoped `TEMP TABLE`**, cleaned up on connection
teardown — which doesn't survive being read by many drain threads' separate
connections over a long backfill. It needs to become a durable, definition-scoped
staging table with an explicit lifecycle (created once, dropped once every
referencing write chunk commits or the definition is dropped). Leaning toward
making the aggregation pass the definition's first work item — singly claimed,
must complete before write chunks are claimable — rather than solving
multi-writer access to an in-progress aggregation. Not settled.

Undecided:

- How the durable staging table is cleaned up if a definition is
  dropped/redefined mid-backfill (orphaned staging table).
- Whether a stalled backfill (every chunk claimed, none completing — e.g. a
  deterministically-erroring chunk) needs its own fuse, distinct from the
  per-column quarantine fuse in [ADR-0003](0003-quarantine-storage-and-api.md),
  since the failure happens before any target row exists to attribute a
  quarantine entry to.
