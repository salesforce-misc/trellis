---
status: accepted
date: 2026-09-09
deciders: Michael Ries
consulted:
informed:
---

# Direct, Set-Based Backfill Bypasses the Staging Ring

When a definition is created (or explicitly backfilled), Trellis must build its
target table from the entire, already-populated source. Routing that through the
same machinery as live CDC — enumerate every source row, append it to the
staging ring, then claim/fold/compute/apply it back out — is correct but
pathologically slow for a from-scratch build: issue #63 measured a 1M-row /
100k-group aggregate at ~55s on the dev box (~1m50s on the poc cluster),
dominated by ring bookkeeping over data that is entirely present up front and
has no concurrent deltas to reconcile.

## Decision

The initial build of a target is computed **directly, set-based, source→target**,
bypassing the ring entirely. The ring is left to do only what it is uniquely good
at: reconciling live CDC deltas after the build fence.

Implemented in [`trellis::defs::backfill`]:

- **1-1 (calc) transforms** walk the source primary key in half-open `(lo, hi]`
  ranges, each range built with `INSERT INTO target SELECT <exprs> FROM source
  WHERE <pk> > lo AND <pk> <= hi ON CONFLICT (<pk>) DO UPDATE`. Range bounds are
  discovered by `max()`-over-`LIMIT`, so every source row falls in exactly one
  range regardless of gaps in the key values.
- **Aggregates** aggregate the whole source in a **single** full-table scan
  into a connection-scoped temp *staging* table (`CREATE TEMP TABLE … AS SELECT
  <group_cols>, <aggs> FROM source WHERE <keys not null> GROUP BY <group_cols>`),
  then chunk the **writes** from that staging table into the target by group-key
  range — each range an `INSERT INTO target SELECT … FROM staging WHERE
  (<group_cols>) > lo AND (<group_cols>) <= hi ON CONFLICT DO UPDATE`. Because a
  group is a single point in group-key space, it lands wholly in exactly one
  chunk.

Each chunk write is one bounded transaction. Chunks are not run in an in-call
loop: they are enumerated as a durable, claimable work queue and executed by
drain (application) threads, so `install_definition` returns after registering
the definition and enumerating its chunk work items, not after the whole build
completes (see [Backgrounding and resumability](#backgrounding-and-resumability)).

## Single-pass aggregation, then chunked writes — not chunked aggregation

The aggregate build must **not** chunk by group-key range directly over the
*source*. An earlier revision did — each chunk ran `INSERT … SELECT … FROM
source WHERE (<group_cols>) > lo AND (<group_cols>) <= hi GROUP BY …`. The source
has no index on the GROUP BY columns (only its PK; an index on the GROUP BY
columns was tried in an earlier milestone and abandoned as ineffective, see ADR
0005), so every chunk did a full **sequential scan of the entire source**
filtered to one key range. With `C` chunks that is `O(C × source_size)` total
scan work — the exact "re-scan the whole table per chunk" pathology milestones 1
and 2 fixed elsewhere in #63, reintroduced by the new bulk-build mechanism. It
was confirmed empirically: 100k groups → 10 chunks → ~1.25s (~125ms/chunk, each
roughly one full-table scan) against a ~65ms single-pass `GROUP BY` floor, and
projected to ~12.5s at 1M groups (100 chunks) — past the 10s ceiling and ~200x
the compute floor.

The accepted design scans the source **exactly once** (the `CREATE TEMP TABLE …
AS SELECT … GROUP BY`), and every later read — boundary discovery and every
chunk write — hits the staging table, which is *group-count*-sized, not
*source*-sized. A primary key on staging's group columns (valid: the group tuple
is unique in an aggregated result) makes each chunk's range-write an index range
scan rather than a staging seq scan, so even at pathological cardinality (nearly
one group per source row) the writes never degrade into repeated full scans.
Total scan work is `O(source_size)` for the one aggregation pass plus
`O(group_count)` for the writes — never `O(C × source_size)`. Empirically this
took the 100k-group phase from ~1.25s to ~0.75s. The staging table is dropped
before creation (in case a crashed prior backfill on a reused pooled connection
left one behind) and after the writes complete, so it never leaks back into the
pool.

## Overwrite-by-group-key, not additive-by-PK

Issue #63's sketch suggested an *additive* merge (`col = target.col +
excluded.col`) walking the source PK for aggregates. We instead **overwrite**
(`col = excluded.col`) and chunk by **group key**. The reasons:

- **Non-additive fields.** `MIN`/`MAX` and composed/`RecomputeOnly` expressions
  cannot be merged additively across chunks. Overwriting a whole group in one
  pass computes each field with its own aggregate (`sum`, `count`, `min`, `max`,
  and AVG's `sum/count` partials), exactly as the ring's bulk-recompute path
  does.
- **Idempotency / crash recovery.** Overwrite is order-independent and
  idempotent: re-running the backfill (the recovery story after a crash partway
  through) recomputes each group to the same value rather than doubling an
  additive sum. This gives the build the same concurrency-safety profile as the
  ring's image-less recompute path.
- **Bounded work per statement.** Group-key chunking bounds the hash-aggregate
  working set per transaction, and — unlike #58's VALUES-list writes — the
  server-side `INSERT … SELECT` carries only the range bounds as bind
  parameters, so there is no bind-parameter ceiling to respect.

## NULL group keys are not built

An aggregate target's `GROUP BY` columns are its primary key, so Postgres forbids
a NULL there: a group with a NULL key has no representable target row, and the
ring can't store one either. The direct build excludes such groups when it builds
the staging table (`WHERE <keys> IS NOT NULL`), so they never reach the target,
rather than attempting to insert them.

## Wiring

`trellis::defs::install_definition` is the entry point a real caller reaches
for: it creates the target table, tries `backfill_definition` (the direct path),
persists via `create_definition_without_backfill` on success, and falls back to
`create_definition` (ring enumeration) on `BackfillError::Unsupported`.
`create_definition` (bare ring enumeration) still exists as that fallback but is
no longer a primary entry point. `install_definition` is the shared
implementation both real callers and the generative harness's `ManualBackend`
(`generative/src/backend/manual.rs`) use.

1-1 relationship-enriched definitions were entirely unsupported by the direct
path at first (any `uses_relationships(def)` definition returned
`BackfillError::Unsupported` and fell back to the ring unconditionally). That
was too broad: it caught the shape that actually motivated this issue — a
`KeySpace::OneToOne` transform whose fields `SUM`/`COUNT`/`AVG`/`MIN`/`MAX` over a
to-many relationship (e.g. an `authors` table aggregating over related `posts`/
`comments`) — leaving it on the slow ring path regardless of how fast the plain
aggregate/1-1 cases became. `backfill_relationship_one_to_one` now handles that
specific shape directly: one `GROUP BY`-aggregated temp staging table per
referenced to-many relationship, `LEFT JOIN`ed back to the source on its primary
key and chunked by source PK range (reusing the same range-walk as the plain 1-1
path), matching the ring/oracle's no-match semantics (`COUNT` → 0, `SUM`/`MIN`/
`MAX`/`AVG` → NULL on an empty child set). The `Unsupported` boundary is now
narrower and shape-specific: a bare to-one lookup (`category.name`, no aggregate
wrapper), an aggregate over a to-one relationship, or a relationship reference
nested inside a larger expression still falls back to the ring — only the
to-many-aggregate shape is direct-built.

## Consequences

- The M0 benchmark's aggregate phase drops from ~55s to ~0.75s (100k groups) and
  ~0.04s (100 groups) on the dev box; the two cardinalities now diverge sharply,
  since the direct build's cost tracks group count. Regression ceilings tightened
  to 10s / 5s accordingly.
- The ring is no longer on the critical path for a from-scratch build, only for
  live deltas after it.
- Relationship-enriched `OneToOne` **to-many aggregates** are also off the ring
  now: the committed `relationship-aggregate` benchmark scenario (100k authors,
  1M posts, 4.5M comments, `SUM`/`COUNT` over both) measures ~0.6-0.7s through
  `install_definition` on the direct path. The ~1 minute figure it improves on is
  the ring-path baseline from the original poc report that motivated this work,
  not a number the committed benchmark suite reproduces on this side of the
  comparison (the benchmark doesn't carry an unpatched ring-path variant to
  re-measure against) — so "~90x" is provenance from that report, not a
  same-benchmark before/after. Bare to-one lookups and any other relationship
  shape not listed above remain on the ring until the direct path learns to
  render them.

## Backgrounding and resumability

A synchronous, complete-on-return build doesn't scale to the sizes the public
API design ([ADR-0008](0008-public-api-design.md)) must support: a 1B-row
table's direct build still takes real wall-clock time, and a synchronous in-call
loop means an interrupted process (the caller's, or the one running the loop)
loses everything and starts over. So chunk execution is a durable, claimable
work queue that running drain (application) threads pick up, the same way they
already claim sealed ring segments — this keeps every algorithmic decision above
(range/group-key chunking, overwrite semantics, single-pass aggregation into a
staging table) and changes only *who runs the chunks and when*.

**Chunks are a durable, claimable work queue**, not an in-call loop. Once the
target table exists and the coverage fence is captured (both fast, metadata-only
operations — see issue #79 bug B's fence-before-build requirement),
`install_definition` enumerates the chunk boundaries (PK ranges for 1-1,
group-key ranges for aggregates) and persists them as pending work items rather
than executing them, then returns immediately — the definition is recorded and
visible (`definitions()` lists it, status `waiting_to_backfill`) well before a
single row of the target is built.

**Drain (application) threads execute the queue.** Per-fleet clarification:
`staging_worker` is only about keeping up with the logical replication slot
(intake + ring maintenance); it is `application_threads` — the drain
workers — that own finishing transform work, backfill included. Concretely,
this reuses the exact claim/heartbeat/reclaim-stale machinery
`trellis/src/client.rs`'s app-worker loop already runs for sealed ring segments
(`register_drainer`, `next_claimable_segments`, `HeartbeatDaemon`,
`staging::reclaim_stale`): a backfill chunk becomes a second kind of claimable
unit alongside a sealed segment, claimed by whichever drain thread gets to it
next — including drain threads in entirely different processes across the
fleet. This is also a throughput win, not just a resumability one: a huge
table's chunks can be worked in parallel by every drain thread currently
running, not just whichever single connection happened to call `define()`.

**Crash safety** falls out of reusing that machinery rather than needing new
machinery: a claimed-but-not-yet-committed chunk is protected by the same
heartbeat + `reclaim_stale` TTL every ring-segment claim already relies on, so
a drain thread dying mid-chunk doesn't strand that range — another drain
thread reclaims and redoes it once the claim goes stale. Because each chunk's
write is `overwrite`, not additive (already decided above, for the same
reason), redoing a half-finished or fully-finished chunk is safe either way.

**CDC deltas arriving during the build are parked, not released per-chunk.**
While a definition sits in `waiting_to_backfill`/`backfilling`, any CDC delta
for its source table(s) parks in the ring exactly the way `pending_backfill`
already parks deltas for the ring-fallback path today — deliberately *not*
per-chunk released as each range finishes (that would require re-deriving the
fence/ordering guarantee per chunk instead of once per definition, real added
complexity with no immediate need). Once every chunk work item for a
definition is committed, the definition flips to `live` in one step and parked
deltas discharge in one shot, via the same `run_pending_backfills`-shaped event
the ring-fallback path uses, its trigger being "every chunk claimed and done"
rather than "the ring path's one-shot enumeration finished." The
tradeoff this accepts: a very long build means a correspondingly long queue of
parked deltas to fold through on discharge; acceptable for now, revisit if it
becomes the actual bottleneck.

**Open problem — the aggregate path's staging table.** The single-pass
`GROUP BY` scan aggregates into a **connection-scoped `TEMP TABLE`** today,
cleaned up on connection teardown. That doesn't survive being read by many
different drain threads' own separate connections over however long the
backfill takes. This needs to become a durable, definition-scoped staging
table with its own explicit lifecycle (created once, dropped once every write
chunk referencing it has committed or the definition is dropped) rather than
riding on session teardown for cleanup. Leaning toward making the aggregation
pass itself the definition's first work item — singly claimed, must complete
before any write-chunk items are enumerated/claimable — rather than solving
general multi-writer access to an in-progress aggregation. Not settled.

Undecided:

* How the aggregate path's durable staging table gets cleaned up if a
  definition is dropped/redefined mid-backfill (orphaned staging table).
* Whether a stalled backfill (every chunk claimed, none completing — e.g. a
  chunk that deterministically errors) needs its own fuse, distinct from the
  per-column quarantine fuse in
  [ADR-0003](0003-quarantine-storage-and-api.md), since a failure
  here happens before a target row ever exists to attribute a quarantine
  entry to.
