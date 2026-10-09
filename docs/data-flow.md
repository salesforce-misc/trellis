# Data Flow

How a source-data change travels through Trellis to the calculated tables it
feeds — the **physical** flow and timing that [transforms](transforms.md)
deliberately omits. Why it's asynchronous:
[ADR-0002](decisions/0002-async-data-flow.md).

Trellis captures each change to a source table with **triggers** that write it
into a staging ring in the writer's own transaction, pulls the staged changes
in batches, collapses each batch into the minimal set of writes against the
affected calculated rows, and re-feeds those writes to any downstream
transforms. Only the capture runs on the application's write path.

This is the **logical** map. The machinery that implements it — durable staging
ring, sealing, claiming and folding, exactly-once deltas, the read-your-writes
predicate — is written up stage by stage in
[staging-and-claiming](staging-and-claiming/README.md); each section below links
to the stage that realizes it.

## Why asynchronous

Three properties shape the rest of this design:

* **Batching** — many source changes that fold into one aggregate collapse into
  the minimal number of target writes, not one trigger per row.
* **No writer contention** — derivation work leaves the application's hot path,
  so concurrent updates to related source rows don't serialize on locks over
  shared target rows.
* **Quarantine, not block** — a failing derivation runs after the source commit,
  so it can be isolated while the rest keep flowing.

The costs we design around: eventual consistency (see
[Reading derived data](#reading-derived-data)), and the capture's cost on every
write to a captured table.

## Capture by triggers

Each source table some transform reads carries statement-level `AFTER`
triggers. They append the statement's row-level changes (insert / update /
delete, with old and new images keyed by primary key) to the staging ring in
the writer's own transaction, so a change is staged exactly when it commits.
The staging worker installs them in the background once a transform reads the
table, under a short `lock_timeout` retry so a writer never queues behind the
install. How the triggers work, what they cost the writer and how they're
installed, widened and removed is
[stage 01](staging-and-claiming/01-capture-by-triggers.md).

* **Calculated columns live on a neighbor table**, never on the source row.
  Writing them back onto a captured source row would feed our own writes into
  capture.
* Changes land in a **staging area** — an append-only ring of segments, nothing
  on the hot path ever updated
  ([stage 02](staging-and-claiming/02-the-staging-ring.md)).
* Each staged row carries the WAL insert position at capture, below its
  commit's position, and a read-your-writes token is a WAL position, so
  downstream progress is tracked in the same LSN space (see
  [Reading derived data](#reading-derived-data)).

The triggers capture changes only. The rows a table already holds when a
transform starts reading it are captured separately, by the path below.

## Capturing a table's existing rows

A new transform's target has to reflect every row its source already holds,
not only the changes that arrive after it's defined. The triggers don't see
those rows, so Trellis reads them from the table, and the read and the
triggers have to meet exactly: every commit to the source is either seen by
the read or captured by the triggers, and a commit that both see is counted
once. Mechanics:
[ADR-0002](decisions/0002-async-data-flow.md#a-build-is-re-derive-over-chunks-and-applies-from-its-first-chunk)
and [stage 05](staging-and-claiming/05-apply-and-exactly-once-deltas.md).

### Registration

Registering a transform (`Trellis::apply` with a `TRANSFORM` statement)
validates the definition, creates the target table and its ledger tables,
writes the catalog row as `waiting_to_backfill`, and returns. It reads no
source rows and installs no triggers, so its latency doesn't depend on the
table's size. The table and the catalog row commit in one transaction, so a
registration that fails leaves neither behind and can be retried as is. If
any relation already exists under the target's qualified name, registration
is refused with an error naming it: Trellis never adopts a table it didn't
create, because `DROP` later drops the target table
([ADR-0014](decisions/0014-pause-and-drop-a-transform.md)). Which sources and
targets are accepted is
[Supported sources and targets](transforms.md#supported-sources-and-targets).

Everything after registration runs in the background, driven by the staging
worker's maintenance loop and executed on drain threads. Only the staging
worker changes capture, and the catalog is its only source of truth for what
to capture: a drop removes catalog rows, and the next reconcile pass
uninstalls the triggers of a table nothing reads any more.

### The Re-derive build

A plain aggregate or plain 1-1 definition (no relationship in any field) is
built by the **Re-derive build** (`staging::build`), whether its source is a
captured table or another definition's target. It needs no marker and no
go-live catch-up, because the definition **applies from its first chunk**. A
captured source needs no fence wait either; a source that is another
definition's target takes one fence per build (see below):

1. **Start.** Once the reconcile pass finds the definition ready (its source
   captured with the columns it reads), one transaction moves it
   `waiting_to_backfill -> backfilling` and enqueues a plan job. From that
   commit Apply folds every change to the source into the definition, as for
   a `live` one.
2. **Plan and chunks.** A drain thread walks the source's primary key and
   enqueues chunks of `build_chunk_rows` rows (10,000 by default). Each chunk
   re-derives its key range: it locks the keys' ledger entries, then one
   statement reads the rows and `pg_current_snapshot()` and rewrites the
   entries with that snapshot as their basis. Apply and a chunk agree through
   the entry lock and the basis: a change the chunk's snapshot saw is skipped
   when it drains, and one it didn't see applies over the entry the chunk
   wrote. Commits between the join and the chunk's snapshot are therefore
   read and captured but counted once.
3. **Writes.** A 1-1 chunk upserts and deletes the target rows itself. An
   aggregate chunk never writes a group row; it appends the moves' per-group
   increments to a delta table, and drain threads merge them into the groups,
   one merge partition per thread.
4. **Live.** The transaction that leaves the plan done, every chunk done and
   the delta table empty moves the definition to `live`.

**A source that is another definition's target.** Such a source is never
captured: its writes reach Apply through the target-mutation seam, whose
writer reads the list of applying definitions inside its own transaction. A
writer that read it before the build started can commit after a chunk's
snapshot, with its rows staged for no one, so the plan job first takes a
fence, a transaction id assigned after the start commit, and enqueues no
chunk until the oldest running transaction in the cluster began after it.
While it waits, the definition's `status().build_wait` reads
`BuildWait::Fence { xid }`, naming the id to look for in `pg_stat_activity`.
Only these builds wait, and only on transactions older than the fence. A
source keyed by nullable `GROUP BY` columns has keys that no chunk's range
reaches (a key with a `NULL` part, which a row comparison excludes), so an
aggregate's plan job stages an image-less `Recompute` for each such key when
its walk ends, for a drain page to re-derive; a 1-1 target leaves them out, as
it can hold no row for one.

A chunk that fails on its data splits until the key fails alone, and the key
is quarantined as a drain eviction would quarantine it. Drain threads take
build work only after their segments, and only while the ring's undrained
backlog is small and the next seal has a slot, so a build never starves the
drain or holds off a seal. Each chunk's transaction is short, so the build
holds no long snapshot.

A paused or quarantined definition keeps its ledger, groups and owed deltas.
Its resume starts a Re-derive build over the existing entries with no
truncate, plus a **sweep** that re-derives each live entry no chunk did,
which is a key deleted while the definition was frozen. An `ALTER TRANSFORM`
that adds columns, and a column resume, run a field build: background chunks
that rewrite just those columns from one snapshot.

### Builds that still use a marker and a go-live catch-up

A definition that reads a relationship doesn't yet take the Re-derive build
(milestone F, #625, moves it). Its build is dispatched by a **marker** on the source table
(`intake::markers`):

1. **Join.** The staging worker's reconcile pass installs the source's
   triggers (or, if it is already captured, parks the marker once every
   table the definition reads is captured with the columns it needs). The
   install's commit is the join fence
   ([stage 01](staging-and-claiming/01-capture-by-triggers.md#the-join-fence)).
2. **Wait.** The discharge skips a marker until every transaction open at
   the join has ended. `xmin` is cluster-wide, so an unrelated long
   transaction can hold this step up; the definition's `waiting_to_backfill`
   status is the signal
   ([observability](observability.md#backfill-status-and-the-xmin-caveat)).
3. **Build.** The discharge takes the capture snapshot and builds by shape:
   a ring enumeration (an image-less `Recompute` per source row that drain
   workers fold like any batch), plain 1-1 chunks, or one direct set-based
   `INSERT … SELECT` job. Apply skips a definition that isn't `live`. An
   aggregate's direct job rebuilds the ledger, then deletes, through the
   target-mutation seam, every group the rebuilt ledger has no live entry
   for, before it writes the groups it has.
4. **Go live.** A ring enumeration flips its definition to `live` in the
   discharge transaction. A chunked or direct build moves to `catching_up`
   (applying, not yet `live`) and parks a go-live catch-up on every table the
   build read; the discharge of the last catch-up re-derives the definition
   from the tables' current state, sweeps its target for rows the source no
   longer backs (`intake::resume_orphans`), and flips it `live`. Every
   catch-up re-reads its table even when it looks unchanged, since a row
   inserted and deleted during the build leaves no trace in the table's
   statistics.

### Re-reading a table for its applying readers

When the staging worker reinstalls a table's triggers while definitions
already apply from it (the triggers were dropped by hand, say), those
definitions have missed whatever committed meanwhile. The install parks a
go-live catch-up for each of them, as does an explicit
`Trellis::request_backfill` on a captured table. They report `catching_up`
until the discharge has re-read the table and swept their targets for rows
the source no longer backs.

### What it asks of a deployment

- **No transform is `live` when `apply` returns.** Poll `Trellis::status`
  until it is ([embedding — Poll to `live`](embedding.md#poll-to-live-dont-wait)),
  then take `Trellis::watermark_token` and `await_converged` on it. `live`
  is the steady state: a token awaited after it covers every commit at or
  before the token ([ADR-0002](decisions/0002-async-data-flow.md#what-live-promises)).
  `await_converged` checks the ring, never a definition's status, so on its
  own it doesn't wait for a build.
- **A staging worker must be running**, and drain threads for any build
  ([embedding — Who runs what](embedding.md#who-runs-what),
  [the silent-stall hazard](embedding.md#the-silent-stall-hazard-issue-144)).
- **Only the staging worker needs to own the source tables**, because it
  installs their capture triggers. A process that only registers or drops
  transforms needs catalog access and the right to create target tables; the
  reconcile pass uninstalls a dropped transform's capture up to one
  `reconcile_interval` (5s by default) later, and apply skips the changes
  staged meanwhile because nothing reads the table. One that declares a
  to-one relationship also needs to create a table in the instance schema,
  where the relationship's projection lives, and one that applies a
  transform reading a parent column the projection doesn't carry yet must own
  the projection, since it adds that column.

## Staging and batching

Staged changes are **collapsed** before touching any target:

* An **aggregate** target's N staged changes to a group reduce to one
  recompute/delta write for that group's row.
* **1-1** and **cross-join** targets reduce to the distinct set of target rows
  (or key pairs) affected.

Collapsing turns a burst of source writes into the minimal set of target
writes.

Physically, a batch boundary is cut by *sealing* the active segment
([stage 03](staging-and-claiming/03-sealing-and-the-fence.md)), and the collapse
is the **claim-time fold** — grouping a sealed batch's raw changes to one record
per key ([stage 04](staging-and-claiming/04-claiming-and-the-fold.md)).

## Evaluation and dependency order

Within a batch, calculated columns are evaluated in **dependency order** over the
dependency graph (see
[transforms — Chaining and cycle detection](transforms.md#chaining-and-cycle-detection)).
Cycles are rejected at definition time, so a valid ordering always exists.

Because formulas use only **immutable** functions, re-evaluating one on unchanged
inputs always yields the same result — the property the
[correctness oracle](#correctness) relies on.

Trellis **evaluates formulas in its own execution layer**, not by issuing SQL to
Postgres per batch (modeled on Postgres's own implementation, see
[0004-transform-definition-grammar](decisions/0004-transform-definition-grammar.md)).
It computes each target from the collapsed batch as the **minimal write** — for
an aggregate, a small atomic delta against the group's existing value rather than
a full recompute; for 1-1 and cross-join, only the target rows the batch touched.
This keeps incremental maintenance cheaper than re-running the definition while
landing on the same result.

How that delta is applied **exactly once** — never twice, never zero times — with
apply and mark-complete in a single transaction, is
[stage 05](staging-and-claiming/05-apply-and-exactly-once-deltas.md).

## Writing to calculated tables and chaining

Evaluated results are written to the calculated tables. Those writes are
themselves changes, so any **chained** transform receives the just-written rows
as input to a subsequent batch, walking the dependency graph across tables: each
layer's output feeds the staging area as the next layer's input, so transitive
derivations need no special-casing. Crucially, a worker propagating downstream
appends into the **active** segment, never the batch it is draining, keeping a
claimed batch immutable
([stage 05 — downstream propagation](staging-and-claiming/05-apply-and-exactly-once-deltas.md)).

## Failure handling and quarantine

When a derivation fails (formula error, constraint violation), the async model
lets us **quarantine the minimal affected slice** rather than failing the
already-committed source write: one bad transform or source row affects only its
dependents, and the rest stays readable.

Quarantine storage and the API to discover and clear quarantines are
[ADR-0003](decisions/0003-quarantine-storage-and-api.md). Quarantine is
one state of a transform's broader **lifecycle status** (see
[observability — Transform status lifecycle](observability.md#transform-status-lifecycle)).
For how a killer change is isolated, evicted, and its parked work kept honest so
a quarantined key still blocks a waiting reader, see
[stage 06](staging-and-claiming/06-cleanup-and-reclaim.md).

## Reading derived data

Derived tables are eventually consistent. Two ways to reason about staleness:

* **`await_converged`** — an opt-in primitive that blocks until every
  derivation up to a watermark token has landed, for a strong read when one is
  needed.
* **Lag telemetry** — always-on metrics exposing how far derived data trails the
  source LSN.

The predicate behind `await` — the one that must never falsely report
"converged" — is [stage 07](staging-and-claiming/07-convergence-and-await.md).

## Correctness

The correctness bar for the [generative suite](../README.md#structure): once
Trellis has caught up to a given LSN, each incrementally-maintained target must
be **exactly equal to a full recompute** of its definition against the source
data at that LSN, for any interleaving of source changes. Incremental maintenance
is only ever an optimization over that result, never a different answer — held to
byte-identical convergence against a from-scratch `GROUP BY` oracle after every op
and every drain interleaving
([stage 05](staging-and-claiming/05-apply-and-exactly-once-deltas.md)). The
ways an operator can still leave a target stale are listed in
[known correctness gaps](known-correctness-gaps.md).
