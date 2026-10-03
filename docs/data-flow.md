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
table. How the triggers work, what they cost the writer and how they're
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

> **Superseded design (2026-09-27).** This section describes the code on
> `main`: the backfill discharge, markers and the go-live re-read.
> [ADR-0002](decisions/0002-async-data-flow.md) replaces them with a build that
> applies from its first chunk. The text is rewritten as that lands (#556).

A new transform's target has to reflect every row its source already holds,
not only the changes that arrive after it's defined. The triggers don't see
those rows, so Trellis reads them from the table: the **capture**. The read
and the triggers have to meet exactly. Every commit to the source is either
seen by the read or captured by the triggers, and a commit that both see is
counted once.

There is one capture path, and every definition's initial build goes through
it ([ADR-0002](decisions/0002-async-data-flow.md#what-the-implementation-removes)), except a
plain aggregate's and a plain 1-1 definition's, which is the Re-derive build
([below](#re-derive-built-definitions), #625). Resumes,
explicit `request_backfill` calls and go-live catch-ups use it too. Two
rebuilds of some columns of a `live` transform don't yet: a column resume and an
`ALTER TRANSFORM` that adds columns read those columns' values in-call, then
park a catch-up marker (#425, #426).

### Registration

Registering a transform (`Trellis::apply` with a `TRANSFORM` statement)
validates the definition, creates the target table, writes the catalog row as
`waiting_to_backfill`, and returns. It reads no source rows and installs no
triggers, so its latency doesn't depend on the table's size.

The table and the catalog row commit in one transaction, so a registration that
fails leaves neither behind and can be retried as is. If any relation (table,
view, materialized view, foreign table) already exists under the target's
qualified name, registration is refused with an error naming it. Trellis never
adopts a table it didn't create, because `DROP` later drops the target table
([ADR-0014](decisions/0014-pause-and-drop-a-transform.md)).

### The four steps

Everything after registration runs in the background, driven by the staging
worker's maintenance loop. Chunked builds and direct-build jobs execute on
drain threads.

1. **Join.** The source gets a `pending_backfill` marker, parked with no
   **fence** yet. The first discharge pass to see the marker takes the fence,
   the transaction id of a statement run after reading the committed marker,
   and records it on the marker for later passes.
   - If nothing captures the table yet, the staging worker's reconcile pass
     installs its triggers and parks the marker in the same transaction,
     under a table lock, so no writer of the table is in flight when it
     commits ([stage 01 — The join fence](staging-and-claiming/01-capture-by-triggers.md#the-join-fence)).
     The discharge's fence postdates that commit (#431).
   - If the source is already captured, or is another definition's target,
     which is never captured (#315), the marker comes from the staging
     worker's reconcile pass once every table the definition reads is
     captured with the columns it needs: a widen parks it with the new
     capture functions, and otherwise the pass parks it
     (`park_ready_registration_markers`). Registration itself parks nothing.

   Only the staging worker changes capture, and the catalog is its only
   source of truth for what to capture. That includes the shrink after a
   `DROP`: the drop only removes catalog rows, and the next reconcile pass
   uninstalls the triggers of a table nothing reads any more (#427). A table
   has at most one marker, and a second park merges into it and clears its
   fence, so the next pass fences it afresh.
2. **Wait.** The discharge (`run_pending_backfills_until`, once per maintenance
   pass) skips a marker until its fence settles: every transaction that was
   open when it was fenced has ended (`now.xmin > fence`, where the fence is
   the transaction id of the statement that took it). Because `xmin`
   is cluster-wide, an unrelated long transaction can hold this step up. That
   is safe, and the definition's `waiting_to_backfill` status is the signal
   ([observability](observability.md#backfill-status-and-the-xmin-caveat)).
3. **Capture and build.** The discharge takes the capture snapshot and
   dispatches each of the table's `waiting_to_backfill` definitions' build by
   shape, in one transaction with the marker's delete:

   | Build | Used for | How it runs |
   |---|---|---|
   | Ring enumeration | any shape; the fallback for a shape the direct build can't render | one cursor inside the discharge transaction appends an image-less `Recompute` per source row, which drain workers fold like any batch |
   | Plain 1-1 chunks | plain (no relationship) 1-1 definitions whose source is another definition's target (one on a captured table is the Re-derive build's, [below](#re-derive-built-definitions)) | the discharge cuts the source's key range into `backfill_chunks`, and drain threads claim and execute them ([ADR-0002](decisions/0002-async-data-flow.md#a-build-is-re-derive-over-chunks-and-applies-from-its-first-chunk)) |
   | Direct set-based build | aggregates other than the Re-derive build's ([below](#re-derive-built-definitions)), and relationship-enriched 1-1 definitions | the discharge enqueues one job (a `backfill_chunks` row with no bounds), and a drain thread runs ADR-0007's whole `INSERT … SELECT` build ([ADR-0002](decisions/0002-async-data-flow.md#a-build-is-re-derive-over-chunks-and-applies-from-its-first-chunk)) |

   The discharge checks against the catalog that the direct build can render
   a definition before it dispatches one; a shape it can't
   (`BackfillError::Unsupported`) gets the ring enumeration.

   A definition only reaches `backfilling` together with the work that drives
   it (#404): a chunked build commits `backfilling` with its `backfill_chunks`
   rows, a direct build with its job row. A ring-built definition skips it,
   flipping from `waiting_to_backfill` to `live` (or `catching_up`, below) as
   the discharge transaction's last statement. A drain thread that dies holding a chunk or a
   job loses its claim to the reclaim sweep, and another reruns it. A chunk or
   job that a worker still holds when its definition is paused and resumed is
   superseded, and its writes are fenced so that none lands after the resume
   (#434, [ADR-0002](decisions/0002-async-data-flow.md#what-the-implementation-removes)).
   A direct build that fails goes back to `waiting_to_backfill` behind a
   marker that carries the error and a backoff, the same retry state a failed
   discharge gets (#407), so `Trellis::status` reports it.
4. **Go live.** A ring enumeration flips its definition to `live` inside
   the discharge transaction itself. A chunked or direct build doesn't flip
   it `live` when it finishes (#476): the last chunk's commit, or the job's,
   moves it to `catching_up` and parks its go-live catch-up (below), and the
   discharge of that catch-up flips it `live` in the same transaction as its
   re-read. The staging worker checks for a fresh marker on every
   maintenance tick and discharges it at once, so this adds a tick, not a
   `reconcile_interval`. Apply doesn't fold a live change into a definition
   until its build has finished, so a reader sees a partial target while the
   status says `backfilling`. `live` means the steady state: a watermark
   token awaited after it covers every commit at or before the token. A ring
   enumeration's rows are still undrained when it flips, but they carry no
   `origin_lsn`, so they gate every token
   ([ADR-0002](decisions/0002-async-data-flow.md#what-live-promises)).

### Why the path is gap-free

- **Nothing falls between the read and the triggers.** The capture snapshot is
  taken after the fence settles, and the fence postdates the join's commit. A
  transaction that was open when the join committed had either ended by the
  fence or was open at it and waited out, so the snapshot sees its commit. Any
  write the snapshot doesn't see committed after the join, and every write to
  the table after the join runs its capture trigger.
- **A commit both see is counted once.** Commits between the join and the
  capture snapshot are read *and* captured. For a 1-1 target that's harmless,
  because apply re-evaluates the row from live state. For an aggregate, the
  read's image-less `Recompute` re-derives the key's ledger entry from a
  snapshot, and the entry's basis keeps the captured delta from counting the
  commit a second time
  ([stage 05](staging-and-claiming/05-apply-and-exactly-once-deltas.md#aggregate-groups-the-ledger)).
  A direct build writes the same basis on every entry it writes (#419).
- **Changes during the build aren't lost.** Apply skips a definition that
  isn't `live`, so a change that drains while the build runs doesn't reach it.
  For ring enumeration on a captured source that can't happen: the maintenance
  loop that runs the discharge is also the only sealer, so nothing staged
  after the pass starts drains before the flip to `live`, and everything
  staged before it committed before the capture snapshot. A source that is
  another definition's target is different. Its writes reach readers through
  the target-mutation seam, from drain workers that don't wait for a seal, and
  only to applying ones, so the enumeration moves the definition to
  `catching_up` (applying, not yet `live`) and parks a catch-up marker on it
  (`go_live` in `intake::markers`, #315, #476). Chunked and direct builds
  read the source through many snapshots over a longer time, so finishing one
  parks a fresh catch-up marker on every table the build read
  (`complete_direct_backfill`; a direct build also reads each relationship
  to-side). Its discharge, through this same path, re-derives the definition
  from the tables' current state, and the discharge of its last catch-up
  flips it `live`. Every go-live catch-up re-reads every table its build
  read, even one that looks unchanged: a row inserted and deleted again
  during the build leaves the table's row count and `xmin`s as they were,
  although the build counted it (#468). A commit the build read whose
  captured delta drains after the flip needs no catch-up: it's harmless for a
  1-1 target and kept from double counting by the entry's basis for an
  aggregate (above).
- **A target drops rows its source no longer backs.** The read only reaches
  keys the source still has, so a 1-1 row whose source row is gone, or an
  aggregate group with no source rows left, needs deleting outright. The
  discharge runs an anti-join against the source (`intake::resume_orphans`)
  on two sets of targets:
  - each definition it dispatches, for rows whose source rows went away while
    a resumed definition was frozen (#330);
  - each `catching_up` definition that reads the marker's table, which
    includes every definition this discharge flips `live` (#485). A delete
    that drained while the definition was `backfilling` was skipped, and its
    build may have read the row first.

  The anti-joins are branches of the read's own cursor, so a row is judged
  unbacked on exactly the snapshot the read enumerates (#436). The discharge
  deletes those rows by key as it fetches the cursor.
  A row unbacked on that snapshot is gone, and any later change that backs
  it again rebuilds it; a row backed on it is re-derived by the read's
  `Recompute`. So the sweep is exact for aggregates as well as 1-1. Each
  deleted row goes through the target-mutation seam, so a chained reader
  re-derives from it. A deleted group can also still have deltas staged for
  it (a `catching_up` definition applies CDC). Such a delta applies to its
  key's ledger entry, whose basis says whether the read already counted it,
  so it never subtracts from nothing (#623 D5).

### Re-derive-built definitions

A plain aggregate is built differently (#625,
[ADR-0002](decisions/0002-async-data-flow.md#a-build-is-re-derive-over-chunks-and-applies-from-its-first-chunk)).
That is an aggregate with no relationship, on a captured source (not another
definition's target). Its fields may be kept by
increments (`SUM` or `AVG` of an exact numeric argument, `COUNT`) or
recomputed from the group's entries (`MIN`/`MAX`, `BOOL_AND`/`BOOL_OR`, a
float `SUM`/`AVG`, a composed field like `SUM(a) + COUNT(*)`), and their
arguments may be expressions (`SUM(v + 1)`), which a chunk computes in SQL over
the source exactly as Apply does over a change's row. A plain 1-1 definition
(no relationship in any field, on a captured source) takes the same build,
with the difference set out [after the steps](#a-1-1-definitions-chunks). Its build (`staging::build`) uses no marker, no fence wait, no
direct build and no go-live catch-up:

1. **Start.** Once the reconcile pass finds the definition ready (its source
   captured with the columns it reads, and no change staged before the last
   widen still pending), one transaction moves it `waiting_to_backfill ->
   backfilling` and enqueues its plan job. From that commit it **applies**:
   Apply folds every change to the source into its ledger and groups, as for
   a `live` definition. The discharge never dispatches it.
2. **Plan.** A drain thread walks the source's primary key and enqueues
   chunks of `build_chunk_rows` rows (10,000 by default), committing them in
   batches, so chunks run while the walk goes on.
3. **Chunks.** Each chunk re-derives its key range: it locks the keys'
   ledger entries as a page does, then one statement reads the rows and
   `pg_current_snapshot()`, rewrites the entries (their basis is that
   snapshot) and appends the moves' per-group increments to the target's
   group-delta table. A chunk never writes a group row. Apply and a chunk
   agree through the entry lock and the basis: a change the chunk's snapshot
   saw is skipped when it drains, and one it didn't see applies over the
   entry the chunk wrote.
4. **Merges.** A drain thread claims delta rows, deletes them and upserts
   their sums into the groups in one transaction. Each delta row carries its
   group's merge partition, a hash of the group, so several drain threads
   merge one target at once, one per partition, and never write the same
   group row. A target with recomputed fields gets a delta row for every group an
   entry moved into or out of, even when the sums net to 0, and the merge
   then rewrites those fields: a group whose rows only added entries folds
   their current values into its stored `MIN`/`MAX` or `BOOL_AND`/`BOOL_OR`
   (reading just those entries); any other group, and any group row the
   merge creates, is recomputed from all of its entries, as Apply does.
5. **Live.** The transaction that leaves the plan done, every chunk done
   and the delta table empty moves the definition to `live`, under its row
   lock. There is no `catching_up` in between.

A chunk that fails on its data (an expression that overflows on one row,
say) is narrowed as a 1-1 build's chunk is: it splits until the key fails
alone, the key is quarantined as a drain eviction would quarantine it, and
the chunk runs again without it. Chunks and the sweep leave every
quarantined key out, as the drain does, until it is released.

Drain threads take this work only after their segments, and claim a chunk
only while the ring's undrained backlog is small and the next seal has a
slot, so a build never starves the drain or holds off a seal. Each chunk's
transaction is short, so the build holds no long snapshot: the `xmin` caveat
below doesn't apply to it.

#### A 1-1 definition's chunks

A 1-1 target's rows are absolute: each is the evaluation of one source row.
So a 1-1 chunk writes the target rows itself, and there are no group deltas
and no merges (step 4). It locks its keys' entries on the target's 1-1
ledger, the per-key ordering state Apply keeps there, under the same short
lock timeout. Then one statement reads the rows and `pg_current_snapshot()`,
stamps each entry's basis with that snapshot, upserts the target row of
every key that has a source row (when its values changed), and deletes the
target row of a key whose source row is gone. Every field is computed in SQL
over the source, as the old 1-1 chunks computed it. Apply and a chunk agree
the same way: a change the chunk's snapshot saw is skipped when it drains,
and one it didn't see is newer than the row the chunk wrote. A paused column
is left as it is. Writes reach a definition that reads the target through
the target-mutation seam, as Apply's do.

**A resume rebuilds over what the freeze left.** A paused or quarantined
definition keeps its ledger, its groups and the group deltas still owed to
them. Its resume parks no marker: the next reconcile pass starts a Re-derive
build over the existing entries, with no truncate. Its start also enqueues a
**sweep**, which runs once every chunk is done. It walks the ledger in key
order, in bounded windows, and re-derives each live entry no chunk did,
which is a key deleted while the definition was frozen: the entry becomes a
tombstone and its group sheds it through the deltas (a 1-1 target's row is
deleted).

### Re-reading a table for applying readers

The staging worker can install a table's triggers while definitions already
apply from it: the triggers were dropped by hand, say, and the next reconcile
pass puts them back. Those definitions have missed whatever committed without
the triggers, so the install's marker is a go-live catch-up for each of the
table's applying readers (`park_table_catch_ups`). They report `catching_up`
until the discharge has re-read the table and swept their targets for rows the
source no longer backs, which a re-read alone can't reach
([ADR-0002](decisions/0002-async-data-flow.md#what-the-implementation-removes)).
An explicit `Trellis::request_backfill` parks the same catch-ups for the same
reason, and a re-read table that is a relationship's to-side also has its
settled projections refreshed
([ADR-0002](decisions/0002-async-data-flow.md#what-the-implementation-removes)).

### What it asks of a deployment

- **No transform is `live` when `apply` returns.** Poll `Trellis::status` until
  it is ([embedding — Poll to `live`](embedding.md#poll-to-live-dont-wait)),
  then take `Trellis::watermark_token` and `await_converged` on it.
  `await_converged` checks the ring, never a
  definition's status, so on its own it doesn't wait for a build that hasn't
  started. After `live` it covers everything: `live` waits for a chunked or
  direct build's go-live catch-up (the definition reports `catching_up` until
  then), and a ring enumeration's staged rows gate every token
  ([ADR-0002](decisions/0002-async-data-flow.md#what-live-promises)).
- **A staging worker must be running.** Nothing joins, waits, captures or goes
  live without its maintenance loop. Chunked and direct builds also need drain threads
  ([embedding — Who runs what](embedding.md#who-runs-what),
  [the silent-stall hazard](embedding.md#the-silent-stall-hazard-issue-144)).
- **Only the staging worker needs to own the source tables.** It installs
  their capture triggers (#622), which needs ownership. A process that only
  registers transforms needs catalog access and the right to create target
  tables, and so does one that drops them: a `DROP` only removes catalog rows,
  and the staging worker's reconcile pass uninstalls the capture, up to one
  `reconcile_interval` (5s by default) later (#427). Until then the dropped
  table's changes are still staged, and apply skips them because nothing
  reads the table. One that declares a to-one relationship also needs to
  create a table in the instance schema, where the relationship's projection
  lives (the same right `migrate` already uses there), and one that applies a
  transform reading a parent column the projection doesn't carry yet must own
  the projection, since it adds that column.

## Staging and batching

Staged changes are **collapsed** before touching any target:

* An **aggregate** target's N staged changes to a group reduce to one
  recompute/delta write for that group's row.
* **1-1** and **cross-join** targets reduce to the distinct set of target rows
  (or key pairs) affected.

Collapsing turns a burst of source writes into the minimal set of target writes,
and is the same machinery a ring-enumeration backfill uses: it stages a table's
existing rows as one large batch (see
[Capturing a table's existing rows](#capturing-a-tables-existing-rows)).

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

The storage of quarantine status and the API to discover and clear quarantines
are **not yet decided** — see [open-questions](open-questions.md). Quarantine is
one state of a transform's broader **lifecycle status** (see
[observability — Transform status lifecycle](observability.md#transform-status-lifecycle)).
For how a killer change is isolated, evicted, and its parked work kept honest so
a quarantined key still blocks a waiting reader, see
[stage 06](staging-and-claiming/06-cleanup-and-reclaim.md).

## Reading derived data

Derived tables are eventually consistent. Two ways to reason about staleness:

* **`await(LSN, timeout)`** — an opt-in primitive that blocks until every
  derivation up to a given LSN has landed, for a strong read when one is needed.
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
([stage 05](staging-and-claiming/05-apply-and-exactly-once-deltas.md)).
