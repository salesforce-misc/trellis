# Data Flow

How a source-data change travels through Trellis to the calculated tables it
feeds — the **physical** flow and timing that [transforms](transforms.md)
deliberately omits. Why it's asynchronous:
[ADR-0002](decisions/0002-async-data-flow.md).

Trellis watches source tables over Postgres **logical replication**, pulls
committed changes in batches, collapses each batch into the minimal set of
writes against the affected calculated rows, and re-feeds those writes to any
downstream transforms — all outside the application's write path.

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
[Reading derived data](#reading-derived-data)), replication-slot management, and
the ~1-CPU WAL-decoding ceiling below.

## Ingestion via logical replication

Trellis subscribes to the source tables through a Postgres **logical replication
slot**, which delivers a committed, LSN-ordered stream of row-level changes
(insert / update / delete) for the tables feeding any transform. How a decoded
change becomes a durable staged row, and why the slot is acknowledged only
*after* that stage commits, is
[stage 01](staging-and-claiming/01-intake-and-lsn-confirmation.md).

* **Calculated columns live on a neighbor table**, never on the source row.
  Writing them back onto a replicated source row would feed our own writes into
  ingestion.
* At least one Trellis client should stay connected while writes may happen, so
  the slot doesn't accumulate unbounded WAL and block source writes. Changes
  drain into a **staging area** — an append-only ring of segments, nothing on
  the hot path ever updated ([stage 02](staging-and-claiming/02-the-staging-ring.md)).
* WAL decoding is capped at ~1 CPU by Postgres, the primary throughput ceiling
  for the async path.
* The slot's confirmed position is a durable **LSN watermark** — the point up to
  which all changes have been ingested. Downstream progress is tracked in the
  same LSN space (see [Reading derived data](#reading-derived-data)).

The stream carries changes only. The rows a table already holds when a
transform starts reading it are captured separately, by the path below.

## Capturing a table's existing rows

A new transform's target has to reflect every row its source already holds,
not only the changes that arrive after it's defined. Replication doesn't carry
those rows, so Trellis reads them from the table: the **capture**. The capture
and the stream have to meet exactly. Every commit to the source is either seen
by the capture or delivered by the stream afterward, and a commit that both see
is counted once.

There is one capture path, and every definition's initial build goes through
it ([ADR-0016](decisions/0016-single-background-capture-path.md)). Resumes,
explicit `request_backfill` calls and go-live catch-ups use it too.

> **Status.** This section describes the design ADR-0016 settles. Parts marked
> **Planned** aren't implemented yet. Each marker names the issue that
> implements it and says what happens today instead. ADR-0016's inventory lists
> every path that still differs.

### Registration

Registering a transform (`Trellis::apply` with a `TRANSFORM` statement)
validates the definition, creates the target table, writes the catalog row as
`waiting_to_backfill`, and returns. It reads no source rows and makes no
replication change, so its latency doesn't depend on the table's size.

*Planned (#418, #419):* today registration still reads the source.
`create_definition` enumerates it into the ring inside the registration
transaction. A plain 1-1 definition cuts and enqueues its chunks at
registration, returning `backfilling`. An aggregate or relationship-enriched
1-1 definition runs its whole direct build before `apply` returns. Only a
source with an unsettled marker already defers to the path below.

### The four steps

Everything after registration runs in the background, driven by the staging
worker's maintenance loop. Chunked builds, and possibly direct builds (#419's
call), execute on drain threads.

1. **Join.** The source gets a `pending_backfill` marker. Its **fence** is the
   snapshot (`pg_current_snapshot()`) of the transaction that parks it.
   - If the table isn't in the publication yet, the staging worker's reconcile
     pass (`reconcile_publication`) adds it and parks the marker in the same
     transaction. The fence then names exactly the transactions in flight when
     the table joined the stream.
   - If the source needs no publication change, registration parks the marker
     itself, in the catalog row's transaction. That covers a table that is
     already published and a source that is another definition's target, which
     is never published (#315). *Planned (#418):* registration doesn't park
     one yet.

   Only the staging worker changes the publication. A table has at most one
   marker, and a second park merges into it, keeping the later fence
   ([intake](staging-and-claiming/01-intake-and-lsn-confirmation.md#adjacent-invariants-that-are-easy-to-miss)).
2. **Wait.** The discharge (`run_pending_backfills_until`, once per maintenance
   pass) skips a marker until its fence settles: every transaction that was
   open when it was parked has ended (`now.xmin > fence.xmax`). Because `xmin`
   is cluster-wide, an unrelated long transaction can hold this step up. That
   is safe, and the definition's `waiting_to_backfill` status is the signal
   ([observability](observability.md#backfill-status-and-the-xmin-caveat)).
   Once the fence settles, the discharge also waits for intake to stage through
   the WAL position its read snapshot was taken at (#312), so the stream's copy
   of any commit the read also sees is staged no later than the read's output.
3. **Capture and build.** The discharge promotes the table's
   `waiting_to_backfill` definitions to `backfilling`, takes the capture
   snapshot, and dispatches each definition's build:

   | Build | Used for | How it runs |
   |---|---|---|
   | Ring enumeration | any shape; the fallback for a shape the direct build can't render | one cursor inside the discharge transaction appends an image-less `Recompute` per source row, which drain workers fold like any batch |
   | Plain 1-1 chunks | plain (no relationship) 1-1 definitions | the discharge cuts the source's key range into `backfill_chunks`, and drain threads claim and execute them ([ADR-0007](decisions/0007-direct-set-based-backfill.md#backgrounding-and-resumability)) |
   | Direct set-based build | aggregates and relationship-enriched 1-1 definitions | one background job running ADR-0007's `INSERT … SELECT` build |

   *Planned (#418):* the discharge only runs ring enumeration today. Chunks are
   cut at registration. *Planned (#419):* direct builds run inside
   registration. Whether the job runs on the staging worker or a drain thread
   is #419's call.
4. **Go live.** The definition flips to `live` when its build finishes. For
   ring enumeration that's right after the discharge transaction commits. For
   chunks it's when the last chunk commits. For a direct build it's when the
   job commits. Apply doesn't fold a live change into a definition until it's
   `live`, so a reader sees a partial target while the status says
   `backfilling`, and a complete one once it says `live`.

### Why the path is gap-free

- **Nothing falls between the read and the stream.** The capture snapshot is
  taken after the fence settles. A transaction that was open at the join ended
  before that, so the snapshot sees its commit. Any commit the snapshot
  doesn't see belongs to a transaction that began after the join, and the
  stream carries it.
- **A commit both see is counted once.** Commits between the join and the
  capture snapshot are read *and* streamed. For a 1-1 target that's harmless,
  because apply re-evaluates the row from live state. For an aggregate, the
  read's image-less `Recompute` re-derives the whole group, and the recompute
  horizon keeps the streamed delta from counting the commit a second time
  ([stage 05](staging-and-claiming/05-apply-and-exactly-once-deltas.md#aggregate-groups-the-recompute-horizon)).
  #312's wait for intake only makes those re-derivations rarer.
- **Changes during the build aren't lost.** Apply skips a definition that
  isn't `live`, so a change that drains while the build runs doesn't reach it.
  For ring enumeration nothing like that can happen: the maintenance loop that
  runs the discharge is also the only sealer, so no change committed after the
  pass starts drains before the flip to `live`. Chunked and direct builds read
  the source through many snapshots over a longer time, so going live parks a
  fresh catch-up marker (`complete_direct_backfill`). Its discharge, through
  this same path, re-derives the definition from the source's current state.
  A `backfill_coverage` record can let that catch-up skip re-reading a table
  that provably hasn't changed since the build. That saves work but never
  decides correctness.
- **A resumed target drops rows its source no longer backs.** Before the read,
  the discharge deletes every row of a promoted definition's target that no
  current source row backs (#330, `intake::resume_orphans`), since the read
  only reaches keys the source still has.

### A fresh install

A fresh install creates the replication slot and makes sure every published
table has a marker. It reads no source table itself. The first discharge pass
reads each table after the slot exists, so every commit the read misses comes
after the slot's consistent point and is streamed. A lost slot is recovered the
same way (`intake::slot_loss`): the slot is recreated without a read, and each
paused transform's resume parks its own marker.

*Planned (#417):* today `initial_snapshot_handshake` reads every source table
in the slot-creation transaction. Its snapshot predates the slot's consistent
point, so a row committed during slot creation is neither read nor streamed
(#393). Only the join markers `reconcile_publication` left behind repair that,
by re-reading each table on the first maintenance pass.

### What it asks of a deployment

- **No transform is `live` when `apply` returns.** Poll `Trellis::status` until
  it is. `await_converged` follows the change stream and doesn't wait for a
  pending build.
- **A staging worker must be running.** Nothing joins, waits, captures or goes
  live without its maintenance loop. Chunked builds also need drain threads
  ([embedding](embedding.md#the-silent-stall-hazard-issue-144)).
- **Only the staging worker needs publication and replication privileges.** A
  process that only registers transforms needs catalog access and the right to
  create target tables. *Planned (no child issue yet):* `DROP` still
  reconciles the publication from whichever process applies it
  ([ADR-0014](decisions/0014-pause-and-drop-a-transform.md#the-publication-shrinks-by-reconciliation)).

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
