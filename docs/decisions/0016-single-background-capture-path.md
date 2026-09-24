---
status: accepted
date: 2026-09-23
deciders: Michael Ries
consulted:
informed:
---

# One Background Capture Path for Every Definition Backfill

A new transform's target has to reflect the rows its source already holds, not
only the changes that arrive after it's defined. Logical replication carries
changes only, so those existing rows are read from the table directly. We call
that read the **capture**. The capture and the change stream have to meet
exactly: every commit to the source is either seen by the capture or delivered
by the stream afterward, and a commit that both see is counted once.

Trellis grew several capture paths, each with its own idea of where the capture
ends and the stream begins: a read inside the fresh-install handshake, a ring
enumeration inside the registration transaction, a synchronous direct build
inside registration, a chunk enqueue at registration, and the background
discharge of `pending_backfill` markers. Consistency bugs keep turning up where
two of them meet. This ADR keeps one, the marker discharge, and routes every
definition's capture through it.

## Decision

**Registering a transform does no source reads and no replication work.**
Registration validates the definition, creates the target table, writes the
catalog row as `waiting_to_backfill`, and returns. Everything else happens in
the background, on the staging worker's maintenance loop, through the existing
marker-discharge machinery (`intake::publication::run_pending_backfills_until`):

1. **Join.** The source gets a `pending_backfill` marker, fenced at the
   snapshot of the transaction that parks it. The staging worker's reconcile
   pass parks it (`reconcile_publication`), adding the table to the
   publication in the same transaction if it isn't there yet (see
   [Who parks the marker](#who-parks-the-marker)). The fence has to
   cover every transaction that could have written the table before the join
   committed, since the stream doesn't carry those writes. A fence taken inside
   the `ALTER`'s own transaction falls slightly short of that, so the discharge
   **re-fences a marker the first time it sees it**, at a snapshot necessarily
   later than the `ALTER`'s commit (see [The join fence](#the-join-fence)).
   Only the staging worker changes the publication, including the shrink after a
   `DROP` (see [Consequences](#consequences)).
2. **Wait.** The discharge leaves the marker alone until its fence settles:
   every transaction that was open when it was parked has ended. It then waits
   for intake to stage through the WAL position its read snapshot was taken at.
3. **Capture and build.** The discharge picks the capture snapshot and
   dispatches each of the table's `waiting_to_backfill` definitions' build
   **by shape**: the direct set-based build
   ([ADR-0007](0007-direct-set-based-backfill.md)) or chunked work that drain
   threads execute, whichever the shape has a builder for (resume rebuilds
   included). Ring enumeration is the `Unsupported` fallback, not a size
   threshold. A definition moves to `backfilling` only in the transaction
   that commits the work driving it.
4. **Go live.** The definition flips to `live` when its build finishes.

The marker discharge is the only capture path. It handles a table joining the
publication, a resumed transform, an explicit `request_backfill`, and every
go-live catch-up. The [inventory](#inventory-of-capture-paths) below lists every
path it subsumes and its role.

[docs/data-flow.md](../data-flow.md#capturing-a-tables-existing-rows) walks
through the path and why it's gap-free.

### Who parks the marker

Registration parks nothing. The staging worker's reconcile pass
(`reconcile_publication`) parks every registration's marker, in one of two
ways, split by whether the source needs a publication change:

- **The source isn't published yet.** The pass adds the table and parks the
  marker in one transaction (the join marker). A marker parked any earlier
  could discharge before the table joined the stream, and a commit between
  that read and the join would be neither read nor streamed.
- **The source needs no publication change.** It is already published (another
  definition reads it), or it is another definition's target, which is never
  published. The pass parks a marker on the source of every
  `waiting_to_backfill` definition that has none (`park_registration_markers`),
  in the same transaction as its publication changes. A source the pass hasn't
  published is left for the pass that publishes it.

Either way a `waiting_to_backfill` definition always has a marker in its future,
and registration never runs `ALTER PUBLICATION`.

This differs from the split first recorded here, where registration parked the
marker itself when the source needed no publication change. Registration can
tell from the catalog that a source is another definition's target, but not
that it is already published: which publication the staging worker serves is
that worker's own option (`ClientOptions::publication`), not something the
catalog or a registering process knows, and guessing wrong in either
direction is a bug (a marker on a table not yet streamed, or no marker ever
for an already-published table). The staging worker knows exactly which
tables it has just published, so it parks both cases and there is one owner.
The cost is at most one reconcile pass of latency
(`ClientOptions::reconcile_interval`), and the fence is taken later, which
only makes it safer: the definition isn't `live` in the meantime, so nothing
it could miss is folded anywhere, and the capture read comes after the later
fence.

### What each build reads

The capture snapshot is taken after the fence has settled, so every commit it
doesn't see comes from a transaction that began after the join, and the stream
carries it. A source that is another definition's target is the exception: it
isn't streamed, and the target-mutation seam carries its writes only to a `live`
reader. Apply skips a definition that isn't `live`, so every build also needs
something to cover the changes that drain while it runs. What that is depends on
how many snapshots the build reads through:

| Build | Dispatched as | Reads the source | Covers changes during the build by |
|---|---|---|---|
| **Ring enumeration** (any shape; the universal fallback) | one cursor inside the discharge transaction | once, at the capture snapshot | the discharge running on the maintenance loop, the only sealer: nothing staged after the pass starts drains before the flip to `live`. A source that is another definition's target also gets a go-live catch-up marker (`intake::publication::go_live`): its writes arrive through the seam from drain workers, which don't wait for a seal |
| **Plain 1-1 chunks** | chunk boundaries enumerated and enqueued as `backfill_chunks`, executed by drain threads | once per chunk, each under its own snapshot | the catch-up marker parked when the last chunk goes live (`complete_direct_backfill`), discharged by this same path |
| **Direct set-based build** (aggregates, relationship-enriched 1-1) | one background job | once per statement | the same go-live catch-up marker, parked by every direct build |

The go-live catch-up re-reads the source, so a change that drained while the
definition wasn't `live` reaches the target. For an aggregate its re-derivation
of each group also corrects a change the build read whose streamed delta was
folded again after the flip.

A shape the direct build can't render (`BackfillError::Unsupported`) falls back
to ring enumeration, inside the discharge.

### The join fence

`reconcile_publication` parks the join marker inside the `ALTER PUBLICATION`'s
own transaction, so its fence is a snapshot taken before the join commits. A
writer whose transaction id is assigned after that snapshot, and which writes
the table before the `ALTER` commits, is neither waited for by the fence nor
streamed (its write precedes the join); if it commits after the capture snapshot
its row is lost on both sides. The window is short — the park is the `ALTER`
transaction's last statement — but it has been reproduced on Postgres 17.

**The discharge re-fences a marker the first time it sees it.** The marker
exists exactly when the `ALTER` committed, so any snapshot the discharge takes
after reading the committed marker necessarily postdates the commit; it records
that it re-fenced (so later passes reuse the confirmed fence) and waits on the
new fence. This is crash-safe by construction: there is no window between the
`ALTER` and the marker to recover from. Every marker is re-fenced uniformly,
including ones parked where no `ALTER` happened (a table already in the
publication, a resume catch-up); a new park of the same table resets it to
unconfirmed. This is what keeps the join step gap-free.

## Why

- **Postgres has no exact "capture as of registration" without a wait.**
  `ALTER PUBLICATION … ADD TABLE` covers only writes made after it commits. A
  transaction that wrote before the `ALTER` and commits after the reader's
  snapshot is neither streamed nor read. Closing that gap takes one of two
  things. A write-blocking `LOCK TABLE … IN SHARE MODE` stalls the user's own
  writes behind any slow writer. Waiting out every transaction open at the
  `ALTER` is cluster-wide and unbounded. Creating
  a slot has the same unbounded wait. None of those belong on the registration
  path. Adding a table to a publication also requires owning it, a privilege a
  registering process (a web process, say) shouldn't need.
- **The wait already belongs in `waiting_to_backfill`.** That status is the
  documented, observable signal for exactly this wait
  ([observability](../observability.md#backfill-status-and-the-xmin-caveat)).
- **Consistency bugs keep appearing where capture paths meet.** #393: the
  fresh-install handshake read at a snapshot taken *before* the slot's
  consistent point, so a row committed during slot creation was lost on both
  sides. Only the leftover join markers repaired it, and that repair was
  incidental (#417 removed the handshake's read). #79, #312, #330 and #387
  were all bugs where two capture paths meet. One path has no seams to get
  wrong.
- **Registration latency.** Aggregate and relationship-enriched 1-1
  registrations build synchronously in-call, so registering one against a large
  table is slow. Under this decision registration's latency doesn't depend on
  table size.

## Rejected alternatives

- **Publish and read at registration**, with a lock or a fence wait to make the
  cut exact. That means either blocking the user's writes or an unbounded
  registration latency, and every registering process would need owner
  privileges on the source.
- **Export the slot's real snapshot** (`CREATE_REPLICATION_SLOT … (SNAPSHOT
  'export')`). It would fix the fresh-install gap, but only by keeping a second
  capture path alive just for fresh installs. It also needs
  replication-protocol code that neither `pgwire-replication` 0.4.1 nor
  `tokio-postgres` provides.

## Consequences

- **No definition is `live` when registration returns.** Callers and tests wait
  for the status to reach `live` (`Trellis::status`), then take a watermark
  token and `await_converged` on it. `await_converged` checks the ring and
  intake's progress. It never reads a definition's status or a pending marker,
  so on its own it doesn't wait for a build that hasn't started. After `live`
  it's still needed, because `live` doesn't yet mean the target is complete. A
  ring enumeration flips to `live` once its rows are staged, before they drain.
  Their `Recompute` rows carry no `origin_lsn`, which the predicate treats as
  older than any token, so the wait covers them. A chunked or direct build flips
  to `live` with its go-live catch-up marker still waiting for a later
  maintenance pass, and neither signal waits for that.
- **A running staging worker is required for anything to go live.** That's
  already true: live apply needs intake, and a deferred definition needs the
  discharge ([embedding](../embedding.md#the-silent-stall-hazard-issue-144)).
  Chunked builds also still need drain threads.
- **The discharge's failure handling reaches every registration**, because it's
  the single path. Two rules:
  - **A failing marker never starves the ones behind it.** An error on one
    marker logs and moves on rather than ending the pass; a *deferral* (intake
    hasn't caught up) still ends it, since every later marker would defer too.
    Each marker carries retry state — attempt count, last error, a next-attempt
    time with capped exponential backoff — and its last error shows through
    `Trellis::status`, not just the log. No automatic quarantine; a new park of
    the same table resets the state.
  - **Nothing commits `backfilling` without a driver that commits with it.** The
    ring fallback takes no intermediate status: it flips `waiting_to_backfill →
    live` as the last statement of the discharge transaction, together with the
    read, the marker delete and the go-live catch-up parks. Chunked builds commit
    `backfilling` with their `backfill_chunks` rows; direct builds, with a
    durable, reclaimable job row. A crash anywhere leaves the definitions
    `waiting_to_backfill` with the marker intact, so the next pass retries — no
    definition is ever visibly `backfilling` without something driving it forward.
- **Only the staging worker needs publication and replication privileges.**
  Registering processes need catalog access and the right to create target
  tables, and nothing on the publication. A `DROP` is no exception: it only
  removes catalog rows, and the staging worker's reconcile pass shrinks the
  publication from the catalog, its sole source of truth for what to publish.
  This removes `reconcile_publication_after_drop`, stops treating the startup
  `source_tables` copy as a permanent floor, and supersedes
  [ADR-0014](0014-pause-and-drop-a-transform.md)'s "applied at drop time".
- **A ring read is one transaction (known limitation).** The ring
  enumeration stages one `Recompute` row per source key inside the discharge
  transaction. That's the `Unsupported` fallback's whole build, and it's also
  every catch-up's read on a table something reads: the go-live catch-up of
  every chunked or direct build, a resume's rebuild of a shape still built by
  ring, a `request_backfill`. On a very large table that's one long
  transaction holding back `xmin`, and a ring write the size of the table.
  Dispatch by shape keeps it off the initial build of every shape with a
  builder, but not off the catch-ups. Bounding it (paging the enumeration
  across transactions, or a catch-up that reads only what changed) is a
  follow-up under #415.
- **`backfill_coverage` becomes an optimization at most.** It lets a catch-up
  skip re-reading a table that provably hasn't changed since a build read it. No
  path depends on it for correctness.

## Inventory of capture paths

Every place that reads a source's existing rows, or decides when that read
happens, and its role in this design.

| Path | Where in code | Role in the design |
|---|---|---|
| Fresh-install handshake read | was `intake::publication::initial_snapshot_handshake`, called by `client::setup_staging` when the slot is new | **Retired (#417).** `create_slot_and_park_markers` creates the slot, seeds `replication_progress`, and parks a marker on every configured source table in the same transaction, after slot creation returns. It reads nothing, the same shape slot-loss recovery (`intake::slot_loss`) already has. The discharge skips a table no definition reads (`defs::catalog::table_has_reader`) |
| Ring enumeration inside registration | was `defs::catalog::create_definition_inner`'s `enumerate_and_append`, reached through `create_definition` and `install_definition`'s `Unsupported` fallback | **Done (#418).** The discharge's ring fallback does it (`intake::publication::run_pending_backfills`, dispatch by shape). `create_definition` survives only as a test fixture that stands in for that discharge |
| Plain 1-1 chunk enqueue at registration | was `install_definition` → `install_plain_one_to_one` → `chunk_queue::enqueue_one_to_one` | **Done (#418).** The discharge plans the chunks and enqueues them in its own transaction (`chunk_queue::dispatch_one_to_one`); drain threads still execute them |
| Synchronous direct build inside registration | `install_definition` → `backfill::backfill_definition` for aggregates and relationship-enriched 1-1 | The discharge dispatches it as a background job |
| Registration's defer branch | `install_definition`'s `defer_if_fence_unsettled`; was also `create_definition_inner`'s `backfill_marker_unsettled` check | Becomes unconditional: registration always defers to the discharge, and the branch goes away. **Done for every shape but #419's (#418):** only the in-call aggregate and relationship-enriched 1-1 build still asks it |
| Publication-join discharge | `reconcile_publication` parks; `run_pending_backfills_until` discharges | The one path |
| Transform resume (after `PAUSE`, quarantine or slot loss) | `staging::quarantine::resume_transform` parks a marker; the discharge reads | The one path. Dispatch by shape reroutes the rebuild onto the chunked or direct builder like any other capture, ring only for `Unsupported`. **Done for plain 1-1 (#418):** its rebuild is chunked, and a chunk planned before the resume is told apart by the definition's `fuse_rearmed_at` it recorded (`backfill_chunks.fuse_rearmed_at`). Aggregates and relationship-enriched 1-1 definitions are rebuilt by ring until #419 |
| Explicit re-backfill | `Trellis::request_backfill` parks a marker | The one path |
| Go-live catch-ups | `defs::catalog::complete_direct_backfill`, `intake::publication::go_live`, `park_target_catchup_if_read`, discarded-chunk parks in `chunk_queue` | The one path. A chunked or direct build's go-live catch-up stays, because starting a build after the fence doesn't cover the changes that drain while it runs |
| Column resume | `staging::quarantine::resume_column` → `recompute_column`, then a catch-up marker | Separate: a redefinition-side capture that reads one column's values in-call |
| `ALTER TRANSFORM` added columns | `defs::alter_transform` → `backfill::backfill_altered_columns`, then a catch-up marker ([ADR-0015](0015-transform-redefinition.md)) | Separate: a redefinition-side capture that reads the added columns' values in-call |
| Publication change on `DROP` | `Trellis::reconcile_publication_after_drop`, run by whichever process applied the `DROP` | Moves to the staging worker: a `DROP` only removes catalog rows, and the worker's reconcile pass shrinks the publication from the catalog. `reconcile_publication_after_drop` is removed, and the startup `source_tables` copy stops being a permanent floor. Supersedes [ADR-0014](0014-pause-and-drop-a-transform.md)'s "applied at drop time" |

## Open questions

- **Where a direct build runs.** It can run on the staging worker or on a drain
  thread. One background job per definition is enough; splitting these shapes
  into chunks stays a separate optimization (ADR-0007, "Backgrounding and
  resumability").
- **Which consistency bookkeeping stays.** Once every build starts after the
  fence has settled and intake has passed the snapshot, some of the coverage
  fence and `backfill_coverage` may be redundant. The go-live catch-up of a
  chunked or direct build is not: it's what recovers the changes that drained
  while the definition wasn't `live`.
- **What `live` promises.** Today `live` means the build has finished, not that
  the target is complete. Should the flip wait for the go-live catch-up to
  discharge, or should a caller get some other "target complete" signal?
