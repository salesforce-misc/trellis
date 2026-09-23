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
two of them meet (#393, #79, #312, #330, #387). This ADR keeps one of them, the
marker discharge, and routes every definition's capture through it. Epic #415
tracks the work.

## Decision

**Registering a transform does no source reads and no replication work.**
Registration validates the definition, creates the target table, writes the
catalog row as `waiting_to_backfill`, and returns. Everything else happens in
the background, on the staging worker's maintenance loop, through the existing
marker-discharge machinery (`intake::publication::run_pending_backfills_until`):

1. **Join.** The source gets a `pending_backfill` marker, fenced at the
   snapshot of the transaction that parks it. If the table isn't in the
   publication yet, the staging worker's reconcile pass adds it and parks the
   marker in the same transaction (`reconcile_publication`). The fence has to
   cover every transaction that could have written the table before the join
   committed, since the stream doesn't carry those writes. A fence taken inside
   the `ALTER`'s own transaction falls slightly short of that (see
   [below](#left-for-the-children-to-record-here)). Only the staging worker
   changes the publication; `DROP` is an open exception (#427).
2. **Wait.** The discharge leaves the marker alone until its fence settles:
   every transaction that was open when it was parked has ended. It then waits
   for intake to stage through the WAL position its read snapshot was taken at
   (#312's gate).
3. **Capture and build.** The discharge promotes the table's
   `waiting_to_backfill` definitions to `backfilling`, picks the capture
   snapshot, and dispatches each definition's build: ring enumeration, the
   direct set-based build ([ADR-0007](0007-direct-set-based-backfill.md)), or
   chunked work that drain threads execute.
4. **Go live.** The definition flips to `live` when its build finishes. A user
   knows a transform isn't ready until its status moves.

This is today's marker-discharge path, made the **only** capture path. It
already handles a table joining the publication, a resumed transform, an
explicit `request_backfill`, and every go-live catch-up. It replaces the
fresh-install handshake's own read, `create_definition`'s in-transaction
enumeration, the synchronous direct builds `install_definition` runs for
aggregates and relationship-enriched 1-1 definitions, and the registration-time
chunk enqueue for plain 1-1 definitions. The [inventory](#inventory-of-capture-paths)
below lists every path and what becomes of it.

[docs/data-flow.md](../data-flow.md#capturing-a-tables-existing-rows) walks
through the path and why it's gap-free.

### Who parks the marker

The join has two cases, split by whether the source needs a publication change:

- **The source isn't published yet.** Registration parks nothing. The staging
  worker's next reconcile pass adds the table and parks the marker in one
  transaction. A marker parked any earlier could discharge before the table
  joined the stream, and a commit between that read and the join would be
  neither read nor streamed.
- **The source needs no publication change.** It is already published (another
  definition reads it), or it is another definition's target, which is never
  published (#315). Registration parks the marker itself, in the same
  transaction as the catalog row. That's a catalog write, not a source read,
  and its fence is registration's own snapshot.

Either way a `waiting_to_backfill` definition always has a marker in its
future, and registration never runs `ALTER PUBLICATION`.
*Planned (#418):* registration doesn't park a marker yet. It reads the source
itself instead, which is what this ADR retires. #418 records the split here if
the implementation differs.

### What each build reads

The capture snapshot is taken after the fence has settled, so every commit it
doesn't see comes from a transaction that began after the join, and the stream
carries it. A source that is another definition's target is the exception: it
isn't streamed, and the target-mutation seam carries its writes only to a
`live` reader. Apply skips a definition that isn't `live`, so every build also
needs something to cover the changes that drain while it runs. What that is
depends on how many snapshots the build reads through:

| Build | Dispatched as | Reads the source | Covers changes during the build by |
|---|---|---|---|
| **Ring enumeration** (any shape; the universal fallback) | one cursor inside the discharge transaction | once, at the capture snapshot | the discharge running on the maintenance loop, the only sealer: nothing staged after the pass starts drains before the flip to `live`. A source that is another definition's target also gets a go-live catch-up marker (`mark_definitions_live`, #315): its writes arrive through the seam from drain workers, which don't wait for a seal |
| **Plain 1-1 chunks** | chunk boundaries enumerated and enqueued as `backfill_chunks`, executed by drain threads | once per chunk, each under its own snapshot | the catch-up marker parked when the last chunk goes live (`complete_direct_backfill`), discharged by this same path |
| **Direct set-based build** (aggregates, relationship-enriched 1-1) | one background job | once per statement | the same go-live catch-up marker. *Planned (#419):* today's in-registration build goes live through `go_live_if_backfilling`, not `complete_direct_backfill`, and parks a catch-up only when its source is another definition's target. On a source that is already published, a change that drains during the build is lost for good. #419 must park the catch-up for every direct build |

The go-live catch-up re-reads the source, so a change that drained while the
definition wasn't `live` reaches the target. For an aggregate its re-derivation
of each group also corrects a change the build read whose streamed delta was
folded again after the flip.

A shape the direct build can't render (`BackfillError::Unsupported`) falls back
to ring enumeration, now inside the discharge rather than inside registration.

## Why

- **Postgres has no exact "capture as of registration" without a wait.**
  `ALTER PUBLICATION … ADD TABLE` covers only writes made after it commits. A
  transaction that wrote before the `ALTER` and commits after the reader's
  snapshot is neither streamed nor read. Closing that gap takes one of two
  things. A write-blocking `LOCK TABLE … IN SHARE MODE` stalls the user's own
  writes behind any slow writer, because they queue behind the lock. Waiting out
  every transaction open at the `ALTER` is cluster-wide and unbounded. Creating
  a slot has the same unbounded wait. None of those belong on the registration
  path. Adding a table to a publication also requires owning it, a privilege a
  registering process (a web process, say) shouldn't need.
- **The wait already belongs in `waiting_to_backfill`.** That status is the
  documented, observable signal for exactly this wait
  ([observability](../observability.md#backfill-status-and-the-xmin-caveat)).
- **Consistency bugs keep appearing where capture paths meet.** #393: the
  fresh-install handshake reads at a snapshot taken *before* the slot's
  consistent point, so a row committed during slot creation is lost on both
  sides. Only the leftover join markers repair it today, and that repair is
  incidental. #79, #312, #330 and #387 were all bugs where two capture paths
  meet. One path has no seams to get wrong.
- **Registration latency.** Aggregate and relationship-enriched 1-1
  registrations build synchronously in-call today, so registering one against a
  large table is slow. Under this decision registration's latency doesn't
  depend on table size.

## Rejected alternatives

- **Publish and read at registration**, with a lock or a fence wait to make the
  cut exact. That means either blocking the user's writes or an unbounded
  registration latency, and every registering process would need owner
  privileges on the source.
- **Export the slot's real snapshot** (`CREATE_REPLICATION_SLOT … (SNAPSHOT
  'export')`, #393's option b). It would fix the fresh-install gap, but only by
  keeping a second capture path alive just for fresh installs. It also needs
  replication-protocol code that neither `pgwire-replication` 0.4.1 nor
  `tokio-postgres` provides.

## Consequences

- **No definition is `live` when registration returns.** Callers and tests wait
  for the status to reach `live` (`Trellis::status`), then take a watermark
  token and `await_converged` on it. `await_converged` checks the ring and
  intake's progress. It never reads a definition's status or a pending
  marker, so on its own it doesn't wait for a build that hasn't started. After
  `live` it's still needed, because `live` doesn't yet mean the target is
  complete. A ring enumeration flips to `live` once its rows are staged, before
  they drain. Their `Recompute` rows carry no `origin_lsn`, which the predicate
  treats as older than any token, so the wait covers them. A chunked or direct
  build flips to `live` with its go-live catch-up marker still waiting for a
  later maintenance pass, and neither signal waits for that. Plain 1-1
  definitions already work like this: they return `backfilling` with their
  chunks queued.
- **A running staging worker is required for anything to go live.** That's
  already true: live apply needs intake, and a deferred definition needs the
  discharge ([embedding](../embedding.md#the-silent-stall-hazard-issue-144)).
  Chunked builds also still need drain threads.
- **The discharge's failure handling becomes critical.** It's the single path
  for every definition, so one failing marker blocking the queue (#407) would
  stall *every* registration, and a dead connection stranding definitions in
  `backfilling` (#404) would affect every registration. Both block the
  registration-side children (#418, #419).
- **Only the staging worker needs publication and replication privileges.**
  Registering processes need catalog access and the right to create target
  tables, and nothing on the publication. `DROP` is the open exception: it
  reconciles the publication from whichever process applies it (#427).
- **`backfill_coverage` becomes an optimization at most.** It lets a catch-up
  skip re-reading a table that provably hasn't changed since a build read it.
  No path depends on it for correctness.

## Inventory of capture paths

Every place that reads a source's existing rows, or decides when that read
happens, and what this decision does with it. A **Planned** row names the issue
that implements it. That issue updates the row when it lands. The epic is done
when no row is Planned.

| Path | Where today | Reads the source | Under this decision |
|---|---|---|---|
| Fresh-install handshake read | `intake::publication::initial_snapshot_handshake`, called by `client::setup_staging` when the slot is new | every source table, in the slot-creation transaction, at a snapshot that predates the slot's consistent point (#393) | **Retired. Planned (#417).** Setup creates the slot and seeds `replication_progress`, reads nothing, and makes sure every published table has a marker, the same shape slot-loss recovery (`intake::slot_loss`) already has |
| Ring enumeration inside registration | `defs::catalog::create_definition_inner`'s `enumerate_and_append`, reached through `create_definition` and `install_definition`'s `Unsupported` fallback | the whole source, inside the registration transaction | **Rerouted to the discharge. Planned (#418)** |
| Plain 1-1 chunk enqueue at registration | `install_definition` → `install_plain_one_to_one` → `chunk_queue::enqueue_one_to_one` | the source's key range, to cut chunk boundaries at registration | **Rerouted: the discharge enumerates and enqueues the chunks; drain threads still execute them. Planned (#418)** |
| Synchronous direct build inside registration | `install_definition` → `backfill::backfill_definition` for aggregates and relationship-enriched 1-1, with the coverage fence from `plan_direct_backfill_coverage` / `commit_direct_backfill_coverage` | the whole source and every relationship table, in-call | **Rerouted: the discharge dispatches it as a background job. Planned (#419)** |
| Registration's defer branch | `install_definition`'s `defer_if_fence_unsettled`, `create_definition_inner`'s `backfill_marker_unsettled` check | nothing: defers to the discharge when the source already has an unsettled marker | **Becomes unconditional.** This branch is today's form of the one path. Registration always defers, and the branch goes away. **Planned (#418, #420)** |
| Publication-join discharge | `reconcile_publication` parks; `run_pending_backfills_until` discharges | the table, once its fence settles and intake has caught up | **Kept: the one path** |
| Transform resume (after `PAUSE`, quarantine or slot loss) | `staging::quarantine::resume_transform` parks a marker; the discharge reads | through the discharge (ring enumeration, the only build the discharge runs today) | **Already the one path** |
| Explicit re-backfill | `Trellis::request_backfill` parks a marker | through the discharge | **Already the one path** |
| Go-live catch-ups | `defs::catalog::complete_direct_backfill`, `install_definition`'s #315 park, `mark_definitions_live`'s #315 park, `park_target_catchup_if_read`, discarded-chunk parks in `chunk_queue` | through the discharge | **Already the one path.** The chunked and direct builds' go-live catch-up stays, because starting a build after the fence doesn't cover the changes that drain while it runs. #419 decides which of the others correctness still needs; #420 removes the rest |
| Column resume | `staging::quarantine::resume_column` → `recompute_column`, then a catch-up marker | one column's values, in-call | **Differs: a redefinition-side capture that reads in-call. Planned (#425)** |
| `ALTER TRANSFORM` added columns | `defs::alter_transform` → `backfill::backfill_altered_columns`, then a catch-up marker ([ADR-0015](0015-transform-redefinition.md)) | the added columns' values, in-call | **Differs, as above. Planned (#426)** |
| Publication change outside the staging worker | `DROP` → `Trellis::reconcile_publication_after_drop`, run by whichever process applied the `DROP` | nothing, but it can `ALTER PUBLICATION` (and so park a join marker) from a non-staging process | **Open: needs a design decision (#427).** Either it moves to the staging worker's reconcile pass, which already re-reconciles every maintenance pass and would supersede [ADR-0014](0014-pause-and-drop-a-transform.md)'s "applied at drop time", or `DROP` stays an explicit exception to "only the staging worker changes the publication", with the privileges that implies |

## Left for the children to record here

These are real design calls that this ADR doesn't settle. Whichever issue
settles one records it in this section:

- **Discharge failure handling (#404, #407, decided together).** A marker that
  fails every pass must not starve the markers behind it, and a discharge
  connection that dies mid-pass must not strand definitions in `backfilling`.
- **Where a direct build runs (#419).** It can run on the staging worker or on
  a drain thread. One background job per definition is enough. Splitting these
  shapes into chunks stays a separate optimization (ADR-0007, "Backgrounding
  and resumability").
- **Which consistency bookkeeping stays (#419, #420).** Once every build
  starts after the fence has settled and intake has passed the snapshot, some
  of the coverage fence and `backfill_coverage` may be redundant. The go-live
  catch-up of a chunked or direct build is not: it's what recovers the changes
  that drained while the definition wasn't `live`. Keep what correctness needs,
  say why here, and remove the rest.
- **What `live` promises.** Today `live` means the build has finished, not that
  the target is complete (see [Consequences](#consequences)). Whether the flip
  should wait for the go-live catch-up to discharge, or a caller should get
  some other "target complete" signal, is undecided.
- **A join fence taken after the `ALTER` commits.** `reconcile_publication`
  parks the marker inside the `ALTER`'s transaction, so the fence is a snapshot
  from before the join commits. A writer whose transaction id is assigned after
  that snapshot and which writes the table before the `ALTER` commits isn't
  waited for. It isn't streamed either, because its write precedes the join.
  If it commits after the capture snapshot, its row is lost on both sides. The
  window is short, since the park is the `ALTER` transaction's last statement,
  but it has been reproduced on Postgres 17. The fence needs to postdate the
  commit without losing the marker to a crash between the two. One option: the
  discharge re-fences a marker the first time it sees it, which is necessarily
  after the commit.
