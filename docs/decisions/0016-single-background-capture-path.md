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

1. **Join.** The source gets a `pending_backfill` marker. The staging
   worker's reconcile pass parks it (`reconcile_publication`), adding the
   table to the publication in the same transaction if it isn't there yet (see
   [Who parks the marker](#who-parks-the-marker)). The marker's **fence** has
   to cover every transaction that could have written the table before the
   join committed, since the stream doesn't carry those writes. A snapshot
   taken inside the `ALTER`'s own transaction falls slightly short of that, so
   a marker is parked with no fence and the discharge **fences it the first
   time it sees it**, at a snapshot necessarily later than the `ALTER`'s
   commit (see [The join fence](#the-join-fence)).
   Only the staging worker changes the publication, including the shrink after a
   `DROP` (see [Consequences](#consequences)).
2. **Wait.** The discharge leaves the marker alone until its fence settles:
   every transaction that was open when it was fenced has ended. It then waits
   for intake to stage through the WAL position its read snapshot was taken at.
3. **Capture and build.** The discharge picks the capture snapshot and
   dispatches each of the table's `waiting_to_backfill` definitions' build
   **by shape**: the direct set-based build
   ([ADR-0007](0007-direct-set-based-backfill.md)) or chunked work that drain
   threads execute, whichever the shape has a builder for (resume rebuilds
   included). Ring enumeration is the `Unsupported` fallback, not a size
   threshold. A definition moves to `backfilling` only in the transaction
   that commits the work driving it.
4. **Go live.** A ring enumeration flips its definition to `live` in the
   discharge transaction. A chunked or direct build's completion moves it to
   `catching_up` and parks its go-live catch-up, and the discharge of that
   catch-up flips it `live` (see [What `live` promises](#what-live-promises)).

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
(`ClientOptions::reconcile_interval`), and the marker is parked later, which
only makes it safer: the definition isn't `live` in the meantime, so nothing
it could miss is folded anywhere, and the fence and the capture read come
later too.

### What each build reads

The capture snapshot is taken after the fence has settled, so every commit it
doesn't see comes from a transaction that began after the join, and the stream
carries it. A source that is another definition's target is the exception: it
isn't streamed, and the target-mutation seam carries its writes only to an
applying reader (`live` or `catching_up`). Apply skips a definition whose build
hasn't finished, so every build also needs
something to cover the changes that drain while it runs. What that is depends on
how many snapshots the build reads through:

| Build | Dispatched as | Reads the source | Covers changes during the build by |
|---|---|---|---|
| **Ring enumeration** (any shape; the universal fallback) | one cursor inside the discharge transaction | once, at the capture snapshot | the discharge running on the maintenance loop, the only sealer: nothing staged after the pass starts drains before the flip to `live`. A source that is another definition's target instead gets a go-live catch-up marker, and the definition goes to `catching_up` (`intake::publication::go_live`): its writes arrive through the seam from drain workers, which don't wait for a seal |
| **Plain 1-1 chunks** | chunk boundaries enumerated and enqueued as `backfill_chunks`, executed by drain threads | once per chunk, each under its own snapshot | the catch-up marker parked when the last chunk finishes (`complete_direct_backfill`), discharged by this same path |
| **Direct set-based build** (aggregates, relationship-enriched 1-1) | one background job, a `backfill_chunks` row with no bounds that a drain thread runs start to finish ([The direct-build job](#the-direct-build-job)) | once per statement | the same go-live catch-up marker, parked on every table the build read (its source and each relationship to-side) |

The go-live catch-up re-reads the source, so a change that drained while the
definition was still building reaches the target, and deletes the target rows
the source no longer backs, which a re-read can't reach (#485). For an aggregate, a change the
build read whose streamed delta drains after the flip re-derives its group
through the recompute horizon the build records
([Which consistency bookkeeping stays](#which-consistency-bookkeeping-stays)).

A shape the direct build can't render (`BackfillError::Unsupported`) falls back
to ring enumeration, inside the discharge. The discharge finds out before it
dispatches anything: `backfill::check_direct_build` runs the build's own shape
checks against the catalog, reading no source rows.

### The direct-build job

An aggregate or relationship-enriched 1-1 definition's build is one job, not
chunks: it materializes connection-scoped `TEMP TABLE` staging, which a set of
independent workers on separate connections can't share. Splitting it stays a
separate optimization ([ADR-0007](0007-direct-set-based-backfill.md),
"Backgrounding and resumability").

**The job runs on a drain thread, not on the staging worker.** The discharge
enqueues it as a `backfill_chunks` row with no bounds
(`chunk_queue::dispatch_direct_build`), in the transaction that moves the
definition to `backfilling`. That gives it the chunk queue's whole driver
contract unchanged: a drain thread claims it, heartbeats the claim for as long
as the build runs, and finishing it moves the definition to `catching_up` with
its go-live catch-ups (`chunk_queue::finish_chunk`, `complete_direct_backfill`). A
pause withholds it and a resume supersedes it, exactly as for a chunk. The
staging worker was the other option, and a poor one: its maintenance loop is
the only sealer, so a build that runs for minutes there would stall every
drain in the instance, and it has no claim to reclaim if the process dies. The
cost is the one chunked builds already pay: nothing builds without drain
threads somewhere in the fleet.

**Failure contract.** Neither way a job can end short leaves the definition
stranded in `backfilling`:

- **The worker dies.** Its claim stops being heartbeated, the reclaim sweep
  frees it (`reclaim_stale_chunks`), and another drain thread reruns the whole
  build. Every write the build makes is an idempotent overwrite, so a partial
  earlier run costs nothing.
- **The build fails.** The job hands its build back to the discharge, in one
  transaction (`chunk_queue::fail_chunk`): the job row is deleted, the
  definition moves back to `waiting_to_backfill`, and a marker is parked on
  its source carrying the failure as retry state (attempt count, error text,
  next attempt time). That's the same state a failed discharge records, so the
  same capped backoff paces the retry and `Trellis::status` reports the error.
  The attempt count carries over from the marker that dispatched the job
  (`backfill_chunks.prior_attempts`), so the backoff grows across repeated
  failed builds instead of restarting at each dispatch. The retry is a fresh
  dispatch that re-checks the shape, so a build that failed `Unsupported`
  because a relationship changed under it goes to the ring fallback. (The
  check skips the aggregate build's to-side column typing, so a to-side
  column dropped under a definition fails every retry instead, reported on
  status.) Releasing
  the job for an immediate retry, as a failed chunk is, would re-run a
  whole-table build as fast as it can fail.

### A chunk held across a resume

A pause stops new claims but leaves a chunk or job that a worker already holds
alone ([ADR-0014](0014-pause-and-drop-a-transform.md)), since the worker may be
partway through writing it. A resume stamps the definition's
`fuse_rearmed_at`, and a chunk that recorded an older value is **stale**
(`chunk_queue::STALE`, #360, #397, #418). It never completes the definition,
because the resume's rebuild owns the way back to `live`. However its worker
gives it up (finishing, failing or dying), it's deleted, not freed for a rerun.

**Its writes are fenced, so none lands after the resume (#434).** A chunk
writes its target outside the target-mutation seam. That's safe only while
nothing reads the target, and readers can attach once the rebuild finishes,
because `catching_up` is applying. A stale write landing after that would reach
no reader. That includes a relationship declared on the target in the gap:
when the source change the write carried drains, apply finds the target
already current and stages nothing, and so does the source's catch-up. So each
target write a chunk or job makes runs in a transaction that first locks the
chunk's row `for key share` and checks that the claim is still this worker's
and not stale (`chunk_queue::ClaimFence`). Resume locks the definition's held
chunks `for update` before it commits. A write already in flight commits first,
while the definition is still frozen, and the rebuild overwrites it. A write
that starts later writes nothing. A direct-build job is fenced per statement,
so a job superseded partway through stops at its next write. Because no stale
write can land late, discarding a stale chunk parks nothing. The same fence
stops a worker whose claim was reclaimed while it was still running (its
heartbeat stalled) from writing after the chunk's new holder has finished.

The cost is that a resume waits for the definition's in-flight chunk writes:
one chunk's statement, or one statement of a direct build. `for key share`
doesn't block the heartbeat's refresh of `claimed_at`. The stale-claim sweep
skips a chunk whose write is in flight, as it already skipped any locked row.
Those waits are bounded by the write only while its worker keeps running. A
worker that stalls inside a fenced transaction (a frozen process, or a network
partition the server hasn't noticed yet) holds the chunk's lock until its
session ends, and no timeout bounds that: the resume waits, and the sweep
can't reclaim the chunk. An unfenced write committed on its own, so the same
stall used to hold nothing.

The fence replaced repairing a late write after the fact. #360 had the discard
park a catch-up on the chunk's source. That catch-up repaired the target
itself, but it couldn't repair the target's readers: its re-derivation found
the target already current, so it changed nothing and staged nothing for them.
Fixing that would have meant a catch-up for every kind of reader, including a
relationship consumer's settled projection. Preventing the write keeps the
seam's one exception narrow: only a build writes a target outside the seam,
and only before any reader can attach to it.

### Which consistency bookkeeping stays

The in-registration build carried three pieces of bookkeeping: the go-live
catch-up, the coverage record that let a catch-up skip its re-read, and
registration's defer check. With the build starting only after its marker's
fence has settled:

- **The go-live catch-up stays, and correctness needs it.** Apply skips a
  definition whose build hasn't finished, so a change that drains while the
  job runs reaches nothing. The catch-up marker parked when the build
  finishes, on every table the build read (issue #430), re-derives from
  current state, and its discharge is what takes the definition `live`.
- **The go-live catch-up also deletes what the source no longer backs**
  (issue #485). Its re-read enumerates the keys the source still has, so it
  can't reach a key a skipped delete removed: a 1-1 row whose source row is
  gone, or an aggregate group with no source rows left. So the discharge
  that flips a definition `live` runs #330's anti-join first, in the same
  transaction and before the re-read's `DECLARE`
  (`intake::resume_orphans`): it deletes every target row no source row
  backs, through the target-mutation seam. It reads state, so it needs no
  argument about which deletes drained when, only that anything missing from
  its snapshot drains after the flip, which the discharge running on the only
  sealer gives. For a 1-1 target it is exact. For an aggregate the anti-join
  and the re-read read two snapshots, which leaves #436's short race. A
  `catching_up` definition already applies CDC, so a group the anti-join
  deletes can still have deltas staged for it. Like any live read that finds
  a group empty, the discharge raises the target's extinct horizon (#321),
  and such a delta re-derives the group rather than applying to nothing.
- **The coverage fence and `backfill_coverage` are retired** (issues #468,
  #485). A coverage record let a direct build's go-live catch-up skip a table
  none of whose rows had changed since a fence taken before the build read
  it, with the same row count. A row inserted after the fence, read by the
  build and deleted again nets out on both, so the skip kept a row the source
  no longer had (#468). No check on table state can tell that apart from an
  unchanged table, so the skip is gone, and every go-live catch-up re-reads
  every table its build read. That is the cost the record existed to avoid:
  see [Consequences](#consequences).
  A commit the build read *and* the stream carries still needs no catch-up:
  its delta can drain after the flip. For a 1-1 target that's harmless, since
  apply re-evaluates the row from live state. For an aggregate, the build
  records its read as a recompute horizon, on each group row it writes and on
  the target's extinct horizon for groups it found empty
  ([stage 05](../staging-and-claiming/05-apply-and-exactly-once-deltas.md#aggregate-groups-the-recompute-horizon)),
  so such a delta re-derives its group instead of counting the commit twice.
- **Registration's defer check is gone.** Registration always defers now, so
  `defer_if_fence_unsettled` and the `backfill_marker_unsettled` check it asked
  are removed.

The direct build doesn't wait for intake to reach its snapshot, as a ring
enumeration does (#312): the horizon makes that wait unnecessary for
correctness, and it only ever made re-derivations rarer.

A catch-up that reads only what changed (a log of the keys apply skipped
while a definition was building) is the known follow-up if the full re-read
proves too costly (#456, #485's option (c)).

### The join fence

`reconcile_publication` parks the join marker inside the `ALTER PUBLICATION`'s
own transaction, so any snapshot taken there predates the join's commit. A
writer whose transaction id is assigned after that snapshot, and which writes
the table before the `ALTER` commits, is neither waited for by a fence taken
there nor streamed (its write precedes the join). If it commits after the
capture snapshot its row is lost on both sides. The window is short, since the
park is the `ALTER` transaction's last statement, but it was reproduced on
Postgres 17 while the fence was still the parking transaction's own snapshot
(#431).

**The discharge fences a marker the first time it sees it** (#431). A park
leaves the marker's `fence_xid` null. The marker exists exactly when the
`ALTER` committed, so any fence the discharge takes after reading the
committed marker necessarily postdates the commit. The first pass that reads
an unfenced marker records a fence (`confirm_fence`, scoped to the generation
it read) and waits on it. Later passes wait on the recorded fence. This is
crash-safe by construction: there is no window between the `ALTER` and the
marker to recover from.

The fence is a transaction id, not a snapshot: the id of the statement that
takes it, assigned after the read. Every writer in the join's window already
had an id by then, so each is below the fence, and the fence has settled once
a snapshot's `xmin` is past it. A snapshot's `xmax` would not do. It is one
past the latest *completed* transaction, so two open transactions `t < w`
with nothing at or past `t` completed give the snapshot `t:t:`, and once `t`
ends `xmin` is past that `xmax` while `w` is still open.

Every marker is fenced this way, including ones parked where no `ALTER`
happened (a table already in the publication, a resume catch-up, a go-live
catch-up). A new park of the same table clears the fence, so the new
generation is fenced afresh after its own commit. A go-live catch-up parked
inside the flip's transaction therefore needs no second park after the
commit. The pass that takes a fence waits for it to settle, bounded by the
same timeout as its wait for intake (`fresh_fences_settled`), and a pass lets
only one of those waits run out, so it never stalls the maintenance loop for
more than one timeout. The writers a
fresh fence names are normally ones in flight at that instant, so the cost is
the accepted one: one extra fence wait per marker, normally milliseconds. A
long transaction elsewhere in the cluster outlasts the bound and the marker
waits for a later pass, which `waiting_to_backfill` already signals. This is
what keeps the join step gap-free.

### What `live` promises

*Decided 2026-09-24, implemented by #476.* `live` tells an operator that a
transform is in its **steady state**. Once a definition reports `live`, a
watermark token taken after any commit and awaited with
`Trellis::await_converged` guarantees that the transform's values reflect
every commit at or before that token. Nothing else is needed: no marker
checks, no extra waits.

The two signals stay separate on purpose. `await_converged` is a pure LSN wait
over captured changes. It never reads definition status or backfill markers,
so it can return while a definition that isn't `live` yet is still missing
rows. Backfill is an operator concern, reported by `Trellis::status`. A reader
that needs a settled target checks both: `live` from status, then its token.

For that to hold, a definition may flip to `live` only once nothing from its
initial capture is still outstanding:

- **Ring enumeration** flips in the same transaction as its read, and its
  `Recompute` rows carry no `origin_lsn`, so they gate any token.
- **Chunked and direct builds** finish in `catching_up`, not `live`
  (`complete_direct_backfill`), and park their go-live catch-up on every table
  the build read. The discharge of the last of those catch-ups flips the
  definition `live`, in the same transaction as its re-read and its orphan
  sweep (`intake::publication::go_live_caught_up`, #485). That
  discharge runs on the maintenance loop, the only sealer, like ring
  enumeration's.
- **A ring enumeration whose source is another definition's target** goes to
  `catching_up` and parks its catch-up on that source (`go_live`). Its
  source's writes reach it only through the target-mutation seam, from drain
  workers that don't wait for a seal, and only once it applies, so it must
  apply before the catch-up's fence is taken, and report `live` only after
  the catch-up has read.

**`catching_up` is applied exactly as `live` is.** Apply, the seam and every
"does anything read this?" check treat the two alike
(`TransformStatus::is_applying`). Only the status an operator reads differs.
That is what makes each flip sound: the definition has been applying since
before any of its pending catch-ups was fenced, so every change committed
after a catch-up's read reaches it through apply, and everything before is
re-derived by the read's `Recompute` rows, which gate any token taken after
the flip. A definition that reads several tables (a direct build's source and
relationship to-sides) flips only when no marker is pending on any of them,
which is correct however many passes the catch-ups span, because it applied
the whole time.

#### Catch-ups on a definition that is already `live`

Some catch-ups are parked on a definition that is already `live`: a column
resume (`staging::quarantine::resume_column`), an `ALTER TRANSFORM` that
added columns, and an upstream rebuild that wrote a reader's source outside
the seam (`park_target_catchup_if_read`). Each leaves the target missing
something until the catch-up runs.

**Decision:** such a park moves the definition from `live` to `catching_up`
in the same transaction (`intake::publication::park_catch_up`), and the
catch-up's discharge flips it back. It keeps applying throughout, so nothing
stops. Considered and rejected:

- **Moving it back to `backfilling`.** Apply skips a definition that isn't
  applying, so it would stop taking new changes for as long as the catch-up
  waited, and those would then need a catch-up of their own.
- **Leaving it `live` and reporting the pending marker beside the status**
  (a flag on `DefinitionStatus`). Every reader of `live` would have to know
  to check the flag too, which is the contract this section exists to make
  unnecessary; a status word keeps `live` meaning one thing.
- **Deriving the state from pending markers when status is read.** A marker
  is per table, not per definition: a new registration's join marker on a
  shared source would report every `live` definition on it as catching up,
  though none of them is missing anything.

**Locks.** A park locks the definition rows before it parks the markers, and
the discharge locks the `catching_up` definitions it may flip before it
deletes its marker, then checks for pending markers in a later statement. A
park racing a discharge therefore either commits first (and the discharge
sees its marker) or waits for the flip (and moves the definition back).

**Time to `live`.** A chunked or direct build's go-live now waits for one
discharge pass, and nothing but the maintenance loop's reconcile timer used
to start one, up to `ClientOptions::reconcile_interval` (5 s) later. The
maintenance loop now checks every tick for a marker no pass has fenced yet
(`intake::publication::discharge_wanted`) and runs its reconcile pass at
once when it finds one. Each fresh marker triggers one early pass: the pass
fences it, and a marker whose fence doesn't settle in that pass, or whose
enumeration defers on intake, waits for the regular interval as before. A
pass that runs out a whole catch-up timeout (a long transaction holding a
fresh fence, or intake behind) suspends early passes until the next regular
one. Otherwise markers parked one after another behind that transaction
would each start a pass that waits it out, back to back, and the loop, the
only sealer, would barely seal. With the suspension, the stall is bounded as
#431 bounds it: one timeout per `reconcile_interval`.

**A catch-up that keeps failing keeps its definition `catching_up`**, and
the discharge's backoff and error reporting (#407) apply. `Trellis::status`
shows the error when the failing marker is on the definition's source; a
failing marker on a relationship to-side is only in the staging worker's
log.

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
  registrations used to build synchronously in-call, so registering one against
  a large table was slow. Under this decision registration's latency doesn't
  depend on table size.

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
  token and `await_converged` on it ([What `live` promises](#what-live-promises)).
  `await_converged` checks the ring and intake's progress. It never reads a
  definition's status or a pending marker, so on its own it doesn't wait for a
  build that hasn't started. A ring enumeration flips to `live` once its rows
  are staged, before they drain. Their `Recompute` rows carry no `origin_lsn`,
  which the predicate treats as older than any token, so the wait covers them.
  A chunked or direct build reports `catching_up` until its go-live catch-up
  has been discharged (#476).
- **A running staging worker is required for anything to go live.** That's
  already true: live apply needs intake, and a deferred definition needs the
  discharge ([embedding](../embedding.md#the-silent-stall-hazard-issue-144)).
  Chunked builds also still need drain threads. A fleet of drain threads
  with no staging worker finishes builds but leaves them `catching_up`.
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
    durable, reclaimable job row, which a failed build trades back for a marker
    ([failure contract](#the-direct-build-job)). A crash anywhere leaves the
    definitions `waiting_to_backfill` with the marker intact, or `backfilling`
    with their rows, so the next pass or the next claim retries — no definition
    is ever visibly `backfilling` without something driving it forward.
- **Only the staging worker needs publication and replication privileges.**
  Registering processes need catalog access and the right to create target
  tables, and nothing on the publication. A `DROP` is no exception: it only
  removes catalog rows, and the staging worker's reconcile pass shrinks the
  publication from the catalog, its sole source of truth for what to publish.
  This removes `reconcile_publication_after_drop`, stops treating the startup
  `source_tables` copy as a permanent floor, and supersedes
  [ADR-0014](0014-pause-and-drop-a-transform.md)'s "applied at drop time".
  **Done (#427).** `ClientOptions::source_tables` is deleted rather than kept
  as an additive list for engine-level callers: any list the worker unions in
  is a second source of truth, and one that outlives the definitions behind
  it re-adds a dropped table on every pass. The worker reads
  `defs::publication_tables` at startup (`setup_staging`) and on every
  reconcile pass, and nothing else. A caller that wants a table published
  registers a definition that reads it. With the list gone, a staging worker
  no longer needs a definition to start (`TrellisError::NoDefinitions` and
  `ClientError::NoSourceTables` are removed): it starts with an empty
  publication and adds each table on the pass after something registers a
  reader.
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
- **Every build's go-live pays a full re-read and an anti-join (accepted,
  #485).** With `backfill_coverage` retired, the go-live catch-up of every
  chunked or direct build enumerates each table its build read into the ring,
  and runs the orphan anti-join over its target, in one transaction on the
  maintenance loop, which seals nothing meanwhile. On a 5M-row source the
  anti-join measured about 2.1 s for a 1-1 target and 0.45 s for an
  aggregate, on top of the enumeration (about 4 s server-side) and the drain
  of one `Recompute` per row. It scales with table size, not with what
  changed during the build (#456). A catch-up that reads only the keys apply
  skipped is the follow-up if that proves too costly.

## Inventory of capture paths

Every place that reads a source's existing rows, or decides when that read
happens, and its role in this design.

| Path | Where in code | Role in the design |
|---|---|---|
| Fresh-install handshake read | was `intake::publication::initial_snapshot_handshake`, called by `client::setup_staging` when the slot is new | **Retired (#417).** `create_slot_and_park_markers` creates the slot, seeds `replication_progress`, and parks a marker on every table the catalog says to publish in the same transaction, after slot creation returns. It reads nothing, the same shape slot-loss recovery (`intake::slot_loss`) already has. The discharge skips a table no definition reads (`defs::catalog::table_has_reader`) |
| Ring enumeration inside registration | was `defs::catalog::create_definition_inner`'s `enumerate_and_append`, reached through `create_definition` and `install_definition`'s `Unsupported` fallback | **Done (#418).** The discharge's ring fallback does it (`intake::publication::run_pending_backfills`, dispatch by shape). `create_definition` survives only as a test fixture that stands in for that discharge |
| Plain 1-1 chunk enqueue at registration | was `install_definition` → `install_plain_one_to_one` → `chunk_queue::enqueue_one_to_one` | **Done (#418).** The discharge plans the chunks and enqueues them in its own transaction (`chunk_queue::dispatch_one_to_one`); drain threads still execute them |
| Synchronous direct build inside registration | was `install_definition` → `backfill::backfill_definition` for aggregates and relationship-enriched 1-1 | **Done (#419).** The discharge dispatches it as one background job (`chunk_queue::dispatch_direct_build`) that a drain thread runs ([The direct-build job](#the-direct-build-job)) |
| Registration's defer branch | was `install_definition`'s `defer_if_fence_unsettled`, and `create_definition_inner`'s `backfill_marker_unsettled` check | **Done (#418, #419).** Registration always defers to the discharge, and the branch is gone |
| Publication-join discharge | `reconcile_publication` parks; `run_pending_backfills_until` fences and discharges | The one path. **Done (#431):** the discharge fences every marker the first time it sees it ([The join fence](#the-join-fence)) |
| Transform resume (after `PAUSE`, quarantine or slot loss) | `staging::quarantine::resume_transform` parks a marker; the discharge reads | The one path. Dispatch by shape reroutes the rebuild onto the chunked or direct builder like any other capture, ring only for `Unsupported`. **Done (#418, #419):** a plain 1-1 rebuild is chunked, an aggregate or relationship-enriched 1-1 rebuild is a direct-build job, and a chunk or job planned before the resume is told apart by the definition's `fuse_rearmed_at` it recorded (`backfill_chunks.fuse_rearmed_at`). The discharge still deletes the target rows no source row backs before it dispatches (#330); the direct build doesn't visit a group with no source rows, so it relies on that. The go-live catch-up's discharge runs the same deletion again (#485). #436 is the aggregate race left between the deletion and the re-read |
| Explicit re-backfill | `Trellis::request_backfill` parks a marker | The one path |
| Go-live catch-ups | `defs::catalog::complete_direct_backfill`, `intake::publication::go_live`, `park_target_catchup_if_read`, discarded-chunk parks in `chunk_queue`, all through `park_catch_up` | The one path. A chunked or direct build's go-live catch-up stays, because starting a build after the fence doesn't cover the changes that drain while it runs. Its discharge always re-reads (`backfill_coverage` is retired, #468), deletes the target rows no source row backs (#485), and flips the definition `live` (`go_live_caught_up`, #476) |
| Direct-build coverage skip | was `backfill_coverage`, recorded by the direct-build job and read by the discharge's `coverage_covers` | **Retired (#468, #485).** It took a table whose row count and `xmin`s were unchanged since the build's fence for unchanged, which a row inserted and deleted during the build defeats. V48 drops the table |
| Column resume | `staging::quarantine::resume_column` → `recompute_column`, then a catch-up marker | Separate: a redefinition-side capture that reads one column's values in-call. The definition is `catching_up` until the marker discharges (#476) |
| `ALTER TRANSFORM` added columns | `defs::alter_transform` → `backfill::backfill_altered_columns`, then a catch-up marker ([ADR-0015](0015-transform-redefinition.md)) | Separate: a redefinition-side capture that reads the added columns' values in-call. The definition is `catching_up` until the marker discharges (#476) |
| Publication change on `DROP` | was `Trellis::reconcile_publication_after_drop`, run by whichever process applied the `DROP` | **Done (#427).** A `DROP` only removes catalog rows, and the staging worker's reconcile pass (`client::reconcile_source_tables`) shrinks the publication from the catalog. `ClientOptions::source_tables`, the startup copy that used to act as a permanent floor, is deleted. Supersedes [ADR-0014](0014-pause-and-drop-a-transform.md)'s "applied at drop time" |

## Open questions

- None. "What `live` promises" was settled on 2026-09-24 and implemented by
  #476, including how a catch-up on an already-`live` definition shows; see
  [What `live` promises](#what-live-promises).
