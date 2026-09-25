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

Registration can't park the marker itself, even when the source needs no
publication change. It can tell from the catalog that a source is another
definition's target, but not that it is already published: which publication
the staging worker serves is that worker's own option
(`ClientOptions::publication`), not something the catalog or a registering
process knows, and guessing wrong in either direction is a bug (a marker on a
table not yet streamed, or no marker ever for an already-published table). The
staging worker knows exactly which tables it has just published, so it parks
both cases and there is one owner. The cost is at most one reconcile pass of
latency (`ClientOptions::reconcile_interval`), and the marker is parked later,
which only makes it safer: the definition isn't `live` in the meantime, so
nothing it could miss is folded anywhere, and the fence and the capture read
come later too.

A table has at most one marker, and every park goes through
`intake::publication::park_marker`. A park that finds a marker already there
merges into it and gives it a new `generation`, and a discharge deletes only
the generation it read (#311, #367). Parks still race discharges: a build's
go-live catch-up, a resume or a `request_backfill` can park while a discharge
of the same table is mid-read, its snapshot taken before the park's change.
The generation keeps that park's marker for the next pass instead of letting
the discharge delete it.

### A fresh install

A fresh install (no `replication_progress` row for the configured slot)
creates the slot and parks a marker on every table the catalog says to
publish, in one transaction, after slot creation returns
(`intake::publication::create_slot_and_park_markers`). It reads nothing:
`pg_create_logical_replication_slot` exports no snapshot, and the
transaction's own snapshot is taken before slot creation waits out the
transactions in flight, so a read there would miss a row committed during
that wait, which the slot doesn't stream either (#393). The discharge fences
each marker after reading it committed, so its read comes after the slot's
consistent point, and every commit the read misses is streamed.

On a first install the catalog has nothing `live` yet, and the reconcile pass
would park a marker for each `waiting_to_backfill` definition anyway. The
markers on every table matter when the catalog outlived the slot it was built
under (setup pointed at a new slot name, say; a slot lost under the same name
is slot-loss recovery's case, `intake::slot_loss`, which pauses the
transforms it fed). A definition that is already applying has then missed
whatever committed before the new slot's consistent point. So each marker is
a go-live catch-up for the table's applying readers
([A re-read table's readers](#a-re-read-tables-readers)):
they report `catching_up` until the discharge has re-read the table, which
re-derives the rows the source still has, and swept their targets for the
rows it no longer backs, which no re-read reaches. The discharge skips a
table nothing reads.

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
([Covering changes during a build](#covering-changes-during-a-build)).

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
A worker that stalls inside a fenced transaction (a frozen process, or a
network partition the server hasn't noticed yet) would hold the chunk's lock
until its session ended. An unfenced write committed on its own, so the same
stall used to hold nothing. So the fenced transaction sets its
`idle_in_transaction_session_timeout` to the fleet's reclaim TTL
(`ClientOptions::reclaim_ttl`). The server ends a session left idle in the
transaction that long, which releases its locks when the sweep would have
given up on the claim anyway. The resume's wait is then bounded by the TTL,
or by a write statement that is still running. The resume takes no
`lock_timeout` of its own: it can't know the fleet's TTL, and the idle
timeout already bounds the wait.

Considered and rejected: repairing a late write after the fact, as the
discard did before #434 by parking a catch-up on the chunk's source (#360).
That catch-up repaired the target itself, but it couldn't repair the
target's readers: its re-derivation found
the target already current, so it changed nothing and staged nothing for them.
Fixing that would have meant a catch-up for every kind of reader, including a
relationship consumer's settled projection. Preventing the write keeps the
seam's one exception narrow: only a build writes a target outside the seam,
and only before any reader can attach to it.

### Covering changes during a build

Starting a build only after its marker's fence has settled covers the commits
before the build, not the ones that drain while it runs. Three pieces cover
those, and each is load-bearing: removing it fails named tests (#420).

- **The go-live catch-up.** Apply skips a definition whose build hasn't
  finished, so a change that drains while the job runs reaches nothing. The
  catch-up marker parked when the build finishes, on every table the build
  read (issue #430), re-derives from current state, and its discharge is
  what takes the definition `live`. It always re-reads: no check on table
  state can tell a table unchanged since the build from one where a row was
  inserted, read by the build and deleted again, since both leave the row
  count and `xmin`s as they were (#468). That is a cost: see
  [Consequences](#consequences). A build that flips `live` at its finish
  instead fails every test in `direct_build_catchup_marker.rs` and
  `go_live_catch_up_repairs.rs` but one.
- **The go-live catch-up also deletes what the source no longer backs**
  (issue #485). Its re-read enumerates the keys the source still has, so it
  can't reach a key a skipped delete removed: a 1-1 row whose source row is
  gone, or an aggregate group with no source rows left. So the discharge
  that flips a definition `live` also runs #330's anti-join
  (`intake::resume_orphans`), and deletes every target row no source row
  backs, through the target-mutation seam. It reads state, so it needs no
  argument about which deletes drained when, only that anything its
  snapshot misses drains after the flip, which the discharge running on the
  only sealer gives. A `catching_up` definition already applies CDC, so a
  group the anti-join deletes can still have deltas staged for it. Like any
  live read that finds a group empty, the discharge raises the target's
  extinct horizon (#321), and such a delta re-derives the group rather than
  applying to nothing. A catch-up that re-reads but doesn't sweep leaves a
  deleted 1-1 row and an emptied group behind.
- **The anti-join is judged on the re-read's own snapshot** (issue #436). It
  is a branch of the same cursor that enumerates the table, so one
  statement reads both, and the discharge deletes the rows it returns by key
  as the cursor is fetched, after the intake wait. A row unbacked on that
  snapshot is deleted and rebuilt from nothing by whatever changes after
  it; a row backed on it is re-derived by the enumeration's `Recompute`.
  Neither depends on when the source changed, so it is exact for aggregates
  as well as 1-1. An anti-join on a snapshot of its own, before or after the
  re-read's, leaves an aggregate group stale either way: one emptied between
  the anti-join and the re-read and refilled after the re-read is neither
  deleted nor enumerated (#391 measured 103 where the source said 100), and
  one empty at the re-read and refilled before a later anti-join is kept.
  The other way #436 offered was to have each build clear exactly the key
  space it enumerates: stage a group-level `Recompute` for every target
  group as well as every source key, and let apply re-derive or delete each
  one. That needs a new kind of ring row, keyed by target group rather than
  source key and addressed to one definition rather than every reader of
  the table, and apply would learn to consume it. The shared snapshot
  needs neither, and it keeps what #330 settled: nothing is cleared up
  front, so readers never see an empty target, and a discharge that rolls
  back (a deferred intake wait, an error) deletes nothing, so no status
  commits without its driver (#404). Deleting after the wait also means
  the deleted rows stay locked only for the end of the discharge, not
  across the intake wait (#503).

A commit the build read *and* the stream carries needs no catch-up: its delta
can drain after the flip. For a 1-1 target that's harmless, since apply
re-evaluates the row from live state. For an aggregate, the build records its
read as a recompute horizon, on each group row it writes and on the target's
extinct horizon for groups it found empty
([stage 05](../staging-and-claiming/05-apply-and-exactly-once-deltas.md#aggregate-groups-the-recompute-horizon)),
so such a delta re-derives its group instead of counting the commit twice.
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
added columns, an upstream rebuild that wrote a reader's source, or a
table it reads through a relationship, outside the seam
(`park_target_catchup_if_read`, #507), and a re-read of a table whose
changes may not all have reached it: a fresh slot under a catalog that
outlived its old one ([A fresh install](#a-fresh-install)), an explicit
`request_backfill`, or the table rejoining the publication
([A re-read table's readers](#a-re-read-tables-readers)). Each leaves the
target missing something until the catch-up runs.

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
  though none of them is missing anything. (Deriving it from *upstream
  status*, below, is a different rule: a definition's status is per
  definition.)

**Locks.** A park locks the definition rows before it parks the markers, and
the discharge locks the `catching_up` definitions it may flip before it
deletes its marker, then checks for pending markers in a later statement. A
park racing a discharge therefore either commits first (and the discharge
sees its marker) or waits for the flip (and moves the definition back).

**Time to `live`.** A chunked or direct build's go-live waits for one
discharge pass. The reconcile timer alone would start one up to
`ClientOptions::reconcile_interval` (5 s) later, so the maintenance loop
also checks every tick for a marker no pass has fenced yet
(`intake::publication::discharge_wanted`) and runs its reconcile pass at
once when it finds one. Each fresh marker triggers one early pass: the pass
fences it, and a marker whose fence doesn't settle in that pass, or whose
enumeration defers on intake, waits for the regular interval. A pass that
runs out a whole catch-up timeout (a long transaction holding a fresh fence,
or intake behind) suspends early passes until the next regular one.
Otherwise markers parked one after another behind that transaction would
each start a pass that waits it out, back to back, and the loop, the only
sealer, would barely seal. With the suspension, the stall is bounded as
#431 bounds it: one timeout per `reconcile_interval`.

**A catch-up that keeps failing keeps its definition `catching_up`**, and
the discharge's backoff and error reporting (#407) apply. `Trellis::status`
shows the error when the failing marker is on the definition's source; a
failing marker on a relationship to-side is only in the staging worker's
log.

#### A rebuilt target's readers

*Decided 2026-09-24, implemented by #507.* A resumed definition's rebuild
writes its target outside the target-mutation seam, so nothing it writes
reaches the target's readers or a relationship's settled projection. When the
build finishes, `park_target_catchup_if_read` parks a catch-up on the target
for **every** applying definition that reads it, directly as its source or
through a relationship whose to-side it is, and parks it even with no reader
when the target is a relationship's to-side. The discharge of that marker:

- **Re-derives relationship consumers by reverse propagation.** Its
  enumeration stages an image-less `Recompute` per target key, and apply's
  reverse path turns each into a `Recompute` of every from-side row joined to
  it, as it does for any image-less change to a to-side. That covers every
  joined row, changed by the rebuild or not, as the catch-up does for a
  direct reader: the discharge can't tell which keys the rebuild changed.
- **Refreshes a to-one relationship's settled projection** from the target,
  in the discharge's own transaction (`refresh_relationship_projections_in_txn`):
  it deletes, rewrites or inserts each projection row that differs. A 1-1
  consumer reads the projection, never the target, so the re-derive above
  would otherwise reproduce the stale value. It diffs the whole target (about
  0.7 s for a 1M-row target, on the sealer), so only a marker parked for such
  a rewrite asks for it (`pending_backfill.refresh_projections`), or for a
  source to-side's lost changes ([A re-read table's
  readers](#a-re-read-tables-readers)); every other write reaches the
  projection through the seam or CDC.
- **Keeps older seam rows from undoing the refresh.** A seam row staged before
  the rebuild can still be pending when the refresh runs, and its image
  predates what the rebuild wrote. For a to-side that is one of this
  instance's targets, apply compares each reverse record's images with the
  live row; when they disagree it writes the projection from the live row and
  re-derives the from-side rows by the image-less fallback rather than a
  delta (`staging::apply::to_side_superseded`).

Considered and rejected: routing rebuild writes through the seam (every
rebuild would pay full per-row reverse propagation), and rebuilding every
consumer from its own source when the target goes live (a full rebuild per
consumer on every upstream resume).

#### A re-read table's readers

*Implemented by #420 (a fresh install) and #522.* Three parks re-read a
source table because definitions already applying from it may have missed
some of its changes, which reached neither CDC nor their targets:

- **A fresh install** under a catalog that outlived its slot
  (`create_slot_and_park_markers`, [A fresh install](#a-fresh-install)).
- **An explicit re-backfill** (`Trellis::request_backfill`). Its caller
  suspects a target has drifted from its source, and a delete that never
  reached the target is the case a re-read alone can't repair.
- **A table rejoining the publication** (`reconcile_publication`'s join
  marker). A table normally joins with nothing applying from it yet, but one
  an operator dropped from the publication while something applied from it
  has missed everything written meanwhile.

All three park through `intake::publication::park_table_catch_ups`: each
marker is a go-live catch-up for every applying definition that reads the
table, directly or through a relationship (`defs::catalog::applying_readers`).
Each reports `catching_up` until the discharge has re-read the table, which
re-derives the rows the source still has, and swept the reader's target of
the rows it no longer backs, which no re-read reaches (#485's sweep, judged on
the re-read's snapshot, #436).

A table that is a relationship's to-side also asks the discharge to refresh
its settled projections (`pending_backfill.refresh_projections`), as a
rebuilt target does ([A rebuilt target's readers](#a-rebuilt-targets-readers)).
A to-one consumer reads the projection, never the table, and the projection
of a source to-side follows its CDC, so a change whose CDC was lost is
missing there too; the re-read's image-less `Recompute`s would re-derive the
consumer from the stale projection. The refresh runs even when nothing reads
the table yet, so a consumer registered later doesn't read a stale
projection. It is safe against CDC still pending when it runs: that CDC was
committed before the re-read's snapshot, so once it has drained, in commit
order, each projection row is back at the state the refresh wrote.

A marker parked only for a `waiting_to_backfill` definition's build (a
registration's, a resume's, a failed direct build's retry) stays a plain
marker: the definitions already applying from the table have missed nothing,
and a catch-up would only move them to `catching_up` for no reason.

#### A reader whose upstream isn't `live`

*Decided 2026-09-24, implemented by #497.* A definition reads an
**upstream** when another definition's target is its source, or the to-side
of a relationship one of its fields reads through. While that upstream is
paused, quarantined, rebuilding (`waiting_to_backfill`, `backfilling`) or
`catching_up`, its target doesn't reflect its own source, so neither does
the reader, and a token taken after a commit to the upstream's source
wouldn't wait for what's missing. So a `live` reader of an upstream that
isn't `live` reports `catching_up`. The rule is transitive: an upstream that
only reports `catching_up` because of its own upstream counts too. The reader
keeps applying, and `reject_non_live_upstream` stays what it was, a guard on
registration.

**Decision: derived when status is read, not stored.**
`defs::catalog::reported_statuses` computes it from the catalog alone (one
read of the definitions, one of the relationships) for `Trellis::status`,
`Trellis::definitions` and `Trellis::quarantine_status`. The persisted status
never changes for it. Considered and rejected:

- **A stored transition.** Every status change of every upstream (pause,
  resume, quarantine, drop, each go-live) would have to walk its readers down
  the chain and move them, under the same locks the catch-up parks take, and
  a reader's own go-live (`go_live_caught_up`) would have to check its
  upstreams before flipping. Missing any one of those transitions leaves a
  reader stuck `catching_up` or wrongly `live`. The derivation has no such
  transitions to miss, and it's cheap: status is an operator read, and the
  catalog is small.

A rebuilt upstream still parks a catch-up for its readers
(`park_target_catchup_if_read`), because what the rebuild wrote outside the
seam never reached them. That one is stored, like every catch-up, and the
reader reports `live` only once both hold: its upstream is `live` and its own
catch-up has run.

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
  This supersedes [ADR-0014](0014-pause-and-drop-a-transform.md)'s "applied
  at drop time". The worker reads `defs::publication_tables` at startup
  (`setup_staging`) and on every reconcile pass, and nothing else: no
  configured list of tables is unioned in, because any such list is a second
  source of truth, and one that outlives the definitions behind it re-adds a
  dropped table on every pass. A caller that wants a table published
  registers a definition that reads it. A staging worker needs no definition
  to start: it starts with an empty publication and adds each table on the
  pass after something registers a reader.
- **A ring read is one transaction (known limitation).** The ring
  enumeration stages one `Recompute` row per source key inside the discharge
  transaction. That's the `Unsupported` fallback's whole build, and it's also
  every catch-up's read on a table something reads: the go-live catch-up of
  every chunked or direct build, a resume's rebuild of a shape still built by
  ring, a `request_backfill`. On a very large table that's one long
  transaction holding back `xmin`, and a ring write the size of the table.
  Dispatch by shape keeps it off the initial build of every shape with a
  builder, but not off the catch-ups. Bounding it (paging the enumeration
  across transactions, or a catch-up that reads only what changed) is #456.
- **Every build's go-live pays a full re-read and an anti-join (accepted,
  #485).** Nothing lets a catch-up skip a table (#468), so the go-live catch-up of every
  chunked or direct build enumerates each table its build read into the ring,
  and runs the orphan anti-join over its target, in one transaction on the
  maintenance loop, which seals nothing meanwhile. On a 5M-row source the
  anti-join measured about 2.1 s for a 1-1 target and 0.45 s for an
  aggregate, on top of the enumeration (about 4 s server-side) and the drain
  of one `Recompute` per row. It scales with table size, not with what
  changed during the build (#456). A catch-up that reads only the keys apply
  skipped is the follow-up if that proves too costly.

## History

The decision was recorded on 2026-09-23 (#416) and carried out as epic #415's
children, roughly in this order:

- **Hardening the discharge first** (#404, #407, #431), since every
  registration would depend on it: `backfilling` commits only with its
  driver, a failing marker backs off instead of blocking the queue, and the
  discharge takes each marker's fence after reading it rather than the
  parking transaction taking it.
- **A fresh install stops reading** (#417). It used to read every
  configured table inside the slot-creation transaction, which is #393's
  gap. #427 then made the staging worker the only process that changes the
  publication, from the catalog alone: `Trellis::reconcile_publication_after_drop`
  and `ClientOptions::source_tables` are gone, and with them
  `TrellisError::NoDefinitions` and `ClientError::NoSourceTables`.
- **Registration stops reading** (#418, #419). The in-registration ring
  enumeration, chunk enqueue and direct builds moved into the discharge's
  dispatch by shape, and registration's defer check
  (`defer_if_fence_unsettled`, `backfill_marker_unsettled`) went with them,
  since registration now always defers. The split first recorded here had
  registration park the marker itself when the source needed no publication
  change; the staging worker parks it instead ([Who parks the
  marker](#who-parks-the-marker)).
- **Making the go-live catch-up exact.** #436 judged the orphan sweep on the
  re-read's own snapshot. #468 and #485 retired the coverage record
  (`backfill_coverage`, dropped by V48) that let a direct build's catch-up
  skip a table that looked unchanged, and added the sweep to the catch-up.
  #434 replaced #360's catch-up for a stale chunk's late write with a fence
  that stops the write.
- **What `live` means** (#476, #497, #507): a build reports `catching_up`
  until its catch-up has run, a reader of a non-`live` upstream reports
  `catching_up`, and a rebuilt target's readers get a catch-up of their own.
- **The closing audit** (#420) checked what was left for a job other than
  reconciling two capture paths, by removing each piece and running the
  tests. The go-live catch-up, its sweep and the marker generation all stayed
  ([Covering changes during a build](#covering-changes-during-a-build),
  [Who parks the marker](#who-parks-the-marker)). So did the fresh install's
  marker on every table, but the audit found it re-read the table without
  sweeping, so a row deleted before the new slot's consistent point stayed
  in a `live` target; it became a catch-up for the table's applying readers
  ([A fresh install](#a-fresh-install)). Removed: `park_backfill_catchup`, an
  alias of `park_marker`, and `pending_backfill.added_at`, which nothing read
  (V50). The test fixtures `defs::create_definition`,
  `create_definition_without_backfill` and the unfenced
  `backfill::backfill_definition` stay: they are compiled only under
  `cfg(test)` or the `test-util`/`internals` features, which no production
  build enables.
- **Every re-read is a catch-up** (#522). The audit's fix for a fresh
  install applied just as well to an explicit `request_backfill` and to a
  table rejoining the publication, which parked plain markers; all three now
  park through `park_table_catch_ups`, and a re-read to-side's settled
  projections are refreshed too ([A re-read table's
  readers](#a-re-read-tables-readers)).

Two rebuilds of some columns of a `live` transform still read in-call and
then park a catch-up: a column resume (#425) and an `ALTER TRANSFORM` that
adds columns (#426). They are the only capture outside the discharge.

## Inventory of capture paths

Every place that reads a source's existing rows, or decides when that read
happens, and its role in this design.

| Path | Where in code | Role in the design |
|---|---|---|
| Fresh-install handshake read | was `intake::publication::initial_snapshot_handshake`, called by `client::setup_staging` when the slot is new | **Retired (#417).** `create_slot_and_park_markers` creates the slot, seeds `replication_progress`, and parks a marker on every table the catalog says to publish in the same transaction, after slot creation returns. It reads nothing, the same shape slot-loss recovery (`intake::slot_loss`) already has. Each marker is a go-live catch-up for the table's applying readers, for a catalog that outlived its slot (#420, [A fresh install](#a-fresh-install)). The discharge skips a table no definition reads (`defs::catalog::table_has_reader`) |
| Ring enumeration inside registration | was `defs::catalog::create_definition_inner`'s `enumerate_and_append`, reached through `create_definition` and `install_definition`'s `Unsupported` fallback | **Done (#418).** The discharge's ring fallback does it (`intake::publication::run_pending_backfills`, dispatch by shape). `create_definition` survives only as a test fixture that stands in for that discharge |
| Plain 1-1 chunk enqueue at registration | was `install_definition` → `install_plain_one_to_one` → `chunk_queue::enqueue_one_to_one` | **Done (#418).** The discharge plans the chunks and enqueues them in its own transaction (`chunk_queue::dispatch_one_to_one`); drain threads still execute them |
| Synchronous direct build inside registration | was `install_definition` → `backfill::backfill_definition` for aggregates and relationship-enriched 1-1 | **Done (#419).** The discharge dispatches it as one background job (`chunk_queue::dispatch_direct_build`) that a drain thread runs ([The direct-build job](#the-direct-build-job)) |
| Registration's defer branch | was `install_definition`'s `defer_if_fence_unsettled`, and `create_definition_inner`'s `backfill_marker_unsettled` check | **Done (#418, #419).** Registration always defers to the discharge, and the branch is gone |
| Publication-join discharge | `reconcile_publication` parks; `run_pending_backfills_until` fences and discharges | The one path. **Done (#431):** the discharge fences every marker the first time it sees it ([The join fence](#the-join-fence)). A join marker is a go-live catch-up for any definition already applying from the table (one an operator dropped from the publication), through `park_table_catch_ups` (#522, [A re-read table's readers](#a-re-read-tables-readers)) |
| Transform resume (after `PAUSE`, quarantine or slot loss) | `staging::quarantine::resume_transform` parks a marker; the discharge reads | The one path. Dispatch by shape reroutes the rebuild onto the chunked or direct builder like any other capture, ring only for `Unsupported`. **Done (#418, #419):** a plain 1-1 rebuild is chunked, an aggregate or relationship-enriched 1-1 rebuild is a direct-build job, and a chunk or job planned before the resume is told apart by the definition's `fuse_rearmed_at` it recorded (`backfill_chunks.fuse_rearmed_at`). The discharge still deletes the target rows no source row backs when it dispatches (#330), so a reader doesn't see them for the length of the rebuild; the direct build doesn't visit a group with no source rows. The go-live catch-up's discharge runs the same deletion again (#485), judged on its re-read's snapshot, and that one is exact for aggregates too (#436) |
| Explicit re-backfill | `Trellis::request_backfill` parks a marker through `park_table_catch_ups` | The one path. **Done (#522):** the marker is a go-live catch-up for every applying reader of the table, so each reports `catching_up` until the discharge has re-read the table and swept its target, and a to-side's settled projections are refreshed ([A re-read table's readers](#a-re-read-tables-readers)) |
| Test fixtures | `defs::create_definition`, `create_definition_without_backfill`, the unfenced `defs::backfill::backfill_definition` | Not a capture path: compiled only for tests and the benchmark (`cfg(test)`, `test-util`, `internals`) |
| Go-live catch-ups | `defs::catalog::complete_direct_backfill`, `intake::publication::go_live`, `park_target_catchup_if_read`, and `park_table_catch_ups` (`create_slot_and_park_markers`, `request_backfill`, a join marker), all through `park_catch_up` | The one path. A chunked or direct build's go-live catch-up stays, because starting a build after the fence doesn't cover the changes that drain while it runs. Its discharge always re-reads (`backfill_coverage` is retired, #468), deletes the target rows no source row backs (#485), and flips the definition `live` (`go_live_caught_up`, #476) |
| Direct-build coverage skip | was `backfill_coverage`, recorded by the direct-build job and read by the discharge's `coverage_covers` | **Retired (#468, #485).** It took a table whose row count and `xmin`s were unchanged since the build's fence for unchanged, which a row inserted and deleted during the build defeats. V48 drops the table |
| Column resume | `staging::quarantine::resume_column` → `recompute_column`, then a catch-up marker | **Not rerouted yet (#425).** A redefinition-side capture that reads one column's values in-call. The definition is `catching_up` until the marker discharges (#476) |
| `ALTER TRANSFORM` added columns | `defs::alter_transform` → `backfill::backfill_altered_columns`, then a catch-up marker ([ADR-0015](0015-transform-redefinition.md)) | **Not rerouted yet (#426).** A redefinition-side capture that reads the added columns' values in-call. The definition is `catching_up` until the marker discharges (#476) |
| Publication change on `DROP` | was `Trellis::reconcile_publication_after_drop`, run by whichever process applied the `DROP` | **Done (#427).** A `DROP` only removes catalog rows, and the staging worker's reconcile pass (`client::reconcile_source_tables`) shrinks the publication from the catalog. `ClientOptions::source_tables`, the startup copy that used to act as a permanent floor, is deleted. Supersedes [ADR-0014](0014-pause-and-drop-a-transform.md)'s "applied at drop time" |

## Open questions

- None. "What `live` promises" was settled on 2026-09-24 and implemented by
  #476, including how a catch-up on an already-`live` definition shows; see
  [What `live` promises](#what-live-promises).
