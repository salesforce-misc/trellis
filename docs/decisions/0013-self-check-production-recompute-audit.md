---
status: draft
date: 2026-10-17
deciders: Michael Ries
---

# `self_check`: A Production Recompute Audit

Trellis's one hard correctness promise is convergence to a from-scratch recompute
at any caught-up LSN: byte-identical for a 1-1 target, equal values for an
aggregate or JOIN target (see
[Comparison semantics](#comparison-semantics-exact-text-for-1-1-equal-values-for-aggregates-and-joins)
below). The failure mode that promise guards
against is a *silently stale target*: a wrong answer with no error raised and no
metric out of range, invisible without an independent recompute. `self_check` makes
that recompute a shipped, operator-callable capability instead of something only the
test suite can perform. It is also the only way an aggregate or JOIN target's drift
is found: those targets don't repair a change Trellis never saw
([ADR-0002](0002-async-data-flow.md#a-change-trellis-never-saw-stays-wrong-until-it-is-detected-and-repaired)),
so `self_check` detects it and the operator repairs it.

## Decisions

### `self_check` is a public method on `Trellis`, and it starts a background job

It audits one target at a time, is read-only, and returns at once with a job. A
whole-target comparison can take far longer than a public call may
([ADR-0008](0008-public-api-design.md#6-every-public-call-returns-within-30-seconds)),
so no call runs it: `self_check` registers a row in `self_check_jobs` and a
process that runs drain workers (`application_threads`, any process of the
fleet) walks the target a keyset page at a time on a task beside them, not on
a drain worker, since a page waits for the convergence the drain workers
produce. It saves its cursor, its count and the divergences so far in that
row after each page. The caller polls the job with
`self_check_job(id)`. A job is `queued`, `running`, then `done`, `failed` or
`cancelled`; a `done` job carries a `SelfCheckReport` for the whole target:
any divergences (cell, missing row, extra row, missing/extra column, and a
broken capture), the LSN checked through, the rows compared, and whether the
walk stopped short of the end of the keys (`truncated`). The report also
carries the keys the definition holds in quarantine and the drain failures
open on the instance, read when it is polled.

* **One job per target.** A second `self_check` while a target's job is
  `queued` or `running` returns that job, whatever mode and timeout it passed.
  A finished job stays until the next `self_check` of the target replaces it.
* **Every page is bounded and holds nothing.** A page runs under a deadline of
  its own (the comparison's budget plus the two convergence waits a
  `Standard` page can make), so the server stops a stuck statement and the job
  fails rather than hold a connection. It is read-only and takes no lock
  between statements, so it never holds up a drain.
* **A worker stops cleanly.** A worker told to shut down drops its page and
  marks its jobs `cancelled`; a job whose worker vanished is taken over by
  another worker after a TTL and resumes at its saved cursor; dropping the
  transform deletes the job's row, which ends the walk.
* **Refused at the start**: an unknown target.

Fleet-wide sweeps are a caller-side loop over `definitions()`, not a behaviour
of the primitive. There is no cursor-paged variant of the call: paging is how a
worker walks the target, not something the caller drives.

### The capture audit runs first

Capture is statement triggers
([ADR-0002](0002-async-data-flow.md#capture-by-statement-triggers)), so a target is only
as current as the capture feeding it, and a broken capture
is invisible to the convergence wait: convergence is a predicate over ring
rows, and a capture that has stopped writes none. So before it awaits or
compares anything, `self_check` reads from the catalog (`pg_trigger`,
`pg_proc`, `pg_class`, `pg_inherits` and the `has_*_privilege` functions) that every table
the target is computed from (its source, or both sides of a JOIN, less any
this instance's seam feeds) is still captured as
the staging worker installed it:

- all five capture triggers exist, are `ENABLE ALWAYS` (`tgenabled = 'A'`),
  and call their event's function;
- each function exists, is `SECURITY DEFINER`, and is owned by the role that
  owns the ring (the one Trellis role that runs the migrations and installs
  and owns capture). That is not necessarily the schema's owner: a DBA can
  pre-create the schema as another role;
- the role each function runs as still has `USAGE` on the schema, `INSERT` on
  every ring segment, and `USAGE` on the change-id sequence and the ring slot
  mirror;
- the table is still a plain table outside any partition or inheritance
  hierarchy. Acceptance refuses such a table, but nothing refuses a later
  `ATTACH PARTITION` or `INHERIT`, or the table dropped and recreated as one,
  and a statement trigger fires only for the table a statement names. The
  staging worker's reconcile pass runs the same check
  (`defs::hierarchy`) and pauses the table's readers. A partitioned root
  whose tree the pass has recorded is no such fault: the pass pauses the
  readers it doesn't comply for
  ([ADR-0017](0017-partitioned-sources.md)). Its five triggers are checked
  on each recorded node still in the tree, every one calling the root's
  functions, and a fault is reported against the node.

Any fault is reported as a `capture` divergence, and the report stops there:
no recompute comparison runs, because it would only show the symptom. The
audit applies once a definition's capture must be installed (`backfilling`,
`catching_up`, `live`). A fault is a catalog fact, not a race, so it isn't
re-checked. The staging worker's reconcile pass pauses the readers of a
plain table whose trigger is missing or disabled, and a `RESUME` reinstalls
it; the others persist until an operator fixes them.

### Only a `live` definition is compared

The convergence wait never reads a definition's status, and a repair that
rebuilds a definition (an explicit `request_backfill`, a resume, an `ALTER
TRANSFORM`, a capture re-install) does its work in background jobs the wait
knows nothing about. A comparison made while the definition is being built
would agree or disagree with a state the build is about to change. After the
capture audit, `self_check` therefore reads the definition's status and
compares only a `live` one. Any other (`waiting_to_backfill`, `backfilling`,
`catching_up`, `paused`, `quarantined`) returns the outcome `NotLive`, which
carries that status. It compares nothing and waits for nothing. Like
`NotCaughtUp` it is not a verdict on correctness, and the remedy is the same
kind: poll the status until it is `live`, then check again.

A rebuild is a status transition made in the repairing call's own
transaction ([ADR-0002](0002-async-data-flow.md#convergence-and-status)), so a
job started right after `request_backfill` returns is told `backfilling`
instead of comparing a target the rebuild is still changing. Each page reads
the status again: a job whose definition stops being `live` partway ends
there with what it had found.

### Postgres is the oracle; the comparison is two-way

`self_check` compares the persisted target against an equivalent recompute *query*
executed by Postgres. It does not run the engine's Rust evaluator as part of the
comparison. Re-running the
evaluator would check the engine against itself; the authority is Postgres computing
the answer from source rows independently. This also matches the failure class the
audit exists to catch: a stale target diverges as persisted-vs-recompute, where an
independent query is exactly the detector and the evaluator adds nothing.

### Comparison semantics: exact text for 1-1, equal values for aggregates and JOINs

A 1-1 target holds its source's exact text, and `self_check` compares a 1-1
persisted cell against its recompute **byte-for-byte**: the strongest, simplest
yardstick ("diff the text"), and what the primary-key role relies on.

An aggregate or JOIN target holds an **equal value**
([ADR-0020](0020-type-identity.md)): a group key and an aggregate result are
correct when they are equal, under the column type's `=`, to what a fresh
`GROUP BY` computes, and their text is one of the equal values' texts, as
Postgres's own `GROUP BY` gives. A `numeric` group written as `1.25` is the
source's `1.2500` after a rewrite re-renders it. So `self_check` matches an
aggregate's groups, and a JOIN's groups, cells and join keys, by the key
columns' types' equality (the page's dense rank, see
[A walk pages by the key's own columns](#a-walk-pages-by-the-keys-own-columns)), and compares a visible field whose
text differs by its type's `=`. A field of a type with no `=` is compared as
text; no group key is one, since `GROUP BY` needs `=`. The partials compare as
values: counts as integers, a sum without trailing zeros. Text identity is no
yardstick for these shapes: `GROUP BY` merges equal values with different
texts, so a fresh recompute doesn't give text identity either, and a rebuild
doesn't rewrite a key whose text alone changed.

A sum or average kept from partials carries exactly the scale Postgres's own
`sum()` and `avg()` show
([ADR-0002](0002-async-data-flow.md#numeric-arguments-declare-their-scale)). An
aggregate whose `=` admits several textually distinct representatives of one value
(`MIN`/`MAX` over `real` or `interval`) is refused at define.

### The audit query is rendered independently of the write path

`self_check` renders its recompute query — down to the leaf expression level —
separately from the rendering code the backfill and apply paths use to *write* target
rows. If the audit reused the write path's renderer, a rendering bug would agree with
itself: backfilled, never-modified rows would be re-derived by the same code that
produced them, and the audit would pass on a wrong answer. Independent rendering is
what makes "Postgres is the oracle" true rather than nominal. The leaf renderer is
small; duplicating it is the price of an honest audit.

### Quiescence: distinguish "diverged" from "not yet caught up"

The correctness promise is conditional on being caught up to an LSN, so `self_check`
must never report a merely-lagging target as diverged. It takes a watermark token,
awaits convergence through it (bounded by a timeout; on timeout it reports "not caught
up," never a divergence), and reads the target and runs the recompute under one
snapshot: one statement reads both sides, which gives it one snapshot under any
isolation level. This is done for each page of the job's walk: a job's pages are
read at different instants, each after its own wait, and a page that can't catch up
ends the job as "not caught up."

A snapshot alone is not sufficient under live load: a single snapshot pins source and
target at one instant, but a correctly-working target legitimately lags its source by
CDC apply latency. A commit after the token is in the source the statement reads,
and the target may not have applied it yet. Two comparisons handle that lag:

- **1-1, aggregate and JOIN targets compare against the source minus what the target
  hasn't applied.** Capture writes each change's ring row in the change's own
  transaction, so under one snapshot a source's rows are exactly what its target has
  applied plus the changes the ring still holds unapplied. The same statement reads
  those ring rows (the drain's own pending-row predicate, plus the changes a page
  parked for a key the definition holds) and evaluates their images with
  `self_check`'s own renderer: each current row counts `+1`, each unapplied change's
  old image `+1` and new image `-1`, and the partials and fields are sums of those
  weights. An image is left out where its page would refuse it: at or below its
  hidden table's truncate floor, or seen by the basis of the group or cell it lands
  in, whose absolute write already counted it. A JOIN's target partials are the
  definition's own `JOIN … GROUP BY` over both tables' weighted rows. A 1-1
  target's expected row for a key with unapplied changes is the old image of its
  earliest one (no row, for an insert), and the live source row for any other
  key. A change its page would refuse is no unapplied change there: at or below
  the target's truncate floor, at or below its key's applied `lsn` (a later batch
  of the key drained first), or seen by its key's basis. The comparison is exact for every key and group with no lag window, so a
  divergence is reported from one pass.
- **Otherwise a divergence is re-checked.** A divergence is reported as real only if it
  **survives a re-check after a fresh await** — a genuine divergence is stable; a
  convergence race resolves. This applies to a divergence on a key or group the
  subtraction can't vouch for: one an unapplied change with no usable image (a `Recompute`) may touch, or a held key
  whose parked origin is unknown. An unapplied op of the source that touches every
  key or group (a `TRUNCATE`, a partition's truncate, a schema change) makes the whole
  page re-check, as does any image-less unapplied change of either table for a JOIN's
  target part.

The subtraction has one trade-off: a change stuck in the ring indefinitely is
subtracted rather than reported by `self_check`. Such a change is reported where it
is stuck instead: a page that keeps failing is a `drain_failure` and a key the
definition holds is a held key, both on every `self_check` report and in `status()`.

A strict mode, sound only when writes to the audited tables are stopped, never
re-checks; it is the documented strong guarantee for callers that can quiesce.

### A walk pages by the key's own columns

A page selects its keys by a row comparison on the raw key columns,
`(k1, …, kn) > ($1, …, $n) order by k1, …, kn limit L`, on the source side and
on the target side. An index that leads with those columns serves the page with
a range scan, so a walk reads each table about once and its cost is linear in
the rows. Paging on the key's encoded text instead, an expression no index
orders, reads the whole source on every page: a walk of N keys then reads N / L
times the table, which is hours at 100M rows.

* **The key per target kind.** A 1-1 target pages by the source's primary key,
  which always has its index. A group-basis aggregate pages by its `GROUP BY`
  columns, a JOIN's target part by its group columns, `P` and `M` by the
  hidden table's cell identity (the side's join key columns, then its group
  columns not among them) and `J` by `b`'s join key. The aggregate and `J`
  are index-served only when their table has an index leading with those
  columns; without one a page scans the table, so a walk reads G / L times it.
  `P` and `M` lead with the join key because that is the order of the hidden
  table's one index: its page is a range scan of that index, so the hidden side
  costs nothing extra, and an index on the source's join columns (`a`'s for
  `P`, `b`'s for `M`) serves the source side, with the group columns sorted
  within each join key. No second hidden index exists for the walk, so the
  apply path's write cost is unchanged. Define requires that source index (next
  bullet). A JOIN's target part reads the join of both
  tables, which no index orders, so each of its pages scans them.
* **The source index define requires.** A group-basis aggregate needs, on its
  source, a btree index whose leading columns are its `GROUP BY` columns (in
  `GROUP BY` order), and a JOIN needs on each side one leading with that side's `ON`
  columns in `ON` order, which is the order `P` and `M` page by. Define and a
  `RESUME` refuse a definition without it, naming a `create index`, unless it is
  defined `WITHOUT SOURCE INDEX`; a later drop warns in `status` and makes
  `self_check` refuse, and pauses nothing, since it affects speed, never
  correctness. A JOIN's target part needs no index.
* **Order.** Each column ascending, `NULL` last (what a btree holds, and the
  order `NULLS NOT DISTINCT` identities need), each collatable column under
  one collation named in the statement and used on both sides: the key
  column's own when it is deterministic, `"C"` when it is not. The column is the
  target's for a 1-1 target, an aggregate and a JOIN's target part, and the
  hidden table's for `P`, `M` and `J`. Each keeps the collation define gave it
  (a 1-1 target key gets the source key's) when the source is re-collated, so
  a walk that straddles the change pages in one order throughout; a 1-1 walk
  still casts its cursor by the source key's columns and types. When the two
  sides' columns are collated differently, a page pays a scan on the side whose
  index doesn't order it.
* **Nullable keys.** `NULL` sorts last, so the rows past a cursor on a nullable
  column are no single range (`k > c` leaves out the `NULL` rows after it, and
  `k > c or k is null` is no index range). A page reads each way a row can
  first differ from the cursor as a branch of its own, each a range of an index
  on the key, and cuts the union again. A key none of whose columns can be
  `NULL` is one row comparison.
* **Planning.** The planner can't estimate how many groups the weighted union
  of the source and its ring rows holds, and costs an index-ordered aggregate
  by a default 200, so it hashes the whole source for a page of groups. When
  the source has an index leading with the key, the page runs without hash
  aggregation and without the bound to the hidden side's last key, which an
  ordered aggregate would read past to the end of the index.
* **Identity.** The encoded key text is the key a divergence is reported
  by and the text a walk saves as its cursor; it neither orders nor bounds a
  page. A cursor is decoded back into its columns' values for the next page.
  An aggregate's and a JOIN's sides are matched by the dense rank below, so
  keys that are equal values are one key, reported by the text of the first
  side that has it (the source's, when it does), and so is the key of an
  unapplied ring row the page can't vouch for.
* **The page's end.** The two sides are `limit`ed separately, so a page ends at
  the lowest last key of the sides that filled the limit. The sides' keys are
  ranked together in the statement, by a dense rank over the same order, so
  "past the end" is an integer comparison whatever the key's type.
* **One statement.** A page is one statement under one snapshot, and the
  ring-weighted comparison runs inside it.

The cost of a walk is then about (rows read per page) × (pages). The module docs
of `self_check`, `self_check_groups` and `self_check_joins` say what each part
needs indexed.

**Cost.** A page of an aggregate or JOIN target reads its page of keys plus the
undrained ring segments. No index serves the ring slot tables by source, so a page
reads a slot whole, but a slot whose segment is drained is skipped: the statement
carries a condition over no row for each slot, evaluated once, that is false when
every row of the slot is applied, and the slot's scan is not executed. A definition
that holds keys (`poison_held`) reads every slot, as the keys' parked rows may sit
in a drained one.

Drained is not enough on its own. A writer in flight at a segment's fence can leave
a late row in the segment's slot, which the successor's window takes
([03](../staging-and-claiming/03-sealing-and-the-fence.md)), so the row stays
pending after the segment drains. A drained slot is skipped when one of these
holds:

* its fence had no transaction in flight;
* the retire pass counted no late row in it (`segments.late_rows`, counted once,
  after every transaction in the fence has finished);
* its successor is drained too, or gone.

The decision and the pending predicate are read in the page's one statement, so a
segment that finishes draining mid-statement is read or skipped on one snapshot.
The page's ring reads are then bounded by the segment size, not by the rows a ring
has applied and not yet retired. The convergence wait before each page asks the
slot itself ([07](../staging-and-claiming/07-convergence-and-await.md), condition
3), so it still reads a drained slot whole.

### Scope: every target; aggregate and JOIN targets detect, never repair

`self_check` audits 1-1, aggregate and JOIN targets. Every comparison projects the
target's key (a 1-1 target's, which is its source's, an aggregate's or JOIN's `GROUP BY`
columns), so a divergence is reported per key.

- **A 1-1 target** is compared row by row against its source, as the target has applied
  it (see [Quiescence](#quiescence-distinguish-diverged-from-not-yet-caught-up)).
- **An aggregate target** is compared a page of groups at a time: each group's
  hidden partials (its row count, non-null counts and `numeric` sums) against a
  `GROUP BY` of the source over the same groups, and its visible row against the
  partials (a row exactly while the row count is above 0, with each field the
  function of the partials the definition names). A group the source has and the
  target lacks, or the reverse, is a missing or extra row.
- **A JOIN target** is compared a page of groups at a time against the `JOIN …
  GROUP BY` query, and its internal structures are audited against their
  definitions: each side's per-(group, join key) partials against a `GROUP BY` of
  that side, each join key's match count against the joined side's row count for
  it, and the target's partials against the query's, which are the sum, over join
  keys, of the products of the two sides' partials
  ([ADR-0019](0019-join-transforms.md)). Each structure is compared with the two
  tables, not with another structure, so a divergence names the structure that is
  wrong and the key, and an operator can tell a wrong side from a wrong
  combination.

For aggregate and JOIN targets `self_check` **detects and reports; it never
repairs.** A target found wrong is repaired by the operator, with the tools any
definition has: `request_backfill` of the source table, which rebuilds each reading
definition without pausing it, or `PAUSE` and `RESUME`, or `DROP TRANSFORM` and
define it again. A hand edit of a JOIN's hidden tables needs one of the last two
([known correctness gaps](../known-correctness-gaps.md#rows-written-into-a-target-a-hidden-table-or-trelliss-schema)).
There is no background
repair sweep. A sweep would put a continuous source `GROUP BY` load on the
database, hold group locks that compete with pages, and silently mask Trellis's own
bugs, which an audit exists to surface. A repair of only the groups an audit
names is additive and can come later.

An aggregate or JOIN page compares the partials under the same convergence rules
as every page (see [Quiescence](#quiescence-distinguish-diverged-from-not-yet-caught-up)).
Pages apply out of order, so between them a group's partials can be inconsistent;
the comparison is meaningful only once the page's convergence wait has returned,
and it reads the source minus the ring rows the target hasn't applied yet.

### Respect column-level quarantine

`self_check` excludes paused (quarantined) columns from the comparison. A paused
column holds a deliberately stale value; auditing it would report a false divergence
on exactly the targets an operator is most likely to be inspecting.

### Independence from the generative oracle, verified continuously

`self_check`'s renderer and the generative suite's SQL oracle are independently
authored; neither imports the other, and neither shares `SELECT`-assembly logic with
the engine's evaluator. The generative suite runs both renderers over the same
definitions and asserts they agree, on every run. This keeps the two implementations
from drifting and turns the deliberate duplication into a live guarantee rather than a
standing cost.

## Consequences

- `self_check` is a real read load: a full recompute reads source and target
  once when a key's index orders each page, and once per page where none does
  (an aggregate or JOIN part over unindexed group or join columns). It runs
  page by page beside the drain workers, each page bounded and holding no
  lock, and its caller never waits on it.
- It needs a drain worker somewhere in the fleet, as a build does; with none, a
  job stays `queued`.
- Aggregate and JOIN targets depend on it: a change Trellis never saw leaves them
  wrong until an audit finds it and an operator repairs it, and that repair
  rebuilds the whole definition.
- Trellis gains a shipped, public answer to "is this target correct right now,"
  usable by operators, by a thin CLI wrapper, and as a shared end-of-test
  assertion that turns integration tests into correctness tests.
- The engine's evaluator stays internal to the engine and the test suite; the
  public audit path is the independently-rendered query in `self_check`.
