# Operations: stops and repairs

What to do when Trellis stops a definition, and how each repair behaves.
Trellis stops rather than guess: it pauses a definition it can't keep
correct, holds a key it can't apply, and holds back a page it can't commit,
and it says so on `status()`. A stopped target is behind, not wrong.
[Observability](observability.md#transform-status-lifecycle) defines each
status and field. [Known correctness gaps](known-correctness-gaps.md) lists
the ways a target can go wrong without a stop, and names the repair for each.

## When a definition needs you

| You see | It means | Do |
|---|---|---|
| `paused`, with a `capture_failure` saying capture was changed outside Trellis, or the table is gone from its name | A trigger was disabled, dropped or recreated, a capture function was replaced, or the table was dropped, renamed away or swapped, so changes may be missing | Put the table back if it was a mistake, then `RESUME` |
| `paused`, with a `capture_failure` naming a column or the primary key | A schema change the definition can't follow as written, or copies Trellis must re-type | Make the schema acceptable (the message says how), then `RESUME`; or `DROP TRANSFORM` and define it again |
| `paused`, with a `capture_failure` naming a role and a table | A read or write refused to a role Trellis runs as: a missing privilege or row-level security ([A halting failure](observability.md#a-halting-failure)) | Grant the privilege, or `BYPASSRLS`, then `RESUME` |
| `paused`, with a `capture_failure` naming a partition-key column or the leaf limit | A partitioned source stopped meeting the shape define accepts | Make the tree comply ([the refusal table](transforms.md#supported-sources-and-targets)), then `RESUME` |
| `paused`, with a `capture_failure` saying an enum value was renamed | `ALTER TYPE … RENAME VALUE` on an enum it reads: the changes it staged and the keys it holds name the old label | `RESUME`, which rebuilds the target from the source ([capture by triggers](staging-and-claiming/01-capture-by-triggers.md#an-enum-value-relabelled)) |
| `paused`, with a `capture_failure` saying the database was restored from a logical dump | Every definition's ordering state came from the original database | `RESUME` each definition, which rebuilds it ([After a restore or an upgrade](#runbooks)) |
| `paused`, with a `capture_failure` reading "resuming: …" | A `RESUME` is re-typing copies, waiting for an upstream to go `live`, or waiting for the changes staged before an enum relabel, or a re-type to another enum, to drain | Wait. A `PAUSE` withdraws the resume |
| `quarantined` | Too many of its keys failed to apply, so the whole-definition fuse tripped | Fix the cause `sample_quarantined` shows, then `RESUME` |
| `held_keys` | Keys that keep failing to apply are held, with their target rows frozen | Fix the cause, then [release them](#releasing-held-keys) |
| `drain_failure` | A page that fails in a way no key can be charged for is held back, and every watermark taken since waits on it | Fix what the error names; the next pass commits the page ([A page that fails only for several definitions together](#a-page-that-fails-only-for-several-definitions-together)) |
| `backfill_failure` | A build, catch-up or correction keeps failing | Fix what the error names; it retries |
| `capture_wait` | Installing or widening capture is waiting for a table lock | Wait for the lock holder it names |
| A `self_check` report with divergences | The target differs from a recompute of it | Find the gap in [known correctness gaps](known-correctness-gaps.md) and use its repair |

## Repair tools

`PAUSE`, `RESUME` and `DROP` are statements: run them through
`Trellis::apply` or `trellis apply '<statement>'`.

### `request_backfill`

`request_backfill(source_table)` (Rust, Ruby and Elixir) queues a re-read of
one source table for every definition that reads it, without pausing them. A
reader the Re-derive build serves reports `backfilling` and is rebuilt in
place; any other reports `catching_up`. Both return to `live` when the re-read
is done. It's the cheapest repair when you know which table missed changes.

It takes the table's `schema.table` name, or a bare name when exactly one
captured table carries it; a bare name two captured tables share is refused,
naming both. It repairs what a rebuild repairs, with the same exceptions
(below).

### `PAUSE` and `RESUME`

`PAUSE TRANSFORM <target>` freezes a definition, holding its last values
([ADR-0014](decisions/0014-pause-and-drop-a-transform.md)).
`RESUME TRANSFORM <target>` rebuilds it from its sources.

**What a resume checks.** A resume first runs define's validation against the
live schema, and refuses while define would, leaving the definition paused;
the error names the column and what to change. A whole-transform resume also
refuses, as define does, while a source index the definition's `self_check`
pages by is missing, unless it was defined `WITHOUT SOURCE INDEX`; create the
index the message names, then resume. It also refuses in five cases
define doesn't, where the only repair is to `DROP TRANSFORM` and define again:

* a 1-1 definition's source key was redefined;
* its target table is missing;
* its ledger table is missing;
* an aggregate's group-delta table is missing;
* the group-delta table has other running-sum or recompute columns than
  define would create now (a `SUM` or `AVG` argument moved between an exact
  type and floating point).

A `JOIN` has no ledger or group-delta table. Its resume drops and recreates
its hidden tables instead, keeps the target, and rebuilds both sides and the
target's groups from the sources, in one transaction, or per side and leaf
when a side is a partitioned table with rows. Its build rebuilds the target's
readers when it commits, so they need no pause. The resume of an aggregate on
the group basis, which has neither table either, drops and recreates its
hidden partials table and keeps the visible target, which its rebuild
rewrites; its build rebuilds the target's readers likewise. A field resume
(`RESUME TRANSFORM <target>.<column>`) runs define's validation and the
source-key check, and refuses a field whose capture is still being widened.

**What a resume changes.**

* It brings every column Trellis created with a type from the source back to
  the type define would give it now: key, passthrough and `GROUP BY` copies,
  calculated and aggregate columns, ledger contributions and relationship
  projections. The `RESUME` returns at once; the definition stays `paused`,
  its `capture_failure` reading "resuming: …", until the staging worker has
  re-typed them and started the rebuild. A re-type that fails leaves its
  error on `capture_failure` and the target's rows in place. When values
  can't be converted (a key moved from `text` to `uuid` by a `USING` that
  isn't a cast), the repair is to `DROP TRANSFORM` and define it again:
  Trellis doesn't empty a target the application reads on its own.
* A definition that reads another definition's target while that one isn't
  `live` waits for it: the `RESUME` returns at once, and the definition stays
  `paused`, its `capture_failure` reading "resuming: waiting for transform …
  to be live", until the upstream is.
* A definition paused for an enum relabel waits for the changes staged
  before the relabel to drain: the `RESUME` returns at once, and the
  definition stays `paused`, its `capture_failure` reading "resuming:
  waiting for the changes staged before an enum value … was renamed to
  drain", until none is pending. A target keyed by a source key with an
  enum column is then emptied and rebuilt as define builds it.
* It re-reads every current source row and rewrites the target from it. It
  doesn't replay the changes it skipped while paused, so its cost scales with
  the data, not with the length of the pause.
* It releases every key the definition holds in quarantine, and only that
  definition's. A key whose cause is still there fails again and is held
  again.

**What a rebuild doesn't repair.** A rebuild repairs anything that's wrong
because a change was missed. On a 1-1 target it also deletes every row no
source row backs, so it repairs rows edited, deleted, added or re-keyed by
hand, and rows left under the old key of a `varchar` key column narrowed
in place. It keeps:

* an aggregate's group rows edited by hand, and a `MIN` or `MAX` over a
  re-collated column: the rebuild keeps the per-row ledger, with its
  collations, and adds only differences to the groups.
* a hand edit of a `JOIN`'s hidden tables, which a `PAUSE` and `RESUME`
  recreates.

`PAUSE`, `DROP TRANSFORM` and define it again repairs each
([known correctness gaps](known-correctness-gaps.md): [Rows written into a target, a hidden table or Trellis's schema](known-correctness-gaps.md#rows-written-into-a-target-a-hidden-table-or-trelliss-schema),
[A column's values rewritten in place by `ALTER COLUMN TYPE`](known-correctness-gaps.md#a-columns-values-rewritten-in-place-by-alter-column-type)).

### `DROP TRANSFORM` and define again

`DROP TRANSFORM <target>` removes a paused or quarantined definition with
its target, its side tables and its held keys
([ADR-0014](decisions/0014-pause-and-drop-a-transform.md#drop-always-removes-the-associated-data)).
It refuses a definition in any other status, and one another definition
still reads. It first waits for any drain page still writing the definition
that was planned before the pause. Defining it again builds the target from
empty. Indexes and
grants on the target go with it, so give readers `SELECT` through default
privileges
([recommendations](recommendations.md#application-roles-read-targets-and-nothing-else)).
It's the repair when a resume refuses, when copies can't be re-typed, and
when a rebuild keeps something wrong.

### Releasing held keys

A key that keeps failing to apply is held for the definition whose apply
failed, with its parked changes and the error that poisoned it
([ADR-0003](decisions/0003-quarantine-storage-and-api.md#releasing-held-keys)).
That definition's target row for the key stays as it was, and a held key with
changes parked holds back every watermark token taken since. Every other
definition reading the same source keeps applying the key, so two definitions
can disagree on it. A key that fails only when several definitions apply it
together is held for each definition in every failing combination, though
only one of their writes may be at fault.

The definition's `held_keys` reports how many it holds and since when,
whatever its status, `live` included, and `self_check` reports them with every
audit. What clears a held key:

* `Trellis::release_key(transform, source_table, key)` (`trellis release` on
  the CLI, `release_key` in the bindings), once its cause is fixed: every
  definition reading it re-derives it from its current row.
* `RESUME TRANSFORM` releases every key the definition holds.
* `DROP TRANSFORM` deletes them with the definition.
* The automatic retry, off by default (`TrellisOptions::held_key_retry`),
  releases due keys with a growing backoff
  ([ADR-0003 — Retry policy](decisions/0003-quarantine-storage-and-api.md#retry-policy)).
* A build of the target the definition reads (an automatic rebuild,
  `request_backfill`, a capture re-install or a `RESUME` of that upstream)
  retries every key the definition holds once it completes, so a key held
  for a failure the upstream caused while it was being rebuilt clears by
  itself. A key whose failure persists is held again after one attempt, and
  a paused definition keeps its keys.

After a type a definition reads is renamed or moved to another schema, the
capture pass records its new name and then releases the keys held for a cast
by the old one (`42704`), in that pass and each pass for a minute more. A key
held later than that stays held.

### A page that fails only for several definitions together

A trigger or constraint you add can refuse a change only when several
definitions' targets are written in one transaction. The drain isolates which
definitions a failing change belongs to by applying it without each one in
turn. A single failing pair is charged to both of its definitions, which hold
the key. When the change fails in combinations no one definition is in every
one of (two separate pairs, `a` with `b` and `c` with `d`, or any two of
three), leaving any one out still leaves a failing combination, so it's
charged to nobody. The page is a drain holdup: the drain fails on it every
pass, every target reading the page's tables stops short of it, along with
every watermark token taken since, and `drain_failure` and `self_check`
report it ([ADR-0003](decisions/0003-quarantine-storage-and-api.md#retry-policy)).

Fix the trigger or constraint, and the next pass commits the page. Or pause
one definition from each failing combination: once some definition is in
every combination left, isolation charges the ones it pins, and they hold the
key until it's released or they're resumed.

### `self_check`

`self_check` finds a target that differs from its definition, and repairs
nothing. It's a background job
([embedding — `self_check` is a background job](embedding.md#self_check-is-a-background-job));
which targets it compares is in
[known correctness gaps — What `self_check` can't see](known-correctness-gaps.md#what-self_check-cant-see).

## What a repair costs

* **A resume that re-types copies holds `ACCESS EXCLUSIVE` on them.** After a
  widening, the resume's `ALTER … TYPE` on the target (and its ledger, or a
  relationship's projection) waits for every reader of the table, and readers
  queue behind it.
* **A re-type changes the table's row type**, so a client that prepared a
  statement returning one of its columns gets `cached plan must not change
  result type` the next time it runs it, once per prepared statement.
* **`integer` to `bigint` rewrites the table**, so at a billion rows reads of
  the target block for minutes, and the capture pass waits with it. Trellis
  sets no `statement_timeout` and honours the one on its role or database: a
  rewrite that outlasts it is cancelled, rolled back and retried on the next
  pass, and the third cancellation ends the request. The definition stays
  `paused`, its `capture_failure` naming both remedies: raise the timeout for
  Trellis's role or database, restart Trellis (the setting reaches only new
  connections) and `RESUME` again, or `DROP TRANSFORM` and define it again
  ([recommendations — `statement_timeout`](recommendations.md#statement_timeout)).
* **A resume that re-types a target pauses the definitions chained off it,
  and they rebuild one after another.** A definition reading the target whose
  own columns were typed from the old type pauses, its `capture_failure`
  naming the upstream resume, and the capture pass resumes it once the
  upstream is `live` again. Its resume re-types its own target, which pauses
  the next definition down, so a chain of depth k stays paused below the head
  for k full rebuilds in a row. A definition resumed while an upstream isn't
  `live` waits the same way, so a stack resumed at once rebuilds one level at
  a time. A `PAUSE` of a waiting definition withdraws its resume. One whose
  resume is refused once its upstream is live, because the schema changed
  after the pause, stays paused with the refusal and isn't tried again:
  resume it by hand once that's fixed, or drop it and define it again.

## Partitioned sources

[Transforms — Partitioned sources](transforms.md#partitioned-sources) says what a
partition operation does to the counts and how to avoid the window. This is what
each one costs and what to expect around it
([ADR-0017](decisions/0017-partitioned-sources.md)).

* **A correction costs the work of the partition's rows.** On the ledger build,
  the correction of a dropped partition of 1,000,000 rows took 13.0 s in the
  engine (1,000,000 keys in 101 pages, 489 MB of WAL), plus 0.35 s and 23 MB to
  fold the result and a 0.26 s pass to detect the drop (release build,
  PostgreSQL 17.11, a 1,000-leaf tree on disk); only an aggregate that reads
  a relationship path is still on the ledger. On the group basis, which
  keeps every other aggregate, the correction of a `DROP`, `DETACH` or
  `TRUNCATE` took
  27-34 ms and about 0.6 MB of WAL for a partition of 1M or 10M rows over 1,560
  groups (a debug build), and the correction of an `ATTACH` took 0.5-0.6 s at 1M
  rows and 3.9-4.7 s at 10M. Moving rows with DML through the parent instead
  costs about 4-5 s and 0.5-0.6 GB of WAL per 1M-row partition and has no
  window.
* **A retention `DROP` of a large partition takes the definition out of `live`
  for minutes.** On the ledger build, a linear extrapolation of the 1M-row
  figure gives about 22 minutes and 49 GB of WAL for 100M rows, a floor that
  assumes the ledger stays in cache, followed by the garbage collector's deletes
  of the tombstones. A model of the walk once the ledger outgrows the cache gave
  78-95 minutes and 0.9-1.2 TB of WAL for 100M rows. The definition reads
  `backfilling` with the partition in `correcting` until it drains, and every
  other partition's groups stay exact. Delete the partition's rows through the
  parent first to avoid the window.
* **A `DROP` or `DETACH` of a partition can deadlock against a rebuild or a
  page, and Postgres may abort your DDL with `40P01`.** The DDL takes the
  parent `ACCESS EXCLUSIVE`, then waits for the partition. A per-partition
  rebuild of a group-basis aggregate or a `JOIN` (a build, `RESUME` or rebuild
  in place), or a group-basis correction of another change to the partition,
  holds the partition while it waits for its groups, and a drain page
  holding those groups can wait for the parent, so the three wait on each
  other. Postgres breaks the cycle by aborting one of them. The job and
  the page retry. If the DDL is the one aborted, you get `deadlock detected`
  (`40P01`): run the DDL again. Nothing is lost. A `TRUNCATE` of the partition
  doesn't lock the parent and takes no part in this.
* **Installing capture on a large tree stalls writers briefly, once.** A tree
  of up to 100 nodes installs in one transaction, which blocks writers for up to
  about 50 ms (34-44 ms at 100 leaves). A larger tree installs node by node: the
  slowest application-writer commit at 1,000 leaves was typically 12-65 ms, with
  occasional spikes of 0.2-1.5 s, and the install took 1.6-4.6 s in all, against
  0.2-0.7 s in one transaction.
* **A pass over a 1,000-leaf tree takes about a quarter of a second.** The
  fingerprints take 2.6 ms and a steady pass 233-252 ms. A build statement holds
  about 2,000 locks and plans in 25-28 ms, within the default lock table.
* **Some PostgreSQL minor releases refuse to `ATTACH` a table that still
  carries the capture triggers.** 16.8 refuses (`trigger
  "trellis_capture_delete" prevents table … from becoming a partition`), while
  15.19, 16.15, 17.11 and 18.6 attach it. On a server that refuses, drop the
  table's `trellis_capture_*` triggers before attaching it. The next pass
  installs capture on it.
* **A logical dump and restore** pauses every definition of the database, with
  a `capture_failure` naming the restore, discards the changes staged before it
  and the corrections queued, and runs no correction from the recorded
  partition state: the dump's partition oids and `HASH` bounds are no longer
  the live ones. `RESUME` each definition, which rebuilds it from the restored
  source ([After a restore or an upgrade](#runbooks)).
* **`pg_upgrade` needs nothing.** The pass sees the new cluster, compares the
  recorded partitions by oid and bound, and records the upgrade's reset
  `pg_inherits` and `pg_class` `xmin`s and its rewritten bound texts without
  reading any partition as new, so no rebuild or correction runs. Let a pass run
  after partition maintenance before an upgrade, and after the upgrade before
  more maintenance
  ([known correctness gaps](known-correctness-gaps.md)).
* **pg_partman's maintenance needs no operator step on Trellis's side.** The
  opt-in test tier (`trellis/tests/pg_partman.rs`, [testing
  strategy](testing-strategy.md)) drove pg_partman 5.2.4 on PostgreSQL 15.19,
  16.15, 17.11 and 18.6 through six modes (time daily, time without a
  `DEFAULT`, id range, id list, epoch seconds, and time with id
  sub-partitions), with premake, `run_maintenance`, `partition_data_*`,
  retention by detach and by drop, `undo_partition`, its background worker
  running beside Trellis's pass, and concurrent writers through the parent and
  straight to a partition. The readers matched the source after every step and
  none paused. Rows left in pg_partman's `DEFAULT` within its premake horizon
  make pg_partman's own `run_maintenance` fail on `ATTACH`, and its worker fail
  on every interval, until `partition_data_*` moves them, with or without
  Trellis. Other partition managers and pg_partman versions are not tested.

## Runbooks

* **After a table swap, a bulk load with triggers disabled, or a fixture load
  that ends with `ENABLE TRIGGER ALL`:** the readers of the table are paused.
  `RESUME` each one, which rebuilds it
  ([embedding](embedding.md#what-the-staging-worker-needs-from-the-database)).
* **After a restore or an upgrade:**
  * a `pg_upgrade` needs nothing, and a whole-database physical backup or
    PITR needs only a `request_backfill` of each unlogged source
    ([known correctness gaps](known-correctness-gaps.md#an-unlogged-source-emptied-by-a-crash-or-failover));
  * a logical dump (`pg_dump`, `pg_restore`), into another cluster, another
    database or over the same one, pauses every transform of every kind on
    the capture pass's first look, with a `capture_failure` of kind
    `disruption` that says the database was restored from a logical dump, and
    discards the changes staged before it. Nothing rebuilds by itself: `RESUME`
    each transform. The resume resets the ordering state the dump brought
    (the ledger's positions, a group basis's snapshots, truncate floors) and
    rebuilds the target from the restored source, so an update, a delete or a
    group move of a key that existed before the dump applies afterwards. A
    restore into the same cluster costs the same resumes, though its counters
    only move forward;
  * the sources restored alone pause their readers: `RESUME` them;
  * Trellis's schema or the targets restored without the rest need the
    affected transforms paused, dropped and defined again, or the whole
    database restored from one backup.
* **After a crash or a failover,** `request_backfill` each unlogged source
  ([known correctness gaps](known-correctness-gaps.md#an-unlogged-source-emptied-by-a-crash-or-failover)).
* **After an `ALTER COLUMN … TYPE … USING` that changes a read column's
  values,** `request_backfill` the table
  ([known correctness gaps](known-correctness-gaps.md#a-columns-values-rewritten-in-place-by-alter-column-type)).
* **After granting a missing privilege or `BYPASSRLS`:** `RESUME` the paused
  definitions. A drain holdup on a refusal commits on the next pass, which
  clears its `drain_failure`, and a catch-up backing off on one succeeds on its
  next attempt.
