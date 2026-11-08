# Recommendations

How to run Trellis well. An incrementally maintained view pays a constant
write load so that reads are cheap; these recommendations configure PostgreSQL
and its roles to pay that load efficiently and safely. They change nothing in
Trellis itself.

## WAL and checkpoints

A build writes a lot of WAL, and so does steady load on a large target. Three
settings decide how much: `wal_compression`, `max_wal_size` and
`checkpoint_timeout`.

### Why checkpoints matter

The first change to a page after a checkpoint logs the whole page (a full-page
image); later changes before the next checkpoint log only the change. The
more often checkpoints run, the more of the WAL is full-page images.

The share depends on the target's shape. A build that inserts into an index
at random positions logs a full page for almost every insert once the index
outgrows what a checkpoint interval leaves clean: a per-row ledger in a benchmark
with a `GROUP BY` index wrote 25 million full-page images over 100M rows,
against 0.18 million for a ledger without the index.

The benchmark runs set `checkpoint_timeout=1min` and `max_wal_size=4GB`
to stress a build, giving a requested checkpoint about every 10 seconds. There,
a 100M-row, 1M-group build with the index wrote 3,370 B of WAL per built row,
2.6 times the 1,290 B of a 10M-row, 100k-group build.

### `wal_compression`

Set `wal_compression = lz4` where the server is built with LZ4 support, and
`on` otherwise. It compresses full-page images and needs a reload, not a
restart.

On the 100M-row build above, `lz4` alone cut WAL from 3,370 to 1,918 B per
built row and define-to-`live` from 1,446 to 1,090 seconds, and the writers
rose from 1,269 to 1,751 writes per second. The gain is largest for a
build that inserts into such an index; the same build without the index wrote
1,036 B per row and reached `live` in 396 seconds.

### `max_wal_size` and `checkpoint_timeout`

Size them so the clock starts the checkpoints, and the interval is minutes,
not seconds.

* `checkpoint_timeout`: raise it from the default 5 minutes to 15 or 30.
* `max_wal_size`: PostgreSQL forces a checkpoint at about `max_wal_size`
  divided by (1 + `checkpoint_completion_target`), roughly half. Set it to
  about twice the WAL written in one `checkpoint_timeout`. In those runs a
  checkpoint covered about 2.2 GB at `max_wal_size=4GB`.
* Take the WAL rate from your own database: read `pg_stat_wal.wal_bytes`
  before and after a build, or at two points of steady load, and divide by the
  seconds between. A 100M-row invertible aggregate wrote about 260 MB per
  second, so a 15-minute interval writes about 235 GB and wants a
  `max_wal_size` near 450 GB.
* Size `max_wal_size` for steady load, not for builds. If the disk under
  `pg_wal` can't hold more, let a rare large build force a checkpoint every
  few minutes; `wal_compression` offsets the extra full-page images.
* `pg_wal` holds about `max_wal_size` plus one checkpoint's worth of WAL (the
  benchmark runs peaked at 4,192 and 4,576 MiB at a 4,096 MiB setting). It grows
  without bound while WAL archiving fails or another consumer's replication
  slot lags (Trellis has none), so alert on both.

A longer interval makes crash recovery replay more WAL, a trade worth making
for a database that builds large targets.

### How to tell it is working

* In `pg_stat_wal`, `wal_fpi` divided by `wal_records` falls, and so does
  `wal_bytes` per row built.
* Timed checkpoints outnumber requested ones during a build. The counters are
  `num_timed` and `num_requested` in `pg_stat_checkpointer` from PostgreSQL
  17, and `checkpoints_timed` and `checkpoints_req` in `pg_stat_bgwriter`
  before it. `log_checkpoints = on` logs why each one started.

To watch a build's progress, see
[observability — Transform status lifecycle](observability.md#transform-status-lifecycle).

## Ring sizing

Captured changes wait in the staging ring, which lives in your application's
database, until the drain applies them. The ring grows whenever capture
outruns the drain, which it can by about 20 times, and whenever
no Trellis process is running. Each undrained change takes about 188 B for a
narrow row;
a ring row carries the primary key and the columns definitions read, so wider
rows take more, and an update carries both its old and new image.

Size the free disk for the longest Trellis outage you want to ride out:

```
ring bytes ≈ 188 B × captured changes per second × outage seconds
```

At 10,000 changes per second, a one-hour outage needs about 6.8 GB.

* Take the change rate from your own database: sum `n_tup_ins`, `n_tup_upd`
  and `n_tup_del` in `pg_stat_user_tables` over the tables Trellis captures
  (each definition's source, and each table a `JOIN` reads) at two
  points of steady load, and divide by the seconds between. It errs high:
  capture skips an update that changes no column a definition reads. Leave
  out Trellis's own targets: Trellis stages a target's changes for the
  transforms that read it as it writes them, so those rows add to the ring
  only while Trellis runs.
* While every Trellis process is down, nothing drains or bounds the ring, and
  the database's own disk alerts are the only guard. Alert on free disk.
* To see the ring's size on disk, read the `ring_bytes` gauge
  ([observability](observability.md#metrics)), or sum
  `pg_total_relation_size` over the ring tables `seg_0` to `seg_3` in
  Trellis's schema. `ring_undrained_bytes` splits what is still to drain by
  source table, and `ring_oldest_undrained_age_seconds` says how far behind
  the drain is.
* The ring's soft and hard bounds are designed but not built
  ([ADR-0002 — Consequences](decisions/0002-async-data-flow.md#consequences-and-costs)).

## `statement_timeout`

Trellis never lifts the `statement_timeout` on its role or database: it
applies to every statement Trellis runs, the staging and drain workers'
included. Leave it unset on the Trellis role unless you want it to bound
them, and if you set one, make it longer than a resume's re-type of a column
takes (`integer` to `bigint` rewrites the whole target under
`ACCESS EXCLUSIVE`). A value shorter than that rewrite cancels it every time.
The staging worker retries on its next pass, with readers of the target
blocked for up to the timeout each time, and ends the resume after three
cancellations. The transform stays `paused`, and its `capture_failure` names
the two ways out:

* raise `statement_timeout` for the Trellis role or database to a value
  longer than the rewrite takes (`alter role trellis set statement_timeout =
  '1h'`, say; `0` removes the bound altogether), restart Trellis, and
  `RESUME` the transform again. The restart matters: a role or database
  setting reaches only new connections, and the staging worker keeps its
  connection open; or
* `DROP TRANSFORM` and define it again, which builds the target from empty and
  isn't subject to the re-type rewrite.

A `statement_timeout` meant for application queries belongs on the
application's roles.

## Index an aggregate's group key when groups are large

This applies to every aggregate, which Trellis keeps on the group basis
([ADR-0002](decisions/0002-async-data-flow.md#aggregates-a-basis-per-group-kept-by-deltas)).

On the group basis, some writes recompute an aggregate's groups from the
source instead of adding a change to them: releasing a held key, its
automatic retry, and a change that carries no row images. A recompute locks its groups, then runs
a `GROUP BY` of their source rows, and holds the locks until it commits, so
batches that touch those groups wait for it
([ADR-0002](decisions/0002-async-data-flow.md#absolute-group-writes-lock-first-and-cover-every-group-they-could-owe)).

When groups hold many rows, create an index whose leading columns are the
definition's `GROUP BY` columns:

```sql
-- TRANSFORM sales_by_region FROM orders GROUP BY region SELECT SUM(amount) AS total
CREATE INDEX ON orders (region);
```

With one, a recompute reads only its groups' rows: about 1.3 ms for a
10,000-row group, 9 ms at 100,000 rows and 80 ms at 890,000. Without one,
each recompute scans the source. Define requires the index anyway, for
`self_check`, unless the definition opts out with `WITHOUT SOURCE INDEX`
([Keep the indexes a definition relies on](#keep-the-indexes-a-definition-relies-on)).
An index you create or drop changes speed only, never results.

## Index each `MIN`/`MAX` argument, and vacuum retention sources

A `MIN` or `MAX` field ([type support](type-support.md#min-and-max))
re-reads a group's extreme from the source when a deletion or update removes
the current one, while the page holds the group's lock
([ADR-0002](decisions/0002-async-data-flow.md#min-and-max-recompute-on-retract)).
With an index whose leading columns are the `GROUP BY` columns, in order,
then the argument, that read is one index entry. Define requires that index
for each argument, and refuses a `MIN`/`MAX` definition `WITHOUT SOURCE
INDEX`; a whole-transform `RESUME` refuses while one is missing:

```sql
-- TRANSFORM first_order FROM orders GROUP BY customer_id
--   SELECT customer_id AS customer_id, MIN(created_at) AS first_at, MAX(created_at) AS last_at
CREATE INDEX ON orders (customer_id, created_at);
```

One index serves `MIN` and `MAX` of the same argument, and also serves as
the `GROUP BY` index. If it is dropped later, each such read scans the
group's rows through the `GROUP BY` index, or the whole source when that is
missing too, and once it has been missing for 5 minutes the transform pauses
([Keep the indexes a definition relies on](#keep-the-indexes-a-definition-relies-on)). On a 1-million-row source, a page that re-read 3,334 groups' `MIN` and `MAX`
took about 30 ms with the index and 305 ms with only the `GROUP BY` index;
with neither, each group took about 78 ms. A missing index never changes
results.

A source whose oldest rows are deleted on a schedule (`MIN(created_at)`
under a retention job) re-reads a touched group's extreme in nearly every
page of deletions, and each read starts at the end of the index the
deletions just emptied. Until vacuum removes those dead entries, a read
walks them: about 7 to 9 ms after 111,000 deletions in one group. Without
an older transaction open, that read marks them and the next ones cost
well under a millisecond; while a transaction older than the deletions is
open, every read pays the full walk. Keep autovacuum ahead of the retention
job on such a table (a lower `autovacuum_vacuum_scale_factor` for it, or a
`VACUUM` after each retention batch), and avoid long-running transactions,
which keep vacuum from removing the entries.

## Keep the indexes a definition relies on

Some definitions need a key or an index on a source at define time. Keep it
in place for as long as the definition runs. To replace one, create the new
index first, then drop the old one, then rename the new one if you want its
name:

```sql
CREATE UNIQUE INDEX CONCURRENTLY b_k_unique_new ON b (k);
DROP INDEX CONCURRENTLY b_k_unique;
ALTER INDEX b_k_unique_new RENAME TO b_k_unique;
```

A satisfying index then exists at every moment. For a unique index, that is
also what keeps duplicates out of the table during the swap: between dropping
the old one and the new one becoming valid, Postgres accepts them. Replacing
it inside one transaction (`DROP INDEX`, then `CREATE UNIQUE INDEX`, then
`COMMIT`) works too, but the `DROP INDEX` locks the table against reads and
writes until the transaction commits, so for as long as the index builds.

Each requirement, and what dropping it does
([transforms — Supported sources and targets](transforms.md#supported-sources-and-targets)
has the details):

* **A source's primary key**: the staging worker can no longer capture the
  table, and `status()` says so. The transforms that read it pause when
  Trellis next applies a change of it.
* **The unique index backing a source's `KEY (…)`**: build its replacement
  before dropping it. Any equivalent index backs the key, so `REINDEX
  CONCURRENTLY`, a rename, pg_repack and `ADD PRIMARY KEY USING INDEX` change
  nothing. With none valid, the staging worker's next reconcile pass pauses
  every transform reading the table, whether or not a write arrived, and the
  message gives the `CREATE UNIQUE INDEX` to run. So does a swap that drops the
  old index before it builds the new one, when a pass runs between the two.
  On a partitioned table, a root index whose leaf index was repaired stays
  invalid until `ALTER INDEX … ATTACH PARTITION` is run again for that leaf.
  `RESUME` refuses until an index backs the key, then rebuilds.
* **The unique key on a 1-1 `JOIN`'s `b` `ON` columns:** the staging worker's
  next reconcile pass pauses the transform, naming `b` and its `ON` columns,
  whether or not a duplicate arrived. So does a swap that drops the old index
  before it creates the new one, when a pass runs between the two, which is
  likely on a large table or beside a long-running transaction. `RESUME`
  refuses until the key is back, then rebuilds the target from the sources.
* **The `self_check` source index** of an aggregate or a `JOIN`: nothing
  pauses. `status()` lists a `source_index_gap`, and `self_check` refuses to
  walk the definition (unless it was defined `WITHOUT SOURCE INDEX`) until the
  index is back.
* **A `MIN`/`MAX` argument's index** (the `GROUP BY` columns, then the
  argument): the target stays exact. `status()` lists an `extreme_index_gap`,
  and each re-read of an extreme scans its group. Once the staging worker has
  found no valid such index for 5 minutes, it pauses the transform, naming
  the index to create; `RESUME` refuses until it is back, then rebuilds. A
  swap that builds the replacement first pauses nothing; one that drops the
  old index first pauses if the new one takes longer than 5 minutes to
  build, which on a large table or beside a long-running transaction it can.

## Join small, often-edited tables at query time

A `JOIN` transform copies the joined table's columns into its target, so one
edit of a joined row rewrites every target row joined to it. When the joined
table has few distinct keys, such as a status, currency or country lookup of 5
to 100 rows, one edit of the lookup is a fan-out over a large share of the
target. Trellis writes the fan-out in chunks of 10,000 rows, each its own
transaction, and never halts the transform for it. Each chunk is still a
cascade of writes: target rows, WAL, and the work of every transform that reads
the target.

The cost, measured on a 1-1 `JOIN` of 1,000,000 root rows against a 5-key
lookup: one edit of the lookup rewrites about 200,000 target rows, which takes
about 1.8 s of chunks. While the chunks run, root apply proceeds at 0.52 to
0.59 times its usual rate, and with 8 apply workers the transform is 30 to 38%
slower than the same transform without the lookup.

`status()` reports a key that fans out widely: `join.fan_out_warning` names the
key, the number of root rows on it and the threshold, which is 250,000 rows per
key unless `TRELLIS_ONE_TO_ONE_JOIN_FAN_OUT_WARNING` sets another
([observability](observability.md#transform-status-lifecycle)). `trellis
status` prints it as a warning line. The warning fires on a key that is already
large; a lookup under the threshold still costs its fan-out on every edit.

Materializing a join pays on every write to the joined table, and joining at
query time pays on every read of the joined data. Which is cheaper depends on
the read load against the write load, and on how often the joined data changes
compared with how often the target is read. A table that is rarely edited and
read by a hot query is fine to materialize. A table that is edited often, or
that is joined to a very large root, is usually better joined at query time:
leave it out of the transform and join the target to it in the reading query.

* An `orders` transform that joins a `statuses` lookup. A lookup that is
  edited once a quarter is fine to materialize. One whose rows are edited all
  day, such as a status with a running counter, rewrites every order with that
  status on each edit, so leave it out and join it in the query.
* A `currency_rates` table updated every minute. Each update rewrites every
  row in that currency. Join it at query time.
* A `customers` table with one row for every few orders. An edit of a customer
  rewrites a few orders, so materialize it.

## Partition large sources

This applies to an aggregate kept on the group basis and to a `JOIN` (see the
previous section). Their targets are built, resumed and rebuilt in place from the source, and the size
of the source decides how long that takes.

A fresh build, a `RESUME` and a rebuild in place of an **unpartitioned** source
run as one absolute write per structure, in one transaction. A `JOIN` whose
sides are both unpartitioned writes both of its structures in one transaction.
The transaction locks the groups it writes and holds its snapshot until it
commits, so its lock window, its hold on `xmin` and the stall in retiring ring
segments grow with the source. On a `JOIN` over an unpartitioned
100-million-row fact table, a fresh build held `xmin` for 23 seconds grouped by
a dimension attribute, and for 932 seconds, writing 15.9 GB of WAL, grouped by
day and attribute. For that
whole window the source's drain waits on the groups the build holds, and on a
`JOIN` so do the other side's pages on the same keys. No ring segment retires
until the build commits, so once the ring's slots fill, every source's drain
waits for it too. During a rebuild in place, readers of the target see the old
values until the commit, then the new ones
([ADR-0002](decisions/0002-async-data-flow.md#an-aggregates-build-is-one-absolute-write-per-structure),
[ADR-0019](decisions/0019-join-transforms.md#a-build-fills-p-and-m-from-each-source-and-t-from-them)).

A **partitioned** source is bounded per leaf instead. A group-basis build,
`RESUME` or rebuild writes one leaf per transaction, then the groups that route
to no leaf, and a killed build continues after the last leaf it committed. A
`JOIN` with a partitioned side that has rows builds an unpartitioned side
first, as one transaction, then each partitioned side's leaves and the cells
that route to no leaf, then a final step over the result groups, whose cost
grows with the target's group count rather than with source rows. A killed
`JOIN` build continues after its last committed step. While a `JOIN` builds
this way, queries of its target see progressive values on the partitioned
sides, and the status reads `backfilling` until the final step commits. Which
partitioned sources a definition accepts is in
[transforms — Supported sources and targets](transforms.md#supported-sources-and-targets).

So:

* Partition a large source when its aggregate or `JOIN` is kept on the group
  basis. Each leaf then costs what a source of its size costs, and no build
  transaction holds more than one leaf.
* Keep the unpartitioned side of a `JOIN` small (a dimension table), since it
  builds as one transaction.
* Schedule a `RESUME` or a `request_backfill` of a large unpartitioned source
  for a quiet time, since the drain of that source waits for it. The automatic
  rebuilds (`capture_auto_rebuilds_total`) run the same write and can't be
  scheduled.
* A `RESUME` keeps the target's rows and recomputes them in place. Groups the
  build hasn't reached show their old values, and groups that pages touch
  before the build reaches them, or result groups of a `JOIN` over a
  partitioned side, may show partial values until the status is `live`. An
  aggregate on the group basis that reads the resumed target is rebuilt once,
  when the whole build commits, not per leaf. It reports `backfilling` and may
  show wrong values until its own rebuild commits, and that rebuild is as
  large as its own source, or one leaf at a time if that source is
  partitioned.

## Roles and permissions

### One Trellis role

The role that applies a transform owns the target it creates, and every
process that writes a target must be that role or inherit it. So run every
Trellis connection as one dedicated role: the migration step, the processes
that define transforms, and the staging and drain workers. A login role that
is a member of it with `INHERIT` can run workers, as long as the Trellis role
itself applies the transforms. Keep it separate from your application's roles,
so that no application session owns a target.

The role needs:

* `LOGIN`, and not `SUPERUSER` or `REPLICATION`.
* `CREATE` on the database, even when a DBA created the instance schema
  first: attaching runs `create schema if not exists`, which PostgreSQL
  authorizes before it checks whether the schema exists.
* `CONNECT` and `TEMPORARY` on the database. Every role has both through
  `PUBLIC` unless someone revoked them; the migrations and the drain use
  temporary tables.
* `CREATE` and `USAGE` on the schema each target goes in (owning the schema
  gives both), and `USAGE` on the schema of every source.
* Ownership of each source table, or membership in the role that owns it, with `INHERIT`.
* `BYPASSRLS` if row-level security applies to it on any table it reads or
  writes, and on the ring's owner, which doesn't inherit it.

Why each is needed, and what Trellis does when one is missing:
[embedding — What the staging worker needs from the database](embedding.md#what-the-staging-worker-needs-from-the-database),
[transforms — Supported sources and targets](transforms.md#supported-sources-and-targets)
and [observability — A halting failure](observability.md#a-halting-failure).
Define refuses a source whose schema the role lacks
`USAGE` on, or that the role neither owns nor holds any privilege on, and
names the table and what the role lacks.

A setup, with `app_owner` owning the source tables, `app_writer` writing them
and `app_reader` reading targets:

```sql
create role trellis login;
grant create on database mydb to trellis;
-- only if CONNECT or TEMPORARY was revoked from PUBLIC:
grant connect, temporary on database mydb to trellis;
grant app_owner to trellis;                     -- owns the sources, inherited
create schema trellis_targets authorization trellis;

grant usage on schema trellis_targets to app_reader;
alter default privileges for role trellis in schema trellis_targets
  grant select on tables to app_reader;
```

Only Trellis creates tables in `trellis_targets`. Default privileges apply only
to tables the named role creates, so declare them for each role that applies
transforms.

`trellis/tests/least_privilege.rs` runs the whole lifecycle as a role holding
only these privileges, in a database that revoked `PUBLIC`'s, with no
row-level security and so no `BYPASSRLS`.

### Application roles read targets, and nothing else

Grant application roles `SELECT` on target tables and nothing more: no
`INSERT`, `UPDATE`, `DELETE` or `TRUNCATE`, and never ownership. Only Trellis
writes a target, and it doesn't correct a hand edit
([transforms — Target tables are Trellis-owned](transforms.md#target-tables-are-trellis-owned),
[known correctness gaps — Rows written into a target, a hidden table or Trellis's schema](known-correctness-gaps.md#rows-written-into-a-target-a-hidden-table-or-trelliss-schema)).
A read-only grant turns a hand edit into a permission error instead of a
silent divergence.

`DROP TRANSFORM` and defining again, the repair for several gaps, create a new
table; the default privileges above give readers `SELECT` on it without a
grant step.

Row-level security on a target for your readers is fine while the Trellis role
is exempt; the limits are in
[transforms — Target tables are Trellis-owned](transforms.md#target-tables-are-trellis-owned).

Application roles keep their ordinary write grants on source tables, and need
nothing on the instance schema.
