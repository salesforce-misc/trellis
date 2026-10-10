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

The share depends on the target's shape. An aggregate's ledger keeps a
`GROUP BY` index when the aggregate has a recomputed field (`MIN`, `MAX`, a
float `SUM`, a composed field) or a relationship feeds it. A build inserts
into that index at random positions, so once the index outgrows what a
checkpoint interval leaves clean, almost every insert logs a full page. An
invertible aggregate built by the Re-derive build keeps no such index, and
writes few full-page images (0.18 million over 100M rows, against 25 million
with the index) (#723).

The #723 benchmark runs set `checkpoint_timeout=1min` and `max_wal_size=4GB`
to stress a build, giving a requested checkpoint about every 10 seconds. There,
a 100M-row, 1M-group build with the index wrote 3,370 B of WAL per built row,
2.6 times the 1,290 B of a 10M-row, 100k-group build.

### `wal_compression`

Set `wal_compression = lz4` where the server is built with LZ4 support, and
`on` otherwise. It compresses full-page images and needs a reload, not a
restart.

On the 100M-row build above, `lz4` alone cut WAL from 3,370 to 1,918 B per
built row and define-to-`live` from 1,446 to 1,090 seconds, and the writers
rose from 1,269 to 1,751 writes per second (#723). The gain is largest for
targets whose ledger keeps the `GROUP BY` index; an invertible aggregate built
by Re-derive writes 1,036 B per row and reaches `live` in 396 seconds.

### `max_wal_size` and `checkpoint_timeout`

Size them so the clock starts the checkpoints, and the interval is minutes,
not seconds.

* `checkpoint_timeout`: raise it from the default 5 minutes to 15 or 30.
* `max_wal_size`: PostgreSQL forces a checkpoint at about `max_wal_size`
  divided by (1 + `checkpoint_completion_target`), roughly half. Set it to
  about twice the WAL written in one `checkpoint_timeout`. In the #723 runs a
  checkpoint covered about 2.2 GB at `max_wal_size=4GB`.
* Take the WAL rate from your own database: read `pg_stat_wal.wal_bytes`
  before and after a build, or at two points of steady load, and divide by the
  seconds between. A 100M-row invertible aggregate wrote about 260 MB per
  second (#723), so a 15-minute interval writes about 235 GB and wants a
  `max_wal_size` near 450 GB.
* Size `max_wal_size` for steady load, not for builds. If the disk under
  `pg_wal` can't hold more, let a rare large build force a checkpoint every
  few minutes; `wal_compression` offsets the extra full-page images.
* `pg_wal` holds about `max_wal_size` plus one checkpoint's worth of WAL (the
  #723 runs peaked at 4,192 and 4,576 MiB at a 4,096 MiB setting). It grows
  without bound while WAL archiving fails or a replication slot lags, so alert
  on both.

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
outruns the drain, which it can by about 20 times
([#808](https://github.com/salesforce-misc/trellis/issues/808)), and whenever
no Trellis process is running. Each undrained change takes about 188 B for a
narrow row
([#565, E6](https://github.com/salesforce-misc/trellis/issues/565#issuecomment-5844307119));
a ring row carries the primary key and the columns definitions read, so wider
rows take more, and an update carries both its old and new image.

Size the free disk for the longest Trellis outage you want to ride out:

```
ring bytes ≈ 188 B × captured changes per second × outage seconds
```

At 10,000 changes per second, a one-hour outage needs about 6.8 GB.

* Take the change rate from your own database: sum `n_tup_ins`, `n_tup_upd`
  and `n_tup_del` in `pg_stat_user_tables` over the tables Trellis captures
  (each definition's source, and every table its relationships reach) at two
  points of steady load, and divide by the seconds between. It errs high:
  capture skips an update that changes no column a definition reads. Leave
  out Trellis's own targets: Trellis stages a target's changes for the
  transforms that read it as it writes them, so those rows add to the ring
  only while Trellis runs.
* While every Trellis process is down, nothing drains or bounds the ring, and
  the database's own disk alerts are the only guard. Alert on free disk.
* To see the ring's size on disk, sum `pg_total_relation_size` over the ring
  tables `seg_0` to `seg_3` in Trellis's schema.
* The ring's gauges and its soft and hard bounds are designed but not built
  ([#808](https://github.com/salesforce-misc/trellis/issues/808),
  [ADR-0002 — Consequences](decisions/0002-async-data-flow.md#consequences-and-costs)).

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

## Index relationship join columns

Index the column each relationship joins by, on the table that holds it:

* `from_col`, on the from-side table. A change to a related row finds the
  from-side rows to re-derive by looking up `from_col`.
* `to_col` of a to-many relationship, on the to-side table. A definition
  that aggregates the related rows reads them by `to_col`. (A to-one's
  `to_col` is unique, so it has an index already.)

```sql
CREATE INDEX ON orders (customer_id);
```

Trellis doesn't create indexes on your tables, so without one each of those
reads scans the table, and the scan grows with the table, not with the batch
of keys it looks up. With one, a batch reads only its keys' rows.

Trellis checks for the index each time it prepares a batch's relationship
reads. A column counts as indexed if it is the leading column of a plain btree
index that is valid, not partial, and under the column's own collation. For an
indexed column, Trellis plans the read to use the index, which matters most on
PostgreSQL 16: while a table's statistics lag its size, it can price thousands
of probes of a near-unique index above a parallel sequential scan, and read
the whole table. For an unindexed column, it leaves the plan to PostgreSQL,
since with no index to use, turning sequential scans off would only cost the
read its parallel scan. An index you create or drop takes effect on a later
batch, with no restart or redefine. It changes speed only, never results.

`status()` and `self_check` list each join column a definition reads that has no
such index, with the index to create
([observability](observability.md#transform-status-lifecycle)).

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
* Ownership of each source table and each relationship to-side table, or
  membership in the role that owns it, with `INHERIT`.
* `BYPASSRLS` if row-level security applies to it on any table it reads or
  writes, and on the ring's owner, which doesn't inherit it.

Why each is needed, and what Trellis does when one is missing:
[embedding — What the staging worker needs from the database](embedding.md#what-the-staging-worker-needs-from-the-database),
[transforms — Supported sources and targets](transforms.md#supported-sources-and-targets)
and [known correctness gaps, entry 11](known-correctness-gaps.md#11-row-level-security-on-a-role-trellis-runs-as).
Define refuses a source or relationship endpoint whose schema the role lacks
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
[known correctness gaps, entry 8](known-correctness-gaps.md#8-hand-edits-to-a-target-table)).
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
