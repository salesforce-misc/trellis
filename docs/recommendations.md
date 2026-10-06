# Recommendations

How to run Trellis well. An incrementally maintained view trades a constant
write load for avoiding spiky read load: every source change costs a little
work at write time so that reads are cheap. These recommendations help an
operator pay that write load efficiently and safely. They configure
PostgreSQL and its roles; they change nothing in Trellis itself.

## WAL and checkpoints

A build writes a lot of WAL, and so does steady load on a large target. Three
settings decide how much: `wal_compression`, `max_wal_size` and
`checkpoint_timeout`.

### Why checkpoints matter

The first change to a page after a checkpoint logs the whole page (a full-page
image), not just the change. A later change to the same page, before the next
checkpoint, logs only the change. So the more often checkpoints run, the more
of the WAL is full-page images.

The share depends on the target's shape. An aggregate's ledger keeps a
`GROUP BY` index when the aggregate has a recomputed field (`MIN`, `MAX`, a
float `SUM`, a composed field) or a relationship feeds it. A build inserts
into that index at random positions, so once the index outgrows what a
checkpoint interval leaves clean, almost every insert is the first change to
its leaf page and logs a full page. An invertible aggregate built by the
Re-derive build keeps no such index, and its build writes few full-page images
(0.18 million over 100M rows, against 25 million with the index) (#723).

On a 100M-row, 1M-group build under write load, with the index and a
checkpoint about every 10 seconds, WAL per built row was 3,370 B, 2.6 times
the 1,290 B of a 10M-row, 100k-group build. Most sampled backends waited on
`LWLock:WALWrite` (2.35 of 3.7 in `chunk_write`), and the 8 writers managed
1,269 writes per second against the 2,000 they were set to (#723).

### `wal_compression`

Set `wal_compression = lz4` where the server is built with LZ4 support, and
`on` otherwise. It compresses full-page images and needs a reload, not a
restart.

On the 100M-row build above, `lz4` alone cut WAL from 3,370 to 1,918 B per
built row and define-to-`live` from 1,446 to 1,090 seconds, and the writers
rose from 1,269 to 1,751 per second (#723). The gain is largest for targets
whose ledger keeps the `GROUP BY` index. An invertible aggregate built by
Re-derive writes 1,036 B per row, 104 GB over 100M rows, and reaches `live` in
396 seconds (#723).

### `max_wal_size` and `checkpoint_timeout`

Size them so the clock starts the checkpoints, and the interval is minutes,
not seconds.

* `checkpoint_timeout` is how often a checkpoint runs when WAL volume doesn't
  force one earlier. Raise it from the default 5 minutes to 15 or 30.
* PostgreSQL forces a checkpoint at about `max_wal_size` divided by
  (1 + `checkpoint_completion_target`), roughly half of `max_wal_size`. Set
  `max_wal_size` to about twice the WAL written in one `checkpoint_timeout`.
  The #723 runs show the factor: a checkpoint covered about 2.2 GB at
  `max_wal_size=4GB`.
* Take the WAL rate from your own database. Read `pg_stat_wal.wal_bytes`
  before and after a build, or at two points of steady load, and divide by the
  seconds between. A 100M-row invertible aggregate wrote about 260 MB per
  second (#723), so a 15-minute interval writes about 235 GB and wants a
  `max_wal_size` near 450 GB.
* A build is a burst, steady load is not. Size `max_wal_size` for steady load,
  and let the rare large build force a checkpoint every few minutes if the
  disk under `pg_wal` can't hold more. That costs some full-page images, which
  `wal_compression` offsets.
* `pg_wal` holds about `max_wal_size` plus one checkpoint's worth of WAL: the
  #723 runs peaked at 4,192 and 4,576 MiB at a 4,096 MiB setting. It grows
  past that, without bound, while WAL archiving fails or a replication slot
  lags, so alert on both.

A longer interval makes crash recovery replay more WAL. For a database that
builds large targets, that trade is worth it.

### How to tell it is working

* `pg_stat_wal`: `wal_fpi` divided by `wal_records` falls, and `wal_bytes`
  per row built falls, after `wal_compression` is on and checkpoints are
  spaced out.
* Timed checkpoints outnumber requested ones during a build. The counters are
  `num_timed` and `num_requested` in `pg_stat_checkpointer` from PostgreSQL
  17, and `checkpoints_timed` and `checkpoints_req` in `pg_stat_bgwriter`
  before it. `log_checkpoints = on` logs why each one started.

The #723 benchmark runs set `checkpoint_timeout=1min` and `max_wal_size=4GB`
to stress a build, and see a requested checkpoint every 10 seconds or so.
Production settings are larger.

Watching a build's progress is in
[observability — Transform status lifecycle](observability.md#transform-status-lifecycle).

## Roles and permissions

### One Trellis role

The role that applies a transform owns the target it creates, and every
process that writes a target must be that role or inherit it
([embedding](embedding.md#what-the-staging-worker-needs-from-the-database)). So run every Trellis connection as
one dedicated role: the migration step, the processes that define transforms,
and the staging and drain workers. A login role that is a member of it with
`INHERIT` works when it only runs workers and the Trellis role itself applies
the transforms. Keep it separate from the roles your application connects as,
so that no application session owns a target.

The role needs:

* `LOGIN`, and not `SUPERUSER` or `REPLICATION`.
* `CREATE` on the database, even when a DBA created the instance schema
  first: attaching runs `create schema if not exists`, which PostgreSQL
  authorizes before it checks whether the schema exists.
* `CREATE` on the schema each target goes in, and `USAGE` on the schema of
  every source.
* Ownership of each source table and each relationship to-side table, or
  membership in the role that owns it, with `INHERIT`.
* `BYPASSRLS` if row-level security applies to it on any table it reads or
  writes, and on the ring's owner, which doesn't inherit it.

Why each is needed, and what Trellis does when one is missing, is in
[embedding — What the staging worker needs from the database](embedding.md#what-the-staging-worker-needs-from-the-database),
[transforms — Supported sources and targets](transforms.md#supported-sources-and-targets)
and [known correctness gaps, entry 11](known-correctness-gaps.md#11-row-level-security-on-a-role-trellis-runs-as).

A setup, with `app_owner` owning the source tables, `app_writer` writing them
and `app_reader` reading targets:

```sql
create role trellis login;
grant create on database mydb to trellis;
grant app_owner to trellis;                     -- owns the sources, inherited
create schema trellis_targets authorization trellis;

grant usage on schema trellis_targets to app_reader;
alter default privileges for role trellis in schema trellis_targets
  grant select on tables to app_reader;
```

Targets go in `trellis_targets`, a schema only Trellis creates tables in.
Default privileges apply to tables the named role creates, so declare them for
each role that applies transforms.

### Application roles read targets, and nothing else

Grant application roles `SELECT` on target tables and nothing more: no
`INSERT`, `UPDATE`, `DELETE` or `TRUNCATE`, and never ownership. Only Trellis
writes a target, and it doesn't correct a hand edit
([transforms — Target tables are Trellis-owned](transforms.md#target-tables-are-trellis-owned),
[known correctness gaps, entry 8](known-correctness-gaps.md#8-hand-edits-to-a-target-table)).
A read-only grant makes that mistake a permission error at the application
instead of a silent divergence.

`DROP TRANSFORM` and defining again create a new table, and that is the repair
for several gaps. The default privileges above give readers `SELECT` on the
new table without a grant step. Readers also need `USAGE` on the target
schema.

Row-level security on a target for your readers is fine while the Trellis role
is exempt; the limits are in
[transforms — Target tables are Trellis-owned](transforms.md#target-tables-are-trellis-owned).

Application roles keep their ordinary write grants on source tables, and need
nothing on the instance schema.

## See also

* [Embedding Trellis](embedding.md): who runs what, and what each process
  needs from the database.
* [Transforms](transforms.md): what is refused at define, and the rules for
  target tables.
* [Observability](observability.md): metrics, logs and transform status.
* [Known correctness gaps](known-correctness-gaps.md): how a target can go
  stale or wrong.
