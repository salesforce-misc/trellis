# Recommendations

How to run Trellis well. An incrementally maintained view trades a constant
write load for avoiding spiky read load: every source change costs a little
work at write time so that reads are cheap. These recommendations help an
operator pay that write load efficiently and safely. They configure
PostgreSQL and its roles; they change nothing in Trellis itself.

## WAL and checkpoints

A build writes a lot of WAL, and so does steady load on a large target. Two
settings decide how much.

### Why checkpoints matter

The first change to a page after a checkpoint logs the whole page (a full-page
image), not just the change. A later change to the same page, before the next
checkpoint, logs only the change. So the more often checkpoints run, the more
of the WAL is full-page images.

Trellis makes this worse than a typical workload because a build writes to
random places in large indexes and tables. A chunk of an aggregate build
inserted about 10,000 ledger entries at random positions in the ledger's
`GROUP BY` index. Once that index reached about 2.9 GB, and with a checkpoint
every 10 seconds or so, almost every insert was the first change to its leaf
page since the checkpoint, and logged a full page. WAL per built row rose
2.6 times over the same build at 10M rows, every backend waited on
`LWLock:WALWrite`, and the writers fell from 2,000 to 1,269 writes per second
(#723).

### `wal_compression`

Set `wal_compression = lz4` where PostgreSQL is built with LZ4 support
(PostgreSQL 15 and later), and `on` otherwise. It compresses full-page images
and needs a reload, not a restart.

On a 100M-row aggregate build under 8 writers at 2,000 writes per second,
`wal_compression = lz4` alone cut WAL from 3,370 to 1,918 bytes per built row
and define-to-`live` from 1,446 to 1,090 seconds, and the writers' rate rose
from 1,269 to 1,751 per second (#723). That build kept the ledger's `GROUP BY` index. On a separate 100M run of the
same benchmark, a build that skips that index and inserts each chunk's new
entries first wrote 1,036 bytes of WAL per built row, wrote 104 GB in all and
finished in 396 seconds (#723).

### `max_wal_size` and `checkpoint_timeout`

Size them so a checkpoint is started by the clock, not by WAL volume, and the
interval is minutes, not seconds.

* `checkpoint_timeout` is how often a checkpoint runs when the WAL doesn't
  force one earlier. Raise it from the default 5 minutes to 15 or 30.
* `max_wal_size` is the WAL a checkpoint interval may write before a
  checkpoint is forced. Size it to the WAL a build writes in one
  `checkpoint_timeout`: the build's WAL rate times the interval. The rate is
  the rows built per second times the WAL per row, which ran from 1,036 to
  3,370 bytes per row for an aggregate at 100M rows (#723). The build that
  wrote 104 GB in 396 seconds wrote about 260 MB per second, so a 15-minute
  interval needs a `max_wal_size` of about 235 GB to avoid forced
  checkpoints. Give it what the disk under `pg_wal` allows and accept a
  forced checkpoint every few minutes for the largest builds, not every few
  seconds.
* `pg_wal` grows to about `max_wal_size` plus one checkpoint's worth of WAL
  (#723), so leave that headroom on its volume.

A longer interval makes crash recovery replay more WAL. For a build, that
trade is worth it.

To check the sizing, set `log_checkpoints = on` and compare requested and
timed checkpoints during a build (`pg_stat_checkpointer`, or
`pg_stat_bgwriter` before PostgreSQL 17). Most of them should be timed. The
benchmark runs in #723 set `checkpoint_timeout=1min` and `max_wal_size=4GB`
on purpose, to stress the build; they had a requested checkpoint every 10
seconds or so. Production settings are larger.

Watching a build's progress is in [observability](observability.md).

## Roles and permissions

### One Trellis role

Run every Trellis process, the staging worker, the drain workers and the
processes that define transforms, as one dedicated role, or as login roles
that are members of it with `INHERIT`. Keep it separate from the roles your
application connects as.

The role owns the instance schema, the staging ring and the capture functions,
and every target it creates. It needs, and nothing more:

* `LOGIN` for a role that connects. Not `SUPERUSER`, not `REPLICATION`, and
  no `wal_level = logical`: nothing reads the WAL.
* `CREATE` on the database, so it can create the instance schema. A DBA may
  create the schema first, owned by a role the Trellis role is a member of.
* `CREATE` on the schema each target goes in (`public` by default), and
  `USAGE` on the schema of every source.
* Ownership of each source table and each relationship to-side table, or
  membership in the role that owns it. `ALTER TABLE ... ENABLE ALWAYS
  TRIGGER` needs ownership, and the capture triggers are installed with it.
  An owner holds `SELECT` unless it was revoked, and the capture functions
  and the builds read with it.
* `BYPASSRLS` if any table Trellis reads or writes has row-level security
  that applies to the role, and likewise on the ring's owner, which doesn't
  inherit it. Prefer owning the table without `FORCE ROW LEVEL SECURITY`.

The capture functions are `SECURITY DEFINER`, so the roles that write your
source tables need no privilege on the instance schema. How each requirement
shows up, and what Trellis does when one is missing, is in
[embedding — What the staging worker needs from the database](embedding.md#what-the-staging-worker-needs-from-the-database)
and
[transforms — Supported sources and targets](transforms.md#supported-sources-and-targets).
A missing privilege or a policy that applies to the role is entry 11 of the
[known correctness gaps](known-correctness-gaps.md#11-row-level-security-on-a-role-trellis-runs-as).

### Application roles read targets, and nothing else

Grant application roles `SELECT` on target tables and nothing more. Don't
grant `INSERT`, `UPDATE`, `DELETE` or `TRUNCATE`, and don't make an
application role a target's owner.

Only Trellis writes a target. It addresses rows by the target's key, assumes
nobody else writes, and doesn't correct a hand edit. An edited row stays wrong
until its key is recomputed for another reason, and a truncated target stays
empty ([transforms — Target tables are Trellis-owned](transforms.md#target-tables-are-trellis-owned),
[known correctness gaps](known-correctness-gaps.md#8-hand-edits-to-a-target-table)).
A read-only grant makes that mistake a permission error at the application
instead of a silent divergence.

Grants on a target live and die with the table, so `DROP TRANSFORM` takes them
with it.
Row-level security on a target for your readers is fine while the Trellis role
is exempt; the limits are in
[transforms — Target tables are Trellis-owned](transforms.md#target-tables-are-trellis-owned).

Application roles keep their ordinary write grants on source tables. They
need nothing on the instance schema.

## See also

* [Embedding Trellis](embedding.md): who runs what, and what each process
  needs from the database.
* [Transforms](transforms.md): what is refused at define, and the rules for
  target tables.
* [Observability](observability.md): metrics, logs and transform status.
* [Known correctness gaps](known-correctness-gaps.md): how a target can go
  stale or wrong.
