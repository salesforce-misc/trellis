---
status: accepted
date: 2026-08-20
deciders: Michael Ries
consulted: 
informed:
---

# Quarantine Storage and Error API

Because [data-flow](../data-flow.md) commits invalid source data before a
derivation runs, a failing transform or bad row must be **quarantined** rather
than block the write. This ADR settles where per-row and per-column status
lives, the API applications use to discover and clear quarantines, and how the
fuse guarding against runaway per-row tracking behaves.

## Decision

Quarantine is tracked at two independent fuse tiers, with no auto-escalation:

* **Whole-key fuse** — a sparse **`poison`** exception table
  (`V13__quarantine.sql`), one row per poisoned *source* row. Tripping it moves
  the whole transform to `quarantined`; resuming re-runs the full backfill.
* **Per-`(transform, column)` fuse** — `column_failures`, `column_status`, and a
  `column_deaths` counter (`V21__column_quarantine.sql`). A failure in one
  column's formula pauses only that column, leaving healthy columns on the same
  transform computing.

A `TRANSFORM` can compute several columns, so a `live` transform can carry one or
more `paused` columns. A transform's overall lifecycle status
([transforms — Status](../transforms.md#status)) describes the whole keyspace and
is unaffected by column pausing.

Applications discover and clear quarantines through three client-library reads
(below).

## Options considered

Storage granularity for row/column status:

* **In the neighbor table** — status columns beside the derived values. Cheapest
  to read, but a target row can be written by several transforms, so avoiding
  conflated failures needs one column per transform: the table widens with every
  chained transform.
* **Dense `(row, transform)` status table** — one row per key regardless of
  health, written alongside every neighbor-table write. Right granularity, but
  doubles write amplification on the hot path — at 180k+ source changes/sec, a
  second write for every *successful* row.
* **Sparse exception table (chosen)** — dense-table shape, but only failing rows
  are inserted; a row's absence *is* its healthy status. Steady-state write cost
  stays near zero (>99% of rows never touch it) while still answering "is this
  row valid" and giving the error API a table to query.

The tradeoff: validity is checked on read as *absence*, not a column in hand. We
mitigate with primary keys on both paths (below) and expect most consumers to
ask the API "is transform X (or column X.Y) quarantining anything" rather than
check rows inline.

## Exception table shape

The exception key is the source row, not any downstream target: one source
row's change fans out to several transforms/columns at once, so the natural key
for a fold failure is the `(src_table, key)` that failed to fold.

* **`poison`** — `(src_table, key)` primary key, one record per poisoned source
  row, for the whole-key fuse.
* **`column_failures`** — keyed on `(transform_table, column_name, src_table,
  key)`, one row per `(transform, column)` pair that failed while propagating a
  source row.

Column detail is separate from `poison` rather than a `failures` array on it,
because landing in `poison` means "globally excluded from folding" — correct for
the whole-key fuse but wrong for a column-only failure, where a paused column
freezes its value rather than evicting the row from every other column that
still computes cleanly.

## Column status table

`column_status` (`V21__column_quarantine.sql`) is a small dense table of each
transform's currently-paused columns, so "is anything paused right now" is a
cheap read rather than a scan over per-row failure detail. Keyed on
`(transform_table, column_name)`; carries `paused_at` and `last_error`.

`local_fuse` distinguishes *why* a column is paused: `true` means its own fuse
tripped; `false` means an upstream column it reads was paused (see
"Propagation"). A column can be both. A transform absent from this table has
every column live; the table does not replace the transform's overall lifecycle
status.

## Client library API

Three calls, separated by cost:

1. **List paused/quarantined targets** — a flat list across every transform,
   each addressed as `transform` (whole keyspace) or `transform.column`. Cheap:
   reads `column_status` plus each transform's lifecycle status, no join against
   exception detail. This is the one dashboards/health-checks poll.
2. **Status for one target** — given `transform` or `transform.column`, its
   current state and, for a paused column, when it tripped and its last error.
3. **Sample quarantined rows** — a paginated batch of `(src_table, key,
   error_message)` from `poison` or `column_failures`. `column_failures` rows are
   cleared when the column is resumed, not as each row re-evaluates cleanly.

## Fuse: per-column, then transform-wide

Per-row quarantine assumes failures are the exception. When a large fraction of
one column's rows fail (a broken formula, an incompatible upstream schema
change), tracking each individually stops being useful — the application needs
to know that *one column* is broken, not a million identical error rows.

When `column_deaths` for a pair crosses the threshold, the **column fuse** trips:
row-level tracking stops (its `column_failures` rows are retained as evidence
until resume) and the column is marked `paused`. Resuming re-runs the backfill
for just that column's formula, without touching other columns or the
transform's lifecycle status.

The **transform-wide fuse** trips when a failure isn't attributable to one
column (e.g. a key-shape/DDL failure dooming every column's write for a row),
moving the whole transform to `quarantined`. `quarantined` is one state of the
lifecycle (`waiting_to_backfill` → `backfilling` → `live`, plus `quarantined`);
resuming re-runs the full backfill (see [data-flow](../data-flow.md)).

Settled parameters (`V21__column_quarantine.sql` / `staging::quarantine`):

* **Threshold** — a fixed count, matching the row-level fuse's
  `DEFAULT_DEATH_THRESHOLD`; not percentage-based, not per-transform configurable.
* **Counter** — an incrementally-maintained `column_deaths` table, the same
  `key_deaths`-style write-amplification tradeoff, not a live aggregate query.
* **Paused-column value** — freezes at the last computed value; never nulled.
  Staleness is discoverable via `column_status`/the API, not an in-band marker.
* **Propagation** — tripping either fuse cascades the pause to 1-1 downstream
  transforms reading the paused column (`column_pause_cascades`,
  `defs::catalog::column_dependents`), so no downstream reader silently consumes
  a frozen value. Aggregate transforms are *not* cascaded into — aggregate
  accumulation has no per-column pause concept (`staging::apply_aggregate`), a
  deliberate gap.

## Resume re-arms the fuse

The transform-wide fuse counts distinct evicted keys (`poison` rows) for a
source table. A resume **re-arms** the fuse rather than erasing history:
`staging::quarantine::resume_transform` stamps
`transform_definitions.fuse_rearmed_at` (`V29__transform_fuse_rearm.sql`) in the
same transaction as the status drop, and `trip_transform_fuse_if_crossed` counts
only `poison` rows evicted after that instant — a full fresh threshold's budget
of *new* evictions (`null` reads as `-infinity`). Without this, a source that
ever hit the threshold stayed there forever, and the next single new eviction
re-quarantined immediately.

The rejected alternative — deleting the source's `poison`/`key_deaths` rows on
resume — fails because:

* `poison` is not just a counter but the **marker** the fold consults
  (`poisoned_keys_among`) to exclude a key globally, and each key may own parked
  work in `poison_held` that only `release_key` knows how to replay (per key,
  origin positions preserved). Deleting the marker strands that work.
* The fuse is keyed per source table but a resume is per *transform*, and one
  source can back several transforms. Clearing shared rows would un-evict keys
  out from under siblings that aren't being re-backfilled, losing their parked
  deltas.
* Keeping the rows keeps the audit trail of what was evicted and why.

The `key_deaths` (per-key) and `column_deaths` (per-column) tiers are
deliberately unaffected by a whole-transform resume: they clear on a clean drain
of the key and on `resume_column` respectively, and a whole-transform resume
makes no claim about any individual key's or column's health.

## Open questions

* Retry-with-backoff vs. immediate quarantine, and whether older entries move to
  a dead-letter area, are still open. This ADR fixes storage and the read APIs,
  not retry policy.
