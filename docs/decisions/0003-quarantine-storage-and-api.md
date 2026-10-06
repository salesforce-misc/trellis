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
  (`V13__quarantine.sql`, `V71__poison_per_transform.sql`), one row per
  *source* row poisoned for one transform. The key is left out of that
  transform's apply only: every other transform reading the source keeps
  applying it. Tripping the fuse moves that transform to `quarantined`;
  resuming deletes the keys it holds and rebuilds the target from the source
  without clearing it (see [ADR-0014](0014-pause-and-drop-a-transform.md#resume-reconciles-with-source-not-by-catch-up)).
* **Per-`(transform, column)` fuse** — `column_failures`, `column_status`, and a
  `column_deaths` counter (`V21__column_quarantine.sql`). A failure in one
  column's formula pauses only that column, leaving healthy columns on the same
  transform computing.

A `TRANSFORM` can compute several columns, so a `live` transform can carry one or
more `paused` columns. A transform's overall lifecycle status
([transforms — Status](../transforms.md#status)) describes the whole keyspace and
is unaffected by column pausing.

Applications discover and clear quarantines through the client-library reads
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

The exception key is the source row and the transform whose apply of it
failed, not any downstream target: one source row's change fans out to several
transforms/columns at once, and a failure in one of them is that one's to hold.

* **`poison`** — `(transform_id, src_table, key)` primary key, one record per
  source row poisoned for a transform, for the whole-key fuse. `poison_held`
  (the poisoned key's parked later changes) and `key_deaths` (its death
  counter) are keyed by the transform too, and every one of them goes with
  the transform's definition when it is dropped (`on delete cascade`).
* **`column_failures`** — keyed on `(transform_table, column_name, src_table,
  key)`, one row per `(transform, column)` pair that failed while propagating a
  source row.

**`src_table` here is the canonical, fully-qualified identity of the source
table, not whatever spelling the ring row being diagnosed happened to carry**
(issue #283, [ADR-0011](0011-fully-qualified-names.md)'s qualified identity). Every one of these tables —
`poison`, `poison_held`, `key_deaths` and `column_failures` — is both written
and read under it, with `staging::quarantine` resolving the ring spelling once
per source table per batch. A ring row may carry a bare or a qualified spelling
of one source (`orders`, `public.orders`); keying on the raw spelling would
split one source's quarantine state in two: half-threshold fuse budgets that
never trip, two death counters for one row, and a fold exclusion blind to a key
poisoned under the other spelling. The fuse's own `transform_fuse_gate` lock
row is keyed by the transform, so no spelling reaches it.

Column detail is separate from `poison` rather than a `failures` array on it,
because landing in `poison` means "left out of the transform's apply" — correct
for the whole-key fuse but wrong for a column-only failure, where a paused
column freezes its value rather than evicting the row from every other column
that still computes cleanly.

### Whole-key poison is per transform

A whole-key failure in one transform (a target key copy too narrow for a new
value, a constraint on its target, row-level security on its writes) is that
transform's to hold: the key stays live in every other transform on the same
source. Pause loudly when correctness can't be guaranteed, but pause only what
needs pausing, as the per-column tier does.

* **Isolation names the transform.** A record that fails alone is probed again
  with the transforms reading its table directly left out but one, to find the
  one(s) whose apply it fails in (`staging::quarantine::attribute`). A record
  that fails with every direct reader left out fails in the work done for the
  transforms reading its table through a relationship (the to-side's reverse
  recomputes and settled projection), and is charged to each of those. A
  record that fails only with two transforms together is charged to nobody, as
  a failure only two records reproduce together is.
* **The fold runs once per key per page.** Only the poisoned transform's
  apply skips the key, and parks the change in `poison_held` for it. A
  relationship's reverse work skips the key only once every reader of the
  relationship that isn't frozen holds it, or none is left that isn't frozen,
  and a change every reader holds is dropped from the page before its images
  are decoded. A page parks a change for a transform only while the
  transform still holds the key, so a release or resume between the page's
  compute and its apply leaves no held row behind.
* **A failure that is really in the source** (one every reader hits, such as an
  image that won't decode) is poisoned once per transform that hits it.
* **Accepted costs:** two transforms can disagree on one key, one current and
  one frozen, which the quarantine API shows; and the source-wide case writes a
  poison row per transform.
* **Rejected:** one poison row per source row for every reader (one
  transform's failure would freeze the key in every sibling), and per-column
  whole-key poison (a whole-key failure is one that can't be pinned to a
  column, and the per-column tier already exists).

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
   error_message)` from `poison` (the transform's own rows) or
   `column_failures`. `column_failures` rows are cleared when the column is
   resumed, not as each row re-evaluates cleanly.

A fourth read, `poisoned_since`, lists the poisoned keys recorded since a
watermark, each with the transform it's held for.

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

The **transform-wide fuse** trips when the keys poisoned for one transform
reach the threshold, moving that transform to `quarantined`. It counts the
transform's own `poison` rows, so a sibling's evictions on the same source never
trip it. `quarantined` is one state of the lifecycle (`waiting_to_backfill` →
`backfilling` → `live`, plus `quarantined` and `paused`); resuming rebuilds the
target from the source without clearing it (see
[ADR-0014](0014-pause-and-drop-a-transform.md#resume-reconciles-with-source-not-by-catch-up)
and [data-flow](../data-flow.md)).

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
  a frozen value. The column-level fuse only ever freezes a column of a 1-1
  transform, and the cascade stops at aggregates
  ([known correctness gaps](../known-correctness-gaps.md)).
* **Sibling readers** — a field of the same 1-1 definition that reads a paused
  field by alias, directly or through other fields, is paused with it (a
  `column_status` row and a cascade edge), so Apply freezes it rather than
  evaluating it over the paused field's absence. A resume releases it once no
  field it reads is still paused, rebuilding it in the same field build.

## Resume releases the transform's held keys

The transform-wide fuse counts the distinct keys poisoned for the transform
(its `poison` rows). A resume deletes the transform's own `poison`,
`poison_held` and `key_deaths` rows in the same transaction as the status drop,
so the resumed transform starts from a count of zero: a full fresh threshold's
budget of new evictions. Counting the old rows would leave a transform that ever
hit the threshold there forever, and the next single new eviction would
re-quarantine it. Other transforms' rows are untouched: each transform's held
keys are its own, so the delete un-evicts nothing out from under a sibling.

`staging::quarantine::resume_transform` also stamps
`transform_definitions.fuse_rearmed_at` (`V29__transform_fuse_rearm.sql`) as the
resume's epoch: a backfill chunk of the build before it is stale against it, and
an eviction whose isolation read the old epoch poisons nothing for the rebuilt
transform.

The `column_deaths` (per-column) tier is unaffected by a whole-transform
resume: it clears on `resume_column`.

## Releasing held keys

A key poisoned for a transform stays held, its target row for the key frozen,
until one of these releases it:

* **Resume.** `RESUME TRANSFORM` deletes every key the transform holds (above)
  and re-derives every key from the source, so the parked work is superseded
  rather than replayed. Before it does, it re-validates the transform against
  the live schema and brings Trellis's typed copies of its key, passthrough,
  `GROUP BY` and projection-key columns to their sources' live types
  ([ADR-0014](0014-pause-and-drop-a-transform.md#resume-reconciles-with-source-not-by-catch-up)),
  so a key quarantined because a widened source value didn't fit a copy
  (`22003`, `22001`) is rebuilt into the widened copy. A key whose cause is
  still there fails again, and is poisoned again.
* **Drop.** Dropping the transform deletes its held keys with its definition
  (`on delete cascade`), so defining it again starts with none.
* **Per-key release.** `staging::quarantine::release_key(transform, src_table,
  key)` deletes the transform's rows for the key and stages one image-less
  `Recompute` of it, which re-derives the key from its current row, including
  the to-one projection rewrite of #754. The `Recompute` reaches every reader of
  the table: one that doesn't hold the key re-derives it, which is idempotent,
  and one that does parks it. It is test-only (`internals`); a public release
  that also reports held keys in `status` is #759.

## Retry policy

A transient failure (a lost connection, a lock or serialization conflict) is retried.
A failure that reproduces is isolated to the source key that causes it and the transform
whose apply it fails in, and the key is evicted to `poison` for that transform once its
`key_deaths` count reaches `DEFAULT_DEATH_THRESHOLD`.
A failure that is structural rather than one key's fault, so that every key reproduces
it, is not charged to any key: it pauses the definitions it reaches
([ADR-0014](0014-pause-and-drop-a-transform.md)). `poison` and `poison_held` are where
evicted work waits until it is released, or its transform is resumed or dropped
([Releasing held keys](#releasing-held-keys)); nothing moves
entries elsewhere by age. Whether it should, and whether the threshold should be
configurable, is [#803](https://github.com/salesforce-misc/trellis/issues/803).
