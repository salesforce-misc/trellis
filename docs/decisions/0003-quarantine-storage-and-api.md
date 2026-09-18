---
status: draft
date: 2026-08-20
deciders: Michael Ries
consulted: 
informed:
---

# Quarantine Storage and Error API

Because [data-flow](../data-flow.md) commits invalid source data before a
derivation runs, a failing transform or bad row must be **quarantined** rather
than block the write. This ADR settles the questions of where per-row status
lives, and what API applications use to discover and clear quarantines — plus a
guardrail for when quarantine itself is the wrong tool.

**Amendment (2026-09-12):** the original proposal below tracked one exception
per `(target_row_pk, transform_id)` and fused a whole transform at once. In
practice (see [ADR-0008](0008-public-api-design.md) for the discussion
that surfaced this) a single `TRANSFORM` can compute several calculated
columns, and a failure in one column's formula (a broken relationship, a
function that only errors for one field) shouldn't force every other healthy
column on that same transform into quarantine. This amendment adds a **column**
dimension throughout: exception detail is still one record per poisoned
*source* row for the existing whole-key fuse, but a failure attributable to
one column is now tracked separately (see "Exception table shape" below), and
the fuse trips per `(transform, column)` rather than per transform. A transform's
overall lifecycle status ([transforms — Status](../transforms.md#status)) is
unaffected by this — it still describes the whole keyspace — but a `live`
transform can now carry one or more individually `paused` columns without the
whole transform going `quarantined`. The sections below are updated in place;
nothing from the original proposal survives unchanged except the sparse-table
rationale in "Options considered".

**Implementation note (2026-09-13):** the column-fuse machinery described
below shipped in `V21__column_quarantine.sql` with one deliberate departure
from this ADR's literal text, decided during implementation review: rather
than a `poison.failures` jsonb array + GIN index, per-column failure detail
lives in its own dedicated `column_failures` table, and the column-status
table is keyed on `(transform_table, column_name)` rather than
`(transform_id, column_name)`. Reason: a row landing in `poison` at all means
"globally excluded from folding," which is correct for the whole-key fuse but
wrong for a column-only failure (a paused column freezes its value rather
than evicting the row from every other column/transform that still computes
cleanly) — so `poison.failures` would have been write-once, read-never
overhead with a real correctness footgun if anything had used it as an
eviction signal. `column_failures`/`column_deaths` (an incrementally
maintained counter, mirroring `key_deaths`) serve the "sample quarantined
rows" and fuse-threshold reads instead. All "Undecided" items below are now
settled; see "Amendment (2026-09-13): open questions resolved" at the end of
this document.

## Proposal

Track quarantine status in a **separate, sparse exception table**, populated
only for source rows that fail to propagate. A healthy row never appears, so
the common case costs nothing beyond the neighbor-table write it already
makes. This matches what's actually implemented (`poison`, `V13__quarantine.sql`)
rather than the original proposal's `(target_row_pk, transform_id)` keying: a
single *source* row's change can fan out to several downstream
transforms/columns at once, so the natural exception key is the source row
(`src_table`, `key`) that failed to fold, not any one of its downstream
targets. That row's exception record carries a `failures` array — one entry
per `(transform, column)` pair that failed while propagating this row, each
with its own error message — so one poisoned source row can implicate several
transforms/columns without needing several exception rows.

Expose this through three client-library reads (list what's currently
paused/quarantined across every transform and column, then page sample rows
for one, per the addressing scheme below) rather than the original's two — see
"Client library API". If poisoned-row counts for a given `(transform, column)`
pair cross a threshold, trip a **column-level fuse**: stop per-row tracking for
that column and mark it `paused` until a backfill clears it. A transform's
whole keyspace can still go `quarantined` (the original all-or-nothing fuse),
but that's now a coarser, separate tier from column pausing — see "Fuse" below
for how the two relate.

## Options considered

* **In the neighbor table.** Status columns beside the derived values.
  Cheapest to read (no join), but a target row can be written by several
  transforms, so avoiding conflated failures needs one column per transform —
  the table grows wider with every transform chained onto it.
* **A dense `(row, transform)` status table.** One row per key regardless of
  health, written alongside every neighbor-table write. Right granularity, but
  doubles write amplification on the hot path — at 180k+ source changes/sec,
  a second write for every successful row, not just failures.
* **Sparse exception table (chosen).** Dense-table shape, but only failing
  rows are inserted; a row's absence *is* its healthy status. Steady-state
  write cost stays near zero (>99% of rows never touch it) while still
  answering "is this row valid" and giving the error API a table to query.

The tradeoff: checking validity on read means checking for *absence*, not
reading a column in hand. We mitigate with the primary key on `(src_table,
key)` for the whole-key path, and `column_failures`' own primary key for the
column-grain path (see "Exception table shape"), and expect most consumers to
ask "is transform X (or column X.Y) quarantining anything" (via the API
below) rather than check rows inline.

## Exception table shape

One record per poisoned **source** row for the whole-key fuse — the existing
`poison` table (`src_table`, `key` primary key, `last_error text`) is
unchanged by this amendment.

**As shipped (2026-09-13), superseding this section's original proposal:**
rather than folding column-level failure detail into `poison` via a
`failures` jsonb array + GIN index, it lives in its own dedicated
`column_failures` table (`V21__column_quarantine.sql`), keyed on
`(transform_table, column_name, src_table, key)`: one row per
`(transform, column)` pair that failed while propagating a given source row,
with its own `error`/`failed_at`. See "Column status table" below for why —
in short, a row landing in `poison` means "globally excluded from folding,"
which is correct for the whole-key fuse but wrong for a column-only failure.

Retry-with-backoff vs. immediate quarantine, and whether older entries move to
a dead-letter area, are still open — this ADR fixes storage and the read APIs,
not retry policy.

## Column status table

Separate from the exception detail above: a small, dense table recording each
transform's currently-paused columns, so "is anything paused right now" is a
cheap read over a handful of rows rather than a scan/aggregate over per-row
failure detail. As shipped (`column_status`, `V21__column_quarantine.sql`):

* `transform_table`, `column_name` — primary key (keyed on the target table's
  name, not a surrogate `transform_id` — matching how `transform_definitions`
  itself is keyed).
* `paused_at`.
* `last_error` — the most recent failure's message, for a quick glance without
  paging exception detail.
* `local_fuse` — distinguishes *why* a row is paused: `true` means this
  `(transform, column)` pair's own fuse tripped; `false` means it's paused
  only because an upstream column it reads was paused (see "propagation" in
  the resolved open questions below). A column can be both at once.

A transform with no rows here has every column live. This is the table the
per-column fuse writes to when it trips, and clears from when a resume clears
the column — mirroring how the transform-wide `quarantined` lifecycle
state already works, just at column grain. It does **not** replace the
transform's own overall lifecycle status
([transforms — Status](../transforms.md#status)): a transform can be `live`
overall while this table lists one or more of its columns as paused.

## Client library API

Three calls now, still separated by cost so a caller can cheaply poll before
paying for detail:

1. **List paused/quarantined targets** — a flat list across every transform,
   each entry addressed as `transform` (the whole keyspace, from the
   transform-wide fuse/lifecycle) or `transform.column` (a single paused
   column, from the column status table above). Cheap — reads the column
   status table plus each transform's own lifecycle status, no join against
   exception detail. This is the one dashboards/health-checks poll.
2. **Status for one target** — given `transform` or `transform.column`, its
   current state (`live`/`paused`/`quarantined` as applicable) and, for a
   paused column, when it tripped and its last error.
3. **Sample quarantined rows** — for a `transform` or `transform.column`
   target, a paginated batch of `(src_table, key, error_message)` triples
   pulled from `poison` (whole-key target) or `column_failures`
   (`transform.column` target — see "Exception table shape"), to diagnose and
   clear the cause. `column_failures` rows for a column are cleared once it's
   resumed, not as each individual row re-evaluates cleanly — see "Fuse"
   below.

## Fuse: per-column, then transform-wide

Per-row quarantine assumes failures are the exception. When a large fraction of
one column's rows fail (a broken formula for that field, an incompatible
upstream schema change touching just the columns it reads), tracking each one
individually stops being useful — the application doesn't need a million
identical error rows, it needs to know that one column is broken.

When poisoned-row counts for a `(transform, column)` pair cross a threshold,
the **column fuse** trips: row-level tracking for that pair stops (its
`column_failures` rows are retained, not cleared, as the evidence base for
"sample quarantined rows" until the column is resumed), and the column is
marked `paused` in the column status table above. Resuming a single paused
column re-runs the backfill for just that column's formula against
already-built rows, without touching the rest of the transform's columns or
its overall lifecycle status.

The original **transform-wide fuse** still exists as a coarser, separate tier:
if a failure isn't attributable to one column (e.g. a key-shape/DDL failure
that dooms every column's write for that row alike), it trips the whole
transform to `quarantined` exactly as before, and resuming re-runs the full
backfill (see [data-flow](../data-flow.md)). `quarantined` remains one state of
a transform's broader **lifecycle status**
(`waiting_to_backfill` → `backfilling` → `live`, plus `quarantined`).

**Amendment (2026-09-13): open questions resolved.** The items below were
"Undecided" as of the previous amendment; each is now settled and shipped in
`V21__column_quarantine.sql`/`staging::quarantine`:

* **Threshold:** a fixed count, matching the existing row-level fuse's
  `DEFAULT_DEATH_THRESHOLD` pattern — not percentage-based, not configurable
  per transform/column (consistent with the row-level fuse's own threshold
  today).
* **Counter mechanism:** an incrementally-maintained counter table
  (`column_deaths`), the same `key_deaths`-style write-amplification
  tradeoff the row-level fuse already makes, rather than a live
  aggregate/GIN query over failure detail.
* **Escalation:** the two fuse tiers (per-column, transform-wide) stay fully
  independent — no auto-escalation. A transform can sit at "every column but
  one paused" indefinitely.
* **Paused-column value semantics:** freeze at the last successfully computed
  value — a paused column's target data is never nulled out. Staleness is
  discoverable via `column_status`/the client library's status read, not an
  in-band per-row marker.
* **Propagation:** tripping either fuse cascades the pause to every
  dependent/downstream transform reading the paused column's output
  (`column_pause_cascades`, `defs::catalog::column_dependents`) — a
  downstream reader never silently consumes a frozen/stale upstream value
  with no signal. Restricted to 1-1 downstream transforms only: an aggregate
  transform reading a paused upstream column is *not* cascaded into (aggregate
  accumulation has no per-column pause concept — see
  `staging::apply_aggregate`), which is a known, deliberate gap rather than an
  oversight.

**Amendment (2026-09-18): resuming a transform re-arms its fuse (issue
#160).** The transform-wide fuse counts the distinct evicted keys for a
source table — rows in `poison` — and `staging::quarantine::resume_transform`
originally left that table completely untouched, so a source that had ever
reached the threshold stayed at/above it forever and the *next single* new
eviction, for any key and any reason, re-quarantined the transform
immediately. "Five fresh failures re-trips" silently became "one failure
re-trips, forever, after the first trip."

Decided: a resume **re-arms** the fuse rather than erasing quarantine
history. `resume_transform` stamps `transform_definitions.fuse_rearmed_at`
(`V29__transform_fuse_rearm.sql`) in the same transaction as the status drop,
and `trip_transform_fuse_if_crossed` counts only `poison` rows evicted after
that instant — a full, fresh threshold's budget of *new* evictions. `null`
(never resumed) reads as `-infinity`, i.e. exactly the pre-#160 count.

The rejected alternative was deleting the source table's `poison`/`key_deaths`
rows on resume:

* `poison` is not just a counter, it is the **marker** the fold consults
  (`poisoned_keys_among`) to exclude a key globally, and each of its keys may
  own real parked work in `poison_held`. Only `release_key` knows how to
  replay that work (per key, with original origin positions preserved);
  deleting the marker without it would silently strand the parked changes.
* The fuse is keyed per source table, but a resume is per *transform*, and one
  source can back several transforms. Clearing the shared rows while resuming
  one of them would un-evict those keys out from under the siblings, which are
  not being re-backfilled and would lose their parked deltas for good.
* Keeping the rows keeps the operator-visible audit trail of what was ever
  evicted and why (`poisoned_at`/`last_error`) — the same reasoning
  `trip_column_fuse` already documents for not clearing `column_failures` when
  the column fuse trips.

The per-key (`key_deaths`) and per-column (`column_deaths`) tiers are
deliberately unaffected: they are cleared by a clean drain of the key itself
and by `resume_column` respectively, and a whole-transform resume makes no
claim about any individual key's or column's health.
