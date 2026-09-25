//! The claim-time fold (issue #10, stage 04): collapsing a sealed batch's
//! fenced window into one record per `(src_table, key)`, entirely in SQL —
//! ordered aggregates run in Postgres, not Rust-side aggregation. See
//! docs/staging-and-claiming/04-claiming-and-the-fold.md, "The claim-time
//! fold" and "The two kinds of missing image".
//!
//! Scope: this module produces folded records. It does not claim buckets
//! (that's `seg_claims`/`ON CONFLICT`, `super::claim`), does not compute
//! deltas or apply them (#11), and does not evaluate `f()` (a later,
//! Rust-side concern). `BucketFilter` exists so the fold is
//! bucket-parameterizable; `super::claim::owned_bucket_filter` is what
//! turns a real claim's buckets into one, but nothing in *this* module
//! populates buckets itself — [`BucketFilter::all`] is the whole unsplit
//! batch.

use std::collections::HashMap;
use std::time::SystemTime;

use tokio_postgres::Transaction;
use tokio_postgres::types::{PgLsn, ToSql};

use super::error::StagingError;
use super::seal::fenced_window;

/// Sorts the fold's ordered aggregates in memory rather than spilling to
/// disk on a large batch (docs/.../04-claiming-and-the-fold.md, "Practical
/// notes on the fold"). `LOCAL` scopes it to the caller's transaction —
/// [`fold`] takes a `Transaction` rather than any `GenericClient` precisely
/// so this can't evaporate before the query below it runs.
const FOLD_WORK_MEM: &str = "SET LOCAL work_mem = '64MB'";

/// `route % bucket_count = ANY(buckets)` — the **one** bucket definition,
/// applied inside the fenced window so a key never folds across a boundary
/// its worker doesn't own (docs/.../04-claiming-and-the-fold.md, "Partitioning
/// a batch across workers"). This struct carries no claim state; it is pure
/// SQL parameterization. [`BucketFilter::all`] selects the whole batch
/// (`bucket_count = 1`, `buckets = [0]`), which is the only constructor this
/// stage needs — the claim machinery that would populate real buckets is
/// #14/#15, not here.
#[derive(Debug, Clone)]
pub struct BucketFilter {
    bucket_count: i64,
    buckets: Vec<i64>,
}

impl BucketFilter {
    /// The whole (single-bucket) batch: `route % 1 = 0` is trivially true
    /// for every row.
    #[cfg(any(test, feature = "internals"))]
    pub fn all() -> Self {
        Self {
            bucket_count: 1,
            buckets: vec![0],
        }
    }

    /// A named subset of buckets out of `bucket_count` — what
    /// `super::claim::owned_bucket_filter` hands the fold, built from a
    /// real claim's `seg_claims` rows. Nothing in this module populates
    /// buckets; this constructor only lets a caller (or a test) express
    /// "restrict to these buckets" against the one SQL bucket definition
    /// below, rather than reimplementing `route % bucket_count` in Rust.
    pub fn buckets(bucket_count: i64, buckets: Vec<i64>) -> Self {
        Self {
            bucket_count,
            buckets,
        }
    }

    /// Whether this filter names no buckets at all — the signal
    /// [`super::apply::drain_once`] uses to short-circuit a claim attempt
    /// that won (and owns) nothing, before folding or computing anything.
    pub fn is_empty(&self) -> bool {
        self.buckets.is_empty()
    }
}

/// One key's folded record: the fenced window's `(src_table, key)` group
/// collapsed to the seven fold outputs the doc's four-rule table (plus
/// `lsn`/`hop_gen`/`first_seen`) specifies. Images cross the wire as text —
/// see `append.rs`'s doc comment on why this crate binds jsonb via
/// `::text` rather than a `serde_json::Value` `FromSql`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FoldedChange {
    pub src_table: String,
    pub key: String,
    /// LAST image-bearing row's post-image, by `(lsn, change_id)` — highest.
    pub new_image: Option<String>,
    /// FIRST image-bearing row's pre-image, by `(lsn, change_id)` — lowest.
    /// A key born inside the batch (insert-then-update) folds this to
    /// `None`: the insert has no pre-image, and that's a fact about the
    /// change, not a missing value to paper over with `COALESCE`.
    pub old_image: Option<String>,
    /// Whether the record is a source change, carried as the latest
    /// non-null `src_changed` timestamp among the group's rows — `Some`
    /// iff *any* row is a source change (an `OR`, expressed via `max`,
    /// which is `NULL` exactly when every input is `NULL`), `None` iff
    /// none are. Pinned representation: the struct has one
    /// `Option<SystemTime>` field, not a separate bool + timestamp, per
    /// the issue's field list.
    pub src_changed: Option<SystemTime>,
    /// LEAST `origin_lsn` over the group, or `None` (unknown) if any row's
    /// is unknown (issue #469): see [`earliest_origin`].
    pub origin_lsn: Option<PgLsn>,
    /// GREATEST `lsn` over *every* row in the group, image-less rows
    /// included, so the watermark still covers them.
    pub lsn: Option<PgLsn>,
    /// Issue #321: LEAST `lsn` over the group's image-bearing rows only (the
    /// same filter the image arg-extremes use), `None` when there are none.
    /// A folded delta telescopes every source commit from this one up to
    /// `lsn`, so this is the commit an aggregate's recompute horizon has to
    /// be compared against: if it is at or below a group's
    /// `__trellis_recompute_lsn`, a forced recompute may already have counted
    /// part of this delta, and `apply_aggregate::apply_aggregate_target`
    /// re-derives the group instead of applying it. Recompute rows carry a
    /// NULL `lsn`, so they never pull this down.
    pub min_image_lsn: Option<PgLsn>,
    /// Reset to 0 if any row in the group is a source change. Otherwise —
    /// underspecified by the doc for the all-non-source case — pinned to
    /// `MAX(hop_gen)`: doc 05 describes `hop_gen` as a schema-derived bound
    /// on propagation depth ("one past its deepest trigger"), checked
    /// against the graph's cross-table depth to catch runaway propagation.
    /// Folding to the *shallowest* row instead (MIN, or an arbitrary carry)
    /// would under-report depth and could mask a wave that should have
    /// tripped the bound — MAX is the conservative choice that cannot
    /// itself cause a missed-detection of infinite propagation. FLAG:
    /// confirm this against #11/the hop-bound check once it exists.
    pub hop_gen: i32,
    /// `min(appended_at)` per key — the end-to-end latency origin, per-row
    /// rather than the batch's creation timestamp (see doc: an idle active
    /// batch's age is unbounded).
    pub first_seen: SystemTime,
    /// Issue #133: the real union of every raw row's `group_key` array in
    /// this `(src_table, key)` group — every join-key value any of the
    /// group's rows' own `old_image`/`new_image` touched for some
    /// relationship's `from_col`, deduplicated, with nulls filtered. Unlike
    /// `new_image`/`old_image` (arg-extremes over the group, picking one
    /// row's value), this is a genuine set union across *every* row, which
    /// is exactly what makes it survive the fold's own "first old image,
    /// last new image" collapse: a from-side row inserted and then
    /// re-pointed within one batch (`ins(post 3)` + `repoint(3 -> 2)`)
    /// folds `new_image`/`old_image` to endpoints that name only post 2 —
    /// post 3 never appears in either — but this field still carries `{3,
    /// 2}`, because it unions the insert's own touched value (3) with the
    /// update's (3 and 2), not just the two folded endpoints. See
    /// `staging::apply::RelationshipGenBump`'s doc comment for why guard
    /// (b) reads this rather than the folded images. (An earlier version of
    /// this field picked one arbitrary non-null value by append order —
    /// "the doc's keep-it-simple choice" — which is exactly the placeholder
    /// #133 replaces: it silently dropped every touched value but one,
    /// which is precisely the erased-parent bug above.)
    pub group_key: Option<Vec<String>>,
    /// Whether any row in this group is a truncate sentinel (`bool_or(op =
    /// 'truncate')`) — issue #60. In practice only the sentinel-key group
    /// (see `append::TRUNCATE_SENTINEL_KEY`) is ever `true`: no real key's
    /// rows ever carry `op = 'truncate'`. `compute` (`staging::apply`) uses
    /// this to find, per drain, which `src_table`s were truncated, without
    /// filtering `op` anywhere upstream of this one flag.
    pub is_truncate: bool,
    /// Issue #134: this key's `relationship_definitions.id`, present iff at
    /// least one row in this group carries `op = 'rel_reverse_deferred'` — a
    /// previously guard-rejected to-one relationship reverse, ready to
    /// retry. Every row that can contribute to one group necessarily shares
    /// the same relationship id (the synthetic `src_table` this op uses,
    /// `staging::apply::relationship_reverse_deferred_src_table`, embeds it,
    /// so two different relationships' rows never land in the same fold
    /// group at all) — mirrors `is_truncate`'s discriminator role: `compute`
    /// (`staging::apply`) filters a group with this set out of the ordinary
    /// per-source forward-evaluation loop entirely (same as a truncate
    /// sentinel) and reconstructs a fresh `RelationshipReverseRecord` from
    /// it instead, re-deriving guard state live rather than from anything
    /// this struct carries.
    pub relationship_reverse_deferred: Option<i64>,
    /// Issue #134: `MAX(retry_count)` across the group's
    /// `rel_reverse_deferred` rows — mirrors `hop_gen`'s own MAX-across-
    /// the-group fold rule (the same rationale applies: folding to the
    /// larger count can't itself cause an under-report of how many times
    /// this reverse has been deferred). `0` when
    /// `relationship_reverse_deferred` is `None`.
    pub retry_count: i32,
    /// Issue #315: the FIRST (lowest `(lsn, change_id)`) prior image any of
    /// the group's `recompute` rows carried — see
    /// `StagedChange::Recompute::prior_image`. Kept apart from `old_image`/
    /// `new_image`: a hinted recompute is still image-less (the fold's
    /// image-bearing filter excludes `op = 'recompute'`), so it never wins
    /// either image arg-extreme and is still re-read live. Only
    /// `staging::target_mutations` produces these, for a target table's key.
    /// The first one is the state downstream consumers last saw before this
    /// batch's writes, which is the group a downstream aggregate must also
    /// re-derive.
    pub prior_image: Option<String>,
    /// Issue #409: how many ring rows folded into this record — `count(*)`
    /// over the group, after the truncate-void filter (a row a later
    /// truncate voided was never applied, so it isn't counted). This is the
    /// unit `trellis_changes_applied_total` counts in: one change per staged
    /// row, whether intake staged it from logical replication or an upstream
    /// hop's target write staged it as a `recompute`. A truncate sentinel is
    /// one row like any other, not the number of rows it erased. Always at
    /// least 1 for a record [`fold`] returns; [`merge_folded_changes`] sums
    /// it across segments.
    pub row_count: u64,
    /// Issue #392: whether any row in this group is a `recompute`
    /// (`bool_or(op = 'recompute')`). A `recompute` never wins an image
    /// arg-extreme, so when it folds with the key's CDC change the record
    /// carries that change's images and looks like a plain delta. This flag
    /// keeps the recompute's intent: an aggregate re-derives every group the
    /// record names rather than applying the delta, which would land on
    /// whatever stale value the recompute was staged to repair.
    pub has_recompute: bool,
    /// Issue #486: when the key was born and died inside this batch, so the
    /// fold's first-old/last-new images are both `None` even though it
    /// staged image-bearing rows, one insert's post-image and one delete's
    /// pre-image (deduplicated). Empty otherwise.
    ///
    /// Such a record's delta is zero, which is right unless a forced
    /// recompute counted the row in between: an insert at or below a
    /// group's recompute horizon and a delete above it. Without an image the
    /// record names no group, so the horizon check never runs and the group
    /// keeps the row. These images name the groups it was born into and
    /// died from. An aggregate checks each against its horizon
    /// (`apply_aggregate::GroupPlan::horizon_check_only`) and re-derives it
    /// only when the check says so (otherwise it only probes that a group
    /// with a row still exists). A key that also moved between groups more
    /// than once inside the batch is still only named by these two.
    ///
    /// Only the no-image case carries them, so the wire cost falls on
    /// born-and-died keys alone.
    pub vanished_images: Vec<String>,
}

/// The fenced window's full column projection the fold needs, with jsonb
/// images cast to text at the source (this crate has no `serde_json`
/// dependency — see `append.rs`). `route` rides along so the bucket filter
/// can be applied to the union of both slots' rows, not to one slot alone.
/// `op` rides along too — issue #60's truncate-void filter and `is_truncate`
/// both need it, even though the fold otherwise deliberately never filters
/// on `op` (see the discriminator comment below).
const FOLD_COLUMNS: &str = "src_table, key, old_image::text as old_image, \
     new_image::text as new_image, lsn, origin_lsn, src_changed, hop_gen, \
     group_key, appended_at, change_id, route, op, relationship_id, retry_count";

/// Runs the claim-time fold over `seg_seq`'s fenced window, restricted to
/// `bucket`. One [`FoldedChange`] per `(src_table, key)` present in that
/// window. See the module doc and docs/.../04-claiming-and-the-fold.md for
/// the rules this SQL encodes.
///
/// Takes a `&Transaction` rather than any `GenericClient`: the fold is meant
/// to run inside the claim's transaction, atomically with the apply (#11),
/// and the type enforces that rather than relying on a caller to remember
/// it — `SET LOCAL work_mem` would otherwise silently evaporate on a bare
/// autocommit `Client`.
pub async fn fold(
    txn: &Transaction<'_>,
    seg_seq: i64,
    bucket: BucketFilter,
) -> Result<Vec<FoldedChange>, StagingError> {
    txn.batch_execute(FOLD_WORK_MEM).await?;

    let (window_sql, fence_params) = fenced_window(txn, seg_seq, FOLD_COLUMNS).await?;
    let bucket_count_idx = fence_params.len() + 1;
    let buckets_idx = fence_params.len() + 2;

    // The discriminator (docs/.../04-claiming-and-the-fold.md, "The two
    // kinds of missing image"): "does this row carry any image at all", not
    // "is this image column null". Scoping both arg-extremes to
    // `image_bearing` rows is what lets a born-in-batch insert's honest
    // `old_image IS NULL` survive, while a bare recompute trigger (both
    // images NULL) never wins either extreme. `op` is deliberately not
    // filtered anywhere here as a row-level WHERE — the `truncate` sentinel
    // is image-less but load-bearing, and filtering `op` would fold it away.
    // Issue #315: a `recompute` row is never image-bearing, even when its
    // `old_image` column holds a prior-image hint (`StagedChange::Recompute::prior_image`);
    // that hint is aggregated separately into `prior_image`.
    //
    // Both `new_image` (LAST, highest `(lsn, change_id)`) and `old_image`
    // (FIRST, lowest) use the `array_agg(... ORDER BY ...) FILTER (...))[1]`
    // idiom for a grouped arg-extreme: ORDER BY sorts the (value, sort-key)
    // pairs together, so `[1]` after filtering is the image from the actual
    // highest/lowest-ordered row, not an independently-sorted value.
    //
    // The one exception to "never filter on `op`" is the truncate-void
    // filter below (issue #60): a truncate is whole-keyspace, but the fold
    // is per-key, so a key's image-bearing row landing *at or below* a
    // truncate for its own `src_table` in this fenced window is stale — the
    // truncate erased whatever it recorded — and must not survive into the
    // group at all, not just lose an arg-extreme. `(t.lsn, t.change_id) >
    // (f.lsn, f.change_id)` is the "strictly above" test; a truncate and an
    // insert in the *same* source transaction share one commit `lsn`, so
    // `change_id` (intake's append order = execution order) is what orders
    // a same-transaction truncate-then-insert correctly.
    //
    // Load-bearing subtlety: a recompute row's `lsn` is NULL, and Postgres's
    // row-comparison against a NULL component evaluates to NULL, not TRUE —
    // so `f`'s own EXISTS test above is not satisfied by *any* truncate for a
    // recompute row, regardless of the truncate's position, and the
    // recompute survives unconditionally. That is correct: a recompute
    // re-reads *live* current source state (already reflecting the
    // truncate) when it's later evaluated, so its position relative to the
    // truncate is irrelevant — only image-bearing rows carry a stale
    // snapshot that must be voided. Referencing the un-bucket-filtered
    // `fenced` CTE (not `filtered`) matters too: a truncate sentinel's own
    // key (the sentinel) could in principle route to a different bucket
    // than the keys it voids, though in practice every truncate-bearing
    // batch seals with `bucket_count = 1` (see `seal::seal_phase1`), making
    // that moot today.
    // Issue #133: `group_key` is a real per-key set union, computed
    // separately from every other column here so it can't perturb them.
    // `group_keys` unnests each raw row's own `group_key` array (a row with
    // no array at all coalesces to `array[]`, so it contributes nothing and
    // drops out of the join) and re-aggregates with `array_agg(distinct
    // ...)`, which is what actually merges/dedups the union rather than
    // picking one row's value — the placeholder this replaces used the same
    // `array_agg(... order by ...) filter (...))[1]` arg-extreme idiom the
    // image columns use, which is correct for "the value from one specific
    // row" but wrong for "everything any row touched." Aggregating this in
    // its own CTE (rather than joining the unnested rows straight into the
    // outer `group by`) matters: cross-joining `filtered` against
    // `unnest(group_key)` multiplies a row with an N-element array into N
    // output rows, which would corrupt every *other* aggregate below
    // (`min`/`max`/the image arg-extremes) by feeding them duplicated rows.
    // `group_keys` collapses back to one row per key before it's ever
    // joined against `filtered`, so the outer query's own row multiplicity
    // — and therefore every other column's aggregate — is untouched.
    //
    // Issues #392 and #486 add two columns without adding a sort or a pass.
    // `has_recompute` is a plain `bool_or`. `vanished_images` repeats the two
    // image arg-extremes verbatim so the planner shares them rather than
    // computing them twice, and is non-null only when both come out NULL.
    // Its two candidates are unordered `max`es under `"C"` collation (a
    // byte compare, only reached when one key has several inserts or
    // deletes), filtered to insert-shaped and delete-shaped rows so an
    // update, the common row, never feeds them.
    let sql = format!(
        "with fenced as ({window_sql}), \
         filtered as ( \
             select * from fenced f \
             where route % ${bucket_count_idx}::bigint = any(${buckets_idx}::bigint[]) \
               and not exists ( \
                   select 1 from fenced t \
                   where t.op = 'truncate' and t.src_table = f.src_table \
                     and (t.lsn, t.change_id) > (f.lsn, f.change_id) \
               ) \
         ), \
         group_keys as ( \
             select src_table, key, \
                    array_agg(distinct gk) filter (where gk is not null) as group_key \
             from filtered, unnest(coalesce(group_key, array[]::text[])) as gk \
             group by src_table, key \
         ) \
         select \
             filtered.src_table, \
             filtered.key, \
             (array_agg(new_image order by lsn desc, change_id desc) \
                 filter (where (old_image is not null or new_image is not null) \
                           and op <> 'recompute'))[1] as new_image, \
             (array_agg(old_image order by lsn asc, change_id asc) \
                 filter (where (old_image is not null or new_image is not null) \
                           and op <> 'recompute'))[1] as old_image, \
             max(src_changed) as src_changed, \
             case when bool_or(origin_lsn is null) then null \
                  else min(origin_lsn) end as origin_lsn, \
             max(lsn) as lsn, \
             case when bool_or(src_changed is not null) then 0 else max(hop_gen) end as hop_gen, \
             min(appended_at) as first_seen, \
             group_keys.group_key, \
             bool_or(op = 'truncate') as is_truncate, \
             max(relationship_id) filter (where op = 'rel_reverse_deferred') \
                 as relationship_reverse_deferred, \
             coalesce(max(retry_count) filter (where op = 'rel_reverse_deferred'), 0) \
                 as retry_count, \
             (array_agg(old_image order by lsn asc, change_id asc) \
                 filter (where op = 'recompute' and old_image is not null))[1] as prior_image, \
             min(lsn) filter (where (old_image is not null or new_image is not null) \
                                and op <> 'recompute') as min_image_lsn, \
             count(*) as row_count, \
             bool_or(op = 'recompute') as has_recompute, \
             case when (array_agg(new_image order by lsn desc, change_id desc) \
                           filter (where (old_image is not null or new_image is not null) \
                                     and op <> 'recompute'))[1] is null \
                   and (array_agg(old_image order by lsn asc, change_id asc) \
                           filter (where (old_image is not null or new_image is not null) \
                                     and op <> 'recompute'))[1] is null \
                  then array_remove(array[ \
                       max(new_image collate \"C\") \
                           filter (where old_image is null and new_image is not null \
                                     and op <> 'recompute'), \
                       max(old_image collate \"C\") \
                           filter (where new_image is null and old_image is not null \
                                     and op <> 'recompute')], null) \
             end as vanished_images \
         from filtered \
         left join group_keys \
             on group_keys.src_table = filtered.src_table and group_keys.key = filtered.key \
         group by filtered.src_table, filtered.key, group_keys.group_key"
    );

    let mut params: Vec<&(dyn ToSql + Sync)> = fence_params
        .iter()
        .map(|p| p as &(dyn ToSql + Sync))
        .collect();
    params.push(&bucket.bucket_count);
    params.push(&bucket.buckets);

    let rows = txn.query(&sql, &params).await?;
    Ok(rows
        .into_iter()
        .map(|row| FoldedChange {
            src_table: row.get(0),
            key: row.get(1),
            new_image: row.get(2),
            old_image: row.get(3),
            src_changed: row.get(4),
            origin_lsn: row.get(5),
            lsn: row.get(6),
            hop_gen: row.get(7),
            first_seen: row.get(8),
            group_key: row.get(9),
            is_truncate: row.get(10),
            relationship_reverse_deferred: row.get(11),
            retry_count: row.get(12),
            prior_image: row.get(13),
            min_image_lsn: row.get(14),
            // `count(*)` is a non-negative bigint.
            row_count: row.get::<_, i64>(15) as u64,
            has_recompute: row.get(16),
            vanished_images: {
                // A key born and died in one group has one image, twice.
                let mut images = row.get::<_, Option<Vec<String>>>(17).unwrap_or_default();
                images.dedup();
                images
            },
        })
        .collect())
}

/// Merges several segments' already-folded change lists — issue #63
/// Milestone 2's segment-coalescing seam. Each element of `per_segment` is
/// one segment's own [`fold`] output, already unique per `(src_table,
/// key)`; `per_segment` itself must be ordered **ascending by seg_seq**
/// (oldest segment first), since [`merge_pair`] assumes its `earlier`
/// argument really did seal before its `later` one.
///
/// A key touched in only one segment passes through unchanged. A key
/// touched in more than one segment is combined via [`merge_pair`], applied
/// left-to-right in seal order — exactly the same reduction [`fold`]'s own
/// `array_agg(... order by lsn, change_id)` arg-extremes compute for one
/// segment's raw rows, just run here over already-reduced per-segment
/// records instead of raw ones. Deduplicating by key here, before the
/// combined list ever reaches [`super::apply::compute`], matters beyond
/// bookkeeping: [`super::apply::apply_target`]'s upsert binds every write in
/// one `INSERT ... ON CONFLICT` statement, and Postgres rejects a statement
/// that would update the same conflict target row twice
/// (`ON CONFLICT DO UPDATE command cannot affect row a second time`) — so
/// coalescing segments without this merge would make any key touched by
/// more than one of them fail outright instead of silently misapplying.
pub fn merge_folded_changes(per_segment: Vec<Vec<FoldedChange>>) -> Vec<FoldedChange> {
    let mut merged: Vec<FoldedChange> = Vec::new();
    let mut index: HashMap<(String, String), usize> = HashMap::new();
    for segment in per_segment {
        for change in segment {
            let dedup_key = (change.src_table.clone(), change.key.clone());
            match index.get(&dedup_key) {
                Some(&i) => {
                    let earlier = std::mem::replace(&mut merged[i], change.clone());
                    merged[i] = merge_pair(earlier, change);
                }
                None => {
                    index.insert(dedup_key, merged.len());
                    merged.push(change);
                }
            }
        }
    }
    merged
}

/// Combines two [`FoldedChange`] records for the same `(src_table, key)`,
/// one from an earlier-sealed segment and one from a later one, into the
/// record [`fold`] would have produced had both segments' underlying rows
/// been folded together in one pass. Field-by-field, mirroring [`fold`]'s
/// own SQL rules (see [`FoldedChange`]'s doc comments):
///
/// - `new_image`/`old_image`: whichever side actually carries image
///   evidence (either image field set) wins its half — `later` for the
///   post-image (its window is strictly the more recent), `earlier` for the
///   pre-image. A side with neither image set contributed no image
///   information at all (a bare recompute trigger folded alone), so it
///   defers entirely to the other side rather than overwriting real
///   evidence with `None`. **Exception, issue #134 review follow-up**: for
///   a `relationship_reverse_deferred` record, `earlier`/`later` segment
///   order does *not* approximate chronological order (a retry's segment
///   reflects when it was *re-staged*, not the underlying parent
///   transition's own `lsn`) — see this function's own inline comment at
///   the branch that handles it, which orders by each side's own `lsn`
///   instead.
/// - `src_changed`/`lsn`: `Option::max` — `None` sorts below every `Some`,
///   and among two `Some`s the greater watermark/timestamp wins, matching
///   "OR across the group" and "GREATEST over every row" respectively.
/// - `origin_lsn`: the lesser of the two, where a missing side is unknown
///   and wins ([`earliest_origin`], issue #469).
/// - `min_image_lsn`: the lesser of the two, ignoring a missing side —
///   `Option::min` would wrongly let a `None` beat a real `Some` (a bare
///   recompute trigger folded alone has no `min_image_lsn`).
/// - `hop_gen`: 0 if the merged `src_changed` is `Some` (a source change
///   resets propagation depth), else the greater of the two hop generations.
/// - `first_seen`: the earlier of the two — first append into either
///   segment.
/// - `group_key`: issue #133's real set union — every distinct value
///   present in either side's array, deduplicated (see
///   [`merge_group_keys`]) — mirroring [`fold`]'s own `array_agg(distinct
///   ...)` union rule rather than the earlier "first non-null side wins"
///   placeholder this replaces (which would have silently dropped
///   `later`'s touched values whenever `earlier` had any at all).
/// - `is_truncate`: OR. In practice always `false` here: the batching layer
///   that builds `per_segment` never coalesces a truncate-bearing segment
///   with any other (a truncate is its own drain barrier — see
///   `apply::next_claimable_segments`), so no truncate row can reach this
///   function paired with anything. Kept as a real OR anyway rather than
///   assumed away, so a future caller that breaks that invariant fails
///   toward "still marked as a truncate" rather than toward silently
///   dropping one.
/// - `relationship_reverse_deferred`: issue #134 — whichever side carries
///   one (`Option::or`); both sides carrying different values would mean two
///   different relationships' rows landed in the same `(src_table, key)`
///   group, which the synthetic per-relationship `src_table` this op uses
///   (`apply::relationship_reverse_deferred_src_table`) makes impossible by
///   construction, so this is "propagate the one real value forward,"
///   exactly like `group_key`'s missing-side convention.
/// - `retry_count`: `MAX` across the two sides, mirroring `hop_gen`'s own
///   cross-segment `MAX` rule and [`fold`]'s SQL `MAX(retry_count)`.
/// - `row_count`: the sum — [`fold`]'s `count(*)` over both segments' rows
///   (issue #409).
/// - `has_recompute`: OR, as [`fold`]'s `bool_or` (issue #392).
/// - `vanished_images`: the union of both sides', plus, when the merged
///   record has no image at all, the two images the merge itself dropped
///   (issue #486; see the inline comment).
fn merge_pair(earlier: FoldedChange, later: FoldedChange) -> FoldedChange {
    let src_changed = earlier.src_changed.max(later.src_changed);
    let hop_gen = if src_changed.is_some() {
        0
    } else {
        earlier.hop_gen.max(later.hop_gen)
    };
    let origin_lsn = earliest_origin(earlier.origin_lsn, later.origin_lsn);
    let relationship_reverse_deferred = earlier
        .relationship_reverse_deferred
        .or(later.relationship_reverse_deferred);

    // Issue #134 review follow-up — CRITICAL: `relationship_reverse_deferred`
    // rows are the one op where "earlier-sealed segment" does *not*
    // approximate "chronologically earlier transition" the way it does for
    // every other op this function merges. Ordinary CDC's own `lsn` is
    // stamped at intake time and a row's *segment* is essentially "whichever
    // one happened to be active when its source committed" — the two are
    // naturally correlated, which is what makes the `earlier`/`later`
    // segment-order convention below a sound proxy for chronological order
    // in the general case. A deferred reverse's *segment* instead reflects
    // *when it was last re-staged after a rejection* — entirely decoupled
    // from the underlying parent transition's own `lsn` (issue #134's own
    // resolved "re-derive fresh" design: a retry is staged into whatever
    // segment is active *at retry time*, which can be arbitrarily later
    // than another, unrelated reverse's own retry for an *older* parent
    // transition). Two deferred reverses for the same relationship+parent
    // key can therefore reach this function with `earlier.lsn >
    // later.lsn` — an inversion the segment-order convention below would
    // silently get backwards: it would pick `earlier`'s (chronologically
    // *newer*) `old_image` and `later`'s (chronologically *older*)
    // `new_image`, producing an internally-inconsistent image pair.
    //
    // **This is why this branch exists as its own case, not just a
    // documentation note**: an inconsistent pair would corrupt not only a
    // hypothetical direct `diff_pass` application (already blocked, today,
    // by the *unrelated* fact that `retry_count` — see below — is always
    // `>= 1` for a merged deferred record, which forces
    // `apply::apply_and_mark_drained_many`'s own `retry_count == 0`
    // fast-path gate to route every deferred reverse to the recompute
    // fallback regardless) but *also*
    // `apply::apply_projection_advance`'s write of the settled parent
    // projection's own data columns from `new_image` — which runs
    // unconditionally once guards pass, on *both* the fast and fallback
    // paths, and is not protected by the `retry_count` coincidence at all.
    // Fixed at the source instead of relying on that coincidence: order by
    // each side's own `lsn` (`None` sorts first, matching this crate's
    // "unknown sorts as earliest" convention elsewhere), and pick images by
    // *that* order, not by which segment sealed first.
    //
    // `#135`/`#136`: if a future change ever lets a deferred reverse regain
    // eligibility for the fast path (lifting the `retry_count == 0` gate –
    // see that gate's own doc comment on `force_every_group` for when that
    // could happen), this branch is what keeps folding sound regardless —
    // it does not depend on that gate staying in place, unlike the
    // `retry_count` coincidence above.
    let (old_image, new_image) = if relationship_reverse_deferred.is_some() {
        let (chronologically_earlier, chronologically_later) = if earlier.lsn <= later.lsn {
            (&earlier, &later)
        } else {
            (&later, &earlier)
        };
        (
            chronologically_earlier
                .old_image
                .clone()
                .or_else(|| chronologically_later.old_image.clone()),
            chronologically_later
                .new_image
                .clone()
                .or_else(|| chronologically_earlier.new_image.clone()),
        )
    } else {
        // Every other op: segment-sealed order is the sound proxy — see
        // this function's own doc comment. Whichever side actually carries
        // image evidence (either image field set) wins its half; a side
        // with neither set contributed no image information at all (a bare
        // recompute trigger folded alone), so it defers entirely to the
        // other side rather than overwriting real evidence with `None`.
        let earlier_has_image = earlier.old_image.is_some() || earlier.new_image.is_some();
        let later_has_image = later.old_image.is_some() || later.new_image.is_some();
        (
            if earlier_has_image {
                earlier.old_image.clone()
            } else {
                later.old_image.clone()
            },
            if later_has_image {
                later.new_image.clone()
            } else {
                earlier.new_image.clone()
            },
        )
    };

    // Issue #486: the same "born and died" loss can happen across segments.
    // An insert sealed into `earlier` and a delete into `later` each fold to
    // a record with an image, but the merge drops the insert's post-image
    // and the delete's pre-image, the state between the two segments. When
    // that leaves no image at all, those two join whatever either side
    // already carried, so the group they name is still checked against its
    // horizon.
    let mut vanished_images = earlier.vanished_images;
    vanished_images.extend(later.vanished_images);
    if old_image.is_none() && new_image.is_none() {
        vanished_images.extend(earlier.new_image.clone());
        vanished_images.extend(later.old_image.clone());
    }
    vanished_images.sort_unstable();
    vanished_images.dedup();

    FoldedChange {
        src_table: earlier.src_table,
        key: earlier.key,
        new_image,
        old_image,
        src_changed,
        origin_lsn,
        lsn: earlier.lsn.max(later.lsn),
        min_image_lsn: least_present(earlier.min_image_lsn, later.min_image_lsn),
        hop_gen,
        first_seen: earlier.first_seen.min(later.first_seen),
        group_key: merge_group_keys(earlier.group_key, later.group_key),
        is_truncate: earlier.is_truncate || later.is_truncate,
        relationship_reverse_deferred,
        // `MAX` across the two sides, mirroring `hop_gen`'s own
        // cross-segment `MAX` rule and `fold`'s own SQL `MAX(retry_count)`
        // — see this field's doc comment on `FoldedChange` for why `MAX`
        // (not, say, sum) is the right merge here, and why every merged
        // deferred record is therefore guaranteed `retry_count >= 1`.
        retry_count: earlier.retry_count.max(later.retry_count),
        // The earlier segment's hint, when it has one: it predates the later
        // segment's writes, the same "first prior image wins" rule the SQL
        // fold applies within one segment.
        prior_image: earlier.prior_image.or(later.prior_image),
        row_count: earlier.row_count + later.row_count,
        has_recompute: earlier.has_recompute || later.has_recompute,
        vanished_images,
    }
}

/// The lesser of two optional LSNs, ignoring a missing side: SQL `min()`'s
/// rule, which `Option::min` gets wrong (it lets `None` win).
/// Merges two folded `origin_lsn`s the way [`fold`]'s SQL does: the earlier
/// origin, where a missing one is *unknown*, not absent. `converged_through`
/// reads an unknown origin as older than any token (issue #469), so a key
/// that folded any row without an origin, such as a backfill `Recompute`,
/// must keep gating every token, not only those past its other rows' origin.
pub(crate) fn earliest_origin(a: Option<PgLsn>, b: Option<PgLsn>) -> Option<PgLsn> {
    Some(a?.min(b?))
}

/// A running [`earliest_origin`] that can start empty. An accumulator can't
/// start at `None`, since `None` means unknown and would win every merge;
/// this keeps "nothing contributed yet" apart from "a contribution of
/// unknown origin".
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub(crate) enum OriginAccum {
    #[default]
    Empty,
    Merged(Option<PgLsn>),
}

impl OriginAccum {
    pub(crate) fn add(&mut self, origin_lsn: Option<PgLsn>) {
        *self = OriginAccum::Merged(match *self {
            OriginAccum::Empty => origin_lsn,
            OriginAccum::Merged(current) => earliest_origin(current, origin_lsn),
        });
    }

    /// The merged origin; unknown (`None`) if nothing contributed, which
    /// gates every token.
    pub(crate) fn get(self) -> Option<PgLsn> {
        match self {
            OriginAccum::Empty => None,
            OriginAccum::Merged(origin_lsn) => origin_lsn,
        }
    }
}

fn least_present(a: Option<PgLsn>, b: Option<PgLsn>) -> Option<PgLsn> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    }
}

/// Issue #133's cross-segment counterpart to [`fold`]'s own SQL
/// `array_agg(distinct ...)` union: a real set union of two segments'
/// already-per-key `group_key` arrays, deduplicated and sorted (matching
/// `array_agg(distinct ...)`'s own stable output — Postgres sorts a
/// `DISTINCT` aggregate's input — so a segment-coalesced merge can't be
/// told apart from a single-pass SQL fold by element order alone), `None`
/// only when neither side has one. `None` sorts as "contributed nothing"
/// (not as its own distinct value), matching every other field's
/// `None`-means-missing convention in this module.
fn merge_group_keys(a: Option<Vec<String>>, b: Option<Vec<String>>) -> Option<Vec<String>> {
    let mut merged = match (a, b) {
        (None, None) => return None,
        (Some(only), None) | (None, Some(only)) => only,
        (Some(mut a), Some(b)) => {
            for value in b {
                if !a.contains(&value) {
                    a.push(value);
                }
            }
            a
        }
    };
    merged.sort_unstable();
    Some(merged)
}

#[cfg(test)]
mod merge_tests {
    use std::time::Duration;

    use super::*;

    fn base(key: &str) -> FoldedChange {
        FoldedChange {
            src_table: "orders".to_string(),
            key: key.to_string(),
            new_image: None,
            old_image: None,
            src_changed: None,
            origin_lsn: None,
            lsn: None,
            min_image_lsn: None,
            hop_gen: 0,
            first_seen: SystemTime::UNIX_EPOCH,
            group_key: None,
            is_truncate: false,
            relationship_reverse_deferred: None,
            retry_count: 0,
            prior_image: None,
            row_count: 1,
            has_recompute: false,
            vanished_images: Vec::new(),
        }
    }

    /// A key touched in only one of the coalesced segments passes straight
    /// through, untouched by the merge — the common case for a burst where
    /// most keys are touched once.
    #[test]
    fn untouched_keys_pass_through_unmerged() {
        let seg_a = vec![base("1")];
        let seg_b = vec![base("2")];
        let mut merged = merge_folded_changes(vec![seg_a, seg_b]);
        merged.sort_by(|a, b| a.key.cmp(&b.key));
        assert_eq!(merged.len(), 2);
        assert_eq!(merged[0].key, "1");
        assert_eq!(merged[1].key, "2");
    }

    /// A key updated in two coalesced segments folds to exactly one
    /// [`FoldedChange`] — the "never affect the same ON CONFLICT row twice"
    /// invariant [`merge_folded_changes`]'s doc comment calls out — carrying
    /// the earliest pre-image and the latest post-image across both
    /// windows, as if the whole span had been folded in one pass.
    #[test]
    fn same_key_across_segments_merges_to_one_record_with_earliest_old_and_latest_new_image() {
        let mut first = base("1");
        first.old_image = Some(r#"{"v":1}"#.to_string());
        first.new_image = Some(r#"{"v":2}"#.to_string());
        first.lsn = Some(PgLsn::from(10));
        first.first_seen = SystemTime::UNIX_EPOCH + Duration::from_secs(1);

        let mut second = base("1");
        second.old_image = Some(r#"{"v":2}"#.to_string());
        second.new_image = Some(r#"{"v":3}"#.to_string());
        second.lsn = Some(PgLsn::from(20));
        second.first_seen = SystemTime::UNIX_EPOCH + Duration::from_secs(2);

        let merged = merge_folded_changes(vec![vec![first], vec![second]]);
        assert_eq!(merged.len(), 1, "one row per key, never two");
        let merged = &merged[0];
        assert_eq!(merged.old_image, Some(r#"{"v":1}"#.to_string()));
        assert_eq!(merged.new_image, Some(r#"{"v":3}"#.to_string()));
        assert_eq!(merged.lsn, Some(PgLsn::from(20)));
        assert_eq!(
            merged.first_seen,
            SystemTime::UNIX_EPOCH + Duration::from_secs(1)
        );
    }

    /// A later segment's bare recompute trigger (no image at all — the
    /// image-less shape a reverse-recompute or backfill enumeration stages)
    /// must not blot out an earlier segment's real image evidence for the
    /// same key.
    #[test]
    fn image_less_later_segment_defers_to_earlier_segments_image() {
        let mut first = base("1");
        first.old_image = Some(r#"{"v":1}"#.to_string());
        first.new_image = Some(r#"{"v":2}"#.to_string());

        let second = base("1"); // no image at all: a bare recompute trigger

        let merged = merge_folded_changes(vec![vec![first], vec![second]]);
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].new_image, Some(r#"{"v":2}"#.to_string()));
        assert_eq!(merged[0].old_image, Some(r#"{"v":1}"#.to_string()));
    }

    /// A genuine delete (`new_image: None`, `old_image: Some(..)`) in a
    /// later segment must survive the merge as a delete, not be treated as
    /// "no new information" just because `new_image` is `None`.
    #[test]
    fn later_segment_delete_overrides_earlier_segments_write() {
        let mut first = base("1");
        first.old_image = Some(r#"{"v":1}"#.to_string());
        first.new_image = Some(r#"{"v":2}"#.to_string());

        let mut second = base("1");
        second.old_image = Some(r#"{"v":2}"#.to_string());
        second.new_image = None; // deleted in the later segment

        let merged = merge_folded_changes(vec![vec![first], vec![second]]);
        assert_eq!(merged.len(), 1);
        assert_eq!(
            merged[0].new_image, None,
            "the later segment's delete must win, not be papered over by the earlier write"
        );
        assert_eq!(merged[0].old_image, Some(r#"{"v":1}"#.to_string()));
    }

    /// `hop_gen` resets to 0 once any contributing segment carries a real
    /// source change, and otherwise takes the max across segments —
    /// mirroring `fold`'s own per-segment rule applied across segments too.
    #[test]
    fn hop_gen_resets_on_a_source_change_else_takes_the_max() {
        let mut first = base("1");
        first.hop_gen = 3;
        let mut second = base("1");
        second.hop_gen = 5;
        let merged = merge_folded_changes(vec![vec![first.clone()], vec![second.clone()]]);
        assert_eq!(merged[0].hop_gen, 5, "no source change: max of the two");

        second.src_changed = Some(SystemTime::UNIX_EPOCH);
        let merged = merge_folded_changes(vec![vec![first], vec![second]]);
        assert_eq!(
            merged[0].hop_gen, 0,
            "a source change in either segment resets hop_gen"
        );
    }

    /// `origin_lsn` takes the lesser of two known sides, and an unknown
    /// (missing) side wins outright (issue #469): the key still carries a
    /// change of unknown age, which gates every convergence token.
    #[test]
    fn origin_lsn_takes_the_lesser_side_and_an_unknown_side_wins() {
        let mut first = base("1");
        first.origin_lsn = Some(PgLsn::from(9));
        let mut second = base("1");
        second.origin_lsn = Some(PgLsn::from(7));
        let merged = merge_folded_changes(vec![vec![first.clone()], vec![second.clone()]]);
        assert_eq!(merged[0].origin_lsn, Some(PgLsn::from(7)));

        first.origin_lsn = None;
        let merged = merge_folded_changes(vec![vec![first], vec![second]]);
        assert_eq!(merged[0].origin_lsn, None);
    }

    /// Issue #486: an insert sealed into one segment and its delete into the
    /// next each fold with an image, but merging them leaves none. The merge
    /// keeps the two images it dropped, so the group is still named.
    #[test]
    fn an_insert_and_delete_merged_across_segments_keep_the_dropped_images() {
        let mut insert = base("1");
        insert.new_image = Some(r#"{"g":"z"}"#.to_string());
        let mut delete = base("1");
        delete.old_image = Some(r#"{"g":"z"}"#.to_string());

        let merged = merge_folded_changes(vec![vec![insert], vec![delete]]);
        assert_eq!((&merged[0].old_image, &merged[0].new_image), (&None, &None));
        assert_eq!(merged[0].vanished_images, vec![r#"{"g":"z"}"#.to_string()]);
    }

    /// Issue #486: a merge that still has an image drops nothing a group
    /// needs, and adds no vanished image of its own; a side's own vanished
    /// images carry through either way.
    #[test]
    fn vanished_images_carry_through_a_merge_and_are_only_added_when_no_image_is_left() {
        let mut update = base("1");
        update.old_image = Some(r#"{"g":"a"}"#.to_string());
        update.new_image = Some(r#"{"g":"b"}"#.to_string());
        let mut delete = base("1");
        delete.old_image = Some(r#"{"g":"b"}"#.to_string());
        let merged = merge_folded_changes(vec![vec![update], vec![delete]]);
        assert!(merged[0].vanished_images.is_empty());

        let mut born_and_died = base("1");
        born_and_died.vanished_images = vec![r#"{"g":"x"}"#.to_string()];
        let mut insert = base("1");
        insert.new_image = Some(r#"{"g":"y"}"#.to_string());
        let merged = merge_folded_changes(vec![vec![born_and_died], vec![insert]]);
        assert_eq!(merged[0].new_image, Some(r#"{"g":"y"}"#.to_string()));
        assert_eq!(merged[0].vanished_images, vec![r#"{"g":"x"}"#.to_string()]);
    }

    /// Issue #392: a recompute in either segment marks the merged record.
    #[test]
    fn has_recompute_is_an_or_across_segments() {
        let mut update = base("1");
        update.old_image = Some(r#"{"v":5}"#.to_string());
        update.new_image = Some(r#"{"v":6}"#.to_string());
        let mut recompute = base("1");
        recompute.has_recompute = true;

        let merged = merge_folded_changes(vec![vec![update.clone()], vec![recompute]]);
        assert!(merged[0].has_recompute);
        assert_eq!(merged[0].new_image, update.new_image);
        let merged = merge_folded_changes(vec![vec![update.clone()], vec![update]]);
        assert!(!merged[0].has_recompute);
    }

    /// Issue #321: `min_image_lsn` merges as the lesser present side, so a
    /// key coalesced across segments is compared against a recompute horizon
    /// by its *earliest* image-bearing commit, and a later segment's bare
    /// recompute trigger (no `min_image_lsn`) never erases it.
    #[test]
    fn min_image_lsn_takes_the_lesser_non_null_side() {
        let mut first = base("1");
        first.min_image_lsn = Some(PgLsn::from(30));
        let mut second = base("1");
        second.min_image_lsn = Some(PgLsn::from(20));
        let merged = merge_folded_changes(vec![vec![first.clone()], vec![second]]);
        assert_eq!(merged[0].min_image_lsn, Some(PgLsn::from(20)));

        let recompute_only = base("1");
        let merged = merge_folded_changes(vec![vec![first], vec![recompute_only]]);
        assert_eq!(merged[0].min_image_lsn, Some(PgLsn::from(30)));
    }

    /// More than two contributing segments still fold to exactly one
    /// record, taking the latest post-image across all of them — the
    /// realistic burst shape this milestone targets.
    #[test]
    fn three_segments_touching_the_same_key_merge_to_one_record() {
        let mut a = base("1");
        a.new_image = Some("\"a\"".to_string());
        a.lsn = Some(PgLsn::from(1));
        let mut b = base("1");
        b.new_image = Some("\"b\"".to_string());
        b.lsn = Some(PgLsn::from(2));
        let mut c = base("1");
        c.new_image = Some("\"c\"".to_string());
        c.lsn = Some(PgLsn::from(3));

        let merged = merge_folded_changes(vec![vec![a], vec![b], vec![c]]);
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].new_image, Some("\"c\"".to_string()));
        assert_eq!(merged[0].lsn, Some(PgLsn::from(3)));
    }

    /// Issue #409: a merged record's `row_count` is the sum of every
    /// contributing segment's, so `trellis_changes_applied_total` counts the
    /// same staged rows whether they sealed into one segment or several.
    #[test]
    fn row_count_sums_across_segments() {
        let mut a = base("1");
        a.row_count = 4;
        let mut b = base("1");
        b.row_count = 2;
        let c = base("1");
        let untouched = base("2");

        let mut merged = merge_folded_changes(vec![vec![a, untouched], vec![b], vec![c]]);
        merged.sort_by(|a, b| a.key.cmp(&b.key));
        assert_eq!(merged.len(), 2);
        assert_eq!(merged[0].row_count, 7);
        assert_eq!(
            merged[1].row_count, 1,
            "a key in one segment keeps its count"
        );
    }

    /// Issue #133: the cross-segment merge's `group_key` rule is a real set
    /// union, matching [`fold`]'s own SQL `array_agg(distinct ...)` — not
    /// the old `earlier.group_key.or(later.group_key)` placeholder, which
    /// would have silently dropped every value `later` touched whenever
    /// `earlier` had any `group_key` at all. Overlapping values must be
    /// deduped, not just concatenated.
    #[test]
    fn group_key_cross_segment_merge_is_a_real_deduplicated_union() {
        let mut first = base("1");
        first.group_key = Some(vec!["a".to_string(), "b".to_string()]);
        let mut second = base("1");
        second.group_key = Some(vec!["b".to_string(), "c".to_string()]);

        let merged = merge_folded_changes(vec![vec![first], vec![second]]);
        assert_eq!(merged.len(), 1);
        let mut group_key = merged[0].group_key.clone().expect("group_key present");
        group_key.sort();
        assert_eq!(
            group_key,
            vec!["a".to_string(), "b".to_string(), "c".to_string()],
            "must be the real union of both segments' touched values, deduplicated \
             (the old `.or()` placeholder would have kept only [\"a\", \"b\"])"
        );
    }

    /// A segment with no `group_key` at all defers entirely to the other
    /// side, same "missing side contributes nothing" convention every other
    /// `Option` field in this merge follows.
    #[test]
    fn group_key_cross_segment_merge_treats_a_missing_side_as_contributing_nothing() {
        let first = base("1"); // no group_key
        let mut second = base("1");
        second.group_key = Some(vec!["only".to_string()]);

        let merged = merge_folded_changes(vec![vec![first], vec![second]]);
        assert_eq!(merged[0].group_key, Some(vec!["only".to_string()]));

        let mut third = base("1");
        third.group_key = Some(vec!["only".to_string()]);
        let fourth = base("1");
        let merged = merge_folded_changes(vec![vec![third], vec![fourth]]);
        assert_eq!(merged[0].group_key, Some(vec!["only".to_string()]));
    }

    /// Issue #134 review follow-up: two `relationship_reverse_deferred`
    /// records for the same relationship+parent key, hand-staged so the
    /// segment that sealed *first* (`per_segment`'s own `earlier` argument)
    /// actually carries the chronologically *later* transition (higher
    /// `lsn`) — the exact inversion a retry's own re-staging timing can
    /// produce (see `merge_pair`'s own inline comment on this branch for
    /// why segment order and `lsn` order decouple specifically for this
    /// op). Before this fix, `merge_pair`'s segment-order convention would
    /// have silently picked the *later*-sealed (but chronologically
    /// *earlier*) segment's `new_image` and the *earlier*-sealed (but
    /// chronologically *later*) segment's `old_image` — an internally
    /// inconsistent pair that (a) would corrupt a direct fast-path
    /// application if one ever ran against it, and, more immediately
    /// relevant since `retry_count` alone already routes every deferred
    /// record away from the fast path, (b) would corrupt
    /// `apply::apply_projection_advance`'s unconditional write of the
    /// settled parent projection's own columns, which is *not* gated by
    /// `retry_count` at all. This pins the fix: `lsn` order wins over
    /// segment order for this op.
    #[test]
    fn relationship_reverse_deferred_cross_segment_merge_orders_images_by_lsn_not_segment_order() {
        // Sealed *first* (the `earlier` argument to `merge_pair`), but
        // chronologically the *later* transition: 400 -> 500 at lsn 200.
        let mut sealed_first_but_chronologically_later = base("1");
        sealed_first_but_chronologically_later.relationship_reverse_deferred = Some(7);
        sealed_first_but_chronologically_later.retry_count = 3;
        sealed_first_but_chronologically_later.old_image = Some(r#"{"id":1,"v":400}"#.to_string());
        sealed_first_but_chronologically_later.new_image = Some(r#"{"id":1,"v":500}"#.to_string());
        sealed_first_but_chronologically_later.lsn = Some(PgLsn::from(200));

        // Sealed *second* (the `later` argument to `merge_pair`), but
        // chronologically the *earlier* transition: 100 -> 400 at lsn 100.
        let mut sealed_second_but_chronologically_earlier = base("1");
        sealed_second_but_chronologically_earlier.relationship_reverse_deferred = Some(7);
        sealed_second_but_chronologically_earlier.retry_count = 1;
        sealed_second_but_chronologically_earlier.old_image =
            Some(r#"{"id":1,"v":100}"#.to_string());
        sealed_second_but_chronologically_earlier.new_image =
            Some(r#"{"id":1,"v":400}"#.to_string());
        sealed_second_but_chronologically_earlier.lsn = Some(PgLsn::from(100));

        let merged = merge_folded_changes(vec![
            vec![sealed_first_but_chronologically_later],
            vec![sealed_second_but_chronologically_earlier],
        ]);
        assert_eq!(merged.len(), 1);
        let merged = &merged[0];
        assert_eq!(
            merged.old_image,
            Some(r#"{"id":1,"v":100}"#.to_string()),
            "old_image must come from the chronologically earliest transition \
             (lsn 100), not whichever segment happened to seal first"
        );
        assert_eq!(
            merged.new_image,
            Some(r#"{"id":1,"v":500}"#.to_string()),
            "new_image must come from the chronologically latest transition \
             (lsn 200), not whichever segment happened to seal second"
        );
        assert_eq!(
            merged.lsn,
            Some(PgLsn::from(200)),
            "the merged record's own lsn is still the greatest of the two, \
             unaffected by this fix"
        );
        assert_eq!(
            merged.relationship_reverse_deferred,
            Some(7),
            "the relationship id must survive the merge"
        );
        assert_eq!(
            merged.retry_count, 3,
            "retry_count still folds via MAX regardless of this fix — every \
             merged deferred record is guaranteed retry_count >= 1"
        );
    }

    /// The ordinary-CDC merge path is untouched by the `lsn`-ordering
    /// branch above: with `relationship_reverse_deferred` absent on both
    /// sides, segment order still governs, exactly as before this issue's
    /// review follow-up.
    #[test]
    fn non_deferred_records_still_merge_by_segment_order_not_lsn() {
        let mut first = base("1");
        first.old_image = Some(r#"{"v":1}"#.to_string());
        first.new_image = Some(r#"{"v":2}"#.to_string());
        first.lsn = Some(PgLsn::from(50));

        let mut second = base("1");
        second.old_image = Some(r#"{"v":2}"#.to_string());
        second.new_image = Some(r#"{"v":3}"#.to_string());
        // Deliberately a *smaller* lsn than `first`'s, to prove this path
        // ignores lsn ordering entirely (unlike the deferred-reverse
        // branch) and defers purely to segment (append) order.
        second.lsn = Some(PgLsn::from(10));

        let merged = merge_folded_changes(vec![vec![first], vec![second]]);
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].old_image, Some(r#"{"v":1}"#.to_string()));
        assert_eq!(merged[0].new_image, Some(r#"{"v":3}"#.to_string()));
    }
}
