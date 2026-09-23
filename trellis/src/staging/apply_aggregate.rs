//! Aggregate (`GROUP BY`) delta maintenance — the [`crate::defs::ast::KeySpace::Aggregate`]
//! counterpart to [`super::apply`]'s 1-1 write path (issue #11, stage 05).
//!
//! [`super::apply`] owns the version fence, the completion statement,
//! downstream propagation, and the retry loop — all of that is generic over
//! *which* target rows changed, not over the 1-1/aggregate distinction, so
//! this module only supplies the two aggregate-specific pieces that plug
//! into it: [`accumulate_changes`] (Phase 2: fold a batch's per-key changes
//! into per-group deltas) and [`apply_aggregate_target`] (Phase 3: write
//! those deltas, per group, in ascending group-key order).
//!
//! # The delta model
//!
//! Every calculated field on an [`KeySpace::Aggregate`] definition is
//! classified once, via [`super::super::defs::invertibility::classify`]
//! (reused, not reimplemented — see that module's doc comment):
//!
//! - A field that is a direct `SUM(<numeric expr>)` or `AVG(<numeric
//!   expr>)` call is [`AggFieldKind::Sum`]/[`AggFieldKind::Avg`] —
//!   **invertible**: its new value is `old value + f(new row) - f(old
//!   row)`, so [`accumulate_changes`] folds every touched row's signed
//!   contribution into a per-group delta, and [`apply_aggregate_target`]
//!   applies that delta as a Postgres-side increment (`col = col + ...`),
//!   never re-reading the source table for the field's own sake. `AVG`
//!   needs two hidden partial columns (`__{field}_sum` — see
//!   `defs::ddl::avg_sum_column` — and a running-count partial) since `avg =
//!   sum / count` and neither half alone is invertible; the visible `avg`
//!   column is derived from them in the same statement. `SUM` needs a
//!   running-count partial too, even though its own arithmetic never needs a
//!   count: Postgres's `sum()` is `NULL`, not `0`, over zero non-null values,
//!   and without a running count the delta model can't tell "this group's
//!   last non-null contributor just left" (must go `NULL`) from "this
//!   group's contributions happen to net to zero" (a real `0`) — a plain
//!   running total alone can't distinguish those two cases once other rows
//!   in the group remain. The running-count partial's *column name* is
//!   shared across every `SUM`/`AVG` field that aggregates the exact same
//!   argument (issue #48 — see `defs::ddl::count_column_names`'s doc comment
//!   for why that sharing is scoped to "same argument", not "any
//!   count-needing field on this target"); `AggregateTargetPlan::count_column_names`
//!   holds the resolved name for each field. Both fields' full-recompute path
//!   (image-less changes, or a group forced onto it — see below) re-derives
//!   *both* partials from a live probe, keeping them consistent with what a
//!   fresh delta from that point forward would produce.
//! - A field that is a direct `COUNT(*)` or `COUNT(<expr>)` call (issue #75,
//!   widened to a real argument by issue #120) is [`AggFieldKind::Count`] —
//!   also invertible, and simpler than `SUM`: its new value is `old value +
//!   (contributions added) - (contributions removed)`, with no hidden
//!   partial column (unlike `SUM`/`AVG`, whose running count partial exists
//!   only to recover Postgres's own "sum of zero non-null values is NULL"
//!   rule — `COUNT` never needs that, because it *is* its own count).
//!   `COUNT(*)` counts every row unconditionally (no NULL to skip at all);
//!   `COUNT(<expr>)` counts non-null occurrences of `<expr>`, folding
//!   through the exact same per-row contribution/null-skip machinery
//!   `SUM`/`AVG` already use ([`add_contributions`]/[`sub_contributions`] —
//!   a null contribution simply isn't pushed). Either way, a group whose
//!   *row count* hits zero is deleted outright by [`probe_group_exists`],
//!   independent of any field's own value — so `COUNT(<expr>)` legitimately
//!   writes a real `0` while its group stays alive with other rows present
//!   (unlike `COUNT(*)`, whose own zero and the group's extinction always
//!   coincide).
//! - Anything else — `MIN`/`MAX` (always [`Invertibility::RecomputeOnly`]
//!   per the gate: a deleted row might have held the extreme value, and
//!   there is no way to recover the next-best one from the aggregate's
//!   current state alone) or a composed expression this module doesn't
//!   attempt to decompose — is [`AggFieldKind::RecomputeOnly`]: whenever its
//!   group is touched at all, [`apply_aggregate_target`] re-derives it with
//!   one scoped SQL probe (`select <rendered expr> from source where
//!   <group columns> = ...`), reusing [`crate::defs::oracle::render_expr_sql`]
//!   so the probe is *literally* the same SQL the correctness oracle would
//!   run — never an approximated inverse.
//!
//! # Relationship-reading aggregates (issue #94, delta path since #136)
//!
//! A definition whose fields aggregate a **to-one** relationship path
//! (`SUM(post.word_count)`) *used to* opt out of the delta model entirely —
//! every group any change touched was marked [`GroupPlan::force_full_recompute`]
//! regardless of which side of a grain migration it was on, so Phase 3
//! re-derived it from the source `LEFT JOIN`ed to the *live* to-side table
//! (see [`apply_forced_groups_bulk`]) rather than folding per-row
//! contributions. Issue #136 (epic #127) replaces that for an ordinary
//! (non-image-less) change: [`accumulate_changes`] now resolves a row's
//! relationship read(s) from the settled parent projection (issue #130's
//! mechanism, the same one a `KeySpace::OneToOne` target's forward path
//! already uses) via [`build_forward_relationship_shape`]/
//! [`forward_row_contribution`], substituting each `RelationshipPath` for a
//! synthetic `Column` carrying the resolved value (mirroring
//! [`super::apply`]'s issue #131 reverse-path shape,
//! `ReverseAggregateShape`) and folding the result through the ordinary
//! per-row delta machinery below — no live `JOIN`, no forced full-group
//! recompute, for this case.
//!
//! [`GroupPlan::force_full_recompute`]/[`apply_forced_groups_bulk`]'s
//! live-`JOIN` machinery is **not** dead code, though: an **image-less**
//! change (a bare recompute trigger — reverse propagation's own fallback
//! path for anything the issue #131 fast path doesn't cover, definition
//! re-derive, backfill) still forces its group onto it below, for the exact
//! reason the "Image-less changes" section below explains — no prior
//! state to diff against, so a full-recompute re-derivation (a live `JOIN`
//! is simply the correct way to compute a *fresh* group value, not an
//! incremental adjustment, so it carries none of the double-counting risk a
//! live-read *delta* would) sidesteps the ambiguity. Truncate propagation
//! (a synthetic image-less recompute over every affected key) reaches the
//! same path the same way.
//!
//! # Grain migration
//!
//! An `UPDATE` whose old and new images carry different `GROUP BY` column
//! values moves one row from one group to another. [`accumulate_changes`]
//! derives the old and new group keys itself from the decoded images'
//! `GROUP BY` columns (not from [`super::fold::FoldedChange::group_key`],
//! which only carries one representative value per key — insufficient once
//! a single change can touch two different groups) and, when they differ,
//! subtracts the row's old contribution from the old group and adds its new
//! contribution to the new group as two independent deltas.
//!
//! # Image-less changes, and issues #180/#196's fixes for two producers of them
//!
//! A folded change with neither an old nor a new image (a bare recompute
//! trigger — reverse propagation, definition re-derive, backfill) carries no
//! usable prior state: this module cannot tell whether the row it re-reads
//! live already contributed to its group before this batch, so folding it
//! as a plain delta risks double-counting. [`accumulate_changes`] routes
//! these to [`GroupPlan::force_full_recompute`] instead: the affected
//! group's *every* field (not just the [`AggFieldKind::RecomputeOnly`] ones)
//! is re-derived by probe in Phase 3, sidestepping the ambiguity at the cost
//! of losing the delta's O(1)-per-touch cost for that one group, that one
//! batch. **If the image-less change's live re-read finds the key already
//! gone, there is no group to locate at all** (its prior group, if any, is
//! unknowable) **and the change is dropped** — this module's own logic here
//! is unchanged, and the gap is still real for a genuinely bare recompute
//! trigger reaching an already-vanished key (reverse propagation, definition
//! re-derive, or backfill racing a delete — not exercised by today's
//! producers).
//!
//! Issue #180 closes this gap for the one producer that *can* know the prior
//! state and previously threw it away: a chained aggregate's own upstream
//! source is itself an aggregate target, and when **that** target's group
//! goes extinct, [`super::apply::apply_and_mark_drained_many`]'s Phase 3
//! (`delete_group_row`/`apply_forced_groups_bulk`'s own `DELETE ...
//! RETURNING` — an explicit per-column `jsonb_build_object`, issue #248, not
//! `to_jsonb(t.*)`) captures the deleted row's exact pre-delete
//! image before it's gone, and downstream propagation (step 4) stages that
//! as a real image-bearing delete (`StagedChange::Cdc`, `old_image: Some(..)`,
//! `new_image: None`) instead of an image-less `Recompute`. Once staged that
//! way, this module never even sees an image-less change for that key: the
//! decoded `old_image` reaches [`accumulate_changes`] as an ordinary
//! `(Some(old_row), None)` change — the "a row leaves its group" case
//! [`sub_contributions`] already handles, subtracting the extinct group's
//! last-known contribution exactly like any other row-leaves-group delta.
//! No change to this module's own delta logic was needed; the fix is
//! entirely in what `super::apply` chooses to stage.
//!
//! Issue #196 widens this to the sibling producer #180 deliberately scoped
//! out: a chained aggregate's upstream source can just as well be a plain
//! [`KeySpace::OneToOne`] target, and deleting *that* target's row is the
//! same shape of "the prior state is knowable at delete time, but was being
//! thrown away." `super::apply::apply_target`'s own delete statement now
//! carries the identical shape of `RETURNING` capture (issue #248's explicit
//! per-column `jsonb_build_object`, not `to_jsonb(t.*)`), threaded
//! through the same `super::apply::ChangedKey` slot (a module-private type
//! alias, so deliberately not an intra-doc link), so step 4 stages the
//! same image-bearing `StagedChange::Cdc` for a deleted 1-1 row — reaching
//! this module's `(Some(old_row), None)` branch exactly as issue #180's
//! aggregate-group case does. Again, no change to this module's own delta
//! logic was needed.

use std::borrow::Cow;
use std::collections::HashMap;
use std::collections::HashSet;

use tokio_postgres::Transaction;
use tokio_postgres::types::ToSql;

use crate::defs::ast::{Expr, GroupByKey, KeySpace, TransformDef, ValueType, group_by_contains};
use crate::defs::ddl::{self, avg_sum_column};
use crate::defs::eval::{self, RegexCache, Row};
use crate::defs::invertibility::{self, AggregateArg, CountArg};
use crate::defs::oracle;
use crate::defs::pg_type::PgType;
use crate::defs::validate::{self, ResolvedRelationship};
use crate::pool::quote_ident;

use super::apply::{ApplyError, substitute_relationship_path};
use super::fold::FoldedChange;
use super::target_mutations::TargetMutations;

// ---------------------------------------------------------------------
// Field classification
// ---------------------------------------------------------------------

/// One aggregate field's delta strategy — see the module doc comment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum AggFieldKind {
    /// A direct `SUM(...)` call: the visible column *is* the running sum,
    /// incremented directly.
    Sum,
    /// A direct `AVG(...)` call: the visible column is derived from two
    /// hidden partials (`__{field}_sum`/`__{field}_count`), both
    /// incremented.
    Avg,
    /// A direct `COUNT(*)` call (issue #75): the visible column *is* the
    /// running row count, incremented directly — no hidden partials, unlike
    /// `SUM`/`AVG`, since `COUNT(*)` counts every row unconditionally (never
    /// skips a NULL, so there is no "no non-null contributions" ambiguity
    /// a running count would need to disambiguate) and a group whose count
    /// would otherwise hit zero is deleted outright by
    /// [`apply_aggregate_target`]'s extinction check rather than ever
    /// needing to represent a zero-row group's count as a column value.
    Count,
    /// Everything else (`MIN`/`MAX`, or a composed expression this module
    /// doesn't decompose): re-derived by probe whenever its group is
    /// touched, never incremented.
    RecomputeOnly,
}

/// One calculated field's plan on an aggregate target: its column name, its
/// inferred [`ValueType`] (the visible column's type — the same map every
/// other column type in this grammar is derived from), and its
/// [`AggFieldKind`].
#[derive(Debug, Clone)]
pub(super) struct AggFieldPlan {
    pub name: String,
    pub value_type: ValueType,
    pub kind: AggFieldKind,
}

/// Classifies `def`'s fields (skipping `GROUP BY` passthrough fields, which
/// contribute no column of their own — see `ddl::create_aggregate_target_table`)
/// into their [`AggFieldPlan`]s, via [`invertibility::classify`].
///
/// Every non-`COUNT` aggregate call this grammar can parse takes exactly one
/// `Numeric` argument (`registry::AGGREGATE_FUNCTION_SPECS` enforces this at
/// parse time — see `is_avg_field`'s doc comment in `defs::ddl` for the same
/// observation applied to `AVG` specifically), so a one-argument call always
/// classifies against [`AggregateArg::Column(ValueType::Numeric)`]; `COUNT`
/// (issue #75) is arity-0 (`COUNT(*)`, the only shape the parser accepts) and
/// classifies against [`AggregateArg::Count`]`(`[`CountArg::Star`]`)`
/// instead. A field that isn't a direct `COUNT`/one-argument aggregate call
/// at all (a composed expression, or a bare passthrough this loop didn't
/// already skip) has nothing to classify and defaults to
/// [`AggFieldKind::RecomputeOnly`] — the safe fallback the issue calls for
/// ("everything else goes on the recompute path").
///
/// `field_exprs` is every field's *substituted* expression (see
/// `defs::backfill::substituted_field_exprs`), keyed by field name, computed
/// once by the caller and shared with the `field_exprs` map
/// [`AggregateTargetPlan`] later renders from — classification must run
/// against the same self-contained expression that rendering uses, not
/// `field.expr` directly: a `GROUP BY` field may reference another
/// calculated field by name (e.g. `double_total = total + total` where
/// `total = SUM(amount)`), and only the substituted form exposes the
/// underlying aggregate call shape this match inspects.
pub(super) fn classify_fields(
    def: &TransformDef,
    group_by: &[GroupByKey],
    source_columns: &HashMap<String, ValueType>,
    field_exprs: &HashMap<String, Expr>,
    relationships: &HashMap<String, ResolvedRelationship>,
) -> Result<Vec<AggFieldPlan>, ApplyError> {
    // A field aggregating a to-one relationship path (issue #94) takes its
    // type from the *to-side* column, which only `relationships` carries;
    // a relationship-free definition passes an empty map and infers exactly
    // as before.
    let field_types = validate::infer_field_types(def, source_columns, relationships)?;
    let mut plans = Vec::with_capacity(def.fields.len());
    for field in &def.fields {
        if group_by_contains(group_by, &field.name) {
            continue;
        }
        let value_type = field_types
            .get(&field.name)
            .copied()
            .unwrap_or(ValueType::Numeric);
        let kind = match &field_exprs[&field.name] {
            Expr::FunctionCall { name, args } if name == "COUNT" && args.is_empty() => {
                match invertibility::classify("COUNT", AggregateArg::Count(CountArg::Star)) {
                    Some(v) if v.is_invertible() => AggFieldKind::Count,
                    _ => AggFieldKind::RecomputeOnly,
                }
            }
            // Issue #120: `COUNT(<expr>)` — see `defs::backfill::classify_field`'s
            // twin of this arm for why it must be checked ahead of the
            // generic one-argument arm below (that arm would otherwise
            // classify it against `AggregateArg::Column(value_type)`, a
            // shape `invertibility::classify` doesn't recognize for `COUNT`).
            Expr::FunctionCall { name, args } if name == "COUNT" && args.len() == 1 => {
                match invertibility::classify("COUNT", AggregateArg::Count(CountArg::Column)) {
                    Some(v) if v.is_invertible() => AggFieldKind::Count,
                    _ => AggFieldKind::RecomputeOnly,
                }
            }
            Expr::FunctionCall { name, args } if args.len() == 1 => {
                // Issue #112: the field's own inferred type, not a
                // hardcoded `Numeric`. A float `SUM`/`AVG` is *not*
                // invertible (float addition has no exact inverse, and
                // `NaN`/`Infinity` absorb), so passing `Numeric` here would
                // put it on the delta path and let it drift from a
                // server-side `sum()`. `defs::backfill::classify_field`
                // makes the same call for the same reason, and the two must
                // agree or a directly-built target and a ring-built one
                // would differ.
                match invertibility::classify(name, AggregateArg::Column(value_type)) {
                    Some(v) if v.is_invertible() && name == "SUM" => AggFieldKind::Sum,
                    Some(v) if v.is_invertible() && name == "AVG" => AggFieldKind::Avg,
                    _ => AggFieldKind::RecomputeOnly,
                }
            }
            _ => AggFieldKind::RecomputeOnly,
        };
        plans.push(AggFieldPlan {
            name: field.name.clone(),
            value_type,
            kind,
        });
    }
    Ok(plans)
}

// ---------------------------------------------------------------------
// Phase 2: per-group delta accumulation
// ---------------------------------------------------------------------

/// One `SUM`/`AVG` field's accumulated signed contributions for one group,
/// within one batch: every non-null per-row contribution this batch added
/// to the group (a row entering, or an existing row's argument changing
/// upward) and subtracted from it (a row leaving, or the argument changing
/// away from its old value), still as text — cast and summed in SQL, not
/// added in Rust (this crate's `Numeric` has no subtraction; see
/// `apply_aggregate_target`'s doc comment on why letting Postgres do the
/// arithmetic sidesteps that rather than growing `Numeric` for it).
///
/// An entry only exists in [`GroupPlan::field_accum`] for a field whose
/// group was actually touched by a row entering, leaving, or (for an
/// in-place update to the same group) genuinely changing its contribution
/// this batch (see [`accumulate_changes`]'s per-change cancellation) — a
/// field with no entry at all is left untouched by [`apply_aggregate_target`],
/// which suppresses a true no-op write for a group nothing here ever
/// touched. Critically, this is *presence*, not *non-emptiness*: a row
/// entering or leaving the group always gets an entry for every
/// `Sum`/`Avg`/`Count` field (via [`add_contributions`]/[`sub_contributions`]),
/// even when that row's own contribution is NULL and so pushes nothing into
/// either `adds` or `subs` — an empty pair is a legitimate zero-effect delta
/// (`sum_array_expr`'s `coalesce(sum(v), 0)` reduces it to `0`, and the
/// accompanying zero net count then yields Postgres's own "sum of zero
/// non-null values is NULL" rule downstream in [`upsert_group`]), not an
/// absent one. This distinction is exactly what fixed issue #11 review
/// finding #3: a brand-new group whose only row has a NULL `SUM`/`AVG`
/// argument, and no `MIN`/`MAX`/`COUNT` field to otherwise anchor a write,
/// used to get *no* `field_accum` entry at all (the old code only inserted
/// one for a non-NULL contribution), so [`group_has_activity`] saw no
/// activity and the group's target row was never written — not merely left
/// at a wrong value, but silently never created.
#[derive(Debug, Clone, Default)]
struct FieldAccum {
    adds: Vec<String>,
    subs: Vec<String>,
}

/// One group's pending Phase-3 write: its `GROUP BY` column values (text,
/// aligned with [`AggregateTargetPlan::group_by`]), the invertible fields'
/// accumulated deltas, the deepest `hop_gen` among the changes that touched
/// it, whether an image-less change forced it onto the full-recompute path
/// (see the module doc comment), and (issue #104, follow-up to #51/#52 once
/// #103 made aggregate-target chaining live rather than moot) the earliest
/// `src_changed` origin among those same changes — [`accumulate_changes`]
/// folds each touched change's [`FoldedChange::src_changed`] into this field
/// via [`super::apply::earliest_src_changed`], the identical fan-in
/// tie-break (earliest wins) the 1-1 path already uses, so this group's own
/// `hop_gen`/`src_changed` merge the same way a 1-1 target's touched key
/// does.
#[derive(Debug, Clone)]
pub(super) struct GroupPlan {
    pub group_values: Vec<Option<String>>,
    field_accum: HashMap<String, FieldAccum>,
    pub hop_gen: i32,
    pub src_changed: Option<std::time::SystemTime>,
    pub force_full_recompute: bool,
}

impl GroupPlan {
    /// Widened to `pub(super)` for issue #131, epic #127: the reverse-delta
    /// apply path (`super::apply`'s `RelationshipReverseRecord` handling)
    /// builds `GroupPlan`s directly from live-enumerated from-side rows,
    /// outside `accumulate_changes`' own batch loop, and needs this same
    /// constructor rather than a duplicate.
    pub(super) fn new(group_values: Vec<Option<String>>) -> Self {
        GroupPlan {
            group_values,
            field_accum: HashMap::new(),
            hop_gen: 0,
            src_changed: None,
            force_full_recompute: false,
        }
    }
}

/// One aggregate target table's Phase 2 output: its `GROUP BY` shape, its
/// fields' delta strategies, every group this batch touched, and (since
/// Phase 3's [`apply_aggregate_target`] holds no catalog connection of its
/// own, only `txn` — the same constraint [`super::apply::apply_and_mark_drained`]'s
/// doc comment notes for the 1-1 case) everything Phase 3's probes need to
/// re-derive a [`AggFieldKind::RecomputeOnly`] field or a
/// [`GroupPlan::force_full_recompute`] group without a second catalog
/// lookup: the source table name and each field's original [`Expr`] (only
/// [`oracle::render_expr_sql`] needs the expression itself — [`AggFieldPlan`]
/// only carries the field's classification, not its expression).
#[derive(Debug, Clone)]
pub(super) struct AggregateTargetPlan {
    /// Every `GROUP BY` key's **target** column name, in `GROUP BY` order —
    /// what DDL created the target's primary key columns as
    /// (`GroupByKey::target_column_name`), used by every statement that
    /// reads/writes the target table itself (the prelock, `delete_group_row`,
    /// `upsert_group`'s insert/conflict columns, …). A relationship path's
    /// target column name is its tail `column`, indistinguishable here from a
    /// plain column of the same name — see [`Self::group_by_source`] for the
    /// list that keeps the two apart, needed anywhere a group key's *value*
    /// must be read off `source` (optionally joined) rather than the target.
    pub group_by: Vec<String>,
    /// Every `GROUP BY` key, as the [`Expr`] it reads off `source` (optionally
    /// joined to `rel_joins`' to-side tables) — a plain [`Expr::Column`] or,
    /// for a [`GroupByKey::RelationshipPath`] key (issue #137), an
    /// [`Expr::RelationshipPath`] resolved through its own join alias.
    /// Same order/arity as [`Self::group_by`]/[`Self::group_by_types`].
    /// Rendered via [`oracle::render_to_one_rel_expr_sql`], exactly like a
    /// `RecomputeOnly` field's own expression already is — see
    /// `group_by_source_cols`.
    pub group_by_source: Vec<Expr>,
    pub group_by_types: Vec<ValueType>,
    pub fields: Vec<AggFieldPlan>,
    pub groups: HashMap<String, GroupPlan>,
    /// The source table's fully-qualified `"schema.table"` identity (issue
    /// #76, ADR-0007) — `super::apply::compute`'s own already-qualified
    /// `change.src_table`, not the bare `catalog_source_key`. Every probe
    /// below (`probe_sum_and_count`, `probe_count_star`, `probe_field_value`,
    /// `probe_group_exists`) and bulk-recompute builder
    /// (`apply_forced_groups_bulk`, `probe_recompute_fields_bulk`) reads this
    /// through [`ddl::qualified_source_table`] rather than a bare
    /// `quote_ident`, so a same-named source table in a different schema
    /// can't make a forced-recompute probe silently read the wrong physical
    /// relation.
    pub source: String,
    /// The target table's fully-qualified `"schema.table"` identity (issue
    /// #73's `Definition::target_table`, threaded through the identical way
    /// [`AggregateTargetPlan::source`] already threads issue #76's
    /// `qualified_source`) — reviewer follow-up to issue #74 (epic #78's own
    /// whole-branch review): every DML-emission site below
    /// (`delete_group_row`, `upsert_group`, `apply_delta_groups_bulk`,
    /// `apply_forced_groups_bulk`, `apply_aggregate_target`'s own pre-lock)
    /// reads its `target: &str` parameter through
    /// [`ddl::qualified_target_table_ident`] rather than a bare
    /// `quote_ident`, so a target explicitly qualified into a non-default
    /// schema (issue #76) resolves to the right physical relation instead
    /// of silently leaning on the connection's pinned `search_path`. The
    /// `target: &str` parameters those functions still take are always this
    /// same qualified string by the time they're called — see
    /// `super::apply::apply_and_mark_drained_many`'s call site.
    pub target: String,
    pub field_exprs: HashMap<String, Expr>,
    /// Every [`AggFieldKind::Sum`]/[`AggFieldKind::Avg`] field's hidden
    /// running-count partial column name (issue #48) — derived once here via
    /// [`ddl::count_column_names_from`] over this plan's own `fields`/
    /// `field_exprs`, the exact same merge rule (and, for fields sharing an
    /// argument, the exact same resulting name) `ddl::count_column_names`
    /// uses to decide which columns `create_aggregate_target_table` actually
    /// creates. See that function's doc comment for why two fields only ever
    /// share a column when they aggregate the identical argument expression.
    pub count_column_names: HashMap<String, String>,
    /// Every **to-one** relationship this definition's fields read (issue
    /// #94), as `(relationship name, to_table, to_col, from_col)`, sorted by
    /// name for deterministic SQL. Empty for the overwhelmingly common
    /// relationship-free aggregate, in which case every SQL statement this
    /// module emits is byte-identical to what it emitted before #94.
    ///
    /// Before issue #136, non-empty also *meant* "every group of this target
    /// is on the forced-recompute path" (`force_every_group`). #136 deleted
    /// that routing — an invertible field (`SUM`/`AVG`/`COUNT`) reading a
    /// relationship path is still folded incrementally through the ordinary
    /// delta path, exactly like a plain-column field — so this no longer
    /// implies anything about *routing*; it only tells every SQL builder
    /// that reads a `RecomputeOnly` field's expression ([`render_agg_expr`]
    /// and its callers) whether that expression's relationship paths need a
    /// join, regardless of which path (forced or delta) it's rendered from.
    pub rel_joins: Vec<RelJoin>,
}

/// One to-one relationship join an aggregate target needs to resolve its
/// fields: the relationship's name (which doubles as the SQL alias for the
/// joined to-side table, matching `defs::oracle`'s convention) and its
/// endpoints.
#[derive(Debug, Clone)]
pub(super) struct RelJoin {
    pub name: String,
    pub to_table: String,
    pub to_col: String,
    pub from_col: String,
}

impl AggregateTargetPlan {
    /// `group_by` is `def.key_space`'s `GROUP BY` keys directly — this
    /// constructor derives both [`Self::group_by`] (target column names) and
    /// [`Self::group_by_source`] (source-side, join-aware expressions) from
    /// it once, so every caller passes the one list `def.key_space` already
    /// carries rather than deriving the two projections itself.
    pub(super) fn new(
        group_by: &[GroupByKey],
        group_by_types: Vec<ValueType>,
        fields: Vec<AggFieldPlan>,
        source: String,
        target: String,
        field_exprs: HashMap<String, Expr>,
        rel_joins: Vec<RelJoin>,
    ) -> Self {
        let count_column_names = ddl::count_column_names_from(fields.iter().filter_map(|f| {
            if !matches!(f.kind, AggFieldKind::Sum | AggFieldKind::Avg) {
                return None;
            }
            match field_exprs.get(f.name.as_str()) {
                Some(Expr::FunctionCall { args, .. }) => {
                    args.first().map(|arg| (f.name.as_str(), arg))
                }
                _ => None,
            }
        }));
        AggregateTargetPlan {
            group_by: group_by
                .iter()
                .map(|k| k.target_column_name().to_string())
                .collect(),
            group_by_source: group_by.iter().map(GroupByKey::as_expr).collect(),
            group_by_types,
            fields,
            groups: HashMap::new(),
            source,
            target,
            field_exprs,
            count_column_names,
            rel_joins,
        }
    }
}

/// The `GROUP BY` columns' text values off `row`, in `group_by`'s order (a
/// missing or SQL-`NULL` column folds to `None`, matching every other
/// "absent column" convention in this crate), plus a key suitable as a
/// `HashMap` key naming that group.
///
/// For a **composite** (multi-column) `GROUP BY`, the key is every column's
/// text value joined on [`ddl::COMPOSITE_KEY_SEPARATOR`] (U+001F) — exactly
/// [`ddl::pk_key_sql_expr`]'s/[`crate::intake::extract_key`]'s own composite
/// primary-key identity encoding, in the same `GROUP BY` order
/// [`ddl::create_aggregate_target_table`] declares the target's `UNIQUE
/// NULLS NOT DISTINCT` grouping-column constraint in (and therefore the
/// order [`ddl::source_primary_key`] reports those columns back in, since it
/// orders by the chosen index's own `indkey` position).
///
/// This encoding used to be a local, length-prefixed one (`"{len}:{value}"`
/// per column, concatenated — still what the private
/// `defs::oracle::group_key` uses for the oracle's own `Recomputed` map),
/// justified by the claim that a composite grouping key's target could never
/// be chained onto as another definition's source, because
/// `ddl::source_primary_key` rejected a composite source outright. Issue
/// #126 lifted that rejection (narrowed back down at the one 1-1-specific
/// call site that still needed a single column, via the now-removed
/// `ddl::require_single_column_pk` — issue #121 deleted that narrowing too,
/// once every 1-1 consumer learned to take a source's primary key at
/// whatever arity it has), which made the claim false and turned
/// the encoding into a live crash: `written`/`deleted` (via
/// `apply::apply_and_mark_drained_many`'s downstream-propagation step) stage
/// a `Recompute` keyed by this string against the aggregate target, and the
/// chained definition's live refetch (`apply::read_live_rows_batch` →
/// [`ddl::split_pk_key`]) split it on U+001F, found one part where the
/// target's two-column identity wanted two, and failed the whole batch with
/// [`ddl::DdlError::MalformedCompositeKey`] (issue #171 — the multi-column
/// twin of the single-column bug issue #103 fixed below). Emitting the real
/// composite-PK encoding instead makes a chained definition decode an
/// aggregate group key exactly like any other multi-column source primary
/// key.
///
/// Nothing *within* this crate decodes the key (`plan.groups`,
/// `written`/`deleted` diffing, the grain-migration old-key/new-key
/// comparison above all treat it as an opaque, injective token, and every
/// statement this module emits binds `GroupPlan::group_values` — the typed
/// per-column values — never the encoded text), so switching encodings is
/// invisible to the aggregate target's own bookkeeping; it is also not
/// persisted anywhere across versions (the ring's staged `key` text is the
/// only place it lands, and only for the duration of one hop).
///
/// The U+001F join is injective regardless of what a real column value
/// contains: [`ddl::join_pk_key`] (which this function goes through, not a
/// local join) escapes a genuine `COMPOSITE_KEY_SEPARATOR`/`KEY_PART_ESCAPE`
/// occurrence rather than assuming it away (issue #200 — see
/// `ddl::KEY_PART_ESCAPE`'s doc comment). Before that fix this paragraph
/// described an *assumption*, not a guarantee, matching
/// `staging::append::TRUNCATE_SENTINEL_KEY`'s own pre-#200 caveat; the old
/// length-prefixed form was injective without needing any such assumption,
/// which is the one property this encoding gives up in exchange for
/// agreeing with the crate's single composite-key convention — a trade now
/// made safe rather than merely convenient. (Injective *for a fixed arity*,
/// which is all any consumer needs: `ddl::split_pk_key` always knows the
/// arity it is decoding at, and a one-column group's key is deliberately
/// left unescaped — see the single-column paragraph below.)
///
/// That escape composes with (rather than replacing) the
/// [`ddl::encode_key_part`] `NULL` encoding described next: this function
/// hands `join_pk_key` already-`encode_key_part`-encoded parts, and
/// `join_pk_key` escapes U+001F/U+001E on top of them. The two layers touch
/// disjoint characters — see `ddl::COMPOSITE_KEY_SEPARATOR`'s "how this
/// composes with `NULL_KEY_SENTINEL`" section.
///
/// A `NULL` grouping component (issue #110) is rendered through
/// [`ddl::encode_key_part`] as [`ddl::NULL_KEY_SENTINEL`] — a lone U+0001,
/// which a genuine value can never encode to, since `encode_key_part`
/// escapes a real U+0001 by doubling it — rather than
/// `unwrap_or_default()`'s empty string. Before this fix, a `NULL` component
/// rendered as `""`, indistinguishable from a genuine empty-string value, and
/// `array_to_string` drops a bare SQL `NULL` array element outright on the
/// read side (a bare `col::text` of a `NULL` column *is* SQL `NULL`), so a
/// `NULL`-keyed group could never round-trip through a *downstream* refetch:
/// its encoded key looked exactly like "no row," which
/// `staging::apply::read_live_rows_batch`'s `=`-based keyset join (also
/// fixed by #110, to `is not distinct from` wherever a batch's key carries a
/// `NULL` component) mistook for a delete. Both the encoding
/// (`ddl::pk_key_sql_expr`'s SQL-side `ddl::null_key_escape_sql`) and
/// this Rust-side producer route every component through the same
/// `NULL_KEY_SENTINEL` substitution, so a `NULL` group's key text agrees
/// byte-for-byte on both sides of the wire regardless of arity.
///
/// For a **single-column** `GROUP BY`, the key is that one column's own
/// text value, `encode_key_part`-encoded but otherwise untouched — no
/// length prefix, no separator to join on, and (issue #200) no
/// separator escape either: [`ddl::join_pk_key`] passes a lone part through
/// verbatim, byte-identical to what [`ddl::pk_key_sql_expr`] renders for
/// that same single-column key. That arity-1 exemption is deliberate and
/// load-bearing — see `ddl::push_escaped_composite_key_part`'s "why arity 1
/// is exempt". This case *does*
/// need to double as a real primary key value: the aggregate target's
/// actual Postgres identity (its `UNIQUE NULLS NOT DISTINCT` grouping-column
/// constraint, the same one [`ddl::source_primary_key`] falls back to for a
/// chained definition) is genuinely that one column, unencoded — and
/// `ddl::source_primary_key` does *not* reject a single-column source, so a
/// second definition can legally chain onto this aggregate target and read
/// this key as its source row's primary key. Before this fix, the
/// length-prefixed encoding leaked into that path too: `written`/`deleted`
/// (via `apply::apply_and_mark_drained_many`'s downstream-propagation step)
/// staged a `Recompute` keyed by the *encoded* string, which a later
/// `apply::read_live_rows_batch` then tried to bind as this literal
/// primary-key value — `"2:10"` is not a valid `numeric`, so that batch's
/// live refetch failed outright (issue #103); for a text primary key it
/// would silently look up the wrong row instead of failing loudly. Matching
/// the target table's real single-column PK shape exactly (see
/// [`ddl::create_aggregate_target_table`]) closes that gap.
///
/// A `NULL` grouping value and a genuine empty-string value are now
/// distinguishable at every arity (issue #110): the single-column case's
/// bare value is `NULL_KEY_SENTINEL` for `NULL` vs. `""` for a real empty
/// string, exactly as the composite case's own per-part encoding is.
///
/// Issue #119: normalizes one `GROUP BY` key **part**'s text before it feeds
/// [`derive_group_key`]'s `text` — the in-memory dedup key `accumulate_changes`
/// uses as `plan.groups`' `HashMap` key — to a single canonical spelling per
/// logical value, for the one [`ValueType`] this crate admits anywhere with
/// two disagreeing renderers.
///
/// This is *not* the same hazard [`ddl::encode_key_part`] handles just above
/// (a `NULL`/empty-string ambiguity in the *encoding*) — it is two different
/// non-`NULL` **strings for one value**: `boolout` (what CDC/`pgoutput`
/// decodes, verbatim, into `Row` — see `intake::pgoutput`'s tuple decoder)
/// spells a boolean `'t'`/`'f'`; `staging::apply::row_as_text_jsonb_sql`'s
/// `<col>::text` — what a bare, image-less recompute's live refetch decodes
/// into the very same `Row` shape — calls a *different*, dedicated Postgres
/// cast function (`pg_catalog.text(boolean)`, `catalog::
/// TEXT_STABLE_JOIN_KEY_TYPES`'s doc comment has the live `pg_cast`
/// evidence) that spells it `'true'`/`'false'` instead. No other type this
/// crate reads anywhere has two disagreeing renderers, so this is a no-op
/// for everything but `Boolean`.
///
/// Left unnormalized, one Postgres `GROUP BY` group touched once via each
/// path in the same drain batch silently **fragments into two `GroupPlan`s**
/// that each independently write to what the *database* correctly resolves
/// as the same target row (`keyset_unnest`'s `$1::text[]::boolean[]` cast
/// parses either spelling back to the same value via the permissive `boolin`
/// before ever comparing — see `validate::reject_unsupported_group_by_key_type`'s
/// `Boolean` arm) — but the two `GroupPlan`s know nothing of each other, so
/// whichever writes second corrupts the first's contribution instead of
/// replacing it (a delta add on top of an unrelated forced recompute, or
/// vice versa). That is worse than issue #248's "two target rows" symptom:
/// it is silent, wrong-but-plausible arithmetic in *one* row, not a visibly
/// duplicated one. `defs_boolean.rs`'s
/// `a_boolean_group_key_seeded_by_cdc_and_by_live_read_is_one_group_not_two`
/// reproduces the corruption end-to-end without this fix and pins the
/// correct total with it.
///
/// Only the dedup `text` this function feeds is touched — `values` (bound,
/// unmodified, straight into every SQL statement `keyset_unnest` builds)
/// stays exactly as read, since the native-typed cast already reconciles
/// either spelling there. Nothing decodes the dedup `text` back into a
/// value within this crate for `Boolean` specifically: chaining a second
/// definition onto a `Boolean`-keyed aggregate target — the one case where
/// this key text *does* leak out, staged as a downstream primary key (see
/// this function's own doc comment) — is unreachable in the first place,
/// because `ddl::source_primary_key` refuses a `boolean` single-column
/// primary key exactly as it refuses one on any ordinary source table (this
/// function's target table is no exception); normalizing here is therefore
/// safe for `Boolean`, not just correct.
fn canonicalize_group_key_part(value_type: ValueType, text: Option<&str>) -> Option<String> {
    match (value_type, text) {
        (ValueType::Boolean, Some(t)) => Some(
            match t {
                "t" | "true" | "TRUE" | "True" => "t",
                "f" | "false" | "FALSE" | "False" => "f",
                // Defense-in-depth: `parse_value` would itself reject this
                // text as an invalid boolean before it ever reached here.
                // Passed through verbatim rather than guessed at, so a
                // corrupt/unexpected spelling still fails loudly downstream
                // instead of being silently coerced to one arm here.
                other => other,
            }
            .to_string(),
        ),
        // Issue #116: `inet`'s counterpart to the `Boolean` arm above, for
        // the identical reason — `inet_out` (CDC) and `<col>::text`/
        // `network_show` (this engine's own live reads, post-#248) spell a
        // bare-host `inet` value two different ways
        // (`crate::netaddr`'s module doc has the live grid), and this
        // function's whole job is closing that gap before it reaches
        // `derive_group_key`'s in-memory dedup key. See
        // `validate::reject_unsupported_group_by_key_type`'s `Inet` arm for
        // why that makes the `GROUP BY` key role safe despite `inet` staying
        // off `catalog::TEXT_STABLE_JOIN_KEY_TYPES`.
        (ValueType::Other(PgType::Inet), Some(t)) => {
            Some(crate::netaddr::canonicalize_group_key_text(t))
        }
        (_, text) => text.map(str::to_string),
    }
}

/// `pub(super)`: issue #131's reverse-delta apply path derives a live
/// from-side row's group key the same way this batch-driven caller does.
pub(super) fn derive_group_key(
    row: &Row,
    group_by: &[String],
    group_by_types: &[ValueType],
) -> (Vec<Option<String>>, String) {
    let values: Vec<Option<String>> = group_by
        .iter()
        .map(|c| row.get(c).cloned().flatten())
        .collect();
    // One encoding at every arity, exactly what `ddl::pk_key_sql_expr`
    // renders for the aggregate target's own grouping-column identity: the
    // bare value for a single column (issue #103), the U+001F join for a
    // composite one (issue #171) — so a chained definition's live refetch
    // decodes the staged downstream key as that target's real primary key
    // either way. See this function's doc comment. `ddl::encode_key_part`
    // (issue #110, not `unwrap_or_default`) keeps a composite key's arity
    // equal to `group_by`'s, which is what `ddl::split_pk_key` checks on the
    // other end, while keeping a `NULL` component distinguishable from a
    // real empty string.
    //
    // Issue #119: each part is first run through
    // `canonicalize_group_key_part`, a no-op for every `ValueType` but
    // `Boolean` — see that function's doc comment for why this dedup key
    // specifically (not `values`, which stays untouched) needs it.
    let text = ddl::join_pk_key(
        values
            .iter()
            .zip(group_by_types)
            .map(|(v, ty)| canonicalize_group_key_part(*ty, v.as_deref()))
            .map(|v| ddl::encode_key_part(v.as_deref()).into_owned()),
    );
    (values, text)
}

/// One row's contribution to every field on `def`, keyed by field name, as
/// text (`None` for a null/absent contribution — Postgres's "aggregate
/// skips NULL" rule). Reuses [`eval::evaluate_aggregate`] over a
/// single-row slice rather than a second per-row evaluator: aggregating
/// `SUM`/`MIN`/`MAX` over exactly one row *is* that row's contribution (a
/// sum of one value is that value; skipped-if-null falls out of the same
/// "aggregate of zero non-NULL values is NULL" rule `fold_aggregate` already
/// implements) — see the module doc comment's "The delta model" section.
/// Only [`AggFieldKind::Sum`]/[`AggFieldKind::Avg`] fields' entries are read
/// by callers; [`AggFieldKind::RecomputeOnly`] fields' one-row values here
/// are unused (and, for `MIN`/`MAX`, not even meaningful as a "contribution"
/// — those are re-derived by probe instead, never from this map).
///
/// **`AVG` fields are evaluated as `SUM` instead of `AVG`** (see
/// [`contribution_def`]): a naive reading of "an average of one value is
/// that value" is true of the *mathematical* value but not of the exact
/// Postgres `numeric` this crate must bit-for-bit reproduce.
/// `reduce_numeric_aggregate`'s `AVG` arm ends every reduction — even over a
/// single row — with a real `numeric` division (`sum / count`), and
/// Postgres's own division picks its result scale from the *operands'*
/// scale/weight (`Numeric::div`'s `select_div_scale`), not from "the
/// dividend, unchanged": dividing by the `1`-row count this delta model
/// always reduces over inflates the contribution's scale (e.g. `67` becomes
/// `67.0000000000000000`, and `0` becomes `0.00000000000000000000`) well
/// past the value's own natural scale. That inflated-scale text then feeds
/// straight into [`sum_array_expr`]'s SQL-side `sum()` over this field's
/// hidden running-sum partial, and Postgres's `numeric` addition/division
/// both float their own result scale up to at least their operands' —
/// so the inflation compounds with every subsequent row this group ever
/// accumulates, and the *final* `sum / count` division `upsert_group`'s
/// `AvgDelta`/`AvgForced` arms perform for the visible column inherits an
/// already-inflated dividend scale, producing a materially different
/// (over-precise, and per `select_div_scale`'s scale-dependent rounding,
/// not merely differently-*formatted*) result than Postgres's own `avg()`
/// aggregate — which only ever divides once, over the group's true final
/// sum/count, never through this row-by-row intermediate division at all.
/// `SUM`'s reduction has no such step (`reduce_numeric_aggregate`'s `SUM`
/// arm is pure `Numeric::add`, whose result scale is exactly
/// `max(operand scales)` — a no-op scale-wise for a single addend), so
/// evaluating the field as `SUM` instead yields the row's raw, unscaled
/// contribution — exactly what this delta model needs to accumulate and
/// divide, once, at the end.
///
/// `def` must already be [`contribution_def`]'s rewritten form (every
/// caller in this module builds that once per batch and passes it here
/// unchanged) rather than the original, unrewritten definition — this
/// function trusts that rather than re-deriving it per row/change, since
/// every call within one [`accumulate_changes`] batch would otherwise
/// repeat the exact same rewrite of the exact same `def`.
///
/// `pub(super)`: issue #131's reverse-delta apply path also calls this
/// directly, against a *relationship-substituted* rewritten def (a
/// `RelationshipPath` swapped for a synthetic `Column` carrying the
/// parent's old/new image value) rather than `accumulate_changes`' plain
/// per-batch rewrite — see `super::apply`'s `ReverseAggregateShape`.
pub(super) fn row_contribution(
    def: &TransformDef,
    row: &Row,
    source_columns: &HashMap<String, ValueType>,
    regex_cache: &mut RegexCache,
) -> Result<HashMap<String, Option<String>>, ApplyError> {
    let evaluated =
        eval::evaluate_aggregate(def, std::slice::from_ref(row), source_columns, regex_cache)?;
    Ok(evaluated
        .into_iter()
        .map(|(name, value)| (name, value.map(|v| v.to_string())))
        .collect())
}

/// `def`, with every field whose own expression is a direct `AVG(...)` call
/// rewritten to the equivalent `SUM(...)` call (same argument) — see
/// [`row_contribution`]'s doc comment for why. Rewriting only fields whose
/// *own* raw expression is structurally `AVG(...)` (rather than every field
/// [`AggFieldKind::Avg`]-classifies, which uses the cross-field-alias-
/// substituted view) still reaches every `AVG`-derived field: a field that
/// only *aliases* an `AVG` field (e.g. `total2 = total` where
/// `total = AVG(amount)`) has no `AVG` syntax of its own to rewrite, but its
/// value is computed by recursing (via [`Expr::Column`]) into the aliased
/// field's own cache entry — which this rewrite already corrected, since
/// that field's own raw expression *is* the literal `AVG(...)` call. Every
/// other field (grouping-key passthroughs, `SUM`/`MIN`/`MAX`/`COUNT`
/// fields, and any calculated field composing one of those) is left
/// byte-for-byte identical to `def`'s own field, so this changes nothing
/// about their evaluation.
/// `pub(super)`: issue #131's reverse-delta apply path applies this same
/// `AVG`-as-`SUM` rewrite to its own relationship-substituted definition
/// before calling [`row_contribution`].
pub(super) fn contribution_def(def: &TransformDef) -> TransformDef {
    let mut def = def.clone();
    for field in &mut def.fields {
        if let Expr::FunctionCall { name, .. } = &mut field.expr
            && name == "AVG"
        {
            "SUM".clone_into(name);
        }
    }
    def
}

/// The synthetic source-column name issue #136's forward relationship
/// substitution splices a resolved to-one value under — the forward-path
/// counterpart to [`super::apply::synthetic_relationship_column`]'s reverse
/// convention, but scoped by *both* relationship name and to-side column,
/// not the column alone.
///
/// That extra scoping is necessary here and isn't for the reverse path:
/// [`super::apply::build_reverse_relationship_shape`]'s design fork 3
/// restricts a reverse shape to exactly one relationship, so its bare
/// `column`-only naming can never collide. A forward [`AggregateTargetPlan`]
/// carries no such restriction — [`apply_forced_groups_bulk`]'s own
/// `rel_joins_sql` already joins more than one to-side table in a single
/// recompute statement — so two different relationships that happen to
/// share a to-side column name (e.g. both `author` and `editor` relate to a
/// `users` table and both read `.name`) must not collide on the same
/// synthetic column.
fn forward_relationship_synthetic_column(rel: &str, column: &str) -> String {
    format!("__trellis_fwd_{rel}_{column}")
}

/// One resolved `<rel>.<column>` reference [`build_forward_relationship_shape`]
/// rewrote into a synthetic `Column` — `from_col` is the from-row's own join
/// key column (read fresh per row, per side, in
/// [`forward_row_contribution`], since a row's own `from_col` can itself
/// change between its old and new image within one folded update).
#[derive(Debug, Clone)]
struct ForwardRelationshipSynthetic {
    rel_name: String,
    from_col: String,
    to_col: String,
    synthetic: String,
}

/// Issue #136's forward-path counterpart to [`super::apply`]'s
/// `ReverseAggregateShape` (issue #131): everything [`forward_row_contribution`]
/// needs to evaluate one row's contribution against a relationship-reading
/// aggregate definition without a live `JOIN` — built once per
/// [`accumulate_changes`] batch (like [`contribution_def`]'s own per-batch
/// rewrite), not once per row.
///
/// `synthetic` is empty for a relationship-free plan (`plan.rel_joins`
/// empty, the overwhelmingly common case), in which case `contribution_def`/
/// `source_columns` are exactly [`contribution_def`]'s plain rewrite and the
/// caller's own `source_columns`, unchanged from before this issue —
/// [`forward_row_contribution`] special-cases that shape into a plain
/// passthrough.
struct ForwardRelationshipShape {
    contribution_def: TransformDef,
    source_columns: HashMap<String, ValueType>,
    synthetic: Vec<ForwardRelationshipSynthetic>,
}

/// Builds one batch's [`ForwardRelationshipShape`] for `def` against
/// `rel_joins` (issue #136): every `RelationshipPath { rel, column }`
/// reference this definition's fields make into one of `rel_joins`'
/// relationships is rewritten to an ordinary `Column` reference at a
/// synthetic name, via [`super::apply::substitute_relationship_path`] —
/// reused directly rather than reimplemented, since that function is
/// already generic over which relationship name it rewrites (nothing in its
/// own signature is reverse-specific). Called once per relationship in
/// `rel_joins`, since [`substitute_relationship_path`] only ever rewrites
/// paths for the one `rel_name` it's given, leaving every other
/// relationship's paths untouched until their own turn.
///
/// The resolved to-side value's [`ValueType`] comes from `rel_ctx`'s own
/// [`eval::ToOneRelationship::to_columns`] — the same settled-projection-typed
/// map [`super::apply::build_relationship_context`] already resolved for
/// this batch (via `to_column_types`), not a second catalog round trip.
///
/// # Panics
///
/// If `rel_joins` is non-empty but `rel_ctx` is `None` — every caller with a
/// relationship-reading plan must have already built a context (see
/// `super::apply::compute`'s aggregate branch, which builds one via
/// [`super::apply::build_relationship_context`] whenever
/// [`eval::relationship_references`] is non-empty, the same gate this
/// function's caller checks by way of `plan.rel_joins`).
fn build_forward_relationship_shape(
    def: &TransformDef,
    rel_joins: &[RelJoin],
    rel_ctx: Option<&eval::RelationshipContext>,
    source_columns: &HashMap<String, ValueType>,
) -> ForwardRelationshipShape {
    if rel_joins.is_empty() {
        return ForwardRelationshipShape {
            contribution_def: contribution_def(def),
            source_columns: source_columns.clone(),
            synthetic: Vec::new(),
        };
    }
    let ctx = rel_ctx.expect(
        "AggregateTargetPlan carries rel_joins but accumulate_changes was called with no \
         relationship context — see super::apply::compute's aggregate branch, which must \
         build one via build_relationship_context whenever eval::relationship_references \
         is non-empty",
    );

    let refs = eval::relationship_references(def);
    let mut rewritten = def.clone();
    let mut widened_source_columns = source_columns.clone();
    let mut synthetic = Vec::new();

    for join in rel_joins {
        let mut synthetic_map: HashMap<String, String> = HashMap::new();
        for (rel, column) in &refs {
            if rel != &join.name || synthetic_map.contains_key(column) {
                continue;
            }
            let synthetic_name = forward_relationship_synthetic_column(&join.name, column);
            let value_type = ctx
                .to_one(&join.name)
                .and_then(|r| r.to_columns.get(column))
                .copied()
                .unwrap_or(ValueType::Text);
            widened_source_columns.insert(synthetic_name.clone(), value_type);
            synthetic.push(ForwardRelationshipSynthetic {
                rel_name: join.name.clone(),
                from_col: join.from_col.clone(),
                to_col: column.clone(),
                synthetic: synthetic_name.clone(),
            });
            synthetic_map.insert(column.clone(), synthetic_name);
        }
        for field in &mut rewritten.fields {
            substitute_relationship_path(&mut field.expr, &join.name, &synthetic_map);
        }
        // Issue #137: a `GROUP BY` key can itself be exactly this
        // relationship path (`GROUP BY tag, post.author`) — rewritten to the
        // same synthetic column a passthrough field referencing it bare
        // would use, so `eval::evaluate_aggregate`'s own group-by-column
        // recognition (`row_contribution`'s callee) still lines up should a
        // field ever bare-passthrough it (see
        // `a_group_by_relationship_path_bare_passthrough_field_is_allowed`).
        // `derive_group_key` itself never reads `rewritten.key_space` — it's
        // driven by `group_by_row_columns` below, independently — so this
        // rewrite exists solely for `row_contribution`'s sake.
        if let KeySpace::Aggregate { group_by } = &mut rewritten.key_space {
            for key in group_by.iter_mut() {
                if let GroupByKey::RelationshipPath { rel, column } = key
                    && rel == &join.name
                    && let Some(synthetic_name) = synthetic_map.get(column)
                {
                    *key = GroupByKey::Column(synthetic_name.clone());
                }
            }
        }
    }

    ForwardRelationshipShape {
        contribution_def: contribution_def(&rewritten),
        source_columns: widened_source_columns,
        synthetic,
    }
}

/// The row-column name [`derive_group_key`] should read for each of `def`'s
/// `GROUP BY` keys, once a batch's changed rows may need augmenting with a
/// resolved relationship value (issue #137): a plain key's own column name,
/// unchanged, or a relationship-path key's
/// [`forward_relationship_synthetic_column`] — the exact synthetic name
/// [`augment_row_with_forward_relationships`] splices a resolved value under,
/// so a row augmented by that function can be handed straight to
/// `derive_group_key` with this list, with no further translation.
fn group_by_row_columns(group_by: &[GroupByKey]) -> Vec<String> {
    group_by
        .iter()
        .map(|key| match key {
            GroupByKey::Column(name) => name.clone(),
            GroupByKey::RelationshipPath { rel, column } => {
                forward_relationship_synthetic_column(rel, column)
            }
        })
        .collect()
}

/// Splices `row` with each of `shape.synthetic`'s resolved to-one values
/// (issue #137, generalizing issue #136's own per-contribution splice) —
/// `Cow::Borrowed(row)` unchanged for a relationship-free shape (the
/// overwhelmingly common case, no allocation), or a `Cow::Owned` clone with
/// every synthetic column set otherwise. Shared by [`forward_row_contribution`]
/// (a synthetic column a field reads) and, new to issue #137,
/// [`accumulate_changes`]'s own `derive_group_key` calls (a synthetic column
/// a `GROUP BY` key reads) — both need "this row, with every relationship
/// read this batch might need already resolved," so both call this once per
/// row/side rather than resolving twice.
///
/// Resolution is **per row, per call** — deliberately not hoisted or cached
/// across `old_row`/`new_row` for the same change — because a row's own
/// join-key column (`from_col`) can itself differ between its old and new
/// image within one folded update (a re-point): each side must resolve
/// against *its own* `from_col` value, so the old state is computed under the
/// old parent and the new one under the new parent, exactly the "subtract
/// under old membership, add under new membership" rule the module doc
/// comment's grain-migration section already establishes for the row's
/// `GROUP BY` columns generally.
///
/// A from-row whose `from_col` is absent/NULL, or whose join key has no match
/// in `rel_ctx`'s settled projection, resolves the synthetic column to `None`
/// (SQL `NULL`) — the same "no match" LEFT JOIN semantics
/// [`eval::eval_expr`]'s own `RelationshipPath` arm and
/// `super::apply::augment_row_with_relationship_value` both already
/// establish, not a hard error: a synthetic column always exists on the
/// augmented row, it just has no resolved value for this particular
/// row/side.
///
/// # Panics
///
/// If `shape.synthetic` is non-empty but `rel_ctx` is `None` — see
/// [`build_forward_relationship_shape`]'s own panic doc for why that's
/// unreachable for a real caller.
fn augment_row_with_forward_relationships<'a>(
    row: &'a Row,
    rel_ctx: Option<&eval::RelationshipContext>,
    synthetic: &[ForwardRelationshipSynthetic],
) -> Cow<'a, Row> {
    if synthetic.is_empty() {
        return Cow::Borrowed(row);
    }
    let ctx = rel_ctx.expect(
        "ForwardRelationshipShape has synthetic columns but no relationship context was \
         supplied (should be unreachable — build_forward_relationship_shape already \
         requires one to build a non-empty shape)",
    );
    let mut augmented = row.clone();
    for s in synthetic {
        let value = row.get(&s.from_col).cloned().flatten().and_then(|key| {
            ctx.to_one(&s.rel_name)
                .and_then(|r| r.to_rows_by_key.get(&key))
                .and_then(|to_row| to_row.get(&s.to_col))
                .cloned()
                .flatten()
        });
        augmented.insert(s.synthetic.clone(), value);
    }
    Cow::Owned(augmented)
}

/// One (already-augmented, see [`augment_row_with_forward_relationships`])
/// row's contribution against `shape` (issue #136) — a thin
/// [`row_contribution`] wrapper against `shape.contribution_def` (the
/// relationship-substituted rewrite [`build_forward_relationship_shape`]
/// built).
fn forward_row_contribution(
    shape: &ForwardRelationshipShape,
    row: &Row,
    regex_cache: &mut RegexCache,
) -> Result<HashMap<String, Option<String>>, ApplyError> {
    row_contribution(
        &shape.contribution_def,
        row,
        &shape.source_columns,
        regex_cache,
    )
}

/// Folds `changes` (one aggregate definition's slice of one drain's folded
/// changes) into `plan`'s per-group deltas — Phase 2's aggregate
/// counterpart to [`super::apply::compute`]'s per-key evaluation loop.
///
/// `rows`/`old_rows` are the same decoded images `compute` already produced
/// for every change (shared across every definition subscribed to this
/// source, 1-1 or aggregate alike — issue #69's rule): `rows[i]` is the
/// change's *new*-side row (the decoded `new_image`, or `None` for a real
/// delete, or a live re-read for an image-less change — the exact three
/// shapes `compute`'s own doc comment enumerates), `old_rows[i]` is the
/// decoded `old_image`, or `None` when the change carried none. For an
/// image-less change it is the decoded prior-image hint (issue #315) when
/// the change has one, and `None` otherwise (the prior state is genuinely
/// unknown — see the module doc comment).
///
/// `rel_ctx` (issue #136) is the settled-parent-projection-backed
/// [`eval::RelationshipContext`] `super::apply::compute`'s aggregate branch
/// built via [`super::apply::build_relationship_context`] — `Some` whenever
/// `def` reads any relationship ([`eval::relationship_references`]
/// non-empty, the same gate a `KeySpace::OneToOne` definition uses),
/// regardless of whether `plan.rel_joins` ends up needing it for any given
/// change; `None` for the overwhelmingly common relationship-free
/// definition. [`build_forward_relationship_shape`] is the only reader.
#[allow(clippy::too_many_arguments)]
pub(super) fn accumulate_changes(
    plan: &mut AggregateTargetPlan,
    def: &TransformDef,
    changes: &[&FoldedChange],
    rows: &[Option<Row>],
    old_rows: &[Option<Row>],
    source_columns: &HashMap<String, ValueType>,
    regex_cache: &mut RegexCache,
    rel_ctx: Option<&eval::RelationshipContext>,
) -> Result<(), ApplyError> {
    let KeySpace::Aggregate { group_by } = &def.key_space else {
        panic!("accumulate_changes called on a non-aggregate definition");
    };
    // Issue #137: the row-column name `derive_group_key` should read for
    // each `GROUP BY` key, once a row may need augmenting first (a plain
    // key's own name, unchanged; a relationship-path key's synthetic
    // column). Computed once per batch, alongside `shape` below.
    let group_by_cols = group_by_row_columns(group_by);
    // Built once per batch, not once per [`row_contribution`] call (every
    // change needs it, and it's the same rewrite of the same `def` every
    // time) — see [`contribution_def`]'s doc comment for why every `AVG`
    // field is evaluated as the equivalent `SUM` here. Issue #136: for a
    // relationship-reading plan, this is also where every `RelationshipPath`
    // this batch might need gets rewritten to a synthetic `Column` up front
    // — see [`build_forward_relationship_shape`]'s own doc comment. A
    // relationship-free plan gets back a shape whose `contribution_def`/
    // `source_columns` are byte-identical to the pre-#136 rewrite, so
    // [`forward_row_contribution`] below is a plain passthrough to
    // [`row_contribution`] for the overwhelmingly common case.
    let shape = build_forward_relationship_shape(def, &plan.rel_joins, rel_ctx, source_columns);

    for (i, change) in changes.iter().enumerate() {
        let is_image_less = change.old_image.is_none() && change.new_image.is_none();
        let old_row = &old_rows[i];
        let new_row = &rows[i];

        if is_image_less {
            // Issue #315: an image-less change can still carry the key's
            // prior image (`FoldedChange::prior_image`, decoded into
            // `old_row` by `compute`) when it was staged by an upstream
            // target write. A row that moved groups, or was deleted, has to
            // leave its old group correct too, and the live re-read below
            // only names the new one — so the prior image's group is
            // re-derived from live state as well. Idempotent either way.
            if let Some(row) = old_row {
                let augmented =
                    augment_row_with_forward_relationships(row, rel_ctx, &shape.synthetic);
                let (values, key) =
                    derive_group_key(&augmented, &group_by_cols, &plan.group_by_types);
                let group = plan
                    .groups
                    .entry(key)
                    .or_insert_with(|| GroupPlan::new(values));
                group.force_full_recompute = true;
                group.hop_gen = group.hop_gen.max(change.hop_gen);
                group.src_changed =
                    super::apply::earliest_src_changed(group.src_changed, change.src_changed);
            }
            if let Some(row) = new_row {
                // Issue #137: even on the full-recompute path, this group's
                // *key* — used below to bind the bulk recompute's keyset
                // (`apply_forced_groups_bulk`) — must resolve a relationship
                // `GROUP BY` key's value the same guarded way an ordinary
                // delta does, not read straight off the row (which, for a
                // relationship-path key, has no such column at all).
                let augmented =
                    augment_row_with_forward_relationships(row, rel_ctx, &shape.synthetic);
                let (values, key) =
                    derive_group_key(&augmented, &group_by_cols, &plan.group_by_types);
                let group = plan
                    .groups
                    .entry(key)
                    .or_insert_with(|| GroupPlan::new(values));
                group.force_full_recompute = true;
                group.hop_gen = group.hop_gen.max(change.hop_gen);
                group.src_changed =
                    super::apply::earliest_src_changed(group.src_changed, change.src_changed);
            }
            continue;
        }

        match (old_row, new_row) {
            (None, Some(new_row)) => {
                let augmented =
                    augment_row_with_forward_relationships(new_row, rel_ctx, &shape.synthetic);
                let (values, key) =
                    derive_group_key(&augmented, &group_by_cols, &plan.group_by_types);
                let contrib = forward_row_contribution(&shape, &augmented, regex_cache)?;
                let group = plan
                    .groups
                    .entry(key)
                    .or_insert_with(|| GroupPlan::new(values));
                group.hop_gen = group.hop_gen.max(change.hop_gen);
                group.src_changed =
                    super::apply::earliest_src_changed(group.src_changed, change.src_changed);
                add_contributions(&plan.fields, group, &contrib);
            }
            (Some(old_row), None) => {
                let augmented =
                    augment_row_with_forward_relationships(old_row, rel_ctx, &shape.synthetic);
                let (values, key) =
                    derive_group_key(&augmented, &group_by_cols, &plan.group_by_types);
                let contrib = forward_row_contribution(&shape, &augmented, regex_cache)?;
                let group = plan
                    .groups
                    .entry(key)
                    .or_insert_with(|| GroupPlan::new(values));
                group.hop_gen = group.hop_gen.max(change.hop_gen);
                group.src_changed =
                    super::apply::earliest_src_changed(group.src_changed, change.src_changed);
                sub_contributions(&plan.fields, group, &contrib);
            }
            (Some(old_row), Some(new_row)) => {
                let old_augmented =
                    augment_row_with_forward_relationships(old_row, rel_ctx, &shape.synthetic);
                let new_augmented =
                    augment_row_with_forward_relationships(new_row, rel_ctx, &shape.synthetic);
                let (old_values, old_key) =
                    derive_group_key(&old_augmented, &group_by_cols, &plan.group_by_types);
                let (new_values, new_key) =
                    derive_group_key(&new_augmented, &group_by_cols, &plan.group_by_types);
                let old_contrib = forward_row_contribution(&shape, &old_augmented, regex_cache)?;
                let new_contrib = forward_row_contribution(&shape, &new_augmented, regex_cache)?;

                if old_key == new_key {
                    let group = plan
                        .groups
                        .entry(new_key)
                        .or_insert_with(|| GroupPlan::new(new_values));
                    group.hop_gen = group.hop_gen.max(change.hop_gen);
                    group.src_changed =
                        super::apply::earliest_src_changed(group.src_changed, change.src_changed);
                    diff_contributions(&plan.fields, group, &old_contrib, &new_contrib);
                } else {
                    // Grain migration: subtract from the old group, add to
                    // the new one, independently.
                    {
                        let old_group = plan
                            .groups
                            .entry(old_key)
                            .or_insert_with(|| GroupPlan::new(old_values));
                        old_group.hop_gen = old_group.hop_gen.max(change.hop_gen);
                        old_group.src_changed = super::apply::earliest_src_changed(
                            old_group.src_changed,
                            change.src_changed,
                        );
                        sub_contributions(&plan.fields, old_group, &old_contrib);
                    }
                    {
                        let new_group = plan
                            .groups
                            .entry(new_key)
                            .or_insert_with(|| GroupPlan::new(new_values));
                        new_group.hop_gen = new_group.hop_gen.max(change.hop_gen);
                        new_group.src_changed = super::apply::earliest_src_changed(
                            new_group.src_changed,
                            change.src_changed,
                        );
                        add_contributions(&plan.fields, new_group, &new_contrib);
                    }
                }
            }
            (None, None) => {
                // Not image-less (handled above) but both decoded rows are
                // absent: a genuine CDC delete for a key already gone by
                // the time this batch's live re-read would have run, or
                // similar — there is no row to derive a group from and
                // nothing to subtract. Nothing to do.
            }
        }
    }

    Ok(())
}

/// Whether one field's per-row `row_contribution` text should actually be
/// pushed into a [`FieldAccum`]'s `adds`/`subs` (issue #120's `COUNT(<expr>)`
/// wrinkle).
///
/// `Sum`/`Avg` (and `COUNT(*)`) get their "skip this row" signal for free
/// from `row_contribution`'s `None`: a `NULL` argument folds to `None`
/// (Postgres's own "aggregate of zero non-null values is NULL" rule,
/// `eval::reduce_numeric_aggregate`'s empty-`values` check), and `COUNT(*)`
/// never skips at all. `COUNT(<expr>)` (issue #120) cannot reuse that same
/// signal: `count(x)` is *never* `NULL` — a `GROUP BY` group's true
/// `COUNT(<expr>)` value is `0`, not `NULL`, when every row's `expr` is
/// `NULL` — so `eval::eval_aggregate_expr`'s `COUNT(<expr>)` arm, reused here
/// via `row_contribution`'s single-row-slice trick, always returns
/// `Some("0")` or `Some("1")` per row (never `None`), one bit encoding
/// whether *this row's* argument was itself non-null. For `Count`
/// specifically, then, a `"0"` contribution means "skip this row" — the
/// direct counterpart of `Sum`/`Avg`'s `None` — while a real `Sum`/`Avg`
/// contribution of literal `"0"` (e.g. `SUM(amount)` where `amount = 0`) must
/// still be pushed: `0` is a perfectly ordinary non-null value to sum, and
/// treating it as "skip" would wrongly discard it from
/// [`group_has_activity`]'s "this field had real activity" signal (a
/// genuine `SUM(amount) = 0` group would then look inactive on any row whose
/// contribution rounded to exactly `0`). So the special-case is scoped to
/// `AggFieldKind::Count` alone.
fn is_pushable_contribution(kind: AggFieldKind, text: &str) -> bool {
    kind != AggFieldKind::Count || text != "0"
}

/// Registers `contrib`'s row as having entered `group`: every `Sum`/`Avg`/
/// `Count` field always gets a [`FieldAccum`] entry (even one that stays
/// empty, when this row's own contribution is NULL) — the entry's mere
/// *presence* is what tells [`group_has_activity`]/[`upsert_group`] "a live
/// row touched this field's group this batch," independent of whether that
/// row happened to add anything to `adds`. See [`FieldAccum`]'s doc comment
/// for why an empty pair is a legitimate zero-effect delta, not a reason to
/// skip the entry entirely.
/// `pub(super)`: issue #137's reverse fast-path fix also calls this directly
/// — a relationship-path `GROUP BY` key can move a from-side row into a
/// *different* group as a pure side effect of the to-side row's own change
/// (no change to the from-side row itself), which needs the same "this row
/// entered a group" bookkeeping `accumulate_changes`'s own grain-migration
/// branch already gets, just driven by the reverse shape's old/new parent
/// images instead of a live re-read.
pub(super) fn add_contributions(
    fields: &[AggFieldPlan],
    group: &mut GroupPlan,
    contrib: &HashMap<String, Option<String>>,
) {
    for field in fields {
        if !matches!(
            field.kind,
            AggFieldKind::Sum | AggFieldKind::Avg | AggFieldKind::Count
        ) {
            continue;
        }
        let accum = group.field_accum.entry(field.name.clone()).or_default();
        if let Some(v) = contrib.get(&field.name).cloned().flatten()
            && is_pushable_contribution(field.kind, &v)
        {
            accum.adds.push(v);
        }
    }
}

/// [`add_contributions`]'s counterpart for a row leaving `group` — see that
/// function's doc comment; the same "always register the entry, only
/// conditionally push into it" rule applies here for `subs`. `pub(super)`
/// for the same issue #137 reason as [`add_contributions`].
pub(super) fn sub_contributions(
    fields: &[AggFieldPlan],
    group: &mut GroupPlan,
    contrib: &HashMap<String, Option<String>>,
) {
    for field in fields {
        if !matches!(
            field.kind,
            AggFieldKind::Sum | AggFieldKind::Avg | AggFieldKind::Count
        ) {
            continue;
        }
        let accum = group.field_accum.entry(field.name.clone()).or_default();
        if let Some(v) = contrib.get(&field.name).cloned().flatten()
            && is_pushable_contribution(field.kind, &v)
        {
            accum.subs.push(v);
        }
    }
}

/// Diffs one row's contribution before (`old_contrib`) and after
/// (`new_contrib`) some change **that does not move the row into or out of
/// `group`** — factored out of `accumulate_changes`'s own in-place-update
/// branch (an ordinary same-group CDC `UPDATE`) so issue #131's
/// reverse-delta apply can reuse the exact same per-field cancellation
/// rule, which it needs for a different reason than `accumulate_changes`
/// does: a parent-only change usually never adds or removes a from-side row
/// from its group at all (the row's own `GROUP BY` columns don't read the
/// relationship, so they never change), so every field must be diffed
/// individually rather than blindly subtracted-then-added via
/// [`sub_contributions`]/[`add_contributions`] — otherwise a field the
/// relationship doesn't even touch (e.g. a plain `COUNT(*)`, whose
/// contribution is `1` regardless of any relationship value) would wrongly
/// gain a net +1/-1 delta on every reverse apply that touches its group,
/// double-counting it against the value backfill/an earlier apply already
/// established. Issue #137: when a `GROUP BY` key *does* read the changing
/// relationship, this function is only the *same-group* half of the reverse
/// apply's own grain-migration split — see `build_reverse_relationship_shape`'s
/// `diff_pass`, which calls this only when a row's old and new group keys
/// agree, and falls back to [`sub_contributions`]/[`add_contributions`]
/// against two different groups when they don't.
///
/// An unchanged contribution (`old_v == new_v`, always true for a field the
/// change doesn't touch) pushes no accumulator entry at all — not a
/// zero-effect add/sub pair — which is what lets [`apply_aggregate_target`]
/// tell "this field had no activity this batch" from "activity that
/// happened to net to zero" (`group_has_activity`'s own distinction).
pub(super) fn diff_contributions(
    fields: &[AggFieldPlan],
    group: &mut GroupPlan,
    old_contrib: &HashMap<String, Option<String>>,
    new_contrib: &HashMap<String, Option<String>>,
) {
    for field in fields {
        if !matches!(
            field.kind,
            AggFieldKind::Sum | AggFieldKind::Avg | AggFieldKind::Count
        ) {
            continue;
        }
        let old_v = old_contrib.get(&field.name).cloned().flatten();
        let new_v = new_contrib.get(&field.name).cloned().flatten();
        if old_v == new_v {
            continue;
        }
        let accum = group.field_accum.entry(field.name.clone()).or_default();
        if let Some(v) = new_v
            && is_pushable_contribution(field.kind, &v)
        {
            accum.adds.push(v);
        }
        if let Some(v) = old_v
            && is_pushable_contribution(field.kind, &v)
        {
            accum.subs.push(v);
        }
    }
}

// ---------------------------------------------------------------------
// Phase 3: per-group apply
// ---------------------------------------------------------------------

/// One group [`apply_aggregate_target`]'s writers physically wrote or
/// deleted: its [`derive_group_key`] text, and the [`GroupPlan::hop_gen`]/
/// [`GroupPlan::src_changed`] its downstream `Recompute` carries forward
/// (issue #104: the earliest origin among the changes that touched it).
type TouchedGroup = (String, i32, Option<std::time::SystemTime>);

/// A `col IS NOT DISTINCT FROM $n::text::<cast>` clause per `group_by`
/// (**target**-side, plain column name) entry, starting at `$start` — `IS
/// NOT DISTINCT FROM` rather than `=` so a `NULL` grouping column value
/// (legal in Postgres, though this grammar's own [`derive_group_key`]
/// doesn't attempt `GROUP BY`'s "NULLs group together" semantics beyond
/// this) never silently fails to match itself. Used only against the target
/// table itself ([`delete_group_row`]), whose columns are always real,
/// unqualified names — even a [`GroupByKey::RelationshipPath`] key's target
/// column (its tail `column`) needs no join here, since it's just an
/// ordinary column on the target row. See [`group_where_clause_source`] for
/// the join-aware counterpart a probe against `source` needs instead.
fn group_where_clause(group_by: &[String], group_by_types: &[ValueType], start: usize) -> String {
    group_by
        .iter()
        .zip(group_by_types)
        .enumerate()
        .map(|(i, (col, ty))| {
            format!(
                "{} is not distinct from ${}::text::{}",
                quote_ident(col),
                start + i,
                ddl::pg_type_name(*ty)
            )
        })
        .collect::<Vec<_>>()
        .join(" and ")
}

/// [`plan.group_by_source`](AggregateTargetPlan::group_by_source), rendered
/// against `alias` — a plain column qualified by `alias`, or (issue #137) a
/// relationship-path key resolved through its own join alias, exactly like a
/// `RecomputeOnly` field's own expression ([`render_agg_expr`]). Shared by
/// every statement below that reads `source` (optionally `LEFT JOIN`ed to
/// `plan.rel_joins`), so a `GROUP BY` key's source-side reference is rendered
/// consistently everywhere, not just where a field happens to need one.
fn group_by_source_cols(plan: &AggregateTargetPlan, alias: &str) -> Vec<String> {
    plan.group_by_source
        .iter()
        .map(|e| oracle::render_to_one_rel_expr_sql(e, alias))
        .collect()
}

/// [`group_where_clause`]'s source-side counterpart (issue #137): a `col IS
/// NOT DISTINCT FROM $n::text::<cast>` clause per [`group_by_source_cols`]
/// entry. Always aliases `source` as `alias` and renders every position
/// relationship-aware — a relationship-free plan's `GROUP BY` keys are all
/// plain columns, so this differs from the pre-#137 unaliased rendering only
/// by the harmless addition of `alias.` in front of each one (unambiguous
/// SQL either way, since `alias` is the sole `FROM`-clause item when there
/// are no `rel_joins`).
fn group_where_clause_source(plan: &AggregateTargetPlan, start: usize, alias: &str) -> String {
    group_by_source_cols(plan, alias)
        .into_iter()
        .zip(&plan.group_by_types)
        .enumerate()
        .map(|(i, (col_sql, ty))| {
            format!(
                "{col_sql} is not distinct from ${}::text::{}",
                start + i,
                ddl::pg_type_name(*ty)
            )
        })
        .collect::<Vec<_>>()
        .join(" and ")
}

fn group_where_params(values: &[Option<String>]) -> Vec<&(dyn ToSql + Sync)> {
    values.iter().map(|v| v as &(dyn ToSql + Sync)).collect()
}

/// The ` left join <to_table> as <rel> on ...` clauses `plan.rel_joins`
/// needs, aliased `s` — shared by every source-probing statement in this
/// module, so a relationship (whether read by a field or, issue #137, by a
/// `GROUP BY` key) is always joined the same way. Empty for a
/// relationship-free plan, leaving every caller's SQL unaffected beyond the
/// harmless `s` alias [`group_where_clause_source`]/[`group_by_source_cols`]
/// already add unconditionally.
fn rel_joins_sql(plan: &AggregateTargetPlan) -> String {
    oracle::to_one_join_clauses(
        plan.rel_joins.iter().map(|j| {
            (
                j.name.as_str(),
                j.to_table.as_str(),
                j.to_col.as_str(),
                j.from_col.as_str(),
            )
        }),
        "s",
    )
}

/// Whether `source` still has any row for this group — the extinction/
/// creation test: a group's existence is defined by "does the source have
/// any row with these `GROUP BY` values", independent of which fields are
/// declared, so this is checked once per touched group regardless of field
/// shape, rather than inferred from any one field's own delta. A
/// relationship-path `GROUP BY` key (issue #137) needs its to-side table
/// joined in here too — a plain-column key's existence never depended on
/// one, but a relationship key's group is only real when *some* joined row
/// still matches it.
async fn probe_group_exists(
    txn: &Transaction<'_>,
    plan: &AggregateTargetPlan,
    values: &[Option<String>],
) -> Result<bool, ApplyError> {
    let where_sql = group_where_clause_source(plan, 1, "s");
    let sql = format!(
        "select exists(select 1 from {} s{} where {where_sql})",
        ddl::qualified_source_table(&plan.source),
        rel_joins_sql(plan),
    );
    let row = txn.query_one(&sql, &group_where_params(values)).await?;
    Ok(row.get(0))
}

/// Deletes this group's target row, if it still has one, returning whether
/// it did. (The row's prior image, which a downstream aggregate needs to find
/// the group it left, came from [`apply_aggregate_target`]'s pre-lock —
/// issue #315.)
async fn delete_group_row(
    txn: &Transaction<'_>,
    target: &str,
    group_by: &[String],
    group_by_types: &[ValueType],
    values: &[Option<String>],
) -> Result<bool, ApplyError> {
    let where_sql = group_where_clause(group_by, group_by_types, 1);
    // `target` is always [`AggregateTargetPlan::target`]'s qualified
    // identity by the time this is called (reviewer follow-up to issue #74)
    // — quoted component-independently via
    // [`ddl::qualified_target_table_ident`], not a bare `quote_ident`.
    let sql = format!(
        "delete from {} where {where_sql}",
        ddl::qualified_target_table_ident(target)
    );
    Ok(txn.execute(&sql, &group_where_params(values)).await? > 0)
}

/// Probes one [`AggFieldKind::RecomputeOnly`] (or, on the full-recompute
/// path, any) field's current value for one group directly from the source
/// — `select (<rendered expr>)::text from source where <group columns> =
/// ...`, scoped to just this group, never a full-table scan. Rendering via
/// [`render_agg_expr`] — the same relationship-aware branch
/// [`apply_forced_groups_bulk`]/[`probe_recompute_fields_bulk`] use — means
/// this is not a second implementation of the field's semantics: it is the
/// exact SQL the correctness oracle itself would run for this field,
/// evaluated with no `GROUP BY` because the `WHERE` clause has already
/// restricted the input to exactly one group. A plan with to-one
/// relationship joins (issue #94) needs its to-side table joined in here
/// too — reachable whenever an ordinary (non-forced) delta touches exactly
/// one group with a `RecomputeOnly` field reading a relationship path (this
/// module's whole-branch-review regression, epic #127: issue #131's design
/// scoped `MIN`/`MAX`-over-relationship out of the reverse fast path, and
/// issue #136 then routed such a group through the *forward* ordinary delta
/// path instead of the old always-joins-correctly `force_every_group`) — so
/// this mirrors those bulk builders' `rel_joins_sql`/aliasing rather than
/// assuming an unaliased, join-free source scan.
async fn probe_field_value(
    txn: &Transaction<'_>,
    plan: &AggregateTargetPlan,
    expr: &Expr,
    values: &[Option<String>],
) -> Result<Option<String>, ApplyError> {
    let expr_sql = render_agg_expr(plan, expr);
    let source_ident = ddl::qualified_source_table(&plan.source);
    let where_sql = group_where_clause_source(plan, 1, "s");
    let sql = format!(
        "select ({expr_sql})::text from {source_ident} s{} where {where_sql}",
        rel_joins_sql(plan),
    );
    let row = txn.query_one(&sql, &group_where_params(values)).await?;
    Ok(row.get(0))
}

/// Probes a `SUM`/`AVG` field's raw `sum`/`count` of non-null argument
/// values for one group — the full-recompute path's counterpart to the
/// ordinary delta path's incremented hidden partials, used only when
/// [`GroupPlan::force_full_recompute`] is set (see the module doc comment).
/// Shared by both [`AggFieldKind::Sum`] and [`AggFieldKind::Avg`]: both need
/// exactly this pair (a running sum and a running non-null count) to keep
/// their hidden partials consistent with what a fresh delta from this point
/// forward would produce; `SUM` just derives its visible column directly
/// from the sum half rather than dividing. `count` is `NOT NULL` in
/// Postgres's own `count()` semantics, so it binds as a plain `i64`; `sum`
/// follows the same "NULL iff zero non-null values" rule as everywhere else
/// in this module. `expr`'s own argument is rendered via [`render_agg_expr`]
/// (issue #137 fix: previously the relationship-unaware
/// [`oracle::render_expr_sql`] directly, which would panic for a `SUM`/`AVG`
/// over a relationship path reaching this — the full-recompute, single-group
/// probe path — the same way `probe_field_value` already renders its own
/// expression), and the `GROUP BY` filter joins `plan.rel_joins` exactly like
/// every other source probe in this module.
async fn probe_sum_and_count(
    txn: &Transaction<'_>,
    plan: &AggregateTargetPlan,
    expr: &Expr,
    values: &[Option<String>],
) -> Result<(Option<String>, i64), ApplyError> {
    let Expr::FunctionCall { args, .. } = expr else {
        panic!("probe_sum_and_count called on a non-SUM/AVG field");
    };
    let arg_sql = render_agg_expr(plan, &args[0]);
    let where_sql = group_where_clause_source(plan, 1, "s");
    let sql = format!(
        "select sum({arg_sql})::text, count({arg_sql}) from {} s{} where {where_sql}",
        ddl::qualified_source_table(&plan.source),
        rel_joins_sql(plan),
    );
    let row = txn.query_one(&sql, &group_where_params(values)).await?;
    Ok((row.get(0), row.get(1)))
}

/// Probes an [`AggFieldKind::Count`] field's raw count for one group
/// directly from the source — the full-recompute path's counterpart to the
/// ordinary delta path's incremented visible column, used only when
/// [`GroupPlan::force_full_recompute`] is set. `expr` is the field's own
/// (substituted) `COUNT(*)`/`COUNT(<arg>)` call: `COUNT(*)` has no argument
/// expression to render and always probes `count(*)`, matching
/// [`crate::defs::oracle::render_expr_sql`]'s own `COUNT(*)` rendering;
/// `COUNT(<arg>)` (issue #120) renders `count(<arg>)`, `arg` rendered via
/// [`render_agg_expr`] exactly like [`probe_sum_and_count`]'s own argument —
/// relationship-aware, so a `COUNT(post.word_count)`-shaped field reaching
/// this (rather than being folded via the ordinary delta path) still
/// resolves correctly. Joins `plan.rel_joins` like every other source probe,
/// needed when a `GROUP BY` key (issue #137), not any field, is what reads
/// the relationship.
async fn probe_count(
    txn: &Transaction<'_>,
    plan: &AggregateTargetPlan,
    expr: &Expr,
    values: &[Option<String>],
) -> Result<i64, ApplyError> {
    let Expr::FunctionCall { args, .. } = expr else {
        panic!("probe_count called on a non-COUNT field");
    };
    let count_sql = match args.first() {
        Some(arg) => format!("count({})", render_agg_expr(plan, arg)),
        None => "count(*)".to_string(),
    };
    let where_sql = group_where_clause_source(plan, 1, "s");
    let sql = format!(
        "select {count_sql} from {} s{} where {where_sql}",
        ddl::qualified_source_table(&plan.source),
        rel_joins_sql(plan),
    );
    let row = txn.query_one(&sql, &group_where_params(values)).await?;
    Ok(row.get(0))
}

/// `(select coalesce(sum(v), 0) from unnest($n::text[]::numeric[]) v)` — one
/// [`FieldAccum`] side (adds or subs), summed in SQL rather than in Rust:
/// this crate's `Numeric` (issue #24) implements addition and comparison
/// but not subtraction, having never needed it before this delta model, and
/// letting Postgres cast-and-sum a text array is one line here against
/// growing that type for a single caller — matching this module's existing
/// "text in, typed cast in SQL" convention (`super::apply`'s own doc
/// comment) rather than a new one.
fn sum_array_expr(param: usize) -> String {
    format!("(select coalesce(sum(v), 0) from unnest(${param}::text[]::numeric[]) v)")
}

/// Builds and runs one group's combined ordered-lock-free upsert (Phase 3
/// processes groups sequentially in ascending group-key order — see
/// [`apply_aggregate_target`] — which is what gives this the same
/// ascending-lock-order guarantee [`super::apply::apply_target`]'s
/// multi-row pre-lock CTE gives the 1-1 path, without needing that CTE's
/// machinery generalized to an arbitrary-arity composite key): one `INSERT
/// ... ON CONFLICT DO UPDATE` per group, whose `SET`/`VALUES` expressions
/// are increments (`col = col + delta`) for [`AggFieldKind::Sum`]/[`AggFieldKind::Avg`]
/// fields with an accumulated delta, and literal probed values for
/// [`AggFieldKind::RecomputeOnly`] fields (or, on the full-recompute path,
/// every field). A field with neither an accumulated delta nor a probed
/// value is omitted from the statement entirely, leaving its column
/// untouched — see [`FieldAccum`]'s doc comment on why that, not a `0`, is
/// what preserves "sum of nothing is NULL".
/// One column-group's write strategy, decided (and — for the two `Forced`
/// variants — probed) in [`upsert_group`]'s first pass, before that
/// function's second pass borrows into `probed`/`sum_count_probed`/`count_star_probed`/`count_deltas`
/// to build `params`: a probe result pushed into one of those owned vectors
/// *after* an earlier probe's result has already been borrowed by `params`
/// would violate the borrow checker (`Vec::push` needs `&mut`, an
/// outstanding `&probed[i]` needs `&`), so every probe this group's write
/// needs must complete, into vectors that are never touched again, before
/// any of them may be borrowed.
enum ColumnPlan {
    SumDelta(String, usize),
    SumForced(String, usize),
    AvgDelta(String, usize),
    AvgForced(String, usize),
    CountDelta(String, usize),
    CountForced(String, usize),
    Recompute(String, ValueType, usize),
}

async fn upsert_group(
    txn: &Transaction<'_>,
    target: &str,
    plan: &AggregateTargetPlan,
    group: &GroupPlan,
) -> Result<bool, ApplyError> {
    let pk_idents: Vec<String> = plan.group_by.iter().map(|c| quote_ident(c)).collect();
    // `target` is always [`AggregateTargetPlan::target`]'s qualified
    // identity by the time this is called (reviewer follow-up to issue #74).
    let target_ident = ddl::qualified_target_table_ident(target);

    // Pass 1: decide each field's strategy, running every probe this group's
    // write needs and collecting their results — no SQL text or `params`
    // built yet, so no borrow of `probed`/`sum_count_probed`/`count_star_probed`/`count_deltas` exists
    // while they're still being pushed into. See [`ColumnPlan`]'s doc
    // comment.
    let mut probed: Vec<Option<String>> = Vec::new();
    let mut sum_count_probed: Vec<(Option<String>, i64)> = Vec::new();
    let mut count_star_probed: Vec<i64> = Vec::new();
    let mut count_deltas: Vec<i64> = Vec::new();
    let mut columns: Vec<ColumnPlan> = Vec::new();

    for field in &plan.fields {
        match field.kind {
            AggFieldKind::Sum => {
                let has_accum = group.field_accum.contains_key(&field.name);
                // A group forced onto the full-recompute path (an
                // image-less change — see the module doc comment) must
                // still probe this field even with no `field_accum` entry:
                // skipping it here (as if it had no activity this batch)
                // would leave both its visible column and its hidden count
                // partial stale, silently diverging from the group's real
                // current state — the exact bug the [`AggFieldKind::Avg`]
                // arm below has always guarded against.
                if !has_accum && !group.force_full_recompute {
                    continue;
                }
                if group.force_full_recompute {
                    let expr = &plan.field_exprs[field.name.as_str()];
                    let (sum_text, count) =
                        probe_sum_and_count(txn, plan, expr, &group.group_values).await?;
                    sum_count_probed.push((sum_text, count));
                    columns.push(ColumnPlan::SumForced(
                        field.name.clone(),
                        sum_count_probed.len() - 1,
                    ));
                } else {
                    let accum = group.field_accum.get(&field.name).expect("checked above");
                    count_deltas.push(accum.adds.len() as i64 - accum.subs.len() as i64);
                    columns.push(ColumnPlan::SumDelta(
                        field.name.clone(),
                        count_deltas.len() - 1,
                    ));
                }
            }
            AggFieldKind::Avg => {
                let has_accum = group.field_accum.contains_key(&field.name);
                if !has_accum && !group.force_full_recompute {
                    continue;
                }
                if group.force_full_recompute {
                    let expr = &plan.field_exprs[field.name.as_str()];
                    let (sum_text, count) =
                        probe_sum_and_count(txn, plan, expr, &group.group_values).await?;
                    sum_count_probed.push((sum_text, count));
                    columns.push(ColumnPlan::AvgForced(
                        field.name.clone(),
                        sum_count_probed.len() - 1,
                    ));
                } else {
                    let accum = group.field_accum.get(&field.name).expect("checked above");
                    count_deltas.push(accum.adds.len() as i64 - accum.subs.len() as i64);
                    columns.push(ColumnPlan::AvgDelta(
                        field.name.clone(),
                        count_deltas.len() - 1,
                    ));
                }
            }
            AggFieldKind::Count => {
                let has_accum = group.field_accum.contains_key(&field.name);
                if !has_accum && !group.force_full_recompute {
                    continue;
                }
                if group.force_full_recompute {
                    let expr = &plan.field_exprs[field.name.as_str()];
                    let count = probe_count(txn, plan, expr, &group.group_values).await?;
                    count_star_probed.push(count);
                    columns.push(ColumnPlan::CountForced(
                        field.name.clone(),
                        count_star_probed.len() - 1,
                    ));
                } else {
                    let accum = group.field_accum.get(&field.name).expect("checked above");
                    count_deltas.push(accum.adds.len() as i64 - accum.subs.len() as i64);
                    columns.push(ColumnPlan::CountDelta(
                        field.name.clone(),
                        count_deltas.len() - 1,
                    ));
                }
            }
            AggFieldKind::RecomputeOnly => {
                let expr = &plan.field_exprs[field.name.as_str()];
                let value = probe_field_value(txn, plan, expr, &group.group_values).await?;
                probed.push(value);
                columns.push(ColumnPlan::Recompute(
                    field.name.clone(),
                    field.value_type,
                    probed.len() - 1,
                ));
            }
        }
    }

    // Pass 2: every probe is done and `probed`/`sum_count_probed`/
    // `count_deltas` will not be mutated again, so borrowing into them for
    // `params` is safe now.
    let mut insert_cols: Vec<String> = pk_idents.clone();
    let mut insert_exprs: Vec<String> = Vec::new();
    let mut update_sets: Vec<String> = Vec::new();
    let mut params: Vec<&(dyn ToSql + Sync)> = group_where_params(&group.group_values);
    let mut next = params.len() + 1;
    // Issue #48: two fields (e.g. `SUM(amount)`/`AVG(amount)`) can share one
    // hidden count column (`plan.count_column_names`) — this tracks which
    // shared names this statement has already written a column/SET clause
    // for, so a second field sharing that name never emits a duplicate
    // `insert into ... (..., count_col, ..., count_col, ...)` / `set count_col
    // = ..., count_col = ...`, which Postgres rejects outright. The first
    // field to reach a given shared column always wins; since every field
    // sharing a column aggregates the identical argument, their independently
    // computed count deltas for it are numerically identical anyway (see
    // `ddl::count_column_names`'s doc comment), so it does not matter which
    // one's expression is the one actually written.
    let mut emitted_count_cols: std::collections::HashSet<String> =
        std::collections::HashSet::new();

    for column in &columns {
        match column {
            ColumnPlan::SumDelta(name, count_idx) => {
                let accum = &group.field_accum[name];
                let count_col_name = plan.count_column_names[name].clone();
                let count_col = quote_ident(&count_col_name);
                let col = quote_ident(name);

                let add_param = next;
                let sub_param = next + 1;
                params.push(&accum.adds);
                params.push(&accum.subs);
                next += 2;
                let sum_delta = format!(
                    "({} - {})",
                    sum_array_expr(add_param),
                    sum_array_expr(sub_param)
                );

                let count_param = next;
                params.push(&count_deltas[*count_idx]);
                next += 1;
                let count_delta = format!("${count_param}::bigint");

                let insert_count = count_delta.clone();
                // A newly-inserted group whose net count delta is zero has
                // no non-null contributions at all (Postgres's `sum()` over
                // zero rows is `NULL`, never `0` — see `is_sum_field`'s doc
                // comment), so the visible sum column must start `NULL`,
                // not the arithmetic (and here vacuous) `sum_delta`.
                let insert_sum =
                    format!("case when {insert_count} = 0 then null else {sum_delta} end");

                let update_count =
                    format!("coalesce({target_ident}.{count_col}, 0) + {count_delta}");
                let update_sum_raw = format!("coalesce({target_ident}.{col}, 0) + {sum_delta}");
                let update_sum =
                    format!("case when ({update_count}) = 0 then null else ({update_sum_raw}) end");

                insert_cols.push(col.clone());
                insert_exprs.push(insert_sum);
                if emitted_count_cols.insert(count_col_name.clone()) {
                    insert_cols.push(count_col.clone());
                    insert_exprs.push(insert_count);
                    update_sets.push(format!("{count_col} = {update_count}"));
                }

                update_sets.push(format!("{col} = {update_sum}"));
            }
            ColumnPlan::SumForced(name, idx) => {
                let count_col_name = plan.count_column_names[name].clone();
                let count_col = quote_ident(&count_col_name);
                let col = quote_ident(name);
                let sum_expr = format!("${next}::text::numeric");
                let count_expr = format!("${}::bigint", next + 1);
                params.push(&sum_count_probed[*idx].0);
                params.push(&sum_count_probed[*idx].1);
                next += 2;
                insert_cols.push(col.clone());
                insert_exprs.push(sum_expr.clone());
                if emitted_count_cols.insert(count_col_name.clone()) {
                    insert_cols.push(count_col.clone());
                    insert_exprs.push(count_expr.clone());
                    update_sets.push(format!("{count_col} = {count_expr}"));
                }
                update_sets.push(format!("{col} = {sum_expr}"));
            }
            ColumnPlan::AvgDelta(name, count_idx) => {
                let accum = &group.field_accum[name];
                let sum_col_name = avg_sum_column(name);
                let count_col_name = plan.count_column_names[name].clone();
                let sum_col = quote_ident(&sum_col_name);
                let count_col = quote_ident(&count_col_name);
                let avg_col = quote_ident(name);

                let add_param = next;
                let sub_param = next + 1;
                params.push(&accum.adds);
                params.push(&accum.subs);
                next += 2;
                let sum_delta = format!(
                    "({} - {})",
                    sum_array_expr(add_param),
                    sum_array_expr(sub_param)
                );

                let count_param = next;
                params.push(&count_deltas[*count_idx]);
                next += 1;
                let count_delta = format!("${count_param}::bigint");

                let insert_sum = sum_delta.clone();
                let insert_count = count_delta.clone();
                let insert_avg = format!(
                    "case when {insert_count} = 0 then null \
                     else {insert_sum} / ({insert_count})::numeric end"
                );

                let update_sum = format!("coalesce({target_ident}.{sum_col}, 0) + {sum_delta}");
                let update_count =
                    format!("coalesce({target_ident}.{count_col}, 0) + {count_delta}");
                let update_avg = format!(
                    "case when ({update_count}) = 0 then null \
                     else ({update_sum}) / ({update_count})::numeric end"
                );

                insert_cols.push(sum_col.clone());
                insert_exprs.push(insert_sum);
                if emitted_count_cols.insert(count_col_name.clone()) {
                    insert_cols.push(count_col.clone());
                    insert_exprs.push(insert_count);
                    update_sets.push(format!("{count_col} = {update_count}"));
                }
                insert_cols.push(avg_col.clone());
                insert_exprs.push(insert_avg);

                update_sets.push(format!("{sum_col} = {update_sum}"));
                update_sets.push(format!("{avg_col} = {update_avg}"));
            }
            ColumnPlan::AvgForced(name, idx) => {
                let sum_col_name = avg_sum_column(name);
                let count_col_name = plan.count_column_names[name].clone();
                let sum_col = quote_ident(&sum_col_name);
                let count_col = quote_ident(&count_col_name);
                let avg_col = quote_ident(name);

                let sum_expr = format!("${next}::text::numeric");
                let count_expr = format!("${}::bigint", next + 1);
                params.push(&sum_count_probed[*idx].0);
                params.push(&sum_count_probed[*idx].1);
                next += 2;
                let avg_expr = format!(
                    "case when {count_expr} = 0 then null \
                     else {sum_expr} / {count_expr}::numeric end"
                );
                insert_cols.push(sum_col.clone());
                insert_exprs.push(sum_expr.clone());
                if emitted_count_cols.insert(count_col_name.clone()) {
                    insert_cols.push(count_col.clone());
                    insert_exprs.push(count_expr.clone());
                    update_sets.push(format!("{count_col} = {count_expr}"));
                }
                insert_cols.push(avg_col.clone());
                insert_exprs.push(avg_expr.clone());
                update_sets.push(format!("{sum_col} = {sum_expr}"));
                update_sets.push(format!("{avg_col} = {avg_expr}"));
            }
            ColumnPlan::CountDelta(name, count_idx) => {
                let col = quote_ident(name);

                let count_param = next;
                params.push(&count_deltas[*count_idx]);
                next += 1;
                let count_delta = format!("${count_param}::bigint");

                // Issue #120: no `::numeric` cast — `col` is declared
                // `bigint`, matching Postgres's own `count()` return type.
                let update_count = format!("coalesce({target_ident}.{col}, 0) + {count_delta}");

                insert_cols.push(col.clone());
                insert_exprs.push(count_delta.clone());
                update_sets.push(format!("{col} = {update_count}"));
            }
            ColumnPlan::CountForced(name, idx) => {
                let col = quote_ident(name);
                // Issue #120: no trailing `::numeric` — `col` is declared
                // `bigint`.
                let count_expr = format!("${next}::bigint");
                params.push(&count_star_probed[*idx]);
                next += 1;
                insert_cols.push(col.clone());
                insert_exprs.push(count_expr.clone());
                update_sets.push(format!("{col} = {count_expr}"));
            }
            ColumnPlan::Recompute(name, value_type, idx) => {
                let col = quote_ident(name);
                let pg_type = ddl::pg_type_name(*value_type);
                let expr = format!("${next}::text::{pg_type}");
                insert_cols.push(col.clone());
                insert_exprs.push(expr.clone());
                update_sets.push(format!("{col} = {expr}"));
                params.push(&probed[*idx]);
                next += 1;
            }
        }
    }

    if update_sets.is_empty() {
        // Nothing about this group's fields had any activity this batch —
        // pure no-op suppression (see `FieldAccum`'s doc comment): the
        // group was touched (it's in `plan.groups` at all) but nothing this
        // module tracks actually changed, so there is nothing to write.
        return Ok(false);
    }

    let pk_values_sql: Vec<String> = (1..=plan.group_by.len())
        .map(|i| {
            format!(
                "${i}::text::{}",
                ddl::pg_type_name(plan.group_by_types[i - 1])
            )
        })
        .collect();

    let insert_values: Vec<String> = pk_values_sql.into_iter().chain(insert_exprs).collect();

    let sql = format!(
        "insert into {target_ident} ({}) values ({}) \
         on conflict ({}) do update set {} \
         returning 1",
        insert_cols.join(", "),
        insert_values.join(", "),
        pk_idents.join(", "),
        update_sets.join(", "),
    );

    let rows = txn.query(&sql, &params).await?;
    Ok(!rows.is_empty())
}

/// The `k`-alias column name for the `i`th `GROUP BY` column in a keyset
/// `unnest(...)` — see [`keyset_unnest`]. Named `c0`, `c1`, … so they never
/// collide with the source/target's own (arbitrarily-named) grouping columns
/// when both appear in one query's join condition.
fn keyset_col(i: usize) -> String {
    format!("c{i}")
}

/// `unnest($start::text[]::t0[], $start+1::text[]::t1[], …) [with ordinality]
/// as k(c0, c1, …[, ord])` — the bound touched-group keys as a derived
/// relation, one array parameter per `GROUP BY` column (so the whole keyset
/// is `group_by.len()` bind parameters regardless of how many groups it
/// carries, well under Postgres's bind cap — unlike #58's per-row literals).
/// `with_ordinality` adds a 1-based `ord` column, letting a caller map a
/// matched row back to which touched group produced it without re-encoding
/// its (typed) key columns back to the [`derive_group_key`] text form.
fn keyset_unnest(group_by_types: &[ValueType], start: usize, with_ordinality: bool) -> String {
    let arrays: Vec<String> = group_by_types
        .iter()
        .enumerate()
        .map(|(i, ty)| format!("${}::text[]::{}[]", start + i, ddl::pg_type_name(*ty)))
        .collect();
    let mut cols: Vec<String> = (0..group_by_types.len()).map(keyset_col).collect();
    if with_ordinality {
        cols.push("ord".to_string());
    }
    format!(
        "unnest({}){} as k({})",
        arrays.join(", "),
        if with_ordinality {
            " with ordinality"
        } else {
            ""
        },
        cols.join(", ")
    )
}

/// A `<alias>.<group col> <op> k.c<i>` conjunction, matching a row of `alias`
/// against the keyset relation. `null_safe[i]` selects the operator per column:
///
/// - `false` → plain `=`. Safe *and preferred* when no group in this batch
///   binds a NULL for column `i`: with a non-NULL right-hand side, `col = k`
///   and `col IS NOT DISTINCT FROM k` are identical (both reject NULL `col`),
///   but `=` is hashable/indexable so Postgres can pick a Hash Join instead of
///   the `IS NOT DISTINCT FROM` Nested Loop that rescans the whole keyset per
///   source row (the O(table_size²/…) blowup behind issues #59/#62).
/// - `true` → `is not distinct from`, required only for a column that actually
///   carries a NULL group key in this batch (a NULL key never matches under
///   `=`), for the same NULL-grouping reason [`group_where_clause`] uses it.
fn keyset_match(group_by: &[String], alias: &str, null_safe: &[bool]) -> String {
    group_by
        .iter()
        .enumerate()
        .map(|(i, col)| {
            let op = if null_safe[i] {
                "is not distinct from"
            } else {
                "="
            };
            format!("{alias}.{} {op} k.{}", quote_ident(col), keyset_col(i))
        })
        .collect::<Vec<_>>()
        .join(" and ")
}

/// [`keyset_match`]'s source-side counterpart (issue #137): matches
/// [`group_by_source_cols`] against the keyset relation instead of a plain
/// `<alias>.<column>` — needed wherever the keyset is joined to `source`
/// (optionally `LEFT JOIN`ed to `plan.rel_joins`) rather than to the target
/// table, since a relationship-path `GROUP BY` key's value there comes from
/// its own join alias, not `source`'s alias.
fn keyset_match_source(plan: &AggregateTargetPlan, alias: &str, null_safe: &[bool]) -> String {
    group_by_source_cols(plan, alias)
        .into_iter()
        .enumerate()
        .map(|(i, col_sql)| {
            let op = if null_safe[i] {
                "is not distinct from"
            } else {
                "="
            };
            format!("{col_sql} {op} k.{}", keyset_col(i))
        })
        .collect::<Vec<_>>()
        .join(" and ")
}

/// The per-`GROUP BY`-column value arrays for `groups`, transposed so column
/// `j`'s array is every group's `group_values[j]` — the shape each keyset
/// `unnest(...)` array parameter binds (see [`keyset_unnest`]).
fn transpose_group_values(arity: usize, groups: &[&GroupPlan]) -> Vec<Vec<Option<String>>> {
    (0..arity)
        .map(|j| groups.iter().map(|g| g.group_values[j].clone()).collect())
        .collect()
}

/// The full-recompute path for every [`GroupPlan::force_full_recompute`]
/// group in one batch (issue #59), replacing the per-group probe + upsert
/// sequence [`upsert_group`]'s forced branches would otherwise run once each
/// — the aggregate analog of [`super::apply::apply_target`]'s bulk chunked
/// write. Instead of `O(forced groups)` round trips (an existence probe plus
/// one probe per field, per group — each a separate source scan), this is a
/// fixed handful of statements regardless of group count:
///
/// 1. One `SELECT` over the source, joined to the bound keyset, returning the
///    ordinals of forced groups that still have at least one source row (the
///    survivors) — the bulk replacement for the per-group [`probe_group_exists`].
/// 2. One `INSERT … SELECT <group cols>, <per-field aggregate exprs> FROM
///    source JOIN keyset GROUP BY <group cols> ON CONFLICT DO UPDATE` that
///    recomputes every survivor group's visible columns and hidden
///    `SUM`/`AVG` partials in a single grouped pass (the join restricts the
///    scan to touched groups, so extinct groups simply produce no row and are
///    never inserted). Each field's SELECT expression is built exactly as its
///    per-group probe would compute it (`sum(arg)`/`count(arg)` for
///    `SUM`/`AVG`, `count(*)` for `COUNT`, [`oracle::render_expr_sql`] for a
///    `RecomputeOnly` field) — Postgres's `sum()` being NULL over zero
///    non-null values preserves the same "NULL, not 0" rule the per-group
///    path guards, with no extra `case` needed for the visible sum column.
/// 3. One `DELETE … USING keyset` for the extinct groups (touched but with no
///    surviving source row), the bulk replacement for the per-group
///    [`delete_group_row`].
///
/// Callers must have already taken this batch's ascending-ordered pre-lock
/// (see [`apply_aggregate_target`]) — this function's own statements lock
/// rows in planner-chosen order, so the pre-lock is what preserves the
/// ascending-lock-order deadlock-avoidance invariant.
async fn apply_forced_groups_bulk(
    txn: &Transaction<'_>,
    target: &str,
    plan: &AggregateTargetPlan,
    forced: &[(&String, &GroupPlan)],
) -> Result<(Vec<TouchedGroup>, Vec<TouchedGroup>), ApplyError> {
    let arity = plan.group_by.len();
    let forced_groups: Vec<&GroupPlan> = forced.iter().map(|(_, g)| *g).collect();
    let arrays = transpose_group_values(arity, &forced_groups);
    // Per-column: does any touched group bind a NULL key here? Only then does
    // this column need the null-safe (non-hashable) match operator; otherwise
    // plain `=` is equivalent and lets Postgres hash-join the keyset to the
    // source instead of nested-looping it (see [`keyset_match`], #59/#62).
    let null_safe: Vec<bool> = arrays
        .iter()
        .map(|a| a.iter().any(|v| v.is_none()))
        .collect();
    let source_ident = ddl::qualified_source_table(&plan.source);
    // `target` is always [`AggregateTargetPlan::target`]'s qualified
    // identity by the time this is called (reviewer follow-up to issue #74).
    let target_ident = ddl::qualified_target_table_ident(target);
    let group_idents: Vec<String> = plan.group_by.iter().map(|c| quote_ident(c)).collect();
    // Issue #94: to-one relationship joins onto the recompute's source scan,
    // so a `SUM(post.word_count)` field reads the joined to-side column.
    // Issue #137: a relationship-path `GROUP BY` key needs this same join for
    // its *survivor probe* too (unlike a plain-column key, whose existence
    // never depended on one) — `keyset_match_source` below resolves such a
    // key's value through its own join alias, which only exists once the
    // join is present. The extinct-group DELETE still needs none (it matches
    // the *target*'s own real columns, never `source`). Empty for a
    // relationship-free aggregate, leaving its SQL byte-identical.
    let rel_joins_sql = rel_joins_sql(plan);

    // 1. Survivor ordinals: which forced groups still have a source row
    // whose (possibly relationship-resolved) group key matches.
    let survivor_sql = format!(
        "select distinct k.ord::bigint from {} join {source_ident} s on {}{rel_joins_sql}",
        keyset_unnest(&plan.group_by_types, 1, true),
        keyset_match_source(plan, "s", &null_safe),
    );
    let survivor_params: Vec<&(dyn ToSql + Sync)> =
        arrays.iter().map(|a| a as &(dyn ToSql + Sync)).collect();
    let survivor_rows = txn.query(&survivor_sql, &survivor_params).await?;
    let survivor_ords: std::collections::HashSet<i64> =
        survivor_rows.iter().map(|r| r.get::<_, i64>(0)).collect();

    let mut written = Vec::new();
    let mut extinct_ords: Vec<i64> = Vec::new();
    for (i, (key, group)) in forced.iter().enumerate() {
        let ord = (i + 1) as i64;
        if survivor_ords.contains(&ord) {
            written.push(((*key).clone(), group.hop_gen, group.src_changed));
        } else {
            extinct_ords.push(ord);
        }
    }

    // 2. Bulk recompute of every survivor group (the join to the keyset means
    // extinct groups produce no SELECT row, so this touches only survivors).
    if !survivor_ords.is_empty() {
        let mut insert_cols: Vec<String> = group_idents.clone();
        // Issue #137: a relationship-path `GROUP BY` key projects its
        // joined value, not a plain `s.<column>` — `group_by_source_cols`
        // renders both shapes uniformly.
        let mut select_exprs: Vec<String> = group_by_source_cols(plan, "s");
        // Issue #48: same dedup as `upsert_group` — two fields sharing a
        // hidden count column (`plan.count_column_names`) must only
        // contribute that column/select-expression pair once, or this bulk
        // `INSERT ... SELECT` ends up with the same column named twice.
        let mut emitted_count_cols: std::collections::HashSet<String> =
            std::collections::HashSet::new();

        for field in &plan.fields {
            let col = quote_ident(&field.name);
            match field.kind {
                AggFieldKind::Sum => {
                    let arg = agg_arg_sql(plan, &field.name);
                    insert_cols.push(col.clone());
                    select_exprs.push(format!("sum({arg})"));
                    let count_col_name = plan.count_column_names[&field.name].clone();
                    if emitted_count_cols.insert(count_col_name.clone()) {
                        insert_cols.push(quote_ident(&count_col_name));
                        select_exprs.push(format!("count({arg})"));
                    }
                }
                AggFieldKind::Avg => {
                    let arg = agg_arg_sql(plan, &field.name);
                    let sum_col_name = avg_sum_column(&field.name);
                    let sum_col = quote_ident(&sum_col_name);
                    insert_cols.push(sum_col);
                    select_exprs.push(format!("sum({arg})"));
                    let count_col_name = plan.count_column_names[&field.name].clone();
                    if emitted_count_cols.insert(count_col_name.clone()) {
                        insert_cols.push(quote_ident(&count_col_name));
                        select_exprs.push(format!("count({arg})"));
                    }
                    insert_cols.push(col.clone());
                    select_exprs.push(format!(
                        "case when count({arg}) = 0 then null \
                         else sum({arg}) / count({arg})::numeric end"
                    ));
                }
                AggFieldKind::Count => {
                    // Issue #120: `COUNT(<expr>)` renders `count(<expr>)`;
                    // `COUNT(*)` still renders bare `count(*)`. No cast
                    // needed either way — Postgres's `count()` already
                    // returns `bigint`, matching this field's declared
                    // `Integer(Int8)` type.
                    insert_cols.push(col.clone());
                    let expr = &plan.field_exprs[field.name.as_str()];
                    let Expr::FunctionCall { args, .. } = expr else {
                        panic!("AggFieldKind::Count field's own expression must be a FunctionCall");
                    };
                    let count_sql = match args.first() {
                        Some(arg) => format!("count({})", render_agg_expr(plan, arg)),
                        None => "count(*)".to_string(),
                    };
                    select_exprs.push(count_sql);
                }
                AggFieldKind::RecomputeOnly => {
                    let expr = render_agg_expr(plan, &plan.field_exprs[field.name.as_str()]);
                    insert_cols.push(col.clone());
                    select_exprs.push(format!("({expr})"));
                }
            }
        }

        let update_sets: Vec<String> = insert_cols
            .iter()
            .skip(arity)
            .map(|c| format!("{c} = excluded.{c}"))
            .collect();
        // Every field this grammar can put on an aggregate target contributes
        // at least one column, so a definition always has at least one
        // non-`GROUP BY` field to update; an empty `update_sets` would mean a
        // group-by-only "aggregate" the grammar can't express.
        debug_assert!(!update_sets.is_empty());

        // The `GROUP BY` mirrors the leading `arity` SELECT expressions
        // (`select_exprs`'s `s.<group col>` prefix), so reuse them rather
        // than rebuilding the identical list.
        let insert_sql = format!(
            "insert into {target_ident} ({}) \
             select {} from {} join {source_ident} s on {}{rel_joins_sql} \
             group by {} \
             on conflict ({}) do update set {}",
            insert_cols.join(", "),
            select_exprs.join(", "),
            keyset_unnest(&plan.group_by_types, 1, false),
            keyset_match_source(plan, "s", &null_safe),
            select_exprs[..arity].join(", "),
            group_idents.join(", "),
            update_sets.join(", "),
        );
        txn.query(&insert_sql, &survivor_params).await?;
    }

    // 3. Extinct groups: touched, but no surviving source row — delete their
    // target rows in one statement, `ord` telling us which we removed. (Their
    // prior images, which a downstream aggregate needs to find the group they
    // left, came from `apply_aggregate_target`'s pre-lock — issue #315.)
    let mut deleted = Vec::new();
    if !extinct_ords.is_empty() {
        let ord_param = arity + 1;
        let delete_sql = format!(
            "delete from {target_ident} t using {} \
             where {} and k.ord = any(${ord_param}::bigint[]) \
             returning k.ord::bigint",
            keyset_unnest(&plan.group_by_types, 1, true),
            keyset_match(&plan.group_by, "t", &null_safe),
        );
        let mut delete_params: Vec<&(dyn ToSql + Sync)> =
            arrays.iter().map(|a| a as &(dyn ToSql + Sync)).collect();
        delete_params.push(&extinct_ords);
        let rows = txn.query(&delete_sql, &delete_params).await?;
        let deleted_ords: HashSet<i64> = rows.iter().map(|r| r.get::<_, i64>(0)).collect();
        for (i, (key, group)) in forced.iter().enumerate() {
            if deleted_ords.contains(&((i + 1) as i64)) {
                deleted.push(((*key).clone(), group.hop_gen, group.src_changed));
            }
        }
    }

    Ok((written, deleted))
}

/// The rendered SQL for a `SUM`/`AVG` field's single argument expression —
/// [`render_agg_expr`] over the call's one argument, exactly as
/// [`probe_sum_and_count`] renders it, so the bulk path computes the same
/// `sum(arg)`/`count(arg)` the per-group probe would.
fn agg_arg_sql(plan: &AggregateTargetPlan, field_name: &str) -> String {
    let Expr::FunctionCall { args, .. } = &plan.field_exprs[field_name] else {
        panic!("agg_arg_sql called on a non-SUM/AVG field");
    };
    render_agg_expr(plan, &args[0])
}

/// One aggregate field's (or field argument's) expression as SQL over this
/// plan's recompute scan, where the source is aliased `s` (every caller with
/// a non-empty `rel_joins` joins the source under that alias — see
/// [`oracle::to_one_join_clauses`]'s own call sites in this file).
///
/// A relationship-free plan renders through [`oracle::render_expr_sql`]
/// unchanged — unqualified column names, exactly as before #94, which matters
/// because callers with no relationship join ([`probe_field_value`],
/// [`probe_sum_and_count`], [`probe_recompute_fields_bulk`]) render the same
/// expressions against an *unaliased* source. A plan with to-one relationship
/// joins renders through [`oracle::render_to_one_rel_expr_sql`] instead,
/// qualifying source columns with `s` (they'd otherwise be ambiguous against
/// the joined to-side table) and resolving each `<rel>.<column>` path off its
/// join alias.
///
/// Every caller that can be reached for a plan with relationship joins must
/// pair this with [`oracle::to_one_join_clauses`] over `plan.rel_joins`
/// (aliased `s`, matching this function's own aliasing), or the rendered
/// `<rel>.<column>` reference resolves to nothing. Before issue #136,
/// `force_every_group` guaranteed every group of such a plan reached only
/// [`apply_forced_groups_bulk`] (which already did this), making the ordinary
/// delta-path probes ([`probe_field_value`], [`probe_recompute_fields_bulk`])
/// unreachable for a relationship-reading `RecomputeOnly` field; #136 routes
/// such groups through the ordinary delta path instead, so both of those now
/// need — and carry — the same join.
fn render_agg_expr(plan: &AggregateTargetPlan, expr: &Expr) -> String {
    if plan.rel_joins.is_empty() {
        oracle::render_expr_sql(expr)
    } else {
        oracle::render_to_one_rel_expr_sql(expr, "s")
    }
}

// ---------------------------------------------------------------------
// Batched ordinary-delta upsert (issue #63 M4)
// ---------------------------------------------------------------------

/// Whether [`upsert_group`]'s write for `group` would touch any column at
/// all — mirrors that function's own `update_sets.is_empty()` no-op check
/// (see its doc comment), computed up front so [`apply_aggregate_target`]
/// can exclude a genuinely inactive group before it ever reaches a write
/// path, batched or not.
fn group_has_activity(plan: &AggregateTargetPlan, group: &GroupPlan) -> bool {
    plan.fields
        .iter()
        .any(|f| f.kind == AggFieldKind::RecomputeOnly || group.field_accum.contains_key(&f.name))
}

/// Encodes `values` (already-rendered `Numeric` text — see [`FieldAccum`])
/// as a Postgres array-literal string (`{"1.00","-2.50"}`), so one group's
/// whole variable-length adds/subs list can travel as a single `text[]`
/// *element* alongside every other touched group's own list in one bind
/// parameter — the ragged-2-D-array problem [`apply_delta_groups_bulk`]'s
/// doc comment describes. Every element is double-quoted and
/// backslash-escaped defensively; `Numeric`'s text form never actually
/// contains `{`, `}`, `,`, or whitespace (see `numeric.rs`), but quoting
/// costs nothing and removes any need to keep this code in sync with that
/// assumption.
fn array_literal(values: &[String]) -> String {
    let mut out = String::from("{");
    for (i, v) in values.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push('"');
        for ch in v.chars() {
            if ch == '"' || ch == '\\' {
                out.push('\\');
            }
            out.push(ch);
        }
        out.push('"');
    }
    out.push('}');
    out
}

/// [`sum_array_expr`]'s sibling for a per-row array-literal *column*
/// (produced by [`array_literal`], one per touched group) rather than a
/// single top-level bind parameter: `(select coalesce(sum(v), 0) from
/// unnest(<col_ref>::text[]::numeric[]) v)`.
fn array_literal_sum_expr(col_ref: &str) -> String {
    format!("(select coalesce(sum(v), 0) from unnest({col_ref}::text[]::numeric[]) v)")
}

/// One extra `unnest(...)` array parameter for [`delta_carrier_unnest`],
/// type-erased so a single `Vec` can carry the different concrete
/// carrier shapes [`build_delta_carriers`] produces (one `bool[]`/
/// `bigint[]`/`text[]` per field, per touched group) as one homogeneous
/// collection of bind parameters.
enum CarrierArray {
    Text(Vec<String>),
    NullableText(Vec<Option<String>>),
    Bool(Vec<bool>),
    BigInt(Vec<i64>),
}

impl CarrierArray {
    fn as_param(&self) -> &(dyn ToSql + Sync) {
        match self {
            CarrierArray::Text(v) => v,
            CarrierArray::NullableText(v) => v,
            CarrierArray::Bool(v) => v,
            CarrierArray::BigInt(v) => v,
        }
    }
}

/// Like [`keyset_unnest`], but the derived relation also carries this
/// bucket's per-group field data (`extra`: `(column name, pg element type)`
/// pairs, one `unnest(...)` array per pair) alongside the `GROUP BY`
/// columns — the shape [`apply_delta_groups_bulk`]'s statements read every
/// per-group value they need from, keyed by the same 1-based `ord`
/// [`keyset_unnest`] already produces. Always `with ordinality`, and always
/// aliased `k` (like [`keyset_unnest`]) so [`keyset_match`] needs no variant
/// of its own.
fn delta_carrier_unnest(group_by_types: &[ValueType], extra: &[(&str, &str)]) -> String {
    let mut arrays: Vec<String> = group_by_types
        .iter()
        .enumerate()
        .map(|(i, ty)| format!("${}::text[]::{}[]", i + 1, ddl::pg_type_name(*ty)))
        .collect();
    let base = group_by_types.len();
    for (i, (_, pg_type)) in extra.iter().enumerate() {
        arrays.push(format!("${}::{}[]", base + i + 1, pg_type));
    }
    let mut cols: Vec<String> = (0..group_by_types.len()).map(keyset_col).collect();
    cols.extend(extra.iter().map(|(name, _)| name.to_string()));
    cols.push("ord".to_string());
    format!(
        "unnest({}) with ordinality as k({})",
        arrays.join(", "),
        cols.join(", ")
    )
}

/// Bulk counterpart to [`probe_field_value`] for every
/// [`AggFieldKind::RecomputeOnly`] field on `plan`, across every group in
/// this bucket at once — one join-and-group-by over the source instead of
/// one probe per group, reusing [`keyset_unnest`]/[`keyset_match`] exactly
/// as [`apply_forced_groups_bulk`] does for its own bulk recompute. Callers
/// already know (via [`probe_group_exists`], run per group before this
/// bucket is ever assembled) that every group here has at least one
/// surviving source row, so the inner join can never silently drop one.
///
/// Returns one `Vec<Option<String>>` per `RecomputeOnly` field, in
/// [`AggregateTargetPlan::fields`] order, each aligned with `groups`
/// (index `i` is that field's value for `groups[i]`) — an empty outer `Vec`
/// if `plan` has no `RecomputeOnly` field at all, skipping the query
/// entirely.
async fn probe_recompute_fields_bulk(
    txn: &Transaction<'_>,
    plan: &AggregateTargetPlan,
    key_arrays: &[Vec<Option<String>>],
    null_safe: &[bool],
    group_count: usize,
) -> Result<Vec<Vec<Option<String>>>, ApplyError> {
    let recompute_fields: Vec<&AggFieldPlan> = plan
        .fields
        .iter()
        .filter(|f| f.kind == AggFieldKind::RecomputeOnly)
        .collect();
    if recompute_fields.is_empty() {
        return Ok(Vec::new());
    }

    let source_ident = ddl::qualified_source_table(&plan.source);
    let select_exprs: Vec<String> = recompute_fields
        .iter()
        .map(|f| {
            format!(
                "({})::text",
                render_agg_expr(plan, &plan.field_exprs[f.name.as_str()])
            )
        })
        .collect();
    // Issue #94 (same as `apply_forced_groups_bulk`): a `RecomputeOnly` field
    // reading a to-one relationship path (e.g. `MIN(rel.col)`/`MAX(rel.col)`)
    // needs its relationship's to-side table joined onto this scan, or
    // `render_agg_expr` above renders an unresolved `rel.col` path that panics
    // in the relationship-free renderer it would otherwise fall through to.
    // Issue #137: the keyset match itself also needs to be relationship-aware
    // whenever a `GROUP BY` key (not just a field) reads one — see
    // `keyset_match_source`. Empty/plain for a relationship-free plan,
    // leaving this SQL byte-identical.
    let sql = format!(
        "select k.ord::bigint, {} from {} join {source_ident} s on {}{} group by k.ord",
        select_exprs.join(", "),
        keyset_unnest(&plan.group_by_types, 1, true),
        keyset_match_source(plan, "s", null_safe),
        rel_joins_sql(plan),
    );
    let params: Vec<&(dyn ToSql + Sync)> = key_arrays
        .iter()
        .map(|a| a as &(dyn ToSql + Sync))
        .collect();
    let rows = txn.query(&sql, &params).await?;

    let mut result: Vec<Vec<Option<String>>> =
        vec![vec![None; group_count]; recompute_fields.len()];
    for row in &rows {
        let ord: i64 = row.get(0);
        let i = (ord - 1) as usize;
        for (f_idx, field_row) in result.iter_mut().enumerate() {
            field_row[i] = row.get(f_idx + 1);
        }
    }
    Ok(result)
}

/// Builds every field's carrier arrays and SQL fragments for
/// [`apply_delta_groups_bulk`] — the batched counterpart to
/// [`upsert_group`]'s Pass 1 + Pass 2, computed once and shared by that
/// function's `UPDATE`/`INSERT` statements alike. Each field's fragments
/// are [`upsert_group`]'s own expressions, verbatim, just reading a
/// carrier column (`k.f{idx}_...`) instead of a per-group bind parameter,
/// and — for [`AggFieldKind::Sum`]/[`AggFieldKind::Avg`]/[`AggFieldKind::Count`]
/// — gated by `k.f{idx}_active` (`case when ... then ... else <untouched>
/// end`) wherever [`upsert_group`] would have omitted that field's columns
/// entirely for a group with no [`FieldAccum`] entry. `RecomputeOnly`
/// fields need no `active` gate — [`upsert_group`] always probes and writes
/// them unconditionally — and read straight from `recompute_values` (see
/// [`probe_recompute_fields_bulk`]) rather than carrying their own
/// probe-triggering data.
///
/// Returns `(carriers, insert_cols, insert_exprs, update_sets)`: `carriers`
/// is every extra `unnest(...)` array [`delta_carrier_unnest`] needs beyond
/// the `GROUP BY` columns; `insert_cols`/`insert_exprs` are this bucket's
/// non-`GROUP BY` `INSERT` columns and their `SELECT` expressions (a fresh
/// row's own carrier data only — no target row exists yet to reference);
/// `update_sets` are the `UPDATE ... FROM` `SET` assignments (free to
/// reference both the target row and the carrier columns at once — see
/// [`apply_delta_groups_bulk`]'s doc comment on why only this half of the
/// write can do that).
/// `(carriers, insert_cols, insert_exprs, update_sets)` — see
/// [`build_delta_carriers`]'s doc comment for what each element holds.
type DeltaCarrierPlan = (
    Vec<(String, String, CarrierArray)>,
    Vec<String>,
    Vec<String>,
    Vec<String>,
);

fn build_delta_carriers(
    plan: &AggregateTargetPlan,
    groups: &[(&String, &GroupPlan)],
    recompute_values: &[Vec<Option<String>>],
    target_ident: &str,
) -> DeltaCarrierPlan {
    let mut carriers: Vec<(String, String, CarrierArray)> = Vec::new();
    let mut insert_cols: Vec<String> = Vec::new();
    let mut insert_exprs: Vec<String> = Vec::new();
    let mut update_sets: Vec<String> = Vec::new();
    let mut recompute_idx = 0;
    // Issue #48: two fields can share one hidden count column
    // (`plan.count_column_names`) — track which shared names this statement
    // has already emitted a column/SET clause for, mirroring
    // `apply_forced_groups_bulk`'s `emitted_count_cols` guard, so a second
    // field sharing a column never emits a duplicate insert column or `SET
    // count_col = ..., count_col = ...` (which Postgres rejects outright).
    let mut emitted_count_cols: std::collections::HashSet<String> =
        std::collections::HashSet::new();

    for (idx, field) in plan.fields.iter().enumerate() {
        let col = quote_ident(&field.name);
        match field.kind {
            AggFieldKind::Sum | AggFieldKind::Avg => {
                let active_name = format!("f{idx}_active");
                let adds_name = format!("f{idx}_adds");
                let subs_name = format!("f{idx}_subs");
                let count_delta_name = format!("f{idx}_count_delta");

                let active: Vec<bool> = groups
                    .iter()
                    .map(|(_, g)| g.field_accum.contains_key(&field.name))
                    .collect();
                let adds: Vec<String> = groups
                    .iter()
                    .map(|(_, g)| {
                        array_literal(
                            g.field_accum
                                .get(&field.name)
                                .map(|a| a.adds.as_slice())
                                .unwrap_or(&[]),
                        )
                    })
                    .collect();
                let subs: Vec<String> = groups
                    .iter()
                    .map(|(_, g)| {
                        array_literal(
                            g.field_accum
                                .get(&field.name)
                                .map(|a| a.subs.as_slice())
                                .unwrap_or(&[]),
                        )
                    })
                    .collect();
                let count_delta: Vec<i64> = groups
                    .iter()
                    .map(|(_, g)| {
                        g.field_accum
                            .get(&field.name)
                            .map(|a| a.adds.len() as i64 - a.subs.len() as i64)
                            .unwrap_or(0)
                    })
                    .collect();

                carriers.push((
                    active_name.clone(),
                    "bool".to_string(),
                    CarrierArray::Bool(active),
                ));
                carriers.push((
                    adds_name.clone(),
                    "text".to_string(),
                    CarrierArray::Text(adds),
                ));
                carriers.push((
                    subs_name.clone(),
                    "text".to_string(),
                    CarrierArray::Text(subs),
                ));
                carriers.push((
                    count_delta_name.clone(),
                    "bigint".to_string(),
                    CarrierArray::BigInt(count_delta),
                ));

                let active = format!("k.{active_name}");
                let sum_delta = format!(
                    "({} - {})",
                    array_literal_sum_expr(&format!("k.{adds_name}")),
                    array_literal_sum_expr(&format!("k.{subs_name}")),
                );
                let count_delta_ref = format!("k.{count_delta_name}");

                if field.kind == AggFieldKind::Sum {
                    let count_col_name = plan.count_column_names[&field.name].clone();
                    let count_col = quote_ident(&count_col_name);
                    let insert_count = count_delta_ref.clone();
                    let insert_sum =
                        format!("case when {insert_count} = 0 then null else {sum_delta} end");
                    insert_cols.push(col.clone());
                    insert_exprs.push(format!(
                        "case when {active} then {insert_sum} else null end"
                    ));

                    let update_count =
                        format!("coalesce({target_ident}.{count_col}, 0) + {count_delta_ref}");
                    let update_sum_raw = format!("coalesce({target_ident}.{col}, 0) + {sum_delta}");
                    let update_sum = format!(
                        "case when ({update_count}) = 0 then null else ({update_sum_raw}) end"
                    );
                    update_sets.push(format!(
                        "{col} = case when {active} then {update_sum} else {target_ident}.{col} end"
                    ));
                    if emitted_count_cols.insert(count_col_name) {
                        insert_cols.push(count_col.clone());
                        insert_exprs.push(format!(
                            "case when {active} then {insert_count} else null end"
                        ));
                        update_sets.push(format!(
                            "{count_col} = case when {active} then {update_count} else {target_ident}.{count_col} end"
                        ));
                    }
                } else {
                    let sum_col_name = avg_sum_column(&field.name);
                    let count_col_name = plan.count_column_names[&field.name].clone();
                    let sum_col = quote_ident(&sum_col_name);
                    let count_col = quote_ident(&count_col_name);
                    let avg_col = col.clone();

                    let insert_sum = sum_delta.clone();
                    let insert_count = count_delta_ref.clone();
                    let insert_avg = format!(
                        "case when {insert_count} = 0 then null \
                         else {insert_sum} / ({insert_count})::numeric end"
                    );
                    insert_cols.push(sum_col.clone());
                    insert_exprs.push(format!(
                        "case when {active} then {insert_sum} else null end"
                    ));
                    insert_cols.push(avg_col.clone());
                    insert_exprs.push(format!(
                        "case when {active} then {insert_avg} else null end"
                    ));

                    let update_sum = format!("coalesce({target_ident}.{sum_col}, 0) + {sum_delta}");
                    let update_count =
                        format!("coalesce({target_ident}.{count_col}, 0) + {count_delta_ref}");
                    let update_avg = format!(
                        "case when ({update_count}) = 0 then null \
                         else ({update_sum}) / ({update_count})::numeric end"
                    );
                    update_sets.push(format!(
                        "{sum_col} = case when {active} then {update_sum} else {target_ident}.{sum_col} end"
                    ));
                    update_sets.push(format!(
                        "{avg_col} = case when {active} then {update_avg} else {target_ident}.{avg_col} end"
                    ));
                    if emitted_count_cols.insert(count_col_name) {
                        insert_cols.push(count_col.clone());
                        insert_exprs.push(format!(
                            "case when {active} then {insert_count} else null end"
                        ));
                        update_sets.push(format!(
                            "{count_col} = case when {active} then {update_count} else {target_ident}.{count_col} end"
                        ));
                    }
                }
            }
            AggFieldKind::Count => {
                let active_name = format!("f{idx}_active");
                let count_delta_name = format!("f{idx}_count_delta");
                let active: Vec<bool> = groups
                    .iter()
                    .map(|(_, g)| g.field_accum.contains_key(&field.name))
                    .collect();
                let count_delta: Vec<i64> = groups
                    .iter()
                    .map(|(_, g)| {
                        g.field_accum
                            .get(&field.name)
                            .map(|a| a.adds.len() as i64 - a.subs.len() as i64)
                            .unwrap_or(0)
                    })
                    .collect();
                carriers.push((
                    active_name.clone(),
                    "bool".to_string(),
                    CarrierArray::Bool(active),
                ));
                carriers.push((
                    count_delta_name.clone(),
                    "bigint".to_string(),
                    CarrierArray::BigInt(count_delta),
                ));

                let active = format!("k.{active_name}");
                let count_delta_ref = format!("k.{count_delta_name}");
                // Issue #120: no `::numeric` cast needed — `count_delta_ref`
                // is already bound `bigint` (the carrier array's own
                // declared type above), matching this field's declared
                // `Integer(Int8)` column exactly (Postgres's own `count()`
                // is `bigint`, never `numeric`).
                insert_cols.push(col.clone());
                insert_exprs.push(format!(
                    "case when {active} then {count_delta_ref} else null end"
                ));
                update_sets.push(format!(
                    "{col} = case when {active} then coalesce({target_ident}.{col}, 0) + {count_delta_ref} else {target_ident}.{col} end"
                ));
            }
            AggFieldKind::RecomputeOnly => {
                let r_name = format!("f{idx}_r");
                let values: Vec<Option<String>> = (0..groups.len())
                    .map(|i| recompute_values[recompute_idx][i].clone())
                    .collect();
                recompute_idx += 1;
                carriers.push((
                    r_name.clone(),
                    "text".to_string(),
                    CarrierArray::NullableText(values),
                ));
                let pg_type = ddl::pg_type_name(field.value_type);
                let expr = format!("k.{r_name}::text::{pg_type}");
                insert_cols.push(col.clone());
                insert_exprs.push(expr.clone());
                update_sets.push(format!("{col} = {expr}"));
            }
        }
    }

    (carriers, insert_cols, insert_exprs, update_sets)
}

/// Non-forced, source-existing groups' batched counterpart to per-group
/// [`upsert_group`] (issue #63 M4, absorbing #60): the same
/// increment-or-recompute logic [`upsert_group`]'s Pass 2 builds per group
/// (see [`build_delta_carriers`]), expressed once as one `UPDATE ... FROM
/// unnest(...)` plus (only if some group in this bucket has no target row
/// yet) one `INSERT ... SELECT ... FROM unnest(...) ON CONFLICT DO NOTHING`,
/// instead of one `INSERT ... ON CONFLICT DO UPDATE` per group — a
/// round-trip reduction only, never a different computed value. Callers
/// must exclude a group with no activity at all first (see
/// [`group_has_activity`]) and must only reach here for more than one such
/// group at once — [`apply_aggregate_target`] still calls [`upsert_group`]
/// directly for a lone group, since this function's worst case (three round
/// trips) only pays for itself once a batch touches several groups together.
///
/// Two statements (occasionally three) instead of one exist because
/// Postgres's `ON CONFLICT DO UPDATE SET` — and a plain `INSERT ...
/// RETURNING` — can only ever see the target table and the special
/// `excluded` row, never the `unnest(...)` relation an `INSERT ... SELECT`
/// reads from (confirmed empirically: referencing it raises "missing
/// FROM-clause entry"), so a single upsert statement has no way to combine
/// "this row's own delta" with "the target's current value" the way
/// [`upsert_group`]'s literal per-row bind parameters let it do. `UPDATE ...
/// FROM`, by contrast, *can* reference both the target row and the
/// `FROM`-list relation in the same `SET`/`RETURNING` (confirmed
/// empirically too), so the first statement below handles every group that
/// already has a target row.
///
/// Whatever that `UPDATE` doesn't match must be a brand-new group: this
/// function only ever runs after [`apply_aggregate_target`]'s ascending
/// pre-lock has already taken every *existing* touched row for the whole
/// batch, so nothing can race the `UPDATE` — the only race left is a
/// concurrent writer inserting one of the same brand-new rows. The second
/// statement is therefore a plain per-row `INSERT` (no arithmetic against
/// existing state needed — there is none yet), guarded by `ON CONFLICT DO
/// NOTHING`. A straggler that loses that race — unmatched by the `UPDATE`,
/// not actually inserted by this `INSERT` — necessarily has a target row by
/// now (the only way its insert could have found a conflict), so one final
/// `UPDATE ... FROM`, identical to the first but restricted to just the
/// stragglers, always finishes them off in one more round.
///
/// Knowing *which* pending groups the `INSERT` actually inserted (as
/// opposed to skipped via `ON CONFLICT DO NOTHING`) needs its own
/// correlation, for the same reason the `UPDATE`/`INSERT` split exists at
/// all: a plain `INSERT ... RETURNING` cannot see the `unnest(...)` it read
/// from, only real target columns. Wrapping it in a CTE that `RETURNING`s
/// the target's own primary-key columns, then joining that back to a fresh
/// read of the same keyset by native-typed primary-key equality, recovers
/// the touched ordinals without that restriction ever coming into play.
async fn apply_delta_groups_bulk(
    txn: &Transaction<'_>,
    target: &str,
    plan: &AggregateTargetPlan,
    groups: &[(&String, &GroupPlan)],
) -> Result<(), ApplyError> {
    let arity = plan.group_by.len();
    // `target` is always [`AggregateTargetPlan::target`]'s qualified
    // identity by the time this is called (reviewer follow-up to issue #74).
    let target_ident = ddl::qualified_target_table_ident(target);
    let pk_idents: Vec<String> = plan.group_by.iter().map(|c| quote_ident(c)).collect();

    let group_plans: Vec<&GroupPlan> = groups.iter().map(|(_, g)| *g).collect();
    let key_arrays = transpose_group_values(arity, &group_plans);
    let null_safe: Vec<bool> = key_arrays
        .iter()
        .map(|a| a.iter().any(|v| v.is_none()))
        .collect();

    let recompute_values =
        probe_recompute_fields_bulk(txn, plan, &key_arrays, &null_safe, groups.len()).await?;

    let (carriers, insert_cols_fields, insert_exprs_fields, update_sets) =
        build_delta_carriers(plan, groups, &recompute_values, &target_ident);

    let extra: Vec<(&str, &str)> = carriers
        .iter()
        .map(|(name, ty, _)| (name.as_str(), ty.as_str()))
        .collect();
    let unnest_sql = delta_carrier_unnest(&plan.group_by_types, &extra);

    let mut base_params: Vec<&(dyn ToSql + Sync)> = key_arrays
        .iter()
        .map(|a| a as &(dyn ToSql + Sync))
        .collect();
    base_params.extend(carriers.iter().map(|(_, _, data)| data.as_param()));

    // Round 1: every group that already has a target row.
    let update_sql = format!(
        "update {target_ident} set {} from {unnest_sql} where {}",
        update_sets.join(", "),
        keyset_match(&plan.group_by, &target_ident, &null_safe),
    );
    let update_returning_sql = format!("{update_sql} returning k.ord::bigint");
    let updated_rows = txn.query(&update_returning_sql, &base_params).await?;
    let updated_ords: HashSet<i64> = updated_rows.iter().map(|r| r.get::<_, i64>(0)).collect();

    let n = groups.len() as i64;
    let pending_ords: Vec<i64> = (1..=n).filter(|o| !updated_ords.contains(o)).collect();
    if pending_ords.is_empty() {
        return Ok(());
    }

    // Round 2: brand-new groups, correlated back to `ord` via the
    // CTE-plus-join workaround this function's doc comment describes.
    let mut insert_cols = pk_idents.clone();
    insert_cols.extend(insert_cols_fields);
    let select_exprs: Vec<String> = (0..arity)
        .map(|i| format!("k.{}", keyset_col(i)))
        .chain(insert_exprs_fields)
        .collect();
    let pending_param_idx = base_params.len() + 1;
    let insert_sql = format!(
        "with ins as (\
            insert into {target_ident} ({}) \
            select {} from {unnest_sql} where k.ord = any(${pending_param_idx}::bigint[]) \
            on conflict ({}) do nothing \
            returning {} \
         ) \
         select k.ord::bigint from {unnest_sql} join ins on {} \
         where k.ord = any(${pending_param_idx}::bigint[])",
        insert_cols.join(", "),
        select_exprs.join(", "),
        pk_idents.join(", "),
        pk_idents.join(", "),
        keyset_match(&plan.group_by, "ins", &null_safe),
    );
    let mut insert_params = base_params.clone();
    insert_params.push(&pending_ords);
    let inserted_rows = txn.query(&insert_sql, &insert_params).await?;
    let inserted_ords: HashSet<i64> = inserted_rows.iter().map(|r| r.get::<_, i64>(0)).collect();

    let stragglers: Vec<i64> = pending_ords
        .iter()
        .copied()
        .filter(|o| !inserted_ords.contains(o))
        .collect();
    if stragglers.is_empty() {
        return Ok(());
    }

    // Round 3: a concurrent writer's brand-new row this batch also touched
    // — see the doc comment on why this is provably the last round needed.
    let straggler_param_idx = base_params.len() + 1;
    let fallback_sql = format!(
        "{update_sql} and k.ord = any(${straggler_param_idx}::bigint[]) returning k.ord::bigint"
    );
    let mut fallback_params = base_params.clone();
    fallback_params.push(&stragglers);
    let rows = txn.query(&fallback_sql, &fallback_params).await?;
    debug_assert_eq!(
        rows.len(),
        stragglers.len(),
        "a straggler group must have a target row by round 3 — the only way \
         its round-2 insert could have found a conflict"
    );

    Ok(())
}

/// Phase 3 for one aggregate target table. Every group this batch touched is
/// written or deleted under a single ascending-ordered pre-lock taken up
/// front (see below), then split by strategy:
///
/// - [`GroupPlan::force_full_recompute`] groups (image-less changes — every
///   group of a from-scratch backfill, the case issue #59 is about) go
///   through [`apply_forced_groups_bulk`], a fixed handful of bulk statements
///   regardless of how many groups are forced, rather than a per-group probe
///   sequence each.
/// - Ordinary delta groups still probe existence and delete per group,
///   unchanged, but a batch's surviving, active groups (see
///   [`group_has_activity`]) are upserted together: a lone group still goes
///   through [`upsert_group`] directly, while more than one goes through
///   the batched [`apply_delta_groups_bulk`] instead of one
///   `upsert_group` call each (issue #63 M4).
///
/// **Deadlock avoidance.** [`super::apply::apply_target`]'s 1-1 path takes
/// every target row it will touch `FOR UPDATE` in ascending key order, in one
/// pre-lock statement, before any write — so two workers contending on
/// overlapping rows always acquire locks in the same order. This function
/// mirrors that: it pre-locks every touched group's existing target row in
/// ascending `GROUP BY`-column order before running either the bulk or the
/// per-group writes below, since the bulk `INSERT`/`DELETE` lock rows in
/// planner-chosen order (and the per-group loop's own encoded-key order need
/// not match the SQL column order) — the pre-lock, not the write order, is
/// what fixes the acquisition order once every lock is held up front.
///
/// **The seam (issue #315).** Every group written or deleted is reported to
/// `mutations`, keyed by its [`derive_group_key`] text, with its prior image
/// (the target row the pre-lock found, `None` for a new group) — the
/// pre-lock returns each locked row's image when something reads this
/// target. Only the counts come back to the caller. See
/// `super::target_mutations`.
///
/// Issue #56/ADR-0009 decision 3: the aggregate-target counterpart to
/// [`super::apply::apply_target`]'s per-transform span — same `transform`
/// field convention, same place in the propagation tree (Phase 3, one span
/// per consuming transform per batch), just for a
/// [`crate::defs::ast::KeySpace::Aggregate`] target instead of a
/// [`crate::defs::ast::KeySpace::OneToOne`] one.
#[tracing::instrument(
    name = "staging.apply_aggregate_target",
    skip(txn, plan, mutations),
    fields(
        transform = %target,
        groups = plan.groups.len(),
        written = tracing::field::Empty,
        deleted = tracing::field::Empty,
    )
)]
pub(super) async fn apply_aggregate_target(
    txn: &Transaction<'_>,
    target: &str,
    plan: &AggregateTargetPlan,
    mutations: &mut TargetMutations,
) -> Result<(usize, usize), ApplyError> {
    let mut group_keys: Vec<&String> = plan.groups.keys().collect();
    group_keys.sort();
    if group_keys.is_empty() {
        return Ok((0, 0));
    }

    // `target` — the caller's [`AggregateTargetPlan::target`] (reviewer
    // follow-up to issue #74, epic #78's own whole-branch review) — is the
    // persisted, fully-qualified identity, not a bare `def.def.target`; see
    // `super::apply::apply_and_mark_drained_many`'s call site.
    let target_ident = ddl::qualified_target_table_ident(target);
    let all_groups: Vec<&GroupPlan> = group_keys.iter().map(|k| &plan.groups[*k]).collect();
    let arity = plan.group_by.len();

    // Ascending-ordered pre-lock over every touched group's existing target
    // row, in one statement — see this function's doc comment. Locks nothing
    // for brand-new groups (no target row yet), exactly like `apply_target`'s
    // pre-lock, which is why a from-scratch backfill (all groups new) takes no
    // locks here and cannot contend.
    let prelock_arrays = transpose_group_values(arity, &all_groups);
    let prelock_null_safe: Vec<bool> = prelock_arrays
        .iter()
        .map(|a| a.iter().any(|v| v.is_none()))
        .collect();
    let prelock_params: Vec<&(dyn ToSql + Sync)> = prelock_arrays
        .iter()
        .map(|a| a as &(dyn ToSql + Sync))
        .collect();
    let order_by: Vec<String> = plan
        .group_by
        .iter()
        .map(|c| format!("t.{}", quote_ident(c)))
        .collect();
    // Issue #315: when something reads this target, the pre-lock also
    // returns each locked row's image, positioned by the keyset's own
    // ordinality (so it maps back to `group_keys[ord - 1]` exactly, with no
    // re-encoding of the group key in SQL).
    let prior_image_expr = mutations.image_sql(txn, target, "t").await?;
    let prior_select = match &prior_image_expr {
        Some(expr) => format!("k.ord, ({expr})::text"),
        None => "1".to_string(),
    };
    let prelock_sql = format!(
        "select {prior_select} from {target_ident} t join {} on {} order by {} for update of t",
        keyset_unnest(&plan.group_by_types, 1, prior_image_expr.is_some()),
        keyset_match(&plan.group_by, "t", &prelock_null_safe),
        order_by.join(", "),
    );
    let locked = txn.query(&prelock_sql, &prelock_params).await?;
    let mut prior_images: HashMap<&str, String> = HashMap::new();
    if prior_image_expr.is_some() {
        for row in locked {
            let ord: i64 = row.get(0);
            let key = group_keys[usize::try_from(ord - 1).expect("ordinality is 1-based")];
            prior_images.insert(key.as_str(), row.get(1));
        }
    }

    let mut written = Vec::new();
    let mut deleted = Vec::new();

    let forced: Vec<(&String, &GroupPlan)> = group_keys
        .iter()
        .map(|k| (*k, &plan.groups[*k]))
        .filter(|(_, g)| g.force_full_recompute)
        .collect();
    if !forced.is_empty() {
        let (w, d) = apply_forced_groups_bulk(txn, target, plan, &forced).await?;
        written.extend(w);
        deleted.extend(d);
    }

    let mut delta_groups: Vec<(&String, &GroupPlan)> = Vec::new();
    for key in group_keys {
        let group = &plan.groups[key];
        if group.force_full_recompute {
            continue;
        }
        let exists = probe_group_exists(txn, plan, &group.group_values).await?;

        if !exists {
            if delete_group_row(
                txn,
                target,
                &plan.group_by,
                &plan.group_by_types,
                &group.group_values,
            )
            .await?
            {
                deleted.push((key.clone(), group.hop_gen, group.src_changed));
            }
            continue;
        }

        if group_has_activity(plan, group) {
            delta_groups.push((key, group));
        }
    }

    match delta_groups.len() {
        0 => {}
        1 => {
            let (key, group) = delta_groups[0];
            if upsert_group(txn, target, plan, group).await? {
                written.push((key.clone(), group.hop_gen, group.src_changed));
            }
        }
        _ => {
            apply_delta_groups_bulk(txn, target, plan, &delta_groups).await?;
            written.extend(
                delta_groups
                    .iter()
                    .map(|(key, group)| ((*key).clone(), group.hop_gen, group.src_changed)),
            );
        }
    }

    let span = tracing::Span::current();
    span.record("written", written.len());
    span.record("deleted", deleted.len());
    let counts = (written.len(), deleted.len());
    for (key, hop_gen, src_changed) in written.into_iter().chain(deleted) {
        let prior = prior_images.remove(key.as_str());
        mutations.record(target, key, prior, hop_gen, src_changed);
    }
    Ok(counts)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_postgres::NoTls;

    fn row(pairs: &[(&str, Option<&str>)]) -> Row {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), v.map(str::to_string)))
            .collect()
    }

    /// Issue #120: `COUNT(<expr>)`'s row-contribution text is `"0"`/`"1"`
    /// (never absent — see [`is_pushable_contribution`]'s own doc comment),
    /// so [`diff_contributions`] must recognize a `Count` field's `"0"`
    /// contribution as "skip this row" the same way it recognizes `None` for
    /// `Sum`/`Avg`. Isolates the exact bug this issue's own review found
    /// (silently over-counting `COUNT(<column>)` because a `"0"`
    /// contribution used to be pushed just like a real `SUM` `"0"` would be)
    /// with no live database in the loop.
    #[test]
    fn diff_contributions_treats_a_count_fields_zero_contribution_as_a_skip() {
        let field = AggFieldPlan {
            name: "n".to_string(),
            value_type: ValueType::Integer(crate::integer::IntWidth::Int8),
            kind: AggFieldKind::Count,
        };

        // non-null -> null: must push into `subs` only.
        let mut group = GroupPlan::new(vec![Some("1".to_string())]);
        diff_contributions(
            std::slice::from_ref(&field),
            &mut group,
            &HashMap::from([("n".to_string(), Some("1".to_string()))]),
            &HashMap::from([("n".to_string(), Some("0".to_string()))]),
        );
        let accum = &group.field_accum["n"];
        assert_eq!(accum.adds.len(), 0, "a '0' contribution must not be added");
        assert_eq!(
            accum.subs.len(),
            1,
            "the prior '1' contribution must be subtracted"
        );

        // null -> non-null: must push into `adds` only.
        let mut group = GroupPlan::new(vec![Some("1".to_string())]);
        diff_contributions(
            std::slice::from_ref(&field),
            &mut group,
            &HashMap::from([("n".to_string(), Some("0".to_string()))]),
            &HashMap::from([("n".to_string(), Some("1".to_string()))]),
        );
        let accum = &group.field_accum["n"];
        assert_eq!(
            accum.adds.len(),
            1,
            "the new '1' contribution must be added"
        );
        assert_eq!(
            accum.subs.len(),
            0,
            "a '0' contribution must not be subtracted"
        );

        // null -> null (no change): no entry at all, matching
        // `group_has_activity`'s "no real activity" signal.
        let mut group = GroupPlan::new(vec![Some("1".to_string())]);
        diff_contributions(
            std::slice::from_ref(&field),
            &mut group,
            &HashMap::from([("n".to_string(), Some("0".to_string()))]),
            &HashMap::from([("n".to_string(), Some("0".to_string()))]),
        );
        assert!(!group.field_accum.contains_key("n"));
    }

    /// [`add_contributions`]'s own twin of the above: a `Count` field's `"0"`
    /// contribution (row entering the group with its counted argument
    /// `NULL`) must not be pushed into `adds`, while `COUNT(*)`'s
    /// always-`"1"` contribution (or a genuine non-null `COUNT(<column>)`
    /// contribution) still is.
    #[test]
    fn add_contributions_skips_a_count_fields_zero_contribution() {
        let field = AggFieldPlan {
            name: "n".to_string(),
            value_type: ValueType::Integer(crate::integer::IntWidth::Int8),
            kind: AggFieldKind::Count,
        };
        let mut group = GroupPlan::new(vec![Some("1".to_string())]);
        add_contributions(
            std::slice::from_ref(&field),
            &mut group,
            &HashMap::from([("n".to_string(), Some("0".to_string()))]),
        );
        let accum = &group.field_accum["n"];
        assert!(
            accum.adds.is_empty(),
            "a '0' contribution must not be added"
        );

        add_contributions(
            std::slice::from_ref(&field),
            &mut group,
            &HashMap::from([("n".to_string(), Some("1".to_string()))]),
        );
        assert_eq!(group.field_accum["n"].adds.len(), 1);
    }

    /// Issue #171: a composite `GROUP BY`'s key must be the crate's single
    /// composite primary-key encoding ([`ddl::pk_key_sql_expr`]'s
    /// U+001F join, in `GROUP BY` order — the same order
    /// `ddl::create_aggregate_target_table` declares the target's unique
    /// constraint in, and therefore the order `ddl::source_primary_key`
    /// reports it back in), not the length-prefixed form this used to emit
    /// (`"2:w1" + "1:a"`), so a chained definition's live re-fetch
    /// ([`ddl::split_pk_key`]) decodes it as an ordinary composite source PK.
    #[test]
    fn derive_group_key_encodes_a_composite_group_as_a_composite_primary_key() {
        let group_by = vec!["warehouse".to_string(), "sku".to_string()];
        let (values, key) = derive_group_key(
            &row(&[("warehouse", Some("w1")), ("sku", Some("a"))]),
            &group_by,
            &[ValueType::Text, ValueType::Text],
        );
        assert_eq!(
            values,
            vec![Some("w1".to_string()), Some("a".to_string())],
            "the typed per-column values every SQL statement binds are unchanged"
        );
        assert_eq!(key, "w1\u{1f}a");

        let pk = vec![
            ddl::PrimaryKeyColumn {
                name: "warehouse".to_string(),
                data_type: "text".to_string(),
                // An aggregate target's grouping columns are nullable
                // (`UNIQUE NULLS NOT DISTINCT`), which is what selects the
                // NULL-sentinel encoding `derive_group_key` produces.
                nullable: true,
            },
            ddl::PrimaryKeyColumn {
                name: "sku".to_string(),
                data_type: "text".to_string(),
                // An aggregate target's grouping columns are nullable
                // (`UNIQUE NULLS NOT DISTINCT`), which is what selects the
                // NULL-sentinel encoding `derive_group_key` produces.
                nullable: true,
            },
        ];
        assert_eq!(
            ddl::split_pk_key(&pk, "stock_totals", &key)
                .expect("decodes as a composite PK")
                .iter()
                .map(Option::as_deref)
                .collect::<Vec<_>>(),
            vec![Some("w1"), Some("a")],
            "the downstream consumer must decode exactly the grouping values back"
        );
    }

    /// Issue #200: a group whose own column value genuinely contains
    /// `ddl::COMPOSITE_KEY_SEPARATOR` must still decode back to that exact
    /// value, not be misread as an extra field boundary — the same class of
    /// gap issue #171's arity check above guards, except this one could fire
    /// on perfectly valid source data. The grouping columns are nullable, so
    /// this simultaneously exercises issue #110's `NULL_KEY_SENTINEL` layer
    /// underneath the #200 escape: one component is a real `NULL`, one holds
    /// a real U+0001 *and* a real U+001F/U+001E at once.
    #[test]
    fn derive_group_key_round_trips_a_real_separator_valued_component() {
        let group_by = vec!["warehouse".to_string(), "sku".to_string()];
        let (_, key) = derive_group_key(
            &row(&[("warehouse", Some("w1\u{1f}extra")), ("sku", Some("a"))]),
            &group_by,
            &[ValueType::Text, ValueType::Text],
        );
        assert_ne!(
            key, "w1\u{1f}extra\u{1f}a",
            "a real separator inside a component must not pass through unescaped"
        );

        let pk = vec![
            ddl::PrimaryKeyColumn {
                name: "warehouse".to_string(),
                data_type: "text".to_string(),
                nullable: true,
            },
            ddl::PrimaryKeyColumn {
                name: "sku".to_string(),
                data_type: "text".to_string(),
                nullable: true,
            },
        ];
        assert_eq!(
            ddl::split_pk_key(&pk, "stock_totals", &key)
                .expect("decodes despite the embedded separator")
                .iter()
                .map(Option::as_deref)
                .collect::<Vec<_>>(),
            vec![Some("w1\u{1f}extra"), Some("a")],
        );

        // Both layers at once: a genuinely NULL component alongside one
        // carrying every control character either scheme reserves.
        let (_, both) = derive_group_key(
            &row(&[("warehouse", None), ("sku", Some("\u{1}\u{1f}\u{1e}\u{1}"))]),
            &group_by,
            &[ValueType::Text, ValueType::Text],
        );
        assert_eq!(
            ddl::split_pk_key(&pk, "stock_totals", &both)
                .expect("decodes with both escape layers in play")
                .iter()
                .map(Option::as_deref)
                .collect::<Vec<_>>(),
            vec![None, Some("\u{1}\u{1f}\u{1e}\u{1}")],
            "issue #110's NULL sentinel and issue #200's separator escape must \
             compose without either corrupting the other"
        );
    }

    /// Issue #103's single-column case, unchanged by #171: one grouping
    /// column yields its bare value, byte-identical to the aggregate
    /// target's real single-column identity (no separator, no prefix).
    #[test]
    fn derive_group_key_leaves_a_single_column_group_unencoded() {
        let group_by = vec!["order_id".to_string()];
        let (_, key) = derive_group_key(
            &row(&[("order_id", Some("10"))]),
            &group_by,
            &[ValueType::Text],
        );
        assert_eq!(key, "10");
    }

    /// Issue #110: a `NULL`/absent grouping component renders as
    /// [`ddl::NULL_KEY_SENTINEL`], not an empty part, so the encoded key's
    /// arity always equals the `GROUP BY`'s (keeping two different groups
    /// from colliding on one key, as before) *and* the `NULL` component is
    /// unambiguously distinguishable from a genuine empty string — the
    /// pre-#110 encoding folded both to `""`.
    #[test]
    fn derive_group_key_keeps_a_null_components_place_in_a_composite_key() {
        let group_by = vec!["warehouse".to_string(), "sku".to_string()];
        let (values, key) = derive_group_key(
            &row(&[("warehouse", None), ("sku", Some("a"))]),
            &group_by,
            &[ValueType::Text, ValueType::Text],
        );
        assert_eq!(values, vec![None, Some("a".to_string())]);
        assert_eq!(key, "\u{1}\u{1f}a");
        let (_, other) = derive_group_key(
            &row(&[("sku", Some("a"))]),
            &group_by,
            &[ValueType::Text, ValueType::Text],
        );
        assert_eq!(
            other, key,
            "an absent column folds to the same NULL-sentinel part"
        );

        // Distinct from a group whose `warehouse` is a genuine empty string,
        // not NULL — the exact ambiguity issue #110 closes.
        let (_, empty_string_key) = derive_group_key(
            &row(&[("warehouse", Some("")), ("sku", Some("a"))]),
            &group_by,
            &[ValueType::Text, ValueType::Text],
        );
        assert_eq!(empty_string_key, "\u{1f}a");
        assert_ne!(
            empty_string_key, key,
            "a NULL warehouse and an empty-string warehouse must encode differently"
        );

        let pk = vec![
            ddl::PrimaryKeyColumn {
                name: "warehouse".to_string(),
                data_type: "text".to_string(),
                // An aggregate target's grouping columns are nullable
                // (`UNIQUE NULLS NOT DISTINCT`), which is what selects the
                // NULL-sentinel encoding `derive_group_key` produces.
                nullable: true,
            },
            ddl::PrimaryKeyColumn {
                name: "sku".to_string(),
                data_type: "text".to_string(),
                // An aggregate target's grouping columns are nullable
                // (`UNIQUE NULLS NOT DISTINCT`), which is what selects the
                // NULL-sentinel encoding `derive_group_key` produces.
                nullable: true,
            },
        ];
        assert_eq!(
            ddl::split_pk_key(&pk, "stock_totals", &key)
                .expect("decodes as a composite PK")
                .iter()
                .map(Option::as_deref)
                .collect::<Vec<_>>(),
            vec![None, Some("a")],
            "the downstream consumer must decode the NULL component back as None"
        );
        assert_eq!(
            ddl::split_pk_key(&pk, "stock_totals", &empty_string_key)
                .expect("decodes as a composite PK")
                .iter()
                .map(Option::as_deref)
                .collect::<Vec<_>>(),
            vec![Some(""), Some("a")],
            "and the empty-string component back as Some(\"\")"
        );
    }

    /// Issue #110's single-column twin of the composite test above: a
    /// `NULL` single-column group key must decode back to `None`, distinct
    /// from a genuine empty string, and must be the bare `NULL_KEY_SENTINEL`
    /// text (no separator), matching `ddl::pk_key_sql_expr`'s single-column
    /// rendering (`coalesce(<col>::text, chr(1))`, no `array_to_string`).
    #[test]
    fn derive_group_key_encodes_a_null_single_column_group_distinctly_from_empty_string() {
        let group_by = vec!["sku".to_string()];
        let (values, null_key) =
            derive_group_key(&row(&[("sku", None)]), &group_by, &[ValueType::Text]);
        assert_eq!(values, vec![None]);
        assert_eq!(null_key, "\u{1}");

        let (_, empty_key) =
            derive_group_key(&row(&[("sku", Some(""))]), &group_by, &[ValueType::Text]);
        assert_eq!(empty_key, "");
        assert_ne!(null_key, empty_key);

        let pk = vec![ddl::PrimaryKeyColumn {
            name: "sku".to_string(),
            data_type: "text".to_string(),
            // Nullable: an aggregate target's `UNIQUE NULLS NOT DISTINCT`
            // grouping column, not a real PRIMARY KEY.
            nullable: true,
        }];
        assert_eq!(
            ddl::split_pk_key(&pk, "sku_totals", &null_key)
                .expect("decodes")
                .iter()
                .map(Option::as_deref)
                .collect::<Vec<_>>(),
            vec![None]
        );
        assert_eq!(
            ddl::split_pk_key(&pk, "sku_totals", &empty_key)
                .expect("decodes")
                .iter()
                .map(Option::as_deref)
                .collect::<Vec<_>>(),
            vec![Some("")]
        );
    }

    /// Issue #119's own regression: `boolout` (what CDC decodes into `Row`)
    /// and `pg_catalog.text(boolean)` (what a live-refetch's `<col>::text`
    /// decodes into the very same `Row` shape) spell one boolean value two
    /// different ways — `'t'`/`'f'` vs `'true'`/`'false'`. Before
    /// `canonicalize_group_key_part`, those produced two different
    /// `derive_group_key` `text` keys for what is one Postgres `GROUP BY`
    /// group, silently fragmenting `accumulate_changes`'s `plan.groups` into
    /// two independent `GroupPlan`s that both write the same target row —
    /// see `trellis/tests/defs_boolean.rs`'s
    /// `a_boolean_group_key_seeded_by_cdc_and_by_live_read_is_one_group_not_two`
    /// for the live, end-to-end reproduction this unit test pins the root
    /// cause of.
    #[test]
    fn derive_group_key_normalizes_every_boolean_spelling_to_the_same_dedup_key() {
        let group_by = vec!["flag".to_string()];
        let types = [ValueType::Boolean];
        let terse_true = derive_group_key(&row(&[("flag", Some("t"))]), &group_by, &types).1;
        let sql_true = derive_group_key(&row(&[("flag", Some("true"))]), &group_by, &types).1;
        let terse_false = derive_group_key(&row(&[("flag", Some("f"))]), &group_by, &types).1;
        let sql_false = derive_group_key(&row(&[("flag", Some("false"))]), &group_by, &types).1;
        assert_eq!(
            terse_true, sql_true,
            "'t' and 'true' must dedup to the same GroupPlan"
        );
        assert_eq!(
            terse_false, sql_false,
            "'f' and 'false' must dedup to the same GroupPlan"
        );
        assert_ne!(
            terse_true, terse_false,
            "true and false must still be different groups"
        );

        // `values` — what every SQL statement actually binds — is
        // deliberately left untouched; only the dedup `text` is normalized.
        let (values, _) = derive_group_key(&row(&[("flag", Some("t"))]), &group_by, &types);
        assert_eq!(values, vec![Some("t".to_string())]);
    }

    /// The bulk-recompute path's extinct-group `DELETE` (step 3 of
    /// [`apply_forced_groups_bulk`]) removes a forced group's target row when
    /// no source row survives — reachable since issue #315, whenever a
    /// prior-image hint forces the group a row just *left* (see
    /// `accumulate_changes`). Driven directly with a hand-built plan here.
    #[tokio::test]
    async fn apply_forced_groups_bulk_deletes_an_extinct_forced_group() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (mut client, connection) = tokio_postgres::connect(db.dsn(), NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });

        // Empty source (no surviving rows for any group) + a target holding a
        // stale row for group 10.
        client
            .batch_execute(
                "create table order_items \
                 (id integer primary key, order_id integer, amount numeric); \
                 create table order_summary \
                 (order_id numeric primary key, total numeric, __total_count bigint); \
                 insert into order_summary (order_id, total, __total_count) \
                 values (10, 99.00, 1)",
            )
            .await
            .expect("create source/target and seed a stale target row");

        let plan = AggregateTargetPlan::new(
            &[crate::defs::ast::GroupByKey::Column("order_id".to_string())],
            vec![ValueType::Numeric],
            vec![AggFieldPlan {
                name: "total".to_string(),
                value_type: ValueType::Numeric,
                kind: AggFieldKind::Sum,
            }],
            "order_items".to_string(),
            "order_summary".to_string(),
            HashMap::from([(
                "total".to_string(),
                Expr::FunctionCall {
                    name: "SUM".to_string(),
                    args: vec![Expr::Column("amount".to_string())],
                },
            )]),
            Vec::new(),
        );

        let mut group = GroupPlan::new(vec![Some("10".to_string())]);
        group.force_full_recompute = true;
        group.hop_gen = 3;
        let key = "2:10".to_string();
        let forced = vec![(&key, &group)];

        let txn = client.transaction().await.expect("begin");
        let (written, deleted) = apply_forced_groups_bulk(&txn, "order_summary", &plan, &forced)
            .await
            .expect("bulk apply");
        txn.commit().await.expect("commit");

        assert!(written.is_empty(), "an extinct group writes nothing");
        assert_eq!(
            deleted.len(),
            1,
            "the extinct forced group must be reported deleted"
        );
        let (del_key, del_hop_gen, _del_src_changed) = &deleted[0];
        assert_eq!(del_key, &key);
        assert_eq!(*del_hop_gen, 3);

        let remaining: i64 = client
            .query_one("select count(*) from order_summary", &[])
            .await
            .expect("count order_summary")
            .get(0);
        assert_eq!(remaining, 0, "the stale target row must be gone");
    }

    /// A forced group whose key column is *NULL* must still be matched by the
    /// bulk recompute. This is the case that keeps [`keyset_match`]'s null-safe
    /// operator: with a NULL key, `col = k` never matches (even a NULL `col`),
    /// so `null_safe` must flip that column back to `is not distinct from`.
    /// Guards against the `=` fast path (chosen when no key is NULL) ever
    /// swallowing a genuinely NULL-keyed group.
    #[tokio::test]
    async fn apply_forced_groups_bulk_recomputes_a_null_keyed_group() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (mut client, connection) = tokio_postgres::connect(db.dsn(), NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });

        // Two source rows with a NULL group key contribute to the NULL group.
        // The target keys `order_id` as nullable-`unique` rather than a real
        // target's `NOT NULL` PK, so the recomputed NULL group can actually be
        // stored — this test isolates keyset_match's NULL-match semantics, not
        // the target's key-storability (a NULL group can't persist to a PK'd
        // target, independent of this fix).
        client
            .batch_execute(
                "create table order_items \
                 (id integer primary key, order_id integer, amount numeric); \
                 create table order_summary \
                 (order_id numeric unique, total numeric, __total_count bigint); \
                 insert into order_items (id, order_id, amount) \
                 values (1, null, 5.00), (2, null, 7.00)",
            )
            .await
            .expect("create source/target and seed NULL-keyed source rows");

        let plan = AggregateTargetPlan::new(
            &[crate::defs::ast::GroupByKey::Column("order_id".to_string())],
            vec![ValueType::Numeric],
            vec![AggFieldPlan {
                name: "total".to_string(),
                value_type: ValueType::Numeric,
                kind: AggFieldKind::Sum,
            }],
            "order_items".to_string(),
            "order_summary".to_string(),
            HashMap::from([(
                "total".to_string(),
                Expr::FunctionCall {
                    name: "SUM".to_string(),
                    args: vec![Expr::Column("amount".to_string())],
                },
            )]),
            Vec::new(),
        );

        // A forced group keyed by NULL (`group_values = [None]`).
        let mut group = GroupPlan::new(vec![None]);
        group.force_full_recompute = true;
        group.hop_gen = 1;
        let key = "1:".to_string();
        let forced = vec![(&key, &group)];

        let txn = client.transaction().await.expect("begin");
        let (written, deleted) = apply_forced_groups_bulk(&txn, "order_summary", &plan, &forced)
            .await
            .expect("bulk apply");
        txn.commit().await.expect("commit");

        assert_eq!(
            written,
            vec![(key.clone(), 1, None)],
            "the NULL-keyed group survives and is written"
        );
        assert!(deleted.is_empty(), "nothing is extinct");

        let total: String = client
            .query_one(
                "select total::text from order_summary where order_id is null",
                &[],
            )
            .await
            .expect("fetch NULL-keyed summary row")
            .get(0);
        assert_eq!(
            total, "12.00",
            "the NULL group's SUM must include both NULL-keyed source rows"
        );
    }

    /// A minimal, hand-built [`FoldedChange`] for a real (non-image-less)
    /// insert — no database, no ring, just enough for [`accumulate_changes`]
    /// to take its ordinary per-row delta branch. Every field this module's
    /// own logic doesn't read is a cheap, meaningless placeholder.
    fn insert_change(key: &str) -> FoldedChange {
        FoldedChange {
            src_table: "post_tags".to_string(),
            key: key.to_string(),
            new_image: Some("{}".to_string()),
            old_image: None,
            src_changed: None,
            origin_lsn: None,
            lsn: None,
            hop_gen: 0,
            first_seen: std::time::SystemTime::UNIX_EPOCH,
            group_key: None,
            is_truncate: false,
            relationship_reverse_deferred: None,
            retry_count: 0,
            prior_image: None,
        }
    }

    /// Issue #136: an ordinary (non-image-less) from-side insert into a
    /// relationship-reading aggregate must take the real per-row delta path,
    /// never [`GroupPlan::force_full_recompute`] — the direct, unit-level
    /// counterpart to this crate's integration coverage (`defs_aggregate_relationship.rs`'s
    /// `inserting_a_from_side_row_resolves_from_the_projection_not_live_parent_state`),
    /// asserted here against `accumulate_changes`'s own internal state
    /// (`force_full_recompute` and the accumulated `field_accum`, both
    /// `pub(super)`/private but visible to this child module) rather than a
    /// live target table.
    #[test]
    fn accumulate_changes_takes_the_delta_path_not_force_full_recompute_for_a_relationship_read() {
        use crate::defs::ast::{FieldDef, Predicate};
        use crate::defs::model::RelationshipCardinality;

        let def = TransformDef {
            target: "tag_totals".to_string(),
            source: "post_tags".to_string(),
            key_space: KeySpace::Aggregate {
                group_by: vec![crate::defs::ast::GroupByKey::Column("tag".to_string())],
            },
            fields: vec![
                FieldDef {
                    name: "tag".to_string(),
                    expr: Expr::Column("tag".to_string()),
                },
                FieldDef {
                    name: "total_words".to_string(),
                    expr: Expr::FunctionCall {
                        name: "SUM".to_string(),
                        args: vec![Expr::RelationshipPath {
                            rel: "post".to_string(),
                            column: "word_count".to_string(),
                        }],
                    },
                },
            ],
            predicate: Predicate::True,
            explicit_source_schema: None,
            explicit_target_schema: None,
        };
        let source_columns = HashMap::from([
            ("id".to_string(), ValueType::Numeric),
            ("post".to_string(), ValueType::Numeric),
            ("tag".to_string(), ValueType::Text),
        ]);
        let relationships = HashMap::from([(
            "post".to_string(),
            ResolvedRelationship {
                cardinality: RelationshipCardinality::ToOne,
                to_table: "posts".to_string(),
                to_col: "id".to_string(),
                column_types: HashMap::from([("word_count".to_string(), ValueType::Numeric)]),
            },
        )]);
        let group_by = vec![crate::defs::ast::GroupByKey::Column("tag".to_string())];
        let field_plans = classify_fields(
            &def,
            &group_by,
            &source_columns,
            &HashMap::from([("total_words".to_string(), def.fields[1].expr.clone())]),
            &relationships,
        )
        .expect("classify fields");
        let mut plan = AggregateTargetPlan::new(
            &group_by,
            vec![ValueType::Text],
            field_plans,
            "post_tags".to_string(),
            "tag_totals".to_string(),
            HashMap::from([("total_words".to_string(), def.fields[1].expr.clone())]),
            vec![RelJoin {
                name: "post".to_string(),
                to_table: "posts".to_string(),
                to_col: "id".to_string(),
                from_col: "post".to_string(),
            }],
        );

        let rel_ctx = eval::RelationshipContext::new(HashMap::from([(
            "post".to_string(),
            eval::ToOneRelationship {
                from_col: "post".to_string(),
                cardinality: RelationshipCardinality::ToOne,
                to_columns: HashMap::from([("word_count".to_string(), ValueType::Numeric)]),
                to_rows_by_key: HashMap::from([(
                    "1".to_string(),
                    Row::from([
                        ("id".to_string(), Some("1".to_string())),
                        ("word_count".to_string(), Some("100".to_string())),
                    ]),
                )]),
            },
        )]));

        let change = insert_change("15");
        let changes: Vec<&FoldedChange> = vec![&change];
        let rows: Vec<Option<Row>> = vec![Some(Row::from([
            ("id".to_string(), Some("15".to_string())),
            ("post".to_string(), Some("1".to_string())),
            ("tag".to_string(), Some("rust".to_string())),
        ]))];
        let old_rows: Vec<Option<Row>> = vec![None];
        let mut regex_cache = RegexCache::new();

        accumulate_changes(
            &mut plan,
            &def,
            &changes,
            &rows,
            &old_rows,
            &source_columns,
            &mut regex_cache,
            Some(&rel_ctx),
        )
        .expect("accumulate_changes");

        assert_eq!(plan.groups.len(), 1, "exactly one group touched");
        let group = plan
            .groups
            .values()
            .next()
            .expect("the 'rust' group must be present");
        assert!(
            !group.force_full_recompute,
            "an ordinary insert must take the real per-row delta path, not \
             force_full_recompute — the whole point of issue #136"
        );
        assert_eq!(
            group.group_values,
            vec![Some("rust".to_string())],
            "the group's own key must still be 'rust'"
        );
        let accum = group
            .field_accum
            .get("total_words")
            .expect("total_words must have an accumulated entry");
        assert_eq!(
            accum.adds,
            vec!["100".to_string()],
            "the row's contribution must be post 1's word_count (100), \
             resolved from the relationship context's settled projection \
             data, not left unresolved or defaulted to NULL"
        );
        assert!(
            accum.subs.is_empty(),
            "a plain insert has nothing to subtract"
        );
    }

    /// A plan with `Sum`, `Avg`, `Count`, and `RecomputeOnly` fields over
    /// `order_items`/`order_summary` — shared by the batched-upsert tests
    /// below so the schema and field wiring stay in one place.
    fn delta_plan_fields() -> (Vec<AggFieldPlan>, HashMap<String, Expr>) {
        let fields = vec![
            AggFieldPlan {
                name: "total".to_string(),
                value_type: ValueType::Numeric,
                kind: AggFieldKind::Sum,
            },
            AggFieldPlan {
                name: "avg_amount".to_string(),
                value_type: ValueType::Numeric,
                kind: AggFieldKind::Avg,
            },
            AggFieldPlan {
                name: "row_count".to_string(),
                value_type: ValueType::Numeric,
                kind: AggFieldKind::Count,
            },
            AggFieldPlan {
                name: "max_amount".to_string(),
                value_type: ValueType::Numeric,
                kind: AggFieldKind::RecomputeOnly,
            },
        ];
        let exprs = HashMap::from([
            (
                "total".to_string(),
                Expr::FunctionCall {
                    name: "SUM".to_string(),
                    args: vec![Expr::Column("amount".to_string())],
                },
            ),
            (
                "avg_amount".to_string(),
                Expr::FunctionCall {
                    name: "AVG".to_string(),
                    args: vec![Expr::Column("amount".to_string())],
                },
            ),
            (
                "max_amount".to_string(),
                Expr::FunctionCall {
                    name: "MAX".to_string(),
                    args: vec![Expr::Column("amount".to_string())],
                },
            ),
        ]);
        (fields, exprs)
    }

    fn delta_plan() -> AggregateTargetPlan {
        let (fields, exprs) = delta_plan_fields();
        AggregateTargetPlan::new(
            &[crate::defs::ast::GroupByKey::Column("order_id".to_string())],
            vec![ValueType::Numeric],
            fields,
            "order_items".to_string(),
            "order_summary".to_string(),
            exprs,
            Vec::new(),
        )
    }

    // `total` (SUM) and `avg_amount` (AVG) both aggregate the identical
    // `amount` argument, so issue #48 merges them onto one hidden running-count
    // column — `__total_count` (the first of the two fields declared) — rather
    // than each minting its own; there is deliberately no `__avg_amount_count`
    // column here.
    const ORDER_SCHEMA_SQL: &str = "\
        create table order_items \
        (id integer primary key, order_id numeric, amount numeric); \
        create table order_summary \
        (order_id numeric primary key, \
         total numeric, __total_count bigint, \
         avg_amount numeric, __avg_amount_sum numeric, \
         row_count numeric, \
         max_amount numeric)";

    /// Reads back every `order_summary` row, sorted by `order_id`, as text —
    /// used to compare the batched path's output against the unbatched
    /// per-group path's output byte-for-byte.
    async fn read_order_summary(client: &tokio_postgres::Client) -> Vec<Vec<Option<String>>> {
        let rows = client
            .query(
                "select order_id::text, total::text, __total_count::text, \
                 avg_amount::text, __avg_amount_sum::text, \
                 row_count::text, max_amount::text \
                 from order_summary order by order_id",
                &[],
            )
            .await
            .expect("read order_summary");
        rows.iter()
            .map(|r| (0..7).map(|i| r.get(i)).collect())
            .collect()
    }

    /// [`apply_delta_groups_bulk`]'s three touched-group shapes — a plain
    /// value edit on an existing row (net count delta zero, sum delta
    /// nonzero: the case that rules out a single `ON CONFLICT DO UPDATE`
    /// statement, see that function's doc comment), a brand-new group (the
    /// `INSERT` round), and an existing group gaining a row (the `UPDATE`
    /// round with an active count delta) — must land on exactly the values
    /// [`upsert_group`] would have produced one group at a time. Proves it
    /// two ways: against hand-computed expected values, and against a
    /// second database fed the same seed data one group per
    /// [`apply_aggregate_target`] call (forcing the singleton/`upsert_group`
    /// path for every group).
    #[tokio::test]
    async fn apply_delta_groups_bulk_matches_per_group_upsert_across_field_kinds() {
        let cluster = testkit::TestCluster::start();

        let seed_sql = format!(
            "{ORDER_SCHEMA_SQL}; \
             insert into order_items (id, order_id, amount) values \
             (1, 1, 8.00), (2, 2, 7.00), (10, 4, 10.00), (11, 4, 20.00); \
             insert into order_summary \
             (order_id, total, __total_count, avg_amount, __avg_amount_sum, \
              row_count, max_amount) \
             values \
             (1, 5.00, 1, 5.00, 5.00, 1, 5.00), \
             (4, 10.00, 1, 10.00, 10.00, 1, 10.00)"
        );

        // Group 1: an in-place value edit (5.00 -> 8.00) on an existing row —
        // net count delta 0, sum delta +3.00.
        let mut group_1 = GroupPlan::new(vec![Some("1".to_string())]);
        group_1.field_accum.insert(
            "total".to_string(),
            FieldAccum {
                adds: vec!["8.00".to_string()],
                subs: vec!["5.00".to_string()],
            },
        );
        group_1.field_accum.insert(
            "avg_amount".to_string(),
            FieldAccum {
                adds: vec!["8.00".to_string()],
                subs: vec!["5.00".to_string()],
            },
        );
        group_1.hop_gen = 1;

        // Group 2: a brand-new group (no target row yet) — the `INSERT` round.
        let mut group_2 = GroupPlan::new(vec![Some("2".to_string())]);
        group_2.field_accum.insert(
            "total".to_string(),
            FieldAccum {
                adds: vec!["7.00".to_string()],
                subs: vec![],
            },
        );
        group_2.field_accum.insert(
            "avg_amount".to_string(),
            FieldAccum {
                adds: vec!["7.00".to_string()],
                subs: vec![],
            },
        );
        group_2.field_accum.insert(
            "row_count".to_string(),
            FieldAccum {
                adds: vec!["1".to_string()],
                subs: vec![],
            },
        );
        group_2.hop_gen = 2;

        // Group 4: an existing group gaining a second row.
        let mut group_4 = GroupPlan::new(vec![Some("4".to_string())]);
        group_4.field_accum.insert(
            "total".to_string(),
            FieldAccum {
                adds: vec!["20.00".to_string()],
                subs: vec![],
            },
        );
        group_4.field_accum.insert(
            "avg_amount".to_string(),
            FieldAccum {
                adds: vec!["20.00".to_string()],
                subs: vec![],
            },
        );
        group_4.field_accum.insert(
            "row_count".to_string(),
            FieldAccum {
                adds: vec!["1".to_string()],
                subs: vec![],
            },
        );
        group_4.hop_gen = 3;

        let groups = vec![
            ("g1".to_string(), group_1),
            ("g2".to_string(), group_2),
            ("g4".to_string(), group_4),
        ];

        // Bulk run: all three groups through one `apply_aggregate_target`
        // call (`delta_groups.len() == 3`, so [`apply_delta_groups_bulk`]).
        let db_bulk = cluster.create_isolated_database().await;
        let (mut bulk_client, bulk_conn) = tokio_postgres::connect(db_bulk.dsn(), NoTls)
            .await
            .expect("connect bulk");
        tokio::spawn(async move {
            let _ = bulk_conn.await;
        });
        bulk_client
            .batch_execute(&seed_sql)
            .await
            .expect("seed bulk db");
        let mut bulk_plan = delta_plan();
        bulk_plan.groups = groups.clone().into_iter().collect();
        let txn = bulk_client.transaction().await.expect("begin bulk");
        let mut mutations = TargetMutations::assuming_unread();
        let (written, deleted) =
            apply_aggregate_target(&txn, "order_summary", &bulk_plan, &mut mutations)
                .await
                .expect("bulk apply");
        txn.commit().await.expect("commit bulk");
        assert_eq!(deleted, 0, "no group went extinct");
        assert_eq!(written, 3);
        let written_keys = mutations.recorded_keys("order_summary");
        assert_eq!(
            written_keys,
            vec!["g1", "g2", "g4"],
            "every active group must be reported written"
        );

        // Unbatched run: the same seed, but one `apply_aggregate_target`
        // call per group, each with only that one group in its plan — always
        // takes the `delta_groups.len() == 1` branch, i.e. [`upsert_group`]
        // directly, exactly as issue #63 M4 found it.
        let db_single = cluster.create_isolated_database().await;
        let (mut single_client, single_conn) = tokio_postgres::connect(db_single.dsn(), NoTls)
            .await
            .expect("connect single");
        tokio::spawn(async move {
            let _ = single_conn.await;
        });
        single_client
            .batch_execute(&seed_sql)
            .await
            .expect("seed single db");
        for (key, group) in &groups {
            let mut plan = delta_plan();
            plan.groups = HashMap::from([(key.clone(), group.clone())]);
            let txn = single_client.transaction().await.expect("begin single");
            apply_aggregate_target(
                &txn,
                "order_summary",
                &plan,
                &mut TargetMutations::assuming_unread(),
            )
            .await
            .expect("per-group apply");
            txn.commit().await.expect("commit single");
        }

        let bulk_rows = read_order_summary(&bulk_client).await;
        let single_rows = read_order_summary(&single_client).await;
        assert_eq!(
            bulk_rows, single_rows,
            "the batched path must produce byte-for-byte the same rows as \
             one upsert_group call per group"
        );

        assert_eq!(
            bulk_rows,
            vec![
                vec![
                    Some("1".to_string()),
                    Some("8.00".to_string()),
                    Some("1".to_string()),
                    Some("8.0000000000000000".to_string()),
                    Some("8.00".to_string()),
                    Some("1".to_string()),
                    Some("8.00".to_string()),
                ],
                vec![
                    Some("2".to_string()),
                    Some("7.00".to_string()),
                    Some("1".to_string()),
                    Some("7.0000000000000000".to_string()),
                    Some("7.00".to_string()),
                    Some("1".to_string()),
                    Some("7.00".to_string()),
                ],
                vec![
                    Some("4".to_string()),
                    Some("30.00".to_string()),
                    Some("2".to_string()),
                    Some("15.0000000000000000".to_string()),
                    Some("30.00".to_string()),
                    Some("2".to_string()),
                    Some("20.00".to_string()),
                ],
            ],
            "hand-computed expected values for each field kind"
        );
    }

    /// A single active group must still take the unbatched [`upsert_group`]
    /// path (`delta_groups.len() == 1` in [`apply_aggregate_target`]) rather
    /// than ever reaching [`apply_delta_groups_bulk`] — the degenerate case
    /// issue #63 M4 explicitly keeps on the pre-existing path.
    #[tokio::test]
    async fn apply_aggregate_target_routes_a_lone_group_through_upsert_group() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (mut client, connection) = tokio_postgres::connect(db.dsn(), NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });

        client
            .batch_execute(&format!(
                "{ORDER_SCHEMA_SQL}; \
                 insert into order_items (id, order_id, amount) values (1, 9, 7.00)"
            ))
            .await
            .expect("seed");

        let mut group = GroupPlan::new(vec![Some("9".to_string())]);
        group.field_accum.insert(
            "total".to_string(),
            FieldAccum {
                adds: vec!["7.00".to_string()],
                subs: vec![],
            },
        );
        group.field_accum.insert(
            "avg_amount".to_string(),
            FieldAccum {
                adds: vec!["7.00".to_string()],
                subs: vec![],
            },
        );
        group.field_accum.insert(
            "row_count".to_string(),
            FieldAccum {
                adds: vec!["1".to_string()],
                subs: vec![],
            },
        );
        group.hop_gen = 5;

        let mut plan = delta_plan();
        plan.groups = HashMap::from([("g9".to_string(), group)]);

        let txn = client.transaction().await.expect("begin");
        let mut mutations = TargetMutations::assuming_unread();
        let (written, _) = apply_aggregate_target(&txn, "order_summary", &plan, &mut mutations)
            .await
            .expect("apply");
        txn.commit().await.expect("commit");

        assert_eq!(written, 1);
        assert_eq!(
            mutations.recorded_keys("order_summary"),
            vec!["g9".to_string()],
            "the lone group must be reported written"
        );

        let row = client
            .query_one(
                "select total::text, row_count::text, max_amount::text \
                 from order_summary where order_id = 9",
                &[],
            )
            .await
            .expect("fetch");
        assert_eq!(row.get::<_, String>(0), "7.00");
        assert_eq!(row.get::<_, String>(1), "1");
        assert_eq!(row.get::<_, String>(2), "7.00");
    }

    /// A batch touching hundreds of brand-new groups at once — each
    /// [`build_delta_carriers`]/[`delta_carrier_unnest`] parameter is one
    /// bind slot per field regardless of group count (the arrays carrying
    /// per-group data grow in *length*, not in bind-parameter *count* — see
    /// [`apply_delta_groups_bulk`]'s doc comment), so this exercises scale
    /// without ever approaching the bind-parameter cap issue #58 fixed for
    /// the per-row literal case. Also demonstrates the round-trip reduction:
    /// one [`apply_aggregate_target`] call issues a handful of statements
    /// for the whole batch, not one `upsert_group` per group.
    #[tokio::test]
    async fn apply_delta_groups_bulk_handles_hundreds_of_new_groups() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (mut client, connection) = tokio_postgres::connect(db.dsn(), NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });

        client
            .batch_execute(ORDER_SCHEMA_SQL)
            .await
            .expect("create schema");

        const N: i64 = 500;
        let mut insert_sql = String::from("insert into order_items (id, order_id, amount) values ");
        let mut groups: HashMap<String, GroupPlan> = HashMap::new();
        for i in 1..=N {
            if i > 1 {
                insert_sql.push_str(", ");
            }
            let amount = format!("{i}.00");
            insert_sql.push_str(&format!("({i}, {i}, {amount})"));

            let mut group = GroupPlan::new(vec![Some(i.to_string())]);
            group.field_accum.insert(
                "total".to_string(),
                FieldAccum {
                    adds: vec![amount.clone()],
                    subs: vec![],
                },
            );
            group.field_accum.insert(
                "avg_amount".to_string(),
                FieldAccum {
                    adds: vec![amount],
                    subs: vec![],
                },
            );
            group.field_accum.insert(
                "row_count".to_string(),
                FieldAccum {
                    adds: vec!["1".to_string()],
                    subs: vec![],
                },
            );
            group.hop_gen = i as i32;
            groups.insert(format!("g{i}"), group);
        }
        client.batch_execute(&insert_sql).await.expect("seed");

        let mut plan = delta_plan();
        plan.groups = groups;

        let txn = client.transaction().await.expect("begin");
        let (written, deleted) = apply_aggregate_target(
            &txn,
            "order_summary",
            &plan,
            &mut TargetMutations::assuming_unread(),
        )
        .await
        .expect("bulk apply");
        txn.commit().await.expect("commit");

        assert_eq!(written, N as usize);
        assert_eq!(deleted, 0);

        let count: i64 = client
            .query_one("select count(*) from order_summary", &[])
            .await
            .expect("count")
            .get(0);
        assert_eq!(count, N, "every new group must have been inserted");

        let sample = client
            .query_one(
                "select total::text, max_amount::text from order_summary where order_id = 250",
                &[],
            )
            .await
            .expect("fetch sample");
        assert_eq!(sample.get::<_, String>(0), "250.00");
        assert_eq!(sample.get::<_, String>(1), "250.00");
    }

    /// Round 3's mop-up `UPDATE` (issue #63 M4) exists for exactly one case:
    /// a batch's round-2 `INSERT ... ON CONFLICT DO NOTHING` for a brand-new
    /// group loses the unique-constraint race to a truly concurrent writer
    /// inserting that same group. Every other test in this module runs
    /// single-connection, so round 3 has never actually executed before this
    /// test. This forces the real race with two live connections: writer A
    /// holds an uncommitted `INSERT` for group 42 open while writer B's bulk
    /// call (group 42 plus an uncontested group 43, to force the bulk path)
    /// blocks on that row's unique index, then only releases writer A once a
    /// third, monitoring connection has actually observed writer B's backend
    /// waiting on a lock in `pg_stat_activity` — a real, confirmed wait, not
    /// a sleep-and-hope. Proves the mopped-up value both against
    /// hand-computed expectations and against a second database fed the same
    /// two deltas with no race at all.
    #[tokio::test]
    async fn apply_delta_groups_bulk_mops_up_a_straggler_that_lost_the_insert_race() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;

        let (mut client_a, conn_a) = tokio_postgres::connect(db.dsn(), NoTls)
            .await
            .expect("connect a");
        tokio::spawn(async move {
            let _ = conn_a.await;
        });
        let (mut client_b, conn_b) = tokio_postgres::connect(db.dsn(), NoTls)
            .await
            .expect("connect b");
        tokio::spawn(async move {
            let _ = conn_b.await;
        });
        let (monitor, conn_m) = tokio_postgres::connect(db.dsn(), NoTls)
            .await
            .expect("connect monitor");
        tokio::spawn(async move {
            let _ = conn_m.await;
        });

        client_a
            .batch_execute(&format!(
                "{ORDER_SCHEMA_SQL}; \
                 insert into order_items (id, order_id, amount) values \
                 (1, 42, 3.00), (2, 42, 5.00), (3, 43, 10.00)"
            ))
            .await
            .expect("seed");

        let b_pid: i32 = client_b
            .query_one("select pg_backend_pid()", &[])
            .await
            .expect("b pid")
            .get(0);

        // Writer A: wins the race for group 42 — inserts its own delta
        // (amount 3.00, one row) as a brand-new group and holds the
        // transaction open, uncommitted, so the row is a live, lock-held
        // conflict for writer B's round-2 `INSERT` below.
        let txn_a = client_a.transaction().await.expect("begin a");
        txn_a
            .execute(
                "insert into order_summary \
                 (order_id, total, __total_count, avg_amount, __avg_amount_sum, \
                  row_count, max_amount) \
                 values (42, 3.00, 1, 3.00, 3.00, 1, 3.00)",
                &[],
            )
            .await
            .expect("writer a's winning insert");

        // Writer B: group 42 (amount 5.00 — the same group A just won) plus
        // group 43 (a genuinely uncontested new group), batched together so
        // `apply_aggregate_target` takes the bulk path
        // (`delta_groups.len() == 2`), not the lone-group `upsert_group`
        // path.
        let mut group_42 = GroupPlan::new(vec![Some("42".to_string())]);
        group_42.field_accum.insert(
            "total".to_string(),
            FieldAccum {
                adds: vec!["5.00".to_string()],
                subs: vec![],
            },
        );
        group_42.field_accum.insert(
            "avg_amount".to_string(),
            FieldAccum {
                adds: vec!["5.00".to_string()],
                subs: vec![],
            },
        );
        group_42.field_accum.insert(
            "row_count".to_string(),
            FieldAccum {
                adds: vec!["1".to_string()],
                subs: vec![],
            },
        );
        group_42.hop_gen = 1;

        let mut group_43 = GroupPlan::new(vec![Some("43".to_string())]);
        group_43.field_accum.insert(
            "total".to_string(),
            FieldAccum {
                adds: vec!["10.00".to_string()],
                subs: vec![],
            },
        );
        group_43.field_accum.insert(
            "avg_amount".to_string(),
            FieldAccum {
                adds: vec!["10.00".to_string()],
                subs: vec![],
            },
        );
        group_43.field_accum.insert(
            "row_count".to_string(),
            FieldAccum {
                adds: vec!["1".to_string()],
                subs: vec![],
            },
        );
        group_43.hop_gen = 2;

        let mut plan_b = delta_plan();
        plan_b.groups =
            HashMap::from([("g42".to_string(), group_42), ("g43".to_string(), group_43)]);

        // Writer B's call blocks inside round 2's `INSERT ... ON CONFLICT DO
        // NOTHING` on group 42's still-uncommitted row — a real unique-index
        // wait. This future only commits writer A once the monitor
        // connection has actually observed writer B's backend blocked on a
        // lock, so the interleaving is forced, not hoped for.
        let release_a = async {
            loop {
                let blocked: bool = monitor
                    .query_one(
                        "select exists(select 1 from pg_stat_activity \
                         where pid = $1 and wait_event_type = 'Lock')",
                        &[&b_pid],
                    )
                    .await
                    .expect("poll pg_stat_activity")
                    .get(0);
                if blocked {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
            txn_a.commit().await.expect("commit a");
        };

        let run_b = async {
            let txn_b = client_b.transaction().await.expect("begin b");
            let mut mutations = TargetMutations::assuming_unread();
            apply_aggregate_target(&txn_b, "order_summary", &plan_b, &mut mutations)
                .await
                .expect("bulk apply must mop up the straggler");
            txn_b.commit().await.expect("commit b");
            mutations
        };

        let (_, mutations) = tokio::join!(release_a, run_b);

        let written_keys = mutations.recorded_keys("order_summary");
        assert_eq!(
            written_keys,
            vec!["g42", "g43"],
            "both groups — the straggler and the uncontested one — must be reported written"
        );

        let row42 = client_a
            .query_one(
                "select total::text, __total_count::text, avg_amount::text, \
                 __avg_amount_sum::text, row_count::text, \
                 max_amount::text from order_summary where order_id = 42",
                &[],
            )
            .await
            .expect("fetch 42");
        assert_eq!(
            row42.get::<_, String>(0),
            "8.00",
            "group 42's total must be A's 3.00 plus B's 5.00 — not double-applied, not dropped"
        );
        assert_eq!(row42.get::<_, String>(1), "2");
        assert_eq!(row42.get::<_, String>(2), "4.0000000000000000");
        assert_eq!(row42.get::<_, String>(3), "8.00");
        assert_eq!(row42.get::<_, String>(4), "2");
        assert_eq!(row42.get::<_, String>(5), "5.00");

        let row43 = client_a
            .query_one(
                "select total::text, row_count::text, max_amount::text \
                 from order_summary where order_id = 43",
                &[],
            )
            .await
            .expect("fetch 43");
        assert_eq!(row43.get::<_, String>(0), "10.00");
        assert_eq!(row43.get::<_, String>(1), "1");
        assert_eq!(row43.get::<_, String>(2), "10.00");

        // Independent oracle: the same two deltas applied with no race at
        // all (A's delta first, committed, then B's) on a second database
        // must land on the identical final row for group 42 — proving
        // round 3's mop-up is equivalent to the race never happening.
        let db_seq = cluster.create_isolated_database().await;
        let (mut seq_client, seq_conn) = tokio_postgres::connect(db_seq.dsn(), NoTls)
            .await
            .expect("connect sequential");
        tokio::spawn(async move {
            let _ = seq_conn.await;
        });
        seq_client
            .batch_execute(&format!(
                "{ORDER_SCHEMA_SQL}; \
                 insert into order_items (id, order_id, amount) values \
                 (1, 42, 3.00), (2, 42, 5.00), (3, 43, 10.00)"
            ))
            .await
            .expect("seed sequential");

        let mut seed_plan = delta_plan();
        let mut seed_group = GroupPlan::new(vec![Some("42".to_string())]);
        seed_group.field_accum.insert(
            "total".to_string(),
            FieldAccum {
                adds: vec!["3.00".to_string()],
                subs: vec![],
            },
        );
        seed_group.field_accum.insert(
            "avg_amount".to_string(),
            FieldAccum {
                adds: vec!["3.00".to_string()],
                subs: vec![],
            },
        );
        seed_group.field_accum.insert(
            "row_count".to_string(),
            FieldAccum {
                adds: vec!["1".to_string()],
                subs: vec![],
            },
        );
        seed_group.hop_gen = 1;
        seed_plan.groups = HashMap::from([("g42".to_string(), seed_group)]);
        let txn = seq_client.transaction().await.expect("begin seed");
        apply_aggregate_target(
            &txn,
            "order_summary",
            &seed_plan,
            &mut TargetMutations::assuming_unread(),
        )
        .await
        .expect("seed a's delta sequentially");
        txn.commit().await.expect("commit seed");

        let mut plan_b_seq = delta_plan();
        plan_b_seq.groups = plan_b.groups.clone();
        let txn = seq_client.transaction().await.expect("begin seq b");
        apply_aggregate_target(
            &txn,
            "order_summary",
            &plan_b_seq,
            &mut TargetMutations::assuming_unread(),
        )
        .await
        .expect("apply b sequentially");
        txn.commit().await.expect("commit seq b");

        let seq_rows = read_order_summary(&seq_client).await;
        let raced_rows = read_order_summary(&client_a).await;
        assert_eq!(
            raced_rows, seq_rows,
            "the raced result must match the sequential (no-race) result exactly"
        );
    }
}
