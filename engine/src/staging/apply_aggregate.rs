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
//!   needs two hidden partial columns (`__{field}_sum`, `__{field}_count}` —
//!   see `defs::ddl::avg_partial_columns`) since `avg = sum / count` and
//!   neither half alone is invertible; the visible `avg` column is derived
//!   from them in the same statement. `SUM` needs one hidden partial
//!   column too (`__{field}_count` — see `defs::ddl::count_partial_column`),
//!   even though its own arithmetic never needs a count: Postgres's `sum()`
//!   is `NULL`, not `0`, over zero non-null values, and without a running
//!   count the delta model can't tell "this group's last non-null
//!   contributor just left" (must go `NULL`) from "this group's
//!   contributions happen to net to zero" (a real `0`) — a plain running
//!   total alone can't distinguish those two cases once other rows in the
//!   group remain. Both fields' full-recompute path (image-less changes, or
//!   a group forced onto it — see below) re-derives *both* partials from a
//!   live probe, keeping them consistent with what a fresh delta from that
//!   point forward would produce.
//! - A field that is a direct `COUNT(*)` call (issue #75) is
//!   [`AggFieldKind::Count`] — also invertible, and simpler than `SUM`: its
//!   new value is `old value + (rows added) - (rows removed)`, with no
//!   hidden partial column, since `COUNT(*)` counts every row
//!   unconditionally (no NULL-skipping ambiguity for a partial count to
//!   resolve) and a group whose count would hit zero is deleted outright
//!   rather than needing to represent a zero-row group's count.
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
//! # A known gap: image-less changes
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
//! batch. If the image-less change's live re-read finds the key already
//! gone, there is no group to locate at all (its prior group, if any, is
//! unknowable) and the change is dropped — a gap shared with any producer
//! that stages a recompute trigger for a key already deleted by the time
//! this batch drains, which is not exercised by today's producers.

use std::collections::HashMap;

use tokio_postgres::Transaction;
use tokio_postgres::types::ToSql;

use crate::defs::ast::{Expr, KeySpace, TransformDef, ValueType};
use crate::defs::ddl::{self, avg_partial_columns, count_partial_column};
use crate::defs::eval::{self, RegexCache, Row};
use crate::defs::invertibility::{self, AggregateArg, CountArg};
use crate::defs::oracle;
use crate::defs::validate;
use crate::pool::quote_ident;

use super::apply::ApplyError;
use super::fold::FoldedChange;

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
pub(super) fn classify_fields(
    def: &TransformDef,
    group_by: &[String],
    source_columns: &HashMap<String, ValueType>,
) -> Result<Vec<AggFieldPlan>, ApplyError> {
    let field_types = validate::infer_field_types(def, source_columns)?;
    let mut plans = Vec::with_capacity(def.fields.len());
    for field in &def.fields {
        if group_by.contains(&field.name) {
            continue;
        }
        let value_type = field_types
            .get(&field.name)
            .copied()
            .unwrap_or(ValueType::Numeric);
        let kind = match &field.expr {
            Expr::FunctionCall { name, args } if name == "COUNT" && args.is_empty() => {
                match invertibility::classify("COUNT", AggregateArg::Count(CountArg::Star)) {
                    Some(v) if v.is_invertible() => AggFieldKind::Count,
                    _ => AggFieldKind::RecomputeOnly,
                }
            }
            Expr::FunctionCall { name, args } if args.len() == 1 => {
                match invertibility::classify(name, AggregateArg::Column(ValueType::Numeric)) {
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
/// An entry only exists in [`GroupPlan::field_accum`] for a field that
/// actually had *some* activity this batch (see [`accumulate_changes`]'s
/// per-change cancellation) — a field with no entry is left untouched by
/// [`apply_aggregate_target`], which both suppresses a true no-op write and
/// preserves Postgres's "sum of zero non-null values is NULL" rule (a
/// brand-new group whose only rows have a NULL argument gets no entry at
/// all, so its column is left at its default `NULL` rather than a
/// synthesized `0`).
#[derive(Debug, Clone, Default)]
struct FieldAccum {
    adds: Vec<String>,
    subs: Vec<String>,
}

/// One group's pending Phase-3 write: its `GROUP BY` column values (text,
/// aligned with [`AggregateTargetPlan::group_by`]), the invertible fields'
/// accumulated deltas, the deepest `hop_gen` among the changes that touched
/// it, and whether an image-less change forced it onto the full-recompute
/// path (see the module doc comment).
#[derive(Debug, Clone)]
pub(super) struct GroupPlan {
    pub group_values: Vec<Option<String>>,
    field_accum: HashMap<String, FieldAccum>,
    pub hop_gen: i32,
    pub force_full_recompute: bool,
}

impl GroupPlan {
    fn new(group_values: Vec<Option<String>>) -> Self {
        GroupPlan {
            group_values,
            field_accum: HashMap::new(),
            hop_gen: 0,
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
    pub group_by: Vec<String>,
    pub group_by_types: Vec<ValueType>,
    pub fields: Vec<AggFieldPlan>,
    pub groups: HashMap<String, GroupPlan>,
    pub source: String,
    pub field_exprs: HashMap<String, Expr>,
}

impl AggregateTargetPlan {
    pub(super) fn new(
        group_by: Vec<String>,
        group_by_types: Vec<ValueType>,
        fields: Vec<AggFieldPlan>,
        source: String,
        field_exprs: HashMap<String, Expr>,
    ) -> Self {
        AggregateTargetPlan {
            group_by,
            group_by_types,
            fields,
            groups: HashMap::new(),
            source,
            field_exprs,
        }
    }
}

/// The `GROUP BY` columns' text values off `row`, in `group_by`'s order (a
/// missing or SQL-`NULL` column folds to `None`, matching every other
/// "absent column" convention in this crate), plus a length-prefixed
/// encoding of them suitable as a `HashMap` key — the same collision-safe
/// scheme `defs::oracle::group_key` uses for exactly the same reason (a
/// bare separator could itself appear inside a `Text` grouping column's
/// value), duplicated locally since that one is private to the oracle
/// module and this is the one other place a group needs to be named as a
/// single string.
fn derive_group_key(row: &Row, group_by: &[String]) -> (Vec<Option<String>>, String) {
    let values: Vec<Option<String>> = group_by
        .iter()
        .map(|c| row.get(c).cloned().flatten())
        .collect();
    let text = values
        .iter()
        .map(|v| {
            let s = v.clone().unwrap_or_default();
            format!("{}:{s}", s.len())
        })
        .collect::<String>();
    (values, text)
}

/// One row's contribution to every field on `def`, keyed by field name, as
/// text (`None` for a null/absent contribution — Postgres's "aggregate
/// skips NULL" rule). Reuses [`eval::evaluate_aggregate`] over a
/// single-row slice rather than a second per-row evaluator: aggregating
/// `SUM`/`AVG`/`MIN`/`MAX` over exactly one row *is* that row's
/// contribution (a sum of one value is that value; an average of one value
/// is that value; skipped-if-null falls out of the same "aggregate of zero
/// non-NULL values is NULL" rule `fold_aggregate` already implements) — see
/// the module doc comment's "The delta model" section. Only
/// [`AggFieldKind::Sum`]/[`AggFieldKind::Avg`] fields' entries are read by
/// callers; [`AggFieldKind::RecomputeOnly`] fields' one-row values here are
/// unused (and, for `MIN`/`MAX`, not even meaningful as a "contribution" —
/// those are re-derived by probe instead, never from this map).
fn row_contribution(
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
/// decoded `old_image`, or `None` when the change carried none (including
/// the image-less case, where the prior state is genuinely unknown — see
/// the module doc comment).
#[allow(clippy::too_many_arguments)]
pub(super) fn accumulate_changes(
    plan: &mut AggregateTargetPlan,
    def: &TransformDef,
    changes: &[&FoldedChange],
    rows: &[Option<Row>],
    old_rows: &[Option<Row>],
    source_columns: &HashMap<String, ValueType>,
    regex_cache: &mut RegexCache,
) -> Result<(), ApplyError> {
    let KeySpace::Aggregate { group_by } = &def.key_space else {
        panic!("accumulate_changes called on a non-aggregate definition");
    };

    for (i, change) in changes.iter().enumerate() {
        let is_image_less = change.old_image.is_none() && change.new_image.is_none();
        let old_row = &old_rows[i];
        let new_row = &rows[i];

        if is_image_less {
            if let Some(row) = new_row {
                let (values, key) = derive_group_key(row, group_by);
                let group = plan
                    .groups
                    .entry(key)
                    .or_insert_with(|| GroupPlan::new(values));
                group.force_full_recompute = true;
                group.hop_gen = group.hop_gen.max(change.hop_gen);
            }
            continue;
        }

        match (old_row, new_row) {
            (None, Some(new_row)) => {
                let (values, key) = derive_group_key(new_row, group_by);
                let contrib = row_contribution(def, new_row, source_columns, regex_cache)?;
                let group = plan
                    .groups
                    .entry(key)
                    .or_insert_with(|| GroupPlan::new(values));
                group.hop_gen = group.hop_gen.max(change.hop_gen);
                add_contributions(&plan.fields, group, &contrib);
            }
            (Some(old_row), None) => {
                let (values, key) = derive_group_key(old_row, group_by);
                let contrib = row_contribution(def, old_row, source_columns, regex_cache)?;
                let group = plan
                    .groups
                    .entry(key)
                    .or_insert_with(|| GroupPlan::new(values));
                group.hop_gen = group.hop_gen.max(change.hop_gen);
                sub_contributions(&plan.fields, group, &contrib);
            }
            (Some(old_row), Some(new_row)) => {
                let (old_values, old_key) = derive_group_key(old_row, group_by);
                let (new_values, new_key) = derive_group_key(new_row, group_by);
                let old_contrib = row_contribution(def, old_row, source_columns, regex_cache)?;
                let new_contrib = row_contribution(def, new_row, source_columns, regex_cache)?;

                if old_key == new_key {
                    let group = plan
                        .groups
                        .entry(new_key)
                        .or_insert_with(|| GroupPlan::new(new_values));
                    group.hop_gen = group.hop_gen.max(change.hop_gen);
                    for field in &plan.fields {
                        if !matches!(
                            field.kind,
                            AggFieldKind::Sum | AggFieldKind::Avg | AggFieldKind::Count
                        ) {
                            continue;
                        }
                        let old_v = old_contrib.get(&field.name).cloned().flatten();
                        let new_v = new_contrib.get(&field.name).cloned().flatten();
                        // Per-change cancellation: an unchanged contribution
                        // is not a delta at all, and skipping it is what
                        // lets `apply_aggregate_target` tell "this field had
                        // no activity this batch" (no `field_accum` entry)
                        // from "activity that happened to net to zero".
                        if old_v == new_v {
                            continue;
                        }
                        let accum = group.field_accum.entry(field.name.clone()).or_default();
                        if let Some(v) = new_v {
                            accum.adds.push(v);
                        }
                        if let Some(v) = old_v {
                            accum.subs.push(v);
                        }
                    }
                } else {
                    // Grain migration: subtract from the old group, add to
                    // the new one, independently.
                    {
                        let old_group = plan
                            .groups
                            .entry(old_key)
                            .or_insert_with(|| GroupPlan::new(old_values));
                        old_group.hop_gen = old_group.hop_gen.max(change.hop_gen);
                        sub_contributions(&plan.fields, old_group, &old_contrib);
                    }
                    {
                        let new_group = plan
                            .groups
                            .entry(new_key)
                            .or_insert_with(|| GroupPlan::new(new_values));
                        new_group.hop_gen = new_group.hop_gen.max(change.hop_gen);
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

fn add_contributions(
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
        if let Some(v) = contrib.get(&field.name).cloned().flatten() {
            group
                .field_accum
                .entry(field.name.clone())
                .or_default()
                .adds
                .push(v);
        }
    }
}

fn sub_contributions(
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
        if let Some(v) = contrib.get(&field.name).cloned().flatten() {
            group
                .field_accum
                .entry(field.name.clone())
                .or_default()
                .subs
                .push(v);
        }
    }
}

// ---------------------------------------------------------------------
// Phase 3: per-group apply
// ---------------------------------------------------------------------

/// What one [`apply_aggregate_target`] call did: every group it physically
/// wrote or deleted, as `(group_key_text, hop_gen)` pairs — the same shape
/// [`super::apply::apply_and_mark_drained`]'s downstream-propagation step
/// already consumes for the 1-1 case, so that step needs no branching of
/// its own to handle aggregate targets.
pub(super) struct AggregateApplyResult {
    pub written: Vec<(String, i32)>,
    pub deleted: Vec<(String, i32)>,
}

/// A `col IS NOT DISTINCT FROM $n::text::<cast>` clause per `group_by`
/// column, starting at `$start` — `IS NOT DISTINCT FROM` rather than `=` so
/// a `NULL` grouping column value (legal in Postgres, though this grammar's
/// own [`derive_group_key`] doesn't attempt `GROUP BY`'s "NULLs group
/// together" semantics beyond this) never silently fails to match itself.
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

fn group_where_params(values: &[Option<String>]) -> Vec<&(dyn ToSql + Sync)> {
    values.iter().map(|v| v as &(dyn ToSql + Sync)).collect()
}

/// Whether `source` still has any row for this group — the extinction/
/// creation test: a group's existence is defined by "does the source have
/// any row with these `GROUP BY` values", independent of which fields are
/// declared, so this is checked once per touched group regardless of field
/// shape, rather than inferred from any one field's own delta.
async fn probe_group_exists(
    txn: &Transaction<'_>,
    source: &str,
    group_by: &[String],
    group_by_types: &[ValueType],
    values: &[Option<String>],
) -> Result<bool, ApplyError> {
    let where_sql = group_where_clause(group_by, group_by_types, 1);
    let sql = format!("select exists(select 1 from {source} where {where_sql})");
    let row = txn.query_one(&sql, &group_where_params(values)).await?;
    Ok(row.get(0))
}

async fn delete_group_row(
    txn: &Transaction<'_>,
    target_ident: &str,
    group_by: &[String],
    group_by_types: &[ValueType],
    values: &[Option<String>],
) -> Result<bool, ApplyError> {
    let where_sql = group_where_clause(group_by, group_by_types, 1);
    let sql = format!("delete from {target_ident} where {where_sql} returning 1");
    let rows = txn.query(&sql, &group_where_params(values)).await?;
    Ok(!rows.is_empty())
}

/// Probes one [`AggFieldKind::RecomputeOnly`] (or, on the full-recompute
/// path, any) field's current value for one group directly from the source
/// — `select (<rendered expr>)::text from source where <group columns> =
/// ...`, scoped to just this group, never a full-table scan. Rendering via
/// [`oracle::render_expr_sql`] means this is not a second implementation of
/// the field's semantics: it is the exact SQL the correctness oracle itself
/// would run for this field, evaluated with no `GROUP BY` because the
/// `WHERE` clause has already restricted the input to exactly one group.
async fn probe_field_value(
    txn: &Transaction<'_>,
    source: &str,
    expr: &Expr,
    group_by: &[String],
    group_by_types: &[ValueType],
    values: &[Option<String>],
) -> Result<Option<String>, ApplyError> {
    let expr_sql = oracle::render_expr_sql(expr);
    let where_sql = group_where_clause(group_by, group_by_types, 1);
    let sql = format!("select ({expr_sql})::text from {source} where {where_sql}");
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
/// in this module.
async fn probe_sum_and_count(
    txn: &Transaction<'_>,
    source: &str,
    expr: &Expr,
    group_by: &[String],
    group_by_types: &[ValueType],
    values: &[Option<String>],
) -> Result<(Option<String>, i64), ApplyError> {
    let Expr::FunctionCall { args, .. } = expr else {
        panic!("probe_sum_and_count called on a non-SUM/AVG field");
    };
    let arg_sql = oracle::render_expr_sql(&args[0]);
    let where_sql = group_where_clause(group_by, group_by_types, 1);
    let sql =
        format!("select sum({arg_sql})::text, count({arg_sql}) from {source} where {where_sql}");
    let row = txn.query_one(&sql, &group_where_params(values)).await?;
    Ok((row.get(0), row.get(1)))
}

/// Probes a `COUNT(*)` field's raw row count for one group directly from the
/// source — the full-recompute path's counterpart to the ordinary delta
/// path's incremented visible column, used only when
/// [`GroupPlan::force_full_recompute`] is set. Unlike
/// [`probe_sum_and_count`], `COUNT(*)` has no argument expression to render;
/// this always probes `count(*)`, matching [`crate::defs::oracle::render_expr_sql`]'s
/// own `COUNT(*)` rendering.
async fn probe_count_star(
    txn: &Transaction<'_>,
    source: &str,
    group_by: &[String],
    group_by_types: &[ValueType],
    values: &[Option<String>],
) -> Result<i64, ApplyError> {
    let where_sql = group_where_clause(group_by, group_by_types, 1);
    let sql = format!("select count(*) from {source} where {where_sql}");
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
    target_ident: &str,
    plan: &AggregateTargetPlan,
    group: &GroupPlan,
) -> Result<bool, ApplyError> {
    let pk_idents: Vec<String> = plan.group_by.iter().map(|c| quote_ident(c)).collect();

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
                    let (sum_text, count) = probe_sum_and_count(
                        txn,
                        &plan.source,
                        expr,
                        &plan.group_by,
                        &plan.group_by_types,
                        &group.group_values,
                    )
                    .await?;
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
                    let (sum_text, count) = probe_sum_and_count(
                        txn,
                        &plan.source,
                        expr,
                        &plan.group_by,
                        &plan.group_by_types,
                        &group.group_values,
                    )
                    .await?;
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
                    let count = probe_count_star(
                        txn,
                        &plan.source,
                        &plan.group_by,
                        &plan.group_by_types,
                        &group.group_values,
                    )
                    .await?;
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
                let value = probe_field_value(
                    txn,
                    &plan.source,
                    expr,
                    &plan.group_by,
                    &plan.group_by_types,
                    &group.group_values,
                )
                .await?;
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

    for column in &columns {
        match column {
            ColumnPlan::SumDelta(name, count_idx) => {
                let accum = &group.field_accum[name];
                let count_col_name = count_partial_column(name);
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
                insert_cols.push(count_col.clone());
                insert_exprs.push(insert_count);

                update_sets.push(format!("{col} = {update_sum}"));
                update_sets.push(format!("{count_col} = {update_count}"));
            }
            ColumnPlan::SumForced(name, idx) => {
                let count_col_name = count_partial_column(name);
                let count_col = quote_ident(&count_col_name);
                let col = quote_ident(name);
                let sum_expr = format!("${next}::text::numeric");
                let count_expr = format!("${}::bigint", next + 1);
                params.push(&sum_count_probed[*idx].0);
                params.push(&sum_count_probed[*idx].1);
                next += 2;
                insert_cols.push(col.clone());
                insert_exprs.push(sum_expr.clone());
                insert_cols.push(count_col.clone());
                insert_exprs.push(count_expr.clone());
                update_sets.push(format!("{col} = {sum_expr}"));
                update_sets.push(format!("{count_col} = {count_expr}"));
            }
            ColumnPlan::AvgDelta(name, count_idx) => {
                let accum = &group.field_accum[name];
                let (sum_col_name, count_col_name) = avg_partial_columns(name);
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
                insert_cols.push(count_col.clone());
                insert_exprs.push(insert_count);
                insert_cols.push(avg_col.clone());
                insert_exprs.push(insert_avg);

                update_sets.push(format!("{sum_col} = {update_sum}"));
                update_sets.push(format!("{count_col} = {update_count}"));
                update_sets.push(format!("{avg_col} = {update_avg}"));
            }
            ColumnPlan::AvgForced(name, idx) => {
                let (sum_col_name, count_col_name) = avg_partial_columns(name);
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
                insert_cols.push(count_col.clone());
                insert_exprs.push(count_expr.clone());
                insert_cols.push(avg_col.clone());
                insert_exprs.push(avg_expr.clone());
                update_sets.push(format!("{sum_col} = {sum_expr}"));
                update_sets.push(format!("{count_col} = {count_expr}"));
                update_sets.push(format!("{avg_col} = {avg_expr}"));
            }
            ColumnPlan::CountDelta(name, count_idx) => {
                let col = quote_ident(name);

                let count_param = next;
                params.push(&count_deltas[*count_idx]);
                next += 1;
                let count_delta = format!("${count_param}::bigint");

                let insert_count = format!("({count_delta})::numeric");
                let update_count = format!("coalesce({target_ident}.{col}, 0) + {count_delta}");

                insert_cols.push(col.clone());
                insert_exprs.push(insert_count);
                update_sets.push(format!("{col} = {update_count}"));
            }
            ColumnPlan::CountForced(name, idx) => {
                let col = quote_ident(name);
                let count_expr = format!("${next}::bigint::numeric");
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

/// Phase 3 for one aggregate target table: for every group this batch
/// touched, in ascending group-key order (see [`upsert_group`]'s doc
/// comment on why that ordering alone, with no batched pre-lock CTE, is
/// enough to avoid the deadlocks doc 05 calls for avoiding), checks whether
/// the group still has any source rows at all and either deletes its target
/// row (extinct) or upserts its delta/probed values (still alive).
pub(super) async fn apply_aggregate_target(
    txn: &Transaction<'_>,
    target_ident: &str,
    plan: &AggregateTargetPlan,
) -> Result<AggregateApplyResult, ApplyError> {
    let mut group_keys: Vec<&String> = plan.groups.keys().collect();
    group_keys.sort();

    let mut written = Vec::new();
    let mut deleted = Vec::new();

    for key in group_keys {
        let group = &plan.groups[key];
        let exists = probe_group_exists(
            txn,
            &plan.source,
            &plan.group_by,
            &plan.group_by_types,
            &group.group_values,
        )
        .await?;

        if !exists {
            let did_delete = delete_group_row(
                txn,
                target_ident,
                &plan.group_by,
                &plan.group_by_types,
                &group.group_values,
            )
            .await?;
            if did_delete {
                deleted.push((key.clone(), group.hop_gen));
            }
            continue;
        }

        if upsert_group(txn, target_ident, plan, group).await? {
            written.push((key.clone(), group.hop_gen));
        }
    }

    Ok(AggregateApplyResult { written, deleted })
}
