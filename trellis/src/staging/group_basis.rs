//! Aggregate targets on the group basis (#1100 G2a, ADR-0002 "Aggregates: a
//! basis per group, kept by deltas").
//!
//! # Which definitions take it
//!
//! The group basis keeps every aggregate definition: define refuses an
//! aggregate outside #1096 decision 1's admitted set by family
//! (`defs::validate::validate_aggregate_admitted`), so [`route`] takes every
//! aggregate define accepts. A definition [`route`] takes:
//!
//! - is built, fresh or on a resume, by the plan job's one absolute write of
//!   every group ([`rebuild`]), over a partitioned source one transaction per
//!   leaf of its recorded tree and one for the groups that route to no leaf
//!   (an unrecorded tree only builds over an empty source); a resume drops
//!   and recreates the hidden table and keeps the visible target;
//! - is rebuilt in place once `live` ([`rebuild`]: `request_backfill`, a
//!   capture re-install, an automatic rebuild, or an upstream's rebuild),
//!   over a partitioned source one transaction per leaf of its recorded tree
//!   and one for the groups that route to no leaf (`super::build`'s
//!   `run_group_rebuild`).
//!
//! Over a partitioned source, its partition maintenance is corrected by
//! recomputing the groups that route to the node (#1103,
//! [`recompute_routed`], `super::partition_correction`'s "On the group
//! basis").
//!
//! # A source that is another definition's target
//!
//! A target is never captured. Its writers stage its changes through the
//! target-mutation seam, and a target with a group-basis reader is on the
//! seam's image feed (`super::target_mutations`, "The image feed"): each
//! changed key arrives as a CDC-shaped ring row with its exact old and new
//! images, ordered by the writer's write token, with the writer's
//! transaction as its `row_txid`. A page consumes those rows exactly as it
//! consumes captured ones. An upstream group with no visible row is absent
//! from the images, so a rollup's sums telescope as the group empties and
//! refills.
//!
//! An upstream build or rebuild that writes the target outside the seam
//! parks a catch-up for its readers, and a group-basis reader is rebuilt
//! instead (`crate::intake::catch_up::park_target_catchup_if_read`): an
//! image-less catch-up names keys, not the deltas a group needs. A group-basis
//! build that is a fresh build or a resume parks it too, though it writes
//! through the seam: a resume empties the partials and keeps the target, so
//! the prior images of the seam rows it and its pages stage match no row the
//! readers hold, and the readers' rebuilds overwrite what they did (#1287).
//!
//! # Which definitions
//!
//! A plain aggregate (every `GROUP BY` key a source column) whose every value field composes the admitted aggregates of #1096
//! decision 1 (`COUNT(*)`, `COUNT(x)`, and `SUM`/`AVG` over an integer
//! argument or a `numeric` column) with literals, operators, functions and
//! its `GROUP BY` keys. An argument may be an expression over the row
//! (`SUM(a + b)`); a `numeric` one must be a column, whose declared scale
//! define checks. Define refuses any other aggregate by family.
//!
//! # Where its state lives
//!
//! The visible target is a plain table holding exactly the declared columns
//! and the `UNIQUE NULLS NOT DISTINCT` group identity (#1096 decision 7). The
//! group's partials, its basis and its stamp live in a hidden table in the
//! instance's catalog schema, named by definition id ([`hidden_table_name`],
//! `agg_<id>`, beside a `JOIN`'s `join_<id>_*` tables): the row count
//! `__n`, and per distinct aggregate argument `i` (1-based, in
//! [`distinct_arguments`]' order) its non-null count `__count_<i>` and,
//! when a `SUM` or `AVG` reads it, its sum `__sum_<i>` as `numeric`. A group
//! row is visible in the target only while `__n > 0`; a zero group stays in
//! the hidden table until the GC collects it (below).
//!
//! # Zero groups
//!
//! A group whose partials are all zero (`__zero`, a stored generated
//! column) has no visible row, but its hidden row can still matter (ADR-0002
//! R4): an absolute write (a build's or a recompute's, #1101/#1102) sets
//! its basis, and a change that basis saw but a page hasn't drained yet must
//! still be refused. A fresh row has no basis and would apply it twice. So
//! the absolute write also sets the stamp, the newest segment holding a
//! change its snapshot saw (R5), and a zero group with a basis stays until
//! the contiguous drained prefix reaches its stamp: then every change the
//! basis saw has been applied or refused. A zero group with no basis
//! protects nothing, and a group with any non-zero partial is never
//! collected, whatever its `__n`. A basis with no stamp keeps its group.
//!
//! The maintenance loop's tombstone pass collects them
//! (`super::retire::collect_tombstones`), in batches that claim rows `for
//! update skip locked` in key order through the partial `where __zero`
//! index ([`collect_zero_groups_sql`]). It never waits on a page: a page
//! holds the locks of every group it applies to from its lock statement to
//! its commit. A page whose lock statement meets a group the GC is deleting
//! waits for the GC's commit and inserts the group afresh. Pages only make
//! `__zero` change when a group empties or refills, so an update that keeps
//! a group non-zero changes no indexed column and stays HOT.
//!
//! A `JOIN`'s `P` and `M` cells are groups of this kind, written by the
//! same writer, and their zero cells are collected by the same rule and
//! statement (`crate::defs::join`'s hidden tables). The pass takes no `J`
//! lock: a zero cell adds nothing to the product, so a page that reads the
//! key's cells under `J` gets the same `T` delta with it or without it. Its
//! `J` rows are never collected (ADR-0019).
//!
//! Its `T` groups are collected too, whatever the drained prefix: `T` has no
//! basis, since it changes only by the product's deltas, which no basis
//! filters, so a zero `T` group protects nothing, and adding a delta to it or
//! to a fresh group gives the same row. Every writer of `T` (a page, a
//! release, a partition correction, each build step) creates and locks the
//! groups it writes in one sorted `insert … on conflict` before it adds to
//! them, so a group the pass removed is inserted afresh and no delta lands on
//! a group the writer hasn't locked. A build's enumeration of `T`'s groups
//! reads without a lock, and its lock statement re-inserts a group collected
//! since, at zero. Nothing reads `T` to compute a delta, and `self_check`
//! counts a group `T` lacks as zeros.
//!
//! Zero groups left in place aren't free: #1043's JOIN model ran 16x slower
//! with thousands of them on a few hot keys, and a definition whose groups
//! churn (a `GROUP BY` over a session id, say) would grow its hidden table
//! without bound.
//!
//! # One page, one target ([`apply_group_target`])
//!
//! 1. **Stage** (in [`lock_groups`]): one statement splits each ring row
//!    into sides (its old image negated and its new image, or its
//!    transition images, #1099), evaluates the group and each argument on
//!    each, and sums them per (group, `row_txid`, statement) into a session
//!    temporary table. It reads no basis, so this work, most of a page's,
//!    happens before any group is locked.
//! 2. **Lock** ([`lock_groups`], R1, R7): one statement inserts every group
//!    the staged sides name, sorted, and locks the ones that exist
//!    (`on conflict … do update … where false` locks without writing).
//! 3. A caller may do its own work here, holding the groups' locks: a
//!    `JOIN` (`super::join`) calls the steps itself, on its `P`, `M` and
//!    `T` ([`Writer`]), with its `J` locks between them.
//! 4. **Apply** ([`apply_locked_groups`], R2, F2): one later statement, so
//!    under `READ COMMITTED` its snapshot sees every committed basis. It
//!    sums the staged sides per (group, `row_txid`), keeps a sum only when
//!    its transaction isn't visible in the group's basis (a `NULL` basis
//!    sees nothing) and its `lsn` is above the target's truncate floor, adds
//!    what is kept to the hidden row, and writes the visible row when a
//!    visible value changed: an upsert while `__n > 0`, a delete when it
//!    falls to 0. It returns, per group, the change it applied. It runs with
//!    nested loops off, since its joins are between the page's own rows,
//!    whose sizes the planner can't see, and sequential scans off: it reaches
//!    the hidden table and the target only through their group identity
//!    index, one probe per group, and the hidden rows it updates by `ctid`,
//!    so its cost under the groups' locks is the page's groups, not the
//!    definition's.
//!
//! # Image-less changes, releases and truncates (#1102)
//!
//! An image-less change (a `Recompute`: a release of a key, or a marker's
//! re-read) has no sides. The
//! page recomputes the groups it names instead: its key's current row's
//! group and its prior-image hint's. The page's lock statement locks those
//! groups with its sides' groups, and after the page's sides apply,
//! [`recompute_locked_groups`] rewrites each of them from a `GROUP BY` of its
//! source rows, the statement's snapshot as its basis (R1, R5, R6).
//!
//! A held key's changes are skipped and parked: its held row keeps the
//! groups of every parked image (`poison_held.agg_groups`, the evicting
//! change's among them), not the images. A release or the automatic retry
//! recomputes those groups and the key's current row's ([`release`]), so
//! every group the key could owe is rewritten (R8) whatever absolute writes
//! reached them during the hold.
//!
//! A source `TRUNCATE` empties the hidden table and raises the target's
//! truncate floor on the truncate's own page ([`truncate_groups`]); the
//! drain barrier orders that page after every page of earlier segments and
//! before every page of later ones (R9).
//!
//! # A build or rebuild ([`rebuild`], #1101)
//!
//! A build or rebuild recomputes every group in place, as one absolute write
//! per source (a fresh build starts from an empty hidden table, and its
//! enumeration finds the groups in the source and the ring): lock the groups of the
//! enumeration union (the target's, the source's, and those the ring's old
//! images name; R8) in key order, then one statement runs the source's
//! `GROUP BY` from its snapshot and replaces each group's partials, basis,
//! stamp and visible row. Pages keep applying, against the new bases once it
//! commits; readers see the old values until then. It runs as the rebuild's
//! one job (`super::build`'s sweep job, which a group-basis definition's
//! rebuild is alone), and a fresh build's is its plan job, after the plan's
//! fence for a source that is another definition's target.
//!
//! Sides are never folded per key (#1043 F2): the fold's first old and last
//! new image are wrong when a group's basis falls between two of a key's
//! changes. So each folded change carries its key's ring rows
//! (`super::fold::FoldedChange::group_sides`).
//!
//! A composed field (`SUM(a) + COUNT(b)`) is computed from the partials
//! whenever the visible row is written. Each changed visible row reaches the
//! target-mutation seam with its prior image, computed from the partials
//! before the page's change.

use std::collections::HashMap;
use std::time::SystemTime;

use tokio_postgres::Transaction;
use tokio_postgres::types::PgLsn;

use crate::defs::ast::{Expr, KeySpace, TransformDef, ValueType, group_by_contains};
use crate::defs::ddl::{self, PrimaryKeyColumn};
use crate::defs::oracle::{render_expr_sql, render_qualified_expr_sql};
use crate::integer::IntWidth;
use crate::pool::{quote_ident, quote_literal};

use super::apply::ApplyError;
use super::fold::{FoldedChange, earliest_origin};
use super::target_mutations::TargetMutations;

/// Planted bug `gb_fold_sides_per_key` (`crate::plant`): a key's ring rows
/// (`FoldedChange::group_sides`) folded into one, as the per-key fold folds
/// them: the first row's old image and the last row's new image, under the
/// last row's transaction and `lsn`. `None` when there is nothing to fold
/// (one row), or a row is a `Recompute` or carries transition images.
#[cfg(any(test, feature = "test-util"))]
fn planted_fold(sides: &[String]) -> Option<String> {
    if sides.len() < 2 {
        return None;
    }
    let rows: Vec<serde_json::Value> = sides
        .iter()
        .map(|side| serde_json::from_str(side).ok())
        .collect::<Option<_>>()?;
    if rows.iter().any(|row| {
        !matches!(row["op"].as_str(), Some("insert" | "update" | "delete")) || row["x"].is_object()
    }) {
        return None;
    }
    let (first, last) = (&rows[0], &rows[rows.len() - 1]);
    Some(
        serde_json::json!({
            "t": last["t"],
            "l": last["l"],
            "op": "update",
            "o": first["o"],
            "n": last["n"],
            "x": null,
        })
        .to_string(),
    )
}

/// One aggregate call of a definition's fields, over the group's partials.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum Agg {
    /// `COUNT(*)`: `__n`.
    CountStar,
    /// `COUNT(x)`: argument `x`'s non-null count.
    Count(usize),
    /// `SUM(x)`: its sum, `NULL` while its count is 0, cast to `cast`
    /// (`bigint` over `int2`/`int4`, `numeric` otherwise, as Postgres's
    /// `sum()` returns).
    Sum(usize, &'static str),
    /// `AVG(x)`: its sum over its count, as `numeric` (`numeric_div`, which
    /// is what Postgres's `avg()` over an exact argument computes).
    Avg(usize),
}

/// One distinct aggregate argument.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct Arg {
    /// The argument over the image populated as the source's row type `s`.
    pub(super) sql: String,
    /// The argument, for the audit to render on its own
    /// (`super::self_check_groups`).
    pub(super) expr: Expr,
    /// Whether a `SUM` or `AVG` reads it, so it has a `__sum_<i>`.
    pub(super) summed: bool,
}

/// What a group-basis definition's target looks like ([`route`]).
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct GroupShape {
    /// The `GROUP BY` columns: the source's, the target's and the hidden
    /// table's column of that name.
    pub(super) group_cols: Vec<String>,
    /// Each `GROUP BY` column's type in the target and hidden table. Unread
    /// by a `JOIN`'s writers, whose working tables copy the hidden table's
    /// columns ([`Writer`]).
    pub(super) group_types: Vec<String>,
    pub(super) args: Vec<Arg>,
    pub(super) aggs: Vec<Agg>,
    /// Each value field: its name, its type in the target, and its
    /// expression with each aggregate call replaced by its placeholder
    /// column ([`placeholder`]).
    pub(super) fields: Vec<(String, String, Expr)>,
    /// The `numeric` source columns a `SUM` or `AVG` reads, whose declared
    /// scale define checks ([`check_define`]).
    pub(super) numeric_args: Vec<String>,
}

impl GroupShape {
    /// The `GROUP BY` columns: the source's, the target's and the hidden
    /// table's column of that name.
    pub(crate) fn group_columns(&self) -> &[String] {
        &self.group_cols
    }

    /// Each `GROUP BY` column's type in the target and hidden table, in
    /// [`Self::group_columns`]' order.
    pub(crate) fn group_types(&self) -> &[String] {
        &self.group_types
    }
}

/// The column an aggregate call is read from inside a field's expression.
pub(super) fn placeholder(k: usize) -> String {
    format!("__trellis_a{k}")
}

/// Whether the group basis takes `def` (see the module doc), and its shape
/// if so.
pub(crate) fn route(
    def: &TransformDef,
    source_columns: &HashMap<String, ValueType>,
) -> Option<GroupShape> {
    let KeySpace::Aggregate { group_by } = &def.key_space else {
        return None;
    };
    let mut group_cols = Vec::with_capacity(group_by.len());
    let mut group_types = Vec::with_capacity(group_by.len());
    for key in group_by {
        let value_type = source_columns.get(key.as_str())?;
        group_cols.push(key.clone());
        group_types.push(ddl::pg_type_name(*value_type).into_owned());
    }
    let substituted = crate::defs::backfill::substituted_field_exprs(def).ok()?;
    let field_types = crate::defs::validate::infer_field_types(def, source_columns).ok()?;
    let distinct = distinct_arguments(&def.fields, group_by, &substituted);
    let mut shape = GroupShape {
        group_cols,
        group_types,
        args: distinct
            .iter()
            .map(|arg| Arg {
                sql: render_qualified_expr_sql(arg, "s"),
                expr: arg.clone(),
                summed: false,
            })
            .collect(),
        aggs: Vec::new(),
        fields: Vec::new(),
        numeric_args: Vec::new(),
    };
    let arg_of = |arg: &Expr| distinct.iter().position(|d| d == arg);
    for field in &def.fields {
        if group_by_contains(group_by, &field.name) {
            continue;
        }
        let type_of = |arg: &Expr| crate::defs::validate::infer_expr_type(arg, source_columns).ok();
        let expr = rewrite(&substituted[&field.name], &type_of, &arg_of, &mut shape)?;
        let pg_type = ddl::pg_type_name(*field_types.get(&field.name)?).into_owned();
        shape.fields.push((field.name.clone(), pg_type, expr));
    }
    Some(shape)
}

/// `expr` with each aggregate call replaced by its placeholder column, the
/// call added to `shape.aggs`; `None` when some call isn't admitted.
/// `type_of` types an aggregate's argument, and `arg_of` finds its index in
/// `shape.args`.
pub(super) fn rewrite(
    expr: &Expr,
    type_of: &dyn Fn(&Expr) -> Option<ValueType>,
    arg_of: &dyn Fn(&Expr) -> Option<usize>,
    shape: &mut GroupShape,
) -> Option<Expr> {
    match expr {
        Expr::FunctionCall { name, args } if is_aggregate(name) => {
            let agg = match (name.to_ascii_uppercase().as_str(), args.as_slice()) {
                ("COUNT", []) => Agg::CountStar,
                ("COUNT", [arg]) => Agg::Count(arg_of(arg)?),
                (function @ ("SUM" | "AVG"), [arg]) => {
                    let i = arg_of(arg)?;
                    let value_type = type_of(arg)?;
                    let cast = match value_type {
                        ValueType::Integer(IntWidth::Int2 | IntWidth::Int4) => "bigint",
                        ValueType::Integer(IntWidth::Int8) => "numeric",
                        // A `numeric` argument only as a column, whose
                        // declared scale define checks: an expression's
                        // `numeric` result has none (#1096 decision 2).
                        ValueType::Numeric => match arg {
                            Expr::Column(column) | Expr::SourceColumn { column, .. } => {
                                if !shape.numeric_args.contains(column) {
                                    shape.numeric_args.push(column.clone());
                                }
                                "numeric"
                            }
                            _ => return None,
                        },
                        _ => return None,
                    };
                    shape.args[i].summed = true;
                    if function == "SUM" {
                        Agg::Sum(i, cast)
                    } else {
                        Agg::Avg(i)
                    }
                }
                _ => return None,
            };
            let k = match shape.aggs.iter().position(|a| *a == agg) {
                Some(k) => k,
                None => {
                    shape.aggs.push(agg);
                    shape.aggs.len() - 1
                }
            };
            Some(Expr::Column(placeholder(k)))
        }
        Expr::FunctionCall { name, args } => Some(Expr::FunctionCall {
            name: name.clone(),
            args: args
                .iter()
                .map(|a| rewrite(a, type_of, arg_of, shape))
                .collect::<Option<Vec<_>>>()?,
        }),
        Expr::BinaryOp { op, lhs, rhs } => Some(Expr::BinaryOp {
            op: *op,
            lhs: Box::new(rewrite(lhs, type_of, arg_of, shape)?),
            rhs: Box::new(rewrite(rhs, type_of, arg_of, shape)?),
        }),
        other => Some(other.clone()),
    }
}

/// The distinct arguments of the aggregate calls in the non-`GROUP BY`
/// fields, in field declaration order, then left to right within a field:
/// one per distinct argument, so `SUM(x)` and `AVG(x)` share one. `COUNT(*)`
/// has none. `substituted` is every field's cross-field-alias-resolved
/// expression ([`crate::defs::backfill::substituted_field_exprs`]), so an
/// aliased aggregate (`t2 = t` where `t = SUM(x)`) resolves to its source's
/// argument.
fn distinct_arguments(
    fields: &[crate::defs::ast::FieldDef],
    group_by: &[String],
    substituted: &HashMap<String, Expr>,
) -> Vec<Expr> {
    fn walk(expr: &Expr, out: &mut Vec<Expr>) {
        match expr {
            Expr::FunctionCall { name, args } if args.len() == 1 && is_aggregate(name) => {
                if !out.contains(&args[0]) {
                    out.push(args[0].clone());
                }
            }
            Expr::FunctionCall { args, .. } => args.iter().for_each(|a| walk(a, out)),
            Expr::BinaryOp { lhs, rhs, .. } => {
                walk(lhs, out);
                walk(rhs, out);
            }
            Expr::Column(_)
            | Expr::SourceColumn { .. }
            | Expr::NumberLiteral(_)
            | Expr::StringLiteral(_)
            | Expr::TypedLiteral { .. } => {}
        }
    }
    let mut out = Vec::new();
    for field in fields {
        if !group_by_contains(group_by, &field.name) {
            walk(&substituted[&field.name], &mut out);
        }
    }
    out
}

/// Whether `name` is an aggregate function, admitted or not.
fn is_aggregate(name: &str) -> bool {
    let upper = name.to_ascii_uppercase();
    crate::defs::registry::AGGREGATE_FUNCTIONS.contains(&upper.as_str())
        || crate::defs::validate::is_deferred_aggregate_name(&upper)
}

/// The hidden table's bare name in the catalog schema, for definition `id`
/// (see the module doc).
pub(crate) fn hidden_table_name(id: i64) -> String {
    format!("agg_{id}")
}

/// [`hidden_table_name`] as SQL, over the definition id expression `id`:
/// for a statement that asks which definitions are on the group basis by
/// whether their hidden table exists (`super::target_mutations`' image
/// feed).
pub(crate) fn hidden_table_name_sql(id: &str) -> String {
    format!("('agg_' || {id})")
}

/// The hidden table of definition `id`, quoted and qualified by the catalog
/// schema `schema`.
pub(crate) fn hidden_table_ident(schema: &str, id: i64) -> String {
    format!(
        "{}.{}",
        quote_ident(schema),
        quote_ident(&hidden_table_name(id))
    )
}

/// The statements creating definition `id`'s hidden table in the catalog
/// schema `schema` (see the module doc) and its zero-group index (see "Zero
/// groups" there). Created empty with the definition's row, and dropped with
/// it ([`drop_hidden_table_sql`]).
pub(crate) fn hidden_table_ddl(schema: &str, id: i64, shape: &GroupShape) -> String {
    let mut columns: Vec<String> = shape
        .group_cols
        .iter()
        .zip(&shape.group_types)
        .map(|(c, t)| format!("{} {t}", quote_ident(c)))
        .collect();
    columns.push("__n bigint not null default 0".to_string());
    let mut zero = vec!["__n = 0".to_string()];
    for (i, arg) in shape.args.iter().enumerate() {
        columns.push(format!("__count_{} bigint not null default 0", i + 1));
        zero.push(format!("__count_{} = 0", i + 1));
        if arg.summed {
            columns.push(format!("__sum_{} numeric not null default 0", i + 1));
            zero.push(format!("__sum_{} = 0", i + 1));
        }
    }
    columns.push("__basis pg_snapshot".to_string());
    columns.push("__stamp bigint".to_string());
    columns.push(format!(
        "__zero boolean not null generated always as ({}) stored",
        zero.join(" and ")
    ));
    let group_cols = quoted(&shape.group_cols).join(", ");
    columns.push(format!("unique nulls not distinct ({group_cols})"));
    let table = hidden_table_ident(schema, id);
    format!(
        "create table {table} ({}); create index on {table} ({group_cols}) where __zero",
        columns.join(", ")
    )
}

/// One zero-group GC batch's statement on the hidden table `hidden`
/// (schema-qualified and quoted), whose zero-group index keys on
/// `group_cols` (quoted, joined): deletes up to `$1` zero groups, claimed
/// `for update skip locked` in key order. With `basis` (the table has a
/// basis column), only those that have no basis or whose stamp is at or
/// below `$2`, the contiguous drained prefix; without (a `JOIN`'s `T`),
/// every zero group. See "Zero groups" in the module doc;
/// `super::retire::collect_tombstones` runs it.
pub(crate) fn collect_zero_groups_sql(hidden: &str, group_cols: &str, basis: bool) -> String {
    let rule = if basis {
        " and (__basis is null or __stamp <= $2)"
    } else {
        ""
    };
    format!(
        "delete from {hidden} where ctid = any(array( \
             select ctid from {hidden} \
             where __zero{rule} \
             order by {group_cols} limit $1 for update skip locked))"
    )
}

/// The statement dropping definition `id`'s hidden table from the catalog
/// schema `schema`, if it has one.
pub(crate) fn drop_hidden_table_sql(schema: &str, id: i64) -> String {
    format!("drop table if exists {}", hidden_table_ident(schema, id))
}

/// The visible target's `create table` statement for `shape`: the `GROUP BY`
/// columns, each value field, and the group identity, nothing else (see the
/// module doc). `target` is the target, quoted and qualified.
pub(crate) fn target_table_ddl(target: &str, shape: &GroupShape) -> String {
    let mut columns: Vec<String> = shape
        .group_cols
        .iter()
        .zip(&shape.group_types)
        .map(|(c, t)| format!("{} {t}", quote_ident(c)))
        .collect();
    for (name, pg_type, _) in &shape.fields {
        columns.push(format!("{} {pg_type}", quote_ident(name)));
    }
    columns.push(format!(
        "unique nulls not distinct ({})",
        quoted(&shape.group_cols).join(", ")
    ));
    format!("create table {target} ({})", columns.join(", "))
}

fn quoted(columns: &[String]) -> Vec<String> {
    columns.iter().map(|c| quote_ident(c)).collect()
}

/// What a [`GroupTargetPlan`]'s statements write.
#[derive(Debug, Clone)]
pub(super) enum Writer {
    /// A plain aggregate's target: the hidden table `agg_<id>` and the
    /// visible target (see the module doc).
    Aggregate,
    /// One side's partials of a `JOIN` (`P` or `M`, `super::join`): the
    /// hidden table only, with no visible row. The apply adds the change it
    /// applied to each cell to `deltas`, a session temporary table, for the
    /// `JOIN`'s product step.
    JoinPartials {
        /// Quoted, `pg_temp.`-qualified.
        deltas: String,
    },
    /// A `JOIN`'s target groups (`T`, `super::join`): its hidden partials
    /// and the visible target. Its staged rows are the page's product
    /// deltas, already exact, so no basis or truncate floor filters them.
    JoinTarget,
}

/// One page's work on one group-basis target ([`apply_group_target`]), or
/// on one of a `JOIN`'s structures ([`Writer`]).
#[derive(Debug, Clone)]
pub(crate) struct GroupTargetPlan {
    /// The target, qualified and unquoted (`transform_definitions.target_table`).
    pub(crate) target: String,
    target_ident: String,
    hidden_ident: String,
    /// The session's temporary table the page's sides are summed into
    /// before any group is locked ([`stage_sides`]), quoted.
    staged: String,
    /// The session's temporary table of the groups a page or a release
    /// recomputes ([`recompute_locked_groups`]), quoted.
    recomputed: String,
    /// The source, quoted and qualified: the row type images populate.
    source_row: String,
    /// The source as the ring names it (`src_table`), unquoted.
    source: String,
    /// The ring's segment tables, quoted and qualified, whose old images a
    /// rebuild's enumeration reads ([`rebuild`]).
    ring: Vec<String>,
    /// The source's row identity, which an image-less change's key names.
    source_pk: Vec<PrimaryKeyColumn>,
    /// The target's identity, its `GROUP BY` columns.
    identity: Vec<PrimaryKeyColumn>,
    pub(super) shape: GroupShape,
    /// Every ring row of every change the page has for this target.
    pub(super) sides: Vec<String>,
    /// The keys of the page's image-less changes, whose current rows'
    /// groups the page recomputes.
    recompute_keys: Vec<String>,
    pub(super) hop_gen: i32,
    pub(super) src_changed: Option<SystemTime>,
    pub(super) origin_lsn: Option<PgLsn>,
    pushed: bool,
    writer: Writer,
    /// The `ledger_truncate_floor` row the staged sides' `lsn` must be
    /// above, or `None` for none ([`Writer::JoinTarget`]).
    floor_key: Option<String>,
    /// More conditions, over the populated image `s`, a side must meet to
    /// be staged: ` and …` (with its leading space), or empty.
    stage_filter: String,
}

/// The columns of a plan's staging table, as DDL.
fn staged_columns(shape: &GroupShape) -> Vec<String> {
    let mut columns: Vec<String> = shape
        .group_cols
        .iter()
        .zip(&shape.group_types)
        .map(|(c, t)| format!("{} {t}", quote_ident(c)))
        .collect();
    columns.push("__txid xid8".to_string());
    columns.push("__lsn pg_lsn".to_string());
    columns.push("__n bigint".to_string());
    for (i, arg) in shape.args.iter().enumerate() {
        columns.push(format!("__c{} bigint", i + 1));
        if arg.summed {
            columns.push(format!("__s{} numeric", i + 1));
        }
    }
    columns
}

/// The columns of the recompute's session table: the group columns, typed
/// as the hidden table types them.
fn recompute_columns(shape: &GroupShape) -> Vec<String> {
    shape
        .group_cols
        .iter()
        .zip(&shape.group_types)
        .map(|(c, t)| format!("{} {t}", quote_ident(c)))
        .collect()
}

/// A short stable hash of a session temp table's columns. The tables are
/// session temp tables, and the pool keeps its connections, so a connection
/// keeps the table it first created under a name. Putting the shape in the
/// name means a definition whose group column was re-typed (a resume re-reads
/// the types under the same id) gets a new table; the old one is never
/// touched again and goes with its session. 64 bits keep two shapes of one
/// definition from sharing a name, and the name within Postgres's 63 bytes
/// for any `i64` id. A `JOIN`'s working tables use it too (`super::join`).
/// FNV-1a: fixed by definition, unlike `DefaultHasher`.
pub(super) fn shape_hash(columns: &str) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in columns.bytes() {
        hash = (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

impl GroupTargetPlan {
    pub(crate) fn new(
        catalog_schema: &str,
        definition_id: i64,
        target: &str,
        source: &str,
        source_pk: Vec<PrimaryKeyColumn>,
        identity: Vec<PrimaryKeyColumn>,
        shape: GroupShape,
    ) -> Self {
        Self {
            target: target.to_string(),
            target_ident: ddl::qualified_target_table_ident(target),
            hidden_ident: hidden_table_ident(catalog_schema, definition_id),
            staged: format!(
                "pg_temp.{}",
                quote_ident(&format!(
                    "trellis_group_sides_{definition_id}_{:016x}",
                    shape_hash(&staged_columns(&shape).join(","))
                ))
            ),
            recomputed: format!(
                "pg_temp.{}",
                quote_ident(&format!(
                    "trellis_group_recompute_{definition_id}_{:016x}",
                    shape_hash(&recompute_columns(&shape).join(","))
                ))
            ),
            source_row: ddl::qualified_source_table(source),
            source: source.to_string(),
            ring: (0..super::append::RING_SIZE)
                .map(|slot| format!("{}.seg_{slot}", quote_ident(catalog_schema)))
                .collect(),
            source_pk,
            identity,
            shape,
            sides: Vec::new(),
            recompute_keys: Vec::new(),
            hop_gen: 0,
            src_changed: None,
            origin_lsn: None,
            pushed: false,
            writer: Writer::Aggregate,
            floor_key: Some(target.to_string()),
            stage_filter: String::new(),
        }
    }

    /// The plan of one `JOIN` structure (`super::join`): `hidden` is its
    /// hidden table (quoted, qualified), `staged` its session staging table
    /// (a bare name, made `pg_temp.`), and `target` the `JOIN`'s target,
    /// which errors and the seam name. `source` is the side's table, for a
    /// [`Writer::JoinPartials`]; a [`Writer::JoinTarget`] stages nothing
    /// from images. `floor_key` names the side's truncate floor.
    /// `recomputed` is a [`Writer::JoinPartials`]' table of the cells it
    /// recomputes (a bare name, made `pg_temp.`), and `source_pk` its
    /// table's row identity, which an image-less change's key names; a
    /// [`Writer::JoinTarget`] recomputes nothing and has neither.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn for_join(
        target: &str,
        hidden: String,
        staged: &str,
        source: Option<(&str, &str, Vec<PrimaryKeyColumn>)>,
        identity: Vec<PrimaryKeyColumn>,
        shape: GroupShape,
        writer: Writer,
        floor_key: Option<String>,
        stage_filter: String,
    ) -> Self {
        let (source_name, source_row, recomputed, source_pk) = match source {
            Some((source, recomputed, pk)) => (
                source,
                ddl::qualified_source_table(source),
                format!("pg_temp.{}", quote_ident(recomputed)),
                pk,
            ),
            None => ("", String::new(), String::new(), Vec::new()),
        };
        Self {
            target: target.to_string(),
            target_ident: ddl::qualified_target_table_ident(target),
            hidden_ident: hidden,
            staged: format!("pg_temp.{}", quote_ident(staged)),
            recomputed,
            source_row,
            source: source_name.to_string(),
            ring: Vec::new(),
            source_pk,
            identity,
            shape,
            sides: Vec::new(),
            recompute_keys: Vec::new(),
            hop_gen: 0,
            src_changed: None,
            origin_lsn: None,
            pushed: false,
            writer,
            floor_key,
            stage_filter,
        }
    }

    /// This plan with the ring's segment tables, whose old images a build's
    /// enumeration reads ([`enumerate_cells`]).
    pub(super) fn with_ring(mut self, catalog_schema: &str) -> Self {
        self.ring = (0..super::append::RING_SIZE)
            .map(|slot| format!("{}.seg_{slot}", quote_ident(catalog_schema)))
            .collect();
        self
    }

    /// Adds one folded change's ring rows. An image-less change (a
    /// `Recompute`) has no sides: the page recomputes the groups it names
    /// instead (see the module doc); a `JOIN` side recomputes the cells it
    /// names (`super::join`).
    pub(crate) fn push(&mut self, change: &FoldedChange) -> Result<(), ApplyError> {
        if change.has_recompute {
            if matches!(self.writer, Writer::JoinTarget) {
                return Err(self.unsupported("an image-less change pushed to a JOIN's T"));
            }
            self.recompute_keys.push(change.key.clone());
        }
        if change.group_sides.is_empty() {
            return Err(
                self.unsupported("a page folded without the unfolded ring rows its change needs")
            );
        }
        // Planted bug (#1114): fold the key's ring rows into one side. See
        // `crate::plant`.
        #[cfg(any(test, feature = "test-util"))]
        if let Some(folded) = planted_fold(&change.group_sides)
            && crate::plant::fires(crate::plant::Plant::GbFoldSidesPerKey, true)
        {
            self.sides.push(folded);
            self.note(change);
            return Ok(());
        }
        self.sides.extend(change.group_sides.iter().cloned());
        self.note(change);
        Ok(())
    }

    /// Merges `change`'s provenance into what the seam records for this
    /// plan's rows.
    pub(super) fn note(&mut self, change: &FoldedChange) {
        if self.pushed {
            self.hop_gen = self.hop_gen.max(change.hop_gen);
            self.src_changed =
                super::apply::earliest_src_changed(self.src_changed, change.src_changed);
            self.origin_lsn = earliest_origin(self.origin_lsn, change.origin_lsn);
        } else {
            self.hop_gen = change.hop_gen;
            self.src_changed = change.src_changed;
            self.origin_lsn = change.origin_lsn;
            self.pushed = true;
        }
    }

    pub(super) fn unsupported(&self, what: &'static str) -> ApplyError {
        ApplyError::GroupBasisUnsupported {
            target: self.target.clone(),
            what,
        }
    }

    /// The CTEs every statement starts with: `raw`, one parsed ring row per
    /// element of `$1`, and `sides`, its old and new sides with their
    /// transaction, `lsn` and sign. A `Recompute` has no sides: its `o` is
    /// a prior-image hint, not a change's old image.
    fn sides_sql() -> &'static str {
        "raw as (select x::jsonb as r from unnest($1::text[]) as u(x)), \
         sides as ( \
             select (r ->> 't')::xid8 as __txid, (r ->> 'l')::pg_lsn as __lsn, -1 as __sgn, \
                    case when jsonb_typeof(r -> 'x') = 'object' then r -> 'x' -> 'old' \
                         else r -> 'o' end as __i \
             from raw where r ->> 'op' <> 'recompute' \
             union all \
             select (r ->> 't')::xid8, (r ->> 'l')::pg_lsn, 1, \
                    case when jsonb_typeof(r -> 'x') = 'object' then r -> 'x' -> 'new' \
                         else r -> 'n' end \
             from raw where r ->> 'op' <> 'recompute' \
         )"
    }

    /// `s.<group col>::<type> as <group col>`, for each `GROUP BY` column.
    /// A `JOIN` side's key columns are the image's own columns, at the
    /// hidden table's types, so they need no cast.
    fn group_values(&self) -> Vec<String> {
        self.shape
            .group_cols
            .iter()
            .zip(&self.shape.group_types)
            .map(|(c, t)| {
                let c = quote_ident(c);
                match self.writer {
                    Writer::Aggregate => format!("s.{c}::{t} as {c}"),
                    Writer::JoinPartials { .. } | Writer::JoinTarget => format!("s.{c} as {c}"),
                }
            })
            .collect()
    }

    /// The working columns a staged row or an applied delta carries after
    /// the group's: `<prefix>n`, then per argument `<prefix>c<i>` and, when
    /// summed, `<prefix>s<i>`, each `null::<type> as <name>`.
    fn working_columns(&self, n: &str, count: &str, sum: &str) -> Vec<String> {
        let mut columns = vec![format!("null::bigint as {n}")];
        for (i, arg) in self.shape.args.iter().enumerate() {
            columns.push(format!("null::bigint as {count}{}", i + 1));
            if arg.summed {
                columns.push(format!("null::numeric as {sum}{}", i + 1));
            }
        }
        columns
    }

    /// The `create` of a `JOIN` working table `table` (quoted,
    /// `pg_temp.`-qualified): the hidden table's group columns, at their
    /// exact types and collations, then `extra`. Once a session; its rows go
    /// with each page's transaction.
    fn join_working_ddl(&self, table: &str, extra: &[String]) -> String {
        let mut columns = quoted(&self.shape.group_cols);
        columns.extend(extra.iter().cloned());
        format!(
            "create temp table if not exists {} on commit delete rows as \
             select {} from {} with no data",
            table.trim_start_matches("pg_temp."),
            columns.join(", "),
            self.hidden_ident
        )
    }

    /// The `create` of [`Writer::JoinPartials`]' delta table.
    pub(super) fn deltas_ddl(&self) -> Option<String> {
        match &self.writer {
            Writer::JoinPartials { deltas } => {
                Some(self.join_working_ddl(deltas, &self.working_columns("__dn", "__dc", "__ds")))
            }
            _ => None,
        }
    }

    /// The plan's source table, quoted and qualified: what a `FROM` or a
    /// whole-table `GROUP BY` of it names.
    pub(super) fn source_row(&self) -> &str {
        &self.source_row
    }

    /// The hidden table, quoted and qualified.
    pub(super) fn hidden(&self) -> &str {
        &self.hidden_ident
    }

    /// The staging table, quoted and `pg_temp.`-qualified.
    pub(super) fn staged(&self) -> &str {
        &self.staged
    }

    /// The table of the groups (cells) this plan recomputes, quoted and
    /// `pg_temp.`-qualified ([`recompute_locked_groups`]).
    pub(super) fn recomputed(&self) -> &str {
        &self.recomputed
    }

    /// The keys of the page's image-less changes, whose current rows'
    /// groups (cells) it recomputes.
    pub(super) fn recompute_keys(&self) -> &[String] {
        &self.recompute_keys
    }

    /// The join condition matching the group of `left`'s row to `right`'s,
    /// `NULL` matching `NULL`: per column, `array[left.c] = array[right.c]`.
    /// Array equality compares elements by the column type's default
    /// equality under the column's collation and counts two `NULL` elements
    /// equal, so it matches exactly the groups `GROUP BY` forms, and,
    /// unlike `is not distinct from`, it can be hashed or merged: a join of
    /// two group sets on it is a hash or merge join, not a nested loop
    /// rescanning one set per row of the other (#1318). Both sides must hold
    /// each column at the same type (the hidden table's, as
    /// [`Self::group_values`] reads a source): unlike `is not distinct
    /// from`, array equality coerces no element type.
    pub(super) fn group_match(&self, left: &str, right: &str) -> String {
        quoted(&self.shape.group_cols)
            .iter()
            .map(|c| format!("array[{left}.{c}] = array[{right}.{c}]"))
            .collect::<Vec<_>>()
            .join(" and ")
    }

    /// The lookup of the row of `table` (quoted) whose group is `alias`'s,
    /// `NULL` matching `NULL`, returning `columns` (over `x`), as one index
    /// probe on the group identity's index: plain equality, and, for each
    /// pattern of `NULL` columns, the same equality with `x.<col> is null`
    /// for those columns, behind a one-time test that `alias`'s group has
    /// exactly those `NULL`s. `is not distinct from` would match too, but no
    /// index serves it: each page naming a group with a `NULL` would scan
    /// the whole table. Past [`MAX_NULL_PATTERN_COLUMNS`] group columns the
    /// patterns get too many, and a group with a `NULL` takes that scan.
    fn group_lookup(&self, table: &str, alias: &str, columns: &str) -> String {
        let cols = quoted(&self.shape.group_cols);
        let equal: Vec<String> = cols
            .iter()
            .map(|c| format!("x.{c} = {alias}.{c}"))
            .collect();
        let mut branches = vec![format!(
            "select {columns} from {table} x where {}",
            equal.join(" and ")
        )];
        if cols.len() <= MAX_NULL_PATTERN_COLUMNS {
            for pattern in 1..(1_u32 << cols.len()) {
                let is_null = |i: usize| pattern & (1 << i) != 0;
                let guard: Vec<String> = cols
                    .iter()
                    .enumerate()
                    .map(|(i, c)| {
                        let not = if is_null(i) { "" } else { "not " };
                        format!("{alias}.{c} is {not}null")
                    })
                    .collect();
                let probe: Vec<String> = cols
                    .iter()
                    .enumerate()
                    .map(|(i, c)| {
                        if is_null(i) {
                            format!("x.{c} is null")
                        } else {
                            format!("x.{c} = {alias}.{c}")
                        }
                    })
                    .collect();
                branches.push(format!(
                    "select {columns} from {table} x where ({}) and {}",
                    guard.join(" and "),
                    probe.join(" and ")
                ));
            }
        } else {
            let has_null: Vec<String> = cols
                .iter()
                .map(|c| format!("{alias}.{c} is null"))
                .collect();
            let same: Vec<String> = cols
                .iter()
                .map(|c| format!("x.{c} is not distinct from {alias}.{c}"))
                .collect();
            branches.push(format!(
                "select {columns} from {table} x where ({}) and {}",
                has_null.join(" or "),
                same.join(" and ")
            ));
        }
        format!("({} limit 1)", branches.join(" union all "))
    }

    /// The `create` of [`Self::staged`], one row per (group, transaction,
    /// statement) of the page with each partial's signed sum. Created once a
    /// session; its rows go with each page's transaction.
    pub(super) fn staged_ddl(&self) -> String {
        if !matches!(self.writer, Writer::Aggregate) {
            let mut extra = vec![
                "null::xid8 as __txid".to_string(),
                "null::pg_lsn as __lsn".to_string(),
            ];
            extra.extend(self.working_columns("__n", "__c", "__s"));
            return self.join_working_ddl(&self.staged, &extra);
        }
        format!(
            "create temp table if not exists {} ({}) on commit delete rows",
            self.staged.trim_start_matches("pg_temp."),
            staged_columns(&self.shape).join(", ")
        )
    }

    /// The `create` of [`Self::recomputed`], one row per group to recompute
    /// (repeats allowed). Created once a session; its rows go with each
    /// transaction. A `JOIN` side's copies its hidden table's cell columns.
    fn recomputed_ddl(&self) -> String {
        if !matches!(self.writer, Writer::Aggregate) {
            return self.join_working_ddl(&self.recomputed, &[]);
        }
        format!(
            "create temp table if not exists {} ({}) on commit delete rows",
            self.recomputed.trim_start_matches("pg_temp."),
            recompute_columns(&self.shape).join(", ")
        )
    }

    /// The `create`s of every session table this plan's page stages into:
    /// [`Self::staged`], and but for a `JOIN`'s `T` [`Self::recomputed`].
    pub(super) fn working_ddl(&self) -> String {
        match self.writer {
            Writer::Aggregate | Writer::JoinPartials { .. } => {
                format!("{}; {}", self.staged_ddl(), self.recomputed_ddl())
            }
            Writer::JoinTarget => self.staged_ddl(),
        }
    }

    /// The statement adding groups to [`Self::recomputed`] ([`name_groups`]):
    /// the group of each image of `$1` (a `text[]` of source-row images,
    /// populated as the source's row type, so an image holding only the
    /// group columns names its group), and the group of the current source
    /// row of each key, read by its typed key parts: `$2..`, one `text[]`
    /// per source key column. Each key reads its row by its own index
    /// probe. A nullable key column (another aggregate's grouping column,
    /// when the source is its target) also matches a `NULL` part.
    fn name_groups_sql(&self) -> String {
        let cols = quoted(&self.shape.group_cols).join(", ");
        let mut on = Vec::with_capacity(self.source_pk.len());
        let mut arrays = Vec::with_capacity(self.source_pk.len());
        let mut parts = Vec::with_capacity(self.source_pk.len());
        for (i, column) in self.source_pk.iter().enumerate() {
            let p = format!("__p{i}");
            let c = quote_ident(&column.name);
            on.push(if column.nullable {
                format!("(s.{c} = u.{p} or (s.{c} is null and u.{p} is null))")
            } else {
                format!("s.{c} = u.{p}")
            });
            arrays.push(format!("${}::text[]::{}[]", i + 2, column.data_type));
            parts.push(p);
        }
        format!(
            "insert into {rc} ({cols}) \
             select {values} from ( \
                 select s.* from unnest($1::text[]) as i(x) \
                 cross join lateral jsonb_populate_record(null::{src}, x::jsonb) as s \
                 union all \
                 select s.* from unnest({arrays}) as u({parts}) \
                 cross join lateral (select * from {src} s where {on}) as s \
             ) as s where true{filter}",
            rc = self.recomputed,
            values = self.group_values().join(", "),
            src = self.source_row,
            arrays = arrays.join(", "),
            parts = parts.join(", "),
            on = on.join(" and "),
            filter = self.stage_filter,
        )
    }

    /// Step 0's statement ([`stage_sides`]): parses `$1`'s ring rows, splits
    /// them into sides, evaluates the group and each argument on each, and
    /// sums them per (group, transaction, statement) into [`Self::staged`].
    /// A `Recompute`'s prior-image hint names a group to recompute
    /// ([`Self::recomputed`]). Returns the number of ring rows it can't
    /// apply as deltas; when there are any it stages nothing.
    fn stage_sql(&self) -> String {
        let cols = quoted(&self.shape.group_cols).join(", ");
        let mut values = self.group_values();
        values.push("sd.__txid".to_string());
        values.push("sd.__lsn".to_string());
        values.push("sd.__sgn".to_string());
        let mut sums = vec!["sum(__sgn)::bigint".to_string()];
        for (i, arg) in self.shape.args.iter().enumerate() {
            let i = i + 1;
            values.push(format!("{} as __a{i}", arg.sql));
            sums.push(format!("sum(__sgn * (__a{i} is not null)::int)::bigint"));
            if arg.summed {
                sums.push(format!("sum(__sgn * coalesce(__a{i}, 0)::numeric)"));
            }
        }
        // A `JOIN`'s `T` has no `Recompute` rows ([`Self::push`]) and no
        // recompute table.
        let hinted = if !matches!(self.writer, Writer::JoinTarget) {
            format!(
                "hinted as ( \
                     insert into {recomputed} ({cols}) \
                     select {group_values} from raw \
                     cross join lateral jsonb_populate_record(null::{src}, r -> 'o') as s \
                     where r ->> 'op' = 'recompute' and jsonb_typeof(r -> 'o') = 'object' \
                       and (select __bad from bad) = 0{filter} \
                 ),",
                filter = self.stage_filter,
                recomputed = self.recomputed,
                group_values = self.group_values().join(", "),
                src = self.source_row,
            )
        } else {
            String::new()
        };
        format!(
            "with {sides}, \
             bad as ( \
                 select count(*) as __bad from raw \
                 where r ->> 'op' <> 'recompute' \
                   and jsonb_typeof(r -> 'x') is distinct from 'object' \
                   and ((r ->> 'op' in ('update', 'delete') \
                         and jsonb_typeof(r -> 'o') is distinct from 'object') \
                     or (r ->> 'op' in ('insert', 'update') \
                         and jsonb_typeof(r -> 'n') is distinct from 'object')) \
             ), \
             {hinted} \
             ev as ( \
                 select {values} \
                 from sides sd cross join lateral jsonb_populate_record(null::{src}, sd.__i) as s \
                 where jsonb_typeof(sd.__i) = 'object'{filter} \
             ), \
             staged as ( \
                 insert into {staged} \
                 select {cols}, __txid, __lsn, {sums} from ev \
                 where (select __bad from bad) = 0 \
                 group by {cols}, __txid, __lsn \
             ) \
             select __bad from bad",
            sides = Self::sides_sql(),
            values = values.join(", "),
            src = self.source_row,
            staged = self.staged,
            sums = sums.join(", "),
            hinted = hinted,
            filter = self.stage_filter,
        )
    }

    /// Step 1's statement ([`lock_groups`]): inserts every group the staged
    /// sides or [`Self::recomputed`] name that has no hidden row, and locks
    /// the rest, sorted, in one statement (R1, R7). A `JOIN`'s `T` has only
    /// its staged rows.
    pub(super) fn lock_sql(&self) -> String {
        self.lock_sql_where("")
    }

    /// [`Self::lock_sql`] over only the groups that meet `filter` (` where …`
    /// over the group columns, with its leading space, or empty for all).
    pub(super) fn lock_sql_where(&self, filter: &str) -> String {
        let cols = quoted(&self.shape.group_cols).join(", ");
        let recomputed = if !matches!(self.writer, Writer::JoinTarget) {
            format!(" union all select {cols} from {}", self.recomputed)
        } else {
            String::new()
        };
        format!(
            "insert into {hidden} ({cols}) \
             select distinct {cols} from ( \
                 select {cols} from {staged}{recomputed} \
             ) as g{filter} \
             order by {cols} \
             on conflict ({cols}) do update set __n = excluded.__n where false",
            hidden = self.hidden_ident,
            staged = self.staged,
        )
    }

    /// Planted bugs `gb_skip_group_lock` and `join_t_skip_lock`
    /// (`crate::plant`): [`Self::lock_sql`]'s groups that have no hidden row
    /// yet, inserted, and the rest left unlocked.
    #[cfg(any(test, feature = "test-util"))]
    pub(super) fn planted_insert_missing_sql(&self) -> String {
        let cols = quoted(&self.shape.group_cols).join(", ");
        let recomputed = if !matches!(self.writer, Writer::JoinTarget) {
            format!(" union all select {cols} from {}", self.recomputed)
        } else {
            String::new()
        };
        format!(
            "insert into {hidden} ({cols}) \
             select distinct {cols} from ( \
                 select {cols} from {staged}{recomputed} \
             ) as g \
             where not exists (select 1 from {hidden} x where {matched}) \
             order by {cols} \
             on conflict do nothing",
            hidden = self.hidden_ident,
            staged = self.staged,
            matched = self.group_match("x", "g"),
        )
    }

    /// The SQL of aggregate `agg` over partials named by `n`, `count` and
    /// `sum` (each a function of the argument's 1-based index).
    fn agg_sql(
        agg: &Agg,
        n: &str,
        count: &dyn Fn(usize) -> String,
        sum: &dyn Fn(usize) -> String,
    ) -> String {
        match agg {
            Agg::CountStar => n.to_string(),
            Agg::Count(i) => count(i + 1),
            Agg::Sum(i, cast) => format!(
                "case when {c} = 0 then null else ({s})::{cast} end",
                c = count(i + 1),
                s = sum(i + 1)
            ),
            Agg::Avg(i) => format!(
                "case when {c} = 0 then null else ({s}) / ({c})::numeric end",
                c = count(i + 1),
                s = sum(i + 1)
            ),
        }
    }

    /// A lateral subquery computing every field over partials named by
    /// `n`, `count` and `sum`, as columns `<prefix>0`, `<prefix>1`, ….
    fn fields_lateral(
        &self,
        prefix: &str,
        n: &str,
        count: &dyn Fn(usize) -> String,
        sum: &dyn Fn(usize) -> String,
    ) -> String {
        let aggs: Vec<String> = self
            .shape
            .aggs
            .iter()
            .enumerate()
            .map(|(k, agg)| {
                format!(
                    "{} as {}",
                    Self::agg_sql(agg, n, count, sum),
                    quote_ident(&placeholder(k))
                )
            })
            .collect();
        let fields: Vec<String> = self
            .shape
            .fields
            .iter()
            .enumerate()
            .map(|(j, (_, pg_type, expr))| {
                format!("({})::{pg_type} as {prefix}{j}", render_expr_sql(expr))
            })
            .collect();
        let aggs = if aggs.is_empty() {
            "select".to_string()
        } else {
            format!("select {}", aggs.join(", "))
        };
        format!(
            "lateral (select {} from ({aggs}) as a) as {prefix}",
            fields.join(", ")
        )
    }

    /// Step 3's statement ([`apply_locked_groups`]), over [`Self::staged`].
    /// `$1` is the target (for its truncate floor). `image_columns` are the
    /// target's columns when a reader needs prior images.
    fn apply_sql(&self, image_columns: Option<&[String]>) -> String {
        let tail = match &self.writer {
            Writer::JoinPartials { deltas } => self.deltas_tail_sql(deltas),
            Writer::Aggregate => self.visible_tail_sql(image_columns),
            // A `T` write that staged a group with no hidden row (see
            // `Self::lost_cte_sql`) ends in one more row, which is NULL in
            // its `changed` column, with the count in its `__dn` column.
            Writer::JoinTarget => {
                let mut sentinel = vec![
                    "null::text".to_string(),
                    "null::boolean".to_string(),
                    "null::boolean".to_string(),
                    "null::boolean".to_string(),
                    "null::text".to_string(),
                    "lost.n".to_string(),
                ];
                for arg in &self.shape.args {
                    sentinel.push("null::bigint".to_string());
                    if arg.summed {
                        sentinel.push("null::text".to_string());
                    }
                }
                format!(
                    "{} union all select {} from lost where lost.n > 0",
                    self.visible_tail_sql(image_columns),
                    sentinel.join(", ")
                )
            }
        };
        self.apply_sql_with(&tail)
    }

    /// [`Self::apply_sql`] of `T` in a JOIN build ([`Self::compare_held_sql`]):
    /// adds the staged deltas to the hidden partials and writes no visible
    /// row. Returns how many groups it wrote, and how many staged groups had
    /// no hidden row ([`Self::lost_cte_sql`]).
    fn apply_hidden_sql(&self) -> String {
        self.apply_sql_with("n as (select 1) select count(*), (select n from lost) from up")
    }

    /// The error for `groups` staged groups with no hidden row.
    fn lock_gap(&self, groups: i64) -> ApplyError {
        ApplyError::GroupLockGap {
            target: self.target.clone(),
            groups,
        }
    }

    /// ADR-0019's strict `T` write, as [`Self::apply_sql_with`]'s `lost` CTE:
    /// how many staged groups [`Self::basis_ctes_sql`]'s lookup found no
    /// hidden row for. Every writer of `T` runs [`Self::lock_sql`] first,
    /// which inserts a row for each staged group, so a group missing here is
    /// a gap in the lock set, and the statement would drop its delta. Empty
    /// off a `JOIN`'s `T`: the aggregate path locks the groups it recomputes
    /// too and keeps its statement as it was.
    fn lost_cte_sql(&self) -> &'static str {
        if matches!(self.writer, Writer::JoinTarget) {
            ", lost as (select (select count(*) from pg) - (select count(*) from b) as n)"
        } else {
            ""
        }
    }

    fn apply_sql_with(&self, tail: &str) -> String {
        let cols = quoted(&self.shape.group_cols);
        let args = &self.shape.args;
        // The per-(group, transaction) sums of each partial.
        let mut sums = vec!["sum(__n)::bigint as __n".to_string()];
        let mut totals = vec!["sum(__n)::bigint as __n".to_string()];
        let mut sets = vec!["__n = u.__n + t.__n".to_string()];
        let mut nonzero = vec!["t.__n <> 0".to_string()];
        let mut returning = vec!["t.__n as __dn".to_string(), "u.__n as __nn".to_string()];
        for (i, arg) in args.iter().enumerate() {
            let i = i + 1;
            sums.push(format!("sum(__c{i})::bigint as __c{i}"));
            totals.push(format!("sum(__c{i})::bigint as __c{i}"));
            sets.push(format!("__count_{i} = u.__count_{i} + t.__c{i}"));
            nonzero.push(format!("t.__c{i} <> 0"));
            returning.push(format!("t.__c{i} as __dc{i}, u.__count_{i} as __nc{i}"));
            if arg.summed {
                sums.push(format!("sum(__s{i}) as __s{i}"));
                totals.push(format!("sum(__s{i}) as __s{i}"));
                sets.push(format!("__sum_{i} = u.__sum_{i} + t.__s{i}"));
                nonzero.push(format!("t.__s{i} <> 0"));
                returning.push(format!("t.__s{i} as __ds{i}, u.__sum_{i} as __ns{i}"));
            }
        }
        let up_groups: Vec<String> = cols.iter().map(|c| format!("u.{c}")).collect();
        let matched = self.group_match("sg", "b");
        #[cfg_attr(not(any(test, feature = "test-util")), allow(unused_mut))]
        let mut unseen = "not pg_visible_in_snapshot(sg.__txid, b.__basis)";
        // Planted bug (#1114): order the change against the basis instead of
        // asking whether the basis saw it. See `crate::plant`.
        #[cfg(any(test, feature = "test-util"))]
        if crate::plant::armed() == Some(crate::plant::Plant::GbLsnNotVisibility) {
            unseen = "sg.__txid >= pg_snapshot_xmax(b.__basis)";
        }
        format!(
            "{basis_ctes}{lost}, \
             f as ( \
                 select sg.*, b.__hc from sg join b on {matched} \
                 where b.__basis is null or {unseen} \
             ), \
             t as (select __hc, {totals} from f group by __hc), \
             up as ( \
                 update {hidden} u set {sets} from t \
                 where u.ctid = any(array(select t.__hc from t where {nonzero})) \
                   and u.ctid = t.__hc \
                 returning {up_groups}, {returning} \
             ), \
             {tail}",
            basis_ctes = self.basis_ctes_sql(&sums),
            lost = self.lost_cte_sql(),
            totals = totals.join(", "),
            hidden = self.hidden_ident,
            sets = sets.join(", "),
            nonzero = nonzero.join(" or "),
            up_groups = up_groups.join(", "),
            returning = returning.join(", "),
        )
    }

    /// [`Self::apply_sql_with`]'s first CTEs: `sg`, the staged sides above
    /// the truncate floor (`$1`'s) summed per (group, transaction) into
    /// `sums`; `pg`, their groups; and `b`, each group's hidden row and basis.
    fn basis_ctes_sql(&self, sums: &[String]) -> String {
        let col_list = quoted(&self.shape.group_cols).join(", ");
        format!(
            "with sg as ( \
                 select {col_list}, __txid, {sums} from {staged} \
                 {floor} \
                 group by {col_list}, __txid \
             ), \
             pg as (select distinct {col_list} from sg), \
             b as ( \
                 select pg.*, h.__hc, h.__basis from pg \
                 cross join lateral {lookup_hidden} as h(__hc, __basis) \
             )",
            staged = self.staged,
            sums = sums.join(", "),
            floor = if self.floor_key.is_some() {
                "where __lsn > coalesce( \
                     (select floor from ledger_truncate_floor where target_table = $1), \
                     '0/0'::pg_lsn)"
            } else {
                ""
            },
            lookup_hidden = self.group_lookup(
                &self.hidden_ident,
                "pg",
                if matches!(self.writer, Writer::JoinTarget) {
                    "x.ctid, null::pg_snapshot"
                } else {
                    "x.ctid, x.__basis"
                }
            ),
        )
    }

    /// Planted bug `gb_lsn_not_visibility` (`crate::plant`): how many of
    /// the staged (group, transaction) sums the plant refuses that the
    /// visibility test would apply: those whose transaction was in flight
    /// when the group's basis was taken.
    #[cfg(any(test, feature = "test-util"))]
    fn planted_in_flight_sql(&self) -> String {
        format!(
            "{}, \
             x as (select 1 from sg join b on {} \
                   where b.__basis is not null \
                     and not pg_visible_in_snapshot(sg.__txid, b.__basis) \
                     and sg.__txid < pg_snapshot_xmax(b.__basis)) \
             select count(*) from x",
            self.basis_ctes_sql(&["sum(__n)::bigint as __n".to_string()]),
            self.group_match("sg", "b"),
        )
    }

    /// The end of [`Self::apply_sql`] for [`Writer::JoinPartials`], after its
    /// `up` CTE: adds each written cell's change to `deltas` and returns how
    /// many cells it wrote.
    fn deltas_tail_sql(&self, deltas: &str) -> String {
        let mut columns = quoted(&self.shape.group_cols);
        columns.push("__dn".to_string());
        for (i, arg) in self.shape.args.iter().enumerate() {
            columns.push(format!("__dc{}", i + 1));
            if arg.summed {
                columns.push(format!("__ds{}", i + 1));
            }
        }
        let columns = columns.join(", ");
        format!(
            "d as (insert into {deltas} ({columns}) select {columns} from up) \
             select count(*) from up"
        )
    }

    /// The source rows of the group `rg` (a lateral reference), read through
    /// the root: an index probe of the leaf its key routes to. A group with a
    /// `NULL` part matches each column by `=`, or by `is null` on both sides,
    /// which prunes to the leaves that can hold it and can use an index;
    /// `is not distinct from` does neither, and would scan every leaf of the
    /// source once per such group.
    fn probe_rows_sql(&self) -> String {
        let cols = quoted(&self.shape.group_cols);
        let equal: Vec<String> = cols.iter().map(|c| format!("s.{c} = rg.{c}")).collect();
        let has_null: Vec<String> = cols.iter().map(|c| format!("rg.{c} is null")).collect();
        let same: Vec<String> = cols
            .iter()
            .map(|c| format!("(s.{c} = rg.{c} or (rg.{c} is null and s.{c} is null))"))
            .collect();
        format!(
            "select s.* from {src} s where {equal} \
             union all \
             select s.* from {src} s where ({has_null}) and {same}",
            src = self.source_row,
            equal = equal.join(" and "),
            has_null = has_null.join(" or "),
            same = same.join(" and "),
        )
    }

    /// The recompute's statement ([`recompute_locked_groups`], R1, R5): over
    /// the groups of [`Self::recomputed`], already locked, takes the
    /// snapshot and reads the newest segment's `seg_seq` in this one
    /// statement, `GROUP BY`s each group's source rows into its partials,
    /// and writes them with the snapshot as the group's basis and the
    /// segment as its stamp. A group with no source row keeps its hidden
    /// row at zero with the basis (R4). `image_columns` as
    /// [`Self::apply_sql`]'s.
    ///
    /// `node` (quoted and qualified) reads the groups' rows from one
    /// partition of the source instead of through the root, in one
    /// `GROUP BY` of the whole node rather than an index probe per group: a
    /// partition correction's ([`recompute_routed`]), whose caller has
    /// established that the node holds every row of every group in the set
    /// and no row of a group outside it.
    fn recompute_sql(&self, image_columns: Option<&[String]>, node: Option<&str>) -> String {
        let cols = quoted(&self.shape.group_cols);
        let col_list = cols.join(", ");
        let args = &self.shape.args;
        let mut old = vec!["x.__n".to_string()];
        let mut old_names = vec!["__on".to_string()];
        let mut fresh = vec!["count(*)::bigint as __rn".to_string()];
        // The scan form's partials, `NULL` for a group the node has no row of.
        let mut zeroed = vec!["coalesce(n.__rn, 0) as __rn".to_string()];
        let mut sets = vec!["__n = r.__rn".to_string()];
        let mut returning = vec![
            "u.__n - r.__on as __dn".to_string(),
            "u.__n as __nn".to_string(),
        ];
        for (i, arg) in args.iter().enumerate() {
            let i = i + 1;
            old.push(format!("x.__count_{i}"));
            old_names.push(format!("__oc{i}"));
            fresh.push(format!(
                "(count(*) filter (where ({}) is not null))::bigint as __rc{i}",
                arg.sql
            ));
            sets.push(format!("__count_{i} = r.__rc{i}"));
            zeroed.push(format!("coalesce(n.__rc{i}, 0) as __rc{i}"));
            returning.push(format!(
                "u.__count_{i} - r.__oc{i} as __dc{i}, u.__count_{i} as __nc{i}"
            ));
            if arg.summed {
                old.push(format!("x.__sum_{i}"));
                old_names.push(format!("__os{i}"));
                fresh.push(format!(
                    "coalesce(sum(({})::numeric), 0) as __rs{i}",
                    arg.sql
                ));
                sets.push(format!("__sum_{i} = r.__rs{i}"));
                zeroed.push(format!("coalesce(n.__rs{i}, 0) as __rs{i}"));
                returning.push(format!(
                    "u.__sum_{i} - r.__os{i} as __ds{i}, u.__sum_{i} as __ns{i}"
                ));
            }
        }
        let up_groups: Vec<String> = cols.iter().map(|c| format!("u.{c}")).collect();
        let lookup_hidden = self.group_lookup(
            &self.hidden_ident,
            "rg",
            &format!("x.ctid, {}", old.join(", ")),
        );
        let r = match node {
            None => format!(
                "r as ( \
                     select h.*, a.* from rg \
                     cross join lateral {lookup_hidden} as h(__hc, {old_names}) \
                     cross join lateral (select {fresh} from ({probe}) as s) as a \
                 )",
                old_names = old_names.join(", "),
                fresh = fresh.join(", "),
                probe = self.probe_rows_sql(),
            ),
            Some(node) => {
                // The node's group columns at the hidden table's types, as
                // `rg`'s: array equality needs both sides' element types
                // the same (a `varchar` source column against a `text`
                // group column has no `=`).
                let matched = self.group_match("n", "rg");
                let positions: Vec<String> = (1..=cols.len()).map(|p| p.to_string()).collect();
                format!(
                    "n as materialized ( \
                         select {values}, {fresh} from {node} s group by {positions} \
                     ), \
                     r as ( \
                         select h.*, {zeroed} from rg \
                         cross join lateral {lookup_hidden} as h(__hc, {old_names}) \
                         left join n on {matched} \
                     )",
                    values = self.group_values().join(", "),
                    fresh = fresh.join(", "),
                    positions = positions.join(", "),
                    zeroed = zeroed.join(", "),
                    old_names = old_names.join(", "),
                )
            }
        };
        format!(
            "with snap as materialized ( \
                 select pg_catalog.pg_current_snapshot() as __snap, \
                        coalesce((select max(seg_seq) from segments), 0) as __seg \
             ), \
             rg as (select distinct {col_list} from {recomputed}), \
             {r}, \
             up as ( \
                 update {hidden} u set {sets}, __basis = snap.__snap, __stamp = snap.__seg \
                 from r, snap \
                 where u.ctid = any(array(select r.__hc from r)) and u.ctid = r.__hc \
                 returning {up_groups}, {returning} \
             ), \
             {tail}",
            recomputed = self.recomputed,
            hidden = self.hidden_ident,
            sets = sets.join(", "),
            up_groups = up_groups.join(", "),
            returning = returning.join(", "),
            tail = match &self.writer {
                Writer::JoinPartials { deltas } => self.deltas_tail_sql(deltas),
                Writer::Aggregate | Writer::JoinTarget => self.visible_tail_sql(image_columns),
            },
        )
    }

    /// A `JOIN` build's snapshot statement for one side ([`write_join_build_cells`],
    /// #1097's rule 4): after every lock statement of the build, one `GROUP
    /// BY` of `node` (the side's table or leaf, quoted and qualified) into
    /// `grouped` (a session temp table, quoted and `pg_temp.`-qualified,
    /// dropped at commit), with one more row (`__meta`) holding the
    /// statement's snapshot and the newest segment's `seg_seq`. A `create …
    /// as select` reads in parallel where Postgres can; the data-modifying
    /// write that follows can't.
    fn join_group_sql(&self, node: &str, grouped: &str) -> String {
        let cols = quoted(&self.shape.group_cols);
        let mut fresh = vec!["count(*)::bigint as __rn".to_string()];
        let mut nulls = vec!["null".to_string(); cols.len() + 1];
        for (i, arg) in self.shape.args.iter().enumerate() {
            let i = i + 1;
            fresh.push(format!(
                "(count(*) filter (where ({}) is not null))::bigint as __rc{i}",
                arg.sql
            ));
            nulls.push("null".to_string());
            if arg.summed {
                fresh.push(format!(
                    "coalesce(sum(({})::numeric), 0) as __rs{i}",
                    arg.sql
                ));
                nulls.push("null".to_string());
            }
        }
        let positions: Vec<String> = (4..4 + cols.len()).map(|p| p.to_string()).collect();
        format!(
            "create temp table {table} on commit drop as \
             select false as __meta, null::pg_snapshot as __snap, null::bigint as __seg, \
                    {values}, {fresh} \
             from {node} s group by {positions} \
             union all \
             select true, pg_catalog.pg_current_snapshot(), \
                    coalesce((select max(seg_seq) from segments), 0), {nulls}",
            table = grouped.trim_start_matches("pg_temp."),
            values = self.group_values().join(", "),
            fresh = fresh.join(", "),
            positions = positions.join(", "),
            nulls = nulls.join(", "),
        )
    }

    /// A `JOIN` build's write of one side's cells ([`write_join_build_cells`],
    /// #1097's rules 5 and 6), after [`Self::join_group_sql`] filled
    /// `grouped`: over the cells of [`Self::recomputed`], whose keys the
    /// build holds under `J` and whose `NULL`-key cells it has locked, writes
    /// each cell's partials once from `grouped`: an update of a cell the
    /// hidden table holds, an insert of one it doesn't (zero for a cell the
    /// `GROUP BY` doesn't have, R4), with `grouped`'s snapshot as its basis
    /// and its segment as its stamp. Each cell's change (new partials less the
    /// held ones, which the locks keep as they were at the snapshot) goes to
    /// `deltas`. A cell the snapshot sees in the table but the enumeration
    /// didn't name is left to its pages.
    ///
    /// Two statements: the first fills `cells` (a temp table, quoted and
    /// `pg_temp.`-qualified) with each cell, the `ctid` of its hidden row
    /// (`NULL` if none), its held partials and its new ones; the caller
    /// analyzes it; the second writes from it. The cells are matched to the
    /// hidden rows and to the `GROUP BY` by hash joins on
    /// [`Self::group_match`], not by a lookup per cell: a build's cells are
    /// every cell of the side. Postgres has no statistics for that match's
    /// arrays, so it guesses its rows at orders of magnitude too many; the
    /// write is planned from `cells`' own statistics instead. Planned from
    /// the guess, its anti-join's hash table split into as many batches as
    /// the guessed rows called for, and a build over a hidden table that
    /// already holds its cells (a rebuild, a resume) ran for minutes, or
    /// hours at 100M rows, on the batches alone (#1346).
    ///
    /// A keyed cell is held by `J`, not by a row lock, so the zero-cell
    /// collection (`super::retire`), which takes no `J` lock, may delete one
    /// the first statement read: the update then finds no row, and the cell
    /// is inserted instead. The caller holds a snapshot from before the
    /// first statement until the second is done, so that `ctid` still holds
    /// the dead row, never another key's cell in a freed line pointer.
    /// Its held partials were zero, as the collection
    /// deletes only zero cells, so its change is the same either way. The
    /// cells to insert are those the update didn't write, an anti-join of the
    /// cells against the update's rows (a cell with no hidden row has a
    /// `NULL` `__hc`, which matches none). It is the bare `not exists`: an
    /// `or` beside it keeps it a subplan, which rescans the update's rows
    /// once per cell when they outgrow memory.
    fn join_build_sql(&self, grouped: &str, cells: &str, deltas: &str) -> [String; 2] {
        let cols = quoted(&self.shape.group_cols);
        let col_list = cols.join(", ");
        let args = &self.shape.args;
        let mut held = vec!["x.__n as __on".to_string()];
        let mut cell = vec![
            "coalesce(h.__on, 0) as __on".to_string(),
            "coalesce(n.__rn, 0) as __rn".to_string(),
        ];
        let mut sets = vec!["__n = r.__rn".to_string()];
        let mut insert_cols = cols.clone();
        insert_cols.push("__n".to_string());
        let mut insert_values: Vec<String> = cols.iter().map(|c| format!("r.{c}")).collect();
        insert_values.push("r.__rn".to_string());
        let mut delta_cols = cols.clone();
        delta_cols.push("__dn".to_string());
        let mut delta_values: Vec<String> = cols.iter().map(|c| format!("r.{c}")).collect();
        delta_values.push("r.__rn - r.__on".to_string());
        for (i, arg) in args.iter().enumerate() {
            let i = i + 1;
            held.push(format!("x.__count_{i} as __oc{i}"));
            cell.push(format!("coalesce(h.__oc{i}, 0) as __oc{i}"));
            cell.push(format!("coalesce(n.__rc{i}, 0) as __rc{i}"));
            sets.push(format!("__count_{i} = r.__rc{i}"));
            insert_cols.push(format!("__count_{i}"));
            insert_values.push(format!("r.__rc{i}"));
            delta_cols.push(format!("__dc{i}"));
            delta_values.push(format!("r.__rc{i} - r.__oc{i}"));
            if arg.summed {
                held.push(format!("x.__sum_{i} as __os{i}"));
                cell.push(format!("coalesce(h.__os{i}, 0) as __os{i}"));
                cell.push(format!("coalesce(n.__rs{i}, 0) as __rs{i}"));
                sets.push(format!("__sum_{i} = r.__rs{i}"));
                insert_cols.push(format!("__sum_{i}"));
                insert_values.push(format!("r.__rs{i}"));
                delta_cols.push(format!("__ds{i}"));
                delta_values.push(format!("r.__rs{i} - r.__os{i}"));
            }
        }
        insert_cols.extend(["__basis", "__stamp"].map(String::from));
        insert_values.extend(["snap.__snap", "snap.__seg"].map(String::from));
        let rg_cols: Vec<String> = cols.iter().map(|c| format!("rg.{c}")).collect();
        let x_cols: Vec<String> = cols.iter().map(|c| format!("x.{c}")).collect();
        let fill = format!(
            "create temp table {table} on commit drop as \
             select {rg_cols}, h.__hc, {cell} \
             from (select distinct {col_list} from {recomputed}) rg \
             left join (select x.ctid as __hc, {x_cols}, {held} from {hidden} x) h \
                 on {h_match} \
             left join (select * from {grouped} where not __meta) n on {n_match}",
            table = cells.trim_start_matches("pg_temp."),
            recomputed = self.recomputed,
            hidden = self.hidden_ident,
            x_cols = x_cols.join(", "),
            held = held.join(", "),
            rg_cols = rg_cols.join(", "),
            cell = cell.join(", "),
            h_match = self.group_match("h", "rg"),
            n_match = self.group_match("n", "rg"),
        );
        let write = format!(
            "with snap as materialized (select __snap, __seg from {grouped} where __meta), \
             up as ( \
                 update {hidden} u set {sets}, __basis = snap.__snap, __stamp = snap.__seg \
                 from {cells} r, snap where u.ctid = r.__hc \
                 returning r.__hc \
             ), \
             ins as ( \
                 insert into {hidden} ({insert_cols}) \
                 select {insert_values} from {cells} r, snap \
                 where not exists (select from up where up.__hc = r.__hc) \
             ), \
             d as (insert into {deltas} ({delta_cols}) select {delta_values} from {cells} r) \
             select count(*) from {cells}",
            hidden = self.hidden_ident,
            sets = sets.join(", "),
            insert_cols = insert_cols.join(", "),
            insert_values = insert_values.join(", "),
            delta_cols = delta_cols.join(", "),
            delta_values = delta_values.join(", "),
        );
        [fill, write]
    }

    /// A rebuild's enumeration ([`rebuild`], R8): adds to
    /// [`Self::recomputed`] every group of the target's own (its hidden
    /// rows, zero groups included, and its visible rows), every group of the
    /// source, and the group of every old image in the ring for the source
    /// (`$1`): an old image or a statement's transition `old` row. The ring
    /// read covers every undrained change, and may name the groups of
    /// drained changes whose segment is not yet retired, which costs only
    /// their recompute.
    ///
    /// With a `scope` (one structure of a partitioned source's rebuild) it
    /// adds only the groups the scope's predicates route to the structure,
    /// and, for a leaf, the groups of the leaf's own rows instead of the
    /// source's.
    fn rebuild_groups_sql(&self, scope: Option<&RebuildScope>) -> String {
        format!(
            "insert into {} ({}) {}",
            self.recomputed,
            quoted(&self.shape.group_cols).join(", "),
            self.rebuild_groups_select(scope, "$1")
        )
    }

    /// The `select` of [`Self::rebuild_groups_sql`]: `source` is the SQL of
    /// the ring's name for the source (`src_table`).
    fn rebuild_groups_select(&self, scope: Option<&RebuildScope>, source: &str) -> String {
        let cols = quoted(&self.shape.group_cols).join(", ");
        let (hidden_filter, target_filter, image_filter) = match scope {
            Some(scope) => (
                format!(" where {}", scope.hidden),
                format!(" where {}", scope.target),
                format!(" where {}", scope.images),
            ),
            None => Default::default(),
        };
        let images: Vec<String> = self
            .ring
            .iter()
            .flat_map(|seg| {
                [
                    format!(
                        "select old_image as __i from {seg} \
                         where src_table = {source} and jsonb_typeof(old_image) = 'object'"
                    ),
                    format!(
                        "select transition_images -> 'old' from {seg} \
                         where src_table = {source} \
                           and jsonb_typeof(transition_images -> 'old') = 'object'"
                    ),
                ]
            })
            .collect();
        // The stage filter (` and …`, a `JOIN`'s joined side's: a row with a
        // `NULL` key part is no `M` cell, as on a page) added to `filter` (a
        // ` where …` or nothing). Empty for every other writer, so the
        // statement is unchanged for them.
        let staged = |filter: &str| -> String {
            match (filter.is_empty(), self.stage_filter.is_empty()) {
                (_, true) => filter.to_string(),
                (true, false) => format!(" where true{}", self.stage_filter),
                (false, false) => format!("{filter}{}", self.stage_filter),
            }
        };
        // A leaf's own rows; the whole source for an unpartitioned rebuild;
        // none for the remainder, whose groups have no row of their own.
        let own = match scope {
            None => format!(
                " union all select distinct {values} from {src} s{filter}",
                values = self.group_values().join(", "),
                src = self.source_row,
                filter = staged(""),
            ),
            Some(RebuildScope {
                node: Some(node), ..
            }) => format!(
                " union all select distinct {values} from {node} s{filter}",
                values = self.group_values().join(", "),
                filter = staged(""),
            ),
            Some(_) => String::new(),
        };
        let image_filter = staged(&image_filter);
        // A `JOIN` side's cells have no visible target of their own.
        let target_branch = match self.writer {
            Writer::JoinPartials { .. } => String::new(),
            _ => format!(
                " union all select {cols} from {} t{target_filter}",
                self.target_ident
            ),
        };
        format!(
            "select {cols} from {hidden} h{hidden_filter}{target_branch}{own} \
             union all select {values} from ({images}) as r \
                 cross join lateral jsonb_populate_record(null::{src}, r.__i) as s{image_filter}",
            hidden = self.hidden_ident,
            values = self.group_values().join(", "),
            src = self.source_row,
            images = images.join(" union all "),
        )
    }

    /// A rebuild's write ([`rebuild`], R1, R5): over the groups of
    /// [`Self::recomputed`], already locked, takes the snapshot and reads
    /// the newest segment's `seg_seq` in this one statement, runs the
    /// `GROUP BY` of the whole source, and replaces each group's partials
    /// with its own (zero for a group the `GROUP BY` doesn't have, R4), with
    /// the snapshot as its basis and the segment as its stamp. A group the
    /// snapshot sees in the source but that wasn't locked (one whose first
    /// change committed after the enumeration) is left to its pages.
    ///
    /// The visible row is replaced too: compared with the row the target
    /// holds, not with what the old partials say, so a rebuild repairs a
    /// visible row that disagrees with them. It is upserted where its new
    /// values differ, deleted where the group's count is 0, and each prior
    /// image is the row the target held. Returns rows in
    /// [`Self::visible_tail_sql`]'s layout, with each partial's new value in
    /// place of its change.
    fn rebuild_sql(&self, image_columns: Option<&[String]>, read: &RebuildRead) -> String {
        let cols = quoted(&self.shape.group_cols);
        let col_list = cols.join(", ");
        let args = &self.shape.args;
        let mut fresh = vec!["count(*)::bigint as __rn".to_string()];
        let mut kept = vec!["coalesce(a.__rn, 0) as __rn".to_string()];
        let mut sets = vec!["__n = r.__rn".to_string()];
        let mut returning = vec!["u.__n as __nn".to_string()];
        for (i, arg) in args.iter().enumerate() {
            let i = i + 1;
            fresh.push(format!(
                "(count(*) filter (where ({}) is not null))::bigint as __rc{i}",
                arg.sql
            ));
            kept.push(format!("coalesce(a.__rc{i}, 0) as __rc{i}"));
            sets.push(format!("__count_{i} = r.__rc{i}"));
            returning.push(format!("u.__count_{i} as __nc{i}"));
            if arg.summed {
                fresh.push(format!(
                    "coalesce(sum(({})::numeric), 0) as __rs{i}",
                    arg.sql
                ));
                kept.push(format!("coalesce(a.__rs{i}, 0) as __rs{i}"));
                sets.push(format!("__sum_{i} = r.__rs{i}"));
                returning.push(format!("u.__sum_{i} as __ns{i}"));
            }
        }
        let positions: Vec<String> = (1..=cols.len()).map(|p| p.to_string()).collect();
        let matched = self.group_match("a", "rg");
        let up_groups: Vec<String> = cols.iter().map(|c| format!("u.{c}")).collect();
        let lookup_hidden = self.group_lookup(&self.hidden_ident, "rg", "x.ctid");
        let r = match read {
            #[cfg(any(test, feature = "test-util"))]
            RebuildRead::Early(early) => format!(
                "a as (select * from {early} where __rn is not null), \
                 r as ( \
                     select h.__hc, {kept} from rg \
                     cross join lateral {lookup_hidden} as h(__hc) \
                     left join a on {matched} \
                 )",
                kept = kept.join(", "),
            ),
            // One `GROUP BY` of the source or of a leaf, matched to the
            // locked groups.
            RebuildRead::Source | RebuildRead::Node(_) => {
                let from = match read {
                    RebuildRead::Node(node) => node.as_str(),
                    _ => self.source_row.as_str(),
                };
                format!(
                    "a as ( \
                         select {values}, {fresh} from {from} s group by {positions} \
                     ), \
                     r as ( \
                         select h.__hc, {kept} from rg \
                         cross join lateral {lookup_hidden} as h(__hc) \
                         left join a on {matched} \
                     )",
                    values = self.group_values().join(", "),
                    fresh = fresh.join(", "),
                    positions = positions.join(", "),
                    kept = kept.join(", "),
                )
            }
            // One index probe of the root per group, for a set that is not
            // a leaf's (the remainder's) or a leaf of a tree that is not as
            // recorded.
            RebuildRead::Probe => format!(
                "r as ( \
                     select h.__hc, {kept} from rg \
                     cross join lateral {lookup_hidden} as h(__hc) \
                     cross join lateral (select {fresh} from ({probe}) as s) as a \
                 )",
                kept = kept.join(", "),
                fresh = fresh.join(", "),
                probe = self.probe_rows_sql(),
            ),
        };
        #[cfg_attr(not(any(test, feature = "test-util")), allow(unused_mut))]
        let mut snap = SNAPSHOT_AND_STAMP.to_string();
        #[cfg(any(test, feature = "test-util"))]
        if let RebuildRead::Early(early) = read {
            snap = format!("select __snap, __seg from {early} limit 1");
        }
        format!(
            "with snap as materialized ({snap}), \
             rg as (select distinct {col_list} from {recomputed}), \
             {r}, \
             up as ( \
                 update {hidden} u set {sets}, __basis = snap.__snap, __stamp = snap.__seg \
                 from r, snap where u.ctid = r.__hc \
                 returning {up_groups}, {returning} \
             ), \
             {tail}",
            recomputed = self.recomputed,
            r = r,
            hidden = self.hidden_ident,
            sets = sets.join(", "),
            up_groups = up_groups.join(", "),
            returning = returning.join(", "),
            tail = self.held_tail_sql(image_columns),
        )
    }

    /// Planted bug `gb_build_snapshot_before_locks` (`crate::plant`):
    /// creates `early`, a temporary table holding [`Self::rebuild_sql`]'s
    /// `GROUP BY` of the whole source and its snapshot and stamp, all from
    /// this one statement's snapshot (one row of `NULL`s past the snapshot
    /// when the source is empty).
    #[cfg(any(test, feature = "test-util"))]
    fn planted_early_read_sql(&self, early: &str) -> String {
        let cols = quoted(&self.shape.group_cols);
        let mut fresh = vec!["count(*)::bigint as __rn".to_string()];
        for (i, arg) in self.shape.args.iter().enumerate() {
            let i = i + 1;
            fresh.push(format!(
                "(count(*) filter (where ({}) is not null))::bigint as __rc{i}",
                arg.sql
            ));
            if arg.summed {
                fresh.push(format!(
                    "coalesce(sum(({})::numeric), 0) as __rs{i}",
                    arg.sql
                ));
            }
        }
        let positions: Vec<String> = (1..=cols.len()).map(|p| p.to_string()).collect();
        format!(
            "create temporary table {early} on commit drop as \
             with snap as materialized ({SNAPSHOT_AND_STAMP}) \
             select snap.*, a.* from snap left join ( \
                 select {values}, {fresh} from {src} s group by {positions} \
             ) as a on true",
            values = self.group_values().join(", "),
            fresh = fresh.join(", "),
            positions = positions.join(", "),
            src = self.source_row,
        )
    }

    /// The end of [`Self::rebuild_sql`] and [`Self::compare_held_sql`], after
    /// their `up` CTE, which returns each group's columns and its partials'
    /// values (`__nn`, `__nc<i>`, `__ns<i>`): the visible row each group's
    /// partials give is compared with the row the target **holds**, upserted
    /// where its values differ, deleted where the count is 0, and each prior
    /// image is the held row. Returns rows in [`Self::visible_tail_sql`]'s
    /// layout, with each partial's value in place of its change.
    fn held_tail_sql(&self, image_columns: Option<&[String]>) -> String {
        let cols = quoted(&self.shape.group_cols);
        let col_list = cols.join(", ");
        let args = &self.shape.args;
        let mut applied = vec!["cur.__nn".to_string()];
        for (i, arg) in args.iter().enumerate() {
            let i = i + 1;
            applied.push(format!("cur.__nc{i}"));
            if arg.summed {
                applied.push(format!("cur.__ns{i}::text"));
            }
        }
        let field_count = self.shape.fields.len();
        let field_names: Vec<String> = self
            .shape
            .fields
            .iter()
            .map(|(name, _, _)| quote_ident(name))
            .collect();
        let new_fields = self.fields_lateral("__nf", "up.__nn", &|i| format!("up.__nc{i}"), &|i| {
            format!("up.__ns{i}")
        });
        let nf: Vec<String> = (0..field_count).map(|j| format!("nv.__nf{j}")).collect();
        let held: Vec<String> = (0..field_count).map(|j| format!("t.__vf{j}")).collect();
        let held_names: Vec<String> = (0..field_count).map(|j| format!("__vf{j}")).collect();
        let mut lookup_columns = vec!["x.ctid".to_string()];
        lookup_columns.extend(field_names.iter().map(|f| format!("x.{f}")));
        let mut lookup_names = vec!["__tc".to_string()];
        lookup_names.extend(held_names.iter().cloned());
        let same_values = if field_count == 0 {
            "true".to_string()
        } else {
            format!(
                "({}) is not distinct from ({})",
                nf.join(", "),
                held.join(", ")
            )
        };
        let field_updates: Vec<String> = field_names
            .iter()
            .map(|f| format!("{f} = excluded.{f}"))
            .collect();
        let upsert_action = if field_updates.is_empty() {
            "do nothing".to_string()
        } else {
            format!("do update set {}", field_updates.join(", "))
        };
        let prior_image = match image_columns {
            Some(columns) => {
                let pairs: Vec<String> = columns
                    .iter()
                    .map(|column| {
                        let value = if self.shape.group_cols.contains(column) {
                            format!("cur.{}", quote_ident(column))
                        } else if let Some(j) =
                            self.shape.fields.iter().position(|(n, _, _)| n == column)
                        {
                            format!("cur.__vf{j}")
                        } else {
                            "null".to_string()
                        };
                        format!("{}, ({value})::text", quote_literal(column))
                    })
                    .collect();
                format!(
                    "case when cur.__tc is not null then jsonb_build_object({})::text end",
                    pairs.join(", ")
                )
            }
            None => "null::text".to_string(),
        };
        format!(
            "nv as (select up.*, {nf_all} from up cross join {new_fields}), \
             cur as ( \
                 select nv.*, t.* , \
                        (nv.__nn > 0) is distinct from (t.__tc is not null) \
                            or (nv.__nn > 0 and not ({same_values})) as __changed \
                 from nv left join lateral {lookup_target} as t({lookup_names}) on true \
             ), \
             written as ( \
                 insert into {target} ({col_list}{field_list}) \
                 select {col_list}{nf_list} from cur where __changed and __nn > 0 \
                 order by {col_list} \
                 on conflict ({col_list}) {upsert_action} \
             ), \
             deleted as ( \
                 delete from {target} where ctid = any(array( \
                     select __tc from cur where __changed and __nn <= 0 and __tc is not null)) \
             ) \
             select {key}, cur.__changed, cur.__nn > 0, cur.__tc is not null, {prior_image}, \
                    {applied} \
             from cur",
            nf_all = if field_count == 0 {
                "true as __trellis_none".to_string()
            } else {
                (0..field_count)
                    .map(|j| format!("__nf.__nf{j}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            },
            lookup_target = self.group_lookup(&self.target_ident, "nv", &lookup_columns.join(", ")),
            lookup_names = lookup_names.join(", "),
            target = self.target_ident,
            field_list = field_names
                .iter()
                .map(|f| format!(", {f}"))
                .collect::<String>(),
            nf_list = (0..field_count)
                .map(|j| format!(", __nf{j}"))
                .collect::<String>(),
            key = ddl::pk_key_sql_expr(&self.identity, Some("cur")),
            applied = applied.join(", "),
        )
    }

    /// A JOIN build's visible compare for `T` ([`apply_join_build_groups`]),
    /// over every group of the staging table (the product's groups and
    /// [`Self::target_groups_sql`]'s zero rows), already locked and with the
    /// staged deltas applied to the hidden partials: each group's visible
    /// row, computed from the partials it now holds, is compared with the row
    /// the target holds ([`Self::held_tail_sql`]). That repairs a visible row
    /// that disagrees with the partials, and removes one whose group has no
    /// partial left, which a delta alone never reaches.
    fn compare_held_sql(&self, image_columns: Option<&[String]>) -> String {
        let cols = quoted(&self.shape.group_cols);
        let col_list = cols.join(", ");
        let mut names = vec!["__nn".to_string()];
        let mut columns = vec!["x.__n".to_string()];
        for (i, arg) in self.shape.args.iter().enumerate() {
            let i = i + 1;
            names.push(format!("__nc{i}"));
            columns.push(format!("x.__count_{i}"));
            if arg.summed {
                names.push(format!("__ns{i}"));
                columns.push(format!("x.__sum_{i}"));
            }
        }
        let up_groups: Vec<String> = cols.iter().map(|c| format!("rg.{c}")).collect();
        let up_values: Vec<String> = names.iter().map(|n| format!("h.{n}")).collect();
        format!(
            "with rg as (select distinct {col_list} from {staged}), \
             up as ( \
                 select {up_groups}, {up_values} from rg \
                 cross join lateral {lookup_hidden} as h({names}) \
             ), \
             {tail}",
            staged = self.staged,
            up_groups = up_groups.join(", "),
            up_values = up_values.join(", "),
            lookup_hidden = self.group_lookup(&self.hidden_ident, "rg", &columns.join(", ")),
            names = names.join(", "),
            tail = self.held_tail_sql(image_columns),
        )
    }

    /// A JOIN build's enumeration of `T`'s groups (ADR-0002's R8, one level
    /// up): a zero row in `T`'s staging table for every group the visible
    /// target holds and every group `T`'s hidden table holds, so the lock of
    /// [`Self::lock_sql`] covers them with the product's groups, and the
    /// compare reaches a visible row whose group has no source row left. A
    /// zero row changes no partial.
    fn target_groups_sql(&self) -> String {
        let cols = quoted(&self.shape.group_cols).join(", ");
        let mut insert = vec!["__txid", "__lsn", "__n"]
            .into_iter()
            .map(String::from)
            .collect::<Vec<_>>();
        let mut zeros = vec![
            "pg_current_xact_id()".to_string(),
            "'0/0'::pg_lsn".to_string(),
            "0".to_string(),
        ];
        for (i, arg) in self.shape.args.iter().enumerate() {
            let i = i + 1;
            insert.push(format!("__c{i}"));
            zeros.push("0".to_string());
            if arg.summed {
                insert.push(format!("__s{i}"));
                zeros.push("0".to_string());
            }
        }
        format!(
            "insert into {staged} ({cols}, {insert}) \
             select {cols}, {zeros} from ( \
                 select {cols} from {hidden} union select {cols} from {target} \
             ) as g",
            staged = self.staged,
            insert = insert.join(", "),
            zeros = zeros.join(", "),
            hidden = self.hidden_ident,
            target = self.target_ident,
        )
    }

    /// The end of [`Self::apply_sql`] and [`Self::recompute_sql`], after
    /// their `up` CTE, which returns each written group's columns, and per
    /// partial its change (`__dn`, `__dc<i>`, `__ds<i>`) and its new value
    /// (`__nn`, `__nc<i>`, `__ns<i>`): writes the visible row when a visible
    /// value changed (an upsert while `__n > 0`, a delete when it falls to
    /// 0), and returns per group its key, whether its visible row changed,
    /// whether it is visible now and was before, its prior visible image,
    /// and its change per partial.
    fn visible_tail_sql(&self, image_columns: Option<&[String]>) -> String {
        let cols = quoted(&self.shape.group_cols);
        let col_list = cols.join(", ");
        let args = &self.shape.args;
        let new_fields = self.fields_lateral("__nf", "up.__nn", &|i| format!("up.__nc{i}"), &|i| {
            format!("up.__ns{i}")
        });
        let prior_fields = self.fields_lateral(
            "__pf",
            "(up.__nn - up.__dn)",
            &|i| format!("(up.__nc{i} - up.__dc{i})"),
            &|i| format!("(up.__ns{i} - up.__ds{i})"),
        );
        let field_count = self.shape.fields.len();
        let nf: Vec<String> = (0..field_count).map(|j| format!("__nf.__nf{j}")).collect();
        let pf: Vec<String> = (0..field_count).map(|j| format!("__pf.__pf{j}")).collect();
        let field_names: Vec<String> = self
            .shape
            .fields
            .iter()
            .map(|(name, _, _)| quote_ident(name))
            .collect();
        let vis_new: Vec<String> = (0..field_count).map(|j| format!("__nf{j}")).collect();
        let field_updates: Vec<String> = field_names
            .iter()
            .map(|f| format!("{f} = excluded.{f}"))
            .collect();
        let upsert_action = if field_updates.is_empty() {
            "do nothing".to_string()
        } else {
            format!("do update set {}", field_updates.join(", "))
        };
        // The prior visible row's image, for the seam, from the partials
        // before this statement's change.
        let prior_image = match image_columns {
            Some(columns) => {
                let pairs: Vec<String> = columns
                    .iter()
                    .map(|column| {
                        let value = if self.shape.group_cols.contains(column) {
                            format!("vis.{}", quote_ident(column))
                        } else if let Some(j) =
                            self.shape.fields.iter().position(|(n, _, _)| n == column)
                        {
                            format!("vis.__pf{j}")
                        } else {
                            "null".to_string()
                        };
                        format!("{}, ({value})::text", quote_literal(column))
                    })
                    .collect();
                format!(
                    "case when vis.__nn - vis.__dn > 0 then jsonb_build_object({})::text end",
                    pairs.join(", ")
                )
            }
            None => "null::text".to_string(),
        };
        let mut applied: Vec<String> = vec!["vis.__dn".to_string()];
        for (i, arg) in args.iter().enumerate() {
            applied.push(format!("vis.__dc{}", i + 1));
            if arg.summed {
                applied.push(format!("vis.__ds{}::text", i + 1));
            }
        }
        format!(
            "vis as ( \
                 select up.*, {nf_pf}, \
                        (up.__nn > 0, {nf_cols}) is distinct from \
                            (up.__nn - up.__dn > 0, {pf_cols}) as __changed \
                 from up cross join {new_fields} cross join {prior_fields} \
             ), \
             written as ( \
                 insert into {target} ({col_list}{field_list}) \
                 select {col_list}{vis_new} from vis where __changed and __nn > 0 \
                 order by {col_list} \
                 on conflict ({col_list}) {upsert_action} \
             ), \
             deleted as ( \
                 delete from {target} where ctid = any(array( \
                     select h.__c from vis cross join lateral {lookup_target} as h(__c) \
                     where vis.__changed and vis.__nn <= 0 and vis.__nn - vis.__dn > 0)) \
             ) \
             select {key}, vis.__changed, vis.__nn > 0, vis.__nn - vis.__dn > 0, {prior_image}, \
                    {applied} \
             from vis",
            nf_pf = if field_count == 0 {
                "true as __trellis_none".to_string()
            } else {
                nf.iter().chain(&pf).cloned().collect::<Vec<_>>().join(", ")
            },
            nf_cols = if nf.is_empty() {
                "true".to_string()
            } else {
                nf.join(", ")
            },
            pf_cols = if pf.is_empty() {
                "true".to_string()
            } else {
                pf.join(", ")
            },
            target = self.target_ident,
            field_list = field_names
                .iter()
                .map(|f| format!(", {f}"))
                .collect::<String>(),
            vis_new = vis_new.iter().map(|f| format!(", {f}")).collect::<String>(),
            lookup_target = self.group_lookup(&self.target_ident, "vis", "x.ctid"),
            key = ddl::pk_key_sql_expr(&self.identity, Some("vis")),
            applied = applied.join(", "),
        )
    }
}

/// The settings [`apply_locked_groups`]'s statement runs under, and their
/// reset for the rest of the page's transaction. An error leaves them on,
/// but it also aborts the transaction, where a `set local` ends.
const PLAN_SETTINGS: &str = "set local enable_nestloop = off; set local enable_seqscan = off";
const PLAN_RESET: &str =
    "set local enable_nestloop to default; set local enable_seqscan to default";

/// A `JOIN` build's settings for its statements over every cell
/// ([`write_join_build_cells`], `super::join`'s product): they join cell sets
/// on [`GroupTargetPlan::group_match`]'s arrays, which a merge join must sort,
/// and sorting arrays costs several times hashing them.
pub(super) const BULK_PLAN_SETTINGS: &str = "set local enable_mergejoin = off";
pub(super) const BULK_PLAN_RESET: &str = "set local enable_mergejoin to default";

/// The most `GROUP BY` columns a group lookup writes one index probe per
/// pattern of `NULL`s for (`GroupTargetPlan::group_lookup`): 15 probes at 4.
const MAX_NULL_PATTERN_COLUMNS: usize = 4;

/// What one page applied to one group ([`apply_locked_groups`]): the
/// group's identity key text (`ddl::pk_key_sql_expr`), and the deltas it
/// added to `__n`, to each argument's count and, for a summed argument, to
/// its sum (as text), in argument order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AppliedGroup {
    pub key: String,
    pub n: i64,
    pub counts: Vec<i64>,
    pub sums: Vec<Option<String>>,
}

/// Step 1 of a page on a group-basis target (see the module doc): locks, in
/// one sorted statement, every group a side of the page names and every
/// group its image-less changes name, creating the hidden rows of those
/// that have none.
pub(crate) async fn lock_groups(
    txn: &Transaction<'_>,
    plan: &GroupTargetPlan,
) -> Result<(), ApplyError> {
    stage_sides(txn, plan).await?;
    name_groups(txn, plan, &[], &plan.recompute_keys).await?;
    // Planted bug (#1114): lock none of the groups that exist. See
    // `crate::plant`.
    #[cfg(any(test, feature = "test-util"))]
    if crate::plant::fires(
        crate::plant::Plant::GbSkipGroupLock,
        matches!(plan.writer, Writer::Aggregate),
    ) {
        txn.execute(&plan.planted_insert_missing_sql(), &[]).await?;
        return Ok(());
    }
    txn.execute(&plan.lock_sql(), &[]).await?;
    Ok(())
}

/// Step 0 of a page on a group-basis target: sums the page's sides per
/// (group, transaction, statement) into the session's staging table, before
/// any group is locked. Parsing and evaluating the images is most of a
/// page's work, and none of it reads a basis, so it stays out of the time
/// the page holds its groups.
pub(super) async fn stage_sides(
    txn: &Transaction<'_>,
    plan: &GroupTargetPlan,
) -> Result<(), ApplyError> {
    txn.batch_execute(&plan.working_ddl()).await?;
    let bad: i64 = txn
        .query_one(&plan.stage_sql(), &[&plan.sides])
        .await?
        .get(0);
    if bad > 0 {
        return Err(plan.unsupported(
            "a ring row without the images its change needs (an image-less delete or \
             update); the group basis applies captured images only",
        ));
    }
    Ok(())
}

/// Adds to the groups `plan` recomputes ([`GroupTargetPlan::recomputed`])
/// the group of each of `images` (source-row images, or objects of the
/// group columns alone) and the group of the current source row of each of
/// `keys`. Reads the source before any group is locked: which groups to
/// recompute is decided here, and each is counted only by the recompute's
/// own statement, after its lock.
pub(super) async fn name_groups(
    txn: &Transaction<'_>,
    plan: &GroupTargetPlan,
    images: &[String],
    keys: &[String],
) -> Result<(), ApplyError> {
    if images.is_empty() && keys.is_empty() {
        return Ok(());
    }
    let mut parts: Vec<Vec<Option<String>>> =
        vec![Vec::with_capacity(keys.len()); plan.source_pk.len()];
    for key in keys {
        let split = ddl::split_pk_key(&plan.source_pk, &plan.source_row, key)?;
        for (column, part) in parts.iter_mut().zip(split) {
            column.push(part.map(|p| p.into_owned()));
        }
    }
    let mut params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = vec![&images];
    for column in &parts {
        params.push(column);
    }
    txn.execute(&plan.name_groups_sql(), &params).await?;
    Ok(())
}

/// Step 3 of a page on a group-basis target (see the module doc): applies
/// the page's sides to the groups [`lock_groups`] locked, in a later
/// statement, and writes their visible rows. Each changed visible row is
/// recorded into `mutations`. Returns what it applied per group, and how
/// many visible rows it wrote and deleted.
pub(crate) async fn apply_locked_groups(
    txn: &Transaction<'_>,
    plan: &GroupTargetPlan,
    mutations: &mut TargetMutations,
) -> Result<(Vec<AppliedGroup>, usize, usize), ApplyError> {
    // Planted bug (#1114): count the pages whose basis test the plant
    // changes. See `crate::plant`.
    #[cfg(any(test, feature = "test-util"))]
    if crate::plant::armed() == Some(crate::plant::Plant::GbLsnNotVisibility) {
        let sql = plan.planted_in_flight_sql();
        let row = match &plan.floor_key {
            Some(key) => txn.query_one(&sql, &[key]).await?,
            None => txn.query_one(&sql, &[]).await?,
        };
        crate::plant::fires(
            crate::plant::Plant::GbLsnNotVisibility,
            row.get::<_, i64>(0) > 0,
        );
    }
    if let Writer::JoinPartials { .. } = plan.writer {
        let sql = plan.apply_sql(None);
        txn.batch_execute(PLAN_SETTINGS).await?;
        match &plan.floor_key {
            Some(key) => txn.query_one(&sql, &[key]).await?,
            None => txn.query_one(&sql, &[]).await?,
        };
        txn.batch_execute(PLAN_RESET).await?;
        return Ok((Vec::new(), 0, 0));
    }
    let image_columns = mutations.image_columns(txn, &plan.target).await?;
    let sql = plan.apply_sql(image_columns.as_deref());
    // One index lookup per group (`b`), not per (group, transaction): a
    // page of 500 transactions over 4,000 groups has up to 100k of those.
    // Every other join is between the page's own CTEs, whose sizes the
    // planner can't see (`unnest` of a parameter): as a nested loop it is
    // quadratic in the page, so it's planned without one. A lateral lookup
    // is a nested loop whatever the setting. Without one, the hidden rows'
    // update can't probe each row by its `ctid` from the join, so it fetches
    // them by a TID scan over the array of the page's `ctid`s instead: as a
    // join on `ctid` alone it hashed a sequential scan of the whole hidden
    // table, every group the definition has, while the page held its groups'
    // locks (twice the statement's time at 1M groups and 4,000 a page).
    // Sequential scans are off too: the statement reads the hidden table and
    // the target only by `ctid` or by their group identity index, and a
    // table whose statistics say it is small (a new definition's, before
    // `analyze` reaches it) would otherwise be scanned whole as it grows.
    // The staging table has no index, so its scans stay sequential.
    txn.batch_execute(PLAN_SETTINGS).await?;
    let rows = match &plan.floor_key {
        Some(key) => txn.query(&sql, &[key]).await?,
        None => txn.query(&sql, &[]).await?,
    };
    txn.batch_execute(PLAN_RESET).await?;
    if let Some(sentinel) = rows
        .iter()
        .find(|row| row.get::<_, Option<bool>>(1).is_none())
    {
        return Err(plan.lock_gap(sentinel.get(5)));
    }
    Ok(written_groups(plan, rows, mutations))
}

/// Pins the settings a recorded constraint text's meaning depends on
/// (`crate::capture::partitions::pinned_settings`) for the rest of the
/// transaction, or until [`restore_settings`], and returns what to restore.
/// Only a routed enumeration runs under them, and it waits for a lock as the
/// session does: under its `lock_timeout` cap, which a `set local ... to
/// default` would drop.
pub(super) async fn pin_settings(txn: &Transaction<'_>) -> Result<[String; 4], ApplyError> {
    let row = txn
        .query_one(
            "select current_setting('search_path'), current_setting('lock_timeout'), \
                    current_setting('datestyle'), current_setting('timezone')",
            &[],
        )
        .await?;
    let saved: [String; 4] = [row.get(0), row.get(1), row.get(2), row.get(3)];
    txn.batch_execute(&crate::capture::partitions::pinned_settings())
        .await?;
    txn.execute("select set_config('lock_timeout', $1, true)", &[&saved[1]])
        .await?;
    Ok(saved)
}

/// Undoes [`pin_settings`].
pub(super) async fn restore_settings(
    txn: &Transaction<'_>,
    saved: &[String; 4],
) -> Result<(), ApplyError> {
    txn.execute(
        "select set_config('search_path', $1, true), set_config('lock_timeout', $2, true), \
                set_config('datestyle', $3, true), set_config('timezone', $4, true)",
        &[&saved[0], &saved[1], &saved[2], &saved[3]],
    )
    .await?;
    Ok(())
}

/// A JOIN build's enumeration of one side's cells (`super::join::build`,
/// ADR-0002's R8): every cell the side's hidden table holds, every cell of
/// its table's rows, and the cell of every old image of the ring's changes
/// to the table ([`GroupTargetPlan::rebuild_groups_select`]), into `cells`
/// (a session temp table, quoted and `pg_temp.`-qualified, dropped at
/// commit), which becomes the plan's [`GroupTargetPlan::recomputed`]. A
/// `create … as select`, so the read of the table runs in parallel where
/// Postgres can. With a `scope` (one leaf of a partitioned side, or its
/// remainder, #1286) only the cells the scope routes to the structure.
/// Reads without a lock.
pub(super) async fn enumerate_cells(
    txn: &Transaction<'_>,
    plan: &mut GroupTargetPlan,
    scope: Option<&RebuildScope>,
    cells: &str,
) -> Result<(), ApplyError> {
    let saved = match scope {
        Some(_) => Some(pin_settings(txn).await?),
        None => None,
    };
    txn.batch_execute(&format!(
        "create temp table {} on commit drop as {}",
        cells.trim_start_matches("pg_temp."),
        plan.rebuild_groups_select(scope, &crate::pool::quote_literal(&plan.source))
    ))
    .await?;
    if let Some(saved) = saved {
        restore_settings(txn, &saved).await?;
    }
    plan.recomputed = cells.to_string();
    analyze(txn, &[cells]).await
}

/// A JOIN build's enumeration of `T`'s groups: one zero row per group of the
/// visible target and of `T`'s hidden table, into `T`'s staging table
/// ([`GroupTargetPlan::target_groups_sql`]).
pub(super) async fn enumerate_target_groups(
    txn: &Transaction<'_>,
    plan: &GroupTargetPlan,
) -> Result<(), ApplyError> {
    txn.execute(&plan.target_groups_sql(), &[]).await?;
    Ok(())
}

/// `T`'s write in a JOIN build, over the groups [`GroupTargetPlan::lock_sql`]
/// locked: adds the product's deltas to the hidden partials, then replaces
/// every locked group's visible row by comparing what its partials give with
/// the row the target holds ([`GroupTargetPlan::compare_held_sql`]). Each
/// changed visible row is recorded into `mutations`, with the held row as its
/// prior image. Returns the rows it wrote and deleted.
pub(super) async fn apply_join_build_groups(
    txn: &Transaction<'_>,
    plan: &GroupTargetPlan,
    mutations: &mut TargetMutations,
) -> Result<(usize, usize), ApplyError> {
    txn.batch_execute(PLAN_SETTINGS).await?;
    let applied = txn.query_one(&plan.apply_hidden_sql(), &[]).await?;
    txn.batch_execute(PLAN_RESET).await?;
    let lost: i64 = applied.get(1);
    if lost > 0 {
        return Err(plan.lock_gap(lost));
    }
    let image_columns = mutations.image_columns(txn, &plan.target).await?;
    let rows = txn
        .query(&plan.compare_held_sql(image_columns.as_deref()), &[])
        .await?;
    let (_, written, deleted) = written_groups(plan, rows, mutations);
    Ok((written, deleted))
}

/// The recompute of the groups in [`GroupTargetPlan::recomputed`], which
/// the caller has locked (R1: [`lock_groups`] for a page, [`recompute_groups`]
/// for a release): an absolute write of each group's partials from a
/// `GROUP BY` of its source rows, with the statement's snapshot as its
/// basis (see [`GroupTargetPlan::recompute_sql`]). Each changed visible row
/// is recorded into `mutations`. Returns how many visible rows it wrote and
/// deleted. A page's own sides, applied before it in the same transaction,
/// are counted again here and overwritten: their transactions are committed,
/// so the recompute's snapshot sees them, and its basis refuses them from
/// then on.
///
/// A group-basis definition has no build chunks (#1101: its build is one
/// absolute write), so the recompute counts every source row of its groups.
pub(crate) async fn recompute_locked_groups(
    txn: &Transaction<'_>,
    plan: &GroupTargetPlan,
    mutations: &mut TargetMutations,
) -> Result<(usize, usize), ApplyError> {
    recompute_locked_groups_from(txn, plan, None, mutations).await
}

/// [`recompute_locked_groups`], reading the groups' rows from `node` when
/// it is given ([`GroupTargetPlan::recompute_sql`]).
pub(super) async fn recompute_locked_groups_from(
    txn: &Transaction<'_>,
    plan: &GroupTargetPlan,
    node: Option<&str>,
    mutations: &mut TargetMutations,
) -> Result<(usize, usize), ApplyError> {
    // A `JOIN` side writes no visible row: it adds each cell's change to
    // its deltas table, as its apply does.
    if let Writer::JoinPartials { .. } = plan.writer {
        txn.query_one(&plan.recompute_sql(None, node), &[]).await?;
        return Ok((0, 0));
    }
    let image_columns = mutations.image_columns(txn, &plan.target).await?;
    let rows = txn
        .query(&plan.recompute_sql(image_columns.as_deref(), node), &[])
        .await?;
    let (_, written, deleted) = written_groups(plan, rows, mutations);
    Ok((written, deleted))
}

/// A `JOIN` build's write of one side's cells, `plan` a
/// [`Writer::JoinPartials`] (`super::join::build`): the snapshot statement,
/// one `GROUP BY` of `node` into `grouped` ([`GroupTargetPlan::join_group_sql`]),
/// then every cell of [`GroupTargetPlan::recomputed`] matched to its hidden
/// row and to `grouped` into `cells`, and written once from there
/// ([`GroupTargetPlan::join_build_sql`]). The caller holds `J` on every key
/// the cells name and has locked the cells with a `NULL` key part, so the
/// snapshot follows every lock statement.
pub(super) async fn write_join_build_cells(
    txn: &Transaction<'_>,
    plan: &GroupTargetPlan,
    node: &str,
    grouped: &str,
    cells: &str,
) -> Result<(), ApplyError> {
    let Writer::JoinPartials { deltas } = &plan.writer else {
        return Err(plan.unsupported("a JOIN build's cell write on a structure with no cells"));
    };
    txn.batch_execute(&plan.join_group_sql(node, grouped))
        .await?;
    analyze(txn, &[grouped]).await?;
    let [fill, write] = plan.join_build_sql(grouped, cells, deltas);
    txn.batch_execute(BULK_PLAN_SETTINGS).await?;
    // The write updates each held cell by the `ctid` the match read. A
    // cell the zero-cell collection deletes in between leaves a dead row
    // there, and once no snapshot needs that row a vacuum or a prune may
    // free its line pointer for another key's cell, which the update would
    // then overwrite. The cursor's snapshot, taken before the match and
    // kept until the write is done, keeps every row the match saw from
    // being removed, as one statement's snapshot did.
    txn.batch_execute(&format!(
        "declare {MATCH_HOLD} no scroll cursor for select; {fill}"
    ))
    .await?;
    analyze(txn, &[cells]).await?;
    // Test-only pause point between the match and the write, holding no
    // snapshot of its own, as production holds none but the cursor's
    // between the two statements. See `super::interleave`.
    #[cfg(any(test, feature = "test-util"))]
    super::interleave::pause_between_transactions(
        txn,
        super::interleave::PausePoint::AfterJoinCellMatch,
        &plan.target,
    )
    .await?;
    txn.query_one(&write, &[]).await?;
    txn.batch_execute(&format!("close {MATCH_HOLD}; {BULK_PLAN_RESET}"))
        .await?;
    Ok(())
}

/// The cursor [`write_join_build_cells`] holds open from before its match
/// statement until its write is done, for its snapshot alone.
const MATCH_HOLD: &str = "trellis_join_match_hold";

/// Refreshes the planner's statistics of `tables` (session temp tables,
/// quoted and qualified) after a bulk statement filled them. Postgres never
/// analyzes a temp table on its own, so without this a build's statements
/// are planned for a few hundred rows where there are millions: nested
/// loops and sorts where a hash join is right.
pub(super) async fn analyze(txn: &Transaction<'_>, tables: &[&str]) -> Result<(), ApplyError> {
    if tables.is_empty() {
        return Ok(());
    }
    txn.batch_execute(&format!("analyze {}", tables.join(", ")))
        .await?;
    Ok(())
}

/// A release's recompute on a group-basis target (#1102 decision 1): every
/// group the held key could owe (R8), each named by an object of its group
/// columns in `groups` (the held row's `agg_groups`: the groups of the old
/// and new images of every change skipped while the key was held, the
/// evicting change's among them), plus the group of the key's current row.
/// Locks them in one sorted statement (R1, R6), then recomputes them in
/// the next. The caller holds the version fence of the key's source, so no
/// page of it runs meanwhile. Returns how many visible rows it wrote and
/// deleted.
pub(crate) async fn recompute_groups(
    txn: &Transaction<'_>,
    plan: &GroupTargetPlan,
    groups: &[String],
    key: &str,
    mutations: &mut TargetMutations,
) -> Result<(usize, usize), ApplyError> {
    txn.batch_execute(&plan.working_ddl()).await?;
    name_groups(txn, plan, groups, &[key.to_string()]).await?;
    txn.execute(&plan.lock_sql(), &[]).await?;
    recompute_locked_groups(txn, plan, mutations).await
}

/// [`recompute_groups`] over `groups` alone, reading their rows from `node`
/// when it is given.
async fn recompute_named_groups(
    txn: &Transaction<'_>,
    plan: &GroupTargetPlan,
    groups: &[String],
    node: Option<&str>,
    mutations: &mut TargetMutations,
) -> Result<(usize, usize), ApplyError> {
    txn.batch_execute(&format!("{}; {}", plan.staged_ddl(), plan.recomputed_ddl()))
        .await?;
    name_groups(txn, plan, groups, &[]).await?;
    txn.execute(&plan.lock_sql(), &[]).await?;
    recompute_locked_groups_from(txn, plan, node, mutations).await
}

/// The plan of `definition`'s group-basis target, read in `txn`, if the
/// group basis takes it.
async fn plan_of(
    txn: &Transaction<'_>,
    catalog_schema: &str,
    definition: &crate::defs::model::Definition,
) -> Result<Option<GroupTargetPlan>, ApplyError> {
    let Some(shape) = route(&definition.def, &definition.source_columns) else {
        return Ok(None);
    };
    let source_pk = ddl::source_primary_key_in_txn(txn, &definition.source_table).await?;
    let identity = ddl::identity_key_columns(txn, &definition.target_table).await?;
    Ok(Some(GroupTargetPlan::new(
        catalog_schema,
        definition.id,
        &definition.target_table,
        &definition.source_table,
        source_pk,
        identity,
        shape,
    )))
}

/// A partition correction's recompute on a group-basis target (#1103,
/// `super::partition_correction`): every group of `groups` (objects of the
/// group columns: the groups that route to the node, `super::partition_correction`
/// enumerates them) locked in one sorted statement and recomputed in the
/// next from a `GROUP BY` of its source rows, with the statement's snapshot
/// as its basis, then the seam's rows for every visible row that changed. A
/// change the node's operation removed that is still in the ring has a
/// transaction the basis sees, so it is refused when its page applies.
/// The rows are read through the root, one index probe per group, or, when
/// `node` (quoted and qualified) is given, by one `GROUP BY` of that
/// partition ([`GroupTargetPlan::recompute_sql`]'s condition is the
/// caller's). Returns whether the definition is on the group basis and,
/// when it is, the visible rows written and deleted.
pub(crate) async fn recompute_routed(
    txn: &Transaction<'_>,
    catalog_schema: &str,
    definition: &crate::defs::model::Definition,
    groups: &[String],
    node: Option<&str>,
) -> Result<Option<(usize, usize)>, ApplyError> {
    let Some(plan) = plan_of(txn, catalog_schema, definition).await? else {
        return Ok(None);
    };
    let mut mutations = TargetMutations::new();
    let counts = recompute_named_groups(txn, &plan, groups, node, &mut mutations).await?;
    let propagation = mutations.into_staged(txn).await?;
    super::append::append(txn, &propagation.changes).await?;
    Ok(Some(counts))
}

/// A release's work on `definition` ([`super::quarantine::release_key`]),
/// when the group basis takes it: [`recompute_groups`] over the held row's
/// `agg_groups` and `key`'s current row, then the seam's rows for every
/// visible row that changed. Returns whether the definition is on the group
/// basis; when it isn't, does nothing.
pub(crate) async fn release(
    txn: &Transaction<'_>,
    catalog_schema: &str,
    definition: &crate::defs::model::Definition,
    groups: &[String],
    key: &str,
) -> Result<bool, ApplyError> {
    let Some(plan) = plan_of(txn, catalog_schema, definition).await? else {
        return Ok(false);
    };
    let mut mutations = TargetMutations::new();
    recompute_groups(txn, &plan, groups, key, &mut mutations).await?;
    let propagation = mutations.into_staged(txn).await?;
    super::append::append(txn, &propagation.changes).await?;
    Ok(true)
}

/// One structure of a partitioned source's rebuild (#1101): a leaf of the
/// recorded tree, or the remainder, the groups that route to no leaf. Every
/// group key holds every partition-key column (ADR-0017), so a group routes
/// to exactly one leaf and the structures cover disjoint sets of groups. The
/// predicates are SQL booleans over a row aliased `h` (a hidden row), `t` (a
/// visible row) or `s` (a source-row image), each true for the groups the
/// structure covers; they must run under the pinned settings
/// (`crate::capture::partitions::pinned_settings`).
#[derive(Debug, Clone)]
pub(crate) struct RebuildScope {
    pub hidden: String,
    pub target: String,
    pub images: String,
    /// The leaf (quoted and qualified), if the structure is one, whose own
    /// rows' groups the enumeration adds. The caller holds it `ACCESS SHARE`.
    pub node: Option<String>,
    /// Whether the replace reads the leaf by one `GROUP BY`: the caller has
    /// established that the tree is as recorded, so the leaf holds every row
    /// of every group the scope covers and no row of any other. When it
    /// isn't, the replace reads each group's rows through the root.
    pub read_node: bool,
}

/// An absolute write's snapshot, its basis, and the newest segment there is,
/// its stamp (R5), read in one statement.
const SNAPSHOT_AND_STAMP: &str = "select pg_catalog.pg_current_snapshot() as __snap, \
     coalesce((select max(seg_seq) from segments), 0) as __seg";

/// Where a rebuild's replace reads the groups' rows from.
#[derive(Debug, Clone, PartialEq, Eq)]
enum RebuildRead {
    /// One `GROUP BY` of the whole source.
    Source,
    /// One `GROUP BY` of a partition (quoted and qualified).
    Node(String),
    /// One index probe of the source per group.
    Probe,
    /// Planted bug `gb_build_snapshot_before_locks` (`crate::plant`): the
    /// `GROUP BY` of the whole source and its snapshot, read into this
    /// temporary table before the groups were locked
    /// ([`GroupTargetPlan::planted_early_read_sql`]).
    #[cfg(any(test, feature = "test-util"))]
    Early(String),
}

/// A build or rebuild of a group-basis target (ADR-0002, "An aggregate's
/// build is one absolute write per structure"; #1101's decision), in the
/// caller's `READ COMMITTED` transaction, which
/// commits it: one absolute write of every group, in three statements.
///
/// 1. **Enumerate** ([`GroupTargetPlan::rebuild_groups_sql`], R8): the
///    target's own groups, the source's, and those named by the old images
///    of the ring's changes to the source.
/// 2. **Lock** them in key order, in a statement of their own (R6), creating
///    the hidden rows of those that have none.
/// 3. **Replace** ([`GroupTargetPlan::rebuild_sql`]): one statement takes the
///    snapshot, runs the source's `GROUP BY`, and replaces each locked
///    group's partials and visible row, with the snapshot as its basis and
///    the newest segment as its stamp (R5). A group with no source row goes
///    to zero, and its visible row is removed (R4).
///
/// Pages keep applying. One that touches a locked group waits for the
/// commit, then reads the new basis, which refuses each change the snapshot
/// saw and lets a later one apply as a delta. A group the enumeration
/// didn't see (its first change committed after it) isn't locked, so the
/// replace leaves it to its pages, which apply each of its changes once.
/// Readers see each group's old values until the commit, then the new ones.
///
/// With a `scope`, the write covers one structure of a partitioned source's
/// rebuild (a leaf, or the groups that route to no leaf): the enumeration
/// names only the groups that route to it, and the replace reads the leaf's
/// rows by one `GROUP BY` of it, or each group's through the root.
///
/// Each changed visible row is recorded into `mutations`. Returns how many
/// visible rows it wrote and deleted.
pub(crate) async fn rebuild(
    txn: &Transaction<'_>,
    plan: &GroupTargetPlan,
    scope: Option<&RebuildScope>,
    mutations: &mut TargetMutations,
) -> Result<(usize, usize), ApplyError> {
    txn.batch_execute(&format!("{}; {}", plan.staged_ddl(), plan.recomputed_ddl()))
        .await?;
    let session = if scope.is_some() {
        Some(pin_settings(txn).await?)
    } else {
        None
    };
    // Planted bug (#1114): read the source and take the basis before the
    // groups are locked. See `crate::plant`.
    #[cfg(any(test, feature = "test-util"))]
    let early = if crate::plant::fires(
        crate::plant::Plant::GbBuildSnapshotBeforeLocks,
        scope.is_none() && matches!(plan.writer, Writer::Aggregate),
    ) {
        let early = "pg_temp.trellis_planted_early_read".to_string();
        txn.batch_execute(&format!(
            "drop table if exists {early}; {}",
            plan.planted_early_read_sql(&early)
        ))
        .await?;
        Some(early)
    } else {
        None
    };
    txn.execute(&plan.rebuild_groups_sql(scope), &[&plan.source])
        .await?;
    if let Some(saved) = session {
        restore_settings(txn, &saved).await?;
    }
    // Test-only pause point: the groups are enumerated, and none is locked.
    // See `super::interleave`.
    #[cfg(any(test, feature = "test-util"))]
    super::interleave::pause_at(
        txn,
        super::interleave::PausePoint::AfterRebuildEnumerate,
        &plan.target,
    )
    .await?;
    txn.execute(&plan.lock_sql(), &[]).await?;
    // Test-only pause point: the groups are locked, and the replace's
    // snapshot is not yet taken. See `super::interleave`.
    #[cfg(any(test, feature = "test-util"))]
    super::interleave::pause_at(
        txn,
        super::interleave::PausePoint::AfterRebuildLock,
        &plan.target,
    )
    .await?;
    let image_columns = mutations.image_columns(txn, &plan.target).await?;
    let read = match scope {
        None => RebuildRead::Source,
        Some(RebuildScope {
            node: Some(node),
            read_node: true,
            ..
        }) => RebuildRead::Node(node.clone()),
        Some(_) => RebuildRead::Probe,
    };
    #[cfg(any(test, feature = "test-util"))]
    let read = early.map_or(read, RebuildRead::Early);
    let rows = txn
        .query(&plan.rebuild_sql(image_columns.as_deref(), &read), &[])
        .await?;
    let (_, written, deleted) = written_groups(plan, rows, mutations);
    Ok((written, deleted))
}

/// [`rebuild`] of `definition`'s target in `txn`, then the seam's rows for
/// every visible row that changed. Returns `None` when the group basis
/// doesn't take `definition`, and does nothing then; otherwise the visible
/// rows written and deleted.
pub(crate) async fn rebuild_definition(
    txn: &Transaction<'_>,
    catalog_schema: &str,
    definition: &crate::defs::model::Definition,
    scope: Option<&RebuildScope>,
) -> Result<Option<(usize, usize)>, ApplyError> {
    let Some(shape) = route(&definition.def, &definition.source_columns) else {
        return Ok(None);
    };
    let source_pk = ddl::source_primary_key_in_txn(txn, &definition.source_table).await?;
    let identity = ddl::identity_key_columns(txn, &definition.target_table).await?;
    let plan = GroupTargetPlan::new(
        catalog_schema,
        definition.id,
        &definition.target_table,
        &definition.source_table,
        source_pk,
        identity,
        shape,
    );
    let mut mutations = TargetMutations::new();
    let counts = rebuild(txn, &plan, scope, &mut mutations).await?;
    let propagation = mutations.into_staged(txn).await?;
    super::append::append(txn, &propagation.changes).await?;
    Ok(Some(counts))
}

/// What one statement wrote per group, read from the rows
/// [`GroupTargetPlan::visible_tail_sql`] returns, with each changed visible
/// row recorded into `mutations`; and how many visible rows it wrote and
/// deleted.
fn written_groups(
    plan: &GroupTargetPlan,
    rows: Vec<tokio_postgres::Row>,
    mutations: &mut TargetMutations,
) -> (Vec<AppliedGroup>, usize, usize) {
    let mut applied = Vec::with_capacity(rows.len());
    let (mut written, mut deleted) = (0, 0);
    for row in rows {
        let key: String = row.get(0);
        let changed: bool = row.get(1);
        let visible: bool = row.get(2);
        if changed {
            if visible {
                written += 1;
            } else {
                deleted += 1;
            }
            mutations.record(
                &plan.target,
                key.clone(),
                row.get(4),
                plan.hop_gen,
                plan.src_changed,
                plan.origin_lsn,
            );
        }
        let mut column = 5;
        let n: i64 = row.get(column);
        column += 1;
        let mut counts = Vec::with_capacity(plan.shape.args.len());
        let mut sums = Vec::with_capacity(plan.shape.args.len());
        for arg in &plan.shape.args {
            counts.push(row.get(column));
            column += 1;
            if arg.summed {
                sums.push(row.get(column));
                column += 1;
            } else {
                sums.push(None);
            }
        }
        applied.push(AppliedGroup {
            key,
            n,
            counts,
            sums,
        });
    }
    (applied, written, deleted)
}

/// One page's work on one group-basis target: [`lock_groups`], the
/// interleaving tests' pause point, [`apply_locked_groups`], then
/// [`recompute_locked_groups`] when an image-less change named groups.
/// Returns the visible rows written and deleted.
#[tracing::instrument(
    name = "staging.apply_group_target",
    skip_all,
    fields(target = %plan.target, sides = plan.sides.len())
)]
pub(crate) async fn apply_group_target(
    txn: &Transaction<'_>,
    plan: &GroupTargetPlan,
    mutations: &mut TargetMutations,
) -> Result<(usize, usize), ApplyError> {
    lock_groups(txn, plan).await?;
    // Test-only pause point (#623 D1): between the lock and the read of the
    // bases. See `super::interleave`.
    #[cfg(any(test, feature = "test-util"))]
    super::interleave::pause_at(
        txn,
        super::interleave::PausePoint::AfterEntryLock,
        &plan.target,
    )
    .await?;
    let (_, mut written, mut deleted) = apply_locked_groups(txn, plan, mutations).await?;
    // Only an image-less change names a group to recompute: its key, and
    // its prior-image hint, which rides on the same ring row.
    if !plan.recompute_keys.is_empty() {
        let (w, d) = recompute_locked_groups(txn, plan, mutations).await?;
        written += w;
        deleted += d;
    }
    #[cfg(any(test, feature = "test-util"))]
    super::interleave::pause_at(
        txn,
        super::interleave::PausePoint::AfterGroupUpsert,
        &plan.target,
    )
    .await?;
    Ok((written, deleted))
}

/// A source `TRUNCATE`'s clear of a group-basis target's hidden table
/// `hidden` ([`hidden_table_ident`]; ADR-0002 "Truncate, DDL, drop"):
/// empties every group's partials, basis
/// and stamp, zero groups included, and raises the target's truncate floor
/// to `lsn`, so a change from before the truncate that still reaches a page
/// is refused. The caller clears the visible target. It runs on the
/// truncate's own page, which the drain barrier orders after every page
/// of earlier segments and before every page of later ones
/// (`super::apply::next_claimable_segments`), so no page of the source
/// holds a group meanwhile, and which passes the same claim check as any
/// page.
pub(crate) async fn truncate_groups(
    txn: &Transaction<'_>,
    hidden: &str,
    target: &str,
    lsn: PgLsn,
) -> Result<(), ApplyError> {
    txn.batch_execute(&format!("truncate {hidden}")).await?;
    raise_truncate_floor(txn, target, lsn).await
}

/// Raises the truncate floor `key` names to `lsn`: a staged side at or
/// below it is refused ([`GroupTargetPlan::apply_sql`]).
pub(super) async fn raise_truncate_floor(
    txn: &Transaction<'_>,
    key: &str,
    lsn: PgLsn,
) -> Result<(), ApplyError> {
    txn.execute(
        "insert into ledger_truncate_floor (target_table, floor) values ($1, $2) \
         on conflict (target_table) do update \
         set floor = greatest(ledger_truncate_floor.floor, excluded.floor)",
        &[&key, &lsn],
    )
    .await?;
    Ok(())
}

/// The plan of [`apply_locked_groups`]' statement on the group-basis target
/// `target` (its bare name), as `explain`'s text, under the settings a page
/// runs it with and over an empty staging table. For tests of the plan's
/// shape. It locks and writes nothing.
#[cfg(any(test, feature = "internals"))]
pub async fn explain_apply(pool: &crate::pool::Pool, target: &str) -> Result<String, ApplyError> {
    let Some(definition) = crate::defs::catalog::definition_by_target(pool, target).await? else {
        return Err(ApplyError::TransformNotFound {
            transform: target.to_string(),
        });
    };
    let Some(shape) = route(&definition.def, &definition.source_columns) else {
        return Err(ApplyError::GroupBasisUnsupported {
            target: definition.target_table,
            what: "a definition the group basis doesn't take",
        });
    };
    let mut client = pool.get().await?;
    let identity = ddl::identity_key_columns(&**client, &definition.target_table).await?;
    let plan = GroupTargetPlan::new(
        pool.schema(),
        definition.id,
        &definition.target_table,
        &definition.source_table,
        ddl::source_primary_key_in_txn(&**client, &definition.source_table).await?,
        identity,
        shape,
    );
    let txn = client.transaction().await?;
    txn.batch_execute(&plan.staged_ddl()).await?;
    txn.batch_execute(PLAN_SETTINGS).await?;
    let rows = txn
        .query(
            &format!("explain {}", plan.apply_sql(None)),
            &[&plan.target],
        )
        .await?;
    txn.rollback().await?;
    Ok(rows
        .iter()
        .map(|row| row.get::<_, String>(0))
        .collect::<Vec<_>>()
        .join("\n"))
}

/// The plans of the two statements that read a whole source (or node) by
/// one `GROUP BY` and match its groups to the locked ones: the node form of
/// [`GroupTargetPlan::recompute_sql`] (a `JOIN` side's build, `RESUME` and
/// rebuild) and [`GroupTargetPlan::rebuild_sql`]'s source read (a plain
/// aggregate's), for `target`'s group-basis definition, each read from the
/// whole source. Planned with nested loops disabled, so a nested loop that
/// matches the groups shows only where the match can't be hashed or merged
/// (#1318); the lateral lookups of the hidden rows stay nested loops. It
/// locks and writes nothing.
#[cfg(any(test, feature = "internals"))]
pub async fn explain_whole_source_reads(
    pool: &crate::pool::Pool,
    target: &str,
) -> Result<[String; 2], ApplyError> {
    let Some(definition) = crate::defs::catalog::definition_by_target(pool, target).await? else {
        return Err(ApplyError::TransformNotFound {
            transform: target.to_string(),
        });
    };
    let Some(shape) = route(&definition.def, &definition.source_columns) else {
        return Err(ApplyError::GroupBasisUnsupported {
            target: definition.target_table,
            what: "a definition the group basis doesn't take",
        });
    };
    let mut client = pool.get().await?;
    let identity = ddl::identity_key_columns(&**client, &definition.target_table).await?;
    let plan = GroupTargetPlan::new(
        pool.schema(),
        definition.id,
        &definition.target_table,
        &definition.source_table,
        ddl::source_primary_key_in_txn(&**client, &definition.source_table).await?,
        identity,
        shape,
    );
    let txn = client.transaction().await?;
    txn.batch_execute(&format!(
        "{}; {}; set local enable_nestloop = off",
        plan.staged_ddl(),
        plan.recomputed_ddl()
    ))
    .await?;
    let statements = [
        plan.recompute_sql(None, Some(plan.source_row())),
        plan.rebuild_sql(None, &RebuildRead::Source),
    ];
    let mut plans = [String::new(), String::new()];
    for (out, sql) in plans.iter_mut().zip(statements) {
        let rows = txn.query(&format!("explain {sql}"), &[]).await?;
        *out = rows
            .iter()
            .map(|row| row.get::<_, String>(0))
            .collect::<Vec<_>>()
            .join("\n");
    }
    txn.rollback().await?;
    Ok(plans)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn columns() -> HashMap<String, ValueType> {
        [
            ("g", ValueType::Integer(IntWidth::Int4)),
            ("h", ValueType::Text),
            ("a", ValueType::Integer(IntWidth::Int4)),
            ("b", ValueType::Integer(IntWidth::Int8)),
            ("m", ValueType::Numeric),
            ("f", ValueType::Float(crate::float::FloatWidth::Float8)),
        ]
        .into_iter()
        .map(|(name, ty)| (name.to_string(), ty))
        .collect()
    }

    fn shape(text: &str) -> Option<GroupShape> {
        let def = crate::defs::parse(text).expect("parses");
        route(&def, &columns())
    }

    #[test]
    fn takes_the_admitted_aggregates_and_their_compositions() {
        for text in [
            "TRANSFORM t FROM s GROUP BY g SELECT COUNT(*) AS n",
            "TRANSFORM t FROM s GROUP BY g, h SELECT COUNT(h) AS n, SUM(a) AS s, AVG(b) AS v",
            "TRANSFORM t FROM s GROUP BY g SELECT SUM(m) AS s, AVG(m) AS v",
            "TRANSFORM t FROM s GROUP BY g SELECT SUM(a + b) AS s, COUNT(f) AS n",
            "TRANSFORM t FROM s GROUP BY g SELECT SUM(a) + COUNT(b) AS x, \
             COALESCE(SUM(a), 0) AS y, SUM(a) > COUNT(*) AS z",
        ] {
            assert!(shape(text).is_some(), "{text}");
        }
    }

    #[test]
    fn takes_no_refused_aggregate_and_no_one_to_one() {
        for text in [
            "TRANSFORM t FROM s GROUP BY g SELECT MIN(a) AS n",
            "TRANSFORM t FROM s GROUP BY g SELECT MAX(a) + COUNT(*) AS n",
            "TRANSFORM t FROM s GROUP BY g SELECT SUM(f) AS n",
            "TRANSFORM t FROM s GROUP BY g SELECT AVG(f) AS n",
            // A `numeric` expression has no declared scale.
            "TRANSFORM t FROM s GROUP BY g SELECT SUM(m + a) AS n",
            "TRANSFORM t FROM s SELECT a AS a",
        ] {
            assert!(shape(text).is_none(), "{text}");
        }
    }

    /// A session keeps the temp table it created under a name, and a resume
    /// keeps the definition id while re-reading the column types: the name
    /// must change with the shape, or a connection reuses the old table.
    #[test]
    fn the_staging_table_name_follows_its_shape() {
        let text = "TRANSFORM t FROM s GROUP BY g SELECT SUM(a) AS s";
        let name = |cols: &HashMap<String, ValueType>| {
            let def = crate::defs::parse(text).expect("parses");
            let shape = route(&def, cols).expect("admitted");
            GroupTargetPlan::new(
                "trellis",
                7,
                "public.t",
                "public.s",
                Vec::new(),
                Vec::new(),
                shape,
            )
            .staged
        };
        let int4 = columns();
        let mut int8 = columns();
        int8.insert("g".to_string(), ValueType::Integer(IntWidth::Int8));
        assert_eq!(name(&int4), name(&columns()));
        assert_ne!(name(&int4), name(&int8));
        assert!(name(&int4).starts_with("pg_temp.\"trellis_group_sides_7_"));
        let def = crate::defs::parse(text).expect("parses");
        let shape = route(&def, &int4).expect("admitted");
        let longest = GroupTargetPlan::new(
            "trellis",
            i64::MIN,
            "public.t",
            "public.s",
            Vec::new(),
            Vec::new(),
            shape,
        )
        .staged;
        let bare = longest
            .trim_start_matches("pg_temp.\"")
            .trim_end_matches('"');
        assert!(bare.len() <= 63, "{bare} fits a Postgres identifier");
    }

    /// The recompute table is a session table too: its name follows its
    /// group columns' types, and fits an identifier for any id.
    #[test]
    fn the_recompute_table_name_follows_its_shape() {
        let text = "TRANSFORM t FROM s GROUP BY g SELECT SUM(a) AS s";
        let name = |id: i64, cols: &HashMap<String, ValueType>| {
            let def = crate::defs::parse(text).expect("parses");
            let shape = route(&def, cols).expect("admitted");
            GroupTargetPlan::new(
                "trellis",
                id,
                "public.t",
                "public.s",
                Vec::new(),
                Vec::new(),
                shape,
            )
            .recomputed
        };
        let int4 = columns();
        let mut int8 = columns();
        int8.insert("g".to_string(), ValueType::Integer(IntWidth::Int8));
        assert_eq!(name(7, &int4), name(7, &columns()));
        assert_ne!(name(7, &int4), name(7, &int8));
        assert!(name(7, &int4).starts_with("pg_temp.\"trellis_group_recompute_7_"));
        let longest = name(i64::MIN, &int4);
        let bare = longest
            .trim_start_matches("pg_temp.\"")
            .trim_end_matches('"');
        assert!(bare.len() <= 63, "{bare} fits a Postgres identifier");
    }

    /// One page staged and locked on `client` for definition 7 over the
    /// source columns `cols`, after recreating its hidden table for that
    /// shape: one insert of group `g`. Returns the hidden table's groups.
    async fn stage_one_page(
        client: &mut tokio_postgres::Client,
        cols: &HashMap<String, ValueType>,
        g: i64,
    ) -> Vec<i64> {
        let schema = crate::config::DEFAULT_SCHEMA;
        let def =
            crate::defs::parse("TRANSFORM t FROM s GROUP BY g SELECT SUM(a) AS s").expect("parses");
        let shape = route(&def, cols).expect("admitted");
        let mut plan = GroupTargetPlan::new(
            schema,
            7,
            "public.t",
            "public.s",
            Vec::new(),
            Vec::new(),
            shape.clone(),
        );
        plan.sides = vec![format!(
            r#"{{"op":"insert","t":"100","l":"0/10","n":{{"id":1,"g":{g},"a":5}}}}"#
        )];
        let txn = client.transaction().await.expect("begin");
        txn.batch_execute(&format!(
            "{}; {}",
            drop_hidden_table_sql(schema, 7),
            hidden_table_ddl(schema, 7, &shape)
        ))
        .await
        .expect("hidden table");
        if let Err(error) = lock_groups(&txn, &plan).await {
            panic!("staging group {g}: {error:?}");
        }
        let groups = txn
            .query(&format!("select g::bigint from {}", plan.hidden_ident), &[])
            .await
            .expect("hidden rows")
            .iter()
            .map(|row| row.get(0))
            .collect();
        txn.commit().await.expect("commit");
        groups
    }

    /// The same connection stages a page for one definition id under two
    /// shapes, as after a resume that re-typed the group column (the hidden
    /// table and the plan are rebuilt here by hand): the second page's group, out of the
    /// first shape's range, stages into a table of its own type.
    #[tokio::test]
    async fn a_session_stages_a_retyped_group_into_a_table_of_its_shape() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (mut client, connection) = tokio_postgres::connect(db.dsn(), tokio_postgres::NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
            .batch_execute("create table public.s (id integer primary key, g integer, a integer)")
            .await
            .expect("source");
        assert_eq!(stage_one_page(&mut client, &columns(), 1).await, [1]);
        client
            .batch_execute("alter table public.s alter column g type bigint")
            .await
            .expect("re-type the group column");
        let mut int8 = columns();
        int8.insert("g".to_string(), ValueType::Integer(IntWidth::Int8));
        assert_eq!(
            stage_one_page(&mut client, &int8, 5_000_000_000).await,
            [5_000_000_000]
        );
    }

    /// Names group `g` into the recompute table on `client`, as a recompute
    /// does, for definition 7 over the source columns `cols`.
    async fn name_one_group(
        client: &mut tokio_postgres::Client,
        cols: &HashMap<String, ValueType>,
        g: i64,
    ) {
        let def =
            crate::defs::parse("TRANSFORM t FROM s GROUP BY g SELECT SUM(a) AS s").expect("parses");
        let shape = route(&def, cols).expect("admitted");
        let plan = GroupTargetPlan::new(
            crate::config::DEFAULT_SCHEMA,
            7,
            "public.t",
            "public.s",
            vec![PrimaryKeyColumn {
                name: "id".to_string(),
                data_type: "integer".to_string(),
                nullable: false,
                collation: None,
            }],
            Vec::new(),
            shape,
        );
        let txn = client.transaction().await.expect("begin");
        txn.batch_execute(&format!("{}; {}", plan.staged_ddl(), plan.recomputed_ddl()))
            .await
            .expect("session tables");
        if let Err(error) = name_groups(&txn, &plan, &[format!(r#"{{"g":{g}}}"#)], &[]).await {
            panic!("naming group {g}: {error:?}");
        }
        txn.commit().await.expect("commit");
    }

    /// The recompute's table follows a re-typed group column the same way:
    /// the second recompute, of a group out of the first shape's range,
    /// names it into a table of its own type.
    #[tokio::test]
    async fn a_session_recomputes_a_retyped_group_into_a_table_of_its_shape() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (mut client, connection) = tokio_postgres::connect(db.dsn(), tokio_postgres::NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
            .batch_execute("create table public.s (id integer primary key, g integer, a integer)")
            .await
            .expect("source");
        name_one_group(&mut client, &columns(), 1).await;
        client
            .batch_execute("alter table public.s alter column g type bigint")
            .await
            .expect("re-type the group column");
        let mut int8 = columns();
        int8.insert("g".to_string(), ValueType::Integer(IntWidth::Int8));
        name_one_group(&mut client, &int8, 5_000_000_000).await;
    }

    /// The `explain` text of `sql` on `txn`.
    async fn explain(txn: &Transaction<'_>, sql: &str) -> String {
        txn.query(&format!("explain {sql}"), &[])
            .await
            .expect("explain")
            .iter()
            .map(|row| row.get::<_, String>(0))
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// A `JOIN` build's cell write over a hidden table that already holds
    /// its cells (a rebuild, a resume) matches every set by hash joins: the
    /// cells to the hidden rows and to the `GROUP BY`, then the cells to
    /// insert by an anti-join against the update's rows, planned for the
    /// cells there are. A correlated subplan there rescans the update's rows
    /// once per cell, and a hash table sized for the match's guessed rows
    /// (Postgres has no statistics for its arrays) loops over its batches:
    /// either way a 100M-row rebuild ran for hours (#1346).
    #[tokio::test]
    async fn a_join_build_write_over_held_cells_hashes_every_match() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (mut client, connection) = tokio_postgres::connect(db.dsn(), tokio_postgres::NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let def =
            crate::defs::parse("TRANSFORM t FROM s GROUP BY g SELECT SUM(a) AS s").expect("parses");
        let shape = route(&def, &columns()).expect("admitted");
        let hidden = hidden_table_ident("trellis", 7);
        let deltas = "pg_temp.\"trellis_test_deltas\"".to_string();
        let plan = GroupTargetPlan::for_join(
            "public.t",
            hidden.clone(),
            "trellis_test_staged",
            Some(("public.s", "trellis_test_cells", Vec::new())),
            Vec::new(),
            shape.clone(),
            Writer::JoinPartials {
                deltas: deltas.clone(),
            },
            None,
            String::new(),
        );
        client
            .batch_execute(&format!(
                "create schema if not exists trellis; {}; \
                 create table public.segments (seg_seq bigint); \
                 create table public.s (id integer primary key, g integer, b bigint, a integer); \
                 insert into public.s select i, i, i % 100, i from generate_series(1, 20000) i; \
                 insert into {hidden} (g, __n, __count_1, __sum_1) \
                     select g, 1, 1, 1 from public.s; \
                 analyze public.s, {hidden}",
                hidden_table_ddl("trellis", 7, &shape)
            ))
            .await
            .expect("source and held cells");
        let grouped = "pg_temp.\"trellis_test_grouped\"";
        let txn = client.transaction().await.expect("begin");
        txn.batch_execute(&format!(
            "{}; {}; insert into {} (g) select g from {hidden}",
            plan.working_ddl(),
            plan.deltas_ddl().expect("a side's deltas"),
            plan.recomputed,
        ))
        .await
        .expect("working tables");
        txn.batch_execute(&plan.join_group_sql("public.s", grouped))
            .await
            .expect("group");
        analyze(&txn, &[&plan.recomputed, grouped])
            .await
            .expect("analyze");
        txn.batch_execute(BULK_PLAN_SETTINGS)
            .await
            .expect("settings");
        let matched = "pg_temp.\"trellis_test_matched\"";
        let [fill, write] = plan.join_build_sql(grouped, matched, &deltas);
        let fill_plan = explain(&txn, &fill).await;
        assert!(!fill_plan.contains("Nested Loop"), "{fill_plan}");
        txn.batch_execute(&fill).await.expect("fill");
        analyze(&txn, &[matched]).await.expect("analyze");
        let write_plan = explain(&txn, &write).await;
        assert!(!write_plan.contains("SubPlan"), "{write_plan}");
        let anti = write_plan
            .lines()
            .find(|line| line.contains("Hash") && line.contains("Anti Join"))
            .unwrap_or_else(|| panic!("no hash anti-join: {write_plan}"));
        let rows: f64 = anti
            .split("rows=")
            .nth(1)
            .and_then(|rest| rest.split_whitespace().next())
            .and_then(|rows| rows.parse().ok())
            .expect("estimated rows");
        assert!(rows <= 40_000.0, "planned for 20000 cells: {write_plan}");
    }

    /// ADR-0019's strict `T` write: a `JOIN`'s `T` page or build step whose
    /// staged group has no hidden row (a writer that skipped
    /// [`GroupTargetPlan::lock_sql`], which inserts the missing rows) fails
    /// with [`ApplyError::GroupLockGap`], and rolls nothing in; the same
    /// writer after the lock applies (#1362).
    #[tokio::test]
    async fn a_join_t_write_of_a_group_with_no_hidden_row_is_an_error() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (mut client, connection) = tokio_postgres::connect(db.dsn(), tokio_postgres::NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let def =
            crate::defs::parse("TRANSFORM t FROM s GROUP BY g SELECT SUM(a) AS s").expect("parses");
        let shape = route(&def, &columns()).expect("admitted");
        let hidden = hidden_table_ident("trellis", 7);
        client
            .batch_execute(&format!(
                "create schema if not exists trellis; {}; \
                 create table public.t (g integer primary key, s bigint)",
                hidden_table_ddl("trellis", 7, &shape)
            ))
            .await
            .expect("tables");
        let identity = ddl::identity_key_columns(&client, "public.t")
            .await
            .expect("identity");
        let plan = GroupTargetPlan::for_join(
            "public.t",
            hidden.clone(),
            "trellis_test_t_staged",
            None,
            identity,
            shape,
            Writer::JoinTarget,
            None,
            String::new(),
        );
        let txn = client.transaction().await.expect("begin");
        txn.batch_execute(&plan.staged_ddl()).await.expect("staged");
        // Group 1 gains a row; group 2 nets to zero, and is staged too.
        txn.batch_execute(&format!(
            "insert into {} (g, __txid, __lsn, __n, __c1, __s1) values \
                 (1, pg_current_xact_id(), '0/10', 1, 1, 5), \
                 (2, pg_current_xact_id(), '0/10', 0, 0, 0)",
            plan.staged
        ))
        .await
        .expect("stage");
        let is_gap = |err: ApplyError, groups: i64| {
            assert!(
                matches!(&err, ApplyError::GroupLockGap { target, groups: n }
                    if target == "public.t" && *n == groups),
                "{err:?}"
            );
        };
        // No lock: both groups are missing.
        let mut mutations = TargetMutations::assuming_read();
        let err = apply_locked_groups(&txn, &plan, &mut mutations)
            .await
            .expect_err("a page over groups with no hidden row");
        is_gap(err, 2);
        txn.rollback().await.expect("rollback");

        // The build step's statement reads the same.
        let txn = client.transaction().await.expect("begin");
        txn.batch_execute(&plan.staged_ddl()).await.expect("staged");
        txn.batch_execute(&format!(
            "insert into {} (g, __txid, __lsn, __n, __c1, __s1) values \
                 (1, pg_current_xact_id(), '0/10', 1, 1, 5)",
            plan.staged
        ))
        .await
        .expect("stage");
        let err = apply_join_build_groups(&txn, &plan, &mut TargetMutations::assuming_read())
            .await
            .expect_err("a build step over a group with no hidden row");
        is_gap(err, 1);
        txn.rollback().await.expect("rollback");

        // Locked first, as every writer is: applies.
        let txn = client.transaction().await.expect("begin");
        txn.batch_execute(&plan.staged_ddl()).await.expect("staged");
        txn.batch_execute(&format!(
            "insert into {} (g, __txid, __lsn, __n, __c1, __s1) values \
                 (1, pg_current_xact_id(), '0/10', 1, 1, 5), \
                 (2, pg_current_xact_id(), '0/10', 0, 0, 0)",
            plan.staged
        ))
        .await
        .expect("stage");
        txn.batch_execute(&plan.lock_sql()).await.expect("lock");
        let (_, written, deleted) =
            apply_locked_groups(&txn, &plan, &mut TargetMutations::assuming_read())
                .await
                .expect("a locked page applies");
        assert_eq!((written, deleted), (1, 0));
        let visible: Vec<(i32, i64)> = txn
            .query("select g, s from public.t order by g", &[])
            .await
            .expect("target")
            .iter()
            .map(|row| (row.get(0), row.get(1)))
            .collect();
        assert_eq!(visible, vec![(1, 5)]);
        txn.rollback().await.expect("rollback");
    }

    /// [`a_join_t_write_of_a_group_with_no_hidden_row_is_an_error`] over a
    /// composite text-keyed group and a `COUNT(*)`, `COUNT(h)`, `AVG(b)`
    /// shape (a counted-only argument next to a summed one), with one staged
    /// group locked and one not: the page's statement returns a written
    /// group's row and the sentinel together, so their columns must agree,
    /// and the page still fails, counting only the unlocked group (#1362).
    #[tokio::test]
    async fn a_join_t_write_with_one_unlocked_group_fails_for_any_shape() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (mut client, connection) = tokio_postgres::connect(db.dsn(), tokio_postgres::NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let def = crate::defs::parse(
            "TRANSFORM t FROM s GROUP BY g, h SELECT COUNT(*) AS n, COUNT(h) AS c, AVG(b) AS v",
        )
        .expect("parses");
        let shape = route(&def, &columns()).expect("admitted");
        assert!(
            shape.args.iter().any(|arg| arg.summed) && shape.args.iter().any(|arg| !arg.summed),
            "a counted-only and a summed argument"
        );
        let hidden = hidden_table_ident("trellis", 8);
        client
            .batch_execute(&format!(
                "create schema if not exists trellis; {}; \
                 create table public.t (g integer, h text, n bigint, c bigint, v numeric, \
                                        primary key (g, h))",
                hidden_table_ddl("trellis", 8, &shape)
            ))
            .await
            .expect("tables");
        let identity = ddl::identity_key_columns(&client, "public.t")
            .await
            .expect("identity");
        let plan = GroupTargetPlan::for_join(
            "public.t",
            hidden,
            "trellis_test_t_staged_shape",
            None,
            identity,
            shape,
            Writer::JoinTarget,
            None,
            String::new(),
        );
        // One row per group: `__n` 1, and per argument a count of 1 and a
        // sum of 4 where summed.
        let mut partials = vec!["__n".to_string()];
        let mut ones = vec!["1".to_string()];
        for (i, arg) in plan.shape.args.iter().enumerate() {
            partials.push(format!("__c{}", i + 1));
            ones.push("1".to_string());
            if arg.summed {
                partials.push(format!("__s{}", i + 1));
                ones.push("4".to_string());
            }
        }
        let stage = |g: i32, h: &str| {
            format!(
                "insert into {} (g, h, __txid, __lsn, {}) values \
                     ({g}, '{h}', pg_current_xact_id(), '0/10', {})",
                plan.staged,
                partials.join(", "),
                ones.join(", ")
            )
        };
        let txn = client.transaction().await.expect("begin");
        txn.batch_execute(&plan.staged_ddl()).await.expect("staged");
        txn.batch_execute(&stage(1, "x")).await.expect("stage");
        txn.batch_execute(&plan.lock_sql()).await.expect("lock");
        // Staged after the lock: no hidden row.
        txn.batch_execute(&stage(2, "y")).await.expect("stage");
        let err = apply_locked_groups(&txn, &plan, &mut TargetMutations::assuming_read())
            .await
            .expect_err("a page with an unlocked group");
        assert!(
            matches!(&err, ApplyError::GroupLockGap { target, groups: 1 } if target == "public.t"),
            "{err:?}"
        );
        txn.rollback().await.expect("rollback");

        // Both locked: the page applies, and writes both groups.
        let txn = client.transaction().await.expect("begin");
        txn.batch_execute(&plan.staged_ddl()).await.expect("staged");
        txn.batch_execute(&stage(1, "x")).await.expect("stage");
        txn.batch_execute(&stage(2, "y")).await.expect("stage");
        txn.batch_execute(&plan.lock_sql()).await.expect("lock");
        let (applied, written, deleted) =
            apply_locked_groups(&txn, &plan, &mut TargetMutations::assuming_read())
                .await
                .expect("a locked page applies");
        assert_eq!((applied.len(), written, deleted), (2, 2, 0));
        txn.rollback().await.expect("rollback");
    }

    #[test]
    fn shares_partials_between_calls_on_one_argument() {
        let shape = shape(
            "TRANSFORM t FROM s GROUP BY g \
             SELECT SUM(a) AS s, AVG(a) AS v, COUNT(a) AS c, COUNT(b) AS d, COUNT(*) AS n",
        )
        .expect("taken");
        assert_eq!(
            shape.args,
            vec![
                Arg {
                    sql: "s.\"a\"".to_string(),
                    expr: Expr::Column("a".to_string()),
                    summed: true
                },
                Arg {
                    sql: "s.\"b\"".to_string(),
                    expr: Expr::Column("b".to_string()),
                    summed: false
                },
            ]
        );
        assert_eq!(
            shape.aggs,
            vec![
                Agg::Sum(0, "bigint"),
                Agg::Avg(0),
                Agg::Count(0),
                Agg::Count(1),
                Agg::CountStar
            ]
        );
        assert_eq!(shape.numeric_args, Vec::<String>::new());
    }

    #[test]
    fn hidden_and_visible_tables_hold_what_the_module_doc_says() {
        let shape = shape("TRANSFORM t FROM s GROUP BY g SELECT SUM(m) AS s, COUNT(h) AS c")
            .expect("taken");
        assert_eq!(shape.numeric_args, vec!["m".to_string()]);
        assert_eq!(
            hidden_table_ddl("trellis", 7, &shape),
            "create table \"trellis\".\"agg_7\" (\"g\" integer, __n bigint not null default 0, \
             __count_1 bigint not null default 0, __sum_1 numeric not null default 0, \
             __count_2 bigint not null default 0, __basis pg_snapshot, __stamp bigint, \
             __zero boolean not null generated always as \
             (__n = 0 and __count_1 = 0 and __sum_1 = 0 and __count_2 = 0) stored, \
             unique nulls not distinct (\"g\")); \
             create index on \"trellis\".\"agg_7\" (\"g\") where __zero"
        );
        assert_eq!(
            target_table_ddl("\"public\".\"t\"", &shape),
            "create table \"public\".\"t\" (\"g\" integer, \"s\" numeric, \"c\" bigint, \
             unique nulls not distinct (\"g\"))"
        );
        assert_eq!(
            drop_hidden_table_sql("trellis", 7),
            "drop table if exists \"trellis\".\"agg_7\""
        );
    }
}
