//! Aggregate targets on the group basis (#1100 G2a, ADR-0002 "Aggregates: a
//! basis per group, kept by deltas").
//!
//! # The knob
//!
//! The group basis is built in steps (#1091), and its build (#1101) and its
//! `TRUNCATE`, recompute and quarantine paths (#1102) don't exist yet. Until
//! they do, the ledger (`super::ledger`) keeps every aggregate by default,
//! and this path runs only behind a dev/bench knob: [`enabled`], read from
//! [`KNOB_ENV`] or set with [`set_enabled`] (`trellis::dev::set_group_basis`).
//! [`route`] is the one routing decision: with the knob on, it takes every
//! aggregate it admits, and `super::ledger::route` declines those. Deleting
//! the ledger (#1105) deletes the knob and makes [`route`] unconditional.
//!
//! The knob is process-wide and read whenever a definition is routed, so it
//! must not change while a definition exists: a test binary or a benchmark
//! probe sets it once, before its client starts. With it on, a definition
//! [`route`] takes:
//!
//! - must be defined over a source that is empty and unwritten until it is
//!   `live`: its build only checks that the source is empty
//!   (`super::build`'s plan job fails otherwise), since the group-basis build
//!   is #1101's;
//! - halts on a source `TRUNCATE`, on an image-less change (a Re-derive or a
//!   released key), and on a resume or rebuild, which are #1101's and #1102's;
//! - is refused at define over a partitioned source (#1103), and for
//!   `SUM`/`AVG` over a `numeric` column with no declared scale (#1096
//!   decision 2).
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
//! instead (`crate::intake::markers::park_target_catchup_if_read`): an
//! image-less catch-up names keys, not the deltas a group needs.
//!
//! # Which definitions
//!
//! A plain aggregate (no relationship, every `GROUP BY` key a source
//! column) whose every value field composes the admitted aggregates of #1096
//! decision 1 (`COUNT(*)`, `COUNT(x)`, and `SUM`/`AVG` over an integer
//! argument or a `numeric` column) with literals, operators, functions and
//! its `GROUP BY` keys. An argument may be an expression over the row
//! (`SUM(a + b)`); a `numeric` one must be a column, whose declared scale
//! define checks. Anything else stays on the ledger.
//!
//! # Where its state lives
//!
//! The visible target is a plain table holding exactly the declared columns
//! and the `UNIQUE NULLS NOT DISTINCT` group identity (#1096 decision 7). The
//! group's partials, its basis and its stamp live in a hidden table in the
//! instance's catalog schema, named by definition id ([`hidden_table_name`],
//! `agg_<id>`, beside a `JOIN`'s `join_<id>_p`/`_m`/`_j`): the row count
//! `__n`, and per distinct aggregate argument `i` (1-based, in
//! `defs::ledger::contributions`' order) its non-null count `__count_<i>` and,
//! when a `SUM` or `AVG` reads it, its sum `__sum_<i>` as `numeric`. A group
//! row is visible in the target only while `__n > 0`; a zero group stays in
//! the hidden table.
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
//!    `JOIN` target's derived structures (#1108) call the steps itself.
//! 4. **Apply** ([`apply_locked_groups`], R2, F2): one later statement, so
//!    under `READ COMMITTED` its snapshot sees every committed basis. It
//!    sums the staged sides per (group, `row_txid`), keeps a sum only when
//!    its transaction isn't visible in the group's basis (a `NULL` basis
//!    sees nothing) and its `lsn` is above the target's truncate floor, adds
//!    what is kept to the hidden row, and writes the visible row when a
//!    visible value changed: an upsert while `__n > 0`, a delete when it
//!    falls to 0. It returns, per group, the change it applied. It runs with
//!    nested loops off: its joins are between the page's own rows, whose
//!    sizes the planner can't see.
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
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::SystemTime;

use tokio_postgres::Transaction;
use tokio_postgres::types::PgLsn;

use crate::defs::ast::{Expr, GroupByKey, KeySpace, TransformDef, ValueType, group_by_contains};
use crate::defs::ddl::{self, PrimaryKeyColumn};
use crate::defs::ledger as schema;
use crate::defs::oracle::{render_expr_sql, render_to_one_rel_expr_sql};
use crate::integer::IntWidth;
use crate::pool::{quote_ident, quote_literal};

use super::apply::ApplyError;
use super::fold::{FoldedChange, earliest_origin};
use super::target_mutations::TargetMutations;

/// The environment variable that turns the knob on (`1`, `true` or `on`)
/// when nothing has set it: see the module doc.
pub const KNOB_ENV: &str = "TRELLIS_DEV_GROUP_BASIS";

/// 0: not read yet; 1: off; 2: on.
static KNOB: AtomicU8 = AtomicU8::new(0);

/// Whether the group-basis knob is on (see the module doc).
pub fn enabled() -> bool {
    match KNOB.load(Ordering::Relaxed) {
        1 => false,
        2 => true,
        _ => {
            let on = std::env::var(KNOB_ENV)
                .is_ok_and(|v| matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "on"));
            KNOB.store(if on { 2 } else { 1 }, Ordering::Relaxed);
            on
        }
    }
}

/// Turns the group-basis knob on or off for this process. Only before any
/// client starts: see the module doc.
#[cfg_attr(not(any(test, feature = "test-util")), allow(dead_code))]
pub fn set_enabled(on: bool) {
    KNOB.store(if on { 2 } else { 1 }, Ordering::Relaxed);
}

/// One aggregate call of a definition's fields, over the group's partials.
#[derive(Debug, Clone, PartialEq)]
enum Agg {
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
struct Arg {
    /// The argument over the image populated as the source's row type `s`.
    sql: String,
    /// Whether a `SUM` or `AVG` reads it, so it has a `__sum_<i>`.
    summed: bool,
}

/// What a group-basis definition's target looks like ([`route`]).
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct GroupShape {
    /// The `GROUP BY` columns: the source's, the target's and the hidden
    /// table's column of that name.
    group_cols: Vec<String>,
    /// Each `GROUP BY` column's type in the target and hidden table.
    group_types: Vec<String>,
    args: Vec<Arg>,
    aggs: Vec<Agg>,
    /// Each value field: its name, its type in the target, and its
    /// expression with each aggregate call replaced by its placeholder
    /// column ([`placeholder`]).
    fields: Vec<(String, String, Expr)>,
    /// The `numeric` source columns a `SUM` or `AVG` reads, whose declared
    /// scale define checks ([`check_define`]).
    numeric_args: Vec<String>,
}

/// The column an aggregate call is read from inside a field's expression.
fn placeholder(k: usize) -> String {
    format!("__trellis_a{k}")
}

/// Whether the group basis takes `def` (see the module doc), and its shape
/// if so. `None` whenever the knob is off. The one routing decision between
/// the ledger and the group basis: `super::ledger::route` declines what this
/// takes.
pub(crate) fn route(
    def: &TransformDef,
    source_columns: &HashMap<String, ValueType>,
) -> Option<GroupShape> {
    if !enabled() {
        return None;
    }
    shape_of(def, source_columns)
}

/// [`route`]'s shape of `def`, whatever the knob says.
fn shape_of(def: &TransformDef, source_columns: &HashMap<String, ValueType>) -> Option<GroupShape> {
    let KeySpace::Aggregate { group_by } = &def.key_space else {
        return None;
    };
    if !crate::defs::eval::relationship_references(def).is_empty() {
        return None;
    }
    let mut group_cols = Vec::with_capacity(group_by.len());
    let mut group_types = Vec::with_capacity(group_by.len());
    for key in group_by {
        let GroupByKey::Column(column) = key else {
            return None;
        };
        let value_type = source_columns.get(column)?;
        group_cols.push(key.target_column_name().to_string());
        group_types.push(ddl::pg_type_name(*value_type).into_owned());
    }
    let substituted = crate::defs::backfill::substituted_field_exprs(def).ok()?;
    let field_types =
        crate::defs::validate::infer_field_types(def, source_columns, &HashMap::new()).ok()?;
    let contributions = schema::contributions(&def.fields, group_by, &substituted);
    let mut shape = GroupShape {
        group_cols,
        group_types,
        args: contributions
            .iter()
            .map(|c| Arg {
                sql: render_to_one_rel_expr_sql(&c.arg, "s"),
                summed: false,
            })
            .collect(),
        aggs: Vec::new(),
        fields: Vec::new(),
        numeric_args: Vec::new(),
    };
    let arg_of = |arg: &Expr| contributions.iter().position(|c| c.arg == *arg);
    for field in &def.fields {
        if group_by_contains(group_by, &field.name) {
            continue;
        }
        let expr = rewrite(
            &substituted[&field.name],
            source_columns,
            &arg_of,
            &mut shape,
        )?;
        let pg_type = ddl::pg_type_name(*field_types.get(&field.name)?).into_owned();
        shape.fields.push((field.name.clone(), pg_type, expr));
    }
    Some(shape)
}

/// `expr` with each aggregate call replaced by its placeholder column, the
/// call added to `shape.aggs`; `None` when some call isn't admitted.
fn rewrite(
    expr: &Expr,
    source_columns: &HashMap<String, ValueType>,
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
                    let value_type = crate::defs::validate::infer_expr_type(
                        arg,
                        source_columns,
                        &HashMap::new(),
                    )
                    .ok()?;
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
                .map(|a| rewrite(a, source_columns, arg_of, shape))
                .collect::<Option<Vec<_>>>()?,
        }),
        Expr::BinaryOp { op, lhs, rhs } => Some(Expr::BinaryOp {
            op: *op,
            lhs: Box::new(rewrite(lhs, source_columns, arg_of, shape)?),
            rhs: Box::new(rewrite(rhs, source_columns, arg_of, shape)?),
        }),
        Expr::RelationshipPath { .. } => None,
        other => Some(other.clone()),
    }
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
fn hidden_table_ident(schema: &str, id: i64) -> String {
    format!(
        "{}.{}",
        quote_ident(schema),
        quote_ident(&hidden_table_name(id))
    )
}

/// The `create table` statement of definition `id`'s hidden table in the
/// catalog schema `schema` (see the module doc). Created empty with the
/// definition's row, and dropped with it ([`drop_hidden_table_sql`]).
pub(crate) fn hidden_table_ddl(schema: &str, id: i64, shape: &GroupShape) -> String {
    let mut columns: Vec<String> = shape
        .group_cols
        .iter()
        .zip(&shape.group_types)
        .map(|(c, t)| format!("{} {t}", quote_ident(c)))
        .collect();
    columns.push("__n bigint not null default 0".to_string());
    for (i, arg) in shape.args.iter().enumerate() {
        columns.push(format!("__count_{} bigint not null default 0", i + 1));
        if arg.summed {
            columns.push(format!("__sum_{} numeric not null default 0", i + 1));
        }
    }
    columns.push("__basis pg_snapshot".to_string());
    columns.push("__stamp bigint not null default 0".to_string());
    columns.push(format!(
        "unique nulls not distinct ({})",
        quoted(&shape.group_cols).join(", ")
    ));
    format!(
        "create table {} ({})",
        hidden_table_ident(schema, id),
        columns.join(", ")
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

/// Define's checks of a definition [`route`] takes, in its transaction:
/// the module doc's refusals that need the catalog. `Err` names the reason.
pub(crate) async fn check_define(
    txn: &Transaction<'_>,
    shape: &GroupShape,
    qualified_source: &str,
) -> Result<(), crate::defs::catalog::CatalogError> {
    let refuse =
        |reason: String| Err(crate::defs::catalog::CatalogError::GroupBasisUnsupported { reason });
    if crate::defs::partition_tree::is_partitioned_root(txn, qualified_source).await? {
        return refuse(format!(
            "its source {qualified_source} is partitioned (#1103)"
        ));
    }
    for column in &shape.numeric_args {
        let typmod: Option<i32> = txn
            .query_opt(
                "select atttypmod from pg_catalog.pg_attribute \
                 where attrelid = pg_catalog.to_regclass($1) and attname = $2 \
                   and not attisdropped",
                &[&ddl::regclass_arg(qualified_source), column],
            )
            .await?
            .map(|row| row.get(0));
        if typmod == Some(-1) {
            return refuse(format!(
                "SUM/AVG({column}) is over a numeric with no declared scale (#1096 decision 2)"
            ));
        }
    }
    Ok(())
}

fn quoted(columns: &[String]) -> Vec<String> {
    columns.iter().map(|c| quote_ident(c)).collect()
}

/// One page's work on one group-basis target ([`apply_group_target`]).
#[derive(Debug, Clone)]
pub(crate) struct GroupTargetPlan {
    /// The target, qualified and unquoted (`transform_definitions.target_table`).
    pub(crate) target: String,
    target_ident: String,
    hidden_ident: String,
    /// The session's temporary table the page's sides are summed into
    /// before any group is locked ([`stage_sides`]), quoted.
    staged: String,
    /// The source, quoted and qualified: the row type images populate.
    source_row: String,
    /// The target's identity, its `GROUP BY` columns.
    identity: Vec<PrimaryKeyColumn>,
    shape: GroupShape,
    /// Every ring row of every change the page has for this target.
    sides: Vec<String>,
    hop_gen: i32,
    src_changed: Option<SystemTime>,
    origin_lsn: Option<PgLsn>,
    pushed: bool,
}

impl GroupTargetPlan {
    pub(crate) fn new(
        catalog_schema: &str,
        definition_id: i64,
        target: &str,
        source: &str,
        identity: Vec<PrimaryKeyColumn>,
        shape: GroupShape,
    ) -> Self {
        Self {
            target: target.to_string(),
            target_ident: ddl::qualified_target_table_ident(target),
            hidden_ident: hidden_table_ident(catalog_schema, definition_id),
            staged: format!(
                "pg_temp.{}",
                quote_ident(&format!("trellis_group_sides_{definition_id}"))
            ),
            source_row: ddl::qualified_source_table(source),
            identity,
            shape,
            sides: Vec::new(),
            hop_gen: 0,
            src_changed: None,
            origin_lsn: None,
            pushed: false,
        }
    }

    /// Adds one folded change's ring rows. An image-less change (a
    /// Re-derive) can't be applied as deltas: see the module doc.
    pub(crate) fn push(&mut self, change: &FoldedChange) -> Result<(), ApplyError> {
        if change.has_recompute {
            return Err(self.unsupported(
                "an image-less change (a Re-derive, a released key, or a catch-up); \
                 the group basis applies captured images only until #1101/#1102",
            ));
        }
        if change.group_sides.is_empty() {
            return Err(self.unsupported(
                "a page folded while the group-basis knob was off; the knob must not change \
                 while a definition exists",
            ));
        }
        self.sides.extend(change.group_sides.iter().cloned());
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
        Ok(())
    }

    fn unsupported(&self, what: &'static str) -> ApplyError {
        ApplyError::GroupBasisUnsupported {
            target: self.target.clone(),
            what,
        }
    }

    /// The CTEs every statement starts with: `raw`, one parsed ring row per
    /// element of `$1`, and `sides`, its old and new sides with their
    /// transaction, `lsn` and sign.
    fn sides_sql() -> &'static str {
        "raw as (select x::jsonb as r from unnest($1::text[]) as u(x)), \
         sides as ( \
             select (r ->> 't')::xid8 as __txid, (r ->> 'l')::pg_lsn as __lsn, -1 as __sgn, \
                    case when jsonb_typeof(r -> 'x') = 'object' then r -> 'x' -> 'old' \
                         else r -> 'o' end as __i \
             from raw \
             union all \
             select (r ->> 't')::xid8, (r ->> 'l')::pg_lsn, 1, \
                    case when jsonb_typeof(r -> 'x') = 'object' then r -> 'x' -> 'new' \
                         else r -> 'n' end \
             from raw \
         )"
    }

    /// `s.<group col>::<type> as <group col>`, for each `GROUP BY` column.
    fn group_values(&self) -> Vec<String> {
        self.shape
            .group_cols
            .iter()
            .zip(&self.shape.group_types)
            .map(|(c, t)| {
                let c = quote_ident(c);
                format!("s.{c}::{t} as {c}")
            })
            .collect()
    }

    /// The lookup of the row of `table` (quoted) whose group is `alias`'s,
    /// `NULL` matching `NULL`, returning `columns` (over `x`): plain equality,
    /// which the group identity's index serves, and the null-matching form
    /// behind a one-time test that the group has a `NULL`.
    fn group_lookup(&self, table: &str, alias: &str, columns: &str) -> String {
        let cols = quoted(&self.shape.group_cols);
        let equal: Vec<String> = cols
            .iter()
            .map(|c| format!("x.{c} = {alias}.{c}"))
            .collect();
        let has_null: Vec<String> = cols
            .iter()
            .map(|c| format!("{alias}.{c} is null"))
            .collect();
        let same: Vec<String> = cols
            .iter()
            .map(|c| format!("x.{c} is not distinct from {alias}.{c}"))
            .collect();
        format!(
            "(select {columns} from {table} x where {} \
              union all \
              select {columns} from {table} x where ({}) and {} \
              limit 1)",
            equal.join(" and "),
            has_null.join(" or "),
            same.join(" and ")
        )
    }

    /// The `create` of [`Self::staged`], one row per (group, transaction,
    /// statement) of the page with each partial's signed sum. Created once a
    /// session; its rows go with each page's transaction.
    fn staged_ddl(&self) -> String {
        let mut columns: Vec<String> = self
            .shape
            .group_cols
            .iter()
            .zip(&self.shape.group_types)
            .map(|(c, t)| format!("{} {t}", quote_ident(c)))
            .collect();
        columns.push("__txid xid8".to_string());
        columns.push("__lsn pg_lsn".to_string());
        columns.push("__n bigint".to_string());
        for (i, arg) in self.shape.args.iter().enumerate() {
            columns.push(format!("__c{} bigint", i + 1));
            if arg.summed {
                columns.push(format!("__s{} numeric", i + 1));
            }
        }
        format!(
            "create temp table if not exists {} ({}) on commit delete rows",
            self.staged.trim_start_matches("pg_temp."),
            columns.join(", ")
        )
    }

    /// Step 0's statement ([`stage_sides`]): parses `$1`'s ring rows, splits
    /// them into sides, evaluates the group and each argument on each, and
    /// sums them per (group, transaction, statement) into [`Self::staged`].
    /// Returns the number of ring rows it can't apply as deltas; when there
    /// are any it stages nothing.
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
        format!(
            "with {sides}, \
             bad as ( \
                 select count(*) as __bad from raw \
                 where r ->> 'op' = 'recompute' \
                    or (jsonb_typeof(r -> 'x') is distinct from 'object' \
                        and ((r ->> 'op' in ('update', 'delete') \
                              and jsonb_typeof(r -> 'o') is distinct from 'object') \
                          or (r ->> 'op' in ('insert', 'update') \
                              and jsonb_typeof(r -> 'n') is distinct from 'object'))) \
             ), \
             ev as ( \
                 select {values} \
                 from sides sd cross join lateral jsonb_populate_record(null::{src}, sd.__i) as s \
                 where jsonb_typeof(sd.__i) = 'object' \
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
        )
    }

    /// Step 1's statement ([`lock_groups`]): inserts every group the staged
    /// sides name that has no hidden row, and locks the rest, sorted.
    fn lock_sql(&self) -> String {
        let cols = quoted(&self.shape.group_cols).join(", ");
        format!(
            "insert into {hidden} ({cols}) \
             select distinct {cols} from {staged} \
             order by {cols} \
             on conflict ({cols}) do update set __n = excluded.__n where false",
            hidden = self.hidden_ident,
            staged = self.staged,
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
        let cols = quoted(&self.shape.group_cols);
        let col_list = cols.join(", ");
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
        let matched: Vec<String> = cols
            .iter()
            .map(|c| format!("array[sg.{c}] = array[b.{c}]"))
            .collect();
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
        // before this page's change.
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
            "with sg as ( \
                 select {col_list}, __txid, {sums} from {staged} \
                 where __lsn > coalesce( \
                     (select floor from ledger_truncate_floor where target_table = $1), \
                     '0/0'::pg_lsn) \
                 group by {col_list}, __txid \
             ), \
             pg as (select distinct {col_list} from sg), \
             b as ( \
                 select pg.*, h.__hc, h.__basis from pg \
                 cross join lateral {lookup_hidden} as h(__hc, __basis) \
             ), \
             f as ( \
                 select sg.*, b.__hc from sg join b on {matched} \
                 where b.__basis is null or not pg_visible_in_snapshot(sg.__txid, b.__basis) \
             ), \
             t as (select __hc, {totals} from f group by __hc), \
             up as ( \
                 update {hidden} u set {sets} from t \
                 where u.ctid = t.__hc and ({nonzero}) \
                 returning {up_groups}, {returning} \
             ), \
             vis as ( \
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
            staged = self.staged,
            sums = sums.join(", "),
            lookup_hidden = self.group_lookup(&self.hidden_ident, "pg", "x.ctid, x.__basis"),
            matched = matched.join(" and "),
            totals = totals.join(", "),
            hidden = self.hidden_ident,
            sets = sets.join(", "),
            nonzero = nonzero.join(" or "),
            up_groups = up_groups.join(", "),
            returning = returning.join(", "),
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
const PLAN_SETTINGS: &str = "set local enable_nestloop = off";
const PLAN_RESET: &str = "set local enable_nestloop to default";

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
/// one sorted statement, every group a side of the page names, creating the
/// hidden rows of those that have none.
pub(crate) async fn lock_groups(
    txn: &Transaction<'_>,
    plan: &GroupTargetPlan,
) -> Result<(), ApplyError> {
    stage_sides(txn, plan).await?;
    txn.execute(&plan.lock_sql(), &[]).await?;
    Ok(())
}

/// Step 0 of a page on a group-basis target: sums the page's sides per
/// (group, transaction, statement) into the session's staging table, before
/// any group is locked. Parsing and evaluating the images is most of a
/// page's work, and none of it reads a basis, so it stays out of the time
/// the page holds its groups.
async fn stage_sides(txn: &Transaction<'_>, plan: &GroupTargetPlan) -> Result<(), ApplyError> {
    txn.batch_execute(&plan.staged_ddl()).await?;
    let bad: i64 = txn
        .query_one(&plan.stage_sql(), &[&plan.sides])
        .await?
        .get(0);
    if bad > 0 {
        return Err(plan.unsupported(
            "a ring row without the images its change needs (an image-less delete, or a \
             Re-derive); the group basis applies captured images only",
        ));
    }
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
    let image_columns = mutations.image_columns(txn, &plan.target).await?;
    let sql = plan.apply_sql(image_columns.as_deref());
    // One index lookup per group (`b`), not per (group, transaction): a
    // page of 500 transactions over 4,000 groups has up to 100k of those.
    // Every other join is between the page's own CTEs, whose sizes the
    // planner can't see (`unnest` of a parameter): as a nested loop it is
    // quadratic in the page, so it's planned without one. A lateral lookup
    // is a nested loop whatever the setting.
    txn.batch_execute(PLAN_SETTINGS).await?;
    let rows = txn.query(&sql, &[&plan.target]).await?;
    txn.batch_execute(PLAN_RESET).await?;
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
    Ok((applied, written, deleted))
}

/// One page's work on one group-basis target: [`lock_groups`], the
/// interleaving tests' pause point, then [`apply_locked_groups`]. Returns the
/// visible rows written and deleted.
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
    let (_, written, deleted) = apply_locked_groups(txn, plan, mutations).await?;
    #[cfg(any(test, feature = "test-util"))]
    super::interleave::pause_at(
        txn,
        super::interleave::PausePoint::AfterGroupUpsert,
        &plan.target,
    )
    .await?;
    Ok((written, deleted))
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
        shape_of(&def, &columns())
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
    fn leaves_every_other_aggregate_on_the_ledger() {
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
                    summed: true
                },
                Arg {
                    sql: "s.\"b\"".to_string(),
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
             __count_2 bigint not null default 0, __basis pg_snapshot, \
             __stamp bigint not null default 0, unique nulls not distinct (\"g\"))"
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
