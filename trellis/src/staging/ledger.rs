//! Plain aggregate targets on the ledger (#623 parts D3 and D4; epic #556,
//! ADR-0002 invariants I1, I2, I3 and I5).
//!
//! # Which targets
//!
//! [`route`] sends an aggregate target here iff it reads no relationship,
//! every `GROUP BY` key is a plain source column, no aggregate argument reads
//! a `json`/`jsonb` column, and no `MIN`/`MAX` orders text (refused until
//! #575). (The grammar has no definition filter yet.) Its fields are of two
//! kinds:
//!
//! - **Maintained**: `SUM`/`AVG` over an exact numeric argument, and
//!   `COUNT`, kept by increments in the upsert.
//! - **Recomputed**: every other field (`MIN`/`MAX`, `BOOL_AND`/`BOOL_OR`,
//!   a float `SUM`/`AVG`, a composed field such as `SUM(a) + COUNT(b)`),
//!   rewritten over the ledger's columns ([`crate::defs::ledger::over_ledger`])
//!   and recomputed from each written group's live entries after the upsert
//!   ([`recompute_statement`]).
//!
//! An argument may be an expression (`SUM(v + 1)`): its value is evaluated
//! per change, over the image as the source's row type, into its ledger
//! column. Relationship-fed targets stay on `super::apply_aggregate`'s path,
//! with its pre-lock, probe and recompute horizons, until #623 D5 moves them.
//! A target is entirely on one path or the other.
//!
//! # The ledger
//!
//! `<target>__ledger` ([`crate::defs::ledger`], D2) holds one entry per source
//! key: the key's group, whether it is a live member, one contribution per
//! aggregate argument, and the ordering state (`applied_lsn`, `applied_seg`,
//! `basis`, `tombstone`). A group row is the sum of its live members'
//! entries, plus the hidden member count `__trellis_members`, and is deleted
//! when every accumulator on it (members, each count, each sum) is 0 (I3;
//! #625 F1's B5, see [`finish_groups`]). The one-pass build writes both.
//!
//! A Re-derive build's chunks (`super::build`, #625 F1) rewrite entries with
//! [`lock_entries`] and [`chunk_statement`] but leave the groups to the
//! merger ([`merge_statement`]), which upserts their deltas with the same
//! [`group_upsert_sql`] a page uses.
//!
//! # One page, one target ([`apply_ledger_target`])
//!
//! 1. **Lock** (I1, I5, [`lock_entries`]): insert a non-member placeholder
//!    for every key of the page that has no entry, then `select … for
//!    update` every entry, sorted by key, in one statement. A tombstone
//!    collected between the two (`super::retire::collect_tombstones`, #623
//!    D7) leaves its key with no entry to lock, which fails the page with a
//!    transient error: it rolls back and retries, inserting that key's
//!    placeholder afresh (#712).
//! 2. **Re-derive read**: a record staged as a `recompute` (or folded with
//!    one) is a Re-derive, and so is one with no change identity. One
//!    statement reads those keys' source rows *and* `pg_current_snapshot()`,
//!    after the lock. A key with no row is a tombstone.
//! 3. **One statement** updates the entries, aggregates the moves between
//!    each updated entry's old and new state into per-group increments, and
//!    upserts them in group order (I5), returning each written group:
//!    - a Re-derive writes the entry from its read and sets `basis` to the
//!      read's snapshot, leaving `applied_lsn` alone (the D split's Q1);
//!    - an Apply writes the entry from the change's new image (a delete
//!      makes a tombstone) and sets `applied_lsn`, but only if the change is
//!      not visible in the entry's `basis`, is newer than its `applied_lsn`,
//!      and is above the target's truncate floor (I2, Q6).
//! 4. The recomputed fields of every group the page kept are rewritten from
//!    its live entries.
//! 5. Groups whose every accumulator reached 0 are deleted.
//!
//! Every written or deleted group reaches the target-mutation seam with its
//! prior image, rebuilt from the upsert's result minus the increments (PG 17
//! has no `OLD` in `RETURNING`), or none for a group the upsert created. A
//! recomputed field's prior is the upsert's result itself, which the
//! recompute has not yet rewritten.
//!
//! Contributions and group keys are cast from an image's text by
//! `jsonb_populate_record` over the ledger's own row type, the same text
//! every other image holds, so the ledger's types decide the casts. An
//! expression argument is evaluated over the image populated as the source
//! table's row type instead.

use std::collections::HashMap;
use std::time::SystemTime;

use tokio_postgres::Transaction;
use tokio_postgres::types::PgLsn;

use crate::defs::ast::{
    Expr, FieldDef, GroupByKey, KeySpace, TransformDef, ValueType, group_by_contains,
};
use crate::defs::backfill::{FieldKind, classify_field};
use crate::defs::ddl::{self, PrimaryKeyColumn};
use crate::defs::eval;
use crate::defs::ledger as schema;
use crate::defs::oracle::render_expr_sql;
use crate::defs::pg_type::PgType;
use crate::pool::{quote_ident, quote_literal};

use super::apply::ApplyError;
use super::fold::{FoldedChange, earliest_origin};
use super::target_mutations::TargetMutations;

/// One target column the ledger path maintains, besides its `GROUP BY`
/// columns and `__trellis_members`.
#[derive(Debug, Clone, PartialEq)]
enum LedgerField {
    /// `SUM(x)`: `column` holds the sum of the non-null contributions of
    /// argument `arg`, and is `NULL` when its hidden count `count` is 0.
    Sum {
        column: String,
        count: String,
        arg: usize,
    },
    /// `COUNT(*)`: the group's member count.
    CountStar { column: String },
    /// `COUNT(x)`: the count of argument `arg`'s non-null contributions.
    CountArg { column: String, arg: usize },
    /// `AVG(x)`: `column` is the hidden running sum `sum` over the hidden
    /// count `count` (shared with a `SUM(x)`, issue #48) as `numeric`, which
    /// is Postgres's own `avg()` over an exact numeric argument, and `NULL`
    /// when `count` is 0.
    Avg {
        column: String,
        sum: String,
        count: String,
        arg: usize,
    },
    /// Any other field (`MIN`/`MAX`, `BOOL_AND`/`BOOL_OR`, a float
    /// `SUM`/`AVG`, a composed field): recomputed from the group's live
    /// member entries after the upsert ([`recompute_statement`]). `sql` is the
    /// field over the ledger's columns ([`schema::over_ledger`]), the
    /// expression the one-pass build writes it with.
    Recompute { column: String, sql: String },
}

/// Where a contribution's value comes from in a change's image.
#[derive(Debug, Clone, PartialEq)]
enum ContribSource {
    /// A plain source column: the image's text for it, cast by
    /// `jsonb_populate_record` over the ledger's row type.
    Column(String),
    /// An expression argument (`SUM(v + 1)`): `sql` over the image cast to
    /// the source's own row type (alias `s`), as the build computes it over
    /// the source table. `reads` are the source columns it reads.
    Expr { sql: String, reads: Vec<String> },
}

impl ContribSource {
    /// The source columns a Re-derive must read for it.
    fn reads(&self) -> &[String] {
        match self {
            ContribSource::Column(column) => std::slice::from_ref(column),
            ContribSource::Expr { reads, .. } => reads,
        }
    }
}

/// One contribution column of a ledger-routed target.
#[derive(Debug, Clone, PartialEq)]
struct Contrib {
    /// The ledger column (`__arg<n>`).
    column: String,
    source: ContribSource,
    /// Whether a `SUM` or `AVG` maintained by increments sums it. Any other
    /// argument needs only its NULL-ness for the increments, so it may be of
    /// any type and gets no sum increment.
    summed: bool,
}

/// What a ledger-routed definition's target looks like ([`route`]).
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct LedgerShape {
    /// The `GROUP BY` columns: each is a source column of the same name, and
    /// the target's and the ledger's column of that name.
    group_cols: Vec<String>,
    /// The contribution columns, in [`schema::contributions`]' order.
    contribs: Vec<Contrib>,
    fields: Vec<LedgerField>,
}

impl LedgerShape {
    /// Per contribution, in order, whether a maintained `SUM`/`AVG` sums it:
    /// the contributions whose group-delta row carries a sum increment
    /// ([`schema::aggregate_deltas_ddl`]).
    pub(crate) fn summed(&self) -> Vec<bool> {
        self.contribs.iter().map(|c| c.summed).collect()
    }

    pub(super) fn recomputes(&self) -> bool {
        self.fields
            .iter()
            .any(|f| matches!(f, LedgerField::Recompute { .. }))
    }

    /// Whether a Re-derive build serves this shape yet (#625 F2): every
    /// field is maintained by increments (no recompute) over plain source
    /// columns (no expression argument). #625 F5 widens it to the rest of
    /// what [`route`] takes.
    pub(super) fn rederive_buildable(&self) -> bool {
        !self.recomputes()
            && self
                .contribs
                .iter()
                .all(|c| matches!(c.source, ContribSource::Column(_)))
    }
}

/// Whether `def` is a target the ledger path maintains (see the module doc),
/// and its shape if so.
pub(crate) fn route(
    def: &TransformDef,
    source_columns: &HashMap<String, ValueType>,
) -> Option<LedgerShape> {
    let KeySpace::Aggregate { group_by } = &def.key_space else {
        return None;
    };
    if !eval::relationship_references(def).is_empty() {
        return None;
    }
    let mut group_cols = Vec::with_capacity(group_by.len());
    for key in group_by {
        let GroupByKey::Column(column) = key else {
            return None;
        };
        if !source_columns.contains_key(column) {
            return None;
        }
        group_cols.push(column.clone());
    }
    let substituted = crate::defs::backfill::substituted_field_exprs(def).ok()?;
    let no_relationships = HashMap::new();
    let field_types =
        crate::defs::validate::infer_field_types(def, source_columns, &no_relationships).ok()?;
    let value_fields: Vec<&FieldDef> = def
        .fields
        .iter()
        .filter(|f| !group_by_contains(group_by, &f.name))
        .collect();
    let kinds: Vec<FieldKind> = value_fields
        .iter()
        .map(|f| classify_field(&substituted[&f.name], field_types[&f.name]))
        .collect();
    // `MIN`/`MAX` over text stays on the old path (the handoff's scope for
    // D4; #575 revisits text ordering).
    for field in &value_fields {
        if orders_text(&substituted[&field.name], source_columns) {
            return None;
        }
    }
    let contributions = schema::contributions(&def.fields, group_by, &substituted);
    let summed = |arg: &Expr| {
        value_fields.iter().zip(&kinds).any(|(f, kind)| {
            matches!(kind, FieldKind::Sum | FieldKind::Avg)
                && matches!(&substituted[&f.name], Expr::FunctionCall { args, .. }
                    if args.first() == Some(arg))
        })
    };
    let mut contribs = Vec::with_capacity(contributions.len());
    for contribution in &contributions {
        let mut reads = Vec::new();
        source_reads(&contribution.arg, &mut reads);
        // An image holds a column's output text, which
        // `jsonb_populate_record` would store in a `json`/`jsonb` column as a
        // JSON string, not the value the build reads. Every other type goes
        // through its input function.
        for column in &reads {
            if matches!(
                source_columns.get(column)?,
                ValueType::Other(PgType::Json | PgType::Jsonb)
            ) {
                return None;
            }
        }
        let source = match &contribution.arg {
            Expr::Column(column) => ContribSource::Column(column.clone()),
            arg => ContribSource::Expr {
                sql: crate::defs::oracle::render_to_one_rel_expr_sql(arg, "s"),
                reads,
            },
        };
        contribs.push(Contrib {
            column: contribution.column.clone(),
            source,
            summed: summed(&contribution.arg),
        });
    }
    let arg_of = |arg: &Expr| -> usize {
        contributions
            .iter()
            .position(|c| c.arg == *arg)
            .expect("every aggregate argument has a contribution column")
    };
    let count_cols = ddl::count_column_names(&def.fields, &substituted);
    let fields = value_fields
        .iter()
        .zip(kinds)
        .map(|(field, kind)| {
            let expr = &substituted[&field.name];
            let column = field.name.clone();
            match (kind, expr) {
                (FieldKind::Sum, Expr::FunctionCall { args, .. }) => LedgerField::Sum {
                    count: count_cols[&field.name].clone(),
                    arg: arg_of(&args[0]),
                    column,
                },
                (FieldKind::Avg, Expr::FunctionCall { args, .. }) => LedgerField::Avg {
                    sum: ddl::avg_sum_column(&field.name),
                    count: count_cols[&field.name].clone(),
                    arg: arg_of(&args[0]),
                    column,
                },
                (FieldKind::Count, Expr::FunctionCall { args, .. }) if args.is_empty() => {
                    LedgerField::CountStar { column }
                }
                (FieldKind::Count, Expr::FunctionCall { args, .. }) => LedgerField::CountArg {
                    arg: arg_of(&args[0]),
                    column,
                },
                _ => LedgerField::Recompute {
                    sql: render_expr_sql(&schema::over_ledger(expr, &contributions, group_by)),
                    column,
                },
            }
        })
        .collect();
    Some(LedgerShape {
        group_cols,
        contribs,
        fields,
    })
}

/// Whether `expr` has a `MIN`/`MAX` over a text argument.
fn orders_text(expr: &Expr, source_columns: &HashMap<String, ValueType>) -> bool {
    match expr {
        Expr::FunctionCall { name, args }
            if (name == "MIN" || name == "MAX") && args.len() == 1 =>
        {
            matches!(
                crate::defs::validate::infer_expr_type(&args[0], source_columns, &HashMap::new()),
                Ok(ValueType::Text)
            )
        }
        Expr::FunctionCall { args, .. } => args.iter().any(|a| orders_text(a, source_columns)),
        Expr::BinaryOp { lhs, rhs, .. } => {
            orders_text(lhs, source_columns) || orders_text(rhs, source_columns)
        }
        _ => false,
    }
}

/// The source columns `expr` reads, appended to `out` once each.
fn source_reads(expr: &Expr, out: &mut Vec<String>) {
    match expr {
        Expr::Column(column) => {
            if !out.contains(column) {
                out.push(column.clone());
            }
        }
        Expr::FunctionCall { args, .. } => args.iter().for_each(|a| source_reads(a, out)),
        Expr::BinaryOp { lhs, rhs, .. } => {
            source_reads(lhs, out);
            source_reads(rhs, out);
        }
        Expr::NumberLiteral(_)
        | Expr::StringLiteral(_)
        | Expr::TypedLiteral { .. }
        | Expr::RelationshipPath { .. } => {}
    }
}

/// One folded record for a ledger target.
#[derive(Debug, Clone)]
struct LedgerRecord {
    key: String,
    /// `None`: a Re-derive. `Some`: an Apply of the record's last change,
    /// with its `lsn`, `row_txid` and new image (`None` for a delete).
    apply: Option<(PgLsn, String, Option<String>)>,
    hop_gen: i32,
    src_changed: Option<SystemTime>,
    origin_lsn: Option<PgLsn>,
}

/// One page's work on one ledger target ([`apply_ledger_target`]).
#[derive(Debug, Clone)]
pub(crate) struct LedgerTargetPlan {
    /// The target's qualified identity (`transform_definitions.target_table`).
    pub(crate) target: String,
    target_ident: String,
    ledger_ident: String,
    /// The target's group deltas (#625 F1), which only a build chunk and the
    /// merger ([`super::build`]) read or write.
    deltas_ident: String,
    /// The source's qualified identity, for the Re-derive read.
    source_table: String,
    source_pk: Vec<PrimaryKeyColumn>,
    /// The target's row identity (its `GROUP BY` columns), which renders a
    /// group's key for the seam exactly as a chained reader keys it.
    identity: Vec<PrimaryKeyColumn>,
    shape: LedgerShape,
    records: Vec<LedgerRecord>,
}

impl LedgerTargetPlan {
    pub(crate) fn new(
        target: &str,
        source_table: &str,
        source_pk: Vec<PrimaryKeyColumn>,
        identity: Vec<PrimaryKeyColumn>,
        shape: LedgerShape,
    ) -> Self {
        Self {
            target: target.to_string(),
            target_ident: ddl::qualified_target_table_ident(target),
            ledger_ident: ddl::qualified_target_table_ident(&schema::ledger_table_name(target)),
            deltas_ident: ddl::qualified_target_table_ident(&schema::deltas_table_name(target)),
            source_table: source_table.to_string(),
            source_pk,
            identity,
            shape,
            records: Vec::new(),
        }
    }

    /// The source's qualified identity.
    pub(super) fn source_table(&self) -> &str {
        &self.source_table
    }

    /// The source's primary key.
    pub(super) fn source_pk(&self) -> &[PrimaryKeyColumn] {
        &self.source_pk
    }

    /// Adds a Re-derive of source key `key`, with no provenance (the orphan
    /// sweep's, `intake::resume_orphans`).
    pub(crate) fn push_rederive(&mut self, key: String) {
        self.records.push(LedgerRecord {
            key,
            apply: None,
            hop_gen: 0,
            src_changed: None,
            origin_lsn: None,
        });
    }

    /// Adds one folded record: an Apply of its last change, or a Re-derive
    /// when it was staged as (or folded with) a `recompute`, or has no change
    /// to apply.
    pub(crate) fn push(&mut self, change: &FoldedChange) {
        let apply = match &change.last_change {
            Some(last) if !change.has_recompute => {
                Some((last.lsn, last.row_txid.clone(), change.new_image.clone()))
            }
            _ => None,
        };
        self.records.push(LedgerRecord {
            key: change.key.clone(),
            apply,
            hop_gen: change.hop_gen,
            src_changed: change.src_changed,
            origin_lsn: change.origin_lsn,
        });
    }
}

/// ADR-0002 I2 as SQL over the ledger entry `l`, the batch row `v` (its
/// `lsn` and `txid`) and the target's truncate floor `fl`: whether an Apply
/// changes the entry. `visibility` false renders the `lsn_only_skip` plant.
pub(super) fn apply_predicate(visibility: bool) -> String {
    let basis = quote_ident(schema::BASIS_COLUMN);
    let applied = quote_ident(schema::APPLIED_LSN_COLUMN);
    let seen = if visibility {
        format!("(l.{basis} is null or not pg_visible_in_snapshot(v.__txid, l.{basis})) and ")
    } else {
        String::new()
    };
    format!(
        "{seen}(l.{applied} is null or v.__lsn > l.{applied}) \
         and not exists (select 1 from fl where v.__lsn <= fl.floor)"
    )
}

/// The quoted `GROUP BY` columns and contribution columns of `shape`'s
/// entries: what a Re-derive or an Apply writes, and what a move carries.
fn entry_columns(shape: &LedgerShape) -> (Vec<String>, Vec<String>) {
    (
        shape.group_cols.iter().map(|c| quote_ident(c)).collect(),
        shape
            .contribs
            .iter()
            .map(|c| quote_ident(&c.column))
            .collect(),
    )
}

/// `alias.c` for each of `columns`, comma-joined.
fn prefixed(columns: &[String], alias: &str) -> String {
    columns
        .iter()
        .map(|c| format!("{alias}.{c}"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// The `old` CTE: the entries of the keys bound as `keys` (a `text[]`
/// parameter) as the statement found them, each with whether it counted as
/// a live member.
fn old_cte(plan: &LedgerTargetPlan, keys: &str) -> String {
    let (groups, args) = entry_columns(&plan.shape);
    let values: Vec<String> = groups.into_iter().chain(args).collect();
    format!(
        "old as ( \
             select l.{key} as __k, {l_cols}, l.{member} and not l.{tombstone} as __live \
             from {ledger} l where l.{key} = any({keys}::text[]) \
         )",
        key = quote_ident(schema::KEY_COLUMN),
        member = quote_ident(schema::MEMBER_COLUMN),
        tombstone = quote_ident(schema::TOMBSTONE_COLUMN),
        l_cols = prefixed(&values, "l"),
        ledger = plan.ledger_ident,
    )
}

/// What the `upd` CTE (an update of the ledger `l`) returns: each written
/// entry's key, values and whether it now counts as a live member.
fn upd_returning(plan: &LedgerTargetPlan) -> String {
    let (groups, args) = entry_columns(&plan.shape);
    let values: Vec<String> = groups.into_iter().chain(args).collect();
    format!(
        "l.{key} as __k, {l_cols}, l.{member} and not l.{tombstone} as __live",
        key = quote_ident(schema::KEY_COLUMN),
        member = quote_ident(schema::MEMBER_COLUMN),
        tombstone = quote_ident(schema::TOMBSTONE_COLUMN),
        l_cols = prefixed(&values, "l"),
    )
}

/// The `moves` and `d` CTEs over `old` and `upd`: each written entry's move
/// out of its old group and into its new one, summed per group into `d`'s
/// increments. `d` has the `GROUP BY` columns, `__gk` (the group's identity
/// key), `__dm` (members), per argument `i` `__dc<i>` (its non-null count)
/// and, for a summed one, `__ds<i>` (its sum), and `__ks` (the keys that
/// moved it). A group whose increments are all 0 is left out.
fn moves_and_deltas(plan: &LedgerTargetPlan) -> String {
    let shape = &plan.shape;
    let (groups, args) = entry_columns(shape);
    let values: Vec<String> = groups.iter().chain(&args).cloned().collect();

    // Per-group increments: `dm` members, and per argument `dc<i>` (its
    // non-null count) and, for a summed argument, `ds<i>` (its sum).
    let mut deltas = vec![format!("sum(m.__sign) as {}", schema::DELTA_MEMBERS_COLUMN)];
    let mut nonzero = vec!["sum(m.__sign) <> 0".to_string()];
    // A recomputed field can change with no increment changing (`MAX` of a
    // value moved from 10 to 50): the group is kept if any entry's values
    // changed at all. Compared as text, so `1.5` and `1.50` differ: a `MAX`
    // shows whichever spelling it picks.
    if shape.recomputes() {
        nonzero.push("bool_or(m.__rc)".to_string());
    }
    for (i, a) in args.iter().enumerate() {
        deltas.push(format!(
            "sum(case when m.{a} is null then 0 else m.__sign end) as {}",
            schema::delta_count_column(i)
        ));
        nonzero.push(format!(
            "sum(case when m.{a} is null then 0 else m.__sign end) <> 0"
        ));
        if !shape.contribs[i].summed {
            continue;
        }
        // Summed per sign and then subtracted, not `sum(__sign * a)`: the
        // product is computed in the argument's own type, so negating an
        // `integer`'s -2147483648 (or a `bigint`'s minimum) overflows it.
        // `sum` widens first (`integer` to `bigint`, `bigint` to `numeric`).
        let ds = format!(
            "(coalesce(sum(m.{a}) filter (where m.__sign > 0), 0) \
             - coalesce(sum(m.{a}) filter (where m.__sign < 0), 0))"
        );
        deltas.push(format!("{ds} as {}", schema::delta_sum_column(i)));
        nonzero.push(format!("{ds} <> 0"));
    }
    format!(
        "moves as ( \
             select {u_cols}, 1 as __sign, u.__k, {rc} as __rc \
             from upd u join old o on o.__k = u.__k where u.__live \
             union all \
             select {o_cols}, -1 as __sign, o.__k, {rc} as __rc \
             from old o join upd u on u.__k = o.__k where o.__live \
         ), \
         d as ( \
             select {m_groups}, {m_gk} as __gk, {deltas}, array_agg(m.__k) as __ks \
             from moves m group by {m_groups} \
             having {nonzero} \
         )",
        rc = format!(
            "row(o.__live, {})::text is distinct from row(u.__live, {})::text",
            prefixed(&values, "o"),
            prefixed(&values, "u")
        ),
        u_cols = prefixed(&values, "u"),
        o_cols = prefixed(&values, "o"),
        m_groups = prefixed(&groups, "m"),
        m_gk = ddl::pk_key_sql_expr(&plan.identity, Some("m")),
        deltas = deltas.join(", "),
        nonzero = nonzero.join(" or "),
    )
}

/// The group upsert over a preceding `d` CTE of per-group increments (as
/// [`moves_and_deltas`] renders it): the `up` CTE, and the query that
/// returns each written group's identity key, whether the upsert created
/// it, its `ctid`, `d.__ks`, its prior image for the seam (`null` with no
/// `image_columns`) and whether every accumulator on it is now 0 (#625 F1's
/// B5), in that order.
struct GroupUpsert {
    cte: String,
    select: String,
}

/// Renders [`GroupUpsert`] for `plan`. `racing` is the `drop_racing` plant's
/// filter on `d`, or empty.
fn group_upsert_sql(
    plan: &LedgerTargetPlan,
    image_columns: Option<&[String]>,
    racing: &str,
) -> GroupUpsert {
    let shape = &plan.shape;
    let q = |c: &str| quote_ident(c);
    let (groups, _) = entry_columns(shape);
    let members = q(ddl::MEMBERS_COLUMN);

    // The upsert's columns, inserted values, and conflict updates; each
    // maintained column's prior value over the upsert's result `up` and the
    // increments `d`; and the accumulators B5 reads.
    //
    // A `SUM` (or an `AVG`'s hidden running sum) is `NULL` when its hidden
    // count is 0 *and* the sum is 0, not on the count alone. Under one
    // channel the two agree: no non-null contribution sums to exactly 0.
    // While a Re-derive build runs, a group has two (Apply and the merger,
    // #625 F1), and the count can pass through 0 with the other channel's
    // sum still owed, which a `NULL` would lose (finding 5).
    let mut insert_cols = groups.clone();
    let mut insert_vals: Vec<String> = groups.iter().map(|c| format!("d.{c}")).collect();
    let mut updates = Vec::new();
    let mut priors: HashMap<String, String> = HashMap::new();
    let mut accumulators = vec![format!("t.{members} = 0")];
    for field in &shape.fields {
        match field {
            LedgerField::Sum { column, count, arg } => {
                let (f, c) = (q(column), q(count));
                let new_sum = format!("coalesce(t.{f}, 0) + coalesce(excluded.{f}, 0)");
                insert_cols.push(f.clone());
                insert_vals.push(format!(
                    "case when d.__dc{arg} = 0 and d.__ds{arg} = 0 then null else d.__ds{arg} end"
                ));
                updates.push(format!(
                    "{f} = case when t.{c} + excluded.{c} = 0 and {new_sum} = 0 then null \
                     else {new_sum} end"
                ));
                let old_sum = format!("coalesce(up.{f}, 0) - d.__ds{arg}");
                priors.insert(
                    column.clone(),
                    format!(
                        "case when up.{c} - d.__dc{arg} = 0 and {old_sum} = 0 then null \
                         else {old_sum} end"
                    ),
                );
                accumulators.push(format!("coalesce(t.{f}, 0) = 0"));
            }
            LedgerField::Avg {
                column,
                sum,
                count,
                arg,
            } => {
                // The running sum as `SUM`'s, and the visible column
                // `sum / count::numeric`: Postgres's `avg()` over an exact
                // numeric argument is `numeric_div` of the same two, and the
                // one-pass build writes the same expression.
                let (f, s, c) = (q(column), q(sum), q(count));
                let new_sum = format!("coalesce(t.{s}, 0) + coalesce(excluded.{s}, 0)");
                let new_count = format!("t.{c} + excluded.{c}");
                insert_cols.push(s.clone());
                insert_vals.push(format!(
                    "case when d.__dc{arg} = 0 and d.__ds{arg} = 0 then null else d.__ds{arg} end"
                ));
                insert_cols.push(f.clone());
                insert_vals.push(format!(
                    "case when d.__dc{arg} = 0 then null \
                     else d.__ds{arg} / d.__dc{arg}::numeric end"
                ));
                updates.push(format!(
                    "{s} = case when {new_count} = 0 and {new_sum} = 0 then null \
                     else {new_sum} end"
                ));
                updates.push(format!(
                    "{f} = case when {new_count} = 0 then null \
                     else ({new_sum}) / ({new_count})::numeric end"
                ));
                let (old_sum, old_count) = (
                    format!("coalesce(up.{s}, 0) - d.__ds{arg}"),
                    format!("up.{c} - d.__dc{arg}"),
                );
                priors.insert(
                    sum.clone(),
                    format!(
                        "case when {old_count} = 0 and {old_sum} = 0 then null \
                         else {old_sum} end"
                    ),
                );
                priors.insert(
                    column.clone(),
                    format!(
                        "case when {old_count} = 0 then null \
                         else ({old_sum}) / ({old_count})::numeric end"
                    ),
                );
                accumulators.push(format!("coalesce(t.{s}, 0) = 0"));
            }
            LedgerField::CountStar { column } => {
                let f = q(column);
                insert_cols.push(f.clone());
                insert_vals.push("d.__dm".to_string());
                updates.push(format!("{f} = t.{f} + excluded.{f}"));
                priors.insert(column.clone(), format!("up.{f} - d.__dm"));
                accumulators.push(format!("t.{f} = 0"));
            }
            LedgerField::CountArg { column, arg } => {
                let f = q(column);
                insert_cols.push(f.clone());
                insert_vals.push(format!("d.__dc{arg}"));
                updates.push(format!("{f} = t.{f} + excluded.{f}"));
                priors.insert(column.clone(), format!("up.{f} - d.__dc{arg}"));
                accumulators.push(format!("t.{f} = 0"));
            }
            // Written by `recompute_statement`; the upsert leaves the old
            // value, which is the prior.
            LedgerField::Recompute { .. } => {}
        }
    }
    // A `SUM`'s or `AVG`'s hidden count, once per shared column (issue #48).
    let mut counts_emitted: Vec<&str> = Vec::new();
    for field in &shape.fields {
        let (LedgerField::Sum { count, arg, .. } | LedgerField::Avg { count, arg, .. }) = field
        else {
            continue;
        };
        if counts_emitted.contains(&count.as_str()) {
            continue;
        }
        counts_emitted.push(count);
        let c = q(count);
        insert_cols.push(c.clone());
        insert_vals.push(format!("d.__dc{arg}"));
        updates.push(format!("{c} = t.{c} + excluded.{c}"));
        priors.insert(count.clone(), format!("up.{c} - d.__dc{arg}"));
        accumulators.push(format!("t.{c} = 0"));
    }
    insert_cols.push(members.clone());
    insert_vals.push("d.__dm".to_string());
    updates.push(format!("{members} = t.{members} + excluded.{members}"));
    priors.insert(
        ddl::MEMBERS_COLUMN.to_string(),
        format!("up.{members} - d.__dm"),
    );

    let prior_image = match image_columns {
        Some(columns) => {
            let pairs: Vec<String> = columns
                .iter()
                .map(|c| {
                    let value = priors
                        .get(c)
                        .cloned()
                        .unwrap_or_else(|| format!("up.{}", q(c)));
                    format!("{}, ({value})::text", quote_literal(c))
                })
                .collect();
            format!("jsonb_build_object({})::text", pairs.join(", "))
        }
        None => "null::text".to_string(),
    };
    GroupUpsert {
        cte: format!(
            "up as ( \
                 insert into {target} as t ({insert_cols}) \
                 select {insert_vals} from d{racing} order by {d_groups} \
                 on conflict ({group_cols}) do update set {updates} \
                 returning t.*, {t_gk} as __trellis_gk, (t.xmax = 0) as __trellis_inserted, \
                           t.ctid::text as __trellis_ctid, ({empty}) as __trellis_empty \
             )",
            target = plan.target_ident,
            insert_cols = insert_cols.join(", "),
            insert_vals = insert_vals.join(", "),
            d_groups = prefixed(&groups, "d"),
            group_cols = groups.join(", "),
            updates = updates.join(", "),
            t_gk = ddl::pk_key_sql_expr(&plan.identity, Some("t")),
            empty = accumulators.join(" and "),
        ),
        select: format!(
            "select up.__trellis_gk, up.__trellis_inserted, up.__trellis_ctid, d.__ks, \
                    {prior_image}, up.__trellis_empty \
             from up join d on {up_d}",
            // Each upserted group back to its increments by the group
            // columns' own equality, not by their text: equal values can
            // render differently (`numeric` `1.5` and `1.50`, `float` `0` and
            // `-0`), and the target row keeps whichever spelling created it.
            // A one-element array compares `NULL` equal to `NULL` (the `nulls
            // not distinct` key) and still hashes.
            up_d = groups
                .iter()
                .map(|c| format!("array[up.{c}] = array[d.{c}]"))
                .collect::<Vec<_>>()
                .join(" and "),
        ),
    }
}

/// The page's one ledger-and-groups statement (see the module doc, step 3).
/// Binds `$1` keys, `$2` Re-derive flags, `$3` `lsn`s, `$4` `row_txid`s,
/// `$5` images (all `text[]`, in key order), `$6` the Re-derive read's
/// snapshot, `$7` the page's segment and `$8` the target's identity.
/// `image_columns` are the seam's prior-image columns (`None`: no reader).
/// Returns [`GroupUpsert`]'s columns.
fn ledger_statement(
    plan: &LedgerTargetPlan,
    image_columns: Option<&[String]>,
    visibility: bool,
    drop_racing: bool,
) -> String {
    let shape = &plan.shape;
    let ledger = &plan.ledger_ident;
    let q = |c: &str| quote_ident(c);
    let (groups, args) = entry_columns(shape);
    let values: Vec<String> = groups.iter().chain(&args).cloned().collect();
    let member = q(schema::MEMBER_COLUMN);
    let tombstone = q(schema::TOMBSTONE_COLUMN);
    let key = q(schema::KEY_COLUMN);
    let basis = q(schema::BASIS_COLUMN);
    let applied = q(schema::APPLIED_LSN_COLUMN);
    let seg = q(schema::APPLIED_SEG_COLUMN);

    // An image's plain-column values keyed by ledger column, for
    // `jsonb_populate_record`, and each value `v` carries: the record's
    // column, or an expression argument computed over the image as a source
    // row `s`.
    let plain = shape
        .group_cols
        .iter()
        .map(|c| (c, c))
        .chain(shape.contribs.iter().filter_map(|c| match &c.source {
            ContribSource::Column(source) => Some((&c.column, source)),
            ContribSource::Expr { .. } => None,
        }));
    let doc: Vec<String> = plain
        .map(|(ledger_col, source_col)| {
            format!(
                "{}, b.__img -> {}",
                quote_literal(ledger_col),
                quote_literal(source_col)
            )
        })
        .collect();
    let mut v_cols: Vec<String> = groups.iter().map(|c| format!("r.{c}")).collect();
    let mut typed_row = String::new();
    for contrib in &shape.contribs {
        v_cols.push(match &contrib.source {
            ContribSource::Column(_) => format!("r.{}", q(&contrib.column)),
            ContribSource::Expr { sql, .. } => {
                typed_row = format!(
                    " cross join lateral jsonb_populate_record(null::{}, b.__img) s",
                    ddl::qualified_source_table(&plan.source_table)
                );
                format!("{sql} as {}", q(&contrib.column))
            }
        });
    }
    let set_values: Vec<String> = values.iter().map(|c| format!("{c} = v.{c}")).collect();

    // Planted bug (#557): drop the increments of a group another apply
    // transaction holds. See `crate::plant`.
    let racing = if drop_racing {
        " where pg_try_advisory_xact_lock(hashtext($8), hashtext(d.__gk))"
    } else {
        ""
    };
    let upsert = group_upsert_sql(plan, image_columns, racing);

    format!(
        "with b as ( \
             select u.__k, u.__rederive, u.__lsn::pg_lsn as __lsn, u.__txid::xid8 as __txid, \
                    u.__img::jsonb as __img \
             from unnest($1::text[], $2::bool[], $3::text[], $4::text[], $5::text[]) \
                  as u(__k, __rederive, __lsn, __txid, __img) \
         ), \
         v as ( \
             select b.__k, b.__rederive, b.__lsn, b.__txid, b.__img is not null as __present, {v_cols} \
             from b cross join lateral \
                  jsonb_populate_record(null::{ledger}, jsonb_build_object({doc})) r{typed_row} \
         ), \
         {old}, \
         fl as (select floor from ledger_truncate_floor where target_table = $8), \
         upd as ( \
             update {ledger} l set {set_values}, \
                 {member} = v.__present, {tombstone} = not v.__present, \
                 {basis} = case when v.__rederive then $6::text::pg_snapshot else l.{basis} end, \
                 {applied} = case when v.__rederive then l.{applied} else v.__lsn end, \
                 {seg} = greatest(l.{seg}, $7::bigint) \
             from v \
             where l.{key} = v.__k and (v.__rederive or ({predicate})) \
             returning {returning} \
         ), \
         {moves_and_deltas}, \
         {up} \
         {select}",
        v_cols = v_cols.join(", "),
        doc = doc.join(", "),
        old = old_cte(plan, "$1"),
        set_values = set_values.join(", "),
        predicate = apply_predicate(visibility),
        returning = upd_returning(plan),
        moves_and_deltas = moves_and_deltas(plan),
        up = upsert.cte,
        select = upsert.select,
    )
}

/// A build chunk's one read-and-write statement (#625 F1, the chunk
/// Re-derive; called from [`super::build`]): `pg_current_snapshot()`, the
/// active segment's `seg_seq` and the locked keys' source rows, read
/// together; the keys' entries rewritten from them (`basis` := the
/// snapshot, `applied_seg` raised to that segment, `applied_lsn` left alone,
/// a key with no row a tombstone); and the moves' per-group increments
/// appended to the target's group deltas. No group row is touched, and no
/// row data leaves the statement. Returns the entries written and the
/// delta rows appended.
///
/// `range_where` is the chunk's `(lo, hi]` predicate over the source's bare
/// key columns, binding `$1..$n`; `keys` is the locked keys' `text[]`
/// parameter (`$n+1`). Reading the range as well as the keys lets the read
/// scan the key's index for the chunk, and the keys keep a row inserted since
/// the keys were read out: its entry isn't locked, so its own change applies
/// it.
///
/// The segment stamp is what [`super::retire::collect_tombstones`] reads
/// (#625 finding 11): every change the snapshot sees was captured into the
/// highest segment it sees or an earlier one, so once that segment and all
/// before it are drained, no change the tombstone must outlast can arrive.
/// The next segment's row is inserted by the seal's phase 1
/// (`super::seal::seal_phase1`), and the sealed segment's fence is taken
/// only after that commits, so a snapshot that doesn't see the next segment
/// is older than the fence of the newest one it does see: that fence sees
/// every change the snapshot sees.
pub(super) fn chunk_statement(plan: &LedgerTargetPlan, range_where: &str, keys: &str) -> String {
    let shape = &plan.shape;
    let q = |c: &str| quote_ident(c);
    let (groups, args) = entry_columns(shape);
    let values: Vec<String> = groups.iter().chain(&args).cloned().collect();
    let k_expr = ddl::pk_key_sql_expr(&plan.source_pk, Some("s"));
    let mut src_cols: Vec<String> = groups.iter().map(|c| format!("s.{c} as {c}")).collect();
    for contrib in &shape.contribs {
        src_cols.push(match &contrib.source {
            ContribSource::Column(source) => format!("s.{} as {}", q(source), q(&contrib.column)),
            ContribSource::Expr { sql, .. } => format!("{sql} as {}", q(&contrib.column)),
        });
    }
    let set_values: Vec<String> = values.iter().map(|c| format!("{c} = v.{c}")).collect();
    let mut delta_cols = groups.clone();
    delta_cols.push(schema::DELTA_MEMBERS_COLUMN.to_string());
    for (i, contrib) in shape.contribs.iter().enumerate() {
        delta_cols.push(schema::delta_count_column(i));
        if contrib.summed {
            delta_cols.push(schema::delta_sum_column(i));
        }
    }
    format!(
        "with snap as ( \
             select pg_catalog.pg_current_snapshot() as __snap, \
                    (select max(seg_seq) from segments) as __seg \
         ), \
         src as ( \
             select {k_expr} as __k, {src_cols} \
             from {source} s join unnest({keys}::text[]) as u(__trellis_k) \
                  on u.__trellis_k = {k_expr} \
             where {range_where} \
         ), \
         v as ( \
             select u.__k, r.__k is not null as __present, {r_cols} \
             from unnest({keys}::text[]) as u(__k) left join src r on r.__k = u.__k \
         ), \
         {old}, \
         upd as ( \
             update {ledger} l set {set_values}, \
                 {member} = v.__present, {tombstone} = not v.__present, \
                 {basis} = snap.__snap, {seg} = greatest(l.{seg}, snap.__seg) \
             from v, snap \
             where l.{key} = v.__k \
             returning {returning} \
         ), \
         {moves_and_deltas}, \
         ins as ( \
             insert into {deltas} ({delta_cols}) select {delta_cols} from d \
         ) \
         select (select count(*) from upd), (select count(*) from d)",
        source = ddl::qualified_source_table(&plan.source_table),
        src_cols = src_cols.join(", "),
        r_cols = prefixed(&values, "r"),
        old = old_cte(plan, keys),
        ledger = plan.ledger_ident,
        set_values = set_values.join(", "),
        member = q(schema::MEMBER_COLUMN),
        tombstone = q(schema::TOMBSTONE_COLUMN),
        basis = q(schema::BASIS_COLUMN),
        seg = q(schema::APPLIED_SEG_COLUMN),
        key = q(schema::KEY_COLUMN),
        returning = upd_returning(plan),
        moves_and_deltas = moves_and_deltas(plan),
        deltas = plan.deltas_ident,
        delta_cols = delta_cols.join(", "),
    )
}

/// The merger's one statement (#625 F1; called from [`super::build`]):
/// claims up to `$1` of the target's delta rows that no other merger holds,
/// deletes them, sums them per group and upserts the sums in group order,
/// as Apply's statement does. Returns one row per written group, each
/// [`GroupUpsert`]'s columns after the count of rows claimed, or one row
/// with only that count when no group was written.
pub(super) fn merge_statement(plan: &LedgerTargetPlan, image_columns: Option<&[String]>) -> String {
    let shape = &plan.shape;
    let (groups, _) = entry_columns(shape);
    let mut sums = vec![format!(
        "sum(g.{dm})::bigint as {dm}",
        dm = schema::DELTA_MEMBERS_COLUMN
    )];
    let mut nonzero = vec![format!("sum(g.{}) <> 0", schema::DELTA_MEMBERS_COLUMN)];
    for (i, contrib) in shape.contribs.iter().enumerate() {
        let dc = schema::delta_count_column(i);
        sums.push(format!("sum(g.{dc})::bigint as {dc}"));
        nonzero.push(format!("sum(g.{dc}) <> 0"));
        if contrib.summed {
            let ds = schema::delta_sum_column(i);
            sums.push(format!("sum(g.{ds}) as {ds}"));
            nonzero.push(format!("sum(g.{ds}) <> 0"));
        }
    }
    let upsert = group_upsert_sql(plan, image_columns, "");
    format!(
        "with gone as ( \
             delete from {deltas} where ctid = any(array( \
                 select ctid from {deltas} limit $1 for update skip locked)) \
             returning * \
         ), \
         d as ( \
             select {g_groups}, {g_gk} as __gk, {sums}, '{{}}'::text[] as __ks \
             from gone g group by {g_groups} \
             having {nonzero} \
         ), \
         {up} \
         select c.__claimed, w.* \
         from (select count(*) as __claimed from gone) c \
         left join ({select}) w on true",
        deltas = plan.deltas_ident,
        g_groups = prefixed(&groups, "g"),
        g_gk = ddl::pk_key_sql_expr(&plan.identity, Some("g")),
        sums = sums.join(", "),
        nonzero = nonzero.join(" or "),
        up = upsert.cte,
        select = upsert.select,
    )
}

/// The page's recompute of its written groups' recomputed fields (see
/// [`LedgerField::Recompute`]), or `None` if the target has none. Binds `$1`
/// the groups' `ctid`s (`text[]`).
///
/// Each field is its aggregate over the group's live member entries, the
/// build's expression, read through the ledger's partial `GROUP BY` index. It
/// runs after [`ledger_statement`], whose upsert holds every written group's
/// row lock until the page commits, so its own snapshot (a new statement's,
/// under read committed) sees every entry of those groups: any other page
/// that wrote one of their entries also wrote the group, so it committed
/// before this page's upsert took the group's lock, or waits for this page
/// to commit and recomputes the group again after.
///
/// A group's entries match it by each column's own type's equality, `NULL`
/// matching `NULL` (the target's `nulls not distinct` key). That is two
/// branches: plain equality on every column, which the index serves in full
/// (and which finds nothing for a group with a `NULL` key), and the
/// null-matching form, behind a one-time filter on the group having a `NULL`
/// key, so a group without one never reads the `NULL` group's entries.
///
/// The group's untouched old values are what the upsert returned, so the
/// seam's prior image for these columns is exact without reading them here.
fn recompute_statement(plan: &LedgerTargetPlan) -> Option<String> {
    let shape = &plan.shape;
    let (columns, exprs): (Vec<String>, Vec<&str>) = shape
        .fields
        .iter()
        .filter_map(|f| match f {
            LedgerField::Recompute { column, sql } => Some((quote_ident(column), sql.as_str())),
            _ => None,
        })
        .unzip();
    if columns.is_empty() {
        return None;
    }
    let groups: Vec<String> = shape.group_cols.iter().map(|c| quote_ident(c)).collect();
    let live = format!(
        "l.{} and not l.{}",
        quote_ident(schema::MEMBER_COLUMN),
        quote_ident(schema::TOMBSTONE_COLUMN)
    );
    let equal: Vec<String> = groups
        .iter()
        .map(|c| format!("l.{c} = t.{c}"))
        .chain([live.clone()])
        .collect();
    let mut entries = format!(
        "select l.* from {} l where {}",
        plan.ledger_ident,
        equal.join(" and ")
    );
    let mut grouped = String::new();
    if !groups.is_empty() {
        let has_null: Vec<String> = groups.iter().map(|c| format!("t.{c} is null")).collect();
        let matched: Vec<String> = groups
            .iter()
            .map(|c| format!("(l.{c} = t.{c} or (l.{c} is null and t.{c} is null))"))
            .chain([live])
            .collect();
        entries.push_str(&format!(
            " union all select l.* from {} l where ({}) and {}",
            plan.ledger_ident,
            has_null.join(" or "),
            matched.join(" and ")
        ));
        grouped = format!(
            " group by {}",
            groups
                .iter()
                .map(|c| format!("l.{c}"))
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    Some(format!(
        "update {target} t set ({columns}) = \
             (select {exprs} from ({entries}) l{grouped}) \
         where t.ctid = any($1::text[]::tid[])",
        target = plan.target_ident,
        columns = columns.join(", "),
        exprs = exprs.join(", "),
    ))
}

/// One group a statement wrote, from [`GroupUpsert`]'s columns.
pub(super) struct WrittenGroup {
    key: String,
    inserted: bool,
    ctid: String,
    keys: Vec<String>,
    prior: Option<String>,
    /// Every accumulator on the row is 0 (#625 F1's B5): the row is
    /// equivalent to no row, and is deleted.
    empty: bool,
}

impl WrittenGroup {
    /// [`GroupUpsert`]'s columns of `row`, starting at column `at`. `None`
    /// for the merger's row that only carries its claim count.
    pub(super) fn from_row(row: &tokio_postgres::Row, at: usize) -> Option<Self> {
        Some(Self {
            key: row.get::<_, Option<String>>(at)?,
            inserted: row.get(at + 1),
            ctid: row.get(at + 2),
            keys: row.get(at + 3),
            prior: row.get(at + 4),
            empty: row.get(at + 5),
        })
    }
}

/// The groups a statement kept, by `ctid`: those with some accumulator not
/// 0. [`recompute_statement`] rewrites their recomputed fields.
fn kept_ctids(groups: &[WrittenGroup]) -> Vec<&str> {
    groups
        .iter()
        .filter(|g| !g.empty)
        .map(|g| g.ctid.as_str())
        .collect()
}

/// Deletes the written groups whose every accumulator is 0 (#625 F1's B5),
/// each already locked by the upsert that wrote it, and hands every written
/// or deleted group to the seam with its prior image (none for a group the
/// statement created) and the provenance `provenance` gives it. A group
/// created and emptied by the same statement never existed for anyone else,
/// and isn't recorded. Returns the groups written and deleted.
///
/// B5 is "every accumulator is 0", not "`__trellis_members <= 0`": while a
/// Re-derive build runs, a group has two channels (Apply writes it, and the
/// merger folds a chunk's deltas into it), so its member count can reach 0
/// with the other channel's sum still owed, and deleting it then would lose
/// that sum (#625 finding 5). A row whose accumulators are all 0 is the same
/// as no row to the upsert, so this rule loses nothing. Under one channel
/// it deletes exactly what the member rule did: no members means no
/// non-null contributions and nothing summed.
pub(super) async fn finish_groups(
    txn: &Transaction<'_>,
    plan: &LedgerTargetPlan,
    groups: Vec<WrittenGroup>,
    mutations: &mut TargetMutations,
    provenance: impl Fn(&WrittenGroup) -> (i32, Option<SystemTime>, Option<PgLsn>),
) -> Result<(usize, usize), ApplyError> {
    let emptied: Vec<&str> = groups
        .iter()
        .filter(|g| g.empty)
        .map(|g| g.ctid.as_str())
        .collect();
    if !emptied.is_empty() {
        txn.execute(
            &format!(
                "delete from {} where ctid = any($1::text[]::tid[])",
                plan.target_ident
            ),
            &[&emptied],
        )
        .await?;
    }
    let (mut written, mut deleted) = (0, 0);
    for group in groups {
        if group.empty && group.inserted {
            continue;
        }
        if group.empty {
            deleted += 1;
        } else {
            written += 1;
        }
        let (hop_gen, src_changed, origin) = provenance(&group);
        let prior = if group.inserted { None } else { group.prior };
        mutations.record(&plan.target, group.key, prior, hop_gen, src_changed, origin);
    }
    Ok((written, deleted))
}

/// Locks the ledger entries of `keys` (I1, I5): inserts a non-member
/// placeholder for every key with no entry, then `select … for update`
/// every entry, sorted by key.
///
/// A tombstone the placeholder insert found can be collected
/// (`super::retire::collect_tombstones`, #623 D7) before the lock reaches
/// it, leaving its key with no entry. That fails the call with
/// [`ApplyError::LedgerEntryCollected`], which the caller's transaction
/// rolls back on and retries as a transient error (#712); the retry inserts
/// the key's placeholder afresh. Taking the lock again in the same
/// transaction would insert that placeholder while holding the other keys'
/// locks, out of I5's one order, and deadlock with a transaction that
/// inserted the same placeholder first and then queued on one of them. A
/// savepoint to give those locks back first would cost every call two round
/// trips, a subtransaction and a multixact on the entries it then updates,
/// for a race that needs the GC inside a window of one round trip.
///
/// Shared by a page ([`apply_ledger_target`]) and a build chunk
/// ([`super::build`], #625 F1). `skip_lock` is the `skip_ledger_lock`
/// plant's: read without the lock.
pub(super) async fn lock_entries(
    txn: &Transaction<'_>,
    plan: &LedgerTargetPlan,
    keys: &[&str],
    skip_lock: bool,
) -> Result<(), ApplyError> {
    let ledger = &plan.ledger_ident;
    let key_col = quote_ident(schema::KEY_COLUMN);
    let mut distinct = keys.to_vec();
    distinct.sort_unstable();
    distinct.dedup();
    txn.execute(
        &format!(
            "insert into {ledger} ({key_col}, {}) \
             select k, false from unnest($1::text[]) as k order by k \
             on conflict do nothing",
            quote_ident(schema::MEMBER_COLUMN)
        ),
        &[&distinct],
    )
    .await?;
    // Test-only pause point (#623 D7). See `super::interleave`.
    #[cfg(any(test, feature = "test-util"))]
    super::interleave::pause_at(
        txn,
        super::interleave::PausePoint::AfterPlaceholders,
        &plan.target,
    )
    .await?;
    if !skip_lock {
        let locked = txn
            .execute(
                &format!(
                    "select 1 from {ledger} where {key_col} = any($1::text[]) \
                     order by {key_col} for update"
                ),
                &[&distinct],
            )
            .await?;
        if (locked as usize) < distinct.len() {
            return Err(ApplyError::LedgerEntryCollected {
                target: plan.target.clone(),
            });
        }
    }
    // Test-only pause point (#623 D1). See `super::interleave`.
    #[cfg(any(test, feature = "test-util"))]
    super::interleave::pause_at(
        txn,
        super::interleave::PausePoint::AfterEntryLock,
        &plan.target,
    )
    .await?;
    Ok(())
}

/// Applies one page's records to one ledger target, in the page's
/// transaction. See the module doc. Returns the groups written and deleted.
#[tracing::instrument(
    name = "staging.apply_ledger_target",
    skip(txn, plan, mutations),
    fields(transform = %plan.target, records = plan.records.len())
)]
pub(crate) async fn apply_ledger_target(
    txn: &Transaction<'_>,
    plan: &LedgerTargetPlan,
    seg_seq: i64,
    mutations: &mut TargetMutations,
) -> Result<(usize, usize), ApplyError> {
    if plan.records.is_empty() {
        return Ok((0, 0));
    }
    let mut records: Vec<&LedgerRecord> = plan.records.iter().collect();
    records.sort_by(|a, b| a.key.cmp(&b.key));
    let keys: Vec<&str> = records.iter().map(|r| r.key.as_str()).collect();

    // 1. The entry lock (I1, I5).
    // Planted bug (#557): read without the entry lock. See `crate::plant`.
    #[cfg(any(test, feature = "test-util"))]
    let skip_lock = crate::plant::fires(crate::plant::Plant::SkipLedgerLock, true);
    #[cfg(not(any(test, feature = "test-util")))]
    let skip_lock = false;
    lock_entries(txn, plan, &keys, skip_lock).await?;

    // 2. The Re-derive read: its rows and its snapshot in one statement.
    let rederive: Vec<&str> = records
        .iter()
        .filter(|r| r.apply.is_none())
        .map(|r| r.key.as_str())
        .collect();
    let mut read_images: HashMap<String, String> = HashMap::new();
    let mut snapshot: Option<String> = None;
    if !rederive.is_empty() {
        let mut columns: Vec<String> = plan.shape.group_cols.clone();
        for column in plan.shape.contribs.iter().flat_map(|c| c.source.reads()) {
            if !columns.contains(column) {
                columns.push(column.clone());
            }
        }
        let query = super::apply::live_rows_query(
            &plan.source_table,
            &plan.source_pk,
            &columns,
            &rederive,
        )?;
        let sql = format!(
            "select null::text, pg_catalog.pg_current_snapshot()::text \
             union all select m.k, m.doc::text from ({}) m",
            query.docs
        );
        for row in txn.query(&sql, &query.params()).await? {
            let key: Option<String> = row.get(0);
            let value: String = row.get(1);
            match key {
                None => snapshot = Some(value),
                Some(key) => {
                    read_images.insert(key, value);
                }
            }
        }
        // Test-only pause point (#623 D1), directly after the one
        // read-and-snapshot statement: the chunked-read test depends on
        // nothing committing between the two. See `super::interleave`.
        #[cfg(any(test, feature = "test-util"))]
        super::interleave::pause_at(
            txn,
            super::interleave::PausePoint::AfterRederiveRead,
            &plan.target,
        )
        .await?;
    }

    // 3. The entries and the groups, in one statement.
    let mut flags = Vec::with_capacity(records.len());
    let mut lsns: Vec<Option<String>> = Vec::with_capacity(records.len());
    let mut txids: Vec<Option<&str>> = Vec::with_capacity(records.len());
    let mut images: Vec<Option<&str>> = Vec::with_capacity(records.len());
    for record in &records {
        match &record.apply {
            Some((lsn, txid, image)) => {
                flags.push(false);
                lsns.push(Some(lsn.to_string()));
                txids.push(Some(txid.as_str()));
                images.push(image.as_deref());
            }
            None => {
                flags.push(true);
                lsns.push(None);
                txids.push(None);
                images.push(read_images.get(&record.key).map(String::as_str));
            }
        }
    }
    let image_columns = mutations.image_columns(txn, &plan.target).await?;
    #[cfg(any(test, feature = "test-util"))]
    let (visibility, drop_racing) = (
        !crate::plant::fires(crate::plant::Plant::LsnOnlySkip, !flags.iter().all(|f| *f)),
        crate::plant::fires(crate::plant::Plant::DropRacingGroupDelta, true),
    );
    #[cfg(not(any(test, feature = "test-util")))]
    let (visibility, drop_racing) = (true, false);
    let sql = ledger_statement(plan, image_columns.as_deref(), visibility, drop_racing);
    let rows = txn
        .query(
            &sql,
            &[
                &keys,
                &flags,
                &lsns,
                &txids,
                &images,
                &snapshot,
                &seg_seq,
                &plan.target,
            ],
        )
        .await?;
    let groups: Vec<WrittenGroup> = rows
        .iter()
        .filter_map(|row| WrittenGroup::from_row(row, 0))
        .collect();
    // 4. The recomputed fields of every group the page kept.
    if let Some(recompute) = recompute_statement(plan) {
        let kept = kept_ctids(&groups);
        if !kept.is_empty() {
            txn.execute(&recompute, &[&kept]).await?;
        }
    }
    // Test-only pause point (#623 D1). See `super::interleave`.
    #[cfg(any(test, feature = "test-util"))]
    super::interleave::pause_at(
        txn,
        super::interleave::PausePoint::AfterGroupUpsert,
        &plan.target,
    )
    .await?;

    // 5. Emptied groups go, and the seam gets each group with the
    // provenance of the records that moved it.
    let by_key: HashMap<&str, &LedgerRecord> =
        records.iter().map(|r| (r.key.as_str(), *r)).collect();
    finish_groups(txn, plan, groups, mutations, |group| {
        let mut hop_gen = 0;
        let mut src_changed: Option<SystemTime> = None;
        let mut origin: Option<Option<PgLsn>> = None;
        for key in &group.keys {
            let record = by_key[key.as_str()];
            hop_gen = hop_gen.max(record.hop_gen);
            src_changed = super::apply::earliest_src_changed(src_changed, record.src_changed);
            origin = Some(match origin {
                None => record.origin_lsn,
                Some(o) => earliest_origin(o, record.origin_lsn),
            });
        }
        (hop_gen, src_changed, origin.flatten())
    })
    .await
}

/// Empties a ledger target for a source `TRUNCATE` (the D split's Q6),
/// with its group deltas (#625 F1's B4), and raises its truncate floor to
/// `lsn`, the truncate's ring `lsn`. The caller clears the group rows.
pub(super) async fn truncate_ledger(
    txn: &Transaction<'_>,
    target: &str,
    lsn: Option<PgLsn>,
) -> Result<(), ApplyError> {
    txn.batch_execute(&format!(
        "truncate {}",
        ddl::qualified_target_table_ident(&schema::ledger_table_name(target))
    ))
    .await?;
    schema::truncate_deltas(
        txn,
        &ddl::qualified_target_table_ident(&schema::deltas_table_name(target)),
    )
    .await?;
    if let Some(lsn) = lsn {
        txn.execute(
            "insert into ledger_truncate_floor (target_table, floor) values ($1, $2) \
             on conflict (target_table) do update \
             set floor = greatest(ledger_truncate_floor.floor, excluded.floor)",
            &[&target, &lsn],
        )
        .await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::defs::parse;

    fn shape(text: &str, columns: &[(&str, ValueType)]) -> Option<LedgerShape> {
        let def = parse(text).expect("parse");
        let source_columns: HashMap<String, ValueType> = columns
            .iter()
            .map(|(c, t)| ((*c).to_string(), *t))
            .collect();
        route(&def, &source_columns)
    }

    const INT: ValueType = ValueType::Integer(crate::integer::IntWidth::Int4);

    #[test]
    fn plain_sum_and_count_targets_route_to_the_ledger() {
        let s = shape(
            "TRANSFORM t FROM s GROUP BY g SELECT g AS g, SUM(v) AS total, COUNT(*) AS n, \
             COUNT(v) AS nv",
            &[("g", ValueType::Text), ("v", ValueType::Numeric)],
        )
        .expect("routed");
        assert_eq!(s.group_cols, vec!["g"]);
        assert_eq!(
            s.contribs,
            vec![Contrib {
                column: "__arg0".to_string(),
                source: ContribSource::Column("v".to_string()),
                summed: true
            }]
        );
        assert_eq!(
            s.fields,
            vec![
                LedgerField::Sum {
                    column: "total".to_string(),
                    count: "__total_count".to_string(),
                    arg: 0
                },
                LedgerField::CountStar {
                    column: "n".to_string()
                },
                LedgerField::CountArg {
                    column: "nv".to_string(),
                    arg: 0
                },
            ]
        );
        assert!(
            shape(
                "TRANSFORM t FROM s GROUP BY g SELECT SUM(v) AS x",
                &[("g", INT), ("v", INT)]
            )
            .is_some()
        );
    }

    /// #623 D3b: `AVG` over an exact numeric column shares `SUM`'s hidden
    /// count, and `COUNT(x)` takes a column of any type but `json`/`jsonb`,
    /// whose contribution gets no sum.
    #[test]
    fn avg_and_count_of_any_column_route_to_the_ledger() {
        let s = shape(
            "TRANSFORM t FROM s GROUP BY g SELECT SUM(v) AS total, AVG(v) AS mean, \
             COUNT(name) AS named",
            &[
                ("g", INT),
                ("v", ValueType::Integer(crate::integer::IntWidth::Int8)),
                ("name", ValueType::Text),
            ],
        )
        .expect("routed");
        assert_eq!(
            s.contribs,
            vec![
                Contrib {
                    column: "__arg0".to_string(),
                    source: ContribSource::Column("v".to_string()),
                    summed: true
                },
                Contrib {
                    column: "__arg1".to_string(),
                    source: ContribSource::Column("name".to_string()),
                    summed: false
                },
            ]
        );
        assert_eq!(
            s.fields[1],
            LedgerField::Avg {
                column: "mean".to_string(),
                sum: "__mean_sum".to_string(),
                count: "__total_count".to_string(),
                arg: 0
            }
        );
        assert_eq!(
            s.fields[2],
            LedgerField::CountArg {
                column: "named".to_string(),
                arg: 1
            }
        );
    }

    /// #623 D4: every other field is recomputed from the ledger, and an
    /// expression argument is computed per change into its contribution.
    #[test]
    fn recompute_only_fields_and_expression_arguments_route_to_the_ledger() {
        let cols = &[
            ("g", INT),
            ("v", ValueType::Numeric),
            ("f", ValueType::Float(crate::float::FloatWidth::Float8)),
            ("b", ValueType::Boolean),
        ];
        let s = shape(
            "TRANSFORM t FROM s GROUP BY g SELECT g AS g, MIN(v) AS lo, SUM(f) AS fs, \
             BOOL_AND(b) AS every, SUM(v + 1) AS total, SUM(v) + COUNT(*) AS x",
            cols,
        )
        .expect("routed");
        assert_eq!(
            s.contribs,
            vec![
                Contrib {
                    column: "__arg0".to_string(),
                    source: ContribSource::Column("v".to_string()),
                    summed: false
                },
                Contrib {
                    column: "__arg1".to_string(),
                    source: ContribSource::Column("f".to_string()),
                    summed: false
                },
                Contrib {
                    column: "__arg2".to_string(),
                    source: ContribSource::Column("b".to_string()),
                    summed: false
                },
                Contrib {
                    column: "__arg3".to_string(),
                    source: ContribSource::Expr {
                        sql: r#"(s."v" + 1::numeric)"#.to_string(),
                        reads: vec!["v".to_string()]
                    },
                    summed: true
                },
            ]
        );
        let recompute = |column: &str, sql: &str| LedgerField::Recompute {
            column: column.to_string(),
            sql: sql.to_string(),
        };
        assert_eq!(
            s.fields,
            vec![
                recompute("lo", r#"min("__arg0")"#),
                recompute("fs", r#"sum("__arg1")"#),
                recompute("every", r#"bool_and("__arg2")"#),
                LedgerField::Sum {
                    column: "total".to_string(),
                    count: "__total_count".to_string(),
                    arg: 3
                },
                recompute("x", r#"(sum("__arg0") + count(*))"#),
            ]
        );
    }

    #[test]
    fn text_ordering_json_arguments_and_one_to_one_targets_stay_off_the_ledger() {
        let cols = &[
            ("g", INT),
            ("v", ValueType::Numeric),
            ("name", ValueType::Text),
            ("doc", ValueType::Other(PgType::Jsonb)),
        ];
        for text in [
            "TRANSFORM t FROM s GROUP BY g SELECT MIN(name) AS x",
            "TRANSFORM t FROM s GROUP BY g SELECT SUM(v) AS n, MAX(name) AS x",
            "TRANSFORM t FROM s GROUP BY g SELECT SUM(v) AS x, SUM(name) AS y",
            "TRANSFORM t FROM s GROUP BY g SELECT COUNT(doc) AS x",
            "TRANSFORM t FROM s SELECT v AS x",
        ] {
            assert_eq!(shape(text, cols), None, "{text}");
        }
    }

    /// ADR-0002 I2 evaluated by Postgres: whether an Apply of a change
    /// (`txid`, `lsn`) changes an entry (`basis`, `applied_lsn`) under the
    /// truncate `floor`.
    async fn applies(
        client: &tokio_postgres::Client,
        basis: Option<&str>,
        applied_lsn: Option<&str>,
        (txid, lsn): (&str, &str),
        floor: Option<&str>,
        visibility: bool,
    ) -> bool {
        client
            .query_one(
                &format!(
                    "with l as (select $1::text::pg_snapshot as \"__basis\", \
                                       $2::text::pg_lsn as \"__applied_lsn\"), \
                          v as (select $3::text::xid8 as __txid, $4::text::pg_lsn as __lsn), \
                          fl as (select $5::text::pg_lsn as floor where $5::text is not null) \
                     select {} from l, v",
                    apply_predicate(visibility)
                ),
                &[&basis, &applied_lsn, &txid, &lsn, &floor],
            )
            .await
            .expect("evaluate the apply predicate")
            .get(0)
    }

    #[tokio::test]
    async fn the_apply_predicate_skips_what_the_basis_saw_older_lsns_and_the_truncated() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (client, connection) = tokio_postgres::connect(db.dsn(), tokio_postgres::NoTls)
            .await
            .expect("connect");
        tokio::spawn(connection);
        let c = &client;
        // A basis taken with 102 in flight: 99 and 103 had committed, 106
        // hadn't started.
        let basis = Some("100:105:102");
        let change = |txid| (txid, "0/200");

        // A never-applied entry (a placeholder) takes any change.
        assert!(applies(c, None, None, change("99"), None, true).await);
        // Visible in the basis: the read already counted it.
        assert!(!applies(c, basis, None, change("99"), None, true).await);
        assert!(!applies(c, basis, None, change("103"), None, true).await);
        // In flight when the basis was taken (`xip`), or after it: applied.
        assert!(applies(c, basis, None, change("102"), None, true).await);
        assert!(applies(c, basis, None, change("106"), None, true).await);
        // `applied_lsn`: only a strictly newer change applies.
        assert!(!applies(c, None, Some("0/200"), change("106"), None, true).await);
        assert!(!applies(c, None, Some("0/300"), change("106"), None, true).await);
        assert!(applies(c, None, Some("0/100"), change("106"), None, true).await);
        // The truncate floor: a change at or below it predates the truncate.
        assert!(!applies(c, None, None, change("106"), Some("0/200"), true).await);
        assert!(applies(c, None, None, change("106"), Some("0/1FF"), true).await);
        // The `lsn_only_skip` plant drops the visibility term.
        assert!(applies(c, basis, None, change("99"), None, false).await);
    }

    #[test]
    fn the_apply_predicate_is_visibility_then_lsn_then_the_floor() {
        assert_eq!(
            apply_predicate(true),
            r#"(l."__basis" is null or not pg_visible_in_snapshot(v.__txid, l."__basis")) and (l."__applied_lsn" is null or v.__lsn > l."__applied_lsn") and not exists (select 1 from fl where v.__lsn <= fl.floor)"#
        );
        assert!(!apply_predicate(false).contains("pg_visible_in_snapshot"));
    }
}
