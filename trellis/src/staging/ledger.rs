//! Aggregate targets on the ledger (#623 parts D3, D4 and D5; epic #556,
//! ADR-0002 invariants I1, I2, I3 and I5).
//!
//! # Which targets
//!
//! [`route`] sends an aggregate target here iff every relationship it reads
//! is to-one and every `GROUP BY` key is a source column or a to-one
//! relationship path. (The grammar has no definition filter yet.) Its fields
//! are of two kinds:
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
//! column. A relationship's parent is read live, by a left join to its
//! to-side in the same statement (D5); a parent change re-derives each child
//! it reaches. Since D5 every valid aggregate target routes here.
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
//! [`group_upsert_sql`] a page uses, and then rewrites the recomputed fields
//! of the groups it wrote, folding a group it only added entries to
//! ([`recompute_written`], #625 F5).
//!
//! # One page, one target ([`apply_ledger_target`])
//!
//! 1. **Lock** (I1, I5, [`lock_entries`]): insert an entry for every key of
//!    the page that has none, then `select … for update` every other entry,
//!    sorted by key, in one statement. An Apply's new entry has its change
//!    written in (I2 holds against no entry; #775), unless the target reads
//!    a relationship, whose parents are read after the lock ([`route`]).
//!    Any other new entry is a non-member placeholder. A tombstone
//!    collected between the two (`super::retire::collect_tombstones`, #623
//!    D7) leaves its key with no entry to lock, which fails the page with a
//!    transient error: it rolls back and retries, inserting that key's
//!    placeholder afresh (#712).
//! 2. **Re-derive read**: a record staged as a `recompute` (or folded with
//!    one) is a Re-derive, and so is one with no change identity. So is
//!    every record of a page with a batch at or below the target's
//!    `build_seg`, the segment its Re-derive build started in (#733): such a
//!    batch may hold changes committed before the start, and a later change
//!    to the same key may have drained before the start, unapplied, leaving
//!    the older change's image stale (`super::build`'s start). One statement
//!    reads those keys' source rows *and* `pg_current_snapshot()`, after the
//!    lock. A key with no row is a tombstone.
//! 3. **One statement** updates the entries, aggregates the moves between
//!    each updated entry's old and new state into per-group increments, and
//!    upserts them in group order (I5), returning each written group:
//!    - a Re-derive writes the entry from its read and sets `basis` to the
//!      read's snapshot, leaving `applied_lsn` alone (the D split's Q1);
//!    - an Apply writes the entry from the change's new image (a delete
//!      makes a tombstone) and sets `applied_lsn`, but only if the change is
//!      not visible in the entry's `basis`, is newer than its `applied_lsn`,
//!      and is above the target's truncate floor (I2, Q6);
//!    - either raises `applied_seg` only on an entry it leaves a tombstone
//!      (`schema::tombstone_seg_sql`, #775);
//!    - an entry step 1 wrote with its change is left as it is, and moves
//!      into its group from no entry at all.
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
//! table's row type instead. A `json`/`jsonb` column is cast from its text
//! directly: populating would store the text as a JSON string.

use std::collections::{HashMap, HashSet};
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
use crate::defs::model::RelationshipCardinality;
use crate::defs::oracle::render_expr_sql;
use crate::defs::pg_type::PgType;
use crate::defs::validate::ResolvedRelationship;
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
    /// expression the one-pass build writes it with. `fold` is the aggregate
    /// itself when the field is one `MIN`, `MAX`, `BOOL_AND` or `BOOL_OR`:
    /// the field over a set of entries is then that aggregate of its value
    /// over each part, so a merge that only adds entries to a group folds
    /// theirs into the stored value instead of re-reading the group (#625
    /// F5, #698).
    Recompute {
        column: String,
        sql: String,
        fold: Option<&'static str>,
    },
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
    /// The `GROUP BY` columns: the target's and the ledger's column of that
    /// name.
    group_cols: Vec<String>,
    /// Per `GROUP BY` column, in order, `None` for the source column of the
    /// same name, or a to-one relationship path's value (#623 D5): SQL over
    /// the source row `s` and [`Self::joins`].
    group_paths: Vec<Option<String>>,
    /// One live `left join` per to-one relationship the definition reads,
    /// onto the source row `s` (#623 D5), or empty. Each path reads its
    /// parent's row as the statement finds it, after the child's: see
    /// [`route`].
    joins: String,
    /// The source columns [`Self::joins`] read (each relationship's
    /// from-column), which a Re-derive must read too.
    join_reads: Vec<String>,
    /// The `json`/`jsonb` source columns a contribution reads, each with its
    /// cast. An image holds a column's output text, which
    /// `jsonb_populate_record` would store in a `json`/`jsonb` column as a
    /// JSON string, so an Apply casts the text itself (#623 D5).
    json_reads: Vec<(String, &'static str)>,
    /// The contribution columns, in [`schema::contributions`]' order.
    contribs: Vec<Contrib>,
    fields: Vec<LedgerField>,
}

impl LedgerShape {
    /// The `GROUP BY` columns read from the source row under their own
    /// names: every one but a relationship path.
    fn plain_group_cols(&self) -> impl Iterator<Item = &String> {
        self.group_cols
            .iter()
            .zip(&self.group_paths)
            .filter_map(|(c, path)| path.is_none().then_some(c))
    }

    /// Whether an entry's values need the image as the source's own row
    /// type `s` (an expression argument, or a relationship's join).
    fn reads_typed_row(&self) -> bool {
        !self.joins.is_empty()
            || self
                .contribs
                .iter()
                .any(|c| matches!(c.source, ContribSource::Expr { .. }))
    }

    /// `column`'s cast if it is one of [`Self::json_reads`].
    fn json_cast(&self, column: &str) -> Option<&'static str> {
        self.json_reads
            .iter()
            .find(|(c, _)| c == column)
            .map(|(_, cast)| *cast)
    }

    /// The source columns a Re-derive reads.
    fn source_reads(&self) -> Vec<String> {
        let mut columns: Vec<String> = self.plain_group_cols().cloned().collect();
        let reads = self
            .contribs
            .iter()
            .flat_map(|c| c.source.reads())
            .chain(&self.join_reads);
        for column in reads {
            if !columns.contains(column) {
                columns.push(column.clone());
            }
        }
        columns
    }

    /// Per contribution, in order, whether a maintained `SUM`/`AVG` sums it:
    /// the contributions whose group-delta row carries a sum increment
    /// ([`schema::aggregate_deltas_ddl`]).
    pub(crate) fn summed(&self) -> Vec<bool> {
        self.contribs.iter().map(|c| c.summed).collect()
    }

    /// Whether some field is recomputed from the group's entries
    /// ([`LedgerField::Recompute`]), which the group deltas then carry what
    /// the merger's recompute reads for ([`schema::aggregate_deltas_ddl`]).
    pub(crate) fn recomputes(&self) -> bool {
        self.fields
            .iter()
            .any(|f| matches!(f, LedgerField::Recompute { .. }))
    }

    /// Whether every recomputed field folds ([`LedgerField::Recompute`]'s
    /// `fold`), so a merge that only adds entries to a group can skip the
    /// re-read of the group's entries (#625 F5).
    fn folds(&self) -> bool {
        self.fields.iter().all(|f| match f {
            LedgerField::Recompute { fold, .. } => fold.is_some(),
            _ => true,
        })
    }
}

/// Whether `def` is a target the ledger path maintains (see the module doc),
/// and its shape if so. `relationships` are `def`'s, resolved
/// (`catalog::resolve_relationships`): a definition reading one that isn't
/// there, or isn't to-one, is refused.
///
/// A relationship path's value is read from the to-side's live row, joined
/// in the ledger statement after the entry lock and after the child's own
/// read (an Apply's image, or a Re-derive's read), never from the parent
/// projection (#623 D5). So when a parent changes, the reverse path's
/// Re-derive of each child that points at it reads the new parent; a child
/// that pointed at it but has since moved has a change of its own still to
/// come, whose write reads its new parent.
pub(crate) fn route(
    def: &TransformDef,
    source_columns: &HashMap<String, ValueType>,
    relationships: &HashMap<String, ResolvedRelationship>,
) -> Option<LedgerShape> {
    let KeySpace::Aggregate { group_by } = &def.key_space else {
        return None;
    };
    let mut joined: Vec<&str> = Vec::new();
    for (rel, column) in eval::relationship_references(def) {
        let resolved = relationships.get(&rel)?;
        if resolved.cardinality != RelationshipCardinality::ToOne
            || !resolved.column_types.contains_key(&column)
        {
            return None;
        }
        let (name, _) = relationships.get_key_value(&rel)?;
        if !joined.contains(&name.as_str()) {
            joined.push(name);
        }
    }
    joined.sort_unstable();
    let mut group_cols = Vec::with_capacity(group_by.len());
    let mut group_paths = Vec::with_capacity(group_by.len());
    for key in group_by {
        match key {
            GroupByKey::Column(column) => {
                if !source_columns.contains_key(column) {
                    return None;
                }
                group_paths.push(None);
            }
            GroupByKey::RelationshipPath { .. } => {
                group_paths.push(Some(render_over_source(&key.as_expr())));
            }
        }
        group_cols.push(key.target_column_name().to_string());
    }
    let aliases: Vec<String> = joined.iter().map(|rel| join_alias(rel)).collect();
    let joins = crate::defs::oracle::to_one_join_clauses(
        joined.iter().zip(&aliases).map(|(rel, alias)| {
            let r = &relationships[*rel];
            (
                alias.as_str(),
                r.qualified_to_table.as_str(),
                r.to_col.as_str(),
                r.from_col.as_str(),
            )
        }),
        "s",
    );
    let mut join_reads: Vec<String> = Vec::new();
    for rel in &joined {
        let from_col = &relationships[*rel].from_col;
        if !join_reads.contains(from_col) {
            join_reads.push(from_col.clone());
        }
    }
    let substituted = crate::defs::backfill::substituted_field_exprs(def).ok()?;
    let field_types =
        crate::defs::validate::infer_field_types(def, source_columns, relationships).ok()?;
    let value_fields: Vec<&FieldDef> = def
        .fields
        .iter()
        .filter(|f| !group_by_contains(group_by, &f.name))
        .collect();
    let kinds: Vec<FieldKind> = value_fields
        .iter()
        .map(|f| classify_field(&substituted[&f.name], field_types[&f.name]))
        .collect();
    let contributions = schema::contributions(&def.fields, group_by, &substituted);
    let summed = |arg: &Expr| {
        value_fields.iter().zip(&kinds).any(|(f, kind)| {
            matches!(kind, FieldKind::Sum | FieldKind::Avg)
                && matches!(&substituted[&f.name], Expr::FunctionCall { args, .. }
                    if args.first() == Some(arg))
        })
    };
    let mut contribs = Vec::with_capacity(contributions.len());
    let mut json_reads: Vec<(String, &'static str)> = Vec::new();
    for contribution in &contributions {
        let mut reads = Vec::new();
        source_reads(&contribution.arg, &mut reads);
        for column in &reads {
            let cast = match source_columns.get(column)? {
                ValueType::Other(PgType::Json) => "json",
                ValueType::Other(PgType::Jsonb) => "jsonb",
                _ => continue,
            };
            if !json_reads.iter().any(|(c, _)| c == column) {
                json_reads.push((column.clone(), cast));
            }
        }
        let source = match &contribution.arg {
            Expr::Column(column) => ContribSource::Column(column.clone()),
            arg => ContribSource::Expr {
                sql: render_over_source(arg),
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
                    fold: fold_of(expr),
                    column,
                },
            }
        })
        .collect();
    Some(LedgerShape {
        group_cols,
        group_paths,
        joins,
        join_reads,
        json_reads,
        contribs,
        fields,
    })
}

/// [`route`] for the stored definition `def`, its relationships resolved
/// first (a catalog read only for a definition that reads one).
pub(crate) async fn route_definition(
    pool: &crate::pool::Pool,
    def: &crate::defs::model::Definition,
) -> Result<Option<LedgerShape>, crate::defs::catalog::CatalogError> {
    let relationships =
        crate::defs::catalog::resolve_relationships(pool, &def.def, &def.source_table).await?;
    Ok(route(&def.def, &def.source_columns, &relationships))
}

/// The alias a relationship's join takes in [`LedgerShape::joins`]:
/// prefixed, so a relationship named like one of the statement's own
/// aliases (`s`, `b`, `r`) can't shadow it.
fn join_alias(rel: &str) -> String {
    format!("__trellis_rel_{rel}")
}

/// `expr` as SQL over the source row `s` and [`LedgerShape::joins`]: a
/// source column as `s.<column>`, a relationship path as its join's column.
fn render_over_source(expr: &Expr) -> String {
    fn aliased(expr: &Expr) -> Expr {
        match expr {
            Expr::RelationshipPath { rel, column } => Expr::RelationshipPath {
                rel: join_alias(rel),
                column: column.clone(),
            },
            Expr::FunctionCall { name, args } => Expr::FunctionCall {
                name: name.clone(),
                args: args.iter().map(aliased).collect(),
            },
            Expr::BinaryOp { op, lhs, rhs } => Expr::BinaryOp {
                op: *op,
                lhs: Box::new(aliased(lhs)),
                rhs: Box::new(aliased(rhs)),
            },
            other => other.clone(),
        }
    }
    crate::defs::oracle::render_to_one_rel_expr_sql(&aliased(expr), "s")
}

/// The aggregate a recomputed field `expr` folds by ([`LedgerField::Recompute`]),
/// as SQL names it: `expr` is one `MIN`, `MAX`, `BOOL_AND` or `BOOL_OR` of an
/// argument. Each ignores `NULL`s and is its own combiner (the max of the
/// maxes of the parts is the max of the whole), and orders by the type's own
/// btree comparison, `NaN` above every number included.
fn fold_of(expr: &Expr) -> Option<&'static str> {
    let Expr::FunctionCall { name, args } = expr else {
        return None;
    };
    if args.len() != 1 {
        return None;
    }
    match name.as_str() {
        "MIN" => Some("min"),
        "MAX" => Some("max"),
        "BOOL_AND" => Some("bool_and"),
        "BOOL_OR" => Some("bool_or"),
        _ => None,
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
    pub(crate) deltas_ident: String,
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

    /// The ledger's quoted, qualified identity.
    pub(super) fn ledger_ident(&self) -> &str {
        &self.ledger_ident
    }

    /// The source's primary key.
    pub(super) fn source_pk(&self) -> &[PrimaryKeyColumn] {
        &self.source_pk
    }

    /// Pins the source key's collations to the ones a build chunk's range
    /// was planned under (`ddl::pin_key_collations`, issue #769).
    pub(super) fn pin_source_key_collations(&mut self, recorded: Option<&[Option<String>]>) {
        ddl::pin_key_collations(&mut self.source_pk, recorded);
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
/// moved it). A group whose increments are all 0 is left out, unless (for a
/// target with a recomputed field) some entry in it changed at all.
///
/// `build` adds what a build chunk's delta row carries for the merger's
/// recompute when the target has a recomputed field (#625 F5):
/// [`schema::DELTA_OUT_COLUMN`], whether a changed entry counted in the
/// group before (a value may have left it), and
/// [`schema::DELTA_KEYS_COLUMN`], the changed entries that count in it now.
/// An entry re-derived unchanged moves out and back in with the same values,
/// and is in neither.
///
/// A page's (`build` false) also moves each live entry of its `fresh` CTE
/// into its group: the entries the page inserted with their change already
/// applied ([`lock_entries`]), which had no entry to move out of.
fn moves_and_deltas(plan: &LedgerTargetPlan, build: bool) -> String {
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
        if build {
            deltas.push(format!(
                "bool_or(m.__sign < 0 and m.__rc) as {}",
                schema::DELTA_OUT_COLUMN
            ));
            deltas.push(format!(
                "array_agg(m.__k) filter (where m.__sign > 0 and m.__rc) as {}",
                schema::DELTA_KEYS_COLUMN
            ));
        }
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
             from old o join upd u on u.__k = o.__k where o.__live{fresh} \
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
        fresh = if build {
            String::new()
        } else {
            format!(
                " union all select {}, 1 as __sign, f.__k, true as __rc \
                 from fresh f where f.__live",
                prefixed(&values, "f")
            )
        },
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
/// `image_columns`), whether every accumulator on it is now 0 (#625 F1's
/// B5) and whether its recomputed fields may be folded rather than
/// recomputed (the `folds` expression over `d`; see
/// [`recompute_written`]), in that order.
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
    folds: &str,
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
                    {prior_image}, up.__trellis_empty, {folds} \
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

/// The JSON an image's text for `source_col` spells, cast to `cast`
/// (`json` or `jsonb`): see [`LedgerShape::json_reads`].
fn json_value(source_col: &str, cast: &str) -> String {
    format!("(b.__img ->> {})::{cast}", quote_literal(source_col))
}

/// The image `b.__img` as the source's row type `s`, for an expression
/// argument or a relationship's join. A `json`/`jsonb` column it reads is
/// parsed from the image's text rather than populated, which would read a
/// JSON `null` as SQL `NULL` (#623 D5).
fn typed_row(plan: &LedgerTargetPlan) -> String {
    let shape = &plan.shape;
    let populated = format!(
        "jsonb_populate_record(null::{}, b.__img)",
        ddl::qualified_source_table(&plan.source_table)
    );
    if shape.json_reads.is_empty() {
        return format!(" cross join lateral {populated} s{}", shape.joins);
    }
    let columns: Vec<String> = shape
        .source_reads()
        .iter()
        .map(|c| match shape.json_cast(c) {
            Some(cast) => format!("{} as {}", json_value(c, cast), quote_ident(c)),
            None => format!("p.{}", quote_ident(c)),
        })
        .collect();
    format!(
        " cross join lateral (select {} from {populated} p) s{}",
        columns.join(", "),
        shape.joins
    )
}

/// The `b` and `v` CTEs over a page's records: `b` binds `$1` keys, `$2`
/// Re-derive flags, `$3` `lsn`s, `$4` `row_txid`s and `$5` images (all
/// `text[]`, in key order), and `v` adds each record's entry values, cast or
/// computed from its image, and whether it has one (`__present`). `fresh`,
/// when given, is the parameter (`bool[]`, in the same order) that flags
/// each record whose entry [`lock_entries`] inserted with its change
/// applied (`__fresh`, false when not given).
fn image_ctes(plan: &LedgerTargetPlan, fresh: Option<&str>) -> String {
    let shape = &plan.shape;
    let ledger = &plan.ledger_ident;
    let q = |c: &str| quote_ident(c);
    let (groups, _) = entry_columns(shape);

    // An image's plain-column values keyed by ledger column, for
    // `jsonb_populate_record`, and each value `v` carries: the record's
    // column, or an expression argument or relationship path computed over
    // the image as a source row `s` and the live parent rows its joins find.
    let plain = shape
        .plain_group_cols()
        .map(|c| (c, c))
        .chain(shape.contribs.iter().filter_map(|c| match &c.source {
            ContribSource::Column(source) if shape.json_cast(source).is_none() => {
                Some((&c.column, source))
            }
            _ => None,
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
    let mut v_cols: Vec<String> = groups
        .iter()
        .zip(&shape.group_paths)
        .map(|(c, path)| match path {
            None => format!("r.{c}"),
            Some(sql) => format!("{sql} as {c}"),
        })
        .collect();
    for contrib in &shape.contribs {
        v_cols.push(match &contrib.source {
            ContribSource::Column(source) => match shape.json_cast(source) {
                Some(cast) => format!("{} as {}", json_value(source, cast), q(&contrib.column)),
                None => format!("r.{}", q(&contrib.column)),
            },
            ContribSource::Expr { sql, .. } => format!("{sql} as {}", q(&contrib.column)),
        });
    }
    let typed_row = if shape.reads_typed_row() {
        typed_row(plan)
    } else {
        String::new()
    };
    let (fresh_param, fresh_col) = match fresh {
        Some(param) => (format!(", {param}::bool[]"), "u.__fresh"),
        None => (String::new(), "false"),
    };
    format!(
        "b as ( \
             select u.__k, u.__rederive, u.__lsn::pg_lsn as __lsn, u.__txid::xid8 as __txid, \
                    u.__img::jsonb as __img, {fresh_col} as __fresh \
             from unnest($1::text[], $2::bool[], $3::text[], $4::text[], $5::text[]{fresh_param}) \
                  as u(__k, __rederive, __lsn, __txid, __img{fresh_alias}) \
         ), \
         v as ( \
             select b.__k, b.__rederive, b.__lsn, b.__txid, b.__fresh, \
                    b.__img is not null as __present, {v_cols} \
             from b cross join lateral \
                  jsonb_populate_record(null::{ledger}, jsonb_build_object({doc})) r{typed_row} \
         )",
        v_cols = v_cols.join(", "),
        doc = doc.join(", "),
        fresh_alias = if fresh.is_some() { ", __fresh" } else { "" },
    )
}

/// The insert that makes a page's new keys their entries (step 1, see
/// [`lock_entries`]). Binds [`image_ctes`]' `$1`–`$5`, with a Re-derive's
/// image null, `$6` the page's segment and `$7` the target's identity. A
/// key with no entry gets one: an Apply above the truncate floor writes
/// its change into it, as the page's statement would ([`ledger_statement`]:
/// I2 holds against an entry that isn't there), and stamps `$6` if it is a tombstone. A
/// Re-derive, or an Apply at or below the floor, gets a non-member
/// placeholder that the page's statement then writes. Returns each key it
/// inserted and whether its change was applied, in key order.
///
/// The stamp is the page's latest segment, not the Re-derive read's (#742):
/// the change is in it or an earlier one, and so is every older change to
/// the key, which is all an Apply's tombstone must outlast (I4), as on a
/// 1-1 ledger (`super::one_to_one_ledger::lock_entries`).
fn fresh_entries_statement(plan: &LedgerTargetPlan) -> String {
    let q = |c: &str| quote_ident(c);
    let (groups, args) = entry_columns(&plan.shape);
    let values: Vec<String> = groups.into_iter().chain(args).collect();
    let applied = q(schema::APPLIED_LSN_COLUMN);
    let key = q(schema::KEY_COLUMN);
    format!(
        "with {ctes}, \
         fl as (select floor from ledger_truncate_floor where target_table = $7) \
         insert into {ledger} ({key}, {values}, {member}, {tombstone}, {applied}, {seg}) \
         select v.__k, {when_ok}, o.ok and v.__present, o.ok and not v.__present, \
                case when o.ok then v.__lsn end, {stamp} \
         from v cross join lateral \
              (select not v.__rederive \
                      and not exists (select 1 from fl where v.__lsn <= fl.floor)) as o(ok) \
         order by v.__k \
         on conflict do nothing \
         returning {key}, {applied} is not null",
        ctes = image_ctes(plan, None),
        ledger = plan.ledger_ident,
        values = values.join(", "),
        when_ok = values
            .iter()
            .map(|c| format!("case when o.ok then v.{c} end"))
            .collect::<Vec<_>>()
            .join(", "),
        member = q(schema::MEMBER_COLUMN),
        tombstone = q(schema::TOMBSTONE_COLUMN),
        seg = q(schema::APPLIED_SEG_COLUMN),
        stamp = schema::tombstone_seg_sql(None, "o.ok and not v.__present", "$6::bigint"),
    )
}

/// The page's one ledger-and-groups statement (see the module doc, step 3).
/// Binds [`image_ctes`]' `$1`–`$5`, `$6` the Re-derive read's snapshot, `$7`
/// the page's segment, `$8` the target's identity, `$9` the keys of the
/// records whose entry [`lock_entries`] did not insert with their change
/// applied (`text[]`) and `$10` each record's flag for whether it did
/// (`bool[]`, [`image_ctes`]' `fresh`). Only the first are read and
/// updated; each of the others counts as a move into its group from no
/// entry at all, with the values its image gives, which are the ones the
/// insert wrote, so the statement never reads those entries back.
/// `image_columns` are the seam's prior-image columns (`None`: no reader).
/// Returns [`GroupUpsert`]'s columns.
///
/// `old` and `upd` read the ledger only through `$9`'s keys, and the
/// statement runs under [`ENTRY_PLAN_SETTINGS`] (#778).
///
/// The fresh entries are kept out of the ledger reads, not filtered out of
/// them (#775): a page's every key may be fresh, and a read of the ledger
/// joined against a list of them is a plan the planner can get wrong while
/// a fast-growing ledger's statistics lag. In the paged benchmark an
/// anti-join against them, and a read of them back by key, were planned as
/// scans of the whole ledger and of its `GROUP BY` index, on every page,
/// until `autoanalyze` caught up.
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
    let set_values: Vec<String> = values.iter().map(|c| format!("{c} = v.{c}")).collect();

    // Planted bug (#557): drop the increments of a group another apply
    // transaction holds. See `crate::plant`.
    let racing = if drop_racing {
        " where pg_try_advisory_xact_lock(hashtext($8), hashtext(d.__gk))"
    } else {
        ""
    };
    let upsert = group_upsert_sql(plan, image_columns, racing, "false");

    format!(
        "with {ctes}, \
         {old}, \
         fl as (select floor from ledger_truncate_floor where target_table = $8), \
         upd as ( \
             update {ledger} l set {set_values}, \
                 {member} = v.__present, {tombstone} = not v.__present, \
                 {basis} = case when v.__rederive then $6::text::pg_snapshot else l.{basis} end, \
                 {applied} = case when v.__rederive then l.{applied} else v.__lsn end, \
                 {seg} = {stamp} \
             from v \
             where l.{key} = any($9::text[]) and l.{key} = v.__k and not v.__fresh \
               and (v.__rederive or ({predicate})) \
             returning {returning} \
         ), \
         fresh as ( \
             select v.__k, {fresh_values}, v.__present as __live from v where v.__fresh \
         ), \
         {moves_and_deltas}, \
         {up} \
         {select}",
        ctes = image_ctes(plan, Some("$10")),
        old = old_cte(plan, "$9"),
        fresh_values = prefixed(&values, "v"),
        set_values = set_values.join(", "),
        predicate = apply_predicate(visibility),
        stamp =
            schema::tombstone_seg_sql(Some(&format!("l.{seg}")), "not v.__present", "$7::bigint"),
        returning = upd_returning(plan),
        moves_and_deltas = moves_and_deltas(plan, false),
        up = upsert.cte,
        select = upsert.select,
    )
}

/// A build chunk's one read-and-write statement (#625 F1, the chunk
/// Re-derive; called from [`super::build`]): `pg_current_snapshot()`, the
/// active segment's `seg_seq` and the locked keys' source rows, read
/// together; the keys' entries rewritten from them (`basis` := the
/// snapshot, a tombstone's `applied_seg` raised to that segment, `applied_lsn` left alone,
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
    let k_expr = ddl::pk_key_sql_expr(&plan.source_pk, Some("s"));
    rederive_statement(
        plan,
        &format!(
            "join unnest({keys}::text[]) as u(__trellis_k) on u.__trellis_k = {k_expr} \
             where {range_where}"
        ),
        keys,
    )
}

/// A rebuild's sweep batch's one statement (#625 F3; called from
/// [`super::build`]): [`chunk_statement`] for a set of keys rather than a
/// range, the keys of live entries the build's chunks didn't re-derive.
/// The source is read by its typed key columns, so the read is one index
/// probe per key: `$1` is the keys' `text[]`, and `$2..` one `text[]` per
/// primary-key column holding that column of each key, in key order
/// ([`sweep_key_params`]).
///
/// A key with a `NULL` part matches no row, so its entry becomes a
/// tombstone. Only a source keyed by a nullable unique index (another
/// aggregate's target) has such keys, and the Re-derive build doesn't take
/// those (#625 F6).
pub(super) fn sweep_statement(plan: &LedgerTargetPlan) -> String {
    let on: Vec<String> = plan
        .source_pk
        .iter()
        .enumerate()
        .map(|(i, column)| format!("s.{} = u.__trellis_p{i}", quote_ident(&column.name)))
        .collect();
    let arrays: Vec<String> = plan
        .source_pk
        .iter()
        .enumerate()
        .map(|(i, column)| format!("${}::text[]::{}[]", i + 2, column.data_type))
        .collect();
    let columns: Vec<String> = (0..plan.source_pk.len())
        .map(|i| format!("__trellis_p{i}"))
        .collect();
    rederive_statement(
        plan,
        &format!(
            "join unnest({}) as u({}) on {}",
            arrays.join(", "),
            columns.join(", "),
            on.join(" and ")
        ),
        "$1",
    )
}

/// [`sweep_statement`]'s `$2..` parameters for `keys`: per primary-key
/// column, that column's part of each key, as text (`None` for a `NULL`
/// part).
pub(super) fn sweep_key_params(
    plan: &LedgerTargetPlan,
    keys: &[&str],
) -> Result<Vec<Vec<Option<String>>>, ApplyError> {
    let mut columns = vec![Vec::with_capacity(keys.len()); plan.source_pk.len()];
    for key in keys {
        let parts = ddl::split_pk_key(&plan.source_pk, &plan.source_table, key)?;
        for (column, part) in columns.iter_mut().zip(parts) {
            column.push(part.map(|p| p.into_owned()));
        }
    }
    Ok(columns)
}

/// [`chunk_statement`] and [`sweep_statement`]'s shared body. `src_from` is
/// what follows `from <source> s` in the source read: the join and filter
/// that pick the keys' rows. `keys` is the locked keys' `text[]` parameter,
/// which also bounds the entry rewrite's read of the ledger. The caller runs
/// it under [`ENTRY_PLAN_SETTINGS`] (#778).
fn rederive_statement(plan: &LedgerTargetPlan, src_from: &str, keys: &str) -> String {
    let shape = &plan.shape;
    let q = |c: &str| quote_ident(c);
    let (groups, args) = entry_columns(shape);
    let values: Vec<String> = groups.iter().chain(&args).cloned().collect();
    let k_expr = ddl::pk_key_sql_expr(&plan.source_pk, Some("s"));
    let mut src_cols: Vec<String> = groups
        .iter()
        .zip(&shape.group_paths)
        .map(|(c, path)| match path {
            None => format!("s.{c} as {c}"),
            Some(sql) => format!("{sql} as {c}"),
        })
        .collect();
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
    if shape.recomputes() {
        delta_cols.push(schema::DELTA_OUT_COLUMN.to_string());
        delta_cols.push(schema::DELTA_KEYS_COLUMN.to_string());
    }
    format!(
        "with snap as ( \
             select pg_catalog.pg_current_snapshot() as __snap, \
                    (select max(seg_seq) from segments) as __seg \
         ), \
         src as ( \
             select {k_expr} as __k, {src_cols} from {source} s{joins} {src_from} \
         ), \
         v as ( \
             select u.__k, r.__k is not null as __present, {r_cols} \
             from unnest({keys}::text[]) as u(__k) left join src r on r.__k = u.__k \
         ), \
         {old}, \
         upd as ( \
             update {ledger} l set {set_values}, \
                 {member} = v.__present, {tombstone} = not v.__present, \
                 {basis} = snap.__snap, {seg} = {stamp} \
             from v, snap \
             where l.{key} = any({keys}::text[]) and l.{key} = v.__k \
             returning {returning} \
         ), \
         {moves_and_deltas}, \
         ins as ( \
             insert into {deltas} ({delta_cols}) select {delta_cols} from d \
         ) \
         select (select count(*) from upd), (select count(*) from d)",
        source = ddl::qualified_source_table(&plan.source_table),
        joins = shape.joins,
        src_cols = src_cols.join(", "),
        r_cols = prefixed(&values, "r"),
        old = old_cte(plan, keys),
        ledger = plan.ledger_ident,
        set_values = set_values.join(", "),
        member = q(schema::MEMBER_COLUMN),
        tombstone = q(schema::TOMBSTONE_COLUMN),
        basis = q(schema::BASIS_COLUMN),
        seg = q(schema::APPLIED_SEG_COLUMN),
        stamp = schema::tombstone_seg_sql(
            Some(&format!("l.{}", q(schema::APPLIED_SEG_COLUMN))),
            "not v.__present",
            "snap.__seg"
        ),
        key = q(schema::KEY_COLUMN),
        returning = upd_returning(plan),
        moves_and_deltas = moves_and_deltas(plan, true),
        deltas = plan.deltas_ident,
        delta_cols = delta_cols.join(", "),
    )
}

/// The merger's one statement (#625 F1; called from [`super::build`]):
/// claims up to `$1` of the target's delta rows in merge partition `$2`
/// (`smallint`, #717) that no other merger holds, oldest first through the
/// claim key's index (F2b), deletes them, sums them per group and upserts
/// the sums in group order, as Apply's statement does.
/// Returns one row per written group, each [`GroupUpsert`]'s columns after
/// the count of rows claimed, or one row with only that count when no group
/// was written.
///
/// The caller runs it with nested loops and sequential scans off
/// (`super::build::merge_deltas`). The claim's row estimate comes from the
/// delta table's statistics, which a queue's churn leaves meaningless: an
/// autoanalyze that samples a mostly-dead heap records `reltuples = 0`, the
/// planner then expects one claimed row, and joins the upsert's result back
/// to the group sums (`up join d`) in a nested loop that is quadratic in the
/// batch (#625 F2b: 1.8 s instead of 25 ms for 5,000 rows).
///
/// `keep_claimed` renders the `merge_without_delete` plant: the claimed rows
/// are read and locked but not deleted (see `crate::plant`).
pub(super) fn merge_statement(
    plan: &LedgerTargetPlan,
    image_columns: Option<&[String]>,
    keep_claimed: bool,
) -> String {
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
    let claim = format!(
        "{deltas} where ctid = any(array( \
             select ctid from {deltas} where {part} = $2 order by {seq} limit $1 \
             for update skip locked))",
        deltas = plan.deltas_ident,
        part = quote_ident(schema::DELTA_PART_COLUMN),
        seq = quote_ident(schema::DELTA_SEQ_COLUMN),
    );
    let gone = if keep_claimed {
        format!("select * from {claim}")
    } else {
        format!("delete from {claim} returning *")
    };
    let g_groups = prefixed(&groups, "g");
    let g_gk = ddl::pk_key_sql_expr(&plan.identity, Some("g"));
    // A target with a recomputed field (#625 F5): every group a delta row
    // names is written, even with its increments netting to 0 (an entry
    // whose `MAX` value moved from 10 to 50), so that its recompute runs.
    // `d.__ks` is the keys the rows say entered the group, and `d.__out`
    // whether a value may have left it; the two decide whether the
    // recompute folds or re-reads ([`recompute_written`]).
    let (d, folds) = if shape.recomputes() {
        let out = quote_ident(schema::DELTA_OUT_COLUMN);
        let keys = quote_ident(schema::DELTA_KEYS_COLUMN);
        (
            format!(
                "d0 as ( \
                     select {g_groups}, {g_gk} as __gk, {sums}, bool_or(g.{out}) as __out \
                     from gone g group by {g_groups} \
                 ), \
                 kk as ( \
                     select {g_groups}, array_agg(k.k) as __ks \
                     from gone g cross join lateral unnest(g.{keys}) as k(k) \
                     group by {g_groups} \
                 ), \
                 d as ( \
                     select d0.*, coalesce(kk.__ks, '{{}}'::text[]) as __ks \
                     from d0 left join kk on {d0_kk} \
                 ),",
                sums = sums.join(", "),
                // As the upsert joins its groups back to `d`: by each
                // column's own equality, `NULL` matching `NULL`.
                d0_kk = if groups.is_empty() {
                    "true".to_string()
                } else {
                    groups
                        .iter()
                        .map(|c| format!("array[d0.{c}] = array[kk.{c}]"))
                        .collect::<Vec<_>>()
                        .join(" and ")
                },
            ),
            "not d.__out",
        )
    } else {
        (
            format!(
                "d as ( \
                     select {g_groups}, {g_gk} as __gk, {sums}, '{{}}'::text[] as __ks \
                     from gone g group by {g_groups} \
                     having {nonzero} \
                 ),",
                sums = sums.join(", "),
                nonzero = nonzero.join(" or "),
            ),
            "false",
        )
    };
    let upsert = group_upsert_sql(plan, image_columns, "", folds);
    format!(
        "with gone as ({gone}), \
         {d} \
         {up} \
         select c.__claimed, w.* \
         from (select count(*) as __claimed from gone) c \
         left join ({select}) w on true",
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
            LedgerField::Recompute { column, sql, .. } => Some((quote_ident(column), sql.as_str())),
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

/// The merger's fold of its add-only groups' recomputed fields (#625 F5,
/// #698), or `None` if the target has a recomputed field that doesn't fold
/// ([`LedgerShape::folds`]) or none at all. Binds `$1` a group's `ctid` per
/// key and `$2` the keys (both `text[]`, pairwise): each group's fields
/// become their own aggregate over the stored value and the field over
/// those keys' entries that are live members of the group now.
///
/// The entries are read by key, so the statement costs the batch's keys,
/// not the groups' sizes. It runs after the merge statement, whose upsert
/// holds each group's row lock, under a snapshot of its own, as
/// [`recompute_statement`] does: an entry it reads is current, and one
/// that a later page or chunk changes is that writer's to account for.
fn fold_statement(plan: &LedgerTargetPlan) -> Option<String> {
    let shape = &plan.shape;
    if !shape.recomputes() || !shape.folds() {
        return None;
    }
    let mut columns = Vec::new();
    let mut exprs = Vec::new();
    let mut folded = Vec::new();
    for (i, field) in shape.fields.iter().enumerate() {
        let LedgerField::Recompute {
            column,
            sql,
            fold: Some(fold),
        } = field
        else {
            continue;
        };
        let c = quote_ident(column);
        exprs.push(format!("{sql} as __f{i}"));
        folded.push(format!(
            "(select {fold}(w.v) from (values (t.{c}), (a.__f{i})) as w(v))"
        ));
        columns.push(c);
    }
    let matched: Vec<String> = shape
        .group_cols
        .iter()
        .map(|c| {
            let c = quote_ident(c);
            format!("l.{c} is not distinct from t.{c}")
        })
        .chain([format!(
            "l.{} and not l.{}",
            quote_ident(schema::MEMBER_COLUMN),
            quote_ident(schema::TOMBSTONE_COLUMN)
        )])
        .collect();
    Some(format!(
        "update {target} t set ({columns}) = ( \
             select {folded} from ( \
                 select {exprs} from {ledger} l \
                 where l.{key} = any(x.__keys) and {matched} \
             ) a \
         ) \
         from ( \
             select u.c::tid as __c, array_agg(u.k) as __keys \
             from unnest($1::text[], $2::text[]) as u(c, k) group by u.c \
         ) x \
         where t.ctid = x.__c",
        target = plan.target_ident,
        columns = columns.join(", "),
        folded = folded.join(", "),
        exprs = exprs.join(", "),
        ledger = plan.ledger_ident,
        key = quote_ident(schema::KEY_COLUMN),
        matched = matched.join(" and "),
    ))
}

/// Rewrites the recomputed fields of the groups a merge statement kept
/// (#625 F5), between the upsert and [`finish_groups`], as a page does
/// after its own (see [`apply_ledger_target`]). Returns how many it folded
/// and how many it recomputed from all of their entries.
///
/// A kept group folds ([`fold_statement`]) when every one of its delta rows
/// only added entries to it (`folds`) and the upsert found its row (not
/// `inserted`); every other group is recomputed ([`recompute_statement`]):
///
/// - A value may have left the group (an entry moved out of it, became a
///   tombstone, or changed its values inside it), and the stored extreme
///   may be that value.
/// - The upsert created the row, so it has no stored value to fold into,
///   though its group can have live entries all the same: B5 deletes a row
///   whose accumulators reach 0 while a delta it is owed is pending, and
///   every entry the deleted row counted is still live (#625 F1's review).
///
/// Folding is exact because every write of a group's recomputed fields
/// leaves them equal to the field over a set of entries that were live in
/// the group, and every change that takes an entry's value out of the group
/// is followed by a recompute that sees the change: a page's own, after its
/// upsert, or the merge of the chunk's delta row that carries `__out`. A
/// fold reads the entries' current values, never the values a chunk saw: a
/// page may have changed the entry since, and recomputed the group then.
pub(super) async fn recompute_written(
    txn: &Transaction<'_>,
    plan: &LedgerTargetPlan,
    groups: &[WrittenGroup],
) -> Result<(usize, usize), ApplyError> {
    let Some(recompute) = recompute_statement(plan) else {
        return Ok((0, 0));
    };
    let fold = fold_statement(plan);
    let mut full: Vec<&str> = Vec::new();
    let mut fold_ctids: Vec<&str> = Vec::new();
    let mut fold_keys: Vec<&str> = Vec::new();
    let mut folded = 0;
    for group in groups.iter().filter(|g| !g.empty) {
        if fold.is_none() || group.inserted || !group.folds {
            full.push(&group.ctid);
            continue;
        }
        // Nothing entered and nothing left: every value is the same.
        if group.keys.is_empty() {
            continue;
        }
        folded += 1;
        for key in &group.keys {
            fold_ctids.push(&group.ctid);
            fold_keys.push(key);
        }
    }
    if let Some(fold) = &fold
        && !fold_ctids.is_empty()
    {
        txn.execute(fold, &[&fold_ctids, &fold_keys]).await?;
    }
    if !full.is_empty() {
        txn.execute(&recompute, &[&full]).await?;
    }
    Ok((folded, full.len()))
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
    /// Its recomputed fields may be folded from `keys` rather than
    /// recomputed ([`recompute_written`]).
    folds: bool,
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
            folds: row.get(at + 6),
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

/// The plan settings of a statement that reads or writes a ledger by its
/// entries' keys (#778): no sequential scan, so the ledger is reached
/// through its key's index, and the statement costs the keys it is given
/// rather than the ledger's size. [`ENTRY_PLAN_RESET`] puts them back after
/// the statement. They pin the entry lock ([`lock_statement`]), a page's
/// statement ([`ledger_statement`]), a build chunk's and a sweep batch's
/// ([`rederive_statement`]) and a 1-1 page's entry update
/// (`super::one_to_one_ledger::update_entries`).
///
/// Left to itself, the planner read the whole ledger for a page's few
/// thousand keys whenever it priced that cheaper, which it does while a
/// fast-growing ledger has no statistics yet, or ones from when it was
/// small: the lock as a sequential scan and a sort, and the entry update as
/// a hash join of the page's records to a sequential scan of the ledger.
/// The paged benchmark spent 200–345 ms a page on that scan, at up to
/// 1.15M entries, until `autoanalyze` caught up (#775).
///
/// The setting alone isn't enough for a join. With sequential scans off,
/// the planner hashed a scan of the ledger's whole key index instead. So
/// each statement that joins the ledger to its keys also restricts the
/// ledger to them (`l.key = any(<keys>)`), which bounds the ledger's side of
/// whatever join the planner picks. Hash and merge joins stay on: the
/// statements also join their CTEs to each other (`moves`, the upsert's
/// result back to `d`), where a nested loop would be quadratic in the page.
/// A bitmap scan of the key's index is fine: it reads only the keys too.
pub(super) const ENTRY_PLAN_SETTINGS: &str = "set local enable_seqscan = off";

/// Undoes [`ENTRY_PLAN_SETTINGS`] for the rest of the transaction.
pub(super) const ENTRY_PLAN_RESET: &str = "set local enable_seqscan to default";

/// Runs `sql` under [`ENTRY_PLAN_SETTINGS`], and puts the settings back
/// after it: the one place a page's (and a build chunk's) ledger statements
/// keyed by entry run, and the plan their `explain` tests read. An error
/// leaves the settings on, but it also aborts `txn`, and a `set local` ends
/// with the transaction.
pub(super) async fn query_by_entry_key(
    txn: &Transaction<'_>,
    sql: &str,
    params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
) -> Result<Vec<tokio_postgres::Row>, tokio_postgres::Error> {
    txn.batch_execute(ENTRY_PLAN_SETTINGS).await?;
    let rows = txn.query(sql, params).await?;
    txn.batch_execute(ENTRY_PLAN_RESET).await?;
    Ok(rows)
}

/// [`query_by_entry_key`] for a statement that returns exactly one row.
pub(super) async fn query_one_by_entry_key(
    txn: &Transaction<'_>,
    sql: &str,
    params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
) -> Result<tokio_postgres::Row, tokio_postgres::Error> {
    txn.batch_execute(ENTRY_PLAN_SETTINGS).await?;
    let row = txn.query_one(sql, params).await?;
    txn.batch_execute(ENTRY_PLAN_RESET).await?;
    Ok(row)
}

/// The entry lock's `select … for update` on the ledger `ledger` (quoted,
/// qualified): every entry of the keys `$1` (`text[]`), in key order (I5).
/// One row per entry locked. Shared with the 1-1 ledger
/// (`super::one_to_one_ledger::lock_entries`).
pub(super) fn lock_statement(ledger: &str) -> String {
    let key = quote_ident(schema::KEY_COLUMN);
    format!("select 1 from {ledger} where {key} = any($1::text[]) order by {key} for update")
}

/// How [`lock_entries`] makes the entries of keys that have none.
pub(super) enum NewEntries<'a> {
    /// A non-member placeholder each: a build chunk's, which re-derives
    /// every key it locks, and a page's on a target that reads a
    /// relationship.
    Placeholders,
    /// A page's: [`fresh_entries_statement`]'s parameters, in `keys`' order:
    /// the Re-derive flags, `lsn`s, `row_txid`s and Apply images (null for a
    /// Re-derive), and the page's latest segment.
    Page {
        rederive: &'a [bool],
        lsns: &'a [Option<String>],
        txids: &'a [Option<&'a str>],
        images: &'a [Option<&'a str>],
        seg: i64,
    },
}

/// Locks the ledger entries of `keys` (I1, I5): inserts an entry for every
/// key with none, as `new` says, then `select … for update` every entry the
/// insert didn't write, sorted by key, in one statement. An entry the
/// insert wrote is this transaction's own row, so it is locked already, and
/// another transaction inserting the same key waits on it. Returns the keys
/// whose entry the insert wrote with their change applied ([`NewEntries::Page`]
/// only): the page's statement leaves those as they are.
///
/// A page's Apply of a key with no entry writes the entry in the insert
/// rather than a placeholder its statement then rewrites (#775). That spares
/// the entry a second version, its index entries and a lock, as on a 1-1
/// ledger (#623 D6). Under an insert-only load every source row is such a
/// key, and the placeholder's rewrite could never be HOT, since it sets the
/// entry's group and membership, which the `GROUP BY` index reads.
///
/// A tombstone the insert found can be collected
/// (`super::retire::collect_tombstones`, #623 D7) before the lock reaches
/// it, leaving its key with no entry. That fails the call with
/// [`ApplyError::LedgerEntryCollected`], which the caller's transaction
/// rolls back on and retries as a transient error (#712); the retry inserts
/// the key's entry afresh. Taking the lock again in the same
/// transaction would insert that entry while holding the other keys'
/// locks, out of I5's one order, and deadlock with a transaction that
/// inserted the same entry first and then queued on one of them. A
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
    new: NewEntries<'_>,
    skip_lock: bool,
) -> Result<Vec<String>, ApplyError> {
    let ledger = &plan.ledger_ident;
    let key_col = quote_ident(schema::KEY_COLUMN);
    let mut inserted: HashSet<String> = HashSet::new();
    let mut applied: Vec<String> = Vec::new();
    let mut distinct = keys.to_vec();
    match new {
        NewEntries::Placeholders => {
            distinct.sort_unstable();
            distinct.dedup();
            for row in txn
                .query(
                    &format!(
                        "insert into {ledger} ({key_col}, {}) \
                         select k, false from unnest($1::text[]) as k order by k \
                         on conflict do nothing \
                         returning {key_col}",
                        quote_ident(schema::MEMBER_COLUMN)
                    ),
                    &[&distinct],
                )
                .await?
            {
                inserted.insert(row.get(0));
            }
        }
        NewEntries::Page {
            rederive,
            lsns,
            txids,
            images,
            seg,
        } => {
            for row in txn
                .query(
                    &fresh_entries_statement(plan),
                    &[&keys, &rederive, &lsns, &txids, &images, &seg, &plan.target],
                )
                .await?
            {
                let key: String = row.get(0);
                if row.get::<_, bool>(1) {
                    applied.push(key.clone());
                }
                inserted.insert(key);
            }
        }
    }
    // Test-only pause point (#623 D7). See `super::interleave`.
    #[cfg(any(test, feature = "test-util"))]
    super::interleave::pause_at(
        txn,
        super::interleave::PausePoint::AfterPlaceholders,
        &plan.target,
    )
    .await?;
    let existing: Vec<&str> = distinct
        .iter()
        .copied()
        .filter(|k| !inserted.contains(*k))
        .collect();
    if !existing.is_empty() && !skip_lock {
        let locked = query_by_entry_key(txn, &lock_statement(ledger), &[&existing])
            .await?
            .len();
        if locked < existing.len() {
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
    Ok(applied)
}

/// Whether a page whose lowest segment is `first_seg` may hold a change
/// committed before `target`'s Re-derive build started (#733): its batch
/// is at or below the target's `build_seg` (`super::build`'s start). Such a
/// page re-derives every key it holds for the target rather than applying
/// their changes, on the ledger ([`apply_ledger_target`]) and on a 1-1
/// target alike (`super::apply`'s `settle_one_to_one_target`).
pub(crate) async fn page_may_predate_build(
    txn: &Transaction<'_>,
    target: &str,
    first_seg: i64,
) -> Result<bool, tokio_postgres::Error> {
    Ok(txn
        .query_one(
            "select coalesce($2 <= (select build_seg from transform_definitions \
                                    where target_table = $1), false)",
            &[&target, &first_seg],
        )
        .await?
        .get(0))
}

/// Applies one page's records to one ledger target, in the page's
/// transaction. See the module doc. Returns the groups written and deleted.
/// `first_seg` and `seg_seq` are the lowest and highest segments of the
/// page's batches: the first decides whether the page re-derives every
/// record (step 2), and a tombstone takes the second as its
/// `applied_seg`, or the Re-derive read's newest segment when that is newer
/// (#742, as `super::one_to_one_ledger::read_rows` explains for a 1-1
/// target).
#[tracing::instrument(
    name = "staging.apply_ledger_target",
    skip(txn, plan, mutations),
    fields(transform = %plan.target, records = plan.records.len())
)]
pub(crate) async fn apply_ledger_target(
    txn: &Transaction<'_>,
    plan: &LedgerTargetPlan,
    first_seg: i64,
    seg_seq: i64,
    mutations: &mut TargetMutations,
) -> Result<(usize, usize), ApplyError> {
    if plan.records.is_empty() {
        return Ok((0, 0));
    }
    let mut records: Vec<&LedgerRecord> = plan.records.iter().collect();
    records.sort_by(|a, b| a.key.cmp(&b.key));
    let keys: Vec<&str> = records.iter().map(|r| r.key.as_str()).collect();

    // A page with a batch at or below the segment the target's Re-derive
    // build started in re-derives every record (#733, see the module doc).
    let rederive_all = records.iter().any(|r| r.apply.is_some())
        && page_may_predate_build(txn, &plan.target, first_seg).await?;
    fn apply_of(record: &LedgerRecord, rederive: bool) -> Option<&(PgLsn, String, Option<String>)> {
        if rederive {
            None
        } else {
            record.apply.as_ref()
        }
    }
    // The statements' per-record parameters, in key order. A Re-derive's
    // image is its read's, filled in at step 2.
    let mut flags = Vec::with_capacity(records.len());
    let mut lsns: Vec<Option<String>> = Vec::with_capacity(records.len());
    let mut txids: Vec<Option<&str>> = Vec::with_capacity(records.len());
    let mut images: Vec<Option<&str>> = Vec::with_capacity(records.len());
    for record in &records {
        let apply = apply_of(record, rederive_all);
        flags.push(apply.is_none());
        lsns.push(apply.map(|(lsn, _, _)| lsn.to_string()));
        txids.push(apply.map(|(_, txid, _)| txid.as_str()));
        images.push(apply.and_then(|(_, _, image)| image.as_deref()));
    }

    // 1. The entry lock (I1, I5), which writes a new key's Apply into the
    // entry it inserts.
    // Planted bug (#557): read without the entry lock. See `crate::plant`.
    #[cfg(any(test, feature = "test-util"))]
    let skip_lock = crate::plant::fires(crate::plant::Plant::SkipLedgerLock, true);
    #[cfg(not(any(test, feature = "test-util")))]
    let skip_lock = false;
    // A target that reads a relationship gets placeholders: its parents
    // are read after the entry lock ([`route`]), which an insert that
    // writes the entry can't do.
    let new = if plan.shape.joins.is_empty() {
        NewEntries::Page {
            rederive: &flags,
            lsns: &lsns,
            txids: &txids,
            images: &images,
            seg: seg_seq,
        }
    } else {
        NewEntries::Placeholders
    };
    let fresh = lock_entries(txn, plan, &keys, new, skip_lock).await?;

    // 2. The Re-derive read: its rows and its snapshot in one statement.
    let rederive: Vec<&str> = records
        .iter()
        .filter(|r| apply_of(r, rederive_all).is_none())
        .map(|r| r.key.as_str())
        .collect();
    let mut read_images: HashMap<String, String> = HashMap::new();
    let mut snapshot: Option<String> = None;
    // The entries' segment stamp: the page's latest segment, or the newest
    // one the Re-derive read's snapshot sees when that is newer (#742). The
    // read is live, so it sees later batches' changes, and every change it
    // sees is in that segment or an earlier one (see `chunk_statement`).
    let mut entry_seg = seg_seq;
    if !rederive.is_empty() {
        let columns = plan.shape.source_reads();
        let query = super::apply::live_rows_query(
            &plan.source_table,
            &plan.source_pk,
            &columns,
            &rederive,
        )?;
        let sql = format!(
            "select null::text, pg_catalog.pg_current_snapshot()::text, \
                    (select coalesce(max(seg_seq), 0) from segments) \
             union all select m.k, m.doc::text, null::bigint from ({}) m",
            query.docs
        );
        for row in txn.query(&sql, &query.params()).await? {
            let key: Option<String> = row.get(0);
            let value: String = row.get(1);
            match key {
                None => {
                    snapshot = Some(value);
                    entry_seg = entry_seg.max(row.get::<_, i64>(2));
                }
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
    for (i, record) in records.iter().enumerate() {
        if flags[i] {
            images[i] = read_images.get(&record.key).map(String::as_str);
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
    let fresh: HashSet<String> = fresh.into_iter().collect();
    let fresh_flags: Vec<bool> = keys.iter().map(|k| fresh.contains(*k)).collect();
    let stale_keys: Vec<&str> = keys
        .iter()
        .copied()
        .filter(|k| !fresh.contains(*k))
        .collect();
    let rows = query_by_entry_key(
        txn,
        &sql,
        &[
            &keys,
            &flags,
            &lsns,
            &txids,
            &images,
            &snapshot,
            &entry_seg,
            &plan.target,
            &stale_keys,
            &fresh_flags,
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

/// The plans of a page's statements that read `target`'s ledger by entry
/// key (#778), as `explain`'s text, each labelled, under the settings a
/// page runs them with: the entry lock ([`lock_statement`]) and the page's
/// statement ([`ledger_statement`]), for a page of Applies to `keys`, which
/// already have entries. `target` is the target's bare name. For tests of
/// the plans' shape. It locks and writes nothing.
#[cfg(any(test, feature = "internals"))]
pub async fn explain_page(
    pool: &crate::pool::Pool,
    target: &str,
    keys: &[&str],
) -> Result<Vec<(&'static str, String)>, ApplyError> {
    let Some(definition) = crate::defs::catalog::definition_by_target(pool, target).await? else {
        return Err(ApplyError::TransformNotFound {
            transform: target.to_string(),
        });
    };
    let Some(shape) = route_definition(pool, &definition).await? else {
        return Err(ApplyError::AggregateOffLedger {
            target: definition.target_table,
        });
    };
    let pk = ddl::source_primary_key(pool, &definition.source_table).await?;
    let mut client = pool.get().await?;
    let identity = ddl::identity_key_columns(&**client, &definition.target_table).await?;
    let plan = LedgerTargetPlan::new(
        &definition.target_table,
        &definition.source_table,
        pk,
        identity,
        shape,
    );
    let n = keys.len();
    let rederive = vec![false; n];
    let lsns: Vec<Option<String>> = vec![Some("0/10".to_string()); n];
    let txids: Vec<Option<&str>> = vec![Some("100"); n];
    let images: Vec<Option<&str>> = vec![None; n];
    let snapshot: Option<String> = None;
    let fresh = vec![false; n];
    let txn = client.transaction().await?;
    let explain = |rows: Vec<tokio_postgres::Row>| {
        rows.iter()
            .map(|row| row.get::<_, String>(0))
            .collect::<Vec<_>>()
            .join("\n")
    };
    let lock = explain(
        query_by_entry_key(
            &txn,
            &format!("explain {}", lock_statement(&plan.ledger_ident)),
            &[&keys],
        )
        .await?,
    );
    let page = explain(
        query_by_entry_key(
            &txn,
            &format!("explain {}", ledger_statement(&plan, None, true, false)),
            &[
                &keys,
                &rederive,
                &lsns,
                &txids,
                &images,
                &snapshot,
                &1_i64,
                &plan.target,
                &keys,
                &fresh,
            ],
        )
        .await?,
    );
    txn.rollback().await?;
    Ok(vec![("entry lock", lock), ("page statement", page)])
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
        route(&def, &source_columns, &HashMap::new())
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
        let recompute = |column: &str, sql: &str, fold| LedgerField::Recompute {
            column: column.to_string(),
            sql: sql.to_string(),
            fold,
        };
        assert_eq!(
            s.fields,
            vec![
                recompute("lo", r#"min("__arg0")"#, Some("min")),
                recompute("fs", r#"sum("__arg1")"#, None),
                recompute("every", r#"bool_and("__arg2")"#, Some("bool_and")),
                LedgerField::Sum {
                    column: "total".to_string(),
                    count: "__total_count".to_string(),
                    arg: 3
                },
                recompute("x", r#"(sum("__arg0") + count(*))"#, None),
            ]
        );
    }

    #[test]
    fn invalid_and_one_to_one_targets_stay_off_the_ledger() {
        let cols = &[
            ("g", INT),
            ("v", ValueType::Numeric),
            ("name", ValueType::Text),
        ];
        for text in [
            "TRANSFORM t FROM s GROUP BY g SELECT SUM(v) AS x, SUM(name) AS y",
            "TRANSFORM t FROM s SELECT v AS x",
        ] {
            assert_eq!(shape(text, cols), None, "{text}");
        }
    }

    /// #623 D5: text `MIN`/`MAX` and `json`/`jsonb` arguments route to the
    /// ledger, the json columns parsed from the image's text.
    #[test]
    fn text_ordering_and_json_arguments_route_to_the_ledger() {
        let cols = &[
            ("g", INT),
            ("v", ValueType::Numeric),
            ("name", ValueType::Text),
            ("doc", ValueType::Other(PgType::Jsonb)),
            ("raw", ValueType::Other(PgType::Json)),
        ];
        for text in [
            "TRANSFORM t FROM s GROUP BY g SELECT MIN(name) AS x",
            "TRANSFORM t FROM s GROUP BY g SELECT SUM(v) AS n, MAX(name) AS x",
        ] {
            let s = shape(text, cols).expect(text);
            assert!(s.json_reads.is_empty(), "{text}");
        }
        let s = shape(
            "TRANSFORM t FROM s GROUP BY g SELECT COUNT(doc) AS x, COUNT(raw) AS y",
            cols,
        )
        .expect("routed");
        assert_eq!(
            s.json_reads,
            vec![("doc".to_string(), "jsonb"), ("raw".to_string(), "json")]
        );
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
