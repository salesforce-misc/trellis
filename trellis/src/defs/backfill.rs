//! What the Re-derive build of a 1-1 definition (`staging::build`) shares
//! with the rest of the engine: the inlining of a definition's calculated
//! fields into self-contained expressions ([`substitute_all_fields`]), the
//! primary-key range helpers a build chunk reads its keys with
//! ([`next_pk_ranges`], [`pk_range_where`]), and the
//! narrowing of a chunk that fails on its data to the key that causes it
//! ([`narrow_one_to_one_chunk`]).
//!
//! The key ranges are half-open `(lo, hi]` on the source's full, possibly
//! composite primary key, compared as a row value, which Postgres evaluates
//! lexicographically (matching `ORDER BY c0, c1, ...`) and which degenerates
//! to a bare scalar comparison at arity 1. Every source row's key falls in
//! exactly one range, with no gap or overlap regardless of gaps in the key
//! values. A key with a `NULL` part, which only a source keyed by a nullable
//! unique index can have, falls in no range ([`key_not_null`]); the build's
//! plan stages those keys into the ring instead.

use std::collections::{HashMap, HashSet};

use crate::error_code::{self, ErrorCode};
use crate::pool::quote_ident;
use tokio_postgres::GenericClient;

use super::ast::{Expr, TransformDef};
use super::ddl::{self, PrimaryKeyColumn};

/// Why a direct backfill could not run.
#[derive(Debug)]
pub enum BackfillError {
    /// A direct Postgres protocol/query error.
    Db(tokio_postgres::Error),
    /// Acquiring a connection from the pool failed.
    Pool(crate::error::Error),
    /// Introspecting the source primary key (1-1 build) failed.
    Ddl(ddl::DdlError),
    /// The definition's shape isn't supported by a build — e.g. a cyclic
    /// alias chain, or one whose expansion exceeds the size budget. Define
    /// refuses such a definition, so a build meets this only for a
    /// definition edited since.
    Unsupported(String),
    /// Staging a changed row's downstream propagation through the
    /// target-mutation seam failed, wrapped where a backfill
    /// error is expected.
    Propagation(Box<crate::staging::ApplyError>),
    /// The chunk-queue claim this build ran under no longer holds: its definition was resumed
    /// since the claim was taken, or the claim was reclaimed from this worker. The write stopped
    /// before touching the target; see `chunk_queue::ClaimFence`.
    Superseded,
}

impl BackfillError {
    /// This error's stable, coarse [`ErrorCode`] category (`docs/decisions/0008-public-api-design.md`,
    /// decision 3). Delegates to [`DdlError::code`] for
    /// [`BackfillError::Ddl`] and [`error_code::classify_pg_error`] for a raw
    /// Postgres error, so the mapping composes through nesting rather than
    /// re-deriving a category. [`BackfillError::Unsupported`] doesn't fit
    /// any of the more specific categories — it's a build declining a
    /// definition shape it doesn't render, which define already refuses
    /// (`ValidationError::FieldExpansionTooLarge`) — so it reports
    /// [`ErrorCode::Internal`].
    pub fn code(&self) -> ErrorCode {
        match self {
            BackfillError::Db(err) => error_code::classify_pg_error(err),
            BackfillError::Pool(err) => err.code(),
            BackfillError::Ddl(err) => err.code(),
            BackfillError::Unsupported(_) => ErrorCode::Internal,
            BackfillError::Propagation(err) => err.code(),
            BackfillError::Superseded => ErrorCode::Internal,
        }
    }
}

impl std::fmt::Display for BackfillError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BackfillError::Db(err) => {
                write!(f, "direct backfill database error: ")?;
                crate::error::write_pg_error(f, err)
            }
            BackfillError::Pool(err) => write!(f, "failed to acquire a connection: {err}"),
            BackfillError::Ddl(err) => write!(f, "direct backfill schema error: {err}"),
            BackfillError::Unsupported(what) => {
                write!(f, "direct backfill does not support {what}")
            }
            BackfillError::Propagation(err) => {
                write!(
                    f,
                    "staging the backfill's downstream propagation failed: {err}"
                )
            }
            BackfillError::Superseded => {
                write!(f, "the backfill chunk's claim was superseded")
            }
        }
    }
}

impl std::error::Error for BackfillError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            BackfillError::Db(err) => Some(err),
            BackfillError::Pool(err) => Some(err),
            BackfillError::Ddl(err) => Some(err),
            BackfillError::Propagation(err) => Some(err.as_ref()),
            BackfillError::Unsupported(_) | BackfillError::Superseded => None,
        }
    }
}

impl From<tokio_postgres::Error> for BackfillError {
    fn from(err: tokio_postgres::Error) -> Self {
        BackfillError::Db(err)
    }
}

impl From<crate::error::Error> for BackfillError {
    fn from(err: crate::error::Error) -> Self {
        BackfillError::Pool(err)
    }
}

impl From<crate::staging::ApplyError> for BackfillError {
    fn from(err: crate::staging::ApplyError) -> Self {
        BackfillError::Propagation(Box::new(err))
    }
}

impl From<ddl::DdlError> for BackfillError {
    fn from(err: ddl::DdlError) -> Self {
        BackfillError::Ddl(err)
    }
}

/// Recursively substitutes every cross-field-alias reference in `expr` — a
/// `Column(name)` that names a *different* calculated field of the same
/// definition — with a deep copy of that field's own (also-substituted)
/// expression tree, so the returned expression is self-contained: it
/// references only real source columns, literals, and
/// composition. Substitution repeats transitively (a substituted-in field may
/// itself reference yet another field) until no field-alias references remain.
///
/// `self_name` is the name of the field `expr` belongs to. A
/// `Column(self_name)` is the validator's "self-passthrough" (`price AS price`
/// — a real source column that happens to share the field's name, which
/// `validate::infer_expr` explicitly carves out as a source reference, not a
/// self-dependency), so it is left untouched and never treated as an alias.
///
/// After this pass the existing renderers (`render_expr_sql`) need no
/// awareness of field aliases: every
/// `Column` leaf is a real source column, which is exactly what they render.
/// A qualified [`Expr::SourceColumn`] names a source column by
/// construction, so it is lowered to a plain `Column` here and never reaches
/// them.
///
/// `fields_by_name` maps every field name to its expression. `visiting` is the
/// set of field names currently being expanded on this recursion path; a field
/// re-encountered while already being expanded is a cyclic alias chain
/// (`a = b + 1, b = a + 1`) and yields [`BackfillError::Unsupported`] rather
/// than recursing forever. Registration's validator rejects a cyclic
/// definition first (`validate::infer_field_types` / `ValidationError::Cycle`),
/// so this guard is defensive: transform-definition text is user-supplied
/// DSL, and a definition that reached a build some other way (a test
/// fixture's catalog row, say) must fall back to the ring rather than
/// overflow the stack here.
///
/// `memo` caches each field's fully-substituted expression by field name. A
/// field's substituted form depends only on the field (its own name scopes the
/// self-passthrough carve-out) and `fields_by_name`, never on the reference
/// site, so a field expanded once is reused (not re-expanded from its own
/// definition) everywhere it is referenced. Without this, a field referenced
/// from several places re-runs the full transitive expansion each time — a
/// diamond DAG (`out_a = shared`, `out_b = shared`, `shared = <deep chain>`)
/// pays for the deep chain once per referrer instead of once.
///
/// `budget` caps the number of expression nodes the substituted output may
/// contain, across the whole definition, and is charged as nodes are produced
/// (including the nodes a `memo` clone materializes). Memoization removes
/// *redundant* work but cannot shrink an output that is inherently large: a
/// pure doubling chain (`f0 = f1 + f1`, `f1 = f2 + f2`, … `fn = price + price`)
/// expands to 2^n copies of `price`, and each memo hit still clones its whole
/// cached subtree, so the substituted tree — and thus the work to build it — is
/// genuinely exponential in `n`. Because `install_definition` runs this direct
/// build *before* the validator, such a definition (which is valid DSL: no
/// cycle, no unknown reference) would otherwise OOM/hang the install. Exceeding
/// the budget yields [`BackfillError::Unsupported`], falling back to the ring —
/// the same safe outcome the pre-inlining code gave by never attempting these
/// shapes directly.
fn substitute_field_aliases(
    expr: &Expr,
    self_name: &str,
    fields_by_name: &HashMap<&str, &Expr>,
    visiting: &mut HashSet<String>,
    memo: &mut HashMap<String, Expr>,
    budget: &mut usize,
) -> Result<Expr, BackfillError> {
    match expr {
        Expr::Column(name) => {
            if name != self_name
                && let Some(other) = fields_by_name.get(name.as_str()).copied()
            {
                if let Some(cached) = memo.get(name) {
                    // A cached subtree can never exceed the budget it was itself
                    // built under, so charging its full node count is bounded.
                    charge_budget(budget, node_count(cached))?;
                    return Ok(cached.clone());
                }
                if !visiting.insert(name.clone()) {
                    return Err(BackfillError::Unsupported(
                        "a cyclic calculated-field alias reference".to_string(),
                    ));
                }
                let substituted =
                    substitute_field_aliases(other, name, fields_by_name, visiting, memo, budget)?;
                visiting.remove(name);
                memo.insert(name.clone(), substituted.clone());
                Ok(substituted)
            } else {
                charge_budget(budget, 1)?;
                Ok(Expr::Column(name.clone()))
            }
        }
        Expr::SourceColumn { column, .. } => {
            charge_budget(budget, 1)?;
            Ok(Expr::Column(column.clone()))
        }
        Expr::NumberLiteral(text) => {
            charge_budget(budget, 1)?;
            Ok(Expr::NumberLiteral(text.clone()))
        }
        Expr::StringLiteral(text) => {
            charge_budget(budget, 1)?;
            Ok(Expr::StringLiteral(text.clone()))
        }
        Expr::TypedLiteral { value_type, text } => {
            charge_budget(budget, 1)?;
            Ok(Expr::TypedLiteral {
                value_type: *value_type,
                text: text.clone(),
            })
        }
        Expr::Negate(operand) => {
            charge_budget(budget, 1)?;
            Ok(Expr::Negate(Box::new(substitute_field_aliases(
                operand,
                self_name,
                fields_by_name,
                visiting,
                memo,
                budget,
            )?)))
        }
        Expr::Not(_) | Expr::IsNull { .. } | Expr::Case { .. } => {
            charge_budget(budget, 1)?;
            expr.map_logic_children(|child| {
                substitute_field_aliases(child, self_name, fields_by_name, visiting, memo, budget)
            })
            .expect("a logic node")
        }
        Expr::Cast {
            expr: operand,
            target,
        } => {
            charge_budget(budget, 1)?;
            Ok(Expr::Cast {
                expr: Box::new(substitute_field_aliases(
                    operand,
                    self_name,
                    fields_by_name,
                    visiting,
                    memo,
                    budget,
                )?),
                target: *target,
            })
        }
        Expr::BinaryOp { op, lhs, rhs } => {
            charge_budget(budget, 1)?;
            Ok(Expr::BinaryOp {
                op: *op,
                lhs: Box::new(substitute_field_aliases(
                    lhs,
                    self_name,
                    fields_by_name,
                    visiting,
                    memo,
                    budget,
                )?),
                rhs: Box::new(substitute_field_aliases(
                    rhs,
                    self_name,
                    fields_by_name,
                    visiting,
                    memo,
                    budget,
                )?),
            })
        }
        Expr::FunctionCall { name, args } => {
            charge_budget(budget, 1)?;
            // An aggregate's argument reads source columns, never another
            // aggregate field by alias (SQL's rule; see
            // `validate::aggregate_field_names`). `SUM(val) AS val,
            // MIN(val) AS lo` must stay `MIN(val)` over the source column,
            // not nest into `MIN(SUM(val))`.
            let aggregate_scope;
            let fields_by_name = if super::registry::lookup_aggregate_function(name).is_some() {
                let aggregates = super::validate::aggregate_field_names(
                    fields_by_name.iter().map(|(n, e)| (*n, *e)),
                );
                aggregate_scope =
                    super::validate::without_aggregate_fields(fields_by_name, &aggregates);
                &aggregate_scope
            } else {
                fields_by_name
            };
            let args = args
                .iter()
                .map(|arg| {
                    substitute_field_aliases(arg, self_name, fields_by_name, visiting, memo, budget)
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Expr::FunctionCall {
                name: name.clone(),
                args,
            })
        }
    }
}

/// Node budget for one definition's alias substitution — see
/// [`substitute_field_aliases`]. Any realistic transform inlines a handful of
/// nodes; this cap is orders of magnitude above that, so it only ever trips on
/// a pathologically self-referential definition whose inlined form would
/// explode (which then falls back to the ring rather than OOMing the build).
const MAX_SUBSTITUTED_NODES: usize = 100_000;

/// Charges `cost` nodes against the remaining `budget`, or returns
/// [`BackfillError::Unsupported`] if the budget cannot cover it.
fn charge_budget(budget: &mut usize, cost: usize) -> Result<(), BackfillError> {
    match budget.checked_sub(cost) {
        Some(remaining) => {
            *budget = remaining;
            Ok(())
        }
        None => Err(BackfillError::Unsupported(
            "a calculated-field alias expansion larger than a build's size budget".to_string(),
        )),
    }
}

/// Total number of nodes in `expr`'s tree.
fn node_count(expr: &Expr) -> usize {
    match expr {
        Expr::Column(_)
        | Expr::SourceColumn { .. }
        | Expr::NumberLiteral(_)
        | Expr::StringLiteral(_)
        | Expr::TypedLiteral { .. } => 1,
        Expr::BinaryOp { lhs, rhs, .. } => 1 + node_count(lhs) + node_count(rhs),
        Expr::Negate(operand) => 1 + node_count(operand),
        Expr::Cast { expr, .. } => 1 + node_count(expr),
        Expr::FunctionCall { args, .. } => 1 + args.iter().map(node_count).sum::<usize>(),
        Expr::Not(_) | Expr::IsNull { .. } | Expr::Case { .. } => {
            1 + expr
                .logic_children()
                .into_iter()
                .map(node_count)
                .sum::<usize>()
        }
    }
}

/// Substitutes cross-field-alias references in every field of `def`, returning
/// each field's self-contained expression in definition order. See
/// [`substitute_field_aliases`]; a cyclic alias chain yields
/// [`BackfillError::Unsupported`] (safe fallback to the ring). A field that
/// only ever references real source columns and literals is returned as a
/// verbatim deep copy.
pub(crate) fn substitute_all_fields(def: &TransformDef) -> Result<Vec<Expr>, BackfillError> {
    let fields_by_name: HashMap<&str, &Expr> = def
        .fields
        .iter()
        .map(|f| (f.name.as_str(), &f.expr))
        .collect();
    // One memo shared across every field: a field expanded while substituting
    // an earlier field is reused (not re-expanded) when a later field references
    // it too, keeping the whole pass linear in total substituted size.
    let mut memo: HashMap<String, Expr> = HashMap::new();
    // One node budget shared across every field, so a definition can't slip a
    // huge expansion past the cap by splitting it over many fields.
    let mut budget = MAX_SUBSTITUTED_NODES;
    def.fields
        .iter()
        .map(|f| {
            let mut visiting = HashSet::new();
            substitute_field_aliases(
                &f.expr,
                &f.name,
                &fields_by_name,
                &mut visiting,
                &mut memo,
                &mut budget,
            )
        })
        .collect()
}

/// [`substitute_all_fields`], reshaped into a by-field-name map. The Aggregate
/// key-space's SQL-rendering call sites (`staging::group_basis`'s routing,
/// and the test oracle's `oracle::render_aggregate_select_sql`) all
/// need to look a field's self-contained expression up by name rather than
/// walk `def.fields` positionally — this is the one substitution pass each of
/// them shares (a `GROUP BY` field like `total + total AS
/// double_total` referencing another calculated field `total = SUM(amount)`
/// would otherwise render the raw, un-substituted `Expr::Column("total")` as a
/// bare SQL identifier, which Postgres rejects since `total` is neither a
/// source column nor a same-SELECT-list-visible name).
pub(crate) fn substituted_field_exprs(
    def: &TransformDef,
) -> Result<HashMap<String, Expr>, BackfillError> {
    let substituted = substitute_all_fields(def)?;
    Ok(def
        .fields
        .iter()
        .map(|f| f.name.clone())
        .zip(substituted)
        .collect())
}

/// The bind parameters [`pk_range_where`]'s clause expects for `(lo, hi]`:
/// `lo`'s values (when there is a lower bound), then `hi`'s.
pub(crate) fn range_params<'a>(
    lo: &'a Option<Vec<String>>,
    hi: &'a [String],
) -> Vec<&'a (dyn tokio_postgres::types::ToSql + Sync)> {
    lo.iter()
        .flatten()
        .chain(hi.iter())
        .map(|v| v as &(dyn tokio_postgres::types::ToSql + Sync))
        .collect()
}

/// The alias [`narrow_one_to_one_chunk`] gives the
/// source, so [`not_quarantined`]'s key expression names the source's columns
/// even when one of them shares a name with a column of `poison`.
const SOURCE_ALIAS: &str = "__trellis_src";

/// SQL predicate over a source row aliased [`SOURCE_ALIAS`]: its key isn't
/// quarantined (`poison`) for the source table bound as `$param` (the
/// canonical, qualified identity quarantine keys on) and the definition whose
/// bare target is bound as `$param + 1`. Whole-key poison is per transform,
/// so a key held for a sibling definition on the same source is
/// written as usual.
fn not_quarantined(pk: &[PrimaryKeyColumn], param: usize) -> String {
    format!(
        "not exists (select 1 from poison p \
                     join transform_definitions d on d.id = p.transform_id \
                     where p.src_table = ${param} and p.key = {} \
                       and split_part(d.target_table, '.', 2) = ${})",
        ddl::pk_key_sql_expr(pk, Some(SOURCE_ALIAS)),
        param + 1,
    )
}

/// SQL predicate: no part of the key `pk_idents` is `NULL`. A genuine
/// `PRIMARY KEY` never has one, but a source keyed by a nullable `UNIQUE
/// NULLS NOT DISTINCT` index (an aggregate target's grouping columns) can, and a `(lo, hi]` row
/// comparison still admits such a row whenever an earlier part decides it (`(2, NULL) <= (3,
/// 'a')`), while it leaves out one whose `NULL` part decides it. A range therefore leaves out
/// every such key, so that the ranges partition exactly the keys without
/// one, as [`next_pk_ranges`]' boundaries do. The target holds their rows all
/// the same: the Re-derive build's plan stages them into the
/// ring (`staging::build`).
pub(crate) fn key_not_null(pk_idents: &[String]) -> String {
    pk_idents
        .iter()
        .map(|c| format!("{c} is not null"))
        .collect::<Vec<_>>()
        .join(" and ")
}

/// What [`narrow_one_to_one_chunk`] found in a failed chunk's range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ChunkNarrowing {
    /// The range holds two or more keys that aren't quarantined. `mid` is the
    /// encoded key ([`ddl::join_pk_key`], as a chunk's bounds are) that splits
    /// them in half: `(lo, mid]` and `(mid, hi]` each hold at least one.
    Split { mid: String },
    /// The range holds exactly one key that isn't quarantined, in the
    /// encoding the ring and `poison` use ([`ddl::pk_key_sql_expr`]).
    Key(String),
    /// The range holds no key that isn't quarantined.
    Empty,
}

/// Narrows a failed chunk's range `(lo, hi]` of `source_table`: counts
/// the keys the chunk would write, leaving out the quarantined ones and any
/// with a `NULL` part as the write does (a 1-1 range's, and a Re-derive
/// chunk's too, [`key_not_null`]), and either names the one key or
/// the key that splits them in half. The split point is found the way
/// [`next_pk_ranges`] finds a chunk's upper bound: the largest key of the
/// first half, in the primary key's own order. `key_collations` are the ones
/// the chunk's build planned its ranges under (`backfill_chunks.key_collations`),
/// which the range is read and split under.
pub(crate) async fn narrow_one_to_one_chunk(
    client: &impl GenericClient,
    source_table: &str,
    target: &str,
    lo: Option<&str>,
    hi: &str,
    key_collations: Option<&[Option<String>]>,
) -> Result<ChunkNarrowing, BackfillError> {
    let mut pk = ddl::source_primary_key_in_txn(client, source_table).await?;
    ddl::pin_key_collations(&mut pk, key_collations);
    let decode = |text: &str| -> Result<Vec<String>, BackfillError> {
        Ok(ddl::split_pk_key(&pk, source_table, text)?
            .into_iter()
            .map(|part| part.map(|c| c.into_owned()).unwrap_or_default())
            .collect())
    };
    let lo = lo.map(decode).transpose()?;
    let hi = decode(hi)?;
    let source = ddl::qualified_source_table(source_table);
    let pk_idents: Vec<String> = pk.iter().map(|c| quote_ident(&c.name)).collect();
    let mut params = range_params(&lo, &hi);
    // The keys a chunk writes, by the same predicate.
    let where_clause = format!(
        "{} and {} and {}",
        pk_range_where(&pk_idents, &pk, &lo),
        key_not_null(&pk_idents),
        not_quarantined(&pk, params.len() + 1),
    );
    params.push(&source_table);
    params.push(&target);

    let keys: i64 = client
        .query_one(
            &format!("select count(*) from {source} as {SOURCE_ALIAS} where {where_clause}"),
            &params,
        )
        .await?
        .get(0);
    match keys {
        0 => Ok(ChunkNarrowing::Empty),
        1 => {
            let key: String = client
                .query_one(
                    &format!(
                        "select {} from {source} as {SOURCE_ALIAS} where {where_clause}",
                        ddl::pk_key_sql_expr(&pk, Some(SOURCE_ALIAS))
                    ),
                    &params,
                )
                .await?
                .get(0);
            Ok(ChunkNarrowing::Key(key))
        }
        keys => {
            let col_list = pk_idents.join(", ");
            let order_by = pk_idents
                .iter()
                .zip(&pk)
                .map(|(c, col)| col.ordered(c))
                .collect::<Vec<_>>()
                .join(", ");
            let order_desc = pk_idents
                .iter()
                .zip(&pk)
                .map(|(c, col)| format!("{} desc", col.ordered(&format!("s.{c}"))))
                .collect::<Vec<_>>()
                .join(", ");
            let mid_select = pk_idents
                .iter()
                .map(|c| format!("s.{c}::text"))
                .collect::<Vec<_>>()
                .join(", ");
            let row = client
                .query_one(
                    &format!(
                        "select {mid_select} from \
                         (select {col_list} from {source} as {SOURCE_ALIAS} \
                          where {where_clause} order by {order_by} limit {}) s \
                         order by {order_desc} limit 1",
                        keys / 2
                    ),
                    &params,
                )
                .await?;
            let mid: Vec<String> = (0..pk.len()).map(|i| row.get(i)).collect();
            Ok(ChunkNarrowing::Split {
                mid: ddl::join_pk_key(mid),
            })
        }
    }
}

/// Up to `max` consecutive `(lo, hi]` ranges of `source` (the encoded
/// source table name) of about `rows` rows each, starting above `after` (an
/// encoded key as `backfill_chunks` stores it; `None` from the first row):
/// The walk for a source's ranges, a batch at a time, for the Re-derive build's
/// plan job. Fewer than `max` ranges means the walk reached
/// the last row.
pub(crate) async fn next_pk_ranges(
    client: &impl GenericClient,
    source_table: &str,
    pk: &[PrimaryKeyColumn],
    after: Option<&str>,
    rows: i64,
    max: usize,
) -> Result<Vec<(Option<String>, String)>, BackfillError> {
    let start = after
        .map(|key| -> Result<Vec<String>, BackfillError> {
            Ok(ddl::split_pk_key(pk, source_table, key)?
                .into_iter()
                .map(|part| part.map(|c| c.into_owned()).unwrap_or_default())
                .collect())
        })
        .transpose()?;
    let source = ddl::qualified_source_table(source_table);
    Ok(walk_pk_ranges(client, &source, pk, start, rows.max(1), max)
        .await?
        .into_iter()
        .map(|(lo, hi)| (lo.map(ddl::join_pk_key), ddl::join_pk_key(hi)))
        .collect())
}

/// The walk from `start`, `rows` rows per range, at most
/// `max` ranges.
async fn walk_pk_ranges(
    client: &impl GenericClient,
    source: &str,
    pk: &[PrimaryKeyColumn],
    start: Option<Vec<String>>,
    rows: i64,
    max: usize,
) -> Result<Vec<(Option<Vec<String>>, Vec<String>)>, BackfillError> {
    let pk_idents: Vec<String> = pk.iter().map(|c| quote_ident(&c.name)).collect();
    let col_list = pk_idents.join(", ");
    // Every comparison and sort here is under each column's
    // [`PrimaryKeyColumn::collation`], as the chunks that read
    // these ranges compare.
    let order_by = pk_idents
        .iter()
        .zip(pk)
        .map(|(c, col)| col.ordered(c))
        .collect::<Vec<_>>()
        .join(", ");
    // Qualified with the inner subquery's own alias (`s.`), not bare column
    // names: the outer `select` below casts each column to `::text`, and an
    // unaliased `<col>::text` output column is named after `<col>` itself
    // (confirmed live: `select id::text from t` reports its own output
    // column as `id`, not `text`) — so a *bare* `order by <col>` out here
    // would resolve to that same-named *output* column instead of the
    // subquery's input column (Postgres prefers an output-list name over an
    // input one when both match), silently sorting by the text cast's
    // lexicographic order instead of the column's own native order
    // (`'9999' > '50000'` as text, `9999 < 50000` as bigint) — reproduced
    // live: without this qualification, a 100k-row `bigint`-keyed backfill's
    // first chunk boundary was wrongly discovered as `9999` instead of
    // `50000`, silently reprocessing (not skipping — the overwrite upsert
    // this build uses is idempotent, so this never lost or corrupted a row,
    // only over-chunked) rows 10000..50000 in both chunks. Qualifying with
    // `s.` pins the sort to the input column unambiguously, regardless of
    // what the output side happens to be named.
    let order_desc = pk_idents
        .iter()
        .zip(pk)
        .map(|(c, col)| format!("{} desc nulls last", col.ordered(&format!("s.{c}"))))
        .collect::<Vec<_>>()
        .join(", ");
    let hi_select = pk_idents
        .iter()
        .map(|c| format!("{c}::text"))
        .collect::<Vec<_>>()
        .join(", ");
    // A genuine `PRIMARY KEY` column is always `NOT NULL` (Postgres enforces
    // it), so this is a no-op filter there — it only ever excludes anything
    // when `pk` is the nullable fallback key `source_primary_key_in_txn`
    // resolves for a chained aggregate's `GROUP BY` (`UNIQUE NULLS NOT
    // DISTINCT`). `hi_select` reads each column as a bare
    // `col::text`, which panics on a genuine SQL `NULL` (`Row::get::<_,
    // String>` isn't `Option`-aware) — and a row-value comparison alone
    // doesn't reliably keep such a row out of the window: `(a, b) > (lo_a,
    // lo_b)` is `true` (not `null`/excluded) whenever `a` alone already
    // decides it, even if `b` is `NULL` (Postgres's row-comparison semantics
    // short-circuit on the first deciding member). Filtering every column
    // to `is not null` up front keeps any such row out of both the paging
    // window and the boundary itself. The ranges cover exactly the keys
    // without a `NULL` part, and every build writes the others apart from
    // its ranges (see [`key_not_null`]).
    let not_null_filter = pk_idents
        .iter()
        .map(|c| format!("{c} is not null"))
        .collect::<Vec<_>>()
        .join(" and ");

    let mut ranges = Vec::new();
    let mut lo: Option<Vec<String>> = start;
    while ranges.len() < max {
        let row = match &lo {
            None => {
                client
                    .query_opt(
                        &format!(
                            "select {hi_select} from \
                             (select {col_list} from {source} where {not_null_filter} \
                              order by {order_by} limit {rows}) s \
                             order by {order_desc} limit 1"
                        ),
                        &[],
                    )
                    .await?
            }
            Some(lo_vals) => {
                let where_clause = pk_row_cmp(&pk_idents, pk, ">", 0);
                let params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
                    lo_vals.iter().map(|v| v as _).collect();
                client
                    .query_opt(
                        &format!(
                            "select {hi_select} from \
                             (select {col_list} from {source} \
                              where {where_clause} and {not_null_filter} \
                              order by {order_by} limit {rows}) s \
                             order by {order_desc} limit 1"
                        ),
                        &params,
                    )
                    .await?
            }
        };
        let Some(row) = row else {
            break;
        };
        let hi: Vec<String> = (0..pk.len()).map(|i| row.get(i)).collect();
        ranges.push((lo.clone(), hi.clone()));
        lo = Some(hi);
    }
    Ok(ranges)
}

/// One row-value comparison (`(c0, c1, ...) {op} ($n::text::t0, ...)`) against
/// `pk`'s own columns, `param_offset` binds ahead of the first one this call
/// renders — the shared building block [`pk_range_where`] composes into a
/// full `(lo, hi]` predicate, and the range walk's lower-bound-only
/// probe uses directly. Degenerates to a bare scalar comparison at arity 1
/// (Postgres parses a single parenthesized expression as that expression, not
/// a one-element row constructor), so this is byte-identical SQL to a
/// single-column comparison there.
pub(crate) fn pk_row_cmp(
    pk_idents: &[String],
    pk: &[PrimaryKeyColumn],
    op: &str,
    param_offset: usize,
) -> String {
    let cols = pk_idents
        .iter()
        .zip(pk)
        .map(|(ident, c)| c.ordered(ident))
        .collect::<Vec<_>>()
        .join(", ");
    let binds = pk
        .iter()
        .enumerate()
        .map(|(i, c)| format!("${}::text::{}", param_offset + i + 1, c.data_type))
        .collect::<Vec<_>>()
        .join(", ");
    format!("({cols}) {op} ({binds})")
}

/// The `where` predicate restricting a PK-range chunk to `(lo, hi]` (issue
/// Row-value comparison over `pk`'s full column list, not a single
/// scalar), binding the bounds as `$1..$n` (and `$n+1..$2n` when `lo` is
/// present, `n = pk.len()`). Paired with [`next_pk_ranges`]; the caller
/// binds `hi` (first chunk) or `lo, hi`.
pub(crate) fn pk_range_where(
    pk_idents: &[String],
    pk: &[PrimaryKeyColumn],
    lo: &Option<Vec<String>>,
) -> String {
    match lo {
        None => pk_row_cmp(pk_idents, pk, "<=", 0),
        Some(_) => format!(
            "{} and {}",
            pk_row_cmp(pk_idents, pk, ">", 0),
            pk_row_cmp(pk_idents, pk, "<=", pk.len()),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::super::ast::{FieldDef, KeySpace, Operator, Predicate};
    use super::super::oracle::render_expr_sql;
    use super::*;

    /// Every column of a key range is compared under its
    /// recorded collation, and a column with none bare, so an index of that
    /// collation serves the range.
    #[test]
    fn a_key_range_names_each_column_s_collation() {
        let pk = vec![
            PrimaryKeyColumn {
                name: "name".to_string(),
                data_type: "text".to_string(),
                nullable: false,
                collation: Some(r#"pg_catalog."default""#.to_string()),
            },
            PrimaryKeyColumn {
                name: "n".to_string(),
                data_type: "integer".to_string(),
                nullable: false,
                collation: None,
            },
        ];
        let idents = vec![r#"t."name""#.to_string(), r#"t."n""#.to_string()];
        assert_eq!(
            pk_range_where(&idents, &pk, &Some(vec![String::new(), String::new()])),
            r#"(t."name" collate pg_catalog."default", t."n") > ($1::text::text, $2::text::integer) and (t."name" collate pg_catalog."default", t."n") <= ($3::text::text, $4::text::integer)"#
        );
    }

    fn col(name: &str) -> Expr {
        Expr::Column(name.to_string())
    }

    fn add(lhs: Expr, rhs: Expr) -> Expr {
        Expr::BinaryOp {
            op: Operator::Add,
            lhs: Box::new(lhs),
            rhs: Box::new(rhs),
        }
    }

    fn field(name: &str, expr: Expr) -> FieldDef {
        FieldDef {
            name: name.to_string(),
            expr,
        }
    }

    fn def_with(fields: Vec<FieldDef>) -> TransformDef {
        TransformDef {
            target: "t".to_string(),
            source: "s".to_string(),
            key_space: KeySpace::OneToOne,
            fields,
            predicate: Predicate::True,
            without_source_index: false,
            explicit_source_schema: None,
            explicit_target_schema: None,
        }
    }

    /// `total = double_price + tax`, where `double_price` is itself a
    /// calculated field (`price + price`) — the alias-derived shape a 1-1 build
    /// must render. Substitution must inline
    /// `double_price` into `total`, leaving only real source columns.
    #[test]
    fn substitute_inlines_a_cross_field_alias_chain() {
        let def = def_with(vec![
            field("double_price", add(col("price"), col("price"))),
            field("total", add(col("double_price"), col("tax"))),
        ]);
        let out = substitute_all_fields(&def).expect("no cycle");
        assert_eq!(render_expr_sql(&out[0]), r#"("price" + "price")"#);
        assert_eq!(
            render_expr_sql(&out[1]),
            r#"(("price" + "price") + "tax")"#,
            "total inlines double_price's own expression tree"
        );
    }

    /// A field referencing a source column that happens to share its own name
    /// (`price AS price`) — the self-passthrough `validate::infer_expr` carves
    /// out — must be left untouched, not treated as a (self-)alias reference.
    #[test]
    fn substitute_leaves_self_passthrough_untouched() {
        let def = def_with(vec![field("price", col("price"))]);
        let out = substitute_all_fields(&def).expect("no cycle");
        assert_eq!(render_expr_sql(&out[0]), r#""price""#);
    }

    /// Inside an aggregate's argument, the name of another
    /// aggregate field is the source column (SQL's rule), so `MIN(val)` after
    /// `SUM(val) AS val` is not inlined into `MIN(SUM(val))`. Outside an
    /// aggregate's argument the name is still the field.
    #[test]
    fn substitute_reads_a_source_column_in_an_aggregate_argument() {
        let def = crate::defs::parse(
            "TRANSFORM t FROM s GROUP BY grp WITHOUT SOURCE INDEX SELECT grp AS grp, SUM(val) AS val, \
             MIN(val) AS lo, MAX(val) + 1 AS hi, val + COUNT(*) AS mean",
        )
        .expect("parse");
        let out = substitute_all_fields(&def).expect("no cycle");
        let sql: Vec<String> = out.iter().map(render_expr_sql).collect();
        assert_eq!(sql[2], r#"min("val")"#);
        assert_eq!(sql[3], r#"(max("val") + 1::integer)"#);
        assert_eq!(
            sql[4], r#"(sum("val") + count(*))"#,
            "a name outside an aggregate's argument still inlines the field"
        );
    }

    /// A qualified name is the source column even where a field
    /// of that name exists, and comes out of substitution as a plain column.
    #[test]
    fn substitute_reads_the_source_column_for_a_qualified_name() {
        let def = crate::defs::parse(
            "TRANSFORM t FROM s GROUP BY grp WITHOUT SOURCE INDEX SELECT grp AS grp, (grp + 1) AS val, \
             SUM(s.val) AS total, s.grp + val AS mixed",
        )
        .expect("parse");
        let out = substitute_all_fields(&def).expect("no cycle");
        assert_eq!(out[2], call_sum(Expr::Column("val".to_string())));
        assert_eq!(
            render_expr_sql(&out[3]),
            r#"("grp" + ("grp" + 1::integer))"#,
            "the bare name still inlines the field"
        );

        fn call_sum(arg: Expr) -> Expr {
            Expr::FunctionCall {
                name: "SUM".to_string(),
                args: vec![arg],
            }
        }
    }

    /// An aggregate argument still reads a field that isn't an aggregate:
    /// `(id + 1) AS adj, SUM(adj)`.
    #[test]
    fn substitute_still_inlines_a_non_aggregate_field_into_an_aggregate_argument() {
        let def = def_with(vec![
            field("adj", add(col("a"), Expr::NumberLiteral("1".to_string()))),
            field(
                "t",
                Expr::FunctionCall {
                    name: "SUM".to_string(),
                    args: vec![col("adj")],
                },
            ),
        ]);
        let out = substitute_all_fields(&def).expect("no cycle");
        assert_eq!(render_expr_sql(&out[1]), r#"sum(("a" + 1::integer))"#);
    }

    /// A cyclic alias chain (`a = b + 1, b = a + 1`) must fall back to the ring
    /// (`Unsupported`) rather than recurse forever — the validator's own cycle
    /// check runs only *after* the direct backfill.
    #[test]
    fn substitute_detects_a_cycle() {
        let def = def_with(vec![
            field("a", add(col("b"), Expr::NumberLiteral("1".to_string()))),
            field("b", add(col("a"), Expr::NumberLiteral("1".to_string()))),
        ]);
        assert!(matches!(
            substitute_all_fields(&def),
            Err(BackfillError::Unsupported(_))
        ));
    }

    /// Evaluates a substituted arithmetic tree with every `price` leaf = 1, so
    /// the result counts how many `price` copies the inlining produced — the
    /// value a doubling chain `f_k = f_{k+1} + f_{k+1}` must compute.
    fn eval_price_ones(expr: &Expr) -> f64 {
        match expr {
            Expr::Column(_) => 1.0,
            Expr::NumberLiteral(text) => text.parse().unwrap(),
            Expr::BinaryOp {
                op: Operator::Add,
                lhs,
                rhs,
            } => eval_price_ones(lhs) + eval_price_ones(rhs),
            other => panic!("unexpected node in doubling-chain output: {other:?}"),
        }
    }

    /// Builds a doubling chain `f0 = f1 + f1`, …, `f{levels-1} = f{levels} +
    /// f{levels}`, `f{levels} = price + price`. Its fully-inlined form has
    /// 2^(levels+1) `price` leaves, and each field is referenced twice — the
    /// diamond/chain shape that re-expands catastrophically without care.
    fn doubling_chain(levels: usize) -> TransformDef {
        let mut fields = Vec::new();
        for k in 0..levels {
            let child = format!("f{}", k + 1);
            fields.push(field(&format!("f{k}"), add(col(&child), col(&child))));
        }
        fields.push(field(
            &format!("f{levels}"),
            add(col("price"), col("price")),
        ));
        def_with(fields)
    }

    /// A moderately deep doubling chain (each field referenced twice) inlines
    /// correctly and quickly: memoization keeps the pass bounded, and the
    /// result is `2^(levels+1)` `price` copies (verified via `eval_price_ones`).
    /// This is the shape that, un-memoized, re-expands each level twice.
    #[test]
    fn substitute_inlines_a_deep_diamond_chain_quickly() {
        let levels = 12; // 2^13 = 8192 price leaves — comfortably under the cap.
        let def = doubling_chain(levels);
        let start = std::time::Instant::now();
        let out = substitute_all_fields(&def).expect("under the node budget");
        let elapsed = start.elapsed();
        assert!(
            elapsed < std::time::Duration::from_secs(5),
            "deep diamond substitution should be bounded, took {elapsed:?}"
        );
        // f0 is the first field; its inlined value is 2^(levels+1) price copies.
        assert_eq!(eval_price_ones(&out[0]), 2f64.powi(levels as i32 + 1));
    }

    /// A doubling chain deep enough that its inlined form would explode past the
    /// node budget must fall back to the ring (`Unsupported`) instead of
    /// OOMing/hanging — the budget is the real guard, since the substituted
    /// output of a pure doubling chain is inherently exponential and no amount
    /// of memoization can shrink it. Must return promptly, never blow up.
    #[test]
    fn substitute_bails_when_the_inlined_form_would_explode() {
        let def = doubling_chain(40); // 2^41 leaves — far past MAX_SUBSTITUTED_NODES.
        let start = std::time::Instant::now();
        let result = substitute_all_fields(&def);
        let elapsed = start.elapsed();
        assert!(
            elapsed < std::time::Duration::from_secs(5),
            "budget must trip promptly, took {elapsed:?}"
        );
        assert!(matches!(result, Err(BackfillError::Unsupported(_))));
    }
}
