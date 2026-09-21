//! Direct, set-based, key-range-chunked backfill of a definition's target
//! straight from its source (issue #63 milestone 3).
//!
//! # Why this exists
//!
//! A from-scratch backfill used to run entirely through the staging ring:
//! [`super::catalog::create_definition`] enumerates every source row as a
//! `Recompute` marker (`intake::publication::enumerate_and_append`), which the
//! ring then folds and applies. That stages one marker per source row, folds
//! the whole table in a single ring segment, and applies it as one giant
//! transaction — the cost M0's benchmark measured and M1/M2 chipped at.
//!
//! This module bypasses the ring for the initial build: it computes the target
//! directly with server-side `INSERT … SELECT` statements, chunked by key
//! range so each statement is one bounded transaction. The ring is then left
//! to carry only live CDC deltas that land *after* the build (see the fence
//! discussion below). Callers pair this with
//! [`super::catalog::create_definition_without_backfill`] so the ring
//! enumeration doesn't *also* run.
//!
//! # Correctness — the five concerns issue #63 M3 calls out
//!
//! 1. **Exhaustive, disjoint chunking.** The 1-1 build walks the source
//!    primary key in half-open ranges `(lo, hi]` discovered by
//!    `max()`-over-`LIMIT` (see [`backfill_one_to_one`]): every source row's PK
//!    falls in exactly one range, with no gap or overlap regardless of gaps in
//!    the key values. The aggregate build chunks by *group key* range instead
//!    (see [`backfill_aggregate`]): every group's key is a single point in the
//!    group-key space, so a group lands wholly in exactly one range and is
//!    therefore computed in exactly one chunk.
//!
//! 2. **The build/CDC fence.** Both builds write with `ON CONFLICT DO UPDATE
//!    SET col = excluded.col` — an *overwrite* that recomputes each target row
//!    (or whole group) from the current source, identical in effect to the
//!    ring's own image-less `Recompute` path. Overwrite is idempotent and
//!    order-independent, so the handoff to the ring is the same one the ring
//!    already relies on: the caller runs this build before live CDC
//!    application begins for the definition (target table created, build run,
//!    *then* the client starts), and any genuine post-build delta the ring
//!    later applies lands on a fully-built row. This is deliberately *not* the
//!    additive (`col = target.col + excluded.col`) merge the issue sketches as
//!    the aggregate default: additive merge is not idempotent (a re-run or an
//!    overlapping CDC delta double-counts) and cannot express a
//!    `RecomputeOnly` field (`MIN`/`MAX`/composed) that spans chunks at all.
//!    Chunking by group key lets every field kind use the safe overwrite form.
//!
//! 3. **Per-field-kind aggregates.** Each field is built with the same SQL the
//!    incremental bulk path (`staging::apply_aggregate::apply_forced_groups_bulk`)
//!    emits — `SUM` keeps its hidden `__{f}_count` partial, `AVG` its
//!    `__{f}_sum`/`__{f}_count` partials with the visible column derived as
//!    `sum/count`, `COUNT(*)` a bare `count(*)`, and everything else
//!    (`MIN`/`MAX`/composed) its rendered expression — so a target built here
//!    is byte-identical to one the ring would have produced, and a later CDC
//!    delta folds onto consistent partials.
//!
//! 4. **Idempotent retry.** Every chunk is its own transaction and every write
//!    is an overwrite, so a crash or error partway through is recovered by
//!    simply re-running the whole build: already-built rows/groups are
//!    recomputed to the same value, not doubled.
//!
//! 5. **1-1 bind-param safety.** The 1-1 build uses `INSERT … SELECT` over the
//!    source (server-side), not a `VALUES` list of client-bound rows, so it
//!    carries a fixed handful of bound parameters (the range bounds) regardless
//!    of chunk size — it never approaches the `i16::MAX` bind-parameter cap the
//!    ring's row-at-a-time apply path (`staging::apply::apply_target`) must
//!    chunk around.

use std::collections::{BTreeSet, HashMap, HashSet};

use crate::error_code::{self, ErrorCode};
use crate::pool::{Client, Pool, quote_ident};

use super::ast::{
    Expr, GroupByKey, KeySpace, Operator, RelationshipDef, TransformDef, ValueType,
    group_by_contains,
};
use super::ddl::{
    self, PrimaryKeyColumn, avg_sum_column, qualified_target_table, require_single_column_pk,
    source_primary_key,
};
use super::invertibility::{AggregateArg, CountArg, classify};
use super::model::RelationshipCardinality;
use super::oracle::render_expr_sql;
use super::registry::lookup_aggregate_function;

/// Rows per chunk for the 1-1 primary-key-range build. Each chunk is one
/// bounded transaction; 50k keeps a chunk's write set well within a
/// comfortable transaction size while keeping the number of round trips low
/// for a million-row source.
const BACKFILL_CHUNK_ROWS: i64 = 50_000;

/// Distinct groups per chunk for the aggregate group-key-range build. Bounds
/// the number of target rows one chunk's `ON CONFLICT` transaction touches
/// (and, with it, the group-key ranges' hash-aggregate working set) regardless
/// of how many groups the source has in total.
const BACKFILL_CHUNK_GROUPS: i64 = 10_000;

/// Connection-scoped staging table the aggregate build materializes its
/// single-pass `GROUP BY` into before chunk-writing to the target. A fixed name
/// is safe: the build holds one pooled connection for its whole duration (so no
/// two aggregate builds share this name concurrently — concurrent builds get
/// distinct connections/sessions), and it is dropped both before creation
/// (crash-leftover on a reused pooled connection) and after the writes.
const STAGE_TABLE: &str = "_trellis_backfill_agg_staging";

/// Why a direct backfill could not run.
#[derive(Debug)]
pub enum BackfillError {
    /// A direct Postgres protocol/query error.
    Db(tokio_postgres::Error),
    /// Acquiring a connection from the pool failed.
    Pool(crate::error::Error),
    /// Introspecting the source primary key (1-1 build) failed.
    Ddl(ddl::DdlError),
    /// The definition's shape isn't supported by the direct build yet — e.g. a
    /// relationship-enriched 1-1 definition, whose target the direct build
    /// can't render without the LEFT JOIN/correlated-subquery machinery the
    /// ring path uses. Such definitions must keep going through
    /// [`super::catalog::create_definition`]'s ring enumeration.
    Unsupported(String),
}

impl BackfillError {
    /// This error's stable, coarse [`ErrorCode`] category (`docs/decisions/0008-public-api-design.md`,
    /// decision 3). Delegates to [`DdlError::code`] for
    /// [`BackfillError::Ddl`] and [`error_code::classify_pg_error`] for a raw
    /// Postgres error, so the mapping composes through nesting rather than
    /// re-deriving a category. [`BackfillError::Unsupported`] doesn't fit
    /// any of the more specific categories — it's this build path declining
    /// a definition shape it doesn't render, not a rejection of the
    /// definition itself (the direct-build caller falls back to the ring
    /// path instead of surfacing it) — so it reports [`ErrorCode::Internal`].
    pub fn code(&self) -> ErrorCode {
        match self {
            BackfillError::Db(err) => error_code::classify_pg_error(err),
            BackfillError::Pool(err) => err.code(),
            BackfillError::Ddl(err) => err.code(),
            BackfillError::Unsupported(_) => ErrorCode::Internal,
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
        }
    }
}

impl std::error::Error for BackfillError {}

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

impl From<ddl::DdlError> for BackfillError {
    fn from(err: ddl::DdlError) -> Self {
        BackfillError::Ddl(err)
    }
}

/// Builds `def`'s target directly from its source, in bounded key-range
/// chunks, dispatching on `def`'s key space (see the module docs). `def`'s
/// target table must already exist (created by
/// [`super::ddl::create_target_table`] /
/// [`super::ddl::create_aggregate_target_table`]) — this only writes rows, it
/// does not create the table. `target_schema` and `source_columns` are the
/// same values those DDL calls were given. `source_table` is `def.source`'s
/// fully-qualified `"schema.table"` identity (issue #76, ADR-0007) — the
/// caller's own already-resolved value (`catalog::resolve_source_for_install`,
/// or [`super::model::Definition::source_table`] for a durable chunk-queue
/// caller) — threaded through every read of the live source below instead of
/// a bare `def.source` left to the executing connection's own `search_path`.
pub async fn backfill_definition(
    pool: &Pool,
    def: &TransformDef,
    target_schema: &str,
    source_table: &str,
    source_columns: &HashMap<String, ValueType>,
) -> Result<(), BackfillError> {
    match &def.key_space {
        KeySpace::OneToOne => {
            // Issue #126: the 1-1 direct build's PK-range chunking
            // (`discover_pk_ranges`/`write_one_to_one_range`) only knows how
            // to order/compare a single scalar column — narrow down here,
            // same as `catalog::install_definition`'s own DDL call site (in
            // practice this def's target table already failed DDL for a
            // composite-PK source before backfill is ever reached, so this
            // is a defensive re-check, not the primary enforcement point).
            let pk = require_single_column_pk(
                source_primary_key(pool, source_table).await?,
                source_table,
            )?;
            if uses_relationships(def) {
                backfill_relationship_one_to_one(pool, def, target_schema, source_table, &pk).await
            } else {
                backfill_one_to_one(pool, def, target_schema, source_table, &pk).await
            }
        }
        KeySpace::Aggregate { group_by } => {
            backfill_aggregate(
                pool,
                def,
                target_schema,
                source_table,
                group_by,
                source_columns,
            )
            .await
        }
    }
}

/// Whether any of `def`'s field expressions reads a relationship path — the
/// shape the 1-1 direct build can't render (see [`BackfillError::Unsupported`]).
pub(crate) fn uses_relationships(def: &TransformDef) -> bool {
    fn walk(expr: &Expr) -> bool {
        match expr {
            Expr::RelationshipPath { .. } => true,
            Expr::BinaryOp { lhs, rhs, .. } => walk(lhs) || walk(rhs),
            Expr::FunctionCall { args, .. } => args.iter().any(walk),
            Expr::Column(_)
            | Expr::NumberLiteral(_)
            | Expr::StringLiteral(_)
            | Expr::TypedLiteral { .. } => false,
        }
    }
    def.fields.iter().any(|f| walk(&f.expr))
}

/// Recursively substitutes every cross-field-alias reference in `expr` — a
/// `Column(name)` that names a *different* calculated field of the same
/// definition — with a deep copy of that field's own (also-substituted)
/// expression tree, so the returned expression is self-contained: it
/// references only real source columns, literals, relationship paths, and
/// composition. Substitution repeats transitively (a substituted-in field may
/// itself reference yet another field) until no field-alias references remain.
///
/// `self_name` is the name of the field `expr` belongs to. A
/// `Column(self_name)` is the validator's "self-passthrough" (`price AS price`
/// — a real source column that happens to share the field's name, which
/// `validate::infer_expr` explicitly carves out as a source reference, not a
/// self-dependency), so it is left untouched and never treated as an alias.
///
/// After this pass the existing renderers (`render_expr_sql` and the
/// relationship renderer below) need no awareness of field aliases: every
/// `Column` leaf is a real source column, which is exactly what they render.
///
/// `fields_by_name` maps every field name to its expression. `visiting` is the
/// set of field names currently being expanded on this recursion path; a field
/// re-encountered while already being expanded is a cyclic alias chain
/// (`a = b + 1, b = a + 1`) and yields [`BackfillError::Unsupported`] rather
/// than recursing forever. This guard is load-bearing, not merely defensive:
/// transform-definition text is user-supplied DSL, and the validator's cycle
/// check (`validate::infer_field_types` / `ValidationError::Cycle`) runs
/// *after* the direct backfill in `install_definition`
/// (`backfill_definition` before `create_definition` → `validate`), so a
/// cyclic definition does reach this code — falling back to the ring lets the
/// validator then reject it, instead of overflowing the stack here.
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
        Expr::RelationshipPath { rel, column } => {
            charge_budget(budget, 1)?;
            Ok(Expr::RelationshipPath {
                rel: rel.clone(),
                column: column.clone(),
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
/// explode (which then falls back to the ring rather than OOMing the install).
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
            "a calculated-field alias expansion larger than the direct build's size budget"
                .to_string(),
        )),
    }
}

/// Total number of nodes in `expr`'s tree.
fn node_count(expr: &Expr) -> usize {
    match expr {
        Expr::Column(_)
        | Expr::NumberLiteral(_)
        | Expr::StringLiteral(_)
        | Expr::TypedLiteral { .. } => 1,
        Expr::RelationshipPath { .. } => 1,
        Expr::BinaryOp { lhs, rhs, .. } => 1 + node_count(lhs) + node_count(rhs),
        Expr::FunctionCall { args, .. } => 1 + args.iter().map(node_count).sum::<usize>(),
    }
}

/// Substitutes cross-field-alias references in every field of `def`, returning
/// each field's self-contained expression in definition order. See
/// [`substitute_field_aliases`]; a cyclic alias chain yields
/// [`BackfillError::Unsupported`] (safe fallback to the ring). A field that
/// only ever references real source columns, literals, and relationship paths
/// is returned as a verbatim deep copy.
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
/// key-space's SQL-rendering call sites (`backfill_aggregate`,
/// `staging::apply_aggregate::classify_fields`, `staging::apply`'s aggregate
/// dispatch, and the test oracle's `oracle::render_aggregate_select_sql`) all
/// need to look a field's self-contained expression up by name rather than
/// walk `def.fields` positionally — this is the one substitution pass each of
/// them shares (issue: a `GROUP BY` field like `total + total AS
/// double_total` referencing another calculated field `total = SUM(amount)`
/// previously rendered the raw, un-substituted `Expr::Column("total")` as a
/// bare SQL identifier, which Postgres rejects since `total` is neither a
/// source column nor a same-SELECT-list-visible name; the 1-1 key-space fixed
/// the equivalent bug via this same [`substitute_all_fields`] call for issue
/// #83, but the Aggregate key-space's call sites never adopted it).
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

/// The 1-1 build: walk the source primary key in half-open `(lo, hi]` ranges,
/// each `INSERT … SELECT … ON CONFLICT DO UPDATE`-ing one bounded chunk.
///
/// Range discovery reads the max PK of the next `BACKFILL_CHUNK_ROWS` source
/// rows above `lo` (`select max(pk) from (select pk … where pk > lo order by
/// pk limit N)`); that max becomes `hi`, the chunk covers `pk > lo and pk <=
/// hi`, and the next `lo` is this `hi`. When the discovery query returns `NULL`
/// (no rows left above `lo`) the walk stops. Every source row's PK is `> lo`
/// for exactly one range and `<= hi` for that same range, so the ranges
/// partition the source exactly once with no gap or overlap — the off-by-one
/// this structure guards against is exactly what
/// `defs_backfill_direct`'s boundary test exercises.
async fn backfill_one_to_one(
    pool: &Pool,
    def: &TransformDef,
    target_schema: &str,
    source_table: &str,
    pk: &PrimaryKeyColumn,
) -> Result<(), BackfillError> {
    // Substitute any cross-field-alias reference (e.g. `total = double_price +
    // tax` where `double_price` is itself a field) with a deep copy of the
    // referenced field's expression tree, so every field's expression is
    // self-contained before `render_expr_sql` sees it — it renders each
    // `Column(name)` as a bare source-column reference, which Postgres rejects
    // for a same-SELECT-list alias. A cyclic alias chain falls back to the ring
    // (issue #83 follow-up).
    let substituted = substitute_all_fields(def)?;

    let source = ddl::qualified_source_table(source_table);
    let client = pool.get().await?;
    for (lo, hi) in discover_pk_ranges(&client, &source, pk).await? {
        write_one_to_one_range(
            &client,
            def,
            target_schema,
            source_table,
            pk,
            &substituted,
            &lo,
            &hi,
        )
        .await?;
    }

    Ok(())
}

/// The same `column_status` lookup `staging::quarantine::paused_columns_for`
/// runs from inside a batch's live-CDC apply, reused here (as a plain query
/// against an already-open `&Client`, not a call to that function) for the
/// exact same reason: a durable backfill chunk can be reclaimed and
/// re-executed after a crash (`chunk_queue::reclaim_stale_chunks`), and if
/// live CDC has paused one of this definition's columns in the meantime, a
/// (re-)executed chunk must not silently overwrite that column's frozen
/// value with a freshly (mis)computed one — that would undo the freeze that
/// is the entire point of column-level quarantine (ADR-0003's amendment).
/// Not a call to `staging::quarantine::paused_columns_for` itself: that
/// function takes a `&Pool`, every call site here already holds a `&Client`,
/// and `defs` sits below `staging` in this crate's layering
/// (`staging::apply` already depends on `defs::backfill`, so the reverse
/// dependency would be circular). Empty (the overwhelmingly common case) for
/// a definition with nothing currently paused.
async fn paused_columns_for(
    client: &Client,
    transform_table: &str,
) -> Result<HashSet<String>, BackfillError> {
    let rows = client
        .query(
            "select column_name from column_status where transform_table = $1",
            &[&transform_table],
        )
        .await?;
    Ok(rows.into_iter().map(|row| row.get(0)).collect())
}

/// One `(lo, hi]` PK-range chunk's write — the body [`backfill_one_to_one`]'s
/// loop runs for every range in one call, and the durable chunk queue's
/// [`execute_one_to_one_chunk`] runs for exactly one range claimed off
/// `backfill_chunks` (docs/decisions/0007's "Backgrounding and resumability"
/// amendment). `substituted` is the caller's already-computed
/// [`substitute_all_fields`] output, so a queue-driven caller charged the
/// `Unsupported`-detecting cost once at plan time doesn't pay it again per
/// chunk beyond re-deriving the (cheap, pure) substitution itself.
#[allow(clippy::too_many_arguments)]
async fn write_one_to_one_range(
    client: &Client,
    def: &TransformDef,
    target_schema: &str,
    source_table: &str,
    pk: &PrimaryKeyColumn,
    substituted: &[Expr],
    lo: &Option<String>,
    hi: &str,
) -> Result<(), BackfillError> {
    let source = ddl::qualified_source_table(source_table);
    let target = qualified_target_table(target_schema, def);
    let pk_ident = quote_ident(&pk.name);
    let pk_cast = pk.data_type.as_str();

    // ADR-0003's amendment (column-level quarantine): exclude any column
    // this definition currently has paused from both the computed column
    // list and the `ON CONFLICT` update set — see `paused_columns_for`'s doc
    // comment for why a durable, re-executable chunk write can't skip this.
    let paused = paused_columns_for(client, &def.target).await?;

    let field_idents: Vec<String> = def
        .fields
        .iter()
        .filter(|f| !paused.contains(&f.name))
        .map(|f| quote_ident(&f.name))
        .collect();
    let field_exprs: Vec<String> = def
        .fields
        .iter()
        .zip(substituted)
        .filter(|(f, _)| !paused.contains(&f.name))
        .map(|(_, expr)| render_expr_sql(expr))
        .collect();

    let insert_cols = std::iter::once(pk_ident.clone())
        .chain(field_idents.iter().cloned())
        .collect::<Vec<_>>()
        .join(", ");
    let select_exprs = std::iter::once(pk_ident.clone())
        .chain(field_exprs.iter().cloned())
        .collect::<Vec<_>>()
        .join(", ");
    // Every column of this definition can be paused at once (a single-field
    // definition whose lone column's fuse has tripped is the simplest such
    // case) — mirrors `staging::apply::apply_target`'s identical edge case:
    // `do update set` with an empty set list is invalid SQL, and there is
    // genuinely nothing to update on an existing row anyway; a brand-new key
    // still gets its bare row inserted via the same statement's `insert`
    // half.
    let on_conflict = if field_idents.is_empty() {
        format!("on conflict ({pk_ident}) do nothing")
    } else {
        let update_sets = field_idents
            .iter()
            .map(|f| format!("{f} = excluded.{f}"))
            .collect::<Vec<_>>()
            .join(", ");
        format!("on conflict ({pk_ident}) do update set {update_sets}")
    };

    let where_clause = pk_range_where(&pk_ident, pk_cast, lo);
    let insert_sql = format!(
        "insert into {target} ({insert_cols}) \
         select {select_exprs} from {source} where {where_clause} \
         {on_conflict}"
    );
    match lo {
        None => {
            client.execute(&insert_sql, &[&hi]).await?;
        }
        Some(lo) => {
            client.execute(&insert_sql, &[lo, &hi]).await?;
        }
    }
    Ok(())
}

/// The read-only "planning" half of [`backfill_one_to_one`] (docs/decisions/0007's
/// amendment): fails fast with [`BackfillError::Unsupported`] exactly as
/// `backfill_one_to_one` would (same [`substitute_all_fields`] call), then
/// returns the same `(lo, hi]` PK-range boundaries its loop would have
/// walked — without writing a single row of the target. `defs::catalog::install_definition`
/// calls this instead of `backfill_definition` for a plain (non-relationship)
/// 1-1 definition, persisting the boundaries as durable `backfill_chunks`
/// work items rather than executing them in-call.
pub(crate) async fn plan_one_to_one_chunks(
    pool: &Pool,
    def: &TransformDef,
    source_table: &str,
) -> Result<Vec<(Option<String>, String)>, BackfillError> {
    let _ = substitute_all_fields(def)?;
    let pk = require_single_column_pk(source_primary_key(pool, source_table).await?, source_table)?;
    let source = ddl::qualified_source_table(source_table);
    let client = pool.get().await?;
    discover_pk_ranges(&client, &source, &pk).await
}

/// Executes exactly one previously-[`plan_one_to_one_chunks`]-enumerated
/// chunk — the durable-queue counterpart of [`backfill_one_to_one`]'s loop
/// body, claimed and run by a drain worker
/// (`trellis::client`'s `app_worker_loop`) rather than an in-call loop.
/// Idempotent overwrite, like every chunk write in this module (ADR-0007): a
/// worker that reclaims this chunk after a peer died mid-write redoes it
/// safely.
pub(crate) async fn execute_one_to_one_chunk(
    pool: &Pool,
    def: &TransformDef,
    target_schema: &str,
    source_table: &str,
    lo: Option<&str>,
    hi: &str,
) -> Result<(), BackfillError> {
    let pk = require_single_column_pk(source_primary_key(pool, source_table).await?, source_table)?;
    let substituted = substitute_all_fields(def)?;
    let client = pool.get().await?;
    write_one_to_one_range(
        &client,
        def,
        target_schema,
        source_table,
        &pk,
        &substituted,
        &lo.map(|s| s.to_string()),
        hi,
    )
    .await
}

/// Walks the source primary key in half-open `(lo, hi]` ranges, returning them
/// in order (the first range's `lo` is `None`, meaning "`pk <= hi`"). Each `hi`
/// is the max PK of the next `BACKFILL_CHUNK_ROWS` rows above the previous `hi`
/// (`max()`-over-`LIMIT`); the walk stops when no rows remain above the last
/// boundary. The ranges partition the source exactly once with no gap or
/// overlap regardless of gaps in the key values. Shared by both 1-1 builds so
/// the plain and relationship-enriched paths chunk identically — see
/// [`backfill_one_to_one`]'s doc comment for the off-by-one this guards against.
/// `source` is the already-quoted source table identifier.
async fn discover_pk_ranges(
    client: &Client,
    source: &str,
    pk: &PrimaryKeyColumn,
) -> Result<Vec<(Option<String>, String)>, BackfillError> {
    let pk_ident = quote_ident(&pk.name);
    let pk_cast = pk.data_type.as_str();
    let mut ranges = Vec::new();
    let mut lo: Option<String> = None;
    loop {
        let hi: Option<String> = match &lo {
            None => {
                let row = client
                    .query_one(
                        &format!(
                            "select max({pk_ident})::text from \
                             (select {pk_ident} from {source} \
                              order by {pk_ident} limit {BACKFILL_CHUNK_ROWS}) s"
                        ),
                        &[],
                    )
                    .await?;
                row.get(0)
            }
            Some(lo) => {
                let row = client
                    .query_one(
                        &format!(
                            "select max({pk_ident})::text from \
                             (select {pk_ident} from {source} \
                              where {pk_ident} > $1::text::{pk_cast} \
                              order by {pk_ident} limit {BACKFILL_CHUNK_ROWS}) s"
                        ),
                        &[lo],
                    )
                    .await?;
                row.get(0)
            }
        };
        let Some(hi) = hi else {
            break;
        };
        ranges.push((lo.clone(), hi.clone()));
        lo = Some(hi);
    }
    Ok(ranges)
}

/// The `where` predicate restricting a PK-range chunk to `(lo, hi]`, binding the
/// bounds as `$1` (and `$2` when `lo` is present). Paired with
/// [`discover_pk_ranges`]; the caller binds `hi` (first chunk) or `lo, hi`.
fn pk_range_where(pk_ident: &str, pk_cast: &str, lo: &Option<String>) -> String {
    match lo {
        None => format!("{pk_ident} <= $1::text::{pk_cast}"),
        Some(_) => {
            format!("{pk_ident} > $1::text::{pk_cast} and {pk_ident} <= $2::text::{pk_cast}")
        }
    }
}

/// One aggregate field's build strategy — the direct-build counterpart to
/// `staging::apply_aggregate::AggFieldKind`, kept in lockstep with
/// `classify_fields` there (both route through [`super::invertibility::classify`]
/// so a field lands on the same strategy either way).
///
/// Issue #112 note: `RecomputeOnly` is no longer reachable only for
/// `MIN`/`MAX`. A float `SUM`/`AVG` lands here too — float addition has no
/// exact inverse — which is why [`classify_field`] must be told the field's
/// type rather than assuming `numeric`.
enum FieldKind {
    Sum,
    Avg,
    Count,
    RecomputeOnly,
}

/// Classifies one aggregate field, given the [`ValueType`] the validator
/// inferred for it.
///
/// `value_type` is the field's **result** type, not its argument's, and that
/// is sufficient — every aggregate in
/// [`super::registry::AGGREGATE_FUNCTION_SPECS`] maps a float argument to a
/// float result and an exact argument to an exact one
/// ([`super::registry::aggregate_result_type`]; pinned by that module's
/// `aggregate_results_stay_in_their_argument_s_family` test), so the two
/// agree on the only distinction the invertibility gate draws.
///
/// Passing a hardcoded `ValueType::Numeric` here — which this did before
/// issue #112 — would classify `SUM(<float column>)` as invertible and put
/// it on the delta path, where float addition's non-associativity and
/// `NaN`/`Infinity` absorption would silently drift from a server-side
/// `sum()`. The same hazard #111's review found in the gate itself, one
/// layer up.
fn classify_field(expr: &Expr, value_type: ValueType) -> FieldKind {
    match expr {
        Expr::FunctionCall { name, args } if name == "COUNT" && args.is_empty() => {
            match classify("COUNT", AggregateArg::Count(CountArg::Star)) {
                Some(v) if v.is_invertible() => FieldKind::Count,
                _ => FieldKind::RecomputeOnly,
            }
        }
        Expr::FunctionCall { name, args } if args.len() == 1 => {
            match classify(name, AggregateArg::Column(value_type)) {
                Some(v) if v.is_invertible() && name == "SUM" => FieldKind::Sum,
                Some(v) if v.is_invertible() && name == "AVG" => FieldKind::Avg,
                _ => FieldKind::RecomputeOnly,
            }
        }
        _ => FieldKind::RecomputeOnly,
    }
}

/// A one-argument aggregate call's argument expression — the input to
/// whichever renderer the caller is using, mirroring
/// `staging::apply_aggregate::agg_arg_sql` so `SUM`/`AVG` compute the same
/// `sum(arg)`/`count(arg)` the incremental path's probes do.
fn agg_arg_expr(expr: &Expr) -> &Expr {
    let Expr::FunctionCall { args, .. } = expr else {
        panic!("agg_arg_expr called on a non-function-call field");
    };
    &args[0]
}

/// Every **to-one** relationship `def`'s fields reference, resolved against the
/// catalog to its stored [`RelationshipDef`] and keyed by relationship name, in
/// sorted order so the emitted JOINs (and therefore the SQL text) are
/// deterministic. Issue #94: this is what lets an aggregate build join the
/// to-side before grouping.
///
/// A referenced relationship that is unknown, or resolves to *to-many*, is a
/// shape this build must not render — the validator rejects both in an
/// aggregate definition, so reaching either here means a stored definition
/// predating (or bypassing) that check. Both fall back to the ring with
/// [`BackfillError::Unsupported`] rather than emitting SQL with different
/// semantics.
async fn resolve_to_one_joins(
    pool: &Pool,
    def: &TransformDef,
) -> Result<Vec<(String, RelationshipDef)>, BackfillError> {
    let mut names: Vec<String> = super::eval::relationship_references(def)
        .into_iter()
        .map(|(rel, _column)| rel)
        .collect();
    names.sort();
    names.dedup();

    let mut resolved = Vec::with_capacity(names.len());
    for rel in names {
        let Some(reldef) = super::catalog::relationship_by_name(pool, &def.source, &rel)
            .await
            .map_err(map_rel_lookup_err)?
        else {
            return Err(BackfillError::Unsupported(
                "a definition referencing an unknown relationship".to_string(),
            ));
        };
        if reldef.cardinality != RelationshipCardinality::ToOne {
            return Err(BackfillError::Unsupported(
                "an aggregate over a to-many relationship".to_string(),
            ));
        }
        resolved.push((rel, reldef.def));
    }
    Ok(resolved)
}

/// The aggregate build: aggregate the whole source in a **single** full-table
/// scan into a temporary staging table, then chunk the *writes* from that small
/// (group-count-sized) staging table into the target by group-key range. See
/// the module docs for why overwrite-by-group-key rather than additive-by-PK.
///
/// # Why single-pass-then-chunked-write (issue #63 M3 review)
///
/// An earlier shape chunked by group-key range directly over the *source*: each
/// chunk ran `INSERT … SELECT … FROM source WHERE (<group_cols>) > lo AND
/// (<group_cols>) <= hi GROUP BY …`. The source has no index on the group-key
/// columns (only its PK — an index on the GROUP BY columns was tried and
/// abandoned as ineffective, ADR 0005 / commit e001d8a), so every chunk did a
/// full sequential scan of the entire source filtered to one key range. With
/// `C` chunks that is `O(C × source_size)` total scan work — the exact
/// "rescan-the-whole-table-per-chunk" pathology M1/M2 fixed elsewhere in #63,
/// reappearing here. At 1M distinct groups (100 chunks) it projected to ~12.5s,
/// ~200x the ~60ms single-pass `GROUP BY` floor.
///
/// This design instead scans the source exactly **once** to materialize the
/// aggregate into a staging table (the `CREATE TEMP TABLE … AS SELECT … GROUP
/// BY` below — one seq scan, the ~60ms floor), and every subsequent read is of
/// that staging table, which is *group-count*-sized, not *source*-sized. A
/// primary key on the staging table's group columns turns each chunk's
/// range-write into a cheap index range scan rather than a staging seq scan, so
/// total scan work is `O(source_size)` for the one aggregation pass plus
/// `O(group_count)` for the writes — never `O(C × source_size)`.
///
/// Non-`NULL` group keys are partitioned into `(prev, hi]` ranges over the
/// ordered distinct group tuples in staging, which cover every non-`NULL` group
/// exactly once. A group whose key has a `NULL` component **is** built — issue
/// #128 keys the target's `GROUP BY` columns with `UNIQUE NULLS NOT DISTINCT`
/// rather than a bare `PRIMARY KEY`, precisely so such a row can exist — but a
/// `NULL` component makes Postgres's row-value comparison operators
/// (`<`/`<=`/`>`) return `NULL` rather than `true`/`false` (three-valued
/// logic), which would silently drop that row from every range-chunked
/// write's `WHERE` clause. So NULL-keyed groups are excluded only from this
/// range-chunking scheme, not from the single-pass staging aggregation
/// itself, and are written afterward in one unchunked pass instead (see the
/// `group_key_not_null`/final `insert_for` call below) — matched through the
/// target's `NULLS NOT DISTINCT` constraint, which needs no row-value
/// comparison at all.
async fn backfill_aggregate(
    pool: &Pool,
    def: &TransformDef,
    target_schema: &str,
    source_table: &str,
    group_by: &[GroupByKey],
    source_columns: &HashMap<String, ValueType>,
) -> Result<(), BackfillError> {
    // Substitute any cross-field-alias reference (e.g. `double_total = total +
    // total` where `total` is itself a field) with a deep copy of the
    // referenced field's expression tree, so every field's expression is
    // self-contained before `classify_field`/`render_expr_sql` see it — see
    // [`substituted_field_exprs`]. A cyclic alias chain falls back to the ring.
    let substituted = substituted_field_exprs(def)?;

    let source = ddl::qualified_source_table(source_table);
    let target = qualified_target_table(target_schema, def);

    // Issue #94: an aggregate field may fold a *to-one* relationship path
    // (`SUM(post.word_count)`). Each referenced relationship becomes a LEFT
    // JOIN of its to-side table (aliased by the relationship's name) onto the
    // single source scan below, so the path resolves per source row *before*
    // the GROUP BY folds it — exactly the `LEFT JOIN … GROUP BY` the oracle
    // (`oracle::render_aggregate_relationship_select_sql`) renders. A to-many
    // path here would be a nested aggregation, which the validator rejects; a
    // stored definition that somehow carries one falls back to the ring rather
    // than emitting SQL with different semantics.
    let rel_joins = resolve_to_one_joins(pool, def).await?;
    let joins_sql = super::oracle::to_one_join_clauses(
        rel_joins.iter().map(|(rel, d)| {
            (
                rel.as_str(),
                d.to_table.as_str(),
                d.to_col.as_str(),
                d.from_col.as_str(),
            )
        }),
        &source,
    );
    // With a join in play every source column must be qualified, or a to-side
    // column of the same name makes the reference ambiguous. Without one,
    // render exactly as before so a relationship-free aggregate's SQL is
    // byte-identical to what it has always been.
    let render_field = |expr: &Expr| -> String {
        if rel_joins.is_empty() {
            render_expr_sql(expr)
        } else {
            super::oracle::render_to_one_rel_expr_sql(expr, &source)
        }
    };

    // Issue #137: a `GROUP BY` key's type comes from the to-side column
    // `relationships` reports for a relationship path, or `source_columns`
    // for a plain column — the same split `ddl::create_aggregate_target_table`
    // uses for the same reason (a relationship-free aggregate never resolves
    // any relationships, so this is a cheap no-op call for the overwhelmingly
    // common case).
    let relationships = super::catalog::resolve_relationships(pool, def)
        .await
        .map_err(map_rel_lookup_err)?;
    let group_by_value_type = |key: &GroupByKey| -> ValueType {
        match key {
            GroupByKey::Column(column) => source_columns
                .get(column)
                .copied()
                .unwrap_or(ValueType::Numeric),
            GroupByKey::RelationshipPath { rel, column } => relationships
                .get(rel)
                .and_then(|r| r.column_types.get(column))
                .copied()
                .unwrap_or(ValueType::Numeric),
        }
    };

    // Every field's inferred result type, so `classify_field` can tell a
    // float `SUM` (recompute-only) from an exact one (delta-able) — issue
    // #112. Inferred here rather than threaded in because this is the same
    // call `staging::apply_aggregate::classify_fields` makes for the same
    // purpose, and the two paths must agree.
    // Inference cannot actually fail here — this path only runs for a
    // definition `validate` already accepted — so a failure declines the
    // direct build (`Unsupported`, which the caller falls back to the ring
    // path for) rather than guessing `numeric` and misclassifying a float
    // aggregate.
    let field_types = super::validate::infer_field_types(def, source_columns, &relationships)
        .map_err(|e| {
            BackfillError::Unsupported(format!(
                "cannot infer field types for '{}' in the direct aggregate build: {e}",
                def.target
            ))
        })?;
    let field_value_type =
        |name: &str| -> ValueType { field_types.get(name).copied().unwrap_or(ValueType::Numeric) };

    let group_idents: Vec<String> = group_by
        .iter()
        .map(|k| quote_ident(k.target_column_name()))
        .collect();
    // `render_field` already qualifies a plain column against `source` (or
    // leaves it bare when there's no join) and resolves a relationship path
    // against its own join alias — reused here rather than re-deriving the
    // same qualification rules for `GROUP BY` keys.
    let group_refs: Vec<String> = group_by
        .iter()
        .map(|k| render_field(&k.as_expr()))
        .collect();
    let group_casts: Vec<std::borrow::Cow<'static, str>> = group_by
        .iter()
        .map(|k| ddl::pg_type_name(group_by_value_type(k)))
        .collect();

    // Build the INSERT column list and, for each, the aggregate SELECT
    // expression that computes it from the source — mirroring
    // `apply_forced_groups_bulk`'s per-field-kind construction (issue #63 M3
    // concern #3) so a directly-built target is byte-identical to a ring-built
    // one. Group-key columns come first; every column is a stable target column
    // name, so it doubles as the staging table's column name.
    // Derived from the substituted view (not raw `def.fields`) so a field that
    // only resolves to a bare `SUM`/`AVG` call *after* alias substitution
    // (e.g. `total2 = total` where `total = SUM(amount)`) gets a count-column
    // name consistent with `classify_field`'s own (also substituted)
    // classification in the loop below — mirrors the same
    // classify-then-derive-count-names pattern
    // `staging::apply_aggregate::AggregateTargetPlan::new` already uses over
    // its own substituted `field_exprs`, rather than re-deriving names from a
    // raw-shape-only pass that a purely-aliased field would never match.
    let count_cols = ddl::count_column_names_from(def.fields.iter().filter_map(|f| {
        if group_by_contains(group_by, &f.name) {
            return None;
        }
        let expr = &substituted[&f.name];
        match (classify_field(expr, field_value_type(&f.name)), expr) {
            (FieldKind::Sum | FieldKind::Avg, Expr::FunctionCall { args, .. }) => {
                args.first().map(|arg| (f.name.as_str(), arg))
            }
            _ => None,
        }
    }));
    let mut insert_cols: Vec<String> = group_idents.clone();
    let mut stage_exprs: Vec<String> = group_refs.clone();
    // Issue #48: two fields (e.g. `SUM(amount)`/`AVG(amount)`) can share one
    // hidden count column (`count_cols`) — track which shared names have
    // already been emitted into this staging table's column list, so a
    // second field sharing a column never emits a duplicate, which both
    // `CREATE TEMP TABLE ... AS SELECT` and the later `ON CONFLICT DO
    // UPDATE` reject.
    let mut emitted_count_cols: std::collections::HashSet<String> =
        std::collections::HashSet::new();
    for field in &def.fields {
        if group_by_contains(group_by, &field.name) {
            continue;
        }
        let col = quote_ident(&field.name);
        let expr = &substituted[&field.name];
        match classify_field(expr, field_value_type(&field.name)) {
            FieldKind::Sum => {
                let arg = render_field(agg_arg_expr(expr));
                insert_cols.push(col);
                stage_exprs.push(format!("sum({arg})"));
                let count_col_name = count_cols[&field.name].clone();
                if emitted_count_cols.insert(count_col_name.clone()) {
                    insert_cols.push(quote_ident(&count_col_name));
                    stage_exprs.push(format!("count({arg})"));
                }
            }
            FieldKind::Avg => {
                let arg = render_field(agg_arg_expr(expr));
                let sum_col = avg_sum_column(&field.name);
                let count_col_name = count_cols[&field.name].clone();
                insert_cols.push(quote_ident(&sum_col));
                stage_exprs.push(format!("sum({arg})"));
                if emitted_count_cols.insert(count_col_name.clone()) {
                    insert_cols.push(quote_ident(&count_col_name));
                    stage_exprs.push(format!("count({arg})"));
                }
                insert_cols.push(col);
                stage_exprs.push(format!(
                    "case when count({arg}) = 0 then null \
                     else sum({arg}) / count({arg})::numeric end"
                ));
            }
            FieldKind::Count => {
                insert_cols.push(col);
                stage_exprs.push("count(*)::numeric".to_string());
            }
            FieldKind::RecomputeOnly => {
                insert_cols.push(col);
                stage_exprs.push(format!("({})", render_field(expr)));
            }
        }
    }

    let arity = group_idents.len();
    let update_sets: Vec<String> = insert_cols
        .iter()
        .skip(arity)
        .map(|c| format!("{c} = excluded.{c}"))
        .collect();
    debug_assert!(!update_sets.is_empty());

    let insert_cols_sql = insert_cols.join(", ");
    // `group_by_sql` runs against the *source* (possibly joined, hence
    // qualified); `group_tuple`/`conflict_sql`/`group_key_not_null`/the
    // boundary query all run against the staging table or the target, whose
    // columns are the bare target column names.
    let group_by_sql = group_refs.join(", ");
    let group_tuple = group_idents.join(", ");
    let conflict_sql = group_idents.join(", ");
    let update_sets_sql = update_sets.join(", ");
    // Issue #128: a source grouping column can itself be NULL — `GROUP BY`
    // folds every NULL in a column into one group, same as any other value —
    // so this can no longer be used to *filter* the staging build (that
    // silently dropped every NULL-keyed group). It still names the subset of
    // groups the row-value range-chunking below can safely handle: a NULL
    // component makes Postgres's row-comparison operators (`<`, `<=`, `>`)
    // return NULL rather than `true`/`false` (SQL three-valued logic), which
    // would silently exclude that row from every chunk's `WHERE` — the same
    // drop, just moved from staging-build time to write time. NULL-keyed
    // groups are therefore written in one unchunked pass after the loop
    // instead, where `ON CONFLICT` matches them through the target's
    // NULLS-NOT-DISTINCT unique constraint rather than a row comparison.
    let group_key_not_null = group_idents
        .iter()
        .map(|c| format!("{c} is not null"))
        .collect::<Vec<_>>()
        .join(" and ");
    // Each staging column aliased to its target column name, so the staging
    // table's columns line up with `insert_cols` for a plain SELECT on write.
    let stage_select_sql = insert_cols
        .iter()
        .zip(&stage_exprs)
        .map(|(col, expr)| format!("{expr} as {col}"))
        .collect::<Vec<_>>()
        .join(", ");

    let client = pool.get().await?;

    // Single full-table scan: aggregate the whole source — NULL-keyed groups
    // included — into a connection-scoped staging table. This is the ~60ms
    // `GROUP BY` floor and the *only* pass over the source. The staging table
    // has one row per group. Drop first in case a crashed prior backfill on
    // this pooled connection left one behind; drop again at the end so it
    // doesn't leak back into the pool.
    client
        .batch_execute(&format!("drop table if exists {STAGE_TABLE}"))
        .await?;
    client
        .execute(
            &format!(
                "create temp table {STAGE_TABLE} as \
                 select {stage_select_sql} from {source}{joins_sql} \
                 group by {group_by_sql}"
            ),
            &[],
        )
        .await?;
    // A unique key on the group columns (not a bare `PRIMARY KEY`, which
    // would reject the NULL-keyed group's row the same way the target's own
    // pre-#128 `PRIMARY KEY` did) makes each chunk's range-write below an
    // index range scan of the staging table rather than a full staging scan
    // — the group tuple is unique in the aggregated result, so it is a valid
    // key either way.
    client
        .batch_execute(&format!(
            "alter table {STAGE_TABLE} add unique nulls not distinct ({group_tuple})"
        ))
        .await?;

    let insert_for = |where_clause: &str| {
        format!(
            "insert into {target} ({insert_cols_sql}) \
             select {insert_cols_sql} from {STAGE_TABLE}{where_clause} \
             on conflict ({conflict_sql}) do update set {update_sets_sql}"
        )
    };

    // Discover group-key range boundaries among the non-NULL-keyed groups
    // only (see `group_key_not_null`'s doc above): every BACKFILL_CHUNK_GROUPS-th
    // group tuple in ascending order.
    let boundary_select_text = group_idents
        .iter()
        .map(|c| format!("{c}::text"))
        .collect::<Vec<_>>()
        .join(", ");
    let boundary_sql = format!(
        "select {boundary_select_text} from ( \
             select {group_tuple}, row_number() over (order by {group_tuple}) as rn \
             from {STAGE_TABLE} where {group_key_not_null} \
         ) x where x.rn % {BACKFILL_CHUNK_GROUPS} = 0 order by {group_tuple}"
    );
    let boundary_rows = client.query(&boundary_sql, &[]).await?;
    let boundaries: Vec<Vec<Option<String>>> = boundary_rows
        .iter()
        .map(|row| {
            (0..arity)
                .map(|i| row.get::<_, Option<String>>(i))
                .collect()
        })
        .collect();

    // A row-value comparison `(g1, g2, …) <op> (b1::t1, b2::t2, …)` against a
    // bound tuple, binding the bound components as $start.. text and casting each
    // to its group column's type. Boundaries are drawn only from non-NULL-keyed
    // groups (see above), so every component is `Some`, but `Option<String>` is
    // what the row getter yields, so unwrap defensively.
    let tuple_cmp = |op: &str, start: usize| -> String {
        let lhs = group_tuple.clone();
        let rhs = (0..arity)
            .map(|i| format!("${}::text::{}", start + i, group_casts[i]))
            .collect::<Vec<_>>()
            .join(", ");
        format!("({lhs}) {op} ({rhs})")
    };

    let mut prev: Option<Vec<Option<String>>> = None;
    for hi in &boundaries {
        let where_clause = match &prev {
            None => format!(" where {group_key_not_null} and {}", tuple_cmp("<=", 1)),
            Some(_) => format!(
                " where {group_key_not_null} and {} and {}",
                tuple_cmp(">", 1),
                tuple_cmp("<=", arity + 1),
            ),
        };
        let sql = insert_for(&where_clause);
        let mut params: Vec<String> = Vec::new();
        if let Some(prev) = &prev {
            params.extend(prev.iter().map(|v| v.clone().unwrap_or_default()));
        }
        params.extend(hi.iter().map(|v| v.clone().unwrap_or_default()));
        let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
            params.iter().map(|p| p as _).collect();
        client.execute(&sql, &param_refs).await?;
        prev = Some(hi.clone());
    }

    // Final open-ended range above the last boundary — or, when there were no
    // boundaries at all (non-NULL-keyed group count <= BACKFILL_CHUNK_GROUPS),
    // the single range covering every non-NULL-keyed group in staging.
    match &prev {
        None => {
            client
                .execute(&insert_for(&format!(" where {group_key_not_null}")), &[])
                .await?;
        }
        Some(prev) => {
            let clause = format!(" where {group_key_not_null} and {}", tuple_cmp(">", 1));
            let params: Vec<String> = prev.iter().map(|v| v.clone().unwrap_or_default()).collect();
            let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
                params.iter().map(|p| p as _).collect();
            client.execute(&insert_for(&clause), &param_refs).await?;
        }
    }

    // NULL-keyed groups: any grouping column is NULL, so no row-value range
    // comparison is safe (see `group_key_not_null`'s doc above). Writing them
    // is a single unchunked pass — `ON CONFLICT` resolves them through the
    // target's `NULLS NOT DISTINCT` unique constraint (issue #128), whose
    // conflict-arbiter matching needs no row-value comparison at all. NULL
    // keys are expected to be a small minority of groups, so skipping the
    // chunking optimization for them costs little.
    client
        .execute(
            &insert_for(&format!(" where not ({group_key_not_null})")),
            &[],
        )
        .await?;

    // Return the staging table to a clean slate before the connection goes back
    // to the pool.
    client
        .batch_execute(&format!("drop table if exists {STAGE_TABLE}"))
        .await?;

    Ok(())
}

/// Prefix for the connection-scoped staging tables the relationship build
/// materializes one per referenced to-many relationship. Safe as a fixed name
/// for the same reason [`STAGE_TABLE`] is: the build holds one pooled
/// connection for its whole duration and drops each table before creating it
/// (crash leftover) and after the writes.
const REL_STAGE_TABLE_PREFIX: &str = "_trellis_backfill_rel_staging_";

/// A distinct to-many-aggregate leaf: `agg(rel.column)` — an aggregate
/// function whose sole argument is a relationship path. Ordered `(rel, agg,
/// column)` so a set of leaves gets a deterministic, field-name-independent
/// staging-column assignment (see [`backfill_relationship_one_to_one`]).
type AggLeaf = (String, String, String);

/// If `name(args)` is a to-many-aggregate leaf `agg(rel.column)`, returns
/// `(rel, column)`. This is the exact structural shape the evaluator
/// (`eval::eval_to_many_aggregate`) and oracle (`oracle::render_rel_expr_sql`)
/// match; here it can appear anywhere in a (substituted) expression tree, not
/// just at its top level.
fn agg_leaf_parts(name: &str, args: &[Expr]) -> Option<(String, String)> {
    if lookup_aggregate_function(name).is_some()
        && let [Expr::RelationshipPath { rel, column }] = args
    {
        Some((rel.clone(), column.clone()))
    } else {
        None
    }
}

/// Walks a (substituted) relationship-enriched field expression, collecting
/// every distinct to-many-aggregate leaf `agg(rel.column)` into `leaves`, and
/// validating that the tree contains only shapes the direct build can render:
/// real source columns, literals, aggregate leaves, and composition
/// (`BinaryOp`/`FunctionCall`, including `coalesce(agg(rel.col), literal)` —
/// the aggregate nested inside the wrapping function is collected as a leaf,
/// and the wrapper renders generically). A bare relationship path — a to-one
/// lookup (`category.name`), or any relationship reference that isn't the sole
/// argument of an aggregate — is a shape this build can't render and yields
/// [`BackfillError::Unsupported`], routing the whole definition to the ring.
fn collect_agg_leaves(expr: &Expr, leaves: &mut BTreeSet<AggLeaf>) -> Result<(), BackfillError> {
    match expr {
        Expr::Column(_)
        | Expr::NumberLiteral(_)
        | Expr::StringLiteral(_)
        | Expr::TypedLiteral { .. } => Ok(()),
        Expr::RelationshipPath { .. } => Err(BackfillError::Unsupported(
            "a bare to-one relationship lookup, or a relationship reference nested inside a \
             larger expression"
                .to_string(),
        )),
        Expr::BinaryOp { lhs, rhs, .. } => {
            collect_agg_leaves(lhs, leaves)?;
            collect_agg_leaves(rhs, leaves)
        }
        Expr::FunctionCall { name, args } => {
            if let Some((rel, column)) = agg_leaf_parts(name, args) {
                leaves.insert((rel, name.clone(), column));
                Ok(())
            } else {
                for arg in args {
                    collect_agg_leaves(arg, leaves)?;
                }
                Ok(())
            }
        }
    }
}

/// Renders a (substituted) relationship-enriched field expression to SQL over
/// the source `LEFT JOIN`ed to each relationship's staging table — the
/// staged-column counterpart of `oracle::render_rel_expr_sql`, which renders
/// the same aggregates as correlated subqueries instead. Source columns are
/// qualified with the source table; a to-many-aggregate leaf reads its
/// pre-aggregated staging column via `leaf_cols`/`rel_stage` (with `COUNT`
/// coalesced to `0` for the empty set — the `LEFT JOIN`'s no-match `NULL`
/// otherwise carries the empty-set result, which is `NULL` for every other
/// aggregate); a `coalesce(agg(rel.col), literal)` therefore renders as
/// `coalesce(<staged-ref>, <literal>)` through the generic function arm, so it
/// is just one instance of the general pattern rather than a special case.
/// Returns `None` if a relationship path or an aggregate leaf without a
/// staging column survives (which validation via [`collect_agg_leaves`] should
/// already have ruled out — but this fails safe rather than panicking).
fn render_rel_field_direct(
    expr: &Expr,
    source: &str,
    rel_stage: &HashMap<String, String>,
    leaf_cols: &HashMap<AggLeaf, String>,
) -> Option<String> {
    match expr {
        Expr::Column(name) => Some(format!("{}.{}", quote_ident(source), quote_ident(name))),
        Expr::NumberLiteral(text) => Some(format!("{text}::numeric")),
        Expr::StringLiteral(text) => Some(format!("'{}'::text", text.replace('\'', "''"))),
        Expr::TypedLiteral { value_type, text } => {
            Some(super::typed_literal::render_sql(*value_type, text))
        }
        Expr::RelationshipPath { .. } => None,
        Expr::BinaryOp { op, lhs, rhs } => {
            let symbol = match op {
                Operator::Add => "+",
                Operator::GreaterThan => ">",
            };
            let lhs = render_rel_field_direct(lhs, source, rel_stage, leaf_cols)?;
            let rhs = render_rel_field_direct(rhs, source, rel_stage, leaf_cols)?;
            Some(format!("({lhs} {symbol} {rhs})"))
        }
        Expr::FunctionCall { name, args } => {
            if let Some((rel, column)) = agg_leaf_parts(name, args) {
                let col = leaf_cols.get(&(rel.clone(), name.clone(), column))?;
                let stage = rel_stage.get(&rel)?;
                let stage_ref = format!("{stage}.{}", quote_ident(col));
                if name == "COUNT" {
                    Some(format!("coalesce({stage_ref}, 0)"))
                } else {
                    Some(stage_ref)
                }
            } else if name == "COUNT" && args.is_empty() {
                Some("count(*)".to_string())
            } else {
                let rendered: Option<Vec<String>> = args
                    .iter()
                    .map(|arg| render_rel_field_direct(arg, source, rel_stage, leaf_cols))
                    .collect();
                Some(format!("{}({})", name.to_lowercase(), rendered?.join(", ")))
            }
        }
    }
}

/// Maps a relationship-lookup [`super::catalog::CatalogError`] into a
/// [`BackfillError`]. Only the DB/pool variants arise in the real install flow
/// (a referenced relationship's metadata was already resolved and validated by
/// `create_target_table` just before this build runs); a stored-definition
/// re-parse failure (corruption/parser drift) falls back to the ring, which
/// re-parses and surfaces the real error itself.
fn map_rel_lookup_err(err: super::catalog::CatalogError) -> BackfillError {
    use super::catalog::CatalogError;
    match err {
        CatalogError::Db(e) => BackfillError::Db(e),
        CatalogError::Pool(e) => BackfillError::Pool(e),
        _ => BackfillError::Unsupported(
            "a relationship whose stored definition failed to resolve".to_string(),
        ),
    }
}

/// The relationship-enriched 1-1 build: computes a target whose fields
/// aggregate over to-many relationship paths (`SUM(posts.x)`, `COUNT(comments)`,
/// `coalesce(sum(posts.x), 0)`, `post_count + comment_count`) directly with
/// set-based SQL, instead of staging every source row as a `Recompute` marker
/// for per-row Rust evaluation.
///
/// Each field's expression is first made self-contained by
/// [`substitute_all_fields`] — a cross-field-alias reference like `total =
/// post_count + comment_count` becomes `count(posts.id) + count(comments.id)`
/// in place, so a to-many-aggregate leaf `agg(rel.col)` (bare, or nested
/// anywhere in an arbitrary `BinaryOp`/`FunctionCall` tree, e.g. inside a
/// `coalesce(_, literal)`) is the only relationship shape the tree can hold.
/// [`collect_agg_leaves`] gathers the *distinct* such leaves across all fields
/// and rejects any unsupported shape (a bare to-one path, or an aggregate over
/// a to-one relationship) to the ring.
///
/// Each referenced to-many relationship is aggregated over its whole to-side
/// table **once** into a connection-scoped staging table grouped by the join
/// key (one row per distinct key, primary-keyed for cheap index probes) — the
/// same single-pass-then-chunked-write shape [`backfill_aggregate`] uses, so
/// the to-side is never re-scanned per chunk. That staging table carries one
/// column per *distinct aggregate leaf* reading the relationship — keyed by the
/// leaf itself, not by field name, and named by a synthetic collision-free
/// index (`_agg_0`, …) — so a leaf shared by several fields (`count(posts.id)`
/// appearing in both `post_count` and `post_count + comment_count`) is computed
/// and staged exactly once. The target is then written in source-primary-key
/// range chunks (reusing [`discover_pk_ranges`], identical to
/// [`backfill_one_to_one`]): each chunk `INSERT … SELECT`s a bounded PK range
/// of the source `LEFT JOIN`ed to every relationship's staging table on its
/// join key, with each field rendered by [`render_rel_field_direct`] against
/// those staged columns.
///
/// The result is byte-identical to the correlated-subquery oracle
/// (`oracle::render_relationship_select_sql`) and the per-row evaluator
/// (`eval::eval_to_many_aggregate`): a grouped aggregate over the matching
/// to-side rows equals the correlated aggregate over the same rows, and the
/// `LEFT JOIN`'s no-match `NULL` reproduces Postgres's empty-correlated-set
/// semantics (`SUM`/`MIN`/`MAX`/`AVG` → `NULL`), with a bare `COUNT` `coalesce`d
/// to `0` to match `count` over the empty set. Unlike the aggregate build, this
/// target carries no hidden partial columns: a relationship-enriched 1-1 target
/// has none (see `ddl::create_target_table`) — its live CDC deltas are applied
/// by a full per-parent recompute in the ring, not an incremental partial fold,
/// so there is nothing here to keep in lockstep.
///
/// Falls back with [`BackfillError::Unsupported`] (routing the whole definition
/// to the ring) if any field is a shape this build can't render exactly — a
/// bare to-one lookup (bare or nested), an aggregate over a relationship that
/// resolves to a to-one, an unknown relationship, or a cyclic alias chain.
async fn backfill_relationship_one_to_one(
    pool: &Pool,
    def: &TransformDef,
    target_schema: &str,
    source_table: &str,
    pk: &PrimaryKeyColumn,
) -> Result<(), BackfillError> {
    // Resolve every referenced relationship to its endpoints + cardinality the
    // same way the rest of the catalog does (`relationship_by_name`), so this
    // build reads the identical join metadata the ring/oracle do.
    let mut rel_defs: HashMap<String, RelationshipDef> = HashMap::new();
    let mut rel_cardinality: HashMap<String, RelationshipCardinality> = HashMap::new();
    for (rel, _column) in super::eval::relationship_references(def) {
        if rel_defs.contains_key(&rel) {
            continue;
        }
        let Some(reldef) = super::catalog::relationship_by_name(pool, &def.source, &rel)
            .await
            .map_err(map_rel_lookup_err)?
        else {
            // Unknown relationship name: the direct build can't render it — let
            // the ring path surface the same `UnknownRelationship` the
            // evaluator/validator would.
            return Err(BackfillError::Unsupported(
                "a definition referencing an unknown relationship".to_string(),
            ));
        };
        rel_cardinality.insert(rel.clone(), reldef.cardinality);
        rel_defs.insert(rel, reldef.def);
    }

    // Make every field self-contained (substituting cross-field-alias
    // references), then collect the distinct to-many-aggregate leaves across
    // all fields — bailing to the ring on any unsupported shape (a bare to-one
    // path, a nested relationship reference, or a cyclic alias chain).
    let substituted = substitute_all_fields(def)?;
    let mut leaves: BTreeSet<AggLeaf> = BTreeSet::new();
    for expr in &substituted {
        collect_agg_leaves(expr, &mut leaves)?;
    }

    // Every aggregate leaf must resolve to a known to-many relationship; an
    // aggregate over a to-one (or an unknown relationship) is a shape the
    // validator rejects — fall back rather than emit wrong SQL for it.
    for (rel, _agg, _column) in &leaves {
        match rel_cardinality.get(rel) {
            Some(RelationshipCardinality::ToMany) => {}
            Some(_) => {
                return Err(BackfillError::Unsupported(
                    "an aggregate over a to-one relationship".to_string(),
                ));
            }
            None => {
                return Err(BackfillError::Unsupported(
                    "a definition referencing an unknown relationship".to_string(),
                ));
            }
        }
    }

    // Assign each distinct leaf a synthetic, collision-free staging-column
    // name keyed by its index in the sorted leaf set — never by a field name,
    // since one leaf may be shared by several fields (and thus belong to no
    // single field) once cross-field aliases are substituted.
    let leaf_cols: HashMap<AggLeaf, String> = leaves
        .iter()
        .enumerate()
        .map(|(i, leaf)| (leaf.clone(), format!("_agg_{i}")))
        .collect();

    let source = ddl::qualified_source_table(source_table);
    let target = qualified_target_table(target_schema, def);
    let pk_ident = quote_ident(&pk.name);
    let pk_cast = pk.data_type.as_str();
    let pk_qualified = format!("{source}.{pk_ident}");

    let client = pool.get().await?;

    // The relationships that actually carry an aggregate leaf, in a
    // deterministic order for stable staging-table indices. (Every relationship
    // in `rel_defs` was referenced by the def; any referenced only by an
    // unsupported shape already bailed to the ring in `collect_agg_leaves`.)
    let mut rel_names: Vec<String> = leaves.iter().map(|(rel, ..)| rel.clone()).collect();
    rel_names.sort();
    rel_names.dedup();

    // Materialize one staging table per such relationship: its to-side table
    // aggregated by the join key, one column per distinct aggregate leaf
    // reading it (aliased to the leaf's synthetic name). `rel_stage` maps a
    // relationship name to its staging table's identifier so the chunked write
    // below can `LEFT JOIN` it.
    let mut rel_stage: HashMap<String, String> = HashMap::new();
    for (i, rel) in rel_names.iter().enumerate() {
        let stage = format!("{REL_STAGE_TABLE_PREFIX}{i}");
        let reldef = &rel_defs[rel];
        let agg_exprs: Vec<String> = leaves
            .iter()
            .filter(|(leaf_rel, ..)| leaf_rel == rel)
            .map(|leaf| {
                let (_rel, agg, column) = leaf;
                format!(
                    "{}({}) as {}",
                    agg.to_lowercase(),
                    quote_ident(column),
                    quote_ident(&leaf_cols[leaf]),
                )
            })
            .collect();
        let to_table = quote_ident(&reldef.to_table);
        let to_col = quote_ident(&reldef.to_col);
        client
            .batch_execute(&format!("drop table if exists {stage}"))
            .await?;
        // Group by the join key; a NULL key never joins (SQL `NULL != NULL`), so
        // filtering it out is harmless and lets the key be a primary key.
        client
            .execute(
                &format!(
                    "create temp table {stage} as \
                     select {to_col} as _k, {aggs} from {to_table} \
                     where {to_col} is not null group by {to_col}",
                    aggs = agg_exprs.join(", "),
                ),
                &[],
            )
            .await?;
        client
            .batch_execute(&format!("alter table {stage} add primary key (_k)"))
            .await?;
        rel_stage.insert(rel.clone(), stage);
    }

    // ADR-0003's amendment (column-level quarantine): exclude any column
    // this definition currently has paused, same as `write_one_to_one_range`
    // — see that function's `paused_columns_for` doc comment.
    let paused = paused_columns_for(&client, &def.target).await?;

    // Build the INSERT's column list, its per-field SELECT expression, and the
    // ON CONFLICT update set. The primary key comes first, then one column per
    // (non-paused) field in definition order (mirroring `backfill_one_to_one`).
    // Each field's (substituted) expression renders against the staged
    // aggregate columns.
    let field_idents: Vec<String> = def
        .fields
        .iter()
        .filter(|f| !paused.contains(&f.name))
        .map(|f| quote_ident(&f.name))
        .collect();
    let select_field_exprs: Vec<String> = def
        .fields
        .iter()
        .zip(substituted.iter())
        .filter(|(f, _)| !paused.contains(&f.name))
        .map(|(_, expr)| {
            render_rel_field_direct(expr, &def.source, &rel_stage, &leaf_cols).ok_or_else(|| {
                BackfillError::Unsupported(
                    "a relationship-enriched 1-1 field the direct build can't render".to_string(),
                )
            })
        })
        .collect::<Result<_, _>>()?;

    let insert_cols = std::iter::once(pk_ident.clone())
        .chain(field_idents.iter().cloned())
        .collect::<Vec<_>>()
        .join(", ");
    let select_exprs = std::iter::once(pk_qualified.clone())
        .chain(select_field_exprs.iter().cloned())
        .collect::<Vec<_>>()
        .join(", ");
    // Every column of this definition can be paused at once — mirrors
    // `write_one_to_one_range`'s identical edge case (see its own doc
    // comment): `do update set` with an empty set list is invalid SQL, and a
    // brand-new key still gets its bare row inserted via the same
    // statement's `insert` half.
    let on_conflict = if field_idents.is_empty() {
        format!("on conflict ({pk_ident}) do nothing")
    } else {
        let update_sets = field_idents
            .iter()
            .map(|f| format!("{f} = excluded.{f}"))
            .collect::<Vec<_>>()
            .join(", ");
        format!("on conflict ({pk_ident}) do update set {update_sets}")
    };

    // Each relationship's staging table LEFT JOINed to the source on its join
    // key, so a source row with no matching to-side rows survives with NULL
    // aggregates (Postgres's empty-correlated-set semantics).
    let joins = rel_names
        .iter()
        .map(|rel| {
            let stage = &rel_stage[rel];
            let from_col = quote_ident(&rel_defs[rel].from_col);
            format!("left join {stage} on {stage}._k = {source}.{from_col}")
        })
        .collect::<Vec<_>>()
        .join(" ");

    for (lo, hi) in discover_pk_ranges(&client, &source, pk).await? {
        let where_clause = pk_range_where(&pk_qualified, pk_cast, &lo);
        let insert_sql = format!(
            "insert into {target} ({insert_cols}) \
             select {select_exprs} from {source} {joins} where {where_clause} \
             {on_conflict}"
        );
        match &lo {
            None => {
                client.execute(&insert_sql, &[&hi]).await?;
            }
            Some(lo) => {
                client.execute(&insert_sql, &[lo, &hi]).await?;
            }
        }
    }

    // Drop the staging tables before the connection returns to the pool.
    for stage in rel_stage.values() {
        client
            .batch_execute(&format!("drop table if exists {stage}"))
            .await?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::ast::{FieldDef, Operator, Predicate};
    use super::*;

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

    fn agg(name: &str, rel: &str, column: &str) -> Expr {
        Expr::FunctionCall {
            name: name.to_string(),
            args: vec![Expr::RelationshipPath {
                rel: rel.to_string(),
                column: column.to_string(),
            }],
        }
    }

    fn def_with(fields: Vec<FieldDef>) -> TransformDef {
        TransformDef {
            target: "t".to_string(),
            source: "s".to_string(),
            key_space: KeySpace::OneToOne,
            fields,
            predicate: Predicate::True,
            explicit_source_schema: None,
            explicit_target_schema: None,
        }
    }

    /// `total = double_price + tax`, where `double_price` is itself a
    /// calculated field (`price + price`) — the alias-derived shape issue #83
    /// asks the plain direct build to render. Substitution must inline
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

    #[test]
    fn collect_agg_leaves_gathers_distinct_leaves() {
        // `count(posts.id) + count(comments.id)` — two distinct leaves.
        let expr = add(agg("COUNT", "posts", "id"), agg("COUNT", "comments", "id"));
        let mut leaves = BTreeSet::new();
        collect_agg_leaves(&expr, &mut leaves).expect("supported");
        assert_eq!(
            leaves,
            BTreeSet::from([
                (
                    "comments".to_string(),
                    "COUNT".to_string(),
                    "id".to_string()
                ),
                ("posts".to_string(), "COUNT".to_string(), "id".to_string()),
            ])
        );
    }

    #[test]
    fn collect_agg_leaves_sees_through_coalesce() {
        // `coalesce(sum(posts.word_count), 0)` — the aggregate nested inside a
        // wrapping function is collected as a leaf.
        let expr = Expr::FunctionCall {
            name: "COALESCE".to_string(),
            args: vec![
                agg("SUM", "posts", "word_count"),
                Expr::NumberLiteral("0".to_string()),
            ],
        };
        let mut leaves = BTreeSet::new();
        collect_agg_leaves(&expr, &mut leaves).expect("supported");
        assert_eq!(
            leaves,
            BTreeSet::from([(
                "posts".to_string(),
                "SUM".to_string(),
                "word_count".to_string()
            )])
        );
    }

    #[test]
    fn collect_agg_leaves_rejects_a_bare_to_one_path() {
        let expr = Expr::RelationshipPath {
            rel: "category".to_string(),
            column: "name".to_string(),
        };
        let mut leaves = BTreeSet::new();
        assert!(matches!(
            collect_agg_leaves(&expr, &mut leaves),
            Err(BackfillError::Unsupported(_))
        ));
    }

    #[test]
    fn render_rel_field_direct_renders_leaves_and_coalesce() {
        let rel_stage: HashMap<String, String> =
            HashMap::from([("posts".to_string(), "stage0".to_string())]);
        let leaf_cols: HashMap<AggLeaf, String> = HashMap::from([
            (
                ("posts".to_string(), "COUNT".to_string(), "id".to_string()),
                "_agg_0".to_string(),
            ),
            (
                (
                    "posts".to_string(),
                    "SUM".to_string(),
                    "word_count".to_string(),
                ),
                "_agg_1".to_string(),
            ),
        ]);

        // Bare COUNT keeps empty-set-is-0.
        assert_eq!(
            render_rel_field_direct(&agg("COUNT", "posts", "id"), "s", &rel_stage, &leaf_cols)
                .unwrap(),
            r#"coalesce(stage0."_agg_0", 0)"#
        );
        // Bare SUM is left as the LEFT JOIN's NULL.
        assert_eq!(
            render_rel_field_direct(
                &agg("SUM", "posts", "word_count"),
                "s",
                &rel_stage,
                &leaf_cols
            )
            .unwrap(),
            r#"stage0."_agg_1""#
        );
        // `coalesce(sum(...), 0)` renders through the generic function arm.
        let coalesced = Expr::FunctionCall {
            name: "COALESCE".to_string(),
            args: vec![
                agg("SUM", "posts", "word_count"),
                Expr::NumberLiteral("0".to_string()),
            ],
        };
        assert_eq!(
            render_rel_field_direct(&coalesced, "s", &rel_stage, &leaf_cols).unwrap(),
            r#"coalesce(stage0."_agg_1", 0::numeric)"#
        );
        // A plain source column is qualified with the source table.
        assert_eq!(
            render_rel_field_direct(&col("name"), "s", &rel_stage, &leaf_cols).unwrap(),
            r#""s"."name""#
        );
    }
}
