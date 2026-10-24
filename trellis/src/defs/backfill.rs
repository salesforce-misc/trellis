//! Direct, set-based, key-range-chunked backfill of a definition's target
//! straight from its source (issue #63 milestone 3).
//!
//! # Why this exists
//!
//! A from-scratch backfill used to run entirely through the staging ring:
//! [`super::catalog::create_definition`] enumerates every source row as a
//! `Recompute` marker (`intake::markers::enumerate_and_append`), which the
//! ring then folds and applies. That stages one marker per source row, folds
//! the whole table in a single ring segment, and applies it as one giant
//! transaction — the cost M0's benchmark measured and M1/M2 chipped at.
//!
//! This module bypasses the ring for the initial build: it computes the target
//! directly with server-side `INSERT … SELECT` statements, chunked by key
//! range so each statement is one bounded transaction. The ring is then left
//! to carry only live CDC deltas that land *after* the build (see the fence
//! discussion below). The backfill discharge (ADR-0002) dispatches it by
//! shape instead of a ring enumeration: a plain 1-1 definition's primary-key
//! ranges as durable chunks ([`plan_one_to_one_chunks`]). An aggregate is
//! built by its plan job (`staging::group_basis::rebuild`), never here.
//!
//! # Correctness — the five concerns issue #63 M3 calls out
//!
//! 1. **Exhaustive, disjoint chunking.** The 1-1 build walks the source
//!    primary key in half-open ranges `(lo, hi]` discovered by
//!    `max()`-over-`LIMIT` (see [`backfill_one_to_one`]): every source row's PK
//!    falls in exactly one range, with no gap or overlap regardless of gaps in
//!    the key values.
//!
//! 2. **The build/CDC fence.** The build writes with `ON CONFLICT DO UPDATE
//!    SET col = excluded.col` — an *overwrite* that recomputes each target row
//!    from the current source, identical in effect to the
//!    ring's own image-less `Recompute` path. Overwrite is idempotent and
//!    order-independent, so the handoff to the ring is the same one the ring
//!    already relies on: live CDC isn't applied to the definition until the
//!    build has finished and flipped it `live`, and any genuine post-build
//!    delta the ring later applies lands on a fully-built row. The build runs
//!    as a background job after its source's capture was installed (ADR-0002,
//!    issue #419), so a commit it read can also be captured and drain after
//!    the flip, where it overwrites the row again from its own image.
//!
//! 3. **Per-field-kind aggregates.** An aggregate isn't built here: the group
//!    basis builds it with one absolute write (`staging::group_basis`).
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

use std::collections::{HashMap, HashSet};

use super::chunk_queue::ClaimFence;
use crate::error_code::{self, ErrorCode};
use crate::pool::{Pool, quote_ident};
use tokio_postgres::GenericClient;

use super::ast::{Expr, KeySpace, TransformDef, ValueType};
use super::ddl::{self, PrimaryKeyColumn, qualified_target_table, source_primary_key};
use super::oracle::render_expr_sql;

/// Rows per chunk for the 1-1 primary-key-range build. Each chunk is one
/// bounded transaction; 50k keeps a chunk's write set well within a
/// comfortable transaction size while keeping the number of round trips low
/// for a million-row source.
const BACKFILL_CHUNK_ROWS: i64 = 50_000;

/// Why a direct backfill could not run.
#[derive(Debug)]
pub enum BackfillError {
    /// A direct Postgres protocol/query error.
    Db(tokio_postgres::Error),
    /// Acquiring a connection from the pool failed.
    Pool(crate::error::Error),
    /// Introspecting the source primary key (1-1 build) failed.
    Ddl(ddl::DdlError),
    /// The definition's shape isn't supported by the direct build — e.g. a
    /// cyclic alias chain, or one whose expansion exceeds the size budget.
    /// The backfill discharge builds such a definition by ring enumeration
    /// instead (ADR-0002's dispatch by shape).
    Unsupported(String),
    /// Staging a changed row's downstream propagation through the
    /// target-mutation seam failed (issue #315), wrapped where a backfill
    /// error is expected.
    Propagation(Box<crate::staging::ApplyError>),
    /// The chunk-queue claim this build ran under no longer holds (issue
    /// #434): its definition was resumed since the claim was taken, or the
    /// claim was reclaimed from this worker. The write stopped before
    /// touching the target; see `chunk_queue::ClaimFence`.
    Superseded,
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

/// Builds `def`'s target directly from its source, in bounded key-range
/// chunks, dispatching on `def`'s key space (see the module docs). `def`'s
/// target table must already exist (created by
/// [`super::ddl::create_target_table`] /
/// [`super::ddl::create_aggregate_target_table`]) — this only writes rows, it
/// does not create the table. `target_schema` and `source_columns` are the
/// same values those DDL calls were given. `source_table` is `def.source`'s
/// fully-qualified `"schema.table"` identity (issue #76, ADR-0011) — the
/// caller's own already-resolved value (`catalog::resolve_source_for_install`,
/// or [`super::model::Definition::source_table`] for a durable chunk-queue
/// caller) — threaded through every read of the live source below instead of
/// a bare `def.source` left to the executing connection's own `search_path`.
#[cfg(any(test, feature = "test-util", feature = "internals"))]
pub async fn backfill_definition(
    pool: &Pool,
    def: &TransformDef,
    target_schema: &str,
    source_table: &str,
    source_columns: &HashMap<String, ValueType>,
) -> Result<(), BackfillError> {
    backfill_definition_fenced(pool, def, target_schema, source_table, source_columns, None).await
}

/// [`backfill_definition`], with every target write fenced by `fence` when
/// the build runs as a chunk-queue job (issue #434, see [`ClaimFence`]):
/// each write runs in its own transaction that holds the claim first, and
/// the build stops with [`BackfillError::Superseded`] at the first write
/// whose claim no longer holds.
pub(crate) async fn backfill_definition_fenced(
    pool: &Pool,
    def: &TransformDef,
    target_schema: &str,
    source_table: &str,
    _source_columns: &HashMap<String, ValueType>,
    fence: Option<ClaimFence<'_>>,
) -> Result<(), BackfillError> {
    match &def.key_space {
        KeySpace::Join(_) => {
            unreachable!("a JOIN is built by its plan job (staging::build), never by this dispatch")
        }
        KeySpace::OneToOne => {
            // Issue #121: the 1-1 direct build's PK-range chunking
            // (`discover_pk_ranges`/`write_one_to_one_range`) now orders and
            // compares the source's full (possibly composite) primary key via
            // row-value comparison (`(c0, c1, ...) > (v0, v1, ...)`), which
            // Postgres evaluates lexicographically — matching a plain
            // `ORDER BY c0, c1, ...` — and which degenerates to a bare scalar
            // comparison at arity 1 (Postgres parses a single parenthesized
            // expression as that expression, not a one-element row
            // constructor), so this is no longer narrowed to a single column.
            let pk = source_primary_key(pool, source_table).await?;
            backfill_one_to_one(pool, def, target_schema, source_table, &pk, fence).await
        }
        // A group-basis aggregate is built by its plan job
        // (`staging::group_basis::rebuild`), never by this dispatch.
        KeySpace::Aggregate { .. } => Err(BackfillError::Unsupported(
            "an aggregate is built by its plan job, never by the direct build".to_string(),
        )),
    }
}

/// Runs `write` in a transaction that first holds `fence`, and commits it
/// (issue #434, see [`ClaimFence`]). Returns [`BackfillError::Superseded`],
/// having written nothing, when the claim no longer holds. With no fence (a
/// build outside the chunk queue) `write` runs on `client` directly.
async fn write_fenced<F>(
    client: &mut crate::pool::Client,
    fence: Option<ClaimFence<'_>>,
    write: F,
) -> Result<(), BackfillError>
where
    F: AsyncFnOnce(&tokio_postgres::Client) -> Result<(), BackfillError>,
{
    let Some(fence) = fence else {
        return write(client).await;
    };
    let txn = client.transaction().await?;
    if !fence.hold(&*txn).await? {
        return Err(BackfillError::Superseded);
    }
    write(txn.client()).await?;
    txn.commit().await?;
    Ok(())
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
/// A qualified [`Expr::SourceColumn`] (issue #830) names a source column by
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
            "a calculated-field alias expansion larger than the direct build's size budget"
                .to_string(),
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
        Expr::FunctionCall { args, .. } => 1 + args.iter().map(node_count).sum::<usize>(),
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
/// Range discovery reads the lexicographically-largest key of the next
/// `BACKFILL_CHUNK_ROWS` source rows above `lo` (see [`discover_pk_ranges`]);
/// that becomes `hi`, the chunk covers `pk > lo and pk <= hi` (row-value
/// comparison, issue #121 — a composite key's own declared column order,
/// matching Postgres's own `ORDER BY c0, c1, ...` and degenerating to a bare
/// scalar comparison at arity 1), and the next `lo` is this `hi`. When
/// discovery finds no rows left above `lo` the walk stops. Every source row's
/// PK is `> lo` for exactly one range and `<= hi` for that same range, so the
/// ranges partition the source exactly once with no gap or overlap — the
/// off-by-one this structure guards against is exactly what
/// `defs_backfill_direct`'s boundary test exercises.
async fn backfill_one_to_one(
    pool: &Pool,
    def: &TransformDef,
    target_schema: &str,
    source_table: &str,
    pk: &[PrimaryKeyColumn],
    fence: Option<ClaimFence<'_>>,
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
    let mut client = pool.get().await?;
    for (lo, hi) in discover_pk_ranges(&**client, &source, pk).await? {
        write_fenced(&mut client, fence, async |client| {
            write_one_to_one_range(
                client,
                def,
                target_schema,
                source_table,
                pk,
                &substituted,
                Some((&lo, &hi)),
            )
            .await
        })
        .await?;
    }
    // The keys with a `NULL` part, which no range reaches: only a source
    // keyed by a nullable unique index has them, and the target holds their
    // rows (issue #1141).
    if pk.iter().any(|c| c.nullable) {
        write_fenced(&mut client, fence, async |client| {
            write_one_to_one_range(
                client,
                def,
                target_schema,
                source_table,
                pk,
                &substituted,
                None,
            )
            .await
        })
        .await?;
    }

    Ok(())
}

/// The encoded keys of `source_table`'s rows in `(lo, hi]` of its primary
/// key `pk`, leaving out a quarantined key and a key with a `NULL` part
/// (which the build's plan stages instead, [`key_not_null`]), as a first
/// build leaves them out: a field build's chunk (#625 F8b,
/// `staging::build`).
pub(crate) async fn range_keys(
    client: &impl GenericClient,
    source_table: &str,
    target: &str,
    pk: &[PrimaryKeyColumn],
    lo: &Option<Vec<String>>,
    hi: &[String],
) -> Result<Vec<String>, BackfillError> {
    let source = ddl::qualified_source_table(source_table);
    let pk_idents: Vec<String> = pk.iter().map(|c| quote_ident(&c.name)).collect();
    let mut params = range_params(lo, hi);
    let where_clause = format!(
        "{} and {} and {}",
        pk_range_where(&pk_idents, pk, lo),
        key_not_null(&pk_idents),
        not_quarantined(pk, params.len() + 1),
    );
    params.push(&source_table);
    params.push(&target);
    Ok(client
        .query(
            &format!(
                "select {} from {source} as {SOURCE_ALIAS} where {where_clause}",
                ddl::pk_key_sql_expr(pk, Some(SOURCE_ALIAS)),
            ),
            &params,
        )
        .await?
        .into_iter()
        .map(|row| row.get(0))
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

/// The same `column_status` lookup `staging::quarantine::paused_columns_for`
/// runs from inside a batch's live-CDC apply, reused here (as a plain query
/// against an already-open `&Client`, not a call to that function) for the
/// exact same reason: a durable backfill chunk can be reclaimed and
/// re-executed after a crash (`chunk_queue::reclaim_stale_chunks`), and if
/// live CDC has paused one of this definition's columns in the meantime, a
/// (re-)executed chunk must not silently overwrite that column's frozen
/// value with a freshly (mis)computed one — that would undo the freeze that
/// is the entire point of column-level quarantine (ADR-0003).
/// Not a call to `staging::quarantine::paused_columns_for` itself: that
/// function takes a `&Pool`, every call site here already holds a `&Client`,
/// and `defs` sits below `staging` in this crate's layering
/// (`staging::apply` already depends on `defs::backfill`, so the reverse
/// dependency would be circular). Empty (the overwhelmingly common case) for
/// a definition with nothing currently paused.
///
/// Closed over `def`'s alias readers, as the `staging::quarantine` original
/// is (issue #748).
async fn paused_columns_for(
    client: &impl GenericClient,
    def: &TransformDef,
) -> Result<HashSet<String>, BackfillError> {
    let rows = client
        .query(
            "select column_name from column_status where transform_table = $1",
            &[&def.target],
        )
        .await?;
    let mut paused: HashSet<String> = rows.into_iter().map(|row| row.get(0)).collect();
    if !paused.is_empty() {
        super::eval::AliasReaders::of(def).close(&mut paused);
    }
    Ok(paused)
}

/// One `(lo, hi]` PK-range chunk's write — the body [`backfill_one_to_one`]'s
/// loop runs for every range in one call, and the durable chunk queue's
/// [`execute_one_to_one_chunk`] runs for exactly one range claimed off
/// `backfill_chunks` (ADR-0002's "A build is Re-derive over chunks, and
/// applies from its first chunk"). `substituted` is the caller's already-computed
/// [`substitute_all_fields`] output, so a queue-driven caller charged the
/// `Unsupported`-detecting cost once at plan time doesn't pay it again per
/// chunk beyond re-deriving the (cheap, pure) substitution itself. `range`
/// is the chunk's `(lo, hi]`, or `None` for every key with a `NULL` part,
/// which no range reaches ([`key_not_null`]).
#[allow(clippy::too_many_arguments)]
async fn write_one_to_one_range(
    client: &impl GenericClient,
    def: &TransformDef,
    target_schema: &str,
    source_table: &str,
    pk: &[PrimaryKeyColumn],
    substituted: &[Expr],
    range: Option<(&Option<Vec<String>>, &[String])>,
) -> Result<(), BackfillError> {
    let source = ddl::qualified_source_table(source_table);
    let target = qualified_target_table(target_schema, def);
    let pk_idents: Vec<String> = pk.iter().map(|c| quote_ident(&c.name)).collect();
    let pk_col_list = pk_idents.join(", ");

    // ADR-0003 (column-level quarantine): exclude any column
    // this definition currently has paused from both the computed column
    // list and the `ON CONFLICT` update set — see `paused_columns_for`'s doc
    // comment for why a durable, re-executable chunk write can't skip this.
    let paused = paused_columns_for(client, def).await?;
    let should_write = |name: &str| !paused.contains(name);

    let field_idents: Vec<String> = def
        .fields
        .iter()
        .filter(|f| should_write(&f.name))
        .map(|f| quote_ident(&f.name))
        .collect();
    let field_exprs: Vec<String> = def
        .fields
        .iter()
        .zip(substituted)
        .filter(|(f, _)| should_write(&f.name))
        .map(|(_, expr)| render_expr_sql(expr))
        .collect();

    let insert_cols = pk_idents
        .iter()
        .cloned()
        .chain(field_idents.iter().cloned())
        .collect::<Vec<_>>()
        .join(", ");
    let select_exprs = pk_idents
        .iter()
        .cloned()
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
        format!("on conflict ({pk_col_list}) do nothing")
    } else {
        let update_sets = field_idents
            .iter()
            .map(|f| format!("{f} = excluded.{f}"))
            .collect::<Vec<_>>()
            .join(", ");
        format!("on conflict ({pk_col_list}) do update set {update_sets}")
    };

    // A quarantined key is left out (#616): it stays out of the target until
    // `release_key` re-derives it, exactly as the drain leaves it out of every
    // batch. The build's own narrowing quarantines the key a chunk fails on
    // (`chunk_queue::fail_chunk`), and the chunk then runs without it. A
    // range leaves out a key with a `NULL` part ([`key_not_null`]), which
    // the `None` range writes.
    let (mut params, keys) = match range {
        Some((lo, hi)) => (
            range_params(lo, hi),
            format!(
                "{} and {}",
                pk_range_where(&pk_idents, pk, lo),
                key_not_null(&pk_idents)
            ),
        ),
        None => (Vec::new(), format!("not ({})", key_not_null(&pk_idents))),
    };
    let where_clause = format!("{keys} and {}", not_quarantined(pk, params.len() + 1));
    params.push(&source_table);
    params.push(&def.target);
    let insert_sql = format!(
        "insert into {target} ({insert_cols}) \
         select {select_exprs} from {source} as {SOURCE_ALIAS} where {where_clause} \
         {on_conflict}"
    );
    client.execute(&insert_sql, &params).await?;
    Ok(())
}

/// The read-only "planning" half of [`backfill_one_to_one`] (ADR-0002):
/// fails fast with [`BackfillError::Unsupported`] exactly as
/// `backfill_one_to_one` would (same [`substitute_all_fields`] call), then
/// returns the same `(lo, hi]` PK-range boundaries its loop would have
/// walked — without writing a single row of the target. The backfill
/// discharge (`intake::markers::run_pending_backfills`, ADR-0002) calls
/// this for a 1-1 definition once its capture point
/// has passed, and persists the boundaries as durable `backfill_chunks` work
/// items that drain threads execute.
///
/// Reads only the source's primary-key index, on `client` (the discharge's
/// own connection, outside its transaction). A key committed above the last
/// boundary after this reads is left to the go-live catch-up the last chunk
/// parks (`catalog::complete_direct_backfill`).
pub(crate) async fn plan_one_to_one_chunks(
    client: &impl GenericClient,
    def: &TransformDef,
    source_table: &str,
) -> Result<PlannedRanges, BackfillError> {
    let _ = substitute_all_fields(def)?;
    let pk = ddl::source_primary_key_in_txn(client, source_table).await?;
    let source = ddl::qualified_source_table(source_table);
    let ranges = discover_pk_ranges(client, &source, &pk).await?;
    // `backfill_chunks.lo`/`.hi` (V20__backfill_chunks.sql) are each a single
    // `text` column — issue #121 reuses this crate's existing composite-key
    // identity text ([`ddl::join_pk_key`], the same encoding
    // a capture trigger and `ddl::pk_key_sql_expr` already use
    // everywhere else) to fold a multi-column bound into that one column
    // rather than widening the schema, and degenerates to the bound's own
    // single value, verbatim, at arity 1 (byte-identical to before this
    // issue). [`execute_one_to_one_chunk`] decodes it back the same way.
    Ok(PlannedRanges {
        ranges: ranges
            .into_iter()
            .map(|(lo, hi)| (lo.map(ddl::join_pk_key), ddl::join_pk_key(hi)))
            .collect(),
        key_collations: ddl::key_collations(&pk),
    })
}

/// A 1-1 range build's plan ([`plan_one_to_one_chunks`]): its encoded
/// `(lo, hi]` ranges, and the collations of the source key's columns they
/// were walked under, which every chunk reads its range under
/// (`backfill_chunks.key_collations`, issue #769).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PlannedRanges {
    pub(crate) ranges: Vec<(Option<String>, String)>,
    pub(crate) key_collations: Vec<Option<String>>,
}

/// Executes exactly one previously-[`plan_one_to_one_chunks`]-enumerated
/// chunk — the durable-queue counterpart of [`backfill_one_to_one`]'s loop
/// body, claimed and run by a drain worker
/// (`trellis::client`'s `app_worker_loop`) rather than an in-call loop.
/// Idempotent overwrite, like every chunk write in this module (ADR-0002): a
/// worker that reclaims this chunk after a peer died mid-write redoes it
/// safely.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn execute_one_to_one_chunk(
    pool: &Pool,
    def: &TransformDef,
    target_schema: &str,
    source_table: &str,
    lo: Option<&str>,
    hi: &str,
    key_collations: Option<&[Option<String>]>,
    fence: ClaimFence<'_>,
) -> Result<(), BackfillError> {
    let mut pk = source_primary_key(pool, source_table).await?;
    // Under the collations the range was planned under, not the key's own
    // now (issue #769).
    ddl::pin_key_collations(&mut pk, key_collations);
    let substituted = substitute_all_fields(def)?;
    let mut client = pool.get().await?;
    // The exact inverse of [`plan_one_to_one_chunks`]'s encode: a genuine
    // primary-key column (every real `PRIMARY KEY`, at any arity) is never
    // `NULL`, so every part decodes to `Some` here in practice — propagated
    // as a typed [`ddl::DdlError::MalformedCompositeKey`] via `From`, rather
    // than assumed, should stale/corrupt `backfill_chunks` data ever say
    // otherwise.
    let decode = |text: &str| -> Result<Vec<String>, BackfillError> {
        Ok(ddl::split_pk_key(&pk, source_table, text)?
            .into_iter()
            .map(|part| part.map(|c| c.into_owned()).unwrap_or_default())
            .collect())
    };
    let lo = lo.map(decode).transpose()?;
    let hi = decode(hi)?;
    write_fenced(&mut client, Some(fence), async |client| {
        write_one_to_one_range(
            client,
            def,
            target_schema,
            source_table,
            &pk,
            &substituted,
            Some((&lo, &hi)),
        )
        .await
    })
    .await
}

/// The alias a 1-1 range write and [`narrow_one_to_one_chunk`] give the
/// source, so [`not_quarantined`]'s key expression names the source's columns
/// even when one of them shares a name with a column of `poison`.
const SOURCE_ALIAS: &str = "__trellis_src";

/// SQL predicate over a source row aliased [`SOURCE_ALIAS`]: its key isn't
/// quarantined (`poison`) for the source table bound as `$param` (the
/// canonical, qualified identity quarantine keys on) and the definition whose
/// bare target is bound as `$param + 1`. Whole-key poison is per transform
/// (#799), so a key held for a sibling definition on the same source is
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
/// NULLS NOT DISTINCT` index (an aggregate target's grouping columns, issue
/// #128) can, and a `(lo, hi]` row comparison still admits such a row
/// whenever an earlier part decides it (`(2, NULL) <= (3, 'a')`), while it
/// leaves out one whose `NULL` part decides it. A range therefore leaves out
/// every such key, so that the ranges partition exactly the keys without
/// one, as [`discover_pk_ranges`]' boundaries do. The target holds their
/// rows all the same (issue #1141): [`backfill_one_to_one`] writes them in a
/// pass of their own, the Re-derive build's plan stages them into the ring
/// (`staging::build`), and the old chunked build's go-live catch-up
/// enumerates them.
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

/// Narrows a failed chunk's range `(lo, hi]` of `source_table` (#616): counts
/// the keys the chunk would write, leaving out the quarantined ones and any
/// with a `NULL` part as the write does (a 1-1 range's, and a Re-derive
/// chunk's since #625 F6, [`key_not_null`]), and either names the one key or
/// the key that splits them in half. The split point is found the way
/// [`discover_pk_ranges`] finds a chunk's upper bound: the largest key of the
/// first half, in the primary key's own order. `key_collations` are the ones
/// the chunk's build planned its ranges under (`backfill_chunks.key_collations`,
/// issue #769), which the range is read and split under.
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
    // The keys [`write_one_to_one_range`] writes, by the same predicate.
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

/// Walks the source primary key in half-open `(lo, hi]` ranges, returning them
/// in order (the first range's `lo` is `None`, meaning "`pk <= hi`"). Each
/// `hi` is the lexicographically-largest key (in the primary key's own
/// declared column order — issue #163's convention, same as
/// [`ddl::pk_key_sql_expr`]) of the next `BACKFILL_CHUNK_ROWS` rows above the
/// previous `hi`: the inner query orders ascending and takes the page, the
/// outer query re-sorts descending (`nulls last`, so a `NULL` component —
/// only reachable when `source` is itself a nullable-`GROUP BY` aggregate
/// target, never a real `PRIMARY KEY` — sorts as if absent, mirroring a
/// single-column `max()`'s own null-skipping) and takes the first row —
/// exactly [`max`](https://www.postgresql.org/docs/current/functions-aggregate.html)'s
/// answer at arity 1 (Postgres has no `max()` over an anonymous row type, so
/// this is the composite generalization, issue #121), and byte-identical SQL
/// at arity 1 to a bare column reference (a one-element parenthesized
/// expression is not a row constructor). The walk stops when no rows remain
/// above the last boundary. The ranges partition the source exactly once with
/// no gap or overlap regardless of gaps in the key values. See
/// [`backfill_one_to_one`]'s doc comment for the off-by-one this guards
/// against. `source` is the already-quoted source table identifier.
async fn discover_pk_ranges(
    client: &impl GenericClient,
    source: &str,
    pk: &[PrimaryKeyColumn],
) -> Result<Vec<(Option<Vec<String>>, Vec<String>)>, BackfillError> {
    walk_pk_ranges(client, source, pk, None, BACKFILL_CHUNK_ROWS, usize::MAX).await
}

/// Up to `max` consecutive `(lo, hi]` ranges of `source` (the encoded
/// source table name) of about `rows` rows each, starting above `after` (an
/// encoded key as `backfill_chunks` stores it; `None` from the first row):
/// [`discover_pk_ranges`]' walk, a batch at a time, for the Re-derive build's
/// plan job (#625 F2, Q13). Fewer than `max` ranges means the walk reached
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

/// [`discover_pk_ranges`]' walk from `start`, `rows` rows per range, at most
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
    // [`PrimaryKeyColumn::collation`] (issue #769), as the chunks that read
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
    // DISTINCT`, issue #128). `hi_select` reads each column as a bare
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
/// full `(lo, hi]` predicate, and [`discover_pk_ranges`]'s lower-bound-only
/// probe uses directly. Degenerates to a bare scalar comparison at arity 1
/// (Postgres parses a single parenthesized expression as that expression, not
/// a one-element row constructor), so this is byte-identical SQL to the
/// pre-#121 single-column form there.
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
/// #121: row-value comparison over `pk`'s full column list, not a single
/// scalar), binding the bounds as `$1..$n` (and `$n+1..$2n` when `lo` is
/// present, `n = pk.len()`). Paired with [`discover_pk_ranges`]; the caller
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
    use super::super::ast::{FieldDef, Operator, Predicate};
    use super::*;

    /// Issue #769: every column of a key range is compared under its
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

    /// Issue #695: inside an aggregate's argument, the name of another
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
        assert_eq!(sql[3], r#"(max("val") + 1::numeric)"#);
        assert_eq!(
            sql[4], r#"(sum("val") + count(*))"#,
            "a name outside an aggregate's argument still inlines the field"
        );
    }

    /// Issue #830: a qualified name is the source column even where a field
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
            r#"("grp" + ("grp" + 1::numeric))"#,
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
        assert_eq!(render_expr_sql(&out[1]), r#"sum(("a" + 1::numeric))"#);
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
