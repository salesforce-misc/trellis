//! A from-scratch recompute of a 1-1 target from current source data
//! (issue #25), used to assert any incrementally-maintained target is
//! byte-equal to a recompute (`docs/data-flow.md#correctness`).
//!
//! **This module's [`recompute`] is the evaluator-driven recompute**: it reads
//! each source row's text image and feeds it through the engine's own
//! `evaluate`. Its role is a *secondary cross-check*, not the authority. The
//! primary oracle is Postgres itself (by rendering the definition as a `SELECT`).
//! Comparing the evaluator-driven recompute against the Postgres-SQL oracle
//! ensures the engine mirrors Postgres semantics exactly.

#[cfg(any(test, feature = "test-util"))]
use std::collections::BTreeSet;
#[cfg(any(test, feature = "test-util"))]
use std::collections::{HashMap, HashSet};
#[cfg(any(test, feature = "test-util"))]
use std::fmt;

#[cfg(any(test, feature = "test-util"))]
use crate::pool::Pool;
use crate::pool::quote_ident;

use super::ast::{Expr, Operator};
#[cfg(any(test, feature = "test-util"))]
use super::ast::{GroupByKey, KeySpace, RelationshipDef, TransformDef, ValueType};
#[cfg(any(test, feature = "test-util"))]
use super::eval::{EvalError, RegexCache, Row, Value, evaluate, evaluate_aggregate};
#[cfg(any(test, feature = "test-util"))]
use super::registry::lookup_aggregate_function;

/// Why a from-scratch recompute failed.
#[derive(Debug)]
#[cfg(any(test, feature = "test-util"))]
pub enum OracleError {
    /// A direct Postgres protocol/query error.
    Db(tokio_postgres::Error),
    /// Acquiring a connection from the pool failed.
    Pool(crate::error::Error),
    /// Evaluating a source row's calculated fields failed (see
    /// [`EvalError`]); a row that fails here would be quarantined by the
    /// real apply path, not silently dropped from the recompute.
    Eval(EvalError),
}

#[cfg(any(test, feature = "test-util"))]
impl fmt::Display for OracleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OracleError::Db(err) => {
                write!(f, "oracle recompute database error: ")?;
                crate::error::write_pg_error(f, err)
            }
            OracleError::Pool(err) => write!(f, "failed to acquire a connection: {err}"),
            OracleError::Eval(err) => write!(f, "oracle recompute evaluation error: {err}"),
        }
    }
}

#[cfg(any(test, feature = "test-util"))]
impl std::error::Error for OracleError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            OracleError::Db(err) => Some(err),
            OracleError::Pool(err) => Some(err),
            OracleError::Eval(err) => Some(err),
        }
    }
}

#[cfg(any(test, feature = "test-util"))]
impl From<tokio_postgres::Error> for OracleError {
    fn from(err: tokio_postgres::Error) -> Self {
        OracleError::Db(err)
    }
}

#[cfg(any(test, feature = "test-util"))]
impl From<crate::error::Error> for OracleError {
    fn from(err: crate::error::Error) -> Self {
        OracleError::Pool(err)
    }
}

#[cfg(any(test, feature = "test-util"))]
impl From<EvalError> for OracleError {
    fn from(err: EvalError) -> Self {
        OracleError::Eval(err)
    }
}

/// A from-scratch recompute of `def`'s target: every source row's primary
/// key (as text) mapped to its calculated columns.
#[cfg(any(test, feature = "test-util"))]
pub type Recomputed = HashMap<String, HashMap<String, Option<Value>>>;

/// The `FROM`-clause identifier this test/benchmark-only oracle module
/// should read `def`'s source through (issue #76, ADR-0007 grammar clause
/// 4): `"schema"."table"`, each component quoted independently, when `def`
/// wrote an explicit `FROM <schema>.<source>`
/// ([`TransformDef::explicit_source_schema`]); the plain `"table"` quoted
/// identifier every function here has always rendered, left to the executing
/// connection's own `search_path`, for the far more common bare case.
///
/// Every function in this module is documented test/benchmark-only (see e.g.
/// [`render_aggregate_select_sql`]'s own doc comment) and every caller runs
/// its rendered SQL against the same connection/`search_path` the definition
/// itself was accepted under, so the bare branch can't drift the way the
/// real backfill/apply/quarantine paths' pinned-`search_path` reads could
/// (`docs/decisions/0007`) — only the explicit-schema case needed a real fix
/// here, and it needs no database round trip: the schema is already right on
/// `def`.
#[cfg(any(test, feature = "test-util"))]
fn quoted_source_from(def: &TransformDef) -> String {
    match &def.explicit_source_schema {
        Some(schema) => format!("{}.{}", quote_ident(schema), quote_ident(&def.source)),
        None => quote_ident(&def.source),
    }
}

/// Recomputes `def`'s entire target from `def.source`'s current contents,
/// keyed by `pk_column` (the source table's primary key, e.g. from
/// [`super::ddl::source_primary_key`]). `source_columns` gives each
/// referenced source column's [`ValueType`], the same map `def` was
/// validated against.
///
/// Reads `pk_column` plus every source column any calculated field
/// references, as text, then runs each row through [`evaluate`] — the exact
/// function the incremental path evaluates deltas with. No SQL expression
/// here computes a calculated field's value; only column selection is SQL.
#[cfg(any(test, feature = "test-util"))]
pub async fn recompute(
    pool: &Pool,
    def: &TransformDef,
    pk_column: &str,
    source_columns: &HashMap<String, ValueType>,
) -> Result<Recomputed, OracleError> {
    let referenced = referenced_source_columns(def);

    let mut select_list = vec![format!("{}::text", quote_ident(pk_column))];
    select_list.extend(
        referenced
            .iter()
            .map(|c| format!("{}::text", quote_ident(c))),
    );

    let sql = format!(
        "select {} from {}",
        select_list.join(", "),
        quoted_source_from(def)
    );

    let client = pool.get().await?;
    let rows = client.query(sql.as_str(), &[]).await?;

    let mut result = Recomputed::with_capacity(rows.len());
    // Reused across every row below (issue #68): `def` is fixed for the
    // whole recompute, so any `regexp_count` pattern it references compiles
    // to the same `Regex` on every row.
    let mut regex_cache = RegexCache::new();
    for db_row in rows {
        let pk_value: String = db_row.get(0);
        let mut image: Row = HashMap::with_capacity(referenced.len());
        for (i, column) in referenced.iter().enumerate() {
            let value: Option<String> = db_row.get(i + 1);
            image.insert(column.clone(), value);
        }
        let evaluated = evaluate(def, &image, source_columns, &mut regex_cache)?;
        result.insert(pk_value, evaluated);
    }

    Ok(result)
}

/// Recomputes an [`KeySpace::Aggregate`] `def`'s entire target from
/// `def.source`'s current contents, keyed by the grouping columns' text
/// values joined with `,` (there's no single primary-key column to key
/// [`Recomputed`] by, unlike [`recompute`]'s 1-1 case — the group is the
/// key). Groups rows in Rust (reading every grouping and referenced column
/// as text, then partitioning by the grouping columns' values) rather than
/// issuing a SQL `GROUP BY` itself, so this stays the same kind of
/// evaluator-driven secondary cross-check `recompute` is: the real oracle is
/// Postgres's own `GROUP BY` via [`render_aggregate_select_sql`], not this
/// function.
///
/// # Panics
///
/// If `def.key_space` is not [`KeySpace::Aggregate`].
#[cfg(any(test, feature = "test-util"))]
pub async fn recompute_aggregate(
    pool: &Pool,
    def: &TransformDef,
    source_columns: &HashMap<String, ValueType>,
) -> Result<Recomputed, OracleError> {
    let KeySpace::Aggregate { group_by } = &def.key_space else {
        panic!("recompute_aggregate called on a non-aggregate definition");
    };

    // Issue #137: a `GroupByKey::RelationshipPath` key has no source column
    // to select at all — this pure-Rust, evaluator-driven cross-check is
    // documented as not wiring relationships (see `eval::evaluate_aggregate`'s
    // own doc comment), so a definition with one is expected to skip this
    // function entirely and compare only against the real Postgres oracle
    // (`render_aggregate_relationship_select_sql`), exactly as issue #94's
    // relationship-*field* tests already do. Keying by `target_column_name`
    // keeps a plain-column `GROUP BY` (the overwhelmingly common case)
    // byte-identical to before this issue.
    let mut columns = referenced_source_columns(def);
    for key in group_by {
        columns.insert(key.target_column_name().to_string());
    }
    let columns: Vec<String> = columns.into_iter().collect();

    let select_list: Vec<String> = columns
        .iter()
        .map(|c| format!("{}::text", quote_ident(c)))
        .collect();
    let sql = format!(
        "select {} from {}",
        select_list.join(", "),
        quoted_source_from(def)
    );

    let client = pool.get().await?;
    let db_rows = client.query(sql.as_str(), &[]).await?;

    let mut groups: HashMap<String, Vec<Row>> = HashMap::new();
    for db_row in db_rows {
        let mut image: Row = HashMap::with_capacity(columns.len());
        for (i, column) in columns.iter().enumerate() {
            let value: Option<String> = db_row.get(i);
            image.insert(column.clone(), value);
        }
        let key = group_key(&image, group_by);
        groups.entry(key).or_default().push(image);
    }

    let mut result = Recomputed::with_capacity(groups.len());
    let mut regex_cache = RegexCache::new();
    for rows in groups.values() {
        let evaluated = evaluate_aggregate(def, rows, source_columns, &mut regex_cache)?;
        let key = group_key(&rows[0], group_by);
        result.insert(key, evaluated);
    }

    Ok(result)
}

/// The composite grouping-key text used by [`recompute_aggregate`] to key
/// [`Recomputed`] — every row in a group shares these values, so any row's
/// image gives the same key. A grouping column is assumed non-`NULL` (the
/// typical case for a real foreign/primary key); this doesn't attempt to
/// match Postgres's "`NULL` groups with `NULL`" `GROUP BY` semantics.
#[cfg(any(test, feature = "test-util"))]
fn group_key(image: &Row, group_by: &[GroupByKey]) -> String {
    // Length-prefix each component (`"{len}:{value}"`) rather than joining
    // on a bare separator: a Text grouping column's value can itself
    // contain any separator character (including a comma), which would
    // make two distinct groupings collide, e.g. `(a="x,y", b="z")` vs.
    // `(a="x", b="y,z")`. Prefixing each value with its own byte length
    // makes the encoding unambiguous regardless of what characters the
    // value contains — the length prefix itself is always parsed as a
    // number, not searched for as a delimiter.
    group_by
        .iter()
        .map(|k| {
            image
                .get(k.target_column_name())
                .cloned()
                .flatten()
                .unwrap_or_default()
        })
        .map(|v| format!("{}:{v}", v.len()))
        .collect::<String>()
}

/// Renders an [`KeySpace::Aggregate`] `def` back to the equivalent Postgres
/// `SELECT ... GROUP BY ...` (issue #11 groundwork's correctness oracle):
/// each calculated field renders via [`render_expr_sql`] exactly as the 1-1
/// case does (a `SUM`/`MIN`/`MAX`/`AVG` call renders like any other function
/// call — `render_expr_sql` already lowercases the name and wraps its args),
/// so no separate aggregate-rendering logic is needed; only the `GROUP BY`
/// clause itself is new.
///
/// Every field is first substituted (see
/// [`super::backfill::substituted_field_exprs`]) so a `GROUP BY` field that
/// references another calculated field by name (e.g. `double_total = total +
/// total`) renders as a self-contained expression instead of a bare
/// `Column("total")` — a same-SELECT-list alias Postgres does not resolve.
///
/// # Panics
///
/// If `def.key_space` is not [`KeySpace::Aggregate`], or if substitution
/// fails (a cyclic alias chain, or a pathologically large expansion — see
/// [`super::backfill::substituted_field_exprs`]'s doc comment). This helper
/// is a test-only reference-SQL oracle (only ever called from `trellis`'s and
/// `benchmark`'s test/comparison code, never a production path), so it stays
/// infallible and simply panics on either misuse rather than growing a
/// `Result` that would ripple into every call site for no real benefit here.
#[cfg(any(test, feature = "test-util"))]
pub fn render_aggregate_select_sql(def: &TransformDef) -> String {
    let KeySpace::Aggregate { group_by } = &def.key_space else {
        panic!("render_aggregate_select_sql called on a non-aggregate definition");
    };
    let substituted = super::backfill::substituted_field_exprs(def)
        .expect("aggregate oracle rendering requires a substitutable definition");

    let select_list: Vec<String> = def
        .fields
        .iter()
        .map(|field| {
            format!(
                "{} as {}",
                render_expr_sql(&substituted[&field.name]),
                quote_ident(&field.name)
            )
        })
        .collect();
    // A `GroupByKey::RelationshipPath` key has no plain-column rendering
    // this relationship-unaware oracle can produce — `render_expr_sql`'s own
    // `RelationshipPath` arm panics with a clear message, matching how this
    // whole function is documented as relationship-free; a definition with
    // one must use `render_aggregate_relationship_select_sql` instead
    // (issue #94's own split, unchanged by issue #137).
    let group_cols: Vec<String> = group_by
        .iter()
        .map(|k| render_expr_sql(&k.as_expr()))
        .collect();

    format!(
        "select {} from {} group by {}",
        select_list.join(", "),
        quoted_source_from(def),
        group_cols.join(", ")
    )
}

/// The set of `def.source` column names any calculated field references —
/// i.e. every [`Expr::Column`] name that isn't itself *another* calculated
/// field's name. Used to select only the columns a recompute actually needs.
///
/// A field referencing a source column of its own name (`SELECT c AS c`,
/// the generative suite's improvement-plan task B1 identity-passthrough
/// shape) is a passthrough, not a reference to another calculated field —
/// this must mirror the same `is_self_passthrough` carve-out
/// `eval.rs`'s `eval_expr` and `validate.rs`'s cycle detection already make,
/// or a self-referencing passthrough field's own source column never makes
/// it into the recompute's `SELECT` list and `eval_expr` fails with
/// `EvalError::MissingColumn` even though the same definition installs and
/// backfills correctly (`trellis/tests/defs_backfill_direct.rs`'s `SELECT a
/// AS a` goes through a different path — direct-build/backfill — that
/// already has this carve-out via `ddl::passthrough_source_column`; this
/// oracle-recompute path did not, until task B1's generative coverage of a
/// real end-to-end passthrough-field run found the gap).
///
/// Unlike `eval.rs`'s and `validate.rs`'s versions of this same carve-out,
/// this one doesn't also check that `name` actually names a real source
/// column (`is_self_passthrough = name == field_name &&
/// source_columns.contains_key(name)`) — it can't, cheaply, since this
/// function takes no `source_columns` map. That's safe only because both
/// callers ([`recompute`], [`recompute_aggregate`]) are documented as
/// running exclusively against an already-validated, already-installed
/// `TransformDef` (see `recompute`'s own doc comment), and `validate()`
/// already rejects a field naming itself when that name isn't a real source
/// column (`UnresolvedColumn`, not treated as a passthrough) before a
/// definition can ever be persisted. This is an implicit invariant enforced
/// by caller discipline, not the type system — if a future caller ever
/// invokes `recompute`/`recompute_aggregate` against a `TransformDef` that
/// hasn't been through `validate()`, thread `source_columns` through here
/// too and add the same guard.
#[cfg(any(test, feature = "test-util"))]
fn referenced_source_columns(def: &TransformDef) -> HashSet<String> {
    let field_names: HashSet<&str> = def.fields.iter().map(|f| f.name.as_str()).collect();

    let mut columns = HashSet::new();
    for field in &def.fields {
        collect_columns(&field.expr, field.name.as_str(), &field_names, &mut columns);
    }
    columns
}

#[cfg(any(test, feature = "test-util"))]
fn collect_columns(
    expr: &Expr,
    field_name: &str,
    field_names: &HashSet<&str>,
    out: &mut HashSet<String>,
) {
    match expr {
        Expr::Column(name) => {
            let is_self_passthrough = name == field_name;
            if is_self_passthrough || !field_names.contains(name.as_str()) {
                out.insert(name.clone());
            }
        }
        Expr::NumberLiteral(_) | Expr::StringLiteral(_) => {}
        // Not a source-column reference by name — a path's columns live on
        // the *to-side* table, which [`recompute`]/[`recompute_aggregate`]'s
        // source SELECT doesn't read. Those two evaluator-driven recomputes
        // therefore only handle relationship-free definitions; the SQL oracles
        // ([`render_relationship_select_sql`],
        // [`render_aggregate_relationship_select_sql`]) are what cross-check a
        // relationship-reading one.
        Expr::RelationshipPath { .. } => {}
        Expr::BinaryOp { lhs, rhs, .. } => {
            collect_columns(lhs, field_name, field_names, out);
            collect_columns(rhs, field_name, field_names, out);
        }
        Expr::FunctionCall { args, .. } => {
            for arg in args {
                collect_columns(arg, field_name, field_names, out);
            }
        }
    }
}

/// Renders a calculated-field expression back to the equivalent Postgres
/// SQL expression text (issue #64), so the generative correctness oracle
/// (`docs/generative-test-suite.md`) can cross-check a function call's
/// rendered `SELECT` against this evaluator's output over the same source
/// data — the ADR-0004 claim ("our grammar is an immutable subset of
/// Postgres semantics") made checkable per expression, not just per
/// operator. Column references are quoted identifiers, since a source
/// column name could collide with a SQL keyword; literals carry an explicit
/// cast so the rendered text is unambiguous regardless of context.
pub fn render_expr_sql(expr: &Expr) -> String {
    match expr {
        Expr::Column(name) => quote_ident(name),
        Expr::NumberLiteral(text) => format!("{text}::numeric"),
        Expr::StringLiteral(text) => format!("'{}'::text", text.replace('\'', "''")),
        Expr::RelationshipPath { rel, column } => panic!(
            "render_expr_sql called on an unresolved relationship path '{rel}.{column}' \
             — the validator (#23) should have rejected this before reaching the oracle \
             (issue #25 is grammar + AST only)"
        ),
        Expr::BinaryOp { op, lhs, rhs } => {
            let symbol = match op {
                Operator::Add => "+",
                Operator::GreaterThan => ">",
            };
            format!(
                "({} {symbol} {})",
                render_expr_sql(lhs),
                render_expr_sql(rhs)
            )
        }
        Expr::FunctionCall { name, args } if name == "COUNT" && args.is_empty() => {
            // `COUNT(*)` (issue #75): the parser's only accepted `COUNT`
            // shape, but its AST has no argument to render — `count()` is
            // not valid Postgres, so this renders the `*` back explicitly
            // rather than falling through to the generic `name(args)` case
            // below.
            "count(*)".to_string()
        }
        Expr::FunctionCall { name, args } => {
            let rendered_args: Vec<String> = args.iter().map(render_expr_sql).collect();
            format!("{}({})", name.to_lowercase(), rendered_args.join(", "))
        }
    }
}

/// Renders an expression that may read **to-one** relationship paths, against
/// a source aliased as `source_sql` (already-quoted SQL text — a quoted table
/// name, or a query alias such as `s`) `LEFT JOIN`ed to each referenced
/// relationship's to-side table under an alias equal to the relationship's
/// quoted name.
///
/// This is the shared renderer for every place a to-one path has to become SQL
/// over a real `LEFT JOIN` rather than a correlated subquery: the aggregate
/// direct build (`super::backfill::backfill_aggregate`), the aggregate
/// incremental recompute (`staging::apply_aggregate::apply_forced_groups_bulk`),
/// and this module's own [`render_aggregate_relationship_select_sql`] oracle.
/// Unlike [`render_rel_expr_sql`], an aggregate call whose sole argument is a
/// relationship path is *not* special-cased into a correlated subquery — under
/// a to-one join `sum(rel.col)` is an ordinary aggregate over the joined
/// column, which is exactly issue #94's semantics.
///
/// Source columns are qualified with `source_sql` so they can't collide with a
/// joined to-side column of the same name.
pub(crate) fn render_to_one_rel_expr_sql(expr: &Expr, source_sql: &str) -> String {
    match expr {
        Expr::Column(name) => format!("{source_sql}.{}", quote_ident(name)),
        Expr::NumberLiteral(text) => format!("{text}::numeric"),
        Expr::StringLiteral(text) => format!("'{}'::text", text.replace('\'', "''")),
        Expr::RelationshipPath { rel, column } => {
            format!("{}.{}", quote_ident(rel), quote_ident(column))
        }
        Expr::BinaryOp { op, lhs, rhs } => {
            let symbol = match op {
                Operator::Add => "+",
                Operator::GreaterThan => ">",
            };
            format!(
                "({} {symbol} {})",
                render_to_one_rel_expr_sql(lhs, source_sql),
                render_to_one_rel_expr_sql(rhs, source_sql)
            )
        }
        Expr::FunctionCall { name, args } if name == "COUNT" && args.is_empty() => {
            "count(*)".to_string()
        }
        Expr::FunctionCall { name, args } => {
            let rendered: Vec<String> = args
                .iter()
                .map(|arg| render_to_one_rel_expr_sql(arg, source_sql))
                .collect();
            format!("{}({})", name.to_lowercase(), rendered.join(", "))
        }
    }
}

/// The ` left join <to_table> as <rel> on <rel>.<to_col> = <source_sql>.<from_col>`
/// clauses a [`render_to_one_rel_expr_sql`]-rendered expression needs, one per
/// entry of `joins` (relationship name → `(to_table, to_col, from_col)`), in
/// the iterator's order. Left (not inner) join so a source row whose FK doesn't
/// resolve still reaches the `GROUP BY` and contributes `NULL` — ADR-0006's
/// to-one nullability rule, and the same shape
/// [`render_relationship_select_sql`] already uses for a 1-1 target.
pub(crate) fn to_one_join_clauses<'a>(
    joins: impl Iterator<Item = (&'a str, &'a str, &'a str, &'a str)>,
    source_sql: &str,
) -> String {
    joins
        .map(|(rel, to_table, to_col, from_col)| {
            format!(
                " left join {} as {alias} on {alias}.{} = {source_sql}.{}",
                quote_ident(to_table),
                quote_ident(to_col),
                quote_ident(from_col),
                alias = quote_ident(rel),
            )
        })
        .collect()
}

/// Renders an [`KeySpace::Aggregate`] `def` whose fields aggregate over to-one
/// relationship paths (issue #94) back to the equivalent Postgres `SELECT …
/// LEFT JOIN … GROUP BY …`, the relationship-aware counterpart of
/// [`render_aggregate_select_sql`] — a test/benchmark oracle only, like that
/// one.
///
/// `relationships` is keyed by relationship name, exactly as
/// [`render_relationship_select_sql`] takes it.
///
/// # Panics
///
/// If `def.key_space` is not [`KeySpace::Aggregate`], if substitution fails
/// (see [`render_aggregate_select_sql`]), or if a referenced relationship is
/// missing from `relationships`.
#[cfg(any(test, feature = "test-util"))]
pub fn render_aggregate_relationship_select_sql(
    def: &TransformDef,
    relationships: &HashMap<String, RelationshipDef>,
) -> String {
    let KeySpace::Aggregate { group_by } = &def.key_space else {
        panic!("render_aggregate_relationship_select_sql called on a non-aggregate definition");
    };
    let substituted = super::backfill::substituted_field_exprs(def)
        .expect("aggregate oracle rendering requires a substitutable definition");

    let source_sql = quoted_source_from(def);
    let select_list: Vec<String> = def
        .fields
        .iter()
        .map(|field| {
            format!(
                "{} as {}",
                render_to_one_rel_expr_sql(&substituted[&field.name], &source_sql),
                quote_ident(&field.name)
            )
        })
        .collect();
    // Issue #137: a `GROUP BY` key may itself be a to-one relationship path
    // (`post.author`), rendered exactly like a field's own relationship-path
    // reference — qualified against `source_sql` for a plain column, or
    // against its own join alias for a relationship path.
    let group_cols: Vec<String> = group_by
        .iter()
        .map(|k| render_to_one_rel_expr_sql(&k.as_expr(), &source_sql))
        .collect();

    // Deterministic JOIN order, deduped: a relationship read by several
    // fields (or a field and a `GROUP BY` key alike) is joined once. Issue
    // #137: a `GROUP BY`-only relationship reference (no field reads it at
    // all — e.g. `GROUP BY tag, post.author SELECT COUNT(*) AS post_count`)
    // still needs its join, or `group_cols`' own alias reference above
    // resolves to nothing.
    let mut rel_names: BTreeSet<&str> = BTreeSet::new();
    for expr in substituted.values() {
        collect_rel_names(expr, &mut rel_names);
    }
    for key in group_by {
        if let GroupByKey::RelationshipPath { rel, .. } = key {
            rel_names.insert(rel.as_str());
        }
    }
    let joins = to_one_join_clauses(
        rel_names.iter().map(|rel| {
            let r = relationships.get(*rel).unwrap_or_else(|| {
                panic!("render_aggregate_relationship_select_sql: unknown relationship '{rel}'")
            });
            (
                *rel,
                r.to_table.as_str(),
                r.to_col.as_str(),
                r.from_col.as_str(),
            )
        }),
        &source_sql,
    );

    format!(
        "select {} from {source_sql}{joins} group by {}",
        select_list.join(", "),
        group_cols.join(", ")
    )
}

/// Every relationship name a (substituted) expression reads, regardless of
/// where in the tree the path sits — unlike [`collect_to_one_rels`], which
/// stops at an aggregate-wrapped path because that shape means *to-many* in a
/// 1-1 definition. In an aggregate definition every path is to-one (the
/// validator rejects to-many there), so every one of them needs a `LEFT JOIN`.
#[cfg(any(test, feature = "test-util"))]
fn collect_rel_names<'a>(expr: &'a Expr, out: &mut BTreeSet<&'a str>) {
    match expr {
        Expr::RelationshipPath { rel, .. } => {
            out.insert(rel.as_str());
        }
        Expr::Column(_) | Expr::NumberLiteral(_) | Expr::StringLiteral(_) => {}
        Expr::BinaryOp { lhs, rhs, .. } => {
            collect_rel_names(lhs, out);
            collect_rel_names(rhs, out);
        }
        Expr::FunctionCall { args, .. } => {
            for arg in args {
                collect_rel_names(arg, out);
            }
        }
    }
}

/// Renders a relationship-enriched 1-1 [`TransformDef`] back to the equivalent
/// Postgres `SELECT` (issue #32), so the generative correctness oracle can
/// assert the engine's incrementally-maintained target is byte-equal to what
/// Postgres itself computes over the same source + related data (ADR-0004).
///
/// `relationships` is keyed by relationship name — the head of a
/// `<rel>.<column>` path — mirroring how the evaluator's
/// [`super::eval::RelationshipContext`] is caller-provided (issue #28/#29): the
/// oracle needs the same relationship endpoints (`from_col`, `to_table`,
/// `to_col`) the eval-side context was built from, so both sides describe the
/// same join. A referenced relationship absent from the map is a caller bug
/// (the validator resolves paths before eval/oracle) and panics, like
/// [`render_expr_sql`]'s unresolved-path guard.
///
/// The two relationship shapes render exactly as the evaluator resolves them,
/// so the SQL and the engine agree row-for-row:
///
/// * A **to-one** enrichment (a bare `<rel>.<column>` path, issue #28) becomes
///   a `LEFT JOIN` from the source to the to-side table on
///   `source.from_col = rel.to_col`, projecting `rel.column`. `LEFT JOIN` keeps
///   a from-row with no match (or a `NULL` join key) alive with a `NULL`
///   enrichment — matching #28's "no-match / NULL fk => NULL, from-row
///   survives".
/// * A **to-many** aggregate (`sum(<rel>.<column>)`, issue #29) becomes a
///   correlated aggregate subquery over the to-side table filtered by the join
///   key. Postgres's aggregate over the empty correlated set gives `count → 0`
///   and `sum`/`min`/`max`/`avg → NULL`, and `count(rel.column)` counts only
///   non-`NULL` values — exactly [`super::eval::eval_to_many_aggregate`] /
///   `reduce_numeric_aggregate`.
///
/// The relationship name doubles as the SQL alias for both the `LEFT JOIN`
/// target and the correlated subquery's table, so a to-side table that shares
/// the source table's name (or is referenced twice) stays unambiguous.
///
/// # Panics
///
/// If `def.key_space` is not [`KeySpace::OneToOne`], or a referenced
/// relationship is missing from `relationships`.
#[cfg(any(test, feature = "test-util"))]
pub fn render_relationship_select_sql(
    def: &TransformDef,
    relationships: &HashMap<String, RelationshipDef>,
) -> String {
    assert!(
        matches!(def.key_space, KeySpace::OneToOne),
        "render_relationship_select_sql called on a non-1-1 definition"
    );

    // Every to-one path (a bare `<rel>.<column>`, anywhere in an expression
    // tree) needs a LEFT JOIN. A path that is the sole argument of an aggregate
    // is a to-many enrichment — rendered as a correlated subquery below — and
    // contributes no JOIN. A `BTreeSet` dedups a relationship referenced by
    // several columns and orders the JOINs deterministically for stable output.
    let mut to_one_rels: BTreeSet<&str> = BTreeSet::new();
    for field in &def.fields {
        collect_to_one_rels(&field.expr, &mut to_one_rels);
    }

    let select_list: Vec<String> = def
        .fields
        .iter()
        .map(|field| {
            format!(
                "{} as {}",
                render_rel_expr_sql(&field.expr, &def.source, relationships),
                quote_ident(&field.name)
            )
        })
        .collect();

    let mut sql = format!(
        "select {} from {}",
        select_list.join(", "),
        quoted_source_from(def)
    );
    for rel_name in to_one_rels {
        let rel = relationships.get(rel_name).unwrap_or_else(|| {
            panic!("render_relationship_select_sql: unknown relationship '{rel_name}'")
        });
        sql.push_str(&format!(
            " left join {to_table} as {alias} on {source}.{from_col} = {alias}.{to_col}",
            to_table = quote_ident(&rel.to_table),
            alias = quote_ident(rel_name),
            source = quote_ident(&def.source),
            from_col = quote_ident(&rel.from_col),
            to_col = quote_ident(&rel.to_col),
        ));
    }
    sql
}

/// Collects the names of to-one relationships referenced by a bare path (so
/// [`render_relationship_select_sql`] can emit their `LEFT JOIN`s). Mirrors the
/// evaluator's structural test in #29: a `<rel>.<column>` path that is the sole
/// argument of an aggregate call is a to-many enrichment (a correlated
/// subquery, no JOIN), so recursion stops there; every other path is to-one.
#[cfg(any(test, feature = "test-util"))]
fn collect_to_one_rels<'a>(expr: &'a Expr, out: &mut BTreeSet<&'a str>) {
    match expr {
        Expr::RelationshipPath { rel, .. } => {
            out.insert(rel.as_str());
        }
        Expr::FunctionCall { name, args }
            if lookup_aggregate_function(name).is_some()
                && matches!(args.as_slice(), [Expr::RelationshipPath { .. }]) =>
        {
            // To-many enrichment: rendered as a correlated subquery, no JOIN.
        }
        Expr::FunctionCall { args, .. } => {
            for arg in args {
                collect_to_one_rels(arg, out);
            }
        }
        Expr::BinaryOp { lhs, rhs, .. } => {
            collect_to_one_rels(lhs, out);
            collect_to_one_rels(rhs, out);
        }
        Expr::Column(_) | Expr::NumberLiteral(_) | Expr::StringLiteral(_) => {}
    }
}

/// Renders one calculated-field expression of a relationship-enriched 1-1
/// definition. Like [`render_expr_sql`] but relationship-aware: source columns
/// are qualified with the source table (so they don't collide with a JOINed
/// to-side column of the same name), a bare to-one path reads off its JOIN
/// alias, and an aggregate-wrapped to-many path becomes a correlated subquery.
#[cfg(any(test, feature = "test-util"))]
pub(crate) fn render_rel_expr_sql(
    expr: &Expr,
    source: &str,
    relationships: &HashMap<String, RelationshipDef>,
) -> String {
    match expr {
        Expr::Column(name) => format!("{}.{}", quote_ident(source), quote_ident(name)),
        Expr::NumberLiteral(text) => format!("{text}::numeric"),
        Expr::StringLiteral(text) => format!("'{}'::text", text.replace('\'', "''")),
        // A to-one enrichment reads the referenced column off the to-side table
        // the LEFT JOIN brought in, aliased by the relationship name.
        Expr::RelationshipPath { rel, column } => {
            format!("{}.{}", quote_ident(rel), quote_ident(column))
        }
        Expr::BinaryOp { op, lhs, rhs } => {
            let symbol = match op {
                Operator::Add => "+",
                Operator::GreaterThan => ">",
            };
            format!(
                "({} {symbol} {})",
                render_rel_expr_sql(lhs, source, relationships),
                render_rel_expr_sql(rhs, source, relationships)
            )
        }
        // A to-many aggregate (issue #29): an aggregate whose sole argument is a
        // relationship path — the same structural shape the evaluator matches —
        // renders as a correlated aggregate subquery over the to-side table,
        // filtered by the join key. Postgres's empty-set semantics (`count → 0`,
        // the rest → `NULL`, `count(col)` counting non-`NULL`) are exactly what
        // `eval_to_many_aggregate` reproduces, so the two converge.
        Expr::FunctionCall { name, args }
            if lookup_aggregate_function(name).is_some()
                && matches!(args.as_slice(), [Expr::RelationshipPath { .. }]) =>
        {
            let Expr::RelationshipPath { rel, column } = &args[0] else {
                unreachable!("guarded by the matches! above");
            };
            let reldef = relationships.get(rel.as_str()).unwrap_or_else(|| {
                panic!("render_relationship_select_sql: unknown relationship '{rel}'")
            });
            format!(
                "(select {func}({alias}.{col}) from {to_table} as {alias} \
                 where {alias}.{to_col} = {source}.{from_col})",
                func = name.to_lowercase(),
                alias = quote_ident(rel),
                col = quote_ident(column),
                to_table = quote_ident(&reldef.to_table),
                to_col = quote_ident(&reldef.to_col),
                source = quote_ident(source),
                from_col = quote_ident(&reldef.from_col),
            )
        }
        Expr::FunctionCall { name, args } if name == "COUNT" && args.is_empty() => {
            "count(*)".to_string()
        }
        Expr::FunctionCall { name, args } => {
            let rendered_args: Vec<String> = args
                .iter()
                .map(|arg| render_rel_expr_sql(arg, source, relationships))
                .collect();
            format!("{}({})", name.to_lowercase(), rendered_args.join(", "))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::defs::ast::{FieldDef, KeySpace, Operator, Predicate};

    fn def() -> TransformDef {
        TransformDef {
            target: "order_totals".to_string(),
            source: "orders".to_string(),
            key_space: KeySpace::OneToOne,
            fields: vec![
                FieldDef {
                    name: "double_price".to_string(),
                    expr: Expr::BinaryOp {
                        op: Operator::Add,
                        lhs: Box::new(Expr::Column("price".to_string())),
                        rhs: Box::new(Expr::Column("price".to_string())),
                    },
                },
                FieldDef {
                    name: "total".to_string(),
                    expr: Expr::BinaryOp {
                        op: Operator::Add,
                        lhs: Box::new(Expr::Column("double_price".to_string())),
                        rhs: Box::new(Expr::Column("tax".to_string())),
                    },
                },
            ],
            predicate: Predicate::True,
            explicit_source_schema: None,
            explicit_target_schema: None,
        }
    }

    #[test]
    fn referenced_source_columns_excludes_calculated_field_names() {
        let columns = referenced_source_columns(&def());
        assert_eq!(
            columns,
            HashSet::from(["price".to_string(), "tax".to_string()])
        );
    }

    /// A field naming itself (`SELECT c AS c`, the generative suite's
    /// improvement-plan task B1 identity-passthrough shape) must still
    /// include its own source column — `is_self_passthrough`'s carve-out in
    /// `collect_columns`. Before this test's fix, a field's own name was
    /// treated identically to *any other* calculated field's name (both were
    /// in `field_names`), so a self-named field's source column was silently
    /// dropped from the recompute `SELECT` list and `evaluate` failed with
    /// `EvalError::MissingColumn` even though the definition installs and
    /// backfills correctly via the direct-build path
    /// (`trellis/tests/defs_backfill_direct.rs`'s `SELECT a AS a`), which
    /// already special-cases this via `ddl::passthrough_source_column`.
    #[test]
    fn referenced_source_columns_includes_a_self_named_passthrough_columns_own_source() {
        let def = TransformDef {
            target: "t1".to_string(),
            source: "t0".to_string(),
            key_space: KeySpace::OneToOne,
            fields: vec![FieldDef {
                name: "c".to_string(),
                expr: Expr::Column("c".to_string()),
            }],
            predicate: Predicate::True,
            explicit_source_schema: None,
            explicit_target_schema: None,
        };
        let columns = referenced_source_columns(&def);
        assert_eq!(
            columns,
            HashSet::from(["c".to_string()]),
            "a field's own source column must not be excluded just because it shares the \
             field's name"
        );
    }

    #[test]
    fn render_expr_sql_renders_greater_than_composed_with_a_function_call() {
        let expr = Expr::BinaryOp {
            op: Operator::GreaterThan,
            lhs: Box::new(Expr::FunctionCall {
                name: "STRPOS".to_string(),
                args: vec![
                    Expr::Column("name".to_string()),
                    Expr::StringLiteral("foo".to_string()),
                ],
            }),
            rhs: Box::new(Expr::NumberLiteral("0".to_string())),
        };
        assert_eq!(
            render_expr_sql(&expr),
            "(strpos(\"name\", 'foo'::text) > 0::numeric)"
        );
    }

    #[test]
    fn render_expr_sql_renders_coalesce_as_a_postgres_function_call() {
        // COALESCE renders through the generic `name(args)` arm to lowercase
        // `coalesce(...)` — valid Postgres — so the correctness oracle can
        // cross-check the evaluator's COALESCE output against Postgres's own.
        let expr = Expr::FunctionCall {
            name: "COALESCE".to_string(),
            args: vec![
                Expr::Column("amount".to_string()),
                Expr::NumberLiteral("0".to_string()),
            ],
        };
        assert_eq!(render_expr_sql(&expr), "coalesce(\"amount\", 0::numeric)");
    }

    fn category_rel() -> HashMap<String, RelationshipDef> {
        HashMap::from([(
            "category".to_string(),
            RelationshipDef {
                name: "category".to_string(),
                from_table: "products".to_string(),
                from_col: "category_id".to_string(),
                to_table: "categories".to_string(),
                to_col: "id".to_string(),
            },
        )])
    }

    fn comments_rel() -> HashMap<String, RelationshipDef> {
        HashMap::from([(
            "comments".to_string(),
            RelationshipDef {
                name: "comments".to_string(),
                from_table: "posts".to_string(),
                from_col: "id".to_string(),
                to_table: "comments".to_string(),
                to_col: "post_id".to_string(),
            },
        )])
    }

    #[test]
    fn render_relationship_select_sql_renders_a_to_one_left_join() {
        let def = TransformDef {
            target: "product_view".to_string(),
            source: "products".to_string(),
            key_space: KeySpace::OneToOne,
            fields: vec![
                FieldDef {
                    name: "id".to_string(),
                    expr: Expr::Column("id".to_string()),
                },
                FieldDef {
                    name: "category_name".to_string(),
                    expr: Expr::RelationshipPath {
                        rel: "category".to_string(),
                        column: "name".to_string(),
                    },
                },
            ],
            predicate: Predicate::True,
            explicit_source_schema: None,
            explicit_target_schema: None,
        };
        assert_eq!(
            render_relationship_select_sql(&def, &category_rel()),
            "select \"products\".\"id\" as \"id\", \
             \"category\".\"name\" as \"category_name\" \
             from \"products\" \
             left join \"categories\" as \"category\" \
             on \"products\".\"category_id\" = \"category\".\"id\""
        );
    }

    #[test]
    fn render_relationship_select_sql_renders_a_to_many_correlated_aggregate() {
        let def = TransformDef {
            target: "post_stats".to_string(),
            source: "posts".to_string(),
            key_space: KeySpace::OneToOne,
            fields: vec![FieldDef {
                name: "total_words".to_string(),
                expr: Expr::FunctionCall {
                    name: "SUM".to_string(),
                    args: vec![Expr::RelationshipPath {
                        rel: "comments".to_string(),
                        column: "word_count".to_string(),
                    }],
                },
            }],
            predicate: Predicate::True,
            explicit_source_schema: None,
            explicit_target_schema: None,
        };
        assert_eq!(
            render_relationship_select_sql(&def, &comments_rel()),
            "select (select sum(\"comments\".\"word_count\") \
             from \"comments\" as \"comments\" \
             where \"comments\".\"post_id\" = \"posts\".\"id\") as \"total_words\" \
             from \"posts\""
        );
    }

    #[test]
    fn render_expr_sql_escapes_a_single_quote_in_a_function_call_string_literal() {
        let expr = Expr::FunctionCall {
            name: "REGEXP_COUNT".to_string(),
            args: vec![
                Expr::Column("description".to_string()),
                Expr::StringLiteral("o'clock".to_string()),
            ],
        };
        assert_eq!(
            render_expr_sql(&expr),
            "regexp_count(\"description\", 'o''clock'::text)"
        );
    }
}
