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
use std::collections::HashMap;
use std::collections::HashSet;
#[cfg(any(test, feature = "test-util"))]
use std::fmt;

#[cfg(any(test, feature = "test-util"))]
use crate::pool::Pool;
use crate::pool::quote_ident;

use super::ast::{Expr, Operator, TransformDef};
#[cfg(any(test, feature = "test-util"))]
use super::ast::{KeySpace, ValueType};
#[cfg(any(test, feature = "test-util"))]
use super::eval::{EvalError, RegexCache, Row, Value, evaluate, evaluate_aggregate};
use super::typed_literal;

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
/// should read `def`'s source through (issue #76, ADR-0011 grammar clause
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
/// (`docs/decisions/0011-fully-qualified-names.md`) — only the
/// explicit-schema case needed a real fix here, and it needs no database
/// round trip: the schema is already right on `def`.
#[cfg(any(test, feature = "test-util"))]
fn quoted_source_from(def: &TransformDef) -> String {
    match &def.explicit_source_schema {
        Some(schema) => format!("{}.{}", quote_ident(schema), quote_ident(&def.source)),
        None => quote_ident(&def.source),
    }
}

/// Recomputes `def`'s entire target from `def.source`'s current contents,
/// keyed by `pk_column` (the source table's single-column key, e.g. from
/// [`super::ddl::source_primary_key`]). `source_columns` gives each
/// referenced source column's [`ValueType`], the same map `def` was
/// validated against.
///
/// A source keyed by a nullable unique index (an aggregate target's `GROUP
/// BY` column) can hold a row whose key is `NULL`, and the 1-1 target keeps
/// its row (issue #1141): it is keyed here by the key text such a key
/// renders, [`super::ddl::NULL_KEY_SENTINEL`].
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
        let pk_value: String = db_row
            .get::<_, Option<String>>(0)
            .unwrap_or_else(|| super::ddl::NULL_KEY_SENTINEL.to_string());
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

    let mut columns = referenced_source_columns(def);
    for key in group_by {
        columns.insert(key.clone());
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
fn group_key(image: &Row, group_by: &[String]) -> String {
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
        .map(|k| image.get(k.as_str()).cloned().flatten().unwrap_or_default())
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
    let group_cols: Vec<String> = group_by
        .iter()
        .map(|k| render_expr_sql(&Expr::Column(k.clone())))
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
///
/// Issue #622 (C2): `crate::capture::columns` also calls this, to decide which
/// columns a table's capture trigger images. It runs against persisted
/// definitions only, which `validate()` has already accepted, so the same
/// invariant covers it. It leaves out `GROUP BY` keys; that caller adds
/// them.
pub(crate) fn referenced_source_columns(def: &TransformDef) -> HashSet<String> {
    let field_names: HashSet<&str> = def.fields.iter().map(|f| f.name.as_str()).collect();

    // A name inside an aggregate's argument that matches an aggregate field is
    // the source column of that name (issue #695), so it is read even though
    // it isn't the enclosing field's own name.
    let aggregates = super::validate::aggregate_field_names(
        def.fields.iter().map(|f| (f.name.as_str(), &f.expr)),
    );
    let mut columns = HashSet::new();
    for field in &def.fields {
        collect_columns(&field.expr, field.name.as_str(), &field_names, &mut columns);
        let mut ignored = Vec::new();
        let mut aggregate_args = Vec::new();
        super::validate::collect_reads(
            &field.expr,
            &aggregates,
            false,
            &mut ignored,
            &mut aggregate_args,
        );
        columns.extend(aggregate_args);
    }
    columns
}

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
        Expr::SourceColumn { column, .. } => {
            out.insert(column.clone());
        }
        Expr::NumberLiteral(_) | Expr::StringLiteral(_) | Expr::TypedLiteral { .. } => {}
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
        Expr::Column(name) | Expr::SourceColumn { column: name, .. } => quote_ident(name),
        Expr::NumberLiteral(text) => format!("{text}::numeric"),
        Expr::StringLiteral(text) => format!("'{}'::text", text.replace('\'', "''")),
        Expr::TypedLiteral { value_type, text } => typed_literal::render_sql(*value_type, text),
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

/// [`render_expr_sql`] with every source column qualified by `source_sql`
/// (already-quoted SQL text — a quoted table name, or a query alias such as
/// `s`), for SQL that reads the source under an alias: the aggregate build
/// (`super::backfill::backfill_aggregate`), the ledger Re-derive
/// (`staging::ledger`) and the group-basis and `JOIN` partial reads.
pub(crate) fn render_qualified_expr_sql(expr: &Expr, source_sql: &str) -> String {
    match expr {
        Expr::Column(name) | Expr::SourceColumn { column: name, .. } => {
            format!("{source_sql}.{}", quote_ident(name))
        }
        Expr::NumberLiteral(text) => format!("{text}::numeric"),
        Expr::StringLiteral(text) => format!("'{}'::text", text.replace('\'', "''")),
        Expr::TypedLiteral { value_type, text } => typed_literal::render_sql(*value_type, text),
        Expr::BinaryOp { op, lhs, rhs } => {
            let symbol = match op {
                Operator::Add => "+",
                Operator::GreaterThan => ">",
            };
            format!(
                "({} {symbol} {})",
                render_qualified_expr_sql(lhs, source_sql),
                render_qualified_expr_sql(rhs, source_sql)
            )
        }
        Expr::FunctionCall { name, args } if name == "COUNT" && args.is_empty() => {
            "count(*)".to_string()
        }
        Expr::FunctionCall { name, args } => {
            let rendered: Vec<String> = args
                .iter()
                .map(|arg| render_qualified_expr_sql(arg, source_sql))
                .collect();
            format!("{}({})", name.to_lowercase(), rendered.join(", "))
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
            without_source_index: false,
            explicit_source_schema: None,
            explicit_target_schema: None,
        }
    }

    /// Issue #695: `MIN(b)` reads the source column `b` even though a field
    /// named `b` exists (an aggregate), so the capture trigger must image it.
    #[test]
    fn referenced_source_columns_includes_a_source_column_named_like_an_aggregate_field() {
        let def = crate::defs::parse(
            "TRANSFORM t FROM s GROUP BY g SELECT g AS g, SUM(a) AS b, MIN(b) AS lo",
        )
        .expect("parse");
        let columns = referenced_source_columns(&def);
        assert!(columns.contains("b"), "{columns:?}");
        assert!(columns.contains("a"), "{columns:?}");
    }

    /// Issue #830: `SUM(s.val)` reads the source column `val` though a field
    /// named `val` exists, so the capture trigger images it and the SQL
    /// oracle sums it.
    #[test]
    fn a_qualified_name_in_an_aggregate_reads_the_source_column() {
        let def = crate::defs::parse(
            "TRANSFORM t FROM s GROUP BY grp SELECT grp AS grp, (grp + 1) AS val, \
             SUM(s.val) AS total",
        )
        .expect("parse");
        let columns = referenced_source_columns(&def);
        assert!(columns.contains("val"), "{columns:?}");
        let sql = render_aggregate_select_sql(&def);
        assert!(sql.contains(r#"sum("val")"#), "{sql}");
        assert!(!sql.contains(r#"sum(("grp""#), "{sql}");
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
            without_source_index: false,
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
