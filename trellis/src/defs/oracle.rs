//! Renders a validated definition's expressions as Postgres SQL.
//!
//! Postgres evaluates every calculated field: the engine renders the
//! definition into the statement that writes it (ADR-0002) and computes no
//! value itself. [`render_expr_sql`] and [`render_qualified_expr_sql`] are the
//! renderers; [`render_aggregate_select_sql`] renders a whole aggregate
//! definition as the `SELECT ... GROUP BY` that tests compare a target with.

use std::collections::HashSet;

use crate::pool::quote_ident;

#[cfg(any(test, feature = "test-util"))]
use super::ast::KeySpace;
use super::ast::{Expr, TransformDef, ValueType};
use super::typed_literal;
use crate::integer::IntWidth;

/// The `FROM`-clause identifier this test/benchmark-only oracle module
/// should read `def`'s source through (ADR-0011 grammar clause
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

/// Renders a [`KeySpace::OneToOne`] `def` back to the Postgres `SELECT` that
/// computes its whole target from the source's current contents: the primary
/// key `pk_column` as text first, then each calculated field as text, in
/// definition order. Tests compare a maintained target with the rows this
/// returns. Every field is substituted first, as in
/// [`render_aggregate_select_sql`].
///
/// # Panics
///
/// If `def.key_space` is not [`KeySpace::OneToOne`], or if substitution fails
/// (see [`render_aggregate_select_sql`]).
#[cfg(any(test, feature = "test-util"))]
pub fn render_one_to_one_select_sql(def: &TransformDef, pk_column: &str) -> String {
    assert!(
        matches!(def.key_space, KeySpace::OneToOne),
        "render_one_to_one_select_sql called on a non-1-1 definition"
    );
    let substituted = super::backfill::substituted_field_exprs(def)
        .expect("1-1 oracle rendering requires a substitutable definition");
    let mut select_list = vec![format!("{}::text", quote_ident(pk_column))];
    select_list.extend(def.fields.iter().map(|field| {
        format!(
            "({})::text as {}",
            render_expr_sql(&substituted[&field.name]),
            quote_ident(&field.name)
        )
    }));
    format!(
        "select {} from {}",
        select_list.join(", "),
        quoted_source_from(def)
    )
}

/// Renders an [`KeySpace::Aggregate`] `def` back to the equivalent Postgres
/// `SELECT ... GROUP BY ...` (the correctness oracle):
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
/// field's name. `crate::capture::columns` uses it to decide which columns a
/// table's capture trigger images; it leaves out `GROUP BY` keys, which that
/// caller adds.
///
/// A field referencing a source column of its own name (`SELECT c AS c`, an
/// identity passthrough) is a passthrough, not a reference to another
/// calculated field, so its own source column is included — the same
/// `is_self_passthrough` carve-out `validate.rs`'s cycle detection makes (the
/// build's `SELECT a AS a` has it via `ddl::passthrough_source_column`).
///
/// Unlike `validate.rs`'s version of that carve-out, this one doesn't also
/// check that `name` actually names a real source column: it takes no
/// `source_columns` map. That's safe because the caller runs only against
/// persisted definitions, which `validate()` has already accepted, and
/// `validate()` rejects a field naming itself when that name isn't a real
/// source column (`UnresolvedColumn`). It is an invariant enforced by caller
/// discipline: a caller holding a definition that hasn't been through
/// `validate()` must thread `source_columns` through here and add the same
/// guard.
pub(crate) fn referenced_source_columns(def: &TransformDef) -> HashSet<String> {
    let field_names: HashSet<&str> = def.fields.iter().map(|f| f.name.as_str()).collect();

    // A name inside an aggregate's argument that matches an aggregate field is
    // the source column of that name, so it is read even though
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
        Expr::Negate(operand) | Expr::Cast { expr: operand, .. } => {
            collect_columns(operand, field_name, field_names, out);
        }
        Expr::FunctionCall { args, .. } => {
            for arg in args {
                collect_columns(arg, field_name, field_names, out);
            }
        }
        Expr::Not(_) | Expr::IsNull { .. } | Expr::Case { .. } => {
            for child in expr.logic_children() {
                collect_columns(child, field_name, field_names, out);
            }
        }
    }
}

/// Renders a calculated-field expression back to the equivalent Postgres
/// SQL expression text: the statements that write a 1-1 target embed it, so
/// Postgres computes the field (ADR-0002) — the ADR-0004 claim ("our grammar
/// is an immutable subset of Postgres semantics") made literal for every
/// expression. Column references are quoted identifiers, since a source
/// column name could collide with a SQL keyword; literals carry an explicit
/// cast so the rendered text is unambiguous regardless of context.
pub fn render_expr_sql(expr: &Expr) -> String {
    match expr {
        Expr::Column(name) | Expr::SourceColumn { column: name, .. } => quote_ident(name),
        Expr::NumberLiteral(text) => number_literal_sql(text),
        Expr::StringLiteral(text) => format!("'{}'::text", text.replace('\'', "''")),
        Expr::TypedLiteral { value_type, text } => typed_literal::render_sql(*value_type, text),
        Expr::BinaryOp { op, lhs, rhs } => {
            let symbol = super::registry::operator_spec(*op).symbol;
            format!(
                "({} {symbol} {})",
                render_expr_sql(lhs),
                render_expr_sql(rhs)
            )
        }
        Expr::Negate(operand) => format!("(-({}))", render_expr_sql(operand)),
        Expr::Cast { expr, target } => {
            format!("cast({} as {target})", render_expr_sql(expr))
        }
        Expr::Not(operand) => format!("(not {})", render_expr_sql(operand)),
        Expr::IsNull { expr, negated } => format!(
            "({} is {}null)",
            render_expr_sql(expr),
            if *negated { "not " } else { "" }
        ),
        Expr::Case {
            branches,
            otherwise,
        } => render_case_sql(branches, otherwise.as_deref(), render_expr_sql),
        Expr::FunctionCall { name, args } if name == "COUNT" && args.is_empty() => {
            // `COUNT(*)`: the parser's only accepted `COUNT`
            // shape, but its AST has no argument to render — `count()` is
            // not valid Postgres, so this renders the `*` back explicitly
            // rather than falling through to the generic `name(args)` case
            // below.
            "count(*)".to_string()
        }
        Expr::FunctionCall { name, args } => match super::ast::as_extract(name, args) {
            Some((field, operand)) => format!("extract({field} from {})", render_expr_sql(operand)),
            None => {
                let rendered_args: Vec<String> = args.iter().map(render_expr_sql).collect();
                format!("{}({})", name.to_lowercase(), rendered_args.join(", "))
            }
        },
    }
}

/// A searched `CASE` as SQL text, each condition and result rendered by
/// `render`. Shared by every renderer so they spell the construct alike.
pub(crate) fn render_case_sql(
    branches: &[(Expr, Expr)],
    otherwise: Option<&Expr>,
    mut render: impl FnMut(&Expr) -> String,
) -> String {
    let mut text = String::from("(case");
    for (condition, result) in branches {
        text.push_str(&format!(
            " when {} then {}",
            render(condition),
            render(result)
        ));
    }
    if let Some(otherwise) = otherwise {
        text.push_str(&format!(" else {}", render(otherwise)));
    }
    text.push_str(" end)");
    text
}

/// An unadorned number literal, cast to the type `validate` infers for it
/// (`integer` if it fits, else `bigint`, else `numeric`), which is the type
/// Postgres's own lexer gives the bare literal. Overflow, and the result
/// type an operator over it has, are then the ones the definition's
/// declared column types were inferred with: `int4_col + 1` is `integer`
/// and overflows where an `integer` does. `self_check`'s audit renders a
/// literal with this too, so a date takes `date + integer` there as here.
pub(crate) fn number_literal_sql(text: &str) -> String {
    // A negative literal is parenthesized: `::` binds tighter than the sign,
    // and `-2147483648::integer` casts 2147483648 first.
    let shown = if text.starts_with('-') {
        format!("({text})")
    } else {
        text.to_string()
    };
    let pg_type = match super::validate::number_literal_type(text) {
        ValueType::Integer(IntWidth::Int4) => "integer",
        ValueType::Integer(IntWidth::Int8) => "bigint",
        _ => "numeric",
    };
    format!("{shown}::{pg_type}")
}

/// [`render_expr_sql`] with every source column qualified by `source_sql`
/// (already-quoted SQL text — a quoted table name, or a query alias such as
/// `s`), for SQL that reads the source under an alias: the group-basis and
/// `JOIN` partial reads.
pub(crate) fn render_qualified_expr_sql(expr: &Expr, source_sql: &str) -> String {
    match expr {
        Expr::Column(name) | Expr::SourceColumn { column: name, .. } => {
            format!("{source_sql}.{}", quote_ident(name))
        }
        Expr::NumberLiteral(text) => number_literal_sql(text),
        Expr::StringLiteral(text) => format!("'{}'::text", text.replace('\'', "''")),
        Expr::TypedLiteral { value_type, text } => typed_literal::render_sql(*value_type, text),
        Expr::BinaryOp { op, lhs, rhs } => {
            let symbol = super::registry::operator_spec(*op).symbol;
            format!(
                "({} {symbol} {})",
                render_qualified_expr_sql(lhs, source_sql),
                render_qualified_expr_sql(rhs, source_sql)
            )
        }
        Expr::Negate(operand) => {
            format!("(-({}))", render_qualified_expr_sql(operand, source_sql))
        }
        Expr::Cast { expr, target } => format!(
            "cast({} as {target})",
            render_qualified_expr_sql(expr, source_sql)
        ),
        Expr::Not(operand) => format!("(not {})", render_qualified_expr_sql(operand, source_sql)),
        Expr::IsNull { expr, negated } => format!(
            "({} is {}null)",
            render_qualified_expr_sql(expr, source_sql),
            if *negated { "not " } else { "" }
        ),
        Expr::Case {
            branches,
            otherwise,
        } => render_case_sql(branches, otherwise.as_deref(), |e| {
            render_qualified_expr_sql(e, source_sql)
        }),
        Expr::FunctionCall { name, args } if name == "COUNT" && args.is_empty() => {
            "count(*)".to_string()
        }
        Expr::FunctionCall { name, args } => match super::ast::as_extract(name, args) {
            Some((field, operand)) => format!(
                "extract({field} from {})",
                render_qualified_expr_sql(operand, source_sql)
            ),
            None => {
                let rendered: Vec<String> = args
                    .iter()
                    .map(|arg| render_qualified_expr_sql(arg, source_sql))
                    .collect();
                format!("{}({})", name.to_lowercase(), rendered.join(", "))
            }
        },
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

    /// `MIN(b)` reads the source column `b` even though a field
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

    /// `SUM(s.val)` reads the source column `val` though a field
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

    /// A field naming itself (`SELECT c AS c`, an identity passthrough) must
    /// still include its own source column — `is_self_passthrough`'s carve-out
    /// in `collect_columns` — or the capture trigger would not image it,
    /// though the build (`SELECT a AS a`) special-cases it via
    /// `ddl::passthrough_source_column`.
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
            "(strpos(\"name\", 'foo'::text) > 0::integer)"
        );
    }

    #[test]
    fn render_expr_sql_renders_coalesce_as_a_postgres_function_call() {
        // COALESCE renders through the generic `name(args)` arm to lowercase
        // `coalesce(...)` — valid Postgres.
        let expr = Expr::FunctionCall {
            name: "COALESCE".to_string(),
            args: vec![
                Expr::Column("amount".to_string()),
                Expr::NumberLiteral("0".to_string()),
            ],
        };
        assert_eq!(render_expr_sql(&expr), "coalesce(\"amount\", 0::integer)");
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
