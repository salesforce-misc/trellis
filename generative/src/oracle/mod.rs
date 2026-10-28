//! The independent recompute oracle (design doc §2): renders a
//! [`trellis::dev::defs::ast::TransformDef`]'s formulas back to a `SELECT` against
//! the source and treats Postgres itself as the correctness authority. Uses
//! only the shared components design doc §2 names: the parser AST and a small
//! AST→`SELECT` printer written here, sharing no code with the engine's
//! renderer.
//!
//! # The comparison
//!
//! Per op the persisted target ([`crate::backend::Backend::snapshot`] read back
//! from the maintained target table) is compared with the **SQL oracle**: the
//! definition rendered back to a `SELECT` and run by Postgres itself. The SQL
//! oracle is the authority (ADR-0004: the grammar is an immutable subset of
//! Postgres, so a rendered `SELECT` can never be outgrown by the language). A
//! divergence is a pipeline / apply / fold / ordering bug, or a rendering bug
//! in the engine's compiler, which the independent printer here catches.
//!
//! # DB access
//!
//! The oracle takes a [`trellis::Pool`] (a connection handle). The oracle
//! *reads*; it never imports `trellis::client`/`trellis::staging` or otherwise
//! drives the maintenance pipeline (that seam is [`crate::backend`]'s alone). A
//! pool connection has its `search_path` pinned, so an unqualified source
//! table name resolves the same way the engine's reads do.

use std::collections::{BTreeMap, HashMap};
use std::fmt;

use trellis::Pool;
use trellis::dev::defs::ast::{Expr, KeySpace, Operator, Predicate, TransformDef, ValueType};
use trellis::dev::defs::registry;
use trellis::dev::defs::typed_literal;
use trellis::dev::defs::{FloatWidth, IntWidth};

use crate::model::{Program, group_key};

/// Why an oracle read failed: a Postgres query error, or no pooled connection.
#[derive(Debug)]
pub struct OracleError(Box<dyn std::error::Error + Send + Sync>);

impl fmt::Display for OracleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "oracle read failed: {}", self.0)
    }
}

impl<E: std::error::Error + Send + Sync + 'static> From<E> for OracleError {
    fn from(err: E) -> Self {
        OracleError(Box::new(err))
    }
}

/// One logical target's rows: `pk (rendered text) -> column -> value
/// (rendered text, `None` is SQL `NULL`)`. Matches the inner shape of
/// [`crate::backend::Snapshot`], and always excludes the primary-key column
/// itself (the pk is the row key, so carrying it as a column too would show
/// up as a spurious divergence against the SQL oracle, which keys by
/// pk but never emits it as a field).
pub type Rows = BTreeMap<String, BTreeMap<String, Option<String>>>;

/// How two rendered-text cell values are judged equal (design doc §5).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Comparison {
    /// Byte-exact text equality — int, text, date, timestamp, uuid, boolean.
    Exact,
    /// Decimal by value, ignoring scale (`2.50 ≡ 2.5`).
    DecimalByValue,
    /// Combined absolute+relative tolerance for `real`/`double precision`
    /// (this arm exists because of [`ValueType::Float`]).
    ///
    /// Tolerance rather than [`Comparison::Exact`] even though
    /// `trellis::float::render` reproduces `float4out`/`float8out`
    /// byte-for-byte, because the *values* being compared can legitimately
    /// differ in their last bits: float addition isn't associative, so an
    /// engine-side fold and a server-side `sum()` over the same rows in a
    /// different order really do produce different doubles
    /// (`1e16::float8+1+1+1+1` is `1e+16`, `1+1+1+1+1e16` is
    /// `1.0000000000000004e+16`). That is not a bug to report — it is why
    /// a float `SUM`/`AVG` is refused at define.
    ///
    /// `NaN`/`±Infinity` are compared by Postgres's own float equality
    /// ([`postgres_float_equal`]: `NaN = NaN` is true), not IEEE's, so a
    /// converged `NaN` column doesn't report as a perpetual divergence.
    #[allow(dead_code)]
    FloatTolerance,
}

impl Comparison {
    /// The comparison a column of `value_type` uses.
    fn for_type(value_type: ValueType) -> Comparison {
        match value_type {
            ValueType::Numeric => Comparison::DecimalByValue,
            // An exact integer has exactly one text rendering
            // per value on both sides — Postgres's `int4out` and Rust's
            // `i64` `Display` agree character for character — so byte-exact
            // comparison is both correct *and* strictly stronger than
            // `DecimalByValue` here: it would catch a scale or padding
            // difference that a by-value decimal compare would forgive, and
            // for this type any such difference is a real bug.
            ValueType::Integer(_) => Comparison::Exact,
            // See `Comparison::FloatTolerance`.
            ValueType::Float(_) => Comparison::FloatTolerance,
            // `Other` is passthrough-only (no arithmetic, no
            // scale-insensitive semantics defined for it yet), same footing
            // as `Text`/`Boolean`/`Uuid` — byte-exact text equality is the
            // right default until a family's own epic child teaches it
            // something more specific.
            ValueType::Text | ValueType::Boolean | ValueType::Uuid | ValueType::Other(_) => {
                Comparison::Exact
            }
        }
    }

    fn equal(self, expected: &Option<String>, got: &Option<String>) -> bool {
        match self {
            Comparison::Exact => expected == got,
            Comparison::DecimalByValue => match (expected, got) {
                (None, None) => true,
                (Some(a), Some(b)) => decimal_text(a) == decimal_text(b),
                _ => false,
            },
            Comparison::FloatTolerance => match (expected, got) {
                (None, None) => true,
                (Some(a), Some(b)) => match (a.parse::<f64>(), b.parse::<f64>()) {
                    (Ok(a), Ok(b)) => floats_close(a, b),
                    // Text that isn't a float at all (`NaN`, `Infinity`,
                    // `-Infinity` all *do* parse in Rust, so this is a
                    // genuinely unparseable rendering) can only be compared
                    // exactly, same fallback `DecimalByValue` takes.
                    _ => a == b,
                },
                _ => false,
            },
        }
    }
}

/// Postgres's float equality: `NaN = NaN` is true (IEEE says false) and
/// `-0 = 0` is true.
fn postgres_float_equal(a: f64, b: f64) -> bool {
    a == b || (a.is_nan() && b.is_nan())
}

/// A decimal's text with its scale and sign-of-zero normalized away, so two
/// spellings of one value (`2.50`, `2.5`, `-0`, `0.0`) compare equal. Text that
/// is not a plain decimal is returned as is, so a genuine difference still
/// surfaces rather than being swallowed.
fn decimal_text(text: &str) -> String {
    let (negative, digits) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text),
    };
    let plain = !digits.is_empty()
        && digits.chars().all(|c| c.is_ascii_digit() || c == '.')
        && digits.matches('.').count() <= 1;
    if !plain {
        return text.to_string();
    }
    let (int, frac) = digits.split_once('.').unwrap_or((digits, ""));
    let int = int.trim_start_matches('0');
    let frac = frac.trim_end_matches('0');
    if int.is_empty() && frac.is_empty() {
        return "0".to_string();
    }
    format!(
        "{}{}{}{}",
        if negative { "-" } else { "" },
        if int.is_empty() { "0" } else { int },
        if frac.is_empty() { "" } else { "." },
        frac
    )
}

/// Whether two floats agree within the combined absolute+relative tolerance
/// [`Comparison::FloatTolerance`] applies.
///
/// Equality is checked first, through [`postgres_float_equal`] rather than
/// `==`, so that `NaN`/`NaN` and `-0`/`0` pairs agree exactly as Postgres
/// says they do. Only then does the tolerance apply, and it is deliberately
/// tight: `1e-9` relative is roughly 10^7 times the `f64` epsilon, enough to
/// absorb reassociation of a realistic fold but far too small to hide a
/// genuinely wrong computation. The absolute floor handles values near zero,
/// where a relative bound degenerates.
fn floats_close(a: f64, b: f64) -> bool {
    const RELATIVE: f64 = 1e-9;
    const ABSOLUTE: f64 = 1e-12;
    if postgres_float_equal(a, b) {
        return true;
    }
    // A special value that isn't equal to the other side (e.g. `NaN` vs `1`,
    // or `Infinity` vs `-Infinity`) is a real divergence, not a rounding
    // difference — no tolerance can bridge it.
    if !a.is_finite() || !b.is_finite() {
        return false;
    }
    let diff = (a - b).abs();
    diff <= ABSOLUTE || diff <= RELATIVE * a.abs().max(b.abs())
}

/// One differing cell, row, or column found while comparing a candidate
/// rendering against the SQL-oracle authority. `expected` is always the SQL
/// oracle's value; `got` is the persisted target's.
#[derive(Debug, Clone, PartialEq)]
pub enum Divergence {
    Cell {
        table: String,
        pk: String,
        column: String,
        expected: Option<String>,
        got: Option<String>,
    },
    /// A row the SQL oracle produced but the candidate is missing.
    MissingRow { table: String, pk: String },
    /// A row the candidate has but the SQL oracle did not produce.
    ExtraRow { table: String, pk: String },
    /// A column the SQL oracle produced but the candidate's row is missing.
    MissingColumn {
        table: String,
        pk: String,
        column: String,
    },
    /// A column the candidate's row has but the SQL oracle did not produce.
    ExtraColumn {
        table: String,
        pk: String,
        column: String,
    },
}

fn show(value: &Option<String>) -> String {
    match value {
        Some(text) => text.clone(),
        None => "NULL".to_string(),
    }
}

impl fmt::Display for Divergence {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Divergence::Cell {
                table,
                pk,
                column,
                expected,
                got,
            } => write!(
                f,
                "{table}[{pk}].{column}: expected={} got={}",
                show(expected),
                show(got)
            ),
            Divergence::MissingRow { table, pk } => {
                write!(
                    f,
                    "{table}[{pk}]: present in SQL oracle, absent in candidate"
                )
            }
            Divergence::ExtraRow { table, pk } => {
                write!(
                    f,
                    "{table}[{pk}]: present in candidate, absent in SQL oracle"
                )
            }
            Divergence::MissingColumn { table, pk, column } => write!(
                f,
                "{table}[{pk}].{column}: present in SQL oracle, absent in candidate"
            ),
            Divergence::ExtraColumn { table, pk, column } => write!(
                f,
                "{table}[{pk}].{column}: present in candidate, absent in SQL oracle"
            ),
        }
    }
}

/// The result of comparing one definition's persisted target with the SQL
/// oracle. Prints the whole [`Program`] on any divergence: a diff without the
/// program is unactionable (design doc §5).
#[derive(Debug, Clone)]
pub struct OracleReport {
    /// `persisted target ≠ SQL oracle`: a pipeline/apply/fold/ordering bug, or
    /// a rendering bug in the engine's compiler.
    pub target_vs_sql: Vec<Divergence>,
    program: String,
}

impl OracleReport {
    /// Whether the comparison diverged.
    pub fn diverged(&self) -> bool {
        !self.target_vs_sql.is_empty()
    }
}

impl fmt::Display for OracleReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if !self.diverged() {
            return write!(f, "no divergence");
        }
        writeln!(f, "target != SQL oracle:")?;
        for divergence in &self.target_vs_sql {
            writeln!(f, "  {divergence}")?;
        }
        write!(f, "program:\n{}", self.program)
    }
}

/// Quotes a Postgres identifier for safe interpolation into rendered SQL,
/// mirroring `trellis::pool`'s own (crate-private) helper of the same name.
fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

/// Renders a calculated-field expression back to Postgres SQL text.
///
/// Deliberately independent of
/// `trellis::dev::defs::oracle::render_expr_sql` — the oracle's whole value is that
/// its `SELECT` shares no code with the thing it checks (both happen to
/// choose the same idiomatic rendering — quoted identifiers, an explicit cast
/// on every literal, `name(args)` for a function call — independently, not by
/// sharing an implementation). Improvement-plan task B2 widened this past the
/// original numeric-`+` slice to `Operator::GreaterThan`,
/// `Expr::StringLiteral`, and the five scalar functions
/// (`STRPOS`/`OCTET_LENGTH`/`CHAR_LENGTH`/`REGEXP_COUNT`/`COALESCE`) the
/// generator now draws (see `crate::generate::DerivedShape`); task B4 widens
/// it again to the `KeySpace::Aggregate` functions
/// (`SUM`/`COUNT`/`AVG`/`MIN`/`MAX`, plus `BOOL_AND`/`BOOL_OR`, see
/// `crate::generate::AggregateFn`). Every one of those renders as the same generic `name(args)`
/// Postgres call syntax (the fallthrough `FunctionCall` arm below) except `COUNT`'s
/// argument-less `COUNT(*)`, which the grammar's AST carries with an empty
/// `args` list — special-cased so it doesn't fall through to the generic arm
/// and emit the invalid `count()`. Anything still beyond that (a path into
/// another table, which only a `JOIN` reads, rendered by
/// [`render_join_select`] instead) panics naming the missing work rather than guessing (design doc §2
/// "refuse to guess"), so a future widening fails loudly instead of emitting
/// a false differential.
///
/// **Collation.** `>` here is `Numeric, Numeric -> Boolean`
/// (`trellis::dev::defs::registry::OPERATORS` never gives it a `Text` operand), and
/// none of the five scalar functions performs a collation-sensitive
/// comparison: `STRPOS` is a plain substring search (byte/character match,
/// not locale ordering), `OCTET_LENGTH`/`CHAR_LENGTH` just count,
/// `REGEXP_COUNT`'s patterns are drawn only from
/// `generate::strategy::REGEXP_COUNT_PATTERN_POOL` (literal text and
/// `.`/`*`/`+`/`?`/`[...]`/`|`/`^`/`$` — no locale-dependent POSIX bracket
/// classes like `[[:alpha:]]`), and `COALESCE` does no comparison at all (it
/// just returns its first non-`NULL` argument). `SUM`/`COUNT`/`AVG`/`MIN`/
/// `MAX` are likewise all numeric aggregation, and `BOOL_AND`/`BOOL_OR`
/// boolean aggregation, with no text/collation involvement. So unlike task B1's text *ordering* concern (which that
/// task's own plan flags as the load-bearing collation risk), nothing B2 or
/// B4 adds needs the oracle and the engine's underlying Postgres session
/// pinned to the same collation — there is no ordering comparison in this
/// grammar for the two to disagree about.
fn render_expr(expr: &Expr) -> String {
    match expr {
        Expr::Column(name) | Expr::SourceColumn { column: name, .. } => quote_ident(name),
        Expr::NumberLiteral(text) => text.clone(),
        Expr::StringLiteral(text) => format!("'{}'::text", text.replace('\'', "''")),
        Expr::TypedLiteral { value_type, text } => typed_literal::render_sql(*value_type, text),
        Expr::BinaryOp { op, lhs, rhs } => {
            let symbol = match op {
                Operator::Add => "+",
                Operator::GreaterThan => ">",
            };
            format!("({} {symbol} {})", render_expr(lhs), render_expr(rhs))
        }
        // `COUNT(*)` (task B4, mirroring `trellis::dev::defs::oracle::render_expr_sql`'s
        // own special case): the parser's only accepted `COUNT` shape has no
        // argument at all in the AST (`args` is empty), so `*` is rendered
        // back explicitly rather than falling through to the generic
        // `name(args)` arm below, which would emit the invalid `count()`.
        Expr::FunctionCall { name, args } if name == "COUNT" && args.is_empty() => {
            "count(*)".to_string()
        }
        Expr::FunctionCall { name, args } => {
            let rendered_args: Vec<String> = args.iter().map(render_expr).collect();
            format!("{}({})", name.to_lowercase(), rendered_args.join(", "))
        }
    }
}

/// Renders a 1-1 `def` back to `SELECT <pk>, (<expr>) AS <field>, ... FROM
/// <source>`, or (task B4) an `Aggregate` `def` back to `SELECT (<expr>) AS
/// <field>, ... FROM <source> GROUP BY <group_by>`. Every projection is cast
/// to `text` so the read-back matches the rendered-text shape
/// [`crate::backend::Snapshot`] uses. The `Aggregate` arm renders every field
/// through the same per-field loop as the 1-1 arm — including the
/// grouping-column passthrough field(s) (`SELECT order_id AS order_id, ...`)
/// — rather than special-casing them out of the select list: [`sql_oracle`]
/// is what picks the grouping columns back out of the result by name to build
/// each row's key (see its doc comment), so no separate "leading pk
/// expression" is needed here the way the 1-1 arm needs one for `pk_column`.
///
/// # Panics
///
/// On any shape beyond the numeric-`+`/aggregate slice this oracle models
/// (a non-trivial predicate is a later slice), naming the follow-up that
/// widens it.
fn render_select(def: &TransformDef, pk_column: &str) -> String {
    match def.predicate {
        Predicate::True => {}
    }

    match &def.key_space {
        KeySpace::Join(_) => unreachable!("the generator never produces a JOIN definition"),
        KeySpace::OneToOne => {
            let mut select_list = vec![format!("{}::text", quote_ident(pk_column))];
            for field in &def.fields {
                select_list.push(format!(
                    "({})::text as {}",
                    render_expr(&field.expr),
                    quote_ident(&field.name)
                ));
            }
            format!(
                "select {} from {}",
                select_list.join(", "),
                quote_ident(&def.source)
            )
        }
        KeySpace::Aggregate { group_by } => {
            let select_list: Vec<String> = def
                .fields
                .iter()
                .map(|field| {
                    format!(
                        "({})::text as {}",
                        render_expr(&field.expr),
                        quote_ident(&field.name)
                    )
                })
                .collect();
            let group_cols: Vec<String> =
                group_by.iter().map(|k| quote_ident(k.as_str())).collect();
            format!(
                "select {} from {} group by {}",
                select_list.join(", "),
                quote_ident(&def.source),
                group_cols.join(", ")
            )
        }
    }
}

/// Runs `def` rendered as a `SELECT` (see [`render_select`]) in `pool`'s
/// cluster and reads the result back into field-only [`Rows`] keyed by the
/// primary key (1-1) or the grouping columns' composite [`group_key`]
/// (`Aggregate`, task B4). The authority side of the comparison.
///
/// For an `Aggregate` def, `pk_column` is unused: there is no single leading
/// pk expression in the rendered `SELECT` (see [`render_select`]'s doc
/// comment), so this instead locates each `group_by` column by name among
/// `def.fields` (every grouping column has a passthrough field of the same
/// name — enforced by `defs::validate::validate`'s
/// `GroupingColumnFieldMustBePassthrough` check) and excludes it from the
/// row's own `by_column` map, exactly as [`target_fields`] excludes the 1-1
/// pk column.
pub async fn sql_oracle(
    pool: &Pool,
    def: &TransformDef,
    pk_column: &str,
) -> Result<Rows, OracleError> {
    let sql = render_select(def, pk_column);
    let client = pool.get().await?;
    let db_rows = client.query(sql.as_str(), &[]).await?;

    match &def.key_space {
        KeySpace::Join(_) => unreachable!("the generator never produces a JOIN definition"),
        KeySpace::OneToOne => {
            let mut rows: Rows = BTreeMap::new();
            for db_row in db_rows {
                let pk: Option<String> = db_row.get(0);
                let pk = pk.expect("primary key column is never NULL");
                let mut by_column = BTreeMap::new();
                for (i, field) in def.fields.iter().enumerate() {
                    by_column.insert(field.name.clone(), db_row.get::<_, Option<String>>(i + 1));
                }
                rows.insert(pk, by_column);
            }
            Ok(rows)
        }
        KeySpace::Aggregate { group_by } => {
            let mut rows: Rows = BTreeMap::new();
            for db_row in db_rows {
                let values: Vec<Option<String>> = (0..def.fields.len())
                    .map(|i| db_row.get::<_, Option<String>>(i))
                    .collect();
                let group_values: Vec<Option<String>> = group_by
                    .iter()
                    .map(|key| {
                        let column = key.as_str();
                        let index = def
                            .fields
                            .iter()
                            .position(|f| f.name == column)
                            .unwrap_or_else(|| {
                                panic!(
                                    "oracle: GROUP BY column {column:?} has no matching \
                                     passthrough field on {:?} — a generator bug, or a \
                                     definition that should have failed validation",
                                    def.target
                                )
                            });
                        values[index].clone()
                    })
                    .collect();
                let key = group_key(&group_values);
                let mut by_column = BTreeMap::new();
                for (field, value) in def.fields.iter().zip(values) {
                    if group_by.iter().any(|k| k.as_str() == field.name) {
                        continue;
                    }
                    by_column.insert(field.name.clone(), value);
                }
                rows.insert(key, by_column);
            }
            Ok(rows)
        }
    }
}

/// Strips the primary-key column from a target's rows as read back by
/// [`crate::backend::Backend::snapshot`], yielding the field-only [`Rows`] the
/// comparison expects. The pk stays as each row's key.
pub fn target_fields(target: &Rows, pk_column: &str) -> Rows {
    target
        .iter()
        .map(|(pk, columns)| {
            let fields = columns
                .iter()
                .filter(|(name, _)| name.as_str() != pk_column)
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect();
            (pk.clone(), fields)
        })
        .collect()
}

/// Diffs a `candidate` rendering against the SQL-oracle `authority`,
/// classifying each cell by the comparison its field's [`ValueType`] uses.
/// A column present on one side and absent on the other is a divergence, not
/// a silent skip (design doc §5).
fn diff(
    table: &str,
    authority: &Rows,
    candidate: &Rows,
    comparison_for: &dyn Fn(&str) -> Comparison,
) -> Vec<Divergence> {
    let mut divergences = Vec::new();

    let mut pks: BTreeMap<&String, ()> = BTreeMap::new();
    for pk in authority.keys().chain(candidate.keys()) {
        pks.insert(pk, ());
    }

    for pk in pks.keys() {
        match (authority.get(*pk), candidate.get(*pk)) {
            (Some(expected), Some(got)) => {
                let mut columns: BTreeMap<&String, ()> = BTreeMap::new();
                for column in expected.keys().chain(got.keys()) {
                    columns.insert(column, ());
                }
                for column in columns.keys() {
                    match (expected.get(*column), got.get(*column)) {
                        (Some(a), Some(b)) => {
                            if !comparison_for(column).equal(a, b) {
                                divergences.push(Divergence::Cell {
                                    table: table.to_string(),
                                    pk: (*pk).clone(),
                                    column: (*column).clone(),
                                    expected: a.clone(),
                                    got: b.clone(),
                                });
                            }
                        }
                        (Some(_), None) => divergences.push(Divergence::MissingColumn {
                            table: table.to_string(),
                            pk: (*pk).clone(),
                            column: (*column).clone(),
                        }),
                        (None, Some(_)) => divergences.push(Divergence::ExtraColumn {
                            table: table.to_string(),
                            pk: (*pk).clone(),
                            column: (*column).clone(),
                        }),
                        (None, None) => unreachable!("column came from the union of both sides"),
                    }
                }
            }
            (Some(_), None) => divergences.push(Divergence::MissingRow {
                table: table.to_string(),
                pk: (*pk).clone(),
            }),
            (None, Some(_)) => divergences.push(Divergence::ExtraRow {
                table: table.to_string(),
                pk: (*pk).clone(),
            }),
            (None, None) => unreachable!("pk came from the union of both sides"),
        }
    }

    divergences
}

/// Compares `def`'s persisted `target` (field-only, see [`target_fields`])
/// with the SQL oracle `sql`, producing an [`OracleReport`].
///
/// Field types drive the per-cell comparison: numeric fields (`total =
/// c1 + c2`) compare by decimal value, `Text`/`Boolean`/`Uuid` passthrough
/// fields (task B1) compare byte-exact. This resolves each field's type
/// from `def` and `source_columns` rather than assuming it, so it stays
/// correct as the grammar widens.
pub fn compare(
    program: &Program,
    def: &TransformDef,
    source_columns: &HashMap<String, ValueType>,
    target: &Rows,
    sql: &Rows,
) -> OracleReport {
    let field_types: HashMap<&str, ValueType> = def
        .fields
        .iter()
        .map(|f| (f.name.as_str(), field_value_type(&f.expr, source_columns)))
        .collect();
    let comparison_for = |column: &str| {
        field_types
            .get(column)
            .map(|vt| Comparison::for_type(*vt))
            .unwrap_or(Comparison::Exact)
    };

    OracleReport {
        target_vs_sql: diff(&def.target, sql, target, &comparison_for),
        program: format!("{program:#?}"),
    }
}

/// The [`ValueType`] a field expression produces, given `source_columns` (the
/// source table's own column types — the same map `check` threads through). Panics on any shape [`render_expr`] also
/// refuses, keeping the two in lockstep so the oracle never infers a type for
/// a shape it can't render.
///
/// Improvement-plan task B1 widened the generator to draw bare
/// `Expr::Column` passthrough fields over `Text`/`Boolean`/`Uuid` source
/// columns (`SELECT <col> AS <col>`), not just the numeric operands of
/// `total = c1 + c2` — so a `Column` reference is no longer always Numeric;
/// this now looks its actual type up on the source schema.
///
/// Task B2 widens it again: `BinaryOp`/`FunctionCall`'s return type is no
/// longer hardcoded `Numeric` either — it's read off
/// `trellis::dev::defs::registry::operator_spec`/`lookup_function`, the same
/// source of truth the parser and validator already share
/// (ADR-0004), rather than this oracle keeping its own separate copy of
/// "which operator/function returns which type" that could silently drift
/// from the registry's. `COALESCE` is the one function whose return type
/// isn't a fixed registry entry (it types as its *arguments'* common type,
/// exactly as `validate.rs`'s `infer_expr` computes it) — self-recursing into
/// the first argument mirrors that.
///
/// Task B4 widens it once more: a `FunctionCall` that isn't registered in
/// `registry::FUNCTIONS` (the scalar functions) is also checked against
/// `registry::lookup_aggregate_function` (`SUM`/`COUNT`/`AVG`/`MIN`/`MAX`,
/// `registry::AGGREGATE_FUNCTION_SPECS`) before giving up — both registries'
/// entries happen to return `Numeric` today, but this looks each one up
/// rather than hardcoding that, for the same "don't keep a second, driftable
/// copy of the registry's answer" reason as the scalar-function case above.
fn field_value_type(expr: &Expr, source_columns: &HashMap<String, ValueType>) -> ValueType {
    match expr {
        Expr::Column(name) | Expr::SourceColumn { column: name, .. } => {
            *source_columns.get(name).unwrap_or_else(|| {
                panic!(
                    "oracle: field expression references source column {name:?} that isn't in \
                 source_columns — a generator/caller bug (source_columns must list every column \
                 on the def's actual source table)"
                )
            })
        }
        // Postgres's own literal-typing rule — a bare integral
        // literal is `integer` if it fits, else `bigint`; anything else is
        // `numeric`.
        Expr::NumberLiteral(text) => match text.contains('.') {
            false => match text.parse::<i64>() {
                Ok(value) => ValueType::Integer(IntWidth::narrowest_for(value)),
                Err(_) => ValueType::Numeric,
            },
            true => ValueType::Numeric,
        },
        Expr::StringLiteral(_) => ValueType::Text,
        Expr::TypedLiteral { value_type, .. } => *value_type,
        // `+`'s result type depends on its operands (`int4 +
        // int4` is `integer`, `int4 + numeric` is `numeric`), so it has to
        // be resolved rather than read off a constant.
        Expr::BinaryOp { op, lhs, rhs } => {
            let lhs_t = field_value_type(lhs, source_columns);
            let rhs_t = field_value_type(rhs, source_columns);
            // A float against a bare number literal is the one mixed
            // float/exact comparison define admits: Postgres types the
            // literal as the float.
            let literal_beside_float = registry::is_mixed_float_exact(lhs_t, rhs_t)
                && matches!(
                    (&**lhs, &**rhs),
                    (Expr::NumberLiteral(_), _) | (_, Expr::NumberLiteral(_))
                );
            if literal_beside_float
                && registry::operator_result_type(
                    *op,
                    ValueType::Float(FloatWidth::Float8),
                    ValueType::Float(FloatWidth::Float8),
                ) == Some(ValueType::Boolean)
            {
                return ValueType::Boolean;
            }
            registry::operator_result_type(*op, lhs_t, rhs_t).unwrap_or_else(|| {
                panic!(
                    "oracle: operator {op:?} has no result type for operands \
                     {lhs_t}/{rhs_t} — the generator only ever builds \
                     well-typed expressions"
                )
            })
        }
        Expr::FunctionCall { name, args } if name == "COALESCE" => {
            field_value_type(&args[0], source_columns)
        }
        // Likewise for the aggregates — `sum(int4)` is `bigint`,
        // `sum(numeric)` is `numeric`. `COUNT` is excluded on purpose:
        // `registry::aggregate_result_type` deliberately has no row for it
        // (both `COUNT(*)` and `COUNT(<expr>)` are type-agnostic — Postgres
        // accepts any argument type and always returns `bigint`), so its
        // constant `FunctionSpec` return type (below, `Integer(Int8)`) is the
        // right answer regardless of which shape reaches here. Its
        // one-argument form (`COUNT(<col>)`) is why this needs naming rather
        // than falling out of the `args.is_empty()` guard.
        Expr::FunctionCall { name, args }
            if name != "COUNT"
                && registry::lookup_aggregate_function(name).is_some()
                && !args.is_empty() =>
        {
            let arg_t = field_value_type(&args[0], source_columns);
            registry::aggregate_result_type(name, arg_t).unwrap_or_else(|| {
                panic!(
                    "oracle: aggregate {name:?} has no result type for argument \
                     {arg_t} — the generator only ever builds well-typed \
                     expressions"
                )
            })
        }
        Expr::FunctionCall { name, .. } => registry::lookup_function(name)
            .or_else(|| registry::lookup_aggregate_function(name))
            .map(|spec| spec.return_type)
            .unwrap_or_else(|| {
                panic!(
                    "oracle: unknown function {name:?} — the generator only ever builds calls \
                     registered in trellis::dev::defs::registry::FUNCTIONS or \
                     AGGREGATE_FUNCTION_SPECS; extend field_value_type (and render_expr) if \
                     that set ever widens"
                )
            }),
    }
}

/// The type of each of `def`'s fields, in field order: the types of the
/// columns of the target table the engine builds for it. A
/// second instance that reads that target as a source needs them to model it
/// as a [`Table`].
///
/// `source_columns` lists every column of the definition's source table.
pub fn field_types(
    def: &TransformDef,
    source_columns: &HashMap<String, ValueType>,
) -> Vec<(String, ValueType)> {
    def.fields
        .iter()
        .map(|field| {
            (
                field.name.clone(),
                field_value_type(&field.expr, source_columns),
            )
        })
        .collect()
}

/// The SQL oracle of a `JOIN` definition: the definition's own
/// `select … from a [left] join b on … group by …`, each column as text,
/// group columns first in `GROUP BY` order, then every other field.
///
/// # Panics
///
/// If `def` isn't a `JOIN`.
pub fn render_join_select(def: &TransformDef) -> String {
    let KeySpace::Join(join) = &def.key_space else {
        panic!("render_join_select: {:?} is not a JOIN", def.target);
    };
    let column =
        |table: &str, column: &str| format!("{}.{}", quote_ident(table), quote_ident(column));
    let mut select: Vec<String> = join
        .group_by
        .iter()
        .map(|key| format!("{}::text", column(&key.table, &key.column)))
        .collect();
    for field in &def.fields {
        if join.group_by.iter().any(|key| key.column == field.name) {
            continue;
        }
        select.push(format!(
            "({})::text as {}",
            render_join_expr(&field.expr),
            quote_ident(&field.name)
        ));
    }
    let on: Vec<String> = join
        .on
        .iter()
        .map(|on| {
            format!(
                "{} = {}",
                column(&on.lhs.table, &on.lhs.column),
                column(&on.rhs.table, &on.rhs.column)
            )
        })
        .collect();
    let group_by: Vec<String> = join
        .group_by
        .iter()
        .map(|key| column(&key.table, &key.column))
        .collect();
    format!(
        "select {} from {} {} {} on {} group by {}",
        select.join(", "),
        quote_ident(&def.source),
        match join.kind {
            trellis::dev::defs::ast::JoinKind::Inner => "join",
            trellis::dev::defs::ast::JoinKind::Left => "left join",
        },
        quote_ident(&join.right),
        on.join(" and "),
        group_by.join(", ")
    )
}

/// One `JOIN` field expression as SQL: every column is read through the
/// table it names (`a.x`, or a joined `b.y`).
fn render_join_expr(expr: &Expr) -> String {
    match expr {
        Expr::SourceColumn { table, column, .. } => {
            format!("{}.{}", quote_ident(table), quote_ident(column))
        }
        Expr::BinaryOp { op, lhs, rhs } => format!(
            "({} {} {})",
            render_join_expr(lhs),
            match op {
                Operator::Add => "+",
                Operator::GreaterThan => ">",
            },
            render_join_expr(rhs)
        ),
        Expr::FunctionCall { name, args } if name == "COUNT" && args.is_empty() => {
            "count(*)".to_string()
        }
        Expr::FunctionCall { name, args } => format!(
            "{}({})",
            name.to_lowercase(),
            args.iter()
                .map(render_join_expr)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        other => trellis::dev::defs::oracle::render_expr_sql(other),
    }
}

/// Runs [`render_join_select`] and keys its rows the way
/// [`crate::backend::Backend::snapshot`] keys a `JOIN` target's: by the
/// group columns' [`group_key`].
async fn join_sql_oracle(pool: &Pool, def: &TransformDef) -> Result<Rows, OracleError> {
    let KeySpace::Join(join) = &def.key_space else {
        unreachable!("join_sql_oracle is only called on a JOIN");
    };
    let client = pool.get().await?;
    let db_rows = client.query(render_join_select(def).as_str(), &[]).await?;
    let fields: Vec<&str> = def
        .fields
        .iter()
        .map(|f| f.name.as_str())
        .filter(|name| !join.group_by.iter().any(|key| key.column == *name))
        .collect();
    let groups = join.group_by.len();
    let mut rows: Rows = BTreeMap::new();
    for db_row in db_rows {
        let values: Vec<Option<String>> = (0..groups).map(|i| db_row.get(i)).collect();
        let by_column = fields
            .iter()
            .enumerate()
            .map(|(i, name)| ((*name).to_string(), db_row.get(groups + i)))
            .collect();
        rows.insert(group_key(&values), by_column);
    }
    Ok(rows)
}

/// Runs the comparison for `def`: reads the SQL oracle from `pool`, strips
/// the pk column from the caller-supplied persisted `target` rows (as read back
/// by [`crate::backend::Backend::snapshot`]), and diffs them.
pub async fn check(
    pool: &Pool,
    program: &Program,
    def: &TransformDef,
    pk_column: &str,
    source_columns: &HashMap<String, ValueType>,
    target: &Rows,
) -> Result<OracleReport, OracleError> {
    // A `JOIN`'s oracle is its own `JOIN … GROUP BY` query over the
    // two tables, compared byte for byte.
    if matches!(def.key_space, KeySpace::Join(_)) {
        let sql = join_sql_oracle(pool, def).await?;
        return Ok(OracleReport {
            target_vs_sql: diff(&def.target, &sql, target, &|_| Comparison::Exact),
            program: format!("{program:#?}"),
        });
    }
    let sql = sql_oracle(pool, def, pk_column).await?;
    let target = target_fields(target, pk_column);
    Ok(compare(program, def, source_columns, &target, &sql))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The SQL oracle reads a qualified name inside an aggregate
    /// as the source column it names.
    #[test]
    fn render_expr_reads_a_qualified_source_column_as_the_column() {
        let expr = Expr::FunctionCall {
            name: "SUM".to_string(),
            args: vec![Expr::SourceColumn {
                schema: Some("app".to_string()),
                table: "t0".to_string(),
                column: "c1".to_string(),
            }],
        };
        assert_eq!(render_expr(&expr), r#"sum("c1")"#);
    }

    // A nested slice of literals is the most legible way to spell a fixture
    // inline; the complexity lint isn't worth a wrapper type for a test.
    #[allow(clippy::type_complexity)]
    fn rows(entries: &[(&str, &[(&str, Option<&str>)])]) -> Rows {
        entries
            .iter()
            .map(|(pk, cols)| {
                let by_column = cols
                    .iter()
                    .map(|(name, value)| (name.to_string(), value.map(|v| v.to_string())))
                    .collect();
                (pk.to_string(), by_column)
            })
            .collect()
    }

    fn numeric_kind(_: &str) -> Comparison {
        Comparison::DecimalByValue
    }

    #[test]
    fn decimals_compare_equal_ignoring_scale() {
        assert!(Comparison::DecimalByValue.equal(&Some("2.50".into()), &Some("2.5".into())));
        assert!(Comparison::DecimalByValue.equal(&Some("2.5".into()), &Some("2.50".into())));
        assert!(!Comparison::DecimalByValue.equal(&Some("2.5".into()), &Some("2.6".into())));
        assert!(Comparison::DecimalByValue.equal(&None, &None));
        assert!(!Comparison::DecimalByValue.equal(&Some("2.5".into()), &None));
    }

    /// The decimal comparison normalizes scale, leading zeros and the sign of
    /// zero, and nothing else: a different value, a different sign or a
    /// non-decimal spelling (`NaN`, `Infinity`) still compares by its text.
    #[test]
    fn decimal_text_normalizes_only_spellings_of_one_value() {
        let same = |a: &str, b: &str| {
            Comparison::DecimalByValue.equal(&Some(a.to_string()), &Some(b.to_string()))
        };
        for (a, b) in [
            ("2.50", "2.5"),
            ("10", "10.000"),
            ("0", "0.00"),
            ("-0", "0"),
            ("-0.000", "0.0"),
            ("0.5", ".50"),
            ("007", "7"),
            ("-1.10", "-1.1"),
            (
                "123456789012345678901234567890.10",
                "123456789012345678901234567890.1",
            ),
            ("NaN", "NaN"),
            ("Infinity", "Infinity"),
            ("-Infinity", "-Infinity"),
        ] {
            assert!(same(a, b), "{a} and {b} are one value");
        }
        for (a, b) in [
            ("1.5", "-1.5"),
            ("10", "1"),
            ("100", "1.00"),
            ("0.01", "0.1"),
            ("1", "1.0000000000000000000001"),
            ("NaN", "0"),
            ("Infinity", "-Infinity"),
            ("1e2", "100"),
        ] {
            assert!(!same(a, b), "{a} and {b} differ");
        }
    }

    /// Floats compare by Postgres's equality first (`NaN = NaN`, `-0 = 0`),
    /// then within the tolerance, which never bridges a special value.
    #[test]
    fn floats_compare_by_postgres_equality_then_tolerance() {
        assert!(postgres_float_equal(f64::NAN, f64::NAN));
        assert!(postgres_float_equal(-0.0, 0.0));
        assert!(postgres_float_equal(f64::INFINITY, f64::INFINITY));
        assert!(!postgres_float_equal(f64::INFINITY, f64::NEG_INFINITY));
        assert!(!postgres_float_equal(f64::NAN, 1.0));

        assert!(floats_close(f64::NAN, f64::NAN));
        assert!(floats_close(-0.0, 0.0));
        assert!(floats_close(0.1 + 0.2, 0.3));
        assert!(floats_close(1e300, 1e300 * (1.0 + 1e-12)));
        assert!(!floats_close(1.0, 1.0 + 1e-6));
        assert!(!floats_close(f64::NAN, 1.0));
        assert!(!floats_close(f64::INFINITY, f64::MAX));
        assert!(!floats_close(f64::INFINITY, f64::NEG_INFINITY));

        let tolerant = |a: &str, b: &str| {
            Comparison::FloatTolerance.equal(&Some(a.to_string()), &Some(b.to_string()))
        };
        assert!(tolerant("NaN", "NaN"));
        assert!(tolerant("-0", "0"));
        assert!(tolerant("Infinity", "Infinity"));
        assert!(!tolerant("Infinity", "-Infinity"));
        assert!(!tolerant("1", "2"));
    }

    #[test]
    fn exact_comparison_distinguishes_text_scale() {
        // The exact path must NOT treat "1.50" and "1.500" as equal — only
        // the decimal path does. This is why comparison is per-type.
        assert!(!Comparison::Exact.equal(&Some("1.50".into()), &Some("1.500".into())));
        assert!(Comparison::Exact.equal(&Some("hi".into()), &Some("hi".into())));
    }

    #[test]
    fn a_present_absent_cell_is_a_divergence_not_a_skip() {
        let authority = rows(&[("1", &[("total", Some("5"))])]);
        let candidate = rows(&[("1", &[])]);
        let divergences = diff("t", &authority, &candidate, &numeric_kind);
        assert_eq!(
            divergences,
            vec![Divergence::MissingColumn {
                table: "t".into(),
                pk: "1".into(),
                column: "total".into(),
            }]
        );
    }

    #[test]
    fn a_missing_row_is_a_divergence() {
        let authority = rows(&[("1", &[("total", Some("5"))])]);
        let candidate = rows(&[]);
        let divergences = diff("t", &authority, &candidate, &numeric_kind);
        assert_eq!(
            divergences,
            vec![Divergence::MissingRow {
                table: "t".into(),
                pk: "1".into(),
            }]
        );
    }

    #[test]
    fn a_decimal_cell_matches_across_scale_but_a_wrong_value_diverges() {
        let authority = rows(&[("1", &[("total", Some("2.50"))])]);
        let equal = rows(&[("1", &[("total", Some("2.5"))])]);
        assert!(diff("t", &authority, &equal, &numeric_kind).is_empty());

        let wrong = rows(&[("1", &[("total", Some("2.6"))])]);
        assert_eq!(
            diff("t", &authority, &wrong, &numeric_kind),
            vec![Divergence::Cell {
                table: "t".into(),
                pk: "1".into(),
                column: "total".into(),
                expected: Some("2.50".into()),
                got: Some("2.6".into()),
            }]
        );
    }

    #[test]
    fn report_display_includes_the_program_and_the_diverging_comparison() {
        let program = Program {
            tables: Vec::new(),
            defs: Vec::new(),
            def_install_after_op: Vec::new(),
            ops: Vec::new(),
            restart_after_ops: Vec::new(),
            scale_out_after_ops: Vec::new(),
        };
        let report = OracleReport {
            target_vs_sql: vec![Divergence::Cell {
                table: "d0".into(),
                pk: "1".into(),
                column: "total".into(),
                expected: Some("5".into()),
                got: Some("4".into()),
            }],
            program: format!("{program:#?}"),
        };
        let printed = report.to_string();
        assert!(printed.contains("target != SQL oracle"));
        assert!(printed.contains("d0[1].total: expected=5 got=4"));
        assert!(printed.contains("program:"));
        assert!(printed.contains("Program"));
    }

    #[test]
    fn render_select_renders_a_one_to_one_numeric_add() {
        let def = TransformDef {
            target: "d0".into(),
            source: "t0".into(),
            key_space: KeySpace::OneToOne,
            fields: vec![trellis::dev::defs::ast::FieldDef {
                name: "total".into(),
                expr: Expr::BinaryOp {
                    op: Operator::Add,
                    lhs: Box::new(Expr::Column("c1".into())),
                    rhs: Box::new(Expr::Column("c2".into())),
                },
            }],
            predicate: Predicate::True,
            without_source_index: false,
            explicit_source_schema: None,
            explicit_target_schema: None,
        };
        assert_eq!(
            render_select(&def, "c0"),
            "select \"c0\"::text, ((\"c1\" + \"c2\"))::text as \"total\" from \"t0\""
        );
    }

    /// Task B4: `render_select` now renders a real `GROUP BY` instead of
    /// panicking (the old pin here, `render_select_panics_on_an_aggregate_key_space`,
    /// asserted the pre-B4 refuse-to-guess behavior; this is its
    /// replacement now that the real thing works), over an `order_summary`
    /// model aggregate definition.
    #[test]
    fn render_select_renders_a_group_by_aggregate() {
        let def = TransformDef {
            target: "d0".into(),
            source: "t0".into(),
            key_space: KeySpace::Aggregate {
                group_by: vec!["grain".into()],
            },
            fields: vec![
                trellis::dev::defs::ast::FieldDef {
                    name: "grain".into(),
                    expr: Expr::Column("grain".into()),
                },
                trellis::dev::defs::ast::FieldDef {
                    name: "total".into(),
                    expr: Expr::FunctionCall {
                        name: "SUM".into(),
                        args: vec![Expr::Column("c1".into())],
                    },
                },
                trellis::dev::defs::ast::FieldDef {
                    name: "cnt".into(),
                    expr: Expr::FunctionCall {
                        name: "COUNT".into(),
                        args: Vec::new(),
                    },
                },
            ],
            predicate: Predicate::True,
            without_source_index: false,
            explicit_source_schema: None,
            explicit_target_schema: None,
        };
        assert_eq!(
            render_select(&def, "c0"),
            "select (\"grain\")::text as \"grain\", (sum(\"c1\"))::text as \"total\", \
             (count(*))::text as \"cnt\" from \"t0\" group by \"grain\""
        );
    }

    /// Task B4: `render_expr` now renders `MIN`/`MAX`/`AVG` too, not just
    /// `SUM`/`COUNT` (the two above already get end-to-end coverage via
    /// `render_select_renders_a_group_by_aggregate` and
    /// `field_value_type_classifies_every_aggregate_function_as_numeric`
    /// below) — a direct, narrow pin on the rendering itself, independent of
    /// a full `TransformDef`.
    #[test]
    fn render_expr_renders_min_max_avg_over_a_column() {
        for (name, expected) in [
            ("MIN", "min(\"c1\")"),
            ("MAX", "max(\"c1\")"),
            ("AVG", "avg(\"c1\")"),
        ] {
            let expr = Expr::FunctionCall {
                name: name.to_string(),
                args: vec![Expr::Column("c1".into())],
            };
            assert_eq!(render_expr(&expr), expected);
        }
    }

    /// Post-merge coherence note: the pre-merge B4 pin here
    /// (`render_expr_still_panics_on_a_non_aggregate_function_call`) asserted
    /// that `render_expr` panicked on `STRPOS` — true on the B4 branch in
    /// isolation, where B2's scalar-function rendering didn't exist yet, but
    /// false now that it's merged in (`render_expr`'s `FunctionCall` arm is
    /// fully generic, see its doc comment). `render_expr` itself no longer
    /// refuses *any* function name — the real "refuse to guess" boundary
    /// moved to [`field_value_type`], which panics on a function name absent
    /// from *both* `registry::FUNCTIONS` and `registry::AGGREGATE_FUNCTION_SPECS`
    /// (nothing in this grammar reaches `render_expr` with such a name
    /// without `field_value_type` refusing it first via `check`'s call
    /// sequence) — that's what this test pins instead.
    #[test]
    #[should_panic(expected = "unknown function \"NOT_A_REAL_FUNCTION\"")]
    fn field_value_type_panics_on_an_unregistered_function_name() {
        let expr = Expr::FunctionCall {
            name: "NOT_A_REAL_FUNCTION".into(),
            args: vec![Expr::Column("c1".into())],
        };
        let source_columns = HashMap::from([("c1".to_string(), ValueType::Numeric)]);
        let _ = field_value_type(&expr, &source_columns);
    }

    /// `field_value_type` (which drives per-cell comparison choice) must
    /// agree that every aggregate function a definition keeps (`SUM`,
    /// `COUNT`, `AVG`) produces the type Postgres itself would: `Numeric` for
    /// `SUM`/`AVG` over
    /// a `Numeric` column, and
    /// `Integer(Int8)` (`bigint`) for `COUNT`, matching
    /// `pg_typeof(count(*))`/`pg_typeof(count(x))`, which selects
    /// `Comparison::Exact` rather than `Comparison::DecimalByValue` for the
    /// field (`Comparison::for_type`'s own `ValueType::Integer(_) =>
    /// Comparison::Exact` arm).
    #[test]
    fn field_value_type_classifies_count_as_bigint_and_the_rest_as_numeric() {
        let source_columns = HashMap::from([("c1".to_string(), ValueType::Numeric)]);
        let count_expr = Expr::FunctionCall {
            name: "COUNT".into(),
            args: Vec::new(),
        };
        assert_eq!(
            field_value_type(&count_expr, &source_columns),
            ValueType::Integer(IntWidth::Int8)
        );
        for name in ["SUM", "AVG"] {
            let expr = Expr::FunctionCall {
                name: name.to_string(),
                args: vec![Expr::Column("c1".into())],
            };
            assert_eq!(
                field_value_type(&expr, &source_columns),
                ValueType::Numeric,
                "{name} must classify as Numeric"
            );
        }
    }

    /// A `JOIN`'s oracle is its own query: group columns first, read
    /// through their tables, then each other field, a joined-side column
    /// read through the joined table.
    #[test]
    fn render_join_select_is_the_definitions_own_query() {
        use crate::generate::join::{JoinDefSpec, JoinField, JoinGroups, JoinKey, join_definition};
        let def = join_definition(
            "j",
            &JoinDefSpec {
                left: true,
                key: JoinKey::Pair,
                groups: JoinGroups::Both,
                fields: vec![JoinField::SumY, JoinField::CoalesceSumY],
            },
        );
        assert_eq!(
            render_join_select(&def),
            r#"select "ja"."ga"::text, "jb"."gb"::text, (count(*))::text as "n", (sum("jb"."y"))::text as "sy", (coalesce(sum("jb"."y"), 0::integer))::text as "sy0" from "ja" left join "jb" on "ja"."t" = "jb"."t" and "ja"."k" = "jb"."k" group by "ja"."ga", "jb"."gb""#
        );
    }

    /// Improvement-plan task B2: `>` renders like `+` (parenthesized,
    /// operator between the two rendered operands) — the oracle no longer
    /// panics on `Operator::GreaterThan` the way it did before this task.
    #[test]
    fn render_expr_renders_greater_than() {
        let expr = Expr::BinaryOp {
            op: Operator::GreaterThan,
            lhs: Box::new(Expr::Column("c1".into())),
            rhs: Box::new(Expr::Column("c2".into())),
        };
        assert_eq!(render_expr(&expr), "(\"c1\" > \"c2\")");
    }

    /// A string literal renders as a single-quote-escaped, explicitly
    /// `::text`-cast Postgres literal — the same explicit-cast convention
    /// `trellis::dev::defs::oracle::render_expr_sql` independently uses for the
    /// same reason (an unadorned string constant leaves Postgres to infer a
    /// type, and an explicit cast removes that ambiguity).
    #[test]
    fn render_expr_renders_a_string_literal_with_an_escaped_quote() {
        let expr = Expr::StringLiteral("o'clock".into());
        assert_eq!(render_expr(&expr), "'o''clock'::text");
    }

    /// A function call renders as `<lowercased name>(<rendered args>)` —
    /// Postgres's own function-call syntax, matching every function name the
    /// grammar accepts case-insensitively either way.
    #[test]
    fn render_expr_renders_a_function_call() {
        let expr = Expr::FunctionCall {
            name: "STRPOS".into(),
            args: vec![
                Expr::Column("text_col".into()),
                Expr::StringLiteral("x".into()),
            ],
        };
        assert_eq!(render_expr(&expr), "strpos(\"text_col\", 'x'::text)");
    }

    /// A nested, mixed operator-and-function expression renders with every
    /// `BinaryOp` operand parenthesized and the function call nested inside —
    /// `STRPOS(text_col, 'x') > 0`, the exact shape
    /// `generate::DerivedShape::StrposGreaterThan` draws and
    /// `tests/convergence.rs`'s hand-built pin exercises end-to-end.
    #[test]
    fn render_expr_renders_a_nested_function_and_operator_expression() {
        let expr = Expr::BinaryOp {
            op: Operator::GreaterThan,
            lhs: Box::new(Expr::FunctionCall {
                name: "STRPOS".into(),
                args: vec![
                    Expr::Column("text_col".into()),
                    Expr::StringLiteral("x".into()),
                ],
            }),
            rhs: Box::new(Expr::NumberLiteral("0".into())),
        };
        assert_eq!(render_expr(&expr), "(strpos(\"text_col\", 'x'::text) > 0)");
    }

    /// [`field_value_type`] reads `>`'s return type off the shared registry
    /// (`trellis::dev::defs::registry::operator_spec`) rather than hardcoding it —
    /// this pins that it actually gets `Boolean`, not the `Numeric` every
    /// prior operator (`+`) happened to return.
    #[test]
    fn field_value_type_of_greater_than_is_boolean() {
        let expr = Expr::BinaryOp {
            op: Operator::GreaterThan,
            lhs: Box::new(Expr::Column("c1".into())),
            rhs: Box::new(Expr::Column("c2".into())),
        };
        let source_columns = HashMap::from([
            ("c1".to_string(), ValueType::Numeric),
            ("c2".to_string(), ValueType::Numeric),
        ]);
        assert_eq!(field_value_type(&expr, &source_columns), ValueType::Boolean);
    }

    /// A float against a bare number literal is a comparison define admits,
    /// so the oracle types it `boolean` instead of panicking on the
    /// float/exact mix it refuses between two columns.
    #[test]
    fn a_float_compared_with_a_bare_literal_is_boolean() {
        let source_columns = HashMap::from([
            ("f".to_string(), ValueType::Float(FloatWidth::Float4)),
            ("n".to_string(), ValueType::Numeric),
        ]);
        let compare = |lhs: Expr, rhs: Expr| Expr::BinaryOp {
            op: Operator::GreaterThan,
            lhs: Box::new(lhs),
            rhs: Box::new(rhs),
        };
        let literal = || Expr::NumberLiteral("0.5".to_string());
        let col = |name: &str| Expr::Column(name.to_string());
        for expr in [compare(col("f"), literal()), compare(literal(), col("f"))] {
            assert_eq!(field_value_type(&expr, &source_columns), ValueType::Boolean);
        }
    }

    /// Every one of the four scalar functions type-checks to its registered
    /// return type via [`field_value_type`] — `integer`, the return type
    /// `pg_proc.prorettype` actually reports — and
    /// `COALESCE` specifically types as its first argument's type rather
    /// than a fixed registry entry.
    #[test]
    fn field_value_type_of_each_scalar_function_matches_its_registry_return_type() {
        let source_columns = HashMap::from([("text_col".to_string(), ValueType::Text)]);
        for name in ["STRPOS", "OCTET_LENGTH", "CHAR_LENGTH", "REGEXP_COUNT"] {
            let args = match name {
                "STRPOS" | "REGEXP_COUNT" => vec![
                    Expr::Column("text_col".into()),
                    Expr::StringLiteral("x".into()),
                ],
                _ => vec![Expr::Column("text_col".into())],
            };
            let expr = Expr::FunctionCall {
                name: name.to_string(),
                args,
            };
            assert_eq!(
                field_value_type(&expr, &source_columns),
                ValueType::Integer(IntWidth::Int4),
                "{name} must type-check as integer"
            );
        }

        let coalesce = Expr::FunctionCall {
            name: "COALESCE".into(),
            args: vec![
                Expr::Column("text_col".into()),
                Expr::StringLiteral("x".into()),
            ],
        };
        assert_eq!(
            field_value_type(&coalesce, &source_columns),
            ValueType::Text
        );
    }
}
