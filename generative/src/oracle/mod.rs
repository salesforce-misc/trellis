//! The independent recompute oracle (design doc §2): renders a
//! [`trellis::dev::defs::ast::TransformDef`]'s formulas back to a `SELECT` against
//! the source and treats Postgres itself as the correctness authority. Uses
//! only the shared components design doc §2 names — the parser AST, a small
//! AST→`SELECT` printer written here (sharing no code with the engine's
//! evaluator), and [`trellis::dev::defs::oracle::recompute`] (evaluation, a
//! *secondary* parity check, not the pipeline-driving maintenance code that
//! is [`crate::backend`]'s job alone).
//!
//! # The three-way comparison
//!
//! Per op we compare three renderings of the same logical target:
//!
//! - **persisted target** — what [`crate::backend::Backend::snapshot`] read
//!   back from the maintained target table;
//! - **evaluator** — [`trellis::dev::defs::oracle::recompute`], the engine's own
//!   evaluator run from scratch;
//! - **SQL oracle** — the definition rendered back to a `SELECT` and run by
//!   Postgres itself.
//!
//! The SQL oracle is the authority (ADR-0004: the grammar is an immutable
//! subset of Postgres, so a rendered `SELECT` can never be outgrown by the
//! language). Comparing the other two *against it* localizes a divergence:
//! `target ≠ SQL` is a pipeline / apply / fold / ordering bug;
//! `evaluator ≠ SQL` is an eval-layer drift bug — the direct test of the
//! "engine mirrors Postgres" claim.
//!
//! # DB access
//!
//! Both oracles take an [`trellis::Pool`] — a connection handle — exactly as
//! [`trellis::dev::defs::oracle::recompute`] already does. The oracle *reads*; it
//! never imports `trellis::client`/`trellis::staging` or otherwise drives the
//! maintenance pipeline (that seam is [`crate::backend`]'s alone). A
//! pool connection has its `search_path` pinned, so an unqualified source
//! table name resolves the same way `recompute` relies on.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;

use trellis::Pool;
use trellis::dev::defs::ast::{Expr, KeySpace, Operator, Predicate, TransformDef, ValueType};
use trellis::dev::defs::oracle::{OracleError, recompute, recompute_aggregate};
use trellis::dev::defs::registry;
use trellis::numeric::Numeric;

use crate::model::{Cardinality, Program, Relationship, Table, group_key};

/// One logical target's rows: `pk (rendered text) -> column -> value
/// (rendered text, `None` is SQL `NULL`)`. Matches the inner shape of
/// [`crate::backend::Snapshot`], and always excludes the primary-key column
/// itself (the pk is the row key, so carrying it as a column too would show
/// up as a spurious divergence against the evaluator recompute, which keys by
/// pk but never emits it as a field).
pub type Rows = BTreeMap<String, BTreeMap<String, Option<String>>>;

/// How two rendered-text cell values are judged equal (design doc §5).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Comparison {
    /// Byte-exact text equality — int, text, date, timestamp, uuid, boolean.
    Exact,
    /// Decimal by value, ignoring scale (`2.50 ≡ 2.5`), via
    /// [`trellis::numeric::Numeric`] rather than hand-rolled parsing.
    DecimalByValue,
    /// Combined absolute+relative tolerance for floats. **Unreachable
    /// today**: [`ValueType`] has no float variant, so no column or
    /// derivation can ever produce one, so this is never constructed. Left
    /// as an explicit stub (rather than silently omitted) so the day the
    /// value language gains floats, the missing tolerance logic is a loud
    /// `unimplemented!`, not a wrong exact-equality comparison.
    #[allow(dead_code)]
    FloatTolerance,
}

impl Comparison {
    /// The comparison a column of `value_type` uses.
    fn for_type(value_type: ValueType) -> Comparison {
        match value_type {
            ValueType::Numeric => Comparison::DecimalByValue,
            // Issue #108: `Other` is passthrough-only (no arithmetic, no
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
                (Some(a), Some(b)) => match (Numeric::parse(a), Numeric::parse(b)) {
                    (Ok(a), Ok(b)) => a.compare(&b) == Ordering::Equal,
                    // A value that doesn't parse as a decimal can't be
                    // compared by value; fall back to exact so a genuine
                    // difference still surfaces rather than being swallowed.
                    _ => a == b,
                },
                _ => false,
            },
            Comparison::FloatTolerance => {
                unimplemented!(
                    "float tolerance comparison: the value language has no float type yet \
                     (ValueType has no Float variant), so this is unreachable; implement a \
                     combined absolute+relative tolerance here when floats are added"
                )
            }
        }
    }
}

/// One differing cell, row, or column found while comparing a candidate
/// rendering against the SQL-oracle authority. `expected` is always the SQL
/// oracle's value; `got` is the candidate's (target or evaluator).
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

/// The result of a three-way comparison for one definition's target.
///
/// Splits the findings by *which* candidate diverged from the SQL oracle, so
/// a caller learns which layer is wrong, not merely that something is
/// (design doc §2). Prints the whole [`Program`] on any divergence — a diff
/// without the program is unactionable (design doc §5).
#[derive(Debug, Clone)]
pub struct ThreeWayReport {
    /// `persisted target ≠ SQL oracle` — a pipeline/apply/fold/ordering bug.
    pub target_vs_sql: Vec<Divergence>,
    /// `evaluator ≠ SQL oracle` — an eval-layer drift bug (the ADR-0004
    /// "engine mirrors Postgres" claim, tested directly).
    pub evaluator_vs_sql: Vec<Divergence>,
    /// Whether the evaluator leg ran at all (issue #34). `false` for a
    /// relationship-reading definition, where the engine's evaluator-driven
    /// recompute has no relationship support to compare against — see
    /// [`check`] for the full reasoning. An empty `evaluator_vs_sql` means
    /// "agreed" when this is `true` and "never asked" when it is `false`,
    /// and the two must never be confused for each other.
    pub evaluator_checked: bool,
    program: String,
}

impl ThreeWayReport {
    /// Whether any comparison diverged.
    pub fn diverged(&self) -> bool {
        !self.target_vs_sql.is_empty() || !self.evaluator_vs_sql.is_empty()
    }
}

impl fmt::Display for ThreeWayReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if !self.diverged() {
            return if self.evaluator_checked {
                write!(f, "no divergence")
            } else {
                write!(
                    f,
                    "no divergence (evaluator leg not run: relationship-reading definition)"
                )
            };
        }
        if !self.target_vs_sql.is_empty() {
            writeln!(
                f,
                "target != SQL oracle (localizes a pipeline/apply/fold/ordering bug):"
            )?;
            for divergence in &self.target_vs_sql {
                writeln!(f, "  {divergence}")?;
            }
        }
        if !self.evaluator_vs_sql.is_empty() {
            writeln!(
                f,
                "evaluator != SQL oracle (localizes an eval-layer drift bug):"
            )?;
            for divergence in &self.evaluator_vs_sql {
                writeln!(f, "  {divergence}")?;
            }
        }
        if !self.evaluator_checked {
            writeln!(
                f,
                "(evaluator leg not run: this definition reads a relationship, which the \
                 engine's evaluator-driven recompute does not support — see oracle::check)"
            )?;
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
/// Deliberately independent of the engine's evaluator *and* of
/// `trellis::dev::defs::oracle::render_expr_sql` — the oracle's whole value is that
/// its `SELECT` shares no code with the thing it checks (both happen to
/// choose the same idiomatic rendering — quoted identifiers, an explicit cast
/// on every literal, `name(args)` for a function call — independently, not by
/// sharing an implementation). Improvement-plan task B2 widened this past the
/// original numeric-`+` slice (issue #5) to `Operator::GreaterThan`,
/// `Expr::StringLiteral`, and the five scalar functions
/// (`STRPOS`/`OCTET_LENGTH`/`CHAR_LENGTH`/`REGEXP_COUNT`/`COALESCE`) the
/// generator now draws (see `crate::generate::DerivedShape`); task B4 widens
/// it again to the five `KeySpace::Aggregate` functions
/// (`SUM`/`COUNT`/`AVG`/`MIN`/`MAX`, see `crate::generate::AggregateFn`).
/// Every one of those renders as the same generic `name(args)` Postgres call
/// syntax (the fallthrough `FunctionCall` arm below) except `COUNT`'s
/// argument-less `COUNT(*)`, which the grammar's AST carries with an empty
/// `args` list — special-cased so it doesn't fall through to the generic arm
/// and emit the invalid `count()`. Anything still beyond that (a relationship
/// path) panics naming the missing work rather than guessing (design doc §2
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
/// `MAX` are likewise all numeric aggregation with no text/collation
/// involvement. So unlike task B1's text *ordering* concern (which that
/// task's own plan flags as the load-bearing collation risk), nothing B2 or
/// B4 adds needs the oracle and the engine's underlying Postgres session
/// pinned to the same collation — there is no ordering comparison in this
/// grammar for the two to disagree about.
fn render_expr(expr: &Expr) -> String {
    match expr {
        Expr::Column(name) => quote_ident(name),
        Expr::NumberLiteral(text) => text.clone(),
        Expr::StringLiteral(text) => format!("'{}'::text", text.replace('\'', "''")),
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
        Expr::RelationshipPath { rel, column } => panic!(
            "oracle: relationship-path rendering ('{rel}.{column}') is out of scope for this \
             generator (issue #25 is grammar + AST only; no generator support yet); extend \
             `render_expr` when the generator emits it"
        ),
    }
}

/// Every relationship a [`Program`] declares, indexed by name, alongside the
/// tables needed to type a to-side column (issue #34).
///
/// Built from the [`Program`]'s own plain data — this oracle never asks the
/// engine's catalog what a relationship resolves to, exactly as it never asks
/// the engine's renderer what a definition's SQL is (design doc §2: the
/// oracle's whole value is sharing no code with the thing it checks). If the
/// generator's recorded [`Cardinality`] and the engine's live `pg_catalog`
/// introspection ever disagreed, the resulting divergence is a real finding,
/// not a harness artifact.
struct RelIndex<'a> {
    by_name: HashMap<&'a str, &'a Relationship>,
    tables: &'a [Table],
}

impl<'a> RelIndex<'a> {
    fn new(program: &'a Program) -> Self {
        RelIndex {
            by_name: program
                .relationships
                .iter()
                .map(|r| (r.name.as_str(), r))
                .collect(),
            tables: &program.tables,
        }
    }

    /// The relationship `name` denotes. A referenced-but-undeclared
    /// relationship is a generator bug (the engine would have rejected the
    /// definition at validation time), so this panics rather than guessing —
    /// design doc §2's "refuse to guess on unmodeled shapes".
    fn get(&self, name: &str) -> &'a Relationship {
        self.by_name.get(name).copied().unwrap_or_else(|| {
            panic!(
                "oracle: calculated field references relationship {name:?}, which the program \
                 does not declare — a generator bug (the engine rejects an unknown relationship \
                 at validation time, so this program could never have installed)"
            )
        })
    }

    /// The [`ValueType`] of `column` on `rel`'s to-side table.
    fn to_column_type(&self, rel: &str, column: &str) -> ValueType {
        let relationship = self.get(rel);
        self.tables
            .iter()
            .find(|t| t.name == relationship.to_table)
            .and_then(|t| t.column_type(column))
            .unwrap_or_else(|| {
                panic!(
                    "oracle: relationship path '{rel}.{column}' names a column the to-side table \
                     {:?} does not declare — a generator bug",
                    relationship.to_table
                )
            })
    }
}

/// Whether any of `def`'s calculated fields reads a relationship path at all
/// (issue #34). Drives two decisions: which `SELECT` renderer to use, and
/// whether the evaluator leg of the three-way comparison can run at all (see
/// [`check`]).
fn uses_relationships(def: &TransformDef) -> bool {
    def.fields.iter().any(|f| expr_reads_relationship(&f.expr))
}

fn expr_reads_relationship(expr: &Expr) -> bool {
    match expr {
        Expr::RelationshipPath { .. } => true,
        Expr::Column(_) | Expr::NumberLiteral(_) | Expr::StringLiteral(_) => false,
        Expr::BinaryOp { lhs, rhs, .. } => {
            expr_reads_relationship(lhs) || expr_reads_relationship(rhs)
        }
        Expr::FunctionCall { args, .. } => args.iter().any(expr_reads_relationship),
    }
}

/// Collects every **to-one** relationship name `expr` reads, in sorted order
/// (issue #34) — each one needs its own `LEFT JOIN` in the rendered `SELECT`.
///
/// A *to-many* path contributes no join: it renders as a correlated
/// subquery, which brings its own `FROM`. Note this walks *into* an aggregate
/// call rather than stopping at one, because a to-one path wrapped in an
/// aggregate (`SUM(<rel>.<col>)` inside a `GROUP BY` definition) is still an
/// ordinary aggregate over the joined column and still needs the join —
/// cardinality, not syntactic position, is what decides.
fn collect_join_rels<'a>(expr: &'a Expr, rels: &RelIndex<'_>, out: &mut BTreeSet<&'a str>) {
    match expr {
        Expr::RelationshipPath { rel, .. } => {
            if rels.get(rel).cardinality == Cardinality::ToOne {
                out.insert(rel.as_str());
            }
        }
        Expr::Column(_) | Expr::NumberLiteral(_) | Expr::StringLiteral(_) => {}
        Expr::BinaryOp { lhs, rhs, .. } => {
            collect_join_rels(lhs, rels, out);
            collect_join_rels(rhs, rels, out);
        }
        Expr::FunctionCall { args, .. } => {
            for arg in args {
                collect_join_rels(arg, rels, out);
            }
        }
    }
}

/// Renders one calculated-field expression of a relationship-reading
/// definition (issue #34). Like [`render_expr`], but:
///
/// * every **source** column is qualified with the source table, so it can't
///   be ambiguous against a `LEFT JOIN`ed to-side column of the same name
///   (every generated table uses the same `c0`, `c1`, ... name pool, so this
///   is the common case, not a corner one);
/// * a **to-one** path reads off the join alias — the relationship's own
///   name, which can never collide with a table name (`r0` vs `t0`);
/// * a **to-many** path, always wrapped in exactly one aggregate, becomes a
///   correlated aggregate subquery over the to-side table filtered by the
///   join key.
///
/// The nullability contract falls out of plain Postgres semantics rather than
/// being special-cased, which is exactly why the oracle is a `SELECT`: an
/// unmatched or `NULL` foreign key leaves a `LEFT JOIN`'s to-side columns
/// `NULL` (so a to-one enrichment is `NULL` and the source row survives), and
/// an empty correlated set gives `count → 0` with `sum`/`min`/`max`/`avg →
/// NULL`. This oracle agrees with ADR-0006 because Postgres does, not because
/// it copied the engine's answer.
///
/// # Panics
///
/// On a bare to-many path or an aggregate wrapping something other than a
/// single path — shapes `trellis::dev::defs::validate` rejects, which the generator
/// therefore never emits. Refusing to guess (design doc §2) keeps a future
/// widening a loud failure rather than a silent false differential.
fn render_rel_expr(expr: &Expr, source: &str, rels: &RelIndex<'_>) -> String {
    match expr {
        Expr::Column(name) => format!("{}.{}", quote_ident(source), quote_ident(name)),
        Expr::NumberLiteral(text) => text.clone(),
        Expr::StringLiteral(text) => format!("'{}'::text", text.replace('\'', "''")),
        Expr::BinaryOp { op, lhs, rhs } => {
            let symbol = match op {
                Operator::Add => "+",
                Operator::GreaterThan => ">",
            };
            format!(
                "({} {symbol} {})",
                render_rel_expr(lhs, source, rels),
                render_rel_expr(rhs, source, rels)
            )
        }
        Expr::RelationshipPath { rel, column } => {
            let relationship = rels.get(rel);
            assert_eq!(
                relationship.cardinality,
                Cardinality::ToOne,
                "oracle: bare reference to to-many relationship path '{rel}.{column}' — \
                 ADR-0006 requires a to-many path to be wrapped in exactly one aggregate, and \
                 the generator never emits this; there is no correct value to guess here"
            );
            format!("{}.{}", quote_ident(rel), quote_ident(column))
        }
        Expr::FunctionCall { name, args } if name == "COUNT" && args.is_empty() => {
            "count(*)".to_string()
        }
        Expr::FunctionCall { name, args }
            if matches!(args.as_slice(), [Expr::RelationshipPath { .. }]) =>
        {
            let Expr::RelationshipPath { rel, column } = &args[0] else {
                unreachable!("guarded by the matches! above");
            };
            let relationship = rels.get(rel);
            match relationship.cardinality {
                // A to-one path under an aggregate is just an aggregate over
                // the joined column — the join is emitted by
                // `collect_join_rels`, so nothing extra is needed here.
                Cardinality::ToOne => {
                    format!(
                        "{}({}.{})",
                        name.to_lowercase(),
                        quote_ident(rel),
                        quote_ident(column)
                    )
                }
                Cardinality::ToMany => format!(
                    "(select {func}({alias}.{col}) from {to_table} as {alias} \
                     where {alias}.{to_col} = {source}.{from_col})",
                    func = name.to_lowercase(),
                    alias = quote_ident(rel),
                    col = quote_ident(column),
                    to_table = quote_ident(&relationship.to_table),
                    to_col = quote_ident(&relationship.to_col),
                    source = quote_ident(source),
                    from_col = quote_ident(&relationship.from_col),
                ),
            }
        }
        Expr::FunctionCall { name, args } => {
            let rendered_args: Vec<String> = args
                .iter()
                .map(|arg| render_rel_expr(arg, source, rels))
                .collect();
            format!("{}({})", name.to_lowercase(), rendered_args.join(", "))
        }
    }
}

/// The `left join <to_table> as <rel> on <rel>.<to_col> = <source>.<from_col>`
/// clauses a relationship-reading `SELECT` needs — one per to-one
/// relationship any field reads, deduplicated and in sorted order so the
/// rendered SQL is deterministic.
///
/// `LEFT`, not inner: a source row whose foreign key resolves to nothing must
/// still appear in the result (with `NULL` enrichment). An inner join would
/// silently *drop* that row, which is the single most consequential way this
/// oracle could be quietly wrong — it would agree with a buggy engine that
/// also dropped it.
fn render_joins(def: &TransformDef, rels: &RelIndex<'_>) -> String {
    let mut names: BTreeSet<&str> = BTreeSet::new();
    for field in &def.fields {
        collect_join_rels(&field.expr, rels, &mut names);
    }
    names
        .into_iter()
        .map(|name| {
            let rel = rels.get(name);
            format!(
                " left join {} as {alias} on {alias}.{} = {}.{}",
                quote_ident(&rel.to_table),
                quote_ident(&rel.to_col),
                quote_ident(&def.source),
                quote_ident(&rel.from_col),
                alias = quote_ident(name),
            )
        })
        .collect()
}

/// [`render_select`]'s relationship-reading counterpart (issue #34). Split
/// out rather than folded in so a relationship-free definition renders
/// byte-for-byte as it always did — the existing rendering pins stay honest,
/// and a program that declares no relationship pays nothing for this.
fn render_rel_select(def: &TransformDef, pk_column: &str, rels: &RelIndex<'_>) -> String {
    match def.predicate {
        Predicate::True => {}
    }
    let source = quote_ident(&def.source);
    let joins = render_joins(def, rels);

    match &def.key_space {
        KeySpace::OneToOne => {
            let mut select_list = vec![format!("{source}.{}::text", quote_ident(pk_column))];
            for field in &def.fields {
                select_list.push(format!(
                    "({})::text as {}",
                    render_rel_expr(&field.expr, &def.source, rels),
                    quote_ident(&field.name)
                ));
            }
            format!("select {} from {source}{joins}", select_list.join(", "))
        }
        KeySpace::Aggregate { group_by } => {
            let select_list: Vec<String> = def
                .fields
                .iter()
                .map(|field| {
                    format!(
                        "({})::text as {}",
                        render_rel_expr(&field.expr, &def.source, rels),
                        quote_ident(&field.name)
                    )
                })
                .collect();
            // The generative suite never constructs a relationship-path
            // `GROUP BY` key (issue #137 scopes that support out of this
            // crate), so every key is a plain column here.
            let group_cols: Vec<String> = group_by
                .iter()
                .map(|k| format!("{source}.{}", quote_ident(k.target_column_name())))
                .collect();
            format!(
                "select {} from {source}{joins} group by {}",
                select_list.join(", "),
                group_cols.join(", ")
            )
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
fn render_select(def: &TransformDef, pk_column: &str, rels: &RelIndex<'_>) -> String {
    // Issue #34: a definition that reads a relationship path renders through
    // the join-aware printer instead. Dispatching here (rather than teaching
    // one printer both jobs) keeps a relationship-free definition's SQL
    // byte-identical to what it has always been.
    if uses_relationships(def) {
        return render_rel_select(def, pk_column, rels);
    }

    match def.predicate {
        Predicate::True => {}
    }

    match &def.key_space {
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
            let group_cols: Vec<String> = group_by
                .iter()
                .map(|k| quote_ident(k.target_column_name()))
                .collect();
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
/// (`Aggregate`, task B4). The authority side of the three-way comparison.
///
/// For an `Aggregate` def, `pk_column` is unused: there is no single leading
/// pk expression in the rendered `SELECT` (see [`render_select`]'s doc
/// comment), so this instead locates each `group_by` column by name among
/// `def.fields` (every grouping column has a passthrough field of the same
/// name — enforced by `trellis::dev::defs::validate::validate`'s
/// `GroupingColumnFieldMustBePassthrough` check) and excludes it from the
/// row's own `by_column` map, exactly as [`target_fields`] excludes the 1-1
/// pk column.
pub async fn sql_oracle(
    pool: &Pool,
    program: &Program,
    def: &TransformDef,
    pk_column: &str,
) -> Result<Rows, OracleError> {
    let sql = render_select(def, pk_column, &RelIndex::new(program));
    let client = pool.get().await?;
    let db_rows = client.query(sql.as_str(), &[]).await?;

    match &def.key_space {
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
                        let column = key.target_column_name();
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
                    if group_by
                        .iter()
                        .any(|k| k.target_column_name() == field.name)
                    {
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

/// Recomputes `def`'s target with the engine's own evaluator
/// ([`trellis::dev::defs::oracle::recompute`] for 1-1,
/// [`trellis::dev::defs::oracle::recompute_aggregate`] for `Aggregate` — task B4)
/// and renders each [`Value`] to text, into field-only [`Rows`] keyed by the
/// primary key (1-1) or the grouping columns' composite [`group_key`]
/// (`Aggregate`, already computed by `recompute_aggregate` itself, since it's
/// the engine's own private convention this generative-suite key deliberately
/// mirrors — see [`group_key`]'s doc comment). The evaluator side of the
/// three-way comparison — a secondary parity check on the mirror-Postgres
/// claim, never the authority.
///
/// For an `Aggregate` def, `pk_column` is unused (there is no single source
/// primary key to key an aggregate target by), and each group's grouping
/// column(s) are excluded from `by_column`, matching [`sql_oracle`]'s
/// `Aggregate` arm.
///
/// [`Value`]: trellis::dev::defs::eval::Value
pub async fn evaluator_oracle(
    pool: &Pool,
    def: &TransformDef,
    pk_column: &str,
    source_columns: &HashMap<String, ValueType>,
) -> Result<Rows, OracleError> {
    match &def.key_space {
        KeySpace::OneToOne => {
            let recomputed = recompute(pool, def, pk_column, source_columns).await?;
            let mut rows: Rows = BTreeMap::new();
            for (pk, fields) in recomputed {
                let by_column = fields
                    .into_iter()
                    .map(|(column, value)| (column, value.map(|v| v.to_string())))
                    .collect();
                rows.insert(pk, by_column);
            }
            Ok(rows)
        }
        KeySpace::Aggregate { group_by } => {
            let recomputed = recompute_aggregate(pool, def, source_columns).await?;
            let mut rows: Rows = BTreeMap::new();
            for (key, fields) in recomputed {
                let by_column = fields
                    .into_iter()
                    .filter(|(column, _)| {
                        !group_by.iter().any(|k| k.target_column_name() == column)
                    })
                    .map(|(column, value)| (column, value.map(|v| v.to_string())))
                    .collect();
                rows.insert(key, by_column);
            }
            Ok(rows)
        }
    }
}

/// Strips the primary-key column from a target's rows as read back by
/// [`crate::backend::Backend::snapshot`], yielding the field-only [`Rows`] the
/// three-way comparison expects. The pk stays as each row's key.
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

/// Compares the three field-only renderings of `def`'s target against the SQL
/// oracle `sql`, producing a localized [`ThreeWayReport`].
///
/// Field types drive the per-cell comparison: numeric fields (`total =
/// c1 + c2`) compare by decimal value, `Text`/`Boolean`/`Uuid` passthrough
/// fields (task B1) compare byte-exact — this resolves each field's type
/// from `def` and `source_columns` rather than assuming it, so it stays
/// correct as the grammar widens.
pub fn three_way(
    program: &Program,
    def: &TransformDef,
    source_columns: &HashMap<String, ValueType>,
    target: &Rows,
    evaluator: &Rows,
    sql: &Rows,
) -> ThreeWayReport {
    let rels = RelIndex::new(program);
    let field_types: HashMap<&str, ValueType> = def
        .fields
        .iter()
        .map(|f| {
            (
                f.name.as_str(),
                field_value_type(&f.expr, source_columns, &rels),
            )
        })
        .collect();
    let comparison_for = |column: &str| {
        field_types
            .get(column)
            .map(|vt| Comparison::for_type(*vt))
            .unwrap_or(Comparison::Exact)
    };

    ThreeWayReport {
        target_vs_sql: diff(&def.target, sql, target, &comparison_for),
        evaluator_vs_sql: diff(&def.target, sql, evaluator, &comparison_for),
        evaluator_checked: true,
        program: format!("{program:#?}"),
    }
}

/// The [`ValueType`] a field expression produces, given `source_columns` (the
/// source table's own column types — the same map [`evaluator_oracle`] and
/// `check` already thread through). Panics on any shape [`render_expr`] also
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
/// source of truth the parser/validator/evaluator all already share
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
fn field_value_type(
    expr: &Expr,
    source_columns: &HashMap<String, ValueType>,
    rels: &RelIndex<'_>,
) -> ValueType {
    match expr {
        Expr::Column(name) => *source_columns.get(name).unwrap_or_else(|| {
            panic!(
                "oracle: field expression references source column {name:?} that isn't in \
                 source_columns — a generator/caller bug (source_columns must list every column \
                 on the def's actual source table)"
            )
        }),
        Expr::NumberLiteral(_) => ValueType::Numeric,
        Expr::StringLiteral(_) => ValueType::Text,
        Expr::BinaryOp { op, .. } => registry::operator_spec(*op).return_type,
        Expr::FunctionCall { name, args } if name == "COALESCE" => {
            field_value_type(&args[0], source_columns, rels)
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
        // Issue #34: a `<rel>.<column>` enrichment's type is the *to-side*
        // table's column type, not anything on this definition's own source
        // — resolved through the program's declared relationships rather
        // than assumed, so it stays right if the generator ever references a
        // non-Numeric to-side column.
        Expr::RelationshipPath { rel, column } => rels.to_column_type(rel, column),
    }
}

/// Runs the full three-way comparison for a 1-1 `def`: reads the SQL oracle
/// and the evaluator recompute from `pool`, strips the pk column from the
/// caller-supplied persisted `target` rows (as read back by
/// [`crate::backend::Backend::snapshot`]), and diffs both against the SQL
/// oracle.
#[allow(clippy::too_many_arguments)]
pub async fn check(
    pool: &Pool,
    program: &Program,
    def: &TransformDef,
    pk_column: &str,
    source_columns: &HashMap<String, ValueType>,
    target: &Rows,
) -> Result<ThreeWayReport, OracleError> {
    let sql = sql_oracle(pool, program, def, pk_column).await?;
    let target = target_fields(target, pk_column);

    // Issue #34: the evaluator leg does not exist for a relationship-reading
    // definition. `trellis::dev::defs::oracle::recompute`/`recompute_aggregate`
    // select only *source* columns and run each row through `evaluate`,
    // whose empty `RelationshipContext` makes any path an
    // `EvalError::UnsupportedRelationshipPath` — the engine documents this
    // limitation on its own `collect_columns` ("those two evaluator-driven
    // recomputes therefore only handle relationship-free definitions; the
    // SQL oracles are what cross-check a relationship-reading one").
    //
    // So the comparison narrows to two ways (persisted target vs. the
    // Postgres SQL oracle) for exactly these definitions. That is a real
    // reduction in evidence and is reported as such rather than hidden:
    // `ThreeWayReport::evaluator_checked` is `false`, and the report says so
    // when printed. The *authority* leg — the one that catches a pipeline /
    // fold / apply / ordering bug — is unaffected; what's lost is the
    // secondary ADR-0004 parity cross-check, which would need a
    // `recompute_with_relationships` entry point in `trellis/` to restore
    // (out of scope for this issue, which is generative-crate-only).
    if uses_relationships(def) {
        let mut report = three_way(program, def, source_columns, &target, &sql, &sql);
        report.evaluator_vs_sql.clear();
        report.evaluator_checked = false;
        return Ok(report);
    }

    let evaluator = evaluator_oracle(pool, def, pk_column, source_columns).await?;
    Ok(three_way(
        program,
        def,
        source_columns,
        &target,
        &evaluator,
        &sql,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Column;

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

    /// An empty [`Program`], for the rendering/typing tests below that
    /// exercise relationship-free definitions and so never consult the
    /// relationship index at all.
    fn empty_program() -> Program {
        Program {
            tables: Vec::new(),
            relationships: Vec::new(),
            defs: Vec::new(),
            def_install_after_op: Vec::new(),
            ops: Vec::new(),
            restart_after_ops: Vec::new(),
            scale_out_after_ops: Vec::new(),
        }
    }

    #[test]
    fn decimals_compare_equal_ignoring_scale() {
        assert!(Comparison::DecimalByValue.equal(&Some("2.50".into()), &Some("2.5".into())));
        assert!(Comparison::DecimalByValue.equal(&Some("2.5".into()), &Some("2.50".into())));
        assert!(!Comparison::DecimalByValue.equal(&Some("2.5".into()), &Some("2.6".into())));
        assert!(Comparison::DecimalByValue.equal(&None, &None));
        assert!(!Comparison::DecimalByValue.equal(&Some("2.5".into()), &None));
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
            relationships: Vec::new(),
            defs: Vec::new(),
            def_install_after_op: Vec::new(),
            ops: Vec::new(),
            restart_after_ops: Vec::new(),
            scale_out_after_ops: Vec::new(),
        };
        let report = ThreeWayReport {
            target_vs_sql: vec![Divergence::Cell {
                table: "d0".into(),
                pk: "1".into(),
                column: "total".into(),
                expected: Some("5".into()),
                got: Some("4".into()),
            }],
            evaluator_vs_sql: Vec::new(),
            evaluator_checked: true,
            program: format!("{program:#?}"),
        };
        let printed = report.to_string();
        assert!(printed.contains("target != SQL oracle"));
        assert!(!printed.contains("evaluator != SQL oracle"));
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
            explicit_source_schema: None,
            explicit_target_schema: None,
        };
        assert_eq!(
            render_select(&def, "c0", &RelIndex::new(&empty_program())),
            "select \"c0\"::text, ((\"c1\" + \"c2\"))::text as \"total\" from \"t0\""
        );
    }

    /// Task B4: `render_select` now renders a real `GROUP BY` instead of
    /// panicking (the old pin here, `render_select_panics_on_an_aggregate_key_space`,
    /// asserted the pre-B4 refuse-to-guess behavior; this is its
    /// replacement now that the real thing works). Mirrors the
    /// `order_summary` shape `trellis/tests/apply_aggregate.rs` uses as its
    /// own model aggregate definition.
    #[test]
    fn render_select_renders_a_group_by_aggregate() {
        let def = TransformDef {
            target: "d0".into(),
            source: "t0".into(),
            key_space: KeySpace::Aggregate {
                group_by: vec![trellis::dev::defs::ast::GroupByKey::Column("grain".into())],
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
            explicit_source_schema: None,
            explicit_target_schema: None,
        };
        assert_eq!(
            render_select(&def, "c0", &RelIndex::new(&empty_program())),
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
        let _ = field_value_type(&expr, &source_columns, &RelIndex::new(&empty_program()));
    }

    /// Task B4's invertibility-split coverage claim only means something if
    /// `field_value_type` (which drives per-cell comparison choice) actually
    /// agrees that every one of the five aggregate functions — both the
    /// `Invertible` ones (`SUM`/`COUNT`/`AVG`) and the `RecomputeOnly` ones
    /// (`MIN`/`MAX`, see `trellis::dev::defs::invertibility`) — produces a Numeric
    /// result, since that's what selects `Comparison::DecimalByValue` over
    /// `Comparison::Exact` for the field.
    #[test]
    fn field_value_type_classifies_every_aggregate_function_as_numeric() {
        let source_columns = HashMap::from([("c1".to_string(), ValueType::Numeric)]);
        let count_expr = Expr::FunctionCall {
            name: "COUNT".into(),
            args: Vec::new(),
        };
        assert_eq!(
            field_value_type(
                &count_expr,
                &source_columns,
                &RelIndex::new(&empty_program())
            ),
            ValueType::Numeric
        );
        for name in ["SUM", "AVG", "MIN", "MAX"] {
            let expr = Expr::FunctionCall {
                name: name.to_string(),
                args: vec![Expr::Column("c1".into())],
            };
            assert_eq!(
                field_value_type(&expr, &source_columns, &RelIndex::new(&empty_program())),
                ValueType::Numeric,
                "{name} must classify as Numeric"
            );
        }
    }

    // -----------------------------------------------------------------
    // Issue #34: relationship rendering pins.
    // -----------------------------------------------------------------

    /// A two-table program whose `t1` has a UNIQUE key column `k` and a
    /// plain FK column `f`, plus one relationship of `cardinality` joining
    /// `t0` to `t1`. The endpoints mirror `crate::generate`'s own choice:
    /// to-one joins `t0.f` to `t1.k`, to-many joins `t0.k` to `t1.f`.
    fn rel_program(cardinality: Cardinality) -> Program {
        let table = |name: &str| Table {
            name: name.to_string(),
            pk_col: "pk".to_string(),
            columns: vec![
                Column {
                    name: "pk".to_string(),
                    value_type: ValueType::Numeric,
                },
                Column {
                    name: "c1".to_string(),
                    value_type: ValueType::Numeric,
                },
                Column {
                    name: "k".to_string(),
                    value_type: ValueType::Text,
                },
                Column {
                    name: "f".to_string(),
                    value_type: ValueType::Text,
                },
            ],
            unique_cols: vec!["k".to_string()],
        };
        let (from_col, to_col) = match cardinality {
            Cardinality::ToOne => ("f", "k"),
            Cardinality::ToMany => ("k", "f"),
        };
        Program {
            tables: vec![table("t0"), table("t1")],
            relationships: vec![Relationship {
                name: "r0".to_string(),
                from_table: "t0".to_string(),
                from_col: from_col.to_string(),
                to_table: "t1".to_string(),
                to_col: to_col.to_string(),
                cardinality,
            }],
            defs: Vec::new(),
            def_install_after_op: Vec::new(),
            ops: Vec::new(),
            restart_after_ops: Vec::new(),
            scale_out_after_ops: Vec::new(),
        }
    }

    fn path() -> Expr {
        Expr::RelationshipPath {
            rel: "r0".to_string(),
            column: "c1".to_string(),
        }
    }

    /// A bare to-one enrichment renders as a `LEFT JOIN` aliased by the
    /// *relationship* name, with the source's own columns qualified so they
    /// can't be ambiguous against the joined table's identically-named ones.
    /// `LEFT`, not inner, is the load-bearing word: an inner join would drop
    /// a source row whose FK resolves to nothing, which is precisely the
    /// ADR-0006 rule this oracle exists to check.
    #[test]
    fn render_select_renders_a_to_one_enrichment_as_a_left_join() {
        let program = rel_program(Cardinality::ToOne);
        let def = TransformDef {
            target: "d0".into(),
            source: "t0".into(),
            key_space: KeySpace::OneToOne,
            fields: vec![trellis::dev::defs::ast::FieldDef {
                name: "rel_enrich".into(),
                expr: path(),
            }],
            predicate: Predicate::True,
            explicit_source_schema: None,
            explicit_target_schema: None,
        };
        assert_eq!(
            render_select(&def, "pk", &RelIndex::new(&program)),
            "select \"t0\".\"pk\"::text, (\"r0\".\"c1\")::text as \"rel_enrich\" \
             from \"t0\" left join \"t1\" as \"r0\" on \"r0\".\"k\" = \"t0\".\"f\""
        );
    }

    /// A to-many aggregate renders as a correlated subquery — no JOIN at
    /// all, since the related rows are folded rather than multiplied into
    /// the result. Postgres's empty-set semantics then give the ADR-0006
    /// answer for free (`COUNT` → `0`, the rest → `NULL`).
    #[test]
    fn render_select_renders_a_to_many_aggregate_as_a_correlated_subquery() {
        let program = rel_program(Cardinality::ToMany);
        let def = TransformDef {
            target: "d0".into(),
            source: "t0".into(),
            key_space: KeySpace::OneToOne,
            fields: vec![trellis::dev::defs::ast::FieldDef {
                name: "rel_agg".into(),
                expr: Expr::FunctionCall {
                    name: "SUM".into(),
                    args: vec![path()],
                },
            }],
            predicate: Predicate::True,
            explicit_source_schema: None,
            explicit_target_schema: None,
        };
        let sql = render_select(&def, "pk", &RelIndex::new(&program));
        assert_eq!(
            sql,
            "select \"t0\".\"pk\"::text, ((select sum(\"r0\".\"c1\") from \"t1\" as \"r0\" \
             where \"r0\".\"f\" = \"t0\".\"k\"))::text as \"rel_agg\" from \"t0\""
        );
        assert!(
            !sql.contains("join"),
            "a to-many path folds related rows in a subquery; joining would multiply the \
             source rows instead: {sql}"
        );
    }

    /// A to-one path *aggregated inside a `GROUP BY`* still needs its
    /// `LEFT JOIN` — cardinality decides whether a join is needed, not
    /// whether the path happens to sit under an aggregate call. Getting this
    /// backwards (stopping the join walk at any aggregate, as the to-many
    /// case requires) would silently render `sum(r0.c1)` against nothing.
    #[test]
    fn render_select_joins_a_to_one_path_even_when_it_is_aggregated() {
        let program = rel_program(Cardinality::ToOne);
        let def = TransformDef {
            target: "d0".into(),
            source: "t0".into(),
            key_space: KeySpace::Aggregate {
                group_by: vec![trellis::dev::defs::ast::GroupByKey::Column("k".into())],
            },
            fields: vec![
                trellis::dev::defs::ast::FieldDef {
                    name: "k".into(),
                    expr: Expr::Column("k".into()),
                },
                trellis::dev::defs::ast::FieldDef {
                    name: "rel_agg".into(),
                    expr: Expr::FunctionCall {
                        name: "SUM".into(),
                        args: vec![path()],
                    },
                },
            ],
            predicate: Predicate::True,
            explicit_source_schema: None,
            explicit_target_schema: None,
        };
        assert_eq!(
            render_select(&def, "pk", &RelIndex::new(&program)),
            "select (\"t0\".\"k\")::text as \"k\", (sum(\"r0\".\"c1\"))::text as \"rel_agg\" \
             from \"t0\" left join \"t1\" as \"r0\" on \"r0\".\"k\" = \"t0\".\"f\" \
             group by \"t0\".\"k\""
        );
    }

    /// Design doc §2's "refuse to guess": a bare reference to a to-many path
    /// is a shape ADR-0006 forbids and the generator never emits, so the
    /// renderer refuses rather than picking one of the related rows.
    #[test]
    #[should_panic(expected = "bare reference to to-many relationship path 'r0.c1'")]
    fn render_select_refuses_a_bare_to_many_path() {
        let program = rel_program(Cardinality::ToMany);
        let def = TransformDef {
            target: "d0".into(),
            source: "t0".into(),
            key_space: KeySpace::OneToOne,
            fields: vec![trellis::dev::defs::ast::FieldDef {
                name: "rel_enrich".into(),
                expr: path(),
            }],
            predicate: Predicate::True,
            explicit_source_schema: None,
            explicit_target_schema: None,
        };
        let _ = render_select(&def, "pk", &RelIndex::new(&program));
    }

    /// An undeclared relationship is a generator bug, not something to
    /// render around: the engine rejects such a definition at validation
    /// time, so a program containing one could never have installed.
    #[test]
    #[should_panic(expected = "relationship \"nope\", which the program does not declare")]
    fn render_select_refuses_an_undeclared_relationship() {
        let program = rel_program(Cardinality::ToOne);
        let def = TransformDef {
            target: "d0".into(),
            source: "t0".into(),
            key_space: KeySpace::OneToOne,
            fields: vec![trellis::dev::defs::ast::FieldDef {
                name: "rel_enrich".into(),
                expr: Expr::RelationshipPath {
                    rel: "nope".into(),
                    column: "c1".into(),
                },
            }],
            predicate: Predicate::True,
            explicit_source_schema: None,
            explicit_target_schema: None,
        };
        let _ = render_select(&def, "pk", &RelIndex::new(&program));
    }

    /// A relationship path types as the *to-side* column's type, not
    /// anything on the definition's own source — which is what selects the
    /// right per-cell comparison (decimal-by-value vs. byte-exact) for the
    /// enrichment field.
    #[test]
    fn field_value_type_of_a_relationship_path_comes_from_the_to_side_table() {
        let program = rel_program(Cardinality::ToOne);
        let rels = RelIndex::new(&program);
        let source_columns = HashMap::new();
        assert_eq!(
            field_value_type(&path(), &source_columns, &rels),
            ValueType::Numeric
        );
        let text_path = Expr::RelationshipPath {
            rel: "r0".into(),
            column: "k".into(),
        };
        assert_eq!(
            field_value_type(&text_path, &source_columns, &rels),
            ValueType::Text
        );
    }

    /// `evaluator_checked: false` must read as "never asked", not "agreed":
    /// an empty `evaluator_vs_sql` means opposite things in the two cases,
    /// so the report says which one it is.
    #[test]
    fn a_report_says_so_when_the_evaluator_leg_did_not_run() {
        let report = ThreeWayReport {
            target_vs_sql: Vec::new(),
            evaluator_vs_sql: Vec::new(),
            evaluator_checked: false,
            program: String::new(),
        };
        assert!(!report.diverged());
        assert!(report.to_string().contains("evaluator leg not run"));
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
        assert_eq!(
            field_value_type(&expr, &source_columns, &RelIndex::new(&empty_program())),
            ValueType::Boolean
        );
    }

    /// Every one of the five scalar functions type-checks to its registered
    /// return type (all `Numeric` today) via [`field_value_type`], and
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
                field_value_type(&expr, &source_columns, &RelIndex::new(&empty_program())),
                ValueType::Numeric,
                "{name} must type-check as Numeric"
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
            field_value_type(&coalesce, &source_columns, &RelIndex::new(&empty_program())),
            ValueType::Text
        );
    }
}
