//! The expression generator: random, well-typed expression trees (and
//! near-miss ones) over what define admits, driven by the registry.
//!
//! Nothing here lists an operator or a function by name. The rules a tree is
//! grown from ([`rules`]) are read off [`registry::OPERATORS`] and
//! [`registry::FUNCTIONS`]: for every operator the closed set of operand
//! pairs [`registry::operator_result_type`] admits, and for every function
//! its declared signature. A new registry entry therefore draws, in every
//! property that uses this generator, with no change here. What stays
//! hand-written is what the registry cannot say: `COALESCE` (variadic, so
//! outside it), the literal pools, the one function argument that must be a
//! literal ([`LITERAL_ONLY_ARGS`]) and the unadmitted constructs
//! ([`UNADMITTED_OPERATORS`], [`UNADMITTED_FUNCTIONS`]), each of which stops
//! applying on its own once the registry admits it.
//!
//! The pure parts (rules, pools, rendering a tree as the text a user would
//! write, the near-miss list) are always compiled. The proptest strategies
//! are in [`strategy`], behind the `proptest` feature like the rest of the
//! suite's strategies.
//!
//! See `docs/generative-test-suite.md` ("Expression conformance") for how
//! the properties use it and how to add an operator.

use trellis::dev::defs::ast::{Expr, Operator, ValueType};
use trellis::dev::defs::registry;
use trellis::dev::defs::typed_literal::TYPED_LITERALS;
use trellis::dev::defs::{FloatWidth, IntWidth, PgType};

/// A source column: its name and the type define sees.
pub type Column = (String, ValueType);

/// How large the literals a tree draws are.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Magnitude {
    /// Small literals only: no overflow is reachable from a literal, so an
    /// end-to-end program built from these cannot fail at run time for the
    /// small values the suite's tables hold.
    Small,
    /// Range boundaries, `NaN`, `±Infinity` and numbers wider than any type:
    /// what the conformance properties need.
    Extreme,
}

/// What a draw may contain.
#[derive(Debug, Clone, Copy)]
pub struct Profile {
    /// The deepest nesting of operators, calls and `COALESCE`s.
    pub max_depth: u32,
    pub magnitude: Magnitude,
    /// Draw a `COALESCE` over numeric types of different kinds
    /// (`COALESCE(integer_col, numeric_col)`), whose result type Postgres
    /// picks. Off for the end-to-end tiers, whose oracle types a `COALESCE`
    /// as its first argument.
    pub mixed_coalesce: bool,
}

impl Profile {
    /// The conformance properties: boundary literals, depth 3.
    pub const CONFORMANCE: Profile = Profile {
        max_depth: 3,
        magnitude: Magnitude::Extreme,
        mixed_coalesce: true,
    };
    /// The end-to-end tiers: small literals and depth 2, so a program the
    /// engine accepts also runs.
    pub const END_TO_END: Profile = Profile {
        max_depth: 2,
        magnitude: Magnitude::Small,
        mixed_coalesce: false,
    };
}

/// One way to grow a typed tree, read off the registry.
#[derive(Debug, Clone, PartialEq)]
pub enum Rule {
    /// `lhs OP rhs` of these operand types, giving `out`.
    Op {
        op: Operator,
        lhs: ValueType,
        rhs: ValueType,
        out: ValueType,
    },
    /// A comparison whose operands are a float and a *bare number literal*.
    /// The registry refuses a float against an exact numeric, but define
    /// admits the literal spelling (Postgres types the literal as the
    /// float). Derived for every operator that gives `Boolean` and is refused
    /// for a float/exact pair.
    FloatVsLiteral { op: Operator, float: FloatWidth },
    /// `NAME(args)` of this registry signature.
    Call {
        name: &'static str,
        args: &'static [ValueType],
        out: ValueType,
    },
    /// `COALESCE(a, b, ...)` of arguments that resolve to `ty`.
    Coalesce { ty: ValueType },
}

impl Rule {
    /// The type the rule produces.
    pub fn out(&self) -> ValueType {
        match self {
            Rule::Op { out, .. } | Rule::Call { out, .. } => *out,
            Rule::FloatVsLiteral { .. } => ValueType::Boolean,
            Rule::Coalesce { ty } => *ty,
        }
    }
}

/// Function arguments the grammar requires to be a string literal, as
/// `(function, argument index)`: `regexp_count`'s pattern is compiled at
/// define time. The registry has no field for it.
pub const LITERAL_ONLY_ARGS: &[(&str, usize)] = &[("REGEXP_COUNT", 1)];

/// Every type the generator reasons about: the numeric types, `text`,
/// `boolean`, `uuid` and each type a typed literal can produce.
pub fn universe() -> Vec<ValueType> {
    let mut types: Vec<ValueType> = IntWidth::ALL.into_iter().map(ValueType::Integer).collect();
    types.push(ValueType::Numeric);
    types.extend(FloatWidth::ALL.into_iter().map(ValueType::Float));
    types.extend([ValueType::Text, ValueType::Boolean, ValueType::Uuid]);
    for spec in TYPED_LITERALS {
        if !types.contains(&spec.value_type) {
            types.push(spec.value_type);
        }
    }
    types
}

/// The rules, read off the registry. See the module doc.
pub fn rules() -> Vec<Rule> {
    let types = universe();
    let mut rules = Vec::new();
    for spec in registry::OPERATORS {
        for &lhs in &types {
            for &rhs in &types {
                match registry::operator_result_type(spec.operator, lhs, rhs) {
                    Some(out) => rules.push(Rule::Op {
                        op: spec.operator,
                        lhs,
                        rhs,
                        out,
                    }),
                    None if registry::is_mixed_float_exact(lhs, rhs) => {
                        // Both operands as floats resolve: the operator is
                        // refused only for the mix, so the literal spelling
                        // of the exact side is the exemption.
                        if let (ValueType::Float(w), true) = (
                            lhs,
                            registry::operator_result_type(spec.operator, lhs, lhs)
                                == Some(ValueType::Boolean),
                        ) {
                            rules.push(Rule::FloatVsLiteral {
                                op: spec.operator,
                                float: w,
                            });
                        }
                    }
                    None => {}
                }
            }
        }
    }
    let mut unique: Vec<Rule> = Vec::new();
    for rule in rules.drain(..) {
        if !unique.contains(&rule) {
            unique.push(rule);
        }
    }
    let mut rules = unique;
    for spec in registry::FUNCTIONS {
        rules.push(Rule::Call {
            name: spec.name,
            args: spec.arg_types,
            out: spec.return_type,
        });
    }
    for &ty in &types {
        rules.push(Rule::Coalesce { ty });
    }
    rules
}

// ---------------------------------------------------------------------------
// Literals
// ---------------------------------------------------------------------------

/// The type define gives a bare number literal, as Postgres's lexer does.
pub fn number_literal_type(text: &str) -> ValueType {
    if !text.contains('.')
        && let Ok(value) = text.parse::<i64>()
    {
        return ValueType::Integer(IntWidth::narrowest_for(value));
    }
    ValueType::Numeric
}

/// Bare number literals, by the type they take.
fn number_literals(ty: ValueType, magnitude: Magnitude) -> Vec<&'static str> {
    let pool: &[&str] = match magnitude {
        Magnitude::Small => &["0", "1", "2", "7", "42", "99", "0.5", "1.25", "10.0"],
        Magnitude::Extreme => &[
            "0",
            "1",
            "2",
            "32767",
            "32768",
            "2147483647",
            "2147483648",
            "9223372036854775807",
            "9223372036854775808",
            "99999999999999999999",
            "0.5",
            "0.0000001",
            "123456789012345678901234567890.123456789",
            "1.10",
        ],
    };
    pool.iter()
        .copied()
        .filter(|text| number_literal_type(text) == ty)
        .collect()
}

/// A number literal wider than `double precision`'s range, drawn against a
/// float so the `FloatComparisonLiteralOutOfRange` refusal is reached.
const HUGE_NUMBER_LITERAL: &str = concat!(
    "1000000000000000000000000000000000000000000000000000000000000000000000000000000000",
    "0000000000000000000000000000000000000000000000000000000000000000000000000000000000",
    "0000000000000000000000000000000000000000000000000000000000000000000000000000000000",
    "0000000000000000000000000000000000000000000000000000000000000000000000000000000000",
    "000000000000000000000000000000000000000000000000000000000000"
);

/// Canonical spellings per typed-literal keyword. A keyword with no entry
/// fails `every_typed_literal_has_samples`, so a new allowlist row cannot
/// slip past the generator.
pub fn typed_literal_samples(keyword: &str, magnitude: Magnitude) -> &'static [&'static str] {
    let extreme = magnitude == Magnitude::Extreme;
    match (keyword, extreme) {
        ("REAL", false) => &["0", "1", "1.5", "-2.5", "100"],
        ("REAL", true) => &[
            "0",
            "-0",
            "1.5",
            "NaN",
            "Infinity",
            "-Infinity",
            "3.4028235e+38",
            "-3.4028235e+38",
            "1e-45",
        ],
        ("DOUBLE PRECISION", false) => &["0", "1", "2.5", "-0.5", "1000"],
        ("DOUBLE PRECISION", true) => &[
            "0",
            "-0",
            "1.5",
            "NaN",
            "Infinity",
            "-Infinity",
            "1.7976931348623157e+308",
            "-1.7976931348623157e+308",
            "5e-324",
            "3.4028235e+38",
        ],
        ("DATE", _) => &["2024-02-29", "1970-01-01", "0001-01-01"],
        ("TIMESTAMP", _) => &["2024-02-29 12:00:00", "1970-01-01 00:00:00.5"],
        ("BYTEA", _) => &["\\x", "\\x00ff"],
        ("TIME", _) => &["00:00:00", "23:59:59.5"],
        ("TIMETZ", _) => &["00:00:00+00", "12:30:00-05"],
        ("OID", _) => &["0", "4294967295"],
        ("INET", _) => &["10.0.0.1/32", "::1/128"],
        ("CIDR", _) => &["10.0.0.0/8", "192.168.0.0/16"],
        ("MACADDR", _) => &["08:00:2b:01:02:03", "08:00:2b:01:02:04"],
        ("MACADDR8", _) => &["08:00:2b:01:02:03:04:05", "08:00:2b:01:02:03:04:06"],
        ("VARBIT", _) => &["", "101"],
        ("JSONB", _) => &["{}", "[1, 2]", "null"],
        _ => &[],
    }
}

/// `REGEXP_COUNT` patterns: the syntax Rust's `regex` crate and Postgres's
/// default dialect share (literal text, `.`, `*`, `+`, `?`, `[...]`, `|`,
/// `^`, `$`), which define compiles with the former and Postgres runs.
pub const REGEX_PATTERNS: &[&str] = &[
    "a", "o", "e", "b+", "[ab]", "a.c", "colou?r", "cat|dog", "[a-z]+", "^a", "a$", "c$", "a|b",
    "x.z", "a*", "z?", ".",
];

/// Patterns Rust's `regex` crate compiles (so define admits them) but
/// Postgres's dialect rejects with `2201B`, drawn by the conformance
/// profile only. Filed as #1448; the checker tolerates the failure.
pub const REGEX_DIALECT_PATTERNS: &[&str] = &[
    "(?<n>a)",
    "a\\z",
    "a++",
    "\\pL",
    "a{1,2}{3}",
    "(?i:a)",
    "(?-i)a",
    "x*+",
];

/// Strings for text literals and `STRPOS` needles, some of them drawn from
/// the same alphabet as the end-to-end tables' text columns so a needle
/// sometimes hits.
pub const TEXT_LITERALS: &[&str] = &[
    "", "a", "abc", "b", "o", "e", "wor", "it's", "x y", "é", "%",
];

/// Every literal a tree can use at `ty`.
pub fn literals(ty: ValueType, magnitude: Magnitude) -> Vec<Expr> {
    let mut out = Vec::new();
    match ty {
        ValueType::Integer(_) | ValueType::Numeric => {
            out.extend(
                number_literals(ty, magnitude)
                    .into_iter()
                    .map(|t| Expr::NumberLiteral(t.to_string())),
            );
        }
        ValueType::Text => out.extend(
            TEXT_LITERALS
                .iter()
                .map(|t| Expr::StringLiteral((*t).to_string())),
        ),
        _ => {}
    }
    if let Some(spec) = TYPED_LITERALS.iter().find(|s| s.value_type == ty) {
        out.extend(
            typed_literal_samples(spec.keyword, magnitude)
                .iter()
                .map(|t| Expr::TypedLiteral {
                    value_type: ty,
                    text: (*t).to_string(),
                }),
        );
    }
    out
}

/// The number literals a float is compared against.
pub fn comparison_literals(magnitude: Magnitude) -> Vec<Expr> {
    let mut pool: Vec<&str> = match magnitude {
        Magnitude::Small => vec!["0", "1", "0.5", "42"],
        Magnitude::Extreme => vec![
            "0",
            "1",
            "0.5",
            "2147483648",
            "99999999999999999999",
            "0.0000001",
            "123456789012345678901234567890.123456789",
        ],
    };
    if magnitude == Magnitude::Extreme {
        pool.push(HUGE_NUMBER_LITERAL);
    }
    pool.into_iter()
        .map(|t| Expr::NumberLiteral(t.to_string()))
        .collect()
}

// ---------------------------------------------------------------------------
// Rendering a tree as the text a user would write
// ---------------------------------------------------------------------------

/// How a tree is spelled as text. Parsing must give the same tree back for
/// every spelling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TextStyle {
    /// Parenthesize only where precedence requires it (read off the
    /// registry), instead of every binary operation.
    pub minimal: bool,
    /// Spell function names in lower case.
    pub lower_names: bool,
    /// Spell typed literals `CAST('..' AS type)` instead of `type '..'`.
    pub cast_form: bool,
}

fn precedence_of(expr: &Expr) -> u8 {
    match expr {
        Expr::BinaryOp { op, .. } => registry::operator_spec(*op).precedence,
        _ => u8::MAX,
    }
}

fn symbol_of(op: Operator) -> &'static str {
    registry::operator_spec(op).symbol
}

/// `expr` as the SQL text a user writes in a definition, by `style`.
pub fn render_text(expr: &Expr, style: TextStyle) -> String {
    let mut out = String::new();
    write_text(expr, style, &mut out);
    out
}

fn write_text(expr: &Expr, style: TextStyle, out: &mut String) {
    match expr {
        Expr::Column(name) => out.push_str(name),
        Expr::SourceColumn { table, column, .. } => {
            out.push_str(&format!("{table}.{column}"));
        }
        Expr::NumberLiteral(text) => out.push_str(text),
        Expr::StringLiteral(text) => out.push_str(&format!("'{}'", text.replace('\'', "''"))),
        Expr::TypedLiteral { value_type, text } => {
            let keyword = TYPED_LITERALS
                .iter()
                .find(|s| s.value_type == *value_type)
                .map(|s| s.keyword)
                .expect("a typed literal's type is in the allowlist");
            let text = text.replace('\'', "''");
            if style.cast_form {
                out.push_str(&format!("CAST('{text}' AS {keyword})"));
            } else {
                out.push_str(&format!("{keyword} '{text}'"));
            }
        }
        Expr::BinaryOp { op, lhs, rhs } => {
            let p = registry::operator_spec(*op).precedence;
            let is_comparison = p == registry::precedence::COMPARISON;
            // Left-associative: the right operand needs parentheses at equal
            // precedence. A comparison does not associate in Postgres at
            // all, so it needs them on the left too.
            let wrap_lhs = !style.minimal
                || precedence_of(lhs) < p
                || (is_comparison && precedence_of(lhs) == p);
            let wrap_rhs = !style.minimal || precedence_of(rhs) <= p;
            let side = |e: &Expr, wrap: bool, out: &mut String| {
                if wrap && matches!(e, Expr::BinaryOp { .. }) {
                    out.push('(');
                    write_text(e, style, out);
                    out.push(')');
                } else {
                    write_text(e, style, out);
                }
            };
            side(lhs, wrap_lhs, out);
            out.push(' ');
            out.push_str(symbol_of(*op));
            out.push(' ');
            side(rhs, wrap_rhs, out);
        }
        Expr::FunctionCall { name, args } => {
            out.push_str(&if style.lower_names {
                name.to_lowercase()
            } else {
                name.clone()
            });
            out.push('(');
            for (i, arg) in args.iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                write_text(arg, style, out);
            }
            out.push(')');
        }
    }
}

// ---------------------------------------------------------------------------
// Near misses
// ---------------------------------------------------------------------------

/// An expression define must refuse, and the word its error must contain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NearMiss {
    pub kind: &'static str,
    pub text: String,
    /// Lower-case; the refusal's message, lower-cased, contains one of them.
    /// They name the construct (the operator, the function or the type).
    pub needles: Vec<String>,
}

/// Operator symbols Postgres has and the registry does not. A line whose
/// symbol the registry now admits stops being a near miss and is skipped.
pub const UNADMITTED_OPERATORS: &[&str] = &["-", "*", "/", "%", "=", "<", "<=", ">=", "<>", "||"];

/// Functions and special forms Postgres has and the registry does not. A
/// `STABLE` or `VOLATILE` one is the trap: `now()` and `random()` change
/// between calls, `to_char` reads a session setting.
pub const UNADMITTED_FUNCTIONS: &[&str] = &[
    "ABS",
    "ROUND",
    "FLOOR",
    "CEIL",
    "LENGTH",
    "LOWER",
    "UPPER",
    "NOW",
    "RANDOM",
    "TO_CHAR",
    "CURRENT_DATE",
    "NULLIF",
    "GREATEST",
];

/// A column of type `ty`, when the schema has one.
fn column_of(columns: &[Column], ty: ValueType) -> Option<&str> {
    columns
        .iter()
        .find(|(_, t)| *t == ty)
        .map(|(name, _)| name.as_str())
}

/// Every near miss over `columns`: the wrong-typed operand of each operator
/// and function, the chained comparison, and the unadmitted constructs.
pub fn near_misses(columns: &[Column]) -> Vec<NearMiss> {
    let mut out = Vec::new();
    let types: Vec<ValueType> = {
        let mut t: Vec<ValueType> = columns.iter().map(|(_, t)| *t).collect();
        t.dedup();
        t
    };
    // Wrong-typed operands of every registry operator: the pairs the
    // registry has no result type for, except the float/exact mix a bare
    // literal is exempt from (a column on both sides is refused).
    for spec in registry::OPERATORS {
        for &lhs in &types {
            for &rhs in &types {
                if registry::operator_result_type(spec.operator, lhs, rhs).is_some() {
                    continue;
                }
                let (Some(a), Some(b)) = (column_of(columns, lhs), column_of(columns, rhs)) else {
                    continue;
                };
                out.push(NearMiss {
                    kind: "operator-operand-type",
                    text: format!("{a} {} {b}", spec.symbol),
                    needles: vec![spec.symbol.to_string()],
                });
            }
        }
    }
    // Wrong-typed argument of every registry function.
    for spec in registry::FUNCTIONS {
        for (i, expected) in spec.arg_types.iter().enumerate() {
            for &found in &types {
                if found == *expected || LITERAL_ONLY_ARGS.contains(&(spec.name, i)) {
                    continue;
                }
                let Some(bad) = column_of(columns, found) else {
                    continue;
                };
                let args: Vec<String> = spec
                    .arg_types
                    .iter()
                    .enumerate()
                    .map(|(j, t)| {
                        if i == j {
                            bad.to_string()
                        } else if let Some(c) = column_of(columns, *t) {
                            c.to_string()
                        } else {
                            "''".to_string()
                        }
                    })
                    .collect();
                // Only a literal pattern is checked for the literal-only
                // argument, so give it one when it is not the bad argument.
                let args: Vec<String> = args
                    .into_iter()
                    .enumerate()
                    .map(|(j, a)| {
                        if LITERAL_ONLY_ARGS.contains(&(spec.name, j)) && j != i {
                            "'a'".to_string()
                        } else {
                            a
                        }
                    })
                    .collect();
                out.push(NearMiss {
                    kind: "function-argument-type",
                    text: format!("{}({})", spec.name, args.join(", ")),
                    needles: vec![spec.name.to_lowercase()],
                });
            }
        }
    }
    // A literal-only argument given a column.
    for (name, index) in LITERAL_ONLY_ARGS {
        let Some(spec) = registry::lookup_function(name) else {
            continue;
        };
        let Some(col) = column_of(columns, spec.arg_types[*index]) else {
            continue;
        };
        let args: Vec<String> = spec
            .arg_types
            .iter()
            .enumerate()
            .map(|(j, t)| {
                if j == *index {
                    col.to_string()
                } else {
                    column_of(columns, *t).unwrap_or("''").to_string()
                }
            })
            .collect();
        out.push(NearMiss {
            kind: "literal-only-argument",
            text: format!("{}({})", spec.name, args.join(", ")),
            needles: vec![spec.name.to_lowercase()],
        });
    }
    // Chained comparison: a comparison whose operand is a comparison.
    for spec in registry::OPERATORS {
        if spec.precedence != registry::precedence::COMPARISON {
            continue;
        }
        if let Some(n) = column_of(columns, ValueType::Numeric) {
            out.push(NearMiss {
                kind: "chained-comparison",
                text: format!("{n} {0} {n} {0} {n}", spec.symbol),
                needles: vec!["boolean".to_string()],
            });
        }
    }
    // Operators the registry does not admit.
    if let (Some(a), Some(b)) = (
        column_of(columns, ValueType::Numeric),
        column_of(columns, ValueType::Integer(IntWidth::Int4)),
    ) {
        for symbol in UNADMITTED_OPERATORS {
            if registry::OPERATORS.iter().any(|s| s.symbol == *symbol) {
                continue;
            }
            out.push(NearMiss {
                kind: "unadmitted-operator",
                text: format!("{a} {symbol} {b}"),
                // The refusal names the whole symbol, quoted (`'<='`, not
                // the `'<'` it starts with), except a character the lexer
                // has no operator for at all (`|`), which it names alone.
                needles: vec![if symbol.starts_with('|') {
                    "'|'".to_string()
                } else {
                    format!("'{symbol}'")
                }],
            });
        }
        for name in UNADMITTED_FUNCTIONS {
            if registry::lookup_function(name).is_some() {
                continue;
            }
            out.push(NearMiss {
                kind: "unadmitted-function",
                text: format!("{name}({a})"),
                needles: vec![name.to_lowercase()],
            });
        }
    }
    // Typed literals in a spelling Postgres accepts and define refuses.
    for (keyword, text) in NONCANONICAL_LITERALS {
        if TYPED_LITERALS.iter().any(|s| s.keyword == *keyword) {
            out.push(NearMiss {
                kind: "noncanonical-typed-literal",
                text: format!("{keyword} '{text}'"),
                needles: vec![keyword.to_lowercase()],
            });
        }
    }
    out
}

/// Typed-literal spellings Postgres accepts and define refuses, as
/// `(keyword, text)`: relative or non-canonical ones (`'today'` is `STABLE`).
/// The refusal must name the keyword.
pub const NONCANONICAL_LITERALS: &[(&str, &str)] = &[
    ("DATE", "today"),
    ("DATE", "infinity"),
    ("DATE", "2024-1-5"),
    ("TIMESTAMP", "now"),
    ("TIMESTAMP", "infinity"),
    ("REAL", "+1.5"),
    ("DOUBLE PRECISION", "1.50"),
    ("OID", "-1"),
];

/// Which `ValueType`s the strategies may name as a column's type, for a
/// caller that lists its columns as `(name, type)`.
pub fn columns_by_type(columns: &[Column]) -> Vec<(ValueType, Vec<String>)> {
    let mut map: Vec<(ValueType, Vec<String>)> = Vec::new();
    for (name, ty) in columns {
        match map.iter_mut().find(|(t, _)| t == ty) {
            Some((_, names)) => names.push(name.clone()),
            None => map.push((*ty, vec![name.clone()])),
        }
    }
    map
}

/// The `PgType` of the conformance table's temporal columns, for the
/// callers that build the column list.
pub const DATE: ValueType = ValueType::Other(PgType::Date);
/// See [`DATE`].
pub const TIMESTAMP: ValueType = ValueType::Other(PgType::Timestamp);

#[cfg(feature = "proptest")]
pub mod strategy {
    //! The proptest strategies over [`super::rules`].

    use std::cell::RefCell;
    use std::rc::Rc;

    use proptest::prelude::*;
    use proptest::strategy::Union;

    use super::*;

    /// A drawn expression and how to spell it.
    #[derive(Debug, Clone)]
    pub struct Draw {
        pub expr: Expr,
        pub style: TextStyle,
    }

    impl Draw {
        pub fn text(&self) -> String {
            render_text(&self.expr, self.style)
        }
    }

    pub fn text_style() -> impl Strategy<Value = TextStyle> {
        (any::<bool>(), any::<bool>(), any::<bool>()).prop_map(
            |(minimal, lower_names, cast_form)| TextStyle {
                minimal,
                lower_names,
                cast_form,
            },
        )
    }

    /// Strategies already built, by type and remaining depth.
    type Cache = Vec<((ValueType, u32), Option<BoxedStrategy<Expr>>)>;

    struct Builder {
        columns: Vec<(ValueType, Vec<String>)>,
        rules: Vec<Rule>,
        profile: Profile,
        cache: RefCell<Cache>,
    }

    impl Builder {
        fn leaves(&self, ty: ValueType) -> Vec<(u32, BoxedStrategy<Expr>)> {
            let mut leaves = Vec::new();
            if let Some((_, names)) = self.columns.iter().find(|(t, _)| *t == ty) {
                leaves.push((
                    4,
                    prop::sample::select(names.clone())
                        .prop_map(Expr::Column)
                        .boxed(),
                ));
            }
            let lits = literals(ty, self.profile.magnitude);
            if !lits.is_empty() {
                leaves.push((2, prop::sample::select(lits).boxed()));
            }
            leaves
        }

        /// A strategy for a tree of type `ty` at most `depth` deep, or
        /// `None` when nothing produces `ty` that shallow.
        fn expr(self: &Rc<Self>, ty: ValueType, depth: u32) -> Option<BoxedStrategy<Expr>> {
            if let Some((_, hit)) = self.cache.borrow().iter().find(|(k, _)| *k == (ty, depth)) {
                return hit.clone();
            }
            let mut options = self.leaves(ty);
            if depth > 0 {
                for rule in self.rules.iter().filter(|r| r.out() == ty) {
                    if let Some(strategy) = self.rule(rule, depth - 1) {
                        options.push((1, strategy));
                    }
                }
            }
            let built = if options.is_empty() {
                None
            } else {
                Some(Union::new_weighted(options).boxed())
            };
            self.cache.borrow_mut().push(((ty, depth), built.clone()));
            built
        }

        fn rule(self: &Rc<Self>, rule: &Rule, depth: u32) -> Option<BoxedStrategy<Expr>> {
            match rule {
                Rule::Op { op, lhs, rhs, .. } => {
                    let (op, l, r) = (*op, self.expr(*lhs, depth)?, self.expr(*rhs, depth)?);
                    Some(
                        (l, r)
                            .prop_map(move |(lhs, rhs)| Expr::BinaryOp {
                                op,
                                lhs: Box::new(lhs),
                                rhs: Box::new(rhs),
                            })
                            .boxed(),
                    )
                }
                Rule::FloatVsLiteral { op, float } => {
                    let op = *op;
                    let float = self.expr(ValueType::Float(*float), depth)?;
                    let literal = prop::sample::select(comparison_literals(self.profile.magnitude));
                    Some(
                        (float, literal, any::<bool>())
                            .prop_map(move |(f, lit, flip)| {
                                let (lhs, rhs) = if flip { (lit, f) } else { (f, lit) };
                                Expr::BinaryOp {
                                    op,
                                    lhs: Box::new(lhs),
                                    rhs: Box::new(rhs),
                                }
                            })
                            .boxed(),
                    )
                }
                Rule::Call { name, args, .. } => {
                    let mut parts: BoxedStrategy<Vec<Expr>> = Just(Vec::new()).boxed();
                    for (i, ty) in args.iter().enumerate() {
                        let arg = if LITERAL_ONLY_ARGS.contains(&(*name, i)) {
                            let mut patterns = REGEX_PATTERNS.to_vec();
                            if self.profile.magnitude == Magnitude::Extreme {
                                patterns.extend(REGEX_DIALECT_PATTERNS);
                            }
                            prop::sample::select(patterns)
                                .prop_map(|p| Expr::StringLiteral(p.to_string()))
                                .boxed()
                        } else {
                            self.expr(*ty, depth)?
                        };
                        parts = (parts, arg)
                            .prop_map(|(mut v, e)| {
                                v.push(e);
                                v
                            })
                            .boxed();
                    }
                    let name = (*name).to_string();
                    Some(
                        parts
                            .prop_map(move |args| Expr::FunctionCall {
                                name: name.clone(),
                                args,
                            })
                            .boxed(),
                    )
                }
                Rule::Coalesce { ty } => {
                    let arg = self.expr(*ty, depth)?;
                    Some(
                        prop::collection::vec(arg, 1..=3)
                            .prop_map(|args| Expr::FunctionCall {
                                name: "COALESCE".to_string(),
                                args,
                            })
                            .boxed(),
                    )
                }
            }
        }
    }

    /// A tree of any type the columns and rules can produce, with how to
    /// spell it.
    pub fn any_expr(columns: &[Column], profile: Profile) -> BoxedStrategy<Draw> {
        let builder = Rc::new(Builder {
            columns: columns_by_type(columns),
            rules: rules(),
            profile,
            cache: RefCell::new(Vec::new()),
        });
        let mut strategies: Vec<BoxedStrategy<Expr>> = universe()
            .into_iter()
            .filter_map(|ty| builder.expr(ty, profile.max_depth))
            .collect();
        // `COALESCE` over numeric types of different kinds: Postgres picks
        // the result type, so no rule can name it.
        if profile.max_depth > 0 && profile.mixed_coalesce {
            let numeric: Vec<BoxedStrategy<Expr>> = universe()
                .into_iter()
                .filter(|ty| ty.is_numeric_family())
                .filter_map(|ty| builder.expr(ty, profile.max_depth - 1))
                .collect();
            if !numeric.is_empty() {
                strategies.push(
                    prop::collection::vec(Union::new(numeric), 2..=3)
                        .prop_map(|args| Expr::FunctionCall {
                            name: "COALESCE".to_string(),
                            args,
                        })
                        .boxed(),
                );
            }
        }
        // Boxed trees hold no `Rc`; the builder is dropped here.
        (Union::new(strategies), text_style())
            .prop_map(|(expr, style)| Draw { expr, style })
            .boxed()
    }

    /// A tree of exactly type `ty`, for a caller that needs one (the
    /// end-to-end tiers want a `numeric`, `text` or `boolean` field).
    pub fn expr_of(columns: &[Column], profile: Profile, ty: ValueType) -> BoxedStrategy<Expr> {
        let builder = Rc::new(Builder {
            columns: columns_by_type(columns),
            rules: rules(),
            profile,
            cache: RefCell::new(Vec::new()),
        });
        builder
            .expr(ty, profile.max_depth)
            .unwrap_or_else(|| panic!("nothing produces {ty} over these columns"))
    }

    /// A near miss over `columns`.
    pub fn near_miss(columns: &[Column]) -> BoxedStrategy<NearMiss> {
        prop::sample::select(near_misses(columns)).boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every typed-literal keyword has samples, and each passes the
    /// engine's own canonical-form check: a new allowlist row cannot be
    /// silently left out of the generator.
    #[test]
    fn every_typed_literal_has_samples() {
        for spec in TYPED_LITERALS {
            for magnitude in [Magnitude::Small, Magnitude::Extreme] {
                let samples = typed_literal_samples(spec.keyword, magnitude);
                assert!(
                    samples.len() >= 2,
                    "{} needs two distinct samples (the self_check tampering property writes \
                     one over the other); add them to typed_literal_samples",
                    spec.keyword
                );
                for text in samples {
                    assert!(
                        (spec.canonical)(text).is_ok(),
                        "{} sample {text:?} is not canonical",
                        spec.keyword
                    );
                }
            }
        }
    }

    /// Every registry operator and function yields at least one rule, so
    /// the universe of types is rich enough to exercise it.
    #[test]
    fn every_registry_entry_has_a_rule() {
        let rules = rules();
        for spec in registry::OPERATORS {
            assert!(
                rules
                    .iter()
                    .any(|r| matches!(r, Rule::Op { op, .. } if *op == spec.operator)),
                "operator {} yields no rule",
                spec.symbol
            );
        }
        for spec in registry::FUNCTIONS {
            assert!(
                rules
                    .iter()
                    .any(|r| matches!(r, Rule::Call { name, .. } if *name == spec.name)),
                "function {} yields no rule",
                spec.name
            );
        }
    }

    /// Each non-canonical sample really is rejected by the engine's checker.
    #[test]
    fn noncanonical_literals_are_noncanonical() {
        for (keyword, text) in NONCANONICAL_LITERALS {
            let spec = TYPED_LITERALS
                .iter()
                .find(|s| s.keyword == *keyword)
                .unwrap_or_else(|| panic!("{keyword} is not a typed literal"));
            assert!((spec.canonical)(text).is_err(), "{keyword} '{text}'");
        }
    }

    /// The literal pools hold only what they claim to.
    #[test]
    fn number_literal_pools_have_the_type_they_are_drawn_for() {
        for ty in [
            ValueType::Integer(IntWidth::Int4),
            ValueType::Integer(IntWidth::Int8),
            ValueType::Numeric,
        ] {
            for magnitude in [Magnitude::Small, Magnitude::Extreme] {
                for lit in literals(ty, magnitude) {
                    if let Expr::NumberLiteral(text) = lit {
                        assert_eq!(number_literal_type(&text), ty, "{text}");
                    }
                }
            }
        }
    }
}
