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
    /// Draw a bare `NULL` as either operand of an operator that takes one
    /// type on both sides, typed from the other operand as Postgres types
    /// it. Off for the end-to-end tiers, whose oracle types an operator from
    /// its operands' own types.
    pub bare_null: bool,
}

impl Profile {
    /// The conformance properties: boundary literals, depth 3.
    pub const CONFORMANCE: Profile = Profile {
        max_depth: 3,
        magnitude: Magnitude::Extreme,
        mixed_coalesce: true,
        bare_null: true,
    };
    /// The end-to-end tiers: small literals and depth 2, so a program the
    /// engine accepts also runs.
    pub const END_TO_END: Profile = Profile {
        max_depth: 2,
        magnitude: Magnitude::Small,
        mixed_coalesce: false,
        bare_null: false,
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
    /// Prefix `-` over an operand of type `ty`.
    Negate { ty: ValueType },
    /// `NOT <boolean>`.
    Not,
    /// `<operand of type ty> IS [NOT] NULL`.
    IsNull { ty: ValueType },
    /// A searched `CASE` whose results all have type `ty`.
    Case { ty: ValueType },
    /// A searched `CASE` whose `THEN` results have type `then` and whose
    /// `ELSE` has type `other`, two numeric types Postgres unifies to `out`
    /// (`CASE WHEN c THEN 1 ELSE 2.5 END` is `numeric`).
    CaseMixed {
        then: ValueType,
        other: ValueType,
        out: ValueType,
    },
    /// `NULLIF(a, b)` of operands `lhs` and `rhs` that `=` compares, giving
    /// `lhs`.
    NullIf { lhs: ValueType, rhs: ValueType },
    /// `NULLIF(float, <bare number literal>)`, the spelling define admits
    /// beside a float.
    NullIfLiteral { float: FloatWidth },
    /// `CAST(operand AS to)` over an operand of type `from`.
    Cast { from: ValueType, to: ValueType },
    /// `EXTRACT(<field> FROM operand)` over an operand of type `operand`,
    /// giving a `numeric`. The fields are the type's
    /// ([`registry::extract_fields`]).
    Extract { operand: ValueType },
}

impl Rule {
    /// The type the rule produces.
    pub fn out(&self) -> ValueType {
        match self {
            Rule::Op { out, .. } | Rule::Call { out, .. } => *out,
            Rule::FloatVsLiteral { .. } => ValueType::Boolean,
            Rule::Coalesce { ty } => *ty,
            Rule::Negate { ty } => *ty,
            Rule::Cast { to, .. } => *to,
            Rule::Extract { .. } => ValueType::Numeric,
            Rule::Not | Rule::IsNull { .. } => ValueType::Boolean,
            Rule::Case { ty } => *ty,
            Rule::CaseMixed { out, .. } => *out,
            Rule::NullIf { lhs, .. } => *lhs,
            Rule::NullIfLiteral { float } => ValueType::Float(*float),
        }
    }
}

/// Function arguments the grammar requires to be a string literal, as
/// `(function, argument index)`: `regexp_count`'s pattern is compiled at
/// define time. The registry has no field for it.
pub const LITERAL_ONLY_ARGS: &[(&str, usize)] = &[("REGEXP_COUNT", 1), ("DATE_TRUNC", 0)];

/// The operators whose right operand must be a given string literal, as
/// `(operator, literal)`: the zone of `AT TIME ZONE`. The registry types the
/// operand as `text`, which would draw any string.
pub const LITERAL_RHS_OPERATORS: &[(Operator, &str)] = &[(Operator::AtTimeZone, "UTC")];

/// The scales drawn for an argument in [`registry::INTEGER_LITERAL_ARGS`]:
/// `ROUND(a, n)` of a large positive `n` makes a value with that many digits,
/// so the pool stays small.
pub const SCALE_LITERALS: &[&str] = &["0", "1", "2", "5", "-1", "-2", "-5", "20", "-20"];

/// Every type the generator reasons about: the numeric types, `text`,
/// `boolean`, `uuid` and each type a typed literal can produce.
pub fn universe() -> Vec<ValueType> {
    let mut types: Vec<ValueType> = IntWidth::ALL.into_iter().map(ValueType::Integer).collect();
    types.push(ValueType::Numeric);
    types.extend(FloatWidth::ALL.into_iter().map(ValueType::Float));
    types.extend([ValueType::Text, ValueType::Boolean, ValueType::Uuid]);
    // A type with a column in the conformance table and no typed literal.
    types.push(ValueType::Other(PgType::TimestampTz));
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
    rules.push(Rule::Not);
    for &ty in &types {
        if registry::extract_fields(ty).is_some() {
            rules.push(Rule::Extract { operand: ty });
        }
    }
    for width in FloatWidth::ALL {
        rules.push(Rule::NullIfLiteral { float: width });
    }
    for &ty in &types {
        rules.push(Rule::IsNull { ty });
        rules.push(Rule::Case { ty });
        for &rhs in &types {
            // `NULLIF` is typed by its first argument, and refused where
            // Postgres casts that argument to compare it.
            let cast = matches!((ty, rhs), (ValueType::Integer(_), ValueType::Numeric))
                || (ty.is_exact_numeric_family() && matches!(rhs, ValueType::Float(_)));
            if !cast && registry::comparison_result_type(Operator::Equal, ty, rhs).is_some() {
                rules.push(Rule::NullIf { lhs: ty, rhs });
            }
            if ty != rhs && ty.is_numeric_family() && rhs.is_numeric_family() {
                // The type `COALESCE` and `CASE` resolve: a float beside an
                // exact type is the float, two floats the wider, two integers
                // the wider, otherwise numeric.
                let out = match (ty, rhs) {
                    (ValueType::Float(a), ValueType::Float(b)) => ValueType::Float(a.wider(b)),
                    (ValueType::Float(w), _) | (_, ValueType::Float(w)) => ValueType::Float(w),
                    (ValueType::Integer(a), ValueType::Integer(b)) => {
                        ValueType::Integer(a.wider(b))
                    }
                    _ => ValueType::Numeric,
                };
                rules.push(Rule::CaseMixed {
                    then: ty,
                    other: rhs,
                    out,
                });
            }
        }
        rules.push(Rule::Coalesce { ty });
        if registry::negate_result_type(ty).is_some() {
            rules.push(Rule::Negate { ty });
        }
        for &to in registry::CAST_TARGETS {
            if registry::cast_result_type(ty, to).is_some() {
                rules.push(Rule::Cast { from: ty, to });
            }
        }
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
        Magnitude::Small => &[
            "0", "1", "2", "7", "42", "99", "-1", "-3", "0.5", "1.25", "10.0", "-0.5",
        ],
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
            "-1",
            "-32768",
            "-2147483648",
            "-2147483649",
            "-9223372036854775808",
            "-9223372036854775809",
            "0.5",
            "-0.5",
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

/// `REGEXP_COUNT` patterns Postgres accepts: basic syntax (literal text, `.`,
/// `*`, `+`, `?`, `[...]`, `|`, `^`, `$`) and constructs only Postgres has
/// (a back-reference, lookahead, `\m`).
pub const REGEX_PATTERNS: &[&str] = &[
    "a", "o", "e", "b+", "[ab]", "a.c", "colou?r", "cat|dog", "[a-z]+", "^a", "a$", "c$", "a|b",
    "x.z", "a*", "z?", ".", "(a)\\1", "a(?=b)", "\\ma",
];

/// Patterns Postgres's dialect rejects with `2201B`, which define refuses
/// (Postgres judges each), drawn by the conformance profile only.
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

/// The literals a tree can use at `ty` beyond [`literals`]: `TRUE` and
/// `FALSE` for a `boolean`, and `CAST(NULL AS <ty>)`, which every position of
/// its type takes. Kept apart from [`literals`] so the deterministic
/// one-level sweeps, which take the product of every leaf, stay their size;
/// those sweeps place these one operand at a time instead.
pub fn constant_keyword_literals(ty: ValueType) -> Vec<Expr> {
    let mut out = Vec::new();
    if ty == ValueType::Boolean {
        out.extend([Expr::BooleanLiteral(true), Expr::BooleanLiteral(false)]);
    }
    if registry::NULL_CAST_TARGETS.contains(&ty) || registry::CAST_TARGETS.contains(&ty) {
        out.push(typed_null(ty));
    }
    out
}

/// `CAST(NULL AS <ty>)`.
pub fn typed_null(ty: ValueType) -> Expr {
    Expr::Cast {
        expr: Box::new(Expr::Null),
        target: ty,
    }
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
    /// Spell a `CAST` target as its n-th Postgres alias
    /// (`registry::CAST_TARGET_ALIASES`) instead of its canonical name; 0, or
    /// a target with fewer aliases, keeps the canonical name. Rendering a
    /// parsed tree always gives the canonical name back.
    pub cast_alias: u8,
}

/// How a `CAST` target is spelled under `style`.
fn cast_target_text(target: ValueType, style: TextStyle) -> String {
    let aliases: Vec<&str> = registry::CAST_TARGET_ALIASES
        .iter()
        .filter(|(_, t)| *t == target)
        .map(|&(name, _)| name)
        .collect();
    match usize::from(style.cast_alias)
        .checked_sub(1)
        .and_then(|i| aliases.get(i))
    {
        Some(alias) if style.lower_names => alias.to_string(),
        Some(alias) => alias.to_uppercase(),
        None => target.to_string(),
    }
}

fn registry_extract<'e>(name: &str, args: &'e [Expr]) -> Option<(&'e str, &'e Expr)> {
    trellis::dev::defs::ast::as_extract(name, args)
}

fn precedence_of(expr: &Expr) -> u8 {
    match expr {
        Expr::BinaryOp { op, .. } => registry::operator_spec(*op).precedence,
        Expr::Not(_) => registry::precedence::NOT,
        Expr::IsNull { .. } => registry::precedence::IS,
        _ => u8::MAX,
    }
}

/// Whether `expr` needs parentheses as the operand of a prefix `-`: anything
/// but a column, a call, a cast or a non-negative literal. (`- -a` would
/// read as a comment in SQL, and `-(-5)` is a different tree from `- -5`.)
fn negate_operand_needs_parens(expr: &Expr) -> bool {
    match expr {
        Expr::BinaryOp { .. }
        | Expr::Negate(_)
        | Expr::Not(_)
        | Expr::IsNull { .. }
        | Expr::Case { .. } => true,
        Expr::NumberLiteral(text) => text.starts_with('-'),
        _ => false,
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

/// A keyword in the style's case.
fn keyword(style: TextStyle, word: &str) -> String {
    if style.lower_names {
        word.to_lowercase()
    } else {
        word.to_string()
    }
}

/// Writes `e`, in parentheses when `wrap` and it is an operator expression
/// (the only nodes that can bind looser than their context).
fn write_operand(e: &Expr, wrap: bool, style: TextStyle, out: &mut String) {
    if wrap
        && matches!(
            e,
            Expr::BinaryOp { .. } | Expr::Not(_) | Expr::IsNull { .. }
        )
    {
        out.push('(');
        write_text(e, style, out);
        out.push(')');
    } else {
        write_text(e, style, out);
    }
}

fn write_text(expr: &Expr, style: TextStyle, out: &mut String) {
    match expr {
        Expr::Column(name) => out.push_str(name),
        Expr::SourceColumn { table, column, .. } => {
            out.push_str(&format!("{table}.{column}"));
        }
        Expr::NumberLiteral(text) => out.push_str(text),
        Expr::StringLiteral(text) => out.push_str(&format!("'{}'", text.replace('\'', "''"))),
        Expr::BooleanLiteral(true) => out.push_str(&keyword(style, "TRUE")),
        Expr::BooleanLiteral(false) => out.push_str(&keyword(style, "FALSE")),
        Expr::Null => out.push_str(&keyword(style, "NULL")),
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
            // Left-associative: the right operand needs parentheses at equal
            // precedence. A comparison does not associate in Postgres at
            // all, so it needs them on the left too.
            let wrap_lhs = !style.minimal
                || precedence_of(lhs) < p
                || (op.is_non_associative() && precedence_of(lhs) == p);
            let wrap_rhs = !style.minimal || precedence_of(rhs) <= p;
            write_operand(lhs, wrap_lhs, style, out);
            out.push(' ');
            out.push_str(&keyword(style, symbol_of(*op)));
            out.push(' ');
            write_operand(rhs, wrap_rhs, style, out);
        }
        Expr::Not(operand) => {
            out.push_str(&keyword(style, "NOT"));
            out.push(' ');
            // `NOT` takes everything tighter than `AND`: only an `AND` or
            // `OR` operand needs the parentheses. An operand that starts with
            // a minus would read as a subtraction from a column named `not`.
            let mut text = String::new();
            write_text(operand, style, &mut text);
            let wrap = !style.minimal
                || precedence_of(operand) < registry::precedence::NOT
                || text.starts_with('-');
            if wrap {
                out.push('(');
                out.push_str(&text);
                out.push(')');
            } else {
                out.push_str(&text);
            }
        }
        Expr::IsNull { expr, negated } => {
            // `IS` binds looser than a comparison, and `IS DISTINCT FROM`
            // does not chain with a following `IS`.
            let wrap = !style.minimal
                || precedence_of(expr) < registry::precedence::IS
                || matches!(&**expr, Expr::BinaryOp { op, .. } if op.is_non_associative()
                    && registry::operator_spec(*op).precedence == registry::precedence::IS);
            write_operand(expr, wrap, style, out);
            out.push(' ');
            out.push_str(&keyword(style, "IS"));
            out.push(' ');
            if *negated {
                out.push_str(&keyword(style, "NOT"));
                out.push(' ');
            }
            out.push_str(&keyword(style, "NULL"));
        }
        Expr::Case {
            branches,
            otherwise,
        } => {
            let part = |e: &Expr, out: &mut String| {
                if style.minimal {
                    write_text(e, style, out);
                } else {
                    out.push('(');
                    write_text(e, style, out);
                    out.push(')');
                }
            };
            out.push_str(&keyword(style, "CASE"));
            for (condition, result) in branches {
                out.push(' ');
                out.push_str(&keyword(style, "WHEN"));
                out.push(' ');
                part(condition, out);
                out.push(' ');
                out.push_str(&keyword(style, "THEN"));
                out.push(' ');
                part(result, out);
            }
            if let Some(otherwise) = otherwise {
                out.push(' ');
                out.push_str(&keyword(style, "ELSE"));
                out.push(' ');
                part(otherwise, out);
            }
            out.push(' ');
            out.push_str(&keyword(style, "END"));
        }
        Expr::Negate(operand) => {
            out.push('-');
            if style.minimal && !negate_operand_needs_parens(operand) {
                write_text(operand, style, out);
            } else {
                out.push('(');
                write_text(operand, style, out);
                out.push(')');
            }
        }
        Expr::Cast { expr, target } => {
            let (cast, as_) = if style.lower_names {
                ("cast", "as")
            } else {
                ("CAST", "AS")
            };
            out.push_str(cast);
            out.push('(');
            write_text(expr, style, out);
            out.push_str(&format!(" {as_} {})", cast_target_text(*target, style)));
        }
        Expr::FunctionCall { name, args } if registry_extract(name, args).is_some() => {
            let (field, operand) = registry_extract(name, args).expect("checked");
            out.push_str(&keyword(style, "EXTRACT"));
            out.push('(');
            // The field is a bare word, in any case.
            out.push_str(&if style.lower_names {
                field.to_string()
            } else {
                field.to_uppercase()
            });
            out.push(' ');
            out.push_str(&keyword(style, "FROM"));
            out.push(' ');
            write_text(operand, style, out);
            out.push(')');
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
pub const UNADMITTED_OPERATORS: &[&str] = &["||"];

/// Functions and special forms Postgres has and the registry does not. A
/// `STABLE` or `VOLATILE` one is the trap: `now()` and `random()` change
/// between calls, `to_char` reads a session setting.
pub const UNADMITTED_FUNCTIONS: &[&str] = &[
    "LENGTH",
    "LOWER",
    "UPPER",
    "NOW",
    "RANDOM",
    "TO_CHAR",
    "CURRENT_DATE",
    "GREATEST",
    "DATE_PART",
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
                    needles: vec![spec.symbol.to_lowercase()],
                });
            }
        }
    }
    // Wrong-typed argument of every registry function: a call no overload
    // of the name accepts, varying one argument at a time.
    let mut names: Vec<&'static str> = registry::FUNCTIONS.iter().map(|s| s.name).collect();
    names.dedup();
    for name in names {
        for spec in registry::function_overloads(name) {
            for (i, expected) in spec.arg_types.iter().enumerate() {
                for &found in &types {
                    if found == *expected
                        || LITERAL_ONLY_ARGS.contains(&(name, i))
                        || registry::INTEGER_LITERAL_ARGS.contains(&(name, i))
                    {
                        continue;
                    }
                    let Some(bad) = column_of(columns, found) else {
                        continue;
                    };
                    let mut arg_types = spec.arg_types.to_vec();
                    arg_types[i] = found;
                    if registry::resolve_function(name, &arg_types).is_some() {
                        continue;
                    }
                    let args: Vec<String> = spec
                        .arg_types
                        .iter()
                        .enumerate()
                        .map(|(j, t)| {
                            if i == j {
                                bad.to_string()
                            } else if LITERAL_ONLY_ARGS.contains(&(name, j)) {
                                // A unit `DATE_TRUNC` takes, so the wrong
                                // type beside it is what is refused.
                                if name == "DATE_TRUNC" { "'day'" } else { "'a'" }.to_string()
                            } else if registry::INTEGER_LITERAL_ARGS.contains(&(name, j)) {
                                "1".to_string()
                            } else if let Some(c) = column_of(columns, *t) {
                                c.to_string()
                            } else {
                                "''".to_string()
                            }
                        })
                        .collect();
                    out.push(NearMiss {
                        kind: "function-argument-type",
                        text: format!("{name}({})", args.join(", ")),
                        needles: vec![name.to_lowercase()],
                    });
                }
            }
        }
    }
    // A scale given as a column rather than a literal.
    for (name, index) in registry::INTEGER_LITERAL_ARGS {
        let Some(spec) = registry::function_overloads(name).find(|s| s.arg_types.len() > *index)
        else {
            continue;
        };
        let Some(col) = column_of(columns, spec.arg_types[*index]) else {
            continue;
        };
        let Some(first) = column_of(columns, spec.arg_types[0]) else {
            continue;
        };
        out.push(NearMiss {
            kind: "scale-not-literal",
            text: format!("{name}({first}, {col})"),
            needles: vec![name.to_lowercase()],
        });
    }
    // Two adjacent minuses: a comment in SQL, refused rather than read as a
    // minus and a negative literal.
    if let Some(col) = column_of(columns, ValueType::Integer(IntWidth::Int4)) {
        out.push(NearMiss {
            kind: "double-minus",
            text: format!("{col}--1"),
            needles: vec!["comment".to_string()],
        });
    }
    // Prefix minus over a non-numeric operand.
    for &ty in &types {
        if registry::negate_result_type(ty).is_none()
            && let Some(col) = column_of(columns, ty)
        {
            out.push(NearMiss {
                kind: "negate-operand-type",
                text: format!("-{col}"),
                needles: vec!["'-'".to_string()],
            });
        }
    }
    // Casts the registry does not admit, to each target.
    for &ty in &types {
        let Some(col) = column_of(columns, ty) else {
            continue;
        };
        for &to in registry::CAST_TARGETS {
            if registry::cast_result_type(ty, to).is_none() {
                out.push(NearMiss {
                    kind: "cast-operand-type",
                    text: format!("CAST({col} AS {to})"),
                    needles: vec![to.to_string()],
                });
            }
        }
    }
    // Cast targets the grammar has no spelling for, and a modifier.
    if let Some(col) = column_of(columns, ValueType::Integer(IntWidth::Int2)) {
        for target in [
            "smallint",
            "text",
            "real",
            "int2",
            "bigserial",
            "float8",
            "numeric(10, 2)",
            "decimal(10, 2)",
            "timestamptz",
            "timestamp(3)",
            "date",
        ] {
            out.push(NearMiss {
                kind: "cast-target",
                text: format!("CAST({col} AS {target})"),
                needles: vec![target.split('(').next().unwrap_or(target).to_string()],
            });
        }
    }
    // A divisor that is the constant zero.
    for &ty in &types {
        if !ty.is_numeric_family() {
            continue;
        }
        let Some(col) = column_of(columns, ty) else {
            continue;
        };
        for op in ["/", "%"] {
            if registry::lookup_operator(op).is_none() {
                continue;
            }
            for zero in ["0", "0.0", "(1 - 1)"] {
                let text = format!("{col} {op} {zero}");
                // Only the pairs the registry types are refused for the
                // zero; the rest are near misses of their own kind.
                let zero_type = number_literal_type(
                    zero.trim_matches(['(', ')'])
                        .split(' ')
                        .next()
                        .unwrap_or("0"),
                );
                if registry::operator_result_type(
                    registry::lookup_operator(op).expect("checked"),
                    ty,
                    zero_type,
                )
                .is_some()
                {
                    out.push(NearMiss {
                        kind: "constant-zero-divisor",
                        text,
                        needles: vec!["zero".to_string()],
                    });
                }
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
    // Chained comparison: `a < b < c`, a syntax error in Postgres.
    for spec in registry::OPERATORS {
        if !spec.operator.is_non_associative() {
            continue;
        }
        if let Some(n) = column_of(columns, ValueType::Numeric) {
            out.push(NearMiss {
                kind: "chained-comparison",
                text: format!("{n} {0} {n} {0} {n}", spec.symbol),
                needles: vec!["do not chain".to_string()],
            });
        }
    }
    let first = |ty: ValueType| column_of(columns, ty);
    let (int, boolean, text, numeric) = (
        first(ValueType::Integer(IntWidth::Int4)),
        first(ValueType::Boolean),
        first(ValueType::Text),
        first(ValueType::Numeric),
    );
    // The simple `CASE`, and the truth-value tests `COALESCE` covers.
    if let Some(int) = int {
        out.push(NearMiss {
            kind: "simple-case",
            text: format!("CASE {int} WHEN 1 THEN 2 END"),
            needles: vec!["simple case".to_string()],
        });
        out.push(NearMiss {
            kind: "case-condition-type",
            text: format!("CASE WHEN {int} THEN 1 END"),
            needles: vec!["case condition".to_string()],
        });
        out.push(NearMiss {
            kind: "not-operand-type",
            text: format!("NOT {int}"),
            needles: vec!["'not'".to_string()],
        });
    }
    if let Some(boolean) = boolean {
        for form in ["IS TRUE", "IS NOT TRUE", "IS FALSE", "IS UNKNOWN"] {
            out.push(NearMiss {
                kind: "unsupported-is-form",
                text: format!("{boolean} {form}"),
                needles: vec![form.to_lowercase()],
            });
        }
        if let Some(int) = int {
            out.push(NearMiss {
                kind: "case-result-types",
                text: format!("CASE WHEN {boolean} THEN {int} ELSE {boolean} END"),
                needles: vec!["share a type".to_string()],
            });
        }
        if let Some(text) = text {
            out.push(NearMiss {
                kind: "case-result-types",
                text: format!("CASE WHEN {boolean} THEN {text} ELSE 1 END"),
                needles: vec!["share a type".to_string()],
            });
        }
    }
    if let (Some(int), Some(numeric)) = (int, numeric) {
        // Postgres types these by a cast of the first argument.
        out.push(NearMiss {
            kind: "nullif-first-argument-cast",
            text: format!("NULLIF({int}, {numeric})"),
            needles: vec!["casts the first argument".to_string()],
        });
        out.push(NearMiss {
            kind: "nullif-first-argument-cast",
            text: format!("NULLIF(0, {numeric}) + NULLIF({int}, 1.5)"),
            needles: vec!["casts the first argument".to_string()],
        });
    }
    if let Some(int) = int {
        out.push(NearMiss {
            kind: "nullif-arity",
            text: format!("NULLIF({int})"),
            needles: vec!["nullif".to_string()],
        });
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
                // The refusal names the whole run, quoted.
                needles: vec![format!("'{symbol}'")],
            });
        }
        // Operator spellings Postgres lexes differently from a naive
        // character-at-a-time reading: a two-character comparison split by
        // whitespace is a syntax error there, and an adjacent run with no
        // operator (`<=>`, `!=-`, `%-`) is one undefined operator, never two.
        for (text, needle) in [
            (format!("{a} < = {b}"), "'< ='"),
            (format!("{a} > = {b}"), "'> ='"),
            (format!("{a} ! = {b}"), "'! ='"),
            (format!("{a} < > {b}"), "'< >'"),
            (format!("{a} <=> {b}"), "'<=>'"),
            (format!("{a} == {b}"), "'=='"),
            (format!("{a} => {b}"), "'=>'"),
            (format!("{a}!=-1"), "'!=-'"),
            (format!("{a}%-1"), "'%-'"),
            (format!("{a} =< {b}"), "'=<'"),
        ] {
            out.push(NearMiss {
                kind: "operator-lexing",
                text,
                needles: vec![needle.to_string()],
            });
        }
        // Numeric literals lexed as Postgres's `scan.l` does: a number
        // directly followed by an identifier character is trailing junk
        // (refused by 15 and later), and the forms Postgres reads that the
        // grammar does not implement are refused by name.
        for text in [
            format!("{a} > 1AND {b} < 2"),
            format!("CASE WHEN {a} > 1THEN 2 ELSE 3 END"),
            "1IS NULL".to_string(),
            format!("{a} + 1AS x"),
            format!("{a} + 1.5AND {b}"),
            format!("{a} + 1.a"),
            format!("{a} + 1e"),
            format!("{a} + 1e3AND {b}"),
            format!("{a} + 12abc"),
        ] {
            out.push(NearMiss {
                kind: "numeric-literal",
                text,
                needles: vec!["trailing junk".to_string()],
            });
        }
        for text in [
            format!("{a} + 1e3"),
            format!("{a} + 1.5E-3"),
            format!("{a} + 0x1F"),
            format!("{a} + 1_000"),
        ] {
            out.push(NearMiss {
                kind: "numeric-literal",
                text,
                needles: vec!["numeric literal".to_string()],
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
    date_near_misses(columns, &mut out);
    // A word Postgres reserves is no column name (ADR-0022), in the position
    // an operand takes and next to an operator. The needle is the word.
    for word in registry::RESERVED_WORDS
        .iter()
        .filter(|w| !matches!(**w, "true" | "false" | "null"))
    {
        for text in [word.to_string(), format!("{word} + 1")] {
            out.push(NearMiss {
                kind: "reserved-word",
                text,
                needles: vec![word.to_string()],
            });
        }
    }
    null_near_misses(columns, &mut out);
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

/// The `NULL`s define can't type (Postgres finds them ambiguous, or types
/// them by candidate rules define doesn't model), each refused with the
/// `CAST` remedy: both operands of an arithmetic operator, the operand of
/// prefix `-` and `AT TIME ZONE`, an operand beside a type no operator takes
/// on both sides, and every argument of every function.
fn null_near_misses(columns: &[Column], out: &mut Vec<NearMiss>) {
    let needles = || vec!["cast(null as <type>)".to_string()];
    let mut push = |kind: &'static str, text: String| {
        out.push(NearMiss {
            kind,
            text,
            needles: needles(),
        });
    };
    push("untyped-null", "-NULL".to_string());
    push("untyped-null", "NULL AT TIME ZONE 'UTC'".to_string());
    let mut types: Vec<ValueType> = columns.iter().map(|(_, t)| *t).collect();
    types.dedup();
    for spec in registry::OPERATORS {
        let op = spec.operator;
        if op.is_comparison() || matches!(op, Operator::And | Operator::Or) {
            continue;
        }
        push("untyped-null", format!("NULL {} NULL", spec.symbol));
        for &ty in &types {
            if registry::operator_result_type(op, ty, ty).is_some() {
                continue;
            }
            if let Some(col) = column_of(columns, ty) {
                push("untyped-null", format!("{col} {} NULL", spec.symbol));
                push("untyped-null", format!("NULL {} {col}", spec.symbol));
            }
        }
    }
    for spec in registry::FUNCTIONS {
        let args: Option<Vec<String>> = spec
            .arg_types
            .iter()
            .map(|t| column_of(columns, *t).map(str::to_string))
            .collect();
        let Some(args) = args else { continue };
        for i in 0..args.len() {
            let mut with_null = args.clone();
            with_null[i] = "NULL".to_string();
            push(
                "untyped-null",
                format!("{}({})", spec.name.to_lowercase(), with_null.join(", ")),
            );
        }
    }
}

/// The near misses of the date constructs: the traps each one has a named
/// refusal for. The typed operand/field matrices come from the registry;
/// what is listed here is what it cannot say.
fn date_near_misses(columns: &[Column], out: &mut Vec<NearMiss>) {
    let Some(date) = column_of(columns, DATE) else {
        return;
    };
    let ts = column_of(columns, TIMESTAMP);
    let tz = column_of(columns, ValueType::Other(PgType::TimestampTz));
    let text = column_of(columns, ValueType::Text);
    let mut miss = |kind: &'static str, text: String, needles: &[&str]| {
        out.push(NearMiss {
            kind,
            text,
            needles: needles.iter().map(|n| n.to_lowercase()).collect(),
        });
    };
    // A field the type does not have, or that is refused for every type.
    for field in registry::EXTRACT_CLOCK_FIELDS {
        miss(
            "extract-field-of-a-date",
            format!("EXTRACT({field} FROM {date})"),
            &["extract", field],
        );
    }
    for field in registry::EXTRACT_REFUSED_FIELDS {
        miss(
            "extract-refused-field",
            format!("EXTRACT({field} FROM {date})"),
            &["extract", field],
        );
        if let Some(ts) = ts {
            miss(
                "extract-refused-field",
                format!("EXTRACT({field} FROM {ts})"),
                &["extract", field],
            );
        }
    }
    for field in ["years", "y", "bogus", "dayofweek"] {
        miss(
            "extract-unknown-field",
            format!("EXTRACT({field} FROM {date})"),
            &["extract", field],
        );
    }
    // The STABLE overloads Postgres picks silently.
    if let Some(tz) = tz {
        miss(
            "extract-timestamptz",
            format!("EXTRACT(year FROM {tz})"),
            &["extract", "at time zone"],
        );
        miss(
            "date-trunc-timestamptz",
            format!("DATE_TRUNC('day', {tz})"),
            &["date_trunc", "at time zone"],
        );
        miss(
            "cast-timestamptz-to-timestamp",
            format!("CAST({tz} AS timestamp)"),
            &["timestamp", "at time zone"],
        );
    }
    miss(
        "date-trunc-date",
        format!("DATE_TRUNC('month', {date})"),
        &["date_trunc", "cast"],
    );
    // A unit that is not one, and one that is not a literal.
    for unit in ["days", "mon", "dow", "", "timezone"] {
        if let Some(ts) = ts {
            miss(
                "date-trunc-unit",
                format!("DATE_TRUNC('{unit}', {ts})"),
                &["date_trunc", "units admitted"],
            );
        }
    }
    if let (Some(ts), Some(text)) = (ts, text) {
        miss(
            "date-trunc-unit-not-literal",
            format!("DATE_TRUNC({text}, {ts})"),
            &["date_trunc", "literal"],
        );
    }
    // A zone that is not the literal 'UTC'.
    if let Some(tz) = tz {
        for zone in ["America/New_York", "utc", "+01:00", "Europe/Paris", ""] {
            miss(
                "at-time-zone-zone",
                format!("{tz} AT TIME ZONE '{zone}'"),
                &["at time zone", "'utc'"],
            );
        }
        if let Some(text) = text {
            miss(
                "at-time-zone-not-literal",
                format!("{tz} AT TIME ZONE {text}"),
                &["at time zone", "literal"],
            );
        }
        miss(
            "at-time-zone-twice",
            format!("({tz} AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'"),
            &["at time zone", "timestamptz"],
        );
    }
    if let Some(ts) = ts {
        miss(
            "at-time-zone-timestamp",
            format!("{ts} AT TIME ZONE 'UTC'"),
            &["at time zone", "timestamptz"],
        );
        miss(
            "interval-difference",
            format!("{ts} - {ts}"),
            &["'-'", "interval", "extract(epoch"],
        );
    }
    miss(
        "at-time-zone-date",
        format!("{date} AT TIME ZONE 'UTC'"),
        &["at time zone", "cast(d as timestamp)"],
    );
    // Date arithmetic with an operand Postgres has no operator for.
    miss(
        "date-plus-date",
        format!("{date} + {date}"),
        &["'+'", "two dates"],
    );
    if let Some(big) = column_of(columns, ValueType::Integer(IntWidth::Int8)) {
        miss(
            "date-plus-bigint",
            format!("{date} + {big}"),
            &["'+'", "bigint", "smallint or integer"],
        );
    }
    if let Some(n) = column_of(columns, ValueType::Numeric) {
        miss(
            "date-plus-numeric",
            format!("{date} - {n}"),
            &["'-'", "numeric"],
        );
    }
    if let Some(int) = column_of(columns, ValueType::Integer(IntWidth::Int4)) {
        miss(
            "integer-minus-date",
            format!("{int} - {date}"),
            &["'-'", "number minus a date"],
        );
    }
    // A constant date past `date`'s range, folded at plan time.
    miss(
        "constant-date-overflow",
        "DATE '2024-02-29' + 2147483647".to_string(),
        &["out of range", "date"],
    );
    miss(
        "constant-date-underflow",
        "DATE '2024-02-29' - 2147483647".to_string(),
        &["out of range", "date"],
    );
    miss(
        "constant-date-overflow",
        "2147483647 + DATE '0001-01-01'".to_string(),
        &["out of range", "date"],
    );
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
        (any::<bool>(), any::<bool>(), any::<bool>(), 0u8..=2).prop_map(
            |(minimal, lower_names, cast_form, cast_alias)| TextStyle {
                minimal,
                lower_names,
                cast_form,
                cast_alias,
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
            let keywords = constant_keyword_literals(ty);
            if !keywords.is_empty() {
                leaves.push((1, prop::sample::select(keywords).boxed()));
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
                    let (op, l) = (*op, self.expr(*lhs, depth)?);
                    // The zone of `AT TIME ZONE` is the literal, not a draw.
                    let literal_rhs = LITERAL_RHS_OPERATORS.iter().find(|(o, _)| *o == op);
                    let r = match literal_rhs {
                        Some((_, zone)) => Just(Expr::StringLiteral((*zone).to_string())).boxed(),
                        None => self.expr(*rhs, depth)?,
                    };
                    // A bare `NULL` takes the other operand's type, so an
                    // operator over one type on both sides takes it on
                    // either side.
                    let nullable = self.profile.bare_null && lhs == rhs && literal_rhs.is_none();
                    Some(
                        (l, r, 0..8u8)
                            .prop_map(move |(lhs, rhs, roll)| {
                                let (lhs, rhs) = match roll {
                                    0 if nullable => (Expr::Null, rhs),
                                    1 if nullable => (lhs, Expr::Null),
                                    _ => (lhs, rhs),
                                };
                                Expr::BinaryOp {
                                    op,
                                    lhs: Box::new(lhs),
                                    rhs: Box::new(rhs),
                                }
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
                        let arg = if registry::INTEGER_LITERAL_ARGS.contains(&(*name, i)) {
                            prop::sample::select(SCALE_LITERALS.to_vec())
                                .prop_map(|n| Expr::NumberLiteral(n.to_string()))
                                .boxed()
                        } else if *name == "DATE_TRUNC" && i == 0 {
                            prop::sample::select(registry::DATE_TRUNC_UNITS.to_vec())
                                .prop_map(|u| Expr::StringLiteral(u.to_string()))
                                .boxed()
                        } else if LITERAL_ONLY_ARGS.contains(&(*name, i)) {
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
                Rule::Negate { ty } => {
                    // A minus applied to a number literal is the negative
                    // literal (as Postgres reads it), so a literal operand
                    // becomes that literal. Postgres types a literal by its
                    // value, so a draw whose sign flip changes the type
                    // (`2147483648` is a `bigint`, `-2147483648` an
                    // `integer`) is dropped: the rule promised `ty`.
                    let operand = self.expr(*ty, depth)?;
                    Some(
                        operand
                            .prop_filter_map("a sign flip that keeps the type", |e| match e {
                                Expr::NumberLiteral(text) => {
                                    let flipped = match text.strip_prefix('-') {
                                        Some(positive) => positive.to_string(),
                                        None => format!("-{text}"),
                                    };
                                    (number_literal_type(&flipped) == number_literal_type(&text))
                                        .then_some(Expr::NumberLiteral(flipped))
                                }
                                e => Some(Expr::Negate(Box::new(e))),
                            })
                            .boxed(),
                    )
                }
                Rule::Cast { from, to } => {
                    let to = *to;
                    let operand = self.expr(*from, depth)?;
                    Some(
                        operand
                            .prop_map(move |e| Expr::Cast {
                                expr: Box::new(e),
                                target: to,
                            })
                            .boxed(),
                    )
                }
                Rule::Extract { operand } => {
                    let fields = registry::extract_fields(*operand)?;
                    let operand = self.expr(*operand, depth)?;
                    Some(
                        (prop::sample::select(fields), operand)
                            .prop_map(|(field, operand)| Expr::FunctionCall {
                                name: "EXTRACT".to_string(),
                                args: vec![Expr::StringLiteral(field.to_string()), operand],
                            })
                            .boxed(),
                    )
                }
                Rule::Not => {
                    let operand = self.expr(ValueType::Boolean, depth)?;
                    Some(operand.prop_map(|e| Expr::Not(Box::new(e))).boxed())
                }
                Rule::IsNull { ty } => {
                    let operand = self.expr(*ty, depth)?;
                    Some(
                        (operand, any::<bool>())
                            .prop_map(|(e, negated)| Expr::IsNull {
                                expr: Box::new(e),
                                negated,
                            })
                            .boxed(),
                    )
                }
                Rule::Case { ty } => {
                    let condition = self.expr(ValueType::Boolean, depth)?;
                    let result = self.expr(*ty, depth)?;
                    Some(
                        (
                            prop::collection::vec((condition, result.clone()), 1..=3),
                            prop::option::of(result),
                        )
                            .prop_map(|(branches, otherwise)| Expr::Case {
                                branches,
                                otherwise: otherwise.map(Box::new),
                            })
                            .boxed(),
                    )
                }
                Rule::CaseMixed { then, other, .. } => {
                    let condition = self.expr(ValueType::Boolean, depth)?;
                    let (then, other) = (self.expr(*then, depth)?, self.expr(*other, depth)?);
                    Some(
                        (condition, then, other, any::<bool>())
                            .prop_map(|(condition, then, other, swap)| {
                                // Either order: the unified type does not
                                // depend on which result comes first.
                                let (first, second) =
                                    if swap { (other, then) } else { (then, other) };
                                Expr::Case {
                                    branches: vec![(condition, first)],
                                    otherwise: Some(Box::new(second)),
                                }
                            })
                            .boxed(),
                    )
                }
                Rule::NullIf { lhs, rhs } => {
                    let (a, b) = (self.expr(*lhs, depth)?, self.expr(*rhs, depth)?);
                    Some(
                        (a, b)
                            .prop_map(|(a, b)| Expr::FunctionCall {
                                name: "NULLIF".to_string(),
                                args: vec![a, b],
                            })
                            .boxed(),
                    )
                }
                Rule::NullIfLiteral { float } => {
                    let a = self.expr(ValueType::Float(*float), depth)?;
                    let literal = prop::sample::select(comparison_literals(self.profile.magnitude));
                    Some(
                        (a, literal)
                            .prop_map(|(a, lit)| Expr::FunctionCall {
                                name: "NULLIF".to_string(),
                                args: vec![a, lit],
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
