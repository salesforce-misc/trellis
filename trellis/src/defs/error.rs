//! Errors the definition parser can produce. Plain enum + `Display`, no
//! `thiserror`/`anyhow`, matching [`crate::error::Error`]'s convention.
//!
//! Each variant names the specific unsupported construct rather than
//! reporting a generic parse failure, so that rejections are actionable.

use std::fmt;

use crate::error_code::ErrorCode;

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ParseError {
    /// The lexer found a character it doesn't know how to tokenize.
    UnexpectedChar { found: char },
    /// A single-quoted string literal was opened but never closed.
    UnterminatedString,
    /// The parser expected a specific token and found something else.
    UnexpectedToken { expected: String, found: String },
    /// The input ended before a required token was found.
    UnexpectedEof { expected: String },
    /// A construct outside the 1-1 key-space this grammar slice supports
    /// (aggregate `GROUP BY`, cross-join `JOIN`).
    UnsupportedKeySpace { construct: String, detail: String },
    /// An operator `super::registry::OPERATORS` does not list.
    UnsupportedOperator { operator: String },
    /// A function call other than one in `super::registry::FUNCTIONS`.
    UnsupportedFunction { name: String },
    /// A registered function called with the wrong number of arguments
    /// — a structural check the parser can make directly from
    /// `super::registry::FunctionSpec::arg_types`'s length, without
    /// needing argument type information (that's the validator's job).
    FunctionArityMismatch {
        name: String,
        expected: usize,
        found: usize,
    },
    /// An identifier known to be non-immutable (`NOW()`, `RANDOM()`, ...).
    NonImmutableConstruct { name: String },
    /// A partial-data predicate other than a literal `TRUE`.
    UnsupportedPredicate { detail: String },
    /// `COALESCE` called with zero arguments.
    AtLeastOneArgumentRequired { name: String },
    /// A `TRANSFORM`/`FROM` table reference (ADR-0011 grammar
    /// clause 4) had more than the two `<schema>.<table>` components this
    /// grammar accepts — e.g. `a.b.c`.
    TooManyQualifiedNameParts { reference: String },
    /// A column reference in an expression had more than the three
    /// `<schema>.<table>.<column>` components a qualified source column takes
    /// — e.g. `a.b.c.d`.
    TooManyColumnNameParts { reference: String },
    /// A two-part `GROUP BY` key `<head>.<column>` of a non-`JOIN` aggregate
    /// whose head is not the `FROM` table. `source` is the `FROM` table's bare
    /// name. A key of another table is grouped by with a `JOIN`.
    GroupByKeyNotFromSource { reference: String, source: String },
    /// A `CAST(<expr> AS <type>)` naming a type that is not one of
    /// `super::registry::CAST_TARGETS`, or naming a modifier (`numeric(10,2)`).
    /// `name` is the target as written. Whether the operand converts to a
    /// target is the validator's to check
    /// ([`super::validate::ValidationError::UnsupportedCast`]).
    UnsupportedCastTarget { name: String },
    /// A run of prefix `-` operators longer than the parser keeps.
    PrefixMinusChainTooLong { max: usize },
    /// A two-character comparison split by whitespace (`a < = b`, `a ! = b`).
    /// Postgres lexes `<=`, `>=`, `<>` and `!=` only when adjacent, so the
    /// split form is a syntax error there and here.
    SplitOperator { found: String, joined: String },
    /// Two adjacent `-` characters, which start a comment in SQL.
    DoubleMinus,
    /// `a < b < c`, `a = b = c` and the like: Postgres's comparisons (and
    /// `IS [NOT] DISTINCT FROM`) are non-associative, so a second one at the
    /// same level is a syntax error there and here.
    NonAssociativeComparison { operator: String },
    /// `IS TRUE`, `IS NOT FALSE`, `IS UNKNOWN` and the like. Only `IS [NOT]
    /// NULL` and `IS [NOT] DISTINCT FROM` are admitted.
    UnsupportedIsForm { found: String },
    /// A run of prefix `NOT` operators longer than the parser keeps.
    NotChainTooLong { max: usize },
    /// `CASE <operand> WHEN <value> THEN ...`, the simple form.
    SimpleCaseUnsupported,
    /// A `CAST(<literal> AS <type>)` or `<type> '<literal>'` naming a type
    /// that isn't in `super::typed_literal::TYPED_LITERALS`'s allowlist.
    UnsupportedLiteralType { name: String },
    /// Postgres's `<expr>::<type>` cast sugar, which this grammar doesn't
    /// accept — rejected by name so the message can point at
    /// `CAST(...)`/`<type> '...'` instead of reporting an unexpected `:`.
    UnsupportedCastOperator,
    /// An expression nested deeper than `super::parser::MAX_EXPRESSION_DEPTH`.
    ExpressionTooDeep { limit: usize },
    /// A `PAUSE`/`RESUME`/`DROP` address whose shape doesn't fit the statement
    /// it was written in: too many `.`-separated parts, or a
    /// `<transform>.<column>` address under `DROP TRANSFORM`, which is
    /// whole-definition only. `statement` names the statement form
    /// (`"DROP TRANSFORM"`), `address` is the address as written, and `detail`
    /// says what shape *is* accepted there.
    MalformedDefinitionAddress {
        statement: String,
        address: String,
        detail: String,
    },
    /// A stored definition was written under a grammar version older than the
    /// oldest this binary reads (ADR-0018). The fix is to drop the transform
    /// and define it again.
    GrammarVersionTooOld { stored: i32, oldest: i32 },
    /// A stored definition was written under a grammar version newer than
    /// this binary knows (ADR-0018): a downgrade, or an older binary running
    /// beside a newer one. Nothing is read or written.
    GrammarVersionTooNew { stored: i32, current: i32 },
}

impl ParseError {
    /// This error's stable, coarse [`ErrorCode`] category (`docs/decisions/0008-public-api-design.md`,
    /// decision 3). Every variant here is a rejection of the input text
    /// itself, so this is always [`ErrorCode::Parse`].
    pub fn code(&self) -> ErrorCode {
        ErrorCode::Parse
    }
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseError::GrammarVersionTooOld { stored, oldest } => write!(
                f,
                "the stored definition was written under grammar version {stored}, and this \
                 binary reads grammar versions {oldest} and later; drop the transform and \
                 define it again"
            ),
            ParseError::GrammarVersionTooNew { stored, current } => write!(
                f,
                "the stored definition was written under grammar version {stored}, newer than \
                 the {current} this binary reads; run a binary that reads grammar version {stored}"
            ),
            ParseError::UnexpectedChar { found } => {
                write!(f, "unexpected character '{found}' in transform definition")
            }
            ParseError::UnterminatedString => {
                write!(f, "unterminated string literal in transform definition")
            }
            ParseError::UnexpectedToken { expected, found } => {
                write!(f, "expected {expected}, found {found}")
            }
            ParseError::UnexpectedEof { expected } => {
                write!(f, "expected {expected}, found end of input")
            }
            ParseError::UnsupportedKeySpace { construct, detail } => {
                write!(f, "unsupported key-space construct '{construct}': {detail}")
            }
            ParseError::UnsupportedOperator { operator } => write!(
                f,
                "unsupported operator '{operator}': only {} are currently implemented (ADR-0004: grammar and renderer function sets must match)",
                crate::defs::registry::OPERATORS
                    .iter()
                    .map(|spec| format!("'{}'", spec.symbol))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            ParseError::UnsupportedFunction { name } => write!(
                f,
                "unsupported function '{name}': not in the registered function set (ADR-0004: grammar and renderer function sets must match)"
            ),
            ParseError::FunctionArityMismatch {
                name,
                expected,
                found,
            } => write!(
                f,
                "function '{name}' expects {expected} argument(s), found {found}"
            ),
            ParseError::NonImmutableConstruct { name } => write!(
                f,
                "'{name}' is not immutable and cannot be used in a calculated field (docs/transforms.md#calculated-fields requires immutable functions/operators only)"
            ),
            ParseError::UnsupportedPredicate { detail } => {
                write!(f, "unsupported partial-data predicate: {detail}")
            }
            ParseError::AtLeastOneArgumentRequired { name } => {
                write!(f, "function '{name}' requires at least 1 argument")
            }
            ParseError::TooManyColumnNameParts { reference } => write!(
                f,
                "'{reference}' is not a valid column reference: a column is written \
                 <column>, <table>.<column> or <schema>.<table>.<column> (the FROM table's)"
            ),
            ParseError::GroupByKeyNotFromSource { reference, source } => write!(
                f,
                "GROUP BY key '{reference}' does not name this transform's FROM table \
                 ('{source}'); a qualified key must name the FROM table, and a second table is \
                 read with a JOIN"
            ),
            ParseError::TooManyQualifiedNameParts { reference } => write!(
                f,
                "'{reference}' is not a valid table reference: only a bare <table> or a \
                 qualified <schema>.<table> is supported, not a third '.'-separated \
                 part"
            ),
            ParseError::UnsupportedCastTarget { name } => write!(
                f,
                "'{name}' is not a CAST target: only {} are supported, an integer to a wider \
                 integer or to numeric, or a date to timestamp, with no modifier \
                 (docs/type-support.md)",
                crate::defs::registry::CAST_TARGETS
                    .iter()
                    .map(|t| t.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            ParseError::NonAssociativeComparison { operator } => write!(
                f,
                "'{operator}' follows another comparison: comparisons do not chain in SQL \
                 (a < b < c is a syntax error). Join them with AND, as in a < b AND b < c, \
                 or add parentheses"
            ),
            ParseError::UnsupportedIsForm { found } => write!(
                f,
                "unsupported IS form '{found}': only IS [NOT] NULL and IS [NOT] DISTINCT FROM \
                 are supported; COALESCE(x, <fallback>) covers the truth-value tests \
                 (IS [NOT] TRUE, FALSE, UNKNOWN)"
            ),
            ParseError::NotChainTooLong { max } => write!(
                f,
                "more than {max} prefix NOT operators in a row; write the expression with fewer"
            ),
            ParseError::SimpleCaseUnsupported => write!(
                f,
                "the simple CASE form (CASE <operand> WHEN <value> THEN ...) is not supported: \
                 write the searched form, CASE WHEN <operand> = <value> THEN ... END"
            ),
            ParseError::PrefixMinusChainTooLong { max } => write!(
                f,
                "more than {max} prefix '-' operators in a row; write the expression with fewer"
            ),
            ParseError::UnsupportedLiteralType { name } => write!(
                f,
                "'{name}' is not a type a literal can be spelled as: only {} are supported \
                 (see docs/type-support.md and docs/decisions/0004-transform-definition-grammar.md)",
                crate::defs::typed_literal::TYPED_LITERALS
                    .iter()
                    .map(|spec| spec.keyword)
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            ParseError::ExpressionTooDeep { limit } => write!(
                f,
                "expression nested too deeply: an expression may nest at most {limit} levels (a chain of {limit} operands joined by operators, or fewer nested parentheses or calls); split it into calculated fields"
            ),
            ParseError::SplitOperator { found, joined } => write!(
                f,
                "'{found}' is not an operator: the characters of a two-character operator must be adjacent, as in Postgres; write '{joined}'"
            ),
            ParseError::DoubleMinus => write!(
                f,
                "'--' starts a comment in SQL, and comments are not supported: separate the \
                 two minuses with a space or parentheses (a - -1, -(-a))"
            ),
            ParseError::UnsupportedCastOperator => write!(
                f,
                "the '::' cast operator is not supported: write a typed literal as \
                 <type> '<literal>' (e.g. DATE '2024-01-01') or CAST('<literal>' AS <type>)"
            ),
            ParseError::MalformedDefinitionAddress {
                statement,
                address,
                detail,
            } => write!(
                f,
                "'{address}' is not a valid {statement} address: {detail}"
            ),
        }
    }
}

impl std::error::Error for ParseError {}
