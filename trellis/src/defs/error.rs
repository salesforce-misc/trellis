//! Errors the definition parser can produce. Plain enum + `Display`, no
//! `thiserror`/`anyhow`, matching [`crate::error::Error`]'s convention.
//!
//! Each variant names the specific unsupported construct rather than
//! reporting a generic parse failure, per issue #22's requirement that
//! rejections be actionable.

use std::fmt;

use crate::error_code::ErrorCode;

#[derive(Debug, Clone, PartialEq, Eq)]
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
    /// An operator other than `+`.
    UnsupportedOperator { operator: String },
    /// A function call other than one in [`super::registry::FUNCTIONS`].
    UnsupportedFunction { name: String },
    /// `COUNT(<column>)` or `COUNT()` — issue #75 only implements row-
    /// counting `COUNT(*)`, not Postgres's "count non-null occurrences of
    /// this column" form.
    UnsupportedAggregateFunction { name: String },
    /// A registered function called with the wrong number of arguments
    /// (issue #64) — a structural check the parser can make directly from
    /// [`super::registry::FunctionSpec::arg_types`]'s length, without
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
    /// A `TRANSFORM`/`FROM` table reference (issue #76, ADR-0007 grammar
    /// clause 4) had more than the two `<schema>.<table>` components this
    /// grammar accepts — e.g. `a.b.c`.
    TooManyQualifiedNameParts { reference: String },
    /// A `CAST(<expr> AS <type>)` whose operand is not a single-quoted
    /// string literal (issue #109) — i.e. a request for a **general** cast
    /// rather than a typed literal. See [`super::typed_literal`] for why the
    /// coercion lattice a general cast implies is each type family's own
    /// epic child, not this grammar's.
    UnsupportedCast { found: String },
    /// A `CAST(<literal> AS <type>)` or `<type> '<literal>'` naming a type
    /// that isn't in [`super::typed_literal::TYPED_LITERALS`]'s allowlist.
    UnsupportedLiteralType { name: String },
    /// Postgres's `<expr>::<type>` cast sugar, which this grammar doesn't
    /// accept (issue #109) — rejected by name so the message can point at
    /// `CAST(...)`/`<type> '...'` instead of reporting an unexpected `:`.
    UnsupportedCastOperator,
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
                "unsupported operator '{operator}': only '+' and '>' are currently implemented (ADR-0004: grammar and evaluator function sets must match)"
            ),
            ParseError::UnsupportedFunction { name } => write!(
                f,
                "unsupported function '{name}': not in the registered function set (ADR-0004: grammar and evaluator function sets must match)"
            ),
            ParseError::UnsupportedAggregateFunction { name } => write!(
                f,
                "unsupported aggregate function '{name}': COUNT is only supported as COUNT(*) (row-counting); COUNT(<column>) is not implemented"
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
            ParseError::TooManyQualifiedNameParts { reference } => write!(
                f,
                "'{reference}' is not a valid table reference: only a bare <table> or a \
                 qualified <schema>.<table> is supported (issue #76), not a third '.'-separated \
                 part"
            ),
            ParseError::UnsupportedCast { found } => write!(
                f,
                "CAST is only supported over a single-quoted literal — \
                 CAST('<literal>' AS <type>), equivalently <type> '<literal>' — found {found}. \
                 General casts between expression types are not implemented: each type family's \
                 coercions are decided by its own issue (#113 temporal, #114 bytea, #115 jsonb), \
                 since most interesting pairs are not immutable (ADR-0004)"
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
            ParseError::UnsupportedCastOperator => write!(
                f,
                "the '::' cast operator is not supported: write a typed literal as \
                 <type> '<literal>' (e.g. DATE '2024-01-01') or CAST('<literal>' AS <type>)"
            ),
        }
    }
}

impl std::error::Error for ParseError {}
