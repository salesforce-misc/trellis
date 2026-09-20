//! Recursive-descent parser for the transform-definition grammar.
//!
//! Concrete syntax (documented in full in `docs/decisions/0004-transform-definition-grammar.md`):
//!
//! ```text
//! TRANSFORM <target>
//! FROM <source>
//! SELECT <expr> AS <field> [, <expr> AS <field> ...]
//! [WHERE <predicate>]
//! ```
//!
//! `<target>` and `<source>` (issue #76, ADR-0007 grammar clause 4) each
//! accept either a bare `<table>` (resolved once, later, via `search_path` —
//! issues #72/#73) or an explicit `<schema>.<table>` spelling that names its
//! schema directly and skips that resolution — see [`Self::parse_table_ref`]
//! for the shared parsing logic and why it doesn't collide with
//! `<rel>.<column>` relationship-path syntax elsewhere in this grammar despite
//! reusing the same `ident '.' ident` token shape.
//!
//! `FROM <source>` is where a future aggregate (`GROUP BY <cols>`) or
//! cross-join (`JOIN <other> ON <cond>`) key-space clause will slot in;
//! this slice only accepts the 1-1 case (clause absent) and rejects both
//! keywords by name if present. `<expr>` supports column references,
//! numeric and string literals, `+`, `>` (issue #65), parenthesized
//! grouping (`(<expr>)`, issue #67 — needed once a caller composes `+` and
//! `>` and must override the precedence table's own grouping), `name(args)`
//! function calls against [`super::registry::FUNCTIONS`] (issue #64), and
//! `<rel>.<column>` relationship-path references (issue #25, ADR-0006) —
//! whose head is a relationship name, resolved and cardinality-checked by
//! later issues, not this parser. Issue #109 adds **typed literals**, spelled
//! either `<type> '<text>'` or `CAST('<text>' AS <type>)` — see
//! [`super::typed_literal`] for the allowlisted types, why both spellings
//! build one AST node, and why `<expr>::<type>` and general casts are
//! rejected. `<predicate>` accepts only the literal `TRUE`.
//!
//! A second, standalone statement form (ADR-0006, issue #24) declares a
//! named relationship rather than a transform:
//!
//! ```text
//! RELATIONSHIP <name> FROM <from_table>.<fk_col> TO <to_table>.<pk_col>
//! ```
//!
//! It's parsed by [`parse_relationship`], a sibling entry point to [`parse`]
//! rather than a case [`parse`] itself dispatches on — see [`parse`]'s doc
//! comment for why. This slice is grammar + AST only: referencing a
//! relationship from a calculated field, cardinality validation, and catalog
//! storage are all separate, later issues.

use super::ast::{Expr, FieldDef, GroupByKey, KeySpace, Predicate, RelationshipDef, TransformDef};
use super::error::ParseError;
use super::lexer::{Token, lex};
use super::registry::{
    AGGREGATE_FUNCTIONS, NON_IMMUTABLE_NAMES, lookup_aggregate_function, lookup_function,
    lookup_operator, operator_spec,
};
use super::typed_literal::lookup_typed_literal;

const OPERATOR_CHARS: &[char] = &['+', '-', '*', '/', '%', '>', '<', '='];

/// Parses a transform definition's source text into a [`TransformDef`].
///
/// This entry point is unchanged by issue #24's `RELATIONSHIP` statement: it
/// stays `TransformDef`-typed so existing callers (the catalog, the tests
/// above) don't need to unwrap a statement-kind enum. See
/// [`parse_relationship`] for the sibling entry point that parses the new
/// standalone-relationship grammar; both share the same lexer and error type,
/// and a caller that doesn't yet know which kind of statement it has can
/// peek the first token itself (`RELATIONSHIP` vs. `TRANSFORM`) to choose
/// between them, the same check [`parse_relationship`] makes internally.
pub fn parse(input: &str) -> Result<TransformDef, ParseError> {
    let tokens = lex(input)?;
    Parser {
        tokens,
        pos: 0,
        is_aggregate: false,
    }
    .parse_transform_def()
}

/// Parses a standalone relationship declaration's source text into a
/// [`RelationshipDef`] (ADR-0006, issue #24):
///
/// ```text
/// RELATIONSHIP <name> FROM <from_table>.<fk_col> TO <to_table>.<pk_col>
/// ```
pub fn parse_relationship(input: &str) -> Result<RelationshipDef, ParseError> {
    let tokens = lex(input)?;
    Parser {
        tokens,
        pos: 0,
        is_aggregate: false,
    }
    .parse_relationship_def()
}

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
    /// Set once [`Self::parse_key_space_clause`] has parsed a `GROUP BY`.
    /// Gates whether [`Self::parse_primary`] will accept a
    /// `SUM`/`MIN`/`MAX`/`AVG` call — a plain field rather than threading
    /// key-space through every expression-parsing method, since it's fixed
    /// for the whole statement by the time fields are parsed.
    is_aggregate: bool,
}

impl Parser {
    fn peek(&self) -> &Token {
        &self.tokens[self.pos]
    }

    fn advance(&mut self) -> Token {
        let tok = self.tokens[self.pos].clone();
        if self.pos + 1 < self.tokens.len() {
            self.pos += 1;
        }
        tok
    }

    fn expect_keyword(&mut self, kw: &str) -> Result<(), ParseError> {
        match self.advance() {
            Token::Ident(s) if s.eq_ignore_ascii_case(kw) => Ok(()),
            Token::Eof => Err(ParseError::UnexpectedEof {
                expected: format!("'{kw}'"),
            }),
            other => Err(ParseError::UnexpectedToken {
                expected: format!("'{kw}'"),
                found: other.describe(),
            }),
        }
    }

    fn expect_ident(&mut self) -> Result<String, ParseError> {
        match self.advance() {
            Token::Ident(s) => Ok(s),
            Token::Eof => Err(ParseError::UnexpectedEof {
                expected: "an identifier".to_string(),
            }),
            other => Err(ParseError::UnexpectedToken {
                expected: "an identifier".to_string(),
                found: other.describe(),
            }),
        }
    }

    fn peek_is_keyword(&self, kw: &str) -> bool {
        matches!(self.peek(), Token::Ident(s) if s.eq_ignore_ascii_case(kw))
    }

    fn peek_is_symbol(&self, c: char) -> bool {
        matches!(self.peek(), Token::Symbol(s) if *s == c)
    }

    fn expect_symbol(&mut self, c: char) -> Result<(), ParseError> {
        match self.advance() {
            Token::Symbol(s) if s == c => Ok(()),
            Token::Eof => Err(ParseError::UnexpectedEof {
                expected: format!("'{c}'"),
            }),
            other => Err(ParseError::UnexpectedToken {
                expected: format!("'{c}'"),
                found: other.describe(),
            }),
        }
    }

    /// Parses a dot-qualified `<table>.<col>` reference, the new-relative-to
    /// existing-statement-forms syntax ADR-0006 introduces for a
    /// relationship's endpoints. There's no reuse target in the expression
    /// grammar for this: `parse_primary`'s `a.b` handling (issue #25) builds
    /// an [`Expr::RelationshipPath`] whose head is a relationship name, not
    /// a table — a different semantic than a relationship declaration's
    /// `<table>.<col>` endpoints, so malformed input here still gets its own
    /// report rather than being unified with the expression-grammar path.
    fn expect_table_dot_column(&mut self) -> Result<(String, String), ParseError> {
        let table = self.expect_ident()?;
        self.expect_symbol('.')?;
        let column = self.expect_ident()?;
        Ok((table, column))
    }

    /// Parses a `TRANSFORM`/`FROM` table reference (issue #76, ADR-0007
    /// grammar clause 4): either a bare `<table>` — today's only form,
    /// returned as `(table, None)` — or an explicit `<schema>.<table>`
    /// spelling, returned as `(table, Some(schema))`. Rejects a third
    /// component (`a.b.c`) with a dedicated error naming the whole
    /// over-qualified reference, rather than leaving the trailing `.c`
    /// dangling for a later parse step to trip over with a confusing
    /// "expected end of input".
    ///
    /// **Not ambiguous with [`Expr::RelationshipPath`]'s own `a.b` handling**
    /// in [`Self::parse_primary`], even though both recognize the same
    /// `ident '.' ident` token shape: this method only ever runs immediately
    /// after the `TRANSFORM`/`FROM` keyword, before `SELECT`'s field-expression
    /// list is even reached, so the two never compete for the same tokens.
    /// They *mean* different things by design too — a table reference's `.`
    /// qualifies a table with its schema, while a relationship-path's `.`
    /// addresses a column through a named relationship — but callers of this
    /// method never need to care, since the grammar positions alone already
    /// keep them apart.
    fn parse_table_ref(&mut self) -> Result<(String, Option<String>), ParseError> {
        let first = self.expect_ident()?;
        if !self.peek_is_symbol('.') {
            return Ok((first, None));
        }
        self.advance();
        let second = self.expect_ident()?;
        if !self.peek_is_symbol('.') {
            return Ok((second, Some(first)));
        }
        // Over-qualified (`a.b.c...`): keep consuming `.`-separated
        // components so the error message names the whole reference, instead
        // of bailing after just the third part and leaving the rest to
        // desync the rest of the parse.
        let mut reference = format!("{first}.{second}");
        while self.peek_is_symbol('.') {
            self.advance();
            let extra = self.expect_ident()?;
            reference.push('.');
            reference.push_str(&extra);
        }
        Err(ParseError::TooManyQualifiedNameParts { reference })
    }

    fn parse_transform_def(&mut self) -> Result<TransformDef, ParseError> {
        self.expect_keyword("TRANSFORM")?;
        let (target, explicit_target_schema) = self.parse_table_ref()?;

        self.expect_keyword("FROM")?;
        let (source, explicit_source_schema) = self.parse_table_ref()?;

        let key_space = self.parse_key_space_clause()?;

        self.expect_keyword("SELECT")?;
        let fields = self.parse_field_list()?;

        let predicate = if self.peek_is_keyword("WHERE") {
            self.advance();
            self.parse_predicate()?
        } else {
            Predicate::True
        };

        self.reject_trailing_key_space_clause()?;

        match self.advance() {
            Token::Eof => {}
            other => {
                return Err(ParseError::UnexpectedToken {
                    expected: "end of input".to_string(),
                    found: other.describe(),
                });
            }
        }

        Ok(TransformDef {
            target,
            explicit_target_schema,
            source,
            explicit_source_schema,
            key_space,
            fields,
            predicate,
        })
    }

    /// Parses `RELATIONSHIP <name> FROM <from_table>.<fk_col> TO
    /// <to_table>.<pk_col>` (ADR-0006, issue #24). `RELATIONSHIP`/`FROM`/`TO`
    /// are matched case-insensitively, matching every other keyword in this
    /// grammar (`expect_keyword`); `name` and the four table/column
    /// components are captured as raw identifiers, exactly like
    /// `parse_transform_def`'s `target`/`source` — validating them as real
    /// tables/columns (cardinality, existence, cycles) is deferred to a
    /// later issue per ADR-0006's own scoping.
    fn parse_relationship_def(&mut self) -> Result<RelationshipDef, ParseError> {
        self.expect_keyword("RELATIONSHIP")?;
        let name = self.expect_ident()?;

        self.expect_keyword("FROM")?;
        let (from_table, from_col) = self.expect_table_dot_column()?;

        self.expect_keyword("TO")?;
        let (to_table, to_col) = self.expect_table_dot_column()?;

        match self.advance() {
            Token::Eof => {}
            other => {
                return Err(ParseError::UnexpectedToken {
                    expected: "end of input".to_string(),
                    found: other.describe(),
                });
            }
        }

        Ok(RelationshipDef {
            name,
            from_table,
            from_col,
            to_table,
            to_col,
        })
    }

    /// Parses the key-space clause in its ADR-0004-reserved slot, between
    /// `FROM <source>` and `SELECT`: absent -> [`KeySpace::OneToOne`],
    /// `GROUP BY <key>[, <key>...]` -> [`KeySpace::Aggregate`]. `JOIN` is
    /// rejected outright — cross-join key-spaces aren't supported by any
    /// part of this grammar yet.
    fn parse_key_space_clause(&mut self) -> Result<KeySpace, ParseError> {
        if self.peek_is_keyword("JOIN") {
            return Err(ParseError::UnsupportedKeySpace {
                construct: "JOIN".to_string(),
                detail: "cross-join key-spaces are not supported by this grammar slice \
                    (see issue #22 and docs/transforms.md#granularity)"
                    .to_string(),
            });
        }
        if self.peek_is_keyword("GROUP") {
            self.advance();
            self.expect_keyword("BY")?;
            let mut group_by = vec![self.parse_group_by_key()?];
            while self.peek_is_symbol(',') {
                self.advance();
                group_by.push(self.parse_group_by_key()?);
            }
            self.is_aggregate = true;
            return Ok(KeySpace::Aggregate { group_by });
        }
        Ok(KeySpace::OneToOne)
    }

    /// Parses one `GROUP BY` key (issue #137): a plain `<column>`, or a
    /// to-one relationship path `<rel>.<column>` — the same `ident '.'
    /// ident` shape [`Self::parse_primary`]'s `Expr::RelationshipPath`
    /// handling recognizes, reused here rather than duplicated since a
    /// `GROUP BY` key sits in its own reserved grammar slot (ahead of
    /// `SELECT`) and can never compete with an expression's own tokens.
    /// Whether the head names an actually-declared relationship, and its
    /// cardinality (a to-many path is rejected — grouping by "the many child
    /// rows on the other end of a to-many relationship" has no defined
    /// semantics), is resolved later by the validator, not this parser.
    fn parse_group_by_key(&mut self) -> Result<GroupByKey, ParseError> {
        let first = self.expect_ident()?;
        if self.peek_is_symbol('.') {
            self.advance();
            let column = self.expect_ident()?;
            return Ok(GroupByKey::RelationshipPath { rel: first, column });
        }
        Ok(GroupByKey::Column(first))
    }

    /// Rejects a `JOIN`/`GROUP BY` key-space clause with a construct-specific
    /// error if one starts at the current position — called at the end of
    /// the statement, since SQL-natural placement (`SELECT ... GROUP BY
    /// ...`) would otherwise fall through to a generic "expected end of
    /// input" error rather than naming ADR-0004's actual reserved slot.
    fn reject_trailing_key_space_clause(&self) -> Result<(), ParseError> {
        if self.peek_is_keyword("JOIN") {
            return Err(ParseError::UnsupportedKeySpace {
                construct: "JOIN".to_string(),
                detail: "cross-join key-spaces are not supported by this grammar slice \
                    (see issue #22 and docs/transforms.md#granularity)"
                    .to_string(),
            });
        }
        if self.peek_is_keyword("GROUP") {
            return Err(ParseError::UnsupportedKeySpace {
                construct: "GROUP BY".to_string(),
                detail: "GROUP BY must appear directly after FROM <source> and before \
                    SELECT (ADR-0004's reserved slot), not after SELECT/WHERE"
                    .to_string(),
            });
        }
        Ok(())
    }

    fn parse_field_list(&mut self) -> Result<Vec<FieldDef>, ParseError> {
        let mut fields = Vec::new();
        loop {
            let expr = self.parse_expr()?;
            self.expect_keyword("AS")?;
            let name = self.expect_ident()?;
            fields.push(FieldDef { name, expr });

            if self.peek_is_symbol(',') {
                self.advance();
                continue;
            }
            break;
        }
        Ok(fields)
    }

    /// Parses a binary-operator expression via precedence climbing (a
    /// standard hand-rolled Pratt-parser loop, see [`Self::parse_binary_expr`]),
    /// so `a OP1 b OP2 c` groups the way [`super::registry::OPERATORS`]'s
    /// precedence table (issue #67) says it should, matching a
    /// precedence-aware grammar like Postgres's rather than flat
    /// left-to-right.
    fn parse_expr(&mut self) -> Result<Expr, ParseError> {
        self.parse_binary_expr(0)
    }

    /// Precedence-climbing loop: parses a primary term, then repeatedly
    /// consumes a binary operator whose precedence is `>= min_precedence`,
    /// recursing into the rhs with that operator's precedence + 1.
    ///
    /// Every operator in [`super::registry::OPERATORS`] is left-associative,
    /// so raising the rhs's minimum precedence by 1 (rather than reusing the
    /// same level) stops that recursive call from also swallowing a
    /// same-precedence operator to its right — leaving it for *this* call's
    /// loop instead, which folds it onto the already-built lhs. That's what
    /// keeps a same-precedence chain left-associative (`a + b + c` ->
    /// `(a + b) + c`) while still letting a strictly higher-precedence
    /// operator further right bind its operands first (`a > b + c` ->
    /// `a > (b + c)`, since `+` outranks `>` and so gets pulled into the
    /// `parse_binary_expr(COMPARISON + 1)` call parsing `>`'s rhs).
    fn parse_binary_expr(&mut self, min_precedence: u8) -> Result<Expr, ParseError> {
        let mut lhs = self.parse_primary()?;
        loop {
            let symbol = match self.peek() {
                Token::Symbol(c) if OPERATOR_CHARS.contains(c) => *c,
                _ => break,
            };
            let symbol_str = symbol.to_string();
            let op = match lookup_operator(&symbol_str) {
                Some(op) => op,
                None => {
                    self.advance();
                    return Err(ParseError::UnsupportedOperator {
                        operator: symbol_str,
                    });
                }
            };
            let precedence = operator_spec(op).precedence;
            if precedence < min_precedence {
                break;
            }
            self.advance();
            let rhs = self.parse_binary_expr(precedence + 1)?;
            lhs = Expr::BinaryOp {
                op,
                lhs: Box::new(lhs),
                rhs: Box::new(rhs),
            };
        }
        Ok(lhs)
    }

    fn parse_primary(&mut self) -> Result<Expr, ParseError> {
        match self.advance() {
            Token::Symbol('(') => {
                // Parenthesized grouping (issue #67's precedence-climbing
                // parser needs this for the rare case a caller must override
                // the precedence table's own grouping — e.g. wrapping a
                // `>` comparison as an operand of `+`, which the precedence
                // table alone would never produce since `+` binds tighter).
                // No new `Expr` variant: a grouping paren only steers which
                // subtree `parse_binary_expr` builds around it, it carries no
                // information of its own once parsing is done, so the
                // parenthesized expression's own tree is returned unwrapped.
                let expr = self.parse_expr()?;
                self.expect_symbol(')')?;
                Ok(expr)
            }
            Token::Number(n) => Ok(Expr::NumberLiteral(n)),
            Token::String(s) => Ok(Expr::StringLiteral(s)),
            Token::Ident(name) => {
                let upper = name.to_ascii_uppercase();

                if NON_IMMUTABLE_NAMES.contains(&upper.as_str()) {
                    return Err(ParseError::NonImmutableConstruct { name });
                }

                // A typed literal's `<type> '<text>'` spelling (issue #109).
                // Checked before the `.`/`(` branches below and gated on the
                // *next* token being a string literal, which makes it
                // unambiguous against every other use of an identifier here:
                // a column reference, a relationship-path head and a
                // function name are each followed by end-of-expression, `.`
                // or `(` respectively, never by a string. So a source column
                // genuinely named `date` still parses as a column in
                // `SELECT date AS d` — only `date '...'` is a literal.
                if let Token::String(_) = self.peek() {
                    // An identifier directly followed by a string literal is
                    // a typed-literal attempt and nothing else: everywhere
                    // else an identifier appears in this grammar it's
                    // followed by an identifier (`x AS y`), a `.`
                    // (relationship path), a `(` (function call), an
                    // operator symbol, or end-of-expression — never a
                    // string. So an unallowlisted type keyword here gets the
                    // same purpose-built error the `CAST` spelling gives,
                    // rather than falling through to a confusing "expected
                    // AS, found string 'x'" about a column it never was.
                    let Some(spec) = lookup_typed_literal(&upper) else {
                        return Err(ParseError::UnsupportedLiteralType { name });
                    };
                    let Token::String(text) = self.advance() else {
                        unreachable!("peeked a string literal");
                    };
                    return Ok(Expr::TypedLiteral {
                        pg_type: spec.pg_type,
                        text,
                    });
                }

                if self.peek_is_symbol('.') {
                    self.advance();
                    let column = self.expect_ident()?;
                    return Ok(Expr::RelationshipPath { rel: name, column });
                }

                if self.peek_is_symbol('(') {
                    self.advance();

                    // `CAST('<literal>' AS <type>)` (issue #109) — standard
                    // SQL's spelling of the same constant the `<type>
                    // '<literal>'` form above builds, and the same node,
                    // because Postgres folds both to one `Const`. Handled
                    // ahead of the registry lookups because its `AS` keyword
                    // makes it not a `parse_call_args` argument list at all.
                    if upper == "CAST" {
                        return self.parse_cast_body();
                    }

                    if AGGREGATE_FUNCTIONS.contains(&upper.as_str()) {
                        if !self.is_aggregate {
                            // A to-many relationship enrichment (ADR-0006, #29):
                            // an aggregate whose sole argument is a `<rel>.<col>`
                            // path folds over *related* rows, not a GROUP BY
                            // group, so it is valid in a row-grain (OneToOne)
                            // target even though ordinary aggregates are not.
                            // Any other aggregate shape here — including
                            // `COUNT(*)`, whose `*` is not a `parse_call_args`
                            // expression — is a genuine wrong-key-space error.
                            let unsupported = || ParseError::UnsupportedKeySpace {
                                construct: format!("{name}(...)"),
                                detail: "aggregate functions require an aggregate key-space \
                                    (GROUP BY) or a to-many relationship path argument, \
                                    neither of which applies here"
                                    .to_string(),
                            };
                            if self.peek_is_symbol('*') {
                                return Err(unsupported());
                            }
                            let args = self.parse_call_args()?;
                            if matches!(args.as_slice(), [Expr::RelationshipPath { .. }]) {
                                return Ok(Expr::FunctionCall { name: upper, args });
                            }
                            return Err(unsupported());
                        }

                        if upper == "COUNT" {
                            // `COUNT(*)` (row-counting) is the only supported
                            // shape — `COUNT(<column>)` is not a plain
                            // `parse_call_args` expression, so it's parsed
                            // directly here rather than through
                            // `AGGREGATE_FUNCTION_SPECS`'s expression-argument
                            // machinery.
                            if self.peek_is_symbol('*') {
                                self.advance();
                                match self.advance() {
                                    Token::Symbol(')') => {}
                                    Token::Eof => {
                                        return Err(ParseError::UnexpectedEof {
                                            expected: "')'".to_string(),
                                        });
                                    }
                                    other => {
                                        return Err(ParseError::UnexpectedToken {
                                            expected: "')'".to_string(),
                                            found: other.describe(),
                                        });
                                    }
                                }
                                return Ok(Expr::FunctionCall {
                                    name: upper,
                                    args: Vec::new(),
                                });
                            }
                            self.skip_balanced_parens()?;
                            return Err(ParseError::UnsupportedAggregateFunction { name: upper });
                        }

                        let spec = lookup_aggregate_function(&upper)
                            .expect("every non-COUNT AGGREGATE_FUNCTIONS name is in AGGREGATE_FUNCTION_SPECS");
                        let args = self.parse_call_args()?;
                        if args.len() != spec.arg_types.len() {
                            return Err(ParseError::FunctionArityMismatch {
                                name,
                                expected: spec.arg_types.len(),
                                found: args.len(),
                            });
                        }
                        return Ok(Expr::FunctionCall { name: upper, args });
                    }

                    if upper == "COALESCE" {
                        let args = self.parse_call_args()?;
                        if args.is_empty() {
                            return Err(ParseError::AtLeastOneArgumentRequired { name: upper });
                        }
                        return Ok(Expr::FunctionCall { name: upper, args });
                    }

                    let spec = match lookup_function(&upper) {
                        Some(spec) => spec,
                        None => {
                            self.skip_balanced_parens()?;
                            return Err(ParseError::UnsupportedFunction { name });
                        }
                    };

                    let args = self.parse_call_args()?;
                    if args.len() != spec.arg_types.len() {
                        return Err(ParseError::FunctionArityMismatch {
                            name,
                            expected: spec.arg_types.len(),
                            found: args.len(),
                        });
                    }

                    return Ok(Expr::FunctionCall { name: upper, args });
                }

                Ok(Expr::Column(name))
            }
            Token::Eof => Err(ParseError::UnexpectedEof {
                expected: "an expression".to_string(),
            }),
            other => Err(ParseError::UnexpectedToken {
                expected: "an expression".to_string(),
                found: other.describe(),
            }),
        }
    }

    /// Parses the remainder of a `CAST(...)` — `'<literal>' AS <type>)`,
    /// with `CAST` and its `(` already consumed — into the same
    /// [`Expr::TypedLiteral`] the `<type> '<literal>'` spelling builds.
    ///
    /// Only a single-quoted literal is accepted as the operand. A general
    /// `CAST(<expr> AS <type>)` is rejected by name
    /// ([`ParseError::UnsupportedCast`]) rather than parsed-then-refused
    /// later, so the message can explain that a coercion lattice belongs to
    /// each type family's own issue — see [`super::typed_literal`]'s module
    /// doc comment for the reasoning.
    fn parse_cast_body(&mut self) -> Result<Expr, ParseError> {
        let text = match self.advance() {
            Token::String(text) => text,
            Token::Eof => {
                return Err(ParseError::UnexpectedEof {
                    expected: "a single-quoted literal".to_string(),
                });
            }
            other => {
                // Consume the rest of the call so a rejected general cast
                // doesn't desync the parse into a second, confusing error —
                // the same courtesy `skip_balanced_parens` does for an
                // unsupported function call.
                let found = other.describe();
                self.skip_balanced_parens()?;
                return Err(ParseError::UnsupportedCast { found });
            }
        };
        self.expect_keyword("AS")?;
        let type_name = self.expect_ident()?;
        self.expect_symbol(')')?;

        let Some(spec) = lookup_typed_literal(&type_name.to_ascii_uppercase()) else {
            return Err(ParseError::UnsupportedLiteralType { name: type_name });
        };
        Ok(Expr::TypedLiteral {
            pg_type: spec.pg_type,
            text,
        })
    }

    /// Parses a call's comma-separated argument list up to and including the
    /// `)` matching a `(` already consumed by the caller. General
    /// expression syntax (issue #64): each argument is a full `parse_expr`,
    /// so calls can nest.
    fn parse_call_args(&mut self) -> Result<Vec<Expr>, ParseError> {
        let mut args = Vec::new();
        if self.peek_is_symbol(')') {
            self.advance();
            return Ok(args);
        }
        loop {
            args.push(self.parse_expr()?);
            if self.peek_is_symbol(',') {
                self.advance();
                continue;
            }
            break;
        }
        match self.advance() {
            Token::Symbol(')') => Ok(args),
            Token::Eof => Err(ParseError::UnexpectedEof {
                expected: "')'".to_string(),
            }),
            other => Err(ParseError::UnexpectedToken {
                expected: "')'".to_string(),
                found: other.describe(),
            }),
        }
    }

    /// Consumes tokens up to and including the `)` matching a `(` already
    /// consumed by the caller, so a rejected function call doesn't need its
    /// argument list to itself be valid expression syntax.
    fn skip_balanced_parens(&mut self) -> Result<(), ParseError> {
        let mut depth = 1;
        loop {
            match self.advance() {
                Token::Symbol('(') => depth += 1,
                Token::Symbol(')') => {
                    depth -= 1;
                    if depth == 0 {
                        return Ok(());
                    }
                }
                Token::Eof => {
                    return Err(ParseError::UnexpectedEof {
                        expected: "')'".to_string(),
                    });
                }
                _ => {}
            }
        }
    }

    fn parse_predicate(&mut self) -> Result<Predicate, ParseError> {
        match self.advance() {
            Token::Ident(s) if s.eq_ignore_ascii_case("true") => Ok(Predicate::True),
            other => Err(ParseError::UnsupportedPredicate {
                detail: format!(
                    "only a literal TRUE is accepted (general predicates are deferred; \
                        see docs/open-questions.md); found {}",
                    other.describe()
                ),
            }),
        }
    }
}
