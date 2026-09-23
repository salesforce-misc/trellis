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

use super::ast::{
    AlterClause, AlterTransform, DefinitionRef, Expr, FieldDef, GroupByKey, KeySpace, Predicate,
    RelationshipDef, Statement, TransformDef, TransformRef,
};
use super::error::ParseError;
use super::lexer::{Token, lex};
use super::registry::{
    AGGREGATE_FUNCTIONS, NON_IMMUTABLE_NAMES, lookup_aggregate_function, lookup_function,
    lookup_operator, operator_spec,
};
use super::typed_literal::lookup_typed_literal;

const OPERATOR_CHARS: &[char] = &['+', '-', '*', '/', '%', '>', '<', '='];

/// What [`Parser::parse_statement`] expects to lead a statement, spelled out
/// in the error a caller sees when it leads with something else — the closest
/// thing this grammar has to a keyword table, and deliberately only a message
/// (the parser still recognizes each keyword positionally, via
/// [`Parser::peek_is_keyword`], the way every other keyword here works).
const STATEMENT_KEYWORDS: &str =
    "a statement keyword: TRANSFORM, RELATIONSHIP, PAUSE, RESUME, DROP or ALTER";

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

/// Parses **any** statement in this grammar into a [`Statement`] — the one
/// entry point [`crate::Trellis::apply`] uses (issue #227, ADR-0012).
///
/// The statement's leading keyword alone decides which form is parsed, so
/// dispatch lives here rather than in the caller: before this existed, the CLI
/// sniffed the first whitespace-delimited word itself to choose between
/// [`parse`] and [`parse_relationship`], and every new statement form would
/// have grown that external sniffing (and every embedder's copy of it). Here,
/// a new form is a new arm.
///
/// The forms, in full (issue #228's settled grammar, plus ADR-0015's
/// `ALTER TRANSFORM`, issues #241/#242):
///
/// ```text
/// TRANSFORM <target> FROM <source> [GROUP BY <keys>] SELECT <fields> [WHERE <predicate>]
/// RELATIONSHIP <name> FROM <from_table>.<fk_col> TO <to_table>.<pk_col>
///
/// PAUSE  TRANSFORM <target>[.<column>]
/// RESUME TRANSFORM <target>[.<column>]
/// DROP   TRANSFORM <target>
/// DROP   RELATIONSHIP [<schema>.]<from_table>.<relationship_name>
///
/// ALTER TRANSFORM <target> <clause>[, <clause> ...]
///   where <clause> is  ADD <expr> AS <field>  |  DROP <field>  |  ALTER <field> AS <expr>
/// ```
///
/// Beyond that list, in particular, **there is no `PAUSE`/`RESUME
/// RELATIONSHIP`**: pausing is a transform-only operation,
/// because a relationship is a reusable component of a transform rather than
/// something that does work of its own to suspend. `PAUSE RELATIONSHIP x.y` is
/// therefore just an unrecognized keyword where `TRANSFORM` was expected, no
/// more special-cased than `PAUSE SELECT` would be. `DROP` takes either kind,
/// since a relationship is a definition and definitions can be retired.
///
/// `PAUSE`/`RESUME`/`DROP` keep the kind keyword (issue #228 decision 1 — the
/// two kinds don't share a namespace) and are flat imperatives rather than a
/// `PAUSE`/`RESUME`-flavoured spelling of `ALTER TRANSFORM x PAUSE` (decision
/// 4 — that specific spelling is still not accepted; `ALTER TRANSFORM` itself
/// later became real grammar under ADR-0015, but only for the `ADD`/`DROP`/
/// `ALTER` field-editing clauses above, never as an alternate way to spell
/// `PAUSE`/`RESUME`/`DROP`). They need no lexer changes: `PAUSE`/`RESUME`/
/// `DROP` lex as plain `Ident`s and are matched case-insensitively by
/// [`Parser::peek_is_keyword`], exactly like `TRANSFORM`/`FROM`/`TO` already
/// are.
///
/// [`parse`] and [`parse_relationship`] remain as the narrower, single-form
/// entry points the engine itself uses when it re-parses text it persisted and
/// already knows the kind of (`catalog::definition_by_target`,
/// `catalog::relationship_by_name`), where unwrapping a [`Statement`] would be
/// pure ceremony.
pub fn parse_statement(input: &str) -> Result<Statement, ParseError> {
    let tokens = lex(input)?;
    Parser {
        tokens,
        pos: 0,
        is_aggregate: false,
    }
    .parse_statement()
}

/// Which imperative a `PAUSE`/`RESUME`/`DROP` statement leads with. Threaded
/// into [`Parser::parse_transform_address`] so a malformed address can
/// name the statement form it was written in (`PAUSE TRANSFORM`, not just
/// `TRANSFORM`) in its error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Verb {
    Pause,
    Resume,
    Drop,
}

impl Verb {
    fn keyword(self) -> &'static str {
        match self {
            Verb::Pause => "PAUSE",
            Verb::Resume => "RESUME",
            Verb::Drop => "DROP",
        }
    }
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

    /// The token `n` positions ahead of the cursor (`peek_at(0)` is
    /// [`Self::peek`]), clamped to the trailing `Eof` — two-token lookahead
    /// exists solely for `DOUBLE PRECISION`, the grammar's only multi-word
    /// type keyword (issue #112).
    fn peek_at(&self, n: usize) -> &Token {
        let i = (self.pos + n).min(self.tokens.len() - 1);
        &self.tokens[i]
    }

    /// Consumes the second half of a two-word type keyword, if `first` is
    /// the first half of one, and returns the canonical uppercased keyword
    /// to look up in [`super::typed_literal::TYPED_LITERALS`].
    ///
    /// `DOUBLE PRECISION` (issue #112) is the only such keyword: it is
    /// Postgres's own `format_type` spelling of `float8`, and
    /// `TypedLiteralSpec::keyword` is pinned to that spelling so the
    /// spelling a definition writes is the spelling the oracle renders back
    /// (`type_keyword_matches_value_type`). The lexer produces two
    /// `Ident`s for it, so it is rejoined here rather than taught to the
    /// lexer, which has no notion of type names at all.
    fn take_type_keyword(&mut self, first: &str) -> String {
        if first == "DOUBLE" && self.peek_is_keyword("PRECISION") {
            self.advance();
            return "DOUBLE PRECISION".to_string();
        }
        first.to_string()
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

    /// Rejects anything but end-of-input at the cursor — every statement form
    /// in this grammar accepts exactly one statement, with no `;` terminator
    /// (ADR-0004).
    fn expect_eof(&mut self) -> Result<(), ParseError> {
        match self.advance() {
            Token::Eof => Ok(()),
            other => Err(ParseError::UnexpectedToken {
                expected: "end of input".to_string(),
                found: other.describe(),
            }),
        }
    }

    /// Dispatches on the statement's leading keyword — see
    /// [`parse_statement`]'s doc comment for the grammar this covers.
    ///
    /// The two defining forms delegate to the same
    /// [`Self::parse_transform_def`]/[`Self::parse_relationship_def`] the
    /// narrow entry points use (including their own end-of-input check), so
    /// `apply`ing a definition can't drift from `parse`ing one.
    fn parse_statement(&mut self) -> Result<Statement, ParseError> {
        if self.peek_is_keyword("TRANSFORM") {
            return self.parse_transform_def().map(Statement::DefineTransform);
        }
        if self.peek_is_keyword("RELATIONSHIP") {
            return self
                .parse_relationship_def()
                .map(Statement::DefineRelationship);
        }
        if self.peek_is_keyword("ALTER") {
            return self.parse_alter_transform().map(Statement::AlterTransform);
        }

        let verb = if self.peek_is_keyword("PAUSE") {
            Verb::Pause
        } else if self.peek_is_keyword("RESUME") {
            Verb::Resume
        } else if self.peek_is_keyword("DROP") {
            Verb::Drop
        } else {
            let found = self.peek().describe();
            return Err(match self.peek() {
                Token::Eof => ParseError::UnexpectedEof {
                    expected: STATEMENT_KEYWORDS.to_string(),
                },
                _ => ParseError::UnexpectedToken {
                    expected: STATEMENT_KEYWORDS.to_string(),
                    found,
                },
            });
        };
        self.advance();

        let statement = match verb {
            Verb::Pause => Statement::Pause(self.parse_transform_ref(verb)?),
            Verb::Resume => Statement::Resume(self.parse_transform_ref(verb)?),
            Verb::Drop => Statement::Drop(self.parse_drop_ref()?),
        };
        self.expect_eof()?;
        Ok(statement)
    }

    /// Parses the `TRANSFORM <address>` tail of a `PAUSE`/`RESUME` statement.
    ///
    /// `TRANSFORM` is the **only** kind keyword these two verbs accept: a
    /// relationship is a reusable component of a transform, not something that
    /// does work of its own, so there is nothing a pause could suspend (the
    /// same reason ADR-0014 gives for having no `pause_relationship`).
    /// `PAUSE RELATIONSHIP ...` is simply not a form this grammar has: it is
    /// rejected by the ordinary [`Self::expect_keyword`] path below, exactly
    /// like any other keyword that doesn't belong where it was written, with no
    /// special case for the phrase anywhere. `DROP` is the one verb that takes
    /// either kind ([`Self::parse_drop_ref`]).
    fn parse_transform_ref(&mut self, verb: Verb) -> Result<TransformRef, ParseError> {
        self.expect_keyword("TRANSFORM")?;
        let (target, column) = self.parse_transform_address(verb)?;
        Ok(TransformRef { target, column })
    }

    /// Parses the `TRANSFORM <address>` / `RELATIONSHIP <address>` tail of a
    /// `DROP` statement — the one verb that can name either kind of
    /// definition, since a relationship *is* a definition and definitions can
    /// be retired.
    ///
    /// A `<transform>.<column>` address is refused here rather than silently
    /// ignoring the column half: `DROP` is whole-definition only (issue #228,
    /// decision 2), and dropping a single calculated field is an
    /// `ALTER TRANSFORM ... DROP <field>` (issues #241/#242).
    fn parse_drop_ref(&mut self) -> Result<DefinitionRef, ParseError> {
        if self.peek_is_keyword("TRANSFORM") {
            self.advance();
            let (target, column) = self.parse_transform_address(Verb::Drop)?;
            if let Some(column) = column {
                return Err(ParseError::MalformedDefinitionAddress {
                    statement: "DROP TRANSFORM".to_string(),
                    address: format!("{target}.{column}"),
                    detail: "DROP removes a whole definition, so it takes a bare <target>; \
                             dropping one calculated field is an ALTER TRANSFORM operation \
                             (issues #241/#242), not a DROP"
                        .to_string(),
                });
            }
            return Ok(DefinitionRef::Transform(target));
        }

        if self.peek_is_keyword("RELATIONSHIP") {
            self.advance();
            let (schema, from_table, name) = self.parse_relationship_address()?;
            return Ok(DefinitionRef::Relationship {
                schema,
                from_table,
                name,
            });
        }

        let found = self.peek().describe();
        Err(match self.peek() {
            Token::Eof => ParseError::UnexpectedEof {
                expected: "'TRANSFORM' or 'RELATIONSHIP' after 'DROP'".to_string(),
            },
            _ => ParseError::UnexpectedToken {
                expected: "'TRANSFORM' or 'RELATIONSHIP' after 'DROP'".to_string(),
                found,
            },
        })
    }

    /// Parses a `PAUSE`/`RESUME`/`DROP TRANSFORM` address:
    /// `<target>` (whole definition) or `<target>.<column>` (one calculated
    /// field, issue #228 decision 2).
    ///
    /// **Deliberately not [`Self::parse_table_ref`].** That helper reads
    /// `a.b` as `<schema>.<table>`, which is right in `TRANSFORM`/`FROM`
    /// position and wrong here: a transform's operator-facing identity is its
    /// *bare* target-table name everywhere below the facade (see
    /// [`DefinitionRef::Transform`]'s doc comment), and the same `ident '.'
    /// ident` shape has to mean `<transform>.<column>` in this position for
    /// decision 2's column addressing to exist at all. A third part is
    /// refused naming the whole address, the way `parse_table_ref` does for
    /// its own over-qualified case.
    fn parse_transform_address(
        &mut self,
        verb: Verb,
    ) -> Result<(String, Option<String>), ParseError> {
        let target = self.expect_ident()?;
        if !self.peek_is_symbol('.') {
            return Ok((target, None));
        }
        self.advance();
        let column = self.expect_ident()?;
        if !self.peek_is_symbol('.') {
            return Ok((target, Some(column)));
        }
        // Over-qualified: keep consuming so the error names the whole address
        // rather than bailing mid-way and desyncing the rest of the parse.
        let mut address = format!("{target}.{column}");
        while self.peek_is_symbol('.') {
            self.advance();
            address.push('.');
            address.push_str(&self.expect_ident()?);
        }
        Err(ParseError::MalformedDefinitionAddress {
            statement: format!("{} TRANSFORM", verb.keyword()),
            address,
            detail: "a transform is addressed by its bare target table, optionally with one \
                     '.<column>' suffix — there is no schema-qualified spelling here, since a \
                     dotted address already means <transform>.<column>"
                .to_string(),
        })
    }

    /// Parses a `DROP RELATIONSHIP` address:
    /// `[<schema>.]<from_table>.<relationship_name>` (issue #228 decision 1).
    /// `DROP` is the only verb that reaches here — `PAUSE`/`RESUME` are
    /// transform-only — which is why this takes no [`Verb`] the way
    /// [`Self::parse_transform_address`] does.
    ///
    /// The last dotted component is always the relationship name; the
    /// remaining one or two are the from-table's own `[<schema>.]<table>`
    /// reference. A bare, unscoped name is refused
    /// ([`ParseError::UnscopedRelationshipAddress`]) — it identifies nothing,
    /// since `relationship_definitions` is unique on
    /// `(from_schema, from_table, name)`, not on `name`. Four or more parts
    /// are refused naming the whole address.
    ///
    /// **Deliberately not [`Self::parse_table_ref`]** either: that helper
    /// errors on the third part as over-qualified, which is exactly the
    /// schema-qualified form this address is *supposed* to accept.
    fn parse_relationship_address(
        &mut self,
    ) -> Result<(Option<String>, String, String), ParseError> {
        let mut parts = vec![self.expect_ident()?];
        while self.peek_is_symbol('.') {
            self.advance();
            parts.push(self.expect_ident()?);
        }
        match parts.len() {
            1 => Err(ParseError::UnscopedRelationshipAddress {
                address: parts.swap_remove(0),
            }),
            2 => {
                let name = parts.pop().expect("two parts");
                let from_table = parts.pop().expect("two parts");
                Ok((None, from_table, name))
            }
            3 => {
                let name = parts.pop().expect("three parts");
                let from_table = parts.pop().expect("three parts");
                let schema = parts.pop().expect("three parts");
                Ok((Some(schema), from_table, name))
            }
            _ => Err(ParseError::MalformedDefinitionAddress {
                statement: "DROP RELATIONSHIP".to_string(),
                address: parts.join("."),
                detail: "a relationship is addressed as \
                         [<schema>.]<from_table>.<relationship_name> — at most three \
                         '.'-separated parts"
                    .to_string(),
            }),
        }
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
        self.expect_eof()?;

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
        self.expect_eof()?;

        Ok(RelationshipDef {
            name,
            from_table,
            from_col,
            to_table,
            to_col,
        })
    }

    /// Parses `ALTER TRANSFORM <target> <clause>[, <clause> ...]` (ADR-0015,
    /// issues #241/#242). `<target>` is a bare identifier
    /// ([`Self::expect_ident`], not [`Self::parse_table_ref`]) — an edit
    /// addresses an already-registered definition by its bare operator-facing
    /// identity, the same convention [`Self::parse_transform_address`] uses
    /// for `PAUSE`/`RESUME`/`DROP TRANSFORM`, not the schema-qualifiable table
    /// reference `TRANSFORM`/`FROM` accept when *declaring* one.
    fn parse_alter_transform(&mut self) -> Result<AlterTransform, ParseError> {
        self.expect_keyword("ALTER")?;
        self.expect_keyword("TRANSFORM")?;
        let target = self.expect_ident()?;

        let mut clauses = vec![self.parse_alter_clause()?];
        while self.peek_is_symbol(',') {
            self.advance();
            clauses.push(self.parse_alter_clause()?);
        }
        self.expect_eof()?;

        Ok(AlterTransform { target, clauses })
    }

    /// Parses one `ADD <expr> AS <field>` / `DROP <field>` /
    /// `ALTER <field> AS <expr>` clause of an `ALTER TRANSFORM` statement.
    ///
    /// `is_aggregate` stays `false` for every expression parsed here (the
    /// same fixed setting a plain 1-1 `TRANSFORM ... SELECT` list parses
    /// under), regardless of the target's actual key-space: this grammar slot
    /// doesn't repeat `GROUP BY`, so the parser has no way to know the
    /// target's key-space at parse time. That still accepts every shape a
    /// 1-1 definition's own field list does, including an aggregate-function
    /// call over a to-many relationship path (the one aggregate shape a
    /// row-grain definition already allows) — only a *plain* `SUM`/`COUNT(*)`
    /// grouping aggregate is unavailable here, matching the current release's
    /// scope of `ALTER TRANSFORM` to 1-1 targets (see
    /// [`super::catalog::alter_transform`]'s own doc comment).
    fn parse_alter_clause(&mut self) -> Result<AlterClause, ParseError> {
        if self.peek_is_keyword("ADD") {
            self.advance();
            let expr = self.parse_expr()?;
            self.expect_keyword("AS")?;
            let name = self.expect_ident()?;
            return Ok(AlterClause::Add(FieldDef { name, expr }));
        }
        if self.peek_is_keyword("DROP") {
            self.advance();
            let name = self.expect_ident()?;
            return Ok(AlterClause::Drop(name));
        }
        if self.peek_is_keyword("ALTER") {
            self.advance();
            let name = self.expect_ident()?;
            self.expect_keyword("AS")?;
            let expr = self.parse_expr()?;
            return Ok(AlterClause::Alter(FieldDef { name, expr }));
        }

        let found = self.peek().describe();
        Err(match self.peek() {
            Token::Eof => ParseError::UnexpectedEof {
                expected: "'ADD', 'DROP' or 'ALTER'".to_string(),
            },
            _ => ParseError::UnexpectedToken {
                expected: "'ADD', 'DROP' or 'ALTER'".to_string(),
                found,
            },
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
                // `DOUBLE PRECISION '1.5'` puts an identifier between the
                // type keyword and the string, so the one-token check below
                // needs a second position for it (issue #112).
                let is_two_word_type_literal = upper == "DOUBLE"
                    && matches!(self.peek(), Token::Ident(s) if s.eq_ignore_ascii_case("PRECISION"))
                    && matches!(self.peek_at(1), Token::String(_));

                if is_two_word_type_literal || matches!(self.peek(), Token::String(_)) {
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
                    let keyword = self.take_type_keyword(&upper);
                    let Some(spec) = lookup_typed_literal(&keyword) else {
                        return Err(ParseError::UnsupportedLiteralType { name });
                    };
                    let Token::String(text) = self.advance() else {
                        unreachable!("peeked a string literal");
                    };
                    return Ok(Expr::TypedLiteral {
                        value_type: spec.value_type,
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
                            // `COUNT(*)` (row-counting, issue #75) is `*`
                            // literally — not a `parse_call_args` expression —
                            // so it's parsed directly here rather than
                            // through `AGGREGATE_FUNCTION_SPECS`'s
                            // expression-argument machinery.
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
                            // `COUNT(<expr>)` (issue #120): counts non-null
                            // occurrences of `<expr>` rather than every row —
                            // a different Postgres semantic from `COUNT(*)`,
                            // sharing its name because Postgres's own grammar
                            // does too. Any single expression this grammar can
                            // parse is accepted here (a bare column, a
                            // relationship path, a composed expression); the
                            // validator (`super::validate::infer_expr`) is
                            // what actually resolves and types it —
                            // `count(x)` accepts any argument type in
                            // Postgres, so there is no type restriction to
                            // enforce here, unlike `SUM`/`MIN`/`MAX`/`AVG`.
                            let args = self.parse_call_args()?;
                            if args.len() != 1 {
                                return Err(ParseError::FunctionArityMismatch {
                                    name,
                                    expected: 1,
                                    found: args.len(),
                                });
                            }
                            return Ok(Expr::FunctionCall { name: upper, args });
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
        // `CAST('1.5' AS double precision)` — see `take_type_keyword`.
        let keyword = self.take_type_keyword(&type_name.to_ascii_uppercase());
        self.expect_symbol(')')?;

        let Some(spec) = lookup_typed_literal(&keyword) else {
            return Err(ParseError::UnsupportedLiteralType { name: keyword });
        };
        Ok(Expr::TypedLiteral {
            value_type: spec.value_type,
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
