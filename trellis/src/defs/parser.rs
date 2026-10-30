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
//! `<target>` and `<source>` (ADR-0011) each
//! accept either a bare `<table>` (resolved once, later, via `search_path`)
//! or an explicit `<schema>.<table>` spelling that names its
//! schema directly and skips that resolution — see [`Self::parse_table_ref`]
//! for the shared parsing logic and why it doesn't collide with qualified
//! column references elsewhere in this grammar despite reusing the same
//! `ident '.' ident` token shape.
//!
//! `FROM <source>` is where the key-space clause slots in: absent for 1-1,
//! `GROUP BY <cols>` for an aggregate, `[LEFT|INNER] JOIN <other> ON <cond>
//! GROUP BY <cols>` for a join aggregate (ADR-0019). `<expr>` supports column references,
//! numeric and string literals, `+`, `-`, `*`, `/`, `%`, `>`, prefix `-`,
//! parenthesized grouping (`(<expr>)`, to override the precedence table's own
//! grouping), `CAST(<expr> AS <integer, numeric or timestamp type>)`,
//! `<expr> AT TIME ZONE 'UTC'`, `EXTRACT(<field> FROM <expr>)`, `name(args)`
//! function calls against [`super::registry::FUNCTIONS`], and
//! qualified columns, `<table>.<column>` or `<schema>.<table>.<column>`
//! (see [`Parser::parse_dotted_reference`]). **Typed literals** are spelled
//! either `<type> '<text>'` or `CAST('<text>' AS <type>)` — see
//! [`super::typed_literal`] for the allowlisted types, why both spellings
//! build one AST node, and why `<expr>::<type>` and general casts are
//! rejected. `<predicate>` accepts only the literal `TRUE`.

use super::ast::{
    AlterClause, AlterTransform, DefinitionRef, Expr, FieldDef, JoinKind, JoinOn, JoinSpec,
    KeySpace, Operator, Predicate, QualifiedColumn, Statement, StatementKind, TransformDef,
    TransformRef,
};
use super::error::ParseError;
use super::lexer::{Token, lex};
use super::registry::{
    AGGREGATE_FUNCTIONS, NON_IMMUTABLE_NAMES, cast_target, function_overloads, is_reserved_word,
    lookup_aggregate_function, lookup_operator, null_cast_target, operator_spec,
};
use super::typed_literal::lookup_typed_literal;
use super::validate::is_deferred_aggregate_name;

/// The deepest an expression may nest: the longest `+` chain, the most
/// parentheses and the most nested calls a definition may write. Every pass
/// that walks an expression recurses once per level, so this bounds them all
/// on a 2 MiB thread stack.
pub const MAX_EXPRESSION_DEPTH: usize = 100;

const OPERATOR_CHARS: &[char] = &['+', '-', '*', '/', '%', '>', '<', '=', '!'];

/// Words that continue or close an expression rather than begin an operand,
/// so a prefix `NOT` or `CASE` before one of them is a column of that name.
const CONTINUATION_KEYWORDS: &[&str] = &["AS", "IS", "AND", "OR", "THEN", "ELSE", "END", "WHEN"];

/// The longest run of prefix `NOT` operators the parser keeps.
const MAX_PREFIX_NOT: usize = 100;

/// An infix operator at the cursor: a binary operator, or the postfix
/// `IS [NOT] NULL`.
#[derive(Debug, Clone, Copy)]
enum Infix {
    Binary(Operator),
    IsNull { negated: bool },
}

impl Infix {
    fn precedence(self) -> u8 {
        match self {
            Infix::Binary(op) => operator_spec(op).precedence,
            Infix::IsNull { .. } => super::registry::precedence::IS,
        }
    }

    fn symbol(self) -> &'static str {
        match self {
            Infix::Binary(op) => operator_spec(op).symbol,
            Infix::IsNull { negated: false } => "IS NULL",
            Infix::IsNull { negated: true } => "IS NOT NULL",
        }
    }
}

/// The longest run of prefix `-` operators (`- - - a`) the parser keeps. Each
/// one is a node deep, and the tree is walked recursively.
const MAX_PREFIX_MINUS: usize = 100;

/// What [`Parser::parse_statement`] expects to lead a statement, spelled out
/// in the error a caller sees when it leads with something else — the closest
/// thing this grammar has to a keyword table, and deliberately only a message
/// (the parser still recognizes each keyword positionally, via
/// [`Parser::peek_is_keyword`], the way every other keyword here works).
const STATEMENT_KEYWORDS: &str = "a statement keyword: TRANSFORM, PAUSE, RESUME, DROP or ALTER";

/// Parses a transform definition's source text into a [`TransformDef`].
///
/// `TransformDef`-typed so callers that re-parse persisted text (the catalog)
/// don't need to unwrap a statement-kind enum.
pub fn parse(input: &str) -> Result<TransformDef, ParseError> {
    let tokens = lex(input)?;
    Parser {
        tokens,
        pos: 0,
        nest: 0,
        last_height: 0,
        is_aggregate: false,
        source: None,
        joined: None,
    }
    .parse_transform_def()
}

/// Parses **any** statement in this grammar into a [`Statement`] — the one
/// entry point [`crate::Trellis::apply`] uses (ADR-0012).
///
/// The statement's leading keyword alone decides which form is parsed, so
/// dispatch lives here rather than in the caller: a new form is a new arm.
///
/// The forms, in full (including ADR-0015's `ALTER TRANSFORM`):
///
/// ```text
/// TRANSFORM <target> FROM <source> [GROUP BY <keys>] SELECT <fields> [WHERE <predicate>]
/// TRANSFORM <target> FROM <source> [LEFT|INNER] JOIN <other> ON <equi-join>
///   GROUP BY <keys> SELECT <fields> [WHERE <predicate>]
///
/// PAUSE  TRANSFORM <target>[.<column>]
/// RESUME TRANSFORM <target>[.<column>]
/// DROP   TRANSFORM <target>
///
/// ALTER TRANSFORM <target> <clause>[, <clause> ...]
///   where <clause> is  ADD <expr> AS <field>  |  DROP <field>  |  ALTER <field> AS <expr>
/// ```
///
/// `PAUSE`/`RESUME`/`DROP` keep the kind keyword and
/// are flat imperatives rather than a `PAUSE`/`RESUME`-flavoured spelling of `ALTER TRANSFORM x PAUSE` (decision
/// 4 — that specific spelling is still not accepted; `ALTER TRANSFORM` itself
/// later became real grammar under ADR-0015, but only for the `ADD`/`DROP`/
/// `ALTER` field-editing clauses above, never as an alternate way to spell
/// `PAUSE`/`RESUME`/`DROP`). They need no lexer changes: `PAUSE`/`RESUME`/
/// `DROP` lex as plain `Ident`s and are matched case-insensitively by
/// [`Parser::peek_is_keyword`], exactly like `TRANSFORM`/`FROM`/`TO` already
/// are.
///
/// [`parse`] remains as the narrower, single-form entry point the engine
/// itself uses when it re-parses text it persisted and already knows the kind
/// of (`catalog::definition_by_target`), where unwrapping a [`Statement`]
/// would be pure ceremony.
pub fn parse_statement(input: &str) -> Result<Statement, ParseError> {
    let tokens = lex(input)?;
    Parser {
        tokens,
        pos: 0,
        nest: 0,
        last_height: 0,
        is_aggregate: false,
        source: None,
        joined: None,
    }
    .parse_statement()
}

/// Which form the statement `input` is, decided by parsing all of it with
/// `parse_statement` — so the answer is always the form
/// [`crate::Trellis::apply`] would carry out, and a statement `apply` would
/// refuse as malformed is the same [`ParseError`] here, whatever keyword it
/// leads with (`TRANSFORM oops` is an error, not a
/// [`StatementKind::DefineTransform`]).
///
/// It is pure on the text: nothing is read from or written to the database.
/// That is what lets an embedding binding refuse the wrong form
/// before it applies anything — a `define` that only defines transforms
/// checks for [`StatementKind::DefineTransform`] and then calls `apply` with the
/// same text, and nothing can happen between the two to change the answer.
pub fn statement_kind(input: &str) -> Result<StatementKind, ParseError> {
    parse_statement(input).map(|statement| statement.kind())
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
    /// How many `parse_expr` calls are open (parentheses and call arguments).
    nest: usize,
    /// The height of the expression `parse_expr` or `parse_primary` returned last.
    last_height: usize,
    /// Set once [`Self::parse_key_space_clause`] has parsed a `GROUP BY`.
    /// Gates whether [`Self::parse_primary`] will accept a
    /// `SUM`/`MIN`/`MAX`/`AVG` call — a plain field rather than threading
    /// key-space through every expression-parsing method, since it's fixed
    /// for the whole statement by the time fields are parsed.
    is_aggregate: bool,
    /// The `FROM` table's bare name, once [`Self::parse_transform_def`] has
    /// parsed it: a two-part `GROUP BY` key must name this table. `None` for
    /// an `ALTER TRANSFORM` clause, whose statement doesn't name the source.
    source: Option<String>,
    /// The `JOIN` table's bare name, once [`Self::parse_join_clause`] has
    /// parsed it.
    joined: Option<String>,
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
    /// [`Self::peek`]), clamped to the trailing `Eof` — for `DOUBLE
    /// PRECISION`, the grammar's only multi-word type keyword, and for naming
    /// a two-character operator in its refusal.
    fn peek_at(&self, n: usize) -> &Token {
        let i = (self.pos + n).min(self.tokens.len() - 1);
        &self.tokens[i]
    }

    /// Consumes the second half of a two-word type keyword, if `first` is
    /// the first half of one, and returns the canonical uppercased keyword
    /// to look up in [`super::typed_literal::TYPED_LITERALS`].
    ///
    /// `DOUBLE PRECISION` is the only such keyword: it is
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

    /// Parses a `TRANSFORM`/`FROM` table reference (ADR-0011
    /// grammar clause 4): either a bare `<table>`,
    /// returned as `(table, None)`, or an explicit `<schema>.<table>`
    /// spelling, returned as `(table, Some(schema))`. Rejects a third
    /// component (`a.b.c`) with a dedicated error naming the whole
    /// over-qualified reference, rather than leaving the trailing `.c`
    /// dangling for a later parse step to trip over with a confusing
    /// "expected end of input".
    ///
    /// **Not ambiguous with a qualified column's `a.b`** in
    /// [`Self::parse_primary`], even though both recognize the same
    /// `ident '.' ident` token shape: this method only ever runs immediately
    /// after the `TRANSFORM`/`FROM`/`JOIN` keyword, before `SELECT`'s
    /// field-expression list is reached, so the two never compete for the
    /// same tokens.
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
    /// The defining form delegates to the same [`Self::parse_transform_def`]
    /// the narrow entry point uses (including its own end-of-input check), so
    /// `apply`ing a definition can't drift from `parse`ing one.
    fn parse_statement(&mut self) -> Result<Statement, ParseError> {
        if self.peek_is_keyword("TRANSFORM") {
            return self.parse_transform_def().map(Statement::DefineTransform);
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
    /// `TRANSFORM` is the only kind keyword; anything else is refused by the
    /// ordinary [`Self::expect_keyword`] path below.
    fn parse_transform_ref(&mut self, verb: Verb) -> Result<TransformRef, ParseError> {
        self.expect_keyword("TRANSFORM")?;
        let (target, column) = self.parse_transform_address(verb)?;
        Ok(TransformRef { target, column })
    }

    /// Parses the `TRANSFORM <address>` tail of a `DROP` statement.
    ///
    /// `DROP` is whole-definition only, so a dotted
    /// transform address is its qualified name; dropping a single calculated
    /// field is an `ALTER TRANSFORM ... DROP <field>`.
    fn parse_drop_ref(&mut self) -> Result<DefinitionRef, ParseError> {
        self.expect_keyword("TRANSFORM")?;
        let (target, qualified_table) = self.parse_transform_address(Verb::Drop)?;
        // `DROP` is whole-definition only, so a dotted address is the
        // transform's qualified name, `<schema>.<table>` (dropping one
        // calculated field is an `ALTER TRANSFORM ... DROP <field>`).
        Ok(DefinitionRef::Transform(match qualified_table {
            Some(table) => format!("{target}.{table}"),
            None => target,
        }))
    }

    /// Parses a `PAUSE`/`RESUME`/`DROP TRANSFORM` address: `<target>` or a
    /// two-part `a.b`, returned as its two halves. `PAUSE`/`RESUME` read the
    /// halves as `<transform>.<column>` (the facade
    /// may resolve them to a qualified name, see [`TransformRef`]), `DROP` as
    /// `<schema>.<table>`. A third part is refused naming the whole address,
    /// the way [`Self::parse_table_ref`] does for its own over-qualified case.
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
            detail: "a transform is addressed by its target table, bare or \
                     '<schema>.<table>', or by '<transform>.<column>'; there is no \
                     '<schema>.<transform>.<column>' spelling"
                .to_string(),
        })
    }

    fn parse_transform_def(&mut self) -> Result<TransformDef, ParseError> {
        self.expect_keyword("TRANSFORM")?;
        let (target, explicit_target_schema) = self.parse_table_ref()?;

        self.expect_keyword("FROM")?;
        let (source, explicit_source_schema) = self.parse_table_ref()?;
        self.source = Some(source.clone());

        let key_space = self.parse_key_space_clause()?;

        let without_source_index = self.peek_is_keyword("WITHOUT");
        if without_source_index {
            self.advance();
            self.expect_keyword("SOURCE")?;
            self.expect_keyword("INDEX")?;
            if matches!(key_space, KeySpace::OneToOne | KeySpace::JoinOneToOne(_)) {
                return Err(ParseError::UnsupportedKeySpace {
                    construct: "WITHOUT SOURCE INDEX".to_string(),
                    detail: "only a GROUP BY or JOIN target needs a source index; a 1-1 \
                             target pages by its source's primary key"
                        .to_string(),
                });
            }
        }

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
            without_source_index,
        })
    }

    /// Parses `ALTER TRANSFORM <target> <clause>[, <clause> ...]` (ADR-0015).
    /// `<target>` is a bare identifier
    /// ([`Self::expect_ident`], not [`Self::parse_table_ref`]) — an edit
    /// addresses an already-registered definition by its bare operator-facing
    /// identity, the same convention [`Self::parse_transform_address`] uses
    /// for `PAUSE`/`RESUME`/`DROP TRANSFORM`, not the schema-qualifiable table
    /// reference `TRANSFORM`/`FROM` accept when *declaring* one.
    fn parse_alter_transform(&mut self) -> Result<AlterTransform, ParseError> {
        self.expect_keyword("ALTER")?;
        self.expect_keyword("TRANSFORM")?;
        let mut target = self.expect_ident()?;
        if self.peek_is_symbol('.') {
            // `<schema>.<table>`, the qualified name `apply` returns.
            self.advance();
            target = format!("{target}.{}", self.expect_ident()?);
        }

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
    /// 1-1 definition's own field list does, and no aggregate call, matching the current release's
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
    /// `GROUP BY <key>[, <key>...]` -> [`KeySpace::Aggregate`], and
    /// `[LEFT|INNER] JOIN <other> ON <equi-join> GROUP BY <key>[, ...]` ->
    /// [`KeySpace::Join`] (ADR-0019), or the same without `GROUP BY` ->
    /// [`KeySpace::JoinOneToOne`] (ADR-0021).
    fn parse_key_space_clause(&mut self) -> Result<KeySpace, ParseError> {
        if ["JOIN", "LEFT", "INNER", "RIGHT", "FULL", "CROSS", "NATURAL"]
            .iter()
            .any(|kw| self.peek_is_keyword(kw))
        {
            let (spec, grouped) = self.parse_join_clause()?;
            return Ok(if grouped {
                KeySpace::Join(spec)
            } else {
                KeySpace::JoinOneToOne(spec)
            });
        }
        if self.peek_is_symbol(',') {
            return Err(ParseError::UnsupportedKeySpace {
                construct: "comma join".to_string(),
                detail: "write the join as `FROM a [LEFT|INNER] JOIN b ON a.x = b.y`".to_string(),
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

    /// Parses `[LEFT [OUTER]|INNER] JOIN <other> ON <term> [AND <term>...]
    /// GROUP BY <column>[, <column>...]`, every column written
    /// `<table>.<column>`. Only inner and left equi-joins are admitted, and a
    /// `JOIN` always aggregates, so `GROUP BY` is required. The shape of the
    /// result (`ON` terms across the two tables, column types, aggregates) is
    /// the validator's to check.
    fn parse_join_clause(&mut self) -> Result<(JoinSpec, bool), ParseError> {
        let unsupported = |construct: &str, detail: &str| ParseError::UnsupportedKeySpace {
            construct: construct.to_string(),
            detail: detail.to_string(),
        };
        let kind = if self.peek_is_keyword("LEFT") {
            self.advance();
            if self.peek_is_keyword("OUTER") {
                self.advance();
            }
            JoinKind::Left
        } else if self.peek_is_keyword("INNER") {
            self.advance();
            JoinKind::Inner
        } else if let Some(kw) = ["RIGHT", "FULL", "CROSS", "NATURAL"]
            .into_iter()
            .find(|kw| self.peek_is_keyword(kw))
        {
            let detail = if kw == "RIGHT" {
                "only INNER and LEFT joins are supported: swap the two tables and use LEFT JOIN"
            } else {
                "only INNER and LEFT joins are supported (docs/transforms.md, \"Supported \
                 sources and targets\")"
            };
            return Err(unsupported(&format!("{kw} JOIN"), detail));
        } else {
            JoinKind::Inner
        };
        self.expect_keyword("JOIN")?;
        let (right, explicit_right_schema) = self.parse_table_ref()?;
        if self.source.as_deref() == Some(right.as_str()) {
            return Err(unsupported(
                "JOIN",
                &format!(
                    "both tables are named '{right}': a table can't be joined to itself, and \
                     two tables sharing a bare name can't be told apart, since every column \
                     is written `<table>.<column>` and this grammar has no table aliases"
                ),
            ));
        }
        if self.peek_is_keyword("USING") {
            return Err(unsupported(
                "JOIN USING",
                "write the join columns out with ON: `ON a.x = b.x`",
            ));
        }
        self.joined = Some(right.clone());

        self.expect_keyword("ON")?;
        let mut on = vec![self.parse_join_on_term()?];
        while self.peek_is_keyword("AND") {
            self.advance();
            on.push(self.parse_join_on_term()?);
        }
        if self.peek_is_keyword("OR") {
            return Err(unsupported(
                "JOIN ON",
                "ON admits only equalities joined by AND, not OR",
            ));
        }
        if ["JOIN", "LEFT", "INNER", "RIGHT", "FULL", "CROSS", "NATURAL"]
            .iter()
            .any(|kw| self.peek_is_keyword(kw))
        {
            return Err(unsupported(
                "JOIN",
                "a JOIN transform joins exactly two tables; a second JOIN is not supported",
            ));
        }

        if !self.peek_is_keyword("GROUP") {
            // No `GROUP BY`: a 1-1 JOIN (ADR-0021). Whether it is admitted is
            // the validator's to say (it is gated at define, not here, so a
            // stored definition still loads with the gate off).
            return Ok((
                JoinSpec {
                    kind,
                    right,
                    explicit_right_schema,
                    on,
                    group_by: Vec::new(),
                },
                false,
            ));
        }
        self.advance();
        self.expect_keyword("BY")?;
        let mut group_by = vec![self.parse_join_column("JOIN GROUP BY")?];
        while self.peek_is_symbol(',') {
            self.advance();
            group_by.push(self.parse_join_column("JOIN GROUP BY")?);
        }
        self.is_aggregate = true;
        Ok((
            JoinSpec {
                kind,
                right,
                explicit_right_schema,
                on,
                group_by,
            },
            true,
        ))
    }

    /// One `<table>.<column> = <table>.<column>` term of a `JOIN ... ON`.
    fn parse_join_on_term(&mut self) -> Result<JoinOn, ParseError> {
        let lhs = self.parse_join_column("JOIN ON")?;
        if !self.peek_is_symbol('=') {
            return Err(ParseError::UnsupportedKeySpace {
                construct: "JOIN ON".to_string(),
                detail: "ON admits only equalities between columns: \
                    `<table>.<column> = <table>.<column>`"
                    .to_string(),
            });
        }
        self.advance();
        let rhs = self.parse_join_column("JOIN ON")?;
        Ok(JoinOn { lhs, rhs })
    }

    /// A `<table>.<column>` or `<schema>.<table>.<column>` reference in a
    /// `JOIN`'s `ON` or `GROUP BY`; anything else (a bare column, a literal,
    /// an expression) is refused under `construct`.
    fn parse_join_column(&mut self, construct: &str) -> Result<QualifiedColumn, ParseError> {
        let refuse = |found: String| ParseError::UnsupportedKeySpace {
            construct: construct.to_string(),
            detail: format!(
                "expected a column written `<table>.<column>`, found {found}; a JOIN admits \
                 only side-qualified columns here"
            ),
        };
        let first = match self.advance() {
            Token::Ident(first) => first,
            other => return Err(refuse(other.describe())),
        };
        if !self.peek_is_symbol('.') {
            return Err(refuse(format!("the bare name '{first}'")));
        }
        self.advance();
        let second = self.expect_ident()?;
        if !self.peek_is_symbol('.') {
            return Ok(QualifiedColumn {
                schema: None,
                table: first,
                column: second,
            });
        }
        self.advance();
        let third = self.expect_ident()?;
        Ok(QualifiedColumn {
            schema: Some(first),
            table: second,
            column: third,
        })
    }

    /// Parses one `GROUP BY` key of a non-`JOIN` aggregate: a plain `<column>`, or
    /// `<source>.<column>` naming the `FROM` table. A key is always a source column,
    /// never a field, so the qualifier changes nothing and isn't kept. Any other head
    /// is refused ([`ParseError::GroupByKeyNotFromSource`]).
    fn parse_group_by_key(&mut self) -> Result<String, ParseError> {
        let first = self.expect_ident()?;
        if self.peek_is_symbol('.') {
            self.advance();
            let column = self.expect_ident()?;
            let source = self.source.clone().unwrap_or_default();
            if source != first {
                return Err(ParseError::GroupByKeyNotFromSource {
                    reference: format!("{first}.{column}"),
                    source,
                });
            }
            return Ok(column);
        }
        Ok(first)
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
                detail: "JOIN must appear directly after FROM <source> and before \
                    GROUP BY and SELECT (ADR-0004's reserved slot), not after SELECT/WHERE"
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
    /// precedence table says it should, matching a
    /// precedence-aware grammar like Postgres's rather than flat
    /// left-to-right.
    fn parse_expr(&mut self) -> Result<Expr, ParseError> {
        if self.nest >= MAX_EXPRESSION_DEPTH {
            return Err(ParseError::ExpressionTooDeep {
                limit: MAX_EXPRESSION_DEPTH,
            });
        }
        self.nest += 1;
        let result = self.parse_binary_expr(0);
        self.nest -= 1;
        result
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
        let mut lhs = self.parse_unary()?;
        let mut lhs_height = self.last_height;
        // The precedence of the non-associative operator that built `lhs`
        // (`a < b`), which no operator of its own level may follow.
        let mut non_associative: Option<u8> = None;
        while let Some((infix, tokens)) = self.peek_infix()? {
            let precedence = infix.precedence();
            if precedence < min_precedence {
                break;
            }
            if non_associative == Some(precedence) {
                return Err(ParseError::NonAssociativeComparison {
                    operator: infix.symbol().to_string(),
                });
            }
            for _ in 0..tokens {
                self.advance();
            }
            let node = match infix {
                Infix::Binary(op) => {
                    let rhs = self.parse_binary_expr(precedence + 1)?;
                    lhs_height = 1 + lhs_height.max(self.last_height);
                    non_associative = op.is_non_associative().then_some(precedence);
                    Expr::BinaryOp {
                        op,
                        lhs: Box::new(lhs),
                        rhs: Box::new(rhs),
                    }
                }
                Infix::IsNull { negated } => {
                    lhs_height += 1;
                    non_associative = None;
                    Expr::IsNull {
                        expr: Box::new(lhs),
                        negated,
                    }
                }
            };
            // The tree is measured as it is built, so a chain too deep to
            // walk is refused before it exists (and before it must be dropped).
            if lhs_height > MAX_EXPRESSION_DEPTH {
                return Err(ParseError::ExpressionTooDeep {
                    limit: MAX_EXPRESSION_DEPTH,
                });
            }
            lhs = node;
        }
        self.last_height = lhs_height;
        Ok(lhs)
    }

    /// The infix operator at the cursor and how many tokens spell it, or
    /// `None` when the next token ends the operand. `AND`, `OR` and `IS` are
    /// keywords only here, after a complete operand, so a column of that name
    /// is still a column wherever an operand starts (ADR-0018).
    fn peek_infix(&self) -> Result<Option<(Infix, usize)>, ParseError> {
        match self.peek() {
            Token::Symbol(c) if OPERATOR_CHARS.contains(c) => {
                // A comparison's characters must be adjacent (the lexer
                // emits `>=` whole), as in Postgres: `a > = b` is a syntax
                // error there and here.
                if let Token::Symbol(next) = self.peek_at(1)
                    && matches!((*c, *next), ('<' | '>' | '!', '=') | ('<', '>'))
                {
                    return Err(ParseError::SplitOperator {
                        found: format!("{c} {next}"),
                        joined: format!("{c}{next}"),
                    });
                }
                match lookup_operator(&c.to_string()) {
                    Some(op) => Ok(Some((Infix::Binary(op), 1))),
                    None => Err(ParseError::UnsupportedOperator {
                        operator: c.to_string(),
                    }),
                }
            }
            // A run of adjacent operator characters is one operator, as in
            // Postgres; a run with no registered operator is refused whole
            // rather than split (`a<=>b`, `a!=-1`, `a%-1`).
            Token::Operator(symbol) => match lookup_operator(symbol) {
                Some(op) => Ok(Some((Infix::Binary(op), 1))),
                None => Err(ParseError::UnsupportedOperator {
                    operator: symbol.clone(),
                }),
            },
            // `AT` is a keyword only here, after a complete operand and
            // followed by `TIME ZONE` (ADR-0018): anywhere else the word is a
            // name, or the error it always was.
            Token::Ident(word)
                if word.eq_ignore_ascii_case("AT")
                    && matches!(self.peek_at(1), Token::Ident(w) if w.eq_ignore_ascii_case("TIME"))
                    && matches!(self.peek_at(2), Token::Ident(w) if w.eq_ignore_ascii_case("ZONE")) =>
            {
                Ok(Some((Infix::Binary(Operator::AtTimeZone), 3)))
            }
            Token::Ident(word) if word.eq_ignore_ascii_case("AND") => {
                Ok(Some((Infix::Binary(Operator::And), 1)))
            }
            Token::Ident(word) if word.eq_ignore_ascii_case("OR") => {
                Ok(Some((Infix::Binary(Operator::Or), 1)))
            }
            Token::Ident(word) if word.eq_ignore_ascii_case("IS") => {
                let word_at = |n: usize, kw: &str| matches!(self.peek_at(n), Token::Ident(w) if w.eq_ignore_ascii_case(kw));
                let negated = word_at(1, "NOT");
                let after = 1 + usize::from(negated);
                if word_at(after, "NULL") {
                    Ok(Some((Infix::IsNull { negated }, after + 1)))
                } else if word_at(after, "DISTINCT") && word_at(after + 1, "FROM") {
                    let op = if negated {
                        Operator::IsNotDistinctFrom
                    } else {
                        Operator::IsDistinctFrom
                    };
                    Ok(Some((Infix::Binary(op), after + 2)))
                } else {
                    let mut found = String::from("IS");
                    for n in 1..=after {
                        if let Token::Ident(w) = self.peek_at(n) {
                            found.push(' ');
                            found.push_str(&w.to_ascii_uppercase());
                        }
                    }
                    Err(ParseError::UnsupportedIsForm { found })
                }
            }
            _ => Ok(None),
        }
    }

    /// Whether the cursor is a prefix `NOT` or `CASE` keyword rather than a
    /// column of that name: the next token has to begin an operand. A bare
    /// `not` before `AS`, an operator or the end of a list stays a column
    /// (ADR-0018). A `-` after it begins an operand: `NOT -x > 0` is
    /// `NOT ((-x) > 0)` and `CASE -x WHEN ...` a simple `CASE`, as in Postgres,
    /// where both words are reserved.
    fn peek_prefix_keyword(&self, keyword: &str) -> bool {
        self.peek_is_keyword(keyword)
            && match self.peek_at(1) {
                Token::Number(_) | Token::String(_) | Token::Symbol('(' | '-') => true,
                Token::Ident(word) => !CONTINUATION_KEYWORDS
                    .iter()
                    .any(|kw| word.eq_ignore_ascii_case(kw)),
                _ => false,
            }
    }

    /// A primary term under any number of prefix `-`, which binds tighter
    /// than every binary operator. The operators are counted in a loop, not
    /// recursed into, so a long run costs no stack.
    ///
    /// Postgres reads `-<number>` as one negative constant (`-2147483648` is
    /// an `integer`), so a minus applied straight to a number literal flips
    /// the literal's sign instead of building a node; any other operand is
    /// wrapped once per remaining minus.
    fn parse_unary(&mut self) -> Result<Expr, ParseError> {
        // Prefix `NOT` binds looser than `IS` and every comparison, so its
        // operand is everything at `IS` and tighter (`NOT a = b` is
        // `NOT (a = b)`). A run of them wraps one operand, counted in a loop,
        // so a long run costs no stack. The operand's own parse is a nesting
        // level (`a + NOT b + NOT c + ...` would otherwise recurse once per
        // `NOT`).
        let mut nots = 0usize;
        while self.peek_prefix_keyword("NOT") {
            self.advance();
            nots += 1;
            if nots > MAX_PREFIX_NOT {
                return Err(ParseError::NotChainTooLong {
                    max: MAX_PREFIX_NOT,
                });
            }
        }
        if nots > 0 {
            if self.nest >= MAX_EXPRESSION_DEPTH {
                return Err(ParseError::ExpressionTooDeep {
                    limit: MAX_EXPRESSION_DEPTH,
                });
            }
            self.nest += 1;
            let operand = self.parse_binary_expr(super::registry::precedence::NOT + 1);
            self.nest -= 1;
            let mut expr = operand?;
            self.last_height += nots;
            if self.last_height > MAX_EXPRESSION_DEPTH {
                return Err(ParseError::ExpressionTooDeep {
                    limit: MAX_EXPRESSION_DEPTH,
                });
            }
            for _ in 0..nots {
                expr = Expr::Not(Box::new(expr));
            }
            return Ok(expr);
        }
        let mut minuses = 0usize;
        while self.peek_is_symbol('-') {
            self.advance();
            minuses += 1;
            if minuses > MAX_PREFIX_MINUS {
                return Err(ParseError::PrefixMinusChainTooLong {
                    max: MAX_PREFIX_MINUS,
                });
            }
        }
        let mut expr = self.parse_primary()?;
        if minuses > 0
            && let Expr::NumberLiteral(text) = &mut expr
        {
            if minuses % 2 == 1 {
                *text = match text.strip_prefix('-') {
                    Some(positive) => positive.to_string(),
                    None => format!("-{text}"),
                };
            }
            return Ok(expr);
        }
        for _ in 0..minuses {
            expr = Expr::Negate(Box::new(expr));
        }
        // Each wrapped `Negate` is a level of the tree, so it counts toward
        // the height bound like a call does (otherwise a run of minuses at
        // every parenthesis level builds a tree far deeper than the bound).
        self.last_height += minuses;
        if self.last_height > MAX_EXPRESSION_DEPTH {
            return Err(ParseError::ExpressionTooDeep {
                limit: MAX_EXPRESSION_DEPTH,
            });
        }
        Ok(expr)
    }

    /// [`Self::parse_primary_inner`], recording the term's height in
    /// `last_height`: a leaf is 1, a call is 1 above its tallest argument
    /// (left in `last_height` by `parse_call_args`), a parenthesized
    /// expression keeps its own height.
    fn parse_primary(&mut self) -> Result<Expr, ParseError> {
        self.last_height = 0;
        let grouping = self.peek_is_symbol('(');
        let expr = self.parse_primary_inner()?;
        if grouping {
            // Keeps the inner expression's height.
        } else if matches!(
            expr,
            Expr::FunctionCall { .. } | Expr::Cast { .. } | Expr::Case { .. }
        ) {
            self.last_height += 1;
            if self.last_height > MAX_EXPRESSION_DEPTH {
                return Err(ParseError::ExpressionTooDeep {
                    limit: MAX_EXPRESSION_DEPTH,
                });
            }
        } else if self.last_height == 0 {
            self.last_height = 1;
        }
        Ok(expr)
    }

    fn parse_primary_inner(&mut self) -> Result<Expr, ParseError> {
        match self.advance() {
            Token::Symbol('(') => {
                // Parenthesized grouping (the precedence-climbing
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

                // `CASE` is a keyword only where an operand follows it.
                if upper == "CASE" && self.case_follows() {
                    return self.parse_case_body();
                }

                // Postgres's three constant keywords. They are reserved, so
                // never a column; what follows one is read as after any
                // operand (`null(1)` is refused, as Postgres refuses it).
                match upper.as_str() {
                    "TRUE" => return Ok(Expr::BooleanLiteral(true)),
                    "FALSE" => return Ok(Expr::BooleanLiteral(false)),
                    "NULL" => return Ok(Expr::Null),
                    _ => {}
                }

                if NON_IMMUTABLE_NAMES.contains(&upper.as_str()) {
                    return Err(ParseError::NonImmutableConstruct { name });
                }

                // A typed literal's `<type> '<text>'` spelling.
                // Checked before the `.`/`(` branches below and gated on the
                // *next* token being a string literal, which makes it
                // unambiguous against every other use of an identifier here:
                // a column reference, a qualified column's head and a
                // function name are each followed by end-of-expression, `.`
                // or `(` respectively, never by a string. So a source column
                // genuinely named `date` still parses as a column in
                // `SELECT date AS d` — only `date '...'` is a literal.
                // `DOUBLE PRECISION '1.5'` puts an identifier between the
                // type keyword and the string, so the one-token check below
                // needs a second position for it.
                let is_two_word_type_literal = upper == "DOUBLE"
                    && matches!(self.peek(), Token::Ident(s) if s.eq_ignore_ascii_case("PRECISION"))
                    && matches!(self.peek_at(1), Token::String(_));

                if is_two_word_type_literal || matches!(self.peek(), Token::String(_)) {
                    // An identifier directly followed by a string literal is
                    // a typed-literal attempt and nothing else: everywhere
                    // else an identifier appears in this grammar it's
                    // followed by an identifier (`x AS y`), a `.`
                    // (qualified column), a `(` (function call), an
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

                // A reserved word is not a column name: Postgres needs it
                // quoted, and this grammar has no quoted identifiers
                // (ADR-0022). Before `(` it is a construct or function name
                // (`left(...)`), after a `.` it is a label (read by
                // `parse_dotted_reference`), neither of which reaches here.
                if is_reserved_word(&upper) && !self.peek_is_symbol('(') {
                    return Err(ParseError::ReservedWord { name });
                }

                if self.peek_is_symbol('.') {
                    return self.parse_dotted_reference(name);
                }

                if self.peek_is_symbol('(') {
                    self.advance();

                    // `CAST('<literal>' AS <type>)` — standard
                    // SQL's spelling of the same constant the `<type>
                    // '<literal>'` form above builds, and the same node,
                    // because Postgres folds both to one `Const`. Handled
                    // ahead of the registry lookups because its `AS` keyword
                    // makes it not a `parse_call_args` argument list at all.
                    if upper == "CAST" {
                        return self.parse_cast_body();
                    }

                    // `EXTRACT(<field> FROM <operand>)`: the one call whose
                    // argument list is not comma-separated. It builds a call
                    // named `EXTRACT` whose first argument is the field.
                    if upper == "EXTRACT" {
                        return self.parse_extract_body();
                    }

                    // In a `JOIN` the deferred aggregate families parse (to
                    // any argument list) so the validator can refuse each by
                    // name rather than the parser calling it an unknown
                    // function.
                    if self.joined.is_some() && is_deferred_aggregate_name(&upper) {
                        let args = self.parse_call_args()?;
                        return Ok(Expr::FunctionCall { name: upper, args });
                    }

                    if AGGREGATE_FUNCTIONS.contains(&upper.as_str()) {
                        if !self.is_aggregate {
                            return Err(ParseError::UnsupportedKeySpace {
                                construct: format!("{name}(...)"),
                                detail: "aggregate functions require an aggregate key-space \
                                    (GROUP BY)"
                                    .to_string(),
                            });
                        }

                        if upper == "COUNT" {
                            // `COUNT(*)` is `*`
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
                            // `COUNT(<expr>)`: counts non-null
                            // occurrences of `<expr>` rather than every row —
                            // a different Postgres semantic from `COUNT(*)`,
                            // sharing its name because Postgres's own grammar
                            // does too. Any single expression this grammar can
                            // parse is accepted here (a bare column, a
                            // qualified column, a composed expression); the
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

                    if upper == "NULLIF" {
                        let args = self.parse_call_args()?;
                        if args.len() != 2 {
                            return Err(ParseError::FunctionArityMismatch {
                                name: upper,
                                expected: 2,
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

                    // A name can have several signatures (`ROUND(a)`,
                    // `ROUND(a, n)`); the arity must match one of them.
                    let arities: Vec<usize> = function_overloads(&upper)
                        .map(|spec| spec.arg_types.len())
                        .collect();
                    let Some(&widest) = arities.iter().max() else {
                        self.skip_balanced_parens()?;
                        return Err(ParseError::UnsupportedFunction { name });
                    };

                    let mut args = self.parse_call_args()?;
                    // A unit is case-insensitive in Postgres; the tree keeps
                    // the lower-case spelling `DATE_TRUNC_UNITS` lists.
                    if upper == "DATE_TRUNC"
                        && let Some(Expr::StringLiteral(unit)) = args.first_mut()
                    {
                        unit.make_ascii_lowercase();
                    }
                    if !arities.contains(&args.len()) {
                        return Err(ParseError::FunctionArityMismatch {
                            name,
                            expected: if args.len() < widest {
                                *arities.iter().min().unwrap_or(&widest)
                            } else {
                                widest
                            },
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

    /// Parses the rest of a dotted name whose head `first` (and its `.`) is
    /// at the cursor: `<table>.<column>` or
    /// `<schema>.<table>.<column>`, a qualified column either way. Whether it
    /// names the `FROM` (or `JOIN`) table and a real column is the
    /// validator's to check, so an `ALTER TRANSFORM` clause, parsed without
    /// knowing its source, builds the same node a `TRANSFORM` does.
    fn parse_dotted_reference(&mut self, first: String) -> Result<Expr, ParseError> {
        self.expect_symbol('.')?;
        let second = self.expect_ident()?;
        if !self.peek_is_symbol('.') {
            return Ok(Expr::SourceColumn {
                schema: None,
                table: first,
                column: second,
            });
        }
        self.advance();
        let third = self.expect_ident()?;
        if self.peek_is_symbol('.') {
            let mut reference = format!("{first}.{second}.{third}");
            while self.peek_is_symbol('.') {
                self.advance();
                reference.push('.');
                reference.push_str(&self.expect_ident()?);
            }
            return Err(ParseError::TooManyColumnNameParts { reference });
        }
        Ok(Expr::SourceColumn {
            schema: Some(first),
            table: second,
            column: third,
        })
    }

    /// Whether the `CASE` just consumed begins a `CASE` expression (an
    /// operand follows) rather than being a column named `case`
    /// (`SELECT case AS c`, ADR-0018).
    fn case_follows(&self) -> bool {
        self.peek_is_keyword("WHEN")
            || match self.peek() {
                Token::Number(_) | Token::String(_) | Token::Symbol('(' | '-') => true,
                Token::Ident(word) => !CONTINUATION_KEYWORDS
                    .iter()
                    .any(|kw| word.eq_ignore_ascii_case(kw)),
                _ => false,
            }
    }

    /// Parses the remainder of a searched `CASE`, with `CASE` consumed:
    /// `WHEN <condition> THEN <result>` once or more, an optional `ELSE
    /// <result>`, and `END`. The simple form (`CASE <operand> WHEN ...`) is
    /// refused by name. Each condition and result is a nesting level, and the
    /// node is one level above its tallest.
    fn parse_case_body(&mut self) -> Result<Expr, ParseError> {
        if !self.peek_is_keyword("WHEN") {
            return Err(ParseError::SimpleCaseUnsupported);
        }
        let mut branches = Vec::new();
        let mut tallest = 0;
        while self.peek_is_keyword("WHEN") {
            self.advance();
            let condition = self.parse_expr()?;
            tallest = tallest.max(self.last_height);
            self.expect_keyword("THEN")?;
            let result = self.parse_expr()?;
            tallest = tallest.max(self.last_height);
            branches.push((condition, result));
        }
        let otherwise = if self.peek_is_keyword("ELSE") {
            self.advance();
            let result = self.parse_expr()?;
            tallest = tallest.max(self.last_height);
            Some(Box::new(result))
        } else {
            None
        };
        self.expect_keyword("END")?;
        self.last_height = tallest;
        Ok(Expr::Case {
            branches,
            otherwise,
        })
    }

    /// Parses the remainder of a `CAST(...)`, with `CAST` and its `(`
    /// already consumed.
    ///
    /// A quoted literal operand (`'<literal>' AS <type>)`) builds the same
    /// [`Expr::TypedLiteral`] the `<type> '<literal>'` spelling builds. Any
    /// other operand is a cast of an expression, to one of
    /// `CAST_TARGETS` or an alias of one; whether that operand converts to it is the
    /// validator's to check.
    fn parse_cast_body(&mut self) -> Result<Expr, ParseError> {
        if !matches!(self.peek(), Token::String(_)) {
            let operand = self.parse_expr()?;
            self.expect_keyword("AS")?;
            let type_name = self.expect_ident()?;
            let keyword = self.take_type_keyword(&type_name.to_ascii_uppercase());
            // `CAST(NULL AS <type>)` converts no value, so it may name any
            // type the grammar has a value of.
            let target = if operand == Expr::Null {
                null_cast_target(&keyword)
            } else {
                cast_target(&keyword)
            };
            if self.peek_is_symbol('(') {
                // A modifier: `numeric(10,2)`. Consume it so the error is the
                // only one reported.
                self.advance();
                self.skip_balanced_parens()?;
                self.expect_symbol(')')?;
                return Err(ParseError::UnsupportedCastTarget {
                    name: format!("{type_name}(...)"),
                });
            }
            let Some(target) = target else {
                self.skip_balanced_parens()?;
                return Err(ParseError::UnsupportedCastTarget { name: type_name });
            };
            self.expect_symbol(')')?;
            return Ok(Expr::Cast {
                expr: Box::new(operand),
                target,
            });
        }
        let Token::String(text) = self.advance() else {
            unreachable!("peeked a string literal");
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

    /// Parses the remainder of an `EXTRACT(...)`, with `EXTRACT` and its `(`
    /// already consumed: `<field> FROM <operand> )`. The field is a bare word,
    /// kept lower-case as a string literal; whether the operand's type has it
    /// is the validator's to check.
    fn parse_extract_body(&mut self) -> Result<Expr, ParseError> {
        let field = match self.advance() {
            Token::Ident(field) => field.to_ascii_lowercase(),
            Token::Eof => {
                return Err(ParseError::UnexpectedEof {
                    expected: "an EXTRACT field such as year".to_string(),
                });
            }
            other => {
                return Err(ParseError::UnexpectedToken {
                    expected: "an EXTRACT field such as year".to_string(),
                    found: other.describe(),
                });
            }
        };
        self.expect_keyword("FROM")?;
        let operand = self.parse_expr()?;
        self.expect_symbol(')')?;
        Ok(Expr::FunctionCall {
            name: "EXTRACT".to_string(),
            args: vec![Expr::StringLiteral(field), operand],
        })
    }

    /// Parses a call's comma-separated argument list up to and including the
    /// `)` matching a `(` already consumed by the caller. General
    /// expression syntax: each argument is a full `parse_expr`,
    /// so calls can nest.
    fn parse_call_args(&mut self) -> Result<Vec<Expr>, ParseError> {
        let mut args = Vec::new();
        let mut tallest = 0;
        if self.peek_is_symbol(')') {
            self.advance();
            self.last_height = 0;
            return Ok(args);
        }
        loop {
            args.push(self.parse_expr()?);
            tallest = tallest.max(self.last_height);
            if self.peek_is_symbol(',') {
                self.advance();
                continue;
            }
            break;
        }
        self.last_height = tallest;
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
                    "only a literal TRUE is accepted (general predicates are not supported; \
                        see docs/transforms.md); found {}",
                    other.describe()
                ),
            }),
        }
    }
}

#[cfg(test)]
mod depth_tests {
    use super::*;
    use std::collections::HashMap;

    /// Runs `f` on a thread with the default 2 MiB stack, the smallest stack a
    /// tokio worker or a `cargo test` thread gets.
    fn on_default_stack<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> T {
        std::thread::Builder::new()
            .stack_size(2 * 1024 * 1024)
            .spawn(f)
            .unwrap()
            .join()
            .expect("thread overflowed or panicked")
    }

    /// `1 + 1 + ... + 1` with `n` terms: a left-deep chain `n` high.
    fn chain(n: usize) -> String {
        vec!["1"; n].join(" + ")
    }

    /// `((((1))))` nested `n` parentheses deep.
    fn parens(n: usize) -> String {
        format!("{}1{}", "(".repeat(n), ")".repeat(n))
    }

    /// `COALESCE(COALESCE(... 1 ...), 1)` with `n` calls.
    fn calls(n: usize) -> String {
        format!("{}1{}", "COALESCE(".repeat(n), ")".repeat(n))
    }

    /// Defines `SELECT <expr> AS out` all the way through the validator, as a
    /// define does, and reports whether the parser refused it for depth.
    fn define(expr: String) -> Result<(), ParseError> {
        on_default_stack(move || {
            let sql = format!("TRANSFORM t FROM s SELECT {expr} AS out");
            let def = parse(&sql)?;
            walk_every_way(&def);
            Ok(())
        })
    }

    /// Runs the passes that walk a definition's expressions, as a define does.
    fn walk_every_way(def: &TransformDef) {
        let columns: HashMap<String, crate::defs::ValueType> =
            HashMap::from([("c".to_string(), crate::defs::ValueType::Numeric)]);
        let _ = crate::defs::validate::validate(def, &columns);
        let _ = crate::defs::backfill::substitute_all_fields(def);
        for field in &def.fields {
            let _ = crate::defs::ast::render_field_expr(&field.expr);
            let _ = crate::defs::oracle::render_expr_sql(&field.expr);
            let _ = crate::defs::oracle::render_qualified_expr_sql(&field.expr, "s");
            drop(field.expr.clone());
        }
    }

    /// Defines `n` calculated fields, each reading the previous one through
    /// `step` (`"{prev}"` is replaced by its name), and reports whether the
    /// validator refused the chain as too deep.
    fn fields_refused(n: usize, step: &'static str) -> bool {
        on_default_stack(move || {
            let mut fields = vec!["1 AS f0".to_string()];
            for i in 1..n {
                let body = step.replace("{prev}", &format!("f{}", i - 1));
                fields.push(format!("{body} AS f{i}"));
            }
            let def = parse(&format!("TRANSFORM t FROM s SELECT {}", fields.join(", "))).unwrap();
            let refused = matches!(
                crate::defs::validate::validate(&def, &HashMap::new()),
                Err(crate::defs::validate::ValidationError::FieldExpansionTooLarge)
            );
            if !refused {
                walk_every_way(&def);
            }
            refused
        })
    }

    /// `-(-(... -(c)))` with `minuses` prefix minuses at each of `levels`
    /// parenthesis levels (spaced so no `--` comment forms).
    fn minuses_at_each_level(minuses: usize, levels: usize) -> String {
        let unit = format!("{}(", "- ".repeat(minuses));
        format!("{}c{}", unit.repeat(levels), ")".repeat(levels))
    }

    #[test]
    fn prefix_minuses_and_casts_count_toward_the_height_bound() {
        // 100 minuses at each of 99 parenthesis levels is ~10,000 nodes deep
        // while no single chain or nest exceeds its own bound.
        assert!(matches!(
            define(minuses_at_each_level(100, 99)),
            Err(ParseError::ExpressionTooDeep { .. })
        ));
        // Minuses count one level each, wherever they sit.
        assert!(define(minuses_at_each_level(1, 50)).is_ok());
        assert!(matches!(
            define(minuses_at_each_level(2, 99)),
            Err(ParseError::ExpressionTooDeep { .. })
        ));
        // A cast counts a level like a call.
        let casts = |n: usize| format!("{}1{}", "CAST(".repeat(n), " AS bigint)".repeat(n));
        assert!(matches!(
            define(casts(MAX_EXPRESSION_DEPTH + 1)),
            Err(ParseError::ExpressionTooDeep { .. })
        ));
        assert!(!matches!(
            define(casts(MAX_EXPRESSION_DEPTH - 1)),
            Err(ParseError::ExpressionTooDeep { .. })
        ));
    }

    #[test]
    fn calculated_fields_reading_each_other_through_minus_and_cast_are_bounded() {
        assert!(fields_refused(MAX_EXPRESSION_DEPTH / 2 + 1, "-{prev} + 1"));
        assert!(fields_refused(
            MAX_EXPRESSION_DEPTH + 1,
            "CAST({prev} AS bigint)"
        ));
        assert!(fields_refused(100_000, "-{prev}"));
    }

    #[test]
    fn calculated_fields_reading_each_other_are_bounded_by_the_inlined_height() {
        // A read at the root costs one level per hop: 100 fields in a chain.
        assert!(!fields_refused(MAX_EXPRESSION_DEPTH, "{prev}"));
        assert!(fields_refused(MAX_EXPRESSION_DEPTH + 1, "{prev}"));
        // `{prev} + 1` puts the read one level down, so each hop costs two.
        assert!(!fields_refused(MAX_EXPRESSION_DEPTH / 2, "{prev} + 1"));
        assert!(fields_refused(MAX_EXPRESSION_DEPTH / 2 + 1, "{prev} + 1"));
        // Far past the limit, without overflowing the stack.
        assert!(fields_refused(100_000, "{prev} + 1"));
    }

    #[test]
    fn a_long_alias_chain_walked_from_its_head_is_refused_without_overflowing() {
        // Named so the chain's head sorts first: the walk starts at the head
        // and would recurse once per hop before it knows any height.
        let n = 100_000;
        let refused = on_default_stack(move || {
            let mut fields = vec![format!("1 AS f{n:06}")];
            for i in (1..n).rev() {
                fields.push(format!("f{:06} AS f{i:06}", i + 1));
            }
            let def = parse(&format!("TRANSFORM t FROM s SELECT {}", fields.join(", "))).unwrap();
            crate::defs::validate::validate(&def, &HashMap::new())
        });
        assert_eq!(
            refused,
            Err(crate::defs::validate::ValidationError::FieldExpansionTooLarge)
        );
    }

    #[test]
    fn a_call_over_a_chain_counts_the_call_as_a_level() {
        define(format!("COALESCE({})", chain(MAX_EXPRESSION_DEPTH - 1))).unwrap();
        assert!(too_deep(define(format!(
            "COALESCE({})",
            chain(MAX_EXPRESSION_DEPTH)
        ))));
    }

    #[test]
    fn a_definition_at_the_limit_reparses_from_its_rendered_text() {
        // `ALTER TRANSFORM` stores the rendered text, which wraps every
        // operator in parentheses, and every load parses it again.
        let at_limit = [
            chain(MAX_EXPRESSION_DEPTH),
            format!("COALESCE({})", chain(MAX_EXPRESSION_DEPTH - 1)),
            calls(MAX_EXPRESSION_DEPTH - 1),
            format!("1 + {}1{}", "COALESCE(1 + ".repeat(49), ")".repeat(49)),
        ];
        for expr in at_limit {
            on_default_stack(move || {
                let def = parse(&format!("TRANSFORM t FROM s SELECT {expr} AS out")).unwrap();
                let text = crate::defs::ast::render_definition_text(&def);
                assert_eq!(parse(&text).unwrap(), def, "{text}");
            });
        }
    }

    fn too_deep(r: Result<(), ParseError>) -> bool {
        matches!(r, Err(ParseError::ExpressionTooDeep { .. }))
    }

    #[test]
    fn chain_at_limit_defines_and_one_past_is_refused() {
        define(chain(MAX_EXPRESSION_DEPTH)).unwrap();
        assert!(too_deep(define(chain(MAX_EXPRESSION_DEPTH + 1))));
    }

    #[test]
    fn chain_of_columns_at_limit_validates_against_a_real_column() {
        let sql = format!(
            "TRANSFORM t FROM s SELECT {} AS out",
            vec!["c"; MAX_EXPRESSION_DEPTH].join(" + ")
        );
        on_default_stack(move || {
            let def = parse(&sql).unwrap();
            let columns = HashMap::from([("c".to_string(), crate::defs::ValueType::Numeric)]);
            crate::defs::validate::validate(&def, &columns).unwrap();
            walk_every_way(&def);
        });
    }

    #[test]
    fn nested_parentheses_at_limit_define_and_one_past_is_refused() {
        // The field's own expression is the outermost level.
        define(parens(MAX_EXPRESSION_DEPTH - 1)).unwrap();
        assert!(too_deep(define(parens(MAX_EXPRESSION_DEPTH))));
    }

    #[test]
    fn nested_calls_at_limit_define_and_one_past_is_refused() {
        // The innermost literal is a level of its own.
        define(calls(MAX_EXPRESSION_DEPTH - 1)).unwrap();
        assert!(too_deep(define(calls(MAX_EXPRESSION_DEPTH))));
    }

    #[test]
    fn a_huge_chain_is_refused_without_overflowing() {
        assert!(too_deep(define(chain(200_000))));
        assert!(too_deep(define(parens(200_000))));
        assert!(too_deep(define(calls(200_000))));
    }

    #[test]
    fn a_flat_sum_of_many_columns_still_defines() {
        let cols: Vec<String> = (0..2000).map(|i| format!("c{i}")).collect();
        let flat = format!("COALESCE({})", cols.join(", "));
        define(flat).unwrap();
    }
}
