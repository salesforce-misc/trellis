//! The typed AST a transform definition parses into.
//!
//! Shapes mirror the logical model in `docs/transforms.md`: a definition
//! picks a key-space, a set of calculated fields, and a partial-data
//! predicate. This slice only populates the 1-1 key-space and
//! the `+`-only expression language; [`KeySpace`] and [`Predicate`] have
//! room to grow (aggregate/cross-join variants, general predicates) without
//! changing the shape callers already match on.
//!
//! The value model is [`ValueType`]; see `docs/transforms.md` and ADR-0004's
//! growth policy.

use std::fmt;

use super::pg_type::PgType;
use crate::float::FloatWidth;
use crate::integer::IntWidth;

/// A parsed transform definition, ready for the validator and
/// the SQL renderer.
///
/// **`target`/`source` are always bare table names, never a dotted
/// `schema.table` spelling — even when the definition explicitly qualified
/// one (ADR-0011).** That's a deliberate split,
/// not an oversight: a huge number of call sites (`backfill.rs`, `oracle.rs`,
/// `ddl.rs`, `apply.rs`, `ledger.rs`, `quarantine.rs`) pass
/// `&def.source`/`&def.target` straight into `quote_ident`, which quotes its
/// argument as a *single* identifier — handing it `"schema.table"` would
/// quote the dot right along with it, producing an invalid, never-resolving
/// identifier instead of a schema-qualified one. `intake::qualify`
/// enforces the same assumption from the other direction: it hard-rejects a
/// `.`-containing component rather than silently double-qualifying. So the
/// dotted spelling a definition writes never survives into these fields —
/// [`Self::explicit_source_schema`]/[`Self::explicit_target_schema`] carry the
/// schema half separately, as a side-channel [`super::catalog::create_definition_inner`]
/// consults only to decide *how* to resolve each bare name (trust the named
/// schema outright vs. walk `search_path`), not as part of the name itself.
#[derive(Debug, Clone, PartialEq)]
pub struct TransformDef {
    pub target: String,
    /// `Some(schema)` when this definition's `TRANSFORM <target>` clause
    /// explicitly spelled `<schema>.<target>` rather than a bare
    /// table name. When present, [`super::catalog::create_definition_inner`]
    /// validates that *exact* relation (schema and table both, via
    /// `information_schema.tables`) and persists it qualified, skipping
    /// `Config::target_schema` resolution entirely — a qualified spelling
    /// names its own schema, it doesn't inherit the configured default.
    /// `None` (the bare, far more common case) resolves
    /// through `Config::target_schema`.
    pub explicit_target_schema: Option<String>,
    pub source: String,
    /// The source-side twin of [`Self::explicit_target_schema`]: `Some(schema)`
    /// when `FROM <schema>.<source>` was written explicitly, in which case
    /// `create_definition_inner` validates that exact relation instead of
    /// walking `search_path` the way bare-name resolution
    /// ([`super::catalog::resolve_source_schema_in_txn`]) does. `None` for a
    /// bare `FROM <source>`.
    pub explicit_source_schema: Option<String>,
    pub key_space: KeySpace,
    pub fields: Vec<FieldDef>,
    pub predicate: Predicate,
    /// `WITHOUT SOURCE INDEX` was written: the definition runs without
    /// the source index Trellis requires of a `GROUP BY` or `JOIN` target
    /// (`super::source_index`), accepting the per-page scan a `self_check`
    /// walk then costs. Never set for a 1-1 target, which pages by its
    /// primary key.
    pub without_source_index: bool,
}

/// One whole statement in Trellis's grammar, as
/// [`super::parser::parse_statement`] returns it — the single shape
/// [`crate::Trellis::apply`] dispatches on (ADR-0012: "parsing
/// the statement is where the operation is decided; the facade signature does
/// not change as operations are added").
///
/// Every variant's leading keyword is what distinguishes it, so the parser
/// needs no lookahead past the first token to choose: `TRANSFORM` defines,
/// `PAUSE` / `RESUME` / `DROP` act on something already defined, `ALTER`
/// edits one. The kind keyword is repeated on the imperative forms
/// (`PAUSE TRANSFORM x`, not `PAUSE x`).
///
/// `AMEND` is deliberately absent: it needs its own semantics ADR before it
/// can be a grammar addition.
#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    /// `TRANSFORM <target> FROM <source> ... SELECT ...`
    DefineTransform(TransformDef),
    /// `PAUSE TRANSFORM <target>[.<column>]` — ADR-0014's operator-driven
    /// freeze. Idempotent.
    Pause(TransformRef),
    /// `RESUME TRANSFORM <target>[.<column>]` — ADR-0014's
    /// rebuild-by-backfill recovery (*not* a catch-up).
    Resume(TransformRef),
    /// `DROP TRANSFORM <target>` — ADR-0014's terminal reap. Idempotent; refused if a still-registered dependent
    /// chains off the subject.
    Drop(DefinitionRef),
    /// `ALTER TRANSFORM <target> <clause>[, <clause> ...]` (ADR-0015) — edits an
    /// already-registered transform's calculated fields in place: `ADD`, `DROP`, and
    /// `ALTER` clauses, one calculated field each. The key-space is not named and
    /// cannot change; a granularity change is a new transform plus cutover, not an edit
    /// — the grammar has no key-space clause here to write, rather than a validator
    /// rejecting one. Idempotent per-clause, matching pause/resume/drop's discipline:
    /// re-adding a field with the same formula, re-altering to the formula it already
    /// has, and dropping an already-absent field are all no-op successes.
    AlterTransform(AlterTransform),
}

impl Statement {
    /// Which form this statement is — the same decision
    /// [`crate::Trellis::apply`] dispatches on, with the parsed payload
    /// dropped.
    pub fn kind(&self) -> StatementKind {
        match self {
            Statement::DefineTransform(_) => StatementKind::DefineTransform,
            Statement::Pause(_) => StatementKind::PauseTransform,
            Statement::Resume(_) => StatementKind::ResumeTransform,
            Statement::Drop(DefinitionRef::Transform(_)) => StatementKind::DropTransform,
            Statement::AlterTransform(_) => StatementKind::AlterTransform,
        }
    }
}

/// Which form a statement of Trellis's grammar is, without what it says —
/// what [`crate::statement_kind`] returns.
///
/// One variant per statement head: the verb *and* the kind of definition it
/// acts on, as the grammar spells them ([`StatementKind::keywords`]). A
/// binding that accepts only one of them (a `drop_transform` that must not
/// pause one) can check for exactly that.
///
/// `#[non_exhaustive]` for the reason [`crate::Applied`] is: the grammar
/// grows a form at a time (`AMEND` is next once it has its own ADR), and a
/// binding matching on this must already handle a kind it doesn't know.
/// [`StatementKind::ALL`] is the list a binding checks its own mapping
/// against.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum StatementKind {
    /// `TRANSFORM <target> FROM <source> ...` — defines a transform.
    DefineTransform,
    /// `PAUSE TRANSFORM <target>[.<column>]`.
    PauseTransform,
    /// `RESUME TRANSFORM <target>[.<column>]`.
    ResumeTransform,
    /// `DROP TRANSFORM <target>`.
    DropTransform,
    /// `ALTER TRANSFORM <target> <clause>[, <clause> ...]`.
    AlterTransform,
}

impl StatementKind {
    /// Every variant, once — the list a binding checks its own mapping
    /// against, since `#[non_exhaustive]` stops any crate but this one from
    /// matching the enum exhaustively. `every_statement_kind_is_listed_in_all`
    /// fails until a new variant is added here.
    pub const ALL: [StatementKind; 5] = [
        StatementKind::DefineTransform,
        StatementKind::PauseTransform,
        StatementKind::ResumeTransform,
        StatementKind::DropTransform,
        StatementKind::AlterTransform,
    ];

    /// A stable `snake_case` name for this kind (`"drop_transform"`) —
    /// the identifier a host language keys off of, in the style of
    /// [`crate::TransformStatus::as_str`].
    pub fn as_str(self) -> &'static str {
        match self {
            StatementKind::DefineTransform => "define_transform",
            StatementKind::PauseTransform => "pause_transform",
            StatementKind::ResumeTransform => "resume_transform",
            StatementKind::DropTransform => "drop_transform",
            StatementKind::AlterTransform => "alter_transform",
        }
    }

    /// The keywords a statement of this kind opens with, as the grammar
    /// spells them (`"DROP TRANSFORM"`, `"TRANSFORM"`) — for a message
    /// that names the form a user wrote, so a binding needn't spell the
    /// grammar out a second time.
    pub fn keywords(self) -> &'static str {
        match self {
            StatementKind::DefineTransform => "TRANSFORM",
            StatementKind::PauseTransform => "PAUSE TRANSFORM",
            StatementKind::ResumeTransform => "RESUME TRANSFORM",
            StatementKind::DropTransform => "DROP TRANSFORM",
            StatementKind::AlterTransform => "ALTER TRANSFORM",
        }
    }
}

impl fmt::Display for StatementKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// What a [`Statement::Pause`]/[`Statement::Resume`] addresses: a registered
/// transform, whole (`column: None`) or one of its calculated fields.
///
/// The parser reads a dotted address as `<transform>.<column>`
/// ([`crate::QuarantineTarget`]'s `"transform"`/`"transform.column"`
/// addressing, folded into this grammar), with `target` the bare transform.
/// `<transform>.<column>` and `<schema>.<table>` are the same token shape, so
/// [`crate::Trellis::apply`] reads `a.b` as the whole transform registered as
/// `a.b` when transform `a` declares no field `b`; the column reading wins
/// otherwise, since the transform can still be named bare.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransformRef {
    pub target: String,
    pub column: Option<String>,
}

impl fmt::Display for TransformRef {
    /// Renders the address back in the spelling the grammar accepts, so an
    /// error message can name what it was handed.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.column {
            None => write!(f, "{}", self.target),
            Some(column) => write!(f, "{}.{column}", self.target),
        }
    }
}

/// What a [`Statement::Drop`] addresses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DefinitionRef {
    /// `TRANSFORM <target>` — the target table, bare or `<schema>.<table>`.
    ///
    /// No column half, unlike [`TransformRef`]: dropping one calculated field
    /// is an `ALTER TRANSFORM ... DROP <field>`, a different operation
    /// entirely, so a dotted
    /// address is a qualified name. [`crate::Trellis::apply`] refuses one that
    /// names no transform while its first half does, as the column address it
    /// reads as.
    Transform(String),
}

impl fmt::Display for DefinitionRef {
    /// Renders the address back in the spelling the grammar accepts, so an
    /// error message can name what it was handed.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DefinitionRef::Transform(target) => write!(f, "{target}"),
        }
    }
}

/// `ALTER TRANSFORM <target> <clause>[, <clause> ...]` (ADR-0015).
///
/// `target` is the registered transform's target table, bare or
/// `<schema>.<table>`, as [`DefinitionRef::Transform`] takes it: an edit acts
/// on an already-registered definition, rather than re-declaring the table.
#[derive(Debug, Clone, PartialEq)]
pub struct AlterTransform {
    pub target: String,
    /// At least one clause — the parser refuses an empty list (there is
    /// nothing to apply `ALTER TRANSFORM <target>` alone would mean).
    pub clauses: Vec<AlterClause>,
}

/// One clause of an [`AlterTransform`] statement — one calculated field, one
/// operation.
#[derive(Debug, Clone, PartialEq)]
pub enum AlterClause {
    /// `ADD <expr> AS <field>` — introduces a new calculated field.
    Add(FieldDef),
    /// `DROP <field>` — removes a calculated field and the data it computed.
    Drop(String),
    /// `ALTER <field> AS <expr>` — replaces an existing field's formula.
    Alter(FieldDef),
}

/// The target table's primary-key space (see `docs/transforms.md#granularity`).
///
/// [`KeySpace::Aggregate`] is a `GROUP BY <cols>`
/// definition, whose target's primary key is the grouping columns rather
/// than an inherited source column. [`KeySpace::Join`] is the same over the
/// join of two tables.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeySpace {
    OneToOne,
    /// `group_by` holds the source columns named after `GROUP BY`, in the
    /// order they were written — that order becomes the target table's
    /// composite primary key column order. A key's target column is named
    /// after it (a key has no aliasing syntax), and a two-part
    /// `<source>.<column>` key parses to its bare column.
    Aggregate {
        group_by: Vec<String>,
    },
    /// `FROM a [LEFT|INNER] JOIN b ON <equi-join> GROUP BY <keys>` (ADR-0019):
    /// an aggregate over the join of two tables.
    Join(JoinSpec),
}

/// Which rows of the `FROM` table a [`JoinSpec`] keeps when no `JOIN` table
/// row matches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinKind {
    /// `INNER JOIN` (or bare `JOIN`): a row with no match contributes nothing.
    Inner,
    /// `LEFT [OUTER] JOIN`: a row with no match still contributes, with the
    /// joined side's columns `NULL`.
    Left,
}

/// A column reference as written in a `JOIN` definition: always qualified
/// with its table, optionally with the schema too.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QualifiedColumn {
    pub schema: Option<String>,
    pub table: String,
    pub column: String,
}

/// One `<table>.<column> = <table>.<column>` term of a `JOIN ... ON` clause,
/// in the order written. Which side each column belongs to is the
/// validator's to resolve.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinOn {
    pub lhs: QualifiedColumn,
    pub rhs: QualifiedColumn,
}

/// The `JOIN` half of a [`KeySpace::Join`] definition. The `FROM` table is
/// [`TransformDef::source`]; this is the other table, how it joins, and the
/// `GROUP BY` keys, each of which may come from either table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinSpec {
    pub kind: JoinKind,
    /// The joined table's bare name.
    pub right: String,
    /// `Some(schema)` when the joined table was written `<schema>.<table>`;
    /// the twin of [`TransformDef::explicit_source_schema`].
    pub explicit_right_schema: Option<String>,
    /// The `ON` terms, joined by `AND`. At least one.
    pub on: Vec<JoinOn>,
    /// The `GROUP BY` keys in the order written; at least one.
    pub group_by: Vec<QualifiedColumn>,
}

/// Whether `name` is one of `group_by`'s keys' target column names — the
/// common "is this field a `GROUP BY` passthrough" check every layer that
/// touches an [`super::ast::KeySpace::Aggregate`] definition's fields needs
/// (DDL's column dedup, `staging::group_basis`'s field shape, …), pulled out once here
/// rather than re-implemented at each call site.
pub fn group_by_contains(group_by: &[String], name: &str) -> bool {
    group_by.iter().any(|key| key == name)
}

/// One `<expr> AS <name>` calculated-field entry.
#[derive(Debug, Clone, PartialEq)]
pub struct FieldDef {
    pub name: String,
    pub expr: Expr,
}

/// A calculated-field scalar expression.
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    /// A reference to a column on the (single, 1-1) source row.
    Column(String),
    /// A numeric literal, kept as the source text so the validator picks
    /// the numeric type/precision rather than the parser.
    NumberLiteral(String),
    /// A single-quoted string literal.
    StringLiteral(String),
    /// A **typed literal**: a constant of a Postgres type that
    /// has no literal syntax of its own in this grammar, spelled either
    /// `<type> '<text>'` (Postgres's typed-literal form — `DATE '2024-01-01'`)
    /// or `CAST('<text>' AS <type>)`. Both spellings build *this* node,
    /// because Postgres itself folds them to the same constant: `explain
    /// (verbose) select cast('2024-01-01' as date), date '2024-01-01'` prints
    /// `'2024-01-01'::date` twice.
    ///
    /// This is the one way a calculated field can *produce* a
    /// [`ValueType::Other`] or [`ValueType::Float`] value rather than merely
    /// pass one through, which is what promotes those types from "ingest
    /// only" to `docs/type-support.md`'s **computed 1-1 target** role.
    ///
    /// `value_type` is restricted to [`super::typed_literal::TYPED_LITERALS`]'s
    /// allowlist — not every type the OID registry recognizes — and `text`
    /// is the literal's *raw source text* (quotes stripped, `''` unescaped),
    /// required by [`super::validate`] to already be in that type's
    /// canonical Postgres output spelling. See
    /// [`super::typed_literal::TYPED_LITERALS`] for why both restrictions exist.
    ///
    /// `value_type` is a full [`ValueType`] (not a bare [`PgType`]) so
    /// `REAL '1.5'` / `CAST('1.5' AS double precision)` can
    /// produce a first-class [`ValueType::Float`] — floats need this grammar
    /// because Postgres gives them no bare literal syntax at all
    /// (`pg_typeof(1.5)` is `numeric`).
    TypedLiteral { value_type: ValueType, text: String },
    /// A source column named through the `FROM` table:
    /// `<table>.<column>` or `<schema>.<table>.<column>`. It always reads the
    /// source column, never a calculated field of the same name, which is how
    /// a definition reads a column that a field's name shadows (a bare name
    /// that could mean either is refused at define, see
    /// [`super::validate::ValidationError::AmbiguousColumnReference`]).
    ///
    /// `table` and `schema` are kept as written. The parser builds this node
    /// for every two- and three-part name outside a `JOIN` definition (in a
    /// `JOIN`, a head naming the joined table builds it too). The validator
    /// checks that `table` is the `FROM` table
    /// ([`super::validate::ValidationError::QualifiedColumnNotFromSource`]
    /// names any other head) and, when the `FROM` clause names its schema,
    /// that `schema` matches; the catalog checks `schema` against the schema
    /// a bare `FROM` resolved to.
    SourceColumn {
        schema: Option<String>,
        table: String,
        column: String,
    },
    BinaryOp {
        op: Operator,
        lhs: Box<Expr>,
        rhs: Box<Expr>,
    },
    /// A `name(args)` function call — the first function-call
    /// syntax the grammar accepts. `name` is the uppercased canonical form
    /// looked up in [`super::registry::FUNCTIONS`]; arity and argument types
    /// are checked against that same registry entry, not hardcoded here.
    ///
    /// In an [`KeySpace::Aggregate`] definition, `name` may instead be one of
    /// `SUM`/`MIN`/`MAX`/`AVG` (looked up in
    /// [`super::registry::AGGREGATE_FUNCTION_SPECS`]) with a single orderable
    /// argument (`MIN`/`MAX` accept any
    /// type with a real Postgres ordering — see
    /// [`super::registry::aggregate_result_type`]), or `COUNT` with either an
    /// empty `args` (`COUNT(*)` row-counting — `*` is not itself
    /// an expression, so this is the one arity-0 shape) or a single argument
    /// of any type (`COUNT(<expr>)` — counts non-null
    /// occurrences of `<expr>`, a different semantic from `COUNT(*)`).
    /// There's no separate AST node for aggregate calls, since they're
    /// syntactically identical `name(args)` calls, just resolved against a
    /// different registry depending on key-space.
    FunctionCall { name: String, args: Vec<Expr> },
}

/// The value type a column or expression carries. `Text` and
/// `Boolean` are plumbed through so columns of those types can be declared
/// and passed through a calculated field; `Text`-argument
/// functions exist, and the `>` comparison operator is the first
/// operator whose result type differs from its operands' (`Numeric,
/// Numeric -> Boolean`). `Uuid` is narrower still: it's
/// representable, comparable, and passthrough-able (including as an
/// aggregate `GROUP BY` key), but has no arithmetic/regex operations the way
/// `Numeric`/`Text` do — there's no real-world use for `uuid + uuid`, so
/// [`super::registry`] never grants it an operator or function.
///
/// [`ValueType::Other`] widens this from a 4-bucket lattice to
/// an open one: any Postgres type the [`super::pg_type`] OID registry
/// recognizes as a distinct family, but that hasn't yet earned its own
/// first-class variant here, carries its [`PgType`] tag instead of silently
/// collapsing into `Text` the way it used to. Per `docs/type-support.md`,
/// it's passthrough-only — [`super::registry`] grants it no operator or
/// function, [`super::validate`] rejects it as a key/predicate/computed
/// target — until the type family is promoted to a real variant with real semantics.
/// `Other` only says truthfully what the column is; it adds no semantics.
///
/// [`ValueType::Integer`] is the first such promotion, and the
/// first variant whose payload means something other than "this family is
/// still opaque": `smallint`/`integer`/`bigint` used to *share*
/// [`ValueType::Numeric`] with `numeric`/`real`/`double precision`, which
/// made every exact-integer computation arbitrary-precision (so it never
/// overflowed where Postgres does) and every derived integer column
/// `numeric`. See [`crate::integer`] for the full rationale, including why
/// the width rides along as an [`IntWidth`] payload on one variant — the
/// `Other(PgType)` shape — rather than becoming three sibling variants, and
/// why `oid` deliberately stays an `Other(PgType::Oid)` instead of joining
/// this family.
///
/// [`ValueType::Float`] is the second promotion and finishes
/// the job: `real`/`double precision` were the last two types still sharing
/// [`ValueType::Numeric`] with `numeric` itself, despite being fixed-width
/// *binary* floats with their own arithmetic (inexact, non-associative,
/// overflowing), their own special values (`NaN`/`±Infinity`) and their own
/// deliberately-non-IEEE comparison order. See [`crate::float`] for the full
/// rationale, including why `NaN = NaN` is true here and why ±0's text
/// instability is what keeps floats off the key roles.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueType {
    Numeric,
    /// An exact integer of a known Postgres width. Match `Integer(_)` unless
    /// the width genuinely matters (range/overflow, DDL rendering).
    Integer(IntWidth),
    /// An IEEE binary float of a known Postgres width. Match `Float(_)`
    /// unless the width genuinely matters (rounding grid, overflow, DDL
    /// rendering).
    Float(FloatWidth),
    Text,
    Boolean,
    Uuid,
    Other(PgType),
}

impl ValueType {
    /// Whether this is an *exact-numeric-family* type — either
    /// [`ValueType::Numeric`] itself or any [`ValueType::Integer`] width.
    ///
    /// This is the set Postgres's implicit `int -> numeric` coercion makes
    /// mutually admissible, and it is the single predicate every site that
    /// used to test `== ValueType::Numeric` for "can this be added /
    /// compared / summed" should use instead (see
    /// [`super::registry::operator_result_type`],
    /// [`super::registry::aggregate_result_type`], and `validate`'s
    /// `COALESCE` unification). Writing the test this way, rather than
    /// enumerating widths at each call site, is what keeps a future width
    /// (or a future decision to admit `oid` here) a one-line change.
    pub fn is_exact_numeric_family(self) -> bool {
        matches!(self, ValueType::Numeric | ValueType::Integer(_))
    }

    /// Whether this is *any* numeric type — [`Self::is_exact_numeric_family`]
    /// widened with [`ValueType::Float`].
    ///
    /// This is the admissibility set for `+`, `>` and the four aggregates,
    /// because Postgres admits a float operand everywhere it admits an
    /// exact one. The two predicates are deliberately separate rather than
    /// one: the *exact* family is the set that is closed under arbitrary
    /// precision (so `int + numeric` cannot overflow and `sum(int8)` is
    /// `numeric`), while a float operand pulls the result out of that
    /// family entirely — `real + numeric` is `double precision`, not
    /// `numeric`. Anything asking "can these be added" wants this one;
    /// anything asking "does the arbitrary-precision path apply" wants the
    /// other.
    pub fn is_numeric_family(self) -> bool {
        self.is_exact_numeric_family() || matches!(self, ValueType::Float(_))
    }
}

impl fmt::Display for ValueType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ValueType::Numeric => write!(f, "numeric"),
            ValueType::Integer(width) => write!(f, "{width}"),
            ValueType::Float(width) => write!(f, "{width}"),
            ValueType::Text => write!(f, "text"),
            ValueType::Boolean => write!(f, "boolean"),
            ValueType::Uuid => write!(f, "uuid"),
            ValueType::Other(pg_type) => write!(f, "{pg_type}"),
        }
    }
}

/// An operator accepted by the expression grammar; see [`super::registry`]
/// for the list the parser validates against and the renderer reuses.
/// [`Operator::Add`] is `Numeric, Numeric -> Numeric`; [`Operator::GreaterThan`]
/// is `Numeric, Numeric -> Boolean`, matching Postgres's `>` on
/// `numeric` exactly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operator {
    Add,
    GreaterThan,
}

/// Renders `def` back into valid Trellis grammar text — the inverse of
/// [`super::parser::parse`] — so [`super::catalog::alter_transform`] can
/// persist an edited field list the same way [`super::catalog`] persists
/// every definition: as `definition_text`, re-parsed on read (see that
/// module's doc comment for why), never a serialized AST. Only
/// `ALTER TRANSFORM`'s caller needs this (a first `TRANSFORM ...` statement
/// is already source text, verbatim); it lives here, next to the AST it
/// renders, rather than in `catalog`.
///
/// Always fully parenthesizes a [`Expr::BinaryOp`] rather than reproducing
/// [`super::registry`]'s precedence table in reverse — verbose, but
/// unconditionally round-trip-safe: [`super::parser::parse`] accepts a
/// parenthesized subexpression anywhere `docs/decisions/0004-transform-definition-grammar.md`'s
/// grammar accepts one, and a fresh render never needs to *look* like the
/// original text, only to re-parse into the same [`TransformDef`].
pub(crate) fn render_definition_text(def: &TransformDef) -> String {
    let mut out = String::from("TRANSFORM ");
    render_table_ref(&mut out, &def.target, def.explicit_target_schema.as_deref());
    out.push_str(" FROM ");
    render_table_ref(&mut out, &def.source, def.explicit_source_schema.as_deref());

    match &def.key_space {
        KeySpace::OneToOne => {}
        KeySpace::Aggregate { group_by } => {
            out.push_str(" GROUP BY ");
            out.push_str(&group_by.join(", "));
        }
        KeySpace::Join(join) => {
            let qualified = |column: &QualifiedColumn| match &column.schema {
                Some(schema) => format!("{schema}.{}.{}", column.table, column.column),
                None => format!("{}.{}", column.table, column.column),
            };
            out.push_str(match join.kind {
                JoinKind::Inner => " INNER JOIN ",
                JoinKind::Left => " LEFT JOIN ",
            });
            render_table_ref(&mut out, &join.right, join.explicit_right_schema.as_deref());
            out.push_str(" ON ");
            let terms: Vec<String> = join
                .on
                .iter()
                .map(|term| format!("{} = {}", qualified(&term.lhs), qualified(&term.rhs)))
                .collect();
            out.push_str(&terms.join(" AND "));
            out.push_str(" GROUP BY ");
            let keys: Vec<String> = join.group_by.iter().map(qualified).collect();
            out.push_str(&keys.join(", "));
        }
    }

    if def.without_source_index {
        out.push_str(" WITHOUT SOURCE INDEX");
    }

    out.push_str(" SELECT ");
    let fields: Vec<String> = def
        .fields
        .iter()
        .map(|field| format!("{} AS {}", render_field_expr(&field.expr), field.name))
        .collect();
    out.push_str(&fields.join(", "));

    // `def.predicate` is always `Predicate::True` (the grammar's only
    // variant, and the same value an omitted `WHERE` parses to) — omitting
    // the clause entirely round-trips identically to writing `WHERE TRUE`,
    // so there is nothing this render ever needs to spell out.
    let Predicate::True = def.predicate;

    out
}

fn render_table_ref(out: &mut String, table: &str, explicit_schema: Option<&str>) {
    if let Some(schema) = explicit_schema {
        out.push_str(schema);
        out.push('.');
    }
    out.push_str(table);
}

pub(crate) fn render_field_expr(expr: &Expr) -> String {
    match expr {
        Expr::Column(name) => name.clone(),
        Expr::NumberLiteral(text) => text.clone(),
        Expr::StringLiteral(text) => format!("'{}'", text.replace('\'', "''")),
        Expr::TypedLiteral { value_type, text } => format!(
            "CAST('{}' AS {})",
            text.replace('\'', "''"),
            typed_literal_keyword(*value_type)
        ),
        Expr::SourceColumn {
            schema,
            table,
            column,
        } => qualified_column_text(schema.as_deref(), table, column),
        Expr::BinaryOp { op, lhs, rhs } => format!(
            "({} {} {})",
            render_field_expr(lhs),
            render_operator(*op),
            render_field_expr(rhs)
        ),
        // `COUNT(*)` is the one call this grammar parses with an empty
        // argument list (`*` is not itself an expression) — every other
        // empty-args shape is unreachable (every other function/aggregate
        // this grammar accepts requires at least one argument), so this
        // check unambiguously identifies it rather than merely guessing.
        Expr::FunctionCall { name, args } if name == "COUNT" && args.is_empty() => {
            "COUNT(*)".to_string()
        }
        Expr::FunctionCall { name, args } => {
            let rendered_args: Vec<String> = args.iter().map(render_field_expr).collect();
            format!("{name}({})", rendered_args.join(", "))
        }
    }
}

impl FieldDef {
    /// The column this field passes through, if its expression is exactly one
    /// column reference that reads a column rather than another field: a
    /// qualified [`Expr::SourceColumn`], its own name (`val AS val`), or a
    /// bare name no other field of `fields` takes. Whether that column exists
    /// on the source is the caller's to check.
    pub(crate) fn passthrough_column<'a>(&'a self, fields: &[FieldDef]) -> Option<&'a str> {
        match &self.expr {
            Expr::SourceColumn { column, .. } => Some(column),
            Expr::Column(name) if name == &self.name || !fields.iter().any(|f| &f.name == name) => {
                Some(name)
            }
            _ => None,
        }
    }
}

/// How an [`Expr::SourceColumn`] is written: `<table>.<column>`, or
/// `<schema>.<table>.<column>` when a schema is given.
pub(crate) fn qualified_column_text(schema: Option<&str>, table: &str, column: &str) -> String {
    match schema {
        Some(schema) => format!("{schema}.{table}.{column}"),
        None => format!("{table}.{column}"),
    }
}

fn render_operator(op: Operator) -> &'static str {
    match op {
        Operator::Add => "+",
        Operator::GreaterThan => ">",
    }
}

/// The typed-literal keyword [`super::typed_literal::TYPED_LITERALS`] spells
/// `value_type` with — the reverse of [`super::typed_literal::lookup_typed_literal`].
/// Every allowlisted entry's keyword is, by that table's own invariant,
/// unique per [`ValueType`] it produces (`type_keyword_matches_value_type`
/// pins the forward half of that bijection), so a [`Expr::TypedLiteral`]
/// already carrying one of those [`ValueType`]s always finds its keyword
/// back here.
fn typed_literal_keyword(value_type: ValueType) -> &'static str {
    super::typed_literal::TYPED_LITERALS
        .iter()
        .find(|spec| spec.value_type == value_type)
        .map(|spec| spec.keyword)
        .unwrap_or_else(|| {
            panic!(
                "render_definition_text: no typed-literal keyword registered for {value_type} — \
                 every TypedLiteral this grammar can parse must have one"
            )
        })
}

/// The partial-data predicate slot (`docs/transforms.md#partial-data`).
///
/// This slice accepts only a trivially-true predicate — either `WHERE` is
/// omitted, or written as the literal `WHERE TRUE`. General predicate
/// expressions are deferred to a later issue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Predicate {
    True,
}
