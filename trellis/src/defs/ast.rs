//! The typed AST a transform definition parses into.
//!
//! Shapes mirror the logical model in `docs/transforms.md`: a definition
//! picks a key-space, a set of calculated fields, and a partial-data
//! predicate. This slice (issue #22) only populates the 1-1 key-space and
//! the `+`-only expression language; [`KeySpace`] and [`Predicate`] have
//! room to grow (aggregate/cross-join variants, general predicates) without
//! changing the shape callers already match on.
//!
//! Issue #63 widens the value model from numeric-only to [`ValueType`]'s
//! three variants; see `docs/transforms.md` and ADR-0004's growth policy.

use std::fmt;

use super::pg_type::PgType;
use crate::float::FloatWidth;
use crate::integer::IntWidth;

/// A parsed transform definition, ready for the validator (issue #23) and
/// evaluator (issue #24).
///
/// **`target`/`source` are always bare table names, never a dotted
/// `schema.table` spelling — even when the definition explicitly qualified
/// one (issue #76, ADR-0007 grammar clause 4).** That's a deliberate split,
/// not an oversight: a huge number of call sites (`backfill.rs`, `oracle.rs`,
/// `ddl.rs`, `apply.rs`, `apply_aggregate.rs`, `quarantine.rs`) pass
/// `&def.source`/`&def.target` straight into `quote_ident`, which quotes its
/// argument as a *single* identifier — handing it `"schema.table"` would
/// quote the dot right along with it, producing an invalid, never-resolving
/// identifier instead of a schema-qualified one. `intake::publication::qualify`
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
    /// explicitly spelled `<schema>.<target>` (issue #76) rather than a bare
    /// table name. When present, [`super::catalog::create_definition_inner`]
    /// validates that *exact* relation (schema and table both, via
    /// `information_schema.tables`) and persists it qualified, skipping
    /// `Config::target_schema` resolution entirely — a qualified spelling
    /// names its own schema, it doesn't inherit the configured default.
    /// `None` (the bare, far more common case) keeps issue #73's
    /// `Config::target_schema`-resolved behavior exactly as it was.
    pub explicit_target_schema: Option<String>,
    pub source: String,
    /// The source-side twin of [`Self::explicit_target_schema`]: `Some(schema)`
    /// when `FROM <schema>.<source>` was written explicitly, in which case
    /// `create_definition_inner` validates that exact relation instead of
    /// walking `search_path` the way issue #72's bare-name resolution
    /// ([`super::catalog::resolve_source_schema_in_txn`]) does. `None` for a
    /// bare `FROM <source>`, unchanged from before issue #76.
    pub explicit_source_schema: Option<String>,
    pub key_space: KeySpace,
    pub fields: Vec<FieldDef>,
    pub predicate: Predicate,
}

/// A parsed standalone relationship declaration (ADR-0006):
/// `RELATIONSHIP <name> FROM <from_table>.<fk_col> TO <to_table>.<pk_col>`.
///
/// This slice (issue #24) is grammar + AST only. Catalog storage
/// ([`super::catalog::create_relationship`]) and endpoint/cardinality
/// validation (issue #27) build on top of it; referencing a relationship
/// from a calculated field's expression is still deferred — see
/// [`super::ast::Expr::RelationshipPath`]'s doc comment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationshipDef {
    pub name: String,
    pub from_table: String,
    pub from_col: String,
    pub to_table: String,
    pub to_col: String,
}

/// One whole statement in Trellis's grammar, as
/// [`super::parser::parse_statement`] returns it — the single shape
/// [`crate::Trellis::apply`] dispatches on (issue #227, ADR-0012: "parsing
/// the statement is where the operation is decided; the facade signature does
/// not change as operations are added").
///
/// Every variant's leading keyword is what distinguishes it, so the parser
/// needs no lookahead past the first token to choose: `TRANSFORM` /
/// `RELATIONSHIP` define, `PAUSE` / `RESUME` / `DROP` act on something
/// already defined. The kind keyword is repeated on the imperative forms
/// (`PAUSE TRANSFORM x`, not `PAUSE x`) because transforms and relationships
/// do *not* share a namespace (issue #228, decision 1).
///
/// **Pause and resume are transform-only**, which is why they carry a
/// [`TransformRef`] while `DROP` carries a [`DefinitionRef`] that can name
/// either kind. A relationship is a reusable *component* of a transform, not
/// something that does anything on its own — there is no work of its own to
/// suspend, so there is nothing a pause could mean. (It matches ADR-0014's own
/// reasoning for having no `pause_relationship`: a relationship carries no
/// lifecycle status and nothing in the fold gates on one.) Dropping one is a
/// different matter — a relationship is a definition, and definitions can be
/// retired — so `DROP RELATIONSHIP` exists. Spelling the distinction in the
/// types rather than in a runtime check is what keeps the grammar and the
/// available operations the same set, per ADR-0004's "the accepted language
/// *is* the spec".
///
/// `AMEND` is deliberately absent: it needs its own semantics ADR before it
/// can be a grammar addition (issue #228, decision 3).
#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    /// `TRANSFORM <target> FROM <source> ... SELECT ...`
    DefineTransform(TransformDef),
    /// `RELATIONSHIP <name> FROM <table>.<col> TO <table>.<col>`
    DefineRelationship(RelationshipDef),
    /// `PAUSE TRANSFORM <target>[.<column>]` — ADR-0014's operator-driven
    /// freeze. Idempotent.
    Pause(TransformRef),
    /// `RESUME TRANSFORM <target>[.<column>]` — ADR-0014's
    /// rebuild-by-backfill recovery (*not* a catch-up).
    Resume(TransformRef),
    /// `DROP TRANSFORM <target>` /
    /// `DROP RELATIONSHIP [<schema>.]<from_table>.<name>` — ADR-0014's
    /// terminal reap. Idempotent; refused if a still-registered dependent
    /// chains off the subject.
    Drop(DefinitionRef),
    /// `ALTER TRANSFORM <target> <clause>[, <clause> ...]` (ADR-0015, issues
    /// #241/#242) — edits an already-registered transform's calculated
    /// fields in place: `ADD`, `DROP`, and `ALTER` clauses, one calculated
    /// field each. The key-space is not named and cannot change; a
    /// granularity change is a new transform plus cutover, not an edit — the
    /// grammar has no key-space clause here to write, rather than a
    /// validator rejecting one. Idempotent per-clause, matching
    /// pause/resume/drop's discipline: re-adding a field with the same
    /// formula, re-altering to the formula it already has, and dropping an
    /// already-absent field are all no-op successes.
    AlterTransform(AlterTransform),
}

impl Statement {
    /// Which form this statement is — the same decision
    /// [`crate::Trellis::apply`] dispatches on, with the parsed payload
    /// dropped.
    pub fn kind(&self) -> StatementKind {
        match self {
            Statement::DefineTransform(_) => StatementKind::Transform,
            Statement::DefineRelationship(_) => StatementKind::Relationship,
            Statement::Pause(_) => StatementKind::Pause,
            Statement::Resume(_) => StatementKind::Resume,
            Statement::Drop(_) => StatementKind::Drop,
            Statement::AlterTransform(_) => StatementKind::Alter,
        }
    }
}

/// Which form a statement of Trellis's grammar is, without what it says —
/// what [`crate::statement_kind`] returns (issue #580).
///
/// One variant per form [`crate::Trellis::apply`] carries out, each named for
/// the keyword that leads it, since that keyword alone is what the parser
/// picks the form by. `DROP TRANSFORM` and `DROP RELATIONSHIP` are one
/// [`StatementKind::Drop`], as they are one [`crate::Applied::Dropped`].
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
    Transform,
    /// `RELATIONSHIP <name> FROM <table>.<col> TO <table>.<col>` — defines a
    /// relationship.
    Relationship,
    /// `PAUSE TRANSFORM <target>[.<column>]`.
    Pause,
    /// `RESUME TRANSFORM <target>[.<column>]`.
    Resume,
    /// `DROP TRANSFORM <target>` or `DROP RELATIONSHIP [<schema>.]<table>.<name>`.
    Drop,
    /// `ALTER TRANSFORM <target> <clause>[, <clause> ...]`.
    Alter,
}

impl StatementKind {
    /// Every variant, once — the list a binding checks its own mapping
    /// against, since `#[non_exhaustive]` stops any crate but this one from
    /// matching the enum exhaustively. `every_statement_kind_is_listed_in_all`
    /// fails until a new variant is added here.
    pub const ALL: [StatementKind; 6] = [
        StatementKind::Transform,
        StatementKind::Relationship,
        StatementKind::Pause,
        StatementKind::Resume,
        StatementKind::Drop,
        StatementKind::Alter,
    ];

    /// A stable, lowercase name for this kind: its leading keyword,
    /// lowercased — the form a host language keys off of.
    pub fn as_str(self) -> &'static str {
        match self {
            StatementKind::Transform => "transform",
            StatementKind::Relationship => "relationship",
            StatementKind::Pause => "pause",
            StatementKind::Resume => "resume",
            StatementKind::Drop => "drop",
            StatementKind::Alter => "alter",
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
/// `target` is the **bare** target-table name, never a `schema.table`
/// spelling — a dotted address is `<transform>.<column>` (issue #228,
/// decision 2: [`crate::QuarantineTarget`]'s existing
/// `"transform"`/`"transform.column"` addressing, folded into this grammar).
/// That is the one place this grammar's addressing deliberately diverges from
/// `TRANSFORM`/`FROM`'s own `[<schema>.]<table>` table references: every
/// operator-facing entry point below the facade
/// ([`super::lifecycle::pause_transform`],
/// [`crate::staging::quarantine::resume_transform`],
/// [`crate::Trellis::status`]) already identifies a transform by its bare
/// name, and `<transform>.<column>` and `<schema>.<transform>` are the same
/// token shape — so one of the two readings had to win, and the column reading
/// is the one the engine can actually resolve.
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

/// What a [`Statement::Drop`] addresses — the one statement that can name
/// either kind of definition.
///
/// **The two kinds address differently, on purpose** (issue #228,
/// decision 1). A transform's identity is its target table, which is unique on
/// its own, so it is addressed bare. A relationship's name is unique only *per
/// from-table* (`relationship_definitions`' own unique constraint), so a bare
/// relationship name doesn't identify anything — the address is always scoped
/// to the from-table, and a bare one is a parse error
/// ([`super::error::ParseError::UnscopedRelationshipAddress`]) rather than a
/// lookup that guesses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DefinitionRef {
    /// `TRANSFORM <target>` — the bare target-table name.
    ///
    /// No column half, unlike [`TransformRef`]: dropping one calculated field
    /// is an `ALTER TRANSFORM ... DROP <field>`, a different operation
    /// entirely (issue #228 decision 2; issues #241/#242), so a column address
    /// has nothing to parse into here and the parser refuses one.
    Transform(String),
    /// `[<schema>.]<from_table>.<relationship_name>`.
    ///
    /// `schema` is the optional leading qualifier of the *from-table*. A
    /// relationship's identity is `(from_schema, from_table, name)` (issues
    /// #285/#288), where `from_schema` is the schema the bare `from_table`
    /// resolved to when the relationship was declared — the `RELATIONSHIP`
    /// grammar has no schema-qualified endpoint spelling of its own. A
    /// qualified address names exactly that relationship; a bare one names
    /// whichever single schema declares `from_table.name`, and is refused as
    /// ambiguous when more than one does. See
    /// `defs::catalog::relationship_at_address`.
    Relationship {
        schema: Option<String>,
        from_table: String,
        name: String,
    },
}

impl fmt::Display for DefinitionRef {
    /// Renders the address back in the spelling the grammar accepts, so an
    /// error message can name what it was handed.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DefinitionRef::Transform(target) => write!(f, "{target}"),
            DefinitionRef::Relationship {
                schema: None,
                from_table,
                name,
            } => write!(f, "{from_table}.{name}"),
            DefinitionRef::Relationship {
                schema: Some(schema),
                from_table,
                name,
            } => write!(f, "{schema}.{from_table}.{name}"),
        }
    }
}

/// `ALTER TRANSFORM <target> <clause>[, <clause> ...]` (ADR-0015, issues
/// #241/#242).
///
/// `target` is the **bare** target-table name — the same operator-facing
/// addressing [`TransformRef`]/[`DefinitionRef::Transform`] use, not the
/// `TRANSFORM`/`FROM` grammar's schema-qualifiable table reference: an edit
/// acts on an already-registered definition by its bare identity, exactly
/// like `PAUSE`/`RESUME`/`DROP TRANSFORM` do, rather than re-declaring the
/// table.
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
/// [`KeySpace::Aggregate`] (issue #11's groundwork) is a `GROUP BY <cols>`
/// definition, whose target's primary key is the grouping columns rather
/// than an inherited source column. Cross-join key-spaces are still rejected
/// at parse time with a specific error rather than represented here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeySpace {
    OneToOne,
    /// `group_by` holds the keys named after `GROUP BY`, in the order they
    /// were written — that order becomes the target table's composite
    /// primary key column order. Issue #137 widened each key from a bare
    /// source column name to [`GroupByKey`], which also admits a to-one
    /// relationship path (`rel.column`).
    Aggregate {
        group_by: Vec<GroupByKey>,
    },
}

/// One `GROUP BY` key (issue #137): either a plain source column, or a
/// to-one relationship path (`rel.column`) — grouping by a linked row's
/// column without that column ever appearing on the source table itself
/// (e.g. `GROUP BY tag, post.author` on `post_tags`, where `author` lives on
/// `posts` via the `post` relationship). A **to-many** relationship path is
/// rejected by the validator, not represented here — grouping by "the many
/// child rows on the other end of a to-many relationship" has no defined
/// semantics.
///
/// There is no separate aliasing syntax for a `GROUP BY` key (unlike a
/// `SELECT <expr> AS <name>` field): a plain column's target column name is
/// its own name, and a relationship path's target column name is its tail
/// `column` — see [`Self::target_column_name`]. Every call site that used to
/// treat a `group_by` entry as a bare `String` (target column name,
/// dedup/lookup key, DDL column name, …) should use that method instead of
/// assuming the entry itself is the name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroupByKey {
    Column(String),
    RelationshipPath { rel: String, column: String },
}

impl GroupByKey {
    /// The target table's column name this key becomes — the plain column's
    /// own name, or a relationship path's tail `column` (there is no
    /// separate aliasing syntax for a `GROUP BY` key, so `post.author`
    /// becomes target column `author`, exactly like `tag` becomes target
    /// column `tag`).
    pub fn target_column_name(&self) -> &str {
        match self {
            GroupByKey::Column(name) => name,
            GroupByKey::RelationshipPath { column, .. } => column,
        }
    }

    /// This key, as the [`Expr`] it's semantically equivalent to — a plain
    /// [`Expr::Column`] or [`Expr::RelationshipPath`] — so a renderer that
    /// already knows how to qualify/join an arbitrary expression (e.g.
    /// [`super::oracle::render_to_one_rel_expr_sql`]) can render a `GROUP BY`
    /// key without a second, key-specific rendering path.
    pub fn as_expr(&self) -> Expr {
        match self {
            GroupByKey::Column(name) => Expr::Column(name.clone()),
            GroupByKey::RelationshipPath { rel, column } => Expr::RelationshipPath {
                rel: rel.clone(),
                column: column.clone(),
            },
        }
    }
}

/// Whether `name` is one of `group_by`'s keys' target column names — the
/// common "is this field a `GROUP BY` passthrough" check every layer that
/// touches an [`super::ast::KeySpace::Aggregate`] definition's fields needs
/// (DDL's column dedup, `staging::apply_aggregate`'s field-plan skip, the
/// reverse-relationship shape's field-expr filter, …), pulled out once here
/// rather than re-implemented at each call site.
pub fn group_by_contains(group_by: &[GroupByKey], name: &str) -> bool {
    group_by.iter().any(|key| key.target_column_name() == name)
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
    /// A numeric literal, kept as the source text so the evaluator picks
    /// the numeric type/precision rather than the parser.
    NumberLiteral(String),
    /// A single-quoted string literal (issue #63).
    StringLiteral(String),
    /// A **typed literal** (issue #109): a constant of a Postgres type that
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
    /// Issue #109 typed this field as a [`PgType`], since the allowlist then
    /// held only passthrough families. Issue #112 widened it to a full
    /// [`ValueType`] so `REAL '1.5'` / `CAST('1.5' AS double precision)` can
    /// produce a first-class [`ValueType::Float`] — floats need this grammar
    /// because Postgres gives them no bare literal syntax at all
    /// (`pg_typeof(1.5)` is `numeric`).
    TypedLiteral { value_type: ValueType, text: String },
    /// A `<rel>.<column>` relationship-path reference (issue #25, ADR-0006).
    /// `rel` is the head's **relationship name**, not a table/alias —
    /// resolving whether it's an actually-declared relationship, and its
    /// cardinality, is deferred to later validation/eval issues; this
    /// variant is grammar + AST only.
    RelationshipPath { rel: String, column: String },
    BinaryOp {
        op: Operator,
        lhs: Box<Expr>,
        rhs: Box<Expr>,
    },
    /// A `name(args)` function call (issue #64) — the first function-call
    /// syntax the grammar accepts. `name` is the uppercased canonical form
    /// looked up in [`super::registry::FUNCTIONS`]; arity and argument types
    /// are checked against that same registry entry, not hardcoded here.
    ///
    /// In an [`KeySpace::Aggregate`] definition, `name` may instead be one of
    /// `SUM`/`MIN`/`MAX`/`AVG` (looked up in
    /// [`super::registry::AGGREGATE_FUNCTION_SPECS`]) with a single orderable
    /// argument (issue #120 generalized `MIN`/`MAX` past `Numeric` to any
    /// type with a real Postgres ordering — see
    /// [`super::registry::aggregate_result_type`]), or `COUNT` with either an
    /// empty `args` (`COUNT(*)` row-counting, issue #75 — `*` is not itself
    /// an expression, so this is the one arity-0 shape) or a single argument
    /// of any type (`COUNT(<expr>)`, issue #120 — counts non-null
    /// occurrences of `<expr>`, a different semantic from `COUNT(*)`).
    /// There's no separate AST node for aggregate calls, since they're
    /// syntactically identical `name(args)` calls, just resolved against a
    /// different registry depending on key-space.
    FunctionCall { name: String, args: Vec<Expr> },
}

/// The value type a column or expression carries (issue #63). `Text` and
/// `Boolean` are plumbed through so columns of those types can be declared
/// and passed through a calculated field; issue #64 added `Text`-argument
/// functions, and issue #65 adds the `>` comparison operator, the first
/// operator whose result type differs from its operands' (`Numeric,
/// Numeric -> Boolean`). `Uuid` (issue #79) is narrower still: it's
/// representable, comparable, and passthrough-able (including as an
/// aggregate `GROUP BY` key), but has no arithmetic/regex operations the way
/// `Numeric`/`Text` do — there's no real-world use for `uuid + uuid`, so
/// [`super::registry`] never grants it an operator or function.
///
/// [`ValueType::Other`] (issue #108) widens this from a 4-bucket lattice to
/// an open one: any Postgres type the [`super::pg_type`] OID registry
/// recognizes as a distinct family, but that hasn't yet earned its own
/// first-class variant here, carries its [`PgType`] tag instead of silently
/// collapsing into `Text` the way it used to. Per `docs/type-support.md`,
/// it's passthrough-only — [`super::registry`] grants it no operator or
/// function, [`super::validate`] rejects it as a key/predicate/computed
/// target — until the type family's own epic child (#109, #111–#122)
/// promotes it to a real variant with real semantics. This issue's job is
/// only to stop lying about what the column is, not to add those semantics.
///
/// [`ValueType::Integer`] (issue #111) is the first such promotion, and the
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
/// [`ValueType::Float`] (issue #112) is the second promotion and finishes
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
    /// widened with [`ValueType::Float`] (issue #112).
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
/// for the list the parser validates against and the evaluator reuses.
/// [`Operator::Add`] is `Numeric, Numeric -> Numeric`; [`Operator::GreaterThan`]
/// (issue #65) is `Numeric, Numeric -> Boolean`, matching Postgres's `>` on
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

    if let KeySpace::Aggregate { group_by } = &def.key_space {
        out.push_str(" GROUP BY ");
        let keys: Vec<String> = group_by
            .iter()
            .map(|key| match key {
                GroupByKey::Column(column) => column.clone(),
                GroupByKey::RelationshipPath { rel, column } => format!("{rel}.{column}"),
            })
            .collect();
        out.push_str(&keys.join(", "));
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

fn render_field_expr(expr: &Expr) -> String {
    match expr {
        Expr::Column(name) => name.clone(),
        Expr::NumberLiteral(text) => text.clone(),
        Expr::StringLiteral(text) => format!("'{}'", text.replace('\'', "''")),
        Expr::TypedLiteral { value_type, text } => format!(
            "CAST('{}' AS {})",
            text.replace('\'', "''"),
            typed_literal_keyword(*value_type)
        ),
        Expr::RelationshipPath { rel, column } => format!("{rel}.{column}"),
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
