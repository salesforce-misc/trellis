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
    /// [`super::registry::AGGREGATE_FUNCTION_SPECS`]) with a single Numeric
    /// argument, or `COUNT` (issue #75) with an empty `args` — `COUNT(*)`
    /// row-counting, the only `COUNT` shape this grammar accepts — since
    /// `*` is not itself an expression. There's no separate AST node for
    /// aggregate calls, since they're syntactically identical `name(args)`
    /// calls, just resolved against a different registry depending on
    /// key-space.
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueType {
    Numeric,
    Text,
    Boolean,
    Uuid,
}

impl fmt::Display for ValueType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ValueType::Numeric => write!(f, "numeric"),
            ValueType::Text => write!(f, "text"),
            ValueType::Boolean => write!(f, "boolean"),
            ValueType::Uuid => write!(f, "uuid"),
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

/// The partial-data predicate slot (`docs/transforms.md#partial-data`).
///
/// This slice accepts only a trivially-true predicate — either `WHERE` is
/// omitted, or written as the literal `WHERE TRUE`. General predicate
/// expressions are deferred to a later issue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Predicate {
    True,
}
