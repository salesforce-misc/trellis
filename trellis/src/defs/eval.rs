//! Evaluator for the 1-1 subset (issue #24), widened by issue #63 from
//! numeric-only to [`Value`]'s three variants (`Numeric`/`Text`/`Boolean`).
//!
//! Pure function: a source-row image plus a validated [`TransformDef`] in,
//! the target row's calculated columns out. No database access here — the
//! row image is "the staged post-image at the claimed position" (stage 05),
//! not a live read, so evaluating a staged image and evaluating the
//! equivalent live row always agree (there's exactly one code path).
//!
//! [`super::validate`] is responsible for guaranteeing a definition's
//! expressions are well-typed (e.g. `+`'s operands are both `Numeric`)
//! before it reaches here, so [`apply_operator`] doesn't need to return a
//! type error — the same "validator's job, not the evaluator's" split
//! [`super::validate`]'s module docs describe for [`super::ast::KeySpace`]
//! and [`super::ast::Predicate`].
//!
//! **Open question**: the interface this hands to apply (#11) — this
//! returns [`Value`]s; whether the SQL write path wants those, or their
//! canonical text form, or something else, is undecided (called out in the
//! issue #24 ticket, still open post-#63 since only `Numeric` values are
//! wired into the staging apply path today).

use std::collections::{HashMap, HashSet};
use std::fmt;

use regex::Regex;

use crate::numeric::{Numeric, NumericParseError};

use super::ast::{Expr, FieldDef, GroupByKey, KeySpace, Operator, TransformDef, ValueType};
use super::model::RelationshipCardinality;
use super::pg_type::PgType;
use crate::error_code::ErrorCode;

/// A source-row image: column name to its text value, or `None` for SQL
/// `NULL`. This is the staged post-image, not a live database row — the
/// evaluator never touches Postgres.
pub type Row = HashMap<String, Option<String>>;

/// Memoizes `regexp_count`'s compiled [`Regex`] per pattern literal (issue
/// #68), so a caller driving `evaluate` across many rows for the same
/// [`TransformDef`] compiles each distinct pattern once rather than on every
/// row. The validator (#64) already restricts `regexp_count`'s pattern
/// argument to a string literal, so the same pattern text always means the
/// same compiled `Regex` — caching by that text is exact, not a heuristic.
/// A fresh, empty cache is always safe to pass (just costs the first-row
/// compile); reusing one across `evaluate` calls for the same definition is
/// what avoids the per-row recompilation.
pub type RegexCache = HashMap<String, Regex>;

/// One to-one relationship's read-side data, everything the evaluator needs
/// to resolve a `<rel>.<column>` path (issue #28): the from-row column that
/// holds the join key, the to-side rows to look that key up in, and the
/// to-side column types so a referenced column's text parses the same way a
/// source column's does.
///
/// `to_rows_by_key` is indexed by the *text* of the to-side join column
/// (`to_col`), and the from-row's join key is matched against it by the same
/// text. That's exact for the join-key types Trellis relationships actually
/// use as a to-side `PRIMARY KEY`/`UNIQUE` column — integers, UUIDs, text —
/// whose canonical text encoding is stable; it is *not* scale-insensitive for
/// a fractional-numeric key (Postgres treats `1` and `1.0` as equal in a
/// join, this index would not — issue #110's *type*-lossiness axis, still
/// open, blocked on #108). A to-side row whose `to_col` is `NULL` must
/// be omitted from the index by the builder: SQL `NULL` never joins, so it
/// has no key.
///
/// This `NULL`-exclusion is deliberately **not** issue #110's NULL-lossiness
/// bug: this is a real SQL join (a relationship's `from_col = to_col`), where
/// ANSI `NULL <> NULL` is the *correct* semantics (a from-row with a `NULL`
/// join value has no related row, same as `LEFT JOIN ON from_col = to_col`
/// would compute) — unlike an aggregate group's downstream propagation
/// identity (`staging::apply_aggregate::derive_group_key`/
/// `staging::apply::read_live_rows_batch`), which is a *primary-key*
/// round-trip ("is this the same group as before"), not a join, and where a
/// `NULL` component legitimately needs to resolve to itself. Confirmed by
/// reading this module during #110's investigation: no change needed here.
pub struct ToOneRelationship {
    /// The from-row column whose value is the join key (the relationship's
    /// `from_col`).
    pub from_col: String,
    /// The relationship's cardinality, so a to-*many* relationship referenced
    /// as a bare path can be rejected here rather than silently taking an
    /// arbitrary matching row (aggregate-wrapped to-many is issue #29).
    pub cardinality: RelationshipCardinality,
    /// The to-side column value-types, used to parse a referenced column's
    /// text. A column absent here defaults to `Numeric`, matching the
    /// [`Row`]-column handling in [`eval_expr`].
    pub to_columns: HashMap<String, ValueType>,
    /// The to-side rows, keyed by their `to_col` text value. A from-row whose
    /// join key is absent (or `NULL`) has no match — LEFT JOIN semantics, the
    /// enrichment column evaluates to `NULL`.
    pub to_rows_by_key: HashMap<String, Row>,
}

/// One to-many relationship's read-side data (issue #29): the aggregate
/// counterpart to [`ToOneRelationship`]. Where a to-one path resolves a single
/// related row, a to-many path is only legal wrapped in an aggregate
/// (`SUM(comments.word_count)`, ADR-0006), which folds over the *set* of
/// related to-side rows sharing the from-row's join value. That set is what
/// `to_rows_by_key` holds — a `Vec<Row>` per join-key text, instead of the
/// single `Row` a to-one carries.
///
/// The join value is keyed by the same raw *text* as [`ToOneRelationship`]
/// (see that type's note on scale-insensitivity for fractional-numeric keys),
/// for consistency; a from-row whose join key is absent from the map (or
/// `NULL`) has an empty related set — the aggregate's empty result
/// (`COUNT` → `0`, `SUM`/`MIN`/`MAX`/`AVG` → `NULL`).
pub struct ToManyRelationship {
    /// The from-row column whose value is the join key (the relationship's
    /// `from_col`), matched against the to-side rows' key text.
    pub from_col: String,
    /// The to-side column value-types, used to parse a referenced column's
    /// text, exactly as [`ToOneRelationship::to_columns`].
    pub to_columns: HashMap<String, ValueType>,
    /// The related to-side rows grouped by their join-value text. A key with
    /// no entry (or a `NULL` from-side join key) is the empty set.
    pub to_rows_by_key: HashMap<String, Vec<Row>>,
}

/// The relationships available to the evaluator, keyed by the relationship
/// name that heads a `<rel>.<column>` path. The pure [`evaluate`] entry
/// supplies an empty one (matching pre-#28 behavior: a path then errors with
/// [`EvalError::UnknownRelationship`]); [`evaluate_with_relationships`] threads
/// a populated one built by the caller (the staging integration is issue #30).
///
/// To-one relationships (a bare `<rel>.<column>` path, issue #28) live in
/// `by_name`; to-many relationships (an aggregate-wrapped path, issue #29) in
/// `to_many_by_name`. A given relationship name resolves as exactly one of the
/// two — the two maps are disjoint.
#[derive(Default)]
pub struct RelationshipContext {
    by_name: HashMap<String, ToOneRelationship>,
    to_many_by_name: HashMap<String, ToManyRelationship>,
}

impl RelationshipContext {
    /// Builds a context from relationship-name to its resolved to-one data,
    /// with no to-many relationships.
    pub fn new(by_name: HashMap<String, ToOneRelationship>) -> Self {
        Self {
            by_name,
            to_many_by_name: HashMap::new(),
        }
    }

    /// The resolved to-one relationship data for `rel`, if any — the same
    /// lookup [`eval_expr`]'s own `RelationshipPath` arm does internally
    /// against `by_name`, exposed for issue #136's forward aggregate
    /// substitution path (`staging::apply_aggregate`'s
    /// `build_forward_relationship_shape`/`forward_row_contribution`), which
    /// needs to resolve a relationship's current value from the same
    /// settled-projection-backed context this module's own pure evaluator
    /// already consumes, without duplicating `by_name`'s storage.
    pub(crate) fn to_one(&self, rel: &str) -> Option<&ToOneRelationship> {
        self.by_name.get(rel)
    }

    /// Adds the to-many relationship data (issue #29), for a context that has
    /// aggregate-wrapped relationship paths to resolve. Chains onto [`new`].
    #[must_use]
    pub fn with_to_many(mut self, to_many_by_name: HashMap<String, ToManyRelationship>) -> Self {
        self.to_many_by_name = to_many_by_name;
        self
    }
}

/// A calculated value the evaluator produces, per [`ValueType`]. `Uuid`
/// (issue #79) carries its Postgres text rendering verbatim, the same way
/// `Text` does — there's no arithmetic to normalize it against, just
/// passthrough and equality comparison.
///
/// [`Value::Other`] (issue #108) is [`ValueType::Other`]'s value-level twin:
/// a [`PgType`]-tagged column's CDC-decoded text, carried verbatim exactly
/// like `Uuid`/`Text` — this is the "typed CDC round-trip" the issue asks
/// for, a value now remembers its real Postgres type family all the way
/// through [`parse_value`] instead of arriving pre-flattened to `Text`.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Numeric(Numeric),
    Text(String),
    Boolean(bool),
    Uuid(String),
    Other(PgType, String),
}

impl Value {
    pub fn value_type(&self) -> ValueType {
        match self {
            Value::Numeric(_) => ValueType::Numeric,
            Value::Text(_) => ValueType::Text,
            Value::Boolean(_) => ValueType::Boolean,
            Value::Uuid(_) => ValueType::Uuid,
            Value::Other(pg_type, _) => ValueType::Other(*pg_type),
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Numeric(n) => write!(f, "{n}"),
            Value::Text(s) => write!(f, "{s}"),
            Value::Boolean(b) => write!(f, "{b}"),
            Value::Uuid(u) => write!(f, "{u}"),
            Value::Other(_, text) => write!(f, "{text}"),
        }
    }
}

/// Why evaluation failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EvalError {
    /// A column an expression references is absent from the row image
    /// entirely (as opposed to present with a `NULL`/`None` value).
    MissingColumn { field: String, column: String },
    /// A column or literal's text value isn't a valid decimal number.
    InvalidNumber {
        field: String,
        text: String,
        source: NumericParseError,
    },
    /// A `Boolean`-typed column's text value isn't a recognized boolean
    /// spelling (Postgres's own `t`/`f` text encoding, or `true`/`false`).
    InvalidBoolean { field: String, text: String },
    /// A calculated field was re-entered while still being resolved on the
    /// current recursion path. The validator (#23) is supposed to reject
    /// cyclic definitions before they reach here, but `evaluate` is `pub`
    /// and gets exercised standalone in tests and by future callers (#25),
    /// so this is defense-in-depth against a stack overflow.
    Cycle(String),
    /// A field's expression contains a `<rel>.<column>` relationship-path
    /// reference (issue #25's grammar). The validator (#23) rejects this
    /// outright via `ValidationError::UnsupportedRelationshipPath`, so this
    /// arm is defense-in-depth for the same reason as [`EvalError::Cycle`]:
    /// `evaluate`/`evaluate_aggregate` are `pub` and can be called directly,
    /// bypassing `validate`. Resolving and evaluating a relationship path is
    /// a separate, later issue.
    UnsupportedRelationshipPath {
        field: String,
        rel: String,
        column: String,
    },
    /// A field references a relationship name (the `<rel>` head of a path)
    /// that the caller supplied no data for. The validator is supposed to
    /// reject an unknown relationship before eval, so this is defense-in-depth
    /// for the same reason as [`EvalError::Cycle`] — and it's also what the
    /// pure [`evaluate`] entry (empty [`RelationshipContext`]) returns for any
    /// relationship path.
    UnknownRelationship { field: String, rel: String },
    /// A field references a to-*many* relationship as a bare `<rel>.<column>`
    /// path in a non-aggregate (row) context. A to-many relationship's
    /// enrichment must be aggregate-wrapped (issue #29, ADR-0006); a bare path
    /// has no single row to read.
    AggregateRequiredForToMany {
        field: String,
        rel: String,
        column: String,
    },
}

impl EvalError {
    /// This error's stable, coarse [`ErrorCode`] category (`docs/decisions/0008-public-api-design.md`,
    /// decision 3). Every variant here is defense-in-depth for an invariant
    /// [`super::validate::validate`] is supposed to have already enforced
    /// before evaluation runs (see this type's own doc comment) — reaching
    /// any of them means something upstream didn't hold, not that the
    /// caller supplied bad input, so all of them report [`ErrorCode::Internal`].
    pub fn code(&self) -> ErrorCode {
        ErrorCode::Internal
    }

    /// The calculated-field name this failure is attributed to — every
    /// variant carries one (see each variant's `field`). ADR-0003's amendment
    /// (column-level quarantine) uses this to attribute a failure to a
    /// `(transform, column)` pair: `staging::quarantine`'s isolate-before-
    /// blaming probe knows which [`TransformDef`] it just evaluated, and
    /// pairs that with this field name rather than needing this error type
    /// (or [`evaluate`]/[`evaluate_with_relationships`]'s signature) to carry
    /// transform identity itself.
    pub fn field(&self) -> &str {
        match self {
            EvalError::MissingColumn { field, .. }
            | EvalError::InvalidNumber { field, .. }
            | EvalError::InvalidBoolean { field, .. }
            | EvalError::UnsupportedRelationshipPath { field, .. }
            | EvalError::UnknownRelationship { field, .. }
            | EvalError::AggregateRequiredForToMany { field, .. } => field,
            EvalError::Cycle(field) => field,
        }
    }
}

impl fmt::Display for EvalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EvalError::MissingColumn { field, column } => write!(
                f,
                "calculated field '{field}' references column '{column}', which is absent \
                 from the row image"
            ),
            EvalError::InvalidNumber {
                field,
                text,
                source,
            } => write!(
                f,
                "calculated field '{field}' could not parse '{text}' as a number: {source}"
            ),
            EvalError::InvalidBoolean { field, text } => write!(
                f,
                "calculated field '{field}' could not parse '{text}' as a boolean"
            ),
            EvalError::Cycle(field) => write!(
                f,
                "calculated field '{field}' is part of a cyclic reference"
            ),
            EvalError::UnsupportedRelationshipPath { field, rel, column } => write!(
                f,
                "calculated field '{field}' references relationship path '{rel}.{column}', \
                 which is not yet supported (grammar-only per issue #25)"
            ),
            EvalError::UnknownRelationship { field, rel } => write!(
                f,
                "calculated field '{field}' references relationship '{rel}', which is not \
                 available to the evaluator"
            ),
            EvalError::AggregateRequiredForToMany { field, rel, column } => write!(
                f,
                "calculated field '{field}' references to-many relationship path \
                 '{rel}.{column}' without an aggregate; a to-many relationship must be \
                 aggregate-wrapped"
            ),
        }
    }
}

impl std::error::Error for EvalError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            EvalError::InvalidNumber { source, .. } => Some(source),
            EvalError::MissingColumn { .. }
            | EvalError::InvalidBoolean { .. }
            | EvalError::Cycle(_)
            | EvalError::UnsupportedRelationshipPath { .. }
            | EvalError::UnknownRelationship { .. }
            | EvalError::AggregateRequiredForToMany { .. } => None,
        }
    }
}

/// Evaluates every calculated field in `def` against `row`, returning the
/// target row's calculated (non-key) columns. `row` must already resolve
/// every source column the definition's fields reference — that's the
/// validator's (#23) job against the source schema; a mismatch here (a
/// column genuinely missing from the image) is an [`EvalError`], not a
/// panic, since a staged image could in principle disagree with the
/// definition it's evaluated against.
///
/// `source_columns` gives each source column's [`ValueType`], so a column's
/// text value is parsed correctly (a `Text` column's text is passed through
/// verbatim; a `Boolean` column's is parsed as a boolean; a `Numeric`
/// column's as a decimal). A column absent from `source_columns` defaults to
/// `Numeric`, preserving this evaluator's pre-#63 behavior for callers that
/// don't yet declare column types.
pub fn evaluate(
    def: &TransformDef,
    row: &Row,
    source_columns: &HashMap<String, ValueType>,
    regex_cache: &mut RegexCache,
) -> Result<HashMap<String, Option<Value>>, EvalError> {
    evaluate_with_relationships(
        def,
        row,
        source_columns,
        &RelationshipContext::default(),
        regex_cache,
    )
}

/// [`evaluate`], skipping every field named in `excluded` — ADR-0003's
/// amendment (column-level quarantine): a paused column must not be
/// re-evaluated (it would just reproduce the same failure that paused it
/// forever), and its target value freezes at whatever it last held rather
/// than being overwritten. See [`evaluate_with_relationships_excluding`] for
/// the full contract; this is its no-relationships convenience form, mirroring
/// [`evaluate`]'s own relationship to [`evaluate_with_relationships`].
pub fn evaluate_excluding(
    def: &TransformDef,
    row: &Row,
    source_columns: &HashMap<String, ValueType>,
    regex_cache: &mut RegexCache,
    excluded: &HashSet<String>,
) -> Result<HashMap<String, Option<Value>>, EvalError> {
    evaluate_with_relationships_excluding(
        def,
        row,
        source_columns,
        &RelationshipContext::default(),
        regex_cache,
        excluded,
    )
}

/// Like [`evaluate`], but with [`RelationshipContext`] read-side data so a
/// field's `<rel>.<column>` to-one relationship path (issue #28) resolves the
/// single related row and reads the referenced column. A path whose `rel` has
/// no entry in `relationships` errors ([`EvalError::UnknownRelationship`]);
/// with the empty context [`evaluate`] passes, any path errors, matching the
/// pure evaluator's pre-#28 behavior.
pub fn evaluate_with_relationships(
    def: &TransformDef,
    row: &Row,
    source_columns: &HashMap<String, ValueType>,
    relationships: &RelationshipContext,
    regex_cache: &mut RegexCache,
) -> Result<HashMap<String, Option<Value>>, EvalError> {
    evaluate_with_relationships_excluding(
        def,
        row,
        source_columns,
        relationships,
        regex_cache,
        &HashSet::new(),
    )
}

/// [`evaluate_with_relationships`], skipping every field named in `excluded`.
///
/// A field in `excluded` is never handed to [`eval_field`] at all — its cache
/// slot is pre-seeded `None` instead, so any *other* field that references it
/// (a same-table calculated-field alias) sees a plain absent value rather than
/// re-triggering the excluded field's own broken expression, and the excluded
/// field itself is omitted from the returned map entirely (not present as
/// `None`) so [`super::super::staging::apply::compute`]'s write plan can tell
/// "paused, don't touch this column" apart from "genuinely evaluated to
/// NULL." `excluded` is normally empty (every existing caller stays on this
/// behavior via [`evaluate_with_relationships`]/[`evaluate`]); it's non-empty
/// only when the caller has already looked up which of this definition's
/// columns are currently paused (ADR-0003's amendment).
pub fn evaluate_with_relationships_excluding(
    def: &TransformDef,
    row: &Row,
    source_columns: &HashMap<String, ValueType>,
    relationships: &RelationshipContext,
    regex_cache: &mut RegexCache,
    excluded: &HashSet<String>,
) -> Result<HashMap<String, Option<Value>>, EvalError> {
    let fields_by_name: HashMap<&str, &FieldDef> =
        def.fields.iter().map(|f| (f.name.as_str(), f)).collect();

    let mut cache: HashMap<String, Option<Value>> = HashMap::with_capacity(def.fields.len());
    let mut in_progress: HashSet<String> = HashSet::new();
    for excluded_name in excluded {
        cache.insert(excluded_name.clone(), None);
    }
    for field in &def.fields {
        if excluded.contains(&field.name) {
            continue;
        }
        if !cache.contains_key(&field.name) {
            let value = eval_field(
                field,
                row,
                source_columns,
                relationships,
                &fields_by_name,
                &mut cache,
                &mut in_progress,
                regex_cache,
            )?;
            cache.insert(field.name.clone(), value);
        }
    }

    for excluded_name in excluded {
        cache.remove(excluded_name);
    }
    Ok(cache)
}

/// Every `<rel>.<column>` relationship reference in `def`'s field expressions
/// *and* (issue #137) its `GROUP BY` keys, as `(relationship_name, column)`
/// pairs — bare to-one paths ([`Expr::RelationshipPath`]), aggregate-wrapped
/// to-many paths (`SUM(<rel>.<column>)`, which parse to a
/// [`Expr::FunctionCall`] whose sole argument is a path), and a to-one
/// relationship path used as a [`super::ast::KeySpace::Aggregate`] `GROUP BY`
/// key. The staging reverse-recompute path (issue #30) uses this to learn
/// which relationships a target reads — and which of their to-side columns —
/// so it can build a [`RelationshipContext`] for the from-side recompute
/// without re-walking the AST itself; [`super::catalog::resolve_relationships`]
/// uses it the same way to resolve every relationship the validator needs to
/// check, regardless of whether a definition reads it via a field or a
/// `GROUP BY` key. Pairs may repeat if the same reference appears more than
/// once; the caller dedups.
pub fn relationship_references(def: &TransformDef) -> Vec<(String, String)> {
    let mut refs = Vec::new();
    for field in &def.fields {
        collect_relationship_refs(&field.expr, &mut refs);
    }
    if let KeySpace::Aggregate { group_by } = &def.key_space {
        for key in group_by {
            if let GroupByKey::RelationshipPath { rel, column } = key {
                refs.push((rel.clone(), column.clone()));
            }
        }
    }
    refs
}

fn collect_relationship_refs(expr: &Expr, out: &mut Vec<(String, String)>) {
    match expr {
        Expr::RelationshipPath { rel, column } => out.push((rel.clone(), column.clone())),
        Expr::BinaryOp { lhs, rhs, .. } => {
            collect_relationship_refs(lhs, out);
            collect_relationship_refs(rhs, out);
        }
        Expr::FunctionCall { args, .. } => {
            for arg in args {
                collect_relationship_refs(arg, out);
            }
        }
        Expr::Column(_) | Expr::NumberLiteral(_) | Expr::StringLiteral(_) => {}
    }
}

/// Evaluates one field, memoizing into `cache` (also used to resolve
/// forward/backward references to other calculated fields on the same
/// target). [`super::validate`] is supposed to guarantee the reference
/// graph is acyclic before this runs, but `in_progress` — the set of field
/// names currently being resolved on this recursion path — is a
/// defense-in-depth guard against a cyclic definition that reaches here
/// anyway (`evaluate` is `pub` and used standalone in tests/#25).
#[allow(clippy::too_many_arguments)]
fn eval_field(
    field: &FieldDef,
    row: &Row,
    source_columns: &HashMap<String, ValueType>,
    relationships: &RelationshipContext,
    fields_by_name: &HashMap<&str, &FieldDef>,
    cache: &mut HashMap<String, Option<Value>>,
    in_progress: &mut HashSet<String>,
    regex_cache: &mut RegexCache,
) -> Result<Option<Value>, EvalError> {
    if let Some(cached) = cache.get(&field.name) {
        return Ok(cached.clone());
    }
    if !in_progress.insert(field.name.clone()) {
        return Err(EvalError::Cycle(field.name.clone()));
    }
    let value = eval_expr(
        &field.expr,
        &field.name,
        row,
        source_columns,
        relationships,
        fields_by_name,
        cache,
        in_progress,
        regex_cache,
    );
    in_progress.remove(&field.name);
    let value = value?;
    cache.insert(field.name.clone(), value.clone());
    Ok(value)
}

// This recursive evaluator's params are each a distinct piece of per-call
// state (the expression being reduced, the calc-field memoization cache and
// its cycle guard, and now the regex cache added by #68) rather than a
// natural grouping — bundling them into a context struct wouldn't reduce
// call-site complexity, just move it, so allow the count over grouping.
#[allow(clippy::too_many_arguments)]
fn eval_expr(
    expr: &Expr,
    field_name: &str,
    row: &Row,
    source_columns: &HashMap<String, ValueType>,
    relationships: &RelationshipContext,
    fields_by_name: &HashMap<&str, &FieldDef>,
    cache: &mut HashMap<String, Option<Value>>,
    in_progress: &mut HashSet<String>,
    regex_cache: &mut RegexCache,
) -> Result<Option<Value>, EvalError> {
    match expr {
        Expr::Column(name) => {
            // A field referencing a source column of its own name (e.g.
            // `SELECT c AS c`) is a passthrough, not a self-reference —
            // mirrors the `is_self_passthrough` exemption in
            // `validate.rs`'s cycle detection. Without this, resolving via
            // `fields_by_name` below would recurse into this very field via
            // `eval_field`, tripping a spurious `EvalError::Cycle`.
            let is_self_passthrough = name == field_name && source_columns.contains_key(name);
            if !is_self_passthrough && let Some(calc_field) = fields_by_name.get(name.as_str()) {
                return eval_field(
                    calc_field,
                    row,
                    source_columns,
                    relationships,
                    fields_by_name,
                    cache,
                    in_progress,
                    regex_cache,
                );
            }
            match row.get(name) {
                Some(Some(text)) => {
                    let value_type = source_columns
                        .get(name)
                        .copied()
                        .unwrap_or(ValueType::Numeric);
                    parse_value(field_name, value_type, text).map(Some)
                }
                Some(None) => Ok(None),
                None => Err(EvalError::MissingColumn {
                    field: field_name.to_string(),
                    column: name.clone(),
                }),
            }
        }
        Expr::NumberLiteral(text) => {
            parse_number(field_name, text).map(|n| Some(Value::Numeric(n)))
        }
        Expr::StringLiteral(text) => Ok(Some(Value::Text(text.clone()))),
        Expr::RelationshipPath { rel, column } => {
            // To-one resolution (issue #28): find the relationship, read the
            // from-row's join key, look up the single to-side row by that key,
            // and read the referenced column from it. A missing match (no
            // to-side row, or a NULL join key) is NULL enrichment — LEFT JOIN
            // semantics — leaving the from-row itself intact.
            let Some(reldata) = relationships.by_name.get(rel) else {
                return Err(EvalError::UnknownRelationship {
                    field: field_name.to_string(),
                    rel: rel.clone(),
                });
            };
            if reldata.cardinality == RelationshipCardinality::ToMany {
                // A bare to-many path has no single row to read; issue #29
                // handles the aggregate-wrapped form.
                return Err(EvalError::AggregateRequiredForToMany {
                    field: field_name.to_string(),
                    rel: rel.clone(),
                    column: column.clone(),
                });
            }
            // The join key comes from the from-row's `from_col`. A NULL key
            // never joins (SQL `NULL != NULL`), so it's a no-match → NULL.
            let key = match row.get(&reldata.from_col) {
                Some(Some(text)) => text,
                Some(None) => return Ok(None),
                None => {
                    return Err(EvalError::MissingColumn {
                        field: field_name.to_string(),
                        column: reldata.from_col.clone(),
                    });
                }
            };
            let Some(to_row) = reldata.to_rows_by_key.get(key) else {
                return Ok(None);
            };
            // Read the enrichment column off the matched to-side row, parsing
            // its text with the to-side column's type (defaulting to Numeric,
            // matching the `Expr::Column` arm above).
            match to_row.get(column) {
                Some(Some(text)) => {
                    let value_type = reldata
                        .to_columns
                        .get(column)
                        .copied()
                        .unwrap_or(ValueType::Numeric);
                    parse_value(field_name, value_type, text).map(Some)
                }
                Some(None) => Ok(None),
                None => Err(EvalError::MissingColumn {
                    field: field_name.to_string(),
                    column: column.clone(),
                }),
            }
        }
        Expr::BinaryOp { op, lhs, rhs } => {
            let lhs = eval_expr(
                lhs,
                field_name,
                row,
                source_columns,
                relationships,
                fields_by_name,
                cache,
                in_progress,
                regex_cache,
            )?;
            let rhs = eval_expr(
                rhs,
                field_name,
                row,
                source_columns,
                relationships,
                fields_by_name,
                cache,
                in_progress,
                regex_cache,
            )?;
            Ok(apply_operator(*op, lhs, rhs))
        }
        // A to-many relationship enrichment (issue #29): an aggregate function
        // whose sole argument is a `<rel>.<column>` path. Unlike a GROUP BY
        // aggregate — which folds over a group of the target's own source rows
        // (`eval_aggregate_expr`) — this folds over the *related* to-side rows
        // sharing this from-row's join value, keyed by that value rather than a
        // grouping tuple (ADR-0006). The field itself lives on the referencing
        // (row-grain) target, so it is evaluated here in the row path, not the
        // aggregate path.
        Expr::FunctionCall { name, args }
            if super::registry::lookup_aggregate_function(name).is_some()
                && matches!(args.as_slice(), [Expr::RelationshipPath { .. }]) =>
        {
            let Expr::RelationshipPath { rel, column } = &args[0] else {
                unreachable!("guarded by the matches! above");
            };
            eval_to_many_aggregate(name, rel, column, field_name, row, relationships)
        }
        Expr::FunctionCall { name, args } if name == "COALESCE" => {
            for arg in args {
                let val = eval_expr(
                    arg,
                    field_name,
                    row,
                    source_columns,
                    relationships,
                    fields_by_name,
                    cache,
                    in_progress,
                    regex_cache,
                )?;
                if val.is_some() {
                    return Ok(val); // Short-circuit: first non-null wins
                }
            }
            Ok(None)
        }
        Expr::FunctionCall { name, args } => {
            let mut arg_values = Vec::with_capacity(args.len());
            for arg in args {
                arg_values.push(eval_expr(
                    arg,
                    field_name,
                    row,
                    source_columns,
                    relationships,
                    fields_by_name,
                    cache,
                    in_progress,
                    regex_cache,
                )?);
            }
            Ok(apply_function(name, arg_values, regex_cache))
        }
    }
}

/// Folds an aggregate (`SUM`/`MIN`/`MAX`/`AVG`/`COUNT`) over the to-side rows
/// of a to-many relationship for one from-row (issue #29). The from-row's
/// `from_col` value is the join key; the related rows are the set stored under
/// that key, and `column` is read off each related row (typed by the
/// relationship's `to_columns`, defaulting to `Numeric` like the [`Row`]-column
/// handling in [`eval_expr`]).
///
/// Empty-set semantics match Postgres's correlated aggregate over zero rows:
/// `COUNT` → `0`, `SUM`/`MIN`/`MAX`/`AVG` → `NULL`. An empty set arises from a
/// `NULL` join key (SQL `NULL` never joins), a key with no related rows, or a
/// relationship with no entry under this key. `COUNT(<rel>.<column>)` counts
/// related rows whose `column` is non-`NULL` (Postgres `COUNT(<col>)`); the
/// four numeric folds skip `NULL` rows and yield `NULL` if none remain, reusing
/// [`reduce_numeric_aggregate`] — the same reducer as [`fold_aggregate`].
fn eval_to_many_aggregate(
    name: &str,
    rel: &str,
    column: &str,
    field_name: &str,
    from_row: &Row,
    relationships: &RelationshipContext,
) -> Result<Option<Value>, EvalError> {
    // The relationship must be a known to-many. A name that isn't (unknown, or
    // a to-one used with an aggregate wrapper — the validator's job to reject)
    // errors as unknown, defense-in-depth like the to-one path arm.
    let Some(reldata) = relationships.to_many_by_name.get(rel) else {
        return Err(EvalError::UnknownRelationship {
            field: field_name.to_string(),
            rel: rel.to_string(),
        });
    };
    // The join key comes from the from-row's `from_col`. A NULL key never joins
    // (SQL `NULL != NULL`), and a key with no stored rows both mean the empty
    // set — the aggregate's empty result below.
    let related: &[Row] = match from_row.get(&reldata.from_col) {
        Some(Some(key)) => reldata
            .to_rows_by_key
            .get(key)
            .map_or(&[][..], Vec::as_slice),
        Some(None) => &[],
        None => {
            return Err(EvalError::MissingColumn {
                field: field_name.to_string(),
                column: reldata.from_col.clone(),
            });
        }
    };

    if name == "COUNT" {
        // `COUNT(<rel>.<column>)`: related rows whose `column` is non-NULL,
        // matching Postgres `COUNT(<col>)`. The empty set counts to 0.
        let mut count: usize = 0;
        for row in related {
            match row.get(column) {
                Some(Some(_)) => count += 1,
                Some(None) => {}
                None => {
                    return Err(EvalError::MissingColumn {
                        field: field_name.to_string(),
                        column: column.to_string(),
                    });
                }
            }
        }
        return Ok(Some(Value::Numeric(int_numeric(count))));
    }

    // SUM/MIN/MAX/AVG: read `column` off each related row, skipping NULLs (and,
    // defense-in-depth, any non-Numeric value a hand-built AST slipped past the
    // validator's Numeric-only check), then fold. All-NULL / empty => NULL.
    let value_type = reldata
        .to_columns
        .get(column)
        .copied()
        .unwrap_or(ValueType::Numeric);
    let mut values: Vec<Numeric> = Vec::new();
    for row in related {
        match row.get(column) {
            Some(Some(text)) => {
                if let Value::Numeric(n) = parse_value(field_name, value_type, text)? {
                    values.push(n);
                }
            }
            Some(None) => {}
            None => {
                return Err(EvalError::MissingColumn {
                    field: field_name.to_string(),
                    column: column.to_string(),
                });
            }
        }
    }
    Ok(reduce_numeric_aggregate(name, values))
}

/// Evaluates every calculated field of an [`KeySpace::Aggregate`] definition
/// against one group's full row set — the aggregate counterpart to
/// [`evaluate`]. `SUM`/`MIN`/`MAX`/`AVG` need every row in the group up
/// front rather than one row image at a time, so there is no incremental
/// per-row form of this (that's issue #11's job, blocked on this one).
///
/// A bare grouping-key column reference is read off any one row (every row
/// in the group shares that value by definition); everything else the
/// validator (#23) allows here is either another calculated field or an
/// aggregate call, both handled by recursing through [`eval_aggregate_expr`].
///
/// # Panics
///
/// If `def.key_space` is not [`KeySpace::Aggregate`], or if `rows` is empty
/// (a group has at least one row by construction).
pub fn evaluate_aggregate(
    def: &TransformDef,
    rows: &[Row],
    source_columns: &HashMap<String, ValueType>,
    regex_cache: &mut RegexCache,
) -> Result<HashMap<String, Option<Value>>, EvalError> {
    let KeySpace::Aggregate { group_by } = &def.key_space else {
        panic!("evaluate_aggregate called on a non-aggregate definition");
    };
    assert!(!rows.is_empty(), "a group must have at least one row");
    // A `GroupByKey::RelationshipPath` group key is never representable here
    // (this pure evaluator carries no relationship data — see this
    // function's module doc comment on `fold_aggregate`'s own
    // relationship-free `RelationshipContext::default()`); a caller with one
    // must first substitute it (and any field referencing it bare) for a
    // synthetic `Column`, mirroring `staging::apply_aggregate`'s forward
    // relationship shape for fields. Keying this set by each entry's target
    // column name keeps a plain-column `GROUP BY` (the overwhelmingly common
    // case) byte-identical to before issue #137.
    let group_by: HashSet<&str> = group_by.iter().map(|k| k.target_column_name()).collect();

    let fields_by_name: HashMap<&str, &FieldDef> =
        def.fields.iter().map(|f| (f.name.as_str(), f)).collect();

    let mut cache: HashMap<String, Option<Value>> = HashMap::with_capacity(def.fields.len());
    let mut in_progress: HashSet<String> = HashSet::new();
    for field in &def.fields {
        if !cache.contains_key(&field.name) {
            let value = eval_aggregate_field(
                field,
                rows,
                &group_by,
                source_columns,
                &fields_by_name,
                &mut cache,
                &mut in_progress,
                regex_cache,
            )?;
            cache.insert(field.name.clone(), value);
        }
    }

    Ok(cache)
}

#[allow(clippy::too_many_arguments)]
fn eval_aggregate_field(
    field: &FieldDef,
    rows: &[Row],
    group_by: &HashSet<&str>,
    source_columns: &HashMap<String, ValueType>,
    fields_by_name: &HashMap<&str, &FieldDef>,
    cache: &mut HashMap<String, Option<Value>>,
    in_progress: &mut HashSet<String>,
    regex_cache: &mut RegexCache,
) -> Result<Option<Value>, EvalError> {
    if let Some(cached) = cache.get(&field.name) {
        return Ok(cached.clone());
    }
    if !in_progress.insert(field.name.clone()) {
        return Err(EvalError::Cycle(field.name.clone()));
    }
    let value = eval_aggregate_expr(
        &field.expr,
        &field.name,
        rows,
        group_by,
        source_columns,
        fields_by_name,
        cache,
        in_progress,
        regex_cache,
    );
    in_progress.remove(&field.name);
    let value = value?;
    cache.insert(field.name.clone(), value.clone());
    Ok(value)
}

#[allow(clippy::too_many_arguments)]
fn eval_aggregate_expr(
    expr: &Expr,
    field_name: &str,
    rows: &[Row],
    group_by: &HashSet<&str>,
    source_columns: &HashMap<String, ValueType>,
    fields_by_name: &HashMap<&str, &FieldDef>,
    cache: &mut HashMap<String, Option<Value>>,
    in_progress: &mut HashSet<String>,
    regex_cache: &mut RegexCache,
) -> Result<Option<Value>, EvalError> {
    match expr {
        Expr::Column(name) if group_by.contains(name.as_str()) => {
            // Every row in the group shares this value, so any row's is
            // representative.
            eval_row_scalar(name, field_name, &rows[0], source_columns)
        }
        Expr::Column(name) => {
            // The validator guarantees a bare column reference reaching here
            // is either a grouping key (handled above) or another
            // calculated field on this same target — a bare source column
            // would have had to be wrapped in an aggregate call instead.
            let calc_field =
                fields_by_name
                    .get(name.as_str())
                    .ok_or_else(|| EvalError::MissingColumn {
                        field: field_name.to_string(),
                        column: name.clone(),
                    })?;
            eval_aggregate_field(
                calc_field,
                rows,
                group_by,
                source_columns,
                fields_by_name,
                cache,
                in_progress,
                regex_cache,
            )
        }
        Expr::NumberLiteral(text) => {
            parse_number(field_name, text).map(|n| Some(Value::Numeric(n)))
        }
        Expr::StringLiteral(text) => Ok(Some(Value::Text(text.clone()))),
        Expr::RelationshipPath { rel, column } => Err(EvalError::UnsupportedRelationshipPath {
            field: field_name.to_string(),
            rel: rel.clone(),
            column: column.clone(),
        }),
        Expr::BinaryOp { op, lhs, rhs } => {
            let lhs = eval_aggregate_expr(
                lhs,
                field_name,
                rows,
                group_by,
                source_columns,
                fields_by_name,
                cache,
                in_progress,
                regex_cache,
            )?;
            let rhs = eval_aggregate_expr(
                rhs,
                field_name,
                rows,
                group_by,
                source_columns,
                fields_by_name,
                cache,
                in_progress,
                regex_cache,
            )?;
            Ok(apply_operator(*op, lhs, rhs))
        }
        Expr::FunctionCall { name, args } if name == "COUNT" && args.is_empty() => {
            // `COUNT(*)` (issue #75): counts every row in the group,
            // unconditionally — unlike `SUM`/`MIN`/`MAX`/`AVG`'s
            // `fold_aggregate`, there is no per-row argument to evaluate or
            // skip-if-NULL, so `rows.len()` is the whole computation. This
            // also covers `row_contribution`'s single-row-slice call in
            // `staging::apply_aggregate` (issue #11's delta model): a lone
            // row's "contribution" to a group's count is always exactly 1.
            Ok(Some(Value::Numeric(int_numeric(rows.len()))))
        }
        Expr::FunctionCall { name, args }
            if super::registry::lookup_aggregate_function(name).is_some() =>
        {
            fold_aggregate(
                name,
                &args[0],
                field_name,
                rows,
                source_columns,
                fields_by_name,
                regex_cache,
            )
        }
        Expr::FunctionCall { name, args } if name == "COALESCE" => {
            for arg in args {
                let val = eval_aggregate_expr(
                    arg,
                    field_name,
                    rows,
                    group_by,
                    source_columns,
                    fields_by_name,
                    cache,
                    in_progress,
                    regex_cache,
                )?;
                if val.is_some() {
                    return Ok(val);
                }
            }
            Ok(None)
        }
        Expr::FunctionCall { name, args } => {
            let mut arg_values = Vec::with_capacity(args.len());
            for arg in args {
                arg_values.push(eval_aggregate_expr(
                    arg,
                    field_name,
                    rows,
                    group_by,
                    source_columns,
                    fields_by_name,
                    cache,
                    in_progress,
                    regex_cache,
                )?);
            }
            Ok(apply_function(name, arg_values, regex_cache))
        }
    }
}

/// Reads a bare column reference off a single row — shared by the
/// grouping-key case in [`eval_aggregate_expr`] (any one row is
/// representative of the whole group) and by [`fold_aggregate`]'s per-row
/// argument evaluation.
fn eval_row_scalar(
    name: &str,
    field_name: &str,
    row: &Row,
    source_columns: &HashMap<String, ValueType>,
) -> Result<Option<Value>, EvalError> {
    match row.get(name) {
        Some(Some(text)) => {
            let value_type = source_columns
                .get(name)
                .copied()
                .unwrap_or(ValueType::Numeric);
            parse_value(field_name, value_type, text).map(Some)
        }
        Some(None) => Ok(None),
        None => Err(EvalError::MissingColumn {
            field: field_name.to_string(),
            column: name.to_string(),
        }),
    }
}

/// Folds `SUM`/`MIN`/`MAX`/`AVG` over `arg_expr` evaluated against every row
/// in the group, matching Postgres's NULL handling for these aggregates: a
/// `NULL` row is skipped entirely (not treated as zero), and if every row's
/// value is `NULL` the result is `NULL` too (Postgres's "aggregate of zero
/// non-NULL values is NULL" rule — `sum('{}'::numeric[])`, for instance).
///
/// `arg_expr` is evaluated per row via the ordinary single-row [`eval_expr`],
/// one row at a time so each row gets its own fresh memoization scope.
/// `fields_by_name` is threaded through so `arg_expr` can reference another
/// (non-aggregate) calculated field on this same target — e.g.
/// `GROUP BY id SELECT (id + 1) AS adj, SUM(adj) AS t` — the same way
/// Postgres itself resolves it; the validator doesn't restrict an aggregate
/// argument to source-only columns, so the evaluator must not either.
fn fold_aggregate(
    name: &str,
    arg_expr: &Expr,
    field_name: &str,
    rows: &[Row],
    source_columns: &HashMap<String, ValueType>,
    fields_by_name: &HashMap<&str, &FieldDef>,
    regex_cache: &mut RegexCache,
) -> Result<Option<Value>, EvalError> {
    let mut values: Vec<Numeric> = Vec::new();
    // The aggregate path does not wire relationships (issue #29 handles a
    // to-many relationship's aggregate-wrapped enrichment); a bare path in an
    // aggregate argument therefore errors as unknown, defense-in-depth.
    let relationships = RelationshipContext::default();
    for row in rows {
        let mut per_row_cache = HashMap::new();
        let mut per_row_in_progress = HashSet::new();
        let value = eval_expr(
            arg_expr,
            field_name,
            row,
            source_columns,
            &relationships,
            fields_by_name,
            &mut per_row_cache,
            &mut per_row_in_progress,
            regex_cache,
        )?;
        if let Some(Value::Numeric(n)) = value {
            values.push(n);
        }
        // A `NULL` (`None`) or non-Numeric evaluation is skipped: `NULL` per
        // Postgres's aggregate semantics above, and a non-Numeric value here
        // would mean a hand-built AST bypassed the validator's Numeric-only
        // check on these four functions' argument (defense-in-depth, as
        // elsewhere in this module).
    }

    Ok(reduce_numeric_aggregate(name, values))
}

/// Reduces the collected non-`NULL` numeric values of `SUM`/`MIN`/`MAX`/`AVG`
/// to the aggregate's result, or `None` (SQL `NULL`) when `values` is empty —
/// Postgres's "aggregate of zero non-NULL values is NULL" rule. Shared by
/// [`fold_aggregate`] (a GROUP BY group's rows) and [`eval_to_many_aggregate`]
/// (a to-many relationship's related rows) so both fold identically.
fn reduce_numeric_aggregate(name: &str, values: Vec<Numeric>) -> Option<Value> {
    if values.is_empty() {
        return None;
    }

    let result = match name {
        "SUM" => values
            .into_iter()
            .reduce(|a, b| a.add(&b))
            .expect("checked non-empty above"),
        "MIN" => values
            .into_iter()
            .reduce(|a, b| {
                if b.compare(&a) == std::cmp::Ordering::Less {
                    b
                } else {
                    a
                }
            })
            .expect("checked non-empty above"),
        "MAX" => values
            .into_iter()
            .reduce(|a, b| {
                if b.compare(&a) == std::cmp::Ordering::Greater {
                    b
                } else {
                    a
                }
            })
            .expect("checked non-empty above"),
        "AVG" => {
            let count = values.len();
            let sum = values
                .into_iter()
                .reduce(|a, b| a.add(&b))
                .expect("checked non-empty above");
            let count_numeric = Numeric::parse(&count.to_string())
                .expect("a usize always renders as a valid decimal literal");
            sum.div(&count_numeric)
        }
        _ => unreachable!("reduce_numeric_aggregate is only called for SUM/MIN/MAX/AVG"),
    };
    Some(Value::Numeric(result))
}

/// `+` and `>` are both Numeric-operand operators (per ADR-0004, neither
/// gains an implicit string form); a non-Numeric operand here would mean the
/// validator (#23) let a type-mismatched definition through, so this falls
/// back to `None` rather than a panic, matching this module's existing
/// defense-in-depth posture toward validator bugs. Postgres's own `>` on
/// `numeric` is `STRICT` (any `NULL` operand yields `NULL`), matched by the
/// same `None` fallback.
fn apply_operator(op: Operator, lhs: Option<Value>, rhs: Option<Value>) -> Option<Value> {
    match op {
        Operator::Add => match (lhs, rhs) {
            (Some(Value::Numeric(a)), Some(Value::Numeric(b))) => Some(Value::Numeric(a.add(&b))),
            _ => None,
        },
        Operator::GreaterThan => match (lhs, rhs) {
            (Some(Value::Numeric(a)), Some(Value::Numeric(b))) => {
                Some(Value::Boolean(a.compare(&b) == std::cmp::Ordering::Greater))
            }
            _ => None,
        },
    }
}

/// Applies a registered function to its already-evaluated arguments (issue
/// #64). Postgres's built-in functions are `STRICT` (return `NULL` given any
/// `NULL` argument) — matched here by short-circuiting to `None` before
/// dispatching on `name` — so no per-function null handling is needed below.
///
/// An `args` type mismatch or an unregistered `name` here would mean the
/// validator (#23) let a bad definition through; as in [`apply_operator`],
/// this falls back to `None` rather than panicking, matching this module's
/// defense-in-depth posture toward validator bugs.
fn apply_function(
    name: &str,
    args: Vec<Option<Value>>,
    regex_cache: &mut RegexCache,
) -> Option<Value> {
    let mut texts = Vec::with_capacity(args.len());
    for arg in args {
        match arg {
            Some(Value::Text(text)) => texts.push(text),
            _ => return None,
        }
    }

    let result = match (name, texts.as_slice()) {
        ("OCTET_LENGTH", [text]) => int_numeric(text.len()),
        ("CHAR_LENGTH", [text]) => int_numeric(text.chars().count()),
        ("STRPOS", [haystack, needle]) => int_numeric(strpos(haystack, needle)),
        ("REGEXP_COUNT", [text, pattern]) => int_numeric(regexp_count(text, pattern, regex_cache)?),
        _ => return None,
    };
    Some(Value::Numeric(result))
}

fn int_numeric(n: usize) -> Numeric {
    Numeric::parse(&n.to_string()).expect("a usize always renders as a valid decimal literal")
}

/// `strpos(haystack, needle)`: the 1-based *character* (not byte) position
/// of the first match, `0` if absent — matching Postgres's `strpos` exactly,
/// including its `strpos(x, '') = 1` convention (`str::find` on an empty
/// needle always matches at byte offset 0, which lands on that case for
/// free).
fn strpos(haystack: &str, needle: &str) -> usize {
    match haystack.find(needle) {
        Some(byte_idx) => haystack[..byte_idx].chars().count() + 1,
        None => 0,
    }
}
/// `regexp_count(text, pattern)`, matching Postgres's non-overlapping,
/// leftmost-first counting semantics exactly, including for a `pattern` that
/// can match the empty string (`a*`, `x?`, ...). The validator restricts
/// `pattern` to a string literal and rejects one that fails to compile
/// before this ever runs; a `None` here means a hand-built AST bypassed that
/// check (defense-in-depth, as elsewhere in this module), not a normal-path
/// failure.
///
/// Implemented as the classic global-match loop (the same one `sed`/`awk`/a
/// language's own "replace all" use) rather than `Regex::find_iter`:
/// `find_iter` deliberately suppresses a zero-width match landing exactly at
/// the end of a preceding non-empty match, whereas Postgres counts it (e.g.
/// `regexp_count('aaa', 'a*') = 2`: the `aaa` match, then an empty match at
/// the end). Advancing by one *character* (not byte) after a zero-width
/// match keeps every search restart on a valid UTF-8 boundary.
///
/// `pattern` is compiled at most once per distinct pattern text via
/// `regex_cache` (issue #68) rather than on every call — the validator (#64)
/// already guarantees `pattern` is a string literal, so the same text always
/// yields the same compiled `Regex`.
fn regexp_count(text: &str, pattern: &str, regex_cache: &mut RegexCache) -> Option<usize> {
    let re = match regex_cache.get(pattern) {
        Some(re) => re,
        None => {
            let re = Regex::new(pattern).ok()?;
            regex_cache.entry(pattern.to_string()).or_insert(re)
        }
    };
    let mut count = 0;
    let mut pos = 0;
    while pos <= text.len() {
        let Some(m) = re.find_at(text, pos) else {
            break;
        };
        count += 1;
        pos = if m.end() > pos {
            m.end()
        } else {
            match text[pos..].chars().next() {
                Some(c) => pos + c.len_utf8(),
                None => pos + 1,
            }
        };
    }
    Some(count)
}

fn parse_value(field_name: &str, value_type: ValueType, text: &str) -> Result<Value, EvalError> {
    match value_type {
        ValueType::Numeric => parse_number(field_name, text).map(Value::Numeric),
        ValueType::Text => Ok(Value::Text(text.to_string())),
        ValueType::Boolean => parse_boolean(field_name, text).map(Value::Boolean),
        ValueType::Uuid => Ok(Value::Uuid(text.to_string())),
        // Passthrough, like `Uuid`/`Text` above: the epic's per-family
        // children (#111–#122) are what teach a `PgType` how to actually
        // parse/validate its own text form; #108's job is only to carry it
        // through tagged with its real type instead of mislabeling it.
        ValueType::Other(pg_type) => Ok(Value::Other(pg_type, text.to_string())),
    }
}

fn parse_number(field_name: &str, text: &str) -> Result<Numeric, EvalError> {
    Numeric::parse(text).map_err(|source| EvalError::InvalidNumber {
        field: field_name.to_string(),
        text: text.to_string(),
        source,
    })
}

/// Accepts Postgres's own `t`/`f` text encoding of `boolean` (what a
/// `::text` cast produces) as well as the spelled-out `true`/`false`.
fn parse_boolean(field_name: &str, text: &str) -> Result<bool, EvalError> {
    match text {
        "t" | "true" | "TRUE" | "True" => Ok(true),
        "f" | "false" | "FALSE" | "False" => Ok(false),
        _ => Err(EvalError::InvalidBoolean {
            field: field_name.to_string(),
            text: text.to_string(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::defs::ast::{KeySpace, Predicate};

    fn def(fields: Vec<FieldDef>) -> TransformDef {
        TransformDef {
            target: "t".to_string(),
            source: "s".to_string(),
            key_space: KeySpace::OneToOne,
            fields,
            predicate: Predicate::True,
            explicit_source_schema: None,
            explicit_target_schema: None,
        }
    }

    fn col(name: &str) -> Expr {
        Expr::Column(name.to_string())
    }

    fn row(pairs: &[(&str, Option<&str>)]) -> Row {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.map(|s| s.to_string())))
            .collect()
    }

    fn add(lhs: Expr, rhs: Expr) -> Expr {
        Expr::BinaryOp {
            op: Operator::Add,
            lhs: Box::new(lhs),
            rhs: Box::new(rhs),
        }
    }

    fn numeric_types(names: &[&str]) -> HashMap<String, ValueType> {
        names
            .iter()
            .map(|n| (n.to_string(), ValueType::Numeric))
            .collect()
    }

    /// Test-only convenience: most tests exercise a single `evaluate` call
    /// against a fresh definition, so a fresh [`RegexCache`] each time is
    /// fine — only a caller reusing one definition across many rows (like
    /// `apply.rs`) needs to reuse the cache itself.
    fn eval(
        d: &TransformDef,
        r: &Row,
        types: &HashMap<String, ValueType>,
    ) -> Result<HashMap<String, Option<Value>>, EvalError> {
        evaluate(d, r, types, &mut RegexCache::new())
    }

    #[test]
    fn evaluates_column_plus_literal() {
        let d = def(vec![FieldDef {
            name: "total".to_string(),
            expr: add(col("price"), Expr::NumberLiteral("5".to_string())),
        }]);
        let r = row(&[("price", Some("10.50"))]);
        let result = eval(&d, &r, &numeric_types(&["price"])).unwrap();
        match result["total"].as_ref().unwrap() {
            Value::Numeric(n) => assert_eq!(n.to_string(), "15.50"),
            other => panic!("expected Numeric, got {other:?}"),
        }
    }

    #[test]
    fn same_name_passthrough_of_a_source_column_evaluates_without_cycle_error() {
        let d = def(vec![FieldDef {
            name: "c".to_string(),
            expr: col("c"),
        }]);
        let r = row(&[("c", Some("10.50"))]);
        let result = eval(&d, &r, &numeric_types(&["c"])).unwrap();
        match result["c"].as_ref().unwrap() {
            Value::Numeric(n) => assert_eq!(n.to_string(), "10.50"),
            other => panic!("expected Numeric, got {other:?}"),
        }
    }

    #[test]
    fn null_propagates_through_addition() {
        let d = def(vec![FieldDef {
            name: "total".to_string(),
            expr: add(col("price"), col("tax")),
        }]);
        let r = row(&[("price", Some("10")), ("tax", None)]);
        let result = eval(&d, &r, &numeric_types(&["price", "tax"])).unwrap();
        assert_eq!(result["total"], None);
    }

    #[test]
    fn double_null_is_null() {
        let d = def(vec![FieldDef {
            name: "total".to_string(),
            expr: add(col("a"), col("b")),
        }]);
        let r = row(&[("a", None), ("b", None)]);
        let result = eval(&d, &r, &numeric_types(&["a", "b"])).unwrap();
        assert_eq!(result["total"], None);
    }

    #[test]
    fn resolves_forward_reference_to_another_calculated_field() {
        let d = def(vec![
            FieldDef {
                name: "total".to_string(),
                expr: add(col("double_price"), Expr::NumberLiteral("1".to_string())),
            },
            FieldDef {
                name: "double_price".to_string(),
                expr: add(col("price"), col("price")),
            },
        ]);
        let r = row(&[("price", Some("2"))]);
        let result = eval(&d, &r, &numeric_types(&["price"])).unwrap();
        match (&result["double_price"], &result["total"]) {
            (Some(Value::Numeric(dp)), Some(Value::Numeric(t))) => {
                assert_eq!(dp.to_string(), "4");
                assert_eq!(t.to_string(), "5");
            }
            other => panic!("expected Numeric values, got {other:?}"),
        }
    }

    #[test]
    fn missing_column_is_an_error_not_null() {
        let d = def(vec![FieldDef {
            name: "total".to_string(),
            expr: col("price"),
        }]);
        let r = row(&[]);
        let err = eval(&d, &r, &numeric_types(&["price"])).unwrap_err();
        assert_eq!(
            err,
            EvalError::MissingColumn {
                field: "total".to_string(),
                column: "price".to_string(),
            }
        );
    }

    #[test]
    fn cyclic_definition_returns_error_instead_of_overflowing() {
        // Bypasses the validator (#23), which is supposed to reject this,
        // to exercise the evaluator's own defense-in-depth guard.
        let d = def(vec![
            FieldDef {
                name: "a".to_string(),
                expr: add(col("b"), Expr::NumberLiteral("1".to_string())),
            },
            FieldDef {
                name: "b".to_string(),
                expr: add(col("a"), Expr::NumberLiteral("1".to_string())),
            },
        ]);
        let r = row(&[]);
        let err = eval(&d, &r, &HashMap::new()).unwrap_err();
        assert!(matches!(err, EvalError::Cycle(_)));
    }

    #[test]
    fn evaluation_is_deterministic() {
        let d = def(vec![FieldDef {
            name: "total".to_string(),
            expr: add(col("price"), col("tax")),
        }]);
        let r = row(&[("price", Some("10.5")), ("tax", Some("2.25"))]);
        let types = numeric_types(&["price", "tax"]);
        assert_eq!(eval(&d, &r, &types).unwrap(), eval(&d, &r, &types).unwrap());
    }

    #[test]
    fn text_column_passes_through_unchanged() {
        let d = def(vec![FieldDef {
            name: "out".to_string(),
            expr: col("text_col"),
        }]);
        let r = row(&[("text_col", Some("hello world"))]);
        let types: HashMap<String, ValueType> =
            HashMap::from([("text_col".to_string(), ValueType::Text)]);
        let result = eval(&d, &r, &types).unwrap();
        assert_eq!(result["out"], Some(Value::Text("hello world".to_string())));
    }

    #[test]
    fn boolean_column_passes_through_unchanged() {
        let d = def(vec![FieldDef {
            name: "out".to_string(),
            expr: col("flag"),
        }]);
        let r = row(&[("flag", Some("t"))]);
        let types: HashMap<String, ValueType> =
            HashMap::from([("flag".to_string(), ValueType::Boolean)]);
        let result = eval(&d, &r, &types).unwrap();
        assert_eq!(result["out"], Some(Value::Boolean(true)));
    }

    #[test]
    fn uuid_column_passes_through_unchanged() {
        let d = def(vec![FieldDef {
            name: "author".to_string(),
            expr: col("author"),
        }]);
        let r = row(&[("author", Some("11111111-1111-1111-1111-111111111111"))]);
        let types: HashMap<String, ValueType> =
            HashMap::from([("author".to_string(), ValueType::Uuid)]);
        let result = eval(&d, &r, &types).unwrap();
        assert_eq!(
            result["author"],
            Some(Value::Uuid(
                "11111111-1111-1111-1111-111111111111".to_string()
            ))
        );
    }

    /// Issue #108's typed CDC round-trip: a column classified as
    /// [`ValueType::Other`] (a recognized-but-not-first-class-yet PG type
    /// family, e.g. `jsonb`) still passes through `parse_value` tagged with
    /// its real [`PgType`], carrying the CDC-decoded text verbatim exactly
    /// like [`Value::Uuid`]/[`Value::Text`] above — not silently
    /// mislabeled/misrouted as `Text` the way it collapsed pre-#108.
    #[test]
    fn other_typed_column_passes_through_tagged_with_its_real_pg_type() {
        let d = def(vec![FieldDef {
            name: "payload".to_string(),
            expr: col("payload"),
        }]);
        let r = row(&[("payload", Some(r#"{"a": 1}"#))]);
        let types: HashMap<String, ValueType> =
            HashMap::from([("payload".to_string(), ValueType::Other(PgType::Jsonb))]);
        let result = eval(&d, &r, &types).unwrap();
        assert_eq!(
            result["payload"],
            Some(Value::Other(PgType::Jsonb, r#"{"a": 1}"#.to_string()))
        );
        // `value_type()`/`Display` stay consistent with the tagged value,
        // the same contract every other `Value` variant holds.
        assert_eq!(
            result["payload"].as_ref().unwrap().value_type(),
            ValueType::Other(PgType::Jsonb)
        );
        assert_eq!(
            result["payload"].as_ref().unwrap().to_string(),
            r#"{"a": 1}"#
        );
    }

    #[test]
    fn string_literal_evaluates_to_text() {
        let d = def(vec![FieldDef {
            name: "out".to_string(),
            expr: Expr::StringLiteral("hi".to_string()),
        }]);
        let result = eval(&d, &Row::new(), &HashMap::new()).unwrap();
        assert_eq!(result["out"], Some(Value::Text("hi".to_string())));
    }

    fn call(name: &str, args: Vec<Expr>) -> Expr {
        Expr::FunctionCall {
            name: name.to_string(),
            args,
        }
    }

    fn text_types(names: &[&str]) -> HashMap<String, ValueType> {
        names
            .iter()
            .map(|n| (n.to_string(), ValueType::Text))
            .collect()
    }

    fn numeric_of(result: &HashMap<String, Option<Value>>, field: &str) -> String {
        match result[field].as_ref().unwrap() {
            Value::Numeric(n) => n.to_string(),
            other => panic!("expected Numeric, got {other:?}"),
        }
    }

    #[test]
    fn octet_length_counts_utf8_bytes_not_characters() {
        let d = def(vec![FieldDef {
            name: "out".to_string(),
            expr: call("OCTET_LENGTH", vec![col("text_col")]),
        }]);
        // "café" is 4 characters but 5 bytes (é is a 2-byte UTF-8 sequence).
        let r = row(&[("text_col", Some("café"))]);
        let result = eval(&d, &r, &text_types(&["text_col"])).unwrap();
        assert_eq!(numeric_of(&result, "out"), "5");
    }

    #[test]
    fn char_length_counts_codepoints_not_bytes() {
        let d = def(vec![FieldDef {
            name: "out".to_string(),
            expr: call("CHAR_LENGTH", vec![col("text_col")]),
        }]);
        let r = row(&[("text_col", Some("café"))]);
        let result = eval(&d, &r, &text_types(&["text_col"])).unwrap();
        assert_eq!(numeric_of(&result, "out"), "4");
    }

    #[test]
    fn octet_length_and_char_length_diverge_on_emoji() {
        let d = def(vec![
            FieldDef {
                name: "bytes".to_string(),
                expr: call("OCTET_LENGTH", vec![col("text_col")]),
            },
            FieldDef {
                name: "chars".to_string(),
                expr: call("CHAR_LENGTH", vec![col("text_col")]),
            },
        ]);
        // A single emoji codepoint is a 4-byte UTF-8 sequence.
        let r = row(&[("text_col", Some("🎉"))]);
        let result = eval(&d, &r, &text_types(&["text_col"])).unwrap();
        assert_eq!(numeric_of(&result, "bytes"), "4");
        assert_eq!(numeric_of(&result, "chars"), "1");
    }

    #[test]
    fn strpos_finds_a_multibyte_substring() {
        let d = def(vec![FieldDef {
            name: "out".to_string(),
            expr: call("STRPOS", vec![col("haystack"), col("needle")]),
        }]);
        let types: HashMap<String, ValueType> = HashMap::from([
            ("haystack".to_string(), ValueType::Text),
            ("needle".to_string(), ValueType::Text),
        ]);
        let r = row(&[("haystack", Some("café bar")), ("needle", Some("bar"))]);
        let result = eval(&d, &r, &types).unwrap();
        // "café " is 5 characters, so "bar" starts at character position 6.
        assert_eq!(numeric_of(&result, "out"), "6");
    }

    #[test]
    fn strpos_returns_zero_when_absent() {
        let d = def(vec![FieldDef {
            name: "out".to_string(),
            expr: call("STRPOS", vec![col("haystack"), col("needle")]),
        }]);
        let types: HashMap<String, ValueType> = HashMap::from([
            ("haystack".to_string(), ValueType::Text),
            ("needle".to_string(), ValueType::Text),
        ]);
        let r = row(&[("haystack", Some("hello")), ("needle", Some("xyz"))]);
        let result = eval(&d, &r, &types).unwrap();
        assert_eq!(numeric_of(&result, "out"), "0");
    }

    #[test]
    fn regexp_count_counts_non_overlapping_literal_matches() {
        let d = def(vec![FieldDef {
            name: "out".to_string(),
            expr: call("REGEXP_COUNT", vec![col("text_col"), col("pattern")]),
        }]);
        let types: HashMap<String, ValueType> = HashMap::from([
            ("text_col".to_string(), ValueType::Text),
            ("pattern".to_string(), ValueType::Text),
        ]);
        let r = row(&[("text_col", Some("abcabcabc")), ("pattern", Some("abc"))]);
        let result = eval(&d, &r, &types).unwrap();
        assert_eq!(numeric_of(&result, "out"), "3");
    }

    #[test]
    fn regexp_count_over_multibyte_text() {
        let d = def(vec![FieldDef {
            name: "out".to_string(),
            expr: call("REGEXP_COUNT", vec![col("text_col"), col("pattern")]),
        }]);
        let types: HashMap<String, ValueType> = HashMap::from([
            ("text_col".to_string(), ValueType::Text),
            ("pattern".to_string(), ValueType::Text),
        ]);
        let r = row(&[
            ("text_col", Some("café café café")),
            ("pattern", Some("café")),
        ]);
        let result = eval(&d, &r, &types).unwrap();
        assert_eq!(numeric_of(&result, "out"), "3");
    }

    #[test]
    fn regexp_count_matches_real_regex_metacharacters() {
        let d = def(vec![FieldDef {
            name: "out".to_string(),
            expr: call("REGEXP_COUNT", vec![col("text_col"), col("pattern")]),
        }]);
        let types: HashMap<String, ValueType> = HashMap::from([
            ("text_col".to_string(), ValueType::Text),
            ("pattern".to_string(), ValueType::Text),
        ]);
        // "a.c" as a regex matches any character between 'a' and 'c'.
        let r = row(&[
            ("text_col", Some("abc adc aec xyz")),
            ("pattern", Some("a.c")),
        ]);
        let result = eval(&d, &r, &types).unwrap();
        assert_eq!(numeric_of(&result, "out"), "3");
    }

    #[test]
    fn regexp_count_counts_a_trailing_empty_match_like_postgres() {
        let d = def(vec![FieldDef {
            name: "out".to_string(),
            expr: call("REGEXP_COUNT", vec![col("text_col"), col("pattern")]),
        }]);
        let types: HashMap<String, ValueType> = HashMap::from([
            ("text_col".to_string(), ValueType::Text),
            ("pattern".to_string(), ValueType::Text),
        ]);
        // Postgres counts the "aaa" match, then a zero-width match at the
        // end of the string: 2, not 1.
        let r = row(&[("text_col", Some("aaa")), ("pattern", Some("a*"))]);
        let result = eval(&d, &r, &types).unwrap();
        assert_eq!(numeric_of(&result, "out"), "2");
    }

    #[test]
    fn regexp_count_with_an_uncompilable_pattern_returns_null() {
        // Bypasses the validator (#23/#64), which is supposed to reject
        // this at validate time, to exercise the evaluator's own
        // defense-in-depth guard against a hand-built AST.
        let d = def(vec![FieldDef {
            name: "out".to_string(),
            expr: call("REGEXP_COUNT", vec![col("text_col"), col("pattern")]),
        }]);
        let types: HashMap<String, ValueType> = HashMap::from([
            ("text_col".to_string(), ValueType::Text),
            ("pattern".to_string(), ValueType::Text),
        ]);
        let r = row(&[("text_col", Some("abc")), ("pattern", Some("("))]);
        let result = eval(&d, &r, &types).unwrap();
        assert_eq!(result["out"], None);
    }

    fn gt(lhs: Expr, rhs: Expr) -> Expr {
        Expr::BinaryOp {
            op: Operator::GreaterThan,
            lhs: Box::new(lhs),
            rhs: Box::new(rhs),
        }
    }

    #[test]
    fn greater_than_evaluates_to_boolean() {
        let d = def(vec![FieldDef {
            name: "out".to_string(),
            expr: gt(col("a"), Expr::NumberLiteral("0".to_string())),
        }]);
        let r = row(&[("a", Some("5"))]);
        let result = eval(&d, &r, &numeric_types(&["a"])).unwrap();
        assert_eq!(result["out"], Some(Value::Boolean(true)));
    }

    #[test]
    fn greater_than_is_false_when_not_strictly_greater() {
        let d = def(vec![FieldDef {
            name: "out".to_string(),
            expr: gt(col("a"), Expr::NumberLiteral("5".to_string())),
        }]);
        let r = row(&[("a", Some("5"))]);
        let result = eval(&d, &r, &numeric_types(&["a"])).unwrap();
        assert_eq!(result["out"], Some(Value::Boolean(false)));
    }

    #[test]
    fn greater_than_propagates_null() {
        let d = def(vec![FieldDef {
            name: "out".to_string(),
            expr: gt(col("a"), Expr::NumberLiteral("0".to_string())),
        }]);
        let r = row(&[("a", None)]);
        let result = eval(&d, &r, &numeric_types(&["a"])).unwrap();
        assert_eq!(result["out"], None);
    }

    #[test]
    fn function_call_composed_with_greater_than() {
        let d = def(vec![FieldDef {
            name: "has_foo".to_string(),
            expr: gt(
                call(
                    "STRPOS",
                    vec![col("name"), Expr::StringLiteral("foo".to_string())],
                ),
                Expr::NumberLiteral("0".to_string()),
            ),
        }]);
        let r = row(&[("name", Some("has foo in it"))]);
        let result = eval(&d, &r, &text_types(&["name"])).unwrap();
        assert_eq!(result["has_foo"], Some(Value::Boolean(true)));

        let r_absent = row(&[("name", Some("no match here"))]);
        let result_absent = eval(&d, &r_absent, &text_types(&["name"])).unwrap();
        assert_eq!(result_absent["has_foo"], Some(Value::Boolean(false)));
    }

    #[test]
    fn function_call_returns_null_when_an_argument_is_null() {
        let d = def(vec![FieldDef {
            name: "out".to_string(),
            expr: call("OCTET_LENGTH", vec![col("text_col")]),
        }]);
        let r = row(&[("text_col", None)]);
        let result = eval(&d, &r, &text_types(&["text_col"])).unwrap();
        assert_eq!(result["out"], None);
    }

    #[test]
    fn coalesce_returns_the_first_non_null_argument() {
        // `COALESCE(a, b)` — matches Postgres: the leftmost argument that
        // isn't NULL wins, and later arguments are never consulted once one
        // is found (the loop returns on the first `is_some()`).
        let d = def(vec![FieldDef {
            name: "out".to_string(),
            expr: call("COALESCE", vec![col("a"), col("b")]),
        }]);

        // a is NULL, so b is used.
        let r = row(&[("a", None), ("b", Some("7"))]);
        let result = eval(&d, &r, &numeric_types(&["a", "b"])).unwrap();
        match result["out"].as_ref().unwrap() {
            Value::Numeric(n) => assert_eq!(n.to_string(), "7"),
            other => panic!("expected Numeric, got {other:?}"),
        }

        // a is present, so a wins and b is irrelevant.
        let r = row(&[("a", Some("3")), ("b", Some("7"))]);
        let result = eval(&d, &r, &numeric_types(&["a", "b"])).unwrap();
        match result["out"].as_ref().unwrap() {
            Value::Numeric(n) => assert_eq!(n.to_string(), "3"),
            other => panic!("expected Numeric, got {other:?}"),
        }
    }

    #[test]
    fn coalesce_falls_through_to_a_literal_default() {
        // The idiomatic "replace NULL with a constant" shape: every column
        // argument is NULL, so the trailing literal is returned.
        let d = def(vec![FieldDef {
            name: "out".to_string(),
            expr: call(
                "COALESCE",
                vec![col("a"), col("b"), Expr::NumberLiteral("0".to_string())],
            ),
        }]);
        let r = row(&[("a", None), ("b", None)]);
        let result = eval(&d, &r, &numeric_types(&["a", "b"])).unwrap();
        match result["out"].as_ref().unwrap() {
            Value::Numeric(n) => assert_eq!(n.to_string(), "0"),
            other => panic!("expected Numeric, got {other:?}"),
        }
    }

    #[test]
    fn coalesce_is_null_when_every_argument_is_null() {
        // Postgres `COALESCE` over all-NULL arguments is itself NULL.
        let d = def(vec![FieldDef {
            name: "out".to_string(),
            expr: call("COALESCE", vec![col("a"), col("b")]),
        }]);
        let r = row(&[("a", None), ("b", None)]);
        let result = eval(&d, &r, &numeric_types(&["a", "b"])).unwrap();
        assert_eq!(result["out"], None);
    }

    fn aggregate_def(group_by: &[&str], fields: Vec<FieldDef>) -> TransformDef {
        TransformDef {
            target: "t".to_string(),
            source: "s".to_string(),
            key_space: KeySpace::Aggregate {
                group_by: group_by
                    .iter()
                    .map(|s| GroupByKey::Column(s.to_string()))
                    .collect(),
            },
            fields,
            predicate: Predicate::True,
            explicit_source_schema: None,
            explicit_target_schema: None,
        }
    }

    #[test]
    fn aggregate_argument_can_reference_another_calculated_field() {
        // `GROUP BY id SELECT (id + 1) AS adj, SUM(adj) AS t` — the
        // validator doesn't restrict an aggregate's argument to source-only
        // columns, and Postgres itself resolves `adj` fine, so the
        // evaluator must too (fold_aggregate must see the real
        // fields_by_name map, not an empty one).
        let d = aggregate_def(
            &["id"],
            vec![
                FieldDef {
                    name: "adj".to_string(),
                    expr: add(col("id"), Expr::NumberLiteral("1".to_string())),
                },
                FieldDef {
                    name: "t".to_string(),
                    expr: call("SUM", vec![col("adj")]),
                },
            ],
        );
        let rows = vec![
            row(&[("id", Some("1"))]),
            row(&[("id", Some("1"))]),
            row(&[("id", Some("1"))]),
        ];
        let result =
            evaluate_aggregate(&d, &rows, &numeric_types(&["id"]), &mut RegexCache::new()).unwrap();
        match result["t"].as_ref().unwrap() {
            Value::Numeric(n) => assert_eq!(n.to_string(), "6"),
            other => panic!("expected Numeric, got {other:?}"),
        }
    }

    #[test]
    fn coalesce_over_an_aggregate_replaces_the_empty_group_null() {
        // The motivating case (PR #64): `SUM` over a group whose summed
        // column is entirely NULL yields NULL (Postgres empty-set SUM), and
        // `COALESCE(SUM(amount), 0)` turns that into 0 rather than letting the
        // NULL propagate. Exercises the aggregate-path COALESCE arm.
        let d = aggregate_def(
            &["id"],
            vec![
                FieldDef {
                    name: "id".to_string(),
                    expr: col("id"),
                },
                FieldDef {
                    name: "total".to_string(),
                    expr: call(
                        "COALESCE",
                        vec![
                            call("SUM", vec![col("amount")]),
                            Expr::NumberLiteral("0".to_string()),
                        ],
                    ),
                },
            ],
        );
        let rows = vec![
            row(&[("id", Some("1")), ("amount", None)]),
            row(&[("id", Some("1")), ("amount", None)]),
        ];
        let result = evaluate_aggregate(
            &d,
            &rows,
            &numeric_types(&["id", "amount"]),
            &mut RegexCache::new(),
        )
        .unwrap();
        match result["total"].as_ref().unwrap() {
            Value::Numeric(n) => assert_eq!(n.to_string(), "0"),
            other => panic!("expected Numeric, got {other:?}"),
        }
    }

    // --- to-one relationship path resolution (issue #28) ---

    fn rel_path(rel: &str, column: &str) -> Expr {
        Expr::RelationshipPath {
            rel: rel.to_string(),
            column: column.to_string(),
        }
    }

    /// A to-one relationship named `category` whose from-side FK column is
    /// `category_id`, joining to a `categories` table keyed by `id`, with a
    /// `name` (Text) and `rate` (Numeric) column on the to-side.
    fn category_context() -> RelationshipContext {
        let mut to_columns = HashMap::new();
        to_columns.insert("name".to_string(), ValueType::Text);
        to_columns.insert("rate".to_string(), ValueType::Numeric);

        let mut to_rows_by_key = HashMap::new();
        to_rows_by_key.insert(
            "10".to_string(),
            row(&[
                ("id", Some("10")),
                ("name", Some("Widgets")),
                ("rate", Some("1.5")),
            ]),
        );
        to_rows_by_key.insert(
            "20".to_string(),
            row(&[("id", Some("20")), ("name", None), ("rate", Some("2"))]),
        );

        let mut by_name = HashMap::new();
        by_name.insert(
            "category".to_string(),
            ToOneRelationship {
                from_col: "category_id".to_string(),
                cardinality: RelationshipCardinality::ToOne,
                to_columns,
                to_rows_by_key,
            },
        );
        RelationshipContext::new(by_name)
    }

    fn eval_rel(
        d: &TransformDef,
        r: &Row,
        types: &HashMap<String, ValueType>,
        rels: &RelationshipContext,
    ) -> Result<HashMap<String, Option<Value>>, EvalError> {
        evaluate_with_relationships(d, r, types, rels, &mut RegexCache::new())
    }

    #[test]
    fn to_one_path_reads_the_matched_to_side_column() {
        let d = def(vec![FieldDef {
            name: "category_name".to_string(),
            expr: rel_path("category", "name"),
        }]);
        let r = row(&[("category_id", Some("10"))]);
        let result = eval_rel(
            &d,
            &r,
            &numeric_types(&["category_id"]),
            &category_context(),
        )
        .unwrap();
        match result["category_name"].as_ref().unwrap() {
            Value::Text(s) => assert_eq!(s, "Widgets"),
            other => panic!("expected Text, got {other:?}"),
        }
    }

    #[test]
    fn to_one_path_numeric_column_parses_with_to_side_type() {
        let d = def(vec![FieldDef {
            name: "category_rate".to_string(),
            expr: rel_path("category", "rate"),
        }]);
        let r = row(&[("category_id", Some("10"))]);
        let result = eval_rel(
            &d,
            &r,
            &numeric_types(&["category_id"]),
            &category_context(),
        )
        .unwrap();
        match result["category_rate"].as_ref().unwrap() {
            Value::Numeric(n) => assert_eq!(n.to_string(), "1.5"),
            other => panic!("expected Numeric, got {other:?}"),
        }
    }

    #[test]
    fn to_one_path_no_match_is_null() {
        // FK 99 has no matching to-side row — LEFT JOIN leaves the enrichment
        // NULL while the from-row still exists.
        let d = def(vec![FieldDef {
            name: "category_name".to_string(),
            expr: rel_path("category", "name"),
        }]);
        let r = row(&[("category_id", Some("99"))]);
        let result = eval_rel(
            &d,
            &r,
            &numeric_types(&["category_id"]),
            &category_context(),
        )
        .unwrap();
        assert_eq!(result["category_name"], None);
    }

    #[test]
    fn to_one_path_null_fk_is_null() {
        // A NULL join key never matches (SQL NULL != NULL).
        let d = def(vec![FieldDef {
            name: "category_name".to_string(),
            expr: rel_path("category", "name"),
        }]);
        let r = row(&[("category_id", None)]);
        let result = eval_rel(
            &d,
            &r,
            &numeric_types(&["category_id"]),
            &category_context(),
        )
        .unwrap();
        assert_eq!(result["category_name"], None);
    }

    #[test]
    fn to_one_path_null_to_side_column_is_null() {
        // FK 20 matches, but that to-side row's `name` is NULL.
        let d = def(vec![FieldDef {
            name: "category_name".to_string(),
            expr: rel_path("category", "name"),
        }]);
        let r = row(&[("category_id", Some("20"))]);
        let result = eval_rel(
            &d,
            &r,
            &numeric_types(&["category_id"]),
            &category_context(),
        )
        .unwrap();
        assert_eq!(result["category_name"], None);
    }

    #[test]
    fn to_one_path_composes_in_a_larger_expression() {
        // A relationship path is an ordinary sub-expression: rate + 10.
        let d = def(vec![FieldDef {
            name: "adjusted".to_string(),
            expr: add(
                rel_path("category", "rate"),
                Expr::NumberLiteral("10".to_string()),
            ),
        }]);
        let r = row(&[("category_id", Some("10"))]);
        let result = eval_rel(
            &d,
            &r,
            &numeric_types(&["category_id"]),
            &category_context(),
        )
        .unwrap();
        match result["adjusted"].as_ref().unwrap() {
            Value::Numeric(n) => assert_eq!(n.to_string(), "11.5"),
            other => panic!("expected Numeric, got {other:?}"),
        }
    }

    #[test]
    fn unknown_relationship_errors() {
        let d = def(vec![FieldDef {
            name: "x".to_string(),
            expr: rel_path("nonexistent", "name"),
        }]);
        let r = row(&[("category_id", Some("10"))]);
        let err = eval_rel(
            &d,
            &r,
            &numeric_types(&["category_id"]),
            &category_context(),
        )
        .unwrap_err();
        assert!(
            matches!(err, EvalError::UnknownRelationship { .. }),
            "got {err:?}"
        );
    }

    #[test]
    fn to_many_bare_path_requires_aggregate() {
        let mut by_name = HashMap::new();
        by_name.insert(
            "orders".to_string(),
            ToOneRelationship {
                from_col: "id".to_string(),
                cardinality: RelationshipCardinality::ToMany,
                to_columns: HashMap::new(),
                to_rows_by_key: HashMap::new(),
            },
        );
        let rels = RelationshipContext::new(by_name);
        let d = def(vec![FieldDef {
            name: "x".to_string(),
            expr: rel_path("orders", "total"),
        }]);
        let r = row(&[("id", Some("1"))]);
        let err = eval_rel(&d, &r, &numeric_types(&["id"]), &rels).unwrap_err();
        assert!(
            matches!(err, EvalError::AggregateRequiredForToMany { .. }),
            "got {err:?}"
        );
    }

    #[test]
    fn plain_evaluate_has_no_relationships_so_a_path_errors() {
        // The pure entry supplies an empty context — matching pre-#28 behavior
        // that any relationship path is unresolvable.
        let d = def(vec![FieldDef {
            name: "x".to_string(),
            expr: rel_path("category", "name"),
        }]);
        let r = row(&[("category_id", Some("10"))]);
        let err = eval(&d, &r, &numeric_types(&["category_id"])).unwrap_err();
        assert!(
            matches!(err, EvalError::UnknownRelationship { .. }),
            "got {err:?}"
        );
    }

    // --- to-many aggregate relationship enrichment (issue #29) ---

    /// A to-many relationship `comments` whose from-side join column is `id`
    /// (the post's PK), keyed by that value; each related comment row carries a
    /// Numeric `word_count`. Post `1` has three comments (one with a NULL
    /// word_count), post `2` has one, post `3` has none.
    fn comments_context() -> RelationshipContext {
        let mut to_columns = HashMap::new();
        to_columns.insert("word_count".to_string(), ValueType::Numeric);

        let mut to_rows_by_key: HashMap<String, Vec<Row>> = HashMap::new();
        to_rows_by_key.insert(
            "1".to_string(),
            vec![
                row(&[("word_count", Some("10"))]),
                row(&[("word_count", Some("20"))]),
                row(&[("word_count", None)]),
            ],
        );
        to_rows_by_key.insert("2".to_string(), vec![row(&[("word_count", Some("5"))])]);

        let mut to_many = HashMap::new();
        to_many.insert(
            "comments".to_string(),
            ToManyRelationship {
                from_col: "id".to_string(),
                to_columns,
                to_rows_by_key,
            },
        );
        RelationshipContext::default().with_to_many(to_many)
    }

    fn agg_rel(func: &str) -> TransformDef {
        def(vec![FieldDef {
            name: "out".to_string(),
            expr: call(func, vec![rel_path("comments", "word_count")]),
        }])
    }

    #[test]
    fn to_many_sum_folds_related_rows_skipping_null() {
        // 10 + 20, the NULL word_count skipped (Postgres SUM skips NULLs).
        let r = row(&[("id", Some("1"))]);
        let result = eval_rel(
            &agg_rel("SUM"),
            &r,
            &numeric_types(&["id"]),
            &comments_context(),
        )
        .unwrap();
        match result["out"].as_ref().unwrap() {
            Value::Numeric(n) => assert_eq!(n.to_string(), "30"),
            other => panic!("expected Numeric, got {other:?}"),
        }
    }

    #[test]
    fn to_many_min_max_avg_over_related_rows() {
        let r = row(&[("id", Some("1"))]);
        let types = numeric_types(&["id"]);
        let ctx = comments_context();
        let min = eval_rel(&agg_rel("MIN"), &r, &types, &ctx).unwrap();
        let max = eval_rel(&agg_rel("MAX"), &r, &types, &ctx).unwrap();
        let avg = eval_rel(&agg_rel("AVG"), &r, &types, &ctx).unwrap();
        assert_eq!(
            min["out"],
            Some(Value::Numeric(Numeric::parse("10").unwrap()))
        );
        assert_eq!(
            max["out"],
            Some(Value::Numeric(Numeric::parse("20").unwrap()))
        );
        // (10 + 20) / 2 — the NULL row does not count toward AVG's divisor.
        // AVG carries a fractional scale (like Postgres `numeric` avg), so
        // compare by value rather than by exact scale/text.
        match avg["out"].as_ref().unwrap() {
            Value::Numeric(n) => {
                assert_eq!(
                    n.compare(&Numeric::parse("15").unwrap()),
                    std::cmp::Ordering::Equal
                )
            }
            other => panic!("expected Numeric, got {other:?}"),
        }
    }

    #[test]
    fn to_many_count_counts_non_null_related_values() {
        // COUNT(comments.word_count): 3 comments, but the NULL word_count is
        // not counted (Postgres COUNT(<col>) counts non-NULLs) => 2.
        let r = row(&[("id", Some("1"))]);
        let result = eval_rel(
            &agg_rel("COUNT"),
            &r,
            &numeric_types(&["id"]),
            &comments_context(),
        )
        .unwrap();
        assert_eq!(
            result["out"],
            Some(Value::Numeric(Numeric::parse("2").unwrap()))
        );
    }

    #[test]
    fn to_many_empty_set_count_is_zero_others_null() {
        // Post 3 has no related comments — the empty set.
        let r = row(&[("id", Some("3"))]);
        let types = numeric_types(&["id"]);
        let ctx = comments_context();
        assert_eq!(
            eval_rel(&agg_rel("COUNT"), &r, &types, &ctx).unwrap()["out"],
            Some(Value::Numeric(Numeric::parse("0").unwrap())),
            "COUNT over empty set is 0"
        );
        for func in ["SUM", "MIN", "MAX", "AVG"] {
            assert_eq!(
                eval_rel(&agg_rel(func), &r, &types, &ctx).unwrap()["out"],
                None,
                "{func} over empty set is NULL"
            );
        }
    }

    #[test]
    fn to_many_null_join_key_is_the_empty_set() {
        // A NULL from-side join key never joins => empty set (COUNT 0).
        let r = row(&[("id", None)]);
        let result = eval_rel(
            &agg_rel("COUNT"),
            &r,
            &numeric_types(&["id"]),
            &comments_context(),
        )
        .unwrap();
        assert_eq!(
            result["out"],
            Some(Value::Numeric(Numeric::parse("0").unwrap()))
        );
    }

    #[test]
    fn to_many_unknown_relationship_errors() {
        let r = row(&[("id", Some("1"))]);
        let d = def(vec![FieldDef {
            name: "out".to_string(),
            expr: call("SUM", vec![rel_path("nope", "word_count")]),
        }]);
        let err = eval_rel(&d, &r, &numeric_types(&["id"]), &comments_context()).unwrap_err();
        assert!(
            matches!(err, EvalError::UnknownRelationship { .. }),
            "got {err:?}"
        );
    }
}
