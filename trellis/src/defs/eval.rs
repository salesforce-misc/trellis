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

use crate::float::{self, FloatError, FloatWidth};
use crate::integer::{self, IntWidth, IntegerError};
use crate::numeric::{Numeric, NumericParseError};

#[cfg(any(test, feature = "test-util"))]
use super::ast::KeySpace;
use super::ast::{Expr, FieldDef, Operator, TransformDef, ValueType};
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

/// A calculated value the evaluator produces, per [`ValueType`]. `Uuid`
/// (issue #79) carries its Postgres text rendering verbatim, the same way
/// `Text` does — there's no arithmetic to normalize it against, just
/// passthrough and equality comparison.
///
/// [`Value::Other`] (issue #108) is [`ValueType::Other`]'s value-level twin:
/// a [`PgType`]-tagged column's captured text, carried verbatim exactly
/// like `Uuid`/`Text` — this is the "typed CDC round-trip" the issue asks
/// for, a value now remembers its real Postgres type family all the way
/// through [`parse_value`] instead of arriving pre-flattened to `Text`.
///
/// [`Value::Integer`] (issue #111) is the exact-integer family's value-level
/// twin, carrying its [`IntWidth`] alongside the value for the same reason
/// [`ValueType::Integer`] does: the width *is* the overflow boundary, so no
/// operation could reproduce Postgres's `22003` without it. It holds a real
/// `i64` — not text, and not a [`Numeric`] — so arithmetic on it is checked
/// rather than arbitrary-precision.
///
/// [`Value::Float`] (issue #112) is the same shape for the binary floats,
/// holding a real `f64` (narrowed onto `real`'s grid when its width says so
/// — see [`FloatWidth::round`]) rather than a decimal. It is why this enum
/// hand-writes [`PartialEq`] instead of deriving it: `f64`'s `==` is IEEE's,
/// and Postgres's float equality deliberately is not (`NaN = NaN` is true,
/// `-0 = 0` is true). See [`crate::float`].
#[derive(Debug, Clone)]
pub enum Value {
    Numeric(Numeric),
    Integer(IntWidth, i64),
    Float(FloatWidth, f64),
    Text(String),
    Boolean(bool),
    Uuid(String),
    Other(PgType, String),
}

/// Postgres's equality, not Rust's derived one. The only variant that
/// differs is [`Value::Float`], and it differs in both directions: two
/// `NaN`s are **equal** (IEEE says they aren't) and `-0.0` equals `0.0`
/// (IEEE agrees, but a derived `PartialEq` on the `f64` would too, so only
/// the `NaN` half is a real change).
///
/// This matters beyond tests. Every "did this value change?" comparison in
/// the engine runs through here; under IEEE semantics a `NaN`-valued field
/// would compare unequal to itself forever, so a converged target row would
/// look perpetually dirty. Routing through [`crate::float::equal`] — the
/// same function `>` and the aggregate folds use — keeps one definition of
/// float equality in the crate.
impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Numeric(a), Value::Numeric(b)) => a == b,
            (Value::Integer(aw, a), Value::Integer(bw, b)) => aw == bw && a == b,
            (Value::Float(aw, a), Value::Float(bw, b)) => aw == bw && float::equal(*a, *b),
            (Value::Text(a), Value::Text(b)) => a == b,
            (Value::Boolean(a), Value::Boolean(b)) => a == b,
            (Value::Uuid(a), Value::Uuid(b)) => a == b,
            (Value::Other(at, a), Value::Other(bt, b)) => at == bt && a == b,
            _ => false,
        }
    }
}

impl Value {
    /// This value's [`ValueType`]: the aggregate folds dispatch on whether a
    /// value is in the exact-numeric family.
    #[cfg(any(test, feature = "test-util"))]
    pub fn value_type(&self) -> ValueType {
        match self {
            Value::Numeric(_) => ValueType::Numeric,
            Value::Integer(width, _) => ValueType::Integer(*width),
            Value::Float(width, _) => ValueType::Float(*width),
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
            // Rust's own `i64` formatting *is* Postgres's canonical integer
            // output: an optional `-`, then digits, with no `+`, no leading
            // zeros and no separators. That agreement is what lets an
            // integer value round-trip through the still-text-keyed
            // join/primary-key/`GROUP BY` paths byte-for-byte — see
            // `join_key_types::TEXT_STABLE_JOIN_KEY_TYPES`.
            Value::Integer(_, n) => write!(f, "{n}"),
            // Issue #112: `crate::float::render` reproduces `float4out`/
            // `float8out` under `extra_float_digits >= 1` exactly, so a
            // float value's text here is byte-identical to what Postgres
            // would have written for the same value — which is what lets it
            // survive the round-trip through the text-carried staging ring
            // and back through a `::real`/`::double precision` cast.
            Value::Float(width, n) => f.write_str(&float::render(*n, *width)),
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
    /// An exact-integer (`smallint`/`integer`/`bigint`) operation left its
    /// result type's range, or a column's text didn't decode as a value of
    /// its declared width (issue #111).
    ///
    /// **Unlike every other variant here, this one is not defense-in-depth.**
    /// It is a genuine, *data-dependent* runtime failure that a perfectly
    /// valid definition can hit on one row and not the next — and it is
    /// exactly the failure Postgres itself raises
    /// (`22003 numeric_value_out_of_range`, `"integer out of range"`) for
    /// the same expression, which ADR-0004 requires Trellis to reproduce
    /// rather than silently widening to arbitrary precision. Per ADR-0003's
    /// column-level quarantine it pauses the offending
    /// `(transform, column)` pair, which is the right outcome: that column's
    /// definition overflows for this data and needs a human, while every
    /// other column on the same target keeps flowing.
    IntegerOutOfRange { field: String, source: IntegerError },
    /// A binary-float (`real`/`double precision`) operation overflowed its
    /// width's finite range, or a column's text didn't decode as a
    /// canonical value of its declared width (issue #112).
    ///
    /// [`EvalError::IntegerOutOfRange`]'s float twin, and not
    /// defense-in-depth for the same reason: `select 3.4e38::float4 +
    /// 3.4e38::float4` raises `22003 value out of range: overflow` on a real
    /// server, so Trellis must raise there too rather than quietly returning
    /// `Infinity`. Note what is deliberately *not* here: `NaN` and
    /// `Infinity` arriving as data, or arising from `'Infinity' + 1`, are
    /// ordinary values in Postgres and stay ordinary values here.
    FloatOutOfRange { field: String, source: FloatError },
    /// A calculated field was re-entered while still being resolved on the
    /// current recursion path. The validator (#23) is supposed to reject
    /// cyclic definitions before they reach here, but `evaluate` is `pub`
    /// and gets exercised standalone in tests and by future callers (#25),
    /// so this is defense-in-depth against a stack overflow.
    Cycle(String),
}

impl EvalError {
    /// This error's stable, coarse [`ErrorCode`] category (`docs/decisions/0008-public-api-design.md`,
    /// decision 3). Every variant here *except* [`EvalError::IntegerOutOfRange`]
    /// is defense-in-depth for an invariant [`super::validate::validate`] is
    /// supposed to have already enforced before evaluation runs (see this
    /// type's own doc comment) — reaching one of those means something
    /// upstream didn't hold, not that the caller supplied bad input.
    ///
    /// `IntegerOutOfRange` (issue #111) and `FloatOutOfRange` (issue #112)
    /// are data-dependent rather than engine-invariant breaches, but still
    /// report [`ErrorCode::Internal`],
    /// which ADR-0008 decision 3 designates as the catch-all "anything
    /// else". Minting a new [`ErrorCode`] variant for it would be a breaking
    /// change to a public, deliberately coarse enum, for a condition a
    /// caller can't act on any differently — the actionable detail (which
    /// column, and Postgres's own "integer out of range" wording) is already
    /// in the `Display` and in the quarantine record ADR-0003 writes.
    pub fn code(&self) -> ErrorCode {
        ErrorCode::Internal
    }

    /// The calculated-field name this failure is attributed to — every
    /// variant carries one (see each variant's `field`). ADR-0003
    /// (column-level quarantine) uses this to attribute a failure to a
    /// `(transform, column)` pair: `staging::quarantine`'s isolate-before-
    /// blaming probe knows which [`TransformDef`] it just evaluated, and
    /// pairs that with this field name rather than needing this error type
    /// (or [`evaluate`]'s signature) to carry
    /// transform identity itself.
    pub fn field(&self) -> &str {
        match self {
            EvalError::MissingColumn { field, .. }
            | EvalError::InvalidNumber { field, .. }
            | EvalError::InvalidBoolean { field, .. }
            | EvalError::IntegerOutOfRange { field, .. }
            | EvalError::FloatOutOfRange { field, .. } => field,
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
            EvalError::IntegerOutOfRange { field, source } => {
                write!(f, "calculated field '{field}': {source}")
            }
            EvalError::FloatOutOfRange { field, source } => {
                write!(f, "calculated field '{field}': {source}")
            }
            EvalError::Cycle(field) => write!(
                f,
                "calculated field '{field}' is part of a cyclic reference"
            ),
        }
    }
}

impl std::error::Error for EvalError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            EvalError::InvalidNumber { source, .. } => Some(source),
            EvalError::IntegerOutOfRange { source, .. } => Some(source),
            EvalError::FloatOutOfRange { source, .. } => Some(source),
            EvalError::MissingColumn { .. }
            | EvalError::InvalidBoolean { .. }
            | EvalError::Cycle(_) => None,
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
#[cfg(any(test, feature = "test-util"))]
pub fn evaluate(
    def: &TransformDef,
    row: &Row,
    source_columns: &HashMap<String, ValueType>,
    regex_cache: &mut RegexCache,
) -> Result<HashMap<String, Option<Value>>, EvalError> {
    evaluate_excluding(def, row, source_columns, regex_cache, &HashSet::new())
}

/// [`evaluate`], skipping every field named in `excluded` — ADR-0003's
/// column-level quarantine: a paused column must not be re-evaluated (it
/// would just reproduce the same failure that paused it forever), and its
/// target value freezes at whatever it last held rather than being
/// overwritten.
///
/// A field in `excluded` is never handed to [`eval_field`] at all — its cache
/// slot is pre-seeded `None` instead, so any *other* field that references it
/// (a same-table calculated-field alias) sees a plain absent value rather than
/// re-triggering the excluded field's own broken expression, and the excluded
/// field itself is omitted from the returned map entirely (not present as
/// `None`) so [`super::super::staging::apply::compute`]'s write plan can tell
/// "paused, don't touch this column" apart from "genuinely evaluated to
/// NULL." `excluded` is non-empty only when the caller has already looked up
/// which of this definition's columns are currently paused (ADR-0003).
///
/// `excluded` must be closed over alias readers ([`AliasReaders::close`],
/// issue #748): a field left in that reads an excluded one by alias would
/// read the `None` seeded here and be written NULL, where ADR-0003 freezes
/// it. Every caller's paused set is read through that closure.
pub fn evaluate_excluding(
    def: &TransformDef,
    row: &Row,
    source_columns: &HashMap<String, ValueType>,
    regex_cache: &mut RegexCache,
    excluded: &HashSet<String>,
) -> Result<HashMap<String, Option<Value>>, EvalError> {
    let fields_by_name: HashMap<&str, &FieldDef> =
        def.fields.iter().map(|f| (f.name.as_str(), f)).collect();

    debug_assert!(
        excluded.is_empty() || {
            let mut closed = excluded.clone();
            AliasReaders::of(def).close(&mut closed);
            closed == *excluded
        },
        "the excluded fields {excluded:?} leave out a field that reads one of them by alias"
    );
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

/// Which fields of one definition read which others by alias (issue #748):
/// for each field, the fields whose expressions name it directly.
///
/// A field `f` reads sibling `g` when `f`'s expression names `g` and `g`
/// isn't `f` itself. A field naming itself reads its source column
/// (`c AS c`), the carve-out [`eval_expr`] and `validate`'s cycle detection
/// make, and a persisted definition passed `validate`, which refuses a field
/// naming itself that isn't a source column.
///
/// ADR-0003 freezes a paused field rather than nulling it, and a field that
/// reads a paused one by alias is paused too: evaluating it would read the
/// paused field as absent ([`evaluate_excluding`]) and
/// write NULL. [`Self::close`] is how every paused-field read
/// (`staging::quarantine::paused_columns_for` and its copies) takes that in,
/// and how a field build adds the fields that read the ones it rebuilds.
#[derive(Debug, Clone, Default)]
pub struct AliasReaders {
    readers: HashMap<String, Vec<String>>,
}

impl AliasReaders {
    /// `def`'s alias readers.
    pub fn of(def: &TransformDef) -> Self {
        let names: HashSet<&str> = def.fields.iter().map(|f| f.name.as_str()).collect();
        let mut readers: HashMap<String, Vec<String>> = HashMap::new();
        let aggregates = super::validate::aggregate_field_names(
            def.fields.iter().map(|f| (f.name.as_str(), &f.expr)),
        );
        for field in &def.fields {
            // A name in an aggregate's argument that matches an aggregate
            // field is a source column, not a read of that field.
            let mut columns = Vec::new();
            super::validate::collect_reads(
                &field.expr,
                &aggregates,
                false,
                &mut columns,
                &mut Vec::new(),
            );
            for column in columns {
                if column != field.name && names.contains(column.as_str()) {
                    let entry = readers.entry(column).or_default();
                    if !entry.contains(&field.name) {
                        entry.push(field.name.clone());
                    }
                }
            }
        }
        Self { readers }
    }

    /// The fields that read `field` by alias directly, in definition order.
    pub fn direct(&self, field: &str) -> &[String] {
        self.readers.get(field).map_or(&[], Vec::as_slice)
    }

    /// Adds to `fields` every field that reads one of them by alias,
    /// directly or through other fields.
    pub fn close(&self, fields: &mut HashSet<String>) {
        if self.readers.is_empty() {
            return;
        }
        let mut queue: Vec<String> = fields.iter().cloned().collect();
        while let Some(field) = queue.pop() {
            for reader in self.direct(&field) {
                if fields.insert(reader.clone()) {
                    queue.push(reader.clone());
                }
            }
        }
    }
}

/// Reads source column `name` from `row`, parsed as the column's type.
fn read_source_column(
    field_name: &str,
    name: &str,
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
                    fields_by_name,
                    cache,
                    in_progress,
                    regex_cache,
                );
            }
            read_source_column(field_name, name, row, source_columns)
        }
        // Issue #830: a qualified name reads the source column even when a
        // field has the same name.
        Expr::SourceColumn { column, .. } => {
            read_source_column(field_name, column, row, source_columns)
        }
        Expr::NumberLiteral(text) => number_literal(field_name, text).map(Some),
        Expr::StringLiteral(text) => Ok(Some(Value::Text(text.clone()))),
        // Issue #109: a typed literal evaluates to its own family-tagged
        // value, carrying the literal source text verbatim exactly as
        // `Value::Other` does for a captured column. That is only sound
        // because `validate` has already required the text to be in the
        // family's canonical Postgres spelling — see `super::typed_literal`.
        Expr::TypedLiteral { value_type, text } => {
            typed_literal_value(*value_type, text, field_name).map(Some)
        }
        Expr::BinaryOp { op, lhs, rhs } => {
            let lhs = eval_expr(
                lhs,
                field_name,
                row,
                source_columns,
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
                fields_by_name,
                cache,
                in_progress,
                regex_cache,
            )?;
            apply_operator(*op, lhs, rhs, field_name)
        }
        Expr::FunctionCall { name, args } if name == "COALESCE" => {
            for arg in args {
                let val = eval_expr(
                    arg,
                    field_name,
                    row,
                    source_columns,
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

#[cfg(any(test, feature = "test-util"))]
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
    // Keyed by each key's target column name.
    let group_by: HashSet<&str> = group_by.iter().map(|k| k.as_str()).collect();

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

#[cfg(any(test, feature = "test-util"))]
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

#[cfg(any(test, feature = "test-util"))]
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
        // Issue #830: outside an aggregate's argument, the validator only
        // lets a qualified name read a grouping key.
        Expr::SourceColumn { column, .. } => {
            eval_row_scalar(column, field_name, &rows[0], source_columns)
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
        Expr::NumberLiteral(text) => number_literal(field_name, text).map(Some),
        Expr::StringLiteral(text) => Ok(Some(Value::Text(text.clone()))),
        // Issue #109: a typed literal evaluates to its own family-tagged
        // value, carrying the literal source text verbatim exactly as
        // `Value::Other` does for a captured column. That is only sound
        // because `validate` has already required the text to be in the
        // family's canonical Postgres spelling — see `super::typed_literal`.
        Expr::TypedLiteral { value_type, text } => {
            typed_literal_value(*value_type, text, field_name).map(Some)
        }
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
            apply_operator(*op, lhs, rhs, field_name)
        }
        Expr::FunctionCall { name, args } if name == "COUNT" && args.is_empty() => {
            // `COUNT(*)` (issue #75): counts every row in the group,
            // unconditionally — unlike `SUM`/`MIN`/`MAX`/`AVG`'s
            // `fold_aggregate`, there is no per-row argument to evaluate or
            // skip-if-NULL, so `rows.len()` is the whole computation.
            //
            // Issue #120: `bigint` (`Integer(Int8)`), matching Postgres's
            // own `pg_typeof(count(*))` — see `registry::AGGREGATE_FUNCTION_SPECS`'s
            // `COUNT` row doc comment.
            Ok(Some(Value::Integer(IntWidth::Int8, rows.len() as i64)))
        }
        // Issue #120: `COUNT(<expr>)` — counts non-null occurrences of
        // `<expr>` across the group, a different Postgres semantic from
        // `COUNT(*)` (which needs no per-row evaluation at all, above).
        // Deliberately **not** routed through `fold_aggregate`/
        // `reduce_numeric_aggregate` below: those two both implement
        // `SUM`/`MIN`/`MAX`/`AVG`'s "an aggregate of zero non-null values is
        // `NULL`" rule (`registry::aggregate_result_type(name, ..)` gates
        // which values even reach the fold, and `COUNT` is deliberately
        // absent from it — see that function's own doc comment — so every
        // value would be filtered out and the fold would wrongly return
        // `NULL`), whereas `count(x)` is **never** `NULL` — it is `0` over a
        // group whose every `x` is `NULL`, or even over zero rows (unreachable
        // here: a `GROUP BY` group always has at least one row).
        Expr::FunctionCall { name, args } if name == "COUNT" && args.len() == 1 => {
            let fields_by_name = &aggregate_argument_scope(fields_by_name);
            let mut count: i64 = 0;
            for row in rows {
                let mut per_row_cache = HashMap::new();
                let mut per_row_in_progress = HashSet::new();
                let value = eval_expr(
                    &args[0],
                    field_name,
                    row,
                    source_columns,
                    fields_by_name,
                    &mut per_row_cache,
                    &mut per_row_in_progress,
                    regex_cache,
                )?;
                if value.is_some() {
                    count += 1;
                }
            }
            Ok(Some(Value::Integer(IntWidth::Int8, count)))
        }
        Expr::FunctionCall { name, args }
            if super::registry::lookup_aggregate_function(name).is_some() =>
        {
            fold_aggregate(
                name,
                args,
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

#[cfg(any(test, feature = "test-util"))]
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

/// The fields an aggregate's argument can read by name: `fields_by_name`
/// without the aggregate fields, whose names mean source columns there (see
/// [`super::validate::aggregate_field_names`]).
#[cfg(any(test, feature = "test-util"))]
fn aggregate_argument_scope<'a>(
    fields_by_name: &HashMap<&'a str, &'a FieldDef>,
) -> HashMap<&'a str, &'a FieldDef> {
    let aggregates = super::validate::aggregate_field_names(
        fields_by_name.iter().map(|(name, f)| (*name, &f.expr)),
    );
    super::validate::without_aggregate_fields(fields_by_name, &aggregates)
}

#[cfg(any(test, feature = "test-util"))]
/// Folds `SUM`/`AVG` over `arg_expr` evaluated against every row in the
/// group, matching Postgres's NULL handling for these aggregates: a `NULL`
/// row is skipped entirely (not treated as zero), and if every row's value is
/// `NULL` the result is `NULL` too (Postgres's "aggregate of zero non-NULL
/// values is NULL" rule: `sum('{}'::numeric[])`, for instance).
///
/// `arg_expr` is evaluated per row via the ordinary single-row [`eval_expr`],
/// one row at a time so each row gets its own fresh memoization scope.
/// `fields_by_name` is threaded through so `arg_expr` can reference another
/// (non-aggregate) calculated field on this same target, e.g.
/// `GROUP BY id SELECT (id + 1) AS adj, SUM(adj) AS t`, the same way
/// Postgres itself resolves it; the validator doesn't restrict an aggregate
/// argument to source-only columns, so the evaluator must not either.
fn fold_aggregate(
    name: &str,
    args: &[Expr],
    field_name: &str,
    rows: &[Row],
    source_columns: &HashMap<String, ValueType>,
    fields_by_name: &HashMap<&str, &FieldDef>,
    regex_cache: &mut RegexCache,
) -> Result<Option<Value>, EvalError> {
    let arg_expr = &args[0];
    let fields_by_name = &aggregate_argument_scope(fields_by_name);

    let mut values: Vec<Value> = Vec::new();
    for row in rows {
        let mut per_row_cache = HashMap::new();
        let mut per_row_in_progress = HashSet::new();
        let value = eval_expr(
            arg_expr,
            field_name,
            row,
            source_columns,
            fields_by_name,
            &mut per_row_cache,
            &mut per_row_in_progress,
            regex_cache,
        )?;
        // A `NULL` (`None`) or non-exact-numeric evaluation is skipped: `NULL` per Postgres's aggregate
        // semantics above, and any other value here would mean a hand-built
        // AST bypassed the validator's type check on the argument
        // (defense-in-depth, as elsewhere in this module).
        if let Some(value) = value
            && matches!(value, Value::Integer(..) | Value::Numeric(_))
        {
            values.push(value);
        }
    }

    reduce_numeric_aggregate(name, values, field_name)
}

#[cfg(any(test, feature = "test-util"))]
/// Reduces the collected non-`NULL` exact-numeric values of `SUM`/`AVG` to
/// the aggregate's result, or `None` (SQL `NULL`) when `values` is empty:
/// Postgres's "aggregate of zero non-NULL values is NULL" rule, over a
/// GROUP BY group's rows ([`fold_aggregate`]).
///
/// The result type follows [`super::registry::aggregate_result_type`], which
/// follows Postgres:
///
/// * `SUM` over `smallint`/`integer` is Postgres's `int8` sum: it
///   accumulates in `bigint` and **raises `22003` on overflow**, exactly as
///   `sum(int4_col)` does server-side. It does *not* silently widen to
///   arbitrary precision, which is what would make Trellis and a backfill of
///   the same definition disagree.
/// * `SUM` over `bigint` is Postgres's `numeric` sum, unbounded, so it
///   cannot raise, and `AVG` over any exact-integer width is likewise
///   `numeric`. Both go through [`Numeric`].
fn reduce_numeric_aggregate(
    name: &str,
    values: Vec<Value>,
    field_name: &str,
) -> Result<Option<Value>, EvalError> {
    if values.is_empty() {
        return Ok(None);
    }

    // The integer fold applies only when **every** value is an exact
    // integer, and its width is the *widest* present, not the first value's.
    // Both details are load-bearing, and both are reachable through
    // `COALESCE`, whose evaluation picks whichever argument is non-`NULL`
    // per row rather than coercing to the field's inferred type: a
    // `SUM(COALESCE(numeric_col, 0))` yields `Numeric` on some rows and the
    // literal's `Integer` on others. Keying off `values.first()` alone would
    // silently drop every value of the other shape, or tag a `bigint`-sized
    // result as `integer`.
    let widest = values.iter().try_fold(IntWidth::Int2, |acc, v| match v {
        Value::Integer(width, _) => Some(acc.wider(*width)),
        _ => None,
    });

    if let Some(width) = widest
        && name == "SUM"
        // `sum(smallint)`/`sum(integer)` accumulate in `bigint`. The
        // `bigint` case deliberately falls through to the `numeric` path
        // below, matching `sum(bigint) -> numeric`.
        && width != IntWidth::Int8
    {
        let mut acc: i64 = 0;
        for v in &values {
            let Value::Integer(_, n) = v else { continue };
            let (next, _) = integer::checked_add(acc, IntWidth::Int8, *n, IntWidth::Int8).map_err(
                |source| EvalError::IntegerOutOfRange {
                    field: field_name.to_string(),
                    source,
                },
            )?;
            acc = next;
        }
        return Ok(Some(Value::Integer(IntWidth::Int8, acc)));
    }

    let values: Vec<Numeric> = values.into_iter().filter_map(as_numeric).collect();
    if values.is_empty() {
        return Ok(None);
    }

    let result = match name {
        "SUM" => values
            .into_iter()
            .reduce(|a, b| a.add(&b))
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
        _ => unreachable!("reduce_numeric_aggregate is only called for SUM/AVG"),
    };
    Ok(Some(Value::Numeric(result)))
}

/// `+` and `>` both take exact-numeric-family operands — [`Value::Numeric`]
/// or [`Value::Integer`] (per ADR-0004, neither gains an implicit string
/// form). An operand of any other type here would mean the validator (#23)
/// let a type-mismatched definition through, so this falls back to `None`
/// rather than a panic, matching this module's existing defense-in-depth
/// posture toward validator bugs. Postgres's own `+`/`>` on these types are
/// `STRICT` (any `NULL` operand yields `NULL`), matched by the same `None`
/// fallback.
///
/// # Mixed operands (issues #111, #112)
///
/// The dispatch mirrors [`super::registry::operator_result_type`] exactly,
/// because it must: `Integer op Integer` runs Postgres's *bounded* integer
/// operator (so `int4 + int4` can raise `22003`), while any mix with
/// `Numeric` promotes through Postgres's implicit `int -> numeric` cast and
/// runs the *unbounded* `numeric` operator (so it cannot). Anything else
/// would make the evaluator and the validator disagree about a field's
/// result type — and therefore about its target column's declared type — or
/// make Trellis and a server-side backfill of the same expression disagree
/// about whether it errors at all.
///
/// For `+`, a [`Value::Float`] operand takes priority over both, on either
/// side, because Postgres has no operator that mixes a float with an exact
/// type: it casts both operands to `double precision` and runs `float8pl`.
/// So `Float + anything` is the binary-float path, `Float + Float` is that
/// path at the wider of the two widths, and only an all-exact pair reaches
/// the integer/`numeric` arms below. `>` admits only `Float > Float` among
/// the floats, plus a float against a *bare number literal*, which Postgres
/// casts to `double precision` (a `real` operand widens to it): any other
/// float against an exact numeric is refused at define (#1418), so the
/// evaluator can't tell a literal from a column here and doesn't need to.
///
/// `>` on floats routes through [`crate::float::compare`], **not** `f64`'s
/// `PartialOrd` — Postgres's float order is total (`NaN` above everything,
/// `NaN = NaN`), and `f64::partial_cmp` returns `None` on a `NaN` operand,
/// which would silently collapse to a `false` predicate instead of the
/// answer Postgres gives (`'NaN'::float8 > 'Infinity'::float8` is `t`).
fn apply_operator(
    op: Operator,
    lhs: Option<Value>,
    rhs: Option<Value>,
    field_name: &str,
) -> Result<Option<Value>, EvalError> {
    let (Some(lhs), Some(rhs)) = (lhs, rhs) else {
        return Ok(None);
    };
    Ok(match op {
        Operator::Add => match (lhs, rhs) {
            (Value::Float(aw, a), Value::Float(bw, b)) => {
                Some(float_add(a, aw, b, bw, field_name)?)
            }
            (Value::Float(aw, a), rhs) => match as_float(rhs) {
                Some(b) => Some(float_add(a, aw, b, FloatWidth::Float8, field_name)?),
                None => None,
            },
            (lhs, Value::Float(bw, b)) => match as_float(lhs) {
                Some(a) => Some(float_add(a, FloatWidth::Float8, b, bw, field_name)?),
                None => None,
            },
            (Value::Integer(aw, a), Value::Integer(bw, b)) => {
                let (sum, width) = integer::checked_add(a, aw, b, bw).map_err(|source| {
                    EvalError::IntegerOutOfRange {
                        field: field_name.to_string(),
                        source,
                    }
                })?;
                Some(Value::Integer(width, sum))
            }
            (lhs, rhs) => match (as_numeric(lhs), as_numeric(rhs)) {
                (Some(a), Some(b)) => Some(Value::Numeric(a.add(&b))),
                _ => None,
            },
        },
        Operator::GreaterThan => match (lhs, rhs) {
            (Value::Float(_, a), Value::Float(_, b)) => Some(Value::Boolean(
                float::compare(a, b) == std::cmp::Ordering::Greater,
            )),
            // A float against a bare number literal (the only mixed pair
            // the validator admits, #1418): Postgres casts the literal to
            // `double precision` and widens a `real` operand to it, so the
            // comparison runs in `f64`. Never narrow the literal to `f32`.
            (Value::Float(_, a), rhs) => as_float(rhs)
                .map(|b| Value::Boolean(float::compare(a, b) == std::cmp::Ordering::Greater)),
            (lhs, Value::Float(_, b)) => as_float(lhs)
                .map(|a| Value::Boolean(float::compare(a, b) == std::cmp::Ordering::Greater)),
            (Value::Integer(_, a), Value::Integer(_, b)) => Some(Value::Boolean(a > b)),
            (lhs, rhs) => match (as_numeric(lhs), as_numeric(rhs)) {
                (Some(a), Some(b)) => {
                    Some(Value::Boolean(a.compare(&b) == std::cmp::Ordering::Greater))
                }
                _ => None,
            },
        },
    })
}

/// A value as a [`Numeric`], for the mixed `Integer`/`Numeric` operand case
/// Postgres resolves by implicitly casting the integer side up. `None` for
/// any value outside the exact-numeric family (defense-in-depth against a
/// validator bug, as above).
///
/// An `i64` always renders as a valid plain decimal, so the reparse can't
/// fail for a real [`Value::Integer`]; going through [`Numeric::parse`]
/// rather than a bespoke constructor keeps `Numeric`'s normalization
/// invariants in one place.
fn as_numeric(value: Value) -> Option<Numeric> {
    match value {
        Value::Numeric(n) => Some(n),
        Value::Integer(_, n) => Numeric::parse(&n.to_string()).ok(),
        _ => None,
    }
}

/// [`as_numeric`]'s float twin (issue #112): a value as an `f64`, for the
/// mixed-operand case Postgres resolves by casting **both** sides to
/// `double precision`. `None` for any value outside the numeric family
/// (defense-in-depth against a validator bug, as above).
///
/// A `numeric` goes through its decimal text, which is what Postgres's own
/// `numeric_float8` cast does (`float8in(numeric_out(n))`), so a value too
/// large for `float8` becomes `Infinity` there exactly as it does here.
fn as_float(value: Value) -> Option<f64> {
    match value {
        Value::Float(_, n) => Some(n),
        Value::Integer(_, n) => Some(n as f64),
        Value::Numeric(n) => n.to_string().parse().ok(),
        _ => None,
    }
}

/// `a + b` on binary floats, attributing an overflow to `field_name` — the
/// thin wrapper that keeps [`apply_operator`]'s four float arms from each
/// repeating the error mapping.
fn float_add(
    a: f64,
    a_width: FloatWidth,
    b: f64,
    b_width: FloatWidth,
    field_name: &str,
) -> Result<Value, EvalError> {
    let (sum, width) = float::checked_add(a, a_width, b, b_width).map_err(|source| {
        EvalError::FloatOutOfRange {
            field: field_name.to_string(),
            source,
        }
    })?;
    Ok(Value::Float(width, sum))
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

    let result: usize = match (name, texts.as_slice()) {
        ("OCTET_LENGTH", [text]) => text.len(),
        ("CHAR_LENGTH", [text]) => text.chars().count(),
        ("STRPOS", [haystack, needle]) => strpos(haystack, needle),
        ("REGEXP_COUNT", [text, pattern]) => regexp_count(text, pattern, regex_cache)?,
        _ => return None,
    };
    // All four are `integer`-returning in Postgres (issue #111, see
    // `registry::FUNCTIONS`), and all four are bounded by the length of a
    // `text` value, which Postgres itself caps at 1GB — so every result
    // here is at most ~2^30 and provably fits `int4` with no range check.
    Some(Value::Integer(IntWidth::Int4, result as i64))
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

/// The [`Value`] an [`Expr::TypedLiteral`] evaluates to.
///
/// Issue #109's allowlist held only passthrough families, so this was a
/// one-liner producing [`Value::Other`]. Issue #112 added `real`/`double
/// precision`, which are a real [`ValueType::Float`] and must decode to a
/// real `f64` — a `REAL '1.5'` carried as verbatim text would not have
/// float arithmetic, float ordering, or a float target column.
///
/// The literal's text was already checked to be in canonical form by
/// [`super::validate`] (whose float checker *is* [`crate::float::parse`]),
/// so the decode below cannot fail on any definition that went through the
/// validator; the error path is defense-in-depth for a hand-built AST, the
/// same posture the rest of this module takes.
fn typed_literal_value(
    value_type: ValueType,
    text: &str,
    field_name: &str,
) -> Result<Value, EvalError> {
    match value_type {
        ValueType::Other(pg_type) => Ok(Value::Other(pg_type, text.to_string())),
        other => parse_value(field_name, other, text),
    }
}

fn parse_value(field_name: &str, value_type: ValueType, text: &str) -> Result<Value, EvalError> {
    match value_type {
        ValueType::Numeric => parse_number(field_name, text).map(Value::Numeric),
        // Issue #111: an exact-integer column's text decodes to a real,
        // range-checked `i64` of the column's own width, not to unbounded
        // decimal digits. The range check is what turns a value that could
        // never have come out of a column of this type into a named error
        // instead of an integer that silently doesn't fit its own type.
        ValueType::Integer(width) => integer::parse(text, width)
            .map(|value| Value::Integer(width, value))
            .map_err(|source| EvalError::IntegerOutOfRange {
                field: field_name.to_string(),
                source,
            }),
        // Issue #112: likewise a float column's text decodes to a real
        // `f64` on that width's grid. `float::parse` accepts exactly what
        // `float4out`/`float8out` emit — `NaN`/`Infinity`/`-Infinity`
        // included, since those are values a `real` column really holds —
        // and nothing else, so a spelling that could not have come out of a
        // column of this type is a named error rather than a silently
        // re-rendered near-miss.
        ValueType::Float(width) => float::parse(text, width)
            .map(|value| Value::Float(width, value))
            .map_err(|source| EvalError::FloatOutOfRange {
                field: field_name.to_string(),
                source,
            }),
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

/// Types an unadorned numeric literal the way Postgres's own lexer does
/// (issue #111): a literal with no decimal point that fits `integer` *is*
/// `integer`, one that only fits `bigint` is `bigint`, and anything else —
/// a fractional literal, or an integral one too wide even for `bigint` — is
/// `numeric`.
///
/// This is why `int_col + 1` has type `integer` rather than `numeric`, and
/// therefore why it overflows exactly where Postgres overflows. Getting the
/// rule wrong in either direction would be visible: typing every literal
/// `integer` would make `numeric_col + 1` fail to resolve, and typing every
/// literal `numeric` would silently promote all integer arithmetic back to
/// arbitrary precision — the exact lie this issue removes.
///
/// [`super::validate`]'s `number_literal_type` is the type-level twin of
/// this function and must agree with it.
fn number_literal(field_name: &str, text: &str) -> Result<Value, EvalError> {
    if !text.contains('.')
        && let Ok(value) = text.parse::<i64>()
    {
        return Ok(Value::Integer(IntWidth::narrowest_for(value), value));
    }
    parse_number(field_name, text).map(Value::Numeric)
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
            without_source_index: false,
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

    fn field(name: &str, expr: Expr) -> FieldDef {
        FieldDef {
            name: name.to_string(),
            expr,
        }
    }

    fn closed(d: &TransformDef, seeds: &[&str]) -> Vec<String> {
        let mut set: HashSet<String> = seeds.iter().map(|s| s.to_string()).collect();
        AliasReaders::of(d).close(&mut set);
        let mut out: Vec<String> = set.into_iter().collect();
        out.sort();
        out
    }

    /// Issue #748: a field reading a paused one by alias, directly or
    /// through a chain, is closed into the paused set; a field naming
    /// itself reads its source column, not itself, and an unrelated field
    /// stays out.
    #[test]
    fn alias_readers_close_over_direct_and_transitive_readers_only() {
        let d = def(vec![
            field("c3", col("c3")),
            field("derived", add(col("c3"), col("price"))),
            field("chained", add(col("derived"), col("derived"))),
            field("other", col("price")),
        ]);
        assert_eq!(closed(&d, &["c3"]), ["c3", "chained", "derived"]);
        assert_eq!(closed(&d, &["derived"]), ["chained", "derived"]);
        assert_eq!(closed(&d, &["other"]), ["other"]);
        assert_eq!(AliasReaders::of(&d).direct("c3"), ["derived"]);
        assert!(AliasReaders::of(&d).direct("chained").is_empty());
    }

    /// The evaluation the closure exists for: with `c3` and its reader
    /// excluded, neither is in the result, so Apply's write plan (which drops
    /// the same set) leaves both as they were rather than writing NULL.
    #[test]
    fn evaluate_excluding_a_closed_set_omits_the_alias_reader() {
        let d = def(vec![
            field("c3", col("c3")),
            field("derived", add(col("c3"), col("price"))),
            field("other", col("price")),
        ]);
        let mut excluded: HashSet<String> = HashSet::from(["c3".to_string()]);
        AliasReaders::of(&d).close(&mut excluded);
        let out = evaluate_excluding(
            &d,
            &row(&[("c3", Some("1")), ("price", Some("2"))]),
            &numeric_types(&["c3", "price"]),
            &mut RegexCache::new(),
            &excluded,
        )
        .expect("evaluate");
        assert_eq!(out.keys().collect::<Vec<_>>(), ["other"]);
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
    /// its real [`PgType`], carrying the captured text verbatim exactly
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

    /// The text of an exact-numeric result, whichever of the two exact
    /// value shapes it landed in. Issue #111 split `integer` out of
    /// `Numeric`, and the four scalar functions below now return `integer`
    /// (as they do in Postgres) — these tests are about the *value* each
    /// computes, so they read it either way; the *type* is pinned
    /// separately by `scalar_functions_return_integer_like_postgres`.
    fn numeric_of(result: &HashMap<String, Option<Value>>, field: &str) -> String {
        match result[field].as_ref().unwrap() {
            Value::Numeric(n) => n.to_string(),
            Value::Integer(_, n) => n.to_string(),
            other => panic!("expected an exact-numeric value, got {other:?}"),
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
        // The literal wins, and issue #111 types a bare `0` as `integer`
        // (`select pg_typeof(0)`), the same as Postgres. The field's own
        // inferred type is still `numeric` — `validate::common_numeric_type`
        // unifies the `numeric` columns with the `integer` literal — and
        // the text is "0" either way, which is what the target column's
        // cast consumes.
        assert_eq!(
            result["out"].as_ref().unwrap(),
            &Value::Integer(IntWidth::Int4, 0)
        );
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
                group_by: group_by.iter().map(|s| s.to_string()).collect(),
            },
            fields,
            predicate: Predicate::True,
            without_source_index: false,
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

    /// Issue #830: `SUM(s.val)` folds the source column `val`, not the field
    /// `val` that shadows it.
    #[test]
    fn a_qualified_name_in_an_aggregate_argument_folds_the_source_column() {
        let d = crate::defs::parse(
            "TRANSFORM t FROM s GROUP BY grp SELECT grp AS grp, (grp + 1) AS val, \
             SUM(s.val) AS total",
        )
        .expect("parse");
        let rows = vec![
            row(&[("grp", Some("1")), ("val", Some("10"))]),
            row(&[("grp", Some("1")), ("val", Some("20"))]),
        ];
        let result = evaluate_aggregate(
            &d,
            &rows,
            &numeric_types(&["grp", "val"]),
            &mut RegexCache::new(),
        )
        .unwrap();
        match result["total"].as_ref().unwrap() {
            Value::Numeric(n) => assert_eq!(n.to_string(), "30"),
            other => panic!("expected Numeric, got {other:?}"),
        }
        match result["val"].as_ref().unwrap() {
            Value::Numeric(n) => assert_eq!(n.to_string(), "2"),
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
        // `COALESCE(SUM(amount), 0)` over an all-NULL group falls through
        // to the literal, which issue #111 types as `integer` (Postgres
        // types a bare `0` the same way). The *field* is still `numeric` —
        // `validate`'s `common_numeric_type` unifies the two arms — and the
        // text written is identical either way, which is what the target
        // column's `::numeric` cast consumes.
        assert_eq!(result["total"].as_ref().unwrap().to_string(), "0");
    }

    // --- bool_and/bool_or (issue #119) ---

    /// `id` stays `Numeric` (the group key), `flag` is `Boolean` — mirrors
    /// `numeric_types`, but for the one aggregate pair (`BOOL_AND`/
    /// `BOOL_OR`) whose argument isn't in the numeric family.
    fn id_and_boolean_flag_types() -> HashMap<String, ValueType> {
        HashMap::from([
            ("id".to_string(), ValueType::Numeric),
            ("flag".to_string(), ValueType::Boolean),
        ])
    }

    #[test]
    fn parse_boolean_accepts_both_the_terse_and_sql_standard_spellings() {
        // Issue #119's live finding: `boolout` (what capture images,
        // and what a bare column fetch renders) spells a boolean `'t'`/`'f'`,
        // while `<col>::text` — a *different*, dedicated Postgres cast
        // function only `boolean` has — spells it `'true'`/`'false'`. The
        // evaluator's own row-text parser must accept both, the same way
        // Postgres's own `boolin` does, or a definition fed CDC-sourced rows
        // would work while the same definition fed a live-recompute's rows
        // would not (or vice versa).
        for (text, expected) in [
            ("t", true),
            ("f", false),
            ("true", true),
            ("false", false),
            ("TRUE", true),
            ("FALSE", false),
        ] {
            assert_eq!(
                parse_boolean("flag", text).unwrap(),
                expected,
                "parse_boolean({text:?})"
            );
        }
    }

    #[test]
    fn bool_and_and_bool_or_over_an_all_null_group_are_null() {
        let d = aggregate_def(
            &["id"],
            vec![
                FieldDef {
                    name: "id".to_string(),
                    expr: col("id"),
                },
                FieldDef {
                    name: "all_true".to_string(),
                    expr: call("BOOL_AND", vec![col("flag")]),
                },
                FieldDef {
                    name: "any_true".to_string(),
                    expr: call("BOOL_OR", vec![col("flag")]),
                },
            ],
        );
        let rows = vec![
            row(&[("id", Some("1")), ("flag", None)]),
            row(&[("id", Some("1")), ("flag", None)]),
        ];
        let result = evaluate_aggregate(
            &d,
            &rows,
            &id_and_boolean_flag_types(),
            &mut RegexCache::new(),
        )
        .unwrap();
        assert_eq!(result["all_true"], None);
        assert_eq!(result["any_true"], None);
    }

    /// Issue #695: an aggregate's argument reads the source column when a
    /// field of the same name is itself an aggregate, so `AVG(val)` after
    /// `SUM(val) AS val` is the average of the rows, not of the sum.
    #[test]
    fn an_aggregate_argument_reads_the_source_column_not_a_field_of_that_name() {
        let d = crate::defs::parse(
            "TRANSFORM t FROM s GROUP BY grp SELECT grp AS grp, SUM(val) AS val, \
             COUNT(*) AS row_count, AVG(val) AS mean, COUNT(val) AS seen",
        )
        .expect("parse");
        let rows = vec![
            row(&[("grp", Some("1")), ("val", Some("5"))]),
            row(&[("grp", Some("1")), ("val", Some("7"))]),
            row(&[("grp", Some("1")), ("val", Some("2"))]),
        ];
        let result = evaluate_aggregate(
            &d,
            &rows,
            &numeric_types(&["grp", "val"]),
            &mut RegexCache::new(),
        )
        .unwrap();
        let text = |field: &str| result[field].as_ref().unwrap().to_string();
        assert_eq!(text("val"), "14");
        assert_eq!(text("row_count"), "3");
        assert_eq!(text("mean"), "4.6666666666666667");
        assert_eq!(text("seen"), "3");
    }
}
