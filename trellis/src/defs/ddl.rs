//! Target-table DDL for a 1-1 [`TransformDef`] (issue #25).
//!
//! **Neighbor-table convention**: the target table is created under the
//! literal name `def.target`, in a separately-configurable *target schema*
//! (`Config::target_schema`, issue #15) — deliberately decoupled from the
//! Trellis-managed catalog schema (`Config::schema`) everything else
//! `crate::pool` points `search_path` at, and defaulting to `public` (the
//! same default any other bare `CREATE TABLE` would land in) rather than
//! the Trellis instance schema: a POC that defaulted target tables into the
//! Trellis schema found a high chance of future name conflicts as Trellis
//! grows its own catalog/state tables there. The target table never lands
//! back onto the source table either way (`docs/data-flow.md`'s
//! "calculated columns live on a neighbor table" rule: writing them onto the
//! replicated source row would feed our own WAL into ingestion). No extra
//! prefix/suffix is added to `def.target` because `transform_definitions.target_table`
//! is already `unique` (see `V2__transform_catalog.sql`), so collisions
//! across definitions are already ruled out at the catalog layer; deriving
//! a *different* name from it would just be a second name to keep in sync
//! for no benefit. That catalog constraint is global across every target
//! schema today (not scoped per-schema) — more conservative than strictly
//! necessary now that target tables can live in different schemas, but
//! left as-is pending a decision on whether per-schema uniqueness is worth
//! the extra catalog complexity.
//!
//! The target's primary key is inherited from the source table's own primary
//! key (name and type, introspected live from `pg_catalog` — the one piece
//! of schema introspection this issue needs, distinct from the general
//! "introspect the whole source schema" question the catalog module (#23)
//! left to intake), at whatever arity the source declares it: a composite
//! (multi-column) source primary key mirrors onto the target as a real,
//! composite `primary key (...)` table constraint (issue #121) — every
//! consumer of a 1-1 target's own key (this DDL, `defs::backfill`'s
//! key-range-chunked direct build, `staging::apply`'s live CDC apply,
//! `staging::quarantine`'s recompute, `staging::self_check`'s divergence
//! detector) renders and compares it through the shared, arity-generic
//! key-contract text ([`pk_key_sql_expr`]/[`join_pk_key`]/[`split_pk_key`]),
//! the same encoding a relationship's from-side row identity and an
//! aggregate's `GROUP BY` key already used before this issue (issue #126
//! first lifted [`source_primary_key`]'s own blanket single-column
//! rejection; this issue finishes the job by removing the single-column
//! narrowing every 1-1-specific consumer still layered on top of it).
//! Every key column's Postgres type must be
//! [text-stable](super::catalog::is_text_stable_join_key_type) — the same
//! allowlist a relationship join key is held to (issue #28) — since every
//! consumer of this key compares it via `::text` casts just like a join key;
//! an unsafe type (e.g. `numeric`, `interval`) is rejected at
//! definition time rather than risking a silent missed/duplicated target row
//! later (issue #107). `bytea` and `timestamptz` used to be other examples
//! here; issue #114 found `bytea`'s `::text` rendering is in fact a
//! bijection once `bytea_output` is pinned, and issue #246 pinned
//! `timestamptz`'s `TimeZone` on the walsender the same way `DateStyle` was
//! already pinned on the pool, so both are on the allowlist now rather than
//! off it.
//!
//! Every calculated field is typed per its inferred
//! [`super::ast::ValueType`] (issue #63 widened this from a blanket
//! `numeric` to `numeric`/`text`/`boolean`, reusing
//! [`super::validate::infer_field_types`] rather than a second type-inference
//! implementation).

use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt;

use tokio_postgres::GenericClient;

use crate::error_code::{self, ErrorCode};
use crate::pool::{Pool, quote_ident};

use super::ast::{
    Expr, FieldDef, GroupByKey, KeySpace, TransformDef, ValueType, group_by_contains,
};
use super::validate::ValidationError;

/// The Postgres column type for a calculated field or grouping column of a
/// given [`ValueType`] — shared by [`create_target_table`] and
/// [`create_aggregate_target_table`] rather than duplicated. `pub(crate)`
/// so `staging::apply_aggregate` (issue #11) can render the same casts for
/// its own group-key/probe SQL without a second type-name table.
///
/// Returns [`Cow`] rather than a bare `&'static str` since issue #117: every
/// family but one renders a fixed keyword and stays `Cow::Borrowed`, exactly
/// as before that issue: [`PgType::Enum`] is the one family whose rendering
/// is a function of *which* enum type a given column is — see
/// [`PgType::sql_type_name`]'s own doc comment.
pub(crate) fn pg_type_name(value_type: ValueType) -> Cow<'static, str> {
    match value_type {
        ValueType::Numeric => Cow::Borrowed("numeric"),
        // Issue #111: a derived exact-integer column is declared with the
        // width Postgres would give the same expression, not the `numeric`
        // every integer used to collapse into.
        ValueType::Integer(width) => Cow::Borrowed(width.pg_name()),
        // Issue #112: likewise a derived float column is declared `real` or
        // `double precision`, the type Postgres gives the same expression —
        // not the `numeric` both used to collapse into.
        ValueType::Float(width) => Cow::Borrowed(width.pg_name()),
        ValueType::Text => Cow::Borrowed("text"),
        ValueType::Boolean => Cow::Borrowed("boolean"),
        ValueType::Uuid => Cow::Borrowed("uuid"),
        // Issue #108: a passthrough-only `Other` family still needs a real
        // column type when it *is* rendered into DDL/cast SQL (e.g. a
        // `SELECT jsonb_col AS jsonb_col` passthrough field) —
        // `PgType::sql_type_name` is that keyword. Note it is deliberately
        // *not* `PgType::name` (the persisted token): `Unrecognized` has no
        // real Postgres type keyword and renders as `text` here, matching
        // the pre-#108 fallthrough. See that method's doc comment.
        ValueType::Other(pg_type) => pg_type.sql_type_name(),
    }
}

/// The source column a bare passthrough/rename field reads, if `field`'s
/// expression is exactly a reference to one source column (`SELECT author AS
/// author` or `SELECT author AS foo`) — the only shape whose target column
/// type can be narrowed to the source column's *concrete* Postgres type
/// (issue #45). Returns `None` for any other expression (arithmetic,
/// aggregates, function calls, literals), whose result genuinely can't be
/// narrower than its inferred [`ValueType`], so those keep collapsing through
/// [`pg_type_name`] exactly as before.
///
/// Mirrors [`super::validate`]/[`super::eval`]'s resolution order: a reference
/// to a *different* calculated field that happens to share a source column's
/// name resolves to that field, not the source column, so it isn't treated as
/// a source-column passthrough here.
fn passthrough_source_column<'a>(
    field: &'a FieldDef,
    def: &TransformDef,
    source_columns: &HashMap<String, ValueType>,
) -> Option<&'a str> {
    let Expr::Column(name) = &field.expr else {
        return None;
    };
    if !source_columns.contains_key(name) {
        return None;
    }
    let resolves_to_other_calc_field =
        name != &field.name && def.fields.iter().any(|f| &f.name == name);
    if resolves_to_other_calc_field {
        return None;
    }
    Some(name.as_str())
}

/// The source table's primary key, as introspected from `pg_catalog`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrimaryKeyColumn {
    pub name: String,
    /// The column's Postgres type, rendered by `format_type` (e.g.
    /// `integer`, `text`, `bigint`) — safe to interpolate directly into DDL
    /// text since it comes from the catalog, not user input.
    pub data_type: String,
    /// Whether this key column can actually hold SQL `NULL` (`not
    /// pg_attribute.attnotnull`), which is what selects between this crate's
    /// two key-component encodings — see [`encode_key_part`]'s
    /// "[`nullable`](PrimaryKeyColumn::nullable) selects the encoding"
    /// section.
    ///
    /// `false` for every real `PRIMARY KEY` column (Postgres makes a primary
    /// key's columns `NOT NULL` unconditionally) and for the all-`NOT NULL`
    /// `UNIQUE` index [`source_primary_key`] also accepts. `true` only for
    /// the `UNIQUE NULLS NOT DISTINCT` grouping-column constraint
    /// [`create_aggregate_target_table`] puts on an aggregate target, whose
    /// `GROUP BY` columns are deliberately nullable — issue #110's whole
    /// subject.
    pub nullable: bool,
}

/// Why target-table DDL could not be generated or executed.
#[derive(Debug)]
pub enum DdlError {
    /// The source table has no primary key at all.
    NoPrimaryKey { source_table: String },
    /// A composite primary-key identity string (U+001F-joined, matching
    /// [`crate::intake::extract_key`]'s own composite-key encoding) split
    /// into a different number of parts than the source table's current
    /// primary key has columns — either stale staged data from before a
    /// primary-key shape change, or a real bug in whatever produced the
    /// string. Not a definition-time validation failure like this enum's
    /// other variants, but the same typed-error posture: surfaced rather
    /// than panicking on staged data this module doesn't fully control the
    /// provenance of.
    MalformedCompositeKey {
        source_table: String,
        key: String,
        expected_arity: usize,
        actual_arity: usize,
    },
    /// The source table's (single-column) primary key resolved to a Postgres
    /// type outside [`super::catalog::is_text_stable_join_key_type`]'s
    /// allowlist (issue #107). Every 1-1 apply/backfill path
    /// (`staging::apply`, `staging::backfill`) compares this primary key via
    /// `::text` casts, exactly like a relationship join key — so a
    /// non-text-stable type (`numeric`/`real`/`double precision`: `1.0` vs
    /// `1.00`; `interval`: `'1 day'` vs `'24 hours'`; `boolean`,
    /// `json`/`jsonb`; or any unknown type) would let logically-identical
    /// keys rendered two different ways silently fail to match, missing or
    /// duplicating target rows with no error. Rejected at definition time
    /// instead, mirroring
    /// [`ValidationError::RelationshipUnsupportedJoinKeyType`]'s treatment of
    /// the same class of type for relationship join keys.
    UnsupportedPrimaryKeyType {
        source_table: String,
        column: String,
        pg_type: String,
    },
    /// `def`'s calculated fields failed type inference — meaning `def`
    /// reached DDL generation without having passed [`super::validate::validate`]
    /// against this same `source_columns`, since a validated definition's
    /// fields always type-check.
    InvalidDefinition(ValidationError),
    /// A persisted relationship's stored `definition_text` failed to re-parse
    /// while resolving relationships referenced by `def` (issue #40). Only
    /// arises on stored-data corruption or cross-version parser drift, but the
    /// catalog layer surfaces it rather than panicking.
    RelationshipReparse(super::error::ParseError),
    /// Substituting an [`super::ast::KeySpace::Aggregate`] definition's
    /// cross-field-alias references (see
    /// [`super::backfill::substituted_field_exprs`]) failed — a cyclic alias
    /// chain or a pathologically large expansion. A real cycle is already
    /// rejected by [`super::validate::validate`] before DDL generation runs,
    /// so this should not be reachable for a definition that reaches this
    /// point; kept as a typed error rather than a panic, matching this
    /// module's treatment of every other "should not happen" case above.
    /// Boxed because [`super::backfill::BackfillError`] itself has a
    /// [`super::backfill::BackfillError::Ddl`] variant holding a [`DdlError`]
    /// — an unboxed cycle here would make both types infinite-sized.
    AliasSubstitution(Box<super::backfill::BackfillError>),
    /// A direct Postgres protocol/query error.
    Db(tokio_postgres::Error),
    /// Acquiring a connection from the pool failed.
    Pool(crate::error::Error),
}

impl DdlError {
    /// This error's stable, coarse [`ErrorCode`] category (`docs/decisions/0008-public-api-design.md`,
    /// decision 3). Delegates to the wrapped error's own `code()` where one
    /// nests here ([`DdlError::InvalidDefinition`], [`DdlError::AliasSubstitution`],
    /// [`DdlError::Pool`]) or to [`error_code::classify_pg_error`] for a raw
    /// Postgres error, so the mapping composes rather than re-deriving a
    /// category for an error type that already has one.
    pub fn code(&self) -> ErrorCode {
        match self {
            // The source table's shape doesn't support the 1-1 DDL slice —
            // a rejected definition, same category as any other validation
            // failure.
            DdlError::NoPrimaryKey { .. } | DdlError::UnsupportedPrimaryKeyType { .. } => {
                ErrorCode::Validation
            }
            // Staged-data corruption or a provenance bug, not a rejection of
            // the current call's input — same category as
            // `RelationshipReparse` below.
            DdlError::MalformedCompositeKey { .. } => ErrorCode::Internal,
            DdlError::InvalidDefinition(err) => err.code(),
            // Stored-data corruption or cross-version parser drift, not a
            // rejection of the current call's input.
            DdlError::RelationshipReparse(_) => ErrorCode::Internal,
            DdlError::AliasSubstitution(err) => err.code(),
            DdlError::Db(err) => error_code::classify_pg_error(err),
            DdlError::Pool(err) => err.code(),
        }
    }
}

impl fmt::Display for DdlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DdlError::NoPrimaryKey { source_table } => {
                write!(f, "source table '{source_table}' has no primary key")
            }
            DdlError::MalformedCompositeKey {
                source_table,
                key,
                expected_arity,
                actual_arity,
            } => write!(
                f,
                "source table '{source_table}' has a {expected_arity}-column primary key, but \
                 staged key '{key}' decodes to {actual_arity} part(s)"
            ),
            DdlError::UnsupportedPrimaryKeyType {
                source_table,
                column,
                pg_type,
            } => write!(
                f,
                "source table '{source_table}' has primary key {column} ({pg_type}), a type \
                 whose equality isn't text-stable, so the 1-1 apply/backfill paths (which \
                 compare primary keys as text) would silently diverge from the Postgres \
                 oracle's typed equality; supported primary key types are integer, bigint, \
                 smallint, uuid, text, and character varying"
            ),
            DdlError::InvalidDefinition(err) => {
                write!(f, "cannot generate target-table DDL: {err}")
            }
            DdlError::RelationshipReparse(err) => write!(
                f,
                "cannot generate target-table DDL: a referenced relationship's stored \
                 definition failed to re-parse: {err}"
            ),
            DdlError::AliasSubstitution(err) => write!(
                f,
                "cannot generate target-table DDL: calculated-field alias substitution \
                 error: {err}"
            ),
            DdlError::Db(err) => {
                write!(f, "target-table DDL database error: ")?;
                crate::error::write_pg_error(f, err)
            }
            DdlError::Pool(err) => write!(f, "failed to acquire a connection: {err}"),
        }
    }
}

impl std::error::Error for DdlError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            DdlError::NoPrimaryKey { .. }
            | DdlError::UnsupportedPrimaryKeyType { .. }
            | DdlError::MalformedCompositeKey { .. } => None,
            DdlError::InvalidDefinition(err) => Some(err),
            DdlError::RelationshipReparse(err) => Some(err),
            DdlError::AliasSubstitution(err) => Some(err),
            DdlError::Db(err) => Some(err),
            DdlError::Pool(err) => Some(err),
        }
    }
}

impl From<tokio_postgres::Error> for DdlError {
    fn from(err: tokio_postgres::Error) -> Self {
        DdlError::Db(err)
    }
}

impl From<crate::error::Error> for DdlError {
    fn from(err: crate::error::Error) -> Self {
        DdlError::Pool(err)
    }
}

impl From<ValidationError> for DdlError {
    fn from(err: ValidationError) -> Self {
        DdlError::InvalidDefinition(err)
    }
}

impl From<super::backfill::BackfillError> for DdlError {
    fn from(err: super::backfill::BackfillError) -> Self {
        DdlError::AliasSubstitution(Box::new(err))
    }
}

/// Collapses a [`super::catalog::CatalogError`] from relationship resolution
/// (issue #40) into a [`DdlError`]. Relationship resolution does catalog reads
/// (DB/pool errors), a `pg_catalog` column-type lookup that raises
/// `Validate(UnknownRelationshipColumn)` for a missing to-side column, and —
/// via `relationship_by_name` — a re-parse of each referenced relationship's
/// stored `definition_text`, which can raise `Parse` on stored-data
/// corruption or parser drift. The backfill/unknown-value-type variants come
/// from paths `resolve_relationships` never exercises, so those remain
/// unreachable here.
fn map_resolve_error(err: super::catalog::CatalogError) -> DdlError {
    use super::catalog::CatalogError;
    match err {
        CatalogError::Db(e) => DdlError::Db(e),
        CatalogError::Pool(e) => DdlError::Pool(e),
        CatalogError::Validate(e) => DdlError::InvalidDefinition(e),
        CatalogError::Parse(e) => DdlError::RelationshipReparse(e),
        other => unreachable!("resolve_relationships cannot produce {other:?}"),
    }
}

/// Introspects `source_table`'s primary key from `pg_catalog`, via a bound
/// `::regclass` cast (`to_regclass($1)`) rather than string-interpolating the
/// table name into the query. `to_regclass` resolves a schema-qualified
/// `"schema.table"` string exactly (issue #76, ADR-0007) — every real caller
/// now passes one (`Definition::source_table`, or
/// `catalog::resolve_source_for_install`'s equivalent at definition-acceptance
/// time), so this no longer depends on the connection's `search_path`
/// (`crate::pool`'s session bootstrap) the way it did before issue #72/#76. A
/// bare table name still resolves via `search_path` exactly as before, for
/// any caller that genuinely has nothing more specific. Rejects only a
/// *zero*-column arity (no primary key, and no qualifying unique index
/// either — [`DdlError::NoPrimaryKey`]); a composite (multi-column) primary
/// key is returned in full, in the key's own declared column order
/// (`array_position(i.indkey, a.attnum)`, the same ordinal-position
/// convention [`crate::intake::primary_key_columns`] already uses for the
/// identical reason) — issue #126 lifted this function's own blanket
/// rejection of an arity greater than one, and issue #121 removed the last
/// caller (the 1-1 target-DDL slice) that still narrowed it back down to one
/// column, so every consumer now takes the key at whatever arity it has.
/// Also gates every resolved column's *type*: only
/// [`super::catalog::is_text_stable_join_key_type`]'s allowlist is accepted,
/// returning [`DdlError::UnsupportedPrimaryKeyType`] otherwise (issue #107)
/// — every caller of this function ultimately compares the returned key via
/// `::text` casts, so an unsafe type here would risk the same silent
/// divergence a relationship join key is already guarded against.
/// Falls back to `source_table`'s own unique constraint when it has no
/// `PRIMARY KEY` (issue #128). [`create_aggregate_target_table`] keys its
/// grouping columns with a `UNIQUE NULLS NOT DISTINCT` constraint rather than
/// a `PRIMARY KEY`, precisely so a NULL grouping value is representable
/// (`PRIMARY KEY` forbids `NULL` outright) — so an aggregate target chained
/// into as another definition's source (`TRANSFORM x FROM some_aggregate`,
/// #102's own worked example) has no `indisprimary` row at all. Without this
/// fallback every such chain would regress from working to
/// [`DdlError::NoPrimaryKey`]. Ties among multiple qualifying unique indexes
/// break on `indexrelid` ascending (oldest first) for a deterministic choice;
/// partial (`indpred`) and deferred (`not indimmediate`) unique indexes are
/// excluded because either would make the index an unreliable stand-in for a
/// row identity (a partial index doesn't cover every row; a deferred one
/// doesn't guarantee uniqueness at statement end).
pub async fn source_primary_key(
    pool: &Pool,
    source_table: &str,
) -> Result<Vec<PrimaryKeyColumn>, DdlError> {
    let client = pool.get().await?;
    source_primary_key_in_txn(&**client, source_table).await
}

/// [`source_primary_key`] against a caller-supplied client instead of a fresh
/// pooled one — for callers already holding an open transaction
/// (`catalog::create_definition_inner`'s issue #177 `KeySpace::OneToOne`
/// gate). Two reasons that matters there rather than just calling
/// [`source_primary_key`]: taking a *second* pooled connection while the
/// first one is mid-transaction is the classic pool-exhaustion deadlock (N
/// concurrent definition creations against a pool of N connections would each
/// wait forever for a connection the others are holding), and reading the
/// source relation's shape on the transaction's own connection keeps this
/// check reading the same session/transaction state (`search_path`, locks,
/// anything that transaction has itself written) as every check around it,
/// instead of an unrelated session's independent view.
pub(crate) async fn source_primary_key_in_txn(
    client: &impl GenericClient,
    source_table: &str,
) -> Result<Vec<PrimaryKeyColumn>, DdlError> {
    let columns = identity_key_columns(client, source_table).await?;
    if columns.is_empty() {
        return Err(DdlError::NoPrimaryKey {
            source_table: source_table.to_string(),
        });
    }
    for column in &columns {
        // Issue #117: a live enum type joins the static allowlist above via
        // the same dynamic `to_regtype`-based check `assert_join_key_type_supported`
        // uses for the relationship join-key role — see
        // `catalog::is_enum_type_name`'s own doc comment for why admitting
        // it is safe.
        if !super::catalog::is_text_stable_join_key_type(&column.data_type)
            && !super::catalog::is_enum_type_name(client, &column.data_type).await?
        {
            return Err(DdlError::UnsupportedPrimaryKeyType {
                source_table: source_table.to_string(),
                column: column.name.clone(),
                pg_type: column.data_type.clone(),
            });
        }
    }
    Ok(columns)
}

/// The catalog half of [`source_primary_key_in_txn`], with none of its
/// definition-time policy: `table`'s row-identity key columns (its `PRIMARY
/// KEY`, else the qualifying `UNIQUE` index — an aggregate target's `UNIQUE
/// NULLS NOT DISTINCT` grouping columns, issue #128), in declared order, each
/// with its nullability. Empty when `table` has no such index; no key-type
/// gate. `table` is anything `to_regclass` resolves.
///
/// Split out for `intake::publication::enumerate_and_append` (issue #308),
/// which must enumerate every table a catch-up marker can name — aggregate
/// targets included — under exactly the key [`pk_key_sql_expr`] renders for
/// that table everywhere else. It has never type-gated the tables it
/// enumerates, and a catch-up marker is not the place to start.
pub(crate) async fn identity_key_columns(
    client: &impl GenericClient,
    table: &str,
) -> Result<Vec<PrimaryKeyColumn>, tokio_postgres::Error> {
    let rows = client
        .query(
            "with chosen_index as (
                 select i.indexrelid
                 from pg_index i
                 where i.indrelid = pg_catalog.to_regclass($1)
                   and (
                     i.indisprimary
                     or (
                       i.indisunique and i.indimmediate and i.indpred is null
                       -- A plain (nulls-distinct) UNIQUE index doesn't reject
                       -- duplicate NULLs, so it isn't a true identity unless
                       -- either NULLS NOT DISTINCT (like create_aggregate_target_table's
                       -- grouping-column constraint) or every indexed column is
                       -- NOT NULL, in which case no NULL can ever occur.
                       and (
                         i.indnullsnotdistinct
                         or not exists (
                           select 1
                           from pg_attribute a
                           where a.attrelid = i.indrelid
                             and a.attnum = any(i.indkey)
                             and not a.attnotnull
                         )
                       )
                     )
                   )
                 order by i.indisprimary desc, i.indexrelid asc
                 limit 1
             )
             select a.attname::text,
                    pg_catalog.format_type(a.atttypid, a.atttypmod),
                    not a.attnotnull
             from pg_index i
             join chosen_index c on c.indexrelid = i.indexrelid
             join pg_attribute a
               on a.attrelid = i.indrelid and a.attnum = any(i.indkey)
             where i.indrelid = pg_catalog.to_regclass($1)
             order by array_position(i.indkey, a.attnum)",
            &[&table],
        )
        .await?;
    Ok(rows
        .into_iter()
        .map(|row| PrimaryKeyColumn {
            name: row.get(0),
            data_type: row.get(1),
            nullable: row.get(2),
        })
        .collect())
}

/// The separator a composite primary-key identity string joins its column
/// values on, matching [`crate::intake::extract_key`]'s own composite-key
/// encoding exactly (see that function's doc comment, and
/// `staging::append::TRUNCATE_SENTINEL_KEY`'s, for the same U+001F choice —
/// but, unlike those whole-key sentinels, a *component's own value* is not
/// simply assumed never to contain it: Postgres `text`/`varchar` happily
/// stores U+001F, so [`join_pk_key`]/[`pk_key_sql_expr`] escape a genuine
/// occurrence in a *multi-column* key (issue #200) rather than assuming it
/// away — a single-column key needs no escape and deliberately gets none,
/// see [`push_escaped_composite_key_part`]'s "why arity 1 is exempt". See
/// [`KEY_PART_ESCAPE`] for the escape itself, and [`split_pk_key`]'s doc
/// comment for what happened pre-#200, when a real separator collided
/// unescaped: a loud [`DdlError::MalformedCompositeKey`] arity mismatch, not
/// silent corruption — still worth closing, just lower urgency than issue
/// #110's silent-NULL-collision counterpart ([`NULL_KEY_SENTINEL`]).
/// Reusing the identical separator here (not a
/// second one) is what lets a from-side row's composite key, however it
/// entered the ring — real CDC intake, or a synthetic
/// [`crate::staging::append::StagedChange::Recompute`] this crate's own
/// reverse-relationship path stages — decode identically wherever it's later
/// read back (`staging::apply::read_live_rows_batch`'s live re-fetch, most
/// notably): both producers, and every consumer, agree on one shape.
///
/// # How this composes with [`NULL_KEY_SENTINEL`]
///
/// The two escapes this module runs answer two *independent* questions, and
/// are applied as two nested layers, never as one combined branch:
///
/// 1. **"Can this column be `NULL`?"** — decided per column from the
///    catalog ([`PrimaryKeyColumn::nullable`]). A nullable column's value
///    goes through [`encode_key_part`]/[`null_key_escape_sql`] (issue #110),
///    which only ever substitutes or doubles U+0001.
/// 2. **"Does this key have a delimiter to protect?"** — decided by the
///    key's arity. At arity ≥ 2 every part (nullable or not) then goes
///    through [`push_escaped_composite_key_part`]/
///    [`composite_key_escape_sql`] (issue #200), which only ever inspects
///    and inserts U+001E/U+001F.
///
/// Layer 1 is always the inner one, layer 2 always the outer one, on both
/// the SQL and the Rust side. They cannot interfere: layer 1 neither reads
/// nor writes U+001E/U+001F, and layer 2 neither reads nor writes U+0001,
/// so layer 2's inverse ([`split_composite_key`]) recovers layer 1's output
/// byte-for-byte before [`decode_key_part`] ever looks at it. A nullable
/// column in a composite key therefore gets *both* treatments, a not-null
/// column in a composite key gets only the second, a nullable column in an
/// arity-1 key gets only the first, and a not-null arity-1 key (a real
/// single-column `PRIMARY KEY`) gets neither — which is exactly the raw
/// `col::text` the write path binds as a literal PK value.
pub(crate) const COMPOSITE_KEY_SEPARATOR: char = '\u{1f}';

/// The escape introducer [`join_pk_key`]/[`pk_key_sql_expr`] prefix onto a
/// real, in-value occurrence of [`COMPOSITE_KEY_SEPARATOR`] (or of this very
/// character) in a *multi-column* key, so [`split_pk_key`] can tell a
/// genuine field boundary apart from a column value that simply *contains*
/// U+001F (issue #200; arity 1 is exempt — see
/// [`push_escaped_composite_key_part`]). U+001E
/// (INFORMATION SEPARATOR TWO) — Postgres `text` can hold this too, so, like
/// [`COMPOSITE_KEY_SEPARATOR`], a real occurrence of *this* character is
/// escaped (by doubling it) rather than assumed absent, exactly like a real
/// [`COMPOSITE_KEY_SEPARATOR`] occurrence is escaped by prefixing it with
/// this character; see [`push_escaped_composite_key_part`].
///
/// # Why not simply double a real [`COMPOSITE_KEY_SEPARATOR`] occurrence
///
/// Issue #110's [`NULL_KEY_SENTINEL`] escape doubles a real sentinel
/// occurrence and that is provably safe *there* because a
/// [`NULL_KEY_SENTINEL`] part is checked as a whole token against exactly
/// one already-delimited field (delimited by the *different* character
/// [`COMPOSITE_KEY_SEPARATOR`]) — doubling can never be confused with a
/// field boundary because the sentinel and the delimiter are different
/// characters.
///
/// [`COMPOSITE_KEY_SEPARATOR`] is itself the delimiter, so doubling *it*
/// directly is genuinely ambiguous, not merely more complex: consider
/// joining `"x\u{1f}"` and `"y"` (arity 2) versus joining `"x"` and
/// `"\u{1f}y"` (arity 2). Naive doubling would render both as
/// `"x\u{1f}\u{1f}\u{1f}y"` — the same three-separator run either way — and
/// a decoder given only that run has no way to tell whether the *first* pair
/// is the escaped literal (leaving the third as the real delimiter) or the
/// *second* pair is (leaving the first as the real delimiter): both
/// segmentations are locally consistent with "pairs are escapes, singles are
/// delimiters." The ambiguity is inherent to using one character as both the
/// delimiter and its own escape target in a multi-field join, not a
/// property of any particular decoder implementation.
///
/// Introducing a second, distinct escape character avoids this: a bare
/// [`COMPOSITE_KEY_SEPARATOR`] in the encoded text is *always* a real field
/// boundary (a genuine one is only ever emitted prefixed by
/// [`KEY_PART_ESCAPE`], never bare), and [`KEY_PART_ESCAPE`] itself is
/// escaped by the same rule, so every occurrence of it in the encoded text
/// is unambiguously the first character of a two-character escape pair. See
/// [`split_composite_key`] for the decoder this makes possible with no
/// lookahead ambiguity.
const KEY_PART_ESCAPE: char = '\u{1e}';

/// Escapes `part` onto `out` (appending, not overwriting) the way every
/// producer of this crate's composite key text must: a real
/// [`COMPOSITE_KEY_SEPARATOR`] or [`KEY_PART_ESCAPE`] character is prefixed
/// with [`KEY_PART_ESCAPE`], everything else copied through unchanged. Used
/// by [`join_pk_key`] for a *multi-part* key only (issue #200).
///
/// This is the **outer** of the two escape layers described in
/// [`COMPOSITE_KEY_SEPARATOR`]'s "how this composes with
/// [`NULL_KEY_SENTINEL`]" section: `part` is whatever the caller already
/// rendered for that column — for a nullable column, that means
/// [`encode_key_part`]'s output (which may be the bare
/// [`NULL_KEY_SENTINEL`]), and for a not-null column the raw value. Since
/// this pass only ever inspects U+001E/U+001F and [`encode_key_part`] only
/// ever emits U+0001 substitutions, the two are order-independent in effect
/// but strictly nested in application: null-encode first, escape second,
/// unescape first, null-decode second.
///
/// # Why arity 1 is exempt
///
/// An arity-1 encoded key is not merely an identity string: it doubles as a
/// real primary-key *value* everywhere this crate writes one.
/// `staging::apply::apply_target` binds the staged `key` text straight into
/// the target table's own PK column (`$n::text::{pk_cast}`), and the
/// delete path filters on it the same way; `defs::backfill`'s initial build
/// of that same target inserts the source's PK column *raw*
/// (`insert into target (pk, …) select pk, …`), never through any encoder;
/// `staging::quarantine::recompute_column` re-reads the source's
/// `pk::text` raw and updates the target by it. Escaping at arity 1 would
/// silently desynchronize those: for a PK value that genuinely contains
/// U+001F, backfill would write the raw value and the incremental apply an
/// escaped one — two rows for one source row, and a delete that matches
/// neither. That is exactly the silent-corruption failure mode
/// [`encode_key_part`]'s own "[`PrimaryKeyColumn::nullable`] selects the
/// encoding" section describes for issue #110's escape, and it would be
/// *introduced*, not closed, by escaping here.
///
/// It is also unnecessary: an arity-1 key has no delimiter to protect, and
/// [`split_pk_key`] knows the target arity, so it returns a single-column
/// key whole rather than splitting it. That, not escaping, is what closes
/// issue #200's single-column half (a value containing U+001F used to reach
/// a loud [`DdlError::MalformedCompositeKey`] arity mismatch, even with no
/// composite key involved at all). Escaping is only needed where a real
/// delimiter is actually emitted — arity ≥ 2.
///
/// Note this exemption is *only* about this separator/escape layer. Issue
/// #110's [`NULL_KEY_SENTINEL`] layer has no arity exemption at all: a
/// nullable arity-1 key (a single-column aggregate `GROUP BY`) still gets
/// the full [`encode_key_part`] treatment, because a nullable column is
/// never a 1-1 target's primary key in the first place. The two decisions
/// are independent — see [`COMPOSITE_KEY_SEPARATOR`]'s composition section.
fn push_escaped_composite_key_part(out: &mut String, part: &str) {
    for ch in part.chars() {
        if ch == COMPOSITE_KEY_SEPARATOR || ch == KEY_PART_ESCAPE {
            out.push(KEY_PART_ESCAPE);
        }
        out.push(ch);
    }
}

/// [`push_escaped_composite_key_part`]'s exact SQL twin, wrapping one
/// column-text expression — [`pk_key_sql_expr`] renders one per key column
/// of a multi-column key (and none at all at arity 1, see
/// [`push_escaped_composite_key_part`]'s "why arity 1 is exempt"), *outside*
/// [`null_key_escape_sql`] where that applies.
/// Two sequential `replace()` calls,
/// in this order: first double a real [`KEY_PART_ESCAPE`] (`chr(30)`), then
/// prefix a real [`COMPOSITE_KEY_SEPARATOR`] (`chr(31)`) with
/// [`KEY_PART_ESCAPE`]. The order matters — reversing it would re-escape the
/// [`KEY_PART_ESCAPE`] characters the second step just inserted — but doing
/// it in *this* order is safe: the first `replace()` only touches
/// `chr(30)` occurrences, so it can't introduce or remove any `chr(31)`
/// for the second `replace()` to react to, and the second `replace()` only
/// touches `chr(31)` occurrences (all of them genuine, since the first step
/// never produced one), so it can't disturb the `chr(30)` pairs the first
/// step already produced. The result is byte-identical, for every input, to
/// [`push_escaped_composite_key_part`]'s single left-to-right pass.
///
/// Neither `replace()` here touches `chr(1)`, so wrapping this *around* a
/// [`null_key_escape_sql`] expression (what [`pk_key_sql_expr`] does for a
/// nullable column of a multi-column key) leaves that inner layer's output
/// recoverable exactly — and, because the inner `coalesce` has already
/// turned a SQL `NULL` into a real `chr(1)` string, this wrapper never sees
/// `NULL` and so can never re-introduce the `array_to_string` element-drop
/// issue #110 closed.
pub(crate) fn composite_key_escape_sql(col_text: &str) -> String {
    format!(
        "replace(replace({col_text}, chr(30), chr(30) || chr(30)), chr(31), chr(30) || chr(31))"
    )
}

/// The per-part text substituted for a `NULL` key/group component wherever
/// this crate's shared key contract renders one — the fix for issue #110's
/// NULL-lossiness axis: before this, a `NULL` component rendered as an empty
/// string (`unwrap_or_default()`/a bare `col::text`, which is SQL `NULL` and
/// gets dropped by `array_to_string`), indistinguishable from a genuine `''`
/// value, so a `NULL`-keyed group's identity couldn't round-trip through a
/// chained definition's live re-fetch (`staging::apply::read_live_rows_batch`)
/// — it looked exactly like "no row," which is mistaken for a delete.
///
/// U+0001 (SOH) is used — but, unlike
/// `staging::append::TRUNCATE_SENTINEL_KEY`/[`COMPOSITE_KEY_SEPARATOR`],
/// **not** on a bare "no real column value contains this control character"
/// assumption. A `text`/`varchar` column can perfectly well hold a U+0001,
/// and a key component that *was* exactly this sentinel would otherwise be
/// read back as a `NULL` component — folding two genuinely distinct groups
/// onto one encoded key, which is a strictly worse (silent, data-dependent)
/// failure than the loud `DdlError::MalformedCompositeKey` an embedded
/// U+001F produces. [`encode_key_part`] therefore *escapes* a real U+0001 by
/// doubling it, so an encoded non-`NULL` part only ever contains U+0001 in
/// even-length runs and can never equal this odd-length-1 sentinel. See that
/// function for the round-trip argument.
///
/// Deliberately *not* U+0000
/// (NUL), which would otherwise be the more obviously-unambiguous choice
/// (`TRUNCATE_SENTINEL_KEY`'s doc comment: Postgres `text` is effectively
/// cstring-based internally and can never store an embedded NUL at all,
/// unlike an ordinary control character, which it merely happens not to see
/// in practice). Postgres's own `chr()` builtin unconditionally refuses to
/// *construct* `chr(0)` in the first place (`ERROR: null character not
/// permitted`, raised from `oracle_compat.c`) — confirmed the hard way, as a
/// live-database `SQLSTATE 54000` failure every drain-touching integration
/// test hit the first time this constant used U+0000 — which would make it
/// impossible to express identically on the SQL side
/// ([`pk_key_sql_expr`]'s [`null_key_escape_sql`] twin) even though
/// the underlying storage guarantee is stronger. U+0001 has no such
/// restriction and is otherwise unused anywhere else in this crate's key
/// contract.
///
/// Every producer of this crate's composite/single key text must route
/// every component of a **nullable** key column — `NULL` or not — through
/// [`encode_key_part`] (Rust side) or the equivalent
/// [`null_key_escape_sql`] (SQL side, baked into [`pk_key_sql_expr`]) before
/// joining it in, and every consumer must decode it back through
/// [`decode_key_part`] (used by [`split_pk_key`]) — so a `NULL` component
/// agrees byte-for-byte on both sides of the wire, the same "one shape,
/// every producer and consumer" property [`COMPOSITE_KEY_SEPARATOR`]'s doc
/// comment describes for the separator. "Every component of that column, not
/// just the `NULL` ones" is what makes the escape above actually hold: a
/// producer that skipped the encode for its non-`NULL` parts would emit a
/// *different* text than one that did, for any value containing a U+0001.
///
/// A `NOT NULL` key column takes the *other* encoding — its raw `col::text`,
/// unchanged — for the reason spelled out in [`encode_key_part`]'s
/// "[`PrimaryKeyColumn::nullable`] selects the encoding" section. Which of
/// the two a given column gets is decided per column, from the catalog, by
/// [`PrimaryKeyColumn::nullable`]; it is never decided by the key's arity.
pub(crate) const NULL_KEY_SENTINEL: &str = "\u{1}";

/// What [`encode_key_part`] rewrites a genuine, in-value U+0001 to: the
/// sentinel doubled. See [`NULL_KEY_SENTINEL`] and [`encode_key_part`].
const NULL_KEY_SENTINEL_ESCAPED: &str = "\u{1}\u{1}";

/// [`encode_key_part`]'s exact SQL twin, wrapping one column-text expression
/// (`<col>::text`) — [`pk_key_sql_expr`] renders one per key column.
/// `replace(...)` performs the same doubling escape and the outer `coalesce`
/// the same `NULL` -> sentinel substitution, in that order: `replace(NULL,
/// ...)` is itself `NULL`, so the `coalesce` still sees a `NULL` input as
/// `NULL`, and a real value's escape happens before it could ever be confused
/// with the substituted one.
pub(crate) fn null_key_escape_sql(col_text: &str) -> String {
    format!("coalesce(replace({col_text}, chr(1), chr(1) || chr(1)), chr(1))")
}

/// The Rust-side counterpart of [`null_key_escape_sql`]: renders one
/// possibly-`NULL` key/group component as the text a producer should feed
/// into [`join_pk_key`].
///
/// - `None` (a SQL `NULL` component) renders as [`NULL_KEY_SENTINEL`] — a
///   *single* U+0001.
/// - `Some(v)` renders `v` with every U+0001 doubled
///   ([`NULL_KEY_SENTINEL_ESCAPED`]), and is otherwise `v` unchanged (the
///   borrowed, zero-allocation case every real value takes).
///
/// That makes the two cases provably disjoint rather than merely
/// improbably-disjoint, which is the whole point: doubling maps a run of `n`
/// U+0001s to a run of `2n`, so every U+0001 run in an encoded `Some` part
/// has *even* length, while the encoded `None` part is a run of length one.
/// No `Some(v)` can therefore ever encode to the sentinel, and
/// [`decode_key_part`] recovers `v` exactly. Without this escape a group
/// whose key column genuinely held a lone U+0001 would silently share one
/// encoded key with the `NULL` group — two distinct groups folded into one,
/// a silent-corruption bug strictly worse than issue #110's own.
///
/// # [`PrimaryKeyColumn::nullable`] selects the encoding
///
/// This encoding is applied to a key component **if and only if that
/// component's key column is nullable** ([`PrimaryKeyColumn::nullable`]) —
/// in practice, only an aggregate target's `UNIQUE NULLS NOT DISTINCT`
/// `GROUP BY` columns ([`create_aggregate_target_table`]) and the Rust-side
/// group key `staging::apply_aggregate::derive_group_key` builds for them. A
/// `NOT NULL` key column — every real `PRIMARY KEY`, and the all-`NOT NULL`
/// `UNIQUE` index [`source_primary_key`] also accepts — keeps its raw
/// `col::text` instead, with no substitution and no escape.
///
/// That is not an optimization, it is a correctness requirement, and it is
/// specifically *not* an arity-1 exemption (a single-column nullable `GROUP
/// BY` is exactly issue #110's original bug and still gets the full
/// treatment here). The reason is that a not-null primary key's encoded text
/// does not stay internal to the ring: for a 1-1 definition,
/// `staging::apply::apply_target` binds the staged key text *directly* as
/// the literal stored value of the target table's own primary-key column
/// (`$n::text::{pk_cast}`), while three other writers/readers of that very
/// column use the source's raw value instead —
/// `defs::backfill`'s `insert into {target} select {pk} from {source}`,
/// `staging::quarantine::recompute_column`'s `where {pk}::text = $2`
/// write-back, and `defs::oracle::recompute`'s `{pk}::text`. An encoding
/// that changes a real value's text therefore makes those paths disagree,
/// duplicating or orphaning the target row for exactly the values it
/// rewrites — empirically reproduced for a `text` primary key holding a
/// literal U+0001 (see `intake::tests`'
/// `extract_key_keeps_a_control_character_in_a_key_value_verbatim` and
/// `trellis/tests/one_to_one_control_char_pk.rs`). Since a not-null key can
/// never *need* the `NULL` substitution, the only safe encoding for it is
/// the identity one.
pub(crate) fn encode_key_part(value: Option<&str>) -> Cow<'_, str> {
    match value {
        None => Cow::Borrowed(NULL_KEY_SENTINEL),
        Some(v) if v.contains(NULL_KEY_SENTINEL) => {
            Cow::Owned(v.replace(NULL_KEY_SENTINEL, NULL_KEY_SENTINEL_ESCAPED))
        }
        Some(v) => Cow::Borrowed(v),
    }
}

/// [`encode_key_part`]'s exact inverse — the read-side counterpart
/// [`split_pk_key`] applies to each of a decoded key's parts: `None` when the
/// part is exactly [`NULL_KEY_SENTINEL`] (which, per that function's doc
/// comment, no encoded non-`NULL` value can be), and otherwise the part with
/// each doubled U+0001 collapsed back to one.
pub(crate) fn decode_key_part(part: &str) -> Option<Cow<'_, str>> {
    if part == NULL_KEY_SENTINEL {
        None
    } else if part.contains(NULL_KEY_SENTINEL) {
        Some(Cow::Owned(
            part.replace(NULL_KEY_SENTINEL_ESCAPED, NULL_KEY_SENTINEL),
        ))
    } else {
        Some(Cow::Borrowed(part))
    }
}

/// The SQL expression computing one row's composite primary-key identity
/// text, from `pk`'s columns (in the key's own declared order) — the
/// multi-column generalization of a bare `{pk}::text`. A single-column key
/// renders with no separator (nothing to join) and, deliberately, no
/// [`composite_key_escape_sql`] either — see
/// [`push_escaped_composite_key_part`]'s "why arity 1 is exempt"; for a
/// *not-null* single column that makes it byte-identical to the bare
/// `{pk}::text` form the write path binds as a real PK value, while a
/// *nullable* single column still gets its [`null_key_escape_sql`] layer
/// (issue #110 has no arity exemption). A multi-arity key renders
/// `array_to_string(array[<escaped col0>, <escaped col1>, ...], chr(31))`,
/// matching
/// [`COMPOSITE_KEY_SEPARATOR`] via `chr(31)` (U+001F's code point) since SQL
/// has no literal syntax for an unprintable control character that survives
/// every driver/encoding path as reliably as the numeric `chr()` form.
/// `alias`, when given, qualifies every column reference (`{alias}."{col}"`
/// — a bare, unquoted alias, since every real caller passes a fixed SQL
/// alias literal like `"t"`, never user input) — needed once the surrounding
/// query joins in a second relation, so an
/// unqualified column name can't become ambiguous.
///
/// # The declared-order convention (issue #163)
///
/// "In the key's own declared order" above is the whole codebase's single
/// convention for *which* order a composite key's parts appear in, and it is
/// load-bearing: a key encoded in one order and decoded in another silently
/// looks up the wrong row (or, if the parts' types differ, fails the batch).
/// It means the order the `PRIMARY KEY (...)`/`UNIQUE (...)` constraint
/// itself lists its columns in — `pg_index.indkey`'s own order, which is what
/// [`source_primary_key`] sorts by (`array_position(i.indkey, a.attnum)`) and
/// therefore the order of the `pk` slice every function here receives. It is
/// deliberately *not* the table's physical column-declaration order
/// (`attnum`), which Postgres allows to differ:
///
/// ```sql
/// create table t (tag text, post int, primary key (post, tag));
/// -- declared key order: (post, tag); physical order: (tag, post)
/// ```
///
/// Every producer and consumer must agree on this one order:
/// [`pk_key_sql_expr`] and [`join_pk_key`] (producers),
/// [`split_pk_key`]/[`transpose_pk_keys`] (consumers), and
/// [`crate::intake::extract_key`] (a producer that reaches the key through
/// `pgoutput`'s physical column order and so has to normalize back onto this
/// one — see its own doc comment).
pub(crate) fn pk_key_sql_expr(pk: &[PrimaryKeyColumn], alias: Option<&str>) -> String {
    // The *outer* escape layer (issue #200) runs only where a real delimiter
    // is actually emitted. Decided once, from the key's arity, and applied
    // uniformly to every column — independently of each column's own
    // nullability, which decides the *inner* layer below. See
    // `COMPOSITE_KEY_SEPARATOR`'s "how this composes with NULL_KEY_SENTINEL".
    let composite = pk.len() > 1;
    let parts: Vec<String> = pk
        .iter()
        .map(|c| {
            let ident = quote_ident(&c.name);
            let col_text = match alias {
                Some(a) => format!("{a}.{ident}::text"),
                None => format!("{ident}::text"),
            };
            // A NOT NULL key column (every real PRIMARY KEY) renders as its
            // raw `col::text`, exactly as it did before issue #110 — it can
            // never produce the NULL component the encoding below exists
            // for, and its encoded text is not private to the ring: a 1-1
            // definition's `staging::apply::apply_target` binds it straight
            // in as the *stored value* of the target's own primary-key
            // column, where `defs::backfill`/`staging::quarantine`/
            // `defs::oracle` all use the raw source value. See
            // `encode_key_part`'s "`PrimaryKeyColumn::nullable` selects the
            // encoding" section; `encode_key_part` is this branch's Rust
            // twin (its callers skip it outright for a not-null key).
            if !c.nullable {
                return if composite {
                    composite_key_escape_sql(&col_text)
                } else {
                    col_text
                };
            }
            // Issue #110: a NULL component is coalesced to `chr(1)`
            // (`NULL_KEY_SENTINEL`; deliberately not `chr(0)` — Postgres's
            // `chr()` itself refuses to construct a NUL byte, see that
            // constant's doc comment) rather than left as SQL `NULL`, so it
            // survives `array_to_string` (which otherwise drops a NULL
            // array element outright, collapsing a composite key's arity)
            // and stays distinguishable from a genuine empty string, both
            // at every arity — and a *real* chr(1) inside the value is
            // doubled so it can never be read back as that substitution.
            // `encode_key_part` is this expression's exact Rust-side
            // counterpart, which every producer of this crate's *other* half
            // of this same key (`join_pk_key`'s callers) must use so the two
            // sides render byte-identical text for the same value.
            //
            // Issue #200's separator escape then wraps *around* that, never
            // inside it: `composite_key_escape_sql` only touches
            // chr(30)/chr(31) and the `coalesce` has already replaced SQL
            // NULL with a real chr(1) string, so neither layer can disturb
            // the other and `split_pk_key` can undo them in the mirror
            // order (unescape, then `decode_key_part`).
            let encoded = null_key_escape_sql(&col_text);
            if composite {
                composite_key_escape_sql(&encoded)
            } else {
                encoded
            }
        })
        .collect();
    match parts.len() {
        // Arity 1: no separator, and no issue-#200 escape (already skipped
        // above) — for a not-null column this is the bare `{pk}::text` whose
        // result doubles as a real primary-key *value* on the write side
        // (`staging::apply::apply_target` binds it into the target's own PK
        // column, and `defs::backfill` inserts that column straight across),
        // so it must equal the column's own text exactly.
        1 => parts.into_iter().next().unwrap(),
        _ => format!("array_to_string(array[{}], chr(31))", parts.join(", ")),
    }
}

/// The Rust-side counterpart of [`pk_key_sql_expr`]: joins already-rendered
/// per-column text values into one composite primary-key identity string, in
/// the key's own declared column order (the caller's responsibility — see
/// [`split_pk_key`]'s doc comment for that convention), and the exact inverse
/// of [`split_pk_key`]. A single part renders as itself, verbatim —
/// byte-identical to what [`pk_key_sql_expr`] renders at arity 1, escape and
/// all (i.e. none): see [`push_escaped_composite_key_part`]'s "why arity 1
/// is exempt" section. Only a genuinely multi-part key escapes, because only
/// a multi-part key has a delimiter to protect.
///
/// The `parts` handed in are already whatever their own columns render —
/// in particular, a nullable column's part must already have been through
/// [`encode_key_part`] (issue #110) by the caller, since only the caller
/// knows each column's nullability. This function applies issue #200's
/// separator escape *on top of* that, the same nesting
/// [`pk_key_sql_expr`] uses on the SQL side.
///
/// Every producer of an encoded key this crate has goes through either this
/// or [`pk_key_sql_expr`] (whichever side of the wire it's on):
/// [`crate::intake::extract_key`] for a row arriving over real CDC,
/// `staging::apply_aggregate::derive_group_key` for an aggregate group's
/// downstream-propagated identity (issue #171), and the SQL form for
/// everything computed in the database — including
/// `intake::publication::enumerate_and_append`'s backfill enumeration, which
/// selects its keys through [`pk_key_sql_expr`] (issue #308). Keeping them one function each —
/// rather than a hand-rolled `join` per site — is what makes the
/// "producers and consumers agree on one shape" claim in
/// [`COMPOSITE_KEY_SEPARATOR`]'s doc comment checkable by grep.
pub(crate) fn join_pk_key<S: AsRef<str>>(parts: impl IntoIterator<Item = S>) -> String {
    let parts: Vec<S> = parts.into_iter().collect();
    // Arity 1: verbatim, no escape — see
    // `push_escaped_composite_key_part`'s "why arity 1 is exempt".
    if parts.len() == 1 {
        return parts[0].as_ref().to_string();
    }
    let mut out = String::new();
    for (i, part) in parts.iter().enumerate() {
        if i > 0 {
            out.push(COMPOSITE_KEY_SEPARATOR);
        }
        push_escaped_composite_key_part(&mut out, part.as_ref());
    }
    out
}

/// Splits `key` into its raw (still-escaped) [`COMPOSITE_KEY_SEPARATOR`]-
/// delimited fields, then un-escapes each one — the exact inverse of
/// [`push_escaped_composite_key_part`] applied across the whole joined
/// string. See [`KEY_PART_ESCAPE`]'s doc comment for why this needs a
/// dedicated scan rather than a bare `str::split`: a bare split would
/// misparse a field whose own value contains a real, escaped
/// [`COMPOSITE_KEY_SEPARATOR`].
///
/// What comes back is each column's *pre-issue-#200* text — i.e. still
/// [`encode_key_part`]-encoded for a nullable column. [`split_pk_key`] runs
/// [`decode_key_part`] over these, undoing the two layers in the mirror of
/// the order [`join_pk_key`]/[`pk_key_sql_expr`] applied them.
///
/// Fast path: a `key` containing no [`KEY_PART_ESCAPE`] at all can only have
/// been produced from parts that themselves contained no
/// [`COMPOSITE_KEY_SEPARATOR`]/[`KEY_PART_ESCAPE`] (any real occurrence
/// would have forced [`push_escaped_composite_key_part`] to insert one) —
/// so every remaining [`COMPOSITE_KEY_SEPARATOR`] in `key` is a genuine
/// field boundary, and a plain `str::split` is both correct and
/// allocation-free (borrowing straight out of `key`). This covers every
/// value in practice.
fn split_composite_key(key: &str) -> Vec<Cow<'_, str>> {
    if !key.contains(KEY_PART_ESCAPE) {
        return key
            .split(COMPOSITE_KEY_SEPARATOR)
            .map(Cow::Borrowed)
            .collect();
    }
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut chars = key.chars();
    while let Some(ch) = chars.next() {
        match ch {
            KEY_PART_ESCAPE => {
                // By construction (`push_escaped_composite_key_part`), a
                // `KEY_PART_ESCAPE` is always immediately followed by the
                // one real character it escaped — a lone trailing escape
                // (`chars.next()` returning `None`) can't come from any key
                // this crate itself produced, but is handled by simply
                // dropping it rather than panicking on data this module
                // doesn't fully control the provenance of (matching
                // `DdlError::MalformedCompositeKey`'s own "stale staged
                // data" posture).
                if let Some(escaped) = chars.next() {
                    current.push(escaped);
                }
            }
            COMPOSITE_KEY_SEPARATOR => parts.push(Cow::Owned(std::mem::take(&mut current))),
            other => current.push(other),
        }
    }
    parts.push(Cow::Owned(current));
    parts
}

/// Splits a composite primary-key identity string (built by
/// [`pk_key_sql_expr`]/[`join_pk_key`], or by [`crate::intake::extract_key`]
/// for a row that
/// arrived via real CDC) back into its per-column parts, in the same
/// declared order — the read-side counterpart used once a set of already-
/// identified rows' keys need to be matched back against `pk`'s live
/// columns (`staging::apply::read_live_rows_batch`). Returns
/// [`DdlError::MalformedCompositeKey`] if `key` doesn't split into exactly
/// `pk.len()` parts, rather than silently truncating/padding — "the same
/// declared order" here is [`pk_key_sql_expr`]'s documented declared-order
/// convention (issue #163), not the table's physical column order; see that
/// variant's doc comment for when this can genuinely happen. Pre-issue-#200,
/// a genuine, unescaped [`COMPOSITE_KEY_SEPARATOR`] inside one column's own
/// value could *also* reach this arity mismatch, at any arity, including a
/// single-column one. Both halves are closed here, by arity:
/// - **arity 1**: `key` *is* the column's own (possibly
///   [`encode_key_part`]-encoded) text, never separator-escaped (see
///   [`push_escaped_composite_key_part`]'s "why arity 1 is exempt"), so it
///   is taken whole, unsplit — a separator inside it is just data;
/// - **arity ≥ 2**: [`split_composite_key`] splits only on a *bare*
///   separator, and an escaped occurrence never contributes a field
///   boundary, so only a real join produces one.
///
/// Each returned part is then run through [`decode_key_part`] (issue #110): a
/// part that is exactly [`NULL_KEY_SENTINEL`] decodes to `None` (a real
/// `NULL` component), not the literal sentinel text, so a caller binding
/// these back against the live columns (`staging::apply::read_live_rows_batch`)
/// can bind a genuine SQL `NULL` — and, critically, tell that column apart
/// from one that merely holds an empty string. That decode is the *inner*
/// layer, undone after [`split_composite_key`]'s outer one — see
/// [`COMPOSITE_KEY_SEPARATOR`]'s composition section.
pub(crate) fn split_pk_key<'a>(
    pk: &[PrimaryKeyColumn],
    source_table: &str,
    key: &'a str,
) -> Result<Vec<Option<Cow<'a, str>>>, DdlError> {
    // Arity 1 carries no separator and no issue-#200 escape, so the whole
    // key is that one column's encoded text — splitting it would misread a
    // separator the column genuinely contains.
    let parts: Vec<Cow<'a, str>> = if pk.len() == 1 {
        vec![Cow::Borrowed(key)]
    } else {
        let parts = split_composite_key(key);
        if parts.len() != pk.len() {
            return Err(DdlError::MalformedCompositeKey {
                source_table: source_table.to_string(),
                key: key.to_string(),
                expected_arity: pk.len(),
                actual_arity: parts.len(),
            });
        }
        parts
    };
    // Per column, the exact inverse of what `pk_key_sql_expr`/
    // `encode_key_part` produced for it: a nullable column's part is
    // decoded, a NOT NULL column's part is its own raw text (which is all
    // that was ever encoded for it). See `encode_key_part`'s
    // "`PrimaryKeyColumn::nullable` selects the encoding" section.
    Ok(parts
        .into_iter()
        .zip(pk)
        .map(|(part, column)| {
            if !column.nullable {
                return Some(part);
            }
            match part {
                // Borrowed straight out of `key`, so `decode_key_part` can
                // keep borrowing from `key` for the (overwhelmingly common)
                // no-sentinel case.
                Cow::Borrowed(s) => decode_key_part(s),
                // Already owned by `split_composite_key`'s un-escaping
                // pass, so the decode has to own its result too.
                Cow::Owned(s) => decode_key_part(&s).map(|d| Cow::Owned(d.into_owned())),
            }
        })
        .collect())
}

/// [`split_pk_key`], applied to a whole batch of keys and transposed so
/// column `j`'s array holds every key's `j`th part — the shape a batched,
/// parameterized `unnest(...)` match needs (one bind array per `pk` column,
/// regardless of how many keys the batch carries), mirroring
/// `staging::apply_aggregate`'s `keyset_unnest`/`transpose_group_values`
/// pair for the identical reason. A `None` part (issue #110: a `NULL` key
/// component, decoded by [`split_pk_key`]) binds as a genuine SQL `NULL` in
/// its column's array, not [`NULL_KEY_SENTINEL`]'s literal text.
pub(crate) fn transpose_pk_keys<'a>(
    pk: &[PrimaryKeyColumn],
    source_table: &str,
    keys: &[&'a str],
) -> Result<Vec<Vec<Option<Cow<'a, str>>>>, DdlError> {
    let mut columns: Vec<Vec<Option<Cow<'a, str>>>> = (0..pk.len())
        .map(|_| Vec::with_capacity(keys.len()))
        .collect();
    for &key in keys {
        let parts = split_pk_key(pk, source_table, key)?;
        for (column, part) in columns.iter_mut().zip(parts) {
            column.push(part);
        }
    }
    Ok(columns)
}

/// Every column of `source_table`, mapped to its *concrete* Postgres type as
/// rendered by `format_type` (e.g. `integer`, `bigint`, `character
/// varying(255)`) — the same `pg_catalog` introspection [`source_primary_key`]
/// does for the primary key, widened to every column. Used by
/// [`create_target_table`] to give a bare source-column passthrough field its
/// source column's exact type rather than collapsing it through [`ValueType`]
/// (issue #45): a passthrough of an `integer` FK must stay `integer` on the
/// target so it remains eligible as a relationship join key, instead of
/// widening to `numeric` (which the join-key allowlist excludes). The rendered
/// type is safe to interpolate into DDL for the same reason
/// [`PrimaryKeyColumn::data_type`] is — it comes from the catalog, not user
/// input.
async fn source_column_pg_types(
    pool: &Pool,
    source_table: &str,
) -> Result<HashMap<String, String>, DdlError> {
    let client = pool.get().await?;
    let rows = client
        .query(
            "select a.attname::text, pg_catalog.format_type(a.atttypid, a.atttypmod)
             from pg_attribute a
             where a.attrelid = pg_catalog.to_regclass($1)
               and a.attnum > 0
               and not a.attisdropped",
            &[&source_table],
        )
        .await?;
    Ok(rows
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect())
}

/// The neighbor target table's name for `def` — see module docs for why this
/// is simply `def.target` unchanged. Bare — `def.target` is always the
/// unqualified table name, even for a definition whose `TRANSFORM` clause
/// explicitly wrote a `schema.table` spelling (issue #76; see
/// [`super::ast::TransformDef`]'s own doc comment for why that dotted
/// spelling never lands in this field), independent of which schema
/// [`qualified_target_table`] actually creates it under. Note this is *not*
/// the same string `transform_definitions.target_table` persists as of issue
/// #73: the catalog's own identity column holds the fully-qualified
/// `schema.table` form (built via `intake::publication::qualify`, at
/// definition-acceptance time — see `catalog::create_definition_inner`), not
/// this bare name. Callers that need a live connection (whose `search_path`
/// already resolves this bare name to the right physical table —
/// `pool::session_bootstrap` pins `target_schema` onto it) can keep using
/// this unqualified; callers that need the persisted identity string must
/// read `transform_definitions.target_table` instead, not reconstruct it
/// from this function.
pub fn neighbor_table_name(def: &TransformDef) -> &str {
    &def.target
}

/// The fully schema-qualified name of `def`'s neighbor target table under
/// `target_schema` (see the module doc comment) — `"{target_schema}"."{def.target}"`,
/// each component quoted independently via [`quote_ident`]. Built for direct
/// interpolation into DDL text, not as a persisted-identity string: this
/// quotes each component separately, whereas `transform_definitions.target_table`
/// (issue #73) is the plain, unquoted `"schema.table"` form
/// `intake::publication::qualify` builds — the two are never byte-for-byte
/// equal, so don't compare or persist this function's output as if it were
/// that identity.
pub fn qualified_target_table(target_schema: &str, def: &TransformDef) -> String {
    format!(
        "{}.{}",
        quote_ident(target_schema),
        quote_ident(neighbor_table_name(def))
    )
}

/// The physical, Trellis-owned table a to-one relationship's settled parent
/// projection lives in (issue #129, epic #127's "settled parent projection"
/// — see `trellis/tests/spikes/issue-102-PLAN-DRAFT.md` §2) —
/// `_trellis_rel_projection_<id>`, named after the relationship's own
/// catalog id rather than its declared name. A relationship name is unique
/// only *per from-table* (`V38__relationship_definitions_schema_qualified_key.sql`'s
/// `unique (from_schema, from_table, name)`), so two different from-tables can declare a
/// same-named relationship; the id is the one thing already guaranteed
/// globally unique by the time this is called
/// (`catalog::create_relationship` names the projection right after
/// inserting the relationship's own row, in the same transaction — see
/// `catalog::ensure_relationship_projection_in_txn`), so it's the natural
/// disambiguator. Matches this crate's existing `_trellis_backfill_*` naming
/// for its own other generated tables ([`super::backfill::STAGE_TABLE`],
/// [`super::backfill::REL_STAGE_TABLE_PREFIX`]).
pub(crate) fn relationship_projection_table_name(relationship_id: i64) -> String {
    format!("_trellis_rel_projection_{relationship_id}")
}

/// The schema-qualified, DDL-ready form of
/// [`relationship_projection_table_name`]'s output — same component-independent
/// quoting as [`qualified_target_table`], for direct interpolation into DDL/DML
/// text.
pub(crate) fn qualified_relationship_projection_table(
    target_schema: &str,
    projection_table: &str,
) -> String {
    format!(
        "{}.{}",
        quote_ident(target_schema),
        quote_ident(projection_table)
    )
}

/// The settled parent projection's per-row generation counter (issue #129,
/// epic #127; the plan doc's §2 guard (b), "optimistic generation check"):
/// bumped by any forward apply that touches this parent row, so a reverse
/// (#131/#132) can detect — by re-reading this column under `FOR UPDATE` in
/// its own apply transaction and comparing against the value it captured
/// when it enumerated — that a forward apply landed in between, and
/// abort/defer rather than overwrite a fresher forward-applied value. Every
/// row starts at `0` at backfill time (see
/// [`super::catalog::ensure_relationship_projection_in_txn`]); #131 is the
/// intended first writer of any advance past that seed, and the plan doc's
/// §3.1 finding (the generation must be bumped from the *raw* change images,
/// not the folded ones) is guidance for that implementation, not something
/// this column's shape enforces on its own.
///
/// `__trellis_`-prefixed rather than a bare `gen`, matching this module's
/// existing `__{field}_sum`/`__{field}_count` hidden-partial-column
/// convention (see [`avg_sum_column`]/[`count_needing_arg`]): this makes a
/// collision with a to-side column a consumer legitimately reads through the
/// relationship *unlikely*, not impossible — a to-side column literally
/// named `__trellis_gen` (equally: [`PROJECTION_LSN_COLUMN`] and a to-side
/// `__trellis_lsn`) would still silently collide with this bookkeeping
/// column, the same residual risk every `__`-prefixed hidden column in this
/// module already accepts. Review follow-up to issue #129: flagged
/// explicitly here, for #130/#131 to keep in mind, rather than solved now —
/// detecting/rejecting it would need its own validation pass over the
/// to-side schema, out of this issue's foundation-only scope, and no
/// generated or hand-written schema in this codebase's own test/fixture
/// corpus exercises it today.
pub(crate) const PROJECTION_GEN_COLUMN: &str = "__trellis_gen";

/// The settled parent projection's per-row LSN chain (issue #129, epic #127;
/// the plan doc's §2 guard (d), "per-parent ordering"): advanced only when a
/// *justified* reverse — one whose own `prev_lsn` matched this column's
/// current value, read under lock — applies, to the LSN of the parent-row
/// change that reverse represents. Two parent changes staged from different
/// ring segments can drain out of order (plan doc §3: "a later segment's
/// bucket can commit before an earlier segment's"), which is what makes this
/// chain necessary rather than a plain "last write wins" advance.
///
/// **Seeding, and what #131/#132 should confirm about it.** At backfill time
/// every row is seeded with the same single value: the current WAL position
/// as of backfill completion (`pg_current_wal_lsn()`, captured once in
/// [`super::catalog::ensure_relationship_projection_in_txn`] and shared by
/// every row that one backfill statement writes) — not any individual
/// row's own "true" last-modified LSN, which a plain `SELECT` against the
/// to-side table has no way to recover (Postgres doesn't expose a per-row
/// last-commit LSN). This is sound *as long as* guard (d) is implemented as
/// a self-referential, optimistic version stamp — a reverse's own `prev_lsn`
/// is populated by reading this same column at enumeration time, and guard
/// (d) simply re-checks it hasn't moved since, the same shape guard (b)'s
/// `gen` check already uses — because then the seed only has to be
/// internally consistent with itself, never externally correct against real
/// WAL history. **Flagged for #131/#132 to double check**: if guard (d)
/// instead needs this to equal some independently-verifiable "last change to
/// this exact row" LSN, this seeding strategy does not provide that, and
/// needs revisiting before guard (d) is implemented against it.
///
/// `not null`, not nullable: every row is written by a backfill that always
/// has a real captured LSN in hand (there is no code path yet — forward
/// apply of a brand-new to-side row is #130/#131 — that inserts a projection
/// row any other way), so there is no seedless state this column needs to
/// represent.
pub(crate) const PROJECTION_LSN_COLUMN: &str = "__trellis_lsn";

/// The read-side counterpart to [`qualified_target_table`] (issue #76,
/// ADR-0007): quotes an already-qualified `"schema.table"` name — as read
/// back from [`super::model::Definition::source_table`]
/// (`transform_definitions.source_table`), or freshly resolved by
/// `catalog::resolve_source_for_install`/`create_definition_inner`'s own
/// `qualified_source` at definition-acceptance time — for direct
/// interpolation into DDL/DML text, each component quoted independently via
/// [`quote_ident`]. This is what every physical SQL-builder that reads a
/// definition's live source table (backfill, CDC apply, quarantine
/// recompute) must use in place of a bare `quote_ident(&def.source)`/
/// `quote_ident(source_key)`, which would otherwise leave the schema to
/// resolve against whatever `search_path` the executing session happens to
/// carry (`pool::session_bootstrap`'s pinned `Config::schema`/
/// `Config::target_schema`/`"public"`) — exactly the bug class ADR-0007
/// exists to close.
///
/// Splits on the first `.`, matching `intake::publication::qualify`'s sole
/// construction site for this shape (which rejects a `.` inside either
/// component, so the first `.` here is always the real separator). Falls
/// back to quoting `qualified` whole when it carries no `.` at all — not a
/// shape any production caller produces (every real source is qualified by
/// the time it reaches here), but keeps this usable by tests/oracles that
/// hand-build a plan against a bare table name in the connection's own
/// default schema.
pub(crate) fn qualified_source_table(qualified: &str) -> String {
    quote_qualified_ident(qualified)
}

/// The target-side counterpart to [`qualified_source_table`] — same
/// component-independent quoting of an already-qualified `"schema.table"`
/// string, for a definition's *target* identity
/// ([`super::model::Definition::target_table`]) rather than its source.
///
/// Broader sweep, reviewer follow-up to issue #74 (epic #78's own
/// whole-branch review): the live CDC-apply write path (`staging::apply`'s
/// `apply_target`/truncate-clears loop, `staging::apply_aggregate`'s
/// target-write sites, `staging::quarantine`'s `recompute_column`) never got
/// this fix on the target side, even though issue #76 already let a
/// `TRANSFORM` clause spell an explicit non-default target schema — every
/// one of those sites was still binding `def.def.target` (bare) straight
/// into `quote_ident`, which silently mis-resolved (or simply couldn't find)
/// a target explicitly qualified outside the connection's pinned
/// `search_path`. Every such site now reads
/// [`super::model::Definition::target_table`] (or a plan field carrying it
/// forward, mirroring how `source`/`qualified_source` already got threaded
/// through in #76) through this function instead.
pub(crate) fn qualified_target_table_ident(qualified: &str) -> String {
    quote_qualified_ident(qualified)
}

/// Shared quoting logic for [`qualified_source_table`]/
/// [`qualified_target_table_ident`]: splits an already-qualified
/// `"schema.table"` string on its first `.` and quotes each component
/// independently, for direct interpolation into DDL/DML text. See
/// [`qualified_source_table`]'s own doc comment for the fallback/splitting
/// rationale — identical for both callers, since neither cares whether the
/// qualified string came from a source or target identity.
fn quote_qualified_ident(qualified: &str) -> String {
    match qualified.split_once('.') {
        Some((schema, table)) => format!("{}.{}", quote_ident(schema), quote_ident(table)),
        None => quote_ident(qualified),
    }
}

/// Creates `def`'s neighbor target table (idempotent: `create table if not
/// exists`) with `pk` as its primary key and one column per calculated
/// field, typed per that field's inferred [`ValueType`] (`numeric`, `text`,
/// or `boolean`). `source_columns` is the same source-column-to-[`ValueType`]
/// map `def` was validated against — needed here to re-derive each field's
/// type, since the grammar has no separate "declare a target column's type"
/// syntax (a field's inferred type *is* its target column's type).
///
/// `target_schema` is the schema the table is created under (see
/// [`qualified_target_table`]) — distinct from the connection's own
/// Trellis-managed schema, so this is always schema-qualified explicitly
/// rather than relying on `search_path`.
///
/// `source_table` is `def.source`'s fully-qualified `"schema.table"` identity
/// (issue #76, ADR-0007) — the caller's own already-resolved
/// `catalog::resolve_source_for_install` result — used below (via
/// [`qualified_source_table`]) to introspect a passthrough field's concrete
/// source column type, rather than the bare `def.source` left to
/// `search_path`.
pub async fn create_target_table(
    pool: &Pool,
    def: &TransformDef,
    target_schema: &str,
    pk: &[PrimaryKeyColumn],
    source_columns: &HashMap<String, ValueType>,
    source_table: &str,
) -> Result<(), DdlError> {
    // Issue #40: a relationship-enriched field's type is the referenced
    // to-side column's type, which `infer_field_types` reads from resolved
    // relationship metadata. Resolve it the same way `create_definition` does
    // (catalog + `pg_catalog` lookups); a relationship-free definition
    // resolves to an empty map and behaves exactly as before.
    let relationships = super::catalog::resolve_relationships(pool, def, source_table)
        .await
        .map_err(map_resolve_error)?;
    let field_types = super::validate::infer_field_types(def, source_columns, &relationships)?;

    // Issue #45: a bare source-column passthrough keeps the source column's
    // *concrete* Postgres type instead of collapsing through `ValueType` (so a
    // passthrough of an `integer` FK stays `integer`, not `numeric`, and can
    // still serve as a relationship join key). Only introspect the source
    // table's column types when at least one such field exists — a definition
    // with none behaves exactly as before, no extra query.
    //
    // Staging (`staging::apply`) still casts every value through its
    // `ValueType`-based cast (e.g. `::text::numeric`) before the INSERT,
    // regardless of the narrower concrete column type declared here — it
    // relies on Postgres's implicit assignment cast (`numeric` -> `integer`,
    // `text` -> `varchar(n)`, ...) to land the value. That's only safe because
    // a bare passthrough's value provably originates from this same,
    // identically-typed source column, so it always satisfies the narrower
    // column's constraints. If a passthrough field's value could ever diverge
    // from its source column's type/width, this coupling would need
    // revisiting (staging would need to cast to the concrete type too).
    let passthroughs: HashMap<&str, &str> = def
        .fields
        .iter()
        .filter_map(|f| {
            passthrough_source_column(f, def, source_columns).map(|col| (f.name.as_str(), col))
        })
        .collect();
    let source_pg_types = if passthroughs.is_empty() {
        HashMap::new()
    } else {
        source_column_pg_types(pool, source_table).await?
    };

    let mut sql = format!(
        "create table if not exists {} (",
        qualified_target_table(target_schema, def),
    );
    let pk_cols: Vec<String> = pk
        .iter()
        .map(|c| format!("{} {}", quote_ident(&c.name), c.data_type))
        .collect();
    sql.push_str(&pk_cols.join(", "));
    for field in &def.fields {
        let pg_type = match passthroughs
            .get(field.name.as_str())
            .and_then(|col| source_pg_types.get(*col))
        {
            Some(concrete) => concrete.clone(),
            None => pg_type_name(
                field_types
                    .get(&field.name)
                    .copied()
                    .unwrap_or(ValueType::Numeric),
            )
            .to_string(),
        };
        sql.push_str(&format!(", {} {}", quote_ident(&field.name), pg_type));
    }
    // A table-level `primary key (...)` constraint, not an inline
    // column-level one — works uniformly for one column or several (issue
    // #121: a composite source primary key mirrors onto the target as a
    // real composite primary key, the same way a single-column one already
    // did before this issue).
    let pk_names: Vec<String> = pk.iter().map(|c| quote_ident(&c.name)).collect();
    sql.push_str(&format!(", primary key ({})", pk_names.join(", ")));
    sql.push(')');

    let client = pool.get().await?;
    client.batch_execute(&sql).await?;
    Ok(())
}

/// Whether `field` is a direct `AVG(...)` call — the shape whose delta
/// model (issue #11) needs hidden running-sum/running-count partials
/// alongside its visible column, since `avg = sum / count` and neither half
/// alone is invertible (see `defs::invertibility`'s doc comment on
/// [`super::invertibility::PartialField`]). Checked structurally rather than
/// via `invertibility::classify` here: the grammar only ever parses an `AVG`
/// call over a `Numeric` argument (`registry::AGGREGATE_FUNCTION_SPECS`
/// restricts every aggregate's argument type to `Numeric`), so there is no
/// `Text`/`Boolean`-argument `AVG` this gate would need to route to the
/// recompute path instead — the name alone determines the answer for every
/// definition this grammar can actually produce.
///
/// Takes the field's expression directly (rather than a [`FieldDef`]) so a
/// caller can classify a *substituted* expression — see
/// [`create_aggregate_target_table`]'s doc comment on why a bare
/// cross-field-alias reference (`total2 = total` where `total = SUM(amount)`)
/// must be classified against its self-contained, alias-resolved form rather
/// than the raw `Expr::Column("total")` a naive per-field read would see.
fn is_avg_field(expr: &Expr) -> bool {
    matches!(expr, Expr::FunctionCall { name, .. } if name == "AVG")
}

/// Whether `field` is a direct `SUM(...)` call — see [`is_avg_field`]'s doc
/// comment for why this is checked structurally rather than via
/// `invertibility::classify`. `SUM` needs its own hidden running-count
/// partial (see [`count_column_names`]) for the same reason `AVG` needs
/// one: Postgres's `sum()` is `NULL`, not `0`, over zero non-null values, and
/// without a count the delta model can't distinguish "no contributions left"
/// from "contributions that net to zero" once a group's row count is no
/// longer directly observable from the running sum alone.
fn is_sum_field(expr: &Expr) -> bool {
    matches!(expr, Expr::FunctionCall { name, .. } if name == "SUM")
}

/// The hidden running-sum partial column name an `AVG` field maintains
/// (issue #11's delta model): the numerator `AVG`'s visible column is derived
/// from (`avg = sum / count`). Always one per `AVG` field — unlike the
/// running-count partial (see [`count_column_names`]'s doc comment on issue
/// #48), two `AVG` fields' running sums can never be shared: even when two
/// fields aggregate the exact same argument, their *sums* are only equal by
/// construction for `SUM`-vs-`AVG` pairs that also share a count (an `AVG`
/// field always needs its own sum regardless, since `SUM`'s visible column
/// already *is* that shared sum for the `SUM` half — there is no second
/// consumer to fold onto). `pub(crate)` so `staging::apply_aggregate` binds
/// against the exact same name this module creates, rather than re-deriving
/// it.
pub(crate) fn avg_sum_column(field_name: &str) -> String {
    format!("__{field_name}_sum")
}

/// The `Expr` a `SUM`/`AVG` field aggregates over — `None` for any other
/// field shape. [`count_column_names`] uses this to decide which fields'
/// hidden count partials can share a column; `staging::apply_aggregate`'s
/// `AggregateTargetPlan` runs the equivalent lookup against its own
/// already-classified fields, over the same [`Expr`] equality, to derive
/// matching names without a second implementation of this rule.
fn count_needing_arg(expr: &Expr) -> Option<&Expr> {
    if !is_sum_field(expr) && !is_avg_field(expr) {
        return None;
    }
    match expr {
        Expr::FunctionCall { args, .. } => args.first(),
        _ => None,
    }
}

/// Assigns every `SUM`/`AVG` field in `fields` (in declaration order) the
/// name of the hidden running-count partial column it maintains (issue #11's
/// delta model: the count of non-null argument values contributing to the
/// group, matching Postgres's own `count(<same argument>)` "skip NULLs"
/// semantics) — `pub(crate)` so `staging::apply_aggregate`'s
/// `AggregateTargetPlan` derives the exact same names this module's DDL
/// creates, rather than re-deriving them independently (a divergence there
/// would mean the apply path writes to a column the DDL never created, or
/// vice versa).
///
/// **Issue #48**: naively, every count-needing field got its own
/// `__{field}_count` column, even when two fields aggregate the exact
/// identical argument expression (e.g. `SUM(amount) AS total, AVG(amount) AS
/// average` both aggregating the same `amount` column) and are therefore
/// provably counting the exact same set of non-null-contributing rows. This
/// function detects that case — via [`Expr`]'s derived structural
/// `PartialEq` on [`count_needing_arg`]'s result — and has the later field's
/// entry point at the earlier field's column name instead of minting a
/// second, redundant one.
///
/// It deliberately does **not** merge count columns across fields whose
/// arguments differ (e.g. the issue's own motivating example, `SUM(word_count)
/// AS total_words, SUM(byte_size) AS total_bytes`) into one target-wide
/// `__group_count`, even though the issue asked for exactly that: two
/// different source columns can have different `NULL`s on the very same row,
/// so their "count of non-null contributing rows" can genuinely diverge (a
/// row with a `NULL` `word_count` but a real `byte_size` contributes to one
/// count and not the other). Forcing them onto one shared column would
/// silently corrupt whichever field's count that column doesn't actually
/// track the moment their arguments' `NULL` patterns diverge — the delta
/// model would compute the wrong "does this group still have any non-null
/// contributor" answer for one of the two fields, producing a stale `0`/wrong
/// number where Postgres would show `NULL`, or vice versa (see
/// `staging::apply_aggregate`'s `sum_goes_null_not_zero_when_a_groups_remaining_rows_are_all_null`
/// test for the exact failure shape this would reintroduce). Only fields
/// that provably always agree — same argument expression — are ever merged.
/// `substituted` maps each field name to its cross-field-alias-resolved
/// expression (see [`super::backfill::substituted_field_exprs`]) — classifying
/// off the substituted view rather than each field's raw, possibly-aliasing
/// `Expr` is what lets a field that only resolves to a bare `SUM`/`AVG` call
/// *after* substitution (`total2 = total` where `total = SUM(amount)`) still
/// get a hidden count-column name here, consistent with
/// [`create_aggregate_target_table`]'s own (also substituted) classification
/// of the very same field.
pub(crate) fn count_column_names(
    fields: &[FieldDef],
    substituted: &HashMap<String, Expr>,
) -> HashMap<String, String> {
    count_column_names_from(
        fields.iter().filter_map(|f| {
            count_needing_arg(&substituted[&f.name]).map(|arg| (f.name.as_str(), arg))
        }),
    )
}

/// The core of [`count_column_names`], generalized over any source of
/// `(field_name, aggregated_arg)` pairs rather than a literal `&[FieldDef]` —
/// `staging::apply_aggregate`'s `AggregateTargetPlan` has already classified
/// its fields into [`super::ast::FieldDef`]-free `AggFieldPlan`s by the time
/// it needs these names, so it builds its own `(name, arg)` pairs from that
/// classification plus its `field_exprs` map and calls straight into this,
/// rather than re-implementing the merge rule (see [`count_column_names`]'s
/// doc comment on why that rule's correctness matters) a second time.
pub(crate) fn count_column_names_from<'a>(
    entries: impl Iterator<Item = (&'a str, &'a Expr)>,
) -> HashMap<String, String> {
    let mut by_arg: Vec<(&Expr, String)> = Vec::new();
    let mut names = HashMap::new();
    for (field_name, arg) in entries {
        let name = match by_arg.iter().find(|(seen, _)| **seen == *arg) {
            Some((_, existing)) => existing.clone(),
            None => {
                let fresh = format!("__{field_name}_count");
                by_arg.push((arg, fresh.clone()));
                fresh
            }
        };
        names.insert(field_name.to_string(), name);
    }
    names
}

/// Creates an [`super::ast::KeySpace::Aggregate`] definition's neighbor
/// target table (idempotent, same convention as [`create_target_table`]),
/// whose key is the composite tuple of grouping columns rather than a single
/// column inherited from the source — a `GROUP BY` target has no single
/// source row to inherit a key from; the group itself is the key.
///
/// The grouping columns are keyed with a `UNIQUE NULLS NOT DISTINCT`
/// constraint rather than a bare `PRIMARY KEY` (issue #128): a source
/// grouping column can itself be `NULL` (Postgres's own `GROUP BY` folds all
/// `NULL`s in a column into one group, same as any other value), and a
/// `PRIMARY KEY` forbids `NULL` in any of its columns outright, which made a
/// NULL-keyed group unrepresentable on the target — backfill silently
/// dropped it and the live delta path raised a `not-null constraint`
/// violation that quarantined the row with no way to recover it. `NULLS NOT
/// DISTINCT` (Postgres 15+, pinned to 17.10 in `.tool-versions`) keeps the
/// same dedup guarantee `PRIMARY KEY` gave — two grouping tuples that agree
/// on every column, NULLs included, still collide — while allowing the NULL
/// tuple to exist at all. `ON CONFLICT (group columns)` (both here and in
/// the live delta path) still resolves against this constraint exactly as it
/// did against the old `PRIMARY KEY`: conflict-target inference matches on
/// the indexed columns, not on the index's nulls-distinctness. Chaining a
/// further definition off this target still works because
/// [`source_primary_key`] falls back to a table's unique constraint when it
/// has no `indisprimary` index.
///
/// Each grouping column's type comes from `source_columns` (the same
/// [`ValueType`]-only map every other column type in this grammar is
/// derived from — there's no separate exact-Postgres-type introspection for
/// grouping columns, unlike the 1-1 primary key's [`source_primary_key`]) —
/// except a [`GroupByKey::RelationshipPath`] key (issue #137), whose type
/// comes from the to-side column `relationships` (resolved the same way
/// issue #94's relationship-field typing already is) reports instead.
///
/// A calculated field whose name matches a grouping column (the
/// `SELECT order_id AS order_id, SUM(amount) AS total` passthrough idiom)
/// contributes no separate column — it's assumed to be that same grouping
/// value passed through, already covered by the unique-keyed column above.
///
/// `target_schema` is the schema the table is created under — see
/// [`create_target_table`]'s doc comment on why this is always
/// schema-qualified explicitly rather than relying on `search_path`.
///
/// # Panics
///
/// If `def.key_space` is not [`KeySpace::Aggregate`].
pub async fn create_aggregate_target_table(
    pool: &Pool,
    def: &TransformDef,
    target_schema: &str,
    source_columns: &HashMap<String, ValueType>,
) -> Result<(), DdlError> {
    let KeySpace::Aggregate { group_by } = &def.key_space else {
        panic!("create_aggregate_target_table called on a non-aggregate definition");
    };

    // Substitute cross-field-alias references (e.g. `total2 = total` where
    // `total = SUM(amount)`) before classifying any field as `SUM`/`AVG`
    // below — `is_avg_field`/`is_sum_field`/`count_column_names` must agree
    // with `backfill_aggregate`'s and `apply_aggregate::classify_fields`'s own
    // (already substituted) classification of the same field, or the columns
    // this DDL creates diverge from the columns those paths later write to.
    let substituted = super::backfill::substituted_field_exprs(def)?;

    // Issue #94: a GROUP BY aggregate field may aggregate a *to-one*
    // relationship path (`SUM(post.word_count)`), whose type is the to-side
    // column's, not any column of `source_columns`. Resolve the relationship
    // metadata the same way [`create_target_table`] does so type inference can
    // reach it; a relationship-free aggregate resolves to an empty map and
    // behaves exactly as before.
    //
    // No qualified source is threaded in here, unlike `create_target_table`:
    // this runs during install, before anything has recorded one, so it is
    // resolved the same way `install_definition` resolves it (issue #288 —
    // relationships are looked up on the qualified source).
    let relationships = super::catalog::resolve_relationships_for_new_definition(pool, def)
        .await
        .map_err(map_resolve_error)?;
    let field_types = super::validate::infer_field_types(def, source_columns, &relationships)?;

    let mut sql = format!(
        "create table if not exists {} (",
        qualified_target_table(target_schema, def)
    );
    for (i, key) in group_by.iter().enumerate() {
        if i > 0 {
            sql.push_str(", ");
        }
        // Issue #137: a `GroupByKey::RelationshipPath` key's type is its
        // to-side column's, resolved via the same `relationships` map issue
        // #94's field typing already reuses above — a plain column keeps
        // reading straight from `source_columns`, unchanged.
        let value_type = match key {
            GroupByKey::Column(column) => source_columns
                .get(column)
                .copied()
                .unwrap_or(ValueType::Numeric),
            GroupByKey::RelationshipPath { rel, column } => relationships
                .get(rel)
                .and_then(|r| r.column_types.get(column))
                .copied()
                .unwrap_or(ValueType::Numeric),
        };
        let pg_type = pg_type_name(value_type);
        sql.push_str(&format!(
            "{} {}",
            quote_ident(key.target_column_name()),
            pg_type
        ));
    }
    // Issue #48: fields aggregating the exact same argument (e.g. `SUM(amount)
    // AS total, AVG(amount) AS average`) share one hidden running-count
    // partial column rather than each minting its own — see
    // `count_column_names`'s doc comment for why this dedup is scoped to
    // "same argument expression" rather than "any count-needing field on this
    // target", which would silently corrupt the delta model once two fields'
    // arguments have different `NULL` patterns.
    let count_cols = count_column_names(&def.fields, &substituted);
    let mut emitted_count_cols: std::collections::HashSet<&str> = std::collections::HashSet::new();
    for field in &def.fields {
        if group_by_contains(group_by, &field.name) {
            continue;
        }
        let pg_type = pg_type_name(
            field_types
                .get(&field.name)
                .copied()
                .unwrap_or(ValueType::Numeric),
        );
        sql.push_str(&format!(", {} {}", quote_ident(&field.name), pg_type));

        let expr = &substituted[&field.name];
        // AVG's hidden partials (see `is_avg_field`'s doc comment): the
        // visible column above holds the derived `sum / count`. The running
        // sum is always this field's own; the running count may already have
        // been declared by an earlier field sharing this exact argument.
        if is_avg_field(expr) {
            let sum_col = avg_sum_column(&field.name);
            sql.push_str(&format!(", {} numeric", quote_ident(&sum_col)));
            let count_col = &count_cols[&field.name];
            if emitted_count_cols.insert(count_col.as_str()) {
                sql.push_str(&format!(", {} bigint", quote_ident(count_col)));
            }
        } else if is_sum_field(expr) {
            // SUM's hidden count partial (see `is_sum_field`'s doc comment):
            // the visible column above holds the running sum directly, but
            // this is needed to tell "sum of nothing" (NULL) from "sum that
            // happens to net to zero" (0).
            let count_col = &count_cols[&field.name];
            if emitted_count_cols.insert(count_col.as_str()) {
                sql.push_str(&format!(", {} bigint", quote_ident(count_col)));
            }
        }
    }
    let pk_columns: Vec<String> = group_by
        .iter()
        .map(|k| quote_ident(k.target_column_name()))
        .collect();
    sql.push_str(&format!(
        ", unique nulls not distinct ({})",
        pk_columns.join(", ")
    ));
    sql.push(')');

    let client = pool.get().await?;
    client.batch_execute(&sql).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::defs::ast::{FieldDef, KeySpace, Operator, Predicate};

    fn def() -> TransformDef {
        TransformDef {
            target: "order_totals".to_string(),
            source: "orders".to_string(),
            key_space: KeySpace::OneToOne,
            fields: vec![FieldDef {
                name: "total".to_string(),
                expr: crate::defs::ast::Expr::BinaryOp {
                    op: Operator::Add,
                    lhs: Box::new(crate::defs::ast::Expr::Column("price".to_string())),
                    rhs: Box::new(crate::defs::ast::Expr::Column("tax".to_string())),
                },
            }],
            predicate: Predicate::True,
            explicit_source_schema: None,
            explicit_target_schema: None,
        }
    }

    #[test]
    fn neighbor_table_name_is_the_definitions_target() {
        assert_eq!(neighbor_table_name(&def()), "order_totals");
    }

    /// A **nullable** key — the aggregate-target (`UNIQUE NULLS NOT
    /// DISTINCT`) shape every issue #110 encoding test here is about. Use
    /// [`not_null_pk`] for a real `PRIMARY KEY`.
    fn pk(columns: &[&str]) -> Vec<PrimaryKeyColumn> {
        columns
            .iter()
            .map(|name| PrimaryKeyColumn {
                name: (*name).to_string(),
                data_type: "text".to_string(),
                nullable: true,
            })
            .collect()
    }

    /// A real `PRIMARY KEY`'s columns: `NOT NULL`, and therefore rendered
    /// and decoded raw — see [`encode_key_part`]'s
    /// "[`PrimaryKeyColumn::nullable`] selects the encoding" section.
    fn not_null_pk(columns: &[&str]) -> Vec<PrimaryKeyColumn> {
        pk(columns)
            .into_iter()
            .map(|c| PrimaryKeyColumn {
                nullable: false,
                ..c
            })
            .collect()
    }

    /// Issue #110: every column reference `pk_key_sql_expr` renders is
    /// [`null_key_escape_sql`]'s wrapper, not a bare `<col>::text` — the
    /// SQL-side half of the NULL-safe encoding, matching
    /// [`encode_key_part`]'s Rust-side substitution *and* its doubling
    /// escape byte-for-byte, so a key's text agrees on both sides of the
    /// wire regardless of arity. `chr(1)`, not `chr(0)`: Postgres's `chr()`
    /// itself refuses to construct a NUL byte (`NULL_KEY_SENTINEL`'s doc
    /// comment) — a live-database regression this exact test would not have
    /// caught (`pk_key_sql_expr` is pure string rendering), which is why
    /// every `defs_aggregate_chained_*_group_key.rs` front-door test drives
    /// this expression against a real Postgres end to end.
    #[test]
    fn pk_key_sql_expr_coalesces_a_null_component_to_the_sentinel() {
        assert_eq!(
            pk_key_sql_expr(&pk(&["warehouse"]), Some("t")),
            "coalesce(replace(t.\"warehouse\"::text, chr(1), chr(1) || chr(1)), chr(1))"
        );
        // At arity ≥ 2, issue #200's separator escape wraps *around* that —
        // see `pk_key_sql_expr_nests_the_null_and_separator_escapes_per_column`.
        assert_eq!(
            pk_key_sql_expr(&pk(&["warehouse", "sku"]), Some("t")),
            format!(
                "array_to_string(array[{}, {}], chr(31))",
                composite_key_escape_sql(&null_key_escape_sql("t.\"warehouse\"::text")),
                composite_key_escape_sql(&null_key_escape_sql("t.\"sku\"::text")),
            )
        );
    }

    /// The other half of that contract, and the fix for the data-corruption
    /// bug issue #110's own escape introduced: a **not-null** key column —
    /// every real `PRIMARY KEY` — renders as its bare `<col>::text`, with no
    /// `coalesce` and no `replace`, at either arity. It can never produce a
    /// `NULL` component to substitute, and its rendered text is not private
    /// to the ring: `staging::apply::apply_target` binds the staged key
    /// straight in as a 1-1 target's own literal primary-key value, which
    /// `defs::backfill`/`staging::quarantine`/`defs::oracle` write and read
    /// raw. Deliberately *not* an arity-1 exemption — the arity-1 nullable
    /// case above (a single-column nullable `GROUP BY`) is issue #110's
    /// original bug and keeps the full encoding.
    #[test]
    fn pk_key_sql_expr_renders_a_not_null_key_column_raw() {
        assert_eq!(
            pk_key_sql_expr(&not_null_pk(&["warehouse"]), Some("t")),
            "t.\"warehouse\"::text"
        );
        assert_eq!(
            pk_key_sql_expr(&not_null_pk(&["warehouse", "sku"]), Some("t")),
            format!(
                "array_to_string(array[{}, {}], chr(31))",
                composite_key_escape_sql("t.\"warehouse\"::text"),
                composite_key_escape_sql("t.\"sku\"::text"),
            ),
            "no `coalesce`/`chr(1)` anywhere — only issue #200's separator \
             escape, which a composite key of any nullability gets"
        );
        assert_eq!(pk_key_sql_expr(&not_null_pk(&["id"]), None), "\"id\"::text");
    }

    /// The Rust-side producer's twin of the test above: nothing encodes a
    /// not-null key's parts, so a genuine U+0001 inside a primary-key value
    /// survives [`join_pk_key`]/[`split_pk_key`] verbatim rather than being
    /// doubled — and the lone-sentinel text is *not* decoded back to `None`
    /// for such a column, since a not-null column never had a `NULL` to
    /// encode.
    #[test]
    fn split_pk_key_leaves_a_not_null_columns_part_verbatim() {
        let key = join_pk_key(["a\u{1}b", "\u{1}"]);
        assert_eq!(key, "a\u{1}b\u{1f}\u{1}");
        assert_eq!(
            split_pk_key(&not_null_pk(&["warehouse", "sku"]), "t", &key)
                .expect("decodes")
                .iter()
                .map(Option::as_deref)
                .collect::<Vec<_>>(),
            vec![Some("a\u{1}b"), Some("\u{1}")]
        );
    }

    /// Nullability is decided per *column*, not per key: a mixed key (a
    /// not-null column alongside a nullable one) encodes each half its own
    /// way, and each decodes back exactly.
    #[test]
    fn a_mixed_nullability_key_encodes_each_column_its_own_way() {
        let mixed = vec![
            PrimaryKeyColumn {
                name: "warehouse".to_string(),
                data_type: "text".to_string(),
                nullable: false,
            },
            PrimaryKeyColumn {
                name: "sku".to_string(),
                data_type: "text".to_string(),
                nullable: true,
            },
        ];
        assert_eq!(
            pk_key_sql_expr(&mixed, Some("t")),
            format!(
                "array_to_string(array[{}, {}], chr(31))",
                composite_key_escape_sql("t.\"warehouse\"::text"),
                composite_key_escape_sql(&null_key_escape_sql("t.\"sku\"::text")),
            )
        );
        let key = join_pk_key(["a\u{1}b", NULL_KEY_SENTINEL]);
        assert_eq!(
            split_pk_key(&mixed, "t", &key)
                .expect("decodes")
                .iter()
                .map(Option::as_deref)
                .collect::<Vec<_>>(),
            vec![Some("a\u{1}b"), None]
        );
    }

    /// [`join_pk_key`] and [`split_pk_key`] are exact inverses, and an
    /// arity-1 key carries no separator at all — the property issue #103's
    /// and issue #171's fixes both lean on (a group key of either arity is
    /// byte-identical to what [`pk_key_sql_expr`] renders for the aggregate
    /// target's own grouping-column identity, so a chained definition's live
    /// re-fetch decodes it as an ordinary source primary key).
    #[test]
    fn join_pk_key_round_trips_through_split_pk_key() {
        assert_eq!(join_pk_key(["w1"]), "w1");
        assert_eq!(join_pk_key(["w1", "a"]), "w1\u{1f}a");
        assert_eq!(
            split_pk_key(&pk(&["warehouse", "sku"]), "t", &join_pk_key(["w1", "a"]))
                .unwrap()
                .iter()
                .map(Option::as_deref)
                .collect::<Vec<_>>(),
            vec![Some("w1"), Some("a")]
        );
        // A genuine empty-string component still occupies its own part, so
        // the arity check can't be fooled by it — and, issue #110, it must
        // decode as `Some("")`, distinct from a real `NULL` component below.
        assert_eq!(
            split_pk_key(&pk(&["warehouse", "sku"]), "t", &join_pk_key(["", "a"]))
                .unwrap()
                .iter()
                .map(Option::as_deref)
                .collect::<Vec<_>>(),
            vec![Some(""), Some("a")]
        );
    }

    /// Issue #110: a `NULL` key/group component, encoded via
    /// [`encode_key_part`] the way every real producer
    /// (`staging::apply_aggregate::derive_group_key`, [`pk_key_sql_expr`]'s
    /// SQL-side twin) must, round-trips back to `None` — distinguishable
    /// from the genuine empty string [`join_pk_key_round_trips_through_split_pk_key`]
    /// pins above, which is exactly the ambiguity the old
    /// `unwrap_or_default()`/bare-`col::text` encoding could not resolve.
    #[test]
    fn a_null_component_round_trips_distinct_from_an_empty_string() {
        let key = join_pk_key([encode_key_part(None), encode_key_part(Some("a"))]);
        assert_eq!(key, "\u{1}\u{1f}a");
        assert_eq!(
            split_pk_key(&pk(&["warehouse", "sku"]), "t", &key)
                .unwrap()
                .iter()
                .map(Option::as_deref)
                .collect::<Vec<_>>(),
            vec![None, Some("a")]
        );

        // The single-column case (no separator at all) round-trips the same
        // way: the bare sentinel text decodes back to `None`.
        let single = join_pk_key([encode_key_part(None)]);
        assert_eq!(single, "\u{1}");
        assert_eq!(
            split_pk_key(&pk(&["warehouse"]), "t", &single)
                .unwrap()
                .iter()
                .map(Option::as_deref)
                .collect::<Vec<_>>(),
            vec![None]
        );

        // A NULL component and an empty-string component encode to
        // different text and therefore never collide.
        assert_ne!(encode_key_part(None), encode_key_part(Some("")));
    }

    /// Review follow-up to issue #110: [`NULL_KEY_SENTINEL`] is an ordinary
    /// control character a `text` column can genuinely hold, so a value that
    /// *was* a lone U+0001 must not be readable back as a `NULL` component —
    /// that would fold two distinct groups onto one encoded key (silently,
    /// and data-dependently, which is worse than the bug #110 fixes). The
    /// doubling escape makes the two cases provably disjoint: an encoded
    /// non-`NULL` part's U+0001 runs are always even-length, and the `NULL`
    /// part is a run of one.
    #[test]
    fn a_real_sentinel_valued_component_never_decodes_as_null() {
        let one_col = pk(&["sku"]);
        let round_trip = |v: Option<&str>| {
            let key = join_pk_key([encode_key_part(v)]);
            let parts = split_pk_key(&one_col, "t", &key).unwrap();
            let back = parts[0].as_deref().map(str::to_string);
            (key, back)
        };

        let (null_key, null_back) = round_trip(None);
        assert_eq!(null_key, NULL_KEY_SENTINEL);
        assert_eq!(null_back, None);

        for value in ["\u{1}", "\u{1}\u{1}", "a\u{1}b", "\u{1}\u{1}\u{1}", ""] {
            let (key, back) = round_trip(Some(value));
            assert_ne!(
                key, null_key,
                "a genuine {value:?} must not encode to the NULL sentinel"
            );
            assert_eq!(
                back.as_deref(),
                Some(value),
                "and must round-trip back to itself"
            );
        }

        // The same holds inside a composite key, where the escape has to
        // survive the separator join/split too.
        let two_col = pk(&["warehouse", "sku"]);
        let key = join_pk_key([encode_key_part(Some("\u{1}")), encode_key_part(None)]);
        assert_eq!(
            split_pk_key(&two_col, "t", &key)
                .unwrap()
                .iter()
                .map(Option::as_deref)
                .collect::<Vec<_>>(),
            vec![Some("\u{1}"), None]
        );
    }

    /// The SQL-side twin of the test above: [`pk_key_sql_expr`] must render
    /// the *escape* as well as the `NULL` substitution, or the two producers
    /// disagree for any value holding a real U+0001.
    #[test]
    fn pk_key_sql_expr_escapes_a_real_sentinel_in_the_value() {
        assert_eq!(
            null_key_escape_sql("t.\"sku\"::text"),
            "coalesce(replace(t.\"sku\"::text, chr(1), chr(1) || chr(1)), chr(1))"
        );
        assert_eq!(
            pk_key_sql_expr(&pk(&["sku"]), Some("t")),
            null_key_escape_sql("t.\"sku\"::text")
        );
    }

    /// The length-prefixed encoding `apply_aggregate::derive_group_key` used
    /// to emit for a composite `GROUP BY` is exactly what issue #171's crash
    /// was: one part where two were expected.
    #[test]
    fn split_pk_key_rejects_the_pre_171_length_prefixed_encoding() {
        match split_pk_key(&pk(&["warehouse", "sku"]), "stock_totals", "2:w11:a") {
            Err(DdlError::MalformedCompositeKey {
                source_table,
                key,
                expected_arity,
                actual_arity,
            }) => {
                assert_eq!(source_table, "stock_totals");
                assert_eq!(key, "2:w11:a");
                assert_eq!(expected_arity, 2);
                assert_eq!(actual_arity, 1);
            }
            other => panic!("expected MalformedCompositeKey, got {other:?}"),
        }
    }

    /// [`split_pk_key`] against a fixed table name, unwrapped — every test
    /// below is about the encoding round-tripping, not about the
    /// [`DdlError::MalformedCompositeKey`] path
    /// `split_pk_key_rejects_the_pre_171_length_prefixed_encoding` covers.
    fn split_ok<'a>(pk: &[PrimaryKeyColumn], key: &'a str) -> Vec<Option<Cow<'a, str>>> {
        split_pk_key(pk, "t", key).expect("decodes")
    }

    /// Issue #200: a real, embedded [`COMPOSITE_KEY_SEPARATOR`] inside one
    /// key component's own value must round-trip through
    /// [`join_pk_key`]/[`split_pk_key`] instead of being misread as a field
    /// boundary — the exact failure mode
    /// `split_pk_key_rejects_the_pre_171_length_prefixed_encoding` above
    /// shows the *error path* for (a genuine arity mismatch), except this
    /// one used to fire on perfectly valid data whenever a real column value
    /// happened to contain U+001F, at any arity, not only >1.
    #[test]
    fn a_real_separator_valued_component_survives_the_round_trip() {
        // Arity 1: pre-#200, `split_pk_key` unconditionally split on
        // `COMPOSITE_KEY_SEPARATOR` regardless of the target arity, so a
        // single-column key whose own value was `"a\u{1f}b"` split into two
        // parts against an arity-1 `pk` and failed loudly
        // (`MalformedCompositeKey { expected_arity: 1, actual_arity: 2 }`)
        // even though there was no real composite key involved at all.
        let one_col = not_null_pk(&["sku"]);
        for value in ["a\u{1f}b", "\u{1f}", "\u{1f}\u{1f}", "\u{1f}a\u{1f}"] {
            let key = join_pk_key([value]);
            assert_eq!(
                key, value,
                "an arity-1 key must stay the column's own raw text, escape-free — it \
                 doubles as a real PK value on the write side (see \
                 `push_escaped_composite_key_part`'s \"why arity 1 is exempt\")"
            );
            assert_eq!(
                split_ok(&one_col, &key)
                    .iter()
                    .map(Option::as_deref)
                    .collect::<Vec<_>>(),
                vec![Some(value)],
                "{value:?} must still decode at arity 1"
            );
        }

        // Arity 2, separator embedded in either component, alongside a
        // genuinely separator-free neighbor — and then in both at once.
        let two_col = not_null_pk(&["warehouse", "sku"]);
        for (a, b) in [
            ("a\u{1f}b", "c"),
            ("c", "a\u{1f}b"),
            ("x\u{1f}y", "\u{1f}z"),
        ] {
            let key = join_pk_key([a, b]);
            assert_eq!(
                split_ok(&two_col, &key)
                    .iter()
                    .map(Option::as_deref)
                    .collect::<Vec<_>>(),
                vec![Some(a), Some(b)],
                "{a:?}/{b:?} must decode at arity 2"
            );
        }
    }

    /// A real [`KEY_PART_ESCAPE`] character in a value must itself round-trip
    /// — it is not a hypothetical: the whole point of introducing a second
    /// escape character (rather than doubling [`COMPOSITE_KEY_SEPARATOR`]
    /// directly, per [`KEY_PART_ESCAPE`]'s doc comment) is that it too can
    /// appear in ordinary column text and must not be assumed absent either.
    #[test]
    fn a_real_escape_valued_component_survives_the_round_trip() {
        let one_col = not_null_pk(&["sku"]);
        for value in ["\u{1e}", "\u{1e}\u{1e}", "a\u{1e}b", "\u{1e}\u{1f}\u{1e}"] {
            let key = join_pk_key([value]);
            assert_eq!(
                key, value,
                "arity 1 is verbatim, escape characters included"
            );
            assert_eq!(
                split_ok(&one_col, &key)
                    .iter()
                    .map(Option::as_deref)
                    .collect::<Vec<_>>(),
                vec![Some(value)]
            );
        }

        // Arity 2 is where the escape actually runs: every adversarial
        // shape — runs of both characters, adjacent to each other and to
        // the real field boundary, and a component that is *nothing but*
        // escape/separator characters — must survive.
        let two_col = not_null_pk(&["warehouse", "sku"]);
        for (a, b) in [
            ("\u{1e}", "\u{1f}"),
            ("\u{1e}\u{1f}", "\u{1f}\u{1e}"),
            ("\u{1e}\u{1e}\u{1f}\u{1f}\u{1e}", "\u{1f}\u{1f}\u{1e}\u{1e}"),
            ("a\u{1e}\u{1e}b", "\u{1e}\u{1f}\u{1e}\u{1f}"),
            ("", "\u{1e}"),
            ("\u{1f}", ""),
        ] {
            let key = join_pk_key([a, b]);
            assert_eq!(
                split_ok(&two_col, &key)
                    .iter()
                    .map(Option::as_deref)
                    .collect::<Vec<_>>(),
                vec![Some(a), Some(b)],
                "adversarial escape/separator run {a:?}/{b:?} must round-trip"
            );
        }
    }

    /// The precise ambiguity [`KEY_PART_ESCAPE`]'s doc comment argues naive
    /// doubling of [`COMPOSITE_KEY_SEPARATOR`] itself would create: joining
    /// `["x\u{1f}", "y"]` and joining `["x", "\u{1f}y"]` both place a
    /// trailing/leading real separator adjacent to the real field-boundary
    /// separator, producing a run of consecutive U+001F either way. A
    /// decoder that can't tell them apart would parse one of these two,
    /// distinct two-part keys wrong. This crate's two-escape-character
    /// scheme must keep them distinct and each internally correct.
    #[test]
    fn adjacent_separator_runs_do_not_collide() {
        let two_col = not_null_pk(&["warehouse", "sku"]);

        let trailing = join_pk_key(["x\u{1f}", "y"]);
        let leading = join_pk_key(["x", "\u{1f}y"]);
        assert_ne!(
            trailing, leading,
            "a trailing separator on the first component and a leading \
             separator on the second must not encode to the same text"
        );

        for (key, expected) in [
            (&trailing, vec![Some("x\u{1f}"), Some("y")]),
            (&leading, vec![Some("x"), Some("\u{1f}y")]),
        ] {
            assert_eq!(
                split_ok(&two_col, key)
                    .iter()
                    .map(Option::as_deref)
                    .collect::<Vec<_>>(),
                expected
            );
        }

        // And the case where *both* sides of the join carry a real
        // separator right at the boundary.
        let both = join_pk_key(["x\u{1f}", "\u{1f}y"]);
        assert_eq!(
            split_ok(&two_col, &both)
                .iter()
                .map(Option::as_deref)
                .collect::<Vec<_>>(),
            vec![Some("x\u{1f}"), Some("\u{1f}y")]
        );
    }

    /// [`composite_key_escape_sql`] must render the exact SQL twin of
    /// [`push_escaped_composite_key_part`]'s Rust-side escape — same
    /// ordering rationale (escape `KEY_PART_ESCAPE` first, then
    /// `COMPOSITE_KEY_SEPARATOR`) — and [`pk_key_sql_expr`] must route every
    /// column of a *multi-column* key through it, while leaving an arity-1
    /// not-null key as the bare `{pk}::text` the write path binds as a real
    /// PK value (see [`push_escaped_composite_key_part`]'s "why arity 1 is
    /// exempt").
    #[test]
    fn pk_key_sql_expr_escapes_a_real_separator_in_the_value() {
        assert_eq!(
            composite_key_escape_sql("t.\"sku\"::text"),
            "replace(replace(t.\"sku\"::text, chr(30), chr(30) || chr(30)), chr(31), \
             chr(30) || chr(31))"
        );
        assert_eq!(
            pk_key_sql_expr(&not_null_pk(&["sku"]), Some("t")),
            "t.\"sku\"::text",
            "an arity-1 not-null key renders the bare column text, unescaped"
        );
        assert_eq!(
            pk_key_sql_expr(&not_null_pk(&["warehouse", "sku"]), Some("t")),
            format!(
                "array_to_string(array[{}, {}], chr(31))",
                composite_key_escape_sql("t.\"warehouse\"::text"),
                composite_key_escape_sql("t.\"sku\"::text"),
            )
        );
    }

    /// The two escape layers are two independent, per-concern decisions —
    /// "can this column be `NULL`?" (issue #110, per column, from the
    /// catalog) and "does this key have a delimiter to protect?" (issue
    /// #200, per key, from its arity) — nested, never merged. This pins all
    /// four combinations on the SQL side: [`null_key_escape_sql`] is always
    /// the *inner* layer and [`composite_key_escape_sql`] always the outer
    /// one, so the `coalesce` still sees SQL `NULL` as `NULL` and the outer
    /// `replace`s only ever see a non-`NULL` string.
    #[test]
    fn pk_key_sql_expr_nests_the_null_and_separator_escapes_per_column() {
        // Arity 1: neither key gets the separator escape; only the nullable
        // one gets the NULL layer.
        assert_eq!(
            pk_key_sql_expr(&not_null_pk(&["id"]), Some("t")),
            "t.\"id\"::text"
        );
        assert_eq!(
            pk_key_sql_expr(&pk(&["sku"]), Some("t")),
            null_key_escape_sql("t.\"sku\"::text")
        );

        // Arity 2, one column of each nullability: the not-null column gets
        // only the separator escape, the nullable one gets it wrapped
        // around its NULL escape.
        let mixed = vec![
            PrimaryKeyColumn {
                name: "warehouse".to_string(),
                data_type: "text".to_string(),
                nullable: false,
            },
            PrimaryKeyColumn {
                name: "sku".to_string(),
                data_type: "text".to_string(),
                nullable: true,
            },
        ];
        assert_eq!(
            pk_key_sql_expr(&mixed, Some("t")),
            format!(
                "array_to_string(array[{}, {}], chr(31))",
                composite_key_escape_sql("t.\"warehouse\"::text"),
                composite_key_escape_sql(&null_key_escape_sql("t.\"sku\"::text")),
            )
        );
    }

    /// The Rust-side twin of the test above, end to end: one composite key
    /// holding *both* concerns at once — a nullable column carrying a real
    /// `NULL` (and, separately, a real [`NULL_KEY_SENTINEL`] value) next to
    /// a not-null column carrying a real [`COMPOSITE_KEY_SEPARATOR`] and
    /// [`KEY_PART_ESCAPE`]. Every combination must round-trip exactly, which
    /// is what "the two layers touch disjoint characters" actually buys.
    #[test]
    fn both_escape_layers_compose_in_one_composite_key() {
        let mixed = vec![
            PrimaryKeyColumn {
                name: "warehouse".to_string(),
                data_type: "text".to_string(),
                nullable: false,
            },
            PrimaryKeyColumn {
                name: "sku".to_string(),
                data_type: "text".to_string(),
                nullable: true,
            },
        ];

        // A NULL nullable component beside a not-null component whose value
        // genuinely contains the separator and the escape character.
        let not_null_value = "w\u{1f}1\u{1e}x";
        let key = join_pk_key([Cow::Borrowed(not_null_value), encode_key_part(None)]);
        assert_eq!(
            split_ok(&mixed, &key)
                .iter()
                .map(Option::as_deref)
                .collect::<Vec<_>>(),
            vec![Some(not_null_value), None]
        );

        // And the fully adversarial shape: every reserved character of
        // either scheme in both components, with the nullable one holding a
        // real (non-NULL) U+0001 that must not be read back as the NULL
        // sentinel.
        for nullable_value in [
            "\u{1}",
            "\u{1}\u{1}",
            "\u{1}\u{1f}\u{1e}\u{1}",
            "",
            "\u{1e}\u{1}\u{1f}",
        ] {
            let key = join_pk_key([
                Cow::Borrowed(not_null_value),
                encode_key_part(Some(nullable_value)),
            ]);
            assert_eq!(
                split_ok(&mixed, &key)
                    .iter()
                    .map(Option::as_deref)
                    .collect::<Vec<_>>(),
                vec![Some(not_null_value), Some(nullable_value)],
                "{nullable_value:?} beside a separator-valued not-null column \
                 must survive both escape layers"
            );
            assert_ne!(
                key,
                join_pk_key([Cow::Borrowed(not_null_value), encode_key_part(None)]),
                "a genuine {nullable_value:?} must never encode to the NULL key"
            );
        }

        // Arity 1 for the nullable column on its own: the NULL layer still
        // applies (issue #110 has no arity exemption) while the separator
        // layer still does not (issue #200's arity-1 exemption), so a value
        // carrying both a real sentinel and a real separator round-trips.
        let one_nullable = pk(&["sku"]);
        for value in ["\u{1}\u{1f}\u{1e}", "\u{1f}", "\u{1}"] {
            let key = join_pk_key([encode_key_part(Some(value))]);
            assert_eq!(
                split_ok(&one_nullable, &key)
                    .iter()
                    .map(Option::as_deref)
                    .collect::<Vec<_>>(),
                vec![Some(value)]
            );
        }
        assert_eq!(
            split_ok(&one_nullable, &join_pk_key([encode_key_part(None)]))
                .iter()
                .map(Option::as_deref)
                .collect::<Vec<_>>(),
            vec![None]
        );
    }

    #[test]
    fn qualified_target_table_combines_target_schema_and_name() {
        assert_eq!(
            qualified_target_table("public", &def()),
            "\"public\".\"order_totals\""
        );
        assert_eq!(
            qualified_target_table("analytics", &def()),
            "\"analytics\".\"order_totals\""
        );
    }
}
