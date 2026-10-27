//! Postgres-backed catalog for transform definitions.
//!
//! **Schema choice**: a definition is stored as its original source text
//! (`transform_definitions.definition_text`) rather than a serialized AST.
//! Reading it back re-parses via [`super::parse`], reusing the grammar's
//! own parser instead of standing up a second, independently-drifting
//! serialization format for the same information. The tradeoff is a
//! re-parse on every read; given definitions are small and read
//! infrequently relative to the source-row volume they govern, that's the
//! right side to take the cost on.
//!
//! Every source table has exactly one monotonically increasing version, in
//! `source_table_versions`, that definition creation bumps in the same
//! transaction as the insert. That version lives in a real, lockable row
//! (not a computed value) so stage 05's version fence can `FOR SHARE`/
//! `FOR UPDATE` it directly.
//!
//! v1 definitions are immutable: this module only exposes creation and
//! read, no update/delete.
//!
//! **Source and target identity are both fully-qualified
//! (ADR-0011).** `transform_definitions.source_table`/`target_table` and
//! `source_table_versions.source_table` hold `def.source`/`def.target`
//! resolved to their `schema.table` identity exactly once, at
//! definition-acceptance time (`create_definition_inner`'s
//! [`resolve_source_schema_in_txn`] call for the source side,
//! `Config::target_schema`/`intake::qualify` for the target
//! side), never the bare spelling the grammar parsed. Every read of any of
//! these columns downstream must treat the value as already-qualified and
//! must not re-resolve it — see [`resolve_source_schema_in_txn`]'s own doc
//! comment for exactly why re-resolving a qualified value fails outright
//! rather than merely being redundant. A handful of read sites deliberately
//! stay bare regardless — `column_dependents`, `definition_by_target`, and
//! every `app.rs` read reachable through
//! `docs/decisions/0003-quarantine-storage-and-api.md`'s
//! `transform.column` addressing scheme — because their callers only ever
//! have the bare name that addressing scheme itself accepts (the *grammar*
//! has an explicit `schema.table` spelling for `TRANSFORM`/
//! `FROM`, but `transform.column` quarantine addressing is a separate,
//! unrelated syntax this module's own read sites still speak, and it hasn't
//! grown qualified-target syntax of its own) or because re-exposing the
//! qualified spelling through that addressing scheme would misparse a real
//! transform address as a column one (see each function's own doc comment);
//! these match
//! `target_table`'s bare table-name suffix via `split_part` rather than the
//! qualified column directly.
//!
//! **`schema_nodes`/`schema_edges` key on qualified identity too
//! (ADR-0011).** `public.posts` and `archive.posts` are distinct nodes
//! with independent edges — `resolve_node_in_txn`/`resolve_node` and
//! `reject_if_table_cycle` all require an already-qualified `table_name`
//! now (see [`resolve_graph_identity_in_txn`]'s doc comment for how a bare
//! `def.source` gets there before either is ever called).
//!
//! **Bare target-table suffixes are still enforced globally unique, just no
//! longer by `target_table`'s own `unique` constraint.** Qualifying
//! `target_table` narrowed that constraint to the qualified spelling
//! only, which would otherwise let e.g. `public.foo` and `custom.foo`
//! coexist as two live definitions — exactly the ambiguity every
//! `split_part`-based bare-suffix read site above assumes can't happen.
//! `create_definition_inner` re-closes that gap itself, at write time,
//! rejecting a new definition with [`CatalogError::TargetTableSuffixCollision`]
//! if its qualified target would collide with another live definition's
//! bare suffix under a different schema — see that check's own comment for
//! why this stays provisional even with the explicit
//! `schema.table` grammar and the graph qualification:
//! every bare-suffix reader above is still bare-keyed, so the guard
//! still applies uniformly regardless of whether a colliding target arrived
//! via explicit qualification or bare resolution. This is now double-enforced,
//! not merely application-level: `transform_definitions_target_suffix_idx`
//! is a real Postgres
//! expression unique index on `split_part(target_table, '.', 2)`, the same
//! DB-level backstop `target_table`'s own `unique` constraint already is for
//! exact-qualified-name collisions — it closes the race two concurrent
//! `create_definition` calls could otherwise win against each other's
//! same-transaction-invisible, still-uncommitted inserts. The app-level
//! check stays the primary path (a typed, name-carrying error beats a raw
//! constraint violation for the common, non-racing case); the rare
//! insert-time failure that check can't see is caught and translated back
//! into the same [`CatalogError::TargetTableSuffixCollision`] rather than
//! surfacing as an opaque [`CatalogError::Db`].

use std::collections::{HashMap, HashSet};
use std::fmt;

use crate::error_code::{self, ErrorCode};
use crate::float::FloatWidth;
use crate::integer::IntWidth;
use crate::pool::{Pool, quote_ident};
use tokio_postgres::GenericClient;
use tokio_postgres::types::Type;

use super::ast::{
    AlterClause, AlterTransform, Expr, FieldDef, KeySpace, TransformDef, ValueType,
    render_definition_text,
};
use super::backfill::BackfillError;
use super::ddl::{self, DdlError};
use super::error::ParseError;
use super::grammar::{self, CURRENT_GRAMMAR_VERSION};
#[cfg(any(test, feature = "internals"))]
use super::model::SchemaEdge;
use super::model::{Definition, EdgeKind, NodeKind, SchemaNode, TransformStatus};
use super::parser::parse;
use super::pg_type::PgType;
use super::validate::{
    KeyColumnRole, NondeterministicCollationTextFunction, NondeterministicKeyCollation,
    ValidationError, infer_field_types, reject_primary_key_named_fields, validate,
    validate_qualified_columns,
};

/// Why creating or reading a definition failed. [`CatalogError::code`]
/// reports a stable, coarse [`ErrorCode`] category for this error alongside
/// its `Display` message — see `docs/decisions/0008-public-api-design.md`, decision 3.
#[derive(Debug)]
#[non_exhaustive]
pub enum CatalogError {
    /// The source text failed to parse.
    Parse(ParseError),
    /// The parsed definition failed validation.
    Validate(ValidationError),
    /// Acquiring a connection or running a query against Postgres failed.
    Db(tokio_postgres::Error),
    /// Acquiring a connection from the pool failed.
    Pool(crate::error::Error),
    /// A definition's persisted `source_columns` jsonb held a value other
    /// than `"numeric"`/`"text"`/`"boolean"`/`"uuid"` for some column —
    /// meaning the row was written by something other than
    /// `create_definition_inner`, the only writer, which only ever encodes
    /// `ValueType`'s variants.
    UnknownValueType { column: String, text: String },
    /// An intake step failed: resolving a qualified table name, rebuilding
    /// the readers of a table, or checking that a source is change-keyed.
    Backfill(crate::intake::IntakeError),
    /// `def.source` doesn't resolve to any schema on this connection's
    /// search path (see `resolve_source_schema_in_txn`) — the table was
    /// dropped, renamed, or never existed under that bare name. Also a table
    /// dropped between resolving it and a later check that reads it.
    SourceTableNotFound(String),
    /// A table a definition names exists, but the role
    /// Trellis connects as can't use it: it lacks `USAGE` on the
    /// table's schema, holds no privilege on the table itself (it neither
    /// owns it nor inherits a role that does), or both. Postgres's name
    /// resolution and `information_schema` hide such a table, so without this
    /// check define reports it as missing, or blames its shape.
    TableNotAccessible {
        /// `schema.table`.
        table: String,
        /// The role the session runs as (`current_user`).
        role: String,
        /// The table's owner, the role to grant membership in.
        owner: String,
        lacks_schema_usage: bool,
        lacks_table_privilege: bool,
    },
    /// A 1-1 definition's source was given another row-identity key after
    /// define: its target is keyed by the old one, so a
    /// resume can't rebuild into it. `revalidate` refuses it; the repair
    /// is to restore the key, or to drop the definition and define it again.
    SourceKeyChanged {
        source_table: String,
        /// The target's key columns: the source key define found.
        target_key: Vec<String>,
        /// The source's key columns now.
        source_key: Vec<String>,
    },
    /// A `GROUP BY` or `JOIN` target's source lacks the index its `self_check`
    /// walk pages by (`super::source_index`). Refused by define and by
    /// a whole-transform resume, unless the definition says `WITHOUT SOURCE
    /// INDEX`; `self_check` refuses it later too. The message names each
    /// table and columns and a `create index` to run.
    SourceIndexMissing {
        target: String,
        missing: Vec<super::source_index::SourceIndex>,
    },
    /// A definition's ledger table doesn't exist, though define creates one
    /// for every 1-1 definition. Only Trellis writes its
    /// tables, so an operator dropped it; the build's `start` reads it and
    /// fails, and the apply writes into it. `check_ledger_table` refuses it,
    /// and Trellis doesn't re-create the table: the repair is to drop the
    /// definition and define it again.
    LedgerTableMissing {
        target: String,
        /// The table, schema-qualified.
        ledger_table: String,
    },
    /// A definition's target table doesn't exist, though define creates it.
    /// Only Trellis writes its targets, so an operator dropped it; the
    /// rebuild a resume starts writes into it and would fail. A whole-transform
    /// resume refuses it (`check_target_table`), and Trellis doesn't
    /// re-create the table: the repair is to drop the definition and define
    /// it again.
    TargetTableMissing { target: String },
    /// A definition's target schema is a Trellis catalog schema: this
    /// instance's own (`own`), which target tables stay out of so they can't
    /// collide with Trellis's tables, or a schema holding another instance's
    /// `trellis_instance` table. The target and a 1-1 target's ledger would
    /// live there, so the other instance's uninstall (`drop
    /// schema ... cascade`) would drop them, and a dump of one instance's
    /// schema would carry the other's data. Refused by define, and by a
    /// resume (`revalidate`) of a definition whose target schema has since
    /// become one. The repair is a target in an ordinary schema: `TRANSFORM
    /// <schema>.<target>` with such a schema, after a `DROP TRANSFORM` for a
    /// definition a resume refused.
    TargetInCatalogSchema {
        /// The definition's target, `schema.table`.
        target: String,
        /// The target's schema.
        schema: String,
        /// Whether `schema` is this instance's catalog schema, rather than
        /// another instance's.
        own: bool,
    },
    /// This definition's resolved, qualified target (`{target_schema}.{def.target}`)
    /// shares a bare table-name suffix with a *different* qualified target
    /// some other still-persisted definition already uses — e.g.
    /// `public.foo` alongside `custom.foo`, most plausibly from
    /// `Config::target_schema` changing between deploys and a `TRANSFORM
    /// ... TARGET foo` being redeclared under it.
    /// `transform_definitions.target_table` stores the qualified spelling, so
    /// `target_table text not null unique` (the catalog migration)
    /// only enforces uniqueness of *that* spelling, not the bare suffix
    /// — checked and rejected here, in
    /// `create_definition_inner`, rather than at any individual read site,
    /// because every `split_part(target_table, '.', 2)`-keyed reader
    /// (`definition_by_target`, `app.rs`'s status/quarantine polls,
    /// `generative`'s `unsettled_definitions`) assumes that suffix is
    /// globally unique and has no way to safely cope with two definitions
    /// colliding under it — see this variant's `Display` message for the
    /// operator-facing explanation. (`dependents_of` joins on the qualified
    /// name, so it's not in this list.)
    ///
    /// Double-enforced: this
    /// application-level pre-check is the primary path (it reports a
    /// clean, typed error naming both spellings — see
    /// `docs/decisions/0008-public-api-design.md` on why that beats a raw
    /// Postgres unique-violation for callers), but it only ever sees its own
    /// transaction's snapshot, so two concurrent `create_definition` calls
    /// resolving different-schema targets for the same bare suffix could
    /// each pass it and both commit. `transform_definitions_target_suffix_idx`
    /// is the real
    /// DB-level guard that closes that race; `create_definition_inner`
    /// catches the rare insert-time unique-violation against it and
    /// translates it into this same variant (with `existing: None` — see the
    /// field's own doc comment) rather than letting it surface as a raw
    /// [`CatalogError::Db`].
    ///
    /// Still provisional even with the grammar's
    /// explicit `schema.table` spelling: an operator *can* now write
    /// `TRANSFORM custom.foo FROM ...` to unambiguously address `custom.foo`
    /// as distinct from `public.foo`, but every bare-suffix reader named
    /// above is still bare-suffix-keyed — so this variant is still raised
    /// for an explicitly-qualified target exactly as it is for a
    /// resolved-bare one (the check it backs has no branch on how the target
    /// was qualified, only on the final qualified string and bare suffix);
    /// only a different addressing scheme can relax this.
    ///
    /// Raised from two different places, both folding into this one variant
    /// since callers only need one type to match on: `create_definition_inner`'s
    /// pre-check (`existing: Some(_)`, the common case — the colliding row is
    /// still visible in this transaction's own snapshot, so its qualified
    /// spelling can be reported) and a genuine insert-time race against
    /// `transform_definitions_target_suffix_idx`
    /// (`existing: None`
    /// — a concurrent transaction's insert that the pre-check's snapshot
    /// couldn't see committed first, so by the time this transaction's own
    /// insert fails on the index, it has no further query available inside
    /// its now-aborted transaction to learn what it lost to).
    TargetTableSuffixCollision {
        /// The bare table-name suffix both spellings share.
        target: String,
        /// The qualified spelling this rejected definition resolved to.
        requested: String,
        /// The qualified spelling already persisted by another live
        /// definition under the same bare suffix, when known. `None` only
        /// for the insert-time race path above, where the aborted
        /// transaction has no way left to look it up.
        existing: Option<String>,
    },
    /// Registering this definition would create its target table, but a
    /// relation with that qualified name already exists: a user
    /// table, view, materialized view, foreign table, or another Trellis
    /// definition's target. Trellis never adopts a table it didn't create —
    /// the user's schema belongs to the user (ADR-0005), and `DROP` drops a
    /// definition's target (ADR-0014), so adopting a user table would hand
    /// that table to a later `DROP`.
    TargetTableExists {
        /// The qualified `schema.table` spelling that already exists.
        table: String,
    },
    /// The definition's source would be captured by triggers,
    /// but they can't key that table's changes (see `change_keyed`): it
    /// has no primary key, or isn't a plain table (a partitioned table, say).
    /// The case that motivated this is another Trellis instance's aggregate
    /// target: this instance's own targets are exempt, since their writes
    /// reach their readers in the writing transaction rather than through
    /// capture.
    SourceNotChangeKeyed { source_table: String },
    /// ADR-0017: a partitioned root can't be read as the definition asks
    /// (`super::partition_tree::PartitionRefusal`; every message names the
    /// column or the limit).
    PartitionedSource(super::partition_tree::PartitionRefusal),
    /// Row-level security on a table the definition reads
    /// (its source, or a `JOIN`'s joined table) applies to a role Trellis
    /// reads the table as, so its policies would filter Trellis's reads
    /// (see `super::row_security`). Or row-level security on
    /// its newly created target applies to the session's role, which would
    /// filter Trellis's writes (`super::row_security::RowSecurity::target`).
    /// Not supported.
    RowSecurityApplies(super::row_security::RowSecurity),
    /// A logical-replication subscription replicates into a
    /// table the definition reads. The subscription's apply worker fires only row-level
    /// triggers, so Trellis's capture never sees its changes (see
    /// `super::subscription`). Not supported.
    Subscribed(super::subscription::Subscribed),
    /// `install_definition`'s target-table DDL failed.
    Ddl(DdlError),
    /// A build step failed with something other than
    /// `BackfillError::Unsupported`: an `ALTER TRANSFORM`'s added-column
    /// build.
    DirectBackfill(BackfillError),
    /// `super::lifecycle::pause_transform` was asked to pause a target with
    /// no corresponding `transform_definitions` row at all. Its
    /// sibling verb, `drop`, deliberately treats the same situation as an
    /// idempotent success instead — see
    /// `super::lifecycle::drop_transform`'s doc comment for why the two
    /// differ.
    TransformNotFound { transform: String },
    /// `super::lifecycle::drop_transform` was asked to drop a definition
    /// that is not frozen (ADR-0014). There is no direct
    /// live-to-gone edge in the lifecycle: quiescing through a pause is a
    /// precondition, so the removal never has to reason about a claim-time
    /// fold still dispatching to the target. Carries the status it actually
    /// found, so the caller knows whether to pause first or to wait out a
    /// backfill.
    TransformNotPaused {
        transform: String,
        status: TransformStatus,
    },
    /// A still-live definition depends on the definition being dropped
    /// (ADR-0014's "Drops go in reverse dependency order — no
    /// cascade"). Trellis refuses rather than cascading, and names the
    /// blockers so the order to retire them in is explicit rather than
    /// something the operator has to reconstruct.
    DependentsBlockDrop {
        /// What the caller asked to drop: a bare transform name, or
        /// `target.column` for an `ALTER TRANSFORM ... DROP <field>`.
        subject: String,
        /// The bare target names of the live definitions standing in the way.
        dependents: Vec<String>,
        /// For every blocker whose exact reference resolved to
        /// one specific column of the subject (column-granularity
        /// dependency edges — `column_dependents`), a human-readable
        /// `"<dependent>.<field> reads <subject>.<column>"` detail line.
        /// This is strictly additive precision over `dependents` alone, not
        /// a replacement. Empty whenever no blocker's reference could be
        /// resolved this precisely.
        column_detail: Vec<String>,
    },
    /// `alter_transform` was asked to edit a target that doesn't apply
    /// (`staging::build::takes_field_build`): one still
    /// `waiting_to_backfill`, `backfilling` behind an old-path build, or
    /// frozen (`paused`/`quarantined`). Retry once it applies.
    ///
    /// Also returned when a new definition's source is another
    /// definition's target whose build hasn't finished (one that isn't
    /// applying yet). A target's own initial build
    /// writes it outside the target-mutation seam
    /// (`staging::target_mutations`), so a reader attached mid-build would
    /// never hear about the rows the rest of the build writes.
    TransformNotLive {
        transform: String,
        status: TransformStatus,
    },
    /// `alter_transform` was asked to `ALTER <field> AS <expr>` a field
    /// that isn't one of the target's declared calculated fields — unlike
    /// `DROP`ping an absent field (idempotent success) or `ADD`ing one that
    /// already exists (idempotent iff the formula matches), there is no
    /// reading under which altering a field that was never there is a
    /// no-op: there is nothing to replace.
    AlterFieldNotFound {
        transform: String,
        field: String,
        declared: Vec<String>,
    },
    /// `alter_transform` was asked to `ADD <expr> AS <field>` a field name
    /// that already exists with a *different* formula than `<expr>` —
    /// distinct from the idempotent case (same formula, a no-op success):
    /// `ADD` only ever introduces a field, so re-adding an existing name
    /// under a new formula is ambiguous with `ALTER` and refused rather than
    /// silently reinterpreted as one.
    AlterFieldAlreadyExists { transform: String, field: String },
    /// `ALTER TRANSFORM` was asked to edit a target/expression shape outside
    /// this release's scope — see `alter_transform`'s own doc comment for
    /// exactly what's covered. Not a rejection of the definition's *shape* in
    /// the way [`CatalogError::Validate`] is (the edit would very likely be
    /// accepted by `define`); it's this operation's own, narrower coverage
    /// declining, with a message that says so.
    UnsupportedAlter(String),
    /// A define, `ALTER TRANSFORM` or `DROP TRANSFORM` waited for the
    /// column-pause lock (`crate::locks::lock_column_pauses`) past its
    /// transaction's `lock_timeout`. Nothing changed: retry the call.
    ColumnPauseLockTimeout(crate::locks::ColumnPauseLockTimeout),
}

impl CatalogError {
    /// This error's stable, coarse [`ErrorCode`] category (`docs/decisions/0008-public-api-design.md`,
    /// decision 3). Delegates to the wrapped error's own `code()` wherever
    /// one nests here ([`CatalogError::Parse`], [`CatalogError::Validate`],
    /// [`CatalogError::Pool`], [`CatalogError::Backfill`], [`CatalogError::Ddl`],
    /// [`CatalogError::DirectBackfill`]) rather than hardcoding one category
    /// for a whole variant.
    pub fn code(&self) -> ErrorCode {
        match self {
            CatalogError::Parse(err) => err.code(),
            CatalogError::Validate(err) => err.code(),
            CatalogError::Db(err) => error_code::classify_pg_error(err),
            CatalogError::Pool(err) => err.code(),
            // Persisted data corruption — written by something other than
            // this module's own writer.
            CatalogError::UnknownValueType { .. } => ErrorCode::Internal,
            CatalogError::Backfill(err) => err.code(),
            CatalogError::SourceTableNotFound(_) => ErrorCode::NotFound,
            // A setup the definition can't run under, like
            // `RowSecurityApplies`: the fix is a grant.
            CatalogError::TableNotAccessible { .. } => ErrorCode::Validation,
            CatalogError::SourceKeyChanged { .. } => ErrorCode::Validation,
            CatalogError::SourceIndexMissing { .. } => ErrorCode::Validation,
            CatalogError::LedgerTableMissing { .. } => ErrorCode::Validation,
            CatalogError::TargetTableMissing { .. } => ErrorCode::Validation,
            // A target the definition may not have, like a row-security
            // table: the fix is a different schema in the definition.
            CatalogError::TargetInCatalogSchema { .. } => ErrorCode::Validation,
            // Collides with existing state (another live definition's
            // persisted target), not a structural/semantic rejection of this
            // definition's own text.
            CatalogError::TargetTableSuffixCollision { .. } => ErrorCode::Conflict,
            CatalogError::TargetTableExists { .. } => ErrorCode::Conflict,
            CatalogError::SourceNotChangeKeyed { .. } => ErrorCode::Validation,
            CatalogError::PartitionedSource(_) => ErrorCode::Validation,
            CatalogError::RowSecurityApplies(_) => ErrorCode::Validation,
            CatalogError::Subscribed(_) => ErrorCode::Validation,
            CatalogError::Ddl(err) => err.code(),
            CatalogError::DirectBackfill(err) => err.code(),
            CatalogError::TransformNotFound { .. } => ErrorCode::NotFound,
            // Both are "the world isn't in the state this operation needs",
            // not a rejection of the request's own shape — the same category
            // `ApplyError::TransformNotPaused` reports for its own
            // precondition.
            CatalogError::TransformNotPaused { .. } => ErrorCode::Conflict,
            CatalogError::DependentsBlockDrop { .. } => ErrorCode::Conflict,
            // Same "the world isn't in the state this operation needs"
            // category as `TransformNotPaused` above.
            CatalogError::TransformNotLive { .. } => ErrorCode::Conflict,
            CatalogError::AlterFieldNotFound { .. } => ErrorCode::NotFound,
            CatalogError::AlterFieldAlreadyExists { .. } => ErrorCode::Conflict,
            CatalogError::UnsupportedAlter(_) => ErrorCode::Validation,
            // The statement itself is under-specified — the fix is to
            // rewrite it with a schema qualifier, not to change any state.
            // A wait that ran out, not a fault: the call changed nothing.
            CatalogError::ColumnPauseLockTimeout(_) => ErrorCode::Timeout,
        }
    }
}

impl CatalogError {
    /// Whether this is define's own refusal of the schema as it stands, as
    /// opposed to a refusal only a resume makes ([`CatalogError::SourceKeyChanged`],
    /// [`CatalogError::LedgerTableMissing`],
    /// [`CatalogError::TargetTableMissing`]), where define would
    /// accept the definition and the repair is to drop it and define it again.
    /// A resume's message says which it is: a define refusal's says to fix the
    /// schema and resume. [`CatalogError::TargetInCatalogSchema`] is define's
    /// refusal too, but counts as a resume's own here, because fixing the
    /// schema would mean removing another instance's catalog from the
    /// schema that holds the target, whose uninstall drops the target with
    /// it: its repair is also to drop the definition and define it again,
    /// in an ordinary schema, and its own message says so.
    pub(crate) fn is_define_refusal(&self) -> bool {
        !matches!(
            self,
            CatalogError::SourceKeyChanged { .. }
                | CatalogError::LedgerTableMissing { .. }
                | CatalogError::TargetTableMissing { .. }
                | CatalogError::TargetInCatalogSchema { .. }
        )
    }
}

impl fmt::Display for CatalogError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CatalogError::Parse(err) => write!(f, "failed to parse transform definition: {err}"),
            CatalogError::Validate(err) => {
                write!(f, "definition failed validation: {err}")
            }
            CatalogError::Db(err) => {
                write!(f, "transform catalog database error: ")?;
                crate::error::write_pg_error(f, err)
            }
            CatalogError::Pool(err) => write!(f, "failed to acquire a connection: {err}"),
            CatalogError::UnknownValueType { column, text } => write!(
                f,
                "column '{column}' has an unrecognized persisted value type '{text}'"
            ),
            CatalogError::Backfill(err) => {
                write!(f, "failed to backfill the definition's source table: {err}")
            }
            CatalogError::SourceTableNotFound(table) => {
                write!(f, "source table \"{table}\" not found on the search path")
            }
            CatalogError::TableNotAccessible {
                table,
                role,
                owner,
                lacks_schema_usage,
                lacks_table_privilege,
            } => {
                let schema = schema_of(table).unwrap_or(table);
                write!(
                    f,
                    "table \"{table}\" exists, but role \"{role}\", which Trellis runs as, "
                )?;
                match (lacks_schema_usage, lacks_table_privilege) {
                    (true, true) => write!(
                        f,
                        "lacks USAGE on schema \"{schema}\", and isn't a member of \"{owner}\", \
                         the table's owner, or granted any privilege on the table"
                    )?,
                    (true, false) => write!(f, "lacks USAGE on schema \"{schema}\"")?,
                    _ => write!(
                        f,
                        "isn't a member of \"{owner}\", the table's owner, or granted any \
                         privilege on the table"
                    )?,
                }
                write!(
                    f,
                    ". Trellis needs USAGE on the schema of every source, and must own each \
                     source table or be a member of the role that owns it, with INHERIT: see \
                     \"One Trellis role\" in docs/recommendations.md"
                )
            }
            CatalogError::SourceKeyChanged {
                source_table,
                target_key,
                source_key,
            } => write!(
                f,
                "the primary key of {source_table} is now ({}), but this definition's target \
                 is keyed by ({}). Restore the key, or `DROP TRANSFORM` and define it again",
                source_key.join(", "),
                target_key.join(", ")
            ),
            CatalogError::SourceIndexMissing { target, missing } => {
                write!(f, "{}", super::source_index::refusal_text(target, missing))
            }
            CatalogError::LedgerTableMissing {
                target,
                ledger_table,
            } => write!(
                f,
                "the ledger table of {target}, {ledger_table}, doesn't exist, but define \
                 creates one for every definition. Trellis doesn't re-create it: `DROP \
                 TRANSFORM` and define it again"
            ),
            CatalogError::TargetTableMissing { target } => write!(
                f,
                "the target table {target} doesn't exist, but define creates it. Trellis \
                 doesn't re-create it: `DROP TRANSFORM` and define it again"
            ),
            CatalogError::TargetInCatalogSchema {
                target,
                schema,
                own,
            } => {
                if *own {
                    write!(
                        f,
                        "target {target} is in schema \"{schema}\", this instance's catalog \
                         schema, which holds Trellis's own tables and no target"
                    )?;
                } else {
                    write!(
                        f,
                        "target {target} is in schema \"{schema}\", which holds another Trellis \
                         instance's catalog (it has a `trellis_instance` table): that \
                         instance's uninstall would drop the target and its ledger"
                    )?;
                }
                write!(
                    f,
                    ". Put the target in an ordinary schema with `TRANSFORM <schema>.<target>` \
                     (or `TRELLIS_TARGET_SCHEMA`, for an unqualified one)"
                )?;
                // Only another instance's catalog can appear under a
                // definition after define (see `is_define_refusal`).
                if !*own {
                    write!(
                        f,
                        "; a transform already defined in \"{schema}\" can't resume there: \
                         `DROP TRANSFORM` it and define it again in an ordinary schema"
                    )?;
                }
                Ok(())
            }
            CatalogError::TargetTableSuffixCollision {
                target,
                requested,
                existing: Some(existing),
            } => write!(
                f,
                "target table \"{target}\" is ambiguous: this definition would persist \
                 \"{requested}\", but \"{existing}\" already exists under the same bare \
                 table name — two definitions cannot share a bare target-table name under \
                 different schemas"
            ),
            CatalogError::TargetTableSuffixCollision {
                target,
                requested,
                existing: None,
            } => write!(
                f,
                "target table \"{target}\" is ambiguous: this definition would persist \
                 \"{requested}\", but another definition was concurrently created under the \
                 same bare table name — two definitions cannot share a bare target-table \
                 name under different schemas"
            ),
            CatalogError::TargetTableExists { table } => write!(
                f,
                "target table \"{table}\" already exists; Trellis only writes to target tables \
                 it creates, so choose a different target name or drop or rename the existing \
                 relation"
            ),
            CatalogError::SourceNotChangeKeyed { source_table } => write!(
                f,
                "Trellis can't capture source table \"{source_table}\"'s changes: it must be a \
                 plain table (not partitioned, and not a partition or in an inheritance \
                 hierarchy) with a primary key. If it is another Trellis instance's aggregate \
                 target, define this transform in that instance instead: \
                 an instance propagates writes to its own targets without capturing them"
            ),
            CatalogError::PartitionedSource(refusal) => write!(f, "{refusal}"),
            CatalogError::RowSecurityApplies(rls) => write!(f, "{rls}"),
            CatalogError::Subscribed(sub) => write!(f, "{sub}"),
            CatalogError::Ddl(err) => write!(f, "failed to create target table: {err}"),
            CatalogError::DirectBackfill(err) => write!(f, "direct backfill failed: {err}"),
            CatalogError::TransformNotFound { transform } => {
                write!(f, "no transform named '{transform}' is registered")
            }
            CatalogError::TransformNotPaused { transform, status } => write!(
                f,
                "'{transform}' is {}, not paused; a definition must be paused before it can \
                 be dropped, so the apply path is quiesced before its target goes away",
                status.as_str()
            ),
            CatalogError::DependentsBlockDrop {
                subject,
                dependents,
                column_detail,
            } => {
                write!(
                    f,
                    "cannot drop '{subject}': {} still derive{} from it — retire {} first \
                     (Trellis refuses rather than cascading)",
                    dependents.join(", "),
                    if dependents.len() == 1 { "s" } else { "" },
                    if dependents.len() == 1 { "it" } else { "them" },
                )?;
                if !column_detail.is_empty() {
                    write!(f, " ({})", column_detail.join("; "))?;
                }
                Ok(())
            }
            CatalogError::TransformNotLive { transform, status } => write!(
                f,
                "'{transform}' is {}, not live; only a fully-built, live definition can be \
                 edited with ALTER TRANSFORM or read by another transform",
                status.as_str()
            ),
            CatalogError::AlterFieldNotFound {
                transform,
                field,
                declared,
            } => write!(
                f,
                "transform '{transform}' declares no field '{field}' to ALTER; it declares: {}",
                if declared.is_empty() {
                    "(none)".to_string()
                } else {
                    declared.join(", ")
                }
            ),
            CatalogError::AlterFieldAlreadyExists { transform, field } => write!(
                f,
                "transform '{transform}' already has a field '{field}' with a different \
                 formula; use ALTER '{field}' AS <expr> to change it, not ADD"
            ),
            CatalogError::UnsupportedAlter(detail) => {
                write!(f, "unsupported ALTER TRANSFORM: {detail}")
            }
            CatalogError::ColumnPauseLockTimeout(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for CatalogError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            CatalogError::Parse(err) => Some(err),
            CatalogError::Validate(err) => Some(err),
            CatalogError::Db(err) => Some(err),
            CatalogError::Pool(err) => Some(err),
            CatalogError::UnknownValueType { .. } => None,
            CatalogError::Backfill(err) => Some(err),
            CatalogError::SourceTableNotFound(_) => None,
            CatalogError::TableNotAccessible { .. } => None,
            CatalogError::SourceKeyChanged { .. } => None,
            CatalogError::SourceIndexMissing { .. } => None,
            CatalogError::LedgerTableMissing { .. } => None,
            CatalogError::TargetTableMissing { .. } => None,
            CatalogError::TargetInCatalogSchema { .. } => None,
            CatalogError::TargetTableSuffixCollision { .. } => None,
            CatalogError::TargetTableExists { .. } => None,
            CatalogError::SourceNotChangeKeyed { .. } => None,
            CatalogError::PartitionedSource(_) => None,
            CatalogError::RowSecurityApplies(_) => None,
            CatalogError::Subscribed(_) => None,
            CatalogError::Ddl(err) => Some(err),
            CatalogError::DirectBackfill(err) => Some(err),
            CatalogError::TransformNotFound { .. } => None,
            CatalogError::TransformNotPaused { .. } => None,
            CatalogError::DependentsBlockDrop { .. } => None,
            CatalogError::TransformNotLive { .. } => None,
            CatalogError::AlterFieldNotFound { .. } => None,
            CatalogError::AlterFieldAlreadyExists { .. } => None,
            CatalogError::UnsupportedAlter(_) => None,
            CatalogError::ColumnPauseLockTimeout(err) => Some(err),
        }
    }
}

impl From<ParseError> for CatalogError {
    fn from(err: ParseError) -> Self {
        CatalogError::Parse(err)
    }
}

impl From<ValidationError> for CatalogError {
    fn from(err: ValidationError) -> Self {
        CatalogError::Validate(err)
    }
}

impl From<crate::locks::ColumnPauseLockError> for CatalogError {
    fn from(err: crate::locks::ColumnPauseLockError) -> Self {
        match err {
            crate::locks::ColumnPauseLockError::Timeout(err) => {
                CatalogError::ColumnPauseLockTimeout(err)
            }
            crate::locks::ColumnPauseLockError::Db(err) => CatalogError::Db(err),
        }
    }
}

impl From<tokio_postgres::Error> for CatalogError {
    fn from(err: tokio_postgres::Error) -> Self {
        CatalogError::Db(err)
    }
}

impl From<crate::error::Error> for CatalogError {
    fn from(err: crate::error::Error) -> Self {
        CatalogError::Pool(err)
    }
}

impl From<crate::intake::IntakeError> for CatalogError {
    fn from(err: crate::intake::IntakeError) -> Self {
        CatalogError::Backfill(err)
    }
}

/// Test fixture: registers a definition exactly as [`install_definition`]
/// registers every one (`waiting_to_backfill`, no source read), then stands
/// in for its build in-call: it enumerates the source into the ring as
/// image-less `Recompute` rows and flips the definition `live` in one
/// transaction. `source_columns` maps the
/// source table's known columns to their [`ValueType`] (see
/// [`super::validate::validate`]).
///
/// No production path calls this: registration never reads the source. It
/// waits for no writer, so the caller must be the only writer of the source.
/// The target table must already exist (see this module's doc comment).
///
/// `pool.target_schema()`, not a parameter of this function's own: this entry
/// point never runs target-table DDL itself, so there is no
/// sibling DDL call for a separately-threaded `target_schema` argument to
/// drift from. See [`crate::pool::Pool::target_schema`]'s own doc comment.
#[cfg(any(test, feature = "internals"))]
pub async fn create_definition(
    pool: &Pool,
    source_text: &str,
    source_columns: &HashMap<String, ValueType>,
) -> Result<Definition, CatalogError> {
    refuse_join_fixture(source_text);
    let mut definition = create_definition_inner(
        pool,
        source_text,
        source_columns,
        TransformStatus::WaitingToBackfill,
        pool.target_schema(),
        None,
    )
    .await?;
    let mut client = pool.get().await?;
    let txn = client.transaction().await?;
    crate::intake::harness::enumerate_and_append(&txn, &definition.source_table).await?;
    txn.execute(
        "update transform_definitions set status = $1 where id = $2",
        &[&TransformStatus::Live.as_str(), &definition.id],
    )
    .await?;
    txn.commit().await?;
    definition.status = TransformStatus::Live;
    Ok(definition)
}

/// Test fixture: like [`create_definition`], but reads nothing. The
/// definition and its version bump are persisted `live` with no backfill of
/// any kind, for a test or benchmark that builds the target itself or
/// needs none. No production path calls this: every real definition is
/// built by a Re-derive build (ADR-0002).
#[cfg(any(test, feature = "test-util"))]
pub async fn create_definition_without_backfill(
    pool: &Pool,
    source_text: &str,
    source_columns: &HashMap<String, ValueType>,
) -> Result<Definition, CatalogError> {
    refuse_join_fixture(source_text);
    // `pool.target_schema()` — see [`create_definition`]'s own doc comment
    // for why this entry point reads it off `pool` rather than taking its own
    // `target_schema` parameter.
    create_definition_inner(
        pool,
        source_text,
        source_columns,
        TransformStatus::Live,
        pool.target_schema(),
        None,
    )
    .await
}

/// The test fixtures above flip what they register `live` without a build,
/// and a `JOIN` reaches `live` only through its build, so they refuse
/// one. Text that doesn't parse is left to the fixture's own error.
#[cfg(any(test, feature = "test-util"))]
fn refuse_join_fixture(source_text: &str) {
    if let Ok(def) = parse(source_text) {
        assert!(
            !matches!(def.key_space, KeySpace::Join(_)),
            "a JOIN definition can't be registered `live`: only its build makes one live"
        );
    }
}

/// The front door real callers use to stand up a new definition:
/// validates it, creates the target table, and records the definition.
///
/// **The target table is created in the same transaction that records the
/// definition,** so a registration that fails anywhere leaves
/// neither a catalog row nor a table behind and can simply be retried. A
/// relation that already exists under the target's qualified name, of any
/// kind, is refused with [`CatalogError::TargetTableExists`] and left
/// untouched: Trellis never adopts a table it didn't create (ADR-0005), since
/// `DROP` would later drop it (ADR-0014). No build path creates the target
/// itself.
///
/// **Registration reads no source rows (ADR-0002).** Every
/// definition is persisted [`TransformStatus::WaitingToBackfill`] and this
/// returns, so its latency doesn't depend on the size of any table the
/// definition reads. Its build runs in the background: the staging worker's
/// reconcile pass starts a Re-derive build once the source's capture is
/// installed or widened (see [`crate::capture::reconcile`] and
/// `staging::build::start_ready_builds`), and drain threads execute it. A
/// caller polls [`crate::Trellis::status`] until it reads `live`.
///
/// **Why nothing is folded into a target still being built.**
/// [`dependents_of`]/[`transforms_for_source`] filter to applying
/// definitions ([`TransformStatus::is_applying`]), so no build path can have
/// a live CDC delta folded into it before its build has started. From the
/// start the definition applies, and the build's chunks and the pages agree
/// through the ledger entry's `basis`: a delta for a commit a chunk's
/// snapshot already saw is skipped rather than counted a second time (I1).
pub async fn install_definition(
    pool: &Pool,
    source_text: &str,
    source_columns: &HashMap<String, ValueType>,
    target_schema: &str,
) -> Result<Definition, CatalogError> {
    let def: TransformDef = parse(source_text)?;

    // ADR-0011: an explicit schema on either
    // side is trusted outright rather than resolved — checked here, before
    // any DDL work runs, so a bogus explicit spelling fails fast with a
    // friendly `ValidationError` instead of surfacing as a confusing DDL
    // failure partway through this function, or as the key read
    // below reporting the missing table as one with no primary key.
    // `create_definition_inner` (below) repeats both checks inside its own
    // transaction; that repeat is the *authoritative* one — it's the only
    // check the test-fixture entry points ([`create_definition`]/
    // [`create_definition_without_backfill`], which never call this
    // function) ever run. This one is a pure fail-fast nicety for the far
    // more common `install_definition` path, redundant-but-harmless on the
    // path that also reaches `create_definition_inner`.
    if let Some(schema) = &def.explicit_source_schema {
        confirm_qualified_source(&**pool.get().await?, schema, &def.source).await?;
    }

    // Validate *before* any DDL is generated or executed, for every key-space.
    // DDL generation type-infers each target column from the same
    // expressions the validator checks, so an invalid definition reaching DDL
    // first surfaces whatever incidental error inference happens to hit —
    // masking the real validation error — and, worse, can leave a target table
    // behind for a definition that is then rejected. `create_definition`/
    // `create_definition_without_backfill` validate again below; that repeat is
    // cheap and keeps those entry points safe when called directly.
    //
    // A 1-1 field named after a source primary key column is
    // refused ahead of `validate`, so it gets that error rather than
    // `CalculatedFieldShadowsSourceColumn`'s "make it a bare passthrough".
    // The same gate as the helper's own, checked first so that a definition
    // with no field named like a source column doesn't resolve its source
    // here at all.
    if let KeySpace::OneToOne = def.key_space
        && def
            .fields
            .iter()
            .any(|f| source_columns.contains_key(&f.name))
    {
        let qualified_source = resolve_source_to_define(pool, &def).await?;
        let client = pool.get().await?;
        reject_fields_named_after_primary_key(
            &**client,
            &qualified_source,
            &def.fields,
            source_columns,
        )
        .await?;
    }
    validate_for_define(pool, &def, source_columns).await?;

    // An explicit `TRANSFORM <schema>.<target>` spelling overrides
    // `target_schema` (`Config::target_schema`, or this function's own
    // caller-supplied override) outright for the remainder of this call:
    // every DDL/backfill step below, and `create_definition_inner`'s
    // own persistence, all thread this (possibly-overridden) binding through
    // rather than the original parameter — so the physical target table this
    // function's DDL step is about to create and the qualified identity
    // `create_definition_inner` persists can never name different schemas.
    //
    // No matching existence check on the target here: registration is about
    // to *create* it, and `create_definition_inner` refuses one that already
    // exists inside the transaction that creates it.
    let target_schema = effective_target_schema(&def, target_schema);

    // ADR-0011: resolved once, here, and threaded through every
    // DDL step below — see [`resolve_source_for_install`]'s own
    // doc comment for why this function needs its own copy rather than
    // waiting for `create_definition_inner`'s later, authoritative one.
    let qualified_source = resolve_source_to_define(pool, &def).await?;
    // Fail fast, before any DDL, on a source that is another
    // definition's not-yet-live target (see `reject_non_live_upstream`).
    reject_non_live_upstream(&**pool.get().await?, &qualified_source).await?;

    // `reject_unkeyed_source` fails fast in both arms, before the
    // target's DDL is rendered. `create_definition_inner` repeats it as the
    // authoritative check.
    //
    // Only *rendered* here: rendering reads source column types through
    // `pool`, but the statement itself runs inside
    // `create_definition_inner`'s transaction, so the table
    // commits or rolls back together with the catalog row.
    let target_ddl = match &def.key_space {
        KeySpace::Join(_) => {
            // Each side is held to the source rules (a primary key,
            // another definition's target, or a partitioned root whose
            // partial key holds its partition key). The target holds only
            // the declared columns; its hidden tables are created with the
            // definition row, named by its id.
            let sides = JoinSides::resolve(pool, &def).await?;
            reject_non_live_upstream(&**pool.get().await?, &sides.joined).await?;
            for side in [&sides.from, &sides.joined] {
                reject_unkeyed_source(&**pool.get().await?, side, &def).await?;
            }
            super::join::target_ddl(
                &def,
                &ddl::qualified_target_table(target_schema, &def),
                &sides.from_columns,
                &sides.joined_columns,
            )?
        }
        KeySpace::OneToOne => {
            // The 1-1 target table's own primary key mirrors the
            // source's in full — at whatever arity the source declares it —
            // rather than narrowing to a single column. `target_table_ddl`
            // renders a composite `pk` as a real, multi-column `primary key
            // (...)` constraint.
            let pk = ddl::source_primary_key(pool, &qualified_source)
                .await
                .map_err(CatalogError::Ddl)?;
            reject_unkeyed_source(&**pool.get().await?, &qualified_source, &def).await?;
            ddl::target_table_ddl(
                pool,
                &def,
                target_schema,
                &pk,
                source_columns,
                &qualified_source,
            )
            .await
            .map_err(CatalogError::Ddl)?
        }
        KeySpace::Aggregate { .. } => {
            // Live apply reads the source's primary key through
            // `ddl::source_primary_key` for an aggregate too, and a key type
            // it refuses halts its readers there. Refuse it here instead,
            // before any DDL, exactly as the 1-1 arm above does.
            ddl::source_primary_key(pool, &qualified_source)
                .await
                .map_err(CatalogError::Ddl)?;
            reject_unkeyed_source(&**pool.get().await?, &qualified_source, &def).await?;
            // An aggregate outside the admitted set is
            // refused by family, before the DDL (which only renders what the
            // group basis keeps).
            check_aggregate_admitted(&**pool.get().await?, &def, &qualified_source).await?;
            ddl::aggregate_target_table_ddl(&def, target_schema, source_columns)
                .map_err(CatalogError::Ddl)?
        }
    };

    // ADR-0002: registration reads no source rows. The
    // definition waits for the staging worker's pass, which starts its build.
    //
    // `target_schema` — the exact value the DDL above was rendered under — is
    // threaded straight through rather than re-derived from `pool` (the
    // persisted qualification must never drift from what the DDL
    // builds).
    create_definition_inner(
        pool,
        source_text,
        source_columns,
        TransformStatus::WaitingToBackfill,
        target_schema,
        Some(&target_ddl),
    )
    .await
}

/// Every applying definition (`live` or `catching_up`) that reads `table`
/// ([`tables_read_by`]), in id order: the readers a rewrite of `table`
/// outside the target-mutation seam has to rebuild
/// (`intake::catch_up::park_target_catchup_if_read`).
pub(crate) async fn applying_readers(
    client: &impl GenericClient,
    table: &str,
) -> Result<Vec<i64>, CatalogError> {
    let rows = client
        .query(
            "select id, source_table, join_table \
             from transform_definitions where status = any($1) order by id",
            &[&TransformStatus::applying()],
        )
        .await?;
    let mut readers = Vec::new();
    for row in rows {
        if tables_read_by(row.get(1), row.get(2))
            .iter()
            .any(|t| t == table)
        {
            readers.push(row.get::<_, i64>(0));
        }
    }
    Ok(readers)
}

/// Whether any definition that isn't frozen reads `table`
/// ([`tables_read_by`]):
/// every status in [`TransformStatus::dispatchable`], so one still waiting
/// for its build, or under any build, counts as well as one that applies.
///
/// The drain asks this only when `table`'s primary key can't be used
/// (`staging::apply::compute`), to tell a table whose rows no
/// reader needs, which it skips, from one a reader still needs, which halts
/// the page and pauses its readers (`staging::halt`). A definition
/// waiting for its build, or under a chunked or direct one, doesn't apply
/// the rows yet, but its build will read them. Only a frozen definition
/// gives them up, and its resume rebuilds it
/// (`staging::quarantine::resume_transform`).
pub(crate) async fn has_unfrozen_reader(
    client: &impl GenericClient,
    table: &str,
) -> Result<bool, CatalogError> {
    let rows = client
        .query(
            "select source_table, join_table \
             from transform_definitions where status = any($1) order by id",
            &[&TransformStatus::dispatchable()],
        )
        .await?;
    for row in rows {
        if tables_read_by(row.get(0), row.get(1))
            .iter()
            .any(|t| t == table)
        {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Every table a definition reads, fully qualified, sorted and
/// deduplicated: its source (`qualified_source`, the persisted
/// `transform_definitions.source_table`) and a `JOIN`'s joined table
/// (`joined`, the persisted `join_table`).
fn tables_read_by(qualified_source: String, joined: Option<String>) -> Vec<String> {
    let mut tables = vec![qualified_source];
    tables.extend(joined);
    tables.sort_unstable();
    tables.dedup();
    tables
}

/// Every definition's status as an operator reads it (`Trellis::status`,
/// `Trellis::definitions`), by id (ADR-0002's "What `live`
/// promises"): its persisted status, except that a `live` definition reading
/// an upstream that isn't `live` reports [`TransformStatus::CatchingUp`].
///
/// An upstream is the definition whose target is a table this one reads
/// ([`tables_read_by`]'s set). While that upstream is paused,
/// quarantined, rebuilding or catching up, its target doesn't reflect its
/// own source, so neither does anything derived from it, and a watermark
/// token wouldn't wait for what's missing. The rule is transitive: an
/// upstream that only *reports* `catching_up` because of its own upstream
/// counts as not `live` too.
///
/// Derived when read, not stored. A stored transition would have to follow
/// every status change of every upstream (pause, resume, quarantine, drop,
/// each build's completion) down the chain, under locks, and a reader's own
/// completion would have to check its upstreams as well. The derivation needs only the
/// catalog: one read of the definitions. The
/// reported status changes nothing else: the definition keeps applying, as
/// `catching_up` always does.
pub(crate) async fn reported_statuses(
    client: &impl GenericClient,
) -> Result<HashMap<i64, TransformStatus>, CatalogError> {
    Ok(reported_statuses_with_causes(client).await?.0)
}

/// [`reported_statuses`] plus, per definition, the paused upstream columns
/// that make it report `catching_up`, each `target.column`, sorted:
/// the columns it reads directly and those its upstream readers do. The map
/// holds only definitions with a cause.
///
/// A column of a 1-1 target is paused when it has a `column_status` row
/// (its fuse tripped, or `PAUSE TRANSFORM t.col`). Its 1-1 readers' fields
/// are paused with it by the cascade, but any other reader (an aggregate, a
/// `JOIN`) keeps applying over the column's frozen values and stays `live`,
/// exact against the upstream target and stale against the source. A `live`
/// definition reading such a column therefore reports `catching_up` too,
/// until the column's resume writes through to it. Nothing is paused or
/// rebuilt. A reader whose reading field the cascade paused already shows
/// that pause, so it isn't a cause here: the column the cascade paused is.
pub(crate) async fn reported_statuses_with_causes(
    client: &impl GenericClient,
) -> Result<(HashMap<i64, TransformStatus>, HashMap<i64, Vec<String>>), CatalogError> {
    let mut nodes = status_nodes(client).await?;
    add_paused_column_causes(client, &mut nodes).await?;
    Ok(derive_reported(&nodes))
}

/// [`reported_statuses`] with paused upstream columns left out: `catching_up`
/// only for a definition with an upstream that isn't stored `live`, down the
/// chain. What a resume waits on: an upstream that is rebuilding or
/// paused can still rewrite its target, but one that only reads a paused
/// column applies as usual, so a reader rebuilt over it is exactly as current
/// as a `live` reader of it, and a column paused for good mustn't
/// hold the resume.
pub(crate) async fn reported_statuses_ignoring_column_pauses(
    client: &impl GenericClient,
) -> Result<HashMap<i64, TransformStatus>, CatalogError> {
    Ok(derive_reported(&status_nodes(client).await?).0)
}

/// Every definition as [`derive_reported`] sees it, without paused columns.
async fn status_nodes(client: &impl GenericClient) -> Result<Vec<StatusNode>, CatalogError> {
    let rows = client
        .query(
            "select id, target_table, source_table, status, join_table \
             from transform_definitions order by id",
            &[],
        )
        .await?;
    let mut nodes = Vec::with_capacity(rows.len());
    for row in rows {
        let status_text: String = row.get(3);
        let status = TransformStatus::from_persisted(&status_text).unwrap_or_else(|| {
            panic!("transform_definitions.status held unrecognized value '{status_text}'")
        });
        let source: String = row.get(2);
        // Only a `live` definition's reported status can differ from its
        // persisted one, so only its reads are needed.
        let mut reads = Vec::new();
        if status == TransformStatus::Live {
            reads = tables_read_by(source, row.get(4));
        }
        nodes.push(StatusNode {
            id: row.get(0),
            target: row.get(1),
            status,
            reads,
            paused_columns: Vec::new(),
        });
    }
    Ok(nodes)
}

/// Records on each `live` node the paused upstream columns it reads
/// ([`reported_statuses_with_causes`]), each once.
async fn add_paused_column_causes(
    client: &impl GenericClient,
    nodes: &mut [StatusNode],
) -> Result<(), CatalogError> {
    let paused: Vec<(String, String)> = client
        .query(
            "select transform_table, column_name from column_status \
             order by transform_table, column_name",
            &[],
        )
        .await?
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    if !paused.is_empty() {
        let paused_set: HashSet<&(String, String)> = paused.iter().collect();
        let by_bare: HashMap<String, usize> = nodes
            .iter()
            .enumerate()
            .map(|(i, n)| {
                (
                    n.target
                        .split_once('.')
                        .map_or(n.target.clone(), |(_, t)| t.to_string()),
                    i,
                )
            })
            .collect();
        for (table, column) in &paused {
            for dep in column_dependents_any_keyspace(client, table, column).await? {
                if paused_set.contains(&dep) {
                    continue;
                }
                let cause = format!("{table}.{column}");
                // A reader with several fields (or a `GROUP BY` key) reading
                // the column is listed once per field.
                if let Some(&i) = by_bare.get(&dep.0)
                    && nodes[i].status == TransformStatus::Live
                    && !nodes[i].paused_columns.contains(&cause)
                {
                    nodes[i].paused_columns.push(cause);
                }
            }
        }
    }
    Ok(())
}

/// One definition as [`derive_reported_statuses`] sees it.
struct StatusNode {
    id: i64,
    /// Its qualified target table.
    target: String,
    /// Its persisted status.
    status: TransformStatus,
    /// Every table it reads, qualified (needed only when `status` is `live`).
    reads: Vec<String>,
    /// The paused columns (`target.column`) of upstream 1-1 targets it
    /// reads, only for a `live` definition.
    paused_columns: Vec<String>,
}

/// [`reported_statuses`]' rule over the loaded catalog: starting from every
/// persisted status, moves a `live` definition to `catching_up` while any
/// table it reads is the target of a definition not reported `live`, until
/// nothing moves. Each round only ever moves `live` to `catching_up`, so it
/// ends within one round per definition, and a cycle (which the catalog
/// can't hold anyway) can't make it loop.
#[cfg(test)]
fn derive_reported_statuses(nodes: &[StatusNode]) -> HashMap<i64, TransformStatus> {
    derive_reported(nodes).0
}

/// [`derive_reported_statuses`], and the paused upstream columns behind each
/// `catching_up` it reports: a `live` definition reading a paused
/// column of an upstream target reports `catching_up`, and so does its
/// reader in turn. A definition's causes are its own paused columns and
/// those of every upstream that isn't reported `live`.
fn derive_reported(
    nodes: &[StatusNode],
) -> (HashMap<i64, TransformStatus>, HashMap<i64, Vec<String>>) {
    let owner: HashMap<&str, i64> = nodes.iter().map(|n| (n.target.as_str(), n.id)).collect();
    let mut reported: HashMap<i64, TransformStatus> =
        nodes.iter().map(|n| (n.id, n.status)).collect();
    loop {
        let mut moved = false;
        for node in nodes {
            if reported[&node.id] != TransformStatus::Live {
                continue;
            }
            let upstream_not_live = node.reads.iter().any(|table| {
                owner
                    .get(table.as_str())
                    .is_some_and(|up| *up != node.id && reported[up] != TransformStatus::Live)
            });
            if upstream_not_live || !node.paused_columns.is_empty() {
                reported.insert(node.id, TransformStatus::CatchingUp);
                moved = true;
            }
        }
        if !moved {
            break;
        }
    }
    let mut causes: HashMap<i64, Vec<String>> = nodes
        .iter()
        .filter(|n| !n.paused_columns.is_empty())
        .map(|n| (n.id, n.paused_columns.clone()))
        .collect();
    loop {
        let mut grew = false;
        for node in nodes {
            for table in &node.reads {
                let Some(&up) = owner.get(table.as_str()) else {
                    continue;
                };
                if up == node.id || reported[&up] == TransformStatus::Live {
                    continue;
                }
                let inherited = causes.get(&up).cloned().unwrap_or_default();
                let mine = causes.entry(node.id).or_default();
                for cause in inherited {
                    if !mine.contains(&cause) {
                        mine.push(cause);
                        grew = true;
                    }
                }
            }
        }
        if !grew {
            break;
        }
    }
    causes.retain(|_, v| !v.is_empty());
    for v in causes.values_mut() {
        v.sort_unstable();
    }
    (reported, causes)
}

/// Refuses (with [`CatalogError::TransformNotLive`]) a new definition whose
/// source, `qualified_source`, is the target of another definition whose
/// build hasn't finished yet: one that isn't
/// [`TransformStatus::is_applying`]. A target's initial build
/// (`defs::backfill`, the chunk queue) writes it directly, outside the
/// target-mutation seam that tells a chained reader about every other target
/// write, so a reader attached mid-build would silently miss whatever the
/// rest of the build writes. Wait for the upstream definition to go live
/// (or at least reach `catching_up`), then define the chained one.
async fn reject_non_live_upstream(
    client: &impl GenericClient,
    qualified_source: &str,
) -> Result<(), CatalogError> {
    let Some(row) = client
        .query_opt(
            "select split_part(target_table, '.', 2), status from transform_definitions \
             where target_table = $1",
            &[&qualified_source],
        )
        .await?
    else {
        return Ok(());
    };
    let status_text: String = row.get(1);
    let status = TransformStatus::from_persisted(&status_text).unwrap_or_else(|| {
        panic!("transform_definitions.status held unrecognized value '{status_text}'")
    });
    // A `catching_up` upstream's build has finished: every write to its
    // target goes through the seam now.
    if status.is_applying() {
        return Ok(());
    }
    Err(CatalogError::TransformNotLive {
        transform: row.get(0),
        status,
    })
}

/// Whether `qualified_table` is some definition's target table.
pub(crate) async fn is_definition_target(
    client: &impl GenericClient,
    qualified_table: &str,
) -> Result<bool, CatalogError> {
    Ok(client
        .query_one(
            "select exists (select 1 from transform_definitions where target_table = $1)",
            &[&qualified_table],
        )
        .await?
        .get(0))
}

/// Edits an already-registered 1-1 transform's calculated fields in place —
/// `ALTER TRANSFORM`'s catalog-side implementation (ADR-0015), reached
/// through [`crate::Trellis::apply`] like every other definition-changing
/// statement.
///
/// **Scope, this release.** Only a [`KeySpace::OneToOne`] target is
/// supported: any edit against an aggregate or `JOIN` target is refused with
/// [`CatalogError::UnsupportedAlter`]. `DROP` alone never needs this
/// restriction in principle (it recomputes nothing), but is held to the same
/// gate for simplicity: building an aggregate-target column-add/drop path
/// (which would need to reason about the hidden `__{f}_sum`/`__{f}_count`
/// partials `ddl::create_aggregate_target_table` maintains) is real
/// follow-up work, not a fundamental blocker.
///
/// **Idempotency, per clause** (ADR-0015's "Edits are idempotent"): re-`ADD`ing
/// a field that already exists with the exact same formula, re-`ALTER`ing a
/// field to the formula it already has, and `DROP`ping a field already
/// absent are each a no-op. The clauses are netted against each other
/// first ([`plan_alter`]): a field added and dropped in the same
/// edit, or altered and altered back, changes nothing, and the refusals
/// below that look at what the edit adds, alters or drops judge that net
/// change, not each clause. If the net change is empty, the whole call is a
/// no-op success — no version bump, no DDL, nothing written — matching
/// pause/resume/drop's own "a replayed migration must be safe in both
/// directions" discipline.
///
/// **The field build.** The call only registers the edit:
/// the DDL, the catalog row and the version fence below, and, when it adds
/// or alters a field, a field build of every such field, in the same
/// transaction (`staging::build::start_field_build`). It moves the
/// definition `live -> backfilling` (one already under a Re-derive build
/// keeps building) and enqueues one plan job for all the fields, so the
/// source is read once however many fields change, by the drain workers in
/// the background. `apply` returns once that commits, and the definition
/// reads `backfilling` until the build's last chunk is done, then `live`.
/// The changed fields apply from the commit, as a new definition does under
/// the Re-derive build: every page after it writes them, and each
/// chunk rewrites them under the keys' entry lock, so no catch-up is needed.
/// Only a definition that applies takes the edit
/// (`staging::build::takes_field_build`). An edit that only drops fields
/// builds nothing.
///
/// **The version fence.** This call bumps both `transform_definitions
/// .definition_version` (an audit-visible, monotonic counter on the edited
/// row itself, per the ADR) and `source_table_versions.version` for the
/// definition's source table — the *second* of which is what actually fences
/// anything: it is the exact value `staging::apply::ApplyPlan`'s Phase 2
/// captures and Phase 3 (`apply_and_mark_drained_many`) re-checks under `FOR
/// SHARE`, so a drain worker whose in-flight batch loaded the *old* field
/// list before this call's transaction commits is forced to hit
/// `ApplyError::VersionFenceMiss` and reload the catalog — landing on the
/// *new* field list, with any `awaiting_capture` pause already visible —
/// rather than applying a change with a field list this edit replaced. This
/// is the same fence every *first* `create_definition_inner` already bumps,
/// for exactly this reason; nothing new is invented here, only reused for an
/// edit instead of only a define.
///
/// **Cycle detection.** The merged field list (existing fields, with `ADD`/
/// `DROP`/`ALTER` applied) is run through [`validate`] — the same validator,
/// same `super::validate::detect_cycle` call, [`create_definition_inner`]
/// runs for a first `define` — before anything is written. The table-level
/// whole-graph check ([`reject_if_table_cycle`]) is deliberately *not*
/// re-run: an edit changes neither `def.source` nor `def.target`, so the
/// table-level graph this call would check against is unchanged from the
/// one already checked when the target was first defined — re-running it
/// would only re-confirm the same edge is still not a cycle.
///
/// **The live schema.** The merged definition is validated
/// against the source's columns as they are when the edit commits
/// ([`validate_edit`]), read in the edit's transaction under the version
/// fence and the definition's row lock, not against the columns recorded when
/// the definition was defined: a column the host added since is readable, and
/// a field named after it is refused, as at a define; a column it dropped, or
/// retyped to a type a field reading it no longer validates over, refuses an
/// edit that leaves that field until the edit drops or changes it. A field
/// the edit builds can't read a column whose type the host changed since the
/// definition recorded it, even one it validates over: its build would
/// compute it over the live type and its applies over the recorded one, and
/// the edit can't move the recorded type under the unedited fields that read
/// the column too. A resume re-types the definition first. The edit records
/// the live type of each column it reads that the host added since
/// ([`record_added_source_columns`]), since a field's builds and applies infer
/// and cast through the recorded types.
///
/// **The `DROP <field>` refusal.** Requires column-granularity dependency
/// edges finer than a whole-transform drop's table-level check, which
/// [`column_dependents_any_keyspace`] provides and which is
/// used here as the *entire* correctness check (not just a naming nicety the
/// way [`super::lifecycle::drop_transform`]'s own use of it is): a dropped
/// field refuses, naming the specific reader, iff some other definition's
/// field or `GROUP BY` key resolves to exactly this `(target, field)` pair.
///
/// **The capture widen.** An edited field can read a source
/// column the old formula didn't, which the source's capture triggers may
/// not image yet. The staging worker widens them in the background, not on
/// this call's path, once it can take the table's lock, and a row the old
/// capture function staged lacks the column: evaluating the field over it
/// fails with `MissingColumn` and quarantines the key. So such an edit holds
/// its fields out of Apply with `column_status` rows marked
/// `awaiting_capture`, and the field build's plan job releases them before
/// it plans a chunk, once the installed capture images every column the
/// definition reads and the source's capture gate is clear, so every row the
/// old function staged has drained without the fields
/// (`staging::build`'s "Field builds"). An edit that reads only columns the
/// definition already read applies at once: those columns have been imaged
/// since the definition was dispatched.
pub async fn alter_transform(
    pool: &Pool,
    alter: &AlterTransform,
) -> Result<AlterOutcome, CatalogError> {
    let Some(current) = definition_by_target(pool, &alter.target).await? else {
        return Err(CatalogError::TransformNotFound {
            transform: alter.target.clone(),
        });
    };
    // The edit is keyed by the bare name throughout, whichever spelling the
    // statement used.
    let normalized = AlterTransform {
        target: current.def.target.clone(),
        clauses: alter.clauses.clone(),
    };
    let alter = &normalized;
    // Re-checked under the row lock below, with the definition's build.
    if !current.status.is_applying() && current.status != TransformStatus::Backfilling {
        return Err(CatalogError::TransformNotLive {
            transform: alter.target.clone(),
            status: current.status,
        });
    }
    if !matches!(current.def.key_space, KeySpace::OneToOne) {
        return Err(CatalogError::UnsupportedAlter(format!(
            "'{}' is an aggregate (GROUP BY) transform; ALTER TRANSFORM only supports 1-1 \
             transforms in this release",
            alter.target
        )));
    }

    let AlterPlan {
        fields,
        real_adds,
        real_alters,
        real_drops,
    } = plan_alter(&alter.target, &current.def.fields, &alter.clauses)?;
    let names = |fields: &[FieldDef]| fields.iter().map(|f| f.name.clone()).collect::<Vec<_>>();
    let added = names(&real_adds);
    let altered = names(&real_alters);
    let dropped = real_drops.clone();

    if real_adds.is_empty() && real_alters.is_empty() && real_drops.is_empty() {
        // Every clause was an idempotent no-op — ADR-0015's "edits are
        // idempotent in both directions" — or the clauses cancelled (an
        // `ADD` and a `DROP` of one name): no version bump, no
        // DDL, nothing written.
        return Ok(AlterOutcome {
            definition: current,
            added,
            dropped,
            altered,
        });
    }

    let merged = TransformDef {
        fields,
        ..current.def.clone()
    };

    // The fields the edit's field build rewrites: each added or altered one,
    // and every field that reads one of them by alias, directly
    // or through others, since its value moves with theirs.
    //
    // An added field an existing one already named, or a dropped one a
    // remaining field still names, moves no reader's value: such a name is
    // a source column, and a field named after a source column must pass
    // that column through (`ValidationError::CalculatedFieldShadowsSourceColumn`).
    let build_fields: Vec<String> = {
        let mut edited: HashSet<String> = real_adds
            .iter()
            .chain(&real_alters)
            .map(|f| f.name.clone())
            .collect();
        super::eval::AliasReaders::of(&merged).close(&mut edited);
        merged
            .fields
            .iter()
            .filter(|f| edited.contains(&f.name))
            .map(|f| f.name.clone())
            .collect()
    };

    // Fail fast, before the transaction's fence waits on any drain: the
    // edited definition against the source as it is now, as a define would
    // read it. Authoritative only after the locks below, which
    // read it again.
    let (field_types, pre_source_columns) = {
        let client = pool.get().await?;
        let edited = validate_edit(
            &**client,
            &current.source_table,
            &merged,
            &real_adds,
            &build_fields,
            &current.source_columns,
        )
        .await?;
        (edited.field_types, edited.source_columns)
    };

    // Pre-transaction dependency check for every real `DROP`: the
    // column-granularity refusal.
    // Best-effort here (re-checked for real under the row lock below — a
    // definition can register a brand-new dependent
    // between this check and the transaction's commit).
    for field in &real_drops {
        check_no_column_dependents(pool, &alter.target, field).await?;
    }

    // Pre-transaction type check for every real `ALTER` (see
    // [`check_alter_column_types`]): a type change that would rewrite the
    // target is refused, as ADR-0015 promises no edit does. Best-effort
    // here, same as the `DROP` dependents check just above (re-checked for
    // real under the row lock below).
    {
        let pre_client = pool.get().await?;
        check_alter_column_types(
            &**pre_client,
            &AlterTypeContext {
                merged: &merged,
                source_table: &current.source_table,
                source_columns: &pre_source_columns,
                field_types: &field_types,
                qualified_target: &current.target_table,
                display_target: &alter.target,
            },
            &real_alters,
        )
        .await?;
    }

    let target_ident = ddl::qualified_target_table_ident(&current.target_table);

    let mut client = pool.get().await?;
    let txn = client.transaction().await?;

    // The same version-fence bump `create_definition_inner` makes for a
    // brand-new definition — see this function's own doc comment. It is
    // this transaction's first lock. The bump waits for every drain in
    // flight that holds the row `for share` (a drain's Phase 3 takes it
    // first and holds it to its commit), so it must hold nothing such a
    // drain could be waiting on, directly or through a third transaction.
    // Taking the target's `ACCESS EXCLUSIVE` lock first deadlocks against
    // the drain itself, which row-locks the target; taking the definition
    // row first deadlocks against a drain queued on a transaction that
    // wants that row, such as a build's completion. A drain
    // that already holds the row finishes first, and one that arrives later
    // waits for this commit and then misses the fence. A refusal below
    // rolls the bump back with everything else.
    txn.query_one(
        "insert into source_table_versions (source_table, version) \
         values ($1, 1) \
         on conflict (source_table) \
         do update set version = source_table_versions.version + 1 \
         returning version",
        &[&current.source_table],
    )
    .await?;

    // The column-pause lock, exclusive, after the fence and before the
    // definition row: an edit that builds a field can pause it at birth
    // (`pause_readers_of_paused_columns` below) and one that drops a field
    // deletes its pause rows and edges, so both write pause state, and what
    // it reads of its upstreams' pauses must not change under it. See
    // `crate::locks::lock_column_pauses` for the order. A no-op edit takes
    // nothing.
    if !build_fields.is_empty() || !real_drops.is_empty() {
        crate::locks::lock_column_pauses(
            &*txn,
            pool.schema(),
            crate::locks::ColumnPauseLock::Exclusive,
            crate::locks::ColumnPauseOp::Alter,
        )
        .await?;
    }

    // Row-lock the definition and recheck its status: a pause, a drop, or
    // another concurrent alter must not interleave between the checks above
    // and this commit (as `lifecycle::drop_transform` does for its own
    // precondition).
    let row = txn
        .query_opt(
            "select status, definition_version, build from transform_definitions \
             where id = $1 for update",
            &[&current.id],
        )
        .await?;
    let Some(row) = row else {
        return Err(CatalogError::TransformNotFound {
            transform: alter.target.clone(),
        });
    };
    let status_text: String = row.get(0);
    let status = TransformStatus::from_persisted(&status_text).unwrap_or_else(|| {
        panic!("transform_definitions.status held unrecognized value '{status_text}'")
    });
    // A field build needs a definition that applies
    // (`staging::build::takes_field_build`): `live`, or
    // already under a Re-derive build, which it joins. An edit that only
    // drops fields builds nothing; it takes the same definitions.
    let build: Option<String> = row.get(2);
    let builds_fields = !build_fields.is_empty();
    if !crate::staging::build::takes_field_build(status, build.as_deref()) {
        return Err(CatalogError::TransformNotLive {
            transform: alter.target.clone(),
            status,
        });
    }
    let old_version: i64 = row.get(1);
    let new_version = old_version + 1;

    // The edited definition against the source's live columns, read under
    // the fence and the row lock: the same read and rules a define makes, so
    // an edit never commits a definition define would refuse.
    // The columns recorded for the definition are consulted only for a
    // column whose type the host changed since, which a field this edit
    // builds can't read.
    let recorded = source_columns_in(&*txn, current.id).await?;
    let EditedSchema {
        source_columns,
        field_types,
    } = validate_edit(
        &*txn,
        &current.source_table,
        &merged,
        &real_adds,
        &build_fields,
        &recorded,
    )
    .await?;
    record_added_source_columns(&*txn, current.id, &merged, &recorded, &source_columns).await?;

    // Re-check every real `DROP`'s dependents inside the transaction,
    // against this transaction's own view of `transform_definitions` — the
    // same ordering discipline `lifecycle::drop_transform`'s
    // `dependency_blockers` call already follows.
    for field in &real_drops {
        check_no_column_dependents_in_txn(&txn, &alter.target, field).await?;
    }

    // Re-check every real `ALTER`'s type-change refusal inside the
    // transaction, under the row lock just taken — same ordering discipline
    // as the `DROP` dependents recheck just above, and the reason no `ALTER
    // TABLE ... TYPE` DDL for any of these fields has been (or ever will be)
    // issued below: this runs strictly before every DDL loop in this
    // function, drops and adds included, so a refusal here still means
    // *nothing* about this edit has touched the target table yet.
    check_alter_column_types(
        &*txn,
        &AlterTypeContext {
            merged: &merged,
            source_table: &current.source_table,
            source_columns: &source_columns,
            field_types: &field_types,
            qualified_target: &current.target_table,
            display_target: &alter.target,
        },
        &real_alters,
    )
    .await?;

    // An added or altered field's `STRPOS`/
    // `REGEXP_COUNT` can't read a nondeterministic-collation column, exactly
    // as at install. Before any DDL, like the rechecks above.
    let edited: Vec<FieldDef> = real_adds.iter().chain(&real_alters).cloned().collect();
    assert_collation_safe_text_functions(&txn, &merged, &current.source_table, Some(&edited))
        .await?;

    for field in &real_drops {
        txn.batch_execute(&format!(
            "alter table {target_ident} drop column if exists {}",
            quote_ident(field)
        ))
        .await?;
    }
    // An added bare passthrough takes its source column's type, as at
    // `DEFINE`: the same rule, not a second copy of it.
    let add_pg_types = ddl::field_column_pg_types(
        &*txn,
        &merged,
        &real_adds,
        &source_columns,
        &field_types,
        &current.source_table,
    )
    .await
    .map_err(|err| match err {
        DdlError::Db(err) => CatalogError::Db(err),
        DdlError::Pool(err) => CatalogError::Pool(err),
        err => CatalogError::Ddl(err),
    })?;
    for field in &real_adds {
        let pg_type = &add_pg_types[&field.name];
        txn.batch_execute(&format!(
            "alter table {target_ident} add column if not exists {} {pg_type}",
            quote_ident(&field.name)
        ))
        .await?;
    }
    // An altered field never re-types its column: anything that would
    // rewrite the target was refused above, before any DDL ran, and a
    // same-type `ALTER` needs no physical change at all; only its data is
    // stale, which the field build recomputes, as for an `ADD`.

    // An edit that builds fields can pause one at birth that
    // already has readers (see `pause_readers_of_paused_columns` below). The
    // column-pause lock it holds since its fence covers that and its
    // upstreams' pauses it reads.
    let upstreams = if builds_fields {
        definition_targets_read(&*txn, &current.source_table).await?
    } else {
        Vec::new()
    };

    for field in &real_drops {
        // ADR-0014's "quarantine state follows its owner", at column
        // granularity — the same bookkeeping `lifecycle::drop_transform`
        // clears for a whole target. A pause left behind here would outlive
        // its column and silently apply to a later `ADD` of the same name,
        // since that `ADD`'s own pause below only ever undoes a pause it
        // created itself. Edges *into* it are cleared so
        // resuming the upstream column never tries to recompute a column
        // that is gone. Edges *out of* it are cleared too, though the
        // dependents check above passed, so no live reader reads it: an
        // `ALTER` that stops a reader reading a column deletes the reader's
        // edge from it, and releases the reader if that was its
        // last reason to be paused, so a drop strands no one. This only
        // makes sure no edge from a column that no longer exists remains.
        for table in ["column_status", "column_deaths", "column_failures"] {
            txn.execute(
                &format!("delete from {table} where transform_table = $1 and column_name = $2"),
                &[&alter.target, field],
            )
            .await?;
        }
        txn.execute(
            "delete from column_pause_cascades \
             where (downstream_transform = $1 and downstream_column = $2) \
                or (upstream_transform = $1 and upstream_column = $2)",
            &[&alter.target, field],
        )
        .await?;
    }

    // An altered field that stopped reading an upstream column
    // it was paused through is no longer held by it. The edges go here; the
    // fields left with no reason are released below, once the edit's own
    // pauses are written.
    let edge_losers = if real_alters.is_empty() {
        Vec::new()
    } else {
        delete_stale_cascade_edges(
            &*txn,
            &alter.target,
            &merged,
            &real_alters,
            &current.source_table,
            &upstreams,
        )
        .await?
    };

    // The capture widen: a field reading a source column the
    // old formula didn't read can't apply until the source's capture images
    // it, so it is held out of Apply, marked `awaiting_capture`, and the
    // field build's plan job releases it once capture is ready (see "The
    // capture widen" in this function's doc comment). `on conflict do
    // nothing`: a field already paused for some other reason (an operator
    // pause, a tripped column fuse, a cascade from a paused upstream column)
    // stays paused through this edit, and its own resume rebuilds it.
    let holds_for_capture = builds_fields
        && reads_new_source_columns(&current.def, &merged)
        && !is_definition_target(&*txn, &current.source_table).await?;
    if holds_for_capture {
        // Every field the build rewrites, so a sibling reading an edited
        // field by alias is held out (and listed as paused) with it, rather
        // than evaluated over its absence and written NULL.
        for field in &build_fields {
            txn.execute(
                "insert into column_status \
                     (transform_table, column_name, paused_at, local_fuse, awaiting_capture) \
                 values ($1, $2, now(), false, true) \
                 on conflict (transform_table, column_name) do nothing",
                &[&alter.target, field],
            )
            .await?;
        }
    }

    // An edited field that now reads another definition's
    // paused column is paused at birth, as the cascade pauses a reader that
    // was there when the pause ran. One that now reads a paused
    // field by alias, directly or through others, is paused with it. A
    // field awaiting its capture gets no edges: the build that releases it
    // writes its readers, whenever they were added
    // (`staging::build::FieldPlan::for_chunk`). An edited field,
    // or a sibling paused with it, may already have readers, so each row
    // this writes owes the cascade to them, which this call walks once this
    // commits.
    if builds_fields {
        pause_readers_of_paused_columns(
            &*txn,
            &alter.target,
            &merged,
            &build_fields,
            &current.source_table,
            &upstreams,
            true,
        )
        .await?;
    }

    // The altered fields that lost an edge and have no reason
    // left to be paused are released into the edit's own field build, and
    // their readers in other definitions are resumed once this commits.
    let released_readers = if edge_losers.is_empty() {
        Vec::new()
    } else {
        release_readers_left_without_a_pause(&*txn, &alter.target, edge_losers, holds_for_capture)
            .await?
    };

    let new_text = render_definition_text(&merged);
    txn.execute(
        "update transform_definitions \
         set definition_text = $1, definition_version = $2, grammar_version = $4 \
         where id = $3",
        &[
            &new_text,
            &new_version,
            &current.id,
            &CURRENT_GRAMMAR_VERSION,
        ],
    )
    .await?;

    // The recompute is a field build: registered here, run
    // by the drain workers. See "The field build" in this function's doc
    // comment.
    if builds_fields {
        crate::staging::build::start_field_build(&*txn, current.id, status, &build_fields).await?;
    }

    txn.commit().await?;

    // The readers in other definitions that the released fields
    // left with no reason, resumed now that the edit's lock is gone.
    crate::staging::build::resume_released_dependents(pool, &alter.target, released_readers).await;

    // Walk the cascade each field this paused owes, now that
    // nothing is held. See `staging::quarantine::cascade_edit_pauses`.
    if builds_fields
        && let Err(err) = crate::staging::quarantine::cascade_edit_pauses(pool, &alter.target).await
    {
        crate::instance_log::warn!(
            transform = %alter.target,
            error = %err,
            "couldn't read the column pauses this edit owes a cascade; the capture pass walks them"
        );
    }

    let definition = definition_by_target(pool, &alter.target)
        .await?
        .expect("the definition this call just edited must still exist");
    Ok(AlterOutcome {
        definition,
        added,
        dropped,
        altered,
    })
}

/// What [`validate_edit`] read from the live schema.
struct EditedSchema {
    /// The source's columns and their value types ([`live_source_columns`]).
    source_columns: HashMap<String, ValueType>,
    /// The result type of each of the edited definition's fields.
    field_types: HashMap<String, ValueType>,
}

/// Validates `merged`, the definition an `ALTER TRANSFORM` would leave,
/// against `source_table`'s live columns, with the rules a
/// define applies: the source still exists, no added field takes a key
/// column's name, and the definition passes [`validate`] over the
/// live columns. A source column added, dropped or
/// retyped since define is judged as it is now, for every field, edited or
/// not: an edit that leaves a field reading a dropped column is refused
/// until the same edit drops or changes that field.
///
/// It also refuses an edit that builds a field (`build_fields`) reading a
/// column whose live type differs from the one `recorded` for the definition
/// ([`field_over_retyped_column`]): the field's build would compute it over
/// the live type and its applies over the recorded one. The edit can't record
/// the live type instead, since every unedited field reading the column
/// applies through that record too, and moving it would change how they
/// evaluate into columns created for the old type: a `3.5` that fails to
/// parse as the recorded `integer`, and holds its key, would round silently
/// into the field's `integer` column. A resume re-types the whole definition.
///
/// Reads through `client`, so the transaction that commits the edit reads
/// the schema it commits against.
async fn validate_edit(
    client: &impl GenericClient,
    source_table: &str,
    merged: &TransformDef,
    real_adds: &[FieldDef],
    build_fields: &[String],
    recorded: &HashMap<String, ValueType>,
) -> Result<EditedSchema, CatalogError> {
    let source_columns = live_source_columns(client, source_table).await?;
    if source_columns.is_empty() {
        return Err(CatalogError::SourceTableNotFound(source_table.to_string()));
    }
    // An added field can't take a key column's name, exactly as
    // at install. Ahead of `validate` for the same reason as there.
    reject_fields_named_after_primary_key(client, source_table, real_adds, &source_columns).await?;
    // Reuse the exact validator (and, inside it, the exact column-cycle
    // detector) a first `define` runs — see `alter_transform`'s own doc
    // comment on why the table-level whole-graph check is deliberately not
    // re-run.
    validate_with_source(merged, &source_columns, source_table)?;
    if let Some(read) = field_over_retyped_column(merged, build_fields, recorded, &source_columns) {
        let target = &merged.target;
        return Err(CatalogError::UnsupportedAlter(format!(
            "field '{}' would read source column '{}', which '{target}' recorded as {} and the \
             source has as {} now; PAUSE TRANSFORM {target} and RESUME TRANSFORM {target} to \
             re-type it over the source as it is now, then edit it",
            read.field,
            read.column,
            ddl::pg_type_name(read.recorded),
            ddl::pg_type_name(read.live),
        )));
    }
    let field_types = infer_field_types(merged, &source_columns)?;
    Ok(EditedSchema {
        source_columns,
        field_types,
    })
}

/// A field an edit builds that reads a source column whose live type differs
/// from its recorded one: [`field_over_retyped_column`]'s result.
#[derive(Debug, PartialEq)]
struct RetypedRead {
    field: String,
    column: String,
    recorded: ValueType,
    live: ValueType,
}

/// The first of `build_fields` that reads, itself or through the fields it
/// reads by alias, a source column whose `live` type differs from the one
/// `recorded` for it. A column missing from either map is not one: an edit
/// records a column the host added since ([`record_added_source_columns`]),
/// and a dropped one fails validation.
///
/// A field's own reads are taken from it alone, so a name it reads by alias
/// counts as a source column of that name. That adds no column the field
/// doesn't read: a field named after a source column passes that column
/// through (`ValidationError::CalculatedFieldShadowsSourceColumn`).
fn field_over_retyped_column(
    merged: &TransformDef,
    build_fields: &[String],
    recorded: &HashMap<String, ValueType>,
    live: &HashMap<String, ValueType>,
) -> Option<RetypedRead> {
    let mut over: HashMap<String, (String, ValueType, ValueType)> = HashMap::new();
    for field in &merged.fields {
        let alone = TransformDef {
            fields: vec![field.clone()],
            ..merged.clone()
        };
        let mut reads: Vec<String> = super::oracle::referenced_source_columns(&alone)
            .into_iter()
            .collect();
        reads.sort();
        let retyped =
            reads
                .into_iter()
                .find_map(|column| match (recorded.get(&column), live.get(&column)) {
                    (Some(&was), Some(&now)) if was != now => Some((column, was, now)),
                    _ => None,
                });
        if let Some(retyped) = retyped {
            over.insert(field.name.clone(), retyped);
        }
    }
    let readers = super::eval::AliasReaders::of(merged);
    let mut queue: Vec<String> = over.keys().cloned().collect();
    while let Some(field) = queue.pop() {
        let retyped = over[&field].clone();
        for reader in readers.direct(&field) {
            if !over.contains_key(reader) {
                over.insert(reader.clone(), retyped.clone());
                queue.push(reader.clone());
            }
        }
    }
    build_fields.iter().find_map(|field| {
        let (column, recorded, live) = over.get(field)?.clone();
        Some(RetypedRead {
            field: field.clone(),
            column,
            recorded,
            live,
        })
    })
}

/// Records, as definition `id`'s persisted source column types, the live type
/// of each source column `edited` reads that has none `recorded`:
/// a column the host added since the definition last recorded its
/// types. The builds and applies of an edit's fields infer and cast through
/// the persisted types, so a field reading such a column couldn't apply
/// without it, and no other field reads it, since none could have validated
/// over it. A recorded column keeps its type, even one the host has changed
/// since: no field the edit builds reads one ([`field_over_retyped_column`]),
/// and the unedited ones keep applying as they did. An edit that reads no
/// added column writes nothing. Under the definition's row lock: a resume's
/// own record ([`record_source_columns`]) can't interleave.
async fn record_added_source_columns(
    client: &impl GenericClient,
    id: i64,
    edited: &TransformDef,
    recorded: &HashMap<String, ValueType>,
    live: &HashMap<String, ValueType>,
) -> Result<(), CatalogError> {
    let added: HashMap<String, ValueType> = super::oracle::referenced_source_columns(edited)
        .into_iter()
        .filter(|column| !recorded.contains_key(column))
        .filter_map(|column| {
            let value_type = *live.get(&column)?;
            Some((column, value_type))
        })
        .collect();
    if added.is_empty() {
        return Ok(());
    }
    let (keys, vals) = encode_type_map(&added);
    client
        .execute(
            "update transform_definitions \
             set source_columns = source_columns || jsonb_object($2::text[], $3::text[]) \
             where id = $1",
            &[&id, &keys, &vals],
        )
        .await?;
    // The OID of an added column's type, beside the name its token
    // gives (`definition_types`). A type already recorded keeps its row.
    super::key_types::record_types(client, id, false).await?;
    Ok(())
}

/// What an edit's clauses change, net of each other: [`plan_alter`]'s result.
#[derive(Debug, PartialEq)]
struct AlterPlan {
    /// The definition's fields once every clause is applied, in order.
    fields: Vec<FieldDef>,
    /// Fields the edit adds, as they end up: each needs a new column.
    real_adds: Vec<FieldDef>,
    /// Fields the definition had, kept, whose formula ends up different.
    real_alters: Vec<FieldDef>,
    /// Fields the definition had that a `DROP` removed, in clause order:
    /// each loses its column, even if an `ADD` then brings the name back.
    real_drops: Vec<String>,
}

/// Applies an edit's `clauses`, in order, to the fields `current` the
/// definition `target` has, and works out what the edit changes as a whole
/// rather than clause by clause: a field added and then dropped
/// in the same edit never had a column, so it is neither added nor dropped,
/// and an `ALTER` of a field the edit added just changes what is added.
///
/// A field the definition had and the edit drops is a real drop, even when
/// a later `ADD` brings the name back: `DROP z, ADD z` replaces the column,
/// the supported path for a change of result type (see
/// [`check_no_type_changing_alter`]).
///
/// Refuses, as [`alter_transform`] does, an `ADD` of a name already
/// present with a different formula and an `ALTER` of a name not present,
/// each as of its own clause. An `ADD` with a present field's formula, an
/// `ALTER` to it, or a `DROP` of an absent name is an idempotent no-op.
fn plan_alter(
    target: &str,
    current: &[FieldDef],
    clauses: &[AlterClause],
) -> Result<AlterPlan, CatalogError> {
    let mut fields = current.to_vec();
    // The names whose field in `fields` this edit added. Every other name
    // in `fields` is one of `current`'s, not dropped.
    let mut fresh: HashSet<String> = HashSet::new();
    let mut real_drops: Vec<String> = Vec::new();

    for clause in clauses {
        match clause {
            AlterClause::Add(new_field) => match fields.iter().find(|f| f.name == new_field.name) {
                Some(existing) if existing.expr == new_field.expr => {}
                Some(_) => {
                    return Err(CatalogError::AlterFieldAlreadyExists {
                        transform: target.to_string(),
                        field: new_field.name.clone(),
                    });
                }
                None => {
                    fields.push(new_field.clone());
                    fresh.insert(new_field.name.clone());
                }
            },
            AlterClause::Alter(new_field) => {
                match fields.iter().position(|f| f.name == new_field.name) {
                    None => {
                        return Err(CatalogError::AlterFieldNotFound {
                            transform: target.to_string(),
                            field: new_field.name.clone(),
                            declared: fields.iter().map(|f| f.name.clone()).collect(),
                        });
                    }
                    Some(idx) => fields[idx] = new_field.clone(),
                }
            }
            AlterClause::Drop(name) => {
                if let Some(idx) = fields.iter().position(|f| &f.name == name) {
                    fields.remove(idx);
                    // A field this edit added cancels; one the definition
                    // had loses its column.
                    if !fresh.remove(name) {
                        real_drops.push(name.clone());
                    }
                }
            }
        }
    }

    let (real_adds, kept): (Vec<FieldDef>, Vec<FieldDef>) = fields
        .iter()
        .cloned()
        .partition(|f| fresh.contains(&f.name));
    let real_alters = kept
        .into_iter()
        .filter(|f| {
            current
                .iter()
                .find(|old| old.name == f.name)
                .is_some_and(|old| old.expr != f.expr)
        })
        .collect();
    Ok(AlterPlan {
        fields,
        real_adds,
        real_alters,
        real_drops,
    })
}

/// The definition target a 1-1 definition with source `qualified_source`
/// reads, if its source is one, as `(bare, qualified)`: bare as
/// `column_status` keys it, qualified as a field's reads resolve.
async fn definition_targets_read(
    txn: &impl GenericClient,
    qualified_source: &str,
) -> Result<Vec<(String, String)>, CatalogError> {
    Ok(txn
        .query(
            "select split_part(target_table, '.', 2), target_table \
             from transform_definitions where target_table = $1",
            &[&qualified_source],
        )
        .await?
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect())
}

/// Pauses each of `fields`, of the 1-1 definition `def` targeting `target`
/// (bare), that reads a paused column as it is defined or edited,
/// as `staging::quarantine`'s cascade pauses a reader that
/// was already there when the pause ran (ADR-0003 decision #5): a
/// `column_status` row, which `status` lists, and a cascade edge, which the
/// paused column's resume walks to release it into that resume's field
/// build. Apply and every build leave a paused field out, so the reader
/// holds no value until then rather than the paused column's frozen one.
///
/// Two kinds of read pause a field:
///
/// - **Another definition's paused column** ([`column_dependents`]'s match,
///   inverted): one of `upstreams` ([`definition_targets_read`]), the
///   tables `def` reads that are a definition's target, has a column paused
///   (by its fuse, an operator, or a cascade), and the field's expression
///   reads that column. A column awaiting its capture widen
///   (`awaiting_capture`) pauses no reader, as it cascades to none.
/// - **A paused field of `def` itself**, read by alias, directly or through
///   other fields: the same closure the cascade walks for a
///   sibling, from every field of `target` paused, including those paused
///   just above.
///
/// **An edited field's readers** (`owes_cascade`). A field an
/// edit pauses may already have readers: definitions reading it, and
/// fields of `def` reading it by alias that the edit didn't touch. Each row
/// this writes then owes the cascade a pause writes
/// (`column_status.cascade_pending`), and [`alter_transform`] walks it
/// right after this commits (`staging::quarantine`'s
/// `cascade_edit_pauses`), leaving a walk that fails to the staging worker's
/// capture pass (`complete_pause_cascades`). The walk isn't run here: each
/// reader's pause bumps its own definition's source fence, and this
/// transaction holds `def`'s already. A define's fields have no readers yet,
/// so it owes none.
///
/// Runs in the define or edit's own transaction, whose first lock bumped
/// the version fence of `def`'s source: the fence `staging::quarantine`'s
/// pause of one of `def`'s fields bumps, so this takes no fence of its own.
/// The caller holds the column-pause lock ([`crate::locks::lock_column_pauses`]):
/// shared for a define, exclusive for an edit. Every pause and resume of an
/// upstream column writes its rows under it exclusive, so:
///
/// - A pause of an upstream column that commits first is read here. One
///   that comes later waits for this commit, and its cascade then reads the
///   definition as it stands and finds this reader paused already.
/// - A resume of an upstream column that commits first leaves nothing to
///   read. One that comes later waits for this commit and then deletes the
///   edge with the rest, releasing the reader.
#[allow(clippy::too_many_arguments)]
async fn pause_readers_of_paused_columns(
    txn: &impl GenericClient,
    target: &str,
    def: &TransformDef,
    fields: &[String],
    qualified_source: &str,
    upstreams: &[(String, String)],
    owes_cascade: bool,
) -> Result<(), CatalogError> {
    let mut paused: Vec<(&str, &str, String)> = Vec::new();
    for (upstream, qualified_upstream) in upstreams {
        paused.extend(
            txn.query(
                "select column_name from column_status \
                 where transform_table = $1 and not awaiting_capture \
                 order by column_name",
                &[upstream],
            )
            .await?
            .into_iter()
            .map(|row| (upstream.as_str(), qualified_upstream.as_str(), row.get(0))),
        );
    }
    // Test-only pause point. See `staging::interleave`.
    #[cfg(any(test, feature = "test-util"))]
    crate::staging::interleave::pause_at(
        txn,
        crate::staging::interleave::PausePoint::AfterUpstreamPausesRead,
        target,
    )
    .await?;
    for (upstream, qualified_upstream, column) in &paused {
        for field in def.fields.iter().filter(|f| fields.contains(&f.name)) {
            if !expr_references_column(&field.expr, qualified_source, qualified_upstream, column) {
                continue;
            }
            pause_reader_in_txn(txn, (target, &field.name), (upstream, column), owes_cascade)
                .await?;
            crate::instance_log::warn!(
                transform = %target,
                column = %field.name,
                upstream_transform = %upstream,
                upstream_column = %column,
                "column paused at birth: it reads a paused upstream column"
            );
        }
    }

    let readers = super::eval::AliasReaders::of(def);
    let mut frontier: Vec<String> = txn
        .query(
            "select column_name from column_status \
             where transform_table = $1 and not awaiting_capture",
            &[&target],
        )
        .await?
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    while let Some(upstream) = frontier.pop() {
        for reader in readers.direct(&upstream) {
            if !fields.contains(reader) {
                continue;
            }
            if pause_reader_in_txn(txn, (target, reader), (target, &upstream), owes_cascade).await?
            {
                frontier.push(reader.clone());
            }
        }
    }
    Ok(())
}

/// Deletes the cascade edges into each of `altered` (fields of the 1-1
/// definition `def` targeting `target`, bare, whose formula this edit
/// changed) from an upstream column the new formula doesn't read. An edge
/// records that its reader is paused because the upstream
/// column is; a reader edited to stop reading the column is no longer held
/// by it, and left in place the edge would keep the reader paused until
/// that column's resume (or leave it with no reason at all once a `DROP`
/// deleted the column's edges). The edges the new formula still has, and
/// those [`pause_readers_of_paused_columns`] writes for a paused column it
/// now reads, are untouched or written afterwards.
///
/// Returns the fields that lost an edge: the candidates for
/// [`release_readers_left_without_a_pause`], which runs once the edit's
/// own pauses are written. The caller holds the column-pause lock.
async fn delete_stale_cascade_edges(
    txn: &impl GenericClient,
    target: &str,
    def: &TransformDef,
    altered: &[FieldDef],
    qualified_source: &str,
    upstreams: &[(String, String)],
) -> Result<Vec<String>, CatalogError> {
    let alias_readers = super::eval::AliasReaders::of(def);
    let mut stale: Vec<String> = Vec::new();
    for field in altered {
        let edges = txn
            .query(
                "select upstream_transform, upstream_column from column_pause_cascades \
                 where downstream_transform = $1 and downstream_column = $2",
                &[&target, &field.name],
            )
            .await?;
        for edge in edges {
            let upstream_transform: String = edge.get(0);
            let upstream_column: String = edge.get(1);
            let still_read = if upstream_transform == target {
                alias_readers.direct(&upstream_column).contains(&field.name)
            } else {
                upstreams
                    .iter()
                    .filter(|(bare, _)| *bare == upstream_transform)
                    .any(|(_, qualified)| {
                        expr_references_column(
                            &field.expr,
                            qualified_source,
                            qualified,
                            &upstream_column,
                        )
                    })
            };
            if still_read {
                continue;
            }
            txn.execute(
                "delete from column_pause_cascades \
                 where downstream_transform = $1 and downstream_column = $2 \
                   and upstream_transform = $3 and upstream_column = $4",
                &[&target, &field.name, &upstream_transform, &upstream_column],
            )
            .await?;
            crate::instance_log::info!(
                transform = %target,
                column = %field.name,
                upstream_transform = %upstream_transform,
                upstream_column = %upstream_column,
                "edit stopped a paused field reading an upstream column; its cascade edge is gone"
            );
            if !stale.contains(&field.name) {
                stale.push(field.name.clone());
            }
        }
    }
    Ok(stale)
}

/// Releases each of `candidates`, fields of `target` that
/// [`delete_stale_cascade_edges`] took an edge from, that is left with no
/// reason to stay paused: no `local_fuse`, no edge into it
/// (including those the edit's own pauses just wrote) and not awaiting its
/// capture. The edit's field build rewrites it (a candidate is an altered
/// field), as a resume's would. Its readers are un-cascaded as a resume's
/// ([`crate::staging::quarantine::uncascade`]): those in `target` go with
/// it, into the same build (the edit's build lists every field reading an
/// edited one by alias), and those in other definitions come back for the
/// caller to resume once this commits
/// ([`crate::staging::build::resume_released_dependents`]).
///
/// With `holds_for_capture`, the edit's new formula reads a source column
/// the capture may not image yet, and the field, paused for
/// another reason when the edit's capture widen skipped it, is held with
/// `awaiting_capture` instead: the build's plan job releases it, and its
/// readers, once capture is ready.
///
/// A candidate that stays paused through another edge gets its reason
/// restated ([`crate::staging::quarantine::restate_cascade_reason`]), so
/// `status` doesn't name the upstream column it stopped reading.
async fn release_readers_left_without_a_pause(
    txn: &impl GenericClient,
    target: &str,
    candidates: Vec<String>,
    holds_for_capture: bool,
) -> Result<Vec<(String, String)>, CatalogError> {
    let free: Vec<String> = txn
        .query(
            "select s.column_name from column_status s \
             where s.transform_table = $1 and s.column_name = any($2) \
               and not s.local_fuse and not s.awaiting_capture \
               and not exists ( \
                   select 1 from column_pause_cascades c \
                   where c.downstream_transform = s.transform_table \
                     and c.downstream_column = s.column_name) \
             order by s.column_name",
            &[&target, &candidates],
        )
        .await?
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    // A candidate that stays paused through an edge left, old or just
    // written, names that edge's upstream rather than the one it lost.
    for field in candidates.iter().filter(|field| !free.contains(field)) {
        crate::staging::quarantine::restate_cascade_reason(txn, target, field).await?;
    }
    if free.is_empty() {
        return Ok(Vec::new());
    }
    if holds_for_capture {
        // Its reason is now its capture's wait, which gives none, as the
        // edit's capture widen's own rows give none.
        txn.execute(
            "update column_status set awaiting_capture = true, last_error = null \
             where transform_table = $1 and column_name = any($2)",
            &[&target, &free],
        )
        .await?;
        return Ok(Vec::new());
    }
    txn.execute(
        "delete from column_status where transform_table = $1 and column_name = any($2)",
        &[&target, &free],
    )
    .await?;
    Ok(crate::staging::quarantine::uncascade(txn, target, free)
        .await?
        .dependents)
}

/// Records the cascade edge from `upstream` to `reader` and pauses
/// `reader`, as the cascade's pause of one reader does
/// (`staging::quarantine`'s `pause_dependent`): the edge and a
/// `column_status` row with no reason of its own (`local_fuse = false`),
/// each left as it is when there already. With `owes_cascade`, the row is
/// marked as owing the walk to `reader`'s own readers (`cascade_pending`),
/// whether it is new or there already: one that was only awaiting its
/// capture widen cascaded to none, and a walk of one paused for another
/// reason only finds its readers paused already. Whether the edge is new.
async fn pause_reader_in_txn(
    txn: &impl GenericClient,
    (reader_transform, reader_column): (&str, &str),
    (upstream_transform, upstream_column): (&str, &str),
    owes_cascade: bool,
) -> Result<bool, CatalogError> {
    let edged = txn
        .execute(
            "insert into column_pause_cascades \
                 (downstream_transform, downstream_column, upstream_transform, upstream_column) \
             values ($1, $2, $3, $4) \
             on conflict do nothing",
            &[
                &reader_transform,
                &reader_column,
                &upstream_transform,
                &upstream_column,
            ],
        )
        .await?;
    let insert = if owes_cascade {
        "insert into column_status \
             (transform_table, column_name, paused_at, last_error, local_fuse, cascade_pending) \
         values ($1, $2, now(), $3, false, true) \
         on conflict (transform_table, column_name) do update set cascade_pending = true"
    } else {
        "insert into column_status \
             (transform_table, column_name, paused_at, last_error, local_fuse) \
         values ($1, $2, now(), $3, false) \
         on conflict (transform_table, column_name) do nothing"
    };
    txn.execute(
        insert,
        &[
            &reader_transform,
            &reader_column,
            &crate::staging::quarantine::cascade_reason(upstream_transform, upstream_column),
        ],
    )
    .await?;
    Ok(edged > 0)
}

/// Whether `edited` reads a source column `original` doesn't:
/// one the source's capture triggers may not image yet. A column `original`
/// already read has been imaged since the definition was dispatched, since no
/// capture narrows away a column a registered definition reads.
fn reads_new_source_columns(original: &TransformDef, edited: &TransformDef) -> bool {
    let before = super::oracle::referenced_source_columns(original);
    super::oracle::referenced_source_columns(edited)
        .iter()
        .any(|column| !before.contains(column))
}

/// [`alter_transform`]'s output: the edited definition's new state, plus
/// which fields were actually added/dropped/altered (excluding any
/// idempotent no-op clause) — the same "report what really happened, not
/// just what was asked" contract [`super::lifecycle::PauseOutcome`]/
/// [`super::lifecycle::DropOutcome`] already keep.
#[derive(Debug, Clone)]
pub struct AlterOutcome {
    pub definition: Definition,
    pub added: Vec<String>,
    pub dropped: Vec<String>,
    pub altered: Vec<String>,
}

/// [`alter_transform`]'s pre-transaction, best-effort half of the `DROP
/// <field>` refusal check — see [`column_dependents_any_keyspace`]'s own doc
/// comment for why this is the *entire* correctness gate for a column drop,
/// not just a naming nicety.
async fn check_no_column_dependents(
    pool: &Pool,
    target: &str,
    field: &str,
) -> Result<(), CatalogError> {
    let client = pool.get().await?;
    check_no_column_dependents_via(&**client, target, field).await
}

/// The transaction-scoped counterpart to [`check_no_column_dependents`],
/// re-run under the target's own row lock so a
/// dependent registered between the pre-check and this commit cannot slip
/// through.
async fn check_no_column_dependents_in_txn(
    txn: &tokio_postgres::Transaction<'_>,
    target: &str,
    field: &str,
) -> Result<(), CatalogError> {
    check_no_column_dependents_via(txn, target, field).await
}

async fn check_no_column_dependents_via(
    client: &impl GenericClient,
    target: &str,
    field: &str,
) -> Result<(), CatalogError> {
    let deps = column_dependents_any_keyspace(client, target, field).await?;
    if deps.is_empty() {
        return Ok(());
    }
    let mut dependents: Vec<String> = deps.iter().map(|(t, _)| t.clone()).collect();
    dependents.sort();
    dependents.dedup();
    let column_detail: Vec<String> = deps
        .iter()
        .map(|(t, f)| format!("{t}.{f} reads {target}.{field}"))
        .collect();
    Err(CatalogError::DependentsBlockDrop {
        subject: format!("{target}.{field}"),
        dependents,
        column_detail,
    })
}

/// What [`check_alter_column_types`] needs to compare an altered field's
/// column with the type `DEFINE` would give it.
struct AlterTypeContext<'a> {
    /// The definition the edit leaves.
    merged: &'a TransformDef,
    source_table: &'a str,
    source_columns: &'a HashMap<String, ValueType>,
    field_types: &'a HashMap<String, ValueType>,
    qualified_target: &'a str,
    display_target: &'a str,
}

/// The refusal of a type-changing `ALTER <field> AS <expr>`: `field`'s
/// column as it is now against the type `DEFINE` would give the altered
/// field (a bare passthrough's source column's type, anything else its value
/// family's, [`ddl::field_column_pg_types`]'s rule).
///
/// ADR-0015 promises "An edit never rewrites the whole target table to
/// change one column", so:
///
/// - the same type needs no change, and neither does a string column
///   (`text`, `varchar(n)` or `varchar`) altered to another: Trellis's
///   copies of a string are unbounded, and no copy has a modifier
///   (ADR-0020), so a change of one never changes a column;
/// - anything else (another width or value family) would rewrite the
///   target under an `ACCESS EXCLUSIVE` lock, so it is refused. `DROP` and
///   `ADD` of the field pays that backfill cost explicitly.
///
/// `client` is a pooled connection for the best-effort pre-check before the
/// transaction opens, and the transaction itself for the authoritative
/// re-check under the row lock.
async fn check_alter_column_types(
    client: &impl GenericClient,
    ctx: &AlterTypeContext<'_>,
    fields: &[FieldDef],
) -> Result<(), CatalogError> {
    for field in fields {
        let Some(current) =
            target_column_type_via(client, ctx.qualified_target, &field.name).await?
        else {
            // Defensive: every real `ALTER` field is, by construction, a
            // physical column already.
            continue;
        };
        let wanted = match ddl::passthrough_source_column(field, ctx.merged, ctx.source_columns) {
            Some(source_column) => {
                // A copy has the source column's type and no modifier.
                match target_column_type_via(client, ctx.source_table, source_column).await? {
                    Some((oid, _)) => (oid, -1),
                    None => continue,
                }
            }
            None => {
                let family = ctx
                    .field_types
                    .get(&field.name)
                    .copied()
                    .unwrap_or(ValueType::Numeric);
                let oid: Option<u32> = client
                    .query_one(
                        "select pg_catalog.to_regtype($1)::oid",
                        &[&ddl::pg_type_name(family)],
                    )
                    .await?
                    .get(0);
                match oid {
                    Some(oid) => (oid, -1),
                    None => continue,
                }
            }
        };
        if current == wanted {
            continue;
        }
        let mut described = std::collections::BTreeMap::new();
        let current = super::copies::describe(client, &mut described, current).await?;
        let wanted = super::copies::describe(client, &mut described, wanted).await?;
        // `text` and `varchar(n)` are one family, and a copy of either is an
        // unbounded `varchar` or `text`: nothing to change.
        if current.ty.is_string() && wanted.ty.is_string() {
            continue;
        }
        return Err(CatalogError::UnsupportedAlter(format!(
            "ALTER '{field}' on '{display_target}' would change its column type from \
             '{current}' to '{wanted}'; Trellis never re-types a column in an edit, because Postgres \
             would rewrite '{display_target}''s entire physical table under an ACCESS EXCLUSIVE \
             lock, blocking reads and writes to every \
             other column too, not just this one. Use DROP '{field}' followed by ADD <expr> AS \
             '{field}' instead — it already pays this same backfill cost explicitly, rather \
             than paying it as a surprise.",
            field = field.name,
            display_target = ctx.display_target,
            current = current.display,
            wanted = wanted.display,
        )));
    }
    Ok(())
}

/// The `(atttypid, atttypmod)` of `qualified_table.column`: `None` if the
/// column somehow doesn't exist (defensive; see this function's callers). A
/// mixed-case name resolves through [`ddl::regclass_arg`].
async fn target_column_type_via(
    client: &impl GenericClient,
    qualified_table: &str,
    column: &str,
) -> Result<Option<(u32, i32)>, CatalogError> {
    let row = client
        .query_opt(
            "select a.atttypid, a.atttypmod \
             from pg_attribute a \
             where a.attrelid = pg_catalog.to_regclass($1) \
               and a.attname = $2 \
               and a.attnum > 0 \
               and not a.attisdropped",
            &[&ddl::regclass_arg(qualified_table), &column],
        )
        .await?;
    Ok(row.map(|row| (row.get(0), row.get(1))))
}

/// Validates `source_text` and persists it as a definition in `status`, with
/// its schema-graph nodes and edge and a `source_table_versions` bump, in one
/// transaction. Reads the source's catalog shape (its key), never its rows
/// (ADR-0002): whoever builds the target does that later.
///
/// `target_ddl` is [`install_definition`]'s rendered `create table` for the
/// target, run in this same transaction after refusing a target name that
/// already exists (see [`create_target_in_txn`]). `None` is the test-fixture
/// entry points' contract: their caller already created the target.
async fn create_definition_inner(
    pool: &Pool,
    source_text: &str,
    source_columns: &HashMap<String, ValueType>,
    status: TransformStatus,
    target_schema: &str,
    target_ddl: Option<&str>,
) -> Result<Definition, CatalogError> {
    let def: TransformDef = parse(source_text)?;
    validate_for_define(pool, &def, source_columns).await?;

    let mut client = pool.get().await?;
    let txn = client.transaction().await?;

    // ADR-0011: resolve `def.source` — always a bare table
    // name (see [`super::ast::TransformDef`]'s own doc comment for why the
    // dotted spelling never lands in this field) — to its fully-qualified
    // `schema.table` identity exactly once, here, at definition-acceptance
    // time. Two ways to get there, branching on
    // [`super::ast::TransformDef::explicit_source_schema`]:
    //
    // * `Some(schema)`: the definition wrote `FROM
    //   <schema>.<source>` explicitly, so `schema` is trusted outright —
    //   [`confirm_qualified_source`] checks that *exact*
    //   relation is real, never walking `search_path` the way the bare case
    //   does (ADR-0011: a qualified spelling resolves to
    //   that one relation, full stop).
    // * `None` (bare, the far more common case): [`resolve_graph_identity_in_txn`]
    //   — a plain `search_path` walk with a
    //   chained-target fallback (see that function's own doc comment for
    //   why, and for why this safely runs *before* the node/cycle
    //   checks below).
    //
    // Either way, from this point on `qualified_source` — never `def.source`
    // — is what gets persisted
    // (`source_table_versions`/`transform_definitions.source_table` below)
    // and threaded into every side effect that must agree with the
    // persisted row *and* `schema_nodes`/`schema_edges`
    // themselves.
    //
    // This matters for the explicit-schema branch specifically: the test
    // fixtures [`create_definition`] and [`create_definition_without_backfill`]
    // never go through [`install_definition`]'s own fail-fast check (that
    // function's own doc comment on its identically-shaped block), so this is
    // the *only* place a bogus explicit source schema is ever caught for
    // them.
    let qualified_source = match &def.explicit_source_schema {
        Some(schema) => {
            confirm_qualified_source(&*txn, schema, &def.source).await?;
            crate::intake::qualify(schema, &def.source)?
        }
        None => match resolve_graph_identity_in_txn(&txn, &def.source).await {
            // A source the role can't see doesn't resolve.
            Err(err @ CatalogError::SourceTableNotFound(_)) => {
                reject_inaccessible_table(&*txn, None, &def.source).await?;
                return Err(err);
            }
            result => result?,
        },
    };
    // The authoritative check (`install_definition` repeats it
    // earlier, before any DDL, as a fail-fast).
    reject_non_live_upstream(&*txn, &qualified_source).await?;
    // A JOIN's joined table resolves the way its `FROM` does, and is
    // held to the same checks. Its columns are read here, in this
    // transaction, for the catalog row and the hidden tables.
    let join_sides = match super::join::spec(&def) {
        Some(join) => {
            let joined = match &join.explicit_right_schema {
                Some(schema) => {
                    confirm_qualified_source(&*txn, schema, &join.right).await?;
                    crate::intake::qualify(schema, &join.right)?
                }
                None => match resolve_graph_identity_in_txn(&txn, &join.right).await {
                    Err(err @ CatalogError::SourceTableNotFound(_)) => {
                        reject_inaccessible_table(&*txn, None, &join.right).await?;
                        return Err(err);
                    }
                    result => result?,
                },
            };
            reject_non_live_upstream(&*txn, &joined).await?;
            let sides = JoinSides::read(&*txn, qualified_source.clone(), joined).await?;
            super::validate::validate_join(&def, &sides.from_columns, &sides.joined_columns)?;
            Some(sides)
        }
        None => {
            // A three-part qualified column names the source's
            // schema, which a bare `FROM` only now resolved. A JOIN's
            // qualified columns are `validate_join`'s.
            validate_qualified_columns(&def, source_columns, schema_of(&qualified_source))?;
            None
        }
    };
    // The index a `self_check` walk of the target pages its source by.
    if !def.without_source_index {
        let missing = super::source_index::unmet_for(
            &*txn,
            &def,
            source_columns,
            &qualified_source,
            join_sides.as_ref().map(|sides| sides.joined.as_str()),
        )
        .await?;
        if !missing.is_empty() {
            return Err(CatalogError::SourceIndexMissing {
                target: def.target.clone(),
                missing,
            });
        }
    }

    // The source's version fence, bumped as this transaction's first lock.
    // The bump waits for every page holding the
    // fence `for share`. Everything above only reads. A failure below rolls
    // the bump back.
    //
    // A JOIN bumps both tables' fences, in table-name order, the
    // order every taker of two fences takes them in; it records the `FROM`
    // table's version.
    let fenced: Vec<&str> = match &join_sides {
        Some(sides) => sides.tables_in_order().to_vec(),
        None => vec![qualified_source.as_str()],
    };
    let mut version: i64 = 0;
    for table in fenced {
        let bumped: i64 = txn
            .query_one(
                "insert into source_table_versions (source_table, version)
                 values ($1, 1)
                 on conflict (source_table)
                 do update set version = source_table_versions.version + 1
                 returning version",
                &[&table],
            )
            .await?
            .get(0);
        if table == qualified_source {
            version = bumped;
        }
    }
    // The column-pause lock, shared, right after the fence and before
    // the definition row or any DDL: the pause state a field reading a
    // paused column is born under (below) is read under it, so a pause
    // either commits before this define reads it or waits for this commit
    // and finds the define in the graph its cascade walks. Held to the
    // commit of this catalog transaction only: the build and backfill a
    // define starts run after it, in transactions of their own.
    crate::locks::lock_column_pauses(
        &*txn,
        pool.schema(),
        crate::locks::ColumnPauseLock::Shared,
        crate::locks::ColumnPauseOp::Define,
    )
    .await?;

    // ADR-0011: resolve `def.target` — likewise always bare
    // — to its fully-qualified identity exactly once, here, mirroring
    // `qualified_source` immediately above. Unlike the source side,
    // `def.target`'s schema is never search-path-resolved:
    // a source table's schema is *discovered* (it already exists
    // somewhere on the path), but a target table's schema is a
    // config-time *decision*, `target_schema` — which is
    // itself already `def`-aware: this function's caller passes
    // [`effective_target_schema`]'s result (either `install_definition`'s own
    // call, or `pool.target_schema()` unmodified for the ring-path entry
    // points, which recompute the same override redundantly below since they
    // never call `effective_target_schema` themselves). So `target_schema`
    // here already *is* `def.explicit_target_schema` when that's `Some` —
    // the `match` below re-derives that from `def` directly rather than
    // trusting the parameter alone, so the existence check runs regardless of
    // which entry point got here.
    //
    // Built via the same `intake::qualify` helper as
    // `qualified_source`, not `ddl::qualified_target_table` directly: the two
    // produce different shapes for different jobs — `qualify` returns the
    // plain, unquoted `"schema.table"` this whole module's qualified-identity
    // convention already uses (what a downstream chained definition's own
    // `resolve_graph_identity_in_txn` will independently reproduce once it
    // names this target), while `qualified_target_table` returns a
    // separately-quoted `"schema"."table"` string built for direct
    // interpolation into DDL text — never meant to be compared as a
    // persisted identity string, and never equal to `qualify`'s output
    // byte-for-byte.
    //
    // Cheap and existence-free for the common (`None`) branch — `qualify` is
    // pure string formatting, no DB round trip — so, unlike the source side,
    // there was never an ordering hazard here to begin with; this always ran
    // (and still runs) ahead of the node/cycle checks below.
    let resolved_target_schema = effective_target_schema(&def, target_schema);
    let qualified_target = crate::intake::qualify(resolved_target_schema, &def.target)?;
    // Not in a catalog schema, this instance's or another's. Before
    // any DDL, so nothing is created there.
    reject_target_in_catalog_schema(&*txn, pool.schema(), &qualified_target).await?;
    // The test-fixture entry points (`target_ddl` is `None`) register against
    // a target their caller already created; `install_definition` creates it
    // below, after every check on the definition itself.
    if target_ddl.is_none()
        && let Some(schema) = &def.explicit_target_schema
        && !qualified_table_exists(&*txn, schema, &def.target).await?
    {
        return Err(ValidationError::QualifiedTargetTableNotFound {
            schema: schema.clone(),
            table: def.target.clone(),
        }
        .into());
    }

    // Trellis reads the source (and a `JOIN`'s joined table) as
    // its own role, so row-level security that applies to that role would
    // filter the reads. A table another definition targets is checked for
    // the session's role alone: the target-mutation seam feeds it, no
    // capture function reads it, so the ring's owner never does (see
    // [`super::row_security::Readers::Session`]).
    //
    // Every Trellis session runs with `row_security = off`, so
    // a read fails outright when the policies apply, and this check is what
    // names the table, the role and the fix. It reads only the catalog, so a
    // source that's another definition's not-yet-built target passes here
    // (`to_regclass` finds no table) and is reported by the checks below.
    let mut read_tables = vec![side_read_table(&*txn, &qualified_source).await?];
    if let Some(sides) = &join_sides {
        read_tables.push(side_read_table(&*txn, &sides.joined).await?);
    }
    reject_row_security(&*txn, pool.schema(), &read_tables).await?;

    // Every definition's source and target resolve to a
    // first-class `SchemaNode`, created on first reference (a source node
    // the moment something first transforms it; a target node the moment
    // its owning definition is created) — a side effect alongside the
    // catalog writes below rather than a change to `TransformDef`'s shape,
    // per the issue's "prefer the smaller change" guidance.
    //
    // Keyed on `qualified_source`/`qualified_target` (ADR-0011), not the
    // bare `def.source`/`def.target`: `schema_nodes`/`schema_edges` key on
    // the exact same qualified identity `transform_definitions` does, so
    // `public.posts` and `archive.posts` are never one node.
    let source_node = resolve_node_in_txn(&txn, &qualified_source, NodeKind::Source).await?;
    let target_node = resolve_node_in_txn(&txn, &qualified_target, NodeKind::Target).await?;

    // Reject this definition if the `Source` edge
    // it's about to add — def.source -> def.target — would close a cycle in
    // the table-level dependency graph, transitively through any edges
    // already persisted. Checked against the transaction's own view of
    // `schema_edges` so it sees the graph exactly as it will look right up
    // to (but not including) the edge this definition is about to add.
    // Known v1 limitation: under Postgres's default read-committed
    // isolation, two concurrent `create_definition` calls (e.g. one adding
    // a->b, another adding b->a) can each pass this check before either
    // commits — nothing here serializes them (no advisory lock/
    // SERIALIZABLE) — so a cycle could theoretically still persist. Same
    // class of race as any check-then-insert pattern; accepted for now,
    // out of scope for this issue.
    //
    // Qualified `qualified_source`/`qualified_target`, matching the node
    // resolution above — `schema_edges` is keyed by
    // `schema_nodes.id`, so this walks the exact same qualified graph those
    // nodes were just resolved into.
    reject_if_table_cycle(&txn, &qualified_source, &qualified_target).await?;

    // A transform's `FROM` is a `Source` dependency edge from its
    // source node to its target node — persisted alongside the node
    // resolutions above so `dependents_of` can walk the graph instead of
    // matching on `transform_definitions.source_table` string equality.
    persist_edge_in_txn(&txn, source_node.id, target_node.id, EdgeKind::Source).await?;

    // A JOIN's joined table is a `Join` edge into the target, checked
    // for a cycle on its own. Self-joins are refused, so it never collides
    // with the `Source` edge.
    if let Some(sides) = &join_sides {
        let joined_node = resolve_node_in_txn(&txn, &sides.joined, NodeKind::Source).await?;
        reject_if_table_cycle(&txn, &sides.joined, &qualified_target).await?;
        persist_edge_in_txn(&txn, joined_node.id, target_node.id, EdgeKind::Join).await?;
    }

    // ADR-0011: reject this definition if
    // `qualified_target` shares a bare table-name suffix with a *different*
    // qualified spelling some other still-persisted definition already
    // uses. `target_table` stores the qualified spelling, so its own
    // `unique` constraint only guarantees the qualified string is unique, and
    // nothing else stopped `public.foo` and `custom.foo` from coexisting
    // (most plausibly: `Config::target_schema` changed between deploys and
    // an operator redeclared a same-named `TRANSFORM ... TARGET foo`).
    // Every `split_part(target_table, '.', 2)`-keyed read site downstream
    // ([`definition_by_target`], `app.rs`'s `status`/`quarantine_status`,
    // `generative`'s `unsettled_definitions`) was written assuming that
    // suffix is globally unique — some (`definition_by_target`) would
    // silently splice two colliding definitions' rows into one corrupted
    // [`super::ast::TransformDef`] rather than error — so this closes the
    // gap once, here, at definition-acceptance time, rather than teaching
    // every one of those call sites to defend against an ambiguity that
    // shouldn't be able to exist. Checked within this same transaction,
    // against the same `transaction`'s view of `transform_definitions`
    // every other check in this function already reads.
    //
    // Still provisional: the grammar
    // accepts an explicit `TRANSFORM <schema>.<target>` spelling, so an
    // operator *can* write `TRANSFORM custom.foo FROM ...` to disambiguate
    // from an existing `public.foo` — but this check doesn't distinguish an
    // explicit qualification from a resolved-bare one, and still rejects the
    // collision either way. That's deliberate, not an oversight: every
    // `split_part(target_table, '.', 2)`-keyed read site named above is
    // still bare-suffix-keyed — ADR-0011 qualifies the
    // `schema_nodes`/`schema_edges` graph (and, with it, [`dependents_of`],
    // which needs no `split_part` — see its own doc comment),
    // but leaves `column_status`'s addressing scheme and the
    // handful of `Trellis`-API-facing bare-name read sites named above
    // bare, for the same reasons: ADR-0003's
    // `transform.column` addressing still parses on the first `.`. So
    // `public.foo` and `custom.foo` coexisting would still silently corrupt
    // those reads regardless of whether the second one arrived via explicit
    // qualification or bare resolution. This check runs purely against the
    // *final* `qualified_target` string and `def.target`'s bare suffix — it
    // has no branch on `def.explicit_target_schema` at all — so it protects
    // an explicitly-qualified target exactly the same way it already
    // protected a resolved-bare one; only a different addressing scheme for
    // those remaining bare-keyed sites can relax this.
    if let Some(row) = txn
        .query_opt(
            "select target_table from transform_definitions \
             where split_part(target_table, '.', 2) = $1 and target_table <> $2 \
             limit 1",
            &[&def.target, &qualified_target],
        )
        .await?
    {
        let existing: String = row.get(0);
        return Err(CatalogError::TargetTableSuffixCollision {
            target: def.target.clone(),
            requested: qualified_target,
            existing: Some(existing),
        });
    }

    // Fail fast on a
    // source with no primary key at all, or an unsafe-to-key-on primary key
    // type, before the definition is persisted — a composite (multi-column) source
    // primary key is no longer rejected here: it mirrors onto the target as
    // a real composite primary key (`ddl::target_table_ddl`), and every
    // 1-1 consumer downstream (backfill, live CDC apply, quarantine,
    // self-check) now renders/compares it through the shared, arity-generic
    // key-contract text instead of assuming a single scalar column.
    //
    // Checked here — after node/edge resolution and the cycle/collision
    // checks above, not immediately after `qualified_source` resolves, even
    // though (unlike those checks) it doesn't depend on `qualified_target` —
    // deliberately: a chained
    // definition's source can legitimately name another *not-yet-physically-
    // built* definition's target (`resolve_graph_identity_in_txn`'s own
    // fallback), and every check above this point tolerates that
    // (they only ever read this transaction's own catalog rows/graph, never
    // the live source relation itself). `ddl::source_primary_key` is the
    // first thing in this function that actually queries the live relation
    // named by `qualified_source` — running it any earlier would turn a
    // would-be-rejected [`ValidationError::TableCycle`]/
    // [`CatalogError::TargetTableSuffixCollision`] into a confusing
    // `DdlError::NoPrimaryKey` instead, for a definition chained off a
    // target its own upstream `install_definition` call hasn't built the
    // physical table for yet.
    // Run against `txn`, not a second pooled connection (`ddl::source_primary_key`'s
    // own `pool`-taking form): this function is mid-transaction here, so taking
    // another connection would risk a pool-exhaustion deadlock and would read the
    // source relation's shape on a different snapshot than every other check
    // around it — see [`ddl::source_primary_key_in_txn`]'s own doc comment.
    //
    // Every key-space, not just `OneToOne`. Live apply calls
    // `ddl::source_primary_key` on an aggregate's source too
    // (`staging::apply`'s by-source and TRUNCATE loops), and
    // `quarantine::classify` halts the table's readers on the key-type error it
    // returns — so an aggregate over a `numeric`-keyed table, or chained off
    // a `numeric`-grouped aggregate (whose identity is its `GROUP BY` key),
    // used to be accepted here and then halt on its first live change.
    // The same checks every resume runs again ([`validate_live_schema`]).
    match &join_sides {
        Some(sides) => validate_live_join_schema(&txn, pool.schema(), &def, sides).await?,
        None => {
            validate_live_schema(&txn, pool.schema(), &def, &qualified_source).await?;
        }
    }

    // Registration creates the target here, in the transaction
    // that records the definition, so a failure anywhere after this rolls the
    // table back with the catalog row. Placed after every check on the
    // definition itself, so a definition that's wrong in its own right (a
    // table cycle, a target-suffix collision, an unkeyed source) reports
    // that, not the incidental fact that its target name is taken: a
    // cycle-closing target always already exists as a table.
    if let Some(ddl) = target_ddl {
        create_target_in_txn(
            &txn,
            resolved_target_schema,
            &def.target,
            &qualified_target,
            ddl,
        )
        .await?;
    }
    // Trellis writes the target as the role each worker connects
    // as, and policies that apply to it would filter those writes. The table
    // was just created, as the session's role with RLS off, so this only
    // fires when DDL around the creation (an event trigger) forced RLS on it
    // or handed it to another owner. The session's role stands in for the
    // workers' (see [`super::row_security`]).
    reject_row_security(
        &*txn,
        pool.schema(),
        &[(
            qualified_target.clone(),
            super::row_security::Readers::Target,
        )],
    )
    .await?;

    let (type_keys, type_vals) = encode_type_map(source_columns);

    let status_text = status.as_str();

    // `transform_definitions_target_suffix_idx`
    // is the DB-level
    // backstop for the exact same invariant the pre-check above enforces
    // optimistically — it's what actually closes the race between two
    // concurrent `create_definition` calls each resolving a different-schema
    // target for the same bare suffix, since the pre-check's `select` only
    // ever sees its own transaction's snapshot and can't see the other
    // transaction's still-uncommitted insert. This insert is normally
    // expected to succeed (the pre-check already ruled out every collision
    // its own snapshot could see); a unique-violation against that specific
    // index here means the check passed but a concurrent transaction won the
    // race and committed first — translated into the same
    // [`CatalogError::TargetTableSuffixCollision`] the pre-check raises,
    // rather than left as an opaque [`CatalogError::Db`], so a caller sees
    // one typed error for this invariant regardless of which of the two
    // paths caught it. `existing` is `None` here (contrast the pre-check's
    // `Some`): the insert failure has already aborted this transaction, so
    // there's no further query available in it to look up what was won
    // against.
    // A JOIN's joined table and its column types beside the source's.
    let joined_types = join_sides.as_ref().map(JoinSides::joined_value_types);
    let (join_keys, join_vals) = joined_types
        .as_ref()
        .map(encode_type_map)
        .unwrap_or_default();
    let join_table = join_sides.as_ref().map(|sides| sides.joined.as_str());
    let id: i64 = match txn
        .query_one(
            "insert into transform_definitions
                (target_table, source_table, source_version, definition_text, source_columns, status,
                 grammar_version, join_table, join_columns)
             values ($1, $2, $3, $4, jsonb_object($5::text[], $6::text[]), $7, $8, $9,
                     case when $9::text is null then null
                          else jsonb_object($10::text[], $11::text[]) end)
             returning id",
            &[
                &qualified_target,
                &qualified_source,
                &version,
                &source_text,
                &type_keys,
                &type_vals,
                &status_text,
                &CURRENT_GRAMMAR_VERSION,
                &join_table,
                &join_keys,
                &join_vals,
            ],
        )
        .await
    {
        Ok(row) => row.get(0),
        Err(err) if is_target_suffix_index_violation(&err) => {
            return Err(CatalogError::TargetTableSuffixCollision {
                target: def.target.clone(),
                requested: qualified_target,
                existing: None,
            });
        }
        Err(err) => return Err(err.into()),
    };
    // The type of each key column now, the one the target and
    // ledger are built from, for the staging worker's
    // capture pass to compare against (`super::key_types`).
    super::key_types::record_definition(&*txn, id, &def, &qualified_source, join_table, false)
        .await?;
    // A JOIN's hidden tables, in the catalog schema,
    // named by this definition's id, created empty with its row.
    if let Some(sides) = &join_sides {
        txn.batch_execute(&super::join::hidden_tables_ddl(
            pool.schema(),
            id,
            &def,
            &sides.from_columns,
            &sides.joined_columns,
        ))
        .await?;
    }
    // An aggregate outside the admitted set is refused by
    // family, whatever its arguments' types were when `validate` ran.
    check_aggregate_admitted(&*txn, &def, &qualified_source).await?;
    // An aggregate on the group basis keeps its
    // partials in a hidden table in the catalog schema, named by this
    // definition's id, created empty with its row. A JOIN's key space is
    // never an aggregate's, so it never routes here.
    let shape = crate::staging::group_basis::route(&def, source_columns);
    // Define's refusals and the routing agree: every aggregate define
    // accepts is on the group basis.
    debug_assert!(
        shape.is_some() || !matches!(def.key_space, KeySpace::Aggregate { .. }),
        "an admitted aggregate the group basis doesn't route: {}",
        def.target
    );
    if let Some(shape) = shape {
        txn.batch_execute(&crate::staging::group_basis::hidden_table_ddl(
            pool.schema(),
            id,
            &shape,
        ))
        .await?;
    }

    // A field reading a column paused now is paused at birth, so
    // the build this registration starts leaves it out rather than writing
    // the column's frozen value. The source's fence above is the one a pause
    // of this definition's fields bumps, so this takes no second one. Only a
    // 1-1 plan holds a paused column out; an aggregate or `JOIN` reading one
    // reports `catching_up` instead (`reported_statuses_with_causes`).
    if matches!(def.key_space, KeySpace::OneToOne) {
        let upstreams = definition_targets_read(&*txn, &qualified_source).await?;
        let all_fields: Vec<String> = def.fields.iter().map(|f| f.name.clone()).collect();
        pause_readers_of_paused_columns(
            &*txn,
            &def.target,
            &def,
            &all_fields,
            &qualified_source,
            &upstreams,
            false,
        )
        .await?;
    }

    txn.commit().await?;

    Ok(Definition {
        id,
        source_version: version,
        def,
        source_columns: source_columns.clone(),
        status,
        source_table: qualified_source,
        target_table: qualified_target,
    })
}

/// Creates a definition's target table inside the registration transaction
/// `txn`, running `ddl` ([`install_definition`]'s rendered `create table`) —
/// unless a relation named `schema.table` already exists, which is refused
/// with [`CatalogError::TargetTableExists`] and left untouched.
///
/// Any relation kind counts (table, view, materialized view, foreign table,
/// and also an index or sequence, which would collide just the same), and so
/// does another definition's target: that one is Trellis's own, but it
/// belongs to that definition, not this one. The check is a clean error for
/// the common case; a concurrent registration that creates the same name
/// between the check and the `create table` fails the DDL instead, which
/// [`is_relation_name_taken`] maps to the same error.
async fn create_target_in_txn(
    txn: &tokio_postgres::Transaction<'_>,
    schema: &str,
    table: &str,
    qualified_target: &str,
    ddl: &str,
) -> Result<(), CatalogError> {
    let already_exists = |taken: &str| CatalogError::TargetTableExists {
        table: if taken == table {
            qualified_target.to_string()
        } else {
            format!("{schema}.{taken}")
        },
    };
    // `ddl` also creates a 1-1 target's ledger, which Trellis owns
    // just the same, so its name has to be free too.
    let ledger = super::ledger::ledger_table_name(table);
    let taken: Option<String> = txn
        .query_opt(
            "select c.relname::text from pg_catalog.pg_class c \
             join pg_catalog.pg_namespace n on n.oid = c.relnamespace \
             where n.nspname = $1 and c.relname = any($2) \
             order by c.relname = $3 desc limit 1",
            &[&schema, &[table, ledger.as_str()].as_slice(), &table],
        )
        .await?
        .map(|row| row.get(0));
    if let Some(taken) = taken {
        return Err(already_exists(&taken));
    }
    match txn.batch_execute(ddl).await {
        Ok(()) => Ok(()),
        Err(err) if is_relation_name_taken(&err) => Err(already_exists(table)),
        Err(err) => Err(CatalogError::Ddl(err.into())),
    }
}

/// Whether a failed `create table` failed because its name was taken.
/// `duplicate_table` covers a relation that was already visible; a
/// concurrent creator that committed while this `create table` waited on it
/// instead trips the unique index on `pg_type` (or `pg_class`) directly, as a
/// plain `unique_violation` naming that system index.
fn is_relation_name_taken(err: &tokio_postgres::Error) -> bool {
    let Some(db_err) = err.as_db_error() else {
        return false;
    };
    *db_err.code() == tokio_postgres::error::SqlState::DUPLICATE_TABLE
        || (*db_err.code() == tokio_postgres::error::SqlState::UNIQUE_VIOLATION
            && matches!(
                db_err.constraint(),
                Some("pg_type_typname_nsp_index" | "pg_class_relname_nsp_index")
            ))
}

/// Whether `err` is a unique-violation against
/// `transform_definitions_target_suffix_idx`
/// specifically — the
/// signal [`create_definition_inner`]'s final insert uses to tell "a
/// concurrent transaction won the bare-target-suffix race the pre-check
/// above couldn't see" apart from any other constraint violation the same
/// insert could raise (most notably `transform_definitions_target_table_key`,
/// an exact-qualified-name duplicate, which stays a raw [`CatalogError::Db`]
/// — see `a_duplicate_target_table_surfaces_the_underlying_postgres_detail`).
/// Matched by constraint/index name, not just [`tokio_postgres::error::SqlState::UNIQUE_VIOLATION`]
/// alone, since that SQLSTATE alone can't distinguish the two — mirrors
/// `staging::quarantine::is_undefined_table`'s style of a small, named
/// `&tokio_postgres::Error -> bool` predicate rather than inlining the check
/// at its one call site.
fn is_target_suffix_index_violation(err: &tokio_postgres::Error) -> bool {
    let Some(db_err) = err.as_db_error() else {
        return false;
    };
    *db_err.code() == tokio_postgres::error::SqlState::UNIQUE_VIOLATION
        && db_err.constraint() == Some("transform_definitions_target_suffix_idx")
}

/// One registered definition, as [`capture_readers`] reads it.
#[derive(Debug)]
pub(crate) struct CaptureReaderRow {
    pub id: i64,
    /// The qualified `source_table`.
    pub source: String,
    pub def: TransformDef,
    /// A `schema_changed` marker paused it (C6, `capture_failures`).
    pub capture_failed: bool,
    /// The qualified `target_table`.
    pub target: String,
    /// A `JOIN`'s qualified joined table.
    pub join: Option<String>,
}

/// Every registered definition ([`CaptureReaderRow`]).
/// This is the whole input `crate::capture::columns`
/// needs to decide which columns a table's capture trigger images.
///
/// Definitions in every status count. A table is captured from the moment
/// something registers a reader of it, before that reader builds, and a
/// paused or quarantined reader still needs its columns when it resumes.
/// The exception is a definition older than the grammar window (ADR-0018):
/// attaching paused it, it never resumes, and its fix is a drop and a fresh
/// define, so it reads nothing and is left out. A definition newer than the
/// window fails the read, since its columns can't be known.
pub(crate) async fn capture_readers(
    client: &impl GenericClient,
) -> Result<Vec<CaptureReaderRow>, CatalogError> {
    client
        .query_typed(
            "select d.id, d.source_table, d.definition_text, \
                    exists (select 1 from capture_failures f where f.transform_id = d.id), \
                    d.target_table, d.grammar_version, d.join_table \
             from transform_definitions d order by d.id",
            &[],
        )
        .await?
        .into_iter()
        .filter_map(|row| match grammar::load(row.get(2), row.get(5)) {
            Ok(def) => Some(Ok(CaptureReaderRow {
                id: row.get(0),
                source: row.get(1),
                def,
                capture_failed: row.get(3),
                target: row.get(4),
                join: row.get(6),
            })),
            Err(ParseError::GrammarVersionTooOld { .. }) => None,
            Err(e) => Some(Err(e.into())),
        })
        .collect::<Result<Vec<_>, CatalogError>>()
}

/// Resolves `source_table`'s actual schema the same way Postgres itself
/// would resolve the bare, unqualified name: the first schema on this
/// connection's `search_path` (`current_schemas(false)`, in `search_path`
/// order) that actually has a table by that name. Mirrors `defctl`'s own
/// `source_columns`/`qualified_source_tables` introspection — see
/// [`create_definition`]'s call site for why this replaced an
/// `is_target`-based guess.
///
/// **Only ever call this on a *bare* name freshly parsed from a
/// definition's own source text** (ADR-0011) — `def.source`, never a value
/// read back from `transform_definitions.source_table`/
/// `source_table_versions.source_table`. Those columns hold the *qualified*
/// result this function already produced once, at the definition's own
/// acceptance time; feeding a qualified `"schema.table"` string
/// back in here wouldn't just be redundant, it would always fail — this
/// query filters `information_schema.tables` by bare `table_name`, which a
/// qualified string never matches, so every call would return
/// [`CatalogError::SourceTableNotFound`].
async fn resolve_source_schema_in_txn(
    txn: &tokio_postgres::Transaction<'_>,
    source_table: &str,
) -> Result<String, CatalogError> {
    let row = txn
        .query_opt(
            "select table_schema from information_schema.tables \
             where table_name = $1 and table_schema = any(current_schemas(false)) \
             order by array_position(current_schemas(false), table_schema) \
             limit 1",
            &[&source_table],
        )
        .await?;
    row.map(|row| row.get(0))
        .ok_or_else(|| CatalogError::SourceTableNotFound(source_table.to_string()))
}

/// Resolves a *bare* table name to its fully-qualified `schema.table`
/// identity for the `schema_nodes`/`schema_edges` graph
/// (ADR-0011) — the single resolution [`create_definition_inner`] calls,
/// early, before touching the graph at all: its two `resolve_node_in_txn`
/// calls, [`reject_if_table_cycle`],
/// and (for [`create_definition_inner`] specifically) the row persisted to
/// `source_table_versions`/`transform_definitions.source_table` all share
/// this one qualified value rather than each re-deriving their own.
///
/// Two-step, mirroring the two ways a bare name can already be real:
///
/// 1. The common case: `table` physically exists, so
///    [`resolve_source_schema_in_txn`]'s `search_path` walk finds it
///    directly — the same resolution used for `qualified_source`.
/// 2. `table` instead names another definition's own persisted target
///    (`transform_definitions.target_table`) — a legitimate
///    chained `FROM`, but not guaranteed to be backed
///    by a physical table at the instant this call names it: the
///    catalog-only test-fixture entry points
///    ([`create_definition`]/[`create_definition_without_backfill`]) are
///    documented as expecting their *caller* to have already created the
///    physical target (see [`install_definition`]'s doc comment on its own
///    identically-shaped DDL step), a precondition this module doesn't
///    itself enforce and that several of its own lighter-weight tests
///    deliberately skip (seeding `schema_nodes` state directly, or chaining
///    a second `create_definition` off a target never backfilled with a
///    real table). Recovered here via `transform_definitions`' bare
///    target-suffix index (`split_part(target_table, '.', 2) = table`) —
///    the same invariant [`CatalogError::TargetTableSuffixCollision`]
///    already keeps globally unique, so this lookup is never ambiguous
///    between two live definitions.
///
/// This ordering — physical resolution first, the catalog's own record of a
/// chained target second — is what lets the graph resolution above run
/// *before* [`reject_if_table_cycle`] without losing one case: a
/// definition that both (a) closes a cycle and (b) names a not-yet-
/// materialized chained source as its `FROM` must still report
/// [`ValidationError::TableCycle`], not a confusing not-found error. The
/// graph requires qualified identity, so qualification has to run
/// first — and it's this function's step 2, not error-ordering, that keeps
/// that guarantee: resolution itself succeeds for a not-yet-materialized
/// chained target, so the cycle check that runs after it is reached at all,
/// and reports the cycle.
///
/// Only ever call this on a bare name freshly parsed from a definition's
/// own source text (same restriction as
/// [`resolve_source_schema_in_txn`]) — never on a value already read back
/// from a qualified column.
async fn resolve_graph_identity_in_txn(
    txn: &tokio_postgres::Transaction<'_>,
    table: &str,
) -> Result<String, CatalogError> {
    match resolve_source_schema_in_txn(txn, table).await {
        Ok(schema) => Ok(crate::intake::qualify(&schema, table)?),
        Err(CatalogError::SourceTableNotFound(_)) => {
            let row = txn
                .query_opt(
                    "select target_table from transform_definitions \
                     where split_part(target_table, '.', 2) = $1 \
                     limit 1",
                    &[&table],
                )
                .await?;
            match row {
                Some(row) => Ok(row.get(0)),
                None => Err(CatalogError::SourceTableNotFound(table.to_string())),
            }
        }
        Err(err) => Err(err),
    }
}

/// Pooled (non-transaction) counterpart to [`resolve_graph_identity_in_txn`]
/// — same two-step resolution (physical `search_path` lookup, falling back
/// to a live definition's own bare target-suffix), for callers outside a
/// catalog-owned transaction. `staging::apply::compute`'s
/// `qualified_schema_node_key` is one caller: a `Recompute` row's `src_table`
/// reaching `compute` bare is this same apply path's own
/// downstream-propagation trigger for a chained definition's target — step 2
/// here, the bare-target-suffix fallback.
pub(crate) async fn resolve_graph_identity(
    pool: &Pool,
    table: &str,
) -> Result<String, CatalogError> {
    match resolve_source_schema(pool, table).await {
        Ok(schema) => Ok(crate::intake::qualify(&schema, table)?),
        Err(CatalogError::SourceTableNotFound(_)) => {
            let client = pool.get().await?;
            let row = client
                .query_opt(
                    "select target_table from transform_definitions \
                     where split_part(target_table, '.', 2) = $1 \
                     limit 1",
                    &[&table],
                )
                .await?;
            match row {
                Some(row) => Ok(row.get(0)),
                None => Err(CatalogError::SourceTableNotFound(table.to_string())),
            }
        }
        Err(err) => Err(err),
    }
}

/// The target schema `def` actually resolves against (ADR-0011
/// grammar clause 4): an explicit `TRANSFORM <schema>.<target>` spelling
/// ([`TransformDef::explicit_target_schema`]) overrides `target_schema`
/// (`Config::target_schema`, or the caller's own override) outright — a
/// qualified spelling names its own schema, it doesn't inherit the
/// configured default. `None` (the bare, common case) keeps using
/// `target_schema`.
fn effective_target_schema<'a>(def: &'a TransformDef, target_schema: &'a str) -> &'a str {
    def.explicit_target_schema
        .as_deref()
        .unwrap_or(target_schema)
}

/// Pooled (non-transaction) counterpart to [`resolve_source_schema_in_txn`],
/// the `search_path` step of [`resolve_graph_identity`], for callers on plain
/// pooled connections outside a catalog transaction — mirrors
/// [`column_type`]/[`column_type_in_txn`]'s same pool-vs-txn split. Same
/// `search_path` walk, same [`CatalogError::SourceTableNotFound`] on no
/// match.
async fn resolve_source_schema(pool: &Pool, source_table: &str) -> Result<String, CatalogError> {
    let client = pool.get().await?;
    let row = client
        .query_opt(
            "select table_schema from information_schema.tables \
             where table_name = $1 and table_schema = any(current_schemas(false)) \
             order by array_position(current_schemas(false), table_schema) \
             limit 1",
            &[&source_table],
        )
        .await?;
    row.map(|row| row.get(0))
        .ok_or_else(|| CatalogError::SourceTableNotFound(source_table.to_string()))
}

/// Refuses `table` with [`CatalogError::TableNotAccessible`] when it exists
/// but the role this session runs as can't use it. `schema`
/// names the schema a qualified reference spelled; `None` looks where a bare
/// name would resolve, the schemas this session's `search_path` setting
/// lists.
///
/// The lookup reads `pg_class` and `pg_namespace`, which show every relation
/// whatever the role's privileges, because the two places a name resolves
/// through hide exactly the tables this is for: `current_schemas` leaves out
/// a schema the role lacks `USAGE` on, and `information_schema.tables` a
/// table it holds no privilege on. "Holds a privilege" is
/// `information_schema.tables`' own test, so a table this passes is one that
/// view shows.
///
/// `Ok(())` when nothing by that name exists in those schemas, or the first
/// one the path reaches is usable: the caller's own not-found (or whatever
/// else it found) stands. Define paths call this only once a name failed to
/// resolve, or on a qualified name before reading it, so a name that
/// resolves never costs the lookup; and the runtime's own resolution
/// ([`resolve_graph_identity`]) never calls it.
pub(crate) async fn reject_inaccessible_table(
    client: &impl GenericClient,
    schema: Option<&str>,
    table: &str,
) -> Result<(), CatalogError> {
    let schemas = match schema {
        Some(schema) => vec![schema.to_string()],
        None => {
            let row = client
                .query_one(
                    "select current_setting('search_path'), current_user::text",
                    &[],
                )
                .await?;
            search_path_schemas(row.get(0), row.get(1))
        }
    };
    let Some(row) = client
        .query_opt(
            "select n.nspname::text, current_user::text, pg_get_userbyid(c.relowner)::text, \
                    has_schema_privilege(n.oid, 'USAGE'), \
                    pg_has_role(c.relowner, 'USAGE') \
                      or has_table_privilege(c.oid, \
                           'SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER') \
                      or has_any_column_privilege(c.oid, 'SELECT, INSERT, UPDATE, REFERENCES') \
             from pg_catalog.pg_class c \
             join pg_catalog.pg_namespace n on n.oid = c.relnamespace \
             where c.relname::text = $1 and n.nspname::text = any($2) \
               and c.relkind in ('r', 'p', 'v', 'f') \
             order by array_position($2, n.nspname::text) \
             limit 1",
            &[&table, &schemas],
        )
        .await?
    else {
        return Ok(());
    };
    // The privilege functions return null for a table or schema dropped
    // since the scan above read it: the caller's not-found stands.
    let (Some(has_schema_usage), Some(has_table_privilege)) =
        (row.get::<_, Option<bool>>(3), row.get::<_, Option<bool>>(4))
    else {
        return Ok(());
    };
    if has_schema_usage && has_table_privilege {
        return Ok(());
    }
    let schema: String = row.get(0);
    Err(CatalogError::TableNotAccessible {
        table: format!("{schema}.{table}"),
        role: row.get(1),
        owner: row.get(2),
        lacks_schema_usage: !has_schema_usage,
        lacks_table_privilege: !has_table_privilege,
    })
}

/// The schemas a `search_path` setting lists, in order, the way Postgres
/// splits it (`SplitIdentifierString`): comma-separated, a double-quoted
/// element taken as written (`""` is a quote), an unquoted one with its ASCII
/// letters lowercased, and `$user` read as `user`. Schemas that don't exist
/// are kept; the caller matches them against `pg_namespace`. Postgres also
/// truncates a name to 63 bytes, which this doesn't: an over-long name just
/// matches nothing, so the caller's not-found stands.
fn search_path_schemas(setting: &str, user: &str) -> Vec<String> {
    let mut schemas = Vec::new();
    let mut chars = setting.chars().peekable();
    loop {
        while chars.next_if(|c| c.is_whitespace()).is_some() {}
        let Some(&first) = chars.peek() else { break };
        let mut name = String::new();
        if first == '"' {
            chars.next();
            while let Some(c) = chars.next() {
                if c == '"' && chars.next_if_eq(&'"').is_none() {
                    break;
                }
                name.push(c);
            }
        } else {
            // `downcase_identifier`: ASCII only, in a UTF-8 database.
            while let Some(c) = chars.next_if(|c| *c != ',' && !c.is_whitespace()) {
                name.push(c.to_ascii_lowercase());
            }
        }
        schemas.push(if name == "$user" {
            user.to_string()
        } else {
            name
        });
        while chars.next_if(|c| *c != ',').is_some() {}
        if chars.next().is_none() {
            break;
        }
    }
    schemas
}

/// [`resolve_source_for_install`] for a definition being defined: a bare
/// source that doesn't resolve because the role can't see it is refused as
/// [`CatalogError::TableNotAccessible`] rather than not found.
async fn resolve_source_to_define(pool: &Pool, def: &TransformDef) -> Result<String, CatalogError> {
    match resolve_source_for_install(pool, def).await {
        Err(err @ CatalogError::SourceTableNotFound(_)) => {
            reject_inaccessible_table(&**pool.get().await?, None, &def.source).await?;
            Err(err)
        }
        result => result,
    }
}

/// Confirms an explicitly qualified `FROM <schema>.<table>` names a table the
/// role can use and that exists, refusing it as
/// [`CatalogError::TableNotAccessible`] or
/// [`ValidationError::QualifiedSourceTableNotFound`]. The privilege check
/// runs first: a role without `USAGE` on the schema still finds the table in
/// `information_schema.tables`, and one without any privilege on the table
/// doesn't.
async fn confirm_qualified_source(
    client: &impl GenericClient,
    schema: &str,
    table: &str,
) -> Result<(), CatalogError> {
    reject_inaccessible_table(client, Some(schema), table).await?;
    if !qualified_table_exists(client, schema, table).await? {
        return Err(ValidationError::QualifiedSourceTableNotFound {
            schema: schema.to_string(),
            table: table.to_string(),
        }
        .into());
    }
    Ok(())
}

/// Refuses a 1-1 field named after one of `qualified_source`'s primary key
/// columns — see [`reject_primary_key_named_fields`]. Every key
/// column is a source column, so the key is only introspected when some field
/// shares a name with one of `source_columns`: a definition with none costs
/// no query.
async fn reject_fields_named_after_primary_key(
    client: &impl GenericClient,
    qualified_source: &str,
    fields: &[FieldDef],
    source_columns: &HashMap<String, ValueType>,
) -> Result<(), CatalogError> {
    if !fields.iter().any(|f| source_columns.contains_key(&f.name)) {
        return Ok(());
    }
    let pk = ddl::source_primary_key_in_txn(client, qualified_source)
        .await
        .map_err(CatalogError::Ddl)?;
    let pk_names: Vec<&str> = pk.iter().map(|c| c.name.as_str()).collect();
    reject_primary_key_named_fields(fields, &pk_names)?;
    Ok(())
}

/// The schema half of a qualified `schema.table` name.
fn schema_of(qualified: &str) -> Option<&str> {
    qualified.split_once('.').map(|(schema, _)| schema)
}

/// [`validate`] for a definition whose source resolved to `qualified_source`:
/// a three-part qualified column must name that schema, and an
/// ambiguous-name refusal spells it in the qualified names it suggests.
fn validate_with_source(
    def: &TransformDef,
    source_columns: &HashMap<String, ValueType>,
    qualified_source: &str,
) -> Result<(), ValidationError> {
    let schema = schema_of(qualified_source);
    validate(def, source_columns).map_err(|err| match schema {
        Some(schema) => err.with_source_schema(schema),
        None => err,
    })?;
    validate_qualified_columns(def, source_columns, schema)
}

/// [`validate`] for a definition being defined, before its source is
/// resolved. Resolves the source only to spell its schema in an
/// ambiguous-name refusal; a definition that fails some other
/// way, or passes, never resolves it here.
///
/// A `JOIN` definition is checked against both tables' live columns instead
/// ([`JoinSides::resolve`], [`super::validate::validate_join`]).
async fn validate_for_define(
    pool: &Pool,
    def: &TransformDef,
    source_columns: &HashMap<String, ValueType>,
) -> Result<(), CatalogError> {
    if let KeySpace::Join(_) = &def.key_space {
        let sides = JoinSides::resolve(pool, def).await?;
        return Ok(super::validate::validate_join(
            def,
            &sides.from_columns,
            &sides.joined_columns,
        )?);
    }
    match validate(def, source_columns) {
        Err(err @ ValidationError::AmbiguousColumnReference(_)) => {
            let err = match resolve_source_for_install(pool, def).await {
                Ok(qualified) => match schema_of(&qualified) {
                    Some(schema) => err.with_source_schema(schema),
                    None => err,
                },
                Err(_) => err,
            };
            Err(err.into())
        }
        result => Ok(result?),
    }
}

/// The two tables of a `JOIN` definition, resolved to their
/// qualified identities, with their live columns as
/// [`super::validate::validate_join`] reads them.
#[derive(Debug, Clone)]
pub(crate) struct JoinSides {
    /// The `FROM` table.
    pub from: String,
    /// The joined table.
    pub joined: String,
    pub from_columns: HashMap<String, super::validate::JoinColumnInfo>,
    pub joined_columns: HashMap<String, super::validate::JoinColumnInfo>,
}

impl JoinSides {
    /// Resolves both tables of `def` the way a `FROM` resolves (an explicit
    /// schema is confirmed, a bare name walks `search_path`) and reads their
    /// columns.
    async fn resolve(pool: &Pool, def: &TransformDef) -> Result<JoinSides, CatalogError> {
        let Some(join) = super::join::spec(def) else {
            panic!("JoinSides::resolve called on a definition that isn't a JOIN");
        };
        let from = resolve_source_for_install(pool, def).await?;
        let joined = match &join.explicit_right_schema {
            Some(schema) => {
                confirm_qualified_source(&**pool.get().await?, schema, &join.right).await?;
                crate::intake::qualify(schema, &join.right)?
            }
            None => resolve_graph_identity(pool, &join.right).await?,
        };
        let client = pool.get().await?;
        JoinSides::read(&**client, from, joined).await
    }

    /// Reads the live columns of the already-qualified `from` and `joined`.
    pub(crate) async fn read(
        client: &impl GenericClient,
        from: String,
        joined: String,
    ) -> Result<JoinSides, CatalogError> {
        let from_columns = join_columns_of(client, &from).await?;
        let joined_columns = join_columns_of(client, &joined).await?;
        Ok(JoinSides {
            from,
            joined,
            from_columns,
            joined_columns,
        })
    }

    /// The two tables in table-name order: the order define and a resume
    /// bump their version fences in.
    fn tables_in_order(&self) -> [&str; 2] {
        let mut tables = [self.from.as_str(), self.joined.as_str()];
        tables.sort_unstable();
        tables
    }

    /// The joined side's column types, as `transform_definitions.join_columns`
    /// records them.
    pub(crate) fn joined_value_types(&self) -> HashMap<String, ValueType> {
        self.joined_columns
            .iter()
            .map(|(name, info)| (name.clone(), info.value_type))
            .collect()
    }
}

/// Refuses a plain aggregate over `qualified_source` that calls an aggregate
/// outside the admitted set, by family
/// ([`super::validate::validate_aggregate_admitted`]), against the source's
/// live columns. Define makes it in its transaction, and a resume's
/// [`revalidate`] again, so a summed column re-typed since (to an
/// unconstrained `numeric`, or a float) refuses the resume.
async fn check_aggregate_admitted(
    client: &impl GenericClient,
    def: &TransformDef,
    qualified_source: &str,
) -> Result<(), CatalogError> {
    if !matches!(def.key_space, KeySpace::Aggregate { .. }) {
        return Ok(());
    }
    let columns = join_columns_of(client, qualified_source).await?;
    Ok(super::validate::validate_aggregate_admitted(def, &columns)?)
}

/// Every column of `qualified` as [`super::validate::validate_join`] reads
/// it. Like [`live_source_columns`], a column whose type the registry can't
/// place is left out, so a reference to it is an unresolved column.
async fn join_columns_of(
    client: &impl GenericClient,
    qualified: &str,
) -> Result<HashMap<String, super::validate::JoinColumnInfo>, CatalogError> {
    let rows = client
        .query(
            &format!(
                "select a.attname::text,
                    pg_catalog.format_type(a.atttypid, a.atttypmod),
                    a.atttypid,
                    a.atttypmod,
                    a.attcollation,
                    co.collname::text,
                    coalesce(co.collisdeterministic, true),
                    t.typtype = 'e',
                    {},
                    {}
             from pg_attribute a
             join pg_catalog.pg_type t on t.oid = a.atttypid
             left join pg_catalog.pg_collation co on co.oid = a.attcollation
             where a.attrelid = pg_catalog.to_regclass($1)
               and a.attnum > 0
               and not a.attisdropped",
                super::copy_type::type_sql("a.atttypid"),
                super::copy_type::collation_sql("a.attcollation")
            ),
            &[&ddl::regclass_arg(qualified)],
        )
        .await?;
    // A column of a table Trellis creates has no type modifier (ADR-0020).
    let trellis_target: bool = client
        .query_one(
            "select exists (select 1 from transform_definitions where target_table = $1)",
            &[&qualified],
        )
        .await?
        .get(0);
    let mut columns = HashMap::with_capacity(rows.len());
    for row in rows {
        let type_oid: u32 = row.get(2);
        let value_type = super::pg_type::value_type_for_oid(client, type_oid).await?;
        if value_type == ValueType::Other(PgType::Unrecognized) {
            continue;
        }
        columns.insert(
            row.get::<_, String>(0),
            super::validate::JoinColumnInfo {
                value_type,
                pg_type: row.get(1),
                type_oid,
                typmod: row.get(3),
                collation_oid: row.get(4),
                collation: row.get(5),
                collation_deterministic: row.get(6),
                is_enum: row.get(7),
                copy_type: row.get(8),
                copy_collation: row.get(9),
                trellis_target,
            },
        );
    }
    Ok(columns)
}

/// The fully-qualified source [`install_definition`]'s own DDL steps read
/// from (ADR-0011) — computed once, early in
/// that function, exactly like `target_schema`/[`effective_target_schema`]
/// immediately above it, and threaded through every one of those steps
/// (`ddl::source_primary_key`, `ddl::target_table_ddl`) so none of them can
/// independently re-derive a different answer, and so every physical SQL
/// builder among them emits the qualified identity rather than a bare
/// `def.source` left to the executing connection's own `search_path`
/// (ADR-0011, clause 3: "Every generated statement emits qualified names").
///
/// `create_definition_inner` (further below) computes its *own* copy inside
/// its own transaction rather than receiving this one as a parameter — see
/// that function's doc comment on `qualified_source` for why: it's the sole,
/// authoritative resolution the test-fixture entry points ([`create_definition`]/
/// [`create_definition_without_backfill`], which never call this function)
/// ever get, so it must stand on its own regardless of what this function
/// computed a few statements earlier. The two are expected to agree (same
/// source text, same connection pool, no concurrent DDL moving `def.source`
/// between the two calls) — matching this same function's caller's own
/// fail-fast explicit-schema check just above it, also redundant-but-harmless
/// against `create_definition_inner`'s copy.
///
/// The bare (`None`) branch uses [`resolve_graph_identity`], the same
/// two-step resolution (with the bare-target-suffix fallback) as
/// `create_definition_inner`'s own resolution of the exact same bare
/// `def.source` (its `qualified_source`, above), and the same pooled
/// resolution `staging::apply::compute`'s `qualified_schema_node_key` reuses.
/// This function's DDL steps run and can fail *before*
/// `create_definition_inner` is ever reached, so a bare `FROM <name>`
/// chained off another definition's target explicitly qualified into a
/// non-default schema must resolve here too, rather than failing as a plain
/// "not found on the search path".
pub(crate) async fn resolve_source_for_install(
    pool: &Pool,
    def: &TransformDef,
) -> Result<String, CatalogError> {
    match &def.explicit_source_schema {
        Some(schema) => Ok(crate::intake::qualify(schema, &def.source)?),
        None => resolve_graph_identity(pool, &def.source).await,
    }
}

/// Whether `schema.table` is a real relation (ADR-0011 grammar
/// clause 4): the existence check an explicitly-qualified `FROM`/`TRANSFORM`
/// reference gets *instead of* [`resolve_source_schema_in_txn`]'s
/// `search_path` walk. A qualified spelling names its schema directly, so
/// this checks that one relation is real rather than asking which schema on
/// the path would have won. Same `information_schema.tables` table that
/// function itself queries, just filtered to the one named schema instead of
/// `current_schemas(false)`. A qualified source goes through
/// [`confirm_qualified_source`], which checks the role can use it first.
async fn qualified_table_exists(
    client: &impl GenericClient,
    schema: &str,
    table: &str,
) -> Result<bool, CatalogError> {
    let exists: bool = client
        .query_one(
            "select exists (select 1 from information_schema.tables \
             where table_schema = $1 and table_name = $2)",
            &[&schema, &table],
        )
        .await?
        .get(0);
    Ok(exists)
}

/// A column's type and collation as Postgres reports them (`pg_attribute`,
/// `pg_collation`), for [`assert_deterministic_key_collation`]
/// and the collation check of a text function's argument.
#[derive(Debug, Clone, PartialEq, Eq)]
struct LiveColumn {
    /// The collation's `collname` (`default` for the database default),
    /// `None` for a type that isn't collatable.
    collation: Option<String>,
    /// `pg_collation.collisdeterministic`; `true` when there is no
    /// collation.
    collation_deterministic: bool,
}

/// `query_table.column`'s collation: `None` for a table or column that
/// doesn't exist. Serves the key-column collation check
/// ([`assert_deterministic_key_collation_in_txn`]), whose caller
/// has already resolved the column some other way.
async fn column_in_txn(
    txn: &impl GenericClient,
    query_table: &str,
    column: &str,
) -> Result<Option<LiveColumn>, CatalogError> {
    let row = txn
        .query_opt(
            "select co.collname::text, coalesce(co.collisdeterministic, true)
             from pg_attribute a
             left join pg_catalog.pg_collation co on co.oid = a.attcollation
             where a.attrelid = pg_catalog.to_regclass($1)
               and a.attname = $2
               and a.attnum > 0
               and not a.attisdropped",
            &[&ddl::regclass_arg(query_table), &column],
        )
        .await?;
    Ok(row.map(|row| LiveColumn {
        collation: row.get(0),
        collation_deterministic: row.get(1),
    }))
}

/// Rejects `table.column`, a column the engine matches as a key in the role
/// `key`, if [`column_in_txn`] finds it has a nondeterministic collation.
/// A column that doesn't exist passes: every caller has already
/// resolved it (it came from the table's own key index, or [`validate`]
/// checked it), and the check that resolved it owns that error.
async fn assert_deterministic_key_collation_in_txn(
    txn: &tokio_postgres::Transaction<'_>,
    key: KeyColumnRole,
    table: &str,
    column: &str,
) -> Result<(), CatalogError> {
    match column_in_txn(txn, table, column).await? {
        Some(resolved) => Ok(assert_deterministic_key_collation(
            key, table, column, &resolved,
        )?),
        None => Ok(()),
    }
}

/// [`assert_deterministic_key_collation_in_txn`]'s decision, on a column
/// already looked up. The key-column twin of
/// [`assert_deterministic_join_collation`]: a nondeterministic collation's
/// `=` (and so its `GROUP BY` and unique indexes) treats distinct strings as
/// equal, while the engine matches keys by their exact text, so Postgres
/// would fold rows the engine keeps apart. Refused, never reinterpreted
/// by design.
///
/// A column with no collation, or the database default's, always passes:
/// Postgres has no nondeterministic database default (`CREATE DATABASE`
/// takes no `deterministic` option, and the `default` collation's
/// `collisdeterministic` is always true), which is also why a target Trellis
/// creates can't have a nondeterministic key. The check reads
/// `collisdeterministic` rather than assuming that, so it would still hold if
/// Postgres ever allowed one.
fn assert_deterministic_key_collation(
    key: KeyColumnRole,
    table: &str,
    column: &str,
    resolved: &LiveColumn,
) -> Result<(), ValidationError> {
    if resolved.collation_deterministic {
        return Ok(());
    }
    Err(ValidationError::NondeterministicKeyCollation(Box::new(
        NondeterministicKeyCollation {
            key,
            table: table.to_string(),
            column: column.to_string(),
            collation: resolved.collation.clone().unwrap_or_default(),
        },
    )))
}

/// Every check of definition `def`, sourced from `qualified_source`, that
/// reads the live schema, in the order define runs them once its source
/// resolves. Define runs it before it creates the target; every resume runs
/// it again ([`revalidate`]), so a definition the schema has
/// drifted from is refused, by the same rule and with the same error, on
/// either path. Returns the source's row-identity key.
///
/// - **The source key** ([`ddl::source_primary_key_in_txn`]): every
///   key-space's, since live apply keys an aggregate's changes by it too,
///   and a type off the key allowlist would halt its readers. Its
///   columns, and any `GROUP BY` key, need a deterministic collation, and
///   so does any column a field's `STRPOS`/`REGEXP_COUNT` reads. A
///   source that capture can't key is refused.
/// - **Row-level security and subscriptions**: row-level
///   security that applies to a role reading the source
///   ([`side_read_table`]) would refuse the reads (every Trellis session runs
///   with `row_security = off`), and a table a logical-replication
///   subscription writes is one whose changes capture never sees. Define
///   checks row-level security once already; the second check here is the
///   resume's.
pub(crate) async fn validate_live_schema(
    txn: &tokio_postgres::Transaction<'_>,
    schema: &str,
    def: &TransformDef,
    qualified_source: &str,
) -> Result<Vec<ddl::PrimaryKeyColumn>, CatalogError> {
    let source_key = ddl::source_primary_key_in_txn(txn, qualified_source)
        .await
        .map_err(CatalogError::Ddl)?;
    assert_deterministic_definition_keys(txn, def, qualified_source, &source_key).await?;
    assert_collation_safe_text_functions(txn, def, qualified_source, None).await?;
    reject_unkeyed_source(txn, qualified_source, def).await?;

    let read_tables = vec![side_read_table(txn, qualified_source).await?];
    reject_row_security(txn, schema, &read_tables).await?;
    let read_tables: Vec<&str> = read_tables
        .iter()
        .map(|(table, _)| table.as_str())
        .collect();
    reject_subscribed(txn, &read_tables).await?;
    Ok(source_key)
}

/// [`validate_live_schema`] for a `JOIN` definition: each side is
/// held to the rules a definition's source is (a usable row-identity key of
/// deterministic collation, a primary key or another definition's target),
/// and neither may be under row-level security that applies to its readers
/// or replicated into by a subscription. The join and `GROUP BY` columns'
/// types and collations are [`super::validate::validate_join`]'s.
pub(crate) async fn validate_live_join_schema(
    txn: &tokio_postgres::Transaction<'_>,
    schema: &str,
    def: &TransformDef,
    sides: &JoinSides,
) -> Result<(), CatalogError> {
    for side in [&sides.from, &sides.joined] {
        let key = ddl::source_primary_key_in_txn(txn, side)
            .await
            .map_err(CatalogError::Ddl)?;
        assert_deterministic_definition_keys(txn, def, side, &key).await?;
        reject_unkeyed_source(txn, side, def).await?;
    }
    let read_tables = vec![
        side_read_table(txn, &sides.from).await?,
        side_read_table(txn, &sides.joined).await?,
    ];
    reject_row_security(txn, schema, &read_tables).await?;
    reject_subscribed(txn, &[sides.from.as_str(), sides.joined.as_str()]).await
}

/// A table a definition reads (its source, or a side of a `JOIN`) with the
/// roles that read it as row-level security sees them: the ring's
/// owner, or the session's role alone when it is another definition's
/// target, which the target-mutation seam feeds and no capture function
/// reads ([`super::row_security::Readers::Session`]).
async fn side_read_table(
    client: &impl GenericClient,
    table: &str,
) -> Result<(String, super::row_security::Readers), CatalogError> {
    let readers = if is_definition_target(client, table).await? {
        super::row_security::Readers::Session
    } else {
        super::row_security::Readers::Ring
    };
    Ok((table.to_string(), readers))
}

/// Refuses `target` (`schema.table`) with
/// [`CatalogError::TargetInCatalogSchema`] when its schema is
/// `catalog_schema`, this instance's catalog schema, or holds a
/// `trellis_instance` table, another instance's. A 1-1 target's
/// ledger lives in the target's schema, so this covers it too.
/// `pg_class`, not `information_schema`, which hides a table the role has no
/// privilege on, as [`crate::identity`]'s scan does.
async fn reject_target_in_catalog_schema(
    client: &impl GenericClient,
    catalog_schema: &str,
    target: &str,
) -> Result<(), CatalogError> {
    let Some(schema) = schema_of(target) else {
        return Ok(());
    };
    let own = schema == catalog_schema;
    if !own {
        let holds_catalog: bool = client
            .query_one(
                "select exists (select 1 from pg_catalog.pg_class c \
                 join pg_catalog.pg_namespace n on n.oid = c.relnamespace \
                 where n.nspname = $1 and c.relname = 'trellis_instance' \
                   and c.relkind in ('r', 'p'))",
                &[&schema],
            )
            .await?
            .get(0);
        if !holds_catalog {
            return Ok(());
        }
    }
    Err(CatalogError::TargetInCatalogSchema {
        target: target.to_string(),
        schema: schema.to_string(),
        own,
    })
}

/// What [`revalidate`] read from the live schema, for the resume to build
/// from.
#[derive(Debug, Clone)]
pub(crate) struct Revalidated {
    /// The source's columns and their value types, as define reads them
    /// ([`live_source_columns`]).
    pub source_columns: HashMap<String, ValueType>,
    /// A `JOIN` definition's two tables and their live columns.
    pub join: Option<JoinSides>,
}

/// Re-runs define-time validation for `definition` against the live schema:
/// the definition's own rules ([`validate`]) over the
/// source's live columns, [`validate_live_schema`], and
/// the checks on the target define makes once it exists. A resume calls it
/// before it changes anything, and refuses while it fails; a definition the
/// schema has drifted from is then refused for what define would refuse
/// it for, naming the column and what to change.
///
/// One check is a resume's own: a 1-1 target is keyed by the source key
/// define found, so a source whose key has since been redefined fails with
/// [`CatalogError::SourceKeyChanged`], whose only repair is to drop and
/// define again.
pub(crate) async fn revalidate(
    txn: &tokio_postgres::Transaction<'_>,
    schema: &str,
    definition: &Definition,
) -> Result<Revalidated, CatalogError> {
    let source = definition.source_table.as_str();
    reject_target_in_catalog_schema(txn, schema, &definition.target_table).await?;
    if let Some(joined) = join_table_of(txn, definition.id).await? {
        return revalidate_join(txn, schema, definition, joined).await;
    }
    let source_columns = live_source_columns(txn, source).await?;
    if source_columns.is_empty() {
        return Err(CatalogError::SourceTableNotFound(source.to_string()));
    }
    validate_with_source(&definition.def, &source_columns, source)?;
    let source_key = validate_live_schema(txn, schema, &definition.def, source).await?;
    if let KeySpace::OneToOne = definition.def.key_space {
        let target_key: Vec<String> = ddl::identity_key_columns(txn, &definition.target_table)
            .await?
            .into_iter()
            .map(|c| c.name)
            .collect();
        let source_key: Vec<String> = source_key.into_iter().map(|c| c.name).collect();
        if !target_key.is_empty() && target_key != source_key {
            return Err(CatalogError::SourceKeyChanged {
                source_table: source.to_string(),
                target_key,
                source_key,
            });
        }
    }
    // The aggregates define refuses by family (a
    // `SUM`/`AVG` over a numeric that lost its declared scale, or over a
    // column re-typed to a float), which define makes once the table exists.
    check_aggregate_admitted(txn, &definition.def, source).await?;
    reject_row_security(
        txn,
        schema,
        &[(
            definition.target_table.clone(),
            super::row_security::Readers::Target,
        )],
    )
    .await?;
    Ok(Revalidated {
        source_columns,
        join: None,
    })
}

/// [`revalidate`] for a `JOIN` definition: both tables, and every
/// check define makes on each, so a failure on either side refuses the
/// resume, naming that side's table.
async fn revalidate_join(
    txn: &tokio_postgres::Transaction<'_>,
    schema: &str,
    definition: &Definition,
    joined: String,
) -> Result<Revalidated, CatalogError> {
    let sides = JoinSides::read(txn, definition.source_table.clone(), joined).await?;
    for (table, columns) in [
        (&sides.from, &sides.from_columns),
        (&sides.joined, &sides.joined_columns),
    ] {
        if columns.is_empty() {
            return Err(CatalogError::SourceTableNotFound(table.clone()));
        }
    }
    super::validate::validate_join(&definition.def, &sides.from_columns, &sides.joined_columns)?;
    validate_live_join_schema(txn, schema, &definition.def, &sides).await?;
    reject_row_security(
        txn,
        schema,
        &[(
            definition.target_table.clone(),
            super::row_security::Readers::Target,
        )],
    )
    .await?;
    Ok(Revalidated {
        source_columns: sides
            .from_columns
            .iter()
            .map(|(name, info)| (name.clone(), info.value_type))
            .collect(),
        join: Some(sides),
    })
}

/// Drops group-basis definition `id`'s hidden partials table from `schema`,
/// the catalog schema, and creates it again, empty, at the shape `shape`
/// gives it: a resume's, since its partials are fully derived. The
/// shape is what define would give it now, so a `GROUP BY` column re-typed
/// while the definition was paused resumes over correctly typed partials.
pub(crate) async fn recreate_group_table(
    txn: &tokio_postgres::Transaction<'_>,
    schema: &str,
    id: i64,
    shape: &crate::staging::group_basis::GroupShape,
) -> Result<(), CatalogError> {
    txn.batch_execute(&format!(
        "{};\n{}",
        crate::staging::group_basis::drop_hidden_table_sql(schema, id),
        crate::staging::group_basis::hidden_table_ddl(schema, id, shape)
    ))
    .await?;
    Ok(())
}

/// Drops `JOIN` definition `id`'s hidden tables from `schema`, the catalog
/// schema, and creates them again, empty, at the shape `sides` gives them:
/// a resume's, since they're fully derived.
pub(crate) async fn recreate_hidden_tables(
    txn: &tokio_postgres::Transaction<'_>,
    schema: &str,
    id: i64,
    def: &TransformDef,
    sides: &JoinSides,
) -> Result<(), CatalogError> {
    txn.batch_execute(&format!(
        "{};\n{}",
        super::join::drop_hidden_tables_sql(schema, id),
        super::join::hidden_tables_ddl(schema, id, def, &sides.from_columns, &sides.joined_columns)
    ))
    .await?;
    Ok(())
}

/// The qualified joined table of definition `id`, a `JOIN`; `None`
/// for any other definition, or an id with no row.
pub(crate) async fn join_table_of(
    client: &impl GenericClient,
    id: i64,
) -> Result<Option<String>, CatalogError> {
    Ok(client
        .query_opt(
            "select join_table from transform_definitions where id = $1",
            &[&id],
        )
        .await?
        .and_then(|row| row.get(0)))
}

/// The qualified joined table of `JOIN` definition `id` and its column
/// types as define or the last resume recorded them; `None` for any
/// other definition, or an id with no row.
pub(crate) async fn join_side_of(
    client: &impl GenericClient,
    id: i64,
) -> Result<Option<(String, HashMap<String, ValueType>)>, CatalogError> {
    let rows = client
        .query(
            "select d.join_table, e.key, e.value from transform_definitions d \
             left join lateral jsonb_each_text(d.join_columns) e on true \
             where d.id = $1 and d.join_table is not null",
            &[&id],
        )
        .await?;
    let Some(first) = rows.first() else {
        return Ok(None);
    };
    let table: String = first.get(0);
    let mut columns = HashMap::with_capacity(rows.len());
    for row in &rows {
        let (Some(key), Some(value)) = (
            row.get::<_, Option<String>>(1),
            row.get::<_, Option<String>>(2),
        ) else {
            continue;
        };
        let Some(value_type) = decode_value_type(&value) else {
            return Err(CatalogError::UnknownValueType {
                column: key,
                text: value,
            });
        };
        columns.insert(key, value_type);
    }
    Ok(Some((table, columns)))
}

/// Persists `columns` as `JOIN` definition `id`'s joined-table column types,
/// as a resume reads them, beside [`record_source_columns`].
pub(crate) async fn record_join_columns(
    client: &impl GenericClient,
    id: i64,
    columns: &HashMap<String, ValueType>,
) -> Result<(), CatalogError> {
    let (keys, vals) = encode_type_map(columns);
    client
        .execute(
            "update transform_definitions \
             set join_columns = jsonb_object($2::text[], $3::text[]) where id = $1",
            &[&id, &keys, &vals],
        )
        .await?;
    Ok(())
}

/// Refuses a whole-transform resume of a 1-1 definition whose ledger table
/// doesn't exist. Define creates one for every 1-1 definition
/// ([`ddl::target_table_ddl`]), and the build's `start` reads it, so a resume that passed would leave
/// every staging-worker pass failing on it until the definition was paused
/// or dropped. Refusing before the resume changes anything leaves the
/// definition paused with its target as it was
/// ([`CatalogError::LedgerTableMissing`]).
///
/// Only a whole-transform resume calls it, through [`check_owned_tables`]: a
/// column resume builds nothing and never reads the ledger.
pub(crate) async fn check_ledger_table(
    txn: &tokio_postgres::Transaction<'_>,
    definition: &Definition,
) -> Result<(), CatalogError> {
    let (target_schema, target_bare) = definition
        .target_table
        .split_once('.')
        .unwrap_or(("", definition.target_table.as_str()));
    let ledger = format!(
        "{target_schema}.{}",
        super::ledger::ledger_table_name(target_bare)
    );
    let present: bool = txn
        .query_one(
            "select pg_catalog.to_regclass($1) is not null",
            &[&ddl::regclass_arg(&ledger)],
        )
        .await?
        .get(0);
    if present {
        return Ok(());
    }
    Err(CatalogError::LedgerTableMissing {
        target: definition.target_table.clone(),
        ledger_table: ledger,
    })
}

/// Refuses a whole-transform resume while a source index the definition
/// needs is missing (`super::source_index`), as define would, unless
/// it was defined `WITHOUT SOURCE INDEX`.
async fn check_source_index(
    txn: &tokio_postgres::Transaction<'_>,
    definition: &Definition,
    revalidated: &Revalidated,
) -> Result<(), CatalogError> {
    if definition.def.without_source_index {
        return Ok(());
    }
    let missing = super::source_index::unmet_for(
        txn,
        &definition.def,
        &revalidated.source_columns,
        &definition.source_table,
        revalidated.join.as_ref().map(|sides| sides.joined.as_str()),
    )
    .await?;
    if missing.is_empty() {
        return Ok(());
    }
    Err(CatalogError::SourceIndexMissing {
        target: definition.def.target.clone(),
        missing,
    })
}

/// Refuses a whole-transform resume of a definition whose target table
/// doesn't exist ([`CatalogError::TargetTableMissing`]). Define creates the
/// target, and the rebuild writes into it, so a resume that passed would
/// leave the definition failing its build. Refusing before the resume changes
/// anything leaves the definition paused. Trellis doesn't re-create it.
pub(crate) async fn check_target_table(
    txn: &tokio_postgres::Transaction<'_>,
    definition: &Definition,
) -> Result<(), CatalogError> {
    let present: bool = txn
        .query_one(
            "select pg_catalog.to_regclass($1) is not null",
            &[&ddl::regclass_arg(&definition.target_table)],
        )
        .await?
        .get(0);
    if present {
        return Ok(());
    }
    Err(CatalogError::TargetTableMissing {
        target: definition.target_table.clone(),
    })
}

/// A whole-transform resume's checks that the target exists
/// ([`check_target_table`]), that the source indexes the definition needs
/// exist ([`check_source_index`]), and that the tables Trellis keeps beside
/// it are what define would create now: a 1-1 target's ledger exists
/// ([`check_ledger_table`]). An aggregate or a `JOIN` keeps only hidden
/// tables that are fully derived: the resume drops and recreates them
/// ([`recreate_group_table`]), so there's nothing to check.
pub(crate) async fn check_owned_tables(
    txn: &tokio_postgres::Transaction<'_>,
    definition: &Definition,
    revalidated: &Revalidated,
) -> Result<(), CatalogError> {
    check_target_table(txn, definition).await?;
    check_source_index(txn, definition, revalidated).await?;
    if !matches!(definition.def.key_space, KeySpace::OneToOne) {
        return Ok(());
    }
    check_ledger_table(txn, definition).await
}

/// Persists `source_columns` as definition `id`'s source column types, the
/// ones its builds and applies cast through. A resume records the live ones
/// ([`revalidate`]), so a source column widened since define is cast to its
/// new type.
pub(crate) async fn record_source_columns(
    client: &impl GenericClient,
    id: i64,
    source_columns: &HashMap<String, ValueType>,
) -> Result<(), CatalogError> {
    let (keys, vals) = encode_type_map(source_columns);
    client
        .execute(
            "update transform_definitions \
             set source_columns = jsonb_object($2::text[], $3::text[]) where id = $1",
            &[&id, &keys, &vals],
        )
        .await?;
    Ok(())
}

/// The live columns of `qualified` (an unquoted `schema.table`) the
/// validator can type, with their [`ValueType`]s, as define reads them.
/// Empty for a table that doesn't exist.
///
/// Classified by each column's `atttypid` through
/// [`super::pg_type::value_type_for_oid`], which also places a user-defined
/// enum. A column whose type the registry can't place at all
/// (an array, a range, a composite, a domain, `citext`) is left out, so the
/// validator reports a reference to it as an unknown column: everything
/// downstream needs its real Postgres type, and `Unrecognized` isn't one.
/// Promoting those families is `docs/type-support.md`'s deferred work
/// (#122).
pub(crate) async fn live_source_columns(
    client: &impl GenericClient,
    qualified: &str,
) -> Result<HashMap<String, ValueType>, CatalogError> {
    let rows = client
        .query(
            "select a.attname::text, a.atttypid \
             from pg_attribute a \
             where a.attrelid = pg_catalog.to_regclass($1) \
               and a.attnum > 0 \
               and not a.attisdropped",
            &[&ddl::regclass_arg(qualified)],
        )
        .await?;
    let mut columns = HashMap::with_capacity(rows.len());
    for row in rows {
        let column_name: String = row.get(0);
        let type_oid: u32 = row.get(1);
        match super::pg_type::value_type_for_oid(client, type_oid).await? {
            ValueType::Other(super::PgType::Unrecognized) => {}
            value_type => {
                columns.insert(column_name, value_type);
            }
        }
    }
    Ok(columns)
}

/// Refuses a definition if any column the engine matches as one
/// of its keys has a nondeterministic collation
/// ([`assert_deterministic_key_collation`]). Those keys are:
///
/// - the source's row-identity key (`source_key`, which
///   [`ddl::source_primary_key_in_txn`] just resolved): apply keys every
///   staged change by it, and a 1-1 target mirrors it as its own key;
/// - an aggregate's `GROUP BY` keys. The target's group key is their exact
///   text.
///
/// The target's own key columns need no check: Trellis creates the target
/// with no `COLLATE` clause, so they take the database default, which is
/// always deterministic.
async fn assert_deterministic_definition_keys(
    txn: &tokio_postgres::Transaction<'_>,
    def: &TransformDef,
    qualified_source: &str,
    source_key: &[ddl::PrimaryKeyColumn],
) -> Result<(), CatalogError> {
    for column in source_key {
        assert_deterministic_key_collation_in_txn(
            txn,
            KeyColumnRole::SourceKey,
            qualified_source,
            &column.name,
        )
        .await?;
    }
    let KeySpace::Aggregate { group_by } = &def.key_space else {
        return Ok(());
    };
    for column in group_by {
        assert_deterministic_key_collation_in_txn(
            txn,
            KeyColumnRole::GroupBy,
            qualified_source,
            column,
        )
        .await?;
    }
    Ok(())
}

/// Refuses a definition whose field `STRPOS`/
/// `REGEXP_COUNT` reads a column with a nondeterministic collation
/// ([`super::validate::collation_refusing_function_reads`]). Postgres raises
/// `0A000` for both under such a collation, while the engine computes them
/// from the exact text, so the two would disagree. `only_fields`, if given,
/// limits the check to those fields (`ALTER TRANSFORM`'s added and altered
/// ones). A column that doesn't exist passes, as in
/// [`assert_deterministic_key_collation_in_txn`]: `validate` owns that error.
async fn assert_collation_safe_text_functions(
    txn: &tokio_postgres::Transaction<'_>,
    def: &TransformDef,
    qualified_source: &str,
    only_fields: Option<&[FieldDef]>,
) -> Result<(), CatalogError> {
    for read in super::validate::collation_refusing_function_reads(def) {
        if let Some(only) = only_fields
            && !only.iter().any(|field| field.name == read.field)
        {
            continue;
        }
        let (table, column) = (qualified_source.to_string(), &read.column);
        let Some(resolved) = column_in_txn(txn, &table, column).await? else {
            continue;
        };
        if !resolved.collation_deterministic {
            return Err(
                ValidationError::NondeterministicCollationTextFunction(Box::new(
                    NondeterministicCollationTextFunction {
                        field: read.field,
                        function: read.function,
                        table,
                        column: column.clone(),
                        collation: resolved.collation.unwrap_or_default(),
                    },
                ))
                .into(),
            );
        }
    }
    Ok(())
}

/// Whether `qualified` (an unquoted `schema.table`) has a primary key, the
/// part of [`change_keyed`]'s rule a partitioned root is held to as well.
async fn has_primary_key(
    client: &impl GenericClient,
    qualified: &str,
) -> Result<bool, tokio_postgres::Error> {
    Ok(client
        .query_one(
            "select exists (select 1 from pg_catalog.pg_index i \
                 where i.indrelid = pg_catalog.to_regclass($1) and i.indisprimary)",
            &[&ddl::regclass_arg(qualified)],
        )
        .await?
        .get(0))
}

/// Whether trigger capture can key `schema.table`'s changes:
/// it is a plain table (`relkind = 'r'`) with a primary key. Every ring row a
/// capture trigger writes is keyed by the primary key, so a table without
/// one can't be captured, whatever unique index it has. That also rules out
/// a key mismatch between a unique index and the primary key.
///
/// A table in a partition or inheritance hierarchy (a partitioned table, a
/// partition, an inheritance parent or an inheritance child) is refused as
/// well, by the check the staging worker's capture pass and `self_check`'s
/// capture audit share ([`super::hierarchy::hierarchy`], whose module doc
/// says why). A view, foreign table or anything else that isn't a plain
/// table is refused too. `false` for a table that doesn't exist.
pub(crate) async fn change_keyed(
    client: &impl GenericClient,
    schema: &str,
    table: &str,
) -> Result<bool, tokio_postgres::Error> {
    let regclass = format!("{}.{}", quote_ident(schema), quote_ident(table));
    let keyed: Option<bool> = client
        .query_opt(
            "select c.relkind = 'r' \
                 and exists ( \
                     select 1 from pg_catalog.pg_index i \
                     where i.indrelid = c.oid and i.indisprimary) \
             from pg_catalog.pg_class c where c.oid = pg_catalog.to_regclass($1)",
            &[&regclass],
        )
        .await?
        .map(|row| row.get(0));
    if keyed != Some(true) {
        return Ok(false);
    }
    Ok(
        super::hierarchy::hierarchy(client, &format!("{schema}.{table}"))
            .await?
            .is_empty(),
    )
}

/// Rejects a definition whose source this instance would capture
/// by triggers when the triggers can't key its changes ([`change_keyed`]: a
/// plain table with a primary key) —
/// [`CatalogError::SourceNotChangeKeyed`].
///
/// This instance's own targets are exempt. Every write to one goes through
/// the target-mutation seam (`staging::target_mutations`), which
/// stages each changed key for the target's readers in the writing
/// transaction, keyed by the same code that wrote the row, so an aggregate
/// target's missing primary key never matters to its own instance.
///
/// Another instance's target gets no such exemption, because nothing in this
/// catalog names it: that instance's seam stages downstream rows only into
/// its own ring, so this instance's only view of the table is its own
/// capture triggers. For a 1-1 target that works, since it mirrors its
/// source's primary key. An aggregate target has none.
///
/// The rule is about the table, not about who owns it, so a plain source
/// table without a primary key is rejected too, as is a partitioned one.
async fn reject_unkeyed_source(
    client: &impl GenericClient,
    qualified_source: &str,
    def: &TransformDef,
) -> Result<(), CatalogError> {
    if is_definition_target(client, qualified_source).await? {
        return Ok(());
    }
    let Some((schema, table)) = qualified_source.split_once('.') else {
        return Err(CatalogError::SourceTableNotFound(
            qualified_source.to_string(),
        ));
    };
    if let Some(tree) = super::partition_tree::read_partition_tree(client, qualified_source).await?
    {
        // The plain-table rule's primary key holds for a partitioned root
        // too: `ddl::source_primary_key` also takes a qualifying unique
        // index, which [`change_keyed`] doesn't.
        if !has_primary_key(client, qualified_source).await? {
            return Err(CatalogError::SourceNotChangeKeyed {
                source_table: qualified_source.to_string(),
            });
        }
        let refusal = match &def.key_space {
            // ADR-0019: a side's partials are a single-table aggregate keyed
            // by its `GROUP BY` columns and join key, so ADR-0017's group-key
            // rule applies to that key.
            KeySpace::Join(_) => match super::join::side_of_table(def, qualified_source) {
                Some(side) => tree.refusal(
                    super::partition_tree::server_version_num(client).await?,
                    &super::join::side_key_columns(def, side),
                ),
                None => None,
            },
            KeySpace::OneToOne => Some(super::partition_tree::PartitionRefusal::OneToOneFrom {
                table: qualified_source.to_string(),
            }),
            KeySpace::Aggregate { group_by } => {
                let columns: Vec<String> = group_by.to_vec();
                tree.refusal(
                    super::partition_tree::server_version_num(client).await?,
                    &columns,
                )
            }
        };
        if let Some(refusal) = refusal {
            return Err(CatalogError::PartitionedSource(refusal));
        }
        return Ok(());
    }
    if change_keyed(client, schema, table).await? {
        return Ok(());
    }
    Err(CatalogError::SourceNotChangeKeyed {
        source_table: qualified_source.to_string(),
    })
}

/// Rejects the first of `tables` (unquoted `schema.table`
/// identities) whose row-level security applies to a role that reads it
/// ([`super::row_security::applying`]): the ring's owner, and the session's
/// role for a table registration itself reads ([`super::row_security::Readers`];
/// not otherwise, since the session may belong to a process that never reads
/// the table), or only the session's role for a table another definition
/// targets, which no capture function reads, and for the target it creates
/// — [`CatalogError::RowSecurityApplies`]. A table that doesn't
/// exist passes: the checks that need it report that.
async fn reject_row_security(
    client: &impl GenericClient,
    schema: &str,
    tables: &[(String, super::row_security::Readers)],
) -> Result<(), CatalogError> {
    for (table, readers) in tables {
        if let Some(rls) = super::row_security::applying(client, schema, table, *readers).await? {
            return Err(CatalogError::RowSecurityApplies(rls));
        }
    }
    Ok(())
}

/// Rejects the first of `tables` (unquoted `schema.table`
/// identities) that a logical-replication subscription replicates into
/// ([`super::subscription::subscribed`]) — [`CatalogError::Subscribed`]. A
/// table that doesn't exist passes: the checks that need it report that.
async fn reject_subscribed(
    client: &impl GenericClient,
    tables: &[&str],
) -> Result<(), CatalogError> {
    for table in tables {
        if let Some(sub) = super::subscription::subscribed(client, table).await? {
            return Err(CatalogError::Subscribed(sub));
        }
    }
    Ok(())
}

/// Resolves `table_name` to its [`SchemaNode`], creating one if this is the
/// first time Trellis has seen the table and setting its `kind` role flag
/// (`is_source`/`is_target`) to `true`. Idempotent, and additive across
/// roles: resolving the same table under both [`NodeKind::Source`] and
/// [`NodeKind::Target`] over separate calls (chained/multi-hop transforms —
/// see [`super::model::NodeKind`]'s doc comment) merges into one node with
/// both flags set, rather than erroring.
///
/// **`table_name` must already be fully-qualified (ADR-0011)** —
/// `public.posts` and `archive.posts` resolve to distinct nodes; this
/// function never resolves a bare name itself (see
/// [`resolve_graph_identity_in_txn`] for the one place that does, ahead of
/// every caller here).
///
/// Runs in its own transaction; [`create_definition`] instead calls
/// [`resolve_node_in_txn`] directly so both of a definition's node
/// resolutions land in the same transaction as the definition write.
#[cfg(any(test, feature = "internals"))]
pub async fn resolve_node(
    pool: &Pool,
    table_name: &str,
    kind: NodeKind,
) -> Result<SchemaNode, CatalogError> {
    let mut client = pool.get().await?;
    let txn = client.transaction().await?;
    let node = resolve_node_in_txn(&txn, table_name, kind).await?;
    txn.commit().await?;
    Ok(node)
}

/// The transactional core of [`resolve_node`] — see its doc comment,
/// including the fully-qualified-`table_name` requirement. Upserts
/// `table_name`, OR-ing `kind`'s role flag into whatever the row already
/// has (or defaulting the other flag `false` if the row is new).
async fn resolve_node_in_txn(
    txn: &tokio_postgres::Transaction<'_>,
    table_name: &str,
    kind: NodeKind,
) -> Result<SchemaNode, CatalogError> {
    let (is_source, is_target) = match kind {
        NodeKind::Source => (true, false),
        NodeKind::Target => (false, true),
    };

    let row = txn
        .query_one(
            "insert into schema_nodes (table_name, is_source, is_target)
             values ($1, $2, $3)
             on conflict (table_name) do update
                set is_source = schema_nodes.is_source or excluded.is_source,
                    is_target = schema_nodes.is_target or excluded.is_target
             returning id, is_source, is_target",
            &[&table_name, &is_source, &is_target],
        )
        .await?;

    Ok(SchemaNode {
        id: row.get(0),
        table_name: table_name.to_string(),
        is_source: row.get(1),
        is_target: row.get(2),
    })
}

/// Pool-level wrapper over [`persist_edge_in_txn`], as [`resolve_node`]
/// wraps [`resolve_node_in_txn`]. Exists so the
/// `on conflict do nothing` dedup path on `schema_edges`'s
/// `(from_node_id, to_node_id, kind)` uniqueness constraint has direct test
/// coverage — [`create_definition`] can never hit it itself, since
/// `transform_definitions.target_table` is unique and so no two definitions
/// can ever resolve to the same `(from_node_id, to_node_id)` pair.
#[cfg(any(test, feature = "internals"))]
pub async fn persist_edge(
    pool: &Pool,
    from_node_id: i64,
    to_node_id: i64,
    kind: EdgeKind,
) -> Result<(), CatalogError> {
    let mut client = pool.get().await?;
    let txn = client.transaction().await?;
    persist_edge_in_txn(&txn, from_node_id, to_node_id, kind).await?;
    txn.commit().await?;
    Ok(())
}

/// Records that `to_node_id` depends on `from_node_id` via `kind` — the
/// transactional core [`create_definition`] calls for a transform's `FROM`
/// edge. `on conflict do nothing` on `schema_edges`'s
/// `(from_node_id, to_node_id, kind)` uniqueness constraint makes
/// re-declaring the same transform's edge idempotent (definitions are
/// immutable, but nothing stops the same source/target pair from being
/// resolved through this path more than once as the graph grows).
async fn persist_edge_in_txn(
    txn: &tokio_postgres::Transaction<'_>,
    from_node_id: i64,
    to_node_id: i64,
    kind: EdgeKind,
) -> Result<(), CatalogError> {
    txn.execute(
        "insert into schema_edges (from_node_id, to_node_id, kind)
         values ($1, $2, $3)
         on conflict (from_node_id, to_node_id, kind) do nothing",
        &[&from_node_id, &to_node_id, &kind.as_str()],
    )
    .await?;
    Ok(())
}

/// Rejects a definition whose `Source` edge — `source_table -> target_table`
/// — would close a cycle in the table-level dependency graph: this holds
/// iff `target_table` can already reach `source_table` through some path of
/// existing [`super::model::SchemaEdge`]s (of any kind — see
/// [`create_definition`]'s call site for why this stays kind-generic).
/// Walks the transaction's current `schema_edges`/`schema_nodes` state as a
/// small in-memory adjacency map, hand-rolled DFS, matching
/// [`super::validate::detect_cycle`]'s column-level convention rather than
/// pulling in a graph crate for a problem this small.
///
/// **`source_table`/`target_table` must already be fully-qualified**
/// (ADR-0011) — `schema_nodes.table_name` is, so comparing a bare name
/// against it would simply never match anything, silently defeating cycle
/// detection rather than erroring. See [`resolve_graph_identity_in_txn`] for
/// how every caller here gets a qualified value before calling this.
async fn reject_if_table_cycle(
    txn: &tokio_postgres::Transaction<'_>,
    source_table: &str,
    target_table: &str,
) -> Result<(), CatalogError> {
    let rows = txn
        .query(
            "select from_node.table_name, to_node.table_name
             from schema_edges se
             join schema_nodes from_node on from_node.id = se.from_node_id
             join schema_nodes to_node on to_node.id = se.to_node_id",
            &[],
        )
        .await?;

    let mut adjacency: HashMap<String, Vec<String>> = HashMap::new();
    for row in rows {
        let from: String = row.get(0);
        let to: String = row.get(1);
        adjacency.entry(from).or_default().push(to);
    }

    if let Some(mut path) = find_table_path(&adjacency, target_table, source_table) {
        // `path` is target_table -> ... -> source_table; appending
        // target_table closes the loop the new source_table -> target_table
        // edge would create, for a message naming the whole cycle.
        path.push(target_table.to_string());
        return Err(ValidationError::TableCycle { cycle: path }.into());
    }
    Ok(())
}

/// DFS from `from` to `to` over `adjacency`, returning the path (inclusive
/// of both ends) if one exists.
fn find_table_path(
    adjacency: &HashMap<String, Vec<String>>,
    from: &str,
    to: &str,
) -> Option<Vec<String>> {
    let mut visited: HashSet<String> = HashSet::new();
    let mut path: Vec<String> = Vec::new();
    if find_table_path_from(adjacency, from, to, &mut visited, &mut path) {
        Some(path)
    } else {
        None
    }
}

fn find_table_path_from(
    adjacency: &HashMap<String, Vec<String>>,
    node: &str,
    to: &str,
    visited: &mut HashSet<String>,
    path: &mut Vec<String>,
) -> bool {
    path.push(node.to_string());
    if node == to {
        return true;
    }
    visited.insert(node.to_string());
    if let Some(neighbors) = adjacency.get(node) {
        for neighbor in neighbors {
            if !visited.contains(neighbor)
                && find_table_path_from(adjacency, neighbor, to, visited, path)
            {
                return true;
            }
        }
    }
    path.pop();
    false
}

/// The [`SchemaNode`] already resolved for `table_name`, or `None` if
/// nothing has ever referenced it as a source or a target. `table_name`
/// must already be fully-qualified (ADR-0011).
#[cfg(any(test, feature = "internals"))]
pub async fn node_for_table(
    pool: &Pool,
    table_name: &str,
) -> Result<Option<SchemaNode>, CatalogError> {
    let client = pool.get().await?;
    let row = client
        .query_opt(
            "select id, is_source, is_target from schema_nodes where table_name = $1",
            &[&table_name],
        )
        .await?;
    let Some(row) = row else { return Ok(None) };

    Ok(Some(SchemaNode {
        id: row.get(0),
        table_name: table_name.to_string(),
        is_source: row.get(1),
        is_target: row.get(2),
    }))
}

/// The [`SchemaEdge`]s of `kind` directed away from `node_table` — a
/// generalized, `transform_definitions`-agnostic sibling of
/// [`dependents_of`] that returns raw edges rather than joining onto any
/// definition table, so it works for any [`EdgeKind`] without needing a
/// kind-specific query. `node_table` must already be fully-qualified
/// (ADR-0011) — matched exactly against `schema_nodes.table_name`.
#[cfg(any(test, feature = "internals"))]
pub async fn edges_from(
    pool: &Pool,
    node_table: &str,
    kind: EdgeKind,
) -> Result<Vec<SchemaEdge>, CatalogError> {
    let client = pool.get().await?;
    let rows = client
        .query(
            "select se.id, se.from_node_id, se.to_node_id
             from schema_edges se
             join schema_nodes from_node on from_node.id = se.from_node_id
             where from_node.table_name = $1 and se.kind = $2
             order by se.id",
            &[&node_table, &kind.as_str()],
        )
        .await?;

    Ok(rows
        .into_iter()
        .map(|row| SchemaEdge {
            id: row.get(0),
            from_node_id: row.get(1),
            to_node_id: row.get(2),
            kind,
        })
        .collect())
}

/// The stable token a [`ValueType`] persists as in
/// `transform_definitions.source_columns` — the write side of
/// [`decode_value_type`], which every read site below (`dependents_of`,
/// `definition_by_id`, `definition_by_target`) shares rather than
/// re-implementing its own copy of this match, the way three independent
/// copies used to.
fn encode_value_type(value_type: &ValueType) -> &'static str {
    match value_type {
        ValueType::Numeric => "numeric",
        // An exact integer persists under its own Postgres
        // spelling (`smallint`/`integer`/`bigint`), which is distinct from
        // every other token here *and* from every `PgType::name`, so all
        // three namespaces still share one column unambiguously. The
        // `every_value_type_round_trips_through_the_persisted_token` test
        // pins that.
        ValueType::Integer(width) => width.pg_name(),
        // Same shape — `real`/`double precision`, each distinct
        // from every other token in all three namespaces.
        ValueType::Float(width) => width.pg_name(),
        ValueType::Text => "text",
        ValueType::Boolean => "boolean",
        ValueType::Uuid => "uuid",
        // `PgType::name` doubles as this persisted token: every
        // name is already a distinct, stable string disjoint from the four
        // above (see that method's doc comment).
        ValueType::Other(pg_type) => pg_type.name(),
    }
}

/// The inverse of [`encode_value_type`]. `None` for a token this build
/// doesn't recognize — every call site turns that into
/// [`CatalogError::UnknownValueType`], the same forward-compat guard the
/// three duplicated inline matches enforced before this was pulled out.
fn decode_value_type(text: &str) -> Option<ValueType> {
    Some(match text {
        "numeric" => ValueType::Numeric,
        "text" => ValueType::Text,
        "boolean" => ValueType::Boolean,
        "uuid" => ValueType::Uuid,
        other => match (
            IntWidth::from_pg_name(other),
            FloatWidth::from_pg_name(other),
        ) {
            (Some(width), _) => ValueType::Integer(width),
            (_, Some(width)) => ValueType::Float(width),
            _ => ValueType::Other(PgType::from_name(other)?),
        },
    })
}

/// Splits `source_columns` into the parallel key/value text arrays
/// `jsonb_object`'s two-array form wants (see `create_definition`'s insert
/// and [`transforms_for_source`]'s matching read side).
fn encode_type_map(source_columns: &HashMap<String, ValueType>) -> (Vec<&str>, Vec<&'static str>) {
    let mut keys = Vec::with_capacity(source_columns.len());
    let mut vals = Vec::with_capacity(source_columns.len());
    for (name, value_type) in source_columns {
        keys.push(name.as_str());
        vals.push(encode_value_type(value_type));
    }
    (keys, vals)
}

/// One definition row's non-`source_columns` fields, accumulated while
/// [`transforms_for_source`] walks its single, lateral-joined query — see
/// that function's doc comment.
struct PendingDefinition {
    source_version: i64,
    text: String,
    grammar_version: i32,
    status: TransformStatus,
    source_columns: HashMap<String, ValueType>,
    source_table: String,
    target_table: String,
}

/// The transform definitions that depend on `node_table` via a `kind` edge
/// in the persisted dependency graph — e.g. `EdgeKind::Source`
/// answers "what reads from `node_table` as its `FROM`". Walking
/// `schema_edges` (rather than matching on
/// `transform_definitions.source_table` string equality) is what makes this
/// a real graph lookup: multi-hop chains resolve by calling this again with
/// a dependent's target table, not by any special-casing here.
///
/// One query, not one-per-definition-row: a `left join lateral
/// jsonb_each_text(...)` unnests every dependent definition's persisted
/// `source_columns` map inline, so this is still "decode JSON via SQL, no
/// serde_json dependency" — matching `staging::apply::decode_images`'s
/// convention — just decoded for every row in one round trip instead of one
/// per definition. The `left join` (rather than an inner join/`cross join
/// lateral`) matters: a definition whose `source_columns` is `{}` must still
/// come back with zero entries, not disappear from the result entirely.
///
/// **Applying definitions only** ([`super::model::APPLYING_SQL`]:
/// `live` or `catching_up`, or `backfilling` under a Re-derive build, which
/// applies from its start; ADR-0002): a `waiting_to_backfill`
/// or `quarantined` definition's target may not reflect every pre-existing
/// source row, so a live CDC delta folded into it now — via
/// [`transforms_for_source`], the apply path's read of this function — could
/// permanently corrupt a value an incremental accumulator (e.g. `AVG`)
/// computes against a baseline. Excluding those rows here means the apply
/// path simply never attempts them. What a skipped delta changed is
/// recovered from state when the definition is built or resumed: its build
/// re-derives every key the source has and sweeps the ones it no longer
/// backs.
///
/// `node_table` ($1) must already be fully-qualified (ADR-0011)
/// — matched exactly against `schema_nodes.table_name`, which is always
/// qualified too, so `public.posts` and `archive.posts` resolve to
/// disjoint dependent sets rather than colliding. The join from `to_node`
/// onto `transform_definitions` is a plain equality on `target_table`
/// as well, since both sides hold the exact same qualified string
/// (both ultimately derived from the same `qualified_target`, in the same
/// transaction, by `create_definition_inner`).
#[cfg(any(test, feature = "internals"))]
pub async fn dependents_of(
    pool: &Pool,
    node_table: &str,
    kind: EdgeKind,
) -> Result<Vec<Definition>, CatalogError> {
    dependents_via(pool, node_table, &[kind]).await
}

/// [`dependents_of`] over an edge of any of `kinds`.
async fn dependents_via(
    pool: &Pool,
    node_table: &str,
    kinds: &[EdgeKind],
) -> Result<Vec<Definition>, CatalogError> {
    let kinds: Vec<&str> = kinds.iter().map(|kind| kind.as_str()).collect();
    let client = pool.get().await?;
    let rows = client
        .query(
            &format!(
                "select t.id, t.source_version, t.definition_text, t.status, t.source_table, \
                        t.target_table, e.key, e.value, t.grammar_version
                 from schema_nodes from_node
                 join schema_edges se on se.from_node_id = from_node.id and se.kind = any($2)
                 join schema_nodes to_node on to_node.id = se.to_node_id
                 join transform_definitions t on t.target_table = to_node.table_name
                 left join lateral jsonb_each_text(t.source_columns) e on true
                 where from_node.table_name = $1 and {}
                 order by t.id",
                super::model::applying_sql("t")
            ),
            &[&node_table, &kinds],
        )
        .await?;

    // `order` preserves the query's `order by t.id` across the group-by
    // done in Rust below (a plain `HashMap` has no ordering of its own).
    let mut order: Vec<i64> = Vec::new();
    let mut by_id: HashMap<i64, PendingDefinition> = HashMap::new();

    for row in rows {
        let id: i64 = row.get(0);
        let key: Option<String> = row.get(6);
        let value: Option<String> = row.get(7);

        let pending = by_id.entry(id).or_insert_with(|| {
            order.push(id);
            let status_text: String = row.get(3);
            let status = TransformStatus::from_persisted(&status_text).unwrap_or_else(|| {
                panic!("transform_definitions.status held unrecognized value '{status_text}'")
            });
            PendingDefinition {
                source_version: row.get(1),
                text: row.get(2),
                grammar_version: row.get(8),
                status,
                source_columns: HashMap::new(),
                source_table: row.get(4),
                target_table: row.get(5),
            }
        });

        if let (Some(key), Some(value)) = (key, value) {
            let Some(value_type) = decode_value_type(&value) else {
                return Err(CatalogError::UnknownValueType {
                    column: key,
                    text: value,
                });
            };
            pending.source_columns.insert(key, value_type);
        }
    }

    let mut result = Vec::with_capacity(order.len());
    for id in order {
        let pending = by_id.remove(&id).expect("id was just pushed to order");
        let def = grammar::load(&pending.text, pending.grammar_version)?;
        result.push(Definition {
            id,
            source_version: pending.source_version,
            def,
            source_columns: pending.source_columns,
            status: pending.status,
            source_table: pending.source_table,
            target_table: pending.target_table,
        });
    }
    Ok(result)
}

/// The transform definitions currently subscribed to `source_table`: a thin
/// wrapper over [`dependents_of`] following the edges a definition reads a
/// table by, its `FROM` ([`EdgeKind::Source`]) and a `JOIN`'s joined table
/// ([`EdgeKind::Join`]). A self-join is refused at define, so a
/// definition is reached through at most one of them. `source_table` must
/// already be fully-qualified (ADR-0011) — see [`dependents_of`]'s
/// doc comment.
pub async fn transforms_for_source(
    pool: &Pool,
    source_table: &str,
) -> Result<Vec<Definition>, CatalogError> {
    dependents_via(pool, source_table, &[EdgeKind::Source, EdgeKind::Join]).await
}

/// Every table that needs CDC capture for at least one registered transform:
/// every distinct `source_table` and `JOIN` `join_table`
/// ([`reachable_tables_cte`]), fully qualified (ADR-0011).
///
/// A running [`crate::Client`]'s maintenance loop polls this to
/// notice a transform registered against a table it hasn't seen before, so it
/// can install that table's capture and start its build without
/// waiting for a restart.
pub async fn all_source_tables(pool: &Pool) -> Result<Vec<String>, CatalogError> {
    let client = pool.get().await?;
    let rows = client
        .query_typed(
            &format!(
                "{} select table_name from reachable",
                reachable_tables_cte("true")
            ),
            &[],
        )
        .await?;
    Ok(rows.into_iter().map(|r| r.get(0)).collect())
}

/// Forgets the capture records of every table no definition reads:
/// the trigger, baseline and partition-tree records and the queued partition
/// corrections. A definition counts as reading a table whatever its status
/// (paused and quarantined included), as its source or a `JOIN`'s joined
/// table ([`reachable_tables_cte`]); a table read
/// only as another definition's target never has a record to keep.
///
/// Run in the transaction that removes a reader, so a definition defined
/// afterwards is never judged against the record of a table it did not read:
/// its table may have been dropped and created again since, and the capture
/// pass that would have forgotten the record had not run. Deletes by table
/// name, so it covers a record kept per node as well. Uninstalling the
/// triggers stays the capture pass's job; the pass's own sweeps stay as the
/// backstop.
pub(crate) async fn forget_unread_capture_records(
    client: &impl GenericClient,
) -> Result<(), CatalogError> {
    let cte = reachable_tables_cte("true");
    for table in [
        "capture_triggers",
        "capture_baselines",
        "capture_gates",
        "partition_corrections",
        "partition_sources",
    ] {
        client
            .execute(
                &format!(
                    "{cte} delete from {table} \
                     where table_name not in (select table_name from reachable)"
                ),
                &[],
            )
            .await?;
    }
    Ok(())
}

/// A CTE, `reachable`, of every table a definition `anchor_filter` (a
/// predicate over `transform_definitions`) keeps reads: its `source_table`
/// and a `JOIN`'s `join_table`. Shared by [`all_source_tables`] and
/// [`table_has_reader`] so the two can't disagree about which tables a
/// definition reads.
fn reachable_tables_cte(anchor_filter: &str) -> String {
    format!(
        "with reachable(table_name) as (
            select distinct source_table from transform_definitions where {anchor_filter}
            union
            select distinct join_table from transform_definitions
            where join_table is not null and {anchor_filter}
         )"
    )
}

/// The tables this instance's capture triggers should cover:
/// every table [`all_source_tables`] reaches, minus the targets of this
/// instance's own definitions.
///
/// A chained definition reads another definition's target, but that target
/// never needs capturing: every write to it goes through the target-mutation
/// seam (`staging::target_mutations`), which stages each changed key for its
/// readers in the writing transaction. Capturing it as well would stage each
/// change twice, and an aggregate target has no primary key to key its
/// captured changes by.
///
/// A reader that needs image-bearing, ordered changes gets them from the seam's
/// image feed (`staging::target_mutations`, "The image feed"), so the seam is
/// the one change feed for every target this instance owns. Another instance's
/// target is just a table here, and is captured like one.
pub async fn tables_to_capture(pool: &Pool) -> Result<Vec<String>, CatalogError> {
    let tables = all_source_tables(pool).await?;
    let client = pool.get().await?;
    let own = own_targets(&**client, &tables).await?;
    Ok(tables.into_iter().filter(|t| !own.contains(t)).collect())
}

/// Which of `tables` are the target of some definition in this instance:
/// tables whose changes reach their readers only through the
/// target-mutation seam, never CDC. See [`tables_to_capture`].
async fn own_targets(
    client: &impl GenericClient,
    tables: &[String],
) -> Result<std::collections::HashSet<String>, CatalogError> {
    let rows = client
        .query_typed(
            "select r.table_name from unnest($1::text[]) as r(table_name) \
             where exists ( \
                 select 1 from transform_definitions d where d.target_table = r.table_name \
             )",
            &[(&tables, Type::TEXT_ARRAY)],
        )
        .await?;
    Ok(rows.into_iter().map(|r| r.get(0)).collect())
}

/// The current version of `source_table`, or `None` if no definition has
/// ever been created against it.
///
/// `source_table` is the qualified identity `source_table_versions` persists
/// and is matched exactly. Its one caller
/// (`staging::apply::compute`) passes the canonical identity it also keys
/// `ApplyPlan::versions` on, and Phase 3's version fence re-reads the same row
/// by the same key. The match is on the exact qualified name, not the bare
/// table-name suffix, so two schemas that each have a source table of the
/// same name stay distinct.
pub async fn source_table_version(
    pool: &Pool,
    source_table: &str,
) -> Result<Option<i64>, CatalogError> {
    let client = pool.get().await?;
    let row = client
        .query_opt(
            "select version from source_table_versions where source_table = $1",
            &[&source_table],
        )
        .await?;
    Ok(row.map(|row| row.get(0)))
}

/// Definition `id`'s persisted source column types, on `client` (an edit's
/// and a re-type's reads of what was recorded). Empty for an id with none.
pub(crate) async fn source_columns_in(
    client: &impl GenericClient,
    id: i64,
) -> Result<HashMap<String, ValueType>, CatalogError> {
    let rows = client
        .query(
            "select e.key, e.value from transform_definitions t \
             cross join lateral jsonb_each_text(t.source_columns) e where t.id = $1",
            &[&id],
        )
        .await?;
    let mut source_columns = HashMap::new();
    for row in rows {
        let key: String = row.get(0);
        let value: String = row.get(1);
        let Some(value_type) = decode_value_type(&value) else {
            return Err(CatalogError::UnknownValueType {
                column: key,
                text: value,
            });
        };
        source_columns.insert(key, value_type);
    }
    Ok(source_columns)
}

/// Reads back one definition by its catalog id, re-parsing `definition_text`
/// exactly like every other read path in this module — used by
/// `chunk_queue`'s claim loop to reconstruct the [`super::ast::TransformDef`]
/// a claimed `backfill_chunks` row's `definition_id` names, so it can render
/// that chunk's write SQL. Returns `None` for an id nothing has ever
/// inserted (or already deleted, e.g. a definition dropped mid-backfill —
/// not exposed by any API yet, but `backfill_chunks`' `on delete cascade`
/// means this can legitimately come back empty for a stale claim).
pub(crate) async fn definition_by_id(
    pool: &Pool,
    id: i64,
) -> Result<Option<Definition>, CatalogError> {
    let client = pool.get().await?;
    definition_by_id_in(&**client, id).await
}

/// [`definition_by_id`] on `client`.
pub(crate) async fn definition_by_id_in(
    client: &impl GenericClient,
    id: i64,
) -> Result<Option<Definition>, CatalogError> {
    // `left join lateral jsonb_each_text(...)` — same "decode JSON via SQL, no
    // serde_json dependency" convention `dependents_of` uses, just for one
    // row instead of a batch.
    let rows = client
        .query(
            "select t.source_version, t.definition_text, t.status, t.source_table, \
                    t.target_table, e.key, e.value, t.grammar_version \
             from transform_definitions t \
             left join lateral jsonb_each_text(t.source_columns) e on true \
             where t.id = $1",
            &[&id],
        )
        .await?;
    if rows.is_empty() {
        return Ok(None);
    }

    let source_version: i64 = rows[0].get(0);
    let text: String = rows[0].get(1);
    let status_text: String = rows[0].get(2);
    let source_table: String = rows[0].get(3);
    let target_table: String = rows[0].get(4);
    let status = TransformStatus::from_persisted(&status_text).unwrap_or_else(|| {
        panic!("transform_definitions.status held unrecognized value '{status_text}'")
    });
    let def = grammar::load(&text, rows[0].get(7))?;

    let mut source_columns = HashMap::new();
    for row in &rows {
        let key: Option<String> = row.get(5);
        let value: Option<String> = row.get(6);
        if let (Some(key), Some(value)) = (key, value) {
            let Some(value_type) = decode_value_type(&value) else {
                return Err(CatalogError::UnknownValueType {
                    column: key,
                    text: value,
                });
            };
            source_columns.insert(key, value_type);
        }
    }

    Ok(Some(Definition {
        id,
        source_version,
        def,
        source_columns,
        status,
        source_table,
        target_table,
    }))
}

/// The one predicate every caller-facing transform name goes through: SQL
/// over `column` (a `transform_definitions.target_table` expression) true
/// when it is the transform the text parameter `$param` names. A name
/// containing a `.` is the schema-qualified spelling `define`/`apply` return
/// and matches `target_table` exactly (so a wrong schema matches nothing); a
/// name without one is the bare spelling and matches the table-name suffix.
/// The suffix is globally unique (`transform_definitions_target_suffix_idx`),
/// so neither form is ever ambiguous.
pub(crate) fn target_match_sql(column: &str, param: usize) -> String {
    format!(
        "(case when position('.' in ${param}::text) > 0 then {column} = ${param}::text \
         else split_part({column}, '.', 2) = ${param}::text end)"
    )
}

/// The bare table-name suffix of the transform `name` (qualified or bare, see
/// [`target_match_sql`]) names, or `None` when no transform matches. Entry
/// points that hand a name on to code keyed by the bare suffix
/// (`column_status`, the quarantine tables) normalize through this once.
pub(crate) async fn bare_target_name(
    client: &impl GenericClient,
    name: &str,
) -> Result<Option<String>, CatalogError> {
    let row = client
        .query_opt(
            &format!(
                "select split_part(target_table, '.', 2) from transform_definitions where {}",
                target_match_sql("target_table", 1)
            ),
            &[&name],
        )
        .await?;
    Ok(row.map(|row| row.get(0)))
}

/// Reads back one definition by its target table name — [`definition_by_id`]
/// keyed the other way, for callers that only have the address a `Trellis`
/// caller would use (`docs/decisions/0003-quarantine-storage-and-api.md`:
/// a quarantine target is `transform` or `transform.column`,
/// where `transform` is this crate's `target_table`). Used by
/// `staging::quarantine`'s column-resume path to reconstruct the
/// [`super::ast::TransformDef`] whose column it's re-deriving.
///
/// `target_table` is the name a caller has of the transform: the qualified
/// `schema.table` that `define`/`apply` return, or the bare table name its
/// `TRANSFORM <name> FROM ...` text declared ([`target_match_sql`]). A
/// `transform.column` address is not a transform name; callers split that on
/// its first `.` before they get here.
pub async fn definition_by_target(
    pool: &Pool,
    target_table: &str,
) -> Result<Option<Definition>, CatalogError> {
    let client = pool.get().await?;
    let rows = client
        .query(
            &format!(
                "select t.id, t.source_version, t.definition_text, t.status, t.source_table, \
                        t.target_table, e.key, e.value, t.grammar_version \
                 from transform_definitions t \
                 left join lateral jsonb_each_text(t.source_columns) e on true \
                 where {}",
                target_match_sql("t.target_table", 1)
            ),
            &[&target_table],
        )
        .await?;
    if rows.is_empty() {
        return Ok(None);
    }

    let id: i64 = rows[0].get(0);
    let source_version: i64 = rows[0].get(1);
    let text: String = rows[0].get(2);
    let status_text: String = rows[0].get(3);
    let source_table: String = rows[0].get(4);
    let qualified_target_table: String = rows[0].get(5);
    let status = TransformStatus::from_persisted(&status_text).unwrap_or_else(|| {
        panic!("transform_definitions.status held unrecognized value '{status_text}'")
    });
    let def = grammar::load(&text, rows[0].get(8))?;

    let mut source_columns = HashMap::new();
    for row in &rows {
        let key: Option<String> = row.get(6);
        let value: Option<String> = row.get(7);
        if let (Some(key), Some(value)) = (key, value) {
            let Some(value_type) = decode_value_type(&value) else {
                return Err(CatalogError::UnknownValueType {
                    column: key,
                    text: value,
                });
            };
            source_columns.insert(key, value_type);
        }
    }

    Ok(Some(Definition {
        id,
        source_version,
        def,
        source_columns,
        status,
        source_table,
        target_table: qualified_target_table,
    }))
}

/// Every `(downstream_target_table, downstream_field_name)` pair whose
/// calculated-field expression reads `(upstream_table, upstream_column)` —
/// a chained transform whose `FROM` (or `JOIN` table) *is* `upstream_table`,
/// referencing the column by its name. This is column-level
/// lineage, deliberately *not* [`dependents_of`]'s table-level
/// `schema_edges` walk: that graph answers "does transform X read table Y at
/// all," which is too coarse for ADR-0003 — cascading a paused
/// *column* must not pause a downstream transform's *other* fields that don't
/// actually reference it. Scanning every definition's own field expressions
/// (rather than a persisted edge) is what makes this precise.
///
/// This function never reads `schema_nodes`/`schema_edges` at all — it
/// scans `transform_definitions` directly. The bare `upstream_table` is
/// resolved to its target's qualified identity, and matched against each
/// definition's qualified `source_table`, so a table
/// in another schema that merely shares the target's bare name is never a
/// match. `upstream_table` itself stays bare for the same
/// `column_status`-addressing reasons the `target` column below does (see
/// this function's own inline comment on that `select`).
///
/// Direct (one-hop) dependents only; `staging::quarantine`'s cascade walks
/// this transitively itself, relying on the same cycle-freedom
/// `docs/transforms.md#chaining-and-cycle-detection` guarantees for the
/// table-level graph (a column-level reference can only exist where a
/// table-level dependency edge already does, so the same DAG property
/// applies).
///
/// Filtered to [`KeySpace::OneToOne`] downstream definitions only — column-
/// level pause/cascade/resume is explicitly scoped to the 1-1 tier (see this
/// module's callers in `staging::quarantine` and that module's "Column-level
/// fuse" section doc comment: the group basis has no notion of
/// `column_status` at all). Without this filter, a
/// downstream [`KeySpace::Aggregate`] transform whose field happens to read a
/// just-paused upstream column would get a `column_status` row cascaded onto
/// it that nothing in the aggregate write path ever consults or clears, and
/// that `resume_column`'s cascade walk would later try (and fail) to
/// recompute through its 1-1 field build.
///
/// **Sibling readers first**. A 1-1 definition's own fields
/// that read `upstream_column` by alias ([`super::eval::AliasReaders`])
/// come first, under `upstream_table` itself: a field reading a paused
/// field is paused too (ADR-0003 freezes it rather than evaluating it over
/// the paused field's absence), so the cascade gives it a `column_status`
/// row, which `status` lists, and an edge, which keeps it paused until every
/// field it reads is resumed. They are one hop, like the rest; the cascade's
/// walk reaches a sibling's own readers, and its downstream ones, from it.
pub(crate) async fn column_dependents(
    pool: &Pool,
    upstream_table: &str,
    upstream_column: &str,
) -> Result<Vec<(String, String)>, CatalogError> {
    let mut deps: Vec<(String, String)> = match definition_by_target(pool, upstream_table).await? {
        Some(definition) if matches!(definition.def.key_space, KeySpace::OneToOne) => {
            super::eval::AliasReaders::of(&definition.def)
                .direct(upstream_column)
                .iter()
                .map(|reader| (upstream_table.to_string(), reader.clone()))
                .collect()
        }
        _ => Vec::new(),
    };
    let client = pool.get().await?;
    deps.extend(column_dependents_via(&**client, upstream_table, upstream_column, true).await?);
    Ok(deps)
}

/// Whether `reader` (a `(transform, field)` pair) is, as the committed
/// catalog stands, a dependent [`column_dependents`] would list for
/// `upstream`: its definition still has the field, is 1-1, and the field
/// still reads the upstream column, directly or by alias. The cascade walk lists a column's readers outside any lock
/// and re-asks this for each pair under the column-pause lock, which every
/// edit of a definition holds exclusive, so the answer can't change before
/// the pair commits.
pub(crate) async fn reads_column(
    client: &impl GenericClient,
    (reader_transform, reader_field): (&str, &str),
    (upstream_table, upstream_column): (&str, &str),
) -> Result<bool, CatalogError> {
    if reader_transform == upstream_table {
        let text: Option<(String, i32)> = client
            .query_opt(
                "select definition_text, grammar_version from transform_definitions \
                 where split_part(target_table, '.', 2) = $1",
                &[&upstream_table],
            )
            .await?
            .map(|row| (row.get(0), row.get(1)));
        if let Some((text, grammar_version)) = text {
            let def = grammar::load(&text, grammar_version)?;
            if matches!(def.key_space, KeySpace::OneToOne)
                && super::eval::AliasReaders::of(&def)
                    .direct(upstream_column)
                    .iter()
                    .any(|reader| reader == reader_field)
            {
                return Ok(true);
            }
        }
    }
    Ok(
        column_dependents_via(client, upstream_table, upstream_column, true)
            .await?
            .iter()
            .any(|(transform, field)| transform == reader_transform && field == reader_field),
    )
}

/// [`column_dependents`], generalized to *every* downstream key-space (not
/// just [`KeySpace::OneToOne`]) and to any `impl GenericClient` (a plain
/// pooled connection, or a transaction) rather than just a fresh pooled one —
/// the column-granularity dependency-edge infrastructure a *drop* refusal
/// needs, as opposed to [`column_dependents`]'s own
/// pause/cascade/resume use.
///
/// **Why the wider key-space net matters here.** `column_dependents`'s
/// 1-1-only filter is correct for its own callers (`staging::quarantine`'s
/// column-pause cascade — nothing outside the 1-1 tier has a `column_status`
/// row to cascade onto), but is exactly *wrong* for a drop refusal: an
/// aggregate transform's `GROUP BY` can key on a chained source column just
/// as readily as a 1-1 field can,
/// and dropping the column such a grouping key reads is exactly as
/// destructive as dropping one a 1-1 field reads — the refusal must see it
/// too. So this walks every downstream definition's [`KeySpace::Aggregate`]
/// `group_by` keys in addition to every definition's `fields`, named by the
/// key's column when a grouping key itself is the match — the same
/// target-column-name convention [`super::validate`]'s own
/// grouping-key/field-passthrough checks use.
///
/// Used by [`super::lifecycle::drop_transform`] (naming precision only —
/// every blocker this finds is already caught by that function's
/// table-level checks, per this module's own `column_dependents` doc
/// comment's DAG-property note) and by [`alter_transform`]'s `DROP <field>`
/// clause (where it is the *entire* refusal check, not just a naming
/// enhancement — column-granularity dependency edges are the only edges a
/// single-field drop has to check against).
pub(crate) async fn column_dependents_any_keyspace(
    client: &impl GenericClient,
    upstream_table: &str,
    upstream_column: &str,
) -> Result<Vec<(String, String)>, CatalogError> {
    column_dependents_via(client, upstream_table, upstream_column, false).await
}

/// A definition that reads a re-typed column of another's target and that
/// define would now refuse ([`refused_readers`]).
#[derive(Debug)]
pub(crate) struct RefusedReader {
    pub id: i64,
    /// What define's validation of it refuses.
    pub reason: CatalogError,
}

/// Every definition that reads one of `columns` of the target `upstream`
/// (bare name) and that define's whole validation ([`revalidate`]) now
/// refuses, as the schema stands in `txn`. The caller has re-typed
/// the columns in `txn`, so the validation sees the new types. A reader
/// that stays valid isn't returned; a database error propagates.
pub(crate) async fn refused_readers(
    txn: &tokio_postgres::Transaction<'_>,
    schema: &str,
    upstream: &str,
    columns: &[String],
) -> Result<Vec<RefusedReader>, tokio_postgres::Error> {
    let mut refused = Vec::new();
    for (id, _) in column_readers(txn, upstream, columns).await? {
        let checked = match definition_by_id_in(txn, id).await {
            Ok(Some(definition)) => revalidate(txn, schema, &definition).await.map(|_| ()),
            Ok(None) => continue,
            Err(err) => Err(err),
        };
        match checked {
            Ok(()) => {}
            Err(CatalogError::Db(err)) => return Err(err),
            Err(reason) => refused.push(RefusedReader { id, reason }),
        }
    }
    Ok(refused)
}

/// The id and bare target of every definition that reads one of `columns`
/// of the target `upstream` (bare name), in id order: the definitions
/// [`refused_readers`] validates. Which definitions read a column doesn't
/// depend on its type, so a caller can read them before it re-types it.
pub(crate) async fn column_readers(
    client: &impl GenericClient,
    upstream: &str,
    columns: &[String],
) -> Result<Vec<(i64, String)>, tokio_postgres::Error> {
    let mut targets: Vec<String> = Vec::new();
    for column in columns {
        match column_dependents_any_keyspace(client, upstream, column).await {
            Ok(deps) => targets.extend(deps.into_iter().map(|(target, _)| target)),
            Err(CatalogError::Db(err)) => return Err(err),
            Err(_) => {}
        }
    }
    targets.sort();
    targets.dedup();
    let mut readers = Vec::new();
    for target in targets {
        if let Some(row) = client
            .query_opt(
                "select id from transform_definitions where split_part(target_table, '.', 2) = $1",
                &[&target],
            )
            .await?
        {
            readers.push((row.get(0), target));
        }
    }
    readers.sort();
    Ok(readers)
}

async fn column_dependents_via(
    client: &impl GenericClient,
    upstream_table: &str,
    upstream_column: &str,
    only_one_to_one: bool,
) -> Result<Vec<(String, String)>, CatalogError> {
    let def_rows = client
        .query(
            // `split_part(target_table, '.', 2)`, not the qualified column
            // directly: the `String` this returns for each row
            // is pushed straight into `deps` below as a *downstream
            // transform* identifier, which flows into
            // `staging::quarantine`'s `column_status`/`column_pause_cascades`
            // bookkeeping — an entirely bare-keyed subsystem seeded from the
            // bare `transform` a `Trellis` caller passes to `pause_column`/
            // `resume_column`. Returning the newly-qualified spelling here
            // instead would split that bookkeeping across two spellings of
            // the same transform depending on whether a row was reached
            // directly or via cascade.
            "select split_part(target_table, '.', 2), definition_text, source_table, \
                    target_table, grammar_version, join_table \
             from transform_definitions",
            &[],
        )
        .await?;

    // `upstream_table` is a bare target name, but a reader is
    // matched against it by qualified identity: a definition over
    // `other.t` doesn't read the target `public.t`. Bare target suffixes are
    // unique ([`CatalogError::TargetTableSuffixCollision`]), so the bare name
    // picks out at most one qualified target. A name that is no definition's
    // target has no chained readers to find.
    let Some(qualified_upstream) = def_rows.iter().find_map(|row| {
        (row.get::<_, String>(0) == upstream_table).then(|| row.get::<_, String>(3))
    }) else {
        return Ok(Vec::new());
    };

    let mut deps = Vec::new();
    for row in def_rows {
        let target: String = row.get(0);
        let text: String = row.get(1);
        // The definition's qualified source, which it reads directly.
        let qualified_source: String = row.get(2);
        // A `JOIN`'s joined table, which it reads directly too.
        let joined: Option<String> = row.get(5);
        // A definition already persisted here is expected to always re-parse
        // (the same assumption every other read path in this module makes);
        // skip rather than fail this best-effort lineage scan on the
        // unexpected chance it doesn't, rather than let one bad row prevent
        // cascading a pause to every other, healthy dependent.
        //
        // The exception is a definition written under a newer grammar
        // (ADR-0018) that could read the upstream table. For the drop refusal this scan is the whole check,
        // and whether that definition reads the column can't be known here,
        // so the drop is refused rather than allowed past it. A definition
        // older than the window is paused for good and redefined from
        // scratch, so it doesn't hold a drop up.
        let def = match grammar::load(&text, row.get(4)) {
            Ok(def) => def,
            Err(e @ ParseError::GrammarVersionTooNew { .. })
                if !only_one_to_one
                    && (qualified_source == qualified_upstream
                        || joined.as_deref() == Some(qualified_upstream.as_str())) =>
            {
                return Err(e.into());
            }
            Err(_) => continue,
        };
        if only_one_to_one && !matches!(def.key_space, KeySpace::OneToOne) {
            continue;
        }
        // A `JOIN` names every column by its table. It reads the upstream column through
        // whichever side is the upstream table: in a field, or in its `ON` or `GROUP BY`
        // (named by that column, which a `GROUP BY` key's target column is).
        if super::join::spec(&def).is_some() {
            for (side, table) in [
                (super::join::Side::From, Some(&qualified_source)),
                (super::join::Side::Joined, joined.as_ref()),
            ] {
                if table != Some(&qualified_upstream) {
                    continue;
                }
                let mut named = false;
                for field in &def.fields {
                    if super::join::expr_reads(&def, &field.expr, side, upstream_column) {
                        deps.push((target.clone(), field.name.clone()));
                        named = true;
                    }
                }
                if !named && super::join::side_columns(&def, side).contains(upstream_column) {
                    deps.push((target.clone(), upstream_column.to_string()));
                }
            }
            continue;
        }
        for field in &def.fields {
            if expr_references_column(
                &field.expr,
                &qualified_source,
                &qualified_upstream,
                upstream_column,
            ) {
                deps.push((target.clone(), field.name.clone()));
            }
        }
        // A `GROUP BY` key is not itself a `FieldDef` in `def.fields` — see
        // `ast::KeySpace::Aggregate`'s own doc comment — so the loop above
        // never sees it. Only reachable when `only_one_to_one` is false
        // (see this function's own doc comment on why the drop-refusal
        // caller needs this and the pause-cascade caller must not).
        if !only_one_to_one && let KeySpace::Aggregate { group_by } = &def.key_space {
            for key in group_by {
                if expr_references_column(
                    &Expr::Column(key.clone()),
                    &qualified_source,
                    &qualified_upstream,
                    upstream_column,
                ) {
                    deps.push((target.clone(), key.clone()));
                }
            }
        }
    }
    Ok(deps)
}

/// Whether `expr` (one calculated field's expression, belonging to a
/// definition whose qualified `FROM` is `def_source`) reads
/// `(upstream_table, upstream_column)`, `upstream_table` also qualified — see
/// [`column_dependents`].
fn expr_references_column(
    expr: &Expr,
    def_source: &str,
    upstream_table: &str,
    upstream_column: &str,
) -> bool {
    match expr {
        Expr::Column(name) | Expr::SourceColumn { column: name, .. } => {
            def_source == upstream_table && name == upstream_column
        }
        Expr::BinaryOp { lhs, rhs, .. } => {
            expr_references_column(lhs, def_source, upstream_table, upstream_column)
                || expr_references_column(rhs, def_source, upstream_table, upstream_column)
        }
        Expr::FunctionCall { args, .. } => args
            .iter()
            .any(|arg| expr_references_column(arg, def_source, upstream_table, upstream_column)),
        Expr::NumberLiteral(_) | Expr::StringLiteral(_) | Expr::TypedLiteral { .. } => false,
    }
}

#[cfg(test)]
mod live_fixture_tests {
    use super::*;

    /// The fixtures that flip a definition `live` without a build
    /// refuse a `JOIN`, which only its build makes `live`; any other shape
    /// passes.
    #[test]
    #[should_panic(expected = "a JOIN definition can't be registered `live`")]
    fn the_live_fixtures_refuse_a_join() {
        refuse_join_fixture("TRANSFORM t FROM orders SELECT id AS id");
        refuse_join_fixture(
            "TRANSFORM t FROM orders JOIN items ON orders.id = items.order_id \
             GROUP BY orders.region SELECT COUNT(*) AS n",
        );
    }
}

/// A column's dependents are the definitions that actually read
/// its target, matched on qualified identity, not on a bare name another
/// schema's table can share.
#[cfg(test)]
mod column_dependents_tests {
    use super::*;

    #[tokio::test]
    async fn a_same_named_table_in_another_schema_is_not_a_dependent() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let client = db.pool.get().await.expect("acquire connection");
        client
            .batch_execute(
                "insert into source_table_versions (source_table, version) \
                 values ('public.s', 1), ('public.t', 1), ('other.t', 1); \
                 insert into transform_definitions \
                 (target_table, source_table, source_version, definition_text, status) \
                 values \
                 ('public.t', 'public.s', 1, 'TRANSFORM t FROM s SELECT a AS a', 'live'), \
                 ('public.chained', 'public.t', 1, \
                  'TRANSFORM chained FROM t SELECT a AS a_copy', 'live'), \
                 ('public.reader', 'other.t', 1, \
                  'TRANSFORM reader FROM other.t SELECT a AS a_copy', 'live')",
            )
            .await
            .expect("seed definitions");

        let mut deps = column_dependents_any_keyspace(&**client, "t", "a")
            .await
            .expect("column dependents");
        deps.sort();
        // `reader` reads `other.t.a`: a different table that only shares the
        // bare name `t` with the target `public.t`.
        assert_eq!(deps, vec![("chained".to_string(), "a_copy".to_string())]);
    }

    /// A `JOIN` reads an upstream target's column through whichever
    /// of its tables the target is, in a field or in its `ON` or `GROUP BY`,
    /// so dropping that field is refused. A column of the other table that
    /// only shares the name isn't a read of the target's.
    #[tokio::test]
    async fn a_join_reads_a_target_column_through_either_of_its_tables() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let client = db.pool.get().await.expect("acquire connection");
        client
            .batch_execute(
                "insert into source_table_versions (source_table, version) \
                 values ('public.s', 1), ('public.t', 1), ('public.orders', 1), \
                        ('public.items', 1); \
                 insert into transform_definitions \
                 (target_table, source_table, source_version, definition_text, status, \
                  join_table, join_columns) \
                 values \
                 ('public.t', 'public.s', 1, \
                  'TRANSFORM t FROM s SELECT a AS a, b AS b, k AS k', 'live', null, null), \
                 ('public.j_joined', 'public.orders', 1, \
                  'TRANSFORM j_joined FROM orders JOIN t ON orders.id = t.k \
                   GROUP BY t.b SELECT t.b AS b, SUM(t.a) AS total', \
                  'waiting_to_backfill', 'public.t', '{}'), \
                 ('public.j_from', 'public.t', 1, \
                  'TRANSFORM j_from FROM t JOIN items ON t.k = items.id \
                   GROUP BY items.sku SELECT items.sku AS sku, SUM(items.a) AS a_sum', \
                  'waiting_to_backfill', 'public.items', '{}')",
            )
            .await
            .expect("seed definitions");

        for (column, expected) in [
            ("a", vec![("j_joined", "total")]),
            ("b", vec![("j_joined", "b")]),
            ("k", vec![("j_from", "k"), ("j_joined", "k")]),
        ] {
            let mut deps = column_dependents_any_keyspace(&**client, "t", column)
                .await
                .expect("column dependents");
            deps.sort();
            let expected: Vec<(String, String)> = expected
                .into_iter()
                .map(|(t, f)| (t.to_string(), f.to_string()))
                .collect();
            assert_eq!(deps, expected, "t.{column}");
        }
    }

    /// The cascade's per-pair re-check, [`reads_column`], says
    /// yes for exactly the pairs [`column_dependents`] lists, in every shape
    /// a reader can take: a sibling reading by alias, a chained field (bare,
    /// qualified, inside a function call, two reads in one field), and an
    /// aggregate reader (which neither lists).
    /// A pair it said no to that the walk lists would leave a genuine reader
    /// unpaused.
    #[tokio::test]
    async fn reads_column_agrees_with_column_dependents_for_every_pair() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let client = db.pool.get().await.expect("acquire connection");
        let definitions = [
            (
                "public.t",
                "public.s",
                "TRANSFORM t FROM s SELECT a + 1 AS x, x + 2 AS y, COALESCE(b, x) AS z, \
                 s.b AS sb",
            ),
            (
                "public.chained",
                "public.t",
                "TRANSFORM chained FROM t SELECT x AS cx, t.y AS cy, COALESCE(z, 0) AS cz, \
                 x + t.y AS both_ways, public.t.z AS qz, 1 AS lit",
            ),
            (
                "public.agg",
                "public.t",
                "TRANSFORM agg FROM t GROUP BY z SELECT SUM(x) AS total",
            ),
        ];
        client
            .batch_execute(
                "insert into source_table_versions (source_table, version) \
                 values ('public.s', 1), ('public.t', 1)",
            )
            .await
            .expect("seed versions");
        let mut readers: Vec<(String, String)> = vec![("chained".into(), "no_such".into())];
        let mut columns: Vec<(String, String)> = vec![
            ("s".into(), "a".into()),
            ("t".into(), "a".into()),
            ("t".into(), "no_such".into()),
        ];
        for (target, source, text) in definitions {
            let def = parse(text).unwrap_or_else(|e| panic!("{text}: {e:?}"));
            let bare = target.split_once('.').expect("qualified").1;
            for field in &def.fields {
                readers.push((bare.into(), field.name.clone()));
                columns.push((bare.into(), field.name.clone()));
            }
            client
                .execute(
                    "insert into transform_definitions \
                     (target_table, source_table, source_version, definition_text, status) \
                     values ($1, $2, 1, $3, 'live')",
                    &[&target, &source, &text],
                )
                .await
                .expect("seed a definition");
        }

        let pool = crate::pool::Pool::new(
            &crate::config::Config::from_dsn(db.dsn().to_string()).expect("valid dsn"),
        )
        .expect("pool");
        let mut listed = HashSet::new();
        for (table, column) in &columns {
            let deps = column_dependents(&pool, table, column)
                .await
                .expect("column dependents");
            for reader in &readers {
                let reads = reads_column(
                    &**client,
                    (&reader.0, &reader.1),
                    (table.as_str(), column.as_str()),
                )
                .await
                .expect("reads column");
                assert_eq!(
                    reads,
                    deps.contains(reader),
                    "{reader:?} reading {table}.{column}: column_dependents lists {deps:?}"
                );
                if reads {
                    listed.insert((
                        reader.0.clone(),
                        reader.1.clone(),
                        table.clone(),
                        column.clone(),
                    ));
                }
            }
        }
        // The shapes the comparison is meant to cover are really there.
        for (transform, field, table, column) in [
            ("t", "y", "t", "x"),
            ("t", "z", "t", "x"),
            ("chained", "cx", "t", "x"),
            ("chained", "cy", "t", "y"),
            ("chained", "cz", "t", "z"),
            ("chained", "qz", "t", "z"),
            ("chained", "both_ways", "t", "x"),
            ("chained", "both_ways", "t", "y"),
        ] {
            let expected = (transform.into(), field.into(), table.into(), column.into());
            assert!(
                listed.contains(&expected),
                "{expected:?} not listed: {listed:?}"
            );
        }
        assert!(
            !listed.iter().any(|(transform, ..)| transform == "agg"),
            "an aggregate reader is never a cascade dependent: {listed:?}"
        );
    }
}

/// The `ALTER` type-change guard reads a mixed-case target's
/// column, not the nonexistent lower-case table an unquoted `to_regclass`
/// folds its name to (which read as "no such column" and let the type change
/// through).
#[cfg(test)]
mod type_changing_alter_tests {
    use super::*;

    #[tokio::test]
    async fn a_type_change_on_a_mixed_case_target_is_refused() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let client = db.pool.get().await.expect("acquire connection");
        client
            .batch_execute(
                "create schema \"Custom\"; \
                 create table \"Custom\".\"Totals\" (id bigint primary key, total numeric)",
            )
            .await
            .expect("seed the mixed-case target");

        let merged = parse("TRANSFORM totals FROM s SELECT a + 1 AS total").expect("parse");
        let fields = merged.fields.clone();
        let check = |family: ValueType| {
            let (merged, fields, client) = (&merged, &fields, &client);
            async move {
                let field_types = HashMap::from([("total".to_string(), family)]);
                check_alter_column_types(
                    &***client,
                    &AlterTypeContext {
                        merged,
                        source_table: "public.s",
                        source_columns: &HashMap::new(),
                        field_types: &field_types,
                        qualified_target: "Custom.Totals",
                        display_target: "Totals",
                    },
                    fields,
                )
                .await
            }
        };
        let err = check(ValueType::Boolean)
            .await
            .expect_err("numeric to boolean is a type change");
        assert!(
            matches!(err, CatalogError::UnsupportedAlter(_)),
            "expected UnsupportedAlter, got {err:?}"
        );
        assert!(
            check(ValueType::Numeric).await.is_ok(),
            "an unchanged type passes"
        );
    }
}

/// [`plan_alter`] nets an edit's clauses against each other.
#[cfg(test)]
mod retyped_read_tests {
    use super::*;
    use crate::float::FloatWidth;
    use crate::integer::IntWidth;

    const INT: ValueType = ValueType::Integer(IntWidth::Int4);
    const DOUBLE: ValueType = ValueType::Float(FloatWidth::Float8);

    /// `t` over `s`, whose `x` was recorded as `integer` and is `double
    /// precision` now, and whose `y` is `numeric` in both.
    fn retyped(definition: &str, build: &[&str]) -> Option<RetypedRead> {
        let def = parse(definition).expect("parse the definition");
        let recorded: HashMap<String, ValueType> = [
            ("x".to_string(), INT),
            ("y".to_string(), ValueType::Numeric),
        ]
        .into();
        let live: HashMap<String, ValueType> = [
            ("x".to_string(), DOUBLE),
            ("y".to_string(), ValueType::Numeric),
            ("w".to_string(), ValueType::Numeric),
        ]
        .into();
        let build: Vec<String> = build.iter().map(|f| f.to_string()).collect();
        field_over_retyped_column(&def, &build, &recorded, &live)
    }

    #[test]
    fn a_built_field_reading_a_retyped_column_is_named_with_both_types() {
        assert_eq!(
            retyped("TRANSFORM t FROM s SELECT y AS y, x + y AS xy", &["xy"]),
            Some(RetypedRead {
                field: "xy".to_string(),
                column: "x".to_string(),
                recorded: INT,
                live: DOUBLE,
            })
        );
    }

    #[test]
    fn an_unbuilt_field_reading_a_retyped_column_is_left_alone() {
        assert_eq!(
            retyped("TRANSFORM t FROM s SELECT x AS x, y + y AS y2", &["y2"]),
            None
        );
    }

    #[test]
    fn a_built_field_reading_a_retyped_column_through_a_field_is_named() {
        let read = retyped(
            "TRANSFORM t FROM s SELECT x + y AS xy, xy + y AS later",
            &["later"],
        )
        .expect("`later` reads `x` through `xy`");
        assert_eq!((read.field.as_str(), read.column.as_str()), ("later", "x"));
    }

    #[test]
    fn a_column_added_since_the_record_is_not_retyped() {
        assert_eq!(
            retyped("TRANSFORM t FROM s SELECT w + y AS wy", &["wy"]),
            None
        );
    }

    #[test]
    fn a_passthrough_of_a_retyped_column_is_named() {
        assert_eq!(
            retyped("TRANSFORM t FROM s SELECT x AS x", &["x"]).map(|read| read.column),
            Some("x".to_string())
        );
    }
}

#[cfg(test)]
mod plan_alter_tests {
    use super::*;
    use crate::defs::ast::Statement;
    use crate::defs::parser::parse_statement;

    /// The definition `t`'s fields, `a` and `z` (`a + a`), edited by
    /// `ALTER TRANSFORM t <clauses>`.
    fn plan(clauses: &str) -> Result<AlterPlan, CatalogError> {
        let current = parse("TRANSFORM t FROM s SELECT a AS a, a + a AS z")
            .expect("parse the definition")
            .fields;
        plan_without(&current, clauses)
    }

    fn plan_without(current: &[FieldDef], clauses: &str) -> Result<AlterPlan, CatalogError> {
        let Statement::AlterTransform(alter) =
            parse_statement(&format!("ALTER TRANSFORM t {clauses}")).expect("parse the edit")
        else {
            panic!("expected an ALTER TRANSFORM");
        };
        plan_alter("t", current, &alter.clauses)
    }

    /// Only `a`: the edit's `z` is new.
    fn plan_new_z(clauses: &str) -> Result<AlterPlan, CatalogError> {
        let current = parse("TRANSFORM t FROM s SELECT a AS a")
            .expect("parse the definition")
            .fields;
        plan_without(&current, clauses)
    }

    fn names(fields: &[FieldDef]) -> Vec<&str> {
        fields.iter().map(|f| f.name.as_str()).collect()
    }

    #[test]
    fn an_add_then_a_drop_of_the_same_field_cancel() {
        let plan = plan_new_z("ADD a + a AS z, DROP z").expect("plan");
        assert_eq!(names(&plan.fields), ["a"]);
        assert!(plan.real_adds.is_empty(), "{plan:?}");
        assert!(plan.real_alters.is_empty(), "{plan:?}");
        assert!(plan.real_drops.is_empty(), "{plan:?}");
    }

    #[test]
    fn an_add_altered_and_then_dropped_cancels() {
        let plan = plan_new_z("ADD a + a AS z, ALTER z AS a + a + a, DROP z").expect("plan");
        assert_eq!(names(&plan.fields), ["a"]);
        assert!(
            plan.real_adds.is_empty() && plan.real_alters.is_empty(),
            "{plan:?}"
        );
        assert!(plan.real_drops.is_empty(), "{plan:?}");
    }

    #[test]
    fn a_drop_then_an_add_replaces_the_field() {
        let plan = plan("DROP z, ADD a > a AS z").expect("plan");
        assert_eq!(names(&plan.fields), ["a", "z"]);
        assert_eq!(plan.real_drops, ["z"]);
        assert_eq!(names(&plan.real_adds), ["z"]);
        assert!(plan.real_alters.is_empty(), "{plan:?}");
    }

    #[test]
    fn a_replace_dropped_again_is_only_a_drop() {
        let plan = plan("DROP z, ADD a > a AS z, DROP z").expect("plan");
        assert_eq!(names(&plan.fields), ["a"]);
        assert_eq!(plan.real_drops, ["z"]);
        assert!(
            plan.real_adds.is_empty() && plan.real_alters.is_empty(),
            "{plan:?}"
        );
    }

    #[test]
    fn an_alter_of_an_added_field_changes_what_is_added() {
        let plan = plan_new_z("ADD a + a AS z, ALTER z AS a > a").expect("plan");
        assert_eq!(names(&plan.real_adds), ["z"]);
        assert_eq!(plan.real_adds[0], plan.fields[1]);
        assert!(
            plan.real_alters.is_empty() && plan.real_drops.is_empty(),
            "{plan:?}"
        );
    }

    #[test]
    fn an_alter_and_its_reversal_cancel() {
        let plan = plan("ALTER z AS a + a + a, ALTER z AS a + a").expect("plan");
        assert!(plan.real_alters.is_empty(), "{plan:?}");
    }

    #[test]
    fn a_field_added_twice_is_added_once() {
        let plan = plan_new_z("ADD a + a AS z, ADD a + a AS z").expect("plan");
        assert_eq!(names(&plan.real_adds), ["z"]);
        assert_eq!(names(&plan.fields), ["a", "z"]);

        let err = plan_new_z("ADD a + a AS z, ADD a > a AS z").expect_err("two formulas");
        assert!(
            matches!(err, CatalogError::AlterFieldAlreadyExists { ref field, .. } if field == "z"),
            "{err:?}"
        );
    }

    #[test]
    fn a_field_dropped_twice_is_dropped_once() {
        let plan = plan("DROP z, DROP z").expect("plan");
        assert_eq!(plan.real_drops, ["z"]);
        assert_eq!(names(&plan.fields), ["a"]);
    }

    /// Every sequence of up to four clauses over two names and two formulas,
    /// from each of a few starting definitions, plans what applying the
    /// clauses one at a time as separate edits would do to the target's
    /// columns: a column the definition had survives unless a clause drops
    /// it, a field whose column didn't survive is added, and a surviving
    /// column whose formula ends up different is altered.
    #[test]
    fn the_plan_matches_the_clauses_applied_one_at_a_time() {
        let exprs = [Expr::Column("a".into()), Expr::NumberLiteral("1".into())];
        let field = |name: &str, e: usize| FieldDef {
            name: name.into(),
            expr: exprs[e].clone(),
        };
        let mut alphabet = Vec::new();
        for name in ["y", "z"] {
            for e in 0..exprs.len() {
                alphabet.push(AlterClause::Add(field(name, e)));
                alphabet.push(AlterClause::Alter(field(name, e)));
            }
            alphabet.push(AlterClause::Drop(name.into()));
        }
        let mut sequences: Vec<Vec<AlterClause>> = vec![vec![]];
        let mut frontier = sequences.clone();
        for _ in 0..4 {
            frontier = frontier
                .iter()
                .flat_map(|s| {
                    alphabet.iter().map(move |c| {
                        let mut s = s.clone();
                        s.push(c.clone());
                        s
                    })
                })
                .collect();
            sequences.extend(frontier.iter().cloned());
        }
        let starts = [
            vec![],
            vec![field("z", 0)],
            vec![field("z", 1)],
            vec![field("y", 0), field("z", 1)],
        ];

        // The reference: each field carries whether its column is one the
        // definition had (`true`) or one an earlier clause created.
        type Model = Vec<(FieldDef, bool)>;
        let apply = |model: &mut Model, clause: &AlterClause| -> Result<(), &'static str> {
            match clause {
                AlterClause::Add(f) => match model.iter().find(|(m, _)| m.name == f.name) {
                    Some((m, _)) if m.expr == f.expr => {}
                    Some(_) => return Err("exists"),
                    None => model.push((f.clone(), false)),
                },
                AlterClause::Alter(f) => match model.iter_mut().find(|(m, _)| m.name == f.name) {
                    Some((m, _)) => m.expr = f.expr.clone(),
                    None => return Err("not found"),
                },
                AlterClause::Drop(name) => model.retain(|(m, _)| &m.name != name),
            }
            Ok(())
        };

        for start in &starts {
            for clauses in &sequences {
                let context = format!("start {start:?}, clauses {clauses:?}");
                let mut model: Model = start.iter().map(|f| (f.clone(), true)).collect();
                let expected = clauses.iter().try_for_each(|c| apply(&mut model, c));
                let plan = match (expected, plan_alter("t", start, clauses)) {
                    (Ok(()), Ok(plan)) => plan,
                    (Err("exists"), Err(CatalogError::AlterFieldAlreadyExists { .. }))
                    | (Err("not found"), Err(CatalogError::AlterFieldNotFound { .. })) => continue,
                    (expected, actual) => panic!("{context}: {expected:?} vs {actual:?}"),
                };
                let fields_where = |keep: &dyn Fn(&FieldDef, bool) -> bool| -> Vec<FieldDef> {
                    model
                        .iter()
                        .filter(|(f, had)| keep(f, *had))
                        .map(|(f, _)| f.clone())
                        .collect()
                };
                assert_eq!(plan.fields, fields_where(&|_, _| true), "{context}");
                assert_eq!(plan.real_adds, fields_where(&|_, had| !had), "{context}");
                assert_eq!(
                    plan.real_alters,
                    fields_where(&|f, had| had && !start.contains(f)),
                    "{context}"
                );
                let lost: Vec<&str> = start
                    .iter()
                    .filter(|s| !model.iter().any(|(m, had)| *had && m.name == s.name))
                    .map(|s| s.name.as_str())
                    .collect();
                let mut drops: Vec<&str> = plan.real_drops.iter().map(String::as_str).collect();
                drops.sort_unstable();
                assert_eq!(drops, lost, "{context}");
            }
        }
    }
}

#[cfg(test)]
mod error_code_tests {
    use super::*;

    #[test]
    fn source_table_not_found_is_not_found() {
        assert_eq!(
            CatalogError::SourceTableNotFound("widgets".to_string()).code(),
            ErrorCode::NotFound
        );
    }

    #[test]
    fn unknown_value_type_is_internal() {
        assert_eq!(
            CatalogError::UnknownValueType {
                column: "price".to_string(),
                // `money` used to be this test's example of an
                // unrecognized persisted token — it isn't anymore (the OID
                // registry now classifies it as `ValueType::Other(PgType::Money)`),
                // so a genuinely made-up token stands in instead.
                text: "frobnicate".to_string(),
            }
            .code(),
            ErrorCode::Internal
        );
    }

    /// [`CatalogError::Parse`] must delegate to [`ParseError::code`] rather
    /// than hardcoding a category.
    #[test]
    fn parse_delegates_to_the_wrapped_parse_error() {
        let inner = ParseError::UnterminatedString;
        let expected = inner.code();
        let wrapped = CatalogError::Parse(inner);

        assert_eq!(wrapped.code(), expected);
        assert_eq!(wrapped.code(), ErrorCode::Parse);
    }

    /// [`CatalogError::Validate`] must delegate to [`ValidationError::code`]
    /// rather than hardcoding a category.
    #[test]
    fn validate_delegates_to_the_wrapped_validation_error() {
        let inner = ValidationError::DuplicateFieldName {
            name: "total".to_string(),
        };
        let expected = inner.code();
        let wrapped = CatalogError::Validate(inner);

        assert_eq!(wrapped.code(), expected);
        assert_eq!(wrapped.code(), ErrorCode::Validation);
    }
}

/// [`encode_value_type`]/[`decode_value_type`] are the
/// persistence codec for `transform_definitions.source_columns` — every
/// [`ValueType`] a real column can carry must round-trip through it exactly,
/// and an unrecognized token must be rejected rather than silently
/// misparsed, since `dependents_of`/`definition_by_id`/`definition_by_target`
/// all trust this pair to reconstruct a live [`TransformDef`]'s typing.
#[cfg(test)]
mod value_type_codec_tests {
    use super::*;

    #[test]
    fn every_value_type_round_trips_through_the_persisted_token() {
        let all = [
            ValueType::Numeric,
            ValueType::Text,
            ValueType::Boolean,
            ValueType::Uuid,
            ValueType::Other(PgType::Bytea),
            ValueType::Other(PgType::Jsonb),
            ValueType::Other(PgType::TimestampTz),
            ValueType::Other(PgType::Unrecognized),
        ];
        // The integer widths, from `IntWidth::ALL` so a future width is
        // covered by construction rather than by remembering to add it.
        let all = all
            .into_iter()
            .chain(IntWidth::ALL.into_iter().map(ValueType::Integer))
            // The float widths, from `FloatWidth::ALL` for the same
            // reason: a future width is covered by construction.
            .chain(FloatWidth::ALL.into_iter().map(ValueType::Float));
        for value_type in all {
            let token = encode_value_type(&value_type);
            assert_eq!(
                decode_value_type(token),
                Some(value_type),
                "token {token:?} did not round-trip"
            );
        }
    }

    #[test]
    fn the_four_original_tokens_are_unchanged() {
        // The original lattice's persisted spelling must stay
        // byte-identical — these tokens are durably stored in
        // `transform_definitions` rows, so changing them would break every
        // persisted definition.
        assert_eq!(encode_value_type(&ValueType::Numeric), "numeric");
        assert_eq!(encode_value_type(&ValueType::Text), "text");
        assert_eq!(encode_value_type(&ValueType::Boolean), "boolean");
        assert_eq!(encode_value_type(&ValueType::Uuid), "uuid");
    }

    #[test]
    fn an_unrecognized_token_does_not_decode() {
        assert_eq!(decode_value_type("frobnicate"), None);
    }
}

#[cfg(test)]
mod reported_status_tests {
    use super::{StatusNode, TransformStatus, derive_reported, derive_reported_statuses};

    fn node(id: i64, target: &str, status: TransformStatus, reads: &[&str]) -> StatusNode {
        StatusNode {
            id,
            target: target.to_string(),
            status,
            reads: reads.iter().map(|t| t.to_string()).collect(),
            paused_columns: Vec::new(),
        }
    }

    #[test]
    fn a_reader_of_a_non_live_upstream_reports_catching_up() {
        use TransformStatus::*;
        for upstream in [
            Paused,
            Quarantined,
            WaitingToBackfill,
            Backfilling,
            CatchingUp,
        ] {
            let reported = derive_reported_statuses(&[
                node(1, "public.u", upstream, &["public.s"]),
                node(2, "public.d", Live, &["public.u"]),
            ]);
            assert_eq!(reported[&1], upstream, "the upstream reports what it is");
            assert_eq!(
                reported[&2], CatchingUp,
                "a reader of a {upstream:?} upstream"
            );
        }
    }

    #[test]
    fn a_reader_of_a_live_upstream_stays_live() {
        use TransformStatus::*;
        let reported = derive_reported_statuses(&[
            node(1, "public.u", Live, &["public.s"]),
            node(2, "public.d", Live, &["public.u", "public.other"]),
        ]);
        assert_eq!(reported[&2], Live);
    }

    #[test]
    fn a_joined_table_is_an_upstream_too() {
        use TransformStatus::*;
        // `public.r` reads its own source and, as its `JOIN` table, the
        // target `public.u`.
        let reported = derive_reported_statuses(&[
            node(1, "public.u", Paused, &["public.s"]),
            node(2, "public.r", Live, &["public.reports", "public.u"]),
        ]);
        assert_eq!(reported[&2], CatchingUp);
    }

    #[test]
    fn the_rule_is_transitive_down_a_chain() {
        use TransformStatus::*;
        // Listed downstream-first so a single pass can't get it right.
        let reported = derive_reported_statuses(&[
            node(3, "public.c", Live, &["public.b"]),
            node(2, "public.b", Live, &["public.a"]),
            node(1, "public.a", CatchingUp, &["public.s"]),
        ]);
        assert_eq!(reported[&1], CatchingUp);
        assert_eq!(reported[&2], CatchingUp);
        assert_eq!(reported[&3], CatchingUp);
    }

    #[test]
    fn a_reader_of_a_paused_column_reports_catching_up_and_names_it_down_the_chain() {
        use TransformStatus::*;
        let mut reader = node(2, "public.agg", Live, &["public.t"]);
        reader.paused_columns = vec!["t.col".to_string()];
        let (reported, causes) = derive_reported(&[
            node(1, "public.t", Live, &["public.s"]),
            reader,
            node(3, "public.chained", Live, &["public.agg"]),
            node(4, "public.unrelated", Live, &["public.t"]),
        ]);
        assert_eq!(reported[&1], Live, "the paused column's own definition");
        assert_eq!(reported[&2], CatchingUp);
        assert_eq!(reported[&3], CatchingUp, "a reader of the reader");
        assert_eq!(reported[&4], Live, "reads no paused column");
        assert_eq!(causes[&2], ["t.col"]);
        assert_eq!(causes[&3], ["t.col"], "names the root cause");
        assert!(!causes.contains_key(&4));
    }

    #[test]
    fn only_a_live_status_is_ever_changed() {
        use TransformStatus::*;
        let reported = derive_reported_statuses(&[
            node(1, "public.u", Paused, &["public.s"]),
            node(2, "public.d", Paused, &["public.u"]),
            node(3, "public.e", Backfilling, &["public.u"]),
        ]);
        assert_eq!(reported[&2], Paused);
        assert_eq!(reported[&3], Backfilling);
    }
}

/// The pure key-collation check, over hand-built
/// [`LiveColumn`]s carrying the OIDs Postgres reports.
#[cfg(test)]
mod key_collation_tests {
    use super::*;

    fn column() -> LiveColumn {
        LiveColumn {
            collation: None,
            collation_deterministic: true,
        }
    }

    fn collated(mut column: LiveColumn, name: &str) -> LiveColumn {
        column.collation = Some(name.to_string());
        column
    }

    /// A key column with a nondeterministic collation is refused
    /// in every key role, and the error names the column, its collation and
    /// the key it belongs to.
    #[test]
    fn a_nondeterministic_key_column_is_rejected_naming_column_collation_and_role() {
        let mut ci = collated(column(), "case_insensitive");
        ci.collation_deterministic = false;
        for (key, role) in [
            (
                KeyColumnRole::SourceKey,
                "part of the source's row-identity key",
            ),
            (KeyColumnRole::GroupBy, "a GROUP BY key"),
        ] {
            let err = assert_deterministic_key_collation(key.clone(), "shop.orders", "code", &ci)
                .unwrap_err();
            assert_eq!(
                err,
                ValidationError::NondeterministicKeyCollation(Box::new(
                    NondeterministicKeyCollation {
                        key,
                        table: "shop.orders".to_string(),
                        column: "code".to_string(),
                        collation: "case_insensitive".to_string(),
                    }
                ))
            );
            let message = err.to_string();
            assert!(
                message.starts_with(&format!("shop.orders.code is {role}, and its collation"))
                    && message.contains(r#""case_insensitive" is nondeterministic"#),
                "{message}"
            );
        }
    }

    /// A deterministic collation passes whether it's the default,
    /// a non-default one (`"C"`), or absent (a type that isn't collatable).
    #[test]
    fn a_deterministic_or_absent_key_collation_passes() {
        for resolved in [
            collated(column(), "default"),
            collated(column(), "C"),
            column(),
        ] {
            assert!(
                assert_deterministic_key_collation(
                    KeyColumnRole::GroupBy,
                    "public.t",
                    "k",
                    &resolved
                )
                .is_ok()
            );
        }
    }
}

#[cfg(test)]
mod table_not_accessible_tests {
    use super::*;

    #[test]
    fn a_search_path_splits_the_way_postgres_splits_it() {
        assert_eq!(
            search_path_schemas("\"$user\", public", "trellis"),
            ["trellis", "public"]
        );
        assert_eq!(
            search_path_schemas("trellis,trellis_targets ,  public", "x"),
            ["trellis", "trellis_targets", "public"]
        );
        // Unquoted is lowercased, quoted is kept, and `""` is a quote.
        assert_eq!(
            search_path_schemas("App, \"My, \"\"Odd\"\" Schema\", $user", "Who"),
            ["app", "My, \"Odd\" Schema", "Who"]
        );
        // Postgres lowercases only ASCII letters in a UTF-8 database: an
        // unquoted `É` names schema `É`, not `é`.
        assert_eq!(search_path_schemas("Éa, éB", "x"), ["Éa", "éb"]);
        assert_eq!(search_path_schemas("", "x"), Vec::<String>::new());
        assert_eq!(search_path_schemas("  ", "x"), Vec::<String>::new());
    }

    fn not_accessible(usage: bool, privilege: bool) -> CatalogError {
        CatalogError::TableNotAccessible {
            table: "public.orders".to_string(),
            role: "trellis".to_string(),
            owner: "app_owner".to_string(),
            lacks_schema_usage: usage,
            lacks_table_privilege: privilege,
        }
    }

    #[test]
    fn the_message_names_what_the_role_lacks_and_the_doc_section() {
        let doc = "see \"One Trellis role\" in docs/recommendations.md";
        let usage = not_accessible(true, false).to_string();
        assert!(
            usage.contains("lacks USAGE on schema \"public\""),
            "{usage}"
        );
        assert!(!usage.contains("app_owner"), "{usage}");
        let privilege = not_accessible(false, true).to_string();
        assert!(
            privilege.contains("isn't a member of \"app_owner\", the table's owner"),
            "{privilege}"
        );
        assert!(!privilege.contains("USAGE on schema"), "{privilege}");
        let both = not_accessible(true, true).to_string();
        assert!(both.contains("lacks USAGE on schema \"public\""), "{both}");
        assert!(both.contains("\"app_owner\""), "{both}");
        for message in [usage, privilege, both] {
            assert!(
                message.starts_with(
                    "table \"public.orders\" exists, but role \"trellis\", which Trellis runs as, "
                ),
                "{message}"
            );
            assert!(message.ends_with(doc), "{message}");
        }
        assert_eq!(not_accessible(true, true).code(), ErrorCode::Validation);
    }
}

/// ADR-0018: the catalog scans that read every definition handle one outside
/// this binary's grammar window as that window's rules require.
#[cfg(test)]
mod grammar_window_tests {
    use super::*;

    /// Seeds `public.t` over `public.s`, `public.chained` reading `t.a`,
    /// and `public.other` over the unrelated `public.u`, then stamps the
    /// grammar versions given for `chained` and `other`.
    async fn seed(client: &impl GenericClient, chained: i32, other: i32) {
        client
            .batch_execute(
                "insert into source_table_versions (source_table, version) \
                 values ('public.s', 1), ('public.t', 1), ('public.u', 1); \
                 insert into transform_definitions \
                 (target_table, source_table, source_version, definition_text, status) \
                 values \
                 ('public.t', 'public.s', 1, 'TRANSFORM t FROM s SELECT a AS a', 'live'), \
                 ('public.chained', 'public.t', 1, \
                  'TRANSFORM chained FROM t SELECT a AS a_copy', 'paused'), \
                 ('public.other', 'public.u', 1, \
                  'TRANSFORM other FROM u SELECT a AS a', 'paused')",
            )
            .await
            .expect("seed definitions");
        for (target, version) in [("public.chained", chained), ("public.other", other)] {
            client
                .execute(
                    "update transform_definitions set grammar_version = $2 \
                     where target_table = $1",
                    &[&target, &version],
                )
                .await
                .expect("stamp");
        }
    }

    /// A definition older than the window is left out of the capture
    /// readers, so it can't stop capture for every other one; a newer one
    /// fails the read.
    #[tokio::test]
    async fn capture_readers_leave_out_a_too_old_definition_and_refuse_a_too_new_one() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let client = db.pool.get().await.expect("acquire connection");
        let old = grammar::OLDEST_SUPPORTED_GRAMMAR_VERSION - 1;
        seed(&**client, old, CURRENT_GRAMMAR_VERSION).await;

        let definitions = capture_readers(&**client).await.expect("capture readers");
        let targets: Vec<&str> = definitions.iter().map(|d| d.target.as_str()).collect();
        assert_eq!(targets, ["public.t", "public.other"]);

        client
            .execute(
                "update transform_definitions set grammar_version = $1 \
                 where target_table = 'public.chained'",
                &[&(CURRENT_GRAMMAR_VERSION + 1)],
            )
            .await
            .expect("stamp newer");
        match capture_readers(&**client).await {
            Err(CatalogError::Parse(ParseError::GrammarVersionTooNew { .. })) => {}
            other => panic!("expected GrammarVersionTooNew, got {other:?}"),
        }
    }

    /// Dropping a field is refused while a definition this binary can't read
    /// could read it; a newer definition over an unrelated table, or a
    /// too-old one, doesn't hold it up, and the pause cascade skips both.
    #[tokio::test]
    async fn a_field_drop_is_refused_past_a_newer_definition_that_could_read_it() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let client = db.pool.get().await.expect("acquire connection");
        let newer = CURRENT_GRAMMAR_VERSION + 1;
        seed(&**client, newer, newer).await;

        match column_dependents_any_keyspace(&**client, "t", "a").await {
            Err(CatalogError::Parse(ParseError::GrammarVersionTooNew { stored, .. })) => {
                assert_eq!(stored, newer);
            }
            other => panic!("expected GrammarVersionTooNew, got {other:?}"),
        }
        let pool = crate::pool::Pool::new(
            &crate::config::Config::from_dsn(db.dsn().to_string()).expect("valid dsn"),
        )
        .expect("pool");
        assert_eq!(
            column_dependents(&pool, "t", "a").await.expect("cascade"),
            Vec::<(String, String)>::new()
        );

        client
            .execute(
                "update transform_definitions set grammar_version = $1 \
                 where target_table = 'public.chained'",
                &[&(grammar::OLDEST_SUPPORTED_GRAMMAR_VERSION - 1)],
            )
            .await
            .expect("stamp too old");
        assert_eq!(
            column_dependents_any_keyspace(&**client, "t", "a")
                .await
                .expect("an unrelated newer definition and a too-old one don't refuse"),
            Vec::<(String, String)>::new()
        );
    }
}
