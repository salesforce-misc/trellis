//! Postgres-backed catalog for transform definitions (issue #23).
//!
//! **Schema choice**: a definition is stored as its original source text
//! (`transform_definitions.definition_text`) rather than a serialized AST.
//! Reading it back re-parses via [`super::parse`], reusing the grammar's
//! own parser instead of standing up a second, independently-drifting
//! serialization format for the same information. The tradeoff is a
//! re-parse on every read; given definitions are small and read
//! infrequently relative to the source-row volume they govern, that's the
//! right side to take the cost on. Flagged in the issue #23 report as the
//! open question to confirm.
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
//! **Source and target identity are both fully-qualified (issues #72/#73,
//! ADR-0007).** `transform_definitions.source_table`/`target_table` and
//! `source_table_versions.source_table` hold `def.source`/`def.target`
//! resolved to their `schema.table` identity exactly once, at
//! definition-acceptance time (`create_definition_inner`'s
//! [`resolve_source_schema_in_txn`] call for the source side,
//! `Config::target_schema`/`intake::publication::qualify` for the target
//! side), never the bare spelling the grammar parsed. Every read of any of
//! these columns downstream must treat the value as already-qualified and
//! must not re-resolve it — see [`resolve_source_schema_in_txn`]'s own doc
//! comment for exactly why re-resolving a qualified value fails outright
//! rather than merely being redundant. A handful of read sites deliberately
//! stay bare regardless — `column_dependents`, `definition_by_target`, and
//! every `app.rs` read reachable through `docs/decisions/0003`'s
//! `transform.column` addressing scheme — because their callers only ever
//! have the bare name that addressing scheme itself accepts (issue #76
//! taught the *grammar* an explicit `schema.table` spelling for `TRANSFORM`/
//! `FROM`, but `transform.column` quarantine addressing is a separate,
//! unrelated syntax this module's own read sites still speak, and it hasn't
//! grown qualified-target syntax of its own) or because re-exposing the
//! qualified spelling through that addressing scheme would misparse a real
//! transform address as a column one (see each function's own doc comment);
//! these match
//! `target_table`'s bare table-name suffix via `split_part` rather than the
//! qualified column directly.
//!
//! **`schema_nodes`/`schema_edges` key on qualified identity too (issue
//! #74, ADR-0007).** `public.posts` and `archive.posts` are distinct nodes
//! with independent edges — `resolve_node_in_txn`/`resolve_node` and
//! `reject_if_table_cycle` all require an already-qualified `table_name`
//! now (see [`resolve_graph_identity_in_txn`]'s doc comment for how a bare
//! `def.source`/`def.from_table`/`def.to_table` gets there before either is
//! ever called). This reaches [`create_relationship`]'s two
//! `resolve_node_in_txn` calls as well as [`create_definition_inner`]'s,
//! even though a relationship endpoint's own *persisted* table name
//! (`relationship_definitions.from_table`/`to_table`) stays bare as written;
//! each endpoint has since gained its resolved schema alongside it
//! (`from_schema`, issue #285, part of a relationship's identity as of issue
//! #288; `to_schema`, issue #372), and every reader uses those rather than
//! re-resolving the bare name — since
//! both resolve into the one shared `schema_nodes` table a dual-role
//! (transform-and-relationship-endpoint) table must land on consistently
//! regardless of which grammar referenced it.
//!
//! **Bare target-table suffixes are still enforced globally unique, just no
//! longer by `target_table`'s own `unique` constraint.** Qualifying
//! `target_table` (#73) narrowed that constraint to the qualified spelling
//! only, which would otherwise let e.g. `public.foo` and `custom.foo`
//! coexist as two live definitions — exactly the ambiguity every
//! `split_part`-based bare-suffix read site above assumes can't happen.
//! `create_definition_inner` re-closes that gap itself, at write time,
//! rejecting a new definition with [`CatalogError::TargetTableSuffixCollision`]
//! if its qualified target would collide with another live definition's
//! bare suffix under a different schema — see that check's own comment for
//! why this stays provisional even now that issue #76's explicit
//! `schema.table` grammar (and issue #74's graph qualification) have
//! landed: every bare-suffix reader above is still bare-keyed, so the guard
//! still applies uniformly regardless of whether a colliding target arrived
//! via explicit qualification or bare resolution. This is now double-enforced,
//! not merely application-level: `transform_definitions_target_suffix_idx`
//! (`V23__transform_definitions_target_suffix_idx.sql`) is a real Postgres
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

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::fmt;

use crate::error_code::{self, ErrorCode};
use crate::float::FloatWidth;
use crate::integer::IntWidth;
use crate::pool::{Pool, quote_ident};
use tokio_postgres::GenericClient;

use super::ast::{
    AlterClause, AlterTransform, Expr, FieldDef, KeySpace, RelationshipDef, TransformDef,
    ValueType, render_definition_text,
};
use super::backfill::{self, BackfillError};
use super::ddl::{self, DdlError};
use super::error::ParseError;
#[cfg(any(test, feature = "internals"))]
use super::model::SchemaEdge;
use super::model::{
    Definition, EdgeKind, NodeKind, RelationshipCardinality, RelationshipDefinition, SchemaNode,
    TransformStatus,
};
use super::parser::{parse, parse_relationship};
use super::pg_type::PgType;
use super::validate::{
    RelationshipTypeMismatch, RelationshipWarning, ResolvedRelationship, ValidationError,
    infer_field_types, validate,
};

/// Why creating or reading a definition failed. [`CatalogError::code`]
/// reports a stable, coarse [`ErrorCode`] category for this error alongside
/// its `Display` message — see `docs/decisions/0008-public-api-design.md`, decision 3.
#[derive(Debug)]
pub enum CatalogError {
    /// The source text failed to parse (issue #22's grammar).
    Parse(ParseError),
    /// The parsed definition failed validation (issue #23).
    Validate(ValidationError),
    /// Acquiring a connection or running a query against Postgres failed.
    Db(tokio_postgres::Error),
    /// Acquiring a connection from the pool failed.
    Pool(crate::error::Error),
    /// A definition's persisted `source_columns` jsonb held a value other
    /// than `"numeric"`/`"text"`/`"boolean"`/`"uuid"` for some column —
    /// meaning the row was written by something other than
    /// [`create_definition`], since that's the only writer and it only ever
    /// encodes [`ValueType`]'s variants.
    UnknownValueType { column: String, text: String },
    /// The definition's initial backfill (issue #23) failed to enumerate its
    /// source table.
    Backfill(crate::intake::IntakeError),
    /// `def.source` doesn't resolve to any schema on this connection's
    /// search path (see [`resolve_source_schema_in_txn`]) — the table was
    /// dropped, renamed, or never existed under that bare name.
    SourceTableNotFound(String),
    /// This definition's resolved, qualified target (`{target_schema}.{def.target}`)
    /// shares a bare table-name suffix with a *different* qualified target
    /// some other still-persisted definition already uses — e.g.
    /// `public.foo` alongside `custom.foo`, most plausibly from
    /// `Config::target_schema` changing between deploys and a `TRANSFORM
    /// ... TARGET foo` being redeclared under it. Issue #73 made
    /// `transform_definitions.target_table` store the qualified spelling, so
    /// `target_table text not null unique` (`V2__transform_catalog.sql`)
    /// only enforces uniqueness of *that* spelling now, not the bare suffix
    /// it used to store outright — checked and rejected here, in
    /// `create_definition_inner`, rather than at any individual read site,
    /// because every `split_part(target_table, '.', 2)`-keyed reader
    /// (`definition_by_target`, `app.rs`'s status/quarantine polls,
    /// `generative`'s `unsettled_definitions`) assumes that suffix is
    /// globally unique and has no way to safely cope with two definitions
    /// colliding under it — see this variant's `Display` message for the
    /// operator-facing explanation. (`dependents_of` used to be bare-suffix-
    /// keyed here too, but issue #74 qualified its join — see that
    /// function's own doc comment — so it's no longer in this list.)
    ///
    /// Double-enforced as of the reviewer follow-up to issue #73: this
    /// application-level pre-check is still the primary path (it reports a
    /// clean, typed error naming both spellings — see
    /// `docs/decisions/0008-public-api-design.md` on why that beats a raw
    /// Postgres unique-violation for callers), but it only ever sees its own
    /// transaction's snapshot, so two concurrent `create_definition` calls
    /// resolving different-schema targets for the same bare suffix could
    /// each pass it and both commit. `transform_definitions_target_suffix_idx`
    /// (`V23__transform_definitions_target_suffix_idx.sql`) is the real
    /// DB-level guard that closes that race; `create_definition_inner`
    /// catches the rare insert-time unique-violation against it and
    /// translates it into this same variant (with `existing: None` — see the
    /// field's own doc comment) rather than letting it surface as a raw
    /// [`CatalogError::Db`].
    ///
    /// Still provisional even now that issue #76 has landed the grammar's
    /// explicit `schema.table` spelling: an operator *can* now write
    /// `TRANSFORM custom.foo FROM ...` to unambiguously address `custom.foo`
    /// as distinct from `public.foo`, but every bare-suffix reader named
    /// above is still bare-suffix-keyed pending issue #74's migration of the
    /// whole graph to qualified identity — so this variant is still raised
    /// for an explicitly-qualified target exactly as it is for a
    /// resolved-bare one (the check it backs has no branch on how the target
    /// was qualified, only on the final qualified string and bare suffix);
    /// only issue #74 (or a different addressing scheme) can relax this.
    ///
    /// Raised from two different places, both folding into this one variant
    /// since callers only need one type to match on: `create_definition_inner`'s
    /// pre-check (`existing: Some(_)`, the common case — the colliding row is
    /// still visible in this transaction's own snapshot, so its qualified
    /// spelling can be reported) and a genuine insert-time race against
    /// `transform_definitions_target_suffix_idx`
    /// (`V23__transform_definitions_target_suffix_idx.sql`, `existing: None`
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
    /// relation with that qualified name already exists (issue #440): a user
    /// table, view, materialized view, foreign table, or another Trellis
    /// definition's target. Trellis never adopts a table it didn't create —
    /// the user's schema belongs to the user (ADR-0005), and `DROP` drops a
    /// definition's target (ADR-0014), so adopting a user table would hand
    /// that table to a later `DROP`.
    TargetTableExists {
        /// The qualified `schema.table` spelling that already exists.
        table: String,
    },
    /// An aggregate (`GROUP BY`) definition (issue #47) was rejected because
    /// its source table's replica identity doesn't guarantee the old row
    /// image the delta-maintenance path (`apply_aggregate.rs`) needs on
    /// delete/update/re-parent. Wraps [`crate::intake::IntakeError`] — the
    /// same [`crate::intake::require_replica_identity_full`] check
    /// [`assert_replica_identity_supports_to_many`] mirrors for relationships
    /// (#41) — rather than [`CatalogError::Backfill`]'s blanket
    /// `From<IntakeError>`, since that variant's message ("failed to
    /// backfill...") would misdescribe a definition-time rejection as a
    /// backfill failure.
    ReplicaIdentityRequired(crate::intake::IntakeError),
    /// Issue #376: the definition's source would be read over logical
    /// replication, but intake can't key that table's changes (see
    /// [`crate::intake::change_keyed`]). Publishing it would either make
    /// Postgres refuse the table's own updates and deletes (no replica
    /// identity) or stop intake on its first change (`MissingKeyValue`).
    /// The case that motivated this is another Trellis instance's aggregate
    /// target: this instance's own targets are exempt, since their writes
    /// reach their readers in the writing transaction rather than over CDC.
    SourceNotChangeKeyed { source_table: String },
    /// Issue #375: a relationship endpoint that isn't one of this instance's
    /// targets fails [`crate::intake::change_keyed`]. The relationship walk in
    /// [`all_source_tables`] publishes it, so it is held to the same rule as a
    /// definition's source ([`CatalogError::SourceNotChangeKeyed`]); the case
    /// that motivated it is another instance's aggregate target.
    RelationshipEndpointNotChangeKeyed { endpoint: String },
    /// [`install_definition`]'s target-table DDL (run before either backfill
    /// path) failed.
    Ddl(DdlError),
    /// [`install_definition`]'s direct backfill attempt
    /// ([`backfill::backfill_definition`]) failed with something other than
    /// [`BackfillError::Unsupported`] — an `Unsupported` shape instead falls
    /// back to the ring ([`create_definition`]) rather than surfacing here.
    DirectBackfill(BackfillError),
    /// [`super::lifecycle::pause_transform`] was asked to pause a target with
    /// no corresponding `transform_definitions` row at all (issue #142). Its
    /// sibling verb, `drop`, deliberately treats the same situation as an
    /// idempotent success instead — see
    /// [`super::lifecycle::drop_transform`]'s doc comment for why the two
    /// differ.
    TransformNotFound { transform: String },
    /// [`super::lifecycle::drop_transform`] was asked to drop a definition
    /// that is not frozen (issue #142, ADR-0014). There is no direct
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
    /// (issue #142, ADR-0014's "Drops go in reverse dependency order — no
    /// cascade"). Trellis refuses rather than cascading, and names the
    /// blockers so the order to retire them in is explicit rather than
    /// something the operator has to reconstruct.
    DependentsBlockDrop {
        /// What the caller asked to drop: a bare transform name,
        /// `schema.from_table.relationship_name` for a relationship (its
        /// qualified address, issue #288, however the `DROP` spelled it), or
        /// `target.column` for an `ALTER TRANSFORM ... DROP <field>`
        /// (issue #241).
        subject: String,
        /// The bare target names of the live definitions standing in the way.
        dependents: Vec<String>,
        /// issue #242: for every blocker whose exact reference resolved to
        /// one specific column of the subject (column-granularity
        /// dependency edges — [`column_dependents`]), a human-readable
        /// `"<dependent>.<field> reads <subject>.<column>"` detail line.
        /// This is strictly additive precision over `dependents` alone, not
        /// a replacement — a bare transform-level blocker (e.g. the
        /// `RELATIONSHIP` declaration itself, which reads no column of its
        /// own) can appear in `dependents` with nothing here to say about
        /// it. Empty whenever no blocker's reference could be resolved this
        /// precisely.
        column_detail: Vec<String>,
    },
    /// [`alter_transform`] was asked to edit a target that isn't currently
    /// applying ([`TransformStatus::is_applying`]: `live`, or `catching_up`
    /// once its build has finished; ADR-0015) — most concretely, still
    /// `backfilling` behind its own initial build, or already frozen
    /// (`paused`/`quarantined`). Mirrors [`crate::staging::apply::ApplyError::DefinitionNotLive`]'s
    /// exact reasoning for [`crate::staging::quarantine::resume_column`]: a
    /// column-add's single-pass backfill takes one read of the source table,
    /// and a row a still-running initial-backfill chunk inserts into the
    /// target *during* that window would never be revisited — so an edit
    /// only ever starts from a settled, fully-built target.
    ///
    /// Also returned (issue #315) when a new definition's source is another
    /// definition's target whose build hasn't finished (one that isn't
    /// applying yet). A target's own initial build
    /// writes it outside the target-mutation seam
    /// (`staging::target_mutations`), so a reader attached mid-build would
    /// never hear about the rows the rest of the build writes. The same
    /// goes for a relationship naming such a target as an endpoint
    /// (issue #403), since the seam is that endpoint's only feed.
    TransformNotLive {
        transform: String,
        status: TransformStatus,
    },
    /// [`alter_transform`] was asked to `ALTER <field> AS <expr>` a field
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
    /// [`alter_transform`] was asked to `ADD <expr> AS <field>` a field name
    /// that already exists with a *different* formula than `<expr>` —
    /// distinct from the idempotent case (same formula, a no-op success):
    /// `ADD` only ever introduces a field, so re-adding an existing name
    /// under a new formula is ambiguous with `ALTER` and refused rather than
    /// silently reinterpreted as one.
    AlterFieldAlreadyExists { transform: String, field: String },
    /// `ALTER TRANSFORM` was asked to edit a target/expression shape outside
    /// this release's scope — see [`alter_transform`]'s own doc comment for
    /// exactly what's covered. Not a rejection of the definition's *shape* in
    /// the way [`CatalogError::Validate`] is (the edit would very likely be
    /// accepted by `define`); it's this operation's own, narrower coverage
    /// declining, with a message that says so.
    UnsupportedAlter(String),
    /// A bare `DROP RELATIONSHIP <from_table>.<name>` address matched a
    /// relationship on more than one same-named from-table — one per entry of
    /// `schemas` (issue #288: `blog.posts.author` and `shop.posts.author` may
    /// both exist). Refused rather than resolved through `search_path`; the
    /// caller qualifies the address to say which one it means. See
    /// [`relationship_at_address`].
    AmbiguousRelationshipAddress {
        from_table: String,
        name: String,
        schemas: Vec<String>,
    },
}

impl CatalogError {
    /// This error's stable, coarse [`ErrorCode`] category (`docs/decisions/0008-public-api-design.md`,
    /// decision 3). Delegates to the wrapped error's own `code()` wherever
    /// one nests here ([`CatalogError::Parse`], [`CatalogError::Validate`],
    /// [`CatalogError::Pool`], [`CatalogError::Backfill`],
    /// [`CatalogError::ReplicaIdentityRequired`], [`CatalogError::Ddl`],
    /// [`CatalogError::DirectBackfill`]) rather than hardcoding one category
    /// for a whole variant — so, for instance, a
    /// [`CatalogError::Validate`]`(`[`ValidationError::DuplicateRelationshipName`]`)`
    /// still reports [`ErrorCode::Conflict`], not [`ErrorCode::Validation`].
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
            // Collides with existing state (another live definition's
            // persisted target), not a structural/semantic rejection of this
            // definition's own text — the same category
            // `ValidationError::DuplicateRelationshipName` reports.
            CatalogError::TargetTableSuffixCollision { .. } => ErrorCode::Conflict,
            CatalogError::TargetTableExists { .. } => ErrorCode::Conflict,
            CatalogError::ReplicaIdentityRequired(err) => err.code(),
            CatalogError::SourceNotChangeKeyed { .. } => ErrorCode::Validation,
            CatalogError::RelationshipEndpointNotChangeKeyed { .. } => ErrorCode::Validation,
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
            CatalogError::AmbiguousRelationshipAddress { .. } => ErrorCode::Validation,
        }
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
            CatalogError::ReplicaIdentityRequired(err) => write!(f, "{err}"),
            CatalogError::SourceNotChangeKeyed { source_table } => write!(
                f,
                "Trellis can't key source table \"{source_table}\"'s changes from logical \
                 replication: it needs a primary key under REPLICA IDENTITY DEFAULT or FULL, or \
                 REPLICA IDENTITY USING INDEX. If it is another Trellis instance's aggregate \
                 target, define this transform in that instance instead: an instance propagates \
                 writes to its own targets without logical replication"
            ),
            CatalogError::RelationshipEndpointNotChangeKeyed { endpoint } => write!(
                f,
                "Trellis can't key relationship endpoint \"{endpoint}\"'s changes from logical \
                 replication: it needs a primary key under REPLICA IDENTITY DEFAULT or FULL, or \
                 REPLICA IDENTITY USING INDEX. If it is another Trellis instance's aggregate \
                 target, it can't be a relationship endpoint here"
            ),
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
                 edited with ALTER TRANSFORM, read by another transform, or made a \
                 relationship endpoint",
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
            CatalogError::AmbiguousRelationshipAddress {
                from_table,
                name,
                schemas,
            } => write!(
                f,
                "relationship address '{from_table}.{name}' is ambiguous: a table named \
                 '{from_table}' declares '{name}' in each of these schemas: {}; qualify it as \
                 <schema>.{from_table}.{name}",
                schemas.join(", ")
            ),
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
            CatalogError::TargetTableSuffixCollision { .. } => None,
            CatalogError::TargetTableExists { .. } => None,
            CatalogError::ReplicaIdentityRequired(err) => Some(err),
            CatalogError::SourceNotChangeKeyed { .. } => None,
            CatalogError::RelationshipEndpointNotChangeKeyed { .. } => None,
            CatalogError::Ddl(err) => Some(err),
            CatalogError::DirectBackfill(err) => Some(err),
            CatalogError::TransformNotFound { .. } => None,
            CatalogError::TransformNotPaused { .. } => None,
            CatalogError::DependentsBlockDrop { .. } => None,
            CatalogError::TransformNotLive { .. } => None,
            CatalogError::AlterFieldNotFound { .. } => None,
            CatalogError::AlterFieldAlreadyExists { .. } => None,
            CatalogError::UnsupportedAlter(_) => None,
            CatalogError::AmbiguousRelationshipAddress { .. } => None,
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
/// in for the backfill discharge's ring fallback
/// (`intake::publication::run_pending_backfills`, ADR-0016) in-call: it
/// enumerates the source into the ring as image-less `Recompute` rows and
/// flips the definition `live` in one transaction. `source_columns` maps the
/// source table's known columns to their [`ValueType`] (see
/// [`super::validate::validate`]).
///
/// No production path calls this: registration never reads the source (issue
/// #418). It skips everything the discharge waits on (the marker's fence and
/// intake's progress), so the caller must be the only writer of the source.
/// The target table must already exist (see this module's doc comment).
///
/// `pool.target_schema()`, not a parameter of this function's own (issue
/// #73): this entry point never runs target-table DDL itself, so there is no
/// sibling DDL call for a separately-threaded `target_schema` argument to
/// drift from. See [`crate::pool::Pool::target_schema`]'s own doc comment.
#[cfg(any(test, feature = "internals"))]
pub async fn create_definition(
    pool: &Pool,
    source_text: &str,
    source_columns: &HashMap<String, ValueType>,
) -> Result<Definition, CatalogError> {
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
    crate::intake::publication::enumerate_and_append(&txn, &definition.source_table).await?;
    txn.execute(
        "update transform_definitions set status = $1 where id = $2",
        &[&TransformStatus::Live.as_str(), &definition.id],
    )
    .await?;
    txn.commit().await?;
    definition.status = TransformStatus::Live;
    // Issue #315: a source that is another definition's target is never
    // published, and its writer's target-mutation seam only reached this
    // definition once it was applying. The discharge fences the marker when
    // it reads it (issue #431), after this commit, so the fence waits out
    // every such writer, and flips the definition `live` (issue #476).
    if is_definition_target(&**client, &definition.source_table).await? {
        crate::intake::publication::park_catch_up(
            &**client,
            &[definition.id],
            std::slice::from_ref(&definition.source_table),
        )
        .await?;
        definition.status = TransformStatus::CatchingUp;
    }
    Ok(definition)
}

/// Like [`create_definition`], but stages *no* ring-enumeration backfill: the
/// definition and its version bump are persisted `live`, but the source table
/// is not enumerated into the ring. Callers that build the target directly
/// (`defs::backfill::backfill_definition` — issue #63 M3's set-based,
/// key-range-chunked source→target build) use this so the from-scratch build
/// doesn't *also* flood the ring with one `Recompute` marker per source row;
/// the ring is then left to handle only live CDC deltas after the direct
/// build's fence.
#[cfg(any(test, feature = "test-util"))]
pub async fn create_definition_without_backfill(
    pool: &Pool,
    source_text: &str,
    source_columns: &HashMap<String, ValueType>,
) -> Result<Definition, CatalogError> {
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

/// The front door real callers use to stand up a new definition (issue #63
/// C1): validates it, creates the target table, and records the definition.
///
/// **The target table is created in the same transaction that records the
/// definition (issue #440),** so a registration that fails anywhere leaves
/// neither a catalog row nor a table behind and can simply be retried. A
/// relation that already exists under the target's qualified name, of any
/// kind, is refused with [`CatalogError::TargetTableExists`] and left
/// untouched: Trellis never adopts a table it didn't create (ADR-0005), since
/// `DROP` would later drop it (ADR-0014). No build path creates the target
/// itself.
///
/// **Registration reads no source rows (ADR-0016, issues #418, #419).** Every
/// definition is persisted [`TransformStatus::WaitingToBackfill`] and this
/// returns, so its latency doesn't depend on the size of any table the
/// definition reads. Its build runs in the background: the staging worker's
/// reconcile pass parks a `pending_backfill` marker on its source (joining
/// the source to the publication first if it has to; see
/// [`crate::intake::publication::reconcile_publication`]), and that marker's
/// discharge dispatches the build by shape once the marker's fence has
/// settled. A plain 1-1 definition gets `backfill_chunks`, an aggregate or
/// relationship-enriched 1-1 definition one direct-build job
/// (`chunk_queue::dispatch_direct_build`); drain threads execute either, and
/// the last one to finish moves it to `catching_up`
/// ([`complete_direct_backfill`]), and the discharge of the go-live catch-up
/// that parks flips it `live` (issue #476). A shape the direct build can't
/// render ([`BackfillError::Unsupported`]) gets a ring enumeration flipped
/// `live` in the discharge's own transaction. A caller polls
/// [`crate::Trellis::status`] until it reads `live`.
///
/// **Why nothing is folded into a target still being built.**
/// [`dependents_of`]/[`transforms_for_source`] filter to applying
/// definitions ([`TransformStatus::is_applying`]), so no build path can have
/// a live CDC delta folded into it while it is still being built. A delta
/// skipped during that window is recovered rather than lost by the catch-up
/// marker parked when the build finishes: its discharge re-reads every
/// table the build read, and deletes the target rows no source row backs
/// any more, which the re-read can't reach (issue #485).
/// The reverse, a delta for a commit the build already read that drains only
/// after the flip, is harmless for a 1-1 target. For an aggregate, the
/// recompute horizon the build stamps on every group row it writes (and on
/// the target, for the groups it found empty) sends that delta to
/// re-derive its group rather than count the commit a second time (issues
/// #419, #442), so that correction doesn't rest on the catch-up.
pub async fn install_definition(
    pool: &Pool,
    source_text: &str,
    source_columns: &HashMap<String, ValueType>,
    target_schema: &str,
) -> Result<Definition, CatalogError> {
    let def: TransformDef = parse(source_text)?;

    // Validate *before* any DDL is generated or executed, for every key-space
    // (issue #94). DDL generation type-infers each target column from the same
    // expressions the validator checks, so an invalid definition reaching DDL
    // first surfaces whatever incidental error inference happens to hit —
    // masking the real validation error — and, worse, can leave a target table
    // behind for a definition that is then rejected. `create_definition`/
    // `create_definition_without_backfill` validate again below; that repeat is
    // cheap and keeps those entry points safe when called directly.
    let relationships = resolve_relationships_for_new_definition(pool, &def).await?;
    validate(&def, source_columns, &relationships)?;

    // Issue #76 / ADR-0007 grammar clause 4: an explicit schema on either
    // side is trusted outright rather than resolved — checked here, before
    // any DDL work runs, so a bogus explicit spelling fails fast with a
    // friendly `ValidationError` instead of surfacing as a confusing DDL
    // failure partway through this function.
    // `create_definition_inner` (below) repeats both checks inside its own
    // transaction; that repeat is the *authoritative* one — it's the only
    // check the ring-path entry points ([`create_definition`]/
    // [`create_definition_without_backfill`], which never call this
    // function) ever run. This one is a pure fail-fast nicety for the far
    // more common `install_definition` path, redundant-but-harmless on the
    // path that also reaches `create_definition_inner`.
    if let Some(schema) = &def.explicit_source_schema
        && !confirm_qualified_table_exists(pool, schema, &def.source).await?
    {
        return Err(ValidationError::QualifiedSourceTableNotFound {
            schema: schema.clone(),
            table: def.source.clone(),
        }
        .into());
    }

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
    // exists (issue #440) inside the transaction that creates it.
    let target_schema = effective_target_schema(&def, target_schema);

    // Issue #76, ADR-0007: resolved once, here, and threaded through every
    // DDL/direct-build step below — see [`resolve_source_for_install`]'s own
    // doc comment for why this function needs its own copy rather than
    // waiting for `create_definition_inner`'s later, authoritative one.
    let qualified_source = resolve_source_for_install(pool, &def).await?;
    // Issue #315: fail fast, before any DDL, on a source that is another
    // definition's not-yet-live target (see `reject_non_live_upstream`).
    reject_non_live_upstream(&**pool.get().await?, &qualified_source).await?;

    // Issue #376: `reject_unkeyed_source` fails fast in both arms, before the
    // target's DDL is rendered. `create_definition_inner` repeats it as the
    // authoritative check.
    //
    // Only *rendered* here: rendering reads relationship metadata and source
    // column types through `pool`, but the statement itself runs inside
    // `create_definition_inner`'s transaction (issue #440), so the table
    // commits or rolls back together with the catalog row.
    let target_ddl = match &def.key_space {
        KeySpace::OneToOne => {
            // Issue #121: the 1-1 target table's own primary key mirrors the
            // source's in full — at whatever arity the source declares it —
            // rather than narrowing to a single column. `target_table_ddl`
            // renders a composite `pk` as a real, multi-column `primary key
            // (...)` constraint.
            let pk = ddl::source_primary_key(pool, &qualified_source)
                .await
                .map_err(CatalogError::Ddl)?;
            reject_unkeyed_source(&**pool.get().await?, &qualified_source).await?;
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
            // Issue #371: live apply reads the source's primary key through
            // `ddl::source_primary_key` for an aggregate too, and a key type
            // it refuses halts the instance there. Refuse it here instead,
            // before any DDL, exactly as the 1-1 arm above does.
            ddl::source_primary_key(pool, &qualified_source)
                .await
                .map_err(CatalogError::Ddl)?;
            reject_unkeyed_source(&**pool.get().await?, &qualified_source).await?;
            ddl::aggregate_target_table_ddl(pool, &def, target_schema, source_columns)
                .await
                .map_err(CatalogError::Ddl)?
        }
    };

    // ADR-0016 (issues #418, #419): registration reads no source rows. The
    // definition waits for the backfill discharge, which dispatches its build
    // by shape.
    //
    // `target_schema` — the exact value the DDL above was rendered under — is
    // threaded straight through rather than re-derived from `pool` (issue
    // #73's persisted qualification must never drift from what the DDL
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

/// What [`catch_up_if_backfilling`] did to a definition whose build finished.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BuildFinished {
    /// It was still `backfilling`, and is now `catching_up`.
    CatchingUp,
    /// It had already left `backfilling` (paused, quarantined, or resumed back
    /// to `waiting_to_backfill`), and was left in this status untouched.
    LeftAs(TransformStatus),
}

impl BuildFinished {
    /// The status the definition is in afterward.
    fn status(self) -> TransformStatus {
        match self {
            BuildFinished::CatchingUp => TransformStatus::CatchingUp,
            BuildFinished::LeftAs(status) => status,
        }
    }
}

/// Moves an already-persisted definition row `backfilling` -> `catching_up`
/// once its build finishes (issues #55, #476) — but only if it is *still*
/// `backfilling` (issue #331). A build runs unlocked against the definition
/// row for as long as it takes, and an operator pause, the poison fuse, or a
/// resume can move the row on meanwhile; an unconditional flip would
/// silently undo that. A definition that moved on is left where it is:
/// whatever moved it owns its way back to `live` (a frozen definition's
/// resume rebuilds it by a fresh backfill; a `waiting_to_backfill` one
/// already sits behind a marker).
async fn catch_up_if_backfilling(
    client: &impl GenericClient,
    id: i64,
) -> Result<BuildFinished, CatalogError> {
    let moved = client
        .execute(
            "update transform_definitions set status = $1 where id = $2 and status = $3",
            &[
                &TransformStatus::CatchingUp.as_str(),
                &id,
                &TransformStatus::Backfilling.as_str(),
            ],
        )
        .await?;
    if moved == 1 {
        tracing::info!(
            definition_id = id,
            from = %TransformStatus::Backfilling.as_str(),
            to = %TransformStatus::CatchingUp.as_str(),
            "transform status transition: backfill build finished"
        );
        return Ok(BuildFinished::CatchingUp);
    }
    let status_text: String = client
        .query_one(
            "select status from transform_definitions where id = $1",
            &[&id],
        )
        .await?
        .get(0);
    let status = TransformStatus::from_persisted(&status_text).unwrap_or_else(|| {
        panic!("transform_definitions.status held unrecognized value '{status_text}'")
    });
    tracing::info!(
        definition_id = id,
        status = %status.as_str(),
        "backfill finished, but the definition is no longer backfilling; \
         leaving its status as it is"
    );
    Ok(BuildFinished::LeftAs(status))
}

/// Moves `definition_id` from [`TransformStatus::Backfilling`] to
/// [`TransformStatus::CatchingUp`] and parks its go-live catch-up markers —
/// the "every chunk done" completion event `chunk_queue::finish_chunk` calls
/// once every `backfill_chunks` row for `definition_id` is done
/// (docs/decisions/0007's amendment). Runs inside the caller's transaction,
/// which must already hold a `for update` lock on `definition_id`'s
/// `transform_definitions` row (see `finish_chunk`) — that lock is what
/// makes two workers finishing different chunks of the same definition
/// near-simultaneously unable to race this completion in either direction
/// (both completing it, or neither).
///
/// The parked marker (reusing the exact `pending_backfill` mechanism the
/// ring-fallback path already relies on — see
/// [`crate::intake::publication::park_catch_up`]) is what makes excluding a
/// definition still being built from [`dependents_of`]/[`transforms_for_source`]
/// safe rather than lossy: any CDC delta for this source table that arrived
/// while this definition sat `backfilling` was never folded into its target
/// (the exclusion), but this marker's later discharge re-derives the target
/// from current source state, folding that delta in after all.
///
/// **Not `live` yet (issue #476).** Until that discharge has run, the target
/// may be missing those deltas, and a watermark token wouldn't wait for
/// them, so the definition reports `catching_up`: applied exactly as a
/// `live` one is, flipped `live` by the discharge of its last catch-up
/// (`intake::publication::go_live_caught_up`). ADR-0016, "What `live`
/// promises".
///
/// Completes every build that runs through `backfill_chunks`: a plain 1-1
/// definition's chunks, and an aggregate or relationship-enriched 1-1
/// definition's direct-build job (issue #419). The catch-up is parked on
/// every table the build read: the source, and each relationship to-side
/// table (issue #430).
///
/// **Only a still-`backfilling` definition completes (issue #331).** The
/// pause gates new chunk claims, not a chunk a worker already holds, so the
/// last chunk can finish after the definition has been paused or quarantined.
/// [`catch_up_if_backfilling`] leaves a definition that has moved on exactly
/// as it is; the chunk itself is still retired by the caller. A chunk held
/// across a *resume* never reaches here: `finish_chunk` discards it instead,
/// since the resumed definition's rebuild owns its way back to `live`
/// (issues #360/#397).
///
/// The catch-up marker is parked unless the definition is **frozen**. A
/// frozen one gets nothing from it: resume
/// ([`crate::staging::quarantine::resume_transform`]) re-parks a marker of
/// its own and rebuilds by a fresh backfill.
///
/// Returns the status the definition is left in:
/// [`TransformStatus::CatchingUp`] when this call completed it or it was
/// already applying, its unchanged current status otherwise.
pub(crate) async fn complete_direct_backfill(
    txn: &tokio_postgres::Transaction<'_>,
    definition_id: i64,
) -> Result<TransformStatus, CatalogError> {
    // Issue #72 / ADR-0007: `transform_definitions.source_table` is already
    // the fully-qualified `schema.table` identity persisted at
    // definition-acceptance time (`create_definition_inner`) — read it back
    // and use it as-is. It must *not* be re-resolved through
    // [`resolve_source_schema_in_txn`] a second time here: that function
    // matches `information_schema.tables.table_name` (a bare name) exactly,
    // so handing it an already-qualified `"schema.table"` string would never
    // match anything and this would fail every time with
    // [`CatalogError::SourceTableNotFound`].
    let status = catch_up_if_backfilling(txn, definition_id).await?.status();
    if status == TransformStatus::CatchingUp {
        // Issue #315: the chunks wrote this target outside the seam; a
        // reader already attached to it (a resumed upstream's) re-derives.
        crate::intake::publication::park_target_catchup_if_read(txn, definition_id).await?;
    }
    if status.is_frozen() {
        return Ok(status);
    }
    let tables = definition_tables_read(txn, definition_id).await?;
    crate::intake::publication::park_catch_up(txn, &[definition_id], &tables).await?;
    // The park moves a `live` definition (one a leftover chunk wrote under)
    // to `catching_up` too.
    Ok(if status.is_applying() {
        TransformStatus::CatchingUp
    } else {
        status
    })
}

/// [`tables_read_by`] for the persisted definition `definition_id`.
async fn definition_tables_read(
    txn: &tokio_postgres::Transaction<'_>,
    definition_id: i64,
) -> Result<Vec<String>, CatalogError> {
    let row = txn
        .query_one(
            "select source_table, definition_text from transform_definitions where id = $1",
            &[&definition_id],
        )
        .await?;
    let qualified: String = row.get(0);
    let def = parse(row.get::<_, &str>(1))?;
    tables_read_by(txn, &def, &qualified).await
}

/// Every `catching_up` definition that reads `table`, locked `for update`,
/// with every table it reads ([`tables_read_by`]): the definitions whose
/// go-live catch-up the discharge of `table`'s marker may be the last of
/// (`intake::publication::go_live_caught_up`, issue #476). The definitions
/// are read unlocked first ([`catching_up_readers_unlocked`]) and only the
/// ones reading `table` are locked, in id order; one that left `catching_up`
/// while this waited for its lock is dropped.
pub(crate) async fn catching_up_readers(
    txn: &tokio_postgres::Transaction<'_>,
    table: &str,
) -> Result<Vec<(i64, Vec<String>)>, CatalogError> {
    let mut readers = catching_up_readers_unlocked(txn, table).await?;
    if readers.is_empty() {
        return Ok(readers);
    }
    let ids: Vec<i64> = readers.iter().map(|(id, _)| *id).collect();
    let still: Vec<i64> = txn
        .query(
            "select id from transform_definitions where id = any($1) and status = $2 \
             order by id for update",
            &[&ids, &TransformStatus::CatchingUp.as_str()],
        )
        .await?
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    readers.retain(|(id, _)| still.contains(id));
    Ok(readers)
}

/// [`catching_up_readers`] without the locks: every definition `catching_up`
/// as of this statement that reads `table`, with every table it reads.
pub(crate) async fn catching_up_readers_unlocked(
    txn: &tokio_postgres::Transaction<'_>,
    table: &str,
) -> Result<Vec<(i64, Vec<String>)>, CatalogError> {
    let rows = txn
        .query(
            "select id, source_table, definition_text from transform_definitions \
             where status = $1 order by id",
            &[&TransformStatus::CatchingUp.as_str()],
        )
        .await?;
    let mut readers = Vec::new();
    for row in rows {
        let qualified: String = row.get(1);
        let def = parse(row.get::<_, &str>(2))?;
        let tables = tables_read_by(txn, &def, &qualified).await?;
        if tables.iter().any(|t| t == table) {
            readers.push((row.get::<_, i64>(0), tables));
        }
    }
    Ok(readers)
}

/// Every applying definition (`live` or `catching_up`) that reads `table`,
/// directly as its source or through a relationship whose to-side it is
/// ([`tables_read_by`]), in id order: the readers a rewrite of `table`
/// outside the target-mutation seam has to catch up (issue #507,
/// `intake::publication::park_target_catchup_if_read`). A relationship
/// consumer counts only if one of its fields reads through the relationship,
/// the same rule the catch-up discharge's go-live check applies
/// ([`catching_up_readers`]).
pub(crate) async fn applying_readers(
    client: &impl GenericClient,
    table: &str,
) -> Result<Vec<i64>, CatalogError> {
    let rows = client
        .query(
            "select id, source_table, definition_text from transform_definitions \
             where status = any($1) order by id",
            &[&TransformStatus::applying()],
        )
        .await?;
    let mut readers = Vec::new();
    for row in rows {
        let qualified: String = row.get(1);
        let def = parse(row.get::<_, &str>(2))?;
        if tables_read_by(client, &def, &qualified)
            .await?
            .iter()
            .any(|t| t == table)
        {
            readers.push(row.get::<_, i64>(0));
        }
    }
    Ok(readers)
}

/// Every table `def`'s direct build reads, fully qualified, sorted and
/// deduplicated: its source (`qualified_source`, the persisted
/// `transform_definitions.source_table`) and the to-side table of every
/// relationship its fields reference, as recorded at declaration time
/// ([`RelationshipDefinition::qualified_to_table`], issue #372). A referenced
/// relationship the source no longer declares contributes nothing.
async fn tables_read_by(
    txn: &impl GenericClient,
    def: &TransformDef,
    qualified_source: &str,
) -> Result<Vec<String>, CatalogError> {
    let mut tables = vec![qualified_source.to_string()];
    if let Some((schema, table)) = qualified_source.split_once('.') {
        for rel in referenced_relationships(def) {
            if let Some(reldef) = relationship_by_name_in(txn, schema, table, &rel).await? {
                tables.push(reldef.qualified_to_table());
            }
        }
    }
    tables.sort_unstable();
    tables.dedup();
    Ok(tables)
}

/// The names of the relationships `def`'s fields read through, sorted and
/// deduplicated.
fn referenced_relationships(def: &TransformDef) -> Vec<String> {
    let mut rels: Vec<String> = super::eval::relationship_references(def)
        .into_iter()
        .map(|(rel, _column)| rel)
        .collect();
    rels.sort_unstable();
    rels.dedup();
    rels
}

/// Every definition's status as an operator reads it (`Trellis::status`,
/// `Trellis::definitions`), by id (issue #497, ADR-0016's "What `live`
/// promises"): its persisted status, except that a `live` definition reading
/// an upstream that isn't `live` reports [`TransformStatus::CatchingUp`].
///
/// An upstream is the definition whose target is a table this one reads:
/// its source, or the to-side of a relationship one of its fields reads
/// through ([`tables_read_by`]'s set). While that upstream is paused,
/// quarantined, rebuilding or catching up, its target doesn't reflect its
/// own source, so neither does anything derived from it, and a watermark
/// token wouldn't wait for what's missing. The rule is transitive: an
/// upstream that only *reports* `catching_up` because of its own upstream
/// counts as not `live` too.
///
/// Derived when read, not stored. A stored transition would have to follow
/// every status change of every upstream (pause, resume, quarantine, drop,
/// each go-live) down the chain, under locks, and a reader's own go-live
/// would have to check its upstreams as well. The derivation needs only the
/// catalog: one read of the definitions and one of the relationships. The
/// reported status changes nothing else: the definition keeps applying, as
/// `catching_up` always does.
pub(crate) async fn reported_statuses(
    client: &impl GenericClient,
) -> Result<HashMap<i64, TransformStatus>, CatalogError> {
    let relationships: HashMap<(String, String, String), String> = client
        .query(
            "select from_schema, from_table, name, to_schema || '.' || to_table \
             from relationship_definitions",
            &[],
        )
        .await?
        .into_iter()
        .map(|row| ((row.get(0), row.get(1), row.get(2)), row.get(3)))
        .collect();
    let rows = client
        .query(
            "select id, target_table, source_table, status, definition_text \
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
            reads.push(source.clone());
            if let Some((schema, table)) = source.split_once('.') {
                let def = parse(row.get::<_, &str>(4))?;
                for rel in referenced_relationships(&def) {
                    let key = (schema.to_string(), table.to_string(), rel);
                    if let Some(to_table) = relationships.get(&key) {
                        reads.push(to_table.clone());
                    }
                }
            }
        }
        nodes.push(StatusNode {
            id: row.get(0),
            target: row.get(1),
            status,
            reads,
        });
    }
    Ok(derive_reported_statuses(&nodes))
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
}

/// [`reported_statuses`]' rule over the loaded catalog: starting from every
/// persisted status, moves a `live` definition to `catching_up` while any
/// table it reads is the target of a definition not reported `live`, until
/// nothing moves. Each round only ever moves `live` to `catching_up`, so it
/// ends within one round per definition, and a cycle (which the catalog
/// can't hold anyway) can't make it loop.
fn derive_reported_statuses(nodes: &[StatusNode]) -> HashMap<i64, TransformStatus> {
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
            if upstream_not_live {
                reported.insert(node.id, TransformStatus::CatchingUp);
                moved = true;
            }
        }
        if !moved {
            return reported;
        }
    }
}

/// Refuses (with [`CatalogError::TransformNotLive`]) a new definition whose
/// source, `qualified_source`, is the target of another definition whose
/// build hasn't finished yet (issue #315): one that isn't
/// [`TransformStatus::is_applying`]. And likewise a
/// relationship endpoint that is such a target ([`create_relationship`],
/// issue #403). A target's initial build
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
    // target goes through the seam now (issue #476).
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
/// `ALTER TRANSFORM`'s catalog-side implementation (ADR-0015, issues
/// #241/#242), reached through [`crate::Trellis::apply`] like every other
/// definition-changing statement (issue #227).
///
/// **Scope, this release.** Only a [`KeySpace::OneToOne`] target with no
/// relationship-enriched field, before or after the edit, is supported:
/// `ADD`/`ALTER` clauses whose merged field list would read any relationship
/// path, and any edit at all against a [`KeySpace::Aggregate`] target, are
/// refused with [`CatalogError::UnsupportedAlter`]. `DROP` alone never needs
/// this restriction in principle (it recomputes nothing), but is held to the
/// same gate for simplicity in this first pass — a deliberate, flagged
/// scope-down, not an oversight: the single-pass backfill this reuses
/// ([`backfill::backfill_altered_columns`], built on
/// [`backfill::backfill_one_to_one`]'s chunked writer) only renders a plain
/// `INSERT ... SELECT ... FROM <source>` and has no join-aware counterpart
/// the way [`backfill::backfill_relationship_one_to_one`] is for a first
/// `define`; wiring column-scoped writes through that relationship-aware
/// path, and building an aggregate-target column-add/drop path (which would
/// need to reason about the hidden `__{f}_sum`/`__{f}_count` partials
/// `ddl::create_aggregate_target_table` maintains), are both real follow-up
/// work, not fundamental blockers.
///
/// **Idempotency, per clause** (ADR-0015's "Edits are idempotent"): re-`ADD`ing
/// a field that already exists with the exact same formula, re-`ALTER`ing a
/// field to the formula it already has, and `DROP`ping a field already
/// absent are each a no-op. If *every* clause in the statement turns out to
/// be one of these, the whole call is a no-op success — no version bump, no
/// DDL, nothing written — matching pause/resume/drop's own "a replayed
/// migration must be safe in both directions" discipline.
///
/// **Single-pass backfill.** Every `ADD`/`ALTER`ed field this call actually
/// changes is populated by *one* enumeration of the source
/// ([`backfill::backfill_altered_columns`]), never a backfill per column —
/// the same single-pass contract [`install_definition`]'s own initial build
/// already honors, reused rather than reimplemented.
///
/// **The pause-state reuse.** While a changed field's single-pass backfill
/// runs, it is parked in `column_status` — the exact mechanism
/// `staging::quarantine`'s operator-driven column pause already is, "the
/// column-granularity form of the pause state a target already has" in the
/// ADR's own words: live CDC apply already excludes any `column_status`-listed
/// column from what it computes/writes (`staging::apply`'s
/// `paused_columns_for`/per-batch plan construction), so the field holds no
/// committed value and nothing but this call's own backfill touches it until
/// the backfill completes and this call clears the row.
///
/// **The catch-up (issue #305).** Clearing that row is not the end of it: a
/// source row changed after the backfill read it, with its delta applied
/// while the field was still paused, would otherwise keep a stale value for
/// the field forever. The unpause therefore parks a `pending_backfill`
/// catch-up marker for the source table in the same transaction — the exact
/// closed loop [`complete_direct_backfill`] ends a first `define`'s chunked
/// build with, whose discharge re-derives every row once the marker's fence
/// settles.
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
/// *new* field list, with the just-edited columns' `column_status` rows
/// already visible — rather than racing this call's own backfill with a
/// half-populated column. This is the same fence every *first*
/// `create_definition_inner` already bumps, for exactly this reason; nothing
/// new is invented here, only reused for an edit instead of only a define.
///
/// **Cycle detection.** The merged field list (existing fields, with `ADD`/
/// `DROP`/`ALTER` applied) is run through [`validate`] — the same validator,
/// same `super::validate::detect_cycle` call, [`create_definition_inner`]
/// runs for a first `define` — before anything is written. The table-level
/// whole-graph check ([`reject_if_table_cycle`]) is deliberately *not*
/// re-run: an edit changes neither `def.source`, `def.target`, nor the set of
/// relationships a table-level edge exists for (a relationship a new field
/// reads was already declared, with its own edge, before this edit could
/// reference it), so the table-level graph this call would check against is
/// unchanged from the one already checked when the target was first defined
/// — re-running it would only re-confirm the same edge is still not a cycle.
///
/// **The `DROP <field>` refusal.** Requires column-granularity dependency
/// edges finer than a whole-transform drop's table-level check — exactly
/// issue #242's gap, closed once via [`column_dependents_any_keyspace`] and
/// used here as the *entire* correctness check (not just a naming nicety the
/// way [`super::lifecycle::drop_transform`]'s own use of it is): a dropped
/// field refuses, naming the specific reader, iff some other definition's
/// field or `GROUP BY` key resolves to exactly this `(target, field)` pair.
pub async fn alter_transform(
    pool: &Pool,
    alter: &AlterTransform,
) -> Result<AlterOutcome, CatalogError> {
    let Some(current) = definition_by_target(pool, &alter.target).await? else {
        return Err(CatalogError::TransformNotFound {
            transform: alter.target.clone(),
        });
    };
    if !current.status.is_applying() {
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

    let mut fields = current.def.fields.clone();
    let mut added = Vec::new();
    let mut altered = Vec::new();
    let mut dropped = Vec::new();
    let mut real_adds: Vec<FieldDef> = Vec::new();
    let mut real_alters: Vec<FieldDef> = Vec::new();
    let mut real_drops: Vec<String> = Vec::new();

    for clause in &alter.clauses {
        match clause {
            AlterClause::Add(new_field) => {
                match fields.iter().find(|f| f.name == new_field.name) {
                    Some(existing) if existing.expr == new_field.expr => {
                        // Idempotent no-op: this field already exists with
                        // this exact formula.
                    }
                    Some(_) => {
                        return Err(CatalogError::AlterFieldAlreadyExists {
                            transform: alter.target.clone(),
                            field: new_field.name.clone(),
                        });
                    }
                    None => {
                        fields.push(new_field.clone());
                        added.push(new_field.name.clone());
                        real_adds.push(new_field.clone());
                    }
                }
            }
            AlterClause::Alter(new_field) => {
                match fields.iter().position(|f| f.name == new_field.name) {
                    None => {
                        return Err(CatalogError::AlterFieldNotFound {
                            transform: alter.target.clone(),
                            field: new_field.name.clone(),
                            declared: fields.iter().map(|f| f.name.clone()).collect(),
                        });
                    }
                    Some(idx) if fields[idx].expr == new_field.expr => {
                        // Idempotent no-op: already this exact formula.
                    }
                    Some(idx) => {
                        fields[idx] = new_field.clone();
                        altered.push(new_field.name.clone());
                        real_alters.push(new_field.clone());
                    }
                }
            }
            AlterClause::Drop(name) => match fields.iter().position(|f| &f.name == name) {
                None => {
                    // Idempotent no-op: already absent.
                }
                Some(idx) => {
                    fields.remove(idx);
                    dropped.push(name.clone());
                    real_drops.push(name.clone());
                }
            },
        }
    }

    if real_adds.is_empty() && real_alters.is_empty() && real_drops.is_empty() {
        // Every clause was an idempotent no-op — ADR-0015's "edits are
        // idempotent in both directions": no version bump, no DDL, nothing
        // written.
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

    if (!real_adds.is_empty() || !real_alters.is_empty()) && backfill::uses_relationships(&merged) {
        return Err(CatalogError::UnsupportedAlter(format!(
            "'{}' would read a relationship path after this edit; ALTER TRANSFORM's ADD/ALTER \
             clauses only support relationship-free 1-1 transforms in this release",
            alter.target
        )));
    }

    // Reuse the exact validator (and, inside it, the exact column-cycle
    // detector) a first `define` runs — see this function's own doc comment
    // on why the table-level whole-graph check is deliberately not re-run.
    let relationships = resolve_relationships(pool, &merged, &current.source_table).await?;
    validate(&merged, &current.source_columns, &relationships)?;
    let field_types = infer_field_types(&merged, &current.source_columns, &relationships)?;

    // Pre-transaction dependency check for every real `DROP` — issue #241's
    // column-granularity refusal, using #242's same infrastructure.
    // Best-effort here (re-checked for real under the row lock below, issue
    // #231's discipline — a definition can register a brand-new dependent
    // between this check and the transaction's commit).
    for field in &real_drops {
        check_no_column_dependents(pool, &alter.target, field).await?;
    }

    // Pre-transaction type-change refusal for every real `ALTER` — the
    // second review round on issue #241/#242: an `ALTER <field> AS <expr>`
    // whose new formula infers a *different* [`ValueType`] than the field's
    // current physical column cannot be applied as ADR-0015's "an edit
    // never rewrites the whole target table to change one column" promises
    // — Postgres can only skip the table rewrite when the old and new
    // column share the same physical representation, and this crate's own
    // [`ValueType`] (every target column is declared from exactly one via
    // [`ddl::pg_type_name`]) *is* that representation, so two different
    // `ValueType`s always mean Postgres would have to rewrite the entire
    // heap under an `ACCESS EXCLUSIVE` lock — blocking reads and writes to
    // every other column too, not just this one. Refusing here, rather than
    // building the shadow-column/backfill/rename-swap fix that would avoid
    // it, is a deliberate v1 scope-down (tracked separately); `DROP`+`ADD`
    // already pays this same backfill cost today, so it remains the
    // supported path for a genuine type change. Best-effort here, same as
    // the `DROP` dependents check just above (re-checked for real under the
    // row lock below, issue #231's discipline).
    for field in &real_alters {
        check_no_type_changing_alter(
            pool,
            &current.target_table,
            &alter.target,
            field,
            &field_types,
        )
        .await?;
    }

    let (target_schema, _) = current
        .target_table
        .split_once('.')
        .expect("target_table is always schema-qualified (issue #73)");
    let target_ident = ddl::qualified_target_table_ident(&current.target_table);

    let mut client = pool.get().await?;
    let txn = client.transaction().await?;

    // Row-lock the definition and recheck its status: a pause, a drop, or
    // another concurrent alter must not interleave between the checks above
    // and this commit (issue #231's lesson, applied here the same way
    // `lifecycle::drop_transform` applies it to its own precondition).
    let row = txn
        .query_opt(
            "select status, definition_version from transform_definitions where id = $1 for update",
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
    if !status.is_applying() {
        return Err(CatalogError::TransformNotLive {
            transform: alter.target.clone(),
            status,
        });
    }
    let old_version: i64 = row.get(1);
    let new_version = old_version + 1;

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
    for field in &real_alters {
        check_no_type_changing_alter_in_txn(
            &txn,
            &current.target_table,
            &alter.target,
            field,
            &field_types,
        )
        .await?;
    }

    // The same version-fence bump `create_definition_inner` makes for a
    // brand-new definition — see this function's own doc comment. It runs
    // *before* the DDL below, not after it: a drain's Phase 3 takes this
    // row `for share` and then row-locks the target, so taking the target's
    // `ACCESS EXCLUSIVE` lock first and this row second is the opposite
    // order and deadlocks against any drain in flight. In this order, a
    // drain that already holds the row finishes first, and one that arrives
    // later waits for this commit and then misses the fence.
    txn.query_one(
        "insert into source_table_versions (source_table, version) \
         values ($1, 1) \
         on conflict (source_table) \
         do update set version = source_table_versions.version + 1 \
         returning version",
        &[&current.source_table],
    )
    .await?;

    for field in &real_drops {
        txn.batch_execute(&format!(
            "alter table {target_ident} drop column if exists {}",
            quote_ident(field)
        ))
        .await?;
        // ADR-0014's "quarantine state follows its owner", at column
        // granularity — the same bookkeeping `lifecycle::drop_transform`
        // clears for a whole target. A pause left behind here would outlive
        // its column and silently apply to a later `ADD` of the same name,
        // since that `ADD`'s own pause below only ever undoes a pause it
        // created itself (issue #309). Edges *into* it are cleared so
        // resuming the upstream column never tries to recompute a column
        // that is gone. Edges *out of* it can still exist even though the
        // dependents check above passed: a pause cascaded onto a reader
        // whose formula was later `ALTER`ed to stop reading this column
        // keeps its edge. Clearing it leaves that reader paused with no
        // recorded reason, and its own `RESUME` recovers it. That is still
        // better than an edge from a column that no longer exists.
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
    for field in &real_adds {
        let pg_type = ddl::pg_type_name(
            field_types
                .get(&field.name)
                .copied()
                .unwrap_or(ValueType::Numeric),
        );
        txn.batch_execute(&format!(
            "alter table {target_ident} add column if not exists {} {pg_type}",
            quote_ident(&field.name)
        ))
        .await?;
    }
    // No `ALTER COLUMN ... TYPE` DDL runs here, deliberately: `real_alters`
    // only ever reaches this point for a field whose new formula infers the
    // *same* `ValueType` its column already has — [`check_no_type_changing_alter_in_txn`]
    // above already refused (and rolled back the whole transaction without
    // issuing a single DDL statement) for any field where that isn't true.
    // A same-type `ALTER` needs no physical schema change at all; only its
    // *data* is stale, which the single-pass backfill below recomputes —
    // same as it already does for a same-type `ADD`.

    // The column-granularity pause: freeze every changed field from live CDC
    // apply (and from any other concurrent backfill) until this call's own
    // single-pass recompute below finishes and clears it. `on conflict do
    // nothing`: a field already paused for some other reason (an operator
    // pause, a tripped column fuse, a cascade from a paused upstream column)
    // simply stays paused through this edit too. Only the rows this insert
    // actually created land in `self_paused`, and those are the only ones
    // the unpause step at the end may clear (issue #309).
    let mut self_paused: Vec<String> = Vec::new();
    for field in real_adds.iter().chain(real_alters.iter()) {
        let inserted = txn
            .execute(
                "insert into column_status (transform_table, column_name, paused_at, local_fuse) \
                 values ($1, $2, now(), false) \
                 on conflict (transform_table, column_name) do nothing",
                &[&alter.target, &field.name],
            )
            .await?;
        if inserted > 0 {
            self_paused.push(field.name.clone());
        }
    }

    let new_text = render_definition_text(&merged);
    txn.execute(
        "update transform_definitions \
         set definition_text = $1, definition_version = $2 \
         where id = $3",
        &[&new_text, &new_version, &current.id],
    )
    .await?;

    txn.commit().await?;

    // Outside the transaction (and, deliberately, holding no lock): the
    // single-pass backfill below is the same "one enumeration of the source"
    // shape `install_definition`'s own build is, sized for however large the
    // source table is — exactly the work a first `define` already does
    // without holding a transaction open across it.
    if !real_adds.is_empty() || !real_alters.is_empty() {
        let mut written_fields: HashSet<String> =
            real_adds.iter().map(|f| f.name.clone()).collect();
        written_fields.extend(real_alters.iter().map(|f| f.name.clone()));
        backfill::backfill_altered_columns(
            pool,
            &merged,
            target_schema,
            &current.source_table,
            &written_fields,
        )
        .await
        .map_err(CatalogError::DirectBackfill)?;

        // Unpause, and close the backfill's tail race in the same
        // transaction (issue #305). Each PK-range chunk above read the
        // source under its own snapshot, and until this commit live CDC
        // apply skips every paused field: a row changed after its chunk read
        // it had its delta applied to every *other* column, leaving the
        // changed field at the chunk's pre-change value, and nothing would
        // ever revisit it once unpaused. The parked catch-up marker is the
        // same closed loop a first `define`'s chunked build ends with
        // ([`complete_direct_backfill`]): once its fence settles,
        // `run_pending_backfills` re-enumerates the source and re-derives
        // every row with these fields unpaused. Atomic with the unpause, not
        // after it: a marker visible before the unpause could be discharged
        // (and its enumeration applied) while the fields were still paused,
        // re-opening exactly this gap; a crash between two separate commits
        // would lose the catch-up outright.
        //
        // Only this call's own pauses are cleared (issue #309), and only if
        // nothing else claimed them while the backfill ran: an operator
        // pause or fuse trip in that window upgrades the row to `local_fuse`
        // (`staging::quarantine::pause_column`/`trip_column_fuse` upsert onto
        // it), and an upstream pause cascading onto it records an edge in
        // `column_pause_cascades`. Either way the row now belongs to that
        // other reason, and its `RESUME` is what recovers the column.
        let mut client = pool.get().await?;
        let txn = client.transaction().await?;
        for field in &self_paused {
            txn.execute(
                "delete from column_status s \
                 where s.transform_table = $1 and s.column_name = $2 \
                   and not s.local_fuse \
                   and not exists ( \
                       select 1 from column_pause_cascades c \
                       where c.downstream_transform = s.transform_table \
                         and c.downstream_column = s.column_name)",
                &[&alter.target, field],
            )
            .await?;
        }
        // Issue #476: the target is missing the changes this marker's
        // discharge folds in, so the definition reports `catching_up` until
        // then (it keeps applying).
        crate::intake::publication::park_catch_up(
            &*txn,
            &[current.id],
            std::slice::from_ref(&current.source_table),
        )
        .await?;
        txn.commit().await?;
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
/// re-run under the target's own row lock (issue #231's discipline) so a
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

/// [`alter_transform`]'s pre-transaction, best-effort half of the
/// type-changing-`ALTER` refusal — see [`check_no_type_changing_alter_via`]'s
/// own doc comment for the full reasoning; this is the [`check_no_column_dependents`]-shaped
/// counterpart, run against a fresh pooled connection before the transaction
/// even opens.
async fn check_no_type_changing_alter(
    pool: &Pool,
    qualified_target: &str,
    display_target: &str,
    field: &FieldDef,
    field_types: &HashMap<String, ValueType>,
) -> Result<(), CatalogError> {
    let client = pool.get().await?;
    check_no_type_changing_alter_via(
        &**client,
        qualified_target,
        display_target,
        field,
        field_types,
    )
    .await
}

/// The transaction-scoped counterpart to [`check_no_type_changing_alter`],
/// re-run under the target definition's own row lock (issue #231's
/// discipline, [`check_no_column_dependents_in_txn`]'s exact shape) so this
/// refusal is the last word, not just an optimistic pre-check.
async fn check_no_type_changing_alter_in_txn(
    txn: &tokio_postgres::Transaction<'_>,
    qualified_target: &str,
    display_target: &str,
    field: &FieldDef,
    field_types: &HashMap<String, ValueType>,
) -> Result<(), CatalogError> {
    check_no_type_changing_alter_via(txn, qualified_target, display_target, field, field_types)
        .await
}

/// The one check both [`check_no_type_changing_alter`] (pre-transaction,
/// best-effort) and [`check_no_type_changing_alter_in_txn`] (under the row
/// lock, authoritative) run: refuses `ALTER <field> AS <expr>` whenever
/// `field`'s newly-inferred [`ValueType`] differs from the [`ValueType`] its
/// column *actually, physically* has right now.
///
/// Why [`ValueType`] equality is the right (not just a convenient) test —
/// ADR-0015 (`docs/decisions/0015-transform-redefinition.md`) promises "An
/// edit never rewrites the whole target table to change one column."
/// Postgres can skip a full-table rewrite on `ALTER COLUMN ... TYPE` only
/// when the transform from old value to new is nothing but a relabeling of
/// the same bytes (no `USING` computation, no assignment cast) — and every
/// target column this crate ever creates is declared from exactly one
/// [`ValueType`] via [`ddl::pg_type_name`], so two columns/formulas sharing
/// one `ValueType` always share the same physical representation, and two
/// different `ValueType`s never do (an `Integer(Int4)` and an `Integer(Int8)`
/// are both "an integer" conversationally, but 4 and 8 physical bytes
/// respectively — Postgres rewrites for that pair exactly as it would for
/// `text` to `numeric`). So within this crate's own type model there is no
/// safe middle ground to special-case: same `ValueType` is always metadata-
/// only-safe, any different `ValueType` always forces the whole-heap rewrite
/// the ADR promises never happens. Classifying the column's *current*
/// physical type via its raw `atttypid` OID and [`super::pg_type::value_type_for_oid`]
/// — the exact machinery [`resolve_relationships`] already uses to classify
/// a relationship's to-side column, and the one place in this crate that
/// turns an OID into a [`ValueType`] — is what makes this comparison honest
/// (a `format_type` *text* comparison, this function's now-removed
/// predecessor, was equally correct in practice here only because every
/// target column's text rendering happens to already be canonical, and
/// would have quietly mis-detected a hypothetical future modifier-bearing
/// target type as "no change").
///
/// A real fix — a shadow column, single-pass backfill, then an atomic
/// rename-swap, none of which needs the old column's rewrite at all — is
/// tracked separately and deliberately out of scope for this pass; this
/// project is pre-release (no compatibility burden to preserve for a caller
/// depending on today's unrestricted behavior), so refusing outright is
/// simpler and more honest than shipping the full-table-lock behavior this
/// ADR explicitly disclaims. `DROP <field>` followed by `ADD <expr> AS
/// <field>` remains available and already pays this same backfill cost
/// today — refusing here only makes that cost explicit instead of a silent
/// surprise.
///
/// Takes `client: &impl GenericClient` rather than a `Pool` or a concrete
/// `Transaction` so the same body serves both the pre-transaction pooled
/// check and the in-transaction authoritative recheck without duplicating
/// the query or the comparison.
async fn check_no_type_changing_alter_via(
    client: &impl GenericClient,
    qualified_target: &str,
    display_target: &str,
    field: &FieldDef,
    field_types: &HashMap<String, ValueType>,
) -> Result<(), CatalogError> {
    let new_type = field_types
        .get(&field.name)
        .copied()
        .unwrap_or(ValueType::Numeric);
    let Some(current_type_oid) =
        target_column_type_oid_via(client, qualified_target, &field.name).await?
    else {
        // Defensive: every real `ALTER` field is, by construction, a
        // physical column already (mirrors this crate's other target-column
        // introspection helpers' own comment to the same effect) — nothing
        // to compare against, so nothing to refuse.
        return Ok(());
    };
    let current_type = super::pg_type::value_type_for_oid(client, current_type_oid).await?;
    if current_type == new_type {
        return Ok(());
    }
    Err(CatalogError::UnsupportedAlter(format!(
        "ALTER '{field}' on '{display_target}' would change its column type from \
         '{current_type}' to '{new_type}'; Trellis does not support a type-changing ALTER in \
         this release because Postgres cannot apply it as a metadata-only change — it would \
         rewrite '{display_target}''s entire physical table under an ACCESS EXCLUSIVE lock, \
         blocking reads and writes to every other column too, not just this one. Use DROP \
         '{field}' followed by ADD <expr> AS '{field}' instead — it already pays this same \
         backfill cost explicitly, rather than paying it as a surprise.",
        field = field.name,
    )))
}

/// The raw `pg_attribute.atttypid` OID of `qualified_target.column`, the
/// input [`super::pg_type::value_type_for_oid`] classifies — the OID
/// counterpart to `column_type_in_txn`'s `format_type` rendering, for the
/// same reason [`resolve_relationships`]' `column_type_oid` prefers the raw
/// OID over `format_type` text: an OID is stable across a length/precision
/// modifier, and (issue #241's own reason for reaching for it here)
/// comparable to another OID's classification without ever needing to
/// parenthesis-strip or otherwise reparse rendered text. `None` if the
/// column somehow doesn't exist yet (defensive; see this function's callers).
async fn target_column_type_oid_via(
    client: &impl GenericClient,
    qualified_target: &str,
    column: &str,
) -> Result<Option<u32>, CatalogError> {
    let row = client
        .query_opt(
            "select a.atttypid \
             from pg_attribute a \
             where a.attrelid = pg_catalog.to_regclass($1) \
               and a.attname = $2 \
               and a.attnum > 0 \
               and not a.attisdropped",
            &[&qualified_target, &column],
        )
        .await?;
    Ok(row.map(|row| row.get(0)))
}

/// Validates `source_text` and persists it as a definition in `status`, with
/// its schema-graph nodes and edge and a `source_table_versions` bump, in one
/// transaction. Reads the source's catalog shape (its key), never its rows
/// (ADR-0016, issue #418): whoever builds the target does that later.
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
    // Issue #40: enrichment fields (`<rel>.<col>`) are validated against
    // catalog-resolved relationship metadata — cardinality (ADR-0006's
    // to-one/to-many rules) and each referenced to-side column's type — which
    // the sync, DB-less validator can't fetch itself, so resolve it here (same
    // caller-supplies-context split as `source_columns`).
    let relationships = resolve_relationships_for_new_definition(pool, &def).await?;
    validate(&def, source_columns, &relationships)?;

    let mut client = pool.get().await?;
    let txn = client.transaction().await?;

    // Issue #47: an aggregate (`GROUP BY`) definition's delta-maintenance
    // path needs the source row's *old* image on delete/update/re-parent
    // (`apply_aggregate.rs`'s `accumulate_changes`) to find which group to
    // decrement — reject up front, before any of this transaction's other
    // side effects, if the source table's replica identity can't guarantee
    // one. Checked first (ahead of node/edge work below) so a
    // doomed-to-fail aggregate definition never touches the schema graph.
    if let KeySpace::Aggregate { .. } = &def.key_space {
        assert_replica_identity_supports_aggregate(&txn, &def).await?;
    }

    // Issue #72 / #76, ADR-0007: resolve `def.source` — always a bare table
    // name (see [`super::ast::TransformDef`]'s own doc comment for why the
    // dotted spelling never lands in this field) — to its fully-qualified
    // `schema.table` identity exactly once, here, at definition-acceptance
    // time. Two ways to get there, branching on
    // [`super::ast::TransformDef::explicit_source_schema`]:
    //
    // * `Some(schema)` (issue #76): the definition wrote `FROM
    //   <schema>.<source>` explicitly, so `schema` is trusted outright —
    //   [`confirm_qualified_table_exists_in_txn`] checks that *exact*
    //   relation is real, never walking `search_path` the way the bare case
    //   does (ADR-0007 grammar clause 4: a qualified spelling resolves to
    //   that one relation, full stop).
    // * `None` (bare, the far more common case): [`resolve_graph_identity_in_txn`]
    //   — issue #72's plain `search_path` walk, extended by issue #74 with a
    //   chained-target fallback (see that function's own doc comment for
    //   why, and for why this can now safely run *before* the node/cycle
    //   checks below rather than after them, unlike issue #72/#73's
    //   original ordering here).
    //
    // Either way, from this point on `qualified_source` — never `def.source`
    // — is what gets persisted
    // (`source_table_versions`/`transform_definitions.source_table` below)
    // and threaded into every side effect that must agree with the
    // persisted row (the discharge's ring entries and
    // `complete_direct_backfill`'s catch-up marker elsewhere) *and* (issue
    // #74) `schema_nodes`/`schema_edges` themselves.
    //
    // This matters for the explicit-schema branch specifically: the test
    // fixtures [`create_definition`] and [`create_definition_without_backfill`]
    // never go through [`install_definition`]'s own fail-fast check (that
    // function's own doc comment on its identically-shaped block), so this is
    // the *only* place a bogus explicit source schema is ever caught for
    // them.
    let qualified_source = match &def.explicit_source_schema {
        Some(schema) => {
            if !confirm_qualified_table_exists_in_txn(&txn, schema, &def.source).await? {
                return Err(ValidationError::QualifiedSourceTableNotFound {
                    schema: schema.clone(),
                    table: def.source.clone(),
                }
                .into());
            }
            crate::intake::publication::qualify(schema, &def.source)?
        }
        None => resolve_graph_identity_in_txn(&txn, &def.source).await?,
    };
    // Issue #315: the authoritative check (`install_definition` repeats it
    // earlier, before any DDL, as a fail-fast).
    reject_non_live_upstream(&*txn, &qualified_source).await?;

    // Issue #73 / #76, ADR-0007: resolve `def.target` — likewise always bare
    // — to its fully-qualified identity exactly once, here, mirroring
    // `qualified_source` immediately above. Unlike the source side,
    // `def.target`'s schema was never search-path-resolved even before issue
    // #76: a source table's schema is *discovered* (it already exists
    // somewhere on the path), but a target table's schema is a
    // config-time *decision*, `target_schema` — which, as of issue #76, is
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
    // Built via the same `intake::publication::qualify` helper as
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
    let qualified_target =
        crate::intake::publication::qualify(resolved_target_schema, &def.target)?;
    // The test-fixture entry points (`target_ddl` is `None`) register against
    // a target their caller already created; `install_definition` creates it
    // below, after every check on the definition itself (issue #440).
    if target_ddl.is_none()
        && let Some(schema) = &def.explicit_target_schema
        && !confirm_qualified_table_exists_in_txn(&txn, schema, &def.target).await?
    {
        return Err(ValidationError::QualifiedTargetTableNotFound {
            schema: schema.clone(),
            table: def.target.clone(),
        }
        .into());
    }

    // Issue #129, epic #127: before this definition is persisted, widen the
    // settled parent projection of every to-one relationship its fields read
    // through to cover those reads — see
    // [`widen_relationship_projections_for_definition_in_txn`]'s own doc
    // comment. Deliberately ahead of the node/cycle/backfill work below, in
    // this same transaction: a definition that fails later in this function
    // rolls the widen back with it, and one that succeeds can never go live
    // observing a projection that hasn't caught up to its own declared
    // reads. A relationship-free definition (by far the common case) costs
    // nothing extra here — [`super::eval::relationship_references`] returns
    // empty and the loop inside never runs a query.
    widen_relationship_projections_for_definition_in_txn(
        &txn,
        &def,
        &qualified_source,
        pool.schema(),
    )
    .await?;

    // Issue #20: every definition's source and target resolve to a
    // first-class `SchemaNode`, created on first reference (a source node
    // the moment something first transforms it; a target node the moment
    // its owning definition is created) — a side effect alongside the
    // catalog writes below rather than a change to `TransformDef`'s shape,
    // per the issue's "prefer the smaller change" guidance.
    //
    // Keyed on `qualified_source`/`qualified_target` (issue #74, ADR-0007),
    // not the bare `def.source`/`def.target` issue #72/#73 left this on:
    // `schema_nodes`/`schema_edges` now key on the exact same qualified
    // identity `transform_definitions` does, closing the node-splitting gap
    // the old bare keying protected against only by never distinguishing
    // schemas at all. [`create_relationship`]'s own two `resolve_node_in_txn`
    // calls are qualified the same way (via this same
    // [`resolve_graph_identity_in_txn`]; a relationship records each
    // endpoint's resolved schema too, issues #285/#372), because both call
    // sites resolve into
    // the *same* `schema_nodes` table: a table that's both a transform
    // source/target and a relationship endpoint (the common case ADR-0006's
    // examples all chain off) must land on one node regardless of which
    // grammar referenced it, and only qualifying both sides achieves that.
    let source_node = resolve_node_in_txn(&txn, &qualified_source, NodeKind::Source).await?;
    let target_node = resolve_node_in_txn(&txn, &qualified_target, NodeKind::Target).await?;

    // Issue #22 (generalized): reject this definition if the `Source` edge
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
    // resolution above (issue #74) — `schema_edges` is keyed by
    // `schema_nodes.id`, so this walks the exact same qualified graph those
    // nodes were just resolved into.
    reject_if_table_cycle(&txn, &qualified_source, &qualified_target).await?;

    // Issue #21: a transform's `FROM` is a `Source` dependency edge from its
    // source node to its target node — persisted alongside the node
    // resolutions above so `dependents_of` can walk the graph instead of
    // matching on `transform_definitions.source_table` string equality.
    persist_edge_in_txn(&txn, source_node.id, target_node.id, EdgeKind::Source).await?;

    // Reviewer follow-up to issue #73 / ADR-0007: reject this definition if
    // `qualified_target` shares a bare table-name suffix with a *different*
    // qualified spelling some other still-persisted definition already
    // uses. Before #73, `target_table` stored the bare name and its own
    // `unique` constraint (`V2__transform_catalog.sql`) enforced this for
    // free; now that the column stores the qualified spelling, that
    // constraint only guarantees the qualified string is unique, and
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
    // Still provisional, not relaxed by issue #76 landing: the grammar now
    // accepts an explicit `TRANSFORM <schema>.<target>` spelling, so an
    // operator *can* write `TRANSFORM custom.foo FROM ...` to disambiguate
    // from an existing `public.foo` — but this check doesn't distinguish an
    // explicit qualification from a resolved-bare one, and still rejects the
    // collision either way. That's deliberate, not an oversight: every
    // `split_part(target_table, '.', 2)`-keyed read site named above is
    // still bare-suffix-keyed — issue #74 (ADR-0007) qualified the
    // `schema_nodes`/`schema_edges` graph (and, with it, [`dependents_of`],
    // which no longer needs `split_part` at all — see its own doc comment),
    // but deliberately left `column_status`'s addressing scheme and the
    // handful of `Trellis`-API-facing bare-name read sites named above
    // exactly as issue #73 did, for the same reasons: ADR-0003's
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

    // Issue #121 (previously issue #177's arity gate): fail fast on a
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
    // fallback, issue #74), and every check above this point tolerates that
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
    // Issue #371: every key-space, not just `OneToOne`. Live apply calls
    // `ddl::source_primary_key` on an aggregate's source too
    // (`staging::apply`'s by-source and TRUNCATE loops), and
    // `quarantine::classify` halts the instance on the key-type error it
    // returns — so an aggregate over a `numeric`-keyed table, or chained off
    // a `numeric`-grouped aggregate (whose identity is its `GROUP BY` key),
    // used to be accepted here and then halt on its first live change.
    ddl::source_primary_key_in_txn(&*txn, &qualified_source)
        .await
        .map_err(CatalogError::Ddl)?;
    // Issue #376: the authoritative copy of `install_definition`'s fail-fast
    // check, placed with the key check above because it reads the live
    // source relation (an own-target source is exempt before that query).
    reject_unkeyed_source(&*txn, &qualified_source).await?;

    // Issue #440: registration creates the target here, in the transaction
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

    let version: i64 = txn
        .query_one(
            "insert into source_table_versions (source_table, version)
             values ($1, 1)
             on conflict (source_table)
             do update set version = source_table_versions.version + 1
             returning version",
            &[&qualified_source],
        )
        .await?
        .get(0);

    let (type_keys, type_vals) = encode_type_map(source_columns);

    let status_text = status.as_str();

    // Reviewer follow-up to issue #73: `transform_definitions_target_suffix_idx`
    // (`V23__transform_definitions_target_suffix_idx.sql`) is the DB-level
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
    let id: i64 = match txn
        .query_one(
            "insert into transform_definitions
                (target_table, source_table, source_version, definition_text, source_columns, status)
             values ($1, $2, $3, $4, jsonb_object($5::text[], $6::text[]), $7)
             returning id",
            &[
                &qualified_target,
                &qualified_source,
                &version,
                &source_text,
                &type_keys,
                &type_vals,
                &status_text,
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
/// with [`CatalogError::TargetTableExists`] and left untouched (issue #440).
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
    let already_exists = || CatalogError::TargetTableExists {
        table: qualified_target.to_string(),
    };
    let exists: bool = txn
        .query_one(
            "select exists (select 1 from pg_catalog.pg_class c \
             join pg_catalog.pg_namespace n on n.oid = c.relnamespace \
             where n.nspname = $1 and c.relname = $2)",
            &[&schema, &table],
        )
        .await?
        .get(0);
    if exists {
        return Err(already_exists());
    }
    match txn.batch_execute(ddl).await {
        Ok(()) => Ok(()),
        Err(err) if is_relation_name_taken(&err) => Err(already_exists()),
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
/// (`V23__transform_definitions_target_suffix_idx.sql`) specifically — the
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

/// Parses, validates, and stores a new relationship declaration (issue #26
/// storage, issue #27 validation, ADR-0006): resolves/creates `schema_nodes`
/// for both endpoints, persists a `schema_edges` row from `from_table` to
/// `to_table` tagged [`EdgeKind::Relationship`], and inserts the immutable
/// `relationship_definitions` row — all in one transaction, mirroring
/// [`create_definition`]'s pattern.
///
/// Validates, in order: both endpoints' `table.column` exist and resolve to
/// comparable Postgres types ([`column_type_in_txn`] /
/// [`assert_comparable_types`], ADR-0006's "type-check the join"); the
/// relationship's name is not already declared on the qualified
/// `from_table` ([`ValidationError::DuplicateRelationshipName`], a friendlier
/// definition-time surfacing of the same rule
/// `relationship_definitions_from_schema_from_table_name_key` backstops at
/// the DB level); and the new `Relationship` edge would not close a cycle
/// ([`reject_if_table_cycle`], generalized unchanged from
/// [`create_definition`]'s `Source`-edge use). Cardinality
/// ([`RelationshipCardinality`]) is determined via
/// [`to_col_cardinality_in_txn`] and persisted, not rejected on — ADR-0006's
/// "a to-many reference must be aggregate-wrapped" rule is a *reference*-time
/// check (validating how a relationship is *used* in a calculated field),
/// deferred past this issue since no such reference resolves yet (see
/// [`super::ast::Expr::RelationshipPath`]).
///
/// Issue #375: an endpoint that isn't one of this instance's targets is
/// published, so it must be keyable over CDC
/// ([`reject_unkeyed_relationship_endpoint`]). One that is (1-1 or
/// aggregate) is never published: the target-mutation seam is its only
/// change feed, so neither that check nor the replica-identity checks below
/// apply to it (#403).
///
/// Node-kind resolution: a relationship's endpoints may each be "a source
/// table or a transform target, in any combination" (ADR-0006), and nothing
/// here can tell which without cross-referencing `transform_definitions`.
/// Both endpoints resolve as [`NodeKind::Source`] — directionally accurate
/// either way, since computing the join reads both tables' columns
/// regardless of whether one side later turns out to also be a transform
/// target — and [`resolve_node_in_txn`]'s flags are additive (OR'd in, never
/// cleared), so a later `create_definition` call that resolves the same
/// table as [`NodeKind::Target`] merges into the same node rather than
/// conflicting with this choice.
pub async fn create_relationship(
    pool: &Pool,
    source_text: &str,
) -> Result<RelationshipDefinition, CatalogError> {
    let def: RelationshipDef = parse_relationship(source_text)?;

    let mut client = pool.get().await?;
    let txn = client.transaction().await?;

    // Issue #74, ADR-0007: resolve both endpoints to their fully-qualified
    // identity via the same [`resolve_graph_identity_in_txn`]
    // [`create_definition_inner`] uses for a transform's source/target,
    // *before* touching `schema_nodes`/`schema_edges` — or, per the reviewer
    // follow-up below, this function's own pg_catalog introspection either —
    // at all. Required now that graph keys on qualified identity, so a table
    // that's both a relationship endpoint and a transform source/target (the
    // common case ADR-0006's own examples all chain off) resolves to one
    // node either way, not two. The schema half of each resolution is
    // persisted too (`from_schema`/`to_schema`, below).
    //
    // Reviewer follow-up to issue #74 (epic #78's own whole-branch review):
    // this resolution used to run *after* this function's own pg_catalog
    // introspection (`column_type_in_txn`/`to_col_cardinality_in_txn`/
    // `has_usable_fk_index_in_txn`/`assert_replica_identity_supports_to_many`,
    // below), which resolve `def.from_table`/`def.to_table` bare via
    // `pg_catalog.to_regclass` — a plain `search_path` walk with no
    // fallback, unlike this call. So a relationship endpoint that bare-names
    // another definition's target explicitly qualified into a non-default
    // schema (issue #76) never reached this resolution at all: it failed
    // first, in one of those four checks, as a false "does not exist" (e.g.
    // `to_col_cardinality_in_txn`'s `to_regclass($1)` resolving to `NULL`
    // reads as "no such index", not "wrong schema"). Moved up here, before
    // any of them, and threaded through via [`resolve_relationship_endpoint_in_txn`]
    // — a thin wrapper around this exact function, not a second fallback
    // implementation — so every pg_catalog lookup below gets the same
    // two-step resolution `create_definition_inner` already relies on.
    let qualified_from = resolve_relationship_endpoint_in_txn(&txn, &def.from_table).await?;
    let qualified_to = resolve_relationship_endpoint_in_txn(&txn, &def.to_table).await?;

    let from_type =
        column_type_in_txn(&txn, &qualified_from, &def.from_table, &def.from_col).await?;
    let to_type = column_type_in_txn(&txn, &qualified_to, &def.to_table, &def.to_col).await?;
    assert_comparable_types(&def, &from_type, &to_type)?;
    assert_join_key_type_supported(&txn, &def, &from_type, &to_type).await?;
    reject_unkeyed_relationship_endpoint(&*txn, &qualified_from).await?;
    reject_unkeyed_relationship_endpoint(&*txn, &qualified_to).await?;
    // Issue #403: an endpoint that is one of this instance's targets must be
    // `live`, for the reason `reject_non_live_upstream` gives a chained
    // reader: the seam is now such an endpoint's only change feed, and a
    // build's writes (the chunk queue's, including a chunk a worker still
    // holds across a pause) land outside it.
    reject_non_live_upstream(&*txn, &qualified_from).await?;
    reject_non_live_upstream(&*txn, &qualified_to).await?;

    // Issues #285/#288: the schema half of `qualified_from` is persisted
    // alongside the bare `from_table` and is part of the relationship's
    // identity — `(from_schema, from_table, name)` — so every later reader
    // (a transform's `<rel>.<column>` path, a scoped `DROP RELATIONSHIP
    // <schema>.<from_table>.<name>`, the reverse-recompute from-side reads)
    // reaches the table this relationship was actually declared against. Read
    // off `qualified_from` — the one resolution this function already trusts
    // for `resolve_node_in_txn`/`reject_if_table_cycle` and every pg_catalog
    // check above — rather than re-resolving `def.from_table` a second time,
    // which could disagree with it.
    //
    // The `None` arm is unreachable from here:
    // [`resolve_relationship_endpoint_in_txn`] returns an *unqualified* name
    // only when `def.from_table` resolves to nothing at all, and that case
    // always fails `column_type_in_txn` above and returns long before this
    // insert. Surfaced as the same "from-table doesn't exist" error those
    // checks would have raised rather than panicking over a state this crate's
    // own writers cannot produce (same stance as
    // [`super::lifecycle`]'s `quote_qualified`).
    let from_schema = qualified_from
        .split_once('.')
        .map(|(schema, _)| schema.to_string())
        .ok_or_else(|| CatalogError::SourceTableNotFound(def.from_table.clone()))?;
    // Issue #372: the to-side's resolved schema is persisted the same way, so
    // no later reader re-resolves the bare `to_table` through its own
    // session's `search_path` and lands on some other schema's same-named
    // table. The `None` arm is unreachable for the reason given above.
    let to_schema = qualified_to
        .split_once('.')
        .map(|(schema, _)| schema.to_string())
        .ok_or_else(|| CatalogError::SourceTableNotFound(def.to_table.clone()))?;

    // Issue #288: a relationship name is unique per *qualified* from-table,
    // so `shop.posts` may declare an `author` even when `blog.posts` already
    // has one — the same `(from_schema, from_table, name)` key
    // `relationship_definitions_from_schema_from_table_name_key` enforces.
    let already_declared: bool = txn
        .query_one(
            "select exists (
                select 1 from relationship_definitions
                where from_schema = $1 and from_table = $2 and name = $3
             )",
            &[&from_schema, &def.from_table, &def.name],
        )
        .await?
        .get(0);
    if already_declared {
        return Err(ValidationError::DuplicateRelationshipName {
            from_table: qualified_from.clone(),
            name: def.name.clone(),
        }
        .into());
    }

    let from_node = resolve_node_in_txn(&txn, &qualified_from, NodeKind::Source).await?;
    // `to_table` is marked `is_source` here too, even though a relationship's
    // to-side is often really a transform target: the flag is additive/OR'd
    // (a later `create_definition` call can still set `is_target` on the
    // same node), and no consumer reads `schema_nodes.is_source` directly —
    // [`all_source_tables`] (the publication feeder) walks `schema_edges`
    // `Relationship` edges from `transform_definitions.source_table` anchors
    // instead (issue #75), never the `is_source` flag itself: that flag is
    // set on *every* relationship endpoint regardless of whether it's
    // actually reachable from a registered transform, so reading it directly
    // would leak an orphaned relationship's tables into the publication
    // (issue #65's test case 4). If a future `is_source` consumer reads
    // `schema_nodes` directly, re-check this call.
    let to_node = resolve_node_in_txn(&txn, &qualified_to, NodeKind::Source).await?;

    // The `Relationship` edge is persisted `to_table -> from_table` (parent
    // -> child), matching `Source`'s "to_node depends on from_node"
    // convention (see `SchemaEdge`'s doc comment): the FK-holding
    // `from_table` is the dependent side — a bare-path reference like
    // `product.x` in a calculated field over `from_table` pulls from
    // `to_table`, so `from_table` depends on `to_table`, not the reverse.
    // Persisting it `from_table -> to_table` instead (the naive reading of
    // "FROM ... TO ...") would invert that: it'd wrongly reject a
    // target-table-references-its-own-source relationship as a false
    // 2-cycle (both edges actually mean "target depends on source"), and it
    // would make future dependents-of-a-changed-table traversals (#28+)
    // miss relationship dependents, since `edges_from(to_table)` wouldn't
    // reach `from_table` at all.
    reject_if_table_cycle(&txn, &qualified_to, &qualified_from).await?;

    persist_edge_in_txn(&txn, to_node.id, from_node.id, EdgeKind::Relationship).await?;

    let cardinality = to_col_cardinality_in_txn(&txn, &qualified_to, &def.to_col).await?;

    // To-many's join key is a non-PK column on the to-side; reverse recompute
    // reads it from delete/re-parent pre-images, which the default (PK)
    // replica identity omits — reject unless the to-side carries it (#41).
    // A to-side that is one of this instance's targets is exempt: it is never
    // published, and the seam's rows carry its full prior image (#403).
    //
    // A to-one relationship instead gets a settled parent projection (issue
    // #129, epic #127) unconditionally — see
    // [`assert_replica_identity_supports_projection`]'s own doc comment for
    // why this is gated on cardinality alone, exactly like the to-many arm,
    // rather than on whether a consumer exists yet. Issue #158: a to-one
    // relationship also needs its *from*-side (child) table on `REPLICA
    // IDENTITY FULL`, not just its to-side — see that same function's doc
    // comment for why both endpoints share one gate, and
    // [`check_source_guarantees`] for the same own-target exemption.
    if cardinality == RelationshipCardinality::ToMany {
        if !is_definition_target(&*txn, &qualified_to).await? {
            assert_replica_identity_supports_to_many(&txn, &def, &qualified_to).await?;
        }
    } else {
        assert_replica_identity_supports_projection(
            &txn,
            &def.to_table,
            &qualified_to,
            &def.from_table,
            &qualified_from,
        )
        .await?;
    }

    let mut warnings = Vec::new();
    if !has_usable_fk_index_in_txn(&txn, &qualified_from, &def.from_col).await? {
        warnings.push(RelationshipWarning::MissingFkIndex {
            from_table: def.from_table.clone(),
            from_col: def.from_col.clone(),
        });
    }

    let id: i64 = txn
        .query_one(
            "insert into relationship_definitions
                (name, from_schema, from_table, from_col, to_schema, to_table, to_col,
                 definition_text, cardinality)
             values ($1, $2, $3, $4, $5, $6, $7, $8, $9)
             returning id",
            &[
                &def.name,
                &from_schema,
                &def.from_table,
                &def.from_col,
                &to_schema,
                &def.to_table,
                &def.to_col,
                &source_text,
                &cardinality.as_str(),
            ],
        )
        .await?
        .get(0);

    // Issue #129, epic #127: every to-one relationship gets a settled parent
    // projection from the moment it's declared, not just once a consumer
    // shows up — see [`ensure_relationship_projection_in_txn`]'s own doc
    // comment for why (a relationship must exist before anything can
    // reference it, so there is never a consumer yet at this point) and for
    // why `needed_columns` is empty here (this call only creates the
    // projection and seeds its bookkeeping columns for every existing
    // to-side row; [`create_definition_inner`]'s own call is what widens it
    // once a consumer's read columns are known). Runs inside this same
    // transaction so a relationship declaration and its projection's
    // creation commit or roll back together.
    if cardinality == RelationshipCardinality::ToOne {
        ensure_relationship_projection_in_txn(
            &txn,
            id,
            &qualified_to,
            &def.to_table,
            &def.to_col,
            &to_type,
            pool.schema(),
            &[],
        )
        .await?;
    }

    txn.commit().await?;

    Ok(RelationshipDefinition {
        id,
        from_schema,
        to_schema,
        def,
        cardinality,
        warnings,
    })
}

/// The columns every relationship read-back below selects, in the order
/// [`relationship_from_row`] expects them.
const RELATIONSHIP_READ_COLUMNS: &str = "id, from_schema, to_schema, definition_text, cardinality";

/// Rebuilds a [`RelationshipDefinition`] from a row selected with
/// [`RELATIONSHIP_READ_COLUMNS`]. Re-parses the persisted `definition_text`
/// rather than reconstructing [`RelationshipDef`] from the denormalized
/// columns, matching [`dependents_of`]'s "reuse the grammar's own parser"
/// convention; `cardinality` is read back from its own column instead, since
/// it isn't part of the source text (issue #27: it's derived, not declared).
fn relationship_from_row(
    row: &tokio_postgres::Row,
) -> Result<RelationshipDefinition, CatalogError> {
    let cardinality_text: String = row.get(4);
    let cardinality =
        RelationshipCardinality::from_persisted(&cardinality_text).unwrap_or_else(|| {
            panic!(
                "relationship_definitions.cardinality held unrecognized value '{cardinality_text}'"
            )
        });
    Ok(RelationshipDefinition {
        id: row.get(0),
        from_schema: row.get(1),
        to_schema: row.get(2),
        def: parse_relationship(row.get(3))?,
        cardinality,
        // Creation-time guidance, not a fact about the persisted row — see
        // the field's doc comment on [`RelationshipDefinition`].
        warnings: Vec::new(),
    })
}

/// Reads back the relationship named `name` declared on the from-table
/// `from_schema.from_table` — the key a [`super::ast::Expr::RelationshipPath`]'s
/// `rel` head resolves against. A relationship name is unique per
/// *qualified* from-table (ADR-0006, issue #288), so all three parts are
/// needed to identify one row: `blog.posts.author` and `shop.posts.author`
/// are two different relationships.
///
/// A transform's relationship references resolve against its own persisted,
/// already-qualified `transform_definitions.source_table` — see
/// [`relationship_on_source`], which splits that spelling for this call.
pub async fn relationship_by_name(
    pool: &Pool,
    from_schema: &str,
    from_table: &str,
    name: &str,
) -> Result<Option<RelationshipDefinition>, CatalogError> {
    let client = pool.get().await?;
    relationship_by_name_in(&**client, from_schema, from_table, name).await
}

/// [`relationship_by_name`] on the caller's own connection or transaction,
/// for a caller already inside one (`intake::resume_orphans`, which runs in a
/// backfill marker's discharge transaction).
pub(crate) async fn relationship_by_name_in(
    client: &impl GenericClient,
    from_schema: &str,
    from_table: &str,
    name: &str,
) -> Result<Option<RelationshipDefinition>, CatalogError> {
    let row = client
        .query_opt(
            &format!(
                "select {RELATIONSHIP_READ_COLUMNS} from relationship_definitions \
                 where from_schema = $1 and from_table = $2 and name = $3"
            ),
            &[&from_schema, &from_table, &name],
        )
        .await?;
    row.as_ref().map(relationship_from_row).transpose()
}

/// [`relationship_by_name`] for a transform whose source is the qualified
/// `"schema.table"` `qualified_source` (`transform_definitions.source_table`,
/// [`Definition::source_table`]) — the lookup every `<rel>.<column>` path in a
/// transform's fields resolves through. A relationship declared on a
/// same-named table in some other schema is a different relationship and is
/// never returned here.
///
/// Every caller in this crate passes a persisted, already-qualified source.
/// A bare one (the public DDL/backfill entry points take a caller-supplied
/// source, and a caller may hand them a bare name) is resolved the way the
/// `RELATIONSHIP` statement itself resolved its from-table —
/// [`resolve_relationship_endpoint`], `search_path` first — and names no
/// relationship if it resolves to nothing.
pub(crate) async fn relationship_on_source(
    pool: &Pool,
    qualified_source: &str,
    name: &str,
) -> Result<Option<RelationshipDefinition>, CatalogError> {
    let resolved;
    let qualified_source = if qualified_source.contains('.') {
        qualified_source
    } else {
        resolved = resolve_relationship_endpoint(pool, qualified_source).await?;
        resolved.as_str()
    };
    let Some((schema, table)) = qualified_source.split_once('.') else {
        return Ok(None);
    };
    relationship_by_name(pool, schema, table, name).await
}

/// Resolves a `DROP RELATIONSHIP [<schema>.]<from_table>.<name>` address to
/// the one relationship it names, or `Ok(None)` when it names none (a drop's
/// idempotent no-op).
///
/// A qualified address is an exact `(from_schema, from_table, name)` lookup
/// ([`relationship_by_name`]). A qualifier naming a schema whose `from_table`
/// declared no such relationship names nothing, even if a same-named table
/// elsewhere did — the case #285 fixed, where `DROP RELATIONSHIP
/// shop.posts.author` used to drop `blog.posts`' `author`.
///
/// A bare address names the relationship if exactly one from-table called
/// `from_table` declares `name`, in whichever schema. Since issue #288 two
/// can (`blog.posts.author` and `shop.posts.author`), and then the bare
/// address is refused as [`CatalogError::AmbiguousRelationshipAddress`]
/// rather than resolved through `search_path`: `DROP` is destructive, and
/// which schema a session's `search_path` happens to reach first is not
/// something the caller can see in the statement they wrote.
pub(crate) async fn relationship_at_address(
    pool: &Pool,
    schema: Option<&str>,
    from_table: &str,
    name: &str,
) -> Result<Option<RelationshipDefinition>, CatalogError> {
    if let Some(schema) = schema {
        return relationship_by_name(pool, schema, from_table, name).await;
    }
    let client = pool.get().await?;
    let rows = client
        .query(
            &format!(
                "select {RELATIONSHIP_READ_COLUMNS} from relationship_definitions \
                 where from_table = $1 and name = $2 order by from_schema"
            ),
            &[&from_table, &name],
        )
        .await?;
    match rows.as_slice() {
        [] => Ok(None),
        [row] => relationship_from_row(row).map(Some),
        rows => Err(CatalogError::AmbiguousRelationshipAddress {
            from_table: from_table.to_string(),
            name: name.to_string(),
            schemas: rows.iter().map(|row| row.get(1)).collect(),
        }),
    }
}

/// Reads back the relationship whose `relationship_definitions.id` is `id` —
/// issue #134's deferred-reverse reconstruction path uses this: a
/// `rel_reverse_deferred` ring row persists only the relationship's id (see
/// `staging::append::StagedChange::RelationshipReverseDeferred`), so a later
/// drain that finds one needs to look the relationship back up by id alone,
/// not by `(from_schema, from_table, name)` ([`relationship_by_name`]) or
/// `to_table` ([`relationships_to_table`]) — neither of which a bare id lets it
/// derive without an extra round trip. `Ok(None)` when the relationship no
/// longer exists (dropped between the deferral and this retry) — the caller's
/// job to decide what "nothing to retry against anymore" means, not this
/// function's.
pub async fn relationship_by_id(
    pool: &Pool,
    id: i64,
) -> Result<Option<RelationshipDefinition>, CatalogError> {
    let client = pool.get().await?;
    let row = client
        .query_opt(
            &format!(
                "select {RELATIONSHIP_READ_COLUMNS} from relationship_definitions where id = $1"
            ),
            &[&id],
        )
        .await?;
    row.as_ref().map(relationship_from_row).transpose()
}

/// Every relationship whose to-side is the qualified `"schema.table"`
/// `qualified_to_table` — the reverse of [`relationship_by_name`]'s from-side
/// lookup. The staging reverse recompute (issue #30) uses this to answer "a
/// row in this table just changed; which relationships point *at* it, so which
/// from-side targets must re-derive?".
///
/// Matched against each relationship's recorded `to_schema` (issue #372), so a
/// same-named table in another schema never picks up this one's
/// relationships. A bare `qualified_to_table` matches nothing.
pub async fn relationships_to_table(
    pool: &Pool,
    qualified_to_table: &str,
) -> Result<Vec<RelationshipDefinition>, CatalogError> {
    let Some((to_schema, to_table)) = qualified_to_table.split_once('.') else {
        return Ok(Vec::new());
    };
    let client = pool.get().await?;
    let rows = client
        .query(
            &format!(
                "select {RELATIONSHIP_READ_COLUMNS} from relationship_definitions \
                 where to_schema = $1 and to_table = $2 order by id"
            ),
            &[&to_schema, &to_table],
        )
        .await?;
    rows.iter().map(relationship_from_row).collect()
}

/// Every relationship declared on the qualified from-table
/// `from_schema.from_table` — the outbound mirror of
/// [`relationships_to_table`]. Issue #133 (epic #127) uses this to build
/// intake's `src_table -> from_col` cache: the columns a from-side row's own
/// CDC images must be read to populate the ring's `group_key` column (the
/// union of join-key values that row's change touched). Schema-scoped (issue
/// #288) so a same-named table in another schema doesn't contribute its own
/// relationships' `from_col`s.
pub async fn relationships_from_table(
    pool: &Pool,
    from_schema: &str,
    from_table: &str,
) -> Result<Vec<RelationshipDefinition>, CatalogError> {
    let client = pool.get().await?;
    relationships_from_table_in(&**client, from_schema, from_table).await
}

/// [`relationships_from_table`] on a caller-supplied client, for a caller
/// already inside a transaction (`staging::target_mutations`, which resolves
/// a from-side endpoint target's join-key columns in the writing
/// transaction).
pub(crate) async fn relationships_from_table_in(
    client: &impl GenericClient,
    from_schema: &str,
    from_table: &str,
) -> Result<Vec<RelationshipDefinition>, CatalogError> {
    let rows = client
        .query(
            &format!(
                "select {RELATIONSHIP_READ_COLUMNS} from relationship_definitions \
                 where from_schema = $1 and from_table = $2 order by id"
            ),
            &[&from_schema, &from_table],
        )
        .await?;
    rows.iter().map(relationship_from_row).collect()
}

/// Resolves every relationship a definition's calculated fields reference
/// (issue #40) into the [`ResolvedRelationship`] map [`super::validate`] needs
/// to enforce ADR-0006's reference-time cardinality and type rules. The
/// validator is sync and DB-less, so — exactly like `source_columns` — the
/// caller does the catalog + `pg_catalog` lookups here and passes the result
/// in.
///
/// For each distinct relationship name used in `def` (via
/// [`super::eval::relationship_references`]), looks it up on
/// `qualified_source` — `def`'s own source as a qualified `"schema.table"`
/// ([`Definition::source_table`]), since a relationship name is unique only
/// per qualified from-table (issue #288) — and resolves the type of every
/// to-side column those paths read. A referenced
/// column that doesn't exist on the to-side is a hard
/// [`ValidationError::UnknownRelationshipColumn`] here (ADR-0005: check, don't
/// assume). An unknown relationship *name* is left absent from the map so the
/// validator reports it as [`ValidationError::UnknownRelationship`] against
/// the specific field, rather than this resolver guessing which field to
/// blame.
pub(crate) async fn resolve_relationships(
    pool: &Pool,
    def: &TransformDef,
    qualified_source: &str,
) -> Result<HashMap<String, ResolvedRelationship>, CatalogError> {
    let mut cols_by_rel: HashMap<String, Vec<String>> = HashMap::new();
    for (rel, column) in super::eval::relationship_references(def) {
        cols_by_rel.entry(rel).or_default().push(column);
    }

    let mut resolved = HashMap::with_capacity(cols_by_rel.len());
    for (rel, columns) in cols_by_rel {
        let Some(reldef) = relationship_on_source(pool, qualified_source, &rel).await? else {
            // Unknown name: leave it out; the validator names the offending
            // field in `ValidationError::UnknownRelationship`.
            continue;
        };
        let to_table = reldef.def.to_table.clone();
        // Issue #372: `column_type_oid` below reads the to-side the
        // relationship was declared against, never the bare `to_table`
        // re-resolved through this session's `search_path` (which may find a
        // different same-named table, or none).
        let query_to_table = reldef.qualified_to_table();
        // Issue #117: a to-side enum column now needs a connection to
        // classify (see `pg_type::value_type_for_oid`'s own doc comment) —
        // acquired once per relationship here rather than per column, since
        // a relationship's enrichment columns are typically few. Also now
        // the connection `column_type_oid` itself queries on (review fix:
        // it used to independently `pool.get()` per column, which meant
        // holding two connections from this same pool at once for the
        // whole loop below — see `column_type_oid`'s own doc comment).
        let client = pool.get().await?;
        let mut column_types = HashMap::with_capacity(columns.len());
        for column in columns {
            let type_oid = column_type_oid(&**client, &query_to_table, &to_table, &column).await?;
            column_types.insert(
                column,
                super::pg_type::value_type_for_oid(&**client, type_oid).await?,
            );
        }
        resolved.insert(
            rel,
            ResolvedRelationship {
                cardinality: reldef.cardinality,
                to_table,
                to_col: reldef.def.to_col.clone(),
                column_types,
            },
        );
    }
    Ok(resolved)
}

/// [`resolve_relationships`] for a definition not yet persisted — the define
/// and `ALTER` paths, which validate `def` before they've resolved (or, for
/// the ring-path entry points, recorded) its qualified source. Resolves it
/// the same way [`install_definition`] does ([`resolve_source_for_install`]),
/// but only when `def` references a relationship at all.
///
/// A source that doesn't resolve has no relationships to find, so it yields
/// an empty map rather than an error: the validator then reports each
/// relationship path as [`ValidationError::UnknownRelationship`] against its
/// field, and the missing source itself is reported by whichever later step
/// needs it — the same order these errors surfaced in before relationships
/// were looked up by qualified source.
pub(crate) async fn resolve_relationships_for_new_definition(
    pool: &Pool,
    def: &TransformDef,
) -> Result<HashMap<String, ResolvedRelationship>, CatalogError> {
    if super::eval::relationship_references(def).is_empty() {
        return Ok(HashMap::new());
    }
    match resolve_source_for_install(pool, def).await {
        Ok(qualified_source) => resolve_relationships(pool, def, &qualified_source).await,
        Err(CatalogError::SourceTableNotFound(_)) => Ok(HashMap::new()),
        Err(err) => Err(err),
    }
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
/// definition's own source text** (ADR-0007) — `def.source`, never a value
/// read back from `transform_definitions.source_table`/
/// `source_table_versions.source_table`. Those columns hold the *qualified*
/// result this function already produced once, at the definition's own
/// acceptance time (issue #72); feeding a qualified `"schema.table"` string
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
/// identity for the `schema_nodes`/`schema_edges` graph (issue #74,
/// ADR-0007) — the single resolution [`create_definition_inner`] and
/// [`create_relationship`] now both call, early, before touching the graph
/// at all: its two `resolve_node_in_txn` calls, [`reject_if_table_cycle`],
/// and (for [`create_definition_inner`] specifically) the row persisted to
/// `source_table_versions`/`transform_definitions.source_table` all share
/// this one qualified value rather than each re-deriving their own.
///
/// Two-step, mirroring the two ways a bare name can already be real:
///
/// 1. The common case: `table` physically exists, so
///    [`resolve_source_schema_in_txn`]'s `search_path` walk finds it
///    directly — same resolution issue #72 already used for
///    `qualified_source`.
/// 2. `table` instead names another definition's own persisted target
///    (issue #73's `transform_definitions.target_table`) — a legitimate
///    chained `FROM`/relationship endpoint, but not guaranteed to be backed
///    by a physical table at the instant this call names it: the
///    catalog-only ring-path entry points
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
/// *before* [`reject_if_table_cycle`] without regressing the one case the
/// old bare-keyed code's ordering used to protect by construction: a
/// definition that both (a) closes a cycle and (b) names a not-yet-
/// materialized chained source as its `FROM` must still report
/// [`ValidationError::TableCycle`], not a confusing not-found error. Before
/// issue #74, that was guaranteed by running the (bare, existence-free)
/// cycle check *before* ever attempting (bare-name) qualification. Now that
/// the graph itself requires qualified identity, qualification has to run
/// first — so it's this function's step 2, not error-ordering, that keeps
/// that guarantee: resolution itself succeeds for a not-yet-materialized
/// chained target, so the cycle check that runs after it is reached at all,
/// and reports the cycle exactly as before.
///
/// Only ever call this on a bare name freshly parsed from a definition's or
/// relationship's own source text (same restriction as
/// [`resolve_source_schema_in_txn`]) — never on a value already read back
/// from a qualified column.
async fn resolve_graph_identity_in_txn(
    txn: &tokio_postgres::Transaction<'_>,
    table: &str,
) -> Result<String, CatalogError> {
    match resolve_source_schema_in_txn(txn, table).await {
        Ok(schema) => Ok(crate::intake::publication::qualify(&schema, table)?),
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

/// Best-effort counterpart to [`resolve_graph_identity_in_txn`], for
/// [`create_relationship`]'s own pg_catalog introspection
/// (`column_type_in_txn`/`to_col_cardinality_in_txn`/
/// `has_usable_fk_index_in_txn`/`assert_replica_identity_supports_to_many`) —
/// reviewer follow-up to issue #74 (epic #78's own whole-branch review). Those
/// four resolve `def.from_table`/`def.to_table` via `pg_catalog.to_regclass`,
/// which — like [`resolve_source_schema_in_txn`]'s own walk — only ever
/// considers *this connection's* `search_path`, so a relationship endpoint
/// that bare-names another definition's target explicitly qualified into a
/// non-default schema (issue #76) needs the exact same bare-target-suffix
/// fallback `create_definition_inner`'s `qualified_source` already gets.
///
/// Unlike [`resolve_graph_identity_in_txn`] itself, a *total* miss (neither a
/// physical table nor a live definition's target) is not an error here — it
/// falls back to returning `table` unchanged. That matters for a genuinely
/// nonexistent endpoint: `column_type_in_txn` et al. below still run their
/// own `to_regclass`-based lookup against the same bare name Postgres itself
/// would have tried, so they still report their own precise
/// [`ValidationError::UnknownRelationshipColumn`]/[`ValidationError::RelationshipToManyRequiresReplicaIdentity`]
/// — naming the actual missing column/table exactly as before this fix —
/// rather than this function's own less specific
/// [`CatalogError::SourceTableNotFound`], which existing callers (e.g.
/// `an_unknown_from_column_is_rejected`) don't expect and which wouldn't say
/// anything about *which* endpoint or column is the problem. The two-step
/// fallback only ever swaps in a qualified identity when doing so can
/// actually resolve something; it never turns one "not found" into a worse
/// one.
///
/// Also doubles as this function's *only* graph-identity resolution for
/// [`create_relationship`] (see that function's own call site): once
/// `column_type_in_txn` has confirmed the qualified/bare identity this
/// returns is real, the same value is reused for
/// [`resolve_node_in_txn`]/[`reject_if_table_cycle`] rather than re-resolving
/// a second time — safe because the bare-fallback case above can only be
/// reached when `table` doesn't resolve at all, which always fails one of
/// those pg_catalog checks first and returns before either graph call is
/// ever reached.
async fn resolve_relationship_endpoint_in_txn(
    txn: &tokio_postgres::Transaction<'_>,
    table: &str,
) -> Result<String, CatalogError> {
    match resolve_graph_identity_in_txn(txn, table).await {
        Ok(qualified) => Ok(qualified),
        Err(CatalogError::SourceTableNotFound(_)) => Ok(table.to_string()),
        Err(err) => Err(err),
    }
}

/// Pooled (non-transaction) counterpart to
/// [`resolve_relationship_endpoint_in_txn`] — same best-effort resolution
/// (falls back to `table` unchanged on a total miss, rather than erroring),
/// via [`resolve_graph_identity`] instead of the txn-scoped
/// [`resolve_graph_identity_in_txn`]. [`relationship_on_source`] is the one
/// caller: it resolves a caller-supplied bare source the way the
/// `RELATIONSHIP` statement resolved its from-table. A persisted relationship's
/// endpoints are never re-resolved through here; each carries its recorded
/// schema ([`RelationshipDefinition::qualified_from_table`]/
/// [`RelationshipDefinition::qualified_to_table`], issues #288/#372).
async fn resolve_relationship_endpoint(pool: &Pool, table: &str) -> Result<String, CatalogError> {
    match resolve_graph_identity(pool, table).await {
        Ok(qualified) => Ok(qualified),
        Err(CatalogError::SourceTableNotFound(_)) => Ok(table.to_string()),
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
/// here, the bare-target-suffix fallback. (Reverse recompute's from-side used
/// to come through here too, re-resolving a relationship's bare `from_table`;
/// since issue #288 it uses the relationship's own recorded schema,
/// [`super::RelationshipDefinition::qualified_from_table`], instead.)
pub(crate) async fn resolve_graph_identity(
    pool: &Pool,
    table: &str,
) -> Result<String, CatalogError> {
    match resolve_source_schema(pool, table).await {
        Ok(schema) => Ok(crate::intake::publication::qualify(&schema, table)?),
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

/// The target schema `def` actually resolves against (issue #76, ADR-0007
/// grammar clause 4): an explicit `TRANSFORM <schema>.<target>` spelling
/// ([`TransformDef::explicit_target_schema`]) overrides `target_schema`
/// (`Config::target_schema`, or the caller's own override) outright — a
/// qualified spelling names its own schema, it doesn't inherit the
/// configured default. `None` (the bare, common case) keeps using
/// `target_schema` exactly as issue #73 already did.
fn effective_target_schema<'a>(def: &'a TransformDef, target_schema: &'a str) -> &'a str {
    def.explicit_target_schema
        .as_deref()
        .unwrap_or(target_schema)
}

/// Pooled (non-transaction) counterpart to [`resolve_source_schema_in_txn`],
/// for [`install_definition`]'s own DDL/direct-build steps ([`ddl::source_primary_key`],
/// [`ddl::target_table_ddl`], [`backfill::backfill_definition`]), which
/// run on plain pooled connections
/// before that function's own [`create_definition_inner`] call opens a
/// transaction and computes its own, independent, authoritative copy —
/// mirrors [`column_type`]/[`column_type_in_txn`]'s same pool-vs-txn split.
/// Same `search_path` walk, same [`CatalogError::SourceTableNotFound`] on no
/// match. Also covers [`install_definition`]'s issue #55 fence check, which
/// likewise runs before any transaction of its own is open.
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

/// The fully-qualified source [`install_definition`]'s own DDL/direct-build
/// steps read from (issue #76, ADR-0007 grammar clause 4) — computed once,
/// early in that function, exactly like `target_schema`/[`effective_target_schema`]
/// immediately above it, and threaded through every one of those steps
/// (`ddl::source_primary_key`, `ddl::target_table_ddl`,
/// `backfill::backfill_definition`) so none of them can independently
/// re-derive a different answer, and so every physical SQL builder among them
/// emits the qualified identity rather than a bare `def.source` left to the
/// executing connection's own `search_path` — the gap a reviewer flagged
/// against issue #76's own new explicit-schema grammar (ADR-0007's whole
/// point: "every generated statement emits qualified names... never a bare
/// name resolved against whatever search_path the executing session happens
/// to carry").
///
/// `create_definition_inner` (further below) computes its *own* copy inside
/// its own transaction rather than receiving this one as a parameter — see
/// that function's doc comment on `qualified_source` for why: it's the sole,
/// authoritative resolution the ring-path entry points ([`create_definition`]/
/// [`create_definition_without_backfill`], which never call this function)
/// ever get, so it must stand on its own regardless of what this function
/// computed a few statements earlier. The two are expected to agree (same
/// source text, same connection pool, no concurrent DDL moving `def.source`
/// between the two calls) — matching this same function's caller's own
/// fail-fast explicit-schema check just above it, also redundant-but-harmless
/// against `create_definition_inner`'s copy.
///
/// Reviewer follow-up to issue #74 (epic #78's own whole-branch review): the
/// bare (`None`) branch used to call [`resolve_source_schema`] directly — a
/// plain `search_path` walk with no fallback — even though
/// `create_definition_inner`'s own resolution of the exact same bare
/// `def.source` (its `qualified_source`, above) has carried issue #74's
/// bare-target-suffix fallback ([`resolve_graph_identity`]) since that issue
/// landed. That gap meant *this*, `install_definition`'s own "far more
/// common" entry point (see its own doc comment), never actually got the
/// fallback in practice: this function's DDL/direct-build steps run and can
/// fail *before* `create_definition_inner` is ever reached (only the plain
/// 1-1 path reaches it at all, and only after this qualification already
/// succeeded), so a bare `FROM <name>` chained off another definition's
/// target explicitly qualified into a non-default schema (issue #76) failed
/// here first, as a plain "not found on the search path" — never getting the
/// chance to resolve the way the ring path always could. Switched to
/// [`resolve_graph_identity`] itself, the same pooled two-step resolution
/// `staging::apply::compute`'s `qualified_schema_node_key` already reuses,
/// rather than re-implementing the fallback a third time.
async fn resolve_source_for_install(
    pool: &Pool,
    def: &TransformDef,
) -> Result<String, CatalogError> {
    match &def.explicit_source_schema {
        Some(schema) => Ok(crate::intake::publication::qualify(schema, &def.source)?),
        None => resolve_graph_identity(pool, &def.source).await,
    }
}

/// Confirms `schema.table` is a real relation (issue #76, ADR-0007 grammar
/// clause 4) — the existence check an explicitly-qualified `FROM`/`TRANSFORM`
/// reference gets *instead of* [`resolve_source_schema_in_txn`]'s
/// `search_path` walk: a qualified spelling names its schema directly, so
/// this checks that one relation is real rather than asking which schema on
/// the path would have won. Same `information_schema.tables` table that
/// function itself queries, just filtered to the one named schema instead of
/// `current_schemas(false)`.
async fn confirm_qualified_table_exists_in_txn(
    txn: &tokio_postgres::Transaction<'_>,
    schema: &str,
    table: &str,
) -> Result<bool, CatalogError> {
    let exists: bool = txn
        .query_one(
            "select exists (select 1 from information_schema.tables \
             where table_schema = $1 and table_name = $2)",
            &[&schema, &table],
        )
        .await?
        .get(0);
    Ok(exists)
}

/// Pooled (non-transaction) counterpart to
/// [`confirm_qualified_table_exists_in_txn`], for [`install_definition`]'s
/// own fail-fast pass — run on a plain connection, before that function opens
/// any transaction or runs any DDL, mirroring [`column_type`]/
/// [`column_type_in_txn`]'s same pool-vs-txn split.
async fn confirm_qualified_table_exists(
    pool: &Pool,
    schema: &str,
    table: &str,
) -> Result<bool, CatalogError> {
    let client = pool.get().await?;
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

/// Pooled (non-transaction) counterpart to [`column_type_in_txn`], for
/// resolvers that run before `create_definition` opens its transaction (issue
/// #40's [`resolve_relationships`]). Same query, same
/// [`ValidationError::UnknownRelationshipColumn`] on a missing column.
///
/// `query_table`/`display_table` split (4th-gap fix, reviewer follow-up to
/// issue #74, epic #78's own whole-branch review): mirrors
/// [`column_type_in_txn`]'s own split, added for [`create_relationship`]'s
/// pg_catalog checks — same reasoning applies here verbatim.
/// [`resolve_relationships`] passes [`resolve_relationship_endpoint`]'s
/// result as `query_table` (schema-qualified whenever that resolution needed
/// to be, so `to_regclass` can find a to-side explicitly qualified into a
/// non-default schema) but always passes the original, bare
/// `reldef.def.to_table` as `display_table`, so a reported
/// [`ValidationError::UnknownRelationshipColumn`] still names the table
/// exactly as the relationship's own source text did.
///
/// Renamed from `column_type` (issue #108): its one caller
/// ([`resolve_relationships`]) only ever fed the result straight into
/// `value_type_from_pg`'s `format_type`-text matching, which is exactly the
/// `_ => Text` fallthrough this issue replaces. Selecting the raw
/// `atttypid` OID instead — and classifying it via
/// [`super::pg_type::value_type_for_oid`] — sidesteps that matching (and its
/// `(...)` modifier-stripping) entirely, since a type's OID doesn't vary
/// with `numeric(10,2)` vs. `numeric`'s modifier the way its `format_type`
/// text does.
///
/// Takes an already-acquired `client` rather than a `Pool` (issue #117
/// review): [`resolve_relationships`] now also needs a connection for
/// [`super::pg_type::value_type_for_oid`] and acquires one once per
/// relationship for that; this used to independently call `pool.get()`
/// per column on top of that, holding *two* connections from the same pool
/// at once for the whole inner loop — exactly the "classic pool-exhaustion
/// deadlock" [`source_primary_key_in_txn`]'s own doc comment warns about
/// elsewhere in this crate, just not inside an explicit transaction here.
/// Reusing the caller's client removes the second checkout entirely.
async fn column_type_oid(
    client: &impl GenericClient,
    query_table: &str,
    display_table: &str,
    column: &str,
) -> Result<u32, CatalogError> {
    let row = client
        .query_opt(
            "select a.atttypid
             from pg_attribute a
             where a.attrelid = pg_catalog.to_regclass($1)
               and a.attname = $2
               and a.attnum > 0
               and not a.attisdropped",
            &[&query_table, &column],
        )
        .await?;
    match row {
        Some(row) => Ok(row.get(0)),
        None => Err(ValidationError::UnknownRelationshipColumn {
            table: display_table.to_string(),
            column: column.to_string(),
        }
        .into()),
    }
}

/// The Postgres type of `table.column`, as rendered by `format_type`, via a
/// bound `::regclass` cast (matching [`super::ddl::source_primary_key`]'s
/// convention) rather than string-interpolating either name into the query.
/// Distinguishes "the table itself doesn't resolve" from "the table exists
/// but has no such column" only in that both are reported the same way
/// (issue #27 doesn't need the distinction: either one means the endpoint
/// isn't real) — see [`ValidationError::UnknownRelationshipColumn`].
///
/// `query_table` and `display_table` deliberately differ (reviewer follow-up
/// to issue #74, epic #78's own whole-branch review): [`create_relationship`]
/// passes [`resolve_relationship_endpoint_in_txn`]'s result as `query_table`
/// — schema-qualified whenever that resolution needed to be, so
/// `to_regclass` can find a target explicitly qualified into a non-default
/// schema — but always passes the original, bare `def.from_table`/
/// `def.to_table` as `display_table`, so a reported
/// [`ValidationError::UnknownRelationshipColumn`] names the table exactly as
/// the relationship's own source text did, unchanged from before this fix,
/// rather than leaking this function's internal schema-qualified resolution
/// into user-facing error text for the ordinary (already-on-`search_path`)
/// case.
async fn column_type_in_txn(
    txn: &tokio_postgres::Transaction<'_>,
    query_table: &str,
    display_table: &str,
    column: &str,
) -> Result<String, CatalogError> {
    let row = txn
        .query_opt(
            "select pg_catalog.format_type(a.atttypid, a.atttypmod)
             from pg_attribute a
             where a.attrelid = pg_catalog.to_regclass($1)
               and a.attname = $2
               and a.attnum > 0
               and not a.attisdropped",
            &[&query_table, &column],
        )
        .await?;
    match row {
        Some(row) => Ok(row.get(0)),
        None => Err(ValidationError::UnknownRelationshipColumn {
            table: display_table.to_string(),
            column: column.to_string(),
        }
        .into()),
    }
}

/// Postgres type names that are freely joinable despite not being textually
/// identical — the common case of an identity primary key (`bigint`) and a
/// foreign key column declared as a plain `integer`, or a `text`/`character
/// varying` split between two independently-authored tables. Anything not
/// named here must match `from_type`/`to_type` exactly to be considered
/// comparable; see [`assert_comparable_types`].
///
/// `pg_type` is [`column_type_in_txn`]'s `format_type(atttypid, atttypmod)`
/// rendering, which includes any length/precision modifier (`character
/// varying(255)`, `numeric(10,2)`). The modifier is stripped before bucket
/// matching — otherwise `varchar(255)` and `varchar(100)`, or `text` and
/// `varchar(n)`, would fall into the `other` catch-all as two distinct
/// strings and be wrongly rejected as a type mismatch, even though they're
/// exactly the kind of join this function exists to allow.
fn type_family(pg_type: &str) -> Cow<'_, str> {
    let base = base_type_name(pg_type);
    match base.as_ref() {
        "smallint" | "integer" | "bigint" => Cow::Borrowed("integer"),
        "numeric" | "real" | "double precision" => Cow::Borrowed("numeric"),
        "text" | "character varying" | "character" => Cow::Borrowed("text"),
        _ => base,
    }
}

/// Strips a `format_type` rendering's `(...)` type modifier, wherever it
/// sits, leaving the bare type name: `character varying(255)` and
/// `numeric(10,2)` become `character varying` and `numeric`, and
/// `timestamp(3) without time zone` becomes `timestamp without time zone`.
///
/// That last case is why this is a function rather than the
/// `split('(').next()` both [`type_family`] and
/// [`is_text_stable_join_key_type`] used before issue #113. Every type name
/// they had to handle until then carried its modifier as a *suffix*, so
/// truncating at the first `(` was equivalent. The SQL-standard temporal
/// names do not: `format_type` renders a `timestamp(3)` column as
/// `timestamp(3) without time zone`, which truncates to `timestamp` — a
/// string matching neither the unmodified `timestamp without time zone` in
/// [`TEXT_STABLE_JOIN_KEY_TYPES`] nor the same column declared without a
/// precision. Left alone, a sub-second-precision `timestamp` key would have
/// been silently refused and a `timestamp(3)`/`timestamp` join pair wrongly
/// reported as a type mismatch.
///
/// A precision modifier never affects text-stability, incidentally: it
/// rounds on *input* (`'12:00:00.5678'::time(2)` stores `12:00:00.57`), and
/// what is stored still renders canonically.
fn base_type_name(pg_type: &str) -> Cow<'_, str> {
    let trimmed = pg_type.trim();
    let Some(open) = trimmed.find('(') else {
        return Cow::Borrowed(trimmed);
    };
    let Some(close) = trimmed[open..].find(')').map(|offset| open + offset) else {
        return Cow::Borrowed(trimmed);
    };
    let head = trimmed[..open].trim_end();
    let tail = trimmed[close + 1..].trim_start();
    if tail.is_empty() {
        return Cow::Borrowed(head);
    }
    Cow::Owned(format!("{head} {tail}"))
}

/// Postgres type base names (modifier already stripped, as in
/// [`type_family`]) whose equality is *text-stable* — `a::text = b::text`
/// agrees with the type's native typed `=` for every value. This is a
/// positive allowlist, not [`type_family`]'s equivalence-class bucketing:
/// [`type_family`] groups `character`/`character varying`/`text` together
/// (correctly, for comparability) even though `character`'s native `=` is
/// blank-padding-insensitive while its `::text` rendering is blank-padded,
/// so a family-based check would wrongly wave it through here.
///
/// Only the join key's *own* type matters for this list, not what it's
/// compared against, so allowed/rejected status is a per-type fact.
/// Anything not named here — `numeric` (`1.0::text` != `1.00::text` though
/// numerically equal), `real`/`double precision` (issue #112: `-0` and `0`
/// are `=` in Postgres but render as `'-0'` and `'0'`, so text matching
/// splits one value across two keys; verified on a live server, and the
/// unlock is #110's typed key index comparing decoded values through
/// `crate::float::compare`, not a new encoding), `character`/`citext`
/// (blank-padding or
/// case-insensitivity native to the type but not its `::text` form),
/// `interval` (issue #113 admitted `timestamp` alongside `date`/`time`/
/// `timetz` once issue #248 fixed its render-consistency defect, and
/// `timestamptz` joined them too once issue #246 fixed the *walsender's*
/// own render-consistency gap; see the temporal block below for why
/// `interval` alone stays off), `boolean`, `json`/`jsonb`, or any unknown
/// type — is rejected as a join key.
///
/// `bytea` (issue #114) joins the list below on the same "text-stability is
/// a property of the rendering" reasoning `oid` and four of the six temporal
/// families were admitted under — `docs/type-support.md` had it marked
/// `🎯 typed index`, and that turned out to be exactly the wrong prediction
/// again. `byteaout` under the already-pinned `bytea_output = 'hex'`
/// (`crate::pool::DETERMINISTIC_TEXT_OUTPUT_GUCS`) is a bijection: every
/// distinct byte string has exactly one canonical `\x`-prefixed, lowercase,
/// even-length hex spelling, and every such spelling names exactly one byte
/// string — there is no `bytea` analogue of float's `-0`/`0` or interval's
/// `'1 day'`/`'24 hours'`, because a fixed-width positional encoding cannot
/// produce two spellings of the same value. Verified on a live server: a
/// grid spanning the empty value, embedded `NUL` bytes, and every byte value
/// from `0x00` to `0xff` produces exactly as many distinct `::text` groups as
/// distinct values (`defs_bytea.rs`'s
/// `bytea_text_rendering_is_a_bijection_under_hex_output`). The issue's own
/// scope note ("key role needs decoded comparison") assumed #110's typed key
/// index was the prerequisite the way #113's temporal block once assumed it
/// for the whole family; asked of a live server per #111's playbook, it is
/// not — pinning `bytea_output` alone already makes raw `::text` matching
/// agree with `bytea`'s native `=` for every value.
const TEXT_STABLE_JOIN_KEY_TYPES: &[&str] = &[
    "smallint",
    "integer",
    "bigint",
    // Issue #111: `oid` is an *unsigned* 32-bit integer whose `oid_out`
    // rendering is canonical decimal — no sign, no leading zeros, no
    // padding — so `a::text = b::text` agrees with `oid`'s native `=` for
    // every value, exactly as it does for the three signed widths above.
    // It is not a `ValueType::Integer` (Postgres gives it no arithmetic at
    // all; see `pg_type::PgType::Oid`), but text-stability is a property of
    // the *rendering*, not of the operator set, so it belongs here.
    "oid",
    "uuid",
    "text",
    "character varying",
    // Issue #113: four of the six temporal families, on the same
    // "text-stability is a property of the rendering" reasoning `oid` was
    // admitted under. `docs/type-support.md` had all six marked `🎯 typed
    // index`, assuming #110's typed key index was the prerequisite; for
    // these four it is not, and the evidence is per-family rather than
    // per-block — see `crate::temporal`'s module doc for the live queries.
    //
    // `date_out` under the already-pinned `DateStyle = 'ISO, YMD'`
    // (`crate::pool::DETERMINISTIC_TEXT_OUTPUT_GUCS`) is a bijection on its
    // values: the year field widens (`5874897-12-31`), the era is an
    // explicit ` BC` suffix, and `infinity`/`-infinity` have their own
    // spellings. `time_out` and `timetz_out` need no GUC at all — verified
    // identical under `ISO`, `SQL`, `Postgres` and `German` `DateStyle`s
    // and under three session `TimeZone`s.
    //
    // `timetz` is the one to double-take on, and it is safe for a reason
    // opposite to the obvious one: its `=` is *narrower* than "same instant
    // of day", not wider. `select '12:00:00+00'::timetz =
    // '17:30:00+05:30'::timetz` is **false** — `timetz_cmp_internal` sorts
    // by GMT-equivalent time and then by zone, so equality is identity on
    // the stored `(time, zone)` pair, which is exactly what `timetz_out`
    // prints.
    //
    // `timestamp without time zone` is a bijection under `::text` too, and
    // it used to be refused anyway: `::text` was not the engine's only
    // renderer. The live-row reads (`staging::apply`'s
    // `read_live_rows_batch`/`fetch_to_side_rows`/
    // `fetch_relationship_projection_rows`, among others) used to build row
    // bodies with `to_jsonb(t.*)`, which renders a `timestamp` through
    // `jsonb`'s ISO-8601 writer: `2024-06-15T12:34:56`, not
    // `2024-06-15 12:34:56`. A key seeded once each way became two target
    // rows for one Postgres group. Issue #248 fixed that by replacing every
    // such `to_jsonb(t.*)` call site with an explicit per-column
    // `jsonb_build_object` rendered the same way `::text` is
    // (`staging::apply::row_as_text_jsonb_sql`), which is why `timestamp`
    // now joins `date`/`time`/`timetz` here — see
    // `crate::temporal::is_render_consistent`.
    //
    // One remaining deliberate absence:
    //
    // * `interval` — `'24 hours'` and `'1 day'` are one value with two
    //   renderings. The float `-0`/`0` defect; no GUC and no renderer
    //   reconciliation fixes it, though #110's typed key index would.
    "date",
    "time without time zone",
    "time with time zone",
    "timestamp without time zone",
    // Issue #246: `timestamp with time zone` joins the four above.
    // `timestamptz_out` renders the stored instant in the session's
    // `TimeZone`, so it needed two independent fixes before it could be
    // text-stable — issue #248's `to_jsonb`/`::text` reconciliation (which
    // it shares with `timestamp`) and, separately, `TimeZone` itself being
    // pinned identically on *every* connection Trellis opens, including the
    // walsender. `pgwire-replication` 0.4 had no way to send startup
    // parameters on a replication connection, so before #246 pinning
    // `TimeZone` on the pool alone would have guaranteed a pool/walsender
    // disagreement on any non-UTC server — worse than the accidental
    // agreement the two renderers had by both falling back to the server
    // default. `pgwire-replication` 0.4.1's `ReplicationConfig::with_options`
    // closes that: `crate::intake::IntakeConfig::replication_config` now
    // pins the walsender to `crate::pool::DETERMINISTIC_TEXT_OUTPUT_GUCS`
    // (which now includes `TimeZone = 'UTC'`) the same way the pool always
    // has, so `timestamptz_out` is a genuine bijection on the instant on
    // both backends. See `crate::temporal::is_bijective_under_text`/
    // `is_render_consistent` for the per-family verdict this list mirrors.
    "timestamp with time zone",
    // Issue #114: `byteaout` under the pinned `bytea_output = 'hex'` is a
    // bijection on its values — see the doc comment above for the live
    // evidence. `bytea` has no length/precision modifier, so
    // `base_type_name` never has anything to strip for it.
    "bytea",
    // Issue #116: `cidr`/`macaddr`/`macaddr8` join the list; `inet` — despite
    // sharing `cidr`'s `pg_cast` row — deliberately does not. Full account in
    // `crate::netaddr`'s module doc, condensed here:
    //
    // `macaddr`/`macaddr8` have no `pg_cast` row for `text` at all (checked
    // the same way #119 checked `boolean`'s), so their `::text` *is*
    // `macaddr_out`/`macaddr8_out`, and both are bijections — every accepted
    // input spelling (colon/hyphen/dot-grouped/bare hex) normalizes on
    // output to one canonical lowercase colon-separated form.
    //
    // `inet` and `cidr` share one `pg_cast` row (`pg_catalog.text(inet)`,
    // `prosrc = network_show`, reused for `cidr` because its on-disk
    // representation *is* an `inet` with host bits forced to zero) — but
    // whether that second renderer actually diverges from the type's own
    // output function is a per-type fact, not a per-row one. `cidr_out`
    // never omits the netmask (a `cidr` value's whole point is that the
    // network prefix matters), so `cidr_out(v)::text = v::text` holds for
    // every value — verified live across a v4/v6 grid including the
    // host-bits-zero-only values `cidr_in` accepts. `inet_out` — what
    // CDC/`pgoutput` decodes, what `intake::extract_key` stores verbatim —
    // *does* diverge from `network_show`: it omits the `/prefixlen` suffix
    // exactly when the stored netmask covers the whole address
    // (`'192.168.1.5'::inet::text` via `inet_out` is `192.168.1.5`; via the
    // cast, `192.168.1.5/32`), the identical "one value, two renderers, no
    // arbiter" shape `boolean` has above — and `staging::apply::
    // check_reverse_guards` and its scalar siblings still do raw
    // `{col}::text = $1` matching, so `inet` would silently mismatch exactly
    // the way `boolean` would. `inet` is refused here for that reason;
    // its `GROUP BY` key role is still admitted, the same split `boolean`
    // got — see `validate::reject_unsupported_group_by_key_type`'s `Inet`
    // arm and `crate::netaddr::canonicalize_group_key_text`.
    "cidr",
    "macaddr",
    "macaddr8",
    // Issue #119: `boolean` is deliberately *not* here, and the reason is
    // new — every other admission/refusal on this list turns on whether
    // `<type>_out` (the type's own output function, what CDC/`pgoutput`
    // sends and what `<col>::text` normally reduces to) is a bijection.
    // `boolout` *is* one (`'t'`/`'f'`, nothing else). The problem is that
    // `<col>::text` does not call `boolout` at all: `select castfunc::regproc
    // from pg_cast where castsource = 'boolean'::regtype and casttarget =
    // 'text'::regtype` names `pg_catalog.text(boolean)`, a *second*,
    // dedicated cast function Postgres ships only for `boolean`, which
    // renders the SQL-standard `'true'`/`'false'` instead of `boolout`'s
    // `'t'`/`'f'`. Checked against every other type on this list
    // (`smallint`/`integer`/`bigint`/`oid`/`uuid`/`text`/`character
    // varying`/the four temporal families/`bytea`) and none has a
    // `pg_cast` row for `text` at all — their `::text` *is* their output
    // function, which is exactly why raw-text matching has been sound for
    // all of them. `boolean` is the only type on the block with a second,
    // independent renderer, and it is silent: nothing here fails to type
    // or parse, two spellings of one value just stop comparing equal.
    //
    // That is live-load-bearing, not theoretical: `intake::pgoutput`'s
    // tuple decoder stores a CDC-decoded column's wire text *verbatim*
    // (`ColumnValue::Text`, straight off the server's `boolout` call), and
    // `intake::extract_key` builds a relationship/primary-key's staged
    // `key` text directly from that — `'t'`/`'f'`. Every *bulk* key lookup
    // in `staging::apply` (`key_array_filter`, issue #125) casts the
    // *bound parameter* to the column's native type
    // (`col = any($1::text[]::boolean[])`), which calls `boolin` — an
    // input function far more permissive than `boolout` is strict, so it
    // parses `'t'`/`'f'` and `'true'`/`'false'` alike back to the same
    // native value and the divergence never surfaces there. But several
    // *scalar* single-key lookups (`staging::apply::check_reverse_guards`
    // and its siblings) still render the older, unindexed
    // `{key}::text = $1` form directly — comparing a live column's
    // `pg_catalog.text(boolean)` rendering (`'true'`/`'false'`) against a
    // CDC-derived `key` built from `boolout` (`'t'`/`'f'`) — and those two
    // strings are simply not equal. Admitting `boolean` here today would
    // silently mismatch exactly the shape #248 fixed for `timestamp`
    // (one value, two renderers, no arbiter), just from a different
    // renderer pair no earlier type-family issue (#111–#114) had reason to
    // find, since `boolean` is the only type with this second cast.
    //
    // The `GROUP BY` key role does *not* have this problem at the final SQL
    // layer — see `validate::reject_unsupported_group_by_key_type`'s own
    // `Boolean` arm — because `staging::apply_aggregate`'s keyset match
    // always goes through exactly the same native-array-cast pattern
    // `key_array_filter` uses (`unnest($1::text[]::boolean[], ...)`), never
    // a bare `col::text` comparison, so it never touches `pg_catalog.text
    // (boolean)` at all. (That role *did* have a second, in-memory-only
    // instance of this same defect, in `accumulate_changes`'s own
    // pre-database `GroupPlan` bucketing — fixed by
    // `apply_aggregate::canonicalize_group_key_part`, not by anything on
    // this list; see that function's doc comment.) Extending the SQL-layer
    // native-array-cast pattern to the remaining scalar lookup sites (or
    // #110's typed key index, which would subsume it) is what would let
    // `boolean` join this list; until then it stays off. See
    // `trellis/tests/defs_boolean.rs` for the live evidence and
    // `docs/type-support.md`'s "Boolean semantics" section.
    //
    // Issue #118: `bit` and `bit varying` join this list on the *original*
    // "text-stability is a property of the rendering" reasoning `oid`/
    // `bytea`/four of the six temporal families were admitted under —
    // checked against `pg_cast`, not assumed, exactly the way #119's review
    // taught this list to check every family going forward. `select
    // castfunc::regproc from pg_cast where castsource in
    // ('bit'::regtype, 'varbit'::regtype) and casttarget = 'text'::regtype`
    // returns **no rows at all** on a live Postgres 17: unlike `boolean`,
    // neither bit-string type has a second, dedicated `::text` cast — their
    // `::text` *is* `bit_out`/`varbit_out`, exactly like every other type on
    // this list except `boolean`. `bit_out`/`varbit_out` are `IMMUTABLE`,
    // read no GUC, and render a bit string as its literal `'0'`/`'1'`
    // characters with no separator, no padding beyond the value's own stored
    // bits, and no second spelling of any value — a fixed-alphabet,
    // fixed-width-per-symbol encoding has no `bytea`-hex-style ambiguity to
    // begin with, so it needs no positional-encoding argument the way
    // `bytea`'s bijection proof did either.
    //
    // Both spellings are admitted *here* — this role never asks a bare
    // `ValueType` to declare a brand-new column, it only ever compares an
    // already-existing one (`{col} = any($1::text[]::{ty}[])`, via
    // `staging::apply`'s `key_column_pg_type`, which introspects the
    // column's own concrete `format_type` — `bit(5)`, not bare `bit` — so a
    // fixed-length key's own width is never lost) or, for a 1-1 primary key,
    // copies the source's concrete introspected type verbatim
    // (`ddl::source_primary_key`). Fixed-length `bit` is *not* admitted as a
    // `GROUP BY` key for a completely different, DDL-only reason — see
    // `validate::reject_unsupported_group_by_key_type`'s `VarBit` arm.
    "bit",
    "bit varying",
    // Issue #115: `jsonb` is deliberately **absent**, and — unlike
    // `boolean`/`inet` — not because of a second, disagreeing `::text`
    // renderer: `select castfunc from pg_cast where castsource =
    // 'jsonb'::regtype and casttarget = 'text'::regtype` returns no rows,
    // so `::text` is `jsonb_out` directly, and `jsonb_out` genuinely
    // canonicalizes object key order (`{"b":1,"a":2}` and `{"a":2,"b":1}`
    // both render `{"a": 2, "b": 1}`). The hazard is one level deeper: an
    // embedded JSON *number* preserves its literal input scale the way
    // `numeric` itself does, so `'{"a":1}'::jsonb` and
    // `'{"a":1.0}'::jsonb` are `=` (verified live) but render as
    // `{"a": 1}` and `{"a": 1.0}` — one value, two renderings, the same
    // equivalence-class shape `real`/`double precision`'s `-0`/`0` and
    // `interval`'s `'1 day'`/`'24 hours'` have. See `crate::jsonb`'s
    // module doc for the full live evidence and `trellis/tests/
    // defs_jsonb.rs`. The unlock is #110's typed key index, exactly as it
    // is for `numeric`/`real`/`double precision`/`timestamptz` above —
    // `jsonb` needs no jsonb-specific mechanism, just the same one every
    // other numeric-bearing family here is waiting on.
];

/// [`TEXT_STABLE_JOIN_KEY_TYPES`] rendered for a user-facing error message,
/// so the "supported join key types are ..." list in
/// [`ValidationError::RelationshipUnsupportedJoinKeyType`]'s `Display`
/// cannot drift out of step with the allowlist it describes.
///
/// It had drifted: the message still named only the pre-#111 six long
/// after `oid` and the temporal families were admitted, which is exactly
/// the failure mode a hand-maintained second copy has.
pub(crate) fn supported_join_key_types() -> String {
    TEXT_STABLE_JOIN_KEY_TYPES.join(", ")
}

/// Whether `pg_type` (a `format_type` rendering, e.g. `integer`, `character
/// varying(255)`) is in [`TEXT_STABLE_JOIN_KEY_TYPES`] — the single source of
/// truth both [`assert_join_key_type_supported`] (relationship join keys,
/// issue #28) and [`super::ddl::source_primary_key`] (1-1 transform primary
/// keys, issue #107) gate on. Any modifier (`(255)`, `(10,2)`, `(3)`) is
/// stripped by [`base_type_name`] before matching, so `varchar(255)` and
/// `varchar(100)` are both recognized as `character varying` — and
/// `timestamp(3) without time zone` as `timestamp without time zone` —
/// rather than falling through to the catch-all rejection as distinct
/// strings.
pub(crate) fn is_text_stable_join_key_type(pg_type: &str) -> bool {
    let base = base_type_name(pg_type);
    TEXT_STABLE_JOIN_KEY_TYPES.contains(&base.as_ref())
}

/// The dynamic counterpart to [`TEXT_STABLE_JOIN_KEY_TYPES`]/
/// [`is_text_stable_join_key_type`], for issue #117's enum types: `pg_type`
/// (a `format_type` rendering, the same string [`is_text_stable_join_key_type`]
/// tests) cannot be enumerated in a fixed list the way every other family
/// here can, because `CREATE TYPE ... AS ENUM` mints an arbitrary,
/// per-schema type with a dynamically-assigned OID — there is no fixed set
/// of enum type names to write down ahead of time. `to_regtype` resolves
/// `pg_type` the same way Postgres itself would resolve any other type name
/// reference (honoring `search_path`/schema-qualification), returning `NULL`
/// rather than erroring for anything it can't resolve as a bare type name —
/// including a modifier-bearing rendering like `numeric(10,2)` or an array
/// type like `text[]`, neither of which is a bare name `to_regtype` accepts
/// — so a `false` result here is always safe: it only ever means "not a
/// live enum type", never a hard error, for any input the static allowlist
/// above already rejects.
///
/// Admitting the result is safe on the same "text-stability is a property
/// of the rendering, not of a fixed list" grounds #111/#114/#118 each
/// established for their own families: `enumout` (what `<col>::text` calls
/// — verified live against `pg_cast`, `trellis/tests/defs_enum.rs`) renders
/// each value as its own label text verbatim, with no separator, no
/// escaping, and no GUC dependence. Unlike `bytea`'s hex encoding or
/// `bit`'s fixed alphabet, an enum value's identity *is* its label text (a
/// `pg_enum` row keyed by OID, `enumlabel` the payload) — there is no
/// deeper representation for two distinct values to collide under, so this
/// is a bijection by construction, not merely by the absence of a
/// discovered counterexample.
pub(crate) async fn is_enum_type_name(
    client: &impl GenericClient,
    pg_type: &str,
) -> Result<bool, tokio_postgres::Error> {
    let base = base_type_name(pg_type);
    let row = client
        .query_one(
            "select coalesce( \
                 (select typtype = 'e' from pg_catalog.pg_type where oid = to_regtype($1)), \
                 false \
             )",
            &[&base.as_ref()],
        )
        .await?;
    Ok(row.get(0))
}

/// Rejects `def` if either endpoint's join key type isn't
/// [`text-stable`](is_text_stable_join_key_type) (or, issue #117, a live
/// enum type — [`is_enum_type_name`]) — issue #28 review, hardened
/// per review of #27/#28 (a numeric-only blocklist missed `character(n)`,
/// `citext`, and `timestamptz`, which also diverge under the engine's
/// `::text`-equality join vs. the Postgres oracle's native typed `=`). Checks
/// both sides rather than relying on [`assert_comparable_types`]'s family
/// match to stand in for the other: `character` and `character varying`
/// share a family but only one is on this allowlist, so a from/to pair could
/// straddle the line.
async fn assert_join_key_type_supported(
    txn: &tokio_postgres::Transaction<'_>,
    def: &RelationshipDef,
    from_type: &str,
    to_type: &str,
) -> Result<(), CatalogError> {
    for (table, column, pg_type) in [
        (&def.from_table, &def.from_col, from_type),
        (&def.to_table, &def.to_col, to_type),
    ] {
        if !is_text_stable_join_key_type(pg_type) && !is_enum_type_name(txn, pg_type).await? {
            return Err(ValidationError::RelationshipUnsupportedJoinKeyType {
                name: def.name.clone(),
                table: table.clone(),
                column: column.clone(),
                pg_type: pg_type.to_string(),
            }
            .into());
        }
    }
    Ok(())
}

/// Rejects `def` if `from_type`/`to_type` (both already resolved by
/// [`column_type_in_txn`]) aren't in the same [`type_family`] — ADR-0006's
/// "type-check the join" requirement.
fn assert_comparable_types(
    def: &RelationshipDef,
    from_type: &str,
    to_type: &str,
) -> Result<(), CatalogError> {
    if type_family(from_type) == type_family(to_type) {
        return Ok(());
    }
    Err(
        ValidationError::RelationshipTypeMismatch(Box::new(RelationshipTypeMismatch {
            name: def.name.clone(),
            from_table: def.from_table.clone(),
            from_col: def.from_col.clone(),
            from_type: from_type.to_string(),
            to_table: def.to_table.clone(),
            to_col: def.to_col.clone(),
            to_type: to_type.to_string(),
        }))
        .into(),
    )
}

/// [`RelationshipCardinality::ToOne`] iff `to_col` is the sole column of a
/// `PRIMARY KEY` or `UNIQUE` index on `to_table`, introspected live against
/// `pg_catalog` (`pg_index.indisunique` covers both index kinds; `indkey`'s
/// length excludes any multi-column index `to_col` merely participates in,
/// since that doesn't make `to_col` alone unique) — ADR-0006's cardinality
/// rule. `indisvalid`/`indpred is null` exclude indexes that don't actually
/// guarantee global uniqueness of `to_col`: a not-yet-validated index (e.g.
/// left behind by a failed `CREATE UNIQUE INDEX CONCURRENTLY`) or a partial
/// unique index (`... where active`), which only constrains the rows it
/// covers. Assumes `to_table`/`to_col` already resolved (callers run this
/// after [`column_type_in_txn`] has confirmed both exist) — [`create_relationship`]
/// passes its own [`resolve_relationship_endpoint_in_txn`] result, not
/// `def.to_table` directly (reviewer follow-up to issue #74), so a to-side
/// explicitly qualified into a non-default schema resolves here too.
async fn to_col_cardinality_in_txn(
    txn: &tokio_postgres::Transaction<'_>,
    to_table: &str,
    to_col: &str,
) -> Result<RelationshipCardinality, CatalogError> {
    let is_unique: bool = txn
        .query_one(
            "select exists (
                select 1
                from pg_index i
                join pg_attribute a
                  on a.attrelid = i.indrelid and a.attname = $2
                where i.indrelid = pg_catalog.to_regclass($1)
                  and i.indisunique
                  and i.indisvalid
                  and i.indpred is null
                  and array_length(i.indkey::int2[], 1) = 1
                  and i.indkey[0] = a.attnum
             )",
            &[&to_table, &to_col],
        )
        .await?
        .get(0);

    Ok(if is_unique {
        RelationshipCardinality::ToOne
    } else {
        RelationshipCardinality::ToMany
    })
}

/// Rejects a *to-many* relationship (issue #41) whose to-side lacks a replica
/// identity that carries the join column (`to_col`) in row pre-images. For
/// to-many the join key is a *non-PK* column, and the staging reverse-recompute
/// resolver reads it from a DELETE/UPDATE pre-image to find which from-side
/// rows to re-derive. Under the default replica identity (`d`, the primary
/// key) — or none (`n`) — that non-PK column is absent from the pre-image, so
/// a delete or re-parent would silently under-recompute and diverge from the
/// Postgres oracle. Correct only when the to-side has:
/// * `REPLICA IDENTITY FULL` (`relreplident = 'f'`) — every column is in the
///   pre-image; or
/// * `REPLICA IDENTITY USING INDEX` (`relreplident = 'i'`) whose index — the
///   one flagged `pg_index.indisreplident` — includes `to_col` among its
///   columns (`indkey` maps to the attnum of `to_col`).
///
/// Callers invoke this only for [`RelationshipCardinality::ToMany`]; to-one
/// carries the FK in the from-side's own row image and needs no extra replica
/// identity (see ADR-0006). Assumes `to_table`/`to_col` already resolved.
///
/// `to_table` — [`create_relationship`]'s own [`resolve_relationship_endpoint_in_txn`]
/// result, not `def.to_table` directly (reviewer follow-up to issue #74,
/// epic #78's own whole-branch review) — is what `to_regclass` below
/// actually queries, so a to-side explicitly qualified into a non-default
/// schema resolves here exactly as it does in every other pg_catalog check
/// this function's caller runs. The reported
/// [`ValidationError::RelationshipToManyRequiresReplicaIdentity`] still names
/// `def.to_table` (bare), matching this file's convention elsewhere
/// (`assert_comparable_types`/`assert_join_key_type_supported`) of reporting
/// the relationship's own source text, not an internally-resolved identity.
async fn assert_replica_identity_supports_to_many(
    txn: &tokio_postgres::Transaction<'_>,
    def: &RelationshipDef,
    to_table: &str,
) -> Result<(), CatalogError> {
    let adequate: bool = txn
        .query_one(
            "select
                c.relreplident = 'f'
                or (
                    c.relreplident = 'i'
                    and exists (
                        select 1
                        from pg_index i
                        join pg_attribute a
                          on a.attrelid = i.indrelid and a.attname = $2
                        where i.indrelid = c.oid
                          and i.indisreplident
                          and a.attnum = any(i.indkey::int2[])
                    )
                )
             from pg_class c
             where c.oid = pg_catalog.to_regclass($1)",
            &[&to_table, &def.to_col],
        )
        .await?
        .get(0);

    if adequate {
        Ok(())
    } else {
        Err(ValidationError::RelationshipToManyRequiresReplicaIdentity {
            name: def.name.clone(),
            to_table: def.to_table.clone(),
            to_col: def.to_col.clone(),
        }
        .into())
    }
}

/// Issue #376: rejects a definition whose source would reach this instance
/// over logical replication with changes intake can't key
/// ([`crate::intake::change_keyed`]) — [`CatalogError::SourceNotChangeKeyed`].
///
/// This instance's own targets are exempt. Every write to one goes through
/// the target-mutation seam (`staging::target_mutations`, issue #315), which
/// stages each changed key for the target's readers in the writing
/// transaction, keyed by the same code that wrote the row, so an aggregate
/// target's missing primary key never matters to its own instance.
///
/// Another instance's target gets no such exemption, because nothing in this
/// catalog names it: that instance's seam stages downstream rows only into
/// its own ring, so this instance's only view of the table is CDC through its
/// own publication. For a 1-1 target that works, since it mirrors its
/// source's primary key. For an aggregate target it can't: publishing it
/// makes Postgres refuse the owning instance's own updates to it (no replica
/// identity), or, with `REPLICA IDENTITY FULL`, stops this instance's intake
/// on its first change.
///
/// The rule is about the table, not about who owns it, so a plain source
/// table intake can't key is rejected too (no primary key under `DEFAULT` or
/// `FULL` identity, or `REPLICA IDENTITY NOTHING`). It fails the same two
/// ways at runtime.
async fn reject_unkeyed_source(
    client: &impl GenericClient,
    qualified_source: &str,
) -> Result<(), CatalogError> {
    if is_definition_target(client, qualified_source).await? {
        return Ok(());
    }
    let Some((schema, table)) = qualified_source.split_once('.') else {
        return Err(CatalogError::SourceTableNotFound(
            qualified_source.to_string(),
        ));
    };
    if crate::intake::change_keyed(client, schema, table).await? {
        return Ok(());
    }
    Err(CatalogError::SourceNotChangeKeyed {
        source_table: qualified_source.to_string(),
    })
}

/// Issue #375: rejects a relationship endpoint that would reach this
/// instance's publication with changes intake can't key.
///
/// - This instance's own targets, aggregate targets included, are exempt,
///   for the reason [`reject_unkeyed_source`] gives: they are never published
///   ([`publication_tables`]), and the target-mutation seam stages their
///   changes, CDC-shaped for an endpoint, keyed by the code that wrote them
///   (#375's direction 1, #403).
/// - Anything else, including another instance's aggregate target, must
///   pass [`crate::intake::change_keyed`]
///   ([`CatalogError::RelationshipEndpointNotChangeKeyed`]), the rule
///   [`reject_unkeyed_source`] holds a definition's source to:
///   [`all_source_tables`]' relationship walk publishes it.
async fn reject_unkeyed_relationship_endpoint(
    client: &impl GenericClient,
    qualified_endpoint: &str,
) -> Result<(), CatalogError> {
    if is_definition_target(client, qualified_endpoint).await? {
        return Ok(());
    }
    let keyed = match qualified_endpoint.split_once('.') {
        Some((schema, table)) => crate::intake::change_keyed(client, schema, table).await?,
        None => false,
    };
    if keyed {
        return Ok(());
    }
    Err(CatalogError::RelationshipEndpointNotChangeKeyed {
        endpoint: qualified_endpoint.to_string(),
    })
}

/// Checks every guarantee [`crate::intake::required_source_guarantees`]
/// derives for `plan` against the live catalog, in order, returning the
/// first violation — issue #173 phase 2's single checker. Before this, each
/// of [`assert_replica_identity_supports_aggregate`] and
/// [`assert_replica_identity_supports_projection`] ran its own ad hoc
/// `pg_class.relreplident` query; this is the one place that both (a) walks
/// the derived [`crate::intake::SourceGuarantee`] list and (b) turns each
/// one into a database round trip, so a future [`crate::intake::SourceGuarantee`]
/// variant only needs a new match arm here, not a fourth hand-rolled
/// assertion function.
///
/// `SourceGuarantee::ReplicaIdentityFull`'s `qualified_table` (not `table`)
/// is what's actually queried — see that variant's own doc comment for why
/// a bare name can't be trusted with a plain `to_regclass` `search_path`
/// walk — while `table` is what the resulting error names, matching every
/// existing replica-identity error message's convention of reporting the
/// relationship/definition's own source text.
///
/// A guarantee on one of this instance's own targets always holds (issues
/// #315, #403). Such a table is never published ([`publication_tables`]),
/// so no CDC image of it is ever read: the target-mutation seam captures
/// each changed row's prior image under its own row lock, whatever the
/// table's replica identity. That covers an aggregate over a target and a
/// to-one relationship with a target endpoint alike.
async fn check_source_guarantees(
    txn: &tokio_postgres::Transaction<'_>,
    plan: &crate::intake::ResolvedPlan<'_>,
) -> Result<(), CatalogError> {
    for guarantee in crate::intake::required_source_guarantees(plan) {
        match guarantee {
            crate::intake::SourceGuarantee::ReplicaIdentityFull {
                table,
                qualified_table,
            } => {
                if is_definition_target(txn, &qualified_table).await? {
                    continue;
                }
                let is_full: bool = txn
                    .query_one(
                        "select relreplident = 'f' from pg_class where oid = \
                         pg_catalog.to_regclass($1)",
                        &[&qualified_table],
                    )
                    .await?
                    .get(0);

                crate::intake::require_replica_identity_full(&table, !is_full)
                    .map_err(CatalogError::ReplicaIdentityRequired)?;
            }
        }
    }
    Ok(())
}

/// Rejects an aggregate (`GROUP BY`) definition (issue #47) whose source
/// table's replica identity doesn't guarantee an old row image on
/// delete/update. Unlike [`assert_replica_identity_supports_to_many`]'s
/// to-many relationship check — which only needs one non-PK join column
/// (`to_col`) present in the pre-image, and so accepts a covering
/// `REPLICA IDENTITY USING INDEX` — an aggregate's delta maintenance
/// (`apply_aggregate.rs`'s `accumulate_changes`) needs the *entire* old row:
/// every `GROUP BY` column (to find which group a deleted/re-parented row
/// was decrementing) and every column any `SUM`/`AVG`/`MIN`/`MAX` field
/// reads (to subtract its old contribution). Only `REPLICA IDENTITY FULL`
/// (`relreplident = 'f'`) guarantees that for an arbitrary set of columns, so
/// this doesn't attempt the narrower per-column index check the to-many path
/// does.
///
/// Issue #173 phase 2: delegates both "does this plan need a guarantee?"
/// and "does the table already have it?" to the shared, single-derivation
/// path — [`crate::intake::required_source_guarantees`] over a
/// [`crate::intake::ResolvedPlan::Transform`], checked by
/// [`check_source_guarantees`] — rather than re-deriving either step here.
/// Kept as a thin, named wrapper (rather than inlining its callers into
/// [`check_source_guarantees`] directly) so `create_definition_inner`'s own
/// call site, and this function's pre-existing doc history below, don't
/// have to change.
///
/// Delegates the actual rejection to
/// [`crate::intake::require_replica_identity_full`] (issue #7's scaffolding,
/// previously unwired — see its module doc) so the error text — including
/// the exact `ALTER TABLE ... REPLICA IDENTITY FULL;` statement — comes from
/// one place rather than being duplicated here. That function's own
/// `needs_old_image` parameter is unconditional (it rejects whenever passed
/// `true`, regardless of the table's actual replica identity), so it is not
/// enough on its own — the key-space rule behind
/// [`crate::intake::required_source_guarantees`] always yields "needs the old
/// image" for [`KeySpace::Aggregate`], which would reject every aggregate
/// definition forever, even after an operator runs the suggested `ALTER
/// TABLE`. [`check_source_guarantees`] closes that gap by querying
/// `pg_class.relreplident` itself first and only passing `true` through
/// when the source table is actually inadequate today.
async fn assert_replica_identity_supports_aggregate(
    txn: &tokio_postgres::Transaction<'_>,
    def: &TransformDef,
) -> Result<(), CatalogError> {
    // Issue #76 follow-up: this runs *before* `create_definition_inner`
    // resolves `qualified_source` (this function is called right at the top
    // of that function, deliberately, per this function's own doc comment —
    // ahead of any side effect, so a doomed aggregate never touches the
    // schema graph), so it can't just reuse that value — it has to redo the
    // same explicit-vs-bare branch here. An explicit `FROM <schema>.<source>`
    // ([`TransformDef::explicit_source_schema`]) must be checked against
    // *that* schema specifically: passing bare `def.source` to
    // `to_regclass` instead would resolve it via this connection's pinned
    // `search_path` (`pool::session_bootstrap`), which can silently name a
    // same-suffixed decoy table in an earlier search-path schema instead of
    // the real, explicitly-qualified source — either wrongly rejecting a
    // fully-qualified source that has `REPLICA IDENTITY FULL` (if the decoy
    // lacks it), or worse, wrongly accepting one that doesn't (if the decoy
    // has it), reintroducing issue #47's aggregate-corruption bug through
    // this issue's own new grammar. `to_regclass` accepts a qualified
    // `"schema.table"` string directly, so no other logic changes.
    //
    // Reviewer follow-up to issue #74 (epic #78's own whole-branch review):
    // the bare (`None`) branch used to pass `def.source` straight through
    // unresolved — a plain `to_regclass` `search_path` walk with no
    // fallback — even though `create_definition_inner`'s own resolution of
    // the identical bare `def.source` (`qualified_source`, below) has
    // carried issue #74's bare-target-suffix fallback
    // ([`resolve_graph_identity_in_txn`]) since that issue landed. Since
    // this check runs first, ahead of that resolution, a bare aggregate
    // source chained off another definition's target explicitly qualified
    // into a non-default schema (issue #76) never reached
    // `qualified_source` at all: `to_regclass` returned `NULL` for the
    // unqualified name, and the `query_one` below then found zero matching
    // `pg_class` rows — an opaque `Db(Error{kind: RowCount})`, not a clean
    // rejection. Switched to [`resolve_graph_identity_in_txn`] itself here
    // too, rather than re-implementing the fallback a third time; a
    // genuinely nonexistent source now surfaces as that function's own
    // [`CatalogError::SourceTableNotFound`] instead of the same `RowCount`
    // confusion, which is strictly clearer even though it's not this
    // gap's main target.
    let qualified_source = match &def.explicit_source_schema {
        Some(schema) => crate::intake::publication::qualify(schema, &def.source)?,
        None => resolve_graph_identity_in_txn(txn, &def.source).await?,
    };

    // A source that is one of this instance's targets passes whatever its
    // replica identity (issue #315): see `check_source_guarantees`.
    check_source_guarantees(
        txn,
        &crate::intake::ResolvedPlan::Transform {
            source_table: &def.source,
            qualified_source_table: &qualified_source,
            key_space: &def.key_space,
        },
    )
    .await
}

/// Rejects a to-one relationship (issue #129, epic #127) whose to-side or
/// from-side table's replica identity can't guarantee an old row image.
/// Every to-one relationship gets a settled parent projection
/// unconditionally — see [`create_relationship`]'s own call site — and the
/// projection's reverse-applied advance (#131) needs the to-side row's
/// *entire* old image to detect and apply a parent update/delete/re-key, the
/// same requirement [`assert_replica_identity_supports_aggregate`] already
/// enforces for an aggregate's source; see that function's doc comment for
/// why only `REPLICA IDENTITY FULL` — not the narrower `USING INDEX` a
/// single-column `to_col` check ([`assert_replica_identity_supports_to_many`])
/// would accept — is enough for an old image of unpredictably-many columns.
///
/// **Issue #158: the from-side (child) table needs this too, not just the
/// to-side.** A to-one relationship's `from_col` is an ordinary non-key
/// column on `from_table` (the foreign key), so under that table's *default*
/// replica identity (primary key only) an `UPDATE` that re-points the FK —
/// changes `from_col` without touching the PK — ships `old_image = None`
/// from `pgoutput`: no pre-image at all, not merely one missing the changed
/// column. That breaks anything reading a from-side row's prior state off a
/// replication message, including the `group_key` union mechanism (issue
/// #133) that recovers a row's true prior parent. The to-side's own gate
/// can't catch this — it only ever inspects `to_table`, and a from-side
/// re-point doesn't touch the to-side row at all.
///
/// Issue #173 phase 2: both endpoints used to be checked via two separate
/// calls to this function (one per table); now it takes both endpoints at
/// once and routes them through a single
/// [`crate::intake::ResolvedPlan::ToOneRelationship`] plan, checked by
/// [`check_source_guarantees`] — one derivation call per relationship
/// installed, not one ad hoc `pg_class` query per endpoint. Order is
/// preserved (to-side checked before from-side, matching
/// [`crate::intake::required_source_guarantees`]'s own ordering for this
/// variant), so an operator whose relationship fails both still sees the
/// same first error they always did.
///
/// **Gated on cardinality alone, not on whether the relationship has a
/// consumer yet.** A relationship must be declared before anything can
/// reference it (a [`super::ast::Expr::RelationshipPath`]'s `rel` head
/// resolves via [`relationship_by_name`], which only ever finds an
/// already-persisted row), so at the point this runs — inside
/// [`create_relationship`], before that row is even committed — no consumer
/// can exist yet. Gating on "has a consumer today" would therefore never
/// fire: it would silently accept a to-one relationship whose to-side (or
/// from-side) can never actually satisfy a projection some *later*
/// definition needs, discovered only when that later definition's widen
/// ([`ensure_relationship_projection_in_txn`]) fails partway through
/// *its* transaction — a confusing place to first learn the real problem is
/// this relationship's declaration. Rejecting here instead matches
/// [`assert_replica_identity_supports_to_many`]'s own unconditional,
/// cardinality-only gate, and is consistent with this epic's Phase 1 design
/// (`issue-102-PLAN-DRAFT.md` §7): the projection is built alongside the
/// relationship itself, not deferred until first use.
async fn assert_replica_identity_supports_projection(
    txn: &tokio_postgres::Transaction<'_>,
    to_table: &str,
    qualified_to_table: &str,
    from_table: &str,
    qualified_from_table: &str,
) -> Result<(), CatalogError> {
    check_source_guarantees(
        txn,
        &crate::intake::ResolvedPlan::ToOneRelationship {
            to_table,
            qualified_to_table,
            from_table,
            qualified_from_table,
        },
    )
    .await
}

/// A to-one relationship's settled parent projection (issue #129, epic
/// #127), as stored in `relationship_projections` — the catalog record of
/// which physical table backs a given relationship's projection, not the
/// projection's live data. The projection's own bookkeeping columns
/// (`__trellis_gen`/`__trellis_lsn`) and projected data columns live on the
/// physical `projection_table` itself — see
/// [`super::ddl::PROJECTION_GEN_COLUMN`]/[`super::ddl::PROJECTION_LSN_COLUMN`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationshipProjection {
    pub id: i64,
    pub relationship_id: i64,
    /// The schema the projection table lives in: always the instance's own
    /// catalog schema ([`crate::Config::schema`]), alongside the rest of
    /// Trellis's internal state, never the target schema (issue #435). Not
    /// stored in `relationship_projections`, which lives in that same schema.
    pub projection_schema: String,
    /// The physical table's bare name. [`Self::qualified_table`] gives the
    /// form to interpolate into SQL.
    pub projection_table: String,
}

impl RelationshipProjection {
    /// `projection_schema.projection_table`, each part quoted for direct
    /// interpolation into DDL/DML text.
    pub fn qualified_table(&self) -> String {
        ddl::qualified_relationship_projection_table(
            &self.projection_schema,
            &self.projection_table,
        )
    }
}

/// Reads back the settled parent projection for the relationship
/// `relationship_id` names, or `None` if it's a to-many relationship (which
/// never gets one in Phase 1 of this epic — to-many relationship aggregates
/// keep going through the existing `rel_joins`/ring path) or an unknown id.
pub async fn relationship_projection(
    pool: &Pool,
    relationship_id: i64,
) -> Result<Option<RelationshipProjection>, CatalogError> {
    let client = pool.get().await?;
    let row = client
        .query_opt(
            "select id, relationship_id, projection_table \
             from relationship_projections where relationship_id = $1",
            &[&relationship_id],
        )
        .await?;
    Ok(row.map(|row| RelationshipProjection {
        id: row.get(0),
        relationship_id: row.get(1),
        projection_schema: pool.schema().to_string(),
        projection_table: row.get(2),
    }))
}

/// Ensures the to-one relationship `relationship_id` names has a settled
/// parent projection (issue #129, epic #127) that (a) carries at least every
/// column in `needed_columns` and (b) has a row for every to-side key —
/// creating the projection from scratch the first time this is called for
/// `relationship_id`, widening an existing one (`alter table ... add
/// column`) for any of `needed_columns` it doesn't carry yet, and — on
/// *every* call, regardless of whether (b) needed anything new — inserting a
/// projection row for any to-side key that doesn't have one yet. A column or
/// row already present is left completely untouched; this only ever adds.
///
/// **Why (b) is not just "the first-creation backfill plus widening the
/// columns of rows that are already there"** (review follow-up to issue
/// #129): nothing keeps the projection continuously in sync with its to-side
/// table yet — that's the forward/reverse paths, #130/#131, neither of which
/// exists. Between this relationship's declaration and whenever a consumer
/// first triggers a widen, the to-side table keeps taking ordinary
/// replicated writes, including plain `INSERT`s of new rows the projection
/// has never seen. A widen that only `ALTER TABLE`s and then `UPDATE ...
/// FROM`s the columns of rows *already in the projection* silently skips
/// every such row forever — there is no later resync to catch it, since
/// #130/#131 aren't built yet. So every call here re-derives "every
/// currently-known column" (bookkeeping plus whatever data columns already
/// exist, plus whatever `needed_columns` just added) and anti-join-inserts
/// any missing to-side key with all of them populated from the live to-side
/// table — the same shape the original from-scratch backfill used, just
/// scoped to `not exists` rather than the unconditional first insert. This
/// makes "create" and "catch up" the same code path instead of two
/// independently-maintained ones that quietly drifted apart (the from-scratch
/// backfill this replaces used to run once, in the `None` branch below, and
/// nothing else ever re-ran it — exactly the gap this fixes).
///
/// Two call sites, both already inside their own open transaction so a
/// projection creation/widen/catch-up commits or rolls back atomically with
/// whatever catalog change occasioned it:
/// * [`create_relationship`] calls this with `needed_columns: &[]` right
///   after inserting the relationship's own row — so every to-one
///   relationship gets a (bookkeeping-columns-only) projection
///   unconditionally, before any consumer exists to read it (see
///   [`assert_replica_identity_supports_projection`]'s doc comment for why
///   that ordering is unavoidable).
/// * [`create_definition_inner`] calls this once per to-one relationship a
///   newly-created definition's fields read through
///   ([`widen_relationship_projections_for_definition_in_txn`]), with that
///   definition's own referenced columns — so the projection grows to cover
///   a new consumer's reads, and a projection several transforms share
///   (issue #129's own scope line: "one projection can serve several
///   transforms") ends up carrying the union of every consumer's reads
///   without any consumer needing to know about the others. This is also
///   the call site that exercises the row catch-up above in practice: any
///   `create_definition`/`install_definition` call is a natural point where
///   directly-written to-side rows accumulated since the relationship (or
///   the last widen) get folded back in.
///
/// `to_col_pg_type` is only consulted on first creation (an existing
/// projection's key column type can't change short of dropping the
/// projection outright, which nothing does in v1).
///
/// `catalog_schema` is the instance's own schema ([`Pool::schema`]), where the
/// projection lives (issue #435): its generated name is unique only within
/// this catalog, since every instance numbers its relationships from 1, so a
/// projection in a target schema two instances share would collide with the
/// other instance's. In the catalog schema the collision can't happen, which
/// is why creation is a plain `create table`: a table already there is a bug,
/// not something to adopt.
#[allow(clippy::too_many_arguments)]
async fn ensure_relationship_projection_in_txn(
    txn: &tokio_postgres::Transaction<'_>,
    relationship_id: i64,
    qualified_to_table: &str,
    to_table_bare: &str,
    to_col: &str,
    to_col_pg_type: &str,
    catalog_schema: &str,
    needed_columns: &[String],
) -> Result<(), CatalogError> {
    // `qualified_to_table` is the plain, unquoted `"schema.table"` identity
    // (`resolve_relationship_endpoint_in_txn`'s own return shape) — safe to
    // bind as a `to_regclass($1)` parameter (as [`column_type_in_txn`]
    // already does with it below), but *not* safe to splice directly into
    // raw SQL text: unlike a bind parameter, interpolated SQL needs each
    // component quoted independently, exactly the gap
    // [`super::ddl::qualified_source_table`] exists to close (see its own
    // doc comment). Every DML statement built below uses this quoted form,
    // never `qualified_to_table` itself.
    let quoted_to_table = ddl::qualified_source_table(qualified_to_table);
    let to_col_ident = quote_ident(to_col);

    let existing = txn
        .query_opt(
            "select projection_table from relationship_projections \
             where relationship_id = $1",
            &[&relationship_id],
        )
        .await?;

    let projection_table = match existing {
        Some(row) => row.get::<_, String>(0),
        None => {
            let projection_table = ddl::relationship_projection_table_name(relationship_id);
            let qualified_projection =
                ddl::qualified_relationship_projection_table(catalog_schema, &projection_table);

            // The projection's own shape: its key (the to-side column
            // itself, same name and type as the to-side's), plus the two
            // bookkeeping columns every projection carries regardless of
            // which consumers it serves — see
            // [`super::ddl::PROJECTION_GEN_COLUMN`]/
            // [`super::ddl::PROJECTION_LSN_COLUMN`]'s doc comments for what
            // each means and how it's meant to be advanced. No data columns
            // yet, and no rows either — the row catch-up below (which runs
            // unconditionally, not just when `needed_columns` is non-empty)
            // is what actually populates it, using this branch's freshly
            // created, still-empty table as its starting point.
            txn.batch_execute(&format!(
                "create table {qualified_projection} (\
                     {to_col_ident} {to_col_pg_type} primary key, \
                     {gen_col} bigint not null default 0, \
                     {lsn_col} pg_lsn not null)",
                gen_col = quote_ident(ddl::PROJECTION_GEN_COLUMN),
                lsn_col = quote_ident(ddl::PROJECTION_LSN_COLUMN),
            ))
            .await?;

            txn.execute(
                "insert into relationship_projections \
                 (relationship_id, projection_table) values ($1, $2)",
                &[&relationship_id, &projection_table],
            )
            .await?;

            projection_table
        }
    };

    let qualified_projection =
        ddl::qualified_relationship_projection_table(catalog_schema, &projection_table);

    // Every data column (i.e. excluding the key and the two bookkeeping
    // columns) the projection currently carries, in ordinal order — the
    // union this function has to keep in step with is exactly this set,
    // widened below by `needed_columns` and then used, in full, by the row
    // catch-up that follows. A `Vec` (not a `HashSet`): a handful of columns
    // at most, and iteration order should match `information_schema`'s own
    // ordinal order for a readable generated `insert`/`update` column list.
    let bookkeeping = [
        to_col,
        ddl::PROJECTION_GEN_COLUMN,
        ddl::PROJECTION_LSN_COLUMN,
    ];
    let mut data_columns: Vec<String> = txn
        .query(
            "select column_name from information_schema.columns \
             where table_schema = $1 and table_name = $2 order by ordinal_position",
            &[&catalog_schema, &projection_table],
        )
        .await?
        .into_iter()
        .map(|row| row.get::<_, String>(0))
        .filter(|c| !bookkeeping.contains(&c.as_str()))
        .collect();

    for column in needed_columns {
        if data_columns.contains(column) {
            continue;
        }
        let pg_type = column_type_in_txn(txn, qualified_to_table, to_table_bare, column).await?;
        let col_ident = quote_ident(column);
        txn.batch_execute(&format!(
            "alter table {qualified_projection} add column if not exists {col_ident} {pg_type}"
        ))
        .await?;
        // Backfill the new column for every projection row that already
        // exists — the row catch-up below handles any to-side key that
        // isn't a projection row yet at all, using this same widened column
        // list, so this `update` only has to cover the "existing row, new
        // column" half.
        txn.batch_execute(&format!(
            "update {qualified_projection} p set {col_ident} = t.{col_ident} \
             from {quoted_to_table} t where t.{to_col_ident} = p.{to_col_ident}"
        ))
        .await?;
        data_columns.push(column.clone());
    }

    // Row catch-up (review follow-up to issue #129 — see this function's own
    // doc comment for the scenario this closes): insert a projection row,
    // with every currently-known column populated, for any to-side key that
    // doesn't have one yet. Runs on every call, not just when `needed_columns`
    // added something — a call with an already-fully-covered column set can
    // still be the first opportunity to notice to-side rows that arrived
    // since the last call. `not exists` makes this a no-op for keys the
    // projection already has, so it's safe to run unconditionally rather
    // than trying to track "did anything actually change" first. Excludes
    // NULL-keyed to-side rows for the same reason the original from-scratch
    // backfill did (`to_col` is `not null` here, and a NULL join key can
    // never match a from-side row anyway) — a different situation from
    // #128's nullable *grouping*-key fix, since a NULL to-side key isn't a
    // bucket needing storage.
    //
    // `pg_current_wal_lsn()` is captured fresh here (via the `seed` CTE, not
    // reused from any earlier call) and shared by every row *this*
    // statement writes — see [`super::ddl::PROJECTION_LSN_COLUMN`]'s doc
    // comment for exactly what that value does and doesn't guarantee, and
    // what #131/#132 should confirm before relying on it.
    let mut insert_col_idents = vec![
        to_col_ident.clone(),
        quote_ident(ddl::PROJECTION_GEN_COLUMN),
        quote_ident(ddl::PROJECTION_LSN_COLUMN),
    ];
    let mut select_col_exprs = vec![
        format!("t.{to_col_ident}"),
        "0".to_string(),
        "seed.lsn".to_string(),
    ];
    for column in &data_columns {
        let col_ident = quote_ident(column);
        select_col_exprs.push(format!("t.{col_ident}"));
        insert_col_idents.push(col_ident);
    }
    // Named (not captured) on purpose: `{insert_col_idents}`/`{select_col_exprs}`
    // must render the *joined* `String` below, not `Debug`-format the `Vec`
    // locals of the same name that captured-identifier interpolation would
    // otherwise reach for.
    let insert_cols = insert_col_idents.join(", ");
    let select_cols = select_col_exprs.join(", ");
    let catch_up_sql = format!(
        "with seed as (select pg_current_wal_lsn() as lsn) \
         insert into {qualified_projection} ({insert_cols}) \
         select {select_cols} \
         from {quoted_to_table} t, seed \
         where t.{to_col_ident} is not null \
           and not exists (\
             select 1 from {qualified_projection} p where p.{to_col_ident} = t.{to_col_ident}\
           )"
    );
    txn.batch_execute(&catch_up_sql).await?;

    Ok(())
}

/// Re-syncs every settled parent projection whose to-side is
/// `qualified_to_table` with that table's current rows (issue #507): deletes
/// each projection row whose to-side row is gone, rewrites the data columns of
/// each row whose to-side row now differs, and inserts a row for each to-side
/// key it lacks, seeded as [`ensure_relationship_projection_in_txn`] seeds
/// one. Returns how many projection rows it changed.
///
/// The catch-up discharge runs this for a marker parked because a rebuild
/// rewrote a definition's target (`pending_backfill.refresh_projections`,
/// `intake::publication::discharge_marker`). It diffs the whole target, about
/// 0.7 s for a 1M-row target whose projection is already current, so no
/// other marker asks for it. Every write to a target
/// reaches its projection through the target-mutation seam's CDC-shaped rows,
/// except a rebuild's (a resumed definition's chunks or direct build), which
/// writes the target directly. Nothing else would ever carry what the rebuild
/// changed into the projection, and a consumer reading through a to-one
/// relationship reads the projection, never the target. The same discharge
/// re-derives those consumers from the refreshed projection (its enumeration
/// of the target reaches them by reverse propagation).
///
/// Neither bookkeeping column moves on a row it rewrites. A seam row staged
/// before this read can still be pending, and its capture of the row's
/// `__trellis_lsn`, before or after this, still matches. If its image is the
/// row's latest it re-applies what this already wrote. If the rebuild
/// superseded it, putting that image back would be wrong with nothing after
/// it to correct it, so Phase 3 checks a seam-fed to-side's images against
/// the live row and writes the projection from the live row instead
/// (`staging::apply::to_side_superseded`). A seam row staged after this read
/// can't drain before the discharge commits: the discharge runs on the only
/// sealer.
///
/// Its projection writes can deadlock with a drain's, which locks projection
/// rows in key order (step 3c's `__trellis_gen` bump) or one record at a
/// time (step 3d), while this locks them in scan order, and with a drain that
/// holds a projection row while it writes an aggregate consumer's target row
/// the same discharge's orphan sweep deleted. Postgres aborts one side, the
/// same bounded case as the sweep's own (`intake::resume_orphans`): an
/// aborted apply is a transient error with no quarantine charge, and an
/// aborted discharge rolls back whole and retries after the marker's
/// backoff.
pub(crate) async fn refresh_relationship_projections_in_txn(
    client: &impl GenericClient,
    qualified_to_table: &str,
) -> Result<u64, CatalogError> {
    let Some((to_schema, to_table)) = qualified_to_table.split_once('.') else {
        return Ok(0);
    };
    // A projection lives in the catalog schema, the same schema as
    // `relationship_projections` itself (issue #435), which this connection's
    // search path already resolves.
    let projections = client
        .query(
            "select rp.projection_table, n.nspname::text, rd.to_col \
             from relationship_projections rp \
             join relationship_definitions rd on rd.id = rp.relationship_id \
             join pg_class c on c.oid = 'relationship_projections'::regclass \
             join pg_namespace n on n.oid = c.relnamespace \
             where rd.to_schema = $1 and rd.to_table = $2 \
             order by rp.relationship_id",
            &[&to_schema, &to_table],
        )
        .await?;
    let quoted_to_table = ddl::qualified_source_table(qualified_to_table);
    let mut changed = 0;
    for row in projections {
        let projection_table: String = row.get(0);
        let catalog_schema: String = row.get(1);
        let to_col: String = row.get(2);
        let qualified_projection =
            ddl::qualified_relationship_projection_table(&catalog_schema, &projection_table);
        let key = quote_ident(&to_col);
        let bookkeeping = [
            to_col.as_str(),
            ddl::PROJECTION_GEN_COLUMN,
            ddl::PROJECTION_LSN_COLUMN,
        ];
        let data_columns: Vec<String> = client
            .query(
                "select column_name::text from information_schema.columns \
                 where table_schema = $1 and table_name = $2 order by ordinal_position",
                &[&catalog_schema, &projection_table],
            )
            .await?
            .into_iter()
            .map(|row| row.get::<_, String>(0))
            .filter(|c| !bookkeeping.contains(&c.as_str()))
            .collect();
        let data: Vec<String> = data_columns.iter().map(|c| quote_ident(c)).collect();

        changed += client
            .execute(
                &format!(
                    "delete from {qualified_projection} p where not exists ( \
                         select 1 from {quoted_to_table} t where t.{key} = p.{key})"
                ),
                &[],
            )
            .await?;
        if !data.is_empty() {
            let sets: Vec<String> = data.iter().map(|c| format!("{c} = t.{c}")).collect();
            let differs: Vec<String> = data
                .iter()
                .map(|c| format!("p.{c} is distinct from t.{c}"))
                .collect();
            changed += client
                .execute(
                    &format!(
                        "update {qualified_projection} p set {sets} \
                         from {quoted_to_table} t \
                         where t.{key} = p.{key} and ({differs})",
                        sets = sets.join(", "),
                        differs = differs.join(" or "),
                    ),
                    &[],
                )
                .await?;
        }
        let mut insert_cols = vec![
            key.clone(),
            quote_ident(ddl::PROJECTION_GEN_COLUMN),
            quote_ident(ddl::PROJECTION_LSN_COLUMN),
        ];
        let mut select_cols = vec![format!("t.{key}"), "0".to_string(), "seed.lsn".to_string()];
        for c in &data {
            insert_cols.push(c.clone());
            select_cols.push(format!("t.{c}"));
        }
        changed += client
            .execute(
                &format!(
                    "with seed as (select pg_current_wal_lsn() as lsn) \
                     insert into {qualified_projection} ({insert_cols}) \
                     select {select_cols} from {quoted_to_table} t, seed \
                     where t.{key} is not null and not exists ( \
                         select 1 from {qualified_projection} p where p.{key} = t.{key})",
                    insert_cols = insert_cols.join(", "),
                    select_cols = select_cols.join(", "),
                ),
                &[],
            )
            .await?;
    }
    Ok(changed)
}

/// The [`create_definition_inner`] half of issue #129's projection-widening:
/// for every to-one relationship `def`'s fields read through
/// ([`super::eval::relationship_references`], grouped by relationship name),
/// ensures its settled parent projection carries every referenced column —
/// via [`ensure_relationship_projection_in_txn`] — before `def` itself is
/// persisted, so a definition can never be live while reading a projection
/// that hasn't caught up to it yet.
///
/// A to-many relationship reference is silently skipped (no projection in
/// Phase 1); an unknown relationship name is silently skipped too — not this
/// function's job to reject it, [`validate`] (already run, by every caller,
/// against this same `def`) is what raises
/// [`ValidationError::UnknownRelationship`] for that, and this function only
/// ever runs as part of persisting a definition that's already passed that
/// check.
async fn widen_relationship_projections_for_definition_in_txn(
    txn: &tokio_postgres::Transaction<'_>,
    def: &TransformDef,
    qualified_source: &str,
    catalog_schema: &str,
) -> Result<(), CatalogError> {
    // Issue #288: `def`'s relationships are the ones declared on its own
    // qualified source, not on any same-named table in another schema.
    let Some((source_schema, source_table)) = qualified_source.split_once('.') else {
        return Ok(());
    };
    let mut columns_by_rel: HashMap<String, Vec<String>> = HashMap::new();
    for (rel, column) in super::eval::relationship_references(def) {
        columns_by_rel.entry(rel).or_default().push(column);
    }

    for (rel, columns) in columns_by_rel {
        let Some(row) = txn
            .query_opt(
                "select id, to_schema, to_table, to_col, cardinality from relationship_definitions \
                 where from_schema = $1 and from_table = $2 and name = $3",
                &[&source_schema, &source_table, &rel],
            )
            .await?
        else {
            continue;
        };
        let relationship_id: i64 = row.get(0);
        let to_schema: String = row.get(1);
        let to_table: String = row.get(2);
        let to_col: String = row.get(3);
        let cardinality_text: String = row.get(4);
        if cardinality_text != RelationshipCardinality::ToOne.as_str() {
            continue;
        }

        // Issue #372: the to-side the relationship was declared against, not
        // a re-resolution of the bare name through this session's
        // `search_path`.
        let qualified_to = format!("{to_schema}.{to_table}");
        let to_col_pg_type = column_type_in_txn(txn, &qualified_to, &to_table, &to_col).await?;

        ensure_relationship_projection_in_txn(
            txn,
            relationship_id,
            &qualified_to,
            &to_table,
            &to_col,
            &to_col_pg_type,
            catalog_schema,
            &columns,
        )
        .await?;
    }

    Ok(())
}

/// Whether `from_table` has a usable index for looking up rows by
/// `from_col` (issue #31) — the query reverse propagation runs when a
/// related `to_table` row changes (ADR-0006). "Usable" means a `btree`
/// index whose *leading* column is `from_col`: a plain `where from_col =
/// $1` lookup can use such an index regardless of what other columns
/// follow it, so — unlike [`to_col_cardinality_in_txn`]'s uniqueness check —
/// this doesn't require `from_col` to be the index's only column.
///
/// Excludes indexes that can't be trusted for this lookup:
/// * `indisvalid` — a not-yet-validated index (e.g. left behind by a failed
///   `CREATE INDEX CONCURRENTLY`) isn't usable yet.
/// * `am.amname = 'btree'` — other access methods (`gin`, `brin`, `hash`)
///   either don't support this leading-column equality lookup the way
///   btree does, or aren't worth special-casing for what's only a
///   performance hint.
/// * `indexprs is null` — an expression index's leading "column" isn't a
///   plain column reference, so `indkey[0]` is `0` and never matches a real
///   `attnum`; this is already excluded by the `indkey[0] = a.attnum` join
///   condition, called out here since it's not obvious from the SQL alone.
/// * `indpred is null` — a partial index only covers the rows satisfying
///   its predicate, so the planner won't use it for an unqualified
///   `from_col = $1` lookup across all rows; same exclusion
///   [`to_col_cardinality_in_txn`] applies for uniqueness, for the same
///   reason.
///
/// Never issues DDL — this only informs the caller's decision to emit
/// [`RelationshipWarning::MissingFkIndex`] (ADR-0005: Trellis never modifies
/// the source schema).
///
/// `from_table` — [`create_relationship`]'s own [`resolve_relationship_endpoint_in_txn`]
/// result, not `def.from_table` directly (reviewer follow-up to issue #74) —
/// so a from-side explicitly qualified into a non-default schema resolves
/// here too; the warning itself, built by the caller from `def.from_table`,
/// is unaffected either way.
async fn has_usable_fk_index_in_txn(
    txn: &tokio_postgres::Transaction<'_>,
    from_table: &str,
    from_col: &str,
) -> Result<bool, CatalogError> {
    let has_index: bool = txn
        .query_one(
            "select exists (
                select 1
                from pg_index i
                join pg_attribute a
                  on a.attrelid = i.indrelid and a.attname = $2
                join pg_class ic on ic.oid = i.indexrelid
                join pg_am am on am.oid = ic.relam
                where i.indrelid = pg_catalog.to_regclass($1)
                  and i.indisvalid
                  and i.indpred is null
                  and am.amname = 'btree'
                  and i.indkey[0] = a.attnum
             )",
            &[&from_table, &from_col],
        )
        .await?
        .get(0);
    Ok(has_index)
}

/// Resolves `table_name` to its [`SchemaNode`], creating one if this is the
/// first time Trellis has seen the table and setting its `kind` role flag
/// (`is_source`/`is_target`) to `true`. Idempotent, and additive across
/// roles: resolving the same table under both [`NodeKind::Source`] and
/// [`NodeKind::Target`] over separate calls (chained/multi-hop transforms —
/// see [`super::model::NodeKind`]'s doc comment) merges into one node with
/// both flags set, rather than erroring.
///
/// **`table_name` must already be fully-qualified (issue #74, ADR-0007)** —
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

/// Pool-level wrapper over [`persist_edge_in_txn`], mirroring
/// [`resolve_node`]'s relationship to [`resolve_node_in_txn`]. Exists so the
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
/// **`source_table`/`target_table` must already be fully-qualified** (issue
/// #74, ADR-0007) — `schema_nodes.table_name` is, so comparing a bare name
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
/// must already be fully-qualified (issue #74, ADR-0007).
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
/// [`dependents_of`] for edge kinds whose dependents don't join back into
/// `transform_definitions` (e.g. [`EdgeKind::Relationship`], whose
/// dependents are `relationship_definitions` rows, read separately via
/// [`relationship_by_name`]). Returns raw edges rather than joining onto any
/// definition table, so it works for any [`EdgeKind`] without needing a
/// kind-specific query. `node_table` must already be fully-qualified (issue
/// #74, ADR-0007) — matched exactly against `schema_nodes.table_name`.
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
/// copies used to (issue #108 review).
fn encode_value_type(value_type: &ValueType) -> &'static str {
    match value_type {
        ValueType::Numeric => "numeric",
        // Issue #111: an exact integer persists under its own Postgres
        // spelling (`smallint`/`integer`/`bigint`), which is distinct from
        // every other token here *and* from every `PgType::name`, so all
        // three namespaces still share one column unambiguously. The
        // `every_value_type_round_trips_through_the_persisted_token` test
        // pins that.
        ValueType::Integer(width) => width.pg_name(),
        // Issue #112: same shape — `real`/`double precision`, each distinct
        // from every other token in all three namespaces.
        ValueType::Float(width) => width.pg_name(),
        ValueType::Text => "text",
        ValueType::Boolean => "boolean",
        ValueType::Uuid => "uuid",
        // `PgType::name` doubles as this persisted token (issue #108): every
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
    status: TransformStatus,
    source_columns: HashMap<String, ValueType>,
    source_table: String,
    target_table: String,
}

/// The transform definitions that depend on `node_table` via a `kind` edge
/// in the persisted dependency graph (issue #21) — e.g. `EdgeKind::Source`
/// answers "what reads from `node_table` as its `FROM`". Walking
/// `schema_edges` (rather than matching on
/// `transform_definitions.source_table` string equality) is what makes this
/// a real graph lookup: multi-hop chains resolve by calling this again with
/// a dependent's target table, not by any special-casing here.
///
/// One query, not one-per-definition-row (issue #69): a `left join lateral
/// jsonb_each_text(...)` unnests every dependent definition's persisted
/// `source_columns` map inline, so this is still "decode JSON via SQL, no
/// serde_json dependency" — matching `staging::apply::decode_image`'s
/// convention — just decoded for every row in one round trip instead of one
/// per definition. The `left join` (rather than an inner join/`cross join
/// lateral`) matters: a definition whose `source_columns` is `{}` must still
/// come back with zero entries, not disappear from the result entirely.
///
/// **Applying definitions only** ([`TransformStatus::is_applying`]: `live`
/// or `catching_up`; the public API design's ADR-0007 amendment,
/// closing the CDC race commit 1fa8570 reopened): a `waiting_to_backfill`/
/// `backfilling`/`quarantined` definition's target may not yet reflect every
/// pre-existing source row (the direct-build chunk queue, or a
/// still-in-flight ring enumeration, hasn't necessarily finished), so a live
/// CDC delta folded into it now — via [`transforms_for_source`], the apply
/// path's read of this function — could permanently corrupt a value an
/// incremental accumulator (e.g. `AVG`) computes against a baseline. Excluding
/// those rows here means the apply path simply never attempts them, and the
/// skipped delta itself is dropped. What it changed is recovered from state
/// instead, by the discharge of the go-live catch-up
/// ([`crate::intake::publication::run_pending_backfills`]; the same
/// `pending_backfill` marker the ring-fallback path relies on, parked when a
/// build finishes and the definition moves to [`TransformStatus::CatchingUp`]:
/// by [`complete_direct_backfill`] for a chunked build or a direct-build job,
/// on every table that build read, issue #430). Its re-read re-derives every
/// key the source still has. That can't reach a key whose source row a
/// skipped delete removed, so the same discharge also deletes every target
/// row no source row backs any more (issue #485, `intake::resume_orphans`).
///
/// `node_table` ($1) must already be fully-qualified (issue #74, ADR-0007)
/// — matched exactly against `schema_nodes.table_name`, which is now always
/// qualified too, so `public.posts` and `archive.posts` resolve to
/// disjoint dependent sets rather than colliding. The join from `to_node`
/// onto `transform_definitions` is now a plain equality on `target_table`
/// as well: before issue #74, `to_node.table_name` was bare while
/// `target_table` had already been qualified (issue #73), so this join used
/// to go through `target_table`'s bare suffix (`split_part`) instead — no
/// longer necessary now both sides hold the exact same qualified string
/// (both ultimately derived from the same `qualified_target`, in the same
/// transaction, by `create_definition_inner`).
pub async fn dependents_of(
    pool: &Pool,
    node_table: &str,
    kind: EdgeKind,
) -> Result<Vec<Definition>, CatalogError> {
    let client = pool.get().await?;
    let rows = client
        .query(
            "select t.id, t.source_version, t.definition_text, t.status, t.source_table, \
                    t.target_table, e.key, e.value
             from schema_nodes from_node
             join schema_edges se on se.from_node_id = from_node.id and se.kind = $2
             join schema_nodes to_node on to_node.id = se.to_node_id
             join transform_definitions t on t.target_table = to_node.table_name
             left join lateral jsonb_each_text(t.source_columns) e on true
             where from_node.table_name = $1 and t.status = any($3)
             order by t.id",
            &[&node_table, &kind.as_str(), &TransformStatus::applying()],
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
        let def = parse(&pending.text)?;
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

/// The transform definitions currently subscribed to `source_table` — the
/// mapping intake (#7/#8) will use to decide what to subscribe to. A thin
/// wrapper over [`dependents_of`] filtered to [`EdgeKind::Source`], the only
/// edge kind persisted today. `source_table` must already be fully-qualified
/// (issue #74, ADR-0007) — see [`dependents_of`]'s doc comment.
pub async fn transforms_for_source(
    pool: &Pool,
    source_table: &str,
) -> Result<Vec<Definition>, CatalogError> {
    dependents_of(pool, source_table, EdgeKind::Source).await
}

/// Every table that needs CDC capture for at least one registered transform
/// (issue #65): every distinct anchor `source_table` (as stored — see
/// [`create_definition`]'s `def.source`), plus every table transitively
/// reachable from one of those anchors by following
/// `relationship_definitions.from_table -> to_table` edges. A calculated
/// field on a transform anchored at `from_table` can read a relationship
/// path into `to_table` (and, through a chained relationship declared with
/// `to_table` as its own `from_table`, into a table beyond that), so
/// `to_table` must be in the CDC publication too, even though no transform
/// is anchored there directly — see the issue for the silently-dropped-write
/// bug this closes.
///
/// The recursive CTE below seeds the set with the same anchor tables the
/// pre-#65 query returned, then unions in each edge's `to_table` reached
/// from a table already in the set, transitively. `union` (not `union all`)
/// is required, not just tidy: Postgres's recursive-query dedup compares
/// each new candidate row against every row already in the accumulated
/// result and drops it if already present, so a relationship cycle (`to_table`
/// eventually looping back to an ancestor `from_table`) can only ever
/// propose table names already in the set — the recursion adds nothing new
/// on that iteration and terminates, rather than looping forever. A
/// relationship declared on a table that never anchors a registered
/// transform never seeds the recursion, so its `to_table` correctly never
/// appears (issue #65's test case 4).
///
/// Issue #14: a running [`crate::Client`]'s maintenance loop polls this to
/// notice a transform (or now, a relationship reachable from one) registered
/// against a table it hasn't seen before, so it can add that table to the
/// publication and discharge its backfill without waiting for a restart.
///
/// Returns **fully-qualified** `"schema.table"` names (issue #75, ADR-0007)
/// — a change from this function's pre-#75 contract, which returned bare
/// suffixes and left both callers to re-qualify them by re-guessing a single
/// assumed schema (see git history for the details of that bug). Walks
/// `schema_nodes`/`schema_edges` (issue #74's qualified graph) instead of
/// `relationship_definitions`'s raw, still-bare `from_table`/`to_table`
/// columns: every relationship endpoint already resolves into that graph at
/// [`create_relationship`] time via the same [`resolve_graph_identity_in_txn`]
/// a transform's own source/target does, so `schema_nodes.table_name` carries
/// each relationship-reachable table's *actual* qualified identity — not a
/// bare name this function (or a caller) would otherwise have to re-resolve
/// against a guessed schema. Anchors are seeded from
/// `transform_definitions.source_table` directly (already qualified as of
/// issue #72), not `schema_nodes.is_source`: that flag is also set on every
/// relationship endpoint regardless of whether it's transitively reachable
/// from a registered transform, so seeding from it would leak an orphaned
/// relationship's `to_table` the way issue #65's test case 4 (preserved
/// below) specifically forbids.
///
/// The recursive step mirrors the pre-#75 walk's direction exactly, just
/// against qualified nodes/edges: `create_relationship` persists a
/// `Relationship` edge `from_node_id = to_table's node`, `to_node_id =
/// from_table's node` (child depends on parent — see that function's own
/// doc comment), so given a reachable node matching a `Relationship` edge's
/// `to_node` (the child/`from_table` side), the edge's `from_node` (the
/// parent/`to_table` side) is the newly-reachable table.
pub async fn all_source_tables(pool: &Pool) -> Result<Vec<String>, CatalogError> {
    let client = pool.get().await?;
    let rows = client
        .query(
            &format!(
                "{} select table_name from reachable",
                reachable_tables_cte("true")
            ),
            &[],
        )
        .await?;
    Ok(rows.into_iter().map(|r| r.get(0)).collect())
}

/// The recursive walk [`all_source_tables`] documents: every anchor
/// `source_table` of a definition `anchor_filter` (a predicate over
/// `transform_definitions`) keeps, plus every table reachable from one
/// through relationship edges. Shared with [`table_has_reader`] so the two
/// can't disagree about which tables a definition reads.
fn reachable_tables_cte(anchor_filter: &str) -> String {
    format!(
        "with recursive reachable(table_name) as (
            select distinct source_table from transform_definitions where {anchor_filter}
            union
            select from_node.table_name
            from schema_edges se
            join schema_nodes to_node on to_node.id = se.to_node_id
            join schema_nodes from_node on from_node.id = se.from_node_id
            join reachable r on r.table_name = to_node.table_name
            where se.kind = 'relationship'
         )"
    )
}

/// Whether any registered definition other than `excluding` reads
/// `qualified_table`, in any status: as its anchor source, or through a
/// relationship path ([`all_source_tables`]'s set). The backfill discharge
/// ([`crate::intake::publication::run_pending_backfills`]) skips enumerating
/// a table this says `false` for (issue #417), since nothing would consume the
/// `Recompute` rows; it excludes the definitions it just dispatched to chunked
/// builds, which read the table themselves (issue #418).
pub(crate) async fn table_has_reader(
    client: &impl GenericClient,
    qualified_table: &str,
    excluding: &[i64],
) -> Result<bool, tokio_postgres::Error> {
    Ok(client
        .query_one(
            &format!(
                "{} select exists (select 1 from reachable where table_name = $1)",
                reachable_tables_cte("not (id = any($2))")
            ),
            &[&qualified_table, &excluding],
        )
        .await?
        .get(0))
}

/// The tables this instance's CDC publication should hold: every table
/// [`all_source_tables`] reaches, minus the targets of this instance's own
/// definitions (issue #315).
///
/// A chained definition reads another definition's target, but that target
/// never needs CDC: every write to it goes through the target-mutation seam
/// (`staging::target_mutations`), which stages each changed key for its
/// readers in the writing transaction. Publishing it as well would stage each
/// change twice (the double-apply class #312 patched with an intake-side
/// filter), and an aggregate target's CDC can't even be decoded:
/// `intake::extract_key` has no primary key to read, and without `REPLICA
/// IDENTITY FULL` Postgres refuses the target's own updates once it is
/// published.
///
/// That includes a target that is a relationship endpoint (issue #375's
/// direction 1, #403). A to-one relationship's settled parent projection and
/// reverse deltas (issues #129-#136), and a from-side's `group_key` (#133),
/// need image-bearing, LSN-ordered changes, and the seam stages exactly that
/// shape for an endpoint target (`staging::target_mutations`, "Standing in
/// for a relationship endpoint's CDC"), so the seam is the one change feed
/// for every target this instance owns. Another instance's target is just a
/// table here, and is published like one.
pub async fn publication_tables(pool: &Pool) -> Result<Vec<String>, CatalogError> {
    let tables = all_source_tables(pool).await?;
    let client = pool.get().await?;
    let own = own_targets(&**client, &tables).await?;
    Ok(tables.into_iter().filter(|t| !own.contains(t)).collect())
}

/// Which of `tables` are the target of some definition in this instance
/// (issue #315): tables whose changes reach their readers only through the
/// target-mutation seam, never CDC. See [`publication_tables`].
async fn own_targets(
    client: &impl GenericClient,
    tables: &[String],
) -> Result<std::collections::HashSet<String>, CatalogError> {
    let rows = client
        .query(
            "select r.table_name from unnest($1::text[]) as r(table_name) \
             where exists ( \
                 select 1 from transform_definitions d where d.target_table = r.table_name \
             )",
            &[&tables],
        )
        .await?;
    Ok(rows.into_iter().map(|r| r.get(0)).collect())
}

/// Whether the qualified `table` is an endpoint, on either side, of any
/// relationship. `staging::target_mutations` asks it of a target it wrote, to
/// decide whether its seam rows must stand in for that target's CDC (issues
/// #402, #403).
pub(crate) async fn is_relationship_endpoint(
    client: &impl GenericClient,
    table: &str,
) -> Result<bool, CatalogError> {
    Ok(client
        .query_one(
            "select exists ( \
                 select 1 from schema_edges se \
                 join schema_nodes n on n.id in (se.from_node_id, se.to_node_id) \
                 where se.kind = 'relationship' and n.table_name = $1 \
             )",
            &[&table],
        )
        .await?
        .get(0))
}

/// The current version of `source_table`, or `None` if no definition has
/// ever been created against it.
///
/// `source_table` is the qualified identity `source_table_versions` persists
/// (issue #72) and is matched exactly. Its one caller
/// (`staging::apply::compute`) passes the canonical identity it also keys
/// `ApplyPlan::versions` on, and Phase 3's version fence re-reads the same row
/// by the same key. Issue #380: this used to match the bare table-name suffix
/// (`split_part(source_table, '.', 2)`), which returned two rows, and so
/// failed every drain, once two schemas each had a source table of the same
/// name.
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
    // `left join lateral jsonb_each_text(...)` — same "decode JSON via SQL, no
    // serde_json dependency" convention `dependents_of` uses, just for one
    // row instead of a batch.
    let rows = client
        .query(
            "select t.source_version, t.definition_text, t.status, t.source_table, \
                    t.target_table, e.key, e.value \
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
    let def = parse(&text)?;

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

/// Reads back one definition by its target table name — [`definition_by_id`]
/// keyed the other way, for callers that only have the address a `Trellis`
/// caller would use (`docs/decisions/0003-quarantine-storage-and-api.md`'s
/// amendment: a quarantine target is `transform` or `transform.column`,
/// where `transform` is this crate's `target_table`). Used by
/// `staging::quarantine`'s column-resume path to reconstruct the
/// [`super::ast::TransformDef`] whose column it's re-deriving.
///
/// `target_table` is — and, per this doc comment, must stay — the *bare*
/// name every caller here actually has: a `Trellis` API consumer only ever
/// knows the bare name their `TRANSFORM <name> FROM ...` text declared (the
/// grammar has no qualified-target syntax yet — issue #76), and
/// `docs/decisions/0003`'s `transform.column` addressing scheme parses on the
/// first `.` (`app::QuarantineTarget::parse`) — a qualified address here
/// would misparse as a column reference the moment a target table lived
/// outside the default schema. Matched against `target_table`'s bare
/// table-name suffix (`split_part`), not the qualified column directly,
/// since it's been fully-qualified since issue #73.
pub async fn definition_by_target(
    pool: &Pool,
    target_table: &str,
) -> Result<Option<Definition>, CatalogError> {
    let client = pool.get().await?;
    let rows = client
        .query(
            "select t.id, t.source_version, t.definition_text, t.status, t.source_table, \
                    t.target_table, e.key, e.value \
             from transform_definitions t \
             left join lateral jsonb_each_text(t.source_columns) e on true \
             where split_part(t.target_table, '.', 2) = $1",
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
    let def = parse(&text)?;

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
/// either directly (a chained 1-1 transform whose `FROM` *is*
/// `upstream_table`, referencing the column by its bare name) or through a
/// declared relationship whose `to_table` is `upstream_table` (a
/// relationship-enriched field's `<rel>.<column>` path). This is column-level
/// lineage, deliberately *not* [`dependents_of`]'s table-level
/// `schema_edges` walk: that graph answers "does transform X read table Y at
/// all," which is too coarse for ADR-0003's amendment — cascading a paused
/// *column* must not pause a downstream transform's *other* fields that don't
/// actually reference it. Scanning every definition's own field expressions
/// (rather than a persisted edge) is what makes this precise.
///
/// **Untouched by issue #74.** This function never reads `schema_nodes`/
/// `schema_edges` at all — it scans `transform_definitions`/
/// `relationship_definitions` directly, matching `upstream_table` against
/// `def.source` (freshly re-parsed, always bare) and the bare `to_table` of
/// every relationship whose recorded to-side is a definition's target (issue
/// #372), both already-bare-and-self-consistent inputs the qualified graph
/// never enters. `upstream_table` itself stays bare for the
/// same `column_status`-addressing reasons the `target` column below does
/// (see this function's own inline comment on that `select`).
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
/// fuse" section doc comment: `staging::apply_aggregate`'s incremental-delta
/// path has no notion of `column_status` at all). Without this filter, a
/// downstream [`KeySpace::Aggregate`] transform whose field happens to read a
/// just-paused upstream column would get a `column_status` row cascaded onto
/// it that nothing in the aggregate write path ever consults or clears, and
/// that `resume_column`'s cascade walk would later try (and fail) to
/// recompute via `staging::quarantine::recompute_column`'s single-row 1-1
/// recompute path.
pub(crate) async fn column_dependents(
    pool: &Pool,
    upstream_table: &str,
    upstream_column: &str,
) -> Result<Vec<(String, String)>, CatalogError> {
    let client = pool.get().await?;
    column_dependents_via(&**client, upstream_table, upstream_column, true).await
}

/// [`column_dependents`], generalized to *every* downstream key-space (not
/// just [`KeySpace::OneToOne`]) and to any `impl GenericClient` (a plain
/// pooled connection, or a transaction) rather than just a fresh pooled one —
/// the column-granularity dependency-edge infrastructure issues #241/#242
/// both need for a *drop* refusal, as opposed to [`column_dependents`]'s own
/// pause/cascade/resume use.
///
/// **Why the wider key-space net matters here.** `column_dependents`'s
/// 1-1-only filter is correct for its own callers (`staging::quarantine`'s
/// column-pause cascade — nothing outside the 1-1 tier has a `column_status`
/// row to cascade onto), but is exactly *wrong* for a drop refusal: an
/// aggregate transform's `GROUP BY` can key on a to-one relationship path or
/// a chained source column just as readily as a 1-1 field can (issue #137),
/// and dropping the column such a grouping key reads is exactly as
/// destructive as dropping one a 1-1 field reads — the refusal must see it
/// too. So this walks every downstream definition's [`KeySpace::Aggregate`]
/// `group_by` keys (via [`GroupByKey::as_expr`]) in addition to every
/// definition's `fields`, named by [`GroupByKey::target_column_name`] when a
/// grouping key itself is the match — the same target-column-name convention
/// [`super::validate`]'s own grouping-key/field-passthrough checks use.
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

async fn column_dependents_via(
    client: &impl GenericClient,
    upstream_table: &str,
    upstream_column: &str,
    only_one_to_one: bool,
) -> Result<Vec<(String, String)>, CatalogError> {
    let def_rows = client
        .query(
            // `split_part(target_table, '.', 2)`, not the qualified column
            // directly (issue #73): the `String` this returns for each row
            // is pushed straight into `deps` below as a *downstream
            // transform* identifier, which flows into
            // `staging::quarantine`'s `column_status`/`column_pause_cascades`
            // bookkeeping — an entirely bare-keyed subsystem seeded from the
            // bare `transform` a `Trellis` caller passes to `pause_column`/
            // `resume_column`. Returning the newly-qualified spelling here
            // instead would split that bookkeeping across two spellings of
            // the same transform depending on whether a row was reached
            // directly or via cascade.
            "select split_part(target_table, '.', 2), definition_text, source_table \
             from transform_definitions",
            &[],
        )
        .await?;
    let rel_rows = client
        .query(
            "select r.from_schema || '.' || r.from_table, r.name, r.to_table \
             from relationship_definitions r \
             where exists ( \
                 select 1 from transform_definitions d \
                 where d.target_table = r.to_schema || '.' || r.to_table \
             )",
            &[],
        )
        .await?;

    // Keyed by the relationship's qualified from-table (issue #288): a
    // relationship name is unique only per qualified from-table, so the bare
    // `(from_table, name)` pair could name two different relationships.
    //
    // Only relationships whose recorded to-side (issue #372) *is* some
    // definition's target are kept: `upstream_table` is always a bare target
    // name, and bare target suffixes are unique, so the bare `to_table` match
    // below is then exact. Without the filter, a relationship pointing at a
    // same-named non-target table in another schema would match too.
    let mut rel_to_table: HashMap<(String, String), String> = HashMap::new();
    for row in rel_rows {
        let qualified_from: String = row.get(0);
        let name: String = row.get(1);
        let to_table: String = row.get(2);
        rel_to_table.insert((qualified_from, name), to_table);
    }

    let mut deps = Vec::new();
    for row in def_rows {
        let target: String = row.get(0);
        let text: String = row.get(1);
        // The definition's qualified source — what its relationship paths
        // resolve against (issue #288); `def.source` below is the bare
        // spelling this scan's table matching uses.
        let qualified_source: String = row.get(2);
        // A definition already persisted here is expected to always re-parse
        // (the same assumption every other read path in this module makes);
        // skip rather than fail this best-effort lineage scan on the
        // unexpected chance it doesn't, rather than let one bad row prevent
        // cascading a pause to every other, healthy dependent.
        let Ok(def) = parse(&text) else { continue };
        if only_one_to_one && !matches!(def.key_space, KeySpace::OneToOne) {
            continue;
        }
        // `def.source` (freshly re-parsed from `definition_text`), not the
        // persisted `transform_definitions.source_table` column — issue #72
        // made that column fully-qualified, but `upstream_table` here is
        // always a bare *target* table name (a downstream transform's
        // `def.source` naming an upstream one's `def.target`, or a paused
        // column's own bare transform — see `staging::quarantine`'s
        // callers), so comparing against it needs the same bare spelling
        // `def.source` already gives for free, matching the `split_part`
        // read of `target_table` above.
        for field in &def.fields {
            if expr_references_column(
                &field.expr,
                (&def.source, &qualified_source),
                upstream_table,
                upstream_column,
                &rel_to_table,
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
                    &key.as_expr(),
                    (&def.source, &qualified_source),
                    upstream_table,
                    upstream_column,
                    &rel_to_table,
                ) {
                    deps.push((target.clone(), key.target_column_name().to_string()));
                }
            }
        }
    }
    Ok(deps)
}

/// Whether `expr` (one calculated field's expression, belonging to a
/// definition whose `FROM` is `def_source` — its bare and qualified spellings)
/// reads `(upstream_table, upstream_column)` — see [`column_dependents`].
fn expr_references_column(
    expr: &Expr,
    def_source: (&str, &str),
    upstream_table: &str,
    upstream_column: &str,
    rel_to_table: &HashMap<(String, String), String>,
) -> bool {
    match expr {
        Expr::Column(name) => def_source.0 == upstream_table && name == upstream_column,
        Expr::RelationshipPath { rel, column } => {
            column == upstream_column
                && rel_to_table
                    .get(&(def_source.1.to_string(), rel.clone()))
                    .is_some_and(|to_table| to_table == upstream_table)
        }
        Expr::BinaryOp { lhs, rhs, .. } => {
            expr_references_column(
                lhs,
                def_source,
                upstream_table,
                upstream_column,
                rel_to_table,
            ) || expr_references_column(
                rhs,
                def_source,
                upstream_table,
                upstream_column,
                rel_to_table,
            )
        }
        Expr::FunctionCall { args, .. } => args.iter().any(|arg| {
            expr_references_column(
                arg,
                def_source,
                upstream_table,
                upstream_column,
                rel_to_table,
            )
        }),
        Expr::NumberLiteral(_) | Expr::StringLiteral(_) | Expr::TypedLiteral { .. } => false,
    }
}

/// Issue #331: every build-finished flip (the chunk queue's completion of a
/// definition's last chunk or its direct-build job) goes through
/// [`catch_up_if_backfilling`], which must only ever move a definition that is
/// still `backfilling` — never one paused, quarantined or resumed meanwhile.
#[cfg(test)]
mod go_live_tests {
    use super::*;

    #[tokio::test]
    async fn only_a_still_backfilling_definition_starts_catching_up() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let client = db.pool.get().await.expect("acquire connection");
        client
            .execute(
                "insert into source_table_versions (source_table, version) \
                 values ('public.orders', 1)",
                &[],
            )
            .await
            .expect("seed source_table_versions");

        for status in TransformStatus::ALL {
            let id: i64 = client
                .query_one(
                    "insert into transform_definitions \
                     (target_table, source_table, source_version, definition_text, status) \
                     values ($1, 'public.orders', 1, '', $2) returning id",
                    &[&format!("public.t_{}", status.as_str()), &status.as_str()],
                )
                .await
                .expect("seed a definition")
                .get(0);

            let outcome = catch_up_if_backfilling(&**client, id)
                .await
                .expect("catch_up_if_backfilling");
            let persisted: String = client
                .query_one(
                    "select status from transform_definitions where id = $1",
                    &[&id],
                )
                .await
                .expect("read status")
                .get(0);

            if status == TransformStatus::Backfilling {
                assert_eq!(outcome, BuildFinished::CatchingUp);
                assert_eq!(persisted, "catching_up");
            } else {
                assert_eq!(outcome, BuildFinished::LeftAs(status));
                assert_eq!(persisted, status.as_str(), "{status:?} is left untouched");
            }
        }
    }

    /// The chunk queue's completion parks its catch-up marker for every
    /// definition it leaves unfrozen, not only one it flipped itself. That
    /// includes a definition resume's rebuild already took `live` while a
    /// leftover chunk (#332) was still writing. A frozen definition gets no
    /// marker, since its resume parks its own.
    #[tokio::test]
    async fn chunk_completion_parks_a_catch_up_unless_the_definition_is_frozen() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let mut client = db.pool.get().await.expect("acquire connection");
        client
            .execute(
                "insert into source_table_versions (source_table, version) \
                 values ('public.orders', 1)",
                &[],
            )
            .await
            .expect("seed source_table_versions");

        for status in TransformStatus::ALL {
            client
                .execute("delete from pending_backfill", &[])
                .await
                .expect("clear markers");
            let id: i64 = client
                .query_one(
                    "insert into transform_definitions \
                     (target_table, source_table, source_version, definition_text, status) \
                     values ($1, 'public.orders', 1, $3, $2) returning id",
                    &[
                        &format!("public.t_{}", status.as_str()),
                        &status.as_str(),
                        &format!("TRANSFORM t_{} FROM orders SELECT a AS x", status.as_str()),
                    ],
                )
                .await
                .expect("seed a definition")
                .get(0);

            let txn = client.transaction().await.expect("begin");
            let left = complete_direct_backfill(&txn, id)
                .await
                .expect("complete_direct_backfill");
            txn.commit().await.expect("commit");

            let parked: bool = client
                .query_one(
                    "select exists(select 1 from pending_backfill \
                     where table_name = 'public.orders')",
                    &[],
                )
                .await
                .expect("read markers")
                .get(0);
            let expected = if status == TransformStatus::Backfilling || status.is_applying() {
                TransformStatus::CatchingUp
            } else {
                status
            };
            assert_eq!(left, expected, "{status:?}: status left behind");
            assert_eq!(
                parked,
                !status.is_frozen(),
                "{status:?}: a catch-up marker is parked exactly when the definition is unfrozen"
            );
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
                // Issue #108: `money` used to be this test's example of an
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
    /// rather than hardcoding a category — [`ValidationError::DuplicateRelationshipName`]
    /// is the one variant that isn't plain [`ErrorCode::Validation`], so this
    /// also exercises that [`ValidationError`]'s own special case survives
    /// the extra layer of nesting.
    #[test]
    fn validate_delegates_to_the_wrapped_validation_error() {
        let inner = ValidationError::DuplicateRelationshipName {
            from_table: "orders".to_string(),
            name: "customer".to_string(),
        };
        let expected = inner.code();
        let wrapped = CatalogError::Validate(inner);

        assert_eq!(wrapped.code(), expected);
        assert_eq!(wrapped.code(), ErrorCode::Conflict);
    }
}

/// Issue #108: [`encode_value_type`]/[`decode_value_type`] are the
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
        // Issue #111's widths, from `IntWidth::ALL` so a future width is
        // covered by construction rather than by remembering to add it.
        let all = all
            .into_iter()
            .chain(IntWidth::ALL.into_iter().map(ValueType::Integer))
            // Issue #112's widths, from `FloatWidth::ALL` for the same
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
        // Issue #108 review: the pre-existing lattice's persisted spelling
        // must stay byte-identical — these tokens are already durably stored
        // in real `transform_definitions` rows, so changing them would break
        // every definition persisted before this issue.
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
    use super::{StatusNode, TransformStatus, derive_reported_statuses};

    fn node(id: i64, target: &str, status: TransformStatus, reads: &[&str]) -> StatusNode {
        StatusNode {
            id,
            target: target.to_string(),
            status,
            reads: reads.iter().map(|t| t.to_string()).collect(),
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
    fn a_relationship_to_side_is_an_upstream_too() {
        use TransformStatus::*;
        // `public.r` reads its own source and, through a relationship, the
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
