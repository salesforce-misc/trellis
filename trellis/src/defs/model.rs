//! The definition model (issue #23): a validated [`TransformDef`] (issue
//! #22's AST) plus the identity the catalog assigns once it's persisted.

use std::collections::HashMap;

use super::ast::{RelationshipDef, TransformDef, ValueType};
use super::validate::RelationshipWarning;

/// A transform definition as stored in the catalog: the parsed AST — source
/// table, target table name, and calculated fields are all on
/// [`TransformDef`] already — plus the catalog-assigned id, the source
/// table's version at the moment this definition was created (the value
/// stage 05's version fence will read), and the source-column type map `def`
/// was validated against (issue #63's write-path gap: persisted so the
/// physical apply path — [`crate::staging::apply::compute`] — can evaluate
/// non-Numeric fields the same way [`super::catalog::create_definition`]
/// validated them, rather than re-deriving or defaulting to Numeric).
#[derive(Debug, Clone, PartialEq)]
pub struct Definition {
    pub id: i64,
    pub source_version: i64,
    pub def: TransformDef,
    pub source_columns: HashMap<String, ValueType>,
    /// Where this transform is in its lifecycle (issue #55) — see
    /// [`TransformStatus`].
    pub status: TransformStatus,
    /// The persisted, fully-qualified `"schema.table"` form of `def.source`
    /// — `transform_definitions.source_table` read back verbatim (issue #72
    /// resolved it once, at acceptance time, via `search_path` for a bare
    /// `FROM`, or trusted an explicit `FROM <schema>.<source>` outright as of
    /// issue #76; see `catalog::create_definition_inner`'s `qualified_source`).
    ///
    /// **This is the identity every physical SQL-builder that reads the live
    /// source table at backfill/CDC-apply/quarantine-recompute time must use**
    /// (ADR-0007) — never `def.source` alone, which is always bare (see
    /// [`super::ast::TransformDef`]'s own doc comment) and, left unqualified
    /// in emitted SQL, would silently resolve against whichever schema the
    /// executing session's pinned `search_path` (`Config::schema`,
    /// `Config::target_schema`, `"public"` — see `pool::session_bootstrap`)
    /// happens to carry, not necessarily the schema this definition actually
    /// resolved against at creation time. `defs::ddl::qualified_source_table`
    /// turns this into a directly-interpolatable, independently-quoted DDL/DML
    /// fragment; a `to_regclass($1)`-style introspection query can bind this
    /// string as-is.
    pub source_table: String,
    /// The persisted, fully-qualified `"schema.table"` form of `def.target`
    /// — `transform_definitions.target_table` read back verbatim (issue #73
    /// resolved it once, at acceptance time, mirroring `source_table` above;
    /// issue #74 additionally made this the exact identity `schema_nodes`'
    /// target-side node is keyed on, so it now doubles as the qualified key
    /// a caller with only `def.target` (bare) can use to re-enter the
    /// `schema_nodes`/`schema_edges` graph — see
    /// `staging::apply::compute`'s `downstream_readers` check for the one
    /// call site that needs exactly this).
    pub target_table: String,
}

/// A transform's lifecycle status (issue #55), persisted as
/// `transform_definitions.status`: `docs/transforms.md`'s "Status" section
/// documents the full state machine this mirrors. Registration
/// ([`super::catalog::install_definition`]) persists
/// [`TransformStatus::WaitingToBackfill`], and the backfill discharge
/// (ADR-0016) moves it on: to [`TransformStatus::Backfilling`] together with
/// the chunks that build it, then [`TransformStatus::Live`] when the last one
/// finishes, or straight to `live` for a ring-built definition. Until issue
/// #419, an aggregate or relationship-enriched 1-1 definition is persisted
/// `backfilling` by registration itself and flipped `live` once its in-call
/// build completes.
///
/// [`TransformStatus::Quarantined`] and [`TransformStatus::Paused`] are the
/// two triggers of ADR-0014's single "frozen" state: the poison fuse trips
/// the first, an operator [`PAUSE`](crate::Trellis::apply) sets the
/// second, and nothing else distinguishes them — both are simply *not*
/// `live`, which is the one gate the claim-time fold has ever honored (the
/// `t.status = 'live'` predicate in [`super::catalog::dependents_of`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransformStatus {
    WaitingToBackfill,
    Backfilling,
    Live,
    Quarantined,
    /// Deliberately frozen by an operator (issue #142, ADR-0014) — the same
    /// freeze [`TransformStatus::Quarantined`] is, reached by the other of
    /// the two triggers the ADR names. The target holds its current,
    /// now-stale value until
    /// [`crate::staging::quarantine::resume_transform`] rebuilds it by a
    /// fresh backfill; meanwhile its share of the change stream is drained
    /// for its siblings and is not recoverable by replay, so a paused
    /// definition never pins the staging ring.
    ///
    /// Kept a distinct persisted word from `quarantined` only so the
    /// *trigger* stays legible: [`crate::Trellis::quarantined`] reports
    /// poison incidents, and an operator pause taken to stage a schema
    /// change is not one.
    Paused,
}

impl TransformStatus {
    /// Every variant, once — the list [`Self::dispatchable`] derives the
    /// chunk queue's positive allowlist from, so that gate can never drift
    /// out of step with this enum the way a hand-written `status <> 'paused'`
    /// denylist did (issue #231).
    ///
    /// Adding a variant means adding it here as well as to [`Self::as_str`]
    /// and [`Self::from_persisted`] (both of which the compiler will already
    /// stop you on) and to `transform_definitions.status`' `check`
    /// constraint; `every_variant_is_listed_in_all` below is the reminder.
    pub const ALL: [TransformStatus; 5] = [
        TransformStatus::WaitingToBackfill,
        TransformStatus::Backfilling,
        TransformStatus::Live,
        TransformStatus::Quarantined,
        TransformStatus::Paused,
    ];

    /// ADR-0014's single **frozen** state, both of its triggers — the one
    /// predicate every pause/resume/drop precondition and every dispatch gate
    /// in this crate asks, rather than each naming `paused` (or `paused` and
    /// `quarantined`) for itself.
    ///
    /// A frozen definition is not being maintained: the claim-time fold does
    /// not write to its target, and [`super::chunk_queue::claim_chunks`] does
    /// not hand out its backfill chunks. Both follow from this one method, so
    /// a future third frozen state closes both gates by being named here and
    /// nowhere else.
    pub fn is_frozen(self) -> bool {
        matches!(self, TransformStatus::Paused | TransformStatus::Quarantined)
    }

    /// The persisted words of every status a definition may still be handed
    /// *new* work in — i.e. every non-[`Self::is_frozen`] variant, derived
    /// from [`Self::ALL`] rather than spelled out.
    ///
    /// Deliberately an allowlist, and deliberately wider than `live`: a
    /// definition's durable backfill chunks exist precisely while it is
    /// `waiting_to_backfill`/`backfilling`, so "only dispatch to a live
    /// definition" would be wrong for the chunk queue. "Dispatch to anything
    /// that is not frozen" is the real rule, and stating it positively means a
    /// status added later has to be *chosen* into dispatch instead of falling
    /// into it (issue #231).
    pub fn dispatchable() -> Vec<&'static str> {
        TransformStatus::ALL
            .iter()
            .filter(|status| !status.is_frozen())
            .map(|status| status.as_str())
            .collect()
    }

    /// The text this variant is persisted/queried as in
    /// `transform_definitions.status`.
    pub fn as_str(self) -> &'static str {
        match self {
            TransformStatus::WaitingToBackfill => "waiting_to_backfill",
            TransformStatus::Backfilling => "backfilling",
            TransformStatus::Live => "live",
            TransformStatus::Quarantined => "quarantined",
            TransformStatus::Paused => "paused",
        }
    }

    /// Parses [`Self::as_str`]'s persisted form back, or `None` for any
    /// other text — meaning the row was written by something other than
    /// this module's own writers, since `transform_definitions.status`'s
    /// `check` constraint only allows these five values. Named
    /// `from_persisted` for the same reason [`RelationshipCardinality::from_persisted`]
    /// is: this isn't `std::str::FromStr` (no matching `Err` type worth
    /// inventing for a value that should only ever come from this table's
    /// own `check` constraint).
    pub fn from_persisted(text: &str) -> Option<Self> {
        match text {
            "waiting_to_backfill" => Some(TransformStatus::WaitingToBackfill),
            "backfilling" => Some(TransformStatus::Backfilling),
            "live" => Some(TransformStatus::Live),
            "quarantined" => Some(TransformStatus::Quarantined),
            "paused" => Some(TransformStatus::Paused),
            _ => None,
        }
    }
}

/// A relationship declaration as stored in the catalog (issue #26): the
/// parsed [`RelationshipDef`] plus the catalog-assigned id. Unlike
/// [`Definition`], there's no `source_version`/`source_columns` to carry —
/// a relationship's endpoints aren't validated against a live column-type
/// map the way a transform's source is (no source-schema DDL, ADR-0005), so
/// nothing here depends on the from-side table's version. `cardinality` is
/// [`super::catalog::create_relationship`]'s one piece of derived state
/// (issue #27): computed once, live against `pg_catalog`, at creation time
/// and persisted rather than re-introspected on every read, so a later
/// reference-time check (validating a bare-path vs. aggregate-wrapped use of
/// the relationship — deferred past issue #27, since no such reference
/// syntax resolves yet) has a stable answer even if the underlying
/// PK/`UNIQUE` index is later dropped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationshipDefinition {
    pub id: i64,
    /// The schema `def.from_table` resolved to when the relationship was
    /// declared (`relationship_definitions.from_schema`, issues #285/#288).
    /// `def.from_table` is the bare name as written; this is what makes it
    /// one specific table. A relationship's identity is
    /// `(from_schema, from_table, name)`, so `blog.posts.author` and
    /// `shop.posts.author` are two different relationships.
    pub from_schema: String,
    pub def: RelationshipDef,
    pub cardinality: RelationshipCardinality,
    /// Non-fatal caveats surfaced alongside this (still-successful)
    /// definition (issue #31) — e.g. a missing from-side index. Empty on the
    /// read-back path ([`super::catalog::relationship_by_name`]): a warning
    /// is creation-time guidance, not a fact about the persisted row, and
    /// `pg_catalog` state (an index dropped later) can drift from what was
    /// true at creation anyway.
    pub warnings: Vec<RelationshipWarning>,
}

impl RelationshipDefinition {
    /// The from-table's fully-qualified `"schema.table"` identity, the same
    /// spelling `transform_definitions.source_table`, `schema_nodes` and a
    /// staged `src_table` all use. Anything that reads the from-side table,
    /// or matches it against a transform's source, must use this rather than
    /// the bare `def.from_table`: a bare name resolves through whatever
    /// `search_path` the reading session has, which need not land on the
    /// table this relationship was declared against.
    pub fn qualified_from_table(&self) -> String {
        format!("{}.{}", self.from_schema, self.def.from_table)
    }
}

/// Whether a relationship's to-side is guaranteed at most one row per
/// from-row (issue #27, ADR-0006): [`RelationshipCardinality::ToOne`] iff
/// `to_col` is the sole column of a `PRIMARY KEY` or `UNIQUE` index on
/// `to_table` at the moment the relationship is declared, `ToMany`
/// otherwise. ADR-0006 requires a `ToMany` relationship's bare-path
/// references to be rejected in favor of an aggregate-wrapped form — that
/// rejection is reference-time (checked where a relationship is *used* in a
/// calculated field), and deferred past issue #27 since no such reference
/// resolves yet (see [`super::ast::Expr::RelationshipPath`]'s current
/// unconditional rejection in [`super::validate`]); this type exists now so
/// that check has something to read once it lands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelationshipCardinality {
    ToOne,
    ToMany,
}

impl RelationshipCardinality {
    /// The text this variant is persisted/queried as in
    /// `relationship_definitions.cardinality`.
    pub fn as_str(self) -> &'static str {
        match self {
            RelationshipCardinality::ToOne => "one",
            RelationshipCardinality::ToMany => "many",
        }
    }

    /// Parses [`Self::as_str`]'s persisted form back, or `None` for any
    /// other text — meaning the row was written by something other than
    /// [`super::catalog::create_relationship`]. Named `from_persisted`
    /// rather than `from_str` so it isn't confused with (and doesn't trip
    /// clippy's `should_implement_trait` lint against) `std::str::FromStr`,
    /// which this isn't — there's no matching `Err` type worth inventing
    /// for a value that should only ever come from this table's own
    /// `check` constraint.
    pub fn from_persisted(text: &str) -> Option<Self> {
        match text {
            "one" => Some(RelationshipCardinality::ToOne),
            "many" => Some(RelationshipCardinality::ToMany),
            _ => None,
        }
    }
}

/// Which role [`super::catalog::resolve_node`] is being asked to establish
/// for a table: a **source** table is one Trellis reads over logical
/// replication and never issues DDL against (ADR-0005); a **target** table
/// is one a transform declares and creates. These aren't mutually
/// exclusive on a table over its lifetime — a transform's target is a
/// completely ordinary table a *later* transform can subscribe to as its
/// source (chained/multi-hop transforms, exercised by
/// `trellis/tests/apply.rs`'s two-hop propagation tests), so the same
/// physical table ends up resolved under both roles. [`SchemaNode`] tracks
/// that as two independent flags rather than one exclusive kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeKind {
    Source,
    Target,
}

/// A first-class identity for a table Trellis knows about — as a source, a
/// target, or (via chained transforms) both — that transforms (and, as of
/// issue #74, relationships too) resolve their endpoints against instead of
/// a bare table-name string. One row per physical table: `is_source`/
/// `is_target` each start `false` and are only ever set to `true` by
/// [`super::catalog::resolve_node`], never back to `false`. Only identity is
/// persisted ([`super::catalog`]'s `schema_nodes` table); a node's columns
/// and types are introspected live from `pg_catalog`/`information_schema`
/// rather than cached here, per ADR-0005.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaNode {
    pub id: i64,
    /// The fully-qualified `"schema.table"` identity this node is keyed on
    /// (issue #74, ADR-0007). `public.posts` and `archive.posts` are
    /// distinct rows with independent `is_source`/`is_target` flags and
    /// independent `schema_edges` — before issue #74 this held the bare
    /// table name, so same-named tables in different schemas collided into
    /// one node; every caller must now pass (and compare against) the
    /// qualified form, never re-deriving it here (same resolve-once
    /// discipline as `transform_definitions.source_table`/`target_table`).
    pub table_name: String,
    pub is_source: bool,
    pub is_target: bool,
}

/// Which kind of dependency a [`super::catalog`] edge represents (issue
/// #21). [`EdgeKind::Relationship`] is persisted by `create_relationship`
/// (issue #26); [`EdgeKind::Source`] remains the only kind a transform
/// itself persists — a transform's `FROM` is its only join input the AST
/// can produce (see `trellis/src/defs/ast.rs`'s `TransformDef::source`, a
/// single `String`, no multi-source join yet). `Join` exists so the column
/// this enum backs doesn't need a migration when join-edge persistence
/// lands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EdgeKind {
    Source,
    // Nothing persists a join edge yet — see this enum's doc comment for why
    // the variant exists ahead of the writer that will construct it.
    #[allow(dead_code)]
    Join,
    Relationship,
}

impl EdgeKind {
    /// The text this variant is persisted/queried as in `schema_edges.kind`.
    pub fn as_str(self) -> &'static str {
        match self {
            EdgeKind::Source => "source",
            EdgeKind::Join => "join",
            EdgeKind::Relationship => "relationship",
        }
    }
}

/// A directed dependency edge between two [`SchemaNode`]s: `to_node`
/// depends on `from_node` via `kind` (e.g. a `Source` edge from `orders` to
/// `order_totals` means `order_totals` is a transform target reading from
/// `orders`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg(any(test, feature = "internals"))]
pub struct SchemaEdge {
    pub id: i64,
    pub from_node_id: i64,
    pub to_node_id: i64,
    pub kind: EdgeKind,
}

#[cfg(test)]
mod tests {
    use super::TransformStatus;

    /// [`TransformStatus::ALL`] is hand-maintained, so this is the guard that
    /// makes forgetting it loud: the `match` has no wildcard, so adding a
    /// variant fails to compile here until it is named, and the length
    /// assertion fails if `ALL` grows without this test being revisited.
    ///
    /// The pair is a forcing function, not a proof: naming a new variant in
    /// the `match` arm alone satisfies the compiler, so a variant can still be
    /// left out of `ALL`. That residual is deliberately fail-*closed* —
    /// [`TransformStatus::dispatchable`] is an allowlist built from `ALL`, so
    /// a variant missing from `ALL` is simply never handed work, which is the
    /// safe direction. `dispatchable_is_every_unfrozen_status` below pins the
    /// resulting word list, so the omission surfaces there rather than
    /// silently re-opening a gate.
    #[test]
    fn every_variant_is_listed_in_all() {
        for status in TransformStatus::ALL {
            match status {
                TransformStatus::WaitingToBackfill
                | TransformStatus::Backfilling
                | TransformStatus::Live
                | TransformStatus::Quarantined
                | TransformStatus::Paused => {}
            }
            assert_eq!(
                TransformStatus::from_persisted(status.as_str()),
                Some(status),
                "every listed variant round-trips through its persisted word"
            );
        }
        assert_eq!(
            TransformStatus::ALL.len(),
            5,
            "bump this alongside `ALL` when a status is added"
        );
    }

    /// The chunk queue's allowlist is exactly "not frozen" — asserted against
    /// the words themselves, since that is what the SQL gate binds.
    #[test]
    fn dispatchable_is_every_unfrozen_status() {
        assert_eq!(
            TransformStatus::dispatchable(),
            vec!["waiting_to_backfill", "backfilling", "live"],
            "a frozen definition is handed no new work; everything else is"
        );
        assert!(TransformStatus::Paused.is_frozen());
        assert!(TransformStatus::Quarantined.is_frozen());
    }
}
