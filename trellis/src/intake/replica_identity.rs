//! The checked REPLICA IDENTITY FULL requirement (issue #7): a definition
//! whose derivation needs a changed row's *old* image (a child deleted from
//! a one-to-many aggregate, or a re-parented row) must be rejected rather
//! than silently fed a missing image, and the rejection must name the exact
//! DDL an operator needs to run — Trellis does not issue DDL against tables
//! it doesn't own. See docs/staging-and-claiming/01-intake-and-lsn-confirmation.md.
//!
//! Issue #173 phase 2: three independently-added call sites each re-derived
//! "does this plan need a source-table guarantee?" ad hoc —
//! `defs::catalog::assert_replica_identity_supports_aggregate` (issue #47),
//! `defs::catalog::assert_replica_identity_supports_projection` (issue #129,
//! extended to the from-side by #158), and this module's own
//! [`require_replica_identity_full`] underneath both. [`required_source_guarantees`]
//! is the one place that now answers "given this resolved plan, what
//! source-table guarantees does it need?" — a plain, DB-free function over
//! [`ResolvedPlan`] — so a plan shape can only be *added* to the match in
//! [`required_source_guarantees`], never silently skipped the way a fourth
//! ad hoc assertion could skip it: [`ResolvedPlan`] and [`SourceGuarantee`]
//! are both matched exhaustively (no wildcard arm), so a new variant is a
//! compile error everywhere it isn't handled, not a silent gap. The three
//! original call sites are kept as thin wrappers over this derivation (see
//! their doc comments in `defs::catalog`) rather than deleted outright, so
//! this migration doesn't have to be one big-bang change.
//!
//! The underlying rule is [`needs_old_image_for_key_space`]: `false` for
//! [`KeySpace::OneToOne`] (a pure function of the *current* row) and `true`
//! for [`KeySpace::Aggregate`] (issue #47: a row leaving or re-entering a
//! group needs the row's old image to know which group it's leaving).
//! [`required_source_guarantees`] is its only caller — a `needs_old_image`
//! wrapper over a whole `TransformDef` existed until phase 2 left it with
//! no callers but its own test, and issue #190 removed it.
//! [`require_replica_identity_full`] takes a plain `bool`, so its own
//! test can exercise the `true` side directly rather than having to
//! construct a `KeySpace::Aggregate` definition just to reach it; it's also
//! what [`crate::defs::catalog::check_source_guarantees`] calls per derived
//! [`SourceGuarantee::ReplicaIdentityFull`], once the live catalog has
//! confirmed the table doesn't already carry it — that split ("the plan
//! needs it" vs. "the table already has it") is why this function takes a
//! bare bool instead of doing its own `pg_catalog` lookup.
use super::error::IntakeError;
use crate::defs::ast::KeySpace;
#[cfg(test)]
use crate::defs::ast::TransformDef;

/// Whether `key_space` needs the old image of a row — `false` for
/// [`KeySpace::OneToOne`], `true` for [`KeySpace::Aggregate`] (see the
/// module doc). The rule behind [`required_source_guarantees`]'s
/// `Transform` arm.
fn needs_old_image_for_key_space(key_space: &KeySpace) -> bool {
    match key_space {
        KeySpace::OneToOne => false,
        // A row leaving or re-entering a group (delete, or an update that
        // changes a grouping-key value) changes that group's aggregate, and
        // recomputing it needs the row's old image to know which group it's
        // leaving.
        KeySpace::Aggregate { .. } => true,
    }
}

/// A source-table guarantee some resolved plan's propagation paths depend
/// on, derived from the plan itself (issue #173 phase 2) rather than
/// asserted ad hoc per call site. `REPLICA IDENTITY FULL` is the only
/// guarantee any current path needs; add a variant here — not a fourth ad
/// hoc assertion — when a path needs a new one, so
/// [`crate::defs::catalog::check_source_guarantees`]'s exhaustive match
/// forces every checker to handle it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourceGuarantee {
    /// `table` (as it should be named in an error message) must carry
    /// `REPLICA IDENTITY FULL` so a delete/update on it ships a complete
    /// pre-image over `pgoutput`. `qualified_table` is the fully-qualified
    /// identity to actually query `pg_catalog` with — kept separate from
    /// `table` because a bare name resolved via `search_path` can silently
    /// name the wrong same-suffixed table (see
    /// `defs::catalog::assert_replica_identity_supports_aggregate`'s own
    /// doc comment for the issue #76 follow-up this guards against).
    ReplicaIdentityFull {
        table: String,
        qualified_table: String,
    },
}

/// The slice of a resolved plan [`required_source_guarantees`] reasons
/// over — enough of "definition + its relationships + its key space"
/// (issue #173's own phrasing) to derive replica-identity guarantees from,
/// across the two places a plan is installed today.
#[derive(Debug, Clone, Copy)]
pub enum ResolvedPlan<'a> {
    /// A transform definition about to be installed
    /// (`defs::catalog::create_definition_inner`), keyed by its key space —
    /// see [`needs_old_image_for_key_space`] for why only `Aggregate` needs
    /// anything today.
    Transform {
        source_table: &'a str,
        qualified_source_table: &'a str,
        key_space: &'a KeySpace,
    },
    /// A to-one relationship's endpoints, about to be installed
    /// (`defs::catalog::create_relationship`, cardinality already
    /// resolved). A to-many relationship's requirement is a separate,
    /// weaker, index-based check
    /// (`defs::catalog::assert_replica_identity_supports_to_many`, issue
    /// #41) this derivation doesn't cover — see that function's doc
    /// comment for why a single-column covering index is enough there but
    /// not here.
    ToOneRelationship {
        to_table: &'a str,
        qualified_to_table: &'a str,
        from_table: &'a str,
        qualified_from_table: &'a str,
    },
}

/// Every source-table guarantee `plan`'s propagation paths depend on —
/// issue #173 phase 2's single derivation, replacing three independently
/// re-derived assertions with one place that answers "given this plan, what
/// guarantees does it need?" Pure and DB-free: [`crate::defs::catalog::check_source_guarantees`]
/// is what actually checks each returned guarantee against the live
/// catalog.
pub fn required_source_guarantees(plan: &ResolvedPlan) -> Vec<SourceGuarantee> {
    match plan {
        ResolvedPlan::Transform {
            source_table,
            qualified_source_table,
            key_space,
        } => {
            if needs_old_image_for_key_space(key_space) {
                vec![SourceGuarantee::ReplicaIdentityFull {
                    table: (*source_table).to_string(),
                    qualified_table: (*qualified_source_table).to_string(),
                }]
            } else {
                Vec::new()
            }
        }
        ResolvedPlan::ToOneRelationship {
            to_table,
            qualified_to_table,
            from_table,
            qualified_from_table,
        } => vec![
            SourceGuarantee::ReplicaIdentityFull {
                table: (*to_table).to_string(),
                qualified_table: (*qualified_to_table).to_string(),
            },
            SourceGuarantee::ReplicaIdentityFull {
                table: (*from_table).to_string(),
                qualified_table: (*qualified_from_table).to_string(),
            },
        ],
    }
}

/// Rejects `src_table` if `needs_old_image` is `true`, with the exact
/// `ALTER TABLE ... REPLICA IDENTITY FULL;` text the error must carry
/// (issue #7's requirement) so an operator can copy it verbatim.
pub fn require_replica_identity_full(
    src_table: &str,
    needs_old_image: bool,
) -> Result<(), IntakeError> {
    if needs_old_image {
        return Err(IntakeError::ReplicaIdentityRequired {
            table: src_table.to_string(),
            statement: format!("ALTER TABLE {src_table} REPLICA IDENTITY FULL;"),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::defs::ast::Predicate;

    fn one_to_one_def(source: &str) -> TransformDef {
        TransformDef {
            target: "derived".to_string(),
            source: source.to_string(),
            key_space: KeySpace::OneToOne,
            fields: vec![],
            predicate: Predicate::True,
            explicit_source_schema: None,
            explicit_target_schema: None,
        }
    }

    #[test]
    fn one_to_one_transforms_never_need_the_old_image() {
        assert!(!needs_old_image_for_key_space(
            &one_to_one_def("orders").key_space
        ));
    }

    #[test]
    fn a_definition_that_does_need_it_is_rejected_with_the_exact_ddl() {
        // No `KeySpace` variant needs the old image yet, so construct the
        // `true` side directly to exercise the rejection.
        let err = require_replica_identity_full("line_items", true)
            .expect_err("a definition needing the old image must be rejected");
        assert_eq!(
            err.to_string(),
            "table line_items needs its old row image for a derivation that requires it; run \
             this against the source database first: ALTER TABLE line_items REPLICA IDENTITY \
             FULL;"
        );
    }

    #[test]
    fn a_definition_that_does_not_need_it_is_accepted() {
        require_replica_identity_full("orders", false).expect("should not be rejected");
    }

    // --- `required_source_guarantees` (issue #173 phase 2) ---

    #[test]
    fn a_one_to_one_transform_plan_requires_no_guarantees() {
        let key_space = KeySpace::OneToOne;
        let plan = ResolvedPlan::Transform {
            source_table: "orders",
            qualified_source_table: "public.orders",
            key_space: &key_space,
        };
        assert_eq!(required_source_guarantees(&plan), Vec::new());
    }

    #[test]
    fn an_aggregate_transform_plan_requires_replica_identity_full_on_its_source() {
        let key_space = KeySpace::Aggregate { group_by: vec![] };
        let plan = ResolvedPlan::Transform {
            source_table: "posts",
            qualified_source_table: "public.posts",
            key_space: &key_space,
        };
        assert_eq!(
            required_source_guarantees(&plan),
            vec![SourceGuarantee::ReplicaIdentityFull {
                table: "posts".to_string(),
                qualified_table: "public.posts".to_string(),
            }]
        );
    }

    #[test]
    fn a_to_one_relationship_plan_requires_replica_identity_full_on_both_endpoints() {
        let plan = ResolvedPlan::ToOneRelationship {
            to_table: "authors",
            qualified_to_table: "public.authors",
            from_table: "posts",
            qualified_from_table: "public.posts",
        };
        assert_eq!(
            required_source_guarantees(&plan),
            vec![
                SourceGuarantee::ReplicaIdentityFull {
                    table: "authors".to_string(),
                    qualified_table: "public.authors".to_string(),
                },
                SourceGuarantee::ReplicaIdentityFull {
                    table: "posts".to_string(),
                    qualified_table: "public.posts".to_string(),
                },
            ]
        );
    }

    /// Pins that [`required_source_guarantees`] doesn't lose the from-side
    /// requirement issue #158 added to the old ad hoc
    /// `assert_replica_identity_supports_projection` call site (two
    /// separate calls, one per endpoint) — a single `ToOneRelationship`
    /// plan must still surface *both* endpoints' guarantees from one
    /// derivation, not just the to-side #129 originally checked.
    #[test]
    fn a_to_one_relationship_plan_does_not_forget_the_from_side_issue_158_added() {
        let plan = ResolvedPlan::ToOneRelationship {
            to_table: "authors",
            qualified_to_table: "public.authors",
            from_table: "posts",
            qualified_from_table: "public.posts",
        };
        let guarantees = required_source_guarantees(&plan);
        let names: Vec<&str> = guarantees
            .iter()
            .map(|g| {
                let SourceGuarantee::ReplicaIdentityFull { table, .. } = g;
                table.as_str()
            })
            .collect();
        assert!(
            names.contains(&"posts"),
            "from-side table must appear in the derived guarantees, got {names:?}"
        );
        assert!(
            names.contains(&"authors"),
            "to-side table must appear in the derived guarantees, got {names:?}"
        );
    }
}
