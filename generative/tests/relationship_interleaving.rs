//! Issue #138 (epic #127 phase 2): stresses the settled-parent-projection
//! mechanism (#132's four correctness rules) for to-one relationships under
//! the specific timing #34's original relationship suite never exercised —
//! a parent (to-side) change landing close enough to a from-side change on
//! the *same* parent that the two can race across a seal boundary, an
//! intake-lag window, or an out-of-order segment drain.
//!
//! [`generative::generate::build_relationship_interleaving_scenario`] builds
//! the five scenario shapes (the canonical parent-field-update case, plus
//! #138's own named variants: parent insert, parent delete, FK re-point to a
//! nonexistent parent, and a NULL parent) via #34's existing
//! `build_program_multi_with_relationships`/`attach_relationship_fields`
//! machinery, with two deliberately-adjacent critical ops appended by hand
//! (see that function's module doc comment for why the ordinary generator
//! can't produce this shape at all: a relationship always points from a
//! lower-indexed table to a higher-indexed one, and ops are emitted one
//! table's entire stream at a time, so a from-side op and a parent-side op
//! can never land adjacent to each other in a generated stream).
//!
//! Every test below is a hand-built pin, not a proptest property: the
//! interesting variable here is a small, discrete set of *shapes* (which
//! variant, which timing), not a continuous value domain proptest's
//! shrinking would help explore — matching `tests/convergence.rs`'s own
//! relationship pins (one named test per engine-supported shape) rather than
//! `tests/convergence.rs`'s `trivial_program`-driven property.
//!
//! Two timing modes are driven per variant:
//!
//! - **`run_in_the_same_intake_window`** (#138 item 2, the intake-lag
//!   window): the parent-side and from-side critical ops are applied
//!   back-to-back with no intervening `quiesce()` call, so the from-side
//!   change may still be uncommitted-to-the-ring (behind the watermark) when
//!   the parent's reverse work is enumerated. Single-worker, no forced seal
//!   — the cheap, always-on half of the coverage.
//! - **`run_across_a_seal_boundary`** (#138 items 1 and 3, the seal-boundary
//!   and out-of-order-segment-drain scenarios): the parent-side change is
//!   sealed into its own segment before the from-side change is even
//!   applied, using [`ManualBackend::force_seal_active_segment`] — mirroring
//!   `trellis/tests/spike_102.rs`'s
//!   `spike_a2_a_from_side_insert_drains_before_the_parents_reverse_work`
//!   (branch `spike/issue-102-validation-v2`) — then a multi-worker backend
//!   quiesces both now-simultaneously-claimable segments, letting the real
//!   engine's own claim/drain scheduling (not the harness) decide which
//!   drains first.
//!
//! Both modes assert the same property every other file in this crate does
//! (design doc §4): once the backend reaches quiescence, the materialized
//! target must equal the independent oracle recompute
//! ([`generative::run::check_program`]) — regardless of which of the two
//! critical ops the engine happened to drain first.
//!
//! # Part 2 — the obligation-table test matrix (issue #173 phase 4)
//!
//! `docs/relationship-propagation.md` (issue #173 phases 1-3) is the prose
//! obligation table: six propagation paths as columns, six recurring edge
//! inputs as rows, each cell either a code site + pinning test, a structural
//! **N/A**, or a named **GAP**. That table is hand-maintained prose, so a
//! genuinely missing cell reads the same as one nobody has checked yet.
//! `obligation_matrix` below is the same table turned into code: `Path` and
//! `EdgeInput` are closed enums, and `cell()` is a `match` over their full
//! cross product with no wildcard arm, so adding a seventh path or a
//! seventh edge input is a compile error here until every new cell is
//! explicitly classified — the same "enums for closed vocabularies, so a
//! new variant forces every `match` to be revisited" discipline
//! `docs/testing-strategy.md` §1 names and `ReverseTrigger`
//! (`trellis/src/staging/apply.rs`, issue #173 phase 3) already applies to
//! production code, applied here to test coverage itself.
//!
//! Every `Cell::Handled` names the real test(s) that pin it — in this file
//! when the scenario fits this file's `ManualBackend`-driven interleaving
//! harness, or elsewhere (`generative/tests/convergence.rs`,
//! `trellis/tests/*.rs`) by `file::test_name` when it doesn't; nothing is
//! duplicated. Every `Cell::NotApplicable` names the structural reason a
//! path can never see that input. A `Cell::Gap` would name a real,
//! unaddressed hole whose fix needs production code, not just a test — this
//! phase found none that couldn't be closed with a test alone (see the two
//! cells below marked "closes a gap issue #173 phase 4 found"), so no arm
//! constructs it today; the variant stays because the next path or edge
//! input that arrives without full coverage should reach for it rather than
//! silently marking `Handled`.
//!
//! Three tests are re-homed here (not duplicated) from
//! `generative/tests/convergence.rs`, per the issue's own example: "#168's
//! pin becomes one cell, and its two passing siblings become two more in
//! the same row." Two more are brand new, each closing a gap this exercise
//! actually found while filling in the table — one in
//! `trellis/tests/defs_aggregate_relationship.rs`, one in
//! `trellis/tests/apply_relationships.rs` — both test-only, no production
//! code changed.

use std::time::{Duration, Instant};

use generative::backend::{Backend, ManualBackend};
use generative::generate::{
    AggregateFn, DefShape, Mutate, RelAggregateFn, RelFieldKind, RelFieldSpec,
    RelInterleavingVariant, TableSpec, build_program_multi_with_relationships,
    build_relationship_interleaving_scenario,
};
use generative::model::{Op, OpOutcome, Program, group_key};
use generative::run::{check_program, run_convergence};
use testkit::TestCluster;
use trellis::{Config, Pool};

/// Applies one op and asserts its actual outcome matches what the scenario
/// builder recorded on it (design doc §4 "operation errors are checked, not
/// swallowed") — the same check `generative::run::run_convergence` makes
/// internally, inlined here since this file drives `ManualBackend` directly
/// rather than through that loop (it needs to control exactly when
/// `quiesce()`/`force_seal_active_segment()` are called relative to the two
/// critical ops, which `run_convergence`'s fixed apply-then-quiesce-every-op
/// loop can't express).
async fn apply_checked(backend: &mut ManualBackend, op: &Op) {
    let actual = match backend.apply(op).await {
        Err(_) => OpOutcome::Fails,
        Ok(0) => OpOutcome::AffectsNoRows,
        Ok(_) => OpOutcome::Succeeds,
    };
    assert!(
        op.expect().matches(&actual),
        "op {op:?} expected {:?} but produced {actual:?}",
        op.expect()
    );
}

/// Polls [`ManualBackend::has_pending`] until a just-committed change has
/// actually reached the ring, or panics after `timeout`. See
/// [`ManualBackend::force_seal_active_segment`]'s doc comment for why a
/// caller must do this before sealing.
async fn wait_until_pending(backend: &ManualBackend, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        if backend.has_pending().await.expect("check pending") {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for a committed change to reach the ring — intake may be stuck"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Quiesces, snapshots, and runs the three-way oracle check
/// ([`generative::run::check_program`]) — the shared tail of every scenario
/// below, regardless of which timing mode drove the two critical ops.
async fn assert_converges(pool: &Pool, program: &Program, backend: &mut ManualBackend) {
    backend.quiesce().await.expect("quiesce");
    let snapshot = backend.snapshot().await.expect("snapshot");
    if let Some((target, report)) = check_program(pool, program, &snapshot)
        .await
        .expect("oracle check")
    {
        panic!(
            "relationship interleaving scenario diverged on target {target:?}:\n{report}\n\
             program: {program:#?}"
        );
    }
}

/// #138 item 2 (the intake-lag window): the parent-side and from-side
/// critical ops commit back-to-back with no `quiesce()` in between, so
/// intake may not yet have staged the from-side change (or may not yet have
/// staged the parent's) when the other's processing begins. Single-worker,
/// no forced seal.
async fn run_in_the_same_intake_window(variant: RelInterleavingVariant) {
    let scenario = build_relationship_interleaving_scenario(variant);
    let program = &scenario.program;

    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut backend = ManualBackend::connect(db.dsn())
        .await
        .expect("connect manual backend");
    let pool = Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");

    backend.install(program).await.expect("install program");
    for op in &program.ops[..scenario.parent_op] {
        apply_checked(&mut backend, op).await;
    }
    backend.quiesce().await.expect("quiesce seeds");

    apply_checked(&mut backend, &program.ops[scenario.parent_op]).await;
    apply_checked(&mut backend, &program.ops[scenario.from_side_op]).await;

    assert_converges(&pool, program, &mut backend).await;
}

/// #138 items 1 and 3 (a seal boundary, then an out-of-order segment drain):
/// the parent-side change is sealed into its own segment before the
/// from-side change is even applied, so the two are guaranteed to land in
/// different, already-sealed segments — then a `workers`-worker backend
/// quiesces both, letting the engine's own claim/drain scheduling (not the
/// harness) pick which one actually drains first.
async fn run_across_a_seal_boundary(variant: RelInterleavingVariant, workers: usize) {
    let scenario = build_relationship_interleaving_scenario(variant);
    let program = &scenario.program;

    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    // Widened maintenance_interval (mirroring `concurrent_convergence.rs`'s
    // `a_batch_that_exceeds_the_split_threshold_converges_across_workers`):
    // the engine's own maintenance loop also seals the active segment on its
    // normal 300ms cadence, which would otherwise race
    // `force_seal_active_segment`'s manual calls below often enough to make
    // which two segments the two critical ops actually land in
    // nondeterministic. 3s comfortably exceeds how long this test's brief
    // critical section takes (a handful of round trips plus a couple of
    // 10ms polls), so in practice the harness's own manual seals are the
    // only ones that fire during it — but it still stays well inside
    // `quiesce()`'s 30s timeout, so the maintenance loop's *other* jobs
    // (recovery, retirement) still run enough to let convergence complete;
    // widening it further (as that pin does, to 10s) is safe there only
    // because it never seals anything by hand and just waits the one
    // natural tick out.
    let mut backend =
        ManualBackend::connect_with_options(db.dsn(), workers, Some(Duration::from_secs(3)))
            .await
            .expect("connect manual backend");
    let pool = Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");

    backend.install(program).await.expect("install program");
    for op in &program.ops[..scenario.parent_op] {
        apply_checked(&mut backend, op).await;
    }
    // Widening `maintenance_interval` above means nothing auto-seals the
    // seed batch either unless this does it by hand first — `quiesce()`
    // only waits for convergence, it never forces a seal on its own. Must
    // wait for intake to actually stage the seeds first (not just check
    // once): checking `has_pending` a single time right after the apply
    // loop can race intake's own WAL consumption and see nothing yet,
    // silently skipping the seal and leaving `quiesce()` to hang until the
    // next (3s-away) automatic tick — or, worse, race that automatic tick
    // mid-manual-seal below.
    wait_until_pending(&backend, Duration::from_secs(5)).await;
    backend
        .force_seal_active_segment()
        .await
        .expect("seal the seed batch");
    backend.quiesce().await.expect("quiesce seeds");
    assert!(
        !backend.has_pending().await.expect("check pending"),
        "seeds must be fully drained before the critical section starts, or the forced seal \
         below could seal leftover seed rows instead of the parent's own change"
    );

    apply_checked(&mut backend, &program.ops[scenario.parent_op]).await;
    wait_until_pending(&backend, Duration::from_secs(5)).await;
    let parent_seg = backend
        .force_seal_active_segment()
        .await
        .expect("seal the parent's own segment");

    apply_checked(&mut backend, &program.ops[scenario.from_side_op]).await;
    wait_until_pending(&backend, Duration::from_secs(5)).await;
    let from_side_seg = backend
        .force_seal_active_segment()
        .await
        .expect("seal the from-side segment");
    assert!(
        from_side_seg > parent_seg,
        "the from-side change (segment {from_side_seg}) must land in a segment strictly after \
         the parent's own (segment {parent_seg}) for this to actually be a seal-boundary crossing"
    );

    assert_converges(&pool, program, &mut backend).await;
}

/// How many application-worker tasks [`run_across_a_seal_boundary`] runs
/// with — `> 1` is the whole point (#138 item 3: more than one segment
/// simultaneously claimable), and small/fixed for the same reason
/// `tests/concurrent_convergence.rs`'s `PROPERTY_WORKERS` is: this property
/// isn't sweeping worker counts, just proving the race is survivable with
/// real concurrency in the picture.
const SEAL_BOUNDARY_WORKERS: usize = 2;

#[tokio::test(flavor = "multi_thread")]
async fn parent_field_update_converges_in_the_same_intake_window() {
    run_in_the_same_intake_window(RelInterleavingVariant::ParentFieldUpdate).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn parent_insert_converges_in_the_same_intake_window() {
    run_in_the_same_intake_window(RelInterleavingVariant::ParentInsert).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn parent_delete_converges_in_the_same_intake_window() {
    run_in_the_same_intake_window(RelInterleavingVariant::ParentDelete).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn repoint_to_nonexistent_parent_converges_in_the_same_intake_window() {
    run_in_the_same_intake_window(RelInterleavingVariant::RepointToNonexistentParent).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn repoint_to_null_parent_converges_in_the_same_intake_window() {
    run_in_the_same_intake_window(RelInterleavingVariant::RepointToNullParent).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn parent_field_update_converges_across_a_seal_boundary() {
    run_across_a_seal_boundary(
        RelInterleavingVariant::ParentFieldUpdate,
        SEAL_BOUNDARY_WORKERS,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn parent_insert_converges_across_a_seal_boundary() {
    run_across_a_seal_boundary(RelInterleavingVariant::ParentInsert, SEAL_BOUNDARY_WORKERS).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn parent_delete_converges_across_a_seal_boundary() {
    run_across_a_seal_boundary(RelInterleavingVariant::ParentDelete, SEAL_BOUNDARY_WORKERS).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn repoint_to_nonexistent_parent_converges_across_a_seal_boundary() {
    run_across_a_seal_boundary(
        RelInterleavingVariant::RepointToNonexistentParent,
        SEAL_BOUNDARY_WORKERS,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn repoint_to_null_parent_converges_across_a_seal_boundary() {
    run_across_a_seal_boundary(
        RelInterleavingVariant::RepointToNullParent,
        SEAL_BOUNDARY_WORKERS,
    )
    .await;
}

// =========================================================================
// Part 2 — the obligation-table test matrix (issue #173 phase 4). See the
// module doc comment at the top of this file for the design rationale.
// =========================================================================

mod obligation_matrix {
    /// The six propagation paths `docs/relationship-propagation.md` names as
    /// columns. Kept in the doc's own left-to-right order so a reader
    /// flipping between the doc and this file doesn't have to re-map
    /// anything.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum Path {
        ForwardRead,
        ReverseDelta,
        ReverseFallback,
        TruncateClear,
        AggregateIncremental,
        Backfill,
    }

    /// The six recurring edge inputs the doc names as rows — issue #173's
    /// own "the recurring inputs are" list, verbatim.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum EdgeInput {
        /// TRUNCATE's key-less, whole-keyspace sentinel.
        TruncateWholeKeyspace,
        /// A literally-NULL join key.
        NullJoinKey,
        /// A composite source primary key.
        CompositePrimaryKey,
        /// A missing/nonexistent parent row, or an FK re-point to one.
        MissingOrRepointedParent,
        /// A source lacking the REPLICA IDENTITY the path needs.
        MissingReplicaIdentity,
        /// A shared from-table reachable via two relationships.
        SharedFromTableTwoRelationships,
    }

    pub(super) const ALL_PATHS: [Path; 6] = [
        Path::ForwardRead,
        Path::ReverseDelta,
        Path::ReverseFallback,
        Path::TruncateClear,
        Path::AggregateIncremental,
        Path::Backfill,
    ];

    pub(super) const ALL_EDGE_INPUTS: [EdgeInput; 6] = [
        EdgeInput::TruncateWholeKeyspace,
        EdgeInput::NullJoinKey,
        EdgeInput::CompositePrimaryKey,
        EdgeInput::MissingOrRepointedParent,
        EdgeInput::MissingReplicaIdentity,
        EdgeInput::SharedFromTableTwoRelationships,
    ];

    /// One obligation-table cell's classification. `Handled`'s payload is
    /// one or more pointers — either a bare test name (a `#[test]` in *this*
    /// file) or a `"path/to/file.rs::test_name"` reference into another
    /// file/crate, for coverage this phase re-homed only by citation, not by
    /// moving code across a crate boundary. `NotApplicable`'s payload is the
    /// structural reason (a declare-time gate, an input shape that
    /// provably can't reach that path, ...). See the module doc comment for
    /// why `Gap` is never constructed today.
    #[derive(Debug)]
    pub(super) enum Cell {
        Handled(&'static [&'static str]),
        NotApplicable(&'static str),
        #[allow(dead_code)]
        Gap(&'static str),
    }

    /// The matrix itself: an exhaustive match with no `_` arm. Adding a
    /// variant to `Path` or `EdgeInput` breaks this function's compile until
    /// every new cell it creates is classified — that's the entire point.
    pub(super) fn cell(path: Path, input: EdgeInput) -> Cell {
        use Cell::{Handled, NotApplicable};
        use EdgeInput::{
            CompositePrimaryKey, MissingOrRepointedParent, MissingReplicaIdentity, NullJoinKey,
            SharedFromTableTwoRelationships, TruncateWholeKeyspace,
        };
        use Path::{
            AggregateIncremental, Backfill, ForwardRead, ReverseDelta, ReverseFallback,
            TruncateClear,
        };

        match (path, input) {
            // --- TRUNCATE's key-less sentinel (#98, #165, #168) ---
            (ForwardRead, TruncateWholeKeyspace) => NotApplicable(
                "Forward read never sees a TRUNCATE directly; it only ever reads whatever \
                 TruncateClear already left behind",
            ),
            (ReverseDelta, TruncateWholeKeyspace) => NotApplicable(
                "a TRUNCATE carries no image, so no RelationshipReverseRecord can ever be \
                 built — from_side_rows_for_trigger_txn's WholeKeyspace arm returns a typed \
                 ApplyError::ReverseTriggerNotResolvable for exactly this reason, pinned by \
                 trellis/src/staging/apply.rs::reverse_trigger_whole_keyspace_is_a_typed_error_not_a_panic_in_the_txn_lookup",
            ),
            (ReverseFallback, TruncateWholeKeyspace) => Handled(&[
                "reused: TruncateClear's own reverse_recomputes accumulator feeds this path \
                 directly — see TruncateClear x TruncateWholeKeyspace, no test distinct from \
                 its pins",
            ]),
            (TruncateClear, TruncateWholeKeyspace) => Handled(&[
                "truncating_a_relationship_to_side_table_leaves_a_stale_enrichment (re-homed \
                 from generative/tests/convergence.rs — issue #98/#168)",
                "deleting_a_relationship_to_side_row_does_clear_the_enrichment (re-homed \
                 control beside it)",
            ]),
            (AggregateIncremental, TruncateWholeKeyspace) => Handled(&[
                "truncating_a_relationship_to_side_table_leaves_a_stale_aggregate_enrichment \
                 (re-homed from generative/tests/convergence.rs — issue #98 aggregate variant)",
            ]),
            (Backfill, TruncateWholeKeyspace) => NotApplicable(
                "backfill runs once against a live snapshot; TRUNCATE is a live CDC event \
                 only the ring ever sees",
            ),

            // --- A NULL join key (#128) ---
            (ForwardRead, NullJoinKey) => Handled(&[
                "generative/tests/convergence.rs::a_to_one_relationship_aggregated_inside_a_group_by_converges_end_to_end",
            ]),
            (ReverseDelta, NullJoinKey) => Handled(&[
                "repoint_to_null_parent_converges_in_the_same_intake_window (this file, Part 1)",
                "repoint_to_null_parent_converges_across_a_seal_boundary (this file, Part 1)",
            ]),
            (ReverseFallback, NullJoinKey) => Handled(&[
                "incidental: WHERE col = ANY($1) can never match NULL by SQL three-valued \
                 logic, and every generative property-test case that draws a NULL FK exercises \
                 it; no dedicated pin",
            ]),
            (TruncateClear, NullJoinKey) => Handled(&[
                "trellis/src/staging/apply.rs::reverse_trigger_whole_keyspace_matches_every_non_null_row_via_from_side_keys",
            ]),
            (AggregateIncremental, NullJoinKey) => Handled(&[
                "generative/tests/convergence.rs::a_to_one_relationship_aggregated_inside_a_group_by_converges_end_to_end \
                 (same test as ForwardRead x NullJoinKey)",
            ]),
            (Backfill, NullJoinKey) => Handled(&[
                "trellis/tests/defs_backfill_relationship.rs::relationship_build_matches_oracle_including_no_match_and_multi_child",
            ]),

            // --- A composite source primary key (#121, #126, #163, #177) ---
            (ForwardRead, CompositePrimaryKey) => Handled(&[
                "trellis/tests/one_to_one_composite_primary_key.rs::a_composite_primary_key_transform_converges_inserts_updates_and_deletes \
                 (issue #121: a 1-1 transform's own source used to be rejected outright at \
                 declare time when composite-keyed — ddl::require_single_column_pk gated \
                 install_definition's DDL step and create_definition_inner itself; #121 removed \
                 that narrowing, so the target's own primary key now mirrors the source's in \
                 full and this scenario is live)",
                "trellis/tests/defs_catalog.rs::a_one_to_one_transform_against_a_composite_primary_key_source_is_accepted",
                "trellis/tests/quarantine.rs::a_composite_primary_key_source_drains_cleanly_with_no_quarantine_or_halt",
            ]),
            (ReverseDelta, CompositePrimaryKey) => Handled(&[
                "trellis/tests/defs_relationship_composite_pk.rs::reverse_update_of_the_to_side_row_updates_every_dependent_group",
            ]),
            (ReverseFallback, CompositePrimaryKey) => {
                Handled(&["same code path as ReverseDelta x CompositePrimaryKey — no distinct pin"])
            }
            (TruncateClear, CompositePrimaryKey) => NotApplicable(
                "same declare-time rejection as ForwardRead x CompositePrimaryKey for a \
                 OneToOne source's own PK; an Aggregate target needs no PK narrowing and \
                 truncate-clears correctly regardless of PK shape",
            ),
            (AggregateIncremental, CompositePrimaryKey) => Handled(&[
                "trellis/tests/defs_relationship_composite_pk.rs::plain_aggregate_over_a_composite_pk_source_drains_with_no_relationship_at_all",
                "trellis/tests/defs_relationship_composite_pk.rs::forward_insert_of_a_composite_pk_from_side_row_updates_its_group_total",
            ]),
            (Backfill, CompositePrimaryKey) => Handled(&[
                "trellis/tests/defs_relationship_composite_pk.rs::aggregate_over_a_to_one_relationship_with_composite_pk_backfills_to_the_oracle",
            ]),

            // --- A missing/nonexistent parent, and an FK re-point to one (#138, #169) ---
            (ForwardRead, MissingOrRepointedParent) => Handled(&[
                "trellis/tests/defs_relationship_nullability.rs::to_one_enrichment_nulls_out_when_the_related_row_appears_then_disappears",
                "trellis/tests/defs_relationship_frontdoor.rs::frontdoor_to_one_enrichment_converges_to_oracle",
                "generative/tests/convergence.rs::a_to_one_relationship_aggregated_inside_a_group_by_converges_end_to_end \
                 (unmatched k9 row)",
            ]),
            (ReverseDelta, MissingOrRepointedParent) => Handled(&[
                "trellis/tests/apply_relationship_reverse.rs::parent_insert_is_picked_up_by_the_reverse_path",
                "trellis/tests/apply_relationship_reverse.rs::parent_delete_is_picked_up_by_the_reverse_path",
                "repoint_to_nonexistent_parent_converges_in_the_same_intake_window (this file, Part 1)",
                "repoint_to_nonexistent_parent_converges_across_a_seal_boundary (this file, Part 1)",
            ]),
            (ReverseFallback, MissingOrRepointedParent) => Handled(&[
                "shares stage_reverse_recompute_fallback's live from-side re-read with \
                 ReverseDelta x MissingOrRepointedParent — no pin distinct from those",
            ]),
            (TruncateClear, MissingOrRepointedParent) => {
                NotApplicable("a TRUNCATE has no per-row key to re-point")
            }
            (AggregateIncremental, MissingOrRepointedParent) => Handled(&[
                "trellis/tests/defs_aggregate_relationship.rs::a_from_side_re_point_within_one_update_diffs_old_and_new_parent_contributions",
                "trellis/tests/defs_aggregate_relationship.rs::updating_a_to_side_row_updates_every_dependent_group",
                "trellis/tests/defs_aggregate_relationship.rs::inserting_a_from_side_row_resolves_from_the_projection_not_live_parent_state",
                "trellis/tests/defs_aggregate_relationship.rs::a_from_side_re_point_to_a_nonexistent_parent_subtracts_the_old_contribution \
                 (closes a gap issue #173 phase 4 found — docs/relationship-propagation.md's \
                 Known Gap 1: no test drove the delta path's own old/new resolution across the \
                 nonexistent-parent boundary, only the reverse path's equivalent)",
            ]),
            (Backfill, MissingOrRepointedParent) => Handled(&[
                "N/A for a bare to-one passthrough (ring only, no direct backfill fast path); \
                 handled for a to-many aggregate's missing-children case: \
                 trellis/tests/defs_backfill_relationship.rs::relationship_build_matches_oracle_including_no_match_and_multi_child",
            ]),

            // --- A source lacking the needed REPLICA IDENTITY (#41, #47, #158) ---
            (ForwardRead, MissingReplicaIdentity) => NotApplicable(
                "rejected earlier, at create_relationship/create_definition time, before apply \
                 ever runs",
            ),
            (ReverseDelta, MissingReplicaIdentity) => {
                NotApplicable("same declare-time rejection as ForwardRead x MissingReplicaIdentity")
            }
            (ReverseFallback, MissingReplicaIdentity) => {
                NotApplicable("same declare-time rejection as ForwardRead x MissingReplicaIdentity")
            }
            (TruncateClear, MissingReplicaIdentity) => NotApplicable(
                "a TRUNCATE carries no image, so replica identity is irrelevant to it",
            ),
            (AggregateIncremental, MissingReplicaIdentity) => NotApplicable(
                "rejected by assert_replica_identity_supports_aggregate -> \
                 intake::require_replica_identity_full, inside create_definition_inner, \
                 pinned by trellis/tests/defs_catalog.rs::an_aggregate_transform_against_default_replica_identity_is_rejected",
            ),
            (Backfill, MissingReplicaIdentity) => NotApplicable(
                "backfill reads a live full-table snapshot, not a CDC image; the later \
                 incremental drain is what the declare-time gate protects",
            ),

            // --- A shared from-table reachable via two relationships (#79) ---
            (ForwardRead, SharedFromTableTwoRelationships) => Handled(&[
                "trellis/tests/defs_aggregate_relationship.rs::two_relationships_sharing_a_to_side_column_name_resolve_independently",
            ]),
            (ReverseDelta, SharedFromTableTwoRelationships) => NotApplicable(
                "a definition referencing more than one distinct relationship always sets \
                 needs_recompute_fallback and never reaches the delta path",
            ),
            (ReverseFallback, SharedFromTableTwoRelationships) => Handled(&[
                "trellis/tests/apply_relationships.rs::reverse_recompute_dedupes_across_relationships_sharing_from_table",
                "trellis/tests/apply_relationships.rs::reverse_recompute_fan_in_keeps_the_earliest_src_changed",
            ]),
            (TruncateClear, SharedFromTableTwoRelationships) => Handled(&[
                "trellis/tests/apply_relationships.rs::reverse_recompute_dedupes_a_truncate_against_a_relationship_sharing_the_same_from_table \
                 (closes a gap issue #173 phase 4 found — docs/relationship-propagation.md said \
                 this combination was 'handled by reuse' on code-reading alone: 'No test \
                 combines a TRUNCATE with two shared-from-table relationships')",
            ]),
            (AggregateIncremental, SharedFromTableTwoRelationships) => Handled(&[
                "trellis/tests/defs_aggregate_relationship.rs::two_relationships_sharing_a_to_side_column_name_resolve_independently \
                 (same test as ForwardRead x SharedFromTableTwoRelationships — it is an \
                 aggregate definition)",
            ]),
            (Backfill, SharedFromTableTwoRelationships) => Handled(&[
                "covered by the backfill half of the same two_relationships_sharing_a_to_side_column_name_resolve_independently \
                 fixture — issue #79's original repro",
            ]),
        }
    }

    /// The structural check itself: walk every cell of the 6x6 matrix and
    /// assert it was actually classified with non-empty content. This can
    /// never catch a *wrong* classification (that's what the tests each
    /// cell names are for) — it only catches a cell nobody has looked at,
    /// which is the failure mode issue #173 is about: "an empty cell is a
    /// visible hole rather than an absent test."
    #[test]
    fn every_path_and_edge_input_combination_is_classified() {
        for &path in &ALL_PATHS {
            for &input in &ALL_EDGE_INPUTS {
                match cell(path, input) {
                    Cell::Handled(refs) => assert!(
                        !refs.is_empty(),
                        "{path:?} x {input:?} is Handled but names no test"
                    ),
                    Cell::NotApplicable(reason) => assert!(
                        !reason.is_empty(),
                        "{path:?} x {input:?} is NotApplicable but gives no reason"
                    ),
                    Cell::Gap(note) => assert!(
                        !note.is_empty(),
                        "{path:?} x {input:?} is a Gap but gives no tracking note"
                    ),
                }
            }
        }
    }
}

// -------------------------------------------------------------------------
// Re-homed from generative/tests/convergence.rs (issue #173 phase 4): the
// TruncateClear x TruncateWholeKeyspace row's three cells — issue #168's own
// pin, plus the two passing siblings its scoping note used to narrow the
// fault to "the bare to-one enrichment truncate path" specifically. Moved
// verbatim (not duplicated); `convergence.rs` keeps a pointer comment where
// these used to live. `rel_spec` below is convergence.rs's own helper of the
// same name, duplicated locally — this file and convergence.rs already keep
// independent copies of their own small test-only helpers (`apply_checked`
// vs. convergence.rs's own harness code) rather than sharing across files.
// -------------------------------------------------------------------------

/// A [`TableSpec`] with `rel_fk_values` and `grain_values` spelled out and
/// every other non-numeric column `NULL` — mirrors
/// `generative/tests/convergence.rs`'s own `rel_spec` (duplicated here, see
/// the section comment above).
fn rel_spec(
    seed_values: Vec<(Option<i64>, Option<i64>)>,
    grain_values: Vec<Option<String>>,
    rel_fk_values: Vec<Option<String>>,
    mutates: Vec<Mutate>,
) -> TableSpec {
    let n = seed_values.len();
    TableSpec {
        seed_values,
        text_values: vec![None; n],
        bool_values: vec![None; n],
        uuid_values: vec![None; n],
        grain_values,
        rel_fk_values,
        mutates,
    }
}

/// **Issue #98 regression pin, issue #168 re-fix.** `TRUNCATE` on a table
/// some definition reads *through a relationship* used to leave that
/// definition's enrichment permanently stale.
///
/// Mechanism (`staging::apply`): the truncate-clear path resolves affected
/// targets with `catalog::transforms_for_source` — definitions whose
/// **source** is the truncated table — while reverse propagation into
/// definitions that merely *read* the table lived in the separate keyed
/// by-source loop, driven by per-row change images and
/// `catalog::relationships_to_table`. A `TRUNCATE` stages one key-less
/// sentinel row (`append::TRUNCATE_SENTINEL_KEY`), not per-row images, so it
/// never reached that loop at all — the "its own logical-decoding message,
/// not a bulk delete" hazard the design doc §7 flags.
///
/// The program: `t0`'s single row has a foreign key resolving to `t1`'s
/// single row, so `rel_enrich` is `22`. Then `t1` is truncated. The oracle's
/// `LEFT JOIN` finds nothing and says `NULL`; the maintained target used to
/// still say `22`.
///
/// Fixed the first time by having the truncate-clear path
/// (`trellis/src/staging/apply.rs`) also resolve
/// `catalog::relationships_to_table` for the truncated table and stage
/// every from-side row with a non-`NULL` join column as a reverse recompute,
/// through the same `reverse_recomputes` accumulator the row-driven path
/// (issue #30) already feeds.
///
/// **Broke again** (issue #168): epic #127's settled-parent-projection
/// reverse mechanism for to-one relationships did not handle TRUNCATE's
/// key-less sentinel — the projection was never cleared on a to-side
/// truncate, so the from-side recompute this path stages kept re-deriving
/// against the stale pre-truncate projection row. Fixed by also clearing the
/// relationship's settled parent projection in full
/// (`ApplyPlan::relationship_projection_clears`) whenever a `TRUNCATE`
/// empties a to-one relationship's to-side table. Kept as a permanent
/// regression pin (design doc §6) — this is the `TruncateClear` column's
/// entry in `obligation_matrix`'s `TruncateWholeKeyspace` row.
#[tokio::test(flavor = "multi_thread")]
async fn truncating_a_relationship_to_side_table_leaves_a_stale_enrichment() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let program = build_program_multi_with_relationships(
        &[
            rel_spec(
                vec![(Some(1), Some(2))],
                vec![None],
                vec![Some("k1".to_string())],
                Vec::new(),
            ),
            rel_spec(
                vec![(Some(22), Some(0))],
                vec![None],
                vec![None],
                vec![Mutate::Truncate],
            ),
        ],
        &[(0, DefShape::OneToOne)],
        &[None],
        &[Some(RelFieldSpec {
            to_table: 1,
            kind: RelFieldKind::ToOneBare,
        })],
    );
    // `t1`'s `mutates: vec![Mutate::Truncate]` above is enough to generate
    // the triggering truncate directly now — the generator no longer steers
    // around this shape (issue #98 removed
    // `without_truncates_on_relationship_to_sides`, the workaround that used
    // to require manually re-chaining a `Truncate` op here to reproduce the
    // finding).

    let mut backend = ManualBackend::connect(db.dsn())
        .await
        .expect("connect manual backend");
    let pool = Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");

    run_convergence(&mut backend, &pool, &program)
        .await
        .expect("truncating a relationship's to-side table must clear the enrichment it fed");
}

/// The control beside the finding above (design doc §6: "a control test
/// beside each finding — a case that must *converge* — proving the harness
/// can tell green from red").
///
/// Byte-for-byte the same program as
/// [`truncating_a_relationship_to_side_table_leaves_a_stale_enrichment`]
/// except the last op `DELETE`s `t1`'s only row instead of `TRUNCATE`ing the
/// table. The end state of the source is identical — `t1` is empty either
/// way — so any oracle that judged the two differently would be wrong. This
/// one converges, which localizes the defect precisely: not reverse
/// propagation in general, but the `TRUNCATE`-shaped change specifically.
#[tokio::test(flavor = "multi_thread")]
async fn deleting_a_relationship_to_side_row_does_clear_the_enrichment() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let program = build_program_multi_with_relationships(
        &[
            rel_spec(
                vec![(Some(1), Some(2))],
                vec![None],
                vec![Some("k1".to_string())],
                Vec::new(),
            ),
            rel_spec(
                vec![(Some(22), Some(0))],
                vec![None],
                vec![None],
                vec![Mutate::Delete { pk: 1 }],
            ),
        ],
        &[(0, DefShape::OneToOne)],
        &[None],
        &[Some(RelFieldSpec {
            to_table: 1,
            kind: RelFieldKind::ToOneBare,
        })],
    );

    let mut backend = ManualBackend::connect(db.dsn())
        .await
        .expect("connect manual backend");
    let pool = Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");

    let outcome = run_convergence(&mut backend, &pool, &program)
        .await
        .expect("deleting the related row must clear the enrichment it fed");
    assert!(outcome.as_pass(), "run did not pass: {outcome}");

    let snapshot = backend.snapshot().await.expect("snapshot after the run");
    assert_eq!(
        snapshot[&program.defs[0].target]["1"]["rel_enrich"], None,
        "the related row is gone, so the enrichment must be NULL: {snapshot:#?}"
    );
}

/// **Issue #98 coverage: the aggregate case.** The pinned finding in
/// [`truncating_a_relationship_to_side_table_leaves_a_stale_enrichment`]
/// above is a `OneToOne` enrichment reading a to-one relationship; this is
/// the same root cause on a `GROUP BY` definition whose aggregated field
/// reads a to-one relationship path (issue #94's `SUM(post.word_count)`
/// shape, mirrored here by `convergence.rs`'s own
/// `a_to_one_relationship_aggregated_inside_a_group_by_converges_end_to_end`'s
/// `rel_agg = SUM(<rel>.c1)`).
///
/// Byte-for-byte that test's two-row-per-group shape, restricted to one
/// grain group to keep the finding minimal, with `t1` (the to-side)
/// `TRUNCATE`d instead of left alone. `t0`'s two rows stay in their group
/// either way (`COUNT(*)` must hold at `2`), but the `SUM` must fall back to
/// `NULL` once the to-side has no rows left for it to match.
///
/// Whether this passes tells us whether the fix above already generalizes
/// to the aggregate path: the reverse-recompute it stages is an ordinary
/// image-less `Recompute` on the from-side (`t0`) row, and
/// `apply_aggregate::accumulate_changes` always forces *any* image-less
/// change's group onto the full-recompute path regardless of whether the
/// touching change carries a real image — so no aggregate-specific code
/// needed to change for this to converge. This is the `AggregateIncremental`
/// column's entry in `obligation_matrix`'s `TruncateWholeKeyspace` row.
#[tokio::test(flavor = "multi_thread")]
async fn truncating_a_relationship_to_side_table_leaves_a_stale_aggregate_enrichment() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let program = build_program_multi_with_relationships(
        &[
            rel_spec(
                vec![(Some(1), Some(0)), (Some(2), Some(0))],
                vec![Some("0".to_string()), Some("0".to_string())],
                vec![Some("k1".to_string()), Some("k9".to_string())],
                Vec::new(),
            ),
            rel_spec(
                vec![(Some(10), Some(0))],
                vec![None],
                vec![None],
                vec![Mutate::Truncate],
            ),
        ],
        &[(
            0,
            DefShape::Aggregate {
                functions: vec![AggregateFn::Count],
            },
        )],
        &[None],
        &[Some(RelFieldSpec {
            to_table: 1,
            kind: RelFieldKind::ToOneAggregate(RelAggregateFn::Sum),
        })],
    );

    let mut backend = ManualBackend::connect(db.dsn())
        .await
        .expect("connect manual backend");
    let pool = Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");

    let outcome = run_convergence(&mut backend, &pool, &program)
        .await
        .expect("truncating a relationship's to-side table must clear the aggregate it fed");
    assert!(outcome.as_pass(), "run did not pass: {outcome}");

    let snapshot = backend.snapshot().await.expect("snapshot after the run");
    let target = &snapshot[&program.defs[0].target];
    let row = &target[&group_key(&[Some("0".to_string())])];
    assert_eq!(
        row["cnt"],
        Some("2".to_string()),
        "both source rows must still count toward COUNT(*) after the truncate: {target:?}"
    );
    assert_eq!(
        row["rel_agg"], None,
        "the to-side table is empty, so the SUM must fall back to NULL: {target:?}"
    );
}
