//! Planted ordering bugs: known-wrong engine behavior the
//! generative concurrent tier must catch, switched on one at a time by an
//! environment variable. **Test-only.** This module, and every hook that
//! consults it, compiles only under `cfg(any(test, feature = "test-util"))`,
//! the same gate as `dev` and `staging::apply`'s pre-commit pause hook, so no
//! production build carries any of it (see `Cargo.toml`'s `test-util`
//! comment for the feature-unification rule that keeps it that way).
//!
//! # Why an environment variable
//!
//! A plant is chosen per *process*: [`PLANT_ENV`] is read once, the first
//! time a hook asks, and never changes after. That is deliberate:
//!
//! - `cargo test --workspace` never sets it, so the default suite runs with
//!   no plant armed. A build flag couldn't promise that: `test-util` is on in
//!   every workspace test build (the `generative` crate's self
//!   dev-dependency turns it on), so a hook that only needed the feature
//!   would be live in the default suite.
//! - A programmatic "arm this plant" call would leak into every other test
//!   running on another thread of the same test binary, because the engine
//!   runs in-process and a plant is process-wide. An environment variable
//!   can only be set for a whole process, so a planted run is always a
//!   process of its own (the concurrent tier's sweep spawns one per plant).
//! - The subprocess backend's engine inherits it, with no extra plumbing.
//!
//! An unknown name panics on first use rather than running unplanted, so a
//! typo can't pass itself off as "the tier caught nothing".
//!
//! # Adding a plant
//!
//! 1. Add a variant to [`Plant`], with a doc comment naming the invariant it
//!    breaks and the issue that fixed (or will fix) the real bug, and add it
//!    to [`Plant::ALL`] and [`Plant::name`].
//! 2. At the engine site, add one clearly marked hook, gated like this:
//!
//!    ```ignore
//!    // Planted bug: <what it breaks>. See `crate::plant`.
//!    #[cfg(any(test, feature = "test-util"))]
//!    if crate::plant::fires(crate::plant::Plant::MyPlant, <would it change anything>) {
//!        <the wrong behavior>;
//!    }
//!    ```
//!
//!    Pass [`fires`] a condition that is true only when the plant actually
//!    changes what the engine does here, so [`fired`] counts real
//!    misbehavior, not visits. Keep the hook to a few lines: the correct
//!    path must read exactly as it did before.
//! 3. Pin it: name the deterministic interleaving test that fails with it
//!    armed (`TRELLIS_TEST_PLANT=<name> cargo test -p trellis --test <file>
//!    <test>`) in its doc comment.
//! 4. Run the sweep of a tier that reaches it (`generative::plant_sweep`:
//!    the concurrent tier's `planted_bugs_are_caught`, the `JOIN` tier's
//!    `join_planted_bugs_are_caught`), list it in the `not_gated` of the
//!    tiers that don't, and record its catch rate.
//!
//! A plant is never "fixed": it is the regression the tier exists to catch.
//! If the engine code it hooks is rewritten,
//! move the hook to the site that now carries the same invariant, or delete
//! the plant if the invariant is gone.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

/// The environment variable that arms a plant, by its [`Plant::name`].
/// Unset or empty arms nothing.
pub const PLANT_ENV: &str = "TRELLIS_TEST_PLANT";

/// One planted ordering bug. Each breaks one invariant, at one engine site.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Plant {
    /// A 1-1 target applies every change without
    /// ADR-0002's I2 (`staging::one_to_one_ledger::update_entries`): not
    /// skipped when a Re-derive already read it, nor when a newer change
    /// already applied. A page that drains after a newer one for the same key
    /// then leaves the older value behind.
    StaleOneToOneWrite,
    /// ADR-0002 I4: tombstone GC (`staging::retire::collect_tombstones`)
    /// collects through the highest drained segment instead of the
    /// contiguous drained prefix, so a tombstone goes while an older change
    /// to its key is still in an undrained segment, which then applies to
    /// the fresh entry and brings the deleted row back.
    EarlyTombstoneGc,
    /// ADR-0002 I1 for the build: a 1-1 Re-derive build's chunk,
    /// or its sweep, reads and rewrites its keys' entries without first
    /// taking their entry locks (`staging::build::one_to_one`), so it reads
    /// an entry a page is between reading and writing.
    ChunkWithoutEntryLock,
    /// ADR-0002 R1 on the group basis: a page on a group-basis
    /// aggregate takes no lock on the groups it already has hidden rows for
    /// (`staging::group_basis::lock_groups` only inserts the missing ones),
    /// so its apply statement can read a group's row while another page is
    /// between its own apply and its commit. Its update by `ctid` then finds
    /// the row moved and adds nothing: the page's change is lost.
    ///
    /// Pinned by `group_basis_interleavings`'s
    /// `r1_a_page_waits_for_a_page_that_applied_to_its_group`.
    GbSkipGroupLock,
    /// ADR-0002 R2 on the group basis: a page refuses a change by
    /// its position against the group's basis (its transaction id below the
    /// basis snapshot's `xmax`), not by the basis's visibility test
    /// (`staging::group_basis`'s apply statement). A change whose
    /// transaction was in flight when the basis was taken, which the basis
    /// didn't see, is refused, and its group loses it.
    ///
    /// Pinned by `group_basis_interleavings`'s
    /// `r2_a_change_in_flight_at_the_basis_applies`.
    GbLsnNotVisibility,
    /// A page folds a key's ring rows into one side (the first
    /// old image and the last new image, under the last transaction) before
    /// staging them (`staging::group_basis::GroupTargetPlan::push`). A group
    /// whose basis falls between two of the key's changes then keeps a row
    /// the basis saw arrive and no change takes away.
    ///
    /// Pinned by `group_basis_interleavings`'s
    /// `f2_a_basis_between_two_changes_of_a_key`.
    GbFoldSidesPerKey,
    /// ADR-0002 R4: the zero-group GC (`staging::retire`'s
    /// `collect_zero_groups`) collects every zero group, ignoring its basis
    /// and its stamp against the contiguous drained prefix. A change the
    /// basis saw but no page has drained yet then lands on a fresh row with
    /// no basis and applies a second time. It covers a `JOIN`'s `P` and `M`
    /// cells too, which the same statement collects.
    ///
    /// Pinned by `group_basis_interleavings`'s
    /// `r4_a_zero_group_with_a_basis_stays_until_the_prefix_passes_its_stamp`
    /// and `join_interleavings`'s `family13_…`.
    GbEarlyZeroGc,
    /// ADR-0002, "A build or rebuild": a group-basis
    /// rebuild reads the source's `GROUP BY` and takes its basis from a
    /// snapshot taken before it locks its groups
    /// (`staging::group_basis::rebuild`). A page that applies a change the
    /// snapshot didn't see and commits before the locks has its change
    /// overwritten by the absolute write, which never had it.
    ///
    /// Pinned by `group_basis_rebuild`'s
    /// `a_page_that_commits_between_the_enumeration_and_the_locks`.
    GbBuildSnapshotBeforeLocks,
    /// ADR-0019 JR1: a `JOIN` page takes no `J` lock on its keys
    /// (`staging::join`'s `write_locked` inserts only the missing `J` rows).
    /// Two pages that change one key from opposite sides without sharing a
    /// `P` or `M` cell then each compute their product against the other
    /// side's partials without the other's change, and the cross term
    /// `ΔP ⊗ ΔM` is lost.
    ///
    /// Pinned by `join_interleavings`'s
    /// `jr1_the_j_lock_orders_two_sides_with_no_shared_cell`.
    JoinNoJLock,
    /// ADR-0019 JR2: a `JOIN` page computes its product from each
    /// side's partials as they were before it took its `J` locks, plus its
    /// own changes (`staging::join`'s `write_locked`), not as they are under
    /// the locks. A page of the other side that commits in between is
    /// missing from the product, and the cross term `ΔP ⊗ ΔM` is lost.
    ///
    /// Pinned by `join_interleavings`'s `family1_…` (a page frozen before
    /// its `J` lock).
    JoinOtherSideBeforeJ,
    /// ADR-0019 JR3: a `LEFT JOIN` page computes each key's
    /// unmatched term as if its `J.matches` never crossed zero
    /// (`staging::join`'s product), so a key's first matching row, or its
    /// last one leaving, doesn't move its `P` cells out of or into the
    /// `NULL` joined group.
    ///
    /// Pinned by `join_interleavings`'s `family2_flips_left`.
    JoinDropFlip,
    /// `staging::group_basis`'s "Zero groups": a `JOIN` page takes
    /// no lock on the `T` groups it already has hidden rows for, only
    /// inserting the missing ones. The zero-group GC then collects a zero
    /// `T` group between the page's lock statement and its write, which
    /// finds no row and drops the delta; and two pages adding to one `T`
    /// group lose one's delta as `gb_skip_group_lock`'s pages do.
    ///
    /// Pinned by `join_interleavings`'s
    /// `a_collection_never_takes_a_t_group_from_under_its_page`.
    JoinTSkipLock,
}

impl Plant {
    /// Every plant, in the order the concurrent tier's sweep runs them.
    pub const ALL: &'static [Plant] = &[
        Plant::StaleOneToOneWrite,
        Plant::EarlyTombstoneGc,
        Plant::ChunkWithoutEntryLock,
        Plant::GbSkipGroupLock,
        Plant::GbLsnNotVisibility,
        Plant::GbFoldSidesPerKey,
        Plant::GbEarlyZeroGc,
        Plant::GbBuildSnapshotBeforeLocks,
        Plant::JoinNoJLock,
        Plant::JoinOtherSideBeforeJ,
        Plant::JoinDropFlip,
        Plant::JoinTSkipLock,
    ];

    /// The name [`PLANT_ENV`] takes.
    pub fn name(self) -> &'static str {
        match self {
            Plant::StaleOneToOneWrite => "stale_one_to_one_write",
            Plant::EarlyTombstoneGc => "early_tombstone_gc",
            Plant::ChunkWithoutEntryLock => "chunk_without_entry_lock",
            Plant::GbSkipGroupLock => "gb_skip_group_lock",
            Plant::GbLsnNotVisibility => "gb_lsn_not_visibility",
            Plant::GbFoldSidesPerKey => "gb_fold_sides_per_key",
            Plant::GbEarlyZeroGc => "gb_early_zero_gc",
            Plant::GbBuildSnapshotBeforeLocks => "gb_build_snapshot_before_locks",
            Plant::JoinNoJLock => "join_no_j_lock",
            Plant::JoinOtherSideBeforeJ => "join_other_side_before_j",
            Plant::JoinDropFlip => "join_drop_flip",
            Plant::JoinTSkipLock => "join_t_skip_lock",
        }
    }

    /// The plant [`Plant::name`] names, if any.
    pub fn from_name(name: &str) -> Option<Plant> {
        Plant::ALL.iter().copied().find(|p| p.name() == name)
    }
}

/// Parses [`PLANT_ENV`]'s value: `None` for unset or empty.
///
/// # Panics
///
/// On a name that isn't a [`Plant::name`], listing the valid ones.
fn parse(value: Option<&str>) -> Option<Plant> {
    let name = value.map(str::trim).filter(|v| !v.is_empty())?;
    match Plant::from_name(name) {
        Some(plant) => Some(plant),
        None => panic!(
            "{PLANT_ENV}={name:?} names no plant; valid names: {}",
            Plant::ALL
                .iter()
                .map(|p| p.name())
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

static ARMED: OnceLock<Option<Plant>> = OnceLock::new();
static FIRED: AtomicU64 = AtomicU64::new(0);

/// The plant this process runs with, read from [`PLANT_ENV`] on the first
/// call. Logs a warning the first time, when one is armed.
pub fn armed() -> Option<Plant> {
    *ARMED.get_or_init(|| {
        let plant = parse(std::env::var(PLANT_ENV).ok().as_deref());
        if let Some(plant) = plant {
            crate::instance_log::warn!(
                plant = plant.name(),
                "{PLANT_ENV} armed a planted ordering bug: this engine is deliberately wrong \
                 (test-only)"
            );
        }
        plant
    })
}

/// The hook every plant site calls: true when `plant` is the armed one and
/// `changes_behavior` says acting on it would change what the engine does
/// here, in which case the caller takes the planted path. Each true return
/// is counted in [`fired`].
pub fn fires(plant: Plant, changes_behavior: bool) -> bool {
    let fires = changes_behavior && armed() == Some(plant);
    if fires {
        FIRED.fetch_add(1, Ordering::Relaxed);
    }
    fires
}

/// How many times the armed plant has changed the engine's behavior in this
/// process. A sweep reads it before and after each case, so a plant that is
/// never reached is told apart from one that is reached and not caught.
pub fn fired() -> u64 {
    FIRED.load(Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_plant_round_trips_through_its_name() {
        for &plant in Plant::ALL {
            assert_eq!(parse(Some(plant.name())), Some(plant));
        }
        let mut names: Vec<&str> = Plant::ALL.iter().map(|p| p.name()).collect();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), Plant::ALL.len(), "plant names must be unique");
    }

    #[test]
    fn unset_or_empty_arms_nothing() {
        assert_eq!(parse(None), None);
        assert_eq!(parse(Some("")), None);
        assert_eq!(parse(Some("  ")), None);
    }

    #[test]
    #[should_panic(expected = "names no plant")]
    fn an_unknown_name_panics_instead_of_running_unplanted() {
        parse(Some("stale_one_to_one_writ"));
    }

    /// The default suite never sets [`PLANT_ENV`], so no hook is live in
    /// it. If this fails, something exported the variable into the test
    /// environment, and every engine test in the process ran planted.
    #[test]
    fn the_default_suite_runs_with_no_plant_armed() {
        assert_eq!(armed(), None, "{PLANT_ENV} is set in the test environment");
        assert!(!fires(Plant::StaleOneToOneWrite, true));
    }
}
