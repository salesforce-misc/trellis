//! Planted ordering bugs (issue #557 part 3): known-wrong engine behavior the
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
//!    // Planted bug (#557): <what it breaks>. See `crate::plant`.
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
//! 3. Run the concurrent tier's sweep for it (`generative/tests/
//!    concurrent_convergence.rs`, `planted_bugs_are_caught`), and record its
//!    catch rate next to the others in #557 / #627.
//!
//! A plant is never "fixed": it is the regression the tier exists to catch.
//! If the engine code it hooks is rewritten (epic #556 rewrites several),
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
    /// Claim exclusivity (the drain's bucket filter,
    /// `staging::claim::HeldShare::filter`): every claimer of a split batch
    /// folds *all* of its buckets instead of the ones it won, so two workers
    /// apply the same rows. Found by #557 part 1's review.
    ClaimAllBuckets,
    /// Aggregate apply (`staging::apply_aggregate`, and a ledger target's
    /// group upsert in `staging::ledger` since #623 D3): each apply
    /// transaction takes a non-waiting advisory lock per group before it
    /// writes the group, and drops its delta for any group another open
    /// apply transaction already holds. Only two workers applying one group
    /// at once can trip it. Found by #557 part 1's review.
    DropRacingGroupDelta,
    /// Issues #344/#392 (#623 D6): a 1-1 target applies every change without
    /// ADR-0002's I2 (`staging::one_to_one_ledger::update_entries`): not
    /// skipped when a Re-derive already read it, nor when a newer change
    /// already applied. A page that drains after a newer one for the same key
    /// then leaves the older value behind.
    StaleOneToOneWrite,
    /// Issue #321: an aggregate delta at or below its group's recompute
    /// horizon applies as a delta, instead of re-deriving the group
    /// (`staging::apply_aggregate::delta_may_be_absorbed`). A commit a
    /// forced recompute already counted is then counted again.
    IgnoreRecomputeHorizon,
    /// ADR-0002 I1 (#623 D3): a ledger target's page takes no entry lock
    /// before its Re-derive read (`staging::ledger`), so the read can run
    /// while another page is between its own read and its write for the
    /// same key, and one of the two writes the entry from a stale read.
    SkipLedgerLock,
    /// ADR-0002 I2 (#623 D3): a ledger target skips a change only by `lsn`
    /// (at or below the entry's `applied_lsn`), without the visibility test
    /// against the entry's Re-derive `basis` (`staging::ledger`). A change a
    /// Re-derive already read is then applied a second time.
    LsnOnlySkip,
    /// ADR-0002 I4 (#623 D7): tombstone GC (`staging::retire::collect_tombstones`)
    /// collects through the highest drained segment instead of the
    /// contiguous drained prefix, so a tombstone goes while an older change
    /// to its key is still in an undrained segment, which then applies to
    /// the fresh entry and brings the deleted row back.
    EarlyTombstoneGc,
}

impl Plant {
    /// Every plant, in the order the concurrent tier's sweep runs them.
    pub const ALL: &'static [Plant] = &[
        Plant::ClaimAllBuckets,
        Plant::DropRacingGroupDelta,
        Plant::StaleOneToOneWrite,
        Plant::IgnoreRecomputeHorizon,
        Plant::SkipLedgerLock,
        Plant::LsnOnlySkip,
        Plant::EarlyTombstoneGc,
    ];

    /// The name [`PLANT_ENV`] takes.
    pub fn name(self) -> &'static str {
        match self {
            Plant::ClaimAllBuckets => "claim_all_buckets",
            Plant::DropRacingGroupDelta => "drop_racing_group_delta",
            Plant::StaleOneToOneWrite => "stale_one_to_one_write",
            Plant::IgnoreRecomputeHorizon => "ignore_recompute_horizon",
            Plant::SkipLedgerLock => "skip_ledger_lock",
            Plant::LsnOnlySkip => "lsn_only_skip",
            Plant::EarlyTombstoneGc => "early_tombstone_gc",
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
            tracing::warn!(
                plant = plant.name(),
                "{PLANT_ENV} armed a planted ordering bug: this engine is deliberately wrong \
                 (test-only, issue #557)"
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
        parse(Some("claim_all_bucket"));
    }

    /// The default suite never sets [`PLANT_ENV`], so no hook is live in
    /// it. If this fails, something exported the variable into the test
    /// environment, and every engine test in the process ran planted.
    #[test]
    fn the_default_suite_runs_with_no_plant_armed() {
        assert_eq!(armed(), None, "{PLANT_ENV} is set in the test environment");
        assert!(!fires(Plant::ClaimAllBuckets, true));
    }
}
