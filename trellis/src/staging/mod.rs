//! The staging ring (issue #6, stage 02 of
//! docs/staging-and-claiming/README.md): the append-only substrate every
//! producer writes into and every consumer (sealing, claiming — later
//! stages) reads out of.
//!
//! - [`append`] is the blind append path plus the `RingFull` predicate.
//! - [`session`] is the session guards (`synchronous_commit`, the producer
//!   singleton) a producer must hold before it may append.
//! - [`state`] is the segment lifecycle's one legal-transition graph.
//! - [`seal`] is stage 03 (issue #9): the fence, the two-phase seal, the
//!   guards, seal-on-demand, and age-gated crash recovery.
//! - [`fold`] is stage 04's claim-time fold (issue #10): collapsing a
//!   sealed batch's fenced window into one record per key.
//! - [`claim`] is stage 04's bucket partitioning and multi-worker claims
//!   (issue #14): deciding a batch's bucket count at seal, the drainer
//!   registry as the share denominator, and the one-statement claim + flip.
//! - [`liveness`] is stage 04's claim-liveness half (issue #15): the
//!   out-of-band heartbeat daemon, release-on-error, reclaim-on-TTL, and
//!   the consecutive-fence-miss backoff.
//! - [`apply`] is stage 05's apply ∪ mark-drained (issue #11), 1-1/scalar
//!   subset only: the three-phase drain (claim + fold, compute, apply ∪
//!   mark-drained), the version fence, and downstream propagation.
//! - [`retire`] is stage 06's retirement half (issue #13/#58): freeing a
//!   `drained` segment's ring slot once nobody can still need it.
//! - [`quarantine`] is stage 06's other half (issue #16): isolate, evict,
//!   park, release — failure classification, per-key isolation and
//!   eviction, parked work as the source of truth for an excluded key's
//!   later healthy changes, operator-driven release, and the one sanctioned
//!   exception to immutability (a dropped source table's purge).
//! - [`watermark`] is issue #132's in-process "staged-through" LSN (guard
//!   (a), "the watermark barrier") — a small, injectable handle intake
//!   advances and the reverse-delta apply path (`apply`) reads.
//! - [`worker_registry`] is issue #144's worker-level liveness registry:
//!   one row per live drain worker, independent of whether it currently
//!   holds a claim — the read behind `Trellis::has_live_drain_workers`.
//! - [`self_check`] is issue #174's production recompute audit
//!   (ADR-0013): a read-only, keyset-bounded comparison of a persisted 1-1
//!   target against an independently-rendered Postgres recompute — see
//!   [`crate::app::Trellis::self_check`] for the public facade.
//! - [`error`] is this module's error type.
//!
//! What this module does *not* do: delta arithmetic for aggregate
//! transforms (blocked on aggregate transform-defs), or intake's
//! replication-slot machinery (stage 01) — see
//! docs/staging-and-claiming/02-the-staging-ring.md,
//! docs/staging-and-claiming/03-sealing-and-the-fence.md,
//! docs/staging-and-claiming/04-claiming-and-the-fold.md, and
//! docs/staging-and-claiming/05-apply-and-exactly-once-deltas.md for exactly
//! where each stage's boundary sits.

pub mod append;
pub mod apply;
pub mod apply_aggregate;
pub mod claim;
pub mod converge;
pub mod error;
pub mod fold;
pub mod liveness;
pub mod quarantine;
pub mod retire;
pub mod seal;
pub mod self_check;
pub mod session;
pub mod state;
pub(crate) mod target_mutations;
pub mod watermark;
pub mod worker_registry;

// This module is tier 3 (`pub(crate)`, ADR-0012), so these flattened
// re-exports are a convenience for the engine itself and for the two gated
// doors onto it — see `crate::dev` and `Cargo.toml`'s `internals` feature.
// Names the engine does not use are split out below and compiled only behind
// those gates, which is what keeps a plain `cargo build` free of
// `unused_imports` rather than an `allow`.
#[cfg(any(test, feature = "internals"))]
pub use append::append;
pub use append::{CdcOp, StagedChange};
pub use apply::{ApplyError, MAX_COALESCE_SEGMENTS, drain_many, next_claimable_segments};
pub use claim::{DEFAULT_DRAINER_WINDOW, claim, count_live_drainers, register_drainer};
pub use error::StagingError;
pub use liveness::{
    DEFAULT_RECLAIM_TTL, HeartbeatDaemon, HeartbeatDaemonConfig, reclaim_stale, release,
};
pub use retire::retire_drained_segments;
pub use seal::{SealConfig, recover_stuck_seals, seal_if_active_nonempty};
// `self_check`'s non-error types are reached through `lib.rs`'s own
// crate-root re-export (`Divergence`/`SelfCheckMode`/etc. traffic in
// `Trellis::self_check`'s public signature, tier 2 per ADR-0012), which
// hops through this flat name — so, unlike most of this tier, this one has
// an always-on external consumer even though the engine itself only ever
// reaches these through the qualified `self_check::` path (see `app.rs`).
// `SelfCheckError` (the `TrellisError::SelfCheck` payload) is deliberately
// left out here: `lib.rs` reaches it via the qualified
// `staging::self_check::SelfCheckError`, matching `ApplyError`/
// `StagingError`'s own error-type precedent, so this flat re-export would
// be unused.
pub use self_check::{
    Divergence, SelfCheckMode, SelfCheckOutcome, SelfCheckReport, SelfCheckScope,
};
pub use session::ProducerSession;
pub use state::segment_state_counts;
pub use watermark::StagedWatermark;
pub use worker_registry::{deregister_worker, reclaim_stale_workers, register_worker};

// Reached from `crate::dev` (ADR-0012's sanctioned exception) by
// `generative`'s backend drivers.
#[cfg(any(test, feature = "test-util"))]
pub use converge::{await_converged, has_pending, watermark_token};
#[cfg(any(test, feature = "test-util"))]
pub use seal::{seal_phase1, seal_phase2};

// Reached only by this crate's own `tests/*.rs`, through the `internals`
// feature (ADR-0012; see `Cargo.toml`). Not part of `dev`.
#[cfg(any(test, feature = "internals"))]
pub use append::{RING_SIZE, TRUNCATE_SENTINEL_KEY, ring_slot_is_free};
#[cfg(any(test, feature = "internals"))]
pub use apply::next_claimable_segment;
#[cfg(any(test, feature = "internals"))]
pub use apply::{ApplyOutcome, ApplyPlan, MAX_HOP_GEN, ManyApplyOutcome, drain_once};
#[cfg(any(test, feature = "internals"))]
pub use claim::{MIN_ROWS_TO_SPLIT, SEG_BUCKETS, owned_bucket_filter};
#[cfg(any(test, feature = "internals"))]
pub use converge::{converged_through, pending_count};
#[cfg(any(test, feature = "internals"))]
pub use fold::{BucketFilter, FoldedChange, fold, merge_folded_changes};
#[cfg(any(test, feature = "internals"))]
pub use liveness::{FENCE_MISS_INITIAL_DELAY, FENCE_MISS_MAX_DELAY, FenceMissBackoff};
#[cfg(any(test, feature = "internals"))]
pub use quarantine::{
    DEFAULT_DEATH_THRESHOLD, FailureClass, HaltingStopStats, classify, halting_stop_stats,
    isolate_and_evict, purge_dropped_table, record_halting_stop, release_key,
};
#[cfg(any(test, feature = "internals"))]
pub use seal::{SealOutcome, fenced_rows};
#[cfg(any(test, feature = "internals"))]
pub use session::producer_singleton_lock_key;
#[cfg(any(test, feature = "internals"))]
pub use state::SegmentState;
#[cfg(any(test, feature = "internals"))]
pub use target_mutations::TargetMutations;
#[cfg(any(test, feature = "internals"))]
pub use worker_registry::has_live_workers;
