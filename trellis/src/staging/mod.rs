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
pub mod watermark;
pub mod worker_registry;

pub use append::{
    CdcOp, RING_SIZE, StagedChange, TRUNCATE_SENTINEL_KEY, append, ring_slot_is_free,
};
pub use apply::{
    ApplyError, ApplyOutcome, ApplyPlan, MAX_COALESCE_SEGMENTS, MAX_HOP_GEN, ManyApplyOutcome,
    drain_many, drain_once, next_claimable_segment, next_claimable_segments,
};
pub use claim::{
    DEFAULT_DRAINER_WINDOW, MIN_ROWS_TO_SPLIT, SEG_BUCKETS, claim, count_live_drainers,
    owned_bucket_filter, register_drainer,
};
pub use converge::{
    await_converged, converged_through, has_pending, pending_count, watermark_token,
};
pub use error::StagingError;
pub use fold::{BucketFilter, FoldedChange, fold, merge_folded_changes};
pub use liveness::{
    DEFAULT_RECLAIM_TTL, FENCE_MISS_INITIAL_DELAY, FENCE_MISS_MAX_DELAY, FenceMissBackoff,
    HeartbeatDaemon, HeartbeatDaemonConfig, reclaim_stale, release,
};
pub use quarantine::{
    DEFAULT_DEATH_THRESHOLD, FailureClass, HaltingStopStats, classify, halting_stop_stats,
    isolate_and_evict, purge_dropped_table, record_halting_stop, release_key,
};
pub use retire::retire_drained_segments;
pub use seal::{
    SealConfig, SealOutcome, fenced_rows, recover_stuck_seals, seal_if_active_nonempty,
    seal_phase1, seal_phase2,
};
pub use self_check::{
    Divergence, SelfCheckError, SelfCheckMode, SelfCheckOutcome, SelfCheckReport, SelfCheckScope,
};
pub use session::{PRODUCER_SINGLETON_LOCK_KEY, ProducerSession};
pub use state::{SegmentState, segment_state_counts};
pub use watermark::StagedWatermark;
pub use worker_registry::{
    deregister_worker, has_live_workers, reclaim_stale_workers, register_worker,
};
