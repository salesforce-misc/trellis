//! The in-process "staged-through" watermark (issue #132, epic #127, guard
//! (a) — "the watermark barrier"). See the plan doc's §2 guard table and §5
//! ("Barrier cost — measured") for the full design; this module is just the
//! shared, cheap, in-process value the barrier reads.
//!
//! Guard (a) and a backfill enumeration (issue #312) wait on it. Trigger
//! capture (issue #622) stages every change in the writer's own transaction,
//! so every commit a snapshot sees already has its ring rows, and the staging
//! worker's watermark is [`StagedWatermark::saturated`] from the start: the
//! wait is a no-op. F (#625) deletes the wait and this module with it.
//!
//! A `Relaxed`-ordered `AtomicU64` is enough: this value carries no other
//! memory alongside it that a reader needs synchronized.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio_postgres::types::PgLsn;

/// A cheap, `Clone`-able handle onto one shared in-process watermark.
/// Construct one per staging worker and clone it into every consumer that
/// needs to check guard (a) — the drain path
/// ([`super::apply::apply_and_mark_drained_many`] and its callers) and any
/// test that wants to exercise the guard directly.
#[derive(Debug, Clone)]
pub struct StagedWatermark(Arc<AtomicU64>);

impl StagedWatermark {
    /// Starts at LSN 0 — the fail-closed choice: until something advances
    /// this value (see [`Self::advance`]), guard (a) rejects every relationship
    /// reverse record whose captured `X` is nonzero (i.e. every one that
    /// ever exists in practice — Postgres never hands out LSN `0/0` for a
    /// real commit). That is the correct behavior at cold start: nothing
    /// has been proven staged yet, so nothing should pass the barrier
    /// vacuously. Under trigger capture nothing advances it, so only tests
    /// that exercise the guard use this.
    pub fn new() -> Self {
        Self(Arc::new(AtomicU64::new(0)))
    }

    /// A watermark that has already "caught up" to any `X` a caller could
    /// capture: what the staging worker runs with under trigger capture (issue
    /// #622), and the test default issue #132 calls for: most of the existing
    /// test suite drives `compute()`/`apply_and_mark_drained_many` directly, by
    /// hand, with nothing running to advance a real watermark. Using
    /// [`Self::new`] there would make guard (a) reject every relationship
    /// reverse record unconditionally, which is never what an unrelated test
    /// wants — this constructor makes guard (a) a pure no-op instead, so only a
    /// test that deliberately exercises it (by constructing its own
    /// [`Self::new`] and choosing when to [`Self::advance`] it) ever sees it
    /// reject anything.
    pub fn saturated() -> Self {
        Self(Arc::new(AtomicU64::new(u64::MAX)))
    }

    /// Advances the watermark to `lsn`, monotonically — a lower or equal
    /// value already published (by this or a concurrent advance) is a
    /// no-op, so an out-of-order call can never regress the barrier. Only
    /// tests advance a watermark: under trigger capture the staging worker's
    /// is [`Self::saturated`] from the start (issue #622).
    #[cfg(any(test, feature = "internals"))]
    pub fn advance(&self, lsn: PgLsn) {
        self.0.fetch_max(u64::from(lsn), Ordering::Relaxed);
    }

    /// The current published position — guard (a)'s own read.
    pub fn get(&self) -> PgLsn {
        PgLsn::from(self.0.load(Ordering::Relaxed))
    }
}

impl Default for StagedWatermark {
    /// [`Self::new`] — the fail-closed default, not [`Self::saturated`].
    /// Production code must never silently get the fail-*open* behavior
    /// just by relying on `Default`; a test that wants the no-op behavior
    /// has to ask for [`Self::saturated`] explicitly.
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_starts_at_zero_and_rejects_any_real_lsn() {
        let w = StagedWatermark::new();
        assert_eq!(w.get(), PgLsn::from(0));
        assert!(w.get() < PgLsn::from(100));
    }

    #[test]
    fn saturated_is_at_or_past_any_practical_lsn() {
        let w = StagedWatermark::saturated();
        assert!(w.get() >= PgLsn::from(u64::MAX - 1));
    }

    #[test]
    fn advance_is_monotonic() {
        let w = StagedWatermark::new();
        w.advance(PgLsn::from(50));
        assert_eq!(w.get(), PgLsn::from(50));
        w.advance(PgLsn::from(20));
        assert_eq!(
            w.get(),
            PgLsn::from(50),
            "a lower advance must never regress it"
        );
        w.advance(PgLsn::from(80));
        assert_eq!(w.get(), PgLsn::from(80));
    }

    #[test]
    fn clones_share_the_same_underlying_value() {
        let w1 = StagedWatermark::new();
        let w2 = w1.clone();
        w1.advance(PgLsn::from(42));
        assert_eq!(w2.get(), PgLsn::from(42));
    }
}
