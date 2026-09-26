//! Errors the staging ring's append path and session guards can produce.
//!
//! A staging-local enum, plain `Display` + `std::error::Error`, no
//! `thiserror`/`anyhow` — matching [`crate::error::Error`]'s convention
//! (see that module's doc comment) and [`crate::defs::catalog::CatalogError`]
//! for the pattern of a module-local error that composes with the crate's
//! via `From`.

use std::fmt;
use std::time::Duration;

use tokio_postgres::types::PgLsn;

use crate::error_code::{self, ErrorCode};

/// Failure modes specific to the staging ring (issue #6): resolving the
/// active ring slot, appending, and the session guards a producer must
/// hold before it may append.
#[derive(Debug)]
pub enum StagingError {
    /// `segment_pointer` or its `ring_slot_mirror` sequence named a
    /// `ring_slot` outside `0..RING_SIZE` (`-1`: the mirror read NULL).
    /// Should never happen — only this module writes either — but resolving
    /// it into a table name is checked and typed rather than assumed.
    InvalidRingSlot(i16),
    /// A [`super::session::ProducerSession`] was refused because the
    /// connection's effective `synchronous_commit` was `off`. A correctness
    /// requirement, not tuning: intake's durability guarantee assumes the
    /// commit that stages a change waited for its WAL to be flushed.
    SynchronousCommitOff,
    /// A second producer session tried to start while another holds the
    /// singleton lock. Two concurrent producers would double-append every
    /// change.
    ProducerAlreadyRunning,
    /// A seal would lap a ring slot that still holds a live registry row.
    /// A retirement pass is run and the seal retried once (see
    /// [`super::seal::seal_if_active_nonempty`], [`super::retire::retire_drained_segments`]);
    /// this is what's returned if it's still full after that retry.
    RingFull { ring_slot: i16 },
    /// The seal gate: the predecessor's fence hasn't settled yet — a writer
    /// that targeted its slot may still be in flight. Backpressure, not an
    /// error to surface to an operator; the caller should back off and retry
    /// the seal later.
    SealGateBlocked,
    /// Another worker's `state = 'active'` update won the race; this
    /// worker's matched zero rows. Back off — the ring already advanced.
    Raced,
    /// A batch's [`super::seal::fenced_rows`] read found `state = 'sealed'`
    /// with no `fence_snapshot` — the crash window between the seal's two
    /// phases. Failing loud here (rather than admitting or excluding every
    /// row) is what turns that window into a stall instead of a silent
    /// double-apply or data loss; see
    /// docs/staging-and-claiming/03-sealing-and-the-fence.md ("A
    /// deliberately-skipped case").
    UnfencedSealedSegment { seg_seq: i64 },
    /// [`super::converge::await_converged`] exhausted its timeout budget
    /// without `converged_through(token)` ever returning true. Named rather
    /// than a generic "values not stabilizing" message
    /// (docs/staging-and-claiming/07-convergence-and-await.md, "A round did
    /// no work"): that framing implies a schema cycle, which is not what
    /// this is — it just means the token hasn't cleared yet, and the caller
    /// should widen its budget or investigate the drain, not the schema.
    ConvergenceTimeout { token: PgLsn, waited: Duration },
    /// A direct Postgres protocol/query error.
    Db(tokio_postgres::Error),
}

impl StagingError {
    /// This error's stable, coarse [`ErrorCode`] category (`docs/decisions/0008-public-api-design.md`,
    /// decision 3). [`StagingError::ProducerAlreadyRunning`] is the one
    /// variant that's a genuine collision with existing state (a singleton
    /// lock already held) -> [`ErrorCode::Conflict`]; everything else here
    /// is either an internal ring-mechanics condition (a race lost, a full
    /// slot, a blocked seal gate, an unfenced segment, a convergence
    /// timeout, an invalid slot index) that a caller can't act on any
    /// differently than "internal failure, maybe retry", or a Postgres
    /// error classified generically.
    pub fn code(&self) -> ErrorCode {
        match self {
            StagingError::ProducerAlreadyRunning => ErrorCode::Conflict,
            // A misconfigured connection's `synchronous_commit` setting is
            // an environment precondition not met, same category as other
            // config-rejection errors.
            StagingError::SynchronousCommitOff => ErrorCode::Validation,
            StagingError::InvalidRingSlot(_)
            | StagingError::RingFull { .. }
            | StagingError::SealGateBlocked
            | StagingError::Raced
            | StagingError::UnfencedSealedSegment { .. }
            | StagingError::ConvergenceTimeout { .. } => ErrorCode::Internal,
            StagingError::Db(err) => error_code::classify_pg_error(err),
        }
    }
}

impl fmt::Display for StagingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StagingError::InvalidRingSlot(slot) => {
                write!(f, "ring slot {slot} is outside the valid range")
            }
            StagingError::SynchronousCommitOff => write!(
                f,
                "refusing a producer session: synchronous_commit is off, which breaks the \
                 staging ring's durability guarantee"
            ),
            StagingError::ProducerAlreadyRunning => write!(
                f,
                "another producer session already holds the singleton advisory lock"
            ),
            StagingError::RingFull { ring_slot } => write!(
                f,
                "ring slot {ring_slot} still holds a live registry row; sealing into it would \
                 lap unretired work"
            ),
            StagingError::SealGateBlocked => write!(
                f,
                "the seal gate is blocked: a writer targeting the predecessor segment's slot \
                 has not yet settled"
            ),
            StagingError::Raced => write!(
                f,
                "another worker sealed this active segment first; back off and retry"
            ),
            StagingError::UnfencedSealedSegment { seg_seq } => write!(
                f,
                "segment {seg_seq} is sealed but has no fence_snapshot; refusing to claim it \
                 rather than guess"
            ),
            StagingError::ConvergenceTimeout { token, waited } => write!(
                f,
                "convergence through {token} was not reached after waiting {waited:?}"
            ),
            StagingError::Db(err) => {
                write!(f, "staging ring database error: ")?;
                crate::error::write_pg_error(f, err)
            }
        }
    }
}

impl std::error::Error for StagingError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            StagingError::Db(err) => Some(err),
            _ => None,
        }
    }
}

impl From<tokio_postgres::Error> for StagingError {
    fn from(err: tokio_postgres::Error) -> Self {
        StagingError::Db(err)
    }
}
