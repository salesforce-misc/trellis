//! Trellis engine: a declarative API for creating incrementally maintained
//! data transformations in PostgreSQL.
//!
//! [`app::Trellis`] is the embedder's entry point — one facade covering the
//! whole lifecycle: apply migrations, register relationships and transform
//! definitions, list them, request backfills, and run the live pipeline
//! (CDC intake, ring maintenance, and N application workers). It composes
//! everything below into one interface so callers never stitch the
//! primitives together themselves; [`client::Client`] is the runtime it
//! starts, still public for embedders that want to drive it directly.
//! [`blocking::BlockingTrellis`] wraps the same facade for callers that
//! can't assume a `tokio` runtime on their own thread (issue #87's future
//! FFI embedding). Everything else in the crate is machinery these compose:
//!
//! - [`config`] resolves a [`Config`] from CLI args + environment.
//! - [`pool`] manages a `deadpool-postgres` connection pool and exposes the
//!   per-connection session bootstrap seam.
//! - [`migrate`] applies Trellis's embedded SQL migrations.
//! - [`identity`] decides, before migrations run, whether the configured
//!   schema is safe to attach to — see `docs/instance-identity.md`.
//! - [`intake`] streams committed source-table changes off a logical
//!   replication slot into the durable staging ring.
//! - [`staging`] owns the ring, sealing, claim-time fold, and the exactly-once
//!   apply path — see `docs/staging-and-claiming/README.md`.
//! - [`defs`] parses, validates, and catalogs transform definitions (today:
//!   the 1-1, `+`-only grammar — see `docs/decisions/0004-transform-definition-grammar.md`).
//! - [`error_code`] is a small, stable [`error_code::ErrorCode`] taxonomy
//!   every error type in this crate can report via a `code()` method,
//!   independent of its own (freely growing) internal variants — see
//!   `docs/decisions/0008-public-api-design.md`, decision 3.
//! - [`metrics`] is a facade over the `metrics`/`metrics-exporter-prometheus`
//!   in-process registry (issue #51, epic #49) and its Prometheus text
//!   exposition (issue #53, [`app::Trellis::metrics`]) — see
//!   `docs/observability.md` and
//!   `docs/decisions/0009-observability-decisions.md`.
//! - Logs/traces (issue #56, epic #49) flow through the plain `tracing`
//!   facade — spans modeling the propagation path (source commit → hop →
//!   hop → apply, ADR-0009 decision 3) and events at operationally
//!   meaningful points, in [`intake`] and [`staging`] directly, not a
//!   separate module of their own. [`otel`] (behind the optional `otlp`
//!   Cargo feature, off by default) is the OTLP export layer an embedder can
//!   add to their own subscriber; with the feature off, or with no
//!   subscriber installed at all, spans/events cost only `tracing`'s own
//!   near-zero no-subscriber overhead.
//!
//! **Current subset**: 1-1 scalar transforms only end to end (issue #11's
//! 1-1 slice). Aggregate/invertible-delta maintenance is not yet wired up —
//! the apply frame that will carry it (fence, lock order, atomic apply ∪
//! mark-drained) is already in place in [`staging`].
//!
//! The ring has a fixed [`staging::RING_SIZE`] of 4 slots; a `drained`
//! segment's slot is freed for reuse by retirement (stage 06,
//! `docs/staging-and-claiming/06-cleanup-and-reclaim.md`,
//! [`staging::retire_drained_segments`]), run both as
//! [`staging::seal_if_active_nonempty`]'s one-retry-on-`RingFull` step and on
//! every maintenance tick ([`client::ClientOptions::maintenance_interval`]).
//! Quarantine, stage 06's other half, is not yet implemented.

pub mod app;
pub mod blocking;
pub mod client;
pub mod config;
pub mod defs;
pub mod error;
pub mod error_code;
pub mod identity;
pub mod intake;
pub mod metrics;
pub mod migrate;
pub mod numeric;
#[cfg(feature = "otlp")]
pub mod otel;
pub mod pool;
pub mod staging;

pub use app::{
    DefinitionSummary, PoisonEntry, PoisonSample, QuarantineEntry, QuarantineState,
    QuarantineTarget, RelationshipSummary, Trellis, TrellisError, TrellisOptions,
};
pub use blocking::BlockingTrellis;
pub use client::{Client, ClientError, ClientOptions};
pub use config::Config;
pub use defs::{Definition, RelationshipCardinality, RelationshipDefinition, TransformStatus};
pub use error::Error;
pub use error_code::ErrorCode;
pub use identity::Identity;
pub use migrate::migrate;
pub use numeric::Numeric;
pub use pool::Pool;
pub use staging::{Divergence, SelfCheckMode, SelfCheckOutcome, SelfCheckReport, SelfCheckScope};

/// Placeholder entry point exercising the async plumbing the engine will
/// build on. Returns the crate version so callers have something to check.
pub async fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn version_is_reported() {
        assert_eq!(version().await, env!("CARGO_PKG_VERSION"));
    }
}
