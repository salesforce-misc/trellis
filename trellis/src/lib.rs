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
//! FFI embedding). Those three are tier 1 in
//! `docs/decisions/0012-curate-public-api-demote-engine-modules.md` — the
//! only front doors.
//!
//! Tier 2 is the small set of composable primitives an embedder may reach
//! for directly instead of going through the facade:
//!
//! - [`config`] resolves a [`Config`] from CLI args + environment.
//! - [`pool`] manages a `deadpool-postgres` connection pool and exposes the
//!   per-connection session bootstrap seam.
//! - [`migrate`] applies Trellis's embedded SQL migrations.
//! - [`identity`] decides, before migrations run, whether the configured
//!   schema is safe to attach to — see `docs/instance-identity.md`.
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
//!   meaningful points, in `intake` and `staging` directly, not a
//!   separate module of their own. `otel` (behind the optional `otlp`
//!   Cargo feature, off by default) is the OTLP export layer an embedder can
//!   add to their own subscriber; with the feature off, or with no
//!   subscriber installed at all, spans/events cost only `tracing`'s own
//!   near-zero no-subscriber overhead.
//!
//! Tier 3 is the engine — `defs` (parses, validates, and catalogs transform
//! definitions), `intake` (streams committed source-table changes off a
//! logical replication slot into the durable staging ring), and `staging`
//! (the ring, sealing, claim-time fold, and the exactly-once apply path) —
//! together with everything beneath them. They are `pub(crate)`: machinery
//! the facade composes, not names an embedder can write. See
//! `docs/staging-and-claiming/README.md` and
//! `docs/decisions/0004-transform-definition-grammar.md` for what they do,
//! and ADR-0012 for why they aren't API. `dev` (behind the off-by-default
//! `test-util` feature) is the one curated, uncommitted door onto them, for
//! the generative suite's independent cross-check leg.
//!
//! **Current subset**: 1-1 scalar transforms only end to end (issue #11's
//! 1-1 slice). Aggregate/invertible-delta maintenance is not yet wired up —
//! the apply frame that will carry it (fence, lock order, atomic apply ∪
//! mark-drained) is already in place in `staging`.
//!
//! The ring has a fixed `staging::RING_SIZE` of 4 slots; a `drained`
//! segment's slot is freed for reuse by retirement (stage 06,
//! `docs/staging-and-claiming/06-cleanup-and-reclaim.md`,
//! `staging::retire_drained_segments`), run both as
//! `staging::seal_if_active_nonempty`'s one-retry-on-`RingFull` step and on
//! every maintenance tick ([`client::ClientOptions::maintenance_interval`]).
//! Quarantine, stage 06's other half, is not yet implemented.

// Tier 1 (the facade) and tier 2 (composable primitives) — see ADR-0012.
pub mod app;
pub mod blocking;
pub mod client;
pub mod config;
pub mod error;
pub mod error_code;
pub mod float;
pub mod identity;
pub mod integer;
pub mod metrics;
pub mod migrate;
pub mod numeric;
#[cfg(feature = "otlp")]
pub mod otel;
pub mod pool;

// Tier 3 (the engine) — see ADR-0012. Crate-private: no external crate names
// `trellis::defs::*`, `trellis::staging::*`, or `trellis::intake::*`. The
// curated crate-root re-exports below are the boundary, not a suggestion
// layered over reachable internals.
//
// `internals` (off by default, never on a production dependency edge) opens
// them back up for this crate's *own* `tests/*.rs`, which are separate
// compilation units and so cannot see `pub(crate)` — see `Cargo.toml`. It is
// not the ADR's sanctioned exception, and it is not API; see `dev` below for
// the sibling crates' curated, equally uncommitted surface.
#[cfg(not(feature = "internals"))]
pub(crate) mod defs;
#[cfg(feature = "internals")]
pub mod defs;
#[cfg(not(feature = "internals"))]
pub(crate) mod intake;
#[cfg(feature = "internals")]
pub mod intake;
#[cfg(not(feature = "internals"))]
pub(crate) mod staging;
#[cfg(feature = "internals")]
pub mod staging;
// `temporal` (issue #113) sits with tier 3 rather than beside `float`/
// `integer` in tier 2 for one concrete reason: its public signatures name
// `defs::pg_type::PgType`, which is itself crate-private above. The temporal
// families never earned their own `ValueType` variant the way #111's
// integers and #112's floats did (see that module's doc comment), so there
// is no tier-2-visible type to hang them off.
#[cfg(not(feature = "internals"))]
pub(crate) mod temporal;
#[cfg(feature = "internals")]
pub mod temporal;
// `netaddr` (issue #116) sits here for the same reason `temporal` does: its
// public signatures name `defs::pg_type::PgType`, itself crate-private
// above.
#[cfg(not(feature = "internals"))]
pub(crate) mod netaddr;
#[cfg(feature = "internals")]
pub mod netaddr;

#[cfg(any(test, feature = "test-util"))]
pub mod dev;

// --- Tier 1: the facade ---------------------------------------------------
pub use app::{
    DefinitionSummary, PoisonEntry, PoisonSample, QuarantineEntry, QuarantineState,
    QuarantineTarget, RelationshipSummary, Trellis, TrellisError, TrellisOptions,
};
pub use blocking::BlockingTrellis;
pub use client::{Client, ClientError, ClientOptions};

// --- Tier 2: composable primitives ----------------------------------------
pub use config::Config;
pub use error::Error;
pub use error_code::ErrorCode;
pub use float::FloatWidth;
pub use identity::Identity;
pub use integer::IntWidth;
pub use metrics::Metrics;
pub use migrate::migrate;
pub use numeric::Numeric;
#[cfg(feature = "otlp")]
pub use otel::OtelError;
pub use pool::Pool;
pub use staging::{Divergence, SelfCheckMode, SelfCheckOutcome, SelfCheckReport, SelfCheckScope};

// --- Tier 2: the error types the public errors wrap -----------------------
// Re-exported here even though they originate in `pub(crate)` modules: an
// embedder handed an `Error` must be able to name and match its payload.
pub use defs::catalog::CatalogError;
pub use defs::ddl::DdlError;
pub use defs::error::ParseError;
pub use defs::validate::ValidationError;
pub use intake::error::IntakeError;
pub use staging::apply::ApplyError;
pub use staging::error::StagingError;
pub use staging::self_check::SelfCheckError;

// --- Tier 2: the types tier-1/tier-2 signatures traffic in ----------------
// Compiler-forced: a `pub fn` on the facade that returns or accepts one of
// these would otherwise expose an unnameable type.
pub use defs::{Definition, RelationshipCardinality, RelationshipDefinition, TransformStatus};

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
