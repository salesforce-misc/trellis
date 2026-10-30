//! Trellis engine: a declarative API for creating incrementally maintained
//! data transformations in PostgreSQL.
//!
//! [`Trellis`] is the embedder's entry point — one facade covering the
//! whole lifecycle: apply migrations, register transform definitions, list
//! them, request backfills, and run the live pipeline
//! (capture, ring maintenance, and N application workers). It composes
//! everything below into one interface so callers never stitch the
//! primitives together themselves; [`Client`] is the runtime it
//! starts, still public for embedders that want to drive it directly.
//! [`BlockingTrellis`] wraps the same facade for callers that
//! can't assume a `tokio` runtime on their own thread (the FFI
//! bindings). Those three are tier 1 in
//! `docs/decisions/0012-curate-public-api-demote-engine-modules.md` — the
//! only front doors.
//!
//! Every public name is re-exported at the crate root; the modules that
//! define them are private (`otel`, behind the `otlp` feature, is the one
//! public module). Tier 2 is the small set of composable primitives
//! an embedder may reach for directly instead of going through the facade:
//!
//! - [`Config`] resolves from CLI args + environment (`Config::resolve`) or
//!   from a DSN.
//! - [`Pool`] manages a `deadpool-postgres` connection pool, which
//!   [`Trellis::pool`] hands back for an embedder's own queries; each
//!   [`Pool::get`] checks out a [`PooledClient`].
//! - [`migrate()`] applies Trellis's embedded SQL migrations, after checking
//!   that the configured schema is safe to attach to — see
//!   `docs/instance-identity.md`.
//! - [`ErrorCode`] is a small, stable taxonomy every error type in this
//!   crate can report via a `code()` method, independent of its own (freely
//!   growing) internal variants — see
//!   `docs/decisions/0008-public-api-design.md`, decision 3.
//! - [`statement_kind`] names which form a statement is ([`StatementKind`])
//!   without applying it, using `apply`'s own parser — the check a binding
//!   makes before handing a statement to `apply`.
//! - [`Metrics`] is a facade over the `metrics`/`metrics-exporter-prometheus`
//!   in-process registry and its Prometheus text
//!   exposition ([`Trellis::metrics`]) — see
//!   `docs/observability.md` and
//!   `docs/decisions/0009-observability-decisions.md`.
//! - Logs/traces flow through the plain `tracing`
//!   facade — spans modeling the propagation path (source commit → hop →
//!   hop → apply, ADR-0009 decision 3) and events at operationally
//!   meaningful points, in `capture` and `staging` directly, not a
//!   separate module of their own. `otel` (behind the optional `otlp`
//!   Cargo feature, off by default) is the OTLP export layer an embedder can
//!   add to their own subscriber; with the feature off, or with no
//!   subscriber installed at all, spans/events cost only `tracing`'s own
//!   near-zero no-subscriber overhead.
//!
//! Tier 3 is the engine — `defs` (parses, validates, and catalogs transform
//! definitions), `capture` (the triggers that stage each source-table change
//! into the durable staging ring in its writer's transaction), `intake` (the
//! rebuild of a table's readers), and `staging`
//! (the ring, sealing, claim-time fold, and the exactly-once apply path) —
//! together with everything beneath them. They are `pub(crate)`: machinery
//! the facade composes, not names an embedder can write. See
//! `docs/staging-and-claiming/README.md` and
//! `docs/decisions/0004-transform-definition-grammar.md` for what they do,
//! and ADR-0012 for why they aren't API. `dev` (behind the off-by-default
//! `test-util` feature) is the one curated, uncommitted door onto them, for
//! the generative suite's independent SQL oracle and the benchmark's scenario
//! oracle.
//!
//! Trellis maintains target tables incrementally from source tables. A
//! definition is one of three shapes: a 1-1 transform (one target row per
//! source row), an aggregate (`GROUP BY` with invertible functions), or a
//! `JOIN` transform; `docs/transforms.md` is the grammar reference and lists
//! the supported sources and targets. Triggers on each source table stage
//! every change into a durable ring in its writer's transaction; application
//! workers claim sealed segments, fold them, and apply them to the targets
//! exactly once. A definition that can no longer be maintained correctly is
//! stopped with a reason rather than left to drift, and rows a transform
//! cannot produce are quarantined; `docs/operations.md` describes both and
//! how to repair them. `docs/data-flow.md` and `docs/staging-and-claiming/`
//! describe the pipeline, and `docs/known-correctness-gaps.md` lists the
//! ways a target can go stale or wrong without Trellis repairing it.
//!
//! The ring has a fixed `staging::RING_SIZE` of 4 slots; a `drained`
//! segment's slot is freed for reuse by retirement
//! (`docs/staging-and-claiming/06-cleanup-and-reclaim.md`), run in the
//! maintenance loop's housekeeping
//! ([`ClientOptions::housekeeping_interval`]) and as the
//! one-retry-on-`RingFull` step of sealing.

// Tier 1 (the facade) and tier 2 (composable primitives) — see ADR-0012.
#[cfg(not(feature = "internals"))]
mod app;
#[cfg(feature = "internals")]
pub mod app;
#[cfg(not(feature = "internals"))]
mod blocking;
#[cfg(feature = "internals")]
pub mod blocking;
#[cfg(not(feature = "internals"))]
mod client;
#[cfg(feature = "internals")]
pub mod client;
#[cfg(not(feature = "internals"))]
mod config;
#[cfg(feature = "internals")]
pub mod config;
pub(crate) mod deadline;
#[cfg(not(feature = "internals"))]
pub(crate) mod dump;
#[cfg(feature = "internals")]
pub mod dump;
#[cfg(not(feature = "internals"))]
mod error;
#[cfg(feature = "internals")]
pub mod error;
#[cfg(not(feature = "internals"))]
mod error_code;
#[cfg(feature = "internals")]
pub mod error_code;
#[cfg(not(feature = "internals"))]
mod float;
#[cfg(feature = "internals")]
pub mod float;
#[cfg(not(feature = "internals"))]
mod identity;
#[cfg(feature = "internals")]
pub mod identity;
pub(crate) mod instance_log;
#[cfg(not(feature = "internals"))]
mod integer;
#[cfg(feature = "internals")]
pub mod integer;
#[cfg(not(feature = "internals"))]
mod metrics;
#[cfg(feature = "internals")]
pub mod metrics;
#[cfg(not(feature = "internals"))]
mod migrate;
#[cfg(feature = "internals")]
pub mod migrate;
#[cfg(not(feature = "internals"))]
mod numeric;
#[cfg(feature = "internals")]
pub mod numeric;
#[cfg(feature = "otlp")]
pub mod otel;
#[cfg(not(feature = "internals"))]
mod pool;
#[cfg(feature = "internals")]
pub mod pool;
pub(crate) mod tls;

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
//
// `capture` generates the statement triggers that append
// source changes to the ring inside the writer's transaction (ADR-0002).
#[cfg(not(feature = "internals"))]
pub(crate) mod capture;
#[cfg(feature = "internals")]
pub mod capture;
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
// `locks` holds the lock discipline every tier-3 module shares
// (ADR-0002 invariants I6 and I7): the session `lock_timeout` bound and the
// user-table DDL retry loop.
#[cfg(not(feature = "internals"))]
pub(crate) mod locks;
#[cfg(feature = "internals")]
pub mod locks;
// `temporal` sits with tier 3 rather than beside `float`/
// `integer` in tier 2 for one concrete reason: its public signatures name
// `defs::pg_type::PgType`, which is itself crate-private above. The temporal
// families have no `ValueType` variant of their own, unlike
// integers and floats (see that module's doc comment), so there
// is no tier-2-visible type to hang them off.
#[cfg(not(feature = "internals"))]
pub(crate) mod temporal;
#[cfg(feature = "internals")]
pub mod temporal;
// `netaddr` sits here for the same reason `temporal` does: its
// public signatures name `defs::pg_type::PgType`, itself crate-private
// above.
#[cfg(not(feature = "internals"))]
pub(crate) mod netaddr;
#[cfg(feature = "internals")]
pub mod netaddr;
// `jsonb` sits here for the same reason `temporal`/`netaddr`
// do: its public signatures name `defs::pg_type::PgType`, itself
// crate-private above.
#[cfg(not(feature = "internals"))]
pub(crate) mod jsonb;
#[cfg(feature = "internals")]
pub mod jsonb;

#[cfg(any(test, feature = "test-util"))]
pub mod dev;
// Planted ordering bugs, armed per process by an env var.
// Test-only, like `dev`; reached from other crates through `dev::plant`.
#[cfg(any(test, feature = "test-util"))]
pub(crate) mod plant;

// --- Tier 1: the facade ---------------------------------------------------
pub use app::{
    Applied, BackfillFailure, BuildWait, CaptureFailure, CaptureFailureKind, CaptureWait,
    DefinitionStatus, DefinitionSummary, PoisonEntry, PoisonSample, QuarantineEntry,
    QuarantineState, QuarantineTarget, Trellis, TrellisError, TrellisOptions,
};
pub use blocking::{BlockingTrellis, PendingCall, Shutdown};
pub use client::{Client, ClientError, ClientOptions};

// --- Tier 2: composable primitives ----------------------------------------
pub use config::Config;
pub use error::Error;
pub use error_code::ErrorCode;
pub use metrics::Metrics;
pub use migrate::migrate;
#[cfg(feature = "otlp")]
pub use otel::OtelError;
pub use pool::{Pool, PooledClient, TransactionBuilder};
pub use staging::quarantine::HeldKeyRetry;
pub use staging::{
    CaptureFault, CorrectingNode, Divergence, DrainFailure, FanOutWarning, HeldKeys,
    HiddenTableSize, JoinBuildStep, JoinSide, JoinSidePart, JoinStatus, OneToOneBuildStep,
    PartialsWarning, SelfCheckJob, SelfCheckJobState, SelfCheckMode, SelfCheckOutcome,
    SelfCheckReport,
};

// --- Tier 2: the error types the public errors wrap -----------------------
// Re-exported here even though they originate in `pub(crate)` modules: an
// embedder handed an `Error` must be able to name and match its payload.
pub use defs::catalog::CatalogError;
pub use defs::ddl::DdlError;
pub use defs::error::ParseError;
pub use defs::source_index::{IndexRole, SourceIndexGap};
pub use defs::validate::ValidationError;
pub use intake::error::IntakeError;
pub use staging::apply::ApplyError;
pub use staging::error::StagingError;
pub use staging::self_check::SelfCheckError;

// --- Tier 2: the types tier-1/tier-2 signatures traffic in ----------------
// Compiler-forced: a `pub fn` on the facade that returns or accepts one of
// these would otherwise expose an unnameable type.
pub use defs::TransformStatus;
// `watermark_token`/`await_converged`'s token, so an embedder names it
// without its own (version-locked) `tokio-postgres` dependency.
pub use tokio_postgres::types::PgLsn;

// --- Tier 2: statement classification -------------------------------------
// Pure on the text, so an embedder can refuse the wrong statement form before
// handing it to `apply`. It is `apply`'s own parser, not a copy
// of its dispatch rule.
pub use defs::{StatementKind, statement_kind};
