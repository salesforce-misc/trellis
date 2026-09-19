//! Generative correctness test suite for the `trellis` crate (issue #3,
//! epic #2). See `docs/generative-test-suite.md` for the architecture.
//!
//! Five strictly-separated modules (design doc §1):
//!
//! - [`model`] — plain-data description of a generated program. Names no
//!   engine internals beyond the AST types it deliberately reuses.
//! - [`generate`] — generators producing only valid programs. Makes no
//!   engine calls.
//! - [`oracle`] — independent recompute, sharing no evaluation code with
//!   the engine.
//! - [`backend`] — the ONLY module that drives the engine's pipeline and
//!   reads back derived state.
//! - [`run`] — drives a program through a backend, asserts the properties.
//!
//! `generate`/`oracle`/`run` are skeletons today; [`model`] and [`backend`]
//! are this issue's substance.

// Every module below names `trellis::dev` — ADR-0012's curated, uncommitted
// re-export of the engine items this suite's oracle and backends need — which
// exists only when `trellis/test-util` is on. This crate turns that on through
// its own `engine-access` feature, enabled by the self dev-dependency in
// `Cargo.toml` and by nothing else, so a plain `cargo build --workspace`
// compiles this crate down to an empty lib rather than leaking the engine's
// test-only hooks into `cli`'s production binary. See `Cargo.toml` for the
// feature-unification rule that protects against.
#[cfg(feature = "engine-access")]
pub mod backend;
#[cfg(feature = "engine-access")]
pub mod generate;
#[cfg(feature = "engine-access")]
pub mod model;
#[cfg(feature = "engine-access")]
pub mod oracle;
#[cfg(feature = "engine-access")]
pub mod run;
