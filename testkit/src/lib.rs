//! Reusable ephemeral-Postgres integration test harness for Trellis.
//!
//! - [`cluster`] spins up a throwaway, logical-replication-enabled Postgres
//!   instance and hands out per-scenario isolated, migrated databases.
//! - [`fixtures`] seeds the common source-table / derived-table shape most
//!   staging/claiming test suites need.
//! - [`crash`] provides the seams durability tests (intake's kill-9 tests,
//!   the straddler/phase-gap tests) build their actual crash-point scenarios
//!   on.
//! - [`lsn`] gives tests a realistic LSN to stage CDC ring rows at.
//!
//! The crate also builds a `trellis-testkit` binary (`src/bin/`) that holds
//! one [`TestCluster`] for a test suite written in another language (the
//! Elixir and Ruby bindings): it prints the connection details as a JSON
//! line and tears the cluster down on a signal or when its stdin closes.
//!
//! This crate is dev-only scaffolding shared across the workspace (any
//! crate's integration tests can dev-depend on it), not part of the
//! engine's runtime behavior. Setup failures panic rather than returning
//! `Result`s: a broken test fixture should fail loudly and immediately,
//! not be handled gracefully.

pub mod cluster;
pub mod crash;
pub mod fixtures;
pub mod lsn;

pub use cluster::{ClusterBackup, StopMode, TestCluster, TestDatabase};
pub use lsn::wal_insert_lsn;
