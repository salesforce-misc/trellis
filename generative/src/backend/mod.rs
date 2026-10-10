//! The backend seam (design doc §1 "The backend seam"): the ONLY module
//! that drives the engine's maintenance pipeline and reads back derived
//! state. Nothing outside this module may import `trellis::client`,
//! `trellis::staging`, or the engine's own `defs::catalog`/`ddl` — the oracle
//! (`crate::oracle`) and generators (`crate::generate`) must stay reachable
//! only through the shared, engine-independent pieces named in the design
//! doc, so a second backend can be added without touching either. Two
//! backends exist today: [`ManualBackend`] (a concurrent-runtime-capable, but
//! always in-process, [`trellis::Client`]) and [`SubprocessBackend`] (issue
//! #166: a real OS subprocess `testkit::CrashGuard` can `SIGKILL`, for tests
//! that need a genuine crash rather than an in-process simulation). Both
//! share [`sql`]'s DDL/DML/read-back rendering — see that module's doc
//! comment.

mod manual;
mod sql;
mod subprocess;

pub use manual::{
    ManualApplier, ManualBackend, ManualBackendError, SEAL_ON_DEMAND_INTERVAL,
    SERVER_STOP_RECLAIM_TTL, SourceTables, await_pool_usable,
};
pub use subprocess::{SubprocessBackend, SubprocessBackendError};

use std::collections::BTreeMap;
use std::future::Future;

use crate::model::{BurstAction, Op, Program, RestartMode};

/// Merged source+derived state, read back deterministically: `table -> pk
/// (rendered text) -> column -> value (rendered text, `None` is SQL
/// `NULL`)`. A `BTreeMap` at every level so two snapshots compare and diff
/// stably regardless of physical row/column order (design doc §1).
pub type Snapshot = BTreeMap<String, BTreeMap<String, BTreeMap<String, Option<String>>>>;

/// A backend that can install a [`Program`]'s schema and definitions, apply
/// its ops as raw source DML, wait for the engine to catch up, and read
/// back merged state. See the module doc comment and design doc §1 for the
/// contract each method must uphold — in particular, [`Backend::apply`]
/// must never go through an application-level notification API, and
/// [`Backend::quiesce`] must be a client-side watermark poll, never a
/// `sleep`.
pub trait Backend {
    type Error: std::fmt::Debug;

    /// Creates every table in `program.tables`, installs every definition
    /// in `program.defs` (and its neighbor target table), and starts
    /// whatever engine machinery this backend needs to keep them
    /// incrementally maintained.
    fn install(
        &mut self,
        program: &Program,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Applies one op as raw source DML. On success, returns the number of
    /// rows the statement affected — `0` for an update/delete that named a
    /// primary key no row has (a source no-op, not an error); an `Err`
    /// return means the statement itself was rejected (e.g. a primary-key
    /// violation). Callers compare this against the op's
    /// [`crate::model::OpOutcome`] expectation (design doc §4 "operation
    /// errors are checked, not swallowed").
    fn apply(&mut self, op: &Op) -> impl Future<Output = Result<u64, Self::Error>> + Send;

    /// Blocks until the engine has caught up with every op applied so far.
    fn quiesce(&mut self) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Reads back merged source+derived state as a [`Snapshot`].
    fn snapshot(&mut self) -> impl Future<Output = Result<Snapshot, Self::Error>> + Send;

    /// Simulates an ungraceful crash-and-restart of the backend's primary
    /// engine client (improvement-plan task E3): tears down whatever is
    /// currently running it and starts a fresh one against the same target.
    /// The ring is durable Postgres state, not in-memory, so a fresh client
    /// is expected to pick back up exactly where the crashed one left off:
    /// no stuck or lost work, no duplicate processing. Each implementation
    /// picks its own faithful stand-in for "the process died": [`ManualBackend`]
    /// (in-process) replaces its `trellis::Client`, awaiting the outgoing
    /// one's graceful `shutdown()` first (issue #251) rather than just
    /// dropping it — `Drop`'s shutdown signal is best-effort and doesn't
    /// join the background thread, so a bare drop-then-restart could race
    /// the old producer's advisory-lock release against the new producer's
    /// acquire and spuriously fail with `ProducerAlreadyRunning`; no
    /// subprocess/`SIGKILL` machinery is needed there either way.
    /// [`SubprocessBackend`] (issue #166) does the real thing: `SIGKILL`s the
    /// actual OS process, *waits for it to actually exit* and then for
    /// Postgres to free its singleton before spawning a fresh one — the same
    /// "don't just drop, wait for the teardown to really finish" discipline,
    /// one layer down.
    fn restart(&mut self) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Starts an additional engine client alongside whatever is already
    /// running, application-worker-only (never a second staging worker —
    /// see `trellis::client`'s module doc comment: exactly one staging worker
    /// per fleet), demonstrating multiple clients can coexist draining the
    /// same ring (improvement-plan task E3).
    fn scale_out(&mut self) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// Applies ops on a connection of its own (issue #557), so the concurrent
/// tier can issue several lanes of a burst at once, each from its own task.
/// Same contract as [`Backend::apply`]: raw source DML, the affected row
/// count on success, `Err` when the statement itself was rejected.
pub trait OpApplier: Send + 'static {
    type Error: std::fmt::Debug + Send;

    fn apply(&mut self, op: &Op) -> impl Future<Output = Result<u64, Self::Error>> + Send;
}

/// A [`Backend`] the concurrent tier can drive (issue #557): it hands out
/// independent [`OpApplier`]s, and it can say how its engine actually sealed
/// and claimed the batches a run produced ([`DrainAudit`]).
pub trait ConcurrentBackend: Backend {
    type Applier: OpApplier;

    /// A new applier on its own connection, for tables already installed.
    fn applier(&self) -> impl Future<Output = Result<Self::Applier, Self::Error>> + Send;

    /// Starts recording every seal and claim from here on, tagged with the
    /// burst passed to [`ConcurrentBackend::begin_burst`]. Call once, after
    /// [`Backend::install`].
    fn start_drain_audit(&mut self) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Tags every batch sealed from now on as belonging to burst `burst`.
    fn begin_burst(&mut self, burst: usize)
    -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// What the recording has seen so far.
    fn drain_audit(&mut self) -> impl Future<Output = Result<DrainAudit, Self::Error>> + Send;

    /// Takes one operator action while a burst's lanes run (issue #557 part
    /// 2), through the public API an operator would use: see each
    /// [`BurstAction`] for the call. `program` supplies an
    /// [`BurstAction::Install`]'s definition, which from then on is part of
    /// what [`Backend::quiesce`] waits for and [`Backend::snapshot`] reads.
    fn act(
        &mut self,
        program: &Program,
        action: &BurstAction,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// The fewest rows a sealed batch needs for the engine to split it into
/// buckets that several workers can claim (`staging::claim`). Re-exported
/// here because nothing outside this module may name `trellis::staging` (the
/// module doc comment); `crate::run::Coverage` reads it.
pub const SPLIT_THRESHOLD_ROWS: usize = trellis::dev::staging::MIN_ROWS_TO_SPLIT as usize;

/// How the engine sealed and claimed one run's batches (issue #557), read
/// from triggers on its own `segments` and `seg_claims` rows. This is the
/// evidence that a run really split batches across workers, as opposed to
/// merely issuing many ops. Plain counts, so runs add up
/// ([`DrainAudit::add`]).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DrainAudit {
    /// Batches sealed.
    pub sealed: u64,
    /// Sealed batches split into more than one bucket.
    pub split: u64,
    /// Split batches whose buckets were claimed by two or more workers.
    pub split_across_workers: u64,
    /// The most distinct workers that claimed buckets of one batch.
    pub max_workers_per_batch: u64,
    /// The most rows sealed into one batch.
    pub max_rows_per_batch: u64,
    /// `(burst, table, key)`s whose changes were sealed into two or more
    /// batches during the same burst: one key's changes in flight in several
    /// batches at once.
    pub keys_in_several_batches: u64,
}

impl DrainAudit {
    /// Adds `other`'s counts into this one, keeping the larger maximum.
    pub fn add(&mut self, other: &DrainAudit) {
        self.sealed += other.sealed;
        self.split += other.split;
        self.split_across_workers += other.split_across_workers;
        self.max_workers_per_batch = self.max_workers_per_batch.max(other.max_workers_per_batch);
        self.max_rows_per_batch = self.max_rows_per_batch.max(other.max_rows_per_batch);
        self.keys_in_several_batches += other.keys_in_several_batches;
    }
}

/// Control over the Postgres server itself, as opposed to a database on it
/// (issue #236): what a [`crate::model::DbAdminAction::RestartPostgres`]
/// needs. A trait rather than a `testkit` type so `run` names no cluster
/// type (its module doc comment), and so a harness that drives some other
/// server can supply its own.
pub trait ClusterControl {
    /// Stops the server in `mode` and starts it again, returning once it
    /// accepts connections. Every open connection is severed.
    fn restart_postgres(&self, mode: RestartMode);
}

impl ClusterControl for testkit::TestCluster {
    fn restart_postgres(&self, mode: RestartMode) {
        self.restart(match mode {
            RestartMode::Fast => testkit::StopMode::Fast,
            RestartMode::Immediate => testkit::StopMode::Immediate,
        });
    }
}

/// Backing a Postgres server up and restoring it into a fresh one (issue
/// #236): what `crate::run::run_convergence_with_restore` needs. A trait for
/// the same reason as [`ClusterControl`].
pub trait BackupRestore: Sized {
    /// A backup, kept until it has been restored from.
    type Backup;

    /// Stops the server, copies its whole data directory while it's down,
    /// and starts it again, returning once it accepts connections. Every
    /// open connection is severed, as by a restart.
    fn cold_backup(&self) -> Self::Backup;

    /// Starts a new server on a copy of `backup`, returning once it accepts
    /// connections.
    fn restore(backup: &Self::Backup) -> Self;

    /// The connection string for database `name` on this server.
    fn database_dsn(&self, name: &str) -> String;
}

impl BackupRestore for testkit::TestCluster {
    type Backup = testkit::ClusterBackup;

    fn cold_backup(&self) -> Self::Backup {
        testkit::TestCluster::cold_backup(self)
    }

    fn restore(backup: &Self::Backup) -> Self {
        testkit::TestCluster::from_backup(backup)
    }

    fn database_dsn(&self, name: &str) -> String {
        testkit::TestCluster::database_dsn(self, name)
    }
}
